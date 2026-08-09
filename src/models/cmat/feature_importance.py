"""Permutation Feature Importance for CMAT models.

Loads a saved checkpoint, recreates the dataset for that fold,
then measures how much each feature's shuffling degrades performance.

Usage:
    python -m src.models.cmat.feature_importance \
        --variant tab --horizon 60 --fold 0 --seed 42 \
        [--n-repeats 5] [--output results/feature_importance_tab_h60.csv]
"""

import argparse
import logging
import time
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import torch
from torch.utils.data import DataLoader

from . import config as C
from .model import CMAT, CMATConfig
from .train import CMATDataset, load_dataset, evaluate, get_feature_columns
from .search import load_best_config

logger = logging.getLogger("cmat.feature_importance")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)


def permutation_importance(
    model: torch.nn.Module,
    test_ds: CMATDataset,
    feature_names: List[str],
    device: torch.device,
    batch_size: int = 64,
    n_repeats: int = 5,
    target_min: float = 0.0,
    target_max: float = 1.0,
) -> pd.DataFrame:
    """Compute permutation importance for all continuous features.

    For each feature:
      1. Shuffle its values across test samples (breaking its signal)
      2. Re-evaluate the model
      3. Measure RMSE/PCC degradation vs. baseline

    Returns a DataFrame with columns:
      feature, baseline_rmse, shuffled_rmse_mean, shuffled_rmse_std,
      importance_rmse, baseline_pcc, shuffled_pcc_mean, shuffled_pcc_std,
      importance_pcc
    """
    model.eval()
    criterion = torch.nn.MSELoss()

    # Baseline evaluation
    loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False, num_workers=0)
    base_loss, preds_norm, targets_norm = evaluate(model, loader, criterion, device)

    target_range = target_max - target_min
    if target_range < 1e-8:
        target_range = 1.0
    preds_mw = preds_norm * target_range + target_min
    targets_mw = targets_norm * target_range + target_min

    from src.models.baselines import evaluation as BE
    base_metrics = BE.compute_metrics(targets_mw, preds_mw)
    logger.info("Baseline: RMSE=%.2f, PCC=%.4f, MAPE=%.2f%%",
                base_metrics.rmse, base_metrics.pcc, base_metrics.mape_pct)

    # Original continuous data (we'll restore after each permutation)
    original_cont = test_ds.cont_data.copy()
    n_features = len(feature_names)

    results = []

    for feat_idx, feat_name in enumerate(feature_names):
        rmse_scores = []
        pcc_scores = []

        for rep in range(n_repeats):
            # Shuffle feature feat_idx across all samples
            # This shuffles the entire column in the raw array, which means
            # different time windows get each other's feature values
            test_ds.cont_data = original_cont.copy()
            perm = np.random.permutation(len(test_ds.cont_data))
            test_ds.cont_data[:, feat_idx] = original_cont[perm, feat_idx]

            # Re-evaluate
            loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False, num_workers=0)
            _, p_norm, t_norm = evaluate(model, loader, criterion, device)
            p_mw = p_norm * target_range + target_min
            t_mw = t_norm * target_range + target_min
            m = BE.compute_metrics(t_mw, p_mw)
            rmse_scores.append(m.rmse)
            pcc_scores.append(m.pcc)

        # Restore original data
        test_ds.cont_data = original_cont.copy()

        rmse_mean = np.mean(rmse_scores)
        rmse_std = np.std(rmse_scores)
        pcc_mean = np.mean(pcc_scores)
        pcc_std = np.std(pcc_scores)

        # Importance = degradation from shuffling
        imp_rmse = rmse_mean - base_metrics.rmse  # positive = important
        imp_pcc = base_metrics.pcc - pcc_mean      # positive = important

        results.append({
            "feature": feat_name,
            "feat_idx": feat_idx,
            "baseline_rmse": base_metrics.rmse,
            "shuffled_rmse_mean": rmse_mean,
            "shuffled_rmse_std": rmse_std,
            "importance_rmse": imp_rmse,
            "baseline_pcc": base_metrics.pcc,
            "shuffled_pcc_mean": pcc_mean,
            "shuffled_pcc_std": pcc_std,
            "importance_pcc": imp_pcc,
        })

        logger.info(
            "[%2d/%d] %-25s  ΔRMSE=%+8.1f  ΔPCC=%+.4f",
            feat_idx + 1, n_features, feat_name, imp_rmse, imp_pcc,
        )

    df = pd.DataFrame(results).sort_values("importance_rmse", ascending=False)
    return df


def main():
    parser = argparse.ArgumentParser(description="CMAT Permutation Feature Importance")
    parser.add_argument("--variant", required=True, choices=["tab", "ntl"])
    parser.add_argument("--horizon", required=True, type=int)
    parser.add_argument("--fold", required=True, type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n-repeats", type=int, default=10)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # ── Load checkpoint ──
    ckpt_dir = Path(C.OUTPUT_DIR) / "checkpoints"
    ckpt_path = ckpt_dir / f"ckpt_{args.variant}_h{args.horizon}d_f{args.fold}_s{args.seed}.pt"

    if not ckpt_path.exists():
        logger.error("Checkpoint not found: %s", ckpt_path)
        logger.error("Run 1 fold of ROCV first to generate the checkpoint.")
        return

    logger.info("Loading checkpoint: %s", ckpt_path)
    ckpt = torch.load(ckpt_path, map_location=device, weights_only=False)

    cfg = ckpt["config"]
    cont_min = ckpt["cont_min"]
    cont_max = ckpt["cont_max"]
    target_min = ckpt["target_min"]
    target_max = ckpt["target_max"]

    # ── Rebuild model ──
    model = CMAT(cfg).to(device)
    model.decoder.set_horizon(cfg.context_window_hours, 1)
    model.decoder._W_H = model.decoder._W_H.to(device)
    model.load_state_dict(ckpt["model_state_dict"])
    model.eval()
    logger.info("Model loaded: %s params", f"{ckpt['n_params']:,}")

    # ── Recreate test dataset for this fold ──
    horizon_days = args.horizon
    horizon_hours = horizon_days * 24

    # Load full dataset and get fold split
    full_df = load_dataset()

    # ROCV split: same logic as run_rocv.py
    cutoffs = pd.date_range("2014-01-01", "2024-01-01", freq="YS")
    fold_idx = args.fold
    train_end = cutoffs[fold_idx]
    test_start = train_end
    test_end = test_start + pd.DateOffset(days=horizon_days)
    val_start = train_end - pd.DateOffset(days=horizon_days)

    train_df = full_df[:val_start - pd.Timedelta(hours=1)].copy()
    val_df = full_df[val_start:train_end - pd.Timedelta(hours=1)].copy()
    test_df = full_df[test_start:test_end].copy()

    logger.info("Fold %d: train=%d, val=%d, test=%d",
                fold_idx, len(train_df), len(val_df), len(test_df))

    # Get feature columns
    cont_cols, cat_cols = get_feature_columns(train_df, cfg)

    # Apply feature shifting (same as train_one_fold)
    if horizon_hours > 0:
        combined = pd.concat([train_df, val_df, test_df])
        for col in cont_cols:
            combined[col] = combined[col].shift(horizon_hours)
        train_df = combined.loc[train_df.index].iloc[horizon_hours:].copy()
        val_df = combined.loc[val_df.index].copy()
        test_df = combined.loc[test_df.index].copy()

    # Forward-fill NaNs
    for split_df in [train_df, val_df, test_df]:
        for col in cont_cols + [C.TARGET]:
            if col in split_df.columns:
                split_df[col] = split_df[col].ffill().bfill()

    # Build test dataset with saved normalization stats
    dataset_kwargs = dict(
        cfg=cfg, cont_cols=cont_cols, cat_cols=cat_cols,
        ntl_store=None, ntl_mean=0.0, ntl_std=1.0,
        target_min=target_min, target_max=target_max,
        cont_min=cont_min, cont_max=cont_max,
        horizon_hours=horizon_hours,
    )
    test_ds = CMATDataset(test_df, **dataset_kwargs)
    logger.info("Test dataset: %d samples", len(test_ds))

    # ── Run permutation importance ──
    logger.info("Running permutation importance (%d repeats per feature)...", args.n_repeats)
    t0 = time.time()

    df = permutation_importance(
        model, test_ds, cont_cols,
        device=device,
        batch_size=cfg.batch_size,
        n_repeats=args.n_repeats,
        target_min=target_min,
        target_max=target_max,
    )

    elapsed = time.time() - t0
    logger.info("Permutation importance completed in %.1fs", elapsed)

    # ── Save results ──
    output_path = args.output or str(
        Path(C.OUTPUT_DIR) / f"feature_importance_{args.variant}_h{args.horizon}d.csv"
    )
    df.to_csv(output_path, index=False)
    logger.info("Results saved → %s", output_path)

    # Print ranked table
    print("\n" + "=" * 80)
    print(f"  Feature Importance: CMAT-{args.variant.upper()} h{args.horizon}d "
          f"(fold {args.fold}, seed {args.seed})")
    print("=" * 80)
    print(f"{'Rank':>4}  {'Feature':<28} {'ΔRMSE':>10} {'ΔPCC':>10}")
    print("-" * 56)
    for rank, (_, row) in enumerate(df.iterrows(), 1):
        print(f"{rank:>4}  {row['feature']:<28} {row['importance_rmse']:>+10.1f} "
              f"{row['importance_pcc']:>+10.4f}")
    print()


if __name__ == "__main__":
    main()
