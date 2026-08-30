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


def _eval_to_metrics(model, test_ds, device, batch_size, target_min, target_max):
    """Helper: evaluate model and return metrics."""
    from src.models.baselines import evaluation as BE
    criterion = torch.nn.MSELoss()
    loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False, num_workers=0)
    _, p_norm, t_norm = evaluate(model, loader, criterion, device)
    target_range = max(target_max - target_min, 1e-8)
    p_mw = p_norm * target_range + target_min
    t_mw = t_norm * target_range + target_min
    return BE.compute_metrics(t_mw, p_mw)


def permutation_importance(
    model: torch.nn.Module,
    test_ds: CMATDataset,
    feature_names: List[str],
    device: torch.device,
    batch_size: int = 64,
    n_repeats: int = 5,
    target_min: float = 0.0,
    target_max: float = 1.0,
    include_images: bool = False,
) -> pd.DataFrame:
    """Compute permutation importance for all continuous features.

    For each feature:
      1. Shuffle its values across test samples (breaking its signal)
      2. Re-evaluate the model
      3. Measure RMSE/PCC degradation vs. baseline

    If include_images=True, also measures importance of the NTL image branch
    by shuffling the image tensor across samples.

    Returns a DataFrame with columns:
      feature, baseline_rmse, shuffled_rmse_mean, shuffled_rmse_std,
      importance_rmse, baseline_pcc, shuffled_pcc_mean, shuffled_pcc_std,
      importance_pcc
    """
    model.eval()

    # Baseline evaluation
    base_metrics = _eval_to_metrics(
        model, test_ds, device, batch_size, target_min, target_max
    )
    logger.info("Baseline: RMSE=%.2f, PCC=%.4f, MAPE=%.2f%%",
                base_metrics.rmse, base_metrics.pcc, base_metrics.mape_pct)

    # Original continuous data (we'll restore after each permutation)
    original_cont = test_ds.cont_data.copy()

    # Build the list of features to test
    features_to_test = list(enumerate(feature_names))
    if include_images:
        features_to_test.append((-1, "ntl_images"))  # sentinel index
    n_features = len(features_to_test)

    results = []

    for step, (feat_idx, feat_name) in enumerate(features_to_test):
        rmse_scores = []
        pcc_scores = []

        for rep in range(n_repeats):
            if feat_idx == -1:
                # Shuffle NTL images: we monkey-patch the NTLStore to return
                # shuffled images by permuting the date→index mapping.
                # Simpler approach: temporarily swap ntl_store with a shuffling wrapper.
                _orig_store = test_ds.ntl_store
                test_ds.ntl_store = _ShuffledNTLStore(_orig_store)
            else:
                # Shuffle tabular feature
                test_ds.cont_data = original_cont.copy()
                perm = np.random.permutation(len(test_ds.cont_data))
                test_ds.cont_data[:, feat_idx] = original_cont[perm, feat_idx]

            # Re-evaluate
            m = _eval_to_metrics(
                model, test_ds, device, batch_size, target_min, target_max
            )
            rmse_scores.append(m.rmse)
            pcc_scores.append(m.pcc)

            # Restore
            if feat_idx == -1:
                test_ds.ntl_store = _orig_store
            else:
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
            step + 1, n_features, feat_name, imp_rmse, imp_pcc,
        )

    df = pd.DataFrame(results).sort_values("importance_rmse", ascending=False)
    return df


class _ShuffledNTLStore:
    """Wrapper that returns images from random dates (breaking temporal signal)."""
    def __init__(self, real_store):
        self._store = real_store
        self._all_dates = list(real_store._date_to_idx.keys()) \
            if hasattr(real_store, '_date_to_idx') else None

    def get_images_for_window(self, end_date, n_days):
        """Return images for random dates instead of the requested window."""
        if self._all_dates is not None:
            random_dates = np.random.choice(self._all_dates, size=n_days, replace=True)
            # Stack images from random dates
            imgs = np.stack([
                self._store.get_images_for_window(end_date=d, n_days=1)[0]
                for d in random_dates
            ])
            return imgs
        else:
            # Fallback: get normal images and shuffle along the time axis
            imgs = self._store.get_images_for_window(end_date=end_date, n_days=n_days)
            np.random.shuffle(imgs)  # shuffle in-place along axis 0
            return imgs

    def get_normalisation_stats(self, *args, **kwargs):
        return self._store.get_normalisation_stats(*args, **kwargs)


def main():
    parser = argparse.ArgumentParser(description="CMAT Permutation Feature Importance")
    parser.add_argument("--variant", required=True, choices=["tab", "ntl", "full", "early"])
    parser.add_argument("--horizon", required=True, type=int)
    parser.add_argument("--fold", required=True, type=int)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--n-repeats", type=int, default=10)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # ── Load checkpoint ──
    # CHECKPOINT_DIR honours the CMAT_CHECKPOINT_DIR env override, matching
    # where run_rocv.py under the leakage-fixed pipeline saves checkpoints.
    ckpt_dir = Path(C.CHECKPOINT_DIR)
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
    ntl_mean = ckpt.get("ntl_mean", 0.0)
    ntl_std = ckpt.get("ntl_std", 1.0)

    # ── Rebuild model ──
    # n_cont must match training exactly: base features + this fold's
    # per-fold PCA columns. The saved cont_min array has one entry per
    # continuous input, so its length is the authoritative count.
    n_cont = len(cont_min)
    model = CMAT(cfg, n_cont=n_cont).to(device)
    model.decoder.set_horizon(cfg.context_window_hours, 1)
    model.decoder._W_H = model.decoder._W_H.to(device)
    model.load_state_dict(ckpt["model_state_dict"])
    model.eval()
    logger.info("Model loaded: %s params (%d continuous inputs)",
                f"{ckpt['n_params']:,}", n_cont)

    # ── Load NTL images if needed ──
    ntl_store = None
    uses_spatial = cfg.uses_spatial
    if uses_spatial:
        from .ntl_images import NTLImageStore
        ntl_store = NTLImageStore()
        ntl_store.preload_all()
        logger.info("NTL images loaded (mean=%.2f, std=%.2f)", ntl_mean, ntl_std)

    # ── Recreate test dataset for this fold ──
    horizon_days = args.horizon
    horizon_hours = horizon_days * 24

    # Load full dataset and get fold split — the SAME 13-month-step ROCV
    # protocol as run_rocv.py (leakage-fixed pipeline), not the old
    # annual-origin split.
    full_df = load_dataset()

    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from baselines import config as BC
    from baselines.data_loader import rocv_folds, split_fold

    rocv_cfg = BC.ROCVConfig()
    folds = list(rocv_folds(full_df, rocv_cfg))
    fold_idx = args.fold
    if fold_idx >= len(folds):
        logger.error("Fold %d not available (max=%d)", fold_idx, len(folds) - 1)
        return
    _, train_end, val_end, test_origin = folds[fold_idx]
    train_df, val_df, test_df = split_fold(
        full_df, train_end, val_end, test_origin, horizon_days
    )

    logger.info("Fold %d: train=%d, val=%d, test=%d",
                fold_idx, len(train_df), len(val_df), len(test_df))

    # Get feature columns
    cont_cols, cat_cols = get_feature_columns(train_df, cfg)

    # ── Preprocessing: must mirror train_one_fold EXACTLY ──
    # Order there: shift → IQR clip (train only) → per-fold PCA (fit on
    # train, transform all splits) → ffill → MinMax (via saved stats).

    # 1. Feature shifting
    if horizon_hours > 0:
        combined = pd.concat([train_df, val_df, test_df])
        for col in cont_cols:
            combined[col] = combined[col].shift(horizon_hours)
        train_df = combined.loc[train_df.index].iloc[horizon_hours:].copy()
        val_df = combined.loc[val_df.index].copy()
        test_df = combined.loc[test_df.index].copy()

    # 2. IQR clipping on training data (needed so the PCA refit below sees
    #    identical inputs to the one fitted at training time)
    from .train import clip_iqr, _fit_fold_pca
    train_df = clip_iqr(train_df, cont_cols)

    # 3. Per-fold PCA — deterministic refit on the same training data
    #    reproduces the identical components the checkpointed model was
    #    trained with; transforms all splits and appends the PCA columns.
    pca_col_names, train_df, val_df, test_df = _fit_fold_pca(
        train_df, val_df, test_df,
    )
    if pca_col_names:
        cont_cols = cont_cols + pca_col_names
        logger.info("Recreated %d per-fold PCA columns. Total continuous: %d.",
                    len(pca_col_names), len(cont_cols))
    if len(cont_cols) != n_cont:
        logger.error(
            "Feature count mismatch: rebuilt %d continuous cols but the "
            "checkpoint expects %d — preprocessing drifted from training. "
            "Aborting rather than computing wrong importances.",
            len(cont_cols), n_cont,
        )
        return

    # 4. Forward-fill NaNs
    for split_df in [train_df, val_df, test_df]:
        for col in cont_cols + [C.TARGET]:
            if col in split_df.columns:
                split_df[col] = split_df[col].ffill().bfill()

    # Build test dataset with saved normalization stats
    dataset_kwargs = dict(
        cfg=cfg, cont_cols=cont_cols, cat_cols=cat_cols,
        ntl_store=ntl_store, ntl_mean=ntl_mean, ntl_std=ntl_std,
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
        include_images=uses_spatial,
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
