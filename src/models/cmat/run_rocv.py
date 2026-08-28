"""CMAT ROCV evaluation: runs all fold/horizon combinations.

Usage:
    python -m src.models.cmat.run_rocv --variant full --horizon 60 [--resume]
    python -m src.models.cmat.run_rocv --variant full --horizon 60 --quick
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import torch

# Baseline utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from baselines import config as BC
from baselines import evaluation as BE
from baselines.data_loader import rocv_folds, split_fold

from . import config as C
from .config import CMATConfig, CMATVariant
from .train import (
    load_dataset,
    train_one_fold,
    save_fold_result_locked,
    is_fold_done,
)

logger = logging.getLogger("cmat.run_rocv")


def run_rocv(
    variant: str = "full",
    horizon_days: int = 60,
    resume: bool = False,
    quick: bool = False,
    config_path: str = None,
    start_fold: int = 0,
    max_folds: int = 0,
    seed: int = 42,
) -> None:
    """Run ROCV evaluation for one CMAT variant at one horizon.

    Parameters
    ----------
    variant : str
        CMAT variant ("tab", "ntl", "early", "full").
    horizon_days : int
        Forecast horizon in days.
    resume : bool
        Skip already-completed folds.
    quick : bool
        Run only the first fold (for smoke testing).
    start_fold : int
        Index of the first fold to run (0-based). Default 0.
    max_folds : int
        Maximum number of folds to run. 0 = all folds.
    config_path : str
        Path to a best_config JSON from the Optuna search.
        If None, uses default config.
    """
    variant_enum = CMATVariant(variant)
    model_name = f"cmat_{variant}"
    output_dir = C.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("CMAT ROCV Evaluation")
    logger.info("  Variant:  %s", variant)
    logger.info("  Horizon:  %d days (%d hours)", horizon_days, horizon_days * 24)
    logger.info("  Resume:   %s", resume)
    logger.info("  Quick:    %s", quick)
    logger.info("  Output:   %s", output_dir)
    logger.info("=" * 70)

    # Load config (from search or default)
    if config_path:
        from .search import load_best_config
        cfg = load_best_config(variant, horizon_days, output_dir=str(Path(config_path).parent))
        logger.info("Loaded best config from %s.", config_path)
    else:
        # Try to load from default search output location
        try:
            from .search import load_best_config
            cfg = load_best_config(variant, horizon_days)
            logger.info("Loaded best config from search results.")
        except FileNotFoundError:
            logger.info("No search results found — using default config.")
            cfg = CMATConfig(
                variant=variant_enum,
                horizon_hours=horizon_days * 24,
            )

    # Ensure variant and horizon match
    cfg.variant = variant_enum
    cfg.horizon_hours = horizon_days * 24

    # Use generous training budget to let the model escape flat-mean minima
    cfg.max_epochs = 50
    cfg.early_stop_patience = 15
    cfg.min_epochs_before_es = 10
    cfg.seed = seed
    logger.info("  Seed:     %d", seed)

    # Load data
    df = load_dataset()

    # NTL image store (for spatial variants)
    ntl_store = None
    if cfg.uses_spatial:
        from .ntl_images import NTLImageStore
        ntl_store = NTLImageStore()
        # Preload all images into RAM — eliminates GPFS I/O during training
        logger.info("Preloading NTL images into RAM...")
        ntl_store.preload_all()

    # Device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Device: %s", device)
    if device.type == "cuda":
        logger.info("GPU: %s", torch.cuda.get_device_name(0))

    # Generate folds
    rocv_cfg = BC.ROCVConfig()
    folds = list(rocv_folds(df, rocv_cfg))
    logger.info("Total folds: %d (quick=%s).", len(folds), quick)

    if quick:
        folds = folds[:1]
    else:
        # Apply fold range limits
        if start_fold > 0:
            folds = [f for f in folds if f[0] >= start_fold]
            logger.info("Starting from fold %d (%d folds remaining).", start_fold, len(folds))
        if max_folds > 0:
            folds = folds[:max_folds]
            logger.info("Limiting to %d folds.", max_folds)

    # Pre-load this (model, horizon, seed)'s own shard so a resumed run
    # appends to its existing rows instead of overwriting them.
    results = []
    shard_path = output_dir / "_shards" / f"{model_name}_h{horizon_days}_s{cfg.seed}.csv"
    if resume and shard_path.exists():
        import pandas as pd
        results = pd.read_csv(shard_path).to_dict("records")
        logger.info("Resumed: loaded %d existing results from shard.", len(results))

    t0_total = time.time()

    for fold_idx, train_end, val_end, test_origin in folds:
        # Check resume
        if resume and is_fold_done(output_dir, model_name, fold_idx, horizon_days, seed=cfg.seed):
            logger.info("Fold %d / H=%dd: SKIP (already done).", fold_idx, horizon_days)
            continue

        logger.info("─" * 60)
        logger.info("Fold %d / H=%dd", fold_idx, horizon_days)
        logger.info("  train_end:   %s", train_end.date())
        logger.info("  val_end:     %s", val_end.date())
        logger.info("  test_origin: %s", test_origin.date())

        # Split data
        train_df, val_df, test_df = split_fold(
            df, train_end, val_end, test_origin, horizon_days
        )

        if len(train_df) == 0 or len(val_df) == 0 or len(test_df) == 0:
            logger.warning("Fold %d: empty split — skipping.", fold_idx)
            continue

        # Check if we have enough data for context window
        if len(train_df) < cfg.context_window_hours + 1:
            logger.warning(
                "Fold %d: insufficient training data (%d < %d) — skipping.",
                fold_idx, len(train_df),
                cfg.context_window_hours + 1,
            )
            continue

        try:
            t0_fold = time.time()
            result = train_one_fold(
                cfg, train_df, val_df, test_df,
                ntl_store=ntl_store, device=device,
                horizon_hours=horizon_days * 24,
                fold_idx=fold_idx,
            )
            fold_time = time.time() - t0_fold

            if result is None:
                logger.warning("Fold %d: train_one_fold returned None — skipping.", fold_idx)
                continue

            metrics = result["metrics"]
            result_row = {
                "model": model_name,
                "fold": fold_idx,
                "train_end": str(train_end.date()),
                "horizon_days": horizon_days,
                "rmse": metrics.rmse,
                "mae": metrics.mae,
                "mape_pct": metrics.mape_pct,
                "mase": metrics.mase,
                "nmae": metrics.nmae,
                "nrmse": metrics.nrmse,
                "pcc": metrics.pcc,
                "n_samples": metrics.n_samples,
                "best_val_loss": result["best_val_loss"],
                "n_params": result["n_params"],
                "n_epochs": result["n_epochs"],
                "train_time_s": result["train_time_s"],
                "infer_time_s": result["infer_time_s"],
                "n_train": result["n_train"],
                "n_val": result["n_val"],
                "n_test": result["n_test"],
                "elapsed_s": fold_time,
                "seed": cfg.seed,
            }

            save_fold_result_locked(result_row, results, output_dir, model_name)
            logger.info(
                "Fold %d done in %.1fs: RMSE=%.2f  MAE=%.2f  MAPE=%.2f%%  PCC=%.4f",
                fold_idx, fold_time, metrics.rmse, metrics.mae,
                metrics.mape_pct, metrics.pcc,
            )

            # ── Budget protection: abort if fold took too long ──
            MAX_FOLD_SECONDS = 8.0 * 3600 if cfg.variant.value == "full" else 2.5 * 3600
            if fold_time > MAX_FOLD_SECONDS:
                limit_h = MAX_FOLD_SECONDS / 3600
                logger.warning(
                    "⚠️  BUDGET GUARD: Fold %d took %.1fh (limit=%.1fh). "
                    "Stopping to protect SBU budget. %d folds saved so far.",
                    fold_idx, fold_time / 3600, limit_h, len(results),
                )
                break

        except Exception as exc:
            logger.error("Fold %d failed: %s", fold_idx, exc, exc_info=True)
            continue

    total_time = time.time() - t0_total
    logger.info("=" * 70)
    logger.info(
        "ROCV complete: %d folds in %.1fs (%.1f min).",
        len(results), total_time, total_time / 60,
    )

    # Best-effort merge of all shards (from this and any other completed
    # tasks) into the combined CSV. Safe to race — shards are the source
    # of truth, so a concurrent merge just means the next merge picks up
    # anything missed.
    try:
        from baselines.merge_shards import merge_shards
        merge_shards(output_dir, model_name)
    except Exception as exc:
        logger.warning("Shard merge failed (non-fatal, shards are safe): %s", exc)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="CMAT ROCV evaluation — run all folds for one horizon."
    )
    parser.add_argument(
        "--variant", type=str, default="full",
        choices=["tab", "ntl", "early", "full"],
        help="CMAT variant to evaluate."
    )
    parser.add_argument(
        "--horizon", type=int, required=True,
        help="Forecast horizon in days (60, 75, ..., 180)."
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Skip already-completed folds."
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="Run only the first fold (smoke test)."
    )
    parser.add_argument(
        "--config", type=str, default=None,
        help="Path to best_config JSON from Optuna search."
    )
    parser.add_argument(
        "--start-fold", type=int, default=0,
        help="Index of the first fold to run (0-based). Default: 0."
    )
    parser.add_argument(
        "--max-folds", type=int, default=0,
        help="Maximum number of folds to run. 0 = all. Default: 0."
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility. Default: 42."
    )
    args = parser.parse_args()

    run_rocv(
        variant=args.variant,
        horizon_days=args.horizon,
        resume=args.resume,
        quick=args.quick,
        config_path=args.config,
        start_fold=args.start_fold,
        max_folds=args.max_folds,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
