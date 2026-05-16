#!/usr/bin/env python3
"""Train and evaluate the Prophet-LSTM baseline using ROCV.

Faithfully reproduces the reference implementation's pipeline:

    1. Load hourly dataset → drop NTL → merge weather
    2. For each forecast horizon (60–180 days, step 15):
        a. Shift data by ``horizon`` days (all predictors become lagged)
        b. For each ROCV fold:
            - Split train/test at cutoff
            - IQR outlier removal on train
            - MICE imputation (fit train → transform test)
            - Prophet with lagged predictors as regressors
            - MinMaxScale (fit train → transform test)
            - LSTM (Huang or Tan) with early stopping
            - Inverse-transform predictions → evaluate (RMSE, MAE, MAPE, r)

Usage
-----
    # Full run (both variants, all horizons, all folds):
    python -u src/models/baselines/train_prophet_lstm.py

    # Single variant:
    python -u src/models/baselines/train_prophet_lstm.py --model-variant huang

    # Quick test (first fold, first horizon):
    python -u src/models/baselines/train_prophet_lstm.py --quick

    # Single horizon:
    python -u src/models/baselines/train_prophet_lstm.py --horizon 60
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# Ensure repo root on sys.path
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.models.baselines import config as C
from src.models.baselines.data_loader import (
    load_dataset,
    rocv_folds,
    shift_data,
)
from src.models.baselines.evaluation import compute_metrics
from src.models.baselines.prophet_lstm import ProphetLSTM

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("train_prophet_lstm")

# Suppress TF noise
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")


# ---------------------------------------------------------------------------
# Main training loop
# ---------------------------------------------------------------------------

def run_rocv(
    df: pd.DataFrame,
    model_variant: str,
    rocv_cfg: C.ROCVConfig,
    prophet_cfg: C.ProphetConfig,
    lstm_cfg: C.LSTMConfig,
    output_dir: Path,
    quick: bool = False,
    single_horizon: int = None,
) -> pd.DataFrame:
    """Execute the full ROCV loop for a single model variant.

    Parameters
    ----------
    model_variant : "huang" (Model 1, no dropout) or "tan" (Model 2, dropout)
    quick : If True, run only the first fold and first horizon.
    single_horizon : If set, only run this specific horizon.
    """
    logger.info("=" * 70)
    logger.info("  PROPHET-LSTM %s", model_variant.upper())
    logger.info("=" * 70)

    horizons = [single_horizon] if single_horizon else rocv_cfg.forecast_horizons_days
    if quick:
        horizons = [horizons[0]]

    results = []

    for h_days in horizons:
        h_t0 = time.time()
        logger.info("━" * 60)
        logger.info("HORIZON = %d days (%d hours)", h_days, h_days * 24)
        logger.info("━" * 60)

        # Shift data for this horizon
        # (each horizon gets its own shifted dataset, matching reference)
        df_shifted = shift_data(df, n_days_lag=h_days)

        for fold_idx, cutoff in rocv_folds(df, rocv_cfg):
            fold_t0 = time.time()
            logger.info("  FOLD %d — cutoff %s, horizon %d days",
                         fold_idx, cutoff.date(), h_days)

            # Run the full pipeline for this fold
            model = ProphetLSTM(
                model_variant=model_variant,
                prophet_cfg=prophet_cfg,
                lstm_cfg=lstm_cfg,
            )

            fold_result = model.run_fold(df_shifted, cutoff, h_days)

            if "predictions_mw" not in fold_result:
                logger.warning("    Fold %d skipped (empty data).", fold_idx)
                continue

            # Compute metrics on original-scale values
            metrics = compute_metrics(
                fold_result["actuals_mw"],
                fold_result["predictions_mw"],
            )

            fold_elapsed = time.time() - fold_t0

            result_row = {
                "model": f"prophet_lstm_{model_variant}",
                "fold": fold_idx,
                "cutoff": str(cutoff.date()),
                "horizon_days": h_days,
                "horizon_hours": h_days * 24,
                "n_train": fold_result["n_train"],
                "n_test": fold_result["n_test"],
                **metrics.to_dict(),
                "elapsed_s": round(fold_elapsed, 1),
            }
            results.append(result_row)

            logger.info(
                "    H=%3dd | RMSE=%7.1f  MAE=%7.1f  "
                "MAPE=%.2f%%  r=%.4f | %d pts  (%.1fs)",
                h_days,
                metrics.rmse, metrics.mae, metrics.mape_pct, metrics.pearson_r,
                metrics.n_samples, fold_elapsed,
            )

            if quick:
                logger.info("    --quick mode: stopping after first fold.")
                break

        h_elapsed = time.time() - h_t0
        logger.info("  Horizon %d days complete in %.0fs.", h_days, h_elapsed)

    results_df = pd.DataFrame(results)

    # Save results
    out_csv = output_dir / f"rocv_results_{model_variant}.csv"
    results_df.to_csv(out_csv, index=False)
    logger.info("Results CSV → %s", out_csv)

    out_json = output_dir / f"rocv_results_{model_variant}.json"
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info("Results JSON → %s", out_json)

    return results_df


def print_summary(results_df: pd.DataFrame, variant: str):
    """Print aggregated summary across folds for each horizon."""
    if results_df.empty:
        return

    logger.info("\n" + "=" * 70)
    logger.info("  SUMMARY — %s", variant)
    logger.info("=" * 70)

    summary = (
        results_df
        .groupby("horizon_days")
        .agg(
            rmse_mean=("rmse", "mean"),
            rmse_std=("rmse", "std"),
            mae_mean=("mae", "mean"),
            mape_mean=("mape_pct", "mean"),
            r_mean=("pearson_r", "mean"),
            n_folds=("fold", "count"),
        )
        .round(2)
    )
    logger.info("\n%s", summary.to_string())
    return summary


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Train Prophet-LSTM baseline via ROCV.",
    )
    parser.add_argument(
        "--data-path", type=str, default=None,
        help=f"Path to final Parquet. Default: {C.DATA_ROOT}",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help=f"Results directory. Default: {C.OUTPUT_DIR}",
    )
    parser.add_argument(
        "--model-variant", type=str, choices=["huang", "tan"], default=None,
        help="Run only one variant. Default: run both.",
    )
    parser.add_argument(
        "--horizon", type=int, default=None,
        help="Run only a single horizon (e.g. 60). Default: all.",
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="Quick test: single fold, single horizon.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else C.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    logger.info("Prophet-LSTM Baseline Training")
    logger.info("  Started:    %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("  Output dir: %s", output_dir)

    # ── Load data ──
    df = load_dataset(args.data_path)

    # ── Configs ──
    rocv_cfg = C.ROCVConfig()
    prophet_cfg = C.ProphetConfig()
    lstm_cfg = C.LSTMConfig()

    logger.info("ROCV horizons: %s days", rocv_cfg.forecast_horizons_days)

    # ── Run ──
    variants = (
        [args.model_variant] if args.model_variant
        else ["huang", "tan"]
    )

    all_results = []
    for variant in variants:
        results_df = run_rocv(
            df=df,
            model_variant=variant,
            rocv_cfg=rocv_cfg,
            prophet_cfg=prophet_cfg,
            lstm_cfg=lstm_cfg,
            output_dir=output_dir,
            quick=args.quick,
            single_horizon=args.horizon,
        )
        all_results.append(results_df)
        print_summary(results_df, f"Prophet-LSTM {variant}")

    # ── Combined results ──
    combined = pd.concat(all_results, ignore_index=True)
    combined.to_csv(output_dir / "rocv_results_combined.csv", index=False)

    # ── Run metadata ──
    meta = {
        "script": "train_prophet_lstm.py",
        "started": datetime.now().isoformat(),
        "data_path": str(args.data_path or C.DATA_ROOT),
        "rocv": {
            "start_date": rocv_cfg.start_date,
            "min_train_years": rocv_cfg.min_train_years,
            "fold_step_years": rocv_cfg.fold_step_years,
            "horizons_days": rocv_cfg.forecast_horizons_days,
        },
        "prophet": {
            "daily_seasonality": prophet_cfg.daily_seasonality,
            "weekly_seasonality": prophet_cfg.weekly_seasonality,
            "yearly_seasonality": prophet_cfg.yearly_seasonality,
        },
        "lstm": {
            "lstm1_units": lstm_cfg.lstm1_units,
            "lstm2_units_huang": lstm_cfg.lstm2_units_huang,
            "lstm2_units_tan": lstm_cfg.lstm2_units_tan,
            "dropout_rates": [lstm_cfg.dropout_rate_1, lstm_cfg.dropout_rate_2],
            "epochs": lstm_cfg.epochs,
            "batch_size": lstm_cfg.batch_size,
            "early_stop_patience": lstm_cfg.early_stop_patience,
            "early_stop_min_delta": lstm_cfg.early_stop_min_delta,
            "early_stop_start_epoch": lstm_cfg.early_stop_start_epoch,
        },
        "variants_run": variants,
        "total_elapsed_s": round(time.time() - t0, 1),
    }
    with open(output_dir / "run_metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    total = time.time() - t0
    logger.info("\n" + "=" * 70)
    logger.info("  DONE — %.0fs (%.1f min)", total, total / 60)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
