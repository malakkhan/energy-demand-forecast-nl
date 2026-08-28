#!/usr/bin/env python3
"""Seasonal Naïve baseline — 1-year lag point prediction.

Prediction: ŷ(t) = y(t − 365 days)

This is the simplest reference baseline: each hourly forecast is the
observed load from exactly one year (8760 hours) prior.  No model
fitting is required; the only computation is a shifted lookup.

Evaluation protocol: same ROCV as all other baselines.
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

# ── Project imports ──────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.models.baselines.evaluation import compute_metrics, ForecastMetrics

# ── Constants ────────────────────────────────────────────────────────
TARGET = "entsoe_load_mw"
ROCV_START = "2012-01-01"
MIN_TRAIN_YEARS = 2
LAG_HOURS = 365 * 24  # 8760 hours = 1 year

# Import fold step from config
from src.models.baselines.config import ROCVConfig, RANDOM_SEEDS

HORIZONS_DAYS = list(range(60, 181, 15))  # [60, 75, ..., 180]

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)


# ═════════════════════════════════════════════════════════════════════
# Data loading
# ═════════════════════════════════════════════════════════════════════

def load_dataset(data_path: str) -> pd.DataFrame:
    """Load the merged parquet dataset."""
    df = pd.read_parquet(data_path)
    if "timestamp" not in df.columns:
        raise ValueError("Dataset must contain 'timestamp' column.")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    df = df.dropna(subset=[TARGET])
    logger.info("Loaded %d rows from %s.", len(df), data_path)
    return df


# ═════════════════════════════════════════════════════════════════════
# ROCV fold generation (consistent with other baselines)
# ═════════════════════════════════════════════════════════════════════

def rocv_folds(df: pd.DataFrame, cfg: ROCVConfig):
    """Generate expanding-window ROCV folds.

    For Seasonal Naïve there is no training, but we still need >= 1 year
    of history before the test origin for the lag lookup.

    Yields: (fold_idx, test_origin)
    """
    start = pd.Timestamp(ROCV_START)
    data_end = df["timestamp"].max()
    max_horizon_td = pd.Timedelta(days=max(HORIZONS_DAYS))

    # First test origin: same as MLR (min_train + 1 year for val)
    test_origin = start + pd.DateOffset(years=MIN_TRAIN_YEARS + 1)

    fold_idx = 0
    while test_origin + max_horizon_td <= data_end:
        yield fold_idx, test_origin
        test_origin += pd.DateOffset(months=cfg.fold_step_months)
        fold_idx += 1


# ═════════════════════════════════════════════════════════════════════
# Seasonal Naïve prediction
# ═════════════════════════════════════════════════════════════════════

def run_seasonal_naive_fold(
    df: pd.DataFrame,
    test_origin: pd.Timestamp,
    horizon_days: int,
) -> Dict:
    """Run Seasonal Naïve for one fold × one horizon.

    For each hour t in the test window [test_origin, test_origin + H):
        ŷ(t) = y(t − 365 days)

    If the lagged value is missing, we fall back to y(t − 364 days) or
    y(t − 366 days) to handle leap years, then NaN if both are missing.
    """
    t0 = time.time()

    horizon_td = pd.Timedelta(days=horizon_days)
    test_mask = (
        (df["timestamp"] >= test_origin) &
        (df["timestamp"] < test_origin + horizon_td)
    )
    test_df = df.loc[test_mask].copy()

    if len(test_df) == 0:
        logger.warning("Empty test for origin=%s H=%dd.", test_origin, horizon_days)
        return {"n_test": 0}

    # Create a lookup series indexed by timestamp
    ts_lookup = df.set_index("timestamp")[TARGET]

    y_true = test_df[TARGET].values
    y_pred = np.full_like(y_true, np.nan, dtype=np.float64)

    for i, ts in enumerate(test_df["timestamp"].values):
        ts = pd.Timestamp(ts)
        # Try exact 365-day lag
        lag_ts = ts - pd.Timedelta(hours=LAG_HOURS)
        if lag_ts in ts_lookup.index:
            y_pred[i] = ts_lookup[lag_ts]
        else:
            # Try 364 and 366 days for leap year handling
            for alt_days in [364, 366]:
                alt_ts = ts - pd.Timedelta(days=alt_days)
                if alt_ts in ts_lookup.index:
                    y_pred[i] = ts_lookup[alt_ts]
                    break

    # Drop NaN pairs
    valid = np.isfinite(y_true) & np.isfinite(y_pred)
    n_valid = valid.sum()

    if n_valid < 10:
        logger.warning("Only %d valid predictions for origin=%s H=%dd.",
                       n_valid, test_origin, horizon_days)
        return {"n_test": n_valid}

    metrics = compute_metrics(y_true[valid], y_pred[valid])
    infer_time_s = time.time() - t0
    elapsed = infer_time_s
    train_time_s = 0.0

    return {
        "horizon_days": horizon_days,
        "test_origin": str(test_origin.date()),
        "n_test": n_valid,
        "train_time_s": round(train_time_s, 2),
        "infer_time_s": round(infer_time_s, 2),
        "elapsed_s": round(elapsed, 2),
        "metrics": metrics,
    }


# ═════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Seasonal Naïve baseline (1-year lag)."
    )
    parser.add_argument(
        "--data-path", type=str, required=True,
        help="Path to merged parquet dataset.",
    )
    parser.add_argument(
        "--results-dir", type=str,
        default=os.path.join(
            os.path.dirname(__file__), "results", "seasonal_naive"
        ),
        help="Directory to write results JSON.",
    )
    parser.add_argument(
        "--horizons", type=int, nargs="+", default=HORIZONS_DAYS,
        help="Forecast horizons in days.",
    )
    parser.add_argument(
        "--fold-step", type=int, default=None,
        help="Override fold_step_months (default: 13).",
    )
    args = parser.parse_args()

    os.makedirs(args.results_dir, exist_ok=True)
    horizons = args.horizons

    # Load data
    df = load_dataset(args.data_path)

    # ROCV config
    cfg = ROCVConfig()
    if args.fold_step is not None:
        cfg.fold_step_months = args.fold_step

    logger.info("Horizons: %s", horizons)
    logger.info("Fold step: %d month(s)", cfg.fold_step_months)

    # Run ROCV
    all_results = {}
    folds = list(rocv_folds(df, cfg))
    logger.info("Total folds: %d", len(folds))

    for fold_idx, test_origin in folds:
        for h in horizons:
            result = run_seasonal_naive_fold(df, test_origin, h)

            if "metrics" in result:
                m = result["metrics"]
                logger.info(
                    "Fold %2d | H=%3dd | RMSE=%7.1f  MAE=%7.1f  "
                    "MAPE=%.2f%%  PCC=%.4f | %d pts  (%.1fs)",
                    fold_idx, h, m.rmse, m.mae, m.mape_pct, m.pcc,
                    result["n_test"], result["elapsed_s"],
                )
                key = f"fold{fold_idx:02d}_H{h}d"
                all_results[key] = {
                    "fold": fold_idx,
                    "horizon_days": h,
                    "test_origin": result["test_origin"],
                    "n_test": result["n_test"],
                    "n_val": 0,
                    "train_time_s": result.get("train_time_s", 0.0),
                    "infer_time_s": result.get("infer_time_s", 0.0),
                    "n_train": result.get("n_train", 0),
                    "seed": "N/A",
                    **m.to_dict(),
                }

    # Save results
    meta = {
        "model": "Seasonal Naïve (1-year lag)",
        "lag_hours": LAG_HOURS,
        "horizons_days": horizons,
        "n_folds": len(folds),
        "fold_step_months": cfg.fold_step_months,
        "results": all_results,
    }

    out_path = os.path.join(args.results_dir, "seasonal_naive_rocv.json")
    with open(out_path, "w") as f:
        json.dump(meta, f, indent=2, default=str)
    logger.info("Results saved to %s", out_path)

    # Print summary table
    print("\n" + "=" * 70)
    print("SEASONAL NAÏVE — ROCV Summary")
    print("=" * 70)
    for h in horizons:
        fold_metrics = [
            v for k, v in all_results.items()
            if v["horizon_days"] == h
        ]
        if fold_metrics:
            rmses = [m["rmse"] for m in fold_metrics]
            pccs = [m["pcc"] for m in fold_metrics]
            print(f"  H={h:3d}d | RMSE={np.mean(rmses):7.1f}±{np.std(rmses):5.1f}  "
                  f"PCC={np.mean(pccs):.4f}±{np.std(pccs):.4f}  "
                  f"({len(fold_metrics)} folds)")
    print("=" * 70)


if __name__ == "__main__":
    main()
