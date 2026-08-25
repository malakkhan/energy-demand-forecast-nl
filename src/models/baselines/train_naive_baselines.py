#!/usr/bin/env python3
"""Naïve and Drift baselines for energy load forecasting.

Naïve (24h persistence):
    ŷ(t) = y(t − 24h)
    Each hour's prediction is the load from the same hour yesterday.
    The last 24h before the test origin are tiled across the full horizon.

Drift:
    ŷ(t) = y(T) + ((t − T) / n) × (y(T) − y(1))
    Linear extrapolation using the average historical change per timestep.
    T = last training timestamp, y(1) = first training timestamp.

Both use the same ROCV protocol as other baselines.
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
from src.models.baselines.config import ROCVConfig

# ── Constants ────────────────────────────────────────────────────────
TARGET = "entsoe_load_mw"
ROCV_START = "2012-01-01"
MIN_TRAIN_YEARS = 2
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
# ROCV fold generation
# ═════════════════════════════════════════════════════════════════════

def rocv_folds(df: pd.DataFrame, cfg: ROCVConfig):
    """Generate expanding-window ROCV folds. Yields (fold_idx, test_origin)."""
    start = pd.Timestamp(ROCV_START)
    data_end = df["timestamp"].max()
    max_horizon_td = pd.Timedelta(days=max(HORIZONS_DAYS))

    test_origin = start + pd.DateOffset(years=MIN_TRAIN_YEARS + 1)
    fold_idx = 0
    while test_origin + max_horizon_td <= data_end:
        yield fold_idx, test_origin
        test_origin += pd.DateOffset(months=cfg.fold_step_months)
        fold_idx += 1


# ═════════════════════════════════════════════════════════════════════
# Naïve baseline (24h persistence)
# ═════════════════════════════════════════════════════════════════════

def run_naive_fold(
    df: pd.DataFrame,
    test_origin: pd.Timestamp,
    horizon_days: int,
) -> Dict:
    """Naïve 24h persistence: tile the last 24 hours across the horizon.

    ŷ(t) = y(t − 24h), using the 24 hours immediately before test_origin,
    repeated cyclically for the entire test window.
    """
    t0 = time.time()

    horizon_td = pd.Timedelta(days=horizon_days)
    test_mask = (
        (df["timestamp"] >= test_origin) &
        (df["timestamp"] < test_origin + horizon_td)
    )
    test_df = df.loc[test_mask].copy()

    if len(test_df) == 0:
        return {"n_test": 0}

    # Get the last 24 hours before test_origin
    seed_start = test_origin - pd.Timedelta(hours=24)
    seed_mask = (
        (df["timestamp"] >= seed_start) &
        (df["timestamp"] < test_origin)
    )
    seed_df = df.loc[seed_mask]

    if len(seed_df) == 0:
        return {"n_test": 0}

    seed_values = seed_df[TARGET].values  # 24 values (one per hour)
    n_seed = len(seed_values)

    y_true = test_df[TARGET].values
    # Tile the 24h seed cyclically
    n_test = len(y_true)
    y_pred = np.tile(seed_values,
        "train_time_s": round(train_time_s, 2),
        "infer_time_s": round(infer_time_s, 2), (n_test // n_seed) + 1)[:n_test]

    valid = np.isfinite(y_true) & np.isfinite(y_pred)
    n_valid = valid.sum()
    if n_valid < 10:
        return {"n_test": n_valid}

    metrics = compute_metrics(y_true[valid],
        "train_time_s": round(train_time_s, 2),
        "infer_time_s": round(infer_time_s, 2), y_pred[valid])
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
# Drift baseline
# ═════════════════════════════════════════════════════════════════════

def run_drift_fold(
    df: pd.DataFrame,
    test_origin: pd.Timestamp,
    horizon_days: int,
) -> Dict:
    """Drift: linear extrapolation from historical trend.

    ŷ(T + h) = y(T) + (h / n) × (y(T) − y(1))

    where T = last training observation, y(1) = first training observation,
    n = number of training timesteps, h = steps ahead.
    """
    t0 = time.time()

    horizon_td = pd.Timedelta(days=horizon_days)

    # Training data: everything before test_origin
    train_mask = df["timestamp"] < test_origin
    train_df = df.loc[train_mask]

    # Test data
    test_mask = (
        (df["timestamp"] >= test_origin) &
        (df["timestamp"] < test_origin + horizon_td)
    )
    test_df = df.loc[test_mask].copy()

    if len(test_df) == 0 or len(train_df) < 2:
        return {"n_test": 0}

    # Drift parameters
    y_first = train_df[TARGET].iloc[0]    # y(1)
    y_last = train_df[TARGET].iloc[-1]    # y(T)
    n_train = len(train_df)               # n

    drift_per_step = (y_last - y_first) / n_train

    # Predictions
    y_true = test_df[TARGET].values
    n_test = len(y_true)
    h_steps = np.arange(1,
        "train_time_s": round(train_time_s, 2),
        "infer_time_s": round(infer_time_s, 2), n_test + 1)
    y_pred = y_last + drift_per_step * h_steps

    valid = np.isfinite(y_true) & np.isfinite(y_pred)
    n_valid = valid.sum()
    if n_valid < 10:
        return {"n_test": n_valid}

    metrics = compute_metrics(y_true[valid],
        "train_time_s": round(train_time_s, 2),
        "infer_time_s": round(infer_time_s, 2), y_pred[valid])
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

def _run_model(model_name, fold_fn, df, folds, horizons):
    """Run one model across all folds and horizons."""
    all_results = {}
    for fold_idx, test_origin in folds:
        for h in horizons:
            result = fold_fn(df, test_origin, h)
            if "metrics" in result:
                m = result["metrics"]
                logger.info(
                    "%s | Fold %2d | H=%3dd | RMSE=%7.1f  MAE=%7.1f  "
                    "MAPE=%.2f%%  PCC=%.4f | %d pts",
                    model_name, fold_idx, h, m.rmse, m.mae,
                    m.mape_pct, m.pcc, result["n_test"],
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
                    "seed": "N/A",
        "train_time_s": round(train_time_s, 2),
        "infer_time_s": round(infer_time_s, 2),
                    **m.to_dict(),
                }
    return all_results


def _print_summary(model_name, all_results, horizons):
    """Print summary table."""
    print(f"\n{'=' * 70}")
    print(f"{model_name} — ROCV Summary")
    print(f"{'=' * 70}")
    for h in horizons:
        fold_metrics = [v for v in all_results.values() if v["horizon_days"] == h]
        if fold_metrics:
            rmses = [m["rmse"] for m in fold_metrics]
            pccs = [m["pcc"] for m in fold_metrics]
            print(f"  H={h:3d}d | RMSE={np.mean(rmses):7.1f}±{np.std(rmses):5.1f}  "
                  f"PCC={np.mean(pccs):.4f}±{np.std(pccs):.4f}  "
                  f"({len(fold_metrics)} folds)")
    print(f"{'=' * 70}")


def main():
    parser = argparse.ArgumentParser(
        description="Naïve and Drift baselines."
    )
    parser.add_argument("--data-path", type=str, required=True)
    parser.add_argument(
        "--results-dir", type=str,
        default=os.path.join(os.path.dirname(__file__), "results"),
    )
    parser.add_argument("--horizons", type=int, nargs="+", default=HORIZONS_DAYS)
    parser.add_argument("--fold-step", type=int, default=None)
    parser.add_argument(
        "--model", type=str, choices=["naive", "drift", "both"],
        default="both", help="Which baseline to run.",
    )
    args = parser.parse_args()

    df = load_dataset(args.data_path)
    cfg = ROCVConfig()
    if args.fold_step is not None:
        cfg.fold_step_months = args.fold_step

    folds = list(rocv_folds(df, cfg))
    logger.info("Total folds: %d", len(folds))

    models = {
        "naive": ("Naïve (24h persistence)", run_naive_fold, "naive_24h"),
        "drift": ("Drift", run_drift_fold, "drift"),
    }

    to_run = list(models.keys()) if args.model == "both" else [args.model]

    for model_key in to_run:
        model_name, fold_fn, dir_name = models[model_key]
        out_dir = os.path.join(args.results_dir, dir_name)
        os.makedirs(out_dir, exist_ok=True)

        results = _run_model(model_name, fold_fn, df, folds, args.horizons)

        meta = {
            "model": model_name,
            "horizons_days": args.horizons,
            "n_folds": len(folds),
            "fold_step_months": cfg.fold_step_months,
            "results": results,
        }

        out_path = os.path.join(out_dir, f"{dir_name}_rocv.json")
        with open(out_path, "w") as f:
            json.dump(meta, f, indent=2)
        logger.info("Results saved to %s", out_path)

        _print_summary(model_name, results, args.horizons)


if __name__ == "__main__":
    main()
