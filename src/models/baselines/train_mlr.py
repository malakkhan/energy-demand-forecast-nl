#!/usr/bin/env python3
"""Dedicated MLR baseline training with explicit 35-feature set and ROCV.

Runs Multiple Linear Regression with:
- 20 hand-picked features + 16 PCA CBS-KNMI full-coverage components (36 total)
- Expanding-window ROCV with validation year merged into training
  (since MLR has no early stopping / hyperparameter tuning)
- Horizon-shifted lag and PCA predictors for strict temporal integrity
- Pre-training safeguards:
    1. Drop columns where >50% of training data is NaN
    2. IQR outlier removal on training data only
    3. Forward-fill → backward-fill → median imputation (no MICE)

ROCV Protocol
-------------
Fold 0:  train = [2012-01-01, 2015-01-01)   origin = 2015-01-01
Fold 1:  train = [2012-01-01, 2016-01-01)   origin = 2016-01-01
  …       (origin advances by ``fold_step`` years each iteration)

For each origin, evaluate at horizons: 60, 75, 90, …, 180 days.

Usage
-----
    # All horizons, all folds:
    python -u src/models/baselines/train_mlr.py

    # Single horizon:
    python -u src/models/baselines/train_mlr.py --horizon 60

    # Quick smoke test (1 fold, 1 horizon):
    python -u src/models/baselines/train_mlr.py --quick
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

# ── Repo path setup ──────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.models.baselines import config as C
from src.models.baselines.data_loader import clear_outliers_iqr, fit_fold_pca
from src.models.baselines.evaluation import (
    compute_metrics,
    plot_metric_evolution,
    print_summary_table,
    save_final_results,
    save_fold_result,
)

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("train_mlr")


# ═══════════════════════════════════════════════════════════════════════
# Feature definitions
# ═══════════════════════════════════════════════════════════════════════

TARGET = "entsoe_load_mw"

# Features that describe the prediction-target timestamp — NOT shifted
# by the forecast horizon (they characterise the time being predicted).
TEMPORAL_FEATURES: List[str] = [
    "day_of_year",
    "week_of_year",
    "quarter",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "month_sin",
    "month_cos",
    "year",
    # Holiday / event flags — tied to the calendar date of the target
    "is_public_holiday",
    "is_school_holiday",
]

# Pre-computed lag features — SHIFTED by horizon_h to avoid data leakage.
# After shifting, e.g. ``load_lag_24h`` at row *t* contains the load from
# ``t − horizon_h − 24`` hours, which is genuinely available at the
# forecast origin ``t − horizon_h``.
LAG_FEATURES: List[str] = [
    "load_lag_12h",
    "load_lag_24h",
    "load_lag_1w",
    "load_lag_1y",
    "solar_rad_lag_10h",
    "humidity_lag_12h",
    "temp_lag_107h",
]

# PCA components derived from CBS economic + KNMI validated weather
# features — SHIFTED by horizon_h (these embed exogenous information).
# Uses the **full-coverage** PCA (2012–2025) that excludes 13 CBS
# tariff columns starting in 2018.  16 components capture ~95% variance.
# PCA_FEATURES removed — PCA is now fitted per-fold on training data.
# Per-fold PCA columns are added dynamically via data_loader.fit_fold_pca().
PCA_FEATURES: List[str] = []  # populated per-fold at runtime

# Union of all features in a fixed, deterministic order.
ALL_FEATURES: List[str] = TEMPORAL_FEATURES + LAG_FEATURES + PCA_FEATURES
# Feature count is dynamic (depends on per-fold PCA n_components)

# ── Pre-training thresholds ──────────────────────────────────────────
_MAX_NAN_FRAC = 0.50  # drop column if >50 % NaN in training set

# ── ROCV defaults ────────────────────────────────────────────────────
ROCV_START = "2012-01-01"
MIN_TRAIN_YEARS = 2       # minimum history before any fold
FOLD_STEP_MONTHS = 13  # 13-month step cycles through calendar months
HORIZONS_DAYS = list(range(60, 181, 15))  # [60, 75, 90, … , 180]


# ═══════════════════════════════════════════════════════════════════════
# 1.  Data loading
# ═══════════════════════════════════════════════════════════════════════

def load_dataset(data_path: Optional[str] = None) -> pd.DataFrame:
    """Load the hourly Parquet dataset with only the required columns.

    The 35 features and the target are all pre-computed in the Parquet,
    so we bypass the generic ``data_loader.load_dataset()`` pipeline
    (which creates merged ``weather_*`` columns and its own calendar
    features) and read exactly what we need.
    """
    path = data_path or str(C.DATA_ROOT)
    logger.info("Loading dataset from %s …", path)

    df = pd.read_parquet(path)

    # ── Ensure timestamp column exists and is datetime ──
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)

    # ── Convert ``year`` from category to int (Parquet partition col) ──
    if "year" in df.columns and df["year"].dtype.name == "category":
        df["year"] = df["year"].astype(int)

    # ── Verify feature availability ──
    available = [c for c in ALL_FEATURES if c in df.columns]
    missing = [c for c in ALL_FEATURES if c not in df.columns]
    if missing:
        logger.warning("Missing %d/%d features: %s",
                        len(missing), len(ALL_FEATURES), missing)

    # ── Select only the columns we need ──
    # Also retain the raw CBS/KNMI covariates: they are not model inputs
    # themselves, but the per-fold PCA (fit_fold_pca, fitted inside each
    # fold on training rows only) needs them present to derive the
    # pca_fold_* components. Without this, the PCA step finds no candidate
    # columns and is silently skipped.
    raw_pca_candidates = [
        c for c in df.columns
        if (c.startswith("cbs_") or c.startswith("knmi_val_"))
        and not c.startswith("pca_")
    ]
    cols_keep = ["timestamp", TARGET] + available + raw_pca_candidates
    df = df[[c for c in cols_keep if c in df.columns]].copy()

    # ── Drop rows where target is NaN ──
    n_before = len(df)
    df = df.dropna(subset=[TARGET])
    n_dropped = n_before - len(df)
    if n_dropped > 0:
        logger.info("Dropped %d rows with NaN target.", n_dropped)

    logger.info(
        "Dataset: %d rows × %d cols  (%s → %s)",
        len(df), len(df.columns),
        df["timestamp"].min().date(), df["timestamp"].max().date(),
    )
    return df


# ═══════════════════════════════════════════════════════════════════════
# 2.  Horizon shifting
# ═══════════════════════════════════════════════════════════════════════

def apply_horizon_shift(
    df: pd.DataFrame,
    horizon_days: int,
) -> pd.DataFrame:
    """Shift lag and PCA features by the forecast horizon.

    **Temporal features** (``day_of_year``, ``hour_sin``, …) are NOT
    shifted — they describe the prediction-target time.

    **Lag features** and **PCA components** ARE shifted by
    ``horizon_days × 24`` hours.  This guarantees that at the forecast
    origin the model only sees genuinely available data.
    """
    horizon_h = horizon_days * 24
    df_shifted = df.copy()

    features_to_shift = [
        c for c in LAG_FEATURES + PCA_FEATURES
        if c in df_shifted.columns
    ]

    for col in features_to_shift:
        df_shifted[col] = df_shifted[col].shift(horizon_h)

    # Drop the leading rows where the shift introduced NaN
    df_shifted = df_shifted.iloc[horizon_h:].reset_index(drop=True)

    logger.info(
        "Horizon shift: %dd (%dh), shifted %d cols, remaining rows = %d.",
        horizon_days, horizon_h, len(features_to_shift), len(df_shifted),
    )
    return df_shifted


# ═══════════════════════════════════════════════════════════════════════
# 3.  ROCV fold generation (validation merged into training)
# ═══════════════════════════════════════════════════════════════════════

def rocv_folds_merged(
    df: pd.DataFrame,
    horizons_days: List[int],
    fold_step: int = FOLD_STEP_MONTHS,
):
    """Generate expanding-window ROCV folds with validation merged
    into training.

    Since MLR has no early stopping, the otherwise-reserved validation
    year is included in the training set.  Consequently the first fold
    requires ``MIN_TRAIN_YEARS + 1`` years of history:

    ========  ====================================  ===================
    Fold      Training window                        Test origin
    ========  ====================================  ===================
    0         [2012-01-01, 2015-01-01)               2015-01-01
    1         [2012-01-01, 2016-01-01)               2016-01-01
    …         expanding …                            …
    ========  ====================================  ===================

    Yields
    ------
    fold_idx : int
    test_origin : pd.Timestamp
        All data before ``test_origin`` is used for training.
    """
    start = pd.Timestamp(ROCV_START)
    data_end = df["timestamp"].max()
    max_horizon_td = pd.Timedelta(days=max(horizons_days))

    # First test origin: start + min_train + 1 year merged validation
    test_origin = start + pd.DateOffset(years=MIN_TRAIN_YEARS + 1)

    fold_idx = 0
    while test_origin + max_horizon_td <= data_end:
        yield fold_idx, test_origin
        test_origin += pd.DateOffset(months=fold_step)
        fold_idx += 1


# ═══════════════════════════════════════════════════════════════════════
# 4.  MLR fold execution
# ═══════════════════════════════════════════════════════════════════════

def run_mlr_fold(
    df_shifted: pd.DataFrame,
    test_origin: pd.Timestamp,
    horizon_days: int,
) -> Dict:
    """Execute one MLR fold.

    Pipeline
    --------
    1. Split: train = [start, origin), test = [origin, origin + horizon)
    2. Drop columns with >50 % NaN in training set
    3. IQR outlier removal on training data only
    4. Forward-fill → backward-fill → median imputation
    5. Fit OLS on training features → predict on test
    6. Compute all seven evaluation metrics
    """
    start = pd.Timestamp(ROCV_START)
    test_end = test_origin + pd.Timedelta(days=horizon_days)
    ts = df_shifted["timestamp"]

    # ── 1. Split ─────────────────────────────────────────────────────
    train_mask = (ts >= start) & (ts < test_origin)
    test_mask = (ts >= test_origin) & (ts < test_end)

    train_df = df_shifted.loc[train_mask].copy()
    test_df = df_shifted.loc[test_mask].copy()

    if len(train_df) == 0 or len(test_df) == 0:
        logger.warning(
            "Empty train (%d) or test (%d) — skipping.",
            len(train_df), len(test_df),
        )
        return {"n_train": len(train_df), "n_test": len(test_df)}

    # ── Identify numeric feature columns actually present ────────────
    feature_cols = [
        c for c in ALL_FEATURES
        if c in train_df.columns
        and train_df[c].dtype in (np.float64, np.float32, np.int64, np.int32)
    ]

    # ── 2. Drop columns >50 % NaN in training set ───────────────────
    nan_fracs = train_df[feature_cols].isna().mean()
    usable = nan_fracs[nan_fracs <= _MAX_NAN_FRAC].index.tolist()
    dropped = len(feature_cols) - len(usable)
    if dropped > 0:
        logger.info(
            "Dropped %d/%d cols (>%.0f%% NaN): %s",
            dropped, len(feature_cols), _MAX_NAN_FRAC * 100,
            sorted(set(feature_cols) - set(usable)),
        )
    feature_cols = usable

    if len(feature_cols) == 0:
        logger.warning("No usable feature columns remain after NaN filter.")
        return {"n_train": len(train_df), "n_test": len(test_df)}

    # ── 3. IQR outlier removal (train only, usable cols + target) ────
    iqr_cols = feature_cols + [TARGET]
    train_df = clear_outliers_iqr(train_df, iqr_cols)

    # ── 4. Fast imputation: ffill → bfill → median ──────────────────
    for col in feature_cols:
        # Train set
        train_df[col] = train_df[col].ffill().bfill()
        med = train_df[col].median()
        fill_val = med if np.isfinite(med) else 0.0
        train_df[col] = train_df[col].fillna(fill_val)

        # Test set — use the training median as the fallback value
        test_df[col] = test_df[col].ffill().bfill()
        test_df[col] = test_df[col].fillna(fill_val)

    # Ensure target has no NaN
    train_df = train_df.dropna(subset=[TARGET])
    test_df = test_df.dropna(subset=[TARGET])

    if len(train_df) == 0 or len(test_df) == 0:
        logger.warning(
            "Empty after imputation: train=%d, test=%d — skipping.",
            len(train_df), len(test_df),
        )
        return {"n_train": len(train_df), "n_test": len(test_df)}

    logger.info(
        "MLR fitting: %d features × %d train rows → %d test rows.",
        len(feature_cols), len(train_df), len(test_df),
    )

    # ── 4b. Per-fold PCA (fitted on training data only) ───────────
    pca_cols, train_df, _, test_df = fit_fold_pca(train_df, None, test_df)
    if pca_cols:
        feature_cols = feature_cols + pca_cols
        # PCA leaves NaN components on rows whose raw covariates are
        # incomplete (imputation runs before PCA here); fill causally
        # within each split, mirroring the CMAT pipeline.
        for _split in (train_df, test_df):
            _split[pca_cols] = _split[pca_cols].ffill().bfill()
        logger.info("Added %d per-fold PCA cols. Total features: %d.", len(pca_cols), len(feature_cols))

    # ── 5. Fit ───────────────────────────────────────────────────────
    X_train = train_df[feature_cols].values
    y_train = train_df[TARGET].values

    model = LinearRegression()
    t_train_start = time.time()
    model.fit(X_train, y_train)
    train_time_s = time.time() - t_train_start

    # ── 6. Predict ───────────────────────────────────────────────────
    X_test = test_df[feature_cols].values
    t_infer_start = time.time()
    y_pred = model.predict(X_test)
    infer_time_s = time.time() - t_infer_start
    y_true = test_df[TARGET].values

    # ── 7. Evaluate ──────────────────────────────────────────────────
    metrics = compute_metrics(y_true, y_pred)
    test_timestamps = (
        test_df["timestamp"].values
        if "timestamp" in test_df.columns
        else None
    )

    return {
        "predictions_mw": y_pred,
        "actuals_mw": y_true,
        "timestamps": test_timestamps,
        "n_train": len(train_df),
        "n_test": len(test_df),
        "train_time_s": round(train_time_s, 2),
        "infer_time_s": round(infer_time_s, 2),
        "metrics": metrics,
        "model": model,
        "n_features": len(feature_cols),
        "feature_cols": feature_cols,
    }


# ═══════════════════════════════════════════════════════════════════════
# 5.  Main ROCV loop
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Train MLR baseline with explicit 35-feature set via ROCV.",
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
        "--horizon", type=int, default=None,
        help="Run only a single horizon (e.g. 60). Default: all.",
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="Quick test: single fold, single horizon.",
    )
    parser.add_argument(
        "--fold-step", type=int, default=FOLD_STEP_MONTHS,
        help=f"Override fold_step_months (default: {FOLD_STEP_MONTHS}).",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else C.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    model_name = "van_de_sande_mlr"

    logger.info("=" * 70)
    logger.info("  MLR Baseline — Explicit 35-Feature ROCV")
    logger.info("  Started:    %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("  Output dir: %s", output_dir)
    logger.info("=" * 70)

    # ── Load data ────────────────────────────────────────────────────
    df = load_dataset(args.data_path)

    # ── Horizons ─────────────────────────────────────────────────────
    horizons = [args.horizon] if args.horizon else HORIZONS_DAYS
    if args.quick:
        horizons = [horizons[0]]

    logger.info("Horizons: %s days", horizons)
    logger.info("Fold step: %d month(s)", args.fold_step)
    logger.info(
        "Features: %d total  (%d temporal + %d lag + %d PCA)",
        len(ALL_FEATURES), len(TEMPORAL_FEATURES),
        len(LAG_FEATURES), len(PCA_FEATURES),
    )

    results: List[Dict] = []

    for h_days in horizons:
        h_t0 = time.time()
        logger.info("━" * 60)
        logger.info("HORIZON = %d days (%d hours)", h_days, h_days * 24)
        logger.info("━" * 60)

        # Apply horizon shift for this specific horizon
        df_shifted = apply_horizon_shift(df, h_days)

        # Run each ROCV fold
        for fold_idx, test_origin in rocv_folds_merged(
            df_shifted, horizons, fold_step=args.fold_step,
        ):
            fold_t0 = time.time()
            logger.info(
                "  FOLD %d — origin=%s, horizon=%dd",
                fold_idx, test_origin.date(), h_days,
            )

            fold_result = run_mlr_fold(df_shifted, test_origin, h_days)

            metrics = fold_result.get("metrics")
            fold_elapsed = time.time() - fold_t0

            if metrics is not None:
                result_row = {
                    "model": model_name,
                    "fold": fold_idx,
                    "cutoff": str(test_origin.date()),
                    "horizon_days": h_days,
                    "horizon_hours": h_days * 24,
                    "n_train": fold_result.get("n_train", 0),
                    "n_test": fold_result.get("n_test", 0),
                    "train_time_s": fold_result.get("train_time_s", 0.0),
                    "infer_time_s": fold_result.get("infer_time_s", 0.0),
                    "seed": "N/A",
                    **metrics.to_dict(),
                    "n_features": fold_result.get("n_features", 0),
                    "elapsed_s": round(fold_elapsed, 1),
                }
                save_fold_result(result_row, results, output_dir, model_name)

                logger.info(
                    "    H=%3dd | RMSE=%7.1f  MAE=%7.1f  "
                    "MAPE=%.2f%%  MASE=%.3f  nMAE=%.4f  nRMSE=%.4f  "
                    "PCC=%.4f | %d pts  (%d feats)  (%.1fs)",
                    h_days,
                    metrics.rmse, metrics.mae,
                    metrics.mape_pct, metrics.mase,
                    metrics.nmae, metrics.nrmse,
                    metrics.pcc, metrics.n_samples,
                    fold_result.get("n_features", 0),
                    fold_elapsed,
                )
            else:
                logger.warning(
                    "    Fold %d skipped — no metrics returned.",
                    fold_idx,
                )

            if args.quick:
                logger.info("    --quick mode: stopping after first fold.")
                break

        h_elapsed = time.time() - h_t0
        logger.info(
            "  Horizon %d days complete in %.0fs.", h_days, h_elapsed,
        )

    # ── Save final results ───────────────────────────────────────────
    results_df = save_final_results(results, output_dir, model_name)
    print_summary_table(results_df, model_name)

    # Generate metric evolution plots
    try:
        plot_metric_evolution(results_df, model_name, output_dir)
    except Exception as plot_err:
        logger.warning("Plotting failed: %s", plot_err)

    # ── Run metadata ─────────────────────────────────────────────────
    meta = {
        "script": "train_mlr.py",
        "started": datetime.now().isoformat(),
        "model": model_name,
        "features": ALL_FEATURES,
        "n_features": len(ALL_FEATURES),
        "feature_groups": {
            "temporal": TEMPORAL_FEATURES,
            "lag": LAG_FEATURES,
            "pca": PCA_FEATURES,
        },
        "rocv": {
            "start_date": ROCV_START,
            "min_train_years": MIN_TRAIN_YEARS,
            "fold_step_months": args.fold_step,
            "horizons_days": horizons,
            "validation_merged_into_training": True,
        },
        "preprocessing": {
            "max_nan_frac": _MAX_NAN_FRAC,
            "imputation": "ffill → bfill → median (no MICE)",
            "outlier_removal": "IQR 1.5× (train only)",
        },
        "total_elapsed_s": round(time.time() - t0, 1),
    }
    meta_path = output_dir / "run_metadata_mlr.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    logger.info("Metadata → %s", meta_path)

    total = time.time() - t0
    logger.info("\n" + "=" * 70)
    logger.info("  DONE — %.0fs (%.1f min)", total, total / 60)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
