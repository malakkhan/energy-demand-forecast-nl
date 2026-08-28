#!/usr/bin/env python3
"""Dedicated SARIMAX-LSTM baseline training with explicit 36-feature set and ROCV.

Two-stage hybrid model:
    Stage 1 — SARIMAX(1,0,1)(1,0,1,168): fitted on training data with
        36 features as exogenous variables.  Training capped at last 2 years
        for computational tractability.
    Stage 2 — Residual LSTM: PyTorch LSTM(50) with 720-hour sliding window
        learns to predict SARIMAX residuals.  Early stopping on 1-year
        validation holdout.
    Inference: final = SARIMAX_forecast + LSTM_residual_correction.

Uses the same 36-feature set, ROCV protocol, and preprocessing as MLR:
- 20 hand-picked features + 16 PCA CBS-KNMI full-coverage components
- IQR outlier removal, ffill/bfill/median imputation
- Horizon-shifted lag and PCA predictors

ROCV Protocol (with validation holdout)
-----------------------------------------
Fold 0:  train=[2012,2014)  val=[2014,2015)  test_origin=2015-01-01
Fold 1:  train=[2012,2015)  val=[2015,2016)  test_origin=2016-01-01
  …

Usage
-----
    python -u src/models/baselines/train_sarimax_lstm_v2.py
    python -u src/models/baselines/train_sarimax_lstm_v2.py --quick
"""

import argparse
import json
import logging
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader, TensorDataset

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
    save_fold_result,
)

logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("train_sarimax_lstm_v2")

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# ═══════════════════════════════════════════════════════════════════════
# Feature definitions (identical to train_mlr.py)
# ═══════════════════════════════════════════════════════════════════════

TARGET = "entsoe_load_mw"

TEMPORAL_FEATURES: List[str] = [
    "day_of_year", "week_of_year", "quarter",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "month_sin", "month_cos",
    "year",
    "is_public_holiday", "is_school_holiday",
]
LAG_FEATURES: List[str] = [
    "load_lag_12h", "load_lag_24h", "load_lag_1w", "load_lag_1y",
    "solar_rad_lag_10h", "humidity_lag_12h", "temp_lag_107h",
]
PCA_FEATURES: List[str] = []  # populated per-fold at runtime (fit_fold_pca)

ALL_FEATURES: List[str] = TEMPORAL_FEATURES + LAG_FEATURES + PCA_FEATURES

_MAX_NAN_FRAC = 0.50
ROCV_START = "2012-01-01"
MIN_TRAIN_YEARS = 2
FOLD_STEP_MONTHS = 13  # 13-month step cycles through calendar months
HORIZONS_DAYS = list(range(60, 181, 15))

# SARIMAX caps
MAX_SARIMAX_TRAIN_HOURS = 24 * 365 * 2  # 2 years


# ═══════════════════════════════════════════════════════════════════════
# Data loading / shifting / ROCV (shared with Prophet-LSTM)
# ═══════════════════════════════════════════════════════════════════════

def load_dataset(data_path: Optional[str] = None) -> pd.DataFrame:
    path = data_path or str(C.DATA_ROOT)
    logger.info("Loading dataset from %s …", path)
    df = pd.read_parquet(path)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)
    if "year" in df.columns and df["year"].dtype.name == "category":
        df["year"] = df["year"].astype(int)
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
    cols_keep = ["timestamp", TARGET] + [c for c in ALL_FEATURES if c in df.columns] + raw_pca_candidates
    df = df[[c for c in cols_keep if c in df.columns]].copy()
    n_before = len(df)
    df = df.dropna(subset=[TARGET])
    if n_before - len(df) > 0:
        logger.info("Dropped %d rows with NaN target.", n_before - len(df))
    logger.info("Dataset: %d rows × %d cols  (%s → %s)",
                len(df), len(df.columns),
                df["timestamp"].min().date(), df["timestamp"].max().date())
    return df


def apply_horizon_shift(df: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    horizon_h = horizon_days * 24
    df_shifted = df.copy()
    features_to_shift = [c for c in LAG_FEATURES + PCA_FEATURES if c in df_shifted.columns]
    for col in features_to_shift:
        df_shifted[col] = df_shifted[col].shift(horizon_h)
    df_shifted = df_shifted.iloc[horizon_h:].reset_index(drop=True)
    logger.info("Horizon shift: %dd (%dh), shifted %d cols, remaining rows = %d.",
                horizon_days, horizon_h, len(features_to_shift), len(df_shifted))
    return df_shifted


def rocv_folds_with_val(df, horizons_days, fold_step=FOLD_STEP_MONTHS):
    start = pd.Timestamp(ROCV_START)
    data_end = df["timestamp"].max()
    max_horizon_td = pd.Timedelta(days=max(horizons_days))
    train_end = start + pd.DateOffset(years=MIN_TRAIN_YEARS)
    val_end = train_end + pd.DateOffset(years=1)
    test_origin = val_end
    fold_idx = 0
    while test_origin + max_horizon_td <= data_end:
        yield fold_idx, train_end, val_end, test_origin
        train_end += pd.DateOffset(months=fold_step)
        val_end = train_end + pd.DateOffset(years=1)
        test_origin = val_end
        fold_idx += 1


# ═══════════════════════════════════════════════════════════════════════
# Residual LSTM
# ═══════════════════════════════════════════════════════════════════════

class ResidualLSTM(nn.Module):
    """LSTM(50, ReLU) → Linear(1) for residual modelling."""
    def __init__(self, n_features: int, hidden_units: int = 50):
        super().__init__()
        self.lstm = nn.LSTM(input_size=n_features, hidden_size=hidden_units, batch_first=True)
        self.relu = nn.ReLU()
        self.fc = nn.Linear(hidden_units, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        out = self.relu(out[:, -1, :])
        return self.fc(out).squeeze(-1)


# ═══════════════════════════════════════════════════════════════════════
# SARIMAX-LSTM fold execution
# ═══════════════════════════════════════════════════════════════════════

def run_sarimax_lstm_fold(
    df_shifted: pd.DataFrame,
    train_end: pd.Timestamp,
    val_end: pd.Timestamp,
    test_origin: pd.Timestamp,
    horizon_days: int,
    seed: int = 42,
) -> Dict:
    """Execute one SARIMAX-LSTM fold."""
    from statsmodels.tsa.statespace.sarimax import SARIMAX

    sarimax_cfg = C.SARIMAXConfig()
    lstm_cfg = C.SARIMAXLSTMConfig()
    start = pd.Timestamp(ROCV_START)
    test_end = test_origin + pd.Timedelta(days=horizon_days)
    ts = df_shifted["timestamp"]

    # ── 1. Split ─────────────────────────────────────────────────────
    train_mask = (ts >= start) & (ts < train_end)
    val_mask = (ts >= train_end) & (ts < val_end)
    test_mask = (ts >= test_origin) & (ts < test_end)

    train_df = df_shifted.loc[train_mask].copy()
    val_df = df_shifted.loc[val_mask].copy()
    test_df = df_shifted.loc[test_mask].copy()

    if len(train_df) == 0 or len(test_df) == 0:
        logger.warning("Empty train (%d) or test (%d).", len(train_df), len(test_df))
        return {"n_train": len(train_df), "n_test": len(test_df)}

    # ── 2. Feature filtering ─────────────────────────────────────────
    feature_cols = [
        c for c in ALL_FEATURES
        if c in train_df.columns
        and train_df[c].dtype in (np.float64, np.float32, np.int64, np.int32)
    ]
    nan_fracs = train_df[feature_cols].isna().mean()
    usable = nan_fracs[nan_fracs <= _MAX_NAN_FRAC].index.tolist()
    dropped = len(feature_cols) - len(usable)
    if dropped > 0:
        logger.info("Dropped %d/%d cols (>%.0f%% NaN).", dropped, len(feature_cols), _MAX_NAN_FRAC * 100)
    feature_cols = usable

    # ── 3. IQR + imputation ──────────────────────────────────────────
    iqr_cols = feature_cols + [TARGET]
    train_df = clear_outliers_iqr(train_df, iqr_cols)

    for col in feature_cols:
        train_df[col] = train_df[col].ffill().bfill()
        med = train_df[col].median()
        fill_val = med if np.isfinite(med) else 0.0
        train_df[col] = train_df[col].fillna(fill_val)
        for part_df in [val_df, test_df]:
            if col in part_df.columns:
                part_df[col] = part_df[col].ffill().bfill().fillna(fill_val)

    train_df = train_df.dropna(subset=[TARGET])
    val_df = val_df.dropna(subset=[TARGET])
    test_df = test_df.dropna(subset=[TARGET])

    if len(train_df) == 0 or len(test_df) == 0:
        return {"n_train": len(train_df), "n_test": len(test_df)}

    # ══════════════════════════════════════════════════════════════════
    # Per-fold PCA (fitted on training data only)
    pca_cols, train_df, val_df, test_df = fit_fold_pca(train_df, val_df, test_df)
    if pca_cols:
        feature_cols = feature_cols + pca_cols
        # PCA leaves NaN components on rows whose raw covariates are
        # incomplete (imputation runs before PCA here); fill causally
        # within each split, mirroring the CMAT pipeline.
        for _split in (train_df, val_df, test_df):
            _split[pca_cols] = _split[pca_cols].ffill().bfill()
        logger.info("Added %d per-fold PCA cols. Total features: %d.", len(pca_cols), len(feature_cols))

    # Stage 1: SARIMAX
    # ══════════════════════════════════════════════════════════════════

    # Cap training window for SARIMAX
    if len(train_df) > MAX_SARIMAX_TRAIN_HOURS:
        sarimax_train = train_df.iloc[-MAX_SARIMAX_TRAIN_HOURS:].copy()
        logger.info("Capped SARIMAX training to last %d hours.", MAX_SARIMAX_TRAIN_HOURS)
    else:
        sarimax_train = train_df.copy()

    exog_train = sarimax_train[feature_cols].values.astype(np.float64) if feature_cols else None
    exog_test = test_df[feature_cols].values.astype(np.float64) if feature_cols else None

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model = SARIMAX(
                endog=sarimax_train[TARGET].values,
                exog=exog_train,
                order=sarimax_cfg.order,
                seasonal_order=sarimax_cfg.seasonal_order,
                enforce_stationarity=sarimax_cfg.enforce_stationarity,
                enforce_invertibility=sarimax_cfg.enforce_invertibility,
            )
            result = model.fit(disp=False, maxiter=sarimax_cfg.maxiter, method="lbfgs")
        logger.info("SARIMAX fitted. AIC=%.1f", result.aic)
    except Exception as e:
        logger.error("SARIMAX fitting failed: %s", e)
        return {"n_train": len(train_df), "n_test": len(test_df), "error": str(e)}

    # SARIMAX forecast for test
    n_test = len(test_df)
    try:
        forecast = result.forecast(steps=n_test, exog=exog_test)
        sarimax_pred_test = np.asarray(forecast, dtype=np.float64)
    except Exception as e:
        logger.error("SARIMAX forecast failed: %s", e)
        return {"n_train": len(train_df), "n_test": n_test, "error": str(e)}

    # In-sample residuals
    train_residuals = result.resid.astype(np.float64)
    y_true_test = test_df[TARGET].values

    # ══════════════════════════════════════════════════════════════════
    # Stage 2: Residual LSTM
    # ══════════════════════════════════════════════════════════════════

    seq_len = lstm_cfg.input_sequence_length  # 720 hours

    if len(train_residuals) < seq_len + 100:
        logger.warning("Not enough residuals (%d) for LSTM. Using SARIMAX-only.", len(train_residuals))
        metrics = compute_metrics(y_true_test, sarimax_pred_test)
        return {
            "predictions_mw": sarimax_pred_test,
            "actuals_mw": y_true_test,
            "n_train": len(train_df), "n_test": n_test,
            "metrics": metrics, "n_features": len(feature_cols),
        }

    # Scale residuals
    residuals_2d = train_residuals.reshape(-1, 1)
    scaler = MinMaxScaler(feature_range=(0, 1))
    residuals_scaled = scaler.fit_transform(residuals_2d).astype(np.float32)

    # Create sequences
    n_seq = len(residuals_scaled) - seq_len
    if n_seq <= 0:
        metrics = compute_metrics(y_true_test, sarimax_pred_test)
        return {
            "predictions_mw": sarimax_pred_test, "actuals_mw": y_true_test,
            "n_train": len(train_df), "n_test": n_test,
            "metrics": metrics, "n_features": len(feature_cols),
        }

    X_seq = np.empty((n_seq, seq_len, 1), dtype=np.float32)
    y_seq = np.empty(n_seq, dtype=np.float32)
    for i in range(n_seq):
        X_seq[i] = residuals_scaled[i:i + seq_len]
        y_seq[i] = residuals_scaled[i + seq_len, 0]

    # Train/val split for LSTM: use last year-equivalent as validation
    val_size = min(8760, len(X_seq) // 5)  # 1 year or 20%
    split_idx = len(X_seq) - val_size

    X_train_t = torch.from_numpy(X_seq[:split_idx]).to(DEVICE)
    y_train_t = torch.from_numpy(y_seq[:split_idx]).to(DEVICE)
    X_val_t = torch.from_numpy(X_seq[split_idx:]).to(DEVICE)
    y_val_t = torch.from_numpy(y_seq[split_idx:]).to(DEVICE)

    model_lstm = ResidualLSTM(n_features=1, hidden_units=lstm_cfg.lstm_units).to(DEVICE)
    optimizer = torch.optim.Adam(model_lstm.parameters(), lr=lstm_cfg.learning_rate)
    criterion = nn.MSELoss()

    train_loader = DataLoader(
        TensorDataset(X_train_t, y_train_t),
        batch_size=lstm_cfg.batch_size, shuffle=False,
    )

    best_val_loss = float("inf")
    best_state = None
    patience_counter = 0

    for epoch in range(lstm_cfg.epochs):
        model_lstm.train()
        for xb, yb in train_loader:
            pred = model_lstm(xb)
            loss = criterion(pred, yb)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        model_lstm.eval()
        with torch.no_grad():
            val_pred = model_lstm(X_val_t)
            val_loss = criterion(val_pred, y_val_t).item()

        if epoch % 10 == 0:
            logger.info("  LSTM epoch %d/%d — val_loss=%.6f", epoch, lstm_cfg.epochs, val_loss)

        if val_loss < best_val_loss - 1e-5:
            best_val_loss = val_loss
            best_state = {k: v.cpu().clone() for k, v in model_lstm.state_dict().items()}
            patience_counter = 0
        else:
            patience_counter += 1
            if patience_counter >= 5:
                logger.info("  LSTM early stopping at epoch %d.", epoch)
                break

    if best_state is not None:
        model_lstm.load_state_dict(best_state)
    model_lstm.eval()

    # ── Inference: predict residual corrections ──────────────────────
    test_residuals_pred = np.zeros(n_test, dtype=np.float64)
    seed = residuals_scaled[-seq_len:].copy()

    with torch.no_grad():
        for t in range(n_test):
            inp = torch.from_numpy(seed.reshape(1, seq_len, 1)).to(DEVICE)
            r_scaled = model_lstm(inp).cpu().numpy().item()
            r_mw = scaler.inverse_transform([[r_scaled]])[0, 0]
            test_residuals_pred[t] = r_mw
            seed = np.roll(seed, -1, axis=0)
            seed[-1, 0] = r_scaled

    final_pred = sarimax_pred_test + test_residuals_pred
    metrics = compute_metrics(y_true_test, final_pred)

    return {
        "predictions_mw": final_pred,
        "actuals_mw": y_true_test,
        "timestamps": test_df["timestamp"].values if "timestamp" in test_df.columns else None,
        "n_train": len(train_df),
        "n_test": n_test,
        "metrics": metrics,
        "n_features": len(feature_cols),
    }


# ═══════════════════════════════════════════════════════════════════════
# Parallel-safe fold persistence
# ═══════════════════════════════════════════════════════════════════════

def save_fold_result_locked(
    result_row: Dict,
    results_list: List[Dict],
    output_dir: Path,
    model_name: str,
) -> None:
    """Append a fold result to this task's own shard file.

    Each (model, horizon, seed) combination owns an exclusive shard file
    under ``_shards/``, so concurrent SLURM array tasks (possibly on
    different nodes) never contend for the same path. This avoids
    relying on fcntl.flock() for cross-node mutual exclusion, which is
    not reliably honoured on this GPFS-mounted scratch filesystem and
    previously caused silent data loss when multiple tasks wrote a
    shared CSV concurrently. Shards are combined into the final
    rocv_results_{model_name}.csv via merge_shards.py.
    """
    results_list.append(result_row)

    shard_dir = output_dir / "_shards"
    shard_dir.mkdir(parents=True, exist_ok=True)
    h = int(result_row["horizon_days"])
    seed = int(result_row.get("seed", 42))
    shard_path = shard_dir / f"{model_name}_h{h}_s{seed}.csv"
    pd.DataFrame(results_list).to_csv(shard_path, index=False)


# ═══════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Train SARIMAX-LSTM with 36-feature ROCV.")
    parser.add_argument("--data-path", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument("--horizon", type=int, default=None)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from existing results, skipping completed folds. "
                             "Safe for parallel per-horizon SLURM jobs.")
    parser.add_argument("--fold-step", type=int, default=FOLD_STEP_MONTHS)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else C.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    model_name = "ashtar_sarimax_lstm"

    logger.info("=" * 70)
    logger.info("  SARIMAX-LSTM Baseline — 36-Feature ROCV")
    logger.info("  Device:     %s", DEVICE)
    logger.info("  Started:    %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("  Output dir: %s", output_dir)
    logger.info("=" * 70)

    df = load_dataset(args.data_path)

    horizons = [args.horizon] if args.horizon else HORIZONS_DAYS
    if args.quick:
        horizons = [horizons[0]]

    logger.info("Horizons: %s days", horizons)
    logger.info("Features: %d total", len(ALL_FEATURES))

    csv_path = output_dir / f"rocv_results_{model_name}.csv"

    import torch, random, numpy
    torch.manual_seed(args.seed)
    random.seed(args.seed)
    numpy.random.seed(args.seed)
    logger.info("Seed: %d", args.seed)

    all_results: List[Dict] = []
    for h_days in horizons:
        h_t0 = time.time()
        logger.info("━" * 60)
        logger.info("HORIZON = %d days (%d hours)", h_days, h_days * 24)
        logger.info("━" * 60)

        # Per-(horizon, seed) shard — exclusive to this task, no cross-node
        # locking needed. Resume reads only this task's own shard.
        shard_dir = output_dir / "_shards"
        shard_path = shard_dir / f"{model_name}_h{h_days}_s{args.seed}.csv"
        results: List[Dict] = []
        completed: set = set()
        if args.resume and shard_path.exists():
            existing_df = pd.read_csv(shard_path)
            results = existing_df.to_dict("records")
            completed = set(int(f) for f in existing_df["fold"])
            logger.info("Resumed: loaded %d existing results from shard (h=%d, seed=%d).",
                         len(results), h_days, args.seed)

        df_shifted = apply_horizon_shift(df, h_days)

        for fold_idx, train_end, val_end, test_origin in rocv_folds_with_val(
            df_shifted, horizons, fold_step=args.fold_step,
        ):
            fold_t0 = time.time()
            logger.info("  FOLD %d — train=[..,%s)  val=[%s,%s)  test=%s  horizon=%dd",
                        fold_idx, train_end.date(), train_end.date(),
                        val_end.date(), test_origin.date(), h_days)

            if fold_idx in completed:
                logger.info("    Already complete (seed=%d) — skipping.", args.seed)
                continue

            fold_result = run_sarimax_lstm_fold(
                df_shifted, train_end, val_end, test_origin, h_days,
            )

            metrics = fold_result.get("metrics")
            fold_elapsed = time.time() - fold_t0

            if metrics is not None:
                result_row = {
                    "model": model_name,
                    "fold": fold_idx,
                    "seed": args.seed,
                    "cutoff": str(test_origin.date()),
                    "horizon_days": h_days,
                    "horizon_hours": h_days * 24,
                    "n_train": fold_result.get("n_train", 0),
                    "n_test": fold_result.get("n_test", 0),
                    **metrics.to_dict(),
                    "n_val": fold_result.get("n_val", 0),
                    "train_time_s": fold_result.get("train_time_s", 0.0),
                    "infer_time_s": fold_result.get("infer_time_s", 0.0),
                    "n_features": fold_result.get("n_features", 0),
                    "elapsed_s": round(fold_elapsed, 1),
                }
                save_fold_result_locked(result_row, results, output_dir, model_name)

                logger.info(
                    "    H=%3dd | RMSE=%7.1f  MAE=%7.1f  MAPE=%.2f%%  PCC=%.4f | "
                    "%d pts  (%d feats)  (%.1fs)",
                    h_days, metrics.rmse, metrics.mae, metrics.mape_pct,
                    metrics.pcc, metrics.n_samples,
                    fold_result.get("n_features", 0), fold_elapsed,
                )
            else:
                logger.warning("    Fold %d skipped — no metrics.", fold_idx)

            if args.quick:
                logger.info("    --quick mode: stopping after first fold.")
                break

        all_results.extend(results)
        logger.info("  Horizon %d days complete in %.0fs.", h_days, time.time() - h_t0)

    # This task's own results (its shard(s) are already saved on disk).
    results_df = pd.DataFrame(all_results) if all_results else pd.DataFrame()

    # Best-effort merge of all shards (from this and any other completed
    # tasks) into the combined CSV. Safe to race — shards are the source
    # of truth, so a concurrent merge just means the next merge picks up
    # anything missed.
    try:
        from .merge_shards import merge_shards
        merged = merge_shards(output_dir, model_name)
        if merged is not None:
            results_df = merged
    except Exception as e:
        logger.warning("Shard merge failed (non-fatal, shards are safe): %s", e)

    print_summary_table(results_df, model_name)

    try:
        plot_metric_evolution(results_df, model_name, output_dir)
    except Exception as e:
        logger.warning("Plotting failed: %s", e)

    meta = {
        "script": "train_sarimax_lstm_v2.py",
        "started": datetime.now().isoformat(),
        "model": model_name,
        "features": ALL_FEATURES,
        "n_features": len(ALL_FEATURES),
        "sarimax": {"order": list(C.SARIMAXConfig().order),
                    "seasonal_order": list(C.SARIMAXConfig().seasonal_order),
                    "max_train_hours": MAX_SARIMAX_TRAIN_HOURS},
        "lstm": {"input_sequence_length": C.SARIMAXLSTMConfig().input_sequence_length,
                 "lstm_units": C.SARIMAXLSTMConfig().lstm_units,
                 "epochs": C.SARIMAXLSTMConfig().epochs},
        "total_elapsed_s": round(time.time() - t0, 1),
    }
    with open(output_dir / "run_metadata_sarimax_lstm.json", "w") as f:
        json.dump(meta, f, indent=2)

    total = time.time() - t0
    logger.info("\n" + "=" * 70)
    logger.info("  DONE — %.0fs (%.1f min)", total, total / 60)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
