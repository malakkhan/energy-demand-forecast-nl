#!/usr/bin/env python3
"""Dedicated Prophet-LSTM baseline training with explicit 36-feature set and ROCV.

Runs Prophet-LSTM (Huang architecture) with:
- 20 hand-picked features + 16 PCA CBS-KNMI full-coverage components (36 total)
- Expanding-window ROCV with 1-year validation holdout for early stopping
- Horizon-shifted lag and PCA predictors for strict temporal integrity
- Pre-training safeguards identical to MLR:
    1. Drop columns where >50% of training data is NaN
    2. IQR outlier removal on training data only
    3. Forward-fill → backward-fill → median imputation (no MICE)

Architecture:
    Stage 1 — Prophet: fitted on training data with additive seasonality.
        Produces 5 component columns (yhat, trend, daily, weekly, yearly).
        All 36 features are added as regressors.
    Stage 2 — PyTorch LSTM (Huang):
        LSTM(30) → LSTM(90) → Linear(1), optimizer=Adam, seq_len=1.
        Early stopping: patience=3, min_delta=1e-4, start_from_epoch=10.
        Input = Prophet components (5) + 36 original features = 41 features.

ROCV Protocol
-------------
Fold 0:  train = [2012, 2014)  val = [2014, 2015)  test_origin = 2015-01-01
Fold 1:  train = [2012, 2015)  val = [2015, 2016)  test_origin = 2016-01-01
  …       (origin advances by 1 year each iteration)

Usage
-----
    python -u src/models/baselines/train_prophet_lstm_v2.py
    python -u src/models/baselines/train_prophet_lstm_v2.py --horizon 60
    python -u src/models/baselines/train_prophet_lstm_v2.py --quick
"""

import argparse
import json
import logging
import os
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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

# ── Logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("train_prophet_lstm_v2")

# Suppress Prophet / cmdstanpy noise
os.environ.setdefault("CMDSTANPY_NO_VERBOSE", "1")
for _name in ("prophet", "cmdstanpy"):
    logging.getLogger(_name).setLevel(logging.WARNING)

# Auto-detect device
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

PCA_FEATURES: List[str] = []  # populated per-fold at runtime

ALL_FEATURES: List[str] = TEMPORAL_FEATURES + LAG_FEATURES + PCA_FEATURES
# assert len(ALL_FEATURES) == 36

# Prophet output columns added to the LSTM input
PROPHET_COMPONENTS: List[str] = ["yhat", "trend", "daily", "weekly", "yearly"]

# ── Pre-training thresholds ──────────────────────────────────────────
_MAX_NAN_FRAC = 0.50

# ── ROCV defaults ────────────────────────────────────────────────────
ROCV_START = "2012-01-01"
MIN_TRAIN_YEARS = 2
FOLD_STEP_MONTHS = 13  # 13-month step cycles through calendar months
HORIZONS_DAYS = list(range(60, 181, 15))


# ═══════════════════════════════════════════════════════════════════════
# 1.  Data loading (identical to train_mlr.py)
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

    available = [c for c in ALL_FEATURES if c in df.columns]
    missing = [c for c in ALL_FEATURES if c not in df.columns]
    if missing:
        logger.warning("Missing %d/%d features: %s", len(missing), len(ALL_FEATURES), missing)

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

    n_before = len(df)
    df = df.dropna(subset=[TARGET])
    n_dropped = n_before - len(df)
    if n_dropped > 0:
        logger.info("Dropped %d rows with NaN target.", n_dropped)

    logger.info("Dataset: %d rows × %d cols  (%s → %s)",
                len(df), len(df.columns),
                df["timestamp"].min().date(), df["timestamp"].max().date())
    return df


# ═══════════════════════════════════════════════════════════════════════
# 2.  Horizon shifting (identical to train_mlr.py)
# ═══════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════
# 3.  ROCV fold generation (with validation holdout)
# ═══════════════════════════════════════════════════════════════════════

def rocv_folds_with_val(df: pd.DataFrame, horizons_days: List[int], fold_step: int = FOLD_STEP_MONTHS):
    """Generate expanding-window ROCV folds with 1-year validation holdout.

    Yields
    ------
    fold_idx : int
    train_end : pd.Timestamp  — end of training window (exclusive)
    val_end : pd.Timestamp    — end of validation window = test_origin
    test_origin : pd.Timestamp
    """
    start = pd.Timestamp(ROCV_START)
    data_end = df["timestamp"].max()
    max_horizon_td = pd.Timedelta(days=max(horizons_days))

    # First fold: 2 years train + 1 year val
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
# 4.  PyTorch LSTM model
# ═══════════════════════════════════════════════════════════════════════

class LSTMHuang(nn.Module):
    """LSTM(30) → LSTM(90) → Linear(1).  No dropout."""
    def __init__(self, n_features: int, cfg: C.LSTMConfig = None):
        super().__init__()
        cfg = cfg or C.LSTMConfig()
        self.lstm1 = nn.LSTM(input_size=n_features, hidden_size=cfg.lstm1_units, batch_first=True)
        self.lstm2 = nn.LSTM(input_size=cfg.lstm1_units, hidden_size=cfg.lstm2_units, batch_first=True)
        self.fc = nn.Linear(cfg.lstm2_units, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm1(x)
        out, _ = self.lstm2(out)
        out = out[:, -1, :]
        return self.fc(out).squeeze(-1)


class EarlyStopping:
    def __init__(self, patience: int, min_delta: float, start_epoch: int):
        self.patience = patience
        self.min_delta = min_delta
        self.start_epoch = start_epoch
        self.best_loss = float("inf")
        self.counter = 0
        self.best_state = None

    def step(self, epoch: int, val_loss: float, model: nn.Module) -> bool:
        if epoch < self.start_epoch:
            return False
        if val_loss < self.best_loss - self.min_delta:
            self.best_loss = val_loss
            self.counter = 0
            self.best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            return False
        self.counter += 1
        return self.counter >= self.patience

    def restore(self, model: nn.Module):
        if self.best_state is not None:
            model.load_state_dict(self.best_state)


# ═══════════════════════════════════════════════════════════════════════
# 5.  Prophet-LSTM fold execution
# ═══════════════════════════════════════════════════════════════════════

def run_prophet_lstm_fold(
    df_shifted: pd.DataFrame,
    train_end: pd.Timestamp,
    val_end: pd.Timestamp,
    test_origin: pd.Timestamp,
    horizon_days: int,
    seed: int = 42,
    lstm_cfg: C.LSTMConfig = None,
) -> Dict:
    """Execute one Prophet-LSTM fold.

    Pipeline:
    1. Split: train / val / test
    2. Drop columns >50% NaN in training set
    3. IQR outlier removal on training data only
    4. Forward-fill → backward-fill → median imputation
    5. Fit Prophet on train with 36 features as regressors
    6. MinMaxScale (fit on train)
    7. LSTM training with early stopping on validation
    8. Predict test, inverse-transform, compute metrics
    """
    lstm_cfg = lstm_cfg or C.LSTMConfig()
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
        logger.warning("Empty train (%d) or test (%d) — skipping.", len(train_df), len(test_df))
        return {"n_train": len(train_df), "n_test": len(test_df)}

    # ── 2. Identify and filter feature columns ───────────────────────
    feature_cols = [
        c for c in ALL_FEATURES
        if c in train_df.columns
        and train_df[c].dtype in (np.float64, np.float32, np.int64, np.int32)
    ]

    nan_fracs = train_df[feature_cols].isna().mean()
    usable = nan_fracs[nan_fracs <= _MAX_NAN_FRAC].index.tolist()
    dropped = len(feature_cols) - len(usable)
    if dropped > 0:
        logger.info("Dropped %d/%d cols (>%.0f%% NaN): %s",
                     dropped, len(feature_cols), _MAX_NAN_FRAC * 100,
                     sorted(set(feature_cols) - set(usable)))
    feature_cols = usable

    if len(feature_cols) == 0:
        logger.warning("No usable feature columns remain.")
        return {"n_train": len(train_df), "n_test": len(test_df)}

    # ── 3. IQR outlier removal (train only) ──────────────────────────
    iqr_cols = feature_cols + [TARGET]
    train_df = clear_outliers_iqr(train_df, iqr_cols)

    # ── 4. Imputation: ffill → bfill → median ───────────────────────
    for col in feature_cols:
        train_df[col] = train_df[col].ffill().bfill()
        med = train_df[col].median()
        fill_val = med if np.isfinite(med) else 0.0
        train_df[col] = train_df[col].fillna(fill_val)

        for part_df in [val_df, test_df]:
            if col in part_df.columns:
                part_df[col] = part_df[col].ffill().bfill()
                part_df[col] = part_df[col].fillna(fill_val)

    # Drop NaN target
    train_df = train_df.dropna(subset=[TARGET])
    val_df = val_df.dropna(subset=[TARGET])
    test_df = test_df.dropna(subset=[TARGET])

    if len(train_df) == 0 or len(test_df) == 0:
        logger.warning("Empty after imputation: train=%d, test=%d.", len(train_df), len(test_df))
        return {"n_train": len(train_df), "n_test": len(test_df)}

    # ── 4b. Per-fold PCA (fitted on training data only) ───────────
    pca_cols, train_df, val_df, test_df = fit_fold_pca(train_df, val_df, test_df)
    if pca_cols:
        feature_cols = feature_cols + pca_cols
        # PCA leaves NaN components on rows whose raw covariates are
        # incomplete (imputation runs before PCA here); fill causally
        # within each split, mirroring the CMAT pipeline.
        for _split in (train_df, val_df, test_df):
            _split[pca_cols] = _split[pca_cols].ffill().bfill()
        logger.info("Added %d per-fold PCA cols. Total features: %d.", len(pca_cols), len(feature_cols))

    # ── 5. Prophet ───────────────────────────────────────────────────
    from prophet import Prophet

    # Build Prophet dataframe
    train_prophet = pd.DataFrame({"ds": train_df["timestamp"].values, "y": train_df[TARGET].values})
    for col in feature_cols:
        train_prophet[col] = train_df[col].values

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        m = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
        for col in feature_cols:
            m.add_regressor(col)
        m.fit(train_prophet)

    logger.info("Prophet fitted on %d rows with %d regressors.", len(train_prophet), len(feature_cols))

    def _prophet_predict(df_part, feature_cols_list):
        """Predict Prophet components for a data split."""
        p_df = pd.DataFrame({"ds": df_part["timestamp"].values, "y": df_part[TARGET].values})
        for col in feature_cols_list:
            p_df[col] = df_part[col].values
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            pred = m.predict(p_df)
        result = df_part.copy()
        for c in PROPHET_COMPONENTS:
            result[c] = pred[c].values if c in pred.columns else 0.0
        return result

    train_df = _prophet_predict(train_df, feature_cols)
    val_df = _prophet_predict(val_df, feature_cols) if len(val_df) > 0 else val_df
    test_df = _prophet_predict(test_df, feature_cols)

    # ── 6. Scale (MinMaxScaler) ──────────────────────────────────────
    prophet_cols_present = [c for c in PROPHET_COMPONENTS if c in train_df.columns]
    lstm_feature_cols = prophet_cols_present + feature_cols
    all_scale_cols = lstm_feature_cols + [TARGET]

    scaler = MinMaxScaler(feature_range=(0, 1))
    train_vals = np.nan_to_num(train_df[all_scale_cols].values.astype(np.float64), nan=0.0)
    scaler.fit(train_vals)

    def _scale(df_part):
        vals = np.nan_to_num(df_part[all_scale_cols].values.astype(np.float64), nan=0.0)
        return scaler.transform(vals).astype(np.float32)

    train_scaled = _scale(train_df)
    test_scaled = _scale(test_df)
    val_scaled = _scale(val_df) if len(val_df) > 0 else None

    n_features = len(lstm_feature_cols)
    target_idx = len(all_scale_cols) - 1  # target is last column

    # Reshape for seq_len=1: (N, 1, F)
    train_X = train_scaled[:, :n_features].reshape(-1, 1, n_features)
    train_y = train_scaled[:, target_idx]
    test_X = test_scaled[:, :n_features].reshape(-1, 1, n_features)
    test_y_scaled = test_scaled[:, target_idx]

    if val_scaled is not None and len(val_scaled) > 0:
        val_X = val_scaled[:, :n_features].reshape(-1, 1, n_features)
        val_y = val_scaled[:, target_idx]
    else:
        val_X, val_y = test_X, test_y_scaled

    logger.info("LSTM input: train=%s, val=%s, test=%s, features=%d.",
                train_X.shape, val_X.shape, test_X.shape, n_features)

    # ── 7. Train LSTM ────────────────────────────────────────────────
    t_train_start = time.time()
    model = LSTMHuang(n_features, lstm_cfg).to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=lstm_cfg.learning_rate)
    criterion = nn.MSELoss()
    es = EarlyStopping(lstm_cfg.early_stop_patience, lstm_cfg.early_stop_min_delta,
                       lstm_cfg.early_stop_start_epoch)

    train_loader = DataLoader(
        TensorDataset(torch.from_numpy(train_X), torch.from_numpy(train_y)),
        batch_size=lstm_cfg.batch_size, shuffle=False,
    )
    val_tensor_X = torch.from_numpy(val_X).to(DEVICE)
    val_tensor_y = torch.from_numpy(val_y).to(DEVICE)

    for epoch in range(lstm_cfg.epochs):
        model.train()
        epoch_loss = 0.0
        for xb, yb in train_loader:
            xb, yb = xb.to(DEVICE), yb.to(DEVICE)
            pred = model(xb)
            loss = criterion(pred, yb)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item() * len(xb)
        epoch_loss /= len(train_X)

        model.eval()
        with torch.no_grad():
            val_pred = model(val_tensor_X)
            val_loss = criterion(val_pred, val_tensor_y).item()

        if not np.isfinite(epoch_loss) or not np.isfinite(val_loss):
            logger.warning("NaN/Inf loss at epoch %d — stopping.", epoch)
            break

        if epoch % 10 == 0 or epoch == lstm_cfg.epochs - 1:
            logger.info("  Epoch %3d/%d — train_loss=%.6f  val_loss=%.6f",
                        epoch, lstm_cfg.epochs, epoch_loss, val_loss)

        if es.step(epoch, val_loss, model):
            logger.info("Early stopping at epoch %d (val_loss=%.6f).", epoch, val_loss)
            break

    es.restore(model)
    train_time_s = time.time() - t_train_start

    # ── 8. Predict ───────────────────────────────────────────────────
    t_infer_start = time.time()
    model.eval()
    with torch.no_grad():
        test_tensor = torch.from_numpy(test_X).to(DEVICE)
        yhat_scaled = model(test_tensor).cpu().numpy()

    # Inverse-transform predictions
    def _inverse(scaled_vals):
        dummy = np.zeros((len(scaled_vals), len(all_scale_cols)), dtype=np.float64)
        dummy[:, target_idx] = scaled_vals.ravel()
        return scaler.inverse_transform(dummy)[:, target_idx]

    y_pred_mw = _inverse(yhat_scaled)
    y_true_mw = _inverse(test_y_scaled)

    infer_time_s = time.time() - t_infer_start if "t_infer_start" in locals() else 0.0
    metrics = compute_metrics(y_true_mw, y_pred_mw)

    return {
        "predictions_mw": y_pred_mw,
        "actuals_mw": y_true_mw,
        "timestamps": test_df["timestamp"].values if "timestamp" in test_df.columns else None,
        "n_train_rows": len(train_df),
        "n_val_rows": len(val_df),
        "n_test_rows": len(test_df),
        "n_train": len(train_dataset) if "train_dataset" in locals() else len(train_df),
        "n_val": len(val_dataset) if "val_dataset" in locals() else len(val_df),
        "n_test": len(test_dataset) if "test_dataset" in locals() else len(test_df),
        "train_time_s": round(train_time_s, 2) if "train_time_s" in locals() else 0.0,
        "infer_time_s": round(infer_time_s, 2) if "infer_time_s" in locals() else 0.0,
        "metrics": metrics,
        "n_features": n_features,
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
# 6.  Main ROCV loop
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Train Prophet-LSTM baseline with 36-feature ROCV.")
    parser.add_argument("--data-path", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument("--horizon", type=int, default=None)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--fold-step", type=int, default=FOLD_STEP_MONTHS)
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--resume", action="store_true", help="Resume by skipping completed folds")
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else C.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    model_name = "van_de_sande_prophet_lstm"
    lstm_cfg = C.LSTMConfig()

    logger.info("=" * 70)
    logger.info("  Prophet-LSTM Baseline — 36-Feature ROCV")
    logger.info("  Device:     %s", DEVICE)
    logger.info("  Started:    %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("  Output dir: %s", output_dir)
    logger.info("=" * 70)

    df = load_dataset(args.data_path)

    horizons = [args.horizon] if args.horizon else HORIZONS_DAYS
    if args.quick:
        horizons = [horizons[0]]

    logger.info("Horizons: %s days", horizons)
    logger.info("Fold step: %d month(s)", args.fold_step)
    logger.info("Features: %d base + %d Prophet = %d LSTM input",
                len(ALL_FEATURES), len(PROPHET_COMPONENTS),
                len(ALL_FEATURES) + len(PROPHET_COMPONENTS))

    # Set seed
    import torch, random, numpy as np
    torch.manual_seed(args.seed)
    random.seed(args.seed)
    np.random.seed(args.seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(args.seed)
    logger.info("Seed: %d", args.seed)

    csv_path = output_dir / f"rocv_results_{model_name}.csv"

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

            fold_result = run_prophet_lstm_fold(
                df_shifted, train_end, val_end, test_origin, h_days, lstm_cfg,
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
                    "    H=%3dd | RMSE=%7.1f  MAE=%7.1f  MAPE=%.2f%%  "
                    "MASE=%.3f  nMAE=%.4f  nRMSE=%.4f  PCC=%.4f | "
                    "%d pts  (%d feats)  (%.1fs)",
                    h_days, metrics.rmse, metrics.mae, metrics.mape_pct,
                    metrics.mase, metrics.nmae, metrics.nrmse, metrics.pcc,
                    metrics.n_samples, fold_result.get("n_features", 0),
                    fold_elapsed,
                )
            else:
                logger.warning("    Fold %d skipped — no metrics returned.", fold_idx)

            if args.quick:
                logger.info("    --quick mode: stopping after first fold.")
                break

        all_results.extend(results)
        h_elapsed = time.time() - h_t0
        logger.info("  Horizon %d days complete in %.0fs.", h_days, h_elapsed)

    # ── Save final results ───────────────────────────────────────────
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
    except Exception as plot_err:
        logger.warning("Plotting failed: %s", plot_err)

    # ── Run metadata ─────────────────────────────────────────────────
    meta = {
        "script": "train_prophet_lstm_v2.py",
        "started": datetime.now().isoformat(),
        "model": model_name,
        "device": str(DEVICE),
        "features": ALL_FEATURES,
        "n_base_features": len(ALL_FEATURES),
        "prophet_components": PROPHET_COMPONENTS,
        "n_lstm_features": len(ALL_FEATURES) + len(PROPHET_COMPONENTS),
        "rocv": {
            "start_date": ROCV_START,
            "min_train_years": MIN_TRAIN_YEARS,
            "fold_step_months": args.fold_step,
            "horizons_days": horizons,
            "validation_holdout": "1 year",
        },
        "lstm": {
            "lstm1_units": lstm_cfg.lstm1_units,
            "lstm2_units": lstm_cfg.lstm2_units,
            "epochs": lstm_cfg.epochs,
            "batch_size": lstm_cfg.batch_size,
            "learning_rate": lstm_cfg.learning_rate,
            "early_stop_patience": lstm_cfg.early_stop_patience,
        },
        "total_elapsed_s": round(time.time() - t0, 1),
    }
    meta_path = output_dir / "run_metadata_prophet_lstm.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    logger.info("Metadata → %s", meta_path)

    total = time.time() - t0
    logger.info("\n" + "=" * 70)
    logger.info("  DONE — %.0fs (%.1f min)", total, total / 60)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
