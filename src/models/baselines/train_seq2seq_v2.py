#!/usr/bin/env python3
"""Dedicated Seq2Seq Encoder-Decoder LSTM baseline with 36-feature ROCV.

Architecture (matching Ashtar et al. FinalModels.ipynb):
    Encoder: LSTM(64) processes 720-hour (30-day) input sequences of
        N features (no target in encoder input).
    Decoder: LSTM(64, return_sequences=True) initialised with encoder
        hidden/cell states, receives shifted target as full sequence.
        TimeDistributed Dense(1) output.
    Teacher forcing: full-sequence during training.
    Autoregressive step-by-step at inference.

For horizons > 168h, iterative multi-chunk decoding is used.

Uses the same 36-feature set, ROCV protocol, and preprocessing as MLR:
- 20 hand-picked features + 16 PCA CBS-KNMI full-coverage components
- IQR outlier removal, ffill/bfill/median imputation
- Horizon-shifted lag and PCA predictors
- 1-year validation holdout for early stopping

Usage
-----
    python -u src/models/baselines/train_seq2seq_v2.py
    python -u src/models/baselines/train_seq2seq_v2.py --quick
"""

import argparse
import fcntl
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader, TensorDataset

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.models.baselines import config as C
from src.models.baselines.data_loader import clear_outliers_iqr
from src.models.baselines.evaluation import (
    compute_metrics,
    plot_metric_evolution,
    print_summary_table,
    save_final_results,
    save_fold_result,
)

logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("train_seq2seq_v2")

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
    "is_public_holiday", "is_school_holiday", "is_energy_crisis",
]
LAG_FEATURES: List[str] = [
    "load_lag_12h", "load_lag_24h", "load_lag_1w", "load_lag_1y",
    "solar_rad_lag_10h", "humidity_lag_12h", "temp_lag_107h",
]
PCA_FEATURES: List[str] = [f"pca_cbs_knmi_full_{i:02d}" for i in range(1, 17)]

ALL_FEATURES: List[str] = TEMPORAL_FEATURES + LAG_FEATURES + PCA_FEATURES
assert len(ALL_FEATURES) == 36

# Temporal features that can be recomputed for future timesteps
CYCLICAL_FEATURES = ["hour_sin", "hour_cos", "dow_sin", "dow_cos", "month_sin", "month_cos"]

_MAX_NAN_FRAC = 0.50
ROCV_START = "2012-01-01"
MIN_TRAIN_YEARS = 2
FOLD_STEP_YEARS = 1
HORIZONS_DAYS = list(range(60, 181, 15))


# ═══════════════════════════════════════════════════════════════════════
# Data loading / shifting / ROCV
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
    cols_keep = ["timestamp", TARGET] + [c for c in ALL_FEATURES if c in df.columns]
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


def rocv_folds_with_val(df, horizons_days, fold_step=FOLD_STEP_YEARS):
    start = pd.Timestamp(ROCV_START)
    data_end = df["timestamp"].max()
    max_horizon_td = pd.Timedelta(days=max(horizons_days))
    train_end = start + pd.DateOffset(years=MIN_TRAIN_YEARS)
    val_end = train_end + pd.DateOffset(years=1)
    test_origin = val_end
    fold_idx = 0
    while test_origin + max_horizon_td <= data_end:
        yield fold_idx, train_end, val_end, test_origin
        train_end += pd.DateOffset(years=fold_step)
        val_end = train_end + pd.DateOffset(years=1)
        test_origin = val_end
        fold_idx += 1


# ═══════════════════════════════════════════════════════════════════════
# Seq2Seq Model (matching Ashtar's architecture)
# ═══════════════════════════════════════════════════════════════════════

class Encoder(nn.Module):
    def __init__(self, n_features: int, hidden_size: int):
        super().__init__()
        self.lstm = nn.LSTM(input_size=n_features, hidden_size=hidden_size, batch_first=True)

    def forward(self, x):
        _, (hidden, cell) = self.lstm(x)
        return hidden, cell


class Decoder(nn.Module):
    """Full-sequence decoder matching Ashtar's Keras implementation.

    During training:  receives full shifted-target sequence (batch, output_len, 1)
                      and returns predictions at all time steps.
    During inference: called step-by-step autoregressively.
    """
    def __init__(self, hidden_size: int):
        super().__init__()
        self.lstm = nn.LSTM(input_size=1, hidden_size=hidden_size, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x, hidden, cell):
        # x: (batch, seq_len, 1) — full sequence or single step
        out, (hidden, cell) = self.lstm(x, (hidden, cell))
        # out: (batch, seq_len, hidden_size)
        pred = self.fc(out)  # (batch, seq_len, 1)
        return pred, hidden, cell


class Seq2SeqModel(nn.Module):
    """Encoder-Decoder LSTM matching Ashtar's architecture.

    Training mode:  full-sequence decoder (teacher forcing with shifted target).
    Inference mode: step-by-step autoregressive decoding.
    """

    def __init__(self, n_features: int, cfg: C.Seq2SeqConfig):
        super().__init__()
        self.encoder = Encoder(n_features, cfg.encoder_units)
        self.decoder = Decoder(cfg.decoder_units)
        self.output_length = cfg.output_sequence_length

    def forward(self, src, trg=None, teacher_forcing=True):
        """
        Parameters
        ----------
        src : (batch, input_seq_len, features) — encoder input
        trg : (batch, output_seq_len) — decoder target values
        teacher_forcing : bool
            If True and trg is provided, use full-sequence decoding
            with shifted target as decoder input.
            If False, use step-by-step autoregressive decoding.
        """
        batch_size = src.size(0)
        hidden, cell = self.encoder(src)

        if teacher_forcing and trg is not None:
            # ── Full-sequence decoding (training) ──
            # Decoder input = shifted target: [last_encoder_target, trg[0], trg[1], ..., trg[-2]]
            # This matches Ashtar's: target_array[i + input_length - 1 : i + input_length - 1 + output_length]
            last_enc_target = src[:, -1, -1:]  # (batch, 1) — last target value from encoder
            shifted_trg = trg[:, :-1]          # (batch, output_len - 1) — all but last target
            dec_input = torch.cat([
                last_enc_target.unsqueeze(1),          # (batch, 1, 1)
                shifted_trg.unsqueeze(2),              # (batch, output_len-1, 1)
            ], dim=1)  # (batch, output_len, 1)

            pred, _, _ = self.decoder(dec_input, hidden, cell)  # (batch, output_len, 1)
            return pred.squeeze(-1)  # (batch, output_len)

        else:
            # ── Step-by-step autoregressive decoding (inference) ──
            outputs = torch.zeros(batch_size, self.output_length, device=src.device)
            dec_input = src[:, -1, -1:].unsqueeze(1)  # (batch, 1, 1)

            for t in range(self.output_length):
                pred, hidden, cell = self.decoder(dec_input, hidden, cell)
                outputs[:, t] = pred.squeeze(-1).squeeze(-1)
                dec_input = pred  # (batch, 1, 1) — feed own prediction

            return outputs


# ═══════════════════════════════════════════════════════════════════════
# Temporal feature computation for future timesteps
# ═══════════════════════════════════════════════════════════════════════

def compute_future_temporal_features(
    test_origin: pd.Timestamp,
    n_hours: int,
    feature_cols: List[str],
    feature_scaler: MinMaxScaler,
) -> np.ndarray:
    """Compute properly-scaled temporal features for future timesteps.

    Returns an array of shape (n_hours, len(feature_cols)) with correct
    cyclical temporal values for each future hour, scaled using the
    training-fit feature scaler.
    """
    future_ts = pd.date_range(start=test_origin, periods=n_hours, freq="h")
    future_df = pd.DataFrame({"timestamp": future_ts})

    # Compute raw temporal features
    future_df["hour_sin"] = np.sin(2 * np.pi * future_ts.hour / 24)
    future_df["hour_cos"] = np.cos(2 * np.pi * future_ts.hour / 24)
    future_df["dow_sin"] = np.sin(2 * np.pi * future_ts.dayofweek / 7)
    future_df["dow_cos"] = np.cos(2 * np.pi * future_ts.dayofweek / 7)
    future_df["month_sin"] = np.sin(2 * np.pi * (future_ts.month - 1) / 12)
    future_df["month_cos"] = np.cos(2 * np.pi * (future_ts.month - 1) / 12)
    future_df["day_of_year"] = future_ts.dayofyear.astype(float)
    future_df["week_of_year"] = future_ts.isocalendar().week.values.astype(float)
    future_df["quarter"] = future_ts.quarter.astype(float)
    future_df["year"] = future_ts.year.astype(float)
    future_df["is_public_holiday"] = 0.0
    future_df["is_school_holiday"] = 0.0
    future_df["is_energy_crisis"] = 0.0

    # Build feature array in the same column order as the scaler
    result = np.zeros((n_hours, len(feature_cols)), dtype=np.float64)
    for col_idx, col_name in enumerate(feature_cols):
        if col_name in future_df.columns:
            result[:, col_idx] = future_df[col_name].values
        else:
            # For lag/PCA features, use 0.0 (will be scaled to a neutral value)
            result[:, col_idx] = 0.0

    # Scale using the fitted feature scaler
    result_scaled = feature_scaler.transform(result).astype(np.float32)
    return result_scaled


# ═══════════════════════════════════════════════════════════════════════
# Seq2Seq fold execution
# ═══════════════════════════════════════════════════════════════════════

def run_seq2seq_fold(
    df_shifted: pd.DataFrame,
    train_end: pd.Timestamp,
    val_end: pd.Timestamp,
    test_origin: pd.Timestamp,
    horizon_days: int,
) -> Dict:
    """Execute one Seq2Seq fold."""
    cfg = C.Seq2SeqConfig()
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

    # ── 4. Scale — SEPARATE scalers for features and target ──────────
    n_features = len(feature_cols)

    feature_scaler = MinMaxScaler(feature_range=(0, 1))
    target_scaler = MinMaxScaler(feature_range=(0, 1))

    train_feat_vals = np.nan_to_num(train_df[feature_cols].values.astype(np.float64), nan=0.0)
    train_target_vals = train_df[[TARGET]].values.astype(np.float64)

    feature_scaler.fit(train_feat_vals)
    target_scaler.fit(train_target_vals)

    train_feat_scaled = feature_scaler.transform(train_feat_vals).astype(np.float32)
    train_target_scaled = target_scaler.transform(train_target_vals).astype(np.float32).flatten()

    # Combined: features + target (target is last column)
    train_scaled = np.column_stack([train_feat_scaled, train_target_scaled])

    # ── 5. Create sequences ──────────────────────────────────────────
    input_len = cfg.input_sequence_length   # 720
    output_len = cfg.output_sequence_length  # 168
    total = input_len + output_len
    target_idx = n_features  # target is the last column

    n_seq = len(train_scaled) - total
    if n_seq <= 0:
        logger.warning("Not enough data for sequences (need %d, have %d).", total, len(train_scaled))
        return {"n_train": len(train_df), "n_test": len(test_df)}

    # Encoder input: all columns (features + target)
    n_enc_features = n_features + 1  # features + target
    X_train = np.empty((n_seq, input_len, n_enc_features), dtype=np.float32)
    Y_train = np.empty((n_seq, output_len), dtype=np.float32)
    for i in range(n_seq):
        X_train[i] = train_scaled[i:i + input_len]
        Y_train[i] = train_scaled[i + input_len:i + total, target_idx]

    logger.info("Seq2Seq: %d training sequences, input=%d, output=%d, features=%d (enc=%d).",
                len(X_train), input_len, output_len, n_features, n_enc_features)

    # Validation sequences (from val_df)
    if len(val_df) > total:
        val_feat_vals = np.nan_to_num(val_df[feature_cols].values.astype(np.float64), nan=0.0)
        val_target_vals = val_df[[TARGET]].values.astype(np.float64)
        val_feat_scaled = feature_scaler.transform(val_feat_vals).astype(np.float32)
        val_target_scaled = target_scaler.transform(val_target_vals).astype(np.float32).flatten()
        val_scaled = np.column_stack([val_feat_scaled, val_target_scaled])

        n_val_seq = len(val_scaled) - total
        X_val = np.empty((n_val_seq, input_len, n_enc_features), dtype=np.float32)
        Y_val = np.empty((n_val_seq, output_len), dtype=np.float32)
        for i in range(n_val_seq):
            X_val[i] = val_scaled[i:i + input_len]
            Y_val[i] = val_scaled[i + input_len:i + total, target_idx]
    else:
        # If val is too short, use last 20% of training sequences
        split_pt = max(1, int(n_seq * 0.8))
        X_val = X_train[split_pt:]
        Y_val = Y_train[split_pt:]
        X_train = X_train[:split_pt]
        Y_train = Y_train[:split_pt]

    # ── 6. Build & train ─────────────────────────────────────────────
    model = Seq2SeqModel(n_enc_features, cfg).to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.learning_rate)
    criterion = nn.MSELoss()

    train_loader = DataLoader(
        TensorDataset(torch.from_numpy(X_train), torch.from_numpy(Y_train)),
        batch_size=cfg.batch_size, shuffle=True,  # Fix 4: shuffle=True
    )
    X_val_t = torch.from_numpy(X_val)   # keep on CPU, batch to GPU
    Y_val_t = torch.from_numpy(Y_val)
    val_loader = DataLoader(
        TensorDataset(X_val_t, Y_val_t),
        batch_size=cfg.batch_size, shuffle=False,
    )

    best_val_loss = float("inf")
    best_state = None
    patience_counter = 0

    for epoch in range(cfg.epochs):
        model.train()
        epoch_loss = 0.0
        for xb, yb in train_loader:
            xb, yb = xb.to(DEVICE), yb.to(DEVICE)
            # Fix 1: full-sequence teacher forcing
            pred = model(xb, yb, teacher_forcing=True)
            loss = criterion(pred, yb)
            optimizer.zero_grad()
            loss.backward()
            # Fix 4: gradient clipping
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            epoch_loss += loss.item() * len(xb)

        model.eval()
        with torch.no_grad():
            val_loss_sum = 0.0
            val_n = 0
            for xb_v, yb_v in val_loader:
                xb_v, yb_v = xb_v.to(DEVICE), yb_v.to(DEVICE)
                vp = model(xb_v, teacher_forcing=False)
                val_loss_sum += criterion(vp, yb_v).item() * len(xb_v)
                val_n += len(xb_v)
            val_loss = val_loss_sum / max(val_n, 1)

        if (epoch + 1) % 5 == 0:
            logger.info("  Seq2Seq epoch %d/%d — train_loss=%.6f  val_loss=%.6f",
                        epoch + 1, cfg.epochs, epoch_loss / max(len(X_train), 1), val_loss)

        if val_loss < best_val_loss - 1e-5:
            best_val_loss = val_loss
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            patience_counter = 0
        else:
            patience_counter += 1
            if patience_counter >= 5:
                logger.info("  Seq2Seq early stopping at epoch %d.", epoch + 1)
                break

    if best_state is not None:
        model.load_state_dict(best_state)
    model.eval()

    # ── 7. Inference: iterative multi-chunk with proper features ─────
    n_test_hours = len(test_df)
    horizon_hours = horizon_days * 24
    chunk = cfg.output_sequence_length

    # Seed: last input_len of training data
    seed = train_scaled[-input_len:].copy()
    all_preds_scaled = []

    # Fix 3: Compute future temporal features
    future_features = compute_future_temporal_features(
        test_origin, horizon_hours, feature_cols, feature_scaler,
    )

    with torch.no_grad():
        remaining = min(n_test_hours, horizon_hours)
        hours_predicted = 0
        while remaining > 0:
            inp = torch.from_numpy(seed.reshape(1, input_len, n_enc_features)).to(DEVICE)
            out = model(inp, teacher_forcing=False)
            chunk_pred = out.cpu().numpy().flatten()

            take = min(len(chunk_pred), remaining)
            all_preds_scaled.extend(chunk_pred[:take])

            # Slide seed window with proper future features
            new_rows = np.zeros((take, n_enc_features), dtype=np.float32)
            # Use precomputed future features for the feature columns
            feat_start = hours_predicted
            feat_end = hours_predicted + take
            if feat_end <= len(future_features):
                new_rows[:, :n_features] = future_features[feat_start:feat_end]
            else:
                # Fallback: use midpoint of scaled range
                new_rows[:, :n_features] = 0.5
            # Target column: use predicted values
            new_rows[:, target_idx] = chunk_pred[:take]

            seed = np.concatenate([seed[take:], new_rows], axis=0)
            remaining -= take
            hours_predicted += take

    preds_scaled = np.array(all_preds_scaled[:n_test_hours], dtype=np.float64)

    # Fix 2: Inverse transform using target-only scaler
    y_pred_mw = target_scaler.inverse_transform(preds_scaled.reshape(-1, 1)).flatten()
    y_true_mw = test_df[TARGET].values[:len(y_pred_mw)]

    metrics = compute_metrics(y_true_mw, y_pred_mw)

    return {
        "predictions_mw": y_pred_mw,
        "actuals_mw": y_true_mw,
        "timestamps": test_df["timestamp"].values[:len(y_pred_mw)] if "timestamp" in test_df.columns else None,
        "n_train": len(train_df),
        "n_test": len(y_true_mw),
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
    """Append a fold result with file-locking for parallel-job safety.

    Reads the current CSV from disk (which may include results from
    other concurrently-running SLURM jobs), appends the new row,
    deduplicates on (model, fold, horizon_days), and writes back
    under an exclusive file lock.
    """
    csv_path = output_dir / f"rocv_results_{model_name}.csv"
    lock_path = output_dir / f".lock_{model_name}"

    with open(lock_path, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            # Read current disk state (may include results from other jobs)
            if csv_path.exists():
                disk_rows = pd.read_csv(csv_path).to_dict("records")
            else:
                disk_rows = []

            disk_rows.append(result_row)

            # Deduplicate on (model, fold, horizon_days) — keep latest
            seen: Dict[tuple, Dict] = {}
            for row in disk_rows:
                key = (row["model"], int(row["fold"]), int(row["horizon_days"]))
                seen[key] = row
            deduped = list(seen.values())

            pd.DataFrame(deduped).to_csv(csv_path, index=False)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)

    # Also update in-memory list
    results_list.append(result_row)


# ═══════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Train Seq2Seq with 36-feature ROCV.")
    parser.add_argument("--data-path", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument("--horizon", type=int, default=None)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from existing results, skipping completed folds. "
                             "Safe for parallel per-horizon SLURM jobs.")
    parser.add_argument("--fold-step", type=int, default=FOLD_STEP_YEARS)
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else C.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    model_name = "ashtar_seq2seq"

    logger.info("=" * 70)
    logger.info("  Seq2Seq Baseline — 36-Feature ROCV (v2 — Ashtar-matched architecture)")
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

    results: List[Dict] = []
    completed: set = set()
    csv_path = output_dir / f"rocv_results_{model_name}.csv"
    if args.resume and csv_path.exists():
        existing_df = pd.read_csv(csv_path)
        results = existing_df.to_dict("records")
        for _, row in existing_df.iterrows():
            completed.add((int(row["horizon_days"]), int(row["fold"])))
        logger.info("Resumed: loaded %d existing results (%d unique horizon×fold pairs).",
                     len(results), len(completed))

    for h_days in horizons:
        h_t0 = time.time()
        logger.info("━" * 60)
        logger.info("HORIZON = %d days (%d hours)", h_days, h_days * 24)
        logger.info("━" * 60)

        df_shifted = apply_horizon_shift(df, h_days)

        for fold_idx, train_end, val_end, test_origin in rocv_folds_with_val(
            df_shifted, horizons, fold_step=args.fold_step,
        ):
            fold_t0 = time.time()
            logger.info("  FOLD %d — train=[..,%s)  val=[%s,%s)  test=%s  horizon=%dd",
                        fold_idx, train_end.date(), train_end.date(),
                        val_end.date(), test_origin.date(), h_days)

            if (h_days, fold_idx) in completed:
                logger.info("    Already complete — skipping.")
                continue

            fold_result = run_seq2seq_fold(
                df_shifted, train_end, val_end, test_origin, h_days,
            )

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
                    **metrics.to_dict(),
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

        logger.info("  Horizon %d days complete in %.0fs.", h_days, time.time() - h_t0)

    # In resume/parallel mode, reload from disk to include all jobs' results
    if args.resume and csv_path.exists():
        results_df = pd.read_csv(csv_path)
        # Also save JSON snapshot
        out_json = output_dir / f"rocv_results_{model_name}.json"
        with open(out_json, "w") as f:
            json.dump(results_df.to_dict("records"), f, indent=2, default=str)
        logger.info("Results: %s (%d rows across %d horizons).",
                     csv_path.name, len(results_df),
                     results_df["horizon_days"].nunique())
    else:
        results_df = save_final_results(results, output_dir, model_name)
    print_summary_table(results_df, model_name)

    try:
        plot_metric_evolution(results_df, model_name, output_dir)
    except Exception as e:
        logger.warning("Plotting failed: %s", e)

    meta = {
        "script": "train_seq2seq_v2.py",
        "started": datetime.now().isoformat(),
        "model": model_name,
        "device": str(DEVICE),
        "features": ALL_FEATURES,
        "n_features": len(ALL_FEATURES),
        "architecture": "Ashtar-matched full-sequence decoder",
        "fixes_applied": [
            "Fix 1: full-sequence decoder with return_sequences=True",
            "Fix 2: separate MinMaxScaler for features and target",
            "Fix 3: proper temporal feature computation during inference",
            "Fix 4: gradient clipping (max_norm=1.0) + shuffle=True",
        ],
        "seq2seq": {
            "input_sequence_length": C.Seq2SeqConfig().input_sequence_length,
            "output_sequence_length": C.Seq2SeqConfig().output_sequence_length,
            "encoder_units": C.Seq2SeqConfig().encoder_units,
            "decoder_units": C.Seq2SeqConfig().decoder_units,
            "epochs": C.Seq2SeqConfig().epochs,
            "batch_size": C.Seq2SeqConfig().batch_size,
        },
        "total_elapsed_s": round(time.time() - t0, 1),
    }
    with open(output_dir / "run_metadata_seq2seq.json", "w") as f:
        json.dump(meta, f, indent=2)

    total = time.time() - t0
    logger.info("\n" + "=" * 70)
    logger.info("  DONE — %.0fs (%.1f min)", total, total / 60)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
