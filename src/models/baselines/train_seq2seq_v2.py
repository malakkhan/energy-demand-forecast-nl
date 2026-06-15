#!/usr/bin/env python3
"""Dedicated Seq2Seq Encoder-Decoder LSTM baseline with 36-feature ROCV.

Architecture:
    Encoder: LSTM(64) processes 720-hour (30-day) input sequences of
        36 features + target = 37 input channels.
    Decoder: LSTM(64) initialised with encoder hidden/cell states,
        produces 168-hour (7-day) output per chunk.
    Output: Linear(1) at each decoder step.
    Teacher forcing during training; autoregressive at inference.

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
# Seq2Seq Model
# ═══════════════════════════════════════════════════════════════════════

class Encoder(nn.Module):
    def __init__(self, n_features: int, hidden_size: int):
        super().__init__()
        self.lstm = nn.LSTM(input_size=n_features, hidden_size=hidden_size, batch_first=True)

    def forward(self, x):
        _, (hidden, cell) = self.lstm(x)
        return hidden, cell


class Decoder(nn.Module):
    def __init__(self, hidden_size: int):
        super().__init__()
        self.lstm = nn.LSTM(input_size=1, hidden_size=hidden_size, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x, hidden, cell):
        out, (hidden, cell) = self.lstm(x, (hidden, cell))
        pred = self.fc(out.squeeze(1))
        return pred, hidden, cell


class Seq2SeqModel(nn.Module):
    def __init__(self, n_features: int, cfg: C.Seq2SeqConfig):
        super().__init__()
        self.encoder = Encoder(n_features, cfg.encoder_units)
        self.decoder = Decoder(cfg.decoder_units)
        self.output_length = cfg.output_sequence_length
        self.teacher_forcing_ratio = cfg.teacher_forcing_ratio

    def forward(self, src, trg=None, teacher_forcing=True):
        batch_size = src.size(0)
        hidden, cell = self.encoder(src)
        outputs = torch.zeros(batch_size, self.output_length, device=src.device)
        dec_input = src[:, -1, -1:].unsqueeze(1)  # last target value

        for t in range(self.output_length):
            pred, hidden, cell = self.decoder(dec_input, hidden, cell)
            outputs[:, t] = pred.squeeze(-1)
            if teacher_forcing and trg is not None:
                dec_input = trg[:, t].unsqueeze(1).unsqueeze(2)
            else:
                dec_input = pred.unsqueeze(1)

        return outputs


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

    # ── 4. Scale ─────────────────────────────────────────────────────
    scale_cols = feature_cols + [TARGET]  # target MUST be last
    n_features = len(scale_cols)

    scaler = MinMaxScaler(feature_range=(0, 1))
    train_vals = np.nan_to_num(train_df[scale_cols].values.astype(np.float64), nan=0.0)
    scaler.fit(train_vals)

    train_scaled = scaler.transform(train_vals).astype(np.float32)
    target_idx = len(scale_cols) - 1  # last col

    # ── 5. Create sequences ──────────────────────────────────────────
    input_len = cfg.input_sequence_length   # 720
    output_len = cfg.output_sequence_length  # 168
    total = input_len + output_len

    n_seq = len(train_scaled) - total
    if n_seq <= 0:
        logger.warning("Not enough data for sequences (need %d, have %d).", total, len(train_scaled))
        return {"n_train": len(train_df), "n_test": len(test_df)}

    X_train = np.empty((n_seq, input_len, n_features), dtype=np.float32)
    Y_train = np.empty((n_seq, output_len), dtype=np.float32)
    for i in range(n_seq):
        X_train[i] = train_scaled[i:i + input_len]
        Y_train[i] = train_scaled[i + input_len:i + total, target_idx]

    logger.info("Seq2Seq: %d training sequences, input=%d, output=%d, features=%d.",
                len(X_train), input_len, output_len, n_features)

    # Validation sequences (from val_df)
    if len(val_df) > total:
        val_vals = np.nan_to_num(val_df[scale_cols].values.astype(np.float64), nan=0.0)
        val_scaled = scaler.transform(val_vals).astype(np.float32)
        n_val_seq = len(val_scaled) - total
        X_val = np.empty((n_val_seq, input_len, n_features), dtype=np.float32)
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
    model = Seq2SeqModel(n_features, cfg).to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.learning_rate)
    criterion = nn.MSELoss()

    train_loader = DataLoader(
        TensorDataset(torch.from_numpy(X_train), torch.from_numpy(Y_train)),
        batch_size=cfg.batch_size, shuffle=False,
    )
    X_val_t = torch.from_numpy(X_val).to(DEVICE)
    Y_val_t = torch.from_numpy(Y_val).to(DEVICE)

    best_val_loss = float("inf")
    best_state = None
    patience_counter = 0

    for epoch in range(cfg.epochs):
        model.train()
        epoch_loss = 0.0
        for xb, yb in train_loader:
            xb, yb = xb.to(DEVICE), yb.to(DEVICE)
            pred = model(xb, yb, teacher_forcing=True)
            loss = criterion(pred, yb)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item() * len(xb)

        model.eval()
        with torch.no_grad():
            val_pred = model(X_val_t, teacher_forcing=False)
            val_loss = criterion(val_pred, Y_val_t).item()

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

    # ── 7. Inference: iterative multi-chunk ──────────────────────────
    test_vals = np.nan_to_num(test_df[scale_cols].values.astype(np.float64), nan=0.0)
    test_scaled = scaler.transform(test_vals).astype(np.float32)

    n_test_hours = len(test_df)
    horizon_hours = horizon_days * 24
    chunk = cfg.output_sequence_length

    # Seed: last input_len of training data
    seed = train_scaled[-input_len:].copy()
    all_preds_scaled = []

    with torch.no_grad():
        remaining = min(n_test_hours, horizon_hours)
        while remaining > 0:
            inp = torch.from_numpy(seed.reshape(1, input_len, n_features)).to(DEVICE)
            out = model(inp, teacher_forcing=False)
            chunk_pred = out.cpu().numpy().flatten()

            take = min(len(chunk_pred), remaining)
            all_preds_scaled.extend(chunk_pred[:take])
            remaining -= take

            # Slide seed window
            new_rows = np.zeros((take, n_features), dtype=np.float32)
            new_rows[:, target_idx] = chunk_pred[:take]
            for feat_i in range(n_features):
                if feat_i != target_idx:
                    new_rows[:, feat_i] = seed[-1, feat_i]
            seed = np.concatenate([seed[take:], new_rows], axis=0)

    preds_scaled = np.array(all_preds_scaled[:n_test_hours], dtype=np.float64)

    # Inverse transform
    dummy = np.zeros((len(preds_scaled), n_features), dtype=np.float64)
    dummy[:, target_idx] = preds_scaled
    y_pred_mw = scaler.inverse_transform(dummy)[:, target_idx]
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
# Main
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Train Seq2Seq with 36-feature ROCV.")
    parser.add_argument("--data-path", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument("--horizon", type=int, default=None)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--fold-step", type=int, default=FOLD_STEP_YEARS)
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else C.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    model_name = "ashtar_seq2seq"

    logger.info("=" * 70)
    logger.info("  Seq2Seq Baseline — 36-Feature ROCV")
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
                save_fold_result(result_row, results, output_dir, model_name)

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
