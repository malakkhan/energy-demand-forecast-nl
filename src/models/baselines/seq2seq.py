"""Model 5: Sequence-to-Sequence Encoder-Decoder LSTM — Ashtar et al.

Architecture:
    Encoder: LSTM(64) processes a 720-hour (30-day) input sequence.
    Decoder: LSTM(64) initialised with encoder hidden/cell states,
             produces a 168-hour (7-day) output.
    Output:  Linear(1) applied at each decoder time step.

Training uses teacher forcing (feed true lagged target values to the
decoder during training).  At inference, the decoder runs
autoregressively using its own predictions.

For horizons longer than 168 hours, iterative multi-chunk decoding is
used: each 168-hour output chunk becomes the tail of the next encoder
input window.

Preprocessing (inherited from Ashtar SARIMAX):
    - Forward-fill → Winsorization → MinMaxScaler
"""

import logging
from typing import Dict, List

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader, TensorDataset

from . import config as C
from .data_loader import (
    add_autoregressive_lags,
    add_rolling_features,
    create_seq2seq_sequences,
    get_calendar_columns,
    impute_ffill,
    split_fold,
    winsorize_outliers,
)
from .evaluation import compute_metrics

logger = logging.getLogger("baselines.seq2seq")

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# ═══════════════════════════════════════════════════════════════════════════
# Encoder-Decoder Model
# ═══════════════════════════════════════════════════════════════════════════

class Encoder(nn.Module):
    def __init__(self, n_features: int, hidden_size: int):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=n_features,
            hidden_size=hidden_size,
            batch_first=True,
        )

    def forward(self, x: torch.Tensor):
        # x: (batch, input_seq_len, features)
        _, (hidden, cell) = self.lstm(x)
        return hidden, cell


class Decoder(nn.Module):
    def __init__(self, hidden_size: int):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=1,  # receives target value from previous step
            hidden_size=hidden_size,
            batch_first=True,
        )
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x: torch.Tensor, hidden: torch.Tensor, cell: torch.Tensor):
        # x: (batch, 1, 1) — single time step input
        out, (hidden, cell) = self.lstm(x, (hidden, cell))
        pred = self.fc(out.squeeze(1))  # (batch, 1)
        return pred, hidden, cell


class Seq2SeqModel(nn.Module):
    """Encoder-Decoder LSTM with teacher forcing."""

    def __init__(self, n_features: int, cfg: C.Seq2SeqConfig):
        super().__init__()
        self.encoder = Encoder(n_features, cfg.encoder_units)
        self.decoder = Decoder(cfg.decoder_units)
        self.output_length = cfg.output_sequence_length
        self.teacher_forcing_ratio = cfg.teacher_forcing_ratio

    def forward(
        self,
        src: torch.Tensor,
        trg: torch.Tensor = None,
        teacher_forcing: bool = True,
    ) -> torch.Tensor:
        """
        Parameters
        ----------
        src : (batch, input_seq_len, features) — encoder input
        trg : (batch, output_seq_len) — decoder target (for teacher forcing)
        """
        batch_size = src.size(0)
        hidden, cell = self.encoder(src)

        outputs = torch.zeros(batch_size, self.output_length, device=src.device)

        # First decoder input: last target value from encoder sequence
        # Use the last feature (which should be the target) as initial input
        dec_input = src[:, -1, -1:].unsqueeze(1)  # (batch, 1, 1)

        for t in range(self.output_length):
            pred, hidden, cell = self.decoder(dec_input, hidden, cell)
            outputs[:, t] = pred.squeeze(-1)

            if teacher_forcing and trg is not None:
                dec_input = trg[:, t].unsqueeze(1).unsqueeze(2)  # (batch, 1, 1)
            else:
                dec_input = pred.unsqueeze(1)  # (batch, 1, 1)

        return outputs


# ═══════════════════════════════════════════════════════════════════════════
# Full Seq2Seq Pipeline
# ═══════════════════════════════════════════════════════════════════════════

class Seq2SeqBaseline:
    """End-to-end Seq2Seq for a single ROCV fold."""

    def __init__(self, cfg: C.Seq2SeqConfig = None):
        self.cfg = cfg or C.Seq2SeqConfig()

    def _prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add generated features (Ashtar-style)."""
        target = C.TARGET
        sarimax_cfg = C.SARIMAXConfig()
        df = add_rolling_features(df, target, sarimax_cfg.rolling_means_hours)
        df = add_autoregressive_lags(
            df, target, sarimax_cfg.lagged_features_hours, prefix="seq2seq_lag",
        )
        return df

    def _get_feature_cols(self, df: pd.DataFrame) -> List[str]:
        """Feature columns for scaling (excluding target)."""
        cols = []
        for c in df.columns:
            if c.startswith("rolling_mean_") or c.startswith("seq2seq_lag_"):
                cols.append(c)
        cal = get_calendar_columns(df)
        cols.extend(cal)
        return sorted(set(cols))

    def run_fold(
        self,
        df: pd.DataFrame,
        train_end: pd.Timestamp,
        val_end: pd.Timestamp,
        test_origin: pd.Timestamp,
        horizon_days: int,
    ) -> Dict:
        target = C.TARGET
        cfg = self.cfg

        # ── Add features ──
        df_feat = self._prepare_features(df.copy())

        # ── Split ──
        train_df, val_df, test_df = split_fold(
            df_feat, train_end, val_end, test_origin, horizon_days,
            use_timestamp_col=False,
        )

        if len(train_df) == 0 or len(test_df) == 0:
            logger.warning("Empty train (%d) or test (%d).", len(train_df), len(test_df))
            return {"n_train": len(train_df), "n_test": len(test_df)}

        # ── Preprocessing ──
        feature_cols = self._get_feature_cols(train_df)
        all_cols = feature_cols + [target]
        numerical_cols = [c for c in all_cols if c in train_df.columns]

        train_df = impute_ffill(train_df, numerical_cols)
        test_df = impute_ffill(test_df, numerical_cols)
        train_df = winsorize_outliers(train_df, numerical_cols)

        for c in numerical_cols:
            if c in train_df.columns:
                train_df[c] = train_df[c].fillna(0.0)
            if c in test_df.columns:
                test_df[c] = test_df[c].fillna(0.0)

        # ── Scale ──
        scale_cols = [c for c in all_cols if c in train_df.columns]
        scaler = MinMaxScaler(feature_range=(0, 1))
        train_scaled = scaler.fit_transform(
            np.nan_to_num(train_df[scale_cols].values.astype(np.float64), nan=0.0)
        ).astype(np.float32)
        test_scaled = scaler.transform(
            np.nan_to_num(test_df[scale_cols].values.astype(np.float64), nan=0.0)
        ).astype(np.float32)

        target_idx = scale_cols.index(target)
        n_features = len(scale_cols)

        # ── Create sequences ──
        X_train, Y_train = create_seq2seq_sequences(
            train_scaled, cfg.input_sequence_length, cfg.output_sequence_length,
            target_idx=target_idx,
        )

        if len(X_train) == 0:
            logger.warning("No training sequences created.")
            return {"n_train": len(train_df), "n_test": len(test_df)}

        logger.info("Seq2Seq: %d training sequences, input=%d, output=%d, features=%d.",
                     len(X_train), cfg.input_sequence_length,
                     cfg.output_sequence_length, n_features)

        # ── Build & train ──
        model = Seq2SeqModel(n_features, cfg).to(DEVICE)
        optimizer = torch.optim.Adam(model.parameters(), lr=cfg.learning_rate)
        criterion = nn.MSELoss()

        X_train_t = torch.from_numpy(X_train)
        Y_train_t = torch.from_numpy(Y_train)
        train_loader = DataLoader(
            TensorDataset(X_train_t, Y_train_t),
            batch_size=cfg.batch_size, shuffle=False,
        )

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

            if (epoch + 1) % 5 == 0:
                logger.info("Seq2Seq epoch %d/%d, loss=%.6f",
                            epoch + 1, cfg.epochs, epoch_loss / len(X_train))

        # ── Inference: iterative multi-chunk prediction ──
        model.eval()
        n_test_hours = len(test_df)
        horizon_hours = horizon_days * 24
        chunk = cfg.output_sequence_length

        # Seed: last input_seq_length of training data
        seed = train_scaled[-cfg.input_sequence_length:].copy()
        all_preds_scaled = []

        with torch.no_grad():
            remaining = min(n_test_hours, horizon_hours)
            while remaining > 0:
                inp = torch.from_numpy(seed.reshape(1, cfg.input_sequence_length, n_features)).to(DEVICE)
                out = model(inp, teacher_forcing=False)  # (1, output_seq_len)
                chunk_pred = out.cpu().numpy().flatten()

                take = min(len(chunk_pred), remaining)
                all_preds_scaled.extend(chunk_pred[:take])
                remaining -= take

                # Slide seed window
                # Create new rows using predicted values (only target column)
                new_rows = np.zeros((take, n_features), dtype=np.float32)
                new_rows[:, target_idx] = chunk_pred[:take]
                # Copy last known feature values for non-target columns
                for feat_i in range(n_features):
                    if feat_i != target_idx:
                        new_rows[:, feat_i] = seed[-1, feat_i]
                seed = np.concatenate([seed[take:], new_rows], axis=0)

        preds_scaled = np.array(all_preds_scaled[:n_test_hours], dtype=np.float64)

        # ── Inverse transform ──
        dummy = np.zeros((len(preds_scaled), n_features), dtype=np.float64)
        dummy[:, target_idx] = preds_scaled
        y_pred_mw = scaler.inverse_transform(dummy)[:, target_idx]

        y_true_mw = test_df[target].values[:len(y_pred_mw)]

        metrics = compute_metrics(y_true_mw, y_pred_mw)

        return {
            "predictions_mw": y_pred_mw,
            "actuals_mw": y_true_mw,
            "timestamps": test_df.index.values[:len(y_pred_mw)],
            "n_train": len(train_df),
            "n_test": len(y_true_mw),
            "metrics": metrics,
        }
