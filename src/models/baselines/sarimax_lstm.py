"""Model 4: SARIMAX-LSTM residual stacking — Ashtar et al.

Two-stage hybrid model:
    Stage 1: Fit SARIMAX, compute residuals = actual − SARIMAX_predicted.
    Stage 2: PyTorch LSTM with 720-hour (30-day) sliding window learns
             to predict the residuals.
    Inference: final = SARIMAX_prediction + LSTM_residual_prediction.

Preprocessing inherits from the SARIMAX baseline (forward-fill +
Winsorization), with MinMaxScaler applied to features and residuals
before the LSTM.
"""

import logging
from typing import Dict

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader, TensorDataset

from . import config as C
from .data_loader import (
    create_sequences,
    split_fold,
)
from .evaluation import compute_metrics
from .sarimax import SARIMAXBaseline

logger = logging.getLogger("baselines.sarimax_lstm")

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# ═══════════════════════════════════════════════════════════════════════════
# Residual LSTM
# ═══════════════════════════════════════════════════════════════════════════

class ResidualLSTM(nn.Module):
    """LSTM(50, ReLU) → Linear(1) for residual modelling."""

    def __init__(self, n_features: int, hidden_units: int = 50):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=n_features,
            hidden_size=hidden_units,
            batch_first=True,
        )
        self.relu = nn.ReLU()
        self.fc = nn.Linear(hidden_units, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        out = self.relu(out[:, -1, :])
        return self.fc(out).squeeze(-1)


# ═══════════════════════════════════════════════════════════════════════════
# SARIMAX-LSTM Pipeline
# ═══════════════════════════════════════════════════════════════════════════

class SARIMAXLSTMBaseline:
    """Hybrid SARIMAX + residual LSTM for a single ROCV fold."""

    def __init__(
        self,
        sarimax_cfg: C.SARIMAXConfig = None,
        lstm_cfg: C.SARIMAXLSTMConfig = None,
    ):
        self.sarimax_cfg = sarimax_cfg or C.SARIMAXConfig()
        self.lstm_cfg = lstm_cfg or C.SARIMAXLSTMConfig()

    def run_fold(
        self,
        df: pd.DataFrame,
        train_end: pd.Timestamp,
        val_end: pd.Timestamp,
        test_origin: pd.Timestamp,
        horizon_days: int,
    ) -> Dict:
        target = C.TARGET
        cfg = self.lstm_cfg
        seq_len = cfg.input_sequence_length

        # ══════════════════════════════════════════════════════
        # Stage 1: SARIMAX
        # ══════════════════════════════════════════════════════
        sarimax = SARIMAXBaseline(self.sarimax_cfg)
        sarimax_result = sarimax.run_fold(
            df, train_end, val_end, test_origin, horizon_days,
        )

        if "predictions_mw" not in sarimax_result:
            return sarimax_result

        sarimax_pred_test = sarimax_result["predictions_mw"]
        y_true_test = sarimax_result["actuals_mw"]
        n_test = len(y_true_test)

        # Compute in-sample residuals for LSTM training
        sarimax_fit = sarimax_result.get("sarimax_result")
        if sarimax_fit is None:
            return sarimax_result

        # Get in-sample (training) residuals
        train_residuals = sarimax_fit.resid.astype(np.float64)

        # Also get in-sample fitted values to reconstruct
        train_fitted = sarimax_fit.fittedvalues.astype(np.float64)

        if len(train_residuals) < seq_len + 100:
            logger.warning("Not enough training residuals (%d) for LSTM seq_len=%d.",
                          len(train_residuals), seq_len)
            # Fall back to SARIMAX-only predictions
            metrics = compute_metrics(y_true_test, sarimax_pred_test)
            return {
                "predictions_mw": sarimax_pred_test,
                "actuals_mw": y_true_test,
                "timestamps": sarimax_result.get("timestamps"),
                "n_train": sarimax_result["n_train"],
                "n_test": n_test,
                "metrics": metrics,
            }

        # ══════════════════════════════════════════════════════
        # Stage 2: LSTM on residuals
        # ══════════════════════════════════════════════════════
        logger.info("Training residual LSTM on %d residuals (seq_len=%d).",
                     len(train_residuals), seq_len)

        # Scale residuals
        residuals_2d = train_residuals.reshape(-1, 1)
        scaler = MinMaxScaler(feature_range=(0, 1))
        residuals_scaled = scaler.fit_transform(residuals_2d).astype(np.float32)

        # Create sequences
        X_seq, y_seq = create_sequences(residuals_scaled, seq_len, target_idx=0)

        if len(X_seq) == 0:
            logger.warning("No sequences created from residuals.")
            metrics = compute_metrics(y_true_test, sarimax_pred_test)
            return {
                "predictions_mw": sarimax_pred_test,
                "actuals_mw": y_true_test,
                "timestamps": sarimax_result.get("timestamps"),
                "n_train": sarimax_result["n_train"],
                "n_test": n_test,
                "metrics": metrics,
            }

        # Train/val split for LSTM (last 20% as validation)
        split_idx = int(len(X_seq) * 0.8)
        X_train_t = torch.from_numpy(X_seq[:split_idx]).to(DEVICE)
        y_train_t = torch.from_numpy(y_seq[:split_idx]).to(DEVICE)
        X_val_t = torch.from_numpy(X_seq[split_idx:]).to(DEVICE)
        y_val_t = torch.from_numpy(y_seq[split_idx:]).to(DEVICE)

        # Build model
        model = ResidualLSTM(
            n_features=1, hidden_units=cfg.lstm_units,
        ).to(DEVICE)
        optimizer = torch.optim.Adam(model.parameters(), lr=cfg.learning_rate)
        criterion = nn.MSELoss()

        train_loader = DataLoader(
            TensorDataset(X_train_t, y_train_t),
            batch_size=cfg.batch_size, shuffle=False,
        )

        # Train
        best_val_loss = float("inf")
        best_state = None
        patience_counter = 0

        for epoch in range(cfg.epochs):
            model.train()
            for xb, yb in train_loader:
                pred = model(xb)
                loss = criterion(pred, yb)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            model.eval()
            with torch.no_grad():
                val_pred = model(X_val_t)
                val_loss = criterion(val_pred, y_val_t).item()

            if val_loss < best_val_loss - 1e-5:
                best_val_loss = val_loss
                best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= 5:
                    break

        if best_state is not None:
            model.load_state_dict(best_state)
        model.eval()

        # ══════════════════════════════════════════════════════
        # Inference: predict residual corrections for test period
        # ══════════════════════════════════════════════════════
        # Use the last seq_len residuals from training as the initial seed
        # Then iteratively predict residuals for the test period
        test_residuals_pred = np.zeros(n_test, dtype=np.float64)
        seed = residuals_scaled[-seq_len:].copy()

        with torch.no_grad():
            for t in range(n_test):
                inp = torch.from_numpy(seed.reshape(1, seq_len, 1)).to(DEVICE)
                r_scaled = model(inp).cpu().numpy().item()
                # Inverse-scale
                r_mw = scaler.inverse_transform([[r_scaled]])[0, 0]
                test_residuals_pred[t] = r_mw
                # Slide window
                seed = np.roll(seed, -1, axis=0)
                seed[-1, 0] = r_scaled

        # ── Final prediction = SARIMAX + LSTM residual correction ──
        final_pred = sarimax_pred_test + test_residuals_pred

        metrics = compute_metrics(y_true_test, final_pred)

        return {
            "predictions_mw": final_pred,
            "actuals_mw": y_true_test,
            "timestamps": sarimax_result.get("timestamps"),
            "n_train": sarimax_result["n_train"],
            "n_test": n_test,
            "metrics": metrics,
            "sarimax_pred": sarimax_pred_test,
            "residual_pred": test_residuals_pred,
        }
