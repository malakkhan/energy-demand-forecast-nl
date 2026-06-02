"""Model 2: Prophet-LSTM ensemble — van de Sande et al. (PyTorch).

Architecture (Huang / Model 1 — no dropout):

    Stage 1 — **Prophet**: fitted on training data with additive
    seasonality (daily + weekly + yearly).  Produces five component
    columns: yhat, trend, daily, weekly, yearly.

    Stage 2 — **PyTorch LSTM**:
        LSTM(30, return_sequences) → LSTM(90) → Linear(1)
        Optimizer = Adam, loss = MSE, seq_len = 1 (per-sample).
        Early stopping: patience=3, min_delta=1e-4, start_from_epoch=10.

    No exogenous predictors — only Prophet components + AR lags.
    MinMaxScaler [0,1] fitted on train, applied to val/test.
"""

import logging
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

from . import config as C
from .data_loader import (
    clear_outliers_iqr,
    fit_scaler,
    apply_scaler,
    inverse_transform_target,
    prepare_shifted_dataset,
    split_fold,
)
from .evaluation import compute_metrics

logger = logging.getLogger("baselines.prophet_lstm_pytorch")

# Auto-detect device
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# ═══════════════════════════════════════════════════════════════════════════
# PyTorch LSTM — Huang Model 1
# ═══════════════════════════════════════════════════════════════════════════

class LSTMHuang(nn.Module):
    """LSTM(30) → LSTM(90) → Linear(1).  No dropout."""

    def __init__(self, n_features: int, cfg: C.LSTMConfig = None):
        super().__init__()
        cfg = cfg or C.LSTMConfig()
        self.lstm1 = nn.LSTM(
            input_size=n_features, hidden_size=cfg.lstm1_units,
            batch_first=True,
        )
        self.lstm2 = nn.LSTM(
            input_size=cfg.lstm1_units, hidden_size=cfg.lstm2_units,
            batch_first=True,
        )
        self.fc = nn.Linear(cfg.lstm2_units, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len=1, features)
        out, _ = self.lstm1(x)           # (batch, 1, 30)
        out, _ = self.lstm2(out)         # (batch, 1, 90)
        out = out[:, -1, :]              # (batch, 90)  — last time step
        out = self.fc(out)               # (batch, 1)
        return out.squeeze(-1)


# ═══════════════════════════════════════════════════════════════════════════
# Early Stopping
# ═══════════════════════════════════════════════════════════════════════════

class EarlyStopping:
    """Early stopping with patience and start-from-epoch guard."""

    def __init__(self, patience: int, min_delta: float, start_epoch: int):
        self.patience = patience
        self.min_delta = min_delta
        self.start_epoch = start_epoch
        self.best_loss = float("inf")
        self.counter = 0
        self.best_state = None

    def step(self, epoch: int, val_loss: float, model: nn.Module) -> bool:
        """Returns True when training should stop."""
        if epoch < self.start_epoch:
            return False

        if val_loss < self.best_loss - self.min_delta:
            self.best_loss = val_loss
            self.counter = 0
            self.best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            return False
        else:
            self.counter += 1
            return self.counter >= self.patience

    def restore(self, model: nn.Module) -> None:
        if self.best_state is not None:
            model.load_state_dict(self.best_state)


# ═══════════════════════════════════════════════════════════════════════════
# Prophet Wrapper
# ═══════════════════════════════════════════════════════════════════════════

class ProphetWrapper:
    """Fit Prophet, extract seasonal components."""

    def __init__(self, cfg: C.ProphetConfig = None):
        self.cfg = cfg or C.ProphetConfig()

    def fit_predict(
        self,
        train_df: pd.DataFrame,
        test_df: pd.DataFrame,
        val_df: Optional[pd.DataFrame] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame]]:
        """Fit Prophet on train; predict components for all splits.

        Returns DataFrames with Prophet component columns appended.
        """
        from prophet import Prophet

        target = C.TARGET

        # Prepare Prophet input
        ts_col = "timestamp" if "timestamp" in train_df.columns else None
        if ts_col:
            train_ts = train_df[ts_col].values
            test_ts = test_df[ts_col].values
        else:
            train_ts = train_df.index
            test_ts = test_df.index

        train_prophet = pd.DataFrame({"ds": train_ts, "y": train_df[target].values})
        test_prophet = pd.DataFrame({"ds": test_ts, "y": test_df[target].values})

        # Suppress Prophet logs
        if self.cfg.suppress_logging:
            import logging as _log
            for name in ("prophet", "cmdstanpy"):
                _log.getLogger(name).setLevel(_log.WARNING)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            m = Prophet(
                daily_seasonality=self.cfg.daily_seasonality,
                weekly_seasonality=self.cfg.weekly_seasonality,
                yearly_seasonality=self.cfg.yearly_seasonality,
            )
            if self.cfg.add_country_holidays:
                m.add_country_holidays(country_name=self.cfg.add_country_holidays)
            m.fit(train_prophet)

        logger.info("Prophet fitted on %d rows.", len(train_prophet))

        # Predict components
        def _extract(df_in, prophet_df):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                pred = m.predict(prophet_df)
            components = C.PROPHET_COMPONENTS_ADDITIVE
            out = df_in.copy()
            for c in components:
                out[c] = pred[c].values if c in pred.columns else 0.0
            return out

        train_out = _extract(train_df, train_prophet)
        test_out = _extract(test_df, test_prophet)

        val_out = None
        if val_df is not None and len(val_df) > 0:
            val_ts = val_df[ts_col].values if ts_col else val_df.index
            val_prophet = pd.DataFrame({"ds": val_ts, "y": val_df[target].values})
            val_out = _extract(val_df, val_prophet)

        return train_out, test_out, val_out


# ═══════════════════════════════════════════════════════════════════════════
# Full Prophet-LSTM Pipeline
# ═══════════════════════════════════════════════════════════════════════════

class ProphetLSTMPyTorch:
    """End-to-end Prophet → LSTM ensemble for a single ROCV fold."""

    def __init__(
        self,
        prophet_cfg: C.ProphetConfig = None,
        lstm_cfg: C.LSTMConfig = None,
    ):
        self.prophet_cfg = prophet_cfg or C.ProphetConfig()
        self.lstm_cfg = lstm_cfg or C.LSTMConfig()
        self.prophet = ProphetWrapper(self.prophet_cfg)

    def run_fold(
        self,
        df_shifted: pd.DataFrame,
        train_end: pd.Timestamp,
        val_end: pd.Timestamp,
        test_origin: pd.Timestamp,
        horizon_days: int,
    ) -> Dict:
        target = C.TARGET
        cfg = self.lstm_cfg

        # ── Split ──
        train_df, val_df, test_df = split_fold(
            df_shifted, train_end, val_end, test_origin, horizon_days,
            use_timestamp_col=True,
        )
        if len(train_df) == 0 or len(test_df) == 0:
            logger.warning("Empty train (%d) or test (%d).", len(train_df), len(test_df))
            return {"n_train": len(train_df), "n_test": len(test_df)}

        # ── Identify predictor cols (lags only, no exogenous) ──
        lag_cols = [c for c in train_df.columns
                    if c.startswith("load_lag_") or c.endswith("_lag")]

        # ── Drop lag cols that are mostly NaN ──
        usable_lags = [c for c in lag_cols
                       if c in train_df.columns and train_df[c].isna().mean() <= 0.5]
        if len(usable_lags) < len(lag_cols):
            logger.info("Dropped %d/%d lag cols (>50%% NaN).",
                        len(lag_cols) - len(usable_lags), len(lag_cols))
        lag_cols = usable_lags

        numerical_cols = lag_cols + [target]

        # ── IQR outlier removal (train only) ──
        train_df = clear_outliers_iqr(train_df, numerical_cols)

        # ── Fast imputation: ffill → bfill → median ──
        for col in lag_cols:
            med = train_df[col].median()
            fill_val = med if np.isfinite(med) else 0.0
            for df_part in [train_df, test_df, val_df]:
                if df_part is not None and col in df_part.columns:
                    df_part[col] = df_part[col].ffill().bfill().fillna(fill_val)

        # Drop NaN target rows
        train_df = train_df.dropna(subset=[target])
        test_df = test_df.dropna(subset=[target])

        if len(train_df) == 0 or len(test_df) == 0:
            logger.warning("Empty after imputation: train=%d, test=%d.",
                           len(train_df), len(test_df))
            return {"n_train": len(train_df), "n_test": len(test_df)}

        # ── Prophet ──
        train_df, test_df, val_df = self.prophet.fit_predict(
            train_df, test_df, val_df if (val_df is not None and len(val_df) > 0) else None,
        )

        # ── Feature columns for LSTM ──
        prophet_cols = [c for c in C.PROPHET_COMPONENTS_ADDITIVE if c in train_df.columns]
        feature_cols = prophet_cols + lag_cols
        all_cols_for_scale = feature_cols + [target]

        # ── Scale ──
        scaler, col_order = fit_scaler(train_df, all_cols_for_scale, target)

        train_scaled = apply_scaler(train_df, scaler, col_order)
        test_scaled = apply_scaler(test_df, scaler, col_order)

        # Reshape for seq_len=1: (N, 1, F)
        n_features = len(col_order) - 1  # exclude target
        train_X = train_scaled[:, :-1].reshape(-1, 1, n_features).astype(np.float32)
        train_y = train_scaled[:, -1].astype(np.float32)
        test_X = test_scaled[:, :-1].reshape(-1, 1, n_features).astype(np.float32)
        test_y = test_scaled[:, -1].astype(np.float32)

        # Validation set
        if val_df is not None and len(val_df) > 0:
            val_scaled = apply_scaler(val_df, scaler, col_order)
            val_X = val_scaled[:, :-1].reshape(-1, 1, n_features).astype(np.float32)
            val_y = val_scaled[:, -1].astype(np.float32)
        else:
            val_X, val_y = test_X, test_y

        logger.info("LSTM input: train=%s, val=%s, test=%s, features=%d.",
                     train_X.shape, val_X.shape, test_X.shape, n_features)

        # ── Build & train LSTM ──
        model = LSTMHuang(n_features, cfg).to(DEVICE)
        optimizer = torch.optim.Adam(model.parameters(), lr=cfg.learning_rate)
        criterion = nn.MSELoss()
        es = EarlyStopping(cfg.early_stop_patience, cfg.early_stop_min_delta,
                           cfg.early_stop_start_epoch)

        train_loader = DataLoader(
            TensorDataset(torch.from_numpy(train_X), torch.from_numpy(train_y)),
            batch_size=cfg.batch_size, shuffle=False,
        )
        val_tensor_X = torch.from_numpy(val_X).to(DEVICE)
        val_tensor_y = torch.from_numpy(val_y).to(DEVICE)

        for epoch in range(cfg.epochs):
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

            # Validation
            model.eval()
            with torch.no_grad():
                val_pred = model(val_tensor_X)
                val_loss = criterion(val_pred, val_tensor_y).item()

            # NaN guard
            if not np.isfinite(epoch_loss) or not np.isfinite(val_loss):
                logger.warning("NaN/Inf loss at epoch %d — stopping.", epoch)
                break

            # Progress logging
            if epoch % 10 == 0 or epoch == cfg.epochs - 1:
                logger.info("  Epoch %3d/%d — train_loss=%.6f  val_loss=%.6f",
                            epoch, cfg.epochs, epoch_loss, val_loss)

            if es.step(epoch, val_loss, model):
                logger.info("Early stopping at epoch %d (val_loss=%.6f).", epoch, val_loss)
                break

        es.restore(model)

        # ── Predict ──
        model.eval()
        with torch.no_grad():
            test_tensor = torch.from_numpy(test_X).to(DEVICE)
            yhat_scaled = model(test_tensor).cpu().numpy()

        # ── Inverse transform ──
        yhat_mw = inverse_transform_target(scaler, yhat_scaled, col_order, target)
        actual_mw = inverse_transform_target(scaler, test_y, col_order, target)

        metrics = compute_metrics(actual_mw, yhat_mw)
        test_timestamps = test_df["timestamp"].values if "timestamp" in test_df.columns else None

        return {
            "predictions_mw": yhat_mw,
            "actuals_mw": actual_mw,
            "timestamps": test_timestamps,
            "n_train": len(train_df),
            "n_test": len(test_df),
            "metrics": metrics,
        }
