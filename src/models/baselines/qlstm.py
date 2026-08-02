"""Model 6: Prophet-QLSTM with XGBoost stacking — Curiël et al.

Three-stage hybrid quantum-classical ensemble:

    Stage 1 — **Prophet** (multiplicative seasonality):
        Extract ``[yhat, weekly, yearly, trend]`` components.

    Stage 2 — **QLSTM** (PennyLane + PyTorch):
        Quantum-enhanced LSTM cells where each classical gate
        (forget, input, output, cell) is replaced by a 4-qubit
        variational quantum circuit.

        Architecture:
            QLSTM(hidden=60) → Dropout(0.3) → QLSTM(hidden=120)
            → Linear(ReLU) → Linear(1)

        Backend: ``lightning.qubit`` (C++ accelerated simulator).
        Sequence length: 48 hours.

    Stage 3 — **XGBoost meta-learner**:
        Stacks Prophet base predictions + QLSTM predictions → final.
        XGBRegressor(n_estimators=100, max_depth=3, lr=0.1).

Preprocessing:
    - MICE imputation, IQR clipping (predictors only, target unaltered)
    - MinMaxScaler [0,1]
"""

import logging
import math
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader, TensorDataset

from . import config as C
from .data_loader import (
    clear_outliers_iqr,
    create_sequences,
    fit_scaler,
    apply_scaler,
    get_exogenous_columns,
    impute_mice,
    inverse_transform_target,
    prepare_shifted_dataset,
    split_fold,
)
from .evaluation import compute_metrics

logger = logging.getLogger("baselines.qlstm")

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# ═══════════════════════════════════════════════════════════════════════════
# Quantum LSTM Cell  (PennyLane)
# ═══════════════════════════════════════════════════════════════════════════

class QLSTMCell(nn.Module):
    """Quantum-enhanced LSTM cell.

    Each of the four LSTM gates (forget, input, output, cell) uses a
    variational quantum circuit (VQC) with angle embedding and basic
    entangling layers.  The circuit output (expectation values of
    Pauli-Z on each qubit) is projected back to the gate dimension
    via a linear layer.

    Parameters
    ----------
    input_size : dimension of input at each time step
    hidden_size : LSTM hidden state dimension
    n_qubits : number of qubits per VQC (default 4)
    n_qlayers : number of variational layers (default 1)
    backend : PennyLane device name
    """

    def __init__(
        self,
        input_size: int,
        hidden_size: int,
        n_qubits: int = 4,
        n_qlayers: int = 1,
        backend: str = "lightning.qubit",
    ):
        super().__init__()
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.n_qubits = n_qubits
        self.n_qlayers = n_qlayers

        # Classical input projection: (input + hidden) → n_qubits
        combined = input_size + hidden_size
        self.proj_forget = nn.Linear(combined, n_qubits)
        self.proj_input = nn.Linear(combined, n_qubits)
        self.proj_cell = nn.Linear(combined, n_qubits)
        self.proj_output = nn.Linear(combined, n_qubits)

        # Classical output projection: n_qubits → hidden_size
        self.out_forget = nn.Linear(n_qubits, hidden_size)
        self.out_input = nn.Linear(n_qubits, hidden_size)
        self.out_cell = nn.Linear(n_qubits, hidden_size)
        self.out_output = nn.Linear(n_qubits, hidden_size)

        # Build quantum circuits
        import pennylane as qml

        self.dev = qml.device(backend, wires=n_qubits)

        # Trainable quantum parameters: 4 gates × n_qlayers × n_qubits
        self.q_params = nn.Parameter(
            0.01 * torch.randn(4, n_qlayers, n_qubits)
        )

        @qml.qnode(self.dev, interface="torch", diff_method="adjoint")
        def _circuit(inputs, weights):
            qml.AngleEmbedding(inputs, wires=range(n_qubits))
            qml.BasicEntanglerLayers(weights, wires=range(n_qubits))
            return [qml.expval(qml.PauliZ(i)) for i in range(n_qubits)]

        self.circuit = _circuit

    def forward(
        self,
        x: torch.Tensor,
        hx: Optional[Tuple[torch.Tensor, torch.Tensor]] = None,
    ) -> Tuple[torch.Tensor, Tuple[torch.Tensor, torch.Tensor]]:
        """Process one time step.

        Parameters
        ----------
        x : (batch, input_size)
        hx : tuple of (h, c), each (batch, hidden_size)

        Returns
        -------
        h_new : (batch, hidden_size)
        (h_new, c_new) : updated states
        """
        batch_size = x.size(0)

        if hx is None:
            h = torch.zeros(batch_size, self.hidden_size, device=x.device)
            c = torch.zeros(batch_size, self.hidden_size, device=x.device)
        else:
            h, c = hx

        combined = torch.cat([x, h], dim=1)  # (batch, input+hidden)

        # Project to qubit dimension
        f_in = self.proj_forget(combined)   # (batch, n_qubits)
        i_in = self.proj_input(combined)
        g_in = self.proj_cell(combined)
        o_in = self.proj_output(combined)

        # Apply VQC to each sample in the batch
        f_q = self._batch_circuit(f_in, self.q_params[0])
        i_q = self._batch_circuit(i_in, self.q_params[1])
        g_q = self._batch_circuit(g_in, self.q_params[2])
        o_q = self._batch_circuit(o_in, self.q_params[3])

        # Project back to hidden dimension and apply activations
        f_gate = torch.sigmoid(self.out_forget(f_q))
        i_gate = torch.sigmoid(self.out_input(i_q))
        g_gate = torch.tanh(self.out_cell(g_q))
        o_gate = torch.sigmoid(self.out_output(o_q))

        c_new = f_gate * c + i_gate * g_gate
        h_new = o_gate * torch.tanh(c_new)

        return h_new, (h_new, c_new)

    def _batch_circuit(
        self, inputs: torch.Tensor, weights: torch.Tensor,
    ) -> torch.Tensor:
        """Run the VQC for each sample in the batch.

        Parameters
        ----------
        inputs : (batch, n_qubits) — classical inputs
        weights : (n_qlayers, n_qubits) — trainable parameters

        Returns
        -------
        (batch, n_qubits) — expectation values
        """
        batch_size = inputs.size(0)
        results = []
        for i in range(batch_size):
            out = self.circuit(inputs[i], weights)
            results.append(torch.stack(out))
        return torch.stack(results)  # (batch, n_qubits)


# ═══════════════════════════════════════════════════════════════════════════
# QLSTM Layer (wraps QLSTMCell for sequence processing)
# ═══════════════════════════════════════════════════════════════════════════

class QLSTMLayer(nn.Module):
    """Process a full sequence through QLSTMCell."""

    def __init__(self, input_size, hidden_size, n_qubits=4, n_qlayers=1,
                 backend="lightning.qubit", return_sequences=True):
        super().__init__()
        self.cell = QLSTMCell(input_size, hidden_size, n_qubits, n_qlayers, backend)
        self.return_sequences = return_sequences
        self.hidden_size = hidden_size

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """x: (batch, seq_len, features) → (batch, seq_len, hidden) or (batch, hidden)."""
        batch, seq_len, _ = x.size()
        hx = None
        outputs = []

        for t in range(seq_len):
            h, hx = self.cell(x[:, t, :], hx)
            outputs.append(h)

        if self.return_sequences:
            return torch.stack(outputs, dim=1)  # (batch, seq_len, hidden)
        else:
            return outputs[-1]  # (batch, hidden)


# ═══════════════════════════════════════════════════════════════════════════
# Full QLSTM Network
# ═══════════════════════════════════════════════════════════════════════════

class QLSTMNetwork(nn.Module):
    """QLSTM(60) → Dropout(0.3) → QLSTM(120) → Linear(ReLU) → Linear(1)."""

    def __init__(self, n_features: int, cfg: C.QLSTMConfig):
        super().__init__()
        self.qlstm1 = QLSTMLayer(
            n_features, cfg.hidden_size_1,
            cfg.n_qubits, cfg.n_qlayers, cfg.quantum_backend,
            return_sequences=True,
        )
        self.dropout = nn.Dropout(cfg.dropout_rate)
        self.qlstm2 = QLSTMLayer(
            cfg.hidden_size_1, cfg.hidden_size_2,
            cfg.n_qubits, cfg.n_qlayers, cfg.quantum_backend,
            return_sequences=False,
        )
        self.fc1 = nn.Linear(cfg.hidden_size_2, cfg.hidden_size_2)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(cfg.hidden_size_2, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out = self.qlstm1(x)       # (batch, seq, 60)
        out = self.dropout(out)
        out = self.qlstm2(out)     # (batch, 120)
        out = self.relu(self.fc1(out))
        out = self.fc2(out)        # (batch, 1)
        return out.squeeze(-1)


# ═══════════════════════════════════════════════════════════════════════════
# Prophet Wrapper (Multiplicative)
# ═══════════════════════════════════════════════════════════════════════════

class ProphetMultWrapper:
    """Prophet with multiplicative seasonality for the Curiël pipeline."""

    def __init__(self, cfg: C.ProphetMultConfig = None):
        self.cfg = cfg or C.ProphetMultConfig()

    def fit_predict(
        self,
        train_df: pd.DataFrame,
        test_df: pd.DataFrame,
        val_df: Optional[pd.DataFrame] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[pd.DataFrame], np.ndarray, np.ndarray]:
        """Fit Prophet and return augmented DataFrames + base predictions."""
        from prophet import Prophet

        target = C.TARGET
        ts_col = "timestamp" if "timestamp" in train_df.columns else None

        train_ts = train_df[ts_col].values if ts_col else train_df.index
        test_ts = test_df[ts_col].values if ts_col else test_df.index

        train_prophet = pd.DataFrame({"ds": train_ts, "y": train_df[target].values})
        test_prophet = pd.DataFrame({"ds": test_ts, "y": test_df[target].values})

        if self.cfg.suppress_logging:
            import logging as _log
            for name in ("prophet", "cmdstanpy"):
                _log.getLogger(name).setLevel(_log.WARNING)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            m = Prophet(
                seasonality_mode=self.cfg.seasonality_mode,
                daily_seasonality=self.cfg.daily_seasonality,
                weekly_seasonality=self.cfg.weekly_seasonality,
                yearly_seasonality=self.cfg.yearly_seasonality,
            )
            m.fit(train_prophet)

        components = C.PROPHET_COMPONENTS_MULTIPLICATIVE

        def _extract(df_in, pdf):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                pred = m.predict(pdf)
            out = df_in.copy()
            for c in components:
                out[c] = pred[c].values if c in pred.columns else 0.0
            return out, pred["yhat"].values

        train_out, train_yhat = _extract(train_df, train_prophet)
        test_out, test_yhat = _extract(test_df, test_prophet)

        val_out = None
        if val_df is not None and len(val_df) > 0:
            val_ts = val_df[ts_col].values if ts_col else val_df.index
            val_prophet = pd.DataFrame({"ds": val_ts, "y": val_df[target].values})
            val_out, _ = _extract(val_df, val_prophet)

        return train_out, test_out, val_out, train_yhat, test_yhat


# ═══════════════════════════════════════════════════════════════════════════
# Full Pipeline
# ═══════════════════════════════════════════════════════════════════════════

class ProphetQLSTMXGBBaseline:
    """Prophet-QLSTM-XGBoost stacked ensemble for one ROCV fold."""

    def __init__(
        self,
        prophet_cfg: C.ProphetMultConfig = None,
        qlstm_cfg: C.QLSTMConfig = None,
        xgb_cfg: C.XGBoostStackerConfig = None,
    ):
        self.prophet_cfg = prophet_cfg or C.ProphetMultConfig()
        self.qlstm_cfg = qlstm_cfg or C.QLSTMConfig()
        self.xgb_cfg = xgb_cfg or C.XGBoostStackerConfig()
        self.prophet = ProphetMultWrapper(self.prophet_cfg)

    def run_fold(
        self,
        df_shifted: pd.DataFrame,
        train_end: pd.Timestamp,
        val_end: pd.Timestamp,
        test_origin: pd.Timestamp,
        horizon_days: int,
    ) -> Dict:
        import xgboost as xgb

        target = C.TARGET
        qlstm_cfg = self.qlstm_cfg
        xgb_cfg = self.xgb_cfg

        # ── Split ──
        train_df, val_df, test_df = split_fold(
            df_shifted, train_end, val_end, test_origin, horizon_days,
            use_timestamp_col=True,
        )

        if len(train_df) == 0 or len(test_df) == 0:
            return {"n_train": len(train_df), "n_test": len(test_df)}

        # ── Feature columns (exogenous + lags) ──
        feature_cols = [
            c for c in train_df.columns
            if c not in (target, "timestamp")
            and train_df[c].dtype in (np.float64, np.float32, np.int64, np.int32)
        ]
        predictor_cols = [c for c in feature_cols if c != target]

        # ── IQR on predictors only (target unaltered per Curiël) ──
        train_df = clear_outliers_iqr(train_df, predictor_cols)

        # ── MICE imputation ──
        train_df, test_df = impute_mice(train_df, test_df, predictor_cols + [target])
        if len(val_df) > 0:
            _, val_df = impute_mice(train_df, val_df, predictor_cols + [target])

        # ══════════════════════════════════════════════════════
        # Stage 1: Prophet (multiplicative)
        # ══════════════════════════════════════════════════════
        train_df, test_df, val_df, train_prophet_yhat, test_prophet_yhat = \
            self.prophet.fit_predict(train_df, test_df, val_df if len(val_df) > 0 else None)

        # ── Update features with Prophet components ──
        prophet_cols = [c for c in C.PROPHET_COMPONENTS_MULTIPLICATIVE if c in train_df.columns]
        all_feature_cols = prophet_cols + predictor_cols

        # ── Scale ──
        all_cols_for_scale = all_feature_cols + [target]
        scale_cols = [c for c in all_cols_for_scale if c in train_df.columns]
        scaler = MinMaxScaler(feature_range=(0, 1))
        train_vals = np.nan_to_num(train_df[scale_cols].values.astype(np.float64), nan=0.0)
        train_scaled = scaler.fit_transform(train_vals).astype(np.float32)

        test_vals = np.nan_to_num(test_df[scale_cols].values.astype(np.float64), nan=0.0)
        test_scaled = scaler.transform(test_vals).astype(np.float32)

        target_idx = scale_cols.index(target) if target in scale_cols else -1
        n_features = len(scale_cols)

        # ══════════════════════════════════════════════════════
        # Stage 2: QLSTM
        # ══════════════════════════════════════════════════════
        seq_len = qlstm_cfg.sequence_length

        X_train_seq, y_train_seq = create_sequences(train_scaled, seq_len, target_idx)

        if len(X_train_seq) == 0:
            logger.warning("Not enough data for QLSTM sequences.")
            return {"n_train": len(train_df), "n_test": len(test_df)}

        logger.info("QLSTM: %d sequences, seq_len=%d, features=%d.",
                     len(X_train_seq), seq_len, n_features)

        # Split sequences into train/val
        n_val = max(1, int(len(X_train_seq) * 0.15))
        X_tr = torch.from_numpy(X_train_seq[:-n_val])
        y_tr = torch.from_numpy(y_train_seq[:-n_val])
        X_va = torch.from_numpy(X_train_seq[-n_val:]).to(DEVICE)
        y_va = torch.from_numpy(y_train_seq[-n_val:]).to(DEVICE)

        # Build QLSTM network
        model = QLSTMNetwork(n_features, qlstm_cfg).to(DEVICE)
        optimizer = torch.optim.Adam(model.parameters(), lr=qlstm_cfg.learning_rate)
        criterion = nn.MSELoss()

        train_loader = DataLoader(
            TensorDataset(X_tr, y_tr),
            batch_size=qlstm_cfg.batch_size, shuffle=False,
        )

        # Warmup + early stopping
        best_val_loss = float("inf")
        best_state = None
        patience_counter = 0

        for epoch in range(qlstm_cfg.epochs):
            # Warmup: reduced lr for first few epochs
            if epoch < qlstm_cfg.warmup_epochs:
                for pg in optimizer.param_groups:
                    pg["lr"] = qlstm_cfg.learning_rate * (epoch + 1) / qlstm_cfg.warmup_epochs

            model.train()
            for xb, yb in train_loader:
                xb, yb = xb.to(DEVICE), yb.to(DEVICE)
                pred = model(xb)
                loss = criterion(pred, yb)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            # Validation
            model.eval()
            with torch.no_grad():
                val_pred = model(X_va)
                val_loss = criterion(val_pred, y_va).item()

            if epoch >= qlstm_cfg.early_stop_start_epoch:
                if val_loss < best_val_loss - qlstm_cfg.early_stop_min_delta:
                    best_val_loss = val_loss
                    best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
                    patience_counter = 0
                else:
                    patience_counter += 1
                    if patience_counter >= qlstm_cfg.early_stop_patience:
                        logger.info("QLSTM early stopping at epoch %d.", epoch)
                        break

            if (epoch + 1) % 10 == 0:
                logger.info("QLSTM epoch %d/%d, val_loss=%.6f",
                            epoch + 1, qlstm_cfg.epochs, val_loss)

        if best_state is not None:
            model.load_state_dict(best_state)

        # QLSTM inference on test
        model.eval()
        # Use last seq_len of training as seed, then iteratively predict
        seed = train_scaled[-seq_len:]
        qlstm_preds_scaled = []

        with torch.no_grad():
            for t in range(len(test_df)):
                inp = torch.from_numpy(seed.reshape(1, seq_len, n_features)).to(DEVICE)
                p = model(inp).cpu().numpy().item()
                qlstm_preds_scaled.append(p)
                # Slide window
                new_row = test_scaled[t].copy() if t < len(test_scaled) else np.zeros(n_features, dtype=np.float32)
                new_row[target_idx] = p  # use predicted value
                seed = np.concatenate([seed[1:], new_row.reshape(1, -1)], axis=0)

        # Inverse-transform QLSTM predictions
        qlstm_preds_mw = np.array(qlstm_preds_scaled, dtype=np.float64)
        dummy = np.zeros((len(qlstm_preds_mw), n_features), dtype=np.float64)
        dummy[:, target_idx] = qlstm_preds_mw
        qlstm_preds_mw = scaler.inverse_transform(dummy)[:, target_idx]

        # Prophet base predictions (already in MW)
        prophet_base = test_prophet_yhat[:len(test_df)]

        # ══════════════════════════════════════════════════════
        # Stage 3: XGBoost Stacker
        # ══════════════════════════════════════════════════════
        # Build stacking features on validation set
        # For training the stacker, we use the QLSTM's in-sample predictions
        # from the validation portion
        if val_df is not None and len(val_df) > 0:
            # Get Prophet predictions for validation
            val_prophet_preds = train_prophet_yhat[-len(val_df):]

            # Get QLSTM predictions for validation (in-sample from training)
            # Use the validation sequences
            val_scaled = scaler.transform(
                np.nan_to_num(val_df[scale_cols].values.astype(np.float64), nan=0.0)
            ).astype(np.float32)
            X_val_seq, y_val_seq = create_sequences(val_scaled, seq_len, target_idx)

            if len(X_val_seq) > 0:
                with torch.no_grad():
                    qlstm_val = model(
                        torch.from_numpy(X_val_seq).to(DEVICE)
                    ).cpu().numpy()
                # Inverse-transform
                dummy_val = np.zeros((len(qlstm_val), n_features), dtype=np.float64)
                dummy_val[:, target_idx] = qlstm_val
                qlstm_val_mw = scaler.inverse_transform(dummy_val)[:, target_idx]

                # Align Prophet predictions with sequence offsets
                val_prophet_aligned = val_prophet_preds[seq_len:seq_len + len(qlstm_val_mw)]
                y_val_true = val_df[target].values[seq_len:seq_len + len(qlstm_val_mw)]

                # Stack features
                X_stack_train = np.column_stack([val_prophet_aligned, qlstm_val_mw])
                y_stack_train = y_val_true
            else:
                # Fallback: skip stacker, return QLSTM predictions directly
                metrics = compute_metrics(test_df[target].values, qlstm_preds_mw)
                return {
                    "predictions_mw": qlstm_preds_mw,
                    "actuals_mw": test_df[target].values,
                    "n_train": len(train_df), "n_test": len(test_df),
                    "metrics": metrics,
                }
        else:
            # No validation set — skip stacker
            metrics = compute_metrics(test_df[target].values, qlstm_preds_mw)
            return {
                "predictions_mw": qlstm_preds_mw,
                "actuals_mw": test_df[target].values,
                "n_train": len(train_df), "n_test": len(test_df),
                "metrics": metrics,
            }

        # Fit XGBoost stacker
        xgb_model = xgb.XGBRegressor(
            n_estimators=xgb_cfg.n_estimators,
            max_depth=xgb_cfg.max_depth,
            learning_rate=xgb_cfg.learning_rate,
            random_state=xgb_cfg.random_state,
        )
        xgb_model.fit(X_stack_train, y_stack_train)
        logger.info("XGBoost stacker fitted on %d samples.", len(X_stack_train))

        # Stack test predictions
        n_test = min(len(prophet_base), len(qlstm_preds_mw))
        X_stack_test = np.column_stack([
            prophet_base[:n_test],
            qlstm_preds_mw[:n_test],
        ])
        final_pred = xgb_model.predict(X_stack_test)

        y_true = test_df[target].values[:n_test]
        metrics = compute_metrics(y_true, final_pred)

        return {
            "predictions_mw": final_pred,
            "actuals_mw": y_true,
            "timestamps": test_df["timestamp"].values[:n_test] if "timestamp" in test_df.columns else None,
            "n_train": len(train_df),
            "n_test": n_test,
            "metrics": metrics,
            "prophet_base": prophet_base[:n_test],
            "qlstm_pred": qlstm_preds_mw[:n_test],
        }
