"""Prophet-LSTM ensemble model — faithful to the reference implementation.

Architecture (from the reference paper / reference code):

    1. **Prophet** is fitted on the training fold with lagged predictors
       as regressors.  Prophet produces predictions (yhat) and
       components (trend, daily, weekly, yearly) for both train and test.

    2. The Prophet outputs + original lagged predictors + target are
       combined into a feature matrix.

    3. **MinMaxScaler** is fitted on training data only, then applied to
       both train and test.

    4. **LSTM** processes each sample as a single time-step
       (seq_len = 1, matching the reference's ``reshape(n, 1, features)``).

    Model 1 (Huang):
        LSTM(30, relu, return_seq) → LSTM(90, relu) → Dense(1)
        optimizer=adam

    Model 2 (Tan):
        LSTM(30, sigmoid, return_seq) → Dropout(0.2) →
        LSTM(60, tanh) → Dropout(0.3) → Dense(1, linear)
        optimizer=rmsprop

    Early stopping: patience=3, min_delta=0.0001, start_from_epoch=8,
    monitor=val_loss.  Validation data = test data (as in reference).
"""

import logging
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from . import config as C
from .data_loader import (
    clear_outliers,
    get_predictor_columns,
    impute_missing,
    inverse_transform_predictions,
    scale_data,
    shift_data,
)

logger = logging.getLogger("baselines.prophet_lstm")


# ═══════════════════════════════════════════════════════════════════════════
# Prophet wrapper
# ═══════════════════════════════════════════════════════════════════════════

class ProphetWrapper:
    """Fit Prophet with lagged predictors as regressors."""

    def __init__(self, cfg: C.ProphetConfig = None):
        self.cfg = cfg or C.ProphetConfig()
        self.model = None

    def fit_predict(self, train_df: pd.DataFrame,
                    test_df: pd.DataFrame,
                    predictor_cols: List[str]
                    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Fit Prophet on training data, predict on both train and test.

        Matches the reference: Prophet receives lagged predictors as
        regressors, is trained on train data, and predicts both sets.

        Parameters
        ----------
        train_df / test_df : DataFrames with columns:
            ``timestamp``, ``{TARGET}``, and lagged predictor columns.
        predictor_cols : list of lagged predictor column names to add
            as Prophet regressors.

        Returns
        -------
        df_train_lstm, df_test_lstm : DataFrames with columns:
            [yhat, trend, daily, weekly, yearly] + predictor_cols + target
        """
        from prophet import Prophet

        target = C.TARGET

        # ── Prepare Prophet DataFrames ──
        train_prophet = pd.DataFrame({
            "ds": train_df["timestamp"].values,
            "y": train_df[target].values,
        })
        test_prophet = pd.DataFrame({
            "ds": test_df["timestamp"].values,
            "y": test_df[target].values,
        })

        # Add lagged predictors as regressors
        for col in predictor_cols:
            if col in train_df.columns:
                train_prophet[col] = train_df[col].values
            if col in test_df.columns:
                test_prophet[col] = test_df[col].values

        # Fill NaN in regressor columns (Prophet cannot handle NaN regressors)
        for col in predictor_cols:
            if col in train_prophet.columns:
                train_prophet[col] = train_prophet[col].fillna(0.0)
            if col in test_prophet.columns:
                test_prophet[col] = test_prophet[col].fillna(0.0)

        # ── Initialize and fit Prophet ──
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
                m.add_country_holidays(
                    country_name=self.cfg.add_country_holidays
                )

            # Add each predictor as a regressor
            for col in predictor_cols:
                if col in train_prophet.columns:
                    m.add_regressor(col)

            m.fit(train_prophet)

        self.model = m
        logger.info("Prophet fitted on %d rows with %d regressors.",
                     len(train_prophet), len(predictor_cols))

        # ── Predict on both train and test ──
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            pred_train = m.predict(train_prophet)
            pred_test = m.predict(test_prophet)

        # ── Combine Prophet outputs with predictors + target ──
        prophet_cols = ["yhat", "trend", "daily", "weekly", "yearly"]

        df_train_lstm = self._combine(pred_train, train_prophet,
                                      prophet_cols, predictor_cols)
        df_test_lstm = self._combine(pred_test, test_prophet,
                                     prophet_cols, predictor_cols)

        return df_train_lstm, df_test_lstm

    @staticmethod
    def _combine(prophet_forecast: pd.DataFrame,
                 original_df: pd.DataFrame,
                 prophet_cols: List[str],
                 predictor_cols: List[str]) -> pd.DataFrame:
        """Combine Prophet predictions with original predictors and target.

        Matches reference: ``combine_prophet_predictions_and_target``.
        """
        result = pd.DataFrame()
        result["ds"] = prophet_forecast["ds"].values

        # Prophet components
        for col in prophet_cols:
            if col in prophet_forecast.columns:
                result[col] = prophet_forecast[col].values
            else:
                result[col] = 0.0

        # Original lagged predictors
        for col in predictor_cols:
            if col in original_df.columns:
                result[col] = original_df[col].values

        # Target (renamed to loadConsumption to match reference pattern)
        result[C.TARGET] = original_df["y"].values

        return result


# ═══════════════════════════════════════════════════════════════════════════
# LSTM model (TensorFlow/Keras — matching reference implementation)
# ═══════════════════════════════════════════════════════════════════════════

def _build_lstm_huang(n_features: int, cfg: C.LSTMConfig):
    """Model 1 (Huang): LSTM(30,relu) → LSTM(90,relu) → Dense(1).

    Matches reference ``choose_model_setup(huang=True)``.
    """
    import tensorflow as tf
    from tensorflow.keras.layers import Dense, InputLayer, LSTM
    from tensorflow.keras.models import Sequential

    model = Sequential([
        InputLayer(input_shape=(1, n_features)),
        LSTM(cfg.lstm1_units, activation="relu", return_sequences=True),
        LSTM(cfg.lstm2_units_huang, activation="relu"),
        Dense(1),
    ])
    model.compile(optimizer="adam", loss="mse")
    return model


def _build_lstm_tan(n_features: int, cfg: C.LSTMConfig):
    """Model 2 (Tan): LSTM(30,sigmoid) → Drop(0.2) → LSTM(60,tanh)
    → Drop(0.3) → Dense(1,linear).

    Matches reference ``choose_model_setup(tan=True)``.
    """
    import tensorflow as tf
    from tensorflow.keras.layers import Dense, Dropout, InputLayer, LSTM
    from tensorflow.keras.models import Sequential

    model = Sequential([
        InputLayer(input_shape=(1, n_features)),
        LSTM(cfg.lstm1_units, activation="sigmoid", return_sequences=True),
        Dropout(rate=cfg.dropout_rate_1),
        LSTM(cfg.lstm2_units_tan, activation="tanh"),
        Dropout(rate=cfg.dropout_rate_2),
        Dense(1, activation="linear"),
    ])
    model.compile(optimizer="rmsprop", loss="mse")
    return model


def fit_lstm(model, train_X, train_y, test_X, test_y,
             cfg: C.LSTMConfig):
    """Fit LSTM with early stopping.  Validation data = test data.

    Matches reference ``fit_model`` function exactly:
    - epochs=50, batch_size=128
    - EarlyStopping(monitor='val_loss', patience=3,
                    start_from_epoch=8, min_delta=0.0001)
    - shuffle=False
    """
    import tensorflow as tf
    from tensorflow.keras.callbacks import EarlyStopping

    callback = EarlyStopping(
        monitor="val_loss",
        patience=cfg.early_stop_patience,
        min_delta=cfg.early_stop_min_delta,
        start_from_epoch=cfg.early_stop_start_epoch,
        restore_best_weights=True,
    )

    history = model.fit(
        train_X, train_y,
        epochs=cfg.epochs,
        batch_size=cfg.batch_size,
        validation_data=(test_X, test_y),
        verbose=2,
        shuffle=False,
        callbacks=[callback],
    )

    final_epoch = len(history.history["loss"])
    final_train_loss = history.history["loss"][-1]
    final_val_loss = history.history["val_loss"][-1]
    logger.info("LSTM training complete: %d epochs, "
                "train_loss=%.6f, val_loss=%.6f.",
                final_epoch, final_train_loss, final_val_loss)

    return model, history


# ═══════════════════════════════════════════════════════════════════════════
# Full Prophet-LSTM pipeline
# ═══════════════════════════════════════════════════════════════════════════

class ProphetLSTM:
    """End-to-end Prophet → LSTM ensemble for a single ROCV fold.

    Usage::

        model = ProphetLSTM(model_variant="huang")
        results = model.run_fold(df_shifted, cutoff, horizon_days)
    """

    def __init__(self, *,
                 model_variant: str = "huang",  # "huang" or "tan"
                 prophet_cfg: C.ProphetConfig = None,
                 lstm_cfg: C.LSTMConfig = None):
        self.model_variant = model_variant
        self.prophet_cfg = prophet_cfg or C.ProphetConfig()
        self.lstm_cfg = lstm_cfg or C.LSTMConfig()
        self.prophet = ProphetWrapper(self.prophet_cfg)

    def run_fold(self, df_shifted: pd.DataFrame,
                 cutoff: pd.Timestamp,
                 horizon_days: int
                 ) -> Dict:
        """Execute a complete fold: split → outliers → impute → Prophet
        → scale → LSTM → evaluate.

        Parameters
        ----------
        df_shifted : DataFrame with shifted/lagged columns (output of
            ``shift_data``).  Must have a ``timestamp`` column.
        cutoff : end of training period (exclusive).
        horizon_days : prediction horizon in days.

        Returns
        -------
        dict with predictions, actuals, timestamps, metrics, scaler, etc.
        """
        target = C.TARGET
        test_end = cutoff + pd.Timedelta(days=horizon_days)

        # ── Split into train / test ──
        train_df = df_shifted[
            df_shifted["timestamp"] < cutoff
        ].copy().reset_index(drop=True)
        test_df = df_shifted[
            (df_shifted["timestamp"] >= cutoff) &
            (df_shifted["timestamp"] < test_end)
        ].copy().reset_index(drop=True)

        if len(train_df) == 0 or len(test_df) == 0:
            logger.warning("Empty train (%d) or test (%d) set.",
                           len(train_df), len(test_df))
            return {"n_train": len(train_df), "n_test": len(test_df)}

        # ── Identify numerical predictor columns ──
        predictor_cols = [c for c in train_df.columns
                         if c.endswith("_lag") or c.startswith("load_lag")]
        numerical_cols = predictor_cols + [target]

        # ── IQR outlier removal (training only) ──
        train_df = clear_outliers(train_df, numerical_cols)

        # ── MICE imputation ──
        train_df, test_df = impute_missing(train_df, test_df, numerical_cols)

        # ── Prophet ──
        df_train_lstm, df_test_lstm = self.prophet.fit_predict(
            train_df, test_df, predictor_cols
        )

        # ── Get feature columns for LSTM ──
        # Reference pattern: [prophet_components, predictors, target]
        prophet_comp = [c for c in C.PROPHET_COMPONENTS
                        if c in df_train_lstm.columns]
        feature_cols = prophet_comp + predictor_cols
        all_cols = feature_cols + [target]

        # ── MinMaxScale ──
        train_X, train_y, test_X, test_y, scaler, col_order = scale_data(
            df_train_lstm, df_test_lstm, all_cols, target
        )

        logger.info("LSTM input: train=%s, test=%s, features=%d.",
                     train_X.shape, test_X.shape, train_X.shape[2])

        # ── Build and train LSTM ──
        n_features = train_X.shape[2]
        if self.model_variant == "huang":
            model = _build_lstm_huang(n_features, self.lstm_cfg)
        else:
            model = _build_lstm_tan(n_features, self.lstm_cfg)

        model, history = fit_lstm(
            model, train_X, train_y, test_X, test_y, self.lstm_cfg
        )

        # ── Predict ──
        yhat_scaled = model.predict(test_X, verbose=0)
        yhat_mw = inverse_transform_predictions(
            scaler, yhat_scaled, target, col_order
        )

        # ── Inverse-transform actual test values ──
        actual_mw = inverse_transform_predictions(
            scaler, test_y, target, col_order
        )

        # ── Timestamps for the test set ──
        test_timestamps = df_test_lstm["ds"].values

        return {
            "predictions_mw": yhat_mw,
            "actuals_mw": actual_mw,
            "timestamps": test_timestamps,
            "n_train": len(train_df),
            "n_test": len(test_df),
            "scaler": scaler,
            "col_order": col_order,
            "history": history.history if history else {},
        }
