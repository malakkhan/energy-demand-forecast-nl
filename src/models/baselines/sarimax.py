"""Model 3: SARIMAX — Ashtar et al.

Statsmodels SARIMAX with generated temporal features.
Order = (1,0,1), seasonal order = (1,0,1,168) for weekly seasonality
on hourly data.

Preprocessing:
    - Forward-fill missing values
    - Winsorization at 1st/99th percentiles
    - Feature engineering: rolling means (7d, 30d), lagged features
      (1h, 24h, 168h), calendar flags

To keep computation tractable, the training window is capped at the
most recent ``max_train_hours`` hours (default 2 years).
"""

import logging
from typing import Dict, List

import numpy as np
import pandas as pd

from . import config as C
from .data_loader import (
    add_autoregressive_lags,
    add_calendar_features,
    add_rolling_features,
    get_calendar_columns,
    impute_ffill,
    split_fold,
    winsorize_outliers,
)
from .evaluation import compute_metrics

logger = logging.getLogger("baselines.sarimax")


class SARIMAXBaseline:
    """SARIMAX model for a single ROCV fold + horizon."""

    def __init__(self, cfg: C.SARIMAXConfig = None):
        self.cfg = cfg or C.SARIMAXConfig()

    def _build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add generated temporal features (Ashtar method)."""
        target = C.TARGET

        # Rolling means
        df = add_rolling_features(df, target, self.cfg.rolling_means_hours)

        # Lagged features
        df = add_autoregressive_lags(
            df, target, self.cfg.lagged_features_hours, prefix="sarimax_lag",
        )

        return df

    def _get_exog_cols(self, df: pd.DataFrame) -> List[str]:
        """Return the exogenous column names for SARIMAX."""
        cols = []
        # Rolling means
        for w in self.cfg.rolling_means_hours:
            c = f"rolling_mean_{w}h"
            if c in df.columns:
                cols.append(c)
        # Lags
        for lag in self.cfg.lagged_features_hours:
            c = f"sarimax_lag_{lag}h"
            if c in df.columns:
                cols.append(c)
        # Calendar features
        cal = get_calendar_columns(df)
        cols.extend(cal)
        return cols

    def run_fold(
        self,
        df: pd.DataFrame,
        train_end: pd.Timestamp,
        val_end: pd.Timestamp,
        test_origin: pd.Timestamp,
        horizon_days: int,
    ) -> Dict:
        """Execute one fold of SARIMAX training and forecasting."""
        from statsmodels.tsa.statespace.sarimax import SARIMAX

        target = C.TARGET
        cfg = self.cfg

        # ── Build features on full dataset ──
        df_feat = self._build_features(df.copy())

        # ── Split ──
        train_df, val_df, test_df = split_fold(
            df_feat, train_end, val_end, test_origin, horizon_days,
            use_timestamp_col=False,  # uses DatetimeIndex
        )

        if len(train_df) == 0 or len(test_df) == 0:
            logger.warning("Empty train (%d) or test (%d).", len(train_df), len(test_df))
            return {"n_train": len(train_df), "n_test": len(test_df)}

        # ── Preprocessing ──
        exog_cols = self._get_exog_cols(train_df)
        numerical_cols = exog_cols + [target]

        # Forward-fill
        train_df = impute_ffill(train_df, numerical_cols)
        test_df = impute_ffill(test_df, numerical_cols)

        # Winsorize
        train_df = winsorize_outliers(train_df, numerical_cols)

        # Fill any remaining NaN
        for c in numerical_cols:
            if c in train_df.columns:
                train_df[c] = train_df[c].fillna(0.0)
            if c in test_df.columns:
                test_df[c] = test_df[c].fillna(0.0)

        # ── Cap training window ──
        if len(train_df) > cfg.max_train_hours:
            train_df = train_df.iloc[-cfg.max_train_hours:]
            logger.info("Capped SARIMAX training to last %d hours.", cfg.max_train_hours)

        # ── Fit SARIMAX ──
        logger.info(
            "Fitting SARIMAX(%s, %s) on %d rows with %d exog features…",
            cfg.order, cfg.seasonal_order, len(train_df), len(exog_cols),
        )

        exog_train = train_df[exog_cols].values if exog_cols else None
        exog_test = test_df[exog_cols].values if exog_cols else None

        try:
            model = SARIMAX(
                endog=train_df[target].values,
                exog=exog_train,
                order=cfg.order,
                seasonal_order=cfg.seasonal_order,
                enforce_stationarity=cfg.enforce_stationarity,
                enforce_invertibility=cfg.enforce_invertibility,
            )
            result = model.fit(
                disp=False,
                maxiter=cfg.maxiter,
                method="lbfgs",
            )
            logger.info("SARIMAX fitted. AIC=%.1f", result.aic)
        except Exception as e:
            logger.error("SARIMAX fitting failed: %s", e)
            return {
                "n_train": len(train_df),
                "n_test": len(test_df),
                "error": str(e),
            }

        # ── Forecast ──
        n_forecast = len(test_df)
        try:
            forecast = result.forecast(steps=n_forecast, exog=exog_test)
            y_pred = np.asarray(forecast, dtype=np.float64)
        except Exception as e:
            logger.error("SARIMAX forecast failed: %s", e)
            return {
                "n_train": len(train_df),
                "n_test": len(test_df),
                "error": str(e),
            }

        y_true = test_df[target].values

        # ── Evaluate ──
        metrics = compute_metrics(y_true, y_pred)

        return {
            "predictions_mw": y_pred,
            "actuals_mw": y_true,
            "timestamps": test_df.index.values if hasattr(test_df.index, 'values') else None,
            "n_train": len(train_df),
            "n_test": len(test_df),
            "metrics": metrics,
            "aic": result.aic,
            "residuals": result.resid,
            "sarimax_result": result,
        }
