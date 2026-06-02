"""Model 1: Multiple Linear Regression — van de Sande et al.

The simplest baseline.  A standard OLS regression fitted on all
available exogenous predictors (weather + CBS economic) plus
autoregressive load lags (24 h, 7 d, 12 m).

Preprocessing pipeline per fold:
    1. Shift predictors by ``horizon_days × 24`` hours
    2. Drop columns where >50 % of training data is NaN (no signal)
    3. IQR outlier removal on training data only
    4. Forward-fill → backward-fill → median imputation
    5. No scaling (OLS is scale-invariant)
"""

import logging
from typing import Dict, List

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from . import config as C
from .data_loader import (
    clear_outliers_iqr,
    get_exogenous_columns,
    split_fold,
)
from .evaluation import compute_metrics

logger = logging.getLogger("baselines.mlr")

# Maximum fraction of NaN allowed in a column before it's dropped
_MAX_NAN_FRAC = 0.50


class MLRBaseline:
    """End-to-end MLR for a single ROCV fold + horizon."""

    def __init__(self, cfg: C.MLRConfig = None):
        self.cfg = cfg or C.MLRConfig()

    def run_fold(
        self,
        df_shifted: pd.DataFrame,
        train_end: pd.Timestamp,
        val_end: pd.Timestamp,
        test_origin: pd.Timestamp,
        horizon_days: int,
    ) -> Dict:
        """Execute one fold: split → clean → impute → fit → predict → evaluate."""
        target = C.TARGET

        # ── Split ──
        train_df, val_df, test_df = split_fold(
            df_shifted, train_end, val_end, test_origin, horizon_days,
            use_timestamp_col=True,
        )

        if len(train_df) == 0 or len(test_df) == 0:
            logger.warning("Empty train (%d) or test (%d).", len(train_df), len(test_df))
            return {"n_train": len(train_df), "n_test": len(test_df)}

        # ── Identify numeric feature columns ──
        feature_cols = [
            c for c in train_df.columns
            if c not in (target, "timestamp")
            and train_df[c].dtype in (np.float64, np.float32, np.int64, np.int32)
        ]

        # ── Drop columns that are >50 % NaN in the training set ──
        nan_fracs = train_df[feature_cols].isna().mean()
        usable = nan_fracs[nan_fracs <= _MAX_NAN_FRAC].index.tolist()
        dropped = len(feature_cols) - len(usable)
        if dropped > 0:
            logger.info("Dropped %d/%d cols (>%.0f%% NaN).",
                        dropped, len(feature_cols), _MAX_NAN_FRAC * 100)
        feature_cols = usable

        if len(feature_cols) == 0:
            logger.warning("No usable feature columns remain after NaN filtering.")
            return {"n_train": len(train_df), "n_test": len(test_df)}

        # ── IQR outlier removal (train only, on usable cols + target) ──
        numerical_cols = feature_cols + [target]
        train_df = clear_outliers_iqr(train_df, numerical_cols)

        # ── Fast imputation: forward-fill → backward-fill → median ──
        for col in feature_cols:
            # Train
            train_df[col] = train_df[col].ffill().bfill()
            med = train_df[col].median()
            if np.isfinite(med):
                train_df[col] = train_df[col].fillna(med)
            else:
                train_df[col] = train_df[col].fillna(0.0)
            # Test — use training median as fill value
            test_df[col] = test_df[col].ffill().bfill()
            test_df[col] = test_df[col].fillna(med if np.isfinite(med) else 0.0)

        # Ensure target has no NaN
        train_df = train_df.dropna(subset=[target])
        test_df = test_df.dropna(subset=[target])

        if len(train_df) == 0 or len(test_df) == 0:
            logger.warning("Empty after imputation: train=%d, test=%d.",
                           len(train_df), len(test_df))
            return {"n_train": len(train_df), "n_test": len(test_df)}

        logger.info("MLR fitting: %d features × %d train rows → %d test rows.",
                     len(feature_cols), len(train_df), len(test_df))

        # ── Fit ──
        X_train = train_df[feature_cols].values
        y_train = train_df[target].values

        model = LinearRegression()
        model.fit(X_train, y_train)

        # ── Predict ──
        X_test = test_df[feature_cols].values
        y_pred = model.predict(X_test)
        y_true = test_df[target].values

        # ── Evaluate ──
        metrics = compute_metrics(y_true, y_pred)
        test_timestamps = test_df["timestamp"].values if "timestamp" in test_df.columns else None

        return {
            "predictions_mw": y_pred,
            "actuals_mw": y_true,
            "timestamps": test_timestamps,
            "n_train": len(train_df),
            "n_test": len(test_df),
            "metrics": metrics,
            "model": model,
            "n_features": len(feature_cols),
        }
