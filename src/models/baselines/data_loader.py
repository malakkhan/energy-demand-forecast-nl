"""Data loading, shifting, outlier removal, and imputation.

Faithfully reproduces the preprocessing pipeline from the reference
implementation:

    1. Load hourly data, drop NTL columns, merge weather (val-first).
    2. **Shift** all predictors by ``n_days_lag × 24`` hours (so that at
       time *t* the model only sees reality from *t − lag*).  Also create
       seasonal load lags: ``load[t−lag−24h]``, ``load[t−lag−7d]``,
       ``load[t−lag−12m]``.
    3. **IQR outlier removal** on training data only → set to NaN.
    4. **MICE imputation** (``IterativeImputer``) fitted on train,
       transform applied to test.
    5. **MinMaxScaler** fitted on train, applied to both train and test.

Exposes helpers for ROCV fold generation.
"""

import logging
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer
from sklearn.preprocessing import MinMaxScaler

from . import config as C

logger = logging.getLogger("baselines.data_loader")


# ═══════════════════════════════════════════════════════════════════════════
# 1.  Load raw dataset
# ═══════════════════════════════════════════════════════════════════════════

def load_dataset(path: Optional[str] = None) -> pd.DataFrame:
    """Load the final hourly dataset, drop NTL, merge weather, add lags.

    Returns a DatetimeIndex-ed DataFrame ready for fold splitting.
    """
    path = path or str(C.DATA_ROOT)
    logger.info("Loading dataset from %s …", path)
    df = pd.read_parquet(path)

    # Ensure datetime index
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").set_index("timestamp")

    # ── Drop NTL columns ──
    ntl_cols = [c for c in df.columns
                if any(c.startswith(p) for p in C.NTL_EXCLUDE_PREFIXES)]
    if ntl_cols:
        logger.info("Dropping %d NTL columns.", len(ntl_cols))
        df = df.drop(columns=ntl_cols)

    # ── Merge weather: prefer validated, fall back to non-validated ──
    df = _merge_weather(df)

    # ── Drop the raw KNMI columns (we now have unified weather_* cols) ──
    raw_knmi = [c for c in df.columns
                if c.startswith("knmi_") or c.startswith("knmi_val_")]
    df = df.drop(columns=[c for c in raw_knmi if c in df.columns],
                 errors="ignore")

    # ── Drop station count columns ──
    df = df.drop(columns=[c for c in df.columns if "station_count" in c],
                 errors="ignore")

    # ── Forward-fill CBS/weather (monthly → hourly) ──
    for col in _weather_cols(df) + [c for c in C.CBS_FEATURES if c in df.columns]:
        df[col] = df[col].ffill()

    # ── Ensure target is present ──
    if C.TARGET not in df.columns:
        raise ValueError(f"Target column {C.TARGET!r} not found.")
    n_before = len(df)
    df = df.dropna(subset=[C.TARGET])
    if n_before - len(df) > 0:
        logger.info("Dropped %d rows with NaN target.", n_before - len(df))

    logger.info("Dataset loaded: %d rows × %d cols  (%s → %s).",
                len(df), len(df.columns),
                df.index.min().date(), df.index.max().date())
    return df


# ═══════════════════════════════════════════════════════════════════════════
# 2.  Predictor columns (what gets shifted / fed to Prophet)
# ═══════════════════════════════════════════════════════════════════════════

def get_predictor_columns(df: pd.DataFrame) -> List[str]:
    """Return the list of raw predictor column names (before shifting).

    These are weather + CBS features that are present in the dataset.
    Does NOT include the target, datetime-derived columns, or metadata.
    """
    exclude_prefixes = ("year", "month", "day", "hour", "quarter",
                        "week_of_year", "day_of_week", "day_of_year",
                        "is_weekend")
    predictors = []
    for col in df.columns:
        if col == C.TARGET:
            continue
        if col.startswith(exclude_prefixes):
            continue
        if col in ("year", "month", "day", "hour", "quarter",
                   "week_of_year", "day_of_week", "day_of_year",
                   "is_weekend"):
            continue
        predictors.append(col)
    return sorted(predictors)


# ═══════════════════════════════════════════════════════════════════════════
# 3.  Shift data  (reference: shift_data function)
# ═══════════════════════════════════════════════════════════════════════════

def shift_data(df: pd.DataFrame, n_days_lag: int) -> pd.DataFrame:
    """Shift all predictors by ``n_days_lag`` days into the past.

    At time *t*, the model sees the predictor values from *t − lag*.
    Also creates seasonal load consumption lags:
        - ``load_lag``          : load at *t − lag*
        - ``load_lag_24h``      : load at *t − lag − 24h*
        - ``load_lag_7d``       : load at *t − lag − 7d*
        - ``load_lag_12m``      : load at *t − lag − 12m*

    The first ``lag + 12 months`` of data is dropped because those rows
    cannot have all lags filled.
    """
    lag_hours = n_days_lag * 24
    target = C.TARGET
    predictors = get_predictor_columns(df)

    df_lag = df.copy()
    df_lag = df_lag.sort_index()

    # Shift each predictor column
    for col in predictors:
        df_lag[f"{col}_lag"] = df_lag[col].shift(lag_hours)
        df_lag = df_lag.drop(columns=[col])

    # Create load consumption lags
    df_lag["load_lag"] = df_lag[target].shift(lag_hours)
    df_lag["load_lag_24h"] = df_lag[target].shift(lag_hours + 24)
    df_lag["load_lag_7d"] = df_lag[target].shift(lag_hours + 24 * 7)
    df_lag["load_lag_12m"] = df_lag[target].shift(lag_hours + 24 * 365)

    # Drop rows that can't have all lags
    min_shift = lag_hours + 24 * 365
    df_lag = df_lag.iloc[min_shift:]
    df_lag = df_lag.reset_index()  # timestamp back to column

    logger.info("Shifted data by %d days (%d hours). "
                "Remaining rows: %d. Lagged predictor cols: %d.",
                n_days_lag, lag_hours, len(df_lag),
                len([c for c in df_lag.columns if c.endswith("_lag")]))
    return df_lag


# ═══════════════════════════════════════════════════════════════════════════
# 4.  IQR outlier removal  (reference: clear_outliers)
# ═══════════════════════════════════════════════════════════════════════════

def clear_outliers(train_df: pd.DataFrame,
                   numerical_cols: List[str]) -> pd.DataFrame:
    """Remove outliers from training data using the IQR method.

    Outliers are set to NaN (later imputed via MICE).
    Only applied to the training fold to prevent overfitting.
    """
    df = train_df.copy()
    n_outliers = 0
    for col in numerical_cols:
        if col not in df.columns:
            continue
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - C.IQR_MULTIPLIER * iqr
        upper = q3 + C.IQR_MULTIPLIER * iqr
        mask = (df[col] < lower) | (df[col] > upper)
        n_outliers += mask.sum()
        df.loc[mask, col] = np.nan
    logger.info("IQR outlier removal: %d values set to NaN across %d cols.",
                n_outliers, len(numerical_cols))
    return df


# ═══════════════════════════════════════════════════════════════════════════
# 5.  MICE imputation  (reference: impute_numerical_columns)
# ═══════════════════════════════════════════════════════════════════════════

def impute_missing(train_df: pd.DataFrame,
                   test_df: pd.DataFrame,
                   numerical_cols: List[str]
                   ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Impute NaNs using MICE (IterativeImputer).

    Fitted on training data only → applied to transform test data.
    This prevents data leakage.
    """
    train = train_df.copy()
    test = test_df.copy()

    cols_present = [c for c in numerical_cols if c in train.columns]

    for col in cols_present:
        n_train_na = train[col].isna().sum()
        n_test_na = test[col].isna().sum() if col in test.columns else 0

        if n_train_na == 0 and n_test_na == 0:
            continue

        # Skip columns that are entirely NaN (cannot impute)
        if train[col].notna().sum() == 0:
            train[col] = 0.0
            if col in test.columns:
                test[col] = 0.0
            continue

        imputer = IterativeImputer(max_iter=10, random_state=42)
        train[col] = imputer.fit_transform(
            train[[col]].values
        ).ravel()
        if col in test.columns:
            test[col] = imputer.transform(
                test[[col]].values
            ).ravel()

    n_train_nan = train[cols_present].isna().sum().sum()
    n_test_nan = test[[c for c in cols_present if c in test.columns]].isna().sum().sum()
    logger.info("MICE imputation complete. Remaining NaN: train=%d, test=%d.",
                n_train_nan, n_test_nan)
    return train, test


# ═══════════════════════════════════════════════════════════════════════════
# 6.  MinMaxScaling  (reference: run_lstm → MinMaxScaler)
# ═══════════════════════════════════════════════════════════════════════════

def scale_data(train_df: pd.DataFrame,
               test_df: pd.DataFrame,
               feature_cols: List[str],
               target_col: str
               ) -> Tuple[np.ndarray, np.ndarray,
                           np.ndarray, np.ndarray,
                           MinMaxScaler, List[str]]:
    """MinMaxScale features and target.  Fit on train only.

    Returns
    -------
    train_X, train_y, test_X, test_y : arrays
    scaler : fitted MinMaxScaler (for inverse transform)
    all_cols : ordered column names (features first, target last)
    """
    # Ensure target is last column (reference pattern)
    all_cols = [c for c in feature_cols if c != target_col] + [target_col]

    # Filter to only columns that exist
    all_cols = [c for c in all_cols if c in train_df.columns]

    train_vals = train_df[all_cols].values.astype(np.float64)
    test_vals = test_df[all_cols].values.astype(np.float64)

    # Replace any remaining NaN with 0 before scaling
    train_vals = np.nan_to_num(train_vals, nan=0.0)
    test_vals = np.nan_to_num(test_vals, nan=0.0)

    scaler = MinMaxScaler(feature_range=(0, 1))
    train_scaled = scaler.fit_transform(train_vals)
    test_scaled = scaler.transform(test_vals)

    # Split X (all but last) and y (last column = target)
    train_X = train_scaled[:, :-1]
    train_y = train_scaled[:, -1]
    test_X = test_scaled[:, :-1]
    test_y = test_scaled[:, -1]

    # Reshape for LSTM: (samples, 1, features) — seq_len=1 per reference
    train_X = train_X.reshape((train_X.shape[0], 1, train_X.shape[1]))
    test_X = test_X.reshape((test_X.shape[0], 1, test_X.shape[1]))

    return train_X, train_y, test_X, test_y, scaler, all_cols


def inverse_transform_predictions(scaler: MinMaxScaler,
                                  predictions: np.ndarray,
                                  target_col: str,
                                  all_cols: List[str]) -> np.ndarray:
    """Inverse-transform scaled predictions back to original MW scale.

    Matches the reference: create a dummy DataFrame with zeros for all
    columns except the target, then inverse-transform.
    """
    predictions = predictions.ravel()
    dummy = np.zeros((len(predictions), len(all_cols)))
    target_idx = all_cols.index(target_col)
    dummy[:, target_idx] = predictions
    inversed = scaler.inverse_transform(dummy)
    return inversed[:, target_idx]


# ═══════════════════════════════════════════════════════════════════════════
# 7.  ROCV fold generation
# ═══════════════════════════════════════════════════════════════════════════

def rocv_folds(df: pd.DataFrame, cfg: C.ROCVConfig):
    """Generate ROCV fold definitions.

    Yields (fold_idx, cutoff_timestamp).
    The cutoff advances by ``fold_step_years`` each fold.
    """
    start = pd.Timestamp(cfg.start_date)
    data_end = df.index.max() if isinstance(df.index, pd.DatetimeIndex) \
        else pd.Timestamp(df["timestamp"].max())
    max_horizon = max(cfg.forecast_horizons_days)

    fold_idx = 0
    cutoff = start + pd.DateOffset(years=cfg.min_train_years)

    while cutoff + pd.Timedelta(days=max_horizon) <= data_end:
        yield fold_idx, cutoff
        cutoff += pd.DateOffset(years=cfg.fold_step_years)
        fold_idx += 1


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════

def _merge_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Create unified weather_* columns, preferring validated."""
    for val_col, nonval_col in zip(C.WEATHER_FEATURES_VAL,
                                   C.WEATHER_FEATURES_NONVAL):
        generic = val_col.replace("knmi_val_", "weather_")
        if val_col in df.columns and nonval_col in df.columns:
            df[generic] = df[val_col].fillna(df[nonval_col])
        elif val_col in df.columns:
            df[generic] = df[val_col]
        elif nonval_col in df.columns:
            df[generic] = df[nonval_col]
    return df


def _weather_cols(df: pd.DataFrame) -> List[str]:
    return sorted(c for c in df.columns if c.startswith("weather_"))
