"""Data loading, preprocessing, and ROCV fold generation for all baselines.

Provides a unified data loading and transformation pipeline consumed by
all six baseline models:

    1. Load hourly Parquet, drop NTL columns, merge validated/non-val weather.
    2. Preprocessing variants per model:
       - IQR clipping  (van de Sande models)
       - Winsorization (Ashtar models)
       - MICE imputation (all models that need it)
       - Forward-fill  (Ashtar)
    3. Feature engineering:
       - Autoregressive lags (configurable)
       - Rolling means (7-day, 30-day)
       - Calendar features (hour, day-of-week, month, season, etc.)
       - Target shifting by horizon
    4. Sequence creation for LSTM / Seq2Seq models.
    5. Expanding-window ROCV fold generation with configurable step.
"""

import logging
from typing import Dict, Generator, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer
from sklearn.preprocessing import MinMaxScaler

from . import config as C

logger = logging.getLogger("baselines.data_loader")

# Dutch public holidays (fixed + approximated movable feasts).
# Movable holidays are approximated — a proper holiday library could
# replace this, but we avoid extra dependencies.
_NL_FIXED_HOLIDAYS_MD = [
    (1, 1),   # New Year
    (4, 27),  # King's Day (or 26th if 27th is Sunday — simplified)
    (5, 5),   # Liberation Day
    (12, 25), # Christmas
    (12, 26), # Second Christmas Day
]


# ═══════════════════════════════════════════════════════════════════════════
# 1.  Load raw dataset
# ═══════════════════════════════════════════════════════════════════════════

def load_dataset(path: Optional[str] = None) -> pd.DataFrame:
    """Load the hourly dataset, drop NTL, merge weather, add calendar cols.

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

    # ── Forward-fill CBS / weather (lower-freq → hourly broadcast) ──
    weather_cols = _weather_cols(df)
    cbs_cols = [c for c in df.columns if c.startswith("cbs_")]
    for col in weather_cols + cbs_cols:
        df[col] = df[col].ffill()

    # ── Add calendar features ──
    df = add_calendar_features(df)

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
# 2.  Predictor columns
# ═══════════════════════════════════════════════════════════════════════════

def get_exogenous_columns(df: pd.DataFrame) -> List[str]:
    """Return weather + CBS feature columns present in the dataframe."""
    wanted = set()
    for col in df.columns:
        if col.startswith("weather_"):
            wanted.add(col)
        elif col.startswith("cbs_"):
            wanted.add(col)
    return sorted(wanted)


def get_calendar_columns(df: pd.DataFrame) -> List[str]:
    """Return the calendar feature columns present in the dataframe."""
    return [c for c in C.CALENDAR_FEATURES if c in df.columns]


# ═══════════════════════════════════════════════════════════════════════════
# 3.  Feature engineering
# ═══════════════════════════════════════════════════════════════════════════

def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add calendar / temporal indicator columns.

    Adds: ``hour_of_day``, ``day_of_week``, ``month``, ``season``,
    ``weekend_indicator``, ``holiday_indicator``.
    """
    idx = df.index if isinstance(df.index, pd.DatetimeIndex) else pd.DatetimeIndex(df.index)

    df["hour_of_day"] = idx.hour
    df["day_of_week"] = idx.dayofweek          # Monday=0 … Sunday=6
    df["month"] = idx.month
    df["season"] = idx.month.map(
        {12: 0, 1: 0, 2: 0,  # Winter
         3: 1, 4: 1, 5: 1,   # Spring
         6: 2, 7: 2, 8: 2,   # Summer
         9: 3, 10: 3, 11: 3} # Autumn
    )
    df["weekend_indicator"] = (idx.dayofweek >= 5).astype(int)

    # Holiday indicator (fixed NL holidays)
    md_tuples = set(_NL_FIXED_HOLIDAYS_MD)
    df["holiday_indicator"] = [
        1 if (ts.month, ts.day) in md_tuples else 0
        for ts in idx
    ]

    return df


def add_autoregressive_lags(
    df: pd.DataFrame,
    target_col: str,
    lag_hours: List[int],
    prefix: str = "load_lag",
) -> pd.DataFrame:
    """Create lagged features of the target variable.

    Parameters
    ----------
    lag_hours : e.g. [24, 168, 8760] for 1-day, 1-week, 1-year lags.
    """
    for lag in lag_hours:
        col_name = f"{prefix}_{lag}h"
        df[col_name] = df[target_col].shift(lag)
    return df


def add_rolling_features(
    df: pd.DataFrame,
    target_col: str,
    windows_hours: List[int],
) -> pd.DataFrame:
    """Add rolling mean features of the target."""
    for w in windows_hours:
        col_name = f"rolling_mean_{w}h"
        df[col_name] = df[target_col].shift(1).rolling(window=w, min_periods=1).mean()
    return df


def shift_predictors(
    df: pd.DataFrame,
    predictor_cols: List[str],
    n_hours_lag: int,
) -> pd.DataFrame:
    """Shift predictor columns by ``n_hours_lag`` into the past.

    At time *t*, the model sees the predictor values from *t − lag*.
    The original (un-shifted) columns are dropped.
    """
    df_out = df.copy()
    for col in predictor_cols:
        if col in df_out.columns:
            df_out[f"{col}_lag"] = df_out[col].shift(n_hours_lag)
            df_out = df_out.drop(columns=[col])
    return df_out


def prepare_shifted_dataset(
    df: pd.DataFrame,
    horizon_days: int,
    lag_hours: List[int],
    include_exogenous: bool = True,
) -> pd.DataFrame:
    """Full shift pipeline for regression-style models (MLR, Prophet-LSTM).

    1. Shift exogenous predictors by ``horizon_days * 24`` hours.
    2. Create autoregressive load lags (shifted by horizon + lag offset).
    3. Drop rows where lags cannot be computed.
    """
    horizon_h = horizon_days * 24
    target = C.TARGET

    # Identify predictors to shift
    exog_cols = get_exogenous_columns(df) if include_exogenous else []
    cal_cols = get_calendar_columns(df)

    # Shift exogenous columns
    df_shifted = df.copy()
    if exog_cols:
        df_shifted = shift_predictors(df_shifted, exog_cols, horizon_h)

    # Create load lags (relative to horizon)
    for lag in lag_hours:
        col_name = f"load_lag_{lag}h"
        df_shifted[col_name] = df_shifted[target].shift(horizon_h + lag)

    # Also add the direct horizon-lagged load
    df_shifted["load_lag_horizon"] = df_shifted[target].shift(horizon_h)

    # Drop rows where lags are unavailable
    max_lag = horizon_h + max(lag_hours)
    df_shifted = df_shifted.iloc[max_lag:]
    df_shifted = df_shifted.reset_index()  # timestamp back as column

    logger.info(
        "Shifted dataset: horizon=%dd (%dh), remaining rows=%d, "
        "shifted cols=%d, lag cols=%d.",
        horizon_days, horizon_h, len(df_shifted),
        len([c for c in df_shifted.columns if c.endswith("_lag")]),
        len([c for c in df_shifted.columns if c.startswith("load_lag_")]),
    )
    return df_shifted


# ═══════════════════════════════════════════════════════════════════════════
# 4.  Outlier removal
# ═══════════════════════════════════════════════════════════════════════════

def clear_outliers_iqr(
    train_df: pd.DataFrame,
    numerical_cols: List[str],
) -> pd.DataFrame:
    """IQR clipping: set outliers to NaN (van de Sande method)."""
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


def winsorize_outliers(
    train_df: pd.DataFrame,
    numerical_cols: List[str],
    limits: Tuple[float, float] = C.WINSORIZE_LIMITS,
) -> pd.DataFrame:
    """Winsorization: clip to percentile bounds (Ashtar method)."""
    df = train_df.copy()
    n_clipped = 0
    for col in numerical_cols:
        if col not in df.columns:
            continue
        lower = df[col].quantile(limits[0])
        upper = df[col].quantile(limits[1])
        mask = (df[col] < lower) | (df[col] > upper)
        n_clipped += mask.sum()
        df[col] = df[col].clip(lower=lower, upper=upper)
    logger.info("Winsorization: %d values clipped across %d cols.",
                n_clipped, len(numerical_cols))
    return df


# ═══════════════════════════════════════════════════════════════════════════
# 5.  Imputation
# ═══════════════════════════════════════════════════════════════════════════

def impute_mice(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    numerical_cols: List[str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """MICE imputation: fit on train, transform both."""
    train = train_df.copy()
    test = test_df.copy()

    cols_present = [c for c in numerical_cols if c in train.columns]
    if not cols_present:
        return train, test

    # Build imputer on columns that have variance
    cols_to_impute = []
    for col in cols_present:
        if train[col].notna().sum() > 1 and train[col].isna().any():
            cols_to_impute.append(col)

    if not cols_to_impute:
        return train, test

    imputer = IterativeImputer(max_iter=10, random_state=42)
    train[cols_to_impute] = imputer.fit_transform(
        train[cols_to_impute].values
    )
    test_cols = [c for c in cols_to_impute if c in test.columns]
    if test_cols:
        test[test_cols] = imputer.transform(test[test_cols].values)

    n_train_nan = train[cols_present].isna().sum().sum()
    n_test_nan = test[[c for c in cols_present if c in test.columns]].isna().sum().sum()
    logger.info("MICE imputation done. Remaining NaN: train=%d, test=%d.",
                n_train_nan, n_test_nan)
    return train, test


def impute_ffill(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Forward-fill imputation (Ashtar method)."""
    df = df.copy()
    for col in cols:
        if col in df.columns:
            df[col] = df[col].ffill().bfill()
    return df


# ═══════════════════════════════════════════════════════════════════════════
# 6.  Scaling
# ═══════════════════════════════════════════════════════════════════════════

def fit_scaler(
    train_df: pd.DataFrame,
    feature_cols: List[str],
    target_col: str,
) -> Tuple[MinMaxScaler, List[str]]:
    """Fit MinMaxScaler on training data.  Returns (scaler, ordered_cols)."""
    all_cols = [c for c in feature_cols if c != target_col and c in train_df.columns]
    all_cols.append(target_col)
    train_vals = np.nan_to_num(train_df[all_cols].values.astype(np.float64), nan=0.0)
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaler.fit(train_vals)
    return scaler, all_cols


def apply_scaler(
    df: pd.DataFrame,
    scaler: MinMaxScaler,
    all_cols: List[str],
) -> np.ndarray:
    """Apply a fitted scaler.  Returns scaled array."""
    vals = np.nan_to_num(df[all_cols].values.astype(np.float64), nan=0.0)
    return scaler.transform(vals)


def inverse_transform_target(
    scaler: MinMaxScaler,
    predictions: np.ndarray,
    all_cols: List[str],
    target_col: str,
) -> np.ndarray:
    """Inverse-transform scaled predictions back to MW scale."""
    predictions = predictions.ravel()
    dummy = np.zeros((len(predictions), len(all_cols)))
    target_idx = all_cols.index(target_col)
    dummy[:, target_idx] = predictions
    inversed = scaler.inverse_transform(dummy)
    return inversed[:, target_idx]


# ═══════════════════════════════════════════════════════════════════════════
# 7.  Sequence creation (for LSTM models)
# ═══════════════════════════════════════════════════════════════════════════

def create_sequences(
    data: np.ndarray,
    seq_length: int,
    target_idx: int = -1,
) -> Tuple[np.ndarray, np.ndarray]:
    """Create sliding-window sequences for LSTM input.

    Parameters
    ----------
    data : 2-D array (timesteps, features) — already scaled.
    seq_length : number of time steps per input window.
    target_idx : column index of the target in ``data``.

    Returns
    -------
    X : (n_samples, seq_length, n_features)
    y : (n_samples,)
    """
    n_samples = len(data) - seq_length
    if n_samples <= 0:
        return np.empty((0, seq_length, data.shape[1])), np.empty(0)

    n_features = data.shape[1]
    X = np.empty((n_samples, seq_length, n_features), dtype=np.float32)
    y = np.empty(n_samples, dtype=np.float32)

    for i in range(n_samples):
        X[i] = data[i : i + seq_length]
        y[i] = data[i + seq_length, target_idx]

    return X, y


def create_seq2seq_sequences(
    data: np.ndarray,
    input_length: int,
    output_length: int,
    target_idx: int = -1,
) -> Tuple[np.ndarray, np.ndarray]:
    """Create encoder-decoder input/output sequence pairs.

    Returns
    -------
    X : (n_samples, input_length, n_features)
    Y : (n_samples, output_length)  — target values only
    """
    total = input_length + output_length
    n_samples = len(data) - total
    if n_samples <= 0:
        return (np.empty((0, input_length, data.shape[1])),
                np.empty((0, output_length)))

    n_features = data.shape[1]
    X = np.empty((n_samples, input_length, n_features), dtype=np.float32)
    Y = np.empty((n_samples, output_length), dtype=np.float32)

    for i in range(n_samples):
        X[i] = data[i : i + input_length]
        Y[i] = data[i + input_length : i + total, target_idx]

    return X, Y


# ═══════════════════════════════════════════════════════════════════════════
# 8.  ROCV fold generation
# ═══════════════════════════════════════════════════════════════════════════

def rocv_folds(
    df: pd.DataFrame,
    cfg: C.ROCVConfig,
) -> Generator[Tuple[int, pd.Timestamp, pd.Timestamp, pd.Timestamp], None, None]:
    """Generate expanding-window ROCV folds.

    For each fold yields:
        ``(fold_idx, train_end, val_end, test_origin)``

    Fold semantics (matching the thesis protocol):

    - **train**: ``[start_date, train_end)``
    - **val**:   ``[train_end, val_end)``  — last calendar year of the
      expanded window, used for early stopping / hyperparameter tuning.
    - **test_origin**: ``val_end`` — the point from which forecasts are
      issued.  The actual test targets are at
      ``test_origin + horizon_days`` for each horizon.

    The origin advances by ``fold_step_years`` each iteration.
    """
    start = pd.Timestamp(cfg.start_date)
    data_end = (
        df.index.max()
        if isinstance(df.index, pd.DatetimeIndex)
        else pd.Timestamp(df["timestamp"].max())
    )
    max_horizon = max(cfg.forecast_horizons_days)

    fold_idx = 0
    # First fold: 2 years train + 1 year val
    train_end = start + pd.DateOffset(years=cfg.min_train_years)
    val_end = train_end + pd.DateOffset(years=1)

    while val_end + pd.Timedelta(days=max_horizon) <= data_end:
        yield fold_idx, train_end, val_end, val_end

        train_end += pd.DateOffset(years=cfg.fold_step_years)
        val_end = train_end + pd.DateOffset(years=1)
        fold_idx += 1


def split_fold(
    df: pd.DataFrame,
    train_end: pd.Timestamp,
    val_end: pd.Timestamp,
    test_origin: pd.Timestamp,
    horizon_days: int,
    use_timestamp_col: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split a dataframe into train / val / test for one ROCV fold + horizon.

    Parameters
    ----------
    use_timestamp_col : If True, df has a ``timestamp`` column (post-shift).
        If False, uses the DatetimeIndex.
    """
    start = pd.Timestamp(C.ROCVConfig.start_date)
    test_end = test_origin + pd.Timedelta(days=horizon_days)

    if use_timestamp_col:
        ts = df["timestamp"]
    else:
        ts = df.index

    train_mask = (ts >= start) & (ts < train_end)
    val_mask = (ts >= train_end) & (ts < val_end)
    test_mask = (ts >= test_origin) & (ts < test_end)

    train_df = df.loc[train_mask].copy()
    val_df = df.loc[val_mask].copy()
    test_df = df.loc[test_mask].copy()

    return train_df, val_df, test_df


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════

def _merge_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Create unified ``weather_*`` columns, preferring validated KNMI."""
    for val_col, nonval_col in zip(
        C.WEATHER_FEATURES_VAL, C.WEATHER_FEATURES_NONVAL
    ):
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
