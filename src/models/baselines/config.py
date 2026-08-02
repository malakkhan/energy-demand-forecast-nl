"""Unified configuration for all baseline forecasting models.

Covers six models:
    1. van de Sande MLR         (scikit-learn LinearRegression)
    2. van de Sande Prophet-LSTM (Prophet + PyTorch LSTM)
    3. Ashtar SARIMAX           (statsmodels SARIMAX)
    4. Ashtar SARIMAX-LSTM      (SARIMAX residuals + PyTorch LSTM)
    5. Ashtar Seq2Seq           (Encoder-Decoder LSTM)
    6. Curiël Prophet-QLSTM-XGB (Prophet + PennyLane QLSTM + XGBoost)

All hyperparameters are taken directly from the reference papers and the
``baseline_config.yaml`` unless noted otherwise.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = REPO_ROOT / "data" / "processed" / "nl_hourly_dataset.parquet"
OUTPUT_DIR = REPO_ROOT / "src" / "models" / "baselines" / "results"

# ---------------------------------------------------------------------------
# Target variable
# ---------------------------------------------------------------------------
TARGET = "entsoe_load_mw"

# Columns to ALWAYS exclude (all NTL / VIIRS satellite data)
NTL_EXCLUDE_PREFIXES = ["ntl_"]

# ---------------------------------------------------------------------------
# Predictor groups (adapted from reference papers' predictor sets)
# ---------------------------------------------------------------------------

# Weather features — prefer validated KNMI, fall back to non-validated.
# These are used to construct unified ``weather_*`` columns in the data
# loader, so downstream models always consume the same column names.
WEATHER_FEATURES_VAL = [
    "knmi_val_temp_c",
    "knmi_val_dewpoint_c",
    "knmi_val_wind_speed_ms",
    "knmi_val_wind_gust_ms",
    "knmi_val_solar_rad_jcm2",
    "knmi_val_sunshine_h",
    "knmi_val_humidity_pct",
]
WEATHER_FEATURES_NONVAL = [
    "knmi_temp_c",
    "knmi_dewpoint_c",
    "knmi_wind_speed_ms",
    "knmi_wind_gust_ms",
    "knmi_solar_rad_jcm2",
    "knmi_sunshine_h",
    "knmi_humidity_pct",
]

# CBS economic indicators (the full set available in the merged dataset)
CBS_FEATURES = [
    "cbs_cpi_energy",
    "cbs_cpi_electricity",
    "cbs_cpi_gas",
    "cbs_gep_gas_hh_total",
    "cbs_gep_gas_hh_supply",
    "cbs_gep_gas_hh_network",
    "cbs_gep_elec_hh_total",
    "cbs_gep_elec_hh_supply",
    "cbs_gep_elec_hh_network",
    "cbs_gdp_yy",
    "cbs_gdp_qq",
    "cbs_consumption_hh_yy",
    "cbs_consumption_hh_qq",
    "cbs_population_million",
    "cbs_capform_total_yy",
    "cbs_exports_total_yy",
    "cbs_imports_total_yy",
]

# Prophet component columns (generated at runtime)
PROPHET_COMPONENTS_ADDITIVE = ["yhat", "trend", "daily", "weekly", "yearly"]
PROPHET_COMPONENTS_MULTIPLICATIVE = ["yhat", "weekly", "yearly", "trend"]

# Temporal / calendar feature names (generated in data_loader)
CALENDAR_FEATURES = [
    "hour_of_day",
    "day_of_week",
    "month",
    "season",
    "weekend_indicator",
    "holiday_indicator",
]

# ---------------------------------------------------------------------------
# Outlier removal
# ---------------------------------------------------------------------------
IQR_MULTIPLIER = 1.5
WINSORIZE_LIMITS = (0.01, 0.99)  # 1st and 99th percentile

# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------
METRICS = ["rmse", "mae", "mape_pct", "mase", "nmae", "nrmse", "pcc"]

# All model names (for CLI)
ALL_MODELS = [
    "van_de_sande_mlr",
    "van_de_sande_prophet_lstm",
    "ashtar_sarimax",
    "ashtar_sarimax_lstm",
    "ashtar_seq2seq",
    "curiel_stacked_prophet_qlstm",
]


# ═══════════════════════════════════════════════════════════════════════════
# Rolling-Origin Cross-Validation
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ROCVConfig:
    """Rolling-Origin Cross-Validation parameters.

    Expanding window with configurable origin step.  The first fold
    uses ``min_train_years`` of data for training and the next full
    calendar year for validation.  The origin advances by
    ``fold_step_years`` each fold.

    Parameters
    ----------
    start_date : str
        First date in the dataset (inclusive).
    min_train_years : int
        Minimum training history required to initialise the first fold.
    fold_step_years : int
        How many years the origin advances between folds.  Set to 1 for
        annual advancement; change for finer granularity in the future.
    forecast_horizons_days : List[int]
        Horizons to evaluate (60, 75, 90, … , 180 days).
    """
    start_date: str = "2012-01-01"
    min_train_years: int = 2
    fold_step_years: int = 1  # Toggle-able: 1 = annual, could be fractional
    forecast_horizons_days: List[int] = field(
        default_factory=lambda: list(range(60, 181, 15))
    )  # [60, 75, 90, 105, 120, 135, 150, 165, 180]


# ═══════════════════════════════════════════════════════════════════════════
# Model 1: MLR — van de Sande et al.
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class MLRConfig:
    """Multiple Linear Regression configuration."""
    include_exogenous: bool = True
    autoregressive_lags_hours: List[int] = field(
        default_factory=lambda: [24, 168, 8760]  # 1 day, 1 week, 1 year
    )


# ═══════════════════════════════════════════════════════════════════════════
# Model 2: Prophet-LSTM — van de Sande et al. (Huang / Model 1 only)
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ProphetConfig:
    """Prophet configuration — additive seasonality."""
    seasonality_mode: str = "additive"
    daily_seasonality: bool = True
    weekly_seasonality: bool = True
    yearly_seasonality: bool = True
    add_country_holidays: str = ""  # "NL" to enable
    suppress_logging: bool = True


@dataclass
class LSTMConfig:
    """PyTorch LSTM hyperparameters — Huang (Model 1) architecture.

    LSTM(30, return_sequences=True) → LSTM(90) → Linear(1)
    No dropout.  Optimizer = Adam.
    """
    lstm1_units: int = 30
    lstm2_units: int = 90
    dropout: float = 0.0
    epochs: int = 50
    batch_size: int = 128
    learning_rate: float = 1e-3
    early_stop_patience: int = 3
    early_stop_min_delta: float = 1e-4
    early_stop_start_epoch: int = 10


# ═══════════════════════════════════════════════════════════════════════════
# Model 3: SARIMAX — Ashtar et al.
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class SARIMAXConfig:
    """SARIMAX hyperparameters.

    The ``max_train_hours`` cap keeps fitting tractable on hourly data.
    With ``m=168`` and ~100 K rows the Kalman filter is extremely
    expensive; we therefore restrict training to the most recent
    ``max_train_hours`` hours within each fold's available history.
    """
    order: Tuple[int, int, int] = (2, 0, 3)
    seasonal_order: Tuple[int, int, int, int] = (1, 0, 1, 168)
    max_train_hours: int = 24 * 365 * 2  # Last 2 years to cap runtime
    rolling_means_hours: List[int] = field(
        default_factory=lambda: [168, 720]  # 7-day, 30-day
    )
    lagged_features_hours: List[int] = field(
        default_factory=lambda: [1, 24, 168]  # prev hour, day, week
    )
    maxiter: int = 50  # L-BFGS iteration cap
    enforce_stationarity: bool = False
    enforce_invertibility: bool = False


# ═══════════════════════════════════════════════════════════════════════════
# Model 4: SARIMAX-LSTM — Ashtar et al.
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class SARIMAXLSTMConfig:
    """Residual-stacking LSTM that models SARIMAX residuals."""
    input_sequence_length: int = 720  # 30 days of hourly residuals
    lstm_units: int = 50
    epochs: int = 30
    batch_size: int = 32
    learning_rate: float = 1e-3


# ═══════════════════════════════════════════════════════════════════════════
# Model 5: Seq2Seq — Ashtar et al.
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Seq2SeqConfig:
    """Encoder-Decoder LSTM configuration.

    The decoder uses teacher forcing during training and autoregressive
    decoding at inference.
    """
    input_sequence_length: int = 720   # 30 days × 24 hours
    output_sequence_length: int = 168  # 7 days × 24 hours
    encoder_units: int = 64
    decoder_units: int = 64
    epochs: int = 20
    batch_size: int = 16
    learning_rate: float = 1e-3
    teacher_forcing_ratio: float = 1.0  # 1.0 = always use ground truth


# ═══════════════════════════════════════════════════════════════════════════
# Model 6: Prophet-QLSTM-XGBoost — Curiël et al.
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ProphetMultConfig:
    """Prophet with multiplicative seasonality (Curiël)."""
    seasonality_mode: str = "multiplicative"
    daily_seasonality: bool = False
    weekly_seasonality: bool = True
    yearly_seasonality: bool = True
    add_country_holidays: str = ""
    suppress_logging: bool = True


@dataclass
class QLSTMConfig:
    """Quantum LSTM hyperparameters (PennyLane + PyTorch)."""
    n_qubits: int = 4
    n_qlayers: int = 1
    quantum_backend: str = "lightning.qubit"
    sequence_length: int = 48  # hours
    hidden_size_1: int = 60
    hidden_size_2: int = 120
    dropout_rate: float = 0.3
    epochs: int = 50
    batch_size: int = 256
    learning_rate: float = 1e-3
    warmup_epochs: int = 3
    early_stop_patience: int = 5
    early_stop_min_delta: float = 1e-5
    early_stop_start_epoch: int = 3


@dataclass
class XGBoostStackerConfig:
    """XGBoost meta-learner for stacking Prophet + QLSTM."""
    n_estimators: int = 100
    max_depth: int = 3
    learning_rate: float = 0.1
    random_state: int = 42
