"""Configuration for Prophet-LSTM baseline model.

Faithfully reproduces the architecture and preprocessing from the
reference paper (van de Sande et al., 2024), adapted to use the
NL energy pipeline's hourly Parquet dataset (without NTL data).

All hyperparameters are taken directly from the paper unless noted.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List

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
# Predictor groups  (adapted from the reference paper's predictor sets)
# ---------------------------------------------------------------------------

# Weather features — prefer validated KNMI, fall back to non-validated
WEATHER_FEATURES_VAL = [
    "knmi_val_temp_c",
    "knmi_val_dewpoint_c",
    "knmi_val_wind_speed_ms",
    "knmi_val_wind_gust_ms",
    "knmi_val_solar_rad_jcm2",
    "knmi_val_humidity_pct",
]
WEATHER_FEATURES_NONVAL = [
    "knmi_temp_c",
    "knmi_dewpoint_c",
    "knmi_wind_speed_ms",
    "knmi_wind_gust_ms",
    "knmi_solar_rad_jcm2",
    "knmi_humidity_pct",
]

# CBS economic indicators (analogous to the paper's price predictors)
CBS_FEATURES = [
    "cbs_cpi_energy",
    "cbs_cpi_electricity",
    "cbs_cpi_gas",
    "cbs_gep_gas_hh_total",
    "cbs_gep_elec_hh_total",
    "cbs_gdp_yy",
    "cbs_consumption_hh_yy",
    "cbs_population_million",
]

# Prophet component columns (generated at runtime)
PROPHET_COMPONENTS = ["yhat", "trend", "daily", "weekly", "yearly"]


# ---------------------------------------------------------------------------
# Rolling-Origin Cross-Validation
# ---------------------------------------------------------------------------
@dataclass
class ROCVConfig:
    """Rolling-Origin Cross-Validation parameters.

    Matches the thesis design: expanding window, 1-year advances,
    minimum 2 years of training data.
    """
    start_date: str = "2012-01-01"
    min_train_years: int = 2
    fold_step_years: int = 1
    forecast_horizons_days: List[int] = field(
        default_factory=lambda: list(range(60, 181, 15))
    )  # [60, 75, 90, 105, 120, 135, 150, 165, 180]


# ---------------------------------------------------------------------------
# Prophet
# ---------------------------------------------------------------------------
@dataclass
class ProphetConfig:
    """Prophet configuration — matches the reference implementation."""
    daily_seasonality: bool = True
    weekly_seasonality: bool = True
    yearly_seasonality: bool = True
    # The reference does NOT set country holidays in Prophet.
    # The daily/weekly/yearly seasonalities handle it.
    add_country_holidays: str = ""  # set to "NL" to enable
    suppress_logging: bool = True


# ---------------------------------------------------------------------------
# LSTM
# ---------------------------------------------------------------------------
@dataclass
class LSTMConfig:
    """LSTM hyperparameters — faithfully matching the reference paper.

    Model 1 (Huang):
        LSTM(30, relu, return_sequences) → LSTM(90, relu) → Dense(1)
        optimizer = adam

    Model 2 (Tan):
        LSTM(30, sigmoid, return_sequences) → Dropout(0.2) →
        LSTM(60, tanh) → Dropout(0.3) → Dense(1, linear)
        optimizer = rmsprop
    """
    # Architecture
    lstm1_units: int = 30
    lstm2_units_huang: int = 90     # Model 1
    lstm2_units_tan: int = 60       # Model 2
    dropout_rate_1: float = 0.2     # Model 2, after LSTM1
    dropout_rate_2: float = 0.3     # Model 2, after LSTM2
    # Training
    epochs: int = 50
    batch_size: int = 128
    learning_rate: float = 1e-3
    # Early stopping — paper says patience=3, start_from_epoch=8
    early_stop_patience: int = 3
    early_stop_min_delta: float = 1e-4
    early_stop_start_epoch: int = 8


# ---------------------------------------------------------------------------
# Outlier removal (IQR method from the paper)
# ---------------------------------------------------------------------------
IQR_MULTIPLIER = 1.5

# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------
METRICS = ["rmse", "mae", "mape_pct", "pearson_r"]
