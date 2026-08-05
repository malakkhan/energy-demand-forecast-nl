"""CMAT configuration: variants, hyperparameters, and search space.

Defines the four ablation variants and all hyperparameters from
Table 4 (thesis), plus the Optuna search space specification.
"""

import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Paths (relative to repo root)
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = REPO_ROOT / "data" / "processed" / "nl_hourly_dataset.parquet"
# Allow runtime override via CMAT_VIIRS_DIR env var (for NVMe staging)
VIIRS_DIR = Path(os.environ.get(
    "CMAT_VIIRS_DIR",
    str(REPO_ROOT.parent / "data" / "viirs" / "A2"),
))
GEOJSON_PATH = REPO_ROOT / "data" / "geo" / "gadm41_NLD_0.json"
OUTPUT_DIR = REPO_ROOT / "src" / "models" / "cmat" / "results"

TARGET = "entsoe_load_mw"

# ---------------------------------------------------------------------------
# ROCV (same protocol as baselines)
# ---------------------------------------------------------------------------
ROCV_START = "2012-01-01"
MIN_TRAIN_YEARS = 2
FOLD_STEP_YEARS = 1
HORIZONS_DAYS = list(range(60, 181, 15))  # [60, 75, ..., 180]

# ---------------------------------------------------------------------------
# NTL image grid (NL bounding-box crop of h18v03 tile)
# ---------------------------------------------------------------------------
# Raw crop is 768 x 985. Padded to 768 x 992 for divisibility by 8/16/32.
NTL_IMG_H = 768
NTL_IMG_W = 992  # padded from 985
NTL_RAW_W = 985  # actual crop width before padding

# NL bounding-box (degrees)
NL_LAT_MIN, NL_LAT_MAX = 50.5, 53.7
NL_LON_MIN, NL_LON_MAX = 3.2, 7.3

# ---------------------------------------------------------------------------
# Feature definitions (36 total = 32 continuous + 4 categorical)
# ---------------------------------------------------------------------------

# 32 continuous features:
#   6 cyclical sin/cos: hour_sin, hour_cos, dow_sin, dow_cos, month_sin, month_cos
#   3 ordinal temporal: day_of_year, week_of_year, quarter
#   4 autoregressive load lags: load_lag_12h, load_lag_24h, load_lag_1w, load_lag_1y
#   3 CCF-optimal weather lags: solar_rad_lag_10h, humidity_lag_12h, temp_lag_107h
#   16 PCA components: pca_cbs_knmi_full_01 ... pca_cbs_knmi_full_16
CONTINUOUS_FEATURES: List[str] = [
    # Cyclical temporal (6)
    "hour_sin", "hour_cos",
    "dow_sin", "dow_cos",
    "month_sin", "month_cos",
    # Ordinal temporal (3)
    "day_of_year", "week_of_year", "quarter",
    # Autoregressive load lags (4)
    "load_lag_12h", "load_lag_24h", "load_lag_1w", "load_lag_1y",
    # CCF-optimal weather lags (3)
    "solar_rad_lag_10h", "humidity_lag_12h", "temp_lag_107h",
    # PCA components (16)
    "pca_cbs_knmi_full_01", "pca_cbs_knmi_full_02", "pca_cbs_knmi_full_03",
    "pca_cbs_knmi_full_04", "pca_cbs_knmi_full_05", "pca_cbs_knmi_full_06",
    "pca_cbs_knmi_full_07", "pca_cbs_knmi_full_08", "pca_cbs_knmi_full_09",
    "pca_cbs_knmi_full_10", "pca_cbs_knmi_full_11", "pca_cbs_knmi_full_12",
    "pca_cbs_knmi_full_13", "pca_cbs_knmi_full_14", "pca_cbs_knmi_full_15",
    "pca_cbs_knmi_full_16",
]

# 4 categorical features with their cardinalities
CATEGORICAL_FEATURES: List[str] = [
    "is_public_holiday",   # 2 levels (0, 1)
    "is_school_holiday",   # 2 levels (0, 1)
    "is_energy_crisis",    # 2 levels (0, 1)
    "year",                # 14 levels (2012–2025)
]
CATEGORICAL_CARDINALITIES: Dict[str, int] = {
    "is_public_holiday": 2,
    "is_school_holiday": 2,
    "is_energy_crisis": 2,
    "year": 14,  # 2012–2025
}
# Offset for year feature so 2012 maps to index 0
YEAR_OFFSET = 2012

ALL_FEATURES = CONTINUOUS_FEATURES + CATEGORICAL_FEATURES
N_CONTINUOUS = len(CONTINUOUS_FEATURES)   # 32
N_CATEGORICAL = len(CATEGORICAL_FEATURES)  # 4

# Features that need horizon-shifting (lag features + PCA)
LAG_FEATURES = [
    "load_lag_12h", "load_lag_24h", "load_lag_1w", "load_lag_1y",
    "solar_rad_lag_10h", "humidity_lag_12h", "temp_lag_107h",
]
PCA_FEATURES = [f"pca_cbs_knmi_full_{i:02d}" for i in range(1, 17)]
FEATURES_TO_SHIFT = LAG_FEATURES + PCA_FEATURES

# NTL aggregate feature for CMAT-NTL variant
NTL_AGGREGATE_FEATURE = "ntl_a2_all_mean"

# Quantiles for probabilistic forecasting
QUANTILES = [0.05, 0.50, 0.95]
N_QUANTILES = len(QUANTILES)

# FFN expansion factor (standard transformer convention)
FFN_EXPANSION = 4


# ---------------------------------------------------------------------------
# Model variants
# ---------------------------------------------------------------------------

class CMATVariant(str, Enum):
    """The four ablation configurations."""
    TAB_ONLY = "tab"        # CMAT-Tab: tabular self-attention only
    TAB_NTL = "ntl"         # CMAT-NTL: tabular + scalar NTL aggregate
    EARLY_FUSION = "early"  # CMAT-Early: both branches, concat + shared SA
    FULL = "full"           # CMAT-Full: both branches, cross-modal attention


# ---------------------------------------------------------------------------
# Hyperparameter configuration
# ---------------------------------------------------------------------------

@dataclass
class CMATConfig:
    """All CMAT hyperparameters.

    Default values are mid-range choices from the search space.
    """
    # Architecture
    context_window_hours: int = 48       # W
    embed_dim: int = 128                 # d
    self_attn_heads: int = 4             # h_s
    cross_attn_heads: int = 4            # h_c
    transformer_depth: int = 2           # L
    ntl_patch_size: int = 16             # P

    # Regularisation
    image_mask_ratio: float = 0.75       # ρ_img
    dropout: float = 0.2                 # p_drop

    # Optimisation
    learning_rate: float = 3e-4          # η
    weight_decay: float = 3e-5           # λ
    batch_size: int = 64                 # B
    max_epochs: int = 50
    early_stop_patience: int = 15
    min_epochs_before_es: int = 10       # Don't allow early stopping before this

    # Cosine annealing
    cosine_T_0: int = 10                 # warm restart period
    cosine_T_mult: int = 2               # period multiplier

    # Variant
    variant: CMATVariant = CMATVariant.FULL

    # Forecast horizon (set per-experiment)
    horizon_hours: int = 60 * 24         # H_pred

    # Reproducibility
    seed: int = 42                       # random seed for torch/numpy/cuda

    def __post_init__(self):
        """Validate head divisibility constraints."""
        if self.embed_dim % self.self_attn_heads != 0:
            raise ValueError(
                f"embed_dim ({self.embed_dim}) must be divisible by "
                f"self_attn_heads ({self.self_attn_heads})"
            )
        if self.embed_dim % self.cross_attn_heads != 0:
            raise ValueError(
                f"embed_dim ({self.embed_dim}) must be divisible by "
                f"cross_attn_heads ({self.cross_attn_heads})"
            )

    @property
    def n_images(self) -> int:
        """Number of NTL images in the lookback window (D_W)."""
        import math
        return max(1, math.ceil(self.context_window_hours / 24))

    @property
    def uses_spatial(self) -> bool:
        """Whether this variant requires the spatial branch."""
        return self.variant in (CMATVariant.EARLY_FUSION, CMATVariant.FULL)

    @property
    def uses_ntl_scalar(self) -> bool:
        """Whether this variant uses the scalar NTL aggregate."""
        return self.variant == CMATVariant.TAB_NTL


# ---------------------------------------------------------------------------
# Optuna search space  (v2 — constrained based on ROCV analysis)
#
# Findings from v1 search across all 9 horizons:
#   - W >= 96h is required for weekly seasonality capture (PCC > 0.96)
#   - embed_dim=128 (500-700K params) >> embed_dim=256 (2.7M+ params)
#   - image_mask_ratio <= 0.75 lets the model actually see satellite data
#   - depth >= 2 avoids shallow underfitting
# ---------------------------------------------------------------------------

# Original broad space (kept for reference):
# SEARCH_SPACE_V1 = {
#     "context_window_hours": [1, 6, 12, 24, 48, 96, 168],
#     "embed_dim": [64, 128, 256],
#     "self_attn_heads": [2, 4, 8],
#     "cross_attn_heads": [2, 4, 8],
#     "transformer_depth": [1, 2, 3],
#     "ntl_patch_size": [8, 16, 32],
#     "image_mask_ratio": [0.50, 0.75, 0.90],
#     "dropout": [0.1, 0.2, 0.3],
#     "learning_rate": (1e-4, 1e-3),
#     "weight_decay": (1e-5, 1e-4),
#     "batch_size": [32, 64, 128],
# }

SEARCH_SPACE = {
    "context_window_hours": [96, 168],
    "embed_dim": [64, 128],
    "self_attn_heads": [2, 4, 8],
    "cross_attn_heads": [2, 4, 8],
    "transformer_depth": [2, 3],
    "ntl_patch_size": [8, 16, 32],
    "image_mask_ratio": [0.25, 0.50, 0.75],
    "dropout": [0.1, 0.2, 0.3],
    "learning_rate": (1e-4, 1e-3),       # log-uniform
    "weight_decay": (1e-5, 1e-4),        # log-uniform
    "batch_size": [32, 64],
}
