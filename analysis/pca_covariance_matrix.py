"""
NL Energy Demand — PCA Covariance Matrix Plot
================================================
Computes and plots the covariance matrix for the reduced feature set:
15 PCA components + satellite + cyclical temporal + lags + flags + target.

Run interactively:
    source .venv/bin/activate
    python analysis/pca_covariance_matrix.py

Run via SLURM:
    sbatch analysis/run_analysis.slurm   (or adapt for this script)
"""

import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")          # must be before importing pyplot
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import seaborn as sns

# ── Style ─────────────────────────────────────────────────────────────────────
plt.rcParams.update({
    "figure.dpi": 120,
    "font.size": 11,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "figure.titlesize": 13,
    "axes.spines.top": False,
    "axes.spines.right": False,
})
sns.set_theme(style="whitegrid", palette="tab10")

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO    = Path(__file__).parent.parent
FIG_DIR = REPO / "analysis" / "figures"
FIG_DIR.mkdir(exist_ok=True)
P_FINAL = REPO / "data" / "processed" / "nl_hourly_dataset.parquet"


# ── Helpers ───────────────────────────────────────────────────────────────────
def savefig(fig: plt.Figure, name: str) -> None:
    path = FIG_DIR / f"{name}.png"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved → {path.relative_to(REPO)}")


def elapsed(t0: float) -> str:
    s = time.time() - t0
    return f"{s:.1f}s" if s < 60 else f"{s/60:.1f}min"


# ══════════════════════════════════════════════════════════════════════════════
# 1  DEFINE FEATURE SET
# ══════════════════════════════════════════════════════════════════════════════
# The 35 columns for the covariance matrix:
#   - 15 PCA components
#   - Satellite (2)
#   - Cyclical temporal (6)
#   - Lag features (7)
#   - Flags (3)
#   - Target (1)
#   = 34 total

COV_COLUMNS = [
    # PCA components (15)
    "pca_cbs_knmi_01", "pca_cbs_knmi_02", "pca_cbs_knmi_03",
    "pca_cbs_knmi_04", "pca_cbs_knmi_05", "pca_cbs_knmi_06",
    "pca_cbs_knmi_07", "pca_cbs_knmi_08", "pca_cbs_knmi_09",
    "pca_cbs_knmi_10", "pca_cbs_knmi_11", "pca_cbs_knmi_12",
    "pca_cbs_knmi_13", "pca_cbs_knmi_14", "pca_cbs_knmi_15",
    # Satellite
    "ntl_a2_all_mean", "ntl_a2_all_sum",
    # Cyclical temporal
    "hour_sin", "hour_cos", "dow_sin", "dow_cos", "month_sin", "month_cos",
    # Lag features
    "load_lag_12h", "load_lag_24h", "load_lag_1w", "load_lag_1y",
    "solar_rad_lag_10h", "humidity_lag_12h", "temp_lag_107h",
    # Flags
    "is_energy_crisis", "is_public_holiday", "is_school_holiday",
    # Target
    "entsoe_load_mw",
]

# Short display labels for the heatmap axes
DISPLAY_LABELS = [
    "PC01", "PC02", "PC03", "PC04", "PC05",
    "PC06", "PC07", "PC08", "PC09", "PC10",
    "PC11", "PC12", "PC13", "PC14", "PC15",
    "ntl_a2_all_mean", "ntl_a2_all_sum",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "month_sin", "month_cos",
    "load_lag_12h", "load_lag_24h", "load_lag_1w", "load_lag_1y",
    "solar_lag_10h", "humid_lag_12h", "temp_lag_107h",
    "is_crisis", "is_pub_hol", "is_sch_hol",
    "load_mw",
]


# ══════════════════════════════════════════════════════════════════════════════
# 2  LOAD DATA
# ══════════════════════════════════════════════════════════════════════════════
t0 = time.time()
print("\n[1] Loading final hourly dataset …")

_first_pq = next(P_FINAL.glob("**/*.parquet"), None)
if _first_pq is None:
    raise FileNotFoundError(f"No parquet files found at {P_FINAL}")
_avail = set(pq.read_schema(_first_pq).names)

# Check which requested columns exist
missing = [c for c in COV_COLUMNS if c not in _avail]
if missing:
    raise ValueError(
        f"Missing columns in final dataset (re-run Phase 3 with --force?): "
        f"{missing}"
    )

hourly = pd.read_parquet(P_FINAL, columns=COV_COLUMNS)
print(f"  rows   : {len(hourly):,}")
print(f"  columns: {len(COV_COLUMNS)}")
print(f"  loaded in {elapsed(t0)}")


# ══════════════════════════════════════════════════════════════════════════════
# 3  COMPUTE AND PLOT COVARIANCE MATRIX
# ══════════════════════════════════════════════════════════════════════════════
print("\n[2] Computing covariance matrix …")
t1 = time.time()

# Drop rows with any NaN in the selected columns
df = hourly[COV_COLUMNS].dropna()
print(f"  complete rows: {len(df):,} (of {len(hourly):,})")

# Compute covariance matrix
cov = df.cov()

# ── Plot ──────────────────────────────────────────────────────────────────
n = len(COV_COLUMNS)
fig, ax = plt.subplots(figsize=(max(16, n * 0.55), max(14, n * 0.50)))

# Use a diverging colormap centered on 0
vmax = np.abs(cov.values).max()

# Determine annotation font size based on matrix size
annot_size = max(5, min(8, 200 // n))

sns.heatmap(
    cov,
    ax=ax,
    annot=True,
    fmt=".1f",
    annot_kws={"size": annot_size},
    cmap="RdBu_r",
    center=0,
    vmin=-vmax,
    vmax=vmax,
    square=True,
    linewidths=0.3,
    linecolor="white",
    xticklabels=DISPLAY_LABELS,
    yticklabels=DISPLAY_LABELS,
    cbar_kws={"shrink": 0.75, "label": "Covariance"},
)

ax.set_title(
    "Covariance Matrix — PCA Components + Satellite + Temporal + Lags + Flags + Target\n"
    f"({len(df):,} complete rows)",
    fontsize=13,
    pad=15,
)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right", fontsize=9)
ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=9)

plt.tight_layout()
savefig(fig, "pca_covariance_matrix")
print(f"  done in {elapsed(t1)}")


# ══════════════════════════════════════════════════════════════════════════════
# 4  COMPUTE AND PLOT CORRELATION MATRIX  (normalised to [−1, +1])
# ══════════════════════════════════════════════════════════════════════════════
print("\n[3] Computing correlation matrix …")
t2 = time.time()

corr = df.corr()

# ── Plot ──────────────────────────────────────────────────────────────────
fig2, ax2 = plt.subplots(figsize=(max(16, n * 0.55), max(14, n * 0.50)))

# Mask upper triangle for cleaner reading
mask = np.triu(np.ones_like(corr, dtype=bool), k=1)

sns.heatmap(
    corr,
    ax=ax2,
    mask=mask,
    annot=True,
    fmt=".2f",
    annot_kws={"size": annot_size},
    cmap="RdBu_r",
    center=0,
    vmin=-1,
    vmax=1,
    square=True,
    linewidths=0.3,
    linecolor="white",
    xticklabels=DISPLAY_LABELS,
    yticklabels=DISPLAY_LABELS,
    cbar_kws={"shrink": 0.75, "label": "Pearson r"},
)

ax2.set_title(
    "Correlation Matrix — PCA Components + Satellite + Temporal + Lags + Flags + Target\n"
    f"({len(df):,} complete rows)",
    fontsize=13,
    pad=15,
)
ax2.set_xticklabels(ax2.get_xticklabels(), rotation=45, ha="right", fontsize=9)
ax2.set_yticklabels(ax2.get_yticklabels(), rotation=0, fontsize=9)

plt.tight_layout()
savefig(fig2, "pca_correlation_matrix")
print(f"  done in {elapsed(t2)}")

print("\n✓ Covariance and correlation matrix plots saved.")

