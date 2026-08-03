"""
NL Energy Demand — PCA Elbow Chart for Lagged Load Features
=============================================================
Runs PCA on the 4 lagged load features (load_lag_12h, load_lag_24h,
load_lag_1w, load_lag_1y) and saves a scree / cumulative-variance
elbow diagram.

Usage:
    source .venv/bin/activate
    python analysis/pca_elbow_load_lags.py
"""

import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

# ── Style ─────────────────────────────────────────────────────────────────────
plt.rcParams.update({
    "figure.dpi": 150,
    "font.size": 11,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "figure.titlesize": 14,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO    = Path(__file__).parent.parent
FIG_DIR = REPO / "analysis" / "figures"
FIG_DIR.mkdir(exist_ok=True)
P_FINAL = REPO / "data" / "processed" / "nl_hourly_dataset.parquet"

# ── Feature set ───────────────────────────────────────────────────────────────
LOAD_LAG_COLS = [
    "load_lag_12h",
    "load_lag_24h",
    "load_lag_1w",
    "load_lag_1y",
]

# ══════════════════════════════════════════════════════════════════════════════
print("\n[1] Loading final hourly dataset …")
t0 = time.time()

df = pd.read_parquet(P_FINAL, columns=LOAD_LAG_COLS)
print(f"  rows: {len(df):,}  columns: {len(LOAD_LAG_COLS)}")

# Drop rows with any NaN
df_clean = df.dropna()
n_complete = len(df_clean)
print(f"  complete rows: {n_complete:,} (of {len(df):,})")

# ══════════════════════════════════════════════════════════════════════════════
print("\n[2] Running PCA …")

X = df_clean.values.astype(np.float64)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

pca = PCA(n_components=len(LOAD_LAG_COLS))
pca.fit(X_scaled)

var = pca.explained_variance_ratio_
cum_var = np.cumsum(var)
n_comp = len(var)

print(f"\n  Explained variance per component:")
for i, (v, cv) in enumerate(zip(var, cum_var), 1):
    print(f"    PC{i}: {v:.4f}  (cumulative {cv:.4f}  = {cv*100:.1f}%)")

# Find thresholds
for thresh in [0.80, 0.90, 0.95]:
    k = int(np.searchsorted(cum_var, thresh) + 1)
    print(f"  {thresh*100:.0f}% threshold → {k} component{'s' if k > 1 else ''}")

# ══════════════════════════════════════════════════════════════════════════════
print("\n[3] Plotting elbow diagram …")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5))

comp_labels = [f"PC{i}" for i in range(1, n_comp + 1)]
x = np.arange(1, n_comp + 1)

# --- Left: individual explained variance (scree plot) ---
bars = ax1.bar(x, var * 100, color="#4361ee", edgecolor="white", width=0.6)
ax1.set_xlabel("Principal Component")
ax1.set_ylabel("Explained Variance (%)")
ax1.set_title("Scree Plot — Lagged Load Features")
ax1.set_xticks(x)
ax1.set_xticklabels(comp_labels)
ax1.set_ylim(0, max(var) * 100 * 1.15)

# Annotate bars
for bar, v in zip(bars, var):
    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
             f"{v*100:.1f}%", ha="center", va="bottom", fontsize=10, fontweight="bold")

# --- Right: cumulative explained variance ---
ax2.plot(x, cum_var * 100, "o-", color="#4361ee", linewidth=2, markersize=8)
ax2.fill_between(x, 0, cum_var * 100, alpha=0.10, color="#4361ee")
ax2.set_xlabel("Number of Components")
ax2.set_ylabel("Cumulative Explained Variance (%)")
ax2.set_title("Cumulative Variance — Lagged Load Features")
ax2.set_xticks(x)
ax2.set_xticklabels(comp_labels)
ax2.set_ylim(0, 105)
ax2.set_xlim(0.5, n_comp + 0.5)

# Threshold lines
for thresh, color, style in [
    (80, "#22c55e", "--"),
    (90, "#f59e0b", "-."),
    (95, "#ef4444", ":"),
]:
    ax2.axhline(thresh, color=color, linestyle=style, linewidth=1.2, alpha=0.7)
    ax2.text(n_comp + 0.35, thresh + 0.5, f"{thresh}%", color=color,
             fontsize=9, va="bottom", fontweight="bold")

# Annotate points
for xi, cv in zip(x, cum_var):
    ax2.annotate(f"{cv*100:.1f}%", (xi, cv * 100),
                 textcoords="offset points", xytext=(0, 12),
                 ha="center", fontsize=10, fontweight="bold")

# Add info box
info = (f"Features: {n_comp}\n"
        f"Complete rows: {n_complete:,}\n"
        f"Columns: {', '.join(LOAD_LAG_COLS)}")
ax1.text(0.98, 0.97, info, transform=ax1.transAxes,
         fontsize=8, va="top", ha="right",
         bbox=dict(boxstyle="round,pad=0.4", facecolor="white",
                   edgecolor="#cbd5e1", alpha=0.9))

fig.suptitle("PCA Elbow Diagram — Lagged Load Features (4 features)",
             fontsize=14, y=1.02)
plt.tight_layout()

out_path = FIG_DIR / "pca_elbow_load_lags.png"
fig.savefig(out_path, bbox_inches="tight")
plt.close(fig)
print(f"  saved → {out_path.relative_to(REPO)}")

print(f"\n✓ Done in {time.time() - t0:.1f}s.")
