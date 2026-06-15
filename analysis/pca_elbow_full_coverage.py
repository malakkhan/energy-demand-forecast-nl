#!/usr/bin/env python3
"""PCA Elbow Analysis — Full-Coverage CBS + KNMI Validated Features (2012–2025).

Computes PCA on the subset of CBS + KNMI validated features that have
complete 2012–2025 coverage (i.e., excluding the 13 CBS tariff columns
that only start in 2018 or later).

Produces:
  1. Elbow diagram (scree + cumulative variance) → analysis/figures/
  2. Printed variance table to determine optimal component count

This analysis informs the number of components for the new
``pca_cbs_knmi_full_XX`` columns that will span the full 2012–2025 timeline.
"""

import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

plt.rcParams.update({
    "figure.dpi": 120,
    "font.size": 11,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "figure.titlesize": 13,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

REPO    = Path(__file__).parent.parent
FIG_DIR = REPO / "analysis" / "figures"
FIG_DIR.mkdir(exist_ok=True)
P_FINAL = REPO / "data" / "processed" / "nl_hourly_dataset.parquet"

# CBS tariff columns that only start in 2018+ (must be excluded for full coverage)
CBS_TARIFF_EXCLUDE = [
    "cbs_elec_energy_tax",
    "cbs_elec_energy_tax_refund",
    "cbs_elec_fixed_supply_rate",
    "cbs_elec_fixed_supply_rate_dynamic",
    "cbs_elec_ode_tax",
    "cbs_elec_total_tax",
    "cbs_elec_transport_rate",
    "cbs_elec_variable_supply_rate_dynamic",
    "cbs_gas_energy_tax",
    "cbs_gas_fixed_supply_rate",
    "cbs_gas_ode_tax",
    "cbs_gas_total_tax",
    "cbs_gas_transport_rate",
]


def savefig(fig, name):
    path = FIG_DIR / f"{name}.png"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved → {path.relative_to(REPO)}")


# ══════════════════════════════════════════════════════════════════════════
# 1. Load data
# ══════════════════════════════════════════════════════════════════════════
print("\n[1] Loading dataset …")
t0 = time.time()

schema = pq.read_schema(next(P_FINAL.glob("**/*.parquet")))
avail = set(schema.names)

cbs_cols = sorted(c for c in avail if c.startswith("cbs_"))
knmi_val_cols = sorted(c for c in avail if c.startswith("knmi_val_"))

# Exclude CBS tariff columns
cbs_full_coverage = sorted(set(cbs_cols) - set(CBS_TARIFF_EXCLUDE))
all_input_cols = sorted(set(cbs_full_coverage + knmi_val_cols))

print(f"  CBS total:          {len(cbs_cols)}")
print(f"  CBS excluded:       {len(CBS_TARIFF_EXCLUDE)} (tariff/2018+)")
print(f"  CBS full-coverage:  {len(cbs_full_coverage)}")
print(f"  KNMI validated:     {len(knmi_val_cols)}")
print(f"  PCA input total:    {len(all_input_cols)}")

hourly = pd.read_parquet(P_FINAL, columns=all_input_cols)
print(f"  Rows: {len(hourly):,}")
print(f"  Loaded in {time.time() - t0:.1f}s")


# ══════════════════════════════════════════════════════════════════════════
# 2. Check NaN coverage of each remaining column
# ══════════════════════════════════════════════════════════════════════════
print("\n[2] Checking column coverage …")
missing_frac = hourly[all_input_cols].isna().mean()

# Verify all remaining columns have <=50% NaN
high_nan = missing_frac[missing_frac > 0.50]
if len(high_nan) > 0:
    print(f"  WARNING: {len(high_nan)} columns still have >50% NaN:")
    for c, pct in high_nan.items():
        print(f"    {c}: {pct*100:.1f}%")
    # Drop them
    keep_cols = sorted(missing_frac[missing_frac <= 0.50].index.tolist())
else:
    keep_cols = all_input_cols
    print(f"  All {len(keep_cols)} columns have ≤50% NaN ✓")

# Also print any columns with >0% NaN
any_nan = missing_frac[missing_frac > 0]
if len(any_nan) > 0:
    print(f"\n  Columns with some NaN (but ≤50%):")
    for c, pct in sorted(any_nan.items(), key=lambda x: -x[1]):
        print(f"    {c}: {pct*100:.1f}%")
else:
    print(f"  All columns have 0% NaN ✓")


# ══════════════════════════════════════════════════════════════════════════
# 3. Run PCA
# ══════════════════════════════════════════════════════════════════════════
print(f"\n[3] Running PCA on {len(keep_cols)} features …")

X = hourly[keep_cols].dropna()
print(f"  Complete rows: {len(X):,} / {len(hourly):,} ({100*len(X)/len(hourly):.1f}%)")

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

n_max = min(X_scaled.shape[0], X_scaled.shape[1])
pca = PCA(n_components=n_max)
pca.fit(X_scaled)

var_ratio = pca.explained_variance_ratio_
cum_var = np.cumsum(var_ratio)
n_comp = np.arange(1, len(var_ratio) + 1)


# ══════════════════════════════════════════════════════════════════════════
# 4. Print variance table
# ══════════════════════════════════════════════════════════════════════════
print(f"\n{'PC':>4}  {'Var Ratio':>10}  {'Cumulative':>10}")
print(f"{'─'*4}  {'─'*10}  {'─'*10}")
for i in range(min(30, len(var_ratio))):
    marker = ""
    if cum_var[i] >= 0.80 and (i == 0 or cum_var[i-1] < 0.80):
        marker = " ← 80%"
    elif cum_var[i] >= 0.90 and (i == 0 or cum_var[i-1] < 0.90):
        marker = " ← 90%"
    elif cum_var[i] >= 0.95 and (i == 0 or cum_var[i-1] < 0.95):
        marker = " ← 95%"
    print(f"{i+1:>4}  {var_ratio[i]:>10.4f}  {cum_var[i]:>10.4f}{marker}")

# Key thresholds
for thresh, label in [(0.80, "80%"), (0.90, "90%"), (0.95, "95%")]:
    idx = np.searchsorted(cum_var, thresh)
    if idx < len(cum_var):
        print(f"\n  {label} variance → {idx+1} components")


# ══════════════════════════════════════════════════════════════════════════
# 5. Elbow diagram
# ══════════════════════════════════════════════════════════════════════════
print("\n[4] Generating elbow diagram …")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle(
    f"PCA Elbow Diagram — CBS + KNMI Val (Full Coverage, 2012–2025)\n"
    f"({len(keep_cols)} features, {len(CBS_TARIFF_EXCLUDE)} CBS tariff cols excluded, "
    f"{len(X):,} complete rows)",
    fontsize=13,
)

# Show first 30 components (or all if fewer)
n_show = min(30, len(var_ratio))
n_plot = n_comp[:n_show]
var_plot = var_ratio[:n_show]
cum_plot = cum_var[:n_show]

# Panel 1: individual variance
ax1.bar(n_plot, var_plot, color="steelblue", alpha=0.7, edgecolor="white", linewidth=0.5)
ax1.plot(n_plot, var_plot, "o-", color="firebrick", ms=4, lw=1.4)
ax1.set_xlabel("Principal Component")
ax1.set_ylabel("Explained Variance Ratio")
ax1.set_title("Scree Plot (Individual)")
ax1.set_xticks(n_plot)
ax1.set_xticklabels([str(i) for i in n_plot], fontsize=7, rotation=45)

# Panel 2: cumulative variance
ax2.plot(n_plot, cum_plot, "o-", color="steelblue", lw=2, ms=5, label="Cumulative")
ax2.fill_between(n_plot, 0, cum_plot, alpha=0.12, color="steelblue")

for thresh, lbl, color in [(0.80, "80%", "seagreen"), (0.90, "90%", "darkorange"),
                            (0.95, "95%", "firebrick")]:
    ax2.axhline(thresh, ls="--", lw=1.2, color=color, alpha=0.7)
    idx = np.searchsorted(cum_var, thresh)
    if idx < len(cum_var) and idx < n_show:
        n_needed = int(n_comp[idx])
        ax2.plot(n_needed, cum_var[idx], "D", color=color, ms=8, zorder=5)
        ax2.annotate(
            f"{lbl}: {n_needed} PCs",
            xy=(n_needed, cum_var[idx]),
            xytext=(12, -18 if thresh < 0.95 else 12),
            textcoords="offset points",
            fontsize=9, fontweight="bold", color=color,
            arrowprops=dict(arrowstyle="->", color=color, lw=1.2),
        )

ax2.set_xlabel("Number of Principal Components")
ax2.set_ylabel("Cumulative Explained Variance")
ax2.set_title("Cumulative Explained Variance")
ax2.set_ylim(0, 1.05)
ax2.set_xticks(n_plot)
ax2.set_xticklabels([str(i) for i in n_plot], fontsize=7, rotation=45)
ax2.legend(fontsize=9, loc="lower right")

plt.tight_layout()
savefig(fig, "pca_elbow_cbs_knmi_val_full_coverage")

print("\n✓ Done.")
