"""
NL Energy Demand — PCA Elbow Diagrams by Feature Group
=======================================================
Performs PCA on three feature groups from the final hourly dataset
and saves elbow diagrams (explained variance ratio per component +
cumulative explained variance) to analysis/figures/.

Feature groups (from the feature dictionary):
  1. CBS features        — all columns starting with ``cbs_``
  2. KNMI validated      — all columns starting with ``knmi_val_``
  3. CBS + KNMI validated — both groups combined

Run interactively:
    source .venv/bin/activate
    python analysis/pca_elbow.py

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
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

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
# 1  LOAD DATA
# ══════════════════════════════════════════════════════════════════════════════
t0 = time.time()
print("\n[1] Loading final hourly dataset …")

_first_pq = next(P_FINAL.glob("**/*.parquet"), None)
if _first_pq is None:
    raise FileNotFoundError(f"No parquet files found at {P_FINAL}")
_avail = set(pq.read_schema(_first_pq).names)

# Identify feature groups by prefix from the available columns
cbs_cols      = sorted([c for c in _avail if c.startswith("cbs_")])
knmi_val_cols = sorted([c for c in _avail if c.startswith("knmi_val_")])
combined_cols = sorted(set(cbs_cols + knmi_val_cols))

# Load only the columns we need
all_needed = sorted(set(cbs_cols + knmi_val_cols))
hourly = pd.read_parquet(P_FINAL, columns=[c for c in all_needed if c in _avail])

print(f"  rows           : {len(hourly):,}")
print(f"  CBS features   : {len(cbs_cols)} columns")
print(f"  KNMI val feats : {len(knmi_val_cols)} columns")
print(f"  combined       : {len(combined_cols)} columns")
print(f"  loaded in {elapsed(t0)}")


# ══════════════════════════════════════════════════════════════════════════════
# 2  PCA HELPER
# ══════════════════════════════════════════════════════════════════════════════
def run_pca_and_plot(
    df: pd.DataFrame,
    cols: list[str],
    group_label: str,
    fig_name: str,
) -> None:
    """
    Run PCA on *cols* of *df*, produce a two-panel elbow diagram, and save it.

    Panel 1: Individual explained variance ratio per component (bar + line).
    Panel 2: Cumulative explained variance with 80 %, 90 %, 95 % thresholds.

    Columns with > 50 % missing values are dropped before row-wise dropna,
    because CBS features have staggered temporal coverage (some only exist
    for a few years) and keeping them would eliminate all complete rows.
    """
    t1 = time.time()
    print(f"\n[PCA] {group_label} ({len(cols)} features) …")

    # ── Filter out columns with > 50 % NaN (too sparse for PCA) ───────────
    subset = df[cols]
    missing_frac = subset.isna().mean()
    keep_cols = sorted(missing_frac[missing_frac <= 0.50].index.tolist())
    drop_cols = sorted(set(cols) - set(keep_cols))

    if drop_cols:
        print(f"    dropped {len(drop_cols)} columns (>50 % NaN):")
        for c in drop_cols:
            pct = missing_frac[c] * 100
            print(f"      - {c}  ({pct:.1f} % missing)")

    if not keep_cols:
        print("    SKIPPED (no columns with ≤ 50 % coverage)")
        return

    # ── Drop remaining rows with any NaN among the kept columns ───────────
    X = df[keep_cols].dropna()
    if X.empty:
        print("    SKIPPED (no complete rows after dropping NaNs)")
        return

    print(f"    kept {len(keep_cols)} features, {len(X):,} complete rows …",
          end=" ", flush=True)

    # ── Standardise (zero-mean, unit-variance) ────────────────────────────
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # ── Fit PCA with all components ───────────────────────────────────────
    n_components = min(X_scaled.shape[0], X_scaled.shape[1])
    pca = PCA(n_components=n_components)
    pca.fit(X_scaled)

    var_ratio = pca.explained_variance_ratio_
    cum_var   = np.cumsum(var_ratio)
    n_comp    = np.arange(1, len(var_ratio) + 1)

    # ── Plot ──────────────────────────────────────────────────────────────
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle(
        f"PCA Elbow Diagram — {group_label}\n"
        f"({len(keep_cols)} of {len(cols)} features retained, {len(X):,} rows)",
        fontsize=13,
    )

    # Panel 1: individual explained variance ratio
    ax1.bar(n_comp, var_ratio, color="steelblue", alpha=0.7, edgecolor="white",
            linewidth=0.5, label="Individual")
    ax1.plot(n_comp, var_ratio, "o-", color="firebrick", ms=4, lw=1.4,
             label="Individual (line)")
    ax1.set_xlabel("Principal Component")
    ax1.set_ylabel("Explained Variance Ratio")
    ax1.set_title("Scree Plot (Individual)")
    ax1.set_xticks(n_comp)
    ax1.set_xticklabels(
        [str(i) for i in n_comp],
        fontsize=max(6, 11 - len(n_comp) // 6),
        rotation=45 if len(n_comp) > 15 else 0,
    )
    ax1.legend(fontsize=9)

    # Panel 2: cumulative explained variance
    ax2.plot(n_comp, cum_var, "o-", color="steelblue", lw=2, ms=5,
             label="Cumulative")
    ax2.fill_between(n_comp, 0, cum_var, alpha=0.12, color="steelblue")

    # Threshold lines and annotations
    thresholds = [(0.80, "80 %", "seagreen"), (0.90, "90 %", "darkorange"),
                  (0.95, "95 %", "firebrick")]
    for thresh, lbl, color in thresholds:
        ax2.axhline(thresh, ls="--", lw=1.2, color=color, alpha=0.7)
        # Find the first component that reaches this threshold
        idx = np.searchsorted(cum_var, thresh)
        if idx < len(cum_var):
            n_needed = int(n_comp[idx])
            ax2.plot(n_needed, cum_var[idx], "D", color=color, ms=8, zorder=5)
            ax2.annotate(
                f"{lbl}: {n_needed} PC{'s' if n_needed != 1 else ''}",
                xy=(n_needed, cum_var[idx]),
                xytext=(12, -18 if thresh < 0.95 else 12),
                textcoords="offset points",
                fontsize=9, fontweight="bold", color=color,
                arrowprops=dict(arrowstyle="->", color=color, lw=1.2),
            )
            print(f"\n    {lbl} variance → {n_needed} components", end="")

    ax2.set_xlabel("Number of Principal Components")
    ax2.set_ylabel("Cumulative Explained Variance")
    ax2.set_title("Cumulative Explained Variance")
    ax2.set_ylim(0, 1.05)
    ax2.set_xticks(n_comp)
    ax2.set_xticklabels(
        [str(i) for i in n_comp],
        fontsize=max(6, 11 - len(n_comp) // 6),
        rotation=45 if len(n_comp) > 15 else 0,
    )
    ax2.legend(fontsize=9, loc="lower right")

    plt.tight_layout()
    savefig(fig, fig_name)
    print(f"\n    done in {elapsed(t1)}")

    # ── Print summary table ───────────────────────────────────────────────
    print(f"\n    {'PC':>4}  {'Var Ratio':>10}  {'Cumulative':>10}")
    print(f"    {'─'*4}  {'─'*10}  {'─'*10}")
    for i in range(len(var_ratio)):
        print(f"    {i+1:>4}  {var_ratio[i]:>10.4f}  {cum_var[i]:>10.4f}")


# ══════════════════════════════════════════════════════════════════════════════
# 3  RUN PCA FOR EACH FEATURE GROUP
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 72)
print("  PCA ELBOW DIAGRAMS")
print("=" * 72)

# 3.1 — CBS features only
run_pca_and_plot(
    hourly, cbs_cols,
    group_label="CBS Features",
    fig_name="pca_elbow_cbs",
)

# 3.2 — KNMI validated features only
run_pca_and_plot(
    hourly, knmi_val_cols,
    group_label="KNMI Validated Features",
    fig_name="pca_elbow_knmi_val",
)

# 3.3 — CBS + KNMI validated combined
run_pca_and_plot(
    hourly, combined_cols,
    group_label="CBS + KNMI Validated Features (Combined)",
    fig_name="pca_elbow_cbs_knmi_val",
)

print("\n\n✓ All PCA elbow diagrams saved.")
