"""
NL Energy Demand — Time Series Analysis (batch script)
=======================================================
Produces all EDA figures and saves them to analysis/figures/.
Designed to be run as an sbatch job so that:

  - matplotlib uses the non-interactive Agg backend (no SSH rendering overhead)
  - CCF uses scipy.signal.correlate (FFT-based, O(n log n) vs O(n²))
  - ACF/PACF uses FFT-accelerated ACF and Yule-Walker PACF on the full dataset
  - VIF is computed on a 50K-row subsample

Sections:
  1. Data loading (ENTSO-E, VIIRS, CBS, KNMI)
  2. ENTSO-E hourly load analysis (time series, STL, ACF/PACF, subseries)
  3. VIIRS A2 daily NTL analysis
  4. VIIRS A1 daily NTL analysis + A1/A2 comparison
  5. CBS monthly indicators
  6. Multicollinearity (correlation, VIF, CCF, pair plot)
  7. KNMI meteorological analysis (time series, STL, ACF/PACF, weather–load)
  8. KNMI validated meteorological analysis
  9. Validated vs non-validated KNMI comparison

Run interactively:
    source .venv/bin/activate
    python analysis/run_analysis.py

Run via SLURM:
    sbatch src/pipeline/run_analysis.slurm
"""

import json
import time
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")          # must be before importing pyplot
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import seaborn as sns
from scipy.signal import correlate as sci_correlate
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.stats.outliers_influence import variance_inflation_factor
from statsmodels.tsa.seasonal import STL
from statsmodels.tsa.stattools import acf

warnings.filterwarnings("ignore")

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

MONTH_NAMES   = ["Jan","Feb","Mar","Apr","May","Jun",
                 "Jul","Aug","Sep","Oct","Nov","Dec"]
DOW_SUN_FIRST = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"]
DOW_MON_FIRST = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO     = Path(__file__).parent.parent
FIG_DIR  = REPO / "analysis" / "figures"
FIG_DIR.mkdir(exist_ok=True)

P_FINAL  = REPO / "data" / "processed" / "nl_hourly_dataset.parquet"
P_P2_A2  = REPO / "data" / "processing_2" / "viirs_a2_daily" / "data"
P_P2_A1  = REPO / "data" / "processing_2" / "viirs_a1_daily" / "data"
P_P2_CBS = REPO / "data" / "processing_2" / "cbs_combined"   / "data"
P_P2_KNMI= REPO / "data" / "processing_2" / "knmi"           / "data"
P_P2_KNMI_VAL = REPO / "data" / "processing_2" / "knmi_validated" / "data"


# ── Helpers ───────────────────────────────────────────────────────────────────
def savefig(fig: plt.Figure, name: str) -> None:
    path = FIG_DIR / f"{name}.png"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved → {path.relative_to(REPO)}")


def elapsed(t0: float) -> str:
    s = time.time() - t0
    return f"{s:.1f}s" if s < 60 else f"{s/60:.1f}min"


def ccf_fft(x: np.ndarray, y: np.ndarray, max_lag: int):
    """FFT-based normalised cross-correlation — O(n log n) not O(n²)."""
    x_z = (x - x.mean()) / (x.std() + 1e-12)
    y_z = (y - y.mean()) / (y.std() + 1e-12)
    full = sci_correlate(x_z, y_z, mode="full", method="fft") / len(x_z)
    mid  = len(full) // 2
    lags = np.arange(-max_lag, max_lag + 1)
    return lags, full[mid - max_lag: mid + max_lag + 1]


# ── Results accumulator (written to JSON at the end) ─────────────────────────
R = {}   # all numerical outputs collected here

# ══════════════════════════════════════════════════════════════════════════════
# 1  LOAD DATA
# ══════════════════════════════════════════════════════════════════════════════
t0 = time.time()
print("\n[1] Loading data …")

_first_pq = next(P_FINAL.glob("**/*.parquet"), None)
if _first_pq is None:
    raise FileNotFoundError(f"No parquet files found at {P_FINAL}")
_avail = set(pq.read_schema(_first_pq).names)

_want = [
    "timestamp", "entsoe_load_mw",
    "ntl_a2_mean", "ntl_a1_mean",
    "cbs_cpi_energy", "cbs_cpi_electricity", "cbs_cpi_gas",
    "cbs_gep_gas_hh_total", "cbs_gep_elec_hh_total",
    "cbs_gas_total_tax", "cbs_elec_total_tax",
    "cbs_gdp_yy", "cbs_consumption_hh_yy", "cbs_population_million",
    "knmi_temp_c", "knmi_humidity_pct", "knmi_wind_speed_ms",
    "knmi_solar_rad_jcm2",
    "knmi_val_temp_c", "knmi_val_humidity_pct", "knmi_val_wind_speed_ms",
    "knmi_val_solar_rad_jcm2",
    "year", "month", "day", "hour", "day_of_week", "is_weekend",
]
hourly = pd.read_parquet(P_FINAL, columns=[c for c in _want if c in _avail])
hourly["timestamp"] = pd.to_datetime(hourly["timestamp"])
hourly = hourly.sort_values("timestamp").set_index("timestamp")
print(f"  hourly  : {len(hourly):,} rows  ({hourly.index.min().date()} → {hourly.index.max().date()})")

_vcols = ["date","ntl_mean","ntl_sum","ntl_valid_count","ntl_fill_count","ntl_invalid_count"]
viirs_a2 = pd.read_parquet(P_P2_A2, columns=_vcols)
viirs_a2["date"] = pd.to_datetime(viirs_a2["date"])
viirs_a2 = viirs_a2.sort_values("date").set_index("date")
print(f"  VIIRS A2: {len(viirs_a2):,} rows")

viirs_a1 = pd.read_parquet(P_P2_A1, columns=_vcols)
viirs_a1["date"] = pd.to_datetime(viirs_a1["date"])
viirs_a1 = viirs_a1.sort_values("date").set_index("date")
print(f"  VIIRS A1: {len(viirs_a1):,} rows")

cbs = pd.read_parquet(P_P2_CBS)
cbs["date"] = pd.to_datetime(cbs[["year","month"]].assign(day=1))
cbs = cbs.sort_values("date").set_index("date")
print(f"  CBS     : {len(cbs):,} rows, {len(cbs.columns)} columns")

_knmi_cols = ["timestamp_utc","knmi_temp_c","knmi_dewpoint_c",
              "knmi_wind_speed_ms","knmi_wind_speed_hourly_ms","knmi_wind_gust_ms",
              "knmi_solar_rad_jcm2","knmi_sunshine_h","knmi_humidity_pct"]
if P_P2_KNMI.exists():
    knmi = pd.read_parquet(P_P2_KNMI, columns=[c for c in _knmi_cols])
    knmi["timestamp_utc"] = pd.to_datetime(knmi["timestamp_utc"])
    knmi = knmi.sort_values("timestamp_utc").set_index("timestamp_utc")
    print(f"  KNMI    : {len(knmi):,} rows  ({knmi.index.min()} → {knmi.index.max()})")
else:
    knmi = pd.DataFrame()
    print("  KNMI    : not found — skipping KNMI analysis")

_knmi_val_cols = ["timestamp_utc","knmi_val_temp_c","knmi_val_dewpoint_c",
              "knmi_val_wind_speed_ms","knmi_val_wind_speed_hourly_ms","knmi_val_wind_gust_ms",
              "knmi_val_solar_rad_jcm2","knmi_val_sunshine_h","knmi_val_humidity_pct"]
if P_P2_KNMI_VAL.exists():
    knmi_val = pd.read_parquet(P_P2_KNMI_VAL, columns=[c for c in _knmi_val_cols])
    knmi_val["timestamp_utc"] = pd.to_datetime(knmi_val["timestamp_utc"])
    knmi_val = knmi_val.sort_values("timestamp_utc").set_index("timestamp_utc")
    print(f"  KNMI Val: {len(knmi_val):,} rows  ({knmi_val.index.min()} → {knmi_val.index.max()})")
else:
    knmi_val = pd.DataFrame()
    print("  KNMI Val: not found — skipping validated KNMI analysis")
print(f"  data loaded in {elapsed(t0)}")


# ══════════════════════════════════════════════════════════════════════════════
# 2  ENTSO-E HOURLY LOAD
# ══════════════════════════════════════════════════════════════════════════════
entsoe = hourly["entsoe_load_mw"].dropna()
e_df   = hourly[["entsoe_load_mw","hour","day_of_week","month"]].dropna(
             subset=["entsoe_load_mw"]).rename(columns={"entsoe_load_mw": "load"})

# 2.1 ── Time series
print("\n[2.1] ENTSOE time series …", end=" ", flush=True)
t1 = time.time()
fig, axes = plt.subplots(2, 1, figsize=(18, 9))
fig.suptitle("ENTSO-E Hourly Electricity Load — Netherlands")
axes[0].plot(entsoe.index, entsoe.values, lw=0.25, color="steelblue", alpha=0.5, label="Hourly")
axes[0].plot(entsoe.rolling(24*30, center=True).mean().index,
             entsoe.rolling(24*30, center=True).mean().values,
             color="firebrick", lw=2, label="30-day rolling mean")
axes[0].set_ylabel("Load (MW)"); axes[0].legend(fontsize=10)
axes[0].set_title("Full hourly series with 30-day rolling mean")
axes[0].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
grp = entsoe.groupby(entsoe.index.year)
ann_mean, ann_std = grp.mean(), grp.std()
dt_ann = pd.to_datetime([f"{y}-07-01" for y in ann_mean.index])
axes[1].bar(dt_ann, ann_mean.values, width=300, color="steelblue", alpha=0.7, label="Annual mean")
axes[1].errorbar(dt_ann, ann_mean.values, yerr=ann_std.values,
                 fmt="none", color="black", capsize=4, lw=1, label="±1 std")
axes[1].set_ylabel("Load (MW)"); axes[1].set_title("Annual mean ± 1 std")
axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
axes[1].legend(fontsize=10)
plt.tight_layout(); savefig(fig, "entsoe_01_timeseries"); print(elapsed(t1))
R["entsoe_annual"] = {str(y): {"mean_mw": round(float(m), 1), "std_mw": round(float(s), 1)}
                      for y, m, s in zip(ann_mean.index, ann_mean.values, ann_std.values)}

# 2.2 ── STL
print("[2.2] ENTSOE STL …", end=" ", flush=True)
t1 = time.time()
stl_in = entsoe.loc["2022":"2023"].asfreq("h").ffill()
stl_e  = STL(stl_in, period=24*7).fit()
fig, axes = plt.subplots(4, 1, figsize=(18, 12), sharex=True)
fig.suptitle("STL Decomposition — ENTSO-E Hourly Load  (period = 168 h = 1 week)")
for ax, (label, data, color) in zip(axes, [
    ("Observed", stl_in.values, "steelblue"), ("Trend", stl_e.trend, "firebrick"),
    ("Seasonal", stl_e.seasonal, "seagreen"), ("Residual", stl_e.resid, "darkorange"),
]):
    ax.plot(stl_in.index, data, color=color, lw=0.3 if label in ("Observed","Residual") else 1.4)
    ax.set_ylabel(label, fontsize=10); ax.axhline(0, color="black", lw=0.4, ls="--")
axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
plt.tight_layout(); savefig(fig, "entsoe_02_stl"); print(elapsed(t1))
obs_var = float(np.var(stl_in.values))
_stl_pct = {}
for name, comp in [("Trend",stl_e.trend),("Seasonal",stl_e.seasonal),("Residual",stl_e.resid)]:
    pct = round(100*float(np.var(comp))/obs_var, 1)
    _stl_pct[name.lower()] = pct
    print(f"  {name}: {pct}%")
R["entsoe_stl_variance_pct"] = _stl_pct

# 2.3 ── ACF / PACF
print("[2.3] ENTSOE ACF/PACF …", end=" ", flush=True)
t1 = time.time()
MAX_H = 24 * 7
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 9))
fig.suptitle(f"ENTSO-E Hourly Load — ACF and PACF  (max lag = {MAX_H} h)")
plot_acf( entsoe, lags=MAX_H, ax=ax1, alpha=0.05, fft=True, title="ACF")
plot_pacf(entsoe, lags=MAX_H, ax=ax2, alpha=0.05, method="ywm", title="PACF")
for ax in (ax1, ax2):
    ax.set_xlabel("Lag (hours)")
    for lag, lbl in [(24, "24 h"), (168, "168 h")]:
        ax.axvline(lag, color="firebrick", lw=1.2, ls="--", alpha=0.7)
plt.tight_layout(); savefig(fig, "entsoe_03_acf_pacf"); print(elapsed(t1))
acf_vals = acf(entsoe.dropna(), nlags=MAX_H, fft=True)
R["entsoe_acf"] = {int(i): round(float(v), 6) for i, v in enumerate(acf_vals)}
top10 = np.argsort(np.abs(acf_vals[1:]))[::-1][:10] + 1
R["entsoe_acf_top10"] = [{"lag_h": int(lag), "r": round(float(acf_vals[lag]), 4)} for lag in top10]
print("  Top-10 ACF lags:")
for lag in top10:
    d, h = divmod(int(lag), 24)
    print(f"    lag {lag:>4}h ({d}d {h:02d}h)  r={acf_vals[lag]:+.4f}")

# 2.4 ── Subseries
print("[2.4] ENTSOE subseries …", end=" ", flush=True)
t1 = time.time()
fig, axes = plt.subplots(2, 2, figsize=(18, 10))
fig.suptitle("ENTSO-E Electricity Load — Seasonal Subseries Analysis")
sns.boxplot(data=e_df, x="hour",       y="load", ax=axes[0,0], color="steelblue",  showfliers=False, linewidth=0.7)
sns.boxplot(data=e_df, x="day_of_week",y="load", order=list(range(1,8)),
                                                  ax=axes[0,1], color="seagreen",   showfliers=False, linewidth=0.7)
sns.boxplot(data=e_df, x="month",      y="load", ax=axes[1,0], color="darkorange", showfliers=False, linewidth=0.7)
axes[0,0].set_title("Intra-day (hour, UTC)"); axes[0,0].set_xlabel("Hour (UTC)"); axes[0,0].set_ylabel("Load (MW)")
axes[0,1].set_title("Day of week"); axes[0,1].set_xticklabels(DOW_SUN_FIRST); axes[0,1].set_ylabel("Load (MW)")
axes[1,0].set_title("Calendar month"); axes[1,0].set_xticklabels(MONTH_NAMES); axes[1,0].set_ylabel("Load (MW)")
pivot = e_df.groupby(["day_of_week","hour"])["load"].median().unstack("hour")
pivot.index = DOW_SUN_FIRST
sns.heatmap(pivot, ax=axes[1,1], cmap="YlOrRd", linewidths=0,
            cbar_kws={"label":"Median load (MW)","shrink":0.8})
axes[1,1].set_title("Median load: day × hour"); axes[1,1].set_xlabel("Hour (UTC)")
plt.tight_layout(); savefig(fig, "entsoe_04_subseries"); print(elapsed(t1))
R["entsoe_subseries"] = {
    "hourly_median_mw": {int(h): round(float(v), 1) for h, v in e_df.groupby("hour")["load"].median().items()},
    "dow_median_mw": {DOW_SUN_FIRST[i]: round(float(v), 1) for i, (_, v) in enumerate(e_df.groupby("day_of_week")["load"].median().sort_index().items())},
    "monthly_median_mw": {MONTH_NAMES[int(m)-1]: round(float(v), 1) for m, v in e_df.groupby("month")["load"].median().items()},
    "dow_hour_heatmap_mw": {row: {int(c): round(float(pivot.loc[row, c]), 1) for c in pivot.columns} for row in pivot.index},
}


# ══════════════════════════════════════════════════════════════════════════════
# 3  VIIRS A2 DAILY
# ══════════════════════════════════════════════════════════════════════════════
def analyse_viirs(viirs: pd.DataFrame, tag: str, label: str, color: str) -> dict:
    """Analyse a VIIRS product and return numerical results."""
    vr = {}  # results for this product
    a_mean   = viirs["ntl_mean"]
    total_px = viirs[["ntl_valid_count","ntl_fill_count","ntl_invalid_count"]].sum(axis=1)
    vpct     = viirs["ntl_valid_count"].div(total_px.replace(0, np.nan)) * 100

    # 3/4.1 ── Time series
    print(f"[{tag}.1] {label} time series …", end=" ", flush=True)
    t1 = time.time()
    fig, axes = plt.subplots(3, 1, figsize=(18, 11), sharex=True)
    fig.suptitle(f"{label} — Daily NTL Radiance, Netherlands")
    axes[0].plot(a_mean.index, a_mean.values, lw=0.35, color=color, alpha=0.65)
    axes[0].plot(a_mean.rolling(30,center=True).mean().index,
                 a_mean.rolling(30,center=True).mean().values, color=color, lw=1.8, label="30d mean")
    axes[0].set_ylabel("ntl_mean (nW/cm²/sr)"); axes[0].legend(fontsize=10)
    axes[0].set_title("Mean NTL radiance")
    axes[1].stackplot(viirs.index,
                      viirs["ntl_valid_count"], viirs["ntl_fill_count"], viirs["ntl_invalid_count"],
                      labels=["valid","fill","invalid"],
                      colors=["seagreen","firebrick","darkorange"], alpha=0.7)
    axes[1].set_ylabel("Pixel count"); axes[1].legend(loc="upper right", fontsize=9)
    axes[2].plot(vpct.index, vpct.values, lw=0.4, color=color, alpha=0.7)
    axes[2].plot(vpct.rolling(30,center=True).mean().index,
                 vpct.rolling(30,center=True).mean().values, color=color, lw=1.8)
    axes[2].set_ylabel("Valid pixel (%)"); axes[2].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    plt.tight_layout(); savefig(fig, f"{tag.lower()}_01_timeseries"); print(elapsed(t1))
    vr["overall_stats"] = {"mean": round(float(a_mean.mean()), 4), "std": round(float(a_mean.std()), 4),
                           "min": round(float(a_mean.min()), 4), "max": round(float(a_mean.max()), 4),
                           "valid_pixel_pct_mean": round(float(vpct.mean()), 2)}

    # 3/4.2 ── STL (last 5 years, period=365, non-robust for speed)
    print(f"[{tag}.2] {label} STL …", end=" ", flush=True)
    t1 = time.time()
    a_full = a_mean.asfreq("D").interpolate(method="linear", limit=7).dropna()
    a_full = a_full[a_full.index >= a_full.index.max() - pd.DateOffset(years=5)]
    stl_a  = STL(a_full, period=365, seasonal=13).fit()
    fig, axes = plt.subplots(4, 1, figsize=(18, 12), sharex=True)
    fig.suptitle(f"STL Decomposition — {label} Daily NTL Mean  (period = 365 days, last 5 yrs)")
    for ax, (lbl, data) in zip(axes, [
        ("Observed",a_full.values), ("Trend",stl_a.trend),
        ("Seasonal",stl_a.seasonal), ("Residual",stl_a.resid)
    ]):
        ax.plot(a_full.index, data, color=color, lw=0.35 if lbl in ("Observed","Residual") else 1.4)
        ax.set_ylabel(lbl, fontsize=10); ax.axhline(0, color="black", lw=0.4, ls="--")
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    plt.tight_layout(); savefig(fig, f"{tag.lower()}_02_stl"); print(elapsed(t1))
    obs_var = float(np.var(a_full.values))
    _vpct = {}
    for n, c in [("Trend",stl_a.trend),("Seasonal",stl_a.seasonal),("Residual",stl_a.resid)]:
        pct = round(100*float(np.var(c))/obs_var, 1)
        _vpct[n.lower()] = pct
        print(f"  {n}: {pct}%")
    vr["stl_variance_pct"] = _vpct

    # 3/4.3 ── ACF / PACF
    print(f"[{tag}.3] {label} ACF/PACF …", end=" ", flush=True)
    t1 = time.time()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 9))
    fig.suptitle(f"{label} Daily NTL Mean — ACF and PACF  (max lag = 100 days)")
    plot_acf( a_full, lags=100, ax=ax1, alpha=0.05, fft=True, title="ACF")
    plot_pacf(a_full, lags=100, ax=ax2, alpha=0.05, method="ywm", title="PACF")
    for ax in (ax1, ax2):
        ax.set_xlabel("Lag (days)")
        for lag, lbl in [(7,"7d"),(30,"30d"),(91,"~Q")]:
            ax.axvline(lag, color=color, lw=0.9, ls="--", alpha=0.6)
    plt.tight_layout(); savefig(fig, f"{tag.lower()}_03_acf_pacf"); print(elapsed(t1))
    _acf_v = acf(a_full.dropna(), nlags=100, fft=True)
    vr["acf"] = {int(i): round(float(v), 6) for i, v in enumerate(_acf_v)}

    # 3/4.4 ── Subseries
    print(f"[{tag}.4] {label} subseries …", end=" ", flush=True)
    t1 = time.time()
    df = viirs[["ntl_mean"]].copy()
    df["month"] = df.index.month; df["dayofwk"] = df.index.dayofweek
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle(f"{label} NTL Mean — Seasonal Subseries")
    sns.boxplot(data=df.dropna(), x="month", y="ntl_mean",   ax=axes[0], color=color, showfliers=False, linewidth=0.7)
    axes[0].set_title("By calendar month"); axes[0].set_xticklabels(MONTH_NAMES, rotation=45)
    sns.boxplot(data=df.dropna(), x="dayofwk", y="ntl_mean", ax=axes[1], color=color, showfliers=False, linewidth=0.7)
    axes[1].set_title("By day of week"); axes[1].set_xticklabels(DOW_MON_FIRST)
    hmap = df.dropna().groupby([df.dropna().index.year,"month"])["ntl_mean"].mean().unstack("month")
    hmap.columns = [MONTH_NAMES[int(m)-1] for m in hmap.columns]
    sns.heatmap(hmap, ax=axes[2], cmap="YlOrRd", cbar_kws={"label":"Mean NTL","shrink":0.8},
                linewidths=0.3, linecolor="white")
    axes[2].set_title("Year × month heatmap")
    plt.tight_layout(); savefig(fig, f"{tag.lower()}_04_subseries"); print(elapsed(t1))
    vr["subseries_monthly_mean"] = {MONTH_NAMES[int(m)-1]: round(float(v), 4)
                                    for m, v in df.dropna().groupby("month")["ntl_mean"].mean().items()}
    return vr


print("\n[3] VIIRS A2")
R["viirs_a2"] = analyse_viirs(viirs_a2, "viirs_a2", "VIIRS VNP46A2", "steelblue")

print("\n[4] VIIRS A1")
R["viirs_a1"] = analyse_viirs(viirs_a1, "viirs_a1", "VIIRS VNP46A1", "mediumpurple")

# A1 vs A2 comparison
print("[4.5] A1 vs A2 comparison …", end=" ", flush=True)
t1 = time.time()
shared = viirs_a2["ntl_mean"].index.intersection(viirs_a1["ntl_mean"].index)
roll_a2 = viirs_a2["ntl_mean"].reindex(shared).rolling(30, center=True).mean()
roll_a1 = viirs_a1["ntl_mean"].reindex(shared).rolling(30, center=True).mean()
fig, axes = plt.subplots(2, 1, figsize=(18, 9), sharex=True)
fig.suptitle("VIIRS A1 vs A2 — 30-day Rolling Mean")
axes[0].plot(roll_a2.index, roll_a2.values, color="steelblue",    lw=1.4, label="A2 (gap-filled, BRDF-corrected)")
axes[0].plot(roll_a1.index, roll_a1.values, color="mediumpurple", lw=1.4, label="A1 (at-sensor raw)")
axes[0].set_ylabel("30-day mean ntl_mean"); axes[0].legend(fontsize=10)
ratio = viirs_a1["ntl_mean"].reindex(shared) / viirs_a2["ntl_mean"].reindex(shared).replace(0, np.nan)
axes[1].plot(ratio.index, ratio.rolling(30, center=True).mean().values, color="darkorange", lw=1.4)
axes[1].axhline(1.0, color="black", lw=0.8, ls="--", alpha=0.5)
axes[1].set_ylabel("A1/A2 ratio"); axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
plt.tight_layout(); savefig(fig, "viirs_a1a2_comparison"); print(elapsed(t1))
_ratio_clean = ratio.dropna()
R["viirs_a1a2_ratio"] = {"mean": round(float(_ratio_clean.mean()), 4),
                         "median": round(float(_ratio_clean.median()), 4),
                         "std": round(float(_ratio_clean.std()), 4)}


# ══════════════════════════════════════════════════════════════════════════════
# 5  CBS MONTHLY
# ══════════════════════════════════════════════════════════════════════════════
print("\n[5] CBS monthly")

cbs_groups = {
    "Energy CPI (2015=100)":       ["cbs_cpi_energy","cbs_cpi_electricity","cbs_cpi_gas"],
    "Gas & Elec Prices (household)":["cbs_gep_gas_hh_total","cbs_gep_elec_hh_total",
                                     "cbs_gep_gas_hh_supply","cbs_gep_elec_hh_supply"],
    "Consumer Tariffs":             ["cbs_gas_total_tax","cbs_elec_total_tax",
                                     "cbs_gas_transport_rate","cbs_elec_transport_rate"],
    "Macroeconomic (y/y %)":        ["cbs_gdp_yy","cbs_consumption_hh_yy",
                                     "cbs_exports_total_yy","cbs_imports_total_yy"],
}

# 5.1 ── Time series overview
print("[5.1] CBS time series …", end=" ", flush=True)
t1 = time.time()
fig, axes = plt.subplots(len(cbs_groups), 1, figsize=(18, 5*len(cbs_groups)))
fig.suptitle("CBS Monthly Indicator Groups — Time Series Evolution", y=1.01)
for ax, (grp, cols) in zip(axes, cbs_groups.items()):
    for col in [c for c in cols if c in cbs.columns]:
        s = cbs[col].dropna()
        ax.plot(s.index, s.values, lw=1.6, label=col.replace("cbs_",""))
    ax.set_title(grp); ax.axhline(0, color="black", lw=0.5, ls="--", alpha=0.4)
    ax.legend(loc="upper left", fontsize=9, ncol=2)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
plt.tight_layout(); savefig(fig, "cbs_01_timeseries"); print(elapsed(t1))

# 5.2 ── STL for key series
stl_targets = [
    ("cbs_cpi_energy",       "Energy CPI",       "steelblue"),
    ("cbs_gep_gas_hh_total", "GEP gas HH total", "firebrick"),
    ("cbs_gdp_yy",           "GDP y/y change",   "seagreen"),
]
for col, label, color in stl_targets:
    if col not in cbs.columns:
        continue
    s = cbs[col].dropna().asfreq("MS")
    if len(s) < 36:
        continue
    print(f"[5.2] CBS STL — {label} …", end=" ", flush=True)
    t1 = time.time()
    stl_c = STL(s, period=12, seasonal=5).fit()
    fig, axes = plt.subplots(4, 1, figsize=(18, 10), sharex=True)
    fig.suptitle(f"STL Decomposition — {label}  (period = 12 months)")
    for ax, (lbl, data) in zip(axes, [
        ("Observed",s.values),("Trend",stl_c.trend),
        ("Seasonal",stl_c.seasonal),("Residual",stl_c.resid)
    ]):
        ax.plot(s.index, data, color=color, lw=0.5 if lbl in ("Observed","Residual") else 1.4)
        ax.set_ylabel(lbl, fontsize=10); ax.axhline(0, color="black", lw=0.4, ls="--")
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    slug = col.replace("cbs_","")
    plt.tight_layout(); savefig(fig, f"cbs_02_stl_{slug}"); print(elapsed(t1))
    _obs_v = float(np.var(s.values))
    R.setdefault("cbs_stl_variance_pct", {})[col] = {
        n.lower(): round(100*float(np.var(c))/_obs_v, 1)
        for n, c in [("Trend",stl_c.trend),("Seasonal",stl_c.seasonal),("Residual",stl_c.resid)]
    }

# 5.3 ── ACF/PACF
acf_cbs = [
    ("cbs_cpi_energy","Energy CPI","steelblue"),
    ("cbs_gep_gas_hh_total","GEP gas HH total","firebrick"),
    ("cbs_gdp_yy","GDP y/y","seagreen"),
    ("cbs_consumption_hh_yy","HH consumption y/y","darkorange"),
]
acf_cbs = [(c,l,clr) for c,l,clr in acf_cbs if c in cbs.columns]
print("[5.3] CBS ACF/PACF …", end=" ", flush=True)
t1 = time.time()
fig, axes = plt.subplots(len(acf_cbs), 2, figsize=(18, 4*len(acf_cbs)))
fig.suptitle("CBS Monthly Series — ACF and PACF  (max lag = 36 months)", y=1.01)
for row, (col, label, color) in enumerate(acf_cbs):
    s = cbs[col].dropna()
    if len(s) <= 41:
        continue
    plot_acf( s, lags=36, ax=axes[row,0], alpha=0.05, fft=True, title=f"ACF — {label}")
    plot_pacf(s, lags=36, ax=axes[row,1], alpha=0.05, method="ywm", title=f"PACF — {label}")
    for ax in axes[row]:
        ax.set_xlabel("Lag (months)")
        ax.axvline(12, color="firebrick", lw=1.2, ls="--", alpha=0.6)
plt.tight_layout(); savefig(fig, "cbs_03_acf_pacf"); print(elapsed(t1))
for col, label, color in acf_cbs:
    s = cbs[col].dropna()
    if len(s) > 41:
        _av = acf(s, nlags=36, fft=True)
        R.setdefault("cbs_acf", {})[col] = {int(i): round(float(v), 6) for i, v in enumerate(_av)}

# 5.4 ── Subseries
print("[5.4] CBS subseries …", end=" ", flush=True)
t1 = time.time()
cbs["_m"] = cbs.index.month
sub_cols = [("cbs_cpi_energy","Energy CPI","steelblue"),
            ("cbs_gep_gas_hh_total","GEP gas HH total","firebrick"),
            ("cbs_gep_elec_hh_total","GEP elec HH total","darkorchid"),
            ("cbs_gdp_yy","GDP y/y","seagreen")]
sub_cols = [(c,l,clr) for c,l,clr in sub_cols if c in cbs.columns]
fig, axes = plt.subplots(1, len(sub_cols), figsize=(5*len(sub_cols), 5))
fig.suptitle("CBS Monthly Series — Seasonal Variation by Calendar Month")
for ax, (col, label, color) in zip(axes, sub_cols):
    data = cbs[["_m",col]].dropna()
    mu, sig = data.groupby("_m")[col].mean(), data.groupby("_m")[col].std()
    ax.fill_between(mu.index, mu-sig, mu+sig, alpha=0.2, color=color)
    ax.plot(mu.index, mu.values, "-o", color=color, lw=2, ms=6)
    ax.set_title(label); ax.set_xlabel("Month")
    ax.set_xticks(range(1,13)); ax.set_xticklabels([m[:3] for m in MONTH_NAMES], rotation=45)
    ax.axhline(float(mu.mean()), color="black", lw=0.8, ls="--", alpha=0.4)
plt.tight_layout(); savefig(fig, "cbs_04_subseries"); print(elapsed(t1))


# ══════════════════════════════════════════════════════════════════════════════
# 6  MULTICOLLINEARITY
# ══════════════════════════════════════════════════════════════════════════════
print("\n[6] Multicollinearity")
_feat_want = [
    "entsoe_load_mw", "ntl_a2_mean", "ntl_a1_mean",
    "cbs_cpi_energy","cbs_cpi_electricity","cbs_cpi_gas",
    "cbs_gep_gas_hh_total","cbs_gep_elec_hh_total",
    "cbs_gas_total_tax","cbs_elec_total_tax",
    "cbs_gdp_yy","cbs_consumption_hh_yy","cbs_population_million",
    "knmi_temp_c","knmi_humidity_pct","knmi_wind_speed_ms","knmi_solar_rad_jcm2",
    "knmi_val_temp_c","knmi_val_humidity_pct","knmi_val_wind_speed_ms","knmi_val_solar_rad_jcm2",
]
feat = hourly[[c for c in _feat_want if c in hourly.columns]].copy()

# 6.2 ── Correlation heat-map
print("[6.2] Pearson correlation heat-map …", end=" ", flush=True)
t1 = time.time()
corr = feat.corr(method="pearson")
R["pearson_correlation"] = {r: {c: round(float(corr.loc[r, c]), 4) for c in corr.columns} for r in corr.index}
fig, ax = plt.subplots(figsize=(14, 12))
sns.heatmap(corr, ax=ax, annot=True, fmt=".2f", annot_kws={"size":8},
            cmap="RdBu_r", center=0, vmin=-1, vmax=1,
            square=True, linewidths=0.4, linecolor="white",
            cbar_kws={"shrink":0.8,"label":"Pearson r"})
ax.set_title("Pearson Correlation Matrix — All Features (hourly aligned)", fontsize=12)
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right", fontsize=9)
ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=9)
plt.tight_layout(); savefig(fig, "multi_01_correlation_heatmap"); print(elapsed(t1))

# 6.3 ── VIF
print("[6.3] VIF …", end=" ", flush=True)
t1 = time.time()
X = feat.drop(columns=["entsoe_load_mw"]).dropna()
# Subsample for speed — VIF on 50 K random rows is statistically equivalent.
if len(X) > 50_000:
    X = X.sample(50_000, random_state=42)
print(f"({len(X):,} rows) …", end=" ", flush=True)
X_vals = X.values.astype(float)
vif = pd.DataFrame({
    "Feature": X.columns,
    "VIF": [variance_inflation_factor(X_vals, i) for i in range(X_vals.shape[1])],
}).sort_values("VIF", ascending=False).reset_index(drop=True)
fig, ax = plt.subplots(figsize=(10, 0.55*len(vif)+1.5))
bar_colors = ["firebrick" if v>10 else "darkorange" if v>5 else "steelblue" for v in vif["VIF"]]
ax.barh(vif["Feature"], vif["VIF"], color=bar_colors)
ax.axvline(5,  color="darkorange", lw=1.5, ls="--", label="VIF=5 (moderate)")
ax.axvline(10, color="firebrick",  lw=1.5, ls="--", label="VIF=10 (high)")
ax.set_xlabel("VIF"); ax.set_title("Variance Inflation Factor")
ax.legend(fontsize=10); ax.invert_yaxis()
ax.set_xlim(0, min(float(vif["VIF"].max())*1.1, 200))
plt.tight_layout(); savefig(fig, "multi_02_vif"); print(elapsed(t1))
print(vif.to_string(index=False))
R["vif"] = {row["Feature"]: round(float(row["VIF"]), 2) for _, row in vif.iterrows()}

# 6.4 ── CCF  (FFT-based — O(n log n))
print("[6.4] Cross-correlation (FFT) …", end=" ", flush=True)
t1 = time.time()
MAX_LAG_CCF  = 24 * 7
ccf_features = [c for c in feat.columns if c != "entsoe_load_mw"]
n_cols = 3; n_rows = (len(ccf_features)+n_cols-1)//n_cols
fig, axes = plt.subplots(n_rows, n_cols, figsize=(18, 3.5*n_rows), sharex=True)
fig.suptitle("Cross-Correlation with ENTSOE Load  (±168 h, FFT-based)", y=1.01)
flat = axes.flatten()
for i, col in enumerate(ccf_features):
    ax = flat[i]
    overlap = feat[["entsoe_load_mw",col]].dropna()
    if len(overlap) < MAX_LAG_CCF*3:
        ax.text(0.5,0.5,f"Insufficient overlap ({len(overlap)} rows)",
                transform=ax.transAxes,ha="center",va="center",fontsize=9)
        ax.set_title(col.replace("cbs_",""),fontsize=9); continue
    lags, r = ccf_fft(overlap["entsoe_load_mw"].values, overlap[col].values, MAX_LAG_CCF)
    ax.plot(lags, r, lw=1.0, color="steelblue")
    ax.axhline(0, color="black", lw=0.5); ax.axvline(0, color="firebrick", lw=0.8, ls="--", alpha=0.6)
    conf = 1.96/np.sqrt(len(overlap))
    ax.fill_between(lags, -conf, conf, alpha=0.15, color="gray")
    ax.set_title(col.replace("cbs_",""),fontsize=9); ax.set_ylim(-1,1)
    peak_i = int(np.argmax(np.abs(r)))
    ax.annotate(f"{lags[peak_i]:+d}h  r={r[peak_i]:.2f}",
                xy=(lags[peak_i],r[peak_i]),xytext=(0,10),textcoords="offset points",
                ha="center",fontsize=7.5,color="firebrick")
    R.setdefault("ccf_peaks", {})[col] = {"peak_lag_h": int(lags[peak_i]),
                                           "peak_r": round(float(r[peak_i]), 4),
                                           "r_at_lag0": round(float(r[len(r)//2]), 4)}
for j in range(i+1, len(flat)): flat[j].set_visible(False)
for ax in flat[:len(ccf_features)]: ax.set_xlabel("Lag (hours)",fontsize=8)
plt.tight_layout(); savefig(fig, "multi_03_ccf"); print(elapsed(t1))

# 6.5 ── Pair plot
print("[6.5] Pair plot …", end=" ", flush=True)
t1 = time.time()
pp_cols = [c for c in ["entsoe_load_mw","ntl_a2_mean","cbs_cpi_energy",
                        "cbs_gep_gas_hh_total","cbs_gdp_yy","knmi_temp_c",
                        "cbs_population_million"]
           if c in feat.columns]
pp_df = feat[pp_cols].dropna()
if len(pp_df) > 5000:
    pp_df = pp_df.sample(5000, random_state=42)
g = sns.pairplot(pp_df, diag_kind="kde",
                 plot_kws={"alpha":0.25,"s":6,"rasterized":True},
                 diag_kws={"fill":True})
g.figure.suptitle(f"Pair Plot — Key Feature Subset  (n={len(pp_df):,} rows)", y=1.02, fontsize=12)
plt.tight_layout(); savefig(g.figure, "multi_04_pairplot"); print(elapsed(t1))


# ══════════════════════════════════════════════════════════════════════════════
# 7  KNMI METEOROLOGICAL
# ══════════════════════════════════════════════════════════════════════════════
if len(knmi) > 0:
    print("\n[7] KNMI Meteorological")

    _knmi_display = {
        "knmi_temp_c":               ("Temperature (°C)",       "steelblue"),
        "knmi_dewpoint_c":           ("Dew point (°C)",         "teal"),
        "knmi_wind_speed_ms":        ("Wind speed 10-min (m/s)","seagreen"),
        "knmi_wind_speed_hourly_ms": ("Wind speed hourly (m/s)","mediumseagreen"),
        "knmi_wind_gust_ms":         ("Wind gust max (m/s)",    "darkorange"),
        "knmi_solar_rad_jcm2":       ("Solar radiation (J/cm²)","goldenrod"),
        "knmi_sunshine_h":           ("Sunshine duration (h)",  "gold"),
        "knmi_humidity_pct":         ("Humidity (%)",           "mediumpurple"),
    }

    # 7.1 ── Time series overview (all variables)
    print("[7.1] KNMI time series …", end=" ", flush=True)
    t1 = time.time()
    plot_vars = [c for c in _knmi_display if c in knmi.columns]
    n_v = len(plot_vars)
    fig, axes = plt.subplots(n_v, 1, figsize=(18, 3*n_v), sharex=True)
    if n_v == 1: axes = [axes]
    fig.suptitle("KNMI Hourly Meteorological Observations — Netherlands (national mean)")
    for ax, col in zip(axes, plot_vars):
        label, color = _knmi_display[col]
        s = knmi[col].dropna()
        ax.plot(s.index, s.values, lw=0.15, color=color, alpha=0.4)
        roll = s.rolling(24*30, center=True, min_periods=24*7).mean()
        ax.plot(roll.index, roll.values, color=color, lw=1.8, label="30-day mean")
        ax.set_ylabel(label, fontsize=9)
        ax.legend(loc="upper right", fontsize=8)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.tight_layout(); savefig(fig, "knmi_01_timeseries"); print(elapsed(t1))

    # 7.2 ── STL decomposition — Temperature (period = 24 h, 1-year sample)
    print("[7.2] KNMI STL (temperature) …", end=" ", flush=True)
    t1 = time.time()
    temp = knmi["knmi_temp_c"].dropna()
    # Take last full year of data for STL
    stl_knmi_in = temp.iloc[-8760:].asfreq("h").ffill()
    if len(stl_knmi_in) >= 24*7*2:
        stl_k = STL(stl_knmi_in, period=24, seasonal=13).fit()
        fig, axes = plt.subplots(4, 1, figsize=(18, 12), sharex=True)
        fig.suptitle("STL Decomposition — KNMI Temperature  (period = 24 h, 1-year sample)")
        for ax, (lbl, data, clr) in zip(axes, [
            ("Observed", stl_knmi_in.values, "steelblue"),
            ("Trend", stl_k.trend, "firebrick"),
            ("Seasonal", stl_k.seasonal, "seagreen"),
            ("Residual", stl_k.resid, "darkorange"),
        ]):
            ax.plot(stl_knmi_in.index, data, color=clr,
                    lw=0.3 if lbl in ("Observed","Residual") else 1.4)
            ax.set_ylabel(lbl, fontsize=10)
            ax.axhline(0, color="black", lw=0.4, ls="--")
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.tight_layout(); savefig(fig, "knmi_02_stl_temp"); print(elapsed(t1))
        obs_var = float(np.var(stl_knmi_in.values))
        _kstl = {}
        for n, c in [("Trend",stl_k.trend),("Seasonal",stl_k.seasonal),("Residual",stl_k.resid)]:
            pct = round(100*float(np.var(c))/obs_var, 1)
            _kstl[n.lower()] = pct
            print(f"  {n}: {pct}%")
        R["knmi_stl_variance_pct"] = _kstl
    else:
        print("insufficient data for STL")

    # 7.3 ── ACF / PACF — Temperature (168 lags)
    print("[7.3] KNMI ACF/PACF …", end=" ", flush=True)
    t1 = time.time()
    MAX_H_K = 24 * 7
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 9))
    fig.suptitle(f"KNMI Temperature — ACF and PACF  (max lag = {MAX_H_K} h)")
    plot_acf( temp, lags=MAX_H_K, ax=ax1, alpha=0.05, fft=True, title="ACF")
    plot_pacf(temp, lags=MAX_H_K, ax=ax2, alpha=0.05, method="ywm", title="PACF")
    for ax in (ax1, ax2):
        ax.set_xlabel("Lag (hours)")
        for lag, lbl in [(24, "24 h"), (168, "168 h")]:
            ax.axvline(lag, color="steelblue", lw=1.2, ls="--", alpha=0.7)
    plt.tight_layout(); savefig(fig, "knmi_03_acf_pacf"); print(elapsed(t1))
    _kacf = acf(temp.dropna(), nlags=MAX_H_K, fft=True)
    R["knmi_temp_acf"] = {int(i): round(float(v), 6) for i, v in enumerate(_kacf)}

    # 7.4 ── Subseries — Temperature by hour, day-of-week, month
    print("[7.4] KNMI subseries …", end=" ", flush=True)
    t1 = time.time()
    kdf = knmi[["knmi_temp_c"]].dropna().copy()
    kdf["hour"] = kdf.index.hour
    kdf["dow"]  = kdf.index.dayofweek  # Mon=0
    kdf["month"]= kdf.index.month
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle("KNMI Temperature — Seasonal Subseries")
    sns.boxplot(data=kdf, x="hour",  y="knmi_temp_c", ax=axes[0],
                color="steelblue", showfliers=False, linewidth=0.7)
    axes[0].set_title("By hour (UTC)"); axes[0].set_ylabel("Temperature (°C)")
    sns.boxplot(data=kdf, x="dow",   y="knmi_temp_c", ax=axes[1],
                color="seagreen", showfliers=False, linewidth=0.7)
    axes[1].set_title("By day of week"); axes[1].set_xticklabels(DOW_MON_FIRST)
    axes[1].set_ylabel("Temperature (°C)")
    sns.boxplot(data=kdf, x="month", y="knmi_temp_c", ax=axes[2],
                color="darkorange", showfliers=False, linewidth=0.7)
    axes[2].set_title("By calendar month"); axes[2].set_xticklabels(MONTH_NAMES)
    axes[2].set_ylabel("Temperature (°C)")
    plt.tight_layout(); savefig(fig, "knmi_04_subseries"); print(elapsed(t1))
    R["knmi_temp_subseries"] = {
        "hourly_mean_c": {int(h): round(float(v), 2) for h, v in kdf.groupby("hour")["knmi_temp_c"].mean().items()},
        "monthly_mean_c": {MONTH_NAMES[int(m)-1]: round(float(v), 2) for m, v in kdf.groupby("month")["knmi_temp_c"].mean().items()},
    }

    # 7.5 ── Weather–Load correlation (the classic heating/cooling U-curve)
    print("[7.5] KNMI weather–load scatter …", end=" ", flush=True)
    t1 = time.time()
    wl = hourly[["entsoe_load_mw"]].copy()
    for c in ["knmi_temp_c","knmi_wind_speed_ms","knmi_solar_rad_jcm2","knmi_humidity_pct"]:
        if c in hourly.columns:
            wl[c] = hourly[c]
    wl = wl.dropna()
    if len(wl) > 0:
        wl_cols = [c for c in wl.columns if c != "entsoe_load_mw"]
        n_c = len(wl_cols)
        fig, axes = plt.subplots(1, n_c, figsize=(6*n_c, 5))
        if n_c == 1: axes = [axes]
        fig.suptitle("Electricity Load vs Weather Variables (hourly)")
        for ax, col in zip(axes, wl_cols):
            label, color = _knmi_display.get(col, (col, "steelblue"))
            # Subsample for scatter
            sample = wl.sample(min(8000, len(wl)), random_state=42)
            ax.scatter(sample[col], sample["entsoe_load_mw"],
                       s=2, alpha=0.15, color=color, rasterized=True)
            # Binned mean curve
            bins = pd.cut(wl[col], bins=30)
            binned = wl.groupby(bins, observed=True)["entsoe_load_mw"].mean()
            bin_centers = [(b.left+b.right)/2 for b in binned.index]
            ax.plot(bin_centers, binned.values, color="firebrick", lw=2.5, label="Binned mean")
            ax.set_xlabel(label); ax.set_ylabel("Load (MW)")
            ax.legend(fontsize=9)
            r = float(wl[col].corr(wl["entsoe_load_mw"]))
            ax.set_title(f"r = {r:.3f}")
            R.setdefault("knmi_weather_load_r", {})[col] = round(r, 4)
            R.setdefault("knmi_weather_load_binned", {})[col] = {
                round(float(bc), 3): round(float(bv), 1) for bc, bv in zip(bin_centers, binned.values)}
        plt.tight_layout(); savefig(fig, "knmi_05_weather_load"); print(elapsed(t1))
    else:
        print("no overlapping data")
else:
    print("\n[7] KNMI: skipped (no data)")


# ══════════════════════════════════════════════════════════════════════════════
# 8  KNMI VALIDATED METEOROLOGICAL
# ══════════════════════════════════════════════════════════════════════════════
if len(knmi_val) > 0:
    print("\n[8] KNMI Validated Meteorological")

    _knmi_val_display = {
        "knmi_val_temp_c":               ("Temperature (\u00b0C)",       "steelblue"),
        "knmi_val_dewpoint_c":           ("Dew point (\u00b0C)",         "teal"),
        "knmi_val_wind_speed_ms":        ("Wind speed 10-min (m/s)","seagreen"),
        "knmi_val_wind_speed_hourly_ms": ("Wind speed hourly (m/s)","mediumseagreen"),
        "knmi_val_wind_gust_ms":         ("Wind gust max (m/s)",    "darkorange"),
        "knmi_val_solar_rad_jcm2":       ("Solar radiation (J/cm\u00b2)","goldenrod"),
        "knmi_val_sunshine_h":           ("Sunshine duration (h)",  "gold"),
        "knmi_val_humidity_pct":         ("Humidity (%)",           "mediumpurple"),
    }

    # 8.1 ── Time series overview (all variables)
    print("[8.1] KNMI Val time series \u2026", end=" ", flush=True)
    t1 = time.time()
    plot_vars_v = [c for c in _knmi_val_display if c in knmi_val.columns]
    n_v = len(plot_vars_v)
    fig, axes = plt.subplots(n_v, 1, figsize=(18, 3*n_v), sharex=True)
    if n_v == 1: axes = [axes]
    fig.suptitle("KNMI Validated Hourly Meteorological Observations \u2014 Netherlands (national mean)")
    for ax, col in zip(axes, plot_vars_v):
        label, color = _knmi_val_display[col]
        s = knmi_val[col].dropna()
        ax.plot(s.index, s.values, lw=0.15, color=color, alpha=0.4)
        roll = s.rolling(24*30, center=True, min_periods=24*7).mean()
        ax.plot(roll.index, roll.values, color=color, lw=1.8, label="30-day mean")
        ax.set_ylabel(label, fontsize=9)
        ax.legend(loc="upper right", fontsize=8)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.tight_layout(); savefig(fig, "knmi_val_01_timeseries"); print(elapsed(t1))

    # 8.2 ── STL decomposition — Temperature (period = 24 h, 1-year sample)
    print("[8.2] KNMI Val STL (temperature) \u2026", end=" ", flush=True)
    t1 = time.time()
    temp_v = knmi_val["knmi_val_temp_c"].dropna()
    stl_kv_in = temp_v.iloc[-8760:].asfreq("h").ffill()
    if len(stl_kv_in) >= 24*7*2:
        stl_kv = STL(stl_kv_in, period=24, seasonal=13).fit()
        fig, axes = plt.subplots(4, 1, figsize=(18, 12), sharex=True)
        fig.suptitle("STL Decomposition \u2014 KNMI Validated Temperature  (period = 24 h, 1-year sample)")
        for ax, (lbl, data, clr) in zip(axes, [
            ("Observed", stl_kv_in.values, "steelblue"),
            ("Trend", stl_kv.trend, "firebrick"),
            ("Seasonal", stl_kv.seasonal, "seagreen"),
            ("Residual", stl_kv.resid, "darkorange"),
        ]):
            ax.plot(stl_kv_in.index, data, color=clr,
                    lw=0.3 if lbl in ("Observed","Residual") else 1.4)
            ax.set_ylabel(lbl, fontsize=10)
            ax.axhline(0, color="black", lw=0.4, ls="--")
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        plt.tight_layout(); savefig(fig, "knmi_val_02_stl_temp"); print(elapsed(t1))
        obs_var_v = float(np.var(stl_kv_in.values))
        _kvstl = {}
        for n, c in [("Trend",stl_kv.trend),("Seasonal",stl_kv.seasonal),("Residual",stl_kv.resid)]:
            pct = round(100*float(np.var(c))/obs_var_v, 1)
            _kvstl[n.lower()] = pct
            print(f"  {n}: {pct}%")
        R["knmi_val_stl_variance_pct"] = _kvstl
    else:
        print("insufficient data for STL")

    # 8.3 ── ACF / PACF — Temperature (168 lags)
    print("[8.3] KNMI Val ACF/PACF \u2026", end=" ", flush=True)
    t1 = time.time()
    MAX_H_KV = 24 * 7
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 9))
    fig.suptitle(f"KNMI Validated Temperature \u2014 ACF and PACF  (max lag = {MAX_H_KV} h)")
    plot_acf( temp_v, lags=MAX_H_KV, ax=ax1, alpha=0.05, fft=True, title="ACF")
    plot_pacf(temp_v, lags=MAX_H_KV, ax=ax2, alpha=0.05, method="ywm", title="PACF")
    for ax in (ax1, ax2):
        ax.set_xlabel("Lag (hours)")
        for lag, lbl in [(24, "24 h"), (168, "168 h")]:
            ax.axvline(lag, color="steelblue", lw=1.2, ls="--", alpha=0.7)
    plt.tight_layout(); savefig(fig, "knmi_val_03_acf_pacf"); print(elapsed(t1))
    _kvacf = acf(temp_v.dropna(), nlags=MAX_H_KV, fft=True)
    R["knmi_val_temp_acf"] = {int(i): round(float(v), 6) for i, v in enumerate(_kvacf)}

    # 8.4 ── Subseries — Temperature by hour, day-of-week, month
    print("[8.4] KNMI Val subseries \u2026", end=" ", flush=True)
    t1 = time.time()
    kvdf = knmi_val[["knmi_val_temp_c"]].dropna().copy()
    kvdf["hour"] = kvdf.index.hour
    kvdf["dow"]  = kvdf.index.dayofweek
    kvdf["month"]= kvdf.index.month
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle("KNMI Validated Temperature \u2014 Seasonal Subseries")
    sns.boxplot(data=kvdf, x="hour",  y="knmi_val_temp_c", ax=axes[0],
                color="steelblue", showfliers=False, linewidth=0.7)
    axes[0].set_title("By hour (UTC)"); axes[0].set_ylabel("Temperature (\u00b0C)")
    sns.boxplot(data=kvdf, x="dow",   y="knmi_val_temp_c", ax=axes[1],
                color="seagreen", showfliers=False, linewidth=0.7)
    axes[1].set_title("By day of week"); axes[1].set_xticklabels(DOW_MON_FIRST)
    axes[1].set_ylabel("Temperature (\u00b0C)")
    sns.boxplot(data=kvdf, x="month", y="knmi_val_temp_c", ax=axes[2],
                color="darkorange", showfliers=False, linewidth=0.7)
    axes[2].set_title("By calendar month"); axes[2].set_xticklabels(MONTH_NAMES)
    axes[2].set_ylabel("Temperature (\u00b0C)")
    plt.tight_layout(); savefig(fig, "knmi_val_04_subseries"); print(elapsed(t1))
    R["knmi_val_temp_subseries"] = {
        "hourly_mean_c": {int(h): round(float(v), 2) for h, v in kvdf.groupby("hour")["knmi_val_temp_c"].mean().items()},
        "monthly_mean_c": {MONTH_NAMES[int(m)-1]: round(float(v), 2) for m, v in kvdf.groupby("month")["knmi_val_temp_c"].mean().items()},
    }

    # 8.5 ── Weather–Load correlation (validated)
    print("[8.5] KNMI Val weather\u2013load scatter \u2026", end=" ", flush=True)
    t1 = time.time()
    wl_v = hourly[["entsoe_load_mw"]].copy()
    for c in ["knmi_val_temp_c","knmi_val_wind_speed_ms","knmi_val_solar_rad_jcm2","knmi_val_humidity_pct"]:
        if c in hourly.columns:
            wl_v[c] = hourly[c]
    wl_v = wl_v.dropna()
    if len(wl_v) > 0:
        wl_v_cols = [c for c in wl_v.columns if c != "entsoe_load_mw"]
        n_c = len(wl_v_cols)
        fig, axes = plt.subplots(1, n_c, figsize=(6*n_c, 5))
        if n_c == 1: axes = [axes]
        fig.suptitle("Electricity Load vs Validated Weather Variables (hourly)")
        for ax, col in zip(axes, wl_v_cols):
            label, color = _knmi_val_display.get(col, (col, "steelblue"))
            sample = wl_v.sample(min(8000, len(wl_v)), random_state=42)
            ax.scatter(sample[col], sample["entsoe_load_mw"],
                       s=2, alpha=0.15, color=color, rasterized=True)
            bins = pd.cut(wl_v[col], bins=30)
            binned = wl_v.groupby(bins, observed=True)["entsoe_load_mw"].mean()
            bin_centers = [(b.left+b.right)/2 for b in binned.index]
            ax.plot(bin_centers, binned.values, color="firebrick", lw=2.5, label="Binned mean")
            ax.set_xlabel(label); ax.set_ylabel("Load (MW)")
            ax.legend(fontsize=9)
            r = float(wl_v[col].corr(wl_v["entsoe_load_mw"]))
            ax.set_title(f"r = {r:.3f}")
            R.setdefault("knmi_val_weather_load_r", {})[col] = round(r, 4)
        plt.tight_layout(); savefig(fig, "knmi_val_05_weather_load"); print(elapsed(t1))
    else:
        print("no overlapping data")
else:
    print("\n[8] KNMI Validated: skipped (no data)")


# ══════════════════════════════════════════════════════════════════════════════
# 9  VALIDATED vs NON-VALIDATED COMPARISON
# ══════════════════════════════════════════════════════════════════════════════
if len(knmi) > 0 and len(knmi_val) > 0:
    print("\n[9] Validated vs Non-validated Comparison")

    # Find overlapping timestamps
    shared_idx = knmi.index.intersection(knmi_val.index)
    if len(shared_idx) > 100:
        # 9.1 ── Side-by-side temperature time series
        print("[9.1] Val vs Non-val temperature \u2026", end=" ", flush=True)
        t1 = time.time()
        roll_nv = knmi["knmi_temp_c"].reindex(shared_idx).rolling(24*30, center=True).mean()
        roll_v  = knmi_val["knmi_val_temp_c"].reindex(shared_idx).rolling(24*30, center=True).mean()
        fig, axes = plt.subplots(2, 1, figsize=(18, 9), sharex=True)
        fig.suptitle("KNMI Temperature: Validated vs Non-validated (30-day rolling mean)")
        axes[0].plot(roll_nv.index, roll_nv.values, color="steelblue", lw=1.4, label="Non-validated")
        axes[0].plot(roll_v.index,  roll_v.values,  color="firebrick", lw=1.4, label="Validated")
        axes[0].set_ylabel("Temperature (\u00b0C)"); axes[0].legend(fontsize=10)
        axes[0].set_title("30-day rolling mean comparison")

        # 9.2 ── Difference plot
        diff = knmi_val["knmi_val_temp_c"].reindex(shared_idx) - knmi["knmi_temp_c"].reindex(shared_idx)
        diff_roll = diff.rolling(24*30, center=True).mean()
        axes[1].plot(diff_roll.index, diff_roll.values, color="darkorange", lw=1.4)
        axes[1].axhline(0, color="black", lw=0.8, ls="--", alpha=0.5)
        axes[1].set_ylabel("\u0394T (val \u2212 non-val) (\u00b0C)")
        axes[1].set_title("Difference: Validated \u2212 Non-validated (30-day rolling mean)")
        axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        plt.tight_layout(); savefig(fig, "knmi_val_vs_nonval_01_temp"); print(elapsed(t1))

        diff_clean = diff.dropna()
        R["knmi_val_vs_nonval"] = {
            "overlapping_hours": len(shared_idx),
            "temp_diff_mean_c": round(float(diff_clean.mean()), 4),
            "temp_diff_std_c": round(float(diff_clean.std()), 4),
            "temp_diff_abs_max_c": round(float(diff_clean.abs().max()), 4),
        }

        # 9.3 ── Correlation matrix between validated and non-validated
        print("[9.3] Val vs Non-val correlation \u2026", end=" ", flush=True)
        t1 = time.time()
        _cmp_pairs = [
            ("knmi_temp_c",      "knmi_val_temp_c"),
            ("knmi_dewpoint_c",  "knmi_val_dewpoint_c"),
            ("knmi_wind_speed_ms","knmi_val_wind_speed_ms"),
            ("knmi_solar_rad_jcm2","knmi_val_solar_rad_jcm2"),
            ("knmi_humidity_pct", "knmi_val_humidity_pct"),
        ]
        _cmp_labels = []; _cmp_r = []
        for nv_col, v_col in _cmp_pairs:
            if nv_col in knmi.columns and v_col in knmi_val.columns:
                nv_s = knmi[nv_col].reindex(shared_idx)
                v_s  = knmi_val[v_col].reindex(shared_idx)
                mask = nv_s.notna() & v_s.notna()
                if mask.sum() > 100:
                    r = float(nv_s[mask].corr(v_s[mask]))
                    suffix = nv_col.replace("knmi_", "")
                    _cmp_labels.append(suffix)
                    _cmp_r.append(round(r, 6))

        if _cmp_labels:
            fig, ax = plt.subplots(figsize=(8, 4))
            bars = ax.barh(_cmp_labels, _cmp_r, color="steelblue")
            ax.set_xlim(min(0.9, min(_cmp_r) - 0.02), 1.005)
            ax.set_xlabel("Pearson r")
            ax.set_title("Validated vs Non-validated: Per-variable Correlation")
            for bar, r in zip(bars, _cmp_r):
                ax.text(bar.get_width() + 0.001, bar.get_y() + bar.get_height()/2,
                        f"{r:.5f}", va="center", fontsize=9)
            ax.invert_yaxis()
            plt.tight_layout(); savefig(fig, "knmi_val_vs_nonval_02_correlation"); print(elapsed(t1))
            R["knmi_val_vs_nonval_pearson"] = dict(zip(_cmp_labels, _cmp_r))
        else:
            print("no matching columns")
    else:
        print("  Insufficient overlap for comparison.")
else:
    print("\n[9] Val vs Non-val comparison: skipped (missing data)")


# ── Write machine-readable results ────────────────────────────────────────────
_results_path = REPO / "analysis" / "eda_results.json"
with open(_results_path, "w") as f:
    json.dump(R, f, indent=2, default=str)
print(f"\n  Results JSON → {_results_path.relative_to(REPO)}")

# ── Summary ───────────────────────────────────────────────────────────────────
total = elapsed(t0)
print(f"\n{'='*60}")
print(f"  All figures saved to: analysis/figures/")
print(f"  Results JSON saved to: analysis/eda_results.json")
print(f"  Total runtime: {total}")
print(f"{'='*60}")
