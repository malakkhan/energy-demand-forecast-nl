#!/usr/bin/env python3
"""Regenerate analysis/figures/entsoe_02_stl_full.png.

Full-timeline (2012--2025) STL decomposition of the ENTSO-E hourly load
with the two structural-break windows highlighted:
  - COVID trough:        2020-03-01 .. 2020-12-31 23:00
  - European energy crisis: 2022-01-01 .. 2023-06-30 23:00
Both use the same pale-red shading style as the original figure.
"""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import STL

REPO = "/gpfs/scratch1/shared/prjs2061_copy/energy-demand-forecast-nl"

df = pd.read_parquet(
    f"{REPO}/data/processed/nl_hourly_dataset.parquet",
    columns=["timestamp", "entsoe_load_mw"],
)
df["timestamp"] = pd.to_datetime(df["timestamp"])
s = df.set_index("timestamp")["entsoe_load_mw"].asfreq("h").ffill()

print(f"series: {len(s)} hours ({s.index.min()} .. {s.index.max()})")
res = STL(s, period=24 * 7).fit()
print("STL fitted.")

WINDOWS = [
    (pd.Timestamp("2020-03-01"), pd.Timestamp("2020-12-31 23:00")),
    (pd.Timestamp("2022-01-01"), pd.Timestamp("2023-06-30 23:00")),
]

fig, axes = plt.subplots(4, 1, figsize=(20, 12.8), sharex=True)
panels = [
    ("Observed", s.values, "steelblue", 0.3),
    ("Trend", res.trend, "firebrick", 1.4),
    ("Seasonal", res.seasonal, "seagreen", 1.4),
    ("Residual", res.resid, "darkorange", 0.3),
]
for ax, (label, data, color, lw) in zip(axes, panels):
    ax.plot(s.index, data, color=color, lw=lw)
    ax.set_ylabel(label)
    ax.axhline(0, color="black", lw=0.4, ls=":")
    for w0, w1 in WINDOWS:
        ax.axvspan(w0, w1, color="red", alpha=0.08, zorder=0)

axes[0].set_title(
    "STL Decomposition — ENTSO-E Hourly Load  "
    "(period = 168 h = 1 week, full 2012–2025)"
)
plt.tight_layout()
out = f"{REPO}/analysis/figures/entsoe_02_stl_full.png"
fig.savefig(out, dpi=150)
print(f"saved {out}")
