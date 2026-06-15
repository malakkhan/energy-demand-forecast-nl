#!/usr/bin/env python3
"""Phase 3: Merge all Phase 2 outputs into a unified hourly dataset.

Final stage of the three-phase NL energy demand data pipeline. Reads the
aggregated/combined Parquet files from ``data/processing_2/`` and joins
them onto a canonical hourly UTC timestamp spine spanning
2012-01-01 to 2025-12-31.

The final dataset is written to ``data/processed/nl_hourly_dataset.parquet``,
partitioned by year, with a comprehensive ``data_quality.json``.

Join strategy (pure pandas — the total dataset is ~122K rows):
    - **ENTSO-E** (hourly): Left-join on ``timestamp``.
    - **VIIRS A2 daily** (daily): Left-join on ``date``; columns prefixed ``ntl_a2_``.
    - **VIIRS A2 all daily** (daily): Left-join on ``date``; columns prefixed ``ntl_a2_all_``.
    - **VIIRS A1 daily** (daily): Left-join on ``date``; columns prefixed ``ntl_a1_``.
    - **CBS Combined** (monthly): Left-join on ``(year, month)``.
    - **KNMI** (hourly): Left-join on ``timestamp``; 8 weather variables + station count.

Usage::

    python phase3_merge.py [--data-root /path/to/data]
                            [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                            [--force]

Engineered features (post-join):
    - **Lag features**: load (−12h, −24h, −168h, −8760h), validated solar
      radiation (−10h), validated humidity (−12h), validated temperature (−107h).
    - **Energy crisis flag**: binary step variable, 1 for 2022-01-01 → 2023-06-30.
    - **Cyclical temporal features**: sin/cos encoding for hour, day-of-week, month.
    - **Holiday flags**: Dutch public and school holiday indicators.
    - **PCA components**: 15 principal components from all CBS + KNMI validated
      features (95% cumulative variance); columns with >50% NaN excluded before fit.
"""

import argparse
import json
import logging
import os
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("phase3_merge")


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def _output_done(parquet_dir: str) -> bool:
    """Return True if a previous write completed (check for _SUCCESS marker
    or the existence of at least one year-partition directory)."""
    if os.path.exists(os.path.join(parquet_dir, "_SUCCESS")):
        return True
    if os.path.isdir(parquet_dir):
        return any(d.startswith("year=") for d in os.listdir(parquet_dir))
    return False


def _clear_path(path: str) -> None:
    """Remove a Parquet directory tree (used under --force)."""
    if os.path.isdir(path):
        shutil.rmtree(path)


# ---------------------------------------------------------------------------
# Data quality helpers
# ---------------------------------------------------------------------------


def _compute_quality_metrics(
    parquet_path: str,
    source_name: str,
    phase: int,
    extra_metrics: Optional[dict] = None,
) -> dict:
    """Compute data quality metrics for the final Parquet output."""
    df = pd.read_parquet(parquet_path)
    total_rows = len(df)
    columns = list(df.columns)

    col_metrics = {}
    for c in columns:
        nc = int(df[c].isna().sum())
        col_metrics[c] = {
            "dtype": str(df[c].dtype),
            "null_count": nc,
            "null_pct": round(100.0 * nc / total_rows, 2) if total_rows > 0 else 0.0,
        }

    size_bytes = 0
    part_files = []
    for root, _, files in os.walk(parquet_path):
        for fname in files:
            size_bytes += os.path.getsize(os.path.join(root, fname))
            if fname.startswith("part-") or fname.endswith(".parquet"):
                part_files.append(fname)

    date_range = {}
    for candidate in ["timestamp", "date", "timestamp_utc"]:
        if candidate in columns and not df[candidate].isna().all():
            date_range["column"] = candidate
            date_range["min"] = str(df[candidate].min())
            date_range["max"] = str(df[candidate].max())
            break

    metrics = {
        "source": source_name,
        "phase": phase,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "row_count": total_rows,
        "column_count": len(columns),
        "columns": col_metrics,
        "size_bytes": size_bytes,
        "size_mb": round(size_bytes / (1024 * 1024), 1),
        "partition_file_count": len(part_files),
        "date_range": date_range,
    }
    if extra_metrics:
        metrics.update(extra_metrics)
    return metrics


def _write_quality_json(metrics: dict, out_dir: str) -> None:
    """Write data_quality.json to the given directory."""
    path = os.path.join(out_dir, "data_quality.json")
    with open(path, "w") as fh:
        json.dump(metrics, fh, indent=2, default=str)
    logger.info("Quality report written → %s", path)


# ---------------------------------------------------------------------------
# Spine construction
# ---------------------------------------------------------------------------


def build_hourly_spine(start_date: str, end_date: str) -> pd.DataFrame:
    """Generate a contiguous hourly UTC timestamp spine.

    Creates one row per hour for the closed interval
    [start_date 00:00 UTC, end_date 23:00 UTC], with 9 temporal features
    and 6 cyclical-encoded features derived from each timestamp.

    Cyclical features use sin/cos encoding so that periodic boundaries
    (e.g. hour 23 → 0, December → January) are continuous:

    - ``hour_sin``, ``hour_cos`` — period 24 h
    - ``dow_sin``, ``dow_cos``   — period 7 days (Monday=0)
    - ``month_sin``, ``month_cos`` — period 12 months
    """
    logger.info("Building hourly spine: %s → %s (UTC)", start_date, end_date)

    timestamps = pd.date_range(
        start=f"{start_date} 00:00:00",
        end=f"{end_date} 23:00:00",
        freq="h",
        tz=None,  # timezone-naive, represents UTC
    )

    spine = pd.DataFrame({"timestamp": timestamps})
    spine["date"] = spine["timestamp"].dt.date
    spine["year"] = spine["timestamp"].dt.year
    spine["month"] = spine["timestamp"].dt.month
    spine["day"] = spine["timestamp"].dt.day
    spine["hour"] = spine["timestamp"].dt.hour
    # dayofweek: Monday=0 in pandas; convert to 1=Sunday..7=Saturday
    spine["day_of_week"] = (spine["timestamp"].dt.dayofweek + 2) % 7
    spine["day_of_week"] = spine["day_of_week"].replace(0, 7)
    spine["is_weekend"] = spine["timestamp"].dt.dayofweek.isin([5, 6]).astype(int)
    spine["day_of_year"] = spine["timestamp"].dt.dayofyear
    spine["week_of_year"] = spine["timestamp"].dt.isocalendar().week.astype(int)
    spine["quarter"] = spine["timestamp"].dt.quarter

    # ── Cyclical (sin/cos) temporal features ──
    two_pi = 2.0 * np.pi

    # Hour of day (period = 24)
    spine["hour_sin"] = np.sin(two_pi * spine["hour"] / 24.0)
    spine["hour_cos"] = np.cos(two_pi * spine["hour"] / 24.0)

    # Day of week (period = 7, Monday = 0)
    dow_zero = spine["timestamp"].dt.dayofweek  # Mon=0 .. Sun=6
    spine["dow_sin"] = np.sin(two_pi * dow_zero / 7.0)
    spine["dow_cos"] = np.cos(two_pi * dow_zero / 7.0)

    # Month of year (period = 12, Jan=0-indexed for trig)
    spine["month_sin"] = np.sin(two_pi * (spine["month"] - 1) / 12.0)
    spine["month_cos"] = np.cos(two_pi * (spine["month"] - 1) / 12.0)

    logger.info("  Spine: %d hourly rows (incl. 6 cyclical features).", len(spine))
    return spine


# ---------------------------------------------------------------------------
# Join functions
# ---------------------------------------------------------------------------


def join_entsoe(spine: pd.DataFrame, p2_entsoe_path: str) -> pd.DataFrame:
    """Left-join ENTSO-E hourly load onto the timestamp spine."""
    if not os.path.exists(p2_entsoe_path):
        logger.warning("ENTSO-E P2 not found — column will be null.")
        spine["entsoe_load_mw"] = np.nan
        return spine

    entsoe = pd.read_parquet(p2_entsoe_path, columns=["timestamp_utc", "entsoe_load_mw"])
    entsoe = entsoe.rename(columns={"timestamp_utc": "timestamp"})
    # Ensure same dtype for join
    entsoe["timestamp"] = pd.to_datetime(entsoe["timestamp"])
    logger.info("  Joining ENTSO-E (%d rows).", len(entsoe))
    return spine.merge(entsoe, on="timestamp", how="left")


def join_viirs(spine: pd.DataFrame, p2_viirs_path: str,
               product: str = "a2") -> pd.DataFrame:
    """Left-join VIIRS daily aggregates onto the spine by date.

    Output columns are prefixed with the product identifier so that A1 and
    A2 can coexist in the final dataset (e.g. ``ntl_a2_mean``, ``ntl_a1_mean``).
    """
    base_cols = [
        "ntl_mean", "ntl_sum", "ntl_valid_count",
        "ntl_fill_count", "ntl_invalid_count",
    ]
    # ntl_mean → ntl_a2_mean (strip "ntl_" prefix, add product-specific prefix)
    prefixed_cols = [f"ntl_{product}_{c[4:]}" for c in base_cols]

    if not os.path.exists(p2_viirs_path):
        logger.warning("VIIRS %s P2 not found — columns will be null.", product.upper())
        for c in prefixed_cols:
            spine[c] = np.nan
        return spine

    viirs = pd.read_parquet(p2_viirs_path, columns=["date"] + base_cols)
    viirs["date"] = pd.to_datetime(viirs["date"]).dt.date
    viirs = viirs.rename(columns=dict(zip(base_cols, prefixed_cols)))
    logger.info("  Joining VIIRS %s daily (%d rows).", product.upper(), len(viirs))
    return spine.merge(viirs, on="date", how="left")


def join_cbs(spine: pd.DataFrame, p2_cbs_path: str) -> pd.DataFrame:
    """Left-join CBS combined (monthly) onto the spine.

    Dynamically discovers all ``cbs_*`` columns from the CBS combined
    Parquet rather than hardcoding a fixed list.
    """
    if not os.path.exists(p2_cbs_path):
        logger.warning("CBS P2 not found — adding null fallback columns.")
        for c in [
            "cbs_gas_total_tax", "cbs_gas_energy_tax",
            "cbs_elec_total_tax", "cbs_elec_energy_tax",
            "cbs_gdp_yy", "cbs_population_million",
        ]:
            spine[c] = np.nan
        return spine

    cbs = pd.read_parquet(p2_cbs_path)
    logger.info("  Joining CBS combined (%d rows × %d columns).", len(cbs), len(cbs.columns))
    return spine.merge(cbs, on=["year", "month"], how="left")


def join_knmi(spine: pd.DataFrame, p2_knmi_path: str,
              col_prefix: str = "knmi") -> pd.DataFrame:
    """Left-join KNMI hourly meteorological data onto the timestamp spine.

    KNMI data is at native hourly resolution, so the join is on
    ``timestamp`` (same as ENTSO-E).  This adds 8 weather columns
    plus a station-count column, all prefixed with ``col_prefix``.

    Parameters
    ----------
    col_prefix : str
        Column name prefix: ``"knmi"`` for non-validated data or
        ``"knmi_val"`` for validated data.
    """
    knmi_suffixes = [
        "temp_c", "dewpoint_c",
        "wind_speed_ms", "wind_speed_hourly_ms",
        "wind_gust_ms", "solar_rad_jcm2",
        "sunshine_h", "humidity_pct",
        "station_count",
    ]
    knmi_cols = [f"{col_prefix}_{s}" for s in knmi_suffixes]

    if not os.path.exists(p2_knmi_path):
        logger.warning("%s P2 not found — columns will be null.", col_prefix.upper())
        for c in knmi_cols:
            spine[c] = np.nan
        return spine

    knmi = pd.read_parquet(p2_knmi_path)
    knmi = knmi.rename(columns={"timestamp_utc": "timestamp"})
    knmi["timestamp"] = pd.to_datetime(knmi["timestamp"])
    # Select only known columns + timestamp for the merge
    merge_cols = ["timestamp"] + [c for c in knmi_cols if c in knmi.columns]
    knmi = knmi[merge_cols]
    logger.info("  Joining %s hourly (%d rows).", col_prefix.upper(), len(knmi))
    return spine.merge(knmi, on="timestamp", how="left")


# ---------------------------------------------------------------------------
# Holiday flags
# ---------------------------------------------------------------------------

# School holiday week-of-year windows derived from 2020–2025 OpenHolidays API
# data.  Dutch school holidays are set by the Ministry of Education and follow
# a very stable annual rotation across three regions (North, Central, South).
# The union of all regional windows gives the national "any region on holiday"
# flag, which is what matters for aggregate electricity demand.
_SCHOOL_HOLIDAY_WEEKS = {
    "christmas": list(range(51, 54)) + [1],   # week 51 → week 1
    "spring":    list(range(6, 10)),           # weeks 6–9
    "may":       [17, 18],                     # weeks 17–18
    "summer":    list(range(27, 36)),           # weeks 27–35
    "autumn":    list(range(41, 45)),           # weeks 41–44
}
_ALL_SCHOOL_HOLIDAY_WEEKS = set()
for _wks in _SCHOOL_HOLIDAY_WEEKS.values():
    _ALL_SCHOOL_HOLIDAY_WEEKS.update(_wks)


def _load_public_holiday_dates(calendar_dir: str) -> set:
    """Load Dutch public holiday dates from the holidays-library CSV."""
    path = os.path.join(calendar_dir, "nl_public_holidays.csv")
    if not os.path.exists(path):
        logger.warning("Public holidays CSV not found at %s.", path)
        return set()
    df = pd.read_csv(path)
    dates = set(pd.to_datetime(df["date"]).dt.date)
    logger.info("  Loaded %d public holiday dates from %s.", len(dates), path)
    return dates


def _load_school_holiday_dates(
    calendar_dir: str,
    start_year: int,
    end_year: int,
) -> set:
    """Load Dutch school holiday dates, extrapolating for years without API data.

    For years covered by the OpenHolidays API (2020–2025), exact date ranges
    from ``nl_holidays.csv`` are expanded into individual dates.

    For years *not* covered (2012–2019), school holidays are approximated
    using stable ISO-week-of-year windows derived from the 2020–2025 data:

    - Christmas:  weeks 51 – 1
    - Spring:     weeks 6 – 9
    - May:        weeks 17 – 18
    - Summer:     weeks 27 – 35
    - Autumn:     weeks 41 – 44

    This is a reasonable approximation because Dutch school holiday dates
    are legislated centrally and the week-of-year windows have been nearly
    identical for decades.
    """
    dates: set = set()

    # ── Exact dates from OpenHolidays API ──
    api_path = os.path.join(calendar_dir, "nl_holidays.csv")
    api_years_covered: set = set()
    if os.path.exists(api_path):
        df = pd.read_csv(api_path)
        school = df[df["type"] == "School"].copy()
        if not school.empty:
            school["start_date"] = pd.to_datetime(school["start_date"])
            school["end_date"] = pd.to_datetime(school["end_date"])
            for _, row in school.iterrows():
                cur = row["start_date"].date()
                end = row["end_date"].date()
                while cur <= end:
                    dates.add(cur)
                    cur += timedelta(days=1)
            # Determine which years have actual API data
            api_years_covered = set(school["start_date"].dt.year.unique())
            # Also include years from end_date (e.g. Christmas spanning Dec→Jan)
            api_years_covered |= set(school["end_date"].dt.year.unique())
        logger.info(
            "  Loaded %d school holiday dates from API (years %s).",
            len(dates),
            sorted(api_years_covered) if api_years_covered else "none",
        )
    else:
        logger.warning("School holidays CSV not found at %s.", api_path)

    # ── Extrapolate missing years using week-of-year windows ──
    missing_years = set(range(start_year, end_year + 1)) - api_years_covered
    if missing_years:
        logger.info(
            "  Extrapolating school holidays for years %s using "
            "week-of-year windows.",
            sorted(missing_years),
        )
        extrap_count = 0
        for year in sorted(missing_years):
            # Generate all dates in the year and check ISO week
            jan1 = pd.Timestamp(year=year, month=1, day=1).date()
            dec31 = pd.Timestamp(year=year, month=12, day=31).date()
            cur = jan1
            while cur <= dec31:
                iso_week = cur.isocalendar()[1]
                if iso_week in _ALL_SCHOOL_HOLIDAY_WEEKS:
                    if cur not in dates:
                        dates.add(cur)
                        extrap_count += 1
                cur += timedelta(days=1)
        logger.info(
            "  Added %d extrapolated school holiday dates for %d years.",
            extrap_count, len(missing_years),
        )

    return dates


def join_holidays(
    spine: pd.DataFrame,
    calendar_dir: str,
    start_year: int = 2012,
    end_year: int = 2025,
) -> pd.DataFrame:
    """Add public and school holiday binary flags to the hourly spine.

    Creates two columns:

    - ``is_public_holiday`` — 1 if the date is a Dutch public holiday
      (from the ``holidays`` Python library, full 2012–2025 coverage).
    - ``is_school_holiday`` — 1 if the date falls within a Dutch school
      holiday period.  Uses exact API data for 2020–2025 and
      week-of-year-based extrapolation for 2012–2019.
    """
    if not os.path.isdir(calendar_dir):
        logger.warning(
            "Calendar directory not found at %s — holiday columns will be 0.",
            calendar_dir,
        )
        spine["is_public_holiday"] = 0
        spine["is_school_holiday"] = 0
        return spine

    pub_dates = _load_public_holiday_dates(calendar_dir)
    sch_dates = _load_school_holiday_dates(calendar_dir, start_year, end_year)

    # Convert spine dates to Python date objects for set lookup
    spine_dates = pd.to_datetime(spine["timestamp"]).dt.date
    spine["is_public_holiday"] = spine_dates.isin(pub_dates).astype(int)
    spine["is_school_holiday"] = spine_dates.isin(sch_dates).astype(int)

    pub_hit = int(spine["is_public_holiday"].sum())
    sch_hit = int(spine["is_school_holiday"].sum())
    logger.info(
        "  Holiday flags: %d hours flagged public, %d hours flagged school.",
        pub_hit, sch_hit,
    )
    return spine


# ---------------------------------------------------------------------------
# Lag features
# ---------------------------------------------------------------------------

# Each tuple: (source_column, lag_hours, output_column_name)
_LAG_FEATURES = [
    # Load autoregressive lags (ACF peaks at 1h, 24h, 168h)
    ("entsoe_load_mw",          12,   "load_lag_12h"),
    ("entsoe_load_mw",          24,   "load_lag_24h"),
    ("entsoe_load_mw",          168,  "load_lag_1w"),
    ("entsoe_load_mw",          8760, "load_lag_1y"),
    # Weather lags (CCF-optimal lags from EDA §9.3)
    ("knmi_val_solar_rad_jcm2", 10,   "solar_rad_lag_10h"),
    ("knmi_val_humidity_pct",   12,   "humidity_lag_12h"),
    ("knmi_val_temp_c",         107,  "temp_lag_107h"),
]


def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create lagged versions of key columns.

    Lag features are computed via :func:`pandas.Series.shift`.  The
    first *N* rows of each lagged column are naturally NaN because there
    is no data *N* hours before the start of the spine.
    """
    for src_col, lag_hours, out_col in _LAG_FEATURES:
        if src_col not in df.columns:
            logger.warning(
                "  Lag source column '%s' not found — '%s' will be null.",
                src_col, out_col,
            )
            df[out_col] = np.nan
            continue
        df[out_col] = df[src_col].shift(lag_hours)
        nn = int(df[out_col].notna().sum())
        logger.info(
            "  Lag feature: %s → %s (shift %dh, %d non-null).",
            src_col, out_col, lag_hours, nn,
        )
    return df


# ---------------------------------------------------------------------------
# Energy crisis step variable
# ---------------------------------------------------------------------------

# The European energy crisis — triggered by the Russia-Ukraine war
# (Feb 2022) and the resulting gas supply disruption — caused Dutch
# electricity demand to drop to its lowest level since 2012 (mean
# 11,460 MW in 2022 vs 13,302 MW in 2018).  The EDA confirms 2022 as
# the trough year; demand recovers through H1 2023 and is back to
# pre-crisis levels by H2 2023 (annual mean 12,466 MW).
#
# The flag covers 2022-01-01 00:00 UTC through 2023-06-30 23:00 UTC
# (inclusive), representing the full structural-break window.
_ENERGY_CRISIS_START = pd.Timestamp("2022-01-01")
_ENERGY_CRISIS_END   = pd.Timestamp("2023-06-30 23:00:00")


def add_energy_crisis_flag(df: pd.DataFrame) -> pd.DataFrame:
    """Add a binary step variable for the 2022–H1-2023 energy crisis.

    ``is_energy_crisis`` is 1 for all hours in the interval
    [2022-01-01, 2023-06-30], and 0 otherwise.
    """
    mask = (df["timestamp"] >= _ENERGY_CRISIS_START) & (
        df["timestamp"] <= _ENERGY_CRISIS_END
    )
    df["is_energy_crisis"] = mask.astype(int)
    n_crisis = int(df["is_energy_crisis"].sum())
    logger.info(
        "  Energy crisis flag: %d hours flagged (%.1f%% of dataset).",
        n_crisis,
        100.0 * n_crisis / len(df),
    )
    return df


# ---------------------------------------------------------------------------
# PCA dimensionality reduction (CBS + KNMI validated)
# ---------------------------------------------------------------------------

_N_PCA_COMPONENTS = 15
_PCA_MAX_NULL_FRAC = 0.50   # drop columns with more than 50 % NaN


def add_pca_features(
    df: pd.DataFrame,
    n_components: int = _N_PCA_COMPONENTS,
) -> pd.DataFrame:
    """Add PCA components derived from all CBS + KNMI validated features.

    Steps:

    1. Identify all ``cbs_*`` and ``knmi_val_*`` columns in *df*.
    2. Drop columns with > 50 % NaN (staggered temporal coverage makes
       some CBS columns mostly null — e.g. ``cbs_gas_ode_tax`` ends 2022,
       ``cbs_elec_fixed_supply_rate_dynamic`` starts 2025).
    3. Fit ``StandardScaler`` + ``PCA(n_components)`` on complete rows.
    4. Transform all rows → ``pca_cbs_knmi_01`` … ``pca_cbs_knmi_15``.
       Rows with any missing input feature receive NaN for all PCA columns.

    The elbow analysis (``analysis/pca_elbow.py``) showed that 15 components
    capture ~95 % of the combined CBS + KNMI validated variance.
    """
    logger.info("Engineering PCA features (CBS + KNMI validated) …")

    # 1. Identify input columns
    cbs_cols = sorted(c for c in df.columns if c.startswith("cbs_"))
    knmi_val_cols = sorted(c for c in df.columns if c.startswith("knmi_val_"))
    all_input_cols = sorted(set(cbs_cols + knmi_val_cols))
    logger.info(
        "  PCA input: %d CBS + %d KNMI val = %d total columns.",
        len(cbs_cols), len(knmi_val_cols), len(all_input_cols),
    )

    # 2. Drop columns with > 50 % NaN
    missing_frac = df[all_input_cols].isna().mean()
    keep_cols = sorted(missing_frac[missing_frac <= _PCA_MAX_NULL_FRAC].index.tolist())
    drop_cols = sorted(set(all_input_cols) - set(keep_cols))
    if drop_cols:
        logger.info(
            "  PCA: dropped %d columns (>%.0f%% NaN): %s",
            len(drop_cols), _PCA_MAX_NULL_FRAC * 100,
            ", ".join(drop_cols),
        )
    if not keep_cols:
        logger.warning("  PCA: no columns with ≤ %.0f%% NaN — skipping.",
                       _PCA_MAX_NULL_FRAC * 100)
        for i in range(1, n_components + 1):
            df[f"pca_cbs_knmi_{i:02d}"] = np.nan
        return df

    logger.info("  PCA: retaining %d features for fitting.", len(keep_cols))

    # 3. Fit on complete rows
    complete_mask = df[keep_cols].notna().all(axis=1)
    n_complete = int(complete_mask.sum())
    logger.info("  PCA: %d complete rows for fitting.", n_complete)

    if n_complete < n_components:
        logger.warning(
            "  PCA: only %d complete rows (need >= %d) — skipping.",
            n_complete, n_components,
        )
        for i in range(1, n_components + 1):
            df[f"pca_cbs_knmi_{i:02d}"] = np.nan
        return df

    X_fit = df.loc[complete_mask, keep_cols].values.astype(np.float64)

    scaler = StandardScaler()
    scaler.fit(X_fit)
    X_fit_scaled = scaler.transform(X_fit)

    pca = PCA(n_components=n_components)
    pca.fit(X_fit_scaled)

    cum_var = np.cumsum(pca.explained_variance_ratio_)
    logger.info(
        "  PCA: %d components explain %.1f%% of variance.",
        n_components, cum_var[-1] * 100,
    )
    for i, (var, cvar) in enumerate(
        zip(pca.explained_variance_ratio_, cum_var), 1
    ):
        logger.info(
            "    PC%02d: %.4f (cumulative %.4f)", i, var, cvar
        )

    # 4. Transform all rows (NaN where any input is missing)
    pca_col_names = [f"pca_cbs_knmi_{i:02d}" for i in range(1, n_components + 1)]

    # Initialise with NaN
    for col_name in pca_col_names:
        df[col_name] = np.nan

    # Transform only complete rows
    X_all = df.loc[complete_mask, keep_cols].values.astype(np.float64)
    X_all_scaled = scaler.transform(X_all)
    pca_values = pca.transform(X_all_scaled)

    df.loc[complete_mask, pca_col_names] = pca_values

    n_pca_nn = int(df[pca_col_names[0]].notna().sum())
    logger.info(
        "  PCA: %d non-null rows (%.1f%% of dataset).",
        n_pca_nn, 100.0 * n_pca_nn / len(df),
    )

    return df


# ---------------------------------------------------------------------------
# Column ordering
# ---------------------------------------------------------------------------


def _ordered_columns(columns: list) -> list:
    """Return columns in a deterministic semantic order.

    Order: timestamp → target → satellite → CBS economic → temporal.
    Any ``cbs_*`` columns not in the explicit list are appended in sorted
    order after the known CBS columns, making this forward-compatible with
    new CBS indicators added in Phase 1.
    """
    existing = set(columns)

    # Explicitly ordered prefix
    preferred_head = [
        "timestamp",
        # Target
        "entsoe_load_mw",
        # VIIRS A2 selective aggregates (quality <= 1 only)
        "ntl_a2_mean", "ntl_a2_sum",
        "ntl_a2_valid_count", "ntl_a2_fill_count", "ntl_a2_invalid_count",
        # VIIRS A2 non-selective aggregates (all non-fill pixels incl. imputed)
        "ntl_a2_all_mean", "ntl_a2_all_sum",
        "ntl_a2_all_valid_count", "ntl_a2_all_fill_count", "ntl_a2_all_invalid_count",
        # VIIRS A1 aggregates (at-sensor radiance)
        "ntl_a1_mean", "ntl_a1_sum",
        "ntl_a1_valid_count", "ntl_a1_fill_count", "ntl_a1_invalid_count",
        # CBS — energy tariffs (explicit order)
        "cbs_gas_transport_rate", "cbs_gas_fixed_supply_rate",
        "cbs_gas_ode_tax", "cbs_gas_energy_tax", "cbs_gas_total_tax",
        "cbs_elec_transport_rate", "cbs_elec_fixed_supply_rate",
        "cbs_elec_fixed_supply_rate_dynamic", "cbs_elec_variable_supply_rate_dynamic",
        "cbs_elec_ode_tax", "cbs_elec_energy_tax", "cbs_elec_total_tax",
        "cbs_elec_energy_tax_refund",
        # CBS — GDP headline
        "cbs_gdp_yy", "cbs_gdp_qq",
        "cbs_gdp_wda_yy", "cbs_gdp_wda_qq",
        # CBS — population
        "cbs_population_million",
    ]

    # KNMI meteorological — non-validated (between CBS and temporal)
    preferred_knmi = [
        "knmi_temp_c", "knmi_dewpoint_c",
        "knmi_wind_speed_ms", "knmi_wind_speed_hourly_ms",
        "knmi_wind_gust_ms", "knmi_solar_rad_jcm2",
        "knmi_sunshine_h", "knmi_humidity_pct",
        "knmi_station_count",
    ]

    # KNMI meteorological — validated
    preferred_knmi_val = [
        "knmi_val_temp_c", "knmi_val_dewpoint_c",
        "knmi_val_wind_speed_ms", "knmi_val_wind_speed_hourly_ms",
        "knmi_val_wind_gust_ms", "knmi_val_solar_rad_jcm2",
        "knmi_val_sunshine_h", "knmi_val_humidity_pct",
        "knmi_val_station_count",
    ]

    # Temporal suffix
    preferred_tail = [
        "year", "month", "day", "hour",
        "day_of_week", "is_weekend",
        "day_of_year", "week_of_year", "quarter",
        # Cyclical temporal features
        "hour_sin", "hour_cos",
        "dow_sin", "dow_cos",
        "month_sin", "month_cos",
        # Holiday flags
        "is_public_holiday", "is_school_holiday",
        # Lag features — load
        "load_lag_12h", "load_lag_24h", "load_lag_1w", "load_lag_1y",
        # Lag features — weather (CCF-optimal)
        "solar_rad_lag_10h", "humidity_lag_12h", "temp_lag_107h",
        # Structural / event flags
        "is_energy_crisis",
    ]

    used = set()
    ordered: list[str] = []
    for c in preferred_head:
        if c in existing:
            ordered.append(c)
            used.add(c)

    # Append any remaining cbs_* columns in sorted order
    extra_cbs = sorted(c for c in existing if c.startswith("cbs_") and c not in used)
    ordered.extend(extra_cbs)
    used.update(extra_cbs)

    # Append KNMI columns in preferred order
    for c in preferred_knmi:
        if c in existing and c not in used:
            ordered.append(c)
            used.add(c)

    # Append any remaining knmi_* columns (non-validated) in sorted order
    extra_knmi = sorted(c for c in existing if c.startswith("knmi_") and not c.startswith("knmi_val_") and c not in used)
    ordered.extend(extra_knmi)
    used.update(extra_knmi)

    # Append validated KNMI columns in preferred order
    for c in preferred_knmi_val:
        if c in existing and c not in used:
            ordered.append(c)
            used.add(c)

    # Append any remaining knmi_val_* columns in sorted order
    extra_knmi_val = sorted(c for c in existing if c.startswith("knmi_val_") and c not in used)
    ordered.extend(extra_knmi_val)
    used.update(extra_knmi_val)

    # Append PCA components in numerical order
    pca_cols = sorted(c for c in existing if c.startswith("pca_cbs_knmi_") and c not in used)
    ordered.extend(pca_cols)
    used.update(pca_cols)

    for c in preferred_tail:
        if c in existing and c not in used:
            ordered.append(c)
            used.add(c)

    return ordered


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    # Default out-root: <repo_root>/data — two directories up from src/pipeline/
    _default_out_root = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "data",
    )
    parser = argparse.ArgumentParser(
        description="Phase 3: Merge all sources into a unified hourly Parquet dataset."
    )
    parser.add_argument(
        "--data-root", default="/projects/prjs2061/data",
        help="Root directory for raw input data (kept for CLI consistency; "
             "not used by Phase 3 itself).",
    )
    parser.add_argument(
        "--out-root", default=_default_out_root,
        help="Root directory for pipeline output data (processing_2/, processed/). "
             "Defaults to <repo>/data/ relative to this script.",
    )
    parser.add_argument(
        "--start", default="2012-01-01",
        help="Spine start date, YYYY-MM-DD (default: 2012-01-01).",
    )
    parser.add_argument(
        "--end", default="2025-12-31",
        help="Spine end date, YYYY-MM-DD (default: 2025-12-31).",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-run even if the final output already exists.",
    )
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Accepted for CLI compatibility; unused (merge is single-threaded).",
    )
    return parser.parse_args()


def main() -> None:
    """Build the final hourly dataset from Phase 2 outputs."""
    args = _parse_args()
    out_root = args.out_root
    p2_dir = os.path.join(out_root, "processing_2")
    out_dir = os.path.join(out_root, "processed")
    out_parquet = os.path.join(out_dir, "nl_hourly_dataset.parquet")
    os.makedirs(out_dir, exist_ok=True)

    logger.info("Phase 2 input : %s", p2_dir)
    logger.info("Final output  : %s", out_parquet)
    logger.info("Spine range   : %s → %s", args.start, args.end)
    logger.info("Force rerun   : %s", args.force)

    if _output_done(out_parquet) and not args.force:
        logger.info("Final output already present — nothing to do "
                    "(use --force to re-run).")
        return

    if args.force:
        _clear_path(out_parquet)

    t_total = time.time()

    # 1. Build the temporal scaffold
    df = build_hourly_spine(args.start, args.end)

    # 2. Join sources — all done in-memory with pandas merge.
    df = join_entsoe(df, os.path.join(p2_dir, "entsoe", "data"))
    df = join_viirs(df, os.path.join(p2_dir, "viirs_a2_daily",     "data"), product="a2")
    df = join_viirs(df, os.path.join(p2_dir, "viirs_a2_all_daily", "data"), product="a2_all")
    df = join_viirs(df, os.path.join(p2_dir, "viirs_a1_daily",     "data"), product="a1")
    df = join_cbs(df, os.path.join(p2_dir, "cbs_combined", "data"))
    df = join_knmi(df, os.path.join(p2_dir, "knmi", "data"), col_prefix="knmi")
    df = join_knmi(df, os.path.join(p2_dir, "knmi_validated", "data"), col_prefix="knmi_val")

    # 2b. Join holiday flags from calendar CSVs
    calendar_dir = os.path.join(args.data_root, "calendar")
    start_year = int(args.start[:4])
    end_year = int(args.end[:4])
    df = join_holidays(df, calendar_dir, start_year, end_year)

    # 2c. Engineer lag features (must be after all source joins)
    logger.info("Engineering lag features …")
    df = add_lag_features(df)

    # 2d. PCA dimensionality reduction (CBS + KNMI validated)
    df = add_pca_features(df)

    # 2e. Add energy crisis step variable
    logger.info("Adding energy crisis flag …")
    df = add_energy_crisis_flag(df)

    # 3. Reorder columns, drop internal join key
    ordered = _ordered_columns(df.columns.tolist())
    df = df[[c for c in ordered if c != "date"]]

    # 4. Sort by timestamp (should already be sorted, but make sure)
    df = df.sort_values("timestamp").reset_index(drop=True)

    logger.info("Merged dataset: %d rows × %d columns (%.1f MB in memory).",
                len(df), len(df.columns),
                df.memory_usage(deep=True).sum() / (1024 * 1024))

    # 5. Write partitioned by year using PyArrow for efficient I/O
    table = pa.Table.from_pandas(df, preserve_index=False)
    os.makedirs(out_parquet, exist_ok=True)
    pq.write_to_dataset(
        table,
        root_path=out_parquet,
        partition_cols=["year"],
    )
    # Write _SUCCESS marker for idempotency
    with open(os.path.join(out_parquet, "_SUCCESS"), "w"):
        pass
    logger.info("Final dataset written → %s (%.1fs)",
                out_parquet, time.time() - t_total)

    # 6. Quality + coverage
    total = len(df)
    entsoe_nn     = int(df["entsoe_load_mw"].notna().sum())
    viirs_a2_nn   = int(df["ntl_a2_mean"].notna().sum())     if "ntl_a2_mean"     in df.columns else 0
    viirs_a2all_nn= int(df["ntl_a2_all_mean"].notna().sum()) if "ntl_a2_all_mean" in df.columns else 0
    viirs_a1_nn   = int(df["ntl_a1_mean"].notna().sum())     if "ntl_a1_mean"     in df.columns else 0
    cbs_energy_nn = int(df["cbs_gas_total_tax"].notna().sum()) if "cbs_gas_total_tax" in df.columns else 0
    cbs_gdp_nn    = int(df["cbs_gdp_yy"].notna().sum())       if "cbs_gdp_yy"        in df.columns else 0
    cbs_cpi_nn    = int(df["cbs_cpi_energy"].notna().sum())    if "cbs_cpi_energy"    in df.columns else 0
    cbs_gep_nn    = int(df["cbs_gep_gas_hh_total"].notna().sum()) if "cbs_gep_gas_hh_total" in df.columns else 0
    knmi_nn       = int(df["knmi_temp_c"].notna().sum())       if "knmi_temp_c"     in df.columns else 0
    knmi_val_nn   = int(df["knmi_val_temp_c"].notna().sum())   if "knmi_val_temp_c" in df.columns else 0
    pca_nn        = int(df["pca_cbs_knmi_01"].notna().sum())   if "pca_cbs_knmi_01" in df.columns else 0
    pub_hol_nn    = int(df["is_public_holiday"].sum())          if "is_public_holiday"  in df.columns else 0
    sch_hol_nn    = int(df["is_school_holiday"].sum())          if "is_school_holiday"  in df.columns else 0
    load = df["entsoe_load_mw"].dropna()

    logger.info("Final: %d hourly rows.", total)
    logger.info(
        "Coverage — ENTSO-E: %.1f%% | VIIRS A2 (sel): %.1f%% | VIIRS A2 (all): %.1f%% "
        "| VIIRS A1: %.1f%% | CBS Tariffs: %.1f%% | CBS GDP: %.1f%% | CBS CPI: %.1f%% "
        "| CBS GEP: %.1f%% | KNMI: %.1f%% | KNMI Val: %.1f%% | PCA: %.1f%%",
        100 * entsoe_nn / total,
        100 * viirs_a2_nn / total,
        100 * viirs_a2all_nn / total,
        100 * viirs_a1_nn / total,
        100 * cbs_energy_nn / total, 100 * cbs_gdp_nn / total,
        100 * cbs_cpi_nn / total, 100 * cbs_gep_nn / total,
        100 * knmi_nn / total, 100 * knmi_val_nn / total,
        100 * pca_nn / total,
    )

    # Coverage assertion for the target variable
    entsoe_pct = entsoe_nn / total if total else 0
    coverage_warning = None
    if entsoe_pct < 0.85:
        coverage_warning = (
            f"ENTSO-E coverage below 85%: {100 * entsoe_pct:.1f}%"
        )
        logger.error("%s — check source data.", coverage_warning)

    extra = {
        "spine": {
            "start": args.start,
            "end": args.end,
            "total_hours": total,
        },
        "source_coverage": {
            "entsoe_load_mw": {
                "non_null": entsoe_nn,
                "pct": round(100 * entsoe_nn / total, 2) if total else 0.0,
            },
            "ntl_a2_mean": {
                "non_null": viirs_a2_nn,
                "pct": round(100 * viirs_a2_nn / total, 2) if total else 0.0,
            },
            "ntl_a2_all_mean": {
                "non_null": viirs_a2all_nn,
                "pct": round(100 * viirs_a2all_nn / total, 2) if total else 0.0,
            },
            "ntl_a1_mean": {
                "non_null": viirs_a1_nn,
                "pct": round(100 * viirs_a1_nn / total, 2) if total else 0.0,
            },
            "cbs_gas_total_tax": {
                "non_null": cbs_energy_nn,
                "pct": round(100 * cbs_energy_nn / total, 2) if total else 0.0,
            },
            "cbs_gdp_yy": {
                "non_null": cbs_gdp_nn,
                "pct": round(100 * cbs_gdp_nn / total, 2) if total else 0.0,
            },
            "cbs_cpi_energy": {
                "non_null": cbs_cpi_nn,
                "pct": round(100 * cbs_cpi_nn / total, 2) if total else 0.0,
            },
            "cbs_gep_gas_hh_total": {
                "non_null": cbs_gep_nn,
                "pct": round(100 * cbs_gep_nn / total, 2) if total else 0.0,
            },
            "knmi_temp_c": {
                "non_null": knmi_nn,
                "pct": round(100 * knmi_nn / total, 2) if total else 0.0,
            },
            "knmi_val_temp_c": {
                "non_null": knmi_val_nn,
                "pct": round(100 * knmi_val_nn / total, 2) if total else 0.0,
            },
        },
        "load_statistics": {
            "min_mw": round(float(load.min()), 1) if len(load) else None,
            "max_mw": round(float(load.max()), 1) if len(load) else None,
            "mean_mw": round(float(load.mean()), 1) if len(load) else None,
            "stddev_mw": round(float(load.std()), 1) if len(load) else None,
        },
        "stage_elapsed_seconds": round(time.time() - t_total, 1),
    }
    if coverage_warning:
        extra["warnings"] = [coverage_warning]

    metrics = _compute_quality_metrics(
        out_parquet, "nl_hourly_merged", 3, extra
    )
    _write_quality_json(metrics, out_dir)
    logger.info("=== Phase 3 complete in %.0fs. ===", time.time() - t_total)


if __name__ == "__main__":
    main()