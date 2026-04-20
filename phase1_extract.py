#!/usr/bin/env python3
"""Phase 1: Extract raw data sources into cleaned, source-level Parquet files.

This is Stage 1 of the three-phase NL energy demand data pipeline. It reads
each raw data source independently and writes cleaned, typed Parquet tables
to ``data/processing_1/<source>/``. A ``data_quality.json`` report is written
alongside each output.

Sources processed:
    - **VIIRS VNP46A2** (HDF5, daily): Netherlands-masked pixel-level raster
      data preserved in spatial form. Each pixel within the NL bounding box
      becomes a Parquet row with its coordinates and radiance value.
      Partitioned by year.
    - **CBS Energy Prices** (CSV, quarterly): Melted to long format, expanded
      to monthly granularity.
    - **CBS Macro** (CSV, annual): GDP and population merged into one table.
    - **ENTSO-E Electricity Load** (XLSX, hourly): Multiple overlapping Excel
      files with two schema variants. Unified, deduplicated, UTC-normalised.
      Partitioned by year.

Usage::

    python phase1_extract.py [--data-root /path/to/data] [--workers N]
"""

import argparse
import glob
import json
import logging
import os
import re
import sys
from datetime import datetime, date, timedelta
from typing import Optional, Tuple

import h5py
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("phase1_extract")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# VNP46A2 HDF5 structure (Collection 2, v002)
_VIIRS_GROUP = "HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields"
_VIIRS_NTL_DATASET = "Gap_Filled_DNB_BRDF-Corrected_NTL"
_VIIRS_QF_DATASET = "Mandatory_Quality_Flag"
_VIIRS_LAT_DATASET = "lat"
_VIIRS_LON_DATASET = "lon"

# Fill value for Gap_Filled_DNB_BRDF-Corrected_NTL (float32)
_VIIRS_FILL_VALUE = -999.9

# Netherlands bounding box (WGS84)
_NL_LAT_MIN = 50.75
_NL_LAT_MAX = 53.55
_NL_LON_MIN = 3.35
_NL_LON_MAX = 7.25

# ENTSO-E Netherlands country code
_NL_COUNTRY_CODE = "NL"


# ---------------------------------------------------------------------------
# Data quality helpers
# ---------------------------------------------------------------------------


def _compute_quality_metrics(
    spark: SparkSession,
    parquet_path: str,
    source_name: str,
    phase: int,
    extra_metrics: dict = None,
) -> dict:
    """Compute comprehensive data quality metrics for a Parquet dataset.

    Reads the Parquet at ``parquet_path``, computes per-column null counts,
    row/column counts, size on disk, and date range if applicable.

    Args:
        spark: Active SparkSession.
        parquet_path: Path to the Parquet directory.
        source_name: Human-readable source identifier.
        phase: Pipeline phase number (1, 2, or 3).
        extra_metrics: Optional dict of additional metrics to include.

    Returns:
        A dictionary of quality metrics ready for JSON serialisation.
    """
    df = spark.read.parquet(parquet_path)
    total_rows = df.count()
    columns = df.columns

    # Per-column null analysis
    col_metrics = {}
    null_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns]
    null_counts = df.select(null_exprs).collect()[0]

    for c in columns:
        dtype = str(df.schema[c].dataType)
        nc = int(null_counts[c])
        col_metrics[c] = {
            "dtype": dtype,
            "null_count": nc,
            "null_pct": round(100.0 * nc / total_rows, 2) if total_rows > 0 else 0.0,
        }

    # Disk size
    size_bytes = 0
    for root, dirs, files in os.walk(parquet_path):
        for fname in files:
            size_bytes += os.path.getsize(os.path.join(root, fname))

    # Partition count (number of part-* files)
    part_files = []
    for root, dirs, files in os.walk(parquet_path):
        part_files.extend(f for f in files if f.startswith("part-"))

    # Date/timestamp range detection
    date_range = {}
    for candidate in ["date", "timestamp", "timestamp_utc"]:
        if candidate in columns:
            min_max = df.select(F.min(candidate), F.max(candidate)).collect()[0]
            date_range["column"] = candidate
            date_range["min"] = str(min_max[0])
            date_range["max"] = str(min_max[1])
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
    """Write a data_quality.json file to the given directory.

    Args:
        metrics: Dictionary of quality metrics.
        out_dir: Directory to write the JSON file into.
    """
    path = os.path.join(out_dir, "data_quality.json")
    with open(path, "w") as fh:
        json.dump(metrics, fh, indent=2, default=str)
    logger.info("Quality report written → %s", path)


# ---------------------------------------------------------------------------
# VIIRS helpers
# ---------------------------------------------------------------------------


def _parse_viirs_date(filename: str) -> Optional[date]:
    """Parse the acquisition date from a VNP46A2 HDF5 filename.

    Filenames encode the date as ``AYYYYDDD`` (year + day-of-year),
    e.g. ``VNP46A2.A2020015.h18v03.002...h5`` → 2020-01-15.

    Args:
        filename: Basename or full path of the HDF5 file.

    Returns:
        A ``datetime.date`` object, or ``None`` if the pattern is not found.
    """
    match = re.search(r"\.A(\d{4})(\d{3})\.", os.path.basename(filename))
    if not match:
        return None
    year = int(match.group(1))
    doy = int(match.group(2))
    return (datetime(year, 1, 1) + timedelta(days=doy - 1)).date()


def _compute_nl_indices(h5_filepath: str) -> tuple:
    """Compute the Netherlands bounding-box row/col indices from one HDF5 file.

    The lat/lon arrays are 1D (shape 2400,) and identical across all h18v03
    files, so this only needs to be called once.

    Args:
        h5_filepath: Path to any VNP46A2 h18v03 HDF5 file.

    Returns:
        A tuple ``(lat_indices, lon_indices, lat_values, lon_values)`` where
        each ``*_indices`` is a 1D numpy array of integer indices and
        ``*_values`` is the corresponding coordinate values.
    """
    with h5py.File(h5_filepath, "r") as hf:
        grp = hf[_VIIRS_GROUP]
        lat = grp[_VIIRS_LAT_DATASET][:]
        lon = grp[_VIIRS_LON_DATASET][:]

    lat_mask = (lat >= _NL_LAT_MIN) & (lat <= _NL_LAT_MAX)
    lon_mask = (lon >= _NL_LON_MIN) & (lon <= _NL_LON_MAX)

    lat_indices = np.where(lat_mask)[0]
    lon_indices = np.where(lon_mask)[0]

    return lat_indices, lon_indices, lat[lat_mask], lon[lon_mask]


def _read_viirs_nl_pixels(
    filepath: str,
    lat_indices: np.ndarray,
    lon_indices: np.ndarray,
    lat_values: np.ndarray,
    lon_values: np.ndarray,
) -> list:
    """Read one VNP46A2 HDF5 file and extract NL-cropped pixel rows.

    Reads the NTL radiance and quality flag rasters, crops to the Netherlands
    bounding box, and returns one tuple per pixel.

    Args:
        filepath: Absolute path to the ``.h5`` file.
        lat_indices: 1D array of row indices within the NL bbox.
        lon_indices: 1D array of column indices within the NL bbox.
        lat_values: 1D array of latitude values for those rows.
        lon_values: 1D array of longitude values for those columns.

    Returns:
        A list of tuples, each representing one pixel:
        ``(date_iso, year, row_idx, col_idx, lat, lon, ntl_radiance_or_None,
          quality_flag, is_fill)``.
        Returns an empty list if the file cannot be read.
    """
    obs_date = _parse_viirs_date(filepath)
    if obs_date is None:
        return []

    date_iso = obs_date.isoformat()
    year = obs_date.year

    try:
        with h5py.File(filepath, "r") as hf:
            grp = hf[_VIIRS_GROUP]
            # Read the NL subgrid using numpy advanced indexing
            # np.ix_ creates an open mesh for cross-indexing
            ntl_full = grp[_VIIRS_NTL_DATASET]
            qf_full = grp[_VIIRS_QF_DATASET]

            # Slice the NL rectangle (contiguous rows/cols)
            r_start, r_end = int(lat_indices[0]), int(lat_indices[-1]) + 1
            c_start, c_end = int(lon_indices[0]), int(lon_indices[-1]) + 1

            ntl_crop = ntl_full[r_start:r_end, c_start:c_end]  # (672, 937)
            qf_crop = qf_full[r_start:r_end, c_start:c_end]    # (672, 937)

    except Exception as exc:
        logger.warning("Failed to read %s: %s", filepath, exc)
        return []

    rows = []
    n_lat = len(lat_values)
    n_lon = len(lon_values)

    for i in range(n_lat):
        ri = int(lat_indices[i])
        lat_val = float(lat_values[i])
        for j in range(n_lon):
            ci = int(lon_indices[j])
            lon_val = float(lon_values[j])

            ntl_val = float(ntl_crop[i, j])
            qf_val = int(qf_crop[i, j])

            # Determine fill status
            is_fill = bool(np.isclose(ntl_val, _VIIRS_FILL_VALUE, atol=0.1))

            # Store radiance as None if fill, else the raw float value
            radiance = None if is_fill else ntl_val

            rows.append((
                date_iso,
                year,
                ri,
                ci,
                lat_val,
                lon_val,
                radiance,
                qf_val,
                is_fill,
            ))

    return rows


# ---------------------------------------------------------------------------
# Phase 1A: VIIRS A2
# ---------------------------------------------------------------------------


def extract_viirs(spark: SparkSession, viirs_dir: str, out_dir: str) -> None:
    """Extract VIIRS VNP46A2 HDF5 files to NL-masked pixel-level Parquet.

    Each HDF5 file is read on a Spark executor. The 2400×2400 raster is
    cropped to the Netherlands bounding box (~672×937 pixels) and stored as
    individual pixel rows preserving spatial coordinates.

    Args:
        spark: Active SparkSession.
        viirs_dir: Directory containing ``.h5`` files (e.g. ``data/viirs/A2``).
        out_dir: Output directory (e.g. ``data/processing_1/viirs_a2``).
    """
    logger.info("=== Phase 1: VIIRS A2 extraction ===")
    h5_files = sorted(glob.glob(os.path.join(viirs_dir, "*.h5")))
    if not h5_files:
        logger.warning("No .h5 files found in %s — skipping.", viirs_dir)
        return

    logger.info("Found %d VIIRS HDF5 files.", len(h5_files))

    # Compute NL bounding box indices from the first file (identical for all)
    lat_idx, lon_idx, lat_vals, lon_vals = _compute_nl_indices(h5_files[0])
    logger.info(
        "NL crop: %d lat rows (idx %d–%d) × %d lon cols (idx %d–%d) = %d pixels/day.",
        len(lat_idx), lat_idx[0], lat_idx[-1],
        len(lon_idx), lon_idx[0], lon_idx[-1],
        len(lat_idx) * len(lon_idx),
    )

    # Broadcast the index arrays to all executors
    bc_lat_idx = spark.sparkContext.broadcast(lat_idx)
    bc_lon_idx = spark.sparkContext.broadcast(lon_idx)
    bc_lat_vals = spark.sparkContext.broadcast(lat_vals)
    bc_lon_vals = spark.sparkContext.broadcast(lon_vals)

    # Distribute files across Spark workers
    num_partitions = max(1, len(h5_files) // 20)
    files_rdd = spark.sparkContext.parallelize(h5_files, num_partitions)

    def _process_partition(filepaths):
        """Read each HDF5 file in this partition and yield pixel rows."""
        li = bc_lat_idx.value
        lo = bc_lon_idx.value
        lv = bc_lat_vals.value
        lov = bc_lon_vals.value
        for fp in filepaths:
            yield from _read_viirs_nl_pixels(fp, li, lo, lv, lov)

    schema = T.StructType([
        T.StructField("date", T.StringType(), nullable=False),
        T.StructField("year", T.IntegerType(), nullable=False),
        T.StructField("row_idx", T.IntegerType(), nullable=False),
        T.StructField("col_idx", T.IntegerType(), nullable=False),
        T.StructField("lat", T.DoubleType(), nullable=False),
        T.StructField("lon", T.DoubleType(), nullable=False),
        T.StructField("ntl_radiance", T.FloatType(), nullable=True),
        T.StructField("quality_flag", T.ShortType(), nullable=False),
        T.StructField("is_fill", T.BooleanType(), nullable=False),
    ])

    rows_rdd = files_rdd.mapPartitions(_process_partition)
    df = spark.createDataFrame(rows_rdd, schema)

    # Cast date string to DateType
    df = df.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, "data")
    df.write.mode("overwrite").partitionBy("year").parquet(parquet_path)

    # Count unique dates for quality reporting
    date_count = df.select("date").distinct().count()
    row_count = df.count()
    logger.info(
        "VIIRS A2: wrote %d pixel rows across %d observation days → %s",
        row_count, date_count, parquet_path,
    )

    # Data quality JSON
    extra = {
        "completeness": {
            "total_h5_files": len(h5_files),
            "total_observation_days": date_count,
            "pixels_per_day": len(lat_idx) * len(lon_idx),
            "nl_lat_range": [float(lat_vals.min()), float(lat_vals.max())],
            "nl_lon_range": [float(lon_vals.min()), float(lon_vals.max())],
        }
    }
    metrics = _compute_quality_metrics(spark, parquet_path, "viirs_a2", 1, extra)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 1B: CBS Energy Prices
# ---------------------------------------------------------------------------


def _parse_cbs_period(period_str: str) -> Optional[Tuple[int, int]]:
    """Parse a CBS period string into a (year, month) tuple.

    Handles quarterly (``"2012 1st quarter"``), monthly (``"2017 October"``),
    and annual (``"2016"``). Annual periods return ``None`` (skipped).

    Args:
        period_str: Raw period label from CBS CSV header.

    Returns:
        A ``(year, month)`` tuple, or ``None`` for annual/unrecognised.
    """
    s = str(period_str).strip().replace("*", "")
    if re.fullmatch(r"\d{4}", s):
        return None
    qtr = re.match(r"(\d{4})\s+(\d)(?:st|nd|rd|th)\s+quarter", s, re.I)
    if qtr:
        return (int(qtr.group(1)), (int(qtr.group(2)) - 1) * 3 + 1)
    months = {
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
        "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
        "november": 11, "december": 12,
    }
    mth = re.match(r"(\d{4})\s+([a-zA-Z]+)", s)
    if mth:
        m = months.get(mth.group(2).lower())
        if m:
            return (int(mth.group(1)), m)
    return None


def extract_cbs_energy(spark: SparkSession, cbs_dir: str, out_dir: str) -> None:
    """Extract CBS energy price CSV to a monthly long-format Parquet.

    The raw file is wide-format with a multi-row header. This function melts
    it to long format, parses period labels, expands quarterly values to all
    constituent months, and writes the result.

    Args:
        spark: Active SparkSession.
        cbs_dir: Directory containing CBS CSV files.
        out_dir: Output directory (e.g. ``data/processing_1/cbs_energy``).
    """
    logger.info("=== Phase 1: CBS Energy extraction ===")
    pattern = os.path.join(cbs_dir, "Energy__consumption*")
    matches = glob.glob(pattern)
    if not matches:
        logger.warning("No CBS energy file found — skipping.")
        return

    filepath = matches[0]
    logger.info("Reading: %s", filepath)

    raw = pd.read_csv(filepath, sep=";", header=3, encoding="utf-8",
                       quotechar='"', dtype=str)

    id_cols = raw.columns[:3].tolist()
    period_cols = raw.columns[3:].tolist()
    raw = raw.rename(columns={
        id_cols[0]: "metric", id_cols[1]: "commodity", id_cols[2]: "unit",
    })

    # Keep only price index rows
    price_df = raw[raw["metric"].str.contains("Price index", na=False)].copy()

    melted = price_df.melt(
        id_vars=["metric", "commodity", "unit"],
        value_vars=period_cols, var_name="period", value_name="value",
    )

    commodity_map = {
        "Total energy commodities": "total_energy",
        "Natural gas": "natural_gas",
        "Crude and petroleum products": "crude_oil",
        "Electricity": "electricity",
    }
    melted["col_name"] = melted["commodity"].map(commodity_map)
    melted = melted.dropna(subset=["col_name"])

    melted["ym"] = melted["period"].apply(_parse_cbs_period)
    melted = melted.dropna(subset=["ym"])
    melted["year"] = melted["ym"].apply(lambda x: x[0])
    melted["month"] = melted["ym"].apply(lambda x: x[1])
    melted["value"] = pd.to_numeric(melted["value"], errors="coerce")

    pivot = melted.pivot_table(
        index=["year", "month"], columns="col_name",
        values="value", aggfunc="mean",
    ).reset_index()
    pivot.columns.name = None

    # Expand quarterly rows (month 1,4,7,10) to fill all 3 months
    expanded_rows = []
    for _, row in pivot.iterrows():
        m = int(row["month"])
        y = int(row["year"])
        months = [m, m + 1, m + 2] if m in {1, 4, 7, 10} else [m]
        for em in months:
            nr = row.copy()
            nr["month"] = em
            expanded_rows.append(nr)
    expanded = pd.DataFrame(expanded_rows)
    expanded = expanded.drop_duplicates(subset=["year", "month"], keep="first")
    expanded = expanded.sort_values(["year", "month"]).reset_index(drop=True)

    rename = {
        "total_energy": "cbs_total_energy_price_idx",
        "natural_gas": "cbs_natural_gas_price_idx",
        "crude_oil": "cbs_crude_oil_price_idx",
        "electricity": "cbs_electricity_price_idx",
    }
    expanded = expanded.rename(columns=rename)
    expanded["year"] = expanded["year"].astype(int)
    expanded["month"] = expanded["month"].astype(int)
    cols = ["year", "month"] + [c for c in expanded.columns if c.startswith("cbs_")]
    expanded = expanded[cols]

    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, "data")
    sdf = spark.createDataFrame(expanded)
    sdf.write.mode("overwrite").parquet(parquet_path)
    logger.info("CBS Energy: wrote %d monthly records → %s", len(expanded), parquet_path)

    metrics = _compute_quality_metrics(spark, parquet_path, "cbs_energy", 1)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 1C: CBS Macro (GDP + Population)
# ---------------------------------------------------------------------------


def extract_cbs_macro(spark: SparkSession, cbs_dir: str, out_dir: str) -> None:
    """Extract CBS GDP and population CSVs into an annual Parquet table.

    Args:
        spark: Active SparkSession.
        cbs_dir: Directory containing CBS CSV files.
        out_dir: Output directory (e.g. ``data/processing_1/cbs_macro``).
    """
    logger.info("=== Phase 1: CBS Macro extraction ===")

    # --- GDP ---
    gdp_path = os.path.join(cbs_dir, "table__84087ENG.csv")
    if os.path.exists(gdp_path):
        raw_gdp = pd.read_csv(gdp_path, sep=",", quotechar='"', dtype=str)
        raw_gdp = raw_gdp[~raw_gdp.iloc[:, 0].str.startswith("Bron", na=True)]
        raw_gdp["year"] = raw_gdp.iloc[:, 0].str.extract(r"(\d{4})").astype(int)
        gdp_col = [c for c in raw_gdp.columns if "Gross domestic product" in c]
        if gdp_col:
            raw_gdp["gdp_million_eur"] = pd.to_numeric(
                raw_gdp[gdp_col[0]].str.replace(".", "", regex=False)
                                    .str.replace(",", ".", regex=False),
                errors="coerce",
            )
        else:
            raw_gdp["gdp_million_eur"] = float("nan")
        gdp_df = raw_gdp[["year", "gdp_million_eur"]].dropna(subset=["year"])
    else:
        logger.warning("GDP file not found: %s", gdp_path)
        gdp_df = pd.DataFrame(columns=["year", "gdp_million_eur"])

    # --- Population ---
    pop_path = os.path.join(cbs_dir, "Population (x million).csv")
    if os.path.exists(pop_path):
        raw_pop = pd.read_csv(pop_path, sep=";", quotechar='"', dtype=str)
        raw_pop.columns = [c.strip() for c in raw_pop.columns]
        year_col = [c for c in raw_pop.columns if "year" in c.lower() or "jaar" in c.lower()]
        pop_col = [c for c in raw_pop.columns if "pop" in c.lower() or "bev" in c.lower()]
        if year_col and pop_col:
            raw_pop["year"] = pd.to_numeric(raw_pop[year_col[0]], errors="coerce")
            raw_pop["population_million"] = pd.to_numeric(
                raw_pop[pop_col[0]].str.replace(",", ".", regex=False), errors="coerce",
            )
        else:
            raw_pop["year"] = pd.to_numeric(raw_pop.iloc[:, 0], errors="coerce")
            raw_pop["population_million"] = pd.to_numeric(
                raw_pop.iloc[:, 1].astype(str).str.replace(",", ".", regex=False),
                errors="coerce",
            )
        pop_df = raw_pop[["year", "population_million"]].dropna(subset=["year"])
        pop_df["year"] = pop_df["year"].astype(int)
    else:
        logger.warning("Population file not found: %s", pop_path)
        pop_df = pd.DataFrame(columns=["year", "population_million"])

    merged = pd.merge(gdp_df, pop_df, on="year", how="outer").sort_values("year")
    merged["year"] = merged["year"].astype(int)

    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, "data")
    sdf = spark.createDataFrame(merged)
    sdf.write.mode("overwrite").parquet(parquet_path)
    logger.info("CBS Macro: wrote %d annual records → %s", len(merged), parquet_path)

    metrics = _compute_quality_metrics(spark, parquet_path, "cbs_macro", 1)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 1D: ENTSO-E Load
# ---------------------------------------------------------------------------


def _read_entsoe_long(filepath: str) -> pd.DataFrame:
    """Read a new-style ENTSO-E XLSX (long format, post-2015 schema).

    Args:
        filepath: Path to the XLSX file.

    Returns:
        DataFrame with ``[timestamp_utc, entsoe_load_mw]`` for NL only.
    """
    xl = pd.ExcelFile(filepath)
    frames = []
    for sheet in xl.sheet_names:
        try:
            df = xl.parse(sheet)
        except Exception:
            continue
        if "CountryCode" not in df.columns or "DateUTC" not in df.columns:
            continue
        nl = df[df["CountryCode"].astype(str).str.strip() == _NL_COUNTRY_CODE].copy()
        if nl.empty:
            continue
        nl["timestamp_utc"] = pd.to_datetime(nl["DateUTC"], utc=True, errors="coerce")
        val_col = "Value" if nl["Value"].notna().any() else "Value_ScaleTo100"
        nl["entsoe_load_mw"] = pd.to_numeric(nl[val_col], errors="coerce")
        frames.append(nl[["timestamp_utc", "entsoe_load_mw"]])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["timestamp_utc", "entsoe_load_mw"]
    )


def _read_entsoe_wide(filepath: str) -> pd.DataFrame:
    """Read the legacy ENTSO-E XLSX (wide format, 2006–2015 schema).

    Args:
        filepath: Path to the XLSX file.

    Returns:
        DataFrame with ``[timestamp_utc, entsoe_load_mw]`` for NL only.
    """
    xl = pd.ExcelFile(filepath)
    frames = []
    for sheet in xl.sheet_names:
        try:
            raw = xl.parse(sheet, header=2)
        except Exception:
            continue
        raw.columns = [str(c).strip() for c in raw.columns]
        if "Country" not in raw.columns:
            continue
        nl = raw[raw["Country"].astype(str).str.strip() == _NL_COUNTRY_CODE].copy()
        if nl.empty:
            continue
        hour_cols = [c for c in nl.columns if re.fullmatch(r"\d+\.0", c)]
        if not hour_cols:
            continue
        id_vars = [c for c in ["Country", "Year", "Month", "Day"] if c in nl.columns]
        melted = nl[id_vars + hour_cols].melt(
            id_vars=id_vars, value_vars=hour_cols,
            var_name="hour_str", value_name="entsoe_load_mw",
        )
        melted["hour"] = melted["hour_str"].str.replace(".0", "", regex=False).astype(int)
        melted["entsoe_load_mw"] = pd.to_numeric(melted["entsoe_load_mw"], errors="coerce")
        melted["timestamp_local"] = pd.to_datetime({
            "year": melted["Year"].astype(int), "month": melted["Month"].astype(int),
            "day": melted["Day"].astype(int), "hour": melted["hour"],
        }, errors="coerce")
        melted["timestamp_utc"] = melted["timestamp_local"].dt.tz_localize(
            "CET", ambiguous="NaT", nonexistent="NaT"
        ).dt.tz_convert("UTC")
        frames.append(melted[["timestamp_utc", "entsoe_load_mw"]])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["timestamp_utc", "entsoe_load_mw"]
    )


def extract_entsoe(spark: SparkSession, entsoe_dir: str, out_dir: str) -> None:
    """Extract all ENTSO-E XLSX files to a deduplicated hourly NL Parquet.

    Handles both wide (2006–2015) and long (2015+) XLSX schemas. Filters to
    NL, normalises to UTC, deduplicates, and writes partitioned by year.

    Args:
        spark: Active SparkSession.
        entsoe_dir: Directory containing XLSX files.
        out_dir: Output directory (e.g. ``data/processing_1/entsoe``).
    """
    logger.info("=== Phase 1: ENTSO-E extraction ===")
    xlsx_files = sorted(glob.glob(os.path.join(entsoe_dir, "*.xlsx")))
    if not xlsx_files:
        logger.warning("No XLSX files found — skipping.")
        return

    all_frames = []
    for fpath in xlsx_files:
        logger.info("  Reading: %s", os.path.basename(fpath))
        try:
            xl = pd.ExcelFile(fpath)
            probe = xl.parse(xl.sheet_names[0], nrows=5)
            cols = [str(c).strip() for c in probe.columns]
        except Exception:
            continue
        if "Country" in cols and any(re.fullmatch(r"\d+\.0", c) for c in cols):
            df = _read_entsoe_wide(fpath)
        else:
            df = _read_entsoe_long(fpath)
        if not df.empty:
            all_frames.append(df)
            logger.info("    → %d NL rows.", len(df))

    if not all_frames:
        logger.warning("No ENTSO-E NL data found.")
        return

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.dropna(subset=["timestamp_utc", "entsoe_load_mw"])
    combined["timestamp_utc"] = combined["timestamp_utc"].dt.floor("h")
    combined = (
        combined.sort_values("timestamp_utc")
                .drop_duplicates(subset=["timestamp_utc"], keep="last")
                .reset_index(drop=True)
    )
    combined["timestamp_utc"] = combined["timestamp_utc"].dt.tz_convert("UTC").dt.tz_localize(None)
    combined["year"] = combined["timestamp_utc"].dt.year

    logger.info("ENTSO-E: %d unique hourly NL records after dedup.", len(combined))

    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, "data")

    schema = T.StructType([
        T.StructField("timestamp_utc", T.TimestampType(), nullable=False),
        T.StructField("entsoe_load_mw", T.DoubleType(), nullable=True),
        T.StructField("year", T.IntegerType(), nullable=False),
    ])
    sdf = spark.createDataFrame(combined, schema=schema)
    sdf.write.mode("overwrite").partitionBy("year").parquet(parquet_path)
    logger.info("ENTSO-E: wrote → %s", parquet_path)

    extra = {
        "completeness": {
            "total_xlsx_files": len(xlsx_files),
            "total_hourly_records": len(combined),
            "year_range": [int(combined["year"].min()), int(combined["year"].max())],
        }
    }
    metrics = _compute_quality_metrics(spark, parquet_path, "entsoe", 1, extra)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 1: Extract raw sources to cleaned Parquet files."
    )
    parser.add_argument(
        "--data-root", default="/projects/prjs2061/data",
        help="Root data directory (default: /projects/prjs2061/data).",
    )
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Number of local Spark executor threads (default: 4).",
    )
    return parser.parse_args()


def main() -> None:
    """Run all Phase 1 extraction stages."""
    args = _parse_args()
    data_root = args.data_root
    p1_dir = os.path.join(data_root, "processing_1")

    logger.info("Data root: %s", data_root)
    logger.info("Output:    %s", p1_dir)

    spark = (
        SparkSession.builder
        .appName("NL_Energy_Phase1")
        .master(f"local[{args.workers}]")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        extract_viirs(spark, os.path.join(data_root, "viirs", "A2"),
                       os.path.join(p1_dir, "viirs_a2"))
        extract_cbs_energy(spark, os.path.join(data_root, "cbs"),
                            os.path.join(p1_dir, "cbs_energy"))
        extract_cbs_macro(spark, os.path.join(data_root, "cbs"),
                           os.path.join(p1_dir, "cbs_macro"))
        extract_entsoe(spark, os.path.join(data_root, "entso-e"),
                        os.path.join(p1_dir, "entsoe"))
        logger.info("=== Phase 1 complete. ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
