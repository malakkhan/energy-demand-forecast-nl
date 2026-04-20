#!/usr/bin/env python3
"""Phase 1: Extract raw data sources into cleaned, source-level Parquet files.

Optimised for high-core-count nodes (e.g. Snellius interactive, 96 cores,
251 GB RAM). Uses ``multiprocessing`` for VIIRS HDF5 extraction (bypassing
Spark's JVM overhead for this embarrassingly-parallel I/O workload) and
plain ``pandas`` + ``pyarrow`` for the small CBS and ENTSO-E sources.

Outputs are written to ``data/processing_1/<source>/`` with a
``data_quality.json`` report alongside each.

Usage::

    python phase1_extract.py [--data-root /path/to/data] [--workers N]
                             [--force] [--start-method fork|spawn]
"""

import argparse
import glob
import json
import logging
import multiprocessing as mp
import os
import re
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, date, timedelta
from typing import Optional, Tuple

import h5py
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("phase1_extract")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# VNP46A2 HDF5 structure (Collection 2, v002). All files in the input
# directory are assumed to be from the same MODIS/VIIRS sinusoidal tile
# (h18v03, which covers Western Europe including the Netherlands), so
# lat/lon grids are identical across files and the NL crop indices can be
# computed once and reused.
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

# HDF5 read retry configuration (for transient GPFS errors under heavy load)
_H5_MAX_RETRIES = 3
_H5_RETRY_BACKOFF_S = 1.0

# Parquet row group size — tuned for downstream columnar scans (Spark/Dask).
# One VIIRS day over NL is ~168k rows, so a single row group per file is fine.
_PARQUET_ROW_GROUP_SIZE = 500_000


# ---------------------------------------------------------------------------
# Data quality helpers
# ---------------------------------------------------------------------------


def _compute_parquet_quality(
    parquet_path: str,
    source_name: str,
    phase: int,
    extra_metrics: dict = None,
) -> dict:
    """Compute quality metrics for a small Parquet dataset using pandas.

    Loads the full dataset into memory — do NOT call this on large outputs
    (e.g. the full VIIRS pixel table). For those, build metrics incrementally
    and supply them via ``extra_metrics`` to a manually-constructed metrics
    dict instead.
    """
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
    if os.path.isdir(parquet_path):
        for root, _, files in os.walk(parquet_path):
            for fname in files:
                size_bytes += os.path.getsize(os.path.join(root, fname))
    else:
        size_bytes = os.path.getsize(parquet_path)

    date_range = {}
    for candidate in ["date", "timestamp", "timestamp_utc"]:
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
    logger.info("Quality report → %s", path)


# ---------------------------------------------------------------------------
# VIIRS helpers
# ---------------------------------------------------------------------------


def _parse_viirs_date(filename: str) -> Optional[date]:
    """Parse acquisition date from a VNP46A2 filename (``AYYYYDDD`` format)."""
    match = re.search(r"\.A(\d{4})(\d{3})\.", os.path.basename(filename))
    if not match:
        return None
    year = int(match.group(1))
    doy = int(match.group(2))
    return (datetime(year, 1, 1) + timedelta(days=doy - 1)).date()


def _open_h5_with_retry(filepath: str):
    """Open an HDF5 file with retries for transient GPFS errors.

    Returns an open ``h5py.File`` handle on success, or raises the last
    exception after ``_H5_MAX_RETRIES`` attempts.
    """
    last_exc = None
    for attempt in range(_H5_MAX_RETRIES):
        try:
            return h5py.File(filepath, "r")
        except (OSError, IOError) as exc:
            last_exc = exc
            if attempt < _H5_MAX_RETRIES - 1:
                time.sleep(_H5_RETRY_BACKOFF_S * (2 ** attempt))
    raise last_exc


def _compute_nl_indices(h5_filepath: str) -> tuple:
    """Compute NL bounding-box row/col indices from one HDF5 file.

    All input files share the same h18v03 sinusoidal-tile grid, so these
    indices are identical across the dataset — compute once, reuse for all.
    """
    with _open_h5_with_retry(h5_filepath) as hf:
        grp = hf[_VIIRS_GROUP]
        lat = grp[_VIIRS_LAT_DATASET][:]
        lon = grp[_VIIRS_LON_DATASET][:]

    lat_mask = (lat >= _NL_LAT_MIN) & (lat <= _NL_LAT_MAX)
    lon_mask = (lon >= _NL_LON_MIN) & (lon <= _NL_LON_MAX)

    return (
        np.where(lat_mask)[0],
        np.where(lon_mask)[0],
        lat[lat_mask],
        lon[lon_mask],
    )


# Module-level cache for NL indices. Populated either by fork-inheritance
# (Linux default) or explicitly via _init_worker() under spawn/forkserver.
_NL_CACHE: dict = {}


def _init_worker(cache: dict) -> None:
    """Pool initializer: populate module-level cache in each worker.

    This makes the code portable across fork/spawn/forkserver start methods.
    Under fork, _NL_CACHE is already inherited; this overwrite is harmless.
    """
    global _NL_CACHE
    _NL_CACHE = cache


def _viirs_output_path(out_dir: str, obs_date: date) -> str:
    """Return the per-day Parquet output path."""
    year = obs_date.year
    doy = obs_date.timetuple().tm_yday
    return os.path.join(out_dir, f"year={year}", f"day_{year}{doy:03d}.parquet")


def _process_one_viirs_file(filepath: str) -> dict:
    """Process a single VNP46A2 HDF5 file → write a Parquet partition file.

    Runs in a child process. Reads the HDF5 file, crops to the NL bounding
    box, builds a pandas DataFrame using vectorised numpy, and writes a
    single Parquet file directly (no Spark/JVM involved). Idempotent:
    skips files whose output already exists unless the cache is marked
    ``force=True``.
    """
    lat_idx = _NL_CACHE["lat_idx"]
    lon_idx = _NL_CACHE["lon_idx"]
    lat_vals = _NL_CACHE["lat_vals"]
    lon_vals = _NL_CACHE["lon_vals"]
    out_dir = _NL_CACHE["out_dir"]
    force = _NL_CACHE.get("force", False)

    obs_date = _parse_viirs_date(filepath)
    if obs_date is None:
        return {"file": filepath, "status": "skip", "reason": "no date"}

    out_path = _viirs_output_path(out_dir, obs_date)

    # --- Checkpoint: skip if output already present (unless --force) ---
    if not force and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return {
            "file": os.path.basename(filepath),
            "status": "cached",
            "date": obs_date.isoformat(),
            "rows": 0,  # not known without re-reading; acceptable for the summary
            "size_bytes": os.path.getsize(out_path),
        }

    try:
        with _open_h5_with_retry(filepath) as hf:
            grp = hf[_VIIRS_GROUP]
            r_start, r_end = int(lat_idx[0]), int(lat_idx[-1]) + 1
            c_start, c_end = int(lon_idx[0]), int(lon_idx[-1]) + 1
            ntl_crop = grp[_VIIRS_NTL_DATASET][r_start:r_end, c_start:c_end]
            qf_crop = grp[_VIIRS_QF_DATASET][r_start:r_end, c_start:c_end]
    except Exception as exc:
        return {"file": filepath, "status": "error", "reason": str(exc)}

    # --- Fully vectorised: numpy → pandas DataFrame (zero Python loops) ---
    ri_grid, ci_grid = np.meshgrid(lat_idx, lon_idx, indexing="ij")
    lat_grid, lon_grid = np.meshgrid(lat_vals, lon_vals, indexing="ij")

    ntl_flat = ntl_crop.ravel().astype(np.float32)
    is_fill = np.isclose(ntl_flat, _VIIRS_FILL_VALUE, atol=0.1)

    # Set fill pixels to NaN (becomes null in Parquet)
    radiance = ntl_flat.copy()
    radiance[is_fill] = np.nan

    df = pd.DataFrame({
        "date": np.datetime64(obs_date),
        "row_idx": ri_grid.ravel().astype(np.int32),
        "col_idx": ci_grid.ravel().astype(np.int32),
        "lat": lat_grid.ravel().astype(np.float32),
        "lon": lon_grid.ravel().astype(np.float32),
        "ntl_radiance": radiance,
        "quality_flag": qf_crop.ravel().astype(np.uint8),
        "is_fill": is_fill,
    })

    # Atomic write: write to a tmp file then rename, so a crash mid-write
    # never leaves a partial Parquet that would later be mis-cached as valid.
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tmp_path = out_path + ".tmp"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(
        table, tmp_path,
        compression="snappy",
        row_group_size=_PARQUET_ROW_GROUP_SIZE,
    )
    os.replace(tmp_path, out_path)

    return {
        "file": os.path.basename(filepath),
        "status": "ok",
        "date": obs_date.isoformat(),
        "rows": len(df),
        "size_bytes": os.path.getsize(out_path),
    }


# ---------------------------------------------------------------------------
# Phase 1A: VIIRS A2
# ---------------------------------------------------------------------------


def extract_viirs(viirs_dir: str, out_dir: str, workers: int,
                   force: bool = False) -> None:
    """Extract VIIRS HDF5 files to NL-masked pixel-level Parquet.

    Uses ``multiprocessing`` for true process-level parallelism, bypassing
    PySpark's JVM serialisation overhead. Each worker reads one HDF5 file,
    builds a pandas DataFrame from vectorised numpy arrays, and writes a
    Parquet file directly to the year-partitioned output directory.

    On a 96-core node with GPFS, this achieves near-linear speedup since
    each file is fully independent (embarrassingly parallel). Already-
    processed files are skipped automatically unless ``force=True``.
    """
    logger.info("=== Phase 1: VIIRS A2 extraction ===")
    h5_files = sorted(glob.glob(os.path.join(viirs_dir, "*.h5")))
    if not h5_files:
        logger.warning("No .h5 files found in %s — skipping.", viirs_dir)
        return

    logger.info("Found %d VIIRS HDF5 files. Using %d workers. force=%s",
                len(h5_files), workers, force)

    # Compute NL crop indices once from any file in the directory. All files
    # share the h18v03 sinusoidal tile grid, so the crop is identical.
    lat_idx, lon_idx, lat_vals, lon_vals = _compute_nl_indices(h5_files[0])
    n_pixels = len(lat_idx) * len(lon_idx)
    logger.info(
        "NL crop: %d lat × %d lon = %d pixels/day (rows %d–%d, cols %d–%d).",
        len(lat_idx), len(lon_idx), n_pixels,
        lat_idx[0], lat_idx[-1], lon_idx[0], lon_idx[-1],
    )

    data_dir = os.path.join(out_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    cache = {
        "lat_idx": lat_idx,
        "lon_idx": lon_idx,
        "lat_vals": lat_vals,
        "lon_vals": lon_vals,
        "out_dir": data_dir,
        "force": force,
    }
    # Populate the parent's cache too, for consistency under fork.
    _init_worker(cache)

    t0 = time.time()
    ok_count = 0
    cached_count = 0
    err_count = 0
    total_rows = 0
    total_bytes = 0

    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=_init_worker,
        initargs=(cache,),
    ) as pool:
        futures = {pool.submit(_process_one_viirs_file, f): f for f in h5_files}

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            status = result["status"]
            if status == "ok":
                ok_count += 1
                total_rows += result["rows"]
                total_bytes += result["size_bytes"]
            elif status == "cached":
                cached_count += 1
                total_bytes += result["size_bytes"]
            else:
                err_count += 1
                logger.warning(
                    "  File %s: %s — %s",
                    result.get("file", "?"),
                    status,
                    result.get("reason", ""),
                )

            if i % 200 == 0 or i == len(h5_files):
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(h5_files) - i) / rate if rate > 0 else 0
                logger.info(
                    "  Progress: %d/%d files (%.0f files/s, ETA %.0fs) | "
                    "%d OK, %d cached, %d err | %.1f GB on disk",
                    i, len(h5_files), rate, eta,
                    ok_count, cached_count, err_count, total_bytes / 1e9,
                )

    elapsed = time.time() - t0
    logger.info(
        "VIIRS A2: %d processed + %d cached + %d errors, "
        "%d new pixel rows, %.1f GB total in %.0fs (%.0f files/s).",
        ok_count, cached_count, err_count, total_rows,
        total_bytes / 1e9, elapsed,
        (ok_count + cached_count) / elapsed if elapsed > 0 else 0,
    )

    # Build data quality JSON *incrementally* — never load all pixels.
    extra = {
        "completeness": {
            "total_h5_files": len(h5_files),
            "processed_ok": ok_count,
            "processed_cached": cached_count,
            "processed_error": err_count,
            "pixels_per_day": n_pixels,
            "total_pixel_rows_new": total_rows,
            "nl_lat_range": [float(lat_vals.min()), float(lat_vals.max())],
            "nl_lon_range": [float(lon_vals.min()), float(lon_vals.max())],
        },
        "performance": {
            "elapsed_seconds": round(elapsed, 1),
            "files_per_second": (
                round((ok_count + cached_count) / elapsed, 1)
                if elapsed > 0 else 0
            ),
            "workers": workers,
        },
        "size_bytes": total_bytes,
        "size_mb": round(total_bytes / (1024 * 1024), 1),
    }

    # Column-level metrics from a single sample file (not the whole dataset).
    sample_files = glob.glob(os.path.join(data_dir, "year=*", "*.parquet"))
    if sample_files:
        sample_df = pd.read_parquet(sample_files[0])
        col_metrics = {}
        for c in sample_df.columns:
            nc = int(sample_df[c].isna().sum())
            col_metrics[c] = {
                "dtype": str(sample_df[c].dtype),
                "null_count_in_sample": nc,
                "null_pct_in_sample": round(100.0 * nc / len(sample_df), 2),
            }
        extra["columns_sample"] = col_metrics

    metrics = {
        "source": "viirs_a2",
        "phase": 1,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "row_count": total_rows,
    }
    metrics.update(extra)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 1B: CBS Energy Prices
# ---------------------------------------------------------------------------


def _parse_cbs_period(period_str: str) -> Optional[Tuple[int, int]]:
    """Parse a CBS period string into a (year, month) tuple."""
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


def extract_cbs_energy(cbs_dir: str, out_dir: str) -> None:
    """Extract CBS energy price CSV to monthly Parquet (pandas only)."""
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

    expanded_rows = []
    for _, row in pivot.iterrows():
        m = int(row["month"])
        y = int(row["year"])
        fill_months = [m, m + 1, m + 2] if m in {1, 4, 7, 10} else [m]
        for em in fill_months:
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
    os.makedirs(parquet_path, exist_ok=True)
    expanded.to_parquet(os.path.join(parquet_path, "cbs_energy.parquet"),
                         index=False, engine="pyarrow")
    logger.info("CBS Energy: %d monthly records → %s", len(expanded), parquet_path)

    metrics = _compute_parquet_quality(parquet_path, "cbs_energy", 1)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 1C: CBS Macro (GDP + Population)
# ---------------------------------------------------------------------------


def extract_cbs_macro(cbs_dir: str, out_dir: str) -> None:
    """Extract CBS GDP and population CSVs into annual Parquet (pandas only)."""
    logger.info("=== Phase 1: CBS Macro extraction ===")

    # GDP
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

    # Population
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
    os.makedirs(parquet_path, exist_ok=True)
    merged.to_parquet(os.path.join(parquet_path, "cbs_macro.parquet"),
                       index=False, engine="pyarrow")
    logger.info("CBS Macro: %d annual records → %s", len(merged), parquet_path)

    metrics = _compute_parquet_quality(parquet_path, "cbs_macro", 1)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 1D: ENTSO-E Load
# ---------------------------------------------------------------------------


def _read_entsoe_long(filepath: str) -> pd.DataFrame:
    """Read new-style ENTSO-E XLSX (long format, post-2015)."""
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
    """Read legacy ENTSO-E XLSX (wide format, 2006–2015)."""
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


def _read_one_entsoe_file(fpath: str) -> pd.DataFrame:
    """Dispatch to wide/long reader based on the first sheet's columns."""
    try:
        xl = pd.ExcelFile(fpath)
        probe = xl.parse(xl.sheet_names[0], nrows=5)
        cols = [str(c).strip() for c in probe.columns]
    except Exception as exc:
        logger.warning("  Could not probe %s: %s", os.path.basename(fpath), exc)
        return pd.DataFrame(columns=["timestamp_utc", "entsoe_load_mw"])

    if "Country" in cols and any(re.fullmatch(r"\d+\.0", c) for c in cols):
        df = _read_entsoe_wide(fpath)
    else:
        df = _read_entsoe_long(fpath)
    if not df.empty:
        logger.info("  %s → %d NL rows.", os.path.basename(fpath), len(df))
    return df


def extract_entsoe(entsoe_dir: str, out_dir: str, workers: int = 1) -> None:
    """Extract ENTSO-E XLSX files to deduplicated hourly NL Parquet."""
    logger.info("=== Phase 1: ENTSO-E extraction ===")
    xlsx_files = sorted(glob.glob(os.path.join(entsoe_dir, "*.xlsx")))
    if not xlsx_files:
        logger.warning("No XLSX files found — skipping.")
        return

    # Parse files in parallel — each XLSX is independent. Cap at 8 to avoid
    # thrashing memory (openpyxl is memory-hungry on large workbooks).
    parse_workers = min(workers, 8, len(xlsx_files))
    all_frames = []

    if parse_workers > 1:
        logger.info("Parsing %d XLSX files with %d workers.",
                    len(xlsx_files), parse_workers)
        with ProcessPoolExecutor(max_workers=parse_workers) as pool:
            for df in pool.map(_read_one_entsoe_file, xlsx_files):
                if not df.empty:
                    all_frames.append(df)
    else:
        for fpath in xlsx_files:
            df = _read_one_entsoe_file(fpath)
            if not df.empty:
                all_frames.append(df)

    if not all_frames:
        logger.warning("No NL data found.")
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

    logger.info("ENTSO-E: %d unique hourly NL records.", len(combined))

    os.makedirs(out_dir, exist_ok=True)
    data_dir = os.path.join(out_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    for year, group in combined.groupby("year"):
        year_dir = os.path.join(data_dir, f"year={year}")
        os.makedirs(year_dir, exist_ok=True)
        group.drop(columns=["year"]).to_parquet(
            os.path.join(year_dir, "entsoe.parquet"),
            index=False, engine="pyarrow",
        )

    extra = {
        "completeness": {
            "total_xlsx_files": len(xlsx_files),
            "total_hourly_records": len(combined),
            "year_range": [int(combined["year"].min()), int(combined["year"].max())],
        }
    }
    metrics = _compute_parquet_quality(data_dir, "entsoe", 1, extra)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 1: Extract raw sources to Parquet (multiprocessing)."
    )
    parser.add_argument(
        "--data-root", default="/projects/prjs2061/data",
        help="Root data directory.",
    )
    parser.add_argument(
        "--workers", type=int, default=os.cpu_count() or 4,
        help="Number of parallel workers (default: all available CPUs — "
             "I/O-bound, so oversubscribing is fine).",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-process files whose Parquet output already exists.",
    )
    parser.add_argument(
        "--start-method", choices=["fork", "spawn", "forkserver"],
        default="fork",
        help="multiprocessing start method (default: fork on Linux).",
    )
    return parser.parse_args()


def main() -> None:
    """Run all Phase 1 extraction stages."""
    args = _parse_args()

    # Set start method before any Pool is created. Linux default is fork,
    # which works with _NL_CACHE inheritance; spawn/forkserver are also
    # supported because _init_worker re-populates the cache explicitly.
    try:
        mp.set_start_method(args.start_method, force=True)
    except RuntimeError:
        pass  # already set; harmless

    data_root = args.data_root
    p1_dir = os.path.join(data_root, "processing_1")
    workers = args.workers

    logger.info("Data root     : %s", data_root)
    logger.info("Output        : %s", p1_dir)
    logger.info("Workers       : %d (of %d available CPUs)",
                workers, os.cpu_count() or 0)
    logger.info("Start method  : %s", args.start_method)
    logger.info("Force reprocess: %s", args.force)

    t_total = time.time()

    extract_viirs(
        os.path.join(data_root, "viirs", "A2"),
        os.path.join(p1_dir, "viirs_a2"),
        workers=workers,
        force=args.force,
    )
    extract_cbs_energy(
        os.path.join(data_root, "cbs"),
        os.path.join(p1_dir, "cbs_energy"),
    )
    extract_cbs_macro(
        os.path.join(data_root, "cbs"),
        os.path.join(p1_dir, "cbs_macro"),
    )
    extract_entsoe(
        os.path.join(data_root, "entso-e"),
        os.path.join(p1_dir, "entsoe"),
        workers=workers,
    )

    logger.info(
        "=== Phase 1 complete in %.0fs. ===", time.time() - t_total
    )


if __name__ == "__main__":
    main()