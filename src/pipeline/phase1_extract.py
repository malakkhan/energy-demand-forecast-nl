#!/usr/bin/env python3
"""Phase 1: Extract raw data sources into cleaned, source-level Parquet files.

Optimised for high-core-count nodes (e.g. Snellius interactive, 96 cores,
251 GB RAM). Uses ``multiprocessing`` for VIIRS HDF5 and KNMI NetCDF
extraction (bypassing Spark's JVM overhead for this embarrassingly-parallel
I/O workload) and plain ``pandas`` + ``pyarrow`` for the small CBS and
ENTSO-E sources.

Outputs are written to ``data/processing_1/<source>/`` with a
``data_quality.json`` report alongside each.

Usage::

    python phase1_extract.py [--data-root /path/to/data] [--workers N]
                             [--force] [--start-method fork|spawn]

Supported raw sources:

* VIIRS VNP46A1 / VNP46A2 — satellite nighttime light (HDF5)
* CBS — consumer energy tariffs, GDP, CPI, GEP (CSV)
* ENTSO-E — electricity load (XLSX)
* KNMI — hourly in-situ meteorological observations (NetCDF)
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

# Both VNP46A1 and VNP46A2 (Collection 2, v002) share the same HDF5 group
# and lat/lon structure for the h18v03 sinusoidal tile.
_VIIRS_GROUP = "HDFEOS/GRIDS/VIIRS_Grid_DNB_2d/Data Fields"
_VIIRS_LAT_DATASET = "lat"
_VIIRS_LON_DATASET = "lon"

# Fill value for both products' NTL datasets (float32)
_VIIRS_FILL_VALUE = -999.9

# VNP46A2-specific dataset names
_A2_NTL_DATASET = "Gap_Filled_DNB_BRDF-Corrected_NTL"
_A2_QF_DATASET = "Mandatory_Quality_Flag"   # uint8: 0=best, 1=good, 2+=degraded

# VNP46A1-specific dataset names
_A1_NTL_DATASET = "DNB_At_Sensor_Radiance"
_A1_QF_DATASET = "QF_DNB"   # uint16 bitmask; bits 0-1 encode quality tier

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
    """Process a single VNP46A1 or VNP46A2 HDF5 file → write a Parquet partition.

    Runs in a child process. Reads the HDF5 file, crops to the NL bounding
    box, builds a pandas DataFrame using vectorised numpy, and writes a
    single Parquet file directly (no Spark/JVM involved). Idempotent:
    skips files whose output already exists unless the cache is marked
    ``force=True``.

    Quality flag handling:
    - A2: ``Mandatory_Quality_Flag`` is uint8; stored as-is.
    - A1: ``QF_DNB`` is uint16 bitmask; bits 0-1 encode the quality tier
      (0=best, 1=low, 2=poor, 3=no retrieval) and are extracted into a uint8.
      This makes the output schema identical for both products, and the
      Phase 2 ``quality_flag <= 1`` filter applies correctly to both.
    """
    lat_idx = _NL_CACHE["lat_idx"]
    lon_idx = _NL_CACHE["lon_idx"]
    lat_vals = _NL_CACHE["lat_vals"]
    lon_vals = _NL_CACHE["lon_vals"]
    out_dir = _NL_CACHE["out_dir"]
    force = _NL_CACHE.get("force", False)
    ntl_dataset = _NL_CACHE["ntl_dataset"]
    qf_dataset = _NL_CACHE["qf_dataset"]
    qf_is_bitmask = _NL_CACHE.get("qf_is_bitmask", False)

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
            ntl_crop = grp[ntl_dataset][r_start:r_end, c_start:c_end]
            qf_crop = grp[qf_dataset][r_start:r_end, c_start:c_end]
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

    # For A1, extract bits 0-1 of the uint16 QF_DNB bitmask so the output
    # quality_flag column is uint8 with the same semantics as A2.
    if qf_is_bitmask:
        quality_flag_flat = (qf_crop.ravel() & np.uint16(0x03)).astype(np.uint8)
    else:
        quality_flag_flat = qf_crop.ravel().astype(np.uint8)

    df = pd.DataFrame({
        "date": np.datetime64(obs_date),
        "row_idx": ri_grid.ravel().astype(np.int32),
        "col_idx": ci_grid.ravel().astype(np.int32),
        "lat": lat_grid.ravel().astype(np.float32),
        "lon": lon_grid.ravel().astype(np.float32),
        "ntl_radiance": radiance,
        "quality_flag": quality_flag_flat,
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
# Phase 1A: VIIRS A1 / A2
# ---------------------------------------------------------------------------


def extract_viirs(viirs_dir: str, out_dir: str, workers: int,
                  force: bool = False, product: str = "a2") -> None:
    """Extract VIIRS HDF5 files to NL-masked pixel-level Parquet.

    Supports both VNP46A1 (``product="a1"``) and VNP46A2 (``product="a2"``).
    The two products share the same HDF5 group and lat/lon structure, but
    differ in their NTL and quality-flag dataset names.

    Uses ``multiprocessing`` for true process-level parallelism, bypassing
    PySpark's JVM serialisation overhead. Each worker reads one HDF5 file,
    builds a pandas DataFrame from vectorised numpy arrays, and writes a
    Parquet file directly to the year-partitioned output directory.

    On a 96-core node with GPFS, this achieves near-linear speedup since
    each file is fully independent (embarrassingly parallel). Already-
    processed files are skipped automatically unless ``force=True``.
    """
    product = product.lower()
    if product == "a2":
        ntl_dataset = _A2_NTL_DATASET
        qf_dataset = _A2_QF_DATASET
        qf_is_bitmask = False
    elif product == "a1":
        ntl_dataset = _A1_NTL_DATASET
        qf_dataset = _A1_QF_DATASET
        qf_is_bitmask = True
    else:
        raise ValueError(f"Unknown VIIRS product: {product!r}. Expected 'a1' or 'a2'.")

    logger.info("=== Phase 1: VIIRS %s extraction ===", product.upper())
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
        "ntl_dataset": ntl_dataset,
        "qf_dataset": qf_dataset,
        "qf_is_bitmask": qf_is_bitmask,
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
        "VIIRS %s: %d processed + %d cached + %d errors, "
        "%d new pixel rows, %.1f GB total in %.0fs (%.0f files/s).",
        product.upper(), ok_count, cached_count, err_count, total_rows,
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
        "source": f"viirs_{product}",
        "phase": 1,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "row_count": total_rows,
    }
    metrics.update(extra)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 1B: CBS Consumer Energy Tariffs (harmonized old + new schema)
# ---------------------------------------------------------------------------

# Column names for the two CBS consumer‑tariff CSV schemas.  The order must
# match the semicolon‑delimited columns *after* skipping the 6‑line header.
_CBS_TARIFF_OLD_COLS = [
    "period_raw",
    "gas_transport_rate", "gas_fixed_supply_rate", "gas_variable_delivery",
    "gas_ode_tax", "gas_energy_tax",
    "elec_transport_rate", "elec_fixed_supply_rate", "elec_variable_delivery",
    "elec_ode_tax", "elec_energy_tax", "elec_energy_tax_refund",
]
_CBS_TARIFF_NEW_COLS = [
    "period_raw",
    "gas_transport_rate", "gas_fixed_supply_rate", "gas_variable_contract",
    "gas_energy_tax",
    "elec_transport_rate", "elec_fixed_supply_rate", "elec_variable_contract",
    "elec_fixed_supply_rate_dynamic", "elec_variable_supply_rate_dynamic",
    "elec_energy_tax", "elec_energy_tax_refund",
]

_ENGLISH_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june",
     "july", "august", "september", "october", "november", "december"],
    start=1,
)}

_DUTCH_MONTHS = {m: i for i, m in enumerate(
    ["januari", "februari", "maart", "april", "mei", "juni",
     "juli", "augustus", "september", "oktober", "november", "december"],
    start=1,
)}


def _read_cbs_tariff(path: str, colnames: list) -> pd.DataFrame:
    """Read a CBS semicolon export with 6 header lines and a footer."""
    df = pd.read_csv(
        path, sep=";", skiprows=6, header=None, names=colnames,
        encoding="utf-8-sig", na_values=["", ".", " "],
        skipfooter=1, engine="python", dtype=str,
    )
    df["period_raw"] = df["period_raw"].str.strip()
    for c in df.columns:
        if c != "period_raw":
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _parse_tariff_period(raw: str) -> Optional[Tuple[int, Optional[int]]]:
    """Parse ``'2022 March'``, ``'2026 March*'``, or ``'2025'``.

    Returns ``(year, month)`` for monthly rows, ``(year, None)`` for
    annual rows, or ``None`` if the label can't be parsed.
    """
    s = str(raw).strip().rstrip("*").strip()
    m = re.fullmatch(r"(\d{4})(?:\s+(\w+))?", s)
    if not m:
        return None
    year = int(m.group(1))
    month_name = m.group(2)
    if month_name is None:
        return (year, None)
    month = _ENGLISH_MONTHS.get(month_name.lower())
    return (year, month) if month else None


def extract_cbs_energy(cbs_dir: str, out_dir: str) -> None:
    """Harmonize CBS consumer energy tariff tables into monthly Parquet.

    Reads two CBS CSVs — the *old schema* (2018–2023) and the *new schema*
    (2021+) — and splices them at the 2020/2021 boundary:

    * Old file contributes 2018‑01 through 2020‑12.
    * New file contributes 2021‑01 onward.

    Design decisions (per the user's analysis):

    * **Variable supply rates dropped** — the methodology break between
      old 'delivery rate' and new 'contract prices' makes them
      non‑comparable, so they are excluded from the output.
    * **ODE levy + energy tax → total_tax** — summed for ≤2022; from 2023
      CBS merged ODE into the energy‑tax line, so ``energy_tax`` alone is
      used.
    * **Dynamic‑contract columns** from the new schema are kept; ``NULL``
      before 2025‑01 (the product category wasn't separately tracked).
    * **Annual rows dropped** — downstream joins on ``(year, month)`` so
      only monthly rows are written.
    """
    logger.info("=== Phase 1: CBS Consumer Energy Tariffs extraction ===")

    # ---- locate files (glob for date‑stamped suffixes) ----
    old_matches = glob.glob(os.path.join(cbs_dir, "Average_energy_prices_for_consumers__2018*"))
    new_matches = glob.glob(os.path.join(cbs_dir, "Average_energy_prices_for_consumers_2*"))
    # new_matches may also catch the old file; filter it out
    new_matches = [p for p in new_matches if "2018" not in os.path.basename(p)]

    if not old_matches and not new_matches:
        logger.warning("No CBS consumer tariff files found — skipping.")
        return

    frames = []

    # ---- old schema: 2018–2020 ----
    if old_matches:
        old_path = old_matches[0]
        logger.info("Reading OLD tariff schema: %s", old_path)
        old = _read_cbs_tariff(old_path, _CBS_TARIFF_OLD_COLS)
        # parse periods
        parsed = old["period_raw"].apply(_parse_tariff_period)
        old["year"] = parsed.apply(lambda x: x[0] if x else None)
        old["month"] = parsed.apply(lambda x: x[1] if x else None)
        old = old.dropna(subset=["year"])
        old["year"] = old["year"].astype(int)
        # Keep the full old DataFrame for ODE tax lookup (2021/2022 ODE
        # values are in the old file but not in the new schema).
        old_full = old.copy()
        # For the data contribution, only use ≤2020 from old file
        old = old[old["year"] <= 2020].copy()
        frames.append(old)
        logger.info("  Old schema: %d rows (≤2020).", len(old))
    else:
        logger.warning("Old tariff file not found.")
        old_full = pd.DataFrame()

    # ---- new schema: 2021+ ----
    if new_matches:
        new_path = new_matches[0]
        logger.info("Reading NEW tariff schema: %s", new_path)
        new = _read_cbs_tariff(new_path, _CBS_TARIFF_NEW_COLS)
        parsed = new["period_raw"].apply(_parse_tariff_period)
        new["year"] = parsed.apply(lambda x: x[0] if x else None)
        new["month"] = parsed.apply(lambda x: x[1] if x else None)
        new = new.dropna(subset=["year"])
        new["year"] = new["year"].astype(int)
        # Keep only ≥2021 from new file
        new = new[new["year"] >= 2021].copy()

        # Merge ODE tax from old file into 2021–2022 rows of new file
        # (ODE was abolished in 2023, so only 2021/2022 need it).
        if not old_full.empty:
            old_monthly = old_full[old_full["month"].notna()].copy()
            old_monthly["month"] = old_monthly["month"].astype(int)
            ode_cols = old_monthly[["year", "month", "gas_ode_tax", "elec_ode_tax"]].copy()
            new = new.merge(ode_cols, on=["year", "month"], how="left",
                            suffixes=("", "_old"))
        else:
            new["gas_ode_tax"] = pd.NA
            new["elec_ode_tax"] = pd.NA

        frames.append(new)
        logger.info("  New schema: %d rows (≥2021).", len(new))
    else:
        logger.warning("New tariff file not found.")

    if not frames:
        logger.warning("No tariff data parsed — skipping.")
        return

    combined = pd.concat(frames, ignore_index=True)

    # ---- Drop annual rows (month is None) — we only need monthly ----
    combined = combined[combined["month"].notna()].copy()
    combined["month"] = combined["month"].astype(int)

    # ---- Compute continuous total‑tax columns ----
    #   ≤2022: ODE + energy_tax   |   ≥2023: energy_tax alone (ODE merged)
    def _total_tax(row: pd.Series, ode_col: str, tax_col: str):
        tax = row[tax_col]
        if pd.isna(tax):
            return pd.NA
        if row["year"] <= 2022:
            ode = row.get(ode_col)
            if pd.isna(ode):
                return pd.NA
            return ode + tax
        return tax

    combined["gas_total_tax"] = combined.apply(
        lambda r: _total_tax(r, "gas_ode_tax", "gas_energy_tax"), axis=1,
    )
    combined["elec_total_tax"] = combined.apply(
        lambda r: _total_tax(r, "elec_ode_tax", "elec_energy_tax"), axis=1,
    )

    # ---- Select and rename output columns (cbs_ prefix) ----
    output_col_map = {
        # Gas
        "gas_transport_rate":                "cbs_gas_transport_rate",
        "gas_fixed_supply_rate":             "cbs_gas_fixed_supply_rate",
        "gas_ode_tax":                       "cbs_gas_ode_tax",
        "gas_energy_tax":                    "cbs_gas_energy_tax",
        "gas_total_tax":                     "cbs_gas_total_tax",
        # Electricity
        "elec_transport_rate":               "cbs_elec_transport_rate",
        "elec_fixed_supply_rate":            "cbs_elec_fixed_supply_rate",
        "elec_fixed_supply_rate_dynamic":    "cbs_elec_fixed_supply_rate_dynamic",
        "elec_variable_supply_rate_dynamic": "cbs_elec_variable_supply_rate_dynamic",
        "elec_ode_tax":                      "cbs_elec_ode_tax",
        "elec_energy_tax":                   "cbs_elec_energy_tax",
        "elec_total_tax":                    "cbs_elec_total_tax",
        "elec_energy_tax_refund":            "cbs_elec_energy_tax_refund",
    }

    # Ensure all source columns exist (some may be absent depending on schema)
    for src in output_col_map:
        if src not in combined.columns:
            combined[src] = pd.NA

    combined = combined.rename(columns=output_col_map)
    out_cols = ["year", "month"] + list(output_col_map.values())
    result = combined[out_cols].copy()

    result = result.sort_values(["year", "month"]).reset_index(drop=True)
    result["year"] = result["year"].astype(int)
    result["month"] = result["month"].astype(int)

    logger.info(
        "CBS Energy Tariffs: %d monthly rows, %d columns (years %d–%d).",
        len(result),
        len([c for c in result.columns if c.startswith("cbs_")]),
        int(result["year"].min()),
        int(result["year"].max()),
    )

    # ---- Write Parquet ----
    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, "data")
    os.makedirs(parquet_path, exist_ok=True)
    result.to_parquet(
        os.path.join(parquet_path, "cbs_energy.parquet"),
        index=False, engine="pyarrow",
    )
    logger.info("CBS Energy Tariffs written → %s", parquet_path)

    metrics = _compute_parquet_quality(parquet_path, "cbs_energy", 1)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 1C: CBS GDP / Quarterly National Accounts + Population
# ---------------------------------------------------------------------------

# Mapping from CBS topic strings (pipe-separated hierarchy) to clean
# snake_case column base names.  Each gets a ``_yy`` and ``_qq`` suffix
# for year-over-year and quarter-over-quarter volume changes respectively.
_CBS_GDP_TOPIC_MAP: list[tuple[str, str]] = [
    ("Disposable for final expenditure|Total", "disposable_total"),
    ("Disposable for final expenditure|Gross domestic product", "gdp"),
    ("Disposable for final expenditure|GDP, working days adjusted", "gdp_wda"),
    ("Disposable for final expenditure|Imports of goods and services|Total", "imports_total"),
    ("Disposable for final expenditure|Imports of goods and services|Imports of goods", "imports_goods"),
    ("Disposable for final expenditure|Imports of goods and services|Imports of services", "imports_services"),
    ("Final expenditure|Total", "final_exp_total"),
    ("Final expenditure|National final expenditure|Total", "natl_final_exp"),
    ("Final expenditure|National final expenditure|Final consumption expenditure|Total", "consumption_total"),
    ("Final expenditure|National final expenditure|Final consumption expenditure|Households including NPISHs", "consumption_hh"),
    ("Final expenditure|National final expenditure|Final consumption expenditure|General government", "consumption_gov"),
    ("Final expenditure|National final expenditure|Gross fixed capital formation|Total", "capform_total"),
    ("Final expenditure|National final expenditure|Gross fixed capital formation|Enterprises and households", "capform_enterprise"),
    ("Final expenditure|National final expenditure|Gross fixed capital formation|General government", "capform_gov"),
    ("Final expenditure|National final expenditure|Changes in inventories incl. valuables", "inventories"),
    ("Final expenditure|Exports of goods and services|Total", "exports_total"),
    ("Final expenditure|Exports of goods and services|Exports of goods", "exports_goods"),
    ("Final expenditure|Exports of goods and services|Exports of services", "exports_services"),
]

# Quarter number → first month of that quarter (for forward-fill)
_QUARTER_TO_MONTHS = {1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9], 4: [10, 11, 12]}


def _match_topic(topic_str: str) -> Optional[str]:
    """Match a raw CBS topic string to its clean column base name."""
    for suffix, col_name in _CBS_GDP_TOPIC_MAP:
        if topic_str.strip().endswith(suffix):
            return col_name
    return None


def _parse_cbs_quarter_period(period_str: str) -> Optional[Tuple[int, int]]:
    """Parse a CBS period string into ``(year, quarter)`` or ``None``.

    Accepts formats like ``"2015 1st quarter"``, ``"2023 3rd quarter*"``.
    Annual-only periods (``"2015"``, ``"2024*"``) return ``None`` because
    the quarterly values already cover the full year.
    """
    s = str(period_str).strip().replace("*", "")
    m = re.match(r"(\d{4})\s+(\d)(?:st|nd|rd|th)\s+quarter", s, re.I)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return None


def extract_cbs_gdp(cbs_dir: str, out_dir: str) -> None:
    """Extract CBS Quarterly National Accounts + Population into monthly Parquet.

    Reads the wide-format CBS GDP CSV containing 18 economic indicators at
    quarterly resolution, with two metric types each (year-over-year volume
    change and quarter-over-quarter volume change).  Quarterly values are
    forward-filled to monthly granularity (Q1 → Jan/Feb/Mar, etc.) so they
    align with the monthly CBS energy prices in Phase 2.

    Population data from a separate CSV is merged as an annual column,
    forward-filled across all 12 months of each year.
    """
    logger.info("=== Phase 1: CBS GDP / Quarterly National Accounts extraction ===")

    # --- Locate the GDP file (glob for the varying date suffix) ---
    gdp_pattern = os.path.join(cbs_dir, "GDP__output_and_expenditures__changes__*")
    gdp_matches = glob.glob(gdp_pattern)
    if not gdp_matches:
        logger.warning("No CBS GDP file found matching %s — skipping.", gdp_pattern)
        return

    gdp_path = gdp_matches[0]
    logger.info("Reading CBS GDP: %s", gdp_path)

    # The CSV has 4 header rows above the data header.  Row index 4
    # (0-based) contains "Topic ; Periods ; <period1> ; <period2> ; ..."
    raw = pd.read_csv(
        gdp_path, sep=";", header=4, encoding="utf-8",
        quotechar='"', dtype=str,
    )
    # Drop the footer row ("Source: CBS")
    raw = raw[~raw.iloc[:, 0].astype(str).str.startswith("Source", na=True)]

    topic_col = raw.columns[0]   # "Topic"
    # unit_col  = raw.columns[1]   # "Periods" (actually contains the unit)
    period_cols = raw.columns[2:].tolist()

    # The period columns are split into two halves of equal size:
    # first half = year-over-year (y/y), second half = quarter-over-quarter (q/q).
    n_periods = len(period_cols) // 2
    yy_period_cols = period_cols[:n_periods]
    qq_period_cols = period_cols[n_periods:]

    # Build a clean period label list from the y/y half (both halves share
    # the same period labels, pandas just suffixes the duplicates with ".1").
    yy_labels = [str(c).split(".")[0].strip() for c in yy_period_cols]

    # --- Parse each indicator row × each metric type → long format ---
    records: list[dict] = []

    for _, row in raw.iterrows():
        topic_raw = str(row[topic_col]).strip()
        col_base = _match_topic(topic_raw)
        if col_base is None:
            continue

        for metric_suffix, cols in [("_yy", yy_period_cols), ("_qq", qq_period_cols)]:
            col_name = f"cbs_{col_base}{metric_suffix}"
            for pcol, label in zip(cols, yy_labels):
                parsed = _parse_cbs_quarter_period(label)
                if parsed is None:
                    continue  # skip annual-total columns
                year, quarter = parsed
                val = pd.to_numeric(
                    str(row[pcol]).strip().replace(",", "."),
                    errors="coerce",
                )
                # Forward-fill: each quarterly value fills 3 months.
                for month in _QUARTER_TO_MONTHS[quarter]:
                    records.append({
                        "year": year,
                        "month": month,
                        "column": col_name,
                        "value": val,
                    })

    if not records:
        logger.warning("No quarterly records parsed from CBS GDP file.")
        return

    long_df = pd.DataFrame(records)

    # Pivot: one row per (year, month), one column per indicator.
    pivot = long_df.pivot_table(
        index=["year", "month"], columns="column",
        values="value", aggfunc="first",
    ).reset_index()
    pivot.columns.name = None
    pivot = pivot.sort_values(["year", "month"]).reset_index(drop=True)

    logger.info(
        "CBS GDP: %d monthly rows × %d indicator columns (years %d–%d).",
        len(pivot),
        len([c for c in pivot.columns if c.startswith("cbs_")]),
        int(pivot["year"].min()),
        int(pivot["year"].max()),
    )

    # --- Population (from separate CSV, annual → forward-filled monthly) ---
    pop_path = os.path.join(cbs_dir, "Population (x million).csv")
    if os.path.exists(pop_path):
        raw_pop = pd.read_csv(pop_path, sep=";", quotechar='"', dtype=str)
        raw_pop.columns = [c.strip() for c in raw_pop.columns]
        year_col = [c for c in raw_pop.columns if "year" in c.lower() or "jaar" in c.lower()]
        pop_col = [c for c in raw_pop.columns if "pop" in c.lower() or "bev" in c.lower()]
        if year_col and pop_col:
            raw_pop["year"] = pd.to_numeric(raw_pop[year_col[0]], errors="coerce")
            raw_pop["cbs_population_million"] = pd.to_numeric(
                raw_pop[pop_col[0]].str.replace(",", ".", regex=False), errors="coerce",
            )
        else:
            raw_pop["year"] = pd.to_numeric(raw_pop.iloc[:, 0], errors="coerce")
            raw_pop["cbs_population_million"] = pd.to_numeric(
                raw_pop.iloc[:, 1].astype(str).str.replace(",", ".", regex=False),
                errors="coerce",
            )
        pop_df = raw_pop[["year", "cbs_population_million"]].dropna(subset=["year"])
        pop_df["year"] = pop_df["year"].astype(int)

        # Merge annual population onto the monthly GDP table.
        pivot = pd.merge(pivot, pop_df, on="year", how="left")
        logger.info("Population merged (%d annual records).", len(pop_df))
    else:
        logger.warning("Population file not found: %s", pop_path)

    # --- Write output ---
    pivot["year"] = pivot["year"].astype(int)
    pivot["month"] = pivot["month"].astype(int)

    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, "data")
    os.makedirs(parquet_path, exist_ok=True)
    pivot.to_parquet(
        os.path.join(parquet_path, "cbs_gdp.parquet"),
        index=False, engine="pyarrow",
    )
    logger.info("CBS GDP: %d monthly records → %s", len(pivot), parquet_path)

    metrics = _compute_parquet_quality(parquet_path, "cbs_gdp", 1)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 1D: CBS Consumer Price Index (CPI) — Energy
# ---------------------------------------------------------------------------


def _parse_cpi_period(raw: str) -> Optional[Tuple[int, int]]:
    """Parse a Dutch CBS period string into ``(year, month)`` or ``None``.

    Accepts monthly strings like ``"1996 januari"`` or ``"2025 december*"``.
    Annual-only strings like ``"1996"`` return ``None`` and are dropped — the
    CPI file includes one annual-average row per year which duplicates the
    monthly data; we only need the monthly granularity.
    """
    s = str(raw).strip().rstrip("*").strip()
    parts = s.split()
    if len(parts) != 2:
        return None
    try:
        year = int(parts[0])
    except ValueError:
        return None
    month = _DUTCH_MONTHS.get(parts[1].lower())
    return (year, month) if month else None


def extract_cbs_cpi(cbs_dir: str, out_dir: str) -> None:
    """Extract CBS Consumer Price Index (CPI) for energy into monthly Parquet.

    Reads the CBS ``Consumentenprijzen; prijsindex 2015=100`` file, which
    contains three monthly CPI series for the Netherlands:

    * **Energy (045000)** — overall energy basket (electricity + gas,
      weighted by household spending shares).
    * **Electricity (045100)** — electricity component.
    * **Gas (045200)** — gas component.

    All three are dimensionless index values normalised to 2015 = 100.
    A value of 162 in 2024 therefore means energy cost 62 % more in 2024
    than it did in 2015.

    The file uses Dutch month names and comma decimal separators.  Annual
    summary rows (one per year) are filtered out; only the 12 monthly rows
    per year are retained.

    Coverage: January 1996 – December 2025 (~360 monthly rows).  This
    extends the energy price signal back to 1996, filling the 2012–2017
    gap left by the tariff files which only start in 2018.
    """
    logger.info("=== Phase 1: CBS Consumer Price Index extraction ===")

    cpi_matches = glob.glob(
        os.path.join(cbs_dir, "Consumentenprijzen__prijsindex_2015_100__*")
    )
    if not cpi_matches:
        logger.warning("No CBS CPI file found in %s — skipping.", cbs_dir)
        return

    path = cpi_matches[0]
    logger.info("Reading CBS CPI: %s", path)

    # 6-line header: title · 2 blank · sub-header · category names · units
    # Column order after skipping: period_raw | energy | electricity | gas
    df = pd.read_csv(
        path, sep=";", skiprows=6, header=None,
        names=["period_raw", "cbs_cpi_energy", "cbs_cpi_electricity", "cbs_cpi_gas"],
        encoding="utf-8-sig", skipfooter=1, engine="python", dtype=str,
    )

    df["period_raw"] = df["period_raw"].str.strip()
    parsed = df["period_raw"].apply(_parse_cpi_period)
    df["year"] = parsed.apply(lambda x: x[0] if x else None)
    df["month"] = parsed.apply(lambda x: x[1] if x else None)

    # Keep only monthly rows (annual summaries have month=None)
    df = df.dropna(subset=["year", "month"]).copy()
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    # CBS uses Dutch decimal comma ("42,06" → 42.06)
    for col in ["cbs_cpi_energy", "cbs_cpi_electricity", "cbs_cpi_gas"]:
        df[col] = (
            df[col].str.strip()
                   .str.replace(",", ".", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    result = (
        df[["year", "month", "cbs_cpi_energy", "cbs_cpi_electricity", "cbs_cpi_gas"]]
        .sort_values(["year", "month"])
        .reset_index(drop=True)
    )

    logger.info(
        "CBS CPI: %d monthly rows, years %d–%d.",
        len(result), int(result["year"].min()), int(result["year"].max()),
    )

    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, "data")
    os.makedirs(parquet_path, exist_ok=True)
    result.to_parquet(
        os.path.join(parquet_path, "cbs_cpi.parquet"),
        index=False, engine="pyarrow",
    )
    logger.info("CBS CPI written → %s", parquet_path)

    metrics = _compute_parquet_quality(parquet_path, "cbs_cpi", 1)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 1E: CBS Gas & Electricity Prices (semi-annual GEP)
# ---------------------------------------------------------------------------

# Maps the raw CBS component label (lowercase) to the short tag used in column names.
_GEP_COMPONENT_MAP = {
    "total price":   "total",
    "supply price":  "supply",
    "network price": "network",
}

# Maps semester number to the calendar months it covers.
_SEMESTER_TO_MONTHS = {1: [1, 2, 3, 4, 5, 6], 2: [7, 8, 9, 10, 11, 12]}

# Ordered list of value-column short names matching the CSV column order.
_GEP_VALUE_COLS = [
    "gas_hh", "gas_nnh_med", "gas_nnh_lrg",
    "elec_hh", "elec_nnh_med", "elec_nnh_lrg",
]


def _parse_gep_period(raw: str) -> Optional[Tuple[int, int]]:
    """Parse a CBS GEP period string into ``(year, semester)`` or ``None``.

    Accepts ``"2009 1st semester"`` or ``"2025 2nd semester*"``.
    Annual-only rows like ``"2009"`` return ``None`` and are dropped.
    """
    s = str(raw).strip().rstrip("*").strip()
    m = re.match(r"^(\d{4})\s+(\d)(?:st|nd|rd|th)\s+semester", s, re.I)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return None


def extract_cbs_gep(cbs_dir: str, out_dir: str) -> None:
    """Extract CBS Gas & Electricity Prices (semi-annual) into monthly Parquet.

    Reads the CBS ``Prices of natural gas and electricity`` file, which
    contains semi-annual prices for six consumption-band segments across
    three price components:

    * **Total price** — combined supply + network tariff (incl. VAT/taxes).
    * **Supply price** — commodity and supply charges only.
    * **Network price** — distribution/transport tariff only.

    Six consumption bands:

    * ``gas_hh``       — gas household (569–5 687 m³/yr)
    * ``gas_nnh_med``  — gas non-household medium (28 433–284 333 m³/yr)
    * ``gas_nnh_lrg``  — gas non-household large (≥28 433 324 m³/yr)
    * ``elec_hh``      — electricity household (2.5–5 MWh/yr)
    * ``elec_nnh_med`` — electricity non-household medium (500–2 000 MWh/yr)
    * ``elec_nnh_lrg`` — electricity non-household large (≥150 000 MWh/yr)

    The file is semi-annual (H1 = Jan–Jun, H2 = Jul–Dec).  Each semester is
    expanded to six monthly rows so the output aligns with all other CBS
    monthly sources.  Annual-average rows (one per year) are dropped.

    Produces 18 ``cbs_gep_{band}_{component}`` columns.
    Coverage: H1 2009 – H2 2025 (~204 monthly rows).
    """
    logger.info("=== Phase 1: CBS Gas & Electricity Prices (GEP) extraction ===")

    gep_matches = glob.glob(
        os.path.join(cbs_dir, "Prices_of_natural_gas_and_electricity_*")
    )
    if not gep_matches:
        logger.warning("No CBS GEP file found in %s — skipping.", cbs_dir)
        return

    path = gep_matches[0]
    logger.info("Reading CBS GEP: %s", path)

    # 5-line header: title · VAT note · blank topic row · band labels · units
    colnames = ["period_raw", "component"] + _GEP_VALUE_COLS
    df = pd.read_csv(
        path, sep=";", skiprows=5, header=None, names=colnames,
        encoding="utf-8-sig", skipfooter=1, engine="python", dtype=str,
    )

    df["period_raw"] = df["period_raw"].str.strip()
    df["component"] = df["component"].str.strip()

    # Parse semi-annual periods; drop annual rows (None)
    parsed = df["period_raw"].apply(_parse_gep_period)
    df["year"] = parsed.apply(lambda x: x[0] if x else None)
    df["semester"] = parsed.apply(lambda x: x[1] if x else None)
    df = df.dropna(subset=["year", "semester"]).copy()
    df["year"] = df["year"].astype(int)
    df["semester"] = df["semester"].astype(int)

    # Normalise component label → "total" / "supply" / "network"
    df["component"] = df["component"].str.lower().map(_GEP_COMPONENT_MAP)
    df = df.dropna(subset=["component"]).copy()

    # Convert value columns to float (decimal point, strip trailing asterisks)
    for col in _GEP_VALUE_COLS:
        df[col] = (
            df[col].str.strip().str.rstrip("*")
            .pipe(pd.to_numeric, errors="coerce")
        )

    # Melt bands into long format then construct the final column name
    long = df.melt(
        id_vars=["year", "semester", "component"],
        value_vars=_GEP_VALUE_COLS,
        var_name="band",
        value_name="price",
    )
    long["col_name"] = "cbs_gep_" + long["band"] + "_" + long["component"]

    # Pivot: one row per (year, semester), one column per cbs_gep_*
    pivot = long.pivot_table(
        index=["year", "semester"],
        columns="col_name",
        values="price",
        aggfunc="first",
    ).reset_index()
    pivot.columns.name = None

    # Expand each semester to six monthly rows
    gep_cols_found = sorted([c for c in pivot.columns if c.startswith("cbs_gep_")])
    records = []
    for _, row in pivot.iterrows():
        for month in _SEMESTER_TO_MONTHS[int(row["semester"])]:
            rec = {"year": int(row["year"]), "month": month}
            for col in gep_cols_found:
                rec[col] = row[col]
            records.append(rec)

    result = (
        pd.DataFrame(records)
        .sort_values(["year", "month"])
        .reset_index(drop=True)
    )
    result = result[["year", "month"] + gep_cols_found]

    logger.info(
        "CBS GEP: %d monthly rows, %d price columns (years %d–%d).",
        len(result), len(gep_cols_found),
        int(result["year"].min()), int(result["year"].max()),
    )

    os.makedirs(out_dir, exist_ok=True)
    parquet_path = os.path.join(out_dir, "data")
    os.makedirs(parquet_path, exist_ok=True)
    result.to_parquet(
        os.path.join(parquet_path, "cbs_gep.parquet"),
        index=False, engine="pyarrow",
    )
    logger.info("CBS GEP written → %s", parquet_path)

    metrics = _compute_parquet_quality(parquet_path, "cbs_gep", 1)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 1F: ENTSO-E Load
# ---------------------------------------------------------------------------


def _read_entsoe_long(filepath: str) -> pd.DataFrame:
    """Read new-style ENTSO-E XLSX (long format, post-2015)."""
    xl = pd.ExcelFile(filepath)
    frames = []
    for sheet in xl.sheet_names:
        try:
            df = xl.parse(sheet)
        except Exception as exc:
            logger.warning("  Could not parse sheet %r in %s: %s",
                           sheet, os.path.basename(filepath), exc)
            continue
        if "CountryCode" not in df.columns or "DateUTC" not in df.columns:
            continue
        nl = df[df["CountryCode"].astype(str).str.strip() == _NL_COUNTRY_CODE].copy()
        if nl.empty:
            continue
        nl["timestamp_utc"] = pd.to_datetime(nl["DateUTC"], utc=True, errors="coerce")
        if "Value" in nl.columns and nl["Value"].notna().any():
            val_col = "Value"
        elif "Value_ScaleTo100" in nl.columns:
            val_col = "Value_ScaleTo100"
        else:
            logger.warning("  No value column found in sheet %r of %s — skipping sheet.",
                           sheet, os.path.basename(filepath))
            continue
        nl["entsoe_load_mw"] = pd.to_numeric(nl[val_col], errors="coerce")
        frames.append(nl[["timestamp_utc", "entsoe_load_mw"]])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["timestamp_utc", "entsoe_load_mw"]
    )


def _read_entsoe_wide(filepath: str, header_row: int = 3) -> pd.DataFrame:
    """Read legacy ENTSO-E XLSX (wide format, 2006–2015)."""
    xl = pd.ExcelFile(filepath)
    frames = []
    for sheet in xl.sheet_names:
        try:
            raw = xl.parse(sheet, header=header_row)
        except Exception as exc:
            logger.warning("  Could not parse sheet %r in %s: %s",
                           sheet, os.path.basename(filepath), exc)
            continue
        raw.columns = [str(c).strip() for c in raw.columns]
        if "Country" not in raw.columns:
            continue
        nl = raw[raw["Country"].astype(str).str.strip() == _NL_COUNTRY_CODE].copy()
        if nl.empty:
            continue
        hour_cols = [c for c in nl.columns if re.fullmatch(r"\d+(?:\.0)?", c)]
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
    """Dispatch to wide/long reader based on the first sheet's columns.

    The legacy 2006–2015 file has 3 metadata header rows before the real
    column names, so the probe tries multiple header offsets to find the
    ``Country`` column that identifies the wide format.
    """
    try:
        xl = pd.ExcelFile(fpath)
        sheet = xl.sheet_names[0]
        # Try header offsets 0–4 to detect the wide-format signature.
        is_wide = False
        for h in range(5):
            probe = xl.parse(sheet, header=h, nrows=2)
            cols = [str(c).strip() for c in probe.columns]
            if "Country" in cols and any(re.fullmatch(r"\d+(?:\.0)?", c) for c in cols):
                is_wide = True
                break
    except Exception as exc:
        logger.warning("  Could not probe %s: %s", os.path.basename(fpath), exc)
        return pd.DataFrame(columns=["timestamp_utc", "entsoe_load_mw"])

    if is_wide:
        df = _read_entsoe_wide(fpath, header_row=h)
    else:
        df = _read_entsoe_long(fpath)
    if not df.empty:
        logger.info("  %s → %d NL rows.", os.path.basename(fpath), len(df))
    else:
        logger.warning("  %s returned 0 NL rows.", os.path.basename(fpath))
    return df


def extract_entsoe(entsoe_dir: str, out_dir: str, workers: int = 1) -> None:
    """Extract ENTSO-E XLSX files to deduplicated hourly NL Parquet."""
    logger.info("=== Phase 1F: ENTSO-E extraction ===")
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
        tbl = pa.Table.from_pandas(
            group.drop(columns=["year"]), preserve_index=False
        )
        pq.write_table(
            tbl, os.path.join(year_dir, "entsoe.parquet"),
            compression="snappy",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
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
# Phase 1G: KNMI Hourly Meteorological Observations
# ---------------------------------------------------------------------------

# KNMI NetCDF files are HDF5-based, so h5py reads them natively. Each file
# contains one hour of observations from 61 stations.  We average across
# stations to produce national-mean hourly scalars.

# Variables to extract and their output column suffixes (after the prefix).
# Only the 8 most energy-relevant fields are selected (temperature, wind,
# solar, humidity). The full column name is ``{col_prefix}_{suffix}``.
_KNMI_VAR_SUFFIXES: list[tuple[str, str]] = [
    ("T",   "temp_c"),               # Temperature at 1.5m (°C)
    ("TD",  "dewpoint_c"),            # Dew point temperature (°C)
    ("FF",  "wind_speed_ms"),         # 10-min mean wind speed (m/s)
    ("FH",  "wind_speed_hourly_ms"),  # Hourly mean wind speed (m/s)
    ("FX",  "wind_gust_ms"),          # Max wind gust (m/s)
    ("Q",   "solar_rad_jcm2"),        # Global solar radiation (J/cm²)
    ("SQ",  "sunshine_h"),            # Sunshine duration (h)
    ("U",   "humidity_pct"),          # Relative humidity (%)
]


def _knmi_var_map(col_prefix: str = "knmi") -> list[tuple[str, str]]:
    """Build the KNMI variable → column-name mapping with the given prefix.

    Default (non-validated):  ``knmi_temp_c``, ``knmi_wind_speed_ms``, …
    Validated:                ``knmi_val_temp_c``, ``knmi_val_wind_speed_ms``, …
    """
    return [(nc_var, f"{col_prefix}_{suffix}") for nc_var, suffix in _KNMI_VAR_SUFFIXES]

# Reference epoch for the KNMI time variable (seconds since 1950-01-01).
_KNMI_EPOCH = datetime(1950, 1, 1)


def _parse_knmi_timestamp_from_file(filepath: str) -> Optional[datetime]:
    """Parse the UTC timestamp from a KNMI NetCDF filename.

    Filename format: ``hourly-observations-YYYYMMDD-HH.nc``
                 or: ``hourly-observations-validated-YYYYMMDD-HH.nc``
    Returns a timezone-naive datetime (represents UTC) or ``None``.
    """
    match = re.search(
        r"hourly-observations-(?:validated-)?(\d{4})(\d{2})(\d{2})-(\d{2})\.nc$",
        os.path.basename(filepath),
    )
    if not match:
        return None
    return datetime(
        int(match.group(1)), int(match.group(2)),
        int(match.group(3)), int(match.group(4)),
    )


def _process_one_knmi_file(filepath: str) -> dict:
    """Read a single KNMI NetCDF file and compute station-mean values.

    Runs in a child process. Opens the file with ``h5py`` (NetCDF4 files
    are HDF5-based), reads 8 meteorological variables each of shape
    ``(n_stations, 1)``, and computes the mean across all reporting
    stations (ignoring NaN).

    The column prefix (e.g. ``knmi`` or ``knmi_val``) is read from the
    module-level ``_NL_CACHE`` dict, populated by ``_init_worker``.

    Returns a dict with the timestamp and averaged meteorological values,
    plus metadata for progress tracking.
    """
    ts = _parse_knmi_timestamp_from_file(filepath)
    if ts is None:
        return {"file": filepath, "status": "skip", "reason": "no timestamp"}

    col_prefix = _NL_CACHE.get("col_prefix", "knmi")
    var_map = _knmi_var_map(col_prefix)
    station_count_col = f"{col_prefix}_station_count"

    try:
        with _open_h5_with_retry(filepath) as hf:
            record: dict = {"timestamp_utc": ts}

            # Count reporting stations from the temperature field
            station_count = 0

            for nc_var, col_name in var_map:
                if nc_var in hf:
                    data = hf[nc_var][:].ravel().astype(np.float64)
                    valid = data[~np.isnan(data)]
                    record[col_name] = float(np.mean(valid)) if len(valid) > 0 else np.nan
                    if nc_var == "T":
                        station_count = len(valid)
                else:
                    record[col_name] = np.nan

            record[station_count_col] = station_count

    except Exception as exc:
        return {"file": filepath, "status": "error", "reason": str(exc)}

    return {
        "file": os.path.basename(filepath),
        "status": "ok",
        "record": record,
    }


def extract_knmi(
    knmi_dir: str,
    out_dir: str,
    workers: int,
    force: bool = False,
    col_prefix: str = "knmi",
    source_label: str = "knmi",
) -> None:
    """Extract KNMI hourly meteorological NetCDF files to Parquet.

    Reads all ``hourly-observations-*.nc`` files from the KNMI download
    directory.  Each file contains one hour of observations from ~61
    weather stations across the Netherlands.  For each meteorological
    variable, the mean across all reporting stations is computed to
    produce one national-level hourly value.

    Uses ``multiprocessing`` for parallelism — each NetCDF file is
    fully independent (embarrassingly parallel). The ``h5py`` library
    reads NetCDF4 files natively (they are HDF5-based), so no extra
    dependencies are required.

    Output is written as year-partitioned Parquet files.

    Parameters
    ----------
    col_prefix : str
        Column name prefix: ``"knmi"`` for non-validated (produces
        ``knmi_temp_c``, etc.) or ``"knmi_val"`` for validated
        (produces ``knmi_val_temp_c``, etc.).
    source_label : str
        Label used in quality reports and log messages.
    """
    logger.info("=== Phase 1: %s hourly meteorological extraction ===", source_label.upper())
    t0 = time.time()

    nc_files = sorted(glob.glob(os.path.join(knmi_dir, "*.nc")))
    if not nc_files:
        logger.warning("No .nc files found in %s — skipping.", knmi_dir)
        return

    logger.info(
        "Found %d KNMI NetCDF files. Using %d workers. force=%s",
        len(nc_files), workers, force,
    )

    data_dir = os.path.join(out_dir, "data")
    os.makedirs(data_dir, exist_ok=True)

    # --- Checkpoint: if year-partitioned output exists, skip unless --force ---
    existing_years = [
        d for d in os.listdir(data_dir)
        if d.startswith("year=") and os.path.isdir(os.path.join(data_dir, d))
    ] if os.path.isdir(data_dir) else []

    if existing_years and not force:
        logger.info(
            "KNMI output already present (%d year partitions) — skipping "
            "(use --force to re-run).", len(existing_years),
        )
        return

    # --- Parallel extraction ---
    records: list[dict] = []
    ok_count = 0
    err_count = 0
    skip_count = 0

    # Populate the cache with col_prefix so workers can build column names.
    # Under fork (Linux default), the parent's cache is inherited; under
    # spawn/forkserver, the initializer re-populates it.
    cache = {"col_prefix": col_prefix}
    _init_worker(cache)

    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=_init_worker,
        initargs=(cache,),
    ) as pool:
        futures = {pool.submit(_process_one_knmi_file, f): f for f in nc_files}

        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            status = result["status"]
            if status == "ok":
                ok_count += 1
                records.append(result["record"])
            elif status == "skip":
                skip_count += 1
            else:
                err_count += 1
                if err_count <= 10:
                    logger.warning(
                        "  File %s: %s — %s",
                        result.get("file", "?"),
                        status,
                        result.get("reason", ""),
                    )

            if i % 5000 == 0 or i == len(nc_files):
                elapsed = time.time() - t0
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(nc_files) - i) / rate if rate > 0 else 0
                logger.info(
                    "  Progress: %d/%d files (%.0f files/s, ETA %.0fs) | "
                    "%d OK, %d skip, %d err",
                    i, len(nc_files), rate, eta,
                    ok_count, skip_count, err_count,
                )

    if not records:
        logger.warning("No valid KNMI records extracted — skipping.")
        return

    # --- Build DataFrame, deduplicate, write year-partitioned Parquet ---
    df = pd.DataFrame(records)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    df = (
        df.sort_values("timestamp_utc")
          .drop_duplicates(subset=["timestamp_utc"], keep="last")
          .reset_index(drop=True)
    )
    df["year"] = df["timestamp_utc"].dt.year
    station_count_col = f"{col_prefix}_station_count"
    df[station_count_col] = df[station_count_col].astype(np.int32)

    elapsed = time.time() - t0
    logger.info(
        "KNMI: %d unique hourly records from %d files "
        "(%d errors, %d skipped) in %.0fs.",
        len(df), ok_count, err_count, skip_count, elapsed,
    )

    # Write partitioned Parquet
    for year, group in df.groupby("year"):
        year_dir = os.path.join(data_dir, f"year={year}")
        os.makedirs(year_dir, exist_ok=True)
        tbl = pa.Table.from_pandas(
            group.drop(columns=["year"]), preserve_index=False,
        )
        pq.write_table(
            tbl, os.path.join(year_dir, "knmi.parquet"),
            compression="snappy",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )

    logger.info("KNMI written → %s", data_dir)

    # --- Quality report ---
    extra = {
        "completeness": {
            "total_nc_files": len(nc_files),
            "processed_ok": ok_count,
            "processed_error": err_count,
            "processed_skip": skip_count,
            "total_hourly_records": len(df),
            "year_range": [int(df["year"].min()), int(df["year"].max())],
            "station_count_range": [
                int(df[station_count_col].min()),
                int(df[station_count_col].max()),
            ],
        },
        "performance": {
            "elapsed_seconds": round(elapsed, 1),
            "files_per_second": round(ok_count / elapsed, 1) if elapsed > 0 else 0,
            "workers": workers,
        },
    }
    metrics = _compute_parquet_quality(data_dir, source_label, 1, extra)
    _write_quality_json(metrics, out_dir)


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
        description="Phase 1: Extract raw sources to Parquet (multiprocessing)."
    )
    parser.add_argument(
        "--data-root", default="/projects/prjs2061/data",
        help="Root directory for raw input data (VIIRS HDF5, CBS CSVs, "
             "ENTSO-E XLSXs, KNMI NetCDFs).",
    )
    parser.add_argument(
        "--out-root", default=_default_out_root,
        help="Root directory for pipeline output data (processing_1/, etc.). "
             "Defaults to <repo>/data/ relative to this script.",
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

    data_root = args.data_root   # raw input data (VIIRS, CBS, ENTSO-E, KNMI)
    out_root = args.out_root     # pipeline output data (inside repo)
    p1_dir = os.path.join(out_root, "processing_1")
    workers = args.workers

    logger.info("Raw data root : %s", data_root)
    logger.info("Output root   : %s", out_root)
    logger.info("Phase 1 output: %s", p1_dir)
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
        product="a2",
    )
    extract_viirs(
        os.path.join(data_root, "viirs", "A1"),
        os.path.join(p1_dir, "viirs_a1"),
        workers=workers,
        force=args.force,
        product="a1",
    )
    extract_cbs_energy(
        os.path.join(data_root, "cbs"),
        os.path.join(p1_dir, "cbs_energy"),
    )
    extract_cbs_gdp(
        os.path.join(data_root, "cbs"),
        os.path.join(p1_dir, "cbs_gdp"),
    )
    extract_cbs_cpi(
        os.path.join(data_root, "cbs"),
        os.path.join(p1_dir, "cbs_cpi"),
    )
    extract_cbs_gep(
        os.path.join(data_root, "cbs"),
        os.path.join(p1_dir, "cbs_gep"),
    )
    extract_entsoe(
        os.path.join(data_root, "entso-e"),
        os.path.join(p1_dir, "entsoe"),
        workers=workers,
    )
    extract_knmi(
        os.path.join(data_root, "knmi"),
        os.path.join(p1_dir, "knmi"),
        workers=workers,
        force=args.force,
        col_prefix="knmi",
        source_label="knmi",
    )
    extract_knmi(
        os.path.join(data_root, "knmi_validated"),
        os.path.join(p1_dir, "knmi_validated"),
        workers=workers,
        force=args.force,
        col_prefix="knmi_val",
        source_label="knmi_validated",
    )

    logger.info(
        "=== Phase 1 complete in %.0fs. ===", time.time() - t_total
    )


if __name__ == "__main__":
    main()