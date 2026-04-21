#!/usr/bin/env python3
"""Phase 2: Aggregate and combine Phase 1 outputs.

This is Stage 2 of the three-phase NL energy demand data pipeline. It reads
the cleaned, source-level Parquet files from ``data/processing_1/`` and
produces aggregated/combined tables in ``data/processing_2/``.

Operations:
    - **VIIRS A2**: Pixel-level spatial data → daily scalar aggregates
      (mean, sum, valid/fill/invalid pixel counts).
    - **CBS Combined**: Merge CBS energy (monthly) and CBS macro (annual)
      into a single aligned table.
    - **ENTSO-E**: Pass-through with consistent year-partitioning and a
      fresh quality report.

Optimised for Snellius: column-pruned reads, AQE-enabled shuffles, scratch
spill on node-local fast storage, and stage-level checkpointing so reruns
skip completed stages unless ``--force`` is given.

Usage::

    python phase2_aggregate.py [--data-root /path/to/data] [--workers N]
                                [--force] [--driver-memory 64g]
"""

import argparse
import json
import logging
import os
import shutil
import time
from datetime import datetime
from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("phase2_aggregate")


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def _stage_done(out_dir: str) -> bool:
    """Return True if a previous Spark write completed successfully.

    Spark writes a ``_SUCCESS`` marker file on successful Parquet output,
    so its presence is a reliable idempotency signal.
    """
    return os.path.exists(os.path.join(out_dir, "data", "_SUCCESS"))


def _clear_output(out_dir: str) -> None:
    """Remove a stage's output directory (used under --force)."""
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)


def _scratch_dir() -> str:
    """Return the best node-local scratch directory for Spark shuffle spill.

    On Snellius under SLURM, ``$TMPDIR`` points at node-local fast SSD. Using
    that for ``spark.local.dir`` avoids shuffling through GPFS, which is
    dramatically slower for the many-small-file access pattern of shuffle.
    """
    for var in ("SLURM_TMPDIR", "TMPDIR"):
        v = os.environ.get(var)
        if v and os.path.isdir(v):
            return v
    return "/tmp"


# ---------------------------------------------------------------------------
# Data quality helpers — pandas-based (outputs of Phase 2 are all small)
# ---------------------------------------------------------------------------


def _compute_quality_metrics(
    parquet_path: str,
    source_name: str,
    phase: int,
    extra_metrics: Optional[dict] = None,
) -> dict:
    """Compute data quality metrics for a small Parquet dataset.

    Uses pandas rather than Spark: every Phase 2 output (VIIRS daily,
    CBS combined, ENTSO-E hourly) fits comfortably in driver memory, and
    avoiding the Spark job overhead for metrics speeds up each stage by
    several seconds.
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
    part_files = []
    for root, _, files in os.walk(parquet_path):
        for fname in files:
            size_bytes += os.path.getsize(os.path.join(root, fname))
            if fname.startswith("part-"):
                part_files.append(fname)

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
# Phase 2A: VIIRS A2 daily aggregates
# ---------------------------------------------------------------------------


def aggregate_viirs(
    spark: SparkSession,
    p1_dir: str,
    out_dir: str,
    force: bool = False,
) -> None:
    """Aggregate pixel-level VIIRS data to daily scalar statistics.

    Reads the NL-masked pixel-level Parquet from Phase 1 and computes per-day
    aggregates over the spatial dimension. This is the heaviest Phase 2 stage
    — potentially billions of input rows — so we explicitly prune to only
    the four columns we need (date, ntl_radiance, quality_flag, is_fill)
    to halve the bytes read from Parquet.

    Output columns:
        - ``date``: Observation date.
        - ``ntl_mean``: Mean NTL radiance of valid pixels (quality ≤ 1, not
          fill). Unit: nW/cm²/sr (raw values from the VNP46A2 Gap-Filled
          layer, which is already in physical units).
        - ``ntl_sum``: Sum of NTL radiance of valid pixels — total emission
          over the NL region.
        - ``ntl_valid_count``: Number of valid pixels (quality ≤ 1, not fill).
        - ``ntl_fill_count``: Number of fill-value pixels (``-999.9``),
          indicating sensor/retrieval gaps.
        - ``ntl_invalid_count``: Number of non-fill pixels with poor quality
          (quality flag > 1), indicating cloud/atmospheric contamination.
    """
    logger.info("=== Phase 2: VIIRS A2 daily aggregation ===")
    t0 = time.time()

    if _stage_done(out_dir) and not force:
        logger.info("VIIRS daily output already present — skipping (use --force to re-run).")
        return

    if force:
        _clear_output(out_dir)

    parquet_path = os.path.join(p1_dir, "data")
    if not os.path.exists(parquet_path):
        logger.warning("VIIRS Phase 1 data not found at %s — skipping.", parquet_path)
        return

    logger.info("Reading VIIRS pixel data from %s ...", parquet_path)

    # --- Column pruning: read only the 4 fields used downstream. On a
    # ~3 B-row pixel table, dropping lat/lon/row_idx/col_idx roughly halves
    # the Parquet bytes read and the in-memory column count. ---
    df = spark.read.parquet(parquet_path).select(
        "date", "ntl_radiance", "quality_flag", "is_fill"
    )

    # Classification predicates, defined once for readability.
    is_valid = (~F.col("is_fill")) & (F.col("quality_flag") <= 1)
    is_fill = F.col("is_fill")
    is_invalid = (~F.col("is_fill")) & (F.col("quality_flag") > 1)

    agg_df = df.groupBy("date").agg(
        F.mean(F.when(is_valid, F.col("ntl_radiance"))).alias("ntl_mean"),
        F.sum(F.when(is_valid, F.col("ntl_radiance"))).alias("ntl_sum"),
        F.sum(F.when(is_valid, 1).otherwise(0)).cast(T.IntegerType()).alias("ntl_valid_count"),
        F.sum(F.when(is_fill, 1).otherwise(0)).cast(T.IntegerType()).alias("ntl_fill_count"),
        F.sum(F.when(is_invalid, 1).otherwise(0)).cast(T.IntegerType()).alias("ntl_invalid_count"),
    ).withColumn("year", F.year("date"))

    # The aggregated result is ~5k rows across ~15 years. Coalesce to one
    # file per year partition instead of Spark's default many-small-files.
    out_parquet = os.path.join(out_dir, "data")
    os.makedirs(out_dir, exist_ok=True)
    (
        agg_df.repartition("year")
              .sortWithinPartitions("date")
              .write.mode("overwrite")
              .partitionBy("year")
              .parquet(out_parquet)
    )
    logger.info("VIIRS daily aggregates written → %s (%.1fs)",
                out_parquet, time.time() - t0)

    # --- Quality metrics: load the tiny result with pandas and compute locally. ---
    result = pd.read_parquet(out_parquet)
    total_per_day = (result["ntl_valid_count"]
                     + result["ntl_fill_count"]
                     + result["ntl_invalid_count"])
    valid_frac = result["ntl_valid_count"] / total_per_day.replace(0, pd.NA)

    extra = {
        "pixel_counts_per_day": {
            "min": int(total_per_day.min()),
            "max": int(total_per_day.max()),
            "mean": round(float(total_per_day.mean()), 0),
        },
        "valid_pixel_fraction": {
            "mean": round(float(valid_frac.mean()), 4) if valid_frac.notna().any() else None,
            "min": round(float(valid_frac.min()), 4) if valid_frac.notna().any() else None,
        },
        "integrity": {
            "total_aggregated_days": len(result),
        },
        "stage_elapsed_seconds": round(time.time() - t0, 1),
    }
    metrics = _compute_quality_metrics(out_parquet, "viirs_a2_daily", 2, extra)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 2B: CBS Combined
# ---------------------------------------------------------------------------


def combine_cbs(
    spark: SparkSession,
    p1_dir: str,
    out_dir: str,
    force: bool = False,
) -> None:
    """Combine CBS energy (monthly) and CBS GDP (monthly, from quarterly) into one table.

    Both sources are at monthly resolution: CBS energy prices are natively
    monthly, and CBS GDP indicators have been forward-filled from quarterly
    to monthly in Phase 1.  The join on ``(year, month)`` produces a clean
    combined table with no orphaned rows.
    """
    logger.info("=== Phase 2: CBS combination ===")
    t0 = time.time()

    if _stage_done(out_dir) and not force:
        logger.info("CBS combined output already present — skipping (use --force to re-run).")
        return

    if force:
        _clear_output(out_dir)

    energy_path = os.path.join(p1_dir, "cbs_energy", "data")
    gdp_path = os.path.join(p1_dir, "cbs_gdp", "data")

    has_energy = os.path.exists(energy_path)
    has_gdp = os.path.exists(gdp_path)

    if not has_energy and not has_gdp:
        logger.warning("No CBS Phase 1 data found — skipping.")
        return

    energy_df = None
    gdp_df = None

    if has_energy:
        energy_df = spark.read.parquet(energy_path)
        logger.info("CBS Energy loaded from %s.", energy_path)
    else:
        logger.warning("CBS Energy Phase 1 not found.")

    if has_gdp:
        gdp_df = spark.read.parquet(gdp_path)
        logger.info("CBS GDP loaded from %s.", gdp_path)
    else:
        logger.warning("CBS GDP Phase 1 not found.")

    if energy_df is not None and gdp_df is not None:
        combined = energy_df.join(
            F.broadcast(gdp_df), on=["year", "month"], how="outer"
        )
    elif energy_df is not None:
        combined = energy_df
    else:
        combined = gdp_df

    combined = combined.orderBy("year", "month")

    # CBS combined is ~400 rows — one output file is plenty.
    out_parquet = os.path.join(out_dir, "data")
    os.makedirs(out_dir, exist_ok=True)
    combined.coalesce(1).write.mode("overwrite").parquet(out_parquet)
    logger.info("CBS Combined written → %s (%.1fs)", out_parquet, time.time() - t0)

    # Pandas-side quality analysis.
    result = pd.read_parquet(out_parquet)
    years = sorted(result["year"].dropna().astype(int).unique().tolist())
    expected_years = list(range(min(years), max(years) + 1)) if years else []
    missing_years = sorted(set(expected_years) - set(years))

    extra = {
        "year_range": [min(years), max(years)] if years else [],
        "year_count": len(years),
        "missing_years": missing_years,
        "stage_elapsed_seconds": round(time.time() - t0, 1),
    }
    metrics = _compute_quality_metrics(out_parquet, "cbs_combined", 2, extra)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 2C: ENTSO-E (pass-through with quality check)
# ---------------------------------------------------------------------------


def passthrough_entsoe(
    spark: SparkSession,
    p1_dir: str,
    out_dir: str,
    force: bool = False,
) -> None:
    """Re-partition ENTSO-E data by year and generate a Phase 2 quality report.

    Semantically a pass-through — the data is already at its native hourly
    resolution from Phase 1. We re-write it with one file per year partition
    (instead of Spark's default many-small-files split) and produce a
    Phase 2 quality JSON covering per-year hour coverage and load statistics.
    """
    logger.info("=== Phase 2: ENTSO-E pass-through ===")
    t0 = time.time()

    if _stage_done(out_dir) and not force:
        logger.info("ENTSO-E output already present — skipping (use --force to re-run).")
        return

    if force:
        _clear_output(out_dir)

    parquet_path = os.path.join(p1_dir, "data")
    if not os.path.exists(parquet_path):
        logger.warning("ENTSO-E Phase 1 data not found — skipping.")
        return

    df = spark.read.parquet(parquet_path)
    logger.info("ENTSO-E loaded from %s.", parquet_path)

    out_parquet = os.path.join(out_dir, "data")
    os.makedirs(out_dir, exist_ok=True)
    (
        df.repartition("year")
          .sortWithinPartitions("timestamp_utc")
          .write.mode("overwrite")
          .partitionBy("year")
          .parquet(out_parquet)
    )
    logger.info("ENTSO-E re-partitioned → %s (%.1fs)", out_parquet, time.time() - t0)

    # --- Pandas-side quality analysis. ~140 k rows fit easily. ---
    result = pd.read_parquet(out_parquet)
    year_coverage = {}
    for y, grp in result.groupby("year"):
        is_leap = (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0)
        expected = 8784 if is_leap else 8760
        actual = len(grp)
        year_coverage[str(int(y))] = {
            "expected_hours": expected,
            "actual_hours": int(actual),
            "coverage_pct": round(100.0 * actual / expected, 1),
        }

    load = result["entsoe_load_mw"].dropna()
    load_stats = {
        "min_mw": round(float(load.min()), 1) if len(load) else None,
        "max_mw": round(float(load.max()), 1) if len(load) else None,
        "mean_mw": round(float(load.mean()), 1) if len(load) else None,
        "stddev_mw": round(float(load.std()), 1) if len(load) else None,
    }

    extra = {
        "year_coverage": year_coverage,
        "load_statistics": load_stats,
        "stage_elapsed_seconds": round(time.time() - t0, 1),
    }
    metrics = _compute_quality_metrics(out_parquet, "entsoe", 2, extra)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 2: Aggregate and combine Phase 1 outputs."
    )
    parser.add_argument(
        "--data-root", default="/projects/prjs2061/data",
        help="Root data directory.",
    )
    parser.add_argument(
        "--workers", type=int, default=os.cpu_count() or 4,
        help="Number of local Spark executor threads "
             "(default: all available CPUs).",
    )
    parser.add_argument(
        "--driver-memory", default="64g",
        help="Spark driver memory in local mode (default: 64g).",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-run stages even if their output already exists.",
    )
    return parser.parse_args()


def main() -> None:
    """Run all Phase 2 aggregation stages."""
    args = _parse_args()
    data_root = args.data_root
    p1_dir = os.path.join(data_root, "processing_1")
    p2_dir = os.path.join(data_root, "processing_2")
    scratch = _scratch_dir()

    logger.info("Phase 1 input : %s", p1_dir)
    logger.info("Phase 2 output: %s", p2_dir)
    logger.info("Workers       : %d (of %d CPUs)",
                args.workers, os.cpu_count() or 0)
    logger.info("Driver memory : %s", args.driver_memory)
    logger.info("Spark scratch : %s", scratch)
    logger.info("Force rerun   : %s", args.force)

    t_total = time.time()

    # --- Spark configuration tuned for a single Snellius node ---
    # - AQE auto-coalesces post-shuffle partitions and handles skew, so we
    #   don't need to hand-tune shuffle.partitions for every stage.
    # - spark.local.dir goes on node-local SSD rather than GPFS — shuffle
    #   I/O on a parallel filesystem is disastrously slow.
    # - In local[N] mode only driver memory is honoured; executor.memory is
    #   ignored, so we don't set it.
    spark = (
        SparkSession.builder
        .appName("NL_Energy_Phase2")
        .master(f"local[{args.workers}]")
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.local.dir", scratch)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(args.workers * 4))
        .config("spark.default.parallelism", str(args.workers * 2))
        .config("spark.sql.files.maxPartitionBytes", "256m")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        aggregate_viirs(
            spark,
            p1_dir=os.path.join(p1_dir, "viirs_a2"),
            out_dir=os.path.join(p2_dir, "viirs_a2_daily"),
            force=args.force,
        )
        combine_cbs(
            spark,
            p1_dir=p1_dir,
            out_dir=os.path.join(p2_dir, "cbs_combined"),
            force=args.force,
        )
        passthrough_entsoe(
            spark,
            p1_dir=os.path.join(p1_dir, "entsoe"),
            out_dir=os.path.join(p2_dir, "entsoe"),
            force=args.force,
        )
        logger.info("=== Phase 2 complete in %.0fs. ===", time.time() - t_total)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()