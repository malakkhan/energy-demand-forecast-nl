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

Usage::

    python phase2_aggregate.py [--data-root /path/to/data] [--workers N]
"""

import argparse
import json
import logging
import os
from datetime import datetime

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
# Data quality helpers (shared with phase1 — duplicated for standalone use)
# ---------------------------------------------------------------------------


def _compute_quality_metrics(
    spark: SparkSession,
    parquet_path: str,
    source_name: str,
    phase: int,
    extra_metrics: dict = None,
) -> dict:
    """Compute data quality metrics for a Parquet dataset.

    Args:
        spark: Active SparkSession.
        parquet_path: Path to the Parquet directory.
        source_name: Human-readable source identifier.
        phase: Pipeline phase number.
        extra_metrics: Optional additional metrics to include.

    Returns:
        A dictionary of quality metrics.
    """
    df = spark.read.parquet(parquet_path)
    total_rows = df.count()
    columns = df.columns

    col_metrics = {}
    null_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns
    ]
    if total_rows > 0:
        null_counts = df.select(null_exprs).collect()[0]
    else:
        null_counts = {c: 0 for c in columns}

    for c in columns:
        dtype = str(df.schema[c].dataType)
        nc = int(null_counts[c]) if total_rows > 0 else 0
        col_metrics[c] = {
            "dtype": dtype,
            "null_count": nc,
            "null_pct": round(100.0 * nc / total_rows, 2) if total_rows > 0 else 0.0,
        }

    size_bytes = 0
    for root, _, files in os.walk(parquet_path):
        for fname in files:
            size_bytes += os.path.getsize(os.path.join(root, fname))

    part_files = []
    for root, _, files in os.walk(parquet_path):
        part_files.extend(f for f in files if f.startswith("part-"))

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
    """Write data_quality.json to the given directory."""
    path = os.path.join(out_dir, "data_quality.json")
    with open(path, "w") as fh:
        json.dump(metrics, fh, indent=2, default=str)
    logger.info("Quality report written → %s", path)


# ---------------------------------------------------------------------------
# Phase 2A: VIIRS A2 daily aggregates
# ---------------------------------------------------------------------------


def aggregate_viirs(spark: SparkSession, p1_dir: str, out_dir: str) -> None:
    """Aggregate pixel-level VIIRS data to daily scalar statistics.

    Reads the NL-masked pixel-level Parquet from Phase 1 and computes per-day
    aggregates over the spatial dimension.

    Output columns:
        - ``date``: Observation date.
        - ``ntl_mean``: Mean NTL radiance of valid pixels (quality ≤ 1,
          not fill). Unit: nW/cm²/sr (raw float32 values from the product —
          the VNP46A2 Gap-Filled layer values are already in physical units).
        - ``ntl_sum``: Sum of NTL radiance of valid pixels. Captures total
          light emission over the masked region.
        - ``ntl_valid_count``: Number of valid pixels (quality ≤ 1, not fill).
        - ``ntl_fill_count``: Number of fill-value pixels (``-999.9``).
          Indicates sensor/retrieval gaps.
        - ``ntl_invalid_count``: Number of non-fill pixels with poor quality
          (quality flag = 255, i.e. no retrieval or quality ≥ 2). Indicates
          cloud/atmospheric contamination.

    Args:
        spark: Active SparkSession.
        p1_dir: Phase 1 input directory (``data/processing_1/viirs_a2``).
        out_dir: Phase 2 output dir (``data/processing_2/viirs_a2_daily``).
    """
    logger.info("=== Phase 2: VIIRS A2 daily aggregation ===")
    parquet_path = os.path.join(p1_dir, "data")
    if not os.path.exists(parquet_path):
        logger.warning("VIIRS Phase 1 data not found at %s — skipping.", parquet_path)
        return

    df = spark.read.parquet(parquet_path)
    total_pixels = df.count()
    logger.info("Read %d pixel rows from Phase 1.", total_pixels)

    # A pixel is "valid" when it is not a fill value AND quality flag ≤ 1
    # A pixel is "fill" when is_fill = true
    # A pixel is "invalid" when is_fill = false AND quality_flag > 1 (typically 255)
    agg_df = df.groupBy("date").agg(
        # Mean of valid pixels only
        F.mean(
            F.when(
                (~F.col("is_fill")) & (F.col("quality_flag") <= 1),
                F.col("ntl_radiance"),
            )
        ).alias("ntl_mean"),

        # Sum of valid pixels only
        F.sum(
            F.when(
                (~F.col("is_fill")) & (F.col("quality_flag") <= 1),
                F.col("ntl_radiance"),
            )
        ).alias("ntl_sum"),

        # Count of valid pixels
        F.sum(
            F.when(
                (~F.col("is_fill")) & (F.col("quality_flag") <= 1),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).cast(T.IntegerType()).alias("ntl_valid_count"),

        # Count of fill-value pixels
        F.sum(
            F.when(F.col("is_fill"), F.lit(1)).otherwise(F.lit(0))
        ).cast(T.IntegerType()).alias("ntl_fill_count"),

        # Count of non-fill, non-valid (quality > 1) pixels
        F.sum(
            F.when(
                (~F.col("is_fill")) & (F.col("quality_flag") > 1),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).cast(T.IntegerType()).alias("ntl_invalid_count"),
    )

    # Add year column for partitioning
    agg_df = agg_df.withColumn("year", F.year("date"))
    agg_df = agg_df.orderBy("date")

    os.makedirs(out_dir, exist_ok=True)
    out_parquet = os.path.join(out_dir, "data")
    agg_df.write.mode("overwrite").partitionBy("year").parquet(out_parquet)

    row_count = agg_df.count()
    logger.info("VIIRS daily aggregates: %d rows → %s", row_count, out_parquet)

    # Quality checks
    # Verify that all days have the expected pixel counts
    total_expected_per_day = agg_df.select(
        (F.col("ntl_valid_count") + F.col("ntl_fill_count") + F.col("ntl_invalid_count"))
        .alias("total")
    )
    pixel_stats = total_expected_per_day.select(
        F.min("total").alias("min_total"),
        F.max("total").alias("max_total"),
        F.mean("total").alias("mean_total"),
    ).collect()[0]

    # Statistics on valid pixel fractions
    valid_frac = agg_df.select(
        (F.col("ntl_valid_count") /
         (F.col("ntl_valid_count") + F.col("ntl_fill_count") + F.col("ntl_invalid_count")))
        .alias("valid_frac")
    )
    frac_stats = valid_frac.select(
        F.mean("valid_frac").alias("mean"),
        F.min("valid_frac").alias("min"),
    ).collect()[0]

    extra = {
        "pixel_counts_per_day": {
            "min": int(pixel_stats["min_total"]),
            "max": int(pixel_stats["max_total"]),
            "mean": round(float(pixel_stats["mean_total"]), 0),
        },
        "valid_pixel_fraction": {
            "mean": round(float(frac_stats["mean"]), 4) if frac_stats["mean"] else None,
            "min": round(float(frac_stats["min"]), 4) if frac_stats["min"] else None,
        },
        "integrity": {
            "total_source_pixels": total_pixels,
            "total_aggregated_days": row_count,
        },
    }
    metrics = _compute_quality_metrics(spark, out_parquet, "viirs_a2_daily", 2, extra)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 2B: CBS Combined
# ---------------------------------------------------------------------------


def combine_cbs(spark: SparkSession, p1_dir: str, out_dir: str) -> None:
    """Combine CBS energy (monthly) and CBS macro (annual) into one table.

    The annual GDP/population values are broadcast-joined to each month of
    their respective year, yielding a single monthly-resolution table with
    all CBS indicators.

    Args:
        spark: Active SparkSession.
        p1_dir: Phase 1 base directory (``data/processing_1``).
        out_dir: Phase 2 output dir (``data/processing_2/cbs_combined``).
    """
    logger.info("=== Phase 2: CBS combination ===")

    energy_path = os.path.join(p1_dir, "cbs_energy", "data")
    macro_path = os.path.join(p1_dir, "cbs_macro", "data")

    has_energy = os.path.exists(energy_path)
    has_macro = os.path.exists(macro_path)

    if not has_energy and not has_macro:
        logger.warning("No CBS Phase 1 data found — skipping.")
        return

    if has_energy:
        energy_df = spark.read.parquet(energy_path)
        logger.info("CBS Energy: %d monthly records loaded.", energy_df.count())
    else:
        logger.warning("CBS Energy Phase 1 not found.")
        energy_df = None

    if has_macro:
        macro_df = spark.read.parquet(macro_path)
        # Rename columns with cbs_ prefix for clarity in the combined table
        macro_df = macro_df.select(
            F.col("year"),
            F.col("gdp_million_eur").alias("cbs_gdp_million_eur"),
            F.col("population_million").alias("cbs_population_million"),
        )
        logger.info("CBS Macro: %d annual records loaded.", macro_df.count())
    else:
        logger.warning("CBS Macro Phase 1 not found.")
        macro_df = None

    # Combine: join macro onto energy by year (broadcast macro since it's tiny)
    if energy_df is not None and macro_df is not None:
        combined = energy_df.join(F.broadcast(macro_df), on="year", how="outer")
    elif energy_df is not None:
        combined = energy_df
    else:
        # Only macro: expand to 12 months per year for consistency
        months = spark.range(1, 13).withColumnRenamed("id", "month")
        combined = macro_df.crossJoin(months)

    combined = combined.orderBy("year", "month")

    os.makedirs(out_dir, exist_ok=True)
    out_parquet = os.path.join(out_dir, "data")
    combined.write.mode("overwrite").parquet(out_parquet)

    row_count = combined.count()
    logger.info("CBS Combined: %d records → %s", row_count, out_parquet)

    # Quality: check for year gaps
    years = sorted([
        r["year"] for r in combined.select("year").distinct().collect()
    ])
    expected_years = list(range(min(years), max(years) + 1)) if years else []
    missing_years = sorted(set(expected_years) - set(years))

    extra = {
        "year_range": [min(years), max(years)] if years else [],
        "year_count": len(years),
        "missing_years": missing_years,
    }
    metrics = _compute_quality_metrics(spark, out_parquet, "cbs_combined", 2, extra)
    _write_quality_json(metrics, out_dir)


# ---------------------------------------------------------------------------
# Phase 2C: ENTSO-E (pass-through with quality check)
# ---------------------------------------------------------------------------


def passthrough_entsoe(spark: SparkSession, p1_dir: str, out_dir: str) -> None:
    """Re-partition ENTSO-E data by year and generate a Phase 2 quality report.

    This is semantically a pass-through: the ENTSO-E data is already at its
    native hourly resolution from Phase 1. We re-write it with consistent
    year-partitioning and produce a Phase 2 quality JSON that includes
    per-year hour counts and gap analysis.

    Args:
        spark: Active SparkSession.
        p1_dir: Phase 1 ENTSO-E directory (``data/processing_1/entsoe``).
        out_dir: Phase 2 output dir (``data/processing_2/entsoe``).
    """
    logger.info("=== Phase 2: ENTSO-E pass-through ===")
    parquet_path = os.path.join(p1_dir, "data")
    if not os.path.exists(parquet_path):
        logger.warning("ENTSO-E Phase 1 data not found — skipping.")
        return

    df = spark.read.parquet(parquet_path)
    total = df.count()
    logger.info("ENTSO-E: %d hourly records loaded.", total)

    os.makedirs(out_dir, exist_ok=True)
    out_parquet = os.path.join(out_dir, "data")
    df.write.mode("overwrite").partitionBy("year").parquet(out_parquet)
    logger.info("ENTSO-E: re-partitioned → %s", out_parquet)

    # Per-year coverage analysis
    # Expected hours per year: 365*24=8760 (or 366*24=8784 for leap years)
    year_counts = (
        df.groupBy("year")
          .agg(F.count("*").alias("hour_count"))
          .orderBy("year")
          .collect()
    )
    year_coverage = {}
    for row in year_counts:
        y = row["year"]
        is_leap = (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0)
        expected = 8784 if is_leap else 8760
        actual = row["hour_count"]
        year_coverage[str(y)] = {
            "expected_hours": expected,
            "actual_hours": actual,
            "coverage_pct": round(100.0 * actual / expected, 1),
        }

    # Detect load value anomalies
    load_stats = df.select(
        F.min("entsoe_load_mw").alias("min"),
        F.max("entsoe_load_mw").alias("max"),
        F.mean("entsoe_load_mw").alias("mean"),
        F.stddev("entsoe_load_mw").alias("stddev"),
    ).collect()[0]

    extra = {
        "year_coverage": year_coverage,
        "load_statistics": {
            "min_mw": round(float(load_stats["min"]), 1) if load_stats["min"] else None,
            "max_mw": round(float(load_stats["max"]), 1) if load_stats["max"] else None,
            "mean_mw": round(float(load_stats["mean"]), 1) if load_stats["mean"] else None,
            "stddev_mw": round(float(load_stats["stddev"]), 1) if load_stats["stddev"] else None,
        },
    }
    metrics = _compute_quality_metrics(spark, out_parquet, "entsoe", 2, extra)
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
        help="Root data directory (default: /projects/prjs2061/data).",
    )
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Number of local Spark executor threads (default: 4).",
    )
    return parser.parse_args()


def main() -> None:
    """Run all Phase 2 aggregation stages."""
    args = _parse_args()
    data_root = args.data_root
    p1_dir = os.path.join(data_root, "processing_1")
    p2_dir = os.path.join(data_root, "processing_2")

    logger.info("Phase 1 input:  %s", p1_dir)
    logger.info("Phase 2 output: %s", p2_dir)

    spark = (
        SparkSession.builder
        .appName("NL_Energy_Phase2")
        .master(f"local[{args.workers}]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        aggregate_viirs(
            spark,
            p1_dir=os.path.join(p1_dir, "viirs_a2"),
            out_dir=os.path.join(p2_dir, "viirs_a2_daily"),
        )
        combine_cbs(
            spark,
            p1_dir=p1_dir,
            out_dir=os.path.join(p2_dir, "cbs_combined"),
        )
        passthrough_entsoe(
            spark,
            p1_dir=os.path.join(p1_dir, "entsoe"),
            out_dir=os.path.join(p2_dir, "entsoe"),
        )
        logger.info("=== Phase 2 complete. ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
