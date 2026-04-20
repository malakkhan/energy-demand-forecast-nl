#!/usr/bin/env python3
"""Phase 3: Merge all Phase 2 outputs into a unified hourly dataset.

This is the final stage of the three-phase NL energy demand data pipeline.
It reads the aggregated/combined Parquet files from ``data/processing_2/``
and joins them onto a canonical hourly UTC timestamp spine spanning
2012-01-01 to today.

The final dataset is written to ``data/processed/nl_hourly_dataset.parquet``,
partitioned by year, with a comprehensive ``data_quality.json``.

Join strategy:
    - **ENTSO-E** (hourly): Equi-join on ``timestamp``.
    - **VIIRS A2 daily** (daily): Join on ``date``. Missing days are left null.
    - **CBS Combined** (monthly): Join on ``(year, month)``.

Usage::

    python phase3_merge.py [--data-root /path/to/data]
                           [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                           [--workers N]
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("phase3_merge")


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
    for candidate in ["timestamp", "date", "timestamp_utc"]:
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
# Spine construction
# ---------------------------------------------------------------------------


def build_hourly_spine(
    spark: SparkSession, start_date: str, end_date: str
):
    """Generate a contiguous hourly UTC timestamp spine.

    Creates one row per hour for the closed interval [start_date 00:00,
    end_date 23:00], providing a complete temporal scaffold. Any missing
    source data appears as explicit nulls after left-joining.

    Args:
        spark: Active SparkSession.
        start_date: Start date (inclusive) in ``YYYY-MM-DD`` format.
        end_date: End date (inclusive) in ``YYYY-MM-DD`` format.

    Returns:
        A Spark DataFrame with temporal columns: ``timestamp``, ``date``,
        ``year``, ``month``, ``day``, ``hour``, ``day_of_week``,
        ``is_weekend``, ``day_of_year``, ``week_of_year``, ``quarter``.
    """
    logger.info("Building hourly spine: %s → %s", start_date, end_date)

    start_ts = F.to_timestamp(F.lit(f"{start_date} 00:00:00"))
    end_ts = F.to_timestamp(F.lit(f"{end_date} 23:00:00"))

    spine = spark.range(1).select(
        F.explode(
            F.sequence(start_ts, end_ts, F.expr("INTERVAL 1 HOUR"))
        ).alias("timestamp")
    )

    spine = (
        spine
        .withColumn("date",         F.to_date("timestamp"))
        .withColumn("year",         F.year("timestamp"))
        .withColumn("month",        F.month("timestamp"))
        .withColumn("day",          F.dayofmonth("timestamp"))
        .withColumn("hour",         F.hour("timestamp"))
        .withColumn("day_of_week",  F.dayofweek("timestamp"))
        .withColumn("is_weekend",   (F.dayofweek("timestamp").isin(1, 7)).cast(T.IntegerType()))
        .withColumn("day_of_year",  F.dayofyear("timestamp"))
        .withColumn("week_of_year", F.weekofyear("timestamp"))
        .withColumn("quarter",      F.quarter("timestamp"))
    )

    count = spine.count()
    logger.info("Spine: %d hourly rows.", count)
    return spine


# ---------------------------------------------------------------------------
# Join functions
# ---------------------------------------------------------------------------


def join_entsoe(spine, p2_entsoe_path: str):
    """Left-join ENTSO-E hourly load onto the timestamp spine.

    Args:
        spine: Hourly spine DataFrame.
        p2_entsoe_path: Path to Phase 2 ENTSO-E Parquet directory.

    Returns:
        Spine augmented with ``entsoe_load_mw``.
    """
    if not os.path.exists(p2_entsoe_path):
        logger.warning("ENTSO-E P2 not found — column will be null.")
        return spine.withColumn("entsoe_load_mw", F.lit(None).cast(T.DoubleType()))

    spark = spine.sparkSession
    entsoe = spark.read.parquet(p2_entsoe_path).select(
        F.col("timestamp_utc").alias("timestamp"),
        "entsoe_load_mw",
    )
    logger.info("ENTSO-E: %d hourly records.", entsoe.count())

    result = spine.join(entsoe, on="timestamp", how="left")
    non_null = result.filter(F.col("entsoe_load_mw").isNotNull()).count()
    total = spine.count()
    logger.info(
        "ENTSO-E join: %d / %d non-null (%.1f%%).",
        non_null, total, 100 * non_null / total if total else 0,
    )
    return result


def join_viirs(spine, p2_viirs_path: str):
    """Left-join VIIRS daily aggregates onto the spine by date.

    Daily aggregates are broadcast-joined since they are small (~5k rows).
    All 24 hours of a date receive the same daily aggregate values.

    Args:
        spine: Hourly spine DataFrame (must have ``date`` column).
        p2_viirs_path: Path to Phase 2 VIIRS daily aggregate Parquet.

    Returns:
        Spine augmented with VIIRS aggregate columns.
    """
    viirs_cols = [
        "ntl_mean", "ntl_sum", "ntl_valid_count",
        "ntl_fill_count", "ntl_invalid_count",
    ]
    if not os.path.exists(p2_viirs_path):
        logger.warning("VIIRS P2 not found — columns will be null.")
        for c in viirs_cols:
            dtype = T.DoubleType() if c in ("ntl_mean", "ntl_sum") else T.IntegerType()
            spine = spine.withColumn(c, F.lit(None).cast(dtype))
        return spine

    spark = spine.sparkSession
    viirs = spark.read.parquet(p2_viirs_path).drop("year")
    logger.info("VIIRS daily: %d rows.", viirs.count())

    result = spine.join(F.broadcast(viirs), on="date", how="left")
    non_null = result.filter(F.col("ntl_mean").isNotNull()).count()
    total = spine.count()
    logger.info(
        "VIIRS join: %d / %d hours with NTL data (%.1f%%).",
        non_null, total, 100 * non_null / total if total else 0,
    )
    return result


def join_cbs(spine, p2_cbs_path: str):
    """Left-join CBS combined (monthly) onto the spine by (year, month).

    The CBS data is broadcast-joined since it is tiny (<1 KB).

    Args:
        spine: Hourly spine DataFrame.
        p2_cbs_path: Path to Phase 2 CBS combined Parquet.

    Returns:
        Spine augmented with all CBS columns.
    """
    if not os.path.exists(p2_cbs_path):
        logger.warning("CBS P2 not found — columns will be null.")
        for c in [
            "cbs_total_energy_price_idx", "cbs_natural_gas_price_idx",
            "cbs_crude_oil_price_idx", "cbs_electricity_price_idx",
            "cbs_gdp_million_eur", "cbs_population_million",
        ]:
            spine = spine.withColumn(c, F.lit(None).cast(T.DoubleType()))
        return spine

    spark = spine.sparkSession
    cbs = spark.read.parquet(p2_cbs_path)
    logger.info("CBS combined: %d records.", cbs.count())

    result = spine.join(F.broadcast(cbs), on=["year", "month"], how="left")
    return result


# ---------------------------------------------------------------------------
# Column ordering
# ---------------------------------------------------------------------------


def _ordered_columns(df) -> list:
    """Return columns in a deterministic semantic order.

    Order: timestamp → target → satellite → economic → temporal → internal.

    Args:
        df: The combined Spark DataFrame.

    Returns:
        List of column names in the desired output order.
    """
    preferred = [
        "timestamp",
        # Target
        "entsoe_load_mw",
        # VIIRS aggregates
        "ntl_mean", "ntl_sum",
        "ntl_valid_count", "ntl_fill_count", "ntl_invalid_count",
        # CBS
        "cbs_total_energy_price_idx", "cbs_natural_gas_price_idx",
        "cbs_crude_oil_price_idx", "cbs_electricity_price_idx",
        "cbs_gdp_million_eur", "cbs_population_million",
        # Temporal
        "year", "month", "day", "hour",
        "day_of_week", "is_weekend",
        "day_of_year", "week_of_year", "quarter",
        # Internal join key
        "date",
    ]
    existing = set(df.columns)
    return [c for c in preferred if c in existing]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase 3: Merge all sources into a unified hourly Parquet dataset."
    )
    parser.add_argument(
        "--data-root", default="/projects/prjs2061/data",
        help="Root data directory (default: /projects/prjs2061/data).",
    )
    parser.add_argument(
        "--start", default="2012-01-01",
        help="Spine start date, YYYY-MM-DD (default: 2012-01-01).",
    )
    parser.add_argument(
        "--end", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Spine end date, YYYY-MM-DD (default: today UTC).",
    )
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Number of local Spark executor threads (default: 4).",
    )
    return parser.parse_args()


def main() -> None:
    """Build the final hourly dataset from Phase 2 outputs."""
    args = _parse_args()
    data_root = args.data_root
    p2_dir = os.path.join(data_root, "processing_2")
    out_dir = os.path.join(data_root, "processed")
    os.makedirs(out_dir, exist_ok=True)

    logger.info("Phase 2 input : %s", p2_dir)
    logger.info("Final output  : %s", out_dir)
    logger.info("Spine range   : %s → %s", args.start, args.end)

    spark = (
        SparkSession.builder
        .appName("NL_Energy_Phase3")
        .master(f"local[{args.workers}]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Build the temporal scaffold
        df = build_hourly_spine(spark, args.start, args.end)

        # 2. Join sources — ENTSO-E first (primary target), then features
        df = join_entsoe(df, os.path.join(p2_dir, "entsoe", "data"))
        df = join_viirs(df, os.path.join(p2_dir, "viirs_a2_daily", "data"))
        df = join_cbs(df, os.path.join(p2_dir, "cbs_combined", "data"))

        # 3. Reorder columns, drop internal join key
        df = df.select(_ordered_columns(df)).drop("date")

        # 4. Data quality summary
        total = df.count()
        entsoe_nn = df.filter(F.col("entsoe_load_mw").isNotNull()).count()
        viirs_nn = df.filter(F.col("ntl_mean").isNotNull()).count()
        cbs_nn = df.filter(F.col("cbs_total_energy_price_idx").isNotNull()).count()

        logger.info("Final: %d hourly rows.", total)
        logger.info(
            "Coverage — ENTSO-E: %.1f%% | VIIRS: %.1f%% | CBS: %.1f%%",
            100 * entsoe_nn / total, 100 * viirs_nn / total, 100 * cbs_nn / total,
        )

        # Coverage assertion for the target variable
        entsoe_pct = entsoe_nn / total if total else 0
        if entsoe_pct < 0.85:
            logger.error(
                "ENTSO-E coverage is only %.1f%% — check source data.",
                100 * entsoe_pct,
            )
            # Still write the file but flag the issue in the quality JSON
            coverage_warning = (
                f"ENTSO-E coverage below 85%: {100*entsoe_pct:.1f}%"
            )
        else:
            coverage_warning = None

        # 5. Write final Parquet, partitioned by year
        out_parquet = os.path.join(out_dir, "nl_hourly_dataset.parquet")
        (
            df.repartition("year")
              .write
              .mode("overwrite")
              .partitionBy("year")
              .parquet(out_parquet)
        )
        logger.info("Final dataset → %s", out_parquet)

        # 6. Quality JSON
        # Load statistics for the target variable
        load_stats_row = df.select(
            F.min("entsoe_load_mw").alias("min"),
            F.max("entsoe_load_mw").alias("max"),
            F.mean("entsoe_load_mw").alias("mean"),
            F.stddev("entsoe_load_mw").alias("stddev"),
        ).collect()[0]

        extra = {
            "spine": {
                "start": args.start,
                "end": args.end,
                "total_hours": total,
            },
            "source_coverage": {
                "entsoe_load_mw": {
                    "non_null": entsoe_nn,
                    "pct": round(100 * entsoe_nn / total, 2),
                },
                "ntl_mean": {
                    "non_null": viirs_nn,
                    "pct": round(100 * viirs_nn / total, 2),
                },
                "cbs_total_energy_price_idx": {
                    "non_null": cbs_nn,
                    "pct": round(100 * cbs_nn / total, 2),
                },
            },
            "load_statistics": {
                "min_mw": round(float(load_stats_row["min"]), 1) if load_stats_row["min"] else None,
                "max_mw": round(float(load_stats_row["max"]), 1) if load_stats_row["max"] else None,
                "mean_mw": round(float(load_stats_row["mean"]), 1) if load_stats_row["mean"] else None,
                "stddev_mw": round(float(load_stats_row["stddev"]), 1) if load_stats_row["stddev"] else None,
            },
        }
        if coverage_warning:
            extra["warnings"] = [coverage_warning]

        metrics = _compute_quality_metrics(
            spark, out_parquet, "nl_hourly_merged", 3, extra
        )
        _write_quality_json(metrics, out_dir)
        logger.info("=== Phase 3 complete. ===")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
