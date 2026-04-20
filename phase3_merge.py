#!/usr/bin/env python3
"""Phase 3: Merge all Phase 2 outputs into a unified hourly dataset.

Final stage of the three-phase NL energy demand data pipeline. Reads the
aggregated/combined Parquet files from ``data/processing_2/`` and joins
them onto a canonical hourly UTC timestamp spine spanning
2012-01-01 to today.

The final dataset is written to ``data/processed/nl_hourly_dataset.parquet``,
partitioned by year, with a comprehensive ``data_quality.json``.

Join strategy:
    - **ENTSO-E** (hourly): Equi-join on ``timestamp`` (broadcast — small).
    - **VIIRS A2 daily** (daily): Broadcast-join on ``date``.
    - **CBS Combined** (monthly): Broadcast-join on ``(year, month)``.

Optimised for Snellius: AQE, node-local scratch, UTC session timezone,
single-pass quality stats, pandas-based quality metrics, and
checkpointing via Spark's ``_SUCCESS`` marker.

Usage::

    python phase3_merge.py [--data-root /path/to/data]
                            [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                            [--workers N] [--force]
"""

import argparse
import json
import logging
import os
import shutil
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("phase3_merge")


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def _stage_done(parquet_path: str) -> bool:
    """Return True if a previous Spark write completed successfully."""
    return os.path.exists(os.path.join(parquet_path, "_SUCCESS"))


def _clear_path(path: str) -> None:
    """Remove a Parquet directory (used under --force)."""
    if os.path.isdir(path):
        shutil.rmtree(path)


def _scratch_dir() -> str:
    """Return the best node-local scratch directory for Spark shuffle spill."""
    for var in ("SLURM_TMPDIR", "TMPDIR"):
        v = os.environ.get(var)
        if v and os.path.isdir(v):
            return v
    return "/tmp"


# ---------------------------------------------------------------------------
# Data quality helpers — pandas-based (final dataset is ~115 k rows)
# ---------------------------------------------------------------------------


def _compute_quality_metrics(
    parquet_path: str,
    source_name: str,
    phase: int,
    extra_metrics: Optional[dict] = None,
) -> dict:
    """Compute data quality metrics for the final Parquet output.

    Uses pandas — the full hourly dataset is ~115 k rows × ~20 columns,
    which loads instantly and avoids a redundant Spark job.
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


def build_hourly_spine(
    spark: SparkSession, start_date: str, end_date: str
) -> DataFrame:
    """Generate a contiguous hourly UTC timestamp spine.

    Creates one row per hour for the closed interval
    [start_date 00:00 UTC, end_date 23:00 UTC], providing a complete
    temporal scaffold. Any missing source data appears as explicit nulls
    after left-joining.

    Timezone: the Spark session timezone is set to UTC in main() so the
    ``F.to_timestamp`` calls below produce genuine UTC instants that match
    the UTC-naive timestamps from the Phase 1/2 ENTSO-E extraction.
    """
    logger.info("Building hourly spine: %s → %s (UTC)", start_date, end_date)

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

    return spine


# ---------------------------------------------------------------------------
# Join functions — no intermediate .count() calls; stats computed once at end
# ---------------------------------------------------------------------------


def join_entsoe(spine: DataFrame, p2_entsoe_path: str) -> DataFrame:
    """Left-join ENTSO-E hourly load onto the timestamp spine.

    ENTSO-E is broadcast-joined: at ~140 k rows × 2 columns (~3 MB
    serialized), it fits comfortably under the autoBroadcast threshold,
    but the explicit hint avoids a shuffle if AQE guesses wrong.
    """
    if not os.path.exists(p2_entsoe_path):
        logger.warning("ENTSO-E P2 not found — column will be null.")
        return spine.withColumn("entsoe_load_mw", F.lit(None).cast(T.DoubleType()))

    spark = spine.sparkSession
    entsoe = spark.read.parquet(p2_entsoe_path).select(
        F.col("timestamp_utc").alias("timestamp"),
        "entsoe_load_mw",
    )
    logger.info("  Joining ENTSO-E (broadcast).")
    return spine.join(F.broadcast(entsoe), on="timestamp", how="left")


def join_viirs(spine: DataFrame, p2_viirs_path: str) -> DataFrame:
    """Broadcast-left-join VIIRS daily aggregates onto the spine by date."""
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
    logger.info("  Joining VIIRS daily (broadcast).")
    return spine.join(F.broadcast(viirs), on="date", how="left")


def join_cbs(spine: DataFrame, p2_cbs_path: str) -> DataFrame:
    """Broadcast-left-join CBS combined (monthly) onto the spine."""
    cbs_cols = [
        "cbs_total_energy_price_idx", "cbs_natural_gas_price_idx",
        "cbs_crude_oil_price_idx", "cbs_electricity_price_idx",
        "cbs_gdp_million_eur", "cbs_population_million",
    ]
    if not os.path.exists(p2_cbs_path):
        logger.warning("CBS P2 not found — columns will be null.")
        for c in cbs_cols:
            spine = spine.withColumn(c, F.lit(None).cast(T.DoubleType()))
        return spine

    spark = spine.sparkSession
    cbs = spark.read.parquet(p2_cbs_path)
    logger.info("  Joining CBS combined (broadcast).")
    return spine.join(F.broadcast(cbs), on=["year", "month"], how="left")


# ---------------------------------------------------------------------------
# Column ordering
# ---------------------------------------------------------------------------


def _ordered_columns(df: DataFrame) -> list:
    """Return columns in a deterministic semantic order.

    Order: timestamp → target → satellite → economic → temporal → internal.
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
        help="Root data directory.",
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
        help="Re-run even if the final output already exists.",
    )
    return parser.parse_args()


def main() -> None:
    """Build the final hourly dataset from Phase 2 outputs."""
    args = _parse_args()
    data_root = args.data_root
    p2_dir = os.path.join(data_root, "processing_2")
    out_dir = os.path.join(data_root, "processed")
    out_parquet = os.path.join(out_dir, "nl_hourly_dataset.parquet")
    os.makedirs(out_dir, exist_ok=True)
    scratch = _scratch_dir()

    logger.info("Phase 2 input : %s", p2_dir)
    logger.info("Final output  : %s", out_parquet)
    logger.info("Spine range   : %s → %s", args.start, args.end)
    logger.info("Workers       : %d (of %d CPUs)",
                args.workers, os.cpu_count() or 0)
    logger.info("Driver memory : %s", args.driver_memory)
    logger.info("Spark scratch : %s", scratch)
    logger.info("Force rerun   : %s", args.force)

    if _stage_done(out_parquet) and not args.force:
        logger.info("Final output already present — nothing to do "
                    "(use --force to re-run).")
        return

    if args.force:
        _clear_path(out_parquet)

    t_total = time.time()

    # --- Spark config tuned for a single Snellius node ---
    # - UTC session timezone: guarantees F.to_timestamp() produces UTC
    #   instants that match ENTSO-E's UTC-naive timestamps.
    # - AQE: auto-coalesces shuffle partitions, handles skew.
    # - spark.local.dir on node-local SSD — shuffle on GPFS is disastrous.
    # - autoBroadcastJoinThreshold raised to 50 MB so all three source
    #   tables get auto-broadcast even without explicit hints.
    spark = (
        SparkSession.builder
        .appName("NL_Energy_Phase3")
        .master(f"local[{args.workers}]")
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.local.dir", scratch)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(args.workers * 4))
        .config("spark.default.parallelism", str(args.workers * 2))
        .config("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 1. Build the temporal scaffold (lazy — no count() yet)
        df = build_hourly_spine(spark, args.start, args.end)

        # 2. Join sources — each is a broadcast join, no shuffle.
        df = join_entsoe(df, os.path.join(p2_dir, "entsoe", "data"))
        df = join_viirs(df, os.path.join(p2_dir, "viirs_a2_daily", "data"))
        df = join_cbs(df, os.path.join(p2_dir, "cbs_combined", "data"))

        # 3. Reorder columns, drop internal join key
        df = df.select(_ordered_columns(df)).drop("date")

        # 4. Write partitioned by year. Repartitioning by year yields one
        #    file per year; sorting within partition lets Parquet's
        #    row-group min/max indexes prune time-range queries downstream.
        (
            df.repartition("year")
              .sortWithinPartitions("timestamp")
              .write
              .mode("overwrite")
              .partitionBy("year")
              .parquet(out_parquet)
        )
        logger.info("Final dataset written → %s", out_parquet)

        # 5. Quality + coverage — pandas pass over the written output.
        #    One read, one set of stats, no extra Spark jobs.
        result = pd.read_parquet(out_parquet)
        total = len(result)
        entsoe_nn = int(result["entsoe_load_mw"].notna().sum())
        viirs_nn = int(result["ntl_mean"].notna().sum())
        cbs_nn = int(result["cbs_total_energy_price_idx"].notna().sum())
        load = result["entsoe_load_mw"].dropna()

        logger.info("Final: %d hourly rows.", total)
        logger.info(
            "Coverage — ENTSO-E: %.1f%% | VIIRS: %.1f%% | CBS: %.1f%%",
            100 * entsoe_nn / total, 100 * viirs_nn / total, 100 * cbs_nn / total,
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
                "ntl_mean": {
                    "non_null": viirs_nn,
                    "pct": round(100 * viirs_nn / total, 2) if total else 0.0,
                },
                "cbs_total_energy_price_idx": {
                    "non_null": cbs_nn,
                    "pct": round(100 * cbs_nn / total, 2) if total else 0.0,
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

    finally:
        spark.stop()


if __name__ == "__main__":
    main()