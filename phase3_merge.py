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
    - **VIIRS A2 daily** (daily): Left-join on ``date``.
    - **CBS Combined** (monthly): Left-join on ``(year, month)``.

Usage::

    python phase3_merge.py [--data-root /path/to/data]
                            [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                            [--force]
"""

import argparse
import json
import logging
import os
import shutil
import time
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

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
    derived from each timestamp.
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

    logger.info("  Spine: %d hourly rows.", len(spine))
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


def join_viirs(spine: pd.DataFrame, p2_viirs_path: str) -> pd.DataFrame:
    """Left-join VIIRS daily aggregates onto the spine by date."""
    viirs_cols = [
        "ntl_mean", "ntl_sum", "ntl_valid_count",
        "ntl_fill_count", "ntl_invalid_count",
    ]
    if not os.path.exists(p2_viirs_path):
        logger.warning("VIIRS P2 not found — columns will be null.")
        for c in viirs_cols:
            spine[c] = np.nan
        return spine

    viirs = pd.read_parquet(p2_viirs_path, columns=["date"] + viirs_cols)
    viirs["date"] = pd.to_datetime(viirs["date"]).dt.date
    logger.info("  Joining VIIRS daily (%d rows).", len(viirs))
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
        # VIIRS aggregates
        "ntl_mean", "ntl_sum",
        "ntl_valid_count", "ntl_fill_count", "ntl_invalid_count",
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

    # Temporal suffix
    preferred_tail = [
        "year", "month", "day", "hour",
        "day_of_week", "is_weekend",
        "day_of_year", "week_of_year", "quarter",
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

    for c in preferred_tail:
        if c in existing and c not in used:
            ordered.append(c)
            used.add(c)

    return ordered


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
    data_root = args.data_root
    p2_dir = os.path.join(data_root, "processing_2")
    out_dir = os.path.join(data_root, "processed")
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
    df = join_viirs(df, os.path.join(p2_dir, "viirs_a2_daily", "data"))
    df = join_cbs(df, os.path.join(p2_dir, "cbs_combined", "data"))

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
    entsoe_nn = int(df["entsoe_load_mw"].notna().sum())
    viirs_nn = int(df["ntl_mean"].notna().sum())
    cbs_energy_nn = int(df["cbs_gas_total_tax"].notna().sum()) if "cbs_gas_total_tax" in df.columns else 0
    cbs_gdp_nn = int(df["cbs_gdp_yy"].notna().sum()) if "cbs_gdp_yy" in df.columns else 0
    load = df["entsoe_load_mw"].dropna()

    logger.info("Final: %d hourly rows.", total)
    logger.info(
        "Coverage — ENTSO-E: %.1f%% | VIIRS: %.1f%% | CBS Tariffs: %.1f%% | CBS GDP: %.1f%%",
        100 * entsoe_nn / total, 100 * viirs_nn / total,
        100 * cbs_energy_nn / total, 100 * cbs_gdp_nn / total,
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
            "cbs_gas_total_tax": {
                "non_null": cbs_energy_nn,
                "pct": round(100 * cbs_energy_nn / total, 2) if total else 0.0,
            },
            "cbs_gdp_yy": {
                "non_null": cbs_gdp_nn,
                "pct": round(100 * cbs_gdp_nn / total, 2) if total else 0.0,
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