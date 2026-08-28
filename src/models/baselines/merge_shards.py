#!/usr/bin/env python3
"""Merge per-(model, horizon, seed) result shards into one combined CSV.

Each parallel SLURM array task owns an exclusive shard file
(``_shards/{model_name}_h{horizon}_s{seed}.csv``), so concurrent tasks
never contend for the same path and no cross-node file locking is
needed. This script performs the one-time, single-process merge of all
shards for a model into ``rocv_results_{model_name}.csv``.

Usage
-----
    python -m src.models.baselines.merge_shards --model-name ashtar_seq2seq
    python -m src.models.baselines.merge_shards --model-name van_de_sande_prophet_lstm --output-dir /path/to/results
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger("merge_shards")


def merge_shards(output_dir: Path, model_name: str) -> Optional[pd.DataFrame]:
    """Merge all shards for ``model_name`` under ``output_dir/_shards`` into
    ``output_dir/rocv_results_{model_name}.csv``. Returns the merged
    DataFrame, or None if no shards were found.
    """
    shard_dir = Path(output_dir) / "_shards"
    shard_paths = sorted(shard_dir.glob(f"{model_name}_h*_s*.csv"))
    if not shard_paths:
        logger.warning("No shards found for %s in %s.", model_name, shard_dir)
        return None

    frames = [pd.read_csv(p) for p in shard_paths]
    merged = pd.concat(frames, ignore_index=True)

    # Defensive de-dup: each shard is already unique per (horizon, seed),
    # but guard against a stray duplicate fold within one shard.
    merged = merged.drop_duplicates(subset=["fold", "horizon_days", "seed"], keep="last")
    merged = merged.sort_values(["horizon_days", "seed", "fold"]).reset_index(drop=True)

    csv_path = Path(output_dir) / f"rocv_results_{model_name}.csv"
    merged.to_csv(csv_path, index=False)

    json_path = Path(output_dir) / f"rocv_results_{model_name}.json"
    with open(json_path, "w") as f:
        json.dump(merged.to_dict("records"), f, indent=2, default=str)

    logger.info(
        "Merged %d shards → %d rows (%d horizons, %d seeds) → %s",
        len(shard_paths), len(merged),
        merged["horizon_days"].nunique(), merged["seed"].nunique(),
        csv_path,
    )
    return merged


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
                         datefmt="%H:%M:%S")
    parser = argparse.ArgumentParser(description="Merge per-task result shards into one CSV.")
    parser.add_argument("--model-name", type=str, required=True)
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        from . import config as C
        output_dir = C.OUTPUT_DIR

    merge_shards(output_dir, args.model_name)


if __name__ == "__main__":
    main()
