#!/usr/bin/env python3
"""Post-processing: aggregate and compare results from all baseline models.

Produces:
    1. Unified CSV with all models' per-fold results
    2. Per-horizon summary table (mean ± std across folds) for all 7 metrics
    3. LaTeX-formatted comparison table for the thesis
    4. JSON summary for downstream consumption
    5. Metric evolution plots (re-generated from saved CSVs)

Usage
-----
    python -u src/models/baselines/aggregate_results.py
    python -u src/models/baselines/aggregate_results.py --results-dir /path/to/results
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.models.baselines import config as C
from src.models.baselines.evaluation import (
    METRIC_COLUMNS,
    plot_metric_evolution,
    summarise_by_horizon,
)

logging.basicConfig(
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("aggregate_results")

# Pretty model names for LaTeX
_MODEL_LABELS = {
    "van_de_sande_mlr": "MLR",
    "van_de_sande_prophet_lstm": "Prophet-LSTM",
    "ashtar_sarimax": "SARIMAX",
    "ashtar_sarimax_lstm": "SARIMAX-LSTM",
    "ashtar_seq2seq": "Seq2Seq",
    "curiel_stacked_prophet_qlstm": "Prophet-QLSTM-XGB",
}


def load_all_results(results_dir: Path) -> pd.DataFrame:
    """Load per-model result CSVs and merge."""
    frames = []
    for model_name in C.ALL_MODELS:
        csv_path = results_dir / f"rocv_results_{model_name}.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            frames.append(df)
            logger.info("Loaded %d rows from %s", len(df), csv_path.name)
        else:
            logger.warning("Missing: %s", csv_path)

    if not frames:
        logger.error("No result files found in %s", results_dir)
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    return combined


def per_horizon_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Compute mean ± std of each metric per model per horizon."""
    if df.empty:
        return pd.DataFrame()

    agg_spec = {}
    for m in METRIC_COLUMNS:
        if m in df.columns:
            agg_spec[f"{m}_mean"] = (m, "mean")
            agg_spec[f"{m}_std"] = (m, "std")
    agg_spec["n_folds"] = ("fold", "count")
    agg_spec["total_elapsed_s"] = ("elapsed_s", "sum")

    return (
        df.groupby(["model", "horizon_days"])
        .agg(**agg_spec)
        .round(4)
        .reset_index()
    )


def generate_latex_table(summary: pd.DataFrame) -> str:
    """Generate a LaTeX table with all 7 metrics.

    Format: Model | Horizon | RMSE | MAE | MAPE | MASE | nMAE | nRMSE | PCC
    """
    if summary.empty:
        return ""

    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        r"\caption{Baseline model comparison — ROCV results (mean $\pm$ std across folds)}",
        r"\label{tab:baseline_results}",
        r"\footnotesize",
        r"\setlength{\tabcolsep}{3pt}",
        r"\begin{tabular}{llrrrrrrr}",
        r"\toprule",
        r"Model & H (d) & RMSE (MW) & MAE (MW) & MAPE (\%) & MASE & nMAE & nRMSE & PCC \\",
        r"\midrule",
    ]

    prev_model = None
    for _, row in summary.iterrows():
        model_key = row["model"]
        model_label = _MODEL_LABELS.get(model_key, model_key)
        horizon = int(row["horizon_days"])

        def _fmt(mean_col, std_col, precision=1):
            m = row.get(mean_col, np.nan)
            s = row.get(std_col, np.nan)
            if np.isnan(m):
                return "---"
            if np.isnan(s):
                return f"{m:.{precision}f}"
            return f"{m:.{precision}f} $\\pm$ {s:.{precision}f}"

        rmse_str = _fmt("rmse_mean", "rmse_std", 1)
        mae_str = _fmt("mae_mean", "mae_std", 1)
        mape_str = _fmt("mape_pct_mean", "mape_pct_std", 2)
        mase_str = _fmt("mase_mean", "mase_std", 3)
        nmae_str = _fmt("nmae_mean", "nmae_std", 4)
        nrmse_str = _fmt("nrmse_mean", "nrmse_std", 4)
        pcc_str = _fmt("pcc_mean", "pcc_std", 4)

        if prev_model is not None and model_key != prev_model:
            lines.append(r"\midrule")
        prev_model = model_key

        lines.append(
            f"{model_label} & {horizon} & {rmse_str} & {mae_str} & "
            f"{mape_str} & {mase_str} & {nmae_str} & {nrmse_str} & {pcc_str} \\\\"
        )

    lines.extend([
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ])

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Fold × Horizon comparison across all models
# ═══════════════════════════════════════════════════════════════════════════

# For lower-is-better metrics, the minimum across models is "best".
# For higher-is-better (PCC), the maximum is "best".
_LOWER_IS_BETTER = {"rmse", "mae", "mape_pct", "mase", "nmae", "nrmse"}
_HIGHER_IS_BETTER = {"pcc"}


def generate_fold_horizon_comparison(combined: pd.DataFrame, results_dir: Path):
    """Generate per-fold × per-horizon comparison tables.

    For each (fold, horizon_days) pair, produce a row per model with all 7
    metrics.  The best value in each metric column across models is
    highlighted in **bold** (LaTeX ``\\textbf{}``, CSV with ``*`` suffix).

    Saves:
        - ``comparison_fold_horizon.csv``  — flat CSV with best markers
        - ``comparison_fold_horizon.tex``  — LaTeX table
        - ``comparison_by_horizon_mean.csv`` — mean across folds per model
    """
    if combined.empty:
        logger.warning("No data for fold×horizon comparison.")
        return

    metrics = [m for m in METRIC_COLUMNS if m in combined.columns]
    if not metrics:
        logger.warning("No metric columns found.")
        return

    # ── 1. Flat comparison CSV ────────────────────────────────────────
    pivot_rows = []
    for (fold, h_days), grp in combined.groupby(["fold", "horizon_days"]):
        for _, row in grp.iterrows():
            pivot_rows.append({
                "fold": int(fold),
                "cutoff": row.get("cutoff", ""),
                "horizon_days": int(h_days),
                "model": row["model"],
                **{m: row.get(m, np.nan) for m in metrics},
            })

    comparison = pd.DataFrame(pivot_rows)

    # Mark best per (fold, horizon) group
    for (fold, h_days), grp in comparison.groupby(["fold", "horizon_days"]):
        for m in metrics:
            vals = grp[m].dropna()
            if vals.empty:
                continue
            if m in _LOWER_IS_BETTER:
                best_idx = vals.idxmin()
            else:
                best_idx = vals.idxmax()
            comparison.loc[best_idx, f"{m}_best"] = True

    comparison.to_csv(results_dir / "comparison_fold_horizon.csv", index=False)
    logger.info("Fold×horizon comparison CSV saved (%d rows).", len(comparison))

    # ── 2. Mean across folds per model × horizon ─────────────────────
    mean_cols = {m: (m, "mean") for m in metrics}
    mean_cols["n_folds"] = ("fold", "count")
    mean_df = (
        combined.groupby(["model", "horizon_days"])
        .agg(**mean_cols)
        .round(4)
        .reset_index()
    )

    # Mark best model per horizon
    for h_days, grp in mean_df.groupby("horizon_days"):
        for m in metrics:
            vals = grp[m].dropna()
            if vals.empty:
                continue
            if m in _LOWER_IS_BETTER:
                best_idx = vals.idxmin()
            else:
                best_idx = vals.idxmax()
            mean_df.loc[best_idx, f"{m}_best"] = True

    mean_df.to_csv(results_dir / "comparison_by_horizon_mean.csv", index=False)
    logger.info("Mean comparison CSV saved (%d rows).", len(mean_df))

    # ── 3. LaTeX table (mean per horizon, best in bold) ──────────────
    latex_lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        r"\caption{Cross-model comparison — mean metric values per horizon (best in \textbf{bold})}",
        r"\label{tab:cross_model_comparison}",
        r"\footnotesize",
        r"\setlength{\tabcolsep}{3pt}",
        r"\begin{tabular}{llrrrrrrr}",
        r"\toprule",
        r"Model & H (d) & RMSE (MW) & MAE (MW) & MAPE (\%) & MASE & nMAE & nRMSE & PCC \\",
        r"\midrule",
    ]

    prev_horizon = None
    for _, row in mean_df.sort_values(["horizon_days", "model"]).iterrows():
        model_label = _MODEL_LABELS.get(row["model"], row["model"])
        h = int(row["horizon_days"])

        if prev_horizon is not None and h != prev_horizon:
            latex_lines.append(r"\midrule")
        prev_horizon = h

        def _fmt(m, precision=2):
            val = row.get(m, np.nan)
            if np.isnan(val):
                return "---"
            s = f"{val:.{precision}f}"
            if row.get(f"{m}_best", False):
                return f"\\textbf{{{s}}}"
            return s

        latex_lines.append(
            f"{model_label} & {h} & "
            f"{_fmt('rmse', 1)} & {_fmt('mae', 1)} & {_fmt('mape_pct', 2)} & "
            f"{_fmt('mase', 3)} & {_fmt('nmae', 4)} & {_fmt('nrmse', 4)} & "
            f"{_fmt('pcc', 4)} \\\\"
        )

    latex_lines.extend([
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ])

    latex_path = results_dir / "cross_model_comparison.tex"
    with open(latex_path, "w") as f:
        f.write("\n".join(latex_lines))
    logger.info("LaTeX cross-model table → %s", latex_path)

    # ── 4. Print formatted console table ─────────────────────────────
    logger.info("\n" + "=" * 90)
    logger.info("  CROSS-MODEL COMPARISON (mean across folds)")
    logger.info("=" * 90)

    # Print per horizon
    for h_days in sorted(mean_df["horizon_days"].unique()):
        h_grp = mean_df[mean_df["horizon_days"] == h_days].copy()
        logger.info("\n  Horizon = %d days:", h_days)
        for _, row in h_grp.iterrows():
            label = _MODEL_LABELS.get(row["model"], row["model"])
            parts = []
            for m in metrics:
                val = row.get(m, np.nan)
                best = row.get(f"{m}_best", False)
                if np.isnan(val):
                    parts.append(f"{m}=---")
                else:
                    marker = " *" if best else ""
                    parts.append(f"{m}={val:.4f}{marker}")
            logger.info("    %-20s  %s", label, "  ".join(parts))


def main():
    parser = argparse.ArgumentParser(description="Aggregate baseline results.")
    parser.add_argument(
        "--results-dir", type=str, default=None,
        help=f"Results directory. Default: {C.OUTPUT_DIR}",
    )
    parser.add_argument(
        "--no-plots", action="store_true",
        help="Skip plot generation.",
    )
    args = parser.parse_args()

    results_dir = Path(args.results_dir) if args.results_dir else C.OUTPUT_DIR

    # Load
    combined = load_all_results(results_dir)
    if combined.empty:
        return

    # Save combined
    combined.to_csv(results_dir / "rocv_results_all_models.csv", index=False)
    logger.info("Combined CSV: %d rows", len(combined))

    # Summary
    summary = per_horizon_summary(combined)
    summary.to_csv(results_dir / "rocv_summary_by_horizon.csv", index=False)
    logger.info("Summary CSV saved.")

    # LaTeX
    latex = generate_latex_table(summary)
    latex_path = results_dir / "baseline_comparison_table.tex"
    with open(latex_path, "w") as f:
        f.write(latex)
    logger.info("LaTeX table → %s", latex_path)

    # JSON summary
    json_summary = {"models": []}
    for model_name in summary["model"].unique():
        model_df = summary[summary["model"] == model_name]
        cols_for_json = ["horizon_days"] + \
            [f"{m}_mean" for m in METRIC_COLUMNS if f"{m}_mean" in model_df.columns] + \
            ["n_folds"]
        json_summary["models"].append({
            "name": model_name,
            "horizons": model_df[cols_for_json].to_dict("records"),
        })

    with open(results_dir / "rocv_summary.json", "w") as f:
        json.dump(json_summary, f, indent=2, default=str)

    # ── Cross-model fold × horizon comparison ────────────────────────
    generate_fold_horizon_comparison(combined, results_dir)

    # Plots
    if not args.no_plots:
        for model_name in combined["model"].unique():
            model_df = combined[combined["model"] == model_name]
            try:
                plot_metric_evolution(model_df, model_name, results_dir)
            except Exception as e:
                logger.warning("Plotting failed for %s: %s", model_name, e)

    # Print
    logger.info("\n" + "=" * 80)
    logger.info("  BASELINE COMPARISON SUMMARY")
    logger.info("=" * 80)
    logger.info("\n%s", summary.to_string(index=False))


if __name__ == "__main__":
    main()
