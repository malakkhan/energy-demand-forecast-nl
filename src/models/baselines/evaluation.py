"""Evaluation metrics, results persistence, and visualisation.

All seven standard metrics are computed on **original-scale (MW)** values
regardless of the loss function used during model training:

    ┌────────┬────────────────────────────────────────────────────────┐
    │ Metric │ Definition                                             │
    ├────────┼────────────────────────────────────────────────────────┤
    │ RMSE   │ √(mean(ε²))                                           │
    │ MAE    │ mean(|ε|)                                              │
    │ MAPE   │ mean(|ε / y|) × 100  (%)                              │
    │ MASE   │ MAE / MAE_naive      (naive = 24-h persistence)       │
    │ nMAE   │ MAE / mean(y)                                         │
    │ nRMSE  │ RMSE / mean(y)                                        │
    │ PCC    │ Pearson Correlation Coefficient                       │
    └────────┴────────────────────────────────────────────────────────┘

    where ε = y_pred − y_true  and  y = y_true.

Also provides:
    - Incremental CSV checkpoint saving (crash-safe for long SLURM jobs)
    - Per-horizon aggregation (mean ± std across folds)
    - Multi-model comparison table
    - **Metric evolution plots**: grouped line-charts showing how each
      metric changes across ROCV folds (one PNG per model per horizon).
"""

import json
import logging
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from scipy.stats import pearsonr

logger = logging.getLogger("baselines.evaluation")

# Metric column names — used across the pipeline for consistency.
METRIC_COLUMNS = ["rmse", "mae", "mape_pct", "mase", "nmae", "nrmse", "pcc"]


# ═══════════════════════════════════════════════════════════════════════════
# Metric container
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ForecastMetrics:
    """Container for all seven evaluation metrics."""
    rmse: float
    mae: float
    mape_pct: float     # MAPE as percentage (0–100)
    mase: float         # Mean Absolute Scaled Error (vs 24-h persistence)
    nmae: float         # Normalised MAE  (MAE / mean(y_true))
    nrmse: float        # Normalised RMSE (RMSE / mean(y_true))
    pcc: float          # Pearson Correlation Coefficient
    n_samples: int

    def to_dict(self) -> Dict[str, float]:
        return {
            "rmse": round(self.rmse, 2),
            "mae": round(self.mae, 2),
            "mape_pct": round(self.mape_pct, 2),
            "mase": round(self.mase, 4),
            "nmae": round(self.nmae, 4),
            "nrmse": round(self.nrmse, 4),
            "pcc": round(self.pcc, 4),
            "n_samples": self.n_samples,
        }


# ═══════════════════════════════════════════════════════════════════════════
# Core metric computation
# ═══════════════════════════════════════════════════════════════════════════

def compute_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    naive_season: int = 24,
) -> ForecastMetrics:
    """Compute all seven evaluation metrics on original-scale (MW) values.

    Parameters
    ----------
    y_true : 1-D array of actual load values (MW, inverse-transformed).
    y_pred : 1-D array of predicted load values (MW, inverse-transformed).
    naive_season : Seasonal period for the naïve persistence baseline
        used in MASE.  Default ``24`` = "same hour yesterday".
    """
    y_true = np.asarray(y_true, dtype=np.float64).ravel()
    y_pred = np.asarray(y_pred, dtype=np.float64).ravel()

    # Drop NaN pairs
    mask = np.isfinite(y_true) & np.isfinite(y_pred)
    y_true, y_pred = y_true[mask], y_pred[mask]
    n = len(y_true)

    if n == 0:
        return ForecastMetrics(
            rmse=np.nan, mae=np.nan, mape_pct=np.nan, mase=np.nan,
            nmae=np.nan, nrmse=np.nan, pcc=np.nan, n_samples=0,
        )

    errors = y_pred - y_true
    abs_errors = np.abs(errors)
    mean_y = float(np.mean(y_true))

    # ── RMSE ──
    rmse = float(np.sqrt(np.mean(errors ** 2)))

    # ── MAE ──
    mae = float(np.mean(abs_errors))

    # ── MAPE (%) ──
    nonzero = np.abs(y_true) > 1.0
    if nonzero.sum() > 0:
        mape_pct = float(
            np.mean(np.abs(errors[nonzero] / y_true[nonzero])) * 100
        )
    else:
        mape_pct = np.nan

    # ── MASE (vs 24-h persistence baseline) ──
    if n > naive_season:
        naive_errors = np.abs(y_true[naive_season:] - y_true[:-naive_season])
        mae_naive = float(np.mean(naive_errors))
        mase = mae / mae_naive if mae_naive > 0 else np.nan
    else:
        mase = np.nan

    # ── nMAE (normalised by mean load) ──
    nmae = mae / mean_y if mean_y > 0 else np.nan

    # ── nRMSE (normalised by mean load) ──
    nrmse = rmse / mean_y if mean_y > 0 else np.nan

    # ── PCC (Pearson Correlation Coefficient) ──
    if np.std(y_true) > 0 and np.std(y_pred) > 0:
        pcc, _ = pearsonr(y_true, y_pred)
        pcc = float(pcc)
    else:
        pcc = np.nan

    return ForecastMetrics(
        rmse=rmse, mae=mae, mape_pct=mape_pct, mase=mase,
        nmae=nmae, nrmse=nrmse, pcc=pcc, n_samples=n,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Results persistence
# ═══════════════════════════════════════════════════════════════════════════

def save_fold_result(
    result_row: Dict,
    results_list: List[Dict],
    output_dir: Path,
    model_name: str,
) -> None:
    """Append a fold result and persist incrementally (crash-safe)."""
    results_list.append(result_row)
    # Overwrite each time — acts as a checkpoint
    out_csv = output_dir / f"rocv_results_{model_name}.csv"
    pd.DataFrame(results_list).to_csv(out_csv, index=False)


def save_final_results(
    results: List[Dict],
    output_dir: Path,
    model_name: str,
) -> pd.DataFrame:
    """Save final CSV + JSON for one model, return the DataFrame."""
    results_df = pd.DataFrame(results)

    out_csv = output_dir / f"rocv_results_{model_name}.csv"
    results_df.to_csv(out_csv, index=False)
    logger.info("Results CSV → %s", out_csv)

    out_json = output_dir / f"rocv_results_{model_name}.json"
    with open(out_json, "w") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info("Results JSON → %s", out_json)

    return results_df


# ═══════════════════════════════════════════════════════════════════════════
# Per-horizon aggregation
# ═══════════════════════════════════════════════════════════════════════════

def summarise_by_horizon(results_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate all seven metrics across folds for each horizon."""
    if results_df.empty:
        return pd.DataFrame()

    agg_spec = {}
    for m in METRIC_COLUMNS:
        if m in results_df.columns:
            agg_spec[f"{m}_mean"] = (m, "mean")
            agg_spec[f"{m}_std"] = (m, "std")
    agg_spec["n_folds"] = ("fold", "count")

    return (
        results_df
        .groupby("horizon_days")
        .agg(**agg_spec)
        .round(4)
    )


def combine_model_results(
    output_dir: Path,
    model_names: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Load and merge results from multiple models into one table."""
    from . import config as C
    model_names = model_names or C.ALL_MODELS

    frames = []
    for name in model_names:
        csv_path = output_dir / f"rocv_results_{name}.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            frames.append(df)
            logger.info("Loaded %d rows from %s", len(df), csv_path.name)

    if not frames:
        logger.warning("No result files found in %s", output_dir)
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined.to_csv(output_dir / "rocv_results_all_models.csv", index=False)
    return combined


def print_summary_table(
    results_df: pd.DataFrame,
    model_name: str,
) -> None:
    """Log a formatted summary of per-horizon metrics."""
    summary = summarise_by_horizon(results_df)
    if summary.empty:
        return
    logger.info(
        "\n%s\n  SUMMARY — %s\n%s\n%s",
        "=" * 70, model_name, "=" * 70, summary.to_string(),
    )


# ═══════════════════════════════════════════════════════════════════════════
# Visualisation — Metric evolution over ROCV folds
# ═══════════════════════════════════════════════════════════════════════════

# Define metric groups for co-plotting (same-scale metrics share a panel).
_METRIC_GROUPS = [
    {
        "title": "Absolute Error Metrics (MW)",
        "suffix": "absolute",
        "metrics": [
            ("rmse", "RMSE", "#e63946"),     # red
            ("mae", "MAE", "#457b9d"),        # blue
        ],
        "ylabel": "MW",
    },
    {
        "title": "Normalised Error Metrics (ratio)",
        "suffix": "normalised",
        "metrics": [
            ("nrmse", "nRMSE", "#e76f51"),   # coral
            ("nmae", "nMAE", "#2a9d8f"),      # teal
        ],
        "ylabel": "ratio (metric / mean load)",
    },
    {
        "title": "Percentage & Scaled Error",
        "suffix": "pct_scaled",
        "metrics": [
            ("mape_pct", "MAPE (%)", "#f4a261"),  # orange
            ("mase", "MASE", "#264653"),            # dark blue
        ],
        "ylabel": "value",
    },
    {
        "title": "Correlation",
        "suffix": "correlation",
        "metrics": [
            ("pcc", "PCC", "#6a4c93"),   # purple
        ],
        "ylabel": "Pearson r",
    },
]

# Pretty model name map
_MODEL_LABELS = {
    "van_de_sande_mlr": "MLR",
    "van_de_sande_prophet_lstm": "Prophet-LSTM",
    "ashtar_sarimax": "SARIMAX",
    "ashtar_sarimax_lstm": "SARIMAX-LSTM",
    "ashtar_seq2seq": "Seq2Seq",
    "curiel_stacked_prophet_qlstm": "Prophet-QLSTM-XGB",
}


def plot_metric_evolution(
    results_df: pd.DataFrame,
    model_name: str,
    output_dir: Path,
) -> List[Path]:
    """Create and save metric-evolution plots for one model.

    For each forecast horizon, generates grouped line-charts showing
    how each metric evolves as the ROCV origin advances through time.
    Metrics that share the same scale are grouped into the same panel.

    Plots are saved as PNG files under ``output_dir/plots/``.

    Returns
    -------
    List of file paths for the saved figures.
    """
    import matplotlib
    matplotlib.use("Agg")          # non-interactive backend for HPC
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mtick

    if results_df.empty:
        logger.warning("No results to plot for %s.", model_name)
        return []

    plot_dir = output_dir / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    label = _MODEL_LABELS.get(model_name, model_name)
    horizons = sorted(results_df["horizon_days"].unique())
    saved = []

    for h_days in horizons:
        h_df = (
            results_df[results_df["horizon_days"] == h_days]
            .sort_values("fold")
            .copy()
        )
        if h_df.empty:
            continue

        # x-axis: fold cutoff dates
        x_labels = h_df["cutoff"].astype(str).tolist()
        x = np.arange(len(x_labels))

        n_groups = len(_METRIC_GROUPS)
        fig, axes = plt.subplots(
            n_groups, 1,
            figsize=(max(8, len(x_labels) * 1.2), 3.2 * n_groups),
            sharex=True,
        )
        if n_groups == 1:
            axes = [axes]

        fig.suptitle(
            f"{label}  —  Horizon {h_days} d  ({h_days * 24} h)\n"
            f"Metric Evolution across ROCV Folds",
            fontsize=13, fontweight="bold", y=0.995,
        )

        for ax, group in zip(axes, _METRIC_GROUPS):
            for metric_col, metric_label, colour in group["metrics"]:
                if metric_col not in h_df.columns:
                    continue
                vals = h_df[metric_col].values
                ax.plot(
                    x, vals,
                    marker="o", markersize=5, linewidth=1.8,
                    color=colour, label=metric_label,
                )
                # Annotate each point
                for xi, vi in zip(x, vals):
                    if np.isfinite(vi):
                        ax.annotate(
                            f"{vi:.2f}", (xi, vi),
                            textcoords="offset points", xytext=(0, 8),
                            fontsize=7, ha="center", color=colour,
                        )

            ax.set_ylabel(group["ylabel"], fontsize=10)
            ax.set_title(group["title"], fontsize=10, loc="left", pad=4)
            ax.legend(loc="best", fontsize=8, framealpha=0.9)
            ax.grid(True, alpha=0.3, linestyle="--")
            ax.tick_params(labelsize=8)

        # x-axis labels on the bottom panel
        axes[-1].set_xticks(x)
        axes[-1].set_xticklabels(x_labels, rotation=35, ha="right", fontsize=8)
        axes[-1].set_xlabel("ROCV Fold Cutoff", fontsize=10)

        fig.tight_layout(rect=[0, 0, 1, 0.96])

        fname = f"metrics_evolution_{model_name}_h{h_days}d.png"
        fpath = plot_dir / fname
        fig.savefig(fpath, dpi=150, bbox_inches="tight")
        plt.close(fig)
        saved.append(fpath)
        logger.info("  Plot saved → %s", fpath)

    return saved
