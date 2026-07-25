#!/usr/bin/env python3
"""Generate a comprehensive LaTeX longtable of baseline results.

Reads the per-model CSV results, identifies models with all 99 fold/horizon
combos complete (11 folds × 9 horizons), and produces a LaTeX longtable where:
- Each row = one horizon + fold combination
- Each column = one model
- Each cell = RMSE / MAPE / MAE / PCC  (stacked in a minipage for readability)
- Best value per metric per row is bolded
"""

import pandas as pd
import numpy as np
from pathlib import Path

RESULTS_DIR = Path(__file__).parent

# ---------------------------------------------------------------------------
# 1. Load all result CSVs
# ---------------------------------------------------------------------------
df_all = pd.read_csv(RESULTS_DIR / "rocv_results_all_models.csv")
df_seq2seq = pd.read_csv(RESULTS_DIR / "rocv_results_ashtar_seq2seq.csv")
df_sarimax = pd.read_csv(RESULTS_DIR / "rocv_results_ashtar_sarimax_lstm.csv")

# Combine all
df = pd.concat([df_all, df_seq2seq, df_sarimax], ignore_index=True)

# Standardise column names
if "pearson_r" in df.columns and "pcc" not in df.columns:
    df["pcc"] = df["pearson_r"]
elif "pearson_r" in df.columns:
    df["pcc"] = df["pcc"].fillna(df["pearson_r"])

# ---------------------------------------------------------------------------
# 2. Identify models with all 99 combos
# ---------------------------------------------------------------------------
HORIZONS = list(range(60, 181, 15))
FOLDS = list(range(11))
EXPECTED = len(HORIZONS) * len(FOLDS)  # 99

model_counts = {}
for model_name in df["model"].unique():
    sub = df[df["model"] == model_name]
    combos = set()
    for _, row in sub.iterrows():
        combos.add((int(row["horizon_days"]), int(row["fold"])))
    model_counts[model_name] = len(combos)

complete_models = [m for m, c in model_counts.items() if c >= EXPECTED]
complete_models.sort()

print("Model completion status:")
for m, c in sorted(model_counts.items()):
    status = "✓ COMPLETE" if c >= EXPECTED else f"  INCOMPLETE ({c}/{EXPECTED})"
    print(f"  {m}: {c} combos {status}")
print(f"\nComplete models for table: {complete_models}")

# ---------------------------------------------------------------------------
# 3. Build lookup
# ---------------------------------------------------------------------------
METRICS = ["rmse", "mape_pct", "mae", "pcc"]
LOWER_IS_BETTER = {"rmse": True, "mae": True, "mape_pct": True, "pcc": False}

lookup = {}
for _, row in df.iterrows():
    key = (row["model"], int(row["horizon_days"]), int(row["fold"]))
    lookup[key] = {
        "rmse": float(row["rmse"]),
        "mae": float(row["mae"]),
        "mape_pct": float(row["mape_pct"]),
        "pcc": float(row["pcc"]) if pd.notna(row.get("pcc")) else float("nan"),
    }

DISPLAY_NAMES = {
    "van_de_sande_mlr": "MLR",
    "van_de_sande_prophet_lstm": "Prophet-LSTM",
    "ashtar_seq2seq": "Seq2Seq",
}

# Build fold -> cutoff date mapping from cutoff column
FOLD_CUTOFF = {}
for _, row in df.iterrows():
    fold = int(row["fold"])
    if fold not in FOLD_CUTOFF and "cutoff" in row and pd.notna(row["cutoff"]):
        FOLD_CUTOFF[fold] = pd.Timestamp(row["cutoff"])

print(f"Fold -> Cutoff mapping: {FOLD_CUTOFF}")


def target_period_label(fold, horizon_days):
    """Return a compact date-range string, e.g. 'Jan 1 -- Mar 1, 2015'."""
    cutoff = FOLD_CUTOFF[fold]
    end = cutoff + pd.Timedelta(days=horizon_days)
    fmt_start = cutoff.strftime("%b~%d")
    if cutoff.year == end.year:
        fmt_end = end.strftime("%b~%d, %Y")
    else:
        fmt_start = cutoff.strftime("%b~%d, %Y")
        fmt_end = end.strftime("%b~%d, %Y")
    return f"{fmt_start} -- {fmt_end}"

# ---------------------------------------------------------------------------
# 4. Generate LaTeX — longtable version
# ---------------------------------------------------------------------------

def fmt_val(val, metric, is_best):
    """Format a metric value, bolding if best."""
    if np.isnan(val):
        return "---"
    if metric == "pcc":
        s = f"{val:.4f}"
    elif metric == "mape_pct":
        s = f"{val:.2f}"
    elif metric in ("rmse", "mae"):
        if abs(val) >= 10000:
            s = f"{val:.0f}"
        elif abs(val) >= 100:
            s = f"{val:.1f}"
        else:
            s = f"{val:.2f}"
    else:
        s = f"{val:.2f}"
    if is_best:
        s = r"\textbf{" + s + "}"
    return s


def generate_longtable(complete_models):
    n_models = len(complete_models)
    lines = []

    # Preamble comments
    lines.append(r"% Requires: \usepackage{longtable, booktabs}")
    lines.append(r"% Each cell shows: RMSE / MAPE(\%) / MAE / PCC")
    lines.append(r"% Best per metric per row in \textbf{bold}")
    lines.append("")

    # Column spec
    col_spec = "r r " + " ".join(["l"] * n_models)
    lines.append(r"\begin{longtable}{" + col_spec + "}")

    # Caption & label
    lines.append(r"\caption{Per-fold ROCV baseline results (RMSE / MAPE\% / MAE / PCC). "
                 r"Best per metric in \textbf{bold}.}")
    lines.append(r"\label{tab:baseline_full_results} \\")
    lines.append("")

    # First head
    lines.append(r"\toprule")
    header_cells = [r"\textbf{H (d)}", r"\textbf{Target Period}"]
    for m in complete_models:
        header_cells.append(r"\textbf{" + DISPLAY_NAMES.get(m, m) + "}")
    lines.append(" & ".join(header_cells) + r" \\")
    lines.append(r"\midrule")
    lines.append(r"\endfirsthead")
    lines.append("")

    # Continuation head
    lines.append(r"\toprule")
    lines.append(" & ".join(header_cells) + r" \\")
    lines.append(r"\midrule")
    lines.append(r"\endhead")
    lines.append("")

    # Foot
    lines.append(r"\midrule")
    lines.append(r"\multicolumn{" + str(2 + n_models) + r"}{r}{\textit{Continued on next page}} \\")
    lines.append(r"\endfoot")
    lines.append("")
    lines.append(r"\bottomrule")
    lines.append(r"\endlastfoot")
    lines.append("")

    for h_idx, horizon in enumerate(HORIZONS):
        for f_idx, fold in enumerate(FOLDS):
            # Collect values
            model_vals = {}
            for m in complete_models:
                key = (m, horizon, fold)
                model_vals[m] = lookup.get(key, {
                    "rmse": float("nan"), "mae": float("nan"),
                    "mape_pct": float("nan"), "pcc": float("nan"),
                })

            # Find best per metric
            best = {}
            for metric in METRICS:
                vals = [(m, model_vals[m][metric]) for m in complete_models
                        if not np.isnan(model_vals[m][metric])]
                if vals:
                    if LOWER_IS_BETTER[metric]:
                        best[metric] = min(vals, key=lambda x: x[1])[0]
                    else:
                        best[metric] = max(vals, key=lambda x: x[1])[0]

            # Build row
            cells = [str(horizon), target_period_label(fold, horizon)]
            for m in complete_models:
                v = model_vals[m]
                parts = []
                for metric in METRICS:
                    is_best = (best.get(metric) == m)
                    parts.append(fmt_val(v[metric], metric, is_best))
                cells.append(" / ".join(parts))

            lines.append(" & ".join(cells) + r" \\")

        # Midrule between horizons
        if h_idx < len(HORIZONS) - 1:
            lines.append(r"\midrule")

    lines.append(r"\end{longtable}")
    return "\n".join(lines)


tex = generate_longtable(complete_models)

out_path = RESULTS_DIR / "baseline_full_comparison_table.tex"
out_path.write_text(tex)
print(f"\nLaTeX longtable written to {out_path}")
print(f"Table: {len(HORIZONS)} horizons × {len(FOLDS)} folds = {EXPECTED} rows, {len(complete_models)} model columns")
