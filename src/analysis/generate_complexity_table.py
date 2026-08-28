#!/usr/bin/env python3
"""Generate the computational-complexity LaTeX table (appendix).

Parameters, mean train/inference wall-clock per ROCV fold, and epochs for
every model under the leakage-fixed pipeline. Reads the live result CSVs, so
rerunning after new results (e.g. CMAT-Full ROCV) updates the table.

Usage (from repo root):
    .venv/bin/python3 src/analysis/generate_complexity_table.py > reports/complexity_table.tex
"""

import json
import os

import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
R = os.path.join(REPO, "src", "models", "baselines", "results")
CR = os.path.join(REPO, "src", "models", "cmat", "results")
HORIZONS = [60, 120, 180]


def lstm_params(inp, hidden):
    """PyTorch LSTM layer parameter count (bias=True)."""
    return 4 * hidden * inp + 4 * hidden * hidden + 8 * hidden


# ── Analytic parameter counts ──
# Input widths now include the per-fold PCA components, so they vary by
# horizon and fold; counts below use the modal recorded n_features per
# horizon (footnoted). Architectures themselves are fixed:
# Seq2Seq (Ashtar): encoder LSTM(64) on (n_features + target) inputs,
#   decoder LSTM(64) on 1 input, Linear(64,1).
# Prophet-LSTM (v2): LSTM(30) -> LSTM(90) -> Linear(90,1) on n_features
#   inputs (base + Prophet components + PCA), plus ~62 Prophet parameters.

def seq2seq_params(n_features):
    return lstm_params(n_features + 1, 64) + lstm_params(1, 64) + 64 + 1

def prophet_nn_params(n_features):
    return lstm_params(n_features, 30) + lstm_params(30, 90) + 90 + 1


def stats(df, h, col):
    sub = df[(df["horizon_days"] == h) & (df["fold"] <= 9)]
    if len(sub) == 0 or col not in sub.columns:
        return None, None
    return float(sub[col].mean()), float(sub[col].std())


def fmt_pm(m, s, dp=1):
    if m is None:
        return "---"
    return f"{m:,.{dp}f}$\\pm${s:,.{dp}f}".replace(",", "{,}")


def fmt_int(n):
    return f"{n:,}".replace(",", "{,}")


mlr = pd.read_csv(f"{R}/rocv_results_van_de_sande_mlr.csv")
seq = pd.read_csv(f"{R}/rocv_results_ashtar_seq2seq.csv")
pro = pd.read_csv(f"{R}/rocv_results_van_de_sande_prophet_lstm.csv")
tab = pd.read_csv(f"{CR}/rocv_results_cmat_tab.csv")
ntl = pd.read_csv(f"{CR}/rocv_results_cmat_ntl.csv")
try:
    full = pd.read_csv(f"{CR}/rocv_results_cmat_full.csv")
except FileNotFoundError:
    full = None


def naive_df(name):
    d = json.load(open(f"{R}/{name}/{name}_rocv.json"))
    rows = [r for r in d["results"].values() if r["horizon_days"] in HORIZONS]
    df = pd.DataFrame(rows)
    df["infer_time_s"] = df["infer_time_s"].astype(float)
    df["train_time_s"] = df["train_time_s"].astype(float)
    return df


# CMAT-Full interim parameter counts from best search trials (used only
# while the ROCV results do not exist yet).
def full_interim_params(h):
    recs = [json.loads(l) for l in open(f"{CR}/trial_history_full_h{h}d.jsonl") if l.strip()]
    comp = [r for r in recs if r.get("state") == "COMPLETE" and r.get("value") is not None]
    best = min(comp, key=lambda r: float(r["value"]))
    return best.get("user_attrs", {}).get("n_params")


rows = []

def add_model(name, per_h):
    for i, h in enumerate(HORIZONS):
        label = name if i == 0 else ""
        rows.append((label, h) + per_h(h))

add_model("MLR", lambda h: (
    fmt_int(int(mlr[mlr.horizon_days == h]["n_features"].mode().iloc[0]) + 1),
    "$<$1", f"$<$0.01", "---"))

add_model("Na\\\"ive-24h", lambda h: ("0", "---",
    fmt_pm(*stats(naive_df("naive_24h"), h, "infer_time_s"), dp=2), "---"))
add_model("Drift", lambda h: ("0", "---",
    fmt_pm(*stats(naive_df("drift"), h, "infer_time_s"), dp=2), "---"))
add_model("S-Na\\\"ive", lambda h: ("0", "---",
    fmt_pm(*stats(naive_df("seasonal_naive"), h, "infer_time_s"), dp=2), "---"))

def modal_nfeat(df, h):
    return int(df[df.horizon_days == h]["n_features"].mode().iloc[0])

add_model("Prophet-LSTM", lambda h: (
    fmt_int(prophet_nn_params(modal_nfeat(pro, h)) + 62) + "\\textsuperscript{\\dag\\ddag}",
    fmt_pm(*stats(pro, h, "train_time_s")),
    fmt_pm(*stats(pro, h, "infer_time_s"), dp=2),
    "50"))

add_model("Seq2Seq", lambda h: (
    fmt_int(seq2seq_params(modal_nfeat(seq, h))) + "\\textsuperscript{\\ddag}",
    fmt_pm(*stats(seq, h, "train_time_s")),
    fmt_pm(*stats(seq, h, "infer_time_s"), dp=2),
    "20"))

def cmat_row(df, h):
    params = int(df[df.horizon_days == h]["n_params"].mode().iloc[0])
    ep = df[df.horizon_days == h]["n_epochs"].mean()
    return (fmt_int(params) + "\\textsuperscript{\\ddag}",
            fmt_pm(*stats(df, h, "train_time_s")),
            fmt_pm(*stats(df, h, "infer_time_s"), dp=2),
            f"{ep:.0f}")

add_model("CMAT-Tab", lambda h: cmat_row(tab, h))
add_model("CMAT-NTL", lambda h: cmat_row(ntl, h))

if full is not None and len(full[full.fold <= 9]) > 0:
    add_model("CMAT-Full", lambda h: cmat_row(full, h))
else:
    add_model("CMAT-Full", lambda h: (
        fmt_int(full_interim_params(h)) + "\\textsuperscript{\\ddag *}",
        "---", "---", "---"))

print(r"""\section{Computational Complexity Data}
\label{sec:apx:comp}
% Generated by src/analysis/generate_complexity_table.py — do not hand-edit.

\begin{table}[h]
\centering
\footnotesize
\caption{Model complexity comparison: trainable parameters and mean wall-clock
training and inference time per ROCV fold (mean$\pm$std over folds and, where
applicable, seeds; inference = one forward pass over the full test horizon).
Prophet-LSTM combines ${\sim}62$ Prophet parameters fitted via Stan MAP
estimation with a stacked LSTM; the total includes both stages. CMAT variants
select different architectures per horizon via Optuna search. Epochs are
maxima for fixed-budget models (early stopping applies) and mean realised
epochs for CMAT. All neural models trained on a single NVIDIA A100 GPU; MLR,
Prophet, and the na\"ive baselines use CPU only. CMAT-Full timing pending
(ROCV not yet run under the corrected pipeline).}
\label{tab:model_complexity}
\begin{tabular}{l r r r r r}
\toprule
\textbf{Model} & \textbf{H (d)} & \textbf{Parameters} & \textbf{Train Time (s)} & \textbf{Infer.\ Time (s)} & \textbf{Epochs} \\
\midrule""")

prev = ""
out = []
for (label, h, params, tr, inf, ep) in rows:
    if label and out:
        out.append("\\midrule")
    out.append(f"{label:<14} & {h:>4} & {params} & {tr} & {inf} & {ep} \\\\")
print("\n".join(out))

print(r"""\bottomrule
\end{tabular}
\begin{flushleft}
\textsuperscript{\dag}Includes ${\sim}62$ Prophet parameters (changepoint
weights, Fourier seasonal coefficients, trend \& scale) fitted via Stan
L-BFGS; the remainder are LSTM parameters. \\
\textsuperscript{\ddag}Modal value across folds: the per-fold PCA retains
7--16 components depending on the fold's training-window length, shifting the
input-layer width and thus the total parameter count between folds (${<}1\%$
for the CMAT variants; up to ${\sim}7\%$ for the small LSTM baselines). \\
\textsuperscript{*}Interim: parameter count of the best configuration found so
far by the (still running) CMAT-Full hyperparameter search.
\end{flushleft}
\end{table}
""")

# ── Sample counts per fold ──────────────────────────────────────────────
import sys
sys.path.insert(0, os.path.join(REPO, "src", "models"))
from baselines import config as BC             # noqa: E402
from baselines.data_loader import rocv_folds, split_fold  # noqa: E402

sdf = pd.read_parquet(os.path.join(REPO, "data", "processed", "nl_hourly_dataset.parquet"))
if "timestamp" in sdf.columns:
    sdf["timestamp"] = pd.to_datetime(sdf["timestamp"])
    sdf = sdf.set_index("timestamp")
sdf = sdf.dropna(subset=["entsoe_load_mw"])

HEADER_SC = (r"\textbf{H (d)} & \textbf{Fold} & \textbf{Test Origin} & "
             r"\textbf{Train} & \textbf{Val} & \textbf{Test} & "
             r"\textbf{Train} & \textbf{Val} & \textbf{Test} & "
             r"\textbf{Train} & \textbf{Val} & \textbf{Test} \\")
GROUP_SC = (r" &  &  & \multicolumn{3}{c}{\textbf{Raw observations}} & "
            r"\multicolumn{3}{c}{\textbf{CMAT-Tab sequences}} & "
            r"\multicolumn{3}{c}{\textbf{CMAT-NTL sequences}} \\")

print(r"""\begin{longtable}{r r l r r r r r r r r r}
\caption{Per-fold sample counts under the 13-month-step ROCV protocol: raw
hourly observations per split (expanding training window, ${\sim}$1-year
validation window, $H \times 24$-hour test window), and the training /
validation / test sequence counts realised by the windowed CMAT variants
(the context window $W$ trims the first $W$ hours of each split, and the
horizon shift removes the leading $H \times 24$ training rows). The remaining
models follow fixed offsets of the raw counts: Seq2Seq derives
$\mathrm{raw} - H{\times}24 - 888$ training sequences (720-hour input +
168-hour output windows); Prophet-LSTM trains on $\mathrm{raw} - H{\times}24$
rows and MLR on the raw rows directly, with both using the raw validation and
test rows unchanged. CMAT-Full pending.}
\label{tab:fold_sample_counts} \\
\toprule""" + "\n" + GROUP_SC + "\n"
+ r"\cmidrule(lr){4-6} \cmidrule(lr){7-9} \cmidrule(lr){10-12}" + "\n"
+ HEADER_SC + "\n" + r"""\midrule
\endfirsthead
\toprule""" + "\n" + GROUP_SC + "\n"
+ r"\cmidrule(lr){4-6} \cmidrule(lr){7-9} \cmidrule(lr){10-12}" + "\n"
+ HEADER_SC + "\n" + r"""\midrule
\endhead
\midrule
\multicolumn{12}{r}{\textit{Continued on next page}} \\
\endfoot
\bottomrule
\endlastfoot""")


def seq_counts(df, h, fold):
    sub = df[(df["horizon_days"] == h) & (df["fold"] == fold)]
    if len(sub) == 0:
        return ["---"] * 3
    r = sub.iloc[0]
    return [fmt_int(int(r["n_train"])), fmt_int(int(r["n_val"])), fmt_int(int(r["n_test"]))]


folds_list = list(rocv_folds(sdf, BC.ROCVConfig()))
for hi, h in enumerate(HORIZONS):
    if hi:
        print(r"\midrule")
    for fold_idx, train_end, val_end, test_origin in folds_list:
        tr, va, te = split_fold(sdf, train_end, val_end, test_origin, h)
        raw = [fmt_int(len(tr)), fmt_int(len(va)), fmt_int(len(te))]
        cells = raw + seq_counts(tab, h, fold_idx) + seq_counts(ntl, h, fold_idx)
        print(f"{h} & {fold_idx} & {test_origin.date()} & " + " & ".join(cells) + " \\\\")

print(r"""\end{longtable}

\clearpage""")
