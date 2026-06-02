# Baseline Forecasting Models

Six time-series forecasting baselines for predicting Dutch hourly
electricity load (`entsoe_load_mw`).  All models consume the same input
data (KNMI validated weather, CBS economic indicators, ENTSO-E load)
and are evaluated using a strict expanding-window **Rolling-Origin
Cross-Validation (ROCV)** protocol.

---

## Models

| # | Name (CLI key) | Architecture | Framework | Reference |
|---|---|---|---|---|
| 1 | `van_de_sande_mlr` | Multiple Linear Regression | scikit-learn | van de Sande et al. |
| 2 | `van_de_sande_prophet_lstm` | Prophet → LSTM(30)→LSTM(90)→Dense(1) | Prophet + PyTorch | van de Sande et al. |
| 3 | `ashtar_sarimax` | SARIMAX(1,0,1)(1,0,1,168) | statsmodels | Ashtar et al. |
| 4 | `ashtar_sarimax_lstm` | SARIMAX + residual LSTM(50) | statsmodels + PyTorch | Ashtar et al. |
| 5 | `ashtar_seq2seq` | Encoder-LSTM(64) → Decoder-LSTM(64) | PyTorch | Ashtar et al. |
| 6 | `curiel_stacked_prophet_qlstm` | Prophet → QLSTM(60,120) → XGBoost stacker | PennyLane + PyTorch + XGBoost | Curiël et al. |

---

## Directory Layout

```
src/models/baselines/
├── README.md                   ← you are here
│
│   ── Configuration ──
├── config.py                   ← all hyperparameters, feature lists, ROCV params
│
│   ── Data & Preprocessing ──
├── data_loader.py              ← load dataset, ROCV folds, feature engineering,
│                                  outlier removal, imputation, scaling, sequences
│
│   ── Model Implementations ──
├── mlr.py                      ← Model 1: MLR
├── prophet_lstm_pytorch.py     ← Model 2: Prophet-LSTM (PyTorch, Huang variant)
├── sarimax.py                  ← Model 3: SARIMAX
├── sarimax_lstm.py             ← Model 4: SARIMAX + residual LSTM
├── seq2seq.py                  ← Model 5: Encoder-Decoder Seq2Seq
├── qlstm.py                    ← Model 6: Prophet-QLSTM-XGBoost stacking
│
│   ── Evaluation ──
├── evaluation.py               ← 7 metrics, results saving, metric evolution plots
│
│   ── Training & Orchestration ──
├── train_baselines.py          ← unified CLI for all models (main entry point)
├── aggregate_results.py        ← post-hoc results aggregation + LaTeX table
│
│   ── SLURM (Snellius HPC) ──
├── run_baselines.slurm         ← CPU job array (models 1-6)
├── run_baselines_gpu.slurm     ← GPU job script (for PyTorch models)
│
│   ── Legacy ──
├── prophet_lstm_tf.py          ← original TensorFlow implementation (archived)
├── prophet_lstm.py             ← original TF wrapper (archived)
├── train_prophet_lstm.py       ← original TF training script (deprecated)
├── train_prophet_lstm.slurm    ← original TF SLURM job
│
│   ── Output ──
└── results/
    ├── rocv_results_{model}.csv      ← per-fold metrics (one file per model)
    ├── rocv_results_{model}.json     ← same, as JSON
    ├── rocv_results_all_models.csv   ← combined across all models
    ├── rocv_summary_by_horizon.csv   ← mean ± std per horizon per model
    ├── rocv_summary.json             ← machine-readable summary
    ├── baseline_comparison_table.tex ← LaTeX table for the thesis
    ├── run_metadata.json             ← run parameters + timing
    └── plots/
        └── metrics_evolution_{model}_h{horizon}d.png
```

---

## Configuration

All hyperparameters are centralised in
[`config.py`](config.py).

| Dataclass | What it controls |
|---|---|
| `ROCVConfig` | Expanding-window protocol: `start_date`, `min_train_years`, `fold_step_years`, `forecast_horizons_days` |
| `MLRConfig` | Autoregressive lags for the linear model |
| `ProphetConfig` | Prophet additive seasonality (Model 2) |
| `LSTMConfig` | PyTorch LSTM architecture: units, dropout, epochs, early stopping |
| `SARIMAXConfig` | SARIMAX order, seasonal order, `max_train_hours` window cap |
| `SARIMAXLSTMConfig` | Residual LSTM units, sequence length |
| `Seq2SeqConfig` | Encoder/decoder units, teacher forcing ratio |
| `ProphetMultConfig` | Prophet multiplicative seasonality (Model 6) |
| `QLSTMConfig` | Quantum circuit params: qubits, layers, backend, warmup |
| `XGBoostStackerConfig` | XGBoost meta-learner: n_estimators, depth, learning rate |

**Key tuneable parameters:**

```python
# Change ROCV fold advancement (default: 1 year)
rocv_cfg = ROCVConfig(fold_step_years=1)  # also settable via --fold-step CLI flag

# Forecast horizons evaluated (days)
rocv_cfg.forecast_horizons_days  # [60, 75, 90, 105, 120, 135, 150, 165, 180]

# SARIMAX training window cap (default: 2 years of hourly data)
sarimax_cfg = SARIMAXConfig(max_train_hours=17520)
```

---

## Evaluation Protocol

### ROCV (Rolling-Origin Cross-Validation)

Expanding-window protocol following van de Sande et al. (2024) and
Curiël et al. (2025):

```
Data range: 2012-01-01 → latest available

Fold 0:  train = [2012, 2014)    val = [2014, 2015)    origin = 2015-01-01
Fold 1:  train = [2012, 2015)    val = [2015, 2016)    origin = 2016-01-01
Fold 2:  train = [2012, 2016)    val = [2016, 2017)    origin = 2017-01-01
  …       (origin advances by fold_step_years each iteration)

For each origin, evaluate at horizons: 60, 75, 90, …, 180 days.
```

The origin advancement step (`fold_step_years`) defaults to **1 year**
and can be changed via `--fold-step` at the command line or by
modifying `ROCVConfig.fold_step_years` in `config.py`.

### Metrics

Seven metrics are computed on **original-scale (MW)** values,
regardless of which loss function is used during model training:

| Metric | Formula | Interpretation |
|---|---|---|
| **RMSE** | √(mean(ε²)) | Root Mean Squared Error — penalises large deviations |
| **MAE** | mean(\|ε\|) | Mean Absolute Error — robust average error |
| **MAPE** | mean(\|ε/y\|) × 100 | Mean Absolute Percentage Error (%) |
| **MASE** | MAE / MAE_naive | Mean Absolute Scaled Error vs 24-h persistence |
| **nMAE** | MAE / mean(y) | Normalised MAE (ratio, scale-free) |
| **nRMSE** | RMSE / mean(y) | Normalised RMSE (ratio, scale-free) |
| **PCC** | Pearson(y, ŷ) | Pearson Correlation Coefficient |

where ε = ŷ − y and y = actual load (MW).

---

## Running the Models

### Quick Smoke Test

```bash
# Single model, single fold, single horizon (fastest):
python -u src/models/baselines/train_baselines.py \
    --model van_de_sande_mlr --quick
```

### Single Model

```bash
python -u src/models/baselines/train_baselines.py \
    --model van_de_sande_prophet_lstm
```

### Single Model + Single Horizon

```bash
python -u src/models/baselines/train_baselines.py \
    --model ashtar_sarimax --horizon 60
```

### All Models

```bash
python -u src/models/baselines/train_baselines.py
```

### Override Fold Step

```bash
# Semi-annual origin advancement:
python -u src/models/baselines/train_baselines.py \
    --model van_de_sande_mlr --fold-step 1
```

### CLI Options

```
--data-path PATH      Path to the Parquet dataset
--output-dir PATH     Where to write results (default: results/)
--model MODEL_NAME    Run only this model
--horizon N           Run only this horizon (days)
--quick               1 fold, 1 horizon (smoke test)
--fold-step N         Override fold_step_years in ROCV config
```

---

## Running on Snellius (SLURM)

### CPU (all models, job array)

```bash
# Submit all 6 models in parallel (one job per model):
sbatch src/models/baselines/run_baselines.slurm

# Submit only MLR (array task 1):
sbatch --array=1 src/models/baselines/run_baselines.slurm

# Quick test:
sbatch --array=1 --time=01:00:00 src/models/baselines/run_baselines.slurm --quick
```

Array task → model mapping:

| Task ID | Model |
|---|---|
| 1 | `van_de_sande_mlr` |
| 2 | `van_de_sande_prophet_lstm` |
| 3 | `ashtar_sarimax` |
| 4 | `ashtar_sarimax_lstm` |
| 5 | `ashtar_seq2seq` |
| 6 | `curiel_stacked_prophet_qlstm` |

### GPU (PyTorch models only)

```bash
sbatch src/models/baselines/run_baselines_gpu.slurm \
    --model van_de_sande_prophet_lstm
```

### Monitoring

```bash
tail -f logs/baseline_<jobid>_<taskid>.log
```

---

## Output & Results

### Per-model results

After training, each model produces:

| File | Format | Contents |
|---|---|---|
| `results/rocv_results_{model}.csv` | CSV | One row per (fold, horizon) with all 7 metrics |
| `results/rocv_results_{model}.json` | JSON | Same data, nested by fold |

**CSV columns:**

```
model, fold, cutoff, horizon_days, horizon_hours,
n_train, n_test,
rmse, mae, mape_pct, mase, nmae, nrmse, pcc,
n_samples, elapsed_s
```

Results are saved **incrementally after each fold** (crash-safe
checkpointing).  If a SLURM job is interrupted, the completed folds
are preserved and can be resumed.

### Combined results

After all models finish (or via `aggregate_results.py`):

| File | Contents |
|---|---|
| `results/rocv_results_all_models.csv` | All models merged |
| `results/rocv_summary_by_horizon.csv` | Mean ± std per model per horizon |
| `results/rocv_summary.json` | Machine-readable summary |
| `results/baseline_comparison_table.tex` | LaTeX table ready for the thesis |
| `results/run_metadata.json` | Run parameters, timing, data path |

### Metric evolution plots

For each model and each horizon, a PNG is saved to
`results/plots/`:

```
results/plots/metrics_evolution_{model}_h{horizon}d.png
```

Each plot contains **4 panels** grouping metrics by scale:

1. **Absolute errors** — RMSE and MAE (MW)
2. **Normalised errors** — nRMSE and nMAE (ratio)
3. **Percentage & scaled** — MAPE (%) and MASE
4. **Correlation** — PCC

The x-axis is the ROCV fold cutoff date, showing how each metric
evolves as the training window expands through time.

### Reading results programmatically

```python
import pandas as pd

# Per-fold results for one model:
df = pd.read_csv("src/models/baselines/results/rocv_results_van_de_sande_mlr.csv")

# All models:
df = pd.read_csv("src/models/baselines/results/rocv_results_all_models.csv")

# Summarised:
summary = pd.read_csv("src/models/baselines/results/rocv_summary_by_horizon.csv")

# Filter by model + horizon:
mask = (summary["model"] == "van_de_sande_mlr") & (summary["horizon_days"] == 60)
print(summary[mask][["rmse_mean", "mae_mean", "pcc_mean"]])
```

### Re-generating aggregates and plots

```bash
# From existing per-model CSVs (no re-training):
python -u src/models/baselines/aggregate_results.py

# Skip plots:
python -u src/models/baselines/aggregate_results.py --no-plots
```

---

## Dependencies

| Package | Purpose | Models |
|---|---|---|
| `torch` | LSTM, Seq2Seq, QLSTM | 2, 4, 5, 6 |
| `prophet` | Seasonal decomposition | 2, 6 |
| `statsmodels` | SARIMAX | 3, 4 |
| `scikit-learn` | MinMaxScaler, IterativeImputer, LinearRegression | all |
| `scipy` | Pearson r, statistics | all |
| `pennylane` + `pennylane-lightning` | Quantum LSTM circuits | 6 |
| `xgboost` | Stacking meta-learner | 6 |
| `matplotlib` | Metric evolution plots | all |
| `pandas`, `numpy` | Data manipulation | all |

Install via:

```bash
pip install -r requirements.in
```
