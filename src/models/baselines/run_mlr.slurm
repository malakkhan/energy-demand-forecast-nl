#!/bin/bash
#SBATCH --job-name=mlr_rocv
#SBATCH --partition=rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --output=/projects/prjs2061/energy-demand-forecast-nl/logs/mlr_baseline_%j.log
#SBATCH --mail-type=END,FAIL

# =============================================================================
# MLR Baseline — Explicit 35-Feature ROCV Experiment
#
# Multiple Linear Regression with 20 hand-picked features + 15 PCA
# CBS-KNMI components, evaluated using expanding-window Rolling-Origin
# Cross-Validation (validation year merged into training).
#
# Features (35 total):
#   Temporal (13): day_of_year, week_of_year, quarter, hour_sin, hour_cos,
#                  dow_sin, dow_cos, month_sin, month_cos, year,
#                  is_public_holiday, is_school_holiday, is_energy_crisis
#   Lag (7):       load_lag_12h, load_lag_24h, load_lag_1w, load_lag_1y,
#                  solar_rad_lag_10h, humidity_lag_12h, temp_lag_107h
#   PCA (15):      pca_cbs_knmi_01 … pca_cbs_knmi_15
#
# Pre-training pipeline:
#   1. Drop columns with >50% NaN in training set
#   2. IQR outlier removal (train only)
#   3. Forward-fill → backward-fill → median imputation (no MICE)
#
# ROCV protocol:
#   - Expanding window from 2012-01-01
#   - Validation year merged into training (MLR has no early stopping)
#   - First test origin: 2015-01-01 (3 years of training data)
#   - Origin advances by 1 year per fold
#   - Horizons: 60, 75, 90, 105, 120, 135, 150, 165, 180 days
#
# Submit:     sbatch src/models/baselines/run_mlr.slurm
# Quick test: sbatch --time=00:30:00 src/models/baselines/run_mlr.slurm --quick
# Single H:   sbatch src/models/baselines/run_mlr.slurm --horizon 60
# Monitor:    tail -f logs/mlr_baseline_<jobid>.log
# Results:    ls src/models/baselines/results/rocv_results_van_de_sande_mlr.*
# =============================================================================

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
mkdir -p "$REPO_ROOT/logs"

# ── Load modules and activate venv ────────────────────────────────────
module load 2023
module load Python/3.11.3-GCCcore-12.3.0
source "${REPO_ROOT}/.venv/bin/activate"

echo "========================================"
echo "  Job ID    : ${SLURM_JOB_ID}"
echo "  Model     : MLR Baseline (35 features)"
echo "  Node      : $(hostname)"
echo "  CPUs      : ${SLURM_CPUS_PER_TASK}"
echo "  Memory    : ${SLURM_MEM_PER_NODE}MB"
echo "  Started   : $(date)"
echo "  Python    : $(python --version)"
echo "========================================"
echo ""

# ── Threading config ──────────────────────────────────────────────────
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export NUMEXPR_MAX_THREADS=${SLURM_CPUS_PER_TASK}
export PYTHONUNBUFFERED=1

# ── Run ───────────────────────────────────────────────────────────────
python -u "$REPO_ROOT/src/models/baselines/train_mlr.py" "$@"

echo ""
echo "Finished: $(date)"
