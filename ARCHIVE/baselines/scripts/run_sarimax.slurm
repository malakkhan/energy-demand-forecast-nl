#!/bin/bash
#SBATCH --job-name=sarimax
#SBATCH --partition=rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --time=48:00:00
#SBATCH --output=/projects/prjs2061/energy-demand-forecast-nl/logs/baseline_sarimax_%j.log
#SBATCH --mail-type=END,FAIL

# =============================================================================
# Model 3: SARIMAX (Ashtar et al.)
#
# CPU-only statsmodels SARIMAX with seasonal order (1,0,1,168).
# Most expensive model due to Kalman filter on hourly data.
# Training capped to last 2 years per fold to keep it tractable.
#
# Estimated runtime: ~48h (9 horizons × 11 folds)
#
# Submit:   sbatch src/models/baselines/run_sarimax.slurm
# Monitor:  tail -f logs/baseline_sarimax_<jobid>.log
# =============================================================================

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
mkdir -p "$REPO_ROOT/logs"

# ── Load modules and activate venv ────────────────────────────────────────
module load 2023
module load Python/3.11.3-GCCcore-12.3.0
source "${REPO_ROOT}/.venv/bin/activate"

echo "========================================"
echo "  Job ID   : ${SLURM_JOB_ID}"
echo "  Model    : ashtar_sarimax"
echo "  Node     : $(hostname)"
echo "  CPUs     : ${SLURM_CPUS_PER_TASK}"
echo "  Memory   : ${SLURM_MEM_PER_NODE}MB"
echo "  Started  : $(date)"
echo "  Python   : $(python --version)"
echo "========================================"
echo ""

# ── Threading config ──────────────────────────────────────────────────────
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export NUMEXPR_MAX_THREADS=${SLURM_CPUS_PER_TASK}
export PYTHONUNBUFFERED=1

# ── Run ───────────────────────────────────────────────────────────────────
python -u "$REPO_ROOT/src/models/baselines/train_baselines.py" \
    --model ashtar_sarimax

echo ""
echo "Finished: $(date)"
