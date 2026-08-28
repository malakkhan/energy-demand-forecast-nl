#!/bin/bash
#SBATCH --job-name=mlr-rocv
#SBATCH --partition=fat_rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=03:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/mlr_%j.log

# =============================================================================
# MLR baseline — leakage-fixed pipeline (13-month ROCV, per-fold PCA at 95%
# variance fitted on training rows only, no crisis flag). All 9 horizons.
# =============================================================================

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
mkdir -p logs

PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

echo "========================================"
echo "  Job ID      : ${SLURM_JOB_ID}"
echo "  Model       : MLR (per-fold PCA enabled)"
echo "  Node        : $(hostname)"
echo "  Started     : $(date)"
echo "========================================"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
umask 002
export PYTHONUNBUFFERED=1

"$PYTHON_BIN" -u src/models/baselines/train_mlr.py

echo "Finished: $(date)"
