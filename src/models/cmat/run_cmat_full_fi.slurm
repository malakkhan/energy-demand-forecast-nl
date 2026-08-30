#!/bin/bash
#SBATCH --job-name=cmat-full-fi
#SBATCH --partition=gpu_a100
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=18
#SBATCH --gpus=1
#SBATCH --mem=96G
#SBATCH --time=08:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/cmat_full_fi_%A_%a.log
#SBATCH --array=1-2

# =============================================================================
# CMAT-Full — permutation feature importance (leakage-fixed pipeline).
# Fold 0, seed 42, 10 shuffle-repeats per feature + the NTL image modality,
# using the checkpoints_v2 fold-0 checkpoints from the completed ROCV.
# Study scope: h60 + h180 (h120 deferred). Needs the full 15.5GB NTL
# preload, hence 96G and the long walltime.
# =============================================================================

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
mkdir -p logs

module load 2023 CUDA/12.1.1

PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

HORIZONS=(60 180)
H=${HORIZONS[$((SLURM_ARRAY_TASK_ID - 1))]}

echo "========================================"
echo "  Job ID      : ${SLURM_JOB_ID}"
echo "  Task        : Permutation FI — CMAT-Full h${H}d"
echo "  Fold        : 0   Seed: 42   Repeats: 10"
echo "  Node        : $(hostname)"
echo "  Started     : $(date)"
echo "========================================"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
umask 002
export PYTHONUNBUFFERED=1
export CMAT_CHECKPOINT_DIR="${REPO_ROOT}/src/models/cmat/results/checkpoints_v2"
export CMAT_SPATIAL_NUM_WORKERS=4
export CMAT_PREFETCH_FACTOR=2

"$PYTHON_BIN" -u -m src.models.cmat.feature_importance \
    --variant full \
    --horizon "$H" \
    --fold 0 \
    --seed 42 \
    --n-repeats 10

echo "Finished: $(date)"
