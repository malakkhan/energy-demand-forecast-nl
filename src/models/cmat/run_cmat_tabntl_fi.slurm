#!/bin/bash
#SBATCH --job-name=cmat-tabntl-fi
#SBATCH --partition=gpu_a100
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=18
#SBATCH --gpus=1
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/cmat_fi_%A_%a.log
#SBATCH --array=1-6

# =============================================================================
# CMAT-Tab / CMAT-NTL — permutation feature importance under the
# leakage-fixed pipeline (13-month ROCV folds, per-fold PCA, MSE point
# forecasting). Fold 0, seed 42, 10 shuffle-repeats per feature, using the
# checkpoints_v2 checkpoints from the fresh ROCV run. No NTL images needed
# for these variants (uses_spatial=False), so tasks are light.
# =============================================================================

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
mkdir -p logs

module load 2023 CUDA/12.1.1

PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

VARIANTS=(tab ntl)
HORIZONS=(60 120 180)

TASK_IDX=$((SLURM_ARRAY_TASK_ID - 1))
V_IDX=$((TASK_IDX / 3))
H_IDX=$((TASK_IDX % 3))

VARIANT=${VARIANTS[$V_IDX]}
H=${HORIZONS[$H_IDX]}

echo "========================================"
echo "  Job ID      : ${SLURM_JOB_ID}"
echo "  Array Task  : ${SLURM_ARRAY_TASK_ID}"
echo "  Task        : Permutation FI — CMAT-${VARIANT} h${H}d"
echo "  Fold        : 0   Seed: 42   Repeats: 10"
echo "  Node        : $(hostname)"
echo "  Started     : $(date)"
echo "========================================"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
umask 002
export PYTHONUNBUFFERED=1
export CMAT_CHECKPOINT_DIR="${REPO_ROOT}/src/models/cmat/results/checkpoints_v2"

"$PYTHON_BIN" -u -m src.models.cmat.feature_importance \
    --variant "$VARIANT" \
    --horizon "$H" \
    --fold 0 \
    --seed 42 \
    --n-repeats 10

echo "Finished: $(date)"
