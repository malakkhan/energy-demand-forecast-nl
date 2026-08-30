#!/bin/bash
#SBATCH --job-name=cmat-quickval
#SBATCH --partition=gpu_a100
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=18
#SBATCH --gpus=1
#SBATCH --mem=64G
#SBATCH --time=01:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/cmat_quickval_%A_%a.log
#SBATCH --array=1-6

# =============================================================================
# CMAT-Tab / CMAT-NTL — fold-0-only smoke test of the leakage-fixed pipeline
# (per-fold PCA, MSE point forecasting, 13-month ROCV step, no crisis flag)
# Seed 42 only. Validates before committing to the full 10-fold ROCV runs.
# =============================================================================

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
mkdir -p logs

module load 2023 CUDA/12.1.1

PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

VARIANTS=(tab ntl)
HORIZONS=(60 120 180)

# Map SLURM_ARRAY_TASK_ID (1..6) to (Variant, Horizon)
TASK_IDX=$((SLURM_ARRAY_TASK_ID - 1))
V_IDX=$((TASK_IDX / 3))
H_IDX=$((TASK_IDX % 3))

VARIANT=${VARIANTS[$V_IDX]}
H=${HORIZONS[$H_IDX]}
SEED=42

echo "========================================"
echo "  Job ID      : ${SLURM_JOB_ID}"
echo "  Array Task  : ${SLURM_ARRAY_TASK_ID}"
echo "  Model       : CMAT-${VARIANT}"
echo "  Horizon     : ${H} days"
echo "  Seed        : ${SEED}"
echo "  Mode        : --quick (fold 0 only)"
echo "  Node        : $(hostname)"
echo "  CPUs        : ${SLURM_CPUS_PER_TASK}"
echo "  GPUs        : ${SLURM_GPUS}"
echo "  Started     : $(date)"
echo "  Python      : $($PYTHON_BIN --version)"
echo "  PyTorch     : $($PYTHON_BIN -c 'import torch; print(torch.__version__)')"
echo "  CUDA avl    : $($PYTHON_BIN -c 'import torch; print(torch.cuda.is_available())')"
echo "========================================"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export PYTHONUNBUFFERED=1
export CMAT_CHECKPOINT_DIR="${REPO_ROOT}/src/models/cmat/results/checkpoints_v2"

"$PYTHON_BIN" -u -m src.models.cmat.run_rocv \
    --variant "$VARIANT" \
    --horizon "$H" \
    --seed "$SEED" \
    --quick \
    --resume

echo "Finished: $(date)"
