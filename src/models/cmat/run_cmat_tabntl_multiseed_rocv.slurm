#!/bin/bash
#SBATCH --job-name=cmat-tabntl-rocv-ms
#SBATCH --partition=gpu_a100
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=18
#SBATCH --gpus=1
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/cmat_tabntl_rocv_ms_%A_%a.log
#SBATCH --array=1-12

# =============================================================================
# CMAT-Tab / CMAT-NTL — full ROCV (all 10 folds), seeds 123 and 7, to match
# the 3-seed treatment already used for the baselines (seed 42 already done
# in job 26036294). h60/h120/h180.
# =============================================================================

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
mkdir -p logs

module load 2023 CUDA/12.1.1

PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

VARIANTS=(tab ntl)
HORIZONS=(60 120 180)
SEEDS=(123 7)

# Map SLURM_ARRAY_TASK_ID (1..12) to (Variant, Horizon, Seed)
TASK_IDX=$((SLURM_ARRAY_TASK_ID - 1))
V_IDX=$((TASK_IDX / 6))
REM=$((TASK_IDX % 6))
H_IDX=$((REM / 2))
S_IDX=$((REM % 2))

VARIANT=${VARIANTS[$V_IDX]}
H=${HORIZONS[$H_IDX]}
SEED=${SEEDS[$S_IDX]}

echo "========================================"
echo "  Job ID      : ${SLURM_JOB_ID}"
echo "  Array Task  : ${SLURM_ARRAY_TASK_ID}"
echo "  Model       : CMAT-${VARIANT}"
echo "  Horizon     : ${H} days"
echo "  Seed        : ${SEED}"
echo "  Mode        : full ROCV (10 folds)"
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
    --resume

echo "Finished: $(date)"
