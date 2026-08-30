#!/bin/bash
#SBATCH --job-name=cmat-full-rocv
#SBATCH --partition=gpu_h100
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --gpus=1
#SBATCH --mem=90G
#SBATCH --time=20:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/cmat_full_rocv_%A_%a.log
#SBATCH --array=1-9

# =============================================================================
# CMAT-Full — 3-seed x 3-horizon ROCV (10 folds each) with the FINAL searched
# configs under the leakage-fixed pipeline. One array task per (seed, horizon)
# combo; each combo may need more than one 24h walltime, so submit this script
# twice with --dependency=afterany chaining — the second run --resume's from
# the per-combo shard and skips completed folds (exits quickly if all done).
# Exactly one task ever writes a given shard at a time.
# =============================================================================

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
mkdir -p logs

module load 2023 CUDA/12.1.1
PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

SEEDS=(42 123 7)
HORIZONS=(60 120 180)
TASK_IDX=$((SLURM_ARRAY_TASK_ID - 1))
H_IDX=$((TASK_IDX / 3))
S_IDX=$((TASK_IDX % 3))
H=${HORIZONS[$H_IDX]}
SEED=${SEEDS[$S_IDX]}

echo "========================================"
echo "  Job ID      : ${SLURM_JOB_ID}  (array task ${SLURM_ARRAY_TASK_ID})"
echo "  Model       : CMAT-Full ROCV"
echo "  Horizon     : ${H} days   Seed: ${SEED}"
echo "  Node        : $(hostname)"
echo "  Started     : $(date)"
echo "========================================"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
umask 002
export PYTHONUNBUFFERED=1
export CMAT_CHECKPOINT_DIR="${REPO_ROOT}/src/models/cmat/results/checkpoints_v2"
export CMAT_SPATIAL_NUM_WORKERS=2

"$PYTHON_BIN" -u -m src.models.cmat.run_rocv \
    --variant full \
    --horizon "$H" \
    --seed "$SEED" \
    --resume

echo "Finished: $(date)"
