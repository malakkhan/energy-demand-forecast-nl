#!/bin/bash
#SBATCH --job-name=cmat-full-fold
#SBATCH --partition=gpu_h100
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --gpus=1
#SBATCH --mem=90G
#SBATCH --time=08:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/cmat_full_fold_%j.log

# =============================================================================
# CMAT-Full — single-fold ROCV task (per-fold parallel fan-out).
# One SLURM job = one (horizon, seed, fold) triple, writing its OWN shard
# file cmat_full_h{H}_s{SEED}_f{FOLD}.csv via CMAT_SHARD_SUFFIX, so any
# number of these can run concurrently without ever sharing a writer.
# merge_shards combines + dedups on (fold, horizon_days, seed).
#
# Required via --export:  H (horizon days), SEED, FOLD
# Optional via --export:  WORKERS (default 2), PREFETCH (default 4)
# Partition/walltime overridable at sbatch time (-p gpu_a100 -t HH:MM:SS).
# =============================================================================

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
mkdir -p logs

module load 2023 CUDA/12.1.1

PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

: "${H:?H (horizon days) required via --export}"
: "${SEED:?SEED required via --export}"
: "${FOLD:?FOLD required via --export}"
WORKERS="${WORKERS:-2}"
PREFETCH="${PREFETCH:-4}"

echo "========================================"
echo "  Job ID      : ${SLURM_JOB_ID}"
echo "  Model       : CMAT-full  h${H}d  seed ${SEED}  fold ${FOLD}"
echo "  Partition   : ${SLURM_JOB_PARTITION}"
echo "  Workers     : ${WORKERS}  (prefetch ${PREFETCH})"
echo "  Node        : $(hostname)"
echo "  Started     : $(date)"
echo "========================================"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
umask 002
export PYTHONUNBUFFERED=1
export CMAT_CHECKPOINT_DIR="${REPO_ROOT}/src/models/cmat/results/checkpoints_v2"
export CMAT_SPATIAL_NUM_WORKERS=${WORKERS}
export CMAT_PREFETCH_FACTOR=${PREFETCH}
export CMAT_SHARD_SUFFIX="_f${FOLD}"

"$PYTHON_BIN" -u -m src.models.cmat.run_rocv \
    --variant full \
    --horizon "$H" \
    --seed "$SEED" \
    --start-fold "$FOLD" \
    --max-folds 1 \
    --resume

echo "Finished: $(date)"
