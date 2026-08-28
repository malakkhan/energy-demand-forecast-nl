#!/bin/bash
#SBATCH --job-name=cmat-full-workertest
#SBATCH --partition=gpu_a100
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=18
#SBATCH --gpus=1
#SBATCH --mem=128G
#SBATCH --time=03:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/cmat_full_workertest_%j.log

# =============================================================================
# CONTROLLED EXPERIMENT — do not treat as the real search.
# Tests whether CMAT_SPATIAL_NUM_WORKERS=2 + doubled --mem (128G) avoids
# the worker-spawn-time OOM seen with num_workers=8, while still beating
# num_workers=0's throughput. h60 only (smallest model, ~150k params) to
# minimize cost if this fails. Short walltime (3h) — just needs to observe
# a handful of trials to see if memory grows or another spawn-time OOM hits.
# =============================================================================

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
mkdir -p logs

module load 2023 CUDA/12.1.1

PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

echo "========================================"
echo "  Job ID      : ${SLURM_JOB_ID}"
echo "  Model       : CMAT-Full (WORKER TEST)"
echo "  Horizon     : 60 days"
echo "  Trials      : 5 (just observing behavior)"
echo "  Fold        : 0"
echo "  mem         : 128G"
echo "  workers     : CMAT_SPATIAL_NUM_WORKERS=2"
echo "  Node        : $(hostname)"
echo "  Started     : $(date)"
echo "========================================"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export PYTHONUNBUFFERED=1
export CMAT_CHECKPOINT_DIR="${REPO_ROOT}/src/models/cmat/results/checkpoints_v2_workertest"
export CMAT_SPATIAL_NUM_WORKERS=2

"$PYTHON_BIN" -u -m src.models.cmat.search \
    --variant full \
    --horizon 60 \
    --n-trials 5 \
    --fold 0 \
    --output-dir "${REPO_ROOT}/src/models/cmat/results_workertest"

echo "Finished: $(date)"
