#!/bin/bash
#SBATCH --job-name=cmat-full-search
#SBATCH --partition=gpu_a100
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=18
#SBATCH --gpus=1
#SBATCH --mem=96G
#SBATCH --time=24:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/cmat_full_search_%A_%a.log
#SBATCH --array=1-3

# =============================================================================
# CMAT-Full — Optuna hyperparameter search under the leakage-fixed pipeline
# (per-fold PCA, MSE point forecasting, 13-month ROCV step, no crisis flag,
# val-only PCC objective). Fold 0 only, h60/h120/h180.
#
# n_trials is set to 50 as a ceiling — the walltime is the real budget.
# search.py saves the best config after every completed trial and on
# SIGTERM (SLURM's pre-kill signal), and logs every tested/rejected trial
# to trial_history_full_h{H}d.jsonl, so a wall-time cutoff or crash never
# loses what's already been found.
# =============================================================================

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
mkdir -p logs

module load 2023 CUDA/12.1.1

PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

HORIZONS=(60 120 180)
H=${HORIZONS[$((SLURM_ARRAY_TASK_ID - 1))]}

echo "========================================"
echo "  Job ID      : ${SLURM_JOB_ID}"
echo "  Array Task  : ${SLURM_ARRAY_TASK_ID}"
echo "  Model       : CMAT-Full"
echo "  Horizon     : ${H} days"
echo "  Trials      : 50 (ceiling; walltime is the real budget)"
echo "  Fold        : 0"
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
umask 002
export PYTHONUNBUFFERED=1
export CMAT_CHECKPOINT_DIR="${REPO_ROOT}/src/models/cmat/results/checkpoints_v2"
# 2 DataLoader workers for the spatial variant: ~1.6-1.9x faster epochs than
# num_workers=0, verified memory-stable (~34GB peak, plateaued) in the
# 2026-08-26 worker test. 96G keeps 1-GPU billing (<=120G/GPU share) with
# ~2.8x headroom over the observed peak.
export CMAT_SPATIAL_NUM_WORKERS=2

"$PYTHON_BIN" -u -m src.models.cmat.search \
    --variant full \
    --horizon "$H" \
    --n-trials 50 \
    --fold 0

echo "Finished: $(date)"
