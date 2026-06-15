#!/bin/bash
#SBATCH --job-name=sarimax-lstm-v2
#SBATCH --partition=gpu_a100
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=18
#SBATCH --gpus=1
#SBATCH --mem=64G
#SBATCH --time=24:00:00
#SBATCH --output=/projects/prjs2061/energy-demand-forecast-nl/logs/sarimax_lstm_v2_%j.log
#SBATCH --mail-type=END,FAIL

# =============================================================================
# SARIMAX-LSTM baseline — 36-feature ROCV.
# SARIMAX is CPU-bound but the residual LSTM benefits from GPU.
# Allocated 24h due to SARIMAX fitting time per fold.
#
# Submit:   sbatch src/models/baselines/run_sarimax_lstm_v2.slurm
# Quick:    sbatch --time=02:00:00 src/models/baselines/run_sarimax_lstm_v2.slurm --quick
# =============================================================================

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
mkdir -p "$REPO_ROOT/logs"

module load 2023
module load Python/3.11.3-GCCcore-12.3.0
module load CUDA/12.1.1
source "${REPO_ROOT}/.venv/bin/activate"

echo "========================================"
echo "  Job ID   : ${SLURM_JOB_ID}"
echo "  Node     : $(hostname)"
echo "  CPUs     : ${SLURM_CPUS_PER_TASK}"
echo "  GPUs     : ${SLURM_GPUS}"
echo "  Memory   : ${SLURM_MEM_PER_NODE}MB"
echo "  Started  : $(date)"
echo "  Python   : $(python --version)"
echo "========================================"
echo ""

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export PYTHONUNBUFFERED=1

python -u "$REPO_ROOT/src/models/baselines/train_sarimax_lstm_v2.py" "$@"

echo ""
echo "Finished: $(date)"
