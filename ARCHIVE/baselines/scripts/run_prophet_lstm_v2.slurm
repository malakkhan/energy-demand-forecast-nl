#!/bin/bash
#SBATCH --job-name=prophet-lstm-v2
#SBATCH --partition=gpu_a100
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=18
#SBATCH --gpus=1
#SBATCH --mem=64G
#SBATCH --time=18:00:00
#SBATCH --output=/projects/prjs2061/energy-demand-forecast-nl/logs/prophet_lstm_v2_%j.log
#SBATCH --mail-type=END,FAIL

# =============================================================================
# Prophet-LSTM baseline — 36-feature ROCV with GPU acceleration.
#
# Submit:   sbatch src/models/baselines/run_prophet_lstm_v2.slurm
# Quick:    sbatch --time=01:00:00 src/models/baselines/run_prophet_lstm_v2.slurm --quick
# =============================================================================

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
mkdir -p "$REPO_ROOT/logs"

# ── Load modules and activate venv ────────────────────────────────────────
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
echo "  PyTorch  : $(python -c 'import torch; print(torch.__version__)')"
echo "  CUDA avl : $(python -c 'import torch; print(torch.cuda.is_available())')"
echo "========================================"
echo ""

# ── Threading ─────────────────────────────────────────────────────────────
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export CMDSTANPY_NO_VERBOSE=1
export PYTHONUNBUFFERED=1

# ── Run ───────────────────────────────────────────────────────────────────
python -u "$REPO_ROOT/src/models/baselines/train_prophet_lstm_v2.py" "$@"

echo ""
echo "Finished: $(date)"
