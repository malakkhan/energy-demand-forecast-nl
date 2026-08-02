#!/bin/bash
#SBATCH --job-name=prophet-lstm
#SBATCH --partition=rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=24:00:00
#SBATCH --output=/projects/prjs2061/energy-demand-forecast-nl/logs/prophet_lstm_%j.log
#SBATCH --mail-type=END,FAIL

# =============================================================================
# Train the Prophet-LSTM baseline model using Rolling-Origin Cross-Validation.
#
# Submit:   sbatch src/models/baselines/train_prophet_lstm.slurm
# Monitor:  tail -f logs/prophet_lstm-<jobid>.log
# Results:  ls src/models/baselines/results/
#
# For a quick test (single fold, single horizon):
#   sbatch --time=01:00:00 src/models/baselines/train_prophet_lstm.slurm --quick
#
# For a single variant + horizon:
#   sbatch src/models/baselines/train_prophet_lstm.slurm --model-variant huang --horizon 60
# =============================================================================

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
mkdir -p "$REPO_ROOT/logs"

# ── Load modules and activate venv ────────────────────────────────────────────
module load 2023
module load Python/3.11.3-GCCcore-12.3.0
source "${REPO_ROOT}/.venv/bin/activate"

echo "========================================"
echo "  Job ID   : ${SLURM_JOB_ID}"
echo "  Node     : $(hostname)"
echo "  CPUs     : ${SLURM_CPUS_PER_TASK}"
echo "  Memory   : ${SLURM_MEM_PER_NODE}MB"
echo "  Started  : $(date)"
echo "  Python   : $(python --version)"
echo "========================================"
echo ""

# TensorFlow/Keras CPU threading
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export TF_NUM_INTEROP_THREADS=${SLURM_CPUS_PER_TASK}
export TF_NUM_INTRAOP_THREADS=${SLURM_CPUS_PER_TASK}
export TF_CPP_MIN_LOG_LEVEL=2

# Suppress Prophet's verbose cmdstanpy output
export CMDSTANPY_NO_VERBOSE=1

python -u "$REPO_ROOT/src/models/baselines/train_prophet_lstm.py" "$@"

echo ""
echo "Finished: $(date)"
