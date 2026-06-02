#!/bin/bash
#SBATCH --job-name=baselines
#SBATCH --partition=rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --time=48:00:00
#SBATCH --output=/projects/prjs2061/energy-demand-forecast-nl/logs/baseline_%A_%a.log
#SBATCH --mail-type=END,FAIL
#SBATCH --array=1-6

# =============================================================================
# Train baseline forecasting models using Rolling-Origin Cross-Validation.
#
# This uses a SLURM job array to run each model as a separate job.
# Each array task maps to one of the six baseline models:
#
#   1 = van_de_sande_mlr          (CPU,   ~1h)
#   2 = van_de_sande_prophet_lstm (CPU,   ~6h)
#   3 = ashtar_sarimax            (CPU,  ~48h)
#   4 = ashtar_sarimax_lstm       (CPU,  ~12h)
#   5 = ashtar_seq2seq            (CPU,   ~4h)
#   6 = curiel_stacked_prophet_qlstm (CPU, ~24h)
#
# Submit all:     sbatch src/models/baselines/run_baselines.slurm
# Submit one:     sbatch --array=1 src/models/baselines/run_baselines.slurm
# Quick test:     sbatch --array=1 --time=01:00:00 src/models/baselines/run_baselines.slurm
#                 (then pass --quick to the Python script below)
#
# Monitor:        tail -f logs/baseline_<jobid>_<taskid>.log
# Results:        ls src/models/baselines/results/
# =============================================================================

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
mkdir -p "$REPO_ROOT/logs"

# ── Load modules and activate venv ────────────────────────────────────────
module load 2023
module load Python/3.11.3-GCCcore-12.3.0
source "${REPO_ROOT}/.venv/bin/activate"

# ── Map array task to model name ──────────────────────────────────────────
declare -a MODELS=(
    "van_de_sande_mlr"
    "van_de_sande_prophet_lstm"
    "ashtar_sarimax"
    "ashtar_sarimax_lstm"
    "ashtar_seq2seq"
    "curiel_stacked_prophet_qlstm"
)

MODEL_IDX=$((SLURM_ARRAY_TASK_ID - 1))
MODEL_NAME=${MODELS[$MODEL_IDX]}

echo "========================================"
echo "  Job ID    : ${SLURM_JOB_ID}"
echo "  Array ID  : ${SLURM_ARRAY_TASK_ID}"
echo "  Model     : ${MODEL_NAME}"
echo "  Node      : $(hostname)"
echo "  CPUs      : ${SLURM_CPUS_PER_TASK}"
echo "  Memory    : ${SLURM_MEM_PER_NODE}MB"
echo "  Started   : $(date)"
echo "  Python    : $(python --version)"
echo "========================================"
echo ""

# ── Threading config ──────────────────────────────────────────────────────
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export NUMEXPR_MAX_THREADS=${SLURM_CPUS_PER_TASK}

# PyTorch CPU threading
export TORCH_NUM_THREADS=${SLURM_CPUS_PER_TASK}

# Suppress noisy logging
export TF_CPP_MIN_LOG_LEVEL=2
export CMDSTANPY_NO_VERBOSE=1
export PYTHONUNBUFFERED=1

# ── Run ───────────────────────────────────────────────────────────────────
python -u "$REPO_ROOT/src/models/baselines/train_baselines.py" \
    --model "$MODEL_NAME" \
    "$@"

echo ""
echo "Finished: $(date)"
