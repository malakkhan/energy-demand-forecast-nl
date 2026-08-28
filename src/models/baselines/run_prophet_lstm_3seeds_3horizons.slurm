#!/bin/bash
#SBATCH --job-name=prophet-lstm
#SBATCH --partition=fat_rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=03:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/prophet_lstm_%A_%a.log
#SBATCH --array=1-9

# =============================================================================
# Prophet-LSTM ROCV: 3 Seeds x 3 Horizons (60d, 120d, 180d) on CPU
# =============================================================================

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
mkdir -p logs

PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

SEEDS=(42 123 7)
HORIZONS=(60 120 180)

# Map SLURM_ARRAY_TASK_ID (1..9) to (Horizon, Seed)
TASK_IDX=$((SLURM_ARRAY_TASK_ID - 1))
H_IDX=$((TASK_IDX / 3))
S_IDX=$((TASK_IDX % 3))

H=${HORIZONS[$H_IDX]}
SEED=${SEEDS[$S_IDX]}

echo "========================================"
echo "  Job ID      : ${SLURM_JOB_ID}"
echo "  Array Task  : ${SLURM_ARRAY_TASK_ID}"
echo "  Model       : Prophet-LSTM"
echo "  Horizon     : ${H} days"
echo "  Seed        : ${SEED}"
echo "  Node        : $(hostname)"
echo "  CPUs        : ${SLURM_CPUS_PER_TASK}"
echo "  Started     : $(date)"
echo "  Python      : $($PYTHON_BIN --version)"
echo "========================================"

export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export CMDSTANPY_NO_VERBOSE=1
umask 002
export PYTHONUNBUFFERED=1

"$PYTHON_BIN" -u src/models/baselines/train_prophet_lstm_v2.py     --horizon "$H"     --seed "$SEED"     --resume

echo "Finished: $(date)"
