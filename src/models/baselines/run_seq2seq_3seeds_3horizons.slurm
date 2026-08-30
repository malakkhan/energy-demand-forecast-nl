#!/bin/bash
#SBATCH --job-name=seq2seq
#SBATCH --partition=gpu_a100
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=18
#SBATCH --gpus=1
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/seq2seq_%A_%a.log
#SBATCH --array=1-9

# =============================================================================
# Seq2Seq ROCV: 3 Seeds x 3 Horizons (60d, 120d, 180d) on GPU (gpu_a100)
# =============================================================================

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
mkdir -p logs

module load 2023 CUDA/12.1.1

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
echo "  Model       : Seq2Seq"
echo "  Horizon     : ${H} days"
echo "  Seed        : ${SEED}"
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

"$PYTHON_BIN" -u src/models/baselines/train_seq2seq_v2.py     --horizon "$H"     --seed "$SEED"     --resume

echo "Finished: $(date)"
