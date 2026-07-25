#!/bin/bash
#SBATCH --partition=gpu_a100
#SBATCH --gpus=1
#SBATCH --cpus-per-task=18
#SBATCH --mem=64G
#SBATCH --time=24:00:00
#SBATCH --output=logs/cmat_%j.log
#SBATCH --mail-type=END,FAIL

# ============================================================
# CMAT GPU SLURM Job
# ============================================================
# Usage (search mode):
#   sbatch run_cmat.slurm search --variant full --horizon 60 --n-trials 50
#
# Usage (ROCV mode — default):
#   sbatch run_cmat.slurm --variant full --horizon 60 --resume
# ============================================================

set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
cd "$REPO_ROOT"
source .venv/bin/activate
mkdir -p "$REPO_ROOT/logs"

echo "═══════════════════════════════════════════════════════"
echo "  CMAT Job — $(date)"
echo "  Node:    $(hostname)"
echo "  GPU:     $(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null || echo 'N/A')"
echo "  PyTorch: $(python3 -c 'import torch; print(torch.__version__)')"
echo "  CUDA:    $(python3 -c 'import torch; print(torch.cuda.is_available())')"
echo "  Args:    $@"
echo "═══════════════════════════════════════════════════════"

# Check if first argument is "search" (mode selector)
if [[ "${1:-rocv}" == "search" ]]; then
    shift
    echo "Mode: Optuna hyperparameter search"
    python3 -u -m src.models.cmat.search "$@"
else
    echo "Mode: ROCV evaluation"
    python3 -u -m src.models.cmat.run_rocv "$@"
fi

echo "═══════════════════════════════════════════════════════"
echo "  Done — $(date)"
echo "═══════════════════════════════════════════════════════"
