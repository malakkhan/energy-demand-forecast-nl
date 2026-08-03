#!/bin/bash
#SBATCH --partition=gpu_h100
#SBATCH --gpus=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=160G
#SBATCH --time=08:00:00
#SBATCH --constraint=scratch-node
#SBATCH --output=logs/cmat_%j.log
#SBATCH --mail-type=END,FAIL

# ============================================================
# CMAT GPU SLURM Job — H100 + NVMe staging
# ============================================================
# Usage (ROCV mode):
#   sbatch run_cmat_h100.slurm --variant full --horizon 60 --resume
#
# NVMe staging: VIIRS data (85GB) is rsynced to $TMPDIR at job
# start for ~10x faster NTL image preloading.
# ============================================================

set -euo pipefail

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
VIIRS_SRC=/scratch-shared/prjs2061_copy/data/viirs/A2

cd "$REPO_ROOT"
source .venv/bin/activate
mkdir -p "$REPO_ROOT/logs"

echo "═══════════════════════════════════════════════════════"
echo "  CMAT Job (H100 + NVMe) — $(date)"
echo "  Node:    $(hostname)"
echo "  GPU:     $(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null || echo 'N/A')"
echo "  PyTorch: $(.venv/bin/python3 -c 'import torch; print(torch.__version__)')"
echo "  CUDA:    $(.venv/bin/python3 -c 'import torch; print(torch.cuda.is_available())')"
echo "  TMPDIR:  $TMPDIR"
echo "  Args:    $@"
echo "═══════════════════════════════════════════════════════"

# ── Stage VIIRS data to NVMe scratch ──
echo "Staging VIIRS data to NVMe ($TMPDIR) ..."
STAGE_START=$(date +%s)
VIIRS_LOCAL="$TMPDIR/viirs/A2"
mkdir -p "$VIIRS_LOCAL"
rsync -a --info=progress2 "$VIIRS_SRC/" "$VIIRS_LOCAL/"
STAGE_END=$(date +%s)
echo "Staging complete: $((STAGE_END - STAGE_START))s"
echo "Local VIIRS: $(du -sh "$VIIRS_LOCAL" | cut -f1)"

export CMAT_VIIRS_DIR="$VIIRS_LOCAL"
echo "CMAT_VIIRS_DIR=$CMAT_VIIRS_DIR"

# ── Run Python Script ──
if [[ "${1:-rocv}" == "search" ]]; then
    shift
    echo "Mode: Optuna hyperparameter search"
    .venv/bin/python3 -u -m src.models.cmat.search "$@"
elif [[ "${1:-}" == "rocv" ]]; then
    shift
    echo "Mode: ROCV evaluation"
    .venv/bin/python3 -u -m src.models.cmat.run_rocv "$@"
else
    echo "Mode: ROCV evaluation (default)"
    .venv/bin/python3 -u -m src.models.cmat.run_rocv "$@"
fi

echo "═══════════════════════════════════════════════════════"
echo "  Done — $(date)"
echo "═══════════════════════════════════════════════════════"
