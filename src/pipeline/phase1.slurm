#!/bin/bash
#SBATCH --job-name=phase1_extract
#SBATCH --partition=rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=96
#SBATCH --time=04:00:00
#SBATCH --output=/projects/prjs2061/energy-demand-forecast-nl/logs/phase1_%j.log

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
mkdir -p "$REPO_ROOT/logs"

# ── Load modules and activate venv ────────────────────────────────────────────
module load 2023
module load Python/3.11.3-GCCcore-12.3.0
source "${REPO_ROOT}/.venv/bin/activate"

python -u "$REPO_ROOT/src/pipeline/phase1_extract.py" \
    --data-root /projects/prjs2061/data \
    --out-root  "$REPO_ROOT/data" \
    --workers   "$SLURM_CPUS_PER_TASK"
