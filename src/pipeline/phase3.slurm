#!/bin/bash
#SBATCH --job-name=phase3_merge
#SBATCH --partition=rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=00:30:00
#SBATCH --output=/projects/prjs2061/energy-demand-forecast-nl/logs/phase3_%j.log

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
mkdir -p "$REPO_ROOT/logs"

python -u "$REPO_ROOT/src/pipeline/phase3_merge.py" \
    --out-root "$REPO_ROOT/data" \
    --workers  "$SLURM_CPUS_PER_TASK"
