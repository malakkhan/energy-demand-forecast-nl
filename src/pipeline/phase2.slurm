#!/bin/bash
#SBATCH --job-name=phase2_aggregate
#SBATCH --partition=rome
#SBATCH --exclusive
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --time=02:00:00
#SBATCH --output=/projects/prjs2061/energy-demand-forecast-nl/logs/phase2_%j.log

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
mkdir -p "$REPO_ROOT/logs"

# ── Load modules and activate venv ────────────────────────────────────────────
module load 2023
module load Python/3.11.3-GCCcore-12.3.0
module load Java/17.0.6
source "${REPO_ROOT}/.venv/bin/activate"

# $TMPDIR on Snellius is node-local fast SSD — phase2_aggregate.py picks it up
# automatically via _scratch_dir(), which is critical for Spark shuffle.
python -u "$REPO_ROOT/src/pipeline/phase2_aggregate.py" \
    --out-root "$REPO_ROOT/data" \
    --workers  "$SLURM_CPUS_PER_TASK" \
    --force
