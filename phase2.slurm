#!/bin/bash
#SBATCH --job-name=phase2_aggregate
#SBATCH --partition=rome
#SBATCH --exclusive
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --time=02:00:00
#SBATCH --output=phase2_%j.log

cd "$SLURM_SUBMIT_DIR"
# $TMPDIR on Snellius is node-local fast SSD — phase2_aggregate.py picks it up
# automatically via _scratch_dir(), which is critical for Spark shuffle.
python -u phase2_aggregate.py --workers "$SLURM_CPUS_PER_TASK"
