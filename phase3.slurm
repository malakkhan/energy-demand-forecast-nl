#!/bin/bash
#SBATCH --job-name=phase3_merge
#SBATCH --partition=rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=64G
#SBATCH --time=00:30:00
#SBATCH --output=phase3_%j.log

cd "$SLURM_SUBMIT_DIR"
python -u phase3_merge.py --workers "$SLURM_CPUS_PER_TASK"
