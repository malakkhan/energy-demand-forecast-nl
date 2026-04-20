#!/bin/bash
#SBATCH --job-name=phase1_extract
#SBATCH --partition=rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=96
#SBATCH --time=04:00:00
#SBATCH --output=phase1_%j.log

cd "$SLURM_SUBMIT_DIR"
python -u phase1_extract.py --workers "$SLURM_CPUS_PER_TASK"
