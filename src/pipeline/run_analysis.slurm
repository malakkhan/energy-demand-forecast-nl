#!/bin/bash
#SBATCH --job-name=ts-analysis
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --partition=rome
#SBATCH --output=logs/analysis-%j.out
#SBATCH --mail-type=END,FAIL

# =============================================================================
# Run the full time series analysis as a batch job.
# All figures are saved to analysis/figures/*.png — no display required.
#
# Submit:   sbatch src/pipeline/run_analysis.slurm
# Monitor:  tail -f logs/analysis-<jobid>.out
# Figures:  ls analysis/figures/
# =============================================================================

module load 2023
module load Python/3.11.3-GCCcore-12.3.0

REPO="/projects/prjs2061/energy-demand-forecast-nl"
source "${REPO}/.venv/bin/activate"

echo "========================================"
echo "  Job ID   : ${SLURM_JOB_ID}"
echo "  Node     : $(hostname)"
echo "  Started  : $(date)"
echo "  Python   : $(python --version)"
echo "========================================"
echo ""

python "${REPO}/analysis/run_analysis.py"

echo ""
echo "Finished: $(date)"
