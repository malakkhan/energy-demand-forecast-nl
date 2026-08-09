#!/bin/bash
#SBATCH --job-name=cmat-feat-importance
#SBATCH --partition=rome
#SBATCH --nodes=1
#SBATCH --cpus-per-task=18
#SBATCH --mem=64G
#SBATCH --time=18:00:00
#SBATCH --output=/projects/prjs2061/energy-demand-forecast-nl/logs/cmat-feat-importance_%j.log

set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
cd "$REPO_ROOT"
source .venv/bin/activate

FOLD=0
SEED=42
HORIZONS=(60 120 180)
VARIANTS=(tab ntl)

echo "═══════════════════════════════════════════════════════"
echo "  Feature Importance Pipeline (CPU)"
echo "  Variants: ${VARIANTS[*]}"
echo "  Horizons: ${HORIZONS[*]}"
echo "  Fold=$FOLD, Seed=$SEED"
echo "  Node: $(hostname)"
echo "  CPUs: $SLURM_CPUS_PER_TASK"
echo "═══════════════════════════════════════════════════════"

# Set PyTorch to use all available CPU cores
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export MKL_NUM_THREADS=$SLURM_CPUS_PER_TASK

STEP=1
TOTAL=12

for VARIANT in "${VARIANTS[@]}"; do
    for H in "${HORIZONS[@]}"; do
        # ── Train 1 fold WITHOUT --resume to force checkpoint creation ──
        echo ""
        echo "=== Step $STEP/$TOTAL: Training CMAT-${VARIANT^^} (fold $FOLD, h${H}d) [CPU] ==="
        .venv/bin/python3 -u -m src.models.cmat.run_rocv \
            --variant "$VARIANT" --horizon "$H" \
            --start-fold "$FOLD" --max-folds 1 --seed "$SEED"
        STEP=$((STEP + 1))

        # ── Permutation importance ──
        echo ""
        echo "=== Step $STEP/$TOTAL: Permutation Importance — CMAT-${VARIANT^^} h${H}d ==="
        .venv/bin/python3 -u -m src.models.cmat.feature_importance \
            --variant "$VARIANT" --horizon "$H" --fold "$FOLD" --seed "$SEED" \
            --n-repeats 10
        STEP=$((STEP + 1))
    done
done

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  All done — $(date)"
echo "  Results:"
for VARIANT in "${VARIANTS[@]}"; do
    for H in "${HORIZONS[@]}"; do
        echo "    results/feature_importance_${VARIANT}_h${H}d.csv"
    done
done
echo "═══════════════════════════════════════════════════════"
