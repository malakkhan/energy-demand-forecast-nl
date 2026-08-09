#!/bin/bash
# ============================================================
# CMAT-Full ROCV — SLURM dependency chain (no fragile bash chainer)
#
# Submits ALL folds upfront with SLURM --dependency chains.
# Each seed's folds run sequentially; seeds run in parallel.
# Uses --resume to skip already-completed folds.
#
# Usage: bash submit_rocv_chain.sh <horizon> [--after JOBID1,JOBID2,...]
#   e.g.: bash submit_rocv_chain.sh 120 --after 25381700,25379628
# ============================================================
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat_h100.slurm"
VARIANT="full"
SEEDS=(42 123 456)
HORIZON=${1:?Usage: $0 <horizon_days> [--after JOBID1,JOBID2,...]}

# Optional: wait for previous jobs
AFTER_DEPS=""
if [ "${2:-}" = "--after" ] && [ -n "${3:-}" ]; then
    AFTER_DEPS="--dependency=afterany:${3}"
    echo "Will wait for jobs: ${3}"
fi

echo "═══════════════════════════════════════════════════════"
echo "  CMAT-Full h${HORIZON} ROCV — SLURM Dependency Chain"
echo "  Seeds: ${SEEDS[*]}"
echo "  Folds: 0-10 (11 per seed, 33 total)"
echo "  Each fold uses --resume to skip if already done"
echo "═══════════════════════════════════════════════════════"

for SEED in "${SEEDS[@]}"; do
    echo ""
    echo "--- Seed ${SEED} ---"
    PREV_JOB=""

    for FOLD in $(seq 0 10); do
        JOB_NAME="cmat-full-h${HORIZON}-s${SEED}-f${FOLD}"

        # Build dependency string
        DEP_FLAG=""
        if [ -n "$PREV_JOB" ]; then
            DEP_FLAG="--dependency=afterany:${PREV_JOB}"
        elif [ -n "$AFTER_DEPS" ]; then
            DEP_FLAG="$AFTER_DEPS"
        fi

        OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="08:00:00" \
            --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
            ${DEP_FLAG} \
            "$SLURM_SCRIPT" rocv \
            --variant "$VARIANT" --horizon "$HORIZON" \
            --start-fold "$FOLD" --max-folds 1 \
            --seed "$SEED" 2>&1)

        JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
        if [ "$JOB_ID" = "FAILED" ]; then
            echo "  ERROR: $JOB_NAME: $OUTPUT"
            break
        fi

        DEP_INFO=""
        if [ -n "$DEP_FLAG" ]; then
            DEP_INFO=" (after ${PREV_JOB:-initial})"
        fi
        echo "  $JOB_NAME -> $JOB_ID${DEP_INFO}"

        PREV_JOB=$JOB_ID
    done
done

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  All jobs submitted. SLURM manages the chain."
echo "  Monitor: squeue -u \$USER -o '%.10i %.40j %.8T %.12M'"
echo "  No bash chainer needed — survives server restarts."
echo "═══════════════════════════════════════════════════════"
