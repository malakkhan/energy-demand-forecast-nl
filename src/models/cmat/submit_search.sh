#!/bin/bash
# ============================================================
# Submit Optuna hyperparameter search jobs for CMAT.
# One job per (variant, horizon) — per-horizon search.
# Submits in parallel with a concurrency throttle.
# ============================================================

set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat.slurm"
VARIANT="${1:-tab}"
N_TRIALS="${2:-50}"
MAX_CONCURRENT="${3:-4}"  # max parallel jobs (conservative)
WALLTIME="14:00:00"       # 14h — plenty for 50 trials of Tab/NTL

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  CMAT Optuna Search Submission"
echo "  Variant:        ${VARIANT}"
echo "  Trials:         ${N_TRIALS} per horizon"
echo "  Max concurrent: ${MAX_CONCURRENT}"
echo "  Wall time:      ${WALLTIME}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

mkdir -p "${REPO_ROOT}/logs"

COUNT=0
for H in 60 75 90 105 120 135 150 165 180; do
    JOB_NAME="cmat-search-${VARIANT}-h${H}"

    # Throttle: wait until we're below MAX_CONCURRENT
    while true; do
        RUNNING=$(squeue -u "$USER" --noheader 2>/dev/null | wc -l)
        if [ "$RUNNING" -lt "$MAX_CONCURRENT" ]; then
            break
        fi
        echo "  Throttle: $RUNNING jobs running (max $MAX_CONCURRENT), waiting..."
        sleep 60
    done

    output=$(sbatch \
        --job-name="${JOB_NAME}" \
        --time="${WALLTIME}" \
        --output="${REPO_ROOT}/logs/cmat_search_${VARIANT}_h${H}_%j.log" \
        "${SLURM_SCRIPT}" search \
            --variant "${VARIANT}" \
            --horizon "${H}" \
            --n-trials "${N_TRIALS}" 2>&1)

    JOB_ID=$(echo "$output" | grep -o '[0-9]*$' || echo "FAILED")
    echo "  H=${H}d → Job ${JOB_ID}"
    COUNT=$((COUNT + 1))
    sleep 2  # brief pause between submissions
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ${COUNT} search jobs submitted for ${VARIANT}."
echo "  Monitor: squeue -u \$USER"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
