#!/bin/bash
# ============================================================
# Submit Optuna hyperparameter search jobs for CMAT.
# One job per (variant, horizon) — per-horizon search as specified.
# ============================================================

set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat.slurm"
VARIANT="${1:-full}"
N_TRIALS="${2:-50}"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  CMAT Optuna Search Submission"
echo "  Variant:  ${VARIANT}"
echo "  Trials:   ${N_TRIALS} per horizon"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

mkdir -p "${REPO_ROOT}/logs"

COUNT=0
for H in 60 75 90 105 120 135 150 165 180; do
    JOB_NAME="cmat-search-${VARIANT}-h${H}"

    output=$(sbatch \
        --job-name="${JOB_NAME}" \
        --time=24:00:00 \
        --output="${REPO_ROOT}/logs/cmat_search_${VARIANT}_h${H}_%j.log" \
        "${SLURM_SCRIPT}" search \
            --variant "${VARIANT}" \
            --horizon "${H}" \
            --n-trials "${N_TRIALS}" 2>&1)

    JOB_ID=$(echo "$output" | grep 'Submitted batch job' | awk '{print $4}')
    echo "  H=${H}d → Job ${JOB_ID}"
    COUNT=$((COUNT + 1))
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ${COUNT} search jobs submitted."
echo "  Monitor: squeue -u \$USER -n 'cmat-search-*'"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
