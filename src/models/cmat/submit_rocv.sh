#!/bin/bash
# ============================================================
# Submit CMAT ROCV evaluation jobs.
# One job per (variant, horizon) with --resume for crash safety.
# ============================================================

set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat.slurm"
VARIANT="${1:-full}"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  CMAT ROCV Submission"
echo "  Variant: ${VARIANT}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

mkdir -p "${REPO_ROOT}/logs"

COUNT=0
for H in 60 75 90 105 120 135 150 165 180; do
    JOB_NAME="cmat-rocv-${VARIANT}-h${H}"

    output=$(sbatch \
        --job-name="${JOB_NAME}" \
        --time=24:00:00 \
        --output="${REPO_ROOT}/logs/cmat_rocv_${VARIANT}_h${H}_%j.log" \
        "${SLURM_SCRIPT}" \
            --variant "${VARIANT}" \
            --horizon "${H}" \
            --resume 2>&1)

    JOB_ID=$(echo "$output" | grep 'Submitted batch job' | awk '{print $4}')
    echo "  H=${H}d → Job ${JOB_ID}"
    COUNT=$((COUNT + 1))
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ${COUNT} ROCV jobs submitted."
echo "  Monitor: squeue -u \$USER -n 'cmat-rocv-*'"
echo "  Results: wc -l ${REPO_ROOT}/src/models/cmat/results/rocv_results_cmat_${VARIANT}.csv"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
