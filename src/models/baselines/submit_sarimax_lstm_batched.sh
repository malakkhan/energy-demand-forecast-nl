#!/bin/bash
# ============================================================================
# Submit SARIMAX-LSTM ROCV as batched SLURM jobs to complete all 99 folds.
#
# Strategy:
#   - Each SARIMAX fold takes ~3.5-5.2 hours (SARIMAX fitting is CPU-bound).
#   - With a 24h time limit, ~4-5 folds can complete per job.
#   - For horizons 60/75/90d: 5 folds already done, 6 remain → 1 job each.
#   - For horizons 105-180d: 0 folds done, 11 remain → 3 chained jobs each.
#   - All jobs use --resume so completed folds are safely skipped.
#   - File-locking ensures parallel-safe CSV writes.
#
# USAGE:
#   bash src/models/baselines/submit_sarimax_lstm_batched.sh
#
# MONITORING:
#   squeue -u $USER -n "sxl-*"
#   wc -l src/models/baselines/results/rocv_results_ashtar_sarimax_lstm.csv
#                                      # Target: 100 lines (1 header + 99 data)
# ============================================================================

set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/baselines/run_sarimax_lstm_v2.slurm"
RESULTS_CSV="${REPO_ROOT}/src/models/baselines/results/rocv_results_ashtar_sarimax_lstm.csv"
TIME_LIMIT="24:00:00"
JOB_COUNT=0

# Helper: submit one job and print its ID
do_submit() {
    local horizon=$1
    local tag=$2
    local dep_arg="${3:-}"  # optional: "--dependency=afterany:JOBID"

    local output=$(sbatch \
        --job-name="sxl-h${horizon}-${tag}" \
        --time="${TIME_LIMIT}" \
        --output="${REPO_ROOT}/logs/sarimax_lstm_v2_h${horizon}_${tag}_%j.log" \
        ${dep_arg} \
        "$SLURM_SCRIPT" --horizon "$horizon" --resume 2>&1)

    local job_id=$(echo "$output" | grep 'Submitted batch job' | awk '{print $4}')

    if [[ -z "$job_id" ]]; then
        echo "  ⚠ H=${horizon}d batch ${tag}: sbatch failed!" >&2
        echo "$output" >&2
        return 1
    fi

    printf "  H=%3dd  batch %-3s → Job %s" "$horizon" "$tag" "$job_id"
    if [[ -n "$dep_arg" ]]; then
        printf "  (chained)"
    fi
    echo ""

    JOB_COUNT=$((JOB_COUNT + 1))
    # Store job ID in a variable for chaining
    LAST_JOB_ID="$job_id"
}

# ── Show status ──────────────────────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SARIMAX-LSTM Batched Submission"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ -f "$RESULTS_CSV" ]]; then
    EXISTING=$(tail -n +2 "$RESULTS_CSV" | grep -c . || true)
    echo "  Existing results: ${EXISTING}/99 folds completed"
    echo ""
    echo "  Per-horizon status:"
    for H in 60 75 90 105 120 135 150 165 180; do
        COUNT=$(tail -n +2 "$RESULTS_CSV" | awk -F, -v h="$H" '$4==h' | wc -l)
        REMAINING=$((11 - COUNT))
        if [[ $REMAINING -eq 0 ]]; then
            echo "    H=${H}d: ${COUNT}/11 ✓ COMPLETE"
        else
            echo "    H=${H}d: ${COUNT}/11 (${REMAINING} remaining)"
        fi
    done
else
    echo "  No existing results found — starting fresh."
fi

echo ""
echo "  Time limit: ${TIME_LIMIT} per job"
echo ""
echo "Submitting jobs..."
echo ""

# ── Horizons 60, 75, 90: partially done → 1 job each ────────────────
echo "--- Partially completed horizons (1 job each) ---"
for H in 60 75 90; do
    do_submit "$H" "b1"
done
echo ""

# ── Horizons 105-180: not started → 3 chained jobs each ─────────────
echo "--- New horizons (3 chained jobs each) ---"
for H in 105 120 135 150 165 180; do
    do_submit "$H" "b1"
    JOB_A="$LAST_JOB_ID"

    do_submit "$H" "b2" "--dependency=afterany:${JOB_A}"
    JOB_B="$LAST_JOB_ID"

    do_submit "$H" "b3" "--dependency=afterany:${JOB_B}"
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ${JOB_COUNT} jobs submitted total."
echo ""
echo "  Monitor:   squeue -u \$USER"
echo "  Logs:      ls -lt ${REPO_ROOT}/logs/sarimax_lstm_v2_h*"
echo "  Progress:  wc -l ${RESULTS_CSV}"
echo "             (target: 100 lines = 1 header + 99 data rows)"
echo ""
echo "  h60/75/90:  1 job each (6 folds remaining per horizon)"
echo "  h105-180:   3 chained jobs each (11 folds, ~4-5 per batch)"
echo "  All jobs use --resume — re-running is always safe."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
