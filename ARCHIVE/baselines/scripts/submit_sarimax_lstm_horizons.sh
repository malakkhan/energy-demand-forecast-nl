#!/bin/bash
# ============================================================================
# Submit SARIMAX-LSTM ROCV as 9 parallel SLURM jobs (one per forecast horizon).
#
# Each job runs 11 ROCV folds for a single horizon. Results are safely merged
# into one CSV via file-locking (--resume flag).
#
# USAGE:
#   bash src/models/baselines/submit_sarimax_lstm_horizons.sh
#
# MONITORING:
#   squeue -u $USER
#   wc -l src/models/baselines/results/rocv_results_ashtar_sarimax_lstm.csv
#                                      # Expected: 100 lines (1 header + 99 data)
# ============================================================================

set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/baselines/run_sarimax_lstm_v2.slurm"
RESULTS_CSV="${REPO_ROOT}/src/models/baselines/results/rocv_results_ashtar_sarimax_lstm.csv"
HORIZONS=(60 75 90 105 120 135 150 165 180)

# SARIMAX(2,0,3)(1,0,1,168) is CPU-bound: ~30-90min/fold, 11 folds ~6-16h.
# 24h SLURM limit from the SLURM script should be sufficient.
TIME_LIMIT="24:00:00"

echo "Mode: GPU (${TIME_LIMIT} per job, SARIMAX is CPU-bound + LSTM on GPU)"

# ── Check existing progress ──────────────────────────────────────────────
if [[ -f "$RESULTS_CSV" ]]; then
    EXISTING=$(tail -n +2 "$RESULTS_CSV" | grep -c . || true)
    echo "Existing results: ${EXISTING}/99 folds"
    echo "  (completed folds will be skipped via --resume)"
    echo ""
fi

# ── Submit one job per horizon ───────────────────────────────────────────
echo "Submitting ${#HORIZONS[@]} SARIMAX-LSTM horizon jobs..."
echo ""

JOB_IDS=()
for H in "${HORIZONS[@]}"; do
    JOB_ID=$(sbatch \
        --job-name="sxlstm-h${H}" \
        --time="${TIME_LIMIT}" \
        --output="${REPO_ROOT}/logs/sarimax_lstm_v2_h${H}_%j.log" \
        "$SLURM_SCRIPT" --horizon "$H" --resume \
        2>&1 | grep -oP '\d+')
    JOB_IDS+=("$JOB_ID")
    printf "  Horizon %3dd → Job %s\n" "$H" "$JOB_ID"
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ${#JOB_IDS[@]} jobs submitted."
echo ""
echo "  Monitor:  squeue -u \$USER"
echo "  Logs:     ls -lt ${REPO_ROOT}/logs/sarimax_lstm_v2_h*"
echo "  Progress: wc -l ${RESULTS_CSV}"
echo "            (target: 100 lines = 1 header + 99 data rows)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
