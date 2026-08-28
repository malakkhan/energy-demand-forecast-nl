#!/bin/bash
# ============================================================
# CMAT-Full h60 ROCV — 3 seeds × 11 folds
# Submits 1 fold per job (Full folds take 3-5h each).
# Uses --resume to skip already-completed folds.
#
# Usage: nohup bash submit_full_h60_rocv_only.sh &>> logs/pipeline_full_h60_rocv.log &
# ============================================================
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat_h100.slurm"
VARIANT="full"
SEEDS=(42 123 456)

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*"; }

wait_for_job() {
    local JOB_ID=$1
    local JOB_NAME=$2
    while true; do
        STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
        case "$STATE" in
            COMPLETED) log "  [✅] $JOB_NAME COMPLETED."; return 0 ;;
            FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*) log "  [⚠️] $JOB_NAME: $STATE"; return 1 ;;
            RUNNING|PENDING) sleep 120 ;;
            *) sleep 120 ;;
        esac
    done
}

log "=== h60 Full ROCV: 3 seeds × 11 folds (1 fold per job) ==="

for SEED in "${SEEDS[@]}"; do
    log "--- Seed ${SEED} ---"
    for FOLD in $(seq 0 10); do
        JOB_NAME="cmat-full-h60-s${SEED}-f${FOLD}"

        OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="08:00:00" \
            --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
            "$SLURM_SCRIPT" rocv \
            --variant "$VARIANT" --horizon 60 \
            --start-fold "$FOLD" --max-folds 1 \
            --seed "$SEED" --resume 2>&1)

        JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
        if [ "$JOB_ID" = "FAILED" ]; then
            log "ERROR submitting $JOB_NAME: $OUTPUT"
            continue
        fi
        log "  Submitted $JOB_NAME -> $JOB_ID"
        wait_for_job "$JOB_ID" "$JOB_NAME"
    done
    log "=== Seed ${SEED} complete ==="
done

log "=== ALL DONE: h60 Full ROCV (3 seeds × 11 folds) ==="

RESULTS_CSV="${REPO_ROOT}/src/models/cmat/results/rocv_results_cmat_full.csv"
if [ -f "$RESULTS_CSV" ]; then
    ROWS=$(wc -l < "$RESULTS_CSV")
    log "Results: $RESULTS_CSV ($((ROWS - 1)) data rows)"
fi
