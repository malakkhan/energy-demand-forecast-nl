#!/bin/bash
# ============================================================
# CMAT-Full Search + ROCV End-to-End Pipeline
# ============================================================
# 1. Submits Optuna searches (20 trials) for h60, h120, h180 in parallel.
# 2. Waits for all searches to finish.
# 3. Kicks off submit_full_rocv.sh to evaluate all folds using the new configs.
#
# Usage: nohup bash submit_search_then_rocv_h100.sh &> logs/pipeline.log &
# ============================================================
set -euo pipefail

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat_h100.slurm"
LOG="${REPO_ROOT}/logs/pipeline.log"
mkdir -p "${REPO_ROOT}/logs"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG"; }

# ── 1. Submit Optuna Search Jobs ──
log "=== PHASE 1: Optuna Search ==="
declare -a SEARCH_JOBS=()

for HORIZON in 60 120 180; do
    JOB_NAME="cmat-search-full-h${HORIZON}"
    log "Submitting search for horizon $HORIZON (20 trials)..."
    
    OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="12:00:00" \
        --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
        "$SLURM_SCRIPT" search \
        --variant full --horizon "$HORIZON" --n-trials 20 2>&1)
        
    JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
    if [ "$JOB_ID" = "FAILED" ]; then
        log "ERROR submitting $JOB_NAME: $OUTPUT"
        exit 1
    fi
    log "SUBMITTED $JOB_NAME -> $JOB_ID"
    SEARCH_JOBS+=("$JOB_ID")
done

log "All search jobs submitted. Waiting for completion..."

# ── 2. Wait for Search Jobs ──
for JOB_ID in "${SEARCH_JOBS[@]}"; do
    while true; do
        STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
        case "$STATE" in
            COMPLETED)
                log "Job $JOB_ID COMPLETED."
                break ;;
            FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*)
                log "⚠️ Job $JOB_ID ended with state $STATE!"
                log "Aborting pipeline. Check logs."
                exit 1 ;;
            RUNNING|PENDING)
                sleep 60 ;;
            *)
                sleep 60 ;;
        esac
    done
done

log "=== PHASE 1 COMPLETE: All hyperparameter searches finished successfully. ==="

# ── 3. Kick off ROCV Pipeline ──
log "=== PHASE 2: ROCV Evaluation ==="
log "Launching submit_full_rocv.sh..."

cd "$REPO_ROOT"
# Run the ROCV submitter script directly so it runs sequentially
bash src/models/cmat/submit_full_rocv.sh

log "=== END-TO-END PIPELINE COMPLETE ==="
