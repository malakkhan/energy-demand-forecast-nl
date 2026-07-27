#!/bin/bash
# Sequential ROCV submission: submit ONE job at a time, wait for completion.
# This avoids the SLURM cancellation issue.
#
# Usage: nohup bash submit_rocv.sh &> logs/sequential_rocv.log &
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat.slurm"
RESULTS_DIR="${REPO_ROOT}/src/models/cmat/results"
LOG="${REPO_ROOT}/logs/sequential_rocv.log"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG"; }

# All jobs: variant:horizon:walltime
declare -a JOBS=(
    # Full — all 9 horizons, 24h wall time
    "full:60:24:00:00"
    "full:75:24:00:00"
    "full:90:24:00:00"
    "full:105:24:00:00"
    "full:120:24:00:00"
    "full:135:24:00:00"
    "full:150:24:00:00"
    "full:165:24:00:00"
    "full:180:24:00:00"
    # Early — all 9 horizons, 24h wall time
    "early:60:24:00:00"
    "early:75:24:00:00"
    "early:90:24:00:00"
    "early:105:24:00:00"
    "early:120:24:00:00"
    "early:135:24:00:00"
    "early:150:24:00:00"
    "early:165:24:00:00"
    "early:180:24:00:00"
)

TOTAL=${#JOBS[@]}
log "Starting sequential ROCV: $TOTAL jobs"

for IDX in "${!JOBS[@]}"; do
    IFS=':' read -r VARIANT HORIZON WALLTIME <<< "${JOBS[$IDX]}"
    JOB_NAME="cmat-rocv-${VARIANT}-h${HORIZON}"
    JOB_NUM=$((IDX + 1))

    # ── Skip if all 11 folds already complete ──
    CSV="${RESULTS_DIR}/rocv_results_cmat_${VARIANT}.csv"
    if [ -f "$CSV" ]; then
        DONE=$(python3 -c "
import pandas as pd
df=pd.read_csv('$CSV')
print(len(df[df['horizon_days']==${HORIZON}]))
" 2>/dev/null || echo 0)
        if [ "$DONE" -ge 11 ]; then
            log "[$JOB_NUM/$TOTAL] SKIP $JOB_NAME (all 11 folds done)"
            continue
        fi
        log "[$JOB_NUM/$TOTAL] $JOB_NAME: $DONE/11 folds done, resuming..."
    fi

    # ── Wait for queue to be empty ──
    while true; do
        RUNNING=$(squeue -u "$USER" --noheader 2>/dev/null | wc -l)
        if [ "$RUNNING" -eq 0 ]; then
            break
        fi
        sleep 60
    done

    # ── Submit ──
    OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="$WALLTIME" \
        --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
        "$SLURM_SCRIPT" rocv --variant "$VARIANT" --horizon "$HORIZON" --resume 2>&1)
    
    JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
    if [ "$JOB_ID" = "FAILED" ]; then
        log "[$JOB_NUM/$TOTAL] ERROR submitting $JOB_NAME: $OUTPUT"
        sleep 30
        continue
    fi
    log "[$JOB_NUM/$TOTAL] SUBMITTED $JOB_NAME → job $JOB_ID"

    # ── Wait for job to finish ──
    sleep 30  # give SLURM time to schedule
    while true; do
        STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
        case "$STATE" in
            COMPLETED)
                log "[$JOB_NUM/$TOTAL] COMPLETED $JOB_NAME"
                break
                ;;
            FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*)
                log "[$JOB_NUM/$TOTAL] $STATE $JOB_NAME — continuing to next"
                break
                ;;
            RUNNING|PENDING)
                sleep 120  # poll every 2 minutes
                ;;
            *)
                sleep 60
                ;;
        esac
    done

    # Brief pause between jobs
    sleep 10
done

log "Sequential ROCV complete! All $TOTAL jobs processed."

# Final summary
log "=== Results ==="
for V in full early; do
    CSV="${RESULTS_DIR}/rocv_results_cmat_${V}.csv"
    if [ -f "$CSV" ]; then
        COUNT=$(wc -l < "$CSV")
        log "  $V: $((COUNT - 1)) fold-horizon rows"
    else
        log "  $V: NO RESULTS FILE"
    fi
done
