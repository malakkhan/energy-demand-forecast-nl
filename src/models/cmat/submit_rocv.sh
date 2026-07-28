#!/bin/bash
# Sequential Full ROCV: 3 horizons (60, 120, 180) × 5 folds (6-10)
# Budget: ~10K SBU of 31K remaining
#
# Usage: nohup bash submit_rocv.sh &> logs/sequential_rocv.log &
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat.slurm"
RESULTS_DIR="${REPO_ROOT}/src/models/cmat/results"
LOG="${REPO_ROOT}/logs/sequential_rocv.log"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG"; }

# Jobs: variant:horizon:walltime
# Only Full variant, 3 key horizons, latter 5 folds (6-10)
declare -a JOBS=(
    "full:60:24:00:00"
    "full:120:24:00:00"
    "full:180:24:00:00"
)

START_FOLD=6
MAX_FOLDS=5
TOTAL=${#JOBS[@]}

log "Starting sequential ROCV: $TOTAL jobs (folds $START_FOLD to $((START_FOLD + MAX_FOLDS - 1)))"

for IDX in "${!JOBS[@]}"; do
    IFS=':' read -r VARIANT HORIZON WALLTIME <<< "${JOBS[$IDX]}"
    JOB_NAME="cmat-rocv-${VARIANT}-h${HORIZON}"
    JOB_NUM=$((IDX + 1))

    # ── Skip if target folds already complete ──
    CSV="${RESULTS_DIR}/rocv_results_cmat_${VARIANT}.csv"
    if [ -f "$CSV" ]; then
        DONE=$(python3 -c "
import pandas as pd
df=pd.read_csv('$CSV')
target_folds = list(range($START_FOLD, $START_FOLD + $MAX_FOLDS))
done = len(df[(df['horizon_days']==$HORIZON) & (df['fold_idx'].isin(target_folds))])
print(done)
" 2>/dev/null || echo 0)
        if [ "$DONE" -ge "$MAX_FOLDS" ]; then
            log "[$JOB_NUM/$TOTAL] SKIP $JOB_NAME (all $MAX_FOLDS target folds done)"
            continue
        fi
        log "[$JOB_NUM/$TOTAL] $JOB_NAME: $DONE/$MAX_FOLDS target folds done"
    fi

    # ── Wait for queue to be empty ──
    while true; do
        RUNNING=$(squeue -u "$USER" --noheader 2>/dev/null | wc -l)
        if [ "$RUNNING" -eq 0 ]; then
            break
        fi
        log "[$JOB_NUM/$TOTAL] Waiting for queue to clear ($RUNNING jobs running)..."
        sleep 120
    done

    # ── Submit ──
    OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="$WALLTIME" \
        --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
        "$SLURM_SCRIPT" rocv --variant "$VARIANT" --horizon "$HORIZON" \
        --resume --start-fold "$START_FOLD" --max-folds "$MAX_FOLDS" 2>&1)
    
    JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
    if [ "$JOB_ID" = "FAILED" ]; then
        log "[$JOB_NUM/$TOTAL] ERROR submitting $JOB_NAME: $OUTPUT"
        sleep 30
        continue
    fi
    log "[$JOB_NUM/$TOTAL] SUBMITTED $JOB_NAME → job $JOB_ID (folds $START_FOLD-$((START_FOLD+MAX_FOLDS-1)))"

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
CSV="${RESULTS_DIR}/rocv_results_cmat_full.csv"
if [ -f "$CSV" ]; then
    python3 -c "
import pandas as pd
df = pd.read_csv('$CSV')
for h in [60, 120, 180]:
    subset = df[df['horizon_days'] == h]
    if len(subset) > 0:
        print(f'  H={h:>3d}d: {len(subset)} folds, MAPE={subset[\"mape_pct\"].mean():.2f}% ± {subset[\"mape_pct\"].std():.2f}%')
" 2>/dev/null || true
fi
