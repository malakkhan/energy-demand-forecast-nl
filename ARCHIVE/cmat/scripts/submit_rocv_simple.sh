#!/bin/bash
# Simple sequential ROCV submitter — one job at a time.
# All search configs are ready. Submit ROCV for Tab then NTL.
# Usage: nohup bash submit_rocv_simple.sh &>> logs/rocv_simple.log &
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat.slurm"
RESULTS_DIR="${REPO_ROOT}/src/models/cmat/results"
WALLTIME="02:00:00"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*"; }

# Job list
declare -a JOBS=(
    "tab:60" "tab:75" "tab:90" "tab:105" "tab:120" "tab:135" "tab:150" "tab:165" "tab:180"
    "ntl:60" "ntl:75" "ntl:90" "ntl:105" "ntl:120" "ntl:135" "ntl:150" "ntl:165" "ntl:180"
)
TOTAL=${#JOBS[@]}
log "=== ROCV submitter: $TOTAL jobs, wall=$WALLTIME ==="

for IDX in "${!JOBS[@]}"; do
    IFS=':' read -r VARIANT HORIZON <<< "${JOBS[$IDX]}"
    JOB_NAME="cmat-rocv-${VARIANT}-h${HORIZON}"
    JOB_NUM=$((IDX + 1))

    # Skip if already complete (11 folds)
    CSV="${RESULTS_DIR}/rocv_results_cmat_${VARIANT}.csv"
    if [ -f "$CSV" ]; then
        DONE=$(python3 -c "import pandas as pd; df=pd.read_csv('$CSV'); print(len(df[df['horizon_days']==$HORIZON]))" 2>/dev/null || echo 0)
        if [ "$DONE" -ge 11 ]; then
            log "[$JOB_NUM/$TOTAL] SKIP $JOB_NAME ($DONE folds done)"
            continue
        fi
        log "[$JOB_NUM/$TOTAL] $JOB_NAME: $DONE/11 folds done, resuming"
    fi

    # Wait for queue to be empty
    while true; do
        RUNNING=$(squeue -u "$USER" --noheader 2>/dev/null | wc -l)
        if [ "$RUNNING" -eq 0 ]; then break; fi
        log "[$JOB_NUM/$TOTAL] Waiting ($RUNNING jobs running)..."
        sleep 60
    done

    # Submit
    output=$(sbatch --job-name="$JOB_NAME" --time="$WALLTIME" \
        --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
        "$SLURM_SCRIPT" rocv --variant "$VARIANT" --horizon "$HORIZON" \
        --resume 2>&1)

    JOB_ID=$(echo "$output" | grep -o '[0-9]*$' || echo "FAILED")
    if [ "$JOB_ID" = "FAILED" ]; then
        log "[$JOB_NUM/$TOTAL] ERROR: $output"
        sleep 30
        continue
    fi
    log "[$JOB_NUM/$TOTAL] SUBMITTED $JOB_NAME → $JOB_ID"

    # Wait for completion
    sleep 30
    while true; do
        STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
        case "$STATE" in
            COMPLETED)
                log "[$JOB_NUM/$TOTAL] ✅ $JOB_NAME COMPLETED"
                break ;;
            FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*)
                log "[$JOB_NUM/$TOTAL] ⚠️  $JOB_NAME $STATE"
                LOGFILE="${REPO_ROOT}/logs/${JOB_NAME}_${JOB_ID}.log"
                [ -f "$LOGFILE" ] && tail -5 "$LOGFILE" | while read -r l; do log "    $l"; done
                break ;;
            RUNNING|PENDING)
                sleep 60 ;;
            *)
                sleep 30 ;;
        esac
    done
    sleep 5
done

log "=== All ROCV jobs processed! ==="
# Summary
for V in tab ntl; do
    CSV="${RESULTS_DIR}/rocv_results_cmat_${V}.csv"
    if [ -f "$CSV" ]; then
        python3 -c "
import pandas as pd
df=pd.read_csv('$CSV')
print('  $V:')
for h in sorted(df['horizon_days'].unique()):
    s=df[df['horizon_days']==h]
    print(f'    H={h:>3d}d: {len(s)}/11 folds, MAPE={s[\"mape_pct\"].mean():.2f}%')
" 2>/dev/null
    fi
done
