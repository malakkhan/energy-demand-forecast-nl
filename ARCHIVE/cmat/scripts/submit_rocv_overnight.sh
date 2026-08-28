#!/bin/bash
# ============================================================
# Overnight ROCV submitter: waits for search to finish, then
# runs ROCV for all Tab/NTL horizons, 2 jobs at a time.
#
# Usage: nohup bash submit_rocv_overnight.sh &>> logs/rocv_overnight.log &
# ============================================================
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat.slurm"
RESULTS_DIR="${REPO_ROOT}/src/models/cmat/results"
ROCV_WALLTIME="02:00:00"
MAX_CONCURRENT=2

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*"; }

# ── Phase 1: Wait for all search jobs to finish ──
log "=== Phase 1: Waiting for search jobs to complete ==="
while true; do
    SEARCH_RUNNING=$(squeue -u "$USER" --noheader --name="cmat-search-tab-h60,cmat-search-tab-h75,cmat-search-tab-h90,cmat-search-tab-h105,cmat-search-tab-h120,cmat-search-tab-h135,cmat-search-tab-h150,cmat-search-tab-h165,cmat-search-tab-h180,cmat-search-ntl-h60,cmat-search-ntl-h75,cmat-search-ntl-h90,cmat-search-ntl-h105,cmat-search-ntl-h120,cmat-search-ntl-h135,cmat-search-ntl-h150,cmat-search-ntl-h165,cmat-search-ntl-h180" 2>/dev/null | wc -l)
    # Also check if the sequential submitter is still running
    SUBMITTER=$(pgrep -c -f "submit_search_sequential" 2>/dev/null || echo 0)

    if [ "$SEARCH_RUNNING" -eq 0 ] && [ "$SUBMITTER" -eq 0 ]; then
        break
    fi
    log "  Search: $SEARCH_RUNNING jobs running, submitter alive=$SUBMITTER"
    sleep 120
done

# ── Verify all configs exist ──
log "=== Phase 1 complete. Verifying configs ==="
MISSING=0
for V in tab ntl; do
    for H in 60 75 90 105 120 135 150 165 180; do
        CFG="${RESULTS_DIR}/best_config_${V}_h${H}d.json"
        if [ ! -f "$CFG" ]; then
            log "  ❌ MISSING: $CFG"
            MISSING=$((MISSING + 1))
        fi
    done
done
if [ "$MISSING" -gt 0 ]; then
    log "  ⚠️  $MISSING configs missing — proceeding with available ones"
fi

# ── Phase 2: Submit ROCV jobs ──
log "=== Phase 2: Submitting ROCV jobs (max $MAX_CONCURRENT concurrent) ==="

# Build job list
declare -a ROCV_JOBS=()
for V in tab ntl; do
    for H in 60 75 90 105 120 135 150 165 180; do
        CFG="${RESULTS_DIR}/best_config_${V}_h${H}d.json"
        CSV="${RESULTS_DIR}/rocv_results_cmat_${V}.csv"
        
        # Skip if config doesn't exist
        if [ ! -f "$CFG" ]; then
            continue
        fi
        
        # Skip if already complete (11 folds for this horizon)
        if [ -f "$CSV" ]; then
            DONE=$(python3 -c "
import pandas as pd
df=pd.read_csv('$CSV')
print(len(df[df['horizon_days']==$H]))
" 2>/dev/null || echo 0)
            if [ "$DONE" -ge 11 ]; then
                log "  SKIP ${V}-h${H}d (already $DONE folds done)"
                continue
            fi
        fi
        
        ROCV_JOBS+=("${V}:${H}")
    done
done

TOTAL=${#ROCV_JOBS[@]}
log "  $TOTAL ROCV jobs to run"

# Submit with concurrency control
ACTIVE_JOBS=()
COMPLETED=0

submit_job() {
    local VARIANT=$1
    local HORIZON=$2
    local JOB_NAME="cmat-rocv-${VARIANT}-h${HORIZON}"
    
    local output=$(sbatch --job-name="$JOB_NAME" --time="$ROCV_WALLTIME" \
        --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
        "$SLURM_SCRIPT" rocv --variant "$VARIANT" --horizon "$HORIZON" \
        --resume 2>&1)
    
    local JOB_ID=$(echo "$output" | grep -o '[0-9]*$' || echo "FAILED")
    echo "$JOB_ID"
}

wait_for_slot() {
    # Wait until fewer than MAX_CONCURRENT of our ROCV jobs are running
    while true; do
        local running=0
        local new_active=()
        for jid in "${ACTIVE_JOBS[@]}"; do
            local state=$(sacct --format=State --noheader -j "$jid" 2>/dev/null | head -1 | tr -d ' ')
            case "$state" in
                RUNNING|PENDING)
                    running=$((running + 1))
                    new_active+=("$jid")
                    ;;
                COMPLETED)
                    COMPLETED=$((COMPLETED + 1))
                    log "  ✅ Job $jid COMPLETED ($COMPLETED done)"
                    ;;
                FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*)
                    log "  ⚠️  Job $jid $state"
                    ;;
            esac
        done
        ACTIVE_JOBS=("${new_active[@]+"${new_active[@]}"}")
        
        if [ "$running" -lt "$MAX_CONCURRENT" ]; then
            return
        fi
        sleep 60
    done
}

for IDX in "${!ROCV_JOBS[@]}"; do
    IFS=':' read -r VARIANT HORIZON <<< "${ROCV_JOBS[$IDX]}"
    JOB_NUM=$((IDX + 1))
    
    # Wait for a slot
    wait_for_slot
    
    JOB_ID=$(submit_job "$VARIANT" "$HORIZON")
    if [ "$JOB_ID" = "FAILED" ]; then
        log "  [$JOB_NUM/$TOTAL] ERROR submitting ${VARIANT}-h${HORIZON}d"
        continue
    fi
    
    ACTIVE_JOBS+=("$JOB_ID")
    log "  [$JOB_NUM/$TOTAL] SUBMITTED ${VARIANT}-h${HORIZON}d → $JOB_ID"
    sleep 5
done

# Wait for remaining jobs
log "=== All ROCV jobs submitted. Waiting for completion... ==="
while [ ${#ACTIVE_JOBS[@]} -gt 0 ]; do
    wait_for_slot  # This will drain remaining jobs
    if [ ${#ACTIVE_JOBS[@]} -eq 0 ]; then break; fi
    sleep 60
done

# ── Summary ──
log "=== ROCV Complete! ==="
for V in tab ntl; do
    CSV="${RESULTS_DIR}/rocv_results_cmat_${V}.csv"
    if [ -f "$CSV" ]; then
        python3 -c "
import pandas as pd
df = pd.read_csv('$CSV')
for h in sorted(df['horizon_days'].unique()):
    s = df[df['horizon_days']==h]
    print(f'  ${V} H={h:>3d}d: {len(s)} folds, MAPE={s[\"mape_pct\"].mean():.2f}%')
" 2>/dev/null
    fi
done
log "=== Done! ==="
