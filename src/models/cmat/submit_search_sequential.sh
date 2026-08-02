#!/bin/bash
# Sequential search submitter: ONE job at a time to avoid budget cancellation.
# Tab/NTL search trials take ~30s each, so 50 trials ≈ 25 min.
# Using 2h wall time for safety.
#
# Usage: nohup bash submit_search_sequential.sh &> logs/search_seq.log &
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat.slurm"
RESULTS_DIR="${REPO_ROOT}/src/models/cmat/results"
WALLTIME="02:00:00"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*"; }

# Build job list: Tab h60 is already running, skip it
declare -a JOBS=()
for H in 75 90 105 120 135 150 165 180; do
    JOBS+=("tab:${H}")
done
for H in 60 75 90 105 120 135 150 165 180; do
    JOBS+=("ntl:${H}")
done

TOTAL=${#JOBS[@]}
log "=== Sequential Search: $TOTAL jobs, wall=$WALLTIME ==="

# Wait for any currently running jobs to finish first
while true; do
    RUNNING=$(squeue -u "$USER" --noheader 2>/dev/null | wc -l)
    if [ "$RUNNING" -eq 0 ]; then break; fi
    log "Waiting for $RUNNING existing jobs to finish..."
    sleep 60
done

for IDX in "${!JOBS[@]}"; do
    IFS=':' read -r VARIANT HORIZON <<< "${JOBS[$IDX]}"
    JOB_NAME="cmat-search-${VARIANT}-h${HORIZON}"
    JOB_NUM=$((IDX + 1))

    # Check if config already exists (crash-safe: search saves after each trial)
    CONFIG="${RESULTS_DIR}/best_config_${VARIANT}_h${HORIZON}d.json"
    if [ -f "$CONFIG" ]; then
        # Check if it has enough completed trials
        N_TRIALS=$(python3 -c "
import json
with open('$CONFIG') as f:
    d = json.load(f)
print(d.get('best_trial_number', -1))
" 2>/dev/null || echo "-1")
        if [ "$N_TRIALS" -ge 30 ]; then
            log "[$JOB_NUM/$TOTAL] SKIP $JOB_NAME (best trial #$N_TRIALS, likely complete)"
            continue
        fi
    fi

    log "[$JOB_NUM/$TOTAL] Submitting $JOB_NAME..."

    output=$(sbatch --job-name="$JOB_NAME" --time="$WALLTIME" \
        --output="${REPO_ROOT}/logs/cmat_search_${VARIANT}_h${HORIZON}_%j.log" \
        "$SLURM_SCRIPT" search \
        --variant "$VARIANT" --horizon "$HORIZON" --n-trials 50 2>&1)

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
                log "[$JOB_NUM/$TOTAL] ✅ COMPLETED $JOB_NAME"
                break ;;
            FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*)
                log "[$JOB_NUM/$TOTAL] ⚠️  $STATE $JOB_NAME"
                break ;;
            RUNNING|PENDING)
                sleep 60 ;;
            *)
                sleep 30 ;;
        esac
    done
    sleep 5
done

log "=== All search jobs processed! ==="

# Summary
log "=== Configs found ==="
for V in tab ntl; do
    for H in 60 75 90 105 120 135 150 165 180; do
        F="${RESULTS_DIR}/best_config_${V}_h${H}d.json"
        if [ -f "$F" ]; then
            echo -n "  ✅ ${V}-h${H}d"
        else
            echo -n "  ❌ ${V}-h${H}d"
        fi
    done
    echo ""
done
