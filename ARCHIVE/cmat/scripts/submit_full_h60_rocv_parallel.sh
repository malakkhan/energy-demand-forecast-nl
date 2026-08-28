#!/bin/bash
# ============================================================
# CMAT-Full h60 ROCV — 3 seeds in PARALLEL, 1 fold per job
# ROBUST: checks CSV before submitting, prevents duplicates.
#
# Usage: nohup bash submit_full_h60_rocv_parallel.sh &>> logs/pipeline_full_h60_rocv_parallel.log &
# ============================================================
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat_h100.slurm"
VARIANT="full"
SEEDS=(42 123 456)
RESULTS_CSV="${REPO_ROOT}/src/models/cmat/results/rocv_results_cmat_full.csv"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*"; }

is_fold_done() {
    local SEED=$1 FOLD=$2
    if [ ! -f "$RESULTS_CSV" ]; then return 1; fi
    # Check if this exact seed+fold+horizon combo exists in CSV
    .venv/bin/python3 -c "
import pandas as pd, sys
df = pd.read_csv('$RESULTS_CSV')
match = df[(df['seed']==$SEED) & (df['fold']==$FOLD) & (df['horizon_days']==60)]
sys.exit(0 if len(match)>0 else 1)
" 2>/dev/null
}

is_job_running() {
    local SEED=$1 FOLD=$2
    local JOB_NAME="cmat-full-h60-s${SEED}-f${FOLD}"
    squeue -u $USER -o "%.40j" --noheader 2>/dev/null | grep -q "$JOB_NAME"
}

run_seed() {
    local SEED=$1
    local LOGFILE="${REPO_ROOT}/logs/pipeline_full_h60_rocv_s${SEED}.log"

    log "=== Seed ${SEED}: starting ===" >> "$LOGFILE"

    for FOLD in $(seq 0 10); do
        JOB_NAME="cmat-full-h60-s${SEED}-f${FOLD}"

        # Skip if already in CSV
        if is_fold_done "$SEED" "$FOLD"; then
            log "  $JOB_NAME: SKIP (already in CSV)" >> "$LOGFILE"
            continue
        fi

        # Skip if already running
        if is_job_running "$SEED" "$FOLD"; then
            JOB_ID=$(squeue -u $USER -o "%.10i %.40j" --noheader 2>/dev/null | grep "$JOB_NAME" | awk '{print $1}' | head -1)
            log "  $JOB_NAME: already running ($JOB_ID), waiting..." >> "$LOGFILE"
        else
            # Submit new job
            OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="08:00:00" \
                --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
                "$SLURM_SCRIPT" rocv \
                --variant "$VARIANT" --horizon 60 \
                --start-fold "$FOLD" --max-folds 1 \
                --seed "$SEED" --resume 2>&1)

            JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
            if [ "$JOB_ID" = "FAILED" ]; then
                log "  ERROR submitting $JOB_NAME: $OUTPUT" >> "$LOGFILE"
                continue
            fi
            log "  Submitted $JOB_NAME -> $JOB_ID" >> "$LOGFILE"
        fi

        # Wait for this fold to appear in CSV
        while true; do
            if is_fold_done "$SEED" "$FOLD"; then
                log "  [✅] $JOB_NAME done (in CSV)" >> "$LOGFILE"
                break
            fi
            # Check if job failed
            if [ -n "${JOB_ID:-}" ] && [ "$JOB_ID" != "FAILED" ]; then
                STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
                case "$STATE" in
                    FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*)
                        log "  [⚠️] $JOB_NAME: $STATE — retrying" >> "$LOGFILE"
                        # Resubmit
                        OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="08:00:00" \
                            --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
                            "$SLURM_SCRIPT" rocv \
                            --variant "$VARIANT" --horizon 60 \
                            --start-fold "$FOLD" --max-folds 1 \
                            --seed "$SEED" --resume 2>&1)
                        JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
                        log "  Resubmitted $JOB_NAME -> $JOB_ID" >> "$LOGFILE"
                        ;;
                esac
            fi
            sleep 120
        done
    done

    log "=== Seed ${SEED}: ALL DONE ===" >> "$LOGFILE"
}

log "Launching 3 parallel seed chainers..."

for SEED in "${SEEDS[@]}"; do
    run_seed "$SEED" &
    log "  Seed $SEED -> PID $!"
done

log "All 3 chainers launched."
wait
log "All seeds complete!"
