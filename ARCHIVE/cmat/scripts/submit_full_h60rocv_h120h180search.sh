#!/bin/bash
# ============================================================
# CMAT-Full: h60 ROCV + h120/h180 Search
# All from /projects/prjs2061/
#
# Phase 1: h60 ROCV (3 seeds × 11 folds) using new v4 best config
# Phase 2: h120 Optuna search (20 trials, 24h walltime)
# Phase 3: h180 Optuna search (20 trials, 24h walltime)
#
# Usage: nohup bash submit_full_h60rocv_h120h180search.sh &>> logs/pipeline_full_v4_h60rocv_search.log &
# ============================================================
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat_h100.slurm"
RESULTS_CSV="${REPO_ROOT}/src/models/cmat/results/rocv_results_cmat_full.csv"
VARIANT="full"
N_TRIALS=20
SEEDS=(42 123 456)

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*"; }

# ── PHASE 1: h60 ROCV (3 seeds × 11 folds) ──
log "=== PHASE 1: h60 ROCV (3 seeds, 11 folds each) ==="

for SEED in "${SEEDS[@]}"; do
    log "--- Seed ${SEED} ---"
    for FOLD_START in 0 4 8; do
        # Determine fold range
        FOLD_END=$((FOLD_START + 3))
        if [ $FOLD_END -gt 10 ]; then FOLD_END=10; fi
        
        JOB_NAME="cmat-full-h60-s${SEED}-f${FOLD_START}"
        
        OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="08:00:00" \
            --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
            "$SLURM_SCRIPT" rocv \
            --variant "$VARIANT" --horizon 60 \
            --fold-start "$FOLD_START" --fold-end "$FOLD_END" \
            --seed "$SEED" 2>&1)
        
        JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
        if [ "$JOB_ID" = "FAILED" ]; then
            log "ERROR submitting $JOB_NAME: $OUTPUT"
            exit 1
        fi
        log "  Submitted $JOB_NAME -> $JOB_ID (folds ${FOLD_START}-${FOLD_END})"
        
        # Wait for this batch to complete before next
        while true; do
            STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
            case "$STATE" in
                COMPLETED) log "  [✅] $JOB_NAME COMPLETED."; break ;;
                FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*) log "  [⚠️] $JOB_NAME: $STATE"; break ;;
                RUNNING|PENDING) sleep 120 ;;
                *) sleep 120 ;;
            esac
        done
    done
    log "=== Seed ${SEED} complete. ==="
done

log "=== PHASE 1 DONE: h60 ROCV complete ==="
if [ -f "$RESULTS_CSV" ]; then
    ROWS=$(wc -l < "$RESULTS_CSV")
    log "Results CSV: $RESULTS_CSV ($ROWS rows including header)"
fi

# ── PHASE 2: h120 Search ──
log "=== PHASE 2: h120 Optuna Search ($N_TRIALS trials, 24h walltime) ==="

JOB_NAME="cmat-search-v4-full-h120"
OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="24:00:00" \
    --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
    "$SLURM_SCRIPT" search \
    --variant "$VARIANT" --horizon 120 --n-trials "$N_TRIALS" 2>&1)

JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
if [ "$JOB_ID" = "FAILED" ]; then
    log "ERROR submitting $JOB_NAME: $OUTPUT"
    exit 1
fi
log "SUBMITTED $JOB_NAME -> $JOB_ID"

while true; do
    STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
    case "$STATE" in
        COMPLETED) log "Job $JOB_ID COMPLETED."; break ;;
        FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*) log "⚠️ Job $JOB_ID: $STATE"; break ;;
        RUNNING|PENDING) sleep 120 ;;
        *) sleep 120 ;;
    esac
done

# ── PHASE 3: h180 Search ──
log "=== PHASE 3: h180 Optuna Search ($N_TRIALS trials, 24h walltime) ==="

JOB_NAME="cmat-search-v4-full-h180"
OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="24:00:00" \
    --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
    "$SLURM_SCRIPT" search \
    --variant "$VARIANT" --horizon 180 --n-trials "$N_TRIALS" 2>&1)

JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
if [ "$JOB_ID" = "FAILED" ]; then
    log "ERROR submitting $JOB_NAME: $OUTPUT"
    exit 1
fi
log "SUBMITTED $JOB_NAME -> $JOB_ID"

while true; do
    STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
    case "$STATE" in
        COMPLETED) log "Job $JOB_ID COMPLETED."; break ;;
        FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*) log "⚠️ Job $JOB_ID: $STATE"; break ;;
        RUNNING|PENDING) sleep 120 ;;
        *) sleep 120 ;;
    esac
done

log "=== ALL PHASES COMPLETE ==="
log "h60 ROCV: done"
log "h120 search: done (check best_config_full_h120d.json)"
log "h180 search: done (check best_config_full_h180d.json)"
log "NEXT: Run h120/h180 ROCV with new best configs"
