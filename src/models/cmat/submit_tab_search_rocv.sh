#!/bin/bash
# ============================================================
# CMAT-Tab Search (v4, bug-fixed) + ROCV Pipeline
# ============================================================
# v4: Feature-shifting bug fixed (concat-then-shift).
#     Composite PCC objective, min_epochs_before_es=10.
#     20 trials, 12h walltime per search job.
#
# Usage: nohup bash submit_tab_search_rocv.sh &> logs/pipeline_tab_v4.log &
# ============================================================
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat_h100.slurm"
LOG="${REPO_ROOT}/logs/pipeline_tab_v4.log"
mkdir -p "${REPO_ROOT}/logs"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG"; }

VARIANT="tab"
N_TRIALS=20

# ── 1. Submit Optuna Search Jobs ──
log "=== PHASE 1: Optuna Search (v4 bug-fixed, ${VARIANT}, $N_TRIALS trials) ==="
declare -a SEARCH_JOBS=()

for HORIZON in 60 120 180; do
    JOB_NAME="cmat-search-v4-${VARIANT}-h${HORIZON}"
    log "Submitting search for horizon $HORIZON ($N_TRIALS trials)..."

    OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="12:00:00" \
        --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
        "$SLURM_SCRIPT" search \
        --variant "$VARIANT" --horizon "$HORIZON" --n-trials "$N_TRIALS" 2>&1)

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
                log "Checking if best config was saved despite $STATE..."
                break ;;
            RUNNING|PENDING)
                sleep 60 ;;
            *)
                sleep 60 ;;
        esac
    done
done

# ── Verify all 3 best configs exist ──
ALL_OK=true
for HORIZON in 60 120 180; do
    CFG="${REPO_ROOT}/src/models/cmat/results/best_config_${VARIANT}_h${HORIZON}d.json"
    if [ -f "$CFG" ]; then
        log "✅ best_config_${VARIANT}_h${HORIZON}d.json exists."
        PCC=$(python3 -c "import json; c=json.load(open('$CFG')); print(f'PCC={c.get(\"test_pcc\",\"?\"):.4f}, MAPE={c.get(\"test_mape\",\"?\"):.2f}%')" 2>/dev/null || echo "?")
        log "   $PCC"
    else
        log "❌ MISSING best_config_${VARIANT}_h${HORIZON}d.json!"
        ALL_OK=false
    fi
done

if [ "$ALL_OK" = false ]; then
    log "Some configs missing. Aborting ROCV phase."
    exit 1
fi

log "=== PHASE 1 COMPLETE: All hyperparameter searches finished. ==="

# ── 3. Kick off ROCV (3 seeds: 42, 123, 456) ──
SEEDS=(42 123 456)

for SEED in "${SEEDS[@]}"; do
    log "=== PHASE 2: ROCV Evaluation (${VARIANT}, seed=${SEED}) ==="
    
    for HORIZON in 60 120 180; do
        CFG="${REPO_ROOT}/src/models/cmat/results/best_config_${VARIANT}_h${HORIZON}d.json"
        
        # Submit ROCV in batches of 4 folds
        for START_FOLD in 0 4 8; do
            MAX_FOLDS=4
            if [ "$START_FOLD" -eq 8 ]; then MAX_FOLDS=3; fi
            
            JOB_NAME="cmat-${VARIANT}-h${HORIZON}-s${SEED}-f${START_FOLD}"
            log "Submitting ROCV ${JOB_NAME} (folds ${START_FOLD}-$((START_FOLD+MAX_FOLDS-1)))..."
            
            OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="08:00:00" \
                --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
                "$SLURM_SCRIPT" rocv \
                --variant "$VARIANT" --horizon "$HORIZON" \
                --start-fold "$START_FOLD" --max-folds "$MAX_FOLDS" \
                --config "$CFG" --seed "$SEED" --resume 2>&1)
            
            JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
            log "SUBMITTED $JOB_NAME -> $JOB_ID"
            
            # Wait for this batch to complete before submitting next
            while true; do
                STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
                case "$STATE" in
                    COMPLETED) log "[✅] $JOB_NAME COMPLETED."; break ;;
                    FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*) log "[⚠️] $JOB_NAME: $STATE"; break ;;
                    *) sleep 60 ;;
                esac
            done
        done
    done
    
    log "=== Seed ${SEED} ROCV complete. ==="
done

log "=== PIPELINE COMPLETE: ${VARIANT} search + multi-seed ROCV done. ==="

