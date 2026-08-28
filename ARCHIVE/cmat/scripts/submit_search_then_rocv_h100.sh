#!/bin/bash
# ============================================================
# CMAT-Full Search (v3) + ROCV End-to-End Pipeline
# ============================================================
# v3 improvements over v2:
#   - Composite objective: val_loss * (2 - PCC) to penalize flat-mean
#   - min_epochs_before_es=10: prevents early stopping before model escapes
#   - max_epochs=50, patience=15: generous training budget
#   - 25 trials per horizon (more exploration needed)
#
# Usage: nohup bash submit_search_then_rocv_h100.sh &> logs/pipeline_v3.log &
# ============================================================
set -euo pipefail

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat_h100.slurm"
LOG="${REPO_ROOT}/logs/pipeline_v3.log"
mkdir -p "${REPO_ROOT}/logs"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG"; }

N_TRIALS=25

# ── 1. Submit Optuna Search Jobs ──
log "=== PHASE 1: Optuna Search (v3 composite objective, $N_TRIALS trials) ==="
declare -a SEARCH_JOBS=()

for HORIZON in 60 120 180; do
    JOB_NAME="cmat-search-v3-full-h${HORIZON}"
    log "Submitting search for horizon $HORIZON ($N_TRIALS trials)..."

    OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="12:00:00" \
        --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
        "$SLURM_SCRIPT" search \
        --variant full --horizon "$HORIZON" --n-trials "$N_TRIALS" 2>&1)

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
    CFG="${REPO_ROOT}/src/models/cmat/results/best_config_full_h${HORIZON}d.json"
    if [ -f "$CFG" ]; then
        log "✅ best_config_full_h${HORIZON}d.json exists."
        # Show the PCC from the best config
        PCC=$(python3 -c "import json; c=json.load(open('$CFG')); print(f'PCC={c.get(\"test_pcc\",\"?\"):.4f}, MAPE={c.get(\"test_mape\",\"?\"):.2f}%')" 2>/dev/null || echo "?")
        log "   $PCC"
    else
        log "❌ MISSING best_config_full_h${HORIZON}d.json!"
        ALL_OK=false
    fi
done

if [ "$ALL_OK" = false ]; then
    log "Some configs missing. Aborting ROCV phase."
    exit 1
fi

log "=== PHASE 1 COMPLETE: All hyperparameter searches finished. ==="

# ── 3. Kick off ROCV Pipeline ──
log "=== PHASE 2: ROCV Evaluation ==="
log "Launching submit_full_rocv.sh..."

cd "$REPO_ROOT"
bash src/models/cmat/submit_full_rocv.sh

log "=== END-TO-END PIPELINE v3 COMPLETE ==="
