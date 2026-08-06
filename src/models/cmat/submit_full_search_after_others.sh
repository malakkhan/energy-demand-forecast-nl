#!/bin/bash
# ============================================================
# CMAT-Full Search (v4, bug-fixed)
# ============================================================
# Waits for BOTH Tab-ROCV and NTL pipelines to finish, then
# launches CMAT-Full Optuna search for h60, h120, h180.
#
# Full variant uses spatial branch → needs VIIRS staging.
# Search only (no ROCV) — user will review results first.
# ============================================================
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat_h100.slurm"
LOG="${REPO_ROOT}/logs/pipeline_full_v4_search.log"
mkdir -p "${REPO_ROOT}/logs"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG"; }

VARIANT="full"
N_TRIALS=20

# ── 0. Wait for Tab and NTL pipelines to finish ──
log "=== Waiting for Tab and NTL pipelines to complete ==="

# Wait for Tab pipeline (check for process or "ALL DONE" in log)
while true; do
    TAB_DONE=$(grep -c "ALL DONE" "${REPO_ROOT}/logs/pipeline_tab_v4_rerun.log" 2>/dev/null || echo 0)
    if [ "$TAB_DONE" -gt 0 ]; then
        log "✅ Tab pipeline completed."
        break
    fi
    # Also check if no tab jobs are running
    TAB_JOBS=$(squeue -u "$USER" --name="cmat-tab%" -h 2>/dev/null | wc -l)
    if [ "$TAB_JOBS" -eq 0 ] && [ -f "${REPO_ROOT}/src/models/cmat/results/rocv_results_cmat_tab.csv" ]; then
        log "✅ Tab pipeline completed (no jobs running, CSV exists)."
        break
    fi
    log "  Tab: waiting ($TAB_JOBS jobs running)..."
    sleep 120
done

# Wait for NTL pipeline
while true; do
    NTL_DONE=$(grep -c "ALL DONE" "${REPO_ROOT}/logs/pipeline_ntl_v4.log" 2>/dev/null || echo 0)
    if [ "$NTL_DONE" -gt 0 ]; then
        log "✅ NTL pipeline completed."
        break
    fi
    NTL_JOBS=$(squeue -u "$USER" --name="cmat-ntl%,cmat-sea%" -h 2>/dev/null | wc -l)
    if [ "$NTL_JOBS" -eq 0 ] && [ -f "${REPO_ROOT}/src/models/cmat/results/rocv_results_cmat_ntl.csv" ]; then
        log "✅ NTL pipeline completed (no jobs running, CSV exists)."
        break
    fi
    log "  NTL: waiting ($NTL_JOBS jobs running)..."
    sleep 120
done

log "=== Both Tab and NTL pipelines complete. Starting Full search. ==="

# ── 1. Submit Full Search Jobs ──
log "=== PHASE 1: Optuna Search (v4, ${VARIANT}, $N_TRIALS trials, SEQUENTIAL) ==="

for HORIZON in 60 120 180; do
    JOB_NAME="cmat-search-v4-${VARIANT}-h${HORIZON}"
    log "Submitting search for horizon $HORIZON ($N_TRIALS trials)..."

    OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="24:00:00" \
        --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
        "$SLURM_SCRIPT" search \
        --variant "$VARIANT" --horizon "$HORIZON" --n-trials "$N_TRIALS" 2>&1)

    JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
    if [ "$JOB_ID" = "FAILED" ]; then
        log "ERROR submitting $JOB_NAME: $OUTPUT"
        exit 1
    fi
    log "SUBMITTED $JOB_NAME -> $JOB_ID"

    # Wait for THIS search to complete before starting next
    # (sequential to avoid 3×24h H100 scheduling issues)
    while true; do
        STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
        case "$STATE" in
            COMPLETED) log "Job $JOB_ID COMPLETED."; break ;;
            FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*) log "⚠️ Job $JOB_ID: $STATE"; break ;;
            RUNNING|PENDING) sleep 120 ;;
            *) sleep 120 ;;
        esac
    done
done

# ── 3. Verify configs and report ──
log "=== Search Complete: Results ==="
for HORIZON in 60 120 180; do
    CFG="${REPO_ROOT}/src/models/cmat/results/best_config_${VARIANT}_h${HORIZON}d.json"
    if [ -f "$CFG" ]; then
        PCC=$(.venv/bin/python3 -c "import json; c=json.load(open('$CFG')); print(f'PCC={c.get(\"test_pcc\",0):.4f}, MAPE={c.get(\"test_mape\",0):.2f}%')" 2>/dev/null || echo "?")
        log "✅ h${HORIZON}: $PCC"
    else
        log "❌ MISSING best_config_${VARIANT}_h${HORIZON}d.json!"
    fi
done

log "=== FULL SEARCH COMPLETE. Review results and launch ROCV when ready. ==="
