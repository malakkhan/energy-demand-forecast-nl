#!/bin/bash
# ============================================================
# CMAT-NTL Search (v4, bug-fixed) + Multi-Seed ROCV Pipeline
# ============================================================
# Runs in parallel with CMAT-Tab ROCV (no resource conflict).
# NTL variant does NOT need VIIRS images (uses scalar aggregate).
# ============================================================
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat_h100.slurm"
LOG="${REPO_ROOT}/logs/pipeline_ntl_v4.log"
mkdir -p "${REPO_ROOT}/logs"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG"; }

VARIANT="ntl"
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
            COMPLETED) log "Job $JOB_ID COMPLETED."; break ;;
            FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*) log "⚠️ Job $JOB_ID: $STATE"; break ;;
            RUNNING|PENDING) sleep 30 ;;
            *) sleep 30 ;;
        esac
    done
done

# ── Verify configs ──
ALL_OK=true
for HORIZON in 60 120 180; do
    CFG="${REPO_ROOT}/src/models/cmat/results/best_config_${VARIANT}_h${HORIZON}d.json"
    if [ -f "$CFG" ]; then
        PCC=$(.venv/bin/python3 -c "import json; c=json.load(open('$CFG')); print(f'PCC={c.get(\"test_pcc\",0):.4f}')" 2>/dev/null || echo "?")
        log "✅ h${HORIZON}: $PCC"
    else
        log "❌ MISSING best_config_${VARIANT}_h${HORIZON}d.json!"
        ALL_OK=false
    fi
done

if [ "$ALL_OK" = false ]; then
    log "Some configs missing. Aborting."
    exit 1
fi

log "=== PHASE 1 COMPLETE ==="

# ── 3. Multi-seed ROCV ──
SEEDS=(42 123 456)

for SEED in "${SEEDS[@]}"; do
    log "=== ROCV: ${VARIANT}, seed=${SEED} ==="

    for HORIZON in 60 120 180; do
        CFG="${REPO_ROOT}/src/models/cmat/results/best_config_${VARIANT}_h${HORIZON}d.json"

        for START_FOLD in 0 4 8; do
            MAX_FOLDS=4
            if [ "$START_FOLD" -eq 8 ]; then MAX_FOLDS=3; fi

            JOB_NAME="cmat-${VARIANT}-h${HORIZON}-s${SEED}-f${START_FOLD}"
            log "Submitting ${JOB_NAME}..."

            OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="08:00:00" \
                --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
                "$SLURM_SCRIPT" rocv \
                --variant "$VARIANT" --horizon "$HORIZON" \
                --start-fold "$START_FOLD" --max-folds "$MAX_FOLDS" \
                --config "$CFG" --seed "$SEED" --resume 2>&1)

            JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
            log "  -> $JOB_ID"

            while true; do
                STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
                case "$STATE" in
                    COMPLETED) log "  [✅] $JOB_NAME COMPLETED."; break ;;
                    FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*) log "  [⚠️] $JOB_NAME: $STATE"; break ;;
                    *) sleep 60 ;;
                esac
            done
        done
    done

    log "=== Seed ${SEED} complete. ==="
done

log "=== ALL DONE: ${VARIANT} search + 3-seed ROCV complete. ==="

# ── 4. Summary ──
CSV="${REPO_ROOT}/src/models/cmat/results/rocv_results_cmat_${VARIANT}.csv"
if [ -f "$CSV" ]; then
    log "Results CSV: $CSV"
    .venv/bin/python3 -c "
import pandas as pd
df = pd.read_csv('$CSV')
print(f'Total: {len(df)} rows')
for seed in sorted(df['seed'].unique()):
    sdf = df[df['seed']==seed]
    print(f'Seed {int(seed)}:')
    for h in sorted(sdf['horizon_days'].unique()):
        hdf = sdf[sdf['horizon_days']==h]
        print(f'  h={int(h):>3}d: {len(hdf)} folds, PCC={hdf[\"pcc\"].mean():.4f}+/-{hdf[\"pcc\"].std():.4f}, MAPE={hdf[\"mape_pct\"].mean():.2f}%')
" 2>/dev/null | while read line; do log "$line"; done
fi
