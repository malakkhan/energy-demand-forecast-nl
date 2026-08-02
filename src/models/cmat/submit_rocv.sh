#!/bin/bash
# Sequential Full ROCV: 3 horizons (60, 120, 180) × folds 4,5,6
# NOW with shorter wall times to avoid SLURM budget cancellation
# Optimizations: AMP + NTL preload → ~2x speedup → ~11h per horizon
# Wall time: 14h (enough for 3 folds + preload, with margin)
#
# Usage: nohup bash submit_rocv.sh &> logs/sequential_rocv_v3.log &
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat.slurm"
RESULTS_DIR="${REPO_ROOT}/src/models/cmat/results"
LOG="${REPO_ROOT}/logs/sequential_rocv_v3.log"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG"; }

# Wall time: 14h per job (optimized code needs ~11h for 3 folds)
WALLTIME="14:00:00"

# Jobs: variant:horizon
declare -a JOBS=(
    "full:60"
    "full:120"
    "full:180"
)

START_FOLD=4
MAX_FOLDS=3
TOTAL=${#JOBS[@]}

log "=== Sequential ROCV v3: $TOTAL jobs, folds $START_FOLD-$((START_FOLD + MAX_FOLDS - 1)), wall=$WALLTIME ==="

for IDX in "${!JOBS[@]}"; do
    IFS=':' read -r VARIANT HORIZON <<< "${JOBS[$IDX]}"
    JOB_NAME="cmat-rocv-${VARIANT}-h${HORIZON}"
    JOB_NUM=$((IDX + 1))

    # ── Check which target folds are already complete ──
    CSV="${RESULTS_DIR}/rocv_results_cmat_${VARIANT}.csv"
    if [ -f "$CSV" ]; then
        DONE=$(python3 -c "
import pandas as pd
df=pd.read_csv('$CSV')
target_folds = list(range($START_FOLD, $START_FOLD + $MAX_FOLDS))
done = len(df[(df['horizon_days']==$HORIZON) & (df['fold'].isin(target_folds))])
print(done)
" 2>/dev/null || echo 0)
        if [ "$DONE" -ge "$MAX_FOLDS" ]; then
            log "[$JOB_NUM/$TOTAL] SKIP $JOB_NAME (all $MAX_FOLDS target folds done)"
            continue
        fi
        log "[$JOB_NUM/$TOTAL] $JOB_NAME: $DONE/$MAX_FOLDS target folds done"
    else
        log "[$JOB_NUM/$TOTAL] $JOB_NAME: no results file yet"
    fi

    # ── Wait for queue to be empty ──
    while true; do
        RUNNING=$(squeue -u "$USER" --noheader 2>/dev/null | wc -l)
        if [ "$RUNNING" -eq 0 ]; then
            break
        fi
        log "[$JOB_NUM/$TOTAL] Waiting... ($RUNNING jobs running)"
        sleep 120
    done

    # ── Submit with shorter wall time ──
    OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="$WALLTIME" \
        --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
        "$SLURM_SCRIPT" rocv --variant "$VARIANT" --horizon "$HORIZON" \
        --resume --start-fold "$START_FOLD" --max-folds "$MAX_FOLDS" 2>&1)

    JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
    if [ "$JOB_ID" = "FAILED" ]; then
        log "[$JOB_NUM/$TOTAL] ERROR: $OUTPUT"
        sleep 30
        continue
    fi
    log "[$JOB_NUM/$TOTAL] SUBMITTED $JOB_NAME → $JOB_ID (folds $START_FOLD-$((START_FOLD+MAX_FOLDS-1)), wall=$WALLTIME)"

    # ── Wait for completion ──
    sleep 30
    while true; do
        STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
        case "$STATE" in
            COMPLETED)
                log "[$JOB_NUM/$TOTAL] ✅ COMPLETED $JOB_NAME"
                break ;;
            FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*)
                log "[$JOB_NUM/$TOTAL] ⚠️  $STATE $JOB_NAME — checking log..."
                # Print last few lines of job log for debugging
                LOGFILE="${REPO_ROOT}/logs/${JOB_NAME}_${JOB_ID}.log"
                if [ -f "$LOGFILE" ]; then
                    log "  Last 5 lines of job log:"
                    tail -5 "$LOGFILE" | while read -r line; do log "    $line"; done
                fi
                break ;;
            RUNNING|PENDING)
                sleep 120 ;;
            *)
                sleep 60 ;;
        esac
    done
    sleep 10
done

log "=== All jobs processed! ==="
# Summary
if [ -f "${RESULTS_DIR}/rocv_results_cmat_full.csv" ]; then
    python3 -c "
import pandas as pd
df = pd.read_csv('${RESULTS_DIR}/rocv_results_cmat_full.csv')
for h in [60, 120, 180]:
    s = df[df['horizon_days']==h]
    if len(s)>0:
        print(f'  H={h:>3d}d: {len(s)} folds, MAPE={s[\"mape_pct\"].mean():.2f}% ± {s[\"mape_pct\"].std():.2f}%')
" 2>/dev/null || true
fi
