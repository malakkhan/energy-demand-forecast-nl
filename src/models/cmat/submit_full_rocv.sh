#!/bin/bash
# ============================================================
# CMAT-Full ROCV: 3 priority horizons (h60, h120, h180)
# Sequential submitter with budget monitoring.
#
# - h60: folds 5-10 (6 folds, folds 0-4 already done)
# - h120: folds 0-10 (11 folds)
# - h180: folds 0-10 (11 folds)
# Total: 28 folds
#
# Jobs are batched (max 4 folds per job) for fault tolerance.
#
# Usage: nohup bash submit_full_rocv.sh &>> logs/full_rocv_submit.log &
# ============================================================
set -euo pipefail

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat_h100.slurm"
RESULTS_DIR="${REPO_ROOT}/src/models/cmat/results"
LOG="${REPO_ROOT}/logs/full_rocv_submit.log"
mkdir -p "${REPO_ROOT}/logs"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG"; }

WALLTIME="08:00:00"

# ── Job definitions: variant:horizon:start_fold:max_folds ──
declare -a JOBS=(
    "full:60:0:4"    # h60 folds 0-3
    "full:60:4:4"    # h60 folds 4-7
    "full:60:8:3"    # h60 folds 8-10
    "full:120:0:4"   # h120 folds 0-3
    "full:120:4:4"   # h120 folds 4-7
    "full:120:8:3"   # h120 folds 8-10
    "full:180:0:4"   # h180 folds 0-3
    "full:180:4:4"   # h180 folds 4-7
    "full:180:8:3"   # h180 folds 8-10
)
TOTAL=${#JOBS[@]}

# SBU tracking (H100: 384 SBU/hr)
SBU_PER_HOUR=384
TOTAL_SBU=0

log "=== CMAT-Full Sequential ROCV: $TOTAL jobs, wall=$WALLTIME ==="

for IDX in "${!JOBS[@]}"; do
    IFS=':' read -r VARIANT HORIZON START_FOLD MAX_FOLDS <<< "${JOBS[$IDX]}"
    JOB_NAME="cmat-full-h${HORIZON}-f${START_FOLD}"
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

    # ── Submit ──
    OUTPUT=$(sbatch --job-name="$JOB_NAME" --time="$WALLTIME" \
        --output="${REPO_ROOT}/logs/${JOB_NAME}_%j.log" \
        "$SLURM_SCRIPT" --variant "$VARIANT" --horizon "$HORIZON" \
        --resume --start-fold "$START_FOLD" --max-folds "$MAX_FOLDS" 2>&1)

    JOB_ID=$(echo "$OUTPUT" | grep -o '[0-9]*$' || echo "FAILED")
    if [ "$JOB_ID" = "FAILED" ]; then
        log "[$JOB_NUM/$TOTAL] ERROR: $OUTPUT"
        sleep 30
        continue
    fi
    JOB_START=$(date +%s)
    log "[$JOB_NUM/$TOTAL] SUBMITTED $JOB_NAME → $JOB_ID (folds $START_FOLD-$((START_FOLD+MAX_FOLDS-1)), wall=$WALLTIME)"

    # ── Wait for completion ──
    sleep 30
    while true; do
        STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
        case "$STATE" in
            COMPLETED)
                JOB_END=$(date +%s)
                ELAPSED_H=$(echo "scale=2; ($JOB_END - $JOB_START) / 3600" | bc)
                JOB_SBU=$(echo "scale=0; $ELAPSED_H * $SBU_PER_HOUR" | bc)
                TOTAL_SBU=$(echo "scale=0; $TOTAL_SBU + $JOB_SBU" | bc)
                log "[$JOB_NUM/$TOTAL] ✅ COMPLETED $JOB_NAME (${ELAPSED_H}h, ~${JOB_SBU} SBU, cumulative ~${TOTAL_SBU} SBU)"
                break ;;
            FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*)
                JOB_END=$(date +%s)
                ELAPSED_H=$(echo "scale=2; ($JOB_END - $JOB_START) / 3600" | bc)
                JOB_SBU=$(echo "scale=0; $ELAPSED_H * $SBU_PER_HOUR" | bc)
                TOTAL_SBU=$(echo "scale=0; $TOTAL_SBU + $JOB_SBU" | bc)
                log "[$JOB_NUM/$TOTAL] ⚠️  $STATE $JOB_NAME (${ELAPSED_H}h, ~${JOB_SBU} SBU wasted, cumulative ~${TOTAL_SBU} SBU)"
                LOGFILE="${REPO_ROOT}/logs/${JOB_NAME}_${JOB_ID}.log"
                if [ -f "$LOGFILE" ]; then
                    log "  Last 10 lines of job log:"
                    tail -10 "$LOGFILE" | while read -r line; do log "    $line"; done
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

log "=== All jobs processed! Total estimated SBU spent: ~${TOTAL_SBU} ==="

# ── Summary ──
if [ -f "${RESULTS_DIR}/rocv_results_cmat_full.csv" ]; then
    python3 -c "
import pandas as pd
df = pd.read_csv('${RESULTS_DIR}/rocv_results_cmat_full.csv')
print('  CMAT-Full ROCV Summary:')
for h in [60, 120, 180]:
    s = df[df['horizon_days']==h]
    if len(s)>0:
        print(f'    H={h:>3d}d: {len(s):>2d}/11 folds, MAPE={s[\"mape_pct\"].mean():.2f}% ± {s[\"mape_pct\"].std():.2f}%, PCC={s[\"pcc\"].mean():.4f}')
    else:
        print(f'    H={h:>3d}d:  0/11 folds')
print(f'  Total train time: {df[\"train_time_s\"].sum()/3600:.1f}h')
" 2>/dev/null || true
fi
log "=== Done! ==="
