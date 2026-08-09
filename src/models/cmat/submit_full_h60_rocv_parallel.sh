#!/bin/bash
# ============================================================
# CMAT-Full h60 ROCV — 3 seeds in PARALLEL, 1 fold per job
# Each seed gets its own chainer. Uses --resume to skip done folds.
#
# Usage: bash submit_full_h60_rocv_parallel.sh
# ============================================================
set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/cmat/run_cmat_h100.slurm"
VARIANT="full"
SEEDS=(42 123 456)

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*"; }

run_seed() {
    local SEED=$1
    local LOGFILE="${REPO_ROOT}/logs/pipeline_full_h60_rocv_s${SEED}.log"

    log "=== Seed ${SEED}: starting ===" >> "$LOGFILE"

    for FOLD in $(seq 0 10); do
        JOB_NAME="cmat-full-h60-s${SEED}-f${FOLD}"

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

        # Wait for completion
        while true; do
            STATE=$(sacct --format=State --noheader -j "$JOB_ID" 2>/dev/null | head -1 | tr -d ' ')
            case "$STATE" in
                COMPLETED) log "  [✅] $JOB_NAME COMPLETED." >> "$LOGFILE"; break ;;
                FAILED|CANCELLED*|TIMEOUT|OUT_OF_ME*) log "  [⚠️] $JOB_NAME: $STATE" >> "$LOGFILE"; break ;;
                RUNNING|PENDING) sleep 120 ;;
                *) sleep 120 ;;
            esac
        done
    done

    log "=== Seed ${SEED}: ALL DONE ===" >> "$LOGFILE"
}

echo "Launching 3 parallel seed chainers..."

for SEED in "${SEEDS[@]}"; do
    run_seed "$SEED" &
    echo "  Seed $SEED -> PID $!"
done

echo "All 3 chainers launched. Monitor with:"
echo "  tail -f logs/pipeline_full_h60_rocv_s*.log"
echo "  squeue -u \$USER"

wait
echo "All seeds complete!"
