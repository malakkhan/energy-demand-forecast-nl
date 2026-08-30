#!/bin/bash
# Submit remaining h180 CMAT-Full folds in batches of 5
# to avoid exceeding scratch-node disk quota (85GB VIIRS × 5 ≈ 425GB)
#
# Batch 1: s42 f0-f4 (ALREADY RUNNING)
# Batch 2: s42 f5-f9
# Batch 3: s42 f10, s123 f0-f3
# Batch 4: s123 f4-f8
# Batch 5: s123 f9-f10

set -euo pipefail
REPO=/projects/prjs2061/energy-demand-forecast-nl
cd "$REPO"

submit_fold() {
    local SEED=$1 FOLD=$2 DEP_JOBID=$3
    local DEP_FLAG=""
    if [ -n "$DEP_JOBID" ]; then
        DEP_FLAG="--dependency=afterany:${DEP_JOBID}"
    fi
    sbatch --job-name="cmat-full-h180-s${SEED}-f${FOLD}" --time="08:00:00" \
        --partition=gpu_h100 --gpus=1 --cpus-per-task=16 --mem=120G \
        --constraint=scratch-node \
        --mail-type=END,FAIL --mail-user=malak.khan@student.uva.nl \
        --output="logs/cmat-full-h180-s${SEED}-f${FOLD}_%j.log" \
        $DEP_FLAG \
        src/models/cmat/run_cmat_h100.slurm rocv \
        --variant full --horizon 180 --start-fold "$FOLD" --max-folds 1 --seed "$SEED" \
        2>&1 | awk '{print $4}'
}

# Get any running h180 job to use as dependency anchor for batch 2
ANCHOR=$(squeue -u $USER -h -o "%i %j" | grep "h180-s42-f0" | awk '{print $1}' | head -1)
if [ -z "$ANCHOR" ]; then
    echo "No running h180 jobs found. Submitting batch 2 without dependency."
    ANCHOR=""
fi
echo "Batch 1 anchor: $ANCHOR"

# Batch 2: s42 f5-f9 (depend on batch 1 anchor)
echo "=== Batch 2: s42 f5-f9 ==="
BATCH2_FIRST=""
for FOLD in 5 6 7 8 9; do
    JID=$(submit_fold 42 $FOLD "$ANCHOR")
    echo "  s42-f${FOLD} -> $JID"
    [ -z "$BATCH2_FIRST" ] && BATCH2_FIRST=$JID
done

# Batch 3: s42 f10, s123 f0-f3 (depend on batch 2)
echo "=== Batch 3: s42-f10 + s123 f0-f3 ==="
BATCH3_FIRST=""
for SEED_FOLD in "42 10" "123 0" "123 1" "123 2" "123 3"; do
    SEED=$(echo $SEED_FOLD | awk '{print $1}')
    FOLD=$(echo $SEED_FOLD | awk '{print $2}')
    JID=$(submit_fold $SEED $FOLD "$BATCH2_FIRST")
    echo "  s${SEED}-f${FOLD} -> $JID"
    [ -z "$BATCH3_FIRST" ] && BATCH3_FIRST=$JID
done

# Batch 4: s123 f4-f8 (depend on batch 3)
echo "=== Batch 4: s123 f4-f8 ==="
BATCH4_FIRST=""
for FOLD in 4 5 6 7 8; do
    JID=$(submit_fold 123 $FOLD "$BATCH3_FIRST")
    echo "  s123-f${FOLD} -> $JID"
    [ -z "$BATCH4_FIRST" ] && BATCH4_FIRST=$JID
done

# Batch 5: s123 f9-f10 (depend on batch 4)
echo "=== Batch 5: s123 f9-f10 ==="
for FOLD in 9 10; do
    JID=$(submit_fold 123 $FOLD "$BATCH4_FIRST")
    echo "  s123-f${FOLD} -> $JID"
done

echo ""
echo "All batches submitted! Total: 17 new jobs in 4 dependency-chained batches."
squeue -u $USER -h | wc -l
echo "total jobs queued"
