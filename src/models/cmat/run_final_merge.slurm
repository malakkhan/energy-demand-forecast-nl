#!/bin/bash
#SBATCH --job-name=cmat-full-merge
#SBATCH --partition=fat_rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=00:20:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/cmat_full_merge_%j.log

# Authoritative merge of all cmat_full shards (base + per-fold suffixed)
# into rocv_results_cmat_full.csv/.json. Runs after the whole fleet via
# --dependency=afterany, set at submit time.

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
umask 002

"${REPO_ROOT}/.venv/bin/python" -m src.models.baselines.merge_shards \
    --output-dir src/models/cmat/results \
    --model-name cmat_full

echo "Merge finished: $(date)"
# Clean up any stray atomic-write tmp files (never matched by the glob)
find src/models/cmat/results/_shards -name '*.tmp.*' -delete 2>/dev/null
# Group perms on everything new
find /gpfs/scratch1/shared/prjs2061_copy -user iyadav ! -perm -g=rwx -exec chmod g+rwx {} + 2>/dev/null
echo "Perm sweep finished: $(date)"
