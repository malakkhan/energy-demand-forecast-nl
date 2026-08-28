#!/bin/bash
#SBATCH --job-name=perm-fix
#SBATCH --partition=fat_rome
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=01:00:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/perm_fix_%j.log

# Grants group prjs2061 (mmalak, jbarbara) rwx on everything iyadav owns in
# prjs2061_copy. Submitted with a dependency so it sweeps up all files the
# CMAT-Full ROCV chain creates.
find /gpfs/scratch1/shared/prjs2061_copy -user iyadav ! -group prjs2061 -exec chgrp prjs2061 {} + 2>/dev/null
find /gpfs/scratch1/shared/prjs2061_copy -user iyadav ! -perm -g=rwx -exec chmod g+rwx {} + 2>/dev/null
echo "perm fix complete: $(date)"
echo "remaining violations: $(find /gpfs/scratch1/shared/prjs2061_copy -user iyadav ! -perm -g=rwx 2>/dev/null | wc -l)"
