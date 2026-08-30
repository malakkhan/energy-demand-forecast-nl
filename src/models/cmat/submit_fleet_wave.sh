#!/bin/bash
# =============================================================================
# Submit one wave of per-fold CMAT-Full ROCV jobs (triage scope: h60 + h180,
# seeds 42/123/7) on gpu_a100 with 4 dataloader workers / prefetch 2.
#
# Usage: submit_fleet_wave.sh <max_fold> [min_fold=1]
#   wave 1: submit_fleet_wave.sh 5      (folds 1-5, ~25 jobs)
#   wave 2: submit_fleet_wave.sh 9 6    (folds 6-9, ~24 jobs)
#
# Safety: the manifest is derived from the shard files at run time (a fold
# with a saved row is never submitted), and any (H,SEED,FOLD) with a job
# already queued/running under its unique name cf-h{H}s{S}f{F} is skipped —
# so no two jobs can ever share a shard file.
# =============================================================================
set -euo pipefail
REPO=/gpfs/scratch1/shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO"
MAX_FOLD=${1:?usage: submit_fleet_wave.sh <max_fold> [min_fold]}
MIN_FOLD=${2:-1}

MANIFEST=$REPO/logs/fleet_wave_f${MIN_FOLD}-f${MAX_FOLD}.manifest
IDS=$REPO/logs/fleet_job_ids.txt

python3 - "$MIN_FOLD" "$MAX_FOLD" <<'EOF' > "$MANIFEST"
import csv, glob, os, sys
done = set()
for f in glob.glob("src/models/cmat/results/_shards/cmat_full_h*_s*.csv"):
    with open(f) as fh:
        for r in csv.DictReader(fh):
            done.add((int(r["horizon_days"]), int(r["seed"]), int(float(r["fold"]))))
lo, hi = int(sys.argv[1]), int(sys.argv[2])
# Horizons default to the main study scope; override via env for the
# deferred-h120 completion: FLEET_HORIZONS=120 bash submit_fleet_wave.sh 9 1
horizons = tuple(int(x) for x in os.environ.get("FLEET_HORIZONS", "60 180").split())
rows = []
for H in horizons:
    for S in (42, 123, 7):
        for F in range(1, 10):
            if lo <= F <= hi and (H, S, F) not in done:
                rows.append((H, S, F))
assert len(rows) == len(set(rows))
for H, S, F in rows:
    wall = "07:00:00" if F <= 3 else ("10:00:00" if F <= 6 else "14:00:00")
    print(H, S, F, wall)
EOF

echo "=== manifest ($(wc -l < "$MANIFEST") jobs) ==="
cat "$MANIFEST"
echo "=== submitting ==="
while read -r H S F T; do
    NAME="cf-h${H}s${S}f${F}"
    if squeue -u iyadav -h -n "$NAME" -o %i | grep -q .; then
        echo "SKIP $NAME (already queued/running)"
        continue
    fi
    # CMAT_NTL_UNSHIFTED=1 pins the legacy image-date convention so every
    # fold of this campaign is produced under the SAME convention as the
    # banked h60/h180 results (train.py now defaults to horizon-shifted
    # images; see the report's limitations discussion).
    JID=$(sbatch --parsable -p gpu_a100 -t "$T" --job-name="$NAME" \
        --export=ALL,H=$H,SEED=$S,FOLD=$F,WORKERS=4,PREFETCH=2,CMAT_NTL_UNSHIFTED=1 \
        src/models/cmat/run_cmat_full_fold.slurm)
    echo "$JID $NAME $T" | tee -a "$IDS"
done < "$MANIFEST"
echo "=== done; all fleet ids in $IDS ==="
