#!/bin/bash
#SBATCH --job-name=shard-selftest
#SBATCH --partition=fat_rome
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=00:15:00
#SBATCH --output=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl/logs/shard_selftest_%j.log

# Unit-test the CMAT_SHARD_SUFFIX save/skip path on a compute node
# (train.py imports torch, so this must never run on the login node).
# Writes ONLY to an isolated tmp dir — never touches results/_shards.

REPO_ROOT=/scratch-shared/prjs2061_copy/energy-demand-forecast-nl
cd "$REPO_ROOT"
umask 002

TESTDIR=$(mktemp -d /scratch-shared/prjs2061_copy/tmp_shard_selftest.XXXX)
export CMAT_SHARD_SUFFIX="_f99"

"${REPO_ROOT}/.venv/bin/python" - "$TESTDIR" <<'EOF'
import os, sys
from pathlib import Path
sys.path.insert(0, ".")
from src.models.cmat.train import save_fold_result_locked, is_fold_done

tmp = Path(sys.argv[1])
row = {"model": "cmat_selftest", "fold": 99, "horizon_days": 60, "seed": 42,
       "rmse": 1.0, "train_time_s": 1.0, "n_epochs": 1}

# 1. suffixed save writes the suffixed file, atomically, no leftovers
save_fold_result_locked(dict(row), [], tmp, "cmat_selftest")
p = tmp / "_shards" / "cmat_selftest_h60_s42_f99.csv"
assert p.exists(), f"suffixed shard missing: {p}"
leftovers = list((tmp / "_shards").glob("*.tmp.*"))
assert not leftovers, f"tmp leftovers: {leftovers}"

# 2. is_fold_done finds the fold via the suffixed shard
assert is_fold_done(tmp, "cmat_selftest", 99, 60, seed=42) is True
# 3. ...but not for a different fold/seed
assert is_fold_done(tmp, "cmat_selftest", 98, 60, seed=42) is False
assert is_fold_done(tmp, "cmat_selftest", 99, 60, seed=7) is False

# 4. suffix-less save still writes the classic base shard path
os.environ.pop("CMAT_SHARD_SUFFIX")
save_fold_result_locked(dict(row), [], tmp, "cmat_selftest2")
assert (tmp / "_shards" / "cmat_selftest2_h60_s42.csv").exists()

# 5. merge glob picks up the suffixed file
import glob as g
hits = g.glob(str(tmp / "_shards" / "cmat_selftest_h*_s*.csv"))
assert len(hits) == 1, hits

print("SELFTEST PASS")
EOF
STATUS=$?
rm -rf "$TESTDIR"
echo "exit=$STATUS"
