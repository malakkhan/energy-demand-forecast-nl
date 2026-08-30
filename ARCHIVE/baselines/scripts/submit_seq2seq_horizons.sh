#!/bin/bash
# ============================================================================
# Submit Seq2Seq ROCV as 9 parallel SLURM jobs (one per forecast horizon).
#
# Each job runs 11 ROCV folds for a single horizon. Results are safely merged
# into one CSV via file-locking (--resume flag).
#
# PREREQUISITES (run once on a login node):
#   1. Fix CUDA-enabled PyTorch:
#        module load 2023 Python/3.11.3-GCCcore-12.3.0 CUDA/12.1.1
#        source /projects/prjs2061/energy-demand-forecast-nl/.venv/bin/activate
#        pip install torch==2.8.0 --index-url https://download.pytorch.org/whl/cu121
#        python -c "import torch; print('CUDA build:', torch.version.cuda)"
#        # Should print: CUDA build: 12.1
#
# USAGE:
#   bash src/models/baselines/submit_seq2seq_horizons.sh          # GPU (default)
#   bash src/models/baselines/submit_seq2seq_horizons.sh --cpu     # CPU fallback
#
# MONITORING:
#   squeue -u $USER --name seq2seq    # Check running jobs
#   wc -l src/models/baselines/results/rocv_results_ashtar_seq2seq.csv
#                                      # Expected: 100 lines (1 header + 99 data)
# ============================================================================

set -euo pipefail

REPO_ROOT=/projects/prjs2061/energy-demand-forecast-nl
SLURM_SCRIPT="${REPO_ROOT}/src/models/baselines/run_seq2seq_v2.slurm"
RESULTS_CSV="${REPO_ROOT}/src/models/baselines/results/rocv_results_ashtar_seq2seq.csv"
HORIZONS=(60 75 90 105 120 135 150 165 180)

# ── Time limits: GPU ~3h/horizon (5h safe), CPU ~14h/horizon (18h safe) ──
TIME_LIMIT="05:00:00"
PARTITION="gpu_a100"

if [[ "${1:-}" == "--cpu" ]]; then
    TIME_LIMIT="18:00:00"
    PARTITION="thin"    # Adjust to your cluster's CPU partition name
    echo "Mode: CPU-only (${TIME_LIMIT} per job, partition=${PARTITION})"
    echo "  ⚠  CPU is ~30× slower — consider fixing CUDA PyTorch first."
    echo ""
else
    echo "Mode: GPU (${TIME_LIMIT} per job, partition=${PARTITION})"
fi

# ── Check existing progress ──────────────────────────────────────────────
if [[ -f "$RESULTS_CSV" ]]; then
    EXISTING=$(tail -n +2 "$RESULTS_CSV" | grep -c . || true)
    echo "Existing results: ${EXISTING}/99 folds"
    echo "  (completed folds will be skipped via --resume)"
    echo ""
fi

# ── Submit one job per horizon ───────────────────────────────────────────
echo "Submitting ${#HORIZONS[@]} Seq2Seq horizon jobs..."
echo ""

JOB_IDS=()
for H in "${HORIZONS[@]}"; do
    JOB_ID=$(sbatch \
        --job-name="seq2seq-h${H}" \
        --partition="${PARTITION}" \
        --time="${TIME_LIMIT}" \
        --output="${REPO_ROOT}/logs/seq2seq_v2_h${H}_%j.log" \
        "$SLURM_SCRIPT" --horizon "$H" --resume \
        2>&1 | grep -oP '\d+')
    JOB_IDS+=("$JOB_ID")
    printf "  Horizon %3dd → Job %s\n" "$H" "$JOB_ID"
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ${#JOB_IDS[@]} jobs submitted."
echo ""
echo "  Monitor:  squeue -u \$USER --name seq2seq"
echo "  Logs:     ls -lt ${REPO_ROOT}/logs/seq2seq_v2_h*"
echo "  Progress: wc -l ${RESULTS_CSV}"
echo "            (target: 100 lines = 1 header + 99 data rows)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
