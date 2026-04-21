#!/bin/bash
#SBATCH --job-name=jupyter-lab
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --partition=thin
#SBATCH --output=logs/jupyter-lab-%j.out

# =============================================================================
# Starts a JupyterLab server on a compute node using the project venv.
# After submitting, read the output file for the SSH tunnel command.
#
# Submit:  sbatch src/pipeline/jupyter_lab.slurm
# Monitor: cat logs/jupyter-lab-<jobid>.out
# =============================================================================

# ── Load modules (must match what was used when building the venv) ────────────
module load 2023
module load Python/3.11.3-GCCcore-12.3.0

# ── Activate project virtual environment ─────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${REPO_ROOT}/.venv/bin/activate"

# ── Pick a random port (5000–5999) to avoid clashes with other users ──────────
PORT=$(shuf -i 5000-5999 -n 1)
LOGIN_HOST="${SLURM_SUBMIT_HOST}-pub.snellius.surf.nl"
BATCH_HOST=$(hostname)

echo "============================================================"
echo "  JupyterLab server starting on ${BATCH_HOST}:${PORT}"
echo "============================================================"
echo ""
echo "Step 1 — Run this command on YOUR LOCAL machine (Mac/Linux terminal):"
echo ""
echo "  ssh -N -J ${USER}@${LOGIN_HOST} ${USER}@${BATCH_HOST} -L ${PORT}:localhost:${PORT}"
echo ""
echo "Step 2 — Open this URL in your local browser:"
echo ""
echo "  http://localhost:${PORT}"
echo ""
echo "  (The token will appear in the JupyterLab startup output below)"
echo "============================================================"
echo ""

# ── Start JupyterLab ─────────────────────────────────────────────────────────
jupyter lab \
    --no-browser \
    --port="${PORT}" \
    --notebook-dir="${REPO_ROOT}"
