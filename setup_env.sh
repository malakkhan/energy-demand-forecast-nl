#!/usr/bin/env bash
# =============================================================================
# setup_env.sh — create / update the project virtual environment on Snellius
#                and register it as a Jupyter kernel.
#
# Usage (from repo root, on a login node or interactive job):
#
#   # First time
#   bash setup_env.sh
#
#   # After changing requirements.in (re-pins and re-installs)
#   bash setup_env.sh --update
#
# On Snellius, Python modules require its toolchain year to be loaded first.
# Check what is available:  module spider Python
#
# Correct two-step load:
#   module load 2023
#   module load Python/3.11.3-GCCcore-12.3.0
#   bash setup_env.sh
#
# If you plan to run PySpark pipeline phases, also load Java before submitting
# those SLURM jobs (not required for the analysis notebook):
#   module load Java/11.0.20
# =============================================================================

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
VENV_DIR=".venv"
KERNEL_NAME="energy-demand-nl"
KERNEL_DISPLAY="Energy Demand NL (Python $(python --version 2>&1 | cut -d' ' -f2))"

# ── Guard: make sure a Python module is loaded ────────────────────────────────
PYTHON_BIN=$(command -v python || true)
if [[ -z "$PYTHON_BIN" ]]; then
    echo "ERROR: python not found. Load a module first, e.g.:"
    echo "  module load Python/3.11.3-GCCcore-12.3.0"
    echo "  module spider Python   # to see all available versions"
    exit 1
fi
echo "Using Python: $PYTHON_BIN  ($(python --version))"

# ── 1. Create venv (skip if it already exists and --update not passed) ─────────
if [[ ! -d "$VENV_DIR" ]]; then
    echo "Creating virtual environment at $VENV_DIR …"
    python -m venv "$VENV_DIR" --prompt "energy-demand"
else
    echo "Virtual environment already exists at $VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

# ── 2. Bootstrap pip + pip-tools ─────────────────────────────────────────────
echo "Upgrading pip and pip-tools …"
pip install --quiet --upgrade pip pip-tools

# ── 3. Pin exact versions: requirements.in → requirements.txt ────────────────
# Re-run only when --update is passed or requirements.in is newer than .txt
SHOULD_COMPILE=false
if [[ "${1:-}" == "--update" ]]; then
    SHOULD_COMPILE=true
elif [[ ! -f requirements.txt ]] || [[ requirements.in -nt requirements.txt ]]; then
    SHOULD_COMPILE=true
fi

if $SHOULD_COMPILE; then
    echo "Compiling requirements.in → requirements.txt …"
    pip-compile requirements.in \
        --output-file requirements.txt \
        --resolver=backtracking \
        --strip-extras \
        --quiet
    echo "  requirements.txt updated."
else
    echo "requirements.txt is up to date (pass --update to force recompile)."
fi

# ── 4. Install / sync pinned dependencies ────────────────────────────────────
echo "Installing dependencies from requirements.txt …"
pip-sync requirements.txt --quiet

# ── 5. Register Jupyter kernel ───────────────────────────────────────────────
echo "Registering Jupyter kernel '$KERNEL_NAME' …"
python -m ipykernel install \
    --user \
    --name  "$KERNEL_NAME" \
    --display-name "$KERNEL_DISPLAY"

# ── Done ─────────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  Environment ready                                   ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Activate  :  source .venv/bin/activate             ║"
echo "║  Deactivate:  deactivate                            ║"
echo "║  Kernel    :  $KERNEL_NAME"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "To verify the kernel is registered:"
echo "  jupyter kernelspec list"
echo ""
echo "To remove the kernel later:"
echo "  jupyter kernelspec remove $KERNEL_NAME"
