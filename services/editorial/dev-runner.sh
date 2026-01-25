#!/bin/sh
set -e

# Trap exits to keep container alive briefly for log inspection
trap 'echo "💀 [Dev] Process exited with code $?. Sleeping 5s..."; sleep 5' EXIT

# 1. Install watchdog if missing
if ! command -v watchmedo > /dev/null 2>&1; then
    echo "🛠️  [Dev] Installing watchdog..."
    pip install watchdog[watchmedo] > /dev/null
fi

# 2. Extract arguments
INPUT_ARG=$1
COMPONENT_DIR=$2

# --- SMART CONVERSION: FILE PATH -> MODULE ---
# If user passes "app/workers/merger.py", convert to "app.workers.merger"
MODULE_NAME=$(echo "$INPUT_ARG" | sed 's/\.py$//')
MODULE_NAME=$(echo "$MODULE_NAME" | sed 's/\//./g')

echo "👀 [Dev] Watchdog active for Module: $MODULE_NAME"

# 3. Run with watchmedo
# FIX: Removed 'uv run'. The venv is already in PATH.
exec watchmedo auto-restart \
    --recursive \
    --patterns="*.py" \
    --directory="$COMPONENT_DIR" \
    --directory="app/core" \
    --directory="app/utils" \
    --directory="app/harvesters" \
    --directory="/app/common" \
    --signal SIGTERM \
    --kill-after 120.0 \
    --debounce-interval=60\
    -- \
    python -u -m "$MODULE_NAME"
