#!/bin/bash
# Backward-compatible wrapper for one-shot MongoDB workload mode.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

WORKLOAD="${1:-a}"
RUNTIME_SECONDS="${2:-0}"

if [[ ! "$WORKLOAD" =~ ^[a-f]$ ]]; then
    echo "ERROR: workload must be one of: a b c d e f"
    exit 1
fi

bash ./run_all_evals.sh --workload "$WORKLOAD" "$RUNTIME_SECONDS"
