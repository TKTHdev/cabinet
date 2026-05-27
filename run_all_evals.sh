#!/bin/bash
# ================================================================
# MASTER MONGODB EVALUATION RUNNER
# Runs fixed MongoDB eval scripts sequentially or one-shot workload mode
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║           COMPREHENSIVE MONGODB WORKLOAD A EVALUATION           ║"
echo "║                    5-Node Heterogeneous Cluster                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

EXIT_STATUS=0

RUN_EVAL1="${1:-1}"
RUN_EVAL2="${2:-1}"
RUN_EVAL3="${3:-1}"
RUN_EVAL4="${4:-1}"
RUN_EVAL5="${5:-1}"
WORKLOAD="${WORKLOAD:-a}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-0}"

START_SCRIPT="start_mongodb_hetero_5n_fixed.sh"
STOP_SCRIPT="stop_mongodb_hetero_5n.sh"
EVAL1_SCRIPT="eval_1_indep_common_ratio_fixed.sh"
EVAL2_SCRIPT="eval_2_max_inflight_fixed.sh"
EVAL3_SCRIPT="eval_3_fault_tolerance_fixed.sh"
EVAL4_SCRIPT="eval_4_network_delay_fixed.sh"
EVAL5_SCRIPT="eval_5_workload_sweep.sh"

if [ "${1:-}" = "--help" ]; then
    cat <<'EOF'
Usage:
    bash run_all_evals.sh [eval1] [eval2] [eval3] [eval4] [eval5]
  bash run_all_evals.sh --workload [a-f] [runtime_seconds]
    echo ""
Eval toggles (1=run, 0=skip):
  eval1: Independent vs Common Ratio (default: 1)
  eval2: Max Pipeline In-Flight (default: 1)
  eval3: Fault Tolerance (default: 1)
  eval4: Network Delay (default: 1)
    eval5: Workload Sweep (default: 1)

Examples:
  bash run_all_evals.sh 1 1 0 0
  bash run_all_evals.sh 0 0 0 1
  bash run_all_evals.sh --workload a 300

Env overrides:
  WORKLOAD=a|b|c|d|e|f   (default: a)
  RUNTIME_SECONDS=0      (for --workload mode; 0 means leave running)
EOF
    exit 0
fi

if [ "${1:-}" = "--workload" ]; then
    WORKLOAD="${2:-a}"
    RUNTIME_SECONDS="${3:-0}"

    if [[ ! "$WORKLOAD" =~ ^[a-f]$ ]]; then
        echo "ERROR: workload must be one of: a b c d e f"
        exit 1
    fi

    if [ ! -f "$START_SCRIPT" ]; then
        echo "ERROR: missing $START_SCRIPT"
        exit 1
    fi

    bash "./$START_SCRIPT" "$WORKLOAD"

    if [ "$RUNTIME_SECONDS" -gt 0 ]; then
        echo "Running workload '$WORKLOAD' for ${RUNTIME_SECONDS}s..."
        sleep "$RUNTIME_SECONDS"
        if [ -f "$STOP_SCRIPT" ]; then
            echo "Stopping cluster after timed run..."
            bash "./$STOP_SCRIPT" || EXIT_STATUS=1
        else
            echo "Warning: stop script $STOP_SCRIPT not found"
            EXIT_STATUS=1
        fi
    else
        echo "Cluster is running workload '$WORKLOAD'. Stop manually with: bash ./$STOP_SCRIPT"
    fi

    exit $EXIT_STATUS
fi

for required in "$EVAL1_SCRIPT" "$EVAL2_SCRIPT" "$EVAL3_SCRIPT" "$EVAL4_SCRIPT" "$EVAL5_SCRIPT"; do
    if [ ! -f "$required" ]; then
        echo "ERROR: required fixed script not found: $required"
        exit 1
    fi
done

START_TIME=$(date +%s)

run_eval_script() {
    local label=$1
    local script=$2

    if ! bash "$script"; then
        echo ""
        echo "Warning: $label failed. Continuing to the remaining steps."
        EXIT_STATUS=1
    fi
}

# EVAL 1: Independent vs Common Ratio
if [ "$RUN_EVAL1" -eq 1 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running EVAL 1: Independent vs Common Ratio"
    echo "════════════════════════════════════════════════════════════════"
    run_eval_script "EVAL 1" "$EVAL1_SCRIPT"
fi

# EVAL 2: Max Pipeline In-Flight
if [ "$RUN_EVAL2" -eq 1 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running EVAL 2: Max Pipeline In-Flight"
    echo "════════════════════════════════════════════════════════════════"
    run_eval_script "EVAL 2" "$EVAL2_SCRIPT"
fi

# EVAL 3: Fault Tolerance
if [ "$RUN_EVAL3" -eq 1 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running EVAL 3: Fault Tolerance"
    echo "════════════════════════════════════════════════════════════════"
    run_eval_script "EVAL 3" "$EVAL3_SCRIPT"
fi

# EVAL 4: Network Delay
if [ "$RUN_EVAL4" -eq 1 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running EVAL 4: Network Delay Impact"
    echo "════════════════════════════════════════════════════════════════"
    run_eval_script "EVAL 4" "$EVAL4_SCRIPT"
fi

# EVAL 5: Workload Sweep
if [ "$RUN_EVAL5" -eq 1 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "Running EVAL 5: Workload Sweep"
    echo "════════════════════════════════════════════════════════════════"
    run_eval_script "EVAL 5" "$EVAL5_SCRIPT"
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                  ALL EVALUATIONS COMPLETE                       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Total execution time: $((DURATION / 3600))h $((DURATION % 3600 / 60))m $((DURATION % 60))s"
echo ""
echo "Collecting and organizing results..."
echo ""

# Run result collection script
if [ -f "collect_eval_results.sh" ]; then
    if ! bash collect_eval_results.sh; then
        EXIT_STATUS=1
    fi
else
    echo "Note: collect_eval_results.sh not found. Manual collection:"
    echo "  ssh -i /path/to/tani.pem ubuntu@192.168.73.159 'ls -lah /home/ubuntu/woc/eval/'"
fi

exit $EXIT_STATUS
