#!/bin/bash
# ================================================================
# HETEROGENEOUS PLAIN-MSG EVALUATION RUNNER (FIXED)
# Runs the 5-node Cabinet cluster across all experiment families.
#
# Fixes vs original:
#   1. BINARY_NAME set to "cabinet" (was "woc") — crash injection
#      now actually kills the right process.
#   2. BASE_ENV keys aligned to what start_cluster_hetero.sh reads.
#      Added MAX_INFLIGHT so eval2 can sweep client pipeline depth.
#   3. archive_latest_result: rm old eval dirs before each run so
#      merged CSVs from a previous scenario don't pollute the next.
#   4. kill_cabinet_on_node replaces kill_woc_on_node throughout.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
SSH_OPTS="-o ConnectTimeout=5 -o StrictHostKeyChecking=no -i ${SSH_KEY}"
REMOTE_DIR="/home/ubuntu/cabinet"

# FIX: must match BINARY in start_cluster_hetero.sh
BINARY_NAME="cabinet"

RESULT_ROOT="${SCRIPT_DIR}/results/hetero_plainmsg"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
EVAL_DIR="${SCRIPT_DIR}/eval"
LOCAL_CONFIG_PATH="${SCRIPT_DIR}/config/cluster_hetero_new.conf"

CLUSTER_ACTIVE=false

SERVER_IPS=(
    "192.168.73.59"
    "192.168.73.243"
    "192.168.73.192"
    "192.168.73.134"
    "192.168.73.132"
)

CLIENT_IPS=(
    "192.168.73.218"
    "192.168.73.219"
)

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
FAULT_PRE_DELAY_SECONDS="${FAULT_PRE_DELAY_SECONDS:-5}"
CRASH_TRIGGER_SECONDS="${CRASH_TRIGGER_SECONDS:-10}"
EVAL_ONLY="${1:-all}"
TPS_TIMELINE_INTERVAL_MS="${TPS_TIMELINE_INTERVAL_MS:-500}"

# FIX: BASE_ENV uses only keys that start_cluster_hetero.sh recognises.
# WoC-specific keys (PIPELINE_MODE, USE_ADAPTIVE_LIMITER,
# PARALLEL_FAST_PATH, SERVER_BATCHING) are removed.
BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=2"
    # Cabinet baseline keeps threshold at 1.
    "THRESHOLD=1"
    "OPS=0"
    "EVAL_TYPE=0"
    "BATCHSIZE=1"
    "MSG_SIZE=512"
    "MODE=1"
    "HOT_RATIO=0"
    "INDEP_RATIO=90"
    "COMMON_RATIO=10"
    "BATCH_MODE=single"
    "BATCH_COMPOSITION=object-specific"
    "LOG_LEVEL=info"
    "ENABLE_PRIORITY=true"
    "RATIO_STEP=0.001"
)

mkdir -p "$RUN_DIR"

# ================================================================
# HELPERS
# ================================================================

remote_exec() {
    local host=$1; shift
    ssh ${SSH_OPTS} "$USER@$host" "$*"
}

detect_interface() {
    local host=$1
    remote_exec "$host" "ip route show default 2>/dev/null | awk '{print \$5; exit}'"
}

cache_all_interfaces() {
    echo "  [iface] Caching network interfaces..."
    _CACHED_SERVER_IFACES=()
    _CACHED_CLIENT_IFACES=()
    for ip in "${SERVER_IPS[@]}"; do
        local iface; iface=$(detect_interface "$ip")
        _CACHED_SERVER_IFACES+=("$iface")
    done
    for ip in "${CLIENT_IPS[@]}"; do
        local iface; iface=$(detect_interface "$ip")
        _CACHED_CLIENT_IFACES+=("$iface")
    done
}

start_cluster() {
    CLUSTER_ACTIVE=true
    env "TPS_TIMELINE_INTERVAL_MS=${TPS_TIMELINE_INTERVAL_MS}" "${BASE_ENV[@]}" bash "$START_SCRIPT"
}

start_cluster_with_timeseries() {
    CLUSTER_ACTIVE=true
    env "ENABLE_TIMESERIES=true" "TPS_TIMELINE_INTERVAL_MS=${TPS_TIMELINE_INTERVAL_MS}" "${BASE_ENV[@]}" bash "$START_SCRIPT"
}

stop_cluster() {
    env "${BASE_ENV[@]}" bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false
}

archive_results() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"

    local merged_dir="${EVAL_DIR}/merged"
    if [ -d "$merged_dir" ]; then
        cp "$merged_dir"/*.csv "$dest_dir/" 2>/dev/null || true
    fi
    # Also grab raw client dirs in case merge failed
    cp -r "${EVAL_DIR}"/client* "$dest_dir/" 2>/dev/null || true

    echo "  Archived results to: $dest_dir"
}

# FIX: renamed from kill_woc_on_node to kill_cabinet_on_node
kill_cabinet_on_node() {
    local ip=$1
    local label=${2:-cabinet}

    if remote_exec "$ip" "pgrep -x ${BINARY_NAME} >/dev/null 2>&1"; then
        echo "  Killing ${label} on ${ip}..."
        remote_exec "$ip" "pkill -TERM -x ${BINARY_NAME} 2>/dev/null || true" || true
        sleep 2
        if remote_exec "$ip" "pgrep -x ${BINARY_NAME} >/dev/null 2>&1"; then
            remote_exec "$ip" "pkill -KILL -x ${BINARY_NAME} 2>/dev/null || true" || true
            sleep 1
        fi
        echo "  Confirmed ${label} stopped on ${ip}"
    else
        echo "  Note: ${label} was not running on ${ip}"
    fi
}

inject_failures() {
    local nodes_csv=$1
    [ -z "$nodes_csv" ] && return 0
    echo "Injecting failures on server nodes: $nodes_csv"
    IFS=',' read -r -a nodes <<< "$nodes_csv"
    for node_id in "${nodes[@]}"; do
        kill_cabinet_on_node "${SERVER_IPS[$node_id]}" "server${node_id}"
    done
}

# ================================================================
# NETEM FUNCTIONS
# ================================================================

apply_uniform_delay() {
    local delay_ms=$1
    local jitter_ms=$2
    if [ "$delay_ms" -eq 0 ]; then
        remove_all_delay; return 0
    fi
    echo "  [netem D1] ${delay_ms}ms ±${jitter_ms}ms on all nodes..."
    for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        local iface; iface=$(detect_interface "$ip")
        [ -z "$iface" ] && continue
        remote_exec "$ip" \
            "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
             sudo tc qdisc add dev '$iface' root netem delay ${delay_ms}ms ${jitter_ms}ms distribution normal" \
            || true
    done
    sleep 1
}

remove_all_delay() {
    echo "  [netem] Removing all impairments..."
    local use_cache=false
    if declare -p _CACHED_SERVER_IFACES _CACHED_CLIENT_IFACES >/dev/null 2>&1 && \
       [ "${#_CACHED_SERVER_IFACES[@]}" -eq "${#SERVER_IPS[@]}" ] && \
       [ "${#_CACHED_CLIENT_IFACES[@]}" -eq "${#CLIENT_IPS[@]}" ]; then
        use_cache=true
    fi

    local all_ips=("${SERVER_IPS[@]}" "${CLIENT_IPS[@]}")

    if [ "$use_cache" = true ]; then
        local all_ifaces=("${_CACHED_SERVER_IFACES[@]}" "${_CACHED_CLIENT_IFACES[@]}")
        for idx in "${!all_ips[@]}"; do
            local ip="${all_ips[$idx]}"
            local iface="${all_ifaces[$idx]}"
            [ -z "$iface" ] && continue
            ssh -i "$SSH_KEY" "$USER@$ip" \
                "sudo tc qdisc del dev '$iface' root 2>/dev/null || true" || true &
        done
        wait
    else
        for ip in "${all_ips[@]}"; do
            local iface; iface=$(detect_interface "$ip")
            [ -z "$iface" ] && continue
            remote_exec "$ip" "sudo tc qdisc del dev '$iface' root 2>/dev/null || true" || true
        done
    fi
    sleep 1
}

# ================================================================
# RUN-CASE FUNCTIONS
# ================================================================

run_case() {
    local label=$1
    local runtime=$2

    echo ""
    echo "=================================================="
    echo "Running: $label  [${runtime}s]"
    echo "=================================================="

    # FIX: clean eval dir before each run so stale CSVs don't bleed through
    rm -rf "${EVAL_DIR}"/client* "${EVAL_DIR}"/server* "${EVAL_DIR}"/merged 2>/dev/null || true

    start_cluster
    sleep "$runtime"
    stop_cluster
    archive_results "$label"
}

run_fault_case() {
    local label=$1
    local failed_nodes=$2

    echo ""
    echo "=================================================="
    echo "Running: $label  [fault: nodes ${failed_nodes}]"
    echo "=================================================="

    rm -rf "${EVAL_DIR}"/client* "${EVAL_DIR}"/server* "${EVAL_DIR}"/merged 2>/dev/null || true

    start_cluster
    sleep "$FAULT_PRE_DELAY_SECONDS"
    inject_failures "$failed_nodes"
    sleep "$((RUNTIME_SECONDS - FAULT_PRE_DELAY_SECONDS))"
    stop_cluster
    archive_results "$label"
}

run_d1_case() {
    local label=$1
    local delay_ms=$2
    local jitter_ms=$3

    echo ""
    echo "=================================================="
    echo "Running: $label  [D1 uniform ${delay_ms}ms ±${jitter_ms}ms]"
    echo "=================================================="

    rm -rf "${EVAL_DIR}"/client* "${EVAL_DIR}"/server* "${EVAL_DIR}"/merged 2>/dev/null || true

    apply_uniform_delay "$delay_ms" "$jitter_ms"
    start_cluster
    sleep "$RUNTIME_SECONDS"
    remove_all_delay
    stop_cluster
    archive_results "$label"
}

run_d4_case() {
    local label=$1
    local calm_duration="${2:-10}"
    local burst_duration="${3:-5}"
    local runtime_override="${4:-$RUNTIME_SECONDS}"

    echo ""
    echo "=================================================="
    echo "Running: $label  [D4 burst: ${calm_duration}s calm / ${burst_duration}s spike, ${runtime_override}s total]"
    echo "=================================================="

    rm -rf "${EVAL_DIR}"/client* "${EVAL_DIR}"/server* "${EVAL_DIR}"/merged 2>/dev/null || true

    cache_all_interfaces
    remove_all_delay
    start_cluster

    local elapsed=0
    local cycle=0

    while [ "$elapsed" -lt "$runtime_override" ]; do
        echo "  [D4] Cycle ${cycle}: CALM (${calm_duration}s)"
        for i in "${!SERVER_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true" || true &
        done
        for i in "${!CLIENT_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${CLIENT_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_CLIENT_IFACES[$i]}' root 2>/dev/null || true" || true &
        done
        wait

        local calm_sleep=$(( calm_duration < (runtime_override - elapsed) ? calm_duration : (runtime_override - elapsed) ))
        sleep "$calm_sleep"
        elapsed=$(( elapsed + calm_sleep ))
        [ "$elapsed" -ge "$runtime_override" ] && break

        echo "  [D4] Cycle ${cycle}: BURST (${burst_duration}s, 1000±100ms)"
        for i in "${!SERVER_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '${_CACHED_SERVER_IFACES[$i]}' root netem delay 1000ms 100ms distribution normal" \
                || true &
        done
        for i in "${!CLIENT_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${CLIENT_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_CLIENT_IFACES[$i]}' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '${_CACHED_CLIENT_IFACES[$i]}' root netem delay 1000ms 100ms distribution normal" \
                || true &
        done
        wait

        local burst_sleep=$(( burst_duration < (runtime_override - elapsed) ? burst_duration : (runtime_override - elapsed) ))
        sleep "$burst_sleep"
        elapsed=$(( elapsed + burst_sleep ))
        cycle=$(( cycle + 1 ))
    done

    remove_all_delay
    stop_cluster
    archive_results "$label"
}

inject_event() {
    local label=$1
    local num_servers="${NUM_SERVERS:-${#SERVER_IPS[@]}}"

    echo "  [event] ${label}"

    for i in "${!CLIENT_IPS[@]}"; do
        local cid=$(( num_servers + i ))
        local event_path="${REMOTE_DIR}/eval/client${cid}/.event"
        timeout 8s ssh ${SSH_OPTS} "$USER@${CLIENT_IPS[$i]}" \
            "mkdir -p '${REMOTE_DIR}/eval/client${cid}' && printf '%s\n' '${label}' > '${event_path}'" \
            >/dev/null 2>&1 || echo "  [event] warning: timeout writing event on ${CLIENT_IPS[$i]}" &
    done
    wait
}

run_d1_case_sampled() {
    local label=$1
    local delay_ms=$2
    local jitter_ms=$3

    echo ""
    echo "=================================================="
    echo "Running (sampled): $label  [D1 ${delay_ms}ms ±${jitter_ms}ms]"
    echo "=================================================="

    rm -rf "${EVAL_DIR}"/client* "${EVAL_DIR}"/server* "${EVAL_DIR}"/merged 2>/dev/null || true

    apply_uniform_delay "$delay_ms" "$jitter_ms"
    start_cluster_with_timeseries
    inject_event "delay_${delay_ms}ms"

    sleep "$RUNTIME_SECONDS"

    remove_all_delay
    stop_cluster
    archive_results "$label"
}

run_d4_case_sampled() {
    local label=$1
    local calm_duration="${2:-10}"
    local burst_duration="${3:-5}"
    local runtime_override="${4:-$RUNTIME_SECONDS}"

    echo ""
    echo "=================================================="
    echo "Running (sampled): $label  [D4 ${calm_duration}s calm / ${burst_duration}s burst]"
    echo "=================================================="

    rm -rf "${EVAL_DIR}"/client* "${EVAL_DIR}"/server* "${EVAL_DIR}"/merged 2>/dev/null || true

    cache_all_interfaces
    remove_all_delay
    start_cluster_with_timeseries
    inject_event "calm_start"

    local elapsed=0
    local cycle=0

    while [ "$elapsed" -lt "$runtime_override" ]; do
        inject_event "calm_c${cycle}"
        for i in "${!SERVER_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true" || true &
        done
        for i in "${!CLIENT_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${CLIENT_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_CLIENT_IFACES[$i]}' root 2>/dev/null || true" || true &
        done
        wait

        local calm_sleep=$(( calm_duration < (runtime_override - elapsed) ? calm_duration : (runtime_override - elapsed) ))
        sleep "$calm_sleep"
        elapsed=$(( elapsed + calm_sleep ))
        [ "$elapsed" -ge "$runtime_override" ] && break

        inject_event "burst_c${cycle}"
        for i in "${!SERVER_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '${_CACHED_SERVER_IFACES[$i]}' root netem delay 1000ms 100ms distribution normal" \
                || true &
        done
        for i in "${!CLIENT_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${CLIENT_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_CLIENT_IFACES[$i]}' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '${_CACHED_CLIENT_IFACES[$i]}' root netem delay 1000ms 100ms distribution normal" \
                || true &
        done
        wait

        local burst_sleep=$(( burst_duration < (runtime_override - elapsed) ? burst_duration : (runtime_override - elapsed) ))
        sleep "$burst_sleep"
        elapsed=$(( elapsed + burst_sleep ))
        cycle=$(( cycle + 1 ))
    done

    inject_event "post_burst"
    remove_all_delay
    stop_cluster
    archive_results "$label"
}

run_crash_case_sampled() {
    local label=$1
    local node_spec=$2

    echo ""
    echo "=================================================="
    echo "Running (sampled): $label  [crash: ${node_spec} at t=${CRASH_TRIGGER_SECONDS}s]"
    echo "=================================================="

    rm -rf "${EVAL_DIR}"/client* "${EVAL_DIR}"/server* "${EVAL_DIR}"/merged 2>/dev/null || true

    start_cluster_with_timeseries
    inject_event "stable"
    echo "  [crash] Waiting ${CRASH_TRIGGER_SECONDS}s before fault injection..."
    sleep "$CRASH_TRIGGER_SECONDS"

    local kind="${node_spec%%:*}"
    local arg="${node_spec#*:}"

    case "$kind" in
        no_failure)
            inject_event "no_failure_baseline"
            ;;
        leader)
            inject_event "crash_leader"
            kill_cabinet_on_node "${SERVER_IPS[0]}" "leader"
            ;;
        follower)
            inject_event "crash_follower${arg}"
            kill_cabinet_on_node "${SERVER_IPS[$arg]}" "server${arg}"
            ;;
        f_of_n)
            local available=()
            for i in "${!SERVER_IPS[@]}"; do
                [ "$i" -eq 0 ] && continue
                available+=("$i")
            done
            local k=0
            while [ $k -lt "$arg" ] && [ "${#available[@]}" -gt 0 ]; do
                local pick=$(( RANDOM % ${#available[@]} ))
                local fid="${available[$pick]}"
                kill_cabinet_on_node "${SERVER_IPS[$fid]}" "server${fid}" &
                available=("${available[@]:0:$pick}" "${available[@]:$(( pick+1 ))}")
                k=$((k + 1))
            done
            wait
            inject_event "crash_f${arg}"
            ;;
        *)
            echo "  ERROR: unknown crash spec '$node_spec'"
            return 1 ;;
    esac

    inject_event "post_crash"
    echo "  [crash] Observing ${RUNTIME_SECONDS}s after fault..."
    sleep "$RUNTIME_SECONDS"

    stop_cluster
    archive_results "$label"
}

run_crash_case() {
    local label=$1
    local node_spec=$2

    echo ""
    echo "=================================================="
    echo "Running: $label  [crash: ${node_spec} at t=${CRASH_TRIGGER_SECONDS}s]"
    echo "=================================================="

    rm -rf "${EVAL_DIR}"/client* "${EVAL_DIR}"/server* "${EVAL_DIR}"/merged 2>/dev/null || true

    start_cluster
    echo "  [crash] Waiting ${CRASH_TRIGGER_SECONDS}s before fault injection..."
    sleep "$CRASH_TRIGGER_SECONDS"

    local kind="${node_spec%%:*}"
    local arg="${node_spec#*:}"

    case "$kind" in
        leader)
            kill_cabinet_on_node "${SERVER_IPS[0]}" "leader"
            ;;
        follower)
            kill_cabinet_on_node "${SERVER_IPS[$arg]}" "server${arg}"
            ;;
        f_of_n)
            # Kill 'arg' random followers (excluding leader at index 0)
            local available=()
            for i in "${!SERVER_IPS[@]}"; do
                [ "$i" -eq 0 ] && continue
                available+=("$i")
            done
            local k=0
            while [ $k -lt "$arg" ] && [ "${#available[@]}" -gt 0 ]; do
                local pick=$(( RANDOM % ${#available[@]} ))
                local fid="${available[$pick]}"
                kill_cabinet_on_node "${SERVER_IPS[$fid]}" "server${fid}" &
                available=("${available[@]:0:$pick}" "${available[@]:$(( pick+1 ))}")
                k=$((k + 1))
            done
            wait
            ;;
        *)
            echo "  ERROR: unknown crash spec '$node_spec'"; return 1 ;;
    esac

    echo "  [crash] Observing ${RUNTIME_SECONDS}s after fault..."
    sleep "$RUNTIME_SECONDS"
    stop_cluster
    archive_results "$label"
}

# ================================================================
# CLEANUP TRAP
# ================================================================
cleanup() {
    remove_all_delay || true
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_cluster || true
    fi
}
trap cleanup EXIT INT

# ================================================================
# ARGUMENT PARSING
# ================================================================
if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_hetero_plainmsg_evals.sh [selector]

Selectors (default: all):
  eval1          Independent vs common ratio sweep
    eval2          Client max inflight sweep
  eval_batching  Batch size sweep (1 10 50 100 500 1000 2000)
  eval_msgsize   Message size sweep (64 512 1024 2048 4096)
  eval_crash     Crash fault injection (leader/follower/random)
  eval4          Network delay: D1 uniform sweep + D4 burst

Environment overrides:
  RUNTIME_SECONDS=30
  FAULT_PRE_DELAY_SECONDS=5
  CRASH_TRIGGER_SECONDS=10

Results: results/hetero_plainmsg/<timestamp>/
EOF
    exit 0
fi

# Strip leading '--' if passed as --eval1 style
[[ "$EVAL_ONLY" == --* ]] && EVAL_ONLY="${EVAL_ONLY#--}"

case "$EVAL_ONLY" in
    all|eval1|eval2|eval_batching|eval_msgsize|eval_crash|eval4|eval4s) ;;
    *)
        echo "ERROR: unknown selector '${EVAL_ONLY}'. Run with --help."
        exit 1 ;;
esac

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║      HETEROGENEOUS PLAIN-MSG EVALUATION RUNNER                ║"
echo "║               5-Node Cabinet + 2 Clients                      ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Result archive: $RUN_DIR"
echo ""

# ================================================================
# EVAL 1: independent vs common ratio sweep
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval1" ]]; then
    echo "── EVAL 1: Ratio sweep ─────────────────────────────────────────"
    for case in "100/0" "90/10" "80/20" "60/40" "40/60" "20/80" "10/90" "0/100"; do
        indep="${case%/*}"
        common="${case#*/}"
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
            "HOT_RATIO=0" "INDEP_RATIO=${indep}" "COMMON_RATIO=${common}"
            "BATCH_MODE=single" "BATCH_COMPOSITION=object-specific"
            "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "RATIO_STEP=0.001"
        )
        run_case "eval1_indep${indep}_common${common}" "$RUNTIME_SECONDS"
    done
fi

# ================================================================
# EVAL 2: baseline 90/10 run
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval2" ]]; then
    echo "── EVAL 2: Client max inflight sweep ───────────────────────────"
    for max_inflight in 1 2 3 4 5 8 10 15 20 25 30 35 40 45 50; do
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
            "HOT_RATIO=0" "INDEP_RATIO=90" "COMMON_RATIO=10"
            "BATCH_MODE=single" "BATCH_COMPOSITION=object-specific"
            "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "RATIO_STEP=0.001"
            "MAX_INFLIGHT=${max_inflight}"
        )
        run_case "eval2_max_inflight_${max_inflight}" "$RUNTIME_SECONDS"
    done
fi

# ================================================================
# EVAL batching: batch size sweep
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval_batching" ]]; then
    echo "── EVAL batching: batch size sweep ─────────────────────────────"
    for batch_size in 1 10 50 100 500 1000 2000; do
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=${batch_size}" "MSG_SIZE=512" "MODE=1"
            "HOT_RATIO=0" "INDEP_RATIO=90" "COMMON_RATIO=10"
            "BATCH_MODE=single" "BATCH_COMPOSITION=object-specific"
            "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "RATIO_STEP=0.001"
        )
        run_case "eval_batching_${batch_size}" "$RUNTIME_SECONDS"
    done
fi

# ================================================================
# EVAL msgsize: message size sweep
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval_msgsize" ]]; then
    echo "── EVAL msgsize: message size sweep ────────────────────────────"
    for msg_size in 64 512 1024 2048 4096; do
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=${msg_size}" "MODE=1"
            "HOT_RATIO=0" "INDEP_RATIO=90" "COMMON_RATIO=10"
            "BATCH_MODE=single" "BATCH_COMPOSITION=object-specific"
            "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "RATIO_STEP=0.001"
        )
        run_case "eval_msgsize_${msg_size}" "$RUNTIME_SECONDS"
    done
fi

# ================================================================
# EVAL crash: fault injection
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval_crash" ]]; then
    echo "── EVAL crash: fault injection ─────────────────────────────────"
    BASE_ENV=(
        "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
        "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
        "HOT_RATIO=0" "INDEP_RATIO=90" "COMMON_RATIO=10"
        "BATCH_MODE=single" "BATCH_COMPOSITION=object-specific"
        "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "RATIO_STEP=0.001"
        "ENABLE_TIMESERIES=true"
    )
    run_crash_case_sampled "eval_crash_no_failure"  "no_failure"
    run_crash_case_sampled "eval_crash_leader"      "leader"
    run_crash_case_sampled "eval_crash_follower1"   "follower:1"
    run_crash_case_sampled "eval_crash_follower4"   "follower:4"
    run_crash_case_sampled "eval_crash_f_of_n1"     "f_of_n:1"
fi

# ================================================================
# EVAL 4: Network delay — D1 uniform sweep + D4 burst
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval4" ]]; then
    echo "── EVAL 4: Network delay ────────────────────────────────────────"

    # D1: uniform delay sweep
    echo "  D1: uniform delay sweep"
    for delay_ms in 0 5 10 20 50 100 200; do
        jitter_ms=0
        [ "$delay_ms" -gt 0 ] && jitter_ms=$(( delay_ms / 5 ))
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
            "HOT_RATIO=0" "INDEP_RATIO=90" "COMMON_RATIO=10"
            "BATCH_MODE=single" "BATCH_COMPOSITION=object-specific"
            "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "RATIO_STEP=0.001"
            "ENABLE_TIMESERIES=true"
        )
        run_d1_case_sampled "eval4_D1_${delay_ms}ms" "$delay_ms" "$jitter_ms"
    done

    # D4: burst stress
    echo "  D4: burst stress (10s calm / 5s spike)"
    BASE_ENV=(
        "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
        "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
        "HOT_RATIO=0" "INDEP_RATIO=90" "COMMON_RATIO=10"
        "BATCH_MODE=single" "BATCH_COMPOSITION=object-specific"
        "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "RATIO_STEP=0.001"
        "ENABLE_TIMESERIES=true"
    )
    D4_RUNTIME=$(( RUNTIME_SECONDS < 45 ? 45 : RUNTIME_SECONDS ))
    run_d4_case_sampled "eval4_D4_burst" 10 5 "$D4_RUNTIME"
fi

# ================================================================
# EVAL 4s: Network delay with sampled client timeseries (fixed MAX_INFLIGHT=5)
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval4s" ]]; then
    echo ""
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║  EVAL 4s: Network Delay (fixed MAX_INFLIGHT=5)                ║"
    echo "╚════════════════════════════════════════════════════════════════╝"

    _SAVED_RUNTIME=$RUNTIME_SECONDS
    RUNTIME_SECONDS=45

    echo ""
    echo "── D1: Uniform delays (MAX_INFLIGHT=5) ──────────────────────────"
    D1_DELAYS=(0 5 10 20 50 100 200)

    for delay_ms in "${D1_DELAYS[@]}"; do
        if [ "$delay_ms" -eq 0 ]; then
            jitter_ms=0
        else
            jitter_ms=$(( delay_ms / 5 ))
        fi
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
            "HOT_RATIO=0" "INDEP_RATIO=90" "COMMON_RATIO=10"
            "BATCH_MODE=single" "BATCH_COMPOSITION=object-specific"
            "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "RATIO_STEP=0.001"
            "MAX_INFLIGHT=5" "ENABLE_TIMESERIES=true"
        )
        run_d1_case_sampled "eval4s_D1_${delay_ms}ms" "$delay_ms" "$jitter_ms"
    done

    RUNTIME_SECONDS=${_SAVED_RUNTIME}

    echo "── D4: Bursting (MAX_INFLIGHT=5, 15s calm / 10s spike) ──────────"
    BASE_ENV=(
        "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=1" "OPS=0"
        "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
        "HOT_RATIO=0" "INDEP_RATIO=90" "COMMON_RATIO=10"
        "BATCH_MODE=single" "BATCH_COMPOSITION=object-specific"
        "LOG_LEVEL=info" "ENABLE_PRIORITY=true" "RATIO_STEP=0.001"
        "MAX_INFLIGHT=5" "ENABLE_TIMESERIES=true"
    )
    D4_RUNTIME=$(( RUNTIME_SECONDS < 90 ? 90 : RUNTIME_SECONDS ))
    run_d4_case_sampled "eval4s_D4_burst" 15 10 "$D4_RUNTIME"
fi

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  All evaluations complete                                      ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Results archived in: $RUN_DIR"