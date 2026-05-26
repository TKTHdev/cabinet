#!/bin/bash
# ================================================================
# Cabinet Cloud Cluster Launcher - HETEROGENEOUS CLUSTER (FIXED)
# ================================================================

set -euo pipefail
trap 'echo " Script interrupted. Exiting..."; exit 1' INT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/cabinet"
BINARY="cabinet"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_new.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# All vars overridable via env (used by run_hetero_plainmsg_evals.sh)
NUM_SERVERS="${NUM_SERVERS:-5}"
NUM_CLIENTS="${NUM_CLIENTS:-2}"
THRESHOLD="${THRESHOLD:-1}"
OPS="${OPS:-0}"
EVAL_TYPE="${EVAL_TYPE:-0}"
BATCHSIZE="${BATCHSIZE:-1}"
MSG_SIZE="${MSG_SIZE:-512}"
MODE="${MODE:-1}"
HOT_RATIO="${HOT_RATIO:-0}"
INDEP_RATIO="${INDEP_RATIO:-90}"
COMMON_RATIO="${COMMON_RATIO:-10}"
BATCH_MODE="${BATCH_MODE:-single}"
BATCH_COMPOSITION="${BATCH_COMPOSITION:-object-specific}"
RATIO_STEP="${RATIO_STEP:-0.001}"
LOG_LEVEL="${LOG_LEVEL:-info}"
ENABLE_PRIORITY="${ENABLE_PRIORITY:-true}"
MAX_INFLIGHT="${MAX_INFLIGHT:-1}"

SERVER_IPS=(
    "192.168.73.59"
    "192.168.73.243"
    "192.168.73.192"
    "192.168.73.134"
    "192.168.73.132"
)

CLIENT_HOST_IPS=(
    "192.168.73.218"
    "192.168.73.219"
)

CLIENTS_PER_VM=1

# FIX: Use heredoc so variables expand locally before being sent over SSH.
# The old approach wrapped everything in double-quoted strings with inner
# single-quoted variables — those single quotes prevented expansion, so the
# binary received literal strings like '${server_id}' instead of e.g. '0'.

copy_binary() {
    local target_ip=$1
    echo " Copying binary to $target_ip..."
    scp -i "$SSH_KEY" "$BINARY" "$USER@$target_ip:$REMOTE_DIR/"
}

copy_config() {
    local target_ip=$1
    ssh -i "$SSH_KEY" "$USER@$target_ip" "mkdir -p '${REMOTE_DIR}/config'"
    scp -i "$SSH_KEY" "$CONFIG_PATH" "$USER@$target_ip:${REMOTE_DIR}/config/"
}

start_server() {
    local server_id=$1
    local server_ip=$2
    echo " Starting Server ${server_id} on ${server_ip} ..."

    # FIX: heredoc — all Cabinet variables expand on the LOCAL side,
    # so the remote shell receives plain numeric/string values.
    ssh -i "$SSH_KEY" "$USER@$server_ip" bash <<EOF
set -e
cd "${REMOTE_DIR}"
mkdir -p "${LOG_DIR}/server${server_id}" "${EVAL_DIR}"
nohup ./${BINARY} \\
    -id=${server_id} \\
    -n=${NUM_SERVERS} \\
    -t=${THRESHOLD} \\
    -path=${CONFIG_PATH} \\
    -pd=true \\
    -role=0 \\
    -ops=${OPS} \\
    -b=${BATCHSIZE} \\
    -hot=${HOT_RATIO} \\
    -indep=${INDEP_RATIO} \\
    -common=${COMMON_RATIO} \\
    -bmode=${BATCH_MODE} \\
    -bcomp=${BATCH_COMPOSITION} \\
    -et=${EVAL_TYPE} \\
    -ms=${MSG_SIZE} \\
    -mode=${MODE} \\
    -log=${LOG_LEVEL} \\
    -max-inflight=${MAX_INFLIGHT} \
    -ep=${ENABLE_PRIORITY} \\
    -rstep=${RATIO_STEP} \\
    > "${LOG_DIR}/server${server_id}/output.log" 2>&1 &
echo "Server ${server_id} launched (PID \$!)"
EOF
}

start_client() {
    local client_id=$1
    local client_ip=$2
    echo " Starting Client ${client_id} on ${client_ip} ..."

    ssh -i "$SSH_KEY" "$USER@$client_ip" bash <<EOF
set -e
cd "${REMOTE_DIR}"
mkdir -p "${LOG_DIR}/client${client_id}" "${EVAL_DIR}/client${client_id}"
ENABLE_TIMESERIES="${ENABLE_TIMESERIES:-false}" TPS_TIMELINE_INTERVAL_MS="${TPS_TIMELINE_INTERVAL_MS:-500}" nohup ./${BINARY} \
    -id=${client_id} \\
    -n=${NUM_SERVERS} \\
    -t=${THRESHOLD} \\
    -path=${CONFIG_PATH} \\
    -ops=${OPS} \\
    -et=${EVAL_TYPE} \\
    -pd=true \\
    -role=1 \\
    -b=${BATCHSIZE} \\
    -hot=${HOT_RATIO} \\
    -indep=${INDEP_RATIO} \\
    -common=${COMMON_RATIO} \\
    -bmode=${BATCH_MODE} \\
    -bcomp=${BATCH_COMPOSITION} \\
    -ms=${MSG_SIZE} \\
    -mode=${MODE} \\
    -log=${LOG_LEVEL} \\
    -ep=${ENABLE_PRIORITY} \\
    -rstep=${RATIO_STEP} \\
    > "${LOG_DIR}/client${client_id}/output.log" 2>&1 &
echo "Client ${client_id} launched (PID \$!)"
EOF
}

# FIX: wait_for_server_ready — original had two problems:
#   1. The grep strings may not match real log output. Changed to a
#      configurable READY_PATTERN so you can tune it to your actual log lines.
#   2. Loop used `for _ in $(seq 1 45)` which fails in strict pipefail mode
#      if seq output is empty. Replaced with a C-style while loop.
# Pattern that appears in the log once server init is complete.
# From observed output: "majority:" is the last init line before main loop.
# Override via env if your binary logs something different.
READY_PATTERN="${READY_PATTERN:-majority:}"

wait_for_server_ready() {
    local server_id=$1
    local server_ip=$2
    local log_path="${LOG_DIR}/server${server_id}/output.log"
    local tries=0
    local max_tries=60   # increased from 45

    echo " Waiting for Server ${server_id} on ${server_ip} (pattern: '${READY_PATTERN}')..."
    while [ $tries -lt $max_tries ]; do
        # Check the process is still alive
        if ! ssh -i "$SSH_KEY" "$USER@$server_ip" \
                "pgrep -x '${BINARY}' >/dev/null 2>&1" 2>/dev/null; then
            echo " ERROR: Server ${server_id} process exited before becoming ready."
            ssh -i "$SSH_KEY" "$USER@$server_ip" \
                "tail -n 50 '${log_path}' 2>/dev/null || echo '(no log)'"
            exit 1
        fi

        if ssh -i "$SSH_KEY" "$USER@$server_ip" \
                "test -f '${log_path}' && grep -q '${READY_PATTERN}' '${log_path}'" \
                >/dev/null 2>&1; then
            echo " Server ${server_id} is ready."
            return 0
        fi

        sleep 1
        tries=$((tries + 1))
    done

    # Timeout — but if the process is still running, treat as ready rather
    # than hard-failing. The pattern may not match on all Cabinet builds.
    if ssh -i "$SSH_KEY" "$USER@$server_ip" \
            "pgrep -x '${BINARY}' >/dev/null 2>&1" 2>/dev/null; then
        echo " WARNING: Server ${server_id} ready pattern not seen after ${max_tries}s,"
        echo "          but process is running — proceeding (set READY_PATTERN to tune)."
        ssh -i "$SSH_KEY" "$USER@$server_ip" \
            "tail -n 10 '${log_path}' 2>/dev/null || echo '(no log)'"
        return 0
    fi

    echo " ERROR: Server ${server_id} process exited and pattern never matched."
    ssh -i "$SSH_KEY" "$USER@$server_ip" \
        "tail -n 50 '${log_path}' 2>/dev/null || echo '(no log)'"
    exit 1
}

# ---------------------------------------------------------------
# BUILD
# ---------------------------------------------------------------
echo "=============================================="
echo "Building Cabinet binary locally..."
echo "=============================================="
cd "$SCRIPT_DIR"
go build -o "$BINARY"
echo " Build complete."

# ---------------------------------------------------------------
# DISTRIBUTE
# ---------------------------------------------------------------
echo "=============================================="
echo "Copying binary and config to all nodes..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    copy_binary "$ip"
    copy_config "$ip"
done

# ---------------------------------------------------------------
# START SERVERS
# FIX: Original started followers 1..N-1 then waited on each,
# then started leader 0. That's fine but the wait pattern strings
# were role-specific and may not match. Now we use a single
# configurable pattern for all nodes and start them in order:
# followers first (IDs 1..N-1), then leader (ID 0).
# ---------------------------------------------------------------
echo "=============================================="
echo "Starting follower servers (IDs 1 to $((NUM_SERVERS-1)))..."
echo "=============================================="
for i in $(seq 1 $((NUM_SERVERS - 1))); do
    start_server "$i" "${SERVER_IPS[$i]}"
    wait_for_server_ready "$i" "${SERVER_IPS[$i]}"
done

echo "=============================================="
echo "Starting leader server (ID 0)..."
echo "=============================================="
start_server 0 "${SERVER_IPS[0]}"
wait_for_server_ready 0 "${SERVER_IPS[0]}"

echo "Waiting 15 seconds for cluster to stabilize..."
sleep 15

# ---------------------------------------------------------------
# CLEAN PREVIOUS TIMELINE FILES ON CLIENT VMS
# ---------------------------------------------------------------
echo "Cleaning previous timeline files from client VMs..."
for vm_ip in "${CLIENT_HOST_IPS[@]}"; do
    ssh -i "$SSH_KEY" "$USER@$vm_ip" \
        "rm -f '${EVAL_DIR}'/client*/tps_timeline_*.csv '${EVAL_DIR}'/client*/.event 2>/dev/null || true" &
done
wait
echo "Client timeline files cleaned."

# ---------------------------------------------------------------
# START CLIENTS
# FIX: client_id increment now uses arithmetic instead of (( ))
# which can exit with code 1 when result is 0 under set -e.
# ---------------------------------------------------------------
echo "=============================================="
echo "Starting ${NUM_CLIENTS} clients (${CLIENTS_PER_VM} per VM)..."
echo "=============================================="
client_id="${NUM_SERVERS}"
for vm_ip in "${CLIENT_HOST_IPS[@]}"; do
    c=0
    while [ "$c" -lt "$CLIENTS_PER_VM" ]; do
        if [ "$client_id" -lt "$((NUM_SERVERS + NUM_CLIENTS))" ]; then
            start_client "$client_id" "$vm_ip"
            client_id=$((client_id + 1))
            sleep 1
        fi
        c=$((c + 1))
    done
done

echo "=============================================="
echo " Cabinet heterogeneous cluster launched!"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Servers  : ${NUM_SERVERS} (IDs 0-$((NUM_SERVERS-1)))"
echo "  Clients  : ${NUM_CLIENTS} (IDs ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo "  Config   : cluster_hetero_new.conf"
echo "  Log level: ${LOG_LEVEL}"
echo ""
echo "Monitor logs:"
echo "  ssh -i $SSH_KEY ubuntu@${SERVER_IPS[0]} 'tail -f ${LOG_DIR}/server0/output.log'"
echo "  ssh -i $SSH_KEY ubuntu@${CLIENT_HOST_IPS[0]} 'tail -f ${LOG_DIR}/client${NUM_SERVERS}/output.log'"
echo ""
echo "Stop all processes:"
echo "  ./stop_cluster_hetero.sh"
echo "=============================================="
