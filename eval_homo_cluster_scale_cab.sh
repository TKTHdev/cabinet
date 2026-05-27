#!/bin/bash
# ================================================================
# HOMOGENEOUS CLUSTER SCALE SWEEP
# Runs increasing server counts with 2 fixed clients.
# MAX_INFLIGHT scales with server count to keep the workload window open.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/cabinet"
REMOTE_EVAL_DIR="${REMOTE_DIR}/eval"
REMOTE_LOG_DIR="${REMOTE_DIR}/logs"
BINARY="cabinet"
CONFIG_PATH_LOCAL="${SCRIPT_DIR}/config/cluster_homo.conf"
CONFIG_PATH_REMOTE="${REMOTE_DIR}/config/cluster_homo.conf"
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"

SERVER_COUNTS=(3 5 7 11 20 30)
CLIENT_IPS=(
    "192.168.73.45"
    "192.168.73.229"
)

WORKLOAD="${WORKLOAD:-a}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
RESULT_ROOT="${SCRIPT_DIR}/results/cluster_scale_homo"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

BASE_ENV=(
    "THRESHOLD=1"
    "OPS=0"
    "EVAL_TYPE=0"
    "BATCHSIZE=1"
    "MSG_SIZE=512"
    "MODE=1"
    "CONFLICT_RATE=0"
    "INDEP_RATIO=90"
    "COMMON_RATIO=10"
    "BATCH_COMPOSITION=object-specific"
    "PIPELINE_MODE=true"
    "USE_ADAPTIVE_LIMITER=false"
    "PARALLEL_FAST_PATH=true"
    "LOG_LEVEL=info"
    "ENABLE_PRIORITY=true"
    "LATENCY_DEBUG=false"
    "SERVER_BATCHING=false"
)

mkdir -p "$RUN_DIR"

go build -o "$BINARY"

remote_exec() {
    local host=$1
    shift
    ssh -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" "$USER@$host" "$*"
}

read_server_ips() {
    local count=$1
    mapfile -t SERVER_IPS < <(awk 'NF >= 2 { print $2 }' "$CONFIG_PATH_LOCAL" | head -n "$count")
    if [ "${#SERVER_IPS[@]}" -lt "$count" ]; then
        echo "ERROR: config file does not contain enough server IPs for ${count} nodes"
        exit 1
    fi
}
        echo "  Archiving eval and logs from ${host}..."
        scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" -r \
            "$USER@$host:${REMOTE_EVAL_DIR}/." "$case_eval_dir/" || echo "  WARNING: failed to copy eval from ${host}"
        scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" -r \
            "$USER@$host:${REMOTE_LOG_DIR}/." "$case_log_dir/" || echo "  WARNING: failed to copy logs from ${host}"

copy_binary() {
    local host=$1
    scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" "$BINARY" "$USER@$host:$REMOTE_DIR/"
}

copy_config() {
    local host=$1
    ssh -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" "$USER@$host" "mkdir -p '$REMOTE_DIR/config'"
    scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" "$CONFIG_PATH_LOCAL" "$USER@$host:$CONFIG_PATH_REMOTE"
}

stop_nodes() {
    local host

    for host in "${CLIENT_IPS[@]}"; do
        remote_exec "$host" "pkill -INT -x ${BINARY} 2>/dev/null || true"
    done

    sleep 35

    for host in "${CLIENT_IPS[@]}"; do
        remote_exec "$host" "pkill -TERM -x ${BINARY} 2>/dev/null || true"
    done

    sleep 5

    for host in "${SERVER_IPS[@]}"; do
        remote_exec "$host" "pkill -TERM -x ${BINARY} 2>/dev/null || true"
    done

    sleep 20

    for host in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        remote_exec "$host" "pkill -9 -x ${BINARY} 2>/dev/null || true"
    done
}

archive_case() {
    local label=$1
    shift
    local case_dir="${RUN_DIR}/${label}"
    mkdir -p "$case_dir"
    local case_eval_dir="${case_dir}/eval"
    local case_log_dir="${case_dir}/logs"
    mkdir -p "$case_eval_dir" "$case_log_dir"

    local host
    for host in "$@"; do
        scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" -r \
            "$USER@$host:${REMOTE_EVAL_DIR}/." "$case_eval_dir/" 2>/dev/null || true
        scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" -r \
            "$USER@$host:${REMOTE_LOG_DIR}/." "$case_log_dir/" 2>/dev/null || true
    done
}

merge_case_results() {
    local label=$1
    local server_count=$2
    local client_count=$3
    local case_dir="${RUN_DIR}/${label}"
    local case_eval_dir="${case_dir}/eval"
    local case_merged_dir="${case_dir}/merged"
    local client_start_id=$server_count
    local client_end_id=$((server_count + client_count - 1))
    local client_id_filter="${client_start_id}-${client_end_id}"
    local server_id_filter="0-$((server_count - 1))"

    mkdir -p "$case_eval_dir" "$case_merged_dir"

    echo "Merging client and server CSVs for ${label}..."
    if [ ! -f "$MERGE_SCRIPT" ]; then
        echo " ✗ merge_eval.py not found at ${MERGE_SCRIPT}"
        return 1
    fi

    # Skip merging if no CSVs were copied
    if ! find "$case_eval_dir" -type f -name '*.csv' | read; then
        echo " ✗ No CSV files found in ${case_eval_dir}; skipping merge"
        return 0
    fi

    python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" --ids "$client_id_filter" || echo "  WARNING: client merge returned non-zero"
    python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" --servers --ids "$server_id_filter" || echo "  WARNING: server merge returned non-zero"
}

start_server() {
    local server_id=$1
    local host=$2
    local server_count=$3
    local max_inflight=$4
    local enable_priority=$5

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
mkdir -p '$REMOTE_LOG_DIR/server_${server_count}_${server_id}' '$REMOTE_EVAL_DIR'
SERVER_BATCHING=false \
PARALLEL_FAST_PATH=true \
nohup ./$BINARY \
    -id=${server_id} \
    -n=${server_count} \
    -t=1 \
    -path='$CONFIG_PATH_REMOTE' \
    -pd=true \
    -role=0 \
    -ops=0 \
    -b=1 \
    -indep=90 \
    -common=10 \
    -et=0 \
    -ms=512 \
    -mode=1 \
    -log=info \
    -ep=${enable_priority} \
    > '$REMOTE_LOG_DIR/server_${server_count}_${server_id}/output.log' 2>&1 &
EOF
}

start_client() {
    local client_id=$1
    local host=$2
    local server_count=$3
    local max_inflight=$4
    local enable_priority=$5

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
    mkdir -p '$REMOTE_LOG_DIR/client_${server_count}_${client_id}' '$REMOTE_EVAL_DIR/client${client_id}'
PIPELINE_MODE=true \
MAX_INFLIGHT=${max_inflight} \
nohup ./$BINARY \
    -id=${client_id} \
    -n=${server_count} \
    -t=1 \
    -path='$CONFIG_PATH_REMOTE' \
    -ops=0 \
    -et=0 \
    -pd=true \
    -role=1 \
    -b=1 \
    -indep=90 \
    -common=10 \
    -conflictrate=0 \
    -bcomp=object-specific \
    -ms=512 \
    -mode=1 \
    -log=info \
    -ep=${enable_priority} \
    > '$REMOTE_LOG_DIR/client_${server_count}_${client_id}/output.log' 2>&1 &
EOF
}

run_case() {
    local server_count=$1
    local label="homo_${server_count}"
    local max_inflight=8
    local client_count=${#CLIENT_IPS[@]}
    local enable_priority=true

    if [ "$server_count" -eq 3 ]; then
        enable_priority=false
    fi

    read_server_ips "$server_count"

    echo ""
    echo "=================================================="
    echo "Running: ${label}"
    echo "  Servers: ${server_count}"
    echo "  Clients : ${client_count}"
    echo "  MAX_INFLIGHT: ${max_inflight}"
    echo "=================================================="

    for host in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        copy_binary "$host"
        copy_config "$host"
    done

    for server_id in "${!SERVER_IPS[@]}"; do
        start_server "$server_id" "${SERVER_IPS[$server_id]}" "$server_count" "$max_inflight" "$enable_priority"
        sleep 1
    done

    sleep 15

    local client_id="$server_count"
    for client_vm_idx in "${!CLIENT_IPS[@]}"; do
        start_client "$client_id" "${CLIENT_IPS[$client_vm_idx]}" "$server_count" "$max_inflight" "$enable_priority"
        client_id=$((client_id + 1))
        sleep 1
    done

    sleep "$RUNTIME_SECONDS"

    stop_nodes "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"
    archive_case "$label" "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"
    merge_case_results "$label" "$server_count" "$client_count"
}

cleanup() {
    stop_nodes "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}" || true
}

trap cleanup EXIT

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash eval_homo_cluster_scale.sh

Environment overrides:
  WORKLOAD=a|b|c|d|e|f
  RUNTIME_SECONDS=30

Runs homogeneous cluster sizes: 3, 5, 7, 11, 20, 30
with 2 fixed clients and MAX_INFLIGHT equal to the server count.
EOF
    exit 0
fi

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║             HOMOGENEOUS CLUSTER SCALE SWEEP                   ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo "Workload: $WORKLOAD"
echo ""

for server_count in "${SERVER_COUNTS[@]}"; do
    run_case "$server_count"
done

echo ""
echo "=================================================="
echo " Homogeneous scale sweep complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"