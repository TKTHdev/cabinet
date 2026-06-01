#!/bin/bash
# ================================================================
# HETEROGENEOUS CLUSTER SCALE SWEEP (RAFT)
# Runs increasing hetero server counts with 2 fixed clients.
# Raft mode disables priority and raises the quorum threshold.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

SSH_USER="${SSH_USER:-ubuntu}"
SSH_KEY="${SSH_KEY:-${HOME}/.ssh/tani.pem}"
CONTROLLER="${CONTROLLER:-auto}"
REMOTE_DIR="/home/ubuntu/cabinet"
REMOTE_EVAL_DIR="${REMOTE_DIR}/eval"
REMOTE_LOG_DIR="${REMOTE_DIR}/logs"
BINARY="cabinet"
CLIENT_IPS=(
    "192.168.73.11"
    "192.168.73.234"
)
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"

WORKLOAD="${WORKLOAD:-a}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
RESULT_ROOT="${SCRIPT_DIR}/results/cluster_scale_hetero_raft"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

SERVER_COUNTS=(3 5 7 11 15)

declare -A CONFIG_BY_COUNT=(
    [3]="config/cluster_hetero_3n_2s_1w.conf"
    [5]="config/cluster_hetero_5n_2s3w.conf"
    [7]="config/cluster_hetero_7n_3s_4w.conf"
    [11]="config/cluster_hetero_11n_4s_7w.conf"
    [15]="config/cluster_hetero_15n_6s_9w.conf"
)

BASE_ENV=(
    "THRESHOLD=2"
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
    "LOG_LEVEL=debug"
    "ENABLE_PRIORITY=false"
    "LATENCY_DEBUG=false"
    "SERVER_BATCHING=false"
)

# Bastion: cora-c32-1 (internal 192.168.73.93 / public 134.87.11.79).
BASTION_PUBLIC_IP="134.87.11.79"
BASTION_INTERNAL_IP="192.168.73.93"

detect_controller_mode() {
    case "$CONTROLLER" in
        laptop|bastion)
            echo "$CONTROLLER"
            ;;
        auto)
            local ips
            ips="$(hostname -I 2>/dev/null || true)"
            if [[ " ${ips} " == *" ${BASTION_INTERNAL_IP} "* ]]; then
                echo "bastion"
            else
                echo "laptop"
            fi
            ;;
        *)
            echo "ERROR: CONTROLLER must be auto, laptop, or bastion (got: $CONTROLLER)" >&2
            exit 1
            ;;
    esac
}

CONTROLLER_MODE="$(detect_controller_mode)"

if [ ! -f "$SSH_KEY" ]; then
    echo "ERROR: SSH key not found: $SSH_KEY" >&2
    echo "Set SSH_KEY=/path/to/key if it is stored elsewhere." >&2
    exit 1
fi

SSH_BASE_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
PROXY_CMD="ssh -i '$SSH_KEY' -o BatchMode=yes -o StrictHostKeyChecking=accept-new -W %h:%p ${SSH_USER}@${BASTION_PUBLIC_IP}"

ssh_opts_for() {
    local host=$1
    SSH_OPTS=("${SSH_BASE_OPTS[@]}")
    SSH_IS_LOCAL=false

    if [ "$CONTROLLER_MODE" = "bastion" ] && [ "$host" = "$BASTION_INTERNAL_IP" ]; then
        SSH_IS_LOCAL=true
        SSH_TARGET="localhost"
    elif [ "$CONTROLLER_MODE" = "bastion" ]; then
        SSH_TARGET="$host"
    elif [ "$host" = "$BASTION_INTERNAL_IP" ]; then
        SSH_TARGET="$BASTION_PUBLIC_IP"
    else
        SSH_OPTS+=(-o "ProxyCommand=$PROXY_CMD")
        SSH_TARGET="$host"
    fi
}


mkdir -p "$RUN_DIR"

go build -o "$BINARY"

remote_exec() {
    local host=$1
    shift
    ssh_opts_for "$host"
    if [ "$SSH_IS_LOCAL" = true ]; then
        bash -s "$@"
    else
        ssh "${SSH_OPTS[@]}" "$SSH_USER@$SSH_TARGET" "$@"
    fi
}

copy_binary() {
    local host=$1
    ssh_opts_for "$host"
    if [ "$SSH_IS_LOCAL" = true ]; then
        cp "$BINARY" "$REMOTE_DIR/"
    else
        scp -q "${SSH_OPTS[@]}" "$BINARY" "$SSH_USER@$SSH_TARGET:$REMOTE_DIR/"
    fi
}

copy_config() {
    local host=$1
    local config_local=$2
    local config_remote="$REMOTE_DIR/$(basename "$config_local")"
    ssh_opts_for "$host"
    if [ "$SSH_IS_LOCAL" = true ]; then
        mkdir -p "$REMOTE_DIR/config"
        cp "$config_local" "$config_remote"
    else
        ssh "${SSH_OPTS[@]}" "$SSH_USER@$SSH_TARGET" "mkdir -p '$REMOTE_DIR/config'"
        scp -q "${SSH_OPTS[@]}" "$config_local" "$SSH_USER@$SSH_TARGET:$config_remote"
    fi
}

read_server_ips() {
    local config_local=$1
    local count=$2
    mapfile -t SERVER_IPS < <(awk 'NF >= 2 { print $2 }' "$config_local" | head -n "$count")
    if [ "${#SERVER_IPS[@]}" -lt "$count" ]; then
        echo "ERROR: ${config_local} does not contain enough server IPs for ${count} nodes"
        exit 1
    fi
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
        ssh_opts_for "$host"
        if [ "$SSH_IS_LOCAL" = true ]; then
            cp -r "${REMOTE_EVAL_DIR}/." "$case_eval_dir/" 2>/dev/null || true
            cp -r "${REMOTE_LOG_DIR}/." "$case_log_dir/" 2>/dev/null || true
        else
            scp -q "${SSH_OPTS[@]}" -r                 "$SSH_USER@$SSH_TARGET:${REMOTE_EVAL_DIR}/." "$case_eval_dir/" 2>/dev/null || true
            scp -q "${SSH_OPTS[@]}" -r                 "$SSH_USER@$SSH_TARGET:${REMOTE_LOG_DIR}/." "$case_log_dir/" 2>/dev/null || true
        fi
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
    if [ -f "$MERGE_SCRIPT" ]; then
        python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" --ids "$client_id_filter"
        python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" --servers --ids "$server_id_filter"
    else
        echo " ✗ merge_eval.py not found at ${MERGE_SCRIPT}"
    fi
}

start_server() {
    local server_id=$1
    local host=$2
    local server_count=$3
    local max_inflight=$4
    local config_remote=$5

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
mkdir -p '$REMOTE_LOG_DIR/server_${server_count}_${server_id}' '$REMOTE_EVAL_DIR'
SERVER_BATCHING=false \
PARALLEL_FAST_PATH=true \
nohup ./$BINARY \
    -id=${server_id} \
    -n=${server_count} \
    -t=2 \
    -path='$config_remote' \
    -pd=true \
    -role=0 \
    -ops=0 \
    -b=1 \
    -indep=90 \
    -common=10 \
    -et=0 \
    -ms=512 \
    -mode=1 \
    -log=debug \
    -ep=false \
    > '$REMOTE_LOG_DIR/server_${server_count}_${server_id}/output.log' 2>&1 &
EOF
}

start_client() {
    local client_id=$1
    local host=$2
    local server_count=$3
    local max_inflight=$4
    local config_remote=$5

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
    mkdir -p '$REMOTE_LOG_DIR/client_${server_count}_${client_id}' '$REMOTE_EVAL_DIR/client${client_id}'
PIPELINE_MODE=true \
MAX_INFLIGHT=${max_inflight} \
nohup ./$BINARY \
    -id=${client_id} \
    -n=${server_count} \
    -t=2 \
    -path='$config_remote' \
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
    -log=debug \
    -ep=false \
    > '$REMOTE_LOG_DIR/client_${server_count}_${client_id}/output.log' 2>&1 &
EOF
}

run_case() {
    local server_count=$1
    local config_local="${CONFIG_BY_COUNT[$server_count]}"
    local config_remote="${REMOTE_DIR}/$(basename "$config_local")"
    local label="raft_hetero_${server_count}"
    local max_inflight=5

    read_server_ips "$config_local" "$server_count"

    echo ""
    echo "=================================================="
    echo "Running: ${label}"
    echo "  Config : ${config_local}"
    echo "  Servers: ${server_count}"
    echo "  Clients : ${#CLIENT_IPS[@]}"
    echo "  MAX_INFLIGHT: ${max_inflight}"
    echo "=================================================="

    for host in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        copy_binary "$host"
        copy_config "$host" "$SCRIPT_DIR/$config_local"
    done

    for server_id in "${!SERVER_IPS[@]}"; do
        start_server "$server_id" "${SERVER_IPS[$server_id]}" "$server_count" "$max_inflight" "$config_remote"
        sleep 1
    done

    sleep 15

    local client_id="$server_count"
    for client_vm_idx in "${!CLIENT_IPS[@]}"; do
        start_client "$client_id" "${CLIENT_IPS[$client_vm_idx]}" "$server_count" "$max_inflight" "$config_remote"
        client_id=$((client_id + 1))
        sleep 1
    done

    sleep "$RUNTIME_SECONDS"

    stop_nodes "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"
    archive_case "$label" "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"
    merge_case_results "$label" "$server_count" "${#CLIENT_IPS[@]}"
}

cleanup() {
    stop_nodes "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}" || true
}

trap cleanup EXIT

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash eval_hetero_cluster_scale_raft.sh

Environment overrides:
  WORKLOAD=a|b|c|d|e|f
  RUNTIME_SECONDS=30

Runs heterogeneous cluster sizes: 3, 5, 7, 11, 15
with 2 fixed clients in raft mode.
EOF
    exit 0
fi

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║             HETEROGENEOUS CLUSTER SCALE SWEEP (RAFT)         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo "Workload: $WORKLOAD"
echo ""

for server_count in "${SERVER_COUNTS[@]}"; do
    run_case "$server_count"
done

echo ""
echo "=================================================="
echo " Raft heterogeneous scale sweep complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
