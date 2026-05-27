#!/bin/bash
# ================================================================
# EVAL 4: Network Delay Evaluation
# Tests latency impact with network emulation (netem): 0ms, 5ms, 10ms, 20ms, 50ms, 100ms, 200ms
# Each configuration runs for 30 seconds
# ================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_5n_2s3w.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"
RESULT_ROOT="${SCRIPT_DIR}/results/eval4_network_delay"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
RUNTIME=30  # 30 seconds per test
NUM_SERVERS=5
NUM_CLIENTS=2

# 5-Node Cluster
SERVER_IPS=(
"192.168.73.159"
"192.168.73.84"
"192.168.73.69"
"192.168.73.235"
"192.168.73.194"
)

CLIENT_HOST_IPS=(
"192.168.73.218"
"192.168.73.219"
)

# Network delays to test (in milliseconds)
DELAYS=(0 5 10 20 50 100 200)

WORKLOAD="a"
SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10)

mkdir -p "$RUN_DIR"

echo "=============================================="
echo "EVAL 4: Network Delay Impact"
echo "=============================================="
echo "Test cases: ${#DELAYS[@]}"
echo "Runtime per test: ${RUNTIME}s"
echo ""

remote_exec() {
    local host=$1
    shift
    ssh "${SSH_OPTS[@]}" "$USER@$host" "$*"
}

detect_interface() {
    local host=$1
    remote_exec "$host" "ip route show default 2>/dev/null | awk '{print \$5; exit}'"
}

create_remote_dirs() {
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "mkdir -p '$REMOTE_DIR' '$LOG_DIR' '$EVAL_DIR' '$REMOTE_DIR/mongodb_data'"
    done
}

wait_for_mongo_ready() {
    local host=$1
    local label=$2
    local attempt

    for attempt in $(seq 1 30); do
        if remote_exec "$host" "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1"; then
            return 0
        fi
        sleep 1
    done

    echo "  Warning: MongoDB readiness timed out on $label ($host)"
    return 1
}

start_mongo_cluster() {
    echo "  Creating remote directories..."
    create_remote_dirs

    echo "  Starting MongoDB on all servers..."
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -f mongod 2>/dev/null || true; rm -f '$REMOTE_DIR/mongodb_data/mongod.lock' '$REMOTE_DIR/mongodb_data/WiredTiger.lock' '$LOG_DIR/mongod.log' 2>/dev/null || true; mkdir -p '$REMOTE_DIR/mongodb_data' '$LOG_DIR'; nohup mongod --port 27017 --replSet wocrs --dbpath '$REMOTE_DIR/mongodb_data' --bind_ip 0.0.0.0 --logpath '$LOG_DIR/mongod.log' --logappend > '$LOG_DIR/mongod.out' 2>&1 &"
    done

    for i in "${!SERVER_IPS[@]}"; do
        wait_for_mongo_ready "${SERVER_IPS[$i]}" "server${i}" || true
    done
}

init_replica_set() {
    echo "  Initializing MongoDB replica set..."
    remote_exec "${SERVER_IPS[0]}" "mongosh --eval \"rs.initiate({ _id: 'wocrs', members: [ {_id: 0, host: '${SERVER_IPS[0]}:27017'}, {_id: 1, host: '${SERVER_IPS[1]}:27017'}, {_id: 2, host: '${SERVER_IPS[2]}:27017'}, {_id: 3, host: '${SERVER_IPS[3]}:27017'}, {_id: 4, host: '${SERVER_IPS[4]}:27017'} ] })\" >/dev/null 2>&1 || true"

    for attempt in $(seq 1 30); do
        if remote_exec "${SERVER_IPS[0]}" "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1"; then
            return 0
        fi
        sleep 1
    done

    echo "  Warning: replica set readiness timed out"
    return 1
}

build_and_distribute() {
    echo "  Building WOC binary..."
    go build -o "$BINARY"
    
    echo "  Distributing to all nodes..."
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        scp "${SSH_OPTS[@]}" "$BINARY" "$USER@$ip:$REMOTE_DIR/" 2>/dev/null &
    done
    wait
    echo "  ✓ Distribution complete"
}

archive_case() {
    local label=$1
    shift
    local case_dir="${RUN_DIR}/${label}"
    mkdir -p "$case_dir"

    local idx=0
    local host
    for host in "$@"; do
        local node_dir="${case_dir}/node_${idx}"
        mkdir -p "$node_dir"
        scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" -r \
            "$USER@$host:${EVAL_DIR}/" "$node_dir/" 2>/dev/null || true
        scp -q -o BatchMode=yes -o ConnectTimeout=10 -i "$SSH_KEY" -r \
            "$USER@$host:${LOG_DIR}/" "$node_dir/" 2>/dev/null || true
        idx=$((idx + 1))
    done
}

merge_case_results() {
    local label=$1
    local case_dir="${RUN_DIR}/${label}"
    local case_eval_dir="${case_dir}/eval"
    local case_merged_dir="${case_dir}/merged"
    local client_start_id=$NUM_SERVERS
    local client_end_id=$((NUM_SERVERS + NUM_CLIENTS - 1))
    local client_id_filter="${client_start_id}-${client_end_id}"
    local server_id_filter="0-$((NUM_SERVERS - 1))"

    mkdir -p "$case_eval_dir" "$case_merged_dir"

    for node_dir in "${case_dir}"/node_*; do
        [ -d "$node_dir/eval" ] || continue
        cp -r "$node_dir/eval/"* "$case_eval_dir/" 2>/dev/null || true
    done

    if [ -f "$MERGE_SCRIPT" ]; then
        python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" --ids "$client_id_filter"
        python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" --servers --ids "$server_id_filter"
    else
        echo "  Warning: merge_eval.py not found at $MERGE_SCRIPT"
    fi
}

start_workload_nodes() {
    local delay=$1

    echo "  Starting WOC servers..."
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -f 'woc.*-path' 2>/dev/null || true; nohup '$REMOTE_DIR/$BINARY' -id=$i -path='$CONFIG_PATH' -et=1 -n=$NUM_SERVERS -t=1 -role=0 -mload='$WORKLOAD' > '$LOG_DIR/server_${i}_delay_${delay}ms.log' 2>&1 &"
    done

    echo "  Starting WOC clients..."
    for i in "${!CLIENT_HOST_IPS[@]}"; do
        ip="${CLIENT_HOST_IPS[$i]}"
        client_id=$((NUM_SERVERS + i))
        remote_exec "$ip" "pkill -f 'woc.*-path' 2>/dev/null || true; nohup '$REMOTE_DIR/$BINARY' -id=$client_id -path='$CONFIG_PATH' -et=1 -n=$NUM_SERVERS -t=1 -role=1 -mload='$WORKLOAD' > '$LOG_DIR/client_${i}_delay_${delay}ms.log' 2>&1 &"
    done
}

stop_workload_nodes() {
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -TERM -x woc 2>/dev/null || true"
    done
    sleep 3
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -9 -x woc 2>/dev/null || true"
    done
}

cleanup() {
    stop_workload_nodes || true
    remove_network_delay || true
    for ip in "${SERVER_IPS[@]}"; do
        remote_exec "$ip" "pkill -f mongod 2>/dev/null || true" || true
    done
}

trap cleanup EXIT

apply_network_delay() {
    local delay=$1
    echo "  Applying ${delay}ms latency to all nodes..."
    
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        iface=$(detect_interface "$ip")
        if [ -z "$iface" ]; then
            echo "  Warning: could not detect interface on $ip; skipping netem"
            continue
        fi
        remote_exec "$ip" "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; if [ '$delay' -gt 0 ]; then sudo tc qdisc add dev '$iface' root netem delay ${delay}ms; fi" 2>/dev/null &
    done
    wait
    sleep 1
}

verify_network_delay() {
    local delay=$1
    echo "  Verifying latency on first node..."
    remote_exec "${SERVER_IPS[0]}" "ping -c 1 ${SERVER_IPS[1]} 2>/dev/null || true" 2>/dev/null || true
}

remove_network_delay() {
    echo "  Removing network delays..."
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        iface=$(detect_interface "$ip")
        if [ -z "$iface" ]; then
            continue
        fi
        remote_exec "$ip" "sudo tc qdisc del dev '$iface' root 2>/dev/null || true" 2>/dev/null &
    done
    wait
}

verify_network_interface() {
    echo "  Detected interfaces:"
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        iface=$(detect_interface "$ip")
        if [ -n "$iface" ]; then
            echo "    $ip -> $iface"
        else
            echo "    $ip -> <not found>"
        fi
    done
}

start_cluster() {
    local delay=$1
    local test_num=$2
    local label="delay_${delay}ms"
    
    echo ""
    echo "--- Test $test_num: NETWORK_DELAY=${delay}ms ---"
    
    # Apply network delay
    apply_network_delay "$delay"
    verify_network_delay "$delay"
    
    start_workload_nodes "$delay"
    
    echo "  Cluster started. Running for ${RUNTIME}s..."
    sleep $RUNTIME
    
    # Stop only workload processes between cases; MongoDB and netem stay controlled across the sweep.
    echo "  Stopping workload processes..."
    stop_workload_nodes
    sleep 2

    echo "  Archiving results..."
    archive_case "$label" "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"
    merge_case_results "$label"
}

# Run tests
build_and_distribute

verify_network_interface

start_mongo_cluster
init_replica_set

test_num=1
for delay in "${DELAYS[@]}"; do
    start_cluster "$delay" "$test_num"
    test_num=$((test_num + 1))
done

# Clean up network delays
echo ""
echo "  Cleaning up network delays..."
remove_network_delay

echo ""
echo "=============================================="
echo "✓ EVAL 4 COMPLETE"
echo "=============================================="
echo ""
echo "Results archived in: $RUN_DIR"
echo ""
echo "Merged client/server summaries are under: $RUN_DIR/*/merged/"
