#!/bin/bash
# ================================================================
# EVAL 3: Fault Tolerance Evaluation
# Tests failure scenarios: No failures, 1 server fails, 2 servers fail
# Each configuration runs for 30 seconds
# ================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/cabinet"
BINARY="cabinet"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_5n_2s3w.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"
RESULT_ROOT="${SCRIPT_DIR}/results/eval3_fault_tolerance"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
RUNTIME=30  # 30 seconds per test
NUM_SERVERS=5
NUM_CLIENTS=2
THRESHOLD=1
BATCHSIZE=1
PIPELINE_MODE=true
MAX_INFLIGHT=5
MONGO_CLIENT_POOL=16
LOG_LEVEL="info"

# 5-Node Cluster
SERVER_IPS=(
"192.168.73.159"  # Index 0
"192.168.73.84"   # Index 1
"192.168.73.69"   # Index 2
"192.168.73.235"  # Index 3
"192.168.73.194"  # Index 4
)

CLIENT_HOST_IPS=(
"192.168.73.218"
"192.168.73.219"
)

WORKLOAD="a"
SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10)

# Test scenarios in a fixed order.
SCENARIO_NAMES=(
    "no_failure"
    "node0_fails"
    "node1_fails"
    "node4_fails"
    "node0_node1_fail"
)

SCENARIO_FAILS=(
    ""
    "0"
    "1"
    "4"
    "0,1"
)

mkdir -p "$RUN_DIR"

echo "=============================================="
echo "EVAL 3: Fault Tolerance"
echo "=============================================="
echo "Test cases: ${#SCENARIO_NAMES[@]}"
echo "Runtime per test: ${RUNTIME}s"
echo ""

remote_exec() {
    local host=$1
    shift
    ssh "${SSH_OPTS[@]}" "$USER@$host" "$*"
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
    remote_exec "${SERVER_IPS[0]}" "mongosh --eval \"rs.initiate({ _id: 'wocrs', members: [ {_id: 0, host: '${SERVER_IPS[0]}:27017', priority: 1}, {_id: 1, host: '${SERVER_IPS[1]}:27017', priority: 1}, {_id: 2, host: '${SERVER_IPS[2]}:27017', priority: 0}, {_id: 3, host: '${SERVER_IPS[3]}:27017', priority: 0}, {_id: 4, host: '${SERVER_IPS[4]}:27017', priority: 0} ] })\" >/dev/null 2>&1 || true"

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
    local scenario=$1

    echo "  Starting WOC servers..."
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -f 'cabinet.*-path' 2>/dev/null || true; nohup '$REMOTE_DIR/$BINARY' -id=$i -path='$CONFIG_PATH' -et=1 -n=$NUM_SERVERS -t=$THRESHOLD -b=$BATCHSIZE -mode=1 -mcli=$MONGO_CLIENT_POOL -mload='$WORKLOAD' -bcomp=object-specific -indep=90 -common=10 -max-inflight=$MAX_INFLIGHT -log=$LOG_LEVEL -ep=true -role=0 > '$LOG_DIR/server_${i}_${scenario}.log' 2>&1 &"
    done

    echo "  Starting WOC clients..."
    for i in "${!CLIENT_HOST_IPS[@]}"; do
        ip="${CLIENT_HOST_IPS[$i]}"
        client_id=$((NUM_SERVERS + i))
        remote_exec "$ip" "pkill -f 'cabinet.*-path' 2>/dev/null || true; nohup '$REMOTE_DIR/$BINARY' -id=$client_id -path='$CONFIG_PATH' -et=1 -n=$NUM_SERVERS -t=$THRESHOLD -b=$BATCHSIZE -mode=1 -mload='$WORKLOAD' -bcomp=object-specific -indep=90 -common=10 -max-inflight=$MAX_INFLIGHT -log=$LOG_LEVEL -ops=0 -role=1 > '$LOG_DIR/client_${i}_${scenario}.log' 2>&1 &"
    done
}

stop_workload_nodes() {
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -TERM -x cabinet 2>/dev/null || true"
    done
    sleep 3
    for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
        remote_exec "$ip" "pkill -9 -x cabinet 2>/dev/null || true"
    done
}

stop_mongo_cluster() {
    for ip in "${SERVER_IPS[@]}"; do
        remote_exec "$ip" "pkill -f mongod 2>/dev/null || true"
    done
}

cleanup() {
    stop_workload_nodes || true
    stop_mongo_cluster || true
}

trap cleanup EXIT

start_cluster() {
    local scenario=$1
    local failed_nodes=$2

    echo ""
    echo "--- Test: $scenario (Failed nodes: ${failed_nodes:-'None'}) ---"

    start_mongo_cluster
    init_replica_set
    start_workload_nodes "$scenario"

    echo "  Cluster started."
    sleep 10  # Let it stabilize

    if [ -n "$failed_nodes" ]; then
        echo "  Injecting failures on nodes: $failed_nodes"
        IFS=',' read -ra nodes <<< "$failed_nodes"
        for node_id in "${nodes[@]}"; do
            ip="${SERVER_IPS[$node_id]}"
            remote_exec "$ip" "pkill -TERM -x cabinet 2>/dev/null || true; pkill -f mongod 2>/dev/null || true" &
        done
        wait
        sleep 2
    fi

    echo "  Running for ${RUNTIME}s..."
    sleep $RUNTIME

    echo "  Stopping workload processes..."
    stop_workload_nodes
    sleep 2
    stop_mongo_cluster

    echo "  Archiving results..."
    archive_case "$scenario" "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"
    merge_case_results "$scenario"
}

# Run tests
build_and_distribute

for idx in "${!SCENARIO_NAMES[@]}"; do
    scenario="${SCENARIO_NAMES[$idx]}"
    failed_nodes="${SCENARIO_FAILS[$idx]}"
    start_cluster "$scenario" "$failed_nodes"
done

echo ""
echo "=============================================="
echo "✓ EVAL 3 COMPLETE"
echo "=============================================="
echo ""
echo "Results archived in: $RUN_DIR"
echo ""
echo "Merged client/server summaries are under: $RUN_DIR/*/merged/"
