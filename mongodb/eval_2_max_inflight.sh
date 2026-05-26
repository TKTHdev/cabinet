#!/bin/bash
# ================================================================
# EVAL 2: Max Pipeline In-Flight Evaluation
# Tests pipeline depths: 1,2,3,4,5,8,10,15,20,25,30,35,40,45,50
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
RESULT_ROOT="${SCRIPT_DIR}/results/eval2_max_inflight"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
RUNTIME=30  # 30 seconds per test
NUM_SERVERS=5
NUM_CLIENTS=2
THRESHOLD=1
BATCHSIZE=1
PIPELINE_MODE=true
MONGO_CLIENT_POOL=16
LOG_LEVEL="info"

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

WORKLOAD="a"
INDEP_RATIO=100
COMMON_RATIO=0
SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10)

mkdir -p "$RUN_DIR"

# Max inflight values to test
MAX_INFLIGHT_VALUES=(1 2 3 4 5 8 10 15 20 25 30 35 40 45 50)

echo "=============================================="
echo "EVAL 2: Max Pipeline In-Flight"
echo "=============================================="
echo "Test cases: ${#MAX_INFLIGHT_VALUES[@]}"
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
    local max_inflight=$1

    echo "  Starting WOC servers..."
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "pkill -f 'cabinet.*-path' 2>/dev/null || true; nohup '$REMOTE_DIR/$BINARY' -id=$i -path='$CONFIG_PATH' -et=1 -n=$NUM_SERVERS -t=$THRESHOLD -b=$BATCHSIZE -mode=1 -mcli=$MONGO_CLIENT_POOL -mload='$WORKLOAD' -bcomp=object-specific -indep=$INDEP_RATIO -common=$COMMON_RATIO -log=$LOG_LEVEL -ep=true -role=0 > '$LOG_DIR/server_${i}_inflight_${max_inflight}.log' 2>&1 &"
    done

    echo "  Starting WOC clients..."
    for i in "${!CLIENT_HOST_IPS[@]}"; do
        ip="${CLIENT_HOST_IPS[$i]}"
        client_id=$((NUM_SERVERS + i))
        remote_exec "$ip" "pkill -f 'cabinet.*-path' 2>/dev/null || true; MAX_INFLIGHT=$max_inflight nohup '$REMOTE_DIR/$BINARY' -id=$client_id -path='$CONFIG_PATH' -et=1 -n=$NUM_SERVERS -t=$THRESHOLD -b=$BATCHSIZE -mode=1 -mload='$WORKLOAD' -bcomp=object-specific -indep=$INDEP_RATIO -common=$COMMON_RATIO -max-inflight=$max_inflight -log=$LOG_LEVEL -ops=0 -role=1 > '$LOG_DIR/client_${i}_inflight_${max_inflight}.log' 2>&1 &"
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

cleanup() {
    stop_workload_nodes || true
    for ip in "${SERVER_IPS[@]}"; do
        remote_exec "$ip" "pkill -f mongod 2>/dev/null || true" || true
    done
}

trap cleanup EXIT

start_cluster() {
    local max_inflight=$1
    local test_num=$2
    local label="inflight_${max_inflight}"
    
    echo ""
    echo "--- Test $test_num: MAX_INFLIGHT=$max_inflight ---"
    
    start_workload_nodes "$max_inflight"
    
    echo "  Cluster started. Running for ${RUNTIME}s..."
    sleep $RUNTIME
    
    # Stop only workload processes between cases; MongoDB stays up for the full sweep.
    echo "  Stopping workload processes..."
    stop_workload_nodes
    sleep 2

    echo "  Archiving results..."
    archive_case "$label" "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"
    merge_case_results "$label"
}

# Run tests
build_and_distribute

start_mongo_cluster
init_replica_set

test_num=1
for max_inflight in "${MAX_INFLIGHT_VALUES[@]}"; do
    start_cluster "$max_inflight" "$test_num"
    test_num=$((test_num + 1))
done

echo ""
echo "=============================================="
echo "✓ EVAL 2 COMPLETE"
echo "=============================================="
echo ""
echo "Results archived in: $RUN_DIR"
echo ""
echo "Merged client/server summaries are under: $RUN_DIR/*/merged/"
