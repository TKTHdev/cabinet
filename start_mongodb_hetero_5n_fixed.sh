#!/bin/bash
# ================================================================
# MongoDB Cluster Launcher - HETEROGENEOUS 5-NODE (2 Strong + 3 Weak)
# FIX: EVAL_TYPE=1 (MongoDB), correct flag names (-indep/-common)
# ================================================================

set -e
trap 'echo " Script interrupted. Exiting..."; exit 1' INT

WORKLOAD="${1:-a}"
if [[ ! "$WORKLOAD" =~ ^[a-f]$ ]]; then
    echo "ERROR: workload must be one of: a b c d e f"
    exit 1
fi

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_5n_2s3w.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"
MONGODB_PORT=27017
MONGODB_REPLICA_SET="wocrs"

# WOC PARAMETERS
NUM_SERVERS=5
NUM_CLIENTS=2
MONGO_CLIENT_POOL=16
THRESHOLD=1
OPS=0
EVAL_TYPE=1
BATCHSIZE=1
MSG_SIZE=512
MODE=1
CONFLICT_RATE=0
INDEP_RATIO=90.0
COMMON_RATIO=10.0
PIPELINE_MODE="true"
MAX_INFLIGHT=5
LOG_LEVEL="info"
ENABLE_PRIORITY="true"
SERVER_BATCHING="false"

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

CLIENTS_PER_VM=1

echo "=============================================="
echo "Building WOC binary locally..."
go build -o "$BINARY"
echo "Build complete."

copy_binary() {
    local target_ip=$1
    ssh -i "$SSH_KEY" "$USER@$target_ip" "mkdir -p '$REMOTE_DIR'"
    scp -i "$SSH_KEY" "$BINARY" "$USER@$target_ip:$REMOTE_DIR/$BINARY.tmp"
    ssh -i "$SSH_KEY" "$USER@$target_ip" "mv -f '$REMOTE_DIR/$BINARY.tmp' '$REMOTE_DIR/$BINARY'"
}

copy_config() {
    local target_ip=$1
    ssh -i "$SSH_KEY" "$USER@$target_ip" "mkdir -p '$REMOTE_DIR/config'"
    scp -i "$SSH_KEY" "./config/cluster_hetero_5n_2s3w.conf" "$USER@$target_ip:$REMOTE_DIR/config/"
}

ensure_remote_dirs() {
    local target_ip=$1
    ssh -i "$SSH_KEY" "$USER@$target_ip" "mkdir -p '$REMOTE_DIR' '$REMOTE_DIR/config' '$LOG_DIR' '$EVAL_DIR' '$REMOTE_DIR/mongodb_data'"
}

setup_mongodb() {
    local target_ip=$1
    ssh -i "$SSH_KEY" "$USER@$target_ip" bash -s <<'EOF'
set -e
REMOTE_DIR="/home/ubuntu/woc"
pkill -f mongod 2>/dev/null || true
sleep 1
rm -rf "$REMOTE_DIR/mongodb_data"/* "$REMOTE_DIR/mongodb_data"/.[!.]* 2>/dev/null || true
rm -f "$REMOTE_DIR/mongodb_data/mongod.lock" "$REMOTE_DIR/mongodb_data/WiredTiger.lock" \
      "$REMOTE_DIR/logs/mongod.log" 2>/dev/null || true
mkdir -p "$REMOTE_DIR/mongodb_data" "$REMOTE_DIR/logs"
nohup mongod --port 27017 --replSet wocrs --dbpath "$REMOTE_DIR/mongodb_data" \
    --bind_ip 0.0.0.0 --logpath "$REMOTE_DIR/logs/mongod.log" --logappend \
    > "$REMOTE_DIR/logs/mongod.out" 2>&1 &
sleep 2
echo "MongoDB started"
EOF
}

wait_for_mongo_ready() {
    local target_ip=$1
    local node_label=$2
    for attempt in $(seq 1 30); do
        if ssh -i "$SSH_KEY" "$USER@$target_ip" \
            "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1"; then
            return 0
        fi
        sleep 1
    done
    echo "  Warning: MongoDB readiness timed out on ${node_label} (${target_ip})"
    return 1
}

wait_for_replica_set_ready() {
    for attempt in $(seq 1 60); do
        if ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[0]}" "mongosh --quiet --eval '
try {
    var s = rs.status();
    var ok = s.members.every(function(m){return m.stateStr===\"PRIMARY\"||m.stateStr===\"SECONDARY\";});
    quit(ok ? 0 : 1);
} catch(e) { quit(1); }
' >/dev/null 2>&1"; then
            echo "  Replica set ready"
            return 0
        fi
        sleep 1
    done
    echo "  Warning: replica set never reached PRIMARY/SECONDARY readiness"
    return 1
}

echo ""
echo "=============================================="
echo "Distributing binary/config to all nodes..."
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    ensure_remote_dirs "$ip"
    copy_binary "$ip"
    copy_config "$ip"
done
echo "Distribution complete"

echo ""
echo "=============================================="
echo "Setting up MongoDB on all servers..."
for ip in "${SERVER_IPS[@]}"; do
    setup_mongodb "$ip"
done
for i in "${!SERVER_IPS[@]}"; do
    wait_for_mongo_ready "${SERVER_IPS[$i]}" "server${i}" || true
done

echo ""
echo "=============================================="
echo "Initializing MongoDB Replica Set..."
ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[0]}" bash -s <<EOF
mongosh --port 27017 --eval "
rs.initiate({
    _id: '${MONGODB_REPLICA_SET}',
    members: [
        {_id: 0, host: '${SERVER_IPS[0]}:${MONGODB_PORT}'},
        {_id: 1, host: '${SERVER_IPS[1]}:${MONGODB_PORT}'},
        {_id: 2, host: '${SERVER_IPS[2]}:${MONGODB_PORT}'},
        {_id: 3, host: '${SERVER_IPS[3]}:${MONGODB_PORT}'},
        {_id: 4, host: '${SERVER_IPS[4]}:${MONGODB_PORT}'}
    ]
})" 2>/dev/null || echo "Replica set may already be initialized"
EOF

sleep 5
wait_for_replica_set_ready || true

echo ""
echo "=============================================="
echo "Starting WOC servers (EVAL_TYPE=1 MongoDB)..."
for i in "${!SERVER_IPS[@]}"; do
    ip="${SERVER_IPS[$i]}"
    ssh -i "$SSH_KEY" "$USER@$ip" bash -s <<EOF &
cd $REMOTE_DIR
nohup ./$BINARY \
    -id=$i \
    -path=$CONFIG_PATH \
    -et=$EVAL_TYPE \
    -n=$NUM_SERVERS \
    -t=$THRESHOLD \
    -b=$BATCHSIZE \
    -bcomp=object-specific \
    -mode=$MODE \
    -mcli=$MONGO_CLIENT_POOL \
    -mload=$WORKLOAD \
    -indep=$INDEP_RATIO \
    -common=$COMMON_RATIO \
    -pipeline=$PIPELINE_MODE \
    -maxinflight=$MAX_INFLIGHT \
    -log=$LOG_LEVEL \
    -ep=$ENABLE_PRIORITY \
    -role=0 \
    > $LOG_DIR/server_${i}.log 2>&1 &
EOF
    sleep 0.5
done
wait

echo ""
echo "=============================================="
echo "Starting WOC clients..."
for i in "${!CLIENT_HOST_IPS[@]}"; do
    ip="${CLIENT_HOST_IPS[$i]}"
    for j in $(seq 0 $((CLIENTS_PER_VM - 1))); do
        client_id=$((NUM_SERVERS + i * CLIENTS_PER_VM + j))
        ssh -i "$SSH_KEY" "$USER@$ip" bash -s <<EOF &
cd $REMOTE_DIR
nohup ./$BINARY \
    -id=$client_id \
    -path=$CONFIG_PATH \
    -et=$EVAL_TYPE \
    -n=$NUM_SERVERS \
    -t=$THRESHOLD \
    -b=$BATCHSIZE \
    -bcomp=object-specific \
    -mode=$MODE \
    -mload=$WORKLOAD \
    -indep=$INDEP_RATIO \
    -common=$COMMON_RATIO \
    -pipeline=$PIPELINE_MODE \
    -maxinflight=$MAX_INFLIGHT \
    -log=$LOG_LEVEL \
    -ops=$OPS \
    -conflictrate=$CONFLICT_RATE \
    -role=1 \
    > $LOG_DIR/client_${i}.log 2>&1 &
EOF
        sleep 0.5
    done
done

echo ""
echo "HETEROGENEOUS 5-NODE MONGODB CLUSTER STARTED (EVAL_TYPE=1)"
echo "Stop with: bash ./stop_mongodb_hetero_5n.sh"
