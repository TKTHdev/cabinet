#!/bin/bash
# ================================================================
# MongoDB Cluster Stopper - HETEROGENEOUS 5-NODE (2 Strong + 3 Weak)
# ================================================================

set -euo pipefail

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
REMOTE_EVAL_DIR="${REMOTE_DIR}/eval"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_EVAL_DIR="${SCRIPT_DIR}/eval"
MERGED_DIR="${LOCAL_EVAL_DIR}/merged"
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"

# 5-Node Cluster: 2 Strong (c16) + 3 Weak (c4)
SERVER_IPS=(
"192.168.73.159"  # tani-hetero-c16-1 (strong)
"192.168.73.84"   # tani-hetero-c16-2 (strong)
"192.168.73.69"   # tani-c4-1 (weak)
"192.168.73.235"  # tani-c4-2 (weak)
"192.168.73.194"  # tani-c4-3 (weak)
)

CLIENT_HOST_IPS=(
"192.168.73.218"
"192.168.73.219"
)

SERVER_ID_FILTER="0-$((${#SERVER_IPS[@]} - 1))"
CLIENT_START_ID="${#SERVER_IPS[@]}"
CLIENT_END_ID="$((CLIENT_START_ID + ${#CLIENT_HOST_IPS[@]} - 1))"
CLIENT_ID_FILTER="${CLIENT_START_ID}-${CLIENT_END_ID}"
BINARY_NAME="woc"

is_local_ip() {
    local ip="$1"
    hostname -I 2>/dev/null | tr ' ' '\n' | grep -Fxq "$ip"
}

copy_eval_dir() {
    local ip=$1
    local remote_subdir=$2
    local local_subdir=$3

    if is_local_ip "$ip"; then
        echo " Detected local source for ${local_subdir} (${ip}), skipping SCP refresh"
        mkdir -p "${LOCAL_EVAL_DIR}/${local_subdir}"
        return 0
    fi

    rm -rf "${LOCAL_EVAL_DIR:?}/${local_subdir}"

    echo " Collecting ${USER}@${ip}:${REMOTE_EVAL_DIR}/${remote_subdir} -> ${LOCAL_EVAL_DIR}/"
    scp -q -o ConnectTimeout=10 -o StrictHostKeyChecking=no -i "$SSH_KEY" -r \
        "$USER@$ip:${REMOTE_EVAL_DIR}/${remote_subdir}" "${LOCAL_EVAL_DIR}/" 2>/dev/null || {
            echo " WARNING: Failed to collect ${remote_subdir} from ${ip}"
            return 1
        }

    return 0
}

kill_on_node() {
    local ip=$1
    local type=$2
    local grace_seconds

    if [ "$type" = "Client" ]; then
        grace_seconds=45
    else
        grace_seconds=60
    fi

    echo ""
    echo "→ Stopping ${type} on ${ip}"

    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
        "$USER@$ip" "pkill -TERM -x ${BINARY_NAME} 2>/dev/null; pkill -TERM -x mongod 2>/dev/null" || true

    echo "  Waiting up to ${grace_seconds}s for graceful shutdown..."
    local count=0
    local elapsed=0
    while [ "$elapsed" -lt "$grace_seconds" ]; do
        count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
            "$USER@$ip" "(pgrep -x ${BINARY_NAME} 2>/dev/null; pgrep -x mongod 2>/dev/null) | wc -l" || echo 0)
        count=$(echo "$count" | tr -d ' \n')
        if [ "$count" -eq 0 ]; then
            break
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    if [ "$count" -gt 0 ]; then
        echo "  Still running -> Killing $count process(es) on $ip"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
            "$USER@$ip" "pkill -9 -x ${BINARY_NAME} 2>/dev/null; pkill -9 -x mongod 2>/dev/null" || true
        sleep 1
    fi

    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
        "$USER@$ip" "rm -f '$REMOTE_DIR/mongodb_data/mongod.lock' '$REMOTE_DIR/mongodb_data/WiredTiger.lock' 2>/dev/null || true" || true

    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
        "$USER@$ip" "(pgrep -x ${BINARY_NAME} 2>/dev/null; pgrep -x mongod 2>/dev/null) | wc -l" || echo 0)
    count=$(echo "$count" | tr -d ' \n')

    if [ "$count" -eq 0 ]; then
        echo "   ${type} on ${ip} stopped"
    else
        echo "  WARNING: $count process(es) still active on ${ip}"
    fi
}

echo "=================================================="
echo " WOC MongoDB 5-NODE Cluster Shutdown"
echo " Clients → Servers"
echo "=================================================="

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 1: Stopping Clients (${#CLIENT_HOST_IPS[@]} nodes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for ip in "${CLIENT_HOST_IPS[@]}"; do
    kill_on_node "$ip" "Client" &
done
wait

echo ""
echo "Waiting 5 seconds for servers to flush metrics..."
sleep 5

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 2: Stopping Servers (${#SERVER_IPS[@]} nodes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for ip in "${SERVER_IPS[@]}"; do
    kill_on_node "$ip" "Server" &
done
wait
sleep 2

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Verification (checking if ANY woc processes remain)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
any_left=false
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" \
        "$USER@$ip" "pgrep -x ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)
    count=$(echo "$count" | tr -d ' \n')
    if [ "$count" -gt 0 ]; then
        echo " ${ip}: ${count} processes STILL running"
        any_left=true
    fi
done

if [ "$any_left" = false ]; then
    echo " All WOC processes stopped on all nodes."
else
    echo " Some processes remain. Use manual pkill if needed."
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 3: Collecting client eval directories"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
mkdir -p "${LOCAL_EVAL_DIR}" "${MERGED_DIR}"
for i in "${!CLIENT_HOST_IPS[@]}"; do
    client_id=$((CLIENT_START_ID + i))
    copy_eval_dir "${CLIENT_HOST_IPS[$i]}" "client${client_id}" "client${client_id}" || true
done

echo ""
echo "Client CSV collection check:"
for i in "${!CLIENT_HOST_IPS[@]}"; do
    client_id=$((CLIENT_START_ID + i))
    if ls "${LOCAL_EVAL_DIR}/client${client_id}"/*.csv >/dev/null 2>&1; then
        echo " client${client_id}: CSV found"
    else
        echo " WARNING: client${client_id}: no CSV found"
    fi
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 4: Collecting server eval directories"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for i in "${!SERVER_IPS[@]}"; do
    copy_eval_dir "${SERVER_IPS[$i]}" "server${i}" "server${i}" || true
done

echo ""
echo "Server CSV collection check:"
for i in "${!SERVER_IPS[@]}"; do
    if ls "${LOCAL_EVAL_DIR}/server${i}"/*.csv >/dev/null 2>&1; then
        echo " server${i}: CSV found"
    else
        echo " WARNING: server${i}: no CSV found"
    fi
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 5: Merging client CSVs locally"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ -f "${MERGE_SCRIPT}" ]; then
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --ids "${CLIENT_ID_FILTER}"
    if [ $? -eq 0 ]; then
        echo " ✓ Merged client CSV written to: ${MERGED_DIR}/merged_woc_clients_*.csv"
    else
        echo " ✗ Error merging client CSVs"
    fi
else
    echo " ✗ merge_eval.py not found at ${MERGE_SCRIPT}"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 6: Merging server CSVs locally"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ -f "${MERGE_SCRIPT}" ]; then
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --servers --ids "${SERVER_ID_FILTER}"
    if [ $? -eq 0 ]; then
        echo " ✓ Merged server CSV written to: ${MERGED_DIR}/merged_woc_servers_*.csv"
    else
        echo " ✗ Error merging server CSVs"
    fi
else
    echo " ✗ merge_eval.py not found at ${MERGE_SCRIPT}"
fi

echo ""
echo "✓ Cluster stopped and results collected."
echo ""
echo "To view merged results:"
echo "  ls -1 ${MERGED_DIR}"
