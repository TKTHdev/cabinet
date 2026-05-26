#!/bin/bash
# ================================================================
# Cabinet Cloud Cluster Stopper - HETEROGENEOUS CLUSTER (FIXED)
# ================================================================

set -euo pipefail

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
BINARY_NAME="cabinet"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_EVAL_DIR="${SCRIPT_DIR}/eval"
MERGED_DIR="${LOCAL_EVAL_DIR}/merged"
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"
REMOTE_EVAL_DIR="/home/ubuntu/cabinet/eval"
SSH_OPTS="-o ConnectTimeout=5 -o StrictHostKeyChecking=no -i ${SSH_KEY}"

# FIX: define NUM_SERVERS here so client IDs are derived correctly.
# If stop script is called standalone, this must match what start used.
NUM_SERVERS="${NUM_SERVERS:-5}"
NUM_CLIENTS="${NUM_CLIENTS:-2}"

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

# FIX: derive client ID range from NUM_SERVERS instead of hardcoding 5
CLIENT_ID_START="${NUM_SERVERS}"
CLIENT_IDS_FILTER="${CLIENT_IDS_FILTER:-${CLIENT_ID_START}-$((CLIENT_ID_START + NUM_CLIENTS - 1))}"

# ---------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------
stop_on_node() {
    local ip=$1
    local role=$2

    echo ""
    echo "→ Stopping ${role} on ${ip}"

    if [ "$role" = "client" ]; then
        ssh ${SSH_OPTS} "$USER@$ip" \
            "pkill -INT -x ${BINARY_NAME} 2>/dev/null || true" || true
        echo "  Waiting up to 35s for client graceful shutdown..."
        local t=0
        while [ $t -lt 35 ]; do
            if ! ssh ${SSH_OPTS} "$USER@$ip" \
                    "pgrep -x ${BINARY_NAME} >/dev/null 2>&1" 2>/dev/null; then
                break
            fi
            sleep 1
            t=$((t + 1))
        done

        if ssh ${SSH_OPTS} "$USER@$ip" \
                "pgrep -x ${BINARY_NAME} >/dev/null 2>&1" 2>/dev/null; then
            echo "  Still running → Sending SIGTERM"
            ssh ${SSH_OPTS} "$USER@$ip" \
                "pkill -TERM -x ${BINARY_NAME} 2>/dev/null || true" || true
            sleep 5
        fi
    else
        ssh ${SSH_OPTS} "$USER@$ip" \
            "pkill -TERM -x ${BINARY_NAME} 2>/dev/null || true" || true
        echo "  Waiting up to 20s for server shutdown..."
        local t=0
        while [ $t -lt 20 ]; do
            if ! ssh ${SSH_OPTS} "$USER@$ip" \
                    "pgrep -x ${BINARY_NAME} >/dev/null 2>&1" 2>/dev/null; then
                break
            fi
            sleep 1
            t=$((t + 1))
        done
    fi

    # Force-kill if still running
    if ssh ${SSH_OPTS} "$USER@$ip" \
            "pgrep -x ${BINARY_NAME} >/dev/null 2>&1" 2>/dev/null; then
        echo "  Still running → SIGKILL"
        ssh ${SSH_OPTS} "$USER@$ip" \
            "pkill -9 -x ${BINARY_NAME} 2>/dev/null || true" || true
        sleep 1
    fi

    if ssh ${SSH_OPTS} "$USER@$ip" \
            "pgrep -x ${BINARY_NAME} >/dev/null 2>&1" 2>/dev/null; then
        echo "  WARNING: ${role} still active on ${ip}"
    else
        echo "   ${role} on ${ip} stopped"
    fi
}

copy_eval_dir() {
    local ip=$1
    local remote_subdir=$2

    if ssh ${SSH_OPTS} "$USER@$ip" \
            "test -d '${REMOTE_EVAL_DIR}/${remote_subdir}'" >/dev/null 2>&1; then
        echo " Collecting ${USER}@${ip}:${REMOTE_EVAL_DIR}/${remote_subdir}"
        scp -q ${SSH_OPTS} -r \
            "$USER@$ip:${REMOTE_EVAL_DIR}/${remote_subdir}" \
            "${LOCAL_EVAL_DIR}/" 2>/dev/null || true
    else
        echo " WARNING: Missing remote dir ${REMOTE_EVAL_DIR}/${remote_subdir} on ${ip}"
    fi
}

# ---------------------------------------------------------------
# STEP 1 — STOP CLIENTS (parallel)
# ---------------------------------------------------------------
echo "=================================================="
echo " Cabinet HETEROGENEOUS Cluster Shutdown"
echo " Clients then Servers"
echo "=================================================="

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 1: Stopping Clients (${#CLIENT_IPS[@]} nodes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for ip in "${CLIENT_IPS[@]}"; do
    stop_on_node "$ip" client &
done
wait

echo ""
echo "Waiting 5 seconds for servers to flush metrics..."
sleep 5

# ---------------------------------------------------------------
# STEP 2 — STOP SERVERS (parallel)
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 2: Stopping Servers (${#SERVER_IPS[@]} nodes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for ip in "${SERVER_IPS[@]}"; do
    stop_on_node "$ip" server &
done
wait

# ---------------------------------------------------------------
# VERIFICATION
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Verification"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
any_left=false
for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
    if ssh ${SSH_OPTS} "$USER@$ip" \
            "pgrep -x ${BINARY_NAME} >/dev/null 2>&1" 2>/dev/null; then
        count=$(ssh ${SSH_OPTS} "$USER@$ip" \
            "pgrep -x ${BINARY_NAME} 2>/dev/null | wc -l" | tr -d ' \n' || echo 0)
        echo " ${ip}: ${count} process(es) STILL running"
        any_left=true
    fi
done

if [ "$any_left" = false ]; then
    echo " All Cabinet processes stopped."
else
    echo " Some processes remain — use manual pkill if needed."
fi

# ---------------------------------------------------------------
# COLLECT + MERGE EVAL CSVs
# ---------------------------------------------------------------
echo ""
echo "=================================================="
echo " Collecting and merging eval CSVs"
echo "=================================================="

mkdir -p "${LOCAL_EVAL_DIR}" "${MERGED_DIR}"
# Clean up previous run's collected dirs so merge isn't polluted
rm -rf "${LOCAL_EVAL_DIR}"/client* "${LOCAL_EVAL_DIR}"/server* 2>/dev/null || true

echo "→ Collecting client CSVs..."
# FIX: use NUM_SERVERS to compute the first client ID, not hardcoded 5
for idx in "${!CLIENT_IPS[@]}"; do
    client_id=$((CLIENT_ID_START + idx))
    copy_eval_dir "${CLIENT_IPS[$idx]}" "client${client_id}"
done

echo ""
echo "Client timeline CSV collection check:"
for idx in "${!CLIENT_IPS[@]}"; do
    client_id=$((CLIENT_ID_START + idx))
    if ls "${LOCAL_EVAL_DIR}/client${client_id}"/tps_timeline_*.csv >/dev/null 2>&1; then
        echo " client${client_id}: timeline CSV found"
    else
        echo " WARNING: client${client_id}: no timeline CSV found"
    fi
done

echo "→ Collecting server CSVs (leader only)..."
copy_eval_dir "${SERVER_IPS[0]}" "server0"

if [ ! -f "${MERGE_SCRIPT}" ]; then
    echo " WARNING: merge_eval.py not found at ${MERGE_SCRIPT} — skipping merge"
    exit 0
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo " WARNING: python3 not available — skipping merge"
    exit 0
fi

echo "→ Running: python3 ${MERGE_SCRIPT} ${LOCAL_EVAL_DIR} ${MERGED_DIR}/ --ids ${CLIENT_IDS_FILTER}"
python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --ids "${CLIENT_IDS_FILTER}"

if [ $? -eq 0 ]; then
    echo " ✓ Merge complete. Output in ${MERGED_DIR}/"
else
    echo " ✗ Merge failed."
fi

echo ""
echo "=================================================="
echo " HETEROGENEOUS CLUSTER SHUTDOWN COMPLETE"
echo "=================================================="