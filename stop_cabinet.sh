#!/bin/bash
# ================================================================
# Cabinet Cloud Cluster Stopper (Graceful Shutdown)
# ================================================================

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_EVAL_DIR="${SCRIPT_DIR}/eval"
MERGED_DIR="${LOCAL_EVAL_DIR}/merged"
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"
REMOTE_EVAL_DIR="/home/ubuntu/cabinet/eval"
SSH_OPTS="-o ConnectTimeout=5 -o StrictHostKeyChecking=no -i ${SSH_KEY}"

# Optional: filter merged clients with IDs/ranges (examples: 11,12 or 11-20)
CLIENT_IDS_FILTER="${CLIENT_IDS_FILTER:-}"

# ---------------------------------------------------------------
# SERVER IPs (11 servers)
# ---------------------------------------------------------------
SERVER_IPS=(
"192.168.73.220"
"192.168.73.240"
"192.168.73.108"
"192.168.73.179"
"192.168.73.154" 
)


# ---------------------------------------------------------------
# CLIENT IPs (4 VMs hosting 50 clients total)
# ---------------------------------------------------------------
CLIENT_IPS=(
"192.168.73.45"
"192.168.73.229"
)

echo "=================================================="
echo " Cabinet Cluster Shutdown (Clients → Servers)"
echo "=================================================="

# ---------------------------------------------------------------
# FUNCTION: Kill processes on a node
# ---------------------------------------------------------------
kill_on_node() {
    local ip=$1
    local type=$2   # "Client" or "Server"

    echo ""
    echo "→ Stopping ${type} on ${ip}"

    # Clients trap INT/TERM for metrics flush; send INT first for graceful stop.
    if [ "$type" = "Client" ]; then
        ssh ${SSH_OPTS} \
            $USER@$ip "pkill -INT -x cabinet 2>/dev/null" || true
    else
        ssh ${SSH_OPTS} \
            $USER@$ip "pkill -TERM -x cabinet 2>/dev/null" || true
    fi

    # Wait longer for clients to finish in-flight ops and save metrics.
    if [ "$type" = "Client" ]; then
        echo "  Waiting 35s for client graceful shutdown..."
        sleep 35

        # If still running, try TERM once before force kill.
        local count_after_int
        count_after_int=$(ssh ${SSH_OPTS} \
            $USER@$ip "pgrep -x cabinet 2>/dev/null | wc -l" 2>/dev/null | tr -d ' \n' || echo 0)
        if [ "$count_after_int" -gt 0 ]; then
            echo "  Still running after SIGINT → Sending SIGTERM"
            ssh ${SSH_OPTS} \
                $USER@$ip "pkill -TERM -x cabinet 2>/dev/null" || true
            sleep 10
        fi
    else
        sleep 2
    fi

    # Check if still running
    local count
    count=$(ssh ${SSH_OPTS} \
        $USER@$ip "pgrep -x cabinet 2>/dev/null | wc -l" 2>/dev/null | tr -d ' \n' || echo 0)

    if [ "$count" -gt 0 ]; then
        echo "  Still running → Killing $count processes on $ip"
        ssh ${SSH_OPTS} \
            $USER@$ip "pkill -9 -x cabinet 2>/dev/null" || true
        sleep 1
    fi
    
    # Final check
    count=$(ssh ${SSH_OPTS} \
        $USER@$ip "pgrep -x cabinet 2>/dev/null | wc -l" 2>/dev/null | tr -d ' \n' || echo 0)

    if [ "$count" -eq 0 ]; then
        echo "   ${type} on ${ip} stopped"
    else
        echo "  WARNING: $count process(es) still active on ${ip}"
    fi
}

# Pull remote client eval directories (client*/...) to local eval folder.
collect_remote_client_evals() {
    mkdir -p "${LOCAL_EVAL_DIR}"
    local copied=0

    for ip in "${CLIENT_IPS[@]}"; do
        echo "→ Collecting client eval CSVs from ${ip}"

        # Diagnostic: show remote eval directory status.
        remote_status=$(ssh ${SSH_OPTS} ${USER}@${ip} "if [ -d ${REMOTE_EVAL_DIR} ]; then echo exists; else echo missing; fi" 2>/dev/null || echo missing)
        if [ "${remote_status}" != "exists" ]; then
            echo "  WARNING: ${REMOTE_EVAL_DIR} missing on ${ip}"
            continue
        fi

        remote_dirs=$(ssh ${SSH_OPTS} ${USER}@${ip} "ls -d ${REMOTE_EVAL_DIR}/client* 2>/dev/null" || true)
        if [ -z "${remote_dirs}" ]; then
            echo "  No remote client directories on ${ip}"
            continue
        fi

        while IFS= read -r remote_dir; do
            [ -z "${remote_dir}" ] && continue
            if scp ${SSH_OPTS} -r "${USER}@${ip}:${remote_dir}" "${LOCAL_EVAL_DIR}/" >/dev/null 2>&1; then
                copied=$((copied + 1))
            else
                echo "  WARNING: failed to copy ${remote_dir} from ${ip}"
            fi
        done <<< "${remote_dirs}"
    done

    echo "→ Remote client directories copied: ${copied}"
}

# ---------------------------------------------------------------
# STEP 1 — STOP CLIENTS (IN PARALLEL)
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 1: Stopping Clients (${#CLIENT_IPS[@]} nodes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for ip in "${CLIENT_IPS[@]}"; do
    kill_on_node "$ip" "Client" &
done

# Wait for all parallel client shutdowns to complete
wait

echo ""
echo "Waiting 5 seconds for servers to flush metrics..."
sleep 5

# ---------------------------------------------------------------
# STEP 2 — STOP SERVERS
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 2: Stopping Servers (${#SERVER_IPS[@]} nodes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for ip in "${SERVER_IPS[@]}"; do
    kill_on_node "$ip" "Server"
done

sleep 2

# ---------------------------------------------------------------
# FINAL VERIFICATION
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Verification (checking if ANY cabinet processes remain)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

any_left=false

for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
    count=$(ssh ${SSH_OPTS} \
        $USER@$ip "pgrep -x cabinet 2>/dev/null | wc -l" 2>/dev/null | tr -d ' \n' || echo 0)
    if [ "$count" -gt 0 ]; then
        echo " ${ip}: ${count} processes STILL running"
        any_left=true
    fi
done

if [ "$any_left" = false ]; then
    echo " All Cabinet processes stopped on all nodes."
else
    echo " Some processes remain. Use manual pkill if needed."
fi

echo ""
echo "=================================================="
echo " CLUSTER SHUTDOWN COMPLETE"
echo "=================================================="

echo ""
echo "=================================================="
echo " MERGING CABINET CLIENT CSVS"
echo "=================================================="

mkdir -p "${LOCAL_EVAL_DIR}" "${MERGED_DIR}"

echo "→ Collecting remote client CSVs to ${LOCAL_EVAL_DIR}"
collect_remote_client_evals

local_client_dirs=$(ls -d "${LOCAL_EVAL_DIR}"/client* 2>/dev/null | wc -l | tr -d ' \n')
if [ "${local_client_dirs}" -eq 0 ]; then
    echo " WARNING: No local client directories found in ${LOCAL_EVAL_DIR} after collection"
    echo "          Merge will fail unless clients saved metrics on remote nodes"
fi

if [ ! -f "${MERGE_SCRIPT}" ]; then
    echo " WARNING: merge_eval.py not found at ${MERGE_SCRIPT}; skipping merge"
    exit 0
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo " WARNING: python3 is not available; skipping merge"
    exit 0
fi

if [ -n "${CLIENT_IDS_FILTER}" ]; then
    echo "→ Running: python3 ${MERGE_SCRIPT} ${LOCAL_EVAL_DIR} ${MERGED_DIR}/ --ids ${CLIENT_IDS_FILTER}"
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --ids "${CLIENT_IDS_FILTER}"
else
    echo "→ Running: python3 ${MERGE_SCRIPT} ${LOCAL_EVAL_DIR} ${MERGED_DIR}/"
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/"
fi

if [ $? -eq 0 ]; then
    echo " ✓ Merge completed. Output in ${MERGED_DIR}/"
else
    echo " ✗ Merge failed. Check logs above."
fi
