#!/bin/bash
# ================================================================
# Cabinet Cloud Cluster Stopper
# ================================================================
# Stops all Cabinet servers and clients across all VMs
# ================================================================

USER="ubuntu"
REMOTE_DIR="/home/ubuntu/cabinet"
LOG_DIR="~/cabinet/logs"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_EVAL_DIR="${SCRIPT_DIR}/eval"
MERGED_DIR="${LOCAL_EVAL_DIR}/merged"
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"
REMOTE_EVAL_DIR="/home/ubuntu/cabinet/eval"
SSH_OPTS="-o ConnectTimeout=5 -o StrictHostKeyChecking=no"

# Optional: filter merged clients with IDs/ranges (examples: 5,6 or 5-10)
CLIENT_IDS_FILTER="${CLIENT_IDS_FILTER:-}"

NODES=(
"192.168.228.176"
"192.168.228.57"
"192.168.228.200"
"192.168.228.113"
"192.168.228.54"
"192.168.228.207"
"192.168.228.150"
"192.168.228.100"
"192.168.228.55"
"192.168.228.144"
"192.168.228.143"
"192.168.228.118"
"192.168.228.84"
"192.168.228.35"
"192.168.228.210"
)

echo "=================================================="
echo " Stopping Cabinet Cloud Cluster (graceful)"
echo "=================================================="

for ip in "${NODES[@]}"; do
    echo "→ Sending SIGTERM to clients on ${ip}..."
    ssh ${SSH_OPTS} ${USER}@${ip} "pkill -TERM -f 'cabinet.*-role=1' 2>/dev/null || true" || true
done

echo "→ Waiting up to 20s for clients to exit..."
for _ in $(seq 1 20); do
    remaining=0
    for ip in "${NODES[@]}"; do
        count=$(ssh ${SSH_OPTS} ${USER}@${ip} "pgrep -f 'cabinet.*-role=1' | wc -l" 2>/dev/null || echo 0)
        count=$(echo "$count" | tr -d ' ')
        remaining=$((remaining + count))
    done
    [ "$remaining" -eq 0 ] && break
    sleep 1
done

for ip in "${NODES[@]}"; do
    echo "→ Sending SIGINT/SIGTERM to servers on ${ip}..."
    ssh ${SSH_OPTS} ${USER}@${ip} "pkill -INT -f 'cabinet.*-role=0' 2>/dev/null || true; pkill -TERM -f 'cabinet.*-role=0' 2>/dev/null || true" || true
done

echo "→ Waiting up to 20s for servers to flush metrics and exit..."
for _ in $(seq 1 20); do
    remaining=0
    for ip in "${NODES[@]}"; do
        count=$(ssh ${SSH_OPTS} ${USER}@${ip} "pgrep -f 'cabinet.*-role=0' | wc -l" 2>/dev/null || echo 0)
        count=$(echo "$count" | tr -d ' ')
        remaining=$((remaining + count))
    done
    [ "$remaining" -eq 0 ] && break
    sleep 1
done

for ip in "${NODES[@]}"; do
    echo "→ Force killing remaining cabinet processes on ${ip} (if any)..."
    ssh ${SSH_OPTS} ${USER}@${ip} "pkill -9 -f cabinet 2>/dev/null || true; rm -f ${LOG_DIR}/*/pid.txt 2>/dev/null || true" || true
    done

echo ""
echo "=================================================="
echo " All Cabinet Nodes Stopped Successfully"
echo "=================================================="

echo ""
echo "=================================================="
echo " Merging Cabinet Client CSVs"
echo "=================================================="

mkdir -p "${LOCAL_EVAL_DIR}" "${MERGED_DIR}"

echo "→ Collecting remote client CSVs to ${LOCAL_EVAL_DIR}"
for ip in "${NODES[@]}"; do
    remote_dirs=$(ssh ${SSH_OPTS} ${USER}@${ip} "ls -d ${REMOTE_EVAL_DIR}/client* 2>/dev/null" || true)
    if [ -z "${remote_dirs}" ]; then
        continue
    fi
    while IFS= read -r remote_dir; do
        [ -z "${remote_dir}" ] && continue
        scp ${SSH_OPTS} -r "${USER}@${ip}:${remote_dir}" "${LOCAL_EVAL_DIR}/" >/dev/null 2>&1 || true
    done <<< "${remote_dirs}"
done

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

