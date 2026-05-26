#!/bin/bash
# ================================================================
# Cabinet Cloud Cluster Launcher - 20 CLIENT VERSION
# ================================================================

set -e
trap 'echo " Script interrupted. Exiting..."; exit 1' INT

# -----------------------------
# CONFIGURATION
# -----------------------------
USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/cabinet"
BINARY="cabinet"
CONFIG_PATH="./config/cluster_homo.conf"
LOG_DIR="/home/ubuntu/cabinet/logs"
EVAL_DIR="/home/ubuntu/cabinet/eval"

# -----------------------------
# CABINET PARAMETERS
# -----------------------------
NUM_SERVERS=5
NUM_CLIENTS=2
THRESHOLD=1
OPS=0
EVAL_TYPE=0
BATCHSIZE=1               # ✅ CHANGED: 1 → 10 (better for cloud)
MSG_SIZE=512
LOG_LEVEL="info"           # ✅ CHANGED: "debug" → "info" (production)
ENABLE_PRIORITY="true"
RATIO_STEP=0.001

# Object ratios
HOT_RATIO=00
INDEP_RATIO=90
COMMON_RATIO=10
BATCH_MODE="single"
BATCH_COMPOSITION="object-specific"
MODE=1

# -----------------------------
# IP LIST
# -----------------------------
SERVER_IPS=(
"192.168.73.220" "192.168.73.240" "192.168.73.108" "192.168.73.179" "192.168.73.154" 
)

# ✅ NEW: 10 VMs for clients (2 clients per VM = 20 total)
CLIENT_HOST_IPS=(
"192.168.73.45" "192.168.73.229"
)

CLIENTS_PER_VM=1
# -----------------------------
# COPY BINARY TO REMOTE VM
# -----------------------------
copy_binary() {
    local SERVER_IP=$1
    echo " Copying binary to $SERVER_IP ..."
    scp -i $SSH_KEY "$BINARY" $USER@$SERVER_IP:$REMOTE_DIR/
}

# -----------------------------
# START SERVER FUNCTION (UNCHANGED)
# -----------------------------
start_server() {
    local SERVER_ID=$1
    local SERVER_IP=$2
    echo " Starting Server $SERVER_ID on $SERVER_IP ..."

    ssh -i $SSH_KEY $USER@$SERVER_IP "
        cd $REMOTE_DIR
        mkdir -p ${LOG_DIR}/server${SERVER_ID} ${EVAL_DIR}
        nohup ./$BINARY \
            -id=${SERVER_ID} \
            -n=${NUM_SERVERS} \
            -t=${THRESHOLD} \
            -path=${CONFIG_PATH} \
            -pd=true \
            -role=0 \
            -ops=${OPS} \
            -b=${BATCHSIZE} \
            -hot=${HOT_RATIO} \
            -indep=${INDEP_RATIO} \
            -common=${COMMON_RATIO} \
            -bmode=${BATCH_MODE} \
            -bcomp=${BATCH_COMPOSITION} \
            -et=${EVAL_TYPE} \
            -ms=${MSG_SIZE} \
            -mode=${MODE} \
            -log=${LOG_LEVEL} \
            -ep=${ENABLE_PRIORITY} \
            -rstep=${RATIO_STEP} \
            > ${LOG_DIR}/server${SERVER_ID}/output.log 2>&1 &
    "
}

# -----------------------------
# START CLIENT FUNCTION (UNCHANGED)
# -----------------------------
start_client() {
    local CLIENT_ID=$1
    local CLIENT_IP=$2
    echo " Starting Client $CLIENT_ID on $CLIENT_IP ..."

    ssh -i $SSH_KEY $USER@$CLIENT_IP "
        cd $REMOTE_DIR
        mkdir -p ${LOG_DIR}/client${CLIENT_ID} ${EVAL_DIR}/client${CLIENT_ID}
        nohup ./$BINARY \
            -id=${CLIENT_ID} \
            -n=${NUM_SERVERS} \
            -t=${THRESHOLD} \
            -path=${CONFIG_PATH} \
            -ops=${OPS} \
            -et=${EVAL_TYPE} \
            -pd=true \
            -role=1 \
            -b=${BATCHSIZE} \
            -hot=${HOT_RATIO} \
            -indep=${INDEP_RATIO} \
            -common=${COMMON_RATIO} \
            -bmode=${BATCH_MODE} \
            -bcomp=${BATCH_COMPOSITION} \
            -ms=${MSG_SIZE} \
            -mode=${MODE} \
            -log=${LOG_LEVEL} \
            -ep=${ENABLE_PRIORITY} \
            -rstep=${RATIO_STEP} \
            > ${LOG_DIR}/client${CLIENT_ID}/output.log 2>&1 &
    "
}

# -----------------------------
# BUILD CABINET BINARY LOCALLY
# -----------------------------
echo "=============================================="
echo "Building Cabinet binary..."
echo "=============================================="
cd ${REMOTE_DIR}
go build -o "$BINARY"
echo " Build complete."

# -----------------------------
# COPY BINARY TO ALL VMS
# -----------------------------
echo "=============================================="
echo "Copying binary to all remote servers and clients..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    copy_binary $ip
done

# -----------------------------
# START SERVERS (UNCHANGED)
# -----------------------------
echo "=============================================="
echo "Starting all servers..."
echo "=============================================="
for i in "${!SERVER_IPS[@]}"; do
    start_server $i "${SERVER_IPS[$i]}"
    if [ $i -eq 0 ]; then
        echo " Waiting 3 seconds for leader to initialize..."
        sleep 3
    else
        sleep 1
    fi
done

echo "Waiting 15 seconds for servers to stabilize..."
sleep 15

# -----------------------------
# START CLIENTS (MODIFIED FOR 20 CLIENTS)
# -----------------------------
echo "=============================================="
echo "Starting ${NUM_CLIENTS} clients (${CLIENTS_PER_VM} per VM)..."
echo "=============================================="

client_id=${NUM_SERVERS}  # ✅ Start from ID 5

# ✅ NEW: Loop over VMs and start 2 clients per VM
for vm_ip in "${CLIENT_HOST_IPS[@]}"; do
    for ((c=0; c<CLIENTS_PER_VM; c++)); do
        if [ $client_id -lt $((NUM_SERVERS + NUM_CLIENTS)) ]; then
            start_client $client_id "$vm_ip"
            ((client_id++))
            sleep 1
        fi
    done
done

echo "=============================================="
echo " Cabinet cluster startup commands sent successfully!"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Servers: ${NUM_SERVERS} (IDs 0-$((NUM_SERVERS-1)))"
echo "  Clients: ${NUM_CLIENTS} (IDs ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo "  Clients per VM: ${CLIENTS_PER_VM}"
echo ""
echo "Monitor logs via SSH on each VM:"
echo "  ssh -i $SSH_KEY ubuntu@${SERVER_IPS[0]} 'tail -f ${LOG_DIR}/server0/output.log'"
echo "  ssh -i $SSH_KEY ubuntu@${CLIENT_HOST_IPS[0]} 'tail -f ${LOG_DIR}/client5/output.log'"
echo ""
echo "Check status:"
echo "  ./check_cabinet_cluster.sh"
echo ""
echo "To stop all processes:"
echo "  ./stop_cabinet_cluster.sh"
echo "=============================================="
