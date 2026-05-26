#!/bin/bash
# ================================================================
# Cabinet Cloud Cluster Launcher - AUTO CLIENT DISTRIBUTION
# ================================================================
set -e
trap 'echo "❌ Script interrupted. Exiting..."; exit 1' INT

# -----------------------------
# EXPERIMENT CONFIG
# -----------------------------
NUM_SERVERS=5
NUM_CLIENTS=2       # try: 1,2,5,10,20,50
BATCHSIZE=1

# -----------------------------
# SSH / PATH CONFIG
# -----------------------------
USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/cabinet"
BINARY="cabinet"
CONFIG_PATH="./config/cluster_localhost.conf"
LOG_DIR="/home/ubuntu/cabinet/logs"
EVAL_DIR="/home/ubuntu/cabinet/eval"

# -----------------------------
# CABINET PARAMS
# -----------------------------
THRESHOLD=1
OPS=0
EVAL_TYPE=0
MSG_SIZE=512
LOG_LEVEL="info"
ENABLE_PRIORITY="false"
RATIO_STEP=0.001
HOT_RATIO=0
INDEP_RATIO=100
COMMON_RATIO=0
BATCH_MODE="single"
BATCH_COMPOSITION="object-specific"
MODE=1

# -----------------------------
# SERVER VMs (5)
# -----------------------------
SERVER_IPS=(
"192.168.228.176"
"192.168.228.57"
"192.168.228.200"
"192.168.228.113"
"192.168.228.54"
)

# ---------------------------------------------------------------
# CLIENT IPs (4 VMs hosting 50 clients total)
# ---------------------------------------------------------------
CLIENT_IPS=(
"192.168.228.207"
"192.168.228.150"
)

# ================================================================
# AUTO CLIENT DISTRIBUTION
# ================================================================
NUM_CLIENT_VMS=${#CLIENT_HOST_IPS[@]}

BASE=$((NUM_CLIENTS / NUM_CLIENT_VMS))
REM=$((NUM_CLIENTS % NUM_CLIENT_VMS))

CLIENTS_PER_VM=()
for ((i=0;i<NUM_CLIENT_VMS;i++)); do
    if [ $i -lt $REM ]; then
        CLIENTS_PER_VM+=($((BASE+1)))
    else
        CLIENTS_PER_VM+=($BASE)
    fi
done

# Trim unused client VMs
USED_VMS=0
for c in "${CLIENTS_PER_VM[@]}"; do
    if [ "$c" -gt 0 ]; then
        USED_VMS=$((USED_VMS + 1))
    fi
done
CLIENT_HOST_IPS=("${CLIENT_HOST_IPS[@]:0:$USED_VMS}")


# ================================================================
# DISPLAY CONFIG
# ================================================================
echo "=============================================="
echo "Cabinet Experiment"
echo "Servers : $NUM_SERVERS"
echo "Clients : $NUM_CLIENTS"
echo "Batch   : $BATCHSIZE"
echo ""
echo "Client distribution:"
for i in "${!CLIENT_HOST_IPS[@]}"; do
    echo "  ${CLIENT_HOST_IPS[$i]} → ${CLIENTS_PER_VM[$i]} clients"
done
echo "=============================================="
echo ""

# ================================================================
# FUNCTIONS
# ================================================================
copy_binary() {
    scp -i $SSH_KEY "$BINARY" $USER@$1:$REMOTE_DIR/ >/dev/null
}

start_server() {
    ssh -i $SSH_KEY $USER@$2 "
        cd $REMOTE_DIR
        mkdir -p $LOG_DIR/server$1
        nohup ./$BINARY \
            -id=$1 -n=$NUM_SERVERS -t=$THRESHOLD \
            -path=$CONFIG_PATH -pd=true -role=0 \
            -ops=$OPS -b=$BATCHSIZE \
            -hot=$HOT_RATIO -indep=$INDEP_RATIO -common=$COMMON_RATIO \
            -bmode=$BATCH_MODE -bcomp=$BATCH_COMPOSITION \
            -et=$EVAL_TYPE -ms=$MSG_SIZE -mode=$MODE \
            -log=$LOG_LEVEL -ep=$ENABLE_PRIORITY -rstep=$RATIO_STEP \
            > $LOG_DIR/server$1/output.log 2>&1 &
    "
}

start_client() {
    ssh -i $SSH_KEY $USER@$2 "
        cd $REMOTE_DIR
        mkdir -p $LOG_DIR/client$1
        nohup ./$BINARY \
            -id=$1 -n=$NUM_SERVERS -t=$THRESHOLD \
            -path=$CONFIG_PATH -pd=true -role=1 \
            -ops=$OPS -b=$BATCHSIZE \
            -hot=$HOT_RATIO -indep=$INDEP_RATIO -common=$COMMON_RATIO \
            -bmode=$BATCH_MODE -bcomp=$BATCH_COMPOSITION \
            -et=$EVAL_TYPE -ms=$MSG_SIZE -mode=$MODE \
            -log=$LOG_LEVEL -ep=$ENABLE_PRIORITY -rstep=$RATIO_STEP \
            > $LOG_DIR/client$1/output.log 2>&1 &
    "
}

# ================================================================
# BUILD + DEPLOY
# ================================================================
echo "🔨 Building cabinet..."
cd $REMOTE_DIR
go build -o $BINARY

echo "📦 Copying binary..."
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    copy_binary $ip &
done
wait

# ================================================================
# START SERVERS
# ================================================================
echo "🚀 Starting servers..."
for i in "${!SERVER_IPS[@]}"; do
    start_server $i "${SERVER_IPS[$i]}"
    [ $i -eq 0 ] && sleep 3 || sleep 1
done

sleep 15

# ================================================================
# START CLIENTS
# ================================================================
cid=$NUM_SERVERS
for i in "${!CLIENT_HOST_IPS[@]}"; do
    for ((c=0;c<${CLIENTS_PER_VM[$i]};c++)); do
        start_client $cid "${CLIENT_HOST_IPS[$i]}"
        ((cid++))
        sleep 0.3
    done
done

echo "✅ Cluster started successfully"

