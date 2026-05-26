#!/bin/bash

# run_client.sh - Helper script to run Cabinet clients

# Default parameters
NUM_SERVERS=10
THRESHOLD=1
BATCH_SIZE=10
EVAL_TYPE=0      # 0=PlainMsg, 1=TPCC, 2=MongoDB
MSG_SIZE=512
NUM_OPS=100      # 0 for infinite
BATCH_MODE="single"
CONFIG_PATH="./config/cluster_localhost.conf"

# Parse arguments
CLIENT_ID=$1

if [ -z "$CLIENT_ID" ]; then
    echo "Usage: $0 <client_id> [num_ops] [batch_size] [msg_size]"
    echo "Example: $0 1 100 10 512"
    echo "Example: $0 2 0 10 512  # 0 = infinite ops"
    exit 1
fi

# Optional parameters
if [ ! -z "$2" ]; then
    NUM_OPS=$2
fi

if [ ! -z "$3" ]; then
    BATCH_SIZE=$3
fi

if [ ! -z "$4" ]; then
    MSG_SIZE=$4
fi

echo "Starting Cabinet Client $CLIENT_ID"
echo "  NumServers: $NUM_SERVERS"
echo "  Threshold: $THRESHOLD (quorum = $((THRESHOLD + 1)))"
echo "  BatchSize: $BATCH_SIZE"
echo "  EvalType: $EVAL_TYPE (0=PlainMsg, 1=TPCC, 2=MongoDB)"
echo "  MsgSize: $MSG_SIZE bytes"
echo "  NumOps: $NUM_OPS (0=infinite)"
echo "  BatchMode: $BATCH_MODE"
echo ""

go run . \
    -client \
    -cid $CLIENT_ID \
    -n $NUM_SERVERS \
    -t $THRESHOLD \
    -b $BATCH_SIZE \
    -et $EVAL_TYPE \
    -ms $MSG_SIZE \
    -ops $NUM_OPS \
    -bmode $BATCH_MODE \
    -path $CONFIG_PATH \
    -log info
