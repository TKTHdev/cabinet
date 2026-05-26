#!/bin/bash

set -e  # Exit on error
set -u  # Exit on undefined variable

# Configuration
declare -i NUM_SERVERS=5
declare -i NUM_CLIENTS=2
declare -i OPS=0            # Operations per client (set to 0 for infinite mode)
declare -i THRESHOLD=1       # Fault tolerance threshold (f)
declare -i EVAL_TYPE=0      # 0=plain msg, 2=mongodb/ycsb
declare -i BATCHSIZE=1   # Batch size (operations per RPC)
declare -i MSG_SIZE=512     # Message size for plain msg
declare -i MODE=0          # 0=localhost, 1=distributed
HOT_RATIO=100               # Hot object percentage (shared between 2 clients)
INDEP_RATIO=0             # Independent object percentage (per-client)
COMMON_RATIO=00           # Common object percentage (shared pool)
BATCH_MODE="single"        # "single" or "round-robin"
CONFIG_PATH="./config/cluster_localhost.conf"
LOG_DIR="./logs"
BINARY="./cabinet"
LOG_LEVEL="debug"          # trace, debug, info, warn, error, fatal, panic (set to debug for latency breakdown)
ENABLE_PRIORITY="true"     # true=cabinet mode, false=raft mode
RATIO_STEP=0.001           # Rate for trying qualified ratio
PIPELINE_MODE="false"       # true=pipelined (high throughput), false=sequential
MAX_INFLIGHT=1           # Max concurrent batches in pipeline mode

# Add batch composition parameter
BATCH_COMPOSITION="object-specific"  # Default value, can be changed to "object-specific"

# Explicit preload controls (aligned to expected object universe)
PRELOAD_ENABLED="true"
PRELOAD_HOT=10
PRELOAD_COMMON=10000
PRELOAD_INDEP=100000
PRELOAD_CLIENT_COUNT=${NUM_CLIENTS}


# Crash test parameters
CRASH_TIME=0   # seconds to wait before crash (set to >0 to enable)
CRASH_MODE=0   # 0=none

# Track PIDs
declare -a SERVER_PIDS=()
declare -a CLIENT_PIDS=()

# Build the binary
echo "============================================"
echo "Building Cabinet Binary..."
echo "============================================"
go build -o "$BINARY"
if [ $? -ne 0 ]; then
    echo " Build failed"
    exit 1
fi
echo " Build complete"
echo ""

# Create log directories
mkdir -p "$LOG_DIR"

# Validate object ratios
TOTAL_RATIO=$((HOT_RATIO + INDEP_RATIO + COMMON_RATIO))
if [ $TOTAL_RATIO -ne 100 ]; then
    echo "  WARNING: Object ratios sum to ${TOTAL_RATIO}% (should be 100%)"
    echo "   Hot: ${HOT_RATIO}% + Indep: ${INDEP_RATIO}% + Common: ${COMMON_RATIO}%"
    echo ""
fi

# Start servers (role=0)
echo "============================================"
echo "Starting Servers (Leader + Followers)"
echo "============================================"
echo "Topology:"
echo "  Leader:      Server 0 (ID=0)"
echo "  Followers:   Server 1-$((NUM_SERVERS-1))"
echo "  Total:       ${NUM_SERVERS} servers"
echo ""

for ((i=0; i<NUM_SERVERS; i++)); do
    mkdir -p "${LOG_DIR}/server${i}"
    
    if [ $i -eq 0 ]; then
        echo " Starting Server ${i} (LEADER)..."
    else
        echo " Starting Server ${i} (FOLLOWER)..."
    fi
    
    "$BINARY" \
        -id=${i} \
        -n=${NUM_SERVERS} \
        -t=${THRESHOLD} \
        -path="${CONFIG_PATH}" \
        -pd=true \
        -role=0 \
        -ops=${OPS} \
        -b=${BATCHSIZE} \
        -hot=${HOT_RATIO} \
        -indep=${INDEP_RATIO} \
        -common=${COMMON_RATIO} \
        -bmode="${BATCH_MODE}" \
        -et=${EVAL_TYPE} \
        -ms=${MSG_SIZE} \
        -mode=${MODE} \
        -log="${LOG_LEVEL}" \
        -ep=${ENABLE_PRIORITY} \
        -rstep=${RATIO_STEP} \
        -preload=${PRELOAD_ENABLED} \
        -preload-hot=${PRELOAD_HOT} \
        -preload-common=${PRELOAD_COMMON} \
        -preload-indep=${PRELOAD_INDEP} \
        -preload-client-start=${NUM_SERVERS} \
        -preload-client-count=${PRELOAD_CLIENT_COUNT} \
        > "${LOG_DIR}/server${i}/output.log" 2>&1 &
    pid=$!
    SERVER_PIDS+=($pid)
    echo "   → PID: ${pid}"
    echo "   → Log: ${LOG_DIR}/server${i}/output.log"
    
    # Longer delay after leader starts
    if [ $i -eq 0 ]; then
        echo "   → Waiting for leader to initialize..."
        sleep 3
    else
        sleep 1
    fi
done

# Wait for servers to establish connections
echo ""
echo "============================================"
echo "Establishing Server Connections..."
echo "============================================"
echo " Leader establishing connections to followers..."
echo " Followers establishing connections to peers..."
echo " This may take up to 25 seconds (includes server preload)..."
sleep 25
echo " Server connections established"
echo ""

# Start clients (role=1)
echo "============================================"
echo "Starting Clients"
echo "============================================"
echo "Clients: ${NUM_CLIENTS}"
echo ""

for ((i=0; i<NUM_CLIENTS; i++)); do
    client_id=$((NUM_SERVERS + i))  # Client IDs start after server IDs
    mkdir -p "${LOG_DIR}/client${client_id}"

    echo " Starting Client ${client_id}..."
    PIPELINE_MODE=${PIPELINE_MODE} MAX_INFLIGHT=${MAX_INFLIGHT} "$BINARY" \
        -id=${client_id} \
        -n=${NUM_SERVERS} \
        -t=${THRESHOLD} \
        -path="${CONFIG_PATH}" \
        -ops=${OPS} \
        -et=${EVAL_TYPE} \
        -pd=true \
        -role=1 \
        -b=${BATCHSIZE} \
        -hot=${HOT_RATIO} \
        -indep=${INDEP_RATIO} \
        -common=${COMMON_RATIO} \
        -bmode="${BATCH_MODE}" \
        -bcomp="${BATCH_COMPOSITION}" \
        -ms=${MSG_SIZE} \
        -mode=${MODE} \
        -log="${LOG_LEVEL}" \
        > "${LOG_DIR}/client${client_id}/output.log" 2>&1 &
    pid=$!
    CLIENT_PIDS+=($pid)
    echo "   → PID: ${pid}"
    echo "   → Log: ${LOG_DIR}/client${client_id}/output.log"
    sleep 0.5
done

echo ""
echo "=============================================="
echo " Cabinet Cluster Started Successfully"
echo "=============================================="
echo ""
echo " Cluster Configuration:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Servers:         ${NUM_SERVERS}"
echo "Clients:         ${NUM_CLIENTS} (IDs: ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo "Threshold:       ${THRESHOLD} (quorum = $((THRESHOLD+1)))"
if [ ${OPS} -le 0 ]; then
    echo "Operations:      infinite (run until stopped)"
else
    echo "Operations:      ${OPS} per client"
fi
echo "Batch size:      ${BATCHSIZE} operations per RPC"
echo "Batch mode:      ${BATCH_MODE}"
echo ""
echo " Object Distribution:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Hot (shared):    ${HOT_RATIO}%  (contention between 2 clients)"
echo "Independent:     ${INDEP_RATIO}%  (per-client, no contention)"
echo "Common (pool):   ${COMMON_RATIO}%  (shared pool, moderate contention)"
echo ""
echo "  System Configuration:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Eval Type:       ${EVAL_TYPE} (0=plain msg, 2=mongodb/ycsb)"
echo "Message Size:    ${MSG_SIZE} bytes"
echo "Mode:            ${MODE} (0=localhost, 1=distributed)"
echo "Algorithm:       Cabinet (weighted consensus)"
echo "Priority Mode:   ${ENABLE_PRIORITY}"
echo "Pipeline Mode:   ${PIPELINE_MODE} (max ${MAX_INFLIGHT} concurrent batches)"
echo "Ratio Step:      ${RATIO_STEP}"
echo "Log Level:       ${LOG_LEVEL}"
echo "Config:          ${CONFIG_PATH}"
echo "=============================================="
echo ""

# Crash test logic
if [ "$CRASH_MODE" -ne 0 ] && [ "$CRASH_TIME" -gt 0 ]; then
    echo "=============================================="
    echo " Crash Test: Will crash in ${CRASH_TIME} seconds (mode=${CRASH_MODE})"
    echo "=============================================="
    sleep "$CRASH_TIME"
    if [ "$CRASH_MODE" -eq 1 ]; then
        echo " Crashing all servers..."
        for pid in "${SERVER_PIDS[@]}"; do
            kill -9 "$pid" 2>/dev/null || true
        done
    elif [ "$CRASH_MODE" -eq 2 ]; then
        echo " Crashing all clients..."
        for pid in "${CLIENT_PIDS[@]}"; do
            kill -9 "$pid" 2>/dev/null || true
        done
    fi
    echo " Crash test complete."
fi
echo " Monitoring Commands:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "# Watch leader logs:"
echo "  tail -f ${LOG_DIR}/server0/output.log"
echo ""
echo "# Watch follower logs:"
echo "  tail -f ${LOG_DIR}/server1/output.log"
echo ""
echo "# Watch client logs:"
echo "  tail -f ${LOG_DIR}/client${NUM_SERVERS}/output.log"
echo ""
echo "# View all logs simultaneously:"
echo "  tail -f ${LOG_DIR}/*/output.log"
echo ""
echo "# Check for errors:"
echo "  grep -i error ${LOG_DIR}/*/output.log"
echo ""
echo "# Check consensus progress (leader):"
echo "  grep -i 'consensus\\|batch\\|weight' ${LOG_DIR}/server0/output.log"
echo ""
echo "# Check performance metrics:"
echo "  ls -lh ./eval/*.csv"
echo ""
echo "=============================================="
echo ""

# Cleanup function for infinite mode
cleanup_infinite() {
    echo ""
    echo "=============================================="
    echo " Interrupt received, stopping all processes..."
    echo "=============================================="
    
    echo "Stopping clients (SIGTERM)..."
    for pid in "${CLIENT_PIDS[@]}"; do
        kill -TERM $pid 2>/dev/null || true
    done

    echo "Waiting up to 20s for clients to exit gracefully..."
    for _ in $(seq 1 20); do
        alive=0
        for pid in "${CLIENT_PIDS[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                alive=$((alive + 1))
            fi
        done
        [ "$alive" -eq 0 ] && break
        sleep 1
    done
    
    echo "Stopping servers (SIGINT/SIGTERM)..."
    for pid in "${SERVER_PIDS[@]}"; do
        kill -INT $pid 2>/dev/null || true
        kill -TERM $pid 2>/dev/null || true
    done

    echo "Waiting up to 15s for servers to flush metrics..."
    for _ in $(seq 1 15); do
        alive=0
        for pid in "${SERVER_PIDS[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                alive=$((alive + 1))
            fi
        done
        [ "$alive" -eq 0 ] && break
        sleep 1
    done
    
    sleep 2
    
    # Force kill any remaining processes
    pkill -f "$BINARY" > /dev/null 2>&1 || true
    
    echo " All processes stopped."
    echo ""
    echo " Check results in:"
    echo "  - Logs:    ${LOG_DIR}/server*/output.log"
    echo "  - Metrics: ./eval/*.csv"
    exit 0
}

# Wait for clients to finish (or run indefinitely)
if [ ${OPS} -le 0 ]; then
    echo " Clients running in infinite mode..."
    echo "   Press Ctrl+C to stop all processes"
    echo ""
    
    # Set trap for user interrupt
    trap cleanup_infinite SIGINT SIGTERM
    
    # Wait for any process to exit
    wait
else
    echo " Waiting for clients to complete ${OPS} operations..."
    echo ""
    
    # Wait for all clients
    for pid in "${CLIENT_PIDS[@]}"; do
        wait $pid 2>/dev/null || {
            echo "  Client PID $pid finished with error (or was killed)"
        }
    done

    echo ""
    echo "=============================================="
    echo " All clients completed!"
    echo "=============================================="
    echo ""
    echo " Results available in:"
    echo "  - Server logs: ${LOG_DIR}/server*/output.log"
    echo "  - Client logs: ${LOG_DIR}/client*/output.log"
    echo "  - Metrics:     ./eval/*.csv"
    echo ""
    echo "Stopping servers after client completion (graceful)..."
    for pid in "${SERVER_PIDS[@]}"; do
        kill -INT $pid 2>/dev/null || true
        kill -TERM $pid 2>/dev/null || true
    done

    for _ in $(seq 1 15); do
        alive=0
        for pid in "${SERVER_PIDS[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                alive=$((alive + 1))
            fi
        done
        [ "$alive" -eq 0 ] && break
        sleep 1
    done

    for pid in "${SERVER_PIDS[@]}"; do
        kill -9 $pid 2>/dev/null || true
    done
fi

echo "Cleaning up any remaining processes..."
pkill -f "$BINARY" > /dev/null 2>&1 || true

echo ""
echo "=============================================="
echo " All processes stopped. Done."
echo "=============================================="
echo ""
echo " View results:"
echo "  cat ./eval/*.csv"
echo ""
