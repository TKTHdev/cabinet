#!/bin/bash

# Cabinet 5-Replica PlainMsg Test Runner
# Usage: 
#   ./test_5replicas.sh start [batchsize] [msgsize]  - Start 5 replicas
#   ./test_5replicas.sh stop                          - Stop all replicas
#   ./test_5replicas.sh logs                          - Tail all logs
#   ./test_5replicas.sh status                        - Check running processes

set -e

NUM_SERVERS=5
QUORUM=2  # t+1 where t=1
CONFIG_PATH="./config/cluster_localhost.conf"

start_replicas() {
    BATCH_SIZE=${1:-100}
    MSG_SIZE=${2:-512}
    ENABLE_PRIORITY=${3:-true}
    
    echo "=========================================="
    echo "Starting Cabinet 5-Replica PlainMsg Test"
    echo "=========================================="
    echo "  Servers:      $NUM_SERVERS"
    echo "  Quorum:       $QUORUM"
    echo "  Batch Size:   $BATCH_SIZE"
    echo "  Message Size: $MSG_SIZE bytes"
    echo "  Priority:     $ENABLE_PRIORITY"
    echo "=========================================="
    
    # Build if needed
    if [ ! -f "./cabinet" ]; then
        echo "Building cabinet..."
        go build -o cabinet .
        [ $? -ne 0 ] && echo "Build failed!" && exit 1
    fi
    
    # Clean up old processes
    echo "Cleaning up old processes..."
    pkill -f "cabinet -n=$NUM_SERVERS" 2>/dev/null || true
    sleep 1
    
    # Create logs directory
    mkdir -p logs
    
    echo "Starting replicas..."
    
    # Start followers (1-4)
    for i in {1..4}; do
        ./cabinet -n=$NUM_SERVERS -t=1 -b=$BATCH_SIZE -id=$i \
            -path=$CONFIG_PATH -log=info -mode=0 -et=0 \
            -ep=$ENABLE_PRIORITY -ms=$MSG_SIZE -suffix=5rep \
            > logs/server_$i.log 2>&1 &
        echo "  Server $i (PID: $!) started"
    done
    
    sleep 2
    
    # Start leader (0)
    ./cabinet -n=$NUM_SERVERS -t=1 -b=$BATCH_SIZE -id=0 \
        -path=$CONFIG_PATH -log=info -mode=0 -et=0 \
        -ep=$ENABLE_PRIORITY -ms=$MSG_SIZE -suffix=5rep \
        > logs/server_0.log 2>&1 &
    
    LEADER_PID=$!
    echo "  Server 0 (Leader, PID: $LEADER_PID) started"
    echo ""
    echo "All replicas started!"
    echo "Monitor: tail -f logs/server_0.log"
    echo "Stop:    ./test_5replicas.sh stop"
    echo "=========================================="
    
    # Wait for leader
    wait $LEADER_PID
    echo ""
    echo "Test completed! Check logs/server_*.log"
}

stop_replicas() {
    echo "=========================================="
    echo "Stopping Cabinet Replicas"
    echo "=========================================="
    
    pkill -INT -f "cabinet -n=$NUM_SERVERS" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "Sent shutdown signal, waiting..."
        sleep 3
    fi
    
    # Force kill if needed
    REMAINING=$(pgrep -f "cabinet -n=$NUM_SERVERS" | wc -l)
    if [ $REMAINING -gt 0 ]; then
        echo "Force killing remaining processes..."
        pkill -KILL -f "cabinet -n=$NUM_SERVERS" 2>/dev/null
        sleep 1
    fi
    
    REMAINING=$(pgrep -f "cabinet -n=$NUM_SERVERS" | wc -l)
    [ $REMAINING -eq 0 ] && echo "All processes stopped." || echo "Warning: $REMAINING processes remain"
    echo "=========================================="
}

show_logs() {
    if [ ! -d "logs" ]; then
        echo "No logs directory found"
        exit 1
    fi
    
    echo "Tailing all server logs (Ctrl+C to exit)..."
    tail -f logs/server_*.log
}

show_status() {
    echo "=========================================="
    echo "Cabinet Process Status"
    echo "=========================================="
    
    PROCS=$(pgrep -f "cabinet -n=$NUM_SERVERS" | wc -l)
    
    if [ $PROCS -eq 0 ]; then
        echo "No Cabinet processes running"
    else
        echo "Running processes: $PROCS"
        echo ""
        ps aux | grep "cabinet -n=$NUM_SERVERS" | grep -v grep
    fi
    
    echo "=========================================="
}

show_usage() {
    cat << EOF
Cabinet 5-Replica PlainMsg Test Runner

Usage:
  $0 start [batchsize] [msgsize]   Start 5 replicas
  $0 stop                           Stop all replicas
  $0 logs                           Tail all logs
  $0 status                         Check process status

Examples:
  $0 start                  # Default: batch=100, msg=512 bytes
  $0 start 50 1024          # Custom batch size and message size
  $0 stop                   # Stop all processes
  $0 logs                   # Watch logs in real-time

EOF
}

# Main command dispatcher
case "${1:-}" in
    start)
        start_replicas "$2" "$3"
        ;;
    stop)
        stop_replicas
        ;;
    logs)
        show_logs
        ;;
    status)
        show_status
        ;;
    *)
        show_usage
        exit 1
        ;;
esac
