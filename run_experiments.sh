#!/bin/bash

# ============================================
# Cabinet: Test Different Object Distributions
# ============================================
# Runs multiple experiments with different 
# hot/independent/common ratios
# ============================================

set -e

echo "=============================================="
echo "Cabinet Object Distribution Experiments"
echo "=============================================="
echo ""

# Kill any existing processes
pkill -f "./cabinet" > /dev/null 2>&1 || true
sleep 1

# Build
echo "Building Cabinet..."
go build -o ./cabinet
if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi
echo "✅ Build complete"
echo ""

# Configuration
NUM_SERVERS=5
NUM_CLIENTS=3
OPS=200
BATCHSIZE=100
BASE_LOG_DIR="./logs"
BASE_EVAL_DIR="./eval"

# Create experiment directory with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
EXPERIMENT_DIR="./experiments/${TIMESTAMP}"
mkdir -p "${EXPERIMENT_DIR}"

# Test scenarios: (hot%, indep%, common%)
declare -a SCENARIOS=(
    "0:100:0:all_independent"
    "0:0:100:all_common"
    "100:0:0:all_hot"
    "20:40:40:balanced"
    "50:25:25:high_contention"
    "10:80:10:mostly_independent"
)

echo "Running ${#SCENARIOS[@]} scenarios..."
echo "Results will be saved to: ${EXPERIMENT_DIR}"
echo ""

run_scenario() {
    local hot=$1
    local indep=$2
    local common=$3
    local name=$4
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Scenario: ${name}"
    echo "  Hot:    ${hot}%"
    echo "  Indep:  ${indep}%"
    echo "  Common: ${common}%"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Clean previous logs
    rm -rf "${BASE_LOG_DIR}"
    mkdir -p "${BASE_LOG_DIR}"
    
    # Start servers
    for ((i=0; i<NUM_SERVERS; i++)); do
        mkdir -p "${BASE_LOG_DIR}/server${i}"
        ./cabinet \
            -id=${i} \
            -n=${NUM_SERVERS} \
            -t=2 \
            -path="./config/cluster_localhost.conf" \
            -pd=true \
            -role=0 \
            -b=${BATCHSIZE} \
            -hot=${hot} \
            -indep=${indep} \
            -common=${common} \
            -et=0 \
            -ms=512 \
            -mode=0 \
            -log="error" \
            -ep=true \
            > "${BASE_LOG_DIR}/server${i}/output.log" 2>&1 &
        
        if [ $i -eq 0 ]; then
            sleep 2
        else
            sleep 0.5
        fi
    done
    
    echo "  Servers started, waiting for connections..."
    sleep 10
    
    # Start clients
    for ((i=0; i<NUM_CLIENTS; i++)); do
        client_id=$((NUM_SERVERS + i))
        mkdir -p "${BASE_LOG_DIR}/client${client_id}"
        ./cabinet \
            -id=${client_id} \
            -n=${NUM_SERVERS} \
            -t=2 \
            -path="./config/cluster_localhost.conf" \
            -ops=${OPS} \
            -pd=true \
            -role=1 \
            -b=${BATCHSIZE} \
            -hot=${hot} \
            -indep=${indep} \
            -common=${common} \
            -et=0 \
            -ms=512 \
            -mode=0 \
            -log="error" \
            > "${BASE_LOG_DIR}/client${client_id}/output.log" 2>&1 &
        
        sleep 0.5
    done
    
    echo "  Clients started, running ${OPS} operations per client..."
    
    # Wait for clients to finish
    wait
    
    # Save results
    SCENARIO_DIR="${EXPERIMENT_DIR}/${name}"
    mkdir -p "${SCENARIO_DIR}/logs"
    mkdir -p "${SCENARIO_DIR}/eval"
    
    # Copy logs and metrics
    cp -r "${BASE_LOG_DIR}"/* "${SCENARIO_DIR}/logs/"
    cp -r "${BASE_EVAL_DIR}"/*.csv "${SCENARIO_DIR}/eval/" 2>/dev/null || true
    
    # Create summary
    echo "Scenario: ${name}" > "${SCENARIO_DIR}/summary.txt"
    echo "Hot: ${hot}%, Independent: ${indep}%, Common: ${common}%" >> "${SCENARIO_DIR}/summary.txt"
    echo "Servers: ${NUM_SERVERS}, Clients: ${NUM_CLIENTS}" >> "${SCENARIO_DIR}/summary.txt"
    echo "Operations: ${OPS} per client, Batch size: ${BATCHSIZE}" >> "${SCENARIO_DIR}/summary.txt"
    echo "" >> "${SCENARIO_DIR}/summary.txt"
    
    # Extract average latency and throughput if available
    if [ -f "${SCENARIO_DIR}/eval/leader.csv" ]; then
        echo "Performance Metrics:" >> "${SCENARIO_DIR}/summary.txt"
        tail -1 "${SCENARIO_DIR}/eval/leader.csv" >> "${SCENARIO_DIR}/summary.txt"
    fi
    
    # Cleanup processes
    pkill -f "./cabinet" > /dev/null 2>&1 || true
    sleep 2
    
    echo "  ✅ Scenario complete"
    echo ""
}

# Run all scenarios
for scenario in "${SCENARIOS[@]}"; do
    IFS=':' read -r hot indep common name <<< "$scenario"
    run_scenario "$hot" "$indep" "$common" "$name"
done

echo "=============================================="
echo "✅ All experiments complete!"
echo "=============================================="
echo ""
echo "📊 Results saved to:"
echo "  ${EXPERIMENT_DIR}/"
echo ""
echo "📋 View summary:"
echo "  cat ${EXPERIMENT_DIR}/*/summary.txt"
echo ""
echo "📈 Compare metrics:"
echo "  for dir in ${EXPERIMENT_DIR}/*/; do"
echo "    echo \"\$(basename \$dir):\""
echo "    tail -1 \$dir/eval/leader.csv 2>/dev/null || echo \"No metrics\""
echo "    echo \"\""
echo "  done"
echo ""
