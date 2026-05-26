#!/bin/bash

# ============================================
# Cabinet Setup Script
# ============================================
# Makes all scripts executable and verifies setup
# ============================================

echo "=============================================="
echo "Cabinet Setup"
echo "=============================================="
echo ""

# Make scripts executable
echo "Making scripts executable..."
chmod +x run_cluster.sh
chmod +x quickstart.sh
chmod +x run_experiments.sh
chmod +x run_cabinet.sh 2>/dev/null || true
chmod +x run_cabinet_eval.sh 2>/dev/null || true

echo "✅ Scripts are now executable"
echo ""

# Check directories
echo "Checking directories..."
mkdir -p ./logs
mkdir -p ./eval
mkdir -p ./experiments
echo "✅ Directories created"
echo ""

# Check config file
echo "Checking configuration..."
if [ -f "./config/cluster_localhost.conf" ]; then
    echo "✅ Config file exists: ./config/cluster_localhost.conf"
else
    echo "⚠️  Config file not found: ./config/cluster_localhost.conf"
    echo "   You may need to create this file before running"
fi
echo ""

# Check Go installation
echo "Checking Go installation..."
if command -v go &> /dev/null; then
    GO_VERSION=$(go version)
    echo "✅ Go is installed: ${GO_VERSION}"
else
    echo "❌ Go is not installed"
    echo "   Install Go from: https://go.dev/dl/"
    exit 1
fi
echo ""

# Test build
echo "Testing build..."
go build -o ./cabinet
if [ $? -eq 0 ]; then
    echo "✅ Build successful"
    rm ./cabinet
else
    echo "❌ Build failed"
    echo "   Check your Go installation and dependencies"
    exit 1
fi
echo ""

echo "=============================================="
echo "✅ Setup Complete!"
echo "=============================================="
echo ""
echo "Available scripts:"
echo "  ./quickstart.sh         - Quick test (3 servers, 2 clients)"
echo "  ./run_cluster.sh        - Full cluster (5 servers, 3 clients)"
echo "  ./run_experiments.sh    - Run multiple experiments"
echo ""
echo "Documentation:"
echo "  cat RUN_SCRIPTS_README.md"
echo ""
echo "Get started:"
echo "  ./quickstart.sh"
echo ""
