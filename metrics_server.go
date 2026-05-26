package main

// metrics_server.go
//
// Exposes a lightweight HTTP /metrics endpoint.
// Port = RPCListenerPort (from config) + 1000
//
// This means it works with ANY config file automatically —
// no hardcoded port numbers anywhere.
//
// The sampler in run_hetero_plainmsg_evals.sh reads the same
// config file to discover the metrics port for each client.
//
// Response is a single JSON line:
//   {"ts_ms":…,"ops":…,"fast":…,"slow":…,"conflict":…,
//    "lat_sum_ms":…,"lat_count":…,"errors":…}
//
// All counters are monotonically increasing since process start.
// The sampler computes per-interval deltas → true TPS + latency.
//
// INTEGRATION — two steps:
//   1. Drop this file alongside client.go.
//   2. In RunClient(), after perfM.Init(...), add:
//        go startMetricsServer(clientID)
//   3. In the pipelined goroutine after each RPC, add:
//        RecordBatch(batchSize, int64(reply.Latency*1000), reply.PathUsed, err != nil)
//   4. Same in the sequential branch.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
	"cabinet/config"
)

// clientMetrics holds live monotonically-increasing counters.
// All fields are updated via atomic ops — safe from any goroutine.
type clientMetrics struct {
	OpsTotal    int64
	FastOps     int64
	SlowOps     int64
	ConflictOps int64
	LatSumMs    int64 // sum of per-op latency in milliseconds
	LatCount    int64 // number of ops that contributed to LatSumMs
	Errors      int64
}

// globalClientMetrics is the single instance per process.
var globalClientMetrics clientMetrics

// RecordBatch records one completed RPC batch.
//   batchSize   — number of operations in the batch
//   latencyMs   — round-trip latency in milliseconds (reply.Latency)
//   path        — reply.PathUsed ("FAST", "SLOW", "MIXED(...)", etc.)
//   isError     — true if the RPC returned an error
func RecordBatch(batchSize int, latencyMs float64, path string, isError bool) {
	if isError {
		atomic.AddInt64(&globalClientMetrics.Errors, 1)
		atomic.AddInt64(&globalClientMetrics.OpsTotal, int64(batchSize))
		atomic.AddInt64(&globalClientMetrics.ConflictOps, int64(batchSize))
		return
	}

	atomic.AddInt64(&globalClientMetrics.OpsTotal, int64(batchSize))

	// Distribute latency across all ops in the batch
	if latencyMs > 0 {
		atomic.AddInt64(&globalClientMetrics.LatSumMs, int64(latencyMs)*int64(batchSize))
		atomic.AddInt64(&globalClientMetrics.LatCount, int64(batchSize))
	}

	fast, slow, conflict := classifyPath(path, batchSize)
	if fast > 0 {
		atomic.AddInt64(&globalClientMetrics.FastOps, int64(fast))
	}
	if slow > 0 {
		atomic.AddInt64(&globalClientMetrics.SlowOps, int64(slow))
	}
	if conflict > 0 {
		atomic.AddInt64(&globalClientMetrics.ConflictOps, int64(conflict))
	}
}

// classifyPath maps PathUsed → (fast, slow, conflict) op counts.
func classifyPath(path string, batchSize int) (fast, slow, conflict int) {
	if len(path) >= 5 && path[:5] == "MIXED" {
		var f, s, h int
		n, _ := fmt.Sscanf(path, "MIXED(FAST:%d,SLOW:%d,HOT:%d)", &f, &s, &h)
		if n < 3 {
			fmt.Sscanf(path, "MIXED(FAST:%d,SLOW:%d)", &f, &s)
		}
		return f, s, h
	}
	if len(path) >= 3 && path[:3] == "HOT" {
		var h int
		fmt.Sscanf(path, "HOT:%d", &h)
		return 0, 0, h
	}
	switch path {
	case "FAST":
		return batchSize, 0, 0
	case "SLOW":
		return 0, batchSize, 0
	default:
		return 0, 0, batchSize
	}
}

// metricsSnapshot is the JSON response.
type metricsSnapshot struct {
	TsMs      int64 `json:"ts_ms"`
	Ops       int64 `json:"ops"`
	Fast      int64 `json:"fast"`
	Slow      int64 `json:"slow"`
	Conflict  int64 `json:"conflict"`
	LatSumMs  int64 `json:"lat_sum_ms"`
	LatCount  int64 `json:"lat_count"`
	Errors    int64 `json:"errors"`
}

// metricsPortForClient reads the RPC port for clientID from the config
// file and returns rpcPort + 1000 as the metrics port.
// This works with any config file — no hardcoded numbers.
func metricsPortForClient(clientID int, cfgPath string, totalNodes int) int {
	// ParseClusterConfig requires the total row count (servers + clients).
	// We pass totalNodes = numOfServers + numClients, but since we only need
	// one row we just use clientID+1 as a safe lower bound.
	needed := clientID + 1
	rows := config.ParseClusterConfig(needed, cfgPath)
	if clientID >= len(rows) {
		log.Warnf("metricsPortForClient: clientID %d not in config (%d rows); using fallback", clientID, len(rows))
		return 19100 + clientID // safe fallback unlikely to conflict
	}
	rpcPort, err := strconv.Atoi(rows[clientID][config.ServerRPCListenerPort])
	if err != nil {
		log.Warnf("metricsPortForClient: bad RPC port in config for id %d: %v; using fallback", clientID, err)
		return 19100 + clientID
	}
	return rpcPort + 1000
}

// startMetricsServer starts the HTTP server on rpcPort+1000.
// Call as: go startMetricsServer(clientID)
func startMetricsServer(clientID int) {
	port := metricsPortForClient(clientID, configPath, numOfServers+2)

	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		snap := metricsSnapshot{
			TsMs:     time.Now().UnixMilli(),
			Ops:      atomic.LoadInt64(&globalClientMetrics.OpsTotal),
			Fast:     atomic.LoadInt64(&globalClientMetrics.FastOps),
			Slow:     atomic.LoadInt64(&globalClientMetrics.SlowOps),
			Conflict: atomic.LoadInt64(&globalClientMetrics.ConflictOps),
			LatSumMs: atomic.LoadInt64(&globalClientMetrics.LatSumMs),
			LatCount: atomic.LoadInt64(&globalClientMetrics.LatCount),
			Errors:   atomic.LoadInt64(&globalClientMetrics.Errors),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snap)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	addr := fmt.Sprintf(":%d", port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	}

	log.Infof("Client %d: metrics endpoint on http://0.0.0.0%s/metrics (rpcPort+1000)", clientID, addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Warnf("Client %d: metrics server error: %v", clientID, err)
	}
}
