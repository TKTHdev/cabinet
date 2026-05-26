package main

import (
	"cabinet/config"
	"cabinet/eval"
	"cabinet/mongodb"
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"strings"
	"time"
)

var (
	hotObjIDs    [10]string
	indepObjIDs  [100000]string
	commonObjIDs [10000]string
	objIDsOnce   sync.Once
)

func initObjectIDPool(clientID int) {
	objIDsOnce.Do(func() {
		start := time.Now()

		for i := 0; i < len(hotObjIDs); i++ {
			hotObjIDs[i] = fmt.Sprintf("obj-HOT-%d", i)
		}
		for i := 0; i < len(indepObjIDs); i++ {
			indepObjIDs[i] = fmt.Sprintf("obj-indep-c%d-%d", clientID, i)
		}
		for i := 0; i < len(commonObjIDs); i++ {
			commonObjIDs[i] = fmt.Sprintf("obj-common-%d", i)
		}

		log.Infof("Client %d: pre-generated %d hot, %d independent, and %d common object IDs in %v",
			clientID, len(hotObjIDs), len(indepObjIDs), len(commonObjIDs), time.Since(start))
	})
}

func timeseriesEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("ENABLE_TIMESERIES"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func timeseriesInterval() time.Duration {
	intervalMs, err := strconv.Atoi(strings.TrimSpace(os.Getenv("TPS_TIMELINE_INTERVAL_MS")))
	if err != nil || intervalMs <= 0 {
		intervalMs = 500
	}
	return time.Duration(intervalMs) * time.Millisecond
}

func readEventLabel(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	label := strings.TrimSpace(string(data))
	if label == "" {
		return ""
	}
	_ = os.WriteFile(path, nil, 0o644)
	return label
}

func startTimeSeriesRecorder(clientID int) func() {
	if !timeseriesEnabled() {
		return func() {}
	}

	dir := fmt.Sprintf("./eval/client%d", clientID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Warnf("Client %d: failed to create timeseries directory %s: %v", clientID, dir, err)
		return func() {}
	}

	filePath := filepath.Join(dir, fmt.Sprintf("tps_timeline_%s.csv", time.Now().Format("20060102_150405")))
	file, err := os.Create(filePath)
	if err != nil {
		log.Warnf("Client %d: failed to create timeseries CSV %s: %v", clientID, filePath, err)
		return func() {}
	}

	if _, err := fmt.Fprintln(file, "elapsed_ms,tps,lat_avg_ms,event"); err != nil {
		log.Warnf("Client %d: failed to write timeseries header %s: %v", clientID, filePath, err)
		_ = file.Close()
		return func() {}
	}

	interval := timeseriesInterval()
	eventPath := filepath.Join(dir, ".event")
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		defer func() { _ = file.Close() }()

		startAt := time.Now()
		prevAt := startAt
		prevOps := atomic.LoadInt64(&globalClientMetrics.OpsTotal)
		prevLatSum := atomic.LoadInt64(&globalClientMetrics.LatSumMs)
		prevLatCount := atomic.LoadInt64(&globalClientMetrics.LatCount)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		writeSample := func(sampleAt time.Time) {
			ops := atomic.LoadInt64(&globalClientMetrics.OpsTotal)
			latSum := atomic.LoadInt64(&globalClientMetrics.LatSumMs)
			latCount := atomic.LoadInt64(&globalClientMetrics.LatCount)

			dOps := ops - prevOps
			dLatSum := latSum - prevLatSum
			dLatCount := latCount - prevLatCount
			dtMs := sampleAt.Sub(prevAt).Milliseconds()
			if dtMs <= 0 {
				return
			}

			if dOps < 0 {
				dOps = 0
			}
			if dLatSum < 0 {
				dLatSum = 0
			}
			if dLatCount < 0 {
				dLatCount = 0
			}

			tps := float64(dOps) * 1000.0 / float64(dtMs)
			latAvg := 0.0
			if dLatCount > 0 {
				latAvg = float64(dLatSum) / float64(dLatCount)
			}

			event := readEventLabel(eventPath)
			elapsedMs := sampleAt.Sub(startAt).Milliseconds()
			if _, err := fmt.Fprintf(file, "%d,%.3f,%.3f,%s\n", elapsedMs, tps, latAvg, event); err != nil {
				log.Warnf("Client %d: failed to write timeseries sample: %v", clientID, err)
			}
			_ = file.Sync()
			prevOps = ops
			prevLatSum = latSum
			prevLatCount = latCount
			prevAt = sampleAt
		}

		for {
			select {
			case sampleAt := <-ticker.C:
				writeSample(sampleAt)
			case <-stopCh:
				writeSample(time.Now())
				return
			}
		}
	}()

	return func() {
		close(stopCh)
		<-doneCh
	}
}

func RunClient(myServerID int, configPath string, numOps int, batchMode string, batchComposition string) {
	log.Infof("Client %d: SEQUENTIAL mode enabled (ordered batches)", myServerID)
	
	// --- 1. Parse cluster configuration ---
	clusterConf := config.ParseClusterConfig(numOfServers, configPath)
	cluster := make(map[int]string)
	for sid, info := range clusterConf {
		cluster[sid] = info[config.ServerIP] + ":" + info[config.ServerRPCListenerPort]
	}

	leaderID := 0
	leaderAddr := cluster[leaderID]
	conn, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		log.Fatalf("Client %d: failed to connect to leader at %s: %v", myServerID, leaderAddr, err)
	}
	defer conn.Close()
	log.Infof("Client %d: connected to leader at %s", myServerID, leaderAddr)
	go startMetricsServer(myServerID)
	stopTimeSeriesRecorder := startTimeSeriesRecorder(myServerID)
	defer stopTimeSeriesRecorder()

	// --- 3. Graceful shutdown ---
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Flag to signal graceful shutdown
	var shutdownRequested atomic.Bool
	var activeRPCs atomic.Int64

	// --- 4. Initialize logical clock and performance meter ---
	var clientClock int
	var clockLock sync.Mutex
	incrementClock := func() int {
		clockLock.Lock()
		defer clockLock.Unlock()
		clientClock++
		return clientClock
	}

	fileSuffix := fmt.Sprintf("client%d_eval", myServerID)
	clientPerfM := eval.PerfMeter{}
	clientPerfM.Init(1, batchsize, fileSuffix)

	// Atomic flag to prevent duplicate metric saves
	var metricsSaved atomic.Bool
	saveMetricsOnce := func(reason string) {
		if !metricsSaved.CompareAndSwap(false, true) {
			return
		}

		fmt.Printf("Client %d %s. Saving metrics...\n", myServerID, reason)
		if err := clientPerfM.SaveToFile(); err != nil {
			log.Errorf("Client %d: failed to save metrics: %v", myServerID, err)
			return
		}
		fmt.Printf("Client %d: ✓ Client metrics saved to ./eval/client%d_eval.csv\n", myServerID, myServerID)
	}

	// Background goroutine to save metrics on interrupt
	go func() {
		sig := <-stop
		fmt.Printf("Client %d received interrupt signal, initiating graceful shutdown...\n", myServerID)
		shutdownRequested.Store(true)
		_ = sig
	}()

	rand.Seed(time.Now().UnixNano() + int64(myServerID))

	opsLabel := fmt.Sprintf("%d", numOps)
	if numOps <= 0 {
		opsLabel = "infinite"
	}

	fmt.Printf("Client %d started | Total Ops: %s | Batch Mode: %s | Batch Size: %d | Eval Type: %s\n",
		myServerID, opsLabel, batchMode, batchsize, evalTypeName(evalType))

	if evalType != PlainMsg && evalType != MongoDB {
		log.Fatalf("Client %d: unsupported eval type %d (only PlainMsg=0 and MongoDB=2 supported)", myServerID, evalType)
	}

	// Validate object ratios
	totalRatio := hotObjRatio + indepObjRatio + commonObjRatio
	if totalRatio != 100 {
		log.Fatalf("Client %d: Object ratios must sum to 100%% (hot=%d%% + indep=%d%% + common=%d%% = %d%%)",
			myServerID, hotObjRatio, indepObjRatio, commonObjRatio, totalRatio)
	}

	initObjectIDPool(myServerID)

	if batchComposition == "mixed" {
		log.Infof("Client %d: MIXED batch mode - each batch contains diverse objects and types", myServerID)
	} else {
		log.Infof("Client %d: OBJECT-SPECIFIC batch mode - each batch targets one object type (different IDs allowed)", myServerID)
	}
	
	fmt.Printf("  Object Distribution: Hot=%d%% | Independent=%d%% | Common=%d%%\n",
		hotObjRatio, indepObjRatio, commonObjRatio)

	// --- 5. Preload MongoDB queries if needed ---
	var mongoDBQueries []mongodb.Query
	if evalType == MongoDB {
		filePath := fmt.Sprintf("%srun_workload%s.dat", mongodb.DataPath, mongoLoadType)
		var err error
		mongoDBQueries, err = mongodb.ReadQueryFromFile(filePath)
		if err != nil {
			log.Errorf("ReadQueryFromFile failed | err: %v", err)
			return
		}
		if len(mongoDBQueries) == 0 {
			log.Errorf("Client %d: MongoDB workload is empty: %s", myServerID, filePath)
			return
		}
		log.Infof("Client %d: loaded %d MongoDB queries", myServerID, len(mongoDBQueries))
	}

	if maxInflight > 1 {
		log.Infof("Client %d: PIPELINED client dispatch enabled (maxInflight=%d)", myServerID, maxInflight)
	} else {
		log.Infof("Client %d: client dispatch is sequential (maxInflight=1)", myServerID)
	}
	inflightGate := make(chan struct{}, maxInflight)
	var dispatchWG sync.WaitGroup

	op := 0
	var conflictCounter atomic.Int64

	// --- 7. Main operation loop ---
	// Run indefinitely if numOps <= 0, otherwise run for numOps operations
	infinite := numOps <= 0
	for infinite || op < numOps {
		// Deterministic stop: no new batches after shutdown is requested.
		if shutdownRequested.Load() {
			break
		}
		
		// Decide batch size
		currentBatch := batchsize
		if !infinite && op+batchsize > numOps {
			currentBatch = numOps - op
		}
		batchStart := op
		op += currentBatch

		if maxInflight > 1 {
			inflightGate <- struct{}{}
			dispatchWG.Add(1)
			activeRPCs.Add(1)
			go func(batchStart int, currentBatch int) {
				defer dispatchWG.Done()
				defer activeRPCs.Add(-1)
				defer func() { <-inflightGate }()
				sendBatchToLeader(myServerID, batchStart, currentBatch, conn, incrementClock,
					mongoDBQueries, &clientPerfM, batchComposition, &conflictCounter)
			}(batchStart, currentBatch)
		} else {
			activeRPCs.Add(1)
			sendBatchToLeader(myServerID, batchStart, currentBatch, conn, incrementClock,
				mongoDBQueries, &clientPerfM, batchComposition, &conflictCounter)
			activeRPCs.Add(-1)
		}
	}

	if maxInflight > 1 {
		dispatchWG.Wait()
	}

	// --- 8. Close connection and save metrics ---
	conn.Close()

	// CRITICAL FIX: Save client metrics at end if not already saved
	saveMetricsOnce("completed run")

	if numOps <= 0 {
		fmt.Printf("Client %d stopped after %d ops (infinite mode).\n", myServerID, op)
	} else {
		fmt.Printf("Client %d finished all %d ops.\n", myServerID, numOps)
	}
}

// sendBatchToLeader sends a batch of operations to the leader
func sendBatchToLeader(myServerID int, batchStart int, currentBatch int,
conn *rpc.Client, incrementClock func() int,
mongoDBQueries []mongodb.Query,
perfM *eval.PerfMeter, batchComposition string, conflictCounterPtr *atomic.Int64) {

    // Sanity check
    if currentBatch <= 0 {
        return
    }

    // Build per-op ObjIDs and ObjTypes (mixed-like batches).
    objIDs := make([]string, currentBatch)
    objTypes := make([]int, currentBatch)

    if batchComposition == "mixed" {
		// Generate mixed object types and IDs
		for b := 0; b < currentBatch; b++ {
			randVal := rand.Float64() * 100.0
			switch {
			case randVal < float64(hotObjRatio):
				objTypes[b] = HotObject
				objIDs[b] = hotObjIDs[(batchStart+b)%len(hotObjIDs)]
			case randVal < float64(hotObjRatio+indepObjRatio):
				objTypes[b] = IndependentObject
				objIDs[b] = indepObjIDs[(batchStart+b)%len(indepObjIDs)]
			default:
				objTypes[b] = CommonObject
				objIDs[b] = commonObjIDs[((batchStart+b)/10)%len(commonObjIDs)]
			}
		}
	} else {
		// OBJECT-SPECIFIC: All ops in batch target the same object type, but can have different object IDs
		var objType int
		randVal := rand.Float64() * 100.0
		if randVal < float64(hotObjRatio) {
			objType = HotObject
		} else if randVal < float64(hotObjRatio+indepObjRatio) {
			objType = IndependentObject
		} else {
			objType = CommonObject
		}

		// Generate different object IDs within the same type for each operation in the batch
		for b := 0; b < currentBatch; b++ {
			objTypes[b] = objType
			switch objType {
			case HotObject:
				objIDs[b] = hotObjIDs[(batchStart+b)%len(hotObjIDs)]
			case IndependentObject:
				objIDs[b] = indepObjIDs[(batchStart+b)%len(indepObjIDs)]
			case CommonObject:
				objIDs[b] = commonObjIDs[((batchStart+b)/10)%len(commonObjIDs)]
			}
		}
	}

    // Increment client logical clock
    CClock := incrementClock()

    // Build the client command, include per-op metadata (ObjIDs/ObjTypes).
    // Keep ObjID/ObjType populated with the first element for compatibility.
    cmd := &ClientArgs{
        ClientID:    myServerID,
        ClientClock: CClock,
        ObjID:       objIDs[0],
        ObjType:     objTypes[0],
        ObjIDs:      objIDs,
        ObjTypes:    objTypes,
        Type:        evalType,
    }

    // Fill the batch payload
    switch evalType {
    case PlainMsg:
        batch := make([][]byte, currentBatch)
        for b := 0; b < currentBatch; b++ {
            batch[b] = genRandomBytes(msgsize)
        }
        cmd.CmdPlain = batch

    case MongoDB:
        // Guard against empty mongoDBQueries to avoid panic
        if len(mongoDBQueries) == 0 {
            log.Warnf("Client %d: no MongoDB queries loaded; skipping batch", myServerID)
            return
        }
        batch := make([]mongodb.Query, currentBatch)
        for b := 0; b < currentBatch; b++ {
			batch[b] = mongoDBQueries[(batchStart+b)%len(mongoDBQueries)]
        }
        cmd.CmdMongo = batch
    }

	sequentialWaitTime := time.Duration(0)

	reply := &ClientReply{}
	log.Debugf("[LATENCY-BREAKDOWN] Client %d | Batch %d | Starting RPC call | SequentialWait: %v ms", 
		myServerID, cmd.ClientClock, sequentialWaitTime.Milliseconds())
	
	
	perfM.RecordStarter(cmd.ClientClock)
	rpcStartTime := time.Now()
	
	err := conn.Call("CabService.ClientRequestService", cmd, reply)
	
	rpcLatency := time.Since(rpcStartTime)

	if err != nil {
		if errRec := perfM.RecordFinisher(cmd.ClientClock); errRec != nil {
			// ignore
		}
		log.Warnf("[Client %d] Batch %d failed: %v | RPC time: %v ms", myServerID, cmd.ClientClock, err, rpcLatency.Milliseconds())
	} else {
		perfM.RecordFinisher(cmd.ClientClock)
		log.Debugf("[LATENCY-BREAKDOWN] Client %d | Batch %d | RPC completed | NetworkRoundTrip: %v ms", 
			myServerID, cmd.ClientClock, rpcLatency.Milliseconds())
	}
	
	slowCount, conflictCount := 0, 0
	for _, objType := range objTypes {
		switch objType {
		case HotObject:
			conflictCount++
		default: // IndependentObject or CommonObject
			slowCount++
		}
	}

	// Record per-operation path metrics: conflicts for hot, slow path for indep/common
	if err == nil {
		if reply.Success {
			// Cabinet only has slow path - indep and common objects use slow path
			for i := 0; i < slowCount; i++ {
				perfM.IncSlowPath(CClock)
			}
			// Hot objects are conflicts (resolved via slow path)
			for i := 0; i < conflictCount; i++ {
				perfM.IncConflict(CClock)
			}
			
			// Update global commit counters
			if slowCount > 0 {
				perfM.AddSlowCommits(slowCount)
			}
			if conflictCount > 0 {
				perfM.AddConflictCommits(conflictCount)
			}
		}
	}

	// Increment conflict counter for hot object commits
	if conflictCount > 0 {
		if conflictCounterPtr != nil {
			conflictCounterPtr.Add(int64(conflictCount))
		}
	}

	// Log batch composition and conflict count with full latency breakdown
	conflicts := 0
	if conflictCounterPtr != nil {
		conflicts = int(conflictCounterPtr.Load())
	}
	
	// Format path based on batch composition
	var pathStr string
	if batchComposition == "mixed" {
		// Mixed mode: show breakdown of different types
		pathStr = fmt.Sprintf("MIXED(SLOW:%d,CONFLICT:%d)", slowCount, conflictCount)
	} else {
		// Object-specific mode: all operations are same type
		if conflictCount > 0 && slowCount == 0 {
			pathStr = fmt.Sprintf("HOT:%d", conflictCount)
		} else if slowCount > 0 && conflictCount == 0 {
			pathStr = fmt.Sprintf("SLOW:%d", slowCount)
		} else if conflictCount > 0 && slowCount > 0 {
			// Shouldn't happen in object-specific, but log it for debugging
			pathStr = fmt.Sprintf("MIXED(SLOW:%d,CONFLICT:%d)", slowCount, conflictCount)
		} else {
			pathStr = "UNKNOWN"
		}
	}

	RecordBatch(currentBatch, float64(rpcLatency.Milliseconds()), pathStr, err != nil)
	
	log.Infof("[LATENCY-BREAKDOWN] Client %d | Batch %d | size=%d | LeaderClock=%d | %s | SequentialWait=%dms | NetworkRoundTrip=%dms | Conflicts=%d",
		myServerID, batchStart, currentBatch, reply.LeaderClock, pathStr,
		sequentialWaitTime.Milliseconds(), rpcLatency.Milliseconds(),conflicts)
}

// Helper to print eval type
func evalTypeName(et int) string {
	switch et {
	case PlainMsg:
		return "Plain Message"
	case MongoDB:
		return "MongoDB (YCSB)"
	default:
		return "Unknown"
	}
}

// Helper to print object type
func getObjTypeName(ot int) string {
	switch ot {
	case HotObject:
		return "HOT"
	case IndependentObject:
		return "INDEP"
	case CommonObject:
		return "COMMON"
	default:
		return "UNKNOWN"
	}
}