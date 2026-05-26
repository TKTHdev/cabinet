package main

import (
	"cabinet/eval"
	"cabinet/mongodb"
	"cabinet/smr"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

var log = logrus.New()

var mypriority = smr.NewServerPriority(0, 0)
var mystate = smr.NewServerState()
var pManager smr.PriorityManager

var perfM eval.PerfMeter
var globalClockCounter int64 // Global counter for RPC arrivals
var serverMetricsSaved atomic.Bool

// Mongo DB variables
var mongoDbFollower *mongodb.MongoFollower

// Create Cabinet alias
type serverID = int
type prioClock = int
type priority = float64

func init() {
	fmt.Println("program starts ...")
	loadCommandLineInputs()
	setLogger(logLevel)

	mystate.SetMyServerID(myServerID)
	mystate.SetLeaderID(0)

	pManager.Init(numOfServers, quorum, 1, ratioTryStep, enablePriority)

	if role == 0 {
		fileName := fmt.Sprintf("s%d_n%d_f%d_b%d_%s", myServerID, numOfServers, quorum, batchsize, suffix)
		perfM.Init(1, batchsize, fileName)
		log.Infof("Server %d: perfM initialized", myServerID)
	}
}

func main() {
	mypriority.SetMajority(pManager.GetMajority())
	pscheme := pManager.GetPriorityScheme()

	fmt.Println("information board")
	fmt.Printf("priority scheme: %v\n", pscheme)
	fmt.Printf("majority: %v\n", mypriority.GetMajority())

	switch role {
	case 0: // SERVER
		runServerRole(pscheme)
	case 1: // CLIENT
		runClientRole()
	default:
		log.Fatalf("Invalid role specified: %d. Must be 0 (server) or 1 (client)", role)
	}
}

// preloadCabinetObjects pre-creates objects used by the client workload generators.
// This reduces first-touch latency but does not change Cabinet's single-path logic.
func preloadCabinetObjects() {
	if !preloadEnabled {
		log.Infof("preload=false, skipping object preloading")
		return
	}

	start := time.Now()

	hotCount := preloadHot
	commonCount := preloadCommon
	if commonCount < 0 {
		commonCount = 1000
		if numOps > 0 {
			commonCount = (numOps + 9) / 10
		}
	}

	indepPerClient := preloadIndepPerClient
	if indepPerClient < 0 {
		indepPerClient = 1000
		if numOps > 0 {
			indepPerClient = numOps
		}
	}

	clientStart := preloadClientStart
	if clientStart < 0 {
		clientStart = numOfServers
	}

	clientCount := preloadClientCount
	if clientCount < 0 {
		clientCount = 0
	}

	added := 0

	for i := 0; i < hotCount; i++ {
		objID := fmt.Sprintf("obj-HOT-%d", i)
		mystate.AddObject(objID, HotObject, numOfServers)
		added++
	}

	for i := 0; i < commonCount; i++ {
		objID := fmt.Sprintf("obj-common-%d", i)
		mystate.AddObject(objID, CommonObject, numOfServers)
		added++
	}

	for cid := clientStart; cid < clientStart+clientCount; cid++ {
		for i := 0; i < indepPerClient; i++ {
			objID := fmt.Sprintf("obj-indep-c%d-%d", cid, i)
			mystate.AddObject(objID, IndependentObject, numOfServers)
			added++
		}
	}

	log.Infof("Preloaded objects on leader: hot=%d common=%d indep_per_client=%d clients=[%d..%d] total=%d in %v",
		hotCount, commonCount, indepPerClient, clientStart, clientStart+clientCount-1, added, time.Since(start))
}

// ------------------ SERVER ROLE ------------------
func runServerRole(pscheme []float64) {
	saveServerMetricsOnce := func(reason string) {
		if !serverMetricsSaved.CompareAndSwap(false, true) {
			return
		}
		fmt.Printf("Server %d %s. Saving metrics...\n", myServerID, reason)
		if err := perfM.SaveToFile(); err != nil {
			log.Errorf("Server %d: failed to save metrics: %v", myServerID, err)
		} else {
			fmt.Printf("Server %d: metrics saved under ./eval/server%d/\n", myServerID, myServerID)
		}
	}

	if myServerID == 0 {
		if len(pscheme) > 0 {
			if err := mypriority.UpdatePriority(0, pscheme[0]); err != nil {
				log.Fatalf("failed to initialize leader priority: %v", err)
			}
		}

		// Start RPC server for client connections
		go startLeaderRPCServer()

		establishRPCs()
		log.Infof("establishRPCs() was successful.")

		preloadCabinetObjects()

		// Graceful shutdown: coordinate with consensus loop
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			sig := <-stop
			fmt.Printf("Server %d received signal (%v). Initiating graceful shutdown...\n", myServerID, sig)
			shutdownInProgress.Store(true)
			shutdownOnce.Do(func() { close(shutdownSignal) })
		}()

		startSyncCabInstanceWithClients()
		// Consensus loop returned: flush metrics and exit main.
		saveServerMetricsOnce("shutdown")

	} else {
		// Graceful shutdown for followers as well
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			sig := <-stop
			fmt.Printf("Server %d received signal (%v). Shutting down...\n", myServerID, sig)
			shutdownInProgress.Store(true)
			shutdownOnce.Do(func() { close(followerStop) })
		}()

		runFollower()
		saveServerMetricsOnce("shutdown")
	}
}

// ------------------ CLIENT ROLE ------------------
func runClientRole() {
	fmt.Printf("Starting in client mode | ClientID: %d | NumOps: %d | BatchMode: %s | BatchComposition: %s\n",
		myServerID, numOps, batchMode, batchComposition)
	RunClient(myServerID, configPath, numOps, batchMode, batchComposition)
}
