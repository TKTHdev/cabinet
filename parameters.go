package main

import (
	"flag"
)

const (
	Localhost = iota
	Distributed
)

const (
	PlainMsg = iota
	MongoDB
)

// Object types for workload characterization
const (
	HotObject = iota
	IndependentObject
	CommonObject
)

var numOfServers int
var threshold int
var quorum int
var myServerID int
var configPath string
var production bool
var logLevel string
var mode int
var evalType int

var batchsize int
var msgsize int

var ratioTryStep float64
var enablePriority bool

// Mongo DB input parameters
var mongoLoadType string
var mongoClientNum int

// crash test parameters
var crashTime int
var crashMode int

// suffix of files
var suffix string

// role-based parameters
var role int
var numOps int
var batchMode string
var maxInflight int

// object distribution parameters
var hotObjRatio int
var indepObjRatio int
var commonObjRatio int

// Add batchComposition as a configurable parameter
var (
	batchComposition string // Defines the batch composition mode: "mixed" or "object-specific"
)

// preload parameters (leader-side object warmup)
var preloadEnabled bool
var preloadHot int
var preloadCommon int
var preloadIndepPerClient int
var preloadClientStart int
var preloadClientCount int

func loadCommandLineInputs() {
	flag.IntVar(&numOfServers, "n", 5, "# of servers")

	flag.IntVar(&threshold, "t", 1, "# of quorum tolerated")
	quorum = threshold + 1
	flag.IntVar(&batchsize, "b", 1, "batch size")
	flag.IntVar(&myServerID, "id", 0, "this server ID")
	flag.StringVar(&configPath, "path", "./config/cluster_localhost.conf", "config file path")

	// Production mode stores log on disk at ./logs/
	flag.BoolVar(&production, "pd", false, "production mode?")
	flag.StringVar(&logLevel, "log", "debug", "trace, debug, info, warn, error, fatal, panic")
	flag.IntVar(&mode, "mode", 0, "0 -> localhost; 1 -> distributed")
	flag.IntVar(&evalType, "et", 0, "0 -> plain msg; 1 -> tpcc; 2 -> mongodb")

	flag.Float64Var(&ratioTryStep, "rstep", 0.001, "rate for trying qualified ratio")

	flag.BoolVar(&enablePriority, "ep", true, "true -> cabinet; false -> raft")

	// Plain message input parameters
	flag.IntVar(&msgsize, "ms", 512, "message size")

	// MongoDB input parameters
	flag.StringVar(&mongoLoadType, "mload", "a", "mongodb load type")
	flag.IntVar(&mongoClientNum, "mcli", 16, "# of mongodb clients")

	// crash test parameters
	flag.IntVar(&crashTime, "ct", 20, "# of rounds before crash")
	flag.IntVar(&crashMode, "cm", 0, "0 -> no crash; 1 -> strong machines; 2 -> weak machines; 3 -> random machines")

	// suffix of files
	flag.StringVar(&suffix, "suffix", "cab", "suffix of files")

	// role-based parameters
	flag.IntVar(&role, "role", 0, "0 -> server; 1 -> client")
	flag.IntVar(&numOps, "ops", 100, "number of operations to send (0 for infinite)")
	flag.StringVar(&batchMode, "bmode", "single", "batch mode: single or round-robin")
	flag.IntVar(&maxInflight, "max-inflight", 1, "maximum concurrent in-flight client batches")

	// object distribution parameters
	flag.IntVar(&hotObjRatio, "hot", 0, "percentage of hot objects (shared between clients)")
	flag.IntVar(&indepObjRatio, "indep", 90, "percentage of independent objects (per-client)")
	flag.IntVar(&commonObjRatio, "common", 10, "percentage of common objects")

	// batch composition parameter
	flag.StringVar(&batchComposition, "bcomp", "object-specific", "batch composition mode: mixed or object-specific")

	// preloading parameters
	flag.BoolVar(&preloadEnabled, "preload", true, "enable leader object preloading")
	flag.IntVar(&preloadHot, "preload-hot", 10, "number of hot objects to preload")
	flag.IntVar(&preloadCommon, "preload-common", -1, "number of common objects to preload (-1 = auto)")
	flag.IntVar(&preloadIndepPerClient, "preload-indep", -1, "number of independent objects per client to preload (-1 = auto)")
	flag.IntVar(&preloadClientStart, "preload-client-start", -1, "starting client ID for independent-object preload (-1 = numOfServers)")
	flag.IntVar(&preloadClientCount, "preload-client-count", 4, "number of client IDs to preload independent objects for")

	flag.Parse()
	if maxInflight < 1 {
		maxInflight = 1
	}
	log.Debugf("CommandLine parameters:\n - numOfServers:%v\n - myServerID:%v\n", numOfServers, myServerID)
}

