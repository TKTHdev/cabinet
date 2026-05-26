package main

import (
	"cabinet/config"
	"cabinet/mongodb"
	"encoding/gob"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// conns does not store the operating servers' information
// Specifically, the leader (serverID 0) is not included in conns.m.
// The leader's weight is handled separately via mypriority.GetPrioVal() as the seed for prioSum
// in startSyncCabInstanceWithClients, and its weight is always set to scheme[0] per Algorithm 1.
// This design ensures the leader's weight is properly accounted for in consensus while maintaining
// a clean separation between the leader's own state and the follower connections.
var conns = struct {
	sync.RWMutex
	m map[int]*ServerDock
}{
	m: make(map[int]*ServerDock),
}

type ServerDock struct {
	serverID   int
	addr       string
	txClient   *rpc.Client
	jobQMu     sync.RWMutex
	jobQ       map[prioClock]chan struct{}
}

// startLeaderRPCServer starts RPC server for the leader to accept client connections
func startLeaderRPCServer() {
	serverConfig := config.ParseClusterConfig(numOfServers, configPath)
	ipIndex := config.ServerIP
	rpcPortIndex := config.ServerRPCListenerPort

	myAddr := serverConfig[myServerID][ipIndex] + ":" + serverConfig[myServerID][rpcPortIndex]
	log.Infof("Leader starting RPC server on %s for client connections", myAddr)

	// Register the service explicitly under the name "CabService" to avoid
	// any ambiguity about the registered name used by the client calls.
	if err := rpc.RegisterName("CabService", NewCabService()); err != nil {
		log.Fatalf("rpc.RegisterName failed | error: %v", err)
		return
	}

	// Start listening
	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		log.Fatalf("Leader RPC Listen error: %v", err)
		return
	}

	log.Infof("Leader RPC server listening on %s", myAddr)

	// Use rpc.Accept to serve incoming connections with the default server.
	// This is the canonical accept loop for net/rpc.
	rpc.Accept(listener)
}

func runFollower() {
	serverConfig := config.ParseClusterConfig(numOfServers, configPath)
	ipIndex := config.ServerIP
	rpcPortIndex := config.ServerRPCListenerPort

	// Initialize follower's local priority from the configured scheme.
	pscheme := pManager.GetPriorityScheme()
	if myServerID >= 0 && myServerID < len(pscheme) {
		if err := mypriority.UpdatePriority(0, pscheme[myServerID]); err != nil {
			log.Fatalf("Follower %d: priority init failed: %v", myServerID, err)
			return
		}
	} else {
		log.Warnf("Follower %d: priority scheme missing entry (schemeLen=%d)", myServerID, len(pscheme))
	}

	myAddr := serverConfig[myServerID][ipIndex] + ":" + serverConfig[myServerID][rpcPortIndex]
	log.Debugf("config: serverID %d | addr: %s", myServerID, myAddr)

	switch evalType {
	case PlainMsg:
		// nothing needs to be done
	case MongoDB:
		go mongoDBCleanUp()
		initMongoDB()
	}

	if err := rpc.RegisterName("CabService", NewCabService()); err != nil {
		log.Fatalf("rpc.RegisterName failed | error: %v", err)
		return
	}

	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		log.Fatalf("ListenTCP error: %v", err)
		return
	}

	log.Infof("Follower %d: RPC server listening on %s", myServerID, myAddr)

	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		rpc.Accept(listener)
	}()

	select {
	case <-followerStop:
		_ = listener.Close()
		<-acceptDone
		return
	case <-acceptDone:
		return
	}
}

func initMongoDB() {
	gob.Register([]mongodb.Query{})

	// Use server ID as DB suffix in all modes to avoid collisions across replicas.
	mongoDbFollower = mongodb.NewMongoFollower(mongoClientNum, int(1), myServerID)

	queriesToLoad, err := mongodb.ReadQueryFromFile(mongodb.DataPath + "workload.dat")
	if err != nil {
		log.Errorf("getting load data failed | error: %v", err)
		return
	}

	err = mongoDbFollower.ClearTable("usertable")
	if err != nil {
		log.Errorf("clean up table failed | err: %v", err)
		return
	}

	log.Debugf("loading data to Mongo DB")
	_, _, err = mongoDbFollower.FollowerAPI(queriesToLoad)
	if err != nil {
		log.Errorf("load data failed | error: %v", err)
		return
	}

	log.Infof("mongo DB initialization done")
}

// mongoDBCleanUp cleans up client connections to DB upon ctrl+C
func mongoDBCleanUp() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Debugf("clean up MongoDb follower")
		err := mongoDbFollower.CleanUp()
		if err != nil {
			log.Errorf("clean up MongoDB follower failed | err: %v", err)
			return
		}
		log.Infof("clean up MongoDB follower succeeded")
	}()
}
