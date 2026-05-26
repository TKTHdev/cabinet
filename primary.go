package main

import (
	"cabinet/config"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

func establishRPCs() {
	serverConfig := config.ParseClusterConfig(numOfServers, configPath)
	id := config.ServerID
	ip := config.ServerIP
	portOfRPCListener := config.ServerRPCListenerPort

	for i := 0; i < numOfServers; i++ {
		if i == myServerID {
			continue
		}

		serverID, err := strconv.Atoi(serverConfig[i][id])
		if err != nil {
			log.Errorf("%v", err)
			return
		}

		newServer := &ServerDock{
			serverID:   serverID,
			addr:       serverConfig[i][ip] + ":" + serverConfig[i][portOfRPCListener],
			txClient:   nil,
			jobQMu:     sync.RWMutex{},
			jobQ:       map[prioClock]chan struct{}{},
		}

		log.Infof("Connecting to server %d at %v", i, newServer.addr)

		// Retry connection with backoff
		var txClient *rpc.Client
		for retry := 0; retry < 10; retry++ {
			txClient, err = rpc.Dial("tcp", newServer.addr)
			if err == nil {
				break
			}
			if retry < 9 {
				log.Debugf("Connection to %v failed (attempt %d/10), retrying...", newServer.addr, retry+1)
				time.Sleep(time.Second)
			}
		}
		if err != nil {
			log.Errorf("txClient rpc.Dial failed to %v after 10 attempts | error: %v", newServer.addr, err)
			continue // Skip this server but continue with others
		}

		newServer.txClient = txClient
		log.Infof("txClient connected to server %d at %v", i, newServer.addr)
		
		conns.Lock()
		conns.m[serverID] = newServer
		conns.Unlock()
	}
	
	log.Infof("Successfully established connections to %d followers", len(conns.m))
}
