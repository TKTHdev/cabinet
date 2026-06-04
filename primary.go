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

	// Dial every peer concurrently. Dialing sequentially meant one slow or
	// not-yet-listening peer blocked all the peers behind it: with N peers and
	// a ~9s per-peer retry budget the leader could spend up to (N-1)*9s in here
	// before startSyncCabInstanceWithClients() (the client-serving loop) runs.
	// At 11 nodes that exceeds the eval window, so the leader never served a
	// single client request. Parallel dialing bounds the wait to one peer's
	// retry budget (~9s) regardless of cluster size.
	var wg sync.WaitGroup
	for i := 0; i < numOfServers; i++ {
		if i == myServerID {
			continue
		}

		serverID, err := strconv.Atoi(serverConfig[i][id])
		if err != nil {
			log.Errorf("%v", err)
			continue // Skip this malformed entry but keep connecting to others.
		}

		addr := serverConfig[i][ip] + ":" + serverConfig[i][portOfRPCListener]

		wg.Add(1)
		go func(peerIdx, peerID int, peerAddr string) {
			defer wg.Done()

			log.Infof("Connecting to server %d at %v", peerIdx, peerAddr)

			// Retry connection with backoff.
			var txClient *rpc.Client
			var err error
			for retry := 0; retry < 10; retry++ {
				txClient, err = rpc.Dial("tcp", peerAddr)
				if err == nil {
					break
				}
				if retry < 9 {
					log.Debugf("Connection to %v failed (attempt %d/10), retrying...", peerAddr, retry+1)
					time.Sleep(time.Second)
				}
			}
			if err != nil {
				log.Errorf("txClient rpc.Dial failed to %v after 10 attempts | error: %v", peerAddr, err)
				return // Skip this server but continue with others.
			}

			newServer := &ServerDock{
				serverID: peerID,
				addr:     peerAddr,
				txClient: txClient,
				jobQMu:   sync.RWMutex{},
				jobQ:     map[prioClock]chan struct{}{},
			}

			log.Infof("txClient connected to server %d at %v", peerIdx, peerAddr)

			conns.Lock()
			conns.m[peerID] = newServer
			conns.Unlock()
		}(i, serverID, addr)
	}

	wg.Wait()

	conns.RLock()
	connected := len(conns.m)
	conns.RUnlock()
	log.Infof("Successfully established connections to %d followers", connected)
}
