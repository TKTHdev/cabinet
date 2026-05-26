package smr

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

type serverID = int
type prioClock = int
type priority = float64

type PriorityManager struct {
	sync.RWMutex
	m        map[prioClock]map[serverID]priority
	scheme   []priority
	majority float64
	n        int
	q        int
}

func (pm *PriorityManager) Init(numOfServers, quorumSize, baseOfPriorities int, ratioTryStep float64, isCab bool) {
	pm.n = numOfServers
	pm.q = quorumSize // quorum size is t+1
	pm.m = make(map[prioClock]map[serverID]priority)
	pm.scheme = []priority{}

	ratio := 1.0
	if isCab {
		var err error
		ratio, err = calcInitPrioRatio(numOfServers, quorumSize, ratioTryStep)
		if err != nil {
			panic(fmt.Sprintf("PriorityManager.Init: %v", err))
		}
	}
	fmt.Println("ratio: ", ratio)

	newPriorities := make(map[serverID]priority)

	for i := 0; i < numOfServers; i++ {
		p := float64(baseOfPriorities) * math.Pow(ratio, float64(i))
		newPriorities[numOfServers-1-i] = p
		pm.scheme = append(pm.scheme, p)
	}

	reverseSlice(pm.scheme)

	pm.majority = sum(pm.scheme) / 2

	pm.Lock()
	pm.m[0] = newPriorities
	pm.Unlock()
	return
}

func (pm *PriorityManager) SetNewPrioritiesUnderNewT(n, q, baseOfPriorities int, ratioTryStep float64, pClock prioClock) (newPriorities map[serverID]priority) {
	pm.n = n
	pm.q = q // quorum size is t+1
	pm.m = make(map[prioClock]map[serverID]priority)

	ratio, err := calcInitPrioRatio(n, q, ratioTryStep)
	if err != nil {
		panic(fmt.Sprintf("PriorityManager.SetNewPrioritiesUnderNewT: %v", err))
	}

	fmt.Println("ratio: ", ratio)

	newPriorities = make(map[serverID]priority)

	// reset pm scheme
	pm.scheme = []priority{}

	for i := 0; i < n; i++ {
		p := float64(baseOfPriorities) * math.Pow(ratio, float64(i))
		newPriorities[n-1-i] = p
		pm.scheme = append(pm.scheme, p)
	}

	fmt.Printf("pm.scheme length: %v\n", len(pm.scheme))
	reverseSlice(pm.scheme)

	pm.majority = sum(pm.scheme) / 2

	pm.Lock()
	pm.m[pClock] = newPriorities
	pm.Unlock()
	return
}

func reverseSlice(slice []priority) {
	length := len(slice)
	for i := 0; i < length/2; i++ {
		j := length - 1 - i
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func calcInitPrioRatio(n, f int, ratioTryStep float64) (float64, error) {
	if ratioTryStep <= 0 {
		return 0, fmt.Errorf("ratioTryStep must be > 0, got %f", ratioTryStep)
	}

	r := 2.0 // initial guess
	for r > 1.0 {
		if math.Pow(r, float64(n-f+1)) > 0.5*(math.Pow(r, float64(n))+1) && 0.5*(math.Pow(r, float64(n))+1) > math.Pow(r, float64(n-f)) {
			return r, nil
		}
		r -= ratioTryStep
	}

	return 0, fmt.Errorf("no valid initial priority ratio found for n=%d f=%d step=%f", n, f, ratioTryStep)
}

func sum(arr []float64) float64 {
	total := 0.0
	for _, val := range arr {
		total += val
	}
	return total
}

func (pm *PriorityManager) UpdateFollowerPriorities(pClock prioClock, prioQueue chan serverID, leaderID serverID) error {

	newPriorities := make(map[serverID]priority)
	arranged := make(map[serverID]bool)

	for i := 0; i < pm.n; i++ {
		arranged[i] = false
	}

	nr := len(prioQueue)

	for i := 0; i < nr; i++ {
		s := <-prioQueue
		if i+1 >= len(pm.scheme) {
			err := fmt.Sprintf("priority queue size [%v] exceeds scheme follower capacity [%v]", nr, len(pm.scheme)-1)
			return errors.New(err)
		}
		// skip leader
		newPriorities[s] = pm.scheme[i+1]

		arranged[s] = true

		//fmt.Printf("pc: %d | processing %d is done | i is: %d | arranged %+v \n ", pClock, s, i, arranged)
	}

	i := nr + 1
	newPriorities[leaderID] = pm.scheme[0]

	unresponded := make([]int, 0, pm.n)
	for id, done := range arranged {
		if !done && id != leaderID {
			unresponded = append(unresponded, id)
		}
	}
	sort.Ints(unresponded)

	for _, id := range unresponded {
		if i >= len(pm.scheme) { // Bug #4 fix: changed == to >= for proper bounds checking
			err := fmt.Sprintf("priority assignment of [%v] exceeds pm scheme length [%v]", i, len(pm.scheme))
			return errors.New(err)
		}
		newPriorities[id] = pm.scheme[i]
		i++
	}

	pm.Lock()
	pm.m[pClock] = newPriorities
	if pClock > 10 {
		delete(pm.m, pClock-10)
	}
	pm.Unlock()
	//fmt.Printf("newPriorities: %+v\n", newPriorities)
	return nil
}

func (pm *PriorityManager) GetFollowerPriorities(pClock int) (fpriorities map[serverID]priority) {
	fpriorities = make(map[serverID]priority)

	pm.RLock()
	defer pm.RUnlock()

	if src, ok := pm.m[pClock]; ok {
		for sid, prio := range src {
			fpriorities[sid] = prio
		}
	}
	return
}

func (pm *PriorityManager) GetMajority() (majority float64) {
	majority = pm.majority
	return
}

func (pm *PriorityManager) GetPriorityScheme() (scheme []priority) {
	scheme = pm.scheme
	return
}

func (pm *PriorityManager) GetQuorumSize() (q int) {
	q = pm.q
	return
}
