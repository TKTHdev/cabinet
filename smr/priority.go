package smr

import (
	"errors"
	"sync"
)

type PriorityState struct {
	sync.RWMutex
	PrioClock int
	PrioVal   float64
	Majority  float64
}

// NewServerPriority creates a new PriorityState.
// initPrioClock is used to initialize the clock.
// initPrioVal is currently unused in practice (kept for API compatibility).
// The actual priority value is set via UpdatePriority() or directly configured per role.
// For the leader, the priority is set to pscheme[0] in runServerRole.
// For followers, it's set during initialization in runFollower.
func NewServerPriority(initPrioClock int, initPrioVal float64) PriorityState {
	return PriorityState{
		PrioClock: initPrioClock,
		PrioVal:   initPrioVal,
	}
}

func (p *PriorityState) UpdatePriority(newPClock int, newPriority float64) error {
	p.Lock()
	defer p.Unlock()

	// Bug #3 fix: Accept updates for any clock >= 0.
	// With pipelined consensus (maxPipeline=50), followers can receive out-of-order RPCs.
	// A follower may process pclock 45 then receive pclock 30 — both are valid.
	// Always accept if newPClock is more recent, silently drop if stale.
	if newPClock < 0 {
		return errors.New("newPClock must be non-negative")
	}
	if newPClock >= p.PrioClock {
		p.PrioClock = newPClock
		p.PrioVal = newPriority
	}
	// Silently drop stale updates (newPClock < p.PrioClock) rather than erroring,
	// since the follower already has a more recent weight.
	return nil
}

func (p *PriorityState) GetPriority() (pClock int, pValue float64) {
	p.RLock()
	defer p.RUnlock()

	pClock = p.PrioClock
	pValue = p.PrioVal
	return
}

func (p *PriorityState) SetMajority(m float64) {
	p.Lock()
	defer p.Unlock()

	p.Majority = m
}

func (p *PriorityState) GetMajority() float64 {
	p.RLock()
	defer p.RUnlock()

	return p.Majority
}

func (p *PriorityState) GetPrioVal() float64 {
	p.RLock()
	defer p.RUnlock()

	return p.PrioVal
}
