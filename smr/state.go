package smr

import (
	"sync"
	"time"
)

type ServerState struct {
	sync.RWMutex
	myID     int
	leaderID int
	term     int
	votedFor bool
	logIndex int
	cmtIndex int
	// map of object id -> object state for per-object commits
	Objects map[string]*ObjectState
}

func NewServerState() ServerState {
	return ServerState{
		myID:     0,
		term:     0,
		votedFor: false,
		logIndex: 0,
		cmtIndex: 0,
		Objects:  make(map[string]*ObjectState),
	}
}

func (s *ServerState) SetMyServerID(id int) {
	s.Lock()
	defer s.Unlock()

	s.myID = id
}

func (s *ServerState) GetMyServerID() int {
	s.RLock()
	defer s.RUnlock()

	return s.myID
}

func (s *ServerState) SetLeaderID(id int) {
	s.Lock()
	defer s.Unlock()

	s.leaderID = id
}

func (s *ServerState) GetLeaderID() int {
	s.RLock()
	defer s.RUnlock()

	return s.leaderID
}

func (s *ServerState) SetTerm(term int) {
	s.Lock()
	defer s.Unlock()

	s.term = term
}

func (s *ServerState) GetTerm() int {
	s.RLock()
	defer s.RUnlock()

	return s.term
}

func (s *ServerState) ResetVotedFor() {
	s.Lock()
	defer s.Unlock()

	s.votedFor = false
}

func (s *ServerState) CheckVotedFor() bool {
	s.RLock()
	defer s.RUnlock()

	return s.votedFor
}

func (s *ServerState) SyncLogIndex(logIndex int) {
	s.Lock()
	defer s.Unlock()

	s.logIndex = logIndex
}

func (s *ServerState) GetLogIndex() int {
	s.RLock()
	defer s.RUnlock()

	return s.logIndex
}

func (s *ServerState) AddLogIndex(n int) {
	s.Lock()
	defer s.Unlock()

	s.logIndex += n
}

func (s *ServerState) SyncCommitIndex(commitIndex int) {
	s.Lock()
	defer s.Unlock()

	s.cmtIndex = commitIndex
}

func (s *ServerState) GetCommitIndex() int {
	s.RLock()
	defer s.RUnlock()

	return s.cmtIndex
}

func (s *ServerState) AddCommitIndex(n int) {
	s.Lock()
	defer s.Unlock()

	s.cmtIndex += n
}

// ---------------- Object state & helpers ----------------
// ObjectState holds per-object metadata used by the consensus layer
type ObjectState struct {
	sync.Mutex
	ID               string
	ObjType          int
	Value            interface{}
	LastCommitType   string
	LastCommitTime   time.Time
	LastCommittedOpID string
	LastProposer     int
}

// AddObject creates a simple object entry (no weight/quorum info here)
func (s *ServerState) AddObject(objID string, objType int, _numReplicas int) {
	s.Lock()
	defer s.Unlock()
	if s.Objects == nil {
		s.Objects = make(map[string]*ObjectState)
	}
	if _, exists := s.Objects[objID]; exists {
		return
	}
	o := &ObjectState{
		ID:      objID,
		ObjType: objType,
	}
	s.Objects[objID] = o
}

// UpdateObjectCommit records a committed value for an object and metadata
func (s *ServerState) UpdateObjectCommit(objID string, proposer int, value interface{}, pathUsed string) {
	s.Lock()
	if s.Objects == nil {
		s.Objects = make(map[string]*ObjectState)
	}
	obj, ok := s.Objects[objID]
	if !ok {
		obj = &ObjectState{
			ID:      objID,
			ObjType: 0,
		}
		s.Objects[objID] = obj
	}
	s.Unlock() // release map lock before touching the object (Bug #6 fix)
	// Safe because s.Objects entries are never removed/replaced — only their fields are mutated.
	// Lock ordering is s.Lock then obj.Lock; this pattern avoids deadlock.

	obj.Lock()
	obj.LastProposer = proposer
	obj.Value = value
	obj.LastCommitType = pathUsed
	obj.LastCommitTime = time.Now()
	obj.Unlock()
}

// GetObject returns the ObjectState pointer for a given id (may be nil)
func (s *ServerState) GetObject(objID string) *ObjectState {
	s.RLock()
	defer s.RUnlock()
	if s.Objects == nil {
		return nil
	}
	return s.Objects[objID]
}
