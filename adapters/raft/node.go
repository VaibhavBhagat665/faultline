// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/VaibhavBhagat665/faultline/internal/bugs"
)

// RaftNode implements the Raft consensus algorithm.
type RaftNode struct {
	// Configuration
	id     NodeID
	peers  []NodeID
	config Config
	env    Env

	// Persistent state (must be synced to disk before replying to RPCs)
	currentTerm uint64
	votedFor    *NodeID
	log         []LogEntry // log[0] is dummy entry at index 0

	// Volatile state (all nodes)
	role        Role
	commitIndex uint64
	lastApplied uint64

	// Volatile state (leaders only)
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	// Leader tracking
	currentLeader *NodeID

	// Client request tracking (for replies)
	pendingRequests map[uint64]pendingRequest // keyed by log index

	// Session deduplication (clientID -> last applied seq and result)
	sessions map[uint64]session

	// Vote tracking (for candidates)
	votesReceived map[NodeID]bool

	// Bug injection (optional, for testing)
	bugInjector *bugs.Injector
}

type pendingRequest struct {
	req   Request
	reply func(Response)
}

type session struct {
	lastSeq uint64
	result  []byte
}

// persistentState captures the state that must be durably stored.
type persistentState struct {
	CurrentTerm uint64      `json:"currentTerm"`
	VotedFor    *NodeID     `json:"votedFor"`
	Log         []LogEntry  `json:"log"`
}

// NewRaftNode creates a new Raft node with the given configuration.
func NewRaftNode(cfg Config) *RaftNode {
	return &RaftNode{
		config:          cfg,
		log:             make([]LogEntry, 1), // log[0] is dummy
		nextIndex:       make(map[NodeID]uint64),
		matchIndex:      make(map[NodeID]uint64),
		pendingRequests: make(map[uint64]pendingRequest),
		sessions:        make(map[uint64]session),
		votesReceived:   make(map[NodeID]bool),
	}
}

// Init initializes the node with its ID, peer list, and environment.
func (n *RaftNode) Init(id NodeID, peers []NodeID, env Env) error {
	n.id = id
	n.peers = peers
	n.env = env

	// Try to recover from disk
	if err := n.Recover(); err != nil {
		// No state on disk; start fresh
		n.currentTerm = 0
		n.votedFor = nil
		n.log = []LogEntry{{Term: 0, Index: 0}} // dummy entry
		n.commitIndex = 0
		n.lastApplied = 0
		if err := n.persistState(); err != nil {
			return fmt.Errorf("failed to persist initial state: %w", err)
		}
	}

	// Start as follower
	n.becomeFollower(n.currentTerm, nil)
	return nil
}

// Recover restores state from disk after a crash.
func (n *RaftNode) Recover() error {
	data, err := n.env.Disk().Read("raft_state")
	if err != nil {
		return err
	}

	var ps persistentState
	if err := json.Unmarshal(data, &ps); err != nil {
		return fmt.Errorf("failed to unmarshal persistent state: %w", err)
	}

	n.currentTerm = ps.CurrentTerm
	n.votedFor = ps.VotedFor
	n.log = ps.Log
	if len(n.log) == 0 {
		n.log = []LogEntry{{Term: 0, Index: 0}} // ensure dummy entry
	}
	n.commitIndex = 0 // recomputed on next AppendEntries
	n.lastApplied = 0

	n.env.Logf("[%s] Recovered from disk: term=%d votedFor=%v logLen=%d", n.id, n.currentTerm, n.votedFor, len(n.log))
	return nil
}

// OnMessage handles an incoming message.
func (n *RaftNode) OnMessage(from NodeID, payload []byte) {
	msg, err := DecodeMessage(payload)
	if err != nil {
		n.env.Logf("[%s] Failed to decode message from %s: %v", n.id, from, err)
		return
	}

	switch msg.Type {
	case RPCRequestVote:
		args, err := DecodeRequestVote(msg.Payload)
		if err != nil {
			n.env.Logf("[%s] Failed to decode RequestVote: %v", n.id, err)
			return
		}
		n.handleRequestVote(from, args)
	case RPCRequestVoteReply:
		reply, err := DecodeRequestVoteReply(msg.Payload)
		if err != nil {
			n.env.Logf("[%s] Failed to decode RequestVoteReply: %v", n.id, err)
			return
		}
		n.handleRequestVoteReply(from, reply)
	case RPCAppendEntries:
		args, err := DecodeAppendEntries(msg.Payload)
		if err != nil {
			n.env.Logf("[%s] Failed to decode AppendEntries: %v", n.id, err)
			return
		}
		n.handleAppendEntries(from, args)
	case RPCAppendEntriesReply:
		reply, err := DecodeAppendEntriesReply(msg.Payload)
		if err != nil {
			n.env.Logf("[%s] Failed to decode AppendEntriesReply: %v", n.id, err)
			return
		}
		n.handleAppendEntriesReply(from, reply)
	default:
		n.env.Logf("[%s] Unknown message type: %s", n.id, msg.Type)
	}
}

// OnTimer handles a timer firing.
func (n *RaftNode) OnTimer(id TimerID) {
	switch id {
	case ElectionTimer:
		n.startElection()
	case HeartbeatTimer:
		if n.role == Leader {
			n.sendHeartbeats()
			n.resetHeartbeatTimer()
		}
	}
}

// OnClientRequest handles a client request.
func (n *RaftNode) OnClientRequest(req Request, reply func(Response)) {
	if n.role != Leader {
		reply(Response{Success: false, Leader: n.currentLeaderID()})
		return
	}

	// Check for duplicate request (session deduplication)
	if sess, ok := n.sessions[req.ClientID]; ok && req.Seq <= sess.lastSeq {
		// Already applied; return cached result
		reply(Response{Success: true, Value: sess.result, Leader: n.id})
		return
	}

	// Append to log
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   uint64(len(n.log)),
		Command: req.Command,
	}
	n.log = append(n.log, entry)
	n.pendingRequests[entry.Index] = pendingRequest{req: req, reply: reply}

	if err := n.persistState(); err != nil {
		n.env.Logf("[%s] Failed to persist log entry: %v", n.id, err)
		reply(Response{Success: false, Leader: n.id})
		return
	}

	n.env.Logf("[%s] Appended entry at index %d", n.id, entry.Index)

	// Replicate to followers immediately
	n.sendAppendEntries()
}

// State returns the current Raft state for introspection.
func (n *RaftNode) State() NodeState {
	// BUG: StaleRead - followers return state without checking leadership
	// In a correct implementation, reads should use ReadIndex or lease mechanism
	if n.shouldInjectBug(bugs.BugStaleRead) && n.role != Leader {
		n.env.Logf("[%s] BUG.StaleRead: Serving read from follower without leadership check (term=%d, commitIndex=%d)",
			n.id, n.currentTerm, n.commitIndex)
		// Return potentially stale state
	}
	
	return NodeState{
		Role:        n.role,
		Term:        n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         append([]LogEntry(nil), n.log...),
		CommitIndex: n.commitIndex,
		Applied:     n.lastApplied,
		Leader:      n.currentLeader,
	}
}

// persistState writes (currentTerm, votedFor, log) to disk.
func (n *RaftNode) persistState() error {
	ps := persistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}
	data, err := json.Marshal(ps)
	if err != nil {
		return err
	}
	if err := n.env.Disk().Write("raft_state", data); err != nil {
		return err
	}
	return n.env.Disk().Sync()
}

// becomeFollower transitions to follower state.
func (n *RaftNode) becomeFollower(term uint64, leader *NodeID) {
	n.role = Follower
	n.currentTerm = term
	n.currentLeader = leader
	n.resetElectionTimer()
	n.env.Logf("[%s] Became Follower (term=%d)", n.id, term)
}

// becomeCandidate transitions to candidate state.
func (n *RaftNode) becomeCandidate() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = &n.id
	n.currentLeader = nil
	n.votesReceived = make(map[NodeID]bool)
	n.votesReceived[n.id] = true // vote for self
	n.resetElectionTimer()
	if err := n.persistState(); err != nil {
		n.env.Logf("[%s] Failed to persist candidate state: %v", n.id, err)
	}
	n.env.Logf("[%s] Became Candidate (term=%d)", n.id, n.currentTerm)
}

// becomeLeader transitions to leader state.
func (n *RaftNode) becomeLeader() {
	n.role = Leader
	n.currentLeader = &n.id

	// Initialize nextIndex and matchIndex
	for _, peer := range n.peers {
		n.nextIndex[peer] = uint64(len(n.log))
		n.matchIndex[peer] = 0
	}

	n.env.CancelTimer(ElectionTimer)
	n.resetHeartbeatTimer()
	n.env.Logf("[%s] Became Leader (term=%d)", n.id, n.currentTerm)

	// Send initial heartbeats
	n.sendHeartbeats()
}

// resetElectionTimer sets a new randomized election timeout.
func (n *RaftNode) resetElectionTimer() {
	minMs := int(n.config.ElectionTimeoutMin / time.Millisecond)
	maxMs := int(n.config.ElectionTimeoutMax / time.Millisecond)
	timeoutMs := minMs + n.env.Rand().Intn(maxMs-minMs)
	n.env.SetTimer(time.Duration(timeoutMs)*time.Millisecond, ElectionTimer)
}

// resetHeartbeatTimer sets the heartbeat timer.
func (n *RaftNode) resetHeartbeatTimer() {
	n.env.SetTimer(n.config.HeartbeatInterval, HeartbeatTimer)
}

// currentLeaderID returns the current leader (or empty if unknown).
func (n *RaftNode) currentLeaderID() NodeID {
	if n.currentLeader != nil {
		return *n.currentLeader
	}
	return ""
}

// lastLogInfo returns the index and term of the last log entry.
func (n *RaftNode) lastLogInfo() (uint64, uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// SetBugInjector sets the bug injector for testing.
// This allows intentional bugs to be injected for framework validation.
func (n *RaftNode) SetBugInjector(injector *bugs.Injector) {
	n.bugInjector = injector
}

// shouldInjectBug returns true if the given bug should be triggered.
func (n *RaftNode) shouldInjectBug(bug bugs.BugFlag) bool {
	if n.bugInjector == nil {
		return false
	}
	return n.bugInjector.ShouldTrigger(bug, func() float64 {
		return float64(n.env.Rand().Intn(100)) / 100.0
	})
}
