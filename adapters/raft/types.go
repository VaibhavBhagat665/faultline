// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"time"

	"github.com/VaibhavBhagat665/faultline/internal/sim"
)

// Role represents a Raft node's role.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// NodeID uniquely identifies a node in the cluster.
type NodeID string

// TimerID identifies a timer.
type TimerID string

const (
	ElectionTimer  TimerID = "election"
	HeartbeatTimer TimerID = "heartbeat"
)

// LogEntry is a single entry in the replicated log.
type LogEntry struct {
	Term    uint64 // term when entry was received by leader
	Index   uint64 // position in log (1-indexed)
	Command []byte // opaque command bytes
}

// Config holds Raft configuration parameters.
type Config struct {
	// Election timeout range: [Min, Max)
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// Heartbeat interval (must be << election timeout)
	HeartbeatInterval time.Duration

	// Max entries per AppendEntries RPC (0 = unlimited)
	MaxEntriesPerRPC int
}

// DefaultConfig returns a reasonable default configuration.
func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		MaxEntriesPerRPC:   100,
	}
}

// Request represents a client request.
type Request struct {
	ClientID uint64 // unique client identifier
	Seq      uint64 // client-side sequence number
	Command  []byte // opaque command bytes
}

// Response represents a reply to a client request.
type Response struct {
	Success bool   // true if command was applied
	Value   []byte // result from state machine (if applicable)
	Leader  NodeID // current leader (redirect hint if not leader)
}

// NodeState exposes internal Raft state for introspection (invariant checking).
type NodeState struct {
	Role        Role
	Term        uint64
	VotedFor    *NodeID
	Log         []LogEntry
	CommitIndex uint64
	Applied     uint64
	Leader      *NodeID // current leader (if known)
}

// Env is the environment interface provided by the simulation engine.
// It abstracts time, networking, disk, randomness, and logging.
type Env interface {
	Now() sim.Time
	SetTimer(d time.Duration, id TimerID)
	CancelTimer(id TimerID)
	Send(to NodeID, payload []byte)
	Disk() Disk
	Rand() Rand
	Logf(format string, args ...interface{})
}

// Rand wraps a random number generator with deterministic streams.
type Rand interface {
	Intn(n int) int
	Uint64() uint64
}

// Disk represents persistent storage with crash semantics.
type Disk interface {
	// Write appends data to a buffer (not yet durable).
	Write(key string, data []byte) error

	// Sync forces buffered writes to durable storage.
	Sync() error

	// Read retrieves data from disk.
	Read(key string) ([]byte, error)
}

// Node is the interface a Raft node must implement.
type Node interface {
	// Init initializes the node with its ID, peer list, and environment.
	Init(id NodeID, peers []NodeID, env Env) error

	// Recover restores state from disk after a crash.
	Recover() error

	// OnMessage handles an incoming message.
	OnMessage(from NodeID, payload []byte)

	// OnTimer handles a timer firing.
	OnTimer(id TimerID)

	// OnClientRequest handles a client request.
	// The reply callback is invoked when the request completes.
	OnClientRequest(req Request, reply func(Response))
}

// Introspector allows external observers to inspect internal Raft state.
type Introspector interface {
	State() NodeState
}
