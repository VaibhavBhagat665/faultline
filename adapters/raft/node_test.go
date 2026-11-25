// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/internal/sim"
)

// mockEnv is a test double for the Env interface.
type mockEnv struct {
	now     sim.Time
	timers  map[TimerID]time.Duration
	sent    []sentMessage
	disk    Disk
	rand    Rand
	logs    []string
}

type sentMessage struct {
	to      NodeID
	payload []byte
}

func newMockEnv() *mockEnv {
	return &mockEnv{
		now:    0,
		timers: make(map[TimerID]time.Duration),
		sent:   []sentMessage{},
		disk:   newMockDisk(),
		rand:   newMockRand(12345),
		logs:   []string{},
	}
}

func (m *mockEnv) Now() sim.Time {
	return m.now
}

func (m *mockEnv) SetTimer(d time.Duration, id TimerID) {
	m.timers[id] = d
}

func (m *mockEnv) CancelTimer(id TimerID) {
	delete(m.timers, id)
}

func (m *mockEnv) Send(to NodeID, payload []byte) {
	m.sent = append(m.sent, sentMessage{to: to, payload: payload})
}

func (m *mockEnv) Disk() Disk {
	return m.disk
}

func (m *mockEnv) Rand() Rand {
	return m.rand
}

func (m *mockEnv) Logf(format string, args ...interface{}) {
	m.logs = append(m.logs, format)
}

// mockDisk is a test double for the Disk interface.
type mockDisk struct {
	data map[string][]byte
}

func newMockDisk() *mockDisk {
	return &mockDisk{
		data: make(map[string][]byte),
	}
}

func (d *mockDisk) Write(key string, data []byte) error {
	d.data[key] = data
	return nil
}

func (d *mockDisk) Sync() error {
	return nil
}

func (d *mockDisk) Read(key string) ([]byte, error) {
	if data, ok := d.data[key]; ok {
		return data, nil
	}
	return nil, ErrKeyNotFound
}

// ErrKeyNotFound is returned when a key doesn't exist in the disk.
var ErrKeyNotFound = &diskError{"key not found"}

type diskError struct {
	msg string
}

func (e *diskError) Error() string {
	return e.msg
}

// mockRand is a test double for the Rand interface.
type mockRand struct {
	seed   uint64
	callNo int
}

func newMockRand(seed uint64) *mockRand {
	return &mockRand{seed: seed}
}

func (r *mockRand) Intn(n int) int {
	r.callNo++
	// Simple LCG for determinism
	r.seed = (r.seed*1103515245 + 12345) & 0x7fffffff
	return int(r.seed % uint64(n))
}

func (r *mockRand) Uint64() uint64 {
	r.callNo++
	r.seed = (r.seed*1103515245 + 12345) & 0x7fffffff
	return r.seed
}

// TestNodeInit verifies node initialization.
func TestNodeInit(t *testing.T) {
	cfg := DefaultConfig()
	node := NewRaftNode(cfg)
	env := newMockEnv()

	if err := node.Init("node1", []NodeID{"node2", "node3"}, env); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	state := node.State()
	if state.Role != Follower {
		t.Errorf("Expected role Follower, got %v", state.Role)
	}
	if state.Term != 0 {
		t.Errorf("Expected term 0, got %d", state.Term)
	}
	if len(state.Log) != 1 {
		t.Errorf("Expected log length 1 (dummy entry), got %d", len(state.Log))
	}
	if _, ok := env.timers[ElectionTimer]; !ok {
		t.Error("Election timer not set")
	}
}

// TestElectionTimeout verifies that election timeout triggers an election.
func TestElectionTimeout(t *testing.T) {
	cfg := DefaultConfig()
	node := NewRaftNode(cfg)
	env := newMockEnv()

	if err := node.Init("node1", []NodeID{"node2", "node3"}, env); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Trigger election timeout
	node.OnTimer(ElectionTimer)

	state := node.State()
	if state.Role != Candidate {
		t.Errorf("Expected role Candidate after election timeout, got %v", state.Role)
	}
	if state.Term != 1 {
		t.Errorf("Expected term 1 after election, got %d", state.Term)
	}
	if state.VotedFor == nil || *state.VotedFor != "node1" {
		t.Errorf("Expected to vote for self, got %v", state.VotedFor)
	}

	// Should have sent RequestVote to both peers
	if len(env.sent) != 2 {
		t.Errorf("Expected 2 RequestVote messages, got %d", len(env.sent))
	}
}

// TestBecomeLeader verifies that a candidate becomes leader with majority votes.
func TestBecomeLeader(t *testing.T) {
	cfg := DefaultConfig()
	node := NewRaftNode(cfg)
	env := newMockEnv()

	if err := node.Init("node1", []NodeID{"node2", "node3"}, env); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Trigger election
	node.OnTimer(ElectionTimer)

	// Simulate receiving a vote from node2
	reply := RequestVoteReply{
		Term:        1,
		VoteGranted: true,
	}
	payload, err := EncodeMessage(RPCRequestVoteReply, reply)
	if err != nil {
		t.Fatalf("Failed to encode reply: %v", err)
	}
	node.OnMessage("node2", payload)

	// Should now be leader (2/3 votes: self + node2)
	state := node.State()
	if state.Role != Leader {
		t.Errorf("Expected role Leader, got %v", state.Role)
	}

	// Should have heartbeat timer set
	if _, ok := env.timers[HeartbeatTimer]; !ok {
		t.Error("Heartbeat timer not set")
	}

	// Election timer should be canceled
	if _, ok := env.timers[ElectionTimer]; ok {
		t.Error("Election timer should be canceled")
	}
}

// TestFollowerResetsElectionTimerOnHeartbeat verifies followers reset election timeout on heartbeat.
func TestFollowerResetsElectionTimerOnHeartbeat(t *testing.T) {
	cfg := DefaultConfig()
	node := NewRaftNode(cfg)
	env := newMockEnv()

	if err := node.Init("node1", []NodeID{"node2", "node3"}, env); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Clear initial timer
	env.timers = make(map[TimerID]time.Duration)

	// Receive heartbeat from leader
	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	payload, err := EncodeMessage(RPCAppendEntries, args)
	if err != nil {
		t.Fatalf("Failed to encode AppendEntries: %v", err)
	}
	node.OnMessage("node2", payload)

	// Should have reset election timer
	if _, ok := env.timers[ElectionTimer]; !ok {
		t.Error("Election timer should be reset after heartbeat")
	}

	// Should remain follower
	state := node.State()
	if state.Role != Follower {
		t.Errorf("Expected role Follower, got %v", state.Role)
	}
}
