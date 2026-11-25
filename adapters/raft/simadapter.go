// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/VaibhavBhagat665/faultline/internal/sim"
	"github.com/VaibhavBhagat665/faultline/internal/simdisk"
)

// SimAdapter adapts a RaftNode to the simulation environment.
type SimAdapter struct {
	node   *RaftNode
	id     NodeID
	kvstore *simdisk.KVStore
	rand   *rand.Rand
	engine SimEngine
}

// SimEngine is the subset of the simulation engine interface needed by the adapter.
type SimEngine interface {
	Now() sim.Time
	Logf(format string, args ...interface{})
}

// NewSimAdapter creates a new simulation adapter for a Raft node.
func NewSimAdapter(id NodeID, cfg Config, kvstore *simdisk.KVStore, rng *rand.Rand, engine SimEngine) *SimAdapter {
	node := NewRaftNode(cfg)
	return &SimAdapter{
		node:   node,
		id:     id,
		kvstore: kvstore,
		rand:   rng,
		engine: engine,
	}
}

// Init initializes the Raft node.
func (a *SimAdapter) Init(peers []NodeID) error {
	return a.node.Init(a.id, peers, a)
}

// OnMessage handles an incoming network message.
func (a *SimAdapter) OnMessage(from NodeID, payload []byte) {
	a.node.OnMessage(from, payload)
}

// OnTimer handles a timer firing.
func (a *SimAdapter) OnTimer(id TimerID) {
	a.node.OnTimer(id)
}

// OnClientRequest handles a client request.
func (a *SimAdapter) OnClientRequest(req Request, reply func(Response)) {
	a.node.OnClientRequest(req, reply)
}

// State returns the current Raft state.
func (a *SimAdapter) State() NodeState {
	return a.node.State()
}

// --- Env interface implementation ---

// Now returns the current simulation time.
func (a *SimAdapter) Now() sim.Time {
	return a.engine.Now()
}

// SetTimer schedules a timer event (simplified for now).
func (a *SimAdapter) SetTimer(d time.Duration, id TimerID) {
	// For now, we'll just log this
	// Full integration with the engine will come later
	a.engine.Logf("[%s] SetTimer(%s, %v)", a.id, id, d)
}

// CancelTimer cancels a timer.
func (a *SimAdapter) CancelTimer(id TimerID) {
	a.engine.Logf("[%s] CancelTimer(%s)", a.id, id)
}

// Send sends a message to another node (simplified for now).
func (a *SimAdapter) Send(to NodeID, payload []byte) {
	// For now, we'll just log this
	// Full network integration will come later
	a.engine.Logf("[%s] Send(to=%s, len=%d)", a.id, to, len(payload))
}

// Disk returns the disk interface.
func (a *SimAdapter) Disk() Disk {
	return &diskAdapter{kvstore: a.kvstore, nodeID: string(a.id)}
}

// Rand returns the random number generator.
func (a *SimAdapter) Rand() Rand {
	return &randAdapter{rng: a.rand}
}

// Logf logs a message.
func (a *SimAdapter) Logf(format string, args ...interface{}) {
	a.engine.Logf(format, args...)
}

// diskAdapter adapts simdisk.KVStore to the Disk interface.
type diskAdapter struct {
	kvstore *simdisk.KVStore
	nodeID string
}

func (d *diskAdapter) Write(key string, data []byte) error {
	// Prefix key with node ID to avoid collisions
	fullKey := fmt.Sprintf("%s:%s", d.nodeID, key)
	return d.kvstore.Write(fullKey, data)
}

func (d *diskAdapter) Sync() error {
	return d.kvstore.Sync()
}

func (d *diskAdapter) Read(key string) ([]byte, error) {
	fullKey := fmt.Sprintf("%s:%s", d.nodeID, key)
	return d.kvstore.Read(fullKey)
}

// randAdapter adapts *rand.Rand to the Rand interface.
type randAdapter struct {
	rng *rand.Rand
}

func (r *randAdapter) Intn(n int) int {
	return r.rng.Intn(n)
}

func (r *randAdapter) Uint64() uint64 {
	return r.rng.Uint64()
}
