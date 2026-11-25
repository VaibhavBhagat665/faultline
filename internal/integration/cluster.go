// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
	"github.com/VaibhavBhagat665/faultline/internal/invariants"
	"github.com/VaibhavBhagat665/faultline/internal/sim"
)

// Cluster manages a simulated Raft cluster.
type Cluster struct {
	nodes           map[string]*Node
	currentTime     sim.Time
	disks           map[string]*memoryDisk
	msgChan         map[string][]pendingMessage
	seed            int64
	messageDropRate float64
	messageDelayMin time.Duration
	messageDelayMax time.Duration
	partitionedNodes map[string]bool
	crashedNodes    map[string]bool
	rng             *rand.Rand
	msgCounter      uint64 // monotonic counter for deterministic message drop decisions
	sentCount       uint64
	droppedCount    uint64
	deliveredCount  uint64
	droppedMsgIDs   []uint64
	debugLog        *os.File
	offeredCount       uint64
	reachedDecision    uint64
	droppedByDecision  uint64
	blockedPartition   uint64
	blockedCrashed     uint64
}

// Node wraps a Raft node with its environment.
type Node struct {
	id      raft.NodeID
	raft    *raft.RaftNode
	env     *nodeEnv
	disk    *memoryDisk
	rng     *rand.Rand
}

type pendingMessage struct {
	from    raft.NodeID
	to      raft.NodeID
	payload []byte
	msgID   uint64 // unique ID for deterministic drop decisions
}

// nodeEnv implements the raft.Env interface for a node.
type nodeEnv struct {
	node      *Node
	cluster   *Cluster
	timers    map[raft.TimerID]sim.Time // timerId -> scheduled fire time
	logBuffer []string
}

// NewCluster creates a new simulated cluster.
func NewCluster(seed int64, nodeIDs []string) *Cluster {
	cluster := &Cluster{
		nodes:            make(map[string]*Node),
		currentTime:      0,
		disks:            make(map[string]*memoryDisk),
		msgChan:          make(map[string][]pendingMessage),
		seed:             seed,
		messageDropRate:  0,
		partitionedNodes: make(map[string]bool),
		crashedNodes:     make(map[string]bool),
		rng:              rand.New(rand.NewSource(seed)),
	}

	// Open debug log if FAULTLINE_DEBUG_LOG is set
	if logPath := os.Getenv("FAULTLINE_DEBUG_LOG"); logPath != "" {
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			cluster.debugLog = f
			fmt.Fprintf(f, "# SEED=%d\n", seed)
		}
	}

	// Create nodes
	for _, id := range nodeIDs {
		// Create simple in-memory disk for this node
		disk := &memoryDisk{data: make(map[string][]byte)}

		// Create RNG for this node
		nodeRng := rand.New(rand.NewSource(seed + int64(len(id))))

		// Create node
		node := &Node{
			id:   raft.NodeID(id),
			raft: raft.NewRaftNode(raft.DefaultConfig()),
			disk: disk,
			rng:  nodeRng,
		}

		// Create environment
		env := &nodeEnv{
			node:      node,
			cluster:   cluster,
			timers:    make(map[raft.TimerID]sim.Time),
			logBuffer: []string{},
		}
		node.env = env

		cluster.nodes[id] = node
		cluster.disks[id] = disk
		cluster.msgChan[id] = []pendingMessage{}
	}

	return cluster
}

// Init initializes all nodes in the cluster.
func (c *Cluster) Init() error {
	// Build peer lists - sort allIDs for determinism
	allIDs := make([]string, 0, len(c.nodes))
	for id := range c.nodes {
		allIDs = append(allIDs, id)
	}
	sort.Strings(allIDs)

	// Initialize each node
	for id, node := range c.nodes {
		peers := []raft.NodeID{}
		for _, peerId := range allIDs {
			if peerId != id {
				peers = append(peers, raft.NodeID(peerId))
			}
		}
		if err := node.raft.Init(node.id, peers, node.env); err != nil {
			return fmt.Errorf("failed to init node %s: %w", id, err)
		}
	}

	return nil
}

// TriggerElection triggers an election timeout on the specified node.
func (c *Cluster) TriggerElection(nodeID string) {
	if node, ok := c.nodes[nodeID]; ok {
		node.raft.OnTimer(raft.ElectionTimer)
	}
}

// StepUntilStable processes events until the cluster is stable (one consistent leader).
func (c *Cluster) StepUntilStable(maxTime sim.Time, maxEvents int) (string, uint64, int) {
	eventsProcessed := 0
	var lastLeader string
	var lastTerm uint64
	stableCount := 0
	checkInterval := sim.Time(100 * time.Millisecond.Nanoseconds())
	
	for eventsProcessed < maxEvents && c.currentTime < maxTime {
		// Process events in small time chunks
		chunkEnd := c.currentTime + checkInterval
		if chunkEnd > maxTime {
			chunkEnd = maxTime
		}
		
		batch := c.Step(chunkEnd, 100)
		if batch == 0 {
			// No events in this chunk, advance time
			c.currentTime = chunkEnd
		}
		eventsProcessed += batch
		
		// Check current leader
		currentLeader, currentTerm := c.GetLeader()
		
		// Check if leader is stable
		if currentLeader == lastLeader && currentTerm == lastTerm && currentLeader != "" {
			stableCount++
			if stableCount >= 5 {
				// Leader has been stable for 5 checks (500ms)
				return currentLeader, currentTerm, eventsProcessed
			}
		} else {
			stableCount = 0
		}
		
		lastLeader = currentLeader
		lastTerm = currentTerm
	}
	
	return lastLeader, lastTerm, eventsProcessed
}

// RunFor processes events for a specific duration of simulated time.
func (c *Cluster) RunFor(duration time.Duration, maxEvents int) int {
	endTime := c.currentTime + sim.Time(duration.Nanoseconds())
	return c.Step(endTime, maxEvents)
}

// hasMessages checks if there are any pending messages.

// Step processes events until the specified time or max events.
// Events are processed in strict time order to maintain causality.
func (c *Cluster) Step(maxTime sim.Time, maxEvents int) int {
	eventsProcessed := 0

	for eventsProcessed < maxEvents && c.currentTime < maxTime {
		// Find the next event (message or timer) based on time
		nextTimer := c.findNextTimer()
		hasMessage := c.hasMessages()

		if nextTimer == nil && !hasMessage {
			// No more events
			break
		}

		// If we have both messages and timers, process the one that comes first in time
		// Messages are instant (happen at current time), timers happen at scheduled time
		if hasMessage {
			// Process one message at current time
			if msg := c.findNextMessage(); msg != nil {
				// Skip if target node is crashed
				if !c.crashedNodes[string(msg.to)] {
					if targetNode, ok := c.nodes[string(msg.to)]; ok {
						targetNode.raft.OnMessage(msg.from, msg.payload)
					}
				}
				eventsProcessed++
			}
		} else if nextTimer != nil && nextTimer.fireTime <= maxTime {
			// Advance time to timer and fire it
			if nextTimer.fireTime > c.currentTime {
				c.currentTime = nextTimer.fireTime
			}
			// Skip if node is crashed
			if !c.crashedNodes[string(nextTimer.node.id)] {
				nextTimer.node.raft.OnTimer(nextTimer.timerID)
			}
			delete(nextTimer.node.env.timers, nextTimer.timerID)
			eventsProcessed++
		} else {
			// Next timer is beyond maxTime
			break
		}

		// Prevent infinite loops - process some messages before checking timers again
		if eventsProcessed%10 == 0 {
			// Every 10 events, check if we should fire any timers
			continue
		}
	}

	return eventsProcessed
}

// hasMessages checks if there are any pending messages.
func (c *Cluster) hasMessages() bool {
	for _, messages := range c.msgChan {
		if len(messages) > 0 {
			return true
		}
	}
	return false
}

type timerToFire struct {
	node     *Node
	timerID  raft.TimerID
	fireTime sim.Time
}

// findNextTimer finds the earliest timer that should fire.
func (c *Cluster) findNextTimer() *timerToFire {
	var earliest *timerToFire

	// Iterate nodes in sorted order for determinism
	nodeIDs := make([]string, 0, len(c.nodes))
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	for _, nodeID := range nodeIDs {
		node := c.nodes[nodeID]
		// Iterate timers in sorted order for determinism
		timerIDs := make([]raft.TimerID, 0, len(node.env.timers))
		for timerID := range node.env.timers {
			timerIDs = append(timerIDs, timerID)
		}
		sort.Slice(timerIDs, func(i, j int) bool {
			return timerIDs[i] < timerIDs[j]
		})
		
		for _, timerID := range timerIDs {
			fireTime := node.env.timers[timerID]
			if earliest == nil || fireTime < earliest.fireTime {
				earliest = &timerToFire{
					node:     node,
					timerID:  timerID,
					fireTime: fireTime,
				}
			}
		}
	}

	return earliest
}

// findNextMessage finds the next message to deliver.
func (c *Cluster) findNextMessage() *pendingMessage {
	// Iterate nodes in sorted order for determinism
	nodeIDs := make([]string, 0, len(c.msgChan))
	for nodeID := range c.msgChan {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	
	for _, nodeID := range nodeIDs {
		messages := c.msgChan[nodeID]
		if len(messages) > 0 {
			msg := messages[0]
			msg.to = raft.NodeID(nodeID)
			c.msgChan[nodeID] = messages[1:]
			return &msg
		}
	}
	return nil
}

func (c *Cluster) deliverNextMessage() bool {
	msg := c.findNextMessage()
	if msg == nil {
		return false
	}
	c.deliveredCount++
	if c.debugLog != nil {
		hash := fmt.Sprintf("%x", sha256.Sum256(msg.payload))[:8]
		fmt.Fprintf(c.debugLog, "t=%d DELIVER from=%s to=%s msgID=%d hash=%s\n",
			c.currentTime, msg.from, msg.to, msg.msgID, hash)
	}
	if node, ok := c.nodes[string(msg.to)]; ok {
		node.raft.OnMessage(msg.from, msg.payload)
	}
	return true
}

// CheckInvariants checks all Raft safety invariants.
func (c *Cluster) CheckInvariants() *invariants.Violation {
	states := make(map[string]invariants.NodeState)

	for id, node := range c.nodes {
		s := node.raft.State()
		
		// Convert log
		var log []invariants.LogEntry
		for _, entry := range s.Log {
			log = append(log, invariants.LogEntry{
				Term:    entry.Term,
				Index:   entry.Index,
				Command: entry.Command,
			})
		}
		
		states[id] = invariants.NodeState{
			Role:        s.Role.String(),
			Term:        s.Term,
			VotedFor:    (*string)(s.VotedFor),
			CommitIndex: s.CommitIndex,
			Applied:     s.Applied,
			Leader:      (*string)(s.Leader),
			Log:         log,
		}
	}

	return invariants.CheckAll(states)
}

// GetLeader returns the current leader, if any.
func (c *Cluster) GetLeader() (string, uint64) {
	for id, node := range c.nodes {
		state := node.raft.State()
		if state.Role == raft.Leader {
			return id, state.Term
		}
	}
	return "", 0
}

// GetNodeStates returns the current state of all nodes.
func (c *Cluster) GetNodeStates() map[string]raft.NodeState {
	states := make(map[string]raft.NodeState)
	for id, node := range c.nodes {
		states[id] = node.raft.State()
	}
	return states
}

// --- nodeEnv implementation ---

func (e *nodeEnv) Now() sim.Time {
	return e.cluster.currentTime
}

func (e *nodeEnv) SetTimer(d time.Duration, id raft.TimerID) {
	fireTime := e.cluster.currentTime + sim.Time(d.Nanoseconds())
	e.timers[id] = fireTime
	e.logBuffer = append(e.logBuffer, fmt.Sprintf("[%s] SetTimer(%s, %v) -> fire at %d", e.node.id, id, d, fireTime))
}

func (e *nodeEnv) CancelTimer(id raft.TimerID) {
	delete(e.timers, id)
	e.logBuffer = append(e.logBuffer, fmt.Sprintf("[%s] CancelTimer(%s)", e.node.id, id))
}

func (e *nodeEnv) Send(to raft.NodeID, payload []byte) {
	e.cluster.offeredCount++
	
	// Assign monotonic message ID
	e.cluster.msgCounter++
	msgID := e.cluster.msgCounter
	e.cluster.sentCount++
	
	// Log send before drop decision
	if e.cluster.debugLog != nil {
		hash := fmt.Sprintf("%x", sha256.Sum256(payload))[:8]
		fmt.Fprintf(e.cluster.debugLog, "t=%d SEND from=%s to=%s msgID=%d hash=%s\n",
			e.cluster.currentTime, e.node.id, to, msgID, hash)
	}
	
	// Check if message should be dropped (fault injection)
	if e.cluster.shouldDropMessage(string(e.node.id), string(to), msgID) {
		e.cluster.droppedCount++
		e.cluster.droppedMsgIDs = append(e.cluster.droppedMsgIDs, msgID)
		if e.cluster.debugLog != nil {
			fmt.Fprintf(e.cluster.debugLog, "t=%d DROP msgID=%d\n", e.cluster.currentTime, msgID)
		}
		e.logBuffer = append(e.logBuffer, fmt.Sprintf("[%s] Message to %s DROPPED (fault)", e.node.id, to))
		return
	}

	// Queue message for delivery
	e.cluster.msgChan[string(to)] = append(e.cluster.msgChan[string(to)], pendingMessage{
		from:    e.node.id,
		to:      to,
		payload: payload,
		msgID:   msgID,
	})
	e.logBuffer = append(e.logBuffer, fmt.Sprintf("[%s] Send(to=%s, len=%d)", e.node.id, to, len(payload)))
}

func (e *nodeEnv) Disk() raft.Disk {
	return e.node.disk
}

func (e *nodeEnv) Rand() raft.Rand {
	return &randAdapter{rng: e.node.rng}
}

func (e *nodeEnv) Logf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	e.logBuffer = append(e.logBuffer, msg)
}

// memoryDisk is a simple in-memory disk implementation.
type memoryDisk struct {
	data map[string][]byte
}

func (d *memoryDisk) Write(key string, value []byte) error {
	d.data[key] = append([]byte(nil), value...)
	return nil
}

func (d *memoryDisk) Sync() error {
	return nil
}

func (d *memoryDisk) Read(key string) ([]byte, error) {
	if value, ok := d.data[key]; ok {
		return append([]byte(nil), value...), nil
	}
	return nil, fmt.Errorf("key not found: %s", key)
}

// diskAdapter is no longer needed - we use memoryDisk directly

// randAdapter adapts *rand.Rand to raft.Rand.
type randAdapter struct {
	rng *rand.Rand
}

func (r *randAdapter) Intn(n int) int {
	return r.rng.Intn(n)
}

func (r *randAdapter) Uint64() uint64 {
	return r.rng.Uint64()
}


// --- Fault Injection Methods ---

// SetMessageDropRate sets the probability [0, 1] of dropping messages.
func (c *Cluster) SetMessageDropRate(rate float64) {
	c.messageDropRate = rate
}

// GetStats returns cluster statistics.
func (c *Cluster) GetStats() map[string]uint64 {
	return map[string]uint64{
		"sent":      c.sentCount,
		"dropped":   c.droppedCount,
		"delivered": c.deliveredCount,
	}
}

// GetNodes returns the cluster nodes for external configuration.
func (c *Cluster) GetNodes() map[string]*Node {
	return c.nodes
}

// SetRaft sets the raft instance for a node (for testing).
func (n *Node) SetRaft(raft *raft.RaftNode) {
	n.raft = raft
}

// SetMessageDelay sets a random delay range for message delivery.
func (c *Cluster) SetMessageDelay(min, max time.Duration) {
	c.messageDelayMin = min
	c.messageDelayMax = max
}

// PartitionNode isolates a node from the rest of the cluster.
func (c *Cluster) PartitionNode(nodeID string) {
	c.partitionedNodes[nodeID] = true
}

// HealPartition removes the partition for a node.
func (c *Cluster) HealPartition(nodeID string) {
	delete(c.partitionedNodes, nodeID)
}

// CrashNode simulates a node crash (stops processing, clears volatile state).
func (c *Cluster) CrashNode(nodeID string) {
	c.crashedNodes[nodeID] = true
	// Clear message queue for this node
	c.msgChan[nodeID] = []pendingMessage{}
	// Note: disk state persists
}

// RecoverNode restores a crashed node by reinitializing from persistent state.
func (c *Cluster) RecoverNode(nodeID string) error {
	delete(c.crashedNodes, nodeID)
	
	node := c.nodes[nodeID]
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	// Recover from disk
	if err := node.raft.Recover(); err != nil {
		// If recovery fails, it means no state on disk - start fresh
		// This is handled in Recover(), so we can ignore the error
	}

	return nil
}

// shouldDropMessage determines if a message should be dropped.
func (c *Cluster) shouldDropMessage(from, to string, msgID uint64) bool {
	c.reachedDecision++
	
	// Drop if either node is partitioned
	if c.partitionedNodes[from] || c.partitionedNodes[to] {
		c.blockedPartition++
		return true
	}
	
	// Drop if either node is crashed
	if c.crashedNodes[from] || c.crashedNodes[to] {
		c.blockedCrashed++
		return true
	}

	// Drop based on drop rate - use msgID for deterministic decision
	if c.messageDropRate > 0 {
		// splitmix64 mixer to get uniform distribution
		h := uint64(c.seed) ^ msgID
		h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
		h = (h ^ (h >> 27)) * 0x94d049bb133111eb
		h = h ^ (h >> 31)
		
		// Convert to float64 in [0,1) using top 53 bits
		const mask53 = (1 << 53) - 1
		f := float64(h&mask53) / float64(1<<53)
		shouldDrop := f < c.messageDropRate
		if shouldDrop {
			c.droppedByDecision++
		}
		return shouldDrop
	}

	return false
}
