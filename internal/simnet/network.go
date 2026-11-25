// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package simnet

import (
	"fmt"
	"math/rand/v2"

	"github.com/VaibhavBhagat665/faultline/internal/sim"
)

// NodeID identifies a node in the network.
type NodeID int

// Message represents a network message in flight.
type Message struct {
	From    NodeID
	To      NodeID
	Payload []byte
	// SendTime is when the message was sent (virtual time).
	SendTime sim.Time
	// DeliverTime is when the message should be delivered (SendTime + latency).
	DeliverTime sim.Time
	// ID is a unique identifier for this message (for duplicate detection).
	ID uint64
}

// DeliveryMode determines message ordering and reliability semantics.
type DeliveryMode int

const (
	// DeliveryTCP provides ordered, connection-based delivery.
	// Messages on the same link are delivered in send order.
	// Connection reset drops all in-flight messages on that link.
	DeliveryTCP DeliveryMode = iota

	// DeliveryUDP provides unordered, best-effort delivery.
	// Messages can be dropped, delayed, duplicated, or reordered.
	DeliveryUDP
)

// NetworkConfig configures the simulated network.
type NetworkConfig struct {
	// Mode is the delivery mode (TCP-like or UDP-like).
	Mode DeliveryMode

	// DefaultLatency is the baseline one-way latency for all links.
	DefaultLatency sim.Time

	// Jitter adds random variation to latency: actual = latency + rand(0, jitter).
	Jitter sim.Time

	// RNG is the random number generator for latency jitter and faults.
	RNG *rand.Rand
}

// Network is a simulated network that delivers messages between nodes.
type Network struct {
	cfg      NetworkConfig
	messages map[uint64]*Message // in-flight messages by ID
	nextID   uint64
	faults   FaultState
}

// FaultState tracks active network faults.
type FaultState struct {
	// Partitions maps (from, to) -> bool. If true, messages from->to are dropped.
	Partitions map[link]bool

	// DropRate is the probability [0, 1] of dropping any message.
	DropRate float64

	// DuplicateRate is the probability [0, 1] of duplicating a message.
	DuplicateRate float64

	// ReorderWindow is the time window in which messages can be reordered.
	// If non-zero, messages within this window may arrive out of order.
	ReorderWindow sim.Time
}

type link struct {
	from NodeID
	to   NodeID
}

// NewNetwork creates a new simulated network.
func NewNetwork(cfg NetworkConfig) *Network {
	if cfg.RNG == nil {
		panic("simnet: RNG is required")
	}
	return &Network{
		cfg:      cfg,
		messages: make(map[uint64]*Message),
		nextID:   1,
		faults: FaultState{
			Partitions: make(map[link]bool),
		},
	}
}

// Send sends a message from one node to another.
// The message will be delivered after latency (unless dropped by a fault).
// Returns the message ID (for testing/debugging).
func (n *Network) Send(from, to NodeID, payload []byte, now sim.Time) uint64 {
	// Check partition
	if n.faults.Partitions[link{from, to}] {
		// Message is dropped silently (partition)
		return 0
	}

	// Check drop fault
	if n.faults.DropRate > 0 && n.cfg.RNG.Float64() < n.faults.DropRate {
		// Message is dropped
		return 0
	}

	// Compute delivery time
	latency := n.cfg.DefaultLatency
	if n.cfg.Jitter > 0 {
		jitter := sim.Time(n.cfg.RNG.Int64N(int64(n.cfg.Jitter)))
		latency += jitter
	}

	// Apply reorder window fault
	if n.faults.ReorderWindow > 0 {
		reorderJitter := sim.Time(n.cfg.RNG.Int64N(int64(n.faults.ReorderWindow)))
		latency += reorderJitter
	}

	deliverTime := now + latency

	// Create message
	msgID := n.nextID
	n.nextID++

	msg := &Message{
		From:        from,
		To:          to,
		Payload:     payload,
		SendTime:    now,
		DeliverTime: deliverTime,
		ID:          msgID,
	}

	n.messages[msgID] = msg

	// Check duplicate fault
	if n.faults.DuplicateRate > 0 && n.cfg.RNG.Float64() < n.faults.DuplicateRate {
		// Schedule a duplicate delivery
		dupID := n.nextID
		n.nextID++
		dupLatency := latency + sim.Time(n.cfg.RNG.Int64N(int64(n.cfg.DefaultLatency)))
		dup := &Message{
			From:        from,
			To:          to,
			Payload:     payload,
			SendTime:    now,
			DeliverTime: now + dupLatency,
			ID:          dupID,
		}
		n.messages[dupID] = dup
	}

	return msgID
}

// Poll returns all messages ready for delivery at or before the given time.
// Delivered messages are removed from the in-flight set.
func (n *Network) Poll(now sim.Time) []*Message {
	var ready []*Message
	for id, msg := range n.messages {
		if msg.DeliverTime <= now {
			ready = append(ready, msg)
			delete(n.messages, id)
		}
	}
	return ready
}

// InFlight returns the number of messages currently in flight.
func (n *Network) InFlight() int {
	return len(n.messages)
}

// Partition creates a partition between two nodes.
// Messages from 'from' to 'to' will be dropped.
func (n *Network) Partition(from, to NodeID) {
	n.faults.Partitions[link{from, to}] = true
}

// Heal removes a partition between two nodes.
func (n *Network) Heal(from, to NodeID) {
	delete(n.faults.Partitions, link{from, to})
}

// IsPartitioned returns true if there is a partition from->to.
func (n *Network) IsPartitioned(from, to NodeID) bool {
	return n.faults.Partitions[link{from, to}]
}

// SetDropRate sets the probability [0, 1] of dropping messages.
func (n *Network) SetDropRate(rate float64) {
	if rate < 0 || rate > 1 {
		panic(fmt.Sprintf("simnet: drop rate must be in [0, 1], got %f", rate))
	}
	n.faults.DropRate = rate
}

// SetDuplicateRate sets the probability [0, 1] of duplicating messages.
func (n *Network) SetDuplicateRate(rate float64) {
	if rate < 0 || rate > 1 {
		panic(fmt.Sprintf("simnet: duplicate rate must be in [0, 1], got %f", rate))
	}
	n.faults.DuplicateRate = rate
}

// SetReorderWindow sets the time window for message reordering.
func (n *Network) SetReorderWindow(window sim.Time) {
	n.faults.ReorderWindow = window
}

// ClearFaults removes all active faults.
func (n *Network) ClearFaults() {
	n.faults.Partitions = make(map[link]bool)
	n.faults.DropRate = 0
	n.faults.DuplicateRate = 0
	n.faults.ReorderWindow = 0
}

// DropAllInFlight drops all messages currently in flight.
// Used for connection reset or catastrophic failure scenarios.
func (n *Network) DropAllInFlight() {
	n.messages = make(map[uint64]*Message)
}
