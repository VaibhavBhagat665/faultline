// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand/v2"
)

// RNGKey uniquely identifies an RNG stream.
// Different keys produce independent, uncorrelated random streams.
type RNGKey struct {
	Component string // e.g., "network", "disk", "node"
	NodeID    int    // node identifier, or -1 for global
	Purpose   string // e.g., "latency", "fault", "timer"
}

// RNGManager manages seeded random number generators.
// It derives independent RNG streams from a master seed using a keyed function.
type RNGManager struct {
	masterSeed uint64
	streams    map[RNGKey]*rand.Rand
}

// NewRNGManager creates a new RNG manager with the given master seed.
func NewRNGManager(masterSeed uint64) *RNGManager {
	return &RNGManager{
		masterSeed: masterSeed,
		streams:    make(map[RNGKey]*rand.Rand),
	}
}

// Stream returns the RNG stream for the given key.
// If the stream does not exist, it is created deterministically from the master seed.
// Calling Stream with the same key always returns the same *rand.Rand instance.
func (m *RNGManager) Stream(key RNGKey) *rand.Rand {
	if rng, ok := m.streams[key]; ok {
		return rng
	}

	// Derive a seed for this stream
	streamSeed := m.deriveStreamSeed(key)
	source := rand.NewPCG(streamSeed, streamSeed^0xdeadbeefcafebabe)
	rng := rand.New(source)
	m.streams[key] = rng
	return rng
}

// deriveStreamSeed derives a unique seed for the given key using a hash function.
// This ensures that different keys produce different, uncorrelated seeds.
func (m *RNGManager) deriveStreamSeed(key RNGKey) uint64 {
	h := fnv.New64a()

	// Hash the master seed
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], m.masterSeed)
	h.Write(buf[:])

	// Hash the key components
	h.Write([]byte(key.Component))
	binary.LittleEndian.PutUint64(buf[:], uint64(key.NodeID))
	h.Write(buf[:])
	h.Write([]byte(key.Purpose))

	return h.Sum64()
}

// Reset clears all cached streams. Used for testing or between runs.
func (m *RNGManager) Reset() {
	m.streams = make(map[RNGKey]*rand.Rand)
}

// GlobalRNG returns an RNG stream for global (non-node-specific) randomness.
func (m *RNGManager) GlobalRNG(component, purpose string) *rand.Rand {
	return m.Stream(RNGKey{Component: component, NodeID: -1, Purpose: purpose})
}

// NodeRNG returns an RNG stream for a specific node.
func (m *RNGManager) NodeRNG(component string, nodeID int, purpose string) *rand.Rand {
	return m.Stream(RNGKey{Component: component, NodeID: nodeID, Purpose: purpose})
}
