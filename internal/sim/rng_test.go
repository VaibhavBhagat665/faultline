// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"testing"
)

func TestRNGManager_SameKeySameStream(t *testing.T) {
	m := NewRNGManager(12345)

	key := RNGKey{Component: "network", NodeID: 0, Purpose: "latency"}

	rng1 := m.Stream(key)
	rng2 := m.Stream(key)

	if rng1 != rng2 {
		t.Error("same key should return the same RNG instance")
	}

	// They should produce the same sequence
	m.Reset()
	rng1 = m.Stream(key)
	v1 := rng1.Uint64()

	m.Reset()
	rng2 = m.Stream(key)
	v2 := rng2.Uint64()

	if v1 != v2 {
		t.Errorf("same key after reset should produce same value: %d vs %d", v1, v2)
	}
}

func TestRNGManager_DifferentKeysIndependent(t *testing.T) {
	m := NewRNGManager(12345)

	key1 := RNGKey{Component: "network", NodeID: 0, Purpose: "latency"}
	key2 := RNGKey{Component: "network", NodeID: 1, Purpose: "latency"}
	key3 := RNGKey{Component: "disk", NodeID: 0, Purpose: "latency"}

	rng1 := m.Stream(key1)
	rng2 := m.Stream(key2)
	rng3 := m.Stream(key3)

	// All should be different instances
	if rng1 == rng2 || rng1 == rng3 || rng2 == rng3 {
		t.Error("different keys should return different RNG instances")
	}

	// Generate sequences and check they're different
	const n = 100
	seq1 := make([]uint64, n)
	seq2 := make([]uint64, n)
	seq3 := make([]uint64, n)

	for i := 0; i < n; i++ {
		seq1[i] = rng1.Uint64()
		seq2[i] = rng2.Uint64()
		seq3[i] = rng3.Uint64()
	}

	// Check that sequences are not identical
	same12 := true
	same13 := true
	same23 := true

	for i := 0; i < n; i++ {
		if seq1[i] != seq2[i] {
			same12 = false
		}
		if seq1[i] != seq3[i] {
			same13 = false
		}
		if seq2[i] != seq3[i] {
			same23 = false
		}
	}

	if same12 || same13 || same23 {
		t.Error("different keys should produce different sequences")
	}
}

func TestRNGManager_SameSeedDeterministic(t *testing.T) {
	const seed = 99999

	m1 := NewRNGManager(seed)
	m2 := NewRNGManager(seed)

	key := RNGKey{Component: "test", NodeID: 5, Purpose: "random"}

	rng1 := m1.Stream(key)
	rng2 := m2.Stream(key)

	const n = 1000
	for i := 0; i < n; i++ {
		v1 := rng1.Uint64()
		v2 := rng2.Uint64()
		if v1 != v2 {
			t.Fatalf("same seed should produce same sequence: iteration %d, %d vs %d", i, v1, v2)
		}
	}
}

func TestRNGManager_DifferentSeedsDifferentStreams(t *testing.T) {
	m1 := NewRNGManager(12345)
	m2 := NewRNGManager(54321)

	key := RNGKey{Component: "test", NodeID: 0, Purpose: "random"}

	rng1 := m1.Stream(key)
	rng2 := m2.Stream(key)

	const n = 100
	same := true
	for i := 0; i < n; i++ {
		v1 := rng1.Uint64()
		v2 := rng2.Uint64()
		if v1 != v2 {
			same = false
			break
		}
	}

	if same {
		t.Error("different master seeds should produce different sequences")
	}
}

func TestRNGManager_GlobalRNG(t *testing.T) {
	m := NewRNGManager(777)

	rng1 := m.GlobalRNG("nemesis", "fault-schedule")
	rng2 := m.GlobalRNG("nemesis", "fault-schedule")

	if rng1 != rng2 {
		t.Error("same global key should return same instance")
	}

	v1 := rng1.Uint64()
	v2 := rng1.Uint64()

	if v1 == v2 {
		t.Error("consecutive calls should produce different values (extremely unlikely collision)")
	}
}

func TestRNGManager_NodeRNG(t *testing.T) {
	m := NewRNGManager(777)

	rng0 := m.NodeRNG("raft", 0, "election-timeout")
	rng1 := m.NodeRNG("raft", 1, "election-timeout")

	if rng0 == rng1 {
		t.Error("different nodes should have different RNG instances")
	}

	// Different nodes should produce different sequences
	const n = 50
	same := true
	for i := 0; i < n; i++ {
		if rng0.Uint64() != rng1.Uint64() {
			same = false
			break
		}
	}

	if same {
		t.Error("different nodes should produce different sequences")
	}
}

func TestRNGManager_StreamIsolation(t *testing.T) {
	// Verify that consuming from one stream does not affect another
	m := NewRNGManager(42)

	key1 := RNGKey{Component: "net", NodeID: 0, Purpose: "delay"}
	key2 := RNGKey{Component: "net", NodeID: 1, Purpose: "delay"}

	rng1 := m.Stream(key1)
	rng2 := m.Stream(key2)

	// Record initial values from rng2
	expected := make([]uint64, 10)
	for i := range expected {
		expected[i] = rng2.Uint64()
	}

	// Reset and consume many values from rng1
	m.Reset()
	rng1 = m.Stream(key1)
	rng2 = m.Stream(key2)

	for i := 0; i < 10000; i++ {
		rng1.Uint64()
	}

	// rng2 should produce the same initial values
	for i, exp := range expected {
		got := rng2.Uint64()
		if got != exp {
			t.Errorf("stream isolation violated at position %d: expected %d, got %d", i, exp, got)
		}
	}
}

func BenchmarkRNGManager_StreamCreation(b *testing.B) {
	m := NewRNGManager(12345)
	key := RNGKey{Component: "bench", NodeID: 0, Purpose: "test"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Reset()
		_ = m.Stream(key)
	}
}

func BenchmarkRNGManager_Uint64(b *testing.B) {
	m := NewRNGManager(12345)
	key := RNGKey{Component: "bench", NodeID: 0, Purpose: "test"}
	rng := m.Stream(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rng.Uint64()
	}
}
