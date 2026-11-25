// SPDX-License-Identifier: Apache-2.0

// Package integration provides end-to-end integration tests.
package integration

import (
	"testing"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
	"github.com/VaibhavBhagat665/faultline/internal/invariants"
)

// TestBasicNodeCreation verifies that we can create Raft nodes.
func TestBasicNodeCreation(t *testing.T) {
	cfg := raft.DefaultConfig()
	node := raft.NewRaftNode(cfg)
	
	if node == nil {
		t.Fatal("Failed to create Raft node")
	}
	
	t.Log("Successfully created Raft node")
}

// TestInvariantCheckers verifies that invariant checkers work.
func TestInvariantCheckers(t *testing.T) {
	// Test election safety: no violation with single leader
	states := map[string]invariants.NodeState{
		"node1": {
			Role: "Leader",
			Term: 1,
		},
		"node2": {
			Role: "Follower",
			Term: 1,
		},
		"node3": {
			Role: "Follower",
			Term: 1,
		},
	}
	
	if v := invariants.CheckElectionSafety(states); v != nil {
		t.Fatalf("Unexpected violation: %v", v)
	}
	
	// Test election safety: violation with two leaders
	states["node2"] = invariants.NodeState{
		Role: "Leader",
		Term: 1,
	}
	
	if v := invariants.CheckElectionSafety(states); v == nil {
		t.Fatal("Expected election safety violation, got none")
	} else {
		t.Logf("Correctly detected violation: %v", v)
	}
}
