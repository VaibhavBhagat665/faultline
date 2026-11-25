// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
	"github.com/VaibhavBhagat665/faultline/internal/sim"
)

// TestThreeNodeElection verifies that a 3-node cluster can elect a leader.
func TestThreeNodeElection(t *testing.T) {
	// Create a 3-node cluster
	cluster := NewCluster(12345, []string{"node1", "node2", "node3"})
	if err := cluster.Init(); err != nil {
		t.Fatalf("Failed to init cluster: %v", err)
	}

	// Trigger election on node1
	cluster.TriggerElection("node1")

	// Step through events
	maxTime := sim.Time(5 * time.Second.Nanoseconds())
	eventsProcessed := cluster.Step(maxTime, 1000)

	t.Logf("Processed %d events", eventsProcessed)

	// Check invariants
	if violation := cluster.CheckInvariants(); violation != nil {
		t.Fatalf("Invariant violation: %v", violation)
	}

	// Verify a leader was elected
	leaderID, leaderTerm := cluster.GetLeader()
	if leaderID == "" {
		t.Fatal("No leader elected")
	}

	t.Logf("Leader elected: %s (term %d)", leaderID, leaderTerm)

	// Verify all nodes agree on the leader
	states := cluster.GetNodeStates()
	for id, state := range states {
		t.Logf("Node %s: role=%s term=%d votedFor=%v leader=%v",
			id, state.Role, state.Term,
			state.VotedFor, state.Leader)

		if state.Leader == nil {
			t.Errorf("Node %s does not know the leader", id)
		} else if string(*state.Leader) != leaderID {
			t.Errorf("Node %s thinks leader is %s, but actual leader is %s",
				id, *state.Leader, leaderID)
		}
	}
}

// TestMultipleElections verifies that multiple elections work correctly.
func TestMultipleElections(t *testing.T) {
	cluster := NewCluster(54321, []string{"node1", "node2", "node3"})
	if err := cluster.Init(); err != nil {
		t.Fatalf("Failed to init cluster: %v", err)
	}

	// First election
	cluster.TriggerElection("node1")
	cluster.Step(sim.Time(2*time.Second.Nanoseconds()), 500)

	// Check first leader
	leader1, term1 := cluster.GetLeader()
	if leader1 == "" {
		t.Fatal("No leader elected in first election")
	}
	t.Logf("First leader: %s (term %d)", leader1, term1)

	// Trigger another election (simulate leader failure)
	cluster.TriggerElection("node2")
	cluster.Step(sim.Time(5*time.Second.Nanoseconds()), 500)

	// Check second leader
	leader2, term2 := cluster.GetLeader()
	if leader2 == "" {
		t.Fatal("No leader elected in second election")
	}
	t.Logf("Second leader: %s (term %d)", leader2, term2)

	// Term should have increased
	if term2 <= term1 {
		t.Errorf("Term did not increase: term1=%d term2=%d", term1, term2)
	}

	// Check invariants
	if violation := cluster.CheckInvariants(); violation != nil {
		t.Fatalf("Invariant violation: %v", violation)
	}
}

// TestFiveNodeElection verifies that a 5-node cluster can elect a leader.
func TestFiveNodeElection(t *testing.T) {
	cluster := NewCluster(99999, []string{"node1", "node2", "node3", "node4", "node5"})
	if err := cluster.Init(); err != nil {
		t.Fatalf("Failed to init cluster: %v", err)
	}

	// Trigger election on node1
	cluster.TriggerElection("node1")

	// Step through events (5-node needs more time)
	eventsProcessed := cluster.RunFor(10*time.Second, 3000)
	t.Logf("Processed %d events", eventsProcessed)

	// Check invariants
	if violation := cluster.CheckInvariants(); violation != nil {
		t.Fatalf("Invariant violation: %v", violation)
	}

	// Verify a leader was elected
	leaderID, leaderTerm := cluster.GetLeader()
	if leaderID == "" {
		t.Fatal("No leader elected")
	}

	t.Logf("Leader elected: %s (term %d)", leaderID, leaderTerm)

	// Verify all nodes agree (with some tolerance for stale state)
	states := cluster.GetNodeStates()
	for id, state := range states {
		// Allow followers to have nil leader if they haven't received heartbeat yet
		if state.Role == raft.Follower && state.Leader == nil {
			t.Logf("Warning: Follower %s has not yet learned leader", id)
		} else if state.Leader != nil && string(*state.Leader) != leaderID {
			t.Errorf("Node %s disagrees on leader: expected %s, got %v",
				id, leaderID, *state.Leader)
		}
	}
}

// TestElectionWithConcurrentCandidates tests split vote scenarios.
func TestElectionWithConcurrentCandidates(t *testing.T) {
	cluster := NewCluster(11111, []string{"node1", "node2", "node3"})
	if err := cluster.Init(); err != nil {
		t.Fatalf("Failed to init cluster: %v", err)
	}

	// Trigger elections on multiple nodes simultaneously
	cluster.TriggerElection("node1")
	cluster.TriggerElection("node2")

	// Step through events - should handle split vote and re-elect
	eventsProcessed := cluster.Step(sim.Time(10*time.Second.Nanoseconds()), 2000)
	t.Logf("Processed %d events", eventsProcessed)

	// Check invariants - should never have two leaders
	if violation := cluster.CheckInvariants(); violation != nil {
		t.Fatalf("Invariant violation: %v", violation)
	}

	// Eventually a leader should be elected
	leaderID, leaderTerm := cluster.GetLeader()
	if leaderID != "" {
		t.Logf("Leader elected after split vote: %s (term %d)", leaderID, leaderTerm)
	} else {
		t.Log("No leader elected (acceptable with split votes)")
	}
}
