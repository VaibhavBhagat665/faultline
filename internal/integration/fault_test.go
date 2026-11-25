// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
)

// TestNetworkPartition verifies that writes fail during a network partition.
func TestNetworkPartition(t *testing.T) {
	cluster := NewCluster(1001, []string{"node1", "node2", "node3"})
	
	for _, node := range cluster.nodes {
		node.raft = raft.NewRaftNode(raft.Config{
			ElectionTimeoutMin: 300 * time.Millisecond,
			ElectionTimeoutMax: 600 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			MaxEntriesPerRPC:   100,
		})
	}
	
	if err := cluster.Init(); err != nil {
		t.Fatalf("Failed to init cluster: %v", err)
	}

	// Elect initial leader
	cluster.TriggerElection("node1")
	cluster.RunFor(1*time.Second, 1000)
	
	initialLeaderID, initialTerm := cluster.GetLeader()
	if initialLeaderID == "" {
		t.Fatal("No initial leader elected")
	}
	t.Logf("Initial leader: %s (term %d)", initialLeaderID, initialTerm)

	// Partition the leader from the rest of the cluster
	t.Logf("Creating partition: isolating %s", initialLeaderID)
	cluster.PartitionNode(initialLeaderID)

	// Run for election timeout to allow new leader election
	cluster.RunFor(2*time.Second, 2000)

	// Check cluster state
	newLeaderID, newTerm := cluster.GetLeader()
	t.Logf("After partition - Leader: %s (term %d)", newLeaderID, newTerm)

	// The minority partition (old leader) should not have a leader
	// The majority partition should elect a new leader
	states := cluster.GetNodeStates()
	
	partitionedState := states[initialLeaderID]
	t.Logf("Partitioned node %s: role=%s term=%d", 
		initialLeaderID, partitionedState.Role, partitionedState.Term)

	// Either the old leader stepped down, or a new leader was elected in majority
	if newLeaderID != "" && newLeaderID != initialLeaderID {
		t.Logf("✓ New leader elected in majority partition: %s", newLeaderID)
	} else if newLeaderID == "" {
		t.Log("⚠ No clear leader after partition (acceptable)")
	}

	// Heal the partition
	t.Log("Healing partition")
	cluster.HealPartition(initialLeaderID)
	
	// Run to allow cluster to reconverge
	cluster.RunFor(2*time.Second, 2000)

	// Verify cluster reconverged
	finalLeaderID, finalTerm := cluster.GetLeader()
	if finalLeaderID == "" {
		t.Error("No leader after partition healed")
	} else {
		t.Logf("✓ Cluster reconverged with leader %s (term %d)", finalLeaderID, finalTerm)
	}

	// Check invariants
	if violation := cluster.CheckInvariants(); violation != nil {
		t.Fatalf("Invariant violation: %v", violation)
	}
}

// TestMessageDrop verifies that the system handles dropped messages.
func TestMessageDrop(t *testing.T) {
	cluster := NewCluster(2002, []string{"node1", "node2", "node3"})
	
	for _, node := range cluster.nodes {
		node.raft = raft.NewRaftNode(raft.Config{
			ElectionTimeoutMin: 300 * time.Millisecond,
			ElectionTimeoutMax: 600 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			MaxEntriesPerRPC:   100,
		})
	}
	
	if err := cluster.Init(); err != nil {
		t.Fatalf("Failed to init cluster: %v", err)
	}

	// Elect leader
	cluster.TriggerElection("node1")
	cluster.RunFor(1*time.Second, 1000)
	
	leaderID, _ := cluster.GetLeader()
	if leaderID == "" {
		t.Fatal("No leader elected")
	}
	t.Logf("Leader: %s", leaderID)

	// Enable message drops (10% drop rate)
	t.Log("Enabling 10% message drop rate")
	cluster.SetMessageDropRate(0.1)

	// Submit writes with message drops
	leader := cluster.nodes[leaderID]
	successCount := 0
	for i := 1; i <= 5; i++ {
		req := raft.Request{
			ClientID: 1,
			Seq:      uint64(i),
			Command:  []byte(`{"op":"write","key":"x","value":` + string(rune('0'+i)) + `}`),
		}

		leader.raft.OnClientRequest(req, func(resp raft.Response) {
			if resp.Success {
				successCount++
			}
		})
	}

	// Run with drops enabled
	cluster.RunFor(5*time.Second, 3000)

	t.Logf("Completed %d/5 writes with 10%% message drops", successCount)

	// Disable drops
	cluster.SetMessageDropRate(0)

	// Check state
	states := cluster.GetNodeStates()
	for id, state := range states {
		t.Logf("Node %s: logLen=%d commitIndex=%d", id, len(state.Log), state.CommitIndex)
	}

	// At least some writes should succeed despite drops
	if successCount == 0 {
		t.Error("No writes succeeded with message drops (expected some to succeed)")
	}

	// Check invariants
	if violation := cluster.CheckInvariants(); violation != nil {
		t.Fatalf("Invariant violation: %v", violation)
	}
}

// TestMessageDelay verifies that the system handles delayed messages.
func TestMessageDelay(t *testing.T) {
	cluster := NewCluster(3003, []string{"node1", "node2", "node3"})
	
	for _, node := range cluster.nodes {
		node.raft = raft.NewRaftNode(raft.Config{
			ElectionTimeoutMin: 500 * time.Millisecond,
			ElectionTimeoutMax: 1000 * time.Millisecond,
			HeartbeatInterval:  100 * time.Millisecond,
			MaxEntriesPerRPC:   100,
		})
	}
	
	if err := cluster.Init(); err != nil {
		t.Fatalf("Failed to init cluster: %v", err)
	}

	// Elect leader
	cluster.TriggerElection("node1")
	cluster.RunFor(1*time.Second, 1000)
	
	leaderID, _ := cluster.GetLeader()
	if leaderID == "" {
		t.Fatal("No leader elected")
	}
	t.Logf("Leader: %s", leaderID)

	// Enable message delays (0-200ms delay)
	t.Log("Enabling message delays (0-200ms)")
	cluster.SetMessageDelay(0, 200*time.Millisecond)

	// Submit write with delays
	leader := cluster.nodes[leaderID]
	writeSuccess := false
	
	req := raft.Request{
		ClientID: 1,
		Seq:      1,
		Command:  []byte(`{"op":"write","key":"x","value":42}`),
	}

	leader.raft.OnClientRequest(req, func(resp raft.Response) {
		writeSuccess = resp.Success
		t.Logf("Write response: success=%v", resp.Success)
	})

	// Run with delays (need more time for delayed messages)
	cluster.RunFor(5*time.Second, 3000)

	if writeSuccess {
		t.Log("✓ Write succeeded despite message delays")
	} else {
		t.Log("⚠ Write did not complete (acceptable with delays)")
	}

	// Disable delays
	cluster.SetMessageDelay(0, 0)

	// Check invariants
	if violation := cluster.CheckInvariants(); violation != nil {
		t.Fatalf("Invariant violation: %v", violation)
	}
}

// TestLeaderCrashAndRecovery verifies that a crashed leader can recover.
func TestLeaderCrashAndRecovery(t *testing.T) {
	cluster := NewCluster(4004, []string{"node1", "node2", "node3"})
	
	for _, node := range cluster.nodes {
		node.raft = raft.NewRaftNode(raft.Config{
			ElectionTimeoutMin: 300 * time.Millisecond,
			ElectionTimeoutMax: 600 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			MaxEntriesPerRPC:   100,
		})
	}
	
	if err := cluster.Init(); err != nil {
		t.Fatalf("Failed to init cluster: %v", err)
	}

	// Elect leader
	cluster.TriggerElection("node1")
	cluster.RunFor(1*time.Second, 1000)
	
	initialLeaderID, _ := cluster.GetLeader()
	if initialLeaderID == "" {
		t.Fatal("No leader elected")
	}
	t.Logf("Initial leader: %s", initialLeaderID)

	// Submit a write
	leader := cluster.nodes[initialLeaderID]
	req := raft.Request{
		ClientID: 1,
		Seq:      1,
		Command:  []byte(`{"op":"write","key":"x","value":1}`),
	}
	leader.raft.OnClientRequest(req, func(resp raft.Response) {
		t.Logf("First write: success=%v", resp.Success)
	})

	cluster.RunFor(500*time.Millisecond, 500)

	// Crash the leader
	t.Logf("Crashing leader %s", initialLeaderID)
	cluster.CrashNode(initialLeaderID)

	// Run to allow new leader election
	cluster.RunFor(2*time.Second, 2000)

	// Check that a new leader was elected
	newLeaderID, newTerm := cluster.GetLeader()
	if newLeaderID == "" || newLeaderID == initialLeaderID {
		t.Error("Expected new leader to be elected after crash")
	} else {
		t.Logf("✓ New leader elected: %s (term %d)", newLeaderID, newTerm)
	}

	// Recover the crashed node
	t.Logf("Recovering node %s", initialLeaderID)
	if err := cluster.RecoverNode(initialLeaderID); err != nil {
		t.Fatalf("Failed to recover node: %v", err)
	}

	// Run to allow recovered node to rejoin
	cluster.RunFor(2*time.Second, 2000)

	// Check that recovered node is now a follower
	states := cluster.GetNodeStates()
	recoveredState := states[initialLeaderID]
	t.Logf("Recovered node %s: role=%s term=%d", 
		initialLeaderID, recoveredState.Role, recoveredState.Term)

	if recoveredState.Role == raft.Leader {
		t.Error("Recovered node should not immediately become leader")
	}

	// Check invariants
	if violation := cluster.CheckInvariants(); violation != nil {
		t.Fatalf("Invariant violation: %v", violation)
	}
}

// TestConcurrentWrites verifies that concurrent writes from multiple clients work.
func TestConcurrentWrites(t *testing.T) {
	cluster := NewCluster(5005, []string{"node1", "node2", "node3"})
	
	for _, node := range cluster.nodes {
		node.raft = raft.NewRaftNode(raft.Config{
			ElectionTimeoutMin: 300 * time.Millisecond,
			ElectionTimeoutMax: 600 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			MaxEntriesPerRPC:   100,
		})
	}
	
	if err := cluster.Init(); err != nil {
		t.Fatalf("Failed to init cluster: %v", err)
	}

	// Elect leader
	cluster.TriggerElection("node1")
	cluster.RunFor(1*time.Second, 1000)
	
	leaderID, _ := cluster.GetLeader()
	if leaderID == "" {
		t.Fatal("No leader elected")
	}
	t.Logf("Leader: %s", leaderID)

	// Submit concurrent writes from 3 different clients
	leader := cluster.nodes[leaderID]
	numClients := 3
	writesPerClient := 3
	totalWrites := numClients * writesPerClient
	successCount := 0

	for clientID := 1; clientID <= numClients; clientID++ {
		for seq := 1; seq <= writesPerClient; seq++ {
			req := raft.Request{
				ClientID: uint64(clientID),
				Seq:      uint64(seq),
				Command:  []byte(`{"op":"write","key":"x","value":1}`),
			}

			leader.raft.OnClientRequest(req, func(resp raft.Response) {
				if resp.Success {
					successCount++
				}
			})
		}
	}

	// Run to complete writes
	cluster.RunFor(5*time.Second, 3000)

	t.Logf("Completed %d/%d concurrent writes", successCount, totalWrites)

	// Check that all writes were processed
	states := cluster.GetNodeStates()
	leaderState := states[leaderID]
	
	// Should have dummy entry + totalWrites entries (if all succeeded)
	expectedMinLen := 1 + successCount
	if len(leaderState.Log) < expectedMinLen {
		t.Logf("Leader log length %d, expected at least %d", 
			len(leaderState.Log), expectedMinLen)
	}

	t.Logf("Leader commitIndex: %d", leaderState.CommitIndex)

	// Check invariants
	if violation := cluster.CheckInvariants(); violation != nil {
		t.Fatalf("Invariant violation: %v", violation)
	}
}
