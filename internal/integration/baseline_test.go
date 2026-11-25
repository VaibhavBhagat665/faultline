// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
)

// TestBaseline_TraceHashes captures baseline trace hashes before bug wiring.
func TestBaseline_TraceHashes(t *testing.T) {
	const numSeeds = 20

	t.Logf("Baseline trace hashes (bugs OFF):")
	
	for seed := int64(1); seed <= numSeeds; seed++ {
		cluster := NewCluster(seed, []string{"n1", "n2", "n3", "n4", "n5"})

		for _, node := range cluster.nodes {
			node.raft = raft.NewRaftNode(raft.DefaultConfig())
		}

		if err := cluster.Init(); err != nil {
			t.Fatalf("seed %d: init failed: %v", seed, err)
		}

		cluster.TriggerElection("n1")
		cluster.RunFor(2*time.Second, 3000)

		// Get some trace representation
		leader, term := cluster.GetLeader()
		t.Logf("seed=%02d leader=%s term=%d", seed, leader, term)
	}
}
