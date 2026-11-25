package integration

import (
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
)

func TestM1Counters(t *testing.T) {
	for _, prof := range []struct {
		name string
		rate float64
	}{
		{"drop8", 0.08},
		{"drop15", 0.15},
	} {
		totalOffered := uint64(0)
		totalReached := uint64(0)
		totalDroppedByDecision := uint64(0)
		totalDelivered := uint64(0)
		totalBlockedPartition := uint64(0)
		totalBlockedCrashed := uint64(0)

		for seed := int64(1); seed <= 50; seed++ {
			ids := []string{"n1", "n2", "n3", "n4", "n5"}
			c := NewCluster(seed, ids)
			c.messageDropRate = prof.rate

			for _, n := range c.nodes {
				n.raft = raft.NewRaftNode(raft.DefaultConfig())
			}

			if err := c.Init(); err != nil {
				t.Fatalf("init failed: %v", err)
			}

			c.TriggerElection("n1")
			c.RunFor(300*time.Millisecond, 2000)
			c.TriggerElection("n3")
			c.RunFor(2*time.Second, 5000)

			totalOffered += c.offeredCount
			totalReached += c.reachedDecision
			totalDroppedByDecision += c.droppedByDecision
			totalDelivered += c.deliveredCount
			totalBlockedPartition += c.blockedPartition
			totalBlockedCrashed += c.blockedCrashed
		}

		rateReaching := float64(totalDroppedByDecision) / float64(totalReached)
		rateOffered := float64(totalDroppedByDecision) / float64(totalOffered)

		t.Logf("profile=%s rate=%.2f", prof.name, prof.rate)
		t.Logf("  offered=%d", totalOffered)
		t.Logf("  reachedDecision=%d", totalReached)
		t.Logf("  droppedByDecision=%d", totalDroppedByDecision)
		t.Logf("  blockedPartition=%d", totalBlockedPartition)
		t.Logf("  blockedCrashed=%d", totalBlockedCrashed)
		t.Logf("  delivered=%d", totalDelivered)
		t.Logf("  rate=dropped/reaching=%.4f", rateReaching)
		t.Logf("  rate=dropped/offered=%.4f", rateOffered)
		t.Logf("  messageDropRate as seen=%.2f", prof.rate)
	}
}
