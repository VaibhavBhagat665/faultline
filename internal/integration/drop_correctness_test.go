// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
)

func TestDropCorrectness(t *testing.T) {
	profiles := []struct {
		name string
		rate float64
	}{
		{"clean", 0},
		{"drop8", 0.08},
		{"drop15", 0.15},
	}

	seedCount := 50
	if testing.Short() {
		seedCount = 20
	}

	for _, profile := range profiles {
		t.Run(profile.name, func(t *testing.T) {
			totalSent, totalDropped := 0, 0
			droppedSets := make(map[string]bool)

			for seed := int64(1); seed <= int64(seedCount); seed++ {
				ids := []string{"n1", "n2", "n3", "n4", "n5"}
				c := NewCluster(seed, ids)
				c.messageDropRate = profile.rate
				
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

				sent := int(c.sentCount)
				dropped := int(c.droppedCount)
				delivered := int(c.deliveredCount)
				totalSent += sent
				totalDropped += dropped

				// Track dropped message IDs to see if they vary by seed
				droppedKey := fmt.Sprintf("%d", c.droppedMsgIDs)
				droppedSets[droppedKey] = true

				t.Logf("seed=%2d sent=%4d dropped=%4d delivered=%4d rate=%.3f", seed, sent, dropped, delivered, float64(dropped)/float64(sent))
			}

			observedRate := float64(totalDropped) / float64(totalSent)
			t.Logf("SUMMARY profile=%s seeds=%d totalSent=%d totalDropped=%d observedRate=%.4f expectedRate=%.2f distinctDropSets=%d",
				profile.name, seedCount, totalSent, totalDropped, observedRate, profile.rate, len(droppedSets))

			// Assertions
			if profile.rate == 0 {
				if totalDropped != 0 {
					t.Errorf("clean profile dropped %d messages (expected 0)", totalDropped)
				}
			} else {
				tolerance := 0.03  // 3% tolerance for drop rate variability
				if observedRate < profile.rate-tolerance || observedRate > profile.rate+tolerance {
					t.Errorf("observedRate %.4f outside [%.4f, %.4f]", 
						observedRate, profile.rate-tolerance, profile.rate+tolerance)
				}
				if len(droppedSets) < 5 {
					t.Errorf("only %d distinct drop patterns across 20 seeds (need >= 5)", len(droppedSets))
				}
			}
		})
	}
}
