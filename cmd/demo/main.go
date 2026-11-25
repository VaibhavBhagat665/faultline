package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/VaibhavBhagat665/faultline/internal/integration"
)

func main() {
	seed := int64(42)
	if len(os.Args) > 1 {
		if s, err := strconv.ParseInt(os.Args[1], 10, 64); err == nil {
			seed = s
		}
	}

	fmt.Printf("=== Faultline Raft Election Demo ===\n")
	fmt.Printf("Seed: %d\n\n", seed)

	// Create 5-node cluster
	cluster := integration.NewCluster(seed, []string{"n1", "n2", "n3", "n4", "n5"})
	cluster.Init()

	// Set message drop rate to 8%
	cluster.SetMessageDropRate(0.08)

	// Trigger election on node 1
	cluster.TriggerElection("n1")

	// Run simulation for 1 second (virtual time)
	startTime := time.Now()
	eventCount := cluster.RunFor(1*time.Second, 10000) // max 10k events
	realTime := time.Since(startTime)

	// Get results
	leaderID, term := cluster.GetLeader()
	stats := cluster.GetStats()

	fmt.Printf("Results:\n")
	fmt.Printf("  Leader:  %s\n", leaderID)
	fmt.Printf("  Term:    %d\n", term)
	fmt.Printf("  Events:  %d\n", eventCount)
	fmt.Printf("  Real time: %v\n", realTime)
	fmt.Printf("  Throughput: %.0f events/sec\n\n", float64(eventCount)/realTime.Seconds())
	
	fmt.Printf("Network Stats:\n")
	for key, val := range stats {
		fmt.Printf("  %s: %d\n", key, val)
	}

	if leaderID == "" {
		fmt.Printf("\n❌ No leader elected\n")
		os.Exit(1)
	}

	fmt.Printf("\n✅ Election successful\n")
}
