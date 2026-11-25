package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/VaibhavBhagat665/faultline/internal/integration"
)

func runScenario(name string, seed int64, dropRate float64) {
	fmt.Printf("=== %s ===\n", name)
	fmt.Printf("Seed: %d, Drop rate: %.0f%%\n\n", seed, dropRate*100)

	cluster := integration.NewCluster(seed, []string{"n1", "n2", "n3", "n4", "n5"})
	cluster.Init()
	cluster.SetMessageDropRate(dropRate)
	cluster.TriggerElection("n1")

	startTime := time.Now()
	eventCount := cluster.RunFor(1*time.Second, 10000)
	realTime := time.Since(startTime)

	leaderID, term := cluster.GetLeader()
	stats := cluster.GetStats()

	fmt.Printf("Results:\n")
	fmt.Printf("  Leader:       %s\n", leaderID)
	fmt.Printf("  Term:         %d\n", term)
	fmt.Printf("  Events:       %d\n", eventCount)
	fmt.Printf("  Sent:         %d\n", stats["sent"])
	fmt.Printf("  Dropped:      %d\n", stats["dropped"])
	fmt.Printf("  Actual drop:  %.1f%%\n", float64(stats["dropped"])/float64(stats["sent"])*100)
	fmt.Printf("  Throughput:   %.0f events/sec\n", float64(eventCount)/realTime.Seconds())

	if leaderID == "" {
		fmt.Printf("  Status:       ❌ No leader elected\n")
	} else {
		fmt.Printf("  Status:       ✅ Election successful\n")
	}
	fmt.Println()
}

func main() {
	seed := int64(42)
	if len(os.Args) > 1 {
		if s, err := strconv.ParseInt(os.Args[1], 10, 64); err == nil {
			seed = s
		}
	}

	fmt.Println("╔═══════════════════════════════════════════════════╗")
	fmt.Println("║   Faultline: Deterministic Raft Simulation       ║")
	fmt.Println("╚═══════════════════════════════════════════════════╝")
	fmt.Println()

	// Scenario 1: Clean run (no drops)
	runScenario("Scenario 1: Clean Network", seed, 0.0)

	// Scenario 2: Light faults (8% drops)
	runScenario("Scenario 2: Light Faults (8%)", seed, 0.08)

	// Scenario 3: Heavy faults (15% drops)
	runScenario("Scenario 3: Heavy Faults (15%)", seed, 0.15)

	// Scenario 4: Extreme faults (25% drops)
	runScenario("Scenario 4: Extreme Faults (25%)", seed, 0.25)

	fmt.Println("═══════════════════════════════════════════════════")
	fmt.Printf("Note: All runs use same seed (%d) for reproducibility\n", seed)
	fmt.Println("Run again with same seed → exact same results!")
}
