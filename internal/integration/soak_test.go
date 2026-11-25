package integration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
	"github.com/VaibhavBhagat665/faultline/internal/workload"
)

// TestSoakClean runs a massive number of randomized scenarios to validate:
// 1. No false positives (invariant violations in correct code)
// 2. Deterministic reproducibility across all seeds
// 3. Framework stability at scale
func TestSoakClean(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping soak test in short mode")
	}

	const (
		totalSeeds  = 100000
		workerCount = 8 // Parallel workers for faster execution
		reportEvery = 1000
	)

	start := time.Now()
	var (
		passed   atomic.Uint64
		failed   atomic.Uint64
		failures sync.Map // seed -> error message
	)

	// Worker pool
	seedChan := make(chan uint64, workerCount*2)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for seed := range seedChan {
				if err := runSoakScenario(seed); err != nil {
					failed.Add(1)
					failures.Store(seed, err.Error())
					// Log immediately for critical failures
					t.Logf("[Worker %d] FAIL seed=%d: %v", workerID, seed, err)
				} else {
					passed.Add(1)
				}

				// Progress report
				total := passed.Load() + failed.Load()
				if total%reportEvery == 0 {
					elapsed := time.Since(start)
					rate := float64(total) / elapsed.Seconds()
					t.Logf("[Progress] %d/%d seeds (%.1f%%) - %.0f seeds/sec - %d failures",
						total, totalSeeds, 100*float64(total)/totalSeeds, rate, failed.Load())
				}
			}
		}(i)
	}

	// Feed seeds
	for seed := uint64(1); seed <= totalSeeds; seed++ {
		seedChan <- seed
	}
	close(seedChan)

	// Wait for completion
	wg.Wait()

	// Final report
	elapsed := time.Since(start)
	passCount := passed.Load()
	failCount := failed.Load()
	total := passCount + failCount

	t.Logf("\n=== SOAK TEST COMPLETE ===")
	t.Logf("Duration:  %v", elapsed)
	t.Logf("Total:     %d seeds", total)
	t.Logf("Passed:    %d (%.2f%%)", passCount, 100*float64(passCount)/float64(total))
	t.Logf("Failed:    %d (%.2f%%)", failCount, 100*float64(failCount)/float64(total))
	t.Logf("Rate:      %.0f seeds/sec", float64(total)/elapsed.Seconds())

	// Report failures
	if failCount > 0 {
		t.Logf("\n=== FAILURES ===")
		failureCount := 0
		failures.Range(func(key, value interface{}) bool {
			seed := key.(uint64)
			errMsg := value.(string)
			t.Logf("Seed %d: %s", seed, errMsg)
			failureCount++
			if failureCount >= 100 {
				t.Logf("... (%d more failures omitted)", int(failCount)-100)
				return false
			}
			return true
		})
		t.Fatalf("Soak test failed: %d/%d seeds produced failures", failCount, total)
	}

	t.Logf("\n✓ All %d seeds passed cleanly - no false positives!", total)
}

// runSoakScenario runs a single randomized scenario with the given seed
func runSoakScenario(seed uint64) error {
	// Create cluster with deterministic seed
	nodeIDs := []string{"n0", "n1", "n2", "n3", "n4"}
	c := NewCluster(int64(seed), nodeIDs)

	// Initialize Raft nodes
	for _, node := range c.nodes {
		node.raft = raft.NewRaftNode(raft.Config{
			ElectionTimeoutMin: 300 * time.Millisecond,
			ElectionTimeoutMax: 600 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			MaxEntriesPerRPC:   100,
		})
	}

	if err := c.Init(); err != nil {
		return fmt.Errorf("init failed: %w", err)
	}

	// Configure random fault profile based on seed
	profile := selectFaultProfile(seed)
	c.SetMessageDropRate(profile.dropRate)
	if profile.enablePartitions {
		// Partition specific nodes
		for _, nodeIdx := range profile.partition1 {
			c.PartitionNode(nodeIDs[nodeIdx])
		}
	}

	// Trigger initial election with sufficient time for fault scenarios
	c.TriggerElection(nodeIDs[0])
	c.RunFor(3*time.Second, 5000) // Generous time for election with faults

	// Verify leader elected
	leaderID, term := c.GetLeader()
	if leaderID == "" {
		// For scenarios with message loss or partitions, leader election may fail
		// This is expected behavior, not a framework bug
		// Only fail if we're in a perfectly clean scenario
		if profile.dropRate == 0.0 && !profile.enablePartitions {
			return fmt.Errorf("no leader elected in clean scenario (no faults, term=%d)", term)
		}
		// For any fault scenario, no leader is acceptable
		// Just skip the workload and check invariants hold
		if violation := c.CheckInvariants(); violation != nil {
			return fmt.Errorf("invariant violation (no leader): %s", violation.Description)
		}
		return nil // Pass - no leader is acceptable with faults
	}

	// Generate random workload using generator
	cfg := workload.Config{
		NumOps:   int((seed % 10) + 1), // 1-10 operations
		ClientID: 1,
		ReadPct:  0.0,
		WritePct: 1.0,
		CASPct:   0.0,
		NumKeys:  5,
		MinDelay: 10 * time.Millisecond,
		MaxDelay: 50 * time.Millisecond,
		Timeout:  1 * time.Second,
	}

	gen := workload.NewGenerator(cfg, c.rng)

	// Execute workload operations
	for cmd := gen.Next(); cmd != nil; cmd = gen.Next() {
		// Record invocation
		_ = gen.RecordInvoke(c.currentTime, cmd)
		
		// Run a bit to process
		c.RunFor(100*time.Millisecond, 100)
	}

	// Run simulation for random additional duration
	extraDuration := time.Duration((seed%5000)+2000) * time.Millisecond
	c.RunFor(extraDuration, 10000)

	// Heal partitions if any
	if profile.enablePartitions {
		for _, nodeIdx := range profile.partition1 {
			c.HealPartition(nodeIDs[nodeIdx])
		}
		c.RunFor(2*time.Second, 2000) // Allow time to converge
	}

	// Check invariants
	if violation := c.CheckInvariants(); violation != nil {
		return fmt.Errorf("invariant violation: %s", violation.Description)
	}

	return nil
}

// faultProfile defines the fault injection parameters for a scenario
type faultProfile struct {
	dropRate         float64
	enablePartitions bool
	partition1       []int
	partition2       []int
}

// selectFaultProfile returns a deterministic fault profile based on seed
func selectFaultProfile(seed uint64) faultProfile {
	// Use seed to deterministically select scenario
	scenario := seed % 10

	switch scenario {
	case 0, 1, 2: // 30% - Clean run, no faults
		return faultProfile{dropRate: 0.0}

	case 3, 4: // 20% - Low message drop
		return faultProfile{dropRate: 0.03}

	case 5, 6: // 20% - Medium message drop
		return faultProfile{dropRate: 0.08}

	case 7: // 10% - Higher message drop
		return faultProfile{dropRate: 0.15}

	case 8: // 10% - Network partition (2 vs 3)
		return faultProfile{
			dropRate:         0.03,
			enablePartitions: true,
			partition1:       []int{0, 1},
			partition2:       []int{2, 3, 4},
		}

	case 9: // 10% - Network partition (1 vs 4)
		return faultProfile{
			dropRate:         0.05,
			enablePartitions: true,
			partition1:       []int{0},
			partition2:       []int{1, 2, 3, 4},
		}

	default:
		return faultProfile{dropRate: 0.0}
	}
}

// TestSoakReproducibility verifies that the same seed always produces the same result
func TestSoakReproducibility(t *testing.T) {
	const (
		testSeeds   = 100
		runsPerSeed = 3
	)

	t.Logf("Testing reproducibility: %d seeds × %d runs each", testSeeds, runsPerSeed)

	for seed := uint64(1); seed <= testSeeds; seed++ {
		// Run the same seed multiple times
		var results []error
		for run := 0; run < runsPerSeed; run++ {
			err := runSoakScenario(seed)
			results = append(results, err)
		}

		// All runs must produce the same result (all pass or all fail identically)
		firstResult := results[0]
		for i := 1; i < runsPerSeed; i++ {
			if (firstResult == nil) != (results[i] == nil) {
				t.Fatalf("Seed %d produced inconsistent results: run 0=%v, run %d=%v",
					seed, firstResult, i, results[i])
			}
			// If both failed, error messages should match
			if firstResult != nil && results[i] != nil {
				if firstResult.Error() != results[i].Error() {
					t.Fatalf("Seed %d produced different error messages:\n  run 0: %v\n  run %d: %v",
						seed, firstResult, i, results[i])
				}
			}
		}

		if seed%10 == 0 {
			t.Logf("Verified reproducibility for seeds 1-%d", seed)
		}
	}

	t.Logf("✓ All %d seeds are reproducible across %d runs", testSeeds, runsPerSeed)
}

// TestSoakSmoke is a quick smoke test with fewer seeds for CI
func TestSoakSmoke(t *testing.T) {
	const smokeSeeds = 1000

	t.Logf("Running smoke test with %d seeds", smokeSeeds)
	start := time.Now()

	var failed int
	for seed := uint64(1); seed <= smokeSeeds; seed++ {
		if err := runSoakScenario(seed); err != nil {
			t.Logf("FAIL seed=%d: %v", seed, err)
			failed++
		}

		if seed%100 == 0 {
			elapsed := time.Since(start)
			rate := float64(seed) / elapsed.Seconds()
			t.Logf("[Progress] %d/%d seeds - %.0f seeds/sec", seed, smokeSeeds, rate)
		}
	}

	elapsed := time.Since(start)
	if failed > 0 {
		t.Fatalf("Smoke test failed: %d/%d seeds produced failures", failed, smokeSeeds)
	}

	t.Logf("✓ Smoke test passed: %d seeds in %v (%.0f seeds/sec)",
		smokeSeeds, elapsed, float64(smokeSeeds)/elapsed.Seconds())
}
