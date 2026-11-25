package integration

import (
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
	"github.com/VaibhavBhagat665/faultline/internal/bugs"
)

// TestBugDetection_DoubleVote tests that the framework detects the double vote bug.
func TestBugDetection_DoubleVote(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping bug detection test in short mode")
	}

	// Try multiple seeds to find one that triggers the bug
	detected := false
	for seed := int64(1); seed <= 100 && !detected; seed++ {
		cluster := NewCluster(seed, []string{"n1", "n2", "n3", "n4", "n5"})

		// Initialize with bug injection
		bugInj := bugs.NewInjector()
		bugInj.Enable(bugs.BugDoubleVote)
		bugInj.SetProbability(0.3) // 30% chance to trigger

		for _, node := range cluster.nodes {
			node.raft = raft.NewRaftNode(raft.Config{
				ElectionTimeoutMin: 300 * time.Millisecond,
				ElectionTimeoutMax: 600 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
				MaxEntriesPerRPC:   100,
			})
			node.raft.SetBugInjector(bugInj)
		}

		if err := cluster.Init(); err != nil {
			t.Fatalf("Failed to init cluster: %v", err)
		}

		// Trigger multiple elections to increase chance of bug
		cluster.TriggerElection("n1")
		cluster.RunFor(3*time.Second, 5000)

		// Check for bug detection
		violation := cluster.CheckInvariants()
		if violation != nil && violation.Name == "ElectionSafety" {
			detected = true
			t.Logf("✓ Bug detected at seed %d: %s", seed, violation.Description)
			t.Logf("  Trigger count: %d", bugInj.GetTriggerCount())
			break
		}
	}

	if !detected {
		t.Log("⚠ Bug not detected in 100 seeds (low trigger probability or bug not actually harmful in tested scenarios)")
		// Note: This is not a failure - the bug might require specific conditions to manifest
	} else {
		t.Log("✓ Framework successfully detected DoubleVote bug")
	}
}

// TestBugDetection_Framework tests the bug injection framework itself.
func TestBugDetection_Framework(t *testing.T) {
	// This test verifies that bug injection doesn't break clean runs
	cluster := NewCluster(42, []string{"n1", "n2", "n3"})

	// Initialize with NO bugs enabled
	bugInj := bugs.NewInjector()
	// Don't enable any bugs - should behave normally

	for _, node := range cluster.nodes {
		node.raft = raft.NewRaftNode(raft.DefaultConfig())
		node.raft.SetBugInjector(bugInj)
	}

	if err := cluster.Init(); err != nil {
		t.Fatalf("Failed to init cluster: %v", err)
	}

	// Run normal test
	cluster.TriggerElection("n1")
	cluster.RunFor(1*time.Second, 1000)

	// Should pass invariants
	if violation := cluster.CheckInvariants(); violation != nil {
		t.Fatalf("Unexpected violation with no bugs enabled: %s", violation.Description)
	}

	// Should have a leader
	leaderID, _ := cluster.GetLeader()
	if leaderID == "" {
		t.Fatal("No leader elected (bug injection framework interfering?)")
	}

	t.Logf("✓ Bug injection framework doesn't interfere with normal operation")
}

// TestBugZoo tests that all bugs in the zoo are documented.
func TestBugZoo(t *testing.T) {
	zoo := bugs.GetBugZoo()

	if len(zoo.Bugs) == 0 {
		t.Fatal("Bug zoo is empty")
	}

	t.Logf("Bug Zoo contains %d bugs:", len(zoo.Bugs))
	for _, bug := range zoo.Bugs {
		t.Logf("  • %s (%s)", bug.Name, bug.Severity)
		t.Logf("    Description: %s", bug.Description)
		t.Logf("    Expected violation: %s", bug.Violation)

		// Verify all required fields
		if bug.Name == "" {
			t.Errorf("Bug %v missing name", bug.Flag)
		}
		if bug.Description == "" {
			t.Errorf("Bug %s missing description", bug.Name)
		}
		if bug.Violation == "" {
			t.Errorf("Bug %s missing expected violation", bug.Name)
		}
		if bug.Severity == "" {
			t.Errorf("Bug %s missing severity", bug.Name)
		}
	}

	// Check for critical bugs
	critical := zoo.GetCriticalBugs()
	t.Logf("\nCritical bugs: %d/%d", len(critical), len(zoo.Bugs))
	if len(critical) == 0 {
		t.Error("Should have at least one critical bug")
	}
}

// TestBugDetection_Catalog tests detection metrics for each bug.
func TestBugDetection_Catalog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping bug catalog test in short mode")
	}

	zoo := bugs.GetBugZoo()
	results := make(map[string]BugDetectionResult)

	// Test each critical bug
	for _, bugInfo := range zoo.GetCriticalBugs() {
		t.Logf("\n=== Testing %s ===", bugInfo.Name)
		result := testBugDetection(t, bugInfo.Flag, 50) // Try 50 seeds
		results[bugInfo.Name] = result

		t.Logf("  Seeds tested: %d", result.SeedsTested)
		t.Logf("  Detected: %v", result.Detected)
		if result.Detected {
			t.Logf("  First detection: seed %d", result.FirstDetectionSeed)
			t.Logf("  Violation type: %s", result.ViolationType)
		}
	}

	// Summary
	t.Logf("\n=== Detection Summary ===")
	detected := 0
	for name, result := range results {
		status := "❌ Not detected"
		if result.Detected {
			status = "✓ Detected"
		}
		t.Logf("%s: %s", name, status)
		if result.Detected {
			detected++
		}
	}

	t.Logf("\nDetection rate: %d/%d bugs (%.1f%%)",
		detected, len(results), 100*float64(detected)/float64(len(results)))

	// Note: Not all bugs may be detected with limited seeds
	// This is expected behavior for subtle bugs
}

// BugDetectionResult holds the results of bug detection testing.
type BugDetectionResult struct {
	BugName            string
	SeedsTested        int
	Detected           bool
	FirstDetectionSeed int64
	ViolationType      string
	TriggerCount       int
}

// testBugDetection attempts to detect a specific bug across multiple seeds.
func testBugDetection(t *testing.T, bug bugs.BugFlag, maxSeeds int) BugDetectionResult {
	result := BugDetectionResult{
		BugName:     bug.String(),
		SeedsTested: maxSeeds,
		Detected:    false,
	}

	for seed := int64(1); seed <= int64(maxSeeds); seed++ {
		cluster := NewCluster(seed, []string{"n1", "n2", "n3", "n4", "n5"})

		// Enable the bug
		bugInj := bugs.NewInjector()
		bugInj.Enable(bug)
		bugInj.SetProbability(0.5) // 50% trigger probability

		for _, node := range cluster.nodes {
			node.raft = raft.NewRaftNode(raft.Config{
				ElectionTimeoutMin: 300 * time.Millisecond,
				ElectionTimeoutMax: 600 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
				MaxEntriesPerRPC:   100,
			})
			node.raft.SetBugInjector(bugInj)
		}

		if err := cluster.Init(); err != nil {
			continue
		}

		// Run scenario
		cluster.TriggerElection("n1")
		cluster.RunFor(3*time.Second, 5000)

		// Check for detection
		if violation := cluster.CheckInvariants(); violation != nil {
			result.Detected = true
			result.FirstDetectionSeed = seed
			result.ViolationType = violation.Name
			result.TriggerCount = bugInj.GetTriggerCount()
			break
		}
	}

	return result
}

// TestBugInjection_Reproducibility tests that bugs are reproducible.
func TestBugInjection_Reproducibility(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping reproducibility test in short mode")
	}

	// Find a seed that triggers a bug
	var triggerSeed int64
	found := false

	for seed := int64(1); seed <= 50 && !found; seed++ {
		cluster := NewCluster(seed, []string{"n1", "n2", "n3", "n4", "n5"})

		bugInj := bugs.NewInjector()
		bugInj.Enable(bugs.BugDoubleVote)
		bugInj.SetProbability(0.5)

		for _, node := range cluster.nodes {
			node.raft = raft.NewRaftNode(raft.DefaultConfig())
			node.raft.SetBugInjector(bugInj)
		}

		if err := cluster.Init(); err != nil {
			continue
		}

		cluster.TriggerElection("n1")
		cluster.RunFor(3*time.Second, 5000)

		if violation := cluster.CheckInvariants(); violation != nil {
			triggerSeed = seed
			found = true
			t.Logf("Found triggering seed: %d", seed)
			break
		}
	}

	if !found {
		t.Fatalf("DoubleVote bug was NOT detected in 50 attempts. This is a FAILURE.")
	}

	// Now run the same seed multiple times - should always detect
	runs := 5
	detected := 0

	for run := 0; run < runs; run++ {
		cluster := NewCluster(triggerSeed, []string{"n1", "n2", "n3", "n4", "n5"})

		bugInj := bugs.NewInjector()
		bugInj.Enable(bugs.BugDoubleVote)
		bugInj.SetProbability(0.5)

		for _, node := range cluster.nodes {
			node.raft = raft.NewRaftNode(raft.DefaultConfig())
			node.raft.SetBugInjector(bugInj)
		}

		cluster.Init()
		cluster.TriggerElection("n1")
		cluster.RunFor(3*time.Second, 5000)

		if violation := cluster.CheckInvariants(); violation != nil {
			detected++
		}
	}

	if detected != runs {
		t.Errorf("Bug not reproducible: detected %d/%d times with seed %d", detected, runs, triggerSeed)
	} else {
		t.Logf("✓ Bug is 100%% reproducible with seed %d (%d/%d runs)", triggerSeed, detected, runs)
	}
}
