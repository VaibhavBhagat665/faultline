package integration

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
	"github.com/VaibhavBhagat665/faultline/internal/viz"
)

// TestVisualization_GenerateTimeline tests timeline generation from a test run.
func TestVisualization_GenerateTimeline(t *testing.T) {
	// Create output directory
	outputDir := "tmp/viz"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	// Run a simple test scenario
	cluster := NewCluster(42, []string{"n1", "n2", "n3"})

	// Initialize nodes
	for _, node := range cluster.nodes {
		node.raft = raft.NewRaftNode(raft.DefaultConfig())
	}

	if err := cluster.Init(); err != nil {
		t.Fatalf("Failed to init cluster: %v", err)
	}

	// Capture timeline
	capture := viz.NewTimelineCapture([]string{"n1", "n2", "n3"})

	// Record initial state
	for _, node := range cluster.nodes {
		capture.RecordStateChange(
			int64(cluster.currentTime),
			string(node.id),
			"",
			"follower",
			0,
		)
	}

	// Run election
	capture.RecordElection(int64(cluster.currentTime), "n1", 1)
	cluster.TriggerElection("n1")

	// Run for a bit
	startTime := cluster.currentTime
	cluster.RunFor(1*time.Second, 1000)

	// Record some mock events for visualization
	capture.RecordVote(int64(startTime+100), "n2", "n1", 1, true)
	capture.RecordVote(int64(startTime+150), "n3", "n1", 1, true)
	capture.RecordStateChange(int64(startTime+200), "n1", "candidate", "leader", 1)
	capture.RecordHeartbeat(int64(startTime+300), "n1", "n2", 1)
	capture.RecordHeartbeat(int64(startTime+300), "n1", "n3", 1)
	capture.RecordCommit(int64(startTime+500), "n1", 1, 1)

	// Generate HTML
	timeline := capture.GetTimeline()
	outputFile := filepath.Join(outputDir, "election_timeline.html")

	f, err := os.Create(outputFile)
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer f.Close()

	if err := timeline.GenerateHTML(f, "Raft Election Test"); err != nil {
		t.Fatalf("Failed to generate HTML: %v", err)
	}

	t.Logf("✓ Timeline generated: %s", outputFile)
	t.Log("  Open the file in a browser to view the interactive timeline")
}

// TestVisualization_WithViolation tests timeline with invariant violations.
func TestVisualization_WithViolation(t *testing.T) {
	outputDir := "tmp/viz"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	// Create timeline with a violation
	capture := viz.NewTimelineCapture([]string{"n1", "n2", "n3"})

	// Simulate a scenario leading to violation
	capture.RecordElection(1000, "n1", 1)
	capture.RecordVote(1100, "n2", "n1", 1, true)
	capture.RecordStateChange(1200, "n1", "candidate", "leader", 1)

	// Simultaneous election on different node (violation!)
	capture.RecordElection(1150, "n3", 1)
	capture.RecordStateChange(1250, "n3", "candidate", "leader", 1)

	// Record the violation
	capture.RecordViolation(
		1300,
		"ElectionSafety",
		"Multiple leaders in term 1: n1 and n3",
		"Critical",
	)

	// Generate HTML
	timeline := capture.GetTimeline()
	outputFile := filepath.Join(outputDir, "violation_timeline.html")

	f, err := os.Create(outputFile)
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer f.Close()

	if err := timeline.GenerateHTML(f, "Raft Test with Violation"); err != nil {
		t.Fatalf("Failed to generate HTML: %v", err)
	}

	t.Logf("✓ Violation timeline generated: %s", outputFile)
	t.Log("  The timeline shows the violation prominently")
}

// TestVisualization_ComplexScenario tests a complex scenario with many events.
func TestVisualization_ComplexScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping complex visualization test in short mode")
	}

	outputDir := "tmp/viz"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	cluster := NewCluster(123, []string{"n1", "n2", "n3", "n4", "n5"})

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

	capture := viz.NewTimelineCapture([]string{"n1", "n2", "n3", "n4", "n5"})

	// Run a complex scenario
	// 1. Initial election
	capture.RecordElection(int64(cluster.currentTime), "n1", 1)
	cluster.TriggerElection("n1")
	cluster.RunFor(1*time.Second, 1000)

	// 2. Some operations
	currentTime := int64(cluster.currentTime)
	for i := 0; i < 5; i++ {
		capture.RecordAppendEntries(currentTime+int64(i*100), "n1", "n2", 1, 1, true)
		capture.RecordCommit(currentTime+int64(i*100+50), "n1", 1, uint64(i+1))
	}

	// 3. Network partition
	cluster.PartitionNode("n1")
	cluster.RunFor(2*time.Second, 2000)

	// 4. New election
	capture.RecordElection(currentTime+2000, "n2", 2)

	// 5. Heal partition
	cluster.HealPartition("n1")
	cluster.RunFor(1*time.Second, 1000)

	// Generate timeline
	timeline := capture.GetTimeline()
	outputFile := filepath.Join(outputDir, "complex_scenario.html")

	f, err := os.Create(outputFile)
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}
	defer f.Close()

	if err := timeline.GenerateHTML(f, "Complex Raft Scenario"); err != nil {
		t.Fatalf("Failed to generate HTML: %v", err)
	}

	t.Logf("✓ Complex timeline generated: %s", outputFile)
	t.Logf("  Events captured: %d", len(timeline.Events))
}
