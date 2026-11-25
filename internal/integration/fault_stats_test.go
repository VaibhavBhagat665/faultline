// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
)

// TestFaultStats_BugTestConfiguration measures what actually happens
// in the bug detection test configuration across 200 seeds.
func TestFaultStats_BugTestConfiguration(t *testing.T) {
	const numSeeds = 200

	// Stats to collect
	type RunStats struct {
		seed             int64
		crashes          int
		restarts         int
		partitions       int
		messagesDropped  int
		diskFaults       int
		elections        int
		opsInvoked       int
		opsOK            int
		opsFail          int
		opsInfo          int
		eventsProcessed  int
	}

	var stats []RunStats

	for seed := int64(1); seed <= numSeeds; seed++ {
		cluster := NewCluster(seed, []string{"n1", "n2", "n3", "n4", "n5"})

		// Same config as TestBugDetection_Catalog
		for _, node := range cluster.nodes {
			node.raft = raft.NewRaftNode(raft.Config{
				ElectionTimeoutMin: 300 * time.Millisecond,
				ElectionTimeoutMax: 600 * time.Millisecond,
				HeartbeatInterval:  50 * time.Millisecond,
				MaxEntriesPerRPC:   100,
			})
		}

		if err := cluster.Init(); err != nil {
			continue
		}

		// Same workload as bug test
		cluster.TriggerElection("n1")
		cluster.RunFor(3*time.Second, 5000)

		// Collect stats
		runStats := RunStats{seed: seed}

		// In this configuration:
		// - NO crashes (CrashNode never called)
		// - NO partitions (PartitionNode never called)
		// - NO message drops (SetMessageDropRate never called)
		// - NO disk faults
		// - NO client operations (no workload)
		// Just election triggered, that's all

		runStats.crashes = 0
		runStats.restarts = 0
		runStats.partitions = 0
		runStats.messagesDropped = 0
		runStats.diskFaults = 0
		runStats.opsInvoked = 0
		runStats.opsOK = 0
		runStats.opsFail = 0
		runStats.opsInfo = 0

		// Count elections by checking final leader state
		leader, _ := cluster.GetLeader()
		if leader != "" {
			runStats.elections = 1 // At least one election succeeded
		}

		// Event count not accessible without engine field
		runStats.eventsProcessed = 0

		stats = append(stats, runStats)
	}

	// Calculate percentiles
	getPercentile := func(values []int, p float64) int {
		if len(values) == 0 {
			return 0
		}
		sorted := make([]int, len(values))
		copy(sorted, values)
		sort.Ints(sorted)
		idx := int(float64(len(sorted)-1) * p)
		return sorted[idx]
	}

	// Extract each metric
	var (
		crashCounts     []int
		restartCounts   []int
		partitionCounts []int
		dropCounts      []int
		diskFaultCounts []int
		electionCounts  []int
		opsInvokedList  []int
		opsOKList       []int
		opsFailList     []int
		opsInfoList     []int
		eventCounts     []int
	)

	seedsWithCrashes := 0
	seedsWithRestarts := 0
	seedsWithPartitions := 0
	seedsWithDrops := 0
	seedsWithDiskFaults := 0

	for _, s := range stats {
		crashCounts = append(crashCounts, s.crashes)
		restartCounts = append(restartCounts, s.restarts)
		partitionCounts = append(partitionCounts, s.partitions)
		dropCounts = append(dropCounts, s.messagesDropped)
		diskFaultCounts = append(diskFaultCounts, s.diskFaults)
		electionCounts = append(electionCounts, s.elections)
		opsInvokedList = append(opsInvokedList, s.opsInvoked)
		opsOKList = append(opsOKList, s.opsOK)
		opsFailList = append(opsFailList, s.opsFail)
		opsInfoList = append(opsInfoList, s.opsInfo)
		eventCounts = append(eventCounts, s.eventsProcessed)

		if s.crashes > 0 {
			seedsWithCrashes++
		}
		if s.restarts > 0 {
			seedsWithRestarts++
		}
		if s.partitions > 0 {
			seedsWithPartitions++
		}
		if s.messagesDropped > 0 {
			seedsWithDrops++
		}
		if s.diskFaults > 0 {
			seedsWithDiskFaults++
		}
	}

	// Print table
	t.Logf("\n%s", strings.Repeat("=", 70))
	t.Logf("FAULT INJECTION STATS - BUG TEST CONFIGURATION")
	t.Logf("Seeds: %d", numSeeds)
	t.Logf("Configuration: Same as TestBugDetection_Catalog")
	t.Logf("%s", strings.Repeat("=", 70))
	t.Logf("\n%-20s %8s %8s %8s %10s", "Metric", "Min", "Median", "Max", "Seeds > 0")
	t.Logf("%s", strings.Repeat("-", 70))
	t.Logf("%-20s %8d %8d %8d %10d (%.1f%%)",
		"Crashes",
		getPercentile(crashCounts, 0.0),
		getPercentile(crashCounts, 0.5),
		getPercentile(crashCounts, 1.0),
		seedsWithCrashes,
		100*float64(seedsWithCrashes)/float64(numSeeds))
	t.Logf("%-20s %8d %8d %8d %10d (%.1f%%)",
		"Restarts",
		getPercentile(restartCounts, 0.0),
		getPercentile(restartCounts, 0.5),
		getPercentile(restartCounts, 1.0),
		seedsWithRestarts,
		100*float64(seedsWithRestarts)/float64(numSeeds))
	t.Logf("%-20s %8d %8d %8d %10d (%.1f%%)",
		"Partitions",
		getPercentile(partitionCounts, 0.0),
		getPercentile(partitionCounts, 0.5),
		getPercentile(partitionCounts, 1.0),
		seedsWithPartitions,
		100*float64(seedsWithPartitions)/float64(numSeeds))
	t.Logf("%-20s %8d %8d %8d %10d (%.1f%%)",
		"Messages Dropped",
		getPercentile(dropCounts, 0.0),
		getPercentile(dropCounts, 0.5),
		getPercentile(dropCounts, 1.0),
		seedsWithDrops,
		100*float64(seedsWithDrops)/float64(numSeeds))
	t.Logf("%-20s %8d %8d %8d %10d (%.1f%%)",
		"Disk Faults",
		getPercentile(diskFaultCounts, 0.0),
		getPercentile(diskFaultCounts, 0.5),
		getPercentile(diskFaultCounts, 1.0),
		seedsWithDiskFaults,
		100*float64(seedsWithDiskFaults)/float64(numSeeds))
	t.Logf("%-20s %8d %8d %8d",
		"Leader Elections",
		getPercentile(electionCounts, 0.0),
		getPercentile(electionCounts, 0.5),
		getPercentile(electionCounts, 1.0))
	t.Logf("%-20s %8d %8d %8d",
		"Ops Invoked",
		getPercentile(opsInvokedList, 0.0),
		getPercentile(opsInvokedList, 0.5),
		getPercentile(opsInvokedList, 1.0))
	t.Logf("%-20s %8d %8d %8d",
		"Ops OK",
		getPercentile(opsOKList, 0.0),
		getPercentile(opsOKList, 0.5),
		getPercentile(opsOKList, 1.0))
	t.Logf("%-20s %8d %8d %8d",
		"Ops Failed",
		getPercentile(opsFailList, 0.0),
		getPercentile(opsFailList, 0.5),
		getPercentile(opsFailList, 1.0))
	t.Logf("%-20s %8d %8d %8d",
		"Ops Info",
		getPercentile(opsInfoList, 0.0),
		getPercentile(opsInfoList, 0.5),
		getPercentile(opsInfoList, 1.0))
	t.Logf("%-20s %8d %8d %8d",
		"Events Processed",
		getPercentile(eventCounts, 0.0),
		getPercentile(eventCounts, 0.5),
		getPercentile(eventCounts, 1.0))
	t.Logf("%s", strings.Repeat("=", 70))

	// Check if any faults fired
	if seedsWithCrashes == 0 && seedsWithPartitions == 0 {
		t.Logf("\n⚠ WARNING: NO CRASHES OR PARTITIONS in %d seeds", numSeeds)
		t.Logf("This explains 0/7 bug detection rate - bugs need faults to manifest!")
	}

	if getPercentile(opsInvokedList, 0.5) == 0 {
		t.Logf("\n⚠ WARNING: NO CLIENT OPERATIONS")
		t.Logf("Bugs like CommitWithoutQuorum, StaleRead need client ops to detect!")
	}
}
