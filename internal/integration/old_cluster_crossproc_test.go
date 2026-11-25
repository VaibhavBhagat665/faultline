// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
)

// TestOldCluster_CrossProcessDeterminism records failure of OLD Cluster architecture
// This test documents that the current Cluster is nondeterministic across processes.
func TestOldCluster_CrossProcessDeterminism(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cross-process test in short mode")
	}

	const processes = 5
	const seeds = 5

	results := make(map[int64][]seedResult)

	for seed := int64(1); seed <= seeds; seed++ {
		for proc := 0; proc < processes; proc++ {
			result, err := runOldClusterInProcess(seed)
			if err != nil {
				t.Fatalf("Seed %d process %d failed: %v", seed, proc, err)
			}
			results[seed] = append(results[seed], result)
		}
	}

	// Check for nondeterminism
	var nondeterministicSeeds []int64
	for seed := int64(1); seed <= seeds; seed++ {
		runs := results[seed]
		firstHash := runs[0].hash
		firstLeader := runs[0].leader
		firstTerm := runs[0].term

		for i := 1; i < len(runs); i++ {
			if runs[i].hash != firstHash || runs[i].leader != firstLeader || runs[i].term != firstTerm {
				nondeterministicSeeds = append(nondeterministicSeeds, seed)
				break
			}
		}
	}

	// Report results
	if len(nondeterministicSeeds) == 0 {
		t.Logf("✓ PASS: All %d seeds deterministic across %d processes", seeds, processes)
		t.Logf("OLD Cluster is deterministic (unexpected but good)")
	} else {
		t.Logf("✗ FAIL: %d/%d seeds nondeterministic", len(nondeterministicSeeds), seeds)
		t.Logf("Nondeterministic seeds: %v", nondeterministicSeeds)

		// Show details for first 3
		for i, seed := range nondeterministicSeeds {
			if i >= 3 {
				break
			}
			runs := results[seed]
			t.Logf("\nSeed %d variations:", seed)
			uniqueOutcomes := make(map[string]int)
			for _, r := range runs {
				key := fmt.Sprintf("leader=%s term=%d log=%d hash=%s", r.leader, r.term, r.logLen, r.hash[:16])
				uniqueOutcomes[key]++
			}
			for outcome, count := range uniqueOutcomes {
				t.Logf("  %dx: %s", count, outcome)
			}
		}

		t.Fatalf("OLD Cluster is nondeterministic (as expected)")
	}
}

type seedResult struct {
	seed   int64
	hash   string
	leader string
	term   uint64
	logLen int
}

// runOldClusterInProcess runs seed in separate OS process
func runOldClusterInProcess(seed int64) (seedResult, error) {
	cmd := exec.Command("go", "test", "-run", "^TestOldCluster_OneShot$", "-timeout", "30s")
	cmd.Dir, _ = os.Getwd()
	cmd.Env = append(os.Environ(), fmt.Sprintf("SEED=%d", seed))

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return seedResult{}, fmt.Errorf("command failed: %v\nstderr: %s", err, stderr.String())
	}

	// Parse JSON output
	var result seedResult
	output := stdout.String()
	start := bytes.Index([]byte(output), []byte("RESULT: "))
	if start == -1 {
		return seedResult{}, fmt.Errorf("no RESULT in output:\n%s", output)
	}
	start += len("RESULT: ")
	end := bytes.IndexByte([]byte(output[start:]), '\n')
	if end == -1 {
		end = len(output) - start
	}
	jsonStr := output[start : start+end]

	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		return seedResult{}, fmt.Errorf("failed to parse JSON: %v\nJSON: %s", err, jsonStr)
	}

	return result, nil
}

// TestOldCluster_OneShot is entry point for cross-process runs
func TestOldCluster_OneShot(t *testing.T) {
	seedStr := os.Getenv("SEED")
	if seedStr == "" {
		t.Skip("SEED not set")
	}

	var seed int64
	fmt.Sscanf(seedStr, "%d", &seed)

	// Run OLD cluster architecture
	cluster := NewCluster(seed, []string{"n1", "n2", "n3", "n4", "n5"})

	for _, node := range cluster.nodes {
		node.raft = raft.NewRaftNode(raft.DefaultConfig())
	}

	if err := cluster.Init(); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	cluster.TriggerElection("n1")
	cluster.RunFor(2*time.Second, 3000)

	leader, term := cluster.GetLeader()
	states := cluster.GetNodeStates()

	var logLen int
	if leader != "" {
		logLen = len(states[leader].Log)
	}

	// Compute hash of outcome (leader, term, all logs sorted)
	var allLogs []string
	nodeIDs := []string{"n1", "n2", "n3", "n4", "n5"}
	sort.Strings(nodeIDs)
	for _, id := range nodeIDs {
		state := states[id]
		allLogs = append(allLogs, fmt.Sprintf("%s:%d:%d:%v", id, state.Term, len(state.Log), state.Log))
	}
	outcomeStr := fmt.Sprintf("leader=%s term=%d logs=%v", leader, term, allLogs)
	hash := fmt.Sprintf("%x", []byte(outcomeStr)) // Simple hash for now

	result := struct {
		Seed   int64  `json:"seed"`
		Hash   string `json:"hash"`
		Leader string `json:"leader"`
		Term   uint64 `json:"term"`
		LogLen int    `json:"logLen"`
	}{
		Seed:   seed,
		Hash:   hash,
		Leader: leader,
		Term:   term,
		LogLen: logLen,
	}

	jsonBytes, _ := json.Marshal(result)
	fmt.Printf("RESULT: %s\n", string(jsonBytes))
}
