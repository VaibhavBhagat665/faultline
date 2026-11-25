// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VaibhavBhagat665/faultline/adapters/raft"
)

// CrossProcResult has EXPORTED fields: encoding/json ignores lowercase ones.
type CrossProcResult struct {
	Seed    int64  `json:"seed"`
	Profile string `json:"profile"`
	Hash    string `json:"hash"`
	Leader  string `json:"leader"`
	Term    uint64 `json:"term"`
	Summary string `json:"summary"`
}

func crossProcHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// runCrossProcScenario is executed inside a worker process.
func runCrossProcScenario(seed int64, profile string) CrossProcResult {
	res := CrossProcResult{Seed: seed, Profile: profile}

	// Negative control: output depends on map iteration order, so it MUST differ across processes.
	if profile == "negctl_map" {
		m := map[int]int{}
		for i := 0; i < 64; i++ {
			m[i*7+int(seed)] = i
		}
		var sb strings.Builder
		for k := range m {
			fmt.Fprintf(&sb, "%d,", k)
		}
		res.Summary = sb.String()
		res.Hash = crossProcHash(res.Summary)
		return res
	}

	ids := []string{"n1", "n2", "n3", "n4", "n5"}
	c := NewCluster(seed, ids)
	for _, n := range c.nodes {
		n.raft = raft.NewRaftNode(raft.DefaultConfig())
	}
	switch profile {
	case "drop8":
		c.messageDropRate = 0.08
	case "drop15":
		c.messageDropRate = 0.15
	}
	if err := c.Init(); err != nil {
		res.Summary = "init error: " + err.Error()
		return res // empty Hash makes the parent fail loudly
	}
	c.TriggerElection("n1")
	c.RunFor(300*time.Millisecond, 2000)
	c.TriggerElection("n3") // competing candidate: outcome depends on event ordering
	c.RunFor(2*time.Second, 5000)

	leader, term := c.GetLeader()
	states := c.GetNodeStates()
	sort.Strings(ids)
	var sb strings.Builder
	fmt.Fprintf(&sb, "leader=%s term=%d;", leader, term)
	for _, id := range ids {
		st := states[id]
		fmt.Fprintf(&sb, "%s:t%d:log%d;", id, st.Term, len(st.Log))
	}
	res.Leader, res.Term, res.Summary = leader, term, sb.String()
	res.Hash = crossProcHash(res.Summary)
	return res
}

// TestCrossProcWorker only does work when launched by the parent test.
func TestCrossProcWorker(t *testing.T) {
	s := os.Getenv("CROSSPROC_SEED")
	if s == "" {
		return
	}
	seed, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("bad seed %q: %v", s, err)
	}
	r := runCrossProcScenario(seed, os.Getenv("CROSSPROC_PROFILE"))
	b, _ := json.Marshal(r)
	fmt.Printf("RESULT %s\n", b)
}

func runCrossProcWorker(t *testing.T, seed int64, profile string) CrossProcResult {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrossProcWorker$", "-test.v", "-test.timeout=120s")
	cmd.Env = append(os.Environ(),
		"CROSSPROC_SEED="+strconv.FormatInt(seed, 10),
		"CROSSPROC_PROFILE="+profile)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("worker failed (profile=%s seed=%d): %v\n%s", profile, seed, err, out.String())
	}
	sc := bufio.NewScanner(&out)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if line := sc.Text(); strings.HasPrefix(line, "RESULT ") {
			var r CrossProcResult
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RESULT ")), &r); err != nil {
				t.Fatalf("bad RESULT json: %v\n%s", err, line)
			}
			return r
		}
	}
	t.Fatalf("no RESULT line (profile=%s seed=%d):\n%s", profile, seed, out.String())
	return CrossProcResult{}
}

func TestCrossProcDeterminism(t *testing.T) {
	seeds, procs := 20, 6
	if v := os.Getenv("CROSSPROC_SEEDS"); v != "" {
		seeds, _ = strconv.Atoi(v)
	}
	if v := os.Getenv("CROSSPROC_PROCS"); v != "" {
		procs, _ = strconv.Atoi(v)
	}

	for _, profile := range []string{"clean", "drop8", "drop15"} {
		diverged := 0
		outcomes := map[string]bool{}
		for seed := int64(1); seed <= int64(seeds); seed++ {
			hashes := map[string]bool{}
			var first CrossProcResult
			for p := 0; p < procs; p++ {
				r := runCrossProcWorker(t, seed, profile)
				if p == 0 {
					first = r
					t.Logf("profile=%s seed=%d hash=%.12s %s", profile, seed, r.Hash, r.Summary)
				}
				hashes[r.Hash] = true
			}
			if first.Hash == "" {
				t.Errorf("profile=%s seed=%d: empty hash (%s)", profile, seed, first.Summary)
			}
			outcomes[first.Hash] = true
			if len(hashes) != 1 {
				diverged++
				t.Errorf("profile=%s seed=%d: %d distinct hashes across %d processes", profile, seed, len(hashes), procs)
			}
		}
		t.Logf("TABLE profile=%-6s seeds=%d procs=%d diverged_seeds=%d distinct_outcomes_across_seeds=%d",
			profile, seeds, procs, diverged, len(outcomes))
		if len(outcomes) < 5 {
			t.Errorf("profile=%s: only %d distinct outcomes across %d seeds; summary too coarse to detect anything", profile, len(outcomes), seeds)
		}
	}

	// Negative control: this MUST diverge, otherwise the check cannot detect nondeterminism.
	hashes := map[string]bool{}
	for p := 0; p < 8; p++ {
		hashes[runCrossProcWorker(t, 1, "negctl_map").Hash] = true
	}
	t.Logf("TABLE negative_control distinct_hashes=%d (must be > 1)", len(hashes))
	if len(hashes) < 2 {
		t.Errorf("negative control did not diverge: this check cannot detect nondeterminism")
	}
}
