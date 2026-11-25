// SPDX-License-Identifier: Apache-2.0

// Package invariants provides Raft safety invariant checkers.
package invariants

import (
	"fmt"
)

// NodeState represents the observable state of a Raft node.
type NodeState struct {
	Role        string
	Term        uint64
	VotedFor    *string
	Log         []LogEntry
	CommitIndex uint64
	Applied     uint64
	Leader      *string
}

// LogEntry represents a log entry.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// Violation represents an invariant violation.
type Violation struct {
	Name        string
	Description string
	Details     map[string]interface{}
}

func (v *Violation) Error() string {
	return fmt.Sprintf("%s: %s", v.Name, v.Description)
}

// CheckElectionSafety verifies that at most one leader exists per term.
// Returns a violation if multiple leaders are found in the same term.
func CheckElectionSafety(states map[string]NodeState) *Violation {
	leadersPerTerm := make(map[uint64][]string)

	for nodeID, state := range states {
		if state.Role == "Leader" {
			leadersPerTerm[state.Term] = append(leadersPerTerm[state.Term], nodeID)
		}
	}

	for term, leaders := range leadersPerTerm {
		if len(leaders) > 1 {
			return &Violation{
				Name:        "ElectionSafety",
				Description: fmt.Sprintf("Multiple leaders in term %d", term),
				Details: map[string]interface{}{
					"term":    term,
					"leaders": leaders,
				},
			}
		}
	}

	return nil
}

// CheckLogMatching verifies the Log Matching Property:
// If two logs contain an entry with the same index and term,
// then the logs are identical in all preceding entries.
func CheckLogMatching(states map[string]NodeState) *Violation {
	// For each pair of nodes, check if they have entries at the same index
	// with the same term, and if so, verify all preceding entries match.
	nodeIDs := make([]string, 0, len(states))
	for id := range states {
		nodeIDs = append(nodeIDs, id)
	}

	for i := 0; i < len(nodeIDs); i++ {
		for j := i + 1; j < len(nodeIDs); j++ {
			id1, id2 := nodeIDs[i], nodeIDs[j]
			log1, log2 := states[id1].Log, states[id2].Log

			// Find common prefix where index and term match
			for idx := uint64(0); idx < uint64(len(log1)) && idx < uint64(len(log2)); idx++ {
				if log1[idx].Index != log2[idx].Index {
					continue // shouldn't happen, but skip
				}
				if log1[idx].Term == log2[idx].Term {
					// Terms match at this index: first verify commands match
					if string(log1[idx].Command) != string(log2[idx].Command) {
						return &Violation{
							Name: "LogMatching",
							Description: fmt.Sprintf(
								"Nodes %s and %s have same term %d at index %d but different commands",
								id1, id2, log1[idx].Term, idx,
							),
							Details: map[string]interface{}{
								"node1":   id1,
								"node2":   id2,
								"index":   idx,
								"term":    log1[idx].Term,
								"command1": string(log1[idx].Command),
								"command2": string(log2[idx].Command),
							},
						}
					}
					
					// Then verify all preceding entries match
					for k := uint64(0); k < idx; k++ {
						if k >= uint64(len(log1)) || k >= uint64(len(log2)) {
							break
						}
						if log1[k].Term != log2[k].Term {
							return &Violation{
								Name: "LogMatching",
								Description: fmt.Sprintf(
									"Nodes %s and %s have matching entry at index %d term %d, but differ at index %d",
									id1, id2, idx, log1[idx].Term, k,
								),
								Details: map[string]interface{}{
									"node1":       id1,
									"node2":       id2,
									"matchIndex":  idx,
									"matchTerm":   log1[idx].Term,
									"conflictIdx": k,
									"term1":       log1[k].Term,
									"term2":       log2[k].Term,
								},
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// CheckLeaderCompleteness verifies the Leader Completeness Property:
// If a log entry is committed in a given term, then that entry will be
// present in the logs of the leaders for all higher-numbered terms.
func CheckLeaderCompleteness(states map[string]NodeState) *Violation {
	// Simplified check: if any node has committed an entry at index I with term T,
	// and there's a leader in a term > T, the leader must have that entry.

	// Find all committed entries (any node's commitIndex)
	maxCommit := uint64(0)
	for _, state := range states {
		if state.CommitIndex > maxCommit {
			maxCommit = state.CommitIndex
		}
	}

	// Collect committed entries from nodes
	committedEntries := make(map[uint64]LogEntry) // index -> entry
	for _, state := range states {
		for i := uint64(1); i <= state.CommitIndex && int(i) < len(state.Log); i++ {
			if existing, ok := committedEntries[i]; ok {
				// Verify consistency: same index should have same term if committed
				if existing.Term != state.Log[i].Term {
					return &Violation{
						Name: "StateMachineSafety",
						Description: fmt.Sprintf(
							"Committed entry at index %d has conflicting terms: %d vs %d",
							i, existing.Term, state.Log[i].Term,
						),
						Details: map[string]interface{}{
							"index": i,
							"term1": existing.Term,
							"term2": state.Log[i].Term,
						},
					}
				}
			} else {
				committedEntries[i] = state.Log[i]
			}
		}
	}

	// Check that all leaders in higher terms have these committed entries
	for nodeID, state := range states {
		if state.Role != "Leader" {
			continue
		}
		for idx, entry := range committedEntries {
			if entry.Term >= state.Term {
				continue // entry is from same or later term, skip
			}
			// Entry is from earlier term and is committed; leader must have it
			if int(idx) >= len(state.Log) {
				return &Violation{
					Name: "LeaderCompleteness",
					Description: fmt.Sprintf(
						"Leader %s (term %d) missing committed entry at index %d from term %d",
						nodeID, state.Term, idx, entry.Term,
					),
					Details: map[string]interface{}{
						"leader":     nodeID,
						"leaderTerm": state.Term,
						"index":      idx,
						"entryTerm":  entry.Term,
					},
				}
			}
			if state.Log[idx].Term != entry.Term {
				return &Violation{
					Name: "LeaderCompleteness",
					Description: fmt.Sprintf(
						"Leader %s (term %d) has different term at index %d: expected %d, got %d",
						nodeID, state.Term, idx, entry.Term, state.Log[idx].Term,
					),
					Details: map[string]interface{}{
						"leader":      nodeID,
						"leaderTerm":  state.Term,
						"index":       idx,
						"expectedTerm": entry.Term,
						"actualTerm":  state.Log[idx].Term,
					},
				}
			}
		}
	}

	return nil
}

// CheckAll runs all invariant checks and returns the first violation found.
func CheckAll(states map[string]NodeState) *Violation {
	if v := CheckElectionSafety(states); v != nil {
		return v
	}
	if v := CheckLogMatching(states); v != nil {
		return v
	}
	if v := CheckLeaderCompleteness(states); v != nil {
		return v
	}
	return nil
}
