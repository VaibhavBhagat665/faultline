// SPDX-License-Identifier: Apache-2.0

// Package bugs provides a bug injection framework for testing the simulation
// framework's ability to detect known bugs.
package bugs

import (
	"sync/atomic"
)

// BugFlag represents an intentional bug that can be injected.
type BugFlag int32

const (
	// BugNone indicates no bugs are active.
	BugNone BugFlag = 0

	// BugDropCommit causes the leader to occasionally drop commit messages.
	// This violates the Raft invariant that committed entries must be
	// replicated to a majority.
	BugDropCommit BugFlag = 1 << 0

	// BugIgnoreVote causes a candidate to ignore some votes during election.
	// This can lead to multiple leaders in the same term.
	BugIgnoreVote BugFlag = 1 << 1

	// BugWrongTerm causes nodes to use incorrect term numbers.
	// This violates the monotonic term requirement.
	BugWrongTerm BugFlag = 1 << 2

	// BugSkipPersist causes nodes to skip persisting critical state.
	// This can lead to data loss after crashes.
	BugSkipPersist BugFlag = 1 << 3

	// BugLogOverwrite causes nodes to overwrite committed log entries.
	// This violates the log matching property.
	BugLogOverwrite BugFlag = 1 << 4

	// BugDoubleVote allows nodes to vote for multiple candidates in same term.
	// This can lead to multiple leaders.
	BugDoubleVote BugFlag = 1 << 5

	// BugCommitWithoutQuorum allows leader to commit without majority.
	// This violates safety properties.
	BugCommitWithoutQuorum BugFlag = 1 << 6

	// BugStaleRead causes followers to serve stale reads.
	// This violates linearizability.
	BugStaleRead BugFlag = 1 << 7

	// BugMissingHeartbeat causes leader to skip sending heartbeats.
	// This can lead to spurious elections.
	BugMissingHeartbeat BugFlag = 1 << 8
)

// String returns a human-readable name for the bug.
func (b BugFlag) String() string {
	switch b {
	case BugNone:
		return "None"
	case BugDropCommit:
		return "DropCommit"
	case BugIgnoreVote:
		return "IgnoreVote"
	case BugWrongTerm:
		return "WrongTerm"
	case BugSkipPersist:
		return "SkipPersist"
	case BugLogOverwrite:
		return "LogOverwrite"
	case BugDoubleVote:
		return "DoubleVote"
	case BugCommitWithoutQuorum:
		return "CommitWithoutQuorum"
	case BugStaleRead:
		return "StaleRead"
	case BugMissingHeartbeat:
		return "MissingHeartbeat"
	default:
		return "Unknown"
	}
}

// Description returns a detailed description of the bug.
func (b BugFlag) Description() string {
	switch b {
	case BugNone:
		return "No bugs injected"
	case BugDropCommit:
		return "Leader occasionally drops commit messages, violating replication guarantees"
	case BugIgnoreVote:
		return "Candidate ignores some votes, potentially leading to multiple leaders"
	case BugWrongTerm:
		return "Nodes use incorrect term numbers, violating term monotonicity"
	case BugSkipPersist:
		return "Nodes skip persisting state, leading to data loss after crashes"
	case BugLogOverwrite:
		return "Nodes overwrite committed log entries, violating log matching"
	case BugDoubleVote:
		return "Nodes vote for multiple candidates in same term"
	case BugCommitWithoutQuorum:
		return "Leader commits without majority quorum"
	case BugStaleRead:
		return "Followers serve stale reads, violating linearizability"
	case BugMissingHeartbeat:
		return "Leader skips sending heartbeats, causing spurious elections"
	default:
		return "Unknown bug"
	}
}

// ExpectedViolation returns the invariant violation this bug should trigger.
func (b BugFlag) ExpectedViolation() string {
	switch b {
	case BugNone:
		return ""
	case BugDropCommit:
		return "CommittedEntriesMatch"
	case BugIgnoreVote:
		return "SingleLeaderPerTerm"
	case BugWrongTerm:
		return "MonotonicTerms"
	case BugSkipPersist:
		return "CommittedEntriesMatch"
	case BugLogOverwrite:
		return "LogMatching"
	case BugDoubleVote:
		return "SingleLeaderPerTerm"
	case BugCommitWithoutQuorum:
		return "CommitQuorum"
	case BugStaleRead:
		return "Linearizability"
	case BugMissingHeartbeat:
		return "LeaderStability"
	default:
		return "Unknown"
	}
}

// Injector manages bug injection for a Raft node.
type Injector struct {
	flags       atomic.Int32
	probability float64 // Probability of triggering bug (0.0-1.0)
	counter     atomic.Int32
}

// NewInjector creates a new bug injector.
func NewInjector() *Injector {
	return &Injector{
		probability: 0.1, // 10% chance to trigger by default
	}
}

// Enable enables one or more bug flags.
func (i *Injector) Enable(bugs ...BugFlag) {
	for _, bug := range bugs {
		i.flags.Add(int32(bug))
	}
}

// Disable disables one or more bug flags.
func (i *Injector) Disable(bugs ...BugFlag) {
	for _, bug := range bugs {
		current := i.flags.Load()
		newFlags := current &^ int32(bug)
		i.flags.Store(newFlags)
	}
}

// IsEnabled checks if a bug flag is enabled.
func (i *Injector) IsEnabled(bug BugFlag) bool {
	return i.flags.Load()&int32(bug) != 0
}

// SetProbability sets the probability (0.0-1.0) of triggering enabled bugs.
func (i *Injector) SetProbability(p float64) {
	if p < 0.0 {
		p = 0.0
	}
	if p > 1.0 {
		p = 1.0
	}
	i.probability = p
}

// ShouldTrigger returns true if an enabled bug should trigger now.
// This uses a deterministic counter-based approach for reproducibility.
func (i *Injector) ShouldTrigger(bug BugFlag, rng func() float64) bool {
	if !i.IsEnabled(bug) {
		return false
	}
	return rng() < i.probability
}

// GetTriggerCount returns how many times bugs have been triggered.
func (i *Injector) GetTriggerCount() int {
	return int(i.counter.Load())
}

// IncrementTriggerCount increments the trigger counter.
func (i *Injector) IncrementTriggerCount() {
	i.counter.Add(1)
}

// Reset resets the injector state.
func (i *Injector) Reset() {
	i.flags.Store(0)
	i.counter.Store(0)
}

// BugZoo contains all available bugs with metadata.
type BugZoo struct {
	Bugs []BugInfo
}

// BugInfo describes a bug in the bug zoo.
type BugInfo struct {
	Flag        BugFlag
	Name        string
	Description string
	Violation   string
	Severity    Severity
}

// Severity indicates how serious a bug is.
type Severity string

const (
	SeverityCritical Severity = "Critical" // Safety violation
	SeverityHigh     Severity = "High"     // Liveness issue
	SeverityMedium   Severity = "Medium"   // Performance degradation
	SeverityLow      Severity = "Low"      // Minor issue
)

// GetBugZoo returns the collection of all injectable bugs.
func GetBugZoo() *BugZoo {
	return &BugZoo{
		Bugs: []BugInfo{
			{
				Flag:        BugDropCommit,
				Name:        "Drop Commit",
				Description: "Leader drops commit messages to followers",
				Violation:   "CommittedEntriesMatch",
				Severity:    SeverityCritical,
			},
			{
				Flag:        BugIgnoreVote,
				Name:        "Ignore Vote",
				Description: "Candidate ignores some votes during election",
				Violation:   "SingleLeaderPerTerm",
				Severity:    SeverityCritical,
			},
			{
				Flag:        BugWrongTerm,
				Name:        "Wrong Term",
				Description: "Nodes use incorrect term numbers",
				Violation:   "MonotonicTerms",
				Severity:    SeverityCritical,
			},
			{
				Flag:        BugSkipPersist,
				Name:        "Skip Persist",
				Description: "Nodes skip persisting critical state",
				Violation:   "CommittedEntriesMatch",
				Severity:    SeverityCritical,
			},
			{
				Flag:        BugLogOverwrite,
				Name:        "Log Overwrite",
				Description: "Nodes overwrite committed log entries",
				Violation:   "LogMatching",
				Severity:    SeverityCritical,
			},
			{
				Flag:        BugDoubleVote,
				Name:        "Double Vote",
				Description: "Nodes vote multiple times in same term",
				Violation:   "SingleLeaderPerTerm",
				Severity:    SeverityCritical,
			},
			{
				Flag:        BugCommitWithoutQuorum,
				Name:        "Commit Without Quorum",
				Description: "Leader commits without majority",
				Violation:   "CommitQuorum",
				Severity:    SeverityCritical,
			},
			{
				Flag:        BugStaleRead,
				Name:        "Stale Read",
				Description: "Followers serve stale reads",
				Violation:   "Linearizability",
				Severity:    SeverityHigh,
			},
			{
				Flag:        BugMissingHeartbeat,
				Name:        "Missing Heartbeat",
				Description: "Leader skips sending heartbeats",
				Violation:   "LeaderStability",
				Severity:    SeverityMedium,
			},
		},
	}
}

// FindBugByName returns a bug by its name.
func (z *BugZoo) FindBugByName(name string) *BugInfo {
	for i := range z.Bugs {
		if z.Bugs[i].Name == name {
			return &z.Bugs[i]
		}
	}
	return nil
}

// GetCriticalBugs returns all critical severity bugs.
func (z *BugZoo) GetCriticalBugs() []BugInfo {
	var bugs []BugInfo
	for _, bug := range z.Bugs {
		if bug.Severity == SeverityCritical {
			bugs = append(bugs, bug)
		}
	}
	return bugs
}
