// SPDX-License-Identifier: Apache-2.0

package bugs

import (
	"testing"
)

func TestBugFlag_String(t *testing.T) {
	tests := []struct {
		flag BugFlag
		want string
	}{
		{BugNone, "None"},
		{BugDropCommit, "DropCommit"},
		{BugIgnoreVote, "IgnoreVote"},
		{BugWrongTerm, "WrongTerm"},
	}

	for _, tt := range tests {
		got := tt.flag.String()
		if got != tt.want {
			t.Errorf("BugFlag(%d).String() = %q, want %q", tt.flag, got, tt.want)
		}
	}
}

func TestInjector_Enable(t *testing.T) {
	inj := NewInjector()

	if inj.IsEnabled(BugDropCommit) {
		t.Error("Bug should not be enabled initially")
	}

	inj.Enable(BugDropCommit)
	if !inj.IsEnabled(BugDropCommit) {
		t.Error("Bug should be enabled after Enable()")
	}

	inj.Disable(BugDropCommit)
	if inj.IsEnabled(BugDropCommit) {
		t.Error("Bug should not be enabled after Disable()")
	}
}

func TestInjector_Multiple(t *testing.T) {
	inj := NewInjector()

	inj.Enable(BugDropCommit, BugIgnoreVote)

	if !inj.IsEnabled(BugDropCommit) {
		t.Error("BugDropCommit should be enabled")
	}
	if !inj.IsEnabled(BugIgnoreVote) {
		t.Error("BugIgnoreVote should be enabled")
	}
	if inj.IsEnabled(BugWrongTerm) {
		t.Error("BugWrongTerm should not be enabled")
	}
}

func TestInjector_ShouldTrigger(t *testing.T) {
	inj := NewInjector()
	inj.Enable(BugDropCommit)
	inj.SetProbability(0.5)

	// With probability 0.5, should trigger about half the time
	triggered := 0
	iterations := 1000

	for i := 0; i < iterations; i++ {
		if inj.ShouldTrigger(BugDropCommit, func() float64 {
			// Deterministic sequence: 0.0, 0.1, 0.2, ... 0.9, 0.0, ...
			return float64(i%10) / 10.0
		}) {
			triggered++
		}
	}

	// Should trigger when RNG < 0.5, so 500 times out of 1000
	if triggered != 500 {
		t.Errorf("Expected ~500 triggers, got %d", triggered)
	}
}

func TestBugZoo(t *testing.T) {
	zoo := GetBugZoo()

	if len(zoo.Bugs) == 0 {
		t.Fatal("Bug zoo should not be empty")
	}

	// Check that all bugs are documented
	expectedBugs := []BugFlag{
		BugDropCommit,
		BugIgnoreVote,
		BugWrongTerm,
		BugSkipPersist,
		BugLogOverwrite,
		BugDoubleVote,
		BugCommitWithoutQuorum,
		BugStaleRead,
		BugMissingHeartbeat,
	}

	if len(zoo.Bugs) != len(expectedBugs) {
		t.Errorf("Expected %d bugs, got %d", len(expectedBugs), len(zoo.Bugs))
	}

	// Check that each bug has required fields
	for _, bug := range zoo.Bugs {
		if bug.Name == "" {
			t.Errorf("Bug %v has empty name", bug.Flag)
		}
		if bug.Description == "" {
			t.Errorf("Bug %s has empty description", bug.Name)
		}
		if bug.Violation == "" {
			t.Errorf("Bug %s has empty violation", bug.Name)
		}
		if bug.Severity == "" {
			t.Errorf("Bug %s has empty severity", bug.Name)
		}
	}
}

func TestBugZoo_FindByName(t *testing.T) {
	zoo := GetBugZoo()

	bug := zoo.FindBugByName("Drop Commit")
	if bug == nil {
		t.Fatal("Should find Drop Commit bug")
	}
	if bug.Flag != BugDropCommit {
		t.Errorf("Expected BugDropCommit, got %v", bug.Flag)
	}

	notFound := zoo.FindBugByName("NonexistentBug")
	if notFound != nil {
		t.Error("Should not find nonexistent bug")
	}
}

func TestBugZoo_GetCriticalBugs(t *testing.T) {
	zoo := GetBugZoo()
	critical := zoo.GetCriticalBugs()

	if len(critical) == 0 {
		t.Fatal("Should have at least one critical bug")
	}

	for _, bug := range critical {
		if bug.Severity != SeverityCritical {
			t.Errorf("Bug %s has severity %s, expected Critical", bug.Name, bug.Severity)
		}
	}
}

func TestInjector_TriggerCount(t *testing.T) {
	inj := NewInjector()

	if inj.GetTriggerCount() != 0 {
		t.Error("Initial trigger count should be 0")
	}

	inj.IncrementTriggerCount()
	if inj.GetTriggerCount() != 1 {
		t.Error("Trigger count should be 1 after increment")
	}

	inj.IncrementTriggerCount()
	inj.IncrementTriggerCount()
	if inj.GetTriggerCount() != 3 {
		t.Error("Trigger count should be 3")
	}

	inj.Reset()
	if inj.GetTriggerCount() != 0 {
		t.Error("Trigger count should be 0 after reset")
	}
}

func TestInjector_Probability(t *testing.T) {
	inj := NewInjector()

	// Test probability bounds
	inj.SetProbability(-0.5)
	if inj.probability != 0.0 {
		t.Error("Negative probability should be clamped to 0.0")
	}

	inj.SetProbability(1.5)
	if inj.probability != 1.0 {
		t.Error("Probability > 1.0 should be clamped to 1.0")
	}

	inj.SetProbability(0.75)
	if inj.probability != 0.75 {
		t.Error("Valid probability should be set correctly")
	}
}
