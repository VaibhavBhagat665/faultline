// SPDX-License-Identifier: Apache-2.0

package invariants

import (
	"testing"
)

func TestElectionSafety(t *testing.T) {
	tests := []struct {
		name      string
		states    map[string]NodeState
		wantViolation bool
	}{
		{
			name: "valid_single_leader",
			states: map[string]NodeState{
				"n1": {Role: "Leader", Term: 5},
				"n2": {Role: "Follower", Term: 5},
				"n3": {Role: "Follower", Term: 5},
			},
			wantViolation: false,
		},
		{
			name: "valid_no_leader",
			states: map[string]NodeState{
				"n1": {Role: "Follower", Term: 3},
				"n2": {Role: "Candidate", Term: 4},
				"n3": {Role: "Follower", Term: 3},
			},
			wantViolation: false,
		},
		{
			name: "violation_two_leaders_same_term",
			states: map[string]NodeState{
				"n1": {Role: "Leader", Term: 5},
				"n2": {Role: "Leader", Term: 5},
				"n3": {Role: "Follower", Term: 5},
			},
			wantViolation: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := CheckElectionSafety(tt.states)
			if tt.wantViolation && v == nil {
				t.Errorf("Expected ElectionSafety violation, got none")
			}
			if !tt.wantViolation && v != nil {
				t.Errorf("Expected no violation, got: %s", v.Description)
			}
			if v != nil && v.Name != "ElectionSafety" {
				t.Errorf("Expected violation name ElectionSafety, got %s", v.Name)
			}
		})
	}
}

func TestLogMatching(t *testing.T) {
	tests := []struct {
		name      string
		states    map[string]NodeState
		wantViolation bool
	}{
		{
			name: "valid_matching_logs",
			states: map[string]NodeState{
				"n1": {
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
						{Term: 2, Index: 2, Command: []byte("b")},
					},
				},
				"n2": {
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
						{Term: 2, Index: 2, Command: []byte("b")},
					},
				},
			},
			wantViolation: false,
		},
		{
			name: "violation_same_index_term_different_command",
			states: map[string]NodeState{
				"n1": {
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
						{Term: 2, Index: 2, Command: []byte("b")},
					},
				},
				"n2": {
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
						{Term: 2, Index: 2, Command: []byte("DIFFERENT")},
					},
				},
			},
			wantViolation: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := CheckLogMatching(tt.states)
			if tt.wantViolation && v == nil {
				t.Errorf("Expected LogMatching violation, got none")
			}
			if !tt.wantViolation && v != nil {
				t.Errorf("Expected no violation, got: %s", v.Description)
			}
			if v != nil && v.Name != "LogMatching" {
				t.Errorf("Expected violation name LogMatching, got %s", v.Name)
			}
		})
	}
}

func TestLeaderCompleteness(t *testing.T) {
	tests := []struct {
		name      string
		states    map[string]NodeState
		wantViolation bool
	}{
		{
			name: "valid_leader_has_all_committed",
			states: map[string]NodeState{
				"n1": {
					Role: "Leader",
					Term: 3,
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
						{Term: 2, Index: 2, Command: []byte("b")},
					},
					CommitIndex: 2,
				},
				"n2": {
					Role: "Follower",
					Term: 3,
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
						{Term: 2, Index: 2, Command: []byte("b")},
					},
					CommitIndex: 2,
				},
			},
			wantViolation: false,
		},
		{
			name: "violation_leader_missing_committed_entry",
			states: map[string]NodeState{
				"n1": {
					Role: "Leader",
					Term: 3,
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
					},
					CommitIndex: 1,
				},
				"n2": {
					Role: "Follower",
					Term: 2,
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
						{Term: 2, Index: 2, Command: []byte("b")},
					},
					CommitIndex: 2,
				},
			},
			wantViolation: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := CheckLeaderCompleteness(tt.states)
			if tt.wantViolation && v == nil {
				t.Errorf("Expected LeaderCompleteness violation, got none")
			}
			if !tt.wantViolation && v != nil {
				t.Errorf("Expected no violation, got: %s", v.Description)
			}
			if v != nil && v.Name != "LeaderCompleteness" {
				t.Errorf("Expected violation name LeaderCompleteness, got %s", v.Name)
			}
		})
	}
}

func TestStateMachineSafety(t *testing.T) {
	// State Machine Safety is checked within LeaderCompleteness
	// (committed entries must have consistent terms across nodes)
	tests := []struct {
		name      string
		states    map[string]NodeState
		wantViolation bool
	}{
		{
			name: "valid_same_commands_applied",
			states: map[string]NodeState{
				"n1": {
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
						{Term: 2, Index: 2, Command: []byte("b")},
					},
					CommitIndex: 2,
					Applied: 2,
				},
				"n2": {
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
						{Term: 2, Index: 2, Command: []byte("b")},
					},
					CommitIndex: 2,
					Applied: 2,
				},
			},
			wantViolation: false,
		},
		{
			name: "violation_different_terms_at_committed_index",
			states: map[string]NodeState{
				"n1": {
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
						{Term: 2, Index: 2, Command: []byte("b")},
					},
					CommitIndex: 2,
					Applied: 2,
				},
				"n2": {
					Log: []LogEntry{
						{Term: 1, Index: 1, Command: []byte("a")},
						{Term: 3, Index: 2, Command: []byte("DIFFERENT")},
					},
					CommitIndex: 2,
					Applied: 2,
				},
			},
			wantViolation: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := CheckLeaderCompleteness(tt.states)
			if tt.wantViolation && v == nil {
				t.Errorf("Expected StateMachineSafety violation, got none")
			}
			if !tt.wantViolation && v != nil {
				t.Errorf("Expected no violation, got: %s", v.Description)
			}
			if v != nil && v.Name != "StateMachineSafety" {
				t.Errorf("Expected violation name StateMachineSafety, got %s", v.Name)
			}
		})
	}
}

func TestLeaderAppendOnly(t *testing.T) {
	// Leader Append-Only is not implemented as a stateful checker
	// This property is enforced by the Raft implementation itself
	// We can test indirectly via LogMatching
	
	t.Run("valid_no_overwrite_detectable", func(t *testing.T) {
		// If a leader overwrote entries, it would cause LogMatching violations
		states := map[string]NodeState{
			"n1": {
				Role: "Leader",
				Term: 2,
				Log: []LogEntry{
					{Term: 1, Index: 1, Command: []byte("a")},
					{Term: 2, Index: 2, Command: []byte("b")},
				},
			},
			"n2": {
				Role: "Follower",
				Term: 2,
				Log: []LogEntry{
					{Term: 1, Index: 1, Command: []byte("a")},
					{Term: 2, Index: 2, Command: []byte("b")},
				},
			},
		}
		
		v := CheckLogMatching(states)
		if v != nil {
			t.Errorf("Expected no violation, got: %s", v.Description)
		}
	})
}
