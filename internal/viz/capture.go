// SPDX-License-Identifier: Apache-2.0

package viz

import "fmt"

// TimelineCapture helps capture events during cluster execution for visualization.
type TimelineCapture struct {
	timeline *Timeline
	enabled  bool
}

// NewTimelineCapture creates a new timeline capture.
func NewTimelineCapture(nodes []string) *TimelineCapture {
	return &TimelineCapture{
		timeline: NewTimeline(nodes),
		enabled:  true,
	}
}

// Enable enables event capture.
func (tc *TimelineCapture) Enable() {
	tc.enabled = true
}

// Disable disables event capture.
func (tc *TimelineCapture) Disable() {
	tc.enabled = false
}

// RecordElection records an election event.
func (tc *TimelineCapture) RecordElection(time int64, nodeID string, term uint64) {
	if !tc.enabled {
		return
	}
	tc.timeline.AddEvent(Event{
		Time:        time,
		Type:        "election",
		NodeID:      nodeID,
		Term:        term,
		Description: "Started election",
		Success:     true,
	})
}

// RecordVote records a vote event.
func (tc *TimelineCapture) RecordVote(time int64, voter, candidate string, term uint64, granted bool) {
	if !tc.enabled {
		return
	}
	desc := "Voted for " + candidate
	if !granted {
		desc = "Rejected vote for " + candidate
	}
	tc.timeline.AddEvent(Event{
		Time:        time,
		Type:        "vote",
		NodeID:      voter,
		Target:      candidate,
		Term:        term,
		Description: desc,
		Success:     granted,
	})
}

// RecordAppendEntries records an AppendEntries RPC.
func (tc *TimelineCapture) RecordAppendEntries(time int64, leader, follower string, term uint64, 
	entryCount int, success bool) {
	if !tc.enabled {
		return
	}
	desc := ""
	if entryCount > 0 {
		desc = fmt.Sprintf("Replicate %d entries", entryCount)
	} else {
		desc = "Heartbeat"
	}
	tc.timeline.AddEvent(Event{
		Time:        time,
		Type:        "append",
		NodeID:      leader,
		Target:      follower,
		Term:        term,
		Description: desc,
		Success:     success,
		Data: map[string]interface{}{
			"entryCount": entryCount,
		},
	})
}

// RecordCommit records a commit event.
func (tc *TimelineCapture) RecordCommit(time int64, nodeID string, term uint64, index uint64) {
	if !tc.enabled {
		return
	}
	tc.timeline.AddEvent(Event{
		Time:        time,
		Type:        "commit",
		NodeID:      nodeID,
		Term:        term,
		Description: fmt.Sprintf("Committed up to index %d", index),
		Success:     true,
		Data: map[string]interface{}{
			"commitIndex": index,
		},
	})
}

// RecordHeartbeat records a heartbeat.
func (tc *TimelineCapture) RecordHeartbeat(time int64, leader, follower string, term uint64) {
	if !tc.enabled {
		return
	}
	tc.timeline.AddEvent(Event{
		Time:        time,
		Type:        "heartbeat",
		NodeID:      leader,
		Target:      follower,
		Term:        term,
		Description: "Heartbeat",
		Success:     true,
	})
}

// RecordStateChange records a role change (follower → candidate → leader).
func (tc *TimelineCapture) RecordStateChange(time int64, nodeID, oldRole, newRole string, term uint64) {
	if !tc.enabled {
		return
	}
	tc.timeline.AddState(NodeState{
		Time:   time,
		NodeID: nodeID,
		Role:   newRole,
		Term:   term,
	})
}

// RecordViolation records an invariant violation.
func (tc *TimelineCapture) RecordViolation(time int64, name, description, severity string) {
	if !tc.enabled {
		return
	}
	tc.timeline.AddViolation(Violation{
		Time:        time,
		Name:        name,
		Description: description,
		Severity:    severity,
	})
}

// GetTimeline returns the captured timeline.
func (tc *TimelineCapture) GetTimeline() *Timeline {
	return tc.timeline
}
