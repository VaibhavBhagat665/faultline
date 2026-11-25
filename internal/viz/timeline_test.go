// SPDX-License-Identifier: Apache-2.0

package viz

import (
	"bytes"
	"strings"
	"testing"
)

func TestTimeline_AddEvent(t *testing.T) {
	timeline := NewTimeline([]string{"n1", "n2", "n3"})

	event := Event{
		Time:        1000,
		Type:        "election",
		NodeID:      "n1",
		Term:        1,
		Description: "Election started",
		Success:     true,
	}

	timeline.AddEvent(event)

	if len(timeline.Events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(timeline.Events))
	}

	if timeline.StartTime != 1000 {
		t.Errorf("Expected StartTime=1000, got %d", timeline.StartTime)
	}

	if timeline.EndTime != 1000 {
		t.Errorf("Expected EndTime=1000, got %d", timeline.EndTime)
	}
}

func TestTimeline_Sort(t *testing.T) {
	timeline := NewTimeline([]string{"n1", "n2"})

	// Add events out of order
	timeline.AddEvent(Event{Time: 3000, Type: "commit", NodeID: "n1"})
	timeline.AddEvent(Event{Time: 1000, Type: "election", NodeID: "n1"})
	timeline.AddEvent(Event{Time: 2000, Type: "vote", NodeID: "n2"})

	timeline.Sort()

	// Verify sorted order
	if timeline.Events[0].Time != 1000 {
		t.Error("Events not sorted correctly")
	}
	if timeline.Events[1].Time != 2000 {
		t.Error("Events not sorted correctly")
	}
	if timeline.Events[2].Time != 3000 {
		t.Error("Events not sorted correctly")
	}
}

func TestTimeline_GenerateHTML(t *testing.T) {
	timeline := NewTimeline([]string{"n1", "n2", "n3"})

	// Add some events
	timeline.AddEvent(Event{
		Time:        1000,
		Type:        "election",
		NodeID:      "n1",
		Term:        1,
		Description: "Started election",
		Success:     true,
	})

	timeline.AddEvent(Event{
		Time:        2000,
		Type:        "vote",
		NodeID:      "n2",
		Target:      "n1",
		Term:        1,
		Description: "Voted for n1",
		Success:     true,
	})

	timeline.AddEvent(Event{
		Time:        3000,
		Type:        "commit",
		NodeID:      "n1",
		Term:        1,
		Description: "Committed entry",
		Success:     true,
	})

	// Add a state
	timeline.AddState(NodeState{
		Time:        2000,
		NodeID:      "n1",
		Role:        "leader",
		Term:        1,
		CommitIndex: 1,
		LogLength:   2,
	})

	var buf bytes.Buffer
	err := timeline.GenerateHTML(&buf, "Test Timeline")
	if err != nil {
		t.Fatalf("Failed to generate HTML: %v", err)
	}

	html := buf.String()

	// Verify HTML contains key elements
	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("Missing DOCTYPE")
	}
	if !strings.Contains(html, "Test Timeline") {
		t.Error("Missing title")
	}
	if !strings.Contains(html, "n1") {
		t.Error("Missing node n1")
	}
	if !strings.Contains(html, "election") {
		t.Error("Missing election event")
	}
	if !strings.Contains(html, "Started election") {
		t.Error("Missing event description")
	}
}

func TestTimeline_WithViolations(t *testing.T) {
	timeline := NewTimeline([]string{"n1", "n2"})

	timeline.AddEvent(Event{
		Time:   1000,
		Type:   "election",
		NodeID: "n1",
		Term:   1,
	})

	timeline.AddViolation(Violation{
		Time:        1500,
		Name:        "ElectionSafety",
		Description: "Multiple leaders in term 1",
		Severity:    "Critical",
	})

	var buf bytes.Buffer
	err := timeline.GenerateHTML(&buf, "Test With Violations")
	if err != nil {
		t.Fatalf("Failed to generate HTML: %v", err)
	}

	html := buf.String()

	// Verify violation is shown
	if !strings.Contains(html, "ElectionSafety") {
		t.Error("Missing violation name")
	}
	if !strings.Contains(html, "Multiple leaders") {
		t.Error("Missing violation description")
	}
	if !strings.Contains(html, "⚠") {
		t.Error("Missing warning symbol")
	}
}

func TestTimeline_EmptyTimeline(t *testing.T) {
	timeline := NewTimeline([]string{"n1"})

	var buf bytes.Buffer
	err := timeline.GenerateHTML(&buf, "Empty Timeline")
	if err != nil {
		t.Fatalf("Failed to generate HTML for empty timeline: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "Empty Timeline") {
		t.Error("Missing title in empty timeline")
	}
}

func TestTimeline_LargeTimeline(t *testing.T) {
	timeline := NewTimeline([]string{"n1", "n2", "n3", "n4", "n5"})

	// Add many events
	for i := 0; i < 1000; i++ {
		timeline.AddEvent(Event{
			Time:   int64(i * 100),
			Type:   "heartbeat",
			NodeID: "n1",
			Term:   1,
		})
	}

	var buf bytes.Buffer
	err := timeline.GenerateHTML(&buf, "Large Timeline")
	if err != nil {
		t.Fatalf("Failed to generate HTML for large timeline: %v", err)
	}

	// Just verify it completes without error
	if buf.Len() == 0 {
		t.Error("Generated empty HTML")
	}
}

func BenchmarkTimeline_GenerateHTML(b *testing.B) {
	timeline := NewTimeline([]string{"n1", "n2", "n3", "n4", "n5"})

	// Add realistic number of events
	for i := 0; i < 500; i++ {
		timeline.AddEvent(Event{
			Time:        int64(i * 100),
			Type:        "heartbeat",
			NodeID:      "n1",
			Term:        1,
			Description: "Heartbeat",
			Success:     true,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		timeline.GenerateHTML(&buf, "Benchmark")
	}
}
