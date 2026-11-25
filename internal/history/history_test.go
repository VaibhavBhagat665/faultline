// SPDX-License-Identifier: Apache-2.0

package history

import (
	"encoding/json"
	"testing"
)

func TestHistory_Recording(t *testing.T) {
	h := NewHistory()
	
	// Record invoke
	h.Append(Event{
		Type:      EventInvoke,
		ClientID:  1,
		RequestID: 100,
		Time:      0,
		Op:        Operation{Type: OpWrite, Key: "x", Input: 42},
	})
	
	// Record ok
	h.Append(Event{
		Type:      EventOk,
		ClientID:  1,
		RequestID: 100,
		Time:      10,
		Op:        Operation{Type: OpWrite, Key: "x", Input: 42},
	})
	
	// Record fail
	h.Append(Event{
		Type:      EventFail,
		ClientID:  2,
		RequestID: 101,
		Time:      20,
		Op:        Operation{Type: OpRead, Key: "y"},
	})
	
	// Record info
	h.Append(Event{
		Type:      EventInfo,
		ClientID:  0,
		RequestID: 0,
		Time:      30,
		Op:        Operation{Type: OpRead, Key: "system"},
	})
	
	if len(h.Events) != 4 {
		t.Errorf("Expected 4 events, got %d", len(h.Events))
	}
	
	if h.Events[0].Type != EventInvoke {
		t.Errorf("Event 0: expected Invoke, got %s", h.Events[0].Type)
	}
	if h.Events[1].Type != EventOk {
		t.Errorf("Event 1: expected Ok, got %s", h.Events[1].Type)
	}
	if h.Events[2].Type != EventFail {
		t.Errorf("Event 2: expected Fail, got %s", h.Events[2].Type)
	}
	if h.Events[3].Type != EventInfo {
		t.Errorf("Event 3: expected Info, got %s", h.Events[3].Type)
	}
}

func TestHistory_CompletedOps(t *testing.T) {
	h := NewHistory()
	
	// Completed operation
	h.Append(Event{
		Type:      EventInvoke,
		ClientID:  1,
		RequestID: 1,
		Time:      0,
		Op:        Operation{Type: OpWrite, Key: "a", Input: 1},
	})
	h.Append(Event{
		Type:      EventOk,
		ClientID:  1,
		RequestID: 1,
		Time:      10,
		Op:        Operation{Type: OpWrite, Key: "a", Input: 1},
	})
	
	// Pending operation (no response)
	h.Append(Event{
		Type:      EventInvoke,
		ClientID:  2,
		RequestID: 2,
		Time:      20,
		Op:        Operation{Type: OpRead, Key: "b"},
	})
	
	ops := h.CompletedOps()
	if len(ops) != 2 {
		t.Errorf("Expected 2 ops (1 complete, 1 pending), got %d", len(ops))
	}
	
	// Find the completed one
	var completed *CompletedOp
	for i := range ops {
		if ops[i].Status == StatusComplete {
			completed = &ops[i]
			break
		}
	}
	
	if completed == nil {
		t.Fatal("Expected at least one completed op")
	}
	
	if completed.Invoke.ClientID != 1 {
		t.Errorf("Expected ClientID 1, got %d", completed.Invoke.ClientID)
	}
}

func TestHistory_Serialization(t *testing.T) {
	h := NewHistory()
	
	h.Append(Event{
		Type:      EventInvoke,
		ClientID:  1,
		RequestID: 1,
		Time:      0,
		Op:        Operation{Type: OpWrite, Key: "x", Input: 42},
	})
	h.Append(Event{
		Type:      EventOk,
		ClientID:  1,
		RequestID: 1,
		Time:      10,
		Op:        Operation{Type: OpWrite, Key: "x", Input: 42},
	})
	
	// Serialize
	data, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	
	// Deserialize
	h2 := NewHistory()
	if err := json.Unmarshal(data, h2); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	
	// Verify
	if len(h.Events) != len(h2.Events) {
		t.Fatalf("Event count mismatch: %d vs %d", len(h.Events), len(h2.Events))
	}
	
	for i := range h.Events {
		if h.Events[i].Type != h2.Events[i].Type {
			t.Errorf("Event %d: type mismatch: %s vs %s", i, h.Events[i].Type, h2.Events[i].Type)
		}
		if h.Events[i].ClientID != h2.Events[i].ClientID {
			t.Errorf("Event %d: ClientID mismatch: %d vs %d", i, h.Events[i].ClientID, h2.Events[i].ClientID)
		}
		if h.Events[i].Op.Key != h2.Events[i].Op.Key {
			t.Errorf("Event %d: Key mismatch: %s vs %s", i, h.Events[i].Op.Key, h2.Events[i].Op.Key)
		}
	}
}

func TestHistory_PartitionByKey(t *testing.T) {
	h := NewHistory()
	
	// Key "a"
	h.Append(Event{
		Type:      EventInvoke,
		ClientID:  1,
		RequestID: 1,
		Time:      0,
		Op:        Operation{Type: OpWrite, Key: "a", Input: 1},
	})
	h.Append(Event{
		Type:      EventOk,
		ClientID:  1,
		RequestID: 1,
		Time:      10,
		Op:        Operation{Type: OpWrite, Key: "a", Input: 1},
	})
	
	// Key "b"
	h.Append(Event{
		Type:      EventInvoke,
		ClientID:  2,
		RequestID: 2,
		Time:      20,
		Op:        Operation{Type: OpWrite, Key: "b", Input: 2},
	})
	h.Append(Event{
		Type:      EventOk,
		ClientID:  2,
		RequestID: 2,
		Time:      30,
		Op:        Operation{Type: OpWrite, Key: "b", Input: 2},
	})
	
	partitions := h.PartitionByKey()
	
	if len(partitions) != 2 {
		t.Errorf("Expected 2 partitions, got %d", len(partitions))
	}
	
	if _, ok := partitions["a"]; !ok {
		t.Error("Missing partition for key 'a'")
	}
	if _, ok := partitions["b"]; !ok {
		t.Error("Missing partition for key 'b'")
	}
	
	if len(partitions["a"].Events) != 2 {
		t.Errorf("Partition 'a': expected 2 events, got %d", len(partitions["a"].Events))
	}
	if len(partitions["b"].Events) != 2 {
		t.Errorf("Partition 'b': expected 2 events, got %d", len(partitions["b"].Events))
	}
}
