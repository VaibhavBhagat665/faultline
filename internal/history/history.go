// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package history

import "fmt"

// OpType represents the type of operation.
type OpType int

const (
	OpRead OpType = iota
	OpWrite
	OpCAS // Compare-and-swap
)

func (t OpType) String() string {
	switch t {
	case OpRead:
		return "read"
	case OpWrite:
		return "write"
	case OpCAS:
		return "cas"
	default:
		return "unknown"
	}
}

// Operation represents a client operation on the distributed system.
type Operation struct {
	// Type is the operation type (read, write, cas).
	Type OpType

	// Key is the object being operated on (for KV systems).
	// Empty for single-register systems.
	Key string

	// Input is the operation input (e.g., value to write, CAS old/new).
	Input interface{}

	// Output is the operation result (e.g., read value, CAS success/failure).
	// Only set after the operation completes.
	Output interface{}
}

// String returns a human-readable representation.
func (op Operation) String() string {
	switch op.Type {
	case OpRead:
		if op.Output != nil {
			return fmt.Sprintf("read(%s) → %v", op.Key, op.Output)
		}
		return fmt.Sprintf("read(%s)", op.Key)
	case OpWrite:
		return fmt.Sprintf("write(%s, %v)", op.Key, op.Input)
	case OpCAS:
		if op.Output != nil {
			return fmt.Sprintf("cas(%s, %v) → %v", op.Key, op.Input, op.Output)
		}
		return fmt.Sprintf("cas(%s, %v)", op.Key, op.Input)
	default:
		return fmt.Sprintf("op(%v)", op.Type)
	}
}

// EventType represents the type of history event.
type EventType int

const (
	// EventInvoke marks the start of an operation.
	EventInvoke EventType = iota

	// EventOk marks successful completion of an operation.
	EventOk

	// EventFail marks failed completion (e.g., "not leader", "timeout").
	EventFail

	// EventInfo marks indeterminate completion (e.g., timeout where the
	// operation may or may not have taken effect).
	EventInfo
)

func (t EventType) String() string {
	switch t {
	case EventInvoke:
		return "invoke"
	case EventOk:
		return "ok"
	case EventFail:
		return "fail"
	case EventInfo:
		return "info"
	default:
		return "unknown"
	}
}

// Event represents a single event in the history.
type Event struct {
	// Type is the event type (invoke, ok, fail, info).
	Type EventType

	// ClientID identifies the client that issued the operation.
	ClientID int

	// RequestID uniquely identifies the request within the client.
	// Pair (ClientID, RequestID) is globally unique.
	RequestID int

	// Time is the virtual time when this event occurred (nanoseconds).
	Time int64

	// Op is the operation being performed.
	Op Operation
}

// String returns a human-readable representation.
func (e Event) String() string {
	return fmt.Sprintf("[t=%d c=%d r=%d] %s: %s", e.Time, e.ClientID, e.RequestID, e.Type, e.Op)
}

// History is a sequence of events representing client operations.
type History struct {
	// Events is the ordered list of events.
	Events []Event
}

// NewHistory creates an empty history.
func NewHistory() *History {
	return &History{
		Events: make([]Event, 0),
	}
}

// Append adds an event to the history.
func (h *History) Append(e Event) {
	h.Events = append(h.Events, e)
}

// Len returns the number of events.
func (h *History) Len() int {
	return len(h.Events)
}

// PartitionByKey partitions the history by key, returning one history per key.
// This enables P-compositionality: checking each key independently.
// Returns a map of key → history for that key.
func (h *History) PartitionByKey() map[string]*History {
	partitions := make(map[string]*History)

	for _, event := range h.Events {
		key := event.Op.Key
		if key == "" {
			key = "_default" // single-register histories
		}

		if partitions[key] == nil {
			partitions[key] = NewHistory()
		}

		partitions[key].Append(event)
	}

	return partitions
}

// Operations returns a list of complete operations (invoke + response pairs).
// An operation is complete if it has both an invoke and a response (ok/fail).
// Info events are treated as indeterminate and kept separate.
type OpStatus int

const (
	StatusComplete OpStatus = iota // Has invoke and ok/fail
	StatusPending                  // Has invoke, no response yet
	StatusInfo                     // Has invoke and info (indeterminate)
)

// CompletedOp represents a completed operation with its invocation and response.
type CompletedOp struct {
	Invoke   Event
	Response Event
	Status   OpStatus
}

// CompletedOps extracts completed operations from the history.
// Returns a list of (invoke, response) pairs.
func (h *History) CompletedOps() []CompletedOp {
	// Map (clientID, requestID) -> invoke event
	invokes := make(map[string]Event)

	var ops []CompletedOp

	for _, event := range h.Events {
		key := fmt.Sprintf("%d-%d", event.ClientID, event.RequestID)

		switch event.Type {
		case EventInvoke:
			invokes[key] = event

		case EventOk, EventFail:
			if invoke, ok := invokes[key]; ok {
				ops = append(ops, CompletedOp{
					Invoke:   invoke,
					Response: event,
					Status:   StatusComplete,
				})
				delete(invokes, key)
			}

		case EventInfo:
			if invoke, ok := invokes[key]; ok {
				ops = append(ops, CompletedOp{
					Invoke:   invoke,
					Response: event,
					Status:   StatusInfo,
				})
				delete(invokes, key)
			}
		}
	}

	// Remaining invokes are pending
	for _, invoke := range invokes {
		ops = append(ops, CompletedOp{
			Invoke: invoke,
			Status: StatusPending,
		})
	}

	return ops
}
