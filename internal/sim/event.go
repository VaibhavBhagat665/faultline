// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package sim

// Priority defines the class priority for tie-breaking when multiple events
// have the same virtual time. Lower priority values execute first.
type Priority int

const (
	// PriorityTimer is for timer expiration events.
	PriorityTimer Priority = 10

	// PriorityMessage is for network message delivery.
	PriorityMessage Priority = 20

	// PriorityClientRequest is for client request arrivals.
	PriorityClientRequest Priority = 30

	// PriorityInternal is for internal simulation events (crash, recovery, etc.).
	PriorityInternal Priority = 40
)

// Event is the interface that all simulation events must implement.
// Events are ordered by (Time, Priority, Seq) for stable, deterministic execution.
type Event interface {
	// Time returns the virtual time at which this event should execute.
	Time() Time

	// Priority returns the class priority for tie-breaking.
	Priority() Priority

	// Seq returns the monotonically increasing sequence number for stable ordering
	// when Time and Priority are equal.
	Seq() uint64

	// Execute runs the event. The engine calls this when the event is dequeued.
	Execute(ctx *Context)
}

// Context is the simulation context passed to event execution.
// It provides access to the clock, RNG, trace, and engine state.
type Context struct {
	Clock  *Clock
	Trace  TraceWriter
	Engine *Engine
}

// TraceWriter is the interface for writing trace events.
// Defined here to avoid import cycles; implemented in internal/trace.
type TraceWriter interface {
	// Log writes a structured log entry to the trace.
	Log(event string, fields map[string]interface{})
}

// baseEvent provides common fields for event implementations.
type baseEvent struct {
	time     Time
	priority Priority
	seq      uint64
}

func (e *baseEvent) Time() Time         { return e.time }
func (e *baseEvent) Priority() Priority { return e.priority }
func (e *baseEvent) Seq() uint64        { return e.seq }

// Less returns true if this event should execute before other.
// Ordering: (Time, Priority, Seq) lexicographically.
func Less(a, b Event) bool {
	if a.Time() != b.Time() {
		return a.Time() < b.Time()
	}
	if a.Priority() != b.Priority() {
		return a.Priority() < b.Priority()
	}
	return a.Seq() < b.Seq()
}
