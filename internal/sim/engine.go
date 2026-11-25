// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"fmt"

	"github.com/VaibhavBhagat665/faultline/internal/trace"
)

// Config contains simulation engine configuration.
type Config struct {
	// Seed is the master random seed for this run.
	Seed uint64

	// MaxEvents is the maximum number of events to process before stopping.
	// Zero means no limit.
	MaxEvents uint64

	// MaxVirtualTime is the maximum virtual time (nanoseconds) before stopping.
	// Zero means no limit.
	MaxVirtualTime Time

	// TraceWriter is the optional writer for trace output (JSONL).
	// If nil, traces are computed for hashing only.
	TraceWriter interface {
		Write([]byte) (int, error)
	}
}

// Result contains the outcome of a simulation run.
type Result struct {
	// Seed is the master seed that was used.
	Seed uint64

	// EventsProcessed is the number of events that were executed.
	EventsProcessed uint64

	// FinalTime is the virtual time when the simulation stopped.
	FinalTime Time

	// TraceHash is the SHA-256 hash of the trace (hex-encoded).
	TraceHash string

	// StopReason indicates why the simulation stopped.
	StopReason StopReason

	// Error is set if the simulation failed (e.g., runaway).
	Error error
}

// StopReason indicates why the simulation stopped.
type StopReason int

const (
	// StopReasonCompleted indicates the event queue was exhausted.
	StopReasonCompleted StopReason = iota

	// StopReasonMaxEvents indicates the event limit was reached.
	StopReasonMaxEvents

	// StopReasonMaxTime indicates the virtual time limit was reached.
	StopReasonMaxTime
)

func (r StopReason) String() string {
	switch r {
	case StopReasonCompleted:
		return "completed"
	case StopReasonMaxEvents:
		return "max_events"
	case StopReasonMaxTime:
		return "max_time"
	default:
		return "unknown"
	}
}

// Engine is the main simulation engine.
type Engine struct {
	cfg    Config
	clock  *Clock
	queue  *EventQueue
	rng    *RNGManager
	trace  *trace.Trace
	ctx    *Context
	nextSeq uint64
}

// NewEngine creates a new simulation engine with the given configuration.
func NewEngine(cfg Config) *Engine {
	clock := NewClock()
	queue := NewEventQueue()
	rng := NewRNGManager(cfg.Seed)
	tr := trace.New(cfg.TraceWriter)

	e := &Engine{
		cfg:    cfg,
		clock:  clock,
		queue:  queue,
		rng:    rng,
		trace:  tr,
		nextSeq: 0,
	}

	e.ctx = &Context{
		Clock:  clock,
		Trace:  e, // Engine implements TraceWriter interface
		Engine: e,
	}

	return e
}

// Log implements TraceWriter for Context.
func (e *Engine) Log(event string, fields map[string]interface{}) {
	// Ignore errors; trace logging is best-effort
	_ = e.trace.Log(int64(e.clock.Now()), event, fields)
}

// Schedule adds an event to the queue with the given time and priority.
// The event will execute when its time arrives.
func (e *Engine) Schedule(t Time, p Priority, evt Event) {
	// This is a placeholder; in Phase 4 we'll have proper event types
	// For now, the event passed in already has time/priority/seq set.
	e.queue.Push(evt)
}

// Run executes the simulation until the queue is empty or a limit is reached.
func (e *Engine) Run() Result {
	e.Log("sim.start", map[string]interface{}{
		"seed": e.cfg.Seed,
	})

	var eventsProcessed uint64

	for {
		// Check runaway guards
		if e.cfg.MaxEvents > 0 && eventsProcessed >= e.cfg.MaxEvents {
			e.Log("sim.stop", map[string]interface{}{
				"reason": "max_events",
				"events": eventsProcessed,
			})
			return Result{
				Seed:            e.cfg.Seed,
				EventsProcessed: eventsProcessed,
				FinalTime:       e.clock.Now(),
				TraceHash:       e.trace.Hash(),
				StopReason:      StopReasonMaxEvents,
				Error:           fmt.Errorf("runaway: exceeded max events %d", e.cfg.MaxEvents),
			}
		}

		if e.cfg.MaxVirtualTime > 0 && e.clock.Now() >= e.cfg.MaxVirtualTime {
			e.Log("sim.stop", map[string]interface{}{
				"reason": "max_time",
				"time":   e.clock.Now(),
			})
			return Result{
				Seed:            e.cfg.Seed,
				EventsProcessed: eventsProcessed,
				FinalTime:       e.clock.Now(),
				TraceHash:       e.trace.Hash(),
				StopReason:      StopReasonMaxTime,
				Error:           fmt.Errorf("runaway: exceeded max virtual time %s", e.cfg.MaxVirtualTime),
			}
		}

		// Pop next event
		evt := e.queue.Pop()
		if evt == nil {
			// Queue exhausted
			e.Log("sim.stop", map[string]interface{}{
				"reason": "completed",
				"events": eventsProcessed,
			})
			return Result{
				Seed:            e.cfg.Seed,
				EventsProcessed: eventsProcessed,
				FinalTime:       e.clock.Now(),
				TraceHash:       e.trace.Hash(),
				StopReason:      StopReasonCompleted,
			}
		}

		// Advance clock to event time
		e.clock.Advance(evt.Time())

		// Log the event execution
		e.Log("event.execute", map[string]interface{}{
			"time":     evt.Time(),
			"priority": evt.Priority(),
			"seq":      evt.Seq(),
		})

		// Execute the event
		evt.Execute(e.ctx)

		eventsProcessed++
	}
}

// NextSeq returns the next sequence number for event creation.
// This ensures stable ordering when multiple events are scheduled at the same time and priority.
func (e *Engine) NextSeq() uint64 {
	seq := e.nextSeq
	e.nextSeq++
	return seq
}

// RNG returns the RNG manager for this simulation.
func (e *Engine) RNG() *RNGManager {
	return e.rng
}

// Clock returns the simulation clock.
func (e *Engine) Clock() *Clock {
	return e.clock
}

// TraceHash returns the current trace hash.
func (e *Engine) TraceHash() string {
	return e.trace.Hash()
}
