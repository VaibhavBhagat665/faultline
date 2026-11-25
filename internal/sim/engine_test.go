// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"testing"
	"time"
)

// counterEvent increments a counter when executed.
type counterEvent struct {
	baseEvent
	counter *int
}

func (e *counterEvent) Execute(ctx *Context) {
	*e.counter++
}

func TestEngine_BasicExecution(t *testing.T) {
	cfg := Config{
		Seed: 12345,
	}

	e := NewEngine(cfg)

	counter := 0

	// Schedule 3 events
	e.queue.Push(&counterEvent{
		baseEvent: baseEvent{time: 100, priority: PriorityTimer, seq: e.NextSeq()},
		counter:   &counter,
	})
	e.queue.Push(&counterEvent{
		baseEvent: baseEvent{time: 200, priority: PriorityTimer, seq: e.NextSeq()},
		counter:   &counter,
	})
	e.queue.Push(&counterEvent{
		baseEvent: baseEvent{time: 300, priority: PriorityTimer, seq: e.NextSeq()},
		counter:   &counter,
	})

	result := e.Run()

	if result.StopReason != StopReasonCompleted {
		t.Errorf("expected StopReasonCompleted, got %v", result.StopReason)
	}

	if result.EventsProcessed != 3 {
		t.Errorf("expected 3 events processed, got %d", result.EventsProcessed)
	}

	if counter != 3 {
		t.Errorf("expected counter=3, got %d", counter)
	}

	if result.FinalTime != 300 {
		t.Errorf("expected final time=300, got %d", result.FinalTime)
	}

	if result.Error != nil {
		t.Errorf("unexpected error: %v", result.Error)
	}
}

func TestEngine_MaxEventsGuard(t *testing.T) {
	cfg := Config{
		Seed:      999,
		MaxEvents: 5,
	}

	e := NewEngine(cfg)

	counter := 0

	// Schedule 10 events, but the guard should stop at 5
	for i := 0; i < 10; i++ {
		e.queue.Push(&counterEvent{
			baseEvent: baseEvent{time: Time(i * 100), priority: PriorityTimer, seq: e.NextSeq()},
			counter:   &counter,
		})
	}

	result := e.Run()

	if result.StopReason != StopReasonMaxEvents {
		t.Errorf("expected StopReasonMaxEvents, got %v", result.StopReason)
	}

	if result.EventsProcessed != 5 {
		t.Errorf("expected 5 events processed, got %d", result.EventsProcessed)
	}

	if counter != 5 {
		t.Errorf("expected counter=5, got %d", counter)
	}

	if result.Error == nil {
		t.Error("expected error for runaway guard")
	}
}

func TestEngine_MaxTimeGuard(t *testing.T) {
	cfg := Config{
		Seed:           777,
		MaxVirtualTime: 500,
	}

	e := NewEngine(cfg)

	counter := 0

	// Schedule events at 100, 200, ..., 1000
	for i := 1; i <= 10; i++ {
		e.queue.Push(&counterEvent{
			baseEvent: baseEvent{time: Time(i * 100), priority: PriorityTimer, seq: e.NextSeq()},
			counter:   &counter,
		})
	}

	result := e.Run()

	if result.StopReason != StopReasonMaxTime {
		t.Errorf("expected StopReasonMaxTime, got %v", result.StopReason)
	}

	// Should have processed events at 100, 200, 300, 400, 500 (5 events)
	if result.EventsProcessed != 5 {
		t.Errorf("expected 5 events processed, got %d", result.EventsProcessed)
	}

	if counter != 5 {
		t.Errorf("expected counter=5, got %d", counter)
	}

	if result.Error == nil {
		t.Error("expected error for runaway guard")
	}
}

func TestEngine_EmptyQueue(t *testing.T) {
	cfg := Config{Seed: 111}
	e := NewEngine(cfg)

	result := e.Run()

	if result.StopReason != StopReasonCompleted {
		t.Errorf("expected StopReasonCompleted, got %v", result.StopReason)
	}

	if result.EventsProcessed != 0 {
		t.Errorf("expected 0 events, got %d", result.EventsProcessed)
	}

	if result.FinalTime != 0 {
		t.Errorf("expected final time=0, got %d", result.FinalTime)
	}
}

func TestEngine_ClockAdvancement(t *testing.T) {
	cfg := Config{Seed: 222}
	e := NewEngine(cfg)

	if e.Clock().Now() != 0 {
		t.Errorf("initial clock should be 0, got %d", e.Clock().Now())
	}

	e.queue.Push(&counterEvent{
		baseEvent: baseEvent{time: 1000, priority: PriorityTimer, seq: e.NextSeq()},
		counter:   new(int),
	})

	e.queue.Push(&counterEvent{
		baseEvent: baseEvent{time: 5000, priority: PriorityTimer, seq: e.NextSeq()},
		counter:   new(int),
	})

	result := e.Run()

	if result.FinalTime != 5000 {
		t.Errorf("expected final time=5000, got %d", result.FinalTime)
	}
}

func TestEngine_TraceHash(t *testing.T) {
	// Two engines with the same seed should produce the same trace hash
	// if they process the same events.

	cfg1 := Config{Seed: 42}
	e1 := NewEngine(cfg1)

	cfg2 := Config{Seed: 42}
	e2 := NewEngine(cfg2)

	// Schedule identical events
	for i := 0; i < 10; i++ {
		e1.queue.Push(&counterEvent{
			baseEvent: baseEvent{time: Time(i * 100), priority: PriorityTimer, seq: uint64(i)},
			counter:   new(int),
		})
		e2.queue.Push(&counterEvent{
			baseEvent: baseEvent{time: Time(i * 100), priority: PriorityTimer, seq: uint64(i)},
			counter:   new(int),
		})
	}

	r1 := e1.Run()
	r2 := e2.Run()

	if r1.TraceHash != r2.TraceHash {
		t.Errorf("same events should produce same trace hash:\n  %s\n  %s", r1.TraceHash, r2.TraceHash)
	}
}

func TestEngine_NextSeq(t *testing.T) {
	cfg := Config{Seed: 1}
	e := NewEngine(cfg)

	seq1 := e.NextSeq()
	seq2 := e.NextSeq()
	seq3 := e.NextSeq()

	if seq1 != 0 || seq2 != 1 || seq3 != 2 {
		t.Errorf("NextSeq should return 0, 1, 2, got %d, %d, %d", seq1, seq2, seq3)
	}
}

// selfSchedulingEvent schedules another event when executed.
type selfSchedulingEvent struct {
	baseEvent
	engine *Engine
	depth  int
	max    int
}

func (e *selfSchedulingEvent) Execute(ctx *Context) {
	if e.depth < e.max {
		e.engine.queue.Push(&selfSchedulingEvent{
			baseEvent: baseEvent{
				time:     e.time + 100,
				priority: PriorityTimer,
				seq:      e.engine.NextSeq(),
			},
			engine: e.engine,
			depth:  e.depth + 1,
			max:    e.max,
		})
	}
}

func TestEngine_SelfSchedulingEvents(t *testing.T) {
	cfg := Config{Seed: 333}
	e := NewEngine(cfg)

	// Start with one event that spawns 9 more (total 10)
	e.queue.Push(&selfSchedulingEvent{
		baseEvent: baseEvent{time: 0, priority: PriorityTimer, seq: e.NextSeq()},
		engine:    e,
		depth:     0,
		max:       9,
	})

	result := e.Run()

	if result.EventsProcessed != 10 {
		t.Errorf("expected 10 events, got %d", result.EventsProcessed)
	}

	expectedTime := Time(9 * 100)
	if result.FinalTime != expectedTime {
		t.Errorf("expected final time=%d, got %d", expectedTime, result.FinalTime)
	}
}

func BenchmarkEngine_SimpleEvents(b *testing.B) {
	cfg := Config{Seed: 12345}
	e := NewEngine(cfg)

	counter := 0

	// Pre-schedule all events
	for i := 0; i < b.N; i++ {
		e.queue.Push(&counterEvent{
			baseEvent: baseEvent{time: Time(i), priority: PriorityTimer, seq: uint64(i)},
			counter:   &counter,
		})
	}

	b.ResetTimer()
	e.Run()
}

func BenchmarkEngine_EventThroughput(b *testing.B) {
	// Measure raw event execution throughput
	const numEvents = 100000

	cfg := Config{
		Seed:      99999,
		MaxEvents: numEvents,
	}

	e := NewEngine(cfg)
	counter := 0

	for i := 0; i < numEvents; i++ {
		e.queue.Push(&counterEvent{
			baseEvent: baseEvent{time: Time(i), priority: PriorityTimer, seq: uint64(i)},
			counter:   &counter,
		})
	}

	b.ResetTimer()
	start := time.Now()

	result := e.Run()

	elapsed := time.Since(start)

	b.ReportMetric(float64(result.EventsProcessed)/elapsed.Seconds(), "events/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(result.EventsProcessed), "ns/event")
}
