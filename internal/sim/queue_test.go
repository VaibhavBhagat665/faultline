// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"math/rand"
	"testing"
)

// testEvent is a simple event implementation for testing.
type testEvent struct {
	baseEvent
	name string
}

func (e *testEvent) Execute(ctx *Context) {
	// no-op for testing
}

func newTestEvent(time Time, priority Priority, seq uint64, name string) *testEvent {
	return &testEvent{
		baseEvent: baseEvent{time: time, priority: priority, seq: seq},
		name:      name,
	}
}

func TestEventQueue_BasicOperations(t *testing.T) {
	q := NewEventQueue()

	if q.Len() != 0 {
		t.Errorf("new queue should be empty, got len=%d", q.Len())
	}

	if q.Pop() != nil {
		t.Error("pop from empty queue should return nil")
	}

	if q.Peek() != nil {
		t.Error("peek empty queue should return nil")
	}

	e1 := newTestEvent(100, PriorityTimer, 1, "e1")
	q.Push(e1)

	if q.Len() != 1 {
		t.Errorf("after push, len should be 1, got %d", q.Len())
	}

	peeked := q.Peek()
	if peeked != e1 {
		t.Error("peek should return e1")
	}

	if q.Len() != 1 {
		t.Error("peek should not remove event")
	}

	popped := q.Pop()
	if popped != e1 {
		t.Error("pop should return e1")
	}

	if q.Len() != 0 {
		t.Error("after pop, queue should be empty")
	}
}

func TestEventQueue_TimeOrdering(t *testing.T) {
	q := NewEventQueue()

	// Push events in reverse time order
	q.Push(newTestEvent(300, PriorityTimer, 3, "e3"))
	q.Push(newTestEvent(100, PriorityTimer, 1, "e1"))
	q.Push(newTestEvent(200, PriorityTimer, 2, "e2"))

	// Should pop in time order
	e := q.Pop()
	if e.Time() != 100 {
		t.Errorf("expected time 100, got %d", e.Time())
	}

	e = q.Pop()
	if e.Time() != 200 {
		t.Errorf("expected time 200, got %d", e.Time())
	}

	e = q.Pop()
	if e.Time() != 300 {
		t.Errorf("expected time 300, got %d", e.Time())
	}
}

func TestEventQueue_PriorityTieBreaking(t *testing.T) {
	q := NewEventQueue()

	// Same time, different priorities
	q.Push(newTestEvent(100, PriorityInternal, 3, "internal"))
	q.Push(newTestEvent(100, PriorityTimer, 1, "timer"))
	q.Push(newTestEvent(100, PriorityMessage, 2, "message"))

	// Should pop in priority order: Timer < Message < Internal
	e := q.Pop().(*testEvent)
	if e.name != "timer" {
		t.Errorf("expected timer first, got %s", e.name)
	}

	e = q.Pop().(*testEvent)
	if e.name != "message" {
		t.Errorf("expected message second, got %s", e.name)
	}

	e = q.Pop().(*testEvent)
	if e.name != "internal" {
		t.Errorf("expected internal third, got %s", e.name)
	}
}

func TestEventQueue_SequenceTieBreaking(t *testing.T) {
	q := NewEventQueue()

	// Same time, same priority, different sequence numbers
	q.Push(newTestEvent(100, PriorityTimer, 3, "e3"))
	q.Push(newTestEvent(100, PriorityTimer, 1, "e1"))
	q.Push(newTestEvent(100, PriorityTimer, 2, "e2"))

	// Should pop in sequence order
	e := q.Pop().(*testEvent)
	if e.name != "e1" {
		t.Errorf("expected e1 first, got %s", e.name)
	}

	e = q.Pop().(*testEvent)
	if e.name != "e2" {
		t.Errorf("expected e2 second, got %s", e.name)
	}

	e = q.Pop().(*testEvent)
	if e.name != "e3" {
		t.Errorf("expected e3 third, got %s", e.name)
	}
}

func TestEventQueue_StableOrdering(t *testing.T) {
	// Verify that identical events maintain insertion order via sequence numbers
	q := NewEventQueue()

	events := []*testEvent{
		newTestEvent(100, PriorityTimer, 1, "a"),
		newTestEvent(100, PriorityTimer, 2, "b"),
		newTestEvent(100, PriorityTimer, 3, "c"),
		newTestEvent(100, PriorityTimer, 4, "d"),
	}

	for _, e := range events {
		q.Push(e)
	}

	for i, expected := range events {
		got := q.Pop().(*testEvent)
		if got.name != expected.name {
			t.Errorf("position %d: expected %s, got %s", i, expected.name, got.name)
		}
	}
}

func TestEventQueue_RandomizedStress(t *testing.T) {
	// Property test: after pushing N random events and popping them,
	// they should come out in sorted order.
	rng := rand.New(rand.NewSource(42))

	for trial := 0; trial < 100; trial++ {
		q := NewEventQueue()
		n := rng.Intn(100) + 10

		var events []Event
		for i := 0; i < n; i++ {
			e := newTestEvent(
				Time(rng.Int63n(1000)),
				Priority(rng.Intn(4)*10), // 0, 10, 20, 30
				uint64(i),
				"",
			)
			events = append(events, e)
			q.Push(e)
		}

		if q.Len() != n {
			t.Fatalf("trial %d: queue len should be %d, got %d", trial, n, q.Len())
		}

		var prev Event
		for i := 0; i < n; i++ {
			curr := q.Pop()
			if curr == nil {
				t.Fatalf("trial %d: pop %d returned nil", trial, i)
			}

			if prev != nil && !Less(prev, curr) {
				t.Fatalf("trial %d: ordering violation: %v should come before %v",
					trial, prev, curr)
			}
			prev = curr
		}

		if q.Len() != 0 {
			t.Fatalf("trial %d: queue should be empty after popping all, got len=%d", trial, q.Len())
		}
	}
}

func TestEventQueue_HeapInvariant(t *testing.T) {
	// Property test: after each push, the heap property is maintained
	// (every parent is <= its children).
	rng := rand.New(rand.NewSource(123))

	q := NewEventQueue()

	for i := 0; i < 200; i++ {
		e := newTestEvent(
			Time(rng.Int63n(1000)),
			Priority(rng.Intn(4)*10),
			uint64(i),
			"",
		)
		q.Push(e)

		// Check heap property
		for j := 1; j < q.events.Len(); j++ {
			parent := (j - 1) / 2
			if !Less(q.events[parent], q.events[j]) {
				t.Fatalf("after push %d: heap property violated at index %d", i, j)
			}
		}
	}
}
