// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package sim

import "container/heap"

// EventQueue is a priority queue of events ordered by (Time, Priority, Seq).
// It maintains the min-heap property: the next event to execute is always at the front.
type EventQueue struct {
	events eventHeap
}

// NewEventQueue creates a new empty event queue.
func NewEventQueue() *EventQueue {
	q := &EventQueue{
		events: make(eventHeap, 0, 128), // preallocate for common case
	}
	heap.Init(&q.events)
	return q
}

// Push adds an event to the queue.
func (q *EventQueue) Push(e Event) {
	heap.Push(&q.events, e)
}

// Pop removes and returns the next event to execute.
// Returns nil if the queue is empty.
func (q *EventQueue) Pop() Event {
	if q.Len() == 0 {
		return nil
	}
	return heap.Pop(&q.events).(Event)
}

// Peek returns the next event without removing it.
// Returns nil if the queue is empty.
func (q *EventQueue) Peek() Event {
	if q.Len() == 0 {
		return nil
	}
	return q.events[0]
}

// Len returns the number of events in the queue.
func (q *EventQueue) Len() int {
	return q.events.Len()
}

// eventHeap implements heap.Interface for Event.
type eventHeap []Event

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool {
	return Less(h[i], h[j])
}

func (h eventHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *eventHeap) Push(x interface{}) {
	*h = append(*h, x.(Event))
}

func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
