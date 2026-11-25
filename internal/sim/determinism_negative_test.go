// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"fmt"
	"testing"
	"time"
)

// negativeTestEvent is a test event for negative controls
type negativeTestEvent struct {
	baseEvent
	logMessage string
}

func (e *negativeTestEvent) Execute(ctx *Context) {
	ctx.Trace.Log(e.logMessage, nil)
}

// TestDeterminism_NegativeControl_MapIteration verifies that the determinism
// check FAILS when an adapter uses map iteration order.
// This is an expected-failure test: we WANT different hashes because
// map iteration order is nondeterministic in Go.
func TestDeterminism_NegativeControl_MapIteration(t *testing.T) {
	const seed = 42
	const runs = 200

	hashes := make(map[string]int)

	for i := 0; i < runs; i++ {
		cfg := Config{
			Seed:           seed, // Same seed every time
			MaxEvents:      100,
			MaxVirtualTime: Time(10 * time.Second),
		}

		e := NewEngine(cfg)

		// Simulate an adapter that uses map iteration
		// This violates determinism because Go randomizes map order
		peers := map[string]int{
			"peer1": 1,
			"peer2": 2,
			"peer3": 3,
		}

		// Schedule events based on map iteration (BAD!)
		// Map iteration order is randomized, so this produces different traces
		for peer, id := range peers {
			evt := &negativeTestEvent{
				baseEvent: baseEvent{
					time:     Time(id * 100),
					priority: 0,
					seq:      e.nextSeq,
				},
				logMessage: fmt.Sprintf("msg.send.%s", peer),
			}
			e.nextSeq++
			e.Schedule(evt.time, evt.priority, evt)
		}

		result := e.Run()
		hashes[result.TraceHash]++
	}

	distinctHashes := len(hashes)

	t.Logf("Ran %d times with seed=%d", runs, seed)
	t.Logf("Distinct hashes: %d", distinctHashes)
	for hash, count := range hashes {
		t.Logf("  %s: %d occurrences", hash, count)
	}

	// NEGATIVE CONTROL: We EXPECT multiple hashes (non-deterministic)
	if distinctHashes == 1 {
		t.Fatalf("CRITICAL: Determinism check PASSED on map-iteration adapter. "+
			"Expected multiple distinct hashes due to Go's randomized map iteration, got only 1. "+
			"This means the determinism check CANNOT detect map iteration bugs!")
	}

	t.Logf("✓ NEGATIVE CONTROL PASSED: Detected %d distinct hashes (expected >1)", distinctHashes)
}

// TestDeterminism_NegativeControl_TimeNow verifies that the determinism
// check FAILS when an adapter uses time.Now().
// This is an expected-failure test: we WANT different hashes.
func TestDeterminism_NegativeControl_TimeNow(t *testing.T) {
	const seed = 42
	const runs = 200

	hashes := make(map[string]int)

	for i := 0; i < runs; i++ {
		cfg := Config{
			Seed:           seed,
			MaxEvents:      100,
			MaxVirtualTime: Time(10 * time.Second),
		}

		e := NewEngine(cfg)

		// Simulate an adapter that calls time.Now() (BAD!)
		wallTime := time.Now().UnixNano()
		evt := &negativeTestEvent{
			baseEvent: baseEvent{
				time:     Time(wallTime % 1000),
				priority: 0,
				seq:      e.nextSeq,
			},
			logMessage: fmt.Sprintf("event.%d", wallTime),
		}
		e.nextSeq++
		e.Schedule(evt.time, evt.priority, evt)

		result := e.Run()
		hashes[result.TraceHash]++
	}

	distinctHashes := len(hashes)

	t.Logf("Ran %d times with seed=%d", runs, seed)
	t.Logf("Distinct hashes: %d", distinctHashes)

	// NEGATIVE CONTROL: We EXPECT multiple hashes (non-deterministic)
	if distinctHashes == 1 {
		t.Fatalf("CRITICAL: Determinism check PASSED on time.Now() adapter. "+
			"Expected multiple distinct hashes due to wall-clock dependency, got only 1. "+
			"This means the determinism check CANNOT detect time.Now() bugs!")
	}

	t.Logf("✓ NEGATIVE CONTROL PASSED: Detected %d distinct hashes (expected >1)", distinctHashes)
}


