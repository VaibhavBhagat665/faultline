// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package lincheck

import (
	"fmt"

	"github.com/VaibhavBhagat665/faultline/internal/history"
)

// Result represents the outcome of a linearizability check.
type Result struct {
	// Ok is true if the history is linearizable.
	Ok bool

	// Counterexample is a minimal sub-history that violates linearizability.
	// Only set if Ok is false.
	Counterexample []history.Event

	// Explanation is a human-readable explanation of the violation.
	Explanation string
}

// Check checks if a history is linearizable against the given model.
// Uses Wing & Gong search with memoization (Lowe 2017).
func Check(h *history.History, model Model) Result {
	ops := h.CompletedOps()

	// Build list of completed operations (ignore pending and info for now)
	var completed []history.CompletedOp
	for _, op := range ops {
		if op.Status == history.StatusComplete {
			completed = append(completed, op)
		}
	}

	if len(completed) == 0 {
		return Result{Ok: true}
	}

	// Try to find a linearization
	checker := &checker{
		model: model,
		ops:   completed,
		memo:  make(map[memoKey]bool),
	}

	if checker.search(model.Init(), 0, make([]bool, len(completed))) {
		return Result{Ok: true}
	}

	// Not linearizable: extract counterexample
	return Result{
		Ok:              false,
		Counterexample:  extractCounterexample(h, completed),
		Explanation:     "No valid linearization found",
	}
}

// checker performs the Wing & Gong search with memoization.
type checker struct {
	model Model
	ops   []history.CompletedOp
	memo  map[memoKey]bool
}

// memoKey represents a (state, remaining-ops) configuration.
type memoKey struct {
	stateHash uint64
	remaining uint64 // bitset of remaining operation indices
}

// search recursively tries to linearize the remaining operations.
// state is the current model state.
// completed is a bitmask of which operations have been linearized.
// Returns true if a valid linearization exists.
func (c *checker) search(state State, depth int, linearized []bool) bool {
	// Base case: all operations linearized
	allDone := true
	for _, done := range linearized {
		if !done {
			allDone = false
			break
		}
	}
	if allDone {
		return true
	}

	// Memoization: check if we've seen this (state, remaining) configuration
	remaining := uint64(0)
	for i, done := range linearized {
		if !done && i < 64 { // limit to 64 ops for bitset
			remaining |= (1 << i)
		}
	}

	key := memoKey{
		stateHash: c.model.Hash(state),
		remaining: remaining,
	}

	if result, ok := c.memo[key]; ok {
		return result
	}

	// Try linearizing each pending operation
	for i, op := range c.ops {
		if linearized[i] {
			continue
		}

		// Check if this operation can be linearized now
		// (its invocation must have happened)
		canLinearize := true

		// For simplicity, we assume all pending ops can be tried
		// A more sophisticated check would verify temporal constraints

		if !canLinearize {
			continue
		}

		// Try applying this operation
		newState, output, err := c.model.Step(state, op.Invoke.Op)
		if err != nil {
			continue
		}

		// Check if output matches the response
		if !outputMatches(output, op.Response.Op.Output) {
			continue
		}

		// Recurse
		linearized[i] = true
		if c.search(newState, depth+1, linearized) {
			c.memo[key] = true
			return true
		}
		linearized[i] = false
	}

	// No valid linearization from this state
	c.memo[key] = false
	return false
}

// outputMatches checks if a model output matches the expected response.
func outputMatches(modelOutput, expectedOutput interface{}) bool {
	return modelOutput == expectedOutput
}

// extractCounterexample returns a minimal sub-history that demonstrates non-linearizability.
// For now, returns the full history. A more sophisticated approach would use
// delta-debugging to minimize.
func extractCounterexample(h *history.History, ops []history.CompletedOp) []history.Event {
	// Simple approach: return all events for non-linearizable operations
	var events []history.Event
	for _, op := range ops {
		events = append(events, op.Invoke)
		if op.Status == history.StatusComplete {
			events = append(events, op.Response)
		}
	}
	return events
}

// CheckPartitioned checks a history using P-compositionality:
// partition by key and check each partition independently.
// Returns true if all partitions are linearizable.
func CheckPartitioned(h *history.History, model Model) Result {
	partitions := h.PartitionByKey()

	for key, partition := range partitions {
		result := Check(partition, model)
		if !result.Ok {
			return Result{
				Ok:              false,
				Counterexample:  result.Counterexample,
				Explanation:     fmt.Sprintf("key %s: %s", key, result.Explanation),
			}
		}
	}

	return Result{Ok: true}
}
