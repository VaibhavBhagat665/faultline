// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package lincheck

import (
	"github.com/VaibhavBhagat665/faultline/internal/history"
)

// BruteForceCheck checks linearizability by trying all permutations.
// Only feasible for small histories (≤10 operations).
// Used for differential testing against the main checker.
func BruteForceCheck(h *history.History, model Model) bool {
	ops := h.CompletedOps()

	// Extract completed operations
	var completed []history.CompletedOp
	for _, op := range ops {
		if op.Status == history.StatusComplete {
			completed = append(completed, op)
		}
	}

	if len(completed) == 0 {
		return true
	}

	if len(completed) > 10 {
		panic("bruteforce: too many operations (max 10)")
	}

	// Generate all permutations and check each
	perm := make([]int, len(completed))
	for i := range perm {
		perm[i] = i
	}

	return checkPermutations(model, completed, perm, 0)
}

// checkPermutations recursively generates and checks all permutations.
func checkPermutations(model Model, ops []history.CompletedOp, perm []int, start int) bool {
	if start == len(perm) {
		// Try this permutation
		return tryLinearization(model, ops, perm)
	}

	for i := start; i < len(perm); i++ {
		// Swap
		perm[start], perm[i] = perm[i], perm[start]

		if checkPermutations(model, ops, perm, start+1) {
			return true
		}

		// Backtrack
		perm[start], perm[i] = perm[i], perm[start]
	}

	return false
}

// tryLinearization checks if a specific permutation is a valid linearization.
func tryLinearization(model Model, ops []history.CompletedOp, perm []int) bool {
	state := model.Init()

	for _, idx := range perm {
		op := ops[idx]

		newState, output, err := model.Step(state, op.Invoke.Op)
		if err != nil {
			return false
		}

		if !outputMatches(output, op.Response.Op.Output) {
			return false
		}

		state = newState
	}

	return true
}
