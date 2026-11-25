// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package lincheck

import (
	"math/rand"
	"testing"

	"github.com/VaibhavBhagat665/faultline/internal/history"
)

// TestRegisterModel_Sequential tests the register model with a simple sequential history.
func TestRegisterModel_Sequential(t *testing.T) {
	h := history.NewHistory()
	model := NewRegisterModel()

	// write(1)
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Input: 1}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Input: 1}})

	// read() → 1
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 2, Op: history.Operation{Type: history.OpRead}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 2, Op: history.Operation{Type: history.OpRead, Output: 1}})

	result := Check(h, model)
	if !result.Ok {
		t.Errorf("expected linearizable, got: %s", result.Explanation)
	}
}

// TestRegisterModel_Concurrent tests a KV model with concurrent writes.
func TestKVModel_Concurrent(t *testing.T) {
	h := history.NewHistory()
	model := NewKVModel()

	// Client 1: write(a, 1)
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Key: "a", Input: 1}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Key: "a", Input: 1}})

	// Client 2: write(b, 2)
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 2, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Key: "b", Input: 2}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 2, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Key: "b", Input: 2}})

	// Client 3: read(a) → 1
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 3, RequestID: 1, Op: history.Operation{Type: history.OpRead, Key: "a"}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 3, RequestID: 1, Op: history.Operation{Type: history.OpRead, Key: "a", Output: 1}})

	// Client 4: read(b) → 2
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 4, RequestID: 1, Op: history.Operation{Type: history.OpRead, Key: "b"}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 4, RequestID: 1, Op: history.Operation{Type: history.OpRead, Key: "b", Output: 2}})

	result := Check(h, model)
	if !result.Ok {
		t.Errorf("expected linearizable, got: %s", result.Explanation)
	}
}

// TestCASModel_Success tests a successful CAS.
func TestCASModel_Success(t *testing.T) {
	h := history.NewHistory()
	model := NewCASModel()

	// cas(nil, 1) → true
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpCAS, Input: CASInput{Old: nil, New: 1}}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpCAS, Input: CASInput{Old: nil, New: 1}, Output: true}})

	// read() → 1
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 2, Op: history.Operation{Type: history.OpRead}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 2, Op: history.Operation{Type: history.OpRead, Output: 1}})

	result := Check(h, model)
	if !result.Ok {
		t.Errorf("expected linearizable, got: %s", result.Explanation)
	}
}

// TestCASModel_Failure tests a failed CAS.
func TestCASModel_Failure(t *testing.T) {
	h := history.NewHistory()
	model := NewCASModel()

	// cas(nil, 1) → true
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpCAS, Input: CASInput{Old: nil, New: 1}}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpCAS, Input: CASInput{Old: nil, New: 1}, Output: true}})

	// cas(nil, 2) → false (should fail, value is now 1)
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 2, RequestID: 1, Op: history.Operation{Type: history.OpCAS, Input: CASInput{Old: nil, New: 2}}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 2, RequestID: 1, Op: history.Operation{Type: history.OpCAS, Input: CASInput{Old: nil, New: 2}, Output: false}})

	// read() → 1
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 2, RequestID: 2, Op: history.Operation{Type: history.OpRead}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 2, RequestID: 2, Op: history.Operation{Type: history.OpRead, Output: 1}})

	result := Check(h, model)
	if !result.Ok {
		t.Errorf("expected linearizable, got: %s", result.Explanation)
	}
}

// TestDifferential_RandomHistories tests the checker against the brute-force oracle.
func TestDifferential_RandomHistories(t *testing.T) {
	model := NewRegisterModel()
	rng := rand.New(rand.NewSource(42))

	const numTrials = 100

	for trial := 0; trial < numTrials; trial++ {
		// Generate a small random history (3-6 operations)
		numOps := 3 + rng.Intn(4)
		h := generateRandomHistory(rng, numOps)

		// Check with main checker
		mainResult := Check(h, model)

		// Check with brute-force oracle
		oracleResult := BruteForceCheck(h, model)

		if mainResult.Ok != oracleResult {
			t.Errorf("trial %d: main=%v, oracle=%v", trial, mainResult.Ok, oracleResult)
			t.Logf("History: %v", h.Events)
		}
	}

	t.Logf("✓ %d random histories: main checker agrees with oracle", numTrials)
}

// generateRandomHistory creates a random history for testing.
func generateRandomHistory(rng *rand.Rand, numOps int) *history.History {
	h := history.NewHistory()

	values := []int{1, 2, 3}

	for i := 0; i < numOps; i++ {
		clientID := rng.Intn(3) + 1
		requestID := i + 1

		if rng.Float64() < 0.5 {
			// Write
			val := values[rng.Intn(len(values))]
			h.Append(history.Event{
				Type:      history.EventInvoke,
				ClientID:  clientID,
				RequestID: requestID,
				Op:        history.Operation{Type: history.OpWrite, Input: val},
			})
			h.Append(history.Event{
				Type:      history.EventOk,
				ClientID:  clientID,
				RequestID: requestID,
				Op:        history.Operation{Type: history.OpWrite, Input: val},
			})
		} else {
			// Read
			val := values[rng.Intn(len(values))]
			h.Append(history.Event{
				Type:      history.EventInvoke,
				ClientID:  clientID,
				RequestID: requestID,
				Op:        history.Operation{Type: history.OpRead},
			})
			h.Append(history.Event{
				Type:      history.EventOk,
				ClientID:  clientID,
				RequestID: requestID,
				Op:        history.Operation{Type: history.OpRead, Output: val},
			})
		}
	}

	return h
}

// TestKnownLinearizable tests known linearizable cases from literature.
func TestKnownLinearizable(t *testing.T) {
	tests := []struct {
		name string
		hist *history.History
	}{
		{
			name: "Empty",
			hist: history.NewHistory(),
		},
		{
			name: "Single write",
			hist: func() *history.History {
				h := history.NewHistory()
				h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Input: 1}})
				h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Input: 1}})
				return h
			}(),
		},
		{
			name: "Sequential write-read",
			hist: func() *history.History {
				h := history.NewHistory()
				h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Input: 42}})
				h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Input: 42}})
				h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 2, Op: history.Operation{Type: history.OpRead}})
				h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 2, Op: history.Operation{Type: history.OpRead, Output: 42}})
				return h
			}(),
		},
	}

	model := NewRegisterModel()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Check(tt.hist, model)
			if !result.Ok {
				t.Errorf("expected linearizable, got: %s", result.Explanation)
			}
		})
	}
}

// TestKnownNonLinearizable tests known non-linearizable cases.
func TestKnownNonLinearizable(t *testing.T) {
	tests := []struct {
		name string
		hist *history.History
	}{
		{
			name: "Read returns wrong value",
			hist: func() *history.History {
				h := history.NewHistory()
				h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Input: 1}})
				h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Input: 1}})
				h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 2, Op: history.Operation{Type: history.OpRead}})
				h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 2, Op: history.Operation{Type: history.OpRead, Output: 99}}) // wrong!
				return h
			}(),
		},
	}

	model := NewRegisterModel()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Check(tt.hist, model)
			if result.Ok {
				t.Error("expected non-linearizable, but got linearizable")
			}
			t.Logf("Correctly detected: %s", result.Explanation)
		})
	}
}

// TestPCompositionality tests partitioned checking.
func TestPCompositionality(t *testing.T) {
	h := history.NewHistory()
	model := NewKVModel()

	// write(a, 1)
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Key: "a", Input: 1}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 1, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Key: "a", Input: 1}})

	// write(b, 2)
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 2, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Key: "b", Input: 2}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 2, RequestID: 1, Op: history.Operation{Type: history.OpWrite, Key: "b", Input: 2}})

	// read(a) → 1
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 3, RequestID: 1, Op: history.Operation{Type: history.OpRead, Key: "a"}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 3, RequestID: 1, Op: history.Operation{Type: history.OpRead, Key: "a", Output: 1}})

	// read(b) → 2
	h.Append(history.Event{Type: history.EventInvoke, ClientID: 4, RequestID: 1, Op: history.Operation{Type: history.OpRead, Key: "b"}})
	h.Append(history.Event{Type: history.EventOk, ClientID: 4, RequestID: 1, Op: history.Operation{Type: history.OpRead, Key: "b", Output: 2}})

	result := CheckPartitioned(h, model)
	if !result.Ok {
		t.Errorf("expected linearizable, got: %s", result.Explanation)
	}

	t.Log("✓ P-compositionality check passed")
}
