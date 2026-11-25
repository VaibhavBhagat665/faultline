// SPDX-License-Identifier: Apache-2.0

package lincheck

import (
	"testing"

	"github.com/VaibhavBhagat665/faultline/internal/history"
)

// TestRegisterModel_PerClientOrderViolation demonstrates the checker hole:
// it accepts a history where a client reads a stale value after its own write.
func TestRegisterModel_PerClientOrderViolation(t *testing.T) {
	h := history.NewHistory()
	model := NewRegisterModel()

	// Client 1: write(x, 42)
	h.Append(history.Event{
		Type:      history.EventInvoke,
		ClientID:  1,
		RequestID: 1,
		Time:      0,
		Op:        history.Operation{Type: history.OpWrite, Key: "x", Input: 42},
	})
	h.Append(history.Event{
		Type:      history.EventOk,
		ClientID:  1,
		RequestID: 1,
		Time:      10,
		Op:        history.Operation{Type: history.OpWrite, Key: "x", Input: 42},
	})

	// Client 1: read(x) → 0 (IMPOSSIBLE: should see 42!)
	h.Append(history.Event{
		Type:      history.EventInvoke,
		ClientID:  1,
		RequestID: 2,
		Time:      20,
		Op:        history.Operation{Type: history.OpRead, Key: "x"},
	})
	h.Append(history.Event{
		Type:      history.EventOk,
		ClientID:  1,
		RequestID: 2,
		Time:      30,
		Op:        history.Operation{Type: history.OpRead, Key: "x", Output: 0},
	})

	result := Check(h, model)

	if result.Ok {
		t.Error("CHECKER BUG: Accepted non-linearizable history!")
		t.Error("Client 1 wrote x=42, then read x=0 (impossible!)")
		t.Error("This violates per-client program order constraint")
		t.Error("Expected: result.Ok == false")
		t.Errorf("Got: result.Ok == %v", result.Ok)
	} else {
		t.Log("✓ Checker correctly rejected per-client order violation")
	}
}

// TestRegisterModel_RealTimeViolation demonstrates real-time precedence hole:
// Op1 completes before Op2 starts, so Op1 must appear before Op2 in any linearization.
func TestRegisterModel_RealTimeViolation(t *testing.T) {
	h := history.NewHistory()
	model := NewRegisterModel()

	// Client 1: write(x, 1) [time 0-10]
	h.Append(history.Event{
		Type:      history.EventInvoke,
		ClientID:  1,
		RequestID: 1,
		Time:      0,
		Op:        history.Operation{Type: history.OpWrite, Key: "x", Input: 1},
	})
	h.Append(history.Event{
		Type:      history.EventOk,
		ClientID:  1,
		RequestID: 1,
		Time:      10,
		Op:        history.Operation{Type: history.OpWrite, Key: "x", Input: 1},
	})

	// Client 2: read(x) → 0 [time 20-30] (IMPOSSIBLE: write completed at t=10!)
	h.Append(history.Event{
		Type:      history.EventInvoke,
		ClientID:  2,
		RequestID: 1,
		Time:      20,
		Op:        history.Operation{Type: history.OpRead, Key: "x"},
	})
	h.Append(history.Event{
		Type:      history.EventOk,
		ClientID:  2,
		RequestID: 1,
		Time:      30,
		Op:        history.Operation{Type: history.OpRead, Key: "x", Output: 0},
	})

	result := Check(h, model)

	if result.Ok {
		t.Error("CHECKER BUG: Accepted non-linearizable history!")
		t.Error("Write(x,1) completed at t=10, Read(x)→0 invoked at t=20")
		t.Error("Real-time precedence requires read to see the write")
		t.Error("Expected: result.Ok == false")
		t.Errorf("Got: result.Ok == %v", result.Ok)
	} else {
		t.Log("✓ Checker correctly rejected real-time precedence violation")
	}
}
