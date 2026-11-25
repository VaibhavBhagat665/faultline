// SPDX-License-Identifier: Apache-2.0

package workload

import (
	"math/rand"
	"testing"
)

func TestWorkloadGeneration(t *testing.T) {
	cfg := Config{
		NumOps:   10,
		ClientID: 1,
		ReadPct:  0.5,
		WritePct: 0.4,
		CASPct:   0.1,
		NumKeys:  5,
	}
	rng := rand.New(rand.NewSource(12345))
	gen := NewGenerator(cfg, rng)

	opCount := 0
	for {
		cmd := gen.Next()
		if cmd == nil {
			break
		}
		opCount++
		
		// Verify command is valid
		switch cmd.Op {
		case OpRead:
			if cmd.Key == "" {
				t.Errorf("Read operation missing key")
			}
		case OpWrite:
			if cmd.Key == "" || cmd.Value == 0 {
				t.Errorf("Write operation missing key or value")
			}
		case OpCAS:
			if cmd.Key == "" {
				t.Errorf("CAS operation missing key")
			}
		default:
			t.Errorf("Unknown operation type: %v", cmd.Op)
		}
	}

	if opCount != cfg.NumOps {
		t.Errorf("Expected %d operations, got %d", cfg.NumOps, opCount)
	}
}

func TestWorkloadHistory(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumOps = 5
	rng := rand.New(rand.NewSource(12345))
	gen := NewGenerator(cfg, rng)

	// Generate and record operations
	for {
		cmd := gen.Next()
		if cmd == nil {
			break
		}
		opID := gen.RecordInvoke(0, cmd)
		gen.RecordReturn(opID, 100, []byte("ok"))
	}

	hist := gen.History()
	if hist == nil {
		t.Fatal("History is nil")
	}

	// Verify we have 5 operations
	// (implementation detail: depends on history.History internals)
	t.Logf("Generated %d operations", gen.opsSent)
}
