// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package nemesis

import (
	"testing"

	"github.com/VaibhavBhagat665/faultline/internal/sim"
)

func TestGenerate_BasicSchedule(t *testing.T) {
	cfg := Config{
		Seed:             12345,
		Duration:         10000,
		NumNodes:         3,
		SwarmProbability: 0.5,
		EventRate:        0.001, // 1 event per 1000ns
	}

	sched := Generate(cfg)

	if sched.Seed != cfg.Seed {
		t.Errorf("expected seed %d, got %d", cfg.Seed, sched.Seed)
	}

	if len(sched.Events) == 0 {
		t.Error("expected at least one fault event")
	}

	if len(sched.EnabledClasses) == 0 {
		t.Error("expected at least one enabled fault class")
	}

	t.Logf("Generated %d events with %d enabled fault classes", len(sched.Events), len(sched.EnabledClasses))
	t.Logf("Enabled classes: %v", sched.EnabledClassNames())
}

func TestGenerate_Determinism(t *testing.T) {
	cfg := Config{
		Seed:             99999,
		Duration:         5000,
		NumNodes:         5,
		SwarmProbability: 0.5,
		EventRate:        0.002,
	}

	sched1 := Generate(cfg)
	sched2 := Generate(cfg)

	// Same seed should produce identical schedules
	if len(sched1.Events) != len(sched2.Events) {
		t.Fatalf("event count mismatch: %d vs %d", len(sched1.Events), len(sched2.Events))
	}

	if len(sched1.EnabledClasses) != len(sched2.EnabledClasses) {
		t.Fatalf("enabled classes count mismatch: %d vs %d", len(sched1.EnabledClasses), len(sched2.EnabledClasses))
	}

	for i := range sched1.Events {
		e1 := sched1.Events[i]
		e2 := sched2.Events[i]

		if e1.Time != e2.Time {
			t.Errorf("event %d: time mismatch: %d vs %d", i, e1.Time, e2.Time)
		}
		if e1.Class != e2.Class {
			t.Errorf("event %d: class mismatch: %s vs %s", i, e1.Class, e2.Class)
		}
		if e1.Target != e2.Target {
			t.Errorf("event %d: target mismatch: %d vs %d", i, e1.Target, e2.Target)
		}
		if e1.Param != e2.Param {
			t.Errorf("event %d: param mismatch: %f vs %f", i, e1.Param, e2.Param)
		}
		if e1.Duration != e2.Duration {
			t.Errorf("event %d: duration mismatch: %d vs %d", i, e1.Duration, e2.Duration)
		}
	}

	for fc := range sched1.EnabledClasses {
		if !sched2.EnabledClasses[fc] {
			t.Errorf("class %s enabled in sched1 but not sched2", fc)
		}
	}

	t.Log("✓ Determinism verified")
}

func TestGenerate_DifferentSeeds(t *testing.T) {
	cfg1 := Config{
		Seed:             111,
		Duration:         5000,
		NumNodes:         3,
		SwarmProbability: 0.5,
		EventRate:        0.001,
	}

	cfg2 := Config{
		Seed:             222,
		Duration:         5000,
		NumNodes:         3,
		SwarmProbability: 0.5,
		EventRate:        0.001,
	}

	sched1 := Generate(cfg1)
	sched2 := Generate(cfg2)

	// Different seeds should (very likely) produce different schedules
	if len(sched1.Events) == len(sched2.Events) {
		same := true
		for i := range sched1.Events {
			if sched1.Events[i].Time != sched2.Events[i].Time ||
				sched1.Events[i].Class != sched2.Events[i].Class {
				same = false
				break
			}
		}
		if same {
			t.Error("different seeds produced identical schedules (extremely unlikely)")
		}
	}

	t.Logf("Seed 111: %d events, %d classes", len(sched1.Events), len(sched1.EnabledClasses))
	t.Logf("Seed 222: %d events, %d classes", len(sched2.Events), len(sched2.EnabledClasses))
}

func TestGenerate_SwarmTesting(t *testing.T) {
	// Test various swarm probabilities
	tests := []struct {
		name  string
		prob  float64
		runs  int
		minPct float64
		maxPct float64
	}{
		{"low", 0.1, 100, 0, 30},      // Expect 10% ± some variance
		{"medium", 0.5, 100, 30, 70},  // Expect 50% ± variance
		{"high", 0.9, 100, 70, 100},   // Expect 90% ± variance
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classCount := make(map[FaultClass]int)

			for i := 0; i < tt.runs; i++ {
				cfg := Config{
					Seed:             uint64(i + 1000),
					Duration:         1000,
					NumNodes:         3,
					SwarmProbability: tt.prob,
					EventRate:        0.001,
				}

				sched := Generate(cfg)

				for fc := range sched.EnabledClasses {
					classCount[fc]++
				}
			}

			// Check that each class appears in roughly the expected percentage of runs
			for fc := FaultClass(0); fc < numFaultClasses; fc++ {
				count := classCount[fc]
				pct := float64(count) / float64(tt.runs) * 100

				if pct < tt.minPct || pct > tt.maxPct {
					t.Errorf("class %s: %.1f%% of runs (expected %.0f-%.0f%%)",
						fc, pct, tt.minPct, tt.maxPct)
				}
			}

			t.Logf("Swarm probability %.1f: average %.1f classes enabled per run",
				tt.prob, float64(sum(classCount))/float64(tt.runs))
		})
	}
}

func sum(m map[FaultClass]int) int {
	total := 0
	for _, v := range m {
		total += v
	}
	return total
}

func TestGenerate_EventsSorted(t *testing.T) {
	cfg := Config{
		Seed:             42,
		Duration:         10000,
		NumNodes:         3,
		SwarmProbability: 0.5,
		EventRate:        0.005,
	}

	sched := Generate(cfg)

	// Check that events are sorted by time
	for i := 1; i < len(sched.Events); i++ {
		if sched.Events[i].Time < sched.Events[i-1].Time {
			t.Errorf("events not sorted: event %d time=%d, event %d time=%d",
				i-1, sched.Events[i-1].Time, i, sched.Events[i].Time)
		}
	}

	t.Logf("✓ %d events are sorted by time", len(sched.Events))
}

func TestGenerate_AlwaysAtLeastOneClass(t *testing.T) {
	// Even with swarm probability 0, should enable at least one class
	cfg := Config{
		Seed:             123,
		Duration:         1000,
		NumNodes:         3,
		SwarmProbability: 0.0, // Never enable randomly
		EventRate:        0.001,
	}

	sched := Generate(cfg)

	if len(sched.EnabledClasses) == 0 {
		t.Error("expected at least one enabled class even with prob=0")
	}

	t.Logf("With prob=0, forced enabled class: %v", sched.EnabledClassNames())
}

func TestFaultCoverageMatrix(t *testing.T) {
	matrix := NewFaultCoverageMatrix()

	// Run multiple schedules
	for i := 0; i < 100; i++ {
		cfg := Config{
			Seed:             uint64(i),
			Duration:         1000,
			NumNodes:         3,
			SwarmProbability: 0.5,
			EventRate:        0.001,
		}
		sched := Generate(cfg)
		matrix.Record(sched)
	}

	if matrix.Runs != 100 {
		t.Errorf("expected 100 runs, got %d", matrix.Runs)
	}

	report := matrix.Report()

	for fc := FaultClass(0); fc < numFaultClasses; fc++ {
		pct := report[fc.String()]
		// With prob=0.5 and 100 runs, each class should appear in ~40-60% of runs
		if pct < 30 || pct > 70 {
			t.Errorf("class %s: %.1f%% coverage (expected ~40-60%%)", fc, pct)
		}
	}

	t.Log("Coverage report:")
	for fc := FaultClass(0); fc < numFaultClasses; fc++ {
		t.Logf("  %s: %.1f%%", fc, report[fc.String()])
	}
}

func TestFaultEvent_TimeRange(t *testing.T) {
	cfg := Config{
		Seed:             77777,
		Duration:         sim.Time(10000),
		NumNodes:         3,
		SwarmProbability: 0.8,
		EventRate:        0.01,
	}

	sched := Generate(cfg)

	// All events should be within [0, Duration)
	for i, evt := range sched.Events {
		if evt.Time < 0 || evt.Time >= cfg.Duration {
			t.Errorf("event %d: time %d outside [0, %d)", i, evt.Time, cfg.Duration)
		}
	}

	t.Logf("✓ All %d events within time range", len(sched.Events))
}

func TestFaultClassString(t *testing.T) {
	tests := []struct {
		fc   FaultClass
		want string
	}{
		{FaultNetworkDrop, "network.drop"},
		{FaultDiskFsyncFailure, "disk.fsync_failure"},
		{FaultProcessCrash, "process.crash"},
		{FaultClockOffset, "clock.offset"},
	}

	for _, tt := range tests {
		got := tt.fc.String()
		if got != tt.want {
			t.Errorf("%v.String() = %q, want %q", tt.fc, got, tt.want)
		}
	}
}
