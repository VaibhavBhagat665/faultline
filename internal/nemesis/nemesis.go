// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package nemesis

import (
	"math/rand/v2"

	"github.com/VaibhavBhagat665/faultline/internal/sim"
)

// FaultClass represents a category of faults that can be enabled or disabled.
type FaultClass int

const (
	// Network faults
	FaultNetworkDrop FaultClass = iota
	FaultNetworkDelay
	FaultNetworkDuplicate
	FaultNetworkReorder
	FaultNetworkPartition

	// Disk faults
	FaultDiskWriteFailure
	FaultDiskFsyncFailure
	FaultDiskCrashTear
	FaultDiskCrashDrop

	// Process faults
	FaultProcessCrash
	FaultProcessPause

	// Clock faults
	FaultClockOffset
	FaultClockDrift

	numFaultClasses // sentinel
)

// FaultClass names for debugging and reporting.
var faultClassNames = map[FaultClass]string{
	FaultNetworkDrop:      "network.drop",
	FaultNetworkDelay:     "network.delay",
	FaultNetworkDuplicate: "network.duplicate",
	FaultNetworkReorder:   "network.reorder",
	FaultNetworkPartition: "network.partition",
	FaultDiskWriteFailure: "disk.write_failure",
	FaultDiskFsyncFailure: "disk.fsync_failure",
	FaultDiskCrashTear:    "disk.crash_tear",
	FaultDiskCrashDrop:    "disk.crash_drop",
	FaultProcessCrash:     "process.crash",
	FaultProcessPause:     "process.pause",
	FaultClockOffset:      "clock.offset",
	FaultClockDrift:       "clock.drift",
}

func (fc FaultClass) String() string {
	if name, ok := faultClassNames[fc]; ok {
		return name
	}
	return "unknown"
}

// FaultEvent represents a scheduled fault injection.
type FaultEvent struct {
	// Time is when the fault should be applied (virtual time).
	Time sim.Time

	// Class is the type of fault.
	Class FaultClass

	// Target identifies the affected entity (e.g., node ID, link).
	// Interpretation depends on the fault class.
	Target int

	// Param is a fault-specific parameter (e.g., duration, rate).
	Param float64

	// Duration is how long the fault lasts (0 = instantaneous).
	Duration sim.Time
}

// Schedule is a sequence of fault events generated from a seed.
type Schedule struct {
	// Seed is the master seed that generated this schedule.
	Seed uint64

	// EnabledClasses tracks which fault classes are active (swarm testing).
	EnabledClasses map[FaultClass]bool

	// Events is the list of scheduled faults, sorted by Time.
	Events []FaultEvent
}

// Config configures the nemesis fault scheduler.
type Config struct {
	// Seed is the master seed for fault generation.
	Seed uint64

	// Duration is the total virtual time span for fault injection.
	Duration sim.Time

	// NumNodes is the number of nodes in the cluster.
	NumNodes int

	// SwarmProbability is the probability [0, 1] that each fault class is enabled.
	// Lower values = more focused testing (fewer concurrent fault types).
	// Higher values = more chaotic testing (many fault types at once).
	// Default: 0.5 (50% chance each class is enabled).
	SwarmProbability float64

	// EventRate is the average number of fault events per unit time.
	// Higher = more frequent faults.
	EventRate float64
}

// Generate creates a fault schedule from the given configuration.
// Uses swarm testing to randomly enable/disable fault classes.
func Generate(cfg Config) Schedule {
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0xdeadbeef))

	// Swarm testing: randomly enable fault classes
	enabled := make(map[FaultClass]bool)
	for fc := FaultClass(0); fc < numFaultClasses; fc++ {
		if rng.Float64() < cfg.SwarmProbability {
			enabled[fc] = true
		}
	}

	// Ensure at least one fault class is enabled
	if len(enabled) == 0 {
		// Pick a random class
		fc := FaultClass(rng.IntN(int(numFaultClasses)))
		enabled[fc] = true
	}

	// Generate fault events
	var events []FaultEvent

	// Compute expected number of events (Poisson-like)
	expectedEvents := int(float64(cfg.Duration) * cfg.EventRate / 1e9) // Duration is in ns
	if expectedEvents < 1 {
		expectedEvents = 1
	}

	// Convert enabled map to sorted list for deterministic iteration
	var enabledList []FaultClass
	for fc := range enabled {
		enabledList = append(enabledList, fc)
	}
	// Sort for determinism
	for i := 0; i < len(enabledList); i++ {
		for j := i + 1; j < len(enabledList); j++ {
			if enabledList[j] < enabledList[i] {
				enabledList[i], enabledList[j] = enabledList[j], enabledList[i]
			}
		}
	}

	for i := 0; i < expectedEvents; i++ {
		if len(enabledList) == 0 {
			break
		}

		fc := enabledList[rng.IntN(len(enabledList))]

		// Random time within duration
		t := sim.Time(rng.Int64N(int64(cfg.Duration)))

		// Random target (node or link)
		target := rng.IntN(cfg.NumNodes)

		// Fault-specific parameters
		var param float64
		var duration sim.Time

		switch fc {
		case FaultNetworkDrop, FaultNetworkDuplicate:
			param = rng.Float64() * 0.5 // 0-50% rate
			duration = sim.Time(rng.Int64N(int64(cfg.Duration) / 4))

		case FaultNetworkDelay, FaultNetworkReorder:
			param = float64(rng.Int64N(1000)) // 0-1000ns extra delay
			duration = sim.Time(rng.Int64N(int64(cfg.Duration) / 4))

		case FaultNetworkPartition:
			duration = sim.Time(rng.Int64N(int64(cfg.Duration) / 2))

		case FaultDiskWriteFailure, FaultDiskFsyncFailure:
			param = rng.Float64() * 0.3 // 0-30% failure rate
			duration = sim.Time(rng.Int64N(int64(cfg.Duration) / 4))

		case FaultDiskCrashTear, FaultDiskCrashDrop:
			param = rng.Float64() * 0.5 // 0-50% rate

		case FaultProcessCrash:
			// Crash is instantaneous, recovery handled separately
			duration = 0

		case FaultProcessPause:
			duration = sim.Time(rng.Int64N(5000)) // 0-5000ns pause

		case FaultClockOffset:
			param = float64(rng.Int64N(2000)) - 1000 // -1000 to +1000ns offset

		case FaultClockDrift:
			param = rng.Float64()*0.0002 - 0.0001 // -0.01% to +0.01% drift
		}

		events = append(events, FaultEvent{
			Time:     t,
			Class:    fc,
			Target:   target,
			Param:    param,
			Duration: duration,
		})
	}

	// Sort events by time (simple bubble sort, good enough for now)
	for i := 0; i < len(events); i++ {
		for j := i + 1; j < len(events); j++ {
			if events[j].Time < events[i].Time {
				events[i], events[j] = events[j], events[i]
			}
		}
	}

	return Schedule{
		Seed:           cfg.Seed,
		EnabledClasses: enabled,
		Events:         events,
	}
}

// EnabledClassNames returns a list of enabled fault class names.
func (s *Schedule) EnabledClassNames() []string {
	var names []string
	for fc := range s.EnabledClasses {
		names = append(names, fc.String())
	}
	return names
}

// FaultCoverageMatrix tracks which fault classes have fired across multiple runs.
// Used for reporting: "Of N runs, how many exercised each fault class?"
type FaultCoverageMatrix struct {
	Runs     int
	Coverage map[FaultClass]int // class -> number of runs that enabled it
}

// NewFaultCoverageMatrix creates a new coverage matrix.
func NewFaultCoverageMatrix() *FaultCoverageMatrix {
	return &FaultCoverageMatrix{
		Coverage: make(map[FaultClass]int),
	}
}

// Record records a schedule's enabled classes.
func (m *FaultCoverageMatrix) Record(s Schedule) {
	m.Runs++
	for fc := range s.EnabledClasses {
		m.Coverage[fc]++
	}
}

// Report returns a human-readable coverage report.
func (m *FaultCoverageMatrix) Report() map[string]float64 {
	report := make(map[string]float64)
	for fc := FaultClass(0); fc < numFaultClasses; fc++ {
		count := m.Coverage[fc]
		pct := 0.0
		if m.Runs > 0 {
			pct = float64(count) / float64(m.Runs) * 100
		}
		report[fc.String()] = pct
	}
	return report
}
