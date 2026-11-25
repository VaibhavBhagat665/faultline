// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// BenchmarkEventQueue_Push benchmarks event queue insertion.
func BenchmarkEventQueue_Push(b *testing.B) {
	q := NewEventQueue()
	events := make([]Event, b.N)

	for i := range events {
		events[i] = &testEvent{
			baseEvent: baseEvent{
				time:     Time(i),
				priority: PriorityTimer,
				seq:      uint64(i),
			},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(events[i])
	}
}

// BenchmarkEventQueue_Pop benchmarks event queue extraction.
func BenchmarkEventQueue_Pop(b *testing.B) {
	q := NewEventQueue()

	for i := 0; i < b.N; i++ {
		q.Push(&testEvent{
			baseEvent: baseEvent{
				time:     Time(i),
				priority: PriorityTimer,
				seq:      uint64(i),
			},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Pop()
	}
}

// BenchmarkEventQueue_PushPop benchmarks alternating push/pop.
func BenchmarkEventQueue_PushPop(b *testing.B) {
	q := NewEventQueue()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(&testEvent{
			baseEvent: baseEvent{
				time:     Time(i),
				priority: PriorityTimer,
				seq:      uint64(i),
			},
		})
		if i%2 == 1 {
			q.Pop()
		}
	}
}

// BenchmarkRawEventThroughput measures events/sec for minimal events.
func BenchmarkRawEventThroughput(b *testing.B) {
	benchmarks := []struct {
		name   string
		events int
	}{
		{"1K", 1000},
		{"10K", 10000},
		{"100K", 100000},
		{"1M", 1000000},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				cfg := Config{
					Seed:      uint64(i),
					MaxEvents: uint64(bm.events),
				}
				e := NewEngine(cfg)

				counter := 0
				for j := 0; j < bm.events; j++ {
					e.queue.Push(&counterEvent{
						baseEvent: baseEvent{
							time:     Time(j),
							priority: PriorityTimer,
							seq:      uint64(j),
						},
						counter: &counter,
					})
				}

				b.StartTimer()
				start := time.Now()
				result := e.Run()
				elapsed := time.Since(start)
				b.StopTimer()

				eventsPerSec := float64(result.EventsProcessed) / elapsed.Seconds()
				b.ReportMetric(eventsPerSec, "events/sec")
			}
		})
	}
}

// BenchmarkSimulationOverhead measures the overhead of the simulation framework.
func BenchmarkSimulationOverhead(b *testing.B) {
	const events = 10000

	b.Run("DirectExecution", func(b *testing.B) {
		// Baseline: direct function calls
		counter := 0
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < events; j++ {
				counter++
			}
		}
	})

	b.Run("ThroughEngine", func(b *testing.B) {
		// Through the simulation engine
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			cfg := Config{Seed: uint64(i)}
			e := NewEngine(cfg)
			counter := 0
			for j := 0; j < events; j++ {
				e.queue.Push(&counterEvent{
					baseEvent: baseEvent{
						time:     Time(j),
						priority: PriorityTimer,
						seq:      uint64(j),
					},
					counter: &counter,
				})
			}
			b.StartTimer()
			e.Run()
		}
	})
}

// BenchmarkTraceHashing measures trace hash computation overhead.
func BenchmarkTraceHashing(b *testing.B) {
	b.Run("WithTrace", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cfg := Config{Seed: uint64(i)}
			e := NewEngine(cfg)

			for j := 0; j < 1000; j++ {
				e.queue.Push(&deterministicTestEvent{
					baseEvent: baseEvent{
						time:     Time(j),
						priority: PriorityTimer,
						seq:      uint64(j),
					},
					value: j,
				})
			}

			e.Run()
			_ = e.TraceHash()
		}
	})
}

// BenchmarkConcurrentSimulations measures parallel simulation throughput.
func BenchmarkConcurrentSimulations(b *testing.B) {
	const eventsPerSim = 1000

	b.RunParallel(func(pb *testing.PB) {
		seed := uint64(0)
		for pb.Next() {
			cfg := Config{Seed: seed}
			e := NewEngine(cfg)

			counter := 0
			for j := 0; j < eventsPerSim; j++ {
				e.queue.Push(&counterEvent{
					baseEvent: baseEvent{
						time:     Time(j),
						priority: PriorityTimer,
						seq:      uint64(j),
					},
					counter: &counter,
				})
			}

			e.Run()
			seed++
		}
	})
}

// reportThroughput is a helper that runs a benchmark and reports results.
func reportThroughput(b *testing.B, name string, events int) {
	b.Helper()

	cfg := Config{
		Seed:      12345,
		MaxEvents: uint64(events),
	}

	e := NewEngine(cfg)
	counter := 0

	for i := 0; i < events; i++ {
		e.queue.Push(&counterEvent{
			baseEvent: baseEvent{
				time:     Time(i),
				priority: PriorityTimer,
				seq:      uint64(i),
			},
			counter: &counter,
		})
	}

	b.ResetTimer()
	start := time.Now()
	result := e.Run()
	elapsed := time.Since(start)
	b.StopTimer()

	eventsPerSec := float64(result.EventsProcessed) / elapsed.Seconds()

	b.ReportMetric(eventsPerSec, "events/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(result.EventsProcessed), "ns/event")

	fmt.Printf("\n%s Throughput Report:\n", name)
	fmt.Printf("  Events processed: %d\n", result.EventsProcessed)
	fmt.Printf("  Wall-clock time:  %v\n", elapsed)
	fmt.Printf("  Events/sec:       %.0f\n", eventsPerSec)
	fmt.Printf("  Nanosec/event:    %.2f\n", float64(elapsed.Nanoseconds())/float64(result.EventsProcessed))
	fmt.Printf("  Virtual time:     %v\n", result.FinalTime)
	fmt.Printf("  Trace hash:       %s\n", result.TraceHash[:16]+"...")
}

// TestBenchmark_Throughput runs a standard throughput test and reports results.
// This is the benchmark that will be recorded in docs/BENCHMARKS.md.
func TestBenchmark_Throughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput benchmark in short mode")
	}

	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  FAULTLINE PHASE 1 THROUGHPUT BENCHMARK")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	fmt.Printf("\nEnvironment:\n")
	fmt.Printf("  OS:           %s\n", runtime.GOOS)
	fmt.Printf("  Arch:         %s\n", runtime.GOARCH)
	fmt.Printf("  Go version:   %s\n", runtime.Version())
	fmt.Printf("  CPUs:         %d\n", runtime.NumCPU())
	fmt.Printf("  GOMAXPROCS:   %d\n", runtime.GOMAXPROCS(0))

	configs := []struct {
		name   string
		events int
	}{
		{"Small (1K events)", 1000},
		{"Medium (10K events)", 10000},
		{"Large (100K events)", 100000},
		{"XLarge (1M events)", 1000000},
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			b := &testing.B{N: 1}
			reportThroughput(b, cfg.name, cfg.events)
		})
	}

	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("  Target: 200,000+ events/sec/core")
	fmt.Println("  Run: go test -v ./internal/sim -run TestBenchmark_Throughput")
	fmt.Print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
}
