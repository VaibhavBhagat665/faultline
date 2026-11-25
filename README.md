# Faultline

**Deterministic Simulation Testing for Raft Consensus**

A framework for testing Raft consensus implementations using deterministic simulation. Inspired by FoundationDB's approach to distributed systems testing.

## Overview

Faultline enables reproducible testing of Raft consensus by simulating network behavior, node failures, and timing variations in a deterministic environment. The same seed produces the exact same execution every time, making distributed system bugs reproducible and debuggable.

## Architecture

```
┌─────────────────────────────────────────┐
│         Raft Implementation             │
│      (Leader Election, Replication)     │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│      Integration Test Layer             │
│  • Cluster orchestration                │
│  • Fault injection                      │
│  • Invariant checking                   │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│    Deterministic Simulation Core        │
│  • Event scheduler (priority queue)     │
│  • Virtual time                         │
│  • Seeded PRNG (splitmix64)             │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│         Simulated Environment           │
│  • Network (drops, delays, partitions)  │
│  • Disk (crash/recovery semantics)      │
│  • Timers (deterministic scheduling)    │
└─────────────────────────────────────────┘
```

## Key Features

### Deterministic Execution
- Same seed → same execution trace
- Reproducible bugs across machines
- No race conditions or timing dependencies

### Network Simulation
- Configurable message drop rates
- Network partitions (symmetric/asymmetric)
- Message delays and reordering
- Full control over message delivery order

### Fault Injection
- Node crashes and recovery
- Disk failures (data loss on crash)
- Clock skew and timeout variations
- Customizable fault profiles

### Verification
- Safety invariants (single leader per term, log matching)
- Liveness properties (progress under majority)
- Linearizability checking (Wing & Gong algorithm)
- History tracking for post-mortem analysis

## Quick Start

### Installation

```bash
git clone https://github.com/VaibhavBhagat665/faultline
cd faultline
go build ./...
```

### Run Demo

```bash
# Simple leader election demo
go run cmd/demo/main.go 42

# Multiple fault scenarios
go run cmd/demo-scenarios/main.go 42
```

**Example output:**
```
=== Faultline Raft Election Demo ===
Seed: 42

Results:
  Leader:  n1
  Term:    4
  Events:  97
  Real time: 1.1ms
  Throughput: 86008 events/sec

Network Stats:
  sent: 93
  dropped: 3
  delivered: 0

✅ Election successful
```

### Run Tests

```bash
# Core simulation tests
go test ./internal/sim/... -v

# Network simulation tests
go test ./internal/simnet/... -v

# Raft integration tests
go test ./internal/integration/... -short -v

# Verification script
./verify.ps1  # Windows
./verify.sh   # Linux/Mac
```

## Usage Example

```go
package main

import (
    "time"
    "github.com/VaibhavBhagat665/faultline/internal/integration"
)

func TestRaftElection() {
    // Create deterministic 5-node cluster
    cluster := integration.NewCluster(42, []string{"n1", "n2", "n3", "n4", "n5"})
    cluster.Init()
    
    // Inject network faults (8% message loss)
    cluster.SetMessageDropRate(0.08)
    
    // Trigger election
    cluster.TriggerElection("n1")
    
    // Run simulation
    cluster.RunFor(1*time.Second, 10000)
    
    // Verify results
    leader, term := cluster.GetLeader()
    if leader == "" {
        panic("No leader elected")
    }
    
    // Check safety invariants
    if violation := cluster.CheckInvariants(); violation != nil {
        panic(violation.Description)
    }
}
```

## Project Structure

```
faultline/
├── adapters/
│   └── raft/                # Raft implementation
├── internal/
│   ├── sim/               # Core simulation engine
│   │   ├── engine.go      # Event scheduler
│   │   ├── queue.go       # Priority queue
│   │   └── rng.go         # Deterministic RNG
│   ├── simnet/            # Network simulation
│   ├── simdisk/           # Disk simulation
│   ├── integration/       # Test framework
│   ├── lincheck/          # Linearizability checker
│   ├── invariants/        # Safety property checkers
│   ├── history/           # Event history tracking
│   └── nemesis/           # Fault injection
├── cmd/
│   ├── demo/              # Simple election demo
│   └── demo-scenarios/    # Multi-scenario demo
└── tests/                 # Integration tests
```

## Core Components

### Event Scheduler (`internal/sim/`)

Deterministic event-driven simulation with virtual time:
- Priority queue for time-ordered event processing
- Seeded splitmix64 PRNG for reproducibility
- No dependency on wall-clock time

### Network Simulator (`internal/simnet/`)

Realistic network behavior without real networking:
- Configurable message drop rates (tested: 0%-25%)
- Network partition support
- Message delay modeling
- Deterministic delivery order based on seed

### Disk Simulator (`internal/simdisk/`)

Persistent storage with crash semantics:
- In-memory key-value store
- fsync() semantics (data loss on crash without sync)
- Deterministic crash injection

### Integration Framework (`internal/integration/`)

High-level orchestration for testing:
- Multi-node cluster management
- Fault injection API
- Invariant checking
- Statistics collection

## Testing Strategy

### Determinism

Same seed produces identical execution:
```bash
go run cmd/demo/main.go 777
go run cmd/demo/main.go 777  # Identical output
```

### Coverage

Different seeds explore different scenarios:
```bash
# Seed 1: Leader n3, Term 4
go run cmd/demo/main.go 1

# Seed 999: Leader n1, Term 5
go run cmd/demo/main.go 999
```

### Fault Injection

Test system behavior under failures:
```go
cluster.SetMessageDropRate(0.15)  // 15% message loss
cluster.PartitionNode("n3")       // Isolate node
cluster.CrashNode("n1")           // Node failure
```

## Performance

| Metric | Value |
|--------|-------|
| Event throughput | 50K-200K events/sec |
| Simulation overhead | 1000x faster than real network |
| Memory usage | O(events) |
| Test execution | ~1ms per election test |

## Comparison with Other Approaches

| Approach | Speed | Determinism | Reproducibility | Setup |
|----------|-------|-------------|-----------------|-------|
| **Faultline** | ⚡⚡⚡ Fast | ✅ 100% | ✅ Perfect | Simple |
| Real network | 🐌 Slow | ❌ No | ⚠️ Limited | Complex |
| Jepsen | 🐌 Very slow | ❌ No | ⚠️ Limited | Complex |
| TLA+ | ⚡⚡⚡ Fast | ✅ 100% | ✅ Perfect | High learning curve |

**Faultline's niche**: Fast, deterministic testing for Go-based Raft implementations with low learning curve.

## Implementation Details

### Determinism

Three key techniques ensure reproducibility:

1. **Deterministic scheduling**: All events (messages, timers) processed in time-ordered priority queue
2. **Seeded RNG**: All randomness derived from initial seed using splitmix64
3. **Virtual time**: No dependency on wall-clock or system time

### Network Simulation

Message drops use deterministic hash function:
```go
hash := hash64(seed, fromNode, toNode, msgID)
drop := (hash % 1000) < dropRatePermille
```

Same seed + same message = same drop decision.

### Raft Implementation

Implementation (`adapters/raft/`) includes:
- Leader election with randomized timeouts
- Log replication (AppendEntries RPC)
- Safety properties (election safety, leader completeness)
- Persistence (term, votedFor, log entries)

## Limitations

- **Go only**: Requires Raft implementation in Go
- **Single process**: Simulates network, doesn't use real networking
- **Raft-focused**: Optimized for Raft, not general consensus
- **Simulated I/O**: Doesn't test real disk or network behavior

## Techniques Used

This project demonstrates:
- Event-driven simulation
- Deterministic testing for distributed systems
- Priority queue-based scheduling
- Network fault modeling
- Consensus algorithm implementation
- Property-based invariant checking

## References

- [Raft Consensus Algorithm](https://raft.github.io/) - Diego Ongaro & John Ousterhout
- [FoundationDB Deterministic Simulation](https://www.youtube.com/watch?v=4fFDFbi3toc) - Will Wilson talk
- [TigerBeetle Simulator](https://github.com/tigerbeetle/tigerbeetle) - Financial database simulation
- [Linearizability Testing](https://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/) - Gavin Lowe

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

## Author

Vaibhav Bhagat

## Contributing

Contributions welcome. Areas of interest:
- Additional consensus algorithms (Paxos, EPaxos)
- Performance optimizations
- Visualization tools
- Bug reports and test cases

---

*Built with deterministic simulation techniques used in production distributed systems.*
