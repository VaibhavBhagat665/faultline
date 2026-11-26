<div align="center">

# 🔥 Faultline

### Deterministic Simulation Testing for Distributed Consensus

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/VaibhavBhagat665/faultline)

*Making distributed systems bugs reproducible, one seed at a time.*

[Features](#-features) • [Quick Start](#-quick-start) • [Architecture](#-architecture) • [Documentation](#-documentation)

</div>

---

## 🎯 Overview

**Faultline** is a deterministic simulation framework for testing Raft consensus implementations. Inspired by FoundationDB's groundbreaking approach, it enables reproducible testing of distributed systems by eliminating non-determinism from network, disk, and timing behavior.

### The Problem

Traditional distributed systems testing is fundamentally flawed:

```
Real Network Test:
├── ❌ Non-deterministic (race conditions, timing dependencies)
├── ❌ Slow (actual network delays, 100ms+ per message)
├── ❌ Limited coverage (can't explore rare scenarios)
└── ❌ Non-reproducible (bugs disappear when you debug them)
```

### The Solution

```
Faultline Simulation:
├── ✅ 100% Deterministic (same seed = exact same execution)
├── ✅ Fast (50K-200K events/sec, no real network)
├── ✅ Comprehensive (test millions of scenarios)
└── ✅ Reproducible (replay any failure instantly)
```

---

## 🚀 Features

### 🎲 **Deterministic Execution**

Every aspect of execution is seeded and reproducible:

| Component | Determinism Mechanism |
|-----------|----------------------|
| **Event Ordering** | Priority queue with virtual time |
| **Randomness** | Seeded splitmix64 PRNG |
| **Message Delivery** | Hash-based deterministic drops |
| **Timers** | Virtual time, no wall-clock dependency |

**Result:** Same seed → Exact same execution, every time.

```go
cluster := NewCluster(42, nodes)  // Seed 42
cluster.RunFor(1*time.Second)
// Always produces identical results
```

### 🌐 **Network Simulation**

Realistic network behavior without real sockets:

```go
// Configure network faults
cluster.SetMessageDropRate(0.15)         // 15% packet loss
cluster.SetMessageDelay(10ms, 100ms)      // Variable latency
cluster.PartitionNode("n3")               // Network partition
```

**Features:**
- ✅ Configurable message drop rates (0-100%)
- ✅ Asymmetric network partitions
- ✅ Message delays and reordering
- ✅ Deterministic delivery based on seed

### 💾 **Disk Simulation**

Persistent storage with crash semantics:

```go
disk.Write(key, value)
disk.Sync()          // fsync() semantics
// Crash before sync → data lost
// Crash after sync → data persisted
```

**Features:**
- ✅ In-memory key-value store
- ✅ fsync() crash semantics
- ✅ Deterministic data loss patterns

### 🔍 **Fault Injection**

Comprehensive failure scenario testing:

| Fault Type | Description | Use Case |
|------------|-------------|----------|
| **Message Loss** | Probabilistic drops | Test timeout handling |
| **Network Partition** | Isolate nodes | Test split-brain scenarios |
| **Node Crashes** | Sudden termination | Test recovery logic |
| **Disk Failures** | Data loss | Test durability |

### ✅ **Verification & Invariants**

Automatic correctness checking:

```go
// Built-in Raft invariants
✓ Single leader per term
✓ Log matching property
✓ Leader completeness
✓ State machine safety

// Linearizability checking
✓ Wing & Gong algorithm
✓ Automatic verification
✓ Client-centric correctness
```

---

## 📦 Quick Start

### Installation

```bash
git clone https://github.com/VaibhavBhagat665/faultline
cd faultline
go build ./...
```

### Run Your First Simulation

```bash
# Simple leader election demo
go run cmd/demo/main.go 42
```

**Output:**
```
=== Faultline Raft Election Demo ===
Seed: 42

Results:
  Leader:  n1
  Term:    4
  Events:  97
  Throughput: 161,156 events/sec

Network Stats:
  sent: 93
  dropped: 3
  delivered: 90

✅ Election successful
```

### Run Fault Scenarios

```bash
# Multiple fault profiles
go run cmd/demo-scenarios/main.go 42
```

**Output:**
```
╔═══════════════════════════════════════════════════╗
║   Faultline: Deterministic Raft Simulation       ║
╚═══════════════════════════════════════════════════╝

=== Scenario 1: Clean Network ===
Leader: n4, Term: 4, Dropped: 0 (0.0%)

=== Scenario 2: Light Faults (8%) ===
Leader: n1, Term: 4, Dropped: 3 (3.2%)

=== Scenario 3: Heavy Faults (15%) ===
Leader: n4, Term: 5, Dropped: 12 (10.4%)

=== Scenario 4: Extreme Faults (25%) ===
Leader: n5, Term: 5, Dropped: 18 (17.8%)
```

---

## 🏗️ Architecture

### System Overview

```
┌─────────────────────────────────────────────────────┐
│                  Your Raft Code                     │
│         (implements Leader Election & Replication)  │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│            Integration Test Framework               │
│  ┌────────────────────────────────────────────┐    │
│  │ Cluster Orchestration                      │    │
│  │  • Multi-node management                   │    │
│  │  • Fault injection API                     │    │
│  │  • Invariant checking                      │    │
│  └────────────────────────────────────────────┘    │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│          Deterministic Simulation Engine            │
│  ┌────────────────────────────────────────────┐    │
│  │ Event Scheduler                            │    │
│  │  • Priority queue (min-heap)               │    │
│  │  • Virtual time management                 │    │
│  │  • 50K-200K events/sec                     │    │
│  └────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────┐    │
│  │ Deterministic PRNG (splitmix64)            │    │
│  │  • Seeded random generation                │    │
│  │  • 100% reproducible                       │    │
│  └────────────────────────────────────────────┘    │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│              Simulated Environment                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │
│  │   Network    │  │     Disk     │  │  Timers  │ │
│  │              │  │              │  │          │ │
│  │ • Drops      │  │ • KV store   │  │ • Virtual│ │
│  │ • Delays     │  │ • Crashes    │  │ • Ordered│ │
│  │ • Partitions │  │ • fsync()    │  │ • Exact  │ │
│  └──────────────┘  └──────────────┘  └──────────┘ │
└─────────────────────────────────────────────────────┘
```

### Core Components

| Component | Location | Description |
|-----------|----------|-------------|
| **Simulation Engine** | `internal/sim/` | Priority queue scheduler, virtual time, PRNG |
| **Network Simulator** | `internal/simnet/` | Message drops, delays, partitions |
| **Disk Simulator** | `internal/simdisk/` | Persistent storage with crash semantics |
| **Raft Implementation** | `adapters/raft/` | Leader election, log replication |
| **Integration Framework** | `internal/integration/` | Cluster orchestration, fault injection |
| **Verification** | `internal/invariants/` | Safety properties, linearizability |

---

## 📚 Documentation

### Usage Example

```go
package main

import (
    "time"
    "github.com/VaibhavBhagat665/faultline/internal/integration"
)

func TestRaftElection() {
    // Create deterministic cluster (seed: 42)
    cluster := integration.NewCluster(42, []string{
        "n1", "n2", "n3", "n4", "n5",
    })
    cluster.Init()
    
    // Configure network faults
    cluster.SetMessageDropRate(0.08)  // 8% loss
    
    // Trigger election on node 1
    cluster.TriggerElection("n1")
    
    // Run simulation for 1 second (virtual time)
    cluster.RunFor(1*time.Second, 10000)
    
    // Verify results
    leader, term := cluster.GetLeader()
    if leader == "" {
        panic("No leader elected!")
    }
    
    // Check safety invariants
    if err := cluster.CheckInvariants(); err != nil {
        panic(err)
    }
    
    println("✅ Leader:", leader, "Term:", term)
}
```

### Project Structure

```
faultline/
├── adapters/
│   └── raft/                    # Raft consensus implementation
│       ├── node.go              # State machine, RPC handlers
│       ├── raft.go              # Core Raft logic
│       └── types.go             # Data structures
├── internal/
│   ├── sim/                     # Simulation engine core
│   │   ├── engine.go            # Event scheduler
│   │   ├── queue.go             # Priority queue
│   │   └── rng.go               # Deterministic PRNG
│   ├── simnet/                  # Network simulation
│   │   └── network.go           # Message drops, partitions
│   ├── simdisk/                 # Disk simulation
│   │   └── disk.go              # Crash/recovery semantics
│   ├── integration/             # Test framework
│   │   ├── cluster.go           # Cluster orchestration
│   │   └── *_test.go            # Integration tests
│   ├── invariants/              # Safety checkers
│   ├── lincheck/                # Linearizability verification
│   ├── history/                 # Event tracking
│   └── viz/                     # Visualization tools
├── cmd/
│   ├── demo/                    # Simple election demo
│   └── demo-scenarios/          # Multi-scenario demo
└── README.md
```

---

## 🧪 Testing

### Run Tests

```bash
# Core simulation tests
go test ./internal/sim/... -v

# Network simulation tests
go test ./internal/simnet/... -v

# Disk simulation tests  
go test ./internal/simdisk/... -v

# Raft implementation tests
go test ./adapters/raft/... -v

# Integration tests
go test ./internal/integration/... -short -v

# Full test suite
make test
```

### Verification Script

```bash
# Automated verification
./verify.ps1   # Windows
./verify.sh    # Linux/Mac
```

Verifies:
- ✅ Compilation
- ✅ Core simulation tests
- ✅ Network/disk tests
- ✅ Raft unit tests
- ✅ Determinism (same seed = same output)
- ✅ Demo execution

---

## 🎓 Key Concepts

### Deterministic Simulation

**Traditional Testing:**
```
Test Run 1: Bug appears
Test Run 2: Bug disappears (Heisenbug)
Test Run 3: Different bug appears
Result: 😭 Impossible to debug
```

**Faultline:**
```
Test Run 1 (seed=42): Bug at event 1337
Test Run 2 (seed=42): Bug at event 1337
Test Run 3 (seed=42): Bug at event 1337
Result: 🎯 Perfectly reproducible
```

### How It Works

1. **Virtual Time:** No wall-clock dependency
   ```
   Real time: variable, non-deterministic
   Virtual time: advances by events, fully controlled
   ```

2. **Seeded Randomness:** All randomness from seed
   ```go
   rng := NewRNG(seed)  // Deterministic source
   timeout := rng.Intn(300, 600)  // Always same value for same seed
   ```

3. **Event Ordering:** Priority queue guarantees order
   ```
   Event 1: t=100ms, Node n1 sends vote request
   Event 2: t=105ms, Node n2 receives vote request
   Event 3: t=110ms, Node n2 sends vote response
   ```

4. **Deterministic Drops:** Hash-based decisions
   ```go
   hash := hash64(seed, from, to, msgID)
   drop := (hash % 1000) < dropRate * 1000
   ```

---

## 📊 Performance

| Metric | Value |
|--------|-------|
| **Event Throughput** | 50,000 - 200,000 events/sec |
| **Simulation Speedup** | 1000x faster than real network |
| **Memory Usage** | O(events) ~10MB for typical test |
| **Election Time** | ~1ms (virtual: 300-600ms) |

### Benchmark Results

```bash
go test -bench=. ./internal/sim/

BenchmarkEngine_Schedule-8           5000000    247 ns/op
BenchmarkEngine_Process-8            2000000    618 ns/op
BenchmarkPriorityQueue_Push-8       10000000    142 ns/op
BenchmarkPriorityQueue_Pop-8        20000000     87 ns/op
BenchmarkRNG_Splitmix64-8          100000000     13 ns/op
```

---

## 🔬 Techniques & Inspiration

### Techniques Demonstrated

- ✅ **Event-driven simulation** - Discrete event simulation with priority queue
- ✅ **Deterministic testing** - Eliminating non-determinism for reproducibility
- ✅ **Virtual time** - Logical time advancement independent of wall clock
- ✅ **Fault injection** - Systematic exploration of failure scenarios
- ✅ **Invariant checking** - Automated verification of safety properties
- ✅ **Consensus algorithms** - Raft leader election and log replication

### Inspired By

- **[FoundationDB](https://www.youtube.com/watch?v=4fFDFbi3toc)** - Pioneering deterministic simulation testing
  > "Simulation testing found more bugs than 3 years of real-world usage"
  
- **[TigerBeetle](https://github.com/tigerbeetle/tigerbeetle)** - Financial database using simulation
  > "Every commit tested against trillions of scenarios"
  
- **[Antithesis](https://antithesis.com/)** - Autonomous testing platform
  > "Deterministic simulation as a commercial product"

---

## 🆚 Comparison

| Tool | Language | Deterministic | Speed | Real Network | Learning Curve |
|------|----------|---------------|-------|--------------|----------------|
| **Faultline** | Go | ✅ 100% | ⚡ Very Fast | ❌ Simulated | 📊 Medium |
| **Jepsen** | Clojure | ❌ No | 🐌 Slow | ✅ Docker | 📚 High |
| **FoundationDB** | C++ | ✅ 100% | ⚡ Very Fast | ❌ Simulated | 📚 Very High |
| **TLA+** | Spec | ✅ 100% | ⚡ Very Fast | ❌ Model | 📚 Very High |
| **Real Network Tests** | Any | ❌ No | 🐌 Very Slow | ✅ Real | 📊 Medium |

### Faultline's Sweet Spot

✅ **Use Faultline when you want:**
- Go-native testing (no Clojure, no TLA+)
- 100% reproducible bugs
- Fast iteration (1000x faster than real network)
- Comprehensive failure scenario exploration

❌ **Don't use Faultline for:**
- Testing non-Go implementations
- Validating real network behavior
- Multi-process integration tests
- General consensus algorithms (currently Raft-focused)

---

## 🛠️ Development

### Build & Test

```bash
# Build everything
make

# Run linter
make lint

# Run tests
make test

# Run with race detector
make race

# Run benchmarks
make bench

# Clean build artifacts
make clean
```

### Contributing

Contributions welcome! Areas of interest:
- 🎯 Additional consensus algorithms (Paxos, EPaxos)
- 📊 Visualization dashboard
- ⚡ Performance optimizations
- 🧪 More fault injection scenarios
- 📝 Documentation improvements

---

## 📖 References

### Academic Papers

- [In Search of an Understandable Consensus Algorithm (Raft)](https://raft.github.io/raft.pdf) - Diego Ongaro & John Ousterhout
- [Linearizability Testing](https://www.cs.ox.ac.uk/people/gavin.lowe/LinearizabiltyTesting/) - Gavin Lowe
- [Testing Linearizability with Wing & Gong](https://dl.acm.org/doi/10.1145/78969.78972) - J. Wing & C. Gong

### Production Systems

- [FoundationDB Deterministic Simulation](https://www.youtube.com/watch?v=4fFDFbi3toc) - Will Wilson (Apple)
- [TigerBeetle Simulator](https://github.com/tigerbeetle/tigerbeetle/tree/main/src/simulator)
- [Jepsen Testing](https://jepsen.io/) - Kyle Kingsbury

### Consensus Resources

- [Raft Visualization](https://raft.github.io/) - Interactive Raft visualization
- [The Raft Consensus Algorithm](https://raft.github.io/) - Official website
- [Designing Data-Intensive Applications](https://dataintensive.net/) - Martin Kleppmann

---

## 📄 License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

## 👨‍💻 Author

**Vaibhav Bhagat**

- GitHub: [@VaibhavBhagat665](https://github.com/VaibhavBhagat665)

---

<div align="center">

### ⭐ Star this repo if you find it useful!

**Built with ❤️ for correctness-obsessed distributed systems engineers**

[Report Bug](https://github.com/VaibhavBhagat665/faultline/issues) • [Request Feature](https://github.com/VaibhavBhagat665/faultline/issues)

</div>
