// SPDX-License-Identifier: Apache-2.0

// Package workload provides client workload generators for testing.
package workload

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/VaibhavBhagat665/faultline/internal/history"
	"github.com/VaibhavBhagat665/faultline/internal/sim"
)

// Operation represents a client operation type.
type Operation string

const (
	OpRead  Operation = "read"
	OpWrite Operation = "write"
	OpCAS   Operation = "cas"
)

// Command represents a client command.
type Command struct {
	Op    Operation `json:"op"`
	Key   string    `json:"key,omitempty"`
	Value int64     `json:"value,omitempty"`
	Old   int64     `json:"old,omitempty"` // for CAS
	New   int64     `json:"new,omitempty"` // for CAS
}

// Config holds workload configuration.
type Config struct {
	// Number of client operations to generate
	NumOps int

	// Client ID
	ClientID uint64

	// Operation mix (probabilities, must sum to 1.0)
	ReadPct  float64
	WritePct float64
	CASPct   float64

	// Key space size
	NumKeys int

	// Inter-operation delay range
	MinDelay time.Duration
	MaxDelay time.Duration

	// Request timeout
	Timeout time.Duration
}

// DefaultConfig returns a reasonable default workload configuration.
func DefaultConfig() Config {
	return Config{
		NumOps:   100,
		ClientID: 1,
		ReadPct:  0.5,
		WritePct: 0.4,
		CASPct:   0.1,
		NumKeys:  10,
		MinDelay: 10 * time.Millisecond,
		MaxDelay: 50 * time.Millisecond,
		Timeout:  1 * time.Second,
	}
}

// Generator generates client operations.
type Generator struct {
	cfg      Config
	rng      *rand.Rand
	hist     *history.History
	opsSent  int
	seq      uint64
	nextReqID int
}

// NewGenerator creates a new workload generator.
func NewGenerator(cfg Config, rng *rand.Rand) *Generator {
	return &Generator{
		cfg:      cfg,
		rng:      rng,
		hist:     history.NewHistory(),
		seq:      0,
		nextReqID: 1,
	}
}

// Next generates the next operation.
// Returns nil when the workload is complete.
func (g *Generator) Next() *Command {
	if g.opsSent >= g.cfg.NumOps {
		return nil
	}
	g.opsSent++
	g.seq++

	// Choose operation type
	p := g.rng.Float64()
	if p < g.cfg.ReadPct {
		return g.generateRead()
	} else if p < g.cfg.ReadPct+g.cfg.WritePct {
		return g.generateWrite()
	} else {
		return g.generateCAS()
	}
}

// generateRead generates a read operation.
func (g *Generator) generateRead() *Command {
	key := fmt.Sprintf("key%d", g.rng.Intn(g.cfg.NumKeys))
	return &Command{
		Op:  OpRead,
		Key: key,
	}
}

// generateWrite generates a write operation.
func (g *Generator) generateWrite() *Command {
	key := fmt.Sprintf("key%d", g.rng.Intn(g.cfg.NumKeys))
	value := g.rng.Int63()
	return &Command{
		Op:    OpWrite,
		Key:   key,
		Value: value,
	}
}

// generateCAS generates a compare-and-swap operation.
func (g *Generator) generateCAS() *Command {
	key := fmt.Sprintf("key%d", g.rng.Intn(g.cfg.NumKeys))
	old := g.rng.Int63()
	new := g.rng.Int63()
	return &Command{
		Op:  OpCAS,
		Key: key,
		Old: old,
		New: new,
	}
}

// Delay returns a random delay before the next operation.
func (g *Generator) Delay() time.Duration {
	minMs := g.cfg.MinDelay.Milliseconds()
	maxMs := g.cfg.MaxDelay.Milliseconds()
	delayMs := minMs + g.rng.Int63n(maxMs-minMs+1)
	return time.Duration(delayMs) * time.Millisecond
}

// RecordInvoke records an invocation in the history.
func (g *Generator) RecordInvoke(time sim.Time, cmd *Command) int {
	reqID := g.nextReqID
	g.nextReqID++
	
	// Convert command to history.Operation
	var opType history.OpType
	switch cmd.Op {
	case OpRead:
		opType = history.OpRead
	case OpWrite:
		opType = history.OpWrite
	case OpCAS:
		opType = history.OpCAS
	}
	
	event := history.Event{
		Type:      history.EventInvoke,
		ClientID:  int(g.cfg.ClientID),
		RequestID: reqID,
		Time:      int64(time),
		Op: history.Operation{
			Type:  opType,
			Key:   cmd.Key,
			Input: cmd,
		},
	}
	g.hist.Append(event)
	return reqID
}

// RecordReturn records a return (success) in the history.
func (g *Generator) RecordReturn(reqID int, time sim.Time, result []byte) {
	event := history.Event{
		Type:      history.EventOk,
		ClientID:  int(g.cfg.ClientID),
		RequestID: reqID,
		Time:      int64(time),
		Op: history.Operation{
			Output: result,
		},
	}
	g.hist.Append(event)
}

// RecordFail records a failure in the history.
func (g *Generator) RecordFail(reqID int, time sim.Time) {
	event := history.Event{
		Type:      history.EventFail,
		ClientID:  int(g.cfg.ClientID),
		RequestID: reqID,
		Time:      int64(time),
	}
	g.hist.Append(event)
}

// RecordInfo records an indeterminate outcome in the history.
func (g *Generator) RecordInfo(reqID int, time sim.Time) {
	event := history.Event{
		Type:      history.EventInfo,
		ClientID:  int(g.cfg.ClientID),
		RequestID: reqID,
		Time:      int64(time),
	}
	g.hist.Append(event)
}

// History returns the accumulated history.
func (g *Generator) History() *history.History {
	return g.hist
}

// ClientID returns the client ID.
func (g *Generator) ClientID() uint64 {
	return g.cfg.ClientID
}

// Seq returns the current sequence number.
func (g *Generator) Seq() uint64 {
	return g.seq
}
