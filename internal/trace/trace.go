// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package trace

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"sync"
)

// Entry is a single structured trace entry (one line of JSONL).
type Entry struct {
	// Seq is the monotonically increasing sequence number of this entry.
	Seq uint64 `json:"seq"`

	// Time is the virtual time (nanoseconds) when this event occurred.
	Time int64 `json:"time"`

	// Event is the event type (e.g., "timer.fire", "msg.send", "node.crash").
	Event string `json:"event"`

	// Fields contains event-specific structured data.
	Fields map[string]interface{} `json:"fields,omitempty"`
}

// Trace records a structured trace of simulation events in JSONL format
// and computes a deterministic hash of the trace.
type Trace struct {
	mu     sync.Mutex
	writer io.Writer
	hasher hash.Hash
	seq    uint64
	buf    []byte // reusable buffer for JSON encoding
}

// New creates a new trace that writes to the given writer.
// If writer is nil, the trace is write-only (hash computation only).
func New(writer io.Writer) *Trace {
	return &Trace{
		writer: writer,
		hasher: sha256.New(),
		buf:    make([]byte, 0, 256),
	}
}

// Log writes a trace entry with the given event type and fields.
// Time is provided by the caller (the simulation clock).
func (t *Trace) Log(time int64, event string, fields map[string]interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.seq++
	entry := Entry{
		Seq:    t.seq,
		Time:   time,
		Event:  event,
		Fields: fields,
	}

	// Encode to JSON
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("trace: marshal entry: %w", err)
	}

	// Write to trace file (if writer is set)
	if t.writer != nil {
		if _, err := t.writer.Write(data); err != nil {
			return fmt.Errorf("trace: write: %w", err)
		}
		if _, err := t.writer.Write([]byte("\n")); err != nil {
			return fmt.Errorf("trace: write newline: %w", err)
		}
	}

	// Update hash
	t.hasher.Write(data)
	t.hasher.Write([]byte("\n"))

	return nil
}

// Hash returns the SHA-256 hash of all trace entries written so far.
// The hash is deterministic: the same sequence of Log calls produces the same hash.
func (t *Trace) Hash() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Compute hash of current state
	sum := t.hasher.Sum(nil)
	return hex.EncodeToString(sum)
}

// Seq returns the current sequence number (number of entries logged).
func (t *Trace) Seq() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seq
}

// Reset clears the trace and hash state. Used for testing.
func (t *Trace) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.seq = 0
	t.hasher = sha256.New()
}
