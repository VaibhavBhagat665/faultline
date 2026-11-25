// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package trace

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestTrace_BasicLogging(t *testing.T) {
	var buf bytes.Buffer
	tr := New(&buf)

	err := tr.Log(1000, "test.event", map[string]interface{}{
		"node": 1,
		"msg":  "hello",
	})
	if err != nil {
		t.Fatalf("Log failed: %v", err)
	}

	if tr.Seq() != 1 {
		t.Errorf("expected seq=1, got %d", tr.Seq())
	}

	// Parse the JSONL output
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}

	var entry Entry
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if entry.Seq != 1 {
		t.Errorf("expected seq=1, got %d", entry.Seq)
	}
	if entry.Time != 1000 {
		t.Errorf("expected time=1000, got %d", entry.Time)
	}
	if entry.Event != "test.event" {
		t.Errorf("expected event=test.event, got %s", entry.Event)
	}
	if entry.Fields["node"] != float64(1) { // JSON unmarshals numbers as float64
		t.Errorf("expected node=1, got %v", entry.Fields["node"])
	}
	if entry.Fields["msg"] != "hello" {
		t.Errorf("expected msg=hello, got %v", entry.Fields["msg"])
	}
}

func TestTrace_SequenceNumbering(t *testing.T) {
	tr := New(nil) // no writer, hash only

	for i := 1; i <= 10; i++ {
		if err := tr.Log(int64(i*100), "event", nil); err != nil {
			t.Fatalf("Log %d failed: %v", i, err)
		}
		if tr.Seq() != uint64(i) {
			t.Errorf("after %d logs, expected seq=%d, got %d", i, i, tr.Seq())
		}
	}
}

func TestTrace_HashDeterminism(t *testing.T) {
	// Same sequence of logs should produce the same hash
	hash1 := computeHash(t, []logEntry{
		{100, "event1", map[string]interface{}{"x": 1}},
		{200, "event2", map[string]interface{}{"x": 2}},
		{300, "event3", map[string]interface{}{"x": 3}},
	})

	hash2 := computeHash(t, []logEntry{
		{100, "event1", map[string]interface{}{"x": 1}},
		{200, "event2", map[string]interface{}{"x": 2}},
		{300, "event3", map[string]interface{}{"x": 3}},
	})

	if hash1 != hash2 {
		t.Errorf("same logs should produce same hash:\n  %s\n  %s", hash1, hash2)
	}
}

func TestTrace_HashSensitivity(t *testing.T) {
	// Different logs should produce different hashes

	base := []logEntry{
		{100, "event1", map[string]interface{}{"x": 1}},
		{200, "event2", map[string]interface{}{"x": 2}},
	}

	baseHash := computeHash(t, base)

	// Different time
	diffTimeHash := computeHash(t, []logEntry{
		{101, "event1", map[string]interface{}{"x": 1}},
		{200, "event2", map[string]interface{}{"x": 2}},
	})
	if baseHash == diffTimeHash {
		t.Error("different time should produce different hash")
	}

	// Different event
	diffEventHash := computeHash(t, []logEntry{
		{100, "event1.modified", map[string]interface{}{"x": 1}},
		{200, "event2", map[string]interface{}{"x": 2}},
	})
	if baseHash == diffEventHash {
		t.Error("different event should produce different hash")
	}

	// Different fields
	diffFieldsHash := computeHash(t, []logEntry{
		{100, "event1", map[string]interface{}{"x": 99}},
		{200, "event2", map[string]interface{}{"x": 2}},
	})
	if baseHash == diffFieldsHash {
		t.Error("different fields should produce different hash")
	}

	// Different order
	diffOrderHash := computeHash(t, []logEntry{
		{200, "event2", map[string]interface{}{"x": 2}},
		{100, "event1", map[string]interface{}{"x": 1}},
	})
	if baseHash == diffOrderHash {
		t.Error("different order should produce different hash")
	}

	// Extra event
	extraEventHash := computeHash(t, []logEntry{
		{100, "event1", map[string]interface{}{"x": 1}},
		{200, "event2", map[string]interface{}{"x": 2}},
		{300, "event3", map[string]interface{}{"x": 3}},
	})
	if baseHash == extraEventHash {
		t.Error("extra event should produce different hash")
	}
}

func TestTrace_EmptyFields(t *testing.T) {
	var buf bytes.Buffer
	tr := New(&buf)

	if err := tr.Log(500, "event.no.fields", nil); err != nil {
		t.Fatalf("Log with nil fields failed: %v", err)
	}

	var entry Entry
	line := strings.TrimSpace(buf.String())
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if entry.Fields != nil {
		t.Errorf("expected nil fields, got %v", entry.Fields)
	}
}

func TestTrace_Reset(t *testing.T) {
	tr := New(nil)

	tr.Log(100, "event1", nil)
	tr.Log(200, "event2", nil)

	hash1 := tr.Hash()
	if tr.Seq() != 2 {
		t.Errorf("expected seq=2 before reset, got %d", tr.Seq())
	}

	tr.Reset()

	if tr.Seq() != 0 {
		t.Errorf("expected seq=0 after reset, got %d", tr.Seq())
	}

	tr.Log(100, "event1", nil)
	tr.Log(200, "event2", nil)

	hash2 := tr.Hash()

	if hash1 != hash2 {
		t.Error("same logs after reset should produce same hash")
	}
}

// Helper types and functions

type logEntry struct {
	time   int64
	event  string
	fields map[string]interface{}
}

func computeHash(t *testing.T, entries []logEntry) string {
	t.Helper()
	tr := New(nil)
	for _, e := range entries {
		if err := tr.Log(e.time, e.event, e.fields); err != nil {
			t.Fatalf("Log failed: %v", err)
		}
	}
	return tr.Hash()
}

func BenchmarkTrace_Log(b *testing.B) {
	tr := New(nil) // no I/O, hash only
	fields := map[string]interface{}{
		"node": 1,
		"msg":  "benchmark",
		"seq":  42,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tr.Log(int64(i), "bench.event", fields); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTrace_LogWithWriter(b *testing.B) {
	var buf bytes.Buffer
	tr := New(&buf)
	fields := map[string]interface{}{
		"node": 1,
		"msg":  "benchmark",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tr.Log(int64(i), "bench.event", fields); err != nil {
			b.Fatal(err)
		}
	}
}
