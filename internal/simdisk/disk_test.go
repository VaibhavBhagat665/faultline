// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package simdisk

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

func TestDisk_BasicWriteReadSync(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	cfg := DiskConfig{
		InitialSize:          1024,
		AtomicityGranularity: 512,
		RNG:                  rng,
	}

	disk := NewDisk(cfg)

	// Write some data
	data := []byte("hello world")
	if err := disk.Write(0, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Data should be in cache, not durable yet
	if disk.CachedWrites() != 1 {
		t.Errorf("expected 1 cached write, got %d", disk.CachedWrites())
	}

	// Read before sync should return zeros (old durable state)
	buf := make([]byte, len(data))
	n, err := disk.Read(0, buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected to read %d bytes, got %d", len(data), n)
	}
	if !bytes.Equal(buf, make([]byte, len(data))) {
		t.Errorf("expected zeros before sync, got %q", buf)
	}

	// Sync
	if err := disk.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Cache should be clear
	if disk.CachedWrites() != 0 {
		t.Errorf("expected 0 cached writes after sync, got %d", disk.CachedWrites())
	}

	// Read after sync should return the written data
	buf = make([]byte, len(data))
	n, err = disk.Read(0, buf)
	if err != nil {
		t.Fatalf("Read after sync failed: %v", err)
	}
	if !bytes.Equal(buf, data) {
		t.Errorf("expected %q after sync, got %q", data, buf)
	}
}

func TestDisk_FsyncFailure(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 43))
	cfg := DiskConfig{
		InitialSize: 1024,
		RNG:         rng,
	}

	disk := NewDisk(cfg)
	disk.SetFsyncFailureRate(1.0) // Always fail

	// Write
	if err := disk.Write(0, []byte("data")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Sync should fail
	if err := disk.Sync(); err != ErrFsyncFailed {
		t.Errorf("expected ErrFsyncFailed, got %v", err)
	}

	// Data should still be in cache (fsyncgate: may or may not be dropped)
	// Our implementation keeps it
	if disk.CachedWrites() != 1 {
		t.Errorf("expected cache to remain after failed fsync, got %d writes", disk.CachedWrites())
	}

	// Read should still return old (empty) data
	buf := make([]byte, 4)
	disk.Read(0, buf)
	if !bytes.Equal(buf, make([]byte, 4)) {
		t.Errorf("durable should still be empty after failed fsync, got %q", buf)
	}
}

func TestDisk_WriteFailure(t *testing.T) {
	rng := rand.New(rand.NewPCG(99, 100))
	cfg := DiskConfig{
		InitialSize: 1024,
		RNG:         rng,
	}

	disk := NewDisk(cfg)
	disk.SetWriteFailureRate(1.0) // Always fail

	// Write should fail
	if err := disk.Write(0, []byte("data")); err != ErrWriteFailed {
		t.Errorf("expected ErrWriteFailed, got %v", err)
	}

	// Nothing should be cached
	if disk.CachedWrites() != 0 {
		t.Error("failed write should not add to cache")
	}
}

func TestDisk_NoSpace(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	cfg := DiskConfig{
		InitialSize: 100,
		MaxSize:     200,
		RNG:         rng,
	}

	disk := NewDisk(cfg)

	// Write within limit
	if err := disk.Write(0, make([]byte, 150)); err != nil {
		t.Fatalf("Write within limit failed: %v", err)
	}

	// Write beyond limit
	if err := disk.Write(0, make([]byte, 300)); err != ErrNoSpace {
		t.Errorf("expected ErrNoSpace, got %v", err)
	}
}

func TestDisk_CrashDropsUnsyncedData(t *testing.T) {
	rng := rand.New(rand.NewPCG(77, 88))
	cfg := DiskConfig{
		InitialSize: 1024,
		RNG:         rng,
	}

	disk := NewDisk(cfg)
	disk.SetCrashDropRate(1.0) // Always drop unsynced writes on crash

	// Write data
	data := []byte("lost forever")
	if err := disk.Write(0, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Crash before sync
	disk.Crash()

	// Cache should be cleared
	if disk.CachedWrites() != 0 {
		t.Error("cache should be cleared after crash")
	}

	// Data should not be durable
	buf := make([]byte, len(data))
	disk.Read(0, buf)
	if !bytes.Equal(buf, make([]byte, len(data))) {
		t.Errorf("crashed data should be lost, got %q", buf)
	}
}

func TestDisk_CrashPersistsUnsyncedData(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 22))
	cfg := DiskConfig{
		InitialSize: 1024,
		RNG:         rng,
	}

	disk := NewDisk(cfg)
	disk.SetCrashDropRate(0.0)  // Never drop
	disk.SetCrashTearRate(0.0) // Never tear

	// Write data
	data := []byte("lucky persistence")
	if err := disk.Write(0, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Crash before sync
	disk.Crash()

	// Data should be persisted (lucky)
	buf := make([]byte, len(data))
	disk.Read(0, buf)
	if !bytes.Equal(buf, data) {
		t.Errorf("expected %q after lucky crash, got %q", data, buf)
	}
}

func TestDisk_CrashTornWrites(t *testing.T) {
	rng := rand.New(rand.NewPCG(33, 44))
	cfg := DiskConfig{
		InitialSize:          1024,
		AtomicityGranularity: 4, // Small granule for testing
		RNG:                  rng,
	}

	disk := NewDisk(cfg)
	disk.SetCrashTearRate(1.0) // Always tear
	disk.SetCrashDropRate(0.0) // Never drop entirely

	// Write data: 16 bytes (4 granules of 4 bytes each)
	data := []byte("0123456789ABCDEF")
	if err := disk.Write(0, data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Crash
	disk.Crash()

	// Read back
	buf := make([]byte, len(data))
	disk.Read(0, buf)

	// Should be a mix of written and zeros (torn)
	allZero := bytes.Equal(buf, make([]byte, len(data)))
	allData := bytes.Equal(buf, data)

	if allZero {
		t.Error("torn write should persist some granules, got all zeros")
	}
	if allData {
		t.Error("torn write should drop some granules, got all data")
	}

	t.Logf("Torn write result: %q (original: %q)", buf, data)
}

func TestDisk_MultipleCachedWrites(t *testing.T) {
	rng := rand.New(rand.NewPCG(55, 66))
	cfg := DiskConfig{
		InitialSize: 1024,
		RNG:         rng,
	}

	disk := NewDisk(cfg)

	// Write at different offsets
	disk.Write(0, []byte("aaa"))
	disk.Write(100, []byte("bbb"))
	disk.Write(200, []byte("ccc"))

	if disk.CachedWrites() != 3 {
		t.Errorf("expected 3 cached writes, got %d", disk.CachedWrites())
	}

	// Sync
	if err := disk.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// All should be durable
	tests := []struct {
		offset int64
		expect []byte
	}{
		{0, []byte("aaa")},
		{100, []byte("bbb")},
		{200, []byte("ccc")},
	}

	for _, tt := range tests {
		buf := make([]byte, len(tt.expect))
		disk.Read(tt.offset, buf)
		if !bytes.Equal(buf, tt.expect) {
			t.Errorf("offset %d: expected %q, got %q", tt.offset, tt.expect, buf)
		}
	}
}

func TestDisk_Expansion(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	cfg := DiskConfig{
		InitialSize: 100,
		RNG:         rng,
	}

	disk := NewDisk(cfg)

	if disk.Size() != 100 {
		t.Errorf("initial size should be 100, got %d", disk.Size())
	}

	// Write beyond initial size
	if err := disk.Write(500, []byte("expand")); err != nil {
		t.Fatalf("Write for expansion failed: %v", err)
	}

	// Size should have grown
	if disk.Size() <= 500 {
		t.Errorf("disk should have expanded beyond 500, got %d", disk.Size())
	}

	// Sync and read
	disk.Sync()
	buf := make([]byte, 6)
	disk.Read(500, buf)
	if !bytes.Equal(buf, []byte("expand")) {
		t.Errorf("expanded write failed: got %q", buf)
	}
}

func TestDisk_ClearFaults(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	cfg := DiskConfig{
		InitialSize: 1024,
		RNG:         rng,
	}

	disk := NewDisk(cfg)

	// Set all faults
	disk.SetFsyncFailureRate(0.5)
	disk.SetWriteFailureRate(0.5)
	disk.SetCrashDropRate(0.5)
	disk.SetCrashTearRate(0.5)

	// Clear
	disk.ClearFaults()

	// All rates should be 0
	if disk.faults.FsyncFailureRate != 0 {
		t.Error("fsync failure rate should be 0")
	}
	if disk.faults.WriteFailureRate != 0 {
		t.Error("write failure rate should be 0")
	}
	if disk.faults.CrashDropRate != 0 {
		t.Error("crash drop rate should be 0")
	}
	if disk.faults.CrashTearRate != 0 {
		t.Error("crash tear rate should be 0")
	}

	// Operations should succeed
	if err := disk.Write(0, []byte("ok")); err != nil {
		t.Errorf("write should succeed after clear: %v", err)
	}
	if err := disk.Sync(); err != nil {
		t.Errorf("sync should succeed after clear: %v", err)
	}
}

func TestDisk_Snapshot(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	cfg := DiskConfig{
		InitialSize: 1024,
		RNG:         rng,
	}

	disk := NewDisk(cfg)

	data := []byte("snapshot test")
	disk.Write(0, data)
	disk.Sync()

	snap := disk.Snapshot()

	if !bytes.Equal(snap[:len(data)], data) {
		t.Errorf("snapshot mismatch: expected %q, got %q", data, snap[:len(data)])
	}

	// Modify disk
	disk.Write(0, []byte("modified"))
	disk.Sync()

	// Snapshot should be unchanged
	if !bytes.Equal(snap[:len(data)], data) {
		t.Error("snapshot was not a copy")
	}
}
