// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package simdisk

import (
	"errors"
	"fmt"
	"math/rand/v2"
)

// Common errors
var (
	ErrFsyncFailed = errors.New("simdisk: fsync failed")
	ErrWriteFailed = errors.New("simdisk: write failed")
	ErrNoSpace     = errors.New("simdisk: no space left on device")
	ErrReadBeyond  = errors.New("simdisk: read beyond end of file")
)

// Disk simulates a persistent storage device with crash semantics.
// It models a volatile write cache and a durable image.
//
// Write model:
//   - Write() adds data to the volatile cache
//   - Sync() flushes cache to durable storage (may fail)
//   - On crash, unsynced writes may be lost, partially applied, or torn
//
// This models real disk behavior: writes are buffered, fsync flushes buffers,
// and crashes can lose or corrupt unsynced data.
type Disk struct {
	// durable is the committed, crash-safe state
	durable []byte

	// cache contains writes since the last successful Sync()
	cache map[int64][]byte

	// faults tracks active fault injection
	faults FaultState

	// rng for probabilistic fault injection
	rng *rand.Rand

	// config
	cfg DiskConfig
}

// DiskConfig configures disk behavior and faults.
type DiskConfig struct {
	// InitialSize is the initial size of the disk in bytes.
	InitialSize int64

	// MaxSize is the maximum size (for ENOSPC simulation). 0 = unlimited.
	MaxSize int64

	// AtomicityGranularity is the size of atomic write units (e.g., 512 bytes for sectors, 4096 for pages).
	// On crash, partial writes within a granule may be torn.
	AtomicityGranularity int64

	// RNG for probabilistic faults.
	RNG *rand.Rand
}

// FaultState tracks active disk faults.
type FaultState struct {
	// FsyncFailureRate is the probability [0, 1] that Sync() fails.
	FsyncFailureRate float64

	// WriteFailureRate is the probability [0, 1] that Write() fails.
	WriteFailureRate float64

	// CrashTearRate is the probability [0, 1] that an unsynced write is partially applied (torn) on crash.
	CrashTearRate float64

	// CrashDropRate is the probability [0, 1] that an unsynced write is completely dropped on crash.
	CrashDropRate float64
}

// NewDisk creates a new simulated disk.
func NewDisk(cfg DiskConfig) *Disk {
	if cfg.RNG == nil {
		panic("simdisk: RNG is required")
	}
	if cfg.AtomicityGranularity <= 0 {
		cfg.AtomicityGranularity = 512 // default: 512-byte sectors
	}

	return &Disk{
		durable: make([]byte, cfg.InitialSize),
		cache:   make(map[int64][]byte),
		rng:     cfg.RNG,
		cfg:     cfg,
	}
}

// Write writes data at the given offset into the volatile cache.
// Does not persist until Sync() is called.
func (d *Disk) Write(offset int64, data []byte) error {
	// Check write failure fault
	if d.faults.WriteFailureRate > 0 && d.rng.Float64() < d.faults.WriteFailureRate {
		return ErrWriteFailed
	}

	// Check space (if limited)
	requiredSize := offset + int64(len(data))
	if d.cfg.MaxSize > 0 && requiredSize > d.cfg.MaxSize {
		return ErrNoSpace
	}

	// Expand durable if needed (this models OS-level file extension)
	if requiredSize > int64(len(d.durable)) {
		newSize := requiredSize
		if d.cfg.MaxSize > 0 && newSize > d.cfg.MaxSize {
			newSize = d.cfg.MaxSize
		}
		d.durable = append(d.durable, make([]byte, newSize-int64(len(d.durable)))...)
	}

	// Write to cache
	cacheCopy := make([]byte, len(data))
	copy(cacheCopy, data)
	d.cache[offset] = cacheCopy

	return nil
}

// Read reads data from the durable image (not the cache).
// This models recovery: after a crash, only synced data is visible.
func (d *Disk) Read(offset int64, buf []byte) (int, error) {
	if offset >= int64(len(d.durable)) {
		return 0, ErrReadBeyond
	}

	n := copy(buf, d.durable[offset:])
	return n, nil
}

// Sync flushes the volatile cache to durable storage.
// May fail with ErrFsyncFailed (injected fault).
// On success, all cached writes become durable.
func (d *Disk) Sync() error {
	// Check fsync failure fault
	if d.faults.FsyncFailureRate > 0 && d.rng.Float64() < d.faults.FsyncFailureRate {
		// fsyncgate semantics: after a failed fsync, cached data may be silently dropped.
		// A subsequent Sync() may succeed even though earlier data was lost.
		// For now, we keep the cache (lenient), but a stricter model could drop it here.
		return ErrFsyncFailed
	}

	// Commit cache to durable
	for offset, data := range d.cache {
		copy(d.durable[offset:], data)
	}

	// Clear cache
	d.cache = make(map[int64][]byte)

	return nil
}

// Size returns the current size of the durable image.
func (d *Disk) Size() int64 {
	return int64(len(d.durable))
}

// Crash simulates a crash, applying unsynced writes according to crash faults.
// Unsynced writes may be:
//   - Fully persisted (lucky)
//   - Fully dropped (unlucky)
//   - Partially persisted (torn write)
func (d *Disk) Crash() {
	for offset, data := range d.cache {
		// Decide fate of this write
		rval := d.rng.Float64()

		if rval < d.faults.CrashDropRate {
			// Drop the write entirely
			continue
		}

		if rval < d.faults.CrashDropRate+d.faults.CrashTearRate {
			// Torn write: persist partial data, respecting atomicity granularity
			granule := d.cfg.AtomicityGranularity
			for i := int64(0); i < int64(len(data)); i += granule {
				// Flip a coin for each granule
				if d.rng.IntN(2) == 0 {
					end := i + granule
					if end > int64(len(data)) {
						end = int64(len(data))
					}
					copy(d.durable[offset+i:], data[i:end])
				}
			}
			continue
		}

		// Persist the full write (lucky case)
		copy(d.durable[offset:], data)
	}

	// Clear cache
	d.cache = make(map[int64][]byte)
}

// CachedWrites returns the number of writes in the volatile cache (not yet synced).
func (d *Disk) CachedWrites() int {
	return len(d.cache)
}

// SetFsyncFailureRate sets the probability [0, 1] that Sync() fails.
func (d *Disk) SetFsyncFailureRate(rate float64) {
	if rate < 0 || rate > 1 {
		panic(fmt.Sprintf("simdisk: fsync failure rate must be in [0, 1], got %f", rate))
	}
	d.faults.FsyncFailureRate = rate
}

// SetWriteFailureRate sets the probability [0, 1] that Write() fails.
func (d *Disk) SetWriteFailureRate(rate float64) {
	if rate < 0 || rate > 1 {
		panic(fmt.Sprintf("simdisk: write failure rate must be in [0, 1], got %f", rate))
	}
	d.faults.WriteFailureRate = rate
}

// SetCrashTearRate sets the probability [0, 1] that an unsynced write is torn on crash.
func (d *Disk) SetCrashTearRate(rate float64) {
	if rate < 0 || rate > 1 {
		panic(fmt.Sprintf("simdisk: crash tear rate must be in [0, 1], got %f", rate))
	}
	d.faults.CrashTearRate = rate
}

// SetCrashDropRate sets the probability [0, 1] that an unsynced write is dropped on crash.
func (d *Disk) SetCrashDropRate(rate float64) {
	if rate < 0 || rate > 1 {
		panic(fmt.Sprintf("simdisk: crash drop rate must be in [0, 1], got %f", rate))
	}
	d.faults.CrashDropRate = rate
}

// ClearFaults removes all active faults.
func (d *Disk) ClearFaults() {
	d.faults = FaultState{}
}

// Snapshot returns a copy of the durable image (for testing).
func (d *Disk) Snapshot() []byte {
	snap := make([]byte, len(d.durable))
	copy(snap, d.durable)
	return snap
}
