// SPDX-License-Identifier: Apache-2.0

package simdisk

import (
	"encoding/json"
	"errors"
)

// KVStore provides a key-value store on top of Disk.
type KVStore struct {
	disk *Disk
	data map[string][]byte
}

// NewKVStore creates a new key-value store.
func NewKVStore(disk *Disk) *KVStore {
	return &KVStore{
		disk: disk,
		data: make(map[string][]byte),
	}
}

// Write sets the value for a key in the volatile cache.
func (kv *KVStore) Write(key string, value []byte) error {
	kv.data[key] = value
	return nil
}

// Read retrieves the value for a key from durable storage.
func (kv *KVStore) Read(key string) ([]byte, error) {
	// Try to read from disk (durable storage)
	buf := make([]byte, kv.disk.Size())
	n, err := kv.disk.Read(0, buf)
	if err != nil && !errors.Is(err, ErrReadBeyond) {
		return nil, err
	}
	
	if n == 0 {
		return nil, errors.New("key not found")
	}
	
	// Deserialize the data map
	var storedData map[string][]byte
	if err := json.Unmarshal(buf[:n], &storedData); err != nil {
		return nil, errors.New("key not found")
	}
	
	if value, ok := storedData[key]; ok {
		return value, nil
	}
	
	return nil, errors.New("key not found")
}

// Sync flushes the volatile cache to durable storage.
func (kv *KVStore) Sync() error {
	// Serialize the data map
	encoded, err := json.Marshal(kv.data)
	if err != nil {
		return err
	}
	
	// Write to disk at offset 0
	if err := kv.disk.Write(0, encoded); err != nil {
		return err
	}
	
	// Sync to make it durable
	return kv.disk.Sync()
}

// Crash simulates a crash.
func (kv *KVStore) Crash() {
	kv.disk.Crash()
	kv.data = make(map[string][]byte)
}
