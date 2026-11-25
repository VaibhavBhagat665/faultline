// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package lincheck

import (
	"fmt"

	"github.com/VaibhavBhagat665/faultline/internal/history"
)

// Model is a sequential specification of a data structure.
// The checker uses the model to determine valid sequential executions.
type Model interface {
	// Init returns the initial state of the model.
	Init() State

	// Step applies an operation to a state and returns the new state and output.
	// Must be deterministic and side-effect-free.
	Step(state State, input history.Operation) (State, interface{}, error)

	// Equal returns true if two states are equivalent.
	Equal(s1, s2 State) bool

	// Hash returns a hash of the state (for memoization).
	Hash(state State) uint64
}

// State represents the state of the sequential model.
// Implementations should be immutable or copy-on-write.
type State interface{}

// RegisterModel is a single-register model supporting read and write.
type RegisterModel struct{}

// NewRegisterModel creates a register model.
func NewRegisterModel() *RegisterModel {
	return &RegisterModel{}
}

func (m *RegisterModel) Init() State {
	return nil // initial value is nil
}

func (m *RegisterModel) Step(state State, input history.Operation) (State, interface{}, error) {
	switch input.Type {
	case history.OpRead:
		return state, state, nil

	case history.OpWrite:
		return input.Input, nil, nil

	default:
		return state, nil, fmt.Errorf("register: unsupported operation %s", input.Type)
	}
}

func (m *RegisterModel) Equal(s1, s2 State) bool {
	return s1 == s2
}

func (m *RegisterModel) Hash(state State) uint64 {
	// Simple hash: treat state as int64
	if state == nil {
		return 0
	}
	if v, ok := state.(int); ok {
		return uint64(v)
	}
	if v, ok := state.(int64); ok {
		return uint64(v)
	}
	// Fallback: use fmt.Sprint
	return hashString(fmt.Sprint(state))
}

// KVModel is a key-value map supporting read and write.
type KVModel struct{}

// NewKVModel creates a KV model.
func NewKVModel() *KVModel {
	return &KVModel{}
}

type kvState map[string]interface{}

func (m *KVModel) Init() State {
	return make(kvState)
}

func (m *KVModel) Step(state State, input history.Operation) (State, interface{}, error) {
	kv := state.(kvState)

	switch input.Type {
	case history.OpRead:
		val, ok := kv[input.Key]
		if !ok {
			val = nil
		}
		return kv, val, nil

	case history.OpWrite:
		// Copy-on-write
		newKV := make(kvState, len(kv)+1)
		for k, v := range kv {
			newKV[k] = v
		}
		newKV[input.Key] = input.Input
		return newKV, nil, nil

	default:
		return state, nil, fmt.Errorf("kv: unsupported operation %s", input.Type)
	}
}

func (m *KVModel) Equal(s1, s2 State) bool {
	kv1 := s1.(kvState)
	kv2 := s2.(kvState)

	if len(kv1) != len(kv2) {
		return false
	}

	for k, v1 := range kv1 {
		v2, ok := kv2[k]
		if !ok || v1 != v2 {
			return false
		}
	}

	return true
}

func (m *KVModel) Hash(state State) uint64 {
	kv := state.(kvState)
	h := uint64(0)

	// XOR hash of all key-value pairs
	for k, v := range kv {
		h ^= hashString(k)
		h ^= hashString(fmt.Sprint(v))
	}

	return h
}

// CASModel is a compare-and-swap register.
type CASModel struct{}

// NewCASModel creates a CAS model.
func NewCASModel() *CASModel {
	return &CASModel{}
}

// CASInput represents the input to a CAS operation.
type CASInput struct {
	Old interface{}
	New interface{}
}

func (m *CASModel) Init() State {
	return nil
}

func (m *CASModel) Step(state State, input history.Operation) (State, interface{}, error) {
	switch input.Type {
	case history.OpRead:
		return state, state, nil

	case history.OpCAS:
		casInput, ok := input.Input.(CASInput)
		if !ok {
			return state, false, fmt.Errorf("cas: invalid input type")
		}

		if state == casInput.Old {
			// CAS succeeds
			return casInput.New, true, nil
		}
		// CAS fails
		return state, false, nil

	default:
		return state, nil, fmt.Errorf("cas: unsupported operation %s", input.Type)
	}
}

func (m *CASModel) Equal(s1, s2 State) bool {
	return s1 == s2
}

func (m *CASModel) Hash(state State) uint64 {
	if state == nil {
		return 0
	}
	return hashString(fmt.Sprint(state))
}

// hashString computes a simple hash of a string.
func hashString(s string) uint64 {
	h := uint64(0)
	for i := 0; i < len(s); i++ {
		h = h*31 + uint64(s[i])
	}
	return h
}
