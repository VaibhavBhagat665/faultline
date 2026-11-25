// SPDX-License-Identifier: Apache-2.0

package raft

import "encoding/json"

// RPCType identifies the type of RPC message.
type RPCType string

const (
	RPCRequestVote         RPCType = "RequestVote"
	RPCRequestVoteReply    RPCType = "RequestVoteReply"
	RPCAppendEntries       RPCType = "AppendEntries"
	RPCAppendEntriesReply  RPCType = "AppendEntriesReply"
	RPCInstallSnapshot     RPCType = "InstallSnapshot"
	RPCInstallSnapshotReply RPCType = "InstallSnapshotReply"
)

// Message is the top-level envelope for all RPC messages.
type Message struct {
	Type    RPCType         `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// RequestVoteArgs is sent by candidates during election.
type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  NodeID `json:"candidateId"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

// AppendEntriesArgs is sent by leader for log replication and heartbeats.
type AppendEntriesArgs struct {
	Term         uint64     `json:"term"`
	LeaderID     NodeID     `json:"leaderId"`
	PrevLogIndex uint64     `json:"prevLogIndex"`
	PrevLogTerm  uint64     `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leaderCommit"`
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`

	// For optimization: on conflict, follower returns the first index
	// in the conflicting term and the term itself.
	ConflictIndex uint64 `json:"conflictIndex,omitempty"`
	ConflictTerm  uint64 `json:"conflictTerm,omitempty"`
}

// InstallSnapshotArgs is sent by leader to bring a follower up to date.
// (For Phase 4, we won't implement snapshots; this is for future extension.)
type InstallSnapshotArgs struct {
	Term              uint64 `json:"term"`
	LeaderID          NodeID `json:"leaderId"`
	LastIncludedIndex uint64 `json:"lastIncludedIndex"`
	LastIncludedTerm  uint64 `json:"lastIncludedTerm"`
	Data              []byte `json:"data"`
}

// InstallSnapshotReply is the response to an InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term uint64 `json:"term"`
}

// EncodeMessage serializes a message to JSON bytes.
func EncodeMessage(typ RPCType, payload interface{}) ([]byte, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	msg := Message{
		Type:    typ,
		Payload: payloadBytes,
	}
	return json.Marshal(msg)
}

// DecodeMessage deserializes a message from JSON bytes.
func DecodeMessage(data []byte) (*Message, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// DecodeRequestVote decodes RequestVoteArgs from a message payload.
func DecodeRequestVote(payload json.RawMessage) (*RequestVoteArgs, error) {
	var args RequestVoteArgs
	if err := json.Unmarshal(payload, &args); err != nil {
		return nil, err
	}
	return &args, nil
}

// DecodeRequestVoteReply decodes RequestVoteReply from a message payload.
func DecodeRequestVoteReply(payload json.RawMessage) (*RequestVoteReply, error) {
	var reply RequestVoteReply
	if err := json.Unmarshal(payload, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

// DecodeAppendEntries decodes AppendEntriesArgs from a message payload.
func DecodeAppendEntries(payload json.RawMessage) (*AppendEntriesArgs, error) {
	var args AppendEntriesArgs
	if err := json.Unmarshal(payload, &args); err != nil {
		return nil, err
	}
	return &args, nil
}

// DecodeAppendEntriesReply decodes AppendEntriesReply from a message payload.
func DecodeAppendEntriesReply(payload json.RawMessage) (*AppendEntriesReply, error) {
	var reply AppendEntriesReply
	if err := json.Unmarshal(payload, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

// DecodeInstallSnapshot decodes InstallSnapshotArgs from a message payload.
func DecodeInstallSnapshot(payload json.RawMessage) (*InstallSnapshotArgs, error) {
	var args InstallSnapshotArgs
	if err := json.Unmarshal(payload, &args); err != nil {
		return nil, err
	}
	return &args, nil
}

// DecodeInstallSnapshotReply decodes InstallSnapshotReply from a message payload.
func DecodeInstallSnapshotReply(payload json.RawMessage) (*InstallSnapshotReply, error) {
	var reply InstallSnapshotReply
	if err := json.Unmarshal(payload, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}
