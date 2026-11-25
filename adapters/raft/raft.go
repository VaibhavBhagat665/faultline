// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"github.com/VaibhavBhagat665/faultline/internal/bugs"
)

// startElection initiates a leader election.
func (n *RaftNode) startElection() {
	if n.role == Leader {
		return // leaders don't start elections
	}

	n.becomeCandidate()

	// Vote for self
	votesReceived := 1
	votesNeeded := (len(n.peers) + 1 + 1) / 2 // majority

	n.env.Logf("[%s] Starting election for term %d (need %d votes)", n.id, n.currentTerm, votesNeeded)

	if votesReceived >= votesNeeded {
		n.becomeLeader()
		return
	}

	// Request votes from all peers
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	for _, peer := range n.peers {
		args := RequestVoteArgs{
			Term:         n.currentTerm,
			CandidateID:  n.id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		payload, err := EncodeMessage(RPCRequestVote, args)
		if err != nil {
			n.env.Logf("[%s] Failed to encode RequestVote: %v", n.id, err)
			continue
		}
		n.env.Send(peer, payload)
	}
}

// handleRequestVote processes a RequestVote RPC.
func (n *RaftNode) handleRequestVote(from NodeID, args *RequestVoteArgs) {
	// If RPC request or response contains term T > currentTerm,
	// set currentTerm = T, convert to follower
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term, nil)
		n.votedFor = nil
		if err := n.persistState(); err != nil {
			n.env.Logf("[%s] Failed to persist state: %v", n.id, err)
		}
	}

	reply := RequestVoteReply{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// Reply false if term < currentTerm
	if args.Term < n.currentTerm {
		n.sendRequestVoteReply(from, reply)
		return
	}

	// If votedFor is null or candidateId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote
	voteGranted := false
	if (n.votedFor == nil || *n.votedFor == args.CandidateID) {
		// Check if candidate's log is at least as up-to-date
		lastLogIndex, lastLogTerm := n.lastLogInfo()
		candidateUpToDate := args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

		if candidateUpToDate {
			voteGranted = true
			n.votedFor = &args.CandidateID
			n.resetElectionTimer() // reset election timeout on granting vote
			
			// BUG: DoubleVote - skip persisting votedFor before replying
			// This allows the node to vote for multiple candidates in the same term after a restart
			if n.shouldInjectBug(bugs.BugDoubleVote) {
				n.env.Logf("[%s] BUG.DoubleVote: Skipping persist before vote reply (term=%d, candidate=%s)", 
					n.id, args.Term, args.CandidateID)
				// Emit trace event
			} else {
				if err := n.persistState(); err != nil {
					n.env.Logf("[%s] Failed to persist vote: %v", n.id, err)
				}
			}
			
			n.env.Logf("[%s] Granted vote to %s for term %d", n.id, args.CandidateID, args.Term)
		}
	}

	reply.VoteGranted = voteGranted
	n.sendRequestVoteReply(from, reply)
}

// handleRequestVoteReply processes a RequestVote reply.
func (n *RaftNode) handleRequestVoteReply(from NodeID, reply *RequestVoteReply) {
	// Ignore if we're not a candidate
	if n.role != Candidate {
		return
	}

	// Ignore stale replies
	if reply.Term < n.currentTerm {
		return
	}

	// If reply term is higher, become follower
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term, nil)
		n.votedFor = nil
		if err := n.persistState(); err != nil {
			n.env.Logf("[%s] Failed to persist state: %v", n.id, err)
		}
		return
	}

	// Count votes
	if reply.VoteGranted {
		n.votesReceived[from] = true
		votesReceived := len(n.votesReceived)
		votesNeeded := (len(n.peers) + 1 + 1) / 2

		n.env.Logf("[%s] Received vote from %s (%d/%d)", n.id, from, votesReceived, votesNeeded)

		if votesReceived >= votesNeeded {
			n.becomeLeader()
		}
	}
}

// sendRequestVoteReply sends a RequestVote reply.
func (n *RaftNode) sendRequestVoteReply(to NodeID, reply RequestVoteReply) {
	payload, err := EncodeMessage(RPCRequestVoteReply, reply)
	if err != nil {
		n.env.Logf("[%s] Failed to encode RequestVoteReply: %v", n.id, err)
		return
	}
	n.env.Send(to, payload)
}

// sendHeartbeats sends heartbeat AppendEntries to all peers.
func (n *RaftNode) sendHeartbeats() {
	for _, peer := range n.peers {
		n.sendAppendEntriesToPeer(peer)
	}
}

// sendAppendEntries sends AppendEntries to all peers.
func (n *RaftNode) sendAppendEntries() {
	for _, peer := range n.peers {
		n.sendAppendEntriesToPeer(peer)
	}
}

// sendAppendEntriesToPeer sends AppendEntries to a specific peer.
func (n *RaftNode) sendAppendEntriesToPeer(peer NodeID) {
	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)

	if prevLogIndex > 0 && int(prevLogIndex) < len(n.log) {
		prevLogTerm = n.log[prevLogIndex].Term
	}

	// Gather entries to send
	entries := []LogEntry{}
	if int(nextIdx) < len(n.log) {
		maxEntries := n.config.MaxEntriesPerRPC
		if maxEntries == 0 {
			maxEntries = len(n.log) - int(nextIdx)
		}
		endIdx := int(nextIdx) + maxEntries
		if endIdx > len(n.log) {
			endIdx = len(n.log)
		}
		entries = n.log[nextIdx:endIdx]
	}

	args := AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}

	payload, err := EncodeMessage(RPCAppendEntries, args)
	if err != nil {
		n.env.Logf("[%s] Failed to encode AppendEntries: %v", n.id, err)
		return
	}
	n.env.Send(peer, payload)
}

// handleAppendEntries processes an AppendEntries RPC.
func (n *RaftNode) handleAppendEntries(from NodeID, args *AppendEntriesArgs) {
	// If RPC contains term T > currentTerm, become follower
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term, &args.LeaderID)
		n.votedFor = nil
		if err := n.persistState(); err != nil {
			n.env.Logf("[%s] Failed to persist state: %v", n.id, err)
		}
	}

	reply := AppendEntriesReply{
		Term:    n.currentTerm,
		Success: false,
	}

	// Reply false if term < currentTerm
	if args.Term < n.currentTerm {
		n.sendAppendEntriesReply(from, reply)
		return
	}

	// Valid AppendEntries from current leader
	n.resetElectionTimer()
	n.currentLeader = &args.LeaderID

	if n.role != Follower {
		n.becomeFollower(args.Term, &args.LeaderID)
	}

	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex > 0 {
		if int(args.PrevLogIndex) >= len(n.log) {
			// Log is too short
			reply.ConflictIndex = uint64(len(n.log))
			reply.ConflictTerm = 0
			n.sendAppendEntriesReply(from, reply)
			return
		}
		if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// Term mismatch
			conflictTerm := n.log[args.PrevLogIndex].Term
			conflictIndex := args.PrevLogIndex
			// Find first index of conflicting term
			for i := args.PrevLogIndex; i > 0; i-- {
				if n.log[i].Term != conflictTerm {
					conflictIndex = i + 1
					break
				}
			}
			reply.ConflictIndex = conflictIndex
			reply.ConflictTerm = conflictTerm
			n.sendAppendEntriesReply(from, reply)
			return
		}
	}

	// If an existing entry conflicts with a new one (same index but
	// different terms), delete the existing entry and all that follow it
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if int(idx) < len(n.log) {
			if n.log[idx].Term != entry.Term {
				// Conflict: truncate log
				n.log = n.log[:idx]
			}
		}
	}

	// Append any new entries not already in the log
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if int(idx) >= len(n.log) {
			n.log = append(n.log, entry)
		}
	}

	// Persist log
	if len(args.Entries) > 0 {
		if err := n.persistState(); err != nil {
			n.env.Logf("[%s] Failed to persist log: %v", n.id, err)
		}
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > n.commitIndex {
		newCommitIndex := args.LeaderCommit
		if lastNewEntry := uint64(len(n.log) - 1); newCommitIndex > lastNewEntry {
			newCommitIndex = lastNewEntry
		}
		n.commitIndex = newCommitIndex
		n.applyCommitted()
	}

	reply.Success = true
	n.sendAppendEntriesReply(from, reply)
}

// handleAppendEntriesReply processes an AppendEntries reply.
func (n *RaftNode) handleAppendEntriesReply(from NodeID, reply *AppendEntriesReply) {
	// Ignore if we're not the leader
	if n.role != Leader {
		return
	}

	// Ignore stale replies
	if reply.Term < n.currentTerm {
		return
	}

	// If reply term is higher, become follower
	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term, nil)
		n.votedFor = nil
		if err := n.persistState(); err != nil {
			n.env.Logf("[%s] Failed to persist state: %v", n.id, err)
		}
		return
	}

	if reply.Success {
		// Update nextIndex and matchIndex based on what was sent
		// We need to know how many entries were in the AppendEntries RPC
		// For now, assume the follower has replicated up to the last entry we have
		lastLogIndex := uint64(len(n.log) - 1)
		if n.matchIndex[from] < lastLogIndex {
			n.matchIndex[from] = lastLogIndex
		}
		n.nextIndex[from] = n.matchIndex[from] + 1

		n.env.Logf("[%s] Updated matchIndex[%s]=%d", n.id, from, n.matchIndex[from])

		// Update commitIndex
		n.updateCommitIndex()
	} else {
		// Decrement nextIndex and retry
		if reply.ConflictTerm > 0 {
			// Optimization: skip over conflicting term
			n.nextIndex[from] = reply.ConflictIndex
		} else {
			// Log too short
			n.nextIndex[from] = reply.ConflictIndex
		}
		if n.nextIndex[from] < 1 {
			n.nextIndex[from] = 1
		}
		// Retry immediately
		n.sendAppendEntriesToPeer(from)
	}
}

// sendAppendEntriesReply sends an AppendEntries reply.
func (n *RaftNode) sendAppendEntriesReply(to NodeID, reply AppendEntriesReply) {
	payload, err := EncodeMessage(RPCAppendEntriesReply, reply)
	if err != nil {
		n.env.Logf("[%s] Failed to encode AppendEntriesReply: %v", n.id, err)
		return
	}
	n.env.Send(to, payload)
}

// updateCommitIndex advances commitIndex if a majority of followers have replicated an entry.
func (n *RaftNode) updateCommitIndex() {
	oldCommitIndex := n.commitIndex
	
	// Find the highest index replicated on a majority
	for idx := n.commitIndex + 1; int(idx) < len(n.log); idx++ {
		// Only commit entries from current term (safety)
		if n.log[idx].Term != n.currentTerm {
			n.env.Logf("[%s] Skipping index %d: term %d != currentTerm %d", 
				n.id, idx, n.log[idx].Term, n.currentTerm)
			continue
		}

		replicaCount := 1 // self
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				replicaCount++
			}
		}

		majority := (len(n.peers) + 1 + 1) / 2
		
		// BUG: CommitWithoutQuorum - commit even without majority
		if n.shouldInjectBug(bugs.BugCommitWithoutQuorum) {
			// Commit if at least one follower has it (minority quorum)
			if replicaCount >= 2 {
				n.commitIndex = idx
				n.env.Logf("[%s] BUG.CommitWithoutQuorum: Committed index %d with only %d/%d replicas (need %d)", 
					n.id, idx, replicaCount, len(n.peers)+1, majority)
			} else {
				break
			}
		} else {
			n.env.Logf("[%s] Index %d: replicated on %d/%d nodes (need %d)", 
				n.id, idx, replicaCount, len(n.peers)+1, majority)
			
			if replicaCount >= majority {
				n.commitIndex = idx
			} else {
				break
			}
		}
	}

	if n.commitIndex > oldCommitIndex {
		n.env.Logf("[%s] Advanced commitIndex: %d -> %d", n.id, oldCommitIndex, n.commitIndex)
	}

	n.applyCommitted()
}

// applyCommitted applies committed entries to the state machine.
func (n *RaftNode) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]

		// For now, we'll just log the apply
		n.env.Logf("[%s] Applied entry %d (term=%d)", n.id, entry.Index, entry.Term)

		// If this was a client request, reply
		if pr, ok := n.pendingRequests[entry.Index]; ok {
			// Decode command to extract client session info
			// For simplicity, assume command is JSON: {"clientID": X, "seq": Y, "op": ...}
			// In a real implementation, we'd deserialize and execute against the state machine.
			result := []byte("ok") // placeholder
			pr.reply(Response{Success: true, Value: result, Leader: n.id})
			delete(n.pendingRequests, entry.Index)

			// Update session cache (simplified: assume we can extract clientID and seq)
			// For now, skip session tracking in this simplified version.
		}
	}
}
