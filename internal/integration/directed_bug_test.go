// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"testing"
)

// TestDirected_DoubleVote demonstrates DoubleVote bug with explicit fault schedule.
// SKIPPED: Scenario too complex to reliably trigger with current cluster API.
// Bug IS wired but creating two leaders in same term requires more sophisticated orchestration.
func TestDirected_DoubleVote(t *testing.T) {
	t.Skip("DoubleVote: scenario requires advanced orchestration")
}

// TestDirected_StaleRead demonstrates StaleRead bug.
// Partition old leader, new leader commits write, read from old leader → lincheck must reject.
func TestDirected_StaleRead(t *testing.T) {
	t.Skip("StaleRead: need workload/lincheck integration")
}

// TestDirected_CommitWithoutQuorum demonstrates CommitWithoutQuorum bug.
// Isolate leader, let it "commit" with minority, crash it, elect new leader → 
// committed write must be lost, State Machine Safety must fire.
func TestDirected_CommitWithoutQuorum(t *testing.T) {
	t.Skip("CommitWithoutQuorum: need client operations")
}
