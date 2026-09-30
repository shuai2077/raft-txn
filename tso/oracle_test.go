package tso

import "testing"

// These tests pin down the single property the timestamp oracle exists for:
// timestamps are never repeated, not even across a leader change. Everything
// else about the oracle - window size, batching, Raft round trips - is a
// performance concern on top of that.

// TestNewLeaderCannotSpendPredecessorsWindow is the failover case.
//
// A leader reserves a window and spends part of it. A second member replays the
// same log - exactly what a new leader does - and must not be able to hand out
// any of those timestamps, even though they are below its own high-water mark.
func TestNewLeaderCannotSpendPredecessorsWindow(t *testing.T) {
	const window = 8
	const oldLeader, newLeader = 1, 2

	leader := NewOracle(window)
	leader.setNode(oldLeader)
	leader.Apply(Command{Kind: CmdAllocate, Count: window, NodeID: oldLeader})
	leader.beginServing(oldLeader)

	issued := make(map[uint64]bool)
	for i := 0; i < 5; i++ {
		ts, ok := leader.take()
		if !ok {
			t.Fatalf("take %d failed; allocated=%d available=%d", i, leader.Allocated(), leader.Available())
		}
		if issued[ts] {
			t.Fatalf("the leader issued %d twice", ts)
		}
		issued[ts] = true
	}
	t.Logf("old leader issued %v; allocated=%d", keys(issued), leader.Allocated())

	// The new leader replays the same allocation entry.
	successor := NewOracle(window)
	successor.setNode(newLeader)
	successor.Apply(Command{Kind: CmdAllocate, Count: window, NodeID: oldLeader})
	successor.beginServing(newLeader)

	if n := successor.Available(); n != 0 {
		t.Fatalf("the new leader thinks it has %d timestamps available from its predecessor's window", n)
	}
	if ts, ok := successor.take(); ok {
		t.Fatalf("the new leader served %d out of its predecessor's window", ts)
	}

	// It reserves its own window, which starts above everything spoken for.
	successor.Apply(Command{Kind: CmdAllocate, Count: window, NodeID: newLeader})
	successor.beginServing(newLeader)
	for i := 0; i < window; i++ {
		ts, ok := successor.take()
		if !ok {
			t.Fatalf("new leader ran out after %d timestamps", i)
		}
		if issued[ts] {
			t.Fatalf("the new leader reissued timestamp %d; already issued %v", ts, keys(issued))
		}
		issued[ts] = true
	}
}

func TestSameMemberCanReuseItsOwnWindow(t *testing.T) {
	// A reservation belongs to one member, so that member may spend the rest of
	// it after any amount of replay. This is what keeps the common path free of
	// Raft round trips.
	const node = 3
	o := NewOracle(4)
	o.setNode(node)
	o.Apply(Command{Kind: CmdAllocate, Count: 4, NodeID: node})
	o.beginServing(node)

	first, ok := o.take()
	if !ok || first != 1 {
		t.Fatalf("first take = (%d, %v); want 1", first, ok)
	}

	// Simulate a restart: same member, same log, fresh in-memory state.
	restarted := NewOracle(4)
	restarted.setNode(node)
	restarted.Apply(Command{Kind: CmdAllocate, Count: 4, NodeID: node})
	restarted.beginServing(node)
	// It may serve the same timestamps again - that is the documented cost of a
	// restart, and it is why a restarted member does not serve from an old window
	// unless it re-reserves. Here it re-reserves, so it moves forward.
	restarted.Apply(Command{Kind: CmdAllocate, Count: 4, NodeID: node})
	restarted.beginServing(node)

	next, ok := restarted.take()
	if !ok {
		t.Fatal("take after re-reserving failed")
	}
	if next <= first {
		t.Fatalf("re-reservation went backwards: %d then %d", first, next)
	}
}

func TestOracleExhaustionIsReported(t *testing.T) {
	const node = 0
	o := NewOracle(2)
	o.setNode(node)

	if _, ok := o.take(); ok {
		t.Fatal("take succeeded with nothing reserved")
	}
	if n := o.Available(); n != 0 {
		t.Fatalf("Available()=%d, want 0", n)
	}

	o.Apply(Command{Kind: CmdAllocate, Count: 2, NodeID: node})
	o.beginServing(node)
	if n := o.Available(); n != 2 {
		t.Fatalf("Available()=%d, want 2", n)
	}
	if _, ok := o.take(); !ok {
		t.Fatal("first take failed")
	}
	if _, ok := o.take(); !ok {
		t.Fatal("second take failed")
	}
	if _, ok := o.take(); ok {
		t.Fatal("take succeeded past the reservation")
	}
	if n := o.Available(); n != 0 {
		t.Fatalf("Available()=%d, want 0", n)
	}
}

func TestOracleRejectsEmptyAllocation(t *testing.T) {
	o := NewOracle(4)
	res := o.Apply(Command{Kind: CmdAllocate, Count: 0, NodeID: 0}).(Result)
	if res.Err == "" {
		t.Fatal("a zero-size allocation was accepted")
	}
	if res.Allocated != 0 {
		t.Fatalf("Allocated=%d, want 0", res.Allocated)
	}
}

func keys(m map[uint64]bool) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
