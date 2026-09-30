package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/eliben/raft/txnkv/txn"
)

// maxCommitTS returns the highest commit timestamp in a group member's state
// machine, and how many versions it holds. This is the ground truth about what a
// replica has actually applied, independent of any watermark the store reports.
func maxCommitTS(c *Cluster, group int, keys ...txn.Key) (txn.Timestamp, int) {
	store := c.GroupNode(group, c.LeaderOf(group)).Store()
	max, count := txn.Timestamp(0), 0
	for _, key := range keys {
		for _, v := range store.VersionsFor(key) {
			count++
			if v.CommitTS > max {
				max = v.CommitTS
			}
		}
	}
	return max, count
}

// TestReadBarrierEstablishesAppliedPrefix checks the property the whole read
// barrier rests on: once a barrier returns on a group, that group's leader has
// applied every entry that was ahead of the barrier in its log.
//
// The claim is about the leader, and that is not a weakening. A barrier is a
// proposal, so only the leader can take one, and reads are served by the leader;
// followers catch up asynchronously and their progress is replication, not
// correctness. Asserting over followers would test replication speed instead, and
// would fail intermittently - which is exactly what it did before this comment
// existed.
func TestReadBarrierEstablishesAppliedPrefix(t *testing.T) {
	c := NewHarness(t, TestConfig(2, PrefixPartitioner{Groups: 2}))

	keys := []txn.Key{"0a", "0b", "0c", "0d", "0e"}
	want := map[txn.Key]txn.Value{}
	for i, key := range keys {
		value := txn.Value(string(rune('1' + i)))
		want[key] = value
		c.PutT(t, key, value)
	}

	watermark, err := c.Barrier(0)
	if err != nil {
		t.Fatalf("barrier on group 0: %v", err)
	}

	leader := c.LeaderOf(0)
	for _, key := range keys {
		value, found := c.GroupNode(0, leader).Store().ValueAtNow(key)
		if !found || value != want[key] {
			t.Errorf("group 0 leader n%d: key %q = (%q, found=%v) after the barrier; want %q",
				leader, key, value, found, want[key])
		}
	}

	// The watermark must cover the writes: a barrier that returned early would
	// leave it below the newest version it claims to have applied.
	newest, count := maxCommitTS(c, 0, keys...)
	if count != len(keys) {
		t.Errorf("group 0 leader has %d versions across the written keys, want %d", count, len(keys))
	}
	if watermark < newest {
		t.Errorf("applied watermark %d is below the newest applied version %d", watermark, newest)
	}

	// Followers are expected to converge, just not necessarily by the time the
	// barrier returns. Waiting for them proves no write was lost, without
	// pretending the barrier is a replication barrier.
	if err := c.WaitForSettled(20 * time.Second); err != nil {
		t.Fatalf("cluster did not settle: %v", err)
	}
	for _, key := range keys {
		for node := 0; node < c.cfg.NodesPerGroup; node++ {
			value, found := c.GroupNode(0, node).Store().ValueAtNow(key)
			if !found || value != want[key] {
				t.Errorf("group 0 node %d: key %q = (%q, found=%v) after settling; want %q",
					node, key, value, found, want[key])
			}
		}
	}
}

// TestReadBarrierAcrossAllGroups is the same guarantee for the shape the client
// actually uses: a transaction that reads keys in several groups takes a barrier
// on each.
//
// Note the comparison is per group. Watermarks are group-local - each records the
// highest commit timestamp *that group* has applied - so the minimum across groups
// says nothing about any individual group's history beyond "at least this far".
func TestReadBarrierAcrossAllGroups(t *testing.T) {
	c := NewHarness(t, TestConfig(2, PrefixPartitioner{Groups: 2}))

	// One write per group, committed as separate transactions.
	c.PutT(t, "0alice", "100")
	c.PutT(t, "1bob", "200")

	if _, err := c.Barriers(); err != nil {
		t.Fatalf("barriers: %v", err)
	}

	// Settle first: the barrier guarantees the leader, and this test is about the
	// leader's state, so followers must not be sampled mid-catch-up.
	if err := c.WaitForSettled(20 * time.Second); err != nil {
		t.Fatalf("cluster did not settle: %v", err)
	}

	for group, key := range map[int]txn.Key{0: "0alice", 1: "1bob"} {
		want := txn.Value("100")
		if group == 1 {
			want = "200"
		}
		watermark := c.GroupNode(group, c.LeaderOf(group)).Store().AppliedCommitTS()
		newest, _ := maxCommitTS(c, group, key)
		if watermark < newest {
			t.Errorf("group %d watermark %d is below its own newest version %d",
				group, watermark, newest)
		}
		for node := 0; node < c.cfg.NodesPerGroup; node++ {
			value, found := c.GroupNode(group, node).Store().ValueAtNow(key)
			if !found || value != want {
				t.Errorf("group %d node %d: key %q = (%q, found=%v); want %q",
					group, node, key, value, found, want)
			}
		}
	}
}

// TestReadBarrierReturnsOnlyAfterApply interleaves a write and a barrier
// repeatedly, so a barrier that returned before the preceding write reached the
// state machine would fail on some iteration rather than by luck.
func TestReadBarrierReturnsOnlyAfterApply(t *testing.T) {
	c := NewHarness(t, TestConfig(2, PrefixPartitioner{Groups: 2}))

	for i := 0; i < 20; i++ {
		key := txn.Key("0k" + string(rune('a'+i%26)))
		value := txn.Value("v")
		ts, err := c.Put(key, value)
		if err != nil {
			t.Fatalf("iteration %d: put: %v", i, err)
		}
		watermark, err := c.Barrier(0)
		if err != nil {
			t.Fatalf("iteration %d: barrier: %v", i, err)
		}
		// The write is decided at ts, so a barrier applied afterwards must have
		// carried the group at least that far.
		if watermark < ts {
			t.Fatalf("iteration %d: watermark %d after the barrier is below the write's ts %d",
				i, watermark, ts)
		}
		if _, found := c.GroupNode(0, c.LeaderOf(0)).Store().ValueAtNow(key); !found {
			t.Fatalf("iteration %d: key %q missing right after a barrier", i, key)
		}
	}
}

// TestBarrierChangesNoState checks that barriers are safe to take repeatedly and
// to retry, which is what lets the client re-propose one after a routing failure.
func TestBarrierChangesNoState(t *testing.T) {
	c := NewHarness(t, TestConfig(2, PrefixPartitioner{Groups: 2}))
	c.PutT(t, "0a", "1")

	group := 0
	before := c.GroupNode(group, c.LeaderOf(group)).Store().Stats()

	// Barrier, then confirm the write is still readable and no state moved.
	if _, err := c.Barrier(group); err != nil {
		t.Fatalf("first barrier: %v", err)
	}
	after := c.GroupNode(group, c.LeaderOf(group)).Store().Stats()
	if after.Keys != before.Keys || after.Locks != before.Locks || after.Versions != before.Versions {
		t.Errorf("barrier changed the state machine: keys %d->%d, versions %d->%d, locks %d->%d",
			before.Keys, after.Keys, before.Versions, after.Versions, before.Locks, after.Locks)
	}
	c.MustGet(t, "0a", "1")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < 25; i++ {
		if _, err := c.BarrierCtx(ctx, group); err != nil {
			t.Fatalf("repeat barrier %d: %v", i, err)
		}
	}
	final := c.GroupNode(group, c.LeaderOf(group)).Store().Stats()
	if final != after {
		t.Errorf("repeated barriers changed the state machine: %+v -> %+v", after, final)
	}
}
