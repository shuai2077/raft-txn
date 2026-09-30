package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/eliben/raft/txnkv/txn"
)

// This file holds the test-facing half of the harness: helpers whose only job is
// to make a test read like the scenario it is describing.

// testHTTPBase is the first port a harness uses. Ports are allocated
// deterministically rather than asking the OS, so a failure is reproducible and
// the cluster's topology is visible in the logs.
//
// Tests are not run in parallel (they share this port range), and each harness
// releases its listeners in Shutdown.
const testHTTPBase = 17600

// TestLockTTL is the intent-lock lease used by test clusters.
//
// It has to balance two things. It must be short enough that a test can watch a
// dead coordinator's locks lapse without waiting seconds. It must also be long
// enough that the locks of *live* transactions - which legitimately collide
// during concurrent workloads - expire before a reader gives up waiting for the
// owner to finish. A lease far shorter than the time it takes to run a
// transaction turns ordinary write contention into spurious conflicts.
const TestLockTTL = time.Second

// TestConfig returns a Config suitable for a test: small groups, three-node
// Raft per group, a three-member oracle, and a short lock lease.
//
// Ports start high and are spaced out by cluster, so several harnesses in one
// test binary - including harnesses that outlive a subtest - cannot collide.
var testPortCursor = testHTTPBase

func TestConfig(numGroups int, partitioner txn.Partitioner) Config {
	testPortCursor += 256
	return Config{
		NumGroups:     numGroups,
		NodesPerGroup: 3,
		TSONodes:      3,
		WindowSize:    64,
		LockTTL:       TestLockTTL,
		HTTPBase:      testPortCursor,
		Partitioner:   partitioner,
	}
}

// NewHarness builds and starts a cluster for a test, registering its shutdown.
func NewHarness(t *testing.T, cfg Config) *Cluster {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	t.Cleanup(c.Shutdown)
	if err := c.WaitForLeaders(10 * time.Second); err != nil {
		t.Fatalf("cluster did not elect leaders: %v", err)
	}
	return c
}

// PrefixPartitioner routes a key to the group named by its first character: key
// "0abc" lives in group 0, "1abc" in group 1, and so on. It makes a
// multi-group transaction trivial to write by hand, which is why the tests use
// it instead of the hash partitioner: the scenario is visible in the key names.
type PrefixPartitioner struct{ Groups int }

// GroupForKey implements txn.Partitioner.
func (p PrefixPartitioner) GroupForKey(key txn.Key) txn.GroupID {
	if len(key) == 0 {
		return 0
	}
	if c := key[0]; c >= '0' && c <= '9' {
		g := int(c - '0')
		if g < p.Groups {
			return txn.GroupID(g)
		}
	}
	return 0
}

// Put commits a single-key write, retrying on conflict.
func (c *Cluster) Put(t *testing.T, key txn.Key, value txn.Value) txn.Timestamp {
	t.Helper()
	ts, err := c.RunTxn(context.Background(), 10, func(tx *txn.Txn) error {
		return tx.Put(key, value)
	})
	if err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
	return ts
}

// Delete commits the removal of a key, retrying on conflict.
func (c *Cluster) Delete(t *testing.T, key txn.Key) txn.Timestamp {
	t.Helper()
	ts, err := c.RunTxn(context.Background(), 10, func(tx *txn.Txn) error {
		return tx.Delete(key)
	})
	if err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}
	return ts
}

// Get reads a key in its own read-only transaction.
func (c *Cluster) Get(t *testing.T, key txn.Key) (txn.Value, bool) {
	t.Helper()
	tx, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Abort(context.Background()) }()
	v, found, err := tx.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return v, found
}

// MustGet asserts a key holds want.
func (c *Cluster) MustGet(t *testing.T, key txn.Key, want txn.Value) {
	t.Helper()
	got, found := c.Get(t, key)
	if !found {
		t.Fatalf("key %q not found; want %q", key, want)
	}
	if got != want {
		t.Fatalf("key %q = %q; want %q", key, got, want)
	}
}

// MustNotFind asserts a key has no value at a fresh snapshot.
func (c *Cluster) MustNotFind(t *testing.T, key txn.Key) {
	t.Helper()
	if got, found := c.Get(t, key); found {
		t.Fatalf("key %q = %q; want it absent", key, got)
	}
}

// PrewriteTxn runs only the prepare phase of a transaction, leaving its intent
// locks in place on every group it spans.
//
// This is how tests create the state a crashed coordinator leaves behind, which
// is the state the recovery protocol exists for.
func (c *Cluster) PrewriteTxn(t *testing.T, tx *txn.Txn) {
	t.Helper()
	if err := txn.Prewrite(tx); err != nil {
		t.Fatalf("prewrite: %v", err)
	}
}

// SealPrimary performs the transaction's atomic commit point by hand: it writes
// a commit timestamp into the lock record of the primary key and then stops,
// exactly where a coordinator would stop if it died between deciding to commit
// and finishing the remaining groups.
//
// It returns the timestamp the transaction committed at. Tests use it to build
// the crash window that lock recovery exists to handle; the state it leaves
// behind is a durable, unfinished commit.
func (c *Cluster) SealPrimary(t *testing.T, tx *txn.Txn, primaryKey txn.Key, group, node int) txn.Timestamp {
	t.Helper()

	// The commit timestamp must exceed the transaction's snapshot but stay below
	// any lock lease the test has taken control of, so it is derived from the
	// transaction rather than from the (possibly frozen) wall clock.
	ts, err := c.txnClient.Oracle().CommitTS(context.Background(), tx.StartTS())
	if err != nil {
		t.Fatalf("obtaining a commit timestamp: %v", err)
	}
	participant := c.GroupNode(group, node).Participant()
	if err := participant.Seal(context.Background(), primaryKey, tx.ID(), tx.StartTS(), ts); err != nil {
		t.Fatalf("sealing the primary lock for %q: %v", primaryKey, err)
	}
	return ts
}

// GuessPrimaryKey returns the primary key a transaction will use, given its
// write set: the lexicographically smallest key. It mirrors the coordinator's
// own rule, and tests use it to know which group holds the commit point.
func GuessPrimaryKey(writes []txn.WriteOp) txn.Key {
	best := txn.Key("")
	for _, op := range writes {
		if best == "" || op.Key < best {
			best = op.Key
		}
	}
	return best
}

// WaitForRead retries a read until it succeeds or the timeout expires.
//
// Reads of a key whose previous writer died mid-commit legitimately fail for as
// long as that writer's lease is live: the reader cannot tell "about to commit"
// from "gone" until the lease lapses. Retrying is what an application does, and
// it keeps tests from having to manipulate the clock to skip the wait.
func (c *Cluster) WaitForRead(t *testing.T, key txn.Key, timeout time.Duration) (txn.Value, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		tx, err := c.txnClient.Begin(context.Background())
		if err != nil {
			lastErr = err
		} else {
			value, found, err := tx.Get(context.Background(), key)
			_ = tx.Abort(context.Background())
			if err == nil {
				return value, found
			}
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("read of %q never succeeded within %s: %v", key, timeout, lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// MustGetEventually is MustGet with retries, for keys whose writer may have died
// holding a lock.
func (c *Cluster) MustGetEventually(t *testing.T, key txn.Key, want txn.Value, timeout time.Duration) {
	t.Helper()
	got, found := c.WaitForRead(t, key, timeout)
	if !found {
		t.Fatalf("key %q not found within %s; want %q", key, timeout, want)
	}
	if got != want {
		t.Fatalf("key %q = %q; want %q", key, got, want)
	}
}

// StoreSnapshot is a read of one group member's state machine, for tests that
// need to see what a replica actually holds rather than what the client reports.
type StoreSnapshot struct {
	// Committed maps keys with a published value to that value at "now".
	Committed map[txn.Key]txn.Value
	// Deleted lists keys whose newest committed version is a delete.
	Deleted []txn.Key
	// Locked lists keys with a live intent lock, and the owning transaction.
	Locked map[txn.Key]txn.TxnID
}

// Snapshot reads a group member's committed state and locks. It reaches into the
// state machine directly, so it is only valid for tests: in a real deployment a
// client would go through the Raft log.
func (c *Cluster) Snapshot(t *testing.T, group, node int) StoreSnapshot {
	t.Helper()
	store := c.GroupNode(group, node).Store()
	if store == nil {
		t.Fatalf("group %d node %d has no state machine", group, node)
	}
	snap := StoreSnapshot{
		Committed: make(map[txn.Key]txn.Value),
		Locked:    make(map[txn.Key]txn.TxnID),
	}
	for _, key := range store.Keys() {
		value, found := store.ValueAtNow(key)
		if !found {
			snap.Deleted = append(snap.Deleted, key)
			continue
		}
		snap.Committed[key] = value
	}
	for key, txnID := range store.Locks() {
		snap.Locked[key] = txnID
	}
	return snap
}

// MustSnapshotGroup asserts that every live, connected member of a group
// eventually reports the given value for a key.
//
// Agreement across replicas is the point of replicating through Raft, but
// replication is asynchronous: a follower may not have applied the entry yet. So
// this waits for convergence rather than demanding it instantly, which is what a
// real client of a Raft group has to do as well.
func (c *Cluster) MustSnapshotGroup(t *testing.T, group int, key txn.Key, want txn.Value, wantFound bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastGap string
	for {
		agreed := true
		lastGap = ""
		for node := 0; node < c.cfg.NodesPerGroup; node++ {
			if !c.alive[group][node] || !c.connected[group][node] {
				continue
			}
			store := c.GroupNode(group, node).Store()
			value, found := store.ValueAtNow(key)
			if found != wantFound || (wantFound && value != want) {
				agreed = false
				lastGap = fmt.Sprintf("node %d has (%q, found=%v)", node, value, found)
				break
			}
		}
		if agreed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("group %d: key %q never reached (%q, found=%v): %s",
				group, key, want, wantFound, lastGap)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// WaitForStoreLocks waits until every live member of a group agrees about the
// set of locked keys, then returns it. Raft replication is asynchronous, so a
// test that has just prewritten a transaction has to wait for the locks to reach
// the followers before it can assert on them.
func (c *Cluster) WaitForStoreLocks(t *testing.T, group int, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		for node := 0; node < c.cfg.NodesPerGroup; node++ {
			if !c.alive[group][node] || !c.connected[group][node] {
				continue
			}
			if n := len(c.GroupNode(group, node).Store().Locks()); n != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			for node := 0; node < c.cfg.NodesPerGroup; node++ {
				if !c.alive[group][node] || !c.connected[group][node] {
					continue
				}
				t.Logf("group %d node %d locks: %v", group, node, c.GroupNode(group, node).Store().Locks())
			}
			t.Fatalf("group %d did not converge on %d locks within %s", group, want, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// MustTxnValueAt reads a key at an explicit snapshot from one group member,
// bypassing the transaction client. It lets tests verify historical reads.
func (c *Cluster) MustTxnValueAt(t *testing.T, group, node int, ts txn.Timestamp, key txn.Key) (txn.Value, bool) {
	t.Helper()
	store := c.GroupNode(group, node).Store()
	lock := store.LockFor(key)
	if lock != nil {
		t.Fatalf("key %q is locked by txn %d; expected a clean key", key, lock.TxnID)
	}
	return store.ValueAt(key, ts)
}

// DumpTopology writes the cluster's leader layout to the test log. It is
// invaluable when a distributed test fails.
func (c *Cluster) DumpTopology(t *testing.T) {
	t.Helper()
	for g := 0; g < c.cfg.NumGroups; g++ {
		parts := make([]string, 0, c.cfg.NodesPerGroup)
		for n := 0; n < c.cfg.NodesPerGroup; n++ {
			state := "down"
			switch {
			case !c.alive[g][n]:
				state = "crashed"
			case !c.connected[g][n]:
				state = "partitioned"
			case c.nodes[g][n].IsLeader():
				state = "LEADER"
			}
			parts = append(parts, fmt.Sprintf("n%d:%s", n, state))
		}
		t.Logf("group %d: %v", g, parts)
	}
	tsoParts := make([]string, 0, c.cfg.TSONodes)
	for i := range c.tsoNodes {
		state := "down"
		switch {
		case !c.tsoAlive[i]:
			state = "crashed"
		case !c.tsoConnected[i]:
			state = "partitioned"
		case c.tsoNodes[i].IsLeader():
			state = "LEADER"
		}
		tsoParts = append(tsoParts, fmt.Sprintf("n%d:%s", i, state))
	}
	t.Logf("tso: %v", tsoParts)
}
