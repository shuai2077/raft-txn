package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/eliben/raft/txnkv/txn"
)

// This file holds the parts of the harness that report failures through
// *testing.T. They are thin wrappers over the package's own API, so a test reads
// like the scenario it describes while non-test callers use the same underlying
// operations.

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

// Put commits a single-key write, failing the test on error.
func (c *Cluster) PutT(t *testing.T, key txn.Key, value txn.Value) txn.Timestamp {
	t.Helper()
	ts, err := c.Put(key, value)
	if err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
	return ts
}

// DeleteT commits the removal of a key, failing the test on error.
func (c *Cluster) DeleteT(t *testing.T, key txn.Key) txn.Timestamp {
	t.Helper()
	ts, err := c.Delete(key)
	if err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}
	return ts
}

// Get reads a key in its own read-only transaction, failing the test on error.
func (c *Cluster) Get(t *testing.T, key txn.Key) (txn.Value, bool) {
	t.Helper()
	value, found, err := c.Read(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return value, found
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

// MustGetEventually is MustGet with retries, for keys whose writer may have died
// holding a lock.
func (c *Cluster) MustGetEventually(t *testing.T, key txn.Key, want txn.Value, timeout time.Duration) {
	t.Helper()
	got, found, err := c.WaitForRead(key, timeout)
	if err != nil {
		t.Fatalf("waiting for %q: %v", key, err)
	}
	if !found {
		t.Fatalf("key %q not found within %s; want %q", key, timeout, want)
	}
	if got != want {
		t.Fatalf("key %q = %q; want %q", key, got, want)
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

// SealPrimaryT is SealPrimary with test failure reporting.
func (c *Cluster) SealPrimaryT(t *testing.T, tx *txn.Txn, primaryKey txn.Key, group, node int) txn.Timestamp {
	t.Helper()
	ts, err := c.SealPrimary(tx, primaryKey, group, node)
	if err != nil {
		t.Fatalf("sealing the primary lock: %v", err)
	}
	return ts
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
				lastGap = gapString(node, value, found)
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

func gapString(node int, value txn.Value, found bool) string {
	return "node " + itoa(node) + " has (" + string(value) + ", found=" + boolString(found) + ")"
}

// WaitForStoreLocks waits until every live member of a group agrees about the
// number of locked keys.
//
// Raft replication is asynchronous, so a test that has just prewritten a
// transaction has to wait for the locks to reach the followers before it can
// assert on them.
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
	if lock := store.LockFor(key); lock != nil {
		t.Fatalf("key %q is locked by txn %d; expected a clean key", key, lock.TxnID)
	}
	return store.ValueAt(key, ts)
}

// DumpTopologyT logs the cluster's leader layout.
func (c *Cluster) DumpTopologyT(t *testing.T) {
	t.Helper()
	for _, line := range c.DumpTopology() {
		t.Log(line)
	}
}

// mustWaitForRead is WaitForRead with test failure reporting.
func mustWaitForRead(c *Cluster, t *testing.T, key txn.Key, timeout time.Duration) (txn.Value, bool) {
	t.Helper()
	value, found, err := c.WaitForRead(key, timeout)
	if err != nil {
		t.Fatalf("waiting for %q: %v", key, err)
	}
	return value, found
}

// RunTxnT is RunTxn with test failure reporting.
func (c *Cluster) RunTxnT(t *testing.T, fn func(*txn.Txn) error) txn.Timestamp {
	t.Helper()
	ts, err := c.RunTxn(context.Background(), 20, fn)
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}
	return ts
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func boolString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
