package cluster

import (
	"fmt"
	"time"

	"github.com/eliben/raft/txnkv/txn"
)

// This file holds the test-facing half of the harness: helpers whose only job is
// to make a test read like the scenario it is describing.
//
// Everything here is in the package proper rather than a _test.go file so that
// non-test programs - the demo in cmd/demo, and anything built on this package -
// can use the same fault injection and the same assertions about cluster state.

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

// testPortCursor spaces successive test clusters apart so that a harness which
// outlives a subtest cannot collide with the next one's ports.
var testPortCursor = testHTTPBase

// TestConfig returns a Config suitable for a test: small groups, three-node Raft
// per group, a three-member oracle, and a short lock lease.
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

// PrefixPartitioner routes a key to the group named by its first character: key
// "0abc" lives in group 0, "1abc" in group 1, and so on. It makes a multi-group
// transaction trivial to write by hand, which is why the tests use it instead of
// the hash partitioner: the scenario is visible in the key names.
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

// --- operations that do not raise test failures ------------------------------

// Put commits a single-key write, retrying transient failures.
func (c *Cluster) Put(key txn.Key, value txn.Value) (txn.Timestamp, error) {
	return c.RunTxn(contextBackground(), 10, func(tx *txn.Txn) error {
		return tx.Put(key, value)
	})
}

// Delete commits the removal of a key, retrying transient failures.
func (c *Cluster) Delete(key txn.Key) (txn.Timestamp, error) {
	return c.RunTxn(contextBackground(), 10, func(tx *txn.Txn) error {
		return tx.Delete(key)
	})
}

// Read returns a key's value at a fresh snapshot, retrying transient failures.
func (c *Cluster) Read(key txn.Key) (txn.Value, bool, error) {
	var value txn.Value
	var found bool
	_, err := c.RunTxn(contextBackground(), 10, func(tx *txn.Txn) error {
		var err error
		value, found, err = tx.Get(contextBackground(), key)
		return err
	})
	return value, found, err
}

// SealPrimary performs the transaction's atomic commit point by hand: it writes
// a commit timestamp into the lock record of the primary key and then stops,
// exactly where a coordinator would stop if it died between deciding to commit
// and finishing the remaining groups.
//
// The state it leaves behind is a durable, unfinished commit - the window that
// lock recovery exists to close.
func (c *Cluster) SealPrimary(tx *txn.Txn, primaryKey txn.Key, group, node int) (txn.Timestamp, error) {
	ts, err := c.txnClient.Oracle().CommitTS(contextBackground(), tx.StartTS())
	if err != nil {
		return 0, fmt.Errorf("obtaining a commit timestamp: %w", err)
	}
	participant := c.GroupNode(group, node).Participant()
	if err := participant.Seal(contextBackground(), primaryKey, tx.ID(), tx.StartTS(), ts); err != nil {
		return 0, fmt.Errorf("sealing the primary lock for %q: %w", primaryKey, err)
	}
	return ts, nil
}

// GuessPrimaryKey returns the primary key a transaction will use, given its
// write set: the lexicographically smallest key. It mirrors the coordinator's
// own rule, so a caller can tell which group holds the commit point.
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
// it keeps callers from having to manipulate the clock to skip the wait.
func (c *Cluster) WaitForRead(key txn.Key, timeout time.Duration) (txn.Value, bool, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		value, found, err := c.Read(key)
		if err == nil {
			return value, found, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return "", false, fmt.Errorf("read of %q never succeeded within %s: %w", key, timeout, lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// SetRaftLogging turns the verbose Raft logging of every member on or off.
//
// The underlying switch is process-global, so this affects the whole cluster at
// once. It exists so that a program embedding the store - or a demo - can report
// what the transaction layer is doing without competing with a line per
// heartbeat.
func (c *Cluster) SetRaftLogging(enabled bool) {
	for g := range c.nodes {
		for n := range c.nodes[g] {
			if c.nodes[g][n] != nil {
				c.nodes[g][n].SetRaftLogging(enabled)
			}
		}
	}
}

// DumpTopology renders the cluster's leader layout. It is invaluable when a
// distributed test fails, and useful when watching a demo run.
func (c *Cluster) DumpTopology() []string {
	out := make([]string, 0, c.cfg.NumGroups+1)
	for g := 0; g < c.cfg.NumGroups; g++ {
		line := fmt.Sprintf("group %d:", g)
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
			line += fmt.Sprintf(" n%d:%s", n, state)
		}
		out = append(out, line)
	}
	line := "tso:"
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
		line += fmt.Sprintf(" n%d:%s", i, state)
	}
	return append(out, line)
}

// StoreSnapshot is a read of one group member's state machine, for callers that
// need to see what a replica actually holds rather than what a client reports.
type StoreSnapshot struct {
	// Committed maps keys with a published value to that value at "now".
	Committed map[txn.Key]txn.Value
	// Deleted lists keys whose newest committed version is a delete.
	Deleted []txn.Key
	// Locked maps keys with an intent lock to the transaction holding it.
	Locked map[txn.Key]txn.TxnID
}

// Snapshot reads a group member's committed state and locks directly from its
// state machine. It bypasses Raft, so it is only meaningful for inspecting a
// specific replica.
func (c *Cluster) Snapshot(group, node int) (StoreSnapshot, error) {
	store := c.GroupNode(group, node).Store()
	if store == nil {
		return StoreSnapshot{}, fmt.Errorf("cluster: group %d node %d has no state machine", group, node)
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
	return snap, nil
}
