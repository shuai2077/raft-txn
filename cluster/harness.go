package cluster

import (
	"context"
	"fmt"
	"net"
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

// TestReplicationTimeout is how long tests wait for every live replica of a group
// to converge on a state before failing.
//
// It is deliberately far longer than any replication or recovery step should take,
// because none of these waits is a performance assertion. A replica that has not
// caught up is exhibiting ordinary replication lag; failing on it turns a loaded
// machine into a spurious failure. That happened repeatedly during stress testing -
// two replicas held a lock and the third had not applied the entry yet - so the
// budget is generous and uniform rather than tuned per call site.
const TestReplicationTimeout = 30 * time.Second

// testPortCursor spaces successive test clusters apart so that a harness which
// outlives a subtest cannot collide with the next one's ports.
//
// Spacing alone is not enough. Ports released by a finished cluster linger briefly
// in TIME_WAIT, and a test that starts a cluster, shuts it down, and starts another
// can land on one of them. That is a property of the operating system, not of how
// far apart the bases are, so the allocator verifies each port is actually bindable
// and skips the ones that are not.
var testPortCursor = testHTTPBase

// reservePorts claims count consecutive bindable ports, advancing the shared cursor
// past them.
//
// It binds each candidate to prove it is free, and skips a port that is not. The
// probe listener is closed before the port is handed out, which leaves a small race
// with another process; in exchange it turns a class of flaky "bind: Only one usage
// of each socket address" failures into a reliable skip.
func reservePorts(count int) int {
	const maxAttempts = 64
	for attempt := 0; attempt < maxAttempts; attempt++ {
		base := testPortCursor
		testPortCursor += 256
		if allBindable(base, count) {
			return base
		}
	}
	// Nothing available in the scanned range. Hand back a base anyway so the
	// config is usable; the caller will surface a clear bind error rather than a
	// hang, and the range is wide enough that this is effectively unreachable.
	return testPortCursor
}

// allBindable reports whether count consecutive ports starting at base can all be
// bound right now.
func allBindable(base, count int) bool {
	listeners := make([]net.Listener, 0, count)
	defer func() {
		for _, ln := range listeners {
			_ = ln.Close()
		}
	}()
	for i := 0; i < count; i++ {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+i))
		if err != nil {
			return false
		}
		listeners = append(listeners, ln)
	}
	return true
}

// TestConfigPorts is how many ports one TestConfig cluster needs: a node and a
// Raft peer listener for every group member, plus the same for every oracle
// member, and a little slack.
func TestConfigPorts(numGroups int) int {
	return 2*(numGroups*3+3) + 4
}

// TestConfig returns a Config suitable for a test: small groups, three-node Raft
// per group, a three-member oracle, and a short lock lease.
func TestConfig(numGroups int, partitioner txn.Partitioner) Config {
	base := reservePorts(TestConfigPorts(numGroups))
	return Config{
		NumGroups:     numGroups,
		NodesPerGroup: 3,
		TSONodes:      3,
		WindowSize:    64,
		LockTTL:       TestLockTTL,
		HTTPBase:      base,
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

// WaitForSettled blocks until every live replica of every group has no intent
// locks and all replicas of a group agree on the key sets and version counts.
//
// It answers the question "has everything in flight finished, and does the
// cluster agree?" without any assumption about timing. Tests that assert an
// invariant over several keys need it, because a read-only transaction's snapshot
// does not by itself guarantee that every transaction committed below it has
// become visible everywhere yet (see docs/02-transaction-design.md).
func (c *Cluster) WaitForSettled(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if err := c.settled(); err != nil {
			lastErr = err
		} else {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster did not settle within %s: %w", timeout, lastErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (c *Cluster) settled() error {
	for g := 0; g < c.cfg.NumGroups; g++ {
		// The reference is the first live replica of the group.
		var refKeys []txn.Key
		var refCounts []int
		for n := 0; n < c.cfg.NodesPerGroup; n++ {
			if !c.alive[g][n] || !c.connected[g][n] {
				continue
			}
			store := c.GroupNode(g, n).Store()
			if store == nil {
				continue
			}
			if locks := store.Locks(); len(locks) != 0 {
				return fmt.Errorf("group %d node %d still holds %d intent locks", g, n, len(locks))
			}
			keys := store.Keys()
			counts := make([]int, len(keys))
			for i, key := range keys {
				counts[i] = len(store.VersionsFor(key))
			}
			if refKeys == nil {
				refKeys, refCounts = keys, counts
				continue
			}
			if len(keys) != len(refKeys) {
				return fmt.Errorf("group %d: replicas disagree on the key set (%d vs %d keys)",
					g, len(keys), len(refKeys))
			}
			for i := range keys {
				if keys[i] != refKeys[i] || counts[i] != refCounts[i] {
					return fmt.Errorf("group %d: node %d has key %q with %d versions; reference has %q with %d",
						g, n, keys[i], counts[i], refKeys[i], refCounts[i])
				}
			}
		}
	}
	return nil
}

// Barrier takes a read barrier on one group and returns the group's applied
// commit-timestamp watermark once the barrier has been applied.
//
// The watermark is the observability half of the barrier's guarantee: a reader
// that has taken a barrier can check that the group has already applied every
// commit timestamp it is about to read at. See docs/03-read-barrier.md.
func (c *Cluster) Barrier(group int) (txn.Timestamp, error) {
	return c.BarrierCtx(context.Background(), group)
}

// BarrierCtx is Barrier with a caller-supplied context.
func (c *Cluster) BarrierCtx(ctx context.Context, group int) (txn.Timestamp, error) {
	participant, err := c.clients.ParticipantFor(txn.GroupID(group))
	if err != nil {
		return 0, err
	}
	if err := participant.Barrier(ctx); err != nil {
		return 0, fmt.Errorf("barrier on group %d: %w", group, err)
	}
	return c.GroupNode(group, c.LeaderOf(group)).Store().AppliedCommitTS(), nil
}

// Barriers takes a read barrier on every group, which is what a transaction that
// reads across all of them needs. It returns the lowest watermark observed, so a
// caller can assert the whole cluster has reached it.
func (c *Cluster) Barriers() (txn.Timestamp, error) {
	lowest := txn.Timestamp(0)
	for g := 0; g < c.NumGroups(); g++ {
		watermark, err := c.Barrier(g)
		if err != nil {
			return 0, err
		}
		if g == 0 || watermark < lowest {
			lowest = watermark
		}
	}
	return lowest, nil
}

// WaitForQuiescent blocks until the cluster has been settled continuously for
// `stable`, then returns.
//
// This is stronger than WaitForSettled, and the difference is exactly the read
// barrier's remaining gap. Settling proves every replica agrees *now*; it does not
// prove that no transaction is sitting between having received its commit
// timestamp and having that write applied in every group. A transaction in that
// window has a commit timestamp below any snapshot taken next, yet part of it is
// invisible - so a read-only transaction spanning two groups can see one side of
// it. Requiring a quiet period long enough for such a transaction to finish closes
// that window, which is what a cross-key conservation check needs.
//
// See docs/03-read-barrier.md for why the per-group barrier alone cannot close it.
func (c *Cluster) WaitForQuiescent(stable, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	quietSince := time.Time{}
	var lastErr error
	for {
		if err := c.settled(); err != nil {
			lastErr = err
			quietSince = time.Time{}
		} else if quietSince.IsZero() {
			quietSince = time.Now()
		} else if time.Since(quietSince) >= stable {
			return nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("cluster was never quiet for %s within %s: %w", stable, timeout, lastErr)
			}
			return fmt.Errorf("cluster was never quiet for %s within %s", stable, timeout)
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
//
// The labels describe each member's role, not only its health:
//
//	crashed      deliberately stopped
//	partitioned  alive but disconnected from its peers
//	LEADER       the group's current leader
//	follower     up, connected, and simply not the leader
//
// "follower" is not a decorative case. Without it every healthy follower fell
// through to a "down" default, so a perfectly healthy cluster printed as one
// leader and two "down" members per group - which is alarming, and wrong.
func (c *Cluster) DumpTopology() []string {
	out := make([]string, 0, c.cfg.NumGroups+1)
	for g := 0; g < c.cfg.NumGroups; g++ {
		line := fmt.Sprintf("group %d:", g)
		for n := 0; n < c.cfg.NodesPerGroup; n++ {
			state := "follower"
			switch {
			case !c.alive[g][n]:
				state = "crashed"
			case !c.connected[g][n]:
				state = "partitioned"
			case c.nodes[g][n] == nil:
				state = "down"
			case c.nodes[g][n].IsLeader():
				state = "LEADER"
			}
			line += fmt.Sprintf(" n%d:%s", n, state)
		}
		out = append(out, line)
	}
	line := "tso:"
	for i := range c.tsoNodes {
		state := "follower"
		switch {
		case !c.tsoAlive[i]:
			state = "crashed"
		case !c.tsoConnected[i]:
			state = "partitioned"
		case c.tsoNodes[i] == nil:
			state = "down"
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
