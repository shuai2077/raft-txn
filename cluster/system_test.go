package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eliben/raft/txnkv/txn"
)

// System tests: these drive the whole stack - HTTP client, Raft replication,
// the transactional state machine, the replicated timestamp oracle and the
// two-phase-commit coordinator - against a real (if in-process) cluster, and
// they break it in the ways distributed systems actually break.

func sleepMs(n int) { time.Sleep(time.Duration(n) * time.Millisecond) }

func testCluster(t *testing.T, groups int) *Cluster {
	t.Helper()
	return NewHarness(t, TestConfig(groups, PrefixPartitioner{Groups: groups}))
}

func TestClusterStartsAndServes(t *testing.T) {
	c := testCluster(t, 3)
	c.DumpTopologyT(t)

	if c.TSOLeader() < 0 {
		t.Fatal("timestamp oracle has no leader")
	}
	for g := 0; g < 3; g++ {
		if c.LeaderOf(g) < 0 {
			t.Errorf("group %d has no leader", g)
		}
	}
}

func TestSingleKeyLifecycle(t *testing.T) {
	c := testCluster(t, 2)

	c.MustNotFind(t, "0a")
	c.PutT(t, "0a", "first")
	c.MustGet(t, "0a", "first")
	c.PutT(t, "0a", "second")
	c.MustGet(t, "0a", "second")
	c.DeleteT(t, "0a")
	c.MustNotFind(t, "0a")
}

func TestRuntimeIsolatedFromRaftCore(t *testing.T) {
	// The transaction layer must not observe Raft's internal commit channel; it
	// works through the state machine. This test is a canary: if it hangs, the
	// commit channel is not being drained and Raft has stalled.
	c := testCluster(t, 1)
	for i := 0; i < 8; i++ {
		c.PutT(t, txn.Key(fmt.Sprintf("0key%d", i)), txn.Value(fmt.Sprintf("v%d", i)))
	}
	for i := 0; i < 8; i++ {
		c.MustGet(t, txn.Key(fmt.Sprintf("0key%d", i)), txn.Value(fmt.Sprintf("v%d", i)))
	}
}

func TestMultiGroupTransactionIsAtomic(t *testing.T) {
	c := testCluster(t, 3)

	tx, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for g := 0; g < 3; g++ {
		key := txn.Key(fmt.Sprintf("%da", g))
		if err := tx.Put(key, txn.Value(fmt.Sprintf("value-%d", g))); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}
	commitTS, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Every key must be visible, and every replica of every group must agree.
	for g := 0; g < 3; g++ {
		key := txn.Key(fmt.Sprintf("%da", g))
		c.MustGet(t, key, txn.Value(fmt.Sprintf("value-%d", g)))
		c.MustSnapshotGroup(t, g, key, txn.Value(fmt.Sprintf("value-%d", g)), true)
	}

	// All three groups published at the same timestamp, which is what makes the
	// transaction atomic as a whole: a snapshot either sees all of it or none of
	// it.
	//
	// Visibility is strict (`CommitTS < snapshot`), so the transaction becomes
	// visible one tick above its commit timestamp, and is invisible *at* it. Both
	// halves of that are asserted, because they are the two sides of the same
	// rule: the sides must agree, or a reader in between would see a fragment.
	for g := 0; g < 3; g++ {
		key := txn.Key(fmt.Sprintf("%da", g))
		node := c.LeaderOf(g)
		if node < 0 {
			t.Fatalf("group %d has no leader", g)
		}
		want := txn.Value(fmt.Sprintf("value-%d", g))
		if got, found := c.MustTxnValueAt(t, g, node, commitTS+1, key); !found || got != want {
			t.Errorf("group %d key %q just above commit ts %d = (%q, found=%v); want %q",
				g, key, commitTS, got, found, want)
		}
		if _, found := c.MustTxnValueAt(t, g, node, commitTS, key); found {
			t.Errorf("group %d key %q is visible at commit ts %d; visibility must be strict", g, key, commitTS)
		}
		if _, found := c.MustTxnValueAt(t, g, node, commitTS-1, key); found {
			t.Errorf("group %d key %q is visible before commit ts %d", g, key, commitTS)
		}
	}
}

func TestMultiGroupAbortLeavesNoTrace(t *testing.T) {
	c := testCluster(t, 3)

	tx, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for g := 0; g < 3; g++ {
		if err := tx.Put(txn.Key(fmt.Sprintf("%da", g)), "x"); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := txn.Prewrite(tx); err != nil {
		t.Fatalf("prewrite: %v", err)
	}
	// The locks are now visible in every group...
	for g := 0; g < 3; g++ {
		c.WaitForStoreLocks(t, g, 1, 5*time.Second)
	}
	if err := tx.Abort(context.Background()); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	// ... and must be gone once it aborts, together with any trace of the values.
	for g := 0; g < 3; g++ {
		c.WaitForStoreLocks(t, g, 0, 5*time.Second)
		c.MustNotFind(t, txn.Key(fmt.Sprintf("%da", g)))
	}
}

func TestWriteConflictAcrossGroupsAbortsEverything(t *testing.T) {
	c := testCluster(t, 2)

	c.PutT(t, "0a", "seed")

	// T takes a snapshot and writes keys in both groups.
	tx, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put("0a", "t-a"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("1b", "t-b"); err != nil {
		t.Fatal(err)
	}

	// Somebody else wins the race on 0a.
	c.PutT(t, "0a", "winner")

	_, err = tx.Commit(context.Background())
	if err == nil {
		t.Fatal("the losing transaction committed")
	}
	if _, conflict := txn.IsWriteConflict(err); !conflict {
		t.Fatalf("got %v; want a write conflict", err)
	}

	// The winner's value stands, and the loser's untouched key was not written:
	// that is atomicity, observed from outside.
	c.MustGet(t, "0a", "winner")
	c.MustNotFind(t, "1b")
	for g := 0; g < 2; g++ {
		c.WaitForStoreLocks(t, g, 0, 5*time.Second)
	}
}

func TestConcurrentTransfersPreserveInvariant(t *testing.T) {
	c := testCluster(t, 2)

	// A classic bank-transfer invariant, with the two accounts deliberately in
	// different groups so every transfer is a cross-group transaction.
	const accounts = 2
	const start = 100
	keys := []txn.Key{"0alice", "1bob"}
	c.PutT(t, keys[0], "100")
	c.PutT(t, keys[1], "100")

	var wg sync.WaitGroup
	workers := 4
	transfersPerWorker := 5
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < transfersPerWorker; i++ {
				from, to := w%2, (w+1)%2
				_, err := c.RunTxn(context.Background(), 50, func(tx *txn.Txn) error {
					fromVal, found, err := tx.Get(context.Background(), keys[from])
					if err != nil {
						return err
					}
					if !found {
						return fmt.Errorf("account %q missing", keys[from])
					}
					amount := int64(1)
					fromNum := mustParse(t, string(fromVal))
					if fromNum < amount {
						return nil // nothing to transfer
					}
					if err := tx.Put(keys[from], txn.Value(fmt.Sprint(fromNum-amount))); err != nil {
						return err
					}
					toVal, _, err := tx.Get(context.Background(), keys[to])
					if err != nil {
						return err
					}
					toNum := mustParse(t, string(toVal))
					return tx.Put(keys[to], txn.Value(fmt.Sprint(toNum+amount)))
				})
				if err != nil {
					t.Errorf("worker %d transfer %d: %v", w, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// Two things must be true of the result, and they are different claims.
	//
	// 1. Every replica must converge on the same state, and no transaction may be
	//    left half-written. This is checked by waiting for the groups to settle:
	//    all locks resolved, all replicas agreeing.
	if err := c.WaitForSettled(30 * time.Second); err != nil {
		t.Fatalf("the cluster never settled after the concurrent transfers: %v", err)
	}

	// 2. The invariant holds at a consistent snapshot: the accounts must still add
	//    up.
	//
	//    The snapshot has to be taken *after* the settling above, and this is the
	//    subtle part. A read-only transaction reads each key through its own Raft
	//    round trip, and a timestamp does not by itself guarantee that every
	//    transaction committed below it has already become visible in every group.
	//    Auditing while transfers are still landing can therefore see one side of a
	//    transfer and not the other - read skew, which is a real property of this
	//    design and is documented in docs/02-transaction-design.md. Settling first
	//    removes that window, so what is left is a statement about the durable
	//    state rather than about the moment we happened to look.
	snapshot, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = snapshot.Abort(context.Background()) }()

	total := int64(0)
	for _, key := range keys {
		value, found, err := snapshot.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("reading %q at the audit snapshot: %v", key, err)
		}
		if !found {
			t.Fatalf("account %q vanished", key)
		}
		total += mustParse(t, string(value))
	}
	if want := int64(accounts * start); total != want {
		// Report the version chains, because "the sum is wrong" is not a
		// diagnosis. The chains say which account moved without its counterpart.
		for g, key := range keys {
			chain := c.GroupNode(g, c.LeaderOf(g)).Store().VersionsFor(key)
			t.Logf("%q: %d versions", key, len(chain))
			for _, v := range chain {
				t.Logf("   commitTS=%d startTS=%d value=%q", v.CommitTS, v.StartTS, v.Value)
			}
		}
		t.Errorf("conservation violated: total = %d, want %d", total, want)
	}
}

func mustParse(t *testing.T, s string) int64 {
	t.Helper()
	var n int64
	if _, err := fmt.Sscan(s, &n); err != nil {
		t.Fatalf("parsing %q as an integer: %v", s, err)
	}
	return n
}

func TestSnapshotIsolationIsRepeatable(t *testing.T) {
	c := testCluster(t, 2)

	c.PutT(t, "0a", "a0")
	c.PutT(t, "1b", "b0")

	// The long reader takes one snapshot for everything it reads.
	long, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if v, _, err := long.Get(context.Background(), "0a"); err != nil || v != "a0" {
		t.Fatalf("first read: got (%q, %v)", v, err)
	}

	// Several committed updates land after the snapshot.
	for i := 1; i <= 3; i++ {
		c.PutT(t, "0a", txn.Value(fmt.Sprintf("a%d", i)))
		c.PutT(t, "1b", txn.Value(fmt.Sprintf("b%d", i)))
	}

	// The long reader still sees its own snapshot, on both groups.
	if v, found := get(t, long, "0a"); !found || v != "a0" {
		t.Errorf("long reader sees %q for 0a; want a0", v)
	}
	if v, found := get(t, long, "1b"); !found || v != "b0" {
		t.Errorf("long reader sees %q for 1b; want b0", v)
	}

	// A fresh reader sees the newest state.
	c.MustGet(t, "0a", "a3")
	c.MustGet(t, "1b", "b3")
}

func get(t *testing.T, tx *txn.Txn, key txn.Key) (txn.Value, bool) {
	t.Helper()
	v, found, err := tx.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return v, found
}

func TestKillGroupLeaderAndContinue(t *testing.T) {
	c := testCluster(t, 2)

	c.PutT(t, "0a", "before")
	leader := c.LeaderOf(0)
	if leader < 0 {
		t.Fatal("group 0 has no leader")
	}
	t.Logf("crashing group 0 leader: node %d", leader)
	if err := c.CrashNode(0, leader); err != nil {
		t.Fatalf("crashing leader: %v", err)
	}

	// The surviving majority must elect a new leader and keep serving.
	c.WaitForLeadersT(t, 10*time.Second)
	newLeader := c.LeaderOf(0)
	if newLeader == leader {
		t.Fatalf("group 0 still reports node %d as leader", leader)
	}
	c.MustGet(t, "0a", "before")
	c.PutT(t, "0a", "after")
	c.MustGet(t, "0a", "after")

	// Restarting the old leader must catch it up from its own log, not from a
	// snapshot we invent: it kept its Raft storage.
	if err := c.RestartNode(0, leader); err != nil {
		t.Fatalf("restarting leader: %v", err)
	}
	sleepMs(300)
	c.MustGet(t, "0a", "after")
	c.MustSnapshotGroup(t, 0, "0a", "after", true)
}

func TestCrossGroupTransactionSurvivesLeaderCrash(t *testing.T) {
	c := testCluster(t, 2)

	c.PutT(t, "0a", "a0")
	c.PutT(t, "1b", "b0")

	// Crash one group's leader, then run a transaction spanning both groups.
	victimGroup := 1
	leader := c.LeaderOf(victimGroup)
	if leader < 0 {
		t.Fatal("group has no leader")
	}
	if err := c.CrashNode(victimGroup, leader); err != nil {
		t.Fatalf("crashing leader: %v", err)
	}
	c.WaitForLeadersT(t, 10*time.Second)

	if _, err := c.RunTxn(context.Background(), 20, func(tx *txn.Txn) error {
		if err := tx.Put("0a", "a1"); err != nil {
			return err
		}
		return tx.Put("1b", "b1")
	}); err != nil {
		t.Fatalf("cross-group transaction after a leader crash: %v", err)
	}
	c.MustGet(t, "0a", "a1")
	c.MustGet(t, "1b", "b1")
}

func TestDeadCoordinatorLocksAreRecovered(t *testing.T) {
	c := testCluster(t, 2)

	c.PutT(t, "0a", "committed")

	// A transaction prepares and then its coordinator disappears: the locks are
	// left behind with no decision. Simulate that by prewriting and dropping the
	// handle.
	dead, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := dead.Put("0a", "zombie"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Prewrite(dead); err != nil {
		t.Fatalf("prewrite: %v", err)
	}
	c.WaitForStoreLocks(t, 0, 1, 5*time.Second)

	// While the lease is live, reads must block rather than guess: the reader
	// cannot tell "about to commit" from "gone". A short deadline keeps the test
	// fast without pretending the wait does not exist.
	tx, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	shortCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	_, _, err = tx.Get(shortCtx, "0a")
	cancel()
	if err == nil {
		t.Fatal("read through a live lock succeeded; want it to block")
	}

	// Once the lease lapses, a reader reclaims the lock and sees the last
	// committed value rather than the dead transaction's value. The zombie's
	// value must never appear.
	v, found := mustWaitForRead(c, t, "0a", 5*time.Second)
	if !found || v != "committed" {
		t.Fatalf("got (%q, found=%v); want the last committed value", v, found)
	}
	for g := 0; g < 2; g++ {
		c.WaitForStoreLocks(t, g, 0, 5*time.Second)
	}
}

func TestCommittedDataSurvivesFullClusterRestart(t *testing.T) {
	c := testCluster(t, 2)

	for g := 0; g < 2; g++ {
		for i := 0; i < 3; i++ {
			c.PutT(t, txn.Key(fmt.Sprintf("%dkey%d", g, i)), txn.Value(fmt.Sprintf("value-%d-%d", g, i)))
		}
	}

	// Crash and restart every member of every group. Storage is retained, so the
	// state must come back from the replayed logs.
	for g := 0; g < 2; g++ {
		for n := 0; n < c.cfg.NodesPerGroup; n++ {
			if err := c.CrashNode(g, n); err != nil {
				t.Fatalf("crashing %d/%d: %v", g, n, err)
			}
		}
	}
	for g := 0; g < 2; g++ {
		for n := 0; n < c.cfg.NodesPerGroup; n++ {
			if err := c.RestartNode(g, n); err != nil {
				t.Fatalf("restarting %d/%d: %v", g, n, err)
			}
		}
	}
	c.WaitForLeadersT(t, 15*time.Second)

	for g := 0; g < 2; g++ {
		for i := 0; i < 3; i++ {
			c.MustGet(t, txn.Key(fmt.Sprintf("%dkey%d", g, i)), txn.Value(fmt.Sprintf("value-%d-%d", g, i)))
		}
	}
	// And the cluster still accepts writes.
	c.PutT(t, "0new", "after-restart")
	c.MustGet(t, "0new", "after-restart")
}

func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := testCluster(t, 1)

	c.PutT(t, "0a", "before")

	// Isolate one follower and then the leader, leaving a minority of one.
	leader := c.LeaderOf(0)
	if leader < 0 {
		t.Fatal("no leader")
	}
	other := (leader + 1) % c.cfg.NodesPerGroup
	c.DisconnectNode(0, other)
	c.DisconnectNode(0, leader)
	sleepMs(200)

	// The isolated leader may still believe it leads, but without a quorum it
	// cannot commit anything. A write must therefore fail rather than silently
	// succeed and be lost.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.RunTxn(ctx, 1, func(tx *txn.Txn) error {
		return tx.Put("0a", "should-not-commit")
	})
	if err == nil {
		t.Fatal("a write succeeded without a quorum")
	}

	// The failed write may have left intent locks on the isolated minority. Its
	// coordinator is gone, so a reader reclaims them once the lease lapses.

	// Reconnect the majority and confirm the old value is intact. The abandoned
	// write's locks lapse on their own, so no clock manipulation is needed.
	c.ReconnectNode(0, other)
	c.WaitForLeadersT(t, 15*time.Second)
	c.MustGetEventually(t, "0a", "before", 10*time.Second)
}

func TestTimestampOracleSurvivesLeaderChange(t *testing.T) {
	c := testCluster(t, 1)

	// Collect timestamps issued before the failover.
	before := make([]txn.Timestamp, 0, 16)
	for i := 0; i < 16; i++ {
		ts, err := c.Client().Oracle().ReadTS(context.Background())
		if err != nil {
			t.Fatalf("ReadTS: %v", err)
		}
		before = append(before, ts)
	}

	leader := c.TSOLeader()
	if leader < 0 {
		t.Fatal("oracle has no leader")
	}
	t.Logf("crashing oracle leader %d", leader)
	if err := c.CrashTSO(leader); err != nil {
		t.Fatalf("crashing oracle leader: %v", err)
	}
	c.WaitForLeadersT(t, 15*time.Second)

	// Timestamps after the failover must all be strictly greater than every
	// timestamp issued before it. Otherwise a transaction could be ordered before
	// a snapshot that already observed its writes.
	seen := make(map[txn.Timestamp]bool, len(before))
	maxBefore := txn.Timestamp(0)
	for _, ts := range before {
		seen[ts] = true
		if ts > maxBefore {
			maxBefore = ts
		}
	}
	for i := 0; i < 32; i++ {
		ts, err := c.Client().Oracle().ReadTS(context.Background())
		if err != nil {
			t.Fatalf("ReadTS after failover: %v", err)
		}
		if seen[ts] {
			t.Fatalf("timestamp %d was issued twice", ts)
		}
		if ts <= maxBefore {
			t.Fatalf("timestamp %d after failover is not greater than %d issued before it", ts, maxBefore)
		}
		seen[ts] = true
		if ts > maxBefore {
			maxBefore = ts
		}
	}
}

func TestWritesContinueAcrossOracleFailover(t *testing.T) {
	c := testCluster(t, 2)

	c.PutT(t, "0a", "v0")
	leader := c.TSOLeader()
	if err := c.CrashTSO(leader); err != nil {
		t.Fatalf("crashing oracle leader: %v", err)
	}
	c.WaitForLeadersT(t, 15*time.Second)

	if _, err := c.RunTxn(context.Background(), 30, func(tx *txn.Txn) error {
		if err := tx.Put("0a", "v1"); err != nil {
			return err
		}
		return tx.Put("1b", "v1")
	}); err != nil {
		t.Fatalf("transaction across an oracle failover: %v", err)
	}
	c.MustGet(t, "0a", "v1")
	c.MustGet(t, "1b", "v1")
}

func TestSecondaryLockIsFinishedAfterPrimaryCommits(t *testing.T) {
	c := testCluster(t, 2)

	// Build the exact crash window: prepare across both groups, seal the primary
	// (the commit point), then stop. The secondary group keeps an intent lock
	// whose owner never comes back.
	tx, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put("0a", "A"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("1b", "B"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Prewrite(tx); err != nil {
		t.Fatalf("prewrite: %v", err)
	}
	c.WaitForStoreLocks(t, 0, 1, 5*time.Second)
	c.WaitForStoreLocks(t, 1, 1, 5*time.Second)

	// Seal the primary lock. This is the transaction's commit point: from here on
	// the transaction is committed even though nobody finished it.
	primary := GuessPrimaryKey(tx.Writes())
	if primary != "0a" {
		t.Fatalf("primary key is %q; want 0a so that group 0 holds the commit point", primary)
	}
	leader0 := c.LeaderOf(0)
	if leader0 < 0 {
		t.Fatal("group 0 has no leader")
	}
	c.SealPrimaryT(t, tx, primary, 0, leader0)

	// A reader that trips over the secondary lock must finish it rather than
	// discard it. Losing it would mean losing an acknowledged write.
	reader, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	v, found, err := reader.Get(context.Background(), "1b")
	if err != nil {
		t.Fatalf("reading the secondary key: %v", err)
	}
	if !found || v != "B" {
		t.Fatalf("got (%q, found=%v); want B: the committed write must not be lost", v, found)
	}

	// And the primary key is readable too.
	if v, found := get(t, reader, "0a"); !found || v != "A" {
		t.Fatalf("primary key: got (%q, found=%v); want A", v, found)
	}
	for g := 0; g < 2; g++ {
		c.WaitForStoreLocks(t, g, 0, 5*time.Second)
	}
}

func TestGroupSetComputesPrimaryConsistently(t *testing.T) {
	// The primary key must be a pure function of the write set, or two
	// coordinators could disagree about where the commit point lives.
	part := PrefixPartitioner{Groups: 2}
	writes := []txn.WriteOp{
		{Kind: txn.OpPut, Key: "1b", Value: "x"},
		{Kind: txn.OpPut, Key: "0a", Value: "y"},
	}
	primary := GuessPrimaryKey(writes)
	if primary != "0a" {
		t.Fatalf("primary key is %q; want 0a: the smallest key is chosen deterministically", primary)
	}
	if g := part.GroupForKey(primary); g != 0 {
		t.Fatalf("primary key landed in group %d; want 0", g)
	}

	// Order of the write set must not matter.
	reversed := []txn.WriteOp{writes[1], writes[0]}
	if got := GuessPrimaryKey(reversed); got != primary {
		t.Fatalf("primary key depends on write-set order: %q vs %q", got, primary)
	}
}

func TestUnknownKeyReadIsNotAnError(t *testing.T) {
	c := testCluster(t, 2)

	tx, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	v, found, err := tx.Get(context.Background(), "0missing")
	if err != nil {
		t.Fatalf("reading an absent key: %v", err)
	}
	if found || v != "" {
		t.Fatalf("got (%q, found=%v); want not found", v, found)
	}
}

func TestOperationsOnClosedTransactionAreRejected(t *testing.T) {
	c := testCluster(t, 1)

	tx, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put("0a", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := tx.Put("0b", "w"); err == nil {
		t.Error("writing to a committed transaction succeeded")
	}
	if _, _, err := tx.Get(context.Background(), "0a"); err == nil {
		t.Error("reading a committed transaction succeeded")
	}
}

func TestConcurrentCrossGroupTransactions(t *testing.T) {
	c := testCluster(t, 2)

	const n = 6
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.RunTxn(context.Background(), 50, func(tx *txn.Txn) error {
				if err := tx.Put(txn.Key(fmt.Sprintf("0key%d", i)), txn.Value(fmt.Sprintf("v%d", i))); err != nil {
					return err
				}
				return tx.Put(txn.Key(fmt.Sprintf("1key%d", i)), txn.Value(fmt.Sprintf("v%d", i)))
			})
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		c.MustGet(t, txn.Key(fmt.Sprintf("0key%d", i)), txn.Value(fmt.Sprintf("v%d", i)))
		c.MustGet(t, txn.Key(fmt.Sprintf("1key%d", i)), txn.Value(fmt.Sprintf("v%d", i)))
	}
}

func TestAtomicityIsPreservedAcrossAPartition(t *testing.T) {
	c := testCluster(t, 2)

	// Prepare a cross-group transaction, then partition the group holding the
	// primary so the coordinator cannot finish it.
	tx, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put("0a", "A"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("1b", "B"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Prewrite(tx); err != nil {
		t.Fatalf("prewrite: %v", err)
	}
	c.WaitForStoreLocks(t, 0, 1, 5*time.Second)
	c.WaitForStoreLocks(t, 1, 1, 5*time.Second)

	leader0 := c.LeaderOf(0)
	c.DisconnectNode(0, leader0)
	sleepMs(300)

	// The coordinator can no longer reach the primary's group. It must not
	// pretend the transaction succeeded, and it must not silently give up on a
	// transaction whose locks are still held: either the commit lands or the
	// transaction stays unresolved for readers to clean up.
	_, commitErr := tx.Commit(context.Background())
	t.Logf("commit during the partition returned: %v", commitErr)

	c.ReconnectNode(0, leader0)
	c.WaitForLeadersT(t, 15*time.Second)

	// Whatever happened, the two groups must agree: either both keys exist or
	// neither does. Never one without the other. Reads retry, because the
	// transaction's locks may still be within their lease.
	_, foundA := mustWaitForRead(c, t, "0a", 10*time.Second)
	_, foundB := mustWaitForRead(c, t, "1b", 10*time.Second)
	if foundA != foundB {
		t.Fatalf("atomicity violated after a partition: 0a present=%v, 1b present=%v", foundA, foundB)
	}
	if commitErr == nil && !foundA {
		t.Fatal("the commit reported success but neither key is readable")
	}
}

func TestManyKeysAcrossGroups(t *testing.T) {
	c := testCluster(t, 3)

	const keys = 30
	for i := 0; i < keys; i++ {
		key := txn.Key(fmt.Sprintf("%dkey%02d", i%3, i))
		c.PutT(t, key, txn.Value(fmt.Sprintf("value%02d", i)))
	}
	for i := 0; i < keys; i++ {
		key := txn.Key(fmt.Sprintf("%dkey%02d", i%3, i))
		c.MustGet(t, key, txn.Value(fmt.Sprintf("value%02d", i)))
	}
	// Every group must hold a share of the keys.
	for g := 0; g < 3; g++ {
		node := c.LeaderOf(g)
		held := 0
		for _, key := range c.GroupNode(g, node).Store().Keys() {
			_ = key
			held++
		}
		if held == 0 {
			t.Errorf("group %d holds no keys", g)
		}
	}
}

func TestErrorsAreTyped(t *testing.T) {
	c := testCluster(t, 1)

	c.PutT(t, "0a", "v")

	// A conflicting transaction must produce a typed conflict, not a stringly
	// typed error: applications branch on this, and matching on text would be
	// brittle.
	//
	// The two writers take their snapshots in order, so T1 is guaranteed to
	// commit after T2's snapshot and before T2's prewrite. Without that ordering
	// the test would be flaky, because which of two concurrent writers loses is
	// genuinely a race.
	t1, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t2, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if err := t1.Put("0a", "t1"); err != nil {
		t.Fatal(err)
	}
	if _, err := t1.Commit(context.Background()); err != nil {
		t.Fatalf("first writer: %v", err)
	}

	// T2's snapshot predates T1's commit, so this write must be refused.
	if err := t2.Put("0a", "t2"); err != nil {
		t.Fatal(err)
	}
	_, err = t2.Commit(context.Background())
	var conflict *txn.WriteConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("got %v; want *txn.WriteConflictError", err)
	}
	if conflict.Key != "0a" {
		t.Errorf("conflict reported on %q; want 0a", conflict.Key)
	}
	if conflict.CommitTS == 0 {
		t.Error("conflict does not name the interfering commit timestamp")
	}

	// The winner's value stands and the loser wrote nothing.
	c.MustGet(t, "0a", "t1")
}
