package txn

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Tests for the transaction coordinator: two-phase commit across groups,
// snapshot reads, isolation, conflict handling and recovery from a coordinator
// that dies mid-commit.

// TestVerifyLocksHeldRefusesAMissingSecondaryLock tests the guard that stops a
// lapsed lease from becoming a lost write.
//
// It is a unit test of the check itself, which is where the guarantee lives: the
// end-to-end scenario can be caught earlier by a different guard, but this pins
// the behaviour the coordinator depends on - if any intent lock is gone before the
// commit point is written, the commit must be refused with a
// LockRolledBackError rather than proceeding.
func TestVerifyLocksHeldRefusesAMissingSecondaryLock(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)

	tx := tc.Begin()
	tc.MustPut(tx, "0a", "A")
	tc.MustPut(tx, "1b", "B")

	prep, err := tc.client.prewriteWrites(context.Background(), tx.id, tx.startTS, tx.writes)
	if err != nil {
		t.Fatalf("prewrite: %v", err)
	}
	if len(prep.groups) != 2 {
		t.Fatalf("prepared %d groups, want 2", len(prep.groups))
	}

	// With both locks in place the check passes.
	if err := tx.verifyLocksHeld(context.Background(), prep); err != nil {
		t.Fatalf("verify with both locks held: %v", err)
	}

	// Discard the secondary lock, as a reader does to a lapsed one whose primary is
	// still unsealed.
	secondary := GroupID(1)
	if err := tc.groups[secondary].Rollback(context.Background(), RollbackRequest{
		TxnID: tx.id, ClientID: tc.client.clientID, RequestID: 99,
		StartTS: tx.startTS, Keys: []Key{"1b"},
	}); err != nil {
		t.Fatalf("rolling back the secondary lock: %v", err)
	}

	// The check must now fail and name the key that is gone.
	err = tx.verifyLocksHeld(context.Background(), prep)
	if err == nil {
		t.Fatal("verify passed although the secondary lock was gone")
	}
	var rolledBack *LockRolledBackError
	if !errors.As(err, &rolledBack) {
		t.Fatalf("got %v; want a LockRolledBackError", err)
	}
	if rolledBack.Key != "1b" {
		t.Errorf("the error names key %q; want %q", rolledBack.Key, "1b")
	}
	if rolledBack.TxnID != tx.id {
		t.Errorf("the error names txn %d; want %d", rolledBack.TxnID, tx.id)
	}
}

// readCommitted reads a key at "now" straight out of one group's state machine,
// bypassing the transaction client. It is how tests observe what a group really
// stores.
func readCommitted(t *testing.T, tc *testCluster, group GroupID, key Key) (Value, bool) {
	t.Helper()
	store := tc.Group(group)
	// A read at the maximum timestamp sees the newest version.
	v, found := store.latestVersionAt(key, ^Timestamp(0))
	return v.Value, found
}

// lockTxn reports which transaction (if any) holds the lock on key in group.
func lockTxn(tc *testCluster, group GroupID, key Key) (TxnID, bool) {
	lock, ok := tc.Group(group).locks[key]
	if !ok {
		return 0, false
	}
	return lock.TxnID, true
}

func TestBeginAssignsUniqueSnapshots(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 3)

	t1 := tc.Begin()
	t2 := tc.Begin()
	if t1.StartTS() >= t2.StartTS() {
		t.Errorf("start timestamps must increase: %d then %d", t1.StartTS(), t2.StartTS())
	}
	if t1.ID() == t2.ID() {
		t.Errorf("transactions share an ID: %d", t1.ID())
	}
	if t1.State() != "active" {
		t.Errorf("state=%s, want active", t1.State())
	}
}

func TestSingleGroupCommitAndRead(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)

	txn := tc.Begin()
	tc.MustPut(txn, "0a", "alpha")
	commitTS := tc.MustCommit(txn)
	if commitTS == 0 {
		t.Fatal("commit returned timestamp 0")
	}
	if got, found := readCommitted(t, tc, 0, "0a"); !found || got != "alpha" {
		t.Fatalf("group 0 holds (%q, %v), want alpha", got, found)
	}

	// A later transaction reads the committed value.
	reader := tc.Begin()
	if v, found := tc.MustGet(reader, "0a"); !found || v != "alpha" {
		t.Fatalf("read (%q, %v), want alpha", v, found)
	}
}

func TestMultiGroupCommitIsAtomic(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 3)

	txn := tc.Begin()
	tc.MustPut(txn, "0a", "A")
	tc.MustPut(txn, "1b", "B")
	tc.MustPut(txn, "2c", "C")
	commitTS := tc.MustCommit(txn)

	for group, want := range map[GroupID]string{0: "A", 1: "B", 2: "C"} {
		key := Key(string('0'+byte(group)) + string('a'+byte(group)))
		got, found := readCommitted(t, tc, group, key)
		if !found || got != Value(want) {
			t.Errorf("group %d key %q: got (%q, %v), want %q", group, key, got, found, want)
		}
		// Every group publishes at the same timestamp, which is what makes the
		// transaction atomic as a whole.
		versions := tc.Group(group).versions[key]
		if len(versions) != 1 || versions[0].CommitTS != commitTS {
			t.Errorf("group %d key %q versions: %+v, want a single version at %d",
				group, key, versions, commitTS)
		}
	}
}

func TestReadYourWritesBeforeCommit(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)

	txn := tc.Begin()
	tc.MustPut(txn, "0a", "draft")
	if v, found := tc.MustGet(txn, "0a"); !found || v != "draft" {
		t.Fatalf("read-your-writes: got (%q, %v), want draft", v, found)
	}
	tc.MustDelete(txn, "0a")
	if v, found := tc.MustGet(txn, "0a"); found {
		t.Fatalf("after buffered delete: got (%q, %v), want not-found", v, found)
	}

	// Nothing has reached any group yet.
	if _, found := readCommitted(t, tc, 0, "0a"); found {
		t.Fatal("an uncommitted write is visible in the store")
	}
}

func TestSnapshotIsolationAcrossGroups(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)

	// Seed two keys in different groups.
	seed := tc.Begin()
	tc.MustPut(seed, "0a", "a0")
	tc.MustPut(seed, "1b", "b0")
	tc.MustCommit(seed)

	// A long-running reader takes a snapshot.
	reader := tc.Begin()
	if v, _ := tc.MustGet(reader, "0a"); v != "a0" {
		t.Fatalf("reader sees %q, want a0", v)
	}

	// Meanwhile two other transactions update both keys.
	w1 := tc.Begin()
	tc.MustPut(w1, "0a", "a1")
	tc.MustPut(w1, "1b", "b1")
	tc.MustCommit(w1)

	w2 := tc.Begin()
	tc.MustPut(w2, "0a", "a2")
	tc.MustPut(w2, "1b", "b2")
	tc.MustCommit(w2)

	// The reader must still see its own snapshot for both keys: this is
	// repeatable read, and it is why the reader's snapshot timestamp is stamped
	// into every read.
	if v, _ := tc.MustGet(reader, "0a"); v != "a0" {
		t.Errorf("reader sees %q for 0a, want its snapshot value a0", v)
	}
	if v, _ := tc.MustGet(reader, "1b"); v != "b0" {
		t.Errorf("reader sees %q for 1b, want its snapshot value b0", v)
	}

	// A new reader sees the latest committed state.
	fresh := tc.Begin()
	if v, _ := tc.MustGet(fresh, "0a"); v != "a2" {
		t.Errorf("fresh reader sees %q, want a2", v)
	}
	if v, _ := tc.MustGet(fresh, "1b"); v != "b2" {
		t.Errorf("fresh reader sees %q, want b2", v)
	}
}

func TestWriteConflictAbortsWholeTransaction(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)

	seed := tc.Begin()
	tc.MustPut(seed, "0a", "v0")
	tc.MustCommit(seed)

	// T holds a snapshot that predates the next commit, and writes 0a and 1b.
	tx := tc.Begin()
	tc.MustPut(tx, "0a", "tx-a")
	tc.MustPut(tx, "1b", "tx-b")

	// Somebody else commits 0a in the meantime.
	other := tc.Begin()
	tc.MustPut(other, "0a", "other-a")
	tc.MustCommit(other)

	// T's commit must fail, and must not leave a partial write anywhere: the
	// conflict is detected during prewrite, before any version is published.
	_, err := tx.Commit(context.Background())
	var wc *WriteConflictError
	if !errors.As(err, &wc) {
		t.Fatalf("got err %v, want *WriteConflictError", err)
	}
	if wc.Key != "0a" {
		t.Errorf("conflict reported on %q, want 0a", wc.Key)
	}

	if v, _ := readCommitted(t, tc, 0, "0a"); v != "other-a" {
		t.Errorf("0a=%q, want other-a (the winner)", v)
	}
	// Critically, 1b must not have been written, even though only 0a conflicted.
	if _, found := readCommitted(t, tc, 1, "1b"); found {
		t.Error("1b was published even though the transaction failed")
	}
	// No locks may survive a failed commit.
	for _, g := range []GroupID{0, 1} {
		if locks := tc.Group(g).LockedKeys(); len(locks) != 0 {
			t.Errorf("group %d left locks behind: %v", g, locks)
		}
	}
}

func TestConcurrentWritersOneWins(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)

	// Both transactions start from the same snapshot and write the same key.
	t1 := tc.Begin()
	t2 := tc.Begin()
	tc.MustPut(t1, "0k", "one")
	tc.MustPut(t2, "0k", "two")

	ts1 := tc.MustCommit(t1)

	_, err := t2.Commit(context.Background())
	if err == nil {
		t.Fatal("the second writer committed; exactly one must lose")
	}
	if _, isConflict := IsWriteConflict(err); !isConflict {
		t.Fatalf("second writer failed with %v, want a write conflict", err)
	}

	if v, _ := readCommitted(t, tc, 0, "0k"); v != "one" {
		t.Errorf("value=%q, want one", v)
	}
	if ts1 == 0 {
		t.Error("winner did not report a commit timestamp")
	}
	if t2.State() != "aborted" {
		t.Errorf("loser state=%s, want aborted", t2.State())
	}
}

func TestReadBlocksOnUncommittedThenSeesCommit(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)
	tc.SetLockTTL(time.Hour) // the writer is alive, so its lock must not expire

	writer := tc.Begin()
	tc.MustPut(writer, "0a", "pending")

	// Prewrite without committing: the lock is now visible.
	tc.PrewriteAll(writer)

	reader := tc.Begin()
	// The read must not succeed, and it must not silently treat the key as
	// absent either: the writer's value is pending, so returning "not found"
	// would be a lost-update bug. A short deadline keeps the test fast.
	if _, found, err := tc.GetWithin(reader, "0a", 80*time.Millisecond); err == nil {
		t.Fatalf("read succeeded (found=%v); want it to block on the lock", found)
	}

	// Let the writer finish, in the background, shortly after the reader gave up.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = writer.Commit(context.Background())
	}()
	time.Sleep(150 * time.Millisecond)

	fresh := tc.Begin()
	if v, found := tc.MustGet(fresh, "0a"); !found || v != "pending" {
		t.Fatalf("read after commit: got (%q, %v), want pending", v, found)
	}
}

func TestDeadWriterLockIsResolvedAndReadProceeds(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)
	// Start with a generous lease so the zombie lock can be observed, then let it
	// lapse by moving the harness clock past the deadline it was stamped with.
	tc.SetLockTTL(time.Hour)
	tc.FreezeClock(1_000_000)

	// A writer that will never commit: simulate its death by abandoning the
	// transaction after prewrite.
	dead := tc.Begin()
	tc.MustPut(dead, "0a", "zombie")
	tc.PrewriteAll(dead)
	if locks := tc.Group(0).LockedKeys(); len(locks) != 1 {
		t.Fatalf("locks=%v, want the zombie lock", locks)
	}

	tc.SetLockTTL(50 * time.Millisecond)
	tc.Advance(time.Hour)

	reader := tc.Begin()
	if _, found, err := reader.Get(context.Background(), "0a"); err != nil {
		t.Fatalf("read of a key locked by a dead writer: %v", err)
	} else if found {
		t.Error("the dead writer's value became visible")
	}

	// Recovery must have cleaned the lock up.
	if locks := tc.Group(0).LockedKeys(); len(locks) != 0 {
		t.Fatalf("locks after recovery: %v, want none", locks)
	}
	if tc.resolver.Calls() == 0 {
		t.Error("the resolver was never consulted")
	}
	if st := tc.client.Stats(); st.StaleLocksAborted == 0 {
		t.Errorf("stats=%+v, want a recorded stale-lock abort", st)
	}

	// The key is writable again.
	next := tc.Begin()
	tc.MustPut(next, "0a", "fresh")
	tc.MustCommit(next)
	if v, _ := readCommitted(t, tc, 0, "0a"); v != "fresh" {
		t.Fatalf("value=%q, want fresh", v)
	}
}

func TestSecondaryLockOfCommittedTransactionIsFinished(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)
	// Keep the locks alive for the whole test: the point is that recovery works
	// even when the locks have *not* expired, because the transaction committed.
	tc.SetLockTTL(time.Hour)

	// Build the exact crash window: prewrite across two groups, seal the primary,
	// then stop. A real coordinator would now publish the secondary; we simulate
	// its death instead.
	tx := tc.Begin()
	tc.MustPut(tx, "0a", "A")
	tc.MustPut(tx, "1b", "B")

	prep, err := tc.client.prewriteWrites(context.Background(), tx.id, tx.startTS, tx.writes)
	if err != nil {
		t.Fatalf("prewrite: %v", err)
	}
	if prep.primaryGroup != 0 {
		t.Fatalf("primary group=%d, want 0 so that the primary is the first key", prep.primaryGroup)
	}
	// Seal the primary, which is the transaction's commit point, and then stop:
	// the secondary group is left holding an unsealed intent.
	//
	// Take the commit timestamp from the transaction's own oracle rather than
	// inventing one, so the oracle's clock actually advances past it - otherwise a
	// reader started afterwards could be handed the same timestamp, and the strict
	// visibility rule would (correctly) hide the write from it.
	cts, err := tc.oracle.CommitTS(context.Background(), tx.startTS)
	if err != nil {
		t.Fatalf("obtaining a commit timestamp: %v", err)
	}
	if err := tc.groups[0].Seal(context.Background(), "0a", tx.id, tx.startTS, cts); err != nil {
		t.Fatalf("seal primary: %v", err)
	}

	// The secondary group still holds a lock, and its own lock record was never
	// sealed, so only the primary can reveal the decision.
	if locks := tc.Group(1).LockedKeys(); len(locks) != 1 {
		t.Fatalf("group 1 locks=%v, want the secondary intent", locks)
	}

	// A reader that trips over the secondary lock must finish the write, not
	// discard it.
	//
	// The reader starts *after* the seal on purpose. Visibility is strict, so a
	// snapshot taken below the commit timestamp would legitimately not see the
	// recovered write - which is correct behaviour, and not what this test is
	// about. Recovery must be observable, so the reader's snapshot is above the
	// commit point.
	reader := tc.Begin()
	if reader.StartTS() <= cts {
		t.Fatalf("reader snapshot %d is not above the commit timestamp %d", reader.StartTS(), cts)
	}
	v, found, err := reader.Get(context.Background(), "1b")
	if err != nil {
		t.Fatalf("read of the secondary key: %v", err)
	}
	if !found || v != "B" {
		t.Fatalf("read (%q, %v), want B: the committed write must not be lost", v, found)
	}
	if locks := tc.Group(1).LockedKeys(); len(locks) != 0 {
		t.Errorf("group 1 locks after recovery: %v, want none", locks)
	}
	if st := tc.client.Stats(); st.ForeignLocksFinished == 0 {
		t.Errorf("stats=%+v, want a recorded finished foreign lock", st)
	}

	// The primary key is readable too.
	if v, found := tc.MustGet(reader, "0a"); !found || v != "A" {
		t.Fatalf("read 0a: (%q, %v), want A", v, found)
	}
}

func TestAbortReleasesLocks(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)
	// Locks must not expire on their own here: the abort is what has to clean up.
	tc.SetLockTTL(time.Hour)

	tx := tc.Begin()
	tc.MustPut(tx, "0a", "A")
	tc.MustPut(tx, "1b", "B")
	tc.PrewriteAll(tx)
	if err := tx.Abort(context.Background()); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if tx.State() != "aborted" {
		t.Errorf("state=%s, want aborted", tx.State())
	}
	for _, g := range []GroupID{0, 1} {
		if locks := tc.Group(g).LockedKeys(); len(locks) != 0 {
			t.Errorf("group %d locks after abort: %v", g, locks)
		}
	}
	// The values never appeared.
	for group, key := range map[GroupID]Key{0: "0a", 1: "1b"} {
		if _, found := readCommitted(t, tc, group, key); found {
			t.Errorf("key %q became visible after an abort", key)
		}
	}
}

func TestCommitFailsWhenOracleRefusesDecideTimestamp(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)

	tx := tc.Begin()
	tc.MustPut(tx, "0a", "A")
	tc.MustPut(tx, "1b", "B")

	// The oracle fails only the CommitTS call, which happens after prewrite.
	tc.oracle.FailNext(errors.New("oracle unavailable"))

	if _, err := tx.Commit(context.Background()); err == nil {
		t.Fatal("commit succeeded despite the oracle failing")
	}
	// Nothing may become visible, and no lock may survive: the coordinator knows
	// the transaction is not going to commit, so it releases what it took.
	for group, key := range map[GroupID]Key{0: "0a", 1: "1b"} {
		if _, found := readCommitted(t, tc, group, key); found {
			t.Errorf("key %q became visible despite a failed commit", key)
		}
		if locks := tc.Group(group).LockedKeys(); len(locks) != 0 {
			t.Errorf("group %d kept locks %v after the commit failed", group, locks)
		}
	}
}

func TestCommitOfReadOnlyTransactionIsFree(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 1)

	tx := tc.Begin()
	if _, _, err := tx.Get(context.Background(), "nope"); err != nil {
		t.Fatalf("get: %v", err)
	}
	ts, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if ts != tx.StartTS() {
		t.Errorf("commit ts=%d, want the start ts %d for a read-only transaction", ts, tx.StartTS())
	}
	if st := tc.Group(0).Stats(); st.Versions != 0 || st.Locks != 0 {
		t.Errorf("a read-only transaction touched the state machine: %+v", st)
	}
}

func TestDeleteThenReadAcrossTransactions(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)

	seed := tc.Begin()
	tc.MustPut(seed, "0a", "alive")
	tc.MustCommit(seed)

	del := tc.Begin()
	tc.MustDelete(del, "0a")
	tc.MustCommit(del)

	fresh := tc.Begin()
	if v, found := tc.MustGet(fresh, "0a"); found {
		t.Fatalf("got (%q, %v), want not-found after delete", v, found)
	}

	// The delete is an MVCC record, not an erasure: the chain still holds the
	// original value, so a historical read can recover it. This is what keeps a
	// long-running snapshot consistent.
	versions := tc.Group(0).versions["0a"]
	if len(versions) != 2 {
		t.Fatalf("versions=%+v, want a put and a delete", versions)
	}
	if versions[1].Kind != OpDelete {
		t.Errorf("second version kind=%s, want delete", versions[1].Kind)
	}
	// A historical read just above the first version's commit timestamp sees it;
	// visibility is strict, so reading *at* that timestamp would be too early.
	historical, foundHistorical := tc.Group(0).latestVersionAt("0a", versions[0].CommitTS+1)
	if !foundHistorical || historical.Value != "alive" {
		t.Errorf("historical read: got (%q, %v), want alive", historical.Value, foundHistorical)
	}
}

func TestAbortAfterCommitIsRefused(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 1)

	tx := tc.Begin()
	tc.MustPut(tx, "0a", "v")
	tc.MustCommit(tx)

	if err := tx.Abort(context.Background()); err == nil {
		t.Fatal("aborting a committed transaction succeeded")
	}
	if v, _ := readCommitted(t, tc, 0, "0a"); v != "v" {
		t.Errorf("value=%q, want v", v)
	}
}

func TestOperationsAfterCommitAreRefused(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 1)

	tx := tc.Begin()
	tc.MustPut(tx, "0a", "v")
	tc.MustCommit(tx)

	if err := tx.Put("0b", "x"); err == nil {
		t.Error("Put on a committed transaction succeeded")
	}
	if _, _, err := tx.Get(context.Background(), "0a"); err == nil {
		t.Error("Get on a committed transaction succeeded")
	}
	if _, err := tx.Commit(context.Background()); err != nil {
		t.Errorf("re-committing a committed transaction: %v, want success", err)
	}
}

func TestCommitTwiceIsIdempotent(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 2)

	tx := tc.Begin()
	tc.MustPut(tx, "0a", "v")
	ts1 := tc.MustCommit(tx)
	ts2 := tc.MustCommit(tx)
	if ts1 != ts2 {
		t.Errorf("commit returned %d then %d; want the same timestamp", ts1, ts2)
	}
	if got := len(tc.Group(0).versions["0a"]); got != 1 {
		t.Errorf("got %d versions after two commits, want 1", got)
	}
}

func TestStartTimestampFailurePropagates(t *testing.T) {
	tc := newTestCluster(t, groupForPrefix{}, 1)
	tc.oracle.FailNext(errors.New("no timestamps"))

	if _, err := tc.client.Begin(context.Background()); err == nil {
		t.Fatal("Begin succeeded while the oracle was failing")
	}
}

func TestHashPartitionerIsDeterministicAndBalanced(t *testing.T) {
	p := NewHashPartitioner(4)
	seen := map[GroupID]int{}
	for i := 0; i < 400; i++ {
		key := Key(string(rune('a'+i%26)) + string(rune('a'+i/26%26)))
		g1 := p.GroupForKey(key)
		g2 := p.GroupForKey(key)
		if g1 != g2 {
			t.Fatalf("GroupForKey(%q) is not deterministic: %d vs %d", key, g1, g2)
		}
		if g1 < 0 || int(g1) >= 4 {
			t.Fatalf("GroupForKey(%q)=%d, out of range", key, g1)
		}
		seen[g1]++
	}
	// With 400 distinct-ish keys over 4 groups, every group should get a share.
	if len(seen) != 4 {
		t.Errorf("keys landed in %d groups, want all 4: %v", len(seen), seen)
	}
}

func TestStaticPartitionerUsesLongestPrefix(t *testing.T) {
	p := &StaticPartitioner{
		Prefixes: map[string]GroupID{"user:": 1, "user:admin": 2},
		Default:  0,
	}
	cases := map[Key]GroupID{
		"other":        0,
		"user:bob":     1,
		"user:admin":   2,
		"user:admin:x": 2,
	}
	for key, want := range cases {
		if got := p.GroupForKey(key); got != want {
			t.Errorf("GroupForKey(%q)=%d, want %d", key, got, want)
		}
	}
}

func TestLocalOracleIsMonotonicAndRejectsStaleCommit(t *testing.T) {
	o := NewLocalOracle(100)
	ctx := context.Background()

	first, err := o.ReadTS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := o.ReadTS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first != 101 || second != 102 {
		t.Errorf("got %d then %d, want 101 then 102", first, second)
	}

	// A commit timestamp must always exceed the transaction's start timestamp.
	cts, err := o.CommitTS(ctx, second)
	if err != nil {
		t.Fatalf("CommitTS: %v", err)
	}
	if cts <= second {
		t.Errorf("commit ts %d <= start ts %d", cts, second)
	}
	if _, err := o.CommitTS(ctx, cts+1000); err == nil {
		t.Error("CommitTS accepted a start timestamp from the future")
	}
}

func TestOracleFuncAdapter(t *testing.T) {
	var n Timestamp
	o := OracleFunc{
		ReadNow:   func() Timestamp { n++; return n },
		CommitNow: func() Timestamp { n += 10; return n },
	}
	ctx := context.Background()
	if ts, _ := o.ReadTS(ctx); ts != 1 {
		t.Errorf("ReadTS=%d, want 1", ts)
	}
	if ts, _ := o.CommitTS(ctx, 1); ts != 11 {
		t.Errorf("CommitTS=%d, want 11", ts)
	}
	// A degenerate oracle must be rejected, not silently trusted.
	bad := OracleFunc{ReadNow: func() Timestamp { return 5 }, CommitNow: func() Timestamp { return 5 }}
	if _, err := bad.CommitTS(ctx, 5); err == nil {
		t.Error("CommitTS accepted a timestamp equal to startTS")
	}
}
