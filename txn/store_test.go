package txn

import (
	"errors"
	"testing"
	"time"
)

// Tests for the per-group replicated state machine: MVCC visibility, intent
// locks, conflict detection, exactly-once retries and lock resolution.

func TestStorePrewriteCommitAndRead(t *testing.T) {
	dl := newDirectLog(0)

	res := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, ClientID: 7, RequestID: 1, StartTS: 10, Primary: "a",
		Ops: []WriteOp{NewPut("a", "v1")},
	}}).(TxnResult)
	if res.Status != StatusOK || !res.Prepared {
		t.Fatalf("prewrite: got status=%s prepared=%v, want OK/true", res.Status, res.Prepared)
	}

	// Before commit the write is invisible: MVCC has no version for the key yet.
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 50, Key: "a"}}).(TxnResult); r.Found {
		t.Fatalf("read before commit found a value (%q); want none", r.Value)
	}
	// The intent lock must be visible to other transactions.
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 50, Key: "a"}}).(TxnResult); r.Status != StatusLocked {
		t.Fatalf("read under lock: got status=%s, want locked", r.Status)
	}

	res = dl.store.Apply(TxnCommand{Kind: CmdCommit, CommitArgs: &CommitRequest{
		TxnID: 1, ClientID: 7, RequestID: 2, StartTS: 10, CommitTS: 20, Keys: []Key{"a"},
	}}).(TxnResult)
	if res.Status != StatusOK || res.CommitTS != 20 || res.KeysFinalized != 1 {
		t.Fatalf("commit: got %+v, want OK/20/1 key", res)
	}

	r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 50, Key: "a"}}).(TxnResult)
	if r.Status != StatusOK || !r.Found || r.Value != "v1" {
		t.Fatalf("read after commit: got %+v, want found v1", r)
	}

	// A snapshot taken before the commit timestamp must not see the value.
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 19, Key: "a"}}).(TxnResult); r.Found {
		t.Fatalf("read at ts=19 found %q; want nothing (commit was at 20)", r.Value)
	}
	// Visibility is strict: a snapshot is a point *before* which versions are
	// visible, so the version committed at 20 is not visible at 20. Reading at 21
	// is where it appears.
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 20, Key: "a"}}).(TxnResult); r.Found {
		t.Fatalf("read at ts=20 found %q; visibility must be strict (CommitTS < snapshot)", r.Value)
	}
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 21, Key: "a"}}).(TxnResult); !r.Found {
		t.Fatal("read at ts=21 found nothing; want the committed value")
	}
	if lock := dl.store.LockedKeys(); len(lock) != 0 {
		t.Fatalf("locks after commit: %v, want none", lock)
	}
}

func TestStoreSnapshotIsolation(t *testing.T) {
	dl := newDirectLog(0)

	commit := func(txnID TxnID, reqID int64, key Key, value Value, startTS, commitTS Timestamp) {
		t.Helper()
		if r := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
			TxnID: txnID, RequestID: reqID, StartTS: startTS, Primary: key,
			Ops: []WriteOp{NewPut(key, value)},
		}}).(TxnResult); r.Status != StatusOK {
			t.Fatalf("prewrite %d: %s", txnID, r.Status)
		}
		if r := dl.store.Apply(TxnCommand{Kind: CmdCommit, CommitArgs: &CommitRequest{
			TxnID: txnID, RequestID: reqID + 100, StartTS: startTS, CommitTS: commitTS, Keys: []Key{key},
		}}).(TxnResult); r.Status != StatusOK {
			t.Fatalf("commit %d: %s", txnID, r.Status)
		}
	}

	commit(1, 1, "k", "one", 5, 10)
	commit(2, 2, "k", "two", 15, 20)
	commit(3, 3, "k", "three", 25, 30)

	// Every snapshot must resolve to exactly the version valid at that instant.
	// Visibility is strict (`CommitTS < snapshot`), so a commit is first visible
	// one tick above its timestamp.
	cases := []struct {
		startTS Timestamp
		want    string
		found   bool
	}{
		{4, "", false}, // before any write
		{11, "one", true},
		{12, "one", true},
		{21, "two", true},
		{29, "two", true},
		{31, "three", true},
		{1000, "three", true},
	}
	for _, c := range cases {
		r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: c.startTS, Key: "k"}}).(TxnResult)
		if r.Status != StatusOK {
			t.Fatalf("read at %d: status=%s", c.startTS, r.Status)
		}
		if r.Found != c.found || r.Value != Value(c.want) {
			t.Errorf("read at %d: got (%q, found=%v), want (%q, found=%v)",
				c.startTS, r.Value, r.Found, c.want, c.found)
		}
	}

	// The version chain must record the validity intervals.
	versions := dl.store.versions["k"]
	if len(versions) != 3 {
		t.Fatalf("got %d versions, want 3", len(versions))
	}
	wantStarts := []Timestamp{5, 15, 25}
	for i, v := range versions {
		if v.StartTS != wantStarts[i] {
			t.Errorf("version %d: StartTS=%d, want %d (the writer's snapshot)",
				i, v.StartTS, wantStarts[i])
		}
	}
}

func TestStoreDeleteIsAnMVCCRecord(t *testing.T) {
	dl := newDirectLog(0)

	// Write a value...
	prewriteCommit(t, dl, 1, 1, "k", OpPut, "v", 5, 10)
	// ... then delete it.
	prewriteCommit(t, dl, 2, 2, "k", OpDelete, "", 15, 20)

	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 100, Key: "k"}}).(TxnResult); r.Found {
		t.Fatalf("read after delete found %q; want nothing", r.Value)
	}
	// The delete is a version, not an erasure: an older snapshot still sees the
	// original value. This is what makes a long-running transaction's reads
	// repeatable.
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 15, Key: "k"}}).(TxnResult); !r.Found || r.Value != "v" {
		t.Fatalf("read at 15: got (%q, found=%v), want v", r.Value, r.Found)
	}
	if got := len(dl.store.versions["k"]); got != 2 {
		t.Fatalf("got %d versions, want 2 (put + delete)", got)
	}
}

func prewriteCommit(t *testing.T, dl *directLog, txnID TxnID, reqID int64, key Key,
	kind OpKind, value Value, startTS, commitTS Timestamp) {
	t.Helper()
	op := WriteOp{Kind: kind, Key: key, Value: value}
	if r := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: txnID, RequestID: reqID, StartTS: startTS, Primary: key, Ops: []WriteOp{op},
	}}).(TxnResult); r.Status != StatusOK {
		t.Fatalf("prewrite txn %d: status=%s (%s)", txnID, r.Status, r.Err)
	}
	if r := dl.store.Apply(TxnCommand{Kind: CmdCommit, CommitArgs: &CommitRequest{
		TxnID: txnID, RequestID: reqID + 1000, StartTS: startTS, CommitTS: commitTS, Keys: []Key{key},
	}}).(TxnResult); r.Status != StatusOK {
		t.Fatalf("commit txn %d: status=%s (%s)", txnID, r.Status, r.Err)
	}
}

func TestStoreWriteWriteConflict(t *testing.T) {
	dl := newDirectLog(0)

	// A transaction writes and commits k at ts 20.
	prewriteCommit(t, dl, 1, 1, "k", OpPut, "v1", 10, 20)

	// A transaction whose snapshot predates that commit may not write k.
	res := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 2, RequestID: 2, StartTS: 15, Primary: "k",
		Ops: []WriteOp{NewPut("k", "v2")},
	}}).(TxnResult)
	if res.Status != StatusWriteConflict {
		t.Fatalf("got status=%s (%s), want write-conflict", res.Status, res.Err)
	}
	if res.ConflictCommit != 20 || res.ConflictStart != 10 {
		t.Errorf("conflict bounds: got start=%d commit=%d, want 10/20", res.ConflictStart, res.ConflictCommit)
	}
	if len(dl.store.LockedKeys()) != 0 {
		t.Error("a rejected prewrite left a lock behind")
	}

	// A transaction that started after the commit is free to write.
	if r := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 3, RequestID: 3, StartTS: 25, Primary: "k",
		Ops: []WriteOp{NewPut("k", "v3")},
	}}).(TxnResult); r.Status != StatusOK {
		t.Fatalf("prewrite at start ts 25: status=%s (%s), want OK", r.Status, r.Err)
	}
}

func TestStoreWriteConflictIsAtomicAcrossTheWriteSet(t *testing.T) {
	dl := newDirectLog(0)

	// k2 already carries a newer committed version.
	prewriteCommit(t, dl, 1, 1, "k2", OpPut, "old", 5, 20)

	// A transaction that writes k1 and k2 from snapshot 10 must be rejected
	// *entirely*: no lock may be left on k1.
	res := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 2, RequestID: 2, StartTS: 10, Primary: "k1",
		Ops: []WriteOp{NewPut("k1", "a"), NewPut("k2", "b")},
	}}).(TxnResult)
	if res.Status != StatusWriteConflict {
		t.Fatalf("got status=%s, want write-conflict", res.Status)
	}
	if locks := dl.store.LockedKeys(); len(locks) != 0 {
		t.Fatalf("locks left behind: %v; the write set must be all-or-nothing", locks)
	}
}

func TestStorePrewriteRejectedWhenKeyLocked(t *testing.T) {
	dl := newDirectLog(0)

	if r := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, RequestID: 1, StartTS: 10, Primary: "k",
		Ops: []WriteOp{NewPut("k", "v1")},
	}}).(TxnResult); r.Status != StatusOK {
		t.Fatalf("prewrite: %s", r.Status)
	}

	res := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 2, RequestID: 2, StartTS: 15, Primary: "k",
		Ops: []WriteOp{NewPut("k", "v2")},
	}}).(TxnResult)
	if res.Status != StatusLocked {
		t.Fatalf("got status=%s, want locked", res.Status)
	}
	if res.Lock == nil || res.Lock.TxnID != 1 || res.Lock.Primary != "k" {
		t.Fatalf("lock payload: got %+v, want txn 1 / primary k", res.Lock)
	}
}

func TestStoreRollbackReleasesLocks(t *testing.T) {
	dl := newDirectLog(0)

	dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, RequestID: 1, StartTS: 10, Primary: "k",
		Ops: []WriteOp{NewPut("k", "v1")},
	}})

	if r := dl.store.Apply(TxnCommand{Kind: CmdRollback, RollbackArgs: &RollbackRequest{
		TxnID: 1, RequestID: 2, StartTS: 10, Keys: []Key{"k"},
	}}).(TxnResult); r.Status != StatusOK || r.State != StateAborted {
		t.Fatalf("rollback: got %+v, want OK/aborted", r)
	}
	if locks := dl.store.LockedKeys(); len(locks) != 0 {
		t.Fatalf("locks after rollback: %v", locks)
	}

	// Readable again, and empty.
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 50, Key: "k"}}).(TxnResult); r.Status != StatusOK || r.Found {
		t.Fatalf("read after rollback: got %+v, want OK/not-found", r)
	}

	// The key may now be taken by somebody else.
	if r := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 2, RequestID: 3, StartTS: 90, Primary: "k",
		Ops: []WriteOp{NewPut("k", "v2")},
	}}).(TxnResult); r.Status != StatusOK {
		t.Fatalf("prewrite after rollback: %s", r.Status)
	}
}

func TestStoreOneShotPrewriteCommitsInOneEntry(t *testing.T) {
	dl := newDirectLog(0)

	res := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, RequestID: 1, StartTS: 10, CommitTS: 25, OneShot: true, Primary: "a",
		Ops: []WriteOp{NewPut("a", "x"), NewPut("b", "y")},
	}}).(TxnResult)
	if res.Status != StatusOK || res.State != StateCommitted || res.CommitTS != 25 {
		t.Fatalf("one-shot prewrite: got %+v, want OK/committed/ts=25", res)
	}
	if locks := dl.store.LockedKeys(); len(locks) != 0 {
		t.Fatalf("locks after one-shot commit: %v, want none", locks)
	}
	for key, want := range map[Key]Value{"a": "x", "b": "y"} {
		// Read one tick above the commit timestamp: visibility is strict.
		r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 26, Key: key}}).(TxnResult)
		if !r.Found || r.Value != want {
			t.Errorf("read %q: got (%q, %v), want %q", key, r.Value, r.Found, want)
		}
	}

	// A one-shot prewrite must reject a commit timestamp that is not greater than
	// the start timestamp.
	res = dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 2, RequestID: 2, StartTS: 30, CommitTS: 30, OneShot: true, Primary: "c",
		Ops: []WriteOp{NewPut("c", "z")},
	}}).(TxnResult)
	if res.Status != StatusCommitTSOutOfOrder {
		t.Fatalf("got status=%s, want commit-ts-out-of-order", res.Status)
	}
}

// TestStoreNoLostUpdateAtTheSnapshotBoundary is a regression test for a lost
// update the original comparisons allowed.
//
// Visibility used to be `CommitTS <= snapshot` while conflict detection used
// `CommitTS > snapshot`. The two rules disagreed in a one-timestamp-wide band: a
// version committed at exactly the snapshot timestamp was visible to reads yet
// did not block writes. A transaction could therefore read a value, have a
// concurrent writer commit at that same timestamp, and then overwrite it - the
// classic lost update, silently, with both transactions reporting success.
//
// Both comparisons are now strict, and this test walks that exact interleaving.
func TestStoreNoLostUpdateAtTheSnapshotBoundary(t *testing.T) {
	const snap = Timestamp(10)
	// The commit must be strictly above the snapshot (a transaction cannot commit at
	// its own start timestamp), so the boundary case is: a writer at snap that must
	// be refused a version committed at commitTS.
	const commitTS = Timestamp(11)

	dl := newDirectLog(0)
	dl.store.SetLockTTL(time.Hour)

	prewrite := func(txnID TxnID, startTS Timestamp, key Key, value Value) TxnResult {
		t.Helper()
		return dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
			TxnID: txnID, RequestID: int64(txnID), StartTS: startTS, Primary: key,
			Ops: []WriteOp{NewPut(key, value)},
		}}).(TxnResult)
	}

	// A transaction writes the key and commits at exactly its own snapshot
	// timestamp: the boundary that used to be the hole.
	if res := prewrite(1, snap, "k", "first"); res.Status != StatusOK {
		t.Fatalf("first prewrite: %s (%s)", res.Status, res.Err)
	}
	if res := dl.store.Apply(TxnCommand{Kind: CmdCommit, CommitArgs: &CommitRequest{
		TxnID: 1, RequestID: 100, StartTS: snap, CommitTS: commitTS, Keys: []Key{"k"},
	}}).(TxnResult); res.Status != StatusOK {
		t.Fatalf("commit: %s (%s)", res.Status, res.Err)
	}

	// A *second* transaction holding that same snapshot must not be allowed to
	// clobber it. Under the old rule this write was accepted, so "first" was lost
	// even though its commit had reported success.
	res := prewrite(2, snap, "k", "second")
	if res.Status != StatusWriteConflict {
		t.Fatalf("writer at snapshot %d after a commit at %d: got status=%s, want write-conflict",
			snap, snap, res.Status)
	}
	if res.ConflictCommit != commitTS {
		t.Errorf("conflict names commit ts %d, want %d", res.ConflictCommit, commitTS)
	}
	if locks := dl.store.lockedKeysFor(2); len(locks) != 0 {
		t.Fatalf("the refused transaction left locks behind: %v", locks)
	}

	// Reading at the same instant must agree: the version is not visible either,
	// because visibility is strict too. Read and conflict are complements.
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: snap, Key: "k"}}).(TxnResult); r.Found {
		t.Fatalf("read at the commit timestamp found %q; visibility must be strict", r.Value)
	}
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: commitTS + 1, Key: "k"}}).(TxnResult); !r.Found || r.Value != "first" {
		t.Fatalf("read just above the commit timestamp: got (%q, %v), want first", r.Value, r.Found)
	}

	// A writer on the safe side of the boundary succeeds, so the rule is not
	// simply refusing everything.
	if res := prewrite(3, commitTS+1, "k", "later"); res.Status != StatusOK {
		t.Fatalf("writer above the commit timestamp: got status=%s, want OK", res.Status)
	}
}

func TestStorePrimaryMustBeInWriteSet(t *testing.T) {
	// A one-shot prewrite commits the whole transaction in one entry, so it is
	// only valid when the group holds the entire write set. Claiming otherwise
	// must be rejected rather than silently committing a partial transaction.
	dl := newDirectLog(0)
	res := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, RequestID: 1, StartTS: 10, CommitTS: 20, OneShot: true, Primary: "missing",
		Ops: []WriteOp{NewPut("a", "1")},
	}}).(TxnResult)
	if res.Status != StatusKeyNotInWriteSet {
		t.Fatalf("one-shot without the primary: got status=%s, want key-not-in-write-set", res.Status)
	}

	// A prewrite that does not name a primary at all is malformed: without the
	// primary's name no reader could ever resolve the locks it creates.
	res = dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 2, RequestID: 2, StartTS: 10, Primary: "",
		Ops: []WriteOp{NewPut("a", "1")},
	}}).(TxnResult)
	if res.Status != StatusKeyNotInWriteSet {
		t.Fatalf("prewrite without a primary: got status=%s, want key-not-in-write-set", res.Status)
	}

	// A group that owns only part of the write set legitimately does not hold the
	// primary key; its locks must still record where the primary lives.
	res = dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 3, RequestID: 3, StartTS: 10, Primary: "elsewhere",
		Ops: []WriteOp{NewPut("a", "1")},
	}}).(TxnResult)
	if res.Status != StatusOK {
		t.Fatalf("partial write set: got status=%s (%s), want OK", res.Status, res.Err)
	}
	if lock := dl.store.locks["a"]; lock == nil || lock.Primary != "elsewhere" {
		t.Fatalf("lock=%+v, want Primary=elsewhere so readers can find the commit point", lock)
	}
	if locks := dl.store.LockedKeys(); len(locks) != 1 {
		t.Fatalf("locks=%v, want exactly the one prepared key", locks)
	}
}

func TestStoreSealRecordsDecisionWithoutPublishing(t *testing.T) {
	dl := newDirectLog(0)

	dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, RequestID: 1, StartTS: 10, Primary: "a",
		Ops: []WriteOp{NewPut("a", "v")},
	}})

	// Seal is the commit point: it records the decision but publishes nothing.
	res := dl.store.Apply(TxnCommand{Kind: CmdSeal, SealArgs: &SealArgs{
		Key: "a", TxnID: 1, StartTS: 10, CommitTS: 20,
	}}).(TxnResult)
	if res.Status != StatusOK || res.CommitTS != 20 {
		t.Fatalf("seal: got %+v, want OK/ts=20", res)
	}
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 100, Key: "a"}}).(TxnResult); r.Found {
		t.Fatal("seal published the value; it must only record the decision")
	}
	if lock := dl.store.locks["a"]; lock == nil || lock.CommitTS != 20 {
		t.Fatalf("lock after seal: %+v, want CommitTS=20", lock)
	}

	// A reader that sees the sealed lock can finish the write with no help from
	// the coordinator: an empty key set means "everything you hold for this txn".
	res = dl.store.Apply(TxnCommand{Kind: CmdCommit, CommitArgs: &CommitRequest{
		TxnID: 1, RequestID: 5, StartTS: 10, CommitTS: 0, Keys: nil,
	}}).(TxnResult)
	if res.Status != StatusOK || res.CommitTS != 20 || res.KeysFinalized != 1 {
		t.Fatalf("resolve commit: got %+v, want OK/ts=20/1 key taken from the lock", res)
	}
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 100, Key: "a"}}).(TxnResult); !r.Found || r.Value != "v" {
		t.Fatalf("read after resolve: got %+v, want v", r)
	}

	// Sealing a lock that does not exist must be reported, not silently ignored.
	res = dl.store.Apply(TxnCommand{Kind: CmdSeal, SealArgs: &SealArgs{
		Key: "a", TxnID: 99, StartTS: 10, CommitTS: 30,
	}}).(TxnResult)
	if res.Status != StatusLocked {
		t.Fatalf("seal of missing lock: got status=%s, want locked/ErrLockGone", res.Status)
	}
}

func TestStoreCommitIsIdempotent(t *testing.T) {
	dl := newDirectLog(0)

	dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, RequestID: 1, StartTS: 10, Primary: "k",
		Ops: []WriteOp{NewPut("k", "v")},
	}})

	req := &CommitRequest{TxnID: 1, ClientID: 7, RequestID: 42, StartTS: 10, CommitTS: 20, Keys: []Key{"k"}}
	if r := dl.store.Apply(TxnCommand{Kind: CmdCommit, CommitArgs: req}).(TxnResult); r.Status != StatusOK {
		t.Fatalf("first commit: %s", r.Status)
	}

	// A retry with the identical request ID must return the memoized answer...
	if r := dl.store.Apply(TxnCommand{Kind: CmdCommit, CommitArgs: req}).(TxnResult); r.Status != StatusOK || r.CommitTS != 20 {
		t.Fatalf("retried commit: got %+v, want the memoized OK/20", r)
	}
	// ... and must not append a second version.
	if got := len(dl.store.versions["k"]); got != 1 {
		t.Fatalf("got %d versions after a retried commit, want 1", got)
	}

	// Even with a fresh request ID, committing an already committed transaction
	// is recognized rather than duplicating the write.
	res := dl.store.Apply(TxnCommand{Kind: CmdCommit, CommitArgs: &CommitRequest{
		TxnID: 1, ClientID: 7, RequestID: 43, StartTS: 10, CommitTS: 20, Keys: []Key{"k"},
	}}).(TxnResult)
	if res.Status != StatusAlreadyCommitted {
		t.Fatalf("got status=%s, want already-committed", res.Status)
	}
	if got := len(dl.store.versions["k"]); got != 1 {
		t.Fatalf("got %d versions, want 1", got)
	}
}

func TestStoreCommittedTransactionCannotBeRolledBack(t *testing.T) {
	dl := newDirectLog(0)
	prewriteCommit(t, dl, 1, 1, "k", OpPut, "v", 10, 20)

	res := dl.store.Apply(TxnCommand{Kind: CmdRollback, RollbackArgs: &RollbackRequest{
		TxnID: 1, RequestID: 5, StartTS: 10, Keys: []Key{"k"},
	}}).(TxnResult)
	if res.Status != StatusAlreadyCommitted {
		t.Fatalf("got status=%s, want already-committed", res.Status)
	}
	// The value must still be there: a committed write can never be undone.
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 100, Key: "k"}}).(TxnResult); !r.Found || r.Value != "v" {
		t.Fatalf("value vanished after a rejected rollback: %+v", r)
	}
}

func TestStoreResolveLockCommittedVsAborted(t *testing.T) {
	// Case 1: a secondary lock whose transaction committed must be finished, not
	// discarded.
	dl := newDirectLog(0)
	dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, RequestID: 1, StartTS: 10, Primary: "a",
		Ops: []WriteOp{NewPut("a", "va"), NewPut("b", "vb")},
	}})
	dl.store.Apply(TxnCommand{Kind: CmdSeal, SealArgs: &SealArgs{Key: "a", TxnID: 1, StartTS: 10, CommitTS: 20}})

	// The reader saw txn 1 committed (it asked the primary group) and asks this
	// group to finish key b, supplying the timestamp it learned there.
	res := dl.store.Apply(TxnCommand{Kind: CmdResolveLock, ResolveLockArgs: &ResolveLockArgs{
		TxnID: 1, RequestID: 2, StartTS: 10, Key: "b", Primary: "a", Committed: true, CommitTS: 20,
	}}).(TxnResult)
	if res.Status != StatusOK {
		t.Fatalf("resolve(committed): %s (%s)", res.Status, res.Err)
	}
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 100, Key: "b"}}).(TxnResult); !r.Found || r.Value != "vb" {
		t.Fatalf("read b: got %+v, want vb", r)
	}

	// Case 2: a lock whose transaction is dead must be discarded, and the key
	// must be readable again at its previous value.
	dl.store.SetLockTTL(time.Hour)
	dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 2, RequestID: 3, StartTS: 30, Primary: "b",
		Ops: []WriteOp{NewPut("b", "leaked")},
	}})
	res = dl.store.Apply(TxnCommand{Kind: CmdResolveLock, ResolveLockArgs: &ResolveLockArgs{
		TxnID: 2, RequestID: 4, StartTS: 30, Key: "b", Primary: "b", Committed: false, Force: true,
	}}).(TxnResult)
	if res.Status != StatusOK {
		t.Fatalf("resolve(aborted): %s", res.Status)
	}
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 100, Key: "b"}}).(TxnResult); r.Value != "vb" {
		t.Fatalf("after abort: got %q, want the previously committed vb", r.Value)
	}
}

func TestStoreResolveRefusesUnexpiredLockWithoutForce(t *testing.T) {
	dl := newDirectLog(0)
	dl.store.SetLockTTL(time.Hour)
	dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, RequestID: 1, StartTS: 10, Primary: "k",
		Ops: []WriteOp{NewPut("k", "v")},
	}})

	// A resolver must not be able to roll a live lock back by accident.
	res := dl.store.Apply(TxnCommand{Kind: CmdResolveLock, ResolveLockArgs: &ResolveLockArgs{
		TxnID: 1, RequestID: 2, StartTS: 10, Key: "k", Primary: "k", Committed: false, Force: false,
	}}).(TxnResult)
	if res.Status != StatusLocked {
		t.Fatalf("got status=%s, want locked (the owner is still alive)", res.Status)
	}
	if len(dl.store.LockedKeys()) != 1 {
		t.Fatal("the lock was dropped despite being unexpired")
	}
}

func TestStoreQueryLock(t *testing.T) {
	dl := newDirectLog(0)
	dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 5, RequestID: 1, StartTS: 10, Primary: "p",
		Ops: []WriteOp{NewPut("p", "v")},
	}})

	res := dl.store.Apply(TxnCommand{Kind: CmdQueryLock, QueryLockArgs: &QueryLockArgs{TxnID: 5, Key: "p"}}).(TxnResult)
	if res.Lock == nil {
		t.Fatal("query-lock returned no lock for a locked key")
	}
	if res.Lock.Primary != "p" || res.Lock.StartTS != 10 || res.Lock.TxnID != 5 {
		t.Fatalf("lock: %+v", res.Lock)
	}
	if res.State != StatePrepared || res.Lock.State != StatePrepared {
		t.Errorf("state: got %s (lock %s), want prepared", res.State, res.Lock.State)
	}

	// After the owner seals the decision, the same query reports committed, and
	// carries the commit timestamp so a resolver can finish the write.
	if r := dl.store.Apply(TxnCommand{Kind: CmdSeal, SealArgs: &SealArgs{
		Key: "p", TxnID: 5, StartTS: 10, CommitTS: 20,
	}}).(TxnResult); r.Status != StatusOK {
		t.Fatalf("seal: %s", r.Status)
	}
	res = dl.store.Apply(TxnCommand{Kind: CmdQueryLock, QueryLockArgs: &QueryLockArgs{TxnID: 5, Key: "p"}}).(TxnResult)
	if res.Lock == nil {
		t.Fatal("query-lock after seal returned no lock")
	}
	if res.Lock.State != StateCommitted || res.Lock.CommitTS != 20 {
		t.Fatalf("lock after seal: %+v, want committed at ts 20", res.Lock)
	}

	// An unrelated transaction reports no lock.
	res = dl.store.Apply(TxnCommand{Kind: CmdQueryLock, QueryLockArgs: &QueryLockArgs{TxnID: 6, Key: "p"}}).(TxnResult)
	if res.Lock != nil {
		t.Fatalf("query-lock for txn 6 returned %+v, want none", res.Lock)
	}
}

func TestStoreRollbackWithEmptyKeySetRemovesEverything(t *testing.T) {
	dl := newDirectLog(0)
	dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, RequestID: 1, StartTS: 10, Primary: "a",
		Ops: []WriteOp{NewPut("a", "1"), NewPut("b", "2"), NewPut("c", "3")},
	}})
	// An unrelated transaction's lock must survive.
	dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 2, RequestID: 2, StartTS: 10, Primary: "z",
		Ops: []WriteOp{NewPut("z", "keep")},
	}})

	res := dl.store.Apply(TxnCommand{Kind: CmdRollback, RollbackArgs: &RollbackRequest{
		TxnID: 1, RequestID: 3, StartTS: 10, Keys: nil,
	}}).(TxnResult)
	if res.KeysFinalized != 3 {
		t.Fatalf("removed %d locks, want 3", res.KeysFinalized)
	}
	locks := dl.store.LockedKeys()
	if len(locks) != 1 || locks[0] != "z" {
		t.Fatalf("locks after rollback: %v, want just z", locks)
	}
}

func TestStoreLockExpiryUsesTheClock(t *testing.T) {
	dl := newDirectLog(0)
	now := uint64(1000)
	dl.store.SetLockTTL(50 * time.Millisecond)
	dl.store.SetClock(func() uint64 { return now })

	dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, RequestID: 1, StartTS: 10, Primary: "k",
		Ops: []WriteOp{NewPut("k", "v")},
	}})
	lock := dl.store.locks["k"]
	if lock.ExpiresAt != 1050 {
		t.Fatalf("ExpiresAt=%d, want 1050", lock.ExpiresAt)
	}
	if lock.Expired(now) {
		t.Fatal("lock reported expired while its lease is current")
	}
	if !lock.Expired(1051) {
		t.Fatal("lock not reported expired past its lease")
	}
}

// TestStoreReadUnderLockFromNewerTransactionIsVisibleAsLocked pins down the rule
// that a lock older than the reader's snapshot is on its read path, while a
// newer lock must not be.
func TestStoreLockOrdering(t *testing.T) {
	dl := newDirectLog(0)

	// A transaction with start ts 100 locks k.
	dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 1, RequestID: 1, StartTS: 100, Primary: "k",
		Ops: []WriteOp{NewPut("k", "v")},
	}})

	// A reader with an older snapshot (ts 50) must not be blocked by a lock that
	// is not part of its snapshot.
	res := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 50, Key: "k"}}).(TxnResult)
	if res.Status != StatusOK {
		t.Fatalf("older reader: got status=%s, want OK (the lock is newer than the snapshot)", res.Status)
	}

	// A reader at a snapshot at or after the lock must be blocked: the write is
	// pending and invisible, so the key must not appear to be absent either.
	for _, ts := range []Timestamp{100, 150} {
		res := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: ts, Key: "k"}}).(TxnResult)
		if res.Status != StatusLocked {
			t.Errorf("reader at %d: got status=%s, want locked", ts, res.Status)
		}
	}
}

// A state-machine operation must never leave the group half-prepared, even when
// it is rejected: this is what makes a single Raft entry a safe transaction
// participant.
func TestStoreNoPartialStateOnRejection(t *testing.T) {
	dl := newDirectLog(0)
	prewriteCommit(t, dl, 1, 1, "conflict", OpPut, "old", 5, 20)
	dl.store.SetLockTTL(time.Hour)

	// ops are ordered so that "ok" is processed before the conflicting key.
	res := dl.store.Apply(TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &PrewriteRequest{
		TxnID: 2, RequestID: 2, StartTS: 10, Primary: "ok",
		Ops: []WriteOp{NewPut("ok", "1"), NewPut("conflict", "2")},
	}}).(TxnResult)
	if res.Status != StatusWriteConflict {
		t.Fatalf("got status=%s, want write-conflict", res.Status)
	}
	if locks := dl.store.LockedKeys(); len(locks) != 0 {
		t.Fatalf("locks after a rejected multi-key prewrite: %v, want none", locks)
	}
	// No lock was created, so no reader is blocked on the good key.
	if r := dl.store.Apply(TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: 50, Key: "ok"}}).(TxnResult); r.Status != StatusOK {
		t.Fatalf("key ok is blocked after a rejected write: status=%s", r.Status)
	}
}

func TestStoreKeysAndStats(t *testing.T) {
	dl := newDirectLog(0)
	prewriteCommit(t, dl, 1, 1, "b", OpPut, "1", 5, 10)
	prewriteCommit(t, dl, 2, 2, "a", OpPut, "2", 15, 20)

	keys := dl.store.Keys()
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("Keys()=%v, want [a b] sorted", keys)
	}
	st := dl.store.Stats()
	if st.Keys != 2 || st.Versions != 2 || st.Committed != 2 || st.Locks != 0 {
		t.Fatalf("Stats()=%+v, want 2 keys / 2 versions / 2 committed / 0 locks", st)
	}
}

func TestStatusStringAndErrorMapping(t *testing.T) {
	if got := StatusWriteConflict.String(); got != "write-conflict" {
		t.Errorf("StatusWriteConflict.String()=%q", got)
	}
	if !StatusOK.OK() || !StatusAlreadyCommitted.OK() || StatusLocked.OK() {
		t.Error("Status.OK is wrong")
	}
	if err := ErrorForStatus(StatusOK, "k", nil, 0, 0, 0); err != nil {
		t.Errorf("ErrorForStatus(OK)=%v, want nil", err)
	}
	lock := &LockInfo{Exists: true, TxnID: 3, Primary: "p", StartTS: 9}
	err := ErrorForStatus(StatusLocked, "k", lock, 11, 0, 0)
	var locked *LockedError
	if !errors.As(err, &locked) {
		t.Fatalf("ErrorForStatus(locked)=%v, want *LockedError", err)
	}
	if locked.Lock.TxnID != 3 || locked.MinCommitTS != 11 || locked.Key != "k" {
		t.Errorf("LockedError=%+v", locked)
	}

	err = ErrorForStatus(StatusWriteConflict, "k", nil, 0, 7, 12)
	var wc *WriteConflictError
	if !errors.As(err, &wc) {
		t.Fatalf("ErrorForStatus(conflict)=%v, want *WriteConflictError", err)
	}
	if wc.StartTS != 7 || wc.CommitTS != 12 {
		t.Errorf("WriteConflictError=%+v", wc)
	}
}
