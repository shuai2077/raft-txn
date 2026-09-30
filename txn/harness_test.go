package txn

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// This file provides the in-process test harness for the transaction layer: a
// Log that applies commands directly to a Store (mimicking what a Raft leader
// does inside applyHandler), a scriptable timestamp oracle, and helpers for
// driving transactions from tests.

// --- an in-process Log/Participant ------------------------------------------

// directLog is a Log implementation that applies commands straight to a Store.
//
// It is the single-node analogue of RaftLog: instead of paying for consensus it
// calls the state machine synchronously, which is exactly the path a real Raft
// leader takes (append -> persist -> apply -> answer), minus replication. Tests
// that need real replication use the system harness instead.
type directLog struct {
	groupID GroupID
	store   *Store

	// failures injects errors, keyed by command kind.
	mu       sync.Mutex
	failures map[TxnCommandKind]error
	// failNextOneShot makes exactly the next one-shot prewrite fail.
	failNextOneShot error
	applied         int
}

func newDirectLog(groupID GroupID) *directLog {
	dl := &directLog{groupID: groupID, failures: make(map[TxnCommandKind]error)}
	dl.store = NewStore(dl)
	return dl
}

func (dl *directLog) ApplyCommand(ctx context.Context, cmd TxnCommand) (TxnResult, error) {
	dl.mu.Lock()
	dl.applied++
	classified := cmd.Kind
	oneShot := cmd.Kind == CmdPrewrite && cmd.PrewriteArgs != nil && cmd.PrewriteArgs.OneShot
	err := dl.failures[classified]
	if oneShot && dl.failNextOneShot != nil {
		err = dl.failNextOneShot
		dl.failNextOneShot = nil
	}
	dl.mu.Unlock()
	if err != nil {
		return TxnResult{}, err
	}
	return dl.store.Apply(cmd).(TxnResult), nil
}

func (dl *directLog) Leader() int { return 0 }

// FailCommands makes every command of the given kind fail until cleared.
func (dl *directLog) FailCommands(kind TxnCommandKind, err error) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	dl.failures[kind] = err
}

// ClearFailures removes all injected failures.
func (dl *directLog) ClearFailures() {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	dl.failures = make(map[TxnCommandKind]error)
}

// FailNextOneShotPrewrite injects a failure into the next one-shot prewrite.
func (dl *directLog) FailNextOneShotPrewrite(err error) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	dl.failNextOneShot = err
}

// --- Participant methods, reusing the raftadapter conversions ----------------

func (dl *directLog) Read(ctx context.Context, startTS Timestamp, key Key) (Value, error) {
	res, err := dl.ApplyCommand(ctx, TxnCommand{Kind: CmdRead, ReadArgs: &ReadArgs{StartTS: startTS, Key: key}})
	return valueOrError(res, err, key)
}

func (dl *directLog) Prewrite(ctx context.Context, req PrewriteRequest) error {
	r := req
	res, err := dl.ApplyCommand(ctx, TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &r})
	return errorForResult(res, err, req.Primary)
}

func (dl *directLog) Seal(ctx context.Context, key Key, txnID TxnID, startTS, commitTS Timestamp) error {
	res, err := dl.ApplyCommand(ctx, TxnCommand{Kind: CmdSeal, SealArgs: &SealArgs{
		Key: key, TxnID: txnID, StartTS: startTS, CommitTS: commitTS,
	}})
	if err != nil {
		return err
	}
	if res.Status == StatusLocked {
		return ErrLockGone
	}
	return errorForResult(res, nil, key)
}

func (dl *directLog) Commit(ctx context.Context, req CommitRequest) error {
	r := req
	res, err := dl.ApplyCommand(ctx, TxnCommand{Kind: CmdCommit, CommitArgs: &r})
	return errorForResult(res, err, firstKeyOrEmpty(req.Keys))
}

func (dl *directLog) Rollback(ctx context.Context, req RollbackRequest) error {
	r := req
	res, err := dl.ApplyCommand(ctx, TxnCommand{Kind: CmdRollback, RollbackArgs: &r})
	return errorForResult(res, err, firstKeyOrEmpty(req.Keys))
}

func (dl *directLog) Locked(ctx context.Context, txnID TxnID, key Key) (*LockInfo, error) {
	res, err := dl.ApplyCommand(ctx, TxnCommand{Kind: CmdQueryLock, QueryLockArgs: &QueryLockArgs{TxnID: txnID, Key: key}})
	if err != nil {
		return nil, err
	}
	return res.Lock, nil
}

func (dl *directLog) Barrier(ctx context.Context) error {
	res, err := dl.ApplyCommand(ctx, TxnCommand{Kind: CmdBarrier})
	if err != nil {
		return err
	}
	return errorForResult(res, nil, "")
}

// --- scriptable oracle -------------------------------------------------------

// fakeOracle hands out timestamps from a strict counter. Unlike LocalOracle it
// lets tests observe how many timestamps were spent and force failures.
type fakeOracle struct {
	mu       sync.Mutex
	last     Timestamp
	readTS   []Timestamp
	commitTS []Timestamp
	err      error
}

func newFakeOracle() *fakeOracle { return &fakeOracle{} }

// Next makes the oracle return ts from the next call.
func (o *fakeOracle) Next(ts Timestamp) *fakeOracle {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.readTS = append(o.readTS, ts)
	return o
}

// NextCommit makes the oracle return ts from the next CommitTS call.
func (o *fakeOracle) NextCommit(ts Timestamp) *fakeOracle {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.commitTS = append(o.commitTS, ts)
	return o
}

// FailNext makes the next oracle call fail.
func (o *fakeOracle) FailNext(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.err = err
}

func (o *fakeOracle) ReadTS(ctx context.Context) (Timestamp, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		err := o.err
		o.err = nil
		return 0, err
	}
	if len(o.readTS) > 0 {
		ts := o.readTS[0]
		o.readTS = o.readTS[1:]
		if ts > o.last {
			o.last = ts
		}
		return ts, nil
	}
	o.last++
	return o.last, nil
}

func (o *fakeOracle) CommitTS(ctx context.Context, startTS Timestamp) (Timestamp, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		err := o.err
		o.err = nil
		return 0, err
	}
	if len(o.commitTS) > 0 {
		ts := o.commitTS[0]
		o.commitTS = o.commitTS[1:]
		if ts <= startTS {
			return 0, fmt.Errorf("fake oracle: commit ts %d <= start ts %d", ts, startTS)
		}
		if ts > o.last {
			o.last = ts
		}
		return ts, nil
	}
	o.last++
	return o.last, nil
}

func (o *fakeOracle) Close() error { return nil }

// --- multi-group cluster harness --------------------------------------------

// testCluster wires several directLog groups into a transaction Client.
type testCluster struct {
	t            *testing.T
	groups       map[GroupID]*directLog
	partitioner  Partitioner
	oracle       *fakeOracle
	resolver     *testResolver
	client       *Client
	lockTTL      time.Duration
	clockMu      sync.Mutex
	fakeClock    uint64
	useFakeClock bool
}

func newTestCluster(t *testing.T, partitioner Partitioner, nGroups int) *testCluster {
	t.Helper()
	groups := make(map[GroupID]*directLog, nGroups)
	for i := 0; i < nGroups; i++ {
		groups[GroupID(i)] = newDirectLog(GroupID(i))
	}
	return newTestClusterWithGroups(t, partitioner, groups)
}

func newTestClusterWithGroups(t *testing.T, partitioner Partitioner, groups map[GroupID]*directLog) *testCluster {
	t.Helper()
	oracle := newFakeOracle()

	participants := make(map[GroupID]Participant, len(groups))
	for id, dl := range groups {
		participants[id] = dl
	}
	tc := &testCluster{
		t:           t,
		groups:      groups,
		partitioner: partitioner,
		oracle:      oracle,
	}
	client := NewClient(oracle, partitioner, participants, nil)
	tc.resolver = &testResolver{inner: NewResolver(client)}
	client.resolver = tc.resolver
	tc.client = client
	tc.client.SetResolveTimeout(300 * time.Millisecond)
	return tc
}

// SetLockTTL shortens the intent-lock lease on every group.
func (tc *testCluster) SetLockTTL(d time.Duration) {
	for _, dl := range tc.groups {
		dl.store.SetLockTTL(d)
	}
}

// FreezeClock makes lock expiry use a controllable counter instead of the wall
// clock, so tests can expire leases deterministically.
func (tc *testCluster) FreezeClock(startMillis uint64) {
	tc.clockMu.Lock()
	tc.useFakeClock = true
	tc.fakeClock = startMillis
	tc.clockMu.Unlock()
	for _, dl := range tc.groups {
		dl.store.SetClock(tc.nowMillis)
	}
}

// NowMillis reports the harness clock.
func (tc *testCluster) NowMillis() uint64 {
	if tc.useFakeClock {
		return tc.fakeClock
	}
	return uint64(time.Now().UnixMilli())
}

func (tc *testCluster) nowMillis() uint64 {
	tc.clockMu.Lock()
	defer tc.clockMu.Unlock()
	if tc.useFakeClock {
		tc.fakeClock += 1000
		return tc.fakeClock
	}
	return uint64(time.Now().UnixMilli())
}

// Advance moves the harness clock forward.
func (tc *testCluster) Advance(d time.Duration) {
	tc.clockMu.Lock()
	defer tc.clockMu.Unlock()
	tc.fakeClock += uint64(d / time.Millisecond)
}

// Group returns the state machine of one group.
func (tc *testCluster) Group(id GroupID) *Store { return tc.groups[id].store }

// Begin starts a transaction.
func (tc *testCluster) Begin() *Txn {
	tc.t.Helper()
	txn, err := tc.client.Begin(context.Background())
	if err != nil {
		tc.t.Fatalf("Begin: %v", err)
	}
	return txn
}

// MustPut buffers a write and fails the test on error.
func (tc *testCluster) MustPut(txn *Txn, key Key, value Value) {
	tc.t.Helper()
	if err := txn.Put(key, value); err != nil {
		tc.t.Fatalf("Put(%q): %v", key, err)
	}
}

// MustDelete buffers a delete and fails the test on error.
func (tc *testCluster) MustDelete(txn *Txn, key Key) {
	tc.t.Helper()
	if err := txn.Delete(key); err != nil {
		tc.t.Fatalf("Delete(%q): %v", key, err)
	}
}

// MustCommit commits and fails the test on error.
func (tc *testCluster) MustCommit(txn *Txn) Timestamp {
	tc.t.Helper()
	ts, err := txn.Commit(context.Background())
	if err != nil {
		tc.t.Fatalf("Commit: %v", err)
	}
	return ts
}

// PrewriteAll runs only the prepare phase of a transaction, leaving its intent
// locks in place. It is the harness's wrapper over txn.Prewrite.
func (tc *testCluster) PrewriteAll(txn *Txn) {
	tc.t.Helper()
	if err := Prewrite(txn); err != nil {
		tc.t.Fatalf("prewrite: %v", err)
	}
}

// MustGet reads and fails the test on error.
func (tc *testCluster) MustGet(txn *Txn, key Key) (Value, bool) {
	tc.t.Helper()
	v, found, err := txn.Get(context.Background(), key)
	if err != nil {
		tc.t.Fatalf("Get(%q): %v", key, err)
	}
	return v, found
}

// GetWithin reads a key with its own deadline, so tests can check that a read
// blocks on a lock without waiting out the whole resolution timeout.
func (tc *testCluster) GetWithin(txn *Txn, key Key, d time.Duration) (Value, bool, error) {
	tc.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return txn.Get(ctx, key)
}

// PutValue is a convenience for a single-key committed put outside any explicit
// transaction (it runs in its own transaction).
func (tc *testCluster) PutValue(key Key, value Value) Timestamp {
	tc.t.Helper()
	txn := tc.Begin()
	tc.MustPut(txn, key, value)
	return tc.MustCommit(txn)
}

// MustReadAt reads a key at a snapshot, failing the test on error.
func (tc *testCluster) MustReadAt(group GroupID, ts Timestamp, key Key) (Value, bool) {
	tc.t.Helper()
	v, err := tc.groups[group].Read(context.Background(), ts, key)
	if err != nil {
		if _, notFound := err.(*KeyNotFoundError); notFound {
			return "", false
		}
		tc.t.Fatalf("Read(%q at %d): %v", key, ts, err)
	}
	return v, true
}

// --- test resolver -----------------------------------------------------------

// testResolver is a thin wrapper over DefaultResolver that counts calls, so
// tests can assert that recovery actually happened.
type testResolver struct {
	inner *DefaultResolver
	mu    sync.Mutex
	calls int
}

func (tr *testResolver) ResolveLock(ctx context.Context, group GroupID, lock TxnLock) error {
	tr.mu.Lock()
	tr.calls++
	tr.mu.Unlock()
	return tr.inner.ResolveLock(ctx, group, lock)
}

// Calls reports how many resolution attempts were made.
func (tr *testResolver) Calls() int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.calls
}

// --- key partitioners used by tests -----------------------------------------

// groupForPrefix routes by a single-character key prefix, so tests can place a
// key in a chosen group by naming it "0foo", "1bar", and so on. Keys without a
// digit prefix go to group 0.
type groupForPrefix struct{}

func (groupForPrefix) GroupForKey(key Key) GroupID {
	if len(key) == 0 {
		return 0
	}
	if c := key[0]; c >= '0' && c <= '9' {
		return GroupID(c - '0')
	}
	return 0
}
