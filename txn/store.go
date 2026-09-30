package txn

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"
)

// This file implements the per-group replicated state machine: a multi-version
// (MVCC) key/value store with intent locks and two-phase-commit bookkeeping.
//
// Layout
//
//	versions[k] = [{commitTS, kind, value}, ...]  append-only, sorted by commitTS
//	locks[k]    = {txnID, startTS, primary, kind, commitTS, expiresAt}
//	actors[id]  = {state, prepared shards, commitTS, request memo}
//
// Every mutation of these maps is driven exclusively by Apply, which Raft calls
// in log order on every replica. Merging two CRDT-ish store states, or applying
// an out-of-order command, is therefore impossible by construction and no
// locking between groups is needed: a group's state is a pure function of its
// Raft log.

// LockTTL is how long an intent lock stays valid without hearing from its
// owner. Past this deadline any reader may abort the lock, which is how the
// system recovers from a coordinator that crashed mid-commit.
const LockTTL = 3 * time.Second

// maxMemoizedRequests bounds the per-transaction request memo used for
// exactly-once retries.
const maxMemoizedRequests = 16

// Log is the subset of the Raft consensus module the transactional state
// machine needs. internal/raft.Server satisfies it through the adapter in
// raftadapter.go; the interface exists so the state machine can be unit tested
// without spinning up Raft.
type Log interface {
	// ApplyCommand replicates cmd through Raft and returns the state machine's
	// result. It is synchronous: when it returns, cmd is durable in the leader's
	// log and has been applied by the leader's state machine.
	//
	// A *NotLeaderError means this replica is not the group leader.
	ApplyCommand(ctx context.Context, cmd TxnCommand) (TxnResult, error)

	// Leader reports this replica's Raft server id when it is the group leader,
	// or -1 when it is not.
	Leader() int
}

// Actor is the per-transaction record kept by a group. It exists on every group
// the transaction touched. Its transitions are replicated, so all replicas of a
// group agree on the transaction's fate.
type Actor struct {
	TxnID TxnID

	State State

	// Primary is the key whose lock records the transaction's decision.
	Primary Key

	// CommitTS is the timestamp the transaction's writes become visible at.
	// Non-zero once the transaction has been told to commit.
	CommitTS Timestamp

	// PreparedShards is reconstructed from the Raft log; it is informational and
	// used for observability.
	PreparedShards []GroupID

	// Keys is the transaction's write set as the group knows it. It is recorded
	// at prewrite time so that a resolve request carrying no explicit key list
	// ("finish everything you hold for this transaction") can be served without
	// scanning the whole lock table.
	Keys []Key

	// applied memoizes the result of each request so that a retried RPC (whose
	// response was lost) returns the original answer instead of re-executing.
	applied map[int64]TxnResult
}

// txnKeys returns the write set the group recorded for a transaction, or nil.
func (s *Store) txnKeys(txnID TxnID) []Key {
	if actor, ok := s.actors[txnID]; ok {
		return actor.Keys
	}
	return nil
}

// Store is the transactional state machine for one Raft group. It is not safe
// for concurrent use by callers: Apply is always invoked while the owning
// ConsensusModule holds its lock, which serializes access.
type Store struct {
	// log replicates commands through this group's Raft instance.
	log Log

	// lockTTL is how long an intent lock survives without progress. Zero means
	// LockTTL.
	lockTTL time.Duration

	// nowMillis returns the current wall clock in milliseconds. It is used only
	// for lock expiry, never for version ordering, so a clock jump can delay
	// recovery but can never cause a correctness violation.
	nowMillis func() uint64

	versions map[Key][]Version
	locks    map[Key]*LockRecord
	actors   map[TxnID]*Actor
}

// NewStore creates the state machine for one group.
func NewStore(log Log) *Store {
	return &Store{
		log:       log,
		lockTTL:   LockTTL,
		nowMillis: func() uint64 { return uint64(time.Now().UnixMilli()) },
		versions:  make(map[Key][]Version),
		locks:     make(map[Key]*LockRecord),
		actors:    make(map[TxnID]*Actor),
	}
}

// SetLockTTL overrides the intent-lock lease. Tests use a short TTL to exercise
// lock recovery without waiting seconds.
func (s *Store) SetLockTTL(d time.Duration) {
	if d > 0 {
		s.lockTTL = d
	}
}

// SetClock overrides the wall clock used for lock expiry.
func (s *Store) SetClock(fn func() uint64) {
	if fn != nil {
		s.nowMillis = fn
	}
}

// Apply is the replicated state-machine hook. Raft calls it with the committed
// log command, in log order, on every replica.
func (s *Store) Apply(command any) any {
	cmd, ok := command.(TxnCommand)
	if !ok {
		// Commands from other layers (for instance the plain KV commands of a
		// co-located non-transactional service) are not ours to apply.
		return TxnResult{Status: StatusInvalid, Err: fmt.Sprintf("unexpected command %T", command)}
	}

	switch cmd.Kind {
	case CmdRead:
		return s.read(*cmd.ReadArgs)
	case CmdPrewrite:
		return s.prewrite(*cmd.PrewriteArgs)
	case CmdSeal:
		return s.seal(*cmd.SealArgs)
	case CmdCommit:
		return s.commit(*cmd.CommitArgs)
	case CmdRollback:
		return s.rollback(*cmd.RollbackArgs)
	case CmdResolveLock:
		return s.resolveLock(*cmd.ResolveLockArgs)
	case CmdQueryLock:
		return s.queryLock(*cmd.QueryLockArgs)
	default:
		panic(fmt.Sprintf("txn: unexpected command kind %v", cmd.Kind))
	}
}

// --- request memoization -----------------------------------------------------

// memoized returns a previously computed result for (clientID, requestID) of
// txnID, if the group still remembers it. This is what makes client retries
// exactly-once: a retried prewrite returns the original verdict instead of
// re-taking locks or re-reporting a conflict.
func (s *Store) memoized(txnID TxnID, clientID, requestID int64) (TxnResult, bool) {
	if clientID == 0 && requestID == 0 {
		return TxnResult{}, false
	}
	actor, ok := s.actors[txnID]
	if !ok {
		return TxnResult{}, false
	}
	res, ok := actor.applied[requestID]
	return res, ok
}

func (s *Store) memoize(txnID TxnID, clientID, requestID int64, res TxnResult) TxnResult {
	if clientID == 0 && requestID == 0 {
		return res
	}
	actor, ok := s.actors[txnID]
	if !ok {
		actor = &Actor{TxnID: txnID, State: StatePending, applied: make(map[int64]TxnResult)}
		s.actors[txnID] = actor
	}
	if actor.applied == nil {
		actor.applied = make(map[int64]TxnResult)
	}
	if len(actor.applied) >= maxMemoizedRequests {
		// Bound memory: the memo is an optimization, not a correctness
		// requirement, so dropping it is safe.
		actor.applied = make(map[int64]TxnResult)
	}
	actor.applied[requestID] = res
	return res
}

// actorOrCreate returns the transaction's record, creating a pending one if the
// group has never seen the transaction.
func (s *Store) actorOrCreate(txnID TxnID) *Actor {
	actor, ok := s.actors[txnID]
	if !ok {
		actor = &Actor{TxnID: txnID, State: StatePending, applied: make(map[int64]TxnResult)}
		s.actors[txnID] = actor
	}
	if actor.applied == nil {
		actor.applied = make(map[int64]TxnResult)
	}
	return actor
}

func (s *Store) actorState(txnID TxnID) (State, bool) {
	actor, ok := s.actors[txnID]
	if !ok {
		return StateInvalid, false
	}
	return actor.State, true
}

// --- CmdRead -----------------------------------------------------------------

// read serves a snapshot read, reporting a *LockedError equivalent when an
// unexpired lock from another transaction shadows the key.
func (s *Store) read(args ReadArgs) TxnResult {
	if lock, ok := s.locks[args.Key]; ok {
		if lock.ExpiresAt != 0 && s.nowMillis() > lock.ExpiresAt {
			// The owner's lease ran out. Report the lock anyway: the coordinator
			// knows how to resolve it (it must ask the primary's group first, to
			// be safe against a coordinator that is merely slow).
			return s.lockedResult(args.Key, lock)
		}
		if lock.StartTS <= args.StartTS {
			// A lock at or before our snapshot is on our read path. It is either
			// ours or an older uncommitted transaction; either way we cannot read
			// through it.
			return s.lockedResult(args.Key, lock)
		}
		// The lock was created after our snapshot: its writes are not part of our
		// snapshot, and we must ignore its pending write record. The version chain
		// is not touched by prewrite, so the read below is correct.
	}

	val, found := s.latestVersionAt(args.Key, args.StartTS)
	return TxnResult{Status: StatusOK, Value: val.Value, Found: found}
}

func (s *Store) lockedResult(key Key, lock *LockRecord) TxnResult {
	li := lock.InfoForKey(key)
	minCommit := Timestamp(0)
	if lock.CommitTS > 0 {
		// A lock whose owner already committed (but has not cleaned up) pushes
		// our commit timestamp past it, the Percolator "push" rule.
		minCommit = lock.CommitTS
	}
	return TxnResult{Status: StatusLocked, Lock: &li, MinCommitTS: minCommit}
}

// latestVersionAt returns the value visible to the snapshot startTS.
func (s *Store) latestVersionAt(key Key, startTS Timestamp) (Version, bool) {
	versions := s.versions[key]
	best := -1
	for i := range versions {
		if versions[i].CommitTS <= startTS {
			best = i
		} else {
			break
		}
	}
	if best < 0 {
		return Version{}, false
	}
	v := versions[best]
	if v.Kind == OpDelete {
		return Version{}, false
	}
	return v, true
}

// --- CmdPrewrite -------------------------------------------------------------

func (s *Store) prewrite(req PrewriteRequest) TxnResult {
	if res, ok := s.memoized(req.TxnID, req.ClientID, req.RequestID); ok {
		return res
	}

	// Validation that must fail before any state changes.
	if state, ok := s.actorState(req.TxnID); ok && state.Final() {
		if state == StateCommitted {
			actor := s.actors[req.TxnID]
			return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
				Status: StatusAlreadyCommitted, State: state, CommitTS: actor.CommitTS})
		}
		return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
			Status: StatusInternal, State: state,
			Err: fmt.Sprintf("transaction %d is %s and cannot be prepared", req.TxnID, state)})
	}
	// A group is told the primary key's identity even when it owns none of the
	// primary's own locks, because every lock must record where the transaction's
	// commit point lives. So the state machine cannot answer "does this group own
	// the primary" - but the primary key is present in the ops list exactly when
	// this group owns it, and that gives a checkable invariant.
	if req.Primary == "" {
		return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
			Status: StatusKeyNotInWriteSet,
			Err:    "prewrite must name the transaction's primary key"})
	}
	if req.OneShot && !containsKey(req.Ops, req.Primary) {
		// The single-group fast path commits the transaction in one entry, which
		// is only sound when this group holds the whole write set - and therefore
		// necessarily the primary key.
		return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
			Status: StatusKeyNotInWriteSet,
			Err:    "one-shot prewrite must include the primary key in its write set"})
	}
	if req.OneShot && req.CommitTS == 0 {
		return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
			Status: StatusInternal, Err: "one-shot prewrite requires a commit timestamp"})
	}
	if req.OneShot && req.CommitTS <= req.StartTS {
		return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
			Status: StatusCommitTSOutOfOrder,
			Err:    fmt.Sprintf("commit ts %d is not greater than start ts %d", req.CommitTS, req.StartTS)})
	}

	expiresAt := s.expiresAt()

	// Validate every write before mutating anything, so the group is never left
	// partially locked by a rejected request. (This mirrors the atomicity
	// assumption internal/raft makes about its own storage: one log entry, one
	// state transition.)
	for _, op := range req.Ops {
		if res, bad := s.checkPrewriteOp(req, op, expiresAt); bad {
			return s.memoize(req.TxnID, req.ClientID, req.RequestID, res)
		}
	}

	// Commit point: install the intent locks.
	for _, op := range req.Ops {
		s.locks[op.Key] = &LockRecord{
			TxnID:     req.TxnID,
			StartTS:   req.StartTS,
			Primary:   req.Primary,
			Kind:      op.Kind,
			Value:     op.Value,
			CommitTS:  0,
			ExpiresAt: expiresAt,
		}
	}

	actor := s.actorOrCreate(req.TxnID)
	actor.Primary = req.Primary
	actor.State = StatePrepared
	// Record the write set as this group sees it (a transaction may have a
	// different subset of keys in each group).
	actor.Keys = make([]Key, 0, len(req.Ops))
	for _, op := range req.Ops {
		actor.Keys = append(actor.Keys, op.Key)
	}

	res := TxnResult{Status: StatusOK, State: StatePrepared, Prepared: true}

	if req.OneShot {
		// Fast path: the group owns every key of the transaction, so the locks it
		// just took can be converted to committed versions in the same replicated
		// log entry. This is a single Raft round trip instead of two, and it is
		// atomic by construction.
		actor.CommitTS = req.CommitTS
		for _, op := range req.Ops {
			s.publishVersion(op.Key, req.StartTS, req.CommitTS, op.Kind, op.Value)
			delete(s.locks, op.Key)
		}
		actor.State = StateCommitted
		res.CommitTS = req.CommitTS
		res.State = StateCommitted
		res.KeysFinalized = len(req.Ops)
	}

	return s.memoize(req.TxnID, req.ClientID, req.RequestID, res)
}

// checkPrewriteOp validates a single write against the current MVCC state,
// returning a non-OK result when the write must be rejected.
func (s *Store) checkPrewriteOp(req PrewriteRequest, op WriteOp, expiresAt uint64) (TxnResult, bool) {
	if lock, ok := s.locks[op.Key]; ok {
		switch {
		case lock.TxnID == req.TxnID:
			// Our own lock from an earlier request in this transaction: idempotent.
		case lock.ExpiresAt != 0 && s.nowMillis() > lock.ExpiresAt:
			// Stale lock owned by a dead coordinator. Surface it so the caller can
			// resolve it (which requires consulting the primary) and retry. We do
			// not resolve it here: this code runs inside the Raft apply path, which
			// must not perform cross-group I/O.
			return s.lockedResult(op.Key, lock), true
		default:
			return s.lockedResult(op.Key, lock), true
		}
	}

	// Write-write conflict: reject a write if any version of the key was
	// committed after our snapshot. This check is what gives the transaction its
	// isolation guarantee, and it is race-free because it is evaluated in the
	// same critical section, on the group's leader, that installs the lock.
	startTS, commitTS, conflict := s.writeConflict(op.Key, req.StartTS)
	if conflict {
		return TxnResult{
			Status:         StatusWriteConflict,
			ConflictStart:  startTS,
			ConflictCommit: commitTS,
		}, true
	}
	return TxnResult{}, false
}

// writeConflict reports whether key carries a committed version newer than the
// writer's snapshot.
func (s *Store) writeConflict(key Key, startTS Timestamp) (Timestamp, Timestamp, bool) {
	versions := s.versions[key]
	if len(versions) == 0 {
		return 0, 0, false
	}
	newest := versions[len(versions)-1]
	if newest.CommitTS > startTS {
		// The newest version is newer than our snapshot. Report the oldest such
		// version, which is the one we are in conflict with.
		for i := range versions {
			if versions[i].CommitTS > startTS {
				return versions[i].StartTS, versions[i].CommitTS, true
			}
		}
	}
	return 0, 0, false
}

func (s *Store) expiresAt() uint64 { return s.nowMillis() + uint64(s.lockTTL/time.Millisecond) }

// --- CmdSeal -----------------------------------------------------------------

// seal writes the decision timestamp into a lock record without publishing the
// write.
//
// For the *primary* key this is the transaction's atomic commit point: it is a
// single Raft log entry, so every replica either has it or does not. From that
// entry onward, any reader that finds one of the transaction's secondary locks
// can read the commit timestamp out of the lock and finish the write itself,
// with no further involvement from the coordinator. A coordinator that dies
// immediately after this call therefore loses nothing.
func (s *Store) seal(args SealArgs) TxnResult {
	lock, ok := s.locks[args.Key]
	if !ok || lock.TxnID != args.TxnID {
		// Somebody else already resolved this lock. That is not necessarily an
		// error for the coordinator (it may mean the transaction committed and a
		// reader finished it), so report a distinct status and let the caller
		// query the outcome.
		return TxnResult{Status: StatusLocked, Err: ErrLockGone.Error()}
	}
	if lock.StartTS != args.StartTS {
		return TxnResult{Status: StatusInternal, Err: fmt.Sprintf(
			"lock on %q belongs to start ts %d, not %d", args.Key, lock.StartTS, args.StartTS)}
	}
	if args.CommitTS <= lock.StartTS {
		return TxnResult{Status: StatusCommitTSOutOfOrder, Err: fmt.Sprintf(
			"commit ts %d is not greater than start ts %d", args.CommitTS, lock.StartTS)}
	}
	if lock.CommitTS != 0 && lock.CommitTS != args.CommitTS {
		return TxnResult{Status: StatusInternal, Err: fmt.Sprintf(
			"lock on %q is already sealed at ts %d", args.Key, lock.CommitTS)}
	}

	lock.CommitTS = args.CommitTS
	// Sealing extends the lease: the transaction is now decided, and a reader
	// should get a chance to see the decision before anybody force-resolves it.
	lock.ExpiresAt = s.expiresAt()

	actor := s.actorOrCreate(args.TxnID)
	if actor.Primary == "" {
		actor.Primary = lock.Primary
	}
	// Sealing records the transaction's decision. It is the commit point: the
	// group now knows the transaction is committed even though the write is not
	// published yet. The CmdCommit that follows (issued by the coordinator, or by
	// any reader that discovers this decision) publishes it.
	if !actor.State.Final() {
		actor.State = StateCommitted
	}
	if actor.CommitTS == 0 {
		actor.CommitTS = args.CommitTS
	}
	return TxnResult{Status: StatusOK, State: actor.State, CommitTS: args.CommitTS, Lock: ptrTo(lock.InfoForKey(args.Key))}
}

// --- CmdCommit ---------------------------------------------------------------

func (s *Store) commit(req CommitRequest) TxnResult {
	if res, ok := s.memoized(req.TxnID, req.ClientID, req.RequestID); ok {
		return res
	}

	actor := s.actors[req.TxnID]

	keys := req.Keys
	if len(keys) == 0 {
		// Resolve-style commit: publish every lock this group holds for the
		// transaction. A reader that discovered "the owner committed" knows the
		// transaction ID but not which of this group's keys were involved, so the
		// state machine substitutes the set it recorded at prewrite time.
		keys = s.lockedKeysFor(req.TxnID)
	}

	if actor != nil && actor.State == StateCommitted && len(keys) == 0 {
		// Nothing left to publish and the decision is already recorded: this is a
		// retry, and it is idempotent.
		return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
			Status: StatusAlreadyCommitted, State: StateCommitted, CommitTS: actor.CommitTS})
	}

	// The commit timestamp may be supplied by the caller or left to the lock
	// (which the owner sealed at its commit point). With neither, the request is
	// not yet decidable.
	if req.CommitTS == 0 && len(keys) == 0 {
		return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
			Status: StatusInternal,
			Err:    "cannot finalize: no commit timestamp and no locks held for this transaction"})
	}

	effectiveTS := req.CommitTS
	committed := 0
	for _, key := range keys {
		lock, ok := s.locks[key]
		if !ok || lock.TxnID != req.TxnID {
			// Nothing to publish: either the key was never locked here, or another
			// reader already resolved the lock with the same verdict.
			continue
		}
		commitTS := req.CommitTS
		if commitTS == 0 {
			// The caller knows the transaction committed but not at which
			// timestamp; the owner recorded it in the lock when it sealed.
			commitTS = lock.CommitTS
		}
		if commitTS == 0 {
			return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
				Status: StatusInternal, Err: fmt.Sprintf(
					"cannot finalize lock on %q: commit timestamp unknown", key)})
		}
		if commitTS <= lock.StartTS {
			return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
				Status: StatusCommitTSOutOfOrder,
				Err:    fmt.Sprintf("commit ts %d is not greater than start ts %d", commitTS, lock.StartTS)})
		}
		s.publishVersion(key, lock.StartTS, commitTS, lock.Kind, lock.Value)
		delete(s.locks, key)
		committed++
		effectiveTS = commitTS
	}

	if effectiveTS != 0 && effectiveTS <= req.StartTS && req.StartTS != 0 {
		return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
			Status: StatusCommitTSOutOfOrder,
			Err: fmt.Sprintf("commit ts %d is not greater than start ts %d",
				effectiveTS, req.StartTS)})
	}

	actor = s.actorOrCreate(req.TxnID)
	actor.State = StateCommitted
	if effectiveTS != 0 {
		actor.CommitTS = effectiveTS
	}

	types := StatusOK
	if actor != nil && actor.State == StateCommitted && committed == 0 {
		// Nothing was published and the decision was already on record: this is a
		// retry of a commit that has fully landed. Report it as such so callers
		// can tell "already done" from "just did it".
		types = StatusAlreadyCommitted
	}

	return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
		Status: types, State: StateCommitted, CommitTS: actor.CommitTS, KeysFinalized: committed})
}

// lockedKeysFor returns the keys on which this group currently holds a lock for
// the transaction, in deterministic order.
func (s *Store) lockedKeysFor(txnID TxnID) []Key {
	keys := make([]Key, 0, 4)
	for key, lock := range s.locks {
		if lock.TxnID == txnID {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// publishVersion appends a committed version to the key's chain.
func (s *Store) publishVersion(key Key, startTS, commitTS Timestamp, kind OpKind, value Value) {
	versions := s.versions[key]
	if n := len(versions); n > 0 && versions[n-1].CommitTS > commitTS {
		// Versions are published in commit-timestamp order within a group,
		// because every publication is ordered by the Raft log. An out-of-order
		// publication would break snapshot reads, so fail loudly rather than
		// silently corrupt the chain.
		log.Panicf("txn: version for key %q published out of order: %d after %d",
			key, commitTS, versions[n-1].CommitTS)
	}
	s.versions[key] = append(versions, Version{
		CommitTS: commitTS,
		// StartTS is the snapshot of the transaction that produced this version.
		// Recording the writer's own start timestamp - rather than inferring it
		// from the previous version - is what lets conflict detection name the
		// exact transaction a writer collided with.
		StartTS: startTS,
		Kind:    kind,
		Value:   value,
	})
}

// --- CmdRollback -------------------------------------------------------------

func (s *Store) rollback(req RollbackRequest) TxnResult {
	if res, ok := s.memoized(req.TxnID, req.ClientID, req.RequestID); ok {
		return res
	}
	if actor, ok := s.actors[req.TxnID]; ok && actor.State == StateCommitted {
		// Committed transactions can never be rolled back: that would resurrect a
		// value a concurrent reader may already have observed.
		return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
			Status: StatusAlreadyCommitted, State: StateCommitted, CommitTS: actor.CommitTS})
	}

	removed := 0
	keys := req.Keys
	if len(keys) == 0 {
		keys = s.lockedKeysFor(req.TxnID)
	}
	for _, key := range keys {
		if lock, ok := s.locks[key]; ok && lock.TxnID == req.TxnID {
			delete(s.locks, key)
			removed++
		}
	}

	actor := s.actorOrCreate(req.TxnID)
	if !actor.State.Final() {
		actor.State = StateAborted
	}
	return s.memoize(req.TxnID, req.ClientID, req.RequestID, TxnResult{
		Status: StatusOK, State: actor.State, KeysFinalized: removed})
}

// --- CmdResolveLock ----------------------------------------------------------

// resolveLock applies a verdict (commit or rollback) that a resolver obtained by
// consulting the owning transaction's primary lock. It is the mechanism that
// lets the system recover from a wedged transaction without its coordinator.
//
// Trust model: this group cannot verify the verdict itself, because the
// transaction's commit point lives in its primary key's group. Resolutions are
// therefore only accepted from a caller that says it consulted the primary
// (Committed=true), or that is reacting to an expired lease (Force=true). A
// bogus verdict from a peer would be a protocol violation, not a failure mode
// this layer can detect.
func (s *Store) resolveLock(args ResolveLockArgs) TxnResult {
	if res, ok := s.memoized(args.TxnID, args.ClientID, args.RequestID); ok {
		return res
	}

	lock, ok := s.locks[args.Key]
	if !ok || lock.TxnID != args.TxnID || lock.StartTS != args.StartTS {
		// Already resolved by somebody else, or never held here.
		return s.memoize(args.TxnID, args.ClientID, args.RequestID, TxnResult{
			Status: StatusOK, State: s.stateOrInvalid(args.TxnID)})
	}

	if args.Committed {
		// The resolver established that the transaction committed. Publish the
		// write rather than dropping it.
		if args.CommitTS == 0 {
			return s.memoize(args.TxnID, args.ClientID, args.RequestID, TxnResult{
				Status: StatusInternal, Err: fmt.Sprintf(
					"committed verdict for key %q carries no commit timestamp", args.Key)})
		}
		if args.CommitTS <= lock.StartTS {
			return s.memoize(args.TxnID, args.ClientID, args.RequestID, TxnResult{
				Status: StatusCommitTSOutOfOrder, Err: fmt.Sprintf(
					"commit ts %d is not greater than start ts %d", args.CommitTS, lock.StartTS)})
		}
		s.publishVersion(args.Key, lock.StartTS, args.CommitTS, lock.Kind, lock.Value)
		delete(s.locks, args.Key)
		actor := s.actorOrCreate(args.TxnID)
		actor.State = StateCommitted
		if actor.CommitTS == 0 {
			actor.CommitTS = args.CommitTS
		}
		return s.memoize(args.TxnID, args.ClientID, args.RequestID, TxnResult{
			Status: StatusOK, State: StateCommitted, CommitTS: actor.CommitTS, KeysFinalized: 1})
	}

	if !args.Force && !lock.Expired(s.nowMillis()) {
		// The owner's lease is still current: it may yet commit. Leave the lock
		// alone so the caller waits instead of destroying a committed write.
		return s.memoize(args.TxnID, args.ClientID, args.RequestID, TxnResult{
			Status: StatusLocked, Lock: ptrTo(lock.InfoForKey(args.Key)), MinCommitTS: lock.CommitTS})
	}

	// Abort: drop the intent. No version was ever published, so there is nothing
	// else to undo.
	delete(s.locks, args.Key)
	actor := s.actorOrCreate(args.TxnID)
	if !actor.State.Final() {
		actor.State = StateAborted
	}
	return s.memoize(args.TxnID, args.ClientID, args.RequestID, TxnResult{
		Status: StatusOK, State: actor.State})
}

func (s *Store) stateOrInvalid(txnID TxnID) State {
	if actor, ok := s.actors[txnID]; ok {
		return actor.State
	}
	return StateInvalid
}

// --- CmdQueryLock ------------------------------------------------------------

// queryLock exposes the lock record a group holds for a key. A coordinator uses
// it to discover the fate of a foreign transaction by asking the group that
// owns the lock's Primary key.
func (s *Store) queryLock(args QueryLockArgs) TxnResult {
	state := s.stateOrInvalid(args.TxnID)
	res := TxnResult{Status: StatusOK, State: state}
	if actor, ok := s.actors[args.TxnID]; ok {
		res.CommitTS = actor.CommitTS
	}
	if lock, ok := s.locks[args.Key]; ok && lock.TxnID == args.TxnID {
		info := lock.InfoForKey(args.Key)
		// Carry the actor state with the lock: a resolver distinguishes
		// "the owner sealed the decision" from "the owner is merely undecided"
		// by looking at the transaction's state, not at the lock's timestamp.
		info.State = state
		if actor, ok := s.actors[args.TxnID]; ok {
			info.ActorCommitTS = actor.CommitTS
		}
		res.Lock = &info
	}
	return res
}

// --- helpers -----------------------------------------------------------------

func containsKey(ops []WriteOp, key Key) bool {
	for _, op := range ops {
		if op.Key == key {
			return true
		}
	}
	return false
}

func ptrTo[T any](v T) *T { return &v }

// Stats reports the size of the state machine, for observability and tests.
type Stats struct {
	Keys      int
	Versions  int
	Locks     int
	Actors    int
	Committed int
	Aborted   int
}

// Stats returns a snapshot of the state machine's size.
func (s *Store) Stats() Stats {
	st := Stats{Keys: len(s.versions), Locks: len(s.locks), Actors: len(s.actors)}
	for _, vs := range s.versions {
		st.Versions += len(vs)
	}
	for _, actor := range s.actors {
		switch actor.State {
		case StateCommitted:
			st.Committed++
		case StateAborted:
			st.Aborted++
		}
	}
	return st
}

// Keys returns the keys this group has versions for, in sorted order. Test-only.
func (s *Store) Keys() []Key {
	keys := make([]Key, 0, len(s.versions))
	for k := range s.versions {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// LockedKeys returns the keys currently holding an unexpired intent lock.
// Test-only.
func (s *Store) LockedKeys() []Key {
	now := s.nowMillis()
	keys := make([]Key, 0, len(s.locks))
	for k, lock := range s.locks {
		if !lock.Expired(now) {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// Sync is a no-op: the state machine has no background goroutines and no
// buffered state to flush, because every mutation arrived through the Raft log.
func (s *Store) Sync() {}
