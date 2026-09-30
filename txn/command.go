package txn

import (
	"encoding/gob"
	"fmt"
)

// This file defines the command vocabulary that is replicated through Raft.
//
// Every operation the transaction layer performs on a group - registering a
// transaction, reading a snapshot, taking intent locks, finalizing or aborting
// - becomes one raft.LogEntry. Because Raft applies log entries in the same
// order on every replica, the resulting MVCC state is identical everywhere;
// this is what turns the plain Raft log from internal/raft into a replicated
// transactional store.

// TxnCommandKind discriminates the entries of the replicated transaction log.
type TxnCommandKind int

const (
	// CmdInvalid is the zero value; applying it is a bug.
	CmdInvalid TxnCommandKind = iota

	// CmdRead reads a key at a snapshot. Read-only: it never mutates the state
	// machine. It travels through the log so that the result is linearizable at
	// a well-defined point in the group's history rather than whatever the local
	// replica happened to have applied.
	CmdRead

	// CmdPrewrite creates the transaction's intent locks (and, when OneShot is
	// set, commits the writes in the same entry).
	CmdPrewrite

	// CmdSeal records the commit timestamp in a lock record without publishing
	// the write. Sealing the primary lock is the transaction's atomic commit
	// point.
	CmdSeal

	// CmdCommit publishes the transaction's buffered writes at CommitTS.
	CmdCommit

	// CmdRollback removes the transaction's intent locks and marks it aborted.
	CmdRollback

	// CmdResolveLock resolves a lock found by a reader, after the reader has
	// learned (possibly from another group) what the owning transaction decided.
	CmdResolveLock

	// CmdQueryLock reads the lock record a group holds for one key, without
	// mutating anything. It is how a coordinator discovers a transaction's
	// primary lock: it asks the primary key's home group.
	CmdQueryLock

	// CmdBarrier mutates nothing and exists only for its position in the log.
	//
	// Appending it to a group's log and waiting for it to be applied is the
	// group's *read barrier*: because Raft applies entries in log order, the
	// barrier cannot be applied until every entry ahead of it has been. A reader
	// that has waited for one barrier on a group can therefore read that group at
	// a snapshot timestamp taken *before* the barrier was proposed and be sure it
	// is not missing a write that was already decided.
	//
	// See docs/03-read-barrier.md for the argument and for what breaks without it.
	CmdBarrier
)

var commandKindNames = map[TxnCommandKind]string{
	CmdInvalid:     "invalid",
	CmdRead:        "read",
	CmdPrewrite:    "prewrite",
	CmdSeal:        "seal",
	CmdCommit:      "commit",
	CmdRollback:    "rollback",
	CmdResolveLock: "resolve-lock",
	CmdQueryLock:   "query-lock",
	CmdBarrier:     "barrier",
}

func (k TxnCommandKind) String() string {
	if name, ok := commandKindNames[k]; ok {
		return name
	}
	return fmt.Sprintf("cmd(%d)", int(k))
}

// TxnCommand is the single command type submitted to every Raft group backing
// the transaction layer. Exactly one of the payload fields is set, according to
// Kind.
type TxnCommand struct {
	Kind TxnCommandKind

	// Read requests (CmdRead).
	ReadArgs *ReadArgs

	// Write requests. A PrewriteRequest / CommitRequest / RollbackRequest can be
	// marshalled directly because the request types are plain data; they are
	// re-declared as pointers here for symmetry with the response payloads.
	PrewriteArgs *PrewriteRequest
	SealArgs     *SealArgs
	CommitArgs   *CommitRequest
	RollbackArgs *RollbackRequest

	// Lock maintenance (CmdResolveLock, CmdQueryLock).
	ResolveLockArgs *ResolveLockArgs
	QueryLockArgs   *QueryLockArgs
}

// ReadArgs is the payload of CmdRead.
type ReadArgs struct {
	StartTS Timestamp
	Key     Key
}

// SealArgs is the payload of CmdSeal. It writes the decision timestamp into a
// lock record without publishing the write, which is what makes the primary
// lock the durable commit point of the transaction.
type SealArgs struct {
	Key      Key
	TxnID    TxnID
	StartTS  Timestamp
	CommitTS Timestamp
}

// ResolveLockArgs is the payload of CmdResolveLock. The coordinator has already
// established whether the owning transaction committed; the state machine just
// applies the verdict to this key.
type ResolveLockArgs struct {
	TxnID    TxnID
	ClientID int64
	// RequestID makes the resolution retry-safe: applying the same resolution
	// twice is a no-op.
	RequestID int64

	StartTS Timestamp
	Key     Key
	// Primary is the key whose lock records the transaction's decision.
	Primary Key
	// Committed is the verdict: true publishes the write, false discards it.
	Committed bool
	// CommitTS is the timestamp the transaction committed at, supplied by the
	// resolver when it learned the decision from the primary's group.
	//
	// It must be non-zero for a committed verdict: a secondary lock may never
	// have been sealed itself, so this group has no other way to learn the
	// timestamp.
	CommitTS Timestamp
	// Force means the verdict must be applied even if the lock has not expired.
	// It is used when the resolver has independently established that the owning
	// transaction can no longer make progress.
	Force bool
}

// QueryLockArgs is the payload of CmdQueryLock.
type QueryLockArgs struct {
	TxnID TxnID
	Key   Key
}

// LockInfo is the state-machine view of one lock, returned by Read and
// QueryLock. It is the information a coordinator needs to resolve a lock.
type LockInfo struct {
	// Exists is false when no lock record is held for the key.
	Exists bool
	// Key is the key this lock protects.
	Key Key
	// TxnID is the owning transaction.
	TxnID TxnID
	// Primary is the key whose lock is the transaction's commit point.
	Primary Key
	// StartTS is the owning transaction's snapshot.
	StartTS Timestamp
	// CommitTS is non-zero once the owner has decided to commit.
	CommitTS Timestamp
	// Kind is the lock's operation kind.
	Kind OpKind
	// ExpiresAt is the owner lease deadline in wall-clock milliseconds.
	ExpiresAt uint64
	// ActorCommitTS is the commit timestamp the group recorded for the owning
	// transaction, even when this particular lock record has not been sealed yet.
	// It is how a resolver learns the timestamp to finish a *secondary* lock: the
	// primary lock's group knows the decision, and the secondary lock itself may
	// never have been sealed.
	ActorCommitTS Timestamp
	// State is the owning transaction's lifecycle state as this group knows it.
	//
	// This is what recovery keys off. A lock whose transaction is
	// StateCommitted must be finished with a commit; a lock whose transaction is
	// still StatePrepared belongs to a live coordinator and must be waited for;
	// anything else may be rolled back once its lease expires. CommitTS alone
	// cannot express that distinction, because a transaction's commit point is
	// the *primary* lock, which may live in a different Raft group than the lock
	// being inspected.
	State State
}

// TxnLock converts the state-machine view into the coordinator-facing TxnLock.
func (li LockInfo) TxnLock() TxnLock {
	return TxnLock{
		Key:       li.Key,
		TxnID:     li.TxnID,
		Primary:   li.Primary,
		StartTS:   li.StartTS,
		CommitTS:  li.CommitTS,
		Kind:      li.Kind,
		ExpiresAt: li.ExpiresAt,
	}
}

// Expired reports whether the lock's owner lease has run out at nowMillis.
func (li LockInfo) Expired(nowMillis uint64) bool {
	return li.Exists && li.ExpiresAt != 0 && nowMillis > li.ExpiresAt
}

// TxnResult is the outcome of applying one TxnCommand. It is deliberately a
// flat, gob-friendly record: it is produced inside the Raft apply path on the
// leader and handed straight back to the submitting goroutine.
type TxnResult struct {
	Status Status

	// Value is the result of CmdRead.
	Value Value
	// Found reports whether the key existed at the read snapshot.
	Found bool

	// Lock carries the blocking lock for StatusLocked, and the lock record for
	// CmdQueryLock.
	Lock *LockInfo
	// MinCommitTS is meaningful with StatusLocked.
	MinCommitTS Timestamp

	// ConflictStart/ConflictCommit describe the interfering transaction for
	// StatusWriteConflict.
	ConflictStart  Timestamp
	ConflictCommit Timestamp

	// State is the transaction state after the command was applied (when the
	// command names a transaction).
	State State
	// CommitTS is the effective commit timestamp after CmdCommit/one-shot
	// CmdPrewrite.
	CommitTS Timestamp
	// Prepared is set by CmdPrewrite when the group now holds all intent locks
	// for the transaction.
	Prepared bool
	// KeysFinalized reports how many keys CmdCommit published.
	KeysFinalized int
	// Err messages an internal failure in human-readable form.
	Err string
}

// Failed reports whether the result represents an error.
func (r TxnResult) Failed() bool { return r.Status != StatusOK && r.Status != StatusAlreadyCommitted }

func init() {
	// The command travels through raft.LogEntry.Command, which Raft gob-encodes
	// when persisting the log; gob needs concrete types registered.
	gob.Register(TxnCommand{})
}
