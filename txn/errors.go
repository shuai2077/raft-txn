package txn

import (
	"errors"
	"fmt"
)

// Status is the outcome code a replicated state-machine operation reports back
// to the coordinator. It travels on the wire, so the numeric values are part of
// the protocol and must not be reordered.
type Status int

const (
	// StatusInvalid is the zero value: never a valid outcome.
	StatusInvalid Status = iota
	// StatusOK means the operation was applied.
	StatusOK
	// StatusNotLeader means the receiving replica is not the group leader and
	// the caller should retry against the leader hinted in the response.
	StatusNotLeader
	// StatusLocked means another transaction holds an unexpired lock.
	StatusLocked
	// StatusWriteConflict means the key was written by a newer committed
	// transaction, so this transaction cannot write it at its snapshot.
	StatusWriteConflict
	// StatusAlreadyCommitted means the group has already finalized this
	// transaction; the operation is a no-op and the commit timestamp is valid.
	StatusAlreadyCommitted
	// StatusTxnNotFound means the named transaction has no state on this group.
	StatusTxnNotFound
	// StatusKeyNotInWriteSet means the coordinator and the state machine disagree
	// about the transaction's write set, which indicates a bug.
	StatusKeyNotInWriteSet
	// StatusCommitTSOutOfOrder means a commit arrived with a timestamp that is
	// not strictly greater than the transaction's start timestamp.
	StatusCommitTSOutOfOrder
	// StatusInternal marks an unexpected state-machine failure.
	StatusInternal
)

var statusNames = map[Status]string{
	StatusInvalid:            "invalid",
	StatusOK:                 "OK",
	StatusNotLeader:          "not-leader",
	StatusLocked:             "locked",
	StatusWriteConflict:      "write-conflict",
	StatusAlreadyCommitted:   "already-committed",
	StatusTxnNotFound:        "txn-not-found",
	StatusKeyNotInWriteSet:   "key-not-in-write-set",
	StatusCommitTSOutOfOrder: "commit-ts-out-of-order",
	StatusInternal:           "internal",
}

func (s Status) String() string {
	if name, ok := statusNames[s]; ok {
		return name
	}
	return fmt.Sprintf("status(%d)", int(s))
}

// OK reports whether the status indicates success.
func (s Status) OK() bool { return s == StatusOK || s == StatusAlreadyCommitted }

// NotLeaderError is returned when a request reached a replica that is not the
// leader of its group. LeaderID is a routing hint (-1 when unknown); callers
// must not treat it as authoritative, since leadership can move at any time.
type NotLeaderError struct {
	GroupID  GroupID
	LeaderID int
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("txn: group %d: not the leader (leader hint: %d)", e.GroupID, e.LeaderID)
}

// LeaderHinter is implemented by Participants that can name the node currently
// believed to lead their group. The coordinator uses it to aim the next attempt
// directly instead of probing group members one by one.
type LeaderHinter interface {
	// LeaderHint returns the node id that leads the participant's group, or -1
	// when unknown. It must not perform I/O.
	LeaderHint() int
}

// LockedError reports that a key is protected by another transaction's intent
// lock, so the requested read or write cannot proceed yet.
//
// The coordinator resolves the situation by asking the lock's home group (see
// TxnLock.Primary) whether the transaction committed, then either committing or
// rolling back the lock and retrying.
type LockedError struct {
	Key Key
	// Lock describes the blocking lock.
	Lock TxnLock
	// MinCommitTS is the smallest commit timestamp a new transaction writing this
	// key may use. It implements the Percolator "push" rule that keeps a snapshot
	// read from missing a write it already observed as locked.
	MinCommitTS Timestamp
}

func (e *LockedError) Error() string {
	return fmt.Sprintf("txn: key %q is locked by txn %d (start ts %d, kind %s)",
		e.Key, e.Lock.TxnID, e.Lock.StartTS, e.Lock.Kind)
}

// WriteConflictError reports a write-write conflict: another transaction wrote
// this key and committed after our snapshot was taken.
//
// This is the fundamental mechanism behind the isolation guarantee: a
// transaction can only commit if no newer version of any key in its write set
// appeared since its start timestamp. Callers typically retry the transaction
// from scratch.
type WriteConflictError struct {
	Key Key
	// StartTS is the conflicting transaction's start timestamp.
	StartTS Timestamp
	// CommitTS is the timestamp the conflicting writer committed at, when it is
	// already committed (0 if it is still in flight).
	CommitTS Timestamp
}

func (e *WriteConflictError) Error() string {
	if e.CommitTS != 0 {
		return fmt.Sprintf("txn: write conflict on key %q: committed at ts %d, our snapshot is %d",
			e.Key, e.CommitTS, e.StartTS)
	}
	return fmt.Sprintf("txn: write conflict on key %q: concurrent transaction with start ts %d",
		e.Key, e.StartTS)
}

// AlreadyCommittedError reports that a transaction has already reached its
// terminal committed state. Operations that arrive after that point (typically
// because a client retried an RPC whose response was lost) are rejected.
type AlreadyCommittedError struct {
	TxnID    TxnID
	CommitTS Timestamp
}

func (e *AlreadyCommittedError) Error() string {
	return fmt.Sprintf("txn: transaction %d already committed at ts %d", e.TxnID, e.CommitTS)
}

// KeyNotFoundError reports that a key has no version visible at the requested
// snapshot. It is a normal outcome of a read, not a failure: it covers both
// "never written" and "deleted before this snapshot".
type KeyNotFoundError struct{ Key Key }

func (e *KeyNotFoundError) Error() string {
	return fmt.Sprintf("txn: key %q not found at this snapshot", e.Key)
}

// ErrLockGone is returned when an operation expected to find an intent lock and
// found none. For the commit path this is meaningful rather than benign: it
// means somebody else already resolved the lock, and the coordinator must find
// out whether that resolution committed or aborted the transaction.
var ErrLockGone = errors.New("txn: intent lock not held")

// CommitUnknownError reports that a transaction definitely committed, but the
// coordinator failed to finish (or to observe the completion of) the last step.
//
// The transaction is not lost: the primary lock records the commit, and any
// reader that encounters a leftover lock will complete it. Callers should treat
// the writes as durable and visible, and may retry for a cleaner answer.
type CommitUnknownError struct {
	TxnID    TxnID
	CommitTS Timestamp
	Err      error
}

func (e *CommitUnknownError) Error() string {
	return fmt.Sprintf("txn: transaction %d committed at ts %d but finalization failed: %v",
		e.TxnID, e.CommitTS, e.Err)
}

func (e *CommitUnknownError) Unwrap() error { return e.Err }

// TxnNotFoundError reports that the state machine has no record of a
// transaction, which normally means it was already rolled back and garbage
// collected.
type TxnNotFoundError struct{ TxnID TxnID }

func (e *TxnNotFoundError) Error() string {
	return fmt.Sprintf("txn: transaction %d not found", e.TxnID)
}

// ProtocolError reports the coordinator and the state machine disagreeing about
// a transaction's shape. It always indicates a bug rather than a data race.
type ProtocolError struct {
	TxnID  TxnID
	Reason string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("txn: transaction %d: protocol error: %s", e.TxnID, e.Reason)
}

// ErrorForStatus converts a state-machine status (and its optional payload) into
// a typed error. It is the single place where the wire status codes turn back
// into the rich errors the coordinator reasons about, which keeps the client
// logic readable.
func ErrorForStatus(st Status, key Key, lock *LockInfo, minCommitTS Timestamp,
	conflictStart, conflictCommit Timestamp) error {
	switch st {
	case StatusOK, StatusAlreadyCommitted:
		return nil
	case StatusLocked:
		if lock == nil {
			return errors.New("txn: status locked without lock payload")
		}
		return &LockedError{Key: key, Lock: lock.TxnLock(), MinCommitTS: minCommitTS}
	case StatusWriteConflict:
		return &WriteConflictError{Key: key, StartTS: conflictStart, CommitTS: conflictCommit}
	default:
		return fmt.Errorf("txn: unexpected status %s", st)
	}
}

// IsLocked reports whether err is a LockedError and returns it.
func IsLocked(err error) (*LockedError, bool) {
	var le *LockedError
	if errors.As(err, &le) {
		return le, true
	}
	return nil, false
}

// IsWriteConflict reports whether err is a WriteConflictError and returns it.
func IsWriteConflict(err error) (*WriteConflictError, bool) {
	var wc *WriteConflictError
	if errors.As(err, &wc) {
		return wc, true
	}
	return nil, false
}

// IsNotLeader reports whether err is a NotLeaderError and returns it.
func IsNotLeader(err error) (*NotLeaderError, bool) {
	var nl *NotLeaderError
	if errors.As(err, &nl) {
		return nl, true
	}
	return nil, false
}
