package txn

// This file defines the physical MVCC representation used by the state machine
// in store.go.
//
// A committed write does not overwrite the previous value. Instead every
// successful transaction appends a Version to the key's chain, stamped with the
// timestamp the transaction committed at. A reader therefore never has to
// reconstruct history: it walks the chain and picks the newest version that was
// already committed at its snapshot timestamp.
//
//	key "a":  commitTS=10 "v1"   commitTS=25 "v2"   commitTS=40 delete
//	              ^                  ^                 ^
//	             a read at         a read at          a read at
//	             ts=15 sees        ts=30 sees         ts=45 sees
//	             "v1"              "v2"               nothing
//
// Because each group is itself a Raft-replicated state machine, a version is
// appended on every replica in the same log position, so the chains are
// byte-identical across replicas and across restarts (the log is replayed).

// Version is one committed record of a key's history.
type Version struct {
	// CommitTS is the timestamp at which the transaction that wrote this version
	// committed: the instant the version became visible.
	CommitTS Timestamp

	// StartTS is the commit timestamp of the previous version of the key, i.e.
	// the start of this version's validity interval. It is carried explicitly
	// (rather than recomputed) so that a version chain remains self-describing
	// and can be checked for ordering violations.
	StartTS Timestamp

	// Kind is the write operation that produced the version.
	Kind OpKind

	// Value is the stored value. For OpDelete it is empty.
	Value Value
}

// Visible reports whether this version is visible to a snapshot at ts.
func (v Version) Visible(ts Timestamp) bool { return v.CommitTS <= ts }

// LockRecord is the state machine's internal form of an intent lock. It is the
// heart of the two-phase commit protocol:
//
//   - During prewrite, a LockRecord is installed on every key the transaction
//     writes. It carries the pending value, so nothing is visible yet.
//   - The lock on the *primary* key is the transaction's commit point. Writing
//     CommitTS into the primary lock record is the instant the transaction
//     becomes committed, and the point from which recovery is possible by any
//     reader that encounters a secondary lock.
//   - On commit, the lock record is converted into a Version and removed.
type LockRecord struct {
	TxnID   TxnID
	StartTS Timestamp

	// Primary is the key whose lock records the transaction's decision.
	Primary Key

	// Kind and Value are the write that is being held back.
	Kind  OpKind
	Value Value

	// CommitTS is zero while the transaction is undecided. A non-zero value means
	// the owner decided to commit; a reader that finds such a lock can safely
	// finish the write itself.
	CommitTS Timestamp

	// ExpiresAt is the wall-clock deadline (milliseconds) after which the lock
	// may be rolled back by a reader. It bounds how long a crashed coordinator
	// can block a key, and it is the reason no distributed deadlock detector is
	// needed: a transaction that waits on a lock whose owner died simply waits
	// out its lease.
	ExpiresAt uint64
}

// Expired reports whether the lock's owner lease has run out.
func (l *LockRecord) Expired(nowMillis uint64) bool {
	return l.ExpiresAt != 0 && nowMillis > l.ExpiresAt
}

// Info returns the observable form of the lock for the given key.
func (l *LockRecord) InfoForKey(key Key) LockInfo {
	info := l.Info()
	info.Key = key
	return info
}

// Info returns the observable form of the lock. The caller must fill in Key when
// it knows which key the record belongs to; see InfoForKey.
func (l *LockRecord) Info() LockInfo {
	return LockInfo{
		Exists:    true,
		TxnID:     l.TxnID,
		Primary:   l.Primary,
		StartTS:   l.StartTS,
		CommitTS:  l.CommitTS,
		Kind:      l.Kind,
		ExpiresAt: l.ExpiresAt,
	}
}
