// Package txn implements distributed ACID transactions on top of a cluster of
// Raft replication groups (a "Multi-Raft" deployment).
//
// The design follows the Percolator / TiKV model:
//
//   - Data is sharded across several independent Raft groups. Each group runs
//     the very same Raft consensus module from internal/raft as a replicated
//     state machine holding one shard of the key space.
//   - Every key holds a chain of multi-version (MVCC) records, so readers can
//     read a consistent snapshot at a timestamp.
//   - A transaction is coordinated across groups with two-phase commit (2PC).
//     The lock written on the "primary" key is the atomic commit point.
//   - Locks are resolved lazily: any reader that trips over a stale lock
//     finishes the transaction one way or the other, so progress never depends
//     on a coordinator surviving a crash.
//
// This file holds the shared vocabulary: identifiers, timestamps, keys, the
// per-group RPC surface (Participant/Resolver) and the timestamp oracle.
package txn

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"
)

// Timestamp is a globally comparable logical time. It is handed out by an
// Oracle (see internal/tso) and plays two distinct roles in the protocol:
//
//   - StartTS: the snapshot at which a transaction reads, and the identity of
//     its locks.
//   - CommitTS: the version at which a transaction's writes become visible.
//
// Timestamps are monotonically non-decreasing and globally unique, which is
// what makes "is version V visible to snapshot S" a simple comparison.
type Timestamp uint64

// TxnID identifies a transaction globally.
type TxnID uint64

// Key uniquely identifies a row of the database. The Multi-Raft stack is not
// restricted to string keys - it just needs the bytes to be stable.
type Key string

// Value is the value stored under a Key.
type Value string

// Encode returns the byte representation of k used for hashing and routing.
func (k Key) Encode() []byte { return []byte(k) }

// Clone returns a copy of k, safe to retain.
func (k Key) Clone() Key { return Key(strings.Clone(string(k))) }

// OpKind describes what a write does to a key.
type OpKind int

const (
	// OpPut stores Value under the key.
	OpPut OpKind = iota
	// OpDelete marks the key as deleted. Deletion is a real MVCC record, not an
	// absence of records, so that a snapshot taken before the delete still sees
	// the old value.
	OpDelete
)

func (ok OpKind) String() string {
	switch ok {
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	default:
		return fmt.Sprintf("opkind(%d)", int(ok))
	}
}

// WriteOp is a single key mutation inside a transaction.
type WriteOp struct {
	Kind  OpKind
	Key   Key
	Value Value
}

// NewPut builds a put operation.
func NewPut(k Key, v Value) WriteOp { return WriteOp{Kind: OpPut, Key: k, Value: v} }

// NewDelete builds a delete operation.
func NewDelete(k Key) WriteOp { return WriteOp{Kind: OpDelete, Key: k} }

// State is the lifecycle state of a transaction as tracked by the replicated
// state machine. It is materialized on every group the transaction touched, and
// the state transitions are themselves replicated through Raft.
type State int

const (
	// StateInvalid is the zero value and never a valid state.
	StateInvalid State = iota
	// StatePending means the transaction was registered but not yet prepared.
	StatePending
	// StatePrepared means intent locks are in place and commit is decided.
	StatePrepared
	// StateCommitted is terminal: the transaction's writes are visible.
	StateCommitted
	// StateAborted is terminal: the transaction's writes will never be visible.
	StateAborted
)

func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StatePrepared:
		return "prepared"
	case StateCommitted:
		return "committed"
	case StateAborted:
		return "aborted"
	default:
		return "invalid"
	}
}

// Final reports whether the state can no longer change.
func (s State) Final() bool { return s == StateCommitted || s == StateAborted }

// Oracle is the source of timestamps for transactions. Implementations must be
// strictly monotonic, unique, and fault tolerant (see internal/tso for a
// Raft-replicated implementation).
//
// The transaction client treats the Oracle as an opaque, possibly remote,
// monotonically increasing clock; everything else in this package needs only
// NowMillis to expire locks that belong to dead coordinators.
type Oracle interface {
	// ReadTS returns the timestamp a new read-only or read-write transaction
	// should use as its snapshot.
	ReadTS(ctx context.Context) (Timestamp, error)

	// CommitTS returns a fresh timestamp at which the current transaction's
	// writes become visible. It must be greater than startTS.
	CommitTS(ctx context.Context, startTS Timestamp) (Timestamp, error)

	// Close releases any resources held by the oracle client.
	Close() error
}

// OracleFunc is an in-process Oracle built from two closures, used by tests and
// by single-process deployments.
type OracleFunc struct {
	// ReadNow returns the current read timestamp.
	ReadNow func() Timestamp
	// CommitNow returns the current commit timestamp.
	CommitNow func() Timestamp
}

func (o OracleFunc) ReadTS(ctx context.Context) (Timestamp, error) {
	if o.ReadNow == nil {
		return 0, fmt.Errorf("txn: OracleFunc has no ReadNow")
	}
	return o.ReadNow(), nil
}

func (o OracleFunc) CommitTS(ctx context.Context, startTS Timestamp) (Timestamp, error) {
	if o.CommitNow == nil {
		return 0, fmt.Errorf("txn: OracleFunc has no CommitNow")
	}
	ts := o.CommitNow()
	if ts <= startTS {
		return 0, fmt.Errorf("txn: oracle returned commit ts %d <= start ts %d", ts, startTS)
	}
	return ts, nil
}

func (o OracleFunc) Close() error { return nil }

// LocalOracle is a self-contained timestamp oracle for single-process use: a
// monotonically increasing counter that hands out one timestamp per call.
//
// It is deliberately the simplest thing that satisfies the Oracle contract. It
// is *not* fault tolerant on its own; internal/tso implements the replicated
// version used by the distributed deployment.
type LocalOracle struct {
	mu   sync.Mutex
	last Timestamp
}

// NewLocalOracle creates a LocalOracle. The first timestamp returned is
// start+1.
func NewLocalOracle(start Timestamp) *LocalOracle {
	return &LocalOracle{last: start}
}

func (o *LocalOracle) next() Timestamp {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.last++
	return o.last
}

func (o *LocalOracle) ReadTS(ctx context.Context) (Timestamp, error) { return o.next(), nil }

func (o *LocalOracle) CommitTS(ctx context.Context, startTS Timestamp) (Timestamp, error) {
	ts := o.next()
	if ts <= startTS {
		return 0, fmt.Errorf("txn: local oracle returned commit ts %d <= start ts %d", ts, startTS)
	}
	return ts, nil
}

func (o *LocalOracle) Close() error { return nil }

// NowMillis returns a monotonic wall-clock reading in milliseconds, used to
// expire locks left behind by a coordinator that died mid-commit.
func (o *LocalOracle) NowMillis() uint64 { return uint64(time.Now().UnixMilli()) }

// Participant is the surface a transaction coordinator needs from one Raft
// group. Every method is a linearizable, replicated state-machine operation.
//
// The service layer (internal/txnservice) implements it over HTTP; tests
// implement it in-process. Because each method is applied atomically by the
// group's Raft log, the state machine itself never has to worry about partial
// application within a group.
type Participant interface {
	// Read reads the key at snapshot startTS.
	//
	// A *KeyNotFoundError means the key has no version at that snapshot. A
	// *LockedError means an intent lock shadows the key and the caller must
	// resolve it before retrying.
	Read(ctx context.Context, startTS Timestamp, key Key) (Value, error)

	// Prewrite installs intent locks on all ops of the transaction. The group
	// is "prepared" once this returns nil.
	Prewrite(ctx context.Context, req PrewriteRequest) error

	// Seal records commitTS in the lock record of the given key without
	// publishing the write.
	//
	// Sealing the *primary* key is the transaction's atomic commit point: from
	// that instant every reader that finds any of the transaction's locks can
	// learn the outcome, without needing the coordinator again. It returns
	// ErrLockGone when the lock no longer exists.
	Seal(ctx context.Context, key Key, txnID TxnID, startTS, commitTS Timestamp) error

	// Commit finalizes the transaction's writes on this group at commitTS.
	//
	// With a non-empty Keys it publishes exactly those keys. With an empty Keys
	// it publishes every lock the group holds for the transaction, which is what
	// a resolving reader needs: it knows the transaction committed but not which
	// of the group's keys were involved.
	Commit(ctx context.Context, req CommitRequest) error

	// Rollback removes the transaction's intent locks on this group. As with
	// Commit, an empty Keys means "every lock held for this transaction".
	Rollback(ctx context.Context, req RollbackRequest) error

	// Locked reports the lock record the group holds for (txnID, key), or nil
	// when there is none. It is a read-only query used to discover a lock's
	// fate.
	Locked(ctx context.Context, txnID TxnID, key Key) (*LockInfo, error)

	// Barrier blocks until this group has applied every entry in its log as of
	// the moment of the call.
	//
	// It is the read barrier. A reader that takes a barrier on a group and then
	// reads that group at a snapshot timestamp acquired *before* the barrier was
	// taken is guaranteed not to miss a write that had already been decided: Raft
	// applies entries in log order, so the barrier cannot be applied until
	// everything ahead of it has been, and a write decided before the snapshot
	// was issued was proposed before the barrier.
	//
	// It changes no state. Without it, a cross-key snapshot read can see one side
	// of a committed transaction and not the other - see docs/03-read-barrier.md.
	Barrier(ctx context.Context) error
}

// Resolver clears intent locks whose owning transaction is dead.
//
// Resolving a lock is a state-machine operation - it writes to the group's lock
// table - so it has to go through Raft, and the coordinator that tripped over
// the lock may be attached to a completely different group. Hence this interface
// rather than direct access to the lock's group.
type Resolver interface {
	// ResolveLock disposes of lock, which the caller found in group `group`.
	//
	// The implementation must first establish the owning transaction's fate on
	// its primary group, and then:
	//
	//   - if the transaction committed: publish the write. Rollback must not be
	//     used here, or a committed value would be silently lost.
	//   - if the transaction is still undecided and its lease is live: leave the
	//     lock alone and return an error, so the caller waits.
	//   - otherwise: discard the lock.
	//
	// Implementations must be idempotent, and must never discard a lock that
	// belongs to a committed transaction.
	ResolveLock(ctx context.Context, group GroupID, lock TxnLock) error
}

// PrewriteRequest asks a group to take intent locks for a transaction.
type PrewriteRequest struct {
	TxnID    TxnID
	ClientID int64
	// RequestID is the per-client, monotonically increasing request number used
	// for exactly-once semantics across retries.
	RequestID int64

	StartTS Timestamp
	// Primary is the key whose lock is the atomic commit point. It must be one
	// of the keys in Ops.
	Primary Key
	Ops     []WriteOp

	// OneShot tells the state machine to commit the transaction in the same log
	// entry that creates its locks (single-group fast path).
	OneShot bool
	// CommitTS is required when OneShot is set, and is the timestamp the writes
	// become visible at.
	CommitTS Timestamp
}

// CommitRequest finalizes an already prepared transaction on one group.
type CommitRequest struct {
	TxnID     TxnID
	ClientID  int64
	RequestID int64

	StartTS  Timestamp
	CommitTS Timestamp
	// Keys are the keys to finalize on this group. It is a subset of the
	// transaction's write set.
	Keys []Key
}

// RollbackRequest aborts an in-flight transaction on one group.
type RollbackRequest struct {
	TxnID     TxnID
	ClientID  int64
	RequestID int64

	StartTS Timestamp
	Keys    []Key

	// RecordDecision asks the group to record "this transaction aborted" as a
	// durable decision before dropping any lock.
	//
	// It exists so a reader can recover a transaction whose coordinator is gone.
	// A crashed coordinator cannot be relied on to have written an abort decision,
	// and a reader must not infer one from a lapsed lease - that inference is what
	// let a reader destroy a write its owner was still about to commit. So the
	// reader *creates* the decision instead, atomically: the group re-checks the
	// transaction's state and refuses to abort one that has committed.
	//
	// The coordinator sets it on its own abort path too, which makes the primary's
	// decision precede any secondary rollback in every case.
	RecordDecision bool
}

// Partitioner decides which Raft group owns a key. Routing is deliberately an
// interface: a real deployment would use a range or hash partitioner that can
// be rebalanced, while tests pin the mapping for determinism.
type Partitioner interface {
	// GroupForKey returns the group ID owning key. It must be a pure function of
	// key: every participant has to agree on the answer.
	GroupForKey(key Key) GroupID
}

// GroupID identifies one Raft replication group (one shard of the key space).
type GroupID int

// GroupSet describes a group's membership: the Raft server IDs in the group.
// The transaction service needs it to route RPCs to the group's leader.
type GroupSet map[GroupID][]int

// HashPartitioner distributes keys over nGroups groups using FNV-1a. It is the
// production-style default; HashPartitioner is deterministic, so all
// participants and clients agree without coordination.
type HashPartitioner struct {
	nGroups int
}

// NewHashPartitioner returns a Partitioner over nGroups groups.
func NewHashPartitioner(nGroups int) *HashPartitioner {
	if nGroups < 1 {
		panic("txn: NewHashPartitioner needs at least one group")
	}
	return &HashPartitioner{nGroups: nGroups}
}

func (hp *HashPartitioner) GroupForKey(key Key) GroupID {
	h := fnv.New64a()
	_, _ = h.Write(key.Encode())
	return GroupID(h.Sum64() % uint64(hp.nGroups))
}

// NumGroups reports how many groups the partitioner spreads keys over.
func (hp *HashPartitioner) NumGroups() int { return hp.nGroups }

// StaticPartitioner maps keys to groups through an explicit table, defaulting to
// the group of the longest matching prefix. Tests use it to force a transaction
// to span specific groups.
type StaticPartitioner struct {
	// Prefixes maps a key prefix to a group. The longest matching prefix wins.
	Prefixes map[string]GroupID
	// Default is used for keys with no matching prefix.
	Default GroupID
}

func (sp *StaticPartitioner) GroupForKey(key Key) GroupID {
	best, bestLen := sp.Default, -1
	for prefix, group := range sp.Prefixes {
		if len(prefix) > bestLen && strings.HasPrefix(string(key), prefix) {
			best, bestLen = group, len(prefix)
		}
	}
	return best
}

// TxnLock is the durable description of an intent lock, as observed by a reader
// that bumped into it. It carries everything needed to resolve the lock: the
// key it protects, the home group (Primary), the lock's identity (StartTS) and,
// if the owner already decided to commit, the commit timestamp.
type TxnLock struct {
	// Key is the key this lock protects. A resolver must know it, because
	// resolving a lock is a write to the specific key's group.
	Key       Key
	TxnID     TxnID
	Primary   Key
	StartTS   Timestamp
	CommitTS  Timestamp
	Kind      OpKind
	ExpiresAt uint64
}

// Expired reports whether the lock's owner lease has run out at nowMillis.
func (l TxnLock) Expired(nowMillis uint64) bool {
	return l.ExpiresAt != 0 && nowMillis > l.ExpiresAt
}

// KeySet is a set of keys with insertion-order independent semantics.
type KeySet map[Key]struct{}

// NewKeySet builds a KeySet from keys.
func NewKeySet(keys ...Key) KeySet {
	ks := make(KeySet, len(keys))
	for _, k := range keys {
		ks[k] = struct{}{}
	}
	return ks
}

// Add inserts k into the set.
func (ks KeySet) Add(k Key) { ks[k] = struct{}{} }

// Has reports whether k is in the set.
func (ks KeySet) Has(k Key) bool { _, ok := ks[k]; return ok }

// Len returns the number of keys in the set.
func (ks KeySet) Len() int { return len(ks) }

// Sorted returns the set's keys in deterministic order.
func (ks KeySet) Sorted() []Key { return sortedKeys(ks) }

// Clone returns an independent copy of the set.
func (ks KeySet) Clone() KeySet { return NewKeySet(ks.Sorted()...) }

// --- small helpers -----------------------------------------------------------

// sortedKeys returns the keys of a set in deterministic (lexicographic) order.
// Determinism matters: replicated state machines must produce byte-identical
// results on every replica, so iteration order can never leak into state.
func sortedKeys[T any](m map[Key]T) []Key {
	keys := make([]Key, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortKeys(keys)
	return keys
}

func sortKeys(keys []Key) {
	// Insertion sort: key sets are tiny (a transaction touches a handful of
	// keys), and this keeps the package free of a sort dependency for the hot
	// path. For larger sets it degenerates, but so does the workload.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
}
