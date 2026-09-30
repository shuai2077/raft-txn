package txn

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// This file implements the transaction coordinator: the piece that turns a
// sequence of reads and writes issued by an application into a Percolator-style
// two-phase commit across several Raft groups.
//
// Protocol recap
//
//	1. Begin:     startTS = oracle.ReadTS()          snapshot for all reads
//	2. Read:      read key at startTS; on a stale lock, resolve it and retry
//	3. Prewrite:  install intent locks on every key of the write set, in every
//	              group that owns one of them
//	4. CommitTS:  commitTS = oracle.CommitTS(startTS); fail here and the
//	              transaction can still be rolled back cleanly
//	5. Commit:    write commitTS into the PRIMARY lock, then convert each
//	              secondary lock into a version
//
// Steps 4-5 are the interesting part. Once the primary lock carries a commit
// timestamp, the transaction is committed for good: a coordinator that dies at
// that instant leaves secondary locks behind, and any reader that trips over
// one asks the primary's group what happened, sees "committed", and finishes
// the write itself. Atomicity therefore does not depend on the coordinator
// surviving - only on the single, atomic, Raft-replicated write to the primary.

// DefaultResolveTimeout bounds how long a reader waits for a lock that its
// owner is still actively working on before giving up. It exists to turn a
// stuck transaction into a visible error rather than a hang.
const DefaultResolveTimeout = 2 * LockTTL

// Client coordinates transactions over a set of Raft groups.
//
// One Client is used by many transactions; it holds no per-transaction state of
// its own, so it is safe for concurrent use.
type Client struct {
	oracle       Oracle
	partitioner  Partitioner
	participants map[GroupID]Participant
	resolver     Resolver

	// resolveTimeout bounds lock-resolution waiting. Zero means
	// DefaultResolveTimeout.
	resolveTimeout time.Duration

	// now returns the current time in milliseconds and is used to decide whether
	// a foreign lock's lease has expired. Production uses the wall clock; tests
	// substitute a controllable clock so that lease expiry can be exercised
	// instantly rather than by waiting out a real TTL.
	now func() uint64

	// clientID identifies this client process; combined with a per-operation
	// request ID it gives the replicated state machines a way to recognize
	// retries.
	clientID int64
	requests atomic.Int64
	txns     atomic.Uint64

	// Resolution statistics, for tests and observability.
	statsMu              sync.Mutex
	locksResolved        int
	staleLocksAborted    int
	foreignLocksFinished int
	writeConflicts       int
}

// NewClient builds a transaction coordinator.
//
//	oracle      - the cluster's timestamp source
//	partitioner - must agree with the routing used when the groups were created
//	participants- the groups this client can reach, keyed by group ID
//	resolver    - used to interrogate a lock's home group (usually the same
//	              object as the participant set, wrapped for foreign-group calls)
func NewClient(oracle Oracle, partitioner Partitioner, participants map[GroupID]Participant, resolver Resolver) *Client {
	return &Client{
		oracle:       oracle,
		partitioner:  partitioner,
		participants: participants,
		resolver:     resolver,
		now:          nowMillis,
		clientID:     nextClientID(),
	}
}

// SetClock replaces the clock used to judge whether a foreign lock's lease has
// expired. It must be called before the client resolves locks.
func (c *Client) SetClock(fn func() uint64) {
	if fn != nil {
		c.now = fn
	}
}

// clock returns the lease clock, defaulting to the wall clock.
func (c *Client) clock() func() uint64 {
	if c.now == nil {
		return nowMillis
	}
	return c.now
}

// ClientID returns the identifier this client stamps on its requests.
func (c *Client) ClientID() int64 { return c.clientID }

// Oracle returns the timestamp source this client uses.
func (c *Client) Oracle() Oracle { return c.oracle }

// SetResolveTimeout overrides how long reads wait on a live foreign lock.
func (c *Client) SetResolveTimeout(d time.Duration) {
	if d > 0 {
		c.resolveTimeout = d
	}
}

// SetResolver installs the lock-resolution policy.
//
// The coordinator and its resolver are mutually recursive - the resolver walks
// the coordinator's participant set, and the coordinator needs a resolver to
// clear foreign locks - so the resolver is bound after construction rather than
// passed in.
func (c *Client) SetResolver(r Resolver) { c.resolver = r }

var globalClientCount atomic.Int64

func nextClientID() int64 { return globalClientCount.Add(1) }

// nextRequestID hands out the monotonically increasing request numbers that
// make retries idempotent at the state machine.
func (c *Client) nextRequestID() int64 { return c.requests.Add(1) }

// ResolutionStats reports what lock recovery has done so far.
type ResolutionStats struct {
	// LocksResolved counts locks this client resolved.
	LocksResolved int
	// StaleLocksAborted counts locks rolled back because their owner was gone.
	StaleLocksAborted int
	// ForeignLocksFinished counts locks this client finished after discovering
	// their transaction had committed.
	ForeignLocksFinished int
	// WriteConflicts counts transactions refused because of a write-write race.
	WriteConflicts int
}

// Stats returns the resolution counters.
func (c *Client) Stats() ResolutionStats {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	return ResolutionStats{
		LocksResolved:        c.locksResolved,
		StaleLocksAborted:    c.staleLocksAborted,
		ForeignLocksFinished: c.foreignLocksFinished,
		WriteConflicts:       c.writeConflicts,
	}
}

func (c *Client) bumpStat(f func(*Client)) {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	f(c)
}

// Close releases the oracle client.
func (c *Client) Close() error {
	if c.oracle != nil {
		return c.oracle.Close()
	}
	return nil
}

// --- transaction handle ------------------------------------------------------

// Txn is a single in-flight transaction. It is not safe for concurrent use.
type Txn struct {
	c       *Client
	id      TxnID
	startTS Timestamp

	// writes buffers the transaction's mutations. Nothing is sent to the groups
	// until Commit, which is what makes the prepare phase the atomic unit.
	writes map[Key]WriteOp

	// prepared records the groups that accepted the transaction's locks, so that
	// a failure can be undone exactly on those groups.
	prepared map[GroupID]struct{}

	// primaryKey is the key whose lock is the commit point.
	primaryKey Key

	// committedTS is non-zero once the transaction is known to be committed.
	committedTS Timestamp

	// state tracks the transaction's lifecycle on the coordinator side, which is
	// what allows a precise diagnosis when a caller misuses the handle.
	state state
}

type state int

const (
	stateActive state = iota
	stateCommitted
	stateAborted
)

func (s state) String() string {
	switch s {
	case stateActive:
		return "active"
	case stateCommitted:
		return "committed"
	case stateAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// Begin starts a new transaction by taking a snapshot timestamp. No locks are
// taken and no state is created on any group yet: an empty transaction costs
// nothing, and its reads are served at its snapshot.
func (c *Client) Begin(ctx context.Context) (*Txn, error) {
	startTS, err := c.oracle.ReadTS(ctx)
	if err != nil {
		return nil, fmt.Errorf("txn: begin: %w", err)
	}
	return &Txn{
		c:        c,
		id:       TxnID(c.txns.Add(1)) | TxnID(c.clientID)<<40,
		startTS:  startTS,
		writes:   make(map[Key]WriteOp),
		prepared: make(map[GroupID]struct{}),
	}, nil
}

// ID returns the transaction's globally unique identifier.
func (t *Txn) ID() TxnID { return t.id }

// StartTS returns the snapshot timestamp of the transaction.
func (t *Txn) StartTS() Timestamp { return t.startTS }

// State reports the coordinator's view of the transaction.
func (t *Txn) State() string { return t.state.String() }

// Writes returns a copy of the transaction's buffered write set.
func (t *Txn) Writes() []WriteOp {
	ops := make([]WriteOp, 0, len(t.writes))
	for _, op := range t.writes {
		ops = append(ops, op)
	}
	return ops
}

// Get reads a key at the transaction's snapshot.
//
// Values written earlier in the same transaction are returned immediately
// (read-your-writes), which also guarantees repeatable reads for the
// transaction's own modifications.
func (t *Txn) Get(ctx context.Context, key Key) (Value, bool, error) {
	if err := t.checkActive(); err != nil {
		return "", false, err
	}
	if op, ok := t.writes[key]; ok {
		if op.Kind == OpDelete {
			return "", false, nil
		}
		return op.Value, true, nil
	}

	group := t.c.partitioner.GroupForKey(key)
	participant, err := t.c.participant(group)
	if err != nil {
		return "", false, err
	}

	deadline := time.Now().Add(t.c.resolveTimeoutOrDefault())
	for attempt := 0; ; attempt++ {
		value, err := participant.Read(ctx, t.startTS, key)
		if err == nil {
			return value, true, nil
		}
		var notFound *KeyNotFoundError
		if errors.As(err, &notFound) {
			return "", false, nil
		}

		locked, isLocked := IsLocked(err)
		if !isLocked {
			return "", false, err
		}

		// The key is shadowed by a foreign intent lock. Establish the owner's
		// fate and clear the lock, then retry the read.
		if err := t.c.resolveLock(ctx, group, locked.Lock); err != nil {
			return "", false, err
		}
		if time.Now().After(deadline) {
			return "", false, fmt.Errorf("txn: read of %q: %w", key,
				fmt.Errorf("timed out waiting for lock owned by txn %d", locked.Lock.TxnID))
		}
		if err := sleepCtx(ctx, resolveBackoff(attempt)); err != nil {
			return "", false, err
		}
	}
}

// Put buffers a write of value to key.
func (t *Txn) Put(key Key, value Value) error {
	if err := t.checkActive(); err != nil {
		return err
	}
	t.writes[key] = WriteOp{Kind: OpPut, Key: key, Value: value}
	return nil
}

// Delete buffers the deletion of key.
func (t *Txn) Delete(key Key) error {
	if err := t.checkActive(); err != nil {
		return err
	}
	t.writes[key] = WriteOp{Kind: OpDelete, Key: key}
	return nil
}

// Commit drives the transaction through two-phase commit and returns the
// commit timestamp at which its writes became visible.
//
// On a *WriteConflictError or *LockedError the transaction has been (or is
// being) rolled back and must be retried from the beginning by the caller.
// Other errors before the commit point also leave the transaction abortable.
//
// An error returned *after* the primary lock was committed to Raft is reported
// as a *CommitUnknownError: the transaction is committed, its writes will
// become visible once a reader resolves the remaining locks, but the
// coordinator did not manage to read the final timestamp back.
func (t *Txn) Commit(ctx context.Context) (Timestamp, error) {
	if t.state == stateCommitted {
		return t.committedTS, nil
	}
	if t.state == stateAborted {
		return 0, fmt.Errorf("txn: transaction %d was aborted", t.id)
	}

	if len(t.writes) == 0 {
		// Nothing to publish: a read-only transaction commits trivially.
		t.state = stateCommitted
		t.committedTS = t.startTS
		return t.committedTS, nil
	}

	prepared, err := t.prewrite(ctx)
	if err != nil {
		// The transaction never reached its commit point, so it is safe and
		// cheap to clean up. Failure to clean up is not fatal: the intent locks
		// carry a lease, and readers resolve them once it expires.
		_ = t.abortLockedGroups(ctx)
		t.state = stateAborted
		if wc, ok := IsWriteConflict(err); ok {
			_ = wc
			t.c.bumpStat(func(c *Client) { c.writeConflicts++ })
		}
		return 0, err
	}

	// The commit timestamp is fetched *after* prewrite. That ordering is
	// important: it guarantees commitTS is greater than the start timestamp of
	// any transaction that could have observed our locks, which is what makes
	// the push rule sound.
	commitTS, err := t.c.oracle.CommitTS(ctx, t.startTS)
	if err != nil {
		// No commit timestamp means no decision was taken, so the transaction can
		// never commit and its locks are removable. Release them rather than
		// leaving other writers blocked until the leases expire.
		_ = t.abortLockedGroups(ctx)
		t.state = stateAborted
		return 0, fmt.Errorf("txn: obtaining commit timestamp: %w", err)
	}

	// The atomic commit point.
	if err := t.commitPrimary(ctx, commitTS, prepared); err != nil {
		t.state = stateCommitted
		t.committedTS = commitTS
		return commitTS, &CommitUnknownError{TxnID: t.id, CommitTS: commitTS, Err: err}
	}

	// All decided; the remaining groups only need to catch up. Failures here are
	// recoverable by any reader, so they are reported but do not undo the commit.
	t.state = stateCommitted
	t.committedTS = commitTS
	var finalizeErr error
	for _, group := range prepared.groups {
		if group == prepared.primaryGroup {
			continue
		}
		if err := t.commitSecondary(ctx, group, commitTS); err != nil && finalizeErr == nil {
			finalizeErr = err
		}
	}
	if finalizeErr != nil {
		return commitTS, &CommitUnknownError{TxnID: t.id, CommitTS: commitTS, Err: finalizeErr}
	}
	return commitTS, nil
}

// Finish re-drives the finalization of a transaction that Commit reported as
// *possibly* incomplete (a *CommitUnknownError).
//
// This distinction matters more than it looks. Once the primary lock is sealed
// the transaction is committed for good, so it must not be rolled back - but its
// remaining writes may still be sitting in other groups as intents that nobody
// has published. A client that treats that as success may observe a partially
// applied transaction: exactly the atomicity violation the whole design exists to
// prevent.
//
// Finish publishes whatever is still pending, using the commit timestamp recorded
// at the commit point. It is idempotent: keys whose writes already landed are
// skipped, so calling it repeatedly (or from several clients) is safe.
//
// It returns nil once every group the transaction touched has published its
// writes. A non-nil error means some group is still unreachable or still holds an
// unexpired lock; the correct response is to retry, not to abandon the
// transaction, because the commit already happened.
func (t *Txn) Finish(ctx context.Context) error {
	if t.state != stateCommitted || t.committedTS == 0 {
		return fmt.Errorf("txn: transaction %d is %s; nothing to finish", t.id, t.state)
	}

	var firstErr error
	for group := range t.prepared {
		participant, err := t.c.participant(group)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		keys := make([]Key, 0, len(t.writes))
		for key := range t.writes {
			if t.c.partitioner.GroupForKey(key) == group {
				keys = append(keys, key)
			}
		}
		if len(keys) == 0 {
			continue
		}
		sortKeys(keys)

		// An empty CommitTS asks the group to use the timestamp its own lock
		// records - which the coordinator sealed at the commit point. That makes
		// this call work even if this particular Txn handle never learned the
		// timestamp.
		if err := participant.Commit(ctx, CommitRequest{
			TxnID:     t.id,
			ClientID:  t.c.clientID,
			RequestID: t.c.nextRequestID(),
			StartTS:   t.startTS,
			CommitTS:  t.committedTS,
			Keys:      keys,
		}); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Abort rolls the transaction back, releasing any locks it took.
func (t *Txn) Abort(ctx context.Context) error {
	if t.state == stateCommitted {
		return fmt.Errorf("txn: cannot abort committed transaction %d", t.id)
	}
	if t.state == stateAborted {
		return nil
	}
	err := t.abortLockedGroups(ctx)
	t.state = stateAborted
	return err
}

func (t *Txn) checkActive() error {
	if t.state != stateActive {
		return fmt.Errorf("txn: transaction %d is %s", t.id, t.state)
	}
	return nil
}

// Prewrite runs only the prepare phase of a transaction, leaving its intent
// locks in place.
//
// It exists so that tests (and administrators) can reproduce the state a
// coordinator leaves behind when it dies after preparing: intent locks with no
// commit decision. A real coordinator never needs it, because Commit performs
// the prepare itself.
func Prewrite(tx *Txn) error {
	if tx.state != stateActive {
		return fmt.Errorf("txn: transaction %d is %s", tx.id, tx.state)
	}
	if len(tx.writes) == 0 {
		return nil
	}
	_, err := tx.prewrite(context.Background())
	return err
}

// --- two-phase commit steps --------------------------------------------------

// prepResult carries the outcome of the prepare phase.
type prepResult struct {
	primaryGroup GroupID
	primaryKey   Key
	groups       []GroupID
}

func (t *Txn) prewrite(ctx context.Context) (*prepResult, error) {
	res, err := t.c.prewriteWrites(ctx, t.id, t.startTS, t.writes)
	// Record every group that may hold locks, even when the prepare failed
	// part-way: a failed prepare still has to be undone.
	for _, g := range res.groups {
		t.prepared[g] = struct{}{}
	}
	t.primaryKey = res.primaryKey
	if err != nil {
		return res, err
	}
	return res, nil
}

// prewriteWrites installs the transaction's intent locks in every group that
// owns part of the write set. The returned prepResult records which groups were
// prepared, so a later failure can be undone precisely.
func (c *Client) prewriteWrites(ctx context.Context, txnID TxnID, startTS Timestamp,
	writes map[Key]WriteOp) (*prepResult, error) {
	byGroup := make(map[GroupID][]WriteOp)
	for _, op := range writes {
		group := c.partitioner.GroupForKey(op.Key)
		byGroup[group] = append(byGroup[group], op)
	}

	// The primary key is chosen deterministically from the write set so that
	// every participant (and every recovery attempt) agrees on the commit point.
	primaryKey := primaryKeyOf(writes)
	primaryGroup := c.partitioner.GroupForKey(primaryKey)

	res := &prepResult{primaryGroup: primaryGroup, primaryKey: primaryKey}
	groups := make([]GroupID, 0, len(byGroup))
	for g := range byGroup {
		groups = append(groups, g)
	}
	sortGroupIDs(groups)
	res.groups = groups

	// Prepare group by group. We deliberately continue past the first failure so
	// that the caller knows the full set of groups to undo; the locks left behind
	// are inert until a reader resolves them.
	var firstErr error
	for _, group := range groups {
		ops := byGroup[group]
		sortWriteOps(ops)

		// Every group is told the name of the primary key, even though it only
		// stores the keys it owns. A lock must record where the transaction's
		// commit point lives: that is how a later reader knows which group to ask
		// before it decides whether the lock committed or died.
		req := PrewriteRequest{
			TxnID:     txnID,
			ClientID:  c.clientID,
			RequestID: c.nextRequestID(),
			StartTS:   startTS,
			Primary:   primaryKey,
			Ops:       ops,
		}
		participant, err := c.participant(group)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := participant.Prewrite(ctx, req); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return res, firstErr
}

// commitPrimary performs the single atomic write that decides the transaction.
// It must be attempted before any secondary is finalized, because the primary
// lock is what other readers consult.
func (t *Txn) commitPrimary(ctx context.Context, commitTS Timestamp, res *prepResult) error {
	participant, err := t.c.participant(res.primaryGroup)
	if err != nil {
		return err
	}

	// The commit timestamp must also be recorded in the lock record, so that a
	// reader resolving a *secondary* lock can learn it without contacting the
	// primary group. Publishing it here is a second Raft write to the primary
	// group; if it succeeds but the finalize below fails, readers still recover
	// correctly, because the lock now carries the timestamp.
	if err := participant.Seal(ctx, res.primaryKey, t.id, t.startTS, commitTS); err != nil {
		if errors.Is(err, ErrLockGone) {
			// Somebody already resolved the primary lock. Either the transaction
			// committed (fine, we are done) or it was aborted by a reader that
			// considered it dead - in which case our commit is too late and the
			// transaction cannot proceed. Distinguish by asking the group.
			state, cts, qerr := t.c.queryPrimaryDecision(ctx, res.primaryGroup, t.id, res.primaryKey)
			if qerr == nil && state == StateCommitted {
				_ = cts
				return nil
			}
			return fmt.Errorf("txn: primary lock for %d disappeared before commit (state=%v)", t.id, state)
		}
		return err
	}

	req := CommitRequest{
		TxnID:     t.id,
		ClientID:  t.c.clientID,
		RequestID: t.c.nextRequestID(),
		StartTS:   t.startTS,
		CommitTS:  commitTS,
		Keys:      []Key{res.primaryKey},
	}
	return participant.Commit(ctx, req)
}

// commitSecondary finalizes one non-primary group. Failures are recoverable: a
// reader that trips over the leftover lock will consult the primary and finish
// the job.
func (t *Txn) commitSecondary(ctx context.Context, group GroupID, commitTS Timestamp) error {
	participant, err := t.c.participant(group)
	if err != nil {
		return err
	}
	keys := make([]Key, 0, len(t.writes))
	for key, op := range t.writes {
		if t.c.partitioner.GroupForKey(key) == group && key != t.primaryKey {
			keys = append(keys, op.Key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sortKeys(keys)
	return participant.Commit(ctx, CommitRequest{
		TxnID:     t.id,
		ClientID:  t.c.clientID,
		RequestID: t.c.nextRequestID(),
		StartTS:   t.startTS,
		CommitTS:  commitTS,
		Keys:      keys,
	})
}

// abortLockedGroups removes the locks the transaction installed. It is used both
// by the user-facing Abort and by the failure path of Commit.
func (t *Txn) abortLockedGroups(ctx context.Context) error {
	var firstErr error
	for group := range t.prepared {
		participant, err := t.c.participant(group)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		keys := make([]Key, 0, len(t.writes))
		for key := range t.writes {
			if t.c.partitioner.GroupForKey(key) == group {
				keys = append(keys, key)
			}
		}
		if len(keys) == 0 {
			continue
		}
		sortKeys(keys)
		if err := participant.Rollback(ctx, RollbackRequest{
			TxnID:     t.id,
			ClientID:  t.c.clientID,
			RequestID: t.c.nextRequestID(),
			StartTS:   t.startTS,
			Keys:      keys,
		}); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// queryPrimaryDecision asks a group what it knows about a transaction's primary
// lock.
func (c *Client) queryPrimaryDecision(ctx context.Context, group GroupID, txnID TxnID, primary Key) (State, Timestamp, error) {
	participant, err := c.participant(group)
	if err != nil {
		return StateInvalid, 0, err
	}
	info, err := participant.Locked(ctx, txnID, primary)
	if err != nil {
		return StateInvalid, 0, err
	}
	if info == nil {
		return StateInvalid, 0, nil
	}
	if info.CommitTS > 0 {
		return StateCommitted, info.CommitTS, nil
	}
	return StatePrepared, 0, nil
}

// --- lock resolution ---------------------------------------------------------

// resolveLock disposes of a lock the caller tripped over, then returns so the
// caller can retry the operation that failed.
//
// This is the safety-critical part of the whole design. The rules are:
//
//	lock carries a timestamp  -> the owner decided to commit; finish the write
//	primary decided commit    -> finish the write
//	primary still undecided   -> the owner may be about to commit; wait
//	primary's actor is aborted-> discard the lock
//	primary's actor unknown   -> the owner died before deciding; discard
//
// The one thing that must never happen is discarding a lock whose transaction
// committed: that would silently lose an acknowledged write. So the decision is
// always taken by inspecting the *primary*, never by the age of this lock alone.
func (c *Client) resolveLock(ctx context.Context, group GroupID, lock TxnLock) error {
	if c.resolver == nil {
		return fmt.Errorf("txn: no lock resolver configured; cannot resolve lock on %q", lock.Primary)
	}

	// Fast path: this lock record itself already carries the commit timestamp,
	// because its owner sealed it. Finishing the write needs no other group.
	if lock.CommitTS != 0 {
		if err := c.finishLock(ctx, group, lock, lock.CommitTS); err != nil {
			return err
		}
		c.bumpStat(func(cl *Client) { cl.foreignLocksFinished++ })
		return nil
	}

	// Everything else is delegated: only the resolver can consult the primary
	// group and act with the full picture.
	if err := c.resolver.ResolveLock(ctx, group, lock); err != nil {
		return err
	}
	c.bumpStat(func(cl *Client) { c.locksResolved++ })
	return nil
}

// probePrimary asks the group owning a transaction's primary lock what happened
// to it. It returns the transaction's state as that group knows it, the commit
// timestamp when known, and the lock record (nil when the group knows nothing
// about the transaction at all).
func (c *Client) probePrimary(ctx context.Context, group GroupID, txnID TxnID,
	primary Key) (State, Timestamp, *LockInfo, error) {
	info, err := c.lookupLock(ctx, group, txnID, primary)
	if err != nil {
		return StateInvalid, 0, nil, err
	}
	if info == nil {
		return StateInvalid, 0, nil, nil
	}
	state := info.State
	if state == StateInvalid && info.CommitTS != 0 {
		// Tolerate a state machine that reports the decision only through the
		// timestamp.
		state = StateCommitted
	}
	// Prefer the timestamp sealed into this lock; fall back to the
	// transaction-level timestamp the group recorded. The latter is what lets a
	// resolver finish a secondary lock that was never sealed itself.
	commitTS := info.CommitTS
	if commitTS == 0 {
		commitTS = info.ActorCommitTS
	}
	return state, commitTS, info, nil
}

// ensurePrimaryResolved waits until the transaction that owns lock has decided
// (committed) or is provably dead, so that the caller can act on the answer.
func (c *Client) ensurePrimaryResolved(ctx context.Context, lock TxnLock) (State, Timestamp, error) {
	primaryGroup := c.partitioner.GroupForKey(lock.Primary)
	deadline := time.Now().Add(c.resolveTimeoutOrDefault())
	var lastSeen *LockInfo
	var lastState State

	for attempt := 0; ; attempt++ {
		state, commitTS, info, err := c.probePrimary(ctx, primaryGroup, lock.TxnID, lock.Primary)
		if err != nil {
			return StateInvalid, 0, fmt.Errorf("txn: probing primary lock of txn %d: %w", lock.TxnID, err)
		}
		lastSeen, lastState = info, state

		switch {
		case state == StateCommitted:
			return StateCommitted, commitTS, nil
		case state == StateAborted:
			return StateAborted, 0, nil
		case info == nil:
			// The primary group has no record of the transaction. It was rolled
			// back and garbage collected, so it never committed.
			return StateAborted, 0, nil
		}

		// Still prepared. Give a live owner a chance to finish; once its lease has
		// expired, declare the transaction dead.
		if !info.Expired(c.clock()()) {
			if time.Now().After(deadline) {
				return StateInvalid, 0, fmt.Errorf(
					"txn: timed out waiting for txn %d to decide about key %q (group %d last reported state=%v lock=%+v)",
					lock.TxnID, lock.Primary, primaryGroup, lastState, lastSeen)
			}
			if err := sleepCtx(ctx, resolveBackoff(attempt)); err != nil {
				return StateInvalid, 0, err
			}
			continue
		}
		return StatePrepared, 0, nil
	}
}

// lookupLock asks a group for the lock it holds for (txnID, key).
func (c *Client) lookupLock(ctx context.Context, group GroupID, txnID TxnID, key Key) (*LockInfo, error) {
	participant, err := c.participant(group)
	if err != nil {
		return nil, err
	}
	return participant.Locked(ctx, txnID, key)
}

// finishLock publishes one lock whose owning transaction is known to have
// committed. commitTS may be zero, in which case the group falls back to the
// timestamp in its own lock record.
func (c *Client) finishLock(ctx context.Context, group GroupID, lock TxnLock, commitTS Timestamp) error {
	participant, err := c.participant(group)
	if err != nil {
		return err
	}
	return participant.Commit(ctx, CommitRequest{
		TxnID:     lock.TxnID,
		ClientID:  c.clientID,
		RequestID: c.nextRequestID(),
		StartTS:   lock.StartTS,
		CommitTS:  commitTS,
		// Name the key explicitly. A resolver knows exactly which lock it tripped
		// over, and naming it keeps the write scoped to that key - which matters
		// when the group's transaction state is already "committed" while other
		// locks of the same transaction are still waiting to be published.
		Keys: []Key{lock.Key},
	})
}

// rollbackLock discards one lock whose owning transaction never committed.
func (c *Client) rollbackLock(ctx context.Context, group GroupID, lock TxnLock) error {
	participant, err := c.participant(group)
	if err != nil {
		return err
	}
	return participant.Rollback(ctx, RollbackRequest{
		TxnID:     lock.TxnID,
		ClientID:  c.clientID,
		RequestID: c.nextRequestID(),
		StartTS:   lock.StartTS,
		Keys:      nil,
	})
}

// resolveRemote is the Resolver-facing form of finishLock: it lives on Client so
// that DefaultResolver can reach arbitrary groups through the same participant
// set.
func (c *Client) resolveRemote(ctx context.Context, group GroupID, lock TxnLock, commitTS Timestamp) error {
	return c.finishLock(ctx, group, lock, commitTS)
}

// DefaultResolver is the standard Resolver implementation: it consults the
// primary group through the same Participant set the coordinator uses, then
// applies the resulting verdict to the group that holds the lock.
//
// It holds a pointer to the Client rather than a copy, so it observes the
// client's resolver binding as soon as it is set. That matters because the
// client and its resolver are mutually recursive: the resolver needs the
// client's participant set, and the client needs a resolver to clear locks.
type DefaultResolver struct {
	client *Client
}

// NewResolver builds the standard lock resolver for a client.
func NewResolver(client *Client) *DefaultResolver { return &DefaultResolver{client: client} }

// ResolveLock implements Resolver.
func (r *DefaultResolver) ResolveLock(ctx context.Context, group GroupID, lock TxnLock) error {
	c := r.client

	state, commitTS, err := c.ensurePrimaryResolved(ctx, lock)
	if err != nil {
		return err
	}

	if state == StateCommitted {
		// The transaction committed: publish this write too, so the reader's next
		// attempt finds a clean key and the value is not lost.
		if err := c.finishLock(ctx, group, lock, commitTS); err != nil {
			return err
		}
		c.bumpStat(func(cl *Client) { cl.foreignLocksFinished++ })
		return nil
	}

	// The transaction is provably dead. The caller asked about `group`, so that
	// is the lock it must be able to retry past; drop it first. The primary lock
	// on the *primary* key is dropped too, so other readers stop consulting it,
	// but only if it is actually held by this same transaction.
	if err := c.rollbackLockOnKey(ctx, group, lock.TxnID, lock.StartTS, lock.Key); err != nil {
		return err
	}
	primaryGroup := c.partitioner.GroupForKey(lock.Primary)
	if primaryGroup != group || lock.Key != lock.Primary {
		if err := c.rollbackLockOnKey(ctx, primaryGroup, lock.TxnID, lock.StartTS, lock.Primary); err != nil {
			return err
		}
	}
	c.bumpStat(func(cl *Client) { cl.staleLocksAborted++ })
	return nil
}

// rollbackLockOnKey discards the lock held on one specific key, leaving any other
// lock of the same transaction alone.
func (c *Client) rollbackLockOnKey(ctx context.Context, group GroupID, txnID TxnID,
	startTS Timestamp, key Key) error {
	participant, err := c.participant(group)
	if err != nil {
		return err
	}
	return participant.Rollback(ctx, RollbackRequest{
		TxnID:     txnID,
		ClientID:  c.clientID,
		RequestID: c.nextRequestID(),
		StartTS:   startTS,
		Keys:      []Key{key},
	})
}

func (c *Client) participant(group GroupID) (Participant, error) {
	p, ok := c.participants[group]
	if !ok {
		return nil, fmt.Errorf("txn: no participant registered for group %d", group)
	}
	return p, nil
}

func (c *Client) resolveTimeoutOrDefault() time.Duration {
	if c.resolveTimeout > 0 {
		return c.resolveTimeout
	}
	return DefaultResolveTimeout
}

// --- helpers -----------------------------------------------------------------

func primaryKeyOf(writes map[Key]WriteOp) Key {
	keys := make([]Key, 0, len(writes))
	for k := range writes {
		keys = append(keys, k)
	}
	sortKeys(keys)
	return keys[0]
}

func sortWriteOps(ops []WriteOp) {
	for i := 1; i < len(ops); i++ {
		for j := i; j > 0 && ops[j].Key < ops[j-1].Key; j-- {
			ops[j], ops[j-1] = ops[j-1], ops[j]
		}
	}
}

func sortGroupIDs(ids []GroupID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
}

// resolveBackoff returns the pause before re-probing a lock. It grows with the
// attempt number so that a hot contended key does not spin.
func resolveBackoff(attempt int) time.Duration {
	d := time.Duration(2<<uint(min(attempt, 6))) * time.Millisecond
	if d > 200*time.Millisecond {
		return 200 * time.Millisecond
	}
	return d
}

func nowMillis() uint64 { return uint64(time.Now().UnixMilli()) }

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
