package txn

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/eliben/raft/txnkv/internal/raft"
)

// This file connects the transactional state machine (Store) to the Raft
// consensus module (internal/raft).
//
// Data flow for one replicated transaction command:
//
//	Store.Prewrite
//	  -> RaftLog.ApplyCommand
//	     -> raft.Server.Apply            (takes the ConsensusModule lock)
//	        -> ConsensusModule.applyHandler
//	           - appends the command to the Raft log
//	           - persists currentTerm, votedFor and the log
//	           - calls Store.Apply (the replicated state machine)
//	           - nudges the commit applier if the entry is already committed
//	        <- the state machine's output travels back up the same call stack
//
// Because the leader's state machine is updated inside the same critical section
// that appends the log entry, Raft's own ordering guarantee ("this entry is at
// index N") and the state machine's ("version V sorts after version U") cannot
// disagree. That is the whole reason the transaction layer can treat a single
// Raft group as a linearizable transaction participant without any additional
// synchronization.

// RaftLog adapts a *raft.Server to the Log interface the Store needs, and
// additionally implements Participant so a client can talk to the group
// directly (used by tests and by in-process deployments).
type RaftLog struct {
	server *raft.Server

	// groupID is used for error messages and for the NotLeaderError hint.
	groupID GroupID

	// mu guards the late binding of store (SetStore is called after the server
	// has started, while ApplyCommand may already run from another goroutine) and
	// the learnedLeader hint.
	mu    sync.RWMutex
	store *Store

	// learnedLeader is the node id a previous rejected proposal was told leads
	// the group. It lets the coordinator aim its next attempt correctly even
	// when this replica has not yet heard from the leader itself.
	learnedLeader int
}

// NewRaftLog wraps a started raft.Server as a transaction group.
func NewRaftLog(groupID GroupID, server *raft.Server) *RaftLog {
	return &RaftLog{server: server, groupID: groupID}
}

// SetStore wires the state machine into the Raft log. It must be called before
// commands are submitted.
func (rl *RaftLog) SetStore(store *Store) {
	rl.mu.Lock()
	rl.store = store
	rl.mu.Unlock()

	// Install the state machine in Raft. From here on every committed command is
	// applied to it, in log order, on this replica.
	rl.server.SetHandler(store)
}

// Store returns the wired state machine, or nil.
func (rl *RaftLog) Store() *Store {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.store
}

// LeaderHint implements LeaderHinter: it reports the node that leads this
// group, without performing any I/O.
func (rl *RaftLog) LeaderHint() int {
	if id := rl.server.LeaderID(); id >= 0 {
		return id
	}
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.learnedLeader
}

// learnLeader records a leadership hint obtained from a rejected proposal.
func (rl *RaftLog) learnLeader(id int) {
	if id < 0 {
		return
	}
	rl.mu.Lock()
	rl.learnedLeader = id
	rl.mu.Unlock()
}

// GroupID reports which group this log belongs to.
func (rl *RaftLog) GroupID() GroupID { return rl.groupID }

// ApplyCommand implements Log: it replicates cmd through this group's Raft
// instance and returns the state machine's result.
func (rl *RaftLog) ApplyCommand(ctx context.Context, cmd TxnCommand) (TxnResult, error) {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return TxnResult{}, ctx.Err()
		default:
		}
	}

	out, err := rl.server.Apply(cmd)
	if err != nil {
		var notLeader *raft.NotLeaderError
		if errors.As(err, &notLeader) {
			rl.learnLeader(notLeader.LeaderID)
			return TxnResult{}, &NotLeaderError{GroupID: rl.groupID, LeaderID: notLeader.LeaderID}
		}
		return TxnResult{}, err
	}

	// A leader that lost leadership between the check and the apply returns nil:
	// the command was not applied. Report it as a routing failure so the caller
	// retries against the new leader.
	if out == nil {
		return TxnResult{}, &NotLeaderError{GroupID: rl.groupID, LeaderID: rl.LeaderHint()}
	}

	res, ok := out.(TxnResult)
	if !ok {
		return TxnResult{}, fmt.Errorf("txn: group %d: state machine returned %T, want TxnResult",
			rl.groupID, out)
	}
	return res, nil
}

// Leader implements Log.
func (rl *RaftLog) Leader() int { return rl.server.LeaderID() }

// --- Participant implementation ---------------------------------------------

// Read implements Participant.
func (rl *RaftLog) Read(ctx context.Context, startTS Timestamp, key Key) (Value, error) {
	res, err := rl.ApplyCommand(ctx, TxnCommand{
		Kind:     CmdRead,
		ReadArgs: &ReadArgs{StartTS: startTS, Key: key},
	})
	return valueOrError(res, err, key)
}

// Prewrite implements Participant.
func (rl *RaftLog) Prewrite(ctx context.Context, req PrewriteRequest) error {
	reqCopy := req
	res, err := rl.ApplyCommand(ctx, TxnCommand{Kind: CmdPrewrite, PrewriteArgs: &reqCopy})
	return errorForResult(res, err, req.Primary)
}

// Seal implements Participant.
func (rl *RaftLog) Seal(ctx context.Context, key Key, txnID TxnID, startTS, commitTS Timestamp) error {
	res, err := rl.ApplyCommand(ctx, TxnCommand{
		Kind: CmdSeal,
		SealArgs: &SealArgs{
			Key: key, TxnID: txnID, StartTS: startTS, CommitTS: commitTS,
		},
	})
	if err != nil {
		return err
	}
	if res.Status == StatusLocked && res.Err == ErrLockGone.Error() {
		return ErrLockGone
	}
	return errorForResult(res, nil, key)
}

// Commit implements Participant.
func (rl *RaftLog) Commit(ctx context.Context, req CommitRequest) error {
	reqCopy := req
	res, err := rl.ApplyCommand(ctx, TxnCommand{Kind: CmdCommit, CommitArgs: &reqCopy})
	return errorForResult(res, err, firstKeyOrEmpty(req.Keys))
}

// Rollback implements Participant.
func (rl *RaftLog) Rollback(ctx context.Context, req RollbackRequest) error {
	reqCopy := req
	res, err := rl.ApplyCommand(ctx, TxnCommand{Kind: CmdRollback, RollbackArgs: &reqCopy})
	return errorForResult(res, err, firstKeyOrEmpty(req.Keys))
}

// Locked implements Participant: it asks the state machine for the lock this
// group holds for (txnID, key).
func (rl *RaftLog) Locked(ctx context.Context, txnID TxnID, key Key) (*LockInfo, error) {
	res, err := rl.ApplyCommand(ctx, TxnCommand{
		Kind:          CmdQueryLock,
		QueryLockArgs: &QueryLockArgs{TxnID: txnID, Key: key},
	})
	if err != nil {
		return nil, err
	}
	if res.Failed() {
		return nil, errorForResult(res, nil, key)
	}
	return res.Lock, nil
}

// Barrier implements Participant: it drives one no-op command through the log and
// waits for it to be applied.
//
// The wait is the whole point. ApplyCommand returns only once the entry has been
// committed by a quorum *and* the state machine has processed it, and Raft
// applies entries in log order, so returning from this call proves the group has
// applied everything that preceded the barrier in its log. That is exactly the
// read barrier's guarantee - see docs/03-read-barrier.md.
func (rl *RaftLog) Barrier(ctx context.Context) error {
	res, err := rl.ApplyCommand(ctx, TxnCommand{Kind: CmdBarrier})
	if err != nil {
		return err
	}
	if res.Failed() {
		return errorForResult(res, nil, "")
	}
	return nil
}

// --- shared conversion helpers ----------------------------------------------

func valueOrError(res TxnResult, err error, key Key) (Value, error) {
	if err != nil {
		return "", err
	}
	if res.Failed() {
		return "", errorForResult(res, nil, key)
	}
	if !res.Found {
		return "", &KeyNotFoundError{Key: key}
	}
	return res.Value, nil
}

func errorForResult(res TxnResult, err error, key Key) error {
	if err != nil {
		return err
	}
	if !res.Failed() {
		return nil
	}
	converted := ErrorForStatus(res.Status, key, res.Lock, res.MinCommitTS,
		res.ConflictStart, res.ConflictCommit)
	if converted != nil {
		return converted
	}
	if res.Err != "" {
		return fmt.Errorf("txn: %s", res.Err)
	}
	return fmt.Errorf("txn: unexpected result status %s", res.Status)
}

func firstKeyOrEmpty(keys []Key) Key {
	if len(keys) == 0 {
		return ""
	}
	return keys[0]
}
