package txnservice

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
)

// This file defines the wire format between transaction clients and group
// members, plus the JSON helpers the HTTP handlers use.
//
// The format is deliberately explicit rather than reusing the txn package's Go
// types directly: the wire contract is a compatibility surface, so keeping it
// separate means the internal types can evolve without silently changing it.
//
// Every response embeds ResponseStatus, which carries both the outcome code and
// an optional routing hint. Encoding all three fields on every response - rather
// than defining a "routing error" shape and a "success" shape - means the client
// can decide how to react with one type switch, with no JSON sniffing.

// --- request types -----------------------------------------------------------

// ReadRequest asks for a key's value at a snapshot.
type ReadRequest struct {
	StartTS uint64 `json:"start_ts"`
	Key     string `json:"key"`
}

// WriteOpJSON is a single key mutation inside a prewrite request.
type WriteOpJSON struct {
	Kind  int    `json:"kind"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// PrewriteRequest asks a group to take intent locks for a transaction.
type PrewriteRequest struct {
	TxnID     uint64        `json:"txn_id"`
	ClientID  int64         `json:"client_id"`
	RequestID int64         `json:"request_id"`
	StartTS   uint64        `json:"start_ts"`
	Primary   string        `json:"primary"`
	Ops       []WriteOpJSON `json:"ops"`

	// OneShot asks the group to commit the transaction in the same log entry that
	// creates its locks. It is only valid when this group holds the whole write
	// set.
	OneShot  bool   `json:"one_shot,omitempty"`
	CommitTS uint64 `json:"commit_ts,omitempty"`
}

// CommitRequest asks a group to publish a prepared transaction's writes.
type CommitRequest struct {
	TxnID     uint64   `json:"txn_id"`
	ClientID  int64    `json:"client_id"`
	RequestID int64    `json:"request_id"`
	StartTS   uint64   `json:"start_ts"`
	CommitTS  uint64   `json:"commit_ts"`
	Keys      []string `json:"keys"`
}

// RollbackRequest asks a group to discard a transaction's intent locks.
type RollbackRequest struct {
	TxnID     uint64   `json:"txn_id"`
	ClientID  int64    `json:"client_id"`
	RequestID int64    `json:"request_id"`
	StartTS   uint64   `json:"start_ts"`
	Keys      []string `json:"keys"`
}

// SealRequest records a transaction's decision in one of its lock records.
type SealRequest struct {
	Key      string `json:"key"`
	TxnID    uint64 `json:"txn_id"`
	StartTS  uint64 `json:"start_ts"`
	CommitTS uint64 `json:"commit_ts"`
}

// LockedRequest asks which lock a group holds for (txn_id, key).
type LockedRequest struct {
	TxnID uint64 `json:"txn_id"`
	Key   string `json:"key"`
}

// BarrierRequest is the (empty) body of a read-barrier request.
//
// It carries nothing because a barrier's meaning is its position in the group's
// log, not its payload. It exists as a named type anyway so the request is sent
// as a body-bearing POST: the transport downgrades a bodyless request to GET, and
// the barrier route is a POST.
type BarrierRequest struct{}

// --- response types ----------------------------------------------------------

// ResponseStatus is embedded in every response.
//
// Status is one of the Status* constants. LeaderID is a routing hint that is
// meaningful when Status is StatusNotLeader, and -1 when unknown. Error carries
// a human-readable explanation for statuses that need one; it is diagnostic
// only, never something a client should branch on.
type ResponseStatus struct {
	Status   string `json:"status"`
	LeaderID int    `json:"leader_id"`
	Error    string `json:"error,omitempty"`
}

// LockInfo is the wire form of a lock record.
//
// Key is part of the payload on purpose: a resolver must know which key a lock
// protects, because clearing the lock is a write to that specific key's group.
type LockInfo struct {
	Exists        bool   `json:"exists"`
	Key           string `json:"key"`
	TxnID         uint64 `json:"txn_id"`
	Primary       string `json:"primary"`
	StartTS       uint64 `json:"start_ts"`
	CommitTS      uint64 `json:"commit_ts"`
	Kind          int    `json:"kind"`
	ExpiresAt     uint64 `json:"expires_at"`
	ActorCommitTS uint64 `json:"actor_commit_ts"`
	State         int    `json:"state"`
}

// StatusResponse answers the leader-discovery endpoint.
type StatusResponse struct {
	ResponseStatus
	OK       bool `json:"ok"`
	GroupID  int  `json:"group_id"`
	NodeID   int  `json:"node_id"`
	IsLeader bool `json:"is_leader"`
}

// ReadResponse answers a snapshot read.
type ReadResponse struct {
	ResponseStatus
	Value       string    `json:"value"`
	Found       bool      `json:"found"`
	Lock        *LockInfo `json:"lock,omitempty"`
	MinCommitTS uint64    `json:"min_commit_ts,omitempty"`
}

// WriteResponse answers every operation that mutates the state machine.
type WriteResponse struct {
	ResponseStatus
	State          int       `json:"state"`
	CommitTS       uint64    `json:"commit_ts,omitempty"`
	Prepared       bool      `json:"prepared,omitempty"`
	KeysFinalized  int       `json:"keys_finalized,omitempty"`
	ConflictKey    string    `json:"conflict_key,omitempty"`
	ConflictStart  uint64    `json:"conflict_start,omitempty"`
	ConflictCommit uint64    `json:"conflict_commit,omitempty"`
	Lock           *LockInfo `json:"lock,omitempty"`
	MinCommitTS    uint64    `json:"min_commit_ts,omitempty"`
}

// LockedResponse answers a lock query.
type LockedResponse struct {
	ResponseStatus
	State int       `json:"state"`
	Lock  *LockInfo `json:"lock,omitempty"`
}

// Wire status strings. They are part of the client/server contract, and are
// named WStatus* to keep them clearly distinct from the state machine's own
// txn.Status values.
const (
	WStatusOK               = "ok"
	WStatusAlreadyCommitted = "already-committed"
	WStatusLocked           = "locked"
	WStatusWriteConflict    = "write-conflict"
	WStatusKeyNotInWriteSet = "key-not-in-write-set"
	WStatusCommitTSOutOfOrd = "commit-ts-out-of-order"
	WStatusNotLeader        = "not-leader"
	WStatusInternal         = "internal"
)

// --- JSON plumbing -----------------------------------------------------------

// readJSON decodes a request body into target, requiring an application/json
// content type and rejecting unknown fields so that client and server cannot
// drift apart silently.
func readJSON(req *http.Request, target any) error {
	mediaType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil {
		return err
	}
	if mediaType != "application/json" {
		return fmt.Errorf("expect application/json Content-Type, got %q", mediaType)
	}
	dec := json.NewDecoder(req.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(target)
}

// writeJSON renders v as JSON.
func writeJSON(w http.ResponseWriter, v any) {
	js, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(js)
}
