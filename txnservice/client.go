package txnservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/eliben/raft/txnkv/txn"
)

// This file is the client half of the wire protocol: a txn.Participant (and
// txn.Resolver helper) that reaches a Raft group's leader over HTTP.
//
// Two responsibilities live here, and they are worth keeping apart:
//
//   - GroupClient implements txn.Participant for one group. It knows the group's
//     member addresses and follows leadership, retrying until it reaches the
//     leader or the caller's context ends. It never interprets the state
//     machine's *answer* - only its transport failures.
//   - Client composes one GroupClient per group into the participant set a
//     txn.Client needs, and implements the Resolver so that stray locks can be
//     cleared through the group that owns them.

// DefaultRequestTimeout bounds a single HTTP attempt. A group member that does
// not answer within this window is treated as unreachable and the next member
// is tried.
const DefaultRequestTimeout = 500 * time.Millisecond

// GroupClient talks to the nodes of one Raft group.
type GroupClient struct {
	groupID txn.GroupID

	// addrs maps a node id to its HTTP address.
	addrs []string

	// assumedLeader is an index into addrs, used as the starting point of leader
	// discovery.
	assumedLeader int

	// leaderNode is the node id a group member told us leads the group, or -1.
	leaderNode int

	timeout time.Duration
	http    *http.Client

	mu sync.Mutex
}

// NewGroupClient creates a client for one group.
//
// addrs must be indexed by node id: addrs[i] is node i's HTTP address. Passing
// addresses in node-id order is what makes the "not leader" routing hint, which
// is a node id, directly usable as an index.
func NewGroupClient(groupID txn.GroupID, addrs []string) *GroupClient {
	return &GroupClient{
		groupID:       groupID,
		addrs:         addrs,
		assumedLeader: 0,
		leaderNode:    -1,
		timeout:       DefaultRequestTimeout,
		http:          &http.Client{},
	}
}

// SetTimeout overrides the per-attempt timeout.
func (gc *GroupClient) SetTimeout(d time.Duration) {
	if d > 0 {
		gc.timeout = d
	}
}

// GroupID reports which group this client reaches.
func (gc *GroupClient) GroupID() txn.GroupID { return gc.groupID }

// LeaderHint implements txn.LeaderHinter: it reports the node id last observed
// to lead the group, or -1.
func (gc *GroupClient) LeaderHint() int {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.leaderNode
}

func (gc *GroupClient) setLeader(node int) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.leaderNode = node
	if node >= 0 && node < len(gc.addrs) {
		gc.assumedLeader = node
	}
}

func (gc *GroupClient) rotate() {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	if len(gc.addrs) > 0 {
		gc.assumedLeader = (gc.assumedLeader + 1) % len(gc.addrs)
	}
}

func (gc *GroupClient) start() int {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	return gc.assumedLeader
}

// call posts one request, following leadership until it reaches the group's
// leader or the context expires.
//
// reqData is only used to detect routing failures early; response decoding is
// handled by callFor.
func (gc *GroupClient) call(ctx context.Context, route string, reqData, respData any) error {
	return gc.callFor(ctx, route, reqData, respData)
}

// --- Participant implementation ---------------------------------------------

// Read implements txn.Participant.
func (gc *GroupClient) Read(ctx context.Context, startTS txn.Timestamp, key txn.Key) (txn.Value, error) {
	var resp ReadResponse
	if err := gc.roundTrip(ctx, "read", ReadRequest{StartTS: uint64(startTS), Key: string(key)}, &resp); err != nil {
		return "", err
	}
	if resp.Status == WStatusOK {
		if !resp.Found {
			return "", &txn.KeyNotFoundError{Key: key}
		}
		return txn.Value(resp.Value), nil
	}
	return "", convertError(resp.Status, key, resp.Lock, txn.Timestamp(resp.MinCommitTS), 0, 0, resp.Error)
}

// Prewrite implements txn.Participant.
func (gc *GroupClient) Prewrite(ctx context.Context, req txn.PrewriteRequest) error {
	ops := make([]WriteOpJSON, 0, len(req.Ops))
	for _, op := range req.Ops {
		ops = append(ops, WriteOpJSON{Kind: int(op.Kind), Key: string(op.Key), Value: string(op.Value)})
	}
	body := PrewriteRequest{
		TxnID:     uint64(req.TxnID),
		ClientID:  req.ClientID,
		RequestID: req.RequestID,
		StartTS:   uint64(req.StartTS),
		Primary:   string(req.Primary),
		Ops:       ops,
		OneShot:   req.OneShot,
		CommitTS:  uint64(req.CommitTS),
	}
	var resp WriteResponse
	if err := gc.roundTrip(ctx, "prewrite", body, &resp); err != nil {
		return err
	}
	return convertWriteError(resp, req.Primary)
}

// Seal implements txn.Participant.
func (gc *GroupClient) Seal(ctx context.Context, key txn.Key, txnID txn.TxnID, startTS, commitTS txn.Timestamp) error {
	body := SealRequest{
		Key:      string(key),
		TxnID:    uint64(txnID),
		StartTS:  uint64(startTS),
		CommitTS: uint64(commitTS),
	}
	var resp WriteResponse
	if err := gc.roundTrip(ctx, "seal", body, &resp); err != nil {
		return err
	}
	if resp.Status == WStatusLocked {
		// The lock vanished between the prewrite and the seal: the coordinator
		// has to find out which way it was resolved.
		return txn.ErrLockGone
	}
	return convertWriteError(resp, key)
}

// Commit implements txn.Participant.
func (gc *GroupClient) Commit(ctx context.Context, req txn.CommitRequest) error {
	body := CommitRequest{
		TxnID:     uint64(req.TxnID),
		ClientID:  req.ClientID,
		RequestID: req.RequestID,
		StartTS:   uint64(req.StartTS),
		CommitTS:  uint64(req.CommitTS),
		Keys:      keysToStrings(req.Keys),
	}
	var resp WriteResponse
	if err := gc.roundTrip(ctx, "commit", body, &resp); err != nil {
		return err
	}
	return convertWriteError(resp, firstKey(req.Keys))
}

// Rollback implements txn.Participant.
func (gc *GroupClient) Rollback(ctx context.Context, req txn.RollbackRequest) error {
	body := RollbackRequest{
		TxnID:     uint64(req.TxnID),
		ClientID:  req.ClientID,
		RequestID: req.RequestID,
		StartTS:   uint64(req.StartTS),
		Keys:      keysToStrings(req.Keys),
	}
	var resp WriteResponse
	if err := gc.roundTrip(ctx, "rollback", body, &resp); err != nil {
		return err
	}
	return convertWriteError(resp, firstKey(req.Keys))
}

// Locked implements txn.Participant.
func (gc *GroupClient) Locked(ctx context.Context, txnID txn.TxnID, key txn.Key) (*txn.LockInfo, error) {
	var resp LockedResponse
	body := LockedRequest{TxnID: uint64(txnID), Key: string(key)}
	if err := gc.roundTrip(ctx, "locked", body, &resp); err != nil {
		return nil, err
	}
	if resp.Lock == nil {
		return nil, nil
	}
	return lockInfoFromWire(resp.Lock), nil
}

// Status queries the leader-discovery endpoint of the group's current leader.
func (gc *GroupClient) Status(ctx context.Context) (StatusResponse, error) {
	var out StatusResponse
	err := gc.call(ctx, "status", nil, &out)
	return out, err
}

// roundTrip sends one request and unmarshals the reply, translating routing
// failures into txn errors.
func (gc *GroupClient) roundTrip(ctx context.Context, route string, reqData any, respData any) error {
	return gc.callFor(ctx, route, reqData, respData)
}

// notLeaderProbe is the shape shared by every routing rejection: every response
// in this protocol carries `status` and `leader_id`, so one struct recognizes
// them all without the client needing to know which handler answered.
type notLeaderProbe struct {
	Status   string `json:"status"`
	LeaderID int    `json:"leader_id"`
}

// callFor sends a request, following leadership, and decodes the response.
//
// Routing rule: when a member rejects a request because it is not the leader,
// the *next* attempt must go to the member it named - that is the whole point of
// the hint. Only when no hint is available does the loop fall back to sweeping
// the members in turn.
func (gc *GroupClient) callFor(ctx context.Context, route string, reqData, respData any) error {
	if len(gc.addrs) == 0 {
		return fmt.Errorf("txnservice: group %d has no addresses", gc.groupID)
	}
	timeout := gc.timeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}

	// missed counts consecutive attempts that produced no usable leader hint, so
	// that a group without a leader is retried politely rather than hammered.
	missed := 0
	for {
		if err := ctx.Err(); err != nil {
			return &txn.NotLeaderError{GroupID: gc.groupID, LeaderID: gc.LeaderHint()}
		}
		node := gc.start()
		url := fmt.Sprintf("http://%s/txn/%s/", gc.addrs[node], route)

		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		raw, err := gc.doPost(attemptCtx, url, reqData)
		cancel()

		if err == nil {
			var probe notLeaderProbe
			if json.Unmarshal(raw, &probe) == nil && probe.Status == WStatusNotLeader {
				if probe.LeaderID >= 0 && probe.LeaderID < len(gc.addrs) {
					// We were told who leads the group: go straight there.
					gc.setLeader(probe.LeaderID)
					missed = 0
					continue
				}
				// No hint: the group is probably electing. Sweep the members and
				// then pause before trying again.
				gc.rotate()
				if missed++; missed >= len(gc.addrs) {
					missed = 0
					if err := sleepCtx(ctx, 20*time.Millisecond); err != nil {
						return &txn.NotLeaderError{GroupID: gc.groupID, LeaderID: -1}
					}
				}
				continue
			}
			if err := json.Unmarshal(raw, respData); err != nil {
				return fmt.Errorf("txnservice: decoding %s response: %w", route, err)
			}
			gc.setLeader(node)
			return nil
		}

		if ctx.Err() != nil {
			return &txn.NotLeaderError{GroupID: gc.groupID, LeaderID: gc.LeaderHint()}
		}
		// Transport failure: this member is unreachable. Try the next one.
		gc.rotate()
		if missed++; missed >= len(gc.addrs) {
			missed = 0
			if err := sleepCtx(ctx, 20*time.Millisecond); err != nil {
				return &txn.NotLeaderError{GroupID: gc.groupID, LeaderID: gc.LeaderHint()}
			}
		}
	}
}

func (gc *GroupClient) doPost(ctx context.Context, url string, reqData any) ([]byte, error) {
	body := new(bytes.Buffer)
	if reqData != nil {
		if err := json.NewEncoder(body).Encode(reqData); err != nil {
			return nil, fmt.Errorf("txnservice: encoding request: %w", err)
		}
	}
	method := http.MethodPost
	if reqData == nil {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := gc.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("txnservice: %s: HTTP %d: %s", url, resp.StatusCode, string(raw))
	}
	return raw, nil
}

func (gc *GroupClient) httpClient() *http.Client {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	if gc.http == nil {
		gc.http = &http.Client{}
	}
	return gc.http
}

// --- error conversion --------------------------------------------------------

// convertWriteError turns a write response into a typed error.
func convertWriteError(resp WriteResponse, key txn.Key) error {
	if resp.Status == WStatusOK || resp.Status == WStatusAlreadyCommitted {
		return nil
	}
	if resp.Status == WStatusInternal {
		return fmt.Errorf("txnservice: %s", resp.Error)
	}
	return convertError(resp.Status, key, resp.Lock, txn.Timestamp(resp.MinCommitTS),
		txn.Timestamp(resp.ConflictStart), txn.Timestamp(resp.ConflictCommit), resp.Error)
}

// convertError is the single place wire statuses become txn errors, mirroring
// txn.ErrorForStatus on the server side.
func convertError(status string, key txn.Key, lock *LockInfo, minCommitTS,
	conflictStart, conflictCommit txn.Timestamp, text string) error {
	switch status {
	case WStatusOK, WStatusAlreadyCommitted:
		return nil
	case WStatusLocked:
		if lock == nil {
			return fmt.Errorf("txnservice: key %q is locked (no lock payload)", key)
		}
		tl := lockInfoFromWire(lock).TxnLock()
		return &txn.LockedError{Key: key, Lock: tl, MinCommitTS: minCommitTS}
	case WStatusWriteConflict:
		return &txn.WriteConflictError{Key: key, StartTS: conflictStart, CommitTS: conflictCommit}
	case WStatusKeyNotInWriteSet:
		return fmt.Errorf("txnservice: protocol error: %s", text)
	case WStatusCommitTSOutOfOrd:
		return fmt.Errorf("txnservice: commit timestamp out of order: %s", text)
	case WStatusNotLeader:
		return fmt.Errorf("txnservice: not the leader")
	default:
		return fmt.Errorf("txnservice: unexpected status %q: %s", status, text)
	}
}

func lockInfoFromWire(w *LockInfo) *txn.LockInfo {
	return &txn.LockInfo{
		Exists:        w.Exists,
		Key:           txn.Key(w.Key),
		TxnID:         txn.TxnID(w.TxnID),
		Primary:       txn.Key(w.Primary),
		StartTS:       txn.Timestamp(w.StartTS),
		CommitTS:      txn.Timestamp(w.CommitTS),
		Kind:          txn.OpKind(w.Kind),
		ExpiresAt:     w.ExpiresAt,
		ActorCommitTS: txn.Timestamp(w.ActorCommitTS),
		State:         txn.State(w.State),
	}
}

func keysToStrings(keys []txn.Key) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, string(k))
	}
	return out
}

func firstKey(keys []txn.Key) txn.Key {
	if len(keys) == 0 {
		return ""
	}
	return keys[0]
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// --- Client: the participant set --------------------------------------------

// ClientConfig describes how to reach every group of the cluster.
type ClientConfig struct {
	// Groups maps a group id to its member HTTP addresses, indexed by node id.
	Groups map[txn.GroupID][]string

	// Timeout overrides the per-attempt HTTP timeout (zero means
	// DefaultRequestTimeout).
	Timeout time.Duration
}

// Client owns one GroupClient per group and exposes them as the participant set
// and resolver a txn.Client needs.
type Client struct {
	groups map[txn.GroupID]*GroupClient

	// resolver holds the lock-resolution policy. Binding it after construction
	// breaks the cycle: the policy needs the participant set, and the participant
	// set is what this Client is.
	resolver txn.Resolver
}

// NewClient builds a Client for the described cluster.
func NewClient(cfg ClientConfig) *Client {
	groups := make(map[txn.GroupID]*GroupClient, len(cfg.Groups))
	for id, addrs := range cfg.Groups {
		gc := NewGroupClient(id, addrs)
		if cfg.Timeout > 0 {
			gc.SetTimeout(cfg.Timeout)
		}
		groups[id] = gc
	}
	return &Client{groups: groups}
}

// Participants returns the participant map for txn.NewClient.
func (c *Client) Participants() map[txn.GroupID]txn.Participant {
	out := make(map[txn.GroupID]txn.Participant, len(c.groups))
	for id, gc := range c.groups {
		out[id] = gc
	}
	return out
}

// Group returns the client for one group.
func (c *Client) Group(id txn.GroupID) (*GroupClient, bool) {
	gc, ok := c.groups[id]
	return gc, ok
}

// ResolveLock implements txn.Resolver for the client's group set.
//
// It delegates the policy to txn.DefaultResolver, which is where the commit-vs-
// abort decision lives, and supplies the reachability this layer provides.
func (c *Client) ResolveLock(ctx context.Context, group txn.GroupID, lock txn.TxnLock) error {
	if c.resolver != nil {
		return c.resolver.ResolveLock(ctx, group, lock)
	}
	return errors.New("txnservice: client has no resolver bound")
}

// BindResolver attaches the policy resolver. It must be called before the
// client is used to resolve locks.
func (c *Client) BindResolver(r txn.Resolver) {
	c.resolver = r
}

var _ txn.Participant = (*GroupClient)(nil)
var _ txn.LeaderHinter = (*GroupClient)(nil)
var _ txn.Resolver = (*Client)(nil)
