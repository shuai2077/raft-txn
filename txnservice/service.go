// Package txnservice exposes a shard of the transactional key/value store as a
// Raft replication group, reachable over HTTP.
//
// A Node is one member of one group. It owns:
//
//   - a raft.Server (internal/raft), whose log is the group's source of truth;
//   - a txn.Store, the replicated state machine applied to that log;
//   - an HTTP server implementing the transaction RPC surface, which turns each
//     request into one Raft log entry.
//
// The layering is deliberate: nothing in this package makes a consistency
// decision. It validates, routes to the group leader, replicates, and reports
// the state machine's answer back. All of the interesting logic lives in the
// state machine and the coordinator.
package txnservice

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eliben/raft/txnkv/internal/raft"
	"github.com/eliben/raft/txnkv/txn"
)

// DebugService enables debug logging.
const DebugService = 0

// leaderPollInterval is how often a node refreshes its view of the group
// leader. It is only used to answer "not the leader" responses with a useful
// routing hint; correctness never depends on it.
const leaderPollInterval = 25 * time.Millisecond

// NodeConfig configures a Node.
type NodeConfig struct {
	// GroupID identifies the replication group this node belongs to.
	GroupID txn.GroupID

	// NodeID is this node's id within the group, and must match the id used by
	// the group's Raft peers.
	NodeID int

	// PeerIDs are the other Raft members of the group.
	PeerIDs []int

	// Storage is the group's durable Raft storage. Sharing one Storage value
	// between the members of a group is not meaningful; each node gets its own.
	Storage raft.Storage

	// Ready must be closed once every node of every group is up and connected.
	Ready <-chan any

	// LockTTL overrides the intent-lock lease (zero means txn's default).
	LockTTL time.Duration

	// HTTPAddr is the host:port to serve transaction RPCs on. An empty string or
	// ":0" asks the OS for a free port; call Addr afterwards to learn it.
	HTTPAddr string
}

// Node is one Raft group member serving the transaction protocol.
type Node struct {
	cfg     NodeConfig
	groupID txn.GroupID

	raftServer *raft.Server
	log        *txn.RaftLog
	store      *txn.Store

	// commitChan receives committed entries from Raft. The state machine is
	// applied synchronously, so this channel exists to drain and validate the
	// commit stream rather than to drive it.
	commitChan chan raft.CommitEntry

	httpServer *http.Server
	listener   net.Listener

	// leaders caches the group leader id for routing hints.
	leaders atomic.Int64

	quit chan struct{}
	wg   sync.WaitGroup

	mu       sync.Mutex
	stopped  bool
	handlers map[string]http.HandlerFunc
}

// NewNode creates and starts a Node.
func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.NodeID < 0 {
		return nil, fmt.Errorf("txnservice: negative node id")
	}
	if cfg.Storage == nil {
		return nil, fmt.Errorf("txnservice: nil Raft storage")
	}

	// The command type travels through raft.LogEntry, which Raft gob-encodes when
	// it persists the log, so it has to be registered before use.
	gob.Register(txn.TxnCommand{})

	n := &Node{
		cfg:        cfg,
		groupID:    cfg.GroupID,
		commitChan: make(chan raft.CommitEntry, 64),
		quit:       make(chan struct{}),
		handlers:   make(map[string]http.HandlerFunc),
	}
	n.leaders.Store(-1)

	// Start Raft first: it binds the peer RPC listener and creates the
	// ConsensusModule.
	n.raftServer = raft.NewServer(cfg.NodeID, cfg.PeerIDs, cfg.Storage, cfg.Ready, n.commitChan)
	n.raftServer.Serve()

	// Wire the replicated state machine into the group's log. From here on, every
	// command the leader appends is applied to this store, in log order, on every
	// replica of the group.
	n.log = txn.NewRaftLog(cfg.GroupID, n.raftServer)
	n.store = txn.NewStore(n.log)
	if cfg.LockTTL > 0 {
		n.store.SetLockTTL(cfg.LockTTL)
	}
	n.log.SetStore(n.store)

	if err := n.serveHTTP(cfg.HTTPAddr); err != nil {
		n.raftServer.Shutdown()
		return nil, err
	}

	n.wg.Add(2)
	go n.drainCommits()
	go n.pollLeader()
	return n, nil
}

// GroupID reports the group this node belongs to.
func (n *Node) GroupID() txn.GroupID { return n.groupID }

// NodeID reports this node's id within its group.
func (n *Node) NodeID() int { return n.cfg.NodeID }

// Store returns the node's replicated state machine. It is exposed for tests and
// for in-process deployments that want to bypass the HTTP hop.
func (n *Node) Store() *txn.Store { return n.store }

// Participant returns an in-process Participant that talks to this node's state
// machine directly. Tests use it to build a coordinator without HTTP.
func (n *Node) Participant() txn.Participant { return n.log }

// SetRaftLogging turns the group's Raft verbose logging on or off.
//
// The switch is global to the process (upstream exposes it as a package-level
// flag), so this is a coarse control: it is meant for a program that embeds the
// store and does not want a log line per heartbeat.
func (n *Node) SetRaftLogging(enabled bool) { n.raftServer.SetDebug(enabled) }

// RaftListenAddr returns the address of this node's Raft peer RPC listener.
func (n *Node) RaftListenAddr() net.Addr { return n.raftServer.GetListenAddr() }

// Addr returns the HTTP address clients should contact.
func (n *Node) Addr() string { return n.listener.Addr().String() }

// IsLeader reports whether this node believes it leads its group.
func (n *Node) IsLeader() bool { return n.raftServer.IsLeader() }

// LeaderHint returns the group leader id this node last observed, or -1.
func (n *Node) LeaderHint() int { return int(n.leaders.Load()) }

// ConnectToPeer establishes the Raft peer connection this node uses to reach
// another member of its group.
func (n *Node) ConnectToPeer(peerID int, addr net.Addr) error {
	return n.raftServer.ConnectToPeer(peerID, addr)
}

// DisconnectFromPeer closes this node's Raft connection to a peer, simulating a
// network partition of that direction.
func (n *Node) DisconnectFromPeer(peerID int) error {
	return n.raftServer.DisconnectPeer(peerID)
}

// DisconnectFromAllPeers closes all of this node's Raft peer connections.
func (n *Node) DisconnectFromAllPeers() { n.raftServer.DisconnectAll() }

// Shutdown stops the node: the HTTP server, the state machine's background
// work, and the Raft server. It is safe to call more than once.
func (n *Node) Shutdown() error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil
	}
	n.stopped = true
	close(n.quit)
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = n.httpServer.Shutdown(ctx)

	n.raftServer.Shutdown()
	close(n.commitChan)
	n.wg.Wait()
	return nil
}

// drainCommits consumes the Raft commit stream.
//
// The state machine is applied synchronously inside the proposal path (see
// raft.Server.Apply), so this goroutine's job is to keep the commit channel
// moving and to surface entries the state machine had no handler for. Without a
// reader, Raft's commit sender would block.
func (n *Node) drainCommits() {
	defer n.wg.Done()
	for entry := range n.commitChan {
		if _, ok := entry.Command.(txn.TxnCommand); !ok {
			n.dlog("committed non-transactional command %T at index %d", entry.Command, entry.Index)
		}
	}
}

// pollLeader keeps the routing hint fresh.
func (n *Node) pollLeader() {
	defer n.wg.Done()
	ticker := time.NewTicker(leaderPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.quit:
			return
		case <-ticker.C:
			n.leaders.Store(int64(n.raftServer.LeaderID()))
		}
	}
}

// --- HTTP surface ------------------------------------------------------------

func (n *Node) serveHTTP(addr string) error {
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("txnservice: listening on %s: %w", addr, err)
	}
	n.listener = listener

	mux := http.NewServeMux()
	mux.HandleFunc("POST /txn/read/", n.handleRead)
	mux.HandleFunc("POST /txn/prewrite/", n.handlePrewrite)
	mux.HandleFunc("POST /txn/seal/", n.handleSeal)
	mux.HandleFunc("POST /txn/commit/", n.handleCommit)
	mux.HandleFunc("POST /txn/rollback/", n.handleRollback)
	mux.HandleFunc("POST /txn/locked/", n.handleLocked)
	mux.HandleFunc("GET /txn/status/", n.handleStatus)

	n.httpServer = &http.Server{Handler: mux}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := n.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[txn svc group=%d node=%d] http serve: %v", n.groupID, n.cfg.NodeID, err)
		}
	}()
	return nil
}

// handleStatus reports whether this node leads its group, and who does. Clients
// use it for leader discovery.
func (n *Node) handleStatus(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, StatusResponse{
		ResponseStatus: ResponseStatus{Status: WStatusOK, LeaderID: n.LeaderHint()},
		OK:             true,
		GroupID:        int(n.groupID),
		NodeID:         n.cfg.NodeID,
		IsLeader:       n.IsLeader(),
	})
}

// errorPayload is the wire form of a failed operation that never reached Raft.
type errorPayload struct {
	Status   string `json:"status"`
	Error    string `json:"error"`
	LeaderID int    `json:"leader_id"`
}

func (n *Node) handleRead(w http.ResponseWriter, req *http.Request) {
	var body ReadRequest
	if !n.decode(w, req, &body) {
		return
	}
	n.propose(w, req, txn.TxnCommand{Kind: txn.CmdRead, ReadArgs: &txn.ReadArgs{
		StartTS: txn.Timestamp(body.StartTS),
		Key:     txn.Key(body.Key),
	}}, func(res txn.TxnResult) any {
		return ReadResponse{
			ResponseStatus: responseStatus(res),
			Value:          string(res.Value),
			Found:          res.Found,
			Lock:           lockFor(res),
			MinCommitTS:    uint64(res.MinCommitTS),
		}
	})
}

func (n *Node) handlePrewrite(w http.ResponseWriter, req *http.Request) {
	var body PrewriteRequest
	if !n.decode(w, req, &body) {
		return
	}
	ops := make([]txn.WriteOp, 0, len(body.Ops))
	for _, op := range body.Ops {
		ops = append(ops, txn.WriteOp{
			Kind:  txn.OpKind(op.Kind),
			Key:   txn.Key(op.Key),
			Value: txn.Value(op.Value),
		})
	}
	n.propose(w, req, txn.TxnCommand{Kind: txn.CmdPrewrite, PrewriteArgs: &txn.PrewriteRequest{
		TxnID:     txn.TxnID(body.TxnID),
		ClientID:  body.ClientID,
		RequestID: body.RequestID,
		StartTS:   txn.Timestamp(body.StartTS),
		Primary:   txn.Key(body.Primary),
		Ops:       ops,
		OneShot:   body.OneShot,
		CommitTS:  txn.Timestamp(body.CommitTS),
	}}, func(res txn.TxnResult) any {
		return WriteResponse{
			ResponseStatus: responseStatus(res),
			State:          int(res.State),
			CommitTS:       uint64(res.CommitTS),
			Prepared:       res.Prepared,
			KeysFinalized:  res.KeysFinalized,
			ConflictStart:  uint64(res.ConflictStart),
			ConflictCommit: uint64(res.ConflictCommit),
			Lock:           lockFor(res),
			MinCommitTS:    uint64(res.MinCommitTS),
		}
	})
}

func (n *Node) handleSeal(w http.ResponseWriter, req *http.Request) {
	var body SealRequest
	if !n.decode(w, req, &body) {
		return
	}
	n.propose(w, req, txn.TxnCommand{Kind: txn.CmdSeal, SealArgs: &txn.SealArgs{
		Key:      txn.Key(body.Key),
		TxnID:    txn.TxnID(body.TxnID),
		StartTS:  txn.Timestamp(body.StartTS),
		CommitTS: txn.Timestamp(body.CommitTS),
	}}, func(res txn.TxnResult) any {
		return WriteResponse{
			ResponseStatus: responseStatus(res),
			State:          int(res.State),
			CommitTS:       uint64(res.CommitTS),
		}
	})
}

func (n *Node) handleCommit(w http.ResponseWriter, req *http.Request) {
	var body CommitRequest
	if !n.decode(w, req, &body) {
		return
	}
	keys := make([]txn.Key, 0, len(body.Keys))
	for _, k := range body.Keys {
		keys = append(keys, txn.Key(k))
	}
	n.propose(w, req, txn.TxnCommand{Kind: txn.CmdCommit, CommitArgs: &txn.CommitRequest{
		TxnID:     txn.TxnID(body.TxnID),
		ClientID:  body.ClientID,
		RequestID: body.RequestID,
		StartTS:   txn.Timestamp(body.StartTS),
		CommitTS:  txn.Timestamp(body.CommitTS),
		Keys:      keys,
	}}, func(res txn.TxnResult) any {
		return WriteResponse{
			ResponseStatus: responseStatus(res),
			State:          int(res.State),
			CommitTS:       uint64(res.CommitTS),
			KeysFinalized:  res.KeysFinalized,
		}
	})
}

func (n *Node) handleRollback(w http.ResponseWriter, req *http.Request) {
	var body RollbackRequest
	if !n.decode(w, req, &body) {
		return
	}
	keys := make([]txn.Key, 0, len(body.Keys))
	for _, k := range body.Keys {
		keys = append(keys, txn.Key(k))
	}
	n.propose(w, req, txn.TxnCommand{Kind: txn.CmdRollback, RollbackArgs: &txn.RollbackRequest{
		TxnID:     txn.TxnID(body.TxnID),
		ClientID:  body.ClientID,
		RequestID: body.RequestID,
		StartTS:   txn.Timestamp(body.StartTS),
		Keys:      keys,
	}}, func(res txn.TxnResult) any {
		return WriteResponse{
			ResponseStatus: responseStatus(res),
			State:          int(res.State),
			KeysFinalized:  res.KeysFinalized,
		}
	})
}

func (n *Node) handleLocked(w http.ResponseWriter, req *http.Request) {
	var body LockedRequest
	if !n.decode(w, req, &body) {
		return
	}
	n.propose(w, req, txn.TxnCommand{Kind: txn.CmdQueryLock, QueryLockArgs: &txn.QueryLockArgs{
		TxnID: txn.TxnID(body.TxnID),
		Key:   txn.Key(body.Key),
	}}, func(res txn.TxnResult) any {
		return LockedResponse{
			ResponseStatus: responseStatus(res),
			State:          int(res.State),
			Lock:           lockFor(res),
		}
	})
}

// propose replicates one command and renders the state machine's answer.
func (n *Node) propose(w http.ResponseWriter, req *http.Request, cmd txn.TxnCommand,
	render func(txn.TxnResult) any) {
	if !n.IsLeader() {
		// Avoid one wasted log round trip per request by answering with a routing
		// hint, so the client goes straight to the leader. This is an
		// optimization, not a consistency requirement: Apply would reject the
		// command anyway.
		writeJSON(w, notLeaderPayload(n.LeaderHint(), int(n.groupID)))
		return
	}

	res, err := n.log.ApplyCommand(req.Context(), cmd)
	if err != nil {
		var notLeader *txn.NotLeaderError
		if errors.As(err, &notLeader) {
			writeJSON(w, notLeaderPayload(notLeader.LeaderID, int(notLeader.GroupID)))
			return
		}
		writeJSON(w, internalErrorPayload(err))
		return
	}
	writeJSON(w, render(res))
}

// decode reads a JSON request body, reporting a 400 on failure.
func (n *Node) decode(w http.ResponseWriter, req *http.Request, target any) bool {
	if err := readJSON(req, target); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func notLeaderPayload(leaderID, groupID int) errorPayload {
	return errorPayload{
		Status:   WStatusNotLeader,
		Error:    fmt.Sprintf("group %d: request reached a follower", groupID),
		LeaderID: leaderID,
	}
}

func internalErrorPayload(err error) errorPayload {
	return errorPayload{Status: WStatusInternal, Error: err.Error(), LeaderID: -1}
}

// responseStatus maps a state-machine result onto the wire status vocabulary.
func responseStatus(res txn.TxnResult) ResponseStatus {
	return ResponseStatus{
		Status:   statusFor(res),
		LeaderID: -1,
		Error:    res.Err,
	}
}

// statusFor maps a state-machine status onto the wire status vocabulary.
func statusFor(res txn.TxnResult) string {
	switch res.Status {
	case txn.StatusOK:
		return WStatusOK
	case txn.StatusAlreadyCommitted:
		return WStatusAlreadyCommitted
	case txn.StatusLocked:
		return WStatusLocked
	case txn.StatusWriteConflict:
		return WStatusWriteConflict
	case txn.StatusKeyNotInWriteSet:
		return WStatusKeyNotInWriteSet
	case txn.StatusCommitTSOutOfOrder:
		return WStatusCommitTSOutOfOrd
	case txn.StatusNotLeader:
		return WStatusNotLeader
	default:
		return WStatusInternal
	}
}

func lockFor(res txn.TxnResult) *LockInfo {
	if res.Lock == nil {
		return nil
	}
	return &LockInfo{
		Exists:        res.Lock.Exists,
		Key:           string(res.Lock.Key),
		TxnID:         uint64(res.Lock.TxnID),
		Primary:       string(res.Lock.Primary),
		StartTS:       uint64(res.Lock.StartTS),
		CommitTS:      uint64(res.Lock.CommitTS),
		Kind:          int(res.Lock.Kind),
		ExpiresAt:     res.Lock.ExpiresAt,
		ActorCommitTS: uint64(res.Lock.ActorCommitTS),
		State:         int(res.Lock.State),
	}
}

func (n *Node) dlog(format string, args ...any) {
	if DebugService > 0 {
		log.Printf("[txn svc group=%d node=%d] "+format, append([]any{n.groupID, n.cfg.NodeID}, args...)...)
	}
}
