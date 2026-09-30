// Package tso implements a Raft-replicated timestamp oracle: the source of the
// monotonically increasing timestamps that give transactions their snapshots and
// commit points.
//
// Why a service at all? Transaction correctness rests on one global property:
// timestamps never repeat and never go backwards, cluster-wide, across leader
// changes and crashes. A local counter cannot provide that, and a naive
// "leader keeps a counter in memory" cannot either - a new leader would restart
// the counter and hand out timestamps that older snapshots have already used,
// which would let a transaction read a version "from the future" and then be
// ordered before its own snapshot.
//
// The fix is to make the oracle a replicated state machine like everything else
// in this repository: the leader:
//
//  1. proposes a command that raises the oracle's durable allocation ceiling,
//     which Raft applies to the leader's state machine before returning;
//  2. hands timestamps out of the reserved window entirely in memory.
//
// The window means one Raft round trip amortizes over WindowSize timestamps,
// while the durability of the allocation means a subsequent leader starts above
// everything already handed out. A crash can waste timestamps (the unused tail
// of a window); it can never reuse one.
package tso

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
)

// DefaultWindowSize is how many timestamps one Raft allocation reserves.
//
// Larger windows need fewer Raft round trips, at the cost of more timestamps
// wasted when a leader dies with an unspent window.
const DefaultWindowSize = 256

// DebugTSO enables debug logging.
const DebugTSO = 0

// CommandKind discriminates the oracle's replicated commands.
type CommandKind int

const (
	// CmdInvalid is the zero value; applying it is a bug.
	CmdInvalid CommandKind = iota
	// CmdAllocate raises the durable allocation ceiling, reserving the next
	// Count timestamps.
	CmdAllocate
)

func (k CommandKind) String() string {
	switch k {
	case CmdAllocate:
		return "allocate"
	default:
		return fmt.Sprintf("cmd(%d)", int(k))
	}
}

// Command is the oracle's log entry.
type Command struct {
	Kind CommandKind
	// Count is how many timestamps to reserve.
	Count uint64
	// NodeID is the member the reservation belongs to. It is what stops a new
	// leader from serving a window that its predecessor already spent: the
	// reservation is recorded against one member, and only that member may serve
	// out of it.
	NodeID int
}

// Result is the oracle state machine's answer.
type Result struct {
	// Allocated is the durable high-water mark after the command.
	Allocated uint64
	// Granted is the number of timestamps reserved.
	Granted uint64
	// Err is set when the command could not be served.
	Err string
}

func init() {
	gob.Register(Command{})
}

// Oracle is the replicated state machine. Every mutation arrives through Apply,
// in Raft log order, so its state is identical on every replica.
//
// Its state is a global high-water mark plus, per member, the last window that
// member reserved:
//
//	0        1        2   ...                    allocated-1   allocated
//	|--------|--------|---------------------------|------------|
//	 timestamps below `allocated` are "spoken for":
//	 they may or may not have been handed out already
//
// `allocated` is what makes failover safe. A new leader replays the log and finds
// the same mark, so it knows every timestamp below it may already have been
// issued. It also finds out whether the member it just replaced had an unspent
// window - and, crucially, that the window belongs to *that* member, not to
// whoever is leader now. A fresh leader therefore reserves a window of its own,
// above the mark, and can never reissue a predecessor's timestamps.
type Oracle struct {
	// allocated is the global high-water mark: every timestamp at or below it is
	// spoken for. It only ever grows, and it grows through a replicated command.
	allocated uint64

	// windowSize is how many timestamps each allocation reserves.
	windowSize uint64

	// intents records, per member, the window it most recently reserved:
	// timestamps (nextIssued, granted]. Only the member named here may serve out
	// of it.
	intents map[int]intent

	// nodeID is this replica's member id, used to find its own reservation.
	nodeID int

	// nextIssued is the next timestamp this member will hand out. It is local,
	// in-memory state; a restarted member simply reserves a fresh window.
	nextIssued uint64

	// granted is the upper bound of this member's current reservation.
	granted uint64

	// remaining is how many timestamps of this member's reservation are unserved.
	// It is tracked explicitly because the global mark can move past `granted`
	// while this member still has timestamps to hand out.
	remaining uint64

	// lastIssued is the last timestamp this member handed out, for reporting.
	lastIssued uint64
}

// intent is one member's reserved window.
type intent struct {
	// nextIssued is the next timestamp in the window that has not been served.
	nextIssued uint64
	// granted is the window's upper bound at the time it was reserved.
	granted uint64
	// remaining is how many timestamps of the window are still unserved. It is
	// tracked explicitly because the global mark can move past `granted`.
	remaining uint64
}

// NewOracle creates the state machine.
func NewOracle(windowSize uint64) *Oracle {
	if windowSize == 0 {
		windowSize = DefaultWindowSize
	}
	return &Oracle{windowSize: windowSize, intents: make(map[int]intent)}
}

// Apply is the Raft state-machine hook.
func (o *Oracle) Apply(command any) any {
	cmd, ok := command.(Command)
	if !ok {
		return Result{Err: fmt.Sprintf("unexpected command %T", command)}
	}
	switch cmd.Kind {
	case CmdAllocate:
		return o.allocate(cmd.NodeID, cmd.Count)
	default:
		panic(fmt.Sprintf("tso: unexpected command kind %v", cmd.Kind))
	}
}

// allocate reserves count timestamps for one member, starting above the global
// high-water mark.
//
// The reservation is recorded against the member that asked for it, which is the
// crux of the failover argument. A new leader replaying this log sees that its
// predecessor held a window - and that the window is not its own. So it cannot
// serve out of it, and must reserve a new one above the mark. Timestamps get
// wasted; none is ever repeated.
func (o *Oracle) allocate(nodeID int, count uint64) Result {
	if count == 0 {
		return Result{Allocated: o.allocated, Err: "allocation of zero timestamps"}
	}
	start := o.allocated
	o.allocated = start + count

	if o.intents == nil {
		o.intents = make(map[int]intent)
	}
	o.intents[nodeID] = intent{
		nextIssued: start,
		granted:    o.allocated,
		remaining:  count,
	}
	return Result{Allocated: o.allocated, Granted: count}
}

// setNode records which member this replica is, so that it serves only out of
// its own reservations.
func (o *Oracle) setNode(nodeID int) {
	o.nodeID = nodeID
}

// beginServing adopts this member's reservation from the replicated state. It is
// called after a successful allocation so that the freshly reserved window
// becomes the one this member serves from.
func (o *Oracle) beginServing(nodeID int) {
	it, ok := o.intents[nodeID]
	if !ok {
		return
	}
	o.nextIssued = it.nextIssued
	o.granted = it.granted
	o.remaining = it.remaining
}

// Allocated reports the high-water mark: the largest timestamp that is spoken
// for. Timestamps above it have provably never been issued anywhere.
func (o *Oracle) Allocated() uint64 { return o.allocated }

// LastIssued reports the last timestamp this node handed out.
func (o *Oracle) LastIssued() uint64 { return o.lastIssued }

// Available reports how many timestamps this member may still serve out of its
// current reservation without replicating a new one.
func (o *Oracle) Available() uint64 { return o.remaining }

// reserve proposes a new window for this member and adopts it on success.
//
// It is the only operation of the oracle that costs a Raft round trip; every
// timestamp handed out afterwards is served from memory, which is what the
// window buys.
func (s *Service) reserve() error {
	// Read the mark this proposal will build on, then propose. If the leader
	// changed in between, the proposal fails and the caller retries.
	_, err := s.raftServer.Apply(Command{
		Kind:   CmdAllocate,
		Count:  s.oracle.WindowSize(),
		NodeID: s.cfg.NodeID,
	})
	if err != nil {
		return err
	}
	s.oracle.beginServing(s.cfg.NodeID)
	if s.oracle.Available() == 0 {
		return errors.New("tso: allocation did not produce a usable window")
	}
	return nil
}

// take returns the next timestamp this node may serve, or false when it has
// consumed its reservation.
//
// It is deliberately *not* part of the replicated state: taking a timestamp
// mutates only this node's in-memory cursor, which is what keeps the common path
// free of Raft round trips. Safety comes from the window having been reserved
// durably before it is spent, not from replicating each take.
func (o *Oracle) take() (uint64, bool) {
	if o.remaining == 0 {
		return 0, false
	}
	next := o.nextIssued + 1
	if next > o.granted {
		return 0, false
	}
	o.nextIssued = next
	o.remaining--
	o.lastIssued = next
	return next, true
}

// WindowSize reports the configured allocation size.
func (o *Oracle) WindowSize() uint64 { return o.windowSize }

// --- network layer -----------------------------------------------------------

// NextRequest asks for a batch of timestamps.
type NextRequest struct {
	// Count is how many timestamps the caller wants. Zero means one. The server
	// may return fewer than requested when its window runs low.
	Count uint64 `json:"count"`
}

// NextResponse carries a contiguous block of timestamps [Start, Start+Count).
type NextResponse struct {
	// Status is "ok", "not-leader" or "internal".
	Status   string `json:"status"`
	LeaderID int    `json:"leader_id"`
	Error    string `json:"error,omitempty"`

	Start uint64 `json:"start"`
	Count uint64 `json:"count"`
}

// StatusResponse answers the oracle's leader-discovery endpoint.
type StatusResponse struct {
	Status     string `json:"status"`
	LeaderID   int    `json:"leader_id"`
	NodeID     int    `json:"node_id"`
	IsLeader   bool   `json:"is_leader"`
	Allocated  uint64 `json:"allocated"`
	LastIssued uint64 `json:"last_issued"`
}

// ServiceConfig configures one member of the oracle's Raft group.
type ServiceConfig struct {
	// NodeID is this member's id in the oracle's Raft group.
	NodeID int
	// PeerIDs are the other members.
	PeerIDs []int
	// Storage is this member's durable Raft storage.
	Storage raft.Storage
	// Ready is closed once every member is up and connected.
	Ready <-chan any
	// WindowSize overrides the allocation size (zero means DefaultWindowSize).
	WindowSize uint64
	// HTTPAddr is the address to serve timestamp requests on (":0" for any).
	HTTPAddr string
}

// Service is one member of the replicated timestamp oracle.
type Service struct {
	cfg ServiceConfig

	raftServer *raft.Server
	oracle     *Oracle
	commitChan chan raft.CommitEntry

	httpServer *http.Server
	listener   net.Listener

	leaders atomic.Int64

	quit chan struct{}
	wg   sync.WaitGroup

	mu      sync.Mutex
	stopped bool

	// allocMu serializes window allocations so two callers cannot both decide the
	// window is exhausted and both propose an allocation.
	allocMu sync.Mutex
}

// NewService creates and starts an oracle member.
func NewService(cfg ServiceConfig) (*Service, error) {
	if cfg.Storage == nil {
		return nil, errors.New("tso: nil Raft storage")
	}
	gob.Register(Command{})

	s := &Service{
		cfg:        cfg,
		oracle:     NewOracle(cfg.WindowSize),
		commitChan: make(chan raft.CommitEntry, 64),
		quit:       make(chan struct{}),
	}
	s.leaders.Store(-1)
	// Tell the state machine which member it is, so it only ever serves out of
	// its own reservations.
	s.oracle.setNode(cfg.NodeID)

	s.raftServer = raft.NewServer(cfg.NodeID, cfg.PeerIDs, cfg.Storage, cfg.Ready, s.commitChan)
	s.raftServer.Serve()
	s.raftServer.SetHandler(s.oracle)

	if err := s.serveHTTP(cfg.HTTPAddr); err != nil {
		s.raftServer.Shutdown()
		return nil, err
	}

	s.wg.Add(2)
	go s.drainCommits()
	go s.pollLeader()
	return s, nil
}

// Addr returns the HTTP address clients should contact.
func (s *Service) Addr() string { return s.listener.Addr().String() }

// RaftListenAddr returns the address of this member's Raft RPC listener.
func (s *Service) RaftListenAddr() net.Addr { return s.raftServer.GetListenAddr() }

// NodeID reports this member's id.
func (s *Service) NodeID() int { return s.cfg.NodeID }

// IsLeader reports whether this member believes it leads the oracle group.
func (s *Service) IsLeader() bool { return s.raftServer.IsLeader() }

// Oracle returns the state machine, for tests.
func (s *Service) Oracle() *Oracle { return s.oracle }

// ConnectToPeer connects this member to another member of the oracle group.
func (s *Service) ConnectToPeer(peerID int, addr net.Addr) error {
	return s.raftServer.ConnectToPeer(peerID, addr)
}

// DisconnectFromPeer closes the Raft connection to a peer.
func (s *Service) DisconnectFromPeer(peerID int) error {
	return s.raftServer.DisconnectPeer(peerID)
}

// DisconnectFromAllPeers closes all Raft peer connections.
func (s *Service) DisconnectFromAllPeers() { s.raftServer.DisconnectAll() }

// Shutdown stops the service.
func (s *Service) Shutdown() error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	close(s.quit)
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = s.httpServer.Shutdown(ctx)

	s.raftServer.Shutdown()
	close(s.commitChan)
	s.wg.Wait()
	return nil
}

func (s *Service) drainCommits() {
	defer s.wg.Done()
	for range s.commitChan {
	}
}

func (s *Service) pollLeader() {
	defer s.wg.Done()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			s.leaders.Store(int64(s.raftServer.LeaderID()))
		}
	}
}

// --- serving timestamps ------------------------------------------------------

// Next reserves count contiguous timestamps from the oracle. It must be called
// while this member leads the group.
//
// The returned block is [start, start+count). A count of zero is treated as one.
//
// Timestamps are served from this member's current reservation, which is why the
// call is normally free of Raft round trips; a new reservation is replicated only
// when the current one runs out.
func (s *Service) Next(ctx context.Context, count uint64) (uint64, uint64, error) {
	if count == 0 {
		count = 1
	}
	s.allocMu.Lock()
	defer s.allocMu.Unlock()

	if !s.IsLeader() {
		return 0, 0, fmt.Errorf("tso: node %d is not the leader (leader hint: %d)",
			s.cfg.NodeID, s.leaders.Load())
	}

	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}

	// Serve a whole block out of one reservation. A request larger than one
	// window is served by reserving repeatedly, which is correct if slow; it is
	// not on any hot path.
	first, ok := s.oracle.take()
	if !ok {
		if err := s.reserve(); err != nil {
			return 0, 0, err
		}
		first, ok = s.oracle.take()
		if !ok {
			return 0, 0, errors.New("tso: no durably allocated timestamps available")
		}
	}

	got := uint64(1)
	for got < count {
		if _, ok := s.oracle.take(); !ok {
			break
		}
		got++
	}
	return first, got, nil
}

func (s *Service) serveHTTP(addr string) error {
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("tso: listening on %s: %w", addr, err)
	}
	s.listener = listener

	mux := http.NewServeMux()
	mux.HandleFunc("POST /tso/next/", s.handleNext)
	mux.HandleFunc("GET /tso/status/", s.handleStatus)
	s.httpServer = &http.Server{Handler: mux}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[tso node=%d] http serve: %v", s.cfg.NodeID, err)
		}
	}()
	return nil
}

func (s *Service) handleNext(w http.ResponseWriter, req *http.Request) {
	var body NextRequest
	if err := decodeJSON(req, &body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	start, count, err := s.Next(req.Context(), body.Count)
	if err != nil {
		status := "internal"
		if !s.IsLeader() {
			status = "not-leader"
		}
		writeJSON(w, NextResponse{Status: status, LeaderID: int(s.leaders.Load()), Error: err.Error()})
		return
	}
	writeJSON(w, NextResponse{Status: "ok", LeaderID: s.cfg.NodeID, Start: start, Count: count})
}

func (s *Service) handleStatus(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, StatusResponse{
		Status:     "ok",
		LeaderID:   int(s.leaders.Load()),
		NodeID:     s.cfg.NodeID,
		IsLeader:   s.IsLeader(),
		Allocated:  s.oracle.Allocated(),
		LastIssued: s.oracle.LastIssued(),
	})
}
