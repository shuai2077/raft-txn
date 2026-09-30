// Package cluster wires the txnkv stack into a runnable Multi-Raft cluster and
// exposes it for testing.
//
// A Cluster owns:
//
//   - N transaction groups, each a Raft cluster of M txnservice.Node values.
//     Every node serves the transaction protocol over HTTP and holds one shard of
//     the key space.
//   - one replicated timestamp oracle (tso.Service) group.
//   - a transaction client bound to all of them.
//
// The harness deliberately knows how to break things: nodes can be disconnected
// from their peers, crashed (losing all volatile state, keeping their Raft
// storage) and restarted from that storage. Those are the scenarios that decide
// whether the transaction protocol is actually correct, so they are part of the
// public surface rather than test-only helpers.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eliben/raft/txnkv/internal/raft"
	"github.com/eliben/raft/txnkv/tso"
	"github.com/eliben/raft/txnkv/txn"
	"github.com/eliben/raft/txnkv/txnservice"
)

// Config describes the cluster to build.
type Config struct {
	// NumGroups is how many transaction groups (shards) to run.
	NumGroups int
	// NodesPerGroup is how many Raft members each transaction group has.
	NodesPerGroup int
	// TSONodes is how many members the timestamp oracle has. One is enough to
	// exercise the protocol; three lets tests fail the oracle's leader over.
	TSONodes int
	// WindowSize is the oracle's timestamp allocation window.
	WindowSize uint64
	// LockTTL is the intent-lock lease used by the groups.
	LockTTL time.Duration
	// HTTPBase is the first TCP port used; nodes take consecutive ports from it.
	// Zero requests OS-assigned ports, which is slower but never collides.
	HTTPBase int
	// Partitioner decides which group owns a key. Required.
	Partitioner txn.Partitioner
}

// Cluster is a running Multi-Raft transactional store.
type Cluster struct {
	cfg Config

	// nodes[group][node] is a transaction group member.
	nodes [][]*txnservice.Node

	// storages[group][node] is the group member's durable Raft storage, retained
	// across crashes so a restart resumes from its own log.
	storages [][]*raft.MapStorage

	// alive[group][node] reports whether that member is currently running.
	alive [][]bool

	// connected[group][node] reports whether that member can currently exchange
	// Raft messages with its peers.
	connected [][]bool

	// tsoNodes are the timestamp oracle members.
	tsoNodes []*tso.Service

	// tsoStorages retains the oracle members' durable storage across restarts.
	tsoStorages  []*raft.MapStorage
	tsoAlive     []bool
	tsoConnected []bool

	// clients is the wire client the transaction coordinator talks through.
	clients *txnservice.Client

	// coordinator is the transaction client.
	txnClient *txn.Client

	mu sync.Mutex
}

// New creates and starts a cluster from cfg.
func New(cfg Config) (*Cluster, error) {
	if cfg.NumGroups < 1 {
		return nil, errors.New("cluster: NumGroups must be at least 1")
	}
	if cfg.NodesPerGroup < 1 {
		return nil, errors.New("cluster: NodesPerGroup must be at least 1")
	}
	if cfg.TSONodes < 1 {
		return nil, errors.New("cluster: TSONodes must be at least 1")
	}
	if cfg.Partitioner == nil {
		return nil, errors.New("cluster: Partitioner is required")
	}

	c := &Cluster{
		cfg:          cfg,
		nodes:        make([][]*txnservice.Node, cfg.NumGroups),
		storages:     make([][]*raft.MapStorage, cfg.NumGroups),
		alive:        make([][]bool, cfg.NumGroups),
		connected:    make([][]bool, cfg.NumGroups),
		tsoNodes:     make([]*tso.Service, cfg.TSONodes),
		tsoStorages:  make([]*raft.MapStorage, cfg.TSONodes),
		tsoAlive:     make([]bool, cfg.TSONodes),
		tsoConnected: make([]bool, cfg.TSONodes),
	}

	ready := make(chan any)
	ports := newPortAllocator(cfg.HTTPBase)

	// --- transaction groups ---
	for g := 0; g < cfg.NumGroups; g++ {
		c.nodes[g] = make([]*txnservice.Node, cfg.NodesPerGroup)
		c.storages[g] = make([]*raft.MapStorage, cfg.NodesPerGroup)
		c.alive[g] = make([]bool, cfg.NodesPerGroup)
		c.connected[g] = make([]bool, cfg.NodesPerGroup)

		peerIDs := idsExcept(cfg.NodesPerGroup, -1)
		for n := 0; n < cfg.NodesPerGroup; n++ {
			c.storages[g][n] = raft.NewMapStorage()
			node, err := txnservice.NewNode(txnservice.NodeConfig{
				GroupID:  txn.GroupID(g),
				NodeID:   n,
				PeerIDs:  idsExcept(cfg.NodesPerGroup, n),
				Storage:  c.storages[g][n],
				Ready:    ready,
				LockTTL:  cfg.LockTTL,
				HTTPAddr: ports.next(),
			})
			if err != nil {
				return nil, fmt.Errorf("cluster: starting group %d node %d: %w", g, n, err)
			}
			c.nodes[g][n] = node
			c.alive[g][n] = true
		}
		_ = peerIDs
	}

	// --- timestamp oracle ---
	for i := 0; i < cfg.TSONodes; i++ {
		c.tsoStorages[i] = raft.NewMapStorage()
		svc, err := tso.NewService(tso.ServiceConfig{
			NodeID:     i,
			PeerIDs:    idsExcept(cfg.TSONodes, i),
			Storage:    c.tsoStorages[i],
			Ready:      ready,
			WindowSize: cfg.WindowSize,
			HTTPAddr:   ports.next(),
		})
		if err != nil {
			return nil, fmt.Errorf("cluster: starting tso node %d: %w", i, err)
		}
		c.tsoNodes[i] = svc
		c.tsoAlive[i] = true
	}

	// --- connect every group member to its peers ---
	for g := 0; g < cfg.NumGroups; g++ {
		for n := 0; n < cfg.NodesPerGroup; n++ {
			for p := 0; p < cfg.NodesPerGroup; p++ {
				if p == n {
					continue
				}
				if err := c.nodes[g][n].ConnectToPeer(p, c.nodes[g][p].RaftListenAddr()); err != nil {
					return nil, fmt.Errorf("cluster: connecting group %d node %d to %d: %w", g, n, p, err)
				}
			}
			c.connected[g][n] = true
		}
	}
	for i := 0; i < cfg.TSONodes; i++ {
		for p := 0; p < cfg.TSONodes; p++ {
			if p == i {
				continue
			}
			if err := c.tsoNodes[i].ConnectToPeer(p, c.tsoNodes[p].RaftListenAddr()); err != nil {
				return nil, fmt.Errorf("cluster: connecting tso %d to %d: %w", i, p, err)
			}
		}
		c.tsoConnected[i] = true
	}

	// Everything is connected; let the Raft instances start electing.
	close(ready)

	// --- client ---
	groups := make(map[txn.GroupID][]string, cfg.NumGroups)
	for g := 0; g < cfg.NumGroups; g++ {
		addrs := make([]string, cfg.NodesPerGroup)
		for n := 0; n < cfg.NodesPerGroup; n++ {
			addrs[n] = c.nodes[g][n].Addr()
		}
		groups[txn.GroupID(g)] = addrs
	}
	tsoAddrs := make([]string, cfg.TSONodes)
	for i := 0; i < cfg.TSONodes; i++ {
		tsoAddrs[i] = c.tsoNodes[i].Addr()
	}

	c.clients = txnservice.NewClient(txnservice.ClientConfig{Groups: groups})
	oracle := tso.NewClient(tsoAddrs)
	c.txnClient = txn.NewClient(oracle, cfg.Partitioner, c.clients.Participants(), nil)

	// The coordinator and its resolver are mutually recursive: the resolver walks
	// the coordinator's participant set, and the coordinator needs a resolver to
	// clear foreign locks. Both are therefore bound here, after both exist.
	resolver := txn.NewResolver(c.txnClient)
	c.txnClient.SetResolver(resolver)
	c.clients.BindResolver(resolver)
	c.txnClient.SetResolveTimeout(2 * time.Second)

	return c, nil
}

// Client returns the transaction client.
func (c *Cluster) Client() *txn.Client { return c.txnClient }

// GroupNode returns one group member, for direct inspection.
func (c *Cluster) GroupNode(group, node int) *txnservice.Node { return c.nodes[group][node] }

// TSOService returns one timestamp oracle member.
func (c *Cluster) TSOService(node int) *tso.Service { return c.tsoNodes[node] }

// NumGroups reports how many transaction groups exist.
func (c *Cluster) NumGroups() int { return c.cfg.NumGroups }

// WaitForLeaders blocks until every group and the oracle have a leader, or the
// deadline passes.
func (c *Cluster) WaitForLeaders(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if c.allHaveLeaders() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster: no leader in some group after %s", timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *Cluster) allHaveLeaders() bool {
	for g := range c.nodes {
		found := false
		for n := range c.nodes[g] {
			if c.connected[g][n] && c.nodes[g][n].IsLeader() {
				if found {
					return false // two leaders: keep waiting for the cluster to settle
				}
				found = true
			}
		}
		if !found {
			return false
		}
	}
	found := false
	for i := range c.tsoNodes {
		if c.tsoConnected[i] && c.tsoNodes[i].IsLeader() {
			if found {
				return false
			}
			found = true
		}
	}
	return found
}

// LeaderOf returns the node id leading a transaction group, or -1.
func (c *Cluster) LeaderOf(group int) int {
	for n := range c.nodes[group] {
		if c.connected[group][n] && c.nodes[group][n].IsLeader() {
			return n
		}
	}
	return -1
}

// TSOLeader returns the oracle member id that leads, or -1.
func (c *Cluster) TSOLeader() int {
	for i := range c.tsoNodes {
		if c.tsoConnected[i] && c.tsoNodes[i].IsLeader() {
			return i
		}
	}
	return -1
}

// --- fault injection ---------------------------------------------------------

// DisconnectNode partitions one group member away from its peers. Its Raft
// instance keeps running, so it will campaign and can even become a leader in
// its own minority - which is exactly the situation the protocol has to survive.
func (c *Cluster) DisconnectNode(group, node int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[group][node].DisconnectFromAllPeers()
	for p := range c.nodes[group] {
		if p != node {
			_ = c.nodes[group][p].DisconnectFromPeer(node)
		}
	}
	c.connected[group][node] = false
}

// ReconnectNode undoes DisconnectNode.
func (c *Cluster) ReconnectNode(group, node int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for p := range c.nodes[group] {
		if p == node || !c.alive[group][p] {
			continue
		}
		_ = c.nodes[group][node].ConnectToPeer(p, c.nodes[group][p].RaftListenAddr())
		_ = c.nodes[group][p].ConnectToPeer(node, c.nodes[group][node].RaftListenAddr())
	}
	c.connected[group][node] = true
}

// CrashNode disconnects and stops one group member. Its Raft storage is kept, so
// RestartNode resumes from the same log.
func (c *Cluster) CrashNode(group, node int) error {
	c.mu.Lock()
	c.nodes[group][node].DisconnectFromAllPeers()
	for p := range c.nodes[group] {
		if p != node {
			_ = c.nodes[group][p].DisconnectFromPeer(node)
		}
	}
	c.connected[group][node] = false
	c.alive[group][node] = false
	c.mu.Unlock()

	return c.nodes[group][node].Shutdown()
}

// RestartNode starts a fresh member in place of a crashed one, reusing its
// durable Raft storage. It must only be called for a crashed node.
func (c *Cluster) RestartNode(group, node int) error {
	c.mu.Lock()
	if c.alive[group][node] {
		c.mu.Unlock()
		return fmt.Errorf("cluster: node %d/%d is alive", group, node)
	}
	c.mu.Unlock()

	ready := make(chan any)
	peerIDs := idsExcept(c.cfg.NodesPerGroup, node)
	restarted, err := txnservice.NewNode(txnservice.NodeConfig{
		GroupID:  txn.GroupID(group),
		NodeID:   node,
		PeerIDs:  peerIDs,
		Storage:  c.storages[group][node],
		Ready:    ready,
		LockTTL:  c.cfg.LockTTL,
		HTTPAddr: c.nodes[group][node].Addr(),
	})
	if err != nil {
		return fmt.Errorf("cluster: restarting node %d/%d: %w", group, node, err)
	}
	c.nodes[group][node] = restarted

	c.mu.Lock()
	for p := range c.nodes[group] {
		if p == node || !c.alive[group][p] {
			continue
		}
		if err := restarted.ConnectToPeer(p, c.nodes[group][p].RaftListenAddr()); err != nil {
			c.mu.Unlock()
			return err
		}
		if err := c.nodes[group][p].ConnectToPeer(node, restarted.RaftListenAddr()); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	c.connected[group][node] = true
	c.alive[group][node] = true
	c.mu.Unlock()

	close(ready)
	return nil
}

// CrashTSO disconnects and stops one oracle member.
func (c *Cluster) CrashTSO(node int) error {
	c.mu.Lock()
	c.tsoNodes[node].DisconnectFromAllPeers()
	for p := range c.tsoNodes {
		if p != node {
			_ = c.tsoNodes[p].DisconnectFromPeer(node)
		}
	}
	c.tsoConnected[node] = false
	c.tsoAlive[node] = false
	c.mu.Unlock()
	return c.tsoNodes[node].Shutdown()
}

// RestartTSO restarts a crashed oracle member from its durable storage.
func (c *Cluster) RestartTSO(node int) error {
	c.mu.Lock()
	if c.tsoAlive[node] {
		c.mu.Unlock()
		return fmt.Errorf("cluster: tso node %d is alive", node)
	}
	c.mu.Unlock()

	ready := make(chan any)
	restarted, err := tso.NewService(tso.ServiceConfig{
		NodeID:     node,
		PeerIDs:    idsExcept(c.cfg.TSONodes, node),
		Storage:    c.tsoStorages[node],
		Ready:      ready,
		WindowSize: c.cfg.WindowSize,
		HTTPAddr:   c.tsoNodes[node].Addr(),
	})
	if err != nil {
		return fmt.Errorf("cluster: restarting tso node %d: %w", node, err)
	}
	c.tsoNodes[node] = restarted

	c.mu.Lock()
	for p := range c.tsoNodes {
		if p == node || !c.tsoAlive[p] {
			continue
		}
		if err := restarted.ConnectToPeer(p, c.tsoNodes[p].RaftListenAddr()); err != nil {
			c.mu.Unlock()
			return err
		}
		if err := c.tsoNodes[p].ConnectToPeer(node, restarted.RaftListenAddr()); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	c.tsoConnected[node] = true
	c.tsoAlive[node] = true
	c.mu.Unlock()

	close(ready)
	return nil
}

// Shutdown stops every member of the cluster.
func (c *Cluster) Shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for g := range c.nodes {
		for n := range c.nodes[g] {
			if c.alive[g][n] {
				c.nodes[g][n].DisconnectFromAllPeers()
				c.connected[g][n] = false
			}
		}
	}
	for i := range c.tsoNodes {
		if c.tsoAlive[i] {
			c.tsoNodes[i].DisconnectFromAllPeers()
			c.tsoConnected[i] = false
		}
	}

	for g := range c.nodes {
		for n := range c.nodes[g] {
			if c.alive[g][n] {
				c.alive[g][n] = false
				_ = c.nodes[g][n].Shutdown()
			}
		}
	}
	for i := range c.tsoNodes {
		if c.tsoAlive[i] {
			c.tsoAlive[i] = false
			_ = c.tsoNodes[i].Shutdown()
		}
	}
	_ = c.txnClient.Close()
}

// --- helpers -----------------------------------------------------------------

// contextBackground is the default context for the harness's convenience calls.
func contextBackground() context.Context { return context.Background() }

func idsExcept(n, except int) []int {
	out := make([]int, 0, n)
	for i := 0; i < n; i++ {
		if i != except {
			out = append(out, i)
		}
	}
	return out
}

// portAllocator hands out consecutive TCP ports, or returns ":0" so the OS picks
// one when the base is zero.
type portAllocator struct {
	nextPort int
}

func newPortAllocator(base int) *portAllocator { return &portAllocator{nextPort: base} }

func (p *portAllocator) next() string {
	if p.nextPort == 0 {
		return "127.0.0.1:0"
	}
	addr := fmt.Sprintf("127.0.0.1:%d", p.nextPort)
	p.nextPort++
	return addr
}

// --- test helpers ------------------------------------------------------------

// WaitForLeadersT fails the test if the cluster does not settle.
func (c *Cluster) WaitForLeadersT(t *testing.T, timeout time.Duration) {
	t.Helper()
	if err := c.WaitForLeaders(timeout); err != nil {
		t.Fatalf("waiting for leaders: %v", err)
	}
}

// RunTxn runs fn inside a transaction, retrying on any transient failure.
//
// This is the retry loop every application over a snapshot-isolation store
// needs, so the tests exercise it rather than hiding it. Two outcomes are
// normal, not fatal:
//
//   - a *txn.WriteConflictError: somebody else committed to a key we wrote;
//   - a *txn.LockedError or a lock-wait timeout: another transaction holds an
//     intent lock on a key we need, and it may still be about to commit.
//
// Retrying is correct in both cases because the failed attempt changed nothing.
//
// Before reporting success it verifies the transaction's own claim: every write
// the caller asked for must be readable. A write lock left behind means "commit
// returned success but part of the transaction was never published", which is
// precisely the failure atomicity is supposed to rule out - so it is checked
// rather than assumed, and reported as an error instead of becoming a silently
// wrong answer.
func (c *Cluster) RunTxn(ctx context.Context, attempts int, fn func(*txn.Txn) error) (txn.Timestamp, error) {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		tx, err := c.txnClient.Begin(ctx)
		if err != nil {
			return 0, err
		}
		if err := fn(tx); err != nil {
			_ = tx.Abort(ctx)
			if isTransient(err) {
				lastErr = err
				continue
			}
			return 0, err
		}
		ts, err := tx.Commit(ctx)
		if err == nil {
			return ts, nil
		}
		lastErr = err
		var unknown *txn.CommitUnknownError
		if errors.As(err, &unknown) {
			// The transaction *is* committed: its commit point is durable. But
			// some of its writes may not be published yet, so reporting success
			// here would let the caller observe a half-applied transaction - the
			// very atomicity violation this design exists to prevent.
			//
			// So drive the remaining writes out before reporting success. The
			// transaction must not be retried as a whole either, because its
			// effects are already partly visible; finalizing it is the only
			// correct move.
			if finishErr := tx.Finish(ctx); finishErr != nil {
				lastErr = fmt.Errorf("committed at ts %d but not finalized: %w", unknown.CommitTS, finishErr)
				continue
			}
			return unknown.CommitTS, nil
		}
		if isTransient(err) {
			continue
		}
		return 0, err
	}
	return 0, fmt.Errorf("cluster: transaction did not succeed after %d attempts: %w", attempts, lastErr)
}

// isTransient reports whether an error means "try again", as opposed to a real
// failure.
func isTransient(err error) bool {
	if _, conflict := txn.IsWriteConflict(err); conflict {
		return true
	}
	var locked *txn.LockedError
	if errors.As(err, &locked) {
		return true
	}
	// A read that gave up waiting for a foreign lock is also transient: the
	// transaction changed nothing, so retrying it is safe and the lock will
	// usually be gone by then.
	var nl *txn.NotLeaderError
	return errors.As(err, &nl)
}
