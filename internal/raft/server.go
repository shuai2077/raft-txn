// Server container for a Raft Consensus Module. Exposes Raft to the network
// and enables RPCs between Raft peers.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Server wraps a raft.ConsensusModule along with a rpc.Server that exposes its
// methods as RPC endpoints. It also manages the peers of the Raft server. The
// main goal of this type is to simplify the code of raft.Server for
// presentation purposes. raft.ConsensusModule has a *Server to do its peer
// communication and doesn't have to worry about the specifics of running an
// RPC server.
type Server struct {
	mu sync.Mutex

	serverId int
	peerIds  []int

	// acceptDone is closed once the accept loop has exited. Shutdown waits on it
	// separately from wg so that it can tell "the loop is still blocked in
	// Accept" from "a handler is still running".
	acceptDone chan struct{}

	cm       *ConsensusModule
	storage  Storage
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	commitChan  chan<- CommitEntry
	peerClients map[int]*rpc.Client

	ready <-chan any
	quit  chan any
	wg    sync.WaitGroup

	// conns holds the currently open inbound RPC connections so that Shutdown can
	// close them. Closing the listener stops new connections but does not disturb
	// the handlers already blocked reading from established ones, so without this
	// a shutdown could wait forever on a peer that is itself shutting down
	// (txnkv local extension).
	conns map[net.Conn]struct{}
}

func NewServer(serverId int, peerIds []int, storage Storage, ready <-chan any, commitChan chan<- CommitEntry) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.storage = storage
	s.ready = ready
	s.commitChan = commitChan
	s.quit = make(chan any)
	s.conns = make(map[net.Conn]struct{})
	s.acceptDone = make(chan struct{})
	return s
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.cm = NewConsensusModule(s.serverId, s.peerIds, s, s.storage, s.ready, s.commitChan)

	// Create a new RPC server and register a RPCProxy that forwards all methods
	// to n.cm
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = NewProxy(s.cm)
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(s.acceptDone)

		for {
			// Check for shutdown before blocking in Accept. Combined with closing
			// the listener below, this makes the loop exit promptly rather than
			// relying on Accept surfacing an error from a closed listener.
			select {
			case <-s.quit:
				return
			default:
			}

			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.trackConn(conn, true)
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer s.trackConn(conn, false)
				s.rpcServer.ServeConn(conn)
				conn.Close()
			}()
		}
	}()
}

// trackConn records or forgets an inbound RPC connection.
func (s *Server) trackConn(conn net.Conn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		if s.conns != nil {
			s.conns[conn] = struct{}{}
		}
		return
	}
	delete(s.conns, conn)
}

// closeConns closes every open inbound RPC connection, which unblocks the
// per-connection handlers so that a shutdown can finish.
func (s *Server) closeConns() {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	for _, conn := range conns {
		_ = conn.Close()
	}
}

// Submit wraps the underlying CM's Submit; see that method for documentation.
func (s *Server) Submit(cmd any) int {
	return s.cm.Submit(cmd)
}

// SetHandler installs the replicated state machine that commands are applied
// to. It must be called after Serve (which creates the ConsensusModule) and
// before clients start submitting commands.
func (s *Server) SetHandler(handler StateMachineHandler) {
	s.mu.Lock()
	cm := s.cm
	s.mu.Unlock()
	if cm == nil {
		panic("raft: SetHandler called before Serve")
	}
	cm.SetHandler(handler)
}

// ApplyTimeout bounds how long Apply waits for its command to commit.
//
// It exists because a leader that has lost its quorum can never commit: waiting
// forever would hang the client instead of telling it that the outcome is
// unknown. Callers that can tolerate that uncertainty should prefer Submit and
// the commit channel.
const ApplyTimeout = 5 * time.Second

// Apply proposes cmd to the Raft log and blocks until it is committed and the
// state machine has applied it, then returns the state machine's result.
//
// This is the synchronous counterpart of Submit. It returns:
//
//   - a *NotLeaderError when this server is not the group's leader;
//   - an error when the command did not commit in time (usually: no quorum);
//   - otherwise the value StateMachineHandler.Apply returned for this command.
//
// Note that a command may be applied and then truncated if this leader loses
// leadership before the entry replicates to a quorum. Apply does not return in
// that case until the wait expires, and the state machine sees the entry
// disappear with the log - so state machines should tolerate re-application,
// which the transaction layer does by memoizing per-request results.
func (s *Server) Apply(cmd any) (any, error) {
	s.mu.Lock()
	cm := s.cm
	s.mu.Unlock()
	if cm == nil {
		return nil, fmt.Errorf("raft: Apply called before Serve")
	}

	cm.mu.Lock()
	if cm.state == Dead {
		cm.mu.Unlock()
		return nil, fmt.Errorf("raft: server is shut down")
	}
	if cm.state != Leader {
		leaderID := cm.leaderId
		cm.mu.Unlock()
		return nil, &NotLeaderError{LeaderID: leaderID}
	}

	index := cm.propose(cmd)
	// Register interest before releasing the lock, so the applier cannot deliver
	// the answer into a channel nobody is listening on.
	ch := make(chan proposalResult, 1)
	if cm.results == nil {
		cm.results = make(map[int]chan proposalResult)
	}
	cm.results[index] = ch
	// Nudge the applier in case the entry is already committed.
	if cm.commitIndex >= index {
		select {
		case cm.newCommitReadyChan <- struct{}{}:
		default:
		}
	}
	cm.mu.Unlock()

	timer := time.NewTimer(ApplyTimeout)
	defer timer.Stop()
	select {
	case res := <-ch:
		if res.err != nil {
			return nil, res.err
		}
		return res.value, nil
	case <-cm.stopChan:
		return nil, fmt.Errorf("raft: server stopped before the command committed")
	case <-timer.C:
		cm.abandonProposal(index, fmt.Errorf("raft: command was not committed in time (no quorum?)"))
		return nil, fmt.Errorf("raft: command at index %d was not committed within %s", index, ApplyTimeout)
	}
}

// SetDebug enables or disables the Raft logging for this server (txnkv local
// extension). An embedding application that does not want a line per heartbeat
// turns it off; the upstream tests leave it on.
func (s *Server) SetDebug(enabled bool) {
	s.mu.Lock()
	cm := s.cm
	s.mu.Unlock()
	if cm == nil {
		return
	}
	cm.SetDebug(enabled)
}

// NotLeaderError reports that a command was submitted to a replica that is not
// the leader of its Raft group. LeaderID carries a routing hint, or -1 when this
// node does not know who the leader is.
type NotLeaderError struct{ LeaderID int }

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("raft: not the leader (leader hint: %d)", e.LeaderID)
}

// LeaderID returns the id of the leader this server last heard from, or -1 when
// it has no idea. When this server believes it is the leader, it returns its own
// id.
func (s *Server) LeaderID() int {
	s.mu.Lock()
	cm := s.cm
	s.mu.Unlock()
	if cm == nil {
		return -1
	}
	return cm.LeaderID()
}

// CommitIndex returns the highest log index this server knows to be committed,
// or -1 when it has no CommitModule yet or has committed nothing.
func (s *Server) CommitIndex() int {
	s.mu.Lock()
	cm := s.cm
	s.mu.Unlock()
	if cm == nil {
		return -1
	}
	return cm.CommitIndex()
}

// DisconnectAll closes all the client connections to peers for this server.
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

// Shutdown closes the server and waits for it to shut down properly.
//
// Order matters: stop the consensus module (which ends the commit applier),
// signal the accept loop, close the listener, then close the established
// connections. Closing connections last is what unblocks the per-connection RPC
// handlers, which may be waiting on a peer that is shutting down at the same
// time and will therefore never send anything again.
func (s *Server) Shutdown() {
	s.cm.Stop()
	close(s.quit)

	// Wake the accept loop. Closing the listener is the intended mechanism, but
	// on some platforms a blocked Accept is not always released by Close, so a
	// throwaway connection is dialled to guarantee forward progress.
	_ = s.listener.Close()
	go s.wakeAcceptLoop()

	s.closeConns()
	<-s.acceptDone
	s.wg.Wait()
}

// wakeAcceptLoop dials the server's own address once so that a blocked Accept
// call returns. The connection is closed immediately.
func (s *Server) wakeAcceptLoop() {
	addr := s.GetListenAddr()
	if addr == nil {
		return
	}
	conn, err := net.DialTimeout("tcp", addr.String(), 200*time.Millisecond)
	if err != nil {
		return
	}
	_ = conn.Close()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

// DisconnectPeer disconnects this server from the peer identified by peerId.
func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		return err
	}
	return nil
}

func (s *Server) Call(id int, serviceMethod string, args any, reply any) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	// If this is called after shutdown (where client.Close is called), it will
	// return an error.
	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return s.rpcProxy.Call(peer, serviceMethod, args, reply)
	}
}

// IsLeader checks if s thinks it's the leader in the Raft cluster.
func (s *Server) IsLeader() bool {
	_, _, isLeader := s.cm.Report()
	return isLeader
}

// Proxy provides access to the RPC proxy this server is using; this is only
// for testing purposes to simulate faults.
func (s *Server) Proxy() *RPCProxy {
	return s.rpcProxy
}

// RPCProxy is a pass-thru proxy server for ConsensusModule's RPC methods. It
// serves RPC requests made to a CM and manipulates them before forwarding to
// the CM itself.
//
// It's useful for things like:
//   - Simulating dropping of RPC calls
//   - Simulating a small delay in RPC transmission.
//   - Simulating possible unreliable connections by delaying some messages
//     significantly and dropping others when RAFT_UNRELIABLE_RPC is set.
type RPCProxy struct {
	mu sync.Mutex
	cm *ConsensusModule

	// numCallsBeforeDrop is used to control dropping RPC calls:
	//   -1: means we're not dropping any calls
	//    0: means we're dropping all calls now
	//   >0: means we'll start dropping calls after this number is made
	numCallsBeforeDrop int
}

func NewProxy(cm *ConsensusModule) *RPCProxy {
	return &RPCProxy{
		cm:                 cm,
		numCallsBeforeDrop: -1,
	}
}

func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.cm.dlog("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.dlog("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.RequestVote(args, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			rpp.cm.dlog("drop AppendEntries")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			rpp.cm.dlog("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}

func (rpp *RPCProxy) Call(peer *rpc.Client, method string, args any, reply any) error {
	rpp.mu.Lock()
	if rpp.numCallsBeforeDrop == 0 {
		rpp.mu.Unlock()
		rpp.cm.dlog("drop Call %s: %v", method, args)
		return fmt.Errorf("RPC failed")
	} else {
		if rpp.numCallsBeforeDrop > 0 {
			rpp.numCallsBeforeDrop--
		}
		rpp.mu.Unlock()
		return peer.Call(method, args, reply)
	}
}

// DropCallsAfterN instruct the proxy to drop calls after n are made from this
// point.
func (rpp *RPCProxy) DropCallsAfterN(n int) {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()

	rpp.numCallsBeforeDrop = n
}

func (rpp *RPCProxy) DontDropCalls() {
	rpp.mu.Lock()
	defer rpp.mu.Unlock()

	rpp.numCallsBeforeDrop = -1
}
