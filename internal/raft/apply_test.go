package raft

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordingHandler is a state machine that records every command it applies.
type recordingHandler struct {
	mu       sync.Mutex
	applied  []int
	byServer map[int][]int
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{byServer: make(map[int][]int)}
}

func (h *recordingHandler) Apply(command any) any {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.applied = append(h.applied, command.(int))
	return command
}

func (h *recordingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.applied)
}

var handlerTestCounter atomic.Int64

// TestApplyReplicatesToFollowers checks the promise Server.Apply makes: after the
// leader applies a command, the entry reaches the followers' state machines too.
// Without that, a leader-local state machine would diverge from the replicas.
func TestApplyReplicatesToFollowers(t *testing.T) {
	const n = 3
	handlers := make([]*recordingHandler, n)
	servers := make([]*Server, n)
	commitChans := make([]chan CommitEntry, n)
	ready := make(chan any)
	var drained sync.WaitGroup

	for i := 0; i < n; i++ {
		peerIds := make([]int, 0, n-1)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}
		handlers[i] = newRecordingHandler()
		commitChans[i] = make(chan CommitEntry, 64)
		servers[i] = NewServer(i, peerIds, NewMapStorage(), ready, commitChans[i])
		servers[i].Serve()
		servers[i].SetHandler(handlers[i])
		drained.Add(1)
		go func(ch chan CommitEntry) {
			defer drained.Done()
			for range ch {
			}
		}(commitChans[i])
	}
	for i := 0; i < n; i++ {
		for p := 0; p < n; p++ {
			if p != i {
				if err := servers[i].ConnectToPeer(p, servers[p].GetListenAddr()); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	close(ready)

	// Wait for a leader.
	var leader int = -1
	for attempt := 0; attempt < 100 && leader < 0; attempt++ {
		for i := 0; i < n; i++ {
			if servers[i].IsLeader() {
				leader = i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader < 0 {
		t.Fatal("no leader elected")
	}
	t.Logf("leader is %d", leader)

	// Submit through the new synchronous path.
	value := int(handlerTestCounter.Add(1))
	out, err := servers[leader].Apply(value)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out != value {
		t.Fatalf("Apply returned %v; want %v", out, value)
	}
	if got := handlers[leader].count(); got != 1 {
		t.Errorf("leader applied %d commands; want 1", got)
	}

	// The followers must catch up. Give them a few heartbeat intervals.
	deadline := time.Now().Add(3 * time.Second)
	for {
		all := true
		for i := 0; i < n; i++ {
			if handlers[i].count() < 1 {
				all = false
			}
		}
		if all {
			break
		}
		if time.Now().After(deadline) {
			for i := 0; i < n; i++ {
				t.Errorf("server %d applied %d commands; want at least 1", i, handlers[i].count())
			}
			t.Fatalf("followers never applied the command")
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("all %d servers applied the command", n)

	for i := 0; i < n; i++ {
		servers[i].Shutdown()
	}
	// Close the commit channels now that the senders have stopped, and wait for
	// the drain goroutines to finish so the test leaks nothing.
	for i := 0; i < n; i++ {
		close(commitChans[i])
	}
	drained.Wait()
}
