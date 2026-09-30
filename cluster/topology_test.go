package cluster

import (
	"testing"
	"time"

	"github.com/eliben/raft/txnkv/txn"
)

// TestNodesLabelledDownAreActuallyRunning is a regression test for a labelling
// bug: DumpTopology reported every healthy *follower* as "down", because it had
// no case for "up and following" and fell through to its "down" default.
//
// It proves the nodes are serving rather than trusting the label: each one must
// have bound listeners, a live state machine, and the group must answer a read.
func TestNodesLabelledDownAreActuallyRunning(t *testing.T) {
	c := NewHarness(t, TestConfig(2, PrefixPartitioner{Groups: 2}))
	c.PutT(t, "0alice", "100")
	c.PutT(t, "1bob", "100")

	// Listeners first: those are bound at startup, so they need no waiting.
	// Values need waiting, and asking a follower for a value it has not
	// replicated yet would test replication lag rather than this bug.
	for g := 0; g < 2; g++ {
		for n := 0; n < c.cfg.NodesPerGroup; n++ {
			node := c.GroupNode(g, n)
			if node == nil {
				t.Fatalf("group %d node %d is nil", g, n)
			}
			if node.RaftListenAddr() == nil {
				t.Errorf("group %d node %d has no Raft listener", g, n)
			}
			if node.Addr() == "" {
				t.Errorf("group %d node %d has no HTTP address", g, n)
			}
			if node.Store() == nil {
				t.Fatalf("group %d node %d has no state machine", g, n)
			}
		}
	}
	for i := 0; i < c.cfg.TSONodes; i++ {
		svc := c.TSOService(i)
		if svc == nil || svc.RaftListenAddr() == nil || svc.Addr() == "" {
			t.Errorf("tso node %d has no listeners", i)
		}
	}

	// Wait for replication, then check that every replica really holds the data.
	// A node that never started cannot.
	if err := c.WaitForSettled(20 * time.Second); err != nil {
		t.Fatalf("the cluster never settled: %v", err)
	}
	for g := 0; g < 2; g++ {
		key := txn.Key("0alice")
		want := txn.Value("100")
		if g == 1 {
			key = txn.Key("1bob")
		}
		for n := 0; n < c.cfg.NodesPerGroup; n++ {
			value, found := c.GroupNode(g, n).Store().ValueAtNow(key)
			if !found || value != want {
				t.Errorf("group %d node %d state = (%q, found=%v); want (%q, true): "+
					"the node is not running or is not receiving the log", g, n, value, found, want)
			}
		}
	}

	// The groups can serve reads, which is the property that actually matters.
	for _, tc := range []struct {
		key  txn.Key
		want txn.Value
	}{{"0alice", "100"}, {"1bob", "100"}} {
		value, found, err := c.Read(tc.key)
		if err != nil || !found || value != tc.want {
			t.Errorf("read %q: (%q, found=%v, err=%v); want (%q, true, nil)",
				tc.key, value, found, err, tc.want)
		}
	}

	// And the label now matches reality: exactly one LEADER per group, and nothing
	// spuriously "down".
	topology := c.DumpTopology()
	t.Logf("topology: %v", topology)
	leaders := 0
	for _, line := range topology {
		if contains(line, "down") {
			t.Errorf("topology reports a down node although every node is serving: %q", line)
		}
		if contains(line, "LEADER") {
			leaders++
		}
	}
	if leaders != c.cfg.NumGroups+1 { // one per group, plus the oracle
		t.Errorf("topology shows %d leaders, want %d", leaders, c.cfg.NumGroups+1)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
