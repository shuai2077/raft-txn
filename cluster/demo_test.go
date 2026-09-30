package cluster

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eliben/raft/txnkv/txn"
)

// The demo in cmd/demo is documentation that can rot: it makes claims about
// atomicity, isolation and recovery, and nothing fails if a refactor quietly
// turns one of them into prose. This test executes it and checks the claims
// against its output.
//
// It runs the real binary rather than calling the same functions, because the
// thing under test is the narrative a reader will see.

// TestDemoOutputMatchesItsClaims runs cmd/demo and asserts the properties it
// advertises.
func TestDemoOutputMatchesItsClaims(t *testing.T) {
	if testing.Short() {
		t.Skip("building and running the demo binary takes a moment")
	}

	repoRoot := repoRootForTest(t)
	binary := filepath.Join(t.TempDir(), "demo.exe")
	if os.PathSeparator != '\\' {
		binary = filepath.Join(t.TempDir(), "demo")
	}

	build := exec.Command("go", "build", "-o", binary, "./cmd/demo")
	build.Dir = repoRoot
	build.Env = os.Environ()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the demo: %v\n%s", err, out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, binary)
	run.Dir = repoRoot
	out, err := run.CombinedOutput()
	output := string(out)
	if err != nil {
		t.Fatalf("running the demo: %v\n%s", err, output)
	}

	// Each check is a claim the demo makes to its reader.
	claims := []struct {
		what string
		want string
	}{
		{
			"a cross-shard transfer conserves the total (70 + 130 = 200)",
			`after the transfer: alice = 70, bob = 130`,
		},
		{
			"a long reader keeps seeing its own snapshot",
			`the same snapshot still sees alice = 70`,
		},
		{
			"a fresh reader sees the newer committed value",
			`a new transaction sees alice = 5`,
		},
		{
			"a losing writer gets a typed conflict naming the key and the winning timestamp",
			`write conflict on key "alice": committed at ts `,
		},
		{
			"the prepared write is proven not yet visible, so recovery is doing real work",
			`with an intent lock held`,
		},
		{
			"the primary lock is sealed, which is the commit point",
			`primary lock sealed at timestamp `,
		},
		{
			"the committed write survives a coordinator that never finished",
			`and finds bob = 999`,
		},
		{
			"the demo runs to completion",
			`=== done ===`,
		},
	}
	for _, claim := range claims {
		if !strings.Contains(output, claim.want) {
			t.Errorf("the demo no longer shows that %s\n  expected to find: %q\n  actual output:\n%s",
				claim.what, claim.want, output)
		}
	}
}

// TestDemoClaimsAreConsistentWithTheStore checks the one claim the demo cannot
// prove by printing: that the reader really did finish the coordinator's work,
// rather than the write having been published by the seal itself.
//
// It rebuilds the same scenario in-process and inspects the shard directly, so
// that the demo's narrative and the store's actual behaviour cannot drift apart.
func TestDemoClaimsAreConsistentWithTheStore(t *testing.T) {
	// The demo routes its two keys by name, so alice and bob really do live in
	// different shards and the transfer really is a cross-shard transaction.
	// PrefixPartitioner would not do: it routes on a digit prefix, so the bare
	// keys "alice" and "bob" would both land in group 0 and the whole scenario
	// would degenerate into a single-group transaction.
	partitioner := &namedPartitioner{shards: map[string]int{"alice": 0, "bob": 1}}
	c := NewHarness(t, TestConfig(2, partitioner))
	if g := partitioner.GroupForKey("alice"); g == partitioner.GroupForKey("bob") {
		t.Fatalf("the scenario needs alice and bob in different groups; both are in group %d", g)
	}

	c.PutT(t, "alice", "100")
	c.PutT(t, "bob", "100")

	transfer, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := transfer.Put("alice", "70"); err != nil {
		t.Fatal(err)
	}
	if err := transfer.Put("bob", "130"); err != nil {
		t.Fatal(err)
	}
	if _, err := transfer.Commit(context.Background()); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	// Use the retrying read: a read of a key whose writer is mid-commit fails
	// until that writer's lease lapses, which is normal behaviour and not what
	// this test is about.
	c.MustGetEventually(t, "alice", "70", 10*time.Second)
	c.MustGetEventually(t, "bob", "130", 10*time.Second)

	// Now the crash window the demo describes.
	orphan, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := orphan.Put("alice", "999"); err != nil {
		t.Fatal(err)
	}
	if err := orphan.Put("bob", "999"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Prewrite(orphan); err != nil {
		t.Fatalf("prewrite: %v", err)
	}

	// Prove the transaction really spans both shards rather than relying on the
	// partitioner's naming: an intent lock must be held in each group, for the
	// key that group owns. Without this, a routing mistake would silently turn
	// the whole scenario into a single-group transaction that still "passes".
	lockedIn := func(group int, key txn.Key) bool {
		_, locked := c.GroupNode(group, c.LeaderOf(group)).Store().Locks()[key]
		return locked
	}
	if !lockedIn(0, "alice") {
		t.Fatal("group 0 holds no lock on alice; the transaction was not prepared there")
	}
	if !lockedIn(1, "bob") {
		t.Fatal("group 1 holds no lock on bob; the transaction is not cross-shard")
	}

	// Before the seal: the last committed value is readable, and the pending
	// write is not. This is the state the demo checks on screen.
	before := c.GroupNode(1, c.LeaderOf(1)).Store()
	value, _ := before.ValueAtNow("bob")
	if value != "130" {
		t.Fatalf("bob = %q before the seal; want the last committed value 130", value)
	}
	if _, locked := before.Locks()["bob"]; !locked {
		t.Fatal("bob has no intent lock; the transaction was not prepared")
	}

	// Seal the primary and stop, exactly as the demo does. The primary key must be
	// the one the demo names, or this test would be sealing a lock that is not the
	// transaction's commit point.
	primary := GuessPrimaryKey(orphan.Writes())
	if primary != "alice" {
		t.Fatalf("the primary key is %q; this test seals %q", primary, "alice")
	}
	if _, err := c.SealPrimary(orphan, primary, 0, c.LeaderOf(0)); err != nil {
		t.Fatalf("sealing the primary lock: %v", err)
	}

	// The secondary shard still holds an unpublished write: 999 is not visible
	// yet, so whatever makes it visible later is genuinely the recovery path.
	stillPending := c.GroupNode(1, c.LeaderOf(1)).Store()
	if value, _ := stillPending.ValueAtNow("bob"); value == "999" {
		t.Fatal("sealing the primary published the secondary write; recovery would be an illusion")
	}

	// A reader resolves the lock and the committed write appears.
	c.MustGetEventually(t, "bob", "999", 10*time.Second)
}

// namedPartitioner routes the demo's two keys to two different shards. It is a
// copy of the one in cmd/demo, kept here so this test controls its own scenario
// rather than depending on the demo's wiring.
type namedPartitioner struct{ shards map[string]int }

func (p *namedPartitioner) GroupForKey(key txn.Key) txn.GroupID {
	if shard, ok := p.shards[string(key)]; ok {
		return txn.GroupID(shard)
	}
	return 0
}

// repoRootForTest walks up from the test's working directory to the module root.
func repoRootForTest(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find the module root")
		}
		dir = parent
	}
}
