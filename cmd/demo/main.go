// Command demo runs a small txnkv cluster and demonstrates what the transaction
// layer actually buys you: atomicity across shards, snapshot isolation, conflict
// handling, and recovery from a coordinator that died mid-commit.
//
// Run it with:
//
//	go run ./cmd/demo
//
// It is deliberately verbose. The point is to watch the mechanism, not to
// benchmark it.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/eliben/raft/txnkv/cluster"
	"github.com/eliben/raft/txnkv/txn"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("demo: ")
	log.SetOutput(os.Stdout)

	partitioner := &namedPartitioner{
		shards: map[string]int{
			"alice": 0,
			"bob":   1,
		},
	}

	fmt.Println("=== starting a cluster: 2 shards x 3 Raft members, 3-member oracle ===")

	// Raft routes all its logging through the standard logger, which is
	// process-global, so it is silenced for cluster startup and leader discovery
	// and restored afterwards.
	log.SetOutput(io.Discard)
	c, err := cluster.New(cluster.Config{
		NumGroups:     2,
		NodesPerGroup: 3,
		TSONodes:      3,
		WindowSize:    64,
		LockTTL:       time.Second,
		HTTPBase:      0, // let the OS pick ports
		Partitioner:   partitioner,
	})
	if err != nil {
		log.SetOutput(os.Stdout)
		log.Fatalf("starting the cluster: %v", err)
	}
	defer c.Shutdown()

	if err := c.WaitForLeaders(10 * time.Second); err != nil {
		log.SetOutput(os.Stdout)
		log.Fatalf("waiting for leaders: %v", err)
	}
	c.SetRaftLogging(false)
	log.SetOutput(os.Stdout)

	fmt.Println("cluster is up:")
	for _, line := range c.DumpTopology() {
		fmt.Println("  " + line)
	}

	ctx := context.Background()

	// ---------------------------------------------------------------- atomicity
	fmt.Println("\n=== 1. atomicity across shards ===")
	fmt.Println("alice's account lives in shard 0, bob's in shard 1.")
	fmt.Println("A transfer therefore has to commit in two Raft groups at once.")

	mustTxn(c, func(tx *txn.Txn) error {
		if err := tx.Put("alice", "100"); err != nil {
			return err
		}
		return tx.Put("bob", "100")
	})
	showBalances(c, "opening balances")

	transfer, err := c.Client().Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if err := transfer.Put("alice", "70"); err != nil {
		log.Fatal(err)
	}
	if err := transfer.Put("bob", "130"); err != nil {
		log.Fatal(err)
	}
	commitTS, err := transfer.Commit(ctx)
	if err != nil {
		log.Fatalf("transfer: %v", err)
	}
	fmt.Printf("transfer committed at timestamp %d\n", commitTS)
	showBalances(c, "after the transfer")

	// ------------------------------------------------------- snapshot isolation
	fmt.Println("\n=== 2. snapshot isolation ===")
	fmt.Println("A reader takes one snapshot and keeps reading from it while others write.")

	snapshot, err := c.Client().Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	before, _, err := snapshot.Get(ctx, "alice")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("reader's snapshot sees alice = %s\n", before)

	mustTxn(c, func(tx *txn.Txn) error { return tx.Put("alice", "5") })
	fmt.Println("another transaction set alice = 5")

	again, _, err := snapshot.Get(ctx, "alice")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("the same snapshot still sees alice = %s  (repeatable read)\n", again)
	fresh, _, _ := freshRead(c, "alice")
	fmt.Printf("a new transaction sees alice = %s\n", fresh)

	// ------------------------------------------------------------ write conflict
	fmt.Println("\n=== 3. write conflicts ===")
	fmt.Println("Two transactions read the same key, then both try to write it.")

	t1, err := c.Client().Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	t2, err := c.Client().Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, _, err := t1.Get(ctx, "alice"); err != nil {
		log.Fatal(err)
	}
	if _, _, err := t2.Get(ctx, "alice"); err != nil {
		log.Fatal(err)
	}
	if err := t1.Put("alice", "10"); err != nil {
		log.Fatal(err)
	}
	if err := t2.Put("alice", "20"); err != nil {
		log.Fatal(err)
	}

	if _, err := t1.Commit(ctx); err != nil {
		log.Fatalf("first writer should have won: %v", err)
	}
	fmt.Println("first writer committed")

	_, err = t2.Commit(ctx)
	var conflict *txn.WriteConflictError
	if errors.As(err, &conflict) {
		fmt.Printf("second writer was refused: %v\n", conflict)
		fmt.Println("this is the isolation guarantee: its snapshot predates the winner's commit,")
		fmt.Println("so applying it would have silently overwritten a value it never saw.")
	} else {
		log.Fatalf("expected a write conflict, got: %v", err)
	}

	// ------------------------------------------------------ crash-mid-commit
	fmt.Println("\n=== 4. a coordinator that dies mid-commit ===")
	fmt.Println("We prepare a cross-shard transaction and seal its primary lock, then stop:")
	fmt.Println("this is the state a coordinator leaves behind if it crashes after deciding")
	fmt.Println("to commit but before finishing the other shards.")

	orphan, err := c.Client().Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if err := orphan.Put("alice", "999"); err != nil {
		log.Fatal(err)
	}
	if err := orphan.Put("bob", "999"); err != nil {
		log.Fatal(err)
	}
	if err := txn.Prewrite(orphan); err != nil {
		log.Fatalf("prewrite: %v", err)
	}
	primary := cluster.GuessPrimaryKey(orphan.Writes())
	fmt.Printf("intent locks are in place; the primary key is %q\n", primary)

	leaderOfShard := c.LeaderOf(0)
	if leaderOfShard < 0 {
		log.Fatal("shard 0 has no leader")
	}
	sealed, sealErr := c.SealPrimary(orphan, primary, 0, leaderOfShard)
	if sealErr != nil {
		log.Fatalf("sealing the primary lock: %v", sealErr)
	}
	fmt.Printf("primary lock sealed at timestamp %d: the transaction is now committed,\n", sealed)
	fmt.Println("even though nobody has finished it.")

	fmt.Println("a reader stumbles onto the secondary lock...")
	recovered, found, readErr := c.WaitForRead("bob", 10*time.Second)
	if readErr != nil {
		log.Fatalf("reading bob after recovery: %v", readErr)
	}
	if !found {
		log.Fatal("bob has no value after recovery")
	}
	fmt.Printf("...and finds bob = %s\n", recovered)
	if recovered != "999" {
		log.Fatalf("the committed write was lost: bob = %s", recovered)
	}
	fmt.Println("the reader finished the transaction's work. Losing that write would have")
	fmt.Println("meant acknowledging a commit and then discarding it.")

	fmt.Println("\n=== done ===")
}

// namedPartitioner routes the demo's two keys to two different shards, which is
// what makes every operation above a cross-shard transaction.
type namedPartitioner struct{ shards map[string]int }

func (p *namedPartitioner) GroupForKey(key txn.Key) txn.GroupID {
	if shard, ok := p.shards[string(key)]; ok {
		return txn.GroupID(shard)
	}
	return 0
}

func mustTxn(c *cluster.Cluster, fn func(*txn.Txn) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.RunTxn(ctx, 20, fn); err != nil {
		log.Fatalf("transaction: %v", err)
	}
}

func freshRead(c *cluster.Cluster, key txn.Key) (string, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := c.Client().Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = tx.Abort(context.Background()) }()
	value, found, err := tx.Get(ctx, key)
	return string(value), found, err
}

func showBalances(c *cluster.Cluster, label string) {
	// Both keys are read inside one transaction, so they come from a single
	// snapshot. Reading them separately could sample two different instants and
	// make a correct transfer look wrong.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := c.Client().Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = tx.Abort(context.Background()) }()

	alice, _, err := tx.Get(ctx, "alice")
	if err != nil {
		log.Fatal(err)
	}
	bob, _, err := tx.Get(ctx, "bob")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: alice = %s, bob = %s\n", label, alice, bob)
}
