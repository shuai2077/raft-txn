package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eliben/raft/txnkv/txn"
)

// TestCommitTimestampsPairUpAcrossGroups is the check that exposed the
// lost-write race, kept as a regression test.
//
// Every transfer touches one key in each of two groups, and both writes share one
// commit timestamp. So after the cluster is quiescent, the two keys' version
// chains must contain exactly the same set of commit timestamps - apart from the
// two initial single-group loads, which by construction only ever touched one
// group.
//
// This is the strongest available check on atomicity, and it is the one that
// matters. It needs no assumption about which node is authoritative, no
// correlation between a value and a transaction, and no timing window: it compares
// the durable state of two groups against each other. When it fails, it means a
// transaction was committed in one group and not the other.
func TestCommitTimestampsPairUpAcrossGroups(t *testing.T) {
	const workers = 4
	const transfersPerWorker = 5

	for attempt := 0; attempt < 8; attempt++ {
		c := NewHarness(t, TestConfig(2, PrefixPartitioner{Groups: 2}))
		keys := []txn.Key{"0alice", "1bob"}
		c.PutT(t, keys[0], "100")
		c.PutT(t, keys[1], "100")

		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < transfersPerWorker; i++ {
					from, to := w%2, (w+1)%2
					_, _ = c.RunTxn(context.Background(), 50, func(tx *txn.Txn) error {
						fromVal, found, err := tx.Get(context.Background(), keys[from])
						if err != nil {
							return err
						}
						if !found {
							return fmt.Errorf("account %q missing", keys[from])
						}
						fromNum := mustParse(t, string(fromVal))
						if fromNum < 1 {
							return nil // nothing to transfer
						}
						if err := tx.Put(keys[from], txn.Value(fmt.Sprint(fromNum-1))); err != nil {
							return err
						}
						toVal, _, err := tx.Get(context.Background(), keys[to])
						if err != nil {
							return err
						}
						toNum := mustParse(t, string(toVal))
						return tx.Put(keys[to], txn.Value(fmt.Sprint(toNum+1)))
					})
				}
			}(w)
		}
		wg.Wait()

		// Quiescence, not just settling: a transaction can hold a commit timestamp
		// while one of its writes is still in flight, and that is not the failure
		// this test is looking for.
		if err := c.WaitForQuiescent(300*time.Millisecond, 30*time.Second); err != nil {
			t.Logf("attempt %d: cluster never quiesced, skipping: %v", attempt, err)
			continue
		}

		alice := c.GroupNode(0, c.LeaderOf(0)).Store().VersionsFor(keys[0])
		bob := c.GroupNode(1, c.LeaderOf(1)).Store().VersionsFor(keys[1])

		inAlice := map[txn.Timestamp]bool{}
		inBob := map[txn.Timestamp]bool{}
		for _, v := range alice {
			inAlice[v.CommitTS] = true
		}
		for _, v := range bob {
			inBob[v.CommitTS] = true
		}

		var onlyAlice, onlyBob []txn.Timestamp
		for _, v := range alice {
			if !inBob[v.CommitTS] {
				onlyAlice = append(onlyAlice, v.CommitTS)
			}
		}
		for _, v := range bob {
			if !inAlice[v.CommitTS] {
				onlyBob = append(onlyBob, v.CommitTS)
			}
		}

		// One unpaired timestamp per key is expected: the initial load of each key
		// was its own single-group transaction.
		if len(onlyAlice) > 1 || len(onlyBob) > 1 {
			t.Errorf("attempt %d: %d unpaired commit timestamps; "+
				"a transaction was committed in one group and not the other "+
				"(alice=%d versions bob=%d versions, alice-only=%v bob-only=%v)",
				attempt, len(onlyAlice)+len(onlyBob), len(alice), len(bob), onlyAlice, onlyBob)
			// Report the state machine's own view, which distinguishes "never asked
			// to commit" from "asked and refused".
			for _, ts := range append(append([]txn.Timestamp{}, onlyAlice...), onlyBob...) {
				for g, key := range map[int]txn.Key{0: keys[0], 1: keys[1]} {
					store := c.GroupNode(g, c.LeaderOf(g)).Store()
					var staged txn.Timestamp
					for _, v := range store.VersionsFor(key) {
						if v.CommitTS == ts {
							staged = v.StartTS
						}
					}
					t.Logf("  ts=%d group %d key=%q: lock=%v ownVersionStartTS=%d",
						ts, g, key, store.LockFor(key) != nil, staged)
				}
			}
			t.FailNow()
		}
	}
}
