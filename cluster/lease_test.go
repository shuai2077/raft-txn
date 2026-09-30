package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/eliben/raft/txnkv/txn"
)

// rollbackSecondaryLock discards the intent lock group holds for key, which is
// what a reader does to a lapsed lock whose owner it believes is dead.
//
// It goes through the group's own state machine - via the same Rollback operation
// production recovery uses - rather than poking at the map, so the test exercises
// the real path.
func rollbackSecondaryLock(t *testing.T, c *Cluster, group int, txnID txn.TxnID, startTS txn.Timestamp, key txn.Key) {
	t.Helper()
	participant, err := c.clients.ParticipantFor(txn.GroupID(group))
	if err != nil {
		t.Fatalf("participant for group %d: %v", group, err)
	}
	err = participant.Rollback(context.Background(), txn.RollbackRequest{
		TxnID:     txnID,
		ClientID:  c.Client().ClientID(),
		RequestID: 1,
		StartTS:   startTS,
		Keys:      []txn.Key{key},
	})
	if err != nil {
		t.Fatalf("rolling back the lock on %q: %v", key, err)
	}
}

// TestCommitRefusesToSealOverARolledBackLock pins the lease-rollback fix.
//
// The scenario is the one that produced a commit present in one group and absent
// from the other: the coordinator prepares, its secondary lock is discarded by a
// reader (because the lease lapsed while the primary was still unsealed), and only
// then does the coordinator try to decide. Sealing at that point would commit a
// transaction whose other write no longer exists.
//
// The assertion is that the commit is refused and the transaction stays entirely
// invisible, which is what makes retrying it safe.
func TestCommitRefusesToSealOverARolledBackLock(t *testing.T) {
	c := NewHarness(t, TestConfig(2, PrefixPartitioner{Groups: 2}))
	c.PutT(t, "0alice", "100")
	c.PutT(t, "1bob", "100")

	tx, err := c.Client().Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.Put("0alice", "90"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("1bob", "110"); err != nil {
		t.Fatal(err)
	}

	// Prepare both groups: both locks exist, nothing is decided.
	c.PrewriteTxn(t, tx)

	secondary := c.GroupNode(1, c.LeaderOf(1)).Store()
	if lock := secondary.LockFor("1bob"); lock == nil {
		t.Fatal("the secondary lock was not installed by prewrite")
	}
	rollbackSecondaryLock(t, c, 1, tx.ID(), tx.StartTS(), "1bob")
	if lock := secondary.LockFor("1bob"); lock != nil {
		t.Fatal("the secondary lock is still present after the rollback")
	}

	// The coordinator must refuse to commit rather than sealing over the gap.
	//
	// Which guard fires depends on ordering: the group that was rolled back now
	// records the transaction as aborted, so a further prewrite there is rejected
	// before verifyLocksHeld even runs. Both are refusals, and the property that
	// matters is identical - nothing was published - so that is asserted directly
	// below instead of pinning one error type.
	if _, err := tx.Commit(context.Background()); err == nil {
		t.Fatal("commit succeeded although its secondary lock was gone")
	} else {
		var rolledBack *txn.LockRolledBackError
		if !errors.As(err, &rolledBack) {
			t.Logf("commit refused by an earlier guard: %v", err)
		} else if rolledBack.Key != "1bob" {
			t.Errorf("the error names key %q; want %q", rolledBack.Key, "1bob")
		}
	}

	// Nothing may have been published, or the transaction would be half-visible.
	for _, key := range []txn.Key{"0alice", "1bob"} {
		group := int(c.cfg.Partitioner.GroupForKey(key))
		store := c.GroupNode(group, c.LeaderOf(group)).Store()
		for _, v := range store.VersionsFor(key) {
			if v.StartTS == tx.StartTS() {
				t.Errorf("key %q has a version from the refused transaction at ts %d", key, v.CommitTS)
			}
		}
	}
	for _, want := range []struct {
		key   txn.Key
		value txn.Value
	}{{"0alice", "100"}, {"1bob", "100"}} {
		value, found, err := c.Read(want.key)
		if err != nil {
			t.Fatalf("read %q: %v", want.key, err)
		}
		if !found || value != want.value {
			t.Errorf("key %q = (%q, found=%v); want %q: the refused transaction leaked",
				want.key, value, found, want.value)
		}
	}

	// A fresh attempt must succeed, which is the retry a caller performs after a
	// LockRolledBackError.
	if _, err := c.Put("0alice", "80"); err != nil {
		t.Fatalf("retry after the refusal: %v", err)
	}
	c.MustGetEventually(t, "0alice", "80", 10*time.Second)
}

// TestLeaseRollbackCannotLoseAWrite checks conservation across a chain of
// transfers when the lock TTL is short enough that leases lapse mid-transaction.
//
// It is deliberately serialized. The concurrent form of this check is broken by the
// still-open race documented in docs/03-read-barrier.md §2.4, and a test for that
// already exists; this one isolates the lease question from the concurrency one.
func TestLeaseRollbackCannotLoseAWrite(t *testing.T) {
	c := NewHarness(t, TestConfig(2, PrefixPartitioner{Groups: 2}))
	c.PutT(t, "0alice", "100")
	c.PutT(t, "1bob", "100")

	transfer := func(from, to txn.Key) error {
		_, err := c.RunTxn(context.Background(), 20, func(tx *txn.Txn) error {
			fromVal, found, err := tx.Get(context.Background(), from)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("account %q missing", from)
			}
			fromNum := mustParse(t, string(fromVal))
			if fromNum < 1 {
				return nil
			}
			if err := tx.Put(from, txn.Value(fmt.Sprint(fromNum-1))); err != nil {
				return err
			}
			toVal, _, err := tx.Get(context.Background(), to)
			if err != nil {
				return err
			}
			toNum := mustParse(t, string(toVal))
			return tx.Put(to, txn.Value(fmt.Sprint(toNum+1)))
		})
		return err
	}

	for i := 0; i < 12; i++ {
		from, to := txn.Key("0alice"), txn.Key("1bob")
		if i%2 == 1 {
			from, to = "1bob", "0alice"
		}
		if err := transfer(from, to); err != nil {
			t.Fatalf("transfer %d: %v", i, err)
		}
		// Conservation must hold after every transfer, so a lost write is caught on
		// the transfer that caused it rather than at the end.
		alice, foundA, errA := c.Read("0alice")
		bob, foundB, errB := c.Read("1bob")
		if errA != nil || errB != nil || !foundA || !foundB {
			t.Fatalf("transfer %d: reading the accounts: %v / %v", i, errA, errB)
		}
		a := mustParse(t, string(alice))
		b := mustParse(t, string(bob))
		if a+b != 200 {
			t.Fatalf("transfer %d: alice=%d bob=%d total=%d; want 200", i, a, b, a+b)
		}
	}
}
