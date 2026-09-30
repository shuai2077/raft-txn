# 03 — The read barrier, and the lease-rollback race

This document covers two things that turned out to be one investigation:

1. **The read barrier** — implemented, with its correctness argument and the
   property it does *not* provide.
2. **A lease-rollback race** — found by that investigation, partially fixed, and
   still open. It is described here with the evidence, because it is the reason
   `TestConcurrentTransfersPreserveInvariant` still fails intermittently.

---

## Part 1 — What a read-only snapshot was missing

### 1.1 The gap

A read-only transaction takes one timestamp `S` from the oracle, then reads each
key through its own Raft round trip. Consensus gives each *group* a linearizable
history, but nothing makes the two reads line up at one global instant:

```
T1 reads A and B at snapshot S
T2 committed at c2 < S, and its write to A's group is applied
                          while its write to B's group is not
=> T1 sees A's new value and B's old value: an inconsistent snapshot
```

Both reads are individually correct, and both are at the same timestamp. The
snapshot is still not a state the system was ever in.

### 1.2 The mechanism

`CmdBarrier` is a no-op command. Appending it to a group's log and waiting for it
to be *applied* proves that every entry ahead of it has been applied, because Raft
applies entries in log order. So a transaction takes one barrier per group the
first time it reads from that group, before the read.

The ordering argument:

| Fact | Why |
| --- | --- |
| If `T2` has commit timestamp `c2 < S`, then `S` was allocated after `c2` | the oracle hands out strictly increasing timestamps |
| `T2` allocated `c2` after it sealed its primary | commit timestamps are taken after prewrite, and after the locks exist |
| `T2`'s write to group `G` was proposed after that seal | the write follows the decision |
| The read's barrier on `G` was proposed after `S`, hence after the seal | barrier comes after `S` |
| Therefore `T2`'s write to `G` precedes the barrier in `G`'s log | append order |
| Therefore `G`'s leader has applied it when the barrier returns | Raft applies in log order |

The boundary matters: visibility is strict (`CommitTS < snapshot`, see
docs/02-transaction-design.md §7.4), so `G` needs every version with commit
timestamp *below* `S`. A version at exactly `S` is invisible and correctly absent.

### 1.3 What is implemented

| Piece | Where |
| --- | --- |
| `CmdBarrier`, a no-op command | `txn/command.go` |
| Barrier application | `txn/store.go` (`Apply`) |
| `AppliedCommitTS`, the applied watermark | `txn/store.go` |
| `Participant.Barrier` | `txn/types.go`, `txn/raftadapter.go`, `txnservice/client.go` |
| `POST /txn/barrier/` | `txnservice/service.go` |
| Lazy, once-per-group barrier on first read | `txn/client.go` (`ensureReadable`) |
| `CommitIndex` on the Raft core, for diagnostics | `internal/raft/raft.go`, `server.go` |

Two implementation details worth keeping:

* **A barrier is sent as a POST with an empty body.** The transport picks GET when
  the body is nil, and the route is a POST. This cost real debugging time: the
  failure surfaced as `HTTP 405` and then as a leader-routing error, not as
  anything mentioning the method.
* **Routing failures are retried at the `Txn` layer, not the client layer.** The
  HTTP client chases leader hints itself; the in-process adapter hands the error
  back to its caller. Retrying in `ensureReadable` covers both. Re-proposing a
  barrier is always safe — it changes no state, and a later position only makes it
  a *stronger* barrier.

### 1.4 The watermark is an observability tool, not the mechanism

`AppliedCommitTS` records the highest commit timestamp a state machine has seen
applied, updated at both a seal and a published version. It is how a test or an
operator can check the guarantee instead of trusting it: "this group has applied
everything up to `X`". It is deliberately **not** what the barrier waits on, so a
broken or stale watermark cannot weaken the barrier.

### 1.5 What the per-group barrier does NOT provide

**This is the important limitation.** The argument in §1.2 only covers writes that
were already proposed to `G` when the barrier was proposed. Consider a
transaction `T2` that:

1. takes commit timestamp `c2 < S`,
2. seals its primary in group `A` (so it is decided),
3. has *not yet* proposed its write to group `B`.

A reader that barriers `B` and then reads it at `S` will miss `T2`'s write to `B`,
even though `c2 < S`. The barrier is in the log *before* that write arrives, so
apply-order proves nothing about it. The window between a transaction's commit
timestamp and the publication of all its writes is a genuine hole that a
per-group barrier cannot close, because the barrier has nothing to order against.

Measured: with the barrier implemented, `TestConcurrentTransfersPreserveInvariant`
still failed about **4 times in 25**, and adding a 300 ms quiescence window before
the audit did not change that (4 in 30). So the barrier is necessary but not
sufficient for a cross-key conservation check.

### 1.6 What closing it properly would take

The missing signal is global: "every transaction with commit timestamp below `X`
has published all of its writes." A per-group barrier cannot express it. The
standard shape is a **read barrier against the timestamp oracle**:

* the oracle tracks, per allocated timestamp, whether the transaction that owns it
  has finished; and
* a reader wanting snapshot `S` waits until everything below `S` has finished, or
  reads at a *safe point* the oracle only advances once it has.

That is a real design change — the oracle would need to learn about completions,
which means either groups reporting progress to it or coordinators reporting
completion, plus a rule for transactions that die and must be written off. It is
not a local fix and it is not implemented here.

---

## Part 2 — The lease-rollback race

### 2.1 What the investigation found

Chasing the remaining failures turned up something worse than read skew. After the
transfers finished and the cluster was quiescent, the two accounts' version chains
did not have matching commit timestamps:

```
alice=21 versions bob=20 versions; unpaired: alice-only=[2 44] bob-only=[4]
```

`2` and `4` are the two initial single-group loads, which never touch the other
group. **`44` is a real commit present in one group and absent from the other.** So
a transaction was committed in one group and never in the other — the atomicity
violation the whole design exists to prevent.

### 2.2 The mechanism

Tracing the state machine's commit path for that transaction:

```
seal    txn=3298534883359 key="0alice" ts=44     <- primary sealed: committed
publish key="0alice" ts=44 value="96"
commit-nolock txn=3298534883359 key="1bob" present=false
```

`1bob` was never sealed at ts=44 and its version was never published. The
coordinator asked group 1 to commit `1bob`, found **no lock**, and published
nothing. 96 such `commit-nolock` events appeared in one run.

So the secondary lock was **discarded** between the coordinator's prewrite and its
commit. Who discards a lock? A reader that finds an *expired* lock whose primary is
still unsealed concludes the owning transaction is dead and rolls it back:

```
lease lapses  ->  reader sees expired lock
              ->  primary still unsealed, so "owner is dead"
              ->  reader rolls the secondary lock back
coordinator   ->  seals the primary (too late)
              ->  commits a transaction whose other write is gone
```

The prewrite-to-seal window is long whenever the coordinator waits out another
transaction's lock (up to `resolveTimeout`, 2 s here) while the lease is 1 s.

### 2.3 The fix: recovery is a decision, not a guess

The root defect was that recovery **inferred** death from a lapsed lease. Three
places did that:

```
ensurePrimaryResolved:  primary still prepared, lease expired -> StatePrepared
ResolveLock:            anything that is not StateCommitted    -> "provably dead", roll back
probePrimary:           primary group has no record            -> "aborted"
```

All three treat absence of evidence as evidence of death. A lease is only a promise
that the owner will refresh the lock; it says nothing about whether the owner can
still seal the commit point, and sealing is what commits. So a reader could discard
a write whose owner was still about to commit it.

Recovery is now a **compare-and-abort recorded through Raft**, which turns "may I
discard this lock?" into a decision rather than an inference:

* the lease still gates **when** recovery may be attempted — while the lock is live
  the owner is entitled to finish and the reader waits;
* but the abort itself is a `CmdRollback` carrying `RecordDecision`, applied at the
  **primary group**, and that group re-checks the transaction's state first.

If the transaction already committed, the group returns `StatusAlreadyCommitted` and
the reader **publishes** the secondary write instead of destroying it. If it had not
committed, the group records `StateAborted` durably. Because the seal and this
decision are applied by the same Raft apply loop on the same group, exactly one of
them can win — which is the mutual exclusion a lease cannot provide.

Two supporting changes make that airtight:

* `Store.seal` refuses to seal a transaction that carries a durable abort decision
  (`StatusAlreadyAborted`). Without it, a coordinator's seal could land after the
  abort and resurrect a commit whose other half had already been discarded.
* `Txn.abortLockedGroups` aborts the **primary group first** and returns early if
  that fails, so the decision always precedes any secondary rollback. Only then may a
  lock be dropped, because the decision is what makes dropping it safe.

`Txn.verifyLocksHeld` remains as a cheap pre-commit check that refuses to seal over a
lock that is already gone, reporting `LockRolledBackError` (retryable). It is now a
fast path to a clean failure rather than the safety mechanism.

### 2.4 Why the fix is complete

There is no interleaving in which one write of a transaction is published and the
other discarded, because publishing requires the commit decision and discarding
requires the abort decision, and both are single entries applied in one order on the
primary group:

| Order of events | Outcome |
| --- | --- |
| Coordinator seals first | The reader's compare-and-abort sees `Committed`, so it publishes the secondary. Both writes land. |
| Reader aborts first | The primary group records `StateAborted`; the coordinator's seal is refused, so **neither** write lands and the transaction is retried. |
| The seal fails for a routing reason | The coordinator reports `CommitUnknownError`, `Finish` re-drives the secondaries, and any reader that trips a leftover lock consults the primary and publishes. |

### 2.5 Verification

The check that exposed the bug is kept as `TestCommitTimestampsPairUpAcrossGroups`.
Every transfer writes one key in each of two groups with a shared commit timestamp,
so after quiescence the two version chains must contain the same set of commit
timestamps. It needs no correlation between a value and a transaction, no assumption
about which node is authoritative, and no timing window — it compares the durable
state of two groups against each other. Before the fix it failed within a handful of
attempts; after it, 24 attempts pass.

Two deterministic unit tests pin the two orderings directly, using the harness clock
so no wall-clock wait is involved:

* `TestSealedSecondaryIsNotLostWhenALeaseLapses` — lapse the leases, let a reader act
  first, then commit; asserts the two groups agree about whether the write exists.
* `TestLapsedLeaseAloneCannotAbortACommittedTransaction` — seal first, then lapse the
  lease and read; asserts the reader publishes rather than discards.

### 2.6 What this did *not* fix

The cross-key snapshot gap of §1.5 is still open: a per-group barrier cannot cover a
transaction that has been decided but whose write has not yet been proposed to a
group. `TestConcurrentTransfersPreserveInvariant` audits one snapshot and can still
observe an unbalanced total for that reason. That is a *read* consistency question,
not a durability one: the writes are all present, as §2.5 establishes, but a snapshot
read is not guaranteed to be a state the system was ever in. Closing it needs the
oracle-side completion registry of §1.6.

---

## Part 3 — A note on how not to test this

Most of the time spent here went into self-checks that were wrong. Recording them
so they are not repeated:

* **"The commit succeeded, so read the value back and compare it with what was
  written."** Unsound. Concurrent transactions write the same values, so a value
  cannot be correlated with a transaction.
* **Sampling a follower immediately after a commit.** Reports ordinary replication
  lag as a lost write.
* **Sampling "the leader" once.** Leadership can move; a fresh leader may not have
  caught up.
* **Comparing a minimum-of-watermarks across groups against one group's newest
  version.** Watermarks are group-local; that comparison is meaningless.
* **Dumping version chains *after* the failure.** Later commits pollute the chain,
  so it no longer shows the state at the audit timestamp.

What actually worked: comparing **commit-timestamp sets across groups** after
quiescence (§2.1). That is what exposed the real bug, because it needs no
assumption about which node is authoritative or when a value became visible.
