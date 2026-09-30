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

### 2.3 What is fixed

`Txn.verifyLocksHeld` runs **between prewrite and the commit point** and refuses to
seal if any intent lock is gone. If a lock has been rolled back then no commit
point exists and nothing was published, so the transaction is still entirely
invisible and aborting is safe. It returns `LockRolledBackError`, which
`Cluster.isTransient` treats as retryable, so callers simply run the transaction
again.

The ordering is the point: the check has to happen *before* the seal, because
sealing later cannot repair a discarded write.

### 2.4 What is still open

**The race is narrowed, not closed.** `verifyLocksHeld` is a read, not an atomic
reservation. Between the check and the seal, a lease can still lapse and a reader
can still roll the lock back. Measured after the fix:
`TestConcurrentTransfersPreserveInvariant` failed **10 times in 30**.

The correct fix makes "may I roll this lock back?" a decision rather than a
guess. A reader can only roll a lock back safely if the owning transaction can no
longer commit, and a lease expiry does not establish that. Two shapes would:

* **A tombstone in the primary group.** Before discarding a lapsed lock, the reader
  records "transaction `X` is aborted" at `X`'s primary group. The coordinator's
  seal then fails on a transaction already declared dead. This is the Percolator
  approach and it genuinely closes the race; it costs an extra Raft write on the
  recovery path and needs the tombstone to expire eventually.
* **A completion registry at the oracle**, which is the same machinery §1.6 needs,
  so the two open problems share a solution.

Neither is implemented. Until one is, this store does **not** guarantee that a
committed transaction's writes are all present: it guarantees that they all appear
*eventually* for transactions whose coordinator stays alive and whose locks are not
reclaimed, which is weaker than the documentation elsewhere implies.

### 2.5 Why the test still fails, and why that is the honest outcome

`TestConcurrentTransfersPreserveInvariant` asserts conservation. It fails roughly a
third of the time because **the store really does violate conservation under
concurrency**, by the mechanism in §2.4. The test is not flaky in the sense of
being badly written; it is detecting a real defect that is not yet fixed. It is
left failing and enabled rather than skipped or pinned with a tolerance, so the
defect stays visible. `WaitForSettled` / `WaitForQuiescent` remain in it because
they are needed to tell "the cluster has not converged" apart from "the cluster
agrees on a wrong state", and the failure message reports that distinction.

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
