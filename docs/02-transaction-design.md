# Design: ACID transactions on a Multi-Raft store

This document explains what was fused into `eliben/raft` and, more importantly,
*why* each piece is shaped the way it is. It is written to be read alongside the
code: every section names the file that implements it.

---

## 1. The problem

Upstream's Raft gives you atomicity for exactly one command. A `Command` is
applied by one group's state machine, at one point in that group's log, and
nothing else happens as part of it. That is enough for `PUT`, and it is enough
for `CAS` *on a single key*.

It is not enough for:

* **several keys in one unit.** Two `PUT`s must both land or neither. Two
  separate Raft submissions can be separated by a crash, and then one landed.
* **several keys in different groups.** Once the key space is sharded — which is
  the only way to scale a Raft log — a single Raft group cannot even express the
  operation.

So the work is to build the thing that sits *above* a federation of Raft groups
and makes a set of writes across them atomic, isolated, and durable. That is a
distributed transaction protocol, and the shape the industry converged on is
Google's Percolator (as used by TiKV, and by CockroachDB in its own variant).

## 2. Resulting architecture

```
                    ┌──────────────────────────────────────────┐
   application ───► │  txn.Client  (coordinator)               │
                    │  Begin/Get/Put/Delete/Commit/Abort        │
                    └───────┬───────────────────────┬──────────┘
                            │                       │
              two-phase commit across groups    timestamps
                            │                       │
        ┌───────────────────┴───────┐      ┌────────┴─────────┐
        │ txn.Participant per group │      │  txn.Oracle      │
        └───────────┬───────────────┘      └────────┬─────────┘
                    │ HTTP                          │ HTTP
   ┌────────────────┴───────────────┐      ┌────────┴──────────┐
   │ txnservice.Node                │      │ tso.Service       │
   │  ├─ HTTP handlers              │      │  ├─ HTTP handlers │
   │  ├─ txn.RaftLog (adapter)      │      │  ├─ tso.Oracle    │
   │  └─ txn.Store (state machine)  │      │  └─ (state machine)│
   │  └─ raft.Server ── RPC ──► peer│      │  └─ raft.Server   │
   └────────────────────────────────┘      └───────────────────┘
              group 0 .. group N-1              oracle group
```

Read bottom-up:

| Package | Layer | Responsibility |
|---|---|---|
| `internal/raft` | consensus | Unmodified upstream Raft plus the hooks in §9 |
| `txn` | transaction | The whole protocol: MVCC state machine, coordinator, lock resolution |
| `txnservice` | transport | One Raft group exposed over HTTP as a `txn.Participant` |
| `tso` | clock | A Raft-replicated timestamp oracle (`txn.Oracle`) |
| `cluster` | wiring | Builds a Multi-Raft cluster and breaks it on purpose |

The dependency rule is strict: `txn` knows nothing about HTTP, Raft, or groups'
addresses. It sees `Participant` (an interface), `Oracle` (an interface), and
`Partitioner` (a pure function). Every other package exists to supply those.

## 3. The isolation model

**Snapshot isolation, implemented as MVCC + write-write conflict detection.**

Every key holds a chain of committed versions, each stamped with the timestamp at
which its writer committed. A reader at snapshot `T` walks the chain and takes
the newest version with `CommitTS <= T` — that is the whole read path.

```
key "a":   commit@10 "v1"     commit@25 "v2"     commit@40 delete
               ▲                  ▲                  ▲
          read at T=15       read at T=30       read at T=45
           sees "v1"          sees "v2"          sees nothing
```

A delete is a *version*, not an erasure, so an older snapshot still sees the
value. This is what makes a long-running transaction's reads repeatable.

Isolation comes from one rule, enforced in the state machine's `prewrite`
handler (an `OpDelete` is a write like any other):

> Reject a write to `k` if `k` has a committed version with
> `CommitTS > startTS`.

Because it is evaluated inside the same Raft entry that installs the lock, there
is no race between checking and locking. The transaction that loses gets
`StatusWriteConflict` and is rolled back whole; a retry loop re-reads and
retries with a new snapshot.

Why not serializable? Because snapshot isolation is what this class of system
actually provides cheaply, and because the interesting engineering is in making
*that* correct across shards. §7.4 records what would be needed.

## 4. The commit protocol

Two-phase commit, Percolator-style, where one key's lock is the commit point.
Steps, with the file that implements each:

| # | Step | Where |
|---|---|---|
| 1 | `startTS = oracle.ReadTS()` | `txn.Client.Begin` |
| 2 | reads at `startTS`, writes buffered in memory | `txn.Txn.Get/Put/Delete` |
| 3 | prepare: install intent locks in every owning group | `prewriteWrites`, `Store.prewrite` |
| 4 | `commitTS = oracle.CommitTS(startTS)`, after preparing | `txn.Txn.Commit` |
| 5 | **primary**: write `commitTS` into the primary lock, then publish its write | `commitPrimary`, `Store.seal` + `Store.commit` |
| 6 | secondary groups: publish their writes | `commitSecondary` |

Three choices carry the correctness argument.

### 4.1 The primary key and why the timestamp is fetched *after* prewrite

The primary key is chosen deterministically as the smallest key in the write set
(`primaryKeyOf`), so any coordinator or resolver arrives at the same answer
without coordination.

Fetching `commitTS` **after** all locks are in place is what makes the "push"
rule sound: any other transaction that observed our lock at `startTS` must commit
at a timestamp greater than the `commitTS` we are about to receive. Fetch it
first and that guarantee evaporates.

### 4.2 `seal` — the atomic commit point

Installing a lock does not decide anything: the lock carries the *pending* value,
and reads still see the previous version. The transaction becomes committed at
one specific write:

```
CmdSeal{Key: primaryKey, TxnID, StartTS, CommitTS}
```

Sealing the primary lock records the decision and changes the group's
transaction state to committed, as a **single Raft log entry**. Every replica
either has it or does not — there is no intermediate state to observe. From that
entry onward:

* the transaction is committed, durably, even if the coordinator never runs
  again;
* any other member, and any other *group*, that finds one of the transaction's
  secondary locks can discover the decision and finish the work.

That last point is why the commit-time failure path in `Txn.Commit` returns a
`CommitUnknownError` while still reporting success: the transaction really is
committed, and the only thing that failed was the coordinator learning the
answer. Rolling back at that point would be the bug.

### 4.3 What makes a crashed coordinator safe

The coordinator is not part of the correctness argument — it is a client that
happens to be well-informed. Anything it fails to finish is finished by the next
reader that touches one of the locks, via the resolution procedure in §5.

## 5. Lock resolution: the safety-critical part

A reader that finds a foreign intent lock must decide whether the lock's
transaction committed. Getting this wrong in one direction loses an
acknowledged write; in the other, it resurrects a value that another reader
already observed.

The rules, implemented in `txn.Client.resolveLock` and `txn.DefaultResolver`:

| Observation about the lock | Action |
|---|---|
| the lock record itself carries a `CommitTS` | the owner committed: **publish** this write |
| the primary's transaction state is `committed` | committed: **publish** |
| the primary's transaction state is `aborted` | dead: **discard** |
| the primary group has no record of the transaction | rolled back and forgotten: **discard** |
| primary is still `prepared`, lease live | owner may still be committing: **wait**, then re-probe |
| primary is still `prepared`, lease expired | coordinator is gone: **discard** the primary and this lock |

The invariant that enforces the safety property:

> A reader never discards a lock without first consulting the transaction's
> **primary** lock, whose group is the only authority on the transaction's fate.

Note what the resolver does *not* do: it does not decide based on the age of the
lock it tripped over. That lock's age says nothing about whether its transaction
committed, because the decision lives in another group.

### 5.1 Why the state machine tracks transaction state, not just `CommitTS`

A secondary lock is never sealed — only the primary is. So a reader that finds a
secondary lock cannot read the decision out of it. It has to ask the primary
group, and what it asks for is the transaction's *state*:

```
LockInfo{ Key, TxnID, Primary, StartTS, CommitTS, ActorCommitTS, State, ExpiresAt }
```

`State` is the field that makes resolution decidable.
`ActorCommitTS` is how the resolver learns the timestamp to finish a secondary
write (`finishLock` passes it through, `Store.commit` publishes with it).

## 6. The timestamp oracle (`tso`)

Transactions need one global property that a local counter cannot give:
timestamps never repeat and never move backwards, cluster-wide, across leader
changes.

The oracle is therefore a replicated state machine like everything else, with a
windowed allocation:

```
   0            8                       16
   |------------|------------------------|
   spoken for   leader 2's window        leader 3's window
```

* `allocated` — a global high-water mark: every timestamp at or below it is
  spoken for. It grows only through a replicated `CmdAllocate`.
* each allocation names the **member** it reserves for. A node serves only out of
  its own reservation, and spends it from memory, so handing out a timestamp
  costs no Raft round trip.

The failover argument is the reason for the per-member reservation. A new leader
replays the log and finds `allocated = 16` and, separately, that the previous
leader held the window `(8, 16]`. It cannot know how much of that window was
served, so it must not serve any of it. It reserves `(16, 24]` for itself and
starts at 17. Timestamps 9..16 are **wasted** — that is the deliberate trade — and
none is ever repeated. `TestNewLeaderCannotSpendPredecessorsWindow` pins this
down directly.

## 7. Design decisions and rejected alternatives

### 7.1 How the coordinator gets answers out of Raft

Upstream's model is asynchronous: `Submit` returns an index, the answer arrives
later on a shared `commitChan`, and the application correlates the two (Part 4's
commit subscriptions). A transaction coordinator cannot use that, because a
transaction is not one command — it is a *conversation*, with each step's inputs
depending on the previous step's output. Rebuilding index correlation for every
step would duplicate machinery for no benefit.

So `Server.Apply` was added: propose, wait for the entry to commit, return the
state machine's answer. The implications are spelled out in the code:

* the state machine is applied **only** from the commit path, so every replica's
  state is a function of its log;
* the state machine runs **outside** the consensus lock, so an application may
  take its own locks without stalling elections or deadlocking shutdown;
* `Apply` has a timeout, because a leader without a quorum can never commit and
  hanging the caller would be worse than telling it the outcome is unknown.

### 7.2 Locking, not optimistic concurrency

The alternative — validate a read set at commit time — cannot work across groups
here, because it would need every group's version history to be checked atomically
against every other's. Locks put the decision where the data is: one group, one
Raft entry, one atomic state transition.

### 7.3 Leases as the liveness mechanism

There is no distributed deadlock detector, and no coordinator heartbeat. A lock
simply carries `ExpiresAt`, and an expired lock whose primary is undecided may be
discarded by anyone. A crashed coordinator therefore costs *latency* bounded by
the lease — never correctness, and never permanent unavailability. The
alternative (a membership-based liveness service) would be a second consensus
system to reason about.

### 7.4 Visibility is strict, and the read and conflict rules must be complements

Visibility is `CommitTS < snapshot`; conflict detection is `CommitTS >= snapshot`.
The two **must** be exact complements. This is not a style choice.

The original implementation used `<=` and `>`. Those two rules disagree in a
one-timestamp-wide band: a version committed at exactly the snapshot timestamp is
visible to reads yet does **not** block writes. A transaction can therefore read a
value, be raced by a concurrent writer committing at that same timestamp, and then
overwrite a commit it had already observed — the classic lost update, with both
transactions reporting success.

`TestStoreNoLostUpdateAtTheSnapshotBoundary` pins this down directly.

Strict visibility has two consequences worth stating, because they are confusing
otherwise:

* **Does a transaction need a read-back to see its own write?** No. Its own writes
  are buffered locally and `Txn.Get` consults the buffer first.
* **Not seeing a value at the boundary is correct.** A commit at ts `C` is
  invisible at snapshot `C` and visible at `C+1`. Assertions of the form "visible
  immediately after commit" must therefore read at `C+1`.

### 7.5 Cross-key snapshot reads, and the read barrier

A read-only transaction reads each key through its own Raft round trip, all at one
read timestamp. A read timestamp does not by itself guarantee that every
transaction committed below it has already become *visible* in every group:

```
T1 reads A and B at snapshot S
T2 committed at c2 < S, and its write to A's group is applied
                         while its write to B's group is not
=> T1 sees A's new value and B's old value: an inconsistent snapshot
```

**A read barrier is implemented** (docs/03-read-barrier.md). Each transaction takes
one no-op log entry per group before its first read from that group; because Raft
applies entries in log order, waiting for that entry to be applied proves the group
has applied everything already ahead of it in its log. The argument is in the
design document, along with the property it does *not* give: a per-group barrier
cannot cover a transaction that has been decided but whose write has not yet been
proposed to the group. Closing that needs a barrier against the timestamp oracle —
a registry of which allocated timestamps have finished — and that is not
implemented.

### 7.6 The lease-rollback race (open, and it breaks atomicity)

Chasing the remaining conservation failures turned up something more serious than
read skew, and it is recorded in full in docs/03-read-barrier.md §2. In short:

* A lock lease can lapse while the coordinator is still working (it may be waiting
  out another transaction's lock, which can take up to `resolveTimeout`).
* A reader that finds a lapsed lock whose primary is still unsealed concludes the
  owner is dead and rolls it back.
* The coordinator then seals the primary and commits a transaction whose other
  write has already been discarded.

Evidence: after a quiescent run, one account's version chain contained commit
timestamp 44 and the other's did not — a commit present in one group and absent
from the other.

`Txn.verifyLocksHeld` now checks that every intent lock is still held **before** the
commit point is written, and aborts with `LockRolledBackError` (retryable) if any
lock is gone. That is correct and removes the common case, but it narrows the race
rather than closing it: the check is a read, not a reservation, so a lease can still
lapse between the check and the seal. Closing it properly needs a tombstone in the
primary group (the Percolator approach), or the same oracle-side completion
registry §7.5 needs.

**Consequence, stated plainly:** until that is fixed, this store does not guarantee
that all writes of a committed transaction are present. It guarantees they appear
eventually for transactions whose coordinator stays alive and whose locks are not
reclaimed. `TestConcurrentTransfersPreserveInvariant` fails intermittently for this
reason and is deliberately left enabled rather than skipped, so the defect stays
visible.

### 7.7 Not serializable, deliberately

Snapshot isolation permits write skew: two transactions reading overlapping sets
and writing disjoint keys can both commit, producing a state no serial order
explains. Preventing it needs either read locks (which reintroduce deadlock, and so
need deadlock detection) or a serializable validation phase. That is a real piece
of work, it is separable (it would add a read-set check to `prewrite`), and
pretending to have it by labelling SI as serializable would be worse than not
having it.

### 7.8 The single-group fast path

When every key of a transaction lives in one group, `CmdPrewrite` can commit the
writes in the *same* log entry that creates the locks (`OneShot`). That is sound
precisely because the entire write set is in one atomic unit, and it halves the
Raft round trips. It is opt-in and off by default, because the interesting
protocol is the general one and the fast path should not be what the tests
exercise by default.

## 8. Correctness argument, summarised

**Atomicity.** The primary seal is a single Raft entry, so it is observed by all
or none. Before it, no version exists; after it, every lock can be resolved
toward commit by any member. A reader can never see a partial transaction,
because partial states exist only as *locks*, and locks are never read through.

**Isolation.** Reads take the newest version at or before their snapshot. Writes
are refused if a newer version exists. Both are decided inside a single Raft
entry on the key's own group, so the check-and-act is atomic with respect to
every other write to that key.

**Durability.** Every state transition — lock installation, the seal, publishing
a version — is a Raft entry, hence fsynced and majority-replicated before the
client is answered. Recovery is log replay, and the state machine is
deterministic, so replay reproduces the state exactly.

**Liveness.** A crashed coordinator's locks lapse with their lease and are
reclaimed by the next reader. A group without a quorum cannot make progress, but
neither can it corrupt anything: proposals fail and callers retry.

**The one thing that is not protected:** a coordinator can die *after* the
primary seal and before finishing the secondaries, leaving locks whose
transaction is committed. Readers resolve them, so the writes become visible —
correct, but delayed. That is the visible cost of the design, and §4.3 explains
why it is the *right* trade.

## 9. What was changed in the Raft core

`internal/raft` is upstream `part3/raft` plus the following. Each is marked in
the source as a local extension; see `docs/01-raft-analysis.md` §5 for the full
rationale.

| Change | Purpose |
|---|---|
| `StateMachineHandler` / `Server.SetHandler` | Let a group *be* a state machine |
| `Server.Apply` | Synchronous propose-and-answer |
| State machine applied from the commit path, outside the lock | Ordering + a working shutdown |
| `leaderId` tracking, `Server.LeaderID` | One-hop leader discovery |
| Connections tracked and closed on shutdown; accept loop woken from `Stop` | Shutdown that actually terminates |
| `stopChan` consulted while delivering commits | A non-reading client cannot wedge shutdown |

Everything else — the algorithm, the locking discipline, the persistence model,
the RPC surface, and the whole upstream test suite — is untouched. That
restraint is deliberate: the point is to show what a transaction layer needs from
consensus, not to rewrite consensus.

## 10. Test strategy

Two independent layers, because the two classes of bug are different:

**Protocol tests (`txn/`)** run the state machine and coordinator in-process,
with a scriptable oracle and an injectable clock. They cover things that are hard
to produce on purpose in a real cluster: a specific version chain, a specific
`LockedError`, an exactly-once retry, a commit timestamp that arrives out of
order. The `directLog` harness applies commands straight to the store, which is
exactly what a Raft leader does minus replication.

**System tests (`cluster/`)** build a real Multi-Raft cluster over HTTP and break
it: partition a group, isolate a minority, crash a leader, restart a whole
cluster from its storage, fail the oracle over, and abandon a coordinator
mid-commit. They assert properties a user would care about — cross-group
atomicity, repeatable reads, conservation of an invariant under concurrent
transfers — plus the internal one that matters most: **replicas agree**.

Two test-design lessons are recorded in the code because getting them wrong cost
real debugging time:

1. **Audit an invariant at one snapshot.** Reading each account with its own
   transaction samples two instants; a transfer committing in between makes the
   sum look wrong while every transaction was correct. The store was fine; the
   assertion was not.
2. **Model the lease, do not manipulate the clock.** An earlier version of the
   recovery tests moved a fake clock to expire locks instantly. It was fragile —
   it depended on exactly when a lock happened to be stamped — and flaky in a way
   that hid the real question. The tests now use a short real lease and retry,
   which is what a real application does.

The upstream Raft suite is run as-is. `TestDisconnectAfterSubmit` and
`TestBug_BecomeFollowerMissingPersist` are timing-sensitive and flake at a
comparable rate against *unmodified* upstream on this machine; that is an
inherited property, not a regression, and it was measured rather than assumed.

## 11. Not implemented

Named explicitly, because a design document that omits its limits is not useful:

| Gap | What it would take |
|---|---|
| **Serializability** | A read-set validation phase in `prewrite`, or read locks plus deadlock detection |
| **Log compaction / snapshots** | The upstream core has none, and a long-lived transaction store needs it more than a KV store does |
| **Dynamic membership** | Upstream has none; resharding needs it |
| **Replicated per-group state machines** | `txn.Store` is replayed from the group's log on restart. A snapshot-based approach would need serializing the version chains and lock table |
| **Garbage collection of old versions** | Chains grow forever; real systems drop versions below the oldest live snapshot |
| **Batched/parallel group RPCs** | Prewrite and commit currently walk groups sequentially |
| **Read leases / read index** | Every read is a Raft round trip today; upstream's §8 optimisations apply |
| **Cross-group deadlock avoidance** | Not needed *only* because prewrite is no-wait. Adding read locks would make it necessary |

## 12. References

* [Percolator](https://research.google.com/pubs/pub36726.html) — Peng & Dabek,
  *Large-scale Incremental Processing Using Distributed Transactions and
  Notifications* (OSDI '10). The primary key, the lock table, and lazy
  resolution all come from here.
* [TiKV transaction model](https://tikv.org/deep-dive/distributed-transaction/percolator/)
  — the same protocol in production, including the `MinCommitTS` push rule.
* [Raft paper](https://raft.github.io/raft.pdf) — §5.4 (safety), §8 (client
  interaction and linearizable reads).
* The upstream blog series — see `docs/01-raft-analysis.md` §7.
