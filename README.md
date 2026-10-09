# txnkv — ACID transactions on a Multi-Raft store

A distributed, transactional key/value store built on top of
[`eliben/raft`](https://github.com/eliben/raft), the instructional Raft
implementation in Go.

Upstream Raft makes **one command** atomic. This repository makes **a set of
writes across several Raft groups** atomic, isolated, and durable — which is what
it takes to build a database rather than a replicated log.

> 中文版：[README.zh-CN.md](README.zh-CN.md)

```
   N transaction groups (shards)              1 replicated timestamp oracle
   ┌─────────┐ ┌─────────┐ ┌─────────┐        ┌─────────┐
   │ group 0 │ │ group 1 │ │ group 2 │        │   tso   │
   │ Raft ×3 │ │ Raft ×3 │ │ Raft ×3 │        │ Raft ×3 │
   │ + MVCC  │ │ + MVCC  │ │ + MVCC  │        │ + clock │
   └────┬────┘ └────┬────┘ └────┬────┘        └────┬────┘
        └───────────┴───────────┘                  │
                    │                              │
              two-phase commit                  timestamps
                    │                              │
            ┌───────┴──────────────────────────────┴───────┐
            │           transaction coordinator            │
            │      Begin / Get / Put / Delete / Commit     │
            └──────────────────────────────────────────────┘
```

## What it does

```go
tx, _ := store.Begin(ctx)
defer tx.Abort(ctx)

balance, found, err := tx.Get(ctx, "user:alice")   // snapshot read
tx.Put(ctx, "user:alice", newBalance)               // buffered, not applied
tx.Put(ctx, "user:bob", otherBalance)               // possibly another group
commitTS, err := tx.Commit(ctx)                     // atomic across all keys
```

* **Atomic** — a commit spanning several shards either happens everywhere or
  nowhere. There is no state in which a reader can see half of it.
* **Isolated** — every transaction reads a snapshot at one timestamp, and is
  refused if another transaction committed a key it wants to write. (Snapshot
  isolation; see the design doc for exactly what that does and does not give.)
* **Durable** — every state change is a Raft log entry, fsynced and
  majority-replicated before the caller is answered.
* **Crash-tolerant** — a coordinator that dies mid-commit does not lose data and
  does not block its keys forever. Any other client finishes its work.
* **Sharded** — keys are partitioned across independent Raft groups, each of
  which is an ordinary Raft cluster.

## Quick start

Requirements: Go 1.23+.

```bash
go build ./...
go test ./...                     # the full suite, ~2 minutes
go test ./txn/                    # the protocol, in-process, fast
go test ./cluster/                # end-to-end against a real cluster
```

`internal/raft` is upstream's own test suite and takes the longest.

> **On flaky tests.** Two tests in that upstream suite —
> `TestDisconnectAfterSubmit` and `TestBug_BecomeFollowerMissingPersist` — are
> timing-sensitive and occasionally fail under load, along with `TestCrashAfterSubmit`
> on rare occasions. This is **inherited, not introduced**, and that was verified
> rather than assumed: running the same package in a worktree of the commit *before*
> the changes to `internal/raft` failed 3 times in 15 (20%), and the current tree
> failed 2 in 10 (20%), with the same tests failing both times. Both pass reliably
> when run together with nothing else on the machine.
>
> Three other causes of flakiness found by stress testing — port collisions between
> test clusters, two goroutines outliving `Shutdown`, and test wait budgets too tight
> for a loaded machine — are fixed; see the commit history for the details. Repeated
> full-suite stress runs now fail only on the inherited tests above.

To watch the mechanism rather than the tests:

```bash
go run ./cmd/demo
```

## Layout

| Path | What lives there |
|---|---|
| `internal/raft/` | Raft consensus — upstream `eliben/raft` `part3/raft`, vendored |
| `txn/` | The transaction protocol: MVCC state machine, intent locks, 2PC coordinator, lock resolution |
| `txnservice/` | One Raft group as an HTTP service, and the client that reaches it |
| `tso/` | A Raft-replicated timestamp oracle |
| `cluster/` | Wiring for a whole Multi-Raft cluster, plus fault injection |
| `docs/` | [Raft analysis](docs/01-raft-analysis.md) · [transaction design](docs/02-transaction-design.md) · [read barrier](docs/03-read-barrier.md) · [UML class diagrams](docs/04-uml-class-diagram.md) · [workflow analysis](docs/05-workflow-analysis.md) |
| `tools/` | Small helper scripts (`doc-check` validates the diagrams in `docs/` and the structure of the translations) |
| `cmd/demo/` | A runnable demonstration |
| `README.zh-CN.md` | This README in Chinese |

Every document under `docs/` also has a Chinese translation, named after the
original with a `.zh-CN.md` suffix — for example
[`docs/02-transaction-design.zh-CN.md`](docs/02-transaction-design.zh-CN.md).

## How it works, in one page

**Sharding.** A `Partitioner` maps a key to a group. Each group runs the same
Raft consensus module as a replicated state machine holding one shard of the key
space.

**Versioning.** A key does not hold a value; it holds a chain of versions, each
stamped with the timestamp at which its writer committed. A read at snapshot `T`
takes the newest version at or before `T`. A delete is a version too, so older
snapshots still see the old value.

**Two-phase commit.** A transaction:

1. takes a snapshot timestamp from the oracle;
2. reads at that snapshot, buffering its writes in memory;
3. **prepares**: installs an *intent lock* — carrying the pending value — on
   every key it writes, in every group that owns one. Nothing is visible yet.
4. takes a commit timestamp from the oracle;
5. **seals** the lock on one designated key, the *primary*: a single Raft entry
   that decides the transaction for good;
6. publishes the writes in the remaining groups.

Between steps 3 and 5 the transaction can still be rolled back, so failing there
is safe. From step 5 on it is committed, and nothing can undo it.

**The commit point is one Raft entry.** That is the whole design. It means any
member of any group — not just the coordinator — can determine what happened to a
transaction by consulting its primary lock, and can finish the transaction's
remaining work. Coordination is therefore an optimisation, not a dependency.

**Recovery.** A reader that trips over a lock asks the primary's group what the
transaction decided, then either publishes the write (committed) or discards the
lock (dead). It never discards a lock belonging to a committed transaction,
because that would lose an acknowledged write.

**Timestamps.** The oracle is itself a Raft group, so timestamps survive leader
changes. It hands them out in windows: one Raft round trip reserves many
timestamps, which are then served from memory. A new leader cannot spend its
predecessor's window — it reserves a fresh one above everything spoken for — so
timestamps may be wasted but are never repeated.

## Design documentation

**[docs/README.md](docs/README.md)** is the index — it lists the five documents with
their Chinese versions and a suggested reading order. The documents themselves:

* **[docs/01-raft-analysis.md](docs/01-raft-analysis.md)** — a detailed analysis
  of the upstream `eliben/raft` project: its architecture, the election and
  replication mechanisms, the two Part-3 optimisations, the KV service's commit
  subscriptions, exactly-once delivery, and what the design deliberately leaves
  out. Also lists every change made to the Raft core here, and why.
* **[docs/02-transaction-design.md](docs/02-transaction-design.md)** — the
  transaction layer: the isolation model, the commit protocol, the lock
  resolution rules and the safety argument behind them, the timestamp oracle, and
  a section on design decisions that were considered and rejected. Ends with an
  explicit list of what is *not* implemented.
* **[docs/03-read-barrier.md](docs/03-read-barrier.md)** — the read barrier: what
  a cross-key snapshot read was missing, the log-position argument that closes it,
  the property it deliberately does *not* provide, and the lease-rollback race that
  was found while investigating it — including the fix, which makes recovery a
  compare-and-abort recorded through Raft rather than an inference from a lapsed
  lease.
* **[docs/04-uml-class-diagram.md](docs/04-uml-class-diagram.md)** — the structural
  view: layer dependencies, UML class diagrams for every package, the commit path as
  a sequence diagram, and a note on how the diagrams were validated. Start here if
  you want to know what the parts are before reading why they are shaped that way.
* **[docs/05-workflow-analysis.md](docs/05-workflow-analysis.md)** — the behavioural
  view: the workflow analysed through **module associations** — module boundaries,
  what each module talks to and through which interface, and the write and read flows
  decomposed into numbered steps naming the module, the call, and the data structure
  that crosses the boundary. Where `04` says which parts exist, this says how they
  cooperate at run time. Its last three sections are a code-level walkthrough of the
  transaction mechanism and its interaction with the consensus layer: the six gates a
  request passes, the three tables of the transaction state machine and the two rules
  that must be exact complements, and the two handshakes (prewrite, and seal-then-commit)
  with the failure-handling closure around them.

Each of the five also has a Chinese translation, named after the original with a
`.zh-CN.md` suffix. The translations are checked rather than trusted: a script
verifies that code blocks, mermaid diagrams, headings, links and inline `code spans`
all survived the translation intact. A second script checks that every Go snippet
quoted in a document actually appears in the sources — which has already caught two
wrong quotations.

## Status and limits

This is a study implementation: it is complete and tested, but it is not a
product. Read [§11 of the design
doc](docs/02-transaction-design.md#11-not-implemented) before pointing it at
anything you care about. In brief: snapshot isolation rather than serializable,
no log compaction or snapshots, no dynamic membership, no version garbage
collection, and sequential group RPCs.

One correctness limit is worth stating separately, because it is a *read*
consistency property rather than a missing feature. A cross-key snapshot read is
not guaranteed to be a state the system was ever in: a per-group read barrier
cannot cover a transaction that has been decided but whose write has not yet been
proposed to a group (docs/03-read-barrier.md §1.5 and §2.6). Writes are not lost —
atomicity is enforced — but a multi-key read at one timestamp may mix two instants.
Closing it needs a completion registry at the timestamp oracle.

## Credits and licence

`internal/raft` is [Eli Bendersky's](https://eli.thegreenplace.net/) instructional
Raft implementation, imported from
[`github.com/eliben/raft`](https://github.com/eliben/raft) (`part3/raft`) with the
small, documented extensions listed in the analysis document. The original work
is in the public domain (Unlicense); see [LICENSE](LICENSE).

The transaction, service, oracle and cluster layers are new work, released under
the same terms.
