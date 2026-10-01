# Analysis of `eliben/raft`: architecture and key mechanisms

> 中文版：[`01-raft-analysis.zh-CN.md`](01-raft-analysis.zh-CN.md)

This document analyses the upstream project
[`github.com/eliben/raft`](https://github.com/eliben/raft) — an instructional
implementation of the Raft consensus algorithm in Go — and is the reference for
the design decisions taken in this repository. `internal/raft` here is that
project's `part3/raft` vendored verbatim apart from changes listed in
[§5](#5-what-this-repository-changes-in-the-raft-core).

Upstream revision analysed: `1a48e2c` ("Document the storage atomicity
assumption"), 9 contributors.

---

## 1. What the repository is

A five-part teaching series, each part a complete, self-contained Go module:

| Directory | Corresponds to | What it adds |
|---|---|---|
| `part1/` | [Part 1: Elections](https://eli.thegreenplace.net/2020/implementing-raft-part-1-elections/) | Leader election, terms, votes, heartbeats |
| `part2/` | [Part 2: Commands and log replication](https://eli.thegreenplace.net/2020/implementing-raft-part-2-commands-and-log-replication/) | Log replication, commit index, the commit channel |
| `part3/raft/` | [Part 3: Persistence and optimizations](https://eli.thegreenplace.net/2020/implementing-raft-part-3-persistence-and-optimizations/) | Durable `currentTerm`/`votedFor`/`log`, conflict-index fast rewind |
| `part4kv/` | [Part 4: Key/Value database](https://eli.thegreenplace.net/2024/implementing-raft-part-4-keyvalue-database/) | A REST KV service built on part 3 — the first *application* of the consensus module |
| `part5kv/` | [Part 5: exactly-once delivery](https://eli.thegreenplace.net/2024/implementing-raft-part-5-exactly-once-delivery/) | Solves Part 4's lost-response duplicate problem |
| `tools/raft-testlog-viz/` | — | Turns `go test -v` output into a browsable HTML timeline |

The duplication between parts is a deliberate authoring decision (stated in the
README): each directory can be read in isolation, and `diff` between them is
itself the lesson. `part3` is the only part whose Raft code lives in an
importable package rather than `package main`, because `part4kv` and `part5kv`
consume it.

The dependency graph, therefore, is:

```
part1, part2   standalone
part3/raft     the consensus core, importable as github.com/eliben/raft/part3/raft
part4kv        -> part3/raft
part5kv        -> part3/raft
```

## 2. Architecture

### 2.1 Layers and where the boundary sits

Upstream draws one boundary that matters, and everything else follows from it:

```
        application (KV service, datastore, HTTP/REST API)
   ────────────────────────────────────────────────────────  ← the seam
        raft.Server      transport: net/rpc + RPCProxy
        raft.ConsensusModule   the algorithm
        raft.Storage     durability
```

Above the seam, the application only ever sees three things: `Submit(cmd) int`,
a `chan CommitEntry`, and `Storage`. The consensus module knows nothing about
keys, values, or HTTP. That is the property that makes `part3` reusable by both
KV services, and it is the property this repository preserves.

### 2.2 `ConsensusModule` — the algorithm

One struct holds both the paper's persistent and volatile state, plus the
channels that connect it to the rest of the process:

```go
// persistent
currentTerm int
votedFor    int
log         []LogEntry

// volatile, all servers
commitIndex, lastApplied int
state                    CMState    // Follower | Candidate | Leader | Dead
electionResetEvent       time.Time

// volatile, leaders only
nextIndex, matchIndex map[int]int
```

Concurrency is handled by a single `sync.Mutex` (`cm.mu`) guarding all of it,
plus three channels:

* `commitChan` — outbound; where the CM reports entries that reached consensus.
* `newCommitReadyChan` — internal notification that `commitIndex` may have
  advanced; a dedicated goroutine (`commitChanSender`) drains it.
* `triggerAEChan` — "send AppendEntries now instead of waiting for the next
  heartbeat".

Three long-lived goroutines per server: the election timer, the leader's
heartbeat loop, and the commit sender.

### 2.3 Elections

`runElectionTimer` polls every 10 ms and compares `time.Since(electionResetEvent)`
against a randomised timeout (`150 + rand(150)` ms; `RAFT_FORCE_MORE_REELECTION`
biases it to 150 ms to induce collisions). The timer exits whenever the state
leaves follower/candidate or the term changes, and `becomeFollower` and
`startElection` each start a fresh one — so there is no explicit "reset the
timer", just "the current timer notices it is obsolete and dies".

Two details are worth noting because they are the ones that were fixed upstream
after review:

* `startElection` persists `currentTerm` and `votedFor = self` **before** sending
  any `RequestVote`. Without that, a candidate that crashes and restarts could
  vote for a different candidate in the same term.
* `becomeFollower` persists when the term advances, for the same reason.

Votes are counted with `votesReceived*2 > len(peerIds)+1`, i.e. a strict
majority.

### 2.4 Log replication and the two optimisations from Part 3

The leader sends, per peer, `AppendEntries{PrevLogIndex, PrevLogTerm, Entries,
LeaderCommit}`. A follower accepts only if its log has an entry at
`PrevLogIndex` whose term equals `PrevLogTerm` — the Log Matching Property
enforced as an RPC precondition.

On mismatch the follower returns `ConflictIndex`/`ConflictTerm`:

```go
if args.PrevLogIndex >= len(cm.log) {
    reply.ConflictIndex = len(cm.log); reply.ConflictTerm = -1
} else {
    reply.ConflictTerm = cm.log[args.PrevLogIndex].Term
    // walk back to the first index of that term
    reply.ConflictIndex = i + 1
}
```

The leader uses that to jump `nextIndex` **back a whole term at a time** instead
of one entry at a time, which is what turns log repair from O(entries) round
trips into O(terms). This is the "faster conflict resolution" of §5.3 in the
paper, and it is the most interesting single piece of Part 3.

Commit advancement implements Figure 2's rule exactly — only entries **of the
current term** are counted:

```go
for i := cm.commitIndex + 1; i < len(cm.log); i++ {
    if cm.log[i].Term == cm.currentTerm {
        matchCount := 1
        for _, peerId := range cm.peerIds { if cm.matchIndex[peerId] >= i { matchCount++ } }
        if matchCount*2 > len(cm.peerIds)+1 { cm.commitIndex = i }
    }
}
```

### 2.5 Persistence

`Storage` is a three-method interface — `Set`, `Get`, `HasData` — with a
`MapStorage` implementation for tests. The doc comment records an important
assumption:

```go
// This example assumes that the sequence of Set calls in persistToStorage
// atomically persists currentTerm, votedFor, and log together.
```

`persistToStorage` gob-encodes the whole log on every change. That is the
simplest thing that can possibly satisfy the requirement, and it is
quadratic — an observation this repository's design leans on, not a criticism
that changes it.

### 2.6 Transport and fault injection

`raft.Server` wraps the CM with a `net/rpc` server and a map of `*rpc.Client` to
peers. All outbound calls go through `Server.Call`, which delegates to
`RPCProxy.Call`. `RPCProxy` is the seam the tests use to inject faults:

* `DropCallsAfterN(n)` — drop calls after the next *n*, simulating a partition.
* `RAFT_UNRELIABLE_RPC` — randomly drop one in ten calls and delay another.

Routing is by server ID: `ConnectToPeer(id, addr)` and `DisconnectPeer(id)`.

### 2.7 The KV service (Part 4)

```
HTTP client ──REST──> KVService ──Submit──> raft.Server ──RPC──> peers
                          │                      │
                          └── runUpdater <── commitChan
                                  │
                              DataStore (map[string]string)
```

The interesting mechanics:

* **`Command` is one flat struct** with a `Kind` discriminator and fields for
  every operation, rather than an algebraic type. `ResultValue`/`ResultFound`
  ride along on the same struct so it can double as the reply.
* **Commit subscriptions.** The handler cannot reply until *its* entry commits,
  but commits arrive on a single shared channel. `Submit` returns the log index;
  the handler registers a one-shot `chan Command` keyed by that index; the
  updater goroutine hands the command to the matching subscription.
* **Reads go through the log too.** Part 4 explains why at length: a deposed
  leader that still believes it leads would otherwise serve a stale read and
  break linearizability. This is section 8 of the Raft paper.
* **The `Id` check.** When a commit arrives at the subscribed index, the handler
  verifies `commitCmd.Id == kvs.id`. Otherwise its entry was lost to a leadership
  change and index 42 was overwritten by another leader's command; the client
  gets `StatusFailedCommit` rather than a wrong answer.
* **Client leader discovery.** `KVClient.send` walks the address list, treats
  `StatusNotLeader` and per-attempt timeouts as "try the next one", and remembers
  the last address that accepted a command.

### 2.8 Exactly-once delivery (Part 5)

Part 4's client retries, which can duplicate a command whose response was lost.
Part 5 adds a `(ClientID, RequestID)` pair to every command, and the updater
keeps `lastRequestIDPerClient`:

```go
lastReqID, ok := kvs.lastRequestIDPerClient[cmd.ClientID]
if ok && lastReqID >= cmd.RequestID {
    cmd = Command{Kind: cmd.Kind, IsDuplicate: true}   // don't apply
} else {
    kvs.lastRequestIDPerClient[cmd.ClientID] = cmd.RequestID
    // ... apply
}
```

The client allocates `RequestID` monotonically per client, so a retry re-sends
the *same* ID and is recognised; the handler reports `StatusDuplicateRequest` and
the client knows its earlier attempt actually landed. Part 5 also adds
`DelayNextHTTPResponse` so the test suite can provoke exactly the ambiguous case:
the service acts on the request but the response is lost.

## 3. Key mechanisms, and what each one buys

| Mechanism | Where | Why it matters |
|---|---|---|
| Single mutex over all CM state | `raft.go` | Makes every RPC handler atomic with respect to the state machine; the price is that nothing under the lock may block |
| One-shot election timer that exits on state/term change | `runElectionTimer` | Avoids reset races; a stale timer simply dies |
| Persist before voting / before `becomeFollower` | `startElection`, `becomeFollower` | Election safety across crashes |
| `ConflictIndex`/`ConflictTerm` | `AppendEntries` / `leaderSendAEs` | Term-at-a-time log repair |
| Commit only same-term entries | `leaderSendAEs` | Figure 2's commit rule; prevents committing entries that could still be overwritten |
| Commit subscription by log index | `part4kv`/`part5kv` | Lets a caller correlate "my submission" with "a commit", on a shared channel |
| Reads through the log | `part4kv`+ | Linearizable reads |
| `(ClientID, RequestID)` dedup | `part5kv` | Exactly-once semantics across retries |
| `RPCProxy` | `server.go` | Deterministic fault injection without touching the algorithm |
| `Storage` interface | `storage.go` | Keeps durability swappable; carries an explicit atomicity assumption |

## 4. What the design deliberately leaves out

These are not oversights; the posts say so. They are the gaps that any
production use — and this repository's transaction layer — has to close:

1. **No cluster membership changes.** The peer set is fixed at construction
   (the paper's §6 is untouched).
2. **No snapshots / log compaction.** The log grows without bound and is
   re-encoded in full on every write, so restart time grows with history.
3. **Whole-log persistence.** Quadratic in log size; no incremental append.
4. **No read-index optimisation.** Reads pay a full log round trip; the paper's
   §8 lease/read-index alternatives are noted as future work.
5. **`gob` for both storage and transport.** Convenient, but brittle across
   schema changes and not language-neutral.
6. **No transactions.** Each command is atomic on its own; nothing composes
   several keys into one all-or-nothing unit. This is the gap this repository
   fills.
7. **One raft group at all.** `part4kv`/`part5kv` are a single group; there is no
   sharding, and therefore no cross-shard anything.

## 5. What this repository changes in the Raft core

`internal/raft` is upstream `part3/raft`, with these additions, each marked in
the source as a local extension:

| Change | Why it was necessary |
|---|---|
| `StateMachineHandler`, `SetHandler`, and applying it from the commit path | Upstream has no state-machine hook at all: the application applies committed commands itself from `commitChan`. A transaction layer needs the *group* to be the state machine, applied in log order on every replica |
| `Server.Apply` blocks until the entry commits and returns the state machine's answer | Gives a coordinator a synchronous request/response over Raft without a second correlation mechanism |
| The state machine runs **outside** the consensus lock | Holding `cm.mu` across an application `Apply` deadlocked `Stop` and let a slow apply stall elections and heartbeats |
| `leaderId` tracking + `Server.LeaderID` | Lets a follower tell a client who the leader is, turning leader discovery from "probe every member" into one hop. (Upstream Part 4 footnote 4 proposes exactly this as an exercise.) |
| Real RPC connections held by `Shutdown`, accept loop woken from `Stop` | Upstream's shutdown waits on handlers that may be blocked reading from a peer that is itself shutting down. This was the single largest source of hangs when many servers stop at once |
| `stopChan` consulted while delivering commit entries | Upstream blocks forever if a client stops reading its commit channel |

Everything else — the algorithm, the locking discipline, the persistence model,
the RPC surface, and the entire upstream test suite — is unmodified.

## 6. Testing approach

Upstream's method is worth copying, and this repository does:

* An in-process `Harness` per part that builds an N-server cluster, connects
  peers, and offers `CrashPeer`, `DisconnectPeer`, `RestartPeer`,
  `PeerDropCallsAfterN`.
* **`RestartPeer` reuses the crashed server's `Storage`**, which is what makes
  the persistence tests real rather than nominal.
* Tests assert *cluster-wide* agreement (`CheckCommitted` compares the commit
  sequence across all live servers), not just local behaviour.
* `leaktest.CheckTimeout` guards against goroutine leaks.
* `tools/raft-testlog-viz` turns verbose test output into an HTML timeline — the
  debugging aid that makes a distributed test failure readable.

This repository's `cluster/` package is the same idea one layer up: it builds a
Multi-Raft cluster, and its fault injection works on the *transaction* system
(partition a group, crash an oracle leader, restart a whole cluster from
storage).

## 7. Bibliography

* [Raft paper](https://raft.github.io/raft.pdf) — Ongaro & Ousterhout, *In
  Search of an Understandable Consensus Algorithm* (USENIX ATC '14).
* [Part 0: Introduction](https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/)
* [Part 1: Elections](https://eli.thegreenplace.net/2020/implementing-raft-part-1-elections/)
* [Part 2: Commands and log replication](https://eli.thegreenplace.net/2020/implementing-raft-part-2-commands-and-log-replication/)
* [Part 3: Persistence and optimizations](https://eli.thegreenplace.net/2020/implementing-raft-part-3-persistence-and-optimizations/)
* [Part 4: Key/Value database](https://eli.thegreenplace.net/2024/implementing-raft-part-4-keyvalue-database/)
* [Part 5: exactly-once delivery](https://eli.thegreenplace.net/2024/implementing-raft-part-5-exactly-once-delivery/)
* [Linearizability in distributed systems](https://eli.thegreenplace.net/2024/linearizability-in-distributed-systems/)
