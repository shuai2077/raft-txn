# 05 — Workflow analysis: by module association

> 中文版：[`05-workflow-analysis.zh-CN.md`](05-workflow-analysis.zh-CN.md)

This document is a workflow analysis of the codebase **organised by module
association**. It does not repeat the design rationale — that is the job of
[`docs/02-transaction-design.md`](02-transaction-design.md); this document answers
only three questions:

1. What **modules** the code is split into, and where each module's responsibility
   boundary lies;
2. **Through what associations** modules relate to each other (calls, channels,
   interface implementations, HTTP);
3. When one request travels through the whole system, **in what order those
   associations are used**.

The analytical method follows an article that walks through the source of etcd's raft
implementation (referred to below as **the reference article**): it first gives a
glossary and a macro architecture, then breaks the core data structures down layer by
layer, then focuses on the **cross-layer interaction flows**, and finally splits the
write flow and the read flow into numbered steps, each one saying outright "which
module → calls what → which data structure crossed the boundary". This document
reuses that skeleton, and at every key point calls out **the structural differences
between this repository and etcd** — those differences are exactly where the design
of this code lies.

---

## 0 Preface

The reference article analyses the raft implementation in etcd v3.1.10: etcd splits
raft into an **algorithm layer** (`./raft/*`, an SDK that can be imported on its own)
and an **application layer** (`contrib/raftexample/*`). The two layers each run in
their own goroutines, communicate asynchronously over a handful of channels, and use
`Ready`/`Advance` as the boundary of one round of interaction.

This repository faces the same structural problem but answers it differently. **That
difference is the key to understanding this codebase**:

| Concern | etcd (the reference article) | This repository |
|---|---|---|
| Boundary between the algorithm layer and the transaction layer | etcd's raft is a general-purpose consensus library | `internal/raft` is likewise a general-purpose consensus library, but **adds one state-machine hook** (`StateMachineHandler`) |
| Communication between the application layer and the algorithm layer | a few channels (`propc`/`recvc`/`readyc`/`advancec`) + the `Ready`/`Advance` protocol, **asynchronous** | `Server.Apply`: propose + wait, **synchronous**. That `Ready`/`Advance` protocol layer was removed in the local extensions |
| What drives the timer | the application layer calls `Node.Tick()` on a ticker | the election timer inside `ConsensusModule` keeps its own time; there is no external Tick |
| Who applies the state machine | the application layer reads committed entries from `commitChan` and applies them itself | **`commitChanSender` applies them directly on the commit path**; the application layer only drains the channel |
| Cross-group transactions | a single group in etcd (the v3.1 era); transactions live elsewhere | the `txn` layer does the 2PC coordination for Multi-Raft; `txnservice` serves **one group** only |

The sections below unfold this point by point.

---

## 1 Module breakdown

The raft-related code is distributed as follows. **The "layer" column is the
coordinate system for everything that follows in this document.**

| Module | Directory | Layer | Responsibility |
|---|---|---|---|
| Consensus algorithm layer | `internal/raft/*` | algorithm layer | Raft itself: elections, log replication, persistence, RPC transport. Upstream `part3/raft` plus a few additions marked "local extension" |
| Group service layer | `txnservice/*` | application layer | Assembles **one** Raft group into a remotely accessible service: algorithm layer + transaction state machine + HTTP interface |
| Transaction protocol layer | `txn/*` | application layer (protocol) | MVCC state machine, intent locks, the 2PC coordinator, lock resolution policy. **Defines every interface** |
| Timestamp oracle | `tso/*` | application layer (its own consensus group) | A Raft-replicated timestamp oracle, likewise running on top of the algorithm layer |
| Cluster assembly | `cluster/*` | assembly layer | Assembles several groups and several oracle members into one Multi-Raft cluster, and provides fault injection |
| Demo program | `cmd/demo/*` | assembly layer | A runnable end-to-end demonstration |

**The key structural fact: `txn` defines the interfaces, and other modules implement
them.** That lets the same coordination logic run both over HTTP (the production
path) and in-process (tests and `cluster`). §3 expands on this.

---

## 2 Glossary

The reference article's practice of fixing terminology up front is well worth
copying: in this codebase "layer", "state machine" and "commit" each have two
meanings, and unless they are settled first the rest is bound to be ambiguous.

| Term | English name / code identifier | Notes |
|---|---|---|
| algorithm layer | algorithm layer / `internal/raft` | The module that encapsulates the Raft consensus mechanism. Imported by the layers above as a library; **it is unaware of transactions, key-values and HTTP** |
| application layer | application layer | The module that brings together storage, communication and HTTP capability. It links the two directions: it serves clients outward and uses the algorithm layer inward |
| group service | group service / `txnservice.Node` | The abstraction of "one Raft group" within the application layer. It holds the algorithm layer's `*raft.Server` and the transaction state machine `*txn.Store`, and exposes HTTP |
| coordinator | coordinator / `txn.Client` | The initiator of the cross-group 2PC. It is **not** a Raft node itself; it is a piece of client-side logic |
| transaction handle | transaction handle / `txn.Txn` | All the local state of one transaction (start timestamp, write set, prepared groups). `Client` holds no transaction state, so it can be reused concurrently |
| data state machine | data state machine / `txn.Store` | The medium that actually stores the data: version chains + lock table + transaction records. To avoid confusion with the "Raft node state machine", this document always calls it the **data state machine** |
| node state machine | node state machine / `ConsensusModule` | A Raft node is itself a state machine: any input (an election timeout, an inbound RPC, a proposed command) drives a change of state |
| proposal | proposal / `ConsensusModule.propose` | The first stage of the two-stage commit: append the command to the log |
| commit | commit / `commitIndex` advancing | The second stage: the log entry has been replicated to a majority and may take effect externally |
| apply | apply / `StateMachineHandler.Apply` | Feed committed log entries to the data state machine, so that a write request actually takes effect |
| barrier | barrier / `CmdBarrier` | A **no-op** log entry whose only meaning is its position in the log: waiting for it to be applied proves that every entry before it has been applied |
| seal | seal / `CmdSeal` | Write the commit timestamp into the **primary key**'s lock record, but **do not** publish a version. **Sealing the primary key is the transaction's atomic commit point** |
| primary / secondary key | primary / secondary key | One distributed lock table. The primary key carries the transaction's commit decision; a secondary key can only go and ask the primary |
| lease | lease / `LockRecord.ExpiresAt` | The lifetime of a lock, which bounds how long a reader waits; **it does not authorise a rollback** (see §7.5) |
| resolution | resolution / `Client.resolveLock` | When a reader trips over a lock someone else left behind, it determines that transaction's fate and publishes or discards accordingly |
| timestamp oracle | timestamp oracle / `tso` | The source of globally monotonic, unique timestamps; itself a Raft group |
| start timestamp | start timestamp / `Txn.startTS` | The consistency viewpoint a transaction reads at |
| commit timestamp | commit timestamp / `Txn.committedTS` | The point in time at which a transaction's writes become externally visible |

---

## 3 Macro architecture

### 3.1 The algorithm layer

The algorithm layer is the core implementation of the Raft consensus mechanism: terms
and voting, log replication, persistence, RPC transport. It has exactly three kinds
of **input**: a peer RPC that arrives, a command proposed locally, and the election
timer expiring.

The most important extension this repository makes to the algorithm layer is the
introduction of a **state-machine hook**:

```
StateMachineHandler.Apply(command any) any
```

Upstream `part3/raft` has no such concept — upstream hands committed entries to the
application layer, and the application layer decides for itself how to apply them.
This repository makes **the group itself the state machine**: after `Server.Apply`
proposes a command, it waits for `commitChanSender` to finish applying that entry, and
then returns the data state machine's answer back along the same path to the proposer.

This has two consequences, and they are the premise for understanding every flow that
follows:

1. **A proposal can be synchronous.** When the coordinator calls
   `Participant.Prewrite`, what it gets back is not a log index but the data state
   machine's verdict on that prewrite (success / conflict / lock held). There is no
   second mechanism for "correlating a result with a log index".
2. **Apply must happen outside the consensus lock.** `commitChanSender` releases
   `cm.mu` before calling `handler.Apply`; otherwise the data state machine's own
   lock would form a cycle with the consensus lock, and one slow apply would hold up
   elections and heartbeats.

### 3.2 The group service layer

`txnservice.Node` is the assembly that sits on top of the algorithm layer; it turns
one Raft group into a remotely accessible service:

```
        HTTP client (txnservice.GroupClient)
                    │  POST /txn/prewrite/ etc.
                    ▼
        ┌───────────────────────────────────────────┐
        │  Node (one Raft group)                     │
        │   httpServer  ──> handlePrewrite ...       │  application layer: protocol & transport
        │        │ propose(cmd, render)              │
        │        ▼                                   │
        │   RaftLog.ApplyCommand ──> Server.Apply    │  ← the inter-layer boundary
        │        │                                   │
        │   raft.Server / ConsensusModule            │  algorithm layer: consensus
        │        │ commitChanSender: handler.Apply   │  ← the state-machine hook
        │        ▼                                   │
        │   txn.Store (data state machine)           │  application layer: MVCC + locks
        └───────────────────────────────────────────┘
```

Note the `commitChan` inside `Node`: the algorithm layer still sends committed entries
to it, but **it no longer drives the state machine** (the state machine has already
been applied on the synchronous path). The `Node.drainCommits` goroutine exists only
to drain the channel; otherwise the algorithm layer's commit sender would block. This
is the structural leftover that necessarily appears once "synchronous `Apply` replaces
`Ready`/`Advance`", and it is the most direct contrast with etcd.

### 3.3 Macro architecture: what connects the modules

This is the most important diagram in the document. It marks every module, and **what
actually sits on each edge between modules**:

```mermaid
flowchart TB
    subgraph app["application layer"]
        HTTP["httpapi<br/>client HTTP requests"]
        GC["txnservice.GroupClient<br/>HTTP client / Participant impl"]
        CO["txn.Client + txn.Txn<br/>2PC coordinator (cross-group)"]
        RS["txn.DefaultResolver<br/>lock resolution policy"]
    end

    subgraph node["group service layer: one Raft group = one Node"]
        SVC["txnservice.Node<br/>handle* + propose"]
        LOG["txn.RaftLog<br/>Log + Participant impl"]
        ST["txn.Store<br/>data state machine"]
    end

    subgraph alg["algorithm layer"]
        SRV["raft.Server<br/>Apply / Serve / RPC"]
        CM["raft.ConsensusModule<br/>node state machine + commitChanSender"]
        STOR["raft.Storage"]
    end

    subgraph tso["timestamp oracle (another Raft group)"]
        TSOS["tso.Service + tso.Oracle"]
        TSOC["tso.Client"]
    end

    subgraph asm["assembly layer"]
        CL["cluster.Cluster"]
        DEMO["cmd/demo"]
    end

    HTTP -->|"POST /txn/*"| SVC
    GC -->|"HTTP + JSON"| SVC
    CO -->|"Participant interface"| GC
    CO -->|"Oracle interface"| TSOC
    CO -->|"Resolver interface"| RS
    RS -->|"Participant interface"| GC

    SVC -->|"Log interface"| LOG
    LOG -->|"Server.Apply (synchronous)"| SRV
    SRV -->|"owns"| CM
    CM -->|"StateMachineHandler.Apply"| ST
    CM -->|"persist / restore"| STOR
    SVC -->|"reads lock table / version chains"| ST

    TSOC -->|"HTTP + JSON"| TSOS
    TSOS -->|"same raft.Server + its own state machine"| SRV

    CL -->|"assembly + fault injection"| node
    CL -->|"assembly"| tso
    CL -->|"Client()"| CO
    DEMO --> CL
```

**How to read it**: the label on a solid edge is the **type** of that association.
Three clear patterns emerge:

1. **`txn` only defines interfaces; it implements no transport.** `Participant`,
   `Log`, `Oracle`, `Resolver` and `Partitioner` are all defined in the `txn`
   package; `txnservice`, `tso` and `internal/raft` each implement one of them.
2. **The algorithm layer connects to the outside world through exactly two
   openings**: inbound `Server.Apply` and outbound `StateMachineHandler.Apply`. The
   algorithm layer does not know that transactions, keys or HTTP exist.
3. **Everything cross-group happens in the coordinator, not in any Raft group.**
   Each Raft group serves only its own slice of the key space; the 2PC orchestration
   lives entirely in the client-side logic of `txn.Client`.

---

## 4 Core data structures of the algorithm layer

This section introduces them one by one, in the manner of the reference article. A
reader who meets an unclear structure in the flow sections later can come back here.

### 4.1 LogEntry / CommitEntry

```go
// internal/raft/raft.go -- simplified
type LogEntry struct {
    Command any
    Term    int
}

type CommitEntry struct {
    Command any
    Index   int
    Term    int
}
```

One `LogEntry` is one write-ahead log record. Note that `Command` is `any`: the
algorithm layer **does not care** whether what is inside is a transaction command or
something else, and that is exactly why it can be reused (and reused by `tso`).
`CommitEntry` is the same log record after it becomes "committed", with an extra
`Index`, because it has to be sent up to the layer above so that the entry can be
located.

In this repository the log index starts at 0, and `LogEntry` has no separate `Index`
field — the index is its position in the slice. That is how upstream does it, and
this repository has not changed it.

### 4.2 Server

```go
// internal/raft/server.go -- simplified
type Server struct {
    serverId    int
    peerIds     []int
    cm          *ConsensusModule   // the algorithm core
    storage     Storage
    ready       <-chan any
    commitChan  chan<- CommitEntry
    rpcServer   *rpc.Server
    rpcProxy    *RPCProxy
    listener    net.Listener
    peerClients map[int]*rpc.Client
    conns       map[net.Conn]struct{}   // local extension: used by Shutdown
    quit        chan any
    acceptDone  chan struct{}           // local extension: waits for the accept loop to exit
    wg          sync.WaitGroup
    mu          sync.Mutex
}
```

`Server` is a wrapper around **transport + lifecycle**; the consensus algorithm itself
lives entirely in `ConsensusModule`. `Server` does three things: it listens on TCP and
provides peer-to-peer communication with `net/rpc`; it manages the outbound
`*rpc.Client` for each peer; and it translates "local calls" such as
`Apply`/`Shutdown` into operations on `ConsensusModule`.

`conns` and `acceptDone` are additions in this repository: upstream's `Shutdown` waits
on handler functions that may block forever reading from a peer, which very easily
hangs when several nodes stop at the same time.

### 4.3 ConsensusModule (the node state machine)

```go
// internal/raft/raft.go -- simplified
type ConsensusModule struct {
    id        int
    peerIds   []int
    state     CMState          // Follower | Candidate | Leader | Dead
    currentTerm int
    votedFor    int

    log         []LogEntry
    commitIndex int
    lastApplied int

    // local extension: the state-machine hook
    handler StateMachineHandler

    // local extension: the correlation table for synchronous proposals
    results map[int]chan proposalResult

    // local extension: wakeup and shutdown
    newCommitReadyChan   chan struct{}
    newCommitReadyChanWg sync.WaitGroup
    stopChan             chan struct{}
    electionTimerWg      sync.WaitGroup

    leaderId int   // local extension: points clients at the leader
    // ... election timer, peer progress, etc.
}
```

The reference article says that an etcd raft node is essentially one big state machine,
and that every operation ends up encapsulated as a message fed into it. **This
repository is not shaped that way**: there is no unified `Step(Message)` entry point,
and no role-dispatched `stepLeader`/`stepCandidate`/`stepFollower`. Instead there are
three distinct entry points:

| Entry point | Trigger | Effect |
|---|---|---|
| `Server.Apply(cmd)` | a local proposal (an HTTP request) | `propose` appends to the log → wakes replication; registers the result channel; waits for the result |
| `RPCProxy.RequestVote/AppendEntries` | a remote peer | enters the corresponding `ConsensusModule` method |
| `runElectionTimer` | the internal timer | on timeout, starts an election |

This is the most fundamental structural difference from etcd, and the reason this
repository's code is small: **it does not need one general-purpose `Message` structure
to cover every scenario**. (The reference article also criticises etcd's
all-encompassing `Message` for breaking the single-responsibility principle; this
repository effectively sidesteps that with three purpose-built entry points, at the
cost of not having etcd's extensibility.)

`results` is the key to synchronous proposals: `map[log index]result channel`. The
proposer registers its own channel, and the applier delivers the result after applying
that entry.

### 4.4 StateMachineHandler (the one outbound interface between layers)

```go
// internal/raft/raft.go -- simplified
type StateMachineHandler interface {
    Apply(command any) any
}
```

An interface with a single method, yet it is the pivot of the whole architecture: it
is the algorithm layer's **only** data outlet. `txn.Store` implements it, so the
"consensus layer" and the "transaction layer" are decoupled at the type level — the
algorithm layer does not know what a `TxnCommand` is.

### 4.5 proposalResult

```go
type proposalResult struct {
    value any
    err   error
}
```

The carrier for the return value of a synchronous proposal. `value` is exactly what
`handler.Apply` returns — for this repository, a `txn.TxnResult`.

### 4.6 Storage

```go
// internal/raft/storage.go -- simplified
type Storage interface {
    Set(key string, value []byte)
    Get(key string) ([]byte, bool)
    HasData() bool
}
```

The persistence interface; `MapStorage` is the in-memory implementation. Note that the
value is `[]byte` rather than `any`: the state machine accepts arbitrary Go values, but
the persistence layer takes bytes only, because the algorithm layer chooses its own
encoding — `persistToStorage` uses `gob` to encode `currentTerm`, `votedFor` and **the
entire log** separately and writes them under three keys.

On every change, upstream's implementation gob-encodes and writes the entire log, which
is quadratic in log length — a known simplification that is deliberately kept.

---

## 5 Core data structures of the group service layer and the transaction protocol layer

### 5.1 txnservice.Node (the group service)

```go
// txnservice/service.go -- simplified
type Node struct {
    cfg     NodeConfig
    groupID txn.GroupID

    raftServer *raft.Server    // entry point to the algorithm layer
    log        *txn.RaftLog    // implements the Log and Participant interfaces
    store      *txn.Store      // the data state machine

    commitChan chan raft.CommitEntry  // only drained; it does not drive the state machine
    httpServer *http.Server
    listener   net.Listener
    leaders    atomic.Int64            // routing hint
    quit       chan struct{}
    wg         sync.WaitGroup
}
```

`Node` is the application layer's abstraction of "one Raft group", playing both
**server** (offering HTTP outward) and **user of the algorithm layer** (calling
`ApplyCommand` inward). Its construction order is worth remembering, because it is
precisely a dependency chain:

```
raft.NewServer(...) → Serve()          algorithm layer is up
txn.NewRaftLog(groupID, raftServer)    adapted into the Log interface
txn.NewStore(log)                      the data state machine
raftServer.SetHandler(store)           the state-machine hook is installed
```

### 5.2 txn.RaftLog (the cross-layer adapter)

```go
// txn/raftadapter.go -- simplified
type RaftLog struct {
    server  *raft.Server
    groupID GroupID
    mu      sync.RWMutex
    store   *Store        // bound late
    learnedLeader int
}
```

`RaftLog` is the **only type that implements both `Log` and `Participant`**:

- As `Log`: `ApplyCommand` → `server.Apply(cmd)`; this is the channel through which
  the data state machine replicates commands.
- As `Participant`: `Read`/`Prewrite`/`Seal`/`Commit`/`Rollback`/`Locked`/`Barrier`,
  where every method wraps its arguments into a `TxnCommand` and goes through the same
  `ApplyCommand`.

In other words, **on the in-process path the seven `Participant` methods are all thin
wrappers around `ApplyCommand`**, differing only in the argument structure and the
interpretation of the result. This becomes very clear in the comparison table in §6.

### 5.3 txn.Store (the data state machine)

```go
// txn/store.go -- simplified
type Store struct {
    log       Log
    lockTTL   time.Duration
    nowMillis func() uint64

    versions map[Key][]Version    // MVCC version chains
    locks    map[Key]*LockRecord  // intent lock table
    actors   map[TxnID]*Actor     // transaction records

    appliedCommitTS Timestamp     // watermark: the highest commit timestamp applied
}
```

Three maps are all the data there is, and `Apply(command any) any` is the only entry
point. **The whole data state machine has no mutex of its own** — because Raft already
guarantees that only one goroutine applies entries at a time. That is the direct
benefit of "letting the group be the state machine".

Each of the three maps answers a different question:

- `versions` answers "what can snapshot S see";
- `locks` answers "is there a write being held back";
- `actors` answers "has this transaction been decided" — the one place a reader of a
  secondary lock can ask.

### 5.4 txn.Client and txn.Txn (the coordinator and the transaction handle)

```go
// txn/client.go -- simplified
type Client struct {
    oracle       Oracle
    partitioner  Partitioner
    participants map[GroupID]Participant
    resolver     Resolver
    resolveTimeout time.Duration
    now          func() uint64
    clientID     int64
    requests     atomic.Int64
    // ... statistics
}

type Txn struct {
    c       *Client
    id      TxnID
    startTS Timestamp
    writes  map[Key]WriteOp
    prepared map[GroupID]struct{}
    primaryKey Key
    committedTS Timestamp
    state   state
    barriered map[GroupID]struct{}
}
```

The split is deliberate: **`Client` holds no transaction state**, so one `Client` can
serve many transactions concurrently; **`Txn` holds all the transaction state**, and is
therefore explicitly "not safe for concurrent use". `barriered` records the groups from
which a read barrier has already been taken, guaranteeing that each group is barriered
once per transaction.

### 5.5 txn.TxnCommand (the command vocabulary of the cross-group protocol)

```go
// txn/command.go -- simplified
type TxnCommand struct {
    Kind TxnCommandKind
    ReadArgs        *ReadArgs
    PrewriteArgs    *PrewriteRequest
    SealArgs        *SealArgs
    CommitArgs      *CommitRequest
    RollbackArgs    *RollbackRequest
    ResolveLockArgs *ResolveLockArgs
    QueryLockArgs   *QueryLockArgs
}
```

Seven kinds of command, all travelling through the same `Raft` log. `CmdBarrier` is the
eighth: it carries no payload at all, and its only meaning is its **position**.

---

## 6 Cross-layer interaction flows

### 6.1 The two Participant implementations — two paths for one coordinator

This is the one piece of structural design in this repository most worth discussing on
its own. In the reference article the application layer and the algorithm layer
communicate asynchronously over channels, and there is only one path; this repository
has **two**, and the coordinator code is completely unaware of which one it is using:

| `Participant` method | HTTP path (`txnservice.GroupClient`) | In-process path (`txn.RaftLog`) |
|---|---|---|
| `Read` | `POST /txn/read/` → `handleRead` → `propose` | `ApplyCommand(CmdRead)` |
| `Prewrite` | `POST /txn/prewrite/` → `handlePrewrite` | `ApplyCommand(CmdPrewrite)` |
| `Seal` | `POST /txn/seal/` → `handleSeal` | `ApplyCommand(CmdSeal)` |
| `Commit` | `POST /txn/commit/` → `handleCommit` | `ApplyCommand(CmdCommit)` |
| `Rollback` | `POST /txn/rollback/` → `handleRollback` | `ApplyCommand(CmdRollback)` |
| `Locked` | `POST /txn/locked/` → `handleLocked` | `ApplyCommand(CmdQueryLock)` |
| `Barrier` | `POST /txn/barrier/` → `handleBarrier` | `ApplyCommand(CmdBarrier)` |

Both paths converge on `Server.Apply` in the end. The HTTP path does exactly two extra
things: JSON encoding and decoding, and **following the leader hint**
(`GroupClient.callFor` jumps straight to the hinted node and retries when it receives
`not-leader`). On the in-process path, `RaftLog.ApplyCommand` hands `NotLeaderError`
back to the caller unchanged, and the caller retries.

**Why this is worth doing**: tests and `cluster` need a multi-group cluster, and if
every group had to start an HTTP port, both the cost and the nondeterminism would rise.
With this interface layer, `cluster` can assemble either path, and not a single line of
`txn`'s 2PC logic has to change.

### 6.2 One synchronous proposal: the six actions of `Server.Apply`

This is the **only inbound boundary** between the algorithm layer and the application
layer, and it is worth unfolding step by step:

```go
// internal/raft/server.go — simplified; structure and comments preserved
index := cm.propose(cmd)                    // (1) append to the log and get the index
ch := make(chan proposalResult, 1)          // (2) build the result channel (buffered by 1)
cm.results[index] = ch                      // (3) register the association: index → channel
if cm.commitIndex >= index {                // (4) if already committed, nudge the applier once
    select {
    case cm.newCommitReadyChan <- struct{}{}:
    default:
    }
}
cm.mu.Unlock()                              // (5) release the lock, then wait
select {
case res := <-ch:                           // (6a) the applier delivered a result
case <-cm.stopChan:                         // (6b) shutdown
case <-timer.C:                             // (6c) timeout (no majority)
}
```

Correspondingly, on the apply side in the algorithm layer:

```go
// internal/raft/raft.go — commitChanSender (simplified; loop body only)
for range cm.newCommitReadyChan {
    cm.mu.Lock()
    entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]   // take the newly committed entries
    cm.lastApplied = cm.commitIndex
    handler := cm.handler
    cm.mu.Unlock()                                          // the key point: release the lock, then apply

    for i, entry := range entries {
        var result any
        if handler != nil {
            result = handler.Apply(entry.Command)           // apply to the data state machine
        }

        cm.mu.Lock()
        if ch, ok := cm.results[index]; ok {                // find the proposer
            delete(cm.results, index)
            cm.mu.Unlock()
            select {
            case ch <- proposalResult{value: result, err: applyErr}:
            default:
            }
        } else {
            cm.mu.Unlock()
        }
        select {
        case cm.commitChan <- CommitEntry{
            Command: entry.Command,
            Index:   index,
            Term:    savedTerm,
        }:
        case <-cm.stopChan:
            return
        }
    }
}
```

**This code explains why this repository does not need `Ready`/`Advance`.** In etcd,
the algorithm layer packages "the results of this round" into a `Ready` and hands it to
the application layer; after finishing persistence and sending, the application layer
calls `Advance` to say it is done, and the pair of methods delimits one round of
interaction. This repository **folds that round into a single function call**:
`propose` registers, the applier delivers, `Apply` picks up the result, and the call
returning is the end of the round. The side effect is that `commitChan` degrades into a
drain channel that exists "only so the sender does not block"
(`Node.drainCommits`).

**Two details that must be got right**, both pointed out in the comments:

- The result channel must be registered **before `cm.mu` is released**; otherwise the
  applier may deliver while nobody is listening.
- Delivery uses a non-blocking `select`: the applier must not stall the whole log
  advance just because one proposer has given up waiting.

### 6.3 Start-up order (the dependency chain)

```mermaid
sequenceDiagram
    autonumber
    participant CL as cluster.Cluster
    participant N as txnservice.Node
    participant RS as raft.Server
    participant ST as txn.Store
    participant TSO as tso.Service

    CL->>N: NewNode(NodeConfig)
    N->>RS: raft.NewServer(...)
    N->>RS: Serve()
    Note over RS: starts accept loop, election timer, commitChanSender
    N->>N: txn.NewRaftLog(groupID, raftServer)
    N->>ST: txn.NewStore(log)
    N->>ST: RaftLog.SetStore(store)
    ST->>RS: SetHandler(store)  ← installs the state-machine hook
    N->>N: go drainCommits() / go pollLeader() / go serveHTTP()
    CL->>TSO: NewService(...) x TSONodes
    CL->>CL: txn.NewClient(oracle, partitioner, participants, nil)
    CL->>CL: SetResolver(NewResolver(txnClient))
    CL->>CL: WaitForLeaders(10s)
```

The ordering of those two lines in `cluster.New` is meaningful: the coordinator and the
resolver **reference each other** (the resolver has to go through the coordinator's
participant set, and the coordinator needs the resolver to clean up foreign locks), so
the coordinator is created first, then the resolver, and then the reference is filled
in.

---

## 7 The role state machine and the two core paths

### 7.1 Raft role transitions (inside the algorithm layer)

This repository does not dispatch role transitions through `Step(Message)`; they are
invoked directly from three places:

```mermaid
stateDiagram-v2
    [*] --> Follower
    Follower --> Candidate: election timeout
    Candidate --> Candidate: vote timeout / new election
    Candidate --> Leader: majority of votes
    Candidate --> Follower: sees a higher term
    Leader --> Follower: sees a higher term
    Follower --> Follower: term advances
```

One point that matters for the transaction layer: **a change of leadership does not
affect correctness, only availability and latency**. The coordinator moves to another
node and retries when it gets `NotLeaderError`; `txnservice.GroupClient` remembers the
hinted leader so that it does not probe repeatedly.

### 7.2 The write flow: committing a cross-group transaction (eight steps)

This is the most central path in the whole codebase. It crosses **two Raft groups + the
timestamp oracle + the coordinator**, so it shows module associations far better than
etcd's single-group write flow.

```mermaid
sequenceDiagram
    autonumber
    participant APP as Application (e.g. cmd/demo)
    participant TX as txn.Txn
    participant CL as txn.Client
    participant ORA as tso.Client
    participant GA as Group A (owns the primary key)
    participant GB as Group B

    APP->>CL: Begin(ctx)
    CL->>ORA: ReadTS()
    ORA-->>CL: startTS
    CL-->>APP: *Txn{startTS}

    APP->>TX: Put("0alice", "70") / Put("1bob", "130")
    Note over TX: buffered locally only, no network I/O

    APP->>TX: Commit(ctx)
    TX->>TX: prewriteWrites: split the write set by group, choose the primary key
    TX->>GA: Prewrite(ops A)
    GA-->>TX: prepared
    TX->>GB: Prewrite(ops B)
    GB-->>TX: prepared
    TX->>ORA: CommitTS(startTS)
    ORA-->>TX: commitTS
    TX->>TX: verifyLocksHeld: confirm every lock is still held
    TX->>GA: Seal(primary, commitTS)
    Note over GA: the atomic commit point
    GA-->>TX: sealed
    TX->>GA: Commit(commitTS, primary)
    TX->>GB: Commit(commitTS, keys)
    GB-->>TX: published
    TX-->>APP: commitTS
```

Step by step, the modules and data structures:

| Step | Module | Call | Data crossing the boundary |
|---|---|---|---|
| 1 | application → `txn.Client` | `Begin(ctx)` | — |
| 2 | `Client` → `tso.Client` | `ReadTS(ctx)` | HTTP + JSON (`NextRequest`/`NextResponse`) |
| 3 | application → `Txn` | `Put` / `Delete` | goes only into `Txn.writes`, **no network at all** |
| 4 | `Txn` → each group | `Participant.Prewrite` per group after `prewriteWrites` | `PrewriteRequest` (containing `Ops`, `Primary`, `StartTS`) |
| 5 | `Txn` → `tso.Client` | `CommitTS(startTS)` | HTTP + JSON |
| 6 | `Txn` → each group | `verifyLocksHeld` → `Participant.Locked` | `CmdQueryLock` |
| 7 | `Txn` → the primary group | `Participant.Seal` | `SealArgs`; **writing into the primary lock is the commit point** |
| 8 | `Txn` → each group | `Participant.Commit` | `CommitRequest`; from there `Store.commit` → `publishVersion` |

**Step 4 expanded inside a group** (group A as the example) — this is where the
algorithm layer is actually put to use:

```
GroupClient.Prewrite        application layer: HTTP client
  → Node.handlePrewrite     application layer: HTTP server, decodes JSON
  → Node.propose            application layer: leader self-check first (saves a log round trip)
  → RaftLog.ApplyCommand    adapter layer: wraps it into a TxnCommand
  → Server.Apply            algorithm layer: propose + wait for the result
      → ConsensusModule.propose        append to the log → trigger replication
      → commitChanSender               once the entry is majority-replicated
          → Store.Apply(TxnCommand)    data state machine: install locks, check conflicts
      → cm.results[index] <- result    hand the answer back
  → render WriteResponse → HTTP response
```

**Why step 7 is the commit point**: `Seal` only writes `commitTS` into the primary
lock's record and **publishes no version**. Once that log entry is applied, any reader
that trips over any one of the transaction's locks can ask the outcome from the primary
key and finish the write itself — that is where the "progress is possible even if the
coordinator dies" mechanism comes from.

### 7.3 The read flow (six steps)

The read flow is shorter than the write flow, but it contains the **read barrier** step
peculiar to this repository:

| Step | Module | Call | Notes |
|---|---|---|---|
| 1 | `Client` → `tso.Client` | `ReadTS` | take the snapshot timestamp S |
| 2 | `Txn` → the target group | `Participant.Barrier` | once per group: wait for one no-op log entry to be applied |
| 3 | after the barrier | `Store.AppliedCommitTS` | the watermark can be used to check; **the barrier itself does not depend on it** |
| 4 | `Txn` → the target group | `Participant.Read(startTS, key)` | take the version under strict visibility `CommitTS < S` |
| 5 | if a lock is tripped over | `Client.resolveLock` → `Resolver` | determine the owner's fate: publish if it committed, otherwise **ask the primary group to record an abort** |
| 6 | retry step 4 | | once the lock is cleared, a clean result is read |

**Why step 2 is necessary**: a read-only transaction sends one Raft round trip per key,
each carrying the same timestamp S. **Raft only guarantees order within each group; it
does not guarantee that two reads in different groups land at the same instant**. The
barrier exploits the property that entries are applied in log order: waiting for one
no-op entry to be applied proves that the group has applied every entry before it; and
any transaction whose commit timestamp is less than S must have had its write enter
that group's log before the barrier. The proof is in
[`docs/03-read-barrier.md`](03-read-barrier.md) §1.2.

**The safety boundary of step 5**: a reader **must not** discard a lock just because its
lease has lapsed. The lease only says that the owner has stopped refreshing the lock; it
does not say that the owner can no longer seal. So the reader must ask the primary group
for a **compare-and-abort**: the primary group re-checks its own state, and if the
transaction has committed it answers "committed", whereupon the reader **publishes**
instead; otherwise it records an abort. This rule is the core of the fix for the
lost-write race in [`docs/03-read-barrier.md`](03-read-barrier.md) §2.

---

## 8 Master table of module associations

All the associations in this document, collected into one table. **This table is the
conclusion of this document**:

| From | To | Association type | Defined in | Implemented in |
|---|---|---|---|---|
| application | `txnservice.Node` | HTTP + JSON | `txnservice/api.go` | `Node.handle*` |
| `txn.Client` | each group | `Participant` interface | `txn/types.go` | `txnservice.GroupClient` (HTTP) / `txn.RaftLog` (in-process) |
| `txn.Client` | timestamp oracle | `Oracle` interface | `txn/types.go` | `tso.Client` |
| `txn.Client` | lock cleanup | `Resolver` interface | `txn/types.go` | `txn.DefaultResolver` |
| `txn.Client` | routing | `Partitioner` interface | `txn/types.go` | `HashPartitioner` / `StaticPartitioner` |
| `txn.Store` | replication | `Log` interface | `txn/store.go` | `txn.RaftLog` |
| `txn.RaftLog` | consensus | `Server.Apply` | `internal/raft/server.go` | — |
| `raft.ConsensusModule` | data state machine | `StateMachineHandler` interface | `internal/raft/raft.go` | `txn.Store` |
| `tso.Service` | consensus | the same `raft.Server` | — | `tso.Oracle` as its handler |
| `cluster.Cluster` | everything | assembly + fault injection | `cluster/cluster.go` | — |

Three patterns run through the whole document:

1. **The interfaces are concentrated in `txn`; the implementations are scattered
   around the periphery.** So swapping the transport layer (HTTP ↔ in-process) needs
   no change to a single line of coordinator code — which is exactly why `cluster` can
   choose freely between the two assemblies.
2. **The algorithm layer touches the outside world at exactly two points**: inbound
   `Server.Apply`, outbound `StateMachineHandler.Apply`. The algorithm layer has no
   idea that what it carries is a transaction, which is also why `tso` can reuse the
   same `raft.Server`.
3. **All cross-group orchestration happens in the coordinator (on the client side).**
   Each Raft group serves only its own slice of the key space, and there is **no**
   direct communication between groups. This is the fundamental difference between
   Multi-Raft and "one Raft group managing all the data", and it is why the whole
   apparatus of 2PC, the primary key and the read barrier exists.

---

## 9 Key code analysis (1): the six gates a request passes before the consensus layer

§3.2 mentions that `handle*` does not call `Store` directly. This section unpacks that
sentence: for an HTTP request to actually enter the Raft log it must pass through six
gates in turn, and **each gate has an explicit reason for rejecting it**.

```go
// txnservice/service.go
func (n *Node) propose(w http.ResponseWriter, req *http.Request, cmd txn.TxnCommand,
	render func(txn.TxnResult) any) {
	if !n.IsLeader() {
		// Avoid one wasted log round trip per request by answering with a routing
		// hint, so the client goes straight to the leader. This is an
		// optimization, not a consistency requirement: Apply would reject the
		// command anyway.
		writeJSON(w, notLeaderPayload(n.LeaderHint(), int(n.groupID)))
		return
	}

	res, err := n.log.ApplyCommand(req.Context(), cmd)
	if err != nil {
		var notLeader *txn.NotLeaderError
		if errors.As(err, &notLeader) {
			writeJSON(w, notLeaderPayload(notLeader.LeaderID, int(notLeader.GroupID)))
			return
		}
		writeJSON(w, internalErrorPayload(err))
		return
	}
	writeJSON(w, render(res))
}
```

There are three easily missed points in these twenty-odd lines:

1. **The `IsLeader()` self-check is a pure optimisation.** The comment says it plainly:
   even without the check, `Server.Apply` would reject a command from a non-leader (it
   re-checks the state itself under the lock). This check only saves one log round trip
   that is doomed anyway. **This matters**: it means the correctness of this fast path
   does not depend on the quality of the `IsLeader()` implementation — it may return a
   stale `true`, and in that case `ApplyCommand` returns `NotLeaderError` and takes the
   branch below.
2. **The two `not-leader` branches do not mean the same thing.** The upper one is
   produced by `Node` itself (the command did not even enter the log); the lower one
   comes from `RaftLog.ApplyCommand` translating `raft.NotLeaderError`. Both return a
   leader hint, so the client need not tell them apart.
3. **`render` is deferred.** The command is replicated and applied first, and only once
   the `TxnResult` is in hand does `render` turn it into the concrete response structure
   (`WriteResponse` or `LockedResponse`). The failure paths do not build a response body
   at all.

Next comes the sixth gate — `RaftLog.ApplyCommand`'s adaptation of `*raft.Server`:

```go
// txn/raftadapter.go
func (rl *RaftLog) ApplyCommand(ctx context.Context, cmd TxnCommand) (TxnResult, error) {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return TxnResult{}, ctx.Err()
		default:
		}
	}

	out, err := rl.server.Apply(cmd)
	if err != nil {
		var notLeader *raft.NotLeaderError
		if errors.As(err, &notLeader) {
			rl.learnLeader(notLeader.LeaderID)
			return TxnResult{}, &NotLeaderError{GroupID: rl.groupID, LeaderID: notLeader.LeaderID}
		}
		return TxnResult{}, err
	}

	// A leader that lost leadership between the check and the apply returns nil:
	// the command was not applied. Report it as a routing failure so the caller
	// retries against the new leader.
	if out == nil {
		return TxnResult{}, &NotLeaderError{GroupID: rl.groupID, LeaderID: rl.LeaderHint()}
	}

	res, ok := out.(TxnResult)
	if !ok {
		return TxnResult{}, fmt.Errorf("txn: group %d: state machine returned %T, want TxnResult",
			rl.groupID, out)
	}
	return res, nil
}
```

This is **the complete content of the inter-layer adaptation**: one type assertion, one
error translation, one boundary case handled. Three points are worth noting:

- **`out == nil` is not an error but "leadership was just lost".** Upstream `Apply`
  returns nil silently on some paths to say that the command was not applied. The
  adapter does not treat it as an empty result; it translates it into `NotLeaderError`
  so that the layer above retries — treated as success, the coordinator would carry on
  on the strength of a write that **never happened**.
- **`learnLeader` is a local cache of the hint.** When a `NotLeaderError` arrives it
  notes the hinted leader id in passing, so the next `LeaderHint()` can answer
  immediately without asking Raft again.
- **A failed type assertion is reported explicitly.** If the state machine returns
  something that is not a `TxnResult`, then some other module has installed itself as
  the `StateMachineHandler` — that is a configuration error, and it has to fail loudly.

---

## 10 Key code analysis (2): the three tables of the transaction state machine

All the data in `Store` is three maps (§5.3). Their **write timing** is completely
different, and that is exactly what the transaction mechanism turns on:

| Table | When it is written | When it is deleted | The question it answers |
|---|---|---|---|
| `versions map[Key][]Version` | `publishVersion`, i.e. **at commit** | never (no garbage collection) | what snapshot S can see |
| `locks map[Key]*LockRecord` | `prewrite`, i.e. **at prepare** | `commit` / `rollback` | is there a write being held back |
| `actors map[TxnID]*Actor` | `actorOrCreate`, created at prepare | never | has this transaction been decided |

### 10.1 prewrite: settle both lock judgements first, then touch the state

```go
// txn/store.go
	expiresAt := s.expiresAt()

	// Validate every write before mutating anything, so the group is never left
	// partially locked by a rejected request. (This mirrors the atomicity
	// assumption internal/raft makes about its own storage: one log entry, one
	// state transition.)
	for _, op := range req.Ops {
		if res, bad := s.checkPrewriteOp(req, op, expiresAt); bad {
			return s.memoize(req.TxnID, req.ClientID, req.RequestID, res)
		}
	}

	// Commit point: install the intent locks.
	for _, op := range req.Ops {
		s.locks[op.Key] = &LockRecord{
			TxnID:     req.TxnID,
			StartTS:   req.StartTS,
			Primary:   req.Primary,
			Kind:      op.Kind,
			Value:     op.Value,
			CommitTS:  0,
			ExpiresAt: expiresAt,
		}
	}
```

**The separation into two steps is deliberate**: validate the whole batch of write
operations first, then install all the locks at once. The comment ties this to the
atomicity assumption `internal/raft` makes about its own storage — "one log entry, one
state transition". If locks were installed while validating, a single rejected request
would leave the group **partially locked**, and that state would be replicated to every
replica.

`CommitTS: 0` is the key: it means "the lock is in place, but the transaction is not yet
decided". Only **sealing** (`seal`) writes a non-zero value there.

### 10.2 The two decision rules must be exact complements

`checkPrewriteOp` contains two judgements, and the relation between them is the core of
the whole isolation model:

```go
// txn/store.go — latestVersionAt (simplified) // simplified
func (s *Store) latestVersionAt(key Key, startTS Timestamp) (Version, bool) {
	versions := s.versions[key]
	best := -1
	for i := range versions {
		if versions[i].CommitTS < startTS {
			best = i
		} else {
			break
		}
	}
	// ...
}
```

```go
// txn/store.go — writeConflict (simplified)
func (s *Store) writeConflict(key Key, startTS Timestamp) (Timestamp, Timestamp, bool) {
	versions := s.versions[key]
	if len(versions) == 0 {
		return 0, 0, false
	}
	newest := versions[len(versions)-1]
	if newest.CommitTS >= startTS {
		// The newest version is not visible to our snapshot. Report the oldest
		// such version, which is the one we are in conflict with.
		for i := range versions {
			if versions[i].CommitTS >= startTS {
				return versions[i].StartTS, versions[i].CommitTS, true
			}
		}
	}
	return 0, 0, false
}
```

Reads use a strict `<`, conflicts use `>=`. **The two must be exact complements**, or a
gap one timestamp wide opens up: some version is **invisible** to that snapshot and yet
does not **block** the write — so the writer overwrites a commit it never saw.

This is pinned down by an explicit comment in the code, and it is the subject of
[`docs/02-transaction-design.md`](02-transaction-design.md) §7.4. Note that the inner
loop in `writeConflict` takes the **oldest** conflicting version rather than the newest:
what should be reported to the caller is "the one you ran into", which is what makes the
error message diagnosable.

### 10.3 seal: write only the decision, publish no data

`seal` is the atomic commit point of the transaction, and it **publishes nothing**:

```go
// txn/store.go — seal (simplified; structure and key branches preserved)
	lock.CommitTS = args.CommitTS
	s.observeCommitTS(args.CommitTS)
	// Sealing extends the lease: the transaction is now decided, and a reader
	// should get a chance to see the decision before anybody force-resolves it.
	lock.ExpiresAt = s.expiresAt()

	actor := s.actorOrCreate(args.TxnID)
	if actor.Primary == "" {
		actor.Primary = lock.Primary
	}
	// Sealing records the transaction's decision. It is the commit point: the
	// group now knows the transaction is committed even though the write is not
	// published yet. The CmdCommit that follows (issued by the coordinator, or by
	// any reader that discovers this decision) publishes it.
	if !actor.State.Final() {
		actor.State = StateCommitted
	}
	if actor.CommitTS == 0 {
		actor.CommitTS = args.CommitTS
	}
```

Three effects, each with a definite purpose:

1. **`lock.CommitTS` becomes non-zero** → any reader that reads this lock learns
   "committed" immediately, without having to ask the primary group.
2. **`actor.State = StateCommitted`** → the primary group has recorded the decision
   itself, so a reader of a secondary lock that comes asking gets an answer.
3. **`lock.ExpiresAt` is extended** → a transaction that has just been decided should
   get a chance to be read out, rather than being force-resolved immediately.

One more gate stands in front of `seal` — **an already-aborted transaction cannot be
sealed** (`StatusAlreadyAborted`). That gate and "a reader may discard a lock only after
the abort decision has landed on disk" corroborate each other: both let **the order
within one and the same Raft log** decide who wins, instead of guessing from timestamps
or leases.

### 10.4 commit: idempotent, and able to tell "just did it" from "done long ago"

```go
// txn/store.go — the tail of commit (simplified) // simplified
		s.publishVersion(key, lock.StartTS, commitTS, lock.Kind, lock.Value)
		delete(s.locks, key)
		committed++
		effectiveTS = commitTS
	}
	// ...
	actor = s.actorOrCreate(req.TxnID)
	actor.State = StateCommitted
	if effectiveTS != 0 {
		actor.CommitTS = effectiveTS
	}

	types := StatusOK
	if actor != nil && actor.State == StateCommitted && committed == 0 {
		// Nothing was published and the decision was already on record: this is a
		// retry of a commit that has fully landed. Report it as such so callers
		// can tell "already done" from "just did it".
		types = StatusAlreadyCommitted
	}
```

`publishVersion` and `delete(s.locks, key)` appear as a pair: **at the very moment a
version is published, its lock disappears**. The two happen in the same log entry, so
they are atomic for every replica.

The distinction between `StatusAlreadyCommitted` and `StatusOK` is not just cosmetic: a
`Commit` request may be re-sent on the network, and the coordinator needs to know whether
"I really published this time" or "somebody already published it" — the former lets it
close the books, the latter says that somebody is already helping to finish up.

---

## 11 Key code analysis (3): the two handshakes between the transaction and the consensus layer

In the eight-step flow of §7.2, step 4 (prewrite) and step 7 (seal) are the two key
points of interaction with the consensus layer. This section lays out one complete round
trip for each of them.

### 11.1 The first handshake: prewrite

```
coordinator                               a Raft group
───────────                               ────────────
Txn.prewrite
 └ Client.prewriteWrites
    │  primaryKeyOf(writes)               ← the primary key is computed locally, no coordination needed
    │  byGroup splits the write set by the partitioner
    └ Participant.Prewrite(req)
       ├─(HTTP path) GroupClient.Prewrite
       │   └ callFor → POST /txn/prewrite/
       │                                  Node.handlePrewrite → propose
       │                                   (1) IsLeader self-check
       └─(in-process path) RaftLog.Prewrite
                                        RaftLog.ApplyCommand
                                         (2) server.Apply(cmd)
                                          ├ cm.propose: append to the log + persist + trigger replication
                                          ├ cm.results[index] = ch
                                          └ wait on ch
                                        commitChanSender (once the entry is replicated to a majority)
                                         (3) handler.Apply(cmd)
                                          └ Store.Apply → Store.prewrite
                                              ├ checkPrewriteOp × len(Ops)
                                              └ install the locks + actor.State = StatePrepared
                                         (4) ch <- proposalResult{value: TxnResult}
       ← WriteResponse{status, prepared}  (5) render(res) → JSON
```

**Every step has a different failure mode, and that is what is worth noting about this
design**:

| Step | On failure | Translated into what, and by whom |
|---|---|---|
| (1) | this node is not the leader | `Node` returns `not-leader` + a hint directly, and produces no log entry |
| (2) | the re-check finds it is no longer the leader | `raft.NotLeaderError` → `txn.NotLeaderError` |
| (2) | no majority | after `ApplyTimeout`, `abandonProposal`; reports "not committed within the time limit" |
| (3) | a write conflict / a lock collision | returns a `TxnResult` **rather than an error**; a business verdict travels in the result, not in the error |
| (4) | the proposer has already given up | the non-blocking delivery is dropped; the applier is unaffected |

**The design stance of step (3) deserves emphasis on its own**: a write conflict and a
lock conflict are both **normal results**, returned through `TxnResult.Status` rather
than as a Go `error`. The line is drawn clearly —

- `error` means **this RPC failed to obtain a verdict** (routing, timeout, shutdown),
  and the caller should retry;
- `TxnResult.Status` means **a verdict was obtained, and the verdict is no** (a conflict,
  a lock held by someone else), and the caller should change its behaviour accordingly.

Mix the two together and the coordinator can no longer tell "a retry may succeed" from "a
retry is certain to fail". §11.3 shows how that distinction is used directly in the retry
decision.

### 11.2 The second handshake: seal and commit

Sealing takes exactly the same path as prewrite, only with a different command:

```
Txn.commitPrimary(commitTS, prepared)
 ├ participant.Seal(primaryKey, id, startTS, commitTS)
 │   └ TxnCommand{Kind: CmdSeal, SealArgs: &SealArgs{...}}
 │      └ …same as steps (2)~(4) of §11.1…
 │         Store.seal: lock.CommitTS = commitTS
 │                    actor.State  = StateCommitted   ← the transaction commits here
 │                    lock.ExpiresAt extended
 └ participant.Commit(CommitRequest{Keys: [primaryKey]})
     └ TxnCommand{Kind: CmdCommit, ...}
        Store.commit: publishVersion + delete(lock)   ← the primary-key write becomes visible
```

**Sealing and committing are two independent log entries, and the split is deliberate.**
The two differ in cost and in consequence:

- `seal` needs only one log entry and an $O(1)$ state change. It decides the fate of the
  transaction. **Whether the transaction has committed when it fails depends on whether
  it was accepted by a majority** — which is exactly why `CommitUnknownError` exists.
- `commit` is what actually writes versions. It has to read the lock record and append to
  the version chain in `CommitTS` order. **Its failure does not affect the fact that the
  transaction has committed**, because any reader can complete it itself from the sealed
  lock.

So a failure of `commitPrimary` is translated into `CommitUnknownError` ("committed, but
possibly not finished") rather than into an ordinary error:

```go
	// The atomic commit point.
	if err := t.commitPrimary(ctx, commitTS, prepared); err != nil {
		t.state = stateCommitted
		t.committedTS = commitTS
		return commitTS, &CommitUnknownError{TxnID: t.id, CommitTS: commitTS, Err: err}
	}
```

Note that `t.state = stateCommitted` is set **before the error is returned**. That looks
counter-intuitive, but it is right: once the seal has been proposed, the transaction can
no longer be rolled back. `Txn.Finish`, or any reader, completes the secondary writes
afterwards.

### 11.3 The complete closure of failure handling

Stringing the two handshakes above together, the closure of failure handling looks like
this:

```mermaid
flowchart TB
    A["Txn.Commit"] --> B["prewrite to each group"]
    B -->|"failure"| C["abortLockedGroups<br/>abort the primary first, then release the secondary locks"]
    B -->|"success"| D["oracle.CommitTS"]
    D -->|"failure"| C
    D -->|"success"| E["verifyLocksHeld"]
    E -->|"lock already lost"| C
    E -->|"lock still held"| F["Seal (the primary key)"]
    F -->|"error"| G["CommitUnknownError<br/>t.state = committed"]
    F -->|"success"| H["Commit the primary + each secondary key"]
    H -->|"partial failure"| G
    G --> I["Txn.Finish<br/>re-send with the recorded commitTS"]
    C --> J["LockRolledBackError<br/>retryable"]
    I -->|"success"| K["done"]
    I -->|"failure"| L["reader resolution as the fallback"]
```

The meaning of the three exits:

| Exit | The transaction's final state | What the caller should do |
|---|---|---|
| ordinary success | committed, every write visible | carry on |
| `LockRolledBackError` | **not committed**, no write published | retry the whole transaction (a new transaction, new timestamps) |
| `CommitUnknownError` | **committed**, possibly with writes not published | call `Txn.Finish` to complete it; **never retry the whole transaction** |

**The last row is the easiest place in this design to get wrong.** Once a transaction has
been sealed, retrying it amounts to letting the same batch of writes be applied twice —
and its write set may be "add 1 to the balance". The job of `Txn.Finish` and of reader
resolution is precisely to turn "committed but not finished" into a state that is
**terminating**, rather than one the caller has to guess about.

The position of `verifyLocksHeld` is equally key: it sits **after the commit timestamp is
taken and before the seal**:

```go
// txn/client.go — the lock check in Txn.Commit (simplified; some comment lines elided)
// Every lock must still be in place before the commit point is written.
// ...
// Sealing later cannot repair that, so the check has to happen first. If any
// lock is gone, no commit point has been written and no version published, so
// the transaction is still entirely invisible and aborting is safe.
if err := t.verifyLocksHeld(ctx, prepared); err != nil {
    _ = t.abortLockedGroups(ctx)
    t.state = stateAborted
    return 0, err
}
```

The reasoning in the comment is worth reading sentence by sentence: **the check has to
happen before the seal, because sealing afterwards cannot repair a write that has already
been dropped**; and aborting when the check fails is **safe**, because no commit point has
been written and no version published — the transaction is still entirely invisible.
Together those two sentences are the complete statement of the precondition for fixing
the lost-write race in [`docs/03-read-barrier.md`](03-read-barrier.md) §2.

---

## 12 Summary of differences from the reference article

| Dimension | The reference article (etcd v3.1.10) | This repository |
|---|---|---|
| Cross-layer communication | channels + `Ready`/`Advance`, asynchronous | `Server.Apply`, synchronous |
| Is there a `Ready` structure | yes; it carries `SoftState`/`HardState`/`Entries`/`CommittedEntries`/`Messages` | **no**; this round's products are carried by `proposalResult` + the `StateMachineHandler` return value |
| What drives the timer | the application layer's `ticker` calls `Node.Tick()` | the election timer inside `ConsensusModule` |
| Where the state machine is applied | the application layer reads `commitChan` itself and then applies | `commitChanSender` applies on the commit path |
| Node state-machine entry point | unified `Step(Message)` + dispatch by role | three purpose-built entry points (`Apply` / RPC / timer) |
| Role state field | `StateType` + `step`/`tick` function pointers | `CMState`, no function pointers |
| Read consistency | `ReadIndex` + a `readOnly` queue + heartbeat ack counting | the `CmdBarrier` no-op entry + the applied watermark |
| Transactions | single group (v3.1) | Multi-Raft cross-group 2PC, primary key + intent locks + lock resolution |
| Log storage | a two-tier `storage` + `unstable`, plus snapshot compaction | a single `Storage` interface, whole-log gob encoding, no compaction |

**In one sentence**: the etcd of the reference article separates the algorithm layer
from the application layer with "channels + `Ready`/`Advance`", which is
**asynchronous decoupling**; this repository joins the two with "`Server.Apply` +
`StateMachineHandler`", which is **synchronous convergence**. The former buys
extensibility and non-blocking behaviour; the latter buys a call site that is plain —
the coordinator gets the consensus result much as it would from a local function call,
at the cost of one waiting goroutine per proposal and of needing the `commitChan` drain
channel to catch the leftovers.

---

## 13 How to verify the claims in this document yourself

Every structural claim in this document can be checked against the source, and most
have a corresponding test:

| Claim | How to check it |
|---|---|
| The state machine is applied outside the consensus lock | read `commitChanSender` in `internal/raft/raft.go`: `cm.mu.Unlock()` comes before `handler.Apply` |
| `commitChan` no longer drives the state machine | read `drainCommits` in `txnservice/service.go` and its comment |
| The two `Participant` paths are equivalent | compare the like-named methods in `txnservice/client.go` and `txn/raftadapter.go` |
| The seal is the commit point | `seal` in `txn/store.go`; the test `TestSecondaryLockIsFinishedAfterPrimaryCommits` |
| The barrier takes effect by log position | `CmdBarrier` in `txn/store.go`; the test `TestReadBarrierEstablishesAppliedPrefix` |
| A reader must not roll back on the strength of a lease | `ensurePrimaryResolved`/`abortAtPrimary` in `txn/client.go`; the test `TestSealedSecondaryIsNotLostWhenALeaseLapses` |
| Cross-group atomicity | the test `TestCommitTimestampsPairUpAcrossGroups` |
| The `IsLeader()` self-check is only an optimisation | the `Node.propose` comment quoted in §9, and the state re-check inside `Server.Apply` |
| Read visibility and write conflict are exact complements | compare `latestVersionAt` (`<`) and `writeConflict` (`>=`) in `txn/store.go`; the test `TestStoreNoLostUpdateAtTheSnapshotBoundary` |
| prewrite validates before it installs locks | the `txn/store.go` excerpt in §10.1: the validation loop comes **before** the locking loop |
| seal publishes no version | `seal` in `txn/store.go` writes only `lock.CommitTS` and `actor.State`; it does not call `publishVersion` |
| The failure exits mean different things | `LockRolledBackError` and `CommitUnknownError` in `txn/errors.go`, and the points in `Txn.Commit` where each is returned |

This document's **code quotations** and **diagrams** are likewise checked by scripts
rather than trusted:

```sh
# every ```go block must appear verbatim in the sources (unless it carries its own
# -- simplified marker, and even then every line it shows must really exist)
node tools/doc-check/verify-quoted-code.js docs/05-workflow-analysis.md .

# structure: code blocks, mermaid diagrams, headings, links, inline code spans
node tools/doc-check/verify-translation.js docs/05-workflow-analysis.md docs/05-workflow-analysis.zh-CN.md

# mermaid grammar
node tools/doc-check/check-mermaid.js docs/05-workflow-analysis.md
```

Each of these three scripts has found real errors — for example this document once gave
the value type of `raft.Storage.Set` as `any` (it is `[]byte`), and one quotation from
`commitChanSender` wrote `entries :=` where the source has `entries =`. Both are fixed.

If this document and the source disagree, **the source wins**; please open an issue or
fix this document directly.
