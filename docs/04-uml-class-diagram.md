# 04 — Architecture: UML class diagrams

> 中文版：[`04-uml-class-diagram.zh-CN.md`](04-uml-class-diagram.zh-CN.md)

This document is the structural companion to docs/01–03. Those explain *why* the
system is built the way it is; this one shows *what the parts are and how they fit
together*.

Every type, field and relationship below was read out of the source rather than
recalled, and every diagram is validated: a script checks that the mermaid blocks
parse, that no class name is invented, and that no method name is invented. See §9
for exactly what that covers and what it does not.

Layer boundaries are the package boundaries, which are also the dependency
direction: nothing in a lower layer imports a higher one.

## 1. Layers and dependencies

```
                     cmd/demo            cluster
                        |                  |
                        +--------+---------+
                                 v
                          txnservice ------> txn
                                 |             ^
                                 v             |
                          internal/raft        |
                                               |
                              tso -------------+
                    (tso imports txn only for Timestamp and Command)
```

Read it as: the transaction protocol (`txn`) is the centre. It defines every
interface, and every other package either implements one of those interfaces
(`txnservice`, `internal/raft`, `tso`) or wires implementations together
(`cluster`, `cmd/demo`). `txn` imports nothing else from this repository except
`internal/raft`, and only in `raftadapter.go`, to adapt a `*raft.Server` into the
`Log` and `Participant` interfaces.

## 2. Core vocabulary — `package txn`

The shared types every other layer speaks in. These are the names that recur in the
protocol diagrams below.

```mermaid
classDiagram
    direction LR

    class Timestamp {
        <<uint64>>
    }
    class TxnID {
        <<uint64>>
    }
    class Key {
        <<string>>
        +Encode() List~byte~
        +Clone() Key
    }
    class Value {
        <<string>>
    }
    class GroupID {
        <<int>>
    }
    class OpKind {
        <<int>>
        OpPut
        OpDelete
    }
    class State {
        <<int>>
        StateInvalid
        StatePending
        StatePrepared
        StateCommitted
        StateAborted
        +Final() bool
    }

    class WriteOp {
        +OpKind Kind
        +Key Key
        +Value Value
    }
    class Version {
        +Timestamp CommitTS
        +Timestamp StartTS
        +OpKind Kind
        +Value Value
        +Visible(ts) bool
    }
    class LockRecord {
        +TxnID TxnID
        +Timestamp StartTS
        +Key Primary
        +OpKind Kind
        +Value Value
        +Timestamp CommitTS
        +uint64 ExpiresAt
        +Expired(nowMillis) bool
        +InfoForKey(key) LockInfo
        +Info() LockInfo
    }
    class LockInfo {
        +bool Exists
        +Key Key
        +TxnID TxnID
        +Key Primary
        +Timestamp StartTS
        +Timestamp CommitTS
        +OpKind Kind
        +uint64 ExpiresAt
        +Timestamp ActorCommitTS
        +State State
        +TxnLock() TxnLock
        +Expired(nowMillis) bool
    }
    class Actor {
        +TxnID TxnID
        +State State
        +Key Primary
        +Timestamp CommitTS
        +List~GroupID~ PreparedShards
        +List~Key~ Keys
        -map~int64,TxnResult~ applied
    }

    WriteOp --> OpKind
    WriteOp --> Key
    Version --> OpKind
    LockRecord --> OpKind
    LockRecord ..> LockInfo : produces
    Actor --> State
    Actor --> Timestamp
```

**Why these records are separate.** `Version` is history: a chain of them per key is
the MVCC store. `LockRecord` is the in-flight write, holding a version back until
the transaction decides. `Actor` is the transaction's own state *as one group sees
it*, and it is what a reader consults to learn whether a lock it tripped over
belongs to a live, committed or dead transaction. `LockRecord.CommitTS` answers that
for one key; `Actor` answers it for the whole transaction, which is what a reader of
a *secondary* lock needs.

## 3. The transaction protocol — `package txn`

Interfaces are the contract; the concrete types below them are the implementations,
which live in other packages.

```mermaid
classDiagram
    direction TB

    class Oracle {
        <<interface>>
        +ReadTS(ctx) Timestamp,error
        +CommitTS(ctx, startTS) Timestamp,error
        +Close() error
    }
    class Partitioner {
        <<interface>>
        +GroupForKey(key) GroupID
    }
    class Log {
        <<interface>>
        +ApplyCommand(ctx, cmd) TxnResult,error
        +Leader() int
    }
    class Participant {
        <<interface>>
        +Read(ctx, startTS, key) Value,error
        +Prewrite(ctx, req) error
        +Seal(ctx, key, txnID, startTS, commitTS) error
        +Commit(ctx, req) error
        +Rollback(ctx, req) error
        +Locked(ctx, txnID, key) LockInfo,error
        +Barrier(ctx) error
    }
    class Resolver {
        <<interface>>
        +ResolveLock(ctx, group, lock) error
    }
    class LeaderHinter {
        <<interface>>
        +LeaderHint() int
    }

    class OracleFunc {
        +ReadNow Timestamp
        +CommitNow Timestamp
    }
    class LocalOracle {
        -Timestamp last
    }
    class HashPartitioner {
        +int Groups
    }
    class StaticPartitioner {
        +map~Key,GroupID~ Groups
    }
    class DefaultResolver {
        -Client client
    }

    Oracle <|.. OracleFunc
    Oracle <|.. LocalOracle
    Partitioner <|.. HashPartitioner
    Partitioner <|.. StaticPartitioner
    Resolver <|.. DefaultResolver
```

`Participant` is the RPC surface of one Raft group, and it is the most important
interface here: it is implemented twice (over HTTP in §5, in-process in §6), and the
coordinator is written against the interface so both work unchanged. `Log` is the
narrower thing the *state machine* needs: replicate a command, and ask who leads.

`DefaultResolver` is the recovery policy. Given a lock found in some group, it works
out the owning transaction's fate from its **primary** group and then either
finishes the write or discards it. It holds a `*Client` because it reuses the
coordinator's probing machinery, which is why the two are bound to each other after
construction rather than passed in.

## 4. The coordinator and the state machine

```mermaid
classDiagram
    direction TB

    class Client {
        -Oracle oracle
        -Partitioner partitioner
        -map~GroupID,Participant~ participants
        -Resolver resolver
        -Duration resolveTimeout
        -func() uint64 now
        -int64 clientID
        -atomic.Int64 requests
        -atomic.Uint64 txns
        +Begin(ctx) Txn,error
        +ClientID() int64
        +Oracle() Oracle
        +SetResolver(r)
        +SetClock(fn)
        +SetResolveTimeout(d)
        +Stats() ResolutionStats
        -prewriteWrites(ctx, id, startTS, writes) prepResult,error
        -probePrimary(ctx, group, txnID, primary) State,Timestamp,LockInfo,error
        -ensurePrimaryResolved(ctx, lock) State,Timestamp,error
        -abortAtPrimary(ctx, group, txnID, startTS, primary) error
        -resolveLock(ctx, group, lock) error
        -commitPrimary(ctx, commitTS, res) error
        -commitSecondary(ctx, group, commitTS) error
    }

    class Txn {
        -Client c
        -TxnID id
        -Timestamp startTS
        -map~Key,WriteOp~ writes
        -map~GroupID,struct~ prepared
        -Key primaryKey
        -Timestamp committedTS
        -state state
        -map~GroupID,struct~ barriered
        +ID() TxnID
        +StartTS() Timestamp
        +State() string
        +Writes() List~WriteOp~
        +Get(ctx, key) Value,bool,error
        +Put(key, value) error
        +Delete(key) error
        +Commit(ctx) Timestamp,error
        +Finish(ctx) error
        +Abort(ctx) error
        -ensureReadable(ctx, group, participant) error
        -verifyLocksHeld(ctx, res) error
        -prewrite(ctx) prepResult,error
        -abortLockedGroups(ctx) error
    }

    class Store {
        -Log log
        -Duration lockTTL
        -func() uint64 nowMillis
        -map~Key,List~Version~~ versions
        -map~Key,LockRecord~ locks
        -map~TxnID,Actor~ actors
        -Timestamp appliedCommitTS
        +Apply(command) any
        +SetLockTTL(d)
        +SetClock(fn)
        +Keys() List~Key~
        +Locks() map~Key,TxnID~
        +LockFor(key) LockInfo
        +LockedKeys() List~Key~
        +ValueAtNow(key) Value,bool
        +ValueAt(key, ts) Value,bool
        +VersionsFor(key) List~Version~
        +AppliedCommitTS() Timestamp
        +Stats() Stats
        -prewrite(req) TxnResult
        -seal(args) TxnResult
        -commit(req) TxnResult
        -rollback(req) TxnResult
        -resolveLock(args) TxnResult
        -queryLock(args) TxnResult
        -read(args) TxnResult
        -checkPrewriteOp(req, op, expiresAt) TxnResult,bool
        -writeConflict(key, startTS) Timestamp,Timestamp,bool
        -latestVersionAt(key, startTS) Version,bool
        -publishVersion(key, startTS, commitTS, kind, value)
        -observeCommitTS(commitTS)
    }

    class prepResult {
        +GroupID primaryGroup
        +Key primaryKey
        +List~GroupID~ groups
    }
    class ResolutionStats {
        +int LocksResolved
        +int StaleLocksAborted
        +int ForeignLocksFinished
        +int WriteConflicts
    }
    class Stats {
        +int Keys
        +int Versions
        +int Locks
        +int Actors
        +int Committed
        +int Aborted
    }
    class TxnCommand {
        +TxnCommandKind Kind
        +ReadArgs ReadArgs
        +PrewriteRequest PrewriteArgs
        +SealArgs SealArgs
        +CommitRequest CommitArgs
        +RollbackRequest RollbackArgs
        +ResolveLockArgs ResolveLockArgs
        +QueryLockArgs QueryLockArgs
    }

    Client "1" *-- "many" Txn : creates
    Txn --> Client : back-reference c
    Txn ..> prepResult : prewrite returns
    Client ..> prepResult
    Client ..> ResolutionStats
    Client --> Oracle
    Client --> Partitioner
    Client --> Resolver
    Client o-- Participant : participants
    Store --> Log
    Store ..> Stats
    Store o-- Version : versions
    Store o-- LockRecord : locks
    Store o-- Actor : actors
    Store ..> TxnCommand : applies
    Store ..> LockInfo
```

`Client` holds no per-transaction state and is safe for concurrent use; `Txn` holds
all of it and is explicitly **not** safe for concurrent use. That split is why one
`Client` serves many transactions.

`Store` is the replicated state machine. `Apply` is its only entry point: every
mutation arrives as a `TxnCommand` that Raft has already ordered, so the state
machine never sees two concurrent writers and needs no internal locking of its own.

## 5. The service layer — `package txnservice`

How a group is reached over HTTP, and how a group member is assembled from
consensus, a state machine, and an HTTP surface.

```mermaid
classDiagram
    direction TB

    class Node {
        -GroupID groupID
        -int nodeID
        -Log log
        -Store store
        -http.Server httpServer
        +GroupID() txn.GroupID
        +NodeID() int
        +Store() *txn.Store
        +Participant() txn.Participant
        +Addr() string
        +RaftListenAddr() net.Addr
        +IsLeader() bool
        +LeaderHint() int
        +ConnectToPeer(peerID, addr) error
        +DisconnectFromPeer(peerID) error
        +DisconnectFromAllPeers()
        +Shutdown() error
        -serveHTTP(addr) error
        -handleRead(w, req)
        -handlePrewrite(w, req)
        -handleSeal(w, req)
        -handleCommit(w, req)
        -handleRollback(w, req)
        -handleLocked(w, req)
        -handleBarrier(w, req)
        -handleStatus(w, req)
        -propose(w, req, cmd, render)
        -decode(w, req, target) bool
    }

    class NodeConfig {
        +GroupID GroupID
        +int NodeID
        +List~int~ PeerIDs
        +Storage Storage
        +string HTTPAddr
        +Duration LockTTL
    }

    class GroupClient {
        -GroupID groupID
        -List~string~ addrs
        -int assumedLeader
        -int leaderNode
        -Duration timeout
        +GroupID() txn.GroupID
        +Read(ctx, startTS, key) txn.Value,error
        +Prewrite(ctx, req) error
        +Seal(ctx, key, txnID, startTS, commitTS) error
        +Commit(ctx, req) error
        +Rollback(ctx, req) error
        +Locked(ctx, txnID, key) *txn.LockInfo,error
        +Barrier(ctx) error
        +Status(ctx) StatusResponse,error
        +LeaderHint() int
        +SetTimeout(d)
        -setLeader(node)
        -rotate()
        -start() int
        -callFor(ctx, route, reqData, respData) error
        -doPost(ctx, url, reqData) List~byte~,error
    }

    class Client {
        -map~txn.GroupID,GroupClient~ groups
        +Participants() map~txn.GroupID,txn.Participant~
        +ParticipantFor(id) txn.Participant,error
        +Group(id) *GroupClient,bool
        +ResolveLock(ctx, group, lock) error
        +BindResolver(r)
    }

    class ClientConfig {
        +map~txn.GroupID,List~string~~ Groups
        +Duration Timeout
    }

    class ResponseStatus {
        +string Status
        +int LeaderID
        +string Error
    }
    class ReadRequest {
        +uint64 StartTS
        +string Key
    }
    class WriteOpJSON {
        +int Kind
        +string Key
        +string Value
    }
    class PrewriteRequest {
        +uint64 TxnID
        +int64 ClientID
        +int64 RequestID
        +uint64 StartTS
        +string Primary
        +List~WriteOpJSON~ Ops
        +uint64 CommitTS
        +bool OneShot
    }
    class SealRequest {
        +string Key
        +uint64 TxnID
        +uint64 StartTS
        +uint64 CommitTS
    }
    class CommitRequest {
        +uint64 TxnID
        +int64 ClientID
        +int64 RequestID
        +uint64 StartTS
        +uint64 CommitTS
        +List~string~ Keys
    }
    class RollbackRequest {
        +uint64 TxnID
        +int64 ClientID
        +int64 RequestID
        +uint64 StartTS
        +List~string~ Keys
        +bool RecordDecision
    }
    class LockedRequest {
        +uint64 TxnID
        +string Key
    }
    class BarrierRequest {
        <<empty>>
    }
    class ReadResponse {
        +ResponseStatus ResponseStatus
        +string Value
        +bool Found
        +LockInfo Lock
        +uint64 MinCommitTS
    }
    class WriteResponse {
        +ResponseStatus ResponseStatus
        +int State
        +uint64 CommitTS
        +bool Prepared
        +int KeysFinalized
        +string ConflictKey
        +uint64 ConflictStart
        +uint64 ConflictCommit
        +LockInfo Lock
        +uint64 MinCommitTS
    }
    class LockedResponse {
        +ResponseStatus ResponseStatus
        +int State
        +LockInfo Lock
    }
    class StatusResponse {
        +ResponseStatus ResponseStatus
        +int NodeID
        +int GroupID
        +int LeaderID
        +bool Leader
    }
    class LockInfo {
        +bool Exists
        +string Key
        +uint64 TxnID
        +string Primary
        +uint64 StartTS
        +uint64 CommitTS
        +int Kind
        +uint64 ExpiresAt
        +uint64 ActorCommitTS
        +int State
    }

    Node "1" *-- "1" NodeConfig : built from
    Node "1" o-- "1" Store : owns
    Node "1" o-- "1" Log : replicates through
    Node ..> ReadRequest
    Node ..> WriteResponse
    Node ..> LockedResponse
    Node ..> BarrierRequest
    GroupClient ..> ReadRequest
    GroupClient ..> PrewriteRequest
    GroupClient ..> CommitRequest
    GroupClient ..> RollbackRequest
    GroupClient ..> BarrierRequest
    GroupClient ..> ReadResponse
    GroupClient ..> WriteResponse
    Client "1" *-- "many" GroupClient : groups
    Client "1" *-- "1" ClientConfig : built from
    ReadResponse --|> ResponseStatus
    WriteResponse --|> ResponseStatus
    LockedResponse --|> ResponseStatus
    StatusResponse --|> ResponseStatus
    ReadResponse --> LockInfo
    WriteResponse --> LockInfo
    LockedResponse --> LockInfo
```

`Node` and `GroupClient` are the two ends of one wire protocol: the `*Request` types
are what `GroupClient` sends and what `Node.handle*` decodes, and the `*Response`
types are what the handler writes and the client parses. Every response embeds
`ResponseStatus`, so a client can recognise `not-leader` and follow the hint without
knowing which handler answered.

## 6. Consensus and the timestamp oracle

```mermaid
classDiagram
    direction TB

    class Server {
        -ConsensusModule cm
        -net.Listener listener
        -chan struct quit
        -map~int,rpc.Client~ peerClients
        -chan struct acceptDone
        +Serve()
        +Apply(cmd) any,error
        +Submit(cmd) int
        +SetHandler(h)
        +SetDebug(enabled)
        +IsLeader() bool
        +LeaderID() int
        +CommitIndex() int
        +GetListenAddr() net.Addr
        +ConnectToPeer(id, addr) error
        +DisconnectPeer(id) error
        +DisconnectAll()
        +Shutdown()
        -trackConn(conn, add)
        -closeConns()
        -wakeAcceptLoop()
    }

    class ConsensusModule {
        -int id
        -List~int~ peerIds
        -CMState state
        -int currentTerm
        -int votedFor
        -List~LogEntry~ log
        -int commitIndex
        -int lastApplied
        -StateMachineHandler handler
        -map~int,chan proposalResult~ results
        -chan struct newCommitReadyChan
        -chan struct stopChan
        -int leaderId
        +propose(cmd) int
        +waitApplied(index, timeout) any,error
        +abandonProposal(index, err)
        +SetHandler(h)
        +SetDebug(enabled)
        +LeaderID() int
        +CommitIndex() int
        +Submit(cmd) int
        +Stop()
        +Report() int,int,bool
        +RequestVote(args, reply) error
        +AppendEntries(args, reply) error
        -spawnElectionTimer()
        -commitChanSender()
        -runElectionTimer()
        -startElection()
        -becomeFollower(term)
        -startLeader()
        -leaderSendAEs()
        -triggerReplication()
        -persistToStorage()
        -restoreFromStorage()
    }

    class RPCProxy {
        +RequestVote(args, reply) error
        +AppendEntries(args, reply) error
    }
    class StateMachineHandler {
        <<interface>>
        +Apply(command) any
    }
    class Storage {
        <<interface>>
        +Set(key, value)
        +Get(key) any,bool
    }
    class MapStorage {
        -map~string,any~ data
    }
    class LogEntry {
        +any Command
        +int Term
    }
    class CommitEntry {
        +any Command
        +int Index
        +int Term
    }
    class proposalResult {
        +any value
        +error err
    }
    class AppendEntriesArgs {
        +int Term
        +int LeaderId
        +int PrevLogIndex
        +int PrevLogTerm
        +List~LogEntry~ Entries
        +int LeaderCommit
    }
    class RequestVoteArgs {
        +int Term
        +int CandidateId
        +int LastLogIndex
        +int LastLogTerm
    }
    class NotLeaderError {
        +int LeaderID
    }

    Server "1" *-- "1" ConsensusModule : owns
    Server "1" *-- "1" RPCProxy : serves
    Server ..> CommitEntry : commit channel
    Server ..> NotLeaderError
    ConsensusModule ..> StateMachineHandler : applies commits
    ConsensusModule o-- LogEntry : log
    ConsensusModule o-- proposalResult : outstanding proposals
    ConsensusModule ..> Storage : persists to
    Storage <|.. MapStorage
    RPCProxy ..> AppendEntriesArgs
    RPCProxy ..> RequestVoteArgs
```

`StateMachineHandler` is the seam between consensus and the transaction layer:
`txn.Store.Apply` is what gets installed, so a committed log entry *is* a
transactional state-machine command. `Server.Apply` is the synchronous path built on
top — propose, then wait for the applier to hand back a result — which is the local
extension that lets a coordinator get an answer instead of merely a log index.

The timestamp oracle reuses the same consensus module with its own state machine:

```mermaid
classDiagram
    direction TB

    class Oracle {
        -uint64 allocated
        -uint64 windowSize
        -uint64 nextIssued
        -uint64 granted
        -uint64 remaining
        -uint64 lastIssued
        -map~int,intent~ intents
        +Apply(command) any
        +Next(count) uint64,uint64
        +Allocated() uint64
        +LastIssued() uint64
        +Available() uint64
        +WindowSize() uint64
        -reserve(nodeID) Result
        -allocate(nodeID, count) Result
        -take() uint64,bool
        -setNode(nodeID)
        -beginServing(nodeID)
    }
    class Service {
        -int nodeID
        -Oracle oracle
        +NodeID() int
        +Oracle() *Oracle
        +Next(ctx, count) uint64,uint64,error
        +Addr() string
        +RaftListenAddr() net.Addr
        +IsLeader() bool
        +Shutdown() error
        -reserve() error
        -serveHTTP(addr) error
        -handleNext(w, req)
        -handleStatus(w, req)
    }
    class Client {
        -List~string~ addrs
        -int leader
        +ReadTS(ctx) txn.Timestamp,error
        +CommitTS(ctx, startTS) txn.Timestamp,error
        +Close() error
        -next(ctx) uint64,error
        -post(ctx, url, reqData) NextResponse,error
        -leaderIndex() int
        -setLeader(idx)
        -rotate()
    }
    class Command {
        +CommandKind Kind
        +int NodeID
        +uint64 Count
    }
    class Result {
        +uint64 First
        +uint64 Last
    }
    class intent {
        +uint64 first
        +uint64 last
    }
    class NextRequest {
        +uint64 Count
    }
    class NextResponse {
        +string Status
        +uint64 First
        +uint64 Last
        +int LeaderID
    }

    Service "1" *-- "1" Oracle : owns
    Oracle o-- intent : per-member reservations
    Oracle ..> Result : allocate returns
    Service ..> Command : proposes
    Service ..> NextResponse
    Client ..> NextRequest
    Client ..> NextResponse
```

The oracle's `intents` map is the part worth noticing. Each member *reserves* a
window of timestamps and may spend only from its own reservation. That is what makes
a leader change safe: a new leader cannot spend its predecessor's window, because
the reservations are part of the replicated state. Timestamps may therefore be
wasted, but never repeated.

## 7. Test harness — `package cluster`

Not production code, but part of the architecture: it is how everything above is
exercised and how the demo is run.

```mermaid
classDiagram
    direction TB

    class Cluster {
        -Config cfg
        -List~List~Node~~ nodes
        -List~List~MapStorage~~ storages
        -List~List~bool~~ alive
        -List~List~bool~~ connected
        -List~Service~ tsoNodes
        -List~MapStorage~ tsoStorages
        -List~bool~ tsoAlive
        -List~bool~ tsoConnected
        -Client clients
        -Client txnClient
        +New(cfg) *Cluster,error
        +Client() *txn.Client
        +GroupNode(group, node) *txnservice.Node
        +TSOService(node) *tso.Service
        +NumGroups() int
        +LeaderOf(group) int
        +TSOLeader() int
        +WaitForLeaders(timeout) error
        +CrashNode(group, node) error
        +RestartNode(group, node) error
        +CrashTSO(node) error
        +RestartTSO(node) error
        +DisconnectNode(group, node)
        +ReconnectNode(group, node)
        +Shutdown()
        +Put(key, value) txn.Timestamp,error
        +Delete(key) txn.Timestamp,error
        +Read(key) txn.Value,bool,error
        +WaitForRead(key, timeout) txn.Value,bool,error
        +WaitForSettled(timeout) error
        +WaitForQuiescent(stable, timeout) error
        +Barrier(group) txn.Timestamp,error
        +Barriers() txn.Timestamp,error
        +SealPrimary(tx, primaryKey, group, node) txn.Timestamp,error
        +RunTxn(ctx, attempts, fn) txn.Timestamp,error
        +DumpTopology() List~string~
        +Snapshot(group, node) StoreSnapshot,error
        +SetRaftLogging(enabled)
        -settled() error
    }
    class Config {
        +int NumGroups
        +int NodesPerGroup
        +int TSONodes
        +uint64 WindowSize
        +Duration LockTTL
        +int HTTPBase
        +txn.Partitioner Partitioner
    }
    class StoreSnapshot {
        +map~txn.Key,txn.Value~ Values
        +map~txn.Key,txn.TxnID~ Locks
        +uint64 AppliedCommitTS
    }

    Cluster "1" *-- "1" Config : built from
    Cluster "1" *-- "many" Node : owns
    Cluster "1" *-- "many" Service : owns oracle members
    Cluster "1" *-- "1" Client : wires
    Cluster ..> StoreSnapshot : inspects
```

## 8. The commit path, as a sequence

Class diagrams show structure; this shows the order the structure is used in, for
one cross-group transaction.

```mermaid
sequenceDiagram
    autonumber
    participant App
    participant Txn
    participant Client
    participant Oracle
    participant GA as Group A primary
    participant GB as Group B

    App->>Client: Begin()
    Client->>Oracle: ReadTS()
    Oracle-->>Client: startTS
    Client-->>App: Txn at startTS

    Note over App,Txn: reads and writes are buffered locally
    App->>Txn: Get(key)
    Txn->>GA: Barrier()
    GA-->>Txn: applied
    Txn->>GA: Read(startTS, key)
    GA-->>Txn: value

    App->>Txn: Commit()
    Txn->>GA: Prewrite(ops A)
    GA-->>Txn: prepared
    Txn->>GB: Prewrite(ops B)
    GB-->>Txn: prepared

    Txn->>Oracle: CommitTS(startTS)
    Oracle-->>Txn: commitTS

    Txn->>Txn: verifyLocksHeld on every group
    Txn->>GA: Seal(primary, commitTS)
    Note over GA: atomic commit point
    GA-->>Txn: sealed
    Txn->>GA: Commit(commitTS, primary)
    Txn->>GB: Commit(commitTS, keys)
    GB-->>Txn: published
    Txn-->>App: commitTS
```

The line that matters most is `Seal(primary, commitTS)`. Until it lands the
transaction can still be aborted; once it lands, any reader that trips over one of
the transaction's locks can learn the outcome and finish the write without the
coordinator's help.

## 9. How these diagrams were validated

Diagrams are easy to get subtly wrong, and a diagram that does not render documents
nothing. Three mechanical checks were run, and they are worth stating precisely
because they also bound what this document claims.

**1. The mermaid blocks parse.** A script checks each `classDiagram` and
`sequenceDiagram` block against its structural rules: the diagram keyword first,
balanced class bodies, every member line matching one of mermaid's member forms, and
every relationship using a real operator. It is stricter than mermaid on purpose —
it reports unrecognised lines rather than skipping them — because mermaid renders an
error box where a strict check fails loudly.

This caught 32 real problems in the first draft. Two kinds:

* **Go slice syntax is not mermaid.** I had written `+[]Key Keys` and
  `+Writes() []WriteOp`. Mermaid's grammar has no `[]` member form; the idiomatic
  spelling is `List~Key~`, which is what the diagrams now use.
* **Prose is not a member.** A class body may contain members and stereotypes, not
  an explanatory sentence. That text moved out into the surrounding document.

**2. No class name is invented.** Every node in every diagram was extracted and
checked against `^type X` across all non-test `.go` files: 71 nodes, against 105
types in the code, all found. The only names not in the code are the three
sequence-diagram outer participants (`App`, `GA`, `GB`), which are deliberately one
level above the classes.

**3. No method name is invented.** Every member containing parentheses was extracted
and matched against `func ... Name(` in the sources: 171 method names checked. This
found one genuine fabrication — a `SetHandlerHint` on `Store` that does not exist —
which has been removed.

**What these checks do not cover.** They cannot confirm a diagram *reads* well, that
the chosen relationships are the most illuminating ones, or that a listed member is
the most important one. They also cannot render: this environment has no network and
no mermaid installed, so rendering was replaced by grammar validation. If you have
mermaid available, `mmdc -i docs/04-uml-class-diagram.md -o /tmp/out.md` renders all
eight blocks. If it disagrees with anything above, believe `mmdc` and the source, not
this document.

Checks 1 is reproducible from the repository:

```sh
node tools/doc-check/check-mermaid.js docs/04-uml-class-diagram.md
```

It takes a markdown file, validates every fenced `mermaid` block, and exits non-zero
on the first problem it can describe. It is a grammar check, not a renderer: it
knows the member and relationship forms of `classDiagram` and `sequenceDiagram`, and
nothing else. Checks 2 and 3 were ad-hoc greps over the sources and are not shipped,
because they depend on this document's particular naming.

One accuracy note the diagrams expose. `Version.Visible` uses `<=`, whereas
docs/02-transaction-design.md §7.4 requires visibility to be `CommitTS < snapshot`.
The two are not in conflict: `Store.latestVersionAt` is the function reads actually
go through, and it uses `<`. `Version.Visible` is a leftover helper the read path
does not call, so §2 shows it as part of the type but it is not drawn as part of the
read path.
