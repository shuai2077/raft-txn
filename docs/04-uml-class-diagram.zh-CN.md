# 04 —— 架构：UML 类图

> English version: [04-uml-class-diagram.md](04-uml-class-diagram.md)

本文档是 docs/01–03 的结构性姊妹篇。那几篇解释系统*为什么*要建成现在这样；这一篇展示
*各个部件是什么，以及它们如何拼合在一起*。

下文的每一个类型、字段和关系都是从源码中读出来的，而不是凭记忆写下的；每一张图都经过
校验：有一个脚本会检查 mermaid 块能否解析、有没有类名是臆造的、有没有方法名是臆造的。
§9 精确说明了这一校验覆盖了什么、又没有覆盖什么。

层的边界就是包的边界，而包边界也正是依赖方向：低层中的任何东西都不会导入更高层。

## 1. 层与依赖

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

这样读它：事务协议（`txn`）居于中心。它定义了每一个接口，而其他每个包要么实现其中一个
接口（`txnservice`、`internal/raft`、`tso`），要么把各个实现装配到一起（`cluster`、
`cmd/demo`）。`txn` 不从这个仓库中导入任何其他东西，唯一的例外是 `internal/raft`，而且
只出现在 `raftadapter.go` 里，用于把一个 `*raft.Server` 适配成 `Log` 与 `Participant`
接口。

## 2. 核心词汇 —— `package txn`

其他每一层都用这些共享类型来「说话」。下面这些名字会在后面的协议图中反复出现。

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

**为什么这些记录是彼此分开的。** `Version` 是历史：每个键上的一条版本链就构成了 MVCC
存储。`LockRecord` 是进行中的写入，它把一个版本扣住，直到事务作出裁决。`Actor` 是事务
自身的状态*在某个组看来*的样子，读者正是通过它来弄清自己撞上的一个锁属于一个存活的、
已提交的还是一个已死亡的事务。`LockRecord.CommitTS` 针对单个键回答这个问题；`Actor`
则针对整个事务回答它，而这正是*从*锁的读者所需要的。

## 3. 事务协议 —— `package txn`

接口就是契约；接口之下的那些具体类型就是实现，它们位于其他包中。

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

`Participant` 是单个 Raft 组的 RPC 表面，也是这里最重要的接口：它有两个实现（§5 中基于
HTTP，§6 中进程内），而协调器是针对这个接口编写的，因此两者都无需改动即可工作。`Log`
是*状态机*所需的那个更窄的东西：复制一条命令，并询问谁是领导者。

`DefaultResolver` 是恢复策略。给定在某个组中发现的一个锁，它会依据该事务的**主键**所在
组判明持有该锁的事务的最终命运，然后要么把这次写入完成，要么丢弃它。它持有一个
`*Client`，因为它复用了协调器的探测机制；这也是为什么二者在构造完成之后相互绑定，而
不是作为参数传入。

## 4. 协调器与状态机

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

`Client` 不持有任何按事务划分的状态，可安全地并发使用；`Txn` 持有全部这类状态，并且
明确**不**可安全地并发使用。正是这一划分使得一个 `Client` 可以服务多个事务。

`Store` 就是那个复制状态机。`Apply` 是它唯一的入口：每一次变更都以一条 `TxnCommand`
的形式到达，而这条命令已经由 Raft 排好序，因此状态机永远不会同时面对两个写者，它自身
也不需要任何内部加锁。

## 5. 服务层 —— `package txnservice`

一个组如何通过 HTTP 被访问，以及一个组成员如何由共识、状态机和一层 HTTP 表面装配而成。

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

`Node` 与 `GroupClient` 是同一个线上协议的两端：`*Request` 类型是 `GroupClient` 发送、
`Node.handle*` 解码的东西，`*Response` 类型则是处理器写出的、客户端解析的东西。每个响应
都内嵌了 `ResponseStatus`，因此客户端能够识别 `not-leader` 并顺着其中的提示走，而不必
知道究竟是哪一个处理器作出的应答。

## 6. 共识与授时服务

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

`StateMachineHandler` 是共识与事务层之间的接缝：被安装进去的正是 `txn.Store.Apply`，
因此一条已提交的日志条目*就是*一条事务状态机命令。`Server.Apply` 是构建在其上的同步
路径 —— 先提议，然后等待应用器交回一个结果 —— 它是本地的一处扩展，让协调器能够得到
一个答案，而不仅仅是一个日志下标。

授时服务复用同一个共识模块，但配有自己的状态机：

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

授时服务里值得注意的部分是那个 `intents` 映射。每个成员都*预留*一个时间戳窗口，并且
只能花用自己预留的那一份。这正是领导者更替之所以安全的原因：新的领导者无法花用前一任
领导者的窗口，因为这些预留是复制状态的一部分。因此时间戳可能被浪费，但绝不会被重复。

## 7. 测试装置 —— `package cluster`

它不是产品代码，但却是架构的一部分：上面的一切都靠它来演练，演示程序也靠它来运行。

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

## 8. 提交路径的时序图表示

类图展示的是结构；这张图展示的则是结构在一次跨组事务中被使用的顺序。

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

最重要的一行是 `Seal(primary, commitTS)`。在它落定之前，事务仍然可以被中止；一旦它
落定，任何一个撞上该事务某个锁的读者都能得知最终结果，并在没有协调器帮助的情况下把
这次写入完成。

## 9. 这些图是如何校验的

图很容易在细微之处出错，而一张渲染不出来的图什么也说明不了。我们运行了三项机械检查，
它们值得被精确地陈述出来，因为它们同时也界定了本文档所主张的范围。

**1. mermaid 块能够解析。** 有一个脚本会按结构规则检查每一个 `classDiagram` 和
`sequenceDiagram` 块：图关键字必须位于最前，类体必须配平，每一行成员都必须匹配
mermaid 的某一种成员形式，每一条关系都必须使用真实存在的运算符。它有意比 mermaid 更
严格 —— 它会报告无法识别的行，而不是跳过它们 —— 因为在 mermaid 渲染出一个错误框的
地方，严格的检查会大声报错。

这在初稿中抓出了 32 个真实问题。分两类：

* **Go 的切片语法不是 mermaid。** 我原来写的是 `+[]Key Keys` 和
  `+Writes() []WriteOp`。mermaid 的文法里没有 `[]` 这种成员形式；惯用的写法是
  `List~Key~`，这也是现在这些图所采用的写法。
* **散文不是成员。** 类体中只能包含成员和构造型，不能包含一个解释性的句子。那些文字
  被移到了周围的正文里。

**2. 没有类名是臆造的。** 每一张图中的每一个节点都被提取出来，并与所有非测试 `.go`
文件中的 `^type X` 逐一核对：71 个节点，对应代码中的 105 个类型，全部找到。代码中不
存在的名字只有时序图里的三个外层参与者（`App`、`GA`、`GB`），它们被有意放在比类高一层
的位置上。

**3. 没有方法名是臆造的。** 每一个含有括号的成员都被提取出来，并与源码中的
`func ... Name(` 逐一匹配：共检查了 171 个方法名。这一步发现了一处货真价实的凭空捏造
—— `Store` 上一个并不存在的 `SetHandlerHint` —— 它已经被移除。

**这些检查没有覆盖什么。** 它们无法确认一张图*读起来*是否顺畅、所选的关系是否是最能
说明问题的那些、或者所列出的成员是否就是最重要的那个。它们也无法渲染：这个环境没有
网络，也没有安装 mermaid，因此渲染被文法校验替代了。如果你手边有 mermaid，
`mmdc -i docs/04-uml-class-diagram.md -o /tmp/out.md` 可以渲染全部八个块。如果它的结果
与上文有任何不一致，请相信 `mmdc` 和源码，而不是本文档。

检查 1 可以从仓库中复现：

```sh
node tools/doc-check/check-mermaid.js docs/04-uml-class-diagram.md
```

它接收一个 markdown 文件，校验其中每一个围栏化的 `mermaid` 块，并在遇到第一个它能描述
的问题时以非零状态退出。它是一次文法检查，而不是渲染器：它认得 `classDiagram` 和
`sequenceDiagram` 的成员形式与关系形式，别的就一概不知了。检查 2 和检查 3 是对源码做的
临时 grep，并未随仓库提供，因为它们依赖于本文档特有的命名。

还有一处准确性问题，是这些图暴露出来的。`Version.Visible` 用的是 `<=`，而
docs/02-transaction-design.md §7.4 要求可见性判定为 `CommitTS < snapshot`。两者并不
冲突：读操作实际走的是 `Store.latestVersionAt` 这个函数，它用的是 `<`。`Version.Visible`
是一个遗留的辅助方法，读路径并不调用它，因此 §2 把它作为类型的一部分展示出来，但没有
把它画成读路径的一部分。
