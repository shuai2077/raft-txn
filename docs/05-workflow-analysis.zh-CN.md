# 05 — 工作流程分析：按模块关联关系

> English version: [05-workflow-analysis.md](05-workflow-analysis.md)

本文是一篇**按模块关联关系**展开的代码库工作流程分析。它不重复讲设计动机 ——
那是 [`docs/02-transaction-design.md`](02-transaction-design.md) 的职责；本文只回答三个问题：

1. 代码被拆成了哪些**模块**，每个模块的职责边界在哪里；
2. 模块之间**通过什么关联**（调用、channel、接口实现、HTTP）；
3. 一条请求走完整个系统时，**这些关联按什么顺序被用到**。

分析方法参照一篇对 etcd 中 raft 实现做源码走读的文章（下文称「参照文章」）：它先给出术语表
与宏观架构，再逐层拆解核心数据结构，然后重点分析**层间交互流程**，最后把写流程、读流程各自
拆成若干带编号的步骤，每一步都点明「哪个模块 → 调用什么 → 什么数据结构跨过了边界」。本文
沿用这套骨架，并在每个关键处指出**本仓库与 etcd 的结构性差异** —— 那些差异正是这套代码的
设计要点所在。

---

## 0 前言

参照文章分析了 etcd v3.1.10 的 raft 实现：etcd 把 raft 拆成**算法层**（`./raft/*`，
可独立引入的 SDK）与**应用层**（`contrib/raftexample/*`），两层各自跑在独立 goroutine 中，
靠几个 channel 做异步通信，并以 `Ready`/`Advance` 作为一轮交互的边界。

本仓库面对同样的结构问题，但给出的答案不同。**这个不同点是理解本代码库的钥匙**：

| 关注点 | etcd（参照文章） | 本仓库 |
|---|---|---|
| 算法层与事务层的边界 | etcd 的 raft 是通用共识库 | `internal/raft` 同样是通用共识库，但**新增了一个状态机钩子**（`StateMachineHandler`） |
| 应用层与算法层的通信 | 几个 channel（`propc`/`recvc`/`readyc`/`advancec`）+ `Ready`/`Advance` 协议，**异步** | `Server.Apply`：提议 + 等待，**同步**。`Ready`/`Advance` 那一层协议在本地扩展中被移除了 |
| 谁驱动定时 | 应用层定时调用 `Node.Tick()` | `ConsensusModule` 内部的选举定时器自己计时，无外部 Tick |
| 谁应用状态机 | 应用层从 `commitChan` 读到已提交条目后自己应用 | **`commitChanSender` 在提交路径上直接应用**，应用层只负责把 channel 排空 |
| 跨组事务 | etcd 单组（v3.1 时代）；事务在别处 | `txn` 层做 Multi-Raft 的 2PC 协调，`txnservice` 只服务**一个组** |

下面逐节展开。

---

## 1 模块划分

与 raft 相关的代码分布如下。**「层」这一列是本文后续所有讨论的坐标**。

| 模块 | 目录 | 层 | 职责 |
|---|---|---|---|
| 共识算法层 | `internal/raft/*` | 算法层 | Raft 本体：选举、日志复制、持久化、RPC 传输。上游 `part3/raft` 加少量标记为「本地扩展」的新增项 |
| 组服务层 | `txnservice/*` | 应用层 | 把**一个** Raft 组装配成可被远程访问的服务：算法层 + 事务状态机 + HTTP 接口 |
| 事务协议层 | `txn/*` | 应用层（协议） | MVCC 状态机、意向锁、2PC 协调器、锁解析策略。**定义全部接口** |
| 授时服务 | `tso/*` | 应用层（独立共识组） | 基于 Raft 复制的 timestamp oracle，同样跑在算法层之上 |
| 集群装配 | `cluster/*` | 装配层 | 把多个组、多个 oracle 成员装成一个 Multi-Raft 集群，并提供故障注入 |
| 演示程序 | `cmd/demo/*` | 装配层 | 可运行的端到端演示 |

**关键的结构事实：`txn` 定义接口，其他模块实现接口。** 这使同一份协调逻辑既能跑在 HTTP 之上
（生产路径），也能跑在进程内（测试与 `cluster`）。§3 会具体展开这一点。

---

## 2 术语表

参照文章先固定术语的做法很值得沿用：本代码库里「层」「状态机」「提交」都有两个含义，不先
说清楚后面必然歧义。

| 中文名 | 英文名 / 代码标识 | 说明 |
|---|---|---|
| 算法层 | algorithm layer / `internal/raft` | 内聚 Raft 共识机制的模块。被上层以库的形式引入，**不感知事务、键值或 HTTP** |
| 应用层 | application layer | 聚合存储、通信与 HTTP 能力的模块。承上启下：对外服务客户端，对内使用算法层 |
| 组服务 | group service / `txnservice.Node` | 应用层中「一个 Raft 组」的抽象。持有算法层 `*raft.Server`、事务状态机 `*txn.Store`，并暴露 HTTP |
| 事务协调器 | coordinator / `txn.Client` | 跨组 2PC 的发起者。本身**不是** Raft 节点，是一段客户端逻辑 |
| 事务句柄 | transaction handle / `txn.Txn` | 一笔事务的全部本地状态（快照时间戳、写集、已准备组）。`Client` 无事务状态，故可并发复用 |
| 数据状态机 | data state machine / `txn.Store` | 真正存数据的介质：版本链 + 锁表 + 事务记录。为避免与「Raft 节点状态机」混淆，本文一律称**数据状态机** |
| 节点状态机 | node state machine / `ConsensusModule` | Raft 节点本身是个状态机：任何输入（选举超时、收到 RPC、被提议命令）都驱动其状态变化 |
| 提议 | proposal / `ConsensusModule.propose` | 两阶段提交的第一阶段：把命令追加进日志 |
| 提交 | commit / `commitIndex` 推进 | 第二阶段：日志被多数派复制，可以对外生效 |
| 应用 | apply / `StateMachineHandler.Apply` | 把已提交日志喂给数据状态机，使写请求真正生效 |
| 屏障 | barrier / `CmdBarrier` | 一条**空操作**日志条目，唯一意义是它在日志中的位置：等它被应用，即证明它之前的条目都已应用 |
| 封印 | seal / `CmdSeal` | 把提交时间戳写进**主键**的锁记录，但**不**发布版本。**封印主键即事务的原子提交点** |
| 主键 / 次键 | primary / secondary key | 一张分布式锁表。主键承载事务的提交决定；次键只能去问主键 |
| 租约 | lease / `LockRecord.ExpiresAt` | 锁的存活期限，界定读者等待多久；**它不授权回滚**（见 §7.5） |
| 解析 | resolution / `Client.resolveLock` | 读者撞上一把他人遗留的锁时，判定该事务的结局并据此发布或丢弃 |
| 授时服务 | timestamp oracle / `tso` | 全局单调唯一的时间戳来源，本身是一个 Raft 组 |
| 快照时间戳 | start timestamp / `Txn.startTS` | 事务读取所用的一致性视点 |
| 提交时间戳 | commit timestamp / `Txn.committedTS` | 事务写入对外可见的时间点 |

---

## 3 宏观架构

### 3.1 算法层

算法层是 Raft 共识机制的核心实现：任期与投票、日志复制、持久化、RPC 传输。它的**全部输入**
只有三类：收到的 peer RPC、被本地提议的命令、选举定时器到期。

本仓库对算法层做的最重要扩展是引入了一个**状态机钩子**：

```
StateMachineHandler.Apply(command any) any
```

上游 `part3/raft` 没有这个概念 —— 上游把已提交条目交给应用层，由应用层自己决定怎么应用。
本仓库让**组本身成为状态机**：`Server.Apply` 提议一条命令后，等 `commitChanSender` 把这条
条目应用完，再把数据状态机的回答原路返回给提议者。

这带来两个后果，是理解后面所有流程的前提：

1. **提议可以是同步的。** 协调器调用 `Participant.Prewrite` 拿到的不是一个日志索引，而是
   数据状态机对这次 prewrite 的判定结果（成功 / 冲突 / 锁被占）。没有第二套「用日志索引去
   关联结果」的机制。
2. **应用必须发生在共识锁之外。** `commitChanSender` 在调用 `handler.Apply` 前先释放
   `cm.mu`；否则数据状态机自己的锁会和共识锁形成环，并让一次慢应用拖住选举与心跳。

### 3.2 组服务层

`txnservice.Node` 是算法层之上的装配体，它把一个 Raft 组变成可远程访问的服务：

```
        HTTP 客户端（txnservice.GroupClient）
                    │  POST /txn/prewrite/ 等
                    ▼
        ┌───────────────────────────────────────────┐
        │  Node（一个 Raft 组）                      │
        │   httpServer  ──> handlePrewrite ...       │  应用层：协议与传输
        │        │ propose(cmd, render)              │
        │        ▼                                   │
        │   RaftLog.ApplyCommand ──> Server.Apply    │  ← 层间边界
        │        │                                   │
        │   raft.Server / ConsensusModule            │  算法层：共识
        │        │ commitChanSender: handler.Apply   │  ← 状态机钩子
        │        ▼                                   │
        │   txn.Store（数据状态机）                   │  应用层：MVCC + 锁
        └───────────────────────────────────────────┘
```

注意 `Node` 里那个 `commitChan`：算法层仍会把已提交条目发到它上面，但**它不再驱动状态机**
（状态机已经在同步路径上应用过了）。`Node.drainCommits` 这个 goroutine 的存在只是为了把
channel 排空，否则算法层的提交发送方会被阻塞。这是「用同步 `Apply` 取代 `Ready`/`Advance`」
之后必然出现的结构性残留，也是与 etcd 最直观的一处对照。

### 3.3 宏观架构：模块之间靠什么关联

这是全文最重要的一张图。它标出每个模块，以及**模块之间那条边上到底是什么**：

```mermaid
flowchart TB
    subgraph app["应用层"]
        HTTP["httpapi<br/>客户端 HTTP 请求"]
        GC["txnservice.GroupClient<br/>HTTP 客户端 / Participant 实现"]
        CO["txn.Client + txn.Txn<br/>2PC 协调器（跨组）"]
        RS["txn.DefaultResolver<br/>锁解析策略"]
    end

    subgraph node["组服务层：一个 Raft 组 = 一个 Node"]
        SVC["txnservice.Node<br/>handle* + propose"]
        LOG["txn.RaftLog<br/>Log + Participant 实现"]
        ST["txn.Store<br/>数据状态机"]
    end

    subgraph alg["算法层"]
        SRV["raft.Server<br/>Apply / Serve / RPC"]
        CM["raft.ConsensusModule<br/>节点状态机 + commitChanSender"]
        STOR["raft.Storage"]
    end

    subgraph tso["授时服务（另一个 Raft 组）"]
        TSOS["tso.Service + tso.Oracle"]
        TSOC["tso.Client"]
    end

    subgraph asm["装配层"]
        CL["cluster.Cluster"]
        DEMO["cmd/demo"]
    end

    HTTP -->|"POST /txn/*"| SVC
    GC -->|"HTTP + JSON"| SVC
    CO -->|"Participant 接口"| GC
    CO -->|"Oracle 接口"| TSOC
    CO -->|"Resolver 接口"| RS
    RS -->|"Participant 接口"| GC

    SVC -->|"Log 接口"| LOG
    LOG -->|"Server.Apply（同步）"| SRV
    SRV -->|"owns"| CM
    CM -->|"StateMachineHandler.Apply"| ST
    CM -->|"persist / restore"| STOR
    SVC -->|"读锁表 / 版本链"| ST

    TSOC -->|"HTTP + JSON"| TSOS
    TSOS -->|"同一个 raft.Server + 自己的状态机"| SRV

    CL -->|"装配 + 故障注入"| node
    CL -->|"装配"| tso
    CL -->|"Client()"| CO
    DEMO --> CL
```

**读法**：实线边上的标签就是那条关联的**类型**。可以看到三条清晰的规律：

1. **`txn` 只定义接口，不实现传输。** `Participant`、`Log`、`Oracle`、`Resolver`、`Partitioner`
   全部定义在 `txn` 包里；`txnservice`、`tso`、`internal/raft` 各自实现其中一个。
2. **算法层只通过两个口与外界相连**：入方向是 `Server.Apply`，出方向是 `StateMachineHandler.Apply`。
   算法层不知道事务、键或 HTTP 的存在。
3. **跨组的一切都发生在协调器里，而不是在任何一个 Raft 组里。** 每个 Raft 组只服务自己那片
   键空间；2PC 的编排完全在 `txn.Client` 这一段客户端逻辑中。

---

## 4 算法层核心数据结构

本节按参照文章的方式逐个介绍。读者在后续流程章节遇到不明确的结构可回查此处。

### 4.1 LogEntry / CommitEntry

```go
// internal/raft/raft.go
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

一条 `LogEntry` 就是一笔预写日志。注意 `Command` 是 `any`：算法层**不关心**里面装的是事务
命令还是别的什么，这正是它能被复用（并被 `tso` 复用）的原因。`CommitEntry` 是同一笔日志在
「已提交」之后的形态，多了 `Index`，因为它要送去给上层定位。

本仓库的日志索引从 0 开始，且 `LogEntry` 没有独立的 `Index` 字段 —— 索引就是它在切片中的
位置。上游如此，本仓库未改。

### 4.2 Server

```go
// internal/raft/server.go
type Server struct {
    serverId    int
    peerIds     []int
    cm          *ConsensusModule   // 算法核心
    storage     Storage
    ready       <-chan any
    commitChan  chan<- CommitEntry
    rpcServer   *rpc.Server
    rpcProxy    *RPCProxy
    listener    net.Listener
    peerClients map[int]*rpc.Client
    conns       map[net.Conn]struct{}   // 本地扩展：Shutdown 用
    quit        chan any
    acceptDone  chan struct{}           // 本地扩展：等 accept 循环退出
    wg          sync.WaitGroup
    mu          sync.Mutex
}
```

`Server` 是**传输 + 生命周期**的封装，共识算法本身全在 `ConsensusModule` 里。`Server` 承担的
三件事：监听 TCP 并用 `net/rpc` 提供 peer 间通信；管理到各 peer 的出向 `*rpc.Client`；把
`Apply`/`Shutdown` 这类「本地调用」翻译成对 `ConsensusModule` 的操作。

`conns` 与 `acceptDone` 是本仓库新增的：上游的 `Shutdown` 会等待可能永久阻塞在读对端数据的
处理函数，多个节点同时停止时极易挂死。

### 4.3 ConsensusModule（节点状态机）

```go
// internal/raft/raft.go
type ConsensusModule struct {
    id        int
    peerIds   []int
    state     CMState          // Follower | Candidate | Leader | Dead
    currentTerm int
    votedFor    int

    log         []LogEntry
    commitIndex int
    lastApplied int

    // 本地扩展：状态机钩子
    handler StateMachineHandler

    // 本地扩展：同步提议的关联表
    results map[int]chan proposalResult

    // 本地扩展：唤醒与停机
    newCommitReadyChan   chan struct{}
    newCommitReadyChanWg sync.WaitGroup
    stopChan             chan struct{}
    electionTimerWg      sync.WaitGroup

    leaderId int   // 本地扩展：给客户端指路
    // ... 选举定时器、peer 进度等
}
```

参照文章说 etcd 的 raft 节点本质是个大状态机，任何操作最后都封装成一则消息输入状态机。
**本仓库不是这个形状**：这里没有统一的 `Step(Message)` 入口，也没有按角色分派的
`stepLeader`/`stepCandidate`/`stepFollower`。取而代之的是三条互不相同的入口：

| 入口 | 触发者 | 效果 |
|---|---|---|
| `Server.Apply(cmd)` | 本地提议（HTTP 请求） | `propose` 追加日志 → 唤醒复制；登记结果 channel；等结果 |
| `RPCProxy.RequestVote/AppendEntries` | 远端 peer | 进入 `ConsensusModule` 的对应方法 |
| `runElectionTimer` | 内部定时器 | 超时则发起选举 |

这是与 etcd 最本质的结构差异，也是本仓库代码量小的原因：**它不需要用一个通用 `Message`
结构去覆盖所有场景**。（参照文章也批评了 etcd 那个大而全的 `Message` 破坏了职责单一原则；
本仓库相当于用三个专用入口规避了这一点，代价是没有 etcd 那样的可扩展性。）

`results` 是同步提议的关键：`map[日志索引]结果channel`。提议者登记自己的 channel，应用者
在应用完那条条目后投递结果。

### 4.4 StateMachineHandler（层间唯一的出向接口）

```go
// internal/raft/raft.go
type StateMachineHandler interface {
    Apply(command any) any
}
```

只有一个方法的接口，却是整个架构的枢纽：它是算法层**唯一**的数据出口。`txn.Store` 实现它，
于是「共识层」与「事务层」在类型层面完成解耦 —— 算法层不知道 `TxnCommand` 是什么。

### 4.5 proposalResult

```go
type proposalResult struct {
    value any
    err   error
}
```

同步提议的返回值载体。`value` 就是 `handler.Apply` 返回的东西 —— 对本仓库而言，是
`txn.TxnResult`。

### 4.6 Storage

```go
// internal/raft/storage.go
type Storage interface {
    Set(key string, value any)
    Get(key string) (any, bool)
}
```

持久化接口，`MapStorage` 是内存实现。上游的实现每次变更都把**整个日志** gob 编码后写入，
是日志长度的平方级开销 —— 这是已知且被有意保留的简化。

---

## 5 组服务层与事务协议层核心数据结构

### 5.1 txnservice.Node（组服务）

```go
// txnservice/service.go
type Node struct {
    cfg     NodeConfig
    groupID txn.GroupID

    raftServer *raft.Server    // 算法层入口
    log        *txn.RaftLog    // Log 接口 + Participant 接口实现
    store      *txn.Store      // 数据状态机

    commitChan chan raft.CommitEntry  // 只用于排空，不驱动状态机
    httpServer *http.Server
    listener   net.Listener
    leaders    atomic.Int64            // 路由提示
    quit       chan struct{}
    wg         sync.WaitGroup
}
```

`Node` 是「一个 Raft 组」在应用层的抽象，同时扮演**服务端**（对外提供 HTTP）和**算法层使用者**
（对内调用 `ApplyCommand`）。它的构造顺序值得记住，因为它就是一条依赖链：

```
raft.NewServer(...) → Serve()          算法层起来
txn.NewRaftLog(groupID, raftServer)    适配成 Log 接口
txn.NewStore(log)                      数据状态机
raftServer.SetHandler(store)           挂上状态机钩子
```

### 5.2 txn.RaftLog（层间适配器）

```go
// txn/raftadapter.go
type RaftLog struct {
    server  *raft.Server
    groupID GroupID
    mu      sync.RWMutex
    store   *Store        // 延迟绑定
    learnedLeader int
}
```

`RaftLog` 是**唯一同时实现 `Log` 与 `Participant` 的类**：

- 实现 `Log`：`ApplyCommand` → `server.Apply(cmd)`；这是数据状态机复制命令的通道。
- 实现 `Participant`：`Read`/`Prewrite`/`Seal`/`Commit`/`Rollback`/`Locked`/`Barrier`，
  每个方法把参数包成 `TxnCommand` 再走同一个 `ApplyCommand`。

也就是说，**`Participant` 的七个方法在进程内路径上都是 `ApplyCommand` 的一层薄包装**，
差别只在参数结构与结果解释。这一点在 §6 的对照表里会非常直观。

### 5.3 txn.Store（数据状态机）

```go
// txn/store.go
type Store struct {
    log       Log
    lockTTL   time.Duration
    nowMillis func() uint64

    versions map[Key][]Version    // MVCC 版本链
    locks    map[Key]*LockRecord  // 意向锁表
    actors   map[TxnID]*Actor     // 事务记录

    appliedCommitTS Timestamp     // 水位：已应用的最高提交时间戳
}
```

三个 map 就是全部数据，`Apply(command any) any` 是唯一入口。**整个数据状态机没有一把自己的
互斥锁** —— 因为 Raft 已经保证了同一时刻只有一个 goroutine 在应用条目。这是「让组成为状态机」
带来的直接好处。

三个 map 各自回答一个不同的问题：

- `versions` 回答「快照 S 能看到什么」；
- `locks` 回答「有没有一笔写入正在被扣住」；
- `actors` 回答「这笔事务决定了没有」—— 这是次级锁的读者唯一能问的地方。

### 5.4 txn.Client 与 txn.Txn（协调器与事务句柄）

```go
// txn/client.go
type Client struct {
    oracle       Oracle
    partitioner  Partitioner
    participants map[GroupID]Participant
    resolver     Resolver
    resolveTimeout time.Duration
    now          func() uint64
    clientID     int64
    requests     atomic.Int64
    // ... 统计
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

这个划分是刻意的：**`Client` 不持有任何事务状态**，所以一个 `Client` 可以并发服务许多事务；
**`Txn` 持有全部事务状态**，因此明确「不安全并发使用」。`barriered` 记录已经取过读屏障的组，
保证每组每事务只取一次。

### 5.5 txn.TxnCommand（跨组协议的命令词汇）

```go
// txn/command.go
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

七种命令，全部走同一条 `Raft` 日志。`CmdBarrier` 是第八种：不携带任何负载，唯一意义是它的
**位置**。

---

## 6 层间交互流程

### 6.1 两种 Participant 实现 —— 同一个协调器的两条路

这是本仓库最值得单独讲的一处结构设计。参照文章里，应用层与算法层靠 channel 异步通信，
只有一条路径；本仓库有**两条**，而且协调器代码对二者完全无感：

| `Participant` 方法 | HTTP 路径（`txnservice.GroupClient`） | 进程内路径（`txn.RaftLog`） |
|---|---|---|
| `Read` | `POST /txn/read/` → `handleRead` → `propose` | `ApplyCommand(CmdRead)` |
| `Prewrite` | `POST /txn/prewrite/` → `handlePrewrite` | `ApplyCommand(CmdPrewrite)` |
| `Seal` | `POST /txn/seal/` → `handleSeal` | `ApplyCommand(CmdSeal)` |
| `Commit` | `POST /txn/commit/` → `handleCommit` | `ApplyCommand(CmdCommit)` |
| `Rollback` | `POST /txn/rollback/` → `handleRollback` | `ApplyCommand(CmdRollback)` |
| `Locked` | `POST /txn/locked/` → `handleLocked` | `ApplyCommand(CmdQueryLock)` |
| `Barrier` | `POST /txn/barrier/` → `handleBarrier` | `ApplyCommand(CmdBarrier)` |

两条路径最终都汇聚到 `Server.Apply`。HTTP 路径多做的事只有两件：JSON 编解码，以及
**跟随 leader 提示**（`GroupClient.callFor` 在收到 `not-leader` 时直接跳到提示的节点重试）。
进程内路径则由 `RaftLog.ApplyCommand` 把 `NotLeaderError` 原样返回给调用方重试。

**为什么值得这样做**：测试与 `cluster` 需要一个多组集群，若每个组都要起 HTTP 端口，成本和
不确定性都会上升。有了这层接口，`cluster` 可以选择任意一条路径装配，而 `txn` 的 2PC 逻辑
一行都不用改。

### 6.2 一轮同步提议：`Server.Apply` 的六个动作

这是算法层与应用层之间**唯一的入向边界**，值得逐步展开：

```go
// internal/raft/server.go —— 已简化，保留结构与注释
index := cm.propose(cmd)                    // (1) 追加日志，拿到索引
ch := make(chan proposalResult, 1)          // (2) 建结果 channel（缓冲 1）
cm.results[index] = ch                      // (3) 登记关联：索引 → channel
if cm.commitIndex >= index {                // (4) 若已提交，催一次应用者
    select { case cm.newCommitReadyChan <- struct{}{}: default: }
}
cm.mu.Unlock()                              // (5) 放锁，然后等
select {
case res := <-ch:                           // (6a) 应用者投递了结果
case <-cm.stopChan:                         // (6b) 停机
case <-timer.C:                             // (6c) 超时（无多数派）
}
```

对应地在算法层的应用侧：

```go
// internal/raft/raft.go —— commitChanSender
for range cm.newCommitReadyChan {
    cm.mu.Lock()
    entries := cm.log[cm.lastApplied+1 : cm.commitIndex+1]  // 取新提交的条目
    cm.lastApplied = cm.commitIndex
    handler := cm.handler
    cm.mu.Unlock()                                          // 关键：放锁再应用

    for i, entry := range entries {
        result := handler.Apply(entry.Command)              // 应用数据状态机
        cm.mu.Lock()
        if ch, ok := cm.results[index]; ok {                // 找到提议者
            delete(cm.results, index)
            cm.mu.Unlock()
            select { case ch <- proposalResult{value: result}: default: }
        } else { cm.mu.Unlock() }
        select { case cm.commitChan <- CommitEntry{...}: case <-cm.stopChan: }
    }
}
```

**这段代码解释了本仓库为什么不需要 `Ready`/`Advance`。** etcd 里，算法层把「这一轮的结果」
打包进 `Ready` 交给应用层，应用层做完持久化与发送后调 `Advance` 表示收工，两者靠这一对方法
界定一轮交互。本仓库把这一轮**折叠进了一次函数调用**：`propose` 登记、应用者投递、
`Apply` 取结果，调用返回即一轮结束。副作用是 `commitChan` 退化成「只为了不让发送方阻塞」
的排空通道（`Node.drainCommits`）。

**两个必须写对的细节**，注释里也点明了：

- 结果 channel 必须在**释放 `cm.mu` 之前**登记，否则应用者可能在无人监听时投递。
- 投递用非阻塞 `select`：应用者不能因为某个提议者已经放弃等待而卡住整个日志推进。

### 6.3 启动顺序（依赖链）

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
    Note over RS: 启动 accept 循环、选举定时器、commitChanSender
    N->>N: txn.NewRaftLog(groupID, raftServer)
    N->>ST: txn.NewStore(log)
    N->>ST: RaftLog.SetStore(store)
    ST->>RS: SetHandler(store)  ← 挂上状态机钩子
    N->>N: go drainCommits() / go pollLeader() / go serveHTTP()
    CL->>TSO: NewService(...) x TSONodes
    CL->>CL: txn.NewClient(oracle, partitioner, participants, nil)
    CL->>CL: SetResolver(NewResolver(txnClient))
    CL->>CL: WaitForLeaders(10s)
```

`cluster.New` 里那两行顺序是有含义的：协调器与解析器**互相引用**（解析器要走协调器的
participant 集合，协调器需要解析器来清理外来的锁），所以先建协调器，再建解析器，然后回填。

---

## 7 角色状态机与两条核心链路

### 7.1 Raft 角色转换（算法层内部）

本仓库的角色转换不用 `Step(Message)` 分派，而是由三处直接调用：

```mermaid
stateDiagram-v2
    [*] --> Follower
    Follower --> Candidate: 选举超时
    Candidate --> Candidate: 拉票超时 / 重新选举
    Candidate --> Leader: 获得多数票
    Candidate --> Follower: 收到更大任期
    Leader --> Follower: 收到更大任期
    Follower --> Follower: 任期推进
```

与事务层相关的一点：**领导权变化不影响正确性，只影响可用性与延迟**。协调器拿到
`NotLeaderError` 就换节点重试；`txnservice.GroupClient` 会记住提示的 leader 以免重复试探。

### 7.2 写流程：跨组事务提交（八个步骤）

这是整个代码库最核心的一条链路。它跨越**两个 Raft 组 + 授时服务 + 协调器**，因此比 etcd
的单组写流程更能体现模块关联关系。

```mermaid
sequenceDiagram
    autonumber
    participant APP as 应用（如 cmd/demo）
    participant TX as txn.Txn
    participant CL as txn.Client
    participant ORA as tso.Client
    participant GA as 组 A（主键所在组）
    participant GB as 组 B

    APP->>CL: Begin(ctx)
    CL->>ORA: ReadTS()
    ORA-->>CL: startTS
    CL-->>APP: *Txn{startTS}

    APP->>TX: Put("0alice", "70") / Put("1bob", "130")
    Note over TX: 只写本地缓冲，不发网络

    APP->>TX: Commit(ctx)
    TX->>TX: prewriteWrites：按组切分写集，选出主键
    TX->>GA: Prewrite(ops A)
    GA-->>TX: prepared
    TX->>GB: Prewrite(ops B)
    GB-->>TX: prepared
    TX->>ORA: CommitTS(startTS)
    ORA-->>TX: commitTS
    TX->>TX: verifyLocksHeld：逐组确认锁仍在
    TX->>GA: Seal(主键, commitTS)
    Note over GA: 原子提交点
    GA-->>TX: sealed
    TX->>GA: Commit(commitTS, 主键)
    TX->>GB: Commit(commitTS, keys)
    GB-->>TX: published
    TX-->>APP: commitTS
```

逐步对应模块与数据结构：

| 步 | 模块 | 调用 | 跨边界的数据 |
|---|---|---|---|
| 1 | 应用 → `txn.Client` | `Begin(ctx)` | — |
| 2 | `Client` → `tso.Client` | `ReadTS(ctx)` | HTTP + JSON（`NextRequest`/`NextResponse`） |
| 3 | 应用 → `Txn` | `Put` / `Delete` | 只进 `Txn.writes`，**无任何网络** |
| 4 | `Txn` → 各组 | `prewriteWrites` 后逐组 `Participant.Prewrite` | `PrewriteRequest`（含 `Ops`、`Primary`、`StartTS`） |
| 5 | `Txn` → `tso.Client` | `CommitTS(startTS)` | HTTP + JSON |
| 6 | `Txn` → 各组 | `verifyLocksHeld` → `Participant.Locked` | `CmdQueryLock` |
| 7 | `Txn` → 主键组 | `Participant.Seal` | `SealArgs`；**写进主锁即提交点** |
| 8 | `Txn` → 各组 | `Participant.Commit` | `CommitRequest`；由此 `Store.commit` → `publishVersion` |

**第 4 步的组内展开**（以组 A 为例），这是算法层真正被用到的地方：

```
GroupClient.Prewrite        应用层：HTTP 客户端
  → Node.handlePrewrite     应用层：HTTP 服务端，解码 JSON
  → Node.propose            应用层：先做 leader 自查（省一次日志往返）
  → RaftLog.ApplyCommand    适配层：包成 TxnCommand
  → Server.Apply            算法层：propose + 等结果
      → ConsensusModule.propose        追加日志 → 触发复制
      → commitChanSender               日志被多数派复制后
          → Store.Apply(TxnCommand)    数据状态机：写锁、查冲突
      → cm.results[index] <- result    回传
  → 渲染 WriteResponse → HTTP 响应
```

**第 7 步为什么是提交点**：`Seal` 只把 `commitTS` 写进主锁的记录里，**不发布任何版本**。
一旦这条日志被应用，任何撞上该事务任一把锁的读者都能从主键问出结局，从而自己把写入补完 ——
这就是「协调器死了也能推进」的机制来源。

### 7.3 读流程（六个步骤）

读流程比写流程短，但包含本仓库特有的**读屏障**环节：

| 步 | 模块 | 调用 | 说明 |
|---|---|---|---|
| 1 | `Client` → `tso.Client` | `ReadTS` | 取快照时间戳 S |
| 2 | `Txn` → 目标组 | `Participant.Barrier` | 每组一次：等一条空操作日志被应用 |
| 3 | 屏障之后 | `Store.AppliedCommitTS` | 水位可用于校验；**屏障本身不依赖它** |
| 4 | `Txn` → 目标组 | `Participant.Read(startTS, key)` | 按严格可见性 `CommitTS < S` 取版本 |
| 5 | 若撞锁 | `Client.resolveLock` → `Resolver` | 判定属主结局：已提交则发布，否则**请求主组记录中止** |
| 6 | 重试第 4 步 | | 锁清掉后读到干净结果 |

**第 2 步的必要性**：只读事务对每个键各发一次 Raft 往返，各自带同一个时间戳 S。**Raft 只保证
每个组内部有序，不保证跨组的两次读落在同一瞬间**。屏障利用「日志按序应用」这一性质：等一条
空操作条目被应用，即证明该组已应用它之前的全部条目；而任何提交时间戳小于 S 的事务，其写入
必然在屏障之前就已进入该组日志。证明见 [`docs/03-read-barrier.md`](03-read-barrier.md) §1.2。

**第 5 步的安全边界**：读者**不得**因为锁的租约过期就丢弃它。租约只说明属主不再刷新锁，
不说明属主不能再封印。因此读者必须向主组请求一次 **compare-and-abort**：主组复查自己的状态，
若事务已提交则返回「已提交」，读者改为**发布**；否则记录中止。这条规则是
[`docs/03-read-barrier.md`](03-read-barrier.md) §2 那场丢写入竞态的修复核心。

---

## 8 模块关联关系总表

把全文的关联收成一张表。**这张表是本文的结论**：

| 从 | 到 | 关联类型 | 定义处 | 实现处 |
|---|---|---|---|---|
| 应用 | `txnservice.Node` | HTTP + JSON | `txnservice/api.go` | `Node.handle*` |
| `txn.Client` | 各组 | `Participant` 接口 | `txn/types.go` | `txnservice.GroupClient`（HTTP）/ `txn.RaftLog`（进程内） |
| `txn.Client` | 授时 | `Oracle` 接口 | `txn/types.go` | `tso.Client` |
| `txn.Client` | 锁清理 | `Resolver` 接口 | `txn/types.go` | `txn.DefaultResolver` |
| `txn.Client` | 路由 | `Partitioner` 接口 | `txn/types.go` | `HashPartitioner` / `StaticPartitioner` |
| `txn.Store` | 复制 | `Log` 接口 | `txn/store.go` | `txn.RaftLog` |
| `txn.RaftLog` | 共识 | `Server.Apply` | `internal/raft/server.go` | — |
| `raft.ConsensusModule` | 数据状态机 | `StateMachineHandler` 接口 | `internal/raft/raft.go` | `txn.Store` |
| `tso.Service` | 共识 | 同一个 `raft.Server` | — | `tso.Oracle` 作为其 handler |
| `cluster.Cluster` | 全部 | 装配 + 故障注入 | `cluster/cluster.go` | — |

三条贯穿全文的规律：

1. **接口集中在 `txn`，实现分散在外围。** 因此替换传输层（HTTP ↔ 进程内）不需要改动一行
   协调器代码 —— 这正是 `cluster` 能在两种装配之间自由选择的原因。
2. **算法层与外界的接触面只有两个**：入向 `Server.Apply`，出向 `StateMachineHandler.Apply`。
   算法层对其承载的是事务毫不知情，这也是 `tso` 能复用同一个 `raft.Server` 的原因。
3. **跨组编排全部在协调器（客户端侧）完成。** 每个 Raft 组只服务自己那片键空间，组与组之间
   **不存在**直接通信。这是 Multi-Raft 与「一个 Raft 组管所有数据」的根本区别，也是 2PC、
   主键、读屏障这一整套机制存在的原因。

---

## 9 本文与参照文章的差异汇总

| 维度 | 参照文章（etcd v3.1.10） | 本仓库 |
|---|---|---|
| 层间通信 | channel + `Ready`/`Advance`，异步 | `Server.Apply`，同步 |
| 是否有 `Ready` 结构 | 有，承载 `SoftState`/`HardState`/`Entries`/`CommittedEntries`/`Messages` | **无**；这一轮的产物由 `proposalResult` + `StateMachineHandler` 返回值承载 |
| 定时驱动 | 应用层 `ticker` 调 `Node.Tick()` | `ConsensusModule` 内部选举定时器 |
| 状态机应用位置 | 应用层自己读 `commitChan` 后应用 | `commitChanSender` 在提交路径上应用 |
| 节点状态机入口 | 统一 `Step(Message)` + 按角色分派 | 三个专用入口（`Apply` / RPC / 定时器） |
| 角色状态字段 | `StateType` + `step`/`tick` 函数指针 | `CMState`，无函数指针 |
| 读一致性 | `ReadIndex` + `readOnly` 队列 + 心跳 ack 计数 | `CmdBarrier` 空操作条目 + 已应用水位 |
| 事务 | 单组（v3.1） | Multi-Raft 跨组 2PC，主键 + 意向锁 + 锁解析 |
| 日志存储 | `storage` + `unstable` 双层 + 快照压缩 | 单一 `Storage` 接口，整日志 gob 编码，无压缩 |

**一句话总结**：参照文章中的 etcd 用「channel + `Ready`/`Advance`」把算法层与应用层隔开，是
**异步解耦**；本仓库用「`Server.Apply` + `StateMachineHandler`」把二者连起来，是**同步收敛**。
前者换来可扩展性与非阻塞，后者换来调用点的直白——协调器能像调用本地函数一样拿到共识结果，
代价是每次提议都占用一个等待中的 goroutine，且需要 `commitChan` 这条排空通道来兜住残留。

---

## 10 如何自行验证本文的说法

本文每一条结构性断言都能在源码中核对，且大部分有对应的测试：

| 断言 | 核对方式 |
|---|---|
| 状态机在共识锁之外应用 | 读 `internal/raft/raft.go` 的 `commitChanSender`：`cm.mu.Unlock()` 在 `handler.Apply` 之前 |
| `commitChan` 不再驱动状态机 | 读 `txnservice/service.go` 的 `drainCommits` 及其注释 |
| 两条 `Participant` 路径等价 | 对照 `txnservice/client.go` 与 `txn/raftadapter.go` 的同名方法 |
| 封印是提交点 | `txn/store.go` 的 `seal`；测试 `TestSecondaryLockIsFinishedAfterPrimaryCommits` |
| 屏障按日志位置生效 | `txn/store.go` 的 `CmdBarrier`；测试 `TestReadBarrierEstablishesAppliedPrefix` |
| 读者不得凭租约回滚 | `txn/client.go` 的 `ensurePrimaryResolved`/`abortAtPrimary`；测试 `TestSealedSecondaryIsNotLostWhenALeaseLapses` |
| 跨组原子性 | 测试 `TestCommitTimestampsPairUpAcrossGroups` |

若本文与源码不一致，**以源码为准**，并请提一个 issue 或直接修正本文。
