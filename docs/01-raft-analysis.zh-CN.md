# `eliben/raft` 分析：架构与关键机制

> English version: [01-raft-analysis.md](01-raft-analysis.md)

本文分析上游项目
[`github.com/eliben/raft`](https://github.com/eliben/raft) —— 一个用 Go 编写的 Raft 共识算法
教学实现 —— 并且是本仓库各项设计决策的参考依据。此处的
`internal/raft` 就是该项目的
`part3/raft`，除此之外以 vendor 方式逐字引入，唯一差异是
[§5](#5-what-this-repository-changes-in-the-raft-core) 中列出的改动。

所分析的上游修订为 `1a48e2c`（"Document the storage atomicity assumption"），
共 9 位贡献者。

---

## 1. 这个仓库是什么

一个由五部分组成的教学系列，每一部分都是一个完整、自包含的 Go 模块：

| 目录 | 对应内容 | 新增内容 |
|---|---|---|
| `part1/` | [第 1 部分：选举](https://eli.thegreenplace.net/2020/implementing-raft-part-1-elections/) | 领导者选举、任期、投票、心跳 |
| `part2/` | [第 2 部分：命令与日志复制](https://eli.thegreenplace.net/2020/implementing-raft-part-2-commands-and-log-replication/) | 日志复制、提交索引、提交通道 |
| `part3/raft/` | [第 3 部分：持久化与优化](https://eli.thegreenplace.net/2020/implementing-raft-part-3-persistence-and-optimizations/) | 持久的 `currentTerm`/`votedFor`/`log`、冲突索引快速回退 |
| `part4kv/` | [第 4 部分：键/值数据库](https://eli.thegreenplace.net/2024/implementing-raft-part-4-keyvalue-database/) | 构建在 part 3 之上的 REST KV 服务 —— 共识模块的第一个*应用* |
| `part5kv/` | [第 5 部分：精确一次投递](https://eli.thegreenplace.net/2024/implementing-raft-part-5-exactly-once-delivery/) | 解决 Part 4 的响应丢失导致的重复问题 |
| `tools/raft-testlog-viz/` | — | 把 `go test -v` 的输出转成可浏览的 HTML 时间线 |

各部分之间的重复是作者有意为之的编排决定
（README 中已说明）：每个目录都可以单独阅读，而它们之间的 `diff`
本身就是课程内容。`part3` 是唯一一个把 Raft 代码放在可导入的包中、
而非 `package main` 中的部分，因为 `part4kv` 与 `part5kv`
都要消费它。

因此，依赖关系图是：

```
part1, part2   独立模块
part3/raft     共识核心，可作为 github.com/eliben/raft/part3/raft 导入
part4kv        -> part3/raft
part5kv        -> part3/raft
```

## 2. 架构

### 2.1 各层，以及分界所在的位置

上游只划出了一条真正重要的边界，其余一切都由它推出：

```
        应用（KV 服务、数据存储、HTTP/REST API）
   ────────────────────────────────────────────────────────  ← 分界
        raft.Server            传输层：net/rpc + RPCProxy
        raft.ConsensusModule   算法
        raft.Storage           持久性
```

在分界之上，应用只需要看到三样东西：`Submit(cmd) int`、
一个 `chan CommitEntry`，以及 `Storage`。共识模块对
键、值或 HTTP 一无所知。正是这一性质使 `part3` 可以被两个
KV 服务复用，也正是本仓库所保持的性质。

### 2.2 `ConsensusModule` —— 算法

一个结构体同时持有论文中的持久状态与易失状态，外加
把它与进程其余部分连接起来的那些通道：

```go
// 持久状态
currentTerm int
votedFor    int
log         []LogEntry

// 易失状态，所有服务器
commitIndex, lastApplied int
state                    CMState    // Follower | Candidate | Leader | Dead
electionResetEvent       time.Time

// 易失状态，仅领导者
nextIndex, matchIndex map[int]int
```

并发由单个 `sync.Mutex`（`cm.mu`）守护上述全部状态来处理，
外加三个通道：

* `commitChan` —— 出站；CM 通过它上报已经达成共识的条目。
* `newCommitReadyChan` —— 内部通知，表示 `commitIndex` 可能已经
  推进；由一个专用 goroutine（`commitChanSender`）消费它。
* `triggerAEChan` —— 「立即发送 AppendEntries，而不是等待下一次
  心跳」。

每台服务器有三个长期存在的 goroutine：选举定时器、领导者
的心跳循环，以及提交发送器。

### 2.3 选举

`runElectionTimer` 每 10 ms 轮询一次，把 `time.Since(electionResetEvent)`
与一个随机化的超时时间作比较（`150 + rand(150)` ms；`RAFT_FORCE_MORE_REELECTION`
会把它偏向 150 ms，以制造选举碰撞）。只要状态离开跟随者/候选者或任期发生变化，
该定时器就会退出，而 `becomeFollower` 与
`startElection` 各自会启动一个新的 —— 因此并不存在显式的「重置定时器」，
只有「当前定时器发现自身已过期并自行结束」。

有两个细节值得注意，因为它们正是上游在评审之后
修复的两处：

* `startElection` 会持久化 `currentTerm` 与 `votedFor = self`，而且是在发送
  任何 `RequestVote` **之前**。否则，一个崩溃后重启的候选者
  可能在同一任期内把票投给另一个候选者。
* `becomeFollower` 在任期推进时执行持久化，原因相同。

选票以 `votesReceived*2 > len(peerIds)+1` 计数，也就是严格
多数派。

### 2.4 日志复制与 Part 3 的两项优化

领导者对每个对等节点发送 `AppendEntries{PrevLogIndex, PrevLogTerm, Entries,
LeaderCommit}`。跟随者只有在自己的日志中存在
`PrevLogIndex` 处的条目、且其任期等于 `PrevLogTerm` 时才接受 —— 这就是把日志匹配性质
作为 RPC 前置条件强制执行。

在不匹配时，跟随者返回 `ConflictIndex`/`ConflictTerm`：

```go
if args.PrevLogIndex >= len(cm.log) {
    reply.ConflictIndex = len(cm.log); reply.ConflictTerm = -1
} else {
    reply.ConflictTerm = cm.log[args.PrevLogIndex].Term
    // 回退到该任期的第一个索引
    reply.ConflictIndex = i + 1
}
```

领导者据此把 `nextIndex` **一次回退一整个任期**，而不是
一次一个条目，正是这一点把日志修复从 O(条目数) 次往返变成
O(任期数)。这就是论文 §5.3 中的「更快的冲突解决」，
也是 Part 3 中最有意思的一处。

提交推进严格实现了图 2 的规则 —— 只统计**当前任期**
的条目：

```go
for i := cm.commitIndex + 1; i < len(cm.log); i++ {
    if cm.log[i].Term == cm.currentTerm {
        matchCount := 1
        for _, peerId := range cm.peerIds { if cm.matchIndex[peerId] >= i { matchCount++ } }
        if matchCount*2 > len(cm.peerIds)+1 { cm.commitIndex = i }
    }
}
```

### 2.5 持久化

`Storage` 是一个三方法接口 —— `Set`、`Get`、`HasData` —— 并带有一个
供测试使用的 `MapStorage` 实现。它的文档注释记录了一个重要的
假设：

```go
// 本示例假设 persistToStorage 中的一系列 Set 调用
// 会原子地把 currentTerm、votedFor 与 log 一起持久化。
```

`persistToStorage` 在每次变更时都对整个日志做 gob 编码。这是
能够满足该要求的最简单做法，而且它是
平方级的 —— 这是本仓库设计所依赖的一个观察，
而不是要改变它的批评。

### 2.6 传输层与故障注入

`raft.Server` 用一个 `net/rpc` 服务器以及一个从 `*rpc.Client` 到
对等节点的映射把 CM 包装起来。所有出站调用都经过 `Server.Call`，它再委托给
`RPCProxy.Call`。`RPCProxy` 就是测试用来注入故障的分界：

* `DropCallsAfterN(n)` —— 在接下来的 *n* 次调用之后丢弃调用，模拟网络分区。
* `RAFT_UNRELIABLE_RPC` —— 随机丢弃十次调用中的一次，并延迟另一次。

路由按服务器 ID 进行：`ConnectToPeer(id, addr)` 与 `DisconnectPeer(id)`。

### 2.7 KV 服务（Part 4）

```
HTTP 客户端 ──REST──> KVService ──Submit──> raft.Server ──RPC──> 对等节点
                          │                      │
                          └── runUpdater <── commitChan
                                  │
                              DataStore (map[string]string)
```

其中有意思的机制：

* **`Command` 是一个扁平结构体**，带有一个 `Kind` 判别字段以及覆盖
  每一种操作的字段，而不是代数类型。`ResultValue`/`ResultFound`
  搭载在同一个结构体上，因此它还能兼作应答。
* **提交订阅。** 处理函数必须等到*它自己*的条目提交后才能应答，
  但提交事件到达的是同一条共享通道。`Submit` 返回日志索引；
  处理函数按该索引注册一个一次性的 `chan Command`；
  updater goroutine 把命令交给与之匹配的那个订阅。
* **读操作同样经过日志。** Part 4 对此有详细解释：一个已被罢黜
  却仍自认为领导者的领导者，否则会提供过期的读结果，
  破坏线性一致性。这就是 Raft 论文的第 8 节。
* **`Id` 校验。** 当提交到达被订阅的索引时，处理函数
  会校验 `commitCmd.Id == kvs.id`。否则说明它的条目在领导者更替中丢失了，
  索引 42 已被另一个领导者的命令覆盖；客户端会得到
  `StatusFailedCommit`，而不是一个错误的答案。
* **客户端的领导者发现。** `KVClient.send` 遍历地址列表，把
  `StatusNotLeader` 以及每次尝试的超时都视为「试下一个」，并记住
  最后一个接受过命令的地址。

### 2.8 精确一次投递（Part 5）

Part 4 的客户端会重试，这可能让响应丢失的命令被重复执行。
Part 5 给每条命令增加一个 `(ClientID, RequestID)` 对，并由 updater
维护 `lastRequestIDPerClient`：

```go
lastReqID, ok := kvs.lastRequestIDPerClient[cmd.ClientID]
if ok && lastReqID >= cmd.RequestID {
    cmd = Command{Kind: cmd.Kind, IsDuplicate: true}   // 不应用
} else {
    kvs.lastRequestIDPerClient[cmd.ClientID] = cmd.RequestID
    // ... 应用
}
```

客户端按客户端维度单调分配 `RequestID`，因此重试重新发送的
是*同一个* ID，从而能被识别；处理函数会报告 `StatusDuplicateRequest`，
客户端由此得知自己先前那次尝试实际上已经生效。Part 5 还增加了
`DelayNextHTTPResponse`，使测试套件能够精确制造出这种歧义情形：
服务已经对请求采取了动作，但响应丢失。

## 3. 关键机制及其收益

| 机制 | 位置 | 为什么重要 |
|---|---|---|
| 覆盖全部 CM 状态的单把互斥锁 | `raft.go` | 使每个 RPC 处理函数相对于状态机都是原子的；代价是锁下的任何操作都不得阻塞 |
| 状态/任期变化即退出的一次性选举定时器 | `runElectionTimer` | 避免重置竞态；过期定时器直接消亡 |
| 投票前 / `becomeFollower` 之前先持久化 | `startElection`、`becomeFollower` | 跨崩溃的选举安全性 |
| `ConflictIndex`/`ConflictTerm` | `AppendEntries` / `leaderSendAEs` | 一次一个任期的日志修复 |
| 只提交同任期的条目 | `leaderSendAEs` | 图 2 的提交规则；防止提交那些仍可能被覆盖的条目 |
| 按日志索引的提交订阅 | `part4kv`/`part5kv` | 让调用方能在一条共享通道上把「我的提交」与「某次提交」对应起来 |
| 读操作经过日志 | `part4kv`+ | 线性一致性读 |
| `(ClientID, RequestID)` 去重 | `part5kv` | 跨重试的精确一次语义 |
| `RPCProxy` | `server.go` | 无需触碰算法即可实现确定性的故障注入 |
| `Storage` 接口 | `storage.go` | 让持久性层保持可替换；并携带一条显式的原子性假设 |

## 4. 设计有意省略了什么

这些并不是疏漏；系列文章明确说明了这一点。它们是任何
生产级使用 —— 以及本仓库的事务层 —— 都必须补上的空白：

1. **没有集群成员变更。** 对等节点集合在构造时即固定
   （论文 §6 未被涉及）。
2. **没有快照 / 日志压缩。** 日志无界增长，且每次写入都整体
   重新编码，因此重启时间随历史增长。
3. **整日志持久化。** 相对于日志规模是平方级的；没有增量追加。
4. **没有读索引优化。** 读操作要付出一次完整的日志往返；论文
   §8 的租约/读索引替代方案被列为未来工作。
5. **存储与传输层都用 `gob`。** 方便，但跨 schema 变更时
   脆弱，而且不具语言中立性。
6. **没有事务。** 每条命令自身是原子的；没有任何东西能把多个
   键组合成一个全有或全无的单元。这正是本仓库
   填补的空白。
7. **只有一个 raft 组。** `part4kv`/`part5kv` 都是单个组；没有
   分片，因此也没有任何跨分片的东西。

## 5. 本仓库对 Raft 核心所做的改动

`internal/raft` 就是上游的 `part3/raft`，另加以下新增项，
每一项在源码中都标记为本地扩展：

| 改动 | 为什么有必要 |
|---|---|
| `StateMachineHandler`、`SetHandler`，以及从提交路径应用它 | 上游完全没有状态机钩子：应用自己从 `commitChan` 应用已提交的命令。事务层需要让*组*成为状态机，并由每个副本按日志顺序应用 |
| `Server.Apply` 阻塞直到条目提交，并返回状态机的回答 | 让协调者无需第二套关联机制，即可在 Raft 之上获得同步的请求/响应 |
| 状态机运行在共识锁**之外** | 持有 `cm.mu` 跨越一次应用的 `Apply` 会让 `Stop` 死锁，并让一次缓慢的 apply 拖住选举和心跳 |
| `leaderId` 跟踪 + `Server.LeaderID` | 让跟随者能告诉客户端领导者是谁，把领导者发现从「探测每个成员」变成一跳。（上游 Part 4 的脚注 4 正是把这个作为练习提出的。） |
| 真实 RPC 连接由 `Shutdown` 持有，accept 循环可由 `Stop` 唤醒 | 上游的关闭流程会等待那些可能阻塞在读对端数据的处理函数，而对端自己也在关闭。当许多服务器同时停止时，这是卡死的最主要单一来源 |
| 投递提交条目时检查 `stopChan` | 如果客户端停止读取自己的提交通道，上游会永远阻塞 |

其余一切 —— 算法、加锁纪律、持久化模型、
RPC 接口面，以及整个上游测试套件 —— 都未被修改。

## 6. 测试方法

上游的做法值得照搬，而本仓库也确实照搬了：

* 每个部分都有一个进程内的 `Harness`，它构建一个 N 个服务器的集群、连接
  对等节点，并提供 `CrashPeer`、`DisconnectPeer`、`RestartPeer`、
  `PeerDropCallsAfterN`。
* **`RestartPeer` 复用崩溃服务器的 `Storage`**，这正是让持久化测试
  名副其实、而非流于形式的关键。
* 测试断言的是*整个集群范围*的一致性（`CheckCommitted` 会比较所有存活
  服务器上的提交序列），而不只是本地行为。
* `leaktest.CheckTimeout` 用于防范 goroutine 泄漏。
* `tools/raft-testlog-viz` 把冗长的测试输出变成一条 HTML 时间线 ——
  这是让分布式测试失败变得可读的调试辅助工具。

本仓库的 `cluster/` 包是同一思路向上一层的版本：它构建
一个 Multi-Raft 集群，其故障注入作用于*事务*系统
（把一个组分区、让 oracle 领导者崩溃、
从存储重启整个集群）。

## 7. 参考文献

* [Raft 论文](https://raft.github.io/raft.pdf) —— Ongaro & Ousterhout，*In
  Search of an Understandable Consensus Algorithm*（USENIX ATC '14）。
* [第 0 部分：引言](https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/)
* [第 1 部分：选举](https://eli.thegreenplace.net/2020/implementing-raft-part-1-elections/)
* [第 2 部分：命令与日志复制](https://eli.thegreenplace.net/2020/implementing-raft-part-2-commands-and-log-replication/)
* [第 3 部分：持久化与优化](https://eli.thegreenplace.net/2020/implementing-raft-part-3-persistence-and-optimizations/)
* [第 4 部分：键/值数据库](https://eli.thegreenplace.net/2024/implementing-raft-part-4-keyvalue-database/)
* [第 5 部分：精确一次投递](https://eli.thegreenplace.net/2024/implementing-raft-part-5-exactly-once-delivery/)
* [分布式系统中的线性一致性](https://eli.thegreenplace.net/2024/linearizability-in-distributed-systems/)
