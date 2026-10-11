# SHM 高性能消息队列设计方案

> 定位：通用、高性能、跨进程共享内存消息队列（IPC）  
> 并发模式：SPSC / SPMC / MPSC / MPMC  
> 核心目标：零拷贝、低延迟、高吞吐、可验证、可维护  
> 应用目标：支撑分布式服务器集群 100K TPS 级消息吞吐  
> 数据面原则：同机优先 Direct SHM；跨机使用 TCP/RPC

---

## 变更记录

### 实现完成项

| 设计节 | 实现内容 | 验证状态 |
|--------|----------|----------|
| §5 | MessageDescriptor 32B 固定大小，static_assert 验证 | ✅ |
| §6 | SlotHandle + Generation，stale descriptor 检测 | ✅ |
| §7 | MessagePool 三层 Tier（Tiny/Small/Large），预分配复用 | ✅ |
| §8 | Payload 生命周期 FREE→ALLOCATED→QUEUED→CLAIMED→FREE | ✅ |
| §9 | 不使用 RefCount，Single Ownership Transfer | ✅ |
| §10 | ShmQueue\<Algorithm\> 模板 + ShmQueueBase 多态接口 + Empty/Full/Notify | ✅ |
| §11 | SPSC 无 CAS 无 mutex，head/tail 分离 cache line | ✅ |
| §12 | SPMC Consumer CAS claim | ✅ |
| §13-16 | MPSC 4 状态 FREE→RESERVED→READY→CONSUMED | ✅ |
| §17-18 | MPMC Vyukov sequence ring，双边 CAS | ✅ |
| §20 | Release/Acquire memory ordering，Payload 字段非 atomic | ✅ |
| §23-25 | interprocess_semaphore 通知，Wait+TryPop 正确流程 | ✅ |
| §26 | Batch Push/Pop，4 种算法全部实现 | ✅ |
| §27 | head/tail 分离 cache line（alignas(64)） | ✅ |
| §28 | 6 种 Backpressure 策略：BUSY_SPIN/YIELD/BACKOFF/BLOCK/DROP/REJECT | ✅ |
| §29 | Stop 独立于算法，stop flag + sem post 唤醒 | ✅ |
| §30-32 | Crash Recovery：SalvageScan 回收 ALLOCATED/CLAIMED 超时 slot | ✅ |
| §33 | Ring Sequence 与 Payload Generation 分层 | ✅ |
| §34 | 不支持 Broadcast/Multicast（明确 Non-Goal） | ✅ |
| §37 | RAII MessageGuard，析构自动 Release；PullMessageQueueT 全面使用 ReceiveGuard | ✅ |
| §40 | ChannelConfig.algorithm 创建时确定，运行期不切换 | ✅ |
| §41-42 | Benchmark 框架：4 算法 × 8 payload 尺寸吞吐矩阵 | ✅ |
| §45 | 文件名严格对齐（见下方更新清单） | ✅ |

### 新增

1. **ShmQueueBase 补全 §10 接口**：新增 `Empty() const`、`Full() const`、`Notify()` 虚方法
   - `Notify()` 对应 sem_empty.post()，用于 ad-hoc 唤醒阻塞的 consumer
   - `Empty()`/`Full()` 通过 ring 的 Size()/Capacity() 查询

2. **Backpressure 补全 §28 全部 6 策略**：
   - `BACKOFF`：指数退避 sleep（1us→2us→...→1000us 上限）
   - `DROP`：TryPush 失败立即返回 false（丢弃语义）
   - `REJECT`：TryPush 失败立即返回 false（拒绝语义）
   - Publish 和 Receive 双侧均按策略分发

3. **PullMessageQueueT RAII 增强**（§37）：
   - 改用 `ReceiveGuard(timeout)` 替代手动 `Receive` + `Release`
   - callback 外层加 try-catch 防止异常逃逸线程
   - guard 析构自动 Release slot，即使 callback 抛异常也不泄漏

4. **文件结构重构**：
   - 文件名去下划线：`spsc_algorithm.h` → `spscalgorithm.h`（及 spmc/mpsc/mpmc）
   - `shmqueue_base.h` → `shmqueuebase.h`
   - `common.hpp` → `channelconfig.h`
   - `shmslot.h` 合并入 `shmdescriptor.h`
   - 新增 `pool/` 子目录（slotlayout.h, shmslabpool.h/.cpp）
   - 新增 `shmutil.h`（PID/timestamp/CRC32 工具）
   - 移除 `shmmanaged.h`, `shmlock.h`, `sharedmemory.hpp`（不再需要）
   - 删除空 `shmmessagequeue.cpp`（MakeShmQueue 已在 .h 中 inline）

5. **枚举值统一 SCREAMING_SNAKE_CASE**：
   - `BackpressureStrategy`: `BUSY_SPIN/YIELD/BACKOFF/BLOCK/DROP/REJECT`

### 测试结果

```
[==========] 100 tests from 14 test suites ran. (11447 ms total)
[  PASSED  ] 97 tests.
[  SKIPPED ] 3 tests (压力测试需手动开启)
```

### Benchmark 结果（SPSC 128B）

- 单条发布：~1.54M msg/s
- 批量发布（batch=8）：~2.53M msg/s（+64%）

---

## 1. 结论

统一支持四种并发模式：

```text
                    ShmQueue<Algorithm>
                            |
        +-----------+-------+--------+-----------+
        |           |                |           |
       SPSC        SPMC             MPSC        MPMC
        |           |                |           |
       1P/1C       1P/NC            NP/1C       NP/NC
```

四种算法共享：

- MessageDescriptor
- MessagePool
- SlotHandle
- generation
- Payload 生命周期
- Stop 状态
- Notification
- ChannelConfig
- 统计与监控接口

不同之处只有一个核心：

> 谁拥有 Producer/Consumer 状态，以及多参与者如何 Claim Ring Slot。

核心边界：

```text
Algorithm
    -> 并发访问算法

Ring
    -> Descriptor 传输

Pool
    -> Payload 生命周期

Semaphore
    -> 阻塞/唤醒
```

---

## 2. 设计目标

### 2.1 功能

支持：

- 同机跨进程通信
- 共享内存
- Zero-Copy Payload
- SPSC / SPMC / MPSC / MPMC
- TryPush / TryPop
- Blocking Push / Pop
- Stop
- Queue Full / Empty
- PayloadPool
- Slot generation
- Crash Recovery
- Batch Push / Pop
- 可观测性

### 2.2 性能

目标不是单纯追求一个 benchmark 数字，而是：

```text
低延迟
+
高吞吐
+
低 CPU
+
低 Cache Miss
+
避免大块 Payload memcpy
```

在集群 100K TPS 目标下：

```text
Gateway / Logic / Scene / DBAgent
        |
        +-- 同机 --> Direct SHM
        |
        +-- 跨机 --> TCP/RPC
```

SHM Queue 不应成为内部瓶颈。

---

## 3. 为什么不直接使用 Boost.Lockfree Queue

Boost.Lockfree 的 SPSC 等实现适合作为无锁 Ring 的算法参考，但不直接作为跨进程 SHM Queue。

普通 Lockfree Queue：

```text
Thread A -> Thread B
```

本项目：

```text
Process A
    |
Shared Memory
    |
Process B
```

跨进程还需要处理：

- 共享内存地址空间
- offset/handle，而不是进程私有裸指针
- interprocess synchronization
- Payload 生命周期
- Pool ownership
- generation
- 进程异常
- Stop / wakeup
- SHM ABI 与 atomic 可用性

因此最终原则：

> 借鉴 Boost.Lockfree、DPDK Ring 等成熟无锁 Ring 的算法思想，但实现自己的 SHM 专用 Ring。

同时 Queue 不传完整 Message，而只传固定大小 Descriptor。

---

## 4. Descriptor + Payload 分离

不要：

```text
Queue<Message>
```

而是：

```text
Queue<MessageDescriptor>
          |
          v
     MessagePool
          |
          v
        Payload
```

例如 16KB 消息：

传统：

```text
Producer
   |
   | memcpy 16KB
   v
Queue
   |
   | memcpy 16KB
   v
Consumer
```

本设计：

```text
Producer
   |
   v
MessagePool Slot
   |
   | 直接写 Payload
   v
MessageDescriptor
   |
   | 只传 Descriptor
   v
Descriptor Ring
   |
   v
Consumer
   |
   v
直接读取 SHM Payload
```

因此：

> Queue 传递的是"消息在哪里"，不是"消息内容"。

---

## 5. MessageDescriptor

建议固定 32 Bytes，实际字段可调整：

```cpp
struct MessageDescriptor
{
    uint32_t slot_id;
    uint32_t generation;

    uint32_t size;
    uint8_t  tier;         // 0..2 = TINY/SMALL/LARGE; 3 = invalid
    uint8_t  reserved;     // 对齐填充
    uint16_t type;

    uint16_t flags;
    uint16_t reserved2;    // 对齐填充

    uint64_t sequence;
};
```

原则：

- 固定大小
- 不包含进程私有裸指针
- 能定位 Payload
- 能检测 Slot 重用
- 支持调试和统计

---

## 6. SlotHandle + Generation

不能只保存：

```cpp
slot_id
```

必须：

```text
slot_id + generation
```

否则：

```text
slot 100
   |
  FREE
   |
重新 Acquire
   |
slot 100 被复用
```

旧 Descriptor 仍可能包含 `slot_id=100`，从而访问新消息。

因此：

```cpp
descriptor.generation == slot.generation
```

才允许访问。

Generation 用于：

- stale descriptor 检测
- Slot 重用检测
- ABA 风险防护
- 崩溃恢复
- 调试定位

---

## 7. MessagePool

Pool 与 Queue 完全分离。

### Pool 负责

- Slot 分配
- Slot 释放
- Payload 存储
- generation
- 状态管理
- 进程状态
- Crash Recovery
- Salvage
- Pool exhaustion 统计

### Pool 不负责

- 消息路由
- TCP
- RPC
- 业务逻辑
- Queue 并发算法
- Notification

建议：

```text
MessagePool
 +-- Tiny
 +-- Small
 +-- Large
```

例如：

```text
Tiny   256B
Small  4KB
Large  64KB
```

最终容量通过 benchmark 决定。

运行期间优先：

```text
启动预分配
      |
Acquire
      |
使用
      |
Release
      |
复用
```

尽量避免数据面频繁 Grow/mmap/malloc。

---

## 8. Payload 生命周期

四种 Queue 模式均采用：

> Single Ownership Transfer

生命周期：

```text
FREE
 |
Acquire
 |
ALLOCATED
 |
Publish
 |
QUEUED
 |
Claim / Pop
 |
CLAIMED
 |
Read Payload
 |
Release
 |
FREE
```

Producer 在 Publish 前拥有写权限。

Consumer Claim 后拥有读权限。

Consumer Release 后，Producer/Pool 才能重新使用 Slot。

---

## 9. 为什么当前不使用 Payload RefCount

RefCount 解决的是：

```text
一个 Payload
    |
    +-- C1
    +-- C2
    +-- C3
```

即 Broadcast/Multicast。

普通单播：

```text
Producer -> Queue -> 一个 Consumer -> Release
```

只有一个 Owner。

因此当前：

> SPSC / SPMC / MPSC / MPMC 都不使用 Payload RefCount。

这样可以减少：

- Atomic RMW
- Cache line bouncing
- 生命周期复杂度
- Crash Recovery 复杂度
- 调试难度

未来 Broadcast/Multicast 单独设计。

---

# 10. 统一 Queue 接口

```cpp
template<class Algorithm>
class ShmQueue
{
public:
    bool TryPush(const MessageDescriptor& desc);
    bool Push(const MessageDescriptor& desc);

    bool TryPop(MessageDescriptor& desc);
    bool Pop(MessageDescriptor& desc);

    bool Empty() const noexcept;
    bool Full() const noexcept;

    void Notify();
    void Stop();

private:
    Algorithm algorithm_;
};
```

高吞吐场景增加：

```cpp
TryPushBatch()
TryPopBatch()
```

---

# 11. SPSC

SPSC：

```text
Single Producer
Single Consumer
```

Producer 独占 `tail`，Consumer 独占 `head`。

### Push

```text
local tail
   |
读取 head
   |
判断 Full
   |
写 Descriptor
   |
tail.store(next, release)
   |
semaphore.post()
```

### Pop

```text
local head
   |
tail.load(acquire)
   |
判断 Empty
   |
读取 Descriptor
   |
head.store(next, release)
```

正常热路径：

- Producer 不 CAS
- Consumer 不 CAS
- 不使用 mutex
- head/tail 分离 Cache Line
- capacity 使用 2 的幂

SPSC 是最低开销模式。

---

# 12. SPMC

SPMC：

```text
1 Producer
N Consumers
```

```text
                 +-- Consumer A
                /
Producer -> Ring -- Consumer B
                                 +-- Consumer C
```

语义：

> 一条消息只允许一个 Consumer 成功取得。

不是 Broadcast。

Producer 仍然独占 tail。

多个 Consumer 竞争 head：

```text
读取 head
   |
读取 tail
   |
判断 Empty
   |
CAS(head, old, old + 1)
   |
成功?
 +-- Yes -> 获得 Descriptor
 +-- No  -> Retry
```

关键：

> 先 Claim，再访问 Descriptor/Payload。

随着 Consumer 增加：

```text
CAS contention
      |
Cache coherence
      |
性能下降
```

因此实际应测试 1P+2C、1P+4C、1P+8C。

---

# 13. MPSC —— 当前重点模式

MPSC：

```text
Multiple Producers
Single Consumer
```

典型架构：

```text
Gateway Worker 1 --+
Gateway Worker 2 --+
Gateway Worker 3 --+--> Logic
Gateway Worker N --+
```

这与当前服务器架构中的大量汇聚场景高度匹配。

MPSC 的核心问题是多个 Producer 竞争：

```text
tail
```

但不能简单：

```cpp
tail.fetch_add(1);
WriteDescriptor();
```

因为：

```text
P1 reservation slot N
P1 暂停

P2 reservation slot N+1
P2 写完

Consumer 看到 tail 已前进
Consumer 访问 slot N
P1 还没写完
```

因此：

> Reservation 和 Publication 必须分离。

---

# 14. MPSC：Reservation + Publication

推荐每个 Ring Slot 带独立 sequence/state：

```text
FREE
 |
RESERVED
 |
READY
 |
CONSUMED
 |
FREE
```

Producer：

```text
Producer position
      |
CAS/FAA reservation
      |
获得 Slot N
      |
写 Descriptor
      |
slot.sequence.store(READY, release)
```

Consumer：

```text
head
 |
检查 slot.sequence
 |
READY?
 |
读取 Descriptor
 |
读取 Payload
 |
Release Slot
 |
head++
```

核心：

> Global tail/position 负责"位置分配"；Per-Slot Sequence 负责"这个 Slot 是否真正可读"。

这是 MPSC 正确性的关键。

---

# 15. MPSC Consumer

MPSC 只有一个 Consumer：

```text
Consumer owns head
```

因此 Consumer 不需要 CAS head。

流程：

```text
head
 |
slot.sequence == expected ?
 |
No  -> Empty / Not Ready
Yes
 |
Read Descriptor
 |
Read Payload
 |
Release Slot
 |
head++
```

所以 MPSC 的主要竞争点：

```text
Multiple Producers
       |
tail reservation
```

---

# 16. MPSC 性能优化

MPSC 是当前最值得优化的模式。

重点：

1. Producer reservation 减少 CAS 重试
2. 支持批量 reservation
3. Slot sequence 合理布局
4. 避免 Producer 之间 false sharing
5. Consumer 批量 Pop
6. 批量处理后减少 semaphore 操作
7. 统计 reservation retry

推荐测试：

```text
2P + 1C
4P + 1C
8P + 1C
16P + 1C
```

---

# 17. MPMC

MPMC：

```text
Multiple Producers
Multiple Consumers
```

```text
P1 --+
P2 --+
P3 --+--> MPMC Ring --+--> C1
P4 --+                 +--> C2
                      +--> C3
                      +--> CN
```

同时存在：

```text
Producer Claim
Consumer Claim
Publication
Reuse
```

因此复杂度最高。

---

# 18. MPMC：Sequence Ring

每个 Slot：

```cpp
struct Slot
{
    AtomicSequence sequence;
    MessageDescriptor descriptor;
};
```

Producer：

```text
读取 producer position
      |
检查 slot.sequence
      |
CAS reservation position
      |
获得 Slot
      |
写 Descriptor
      |
sequence.store(ready, release)
```

Consumer：

```text
读取 consumer position
      |
检查 slot.sequence
      |
CAS claim position
      |
获得 Slot
      |
读取 Descriptor
      |
sequence.store(free, release)
```

核心：

> Global Position 负责 Claim；Per-Slot Sequence 负责 Publication 与 Reuse。

---

# 19. 四种模式统一理解

| 模式 | Producer | Consumer | 主要竞争 |
|---|---:|---:|---|
| SPSC | 1 | 1 | 无 |
| SPMC | 1 | N | Consumer Claim |
| MPSC | N | 1 | Producer Claim |
| MPMC | N | N | Producer + Consumer Claim |

因此：

```text
SPSC
  -> 最低开销

SPMC
  -> 单边 Consumer 竞争

MPSC
  -> 单边 Producer 竞争，当前重点

MPMC
  -> 双边竞争，最复杂
```

---

# 20. Memory Ordering

核心规则：

> Payload/Descriptor 写入必须发生在 Publish 之前；Consumer 必须通过 Acquire 看到 Publish。

Producer：

```cpp
WritePayload();
WriteDescriptor();

slot.sequence.store(
    ready_sequence,
    std::memory_order_release);
```

Consumer：

```cpp
if (slot.sequence.load(
        std::memory_order_acquire) == ready_sequence)
{
    ReadDescriptor();
    ReadPayload();
}
```

逻辑：

```text
Producer writes
      |
release
      |
Publication
      |
acquire
      |
Consumer reads
```

因此 Payload 字段不需要全部 atomic。

---

# 21. Ownership 与 Memory Ordering 的关系

仅仅有 release/acquire 不够。

还必须满足：

```text
Producer Write
      |
Publish
      |
Consumer Read
      |
Release
      |
Producer Reuse
```

因此：

- Memory Ordering 解决可见性
- Ownership 解决生命周期
- Sequence/Generation 解决重用与 ABA

三者不能互相替代。

---

# 22. 跨进程 Atomic 注意事项

需要区分：

### 算法正确性

Release/Acquire 可以描述发布与读取之间的可见性关系。

### 跨进程实现正确性

C++ 标准的内存模型主要以线程为语义基础，因此 SHM Atomic 还必须验证：

- CPU 架构
- 编译器
- ABI
- 对齐
- atomic 是否真正适用于共享内存
- lock-free 属性
- 多进程编译环境兼容性

因此：

> Ring Atomic 必须在目标部署平台做实际验证，不能仅凭线程内测试得出跨进程结论。

Boost.Interprocess 的进程间同步原语可用于跨进程等待/同步。

---

# 23. Semaphore：最终 Notification

最终：

```text
Ring
 |
负责数据状态

Semaphore
 |
负责阻塞/唤醒
```

Producer：

```text
Publish
  |
semaphore.post()
```

Consumer：

```text
semaphore.wait()
  |
TryPop()
```

重要：

> Semaphore 不是消息状态来源。

Ring 才是事实来源。

---

# 24. 为什么选择 Semaphore

Semaphore 是计数型通知：

```text
post()
post()
post()
```

可以累计通知。

与 Ring 的：

```text
消息产生
```

模型天然匹配。

因此不再把：

```text
interprocess_condition
+
interprocess_mutex
```

作为最终数据面通知机制。

设计统一为：

```text
Lock-free Ring
+
interprocess_semaphore
```

---

# 25. Wait 正确性

不能：

```text
wait()
  |
认为一定有消息
  |
直接 Pop
```

正确：

```text
wait()
  |
检查 Stop
  |
TryPop()
  |
+-- Success -> Consume
|
+-- Failure -> Continue Wait
```

原因：

- Semaphore 是通知而不是 Queue 状态
- 多 Consumer 会竞争
- Batch 会改变通知/消息数量关系
- Stop 会唤醒等待者

---

# 26. Batch

高吞吐场景优先支持 Batch。

Producer：

```text
Batch reservation
      |
写多个 Descriptor
      |
Publication
      |
减少 Notify
```

Consumer：

```text
Wake
 |
TryPop
 |
TryPop
 |
TryPop
 |
...
 |
Empty
 |
Wait
```

收益：

- 减少 CAS
- 减少 Cache coherence
- 减少 semaphore 操作
- 减少上下文切换
- 提高吞吐

---

# 27. False Sharing

不要：

```text
Cache Line
+----------------+
| head | tail    |
+----------------+
```

建议：

```text
Cache Line A
+----------------------+
| producer position    |
+----------------------+

Cache Line B
+----------------------+
| consumer position    |
+----------------------+
```

MPSC/MPMC 还要考虑：

- Producer reservation position
- Consumer claim position
- Slot sequence
- Descriptor locality

---

# 28. Queue Full / Backpressure

Queue 是 bounded ring。

```text
Producer
   |
Ring
   |
越来越满
```

需要：

```text
TryPush -> false
```

或者：

```text
Push -> retry/block
```

支持策略：

```text
BUSY_SPIN   — TryPush/TryPop 自旋重试（最低延迟，CPU 占用最高）
YIELD       — 每次失败后 yield（延迟/CPU 折中）
BACKOFF     — 指数退避 sleep（1us→2us→...→1000us 上限）
BLOCK       — 信号量阻塞等待（CPU 占用最低，延迟最高；默认）
DROP        — 满时直接丢弃消息（TryPush 失败即返回 false）
REJECT      — 满时拒绝并返回错误（语义为拒绝服务）
```

Queue 本身不理解业务优先级。

---

# 29. Stop

Stop 必须独立于 Algorithm：

```text
Stop
 |
stop flag
 |
Notify / post
 |
唤醒等待者
 |
检查 stop
 |
退出
```

不依赖：

```text
100ms timeout
```

才能退出。

Timeout 只能作为兜底。

---

# 30. Crash Recovery

SHM 的重点风险：

```text
Process A crash
Process B still alive
```

需要处理：

- Acquire 后 crash
- Publish 前 crash
- Claim 后 crash
- Release 前 crash
- Producer crash
- Consumer crash

Pool 中可记录：

```text
owner process id
heartbeat
timestamp
generation
state
```

异常情况下：

```text
Dead process
    |
Salvage
    |
Recover Slot
```

Salvage 只用于异常路径，不能成为正常数据面操作。

---

# 31. Publish 前崩溃

```text
Acquire
   |
Write Payload
   |
CRASH
   X
Publish
```

此时 Payload 不应该进入 Queue。

恢复机制通过：

```text
owner
heartbeat
TTL
state
```

识别长期停留在：

```text
ALLOCATED
```

的 Slot 并回收。

---

# 32. Claim 后 Consumer 崩溃

```text
Claim
  |
Process
  |
CRASH
  X
Release
```

Payload 可能处于：

```text
CLAIMED
```

不能直接被其他 Consumer 重用。

需要：

```text
owner
heartbeat
TTL
```

结合进程死亡检测进行恢复。

恢复策略必须保守，避免误回收仍在处理的数据。

---

# 33. Ring Sequence 与 Payload Generation

两者必须区分。

### Ring Sequence

解决：

```text
Ring Slot
重用
ABA
wrap-around
```

### Payload Generation

解决：

```text
Pool Slot
重用
stale descriptor
```

因此：

```text
Ring Sequence
      |
Queue 层安全

Payload Generation
      |
Pool 层安全
```

两者都保留。

---

# 34. Broadcast / Multicast

普通 Queue 不定义：

```text
Broadcast
Multicast
```

因为：

```text
一个 Payload
 |
+-- C1
+-- C2
+-- C3
```

改变了 Ownership Model。

未来单独实现：

```text
Shared Payload
+
N Descriptor
+
RefCount
```

或其他 per-subscriber ownership 方案。

---

# 35. 与服务器架构结合

同机：

```text
Gateway
   |
Direct SHM
   |
Logic
```

典型 MPSC：

```text
Gateway Worker 1 --+
Gateway Worker 2 --+
Gateway Worker 3 --+--> MPSC --> Logic
Gateway Worker N --+
```

典型 SPSC：

```text
Logic --> Dedicated Worker
```

典型 SPMC：

```text
Dispatcher --> Worker Pool
```

典型 MPMC：

```text
N Producer --> MPMC --> N Consumer
```

跨机：

```text
Gateway
   |
 TCP/RPC
   |
Remote Logic
```

SHM Queue 不承担跨机传输。

---

# 36. MessageBus 定位

不把 MessageBus 放在普通单播数据面：

```text
Gateway
   |
Direct SHM
   |
Logic
```

而不是：

```text
Gateway
   |
MessageBus
   |
Logic
```

避免：

- 多一次转发
- 多一次调度
- 多一次同步
- 更高延迟
- 更复杂生命周期

未来 MessageBus 可用于：

- Broadcast
- Event
- Pub/Sub
- Control Plane

---

# 37. API 生命周期

Producer：

```cpp
auto msg = queue.Acquire(type, size);

if (!msg)
    return false;

auto payload = msg.Payload();

WritePayload(payload);

if (!queue.Publish(msg))
{
    queue.ReleaseBuffer(msg);
    return false;
}
```

Consumer：

```cpp
MessageDescriptor desc;

if (queue.TryPop(desc))
{
    auto payload = queue.GetPayload(desc);

    Process(payload);

    queue.Release(desc);
}
```

禁止：

> Release 后把裸 SHM 指针继续交给异步 callback。

如果业务需要异步持有，应增加 RAII：

```cpp
MessageGuard
```

由 Guard 生命周期自动 Release。

---

# 38. Hot Path

SPSC：

```text
Load
Store
Load
Store
```

SPMC：

```text
Load
CAS
Load
Store
```

MPSC：

```text
Producer reservation
+
Slot publication
```

MPMC：

```text
Producer reservation CAS
+
Consumer reservation CAS
+
Slot sequence
```

Hot Path 尽量避免：

```text
malloc
mutex
condition_variable
large memcpy
dynamic allocation
logging
```

---

# 39. 锁的最终定位

不是：

> 系统任何地方都不能有锁。

而是：

> 正常消息数据传输路径不能依赖 mutex。

允许锁存在于：

- 初始化
- 管理面
- Stop
- Recovery
- Debug
- Pool 管理路径

正常数据面：

```text
SPSC -> no CAS
SPMC -> Consumer CAS
MPSC -> Producer CAS/FAA
MPMC -> Producer + Consumer CAS
```

---

# 40. Algorithm 不运行时切换

Channel 创建时确定：

```cpp
ChannelConfig
{
    Algorithm algorithm;
    capacity;
    payload tiers;
    wait strategy;
    backpressure;
};
```

例如：

```text
Gateway -> Logic
    -> MPSC
```

运行期间不要：

```text
SPSC -> MPSC
```

动态改变。

原因：

- 并发语义变化
- Position 管理变化
- Ownership 变化
- Recovery 复杂度增加

---

# 41. Benchmark

## SPSC

```text
1P + 1C
```

## SPMC

```text
1P + 2C
1P + 4C
1P + 8C
```

## MPSC

```text
2P + 1C
4P + 1C
8P + 1C
16P + 1C
```

## MPMC

```text
2P + 2C
4P + 4C
8P + 8C
16P + 16C
```

Payload：

```text
64B
128B
256B
512B
1KB
4KB
16KB
64KB
```

---

# 42. Benchmark 指标

必须观察：

```text
Throughput
P50
P95
P99
CPU
Context Switch
Cache Miss
CAS Retry
Queue Full
Queue Empty
Pool Exhaustion
Allocation
Release
Semaphore Wakeup
```

MPSC/MPMC 增加：

```text
Producer contention
Consumer contention
Reservation retry
Publication delay
```

---

# 43. 测试要求

基础：

- Push/Pop
- Empty
- Full
- Wrap-around
- Capacity
- Stop

SPSC：

- 顺序性
- 无丢失
- 无重复
- 长时间运行

SPMC：

- 多 Consumer
- 无重复
- 无丢失
- CAS contention

MPSC：

- 多 Producer
- Reservation
- Publication
- 顺序规则
- Producer crash
- 高竞争

MPMC：

- 多 Producer
- 多 Consumer
- 无重复
- 无丢失
- Wrap-around
- Sequence
- ABA
- 高竞争

Pool：

- Acquire
- Release
- Exhaustion
- Generation
- stale descriptor
- Crash Recovery
- Salvage

---

# 44. 故障测试

必须验证：

```text
Producer kill -9
Consumer kill -9

Producer crash after Acquire
Producer crash before Publish

Consumer crash after Claim
Consumer crash before Release

Queue Full
Pool Exhaustion

Repeated Wrap-around
High contention
```

核心要求：

> 任何异常情况下，都不能让新消息错误使用旧 Payload。

---

# 45. 推荐目录

```text
src/thirdparty/ipc/shm/
├── CMakeLists.txt
├── shmatomic.h
├── shmcontainer.h
├── shmutil.h
├── shmmessagepool.h
├── shmmessagepool.cpp
│
├── pool/
│   ├── slotlayout.h        — PoolConfig / PoolStats / PoolHeader / ProcEntry (POD)
│   ├── shmslabpool.h       — 基础 slab pool 层
│   └── shmslabpool.cpp
│
└── messagequeue/
    ├── channelconfig.h       — ChannelConfig / OpenMode / 异常类 / 序列化
    ├── shmdescriptor.h       — MessageDescriptor / SlotState / SlotHandle / SlotHeader
    │                           (原 shmslot.h 已合并入此文件)
    │
    ├── shmqueue.h            — ShmQueue<Algorithm> 模板实现
    ├── shmqueuebase.h        — ShmQueueBase 抽象基类 + MessageGuard RAII
    │
    ├── spscalgorithm.h       — SPSC: lock-free atomic + semaphore
    ├── spsicalgorithm.cpp
    ├── spmcalgorithm.h       — SPMC: Consumer CAS claim
    ├── spmcalgorithm.cpp
    ├── mpscalgorithm.h       — MPSC: CAS + 4-state (FREE→RESERVED→READY→CONSUMED)
    ├── mpscalgorithm.cpp
    ├── mpmcalgorithm.h       — MPMC: Vyukov sequence ring
    ├── mpmcalgorithm.cpp
    │
    ├── shmmessagequeue.h     — MakeShmQueue 工厂 + 类型别名 (inline)
    │
    ├── pullmessagequeue.h    — PullMessageQueueT<Algorithm> (独立线程 + callback)
    ├── pullmessagequeue.cpp
    ├── pushmessagequeue.h    — PushMessageQueueT<Algorithm> (多 channel 发送)
    ├── pushmessagequeue.cpp
    │
    ├── rpcmessagequeue.h    — RpcMessageQueue<Algorithm> (RPC 请求/响应)
    └── rpcmessagequeue.cpp
```

---

# 46. 最终架构

```text
                         Application
                              |
                              v
                     ShmMessageQueue
                              |
                              v
                     ShmQueue<Algorithm>
                              |
          +-------------------+-------------------+
          |                   |                   |
         SPSC                SPMC                MPSC
          |                   |                   |
        1P/1C               1P/NC               NP/1C
          |                   |                   |
          +-------------------+-------------------+
                              |
                             MPMC
                             NP/NC
                              |
                              v
                     Descriptor Ring
                              |
                 +------------+------------+
                 |                         |
              Sequence                Descriptor
                 |                         |
                 +------------+------------+
                              |
                              v
                        MessagePool
                              |
                   +----------+----------+
                   |          |          |
                 Tiny       Small      Large
                   |          |          |
                   +----------+----------+
                              |
                              v
                           Payload

             Notification
                    |
                    v
          interprocess_semaphore
                    |
              Block / Wakeup
```

---

# 47. 最终技术原则

1. Descriptor 与 Payload 分离。
2. Queue 只传 Descriptor，不传大 Payload。
3. Payload 采用单所有权转移。
4. SPSC 不使用 CAS。
5. SPMC 只在 Consumer Claim 上产生竞争。
6. MPSC 必须区分 Reservation 与 Publication。
7. MPMC 必须同时处理 Producer Claim、Consumer Claim、Publication 和 Slot Reuse。
8. Ring Sequence 与 Payload Generation 是两个不同层次的安全机制。
9. Release/Acquire 负责数据可见性；Ownership 负责避免并发读写。
10. Semaphore 负责阻塞/唤醒，不负责消息正确性。
11. 正常数据路径不使用 mutex。
12. 不直接使用 Boost.Lockfree Queue 作为跨进程 SHM 容器，但可以借鉴成熟算法思想。
13. Broadcast/Multicast 不进入普通 Queue 的 Ownership Model。
14. MPSC 是当前架构的重点优化模式。
15. Algorithm 在 Channel 创建时确定，运行期间不动态改变。
16. 正常数据面优先预分配、复用和 Batch。
17. Recovery/Salvage 属于异常路径，不得拖慢正常 Hot Path。
18. 以实际目标平台的跨进程验证和 benchmark 作为最终验收依据。

---

# 48. 最终结论

最终方案不是"一个万能无锁队列"，而是：

```text
             ShmQueue<Algorithm>
                     |
       +------+------+------+------+
       |      |      |      |
      SPSC   SPMC   MPSC   MPMC
       |      |      |      |
       +------+------+------+
                     |
              Descriptor Ring
                     |
                MessagePool
                     |
                  Payload
```

四种模式各自承担最适合的并发拓扑：

```text
SPSC
-> 最低开销

SPMC
-> 单 Producer，多 Consumer

MPSC
-> 多 Producer，单 Consumer
-> 当前架构最重要的汇聚模型

MPMC
-> 多 Producer，多 Consumer
-> 最完整、最复杂
```

最终复杂度被严格分层：

```text
Algorithm
    -> 并发

Ring
    -> Descriptor

Pool
    -> Payload 生命周期

Sequence
    -> Ring Slot 重用

Generation
    -> Payload Slot 重用

Release/Acquire
    -> 可见性

Semaphore
    -> 阻塞/唤醒

Recovery
    -> 异常进程
```

这套设计作为后续 SHM Queue 实现、单元测试、跨进程压力测试和 100K TPS 架构性能验证的统一基线。