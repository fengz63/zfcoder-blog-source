---
title: kube-apiserver 源码解读（九）Watch 机制与 Watch Cache 实现
date: 2026-07-25 16:00:00
tags:
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 引言

在 Kubernetes 体系中，**Watch 机制**是控制平面中最核心的通信模式。kubelet、scheduler、controller-manager 等组件都通过 Watch 来实时跟踪资源变化，而不需要轮询 apiserver。

Watch 机制的实现横跨三个层次：

```
Client (reflector/informer)
    │
    ▼
[Watch Cache - Cacher]       ← 内存缓存层，服务所有 List/Watch 请求
    │
    ▼
[etcd3 Watch - watchChan]    ← 底层存储层，与 etcd 交互
    │
    ▼
[etcd]                       ← 持久化存储
```

本文将从最上层的 `watch.Interface` 开始，逐层深入直到 etcd 的 MVCC Watch，完整解析 Watch 机制的实现细节。

<!--more-->

## 2. Watch 核心抽象：watch.Interface

所有 Watch 相关类型定义在 `staging/src/k8s.io/apimachinery/pkg/watch/watch.go`：

```go
type Interface interface {
    Stop()
    ResultChan() <-chan Event
}

type EventType string

const (
    Added    EventType = "ADDED"
    Modified EventType = "MODIFIED"
    Deleted  EventType = "DELETED"
    Bookmark EventType = "BOOKMARK"
    Error    EventType = "ERROR"
)

type Event struct {
    Type   EventType
    Object runtime.Object
}
```

这个接口极其简洁——只有 `Stop()` 和 `ResultChan()` 两个方法。任何实现了该接口的类型都可以作为 Watch 事件的消费者。无论底层使用的是 etcd 流、Broadcaster 扇出、还是测试用的 FakeWatcher，对调用方来说 `watch.Interface` 的行为是一致的：

1. 调用 `ResultChan()` 获得一个只读 channel
2. 从 channel 中读取 `Event`：`ADDED`、`MODIFIED`、`DELETED`、`BOOKMARK`、`ERROR`
3. 需要停止时调用 `Stop()`，之后 channel 会被关闭

### Broadcaster：通用的扇出（Fan-Out）模式

`staging/src/k8s.io/apimachinery/pkg/watch/mux.go` 中实现了 `Broadcaster`——一个将单个事件源分发到多个 watcher 的通用组件：

```go
type Broadcaster struct {
    watchers            map[int64]*broadcasterWatcher
    incoming            chan Event
    watchQueueLength    int
    fullChannelBehavior FullChannelBehavior
}
```

Broadcaster 的核心是一个 `loop()` goroutine：

```go
func (m *Broadcaster) loop() {
    for event := range m.incoming {
        if event.Type == internalRunFunctionMarker {
            event.Object.(functionFakeRuntimeObject)()
            continue
        }
        m.distribute(event)
    }
    m.closeAll()
    m.distributing.Done()
}
```

当事件到达 `incoming` 通道时，`distribute()` 会将事件发送到每个 watcher 的 `result` 通道。当某个 watcher 的 channel 满了时，有两种策略：
- `WaitIfChannelFull`：阻塞等待直到 channel 有空位
- `DropIfChannelFull`：跳过该 watcher，不阻塞

虽然 Cacher 没有直接使用 `Broadcaster`，它背后的思想（单一事件源 → 多 watcher 分发）是 Watch Cache 的核心设计哲学。

## 3. etcd3 Watch：最底层的事件源

`staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go` 实现了与 etcd 的直接 Watch 交互。

### watchChan 结构

```go
type watchChan struct {
    watcher           *watcher
    key               string
    initialRev        int64
    recursive         bool
    progressNotify    bool
    internalPred      storage.SelectionPredicate
    incomingEventChan chan *event
    resultChan        chan watch.Event
}
```

`Watch()` 方法创建一个 `watchChan`，然后启动两个 goroutine：

```go
func (w *watcher) Watch(ctx context.Context, key string, rev int64, opts storage.ListOptions) (watch.Interface, error) {
    startWatchRV, _ := w.getStartWatchResourceVersion(ctx, rev, opts)
    wc := w.createWatchChan(ctx, key, startWatchRV, opts.Recursive, opts.ProgressNotify, opts.Predicate)
    go wc.run(isInitialEventsEndBookmarkRequired(opts), areInitialEventsRequired(rev, opts))
    return wc, nil
}
```

`run()` 内部再启动两个并发 goroutine：

```go
func (wc *watchChan) run(initialEventsEndBookmarkRequired, forceInitialEvents bool) {
    watchClosedCh := make(chan struct{})
    var resultChanWG sync.WaitGroup

    resultChanWG.Add(1)
    go func() {
        defer resultChanWG.Done()
        wc.startWatching(watchClosedCh, initialEventsEndBookmarkRequired, forceInitialEvents)
    }()
    wc.processEvents(&resultChanWG)
    // ...
}
```

流程如下：

```
startWatching() goroutine               processEvents() goroutine
    │                                        │
    ├─ 1. sync() 全量 LIST（if rev=0）       │
    │     分页查询 etcd，逐条 queueEvent()    │
    │                                        │
    ├─ 2. 等 sync 结束，确定 initialRev       │
    │                                        │
    ├─ 3. etcd.Watch() 打开流                │
    │     WithRev(initialRev+1)              │
    │     WithPrevKV()                       │
    │     WithProgressNotify()               │
    │                                        │
    ├─ 4. 收到 etcd WatchResponse            │
    │     → 解析 events                      │
    │     → queueEvent() → incomingEventChan──┼──→ 读取 incomingEventChan
    │                                        │    → transform() (解码+过滤)
    │                                        │    → sendEvent() → resultChan
    └─ ...                                   └─ ...
```

### resultChan 被谁消费？

`watchChan` 实现了 `watch.Interface`，它的 `ResultChan()` 返回的就是这个 `resultChan`。那么**谁在循环读这个 channel**？

答案是 Cacher 内部创建的 **Reflector**。看第 4 节会详细展开，先简单说明：

```
etcd3 watchChan.resultChan
    ↑                                 ← reflector.ListAndWatch() 内部循环调用
                                          watch.ResultChan() 读取 event
    |
    谁调用的？ → Cacher.newCacherFromConfig() 中创建的：
                    listerWatcher = NewListerWatcher(config.Storage, ...)
                    //               ↑ config.Storage 就是 etcd3 store
                    reflector = NewReflector(listerWatcher, watchCache, ...)
                    //  ↑ reflector.ListAndWatch() 内部调用 listerWatcher.Watch()
                    //    返回的就是这个 watchChan，然后循环读 resultChan
                    //    读到的事件 → watchCache.Add/Update/Delete
```

即：**resultChan 的消费者是 Cacher 内部的一个 Reflector goroutine**。这个 Reflector 把 etcd 来的原始事件写入 watchCache，驱动整个缓存更新。

### 同步阶段（sync）

当从 `resourceVersion=0` 开始 Watch 时，需要先拉取全量数据作为初始状态：

```go
func (wc *watchChan) sync() error {
    // 分页 LIST etcd
    for {
        getResp, err = wc.watcher.client.KV.Get(wc.ctx, preparedKey, opts...)
        for _, kv := range getResp.Kvs {
            // 每个 kv 对包装成 event，标记 isCreated=true
            wc.queueEvent(...)
        }
        if !getResp.More {
            break
        }
    }
}
```

### 持续 Watch 阶段

sync 完成后，`startWatching()` 打开 etcd 的 `Watch()` 流：

```go
func (wc *watchChan) startWatching(...) {
    // ...
    wc.etcdWatch = wc.watcher.client.Watch(
        wc.ctx,
        wc.key,
        opts...,
    )
    for wresp := range wc.etcdWatch {
        for _, e := range wresp.Events {
            // 解析 etcd Event → 内部 event 结构
            ev := wc.parseEvent(e)
            wc.queueEvent(ev)
        }
    }
}
```

etcd 支持 `WithProgressNotify()`，会在无事件时定期发送进度通知，转换为 `BOOKMARK` 类型的事件。

### 事件处理管道

`processEvents()` goroutine 读取 `incomingEventChan`，执行 `transform()`：

```go
func (wc *watchChan) processEvents(resultChanWG *sync.WaitGroup) {
    // ...
    for {
        select {
        case e := <-wc.incomingEventChan:
            // 并发解码 concurrentWatchDecode (最多 10 goroutine)
            // 但通过 processingQueue 保证顺序
            curEvent := *e
            resultChanWG.Add(1)
            go func(e *event) {
                defer resultChanWG.Done()
                wc.transformAndSendEvent(e)
            }(&curEvent)
        }
    }
}
```

`transform()` 负责：
1. 用 `codec.Decode()` 将 etcd 的 raw bytes 反序列化为 `runtime.Object`
2. 如果配置了 transformer（如加密），先解密
3. 用 `Predicate` 做 label/field 过滤
4. 确定最终的 `EventType`（Added/Modified/Deleted/Bookmark）

`transformAndSendEvent()` 把处理完的 `watch.Event` 写入 `resultChan`。**谁在读？** 见下文第 4 节——Cacher 内部 Reflector 在 `ListAndWatch()` 循环中调用 `watch.ResultChan()` 读取这个 channel。

## 4. Watch Cache：Cacher 架构

Cacher 是 Watch 机制的核心组件，位于 `staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go`。它实现了两个关键功能：

1. **缓存所有资源状态**：所有的 LIST 请求从内存中返回，不需要查 etcd
2. **扇出 Watch 事件**：多个 Watch 请求共享一个底层 etcd Watch

### 整体结构

```go
type Cacher struct {
    incoming              chan watchCacheEvent    // watchCache → dispatch 的事件管道
    watchCache            *watchCache              // 循环事件缓冲区 + 当前状态快照
    reflector             *cache.Reflector         // 底层 ListAndWatch（连接 etcd3 storage）
    watchers              indexedWatchers           // 所有活跃的 cacheWatcher
    watcherIdx            int                      // 单调递增的 watcher ID
    dispatchTimeoutBudget timeBudget               // 分发超时预算
    bookmarkWatchers      *watcherBookmarkTimeBuckets  // 书签时间桶
    // ...
}
```

Cacher 启动时在 `NewCacherFromConfig()` 中创建以下 goroutine：

```
dispatchEvents() goroutine     ← 从 incoming 读取，分发到所有 watcher
    │
progressRequester.Run()        ← 条件性进度请求
    │
wait.Until(startCaching)       ← reflector.ListAndWatch() 驱动 watchCache
```

### Cacher 内部的 Reflector 连的是谁？

关键在 `NewCacherFromConfig()` 的初始化代码（第 437-454 行）：

```go
// 1. 创建 listerWatcher，它包装的是 config.Storage——即 etcd3 storage
listerWatcher := NewListerWatcher(config.Storage, resourcePrefix, config.NewListFunc, contextMetadata)

// 2. 创建 Reflector，传入 listerWatcher 和 watchCache
//    listerWatcher 负责 List/Watch（去 etcd3）
//    watchCache 负责 Store（接收事件写入）
reflector := cache.NewNamedReflector(reflectorName, listerWatcher, obj, watchCache, 0)
```

所以这个 Reflector 的完整路径是：

```
reflector.ListAndWatch()
    ├── List() → listerWatcher.List()
    │               → config.Storage.GetList()     → etcd3.KV.Get()       → 全量 LIST
    │               → watchCache.Replace(list, rv)   ← 结果写入 watchCache
    │
    └── Watch() → listerWatcher.Watch()
                     → config.Storage.Watch()       → etcd3.watchChan     → 打开 Watch 流
                     → 循环 resultChan：
                          read event → watchCache.Add/Update/Delete
```

即这个 Reflector **不是** client-go 在外面用的那个，而是 apiserver **内部**用来把 etcd 数据灌入 watchCache 的桥梁：`etcd → listerWatcher → Reflector → watchCache`。

### 4.1 watchCache：环形事件缓冲区

`watchCache` 定义在 `staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go`，是一个"滑动窗口"式的环形缓冲区：

```go
type watchCache struct {
    cache      []*watchCacheEvent   // 环形缓冲区
    startIndex int                  // 最旧事件索引
    endIndex   int                  // 下一个写入位置
    capacity   int                  // 动态容量 (100 ～ 102400)
    store      store.Indexer        // 当前状态快照（用于 LIST 和初始事件）
    resourceVersion uint64          // 最新 RV
    // ...
}
```

**watchCacheEvent** 比标准 `watch.Event` 包含更多信息：

```go
type watchCacheEvent struct {
    Type            watch.EventType
    Object          runtime.Object
    ObjLabels       labels.Set
    ObjFields       fields.Set
    PrevObject      runtime.Object      // 用于过滤：旧对象是否匹配 watcher 条件
    PrevObjLabels   labels.Set
    PrevObjFields   fields.Set
    Key             string
    ResourceVersion uint64
    RecordTime      time.Time
}
```

**事件写入流程**（`processEvent` 方法被 Reflector 调用）：

```
Reflector 收到 etcd 事件
    │
    ├── 1. 计算 key（对象标识）
    ├── 2. 获取 labels/fields
    ├── 3. 从 store 中查找旧对象
    ├── 4. 加锁：
    │       ├── updateCache() → 追加到环形缓冲区（必要时扩容）
    │       ├── 更新 resourceVersion
    │       ├── 更新 store（Add/Update/Delete）
    │       ├── cond.Broadcast() 唤醒等待者
    │       └── 记录快照（如果启用了 ListFromCacheSnapshot）
    ├── 5. 解锁
    └── 6. eventHandler(wcEvent) → 推送到 c.incoming
```

**动态容量调整**：

```go
func (w *watchCache) resizeCacheLocked(eventTime time.Time) {
    // 缓存满了且所有事件都在 eventFreshDuration 内 → 容量翻倍
    if w.isCacheFullLocked() &&
       eventTime.Sub(w.cache[w.startIndex%w.capacity].RecordTime) < w.eventFreshDuration {
        capacity := min(w.capacity*2, w.upperBoundCapacity)
        // ...
    }
    // 最近 1/4 的事件已过期 → 容量减半
    if w.isCacheFullLocked() &&
       eventTime.Sub(w.cache[(w.endIndex-w.capacity/4)%w.capacity].RecordTime) > w.eventFreshDuration {
        capacity := max(w.capacity/2, w.lowerBoundCapacity)
        // ...
    }
}
```

初始容量为 100，在高频事件场景下可以动态扩大到 102400，同时避免内存爆炸。

### 4.2 watchCacheInterval：惰性事件迭代器

当新的 Watch 请求到来时，需要从 watchCache 中读取历史事件和当前状态。`watchCacheInterval` 是一个惰性填充的迭代器：

```go
type watchCacheInterval struct {
    startIndex, endIndex  int
    indexer               indexerFunc
    indexValidator        indexValidator
    buffer                *watchCacheIntervalBuffer  // 批处理缓冲区 (100个)
    resourceVersion       uint64
    initialEventsEndBookmark *watchCacheEvent
}
```

watchCacheInterval 是新 watcher 用来"追赶历史"的迭代器。processInterval() 循环调用 Next() 逐条取出历史事件，发送给客户端。当追赶完成，watcher 切到 process() 主循环，从 input 通道收增量事件。内部 buffer 每次加锁批量取 100 个到内存，减少锁竞争。

```
Next() 调用
    │
    ├── buffer 有数据 → 直接返回
    │
    └── buffer 耗尽
        ├── 加锁
        ├── fillBuffer()：批量取最多 100 个事件
        ├── 验证 indexValidator（检查缓冲区是否因扩容而失效）
        ├── 解锁
        └── 返回事件
```

### 4.3 Cacher.Watch()：创建新的 Watch 流

`Cacher.Watch()` 是 Watch 请求的入口，完整的流程如下：

```go
func (c *Cacher) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
    // Step 1: 准备 key 和 predicate
    key, _ := c.prepareKey(key, opts.Recursive)

    // Step 2: 解析请求的 ResourceVersion
    requestedWatchRV, _ := c.versioner.ParseResourceVersion(opts.ResourceVersion)

    // Step 3: 等待 Cacher ready
    readyGeneration, _ := c.ready.checkAndReadGeneration()

    // Step 4: 确定 watcher 的作用域（namespace/name）
    scope := determineScope(ctx, pred)

    // Step 5: 确定 trigger value（基于索引的 watcher 优化）
    triggerValue, triggerSupported := determineTrigger(pred)

    // Step 6: 计算需要的 ResourceVersion
    requiredResourceVersion, _ := c.getWatchCacheResourceVersion(ctx, requestedWatchRV, opts)

    // Step 7: 等待 watchCache 追上 requiredResourceVersion
    c.waitUntilWatchCacheFreshAndForceAllEvents(ctx, requiredResourceVersion, opts)

    // Step 8: 加锁获取 cacheInterval
    c.watchCache.RLock()
    cacheInterval, _ := c.watchCache.getAllEventsSinceLocked(requiredResourceVersion, key, opts)

    // Step 9: 如果是 WatchList 请求，附加 InitialEventsEndBookmark
    c.setInitialEventsEndBookmarkIfRequested(cacheInterval, opts, c.watchCache.resourceVersion)

    // Step 10: 创建 cacheWatcher，注册到 Cacher
    watcher := newCacheWatcher(chanSize, filterFunc, emptyFunc, ...)
    c.Lock()
    watcher.forget = forgetWatcher(c, watcher, ...)
    watcher.setBookmarkAfterResourceVersion(bookmarkAfterResourceVersionFn())
    c.watchers.addWatcher(watcher, ...)
    c.watcherIdx++
    c.Unlock()

    // Step 11: 启动 watcher 处理 goroutine
    go watcher.processInterval(ctx, cacheInterval, requiredResourceVersion)
    return watcher, nil
}
```

**关于 ResourceVersion 的语义：**

| ResourceVersion | 行为 |
|----------------|------|
| `0` | 从当前最新状态开始，不会阻塞等待 |
| `> 0` | 返回该版本之后的事件，阻塞直到 cache 赶上该版本（最多 3s） |
| `""` (未指定) | 返回最新版本之后的事件 |

### 4.4 cacheWatcher：每个 Watch 请求一个 goroutine

`cacheWatcher` 是实际返回给 HTTP handler 的 `watch.Interface` 实现：

```go
type cacheWatcher struct {
    input     chan *watchCacheEvent    // dispatchEvents 写入 ← Cacher 扇入
    result    chan watch.Event         // HTTP WatchServer 读取 → 序列化发客户端
    done      chan struct{}
    filter    filterWithAttrsFunc      // label/field 过滤函数
    forget    func(bool)               // 回调：从 Cacher 中注销
    bookmarkAfterResourceVersion uint64
    state     int                      // 三态机：WaitingForBookmark / BookmarkReceived / BookmarkSent
    // ...
}
```

`cacheWatcher` 三个 channel 的分工：

| channel | 谁写入 | 谁读取 | 数据流方向 |
|---------|--------|--------|-----------|
| `input` | dispatchEvents goroutine | `process()` goroutine | incoming → filtered events |
| `result` | `sendWatchCacheEvent()` | **HTTP WatchServer** | serialized events → client |
| `done` | `stopLocked()` (Cacher) | `process()` goroutine | 终止信号 |

`Cacher.Watch()` 返回 `cacheWatcher` 给 `handleWatch()`，后者把它传给 `WatchServer`：

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/handlers/get.go:282
watcher, err := rw.Watch(ctx, &opts)
// watcher 就是 cacheWatcher（实现了 watch.Interface）

// 创建 WatchServer，传入 watcher
server := &WatchServer{Watching: watcher}

// 启动 HTTP 流式响应
server.ServeHTTP(w, req)
```

`WatchServer.HandleHTTP()` 中循环读 `result` channel：

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/handlers/watch.go:293
ch := s.Watching.ResultChan()   // ← cacheWatcher.result
for {
    select {
    case event, ok := <-ch:
        if !ok { return }
        watchEncoder.Encode(event)   // JSON/Protobuf 序列化
        flusher.Flush()              // chunked HTTP flush → 客户端
    }
}
```

即：**`cacheWatcher.result` → `WatchServer.HandleHTTP` → `http.ResponseWriter` → HTTP chunked 流 → 客户端**

**processInterval + process：事件的二级流水线**

`processInterval()` 先处理初始化事件（从 cacheInterval 中读取），然后进入 `process()` 主循环：

```go
func (c *cacheWatcher) processInterval(ctx context.Context, cacheInterval *watchCacheInterval, resourceVersion uint64) {
    // Phase 1: 发送所有初始化事件
    for {
        event, err := cacheInterval.Next()
        if event == nil { break }
        c.sendWatchCacheEvent(event)
    }

    // Phase 1.5: 如果是 WatchList 请求，发送 InitialEventsEndBookmark
    if cacheInterval.initialEventsEndBookmark != nil {
        c.sendWatchCacheEvent(cacheInterval.initialEventsEndBookmark)
    }

    // Phase 2: 进入主循环，处理后续增量事件
    c.process(ctx, resourceVersion)
}
```

`process()` 主循环从 `input` channel 读取事件：

```go
func (c *cacheWatcher) process(ctx context.Context, resourceVersion uint64) {
    for {
        select {
        case event, ok := <-c.input:
            if !ok { return }
            // 确保不发送比已发送版本更旧的事件
            if event.ResourceVersion > resourceVersion ||
               (event.Type == watch.Bookmark && event.ResourceVersion == resourceVersion && !c.wasBookmarkAfterRvSent()) {
                c.sendWatchCacheEvent(event)
            }
        case <-ctx.Done():
            return
        }
    }
}
```

**事件类型转换（convertToWatchEvent）：**

核心过滤逻辑——Watcher 只关心匹配其 label/field selector 的对象：

```go
func (c *cacheWatcher) convertToWatchEvent(event *watchCacheEvent) *watch.Event {
    if event.Type == watch.Bookmark { // 书签事件：直接转发
        return &watch.Event{Type: watch.Bookmark, Object: event.Object.DeepCopyObject()}
    }

    curObjPasses := event.Type != watch.Deleted && c.filter(event.Key, event.ObjLabels, event.ObjFields, event.Object)
    oldObjPasses := false
    if event.PrevObject != nil {
        oldObjPasses = c.filter(event.Key, event.PrevObjLabels, event.PrevObjFields, event.PrevObject)
    }

    switch {
    case curObjPasses && !oldObjPasses: // 新匹配 → Added
        return &watch.Event{Type: watch.Added, Object: getMutableObject(event.Object)}
    case curObjPasses && oldObjPasses:  // 前后都匹配 → Modified
        return &watch.Event{Type: watch.Modified, Object: getMutableObject(event.Object)}
    case !curObjPasses && oldObjPasses: // 不再匹配 → Deleted（用旧对象）
        oldObj := getMutableObject(event.PrevObject)
        updateResourceVersion(oldObj, c.versioner, event.ResourceVersion)
        return &watch.Event{Type: watch.Deleted, Object: oldObj}
    }
    return nil // 都不匹配 → 跳过
}
```

这种设计保证了 Watcher 看到的事件语义正确：即使我只 Watch 某个 namespace 的 Pod，当一个 pod 从我的 namespace 移动到另一个时，我会收到 `DELETED` 事件（旧对象），另一个 namespace 的 watcher 会收到 `ADDED` 事件。

### 4.5 事件分发管道（dispatchEvents）

watchCache 产生的事件通过 `processEvent()` 推送到 Cacher 的 `incoming` 通道，然后被 `dispatchEvents()` goroutine 消费：

```
dispatchEvents() goroutine
    │
    ├── case event ← incoming:
    │       ├── 忽略 Bookmark 类型（来自存储层的 bookmark 频率太高）
    │       ├── dispatchEvent(&event)
    │       └── 更新 lastProcessedResourceVersion
    │
    ├── case ← bookmarkTimer (≈1s jitter):
    │       ├── 创建合成 Bookmark 事件
    │       ├── 设置 ResourceVersion = lastProcessedResourceVersion
    │       └── dispatchEvent(bookmarkEvent)
    │
    └── case ← stopCh: 退出
```

**dispatchEvent 的详细流程：**

```
startDispatching(event)
    │
    ├── 加锁
    ├── 如果是 Bookmark 事件：
    │       └── popExpiredWatchersThreadUnsafe() 取出到期的 watcher
    │
    ├── 如果是普通事件：
    │       ├── 从 allWatchers 查找 namespace/name 匹配的 watcher
    │       └── 从 valueWatchers 查找 trigger value 匹配的 watcher
    │
    ├── 解锁
    │
    ├── 设置 cachingObject（延迟序列化缓存）
    │
    ├── 非阻塞发送给所有 watcher
    ├── 阻塞的 watcher：带超时发送（dispatchTimeoutBudget）
    │       └── 超时仍未发送成功 → forcely forget（终止该 watcher）
    │
    └── finishDispatching()
            ├── 实际停止待终止的 watcher
            └── 将过期的 bookmark watcher 重新入队
```

**dispatchTimeoutBudget** 是一个时间预算系统，防止一个慢 watcher 拖慢整个分发：

```go
// dispatchEvent 中对阻塞 watcher 的处理
timeout := c.dispatchTimeoutBudget.takeAvailable()
c.timer.Reset(timeout)

for _, watcher := range c.blockedWatchers {
    if !watcher.add(event, timer) {
        timer = nil  // 超时后直接终止 watcher
    }
}
c.dispatchTimeoutBudget.returnUnused(timeout - time.Since(startTime))
```

### 4.6 Bookmark 机制

Bookmark 是 Watch 协议的"心跳"事件，只包含 `ResourceVersion` 字段。它的核心作用：确保客户端在断线重连时，可以从最新的 bookmark RV 开始 Watch，既不丢失事件也不重复接收事件。

**Bookmark 有三条路径：**

```
路径1：etcd ProgressNotify 事件（被 Cacher 丢弃）
    etcd → watchChan → Cacher.incoming → dispatchEvents 丢弃（详见下文）

路径2：Cacher 合成 Bookmark（真正下发给客户端的）
    dispatchEvents timer (~1s 抖动) → 创建 Bookmark → dispatchEvent → watcher

路径3：WatchList InitialEventsEndBookmark
    Cacher.Watch() 中创建 → cacheInterval.initialEventsEndBookmark
    → processInterval() 中在所有初始化事件后发送
```

**路径1 为什么会被丢弃？** 关键在 `dispatchEvents` 第 914-917 行：

```go
// Don't dispatch bookmarks coming from the storage layer.
// They can be very frequent (even to the level of subseconds)
// to allow efficient watch resumption on kube-apiserver restarts,
// and propagating them down may overload the whole system.
if event.Type != watch.Bookmark {
    c.dispatchEvent(&event)
}
```

原因：etcd 的 `WithProgressNotify()` 可以让 etcd 在**无数据变化时也高频发送进度通知**（亚秒级），目的是让 apiserver 重启后 Watch 流能快速恢复。但 Cacher 如果把这个 sub-second 的 bookmark 扇出给所有 watcher，网络开销太大。

Cacher 的做法是：

```
incoming 收到 etcd bookmark
    → 只更新 lastProcessedResourceVersion，不下发
    → 自己控制频率：bookmarkTimer 每 ~1s 用 lastProcessedResourceVersion 合成新 bookmark
    → dispatchEvent(合成bookmark)
```

相当于把 etcd 的高频 bookmark 聚合成**可控频率**（~1 秒）的 bookmark 再下发。

**bookmarkAfterResourceVersion 三态机：**

WatchList 请求（设置 `SendInitialEvents=true`）需要确保客户端收到初始化事件后才收到 bookmark：

```
cacheWatcher.state 三态机：
    │
    ├── WaitingForBookmark (初始状态)
    │       └── 收到 RV >= bookmarkAfterResourceVersion 的 bookmark
    │           → BookmarkReceived
    │
    ├── BookmarkReceived
    │       └── bookmark 被发送到 result channel
    │           → BookmarkSent
    │
    └── BookmarkSent (最终状态)
```

这个三态机的作用：
- `WaitingForBookmark` 时，即使 watcher 阻塞，也会被 `graceful` 终止（drain input buffer 再关闭），确保客户端能收到书签
- `BookmarkSent` 之后，watcher 可以直接被终止，客户端可以安全地重连

**bookmarkWatchers 时间桶调度：**

为了高效地调度数千个 watcher 的 bookmark 发送，Cacher 使用时间桶（1 秒粒度）：

```go
type watcherBookmarkTimeBuckets struct {
    watchersBuckets   map[int64][]*cacheWatcher // key: 秒级时间戳
    createTime        time.Time
    startBucketID     int64
    bookmarkFrequency time.Duration
}

func (t *watcherBookmarkTimeBuckets) popExpiredWatchersThreadUnsafe() [][]*cacheWatcher {
    // 从 startBucketID 到 currentBucketID 的所有桶
    for ; t.startBucketID <= currentBucketID; t.startBucketID++ {
        if watchers, ok := t.watchersBuckets[t.startBucketID]; ok {
            // 取出该秒内所有过期的 watcher
        }
    }
}
```

每个 watcher 的下一次 bookmark 时间由 `nextBookmarkTime()` 决定：

- 如果 `bookmarkAfterResourceVersion` 还未收到 → 立即调度
- 常规情况 → 每 ~1 分钟一次 bookmark
- 如果有 deadline → 在 deadline 前 2 秒额外发送一次

### 4.7 cachingObject：延迟序列化缓存

`cachingObject` 是一个优化技巧，避免在分发事件时对同一对象重复序列化：

```go
type cachingObject struct {
    object runtime.Object          // 原始对象
    lock   sync.Mutex
    // 按编码格式缓存的序列化结果
    serializations map[string]result
}
```

在 `dispatchEvent` 中，对于非 bookmark 事件会调用 `setCachingObjects()`，将 `watchCacheEvent.Object` 和 `watchCacheEvent.PrevObject` 包装为 `cachingObject`。

当 `cacheWatcher.sendWatchCacheEvent()` 将事件放入 `result` 通道后，HTTP handler 会在编码时调用 `cachingObject` 的各种序列化方法。如果是同一对象发送给多个 watcher（例如多个 watcher 都 Watch 同一个 Pod），第二次序列化时直接返回缓存结果，避免重复的 `DeepCopyObject()` 和编码。

## 5. 完整事件流

从 etcd 到客户端，事件经过 **6 个阶段、4 个 channel** 的串联管道：

```
Legend:
    ──→  数据流方向
    chan: channel 名称（谁写 → 谁读）
    [Layer]  处理阶段

           etcd MVCC 变更
               │
               │ clientv3.Watch() with WithPrevKV
               ▼
 ┌─────────────────────────┐
 │ Layer 1: etcd3 watch    │
 │ startWatching goroutine │
 │   sync() 全量 LIST      │
 │   WatchResponse 循环    │
 └─────────┬───────────────┘
           │
           │ queueEvent()
           ▼
     chan: incomingEventChan (startWatching → processEvents)
           │
           ▼
 ┌─────────────────────────┐
 │ Layer 2: etcd3 watch    │
 │ processEvents goroutine │
 │   transform():          │
 │    decode + decrypt     │  ← etcd raw bytes → runtime.Object
 │    + filter → watch.Event
 └─────────┬───────────────┘
           │
           │ sendEvent()
           ▼
     chan: resultChan (watchChan → Reflector)
           │
           ▼
 ┌──────────────────────────────┐
 │ Layer 3: Cacher 内部 Reflector │
 │   ListAndWatch()             │
 │   循环读 watch.ResultChan()  │
 │   → watchCache.Add/Update/   │
 │     Delete/Replace()         │
 └──────────┬───────────────────┘
            │
            ▼
 ┌──────────────────────────┐
 │ Layer 4: watchCache      │
 │ processEvent()           │
 │   1. 计算 key, labels    │
 │   2. 查找旧对象           │
 │   3. 追加到环形缓冲区      │
 │   4. 更新 store           │
 │   5. cond.Broadcast()     │
 └──────────┬───────────────┘
            │
            │ eventHandler(wcEvent)
            ▼
     chan: c.incoming (watchCache → dispatchEvents)
           │
           ▼
 ┌──────────────────────────┐
 │ Layer 5: dispatchEvents  │
 │   dispatchEvent():       │
 │   1. startDispatching    │
 │      构建 watchersBuffer │
 │   2. setCachingObjects   │
 │   3. nonblockingAdd →    │
 │      watcher.input       │
 │   4. finishDispatching   │
 │                          │
 │   bookmarkTimer(~1s):    │
 │   合成 bookmark 并 dispatch │
 └──────────┬───────────────┘
            │
            │ nonblockingAdd() / add()
            ▼
     chan: watcher.input (dispatchEvents → cacheWatcher.process)
           │
           ▼
 ┌──────────────────────────┐
 │ Layer 6: cacheWatcher    │
 │ processInterval():       │
 │   追赶历史事件 + bookmark │
 │ process():              │
 │   读取 input             │
 │   convertToWatchEvent(): │
 │    旧/新对象双过滤       │
 │    → Added/Modified/    │
 │      Deleted             │
 └──────────┬───────────────┘
            │
            │ sendWatchCacheEvent()
            ▼
     chan: watcher.result (cacheWatcher → WatchServer)
           │
           ▼
 ┌──────────────────────────┐
 │ Layer 7: WatchServer     │
 │ HandleHTTP()             │
 │   循环读 resultChan      │
 │   → watchEncoder.Encode()│
 │   → flusher.Flush()      │
 │   → HTTP chunked 响应流  │
 └──────────┬───────────────┘
            │
            ▼
       [Client]
       reflector/informer
       更新本地 cache
       触发 Registered EventHandler
```

**四个关键 channel 总结：**

| 编号 | channel | 生产者 | 消费者 | 数据内容 |
|------|---------|--------|--------|---------|
| ① | `watchChan.incomingEventChan` | startWatching goroutine | processEvents goroutine | 内部 `*event`（含 raw bytes） |
| ② | `watchChan.resultChan` | processEvents goroutine | Cacher 内部 Reflector | `watch.Event`（已解码） |
| ③ | `Cacher.incoming` | watchCache.processEvent | dispatchEvents goroutine | `*watchCacheEvent`（含标签/旧对象） |
| ④ | `cacheWatcher.input` | dispatchEvents goroutine | cacheWatcher.process goroutine | `*watchCacheEvent`（按 watcher 过滤） |
| ⑤ | `cacheWatcher.result` | cacheWatcher.process goroutine | **WatchServer.HandleHTTP** | `watch.Event`（最终发给客户端） |

