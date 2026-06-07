---
title: client-go 源码解读（二）List & Watch 与 DeltaFIFO
date: 2025-08-21 21:37:33
tags: 
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 背景

Kubernetes 中所有的API对象都是存储在Etcd中，且只能通过kube-apiserver访问。当访问量很大时，kube-apiserver会不堪重负。

基于上述考虑，kubernetes 中引入了一个informer机制，本质上是在内存中维护一个缓存，客户端的读写请求都可以通过缓存实现，而不必全部穿透到kube-apiserver。

<!--more-->

Kubernetes实现这一缓存的核心就是 list 和 watch 操作。本文基于 Kubernetes 源码（路径：`staging/src/k8s.io/client-go/tools/cache/`），深入剖析list & watch机制以及承载变更数据的 DeltaFIFO。

## 2. ListerWatcher

ListerWatcher 是 lister 和 watcher 的结合体，前者负责列举全量对象，后者负责监视对象的增量变化。

### 2.1 接口定义

Lister、Watcher、ListerWatcher 分别在 `listwatch.go:33`、`listwatch.go:68`、`listwatch.go:106` 中定义：

```go
// listwatch.go:33
type Lister interface {
	List(options metav1.ListOptions) (runtime.Object, error)
}

// listwatch.go:68
type Watcher interface {
	Watch(options metav1.ListOptions) (watch.Interface, error)
}

// listwatch.go:106
type ListerWatcher interface {
	Lister
	Watcher
}
```

新版本已推荐使用带 Context 的版本（`ListerWithContext:listwatch.go:42`，`WatcherWithContext:listwatch.go:80`），`ToListerWatcherWithContext(lw)` 函数（`listwatch.go:120`）负责新旧接口适配。

### 2.2 ListWatch 具体实现

`ListWatch` 结构体（`listwatch.go:193`）是 `ListerWatcher` 接口的具体实现：

```go
// listwatch.go:193
type ListWatch struct {
	ListFunc             ListFunc
	WatchFunc            WatchFunc
	ListWithContextFunc  ListWithContextFunc
	WatchFuncWithContext WatchFuncWithContext
	DisableChunking      bool
}
```

工厂函数 `NewFilteredListWatchFromClient`（`listwatch.go:229`）通过传入 RESTClient、资源名、namespace 和选项修饰函数，自动生成 list 和 watch 的闭包：

```go
// listwatch.go:230 - List 闭包
listFunc := func(options metav1.ListOptions) (runtime.Object, error) {
	optionsModifier(&options)
	return c.Get().
		Namespace(namespace).
		Resource(resource).
		VersionedParams(&options, metav1.ParameterCodec).
		Do(context.Background()).
		Get()
}

// listwatch.go:239 - Watch 闭包
watchFunc := func(options metav1.ListOptions) (watch.Interface, error) {
	options.Watch = true
	optionsModifier(&options)
	return c.Get().
		Namespace(namespace).
		Resource(resource).
		VersionedParams(&options, metav1.ParameterCodec).
		Watch(context.Background())
}
```

List 闭包发起的是 GET 请求，返回全量对象列表；Watch 闭包将 `options.Watch = true` 后发起长连接请求。两者都通过 `VersionedParams` 将 ListOptions 序列化为 HTTP 查询参数。

## 3. Store 接口

`Store` 接口（`store.go:41`）是数据存储的抽象：

```go
// store.go:41
type Store interface {
	Add(obj interface{}) error
	Update(obj interface{}) error
	Delete(obj interface{}) error
	List() []interface{}
	ListKeys() []string
	Get(obj interface{}) (item interface{}, exists bool, err error)
	GetByKey(key string) (item interface{}, exists bool, err error)
	Replace([]interface{}, string) error
	Resync() error
	Bookmark(rv string)
}
```

`ReflectorStore`（`reflector.go:71`）是 Reflector 使用的 Store 子集，只包含 Add/Update/Delete/Replace/Resync 五个方法。具体的实现 `cache` 结构体（`store.go:204`）包装了 `ThreadSafeStore`，而 `ThreadSafeStore` 的底层是 `threadSafeMap`（`thread_safe_store.go:256`），通过 `sync.RWMutex` 保证并发安全，键由 `KeyFunc`（默认 `MetaNamespaceKeyFunc`，生成 `<namespace>/<name>`）生成。

## 4. DeltaFIFO

`DeltaFIFO`（`delta_fifo.go:108`）是 producer-consumer 队列，**Reflector 是生产者**，**Controller 是消费者**。它的核心设计是：每个对象的累加器不是单一的最新对象，而是一个 `Deltas`（`delta_fifo.go:223`）—— 即 `Delta` 的切片，记录了该对象发生的所有变更。

### 4.1 数据结构

```go
// delta_fifo.go:108
type DeltaFIFO struct {
	lock       sync.RWMutex
	cond       sync.Cond
	items      map[string]Deltas   // key -> 该对象的所有增量
	queue      []string            // FIFO 顺序的 key 列表
	synced     chan struct{}       // 初始同步完成后关闭
	populated  bool                // 是否有过数据写入
	initialPopulationCount int     // 首次Replace写入的对象数
	keyFunc    KeyFunc
	knownObjects KeyListerGetter   // 用于检测删除（在Informer中就是indexer）
	emitDeltaTypeReplaced bool
	transformer TransformFunc
}
```

双结构设计：`items` 是 map 负责快速查找，`queue` 是 slice 维持 FIFO 顺序。两者通过 key 关联，一个 key 在 `items` 中当且仅当在 `queue` 中。

### 4.2 Delta 类型

`DeltaType`（`delta_fifo.go:179`）定义了 8 种变更类型：

```go
// delta_fifo.go:182
const (
	Added       DeltaType = "Added"        // 对象新增
	Updated     DeltaType = "Updated"       // 对象更新
	Deleted     DeltaType = "Deleted"       // 对象删除
	Replaced    DeltaType = "Replaced"      // relist 产生的替换（需EmitDeltaTypeReplaced开启）
	ReplacedAll DeltaType = "ReplacedAll"   // 原子替换（新FIFO）
	Sync        DeltaType = "Sync"          // 周期性 resync
	SyncAll     DeltaType = "SyncAll"       // 全量重新处理
	Bookmark    DeltaType = "Bookmark"      // ResourceVersion 推进通知
)
```

### 4.3 核心操作

**Add/Update/Delete**（`delta_fifo.go:386`、`delta_fifo.go:395`、`delta_fifo.go:408`）：
三个方法都会调用 `queueActionLocked()`（`delta_fifo.go:482`），最终进入 `queueActionInternalLocked()`（`delta_fifo.go:491`）。流程如下：

1. 通过 `KeyOf` 计算对象的 key
2. 如果设了 `transformer`，先对对象做变换
3. 将新 Delta 追加到 `f.items[id]` 对应的 Deltas 尾部
4. 调用 `dedupDeltas()` 合并相邻的重复删除事件
5. 如果是新 key，加入 `f.queue` 尾部
6. 通过 `f.cond.Broadcast()` 唤醒等待的消费者

```go
// delta_fifo.go:491
func (f *DeltaFIFO) queueActionInternalLocked(actionType, internalActionType DeltaType, obj interface{}) error {
	id, _ := f.KeyOf(obj)
	// apply transformer
	oldDeltas := f.items[id]
	newDeltas := append(oldDeltas, Delta{actionType, obj})
	newDeltas = dedupDeltas(newDeltas)
	if _, exists := f.items[id]; !exists {
		f.queue = append(f.queue, id)
	}
	f.items[id] = newDeltas
	f.cond.Broadcast()
	return nil
}
```

**Replace**（`delta_fifo.go:619`）：

当执行全量 relist 时调用，这是 DeltaFIFO 中最复杂的方法：

```go
// delta_fifo.go:619
func (f *DeltaFIFO) Replace(list []interface{}, _ string) error {
	// 1. 对每个新对象生成 Replaced/Sync 类型的 Delta
	for _, item := range list {
		f.queueActionInternalLocked(action, Replaced, item)
	}
	// 2. 检测 f.items 中但不在新列表中的 key → 生成 Deleted Delta
	for k, oldItem := range f.items {
		if keys.Has(k) { continue }
		f.queueActionLocked(Deleted, DeletedFinalStateUnknown{k, deletedObj})
	}
	// 3. 检测 knownObjects 中但不在新列表中的 key → 同样生成 Deleted Delta
	if f.knownObjects != nil {
		for _, k := range knownKeys {
			if keys.Has(k) || len(f.items[k]) > 0 { continue }
			f.queueActionLocked(Deleted, DeletedFinalStateUnknown{k, deletedObj})
		}
	}
	// 4. 设置 populated，记录 initialPopulationCount
}
```

Replace 的删除检测机制非常重要：当 Reflector 断连后重新 List，期间发生的删除事件可能丢失。通过比对全量 List 结果与本地缓存的差异，Replace 能"补偿"丢失的删除事件，将缺失的对象包装为 `DeletedFinalStateUnknown` 作为 tombstone。

**Pop**（`delta_fifo.go:562`）：

消费者调用，阻塞直到有数据可用：

```go
// delta_fifo.go:562
func (f *DeltaFIFO) Pop(process PopProcessFunc) (interface{}, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for {
		for len(f.queue) == 0 {
			if f.closed { return nil, ErrFIFOClosed }
			f.cond.Wait()
		}
		id := f.queue[0]; f.queue = f.queue[1:]
		// 递减 initialPopulationCount，检查 synced
		item := f.items[id]; delete(f.items, id)
		err := process(item, isInInitialList)
		return item, err
	}
}
```

`Pop` 在加锁状态下调用 process 函数，确保队列操作与消费者处理的原子性。当 `populated && initialPopulationCount == 0` 时，`f.synced` 通道关闭，标识初始同步完成。

## 5. Reflector

`Reflector`（`reflector.go:106`）是整个 list & watch 机制的核心引擎。它通过 `ListerWatcher` 与 API Server 交互，将数据同步到 `ReflectorStore`（通常是 DeltaFIFO）。

### 5.1 结构体

```go
// reflector.go:106
type Reflector struct {
	store               ReflectorStore            // 目标存储（通常是 DeltaFIFO）
	listerWatcher       ListerWatcherWithContext  // 执行 List/Watch
	resyncPeriod        time.Duration
	lastSyncResourceVersion string                // 最近一次同步的 ResourceVersion
	isLastSyncResourceVersionUnavailable bool      // 前一次因过期不可用
	watchErrorHandler   WatchErrorHandlerWithContext
	WatchListPageSize   int64
	ShouldResync        func() bool
	useWatchList        bool                      // 是否使用 WatchList 流模式
	minWatchTimeout     time.Duration              // 默认 5min
	maxWatchTimeout     time.Duration              // 默认 10min
}
```

### 5.2 主循环：ListAndWatchWithContext

`ListAndWatchWithContext()`（`reflector.go:470`）是 Reflector 的入口：

```go
// reflector.go:470
func (r *Reflector) ListAndWatchWithContext(ctx context.Context) error {
	fallbackToList := !r.useWatchList

	if r.useWatchList {
		w, err = r.watchList(ctx)       // 尝试 WatchList 流模式
		if err != nil {
			fallbackToList = true       // 失败则回退到传统模式
			w = nil
		}
	}

	if fallbackToList {
		err = r.list(ctx)               // 传统 List 全量
		if err != nil { return err }
	}

	return r.watchWithResync(ctx, w)    // 进入 Watch 循环
}
```

两种模式：
- **传统模式**：先 `list()` 全量拉取，再 `watchWithResync()` 增量监听
- **WatchList 流模式**：通过一次流式连接获取一致快照 + 持续增量

### 5.3 list() — 全量拉取

`list()`（`reflector.go:674`）通过分页（pager）从 API Server 拉取全量对象。关键流程：

1. 通过 `relistResourceVersion()`（`reflector.go:1116`）确定 ResourceVersion：
   - 首次调用返回 `"0"`（从 watch cache 读取）
   - 上次过期则返回 `""`（最新一致读，直连 etcd）
   - 否则返回上一次的 `lastSyncResourceVersion`
2. 使用 pager 分页拉取，支持 `WatchListPageSize` 控制分页大小
3. 若返回 `Expired` 或 `TooLargeResourceVersion` 错误，用 `ResourceVersion=""` 重试
4. 通过 `meta.ExtractListWithAlloc()` 提取对象列表
5. 调用 `r.syncWith(items, resourceVersion)` 即 `r.store.Replace(found, resourceVersion)` 写入 DeltaFIFO
6. 更新 `lastSyncResourceVersion`

```go
// reflector.go:674
func (r *Reflector) list(ctx context.Context) error {
	options := metav1.ListOptions{ResourceVersion: r.relistResourceVersion()}
	pager := pager.New(pager.SimplePageFunc(func(opts metav1.ListOptions) (runtime.Object, error) {
		return r.listerWatcher.ListWithContext(ctx, opts)
	}))
	// 分页策略选择
	switch {
	case r.WatchListPageSize != 0:
		pager.PageSize = r.WatchListPageSize
	case r.paginatedResult:
		// 保持默认分页
	case options.ResourceVersion != "" && options.ResourceVersion != "0":
		pager.PageSize = 0 // 关闭分页，从 watch cache 读取
	}
	list, paginatedResult, err = pager.ListWithAlloc(context.Background(), options)
	// ...
	if err := r.syncWith(items, resourceVersion); err != nil { return err }
	r.setLastSyncResourceVersion(resourceVersion)
	return nil
}
```

分页逻辑的关键在于：当 `ResourceVersion != "" && != "0"` 时关闭分页，强制从 watch cache 读取，避免对 etcd 的 thundering herd 问题。

### 5.4 watch() — 增量监听

`watch()`（`reflector.go:561`）是持续监听的循环：

```go
// reflector.go:561
func (r *Reflector) watch(ctx context.Context, w watch.Interface, resyncerrc chan error) error {
	for {
		if w == nil {
			timeoutSeconds := r.minWatchTimeout + random(r.maxWatchTimeout - r.minWatchTimeout)
			options := metav1.ListOptions{
				ResourceVersion:    r.LastSyncResourceVersion(),
				TimeoutSeconds:     &timeoutSeconds,
				AllowWatchBookmarks: true,
			}
			w, err = r.listerWatcher.WatchWithContext(ctx, options)
		}
		err = handleWatch(ctx, start, w, r.store, ...)
		w = nil
		// Expired → 重试; 429 → backoff; InternalError → retry
	}
}
```

关键设计：
- `AllowWatchBookmarks: true`：开启 bookmark 机制，服务器定期推送 ResourceVersion，避免因无事件导致长连接超时
- `TimeoutSeconds` 随机在 `[5min, 10min]` 范围内，避免所有 watcher 同时超时重建
- 410 Expired 错误 → 回到外层重新 `list()`；429 TooManyRequests → backoff 后继续；InternalError → 有限次重试

backoff 参数（`reflector.go:62`）：
- 初始间隔 800ms，最大间隔 30s
- 2 分钟无错后重置，乘数 2.0，抖动 1.0

### 5.5 handleAnyWatch() — 事件处理

`handleAnyWatch()`（`reflector.go:972`）处理 watch 通道的事件并写入 store：

```go
// reflector.go:1037
switch event.Type {
case watch.Added:
	store.Add(event.Object)
case watch.Modified:
	store.Update(event.Object)
case watch.Deleted:
	store.Delete(event.Object)
case watch.Bookmark:
	if meta.GetAnnotations()["k8s.io/initial-events-end"] == "true" {
		watchListBookmarkReceived = true   // WatchList 结束标记
	}
	if bookmarkStore, ok := store.(ReflectorBookmarkStore); ok {
		bookmarkStore.Bookmark(resourceVersion)
	}
}
```

### 5.6 WatchList 流模式（KEP-3157）

WatchList（`reflector.go:804`）是 Kubernetes 1.31+ 引入的新特性，通过一次流式连接完成全量同步 + 增量监听：

```go
// reflector.go:854
options := metav1.ListOptions{
	ResourceVersion:      lastKnownRV,
	AllowWatchBookmarks:  true,
	SendInitialEvents:    ptr.To(true),
	ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
	TimeoutSeconds:       &timeoutSeconds,
}
w, err = r.listerWatcher.WatchWithContext(ctx, options)
```

流程：
1. 建立 watch 流，设置 `SendInitialEvents: true` 和 `ResourceVersionMatchNotOlderThan`
2. 服务器依次发送所有对象的 `Added` 事件（合成事件）
3. 服务器发送带 `k8s.io/initial-events-end` 注解的 `Bookmark` 事件，标识初始快照完成
4. Reflector 调用 `r.store.Replace(temporaryStore.List(), resourceVersion)` 替换存储
5. **复用同一 watch 流** 继续接收后续增量（`stopWatcher = false` 避免关闭连接）

与传统模式对比：

| 特性 | 传统模式 (list+watch) | WatchList 流模式 |
|------|----------------------|-----------------|
| 全量同步 | 分页 GET 请求，可能请求 etcd | 流式 Watch，常驻 watch cache |
| 一致性 | 分页请求非原子，可能看到中间态 | 单流原子快照，一致性保证 |
| 服务器开销 | 大量 GET + 分页对 etcd 的压力 | 单一长连接，资源消耗小 |
| 启用条件 | 默认行为 | `WatchListClient` 特性门控 + API Server 支持 |

### 5.7 运行循环

`RunWithContext()`（`reflector.go:416`）是外层循环，持续调用 `ListAndWatchWithContext`：

```go
// reflector.go:416
func (r *Reflector) RunWithContext(ctx context.Context) {
	r.delayHandler.Until(ctx, true, true, func(ctx context.Context) (bool, error) {
		if err := r.ListAndWatchWithContext(ctx); err != nil {
			r.watchErrorHandler(ctx, r, err)
		}
		return false, nil  // 永不退出，持续重试
	})
}
```

## 6. ResourceVersion 的作用

ResourceVersion 是 Kubernetes 乐观并发控制的核心机制，在 list & watch 中扮演关键角色：

- **List 时**：`relistResourceVersion()`（`reflector.go:1116`）决定拉取版本：
  - `"0"`：从 watch cache 读取（首次）
  - `""`：一致读，直查 etcd（前一次过期后）
  - 具体值：从指定版本读取（包含该版本及之后的数据）
- **Watch 时**：从 `LastSyncResourceVersion` 开始监听，确保不丢事件
- **Expired（410）**：ResourceVersion 太旧，需要重新 list
- **Bookmark 事件**：服务器推送最新 ResourceVersion，使 watcher 安全重连

## 7. 总结

本文剖析了 client-go 数据采集管道的两个核心组件：

1. **ListerWatcher** — 数据源抽象，封装与 API Server 的 List/Watch 交互
2. **Reflector** — 核心引擎，通过 list+watch 或 watchList 将数据同步到 DeltaFIFO
3. **DeltaFIFO** — 变更日志队列，以 Deltas 形式记录每个对象的所有变更，并通过 `knownObjects` 实现删除检测补偿

至此，数据从 API Server 流入了 DeltaFIFO。下一篇文章将介绍 Informer 和 Indexer，看消费者如何从 DeltaFIFO 中取出数据并构建本地缓存。
