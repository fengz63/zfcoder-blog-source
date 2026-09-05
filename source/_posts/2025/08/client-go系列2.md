---
title: client-go 源码解读（二）List & Watch 与 DeltaFIFO
date: 2025-08-21 21:37:33
tags: 
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 背景

Kubernetes 中所有的API对象都是存储在Etcd中，且只能通过kube-apiserver访问，当访问量很大时，kube-apiserver会不堪重负。

基于上述考虑，kubernetes 中引入了一个informer机制。informer 会在客户端内维护一份资源缓存，控制器可以通过 lister/indexer 读取缓存，避免频繁访问kube-apiserver。此外，创建、更新和删除等写操作仍需要通过 kube-apiserver 执行。

<!--more-->

Kubernetes实现这一缓存的核心就是 list 和 watch 操作。本文基于 Kubernetes 源码（`staging/src/k8s.io/client-go/tools/cache/`），深入剖析list & watch机制以及承载变更数据的 DeltaFIFO。大致过程可以概括为：Reflector 将 API Server 返回的对象和事件转换为 Delta，并写入 DeltaFIFO；Informer Controller 再从 DeltaFIFO 中取出 Delta，更新 Indexer，并通知事件处理器。

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

`ReflectorStore`（`reflector.go:71`）是 Reflector 使用的 Store 子集，只包含 Add/Update/Delete/Replace/Resync 五个方法。Reflector 并不直接维护最终的本地缓存，而是把事件写入这个接口；在 Informer 中，ReflectorStore 通常是 DeltaFIFO。

最终用于查询的本地缓存通常是 Indexer。具体实现中，`cache` 结构体（`store.go:204`）使用 `ThreadSafeStore` 作为底层存储，而 `ThreadSafeStore` 的底层是 `threadSafeMap`（`thread_safe_store.go:256`），通过 `sync.RWMutex` 保证并发安全，键由 `KeyFunc`（默认 `MetaNamespaceKeyFunc`，对命名空间对象生成 `<namespace>/<name>`）生成。

## 4. DeltaFIFO

`DeltaFIFO`（`delta_fifo.go:108`）为每个对象维护一个按 key 索引的待处理变更列表，而不是只保存对象的最新状态。这个列表类型就是 `Deltas`，本质上是多个 `Delta` 的切片。同一个 key 在 FIFO 队列中只出现一次，但该 key 对应的 `Deltas` 可以累积多条变更。

FIFO 只对部分重复事件进行去重，目前主要是合并相邻的重复删除事件，并不会把多个更新事件统一压缩为最新对象。

### 4.1 数据结构

```go
// delta_fifo.go:108
type DeltaFIFO struct {
	lock       sync.RWMutex
	cond       sync.Cond
	items      map[string]Deltas   // key -> 该对象的待处理变更
	queue      []string            // FIFO 顺序的 key 列表
	synced     chan struct{}       // 初始同步完成后关闭
	populated  bool                // 是否有过数据写入
	initialPopulationCount int     // 首次Replace写入的对象和删除事件数
	keyFunc    KeyFunc
	knownObjects KeyListerGetter   // 用于检测删除（在Informer中就是indexer）
	emitDeltaTypeReplaced bool
	transformer TransformFunc
}
```

双结构设计：`items` 是 map，负责按 key 保存待处理的 `Deltas`；`queue` 是 slice，负责维持 key 的 FIFO 顺序。两者通过 key 关联：一个 key 在 `items` 中当且仅当它在 `queue` 中。需要注意，`queue` 去重的是 key，不是 Delta；同一个 key 的多次变更会追加到对应的 `Deltas` 中。

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

Replace 用于处理 Reflector 的全量 List 结果。它不会直接修改最终缓存，而是在 DeltaFIFO 内部执行一次带删除检测的批量入队，流程大致如下：
- 为 List 返回的每个对象生成 Sync 或 Replaced Delta；
- 检查 FIFO 中已有但本次 List 未返回的对象，为其生成删除 Delta；
- 检查 knownObjects 中已经处理过、但本次 List 未返回的对象，补生成删除 tombstone；
- 第一次 Replace 时记录初始对象数量，用于判断初始同步是否完成。

因此，Replace 既负责把全量 List 结果转换成待处理事件，也负责补偿 Watch 断连期间丢失的删除事件。

```go
// delta_fifo.go:619
// 以下代码省略了错误处理和部分局部变量，仅展示核心流程
func (f *DeltaFIFO) Replace(list []interface{}, _ string) error {
	// 1. 为本次 list 返回的对象生成 Delta，默认情况下，这些 Delta 的 action 是 Sync；如果启用
	// 了 EmitDeltaTypeReplaced，则使用 Replaced。
	// 这里的 Sync 表示这次全量 list 证明对象当前仍然存在，需要把它重新交给消费者处理。
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

例如，第一次 List 返回对象 A、B，Controller 处理完成后，A、B 已经存在于 Indexer 中。之后 Reflector 与 API Server 断连，B 被删除，但删除事件没有被 Watch 收到。重新 List 只返回 A 时，B 已经不在 `f.items` 中，而是在 `knownObjects`（Informer 中通常就是 Indexer）中。Replace 会发现 B 不在新列表中，于是生成：

```go
DeletedFinalStateUnknown{
    Key: "B",
    Obj: lastKnownObjectOfB,
}
```

这个对象称为 tombstone，随后由 Controller 消费并从 Indexer 中删除。Replace 的删除检测机制因此可以补偿 Reflector 断连期间丢失的删除事件。

第一次执行 Replace 时，`initialPopulationCount` 会记录初始 List 对象以及初始删除事件的数量。Controller 每次 Pop 一个对象后，该计数递减；计数归零后，DeltaFIFO 关闭 `synced` 通道，表示初始数据已经处理完成。

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

`Pop` 在持有 FIFO 锁的状态下调用 `process` 函数，因此 `process` 可以与队列状态保持同步；但这也意味着 `process` 不应该执行耗时的 I/O，否则会阻塞生产者的 Add/Update/Delete。处理失败时，调用方通常需要通过 `AddIfNotPresent` 将对象重新放回队列。当 `populated && initialPopulationCount == 0` 时，`f.synced` 通道关闭，标识初始同步完成。

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

`list()`（`reflector.go:674`）通过分页（pager）从 kube-apiserver 拉取全量对象。关键流程：

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

分页逻辑的关键在于：当 resourceVersion 非空且不为"0"时，reflector 通常会关闭分页，以便在 apiserver 启用 watch cache 时优先从 watch cache 获取数据，从而减少对 etcd 的集中读取压力。

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
- `AllowWatchBookmarks: true`：bookmark 是一种不携带资源对象的 resourceVersion 进度通知。reflector 收到 Bookmark 后可以推进已观察到的 resourceVersion，从而在 watch 重建时降低重复处理或从过旧版本恢复的风险。
- `TimeoutSeconds` 随机在 `[5min, 10min]` 范围内，避免所有 watcher 同时超时重建
- 410 Expired 错误 → 回到外层重新 `list()`；429 TooManyRequests → backoff 后继续；InternalError → 有限次重试

backoff 参数（`reflector.go:62`）：
- 初始间隔 800ms，最大间隔 30s
- 2 分钟无错后重置，乘数 2.0，抖动 1.0

### 5.5 WatchList 流模式（KEP-3157）

WatchList（`reflector.go:804`）通过一次流式 Watch 完成初始状态同步和后续增量监听。初始阶段，apierver 基于 watch cache 获取符合条件对象在目标 ResourceVersion 上的最新状态快照，并将快照中的每个对象转换为合成的 `Added` 事件发送给客户端。

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

初始对象发送完成后，服务端发送带有 `k8s.io/initial-events-end: "true"` 注解的 `Bookmark`，表示初始状态已经同步完成；之后同一条 Watch 流继续发送后续的 `Added`、`Modified`、`Deleted` 和 `Bookmark` 事件。客户端最终看到的事件顺序可以概括为：
```text
watch cache 当前对象快照
    → 合成 Added 事件
    → initial-events-end Bookmark
    → 实时增量事件
```

与传统模式对比：

| 特性 | 传统模式 (list+watch) | WatchList 流模式 |
|------|----------------------|-----------------|
| 全量同步 | 分页 GET 请求，可能请求 etcd | 流式 Watch，常驻 watch cache |
| 一致性 | 分页会增加请求次数和处理链路复杂度 | 单流原子快照，一致性保证 |
| 服务器开销 | 大量 GET + 分页对 etcd 的压力 | 单一长连接，资源消耗小 |
| 启用条件 | 默认行为 | `WatchListClient` 特性门控 + API Server 支持 |

### 5.6 运行循环

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
3. **DeltaFIFO** — 待处理变更队列，以 Deltas 形式暂存对象在被消费前积累的变更，并通过 `knownObjects` 实现删除检测补偿

至此，数据从 API Server 流入了 DeltaFIFO。下一篇文章将介绍 Informer 和 Indexer，看消费者如何从 DeltaFIFO 中取出数据并构建本地缓存。
