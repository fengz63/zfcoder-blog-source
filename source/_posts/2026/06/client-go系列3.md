---
title: client-go 源码解读（三）Informer 与 Indexer
date: 2026-06-07 15:37:33
tags: 
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 概述

前文介绍了 Reflector 如何通过 List & Watch 从 API Server 拉取数据并将变更写入 DeltaFIFO。本文介绍 Informer 和 Indexer——Informer 从 DeltaFIFO 取出 Deltas，更新本地缓存（Indexer），并将事件分发给用户注册的 EventHandler。

<!--more-->

Informer 是整个 client-go 框架中最上层的封装，它将 Reflector、DeltaFIFO、Indexer 和事件分发整合为一个统一的组件。用户通过 `NewSharedInformerFactory` 或 `NewInformer` 创建 Informer 后，只需注册事件处理函数并调用 `Run()`，就能获得一个完整的数据同步 + 事件通知流水线。

## 2. SharedInformer 接口

`SharedInformer` 接口（`shared_informer.go:144`）定义了给用户使用的主要 API：

```go
// shared_informer.go:144
type SharedInformer interface {
	AddEventHandler(handler ResourceEventHandler) (ResourceEventHandlerRegistration, error)
	AddEventHandlerWithResyncPeriod(handler ResourceEventHandler, resyncPeriod time.Duration) (...)
	RemoveEventHandler(handle ResourceEventHandlerRegistration) error
	GetStore() Store
	Run(stopCh <-chan struct{})
	RunWithContext(ctx context.Context)
	HasSynced() bool
	LastSyncResourceVersion() string
	SetWatchErrorHandler(handler WatchErrorHandler) error
	SetTransform(handler TransformFunc) error
	IsStopped() bool
}
```

其中 `SharedIndexInformer` 在此基础上增加了 `GetIndexer()` 和 `AddIndexers()` 等索引相关方法（`shared_informer.go:263`）。

## 3. sharedIndexInformer 结构体

`sharedIndexInformer`（`shared_informer.go:588`）是 SharedInformer 的具体实现，包含三大组件：

```go
// shared_informer.go:588
type sharedIndexInformer struct {
	indexer    Indexer              // 本地缓存（同时做 DeltaFIFO 的 knownObjects）
	controller Controller           // 底层控制器（Reflector + DeltaFIFO）
	processor  *sharedProcessor     // 事件分发器
	listerWatcher ListerWatcher
	objectType runtime.Object
	resyncCheckPeriod time.Duration
	defaultEventHandlerResyncPeriod time.Duration
	blockDeltas sync.Mutex          // 保护事件分发的锁（用于处理延迟注册的 handler）
	started, stopped bool
}
```

三大组件的协作关系：

```
Reflector ──写入──→ DeltaFIFO ──Pop──→ processDeltas ──→ Indexer (本地缓存)
                                                      ──→ sharedProcessor (事件分发)
                                                               │
                                                               ↓
                                                        用户 Handlers
```

### 3.1 RunWithContext

`RunWithContext()`（`shared_informer.go:719`）完成初始化并启动各组件：

```go
// shared_informer.go:719
func (s *sharedIndexInformer) RunWithContext(ctx context.Context) {
	// 1. 创建 DeltaFIFO（indexer 作为 knownObjects）
	fifo := newQueueFIFO(s.objectType, s.indexer, s.transform, ...)

	// 2. 创建 Config，Process = handleDeltas
	cfg := &Config{
		Queue:         fifo,
		ListerWatcher: s.listerWatcher,
		Process: func(obj interface{}, isInInitialList bool) error {
			return s.handleDeltas(logger, obj, isInInitialList)
		},
		ShouldResync: s.processor.shouldResync,
	}
	s.controller = New(cfg)

	// 3. 启动 processor（事件分发器）
	wg.StartWithContext(processorStopCtx, s.processor.run)

	// 4. 启动 controller（内部启动 Reflector + processLoop）
	s.controller.RunWithContext(ctx)
}
```

启动顺序很重要：先启动 processor 准备好接收事件，再启动 controller（它内部会启动 Reflector 开始 list&watch）。

从这一刻开始，后续处理会按照如下所示顺序：

```
DeltaFIFO.Pop()
    ↓
controller.processLoop()
    ↓
Config.Process(handleDeltas)
    ↓
processDeltas()
    ├── 更新 Indexer
    └── 调用 sharedIndexInformer.OnAdd/OnUpdate/OnDelete
              ↓
       sharedProcessor.distribute()
              ↓
       processorListener.pop/run()
              ↓
       用户注册的 ResourceEventHandler
```

这里有两个容易混淆的点：
- `Pop()` 返回的不是单个对象，而是同一个 key 在排队期间积累的 `Deltas`；
- Informer 不是在 Indexer 更新之前通知用户，而是先让本地缓存达到最新状态，再触发事件回调。

### 3.2 handleDeltas

`handleDeltas()`（`shared_informer.go:944`）是 Config 中注册的 Process 函数，每次 DeltaFIFO.Pop 后调用：

```go
// shared_informer.go:944
func (s *sharedIndexInformer) handleDeltas(logger klog.Logger, obj interface{}, isInInitialList bool) error {
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	if deltas, ok := obj.(Deltas); ok {
		return processDeltas(logger, s, s.indexer, deltas, isInInitialList, s.keyFunc)
	}
	return errors.New("object given as Process argument is not Deltas")
}
```

`controller.processLoop()` 会不断调用队列的 `Pop()`。`DeltaFIFO.Pop()` 取出队头 key 对应的全部 `Deltas`，并在调用 `Process` 时传入 `isInInitialList`。Pop 的过程持有 FIFO 的锁，因此 `Process` 不应该执行耗时 I/O；否则 Reflector 写入新事件也会被阻塞。`Pop` 会先从队列中移除这组 Deltas，再调用处理函数；如果处理失败，当前 `processLoop` 只记录/忽略这次错误，并不会自动把对象重新放回队列，所以 Informer 的 `handleDeltas` 通常应把错误视为不可恢复的数据处理问题。

`Deltas` 按从旧到新的顺序保存事件，但它是“同一个对象 key 的变化集合”，不是完整的全局事件日志。对象在排队期间可能经历多次变化，DeltaFIFO 会按 key 合并相邻的重复变化，所以用户的 `OnUpdate(old, new)` 不保证能看到对象的每一次中间状态。

注意 sharedIndexInformer 作为 `ResourceEventHandler` 传入 processDeltas，在 processDeltas 内部会调用 `s.OnAdd/OnUpdate/OnDelete`，将事件交给 sharedProcessor 分发。

`processDeltas()` 的处理顺序是理解整条链路的关键：

```go
for _, d := range deltas { // oldest -> newest
    obj := d.Object
    switch d.Type {
    case Sync, Replaced, Added, Updated:
        if old, exists, _ := clientState.Get(obj); exists {
            clientState.Update(obj)
            handler.OnUpdate(old, obj)
        } else {
            clientState.Add(obj)
            handler.OnAdd(obj, isInInitialList)
        }
    case Deleted:
        clientState.Delete(obj)
        handler.OnDelete(obj)
    }
}
```

因此一次新增事件的实际顺序是：`Get` 判断对象是否已存在 → `Indexer.Add` → `OnAdd`；一次更新是：取出旧对象 → `Indexer.Update` → `OnUpdate(old, new)`；一次删除是：`Indexer.Delete` → `OnDelete`。

删除事件需要特别注意：如果 Watch 断开期间错过了删除，后续 relist 通过对比 Indexer 发现对象消失时，可能只能拿到最后已知对象，于是会把它包装成 `DeletedFinalStateUnknown`。因此用户的 `OnDelete` 不能只假设参数永远是资源对象，还应使用 `DeletionHandlingMetaNamespaceKeyFunc` 等方式兼容这种 tombstone。

### 3.3 事件分发

sharedIndexInformer 实现了 `ResourceEventHandler` 接口（`shared_informer.go:961-995`）：

```go
// shared_informer.go:961
func (s *sharedIndexInformer) OnAdd(obj interface{}, isInInitialList bool) {
	s.cacheMutationDetector.AddObject(obj)
	s.processor.distribute(addNotification{newObj: obj, isInInitialList: isInInitialList}, false)
}

// shared_informer.go:969
func (s *sharedIndexInformer) OnUpdate(old, new interface{}) {
	isSync := (resourceVersion unchanged)
	s.processor.distribute(updateNotification{oldObj: old, newObj: new}, isSync)
}

// shared_informer.go:991
func (s *sharedIndexInformer) OnDelete(old interface{}) {
	s.processor.distribute(deleteNotification{oldObj: old}, false)
}
```

`OnUpdate` 中有一个重要逻辑：如果新旧对象的 ResourceVersion 相同，说明这是 resync 产生的同步事件（而非真正的更新），这种事件仅分发给请求了 resync 的 listener。

## 4. sharedProcessor 与事件分发

`sharedProcessor`（`shared_informer.go:1023`）负责将事件分发给所有注册的 listener：

```go
// shared_informer.go:1023
type sharedProcessor struct {
	listenersStarted bool
	listenersLock    sync.RWMutex
	listeners        map[*processorListener]bool  // value = 是否在 syncing 状态
	clock            clock.Clock
}
```

`distribute()` 方法（`shared_informer.go:1094`）将通知发送给所有 listener：

```go
// shared_informer.go:1094
func (p *sharedProcessor) distribute(obj interface{}, sync bool) {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()
	for listener, isSyncing := range p.listeners {
		// sync 事件只发送给处于 syncing 状态的 listener
		if sync && !isSyncing { continue }
		listener.add(obj)
	}
}
```

### 4.1 processorListener

每个 `processorListener` 对应一个用户注册的 `ResourceEventHandler`。内部有精巧的背压处理机制：

```go
// shared_informer.go:1228
type processorListener struct {
	addCh    chan interface{}            // 无缓冲 channel，接收通知
	nextCh   chan interface{}            // 无缓冲 channel，发送给 handler
	pendingNotifications *buffer.RingGrowing  // 无界 ring buffer，背压缓冲
	handler  ResourceEventHandler
}
```

对于正常启动的 `processorListener`，`sharedProcessor.run()` 会启动三个相关 goroutine（`shared_informer.go:1134-1136`），它们分别是：

1. `watchSynced()`：等待上游 Informer 完成初始同步，更新 `syncTracker` 状态；
2. `pop()`：从 `addCh` 取出待发送事件，直接发送到 `nextCh`，或者暂存到 `pendingNotifications`；
3. `run()`：从 `nextCh` 读取事件，真正调用用户注册的 `OnAdd`、`OnUpdate` 和 `OnDelete`。

`pop()` 函数（`shared_informer.go:1296`）：从 `addCh` 读取通知，优先直接发送到 `nextCh`；如果 `nextCh` 阻塞，则写入 `pendingNotifications` ring buffer 暂存，然后从 ring buffer 读取并发送到 `nextCh`。

```go
// shared_informer.go:1296
func (p *processorListener) pop() {
	for {
		select {
		case notificationToAdd := <-p.addCh:
			// 尝试直接把事件交给正在接收的 run()
			select {
			case p.nextCh <- notificationToAdd:
				continue
			default:
			}
			// 当前没有接收者，写入 ring buffer
			p.pendingNotifications.WriteOne(notificationToAdd)
			for {
				select {
				case p.nextCh <- notification:
					notification, _ = p.pendingNotifications.ReadOne()
				case notificationToAdd := <-p.addCh:
					p.pendingNotifications.WriteOne(notificationToAdd)
				}
			}
		}
	}
}
```

其中 `notificationToAdd` 就是待发送的事件，例如 `addNotification`、`updateNotification` 或 `deleteNotification`。

1. `sharedProcessor.distribute()` 调用 `listener.add()`，把待发送的事件写入 `addCh`。
2. `pop()` 从 `addCh` 取出事件。如果 `run()` 当前正阻塞在接收 `nextCh`，发送操作会立即和这个接收操作配对完成。
3. `run()` 从 `nextCh` 取到事件后，调用用户的 `handler.OnAdd/OnUpdate/OnDelete`。

`addCh` 和 `nextCh` 都是无缓冲 channel。无缓冲 channel 没有“存放一个事件”的空间，发送和接收必须同时发生：`run()` 正在执行 `next := <-p.nextCh` 时，`pop()` 才能通过 `p.nextCh <- notification` 把当前事件交给它；如果 `run()` 正在执行用户 handler，暂时没有执行接收操作，`pop()` 就不能立即发送，只能把事件写入 `pendingNotifications`，等待 `run()` 下一次接收。因此，正常情况下通知路径是：

```text
sharedProcessor.distribute
        ↓ listener.add
待发送事件 → addCh
        ↓
processorListener.pop
        ├── nextCh（run 正在接收，直接交给 run）
        └── pendingNotifications（run 正在处理事件，暂存）
                              ↓
                            nextCh
                              ↓
                    processorListener.run
                              ↓
                    用户 ResourceEventHandler
```

`pendingNotifications` 使用 `RingGrowing`，容量不足时会继续扩容，因此它可以在短时间内存放大量事件。

`run()` 函数（`shared_informer.go:1330`）：从 `nextCh` 读取通知，调用 handler 的对应方法：

```go
// shared_informer.go:1330
func (p *processorListener) run() {
	for next := range p.nextCh {
		switch notification := next.(type) {
		case addNotification:
			p.handler.OnAdd(notification.newObj, notification.isInInitialList)
		case updateNotification:
			p.handler.OnUpdate(notification.oldObj, notification.newObj)
		case deleteNotification:
			p.handler.OnDelete(notification.oldObj)
		}
	}
}
```

**上面提到的 `pendingNotifications` 是一个无限制的 ring buffer，那如果事件过多会发生什么呢？**

如果用户的 `OnAdd`、`OnUpdate` 或 `OnDelete` 执行时间较长，`run()` 就会长时间占用，无法继续从 `nextCh` 读取通知。此时 `pop()` 会把后续通知不断写入 `pendingNotifications`，结果主要有以下几种：

- 事件处理延迟变大：用户 handler 看到的是排队后的旧通知，而不是对象发生变化后的即时通知。
- 内存持续增长：`pendingNotifications` 是可增长 ring buffer。如果生产事件的速度长期高于 handler 的处理速度，积压会不断扩大，增加内存压力，严重时甚至会 OOM。
- 多个 handler 之间可能互相影响：`sharedProcessor.distribute()` 会依次调用每个 listener 的 `add()`。某个 listener 的入口发送阻塞时，当前分发过程也会被拖慢，后面的 listener 暂时收不到通知。
- 反压最终可能传回 DeltaFIFO：当 `distribute()` 无法继续完成时，`processDeltas()` 不能返回；而 `DeltaFIFO.Pop()` 在执行 Process 期间持有 FIFO 锁，Reflector 的新事件就无法及时写入，后续 Indexer 更新和事件处理都会延迟。

所以 EventHandler 通常不应该在回调中执行耗时操作，例如远程 API 调用、复杂计算或长时间等待。更常见的做法是只提取对象 key，把 key 放入 WorkQueue，然后立即返回；真正的业务处理交给后续 worker 执行。


## 5. Indexer

`Indexer` 就是 Informer 的本地缓存，配合前面章节的知识：`processDeltas()` 在处理每个 Delta 时同步更新它，用户通过 `GetIndexer()` 拿到它做只读查询。

Store 只解决一个问题：**对象 ↔ key** 的存取（key 由 `KeyFunc` 生成，默认 `MetaNamespaceKeyFunc` 产出 `<namespace>/<name>`；Informer 实际用 `DeletionHandlingMetaNamespaceKeyFunc`，兼容删除 tombstone 场景，见 `controller.go:394`）。

Indexer 在 Store 之上再解决一个问题：**按对象的某个属性快速找出符合条件的对象**（比如"列出 default 这个 namespace 下的所有 Pod"）。这个能力就是索引。

### 5.1 Indexer 接口

`Indexer` 接口（`index.go:35`）= Store 全部方法 + 5 个索引相关方法：

```go
// index.go:35
type Indexer interface {
	Store
	Index(indexName string, obj interface{}) ([]interface{}, error)      // 按对象找"同类"
	IndexKeys(indexName, indexedValue string) ([]string, error)          // 按索引值拿 key 列表
	ListIndexFuncValues(indexName string) []string                       // 列举索引的全部取值
	ByIndex(indexName, indexedValue string) ([]interface{}, error)       // 按索引值拿对象列表
	GetIndexers() Indexers
	AddIndexers(newIndexers Indexers) error                              // 运行期追加索引
}
```

### 5.2 三个核心类型：Indexers / Indices / index

理解索引的关键在 `index.go:94` 里定义的三个类型，它们很容易混：

```go
// index.go:94
type index map[string]sets.Set[string]        // ① 单个索引的倒排表：索引值 -> 一组存储 key
type Indexers map[string]IndexFunc            // ② 索引规则集合：索引名 -> IndexFunc
type Indices  map[string]index                // ③ 索引结果集合：索引名 -> index
```

- `index` 是最底层的数据结构，一张倒排表："某个属性值 → 属于它的全部对象 key"；
- `Indexers` 是**规则**：给每个索引起名字、指定怎么算。由你在创建 Informer 时提供，是静态的；
- `Indices` 是**结果**：按 `Indexers` 规则算出来的倒排表集合，由 storeIndex 动态维护，随对象增删改实时更新。

规则由 `IndexFunc` 定义（`index.go:58`），输入对象、输出一组索引值（`[]string`，因为一个对象可以同时命中多个值，比如按 label 索引时一个 Pod 可能有多个标签）：

```go
// index.go:58
type IndexFunc func(obj interface{}) ([]string, error)
```

最常见的 `MetaNamespaceIndexFunc`（`index.go:85`）就是取对象的 namespace：

```go
// index.go:85
func MetaNamespaceIndexFunc(obj interface{}) ([]string, error) {
	meta, err := meta.Accessor(obj)
	return []string{meta.GetNamespace()}, nil
}
```
上面写的比较抽象，为了便于理解，我们以一个例子说明这三者的关系。

假设创建 Informer 时给了两个索引规则：

```go
indexers := cache.Indexers{
	cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,   // "namespace" -> 取 namespace
	"nodeName": func(obj interface{}) ([]string, error) { // "nodeName" -> 取调度节点
		pod, ok := obj.(*corev1.Pod)
		if !ok { 
			return []string{}, nil 
		}
		return []string{pod.Spec.NodeName}, nil
	},
}
```

随后 store 里先后存入三个 Pod：

| key | namespace | nodeName |
|---|---|---|
| `default/pod-a` | default | node-x |
| `default/pod-b` | default | node-x |
| `kube-system/pod-c` | kube-system | node-y |

每存入一个对象，Indexer 做两件事：**放进 `items` 主存储**（key → 对象），再用每个 `IndexFunc` 算出索引值、**把 key 挂进对应索引集合**。三个 Pod 全部加入后，`Indices` 内部长这样：

```
indices: Indices（索引名 -> 倒排表）
├─ "namespace": index
│   ├─ "default"       -> { "default/pod-a", "default/pod-b" }
│   └─ "kube-system"   -> { "kube-system/pod-c" }
└─ "nodeName": index
    ├─ "node-x"        -> { "default/pod-a", "default/pod-b" }
    └─ "node-y"        -> { "kube-system/pod-c" }
```

`index` 的 value 是集合（`sets.Set`），所以同一个属性值可以装多个对象的 key。

**更新时索引怎么变？** 假如 `pod-b` 被调度到新节点（nodeName 从 node-x 变成 node-y），Indexer 会从 `nodeName` 索引的旧值 `node-x` 集合里删掉 `default/pod-b`，再加进新值 `node-y` 集合：

```
"nodeName": index（更新前）              "nodeName": index（更新后）
├ "node-x" -> { default/pod-a,          ├ "node-x" -> { default/pod-a
│              default/pod-b }
└ "node-y" -> { kube-system/pod-c }     └ "node-y" -> { default/pod-b,
                                        │              kube-system/pod-c }
```

namespace 没变，`"namespace"` 索引无需改动。删除对象时则把它从所有涉及的索引集合里摘掉。

### 5.3 threadSafeMap：底层实现

`Indexer` 接口的默认实现是 `cache` 结构体（`store.go:204`），它的职责很明确：用 `KeyFunc` 把对象转成存储 key、做可选 transform，真正干活的还是组合进来的 `ThreadSafeStore`（`thread_safe_store.go:46`）。`ThreadSafeStore` 的实现是 `threadSafeMap`（`thread_safe_store.go:255`）：

```go
// thread_safe_store.go:255
type threadSafeMap struct {
	lock  sync.RWMutex
	items map[string]interface{}      // 主存储：storage key -> 对象
	index *storeIndex                 // 索引管理器：持有 Indexers（规则）+ Indices（结果）
	rv    string                      // 最近入库对象的 ResourceVersion
}
```

`lock` 用 `sync.RWMutex`：所有写操作持写锁，查询持读锁，读写互斥、读读并发，保证并发安全。

写入逻辑很直观（`thread_safe_store.go:323`）：先改 `items`，再调 `storeIndex.updateIndices(old, new, key)`（`thread_safe_store.go:226`）把 key 从旧索引值集合移到新索引值集合——就是 5.3 例子里画的那个过程。

另外两个整体操作简单提一下：

- `Replace`（`thread_safe_store.go:388`，Reflector relist 时调用）：整体替换 `items`，先清空全部索引（`reset()`），再对每个对象重算一遍索引。好处是即使平时增量更新有疏漏，一次 relist 也会把索引修回与主存储一致。
- `AddIndexers`（`thread_safe_store.go:502`）：运行期追加新索引，会对存量对象逐个补齐新索引；但**只能在 informer 启动前调用**，且索引名与已有索引冲突时会报错。


### 5.4 查询：ByIndex / Index / IndexKeys

索引最终是为了查询，四种查询方法对照上面的例子就一目了然：

```go
indexer := informer.GetIndexer()

// ByIndex：给出精确索引值，列出落在这个值里的全部对象
pods, _ := indexer.ByIndex(cache.NamespaceIndex, "default")
//   → [pod-a, pod-b]
pods, _ = indexer.ByIndex("nodeName", "node-x")
//   → [pod-a, pod-b]

// IndexKeys：和 ByIndex 同思路，但只返回 key 不返回对象，key 可直接喂给 GetByKey
keys, _ := indexer.IndexKeys("nodeName", "node-x")
//   → ["default/pod-a", "default/pod-b"]

// Index：入参是对象，先算出这个对象的索引值，再返回落在这些值里的"同类"
neighbors, _ := indexer.Index(cache.NamespaceIndex, somePod)  // 假设 somePod 在 kube-system
//   → [pod-c]

// ListIndexFuncValues：列出该索引的全部取值
values := indexer.ListIndexFuncValues("nodeName")
//   → ["node-x", "node-y"]
```

`Index` 和 `ByIndex` 最容易混淆，记住口诀：**`Index` 给对象找"同类"，`ByIndex`/`IndexKeys` 给「索引值」找对象**。

常见封装 `k8s.io/client-go/tools/cache/listers.go` 里的 `PodLister` 用的就是这套查询：按 namespace 枚举一批 Pod 用 `ByIndex(cache.NamespaceIndex, ns)`；按确切 key 取单个 Pod 用 `GetByKey(ns + "/" + name)`。

