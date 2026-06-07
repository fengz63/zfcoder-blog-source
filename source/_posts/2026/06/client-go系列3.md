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

注意 sharedIndexInformer 作为 `ResourceEventHandler` 传入 processDeltas，在 processDeltas 内部会调用 `s.OnAdd/OnUpdate/OnDelete`，将事件交给 sharedProcessor 分发。

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

三个 goroutine 协作：

`pop()` 函数（`shared_informer.go:1296`）：
从 `addCh` 读取通知，优先直接发送到 `nextCh`；如果 `nextCh` 阻塞，则写入 `pendingNotifications` ring buffer 暂存，然后从 ring buffer 读取并发送到 `nextCh`。

```go
// shared_informer.go:1296
func (p *processorListener) pop() {
	for {
		select {
		case notificationToAdd := <-p.addCh:
			// 尝试直接发送
			select {
			case p.nextCh <- notificationToAdd:
				continue
			default:
			}
			// nextCh 阻塞，写入 ring buffer
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

`run()` 函数（`shared_informer.go:1330`）：
从 `nextCh` 读取通知，调用 handler 的对应方法：

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

`watchSynced()` 函数（`shared_informer.go:1374`）：
等待上游同步完成，然后将自己的 syncing 状态置为 false（之后不再接收 sync 事件）。

### 4.2 延迟注册的 Handler

如果用户在 Informer 启动后才添加 EventHandler（`AddEventHandlerWithOptions`，`shared_informer.go:877`），Informer 会：
1. 锁定 `blockDeltas`，暂停事件分发
2. 添加 listener
3. 遍历当前 `indexer` 中的所有对象，为每个对象发送一条合成 `addNotification`（`isInInitialList = true`）
4. 释放锁，继续正常分发

这样延迟注册的 handler 也能获得完整的初始状态，不遗漏任何对象。

## 5. Indexer

`Indexer` 接口（`store.go:96`）在 `Store` 的基础上增加了索引能力：

```go
// store.go:96
type Indexer interface {
	Store
	// Index returns the stored objects whose set of indexed values
	// intersects with the set of indexed values of the given object
	Index(indexName string, obj interface{}) ([]interface{}, error)
	IndexKeys(indexName, indexedValue string) ([]string, error)
	ListIndexFuncValues(indexName string) []string
	ByIndex(indexName, indexedValue string) ([]interface{}, error)
	GetIndexers() Indexers
	AddIndexers(newIndexers Indexers) error
}
```

### 5.1 ThreadSafeStore

`ThreadSafeStore`（`thread_safe_store.go:46`）是 Indexer 的底层实现，使用 `sync.RWMutex` 保证并发安全：

```go
// thread_safe_store.go:256
type threadSafeMap struct {
	lock  sync.RWMutex
	items map[string]interface{}   // key -> object
	index *storeIndex              // 索引管理器
	rv    string                   // 当前 ResourceVersion
}
```

核心操作：

```go
// thread_safe_store.go:301
func (c *threadSafeMap) Add(key string, obj interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	oldObject := c.items[key]
	c.items[key] = obj
	c.index.updateIndices(oldObject, obj, key)
}

// thread_safe_store.go:388
func (c *threadSafeMap) Replace(items map[string]interface{}, resourceVersion string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.items = items
	c.rv = resourceVersion
	c.index.reset()
	for key, item := range c.items {
		c.index.updateIndices(nil, item, key)
	}
}
```

### 5.2 索引机制

`storeIndex`（`thread_safe_store.go:94`）管理多个索引：

```go
// thread_safe_store.go:94
type storeIndex struct {
	indexers Indexers                     // 索引器函数 map: name -> IndexFunc
	indices  map[string]Index             // 索引数据 map: name -> Index map: indexValue -> keys
}
```

`IndexFunc` 接收一个对象，返回一组索引值。例如 namespace 索引：

```go
// thread_safe_store.go:65
type IndexFunc func(obj interface{}) ([]string, error)

// 预定义的 MetaNamespaceIndexFunc
func MetaNamespaceIndexFunc(obj interface{}) ([]string, error) {
	meta, err := meta.Accessor(obj)
	return []string{meta.GetNamespace()}, nil
}
```

创建 Indexer 时可以指定索引：

```go
indexers := Indexers{
	"namespace": MetaNamespaceIndexFunc,
}
indexer := NewIndexer(MetaNamespaceKeyFunc, indexers)
// 后续可以按 namespace 查询：
pods, err := indexer.ByIndex("namespace", "default")
```

`updateIndices` 方法（`thread_safe_store.go:226`）在对象变更时维护索引的增删：

```
对象变更 → 从旧索引值中删除 key → 向新索引值中添加 key
```

## 6. 数据流总结

从 DeltaFIFO Pop 到用户 Handler 的完整链路：

```
DeltaFIFO.Pop()
    |
    v
sharedIndexInformer.handleDeltas()
    |
    v (加锁 blockDeltas)
processDeltas()  ← controller.go:607
    |
    ├── indexer.Add/Update/Delete  (更新本地缓存)
    │       └── threadSafeMap.Add/Delete
    │               └── storeIndex.updateIndices (维护索引)
    │
    └── sharedIndexInformer.OnAdd/OnUpdate/OnDelete
            |
            v
        sharedProcessor.distribute()
            |
            ├── processorListener[0].add() → pop() → run() → handler.OnAdd/OnUpdate/OnDelete
            ├── processorListener[1].add() → pop() → run() → handler.OnAdd/OnUpdate/OnDelete
            └── ...
```

## 7. 总结

Informer 和 Indexer 是 client-go 缓存机制的核心：

1. **sharedIndexInformer** 整合了 Reflector、DeltaFIFO、Indexer 和事件分发，提供一键式数据同步
2. **sharedProcessor + processorListener** 实现多 Handler 并发分发，ring buffer 防止慢 Handler 阻塞
3. **Indexer / ThreadSafeStore** 提供线程安全的本地缓存，支持按任意字段索引查询
4. 延迟注册的 Handler 通过 `blockDeltas` 锁定 + 历史对象回放，保证不丢失初始状态

下一篇文章将介绍 Controller 和 Workqueue，看如何将 Informer 的事件通知转化为可靠的任务处理流水线。
