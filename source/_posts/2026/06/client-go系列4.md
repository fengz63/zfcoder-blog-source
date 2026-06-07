---
title: client-go 源码解读（四）Controller 与 Workqueue
date: 2026-06-07 16:37:33
tags: 
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 概述

前文介绍了 Informer 如何从 DeltaFIFO 取出数据并更新 Indexer + 分发给 EventHandler。本文介绍整个 Informer 的最后一部分：**Controller**（低层协调者）、**ResourceEventHandler**（用户回调）和 **Workqueue**（可靠任务队列），最终展示一个完整的 Custom Controller 模式。

<!--more-->

回顾整个数据流：

```
API Server → Reflector → DeltaFIFO → processDeltas → Indexer
                                                       ↓
                                              ResourceEventHandler
                                                       ↓
                                                 Workqueue
                                                       ↓
                                              Custom Controller (处理逻辑)
```

## 2. 低层 Controller

`controller`（`controller.go:115`）是连接 Reflector 和 DeltaFIFO 的低层协调者，也是 sharedIndexInformer 的内部组件。

### 2.1 Config 配置

```go
// controller.go:44
type Config struct {
	Queue                // 必须是 DeltaFIFO（或 RealFIFO）
	ListerWatcher        // 执行 List/Watch
	Process   ProcessFunc     // 处理 Pop 出的 Deltas
	ProcessBatch ProcessBatchFunc  // 批量处理（可选）
	ObjectType runtime.Object
	FullResyncPeriod time.Duration
	MinWatchTimeout time.Duration
	ShouldResync ShouldResyncFunc
	WatchErrorHandler WatchErrorHandler
	WatchListPageSize int64
}
```

### 2.2 RunWithContext

`RunWithContext()`（`controller.go:170`）完成了生产者-消费者模式的搭建：

```go
// controller.go:170
func (c *controller) RunWithContext(ctx context.Context) {
	// 1. 创建 Reflector，将 Queue (DeltaFIFO) 作为 store 传入
	r := NewReflectorWithOptions(
		c.config.ListerWatcher,
		c.config.ObjectType,
		c.config.Queue,          // DeltaFIFO 作为 ReflectorStore！
		ReflectorOptions{...},
	)
	r.ShouldResync = c.config.ShouldResync

	// 2. 启动 Reflector goroutine（生产者）
	wg.StartWithContext(ctx, r.RunWithContext)

	// 3. 启动 processLoop（消费者）
	wait.UntilWithContext(ctx, c.processLoop, time.Second)
	wg.Wait()
}
```

### 2.3 processLoop

`processLoop()`（`controller.go:236`）持续从 Queue 中 Pop Deltas 并处理：

```go
// controller.go:236
func (c *controller) processLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, err = c.config.Pop(PopProcessFunc(c.config.Process))
			if errors.Is(err, ErrFIFOClosed) { return }
		}
	}
}
```

在 `sharedIndexInformer` 中，`Config.Process` 被设置为 `handleDeltas`，它调用 `processDeltas`。

### 2.4 processDeltas

`processDeltas()`（`controller.go:607`）将 Deltas 应用到 Store 并触发事件回调——它是 DeltaFIFO 消费者侧的核心逻辑：

```go
// controller.go:607
func processDeltas(
	handler ResourceEventHandler,
	clientState Store,
	deltas Deltas,
	isInInitialList bool,
	keyFunc KeyFunc,
) error {
	for _, d := range deltas {     // 从旧到新遍历
		obj := d.Object

		switch d.Type {
		case Sync, Replaced, Added, Updated:
			if old, exists, _ := clientState.Get(obj); exists {
				clientState.Update(obj)         // 更新本地缓存
				handler.OnUpdate(old, obj)      // 通知更新
			} else {
				clientState.Add(obj)            // 添加到本地缓存
				handler.OnAdd(obj, isInInitialList) // 通知新增
			}
		case Deleted:
			clientState.Delete(obj)             // 从本地缓存删除
			handler.OnDelete(obj)               // 通知删除
		case Bookmark:
			info, ok := obj.(BookmarkInfo)
			clientState.Bookmark(info.ResourceVersion) // 更新 ResourceVersion
		}
	}
	return nil
}
```

注意 `ReplacedAll` 和 `SyncAll` 类型（`controller.go:621-638`）用于新式 FIFO 的原子替换和全量同步场景，会直接遍历整个 clientState 调用 `OnUpdate`。

## 3. ResourceEventHandler

`ResourceEventHandler`（`controller.go:279`）是用户注册给 Informer 的回调接口：

```go
// controller.go:279
type ResourceEventHandler interface {
	OnAdd(obj interface{}, isInInitialList bool)
	OnUpdate(oldObj, newObj interface{})
	OnDelete(obj interface{})
}
```

- `OnAdd`：对象新增时调用。`isInInitialList` 标识该事件是否来自初始全量同步（而非后续增量）
- `OnUpdate`：对象更新时调用。注意 oldObj 是最后已知状态，不一定是上一版本
- `OnDelete`：对象删除时调用。obj 可能是 `DeletedFinalStateUnknown`（tombstone，当 watch 断连期间发生了删除）

### 辅助适配器

`ResourceEventHandlerFuncs`（`controller.go:292`）提供便捷方式，只需实现需要的函数：

```go
// controller.go:292
type ResourceEventHandlerFuncs struct {
	AddFunc    func(obj interface{})
	UpdateFunc func(oldObj, newObj interface{})
	DeleteFunc func(obj interface{})
}

func (r ResourceEventHandlerFuncs) OnAdd(obj interface{}, isInInitialList bool) {
	if r.AddFunc != nil { r.AddFunc(obj) }
}
```

`FilteringResourceEventHandler`（`controller.go:340`）提供事件过滤能力，只有满足条件的对象才会触发回调。

## 4. Workqueue

Workqueue（`util/workqueue/`）是 client-go 提供给用户的任务队列，用于将 Informer 的事件通知转化为可靠的异步处理流水线。它分为三层：

```
RateLimitingQueue  ← 限速重试
    ↑
DelayingQueue      ← 延时入队
    ↑
Queue              ← 基础队列（去重 + 并发安全）
```

### 4.1 基础 Queue

基础队列 `Typed[T]`（`queue.go:190`）的核心数据结构：

```go
// queue.go:190
type Typed[T comparable] struct {
	queue      Queue[T]     // 有序存储（FIFO slice）
	dirty      sets.Set[T]  // 待处理集合（去重）
	processing sets.Set[T]  // 处理中集合（保证并发安全）
	cond       *sync.Cond
	shuttingDown bool
}
```

三个集合的协作机制：

```
Add(item):
  如果 item 已经在 dirty 中且不在 processing 中 → Touch（优先级重置）
  否则 → 加入 dirty 和 queue，Signal 唤醒消费者

Get():
  从 queue Pop → 移入 processing，从 dirty 删除 → 返回 item

Done(item):
  从 processing 删除
  如果 item 仍在 dirty 中（处理期间又被 Add 了）→ 重新入 queue
```

这种设计实现了**去重**和**至少一次处理语义**：

```go
// queue.go:227 - Add
func (q *Typed[T]) Add(item T) {
	if q.dirty.Has(item) {
		if !q.processing.Has(item) {
			q.queue.Touch(item)  // 还没被处理，重置优先级
		}
		return
	}
	q.dirty.Insert(item)
	if q.processing.Has(item) {
		return  // 正在处理中，不重复入队，标记为 dirty 即可
	}
	q.queue.Push(item)
	q.cond.Signal()
}

// queue.go:265 - Get
func (q *Typed[T]) Get() (item T, shutdown bool) {
	for q.queue.Len() == 0 && !q.shuttingDown { q.cond.Wait() }
	item = q.queue.Pop()
	q.processing.Insert(item)
	q.dirty.Delete(item)
	return item, false
}

// queue.go:289 - Done
func (q *Typed[T]) Done(item T) {
	q.processing.Delete(item)
	if q.dirty.Has(item) {
		q.queue.Push(item)  // 处理期间又被要求重试，重新入队
		q.cond.Signal()
	}
}
```

### 4.2 DelayingQueue

`DelayingQueue`（`delaying_queue.go:37`）在基础 Queue 之上增加了 `AddAfter(item, duration)`，支持延时入队。

核心实现 `delayingType`（`delaying_queue.go:162`）：

```go
// delaying_queue.go:162
type delayingType[T comparable] struct {
	TypedInterface[T]                // 嵌入基础 Queue
	clock           clock.Clock
	heartbeat       clock.Ticker     // 最大等待时间
	waitingForAddCh chan *waitFor[T] // 等待队列
	stopCh          chan struct{}
}
```

`waitingLoop()` 后台 goroutine：

1. 从 `waitingForAddCh` 接收 `waitFor` 请求（含 item 和 readyAt 时间）
2. 使用最小堆（`waitForPriorityQueue`）按 readyAt 排序
3. 每次循环取堆顶最快要到期的元素，用 `clock.After(remaining)` 等待
4. 到期后调用 `q.Add(item)` 加入主队列

### 4.3 RateLimitingQueue

`RateLimitingQueue`（`rate_limiting_queue.go:27`）在 DelayingQueue 之上增加限速能力：

```go
// rate_limiting_queue.go:27
type TypedRateLimitingInterface[T comparable] interface {
	TypedDelayingInterface[T]
	AddRateLimited(item T)    // 按限速策略入队
	Forget(item T)            // 忘记重试次数
	NumRequeues(item T) int   // 查询重试次数
}
```

`AddRateLimited` 内部调用 `rateLimiter.When(item)` 计算等待时间，然后通过 `AddAfter(item, delay)` 延时入队。

### 4.4 默认限速器

`DefaultControllerRateLimiter()`（`default_rate_limiters.go:44`）组合了两种限速策略：

```go
func DefaultTypedControllerRateLimiter[T comparable]() TypedRateLimiter[T] {
	return TypedMaxOfRateLimiter[T](
		NewTypedItemExponentialFailureRateLimiter[T](5*time.Millisecond, 1000*time.Second),
		// 令牌桶：每秒 10 个，桶容量 100
		&BucketRateLimiter[T]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}
```

- **ItemExponentialFailureRateLimiter**：按 Item 维度指数退避，从 5ms 到 1000s，每次失败翻倍
- **BucketRateLimiter**：全局令牌桶，每秒 10 个，防止整体流量过大

两者取最大值，同时限制单 Item 和全局速率。

## 5. Custom Controller 模式

将以上所有组件组合起来，就是一个标准的 Kubernetes Controller 模式。以下是官方示例的简化版：

```go
// 1. 创建 Informer Factory
factory := informers.NewSharedInformerFactory(clientset, 0)

// 2. 获取具体资源的 Informer
informer := factory.Core().V1().Pods().Informer()

// 3. 创建 RateLimitingQueue
queue := workqueue.NewTypedRateLimitingQueue[string](
	workqueue.DefaultTypedControllerRateLimiter[string](),
)

// 4. 注册 EventHandler（核心逻辑都在这里）
informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
	AddFunc: func(obj interface{}) {
		key, _ := cache.MetaNamespaceKeyFunc(obj)
		queue.AddRateLimited(key)  // 将 key 加入工作队列
	},
	UpdateFunc: func(old, new interface{}) {
		key, _ := cache.MetaNamespaceKeyFunc(new)
		queue.AddRateLimited(key)
	},
	DeleteFunc: func(obj interface{}) {
		key, _ := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
		queue.AddRateLimited(key)
	},
})

// 5. 启动 Informer（开始 List & Watch）
stopCh := make(chan struct{})
defer close(stopCh)
factory.Start(stopCh)
factory.WaitForCacheSync(stopCh)

// 6. 启动 Worker 协程（处理队列）
for i := 0; i < 3; i++ {
	go worker(queue, informer)
}

// 7. Worker 函数
func worker(queue workqueue.TypedRateLimitingInterface[string],
	informer coreinformers.PodInformer) {
	for {
		key, shutdown := queue.Get()
		if shutdown { return }

		func() {
			defer queue.Done(key)

			// 从 Indexer 获取最新对象
			obj, exists, err := informer.GetIndexer().GetByKey(key)
			if err != nil {
				queue.AddRateLimited(key)  // 错误重试
				return
			}
			if !exists {
				// 对象已被删除，处理删除逻辑
				return
			}

			// 执行核心业务逻辑
			if err := reconcile(obj); err != nil {
				queue.AddRateLimited(key)  // 失败，按指数退避重试
				return
			}
			queue.Forget(key)  // 成功，重置限速器
		}()
	}
}
```

这个模式的关键点：

1. **Informer 负责数据同步**：通过 List & Watch 保持本地缓存与 API Server 一致
2. **EventHandler 做轻量级转发**：收到通知后仅将 key 入队，不做重处理
3. **Workqueue 提供可靠性**：去重、延时重试、限速，防止 thundering herd
4. **Worker 负责业务逻辑**：从 queue 读取 key，从 indexer 获取最新数据，执行调谐

