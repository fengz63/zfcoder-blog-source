---
title: apiserver cache label index 优化
date: 2026-09-22 19:45:26
tags:
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 问题背景

在一些业务场景中，会存在很多直接向 apiserver 发起的 list 请求。为了只获取本任务的数据，请求通常会带上任务标识、应用名称等 label selector。

例如，一个集群缓存中存在 10 万个 pod，某个任务只有 100 个 pod，其中 80 个同时满足 app=train。客户端发起如下查询，希望拿到这 80 个 pod：

<!--more-->

```text
label selector: app=train

资源缓存：                 100000 个 pod
norbert-id=job-123：          100 个 pod
app=train：                20000 个 pod
同时满足两个条件：             80 个 pod
```

直觉上，这应该是一个很轻的请求：结果只有几十个对象，而且客户端已经明确给出了筛选条件。但在没有适用索引的缓存读取路径中，apiserver 并不是先定位任务的 100 个 pod，而是先遍历资源缓存中的全部 10 万个对象，再按 selector 过滤，最终返回 80 个。

当类似请求持续到来时，开销便与“资源总量 × list 请求频率”相关，而不是只与返回结果数量相关。更重要的是，枚举缓存需要持有读锁，会与对象变更写入缓存争用锁。

因此，问题不仅表现为 list 消耗 cpu、产生临时分配，还可能表现为客户端 watch 延迟升高。


## 2. 问题分析

### 2.1 list 为什么会影响 watch？

这里可能会有一个疑问，list 请求过高时，为什么会影响 watch lag 呢？

我们先看 watch_cache 中 list 请求获取候选对象的核心逻辑：

```go
// WaitUntilFreshAndList returns list of pointers to <storeElement> objects.
func (w *watchCache) WaitUntilFreshAndList(resourceVersion uint64, matchValues []storage.MatchValue, trace *utiltrace.Trace) ([]interface{}, uint64, error) {
	err := w.waitUntilFreshAndBlock(resourceVersion, trace)
	defer w.RUnlock()
	if err != nil {
		return nil, 0, err
	}

	// This isn't the place where we do "final filtering" - only some "prefiltering" is happening here. So the only
	// requirement here is to NOT miss anything that should be returned. We can return as many non-matching items as we
	// want - they will be filtered out later. The fact that we return less things is only further performance improvement.
	// TODO: if multiple indexes match, return the one with the fewest items, so as to do as much filtering as possible.
	for _, matchValue := range matchValues {
		if result, err := w.store.ByIndex(matchValue.IndexName, matchValue.Value); err == nil {
			return result, w.resourceVersion, nil
		}
	}
	return w.store.List(), w.resourceVersion, nil
}
```

这里存在两条路径：
- 如果请求提供了可用的索引条件，就按索引取候选集；
- 如果没有候选，或者候选索引都不可用，就回退到整个 store 的 list。

回退路径最终会执行 thread_safe_store.go 中的全量枚举：

```go
func (c *threadSafeMap) List() []interface{} {
	c.lock.RLock()
	defer c.lock.RUnlock()
	list := make([]interface{}, 0, len(c.items))
	for _, item := range c.items {
		list = append(list, item)
	}
	return list
}
```

从这段代码可以看出，即使带了 label selector 的 list 请求，仍会遍历 10w 个 pod。

了解了上述的流程后，回到 watchCache 中的`WaitUntilFreshAndList`函数，list 请求会持有 watchCache 读锁读取候选对象（waitUntilFreshAndBlock 函数会对 watchCache 加读写锁），当未配置相关索引时，需要从大量 pod 中取候选对象，读锁持有时间会很长。

与此同时，存储中的 pod 变更被 apiserver 接收后，需要进入同一个 watch cache 更新对象、事件记录和资源版本，代码如下：

```go
// processEvent is safe as long as there is at most one call to it in flight
// at any point in time.
func (w *watchCache) processEvent(event watch.Event, resourceVersion uint64, updateFunc func(*storeElement) error) error {
	key, err := w.keyFunc(event.Object)

	elem := &storeElement{Key: key, Object: event.Object}
	elem.Labels, elem.Fields, err = w.getAttrsFunc(event.Object)

	wcEvent := &watchCacheEvent{
		Type:            event.Type,
		Object:          elem.Object,
		ObjLabels:       elem.Labels,
		ObjFields:       elem.Fields,
		Key:             key,
		ResourceVersion: resourceVersion,
	}

	if err := func() error {
		w.Lock()
		defer w.Unlock()

		w.updateCache(wcEvent)
		w.resourceVersion = resourceVersion
		defer w.cond.Broadcast()

		return updateFunc(elem)
	}(); err != nil {
		return err
	}

	// Avoid calling event handler under lock.
	// This is safe as long as there is at most one call to processEvent in flight
	// at any point in time.
	if w.eventHandler != nil {
		w.eventHandler(wcEvent)
	}
	return nil
}
```

这就形成了读写争用：list 持有读锁枚举候选集，而事件处理需要写锁才能把新状态更新进缓存。全量枚举时间越长，事件更新就越可能排队；回调虽然在释放写锁之后执行，也必须先等前面的缓存更新完成。



### 2.2 watch cache 中的默认索引是怎么实现的？

前面的缓存读取逻辑已经有按索引取候选对象的分支，那么，apiserver 原本提供了哪些索引，又是如何接入的？

这里以 pod 的节点名字段索引为例，沿着“注册 → 维护 → 查询”的顺序看一下。

#### 2.2.1 索引注册：由资源存储显式提供索引函数

在 pod 的 strategy.go 中，内置索引的注册逻辑如下：

```go
func Indexers() *cache.Indexers {
    if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.SelectorIndex) {
        return &cache.Indexers{
            storage.FieldIndex("spec.nodeName"): NodeNameIndexFunc,
        }
    }
    return nil
}

func NodeNameIndexFunc(obj interface{}) ([]string, error) {
    pod, ok := obj.(*api.Pod)
    if !ok {
        return nil, fmt.Errorf("not a pod")
    }
    return []string{pod.Spec.NodeName}, nil
}
```

这里配置了两件事：索引叫什么，以及如何从对象中计算索引值。字段索引名会加上 f:前缀；索引函数则读取 pod 的节点名。例如，某个 pod 被分配到 node-a，索引函数就返回 node-a，表示应把这个 pod 的对象键放入 node-a 对应的桶。

apiserver watch cache 中索引函数从定义到最终生效，完整传递链路如下：

**a. 索引定义** - `strategy.go`

```go
func Indexers() *cache.Indexers {
    return &cache.Indexers{
        storage.FieldIndex("spec.nodeName"): NodeNameIndexFunc,
    }
}
```

**b. 传入 StoreOptions** - `storage.go`

```go
options := &generic.StoreOptions{
    Indexers: registrypod.Indexers(),
}
store.CompleteWithOptions(options)
```

**c. Store 调用 Decorator** - `store.go`

```go
e.Storage.Storage, e.DestroyFunc, err = opts.Decorator(
    opts.StorageConfig,
    prefix,
    keyFunc,
    e.NewFunc,
    e.NewListFunc,
    attrFunc,
    options.TriggerFunc,
    options.Indexers,  // 传入
)
```

**d. StorageWithCacher 创建 Cacher** - `storage_factory.go`

```go
cacherConfig := cacherstorage.Config{
    Indexers: indexers,  // 接收
}
cacher, err := cacherstorage.NewCacherFromConfig(cacherConfig)
```

**e. 创建 watchCache** - `cacher.go`

```go
watchCache := newWatchCache(
    config.CacheCapacity,
    config.KeyFunc,
    cacher.processEvent,
    config.GetAttrsFunc,
    config.Versioner,
    config.Indexers,  // 传入
)
```

**f. 初始化 Indexer** - `watch_cache.go`

```go
store: cache.NewIndexer(storeElementKey, storeElementIndexers(indexers))
```

**g. 底层存储** - `thread_safe_store.go`

```go
func NewThreadSafeStore(indexers Indexers, indices Indices) ThreadSafeStore {
    return &threadSafeMap{
        indexers: indexers,  // 保存索引函数
        indices:  indices,
    }
}
```

索引函数最终存储在 `threadSafeMap.indexers` 中。对象变更时 `updateIndices()` 调用这些函数维护索引。

#### 2.2.2 缓存落地：索引建立在当前对象集合上

watch cache 内部既保存当前对象集合，也维护一段历史事件窗口。这里讨论的字段索引建在当前对象的 store 上，并不是给历史事件数组建 label 或 field 索引。watch_cache 中初始化 store 时使用：

```go
cache.NewIndexer(storeElementKey, storeElementIndexers(indexers))
```

缓存存储的元素带有对象键、原始对象、labels 和 fields，并不是直接存一个裸 pod。因此，中间的适配层先从缓存元素中取出原始对象，再调用资源提供的索引函数。底层复用 client-go 的线程安全 map，维护如下关系：

```text
items：对象键 → 当前对象
  pod-key-a → pod-a，节点为 node-a
  pod-key-b → pod-b，节点为 node-a
  pod-key-c → pod-c，节点为 node-b

indexers：索引名 → 索引函数
  f:spec.nodeName → 提取 pod 的节点名

indices：索引名 → 索引值 → 对象键集合
  f:spec.nodeName
    node-a → {pod-key-a, pod-key-b}
    node-b → {pod-key-c}
```

所以，索引并不额外复制一份完整 pod，只保存指向原对象的键。

#### 2.2.3 索引维护：全量初始化时构建，对象变更时更新

apiserver 的 reflector 从后端存储读取对象后，会把全量结果交给 watch cache 替换当前集合；底层 store 随之清空并重建索引。后续收到对象新增、更新或删除事件时，再同步维护对应的桶。

例如，一个未调度 pod 的节点名为空，会先进入空字符串对应的桶；当它被调度到 node\-a 后，更新流程会删除旧桶中的对象键，再加入 node\-a 的桶。删除 pod 时，相应索引项也会移除。thread_safe_store 中，对象更新与索引维护发生在同一把底层存储写锁内：

```go
func (c *threadSafeMap) Update(key string, obj interface{}) {
    c.lock.Lock()
    defer c.lock.Unlock()
    oldObject := c.items[key]
    c.items[key] = obj
    c.updateIndices(oldObject, obj, key)
}
```

这意味着索引是随缓存写入持续维护的，而不是每次查询时临时扫描所有 pod 重新分组。它省下了读路径的全量枚举，但也带来索引内存和写入维护成本。



#### 2.2.4 索引查询：先识别精确字段条件，再按桶取对象

存储侧有索引，查询侧还必须声明哪些字段可索引。pod 的 predicate 内置了节点名字段候选；请求到来后，匹配器检查 field selector 中是否对这个字段提出了精确值要求，再生成带 f: 前缀的索引查询条件。

```text
请求条件：
  field selector: spec.nodeName=node-a

pod predicate 声明的可索引字段：
  spec.nodeName

生成索引候选：
  索引名 = f:spec.nodeName
  索引值 = node-a

按桶读取：
  indices["f:spec.nodeName"]["node-a"]
    → {pod-key-a, pod-key-b}
    → 从 items 取出 pod-a、pod-b

最后执行完整过滤，返回符合条件的对象
```

**由此可以看到，原有能力已经提供了索引注册、更新维护、候选提取和按桶读取的完整机制，限制主要在于可索引字段由资源代码固定提供。**



### 2.3 优化切入点

经过上面分析，我们知道核心问题在于 list 请求耗时比较久，导致 watchCache 持有的锁时间比较长。由于我们要保证并发场景的一致性，因此锁必须存在，此时自然而然的优化的方向：减少持有锁时间

对于当前业务来说，list 请求本来就携带标识度比较高的选择性条件。既然只需要某个任务的对象，就应该先通过索引拿到它们，而不是把不相关的对象都枚举一遍，这样就能有效减少锁的持有时间。

**优化目标由此变得明确：保留已有锁和最终过滤语义，把锁内枚举的对象数从整个资源缓存的 n，缩小到某个索引桶的 k。**


## 3. 优化方案

apiserver 原本已经有索引存储和按索引读取能力，缺少的是按业务 label 配置并接入查询的链路。因此，我们可以在 generic registry 初始化 storage 时，根据资源配置补充 indexer，相关代码见：

```Go
// SetLabelIndex sets label index keys for each group resource.
// it should be called only once before storage is created.
// each index is in format of "resource[.group]#labelKey1|labelKey2|..."
func SetLabelIndex(indexes []string) error {
    parsedIndexes := make(map[schema.GroupResource][]string)
    for _, index := range indexes {
       gr, keys, err := parseCacheLabelIndex(index)
       if err != nil {
          klog.Errorf(**"Failed to parse label index %s: %v"**, index, err)
          return err
       }
       if gr.Group == **"" **{
          // for core resource like pods, we register 2 groups to compatible with more use cases.
          coreGroupResource := schema.GroupResource{Group: **"core"**, Resource: gr.Resource}
          parsedIndexes[coreGroupResource] = keys
       }
       parsedIndexes[gr] = keys
    }
    storeLabelIndexesLock.Lock()
    defer storeLabelIndexesLock.Unlock()
    storeLabelIndexes = parsedIndexes
    return nil
}
```

例如 norbert-id 会生成 l:norbert-id 这个索引名，对应的索引函数从对象 labels 中取出 norbert-id 的值。后续缓存接收对象的新增、更新、删除时，复用已有索引维护机制更新对象键集合。

因此，当一个 pod 的任务标签从 job-123 变为 job-456 时，旧桶中的索引项会移除，新桶中加入该对象键；删除 pod 时也会移除相应索引项。查询不需要每次重新扫描全部对象来计算任务分组。


只在存储中建立索引还不够。前面分析过，查询使用的是 predicate 声明的可索引字段和 label；如果这里不知道 norbert-id 可索引，请求仍然无法生成对应的候选条件。

因此，还需要把配置中的 label 列表交给查询路径：

```Go
// decorateIndexerForGR decorates indexers used by storage for specified group resource.
func decorateIndexerForGR(resourcePrefix string, gr schema.GroupResource, origin *cache.Indexers) *cache.Indexers {
    ret := map[string]cache.IndexFunc{}
    indexLabelKeys := getLabelIndexKeysByGR(gr)

    // if origin is nil and no label index keys, then no need to decorate.
    if origin == nil && len(indexLabelKeys) == 0 {
       klog.V(4).Infof(**"No label indexes for resource %s"**, gr.String())
       return origin
    }

    // set default index funcs if origin is not nil.
    if origin != nil {
       for indexKey, indexFunc := range *origin {
          ret[indexKey] = indexFunc
       }
    }

    // add label index funcs according to config
    for _, labelKey := range indexLabelKeys {
       indexKey := storage.LabelIndex(labelKey)
       ret[indexKey] = cache.LabelIndexFunc(labelKey)
    }

    wrapRet := cache.Indexers(ret)
    // report cache index in used
    for indexKey := range ret {
       klog.V(4).Infof(**"Decorated label index %s for resource %s"**, indexKey, gr.String())
       cacher.ObserveCacheIndex(resourcePrefix, gr, indexKey)
    }

    return &wrapRet
}
```

随后，apiserver 从请求条件中提取精确的索引值：

```go
// For any index defined by IndexFields, if a matcher can match only (a subset)
// of objects that return <value> for a given index, a pair (<index name>, <value>)
// wil be returned.
func (s *SelectionPredicate) MatcherIndex() []MatchValue {
	var result []MatchValue
	for _, field := range s.IndexFields {
		if value, ok := s.Field.RequiresExactMatch(field); ok {
			result = append(result, MatchValue{IndexName: FieldIndex(field), Value: value})
		}
	}
	for _, label := range s.IndexLabels {
		if value, ok := s.Label.RequiresExactMatch(label); ok {
			result = append(result, MatchValue{IndexName: LabelIndex(label), Value: value})
		}
	}
	return result
}

```

按照上述例子，这一步生成的 label 候选依次是 l:norbert\-id=job\-123、l:app=train。它们被传入前面的缓存读取循环，第一个查询成功的索引桶就成为候选集。

至此，两端真正接通：**存储侧维护索引，查询侧生成候选条件，已有的缓存读取逻辑负责使用索引。** 缺少其中任意一端，都不能完成这次优化。

紧接着还有一个问题：**索引顺序会影响优化效果吗？**

当前实现按“field 候选在前、label 候选在后”的顺序尝试，label 内部再按配置顺序尝试；找到第一个可用索引就返回，不会自动挑选最小桶，也不会把两个桶做交集。

仍然上文的同一个请求，只调整配置顺序，就可能得到不同的执行成本：

|配置|先使用的索引|候选对象数|
|---|---|---|
|pods#norbert-id\|app|l:norbert-id=job-123|100|
|pods#app\|norbert-id|l:app=train|20000|

两种配置都能命中索引，也都返回 80 个 pod，但第二种仍需枚举和过滤 2 万个候选。这说明命中了索引并不等于选到了合适的索引。对于常见查询，应尽量让能够筛出更少对象的 label 排在前面。





## 4. 索引使用举例

看一个完整例子，沿用前面的任务查询，假设在 apiserver 启动参数中配置：

```text
--cache-label-indexes='pods#norbert-id|app'
```

这表示为 pods 资源建立 norbert-id 和 app 两个 label 索引，查询时优先尝试 norbert-id，再尝试 app。配置按 resource[.group]\#key1\|key2 的格式表达，多个资源配置用逗号分隔。

建立索引后，缓存除原有的“对象键 → 对象”映射外，还维护“索引名 → label 值 → 对象键集合”。索引不存另一份完整 pod，而是保存能定位到原对象的键：

```text
对象缓存
  pod-key-001 → pod 对象
  pod-key-002 → pod 对象
  ...

l:norbert-id
  job-123 → {pod-key-001, pod-key-002, ...}   共 100 个
  job-456 → {...}

l:app
  train   → {...}                          共 20000 个
```

现在再次执行 norbert-id=job-123,app=train：查询先命中 l:norbert-id 的 job-123 桶，取得 100 个候选对象，再对这 100 个对象执行完整 selector，得到 80 个结果。

|步骤|没有可用索引|命中任务索引|
|---|---|---|
|锁内枚举候选对象|100000 个|100 个|
|锁外执行完整筛选|检查 100000 个候选|检查 100 个候选|
|最终返回结果|80 个 pod|同样的 80 个 pod|

结果不变，但为得到结果所做的工作显著减少。候选集枚举及其切片规模由 o(n) 缩小到 o(k)，同时缩短读锁占用，为事件更新更早获取写锁创造条件。
