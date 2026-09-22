---
title: watchCache 内存泄漏问题分析与修复
date: 2026-09-21 20:30:00
tags:
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 问题背景与现象

在 kubernetes 的 apiserver 架构中，watchCache 被用来缓存集群中的资源状态，以减轻 storage backend（如 etcd 或 kubebrain）的查询压力。

当 apiserver 启动或 watch 发生断开重连时，watchCache 会通过 reflector 向 storage backend 发起全量 list 请求以初始化状态。如果在某些特定的 storage backend（例如 kubebrainv2）中禁用了分页读，全量 list 请求会一次性返回海量的资源对象（例如 10 万个 pod）。

**现象：** 在这种全量拉取场景下，即使绝大部分的 pod 在后续被删除，只要 watchCache 中还残留着哪怕 1 个旧 pod，整个 10 万个 pod 占用的巨大内存（可能高达上百兆）就无法被 go 垃圾回收器释放，导致 apiserver 出现严重的内存泄漏与驻留。

<!--more-->

## 2. 引起问题的关键细节与流程

导致这个问题的根本原因在于 go 语言的切片（Slice）底层内存模型与 kubernetes 原生数据结构的结合方式。

### 2.1 PodList 的内存布局

kubernetes 中 PodList 的定义如下：

```go
type PodList struct {
    metav1.TypeMeta
    metav1.ListMeta
    // 这是一个值类型切片，而不是 []*Pod
    Items []Pod
}
```

当反序列化一个包含 10 万个 pod 的大包时，`Items []Pod` 会在堆上分配一个**连续且巨大的底层数组**，这 10 万个 pod 的外层结构体紧凑地排列在这个内存块中。

### 2.2 Go 切片的底层内存模型

要理解这个泄漏，必须先理解 go 切片的内存布局。go 语言中一个切片在 runtime 中由三个字段构成：

```
┌─────────────────────────────────────┐
│  ptr (指针)  │  len (长度)  │  cap (容量)  │
│  ──────────────────────────────────  │
│  ptr ──────────►┌───┬───┬───┬───┬───┬───┬─────────┐
│                 │ 0 │ 1 │ 2 │ 3 │...│99999│ (空闲) │
│                 └───┴───┴───┴───┴───┴───┴─────────┘
│                       底层数组 (Backing Array)        │
└─────────────────────────────────────────────────────┘
```

关键点在于：**切片的 ptr 指向底层数组的起始地址，而底层数组是一个连续的内存块。go 的 gc 只看"是否有任何指针指向这块内存的任意位置"。**

举个具体例子：

```go
items := make([]Pod, 100000)  // 分配一个连续的底层数组
sub := items[50000:50001]     // 取子切片，sub.ptr 指向 items[50000]
items = nil                    // items 变量清空，但底层数组仍被 sub.ptr 引用
// 此时 gc 无法回收整个底层数组，因为 sub.ptr 还指向其中的某个位置
```

即使 `items` 变量已经置为 nil，只要 `sub` 还引用着底层数组中的**任何一个元素**，整个 10 万个 Pod 的内存块就无法被回收。这就是 go 官方文档中提到的 [A possible gotcha](https://go.dev/blog/slices-intro#a-possible-gotcha)。

### 2.3 泄漏流程与代码分析（k8s 1.17 - 1.26）

在 k8s 代码中，主要链路如下：

#### reflector 发起全量 list

reflector 通过 pager 向 storage backend 发起全量 list 请求，返回完整的 PodList：

```go
// staging/src/k8s.io/client-go/tools/cache/reflector.go
func (r *Reflector) list(ctx context.Context) error {
    // ...
    pager := pager.New(pager.SimplePageFunc(func(opts metav1.ListOptions) (runtime.Object, error) {
        return r.listerWatcher.ListWithContext(ctx, opts)
    }))
    // 当 etcd 禁用分页时，PageSize=0，一次性返回所有 pod
    pager.PageSize = 0
    list, paginatedResult, err = pager.ListWithAlloc(context.Background(), options)
    // ...
```

此时 list 返回的是一个 `*v1.PodList`，其中 `Items []Pod` 是一个包含 10 万个 Pod 的连续底层数组：

```
podList.Items 底层数组:
┌──────┬──────┬──────┬──────┬─────┬──────────┐
│Pod[0]│Pod[1]│Pod[2]│Pod[3]│ ... │Pod[99999]│
└──────┴──────┴──────┴──────┴─────┴──────────┘
```

#### extractList 提取指针（问题所在）

reflector 调用 `meta.ExtractList` 从 PodList 中提取每个 Pod 的指针：

```go
// staging/src/k8s.io/client-go/tools/cache/reflector.go
items, err := meta.ExtractList(list)  // k8s 1.18 使用的是这个，没有 WithAlloc
```

`ExtractList` 内部调用 `extractList(obj, false)`，关键逻辑如下：

```go
// staging/src/k8s.io/apimachinery/pkg/api/meta/help.go
func extractList(obj runtime.Object, allocNew bool) ([]runtime.Object, error) {
    itemsPtr, _ := GetItemsPtr(obj)    // 获取 PodList.Items 的 reflect.Value
    items, _ := conversion.EnforcePtr(itemsPtr)
    list := make([]runtime.Object, items.Len())

    for i := range list {
        raw := items.Index(i)          // raw = PodList.Items[i]（值类型 Pod）
        // ...
        default:  // allocNew=false 时走这里
            list[i], _ = raw.Addr().Interface().(runtime.Object)
            //       ^^^^
            //       raw.Addr() 就是 &podList.Items[i]
            //       直接取底层数组中第 i 个元素的地址
    }
    return list, nil
}
```

返回的 `list []runtime.Object` 中，每个元素都是 `&podList.Items[i]`——**指向同一个巨大底层数组中不同位置的指针**：

```
list[0] ──► &podList.Items[0] ──┐
list[1] ──► &podList.Items[1] ──┤
list[2] ──► &podList.Items[2] ──┼──► PodList.Items 底层数组（10万个Pod）
   ...                          │
list[99999] ► &podList.Items[99999]┘
```

#### syncWith 存入 watchCache 的 store

reflector 将提取出的指针传给 watchCache.Replace，最终存入 store：

```go
// staging/src/k8s.io/client-go/tools/cache/reflector.go
func (r *Reflector) syncWith(items []runtime.Object, resourceVersion string) error {
    found := make([]interface{}, 0, len(items))
    for _, item := range items {
        found = append(found, item)  // item 就是 &podList.Items[i]
    }
    return r.store.Replace(found, resourceVersion)
}

// staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go
func (w *watchCache) Replace(objs []interface{}, resourceVersion string) error {
    toReplace := make([]interface{}, 0, len(objs))
    for _, obj := range objs {
        object, _ := obj.(runtime.Object)  // object = &podList.Items[i]
        toReplace = append(toReplace, &storeElement{
            key:    key,
            object: object,  // 持有指向底层数组的指针
        })
    }
    w.store.Replace(toReplace, resourceVersion)  // 存入 map store
}
```

此时内存引用链：

```
map[key] → storeElement → object (即 &podList.Items[i]) → PodList.Items 底层数组
```

#### 删除 pod 后底层数组依然存活

当 pod 被删除时，map 中对应的 key 被移除，`&podList.Items[0]` ~ `&podList.Items[99998]` 这些指针都消失了。但如果还有 1 个 pod（比如 Pod[42]）留在 store 中：

```
map["pod-42"] → storeElement → &podList.Items[42] → PodList.Items 底层数组（10万个Pod的完整内存）
```

gc 扫描时发现：底层数组的第 42 个位置仍被引用，**整个数组不能回收**。这就是为什么删除 99,999 个 pod 后，内存纹丝不动。

> **注意：** k8s 1.27 之后改用 `ExtractListWithAlloc`，内部走 `case allocNew` 分支，通过 `reflect.New` + `Set` 做浅拷贝，返回的是独立新对象的指针，不再指向原始底层数组，从而打破引用链。

## 3. 优化与修复方案

为了彻底打破 watchCache 对大底层数组的引用依赖，在 `watchCache.Replace` 的关键入口处引入了浅拷贝机制。

### 3.1 浅拷贝实现逻辑

在将对象存入 `w.store` 之前，通过反射创建一个新的结构体实例，并将原对象的值拷贝过去：

```go
// 浅拷贝对象，打破对大型底层数组的引用
func shallowCopyObject(object runtime.Object) runtime.Object {
    val := reflect.ValueOf(object)
    if val.Kind() == reflect.Ptr {
        val = val.Elem()
    }
    // 在堆上分配一块独立的全新内存
    // 这里只拷贝一个 pod 的外层结构体字段（ObjectMeta、Spec 等），内部的 map/slice（如 Labels map[string]string）
    // 还是共享原对象的引用。但是这些内部数据是只读的，共享是安全的。
    newObj := reflect.New(val.Type())
    newObj.Elem().Set(val)
    return newObj.Interface().(runtime.Object)
}

```
此时修复前后内存引用对比：
- 修复前：
```
map[key] → &podList.Items[42] → PodList.Items 底层数组（整个数组不能回收）
```

- 修复后：
```
map[key] → newPod（堆上独立对象）→ 不指向原数组
                    ↘ newPod.Data → map（与原数组共享，安全）
```

### 3.2 Kubernetes 官方修复：ExtractListWithAlloc

Kubernetes 在 1.27 版本中通过 PR #113362 引入了 `ExtractListWithAlloc` 函数，原理相同：

```go
func ExtractListWithAlloc(obj runtime.Object) ([]runtime.Object, error) {
    return extractList(obj, true)  // allocNew=true
}

// extractList 中的关键逻辑
case allocNew:
    // shallow copy to avoid retaining a reference to the original list item
    itemCopy := reflect.New(raw.Type())
    itemCopy.Elem().Set(raw)
    list[i], _ = itemCopy.Interface().(runtime.Object)
```

Reflector 改用 `ExtractListWithAlloc`：

```go
// k8s 1.27+ reflector.go
items, err := meta.ExtractListWithAlloc(list)  // 使用 WithAlloc 版本
```

### 3.3 优化效果

1. **内存隔离**：浅拷贝会将原本挤在巨大底层数组里的 Pod 外壳，单独"抠"出来放到堆上独立的新内存中。
2. **释放底层数组**：存入 `w.store` 的变成了这些独立新对象的指针。当 Replace 流程执行完毕，原始的 `PodList.Items` 巨大底层数组瞬间失去了所有的指针引用，在下一次 GC 时即可被完全回收。
3. **性能开销极低**：相较于深拷贝（DeepCopy），浅拷贝只拷贝结构体的最外层，而内部复杂的 Map/Slice（如 Containers）依然共享指针。由于缓存对象是只读的，这种共享是安全的，完美兼顾了内存释放与 CPU 性能。


## 4. 参考资料

- [Issue #113305: Reflector retains initial list result for a long time](https://github.com/kubernetes/kubernetes/issues/113305)
- [Issue #102718: client-go memory leak](https://github://ubernetes/kubernetes/issues/102718)
- [PR #113362: Faster ExtractList. Add ExtractListWithAlloc variant](https://github.com/kubernetes/kubernetes/pull/113362)
- [Go Slices: usage and internals - A possible gotcha](https://go.dev/blog/slices-intro#a-possible-gotcha)
