---
title: kube-apiserver 源码解读（八）REST 存储层与 genericregistry 的实现
date: 2026-10-14 10:00:00
tags:
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 引言

在前面的文章里，我们沿着 kube-apiserver 的启动、HTTP 处理链和认证流程看过请求如何进入 API Server。请求通过认证、授权、准入之后，最终还要落到一个具体资源的 REST 实现上：创建 Pod、读取 ConfigMap、更新 Deployment，分别由谁执行？资源对象如何变成 etcd 中的一条记录？

Kubernetes 将这部分拆成了几层：

```text
HTTP Handler（解码请求、准入、组织响应）
        │ 调用 rest.Creater / Getter / Updater / Lister / Watcher
        ▼
资源 REST 实现（ConfigMap REST、Pod REST 等）
        │ 多数资源嵌入 genericregistry.Store
        ▼
genericregistry.Store（通用 CRUD、策略钩子、键、版本与错误语义）
        │ storage.Interface
        ▼
存储装饰层（DryRun、Cacher 等）
        ▼
etcd3 store（编解码、加解密转换、etcd 操作）
        ▼
etcd
```

本文重点分析 `rest` 接口、`genericregistry.Store`、资源存储的装配过程，以及 Create、Update、Delete、List、Watch 如何进入底层存储。源码以 `staging/src/k8s.io/apiserver/pkg/registry` 和 `pkg/registry` 为主。

<!--more-->

## 2. REST 接口：HTTP 层与资源实现之间的契约

API 请求处理代码不需要知道每一种资源的内部实现。它只依赖 `staging/src/k8s.io/apiserver/pkg/registry/rest/rest.go` 中定义的接口。

常用接口包括：

| 接口 | 方法 | 对应操作 |
|------|------|----------|
| `Creater` | `New`、`Create` | 构造请求对象并创建资源 |
| `Getter` | `Get` | 按名称读取资源 |
| `Lister` | `NewList`、`List` | 按标签、字段条件列出资源 |
| `Updater` | `New`、`Update` | 更新资源，也可能支持更新时创建 |
| `GracefulDeleter` | `Delete` | 删除资源，支持优雅删除 |
| `CollectionDeleter` | `DeleteCollection` | 批量删除匹配的资源 |
| `Watcher` | `Watch` | 订阅资源变化 |

其中 `rest.StandardStorage` 聚合了常见的 Get、List、Create、Update、Delete、DeleteCollection、Watch 能力。资源 REST 可以实现整个接口，也可以只实现某个子资源需要的能力。例如 Pod 的主资源使用通用 Store，而 `pods/log`、`pods/exec` 等子资源会提供额外的定制实现。

以创建为例，`endpoints/handlers/create.go` 中的 Handler 负责读取并解码请求体、执行 admission、调用 REST 对象的 `Create`，再把返回对象编码到 HTTP 响应。存储层收到的是已经解码的 `runtime.Object`，不是 HTTP 字节流。这样 HTTP 协议处理和资源持久化可以独立演进。

## 3. genericregistry.Store：通用资源存储骨架

`staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go` 的 `Store` 实现了 `rest.StandardStorage`。它不是一个绑定 etcd 的简单 CRUD 包装，而是把资源通用语义集中起来：

- 对象和列表的构造函数：`NewFunc`、`NewListFunc`
- 资源名、对象名和存储键：`DefaultQualifiedResource`、`ObjectNameFunc`、`KeyFunc`、`KeyRootFunc`
- List/Watch 的选择条件：`PredicateFunc`
- 资源特有策略：`CreateStrategy`、`UpdateStrategy`、`DeleteStrategy`
- 操作后的扩展点：`BeginCreate`、`AfterCreate`、`BeginUpdate`、`AfterUpdate`、`AfterDelete`
- 底层存储与版本信息：`Storage`、`StorageVersioner`
- 表格输出、字段重置、垃圾回收、删除集合并发度等配置

结构体设计的关键是：Store 负责通用行为，Strategy 负责资源规则，底层 `storage.Interface` 负责存储操作。比如创建时，通用代码可以统一处理 `generateName`、系统元数据、验证和存储错误；而 ConfigMap、Pod 等策略决定对象是否命名空间作用域、如何校验或清理字段。

### 3.1 Strategy 将资源语义接入通用流程

`RESTCreateStrategy`、`RESTUpdateStrategy` 和 `RESTDeleteStrategy` 定义在 `rest/create.go`、`rest/update.go`、`rest/delete.go`。Strategy 提供诸如 `NamespaceScoped()`、Prepare、Validate、Warnings、更新时是否允许创建等能力。

Store 在流程中调用 `rest.BeforeCreate`、`rest.BeforeUpdate`、`rest.BeforeDelete` 等公共辅助函数。它们负责把策略的准备、元数据约束和校验步骤放在一致的位置；请求 Handler 提供的 admission 校验也会在适当阶段被调用。因此一次写请求通常包含两类校验：资源策略校验和通用 API 请求链中的准入校验。

### 3.2 Store 的键空间

Store 使用两个函数定位对象：

- `KeyRootFunc(ctx)` 返回集合根路径，用于 List、Watch。
- `KeyFunc(ctx, name)` 返回单个对象路径，用于 Create、Get、Update、Delete。

如果资源没有自定义键函数，`CompleteWithOptions` 会结合 `RESTOptions.ResourcePrefix` 和 `NamespaceScoped()` 自动补齐默认实现。命名空间资源的键形如：

```text
/registry/configmaps/<namespace>/<name>
```

集群级资源则形如：

```text
/registry/nodes/<name>
```

这里的前缀会从存储配置获取，不能简单假定所有集群都使用默认值。Store 的调用方只处理逻辑资源和名称，具体键结构由配置集中决定。

## 4. 从资源 REST 到底层存储：CompleteWithOptions

多数内置资源的存储构造采用相似模式。以 `pkg/registry/core/configmap/storage/storage.go` 为例：

```go
type REST struct {
    *genericregistry.Store
}

func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
    store := &genericregistry.Store{
        NewFunc:                   func() runtime.Object { return &api.ConfigMap{} },
        NewListFunc:               func() runtime.Object { return &api.ConfigMapList{} },
        PredicateFunc:             configmap.Matcher,
        DefaultQualifiedResource:  api.Resource("configmaps"),
        SingularQualifiedResource: api.Resource("configmap"),
        CreateStrategy: configmap.Strategy,
        UpdateStrategy: configmap.Strategy,
        DeleteStrategy: configmap.Strategy,
        TableConvertor:  /* 表格输出转换器 */,
    }

    options := &generic.StoreOptions{
        RESTOptions: optsGetter,
        AttrFunc:    configmap.GetAttrs,
    }
    if err := store.CompleteWithOptions(options); err != nil {
        return nil, err
    }
    return &REST{store}, nil
}
```

资源专属的代码只需描述“这是哪种对象、如何匹配、使用什么策略”，然后通过 `CompleteWithOptions` 接入通用存储配置。`CompleteWithOptions` 会检查必要字段，确定命名空间作用域，补充属性提取函数与默认键函数，并通过 `RESTOptionsGetter` 获取存储配置。

其后，如果 Store 没有显式注入底层 Storage，方法会调用 `opts.Decorator(...)` 创建存储实现，同时设置 Codec、StorageVersioner、ReadinessCheck 等。Decorator 的实际组合由 apiserver 的存储配置决定，通常包含 DryRun 支持以及用于 List/Watch 的 Cacher；Cacher 再以 etcd 存储作为底层数据源。若本来就注入了 Storage（常见于测试或特殊资源），则不会重复创建。

因此数据路径中至少有两个容易混淆的“存储”：

- `genericregistry.Store`：实现 Kubernetes REST 资源语义。
- `storage.Interface`：抽象持久化后端的 Get、Create、GuaranteedUpdate、Delete、GetList、Watch 等操作。

`etcd3.store` 是 `storage.Interface` 的一种实现，但 Store 并不直接依赖 etcd 客户端。

## 5. Create：先构造合法对象，再写入存储

`Store.Create` 的主流程位于 `store.go`：

```text
初始化 ObjectMeta（UID、时间戳等）
    ↓
必要时根据 generateName 生成 name
    ↓
BeginCreate（可选）
    ↓
rest.BeforeCreate(CreateStrategy, ...)
    ↓
Handler 传入的 createValidation（admission 等）
    ↓
ObjectNameFunc + KeyFunc 计算键
    ↓
计算 TTL，构造输出对象
    ↓
Storage.Create(ctx, key, obj, out, ttl, dryRun)
    ↓
错误解释、AfterCreate、Decorator
```

底层 Create 负责以“键不存在”为前提创建，避免并发请求覆盖已有对象。冲突会被转换成 API 层可识别的 `AlreadyExists` 错误；`generateName` 场景还可能重试生成名字。

对象写入前经过 Strategy 准备与校验，写入后由存储版本器填充 `resourceVersion` 等存储元数据。`AfterCreate` 和 `Decorator` 作用于成功结果的后处理。`BeginCreate` 则是较少使用的事务式钩子：它返回一个完成函数，Store 会在操作结束时告知成功或失败，以便调用方提交或回滚外围动作。

DryRun 不需要让每个资源实现自行模拟。Store 将 DryRun 标记传入 `DryRunnableStorage`，由该包装层控制是否真正落盘。

## 6. Update：GuaranteedUpdate 与乐观并发控制

更新是通用 Store 最能体现存储语义的地方。`Store.Update` 通过 `Storage.GuaranteedUpdate` 执行原子更新：底层读取当前对象，将其交给回调生成新对象，并在冲突时按后端语义重试或返回冲突。

核心步骤可以概括为：

1. 按请求上下文和对象名计算 key，并将请求对象里的 UID、`resourceVersion` 前置条件传给存储层。
2. `GuaranteedUpdate` 读取现存对象，并调用 `UpdatedObjectInfo.UpdatedObject(ctx, existing)` 合成候选对象。
3. 检查 `resourceVersion`：默认要求客户端版本与当前版本匹配；资源策略允许无条件更新时，可用最新版本继续。
4. 对象不存在时，只有 `AllowCreateOnUpdate()` 或调用方强制允许时才进入 create-on-update 流程。
5. 对更新对象执行 `rest.BeforeUpdate`、字段管理处理和 updateValidation。
6. 计算 TTL 并由底层存储原子提交；成功后调用 `AfterUpdate` 和 `Decorator`。

`resourceVersion` 是并发控制的一部分。两个客户端基于同一旧版本同时更新时，第一个提交成功后版本会变化；第二个请求因版本不匹配得到 Conflict，而不是静默覆盖第一个客户端的修改。这就是 Kubernetes 更新流程中的乐观并发控制。

`GuaranteedUpdate` 的回调也处理删除相关状态：当对象已有 `deletionTimestamp`、宽限期结束且 finalizer 已清空时，更新可能转入实际删除流程。反过来，普通 Delete 也可能先更新对象的删除时间或 finalizer，再等待控制器完成清理。

## 7. Delete：删除请求不一定马上删除键

`Store.Delete` 先读取对象，再根据 DeleteOptions、DeleteStrategy、宽限期和 finalizer 决定删除行为：

```text
读取现存对象
    ↓
BeforeDelete：计算 graceful / pendingGraceful
    ↓
存在宽限期、finalizer 或垃圾回收约束？
    ├─ 是：更新 deletionTimestamp / grace period / finalizer，等待后续清理
    └─ 否：Storage.Delete，立即移除对象
    ↓
finalizeDelete、AfterDelete，返回对象或 Status
```

因此，DELETE HTTP 请求成功不总表示 etcd 键已经立即消失。对于带 finalizer 的资源，API Server 会先标记删除，相关控制器完成清理并移除 finalizer 后，Store 才能最终删除对象。`Delete` 返回的布尔值用来表示是否立即删除；API 响应可以是被删除对象，也可以是通用 Status。

`DeleteCollection` 则基于 List 条件分页找出对象，再并发调用删除逻辑。源码明确指出它不是原子操作：批量删除中途失败时，前面已经处理的对象不会回滚。

## 8. Get、List、Watch：选择条件与缓存路径

### 8.1 Get

`Store.Get` 根据 `KeyFunc` 生成单对象键，再调用 `Storage.Get`。存储层解码对象并通过版本器还原元数据；Store 会将底层错误解释成带有资源类型和对象名的 Kubernetes API 错误，必要时应用 `Decorator`。

### 8.2 List

`Store.List` 从请求的 label selector 和 field selector 创建 `SelectionPredicate`，并传给 `Storage.GetList`。Predicate 包含 `GetAttrs`，用于从对象中提取标签和字段；资源可通过自己的 Matcher 定义允许选择的字段。

List 支持 `resourceVersion`、`resourceVersionMatch`、`limit` 和 `continue` 等选项。若选择条件只命中一个对象，Store 会尝试把递归集合查询优化为单 key 查询；若条件限定单一 namespace，也会尽量缩小查询范围。

具体 List 是从 Cacher 读取还是走 etcd，由装配的存储装饰器、请求版本选项和功能配置共同决定。`Store` 只表达查询语义并调用 `GetList`，缓存一致性和底层分页由下层实现处理。

### 8.3 Watch

`Store.Watch` 与 List 一样先构造 `SelectionPredicate`，把 `resourceVersion`、bookmark 和初始事件选项放进 `storage.ListOptions`，再基于集合根键调用 `Storage.Watch`。单对象 selector 可优化成单 key Watch。

通常情况下，这个调用先到 Cacher。Cacher 维护本地 watch cache，并将底层存储的变化分发给 API 请求创建的 watcher；Cacher 冷启动或需要同步时会通过内部 reflector 从底层存储获取数据。若请求由缓存无法满足，具体路径由 Cacher 的版本与一致性策略决定。

第九篇将进一步展开 Cacher、watch cache、内部 reflector 与 etcd Watch 的事件链路。本文关注的是 REST 存储边界：对 `genericregistry.Store` 来说，Watch 与 CRUD 一样都通过 `storage.Interface` 抽象执行。

## 9. 资源特例：通用 Store 之外的扩展

“大多数资源用 genericregistry.Store”不等于“所有 REST 都只是一个 Store”。一些资源有特殊子资源或额外不变量，会在嵌入 Store 的同时覆盖方法，或完全自定义 REST 行为。

例如 Pod 存储（`pkg/registry/core/pod/storage/storage.go`）会配置 matcher 和策略，并在 REST 层提供更多子资源支持；Namespace、Node 等也有自己的 REST 组合。Scale、Status 等子资源通常由独立 REST 类型处理，只暴露相应的 Getter、Updater 或其他接口。

这套接口组合让 API 安装代码能够按资源能力注册路由：标准资源走通用 Handler 和 Store，特殊子资源只需实现自己支持的 REST 合约。通用化减少了重复 CRUD 代码，定制点则保留了资源行为的边界。

## 10. 小结：从 HTTP 请求到持久化记录

一次普通资源写请求大致经过以下路径：

```text
HTTP Handler
  ├─ 解码、授权上下文、准入
  └─ 调用 REST 接口
       └─ 资源 REST（例如 ConfigMap REST）
            └─ genericregistry.Store
                 ├─ Strategy 与通用元数据语义
                 ├─ resourceVersion / finalizer / selector 处理
                 └─ storage.Interface
                      └─ Cacher / DryRun 等装饰层
                           └─ etcd3 store
                                └─ etcd
```

可以把各层的职责记成三句话：

- REST 接口规定 Handler 与资源存储之间如何协作。
- `genericregistry.Store` 实现 Kubernetes 资源通用的 CRUD 和并发语义。
- `storage.Interface` 隔离具体后端，etcd3、缓存和 DryRun 在这一层组合。

理解这条边界后，阅读具体资源时就可以先看它如何配置 Store，再只追踪它真正重写或扩展的部分；而遇到持久化、版本一致性和 Watch 缓存问题，则继续向 `storage.Interface` 的装饰器和 etcd3 实现深入。
