---
title: kube-apiserver 源码解读（一）整体架构解析
date: 2026-06-13 16:00:00
tags:
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 引言

Kubernetes API Server（kube-apiserver）是整个 Kubernetes 控制平面的**核心入口**。所有对集群状态的读写操作——`kubectl` 命令、控制器调谐、Pod 调度——最终都汇聚到 apiserver。它承担了以下关键职责：

- **RESTful API 网关**：提供 HTTP(S) 接口，支持所有 Kubernetes 资源的 CRUD + Watch
- **认证与授权**：验证请求者身份，检查操作权限
- **准入控制**：在持久化前对资源进行策略校验和注入
- **存储代理**：将资源对象序列化后写入 etcd
- **聚合层与扩展**：支持 API 聚合（Aggregator）和自定义资源（CRD）

从代码架构上来看，kube-apiserver **并不是一个单体服务**，而是三个 `GenericAPIServer` 实例组成的**委托链**，每个实例只关心自己职责范围内的请求，处理不了的委托给下一个。

<!--more-->

本文是 **kube-apiserver 源码解读系列**的第一篇，从宏观视角俯瞰 apiserver 的整体架构，后续每篇深入一个模块。

---

## 2. 三层委托链架构

核心代码在 `cmd/kube-apiserver/app/server.go:176` 的 `CreateServerChain` 函数中构建：

```go
// cmd/kube-apiserver/app/server.go
func CreateServerChain(config CompletedConfig) (*aggregatorapiserver.APIAggregator, error) {
    // 3. 最底层：API Extensions Server（处理 CRD）
    apiExtensionsServer, _ := config.ApiExtensions.New(
        genericapiserver.NewEmptyDelegateWithCustomHandler(notFoundHandler),
    )

    // 2. 中间层：Kube API Server（处理内置 API）
    kubeAPIServer, _ := config.KubeAPIs.New(apiExtensionsServer.GenericAPIServer)

    // 1. 最外层：Aggregator Server（处理 APIService 代理）
    aggregatorServer, _ := controlplaneapiserver.CreateAggregatorServer(
        config.Aggregator, kubeAPIServer.ControlPlane.GenericAPIServer, ...)

    return aggregatorServer, nil
}
```

请求到达时，三个服务器的处理顺序如下：

```
Client Request
    │
    ▼
┌─────────────────────────────────────┐
│  Aggregator Server                  │  ← 先检查是否是 APIService
│  (kube-aggregator)                  │     是 → 代理到外部 API Server
│                                    │     否 → 委托给下一个
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│  Kube API Server                    │  ← 处理所有内置 API (apps/v1, core/v1...)
│  (core Kubernetes API)              │     是 → REST Storage → etcd
│                                    │     否 → 委托给下一个
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│  API Extensions Server              │  ← 处理 CRD 资源
│  (apiextensions-apiserver)          │     是 → REST Storage → etcd
│                                    │     否 → 404 Not Found
└─────────────────────────────────────┘
```

这种设计实现了**关注点分离**：
- **Aggregator**：管理外部 API 服务器的注册和反向代理（对应 `APIService` 资源）
- **Kube API Server**：管理所有内置资源（Pod、Service、Deployment 等）
- **API Extensions Server**：管理 CRD，动态注册自定义资源的路由和处理逻辑

每个 `GenericAPIServer` 都实现了 `DelegationTarget` 接口（`staging/src/k8s.io/apiserver/pkg/server/genericapiserver.go`）：

```go
type DelegationTarget interface {
    UnprotectedHandler() http.Handler
    PostStartHooks() map[string]postStartHookEntry
    PreShutdownHooks() map[string]preShutdownHookEntry
    HealthzChecks() []healthz.HealthChecker
    ListedPaths() []string
    NextDelegate() DelegationTarget
    PrepareRun() preparedGenericAPIServer
    MuxAndDiscoveryCompleteSignals() map[string]<-chan struct{}
    Destroy()
}
```

`NextDelegate()` 指向链中的下一个环节，形成一个**单向链表**。

---

## 3. HTTP 请求处理链（Handler Chain）

在请求到达具体的 REST Handler 之前，每一个请求都必须**按顺序**通过一系列 HTTP 过滤器。这些过滤器在 `DefaultBuildHandlerChain` 函数中串联起来（`staging/src/k8s.io/apiserver/pkg/server/config.go:1036`）：

```go
func DefaultBuildHandlerChain(apiHandler http.Handler, c *Config) http.Handler {
    handler := apiHandler

    // 最外层：panic 恢复
    handler = genericfilters.WithPanicRecovery(handler, c.RequestInfoResolver)
    // 解析请求信息（group/version/resource/verb）
    handler = genericapifilters.WithRequestInfo(handler, c.RequestInfoResolver)
    // CORS 头
    handler = genericfilters.WithCORS(handler, c.CorsAllowedOriginList, ...)
    // 超时控制
    handler = genericfilters.WithTimeoutForNonLongRunningRequests(handler, c.LongRunningFunc)

    // === 认证 ===
    handler = genericapifilters.WithAuthentication(handler,
        c.Authentication.Authenticator, failedHandler, ...)

    // === 审计 ===
    handler = genericapifilters.WithAudit(handler, c.AuditBackend, ...)

    // === 模拟请求（impersonation）===
    handler = impersonation.WithImpersonation(handler, c.Authorization.Authorizer, ...)

    // === 限流（优先级与公平性 / MaxInFlight）===
    handler = genericfilters.WithPriorityAndFairness(handler, c.LongRunningFunc,
        c.FlowControl, requestWorkEstimator, c.RequestTimeout/4)

    // === 授权 ===
    handler = genericapifilters.WithAuthorization(handler, c.Authorization.Authorizer, ...)

    // 跟踪、缓存控制、HSTS、优雅关闭等...
    return handler
}
```

完整的过滤器顺序（从外到内）：

| 顺序 | 过滤器 | 作用 |
|------|--------|------|
| 1 | WithPanicRecovery | 捕获 panic，返回 500 |
| 2 | WithRequestInfo | 解析 URL 路径到 RequestInfo（group/version/resource/verb） |
| 3 | WithRequestReceivedTimestamp | 记录请求到达时间 |
| 4 | WithCacheControl | 设置 Cache-Control 头 |
| 5 | WithTimeoutForNonLongRunningRequests | 为普通请求设置超时 context |
| 6 | WithAuthentication | **认证**：识别用户身份 |
| 7 | WithTracing | OpenTelemetry 链路追踪 |
| 8 | WithAudit | **审计**：记录请求事件 |
| 9 | WithImpersonation | 处理模拟请求头 |
| 10 | WithPriorityAndFairness / WithMaxInFlightLimit | **限流**：APF 或 MaxInFlight |
| 11 | WithAuthorization | **授权**：检查用户权限 |
| 12 | **REST Endpoint Handler** | **业务处理**：Create/Get/Update/Delete/Watch |

**有一个重要的设计细节**：请求体在整个 filter chain 中**不会被反序列化**，只有在通过了认证和授权之后，到达真正的 REST handler 时才会解析 body。认证层只能看到 HTTP headers，授权层只能看到 RequestInfo（URL path、verb 等），这保证了安全性和效率。

---

## 4. API 注册机制

当请求通过 filter chain 后，`APIServerHandler`（`staging/src/k8s.io/apiserver/pkg/server/handler.go`）将请求路由到对应的 go-restful 或非 RESTful handler。

API 注册的核心流程：

```
GenericAPIServer.InstallAPIGroups(apiGroupInfos...)
    │
    ├── 对每个 APIGroupInfo 调用 InstallAPIGroup()
    │
    ├── 创建 APIGroupVersion（关联了 Scheme、REST 存储、版本信息）
    │
    ├── APIGroupVersion.InstallREST(container)
    │   └── 创建 APIInstaller
    │       └── registerResourceHandlers()
    │           ├── 根据 REST 存储实现的接口（Getter/Creater/Lister/...）
    │           ├── 自动注册对应的 HTTP 路由和 Handler
    │           └── 例如：POST → Creater.Create → restfulCreateResource
    │
    └── 安装 Discovery 端点（列出该 API 组的版本和资源）
```

核心数据结构 `APIGroupInfo` 定义在 `staging/src/k8s.io/apiserver/pkg/endpoints/groupversion.go`：

```go
type APIGroupInfo struct {
    PrioritizedVersions []schema.GroupVersion              // 版本优先级列表
    VersionedResourcesStorageMap map[string]map[string]rest.Storage   // version -> resource -> storage
    Scheme              *runtime.Scheme                    // 类型注册表
    ParameterCodec      runtime.ParameterCodec
    NegotiatedSerializer runtime.NegotiatedSerializer
    OptionsExternalVersion *schema.GroupVersion
    MetaGroupVersion    *schema.GroupVersion
}
```

其中的 `VersionedResourcesStorageMap` 的 key 是 API 版本（如 `"v1"`），value 是一层 map：resource name → `rest.Storage` 实现。

`rest.Storage` 是一个**接口组合体系**（`staging/src/k8s.io/apiserver/pkg/registry/rest/rest.go`）：

```go
type Storage interface {
    New() runtime.Object        // 返回该资源的空对象（用于反序列化）
    Destroy()
}
type Getter interface { Get(ctx, name, options) (runtime.Object, error) }
type Lister interface { NewList() runtime.Object; List(ctx, options) (runtime.Object, error) }
type Creater interface { New() runtime.Object; Create(ctx, obj, validation, options) (runtime.Object, error) }
type Updater interface { Update(ctx, name, objInfo, ...) (runtime.Object, bool, error) }
type GracefulDeleter interface { Delete(ctx, name, ...) (runtime.Object, bool, error) }
type Watcher interface { Watch(ctx, options) (watch.Interface, error) }
type StandardStorage interface {
    Getter; Lister; CreaterUpdater; GracefulDeleter; CollectionDeleter; Watcher; Destroy()
}
```

`APIInstaller.registerResourceHandlers()` 会通过类型断言检查存储对象实现了哪些接口，然后自动注册对应的 HTTP 路由：

```go
// 伪代码逻辑
creater, isCreater := storage.(rest.Creater)
lister, isLister := storage.(rest.Lister)
watcher, isWatcher := storage.(rest.Watcher)
// ... 根据接口实现注册 GET/POST/PUT/DELETE/WATCH 等路由
```

---

## 5. 请求处理全流程（以一个 Pod 创建为例）

以 `POST /api/v1/namespaces/default/pods` 为例，完整流程如下：

```
1.  HTTPS 监听 (secure_serving.go)
    │
2.  APIServerHandler.ServeHTTP (handler.go)
    │   根据路径分发到 go-restful Container 或 NonGoRestfulMux
    │
3.  filter chain（从外到内依次执行）
    │   ├── WithPanicRecovery
    │   ├── WithRequestInfo → 解析出 {Group:"", Version:"v1", Resource:"pods", Verb:"create"}
    │   ├── WithAuthentication → 提取 Bearer Token → 验证 → 返回 user.Info
    │   ├── WithTracing
    │   ├── WithAudit → 开始记录审计事件
    │   ├── WithImpersonation → 检查模拟请求
    │   ├── WithPriorityAndFairness → 对请求分派 FlowSchema 和 PriorityLevel
    │   ├── WithAuthorization → 检查 user 是否有 create pods 权限
    │   └── (通过！进入 REST handler)
    │
4.  restfulCreateResource (endpoints/handlers/create.go)
    │   ├── 反序列化请求体 → runtime.Object (Pod)
    │   ├── 执行 defaulting（Scheme defaulter）
    │   ├── 执行 mutating admission（如 NamespaceLifecycle, DefaultStorageClass...）
    │   ├── 执行 validating admission（如 PodSecurity, ResourceQuota...）
    │   └── 调用 storage.Create()
    │
5.  genericregistry.Store.Create (registry/store.go)
    │   ├── Strategy.PrepareForCreate(ctx, obj)  → 预处理
    │   ├── Strategy.Validate(ctx, obj)          → 业务逻辑校验
    │   ├── Strategy.Canonicalize(obj)           → 规范化
    │   └── storage.Interface.Create(ctx, key, obj, ttl)  → etcd3 Put
    │
6.  响应返回
    └── responsewriters.WriteObjectNegotiated → JSON/Protobuf 序列化后返回
```

---

## 6. 存储层与 Watch 机制

### 存储后端

`staging/src/k8s.io/apiserver/pkg/storage/` 定义了存储抽象：

```go
type Interface interface {
    Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error
    Delete(ctx context.Context, key string, out runtime.Object, preconditions ...) error
    Watch(ctx context.Context, key string, opts WatchOptions) (watch.Interface, error)
    Get(ctx context.Context, key string, opts GetOptions, objPtr runtime.Object) error
    GetList(ctx context.Context, key string, opts ListOptions, listObj runtime.Object) error
    GuaranteedUpdate(ctx context.Context, key string, ...) error
}
```

默认实现是 **etcd3**（`staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go`）。

### Watch Cache

为了减轻 etcd 的压力，apiserver 在存储层之上增加了一层**内存缓存**（`staging/src/k8s.io/apiserver/pkg/storage/cacher/`）：

```
Client → cacher.Watch() → 内存缓存 → etcd3.Watch（仅一个底层 watch）
Client → cacher.List()  → 内存快照  （无需查 etcd）
```

- Cacher 启动时先 LIST 全量数据获取快照 + resourceVersion，然后从这个版本开始 WATCH
- 大量 List/Watch 请求全部从内存缓存中服务
- 写请求（Create/Update/Delete）直接穿透到 etcd，同时推动 cacher 更新

### 乐观并发控制

每个资源都有 `resourceVersion` 字段，对应 etcd 的 `mod_revision`。更新时必须提供当前 `resourceVersion`，否则返回 **409 Conflict**。Server-Side Apply 通过 `managedFields` 进一步精细化了并发控制。

---

## 7. 本系列后续文章规划

| 篇号 | 标题 | 核心源码路径 |
|------|------|-------------|
| 01 | **Kubernetes API Server 整体架构解析**（本文） | `cmd/kube-apiserver/`, `staging/src/k8s.io/apiserver/pkg/server/` |
| 02 | **kube-apiserver 启动流程详解** | `cmd/kube-apiserver/app/server.go`, `app/config.go` |
| 03 | **HTTP 请求处理链：装饰器模式在 Kubernetes 中的实践** | `pkg/server/config.go:1036`, `pkg/endpoints/filters/`, `pkg/server/filters/` |
| 04 | **认证机制（Authentication）** | `pkg/authentication/` |
| 05 | **授权机制（Authorization）** | `pkg/authorization/` |
| 06 | **准入控制（Admission Control）** | `pkg/admission/` |
| 07 | **API 注册与路由分发** | `pkg/endpoints/groupversion.go`, `installer.go` |
| 08 | **REST 存储层与 genericregistry.Store** | `pkg/registry/rest/`, `pkg/registry/generic/registry/` |
| 09 | **Watch 机制与 Watch Cache 实现** | `pkg/storage/cacher/`, `pkg/storage/etcd3/` |
| 10 | **API 聚合与 CRD 扩展原理** | `k8s.io/kube-aggregator/`, `k8s.io/apiextensions-apiserver/` |

---

以上就是本系列的开篇综述。下一篇（第 02 篇）我们从 `main()` 函数开始，追踪 kube-apiserver 的完整启动流程。
