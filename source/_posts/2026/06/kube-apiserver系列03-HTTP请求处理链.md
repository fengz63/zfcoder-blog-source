---
title: kube-apiserver 源码解读（三）HTTP 请求处理链：装饰器模式在 Kubernetes 中的实践
date: 2026-06-15 10:00:00
tags:
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 引言

在上一篇中，我们追踪了 kube-apiserver 的启动流程，看到 `GenericAPIServer.New()` 内部通过 `DefaultBuildHandlerChain` 函数组装了一个层层嵌套的 HTTP 中间件链。这个链是 apiserver 安全性和可观测性的基石——每一个请求在到达实际的 REST 业务逻辑之前，都要按顺序通过数十个过滤器。

本文将深入这个**HTTP 请求处理链**，详细剖析每一层过滤器的职责和实现，并揭示 Kubernetes 如何通过**装饰器模式（Decorator Pattern）** 优雅地组合这些功能。

<!--more-->

从设计模式的角度看，`DefaultBuildHandlerChain` 是装饰器模式的经典实践：

```
http.Handler          ← 抽象组件接口
  │
  ├─ ConcreteComponent: REST endpoint handler（实际的 API 业务逻辑）
  │
  ├─ Decorator: WithAuthentication（认证）
  ├─ Decorator: WithAuthorization（授权）
  ├─ Decorator: WithAudit（审计）
  ├─ Decorator: WithPriorityAndFairness（限流）
  └─ ... 每个过滤器包裹下一个
```

每个装饰器实现的 `ServeHTTP` 在调用下一个 handler 之前和/或之后执行自己的逻辑，形成一个**洋葱模型**（也叫中间件链）。

---

## 2. 处理链全景

`DefaultBuildHandlerChain` 定义在 `staging/src/k8s.io/apiserver/pkg/server/config.go:1036`。它接受一个 `apiHandler`（最内层的 REST handler）和一个 `Config`，返回层层包装后的最终 handler。

**代码中的构建顺序（从内到外）**：

```go
func DefaultBuildHandlerChain(apiHandler http.Handler, c *Config) http.Handler {
    handler := apiHandler

    // 第 1 组：授权（最内层）
    handler = filterlatency.TrackCompleted(handler)
    handler = genericapifilters.WithAuthorization(handler, c.Authorization.Authorizer, c.Serializer)
    handler = filterlatency.TrackStarted(handler, c.TracerProvider, "authorization")

    // 第 2 组：限流（APF 或 MaxInFlight）
    if c.FlowControl != nil {
        handler = genericfilters.WithPriorityAndFairness(handler, ...)
    } else {
        handler = genericfilters.WithMaxInFlightLimit(handler, ...)
    }

    // 第 3 组：模拟请求（Impersonation）
    handler = impersonation.WithImpersonation(handler, ...)
    // 或 WithConstrainedImpersonation (feature gate)

    // 第 4 组：审计
    handler = genericapifilters.WithAudit(handler, ...)

    // 第 5 组：链路追踪（在认证之后，让已认证用户可影响采样决策）
    handler = genericapifilters.WithTracing(handler, ...)

    // 第 6 组：认证
    handler = genericapifilters.WithAuthentication(handler, ...)

    // 第 7 组：通用 HTTP 处理器
    handler = genericfilters.WithCORS(handler, ...)
    handler = genericapifilters.WithWarningRecorder(handler)
    handler = genericfilters.WithTimeoutForNonLongRunningRequests(handler, ...)
    handler = genericapifilters.WithRequestDeadline(handler, ...)
    handler = genericfilters.WithWaitGroup(handler, ...)
    handler = genericfilters.WithWatchTerminationDuringShutdown(handler, ...)
    handler = genericfilters.WithProbabilisticGoaway(handler, ...)
    handler = genericapifilters.WithCacheControl(handler)
    handler = genericfilters.WithHSTS(handler, ...)
    handler = genericfilters.WithRetryAfter(handler, ...)
    handler = genericfilters.WithHTTPLogging(handler)
    handler = genericapifilters.WithLatencyTrackers(handler)
    handler = routine.WithRoutine(handler, ...)              // feature gate
    handler = genericapifilters.WithRequestInfo(handler, ...)
    handler = genericapifilters.WithRequestReceivedTimestamp(handler)
    handler = genericapifilters.WithMuxAndDiscoveryComplete(handler, ...)
    handler = genericfilters.WithPanicRecovery(handler, ...)
    handler = genericapifilters.WithAuditInit(handler)      // 最外层
    return handler
}
```

**请求的实际处理顺序（从外到内）**：

```
WithAuditInit                    ← 初始化审计上下文、生成 Audit-ID
  WithPanicRecovery              ← 捕获 panic，返回 500
    WithMuxAndDiscoveryComplete  ← 路由未就绪时返回 503 而非 404
      WithRequestReceivedTimestamp  ← 记录请求到达时间
        WithRequestInfo           ← 解析 URL → Group/Version/Resource/Verb
          WithRoutine             ← 分离 goroutine 减小栈开销（feature gate）
            WithLatencyTrackers   ← 初始化延迟追踪器
              WithHTTPLogging     ← 记录 HTTP 日志
                WithRetryAfter    ← 关闭期间返回 429 Retry-After
                  WithHSTS        ← 设置 Strict-Transport-Security
                    WithCacheControl  ← 设置 Cache-Control: no-cache
                      WithProbabilisticGoaway  ← 随机发送 HTTP/2 GOAWAY
                        WithWatchTerminationDuringShutdown  ← 关闭时终止 Watch
                          WithWaitGroup  ← 追踪请求以支持优雅关闭
                            WithRequestDeadline  ← 设置 context deadline
                              WithTimeoutForNonLongRunningRequests  ← 超时控制
                                WithWarningRecorder  ← 警告去重与记录
                                  WithCORS  ← 跨域支持
                                    ──────────────────────────────────
                                    [filterlatency TrackStarted] ← 认证
                                    WithAuthentication  ← 认证用户身份
                                    [filterlatency TrackCompleted]
                                    [filterlatency TrackStarted] ← 追踪
                                    WithTracing  ← OpenTelemetry 链路追踪
                                    [filterlatency TrackCompleted]
                                    [filterlatency TrackStarted] ← 审计
                                    WithAudit  ← 记录审计事件
                                    [filterlatency TrackCompleted]
                                    [filterlatency TrackStarted] ← 模拟
                                    WithImpersonation  ← 处理模拟请求
                                    [filterlatency TrackCompleted]
                                    [filterlatency TrackStarted] ← 限流
                                    WithPriorityAndFairness  ← 请求排队与公平调度
                                    [filterlatency TrackCompleted]
                                    [filterlatency TrackStarted] ← 授权
                                    WithAuthorization  ← 校验权限
                                    [filterlatency TrackCompleted]
                                    ──────────────────────────────────
                                    REST Endpoint Handler (apiHandler)
                                      │
                                      ├─ Director → go-restful / NonGoRestfulMux
                                      └─ REST Handler → Storage → etcd
```

这是一个**三层结构**：

| 层 | 范围 | 典型过滤器 |
|----|------|-----------|
| **外层** | 通用 HTTP 处理 | PanicRecovery、RequestInfo、CORS、HSTS、CacheControl、Timeout、RetryAfter |
| **中层** | 安全与可观测 | Authentication、Tracing、Audit、Impersonation、PriorityAndFairness、Authorization |
| **内层** | 认证/授权/限流（用 filterlatency 观测） | 同一组，但被 latency tracking 包裹 |

为什么 `filterlatency.TrackStarted/Completed` 只包裹了中间的这六个过滤器？因为这六个是**安全关键路径**，它们的延迟直接关系到 apiserver 的性能和安全性，需要被重点监控。其他过滤器（如 CORS、HSTS）的耗时可以忽略不计。

---

## 3. 外层过滤器：通用 HTTP 处理

### 3.1 WithAuditInit — 审计初始化

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/filters/audit_init.go
func WithAuditInit(handler http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // 如果客户端提供了 Audit-ID 头部，则复用；否则生成 UUID
        auditID := r.Header.Get("Audit-ID")
        if auditID == "" {
            auditID = uuid.New().String()
        }
        // 将审计上下文注入 request context
        ctx := audit.WithAuditContext(r.Context())
        audit.WithAuditID(ctx, auditID)
        // 将 Audit-ID 通过响应头部回传给客户端
        w.Header().Set("Audit-ID", auditID)
        handler.ServeHTTP(w, r.WithContext(ctx))
    })
}
```

这是**绝对最外层**的过滤器。它在请求开始时创建一个 `AuditContext`，生成或回显 `Audit-ID`，并将其注入请求上下文。后续的所有代码都可以通过 `audit.GetAuditContext(ctx)` 获取这个上下文并添加审计事件。

**为什么在最外层？** 因为审计需要贯穿整个请求生命周期——从入口到出口，包括 panic 恢复后的阶段。

### 3.2 WithPanicRecovery — Panic 恢复

```go
// staging/src/k8s.io/apiserver/pkg/server/filters/wrap.go
func WithPanicRecovery(handler http.Handler, resolver request.RequestInfoResolver) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        defer runtime.HandleCrash(func(panicReason interface{}) {
            // 如果是 ErrAbortHandler（超时中止），只记录指标不记录栈
            if panicReason == http.ErrAbortHandler {
                metrics.RecordRequestAbort()
                return
            }
            // 否则记录完整栈信息，返回 500
            // ...
        })
        handler.ServeHTTP(w, req)
    })
}
```

这层确保了任何内部 panic 都不会导致进程崩溃。`http.ErrAbortHandler` 是一个特殊值——超时过滤器在检测写超时后会用它 panic 来强制断开连接，此时不需要打印栈信息。

### 3.3 WithRequestInfo — 请求信息解析

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/filters/requestinfo.go
func WithRequestInfo(handler http.Handler, resolver request.RequestInfoResolver) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        // 从 URL 路径解析：/api/v1/namespaces/default/pods/mypod
        //  → Info{Group:"", Version:"v1", Resource:"pods", Namespace:"default",
        //          Name:"mypod", Verb:"get"}
        info, err := resolver.NewRequestInfo(req)
        if err != nil {
            // 路径无法解析 → InternalError
            responsewriters.ErrorNegotiated(...)
            return
        }
        ctx := request.WithRequestInfo(req.Context(), info)
        handler.ServeHTTP(w, req.WithContext(ctx))
    })
}
```

**`RequestInfo`** 是整个处理链中最重要的数据结构之一。它包含了经过标准化后的请求元数据：

```go
type RequestInfo struct {
    IsResourceRequest bool     // 是否是资源请求（vs 非资源端点如 /healthz）
    Path              string
    Verb              string   // get/list/create/update/delete/watch/proxy/...
    APIPrefix         string
    APIGroup          string
    APIVersion        string
    Namespace         string
    Resource          string
    Subresource       string
    Name              string
    Parts             []string
}
```

`Verb` 的标准化逻辑有特殊处理：
- `POST` → `create`
- `GET` + 指定 name → `get`，未指定 name + `watch` 参数 → `watch`，否则 → `list`
- `PUT` → `update`
- `PATCH` → `patch`
- `DELETE` → `delete`
- patchname:port 路径 → `proxy`

后续的认证、授权、审计等过滤器全部依赖 RequestInfo 来做决策。

### 3.4 WithMuxAndDiscoveryComplete — 路由就绪保护

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/filters/mux_discovery_complete.go
func WithMuxAndDiscoveryComplete(handler http.Handler, signal <-chan struct{}) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        select {
        case <-signal:
            // 路由已就绪，正常处理
        default:
            // 路由尚未注册完，在 context 中标记
            ctx := request.WithMuxAndDiscoveryComplete(req.Context(), false)
            req = req.WithContext(ctx)
        }
        handler.ServeHTTP(w, req)
    })
}
```

启动初期，API 路由可能还没有完全注册到 mux（go-restful），此时发来的请求如果路由不匹配会返回 404。但在"路由正在安装"阶段返回 404 会误导控制器（如 GC 控制器）认为资源不存在。这个过滤器设置一个 context 标记，让 `NotFoundHandler` 在路由未就绪时返回 503 代替 404。

### 3.5 WithCORS — 跨域支持

```go
// staging/src/k8s.io/apiserver/pkg/server/filters/cors.go
func WithCORS(handler http.Handler, allowedOriginPatterns []string, ...) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        if len(allowedOriginPatterns) == 0 {
            handler.ServeHTTP(w, req)   // 无配置 → 透传
            return
        }
        origin := req.Header.Get("Origin")
        // 正则匹配 allowedOriginPatterns
        if matchedOrigin(origin, allowedOriginPatterns) {
            w.Header().Set("Access-Control-Allow-Origin", origin)
            // ... 设置其他 CORS 头
        }
        if req.Method == "OPTIONS" {
            w.WriteHeader(http.StatusNoContent)  // Preflight → 204
            return
        }
        handler.ServeHTTP(w, req)
    })
}
```

`allowedOriginPatterns` 默认通过 `--cors-allowed-origins` 命令行参数配置。

### 3.6 WithTimeoutForNonLongRunningRequests — 超时控制

```go
// staging/src/k8s.io/apiserver/pkg/server/filters/timeout.go
func WithTimeoutForNonLongRunningRequests(handler http.Handler, longRunning apirequest.LongRunningRequestCheck) http.Handler {
    return WithTimeout(handler, longRunning, func(r *http.Request) (time.Duration, string) {
        // 从 RequestInfo 中获取超时参数
        requestInfo, ok := request.RequestInfoFrom(r.Context())
        if !ok { return 0, "" }
        if requestInfo.Verb == "watch" { return 0, "" }  // watch 不限时
        // 从 URL query 中读取 timeout 参数
        timeout := requestTimeoutFromURL(r)
        return timeout, "request"  // 返回超时值和错误消息
    })
}
```

核心的 `WithTimeout` 函数：

```go
func WithTimeout(handler http.Handler, longRunning apirequest.LongRunningRequestCheck, 
                 timeoutFunc timeoutFactory) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // 长请求（watch/proxy/exec/portforward/attach/log）直接透传
        if longRunning(r) {
            handler.ServeHTTP(w, r)
            return
        }
        // 在 goroutine 中执行实际 handler
        // 原始 context 带有 deadline，超时后 context 取消
        // 如果请求处理超时：
        //   - 还没开始写响应 → 返回 504
        //   - 已经开始写响应 → 用 http.ErrAbortHandler panic 断开连接
    })
}
```

**关键设计**：超时不是简单地返回 504，而是使用 goroutine 并发执行。如果超时发生且 handler 已经开始往 ResponseWriter 写数据（如 list 返回大量对象时），强行返回 504 会导致响应头和 body 混乱。此时用 `http.ErrAbortHandler` panic 来关闭底层 TCP 连接，客户端收到连接重置后自行重试。

这也是为什么 `WithPanicRecovery` 需要特殊处理 `ErrAbortHandler` 的原因。

### 3.7 WithRequestDeadline — context deadline 设置

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/filters/request_deadline.go
func WithRequestDeadline(handler http.Handler, ...) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        // 长请求跳过
        if longRunning(req) { handler.ServeHTTP(w, req); return }
        // 从 URL 参数 ?timeout= 解析（默认 60s，最大可通过 --request-timeout 配置）
        timeout := parseTimeout(req)
        deadline := receivedTimestamp.Add(timeout)
        ctx, cancel := context.WithDeadline(req.Context(), deadline)
        defer cancel()
        handler.ServeHTTP(w, req.WithContext(ctx))
    })
}
```

`WithTimeoutForNonLongRunningRequests` 和 `WithRequestDeadline` 协同工作：
1. `WithRequestDeadline` 在 context 上设置 deadline
2. `WithTimeoutForNonLongRunningRequests` 检测 context 超时，决定返回 504 还是 panic 断开

---

## 4. 中层过滤器：安全与可观测性

这六层过滤器被 `filterlatency.TrackStarted/TrackCompleted` 包裹，意味着它们的延迟会被记录到 Prometheus 指标和审计日志中。

### 4.1 WithAuthentication — 认证

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go
func WithAuthentication(handler http.Handler, auth authenticator.Request, 
                        failed http.Handler, apiAuds authenticator.Audiences, ...) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        // 1. 执行认证
        resp, ok, err := auth.AuthenticateRequest(req)

        // 2. 认证失败 → 交给 failed handler（返回 401）
        if !ok || err != nil {
            failed.ServeHTTP(w, req)
            return
        }

        // 3. 检查 audience 交集（防止 token 被用在错误的受众）
        if len(apiAuds) > 0 && !authenticator.Audiences(apiAuds).Intersect(resp.Audiences) {
            failed.ServeHTTP(w, req)
            return
        }

        // 4. 认证成功：清理认证头，注入用户信息
        CleanAuthHeaders(req)  // 移除 Authorization 和 X-Remote-User 等头部
        ctx := request.WithUser(req.Context(), resp.User)
        // 授权token被请求头中的认证信息替换掉，确保后续组件不能通过重放头部冒充用户
        handler.ServeHTTP(w, req.WithContext(ctx))
    })
}
```

**认证链**：`auth.AuthenticateRequest(req)` 实际上是一个**认证器联合**——尝试 x509 → bearer token → OIDC → webhook → ...，任何一个成功即返回。这个联合链中每个认证器都会检查特定的请求属性。

**安全设计**：认证成功后立即**清除认证相关的 HTTP 头部**（`Authorization`、`X-Remote-User`、`X-Remote-Group` 等）。防止了后续组件（如审计、授权）被伪造的认证头部欺骗。后续代码只能从 `request.Context()` 中获取用户信息。

`failed handler` 被一个专门的 `WithFailedAuthenticationAudit` 包装，用于记录认证失败的审计事件。

### 4.2 WithTracing — 链路追踪

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/filters/traces.go
func WithTracing(handler http.Handler, tp trace.TracerProvider) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        // 使用 otelhttp 包装 handler 以自动创建 span
        // span name 格式: "GET /api/v1/namespaces/{namespace}/pods/{name}"
        // 从 RequestInfo 解析参数化路径
    })
}
```

**为什么追踪在认证之后？** 注释说得很清楚：`WithTracing comes after authentication so we can allow authenticated clients to influence sampling.`——已认证用户可以设置特定头部来影响采样决策，这在调试特定用户请求时非常有用。

### 4.3 WithAudit — 审计

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/filters/audit.go
func WithAudit(handler http.Handler, sink audit.Sink, policy audit.PolicyRuleEvaluator, ...) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // 1. 评估审计策略 → 确定审计级别 (None/Metadata/Request/RequestResponse)
        attribs := buildAuditAttributes(r)
        level := policy.EvaluatePolicyRule(attribs)

        if level == audit.LevelNone {
            handler.ServeHTTP(w, r)
            return
        }

        // 2. 发送 RequestReceived 阶段事件
        auditContext := audit.GetAuditContext(r.Context())
        auditContext.Event = audit.EventFrom(r, level)
        // ...

        // 3. 包装 ResponseWriter 以拦截状态码和响应体
        ew := newAuditResponseWriter(w)

        // 4. 延迟处理：panic / 正常完成时发送 ResponseComplete 事件
        defer func() {
            if panicked := recover(); panicked != nil {
                auditContext.Event.ResponseStatus = &metav1.Status{Code: http.StatusInternalServerError}
                // ...
            }
            // 写入审计日志
            // 同时写入延迟信息（如果请求处理超过 500ms）
            latencies := request.AuditAnnotationsFromLatencyTrackers(r.Context())
            for k, v := range latencies {
                auditContext.Event.Annotations[k] = v
            }
            sink.ProcessEvents(auditContext.Event)
        }()

        handler.ServeHTTP(ew, r)

        // 对于长请求（Watch），在 WriteHeader 时发送 ResponseStarted 事件
    })
}
```

审计策略是一个三阶段事件模型：
- **RequestReceived**：请求到达时
- **ResponseStarted**：响应头开始发送时（仅用于长连接如 Watch）
- **ResponseComplete**：响应完成时

关键细节：审计结束后会读取 `LatencyTrackers` 中记录的各组件延迟，写入审计事件的 `Annotations` 字段。这就是为什么 `LatencyTrackers` 需要在审计之前初始化——它在调用栈中比审计更深，所以审计能读取到它的数据。

### 4.4 WithImpersonation — 用户模拟

用户模拟（Impersonation）允许一个已认证用户"变身"成另一个身份发送请求，主要用于：
- **管理员排障**：`kubectl --as=system:serviceaccount:ns:bot` 以目标 SA 身份复现权限问题
- **审计与安全**：审计员模拟只读用户操作，避免误写
- **扩展调试**：平台团队模拟租户身份验证 RBAC 配置

模拟者必须有 `impersonate` 资源的对应权限（如 `create users/impersonator`），审计日志会同时记录原始用户和模拟目标，保证可追溯。

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/filters/impersonation/impersonation.go
func WithImpersonation(handler http.Handler, a authorizer.Authorizer, s runtime.NegotiatedSerializer) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        // 1. 提取 Impersonate-* 请求头
        //    Impersonate-User: alice
        //    Impersonate-Group: devs
        //    Impersonate-Uid: xxx
        //    Impersonate-Extra-key: value
        headers := extractImpersonationHeaders(req)

        // 2. 对每个模拟目标执行授权：
        //    请求者必须有 impersonate 资源的权限
        //    authorize(verb="impersonate", resource="users", name="alice")
        for _, target := range headers {
            if err := a.Authorize(ctx, impAttr); err != nil { ... }
        }

        // 3. 替换用户信息
        ctx := request.WithUser(req.Context(), impersonatedUser)

        // 4. 清除模拟请求头（防止内部转发时重复模拟）
        cleanImpersonationHeaders(req)

        handler.ServeHTTP(w, req.WithContext(ctx))
    })
}
```

**重要限制**：只有具有 `impersonate` 权限的用户才能模拟其他用户。RBAC 中需要绑定 `ClusterRole` 包含 `impersonate` 资源的规则。

Kubernetes 1.28+ 通过 `ConstrainedImpersonation` feature gate 引入了**约束模拟**模式，提供了更细粒度的控制：可以限制 SA 只能模拟关联的节点用户，或只能模拟特定范围的用户。

### 4.5 WithPriorityAndFairness — 优先级与公平性 (APF)

这是 kube-apiserver **最复杂的过滤器**，也是 1.20+ 默认启用的限流机制。

```go
// staging/src/k8s.io/apiserver/pkg/server/filters/priority-and-fairness.go
func WithPriorityAndFairness(handler http.Handler, ..., fcIfc utilflowcontrol.Interface, 
                             workEstimator flowcontrolrequest.WorkEstimatorFunc, 
                             defaultRequestWaitLimit time.Duration) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // 1. 分类：将请求映射到 FlowSchema + PriorityLevel
        //    （通过 apiserver_flowcontrol 配置资源）
        fs, pl, isWatch := classifyRequest(r)

        // 2. 如果是 Watch 请求，使用专用 goroutine 模式
        if isWatch {
            // APF 的 Handle() 在另一 goroutine 中，
            // 只阻塞直到 Watch 初始化完成，不占用 seats
            go fcIfc.Handle(r, ...)
            handler.ServeHTTP(w, r)
            return
        }

        // 3. 对于普通请求：工作估算
        //    估算请求需要的 "seats"（并行额度）
        seats := workEstimator(r)

        // 4. 排队等待（可能阻塞）
        //    等待超时后返回 429 Too Many Requests
        //    成功获取 seats 后执行 handler
        fcIfc.Handle(r, func() { handler.ServeHTTP(w, r) }, ...)
    })
}
```

**APF 的核心设计思想**：
- **FlowSchema**：定义请求分类规则，如 `system-leader-election`、`kube-controller-manager`、`global-default`
- **PriorityLevel**：定义每个优先级的并发配额，如 `leader-election` 有 10% 的并发，`workload-high` 有 40%
- **Seats**：一个抽象的并行执行单元。简单请求用 1 seat，复杂请求用更多
- **排队机制**：每个优先级内部使用公平排队，避免单个流淹没其他流

APF 的行为通过 `FlowSchema` 和 `PriorityLevel` 两个 API 资源动态配置：

```yaml
apiVersion: flowcontrol.apiserver.k8s.io/v1beta3
kind: FlowSchema
spec:
  priorityLevelConfiguration:
    name: workload-high
  rules:
  - subjects:
    - kind: ServiceAccounts
      serviceAccount:
        name: "*"
        namespace: kube-system
```

当 APF 未启用（`--enable-priority-and-fairness=false`）时，回退到传统的 `WithMaxInFlightLimit`：

```go
// staging/src/k8s.io/apiserver/pkg/server/filters/maxinflight.go
func WithMaxInFlightLimit(handler http.Handler, 
    nonMutatingLimit int, mutatingLimit int, ...) http.Handler {
    // 使用两个带缓冲的 channel 控制并发量：
    //   nonMutatingLimit → 只读请求（GET/LIST）
    //   mutatingLimit    → 写请求（POST/PUT/DELETE/PATCH）
    // channel 满时返回 429
    // system:masters 组和长请求不受限制
}
```

### 4.6 WithAuthorization — 授权

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go
func WithAuthorization(handler http.Handler, auth authorizer.Authorizer, s runtime.NegotiatedSerializer) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // 1. 从 RequestInfo 和 User 构建授权属性
        user, _ := request.UserFrom(r.Context())
        requestInfo, _ := request.RequestInfoFrom(r.Context())

        attribs := authorizer.AttributesRecord{
            User:            user,
            Verb:            requestInfo.Verb,
            Namespace:       requestInfo.Namespace,
            APIGroup:        requestInfo.APIGroup,
            Resource:        requestInfo.Resource,
            Subresource:     requestInfo.Subresource,
            Name:            requestInfo.Name,
            ResourceRequest: requestInfo.IsResourceRequest,
        }

        // 如果启用了 AuthorizeWithSelectors，还要解析 field/label selector
        // 以支持更细粒度的字段级授权

        // 2. 执行授权
        decision, reason, err := auth.Authorize(ctx, attribs)

        // 注意顺序：先检查 decision，再检查 err
        // "RBAC 可能遇到评估错误但仍然允许请求"
        if decision == authorizer.DecisionAllow {
            // 添加审计注解
            audit.AddAuditAnnotation(ctx, "authorization.k8s.io/decision", "allow")
            handler.ServeHTTP(w, r)
            return
        }

        // 3. 拒绝
        if err != nil {
            // 500 Internal Error
        } else {
            // 403 Forbidden
        }
    })
}
```

**授权链**也使用了联合（union）模式：RBAC、Node、Webhook、ABAC 等授权器按顺序尝试，任一授权器返回 Allow 则放行，全部返回 Deny 则拒绝。

有一个值得注意的细节：**先检查 `decision` 再检查 `err`**。这意味着即使 RBAC 评估过程中遇到了错误（比如无法连接到外部 webhook），但如果其他授权器已经允许了请求，请求仍会通过。这保证了可用性（availability）优先于严格性（strictness）。

---

## 5. 可观测性：filterlatency 延迟追踪系统

### 5.1 延迟测量模式

`filterlatency.TrackStarted` 和 `TrackCompleted` 实现了对每个关键过滤器的精确延迟测量：

```go
// 使用模式
handler = filterlatency.TrackCompleted(handler)     // ← 前一个过滤器完成
handler = genericapifilters.WithAuthorization(handler, ...)   // 当前过滤器
handler = filterlatency.TrackStarted(handler, tp, "authorization")  // ← 当前过滤器开始
```

**执行流程的时间线**：

```
时间 →
│
├─ TrackCompleted（前一个过滤器）← 记录前一个结束的时间
│
├─ WithAuthorization（当前过滤器）
│   ├─ auth.Authorize()  ← 授权逻辑执行
│   └─ handler.ServeHTTP ← 调用下一个
│
├─ TrackStarted（当前过滤器）← 记录当前开始的时间
│
└─ ...后续过滤器...
```

最终，当请求返回时，匹配的 `TrackCompleted`/`TrackStarted` 会计算出延迟并发布到 Prometheus `apiserver_filter_latency_seconds` 指标。

### 5.2 LatencyTrackers — 细粒度组件延迟

除了 filter 级别的延迟，Kubernetes 还记录**组件内部**的细粒度延迟。`request.LatencyTrackers` 结构体包含了 11 个 `DurationTracker`：

| Tracker | 聚合方式 | 记录内容 |
|---------|---------|---------|
| AuthenticationTracker | Sum | 认证耗时 |
| AuthorizationTracker | Max | 授权耗时 |
| ImpersonationTracker | Sum | 模拟请求解析耗时 |
| APFQueueWaitTracker | Max | APF 排队等待时间 |
| StorageTracker | Sum | etcd 操作耗时 |
| TransformTracker | Sum | 对象转换耗时 |
| SerializationTracker | Sum | 序列化耗时 |
| ResponseWriteTracker | Sum | 响应写出耗时 |
| DecodeTracker | Sum | etcd 响应解码耗时 |
| MutatingWebhookTracker | Sum | 变更 Webhook 耗时 |
| ValidatingWebhookTracker | Max | 校验 Webhook 耗时 |

当总请求延迟超过 500ms 时，这些数值被写入审计事件的注解中，帮助管理员排查性能瓶颈。

---

## 6. 路由分发：APIServerHandler 与 Director

过滤器链的末端是 `apiHandler`，它在 `APIServerHandler` 结构体中进行最终的路由分发。

`staging/src/k8s.io/apiserver/pkg/server/handler.go` 定义了 `APIServerHandler`：

```go
type APIServerHandler struct {
    FullHandlerChain   http.Handler          // 完整的过滤器链最终 handler
    GoRestfulContainer *restful.Container     // go-restful 路由（用于 API 端点）
    NonGoRestfulMux    *mux.PathRecorderMux   // 非 RESTful 路由（/healthz, /metrics, /version 等）
    Director           http.Handler           // 分发器
}
```

**构造过程**（位于 `NewAPIServerHandler`）：

```go
func NewAPIServerHandler(name string, s runtime.NegotiatedSerializer,
    handlerChainBuilder HandlerChainBuilderFn, notFoundHandler http.Handler) *APIServerHandler {

    nonGoRestfulMux := mux.NewPathRecorderMux(name)
    gorestfulContainer := restful.NewContainer()
    gorestfulContainer.Router(restful.CurlyRouter{})

    director := director{
        goRestfulContainer: gorestfulContainer,
        nonGoRestfulMux:    nonGoRestfulMux,
    }

    return &APIServerHandler{
        FullHandlerChain:   handlerChainBuilder(director),  // 在 director 外包裹所有过滤器
        GoRestfulContainer: gorestfulContainer,
        NonGoRestfulMux:    nonGoRestfulMux,
        Director:           director,
    }
}
```

关键路径：`handlerChainBuilder(director)` — 将所有过滤器包装在 `director` 外层。所以请求流程是：

```
过滤器链（认证/授权/审计/...）
  → director.ServeHTTP()       ← 决定路由
    → go-restful Container     ← API 端点（/api/v1/pods 等）
      或
    → NonGoRestfulMux          ← 非 API 端点（/healthz, /metrics 等）
```

**Director 的分发逻辑**：

```go
func (d director) ServeHTTP(w http.ResponseWriter, req *http.Request) {
    path := req.URL.Path

    for _, ws := range d.goRestfulContainer.RegisteredWebServices() {
        switch {
        case ws.RootPath() == "/apis":
            // /apis 和 /apis/ 有特殊处理
            if path == "/apis" || path == "/apis/" {
                d.goRestfulContainer.Dispatch(w, req)
                return
            }
        case strings.HasPrefix(path, ws.RootPath()):
            // 路径匹配 go-restful WebService → 交给 go-restful 处理
            // 例如 /api/v1/namespaces/default/pods
            if len(path) == len(ws.RootPath()) || path[len(ws.RootPath())] == '/' {
                d.goRestfulContainer.Dispatch(w, req)
                return
            }
        }
    }

    // 不匹配任何 WebService → 交给 NonGoRestfulMux 处理
    d.nonGoRestfulMux.ServeHTTP(w, req)
}
```

`NonGoRestfulMux` 处理的典型端点包括：

| 路径 | 用途 |
|------|------|
| `/healthz`, `/livez`, `/readyz` | 健康检查 |
| `/metrics` | Prometheus 指标 |
| `/version` | 版本信息 |
| `/debug/pprof/` | 性能分析 |
| `/openapi/v2`, `/openapi/v3` | OpenAPI 规范 |
| `/api`, `/apis` | API 发现（旧版，新版走 go-restful） |

**为什么不全部用 go-restful？** 代码注释中有一段精彩的解释（`handler.go:53-66`）：go-restful 要求为每个路径注册精确的 WebService，并且 `/apis` 作为根路径会劫持所有 `/apis/*` 的请求，导致需要 fallthrough 到非 API handler 的路径被 404 拦截。因此 Director 在 go-restful 前面做了一层轻量级路由，只把可能的 API 请求转发给 go-restful，其他路径直接交给 NonGoRestfulMux。

Kubernetes 社区已有计划**彻底移除 go-restful**，转向使用标准的 `net/http` mux。

---

## 7. 优雅关闭的过滤器协同

优雅关闭是一个多过滤器**协同工作**的典型案例，涉及三个过滤器：

### 7.1 RetryAfter — 关闭信号通知

```go
// staging/src/k8s.io/apiserver/pkg/server/filters/with_retry_after.go
func WithRetryAfter(handler http.Handler, shutdownCh <-chan struct{}) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        select {
        case <-shutdownCh:
            // 关闭后，新请求返回 429 Retry-After
            // 但健康检查和 apiserver 内部请求（loopback client）放行
            if isRequestExempt(r) {
                handler.ServeHTTP(w, r)
                return
            }
            http.Error(w, "Shutdown in progress", http.StatusTooManyRequests)
        default:
            handler.ServeHTTP(w, r)
        }
    })
}
```

### 7.2 WaitGroup — 追踪正在处理的请求

```go
// staging/src/k8s.io/apiserver/pkg/server/filters/waitgroup.go
func WithWaitGroup(handler http.Handler, longRunning apirequest.LongRunningRequestCheck, 
                   wg RequestWaitGroup) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if longRunning(r) {
            handler.ServeHTTP(w, r)  // 长请求不追踪
            return
        }
        if wg.Add(1) != nil {  // 如果 Add 失败（关闭已启动），...
            http.Error(w, "Shutdown in progress", http.StatusTooManyRequests)
            return
        }
        defer wg.Done()
        handler.ServeHTTP(w, r)
    })
}
```

### 7.3 WatchTerminationDuringShutdown — Watch 连接的关闭

```go
// staging/src/k8s.io/apiserver/pkg/server/filters/watch_termination.go
func WithWatchTerminationDuringShutdown(handler http.Handler, ...) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if !isWatchRequest(r) {
            handler.ServeHTTP(w, r)  // 非 Watch 请求不处理
            return
        }
        // Watch 请求加入等待组
        if wg.Add(1) != nil {
            http.Error(w, "Shutdown in progress", 429)
            return
        }
        // 在 context 中注入关闭信号，Watch handler 检测到后主动断开
        ctx := request.WithServerShutdownSignal(r.Context(), shutdownCh)
        handler.ServeHTTP(w, r.WithContext(ctx))
    })
}
```

**关闭协同流程**：

```
SIGTERM 到达
  │
  ├─ ShutdownInitiated     → /readyz 返回失败
  ├─ ShutdownDelayDuration  → 等待 LB 摘除
  │
  ├─ AfterShutdownDelayDuration
  │   ├─ PreShutdownHooks 执行
  │   └─ NotAcceptingNewRequest 信号发出
  │       ├─ RetryAfter 开始拒绝新请求
  │       ├─ WaitGroup 的 Add 失败 → 也返回 429
  │       │
  │       ├─ NonLongRunningRequestWaitGroup.Wait()  → 等非长请求完成
  │       └─ WatchRequestWaitGroup.Wait()            → 等 Watch 断开
  │           │
  │           └─ HTTPServer.Serve 停止
```

---

## 8. 特殊实现技巧

### 8.1 ResponseWriter 包装链

多个过滤器（Audit、Timeout、LatencyTrackers）需要拦截 HTTP 响应。它们会包装 `ResponseWriter`：

```go
// 用于从 ResponseWriter 上读取状态码和响应体
type auditResponseWriter struct {
    http.ResponseWriter
    statusCode int
    // ...
}

// 用于在超时后保护 ResponseWriter（防止并发写入）
type timeoutWriter struct {
    http.ResponseWriter
    mu sync.Mutex
    // ...
}
```

由于 Go 的 `ResponseWriter` 可能还实现了 `http.Hijacker`、`http.Flusher`、`http.CloseNotifier`、`http.Pusher` 等接口，Kubernetes 使用 `responsewriter.WrapForHTTP1Or2` 函数确保包装后的对象仍然暴露这些接口，以兼容 WebSocket 和 SPDY 协议。

### 8.2 请求体延迟反序列化

一个重要的安全设计：**请求体在整个过滤器链中不会被反序列化**。直到请求通过了认证、授权、限流等所有检查，到达真正的 REST handler（如 `restfulCreateResource`）时，才解析 body。

这意味着：
- 认证层只能看到 HTTP headers（Token、证书等）
- 授权层只能看到 RequestInfo（URL、Verb 等）
- 限流层只能看到请求方法和路径

**恶意 payload 不会在早期被解析**，减少了攻击面。

### 8.3 长请求特殊处理

多个过滤器中都有 `longRunningRequestCheck` 检查。Kubernetes 定义的长请求包括：

```go
func BasicLongRunningRequestCheck(longRunningVerbs, longRunningSubresources sets.String) LongRunningRequestCheck {
    return func(r *http.Request) bool {
        requestInfo, _ := request.RequestInfoFrom(r.Context())
        if requestInfo == nil { return false }
        // watch / proxy / redirect 等 verb
        if longRunningVerbs.Has(requestInfo.Verb) { return true }
        // proxy / portforward / exec / attach / log 等 subresource
        if longRunningSubresources.Has(requestInfo.Subresource) { return true }
        return false
    }
}
```

长请求会跳过：
- `WithTimeoutForNonLongRunningRequests` — 不超时
- `WithRequestDeadline` — 不设 deadline
- `WithMaxInFlightLimit` — 不占并发配额
- `WithWaitGroup` — 不加入关闭等待组
- APF 中 Watch 请求有专门的处理路径

---

**下一篇预告**：我们将深入**认证机制（Authentication）**，解析 x509 客户端证书、Bearer Token、OIDC、Webhook 和服务账号令牌等多种认证方式的实现。
