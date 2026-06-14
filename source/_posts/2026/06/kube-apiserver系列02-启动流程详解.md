---
title: kube-apiserver 源码解读（二）启动流程详解
date: 2026-06-14 10:00:00
tags:
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 引言

上一篇我们从宏观视角分析了 kube-apiserver 的三层委托链架构和核心能力。本篇深入到**启动流程**——从 `main()` 函数开始，追踪一个 kube-apiserver 进程从命令行参数解析到 HTTPS 服务就绪的完整路径。

启动流程的核心代码集中在以下几个文件中：

<!--more-->

| 文件 | 作用 |
|------|------|
| `cmd/kube-apiserver/apiserver.go` | `main()` 入口，仅 36 行 |
| `cmd/kube-apiserver/app/server.go` | Command 创建、`Run()`、`CreateServerChain()` |
| `cmd/kube-apiserver/app/config.go` | 配置组装：`NewConfig()` + `Complete()` |
| `cmd/kube-apiserver/app/options/options.go` | `ServerRunOptions` 结构定义与默认值 |
| `pkg/controlplane/apiserver/config.go` | `BuildGenericConfig()` — 生成共享的 `genericConfig` |
| `staging/src/k8s.io/apiserver/pkg/server/config.go` | `GenericAPIServer` 的 `New()`、`DefaultBuildHandlerChain()` |
| `staging/src/k8s.io/apiserver/pkg/server/genericapiserver.go` | `PrepareRun()`、`RunWithContext()` |

完整的调用链如下：

```
main()
  │
  └─ NewAPIServerCommand()              创建 cobra.Command
       │
       └─ RunE (cobra 回调)
            ├─ s.Complete(ctx)          补全选项
            ├─ completedOptions.Validate()   验证
            └─ Run(ctx, completedOptions)
                 │
                 ├─ NewConfig(opts)          构建 Config（含三个子 config）
                 ├─ config.Complete()        补全配置
                 ├─ CreateServerChain()      创建三层 Server
                 ├─ server.PrepareRun()      安装 OpenAPI、健康检查
                 └─ prepared.Run(ctx)        启动 HTTPS 监听 + PostStartHooks
```

---

## 2. 入口：`main()` 函数

`cmd/kube-apiserver/apiserver.go:32`：

```go
func main() {
    command := app.NewAPIServerCommand()
    code := cli.Run(command)
    os.Exit(code)
}
```

只有三行：

1. **`app.NewAPIServerCommand()`** — 创建 Cobra Command，绑定所有命令行参数
2. **`cli.Run(command)`** — 来自 `k8s.io/component-base/cli`，负责启动流程，包括信号处理（SIGTERM/SIGINT）、日志初始化、调用 `RunE`
3. **`os.Exit(code)`** — 退出

`cli.Run` 并不是简单的 `cmd.Execute()`，它在底层做了几件额外的事情：
- 调用 `PersistentPreRunE` 初始化全局组件注册表
- 捕获 `os.Signal` 并通过 `context.Context` 传递
- 统一处理错误输出和退出码

---

## 3. Command 构建：`NewAPIServerCommand()`

`cmd/kube-apiserver/app/server.go:70`：

```go
func NewAPIServerCommand() *cobra.Command {
    s := options.NewServerRunOptions()
    ctx := genericapiserver.SetupSignalContext()
    // ...
    cmd := &cobra.Command{
        Use: "kube-apiserver",
        RunE: func(cmd *cobra.Command, args []string) error {
            verflag.PrintAndExitIfRequested()
            // 激活日志
            logsapi.ValidateAndApply(s.Logs, featureGate)
            // 补全选项
            completedOptions, err := s.Complete(ctx)
            // 验证
            if errs := completedOptions.Validate(); len(errs) != 0 { ... }
            // 启动
            return Run(ctx, completedOptions)
        },
    }
    // 绑定 flags
    namedFlagSets := s.Flags()
    for _, f := range namedFlagSets.FlagSets {
        fs.AddFlagSet(f)
    }
    return cmd
}
```

关键步骤：

### 3.1 `options.NewServerRunOptions()` — 选项的默认值

`cmd/kube-apiserver/app/options/options.go:66`：

```go
func NewServerRunOptions() *ServerRunOptions {
    return &ServerRunOptions{
        Options: *controlplaneapiserver.NewOptions(),
        Extra: Extra{
            EndpointReconcilerType: string(reconcilers.LeaseEndpointReconcilerType),
            KubeletConfig:          kubeletclient.KubeletClientConfig{...},
            ServiceNodePortRange:   kubeoptions.DefaultServiceNodePortRange,
            MasterCount:            1,
        },
    }
}
```

内部 `controlplaneapiserver.NewOptions()`（`pkg/controlplane/apiserver/options/options.go:112`）创建了一整套子选项对象：

```go
func NewOptions() *Options {
    return &Options{
        GenericServerRunOptions: genericoptions.NewServerRunOptions(),
        Etcd:                    genericoptions.NewEtcdOptions(),
        SecureServing:           kubeoptions.NewSecureServingOptions(),
        Audit:                   genericoptions.NewAuditOptions(),
        Features:                genericoptions.NewFeatureOptions(),
        Admission:               kubeoptions.NewAdmissionOptions(),
        Authentication:          kubeoptions.NewBuiltInAuthenticationOptions().WithAll(),
        Authorization:           kubeoptions.NewBuiltInAuthorizationOptions(),
        APIEnablement:           genericoptions.NewAPIEnablementOptions(),
        EgressSelector:          genericoptions.NewEgressSelectorOptions(),
        Metrics:                 genericoptions.NewMetricsOptions(),
        Logs:                    genericoptions.NewLogsOptions(),
        Traces:                  genericoptions.NewTracingOptions(),
    }
}
```

每个子选项都有对应的 `ApplyTo()` 方法，后续会将值写入 `genericapiserver.Config`。

### 3.2 `s.Complete(ctx)` — 选项补全

`pkg/controlplane/apiserver/options/options.go:221`：

```go
func (s *Options) Complete(ctx context.Context) (CompletedOptions, error) {
    // 默认监听地址
    if err := s.DefaultAdvertiseAddress(ctx, s.SecureServing); err != nil { ... }
    // 自动生成自签名证书（未指定时）
    if err := s.SecureServing.MaybeDefaultWithSelfSignedCerts(...); err != nil { ... }
    // ServiceAccount 选项补全
    s.completeServiceAccountOptions()
    // RuntimeConfig key 规范化
    s.APIEnablement.RuntimeConfig = normalizeRuntimeConfigKeys(...)
    // 递归 Complete 所有子选项
}
```

两项关键操作：
- `DefaultAdvertiseAddress`：如果未指定 `--advertise-address`，自动推断本机 IP
- `MaybeDefaultWithSelfSignedCerts`：开发环境未指定证书时，生成自签名证书

### 3.3 `completedOptions.Validate()` — 验证

`cmd/kube-apiserver/app/options/validation.go:131`：

```go
func (s CompletedOptions) Validate() []error {
    errs := append(s.CompletedOptions.Validate(), ...)
    errs = append(errs, validateClusterIPFlags(s.Extra)...)
    errs = append(errs, validateServiceNodePort(s.Extra)...)
    errs = append(errs, validatePublicIPServiceClusterIPRangeIPFamilies(s.Extra)...)
    // ...
}
```

主要验证项：
- Service ClusterIP CIDR：至少一个，最多两个，主 CIDR 不能小于 `/20`
- Service NodePort 范围：合法端口
- IP 族匹配：Service IP 必须与 advertise address 在同一 IP 族

---

## 4. Config 构建：`NewConfig()`

验证通过后，`Run()` 函数首先调用 `NewConfig(opts)`。这一步**组装三个子服务器各自的配置对象**。

`cmd/kube-apiserver/app/config.go:74`：

```go
func NewConfig(opts options.CompletedOptions) (*Config, error) {
    c := &Config{Options: opts}

    // 1. 构建共享的 genericConfig
    genericConfig, versionedInformers, storageFactory, err :=
        controlplaneapiserver.BuildGenericConfig(opts.CompletedOptions, ...)

    // 2. 构建 KubeAPIs Config
    kubeAPIs, serviceResolver, pluginInitializer, err :=
        CreateKubeAPIServerConfig(opts, genericConfig, versionedInformers, storageFactory)
    c.KubeAPIs = kubeAPIs

    // 3. 构建 APIExtensions Config — 从 KubeAPIs 的 GenericConfig 派生
    apiExtensions, err := controlplaneapiserver.CreateAPIExtensionsConfig(
        *kubeAPIs.ControlPlane.Generic, ...)
    c.ApiExtensions = apiExtensions

    // 4. 构建 Aggregator Config — 同样从 KubeAPIs 的 GenericConfig 派生
    aggregator, err := controlplaneapiserver.CreateAggregatorConfig(
        *kubeAPIs.ControlPlane.Generic, ...)
    c.Aggregator = aggregator

    return c, nil
}
```

### 4.1 `BuildGenericConfig()` — 共享配置工厂

`pkg/controlplane/apiserver/config.go:116` 是启动流程中**最长的函数**（约 130 行），将所有选项应用到 `genericapiserver.Config`：

```
BuildGenericConfig()
  │
  ├─ genericapiserver.NewConfig(codecs)        创建默认 Config
  ├─ s.GenericServerRunOptions.ApplyTo()       应用通用选项
  ├─ s.SecureServing.ApplyToConfig()           安全配置（TLS）
  ├─ 创建 clientgo 客户端（loopback）            自通信 client
  ├─ s.Features.ApplyTo()                      特性开关、APF
  ├─ s.APIEnablement.ApplyTo()                 API 启停
  ├─ s.EgressSelector.ApplyTo()                出站流量选择器
  ├─ s.Traces.ApplyTo()                        链路追踪
  ├─ 配置 OpenAPI V2/V3（标题: "Kubernetes"）
  ├─ 配置 LongRunningFunc（watch/proxy/attach/exec/log/portforward）
  ├─ 构建 StorageFactory                       etcd 存储工厂
  ├─ s.Etcd.ApplyWithStorageFactoryTo()        应用 etcd 配置
  ├─ s.Authentication.ApplyTo()                认证配置（X509/Token/Webhook...）
  ├─ BuildAuthorizer()                         授权配置
  └─ s.Audit.ApplyTo()                         审计配置
```

这里的 **loopback client** 是一个特例：kube-apiserver 内部会通过 HTTP 与自身通信（例如控制器通过 API 查询资源）。代码中将 ContentType 硬编码为 protobuf，并禁用压缩：

```go
genericConfig.LoopbackClientConfig.ContentConfig.ContentType = "application/vnd.kubernetes.protobuf"
genericConfig.LoopbackClientConfig.DisableCompression = true
```

### 4.2 子 Config 的构建策略

三个子服务器共享同一个 `genericConfig`（通过**浅拷贝 + 选择性覆盖**）：

| 服务器 | GenericConfig 来源 | 独有配置 |
|--------|-------------------|----------|
| KubeAPIs | BuildGenericConfig 的原始对象 | REST Storage Providers、API 安装、bootstrap 控制器 |
| APIExtensions | 浅拷贝 kubeAPIs.Generic，清空 PostStartHooks/RESTOptionsGetter | CRD 专用 codec、自己的 ResourceConfig |
| Aggregator | 浅拷贝 kubeAPIs.Generic，清空 PostStartHooks/RESTOptionsGetter | APIService 代理、proxy client cert、auto-registration 控制器 |

**为什么用浅拷贝？** 认证、授权、审计等配置在三个服务器之间完全共享，浅拷贝确保对同一个字段的修改对所有服务器生效。

### 4.3 `Complete()` — Config 补全

`Config.Complete()`（`cmd/kube-apiserver/app/config.go:61`）非常简单——递归调用三个子 config 的 `Complete()`：

```go
func (c *Config) Complete() (CompletedConfig, error) {
    return CompletedConfig{&completedConfig{
        Aggregator:    c.Aggregator.Complete(),
        KubeAPIs:      c.KubeAPIs.Complete(),
        ApiExtensions: c.ApiExtensions.Complete(),
    }}, nil
}
```

`CompletedConfig` 结构体中嵌入了一个**私有指针** `*completedConfig`，这确保了外部代码无法绕过 `Complete()` 直接构造 `CompletedConfig`：

```go
type CompletedConfig struct {
    *completedConfig    // 私有，外部无法实例化
}
```

这是 Kubernetes 源码中广泛使用的 **"Config / CompletedConfig" 模式**——未补全的和已补全的配置用不同的类型表示，编译器层面防止误用。

---

## 5. `CreateServerChain()` — 构建三层委托链

上一篇已经介绍了三层委托链的整体架构，这里聚焦**代码中的构造细节**。

`cmd/kube-apiserver/app/server.go:176`：

```go
func CreateServerChain(config CompletedConfig) (*aggregatorapiserver.APIAggregator, error) {
    // 创建 404 handler（最底层兜底）
    notFoundHandler := notfoundhandler.New(...)

    // 1. 创建 APIExtensions Server（最内层）
    apiExtensionsServer, err := config.ApiExtensions.New(
        genericapiserver.NewEmptyDelegateWithCustomHandler(notFoundHandler),
    )

    // 2. 创建 KubeAPIServer（中间层）
    kubeAPIServer, err := config.KubeAPIs.New(
        apiExtensionsServer.GenericAPIServer,
    )

    // 3. 创建 Aggregator Server（最外层）
    aggregatorServer, err := controlplaneapiserver.CreateAggregatorServer(
        config.Aggregator,
        kubeAPIServer.ControlPlane.GenericAPIServer,
        apiExtensionsServer.Informers.Apiextensions().V1().CustomResourceDefinitions(),
        crdAPIEnabled,
        apiVersionPriorities,
    )

    return aggregatorServer, nil
}
```

**构造顺序与请求处理顺序相反**：从最内层（APIExtensions）开始构建，逐层向外包裹。

### 5.1 KubeAPIServer 的构造细节

KubeAPIServer 的 `New()` 内部（`pkg/controlplane/apiserver/server.go:92` 和 `pkg/controlplane/instance.go:317`）做了大量工作：

```go
func (c CompletedConfig) New(delegationTarget genericapiserver.DelegationTarget) (*Instance, error) {
    // 1. 先创建 ControlPlane（GenericAPIServer）
    cp, _ := c.ControlPlane.New("kube-apiserver", delegationTarget)

    // 2. 创建所有 REST Storage Provider
    restStorageProviders, _ := c.StorageProviders(client)
    //    ├── corerest (pods, services, configmaps, secrets...)
    //    ├── apps (deployments, statefulsets...)
    //    ├── authorization
    //    ├── autoscaling
    //    ├── batch (jobs, cronjobs)
    //    ├── certificates
    //    ├── coordination (leases)
    //    ├── discovery
    //    ├── networking (ingress, networkpolicies)
    //    ├── node
    //    ├── policy
    //    ├── rbac
    //    ├── scheduling
    //    ├── storage (csidrivers, csinodes)
    //    ├── flowcontrol
    //    ├── apps
    //    └── ...

    // 3. 安装所有 API Group
    s.ControlPlane.InstallAPIs(restStorageProviders...)

    // 4. 注册 PostStartHook: bootstrap-controller
    s.ControlPlane.GenericAPIServer.AddPostStartHookOrDie("bootstrap-controller", ...)

    return s, nil
}
```

`StorageProviders()`（`pkg/controlplane/instance.go:387`）返回了**所有内置 API 组的 REST 存储提供者**。每个 Provider 都会在 `InstallAPIs()` 阶段注册到路由中。

---

## 6. `GenericAPIServer.New()` — 通用流程

每个服务器的 `New()` 最终都落到 `staging/src/k8s.io/apiserver/pkg/server/config.go:787` 的 `completedConfig.New()`。这是最核心的通用构造函数：

```go
func (c completedConfig) New(name string, delegationTarget DelegationTarget) (*GenericAPIServer, error) {
    // 1. 构建 HandlerChain（包裹 filter）
    handlerChainBuilder := func(handler http.Handler) http.Handler {
        return c.BuildHandlerChainFunc(handler, c.Config)
    }

    // 2. 创建 APIServerHandler（路由分发器）
    apiServerHandler := NewAPIServerHandler(name, c.Serializer,
        handlerChainBuilder, delegationTarget.UnprotectedHandler())

    // 3. 构造 GenericAPIServer 结构体
    s := &GenericAPIServer{
        Handler:          apiServerHandler,
        SecureServingInfo: c.SecureServing,
        // ... 数十个字段
    }

    // 4. 合并 DelegationTarget 的 PostStartHooks、PreShutdownHooks
    for k, v := range delegationTarget.PostStartHooks() {
        s.postStartHooks[k] = v
    }

    // 5. 注册通用 PostStartHooks
    s.AddPostStartHook("generic-apiserver-start-informers", ...)
    s.AddPostStartHook("priority-and-fairness-config-consumer", ...)
    s.AddPostStartHook("priority-and-fairness-filter", ...)
    s.AddPostStartHook("storage-object-count-tracker-hook", ...)

    // 6. 安装通用端点
    installAPI(name, s, c.Config)
    //    ├── /index          (资源列表)
    //    ├── /debug/pprof    (性能分析)
    //    ├── /metrics        (Prometheus 指标)
    //    ├── /version        (版本信息)
    //    ├── /api, /apis     (Discovery)
    //    └── /debug/flags    (动态 Flag)

    return s, nil
}
```

### HandlerChain 的构建

`c.BuildHandlerChainFunc` 默认值是 `DefaultBuildHandlerChain`（`staging/src/k8s.io/apiserver/pkg/server/config.go:1036`），它按照**从内到外**的顺序包装 HTTP handler。但由于 HTTP handler 是最外层先执行，所以请求的实际处理顺序是**从外到内**：

```
WithPanicRecovery (最外层，最先执行)
  WithAuditInit
    WithMuxAndDiscoveryComplete
      WithRequestReceivedTimestamp
        WithRequestInfo
          WithRoutine (APIServingWithRoutine feature)
            WithLatencyTrackers
              WithHTTPLogging
                WithRetryAfter (shutdown 时)
                  WithHSTS
                    WithCacheControl
                      WithProbabilisticGoaway
                        WithWatchTerminationDuringShutdown
                          WithWaitGroup
                            WithRequestDeadline
                              WithTimeoutForNonLongRunningRequests
                                WithWarningRecorder
                                  WithCORS
                                    WithAuthentication    ← 认证
                                      WithTracing
                                        WithAudit         ← 审计
                                          WithImpersonation / WithConstrainedImpersonation
                                            WithPriorityAndFairness / WithMaxInFlightLimit  ← 限流
                                              WithAuthorization  ← 授权
                                                TrackStarted (authorization)
                                                  TrackCompleted (最内层, 实际 API Handler)
```

这与上一篇中的简化版本一致，但本篇列出了更完整的过滤器列表。

---

## 7. `PrepareRun()` — 就绪准备

`GenericAPIServer.PrepareRun()`（`staging/src/k8s.io/apiserver/pkg/server/genericapiserver.go:444`）在服务器启动前做最后的准备工作，按顺序执行：

```go
func (s *GenericAPIServer) PrepareRun() preparedGenericAPIServer {
    // 1. 递归调用委托链的 PrepareRun（从最内层到最外层）
    s.delegationTarget.PrepareRun()

    // 2. 安装 OpenAPI V2 & V3 文档
    s.OpenAPIVersionedService, s.StaticOpenAPISpec = routes.OpenAPI{
        Config: s.openAPIConfig,
    }.InstallV2(s.Handler.GoRestfulContainer, s.Handler.NonGoRestfulMux)
    s.OpenAPIV3VersionedService = routes.OpenAPI{
        V3Config: s.openAPIV3Config,
    }.InstallV3(s.Handler.GoRestfulContainer, s.Handler.NonGoRestfulMux)

    // 3. 安装健康检查端点
    s.installHealthz()      // /healthz
    s.installLivez()        // /livez
    s.installReadyz()       // /readyz (含 shutdown 检查)

    // 4. 安装调试端点
    flagz.Install(s.Handler.NonGoRestfulMux, ...)     // /debug/flags
    statusz.Install(s.Handler.NonGoRestfulMux, ...)   // /debug/statusz

    return preparedGenericAPIServer{s}
}
```

返回的 `preparedGenericAPIServer` 包装了 `*GenericAPIServer`，对外暴露 `Run(ctx)` 和 `RunWithContext(ctx)` 方法。

---

## 8. `Run()` — 启动 HTTPS 服务

`preparedGenericAPIServer.RunWithContext()`（`staging/src/k8s.io/apiserver/pkg/server/genericapiserver.go:536`）是服务器的**主循环**。它的核心流程：

### 8.1 NonBlockingRun — 异步启动

```go
func (s preparedGenericAPIServer) NonBlockingRunWithContext(ctx context.Context, shutdownTimeout time.Duration) {
    // 1. 启动 HTTPS 监听
    s.SecureServingInfo.Serve(s.Handler, shutdownTimeout, internalStopCh)
    //    ├── net.Listen("tcp", ":6443")
    //    ├── tls.NewListener(listener, tlsConfig)
    //    └── http.Server.Serve(tlsListener)

    // 2. 运行所有 PostStartHooks（异步）
    s.RunPostStartHooks(ctx)

    // 3. 通知 systemd 服务已就绪
    systemd.SdNotify(true, "READY=1\n")
}
```

### 8.2 PostStartHooks — 启动后钩子

PostStartHooks 是一组在 HTTPS 监听启动后运行的异步任务。按注册顺序启动：

| Hook 名称 | 作用 |
|-----------|------|
| `generic-apiserver-start-informers` | 启动 SharedInformerFactory |
| `priority-and-fairness-config-consumer` | 启动 FlowControl 配置消费 |
| `priority-and-fairness-filter` | 启动 APF 水位线维护 |
| `storage-object-count-tracker-hook` | 启动对象计数追踪 |
| `kube-apiserver-autoregistration` | CRD & APIService 自动注册 |
| `bootstrap-controller` | Kubernetes Service Endpoints 协调 |
| `start-kubernetes-service-cidr-controller` | Service CIDR 控制器（MultiCIDR 特性） |
| ... | identity lease、system namespaces 等 |

### 8.3 优雅关闭（Graceful Shutdown）

`RunWithContext` 的核心逻辑除了启动，还有一套**完整的优雅关闭机制**。关闭流程的同步关系如下：

```
ctx 取消（SIGTERM/SIGINT）
  │
  ├─ Signal: ShutdownInitiated          → /readyz 开始返回失败
  │
  ├─ Sleep: ShutdownDelayDuration       → 给负载均衡器时间摘除本节点
  │
  ├─ Signal: AfterShutdownDelayDuration
  │     │
  │     ├─ Run PreShutdownHooks         → 清理资源（如 Endpoint 摘除）
  │     │
  │     └─ Wait PreShutdownHooksStopped
  │           │
  │           └─ Signal: NotAcceptingNewRequest  → 停止接受新请求
  │                 │
  │                 ├─ NonLongRunningRequestWaitGroup.Wait()   → 等待短请求完成
  │                 ├─ WatchRequestWaitGroup.Wait()            → 等待 Watch 断开
  │                 │
  │                 └─ Signal: InFlightRequestsDrained
  │                       │
  │                       ├─ AuditBackend.Shutdown()
  │                       ├─ http.Server.Shutdown()
  │                       └─ Signal: HTTPServerStoppedListening
```

关键设计点：
- `ShutdownDelayDuration`：默认 0，可通过 `--shutdown-delay-duration` 设置。在收到 SIGTERM 后，先延迟一段时间再开始拒绝请求，让负载均衡器检测到 `/readyz` 变红后摘除节点
- **两阶段关闭**：先等待非长连接请求完成（受 `RequestTimeout` 限制），再等待 Watch 连接断开（受 `ShutdownWatchTerminationGracePeriod` 限制）
- **Retry-After 模式**：启用 `ShutdownSendRetryAfter` 后，服务器在关闭期间对新请求返回 `429 Retry-After` 而不是直接断开连接

---

## 9. 本篇总结

kube-apiserver 的启动流程可以概括为**三步走**：

1. **配置构建**：Options → Config → CompletedConfig。选项被层层补全、验证，最终形成三个子服务器共享的配置集合
2. **服务器链构建**：从 APIExtensions → KubeAPIs → Aggregator 逐层实例化，通过 `DelegationTarget` 接口形成委托链
3. **启动运行**：PrepareRun 安装 OpenAPI、健康检查和调试端点，Run 启动 HTTPS 监听、执行 PostStartHooks，并等待优雅关闭

整个流程中大量运用了 Kubernetes 源码的经典设计模式：
- **Config / CompletedConfig** 模式：编译器强制要求 Complete() 先于 New()
- **委托链**模式：通过接口组合实现关注点分离
- **装饰器**模式：`DefaultBuildHandlerChain` 在 HTTP Handler 上逐层包装中间件
