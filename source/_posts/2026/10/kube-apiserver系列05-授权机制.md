---
title: kube-apiserver 源码解读（五）授权机制：从 Attributes 到 RBAC/Node 授权链
date: 2026-10-22 10:00:00
tags:
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 引言

在上一篇中，我们完整分析了认证机制——它回答的问题是"你是谁"。请求通过认证后，用户信息被注入 `context.Context`，紧接着就要面对下一道安全关卡：**授权（Authorization）**。授权回答的问题是"你能做什么"。

Kubernetes 的授权设计与认证有几个显著差异：

- **三态决策**：认证只有成功/失败两态，授权有 `Allow`、`Deny`、`NoOpinion` 三态。`NoOpinion` 表示"我没有意见，请下一个授权器判断"，这是多个授权器能够组合工作的基础
- **短路聚合**：授权器链中任何一个返回 `Allow` 或 `Deny` 都会立即短路，只有 `NoOpinion` 才会继续向后传递
- **默认拒绝**：如果整个链条走完都没有授权器说 `Allow`，请求被拒绝。RBAC 授权器匹配不到规则时返回的也是 `NoOpinion` 而非 `Deny`——"没有规则允许"不等于"有规则禁止"
- **策略与机制分离**：`Node` 授权器负责 kubelet 的细粒度资源访问，`RBAC` 负责通用的角色绑定，两者组合使用，各司其职

<!--more-->

本文将从授权的核心接口开始，依次深入 `WithAuthorization` 过滤器、授权链的组装、RBAC/Node/ABAC/Webhook 四种授权器的实现，最后串联起完整的授权数据流。

---

## 2. 授权核心接口与数据模型

### 2.1 核心接口

授权系统的核心接口定义在 `staging/src/k8s.io/apiserver/pkg/authorization/authorizer/interfaces.go`：

```go
// 授权器根据请求属性做出授权决策
type Authorizer interface {
    Authorize(ctx context.Context, a Attributes) (authorized Decision, reason string, err error)
}

// Attributes 描述一次授权决策所需的全部请求信息
type Attributes interface {
    GetUser() user.Info          // 认证阶段注入的用户信息
    GetVerb() string             // get/list/watch/create/update/patch/delete/...
    IsReadOnly() bool            // 是否只读（get/list/watch）
    GetNamespace() string
    GetResource() string         // 资源类型，如 pods
    GetSubresource() string      // 子资源，如 status/log
    GetName() string
    GetAPIGroup() string
    GetAPIVersion() string
    IsResourceRequest() bool     // 资源请求 vs 非资源端点（/healthz 等）
    GetPath() string             // 非资源请求的路径
    GetFieldSelector() (fields.Requirements, error)   // 字段选择器（feature gate）
    GetLabelSelector() (labels.Requirements, error)   // 标签选择器（feature gate）
}
```

每个授权器返回三个值：

| 返回值 | 语义 | 链上的处理 |
|--------|------|-----------|
| `DecisionAllow` | 允许 | 立即停止链，请求放行 |
| `DecisionDeny` | 拒绝 | 立即停止链，请求返回 403 |
| `DecisionNoOpinion` | 无意见 | 继续尝试下一个授权器 |

`Decision` 的常量定义值得注意其顺序：

```go
type Decision int

const (
    DecisionDeny Decision = iota  // 0
    DecisionAllow                 // 1
    DecisionNoOpinion             // 2
)
```

### 2.2 AttributesRecord

`AttributesRecord` 是 `Attributes` 接口的默认实现，一个纯数据结构：

```go
type AttributesRecord struct {
    User            user.Info
    Verb            string
    Namespace       string
    APIGroup        string
    APIVersion      string
    Resource        string
    Subresource     string
    Name            string
    ResourceRequest bool
    Path            string

    FieldSelectorRequirements fields.Requirements   // AuthorizeWithSelectors
    FieldSelectorParsingErr   error
    LabelSelectorRequirements labels.Requirements
    LabelSelectorParsingErr   error
}

func (a AttributesRecord) IsReadOnly() bool {
    return a.Verb == "get" || a.Verb == "list" || a.Verb == "watch"
}
```

### 2.3 RuleResolver：授权规则查询接口

除了 `Authorize` 做实时决策外，授权器还可以实现 `RuleResolver` 接口，用于查询"某用户在某命名空间下拥有哪些规则"：

```go
type RuleResolver interface {
    RulesFor(ctx context.Context, user user.Info, namespace string) (
        []ResourceRuleInfo, []NonResourceRuleInfo, bool, error)
}
```

这个接口有两个用途：
- `kubectl auth can-i` 命令——用户自查权限
- RBAC 的**防提权校验**（escalation prevention）——创建 Role/RoleBinding 时确认授予的权限不超过授予者自身拥有的权限

---

## 3. 授权过滤器：WithAuthorization

### 3.1 过滤器位置

授权过滤器位于整个 HTTP 处理链的**最内层**，是请求到达 REST Handler 之前的最后一道关卡：

```go
// staging/src/k8s.io/apiserver/pkg/server/config.go
// DefaultBuildHandlerChain 中的第一组（最内层）
handler = filterlatency.TrackCompleted(handler)
handler = genericapifilters.WithAuthorization(handler, c.Authorization.Authorizer, c.Serializer)
handler = filterlatency.TrackStarted(handler, c.TracerProvider, "authorization")
```

请求到达这里时，已经依次通过了认证（用户身份已确定）、追踪、审计、模拟请求（Impersonation）、限流（APF）。授权是进入业务逻辑前的最后一个检查点。

### 3.2 过滤器实现

**源码**：`staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go`

```go
func withAuthorization(handler http.Handler, a authorizer.Authorizer,
    s runtime.NegotiatedSerializer, metrics recordAuthorizationMetricsFunc) http.Handler {
    if a == nil {
        klog.Warning("Authorization is disabled")
        return handler   // 授权器为 nil 时直接跳过（仅测试场景）
    }
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        ctx := req.Context()
        authorizationStart := time.Now()

        // 1. 从 context 构建授权属性
        attributes, err := GetAuthorizerAttributes(ctx)
        if err != nil {
            responsewriters.InternalError(w, req, err)
            return
        }

        // 2. 执行授权
        authorized, reason, err := a.Authorize(ctx, attributes)

        // 3. 记录延迟与指标
        authorizationFinish := time.Now()
        request.TrackAuthorizationLatency(ctx, authorizationFinish.Sub(authorizationStart))
        defer func() {
            metrics(ctx, authorized, err, authorizationStart, authorizationFinish)
        }()

        // 4. 先检查 decision，再检查 error
        if authorized == authorizer.DecisionAllow {
            audit.AddAuditAnnotations(ctx,
                decisionAnnotationKey, decisionAllow,
                reasonAnnotationKey, reason)
            handler.ServeHTTP(w, req)
            return
        }
        if err != nil {
            audit.AddAuditAnnotation(ctx, reasonAnnotationKey, reasonError)
            responsewriters.InternalError(w, req, err)   // 500
            return
        }

        // 5. 拒绝 → 403 Forbidden
        klog.V(4).InfoS("Forbidden", "URI", req.RequestURI, "reason", reason)
        audit.AddAuditAnnotations(ctx,
            decisionAnnotationKey, decisionForbid,
            reasonAnnotationKey, reason)
        responsewriters.Forbidden(attributes, w, req, reason, s)
    })
}
```

**"先检查 decision 再检查 error"是刻意设计**。源码注释写得很清楚：

> an authorizer like RBAC could encounter evaluation errors and still allow the request, so authorizer decision is checked before error here.

RBAC 授权器在解析规则时可能遇到局部错误（比如某个 ClusterRole 无法从缓存读取），但它仍然可以基于已成功解析的规则做出 `Allow` 决策。规则解析错误只影响 `reason` 的完整性，不应该让一个本应被允许的请求失败。这是**可用性优先**的取舍。

### 3.3 GetAuthorizerAttributes：构建授权属性

```go
func GetAuthorizerAttributes(ctx context.Context) (authorizer.Attributes, error) {
    attribs := authorizer.AttributesRecord{}

    // 用户信息：认证过滤器注入的
    if user, ok := request.UserFrom(ctx); ok {
        attribs.User = user
    }

    // RequestInfo：WithRequestInfo 过滤器从 URL 解析的
    requestInfo, found := request.RequestInfoFrom(ctx)
    if !found {
        return nil, errors.New("no RequestInfo found in the context")
    }

    attribs.ResourceRequest = requestInfo.IsResourceRequest
    attribs.Path = requestInfo.Path
    attribs.Verb = requestInfo.Verb
    attribs.APIGroup = requestInfo.APIGroup
    attribs.APIVersion = requestInfo.APIVersion
    attribs.Resource = requestInfo.Resource
    attribs.Subresource = requestInfo.Subresource
    attribs.Namespace = requestInfo.Namespace
    attribs.Name = requestInfo.Name

    // AuthorizeWithSelectors feature gate：解析字段/标签选择器
    if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.AuthorizeWithSelectors) {
        if len(requestInfo.FieldSelector) > 0 {
            fieldSelector, err := fields.ParseSelector(requestInfo.FieldSelector)
            // 解析结果注入 attribs.FieldSelectorRequirements
        }
        if len(requestInfo.LabelSelector) > 0 {
            labelSelector, err := labels.Parse(requestInfo.LabelSelector)
            // 解析结果注入 attribs.LabelSelectorRequirements
        }
    }
    return &attribs, nil
}
```

授权属性的数据来源完全依赖前序过滤器：用户信息来自认证过滤器，`RequestInfo` 来自 `WithRequestInfo`。这再次印证了处理链的层次设计——每一层只消费上一层产出的 context 数据，不重复解析。

`AuthorizeWithSelectors`（1.32+ alpha）让授权器能看到 `list pods?spec.nodeName=node-1` 这类选择器，从而支持更细粒度的授权。Node 授权器正是利用它限制 kubelet 只能 list/watch 自己节点上的 Pod。

### 3.4 审计注解

授权结果会写入审计事件的两个注解：

| 注解键 | 值 |
|--------|-----|
| `authorization.k8s.io/decision` | `allow` 或 `forbid` |
| `authorization.k8s.io/reason` | 授权器给出的理由，如 `RBAC: allowed by ClusterRoleBinding "view"` |

这意味着每条审计日志都能回答"这次请求为什么被允许/拒绝"，是排查权限问题的第一手资料。

---

## 4. 授权链的组装

### 4.1 支持的授权模式

**源码**：`pkg/kubeapiserver/authorizer/modes/modes.go`

```go
const (
    ModeAlwaysAllow string = "AlwaysAllow"
    ModeAlwaysDeny  string = "AlwaysDeny"
    ModeABAC        string = "ABAC"
    ModeWebhook     string = "Webhook"
    ModeRBAC        string = "RBAC"
    ModeNode        string = "Node"
)
```

生产集群的典型配置是 `--authorization-mode=Node,RBAC`——Node 授权器先处理 kubelet 请求，其余请求交给 RBAC。

### 4.2 组装入口

**源码**：`pkg/kubeapiserver/authorizer/config.go`

`Config.New()` 是授权链的组装入口：

```go
func (config Config) New(ctx context.Context, serverID string) (authorizer.Authorizer, authorizer.RuleResolver, error) {
    r := &reloadableAuthorizerResolver{
        initialConfig: config,
        // ...
    }

    // 第一阶段：构建需要跨 reload 存活的重量级授权器
    for _, configuredAuthorizer := range config.AuthorizationConfiguration.Authorizers {
        switch configuredAuthorizer.Type {
        case modes.ModeNode:
            // Node 授权器：构建对象关系图 + 注册 informer 事件处理器
            graph := node.NewGraph()
            node.AddGraphEventHandlers(graph,
                nodesInformer, podsInformer, pvsInformer,
                volumeAttachmentsInformer, slicesInformer, pcrsInformer)
            r.nodeAuthorizer = node.NewAuthorizer(graph,
                nodeidentifier.NewDefaultNodeIdentifier(),
                bootstrappolicy.NodeRules())   // 静态兜底规则

        case modes.ModeABAC:
            r.abacAuthorizer, err = abac.NewFromFile(config.PolicyFile)

        case modes.ModeRBAC:
            // RBAC 授权器：从 informer lister 读取 Role/RoleBinding 等
            r.rbacAuthorizer = rbac.New(
                &rbac.RoleGetter{Lister: rbacInformer.Roles().Lister()},
                &rbac.RoleBindingLister{Lister: rbacInformer.RoleBindings().Lister()},
                &rbac.ClusterRoleGetter{Lister: rbacInformer.ClusterRoles().Lister()},
                &rbac.ClusterRoleBindingLister{Lister: rbacInformer.ClusterRoleBindings().Lister()},
            )
        }
    }

    // 第二阶段：按配置构造最终授权链
    authorizer, ruleResolver, err := r.newForConfig(r.initialConfig.AuthorizationConfiguration)
    r.current.Store(&authorizerResolver{authorizer: authorizer, ruleResolver: ruleResolver})

    // 如果有配置文件，启动热加载
    if r.initialConfig.ReloadFile != "" {
        go r.runReload(ctx)
    }
    return r, r, nil
}
```

Node、RBAC、ABAC 授权器在启动时构建一次，跨配置热加载存活；Webhook 授权器则在每次 `newForConfig` 时重建（因为它依赖配置文件中的 kubeconfig 路径）。

### 4.3 newForConfig：构建 union 链

**源码**：`pkg/kubeapiserver/authorizer/reload.go`

```go
func (r *reloadableAuthorizerResolver) newForConfig(authzConfig *authzconfig.AuthorizationConfiguration) (authorizer.Authorizer, authorizer.RuleResolver, error) {
    var authorizers []authorizer.Authorizer
    var ruleResolvers []authorizer.RuleResolver

    // 永远在链首插入 system:masters 短路授权器
    superuserAuthorizer := authorizerfactory.NewPrivilegedGroups(user.SystemPrivilegedGroup)
    authorizers = append(authorizers, superuserAuthorizer)

    // 按配置顺序追加授权器，每个都用 metrics 包装
    for _, configuredAuthorizer := range authzConfig.Authorizers {
        switch configuredAuthorizer.Type {
        case modes.ModeNode:
            authorizers = append(authorizers,
                authorizationmetrics.InstrumentedAuthorizer("Node", name, r.nodeAuthorizer))
        case modes.ModeRBAC:
            authorizers = append(authorizers,
                authorizationmetrics.InstrumentedAuthorizer("RBAC", name, r.rbacAuthorizer))
        case modes.ModeWebhook:
            // 每次重建 Webhook 授权器
            webhookAuthorizer, _ := webhook.New(clientConfig, version,
                authorizedTTL, unauthorizedTTL, backoff, decisionOnError,
                matchConditions, name, metrics, compiler)
            authorizers = append(authorizers,
                authorizationmetrics.InstrumentedAuthorizer("Webhook", name, webhookAuthorizer))
        case modes.ModeAlwaysAllow:
            authorizers = append(authorizers,
                authorizationmetrics.InstrumentedAuthorizer("AlwaysAllow", name,
                    authorizerfactory.NewAlwaysAllowAuthorizer()))
        // ModeAlwaysDeny, ModeABAC 类似...
        }
    }

    // 合并为 union 链
    return union.New(authorizers...), union.NewRuleResolvers(ruleResolvers...), nil
}
```

组装出的完整链条：

```
请求 → system:masters 授权器（特权组短路）
     → Node 授权器（如配置）
     → RBAC 授权器（如配置）
     → Webhook 授权器（如配置，可多个）
     → ... 按 --authorization-mode 顺序
     → 全部 NoOpinion → 403 Forbidden
```

### 4.4 Union 授权器

**源码**：`staging/src/k8s.io/apiserver/pkg/authorization/union/union.go`

```go
func (authzHandler unionAuthzHandler) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
    var errlist []error
    var reasonlist []string

    for _, currAuthzHandler := range authzHandler {
        decision, reason, err := currAuthzHandler.Authorize(ctx, a)

        if err != nil {
            errlist = append(errlist, err)   // 记录错误但不中断
        }
        if len(reason) != 0 {
            reasonlist = append(reasonlist, reason)
        }
        switch decision {
        case authorizer.DecisionAllow, authorizer.DecisionDeny:
            return decision, reason, err    // 短路返回
        case authorizer.DecisionNoOpinion:
            // 继续下一个
        }
    }
    // 全部无意见：聚合 reason 和 error 返回
    return authorizer.DecisionNoOpinion, strings.Join(reasonlist, "\n"), utilerrors.NewAggregate(errlist)
}
```

与认证链的 union 对比：认证链是"第一个成功者胜出"，授权链是"第一个有明确意见（Allow/Deny）者胜出"。错误不会中断链条——某个授权器出错（如 Webhook 超时），其他授权器仍然有机会做出决策。

### 4.5 配置方式：legacy flags 与配置文件

kube-apiserver 支持两种配置授权链的方式：

**传统 flags**：
```
--authorization-mode=Node,RBAC
--authorization-webhook-config-file=/etc/kubernetes/webhook-authz.yaml
--abac-policy-file=/etc/kubernetes/abac-policy.jsonl
```

**配置文件**（`--authorization-config`，1.30+ 推荐）：
```yaml
apiVersion: apiserver.config.k8s.io/v1beta1
kind: AuthorizationConfiguration
authorizers:
  - type: Node
    name: node
  - type: RBAC
    name: rbac
  - type: Webhook
    name: my-webhook
    webhook:
      connectionInfo:
        type: KubeConfig
        kubeConfigFile: /etc/kubernetes/webhook-authz.yaml
      matchConditions:
        - expression: "request.resource.subresource == 'exec'"
      failurePolicy: Deny
      authorizedTTL: 5m
      unauthorizedTTL: 30s
```

两种方式不能混用。`ToAuthorizationConfig`（`pkg/kubeapiserver/options/authorization.go`）负责把 legacy flags 转换成统一的 `AuthorizationConfiguration` 结构。

---

## 5. 内置授权器详解

### 5.1 system:masters 特权组授权器

**源码**：`staging/src/k8s.io/apiserver/pkg/authorization/authorizerfactory/builtin.go`

这是链上第一个授权器，实现极简：

```go
func (r *privilegedGroupAuthorizer) Authorize(ctx context.Context, attr authorizer.Attributes) (authorizer.Decision, string, error) {
    if attr.GetUser() == nil {
        return authorizer.DecisionNoOpinion, "Error", errors.New("no user on request.")
    }
    for _, attr_group := range attr.GetUser().GetGroups() {
        for _, priv_group := range r.groups {
            if priv_group == attr_group {
                return authorizer.DecisionAllow, "", nil
            }
        }
    }
    return authorizer.DecisionNoOpinion, "", nil
}
```

只要用户属于 `system:masters` 组（通常通过客户端证书的 O 字段获得），无条件放行。这个授权器**无法被配置移除**——它硬编码在 `newForConfig` 的链首。这是集群的"逃生通道"：即使 RBAC 配置错误导致管理员锁死自己，持有 `system:masters` 证书仍能恢复。

### 5.2 AlwaysAllow 与 AlwaysDeny

同样来自 `builtin.go`：

```go
func (alwaysAllowAuthorizer) Authorize(...) (authorizer.Decision, string, error) {
    return authorizer.DecisionAllow, "", nil
}

func (alwaysDenyAuthorizer) Authorize(...) (authorizer.Decision, string, error) {
    return authorizer.DecisionNoOpinion, "Everything is forbidden.", nil
}
```

注意 AlwaysDeny 返回的是 `DecisionNoOpinion` 而非 `DecisionDeny`——它只是链条末尾的兜底，让最终聚合结果为"无人允许"→ 403。两者主要用于测试和特殊场景，生产环境不应使用 AlwaysAllow。

### 5.3 RBAC 授权器

RBAC 是 kube-apiserver 的默认授权机制。理解它需要看三层：授权器本体、规则匹配、规则解析。

#### 5.3.1 授权器本体

**源码**：`plugin/pkg/auth/authorizer/rbac/rbac.go`

```go
func (r *RBACAuthorizer) Authorize(ctx context.Context, requestAttributes authorizer.Attributes) (authorizer.Decision, string, error) {
    ruleCheckingVisitor := &authorizingVisitor{requestAttributes: requestAttributes}

    // 遍历所有适用于该用户的规则，任一规则匹配即短路
    r.authorizationRuleResolver.VisitRulesFor(ctx,
        requestAttributes.GetUser(), requestAttributes.GetNamespace(),
        ruleCheckingVisitor.visit)

    if ruleCheckingVisitor.allowed {
        return authorizer.DecisionAllow, ruleCheckingVisitor.reason, nil
    }

    // 没有规则匹配 → NoOpinion（不是 Deny！）
    return authorizer.DecisionNoOpinion, reason, nil
}

type authorizingVisitor struct {
    requestAttributes authorizer.Attributes
    allowed bool
    reason  string
    errors  []error
}

func (v *authorizingVisitor) visit(source fmt.Stringer, rule *rbacv1.PolicyRule, err error) bool {
    if rule != nil && RuleAllows(v.requestAttributes, rule) {
        v.allowed = true
        v.reason = fmt.Sprintf("RBAC: allowed by %s", source.String())
        return false   // 短路：停止遍历
    }
    if err != nil {
        v.errors = append(v.errors, err)
    }
    return true    // 继续下一个规则
}
```

关键设计：RBAC 匹配不到规则时返回 `DecisionNoOpinion`，这让后面的 Webhook 授权器还有机会。同时 `reason` 中记录了是哪个 RoleBinding/ClusterRoleBinding 允许的（`source.String()`），便于审计追溯。

#### 5.3.2 规则匹配：RuleAllows

```go
func RuleAllows(requestAttributes authorizer.Attributes, rule *rbacv1.PolicyRule) bool {
    if requestAttributes.IsResourceRequest() {
        // 资源请求：组合 resource 与 subresource
        combinedResource := requestAttributes.GetResource()
        if len(requestAttributes.GetSubresource()) > 0 {
            combinedResource = requestAttributes.GetResource() + "/" + requestAttributes.GetSubresource()
        }

        return rbacv1helpers.VerbMatches(rule, requestAttributes.GetVerb()) &&
            rbacv1helpers.APIGroupMatches(rule, requestAttributes.GetAPIGroup()) &&
            rbacv1helpers.ResourceMatches(rule, combinedResource, requestAttributes.GetSubresource()) &&
            rbacv1helpers.ResourceNameMatches(rule, requestAttributes.GetName())
    }

    // 非资源请求：匹配 verb 和 path
    return rbacv1helpers.VerbMatches(rule, requestAttributes.GetVerb()) &&
        rbacv1helpers.NonResourceURLMatches(rule, requestAttributes.GetPath())
}
```

各匹配函数（`pkg/apis/rbac/v1/evaluation_helpers.go`）都支持 `*` 通配：

```go
func VerbMatches(rule *rbacv1.PolicyRule, requestedVerb string) bool {
    for _, ruleVerb := range rule.Verbs {
        if ruleVerb == rbacv1.VerbAll {   // "*"
            return true
        }
        if ruleVerb == requestedVerb {
            return true
        }
    }
    return false
}

func ResourceMatches(rule *rbacv1.PolicyRule, combinedRequestedResource, requestedSubresource string) bool {
    for _, ruleResource := range rule.Resources {
        if ruleResource == rbacv1.ResourceAll { return true }
        if ruleResource == combinedRequestedResource { return true }
        // 支持 */subresource 形式：如 */status 匹配 pods/status
        if len(requestedSubresource) > 0 &&
            strings.HasPrefix(ruleResource, "*/") &&
            strings.HasSuffix(ruleResource, requestedSubresource) {
            return true
        }
    }
    return false
}

func ResourceNameMatches(rule *rbacv1.PolicyRule, requestedName string) bool {
    if len(rule.ResourceNames) == 0 {
        return true   // 未限定 resourceNames → 匹配所有对象
    }
    for _, ruleName := range rule.ResourceNames {
        if ruleName == requestedName { return true }
    }
    return false
}
```

值得注意的细节：
- **subresource 作为独立授权点**：`pods` 和 `pods/log`、`pods/exec` 是不同的资源，需要分别授权。`kubectl logs` 需要 `pods/log` 的 get 权限，`kubectl exec` 需要 `pods/exec` 的 create 权限
- **`resourceNames` 的限制**：只对 get/update/patch/delete 有意义，无法用于 list/watch/create——因为 list 请求没有 name，create 时 name 可能还未确定

#### 5.3.3 规则解析：DefaultRuleResolver

**源码**：`pkg/registry/rbac/validation/rule.go`

RBAC 授权器不直接读取 API 资源，而是通过 `DefaultRuleResolver` 从 informer 缓存解析规则：

```go
func (r *DefaultRuleResolver) VisitRulesFor(ctx context.Context, user user.Info, namespace string,
    visitor func(source fmt.Stringer, rule *rbacv1.PolicyRule, err error) bool) {

    // 第一部分：所有 ClusterRoleBinding（集群范围）
    if clusterRoleBindings, err := r.clusterRoleBindingLister.ListClusterRoleBindings(ctx); err != nil {
        visitor(nil, nil, err)
    } else {
        for _, clusterRoleBinding := range clusterRoleBindings {
            // 检查 binding 的 subjects 是否包含当前用户
            subjectIndex, applies := appliesTo(user, clusterRoleBinding.Subjects, "")
            if !applies { continue }

            // 解析 roleRef → 获取实际规则
            rules, err := r.GetRoleReferenceRules(ctx, clusterRoleBinding.RoleRef, "")
            for i := range rules {
                if !visitor(sourceDescriber, &rules[i], nil) { return }  // 可短路
            }
        }
    }

    // 第二部分：指定命名空间内的所有 RoleBinding
    if len(namespace) > 0 {
        if roleBindings, err := r.roleBindingLister.ListRoleBindings(ctx, namespace); err != nil {
            visitor(nil, nil, err)
        } else {
            for _, roleBinding := range roleBindings {
                subjectIndex, applies := appliesTo(user, roleBinding.Subjects, namespace)
                if !applies { continue }

                rules, err := r.GetRoleReferenceRules(ctx, roleBinding.RoleRef, namespace)
                for i := range rules {
                    if !visitor(sourceDescriber, &rules[i], nil) { return }
                }
            }
        }
    }
}
```

`GetRoleReferenceRules` 解析 RoleRef 的两种形式：

```go
func (r *DefaultRuleResolver) GetRoleReferenceRules(ctx context.Context, roleRef rbacv1.RoleRef, bindingNamespace string) ([]rbacv1.PolicyRule, error) {
    switch roleRef.Kind {
    case "Role":
        role, err := r.roleGetter.GetRole(ctx, bindingNamespace, roleRef.Name)
        return role.Rules, nil
    case "ClusterRole":
        clusterRole, err := r.clusterRoleGetter.GetClusterRole(ctx, roleRef.Name)
        return clusterRole.Rules, nil
    default:
        return nil, fmt.Errorf("unsupported role reference kind: %q", roleRef.Kind)
    }
}
```

**RoleBinding 可以引用 ClusterRole**——这是常见实践：定义一个 ClusterRole 描述"能做什么"，再用 RoleBinding 把它绑定到某命名空间的用户，实现"命名空间内的权限"。

主体匹配逻辑：

```go
func appliesToUser(user user.Info, subject rbacv1.Subject, namespace string) bool {
    switch subject.Kind {
    case rbacv1.UserKind:
        return user.GetName() == subject.Name
    case rbacv1.GroupKind:
        return has(user.GetGroups(), subject.Name)
    case rbacv1.ServiceAccountKind:
        saNamespace := namespace
        if len(subject.Namespace) > 0 { saNamespace = subject.Namespace }
        // 匹配 system:serviceaccount:<ns>:<name> 格式的用户名
        return serviceaccount.MatchesUsername(saNamespace, subject.Name, user.GetName())
    default:
        return false
    }
}
```

规则解析的范围界定：
- **ClusterRoleBinding**：对所有命名空间生效，因此先全部列出
- **RoleBinding**：只列出请求目标命名空间的——这就是为什么集群范围的 list 请求只能被 ClusterRoleBinding 授权，RoleBinding 无法授予集群范围 list 的权限

#### 5.3.4 防提权校验

`ConfirmNoEscalation`（`pkg/registry/rbac/validation/rule.go`）在创建/更新 Role 和 RoleBinding 时被调用：

```go
func ConfirmNoEscalation(ctx context.Context, ruleResolver AuthorizationRuleResolver, rules []rbacv1.PolicyRule) error {
    user, _ := genericapirequest.UserFrom(ctx)
    namespace, _ := genericapirequest.NamespaceFrom(ctx)

    // 获取操作者当前拥有的所有规则
    ownerRules, err := ruleResolver.RulesFor(ctx, user, namespace)

    // 检查要授予的规则是否被操作者现有规则覆盖
    ownerRightsCover, missingRights := validation.Covers(ownerRules, rules)
    if !ownerRightsCover {
        return fmt.Errorf("user %q is attempting to grant RBAC permissions not currently held", user.GetName())
    }
    return nil
}
```

除非你是 `system:masters` 成员（前面的授权器已短路），否则你**不能授予别人你自己没有的权限**。这是 RBAC 防止权限扩散的核心机制。管理员需要更大权限时，只能通过既有的高权限身份操作。

### 5.4 Node 授权器

Node 授权器解决一个特定问题：**kubelet 只能访问与其节点相关的资源**。它不是通用授权器，而是针对 kubelet 行为的专用细粒度控制。

#### 5.4.1 设计思想

如果不用 Node 授权器，kubelet 需要通过 RBAC 获得"能 get 所有 secret"的权限（因为它不知道会调度哪些 Pod 到自己身上）。这显然过度授权。Node 授权器的做法是：

1. 维护一个**对象关系图**：记录 Pod、Secret、ConfigMap、PVC、PV 等对象与节点的关联
2. kubelet 请求某资源时，检查**图中是否存在从该资源到请求节点的路径**
3. 有路径 → 允许；无路径 → 拒绝

#### 5.4.2 对象关系图

**源码**：`plugin/pkg/auth/authorizer/node/graph.go`

图是一个有向无环图（DAG），所有边最终指向节点：

```
secret    ← pod ← node
configmap ← pod ← node
pvc       ← pod ← node
pv        ← pvc
volumeattachment ← node
resourceslice    ← node
```

```go
type Graph struct {
    lock    sync.RWMutex
    graph   *simple.DirectedAcyclicGraph
    vertices map[vertexType]namespaceVertexMapping  // type → ns → name → vertex

    destinationEdgeIndex map[int]*intSet  // 优化：vertex → 可达的 node ID 集合
    destinationEdgeThreshold int
}

type namedVertex struct {
    name       string
    namespace  string
    id         int
    vertexType vertexType   // configMap/secret/pod/pv/... 单字节枚举
}
```

图的维护通过 **informer 事件驱动**（`graph_populator.go`）：

```go
func AddGraphEventHandlers(graph *Graph, nodes, pods, pvs, attachments, slices, pcrs informers) {
    g := &graphPopulator{graph: graph}
    pods.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: g.addPod, UpdateFunc: g.updatePod, DeleteFunc: g.deletePod,
    })
    pvs.Informer().AddEventHandler(...)
    attachments.Informer().AddEventHandler(...)
    // ...
    go cache.WaitForNamedCacheSync("node_authorizer", wait.NeverStop, synced...)
}
```

Pod 变更时重建其所有边（`graph.go: AddPod`）：

```go
func (g *Graph) AddPod(pod *corev1.Pod) {
    g.deleteVertexLocked(podVertexType, pod.Namespace, pod.Name)  // 先清旧边
    podVertex := g.getOrCreateVertexLocked(podVertexType, pod.Namespace, pod.Name)
    nodeVertex := g.getOrCreateVertexLocked(nodeVertexType, "", pod.Spec.NodeName)
    g.addEdgeLocked(podVertex, nodeVertex, nodeVertex)   // pod → node

    // 镜像 Pod（mirror pod）不添加其他边——节点不得通过创建 Pod 扩大自身权限
    if _, isMirrorPod := pod.Annotations[corev1.MirrorPodAnnotationKey]; isMirrorPod {
        return
    }

    // serviceaccount → pod
    if len(pod.Spec.ServiceAccountName) > 0 {
        saVertex := g.getOrCreateVertexLocked(serviceAccountVertexType, pod.Namespace, pod.Spec.ServiceAccountName)
        g.addEdgeLocked(saVertex, podVertex, nodeVertex)
    }

    // secret → pod（遍历 Pod 引用的所有 secret）
    podutil.VisitPodSecretNames(pod, func(secret string) bool {
        secretVertex := g.getOrCreateVertexLocked(secretVertexType, pod.Namespace, secret)
        g.addEdgeLocked(secretVertex, podVertex, nodeVertex)
        return true
    })

    // configmap → pod
    podutil.VisitPodConfigmapNames(pod, func(configmap string) bool { ... })

    // pvc → pod（包括 ephemeral volume）
    for _, v := range pod.Spec.Volumes {
        if v.PersistentVolumeClaim != nil { ... }
        if v.Ephemeral != nil { ... }
    }

    // resourceclaim → pod（动态资源分配）
    for _, podResourceClaim := range pod.Spec.ResourceClaims { ... }
}
```

`addEdgeLocked` 在建边的同时维护 `destinationEdgeIndex`——一个"从某顶点可达哪些节点"的反向索引。当索引不存在或不完整时，查询退化为深度优先搜索（DFS）。

#### 5.4.3 授权决策流程

**源码**：`plugin/pkg/auth/authorizer/node/node_authorizer.go`

```go
func (r *NodeAuthorizer) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
    // 1. 非节点用户 → NoOpinion（交给下一个授权器）
    nodeName, isNode := r.identifier.NodeIdentity(attrs.GetUser())
    if !isNode {
        return authorizer.DecisionNoOpinion, "", nil
    }
    if len(nodeName) == 0 {
        return authorizer.DecisionNoOpinion, fmt.Sprintf("unknown node for user %q", ...), nil
    }

    // 2. 对敏感资源做图路径检查
    if attrs.IsResourceRequest() {
        requestResource := schema.GroupResource{Group: attrs.GetAPIGroup(), Resource: attrs.GetResource()}
        switch requestResource {
        case secretResource:
            return r.authorizeReadNamespacedObject(nodeName, secretVertexType, attrs)
        case configMapResource:
            return r.authorizeReadNamespacedObject(nodeName, configMapVertexType, attrs)
        case pvcResource:
            if attrs.GetSubresource() == "status" {
                return r.authorizeStatusUpdate(nodeName, pvcVertexType, attrs)
            }
            return r.authorizeGet(nodeName, pvcVertexType, attrs)
        case pvResource:
            return r.authorizeGet(nodeName, pvVertexType, attrs)
        case leaseResource:
            return r.authorizeLease(nodeName, attrs)
        case nodeResource:
            if r.features.Enabled(features.AuthorizeNodeWithSelectors) {
                return r.authorizeNode(nodeName, attrs)
            }
        case podResource:
            if r.features.Enabled(features.AuthorizeNodeWithSelectors) {
                return r.authorizePod(nodeName, attrs)
            }
        // resourceSlice, csiNode, resourceClaim, serviceAccount, pcr...
        }
    }

    // 3. 其他资源：用静态规则兜底
    if rbac.RulesAllow(attrs, r.nodeRules...) {
        return authorizer.DecisionAllow, "", nil
    }
    return authorizer.DecisionNoOpinion, "", nil
}
```

以 Secret 为例，`authorizeReadNamespacedObject` 的限制：

```go
func (r *NodeAuthorizer) authorizeReadNamespacedObject(nodeName string, startingType vertexType, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
    // 只允许 get/list/watch
    switch attrs.GetVerb() {
    case "get", "list", "watch":
    default:
        return authorizer.DecisionNoOpinion, "can only read resources of this type", nil
    }
    // 不允许访问 subresource
    if len(attrs.GetSubresource()) > 0 { ... }
    // 必须是命名空间内的对象
    if len(attrs.GetNamespace()) == 0 { ... }

    return r.authorize(nodeName, startingType, attrs)
}

func (r *NodeAuthorizer) authorize(nodeName string, startingType vertexType, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
    // 图中查找从资源到节点的路径
    ok, err := r.hasPathFrom(nodeName, startingType, attrs.GetNamespace(), attrs.GetName())
    if !ok {
        return authorizer.DecisionNoOpinion,
            fmt.Sprintf("no relationship found between node '%s' and this object", nodeName), nil
    }
    return authorizer.DecisionAllow, "", nil
}
```

`hasPathFrom` 的查询过程：

```go
func (r *NodeAuthorizer) hasPathFrom(nodeName string, startingType vertexType, startingNamespace, startingName string) (bool, error) {
    nodeVertex, _ := r.graph.getVertexRLocked(nodeVertexType, "", nodeName)
    startingVertex, _ := r.graph.getVertexRLocked(startingType, startingNamespace, startingName)

    // 快速路径：destinationEdgeIndex 直接命中
    if index, indexExists := r.graph.destinationEdgeIndex[startingVertex.ID()]; indexExists {
        if index.has(nodeVertex.ID()) {
            return true, nil
        }
        // 某些类型的索引是权威的，可跳过 DFS
        if vertexTypeWithAuthoritativeIndex[startingType] {
            return false, fmt.Errorf("no relationship to this object was found in the node authorizer graph"), nil
        }
    }

    // 慢路径：深度优先搜索
    traversal := &traverse.VisitingDepthFirst{
        EdgeFilter: func(edge graph.Edge) bool {
            if destinationEdge, ok := edge.(*destinationEdge); ok {
                if destinationEdge.DestinationID() != nodeVertex.ID() {
                    return false   // 不走向其他节点
                }
                found = true
            }
            return true
        },
    }
    traversal.Walk(r.graph.graph, startingVertex, func(n graph.Node) bool {
        if n.ID() == nodeVertex.ID() { found = true }
        return found
    })
    return found, nil
}
```

#### 5.4.4 特殊资源的处理

不是所有资源都走图检查。几类特殊规则：

**Lease**（节点心跳）：
```go
func (r *NodeAuthorizer) authorizeLease(nodeName string, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
    // 允许 get/create/update/patch/delete
    // 必须在 kube-node-leases 命名空间
    if attrs.GetNamespace() != api.NamespaceNodeLease { ... }
    // lease 名必须等于节点名（create 时由 NodeRestriction 准入插件检查）
    if verb != "create" && attrs.GetName() != nodeName { ... }
    return authorizer.DecisionAllow, "", nil
}
```

**Node 对象本身**（`AuthorizeNodeWithSelectors` 启用时）：
```go
func (r *NodeAuthorizer) authorizeNode(nodeName string, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
    // create/update/patch：放行，由 NodeRestriction 准入插件限制只能操作自己的对象
    // get/list/watch：只允许读自己的 Node 对象（名字必须匹配）
    // status update/patch：放行，由准入插件限制
}
```

**Pod list/watch**（`AuthorizeNodeWithSelectors` 启用时）：
```go
func (r *NodeAuthorizer) authorizePod(nodeName string, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
    case "list", "watch":
        // 允许带 spec.nodeName=<self> fieldSelector 的请求
        reqs, _ := attrs.GetFieldSelector()
        for _, req := range reqs {
            if req.Field == "spec.nodeName" && req.Operator == selection.Equals && req.Value == nodeName {
                return authorizer.DecisionAllow, "", nil
            }
        }
        // 或读单个已知相关的 Pod
        if attrs.GetName() != "" {
            return r.authorize(nodeName, podVertexType, attrs)
        }
        // 否则拒绝
}
```

这就是 `AuthorizeWithSelectors` feature gate 的价值：授权器能感知选择器，才能精确判断"这个 list 请求是否被限制在本节点范围内"。

#### 5.4.5 静态兜底规则

对于不走图检查的资源（events、nodes 自身、CSR 等），Node 授权器用 `bootstrappolicy.NodeRules()` 提供的静态规则兜底（`plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go`）：

```go
func NodeRules() []rbacv1.PolicyRule {
    return []rbacv1.PolicyRule{
        // 创建 TokenReview/SubjectAccessReview（非变更性检查）
        NewRule("create").Groups("authentication.k8s.io").Resources("tokenreviews"),
        NewRule("create").Groups("authorization.k8s.io").Resources("subjectaccessreviews", "localsubjectaccessreviews"),
        // 注册和更新自身 Node 对象
        NewRule("create", "get", "list", "watch").Groups("").Resources("nodes"),
        NewRule("update", "patch").Groups("").Resources("nodes/status", "nodes"),
        // 事件
        NewRule("create", "update", "patch").Groups("", "events.k8s.io").Resources("events"),
        // 节点租约
        NewRule("get", "create", "update", "patch", "delete").Groups("coordination.k8s.io").Resources("leases"),
        // 创建 SA token（由 Node 授权器 + 准入插件进一步限缩）
        NewRule("create").Groups("").Resources("serviceaccounts/token"),
        // ...
    }
}
```

#### 5.4.6 与 NodeRestriction 准入插件的分工

Node 授权器的一个重要设计是：**它只做粗粒度放行，细粒度限制交给准入插件**。例如：

- Node 授权器允许节点 `create pods`（因为它需要创建镜像 Pod）
- NodeRestriction 准入插件在请求真正执行前检查：创建的 Pod 必须是镜像 Pod 且绑定到本节点

这样分工的原因是授权阶段拿不到请求体（请求体在进入 REST Handler 前不被解析），无法检查 Pod 的具体内容；而准入阶段可以完整校验对象。

### 5.5 ABAC 授权器

**源码**：`pkg/auth/authorizer/abac/abac.go`

ABAC（Attribute-Based Access Control）是 Kubernetes 最早的授权机制，使用 JSONL 策略文件：

```json
{"apiVersion": "abac.authorization.k8s.io/v1beta1", "kind": "Policy", "spec": {"user": "admin", "namespace": "*", "resource": "*"}}
{"apiVersion": "abac.authorization.k8s.io/v1beta1", "kind": "Policy", "spec": {"group": "system:serviceaccounts", "readonly": true}}
```

匹配逻辑：

```go
func matches(p abac.Policy, a authorizer.Attributes) bool {
    return subjectMatches(p, a.GetUser()) && verbMatches(p, a) &&
        (resourceMatches(p, a) || nonResourceMatches(p, a))
}

func (pl PolicyList) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
    for _, p := range pl {
        if matches(*p, a) {
            return authorizer.DecisionAllow, "", nil
        }
    }
    return authorizer.DecisionNoOpinion, "No policy matched.", nil
}
```

ABAC 的局限性明显：策略是静态文件，修改需要重启 apiserver；策略表达能力有限（只有 user/group/readonly/namespace/resource 等字段）；无法表达复杂的角色概念。生产环境已被 RBAC 取代，仅在少数遗留场景使用。

### 5.6 Webhook 授权器

**源码**：`staging/src/k8s.io/apiserver/plugin/pkg/authorizer/webhook/webhook.go`

Webhook 授权器把授权决策委托给外部服务，通过 `SubjectAccessReview` API 交互：

```go
func (w *WebhookAuthorizer) Authorize(ctx context.Context, attr authorizer.Attributes) (authorizer.Decision, string, error) {
    // 1. 构建 SubjectAccessReview 请求
    r := &authorizationv1.SubjectAccessReview{}
    if user := attr.GetUser(); user != nil {
        r.Spec = authorizationv1.SubjectAccessReviewSpec{
            User:   user.GetName(),
            UID:    user.GetUID(),
            Groups: user.GetGroups(),
            Extra:  convertToSARExtra(user.GetExtra()),
        }
    }
    if attr.IsResourceRequest() {
        r.Spec.ResourceAttributes = resourceAttributesFrom(attr)
    } else {
        r.Spec.NonResourceAttributes = &authorizationv1.NonResourceAttributes{
            Path: attr.GetPath(), Verb: attr.GetVerb(),
        }
    }

    // 2. CEL matchConditions：不满足则跳过 webhook
    matches, err := w.match(ctx, r)
    if err != nil {
        return w.decisionOnError, "", err   // 按 failurePolicy 处理
    }
    if !matches {
        return authorizer.DecisionNoOpinion, "", nil   // 跳过，交给下一个
    }

    // 3. 缓存查找
    key, _ := json.Marshal(r.Spec)
    if entry, ok := w.responseCache.Get(string(key)); ok {
        r.Status = entry.(authorizationv1.SubjectAccessReviewStatus)
    } else {
        // 4. 指数退避调用 webhook
        webhook.WithExponentialBackoff(ctx, w.retryBackoff, func() error {
            result, statusCode, sarErr = w.subjectAccessReview.Create(ctx, r, metav1.CreateOptions{})
            // 记录 metrics...
            return sarErr
        }, webhook.DefaultShouldRetry)

        r.Status = result.Status
        // 5. 按 TTL 缓存结果
        if shouldCache(attr) {
            if r.Status.Allowed {
                w.responseCache.Add(string(key), r.Status, w.authorizedTTL)
            } else {
                w.responseCache.Add(string(key), r.Status, w.unauthorizedTTL)
            }
        }
    }

    // 6. 三态转换
    switch {
    case r.Status.Denied && r.Status.Allowed:
        return authorizer.DecisionDeny, r.Status.Reason,
            fmt.Errorf("webhook subject access review returned both allow and deny response")
    case r.Status.Denied:
        return authorizer.DecisionDeny, r.Status.Reason, nil
    case r.Status.Allowed:
        return authorizer.DecisionAllow, r.Status.Reason, nil
    default:
        return authorizer.DecisionNoOpinion, r.Status.Reason, nil
    }
}
```

关键设计点：

**缓存策略**：成功授权结果缓存 `authorizedTTL`（默认 5m），拒绝结果缓存 `unauthorizedTTL`（默认 30s）。拒绝缓存更短，因为用户可能很快获得新权限，需要更快生效。

**DoS 缓解**（`shouldCache`）：

```go
const maxControlledAttrCacheSize = 10000

func shouldCache(attr authorizer.Attributes) bool {
    controlledAttrSize := int64(len(attr.GetNamespace())) +
        int64(len(attr.GetVerb())) +
        int64(len(attr.GetAPIGroup())) +
        int64(len(attr.GetAPIVersion())) +
        int64(len(attr.GetResource())) +
        int64(len(attr.GetSubresource())) +
        int64(len(attr.GetName())) +
        int64(len(attr.GetPath()))
    return controlledAttrSize < maxControlledAttrCacheSize
}
```

请求者可控的属性字段总长度超过 10000 时不缓存——防止攻击者用超长 name 字段撑爆 LRU 缓存。

**failurePolicy**：`NoOpinion`（默认，fail-open）时 webhook 调用失败返回 `DecisionNoOpinion`，请求交给链上后续授权器；`Deny`（fail-close）时返回 `DecisionDeny`。安全敏感的场景（如只想用 Webhook 做授权）应该配置 `Deny`。

**matchConditions**：CEL 表达式在调用 webhook 前求值，不满足则直接跳过。这让 webhook 只处理它关心的请求子集，减少调用开销。例如只对 `pods/exec` 子资源调用外部授权服务。

**Webhook 返回的三态语义**：`allowed: true` → Allow；`denied: true` → Deny；两者都 false → NoOpinion（交给下一个授权器）。`denied` 与 `allowed` 同时为 true 视为非法响应，返回错误。

### 5.7 聚合 API 服务器的委托授权

扩展 API 服务器（如 metrics-server）不做本地授权，而是通过 `DelegatingAuthorizerConfig`（`staging/src/k8s.io/apiserver/pkg/authorization/authorizerfactory/delegating.go`）把授权委托回主 kube-apiserver：

```go
func (c DelegatingAuthorizerConfig) New() (authorizer.Authorizer, error) {
    return webhook.NewFromInterface(
        c.SubjectAccessReviewClient,   // 对主 apiserver 的 SAR 客户端
        c.AllowCacheTTL,
        c.DenyCacheTTL,
        *c.WebhookRetryBackoff,
        authorizer.DecisionNoOpinion,
        NewDelegatingAuthorizerMetrics(),
        compiler,
    )
}
```

本质上就是一个配置好的 Webhook 授权器，SAR 请求发往主 apiserver。这与认证侧的 `DelegatingAuthenticatorConfig`（委托 TokenReview）是对称设计。

---

## 6. 授权配置的热加载

**源码**：`pkg/kubeapiserver/authorizer/reload.go`

使用 `--authorization-config` 配置文件时，kube-apiserver 支持不重启热加载授权链：

```go
func (r *reloadableAuthorizerResolver) runReload(ctx context.Context) {
    metrics.RegisterMetrics()
    filesystem.WatchUntil(ctx, r.reloadInterval /* 1 分钟 */,
        r.initialConfig.ReloadFile,
        func() { r.checkFile(ctx) },
        func(err error) { klog.ErrorS(err, "watching authorization config file") })
}

func (r *reloadableAuthorizerResolver) checkFile(ctx context.Context) {
    r.lastLoadedLock.Lock()
    defer r.lastLoadedLock.Unlock()

    data, err := os.ReadFile(r.initialConfig.ReloadFile)
    if bytes.Equal(data, r.lastReadData) {
        return   // 内容未变
    }
    r.lastReadData = data

    // 重新加载并校验
    config, err := LoadAndValidateData(data, r.compiler, r.requireNonWebhookTypes)
    if err != nil { return }   // 校验失败保留旧配置
    if reflect.DeepEqual(config, r.lastLoadedConfig) {
        return
    }

    // 重建授权链
    authorizer, ruleResolver, err := r.newForConfig(config)
    if err != nil { return }

    // 原子切换
    r.current.Store(&authorizerResolver{authorizer: authorizer, ruleResolver: ruleResolver})
    klog.InfoS("reloaded authz config")
}
```

关键约束：

1. **原子切换**：`atomic.Pointer` 存储当前授权链，`Authorize` 每次 `Load()` 最新指针。切换瞬间在飞的请求继续使用旧链，不会看到半构造状态
2. **非 Webhook 类型不可动态增删**：Node/RBAC/ABAC 授权器依赖启动时建立的 informer 和图，无法热添加。重载时如果配置文件缺少初始启动时的非 Webhook 类型，校验直接失败（`requireNonWebhookTypes`）
3. **Webhook 可以重复和热更**：Webhook 授权器在每次 `newForConfig` 时完整重建，可以配置多个，也可以动态调整数量和参数
4. **失败保留旧配置**：文件解析或校验失败时，仅记录指标，继续使用当前授权链——错误配置不会导致 apiserver 授权瘫痪

`Authorize` 的调用路径极短：

```go
func (r *reloadableAuthorizerResolver) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
    return r.current.Load().authorizer.Authorize(ctx, a)
}
```

`reloadableAuthorizerResolver` 同时实现 `Authorizer` 和 `RuleResolver` 两个接口，对外表现为一个普通的授权器。

---

## 7. 授权链完整数据流

以 `kubectl get pods -n default` （普通用户 alice，通过客户端证书认证）为例：

```
1. 请求经过认证：alice 已在 context 中
   经过审计、APF 限流
        ↓
2. WithAuthorization 过滤器
   │
   ├─ GetAuthorizerAttributes(ctx)
   │    User: alice
   │    Verb: get, Resource: pods, Namespace: default
   │
   ├─ a.Authorize(ctx, attributes)  ← union 链
   │    │
   │    ├─ 1. system:masters 授权器
   │    │     alice 不在 system:masters 组 → NoOpinion
   │    │
   │    ├─ 2. Node 授权器
   │    │     NodeIdentity(alice) → isNode=false → NoOpinion
   │    │
   │    ├─ 3. RBAC 授权器
   │    │     VisitRulesFor(alice, "default")
   │    │     ├─ 扫描所有 ClusterRoleBinding：alice 不是任何 CRB 的 subject → 跳过
   │    │     ├─ 扫描 default 命名空间的 RoleBinding：
   │    │     │   RoleBinding "pod-reader" 引用 Role "pod-reader"，subject 是 User "alice"
   │    │     │   解析 Role "pod-reader" 的规则：
   │    │     │   {Verbs: [get,list,watch], Resources: [pods]}
   │    │     │   RuleAllows(attrs, rule) → VerbMatches("get") ✓
   │    │     │                          → APIGroupMatches("") ✓
   │    │     │                          → ResourceMatches("pods") ✓
   │    │     │                          → ResourceNameMatches("") ✓ (规则未限定)
   │    │     └─ visitor 返回 allowed=true，短路
   │    │     → DecisionAllow, "RBAC: allowed by RoleBinding \"pod-reader/default\" of Role \"pod-reader\" to User \"alice\""
   │    │
   │    └─ union 短路返回 Allow
   │
   ├─ DecisionAllow → 审计注解：decision=allow, reason=RBAC: allowed by ...
   │
   └─ handler.ServeHTTP → REST Handler → etcd → 200 OK
```

如果 alice 访问的是 `secrets`，RBAC 遍历完所有规则没有匹配，返回 `NoOpinion`；union 走完全部授权器都无 `Allow`，过滤器写入 `decision=forbid` 审计注解并返回 403 Forbidden。
