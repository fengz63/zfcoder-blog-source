---
title: kube-apiserver 源码解读（四）认证机制：从 TLS 握手到用户身份确认
date: 2026-06-27 10:20:00
tags:
  - 技术
  - Kubernetes源码解读
categories: [技术]
---

## 1. 引言

在上一篇中，我们完整追踪了 kube-apiserver 的 HTTP 请求处理链。当请求进入过滤器链后，遇到的第一个关键安全关卡就是**认证（Authentication）**。

k8s 的认证设计具有鲜明的特点：

- **多机制并行**：同时支持 X509 客户端证书、Bearer Token、Webhook、OIDC 等多种认证方式，任一成功即通过
- **链式处理**：认证器按优先级组成链条，每个认证器只检查自己能识别的凭证
- **无侵入认证**：认证层只看到 HTTP 头部，不解析请求体
- **安全清理**：认证成功后立即清除认证相关的 HTTP 头部，防止后续组件被欺骗

<!--more-->

本文将从认证的核心接口开始，逐步深入到每个认证器实现，最后串联起完整的认证链路。

---

## 2. 认证核心接口与数据模型

### 2.1 三层接口

认证系统定义了三层核心接口，位于 `staging/src/k8s.io/apiserver/pkg/authentication/authenticator/interfaces.go`：

```go
// token 认证器：验证一个不透明的 token 字符串
type Token interface {
    AuthenticateToken(ctx context.Context, token string) (*Response, bool, error)
}

// request 认证器：从 HTTP 请求中提取并验证凭证
type Request interface {
    AuthenticateRequest(req *http.Request) (*Response, bool, error)
}

// 认证成功后的响应
type Response struct {
    Audiences Audiences   // token 的受众（audience）
    User      user.Info   // 用户信息
}
```

每个认证方法返回三个值：
- `*Response`：认证成功时包含用户信息和受众
- `bool`：是否成功。`false` 表示"我不认识这个凭证，请下一个认证器试试"
- `error`：表示认证过程遇到了错误（如网络不通），而不是认证失败

理解认证器的返回值是理解整个认证链的关键：

| 返回值 | 语义 | 处理方式 |
|------------------------|------|---------|
| `(*Response, true, nil)` | **认证成功** | 立即停止链，使用该用户身份 |
| `(nil, false, nil)` | **无法识别**（不是我能处理的凭证格式） | 继续尝试下一个认证器 |
| `(nil, false, err)` | **处理出错** | 默认模式下：记录错误，继续下一个<br>FailOnError 模式下：立即停止 |

这种设计让不同认证器可以"和平共处"——X509 认证器看到没有 TLS 连接就直接返回 `(nil, false, nil)`，Bearer Token 认证器看到没有 `Authorization: Bearer` 头也返回同样的值。只有"我能处理但凭证无效"才会返回错误。

### 2.2 用户信息模型

`user.Info` 接口定义在 `staging/src/k8s.io/apiserver/pkg/authentication/user/user.go`：

```go
type Info interface {
    GetName() string              // 用户名，如 "admin"、"system:apiserver"
    GetUID() string               // 用户唯一 ID
    GetGroups() []string          // 用户所属组
    GetExtra() map[string][]string // 额外信息（如凭证 ID）
}
```

内置的 `DefaultInfo` 结构体实现了该接口。

Kubernetes 预定义了一些特殊用户和组：

| 常量 | 值 | 用途 |
|------|-----|------|
| `user.Anonymous` | `system:anonymous` | 匿名请求 |
| `user.AllAuthenticated` | `system:authenticated` | 所有已认证用户自动加入的组 |
| `user.AllUnauthenticated` | `system:unauthenticated` | 未认证用户 |
| `user.SystemPrivilegedGroup` | `system:masters` | 超级管理员组 |
| `user.NodesGroup` | `system:nodes` | 所有 Node 节点 |
| `user.APIServerUser` | `system:apiserver` | API Server 自身身份 |
| `user.KubeProxy` | `system:kube-proxy` | kube-proxy 身份 |
| `user.KubeControllerManager` | `system:kube-controller-manager` | Controller Manager 身份 |
| `user.KubeScheduler` | `system:kube-scheduler` | Scheduler 身份 |

---

## 3. 认证过滤器：WithAuthentication

认证作为 HTTP 处理链中的一个中间件，实现在 `staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go`。

### 3.1 过滤器位置

在 `DefaultBuildHandlerChain` 中，认证过滤器的位置是：

```go
// staging/src/k8s.io/apiserver/pkg/server/config.go
handler = genericapifilters.WithAuthentication(handler, ...)
```

它处于整个处理链的中层，在 `WithCORS` 之后、`WithTracing` 之前。具体来说，请求到达认证过滤器时，已经完成了以下前置处理：
- `WithPanicRecovery` — panic 保护
- `WithRequestInfo` — URL 解析为 RequestInfo
- `WithTimeoutForNonLongRunningRequests` — 超时控制
- `WithCORS` — 跨域处理

尚未执行的是：
- `WithTracing` — 链路追踪（在认证之后，因为认证结果可以影响采样）
- `WithAudit` — 审计
- `WithImpersonation` — 模拟请求
- `WithPriorityAndFairness` — 限流
- `WithAuthorization` — 授权

### 3.2 过滤器完整实现

```go
// staging/src/k8s.io/apiserver/pkg/endpoints/filters/authentication.go
func withAuthentication(handler http.Handler, auth authenticator.Request,
    failed http.Handler, apiAuds authenticator.Audiences,
    requestHeaderConfig *authenticatorfactory.RequestHeaderConfig) http.Handler {

    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        // 1. 将 API 受众注入请求 context
        if len(apiAuds) > 0 {
            req = req.WithContext(authenticator.WithAudiences(req.Context(), apiAuds))
        }

        // 2. 执行认证
        resp, ok, err := auth.AuthenticateRequest(req)

        // 3. 认证失败 → 交给 failed handler（返回 401）
        if err != nil || !ok {
            failed.ServeHTTP(w, req)
            return
        }

        // 4. 检查 audience 交集
        if !audiencesAreAcceptable(apiAuds, resp.Audiences) {
            failed.ServeHTTP(w, req)
            return
        }

        // 5. ⭐ 安全关键：清除所有认证相关的 HTTP 头部
        req.Header.Del("Authorization")                          // Bearer token
        headerrequest.ClearAuthenticationHeaders(                // 标准 front-proxy 头部
            req.Header,
            standardRequestHeaderConfig.UsernameHeaders,
            standardRequestHeaderConfig.UIDHeaders,
            standardRequestHeaderConfig.GroupHeaders,
            standardRequestHeaderConfig.ExtraHeaderPrefixes,      // 自定义 front-proxy 头部
        )
        if requestHeaderConfig != nil {
            headerrequest.ClearAuthenticationHeaders(...)
        }

        // 6. ⭐ HTTP/2 DOS 缓解：未认证用户断开连接
        if utilfeature.DefaultFeatureGate.Enabled(
                genericfeatures.UnauthenticatedHTTP2DOSMitigation) &&
            req.ProtoMajor == 2 && isAnonymousUser(resp.User) {
            w.Header().Set("Connection", "close")
        }

        // 7. 将用户信息存入 context，传递给后续过滤器
        req = req.WithContext(genericapirequest.WithUser(req.Context(), resp.User))
        handler.ServeHTTP(w, req)
    })
}
```

**设计思想解读**：

- **"清除认证头"是安全关键步骤**：认证成功后，`Authorization`、`X-Remote-User` 等头部被立即删除。后续的授权、审计、准入控制等模块只能从 `context.Context` 中获取用户信息，无法被恶意设置的 HTTP 头部欺骗

- **两次清除 front-proxy 头部**：先清除标准头部（`X-Remote-User` 等 4 个），再清除自定义头部（用户通过 `--requestheader-*` 配置的）。双重保证

- **HTTP/2 DOS 缓解**：`CVE-2023-44487`（Rapid Reset 攻击）后引入。匿名用户的 HTTP/2 连接在单次请求后就被关闭，防止大量恶意请求利用同一个 HTTP/2 连接

- **Audience 检查**：确保 token 的 audience 与 API server 声明的 audience 有交集，防止 token 被重放到错误的服务器

### 3.3 Failed Handler

认证失败时由独立的 `Unauthorized` handler 处理（`authentication.go:127`）：

```go
func Unauthorized(s runtime.NegotiatedSerializer) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        // 同样：匿名 HTTP/2 请求断开连接
        if utilfeature.DefaultFeatureGate.Enabled(
                genericfeatures.UnauthenticatedHTTP2DOSMitigation) &&
            req.ProtoMajor == 2 {
            w.Header().Set("Connection", "close")
        }
        // 返回 401 Unauthorized
        responsewriters.ErrorNegotiated(
            apierrors.NewUnauthorized("Unauthorized"), s, gv, w, req)
    })
}
```

---

## 4. 认证链的组装

### 4.1 总览

kube-apiserver 的认证链在 `pkg/kubeapiserver/authenticator/config.go:107` 的 `Config.New()` 方法中组装。完整的认证器装配顺序如下：

```
Request 认证器链（从先到后尝试）
│
├── 1. Front-Proxy（Request Header）
│     从 X-Remote-User/X-Remote-Group 等头部读取身份
│     要求前端代理通过 mTLS 连接
│
├── 2. X509 Client Certificate
│     验证客户端证书 → CN → 用户名, Organization → 组
│
├── 3. Bearer Token（内部再 Union 多个 Token 认证器）
│   │
│   ├── 3a. Static Token File (--token-auth-file)
│   ├── 3b. Legacy Service Account（secret-based, 已弃用但兼容）
│   ├── 3c. Projected Service Account（TokenRequest API）
│   ├── 3d. Bootstrap Token（--enable-bootstrap-token-auth）
│   ├── 3e. OIDC / JWT（--oidc-issuer-url 或 --authentication-config）
│   └── 3f. Webhook TokenReview（--authentication-token-webhook-config-file）
│
├── 4. AuthenticatedGroupAdder（自动添加 system:authenticated 组）
│
└── 5. Anonymous（Fallback，--anonymous-auth=true 时）
```

### 4.2 组装代码全览

`pkg/kubeapiserver/authenticator/config.go` 中的 `New()` 方法约 150 行，按以下步骤组装：

```go
func (config Config) New(serverLifecycle context.Context) (authenticator.Request, ...) {
    var authenticators []authenticator.Request
    var tokenAuthenticators []authenticator.Token

    // 第一步：Front-Proxy（Request Header）
    if config.RequestHeaderConfig != nil {
        requestHeaderAuthenticator := headerrequest.NewDynamicVerifyOptionsSecure(
            config.RequestHeaderConfig.CAContentProvider.VerifyOptions,
            config.RequestHeaderConfig.AllowedClientNames,
            config.RequestHeaderConfig.UsernameHeaders,
            config.RequestHeaderConfig.UIDHeaders,
            config.RequestHeaderConfig.GroupHeaders,
            config.RequestHeaderConfig.ExtraHeaderPrefixes,
        )
        authenticators = append(authenticators,
            authenticator.WrapAudienceAgnosticRequest(config.APIAudiences, requestHeaderAuthenticator))
    }

    // 第二步：X509 客户端证书
    if config.ClientCAContentProvider != nil {
        certAuth := x509.NewDynamic(config.ClientCAContentProvider.VerifyOptions,
            x509.CommonNameUserConversion)
        authenticators = append(authenticators, certAuth)
    }

    // 第三步：收集所有 Token 认证器
    // 3a. 静态 Token 文件
    if len(config.TokenAuthFile) > 0 {
        tokenAuth, _ := newAuthenticatorFromTokenFile(config.TokenAuthFile)
        tokenAuthenticators = append(tokenAuthenticators,
            authenticator.WrapAudienceAgnosticToken(config.APIAudiences, tokenAuth))
    }
    // 3b. 遗留 Service Account
    if config.ServiceAccountPublicKeysGetter != nil {
        serviceAccountAuth, _ := newLegacyServiceAccountAuthenticator(...)
        tokenAuthenticators = append(tokenAuthenticators, serviceAccountAuth)
    }
    // 3c. 新版 Service Account（TokenRequest）
    if len(config.ServiceAccountIssuers) > 0 && config.ServiceAccountPublicKeysGetter != nil {
        serviceAccountAuth, _ := newServiceAccountAuthenticator(...)
        tokenAuthenticators = append(tokenAuthenticators, serviceAccountAuth)
    }
    // 3d. Bootstrap Token
    if config.BootstrapToken && config.BootstrapTokenAuthenticator != nil {
        tokenAuthenticators = append(tokenAuthenticators,
            authenticator.WrapAudienceAgnosticToken(config.APIAudiences,
                config.BootstrapTokenAuthenticator))
    }
    // 3e. OIDC（注释说：放在 Service Account 之后，
    //     因为两者都处理 JWT，OIDC 需要查询 provider 更新密钥，放后面减少缓存争用）
    if config.AuthenticationConfig != nil {
        jwtAuthenticator, _ := newJWTAuthenticator(...)
        tokenAuthenticators = append(tokenAuthenticators,
            authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
                return jwtAuthenticator.Load().jwtAuthenticator.AuthenticateToken(ctx, token)
            }))
    }
    // 3f. Webhook TokenReview
    if len(config.WebhookTokenAuthnConfigFile) > 0 {
        webhookTokenAuth, _ := newWebhookTokenAuthenticator(config)
        tokenAuthenticators = append(tokenAuthenticators, webhookTokenAuth)
    }

    // 合并所有 Token 认证器 + 缓存
    if len(tokenAuthenticators) > 0 {
        tokenAuth := tokenunion.New(tokenAuthenticators...)
        if config.TokenSuccessCacheTTL > 0 || config.TokenFailureCacheTTL > 0 {
            tokenAuth = tokencache.New(tokenAuth, true,
                config.TokenSuccessCacheTTL, config.TokenFailureCacheTTL)
        }
        // 通过 Bearer Token 和 WebSocket 协议暴露
        authenticators = append(authenticators,
            bearertoken.New(tokenAuth),
            websocket.NewProtocolAuthenticator(tokenAuth))
    }

    // 第四步：包装最终链
    // Union 所有 Request 认证器
    authenticator := union.New(authenticators...)
    // 自动添加 system:authenticated 组
    authenticator = group.NewAuthenticatedGroupAdder(authenticator)
    // 如果启用了匿名认证，将 Anonymous 放在最后
    if config.Anonymous.Enabled {
        authenticator = union.NewFailOnError(authenticator,
            anonymous.NewAuthenticator(config.Anonymous.Conditions))
    }
    return authenticator, ...
}
```

### 4.3 Token 认证器的 Union

Token 认证器的组合使用 `token/union/union.go`，与 Request 认证器的 `request/union/union.go` 完全相同的模式：

```go
type unionAuthTokenHandler struct {
    Handlers    []authenticator.Token
    FailOnError bool
}

func (authHandler *unionAuthTokenHandler) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
    for _, currAuthHandler := range authHandler.Handlers {
        resp, ok, err := currAuthHandler.AuthenticateToken(ctx, token)
        if err != nil && authHandler.FailOnError {
            return resp, ok, err
        }
        if ok {
            return resp, ok, err
        }
    }
    return nil, false, utilerrors.NewAggregate(errlist)
}
```

### 4.4 共享的 DelegatingAuthenticatorConfig

对于聚合 API 服务器（Aggregator），认证配置使用 `staging/src/k8s.io/apiserver/pkg/authentication/authenticatorfactory/delegating.go` 中的 `DelegatingAuthenticatorConfig`。它与 `kubeauthenticator.Config` 的区别：

| 特性 | kubeauthenticator.Config | DelegatingAuthenticatorConfig |
|------|--------------------------|-------------------------------|
| 适用范围 | kube-apiserver 主服务器 | 聚合 API 服务器 |
| Token 认证 | 多种（文件/SA/OIDC/Webhook/...） | 仅 Webhook TokenReview |
| X509 | 直接验证客户端证书 | 直接验证客户端证书 |
| Front-Proxy | 支持 | 支持 |
| 缓存 | 可配置 TTL | 可配置 TTL |
| 匿名 | 支持 | 支持 |

`DelegatingAuthenticatorConfig.New()` 的代码更简洁：

```go
func (c DelegatingAuthenticatorConfig) New() (authenticator.Request, ...) {
    // 1. Front-Proxy
    if c.RequestHeaderConfig != nil { ... }
    // 2. X509 客户端证书
    if c.ClientCertificateCAContentProvider != nil { ... }
    // 3. Webhook TokenReview（唯一的 Token 认证方式）
    if c.TokenAccessReviewClient != nil {
        tokenAuth, _ := webhooktoken.NewFromInterface(...)
        cachingTokenAuth := cache.New(tokenAuth, false, c.CacheTTL, c.CacheTTL)
        authenticators = append(authenticators, bearertoken.New(cachingTokenAuth), ...)
    }
    // 4. 包装 AuthenticatedGroupAdder + Anonymous
    return authenticator, ...
}
```

---

## 5. 认证器逐一详解

### 5.1 Front-Proxy（Request Header）认证器

**源码**：`staging/src/k8s.io/apiserver/pkg/authentication/request/headerrequest/requestheader.go`

**配置参数**：
- `--requestheader-client-ca-file`：前端代理的 CA 证书（强制 mTLS）
- `--requestheader-allowed-names`：允许的客户端证书 CN（空则全部允许）
- `--requestheader-username-headers`：用户名头部（默认 `X-Remote-User`）
- `--requestheader-uid-headers`：UID 头部（默认 `X-Remote-Uid`）
- `--requestheader-group-headers`：组头部（默认 `X-Remote-Group`）
- `--requestheader-extra-headers-prefix`：额外信息前缀（默认 `X-Remote-Extra-`）

**运作方式**：

```
用户 → 前端代理（如 nginx/HAProxy）→ kube-apiserver
                          │
                          ├── 代理与 apiserver 之间建立 mTLS
                          ├── 代理验证用户身份后，设置：
                          │   X-Remote-User: alice
                          │   X-Remote-Group: devs
                          │   X-Remote-Group: admins
                          │
                          └── apiserver 验证代理的客户端证书
                              然后从头部读取用户信息
```

**核心实现**：

```go
// 包装：mTLS 验证 + 头部读取
func NewDynamicVerifyOptionsSecure(
    verifyOptionFn x509request.VerifyOptionFunc,
    proxyClientNames, nameHeaders, uidHeaders,
    groupHeaders, extraHeaderPrefixes StringSliceProvider,
) authenticator.Request {
    // 1. 先创建头部读取认证器
    headerAuthenticator := NewDynamic(nameHeaders, uidHeaders, groupHeaders, extraHeaderPrefixes)
    // 2. 用 x509 Verifier 包裹：先验证客户端证书，再读取头部
    return x509request.NewDynamicCAVerifier(verifyOptionFn, headerAuthenticator, proxyClientNames)
}
```

头部读取部分：

```go
func (a *requestHeaderAuthRequestHandler) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
    name := headerValue(req.Header, a.nameHeaders.Value())
    if len(name) == 0 {
        return nil, false, nil  // 没有用户身份头部 → 跳过
    }
    uid := headerValue(req.Header, a.uidHeaders.Value())
    groups := allHeaderValues(req.Header, a.groupHeaders.Value())
    extra := newExtra(req.Header, a.extraHeaderPrefixes.Value())

    // 清除头部
    ClearAuthenticationHeaders(req.Header, a.nameHeaders, a.uidHeaders,
        a.groupHeaders, a.extraHeaderPrefixes)

    return &authenticator.Response{
        User: &user.DefaultInfo{
            Name: name, UID: uid, Groups: groups, Extra: extra,
        },
    }, true, nil
}
```

### 5.2 X509 客户端证书认证器

**源码**：`staging/src/k8s.io/apiserver/pkg/authentication/request/x509/x509.go`

**配置参数**：`--client-ca-file`

**运作方式**：

客户端在 TLS 握手时出示证书 → apiserver 用 `--client-ca-file` 中的 CA 验证证书 → 提取用户信息。

**核心实现**：

```go
type Authenticator struct {
    verifyOptionsFn VerifyOptionFunc   // 动态加载 CA（可热更新）
    user            UserConversion     // 从证书提取用户信息
}

func (a *Authenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
    if req.TLS == nil || len(req.TLS.PeerCertificates) == 0 {
        return nil, false, nil  // 没有 TLS 或没有客户端证书 → 跳过
    }

    optsCopy, ok := a.verifyOptionsFn()
    if !ok { return nil, false, nil }

    // 设置中间证书
    if optsCopy.Intermediates == nil && len(req.TLS.PeerCertificates) > 1 { ... }

    // 记录证书剩余有效期到 Prometheus 指标
    remaining := req.TLS.PeerCertificates[0].NotAfter.Sub(time.Now())
    clientCertificateExpirationHistogram.Observe(remaining.Seconds())

    // 验证证书链
    chains, err := req.TLS.PeerCertificates[0].Verify(optsCopy)
    if err != nil { return nil, false, err }

    // 转换为用户信息
    for _, chain := range chains {
        user, ok, err := a.user.User(chain)
        if ok { return user, ok, err }
    }
    return nil, false, utilerrors.NewAggregate(errlist)
}
```

**用户信息提取**——`CommonNameUserConversion`：

```go
var CommonNameUserConversion = UserConversionFunc(func(chain []*x509.Certificate) (*authenticator.Response, bool, error) {
    if len(chain[0].Subject.CommonName) == 0 {
        return nil, false, nil  // 没有 CN → 无法提取身份
    }

    // 计算凭证指纹作为 CredentialID
    fp := sha256.Sum256(chain[0].Raw)
    id := "X509SHA256=" + hex.EncodeToString(fp[:])

    // 从 UID OID（1.3.6.1.4.1.14677.1.1.1）解析 UID
    uid, _ := parseUIDFromCert(chain[0])

    return &authenticator.Response{
        User: &user.DefaultInfo{
            Name:   chain[0].Subject.CommonName,    // CN → 用户名
            UID:    uid,
            Groups: chain[0].Subject.Organization,   // O → 组
            Extra:  map[string][]string{
                user.CredentialIDKey: {id},
            },
        },
    }, true, nil
})
```

与前端代理 Verifier 的关系：`Verifier` 和 `Authenticator` 是两个不同的类型。前者只验证证书合法性然后委托给另一个认证器（Front-Proxy 场景），后者直接提取用户信息。

### 5.3 Bearer Token 认证器

**源码**：`staging/src/k8s.io/apiserver/pkg/authentication/request/bearertoken/bearertoken.go`

这是最常用的认证方式。它将 `Authorization: Bearer <token>` 头部中的 token 提取出来，委托给内部的 `authenticator.Token` 链：

```go
func (a *Authenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
    auth := strings.TrimSpace(req.Header.Get("Authorization"))
    if auth == "" { return nil, false, nil }

    parts := strings.SplitN(auth, " ", 3)
    if len(parts) < 2 || strings.ToLower(parts[0]) != "bearer" {
        return nil, false, nil
    }

    token := parts[1]
    if len(token) == 0 { return nil, false, nil }

    resp, ok, err := a.auth.AuthenticateToken(req.Context(), token)
    // 认证成功后删除 Authorization 头
    if ok { req.Header.Del("Authorization") }
    return resp, ok, err
}
```

**内部 Token 认证器的 6 种实现**：

### 5.3a 静态 Token 文件

**源码**：`staging/src/k8s.io/apiserver/pkg/authentication/token/tokenfile/tokenfile.go`

**配置参数**：`--token-auth-file`

```go
type TokenAuthenticator struct {
    tokens map[string]*user.DefaultInfo
}

func NewCSV(path string) (*TokenAuthenticator, error) {
    // CSV 格式：token,username,useruid[,group1,group2,...]
    reader := csv.NewReader(file)
    for {
        record, err := reader.Read()
        // record[0] → token, record[1] → username,
        // record[2] → uid, record[3] → comma-separated groups
    }
}

func (a *TokenAuthenticator) AuthenticateToken(ctx context.Context, value string) (*authenticator.Response, bool, error) {
    user, ok := a.tokens[value]
    if !ok { return nil, false, nil }  // 不在列表中
    return &authenticator.Response{User: user}, true, nil
}
```

### 5.3b & 5.3c Service Account 令牌认证

Service Account 是 Kubernetes 中最重要的身份类型之一。每个 Pod 都关联一个 Service Account，其令牌用于 Pod 与 API Server 之间的认证。

#### 新版 TokenRequest API（推荐方式）

**源码**：`pkg/serviceaccount/jwt.go`、`pkg/serviceaccount/claims.go`

使用 `TokenRequest` API 签发的 JWT 令牌，格式符合 OIDC 规范：

```go
type jwtTokenAuthenticator[PrivateClaims any] struct {
    issuers      map[string]bool          // 信任的签发者
    keysGetter   PublicKeysGetter          // 公钥获取器
    validator    Validator[PrivateClaims]  // 私有声明验证器
    implicitAuds authenticator.Audiences   // 隐式受众
}
```

JWT 令牌的结构如下（Payload）：

```json
{
  "iss": "https://kubernetes.default.svc.cluster.local",
  "aud": ["https://kubernetes.default.svc"],
  "exp": 1718000000,
  "kubernetes.io": {
    "namespace": "default",
    "serviceaccount": { "name": "my-sa", "uid": "..." },
    "pod": { "name": "my-pod", "uid": "..." },
    "warnafter": 1717900000
  }
}
```

认证流程：

```go
func (j *jwtTokenAuthenticator) AuthenticateToken(ctx context.Context, tokenData string) (*authenticator.Response, bool, error) {
    // 1. 快速 issuer 检查：解析未验证的 JWT，检查 iss 是否在信任列表中
    if !j.hasCorrectIssuer(tokenData) {
        return nil, false, nil
    }
    // 2. 解析并验证 JWT 签名
    tok, err := jwt.ParseSigned(tokenData)
    // 3. 通过 keysGetter 获取公钥验证签名
    // 4. audience 交集验证
    // 5. 调用 validator.Validate() 做领域逻辑验证
    //    - 检查 Service Account 是否还存在
    //    - 检查绑定的 Pod/Secret/Node 是否存在且 UID 匹配
    //    - 检查值是否过期
    //    - 检查 warnafter → 如果接近过期，记录审计警告
    sa, err := j.validator.Validate(ctx, tokenData, public, private)
    // 6. 返回用户信息
    return &authenticator.Response{User: sa.UserInfo(), Audiences: auds}, true, nil
}
```

`Validator.Validate()` 的具体逻辑：

```go
// pkg/serviceaccount/claims.go
func (v *validator) Validate(ctx context.Context, _ string, public *jwt.Claims, private *privateClaims) (*apiserverservicaccount.ServiceAccountInfo, error) {
    // 1. 验证时间声明（exp, nbf, iat）
    // 2. 验证 ServiceAccount 是否存在且 UID 匹配
    sa, err := v.getter.GetServiceAccount(private.Kubernetes.Namespace, private.Kubernetes.Svcacct.Name)
    if sa == nil || sa.UID != private.Kubernetes.Svcacct.UID {
        return nil, fmt.Errorf("service account not found or UID mismatch")
    }
    // 3. 如果绑定了 Pod，验证 Pod 是否存在且 UID 匹配
    // 4. 如果绑定了 Secret，验证 Secret 是否存在且 UID 匹配
    // 5. 如果绑定了 Node，验证 Node 是否存在且 UID 匹配
    // 6. 检查 warnafter：如果超过警告时间，在审计中记录 "token used before it becomes invalid"
    return &ServiceAccountInfo{...}, nil
}
```

#### 遗留 Secret 令牌（Legacy）

**源码**：`pkg/serviceaccount/legacy.go`

早于 `TokenRequest` 的机制，使用 Secret 中存储的 JWT 令牌。其主要工作流程：
1. 管理员或系统自动创建一个 `ServiceAccount` 和对应的 Secret
2. Secret 中包含一个 JWT 令牌（由 API Server 使用 `--service-account-key-file` 中的私钥签名）
3. Pod 挂载该 Secret 到 `/var/run/secrets/kubernetes.io/serviceaccount/token`

遗留验证器在 `Validate()` 中：
- 验证 `kubernetes.io/serviceaccount/secret.name` 声明
- 查找对应的 Secret 并验证其内容
- 验证 ServiceAccount 仍存在
- 检查 Secret 是否被标记为作废（`kubernetes.io/legacy-token-invalid-since` 标签）
- 记录 `last-used` 标签（用于追踪哪些 Secret 令牌仍在使用）

**为什么保留了两种机制？** 新版 `TokenRequest` 令牌有明确的过期时间且绑定到特定 Pod，安全性更高。但大量存量集群仍在使用 Secret 令牌，因此需要兼容。

### 5.3d Bootstrap Token

**源码**：`plugin/pkg/auth/authenticator/token/bootstrap/bootstrap.go`

**配置参数**：`--enable-bootstrap-token-auth`

用于 `kubeadm` 集群初始化时的 TLS 引导流程：

```go
func (t *TokenAuthenticator) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
    // 1. 解析 token 格式：<token-id>.<token-secret>
    tokenID, tokenSecret, err := bootstraptokenutil.ParseToken(token)
    if err != nil { return nil, false, nil }

    // 2. 查找 Secret：kube-system/bootstrap-token-<token-id>
    secret, err := t.lister.Get("bootstrap-token-" + tokenID)

    // 3. 验证：
    //    - Secret 类型是 "bootstrap.kubernetes.io/token"
    //    - token-secret 匹配（常量时间比较，防时序攻击）
    //    - token-id 匹配
    //    - 未过期
    //    - usage-bootstrap-authentication == "true"
    // 4. 读取 auth-extra-groups（额外组）

    return &authenticator.Response{
        User: &user.DefaultInfo{
            Name: "system:bootstrap:<token-id>",
            Groups: groups,  // 包含 system:bootstrappers
        },
    }, true, nil
}
```

### 5.3e OIDC / JWT 认证器

**源码**：`staging/src/k8s.io/apiserver/plugin/pkg/authenticator/token/oidc/oidc.go`

**配置参数**：
- `--oidc-issuer-url`（传统方式）
- `--authentication-config`（新方式，支持多个 OIDC provider 热更新）

OIDC 认证器是 Kubernetes 认证系统中最复杂的实现。以下是其核心架构：

```go
type jwtAuthenticator struct {
    jwtAuthenticator   apiserver.JWTAuthenticator  // 配置
    idTokenVerifier    atomic.Pointer[oidc.IDTokenVerifier]  // 可热更新的验证器
    claimResolver      *claimResolver              // 分布式声明解析
    hasCorrectIssuer   func(token string) bool     // 快速 issuer 检查
    // CEL 表达式引擎
    compiler             authenticationcel.Compiler
    claimValidationRules []cel.ClaimValidationCondition
    userValidationRules  []cel.UserValidationCondition
    // ...
}
```

**认证流程**：

```go
func (a *jwtAuthenticator) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
    // 1. 快速 issuer 检查（解析未验证的 JWT，检查 iss 声明）
    if !hasCorrectIssuer(a.jwtAuthenticator.Issuer.URL, token) {
        return nil, false, nil
    }

    // 2. 使用 OIDC IDTokenVerifier 验证签名和时间声明
    verifier, ok := a.idTokenVerifier()
    if !ok { return nil, false, fmt.Errorf("oidc: not initialized") }
    idToken, err := verifier.Verify(ctx, token)

    // 3. 提取声明
    var c claims
    idToken.Claims(&c)

    // 4. 解析分布式声明（groups 等可能在其他 URL 上）
    if a.resolver != nil { a.resolver.expand(ctx, c) }

    // 5. CEL 表达式映射：
    //    - usernameExpression → 用户名
    //    - groupsExpression → 组
    //    - uidExpression → UID
    //    - extraExpressions → 额外信息
    info.UserName = a.getUsername(ctx, c, claimsValue)
    info.Groups = a.getGroups(ctx, c, claimsValue)
    info.UID = a.getUID(ctx, c, claimsValue)
    info.Extra = a.getExtra(ctx, c, claimsValue)

    // 6. 必填声明验证（requiredClaims）
    // 7. CEL 声明验证规则（claimValidationRules）
    // 8. CEL 用户验证规则（userValidationRules）

    return &authenticator.Response{User: info}, true, nil
}
```

**OIDC 的异步初始化**：

```go
// 在 New() 中异步启动 provider 发现和 JWKS 获取
func New(ctx context.Context, opts Options) (*jwtAuthenticator, error) {
    jwta := &jwtAuthenticator{...}
    go jwta.runInit(ctx)  // 异步初始化
    return jwta, nil
}
```

**动态配置更新**：通过 `--authentication-config` 指定的配置文件热更新时，整个 OIDC 认证器会通过 `atomic.Pointer` 原子替换（`pkg/kubeapiserver/authenticator/config.go:306-367`）：

```go
func (c *authenticationConfigUpdater) updateAuthenticationConfig(ctx context.Context, authConfig *apiserver.AuthenticationConfiguration) error {
    // 1. 构建新的 JWT 认证器
    updatedJWTAuthenticator, _ := newJWTAuthenticator(...)
    // 2. 等待新认证器健康检查通过（最多等待到 context 超时）
    wait.PollUntilContextCancel(ctx, 10*time.Second, true, func(...) (bool, error) {
        return updatedJWTAuthenticator.healthCheck() == nil, nil
    })
    // 3. 原子替换
    oldJWTAuthenticator := c.jwtAuthenticatorPtr.Swap(updatedJWTAuthenticator)
    // 4. 旧认证器在 1 分钟后关闭，确保正在使用它的请求完成
    go func() {
        time.Sleep(time.Minute)
        oldJWTAuthenticator.cancel()
    }()
}
```

### 5.3f Webhook TokenReview 认证器

**源码**：`staging/src/k8s.io/apiserver/plugin/pkg/authenticator/token/webhook/webhook.go`

**配置参数**：`--authentication-token-webhook-config-file`

```go
type WebhookTokenAuthenticator struct {
    tokenReview    tokenReviewer       // TokenReview API 客户端
    retryBackoff   wait.Backoff        // 重试退避策略
    implicitAuds   authenticator.Audiences  // 隐式受众
    requestTimeout time.Duration       // 请求超时
}

func (w *WebhookTokenAuthenticator) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
    wantAuds, checkAuds := authenticator.AudiencesFrom(ctx)
    r := &authenticationv1.TokenReview{
        Spec: authenticationv1.TokenReviewSpec{
            Token:     token,
            Audiences: wantAuds,   // 将请求受众传递给 webhook
        },
    }

    // 指数退避重试调用 TokenReview API
    webhook.WithExponentialBackoff(ctx, w.retryBackoff, func() error {
        result, statusCode, err = w.tokenReview.Create(ctx, r, metav1.CreateOptions{})
        return err
    }, ...)

    // Audience 验证
    if checkAuds {
        gotAuds := w.implicitAuds
        if len(result.Status.Audiences) > 0 {
            gotAuds = result.Status.Audiences  // webhook 返回了受众
        }
        auds = wantAuds.Intersect(gotAuds)
        if len(auds) == 0 { return nil, false, nil }
    }

    if !r.Status.Authenticated { return nil, false, err }

    return &authenticator.Response{
        User: &user.DefaultInfo{
            Name:   r.Status.User.Username,
            UID:    r.Status.User.UID,
            Groups: r.Status.User.Groups,
            Extra:  extra,
        },
        Audiences: auds,
    }, true, nil
}
```

**值得注意的是**：Webhook 认证器默认使用 `token/cache` 缓存结果，`CacheTTL` 默认 2 分钟。这减少了认证服务的压力，但也意味着撤销 token 后最多需要 2 分钟才能生效。

### 5.4 WebSocket 协议认证器

**源码**：`staging/src/k8s.io/apiserver/pkg/authentication/request/websocket/protocol.go`

WebSocket 连接不能使用标准的 `Authorization` 头部进行认证，因为 JavaScript WebSocket API 无法设置自定义头部。Kubernetes 使用 `Sec-WebSocket-Protocol` 头部传递令牌：

```go
// 格式：Sec-WebSocket-Protocol: base64url.bearer.authorization.k8s.io.<base64url-token>
const bearerProtocolPrefix = "base64url.bearer.authorization.k8s.io."
```

```go
func (a *ProtocolAuthenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
    if !wsstream.IsWebSocketRequest(req) { return nil, false, nil }

    for _, protocolHeader := range req.Header[protocolHeader] {
        for _, protocol := range strings.Split(protocolHeader, ",") {
            if !strings.HasPrefix(protocol, bearerProtocolPrefix) { continue }
            // 解码 token
            encodedToken := strings.TrimPrefix(protocol, bearerProtocolPrefix)
            decodedToken, _ := base64.RawURLEncoding.DecodeString(encodedToken)
            token = string(decodedToken)
        }
    }
    // 认证成功后，从 Sec-WebSocket-Protocol 中移除 token 子协议
    resp, ok, err := a.auth.AuthenticateToken(req.Context(), token)
    if ok { req.Header.Set(protocolHeader, strings.Join(filteredProtocols, ",")) }
    return resp, ok, err
}
```

### 5.5 Anonymous 认证器（兜底）

**源码**：`staging/src/k8s.io/apiserver/pkg/authentication/request/anonymous/anonymous.go`

**配置参数**：`--anonymous-auth`（默认 `true`）

```go
type Authenticator struct {
    allowedPaths map[string]bool   // 限制匿名访问的路径
}

func (a *Authenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
    // 如果设置了路径限制，检查当前路径是否在允许列表中
    if len(a.allowedPaths) > 0 && !a.allowedPaths[req.URL.Path] {
        return nil, false, nil
    }
    auds, _ := authenticator.AudiencesFrom(req.Context())
    return &authenticator.Response{
        User: &user.DefaultInfo{
            Name:   user.Anonymous,   // "system:anonymous"
            Groups: []string{user.AllUnauthenticated},  // "system:unauthenticated"
        },
        Audiences: auds,
    }, true, nil
}
```

Anonymous 认证器放在认证链的**最末尾**（通过 `union.NewFailOnError` 包装），这意味着：

```go
authenticator = union.New(authenticators...)                     // 先尝试所有真实认证
authenticator = group.NewAuthenticatedGroupAdder(authenticator)  // 添加 system:authenticated
authenticator = union.NewFailOnError(authenticator,
    anonymous.NewAuthenticator(...))                              // 最后才是匿名兜底
```

这种特殊包装方式产生了一个关键行为：如果前面的认证器返回了**错误**（比如 Webhook 网络不通），`FailOnError` 模式会立即停止并返回该错误，不会降级为匿名用户。这是**安全优先**的设计——宁可让请求失败，也不能在认证系统异常时错误地将未认证请求当作匿名请求放行。

---

## 6. Token 缓存机制

**源码**：`staging/src/k8s.io/apiserver/pkg/authentication/token/cache/cached_token_authenticator.go`

每次认证都调用外部服务（如 Webhook）或进行昂贵的 JWT 签名验证显然不高效。Kubernetes 使用了一个高性能的 Token 缓存层：

```go
type cachedTokenAuthenticator struct {
    authenticator authenticator.Token    // 被包装的真实认证器
    cacheErrs     bool                   // 是否缓存错误结果
    successTTL    time.Duration          // 成功结果缓存时间
    failureTTL    time.Duration          // 失败结果缓存时间
    cache         cache                  // 底层缓存（striped cache）
    group         singleflight.Group     // 防止并发重复请求
    hashPool      *sync.Pool             // HMAC-SHA256 hash 池
}
```

### 缓存键设计

为了安全，缓存键**不是 token 本身**，而是 HMAC-SHA256 哈希：

```go
func keyFunc(hashPool *sync.Pool, auds []string, token string) string {
    h := hashPool.Get().(hash.Hash)
    h.Reset()
    // 写入 token + audiences 的长度前缀编码
    writeLengthPrefixedString(h, b, token)
    writeLength(h, b, len(auds))
    for _, aud := range auds {
        writeLengthPrefixedString(h, b, aud)
    }
    return toString(h.Sum(nil))  // 直接返回二进制哈希作为 key
}
```

使用 HMAC 和随机密钥防止了预计算攻击和长度扩展攻击，也缓解了哈希碰撞 DOS。

### Striped Cache 与 Singleflight

为了支持高并发，缓存分为 32 个 stripe，每个 stripe 包含约 32K 个条目：

```go
cache: newStripedCache(32, fnvHashFunc, func() cache { return newSimpleCache(clock) })
```

同时使用 `singleflight.Group` 确保同一 token 的并发请求只触发一次后端认证：

```go
c := a.group.DoChan(key, func() (val interface{}, _ error) {
    // 只有第一个请求执行到这里
    record.resp, record.ok, record.err = a.authenticator.AuthenticateToken(ctx, token)
    // 缓存结果
    if record.ok && a.successTTL > 0 { a.cache.set(key, record, a.successTTL) }
    if !record.ok && a.failureTTL > 0 { a.cache.set(key, record, a.failureTTL) }
    return record, nil
})
```

### 安全考量

- 缓存会记录审计注解和警告信息，并在命中缓存后重新注入当前 context
- 失败结果默认不缓存（`cacheErrs` 可配置），因为失败可能由临时网络问题引起
- 使用 `context.Background()` 分离共享查询 context，避免某个请求取消影响其他共享同一 token 的请求

---

## 7. Audience 验证机制

**源码**：`staging/src/k8s.io/apiserver/pkg/authentication/authenticator/audiences.go`

Audience（受众）是 OAuth2/OIDC 中的核心概念，防止 token 被滥用。Kubernetes 通过多层 audience 验证确保安全：

### 7.1 三层 Audience 传播

```
API Server 配置的 API Audiences (--api-audiences)
       │
       ▼
WithAuthentication 过滤器：注入 request context
       │
       ▼
Token 认证器：从 context 读取，传递给 TokenReview 或 JWT 验证
       │
       ▼
认证回应 (Response.Audiences) → 与 API Audiences 取交集 → 交集为空则拒绝
```

### 7.2 Audience 交集验证

```go
func audiencesAreAcceptable(apiAuds, responseAudiences authenticator.Audiences) bool {
    if len(apiAuds) == 0 || len(responseAudiences) == 0 { return true }
    return len(apiAuds.Intersect(responseAudiences)) > 0
}
```

### 7.3 Audience Agnostic 包装器

对于那些不理解 audience 概念的认证器（如静态 Token 文件），使用 `WrapAudienceAgnosticToken` 包装：

```go
func authenticate(ctx context.Context, implicitAuds Audiences, authenticate func() (*Response, bool, error)) (*Response, bool, error) {
    targetAuds, ok := AudiencesFrom(ctx)
    if !ok { return authenticate() }  // 没有 audience 限制
    auds := implicitAuds.Intersect(targetAuds)
    if len(auds) == 0 { return nil, false, nil }  // 无交集 → 拒绝
    resp, ok, err := authenticate()
    resp.Audiences = auds  // 注入 audience 信息
    return resp, true, nil
}
```

---

## 8. 认证链完整数据流

以 `kubectl --token=<sa-token> get pods` 为例，完整的认证数据流：

```
1. HTTP 请求到达
   Authorization: Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IiJ9...

2. 过滤器链开始执行（顺序从外到内）
   │
   ├─ WithPanicRecovery          ← 穿透
   ├─ WithRequestInfo            ← 解析: get pods
   ├─ WithCORS                   ← 穿透
   └─ WithAuthentication
        │
        ├─ 1. Front-Proxy 认证器
        │     检查 X-Remote-User → 没有 → (nil, false, nil)
        │
        ├─ 2. X509 认证器
        │     检查 req.TLS.PeerCertificates → 没有客户端证书 → (nil, false, nil)
        │
        ├─ 3. Bearer Token 认证器
        │     提取 Authorization: Bearer <token>
        │     ↓
        │     ├─ Token Union 链
        │     │   ├─ 3a. Static Token File → 不在列表中 → (nil, false, nil)
        │     │   ├─ 3b. Legacy SA → iss 不匹配 → (nil, false, nil)
        │     │   ├─ 3c. TokenRequest SA ← iss 匹配！
        │     │   │     验证 JWT 签名
        │     │   │     验证 audience
        │     │   │     验证 ServiceAccount 存在
        │     │   │     验证 Pod 绑定
        │     │   │     → (*Response{User: "system:serviceaccount:default:my-sa"}, true, nil)
        │     │   └─ (停止，不再尝试 3d/3e/3f)
        │     └─ 返回认证结果
        │
        ├─ 4. AuthenticatedGroupAdder
        │     → 添加 "system:authenticated" 组
        │
        ├─ 5. 清除 Authorization 头
        │
        └─ 6. 注入 context: request.WithUser(ctx, userInfo)
             ↓
   └─ 后续过滤器（Tracing / Audit / Impersonation / APF / Authorization）
        ↓
   REST Handler: get pods → etcd → 200 OK
```

