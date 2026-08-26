# HOW — HTTP On WebSocket

[English](README.md) | [中文](readme.zh.md)

一个基于 WebSocket 隧道传输的透明反向 HTTP 代理，用 Rust 编写。

**HOW** = **H**TTP **O**n **W**ebsocket。HOW **客户端**运行在内网（紧挨着你要暴露的 API），向公网的 HOW **服务端**发起**出站** WebSocket 连接。调用方发一个普通 HTTP 请求到服务端；服务端把它透明地转发给客户端，客户端再路由到配置好的上游，并把响应流式回传。内网侧无需开放任何入站端口——隧道始终是出站的。

> 本 README 是一份上手教程——从头到尾读一遍：
> [理解模型](#1-工作原理) → [构建并试跑](#2-快速开始) →
> [配置服务端](#3-配置服务端) → [配置客户端](#4-配置客户端) →
> [在 TLS 反代后部署](#5-在反向代理后运行-tls) → [把 LLM 接到代理后](#6-把-llm-接到代理后)。

## 目录

1. [工作原理](#1-工作原理)
2. [快速开始](#2-快速开始)
3. [配置服务端](#3-配置服务端)
4. [配置客户端](#4-配置客户端)
5. [在反向代理后运行 TLS](#5-在反向代理后运行-tls)
6. [把 LLM 接到代理后](#6-把-llm-接到代理后)
7. [预编译二进制](#7-预编译二进制)
8. [测试](#8-测试)
9. [参考](#9-参考)

---

## 1. 工作原理

三种角色：

- **HOW 服务端**——一个公网的、透明的*兜底*反向代理。它为调用方暴露一个 HTTP 端口，外加一个 `/register` WebSocket 端点，供客户端提供空闲隧道。
- **HOW 客户端**——运行在内网。它向服务端**出站**拨号，提供一批 WebSocket 隧道，并对真正的上游执行被代理的请求。
- **调用方**——任何能说 HTTP 的东西（curl、SDK、浏览器）。它就像服务端*本身就是* API 那样去请求服务端。

```
            HTTP 请求（任意路径 + 调用方自带的 Auth）       WebSocket（出站拨号）
   调用方  ───────────────────────────────────────►  HOW 服务端  ══════════════════►  HOW 客户端  ──► 内网 API
   (curl)  http://server/chat/completions             /register                       按 host→上游 路由，
           Authorization: Bearer …                     （兜底代理）                      执行并流式回传
                                                    /status （健康检查）
```

### 请求生命周期（一次被代理的请求）

1. **客户端维持一个常驻隧道池。** 启动时它用共享的 `secretkey`（作为 `X-SECRET-KEY`）拨号到服务端的 `/register`，发送一条 `<id>_<poolsize>` 问候，并保持 `poolidlesize` 条空闲 WebSocket 连接，在负载下补充到 `poolmaxsize`。
2. **调用方发一个普通 HTTP 请求**到服务端——任意路径，自带 header（`Authorization`、`Content-Type`、自定义）。
3. **服务端校验**可选的守门人（来源 IP → API key → 绑定域名），从请求的 `Host` header + 路径重建一个到达 URL，并通过一条空闲隧道转发请求：头部作为一帧 JSON 文本，body 作为若干二进制帧，以一个空帧结束。
4. **客户端解析路由：** 它按 `routes` 把到达 host 映射到上游 base URL，拼上请求路径 + query，用 `reqwest` 对真正的上游执行请求——请求体上行、响应体下行都流式传输。
5. **服务端把响应分块按到达顺序流式回传给调用方**，因此没有缓冲，流式（SSE）端到端保真。

### 关键设计点

- **透明代理**——所有 header（`Authorization`、`Content-Type`、自定义）原样透传。代理从不注入或改写密钥；调用方自带。
- **目的地是配置出来的，不是请求指定的**——调用方从不指定真正的上游。服务端只看得到到达的 `Host`；客户端通过 `routes` 把它映射到上游。
- **始终出站**——WebSocket 由客户端发起，因此内网无需任何入站端口。

---

## 2. 快速开始

需要 Rust 工具链。构建三个二进制：

```bash
make release         # = cargo build --release
```

这会产出 `target/release/how-server`、`how-client` 和 `how-test-api`
（一个用于测试的微型上游——提供 `/hello`、`/post`、`/stream` 等）。

示例配置路由到一个占位上游（`internal-api.local`），所以为了拿到真实的往返，我们把客户端指向内置的测试 API。打开**四个终端**：

```bash
# 1) 一个假的“内网 API”，监听 :8081
./target/release/how-test-api -addr 127.0.0.1:8081

# 2) HOW 服务端，监听 :8080
./target/release/how-server -config config.server.example.cfg

# 3) 客户端——把服务端 host 路由到测试 API
mkdir -p /tmp/how && cat > /tmp/how/client.cfg <<'EOF'
---
targets :
 - ws://127.0.0.1:8080/register
secretkey : ThisIsASecret          # 必须与服务端的 secretkey 一致
routes :
 "127.0.0.1:8080" : "http://127.0.0.1:8081"
EOF
./target/release/how-client -config /tmp/how/client.cfg

# 4) 调用方——访问服务端 host；路径被透明转发
curl http://127.0.0.1:8080/hello                       # -> hello world
curl -X POST http://127.0.0.1:8080/post -d 'ping=pong' # -> ping=pong
curl -N http://127.0.0.1:8080/stream                   # -> SSE 分块，逐 token
```

你刚刚把一个 HTTP 请求**经**服务端**送到**客户端**再到**测试 API——这就是全部思路。下面各节解释每一个旋钮。

---

## 3. 配置服务端

服务端配置是一个 YAML 文件（`config.server.example.cfg`）：

```yaml
---
host : 127.0.0.1            # 绑定地址
port : 8080                 # 绑定端口
timeout : 1000              # 在返回 526 前，等待一条空闲 WS 隧道的毫秒数
idletimeout : 60000         # 关闭多余空闲隧道前的毫秒数
secretkey : ThisIsASecret   # 共享密钥；必须与每个客户端的 secretkey 一致
```

服务端是一个透明的兜底反向代理：**除 `/register` 和 `/status` 外的每条路径都被转发**到某个 HOW 客户端。没有调用方提供的目的地 header——真正的上游由客户端从其 `routes` 解析。

### 可选安全守门人

留空 / 注释掉即禁用。三者都在**触及代理池之前**运行，所以被拒请求永远到不了你的后端。每次请求按此顺序检查：**来源 IP → API key → 绑定域名**。

```yaml
# 绑定域名：仅当 Host 主机名在此列表中的请求被接受；IP 和未列出 host -> 403。
# 防止调用方直接访问服务端 IP 来绕过域名。
#allowedhosts :
# - your-domain.example.com

# 来源 IP 白名单：仅来自这些 IP 的请求被服务；其他来源 IP -> 403 "DENY <ip>"。
#allowips :
# - 192.168.1.100

# API key 白名单：每个被代理的请求必须携带
# Authorization: Bearer <key>，且 key 在此列表中；缺失或不匹配 -> 403。
# 防止扫描器把请求推到后端。
#apikeys :
# - sk-your-api-key-1
```

---

## 4. 配置客户端

客户端配置是一个 YAML 文件（`config.client.example.cfg`）：

```yaml
---
targets :                            # 要出站拨号的 HOW 服务端
 - ws://127.0.0.1:8080/register
poolidlesize : 10                    # 每个服务端保持的空闲 WS 隧道数
poolmaxsize : 100                    # 每个服务端的并发 WS 隧道硬上限
secretkey : ThisIsASecret            # 必须与服务端的 secretkey 一致

# 路由表：到达 host（调用方在服务端上访问的 Host）
#        -> 上游 base URL。客户端会拼上请求路径。
# 仅列出的 host 被转发；其他 host/IP -> 527 "No route"。
routes :
 "127.0.0.1:8080" : "http://internal-api.local"
 "llm.example.com" : "https://api.openai.com/v1"
```

### 字段参考

| 字段 | 含义 |
|-------|---------|
| `targets` | 一个或多个要出站拨号的 `/register` URL。客户端对每个开启一个连接池。 |
| `poolidlesize` | 每个服务端保持的空闲隧道数。调高可降低首字节延迟。 |
| `poolmaxsize` | 每个服务端并发隧道的硬上限。按你的峰值并发来定；超出时服务端最多等 `timeout` 毫秒后返回 526。 |
| `secretkey` | WebSocket 握手时作为 `X-SECRET-KEY` 发送。必须等于服务端的 `secretkey`，否则隧道被拒（→ 526）。 |
| `routes` | **到达 host → 上游 base** 映射。到达 host 是调用方在服务端上访问的 `Host`（`127.0.0.1:8080`、`llm.example.com`）。客户端把请求路径 + query 拼到上游 base 后面。匹配先试 `host:port`，再试 `host`。 |
| `id` | 可选客户端 id；省略则在启动时生成随机 UUID。 |

### 可选请求过滤（正则规则）

对重建后的到达 URL 匹配。作为客户端侧的纵深防御 allow/deny 列表很有用。

```yaml
# 拒绝匹配的请求 -> 527 "Destination is forbidden"。
blacklist :
 - method: ".*"
   url: ".*forbidden.*"
   headers:
     X-CUSTOM-HEADER: "^value$"

# 仅放行匹配的请求（非空时）；不匹配 -> 527。
whitelist :
 - method: "^GET$"
   url: "^http(s)?://.*$"
```

---

## 5. 在反向代理后运行 TLS

WebSocket 隧道是纯 `ws://`——客户端**拒绝 `wss://`** 目标（TLS 预期由服务端前面的反向代理终结）。用 nginx / Caddy / … 终结 TLS，并让客户端指向反代的明文 `/register`：

```
调用方 ──https──► nginx (TLS) ──http──► HOW 服务端 :8080
HOW 客户端 ──ws──► nginx ──/register──► HOW 服务端
```

nginx 示例片段（调用方走 TLS，客户端隧道走明文 WebSocket 升级）：

```nginx
server {
    listen 443 ssl;
    server_name your-domain.example.com;

    location / {
        proxy_pass http://127.0.0.1:8080;       # 调用方流量
        proxy_set_header Host $host;
    }
    location /register {
        proxy_pass http://127.0.0.1:8080;        # 客户端 WebSocket 隧道
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 3600s;                # 保持隧道开启
    }
}
```

然后把客户端的 `targets` 设为 `ws://your-domain.example.com/register`；若用了 `allowedhosts`，把 `your-domain.example.com` 列进去。

---

## 6. 把 LLM 接到代理后

一个常见用法：把内网里的 OpenAI 兼容 API 暴露给外部调用方。

1. **服务端**——公开发布（直连或在 §5 的反代之后）。可选开启 `apikeys`（只让持有已知 key 的调用方使用）和 `allowedhosts`（绑定到你的域名）。
2. **客户端**（内网里）——设置 `routes`，让到达 host 映射到真正的 LLM base URL：
   ```yaml
   routes :
    "llm.example.com" : "https://api.openai.com/v1"
   ```
3. **调用方**——把任意 OpenAI 兼容客户端指向服务端 host，并自带 `Authorization`。它被透明转发；真正的 LLM URL 只存在于客户端的 `routes`。

非流式和流式（SSE）都可用；SSE 流端到端保真（首个 token 在响应完成前很久就到达）：

```bash
# 客户端 routes: "llm.example.com" -> "https://api.openai.com/v1"
curl http://llm.example.com/chat/completions \
  -H "Authorization: Bearer $YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"...","messages":[{"role":"user","content":"hello"}],"stream":true}'
```

---

## 7. 预编译二进制

推送 `v*` tag 会触发 `.github/workflows/release.yml`，它交叉编译并发布一个 GitHub release，每个架构一个 tar 包，每个包含 `how-server`、`how-client`、`how-test-api` 和示例配置：

- `how-linux-x64.tar.gz` — Linux x86_64
- `how-linux-arm64.tar.gz` — Linux aarch64
- `how-darwin-arm64.tar.gz` — macOS Apple Silicon

```bash
tar -xzf how-linux-x64.tar.gz
./how-server -config config.server.example.cfg
```

---

## 8. 测试

两套测试都通过真实二进制驱动**真实 HTTP 请求**：

```bash
make e2e            # shell 套件（curl）：GET/POST/header/自定义状态码(666)、
                    #   黑名单(527)、无代理(526)、二进制 body 完整性
make test           # Rust 套件（cargo test --test e2e）——串行运行
```

综合覆盖：GET/POST/header/自定义状态码(666) 转发、header 透明性、客户端侧黑名单(527)、无代理(526)、无路由(527)、错误 secret-key(526)、绑定域名 / 来源 IP / API key 守门人(403)、连接池打满→调度超时(526)、二进制 body 完整性(1 MiB)、SSE 流式（`text/event-stream` 逐 token 投递）以及大体积流式上传。

内置的 `how-test-api` 上游提供 `/hello`、`/header`、`/post`、`/fail`（状态 666）、`/sleep`、`/stream`（SSE）、`/bytes` 和一个模拟的 `/v1/chat/completions`。§2 的快速开始是本地试跑的最简方式。

---

## 9. 参考

### 状态码

| Code | 含义 | 来源 |
|------|---------|--------|
| `526` | 代理错误——无可用客户端/隧道、调度 `timeout` 超时、或隧道在请求中途断开。 | server |
| `527` | 客户端错误——到达 host 无路由、被 `blacklist`/`whitelist` 拒绝、或上游拉取失败。 | client |
| `403` | 守门人拒绝——绑定域名（`allowedhosts`）、来源 IP（`allowips`）或 API key（`apikeys`）。 | server |

### CLI 参数

`how-server` 和 `how-client` 都接受 Go 风格参数：`-config <file>`、`--config <file>` 或 `-config=<file>`。默认值：server → `config.server.example.cfg`；client → `config.client.example.cfg`。`how-test-api` 用 `-addr <host:port>`（默认 `localhost:8081`）。
