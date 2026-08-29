# HOW — HTTP On WebSocket

[English](README.md) | [中文](readme.zh.md)

基于 WebSocket 隧道的透明反向 HTTP 代理，使用 Rust 编写。

**HOW** = **H**TTP **O**n **W**ebsocket。HOW **客户端**部署在内网（与待暴露的 API 同处一网），主动向公网的 HOW **服务端**发起**出站** WebSocket 连接。调用方像访问普通 HTTP 服务一样请求服务端；服务端将请求透明转发给客户端，由客户端路由到配置好的上游，并把响应按流式返回。内网无需开放任何入站端口——连接始终由内网主动发起。

> 本 README 是一份上手教程，建议从头到尾通读：
> [理解模型](#1-工作原理) → [构建并试跑](#2-快速开始) →
> [配置服务端](#3-配置服务端) → [配置客户端](#4-配置客户端) →
> [在 TLS 反代后部署](#5-在反向代理后运行-tls) → [将 LLM 接入代理](#6-把-llm-接到代理后)。

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

涉及三种角色：

- **HOW 服务端**——部署在公网的透明反向代理，接收所有路径（catch-all）。它对外暴露一个 HTTP 端口供调用方访问，并提供一个 `/register` WebSocket 端点供客户端接入、提交空闲隧道。
- **HOW 客户端**——运行在内网。它主动向服务端发起**出站**连接，维护一批 WebSocket 隧道，并代为向真实上游发起请求。
- **调用方**——任何能发 HTTP 请求的端（curl、SDK、浏览器）。对调用方而言，服务端就等同于目标 API。

```
            HTTP 请求（任意路径 + 调用方自带的 Auth）       WebSocket（出站连接）
   调用方  ───────────────────────────────────────►  HOW 服务端  ══════════════════►  HOW 客户端  ──► 内网 API
   (curl)  http://server/chat/completions             /register                       按 Host→上游 路由，
           Authorization: Bearer …                     （全路径代理）                    执行并流式返回
                                                    /status （健康检查）
```

### 请求生命周期（单次代理流程）

1. **客户端维护一个常驻隧道池。** 启动时，它用共享的 `secretkey`（通过 `X-SECRET-KEY` 头）连接到服务端的 `/register`，发送 `<id>_<poolsize>` 作为身份标识，并维护一个共 `poolidlesize` 条（空闲+在忙都计入）的 WebSocket 隧道池：始终优先使用池内空闲隧道，仅在冷池补温到该总量、或全部隧道忙时每次 +1 地扩容，上限 `poolmaxsize`。
2. **调用方发起普通 HTTP 请求**到服务端——路径任意，并自行携带 `Authorization`、`Content-Type` 等头。
3. **服务端依次执行可选的准入校验**（来源 IP → API key → 绑定域名），随后依据请求的 `Host` 头与路径还原请求 URL，通过一条空闲隧道转发：请求头作为一帧 JSON 文本，请求体拆为若干二进制帧，最后以一个空帧收尾。
4. **客户端解析路由：** 依据 `routes` 将请求 Host 映射到上游 base URL，再追加请求路径与查询参数，用 `reqwest` 向真实上游发起请求；请求体上行、响应体下行均按流式传输。
5. **服务端按响应分块到达的顺序逐块回传给调用方**，全程无缓冲，因此流式（SSE）可端到端保真。

### 设计要点

- **透明代理**——所有头（`Authorization`、`Content-Type`、自定义头）原样透传。代理从不注入或改写密钥，密钥由调用方自行携带。
- **目的地由配置决定，而非请求携带**——调用方从不指定真实上游。服务端只能看到请求的 `Host`，由客户端依据 `routes` 完成映射。
- **始终出站**——WebSocket 由客户端主动发起，内网无需开放任何入站端口。

---

## 2. 快速开始

需要 Rust 工具链。构建三个二进制：

```bash
make release         # = cargo build --release
```

产出 `target/release/how-server`、`how-client` 与 `how-test-api`（一个用于测试的微型上游，提供 `/hello`、`/post`、`/stream` 等接口）。

示例配置指向一个占位上游（`internal-api.local`）；要跑通真实往返，需把客户端指向内置的测试 API。打开**四个终端**：

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

一次 HTTP 请求经服务端、客户端，最终抵达测试 API——这就是整套机制。后续各节逐一说明各项配置。

---

## 3. 配置服务端

服务端配置为 YAML 文件（`config.server.example.cfg`）：

```yaml
---
host : 127.0.0.1            # 绑定地址
port : 8080                 # 绑定端口
timeout : 1000              # 在返回 526 前，等待一条空闲 WS 隧道的毫秒数
idletimeout : 60000         # 关闭多余空闲隧道前的毫秒数
livenesstimeout : 120000    # 关闭静默（半开）空闲隧道前的毫秒数
secretkey : ThisIsASecret   # 共享密钥；必须与每个客户端的 secretkey 一致
```

服务端是一个透明的全路径反向代理：**除 `/register` 与 `/status` 外，所有路径都会转发给 HOW 客户端**。调用方无需提供目的地头，真实上游由客户端依据 `routes` 解析。

**心跳保活 / 存活判定。** 由**客户端**每 30 秒发一个 WebSocket `ping`（并对收到的 `ping` 回 `pong`）：客户端是 dial-out/NAT 内一侧，由它产生周期性出向流量，才能刷新沿途 NAT/防火墙的空闲表项，避免几小时无人使用时链路被悄悄断掉。

存活判定在**两端都做**，因为 ping 发出去却收不到 pong，说明对端或路径已失效——而半开链路可能在不向任何一端发出 TCP FIN/RST 的情况下死掉（NAT/防火墙只是悄悄丢掉该四元组），所以既不能指望收到关闭帧，也不能指望操作系统的 TCP keepalive（默认关闭；即便开启，只要 30s ping 让 socket 不空闲，它也永不触发）：

- **服务端**（`livenesstimeout`，默认 120s）：一条隧道若在此时间内**未收到任何帧**（ping/pong/数据），即视为半开并关闭，确保下一个请求不会被派到死隧道上。死链约 2 分钟内被清理，而无需等操作系统的 TCP keepalive（约 2 小时）。
- **客户端**（客户端的 `livenesstimeout`，默认 90s）：客户端记录 `last_activity`（每收到一个 pong/数据帧即刷新），超过该时长未收到任何帧就关闭这条隧道，由连接池 `connector`（每 1s 运行）重新拨号补上——只有客户端能重建隧道，服务端无法回拨到客户端的内网。客户端超时（90s）刻意小于服务端（120s），使客户端在服务端清理整个 pool **之前**就重连自愈。**若没有这个客户端存活检测，客户端会一直攥着一池死掉的半开隧道永不重连，服务端则会回收并清空 pool，第二天早上便报 "No proxy available"**——正是"白天好用、过一晚就死"的症状。

**定时池健康巡检**（`healthcheckinterval`，仅客户端，默认 30s）。上面的被动检测最长要 90s 才能**察觉**死链，而在半开隧道看起来还是 idle 的时间里，按需 connector 一条新连接都不会拨——这个窗口内服务端可能已经没有任何可用隧道，请求全部失败，只能重启客户端。健康巡检补上这个缺口：每隔一个间隔，主动探测每条空闲隧道（发 ping、限时 10s 等 pong），**打印每条隧道的状态**（`pool health: idle=3 ok=3 ... |tunnel#1:ok(0s) ...`），关闭不应答的隧道并立刻补拨，把池子恢复到 `poolidlesize` 条**经过验证**的隧道。若池子已完全清空且 connector 正在拨号退避中（服务端挂过又回来了），每轮巡检仍会按巡检节奏补拨一条"救援"隧道——恢复不再需要等退避结束，也无需重启客户端。

### 可选准入校验

留空或注释即禁用。三项校验均在**调用代理池之前**执行，被拒请求不会触达后端。每条请求按以下顺序检查：**来源 IP → API key → 绑定域名**。

```yaml
# 绑定域名：仅当 Host 主机名在此列表中的请求被接受；IP 与未列出 host -> 403。
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

客户端配置为 YAML 文件（`config.client.example.cfg`）：

```yaml
---
targets :                            # 要出站连接的 HOW 服务端
 - ws://127.0.0.1:8080/register
poolidlesize : 10                    # 每个服务端的隧道总量目标（空闲+在忙计入）；按需创建
poolmaxsize : 100                    # 每个服务端的并发 WS 隧道硬上限
livenesstimeout : 90000              # 静默隧道被回收重连前的毫秒数
healthcheckinterval : 30000          # 池健康巡检间隔（探活 + 状态打印 + 补足），毫秒
secretkey : ThisIsASecret            # 必须与服务端的 secretkey 一致

# 路由表：请求 Host（调用方访问服务端时使用的 Host）
#        -> 上游 base URL。客户端会追加请求路径。
# 仅列出的 host 被转发；其他 host/IP -> 527 "No route"。
routes :
 "127.0.0.1:8080" : "http://internal-api.local"
 "llm.example.com" : "https://api.openai.com/v1"
```

### 字段说明

| 字段 | 含义 |
|-------|---------|
| `targets` | 一个或多个 `/register` URL，由客户端主动发起出站连接。客户端对每个 target 维护一个连接池。 |
| `poolidlesize` | 每个服务端的隧道总量目标（空闲+在忙都计入）。始终优先复用池内空闲隧道；仅在池子未达该总量时补温、或全部隧道忙时每次 +1 扩容。调高可降低首字节延迟。 |
| `poolmaxsize` | 每个服务端的并发隧道硬上限。按峰值并发设置；超出时服务端最多等待 `timeout` 毫秒后返回 526。 |
| `livenesstimeout` | 一条隧道若在此毫秒数内**未收到任何帧**（pong/数据）即视为半开并关闭，由连接池重拨补上。默认 90000（90s，小于服务端的 120s，故客户端会在服务端回收整个 pool 之前自愈）。必须大于 ~2× 30s ping 周期（低于 60000ms 无法可靠观测到两次 pong、会把健康链路误杀；过小值会回退默认并打告警日志）。0 = 默认。 |
| `healthcheckinterval` | 池健康巡检间隔（毫秒）：每隔该时长主动探测每条空闲隧道（发 ping、限时 10s 等 pong）、打印每条隧道状态、关闭不应答的隧道并补拨，使池内始终保持 `poolidlesize` 条**经验证可用**的隧道。池子完全清空且 connector 处于拨号退避（连续失败后最长 60s）时，每轮巡检仍按巡检节奏补拨一条救援隧道——巡检节奏本身就限制了重试频率，服务端恢复后约一个间隔内即重新获得隧道，无需重启客户端。默认 30000；必须大于 10s 探活时限（过小值回退默认并打告警日志）。0 = 默认。 |
| `secretkey` | WebSocket 握手时通过 `X-SECRET-KEY` 发送。必须与服务端的 `secretkey` 一致，否则隧道被拒（→ 526）。 |
| `routes` | **请求 Host → 上游 base** 的映射。请求 Host 即调用方访问服务端时使用的 `Host`（如 `127.0.0.1:8080`、`llm.example.com`）。客户端把请求路径与查询参数追加到上游 base 之后。匹配时先按 `host:port`，再按 `host`。 |
| `id` | 可选客户端 id；省略则启动时生成随机 UUID。 |

### 可选请求过滤（正则规则）

针对还原后的请求 URL 匹配，可用作客户端侧的纵深防御黑白名单。

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

WebSocket 隧道为纯 `ws://`，客户端**拒绝 `wss://`** 目标（TLS 应由服务端前方的反向代理终结）。用 nginx / Caddy 等终结 TLS，并让客户端指向反代后明文的 `/register`：

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

随后把客户端的 `targets` 设为 `ws://your-domain.example.com/register`；若启用了 `allowedhosts`，将 `your-domain.example.com` 加入其中。

---

## 6. 把 LLM 接到代理后

常见用法：把内网中的 OpenAI 兼容 API 暴露给外部调用方。

1. **服务端**——公开发布（直连或置于 §5 的反代之后）。可选开启 `apikeys`（仅允许持有已知 key 的调用方）与 `allowedhosts`（绑定到你的域名）。
2. **客户端**（部署在内网）——配置 `routes`，将请求 Host 映射到真实 LLM 的 base URL：
   ```yaml
   routes :
    "llm.example.com" : "https://api.openai.com/v1"
   ```
3. **调用方**——将任意 OpenAI 兼容客户端指向服务端 host，并自行携带 `Authorization`。该头会被透明转发；真实 LLM URL 仅存在于客户端的 `routes`。

非流式与流式（SSE）均可用；SSE 流端到端保真，首个 token 远在响应完成前即可到达：

```bash
# 客户端 routes: "llm.example.com" -> "https://api.openai.com/v1"
curl http://llm.example.com/chat/completions \
  -H "Authorization: Bearer $YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"...","messages":[{"role":"user","content":"hello"}],"stream":true}'
```

---

## 7. 预编译二进制

推送 `v*` tag 会触发 `.github/workflows/release.yml`，交叉编译并发布 GitHub release；每种架构一个 tar 包，内含 `how-server`、`how-client`、`how-test-api` 及示例配置：

- `how-linux-x64.tar.gz` — Linux x86_64
- `how-linux-arm64.tar.gz` — Linux aarch64
- `how-darwin-arm64.tar.gz` — macOS Apple Silicon

```bash
tar -xzf how-linux-x64.tar.gz
./how-server -config config.server.example.cfg
```

---

## 8. 测试

两套测试均基于真实二进制、发起真实 HTTP 请求：

```bash
make e2e            # shell 套件（curl）：GET/POST/header/自定义状态码(666)、
                    #   黑名单(527)、无可用代理(526)、二进制 body 完整性
make test           # Rust 套件（cargo test --test e2e）——串行运行
```

覆盖范围：GET/POST/header/自定义状态码(666) 的转发、header 透传、客户端黑名单(527)、无可用代理(526)、无路由(527)、密钥错误(526)、绑定域名 / 来源 IP / API key 准入校验(403)、连接池打满→调度超时(526)、二进制 body 完整性(1 MiB)、SSE 流式（`text/event-stream` 逐 token 投递）以及大体积流式上传。

内置的 `how-test-api` 上游提供 `/hello`、`/header`、`/post`、`/fail`（状态 666）、`/sleep`、`/stream`（SSE）、`/bytes` 以及一个模拟的 `/v1/chat/completions`。§2 的快速开始是本地体验的最简途径。

---

## 9. 参考

### 状态码

| Code | 含义 | 来源 |
|------|---------|--------|
| `526` | 代理错误——无可用客户端/隧道、调度 `timeout` 超时、或隧道在请求中途断开。 | server |
| `527` | 客户端错误——请求 Host 无匹配路由、被 `blacklist`/`whitelist` 拒绝、或上游请求失败。 | client |
| `403` | 准入校验拒绝——绑定域名（`allowedhosts`）、来源 IP（`allowips`）或 API key（`apikeys`）。 | server |

### CLI 参数

`how-server` 与 `how-client` 均接受 Go 风格参数：`-config <file>`、`--config <file>` 或 `-config=<file>`。默认值：server → `config.server.example.cfg`；client → `config.client.example.cfg`。`how-test-api` 使用 `-addr <host:port>`（默认 `localhost:8081`）。
