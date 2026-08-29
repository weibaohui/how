# HOW — HTTP On WebSocket

[English](README.md) | [中文](readme.zh.md)

A transparent reverse HTTP proxy tunneled over WebSockets, written in Rust.

**HOW** = **H**TTP **O**n **W**ebsocket. A HOW **client** runs inside an
internal network (next to the APIs you want to expose) and dials **out** to a
public HOW **server** over a WebSocket. A caller sends a normal HTTP request
to the server; the server forwards it transparently to the client, which
routes it to a configured upstream and streams the response back. No inbound
firewall hole is needed on the internal side — the tunnel is always outbound.

> This README is a hands-on tutorial — follow it top to bottom:
> [understand the model](#1-how-it-works) → [build & try it](#2-quick-start)
> → [configure the server](#3-configure-the-server) →
> [configure the client](#4-configure-the-client) →
> [deploy behind TLS](#5-run-behind-a-reverse-proxy-tls) →
> [put an LLM behind it](#6-put-an-llm-behind-the-proxy).

## Contents

1. [How it works](#1-how-it-works)
2. [Quick start](#2-quick-start)
3. [Configure the server](#3-configure-the-server)
4. [Configure the client](#4-configure-the-client)
5. [Run behind a reverse proxy (TLS)](#5-run-behind-a-reverse-proxy-tls)
6. [Put an LLM behind the proxy](#6-put-an-llm-behind-the-proxy)
7. [Prebuilt binaries](#7-prebuilt-binaries)
8. [Testing](#8-testing)
9. [Reference](#9-reference)

---

## 1. How it works

Three roles:

- **HOW server** — a public, transparent *catch-all* reverse proxy. It
  exposes an HTTP port for callers, plus a `/register` WebSocket endpoint
  where clients offer idle tunnels.
- **HOW client** — runs inside the internal network. It dials **out** to the
  server, offers a pool of WebSocket tunnels, and executes proxied requests
  against the real upstream.
- **Caller** — anything that speaks HTTP (curl, an SDK, a browser). It just
  hits the server as if the server *were* the API.

```
           HTTP request (any path + caller's own Auth)        WebSocket (outbound dial)
  caller  ───────────────────────────────────────►  HOW server  ══════════════════►  HOW client  ──► internal API
   (curl)  http://server/chat/completions             /register                       routes host→upstream,
           Authorization: Bearer …                     (catch-all proxy)              executes, streams back
                                                    /status  (health check)
```

### Request lifecycle (one proxied request)

1. **The client keeps a warm pool of tunnels.** On startup it dials the
   server's `/register` with the shared `secretkey` (as `X-SECRET-KEY`), sends
   a `<id>_<poolsize>` greeting, and keeps a pool of `poolidlesize`
   WebSocket tunnels (idle + busy both count). Existing idle tunnels are
   always preferred; new ones are dialed only to warm a cold pool up to
   that total, or +1 at a time when every tunnel is busy, up to
   `poolmaxsize`.
2. **The caller sends a normal HTTP request** to the server — any path, with
   its own headers (`Authorization`, `Content-Type`, custom).
3. **The server validates** its optional gatekeepers (source IP → API key →
   bound domain), reconstructs an arrival URL from the request's `Host`
   header + path, and forwards the request over an idle tunnel: the header as
   one JSON text frame, the body as binary frames, ended by an empty frame.
4. **The client resolves the route:** it maps the arrival host to an upstream
   base URL from its `routes`, appends the request path + query, and executes
   the request against the real upstream with `reqwest` — streaming the
   request body up and the response body back.
5. **The server streams response chunks to the caller as they arrive**, so
   there is no buffering and streaming (SSE) is preserved end-to-end.

### Key design points

- **Transparent proxying** — all headers (`Authorization`, `Content-Type`,
  custom) pass through unchanged. The proxy never injects or rewrites
  secrets; the caller carries its own.
- **Destination is configured, not requested** — the caller never specifies
  the real upstream. The server only sees the arrival `Host`; the client maps
  it to an upstream via `routes`.
- **Always outbound** — the client opens the WebSocket, so the internal
  network needs no inbound port.

---

## 2. Quick start

Requires the Rust toolchain. Build the three binaries:

```bash
make release         # = cargo build --release
```

This produces `target/release/how-server`, `how-client`, and `how-test-api`
(a tiny upstream used for testing — it serves `/hello`, `/post`, `/stream`,
etc.).

The example configs route to a placeholder upstream (`internal-api.local`),
so to get a real round-trip we'll point the client at the bundled test API.
Open **four terminals**:

```bash
# 1) a fake "internal API" on :8081
./target/release/how-test-api -addr 127.0.0.1:8081

# 2) the HOW server on :8080
./target/release/how-server -config config.server.example.cfg

# 3) the client — route the server host -> the test API
mkdir -p /tmp/how && cat > /tmp/how/client.cfg <<'EOF'
---
targets :
 - ws://127.0.0.1:8080/register
secretkey : ThisIsASecret          # must match the server's secretkey
routes :
 "127.0.0.1:8080" : "http://127.0.0.1:8081"
EOF
./target/release/how-client -config /tmp/how/client.cfg

# 4) a caller — hit the server host; the path is forwarded transparently
curl http://127.0.0.1:8080/hello                       # -> hello world
curl -X POST http://127.0.0.1:8080/post -d 'ping=pong' # -> ping=pong
curl -N http://127.0.0.1:8080/stream                   # -> SSE chunks, token by token
```

You just proxied an HTTP request **through** the server **to** the client
**to** the test API — that is the whole idea. The sections below explain
every knob.

---

## 3. Configure the server

Server config is a YAML file (`config.server.example.cfg`):

```yaml
---
host : 127.0.0.1            # bind address
port : 8080                 # bind port
timeout : 1000              # ms to wait for an idle WS tunnel before returning 526
idletimeout : 60000         # ms before closing excess idle tunnels
livenesstimeout : 120000    # ms before closing a silent (half-open) idle tunnel
secretkey : ThisIsASecret   # shared secret; must match every client's secretkey
```

The server is a transparent catch-all reverse proxy: **every path except
`/register` and `/status` is forwarded** to a HOW client. There is no
caller-provided destination header — the real upstream is resolved by the
client from its `routes`.

**Keepalive / liveness.** The *client* sends a WebSocket `ping` every 30 s
(and replies to any incoming ping with a `pong`): the client is the
dial-out/NAT side, so its periodic outbound traffic is what keeps NAT /
firewall idle timers from silently dropping a quiet link after a few hours.

Liveness is checked on **both** ends, because a ping that never gets a pong
means the peer or the path is gone — and a half-open link can die without a
TCP FIN/RST ever reaching either side (a NAT/firewall just drops the
4-tuple silently), so neither a missing close nor OS TCP keepalive (off by
default, and dormant whenever the 30 s pings keep the socket non-idle) can
rely on detecting it:

- **Server side** (`livenesstimeout`, default 120 s): a tunnel that received
  *no* frame (ping/pong/data) for this long is treated as half-open and
  closed, so the next request is never handed to a dead tunnel. When a
  tunnel dies, this reaps dead links in ~2 minutes instead of waiting for the
  OS TCP keepalive (~2 hours).
- **Client side** (`livenesstimeout` on the client, default 90 s): the client
  tracks `last_activity` (refreshed on every received pong/data frame) and
  closes a tunnel that has been silent for longer than the timeout, then the
  pool connector (runs every 1 s) dials a replacement — only the client can
  re-establish the tunnel, since the server cannot dial back into the
  client's private network. The client's timeout (90 s) is shorter than the
  server's (120 s) on purpose, so the client reconnects *before* the server
  reaps and removes the whole pool. **Without this client-side check the
  client would hold a pool full of dead, half-open tunnels forever and the
  server would report "no proxy available" the next morning** — exactly the
  "works all day, dead overnight" symptom.

**Periodic pool health round** (`healthcheckinterval`, client only, default
30 s). The passive check above needs up to 90 s to *notice* a dead link, and
while a half-open tunnel still looks idle the demand-driven connector dials
nothing — during that window the server may have no usable tunnel at all and
requests fail until the client is restarted. The health round closes that
gap: every interval it actively probes each idle tunnel (ping → pong, 10 s
deadline), logs every tunnel's status (`pool health: idle=3 ok=3 ...
|tunnel#1:ok(0s) ...`), closes the tunnels that do not answer, and dials
replacements so the pool is topped back up to `poolidlesize` *verified*
tunnels. If the pool is completely empty while the connector is backing off
(the server was down and came back), each round still dials one rescue
tunnel at the health cadence, so recovery no longer waits out the backoff.

### Optional security gatekeepers

Leave empty / commented to disable. All three run **before** the proxy pool
is touched, so rejected requests never reach your backend. On each request
they are checked in this order: **source IP → API key → bound domain**.

```yaml
# Bound domains: only requests whose Host hostname is in this list are
# accepted; IP and unlisted hosts -> 403. Prevents callers bypassing the
# domain by hitting the server's IP directly.
#allowedhosts :
# - your-domain.example.com

# Source IP whitelist: only requests from these IPs are served; any other
# source IP -> 403 "DENY <ip>".
#allowips :
# - 192.168.1.100

# API key whitelist: every proxied request must carry an
# Authorization: Bearer <key> whose key is in this list; missing or
# non-matching -> 403. Prevents scanners from pushing to the backend.
#apikeys :
# - sk-your-api-key-1
```

---

## 4. Configure the client

Client config is a YAML file (`config.client.example.cfg`):

```yaml
---
targets :                            # HOW servers to dial out to
 - ws://127.0.0.1:8080/register
poolidlesize : 10                    # tunnel target per server (idle+busy count); demand-driven
poolmaxsize : 100                    # max concurrent WS tunnels per server
livenesstimeout : 90000              # ms before a silent tunnel is reaped & reconnected
healthcheckinterval : 30000          # ms between pool health rounds (probe + status log + refill)
secretkey : ThisIsASecret            # must match the server's secretkey

# Route map: arrival host (the Host the caller targets on the server)
#            -> upstream base URL. The client appends the request path.
# Only listed hosts are forwarded; any other host/IP -> 527 "No route".
routes :
 "127.0.0.1:8080" : "http://internal-api.local"
 "llm.example.com" : "https://api.openai.com/v1"
```

### Field reference

| Field | Meaning |
|-------|---------|
| `targets` | One or more `/register` URLs to dial out to. The client opens a connection pool to each. |
| `poolidlesize` | Idle tunnels kept warm per server. Raises it for low-latency first byte. |
| `poolmaxsize` | Hard cap on concurrent tunnels per server. Size to your peak concurrency; beyond it the server waits up to `timeout` ms then returns 526. |
| `livenesstimeout` | ms before a tunnel that has received no frame (pong/data) is closed as half-open and reconnected. Default 90000 (90 s, < the server's 120 s, so the client self-heals before the server reaps the pool). Must exceed ~2× the 30 s ping interval (below 60000 ms pongs can't be observed reliably and healthy links would be false-reaped); a too-small value falls back to the default and logs a warning. 0 = default. |
| `healthcheckinterval` | Cadence (ms) of the periodic pool health round: actively probe every idle tunnel (ping → wait for the pong, 10 s deadline), log each tunnel's status, close the unresponsive ones, and dial replacements so the pool always holds `poolidlesize` *verified-available* tunnels. When the pool is completely empty and the connector is in dial backoff (up to 60 s after consecutive failures), each round still dials one rescue tunnel — the health cadence itself bounds the retry rate — so the server regains a tunnel within ~one interval after it comes back, without restarting the client. Default 30000; must exceed the 10 s probe deadline (below it falls back to the default and logs a warning). 0 = default. |
| `secretkey` | Sent as `X-SECRET-KEY` on the WebSocket handshake. Must equal the server's `secretkey` or the tunnel is rejected (→ 526). |
| `routes` | The **arrival host → upstream base** map. The arrival host is the `Host` the caller targets on the server (`127.0.0.1:8080`, `llm.example.com`). The client appends the request path + query to the upstream base. Matching tries `host:port` first, then `host`. |
| `id` | Optional client id; a random UUID is generated on startup if omitted. |

### Optional request filtering (regex rules)

Matched against the reconstructed arrival URL. Useful as a defence-in-depth
allow/deny list on the client side.

```yaml
# Deny matching requests -> 527 "Destination is forbidden".
blacklist :
 - method: ".*"
   url: ".*forbidden.*"
   headers:
     X-CUSTOM-HEADER: "^value$"

# Allow only matching requests (when non-empty); non-matches -> 527.
whitelist :
 - method: "^GET$"
   url: "^http(s)?://.*$"
```

---

## 5. Run behind a reverse proxy (TLS)

The WebSocket tunnel is plain `ws://` — the client **rejects `wss://`**
targets (TLS is expected to be terminated by a reverse proxy in front of the
server). Terminate TLS with nginx / Caddy / … and point the client at the
reverse proxy's plaintext `/register`:

```
caller ──https──► nginx (TLS) ──http──► HOW server :8080
HOW client ──ws──► nginx ──/register──► HOW server
```

Example nginx snippet (TLS for callers, plaintext WebSocket upgrade for the
client tunnel):

```nginx
server {
    listen 443 ssl;
    server_name your-domain.example.com;

    location / {
        proxy_pass http://127.0.0.1:8080;       # caller traffic
        proxy_set_header Host $host;
    }
    location /register {
        proxy_pass http://127.0.0.1:8080;        # client WebSocket tunnel
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 3600s;                # keep the tunnel open
    }
}
```

Then set the client's `targets` to `ws://your-domain.example.com/register`
and, if you use `allowedhosts`, list `your-domain.example.com`.

---

## 6. Put an LLM behind the proxy

A common use case: expose an OpenAI-compatible API that lives inside an
internal network to outside callers.

1. **Server** — expose it publicly (directly or behind the reverse proxy in
   §5). Optionally enable `apikeys` (so only callers with a known key can use
   it) and `allowedhosts` (bound to your domain).
2. **Client** (inside the network) — set `routes` so the arrival host maps to
   the real LLM base URL:
   ```yaml
   routes :
    "llm.example.com" : "https://api.openai.com/v1"
   ```
3. **Caller** — point any OpenAI-compatible client at the server host and
   carry your own `Authorization`. It is forwarded transparently; the real
   LLM URL lives only in the client's `routes`.

Both non-streaming and streaming (SSE) work; the SSE stream is preserved
end-to-end (the first token arrives well before the response completes):

```bash
# client routes: "llm.example.com" -> "https://api.openai.com/v1"
curl http://llm.example.com/chat/completions \
  -H "Authorization: Bearer $YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"...","messages":[{"role":"user","content":"hello"}],"stream":true}'
```

---

## 7. Prebuilt binaries

Pushing a `v*` tag triggers `.github/workflows/release.yml`, which
cross-builds and publishes a GitHub release with one tarball per
architecture, each containing `how-server`, `how-client`, `how-test-api` and
the example configs:

- `how-linux-x64.tar.gz` — Linux x86_64
- `how-linux-arm64.tar.gz` — Linux aarch64
- `how-darwin-arm64.tar.gz` — macOS Apple Silicon

```bash
tar -xzf how-linux-x64.tar.gz
./how-server -config config.server.example.cfg
```

---

## 8. Testing

Two suites drive **real HTTP requests** through real binaries:

```bash
make e2e            # shell suite (curl): GET/POST/header/custom-status(666),
                    #   blacklist(527), no-proxy(526), binary body integrity
make test           # Rust suite (cargo test --test e2e) — run sequentially
```

Combined coverage: GET/POST/header/custom-status (666) forwarding, header
transparency, client-side blacklist (527), no-proxy (526), no-route (527),
wrong secret-key (526), bound-domain / source-IP / API-key gatekeepers (403),
pool saturation → dispatcher timeout (526), binary body integrity (1 MiB),
SSE streaming (`text/event-stream` delivered token-by-token), and large
streamed uploads.

The bundled `how-test-api` upstream serves `/hello`, `/header`, `/post`,
`/fail` (status 666), `/sleep`, `/stream` (SSE), `/bytes` and a simulated
`/v1/chat/completions`. The Quick Start in §2 is the simplest way to try it
locally.

---

## 9. Reference

### Status codes

| Code | Meaning | Source |
|------|---------|--------|
| `526` | Proxy error — no client/tunnel available, dispatcher `timeout` exceeded, or the tunnel broke mid-request. | server |
| `527` | Client error — no route for the arrival host, request denied by `blacklist`/`whitelist`, or the upstream fetch failed. | client |
| `403` | Gatekeeper rejection — bound domain (`allowedhosts`), source IP (`allowips`), or API key (`apikeys`). | server |

### CLI flags

Both `how-server` and `how-client` accept Go-style flags: `-config <file>`,
`--config <file>`, or `-config=<file>`. Defaults: server →
`config.server.example.cfg`; client → `config.client.example.cfg`.
`how-test-api` uses `-addr <host:port>` (default `localhost:8081`).
