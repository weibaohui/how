# HOW — HTTP On WebSocket

A transparent reverse HTTP proxy over WebSockets, written in Rust and inspired
by [root-gg/wsp](https://github.com/root-gg/wsp) but **redesigned** (the caller
uses it like any reverse proxy; the real upstream lives in the client config).

**HOW** = **H**TTP **O**n **W**ebsocket: a HOW client runs inside an internal
network (alongside the APIs) and dials **out** to a remote HOW server over a
WebSocket, so no inbound firewall hole is needed on the internal side. A caller
sends a normal HTTP request to the server (any path, with its own headers); the
server forwards it transparently to a HOW client, which routes it to a
configured upstream and streams the response back. All headers (Authorization,
Content-Type, custom) are forwarded transparently; only the
**arrival-host → upstream** mapping is configured.

```
            HTTP request (any path, with Auth)        websocket (outbound dial)
  caller  ──────────────────────────────►  HOW server  ═══════════════►  HOW client  ──► internal API
   (curl)  http://server/chat/completions     /register                          routes host→upstream,
           Authorization: Bearer …            (catch-all proxy)                  executes, streams back
                                            /status
```
 

## Build

Requires the Rust toolchain.

```bash
```bash
make release         # builds target/release/{how-server,how-client,how-test-api}
# or: cargo build --release
```

Produces `target/{debug,release}/how-server`, `how-client`, `how-test-api`.

## Run

Start a server (default binds `127.0.0.1:8080`):

```bash
make run-server
# or: ./target/release/how-server -config config.example.cfg
```

Start a client. Its `routes` maps the host the caller targets on the server to
the real upstream base URL:

```bash
make run-client
# or: ./target/release/how-client -config config.client.example.cfg
```

Then the caller just sends a normal HTTP request to the server (with its own
Auth). The path and headers are forwarded transparently; the real destination
is resolved from the client's `routes`:

```bash
# client routes: "127.0.0.1:8080" -> "https://api.example.com/v1"
curl http://127.0.0.1:8080/chat/completions \
     -H 'Authorization: Bearer <your-key>' \
     -H 'Content-Type: application/json' \
     -d '{"model":"...","messages":[...]}'
```

Both binaries accept Go-style flags `-config <file>` (single-dash long form), as
well as `--config <file>` and `-config=<file>`. `how-test-api` uses `-addr`.

## Prebuilt binaries (CI)

Pushing a `v*` tag triggers `.github/workflows/release.yml`, which cross-builds
and publishes a GitHub release with one tarball per architecture, each
containing `how-server`, `how-client`, `how-test-api` and the example configs:

- `how-linux-x64.tar.gz` — Linux x86_64
- `how-linux-arm64.tar.gz` — Linux aarch64
- `how-darwin-arm64.tar.gz` — macOS Apple Silicon

```bash
tar -xzf how-linux-x64.tar.gz
./how-server -config config.example.cfg
```

## Configuration

### Server (`config.example.cfg`)

```yaml
---
host : 127.0.0.1            # bind address
port : 8080                 # bind port
timeout : 1000              # ms to wait before acquiring a WS connection
idletimeout : 60000         # ms before closing excess idle connections
secretkey : ThisIsASecret   # must match the client's secretkey
```

The server is a transparent catch-all reverse proxy: every path except
`/register` and `/status` is forwarded to a HOW client. There is no
caller-provided destination header — the real upstream is configured on the
client.

### Client (`config.client.example.cfg`)

```yaml
---
targets :                          # HOW servers to dial out to
 - ws://127.0.0.1:8080/register
poolidlesize : 10                  # idle WS connections to keep per server
poolmaxsize : 100                   # max concurrent WS connections per server
secretkey : ThisIsASecret          # must match the server's secretkey
# Optional client-side access control (applied to the arrival URL):
#blacklist :
# - method : ".*"
#   url : ".*forbidden.*"
# Routes: arrival host (what the caller targets on the server) -> upstream base.
# The client appends the request path to the upstream base. All headers
# (Authorization, etc.) are forwarded transparently.
routes :
 "127.0.0.1:8080" : "http://internal-api.local"
 "llm.example.com" : "https://api.openai.com/v1"
```

The client generates a random id on startup (overridable via the `id` key).

## End-to-end tests

Two suites drive **real HTTP requests** through real binaries:

```bash
cargo test --test e2e -- --test-threads=1   # Rust (run sequentially due to ephemeral-port selection)
./scripts/e2e.sh                            # Shell (curl)
```

Cover: GET/POST/header/custom-status(666) forwarding, header transparency,
client-side blacklist (527), no-proxy (526), no-route (527), secret-key
rejection, binary body integrity (1 MiB), pool saturation → dispatcher timeout,
SSE streaming (`text/event-stream` delivered token-by-token).

### LLM through the proxy

Point any OpenAI-compatible client at the server (its base URL becomes the
server's host), and carry your own `Authorization` — it is forwarded
transparently. The real LLM URL lives in the client's `routes`. Both
non-streaming and streaming (SSE) work; the SSE stream is preserved end-to-end
(first token arrives well before the response completes).

```bash
# client routes: "127.0.0.1:8080" -> "https://api.example.com/v1"
curl http://127.0.0.1:8080/chat/completions \
  -H "Authorization: Bearer $YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"...","messages":[{"role":"user","content":"hello"}],"stream":true}'
```
