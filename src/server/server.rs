//! The WSP server: a reverse HTTP proxy over WebSockets.
//!
//! Clients offer websocket connections at `/register`; HTTP requests at
//! `/request` (with an `X-PROXY-DESTINATION` header) are forwarded to an idle
//! client websocket, executed locally by the client, and the response is
//! streamed back.

use crate::common::{proxy_error_status, HttpRequest, HttpResponse};
use crate::log;
use crate::server::config::Config;
use crate::server::connection::{msg_text, Connection};
use crate::server::pool::Pool;
use bytes::Bytes;
use futures::StreamExt;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::handshake;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::WebSocketStream;

/// The boxed response body type used by the server (allows both buffered
/// `Full` bodies and streaming `Channel` bodies in the same router).
type Boxed = http_body_util::combinators::BoxBody<Bytes, Infallible>;

/// A request from an HTTP handler for an idle proxy connection.
struct ConnRequest {
    tx: oneshot::Sender<Option<Arc<Connection>>>,
    deadline: Instant,
}

/// The shared server state.
struct Inner {
    config: Config,
    pools: Mutex<Vec<Arc<Pool>>>,
    req_tx: mpsc::Sender<ConnRequest>,
    idle_tx: mpsc::Sender<Arc<Connection>>,
}

/// A reverse HTTP Proxy over WebSocket (server side).
pub struct Server {
    inner: Arc<Inner>,
    req_rx: Option<mpsc::Receiver<ConnRequest>>,
    idle_rx: Option<mpsc::Receiver<Arc<Connection>>>,
}

/// Build a proxy error response (HTTP 526) carrying the error message.
/// Mirrors Go's `common.ProxyError` / `ProxyErrorf`.
fn proxy_error_response(msg: &str) -> Response<Boxed> {
    log::log(msg.to_string());
    Response::builder()
        .status(
            StatusCode::from_u16(proxy_error_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        )
        .body(Full::new(Bytes::copy_from_slice(msg.as_bytes())).boxed())
        .unwrap()
}

impl Server {
    /// Create a new Server.
    pub fn new(config: Config) -> Self {
        let (req_tx, req_rx) = mpsc::channel::<ConnRequest>(64);
        let (idle_tx, idle_rx) = mpsc::channel::<Arc<Connection>>(1024);
        let inner = Arc::new(Inner {
            config,
            pools: Mutex::new(Vec::new()),
            req_tx,
            idle_tx,
        });
        Server {
            inner,
            req_rx: Some(req_rx),
            idle_rx: Some(idle_rx),
        }
    }

    /// Start the server: cleaner, dispatcher, and the HTTP listener.
    pub async fn start(mut self) -> Result<(), String> {
        let req_rx = self.req_rx.take().unwrap();
        let idle_rx = self.idle_rx.take().unwrap();

        // Cleaner (every 5 seconds): remove empty pools, log stats.
        {
            let inner = self.inner.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    clean(&inner);
                }
            });
        }

        // Dispatcher: match HTTP request handlers with idle websocket
        // connections (one request at a time, mirroring the original).
        tokio::spawn(dispatch_loop(req_rx, idle_rx));

        // HTTP listener.
        let addr: SocketAddr = format!("{}:{}", self.inner.config.host, self.inner.config.port)
            .parse()
            .map_err(|e| format!("Invalid bind address : {}", e))?;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| format!("{}", e))?;
        log::log(format!(
            "WSP server listening on {}:{}",
            self.inner.config.host, self.inner.config.port
        ));

        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    log::log(format!("accept error : {}", e));
                    continue;
                }
            };
            // Disable Nagle so small WebSocket frames do not stall behind
            // delayed-ACK (~40ms) on the proxy tunnel.
            let _ = stream.set_nodelay(true);
            let peer_ip = peer.ip();
            let inner = self.inner.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req: Request<Incoming>| {
                    let inner = inner.clone();
                    async move { handle(inner, req, peer_ip).await }
                });
                if let Err(e) = http1::Builder::new()
                    .serve_connection(io, svc)
                    .with_upgrades()
                    .await
                {
                    let _ = peer;
                    log::log(format!("http connection error : {}", e));
                }
            });
        }
    }

    /// Shutdown (best effort).
    #[allow(dead_code)]
    pub fn shutdown(&self) {
        let pools = self.inner.pools.lock().unwrap();
        for pool in pools.iter() {
            pool.shutdown();
        }
        let mut pools = self.inner.pools.lock().unwrap();
        pools.clear();
    }
}

/// Remove empty pools and log pool stats. Mirrors Go's `server.clean`.
fn clean(inner: &Arc<Inner>) {
    let mut pools = inner.pools.lock().unwrap();
    if pools.is_empty() {
        return;
    }
    let mut idle = 0usize;
    let mut busy = 0usize;
    let mut kept: Vec<Arc<Pool>> = Vec::new();
    for pool in pools.drain(..) {
        if pool.is_empty() {
            log::log(format!("Removing empty connection pool : {}", pool.id));
            pool.shutdown();
        } else {
            let (i, b, _c) = pool.size_count();
            idle += i;
            busy += b;
            kept.push(pool);
        }
    }
    log::log(format!(
        "{} pools, {} idle, {} busy",
        kept.len(),
        idle,
        busy
    ));
    *pools = kept;
}

/// Dispatcher: for each incoming connection request, find an idle connection
/// within the request's deadline, taking it (BUSY) before handing it back.
async fn dispatch_loop(
    mut req_rx: mpsc::Receiver<ConnRequest>,
    mut idle_rx: mpsc::Receiver<Arc<Connection>>,
) {
    while let Some(req) = req_rx.recv().await {
        let conn = acquire(&mut idle_rx, req.deadline).await;
        let _ = req.tx.send(conn);
    }
}

async fn acquire(
    idle_rx: &mut mpsc::Receiver<Arc<Connection>>,
    deadline: Instant,
) -> Option<Arc<Connection>> {
    loop {
        let remaining = match deadline.checked_duration_since(Instant::now()) {
            Some(d) => d,
            None => return None, // timed out
        };
        match tokio::time::timeout(remaining, idle_rx.recv()).await {
            Err(_) => return None,   // timed out
            Ok(None) => return None, // no idle senders (no proxies)
            Ok(Some(conn)) => {
                if conn.take() {
                    return Some(conn);
                }
                // Stale connection; try again within the remaining time.
                continue;
            }
        }
    }
}

/// HTTP request router. `/register` and `/status` are control endpoints;
/// every other path is a proxied request (transparent catch-all reverse
/// proxy). The real destination is resolved by the client from its `routes`
/// config, not carried in the request.
async fn handle(
    inner: Arc<Inner>,
    req: Request<Incoming>,
    peer_ip: std::net::IpAddr,
) -> Result<Response<Boxed>, std::convert::Infallible> {
    let path = req.uri().path().to_string();
    match path.as_str() {
        "/register" => Ok(handle_register(inner, req).await),
        "/status" => Ok(handle_status()),
        _ => Ok(handle_request(inner, req, peer_ip).await),
    }
}

/// `/status`: simple health check.
fn handle_status() -> Response<Boxed> {
    Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::from_static(b"ok")).boxed())
        .unwrap()
}

/// Build the proxy request context from the incoming HTTP request. The
/// destination URL is reconstructed from the arrival `Host` header and the
/// request path; the client routes it to the configured upstream. All headers
/// (Authorization, Content-Type, custom) are forwarded transparently.
fn build_request(
    inner: &Arc<Inner>,
    req: &Request<Incoming>,
) -> Result<HttpRequest, Box<Response<Boxed>>> {
    // Reconstruct the arrival URL from the Host header + the request path.
    // The client routes it (arrival host -> configured upstream) and appends
    // the path; the real destination is never carried in a request header.
    let host = req
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{}:{}", inner.config.host, inner.config.port));

    // Bound-domain validation: when `allowed_hosts` is configured, the request
    // must arrive on one of the listed hostnames. Requests addressed by IP
    // (or any unlisted host) are rejected with 403, so callers cannot bypass
    // the bound domain by hitting the server's IP directly.
    if !inner.config.allowed_hosts.is_empty() {
        let hostname = host_name(&host);
        if hostname.parse::<std::net::IpAddr>().is_ok() {
            return Err(Box::new(forbidden("Host must be a domain, not an IP")));
        }
        let allowed = inner
            .config
            .allowed_hosts
            .iter()
            .any(|h| h.eq_ignore_ascii_case(&hostname));
        if !allowed {
            return Err(Box::new(forbidden(&format!(
                "Host not allowed: {hostname}"
            ))));
        }
    }

    let path = req.uri().path();
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let url = format!("http://{host}{path}{query}");

    // Forward all headers transparently (Authorization, Content-Type, custom).
    // Hop-by-hop / framing headers are managed by the HTTP layer, not copied.
    let method = req.method().to_string();
    let mut header: HashMap<String, Vec<String>> = HashMap::new();
    for (name, value) in req.headers().iter() {
        let name = name.as_str();
        if name.eq_ignore_ascii_case("host")
            || name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("transfer-encoding")
            || name.eq_ignore_ascii_case("connection")
        {
            continue;
        }
        let entry = header.entry(name.to_string()).or_default();
        entry.push(value.to_str().unwrap_or("").to_string());
    }
    let content_length = req
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    log::log(format!("[{}] {}", method, url));

    Ok(HttpRequest {
        method,
        url,
        header,
        content_length,
    })
}

/// Extract the hostname from a `Host` header value (strip the port and any
/// IPv6 brackets), e.g. "example.com:8080" -> "example.com", "1.2.3.4" -> "1.2.3.4".
fn host_name(host_header: &str) -> String {
    let mut h = host_header;
    // Strip the port (suffix after the last ':' if it is numeric).
    if let Some(idx) = h.rfind(':') {
        let after = &h[idx + 1..];
        if !after.is_empty() && after.chars().all(|c| c.is_ascii_digit()) {
            h = &h[..idx];
        }
    }
    // Strip IPv6 brackets, e.g. "[::1]" -> "::1".
    h.trim_start_matches('[').trim_end_matches(']').to_string()
}

/// Build a 403 Forbidden response with a message body.
fn forbidden(msg: &str) -> Response<Boxed> {
    log::log(msg.to_string());
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .body(Full::new(Bytes::copy_from_slice(msg.as_bytes())).boxed())
        .unwrap()
}

/// `/request`: forward an HTTP request through an idle proxy connection.
async fn handle_request(
    inner: Arc<Inner>,
    req: Request<Incoming>,
    peer_ip: std::net::IpAddr,
) -> Response<Boxed> {
    // --- Gatekeeper checks (before touching the proxy pool / backend) ---

    // 1) Source IP whitelist.
    if !inner.config.allowips.is_empty() {
        let allowed = inner
            .config
            .allowips
            .iter()
            .any(|ip| *ip == peer_ip.to_string());
        if !allowed {
            return forbidden(&format!("DENY {}", peer_ip));
        }
    }

    // 2) API key validation (Authorization: Bearer <key>).
    if !inner.config.apikeys.is_empty() {
        let token = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("Bearer"))
            .map(|(_, token)| token.trim().to_string());
        match token {
            Some(t) if inner.config.apikeys.iter().any(|k| k == &t) => {}
            Some(_) => return forbidden("Invalid API key"),
            None => return forbidden("Missing or invalid Authorization header"),
        }
    }

    let http_req = match build_request(&inner, &req) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };

    // No proxy available.
    if inner.pools.lock().unwrap().is_empty() {
        return proxy_error_response("No proxy available");
    }

    // Acquire an idle proxy connection.
    let (tx, rx) = oneshot::channel();
    let deadline = Instant::now() + Duration::from_millis(inner.config.timeout as u64);
    if inner
        .req_tx
        .send(ConnRequest { tx, deadline })
        .await
        .is_err()
    {
        return proxy_error_response("Unable to get a proxy connection");
    }
    let conn = match rx.await {
        Ok(Some(c)) => c,
        _ => return proxy_error_response("Unable to get a proxy connection"),
    };

    // 从发送请求头到接收响应头的全链路超时（`upstreamtimeout` 配置；
    // 0/未配置 = 不限时。只覆盖到响应头为止，响应 body 之后流式回传、
    // 不受它限制）。
    //
    // 未配置时沿用原始行为（无限等待）；配置后客户端卡在慢上游时，
    // 调用方最长只等这个期限，即使 WebSocket 链路异常导致客户端错误
    // 传不回来，server 也能自行解挂。
    let upstream_roundtrip_deadline = (inner.config.upstream_timeout > 0)
        .then(|| Duration::from_millis(inner.config.upstream_timeout as u64));
    let req_url = http_req.url.clone();
    let method = http_req.method.clone();
    let roundtrip_start = Instant::now();
    let upstream_future = async {
        // 逐帧流式把请求体写给远端 how-client，最后用空消息标记结束。
        conn.send_request_header(&http_req)
            .await
            .map_err(|e| (e.clone(), e))?;
        {
            use http_body_util::BodyExt as _;
            let mut bs = req.into_body().into_stream();
            while let Some(frame) = bs.next().await {
                let frame = frame.map_err(|e| {
                    let msg = format!("unable to read request body : {}", e);
                    (msg.clone(), msg)
                })?;
                if let Some(data) = frame.data_ref() {
                    if data.is_empty() {
                        continue;
                    }
                    conn.send_body_chunk(data.clone())
                        .await
                        .map_err(|e| (e.clone(), e))?;
                }
            }
        }
        conn.send_body_end().await.map_err(|e| (e.clone(), e))?;

        // 接收响应头。远端 how-client 一拿到上游的 headers 就会立刻发回
        // （不等 body），因此调用方可以尽快得到状态码和 headers。
        let resp = conn
            .recv_response_header()
            .await
            .map_err(|e| (e.clone(), e))?;
        Ok::<HttpResponse, (String, String)>(resp)
    };

    // (HttpResponse, Err((log_msg, user_msg)))，未配置 upstreamtimeout 时
    // 没有超时分支。
    let roundtrip = match upstream_roundtrip_deadline {
        Some(d) => match tokio::time::timeout(d, upstream_future).await {
            Ok(r) => r,
            Err(_) => {
                let waited_ms = roundtrip_start.elapsed().as_millis();
                log::log(format!(
                    "代理往返超时（upstreamtimeout={}ms）：[{}] {} 已等待={waited_ms}ms",
                    inner.config.upstream_timeout, method, req_url,
                ));
                conn.close();
                return proxy_error_response(&format!(
                    "Proxy request timed out waiting for upstream ({}ms)",
                    inner.config.upstream_timeout
                ));
            }
        },
        None => upstream_future.await,
    };
    let http_resp = match roundtrip {
        Ok(h) => {
            log::log(format!(
                "代理往返成功：[{}] {} 状态码={} 耗时={}ms",
                method,
                req_url,
                h.status_code,
                roundtrip_start.elapsed().as_millis()
            ));
            h
        }
        Err((log_msg, user_msg)) => {
            log::log(format!(
                "代理往返失败：[{}] {} 原因={} 耗时={}ms",
                method,
                req_url,
                log_msg,
                roundtrip_start.elapsed().as_millis()
            ));
            conn.close();
            return proxy_error_response(&user_msg);
        }
    };

    // Create a streaming body channel. The drain task pulls response body
    // chunks off the websocket and pushes them here as they arrive, so the
    // caller receives them incrementally (streaming preserved).
    let (body_tx, body_rx) = http_body_util::channel::Channel::<Bytes, Infallible>::new(16);
    let conn2 = conn.clone();
    tokio::spawn(async move {
        match conn2.drain_response_body(body_tx).await {
            // Clean end-of-body marker -> return the connection to the pool.
            Ok(()) => conn2.release(),
            // The stream broke mid-body -> throw the connection away.
            Err(_) => conn2.close(),
        }
    });

    let mut builder = Response::builder()
        .status(StatusCode::from_u16(http_resp.status_code).unwrap_or(StatusCode::OK));
    for (name, values) in &http_resp.header {
        // Let hyper set framing from the streaming body; never forward
        // hop-by-hop / length headers from the upstream.
        if name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("transfer-encoding")
            || name.eq_ignore_ascii_case("connection")
        {
            continue;
        }
        for value in values {
            builder = builder.header(name.clone(), value.clone());
        }
    }
    builder
        .body(body_rx.boxed())
        .unwrap_or_else(|_| proxy_error_response("Unable to build response"))
}

/// `/register`: accept a websocket offered by a remote WSP client.
async fn handle_register(inner: Arc<Inner>, req: Request<Incoming>) -> Response<Boxed> {
    // Secret key check.
    let secret = req
        .headers()
        .get("X-SECRET-KEY")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    if secret != inner.config.secret_key {
        return proxy_error_response("Invalid X-SECRET-KEY");
    }

    // Build the 101 Switching Protocols response.
    let key = req
        .headers()
        .get("sec-websocket-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let accept = handshake::derive_accept_key(key.as_bytes());
    let resp = Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-accept", accept)
        .body(Full::new(Bytes::new()).boxed())
        .unwrap();

    // Spawn the upgraded websocket handler.
    let inner_clone = inner.clone();
    tokio::spawn(async move {
        let upgraded = match hyper::upgrade::on(req).await {
            Ok(u) => u,
            Err(e) => {
                log::log(format!("Unable to upgrade : {}", e));
                return;
            }
        };
        let io = TokioIo::new(upgraded);
        let ws = WebSocketStream::from_raw_socket(io, Role::Server, None).await;
        register_websocket(inner_clone, ws).await;
    });

    resp
}

/// Read the greeting message (`<id>_<poolsize>`), find or create the pool for
/// the client id, update its size, and register the websocket connection.
async fn register_websocket<S>(inner: Arc<Inner>, mut ws: WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    // The first message should contain the remote proxy id and pool size.
    let greeting = match ws.next().await {
        Some(Ok(msg)) => msg,
        _ => {
            log::log("Unable to read greeting message");
            return;
        }
    };
    let greeting_text = match msg_text(&greeting) {
        Some(t) => t,
        None => {
            log::log("Unable to parse greeting message");
            return;
        }
    };
    let parts: Vec<&str> = greeting_text.splitn(2, '_').collect();
    if parts.len() != 2 {
        log::log("Unable to parse greeting message");
        return;
    }
    let id = parts[0].to_string();
    let size: usize = match parts[1].parse() {
        Ok(n) => n,
        Err(_) => {
            log::log("Unable to parse greeting message");
            return;
        }
    };

    let pool = {
        let mut pools = inner.pools.lock().unwrap();
        let existing = pools.iter().find(|p| p.id == id).cloned();
        match existing {
            Some(p) => p,
            None => {
                let p = Pool::new(
                    id.clone(),
                    inner.idle_tx.clone(),
                    inner.config.idle_timeout,
                    inner.config.liveness_timeout,
                );
                pools.push(p.clone());
                p
            }
        }
    };
    pool.set_size(size);
    pool.register(ws);
}
