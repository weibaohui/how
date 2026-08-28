//! A single websocket connection from a WSP client to a WSP server.
//!
//! The client connects to the server's `/register` endpoint, sends a greeting
//! containing its proxy id and desired pool size, then serves proxied HTTP
//! requests: it reads a serialized `HttpRequest`, executes it locally, and
//! streams the `HttpResponse` and body back.

use crate::client::pool::Pool;
use crate::common::{client_error_status, HttpRequest, HttpResponse};
use crate::log;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;
use tokio_util::sync::CancellationToken;

/// Status of a client connection. Mirrors Go's `CONNECTING/IDLE/RUNNING` iota,
/// with an extra `Closed` for idempotent shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Connecting = 0,
    Idle = 1,
    Running = 2,
    Closed = 3,
}

/// A single websocket connection to a WSP server.
pub struct Connection {
    pub pool: Weak<Pool>,
    status: Mutex<Status>,
    /// Last time any frame was received from the server (pong/data/ping).
    /// Refreshed by the driver on every read; checked by the keepalive task
    /// to detect half-open links (no traffic => the peer or the path is
    /// gone). The client sends a ping every 30s, so on a live link a pong
    /// arrives within ~RTT of each ping. If nothing arrives for
    /// `liveness_timeout`, the tunnel is dead and must be re-established.
    last_activity: Mutex<Instant>,
    cancel: CancellationToken,
    self_weak: Weak<Connection>,
}

impl Connection {
    /// Create a new connection object (status Connecting).
    pub fn new(pool: Weak<Pool>) -> Arc<Self> {
        Arc::new_cyclic(|weak: &Weak<Connection>| Connection {
            pool,
            status: Mutex::new(Status::Connecting),
            last_activity: Mutex::new(Instant::now()),
            cancel: CancellationToken::new(),
            self_weak: weak.clone(),
        })
    }

    /// Last time any frame was received from the peer (used by the keepalive
    /// task to detect half-open links).
    fn last_activity(&self) -> Instant {
        *self.last_activity.lock().unwrap()
    }

    /// Dial the server and, on success, spawn the driver, serve and keepalive
    /// tasks.
    pub async fn connect(
        &self,
        target: &str,
        secret_key: &str,
        pool_idle_size: i64,
        proxy_id: String,
        http_client: reqwest::Client,
        config: Arc<crate::client::ClientConfig>,
    ) -> Result<(), String> {
        log::log(format!("Connecting to {}", target));

        let mut req = target
            .to_string()
            .into_client_request()
            .map_err(|e| format!("{}", e))?;
        let hv = http::HeaderValue::from_str(secret_key)
            .unwrap_or_else(|_| http::HeaderValue::from_static(""));
        req.headers_mut().insert("X-SECRET-KEY", hv);

        // Dial the TCP stream ourselves so we can set TCP_NODELAY (avoids
        // delayed-ACK stalls of ~40ms on small frames). TLS is expected to be
        // terminated by an external reverse proxy in front of the server (as
        // documented in the original project); wss:// is therefore rejected.
        if target.starts_with("wss://") {
            return Err("wss:// is not supported; terminate TLS with a reverse \
                        proxy and connect with ws://"
                .to_string());
        }
        let host = req.uri().host().unwrap_or("127.0.0.1").to_string();
        let port = req.uri().port_u16().unwrap_or(80);
        let stream = tokio::net::TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|e| format!("{}", e))?;
        let _ = stream.set_nodelay(true);
        let (ws, _resp) = tokio_tungstenite::client_async(req, stream)
            .await
            .map_err(|e| format!("{}", e))?;

        log::log(format!("Connected to {}", target));

        // Greeting: `<id>_<pool_idle_size>`.
        let greeting = format!("{}_{}", proxy_id, pool_idle_size);

        let (read_tx, read_rx) = mpsc::channel::<Message>(8);
        let (write_tx, write_rx) = mpsc::channel::<Message>(8);

        let mut ws = ws;
        if ws.send(Message::text(greeting)).await.is_err() {
            self.shutdown();
            return Err("greeting error".to_string());
        }

        let this = match self.self_weak.upgrade() {
            Some(a) => a,
            None => return Err("connection gone".to_string()),
        };

        let cancel = self.cancel.clone();
        let driver_cancel = cancel.child_token();
        let liveness_timeout = Duration::from_millis(config.liveness_timeout as u64);

        // Driver: owns the stream, multiplexes reads/writes.
        let driver_conn = this.clone();
        tokio::spawn(async move {
            driver(driver_conn, ws, write_rx, read_tx, driver_cancel).await;
        });

        // Serve: reads requests, executes them, writes responses.
        let serve_conn = this.clone();
        tokio::spawn(serve(serve_conn, read_rx, write_tx.clone(), http_client, config));

        // Keepalive: send a ping every 30s (keeps NAT/firewall idle timers
        // from dropping the tunnel) and reap the connection when no frame at
        // all has been received for `liveness_timeout` (a half-open link the
        // pings cannot revive). Only the client can re-establish the tunnel
        // (the server cannot dial back into the private network), so this
        // self-heal is what keeps the pool warm overnight.
        let keepalive_conn = this.clone();
        tokio::spawn(keepalive_loop(
            keepalive_conn,
            write_tx,
            cancel,
            liveness_timeout,
            Duration::from_secs(10),
            Duration::from_secs(30),
        ));

        Ok(())
    }

    /// Close the connection: cancel the driver/serve/ping and remove it from
    /// the pool. Idempotent.
    pub fn shutdown(&self) {
        let already = {
            let mut s = self.status.lock().unwrap();
            let was = *s == Status::Closed;
            *s = Status::Closed;
            was
        };
        if already {
            return;
        }
        self.cancel.cancel();
        if let Some(pool) = self.pool.upgrade() {
            pool.remove(self);
        }
    }

    pub fn status(&self) -> Status {
        *self.status.lock().unwrap()
    }
    pub fn set_status(&self, s: Status) {
        *self.status.lock().unwrap() = s;
    }
}

/// The background driver task. It owns the websocket stream and multiplexes
/// incoming reads with outgoing writes.
async fn driver<S>(
    conn: Arc<Connection>,
    mut stream: WebSocketStream<S>,
    mut write_rx: mpsc::Receiver<Message>,
    read_tx: mpsc::Sender<Message>,
    cancel: CancellationToken,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let mut pending_pong: Option<bytes::Bytes> = None;
    loop {
        if let Some(p) = pending_pong.take() {
            if stream.send(Message::Pong(p)).await.is_err() {
                break;
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            msg = stream.next() => {
                match msg {
                    Some(Ok(m)) => {
                        // Any frame received (ping/pong/data/close) proves the
                        // peer and the path are alive; refresh the liveness
                        // timestamp the keepalive task uses to detect
                        // half-open links.
                        *conn.last_activity.lock().unwrap() = Instant::now();
                        match m {
                            Message::Ping(p) => {
                                pending_pong = Some(p);
                            }
                            Message::Pong(_) => {}
                            Message::Close(_) => break,
                            mm => {
                                if read_tx.send(mm).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Some(Err(_)) => break,
                    None => break,
                }
            }
            out = write_rx.recv() => {
                match out {
                    Some(m) => {
                        if stream.send(m).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }
    // Dropping read_tx unblocks the serve loop (recv -> None).
    drop(read_tx);
}

/// Keepalive task: send a WebSocket `ping` every `ping_interval` (periodic
/// outbound traffic keeps NAT / firewall idle timers from silently dropping a
/// quiet link after a few hours) AND detect half-open links by reaping the
/// connection when no frame at all (pong/data) has been received for
/// `liveness_timeout`. Sending pings alone is not enough: a ping that never
/// gets a pong means the peer or the path is gone, and without this check the
/// client would hold a pool full of dead connections and never reconnect
/// (while the server reaps its side and reports "no proxy available").
///
/// Only **Idle** connections are reaped: a Running connection is actively
/// proxying a request and is demonstrably alive. During a long streamed
/// response the shared write queue can stay full (dropping pings via
/// `try_send`), and the server only pongs in response to a ping, so
/// `last_activity` is not refreshed mid-response — reaping then would break an
/// in-flight request (the streaming-LLM backpressure case). This mirrors the
/// server, which only liveness-reaps Idle (not Busy) connections.
///
/// `check_interval` is how often the loop wakes to run the liveness check
/// (and maybe send a ping); `ping_interval` is the cadence of pings.
/// Production passes 10s / 30s; tests pass tiny values for speed.
async fn keepalive_loop(
    conn: Arc<Connection>,
    write_tx: mpsc::Sender<Message>,
    cancel: CancellationToken,
    liveness_timeout: Duration,
    check_interval: Duration,
    ping_interval: Duration,
) {
    let mut next_ping = Instant::now();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(check_interval) => {
                // Liveness check FIRST: if no frame from the peer for longer
                // than the timeout, the tunnel is half-open. Close it; the
                // pool connector (runs every 1s) dials a replacement. This is
                // the authoritative reaper — it must not be delayed by a
                // backed-up ping send, so the ping below is best-effort.
                //
                // Only reap IDLE connections: a Running connection is
                // actively proxying a request (exchanging data right now),
                // so it is demonstrably alive. During a long streamed
                // response the shared write queue can stay full, dropping
                // keepalive pings (try_send below); since the server only
                // pongs in response to a ping it never proactively sends, no
                // pong refreshes `last_activity`. Reaping such a connection
                // mid-response would break an in-flight request — exactly the
                // streaming-LLM backpressure case. Mirrors the server, which
                // only liveness-reaps Idle (not Busy) connections.
                let st = conn.status();
                let idle = conn.last_activity().elapsed();
                if st == Status::Idle && idle > liveness_timeout {
                    log::log(format!(
                        "Reaping half-open tunnel: no frame from server for {}ms",
                        idle.as_millis()
                    ));
                    conn.shutdown();
                    break;
                }
                // Send a keepalive ping at the ping interval. Non-blocking:
                // if the write channel is full (the driver is wedged on a
                // dead link's send buffer, or a streamed response is filling
                // it) we just skip this ping. On a dead IDLE link the
                // liveness check above reaps shortly; on a live RUNNING link
                // the data exchange itself keeps the peer alive and the Idle
                // gate prevents a false reap. This keeps the keepalive task
                // responsive so it never stops running the liveness check.
                if Instant::now() >= next_ping {
                    match write_tx.try_send(Message::Ping(Vec::new().into())) {
                        Ok(()) => next_ping = Instant::now() + ping_interval,
                        Err(mpsc::error::TrySendError::Full(_)) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
            }
        }
    }
}

/// The serve loop: wait to receive HTTP requests, execute them locally, and
/// send HTTP responses back to the server.
async fn serve(
    conn: Arc<Connection>,
    mut read_rx: mpsc::Receiver<Message>,
    write_tx: mpsc::Sender<Message>,
    http_client: reqwest::Client,
    config: Arc<crate::client::ClientConfig>,
) {
    loop {
        conn.set_status(Status::Idle);
        let req_msg = match read_rx.recv().await {
            Some(Message::Text(t)) => t.as_str().to_string(),
            Some(Message::Pong(_)) => continue,
            Some(Message::Ping(p)) => {
                let _ = write_tx.send(Message::Pong(p)).await;
                continue;
            }
            Some(_) => break,
            None => break,
        };

        conn.set_status(Status::Running);

        // Trigger a pool refresh to open new connections if needed.
        if let Some(pool) = conn.pool.upgrade() {
            let p = pool.clone();
            tokio::spawn(async move {
                p.connector();
            });
        }

        // Deserialize the request.
        let http_req: HttpRequest = match serde_json::from_str(&req_msg) {
            Ok(r) => r,
            Err(e) => {
                let _ = send_error(
                    &write_tx,
                    &format!("Unable to deserialize json http request : {}\n", e),
                )
                .await;
                break;
            }
        };

        log::log(format!("[{}] {}", http_req.method, http_req.url));

        // Apply the client's blacklist / whitelist.
        let denied = check_rules(&config, &http_req);
        if let Some(reason) = denied {
            // Discard the (streamed) request body up to the end-marker.
            if !discard_body(&mut read_rx).await {
                break;
            }
            let _ = send_error(&write_tx, &reason).await;
            continue;
        }

        // Resolve the route: the arrival host (from the request URL, e.g.
        // "127.0.0.1:8080") maps to a configured upstream base URL. All headers
        // (Authorization, Content-Type, custom) are forwarded transparently —
        // the proxy never injects or rewrites secrets.
        let method = match reqwest::Method::from_bytes(http_req.method.as_bytes()) {
            Ok(m) => m,
            Err(_) => {
                let _ = discard_body(&mut read_rx).await;
                let _ = send_error(&write_tx, "Unable to build request method\n").await;
                continue;
            }
        };
        let parsed = match reqwest::Url::parse(&http_req.url) {
            Ok(u) => u,
            Err(e) => {
                let _ = discard_body(&mut read_rx).await;
                let _ = send_error(
                    &write_tx,
                    &format!("Unable to parse request url : {}\n", e),
                )
                .await;
                continue;
            }
        };
        let host = parsed.host_str().unwrap_or("");
        let authority = match parsed.port() {
            Some(p) => format!("{}:{}", host, p),
            None => host.to_string(),
        };
        let upstream_base = match config
            .routes
            .get(&authority)
            .or_else(|| config.routes.get(host))
        {
            Some(u) => u.trim_end_matches('/').to_string(),
            None => {
                let _ = discard_body(&mut read_rx).await;
                let _ = send_error(&write_tx, &format!("No route for {}\n", authority)).await;
                continue;
            }
        };
        let path_and_query = match parsed.query() {
            Some(q) => format!("{}?{}", parsed.path(), q),
            None => parsed.path().to_string(),
        };
        let upstream_url = format!("{}{}", upstream_base, path_and_query);
        log::log(format!("route {} -> {}", authority, upstream_url));
        let url = match reqwest::Url::parse(&upstream_url) {
            Ok(u) => u,
            Err(e) => {
                let _ = discard_body(&mut read_rx).await;
                let _ = send_error(
                    &write_tx,
                    &format!("Unable to parse upstream url : {}\n", e),
                )
                .await;
                continue;
            }
        };
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, values) in &http_req.header {
            let hname = match reqwest::header::HeaderName::from_bytes(name.as_bytes()) {
                Ok(n) => n,
                Err(_) => continue,
            };
            for v in values {
                if let Ok(hv) = reqwest::header::HeaderValue::from_str(v) {
                    headers.append(hname.clone(), hv);
                }
            }
        }

        // Stream the request body to the upstream: read binary chunks off the
        // websocket and feed them into a reqwest streaming body, concurrently
        // with send() (which pulls the stream). `body_tx` is owned by the
        // producer so it is dropped when the body ends, signalling end-of-body
        // to reqwest; `read_rx` is borrowed so the serve loop keeps it.
        let (body_tx, body_rx) =
            mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(16);
        let req_body = reqwest::Body::wrap_stream(tokio_stream::wrappers::ReceiverStream::new(
            body_rx,
        ));
        let reqwest_req = http_client
            .request(method, url)
            .headers(headers)
            .body(req_body);
        let read_rx_ref = &mut read_rx;
        let producer = async move {
            loop {
                match read_rx_ref.recv().await {
                    Some(Message::Binary(b)) if b.is_empty() => break, // end-marker
                    Some(Message::Binary(b)) => {
                        if body_tx.send(Ok(b)).await.is_err() {
                            break;
                        }
                    }
                    Some(_) | None => break,
                }
            }
            // body_tx dropped here -> reqwest sees end-of-body.
        };
        let resp = match tokio::join!(producer, reqwest_req.send()).1 {
            Ok(r) => r,
            Err(e) => {
                let _ = send_error(
                    &write_tx,
                    &format!("Unable to execute request : {}\n", e),
                )
                .await;
                continue;
            }
        };

        // Serialize the response header. Content length is unknown for a
        // streamed body (-1); the server does not use it for streaming.
        let status = resp.status().as_u16();
        let mut resp_headers: HashMap<String, Vec<String>> = HashMap::new();
        for (name, value) in resp.headers().iter() {
            let key = name.as_str().to_string();
            let val = value.to_str().unwrap_or("").to_string();
            resp_headers.entry(key).or_default().push(val);
        }
        let http_resp = HttpResponse {
            status_code: status,
            header: resp_headers,
            content_length: -1,
        };
        let json = match serde_json::to_string(&http_resp) {
            Ok(s) => s,
            Err(e) => {
                let _ = send_error(
                    &write_tx,
                    &format!("Unable to serialize response : {}\n", e),
                )
                .await;
                continue;
            }
        };

        // Send the response header immediately (so the caller gets status +
        // headers before the body completes), then stream the body chunk by
        // chunk (one binary message per upstream chunk) and finally send an
        // empty binary message to mark end-of-body.
        if write_tx.send(Message::text(json)).await.is_err() {
            break;
        }
        let mut stream_ok = true;
        {
            let mut s = resp.bytes_stream();
            loop {
                match s.next().await {
                    Some(Ok(b)) => {
                        if b.is_empty() {
                            continue;
                        }
                        if write_tx.send(Message::binary(b)).await.is_err() {
                            stream_ok = false;
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        log::log(format!("Unable to pipe response body : {}", e));
                        stream_ok = false;
                        break;
                    }
                    None => break,
                }
            }
        }
        if !stream_ok {
            break;
        }
        // End-of-body marker.
        if write_tx.send(Message::binary(Vec::new())).await.is_err() {
            break;
        }
    }

    conn.shutdown();
}

/// Check the client's blacklist/whitelist against a request. Returns the
/// denial reason if the request must be denied, else `None`.
fn check_rules(
    config: &crate::client::ClientConfig,
    req: &HttpRequest,
) -> Option<String> {
    if !config.blacklist.is_empty() {
        for rule in &config.blacklist {
            if rule.matches(&req.method, &req.url, &req.header) {
                return Some("Destination is forbidden".to_string());
            }
        }
    }
    if !config.whitelist.is_empty() {
        let mut allowed = false;
        for rule in &config.whitelist {
            if rule.matches(&req.method, &req.url, &req.header) {
                allowed = true;
                break;
            }
        }
        if !allowed {
            return Some("Destination is not allowed\n".to_string());
        }
    }
    None
}

/// Drain the (streamed) request body, reading binary messages until the empty
/// end-marker. Returns false on an unexpected message or closed channel (the
/// caller should break the serve loop).
async fn discard_body(read_rx: &mut mpsc::Receiver<Message>) -> bool {
    loop {
        match read_rx.recv().await {
            Some(Message::Binary(b)) if b.is_empty() => return true,
            Some(Message::Binary(_)) => continue,
            Some(_) | None => return false,
        }
    }
}

/// Send a 527 error response (header JSON + body + end marker) to the server.
async fn send_error(write_tx: &mpsc::Sender<Message>, msg: &str) -> Result<(), ()> {
    let resp = HttpResponse {
        status_code: client_error_status(),
        header: HashMap::new(),
        content_length: msg.len() as i64,
    };
    let json = serde_json::to_string(&resp).map_err(|_| ())?;
    write_tx.send(Message::text(json)).await.map_err(|_| ())?;
    write_tx
        .send(Message::binary(msg.as_bytes().to_vec()))
        .await
        .map_err(|_| ())?;
    // End-of-body marker.
    write_tx.send(Message::binary(Vec::new())).await.map_err(|_| ())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Tests for the client-side keepalive / liveness logic.
    //!
    //! These exercise `keepalive_loop` directly (no real websocket / network):
    //! a connection whose `last_activity` goes stale beyond `liveness_timeout`
    //! must be reaped (shutdown) so the pool connector dials a replacement,
    //! while a connection that keeps receiving frames (pong/data) must be
    //! left alone. This is the self-heal that keeps the pool warm after an
    //! idle night — without it the client holds a pool of dead, half-open
    //! tunnels and the server reports "no proxy available".
    //!
    //! Uses real (not paused) time with tiny intervals, so `last_activity`
    //! (an `std::time::Instant` = the real OS clock) actually elapses. We
    //! never construct a past `Instant` via subtraction (that can panic if it
    //! would predate the monotonic epoch); staleness is achieved by simply
    //! not refreshing `last_activity` and letting real time pass the tiny
    //! `liveness_timeout`.

    use super::*;
    use crate::client::{new_config, ClientInner};
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    /// Build a pool handle (no real network) so `Connection::shutdown` can
    /// upgrade its `Weak<Pool>` and call `pool.remove` (a no-op when the
    /// connection was never inserted, which is fine here).
    fn dummy_pool() -> Arc<Pool> {
        let config = Arc::new(new_config());
        let inner = Arc::new(ClientInner {
            config,
            http_client: reqwest::Client::new(),
        });
        Pool::new(
            inner,
            "ws://test.invalid/register".to_string(),
            "k".to_string(),
            CancellationToken::new(),
        )
    }

    /// A connection whose peer has gone silent (no frame received for longer
    /// than the liveness timeout) is reaped: the keepalive task shuts it down
    /// so the pool connector re-establishes a fresh tunnel.
    #[tokio::test(flavor = "current_thread")]
    async fn keepalive_reaps_silent_tunnel() {
        let pool = dummy_pool();
        let conn = Connection::new(Arc::downgrade(&pool));
        let cancel = conn.cancel.clone();
        let (write_tx, _rx) = mpsc::channel::<Message>(8);
        conn.set_status(Status::Idle);
        // last_activity starts "now"; we do NOT refresh it, so real time makes
        // it stale. liveness (30ms) > check (20ms): the first wake (20ms) sees
        // idle=20ms < 30ms (no reap, sends a ping), the second wake (40ms)
        // sees idle=40ms > 30ms and reaps -> exercises >1 loop iteration.
        let handle = tokio::spawn(keepalive_loop(
            conn.clone(),
            write_tx,
            cancel,
            Duration::from_millis(30),
            Duration::from_millis(20),
            Duration::from_millis(50),
        ));

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if conn.status() == Status::Closed {
                break;
            }
            if Instant::now() > deadline {
                panic!("a silent (half-open) tunnel must be reaped by the keepalive task");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let _ = handle.await;
    }

    /// A connection that keeps receiving frames (a "live" peer that pongs) is
    /// NOT reaped: the keepalive task must leave healthy tunnels alone.
    #[tokio::test(flavor = "current_thread")]
    async fn keepalive_keeps_live_tunnel() {
        let pool = dummy_pool();
        let conn = Connection::new(Arc::downgrade(&pool));
        let cancel = conn.cancel.clone();
        let (write_tx, _rx) = mpsc::channel::<Message>(8);
        conn.set_status(Status::Idle);

        let liveness = Duration::from_millis(30);
        let check = Duration::from_millis(20);
        let ping = Duration::from_millis(50);

        // Simulate the driver refreshing last_activity on every received pong
        // (well within the liveness window), as a live peer would.
        let conn_for_refresher = conn.clone();
        let refresher = tokio::spawn(async move {
            loop {
                *conn_for_refresher.last_activity.lock().unwrap() = Instant::now();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        let handle = tokio::spawn(keepalive_loop(
            conn.clone(),
            write_tx,
            cancel.clone(),
            liveness,
            check,
            ping,
        ));

        // Let the keepalive loop run well past several check-intervals (real
        // time). The connection must stay alive.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_ne!(
            conn.status(),
            Status::Closed,
            "a live tunnel that keeps receiving frames must not be reaped"
        );

        handle.abort();
        refresher.abort();
        cancel.cancel();
    }

    /// A connection that is RUNNING (actively proxying a request) is NOT
    /// reaped even if `last_activity` is stale. During a long streamed
    /// response the shared write queue can stay full, dropping keepalive
    /// pings; the server only pongs in response to a ping, so no pong
    /// refreshes `last_activity`. Reaping then would break an in-flight
    /// request — so the reaper only applies to Idle connections (mirroring
    /// the server, which only liveness-reaps Idle, not Busy, connections).
    #[tokio::test(flavor = "current_thread")]
    async fn keepalive_never_reaps_running_tunnel() {
        let pool = dummy_pool();
        let conn = Connection::new(Arc::downgrade(&pool));
        let cancel = conn.cancel.clone();
        // A capped channel that we NEVER drain, so pings are dropped on every
        // `try_send` (Full) — simulating a streamed response backpressuring the
        // shared write queue. No pong ever comes back, so `last_activity` only
        // grows.
        let (write_tx, _rx) = mpsc::channel::<Message>(8);
        conn.set_status(Status::Running);

        let handle = tokio::spawn(keepalive_loop(
            conn.clone(),
            write_tx,
            cancel.clone(),
            // liveness (30ms) < check (20ms)*several; many wake-ups pass with a
            // stale last_activity while Running.
            Duration::from_millis(30),
            Duration::from_millis(20),
            Duration::from_millis(50),
        ));

        // Run well past several liveness timeouts. The connection must stay
        // alive because it is Running (an in-flight request).
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            conn.status(),
            Status::Running,
            "a Running (in-flight) tunnel must never be reaped, even with a \
             stale last_activity and a full write queue"
        );

        handle.abort();
        cancel.cancel();
    }
}
