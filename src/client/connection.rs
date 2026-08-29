//! A single websocket connection from a WSP client to a WSP server.
//!
//! The client connects to the server's `/register` endpoint, sends a greeting
//! containing its proxy id and desired pool size, then serves proxied HTTP
//! requests: it reads a serialized `HttpRequest`, executes it locally, and
//! streams the `HttpResponse` and body back.

use crate::client::pool::Pool;
use crate::common::{client_error_status, HttpRequest, HttpResponse, TUNNEL_ID_HEADER};
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

/// Keepalive cadence: how often the keepalive task wakes to run the liveness
/// check (and maybe send a ping).
pub(crate) const CHECK_INTERVAL_MS: i64 = 10_000;

/// Keepalive ping interval: periodic outbound pings keep NAT/firewall idle
/// timers from dropping a quiet link, and the pongs they elicit are what the
/// liveness check measures. The config-side floor
/// (`MIN_LIVENESS_TIMEOUT_MS` in `config.rs`) is derived from this, so the
/// two must change together.
pub(crate) const PING_INTERVAL_MS: i64 = 30_000;

/// How long an active liveness probe (`Connection::probe`) waits for a pong
/// before declaring the tunnel dead. Pongs are emitted by the server's
/// driver directly (not by the request path), so a healthy link answers in
/// ~RTT even under load; 10s is deliberately generous. This is also the
/// floor for `healthcheckinterval` (a pool health round cannot meaningfully
/// run faster than its own probe).
pub(crate) const PROBE_DEADLINE: Duration = Duration::from_secs(10);

/// A connection that lived past this long since a successful handshake counts
/// as "stable" for the connector's failure backoff: its end resets the backoff.
/// A connection that dies sooner (or never handshaked) counts as a failure —
/// covers both "server unreachable" (connect error) and "server accepts then
/// immediately closes" (handshake ok, dies in ~1s). 10s comfortably exceeds
/// the sub-second "accept-then-close" churn while staying well under the
/// keepalive liveness window, so a genuinely usable tunnel is never miscounted
/// as a failure.
const STABLE_LIFETIME: Duration = Duration::from_secs(10);

/// Run `f` under `deadline` — `None` means "no limit" (the timeout keys are
/// explicit-only: unconfigured applies nothing). On elapse the error names
/// the phase (`what`) and the configured budget; the inner error's `Display`
/// passes through unchanged.
async fn with_deadline<F, T, E>(deadline: Option<Duration>, what: &str, f: F) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    match deadline {
        Some(d) => match tokio::time::timeout(d, f).await {
            Ok(r) => r.map_err(|e| format!("{e}")),
            Err(_) => Err(format!("{what} timeout after {}ms", d.as_millis())),
        },
        None => f.await.map_err(|e| format!("{e}")),
    }
}

/// Status of a client connection. Mirrors Go's `CONNECTING/IDLE/RUNNING` iota,
/// with an extra `Closed` for idempotent shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Connecting = 0,
    Idle = 1,
    Running = 2,
    Closed = 3,
}

/// Outcome of an active liveness probe (`Connection::probe`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// A pong arrived after the probe ping was sent — the tunnel is verified
    /// usable right now (frame reached the server, reply came back).
    Ok,
    /// An idle tunnel that stayed silent for the whole probe deadline (or
    /// that cannot even send a ping) — dead or unanswerable, close it so the
    /// connector dials a replacement.
    Dead,
    /// Cannot conclude — no action, and the passive keepalive reaper still
    /// covers the tunnel: not Idle (Running is demonstrably exchanging data,
    /// Connecting is not up yet, Closed is already gone) or the write queue
    /// is momentarily full (mid-stream backpressure).
    Skipped,
}

/// Monotonic tunnel IDs: every `Connection` gets a process-unique number so
/// logs can be correlated — which tunnel served which request, which tunnel
/// was reaped, which one died young. 1-based (0 would read as "unset").
static NEXT_TUNNEL_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A single websocket connection to a WSP server.
pub struct Connection {
    pub pool: Weak<Pool>,
    /// Tunnel number. The SERVER is the single numbering authority (its
    /// counter is global, so numbers never duplicate across clients); it
    /// assigns the number in the handshake response header `X-TUNNEL-ID`,
    /// and from then on BOTH ends log the same `tunnel#N` for this
    /// connection. Before the handshake completes (and for dials that never
    /// get there) a provisional client-local number is used — such tunnels
    /// never existed server-side, so no cross-end correlation is needed.
    id: Mutex<u64>,
    status: Mutex<Status>,
    /// Requests served over this tunnel (logged on close).
    served: std::sync::atomic::AtomicU64,
    /// Last time any frame was received from the server (pong/data/ping).
    /// Refreshed by the driver on every read; checked by the keepalive task
    /// to detect half-open links (no traffic => the peer or the path is
    /// gone). The client sends a ping every 30s, so on a live link a pong
    /// arrives within ~RTT of each ping. If nothing arrives for
    /// `liveness_timeout`, the tunnel is dead and must be re-established.
    last_activity: Mutex<Instant>,
    /// Last time a PONG was received from the server (`None` until the first
    /// one). Refreshed by the driver on every `Message::Pong`; read by the
    /// active liveness probe, which must see a pong arrive AFTER its own
    /// ping was sent — any pong proves the round trip, including one
    /// elicited by the regular keepalive ping.
    last_pong: Mutex<Option<Instant>>,
    /// Handle to the tunnel's write channel (set once the driver is up,
    /// cleared on shutdown) so the pool's health round can inject probe
    /// pings without owning the channel itself.
    pub(crate) probe_tx: Mutex<Option<mpsc::Sender<Message>>>,
    /// When the tunnel became usable (handshake + greeting succeeded). `None`
    /// until then (or if connect failed before then). Used by `shutdown()` to
    /// tell a stable connection from one the server accepted then immediately
    /// closed, which drives the pool connector's failure backoff.
    connected_at: Mutex<Option<Instant>>,
    cancel: CancellationToken,
    self_weak: Weak<Connection>,
}

impl Connection {
    /// Create a new connection object (status Connecting).
    pub fn new(pool: Weak<Pool>) -> Arc<Self> {
        Arc::new_cyclic(|weak: &Weak<Connection>| Connection {
            pool,
            id: Mutex::new(NEXT_TUNNEL_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)),
            served: std::sync::atomic::AtomicU64::new(0),
            status: Mutex::new(Status::Connecting),
            last_activity: Mutex::new(Instant::now()),
            last_pong: Mutex::new(None),
            probe_tx: Mutex::new(None),
            connected_at: Mutex::new(None),
            cancel: CancellationToken::new(),
            self_weak: weak.clone(),
        })
    }

    /// The tunnel number (server-assigned once the handshake completed;
    /// provisional client-local before that).
    pub fn id(&self) -> u64 {
        *self.id.lock().unwrap()
    }

    /// Last time any frame was received from the peer (used by the keepalive
    /// task to detect half-open links).
    fn last_activity(&self) -> Instant {
        *self.last_activity.lock().unwrap()
    }

    /// When the tunnel became usable, if ever (used by `shutdown` to score the
    /// connection's lifecycle for the connector backoff).
    fn connected_at(&self) -> Option<Instant> {
        *self.connected_at.lock().unwrap()
    }

    /// Test support: overwrite the usable-since timestamp (production sets it
    /// from `connect()`; tests backdate it to simulate an established tunnel
    /// whose death must score as "stable").
    #[cfg(test)]
    pub(crate) fn set_connected_at(&self, t: Option<Instant>) {
        *self.connected_at.lock().unwrap() = t;
    }

    /// Last time a pong was received from the server (`None` until the first
    /// one); used by the health round to report each tunnel's pong age.
    pub(crate) fn last_pong(&self) -> Option<Instant> {
        *self.last_pong.lock().unwrap()
    }

    /// Record that a pong just arrived (called by the driver on every
    /// `Message::Pong`; tests simulate one the same way).
    pub(crate) fn record_pong(&self) {
        *self.last_pong.lock().unwrap() = Some(Instant::now());
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
        // The number shown here is the provisional client-local one; the
        // authoritative server-assigned number appears in the "Connected
        // tunnel#N" line right after the handshake.
        log::log(format!("Connecting tunnel#{} to {}", self.id(), target));

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
        // 隧道建立各阶段限时（`tunneltimeout` 配置；0/未配置 = 不限时）：
        // SYN 被丢弃（防火墙静默丢包）或对端接受 TCP 却不回 101 时，
        // `Connecting` 状态的连接不会永久占用池容量。
        let tunnel_deadline = (config.tunnel_timeout > 0)
            .then(|| Duration::from_millis(config.tunnel_timeout as u64));
        let stream = with_deadline(
            tunnel_deadline,
            "dial",
            tokio::net::TcpStream::connect((host.as_str(), port)),
        )
        .await?;
        let _ = stream.set_nodelay(true);
        let (ws, resp) = with_deadline(
            tunnel_deadline,
            "websocket handshake",
            tokio_tungstenite::client_async(req, stream),
        )
        .await?;

        // Adopt the SERVER-assigned tunnel number from the handshake
        // response — the server is the single numbering authority, so from
        // here on both ends log the same `tunnel#N` for this connection.
        if let Some(id) = resp
            .headers()
            .get(TUNNEL_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
        {
            *self.id.lock().unwrap() = id;
        }
        log::log(format!("Connected tunnel#{} to {}", self.id(), target));

        // Greeting: `<id>_<pool_idle_size>`.
        let greeting = format!("{}_{}", proxy_id, pool_idle_size);

        let (read_tx, read_rx) = mpsc::channel::<Message>(8);
        let (write_tx, write_rx) = mpsc::channel::<Message>(8);
        // Hand the pool's health round a way to inject probe pings. Set right
        // here (before the greeting) so an Idle tunnel always carries a
        // writer; cleared again by shutdown().
        *self.probe_tx.lock().unwrap() = Some(write_tx.clone());

        let mut ws = ws;
        // greeting 发送也限时：握手后对端不消费数据时同样不能永久挂起。
        let greeted = with_deadline(
            tunnel_deadline,
            "greeting",
            ws.send(Message::text(greeting)),
        )
        .await;
        if let Err(e) = greeted {
            self.shutdown();
            return Err(e);
        }
        // Handshake + greeting succeeded: mark when the tunnel became usable.
        // `shutdown()` uses the elapsed-since to tell a stable connection from
        // one the server accepted then immediately closed, which drives the
        // pool connector's failure backoff.
        *self.connected_at.lock().unwrap() = Some(Instant::now());

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
        tokio::spawn(serve(
            serve_conn,
            read_rx,
            write_tx.clone(),
            http_client,
            config,
        ));

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
            Duration::from_millis(CHECK_INTERVAL_MS as u64),
            Duration::from_millis(PING_INTERVAL_MS as u64),
        ));

        Ok(())
    }

    /// Close the connection: cancel the driver/serve/ping and remove it from
    /// the pool. Idempotent.
    ///
    /// Also scores the connection's lifecycle into the pool connector's failure
    /// backoff: a connection that never handshaked (connect error) or that
    /// died within `STABLE_LIFETIME` of becoming usable (server accepts then
    /// immediately closes) counts as a failure and grows the backoff; one that
    /// lived past `STABLE_LIFETIME` counts as a success and resets it.
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
        // Drop the probe writer first so a health round racing this shutdown
        // cannot queue pings into a dead channel.
        *self.probe_tx.lock().unwrap() = None;
        // Lifecycle summary: one line per tunnel death, with the id and what
        // it accomplished — pairs with the per-request "[tunnel#N …]" lines.
        let served = self.served.load(std::sync::atomic::Ordering::Relaxed);
        match self.connected_at() {
            Some(t) => log::log(format!(
                "tunnel#{} closed (lived {}s, served {} request{})",
                self.id(),
                t.elapsed().as_secs(),
                served,
                if served == 1 { "" } else { "s" }
            )),
            None => log::log(format!(
                "tunnel#{} closed (never usable — dial, handshake or greeting failed)",
                self.id()
            )),
        }
        if let Some(pool) = self.pool.upgrade() {
            let stable = is_stable_lifetime(self.connected_at().map(|t| t.elapsed()));
            pool.note_outcome(stable);
            pool.remove(self);
        }
        self.cancel.cancel();
    }

    pub fn status(&self) -> Status {
        *self.status.lock().unwrap()
    }
    pub fn set_status(&self, s: Status) {
        *self.status.lock().unwrap() = s;
    }

    /// Actively verify this tunnel is usable RIGHT NOW: send a ping and wait
    /// for a pong within `deadline`. Unlike the passive keepalive reaper
    /// (which waits `liveness_timeout` for ANY frame and so needs minutes to
    /// notice a dead link), a probe settles in ~RTT and turns "Idle by
    /// status" into "verified available" — the basis of the pool's periodic
    /// health round, which guarantees the server always has usable tunnels
    /// instead of a pool of half-open ones the client still believes in.
    ///
    /// Only Idle tunnels are probed. A Running tunnel is demonstrably
    /// exchanging data; a Connecting one is not up yet; a Closed one is
    /// already gone — all Skipped. The verdict at the deadline re-checks the
    /// status: if a request started during the probe (Idle -> Running) the
    /// tunnel is alive and must not be killed, mirroring the TOCTOU re-check
    /// in the keepalive reaper.
    pub(crate) async fn probe(&self, deadline: Duration) -> ProbeOutcome {
        if self.status() != Status::Idle {
            return ProbeOutcome::Skipped;
        }
        // Any pong arriving at or after this instant proves the round trip —
        // including one elicited by the regular keepalive ping, which proves
        // exactly the same thing.
        let started = Instant::now();
        let sender = self.probe_tx.lock().unwrap().clone();
        let Some(sender) = sender else {
            // Idle tunnel with no writer: it can neither be probed nor serve
            // a request (production always sets the writer in connect();
            // only a broken/anomalous tunnel ends up here) — treat as dead.
            return ProbeOutcome::Dead;
        };
        match sender.try_send(Message::Ping(bytes::Bytes::from_static(b"how-probe"))) {
            Ok(()) => {}
            // Queue full (a streamed response just finished filling it) or
            // the driver side already dropped: inconclusive, never kill on a
            // maybe — the passive keepalive reaper still covers this tunnel.
            Err(mpsc::error::TrySendError::Full(_)) => return ProbeOutcome::Skipped,
            Err(mpsc::error::TrySendError::Closed(_)) => return ProbeOutcome::Skipped,
        }
        // Poll for the pong: a healthy link answers in ~RTT, so poll finely
        // (but never coarser than a quarter of the deadline).
        let poll = (deadline / 4).min(Duration::from_millis(250));
        let deadline_at = started + deadline;
        loop {
            if self.last_pong().is_some_and(|t| t >= started) {
                return ProbeOutcome::Ok;
            }
            if Instant::now() >= deadline_at {
                break;
            }
            tokio::time::sleep(poll).await;
        }
        // No pong in time. If a request started meanwhile the tunnel is
        // exchanging data right now — do not kill it.
        if self.status() != Status::Idle {
            return ProbeOutcome::Skipped;
        }
        ProbeOutcome::Dead
    }
}

/// Decide whether a connection's usable lifetime counts as "stable" for the
/// connector's failure backoff: `None` (never became usable — connect failed
/// before the greeting) or a lifetime below `STABLE_LIFETIME` (server accepts
/// then immediately closes) is a failure; at or past the threshold the server
/// was genuinely serving, which resets the backoff. Pure in the elapsed
/// lifetime (not the clock) so tests can cover every branch without
/// constructing past `Instant`s.
fn is_stable_lifetime(elapsed_since_connected: Option<Duration>) -> bool {
    match elapsed_since_connected {
        Some(e) => e >= STABLE_LIFETIME,
        None => false,
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
                            Message::Pong(_) => conn.record_pong(),
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
                    // 重检 status：读取 status/last_activity 与 shutdown 之间，
                    // serve 循环可能刚好从 read_rx 取到一个请求并把 status
                    // 切到 Running（driver 收到该帧时已刷新了 last_activity，
                    // 但 keepalive 用的可能是刷新前的旧值）。重检可显著缩小
                    // 这个 TOCTOU 窗口，避免误杀正在处理的请求。窗口无法
                    // 完全消除（除非在持锁状态下决策），但此重检已足够。
                    if conn.status() == Status::Idle {
                        log::log(format!(
                            "Reaping half-open tunnel#{}: no frame from server for {}ms",
                            conn.id(),
                            idle.as_millis()
                        ));
                        conn.shutdown();
                        break;
                    }
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

        // Log the request WITH the tunnel id: "which request went through
        // which websocket" is then a single `grep tunnel#N`.
        log::log(format!(
            "[tunnel#{} {}] {}",
            conn.id(),
            http_req.method,
            http_req.url
        ));

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
                let _ =
                    send_error(&write_tx, &format!("Unable to parse request url : {}\n", e)).await;
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
        // Past the last rejection path — this request is actually being
        // forwarded. Count it as served (denied/unroutable requests are
        // logged above but do not count).
        conn.served
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

        // 将请求体流式写入 reqwest：从 WebSocket 上逐个读取二进制帧送入
        // body channel，reqwest 通过 stream 消费它。body_tx 由 producer 拥有，
        // producer 结束（读到空 end-marker）时 body_tx 被 drop，reqwest 就知
        // 道请求体结束了；read_rx 是借用所以 serve 循环继续持有它。
        //
        // 额外加一层上游调用超时（`upstreamtimeout` 配置；0/未配置 = 不限时）：
        // 只覆盖"发出请求 → 收到响应头"这一段，响应 body 在其之后流式回传、
        // 不受它限制。包这一层并打印耗时日志的目的是：
        //   1) 下次再遇到慢请求时，日志里能立刻看到是"上游建立连接慢"
        //      还是"上游返回数据慢"；
        //   2) 遇到配置错误等特殊情况导致 reqwest 内部 timeout 失效时，
        //      仍能保证不会无限挂住一条 Running 连接（Running 连接会阻止
        //      keepalive 的半开回收逻辑，长时间挂死会把池子拖没）。
        let upstream_call_deadline = (config.upstream_timeout > 0)
            .then(|| Duration::from_millis(config.upstream_timeout as u64));
        let (body_tx, body_rx) = mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(16);
        let req_body =
            reqwest::Body::wrap_stream(tokio_stream::wrappers::ReceiverStream::new(body_rx));
        let reqwest_req = http_client
            .request(method, url.clone())
            .headers(headers)
            .body(req_body);
        let read_rx_ref = &mut read_rx;
        let producer = async move {
            loop {
                match read_rx_ref.recv().await {
                    Some(Message::Binary(b)) if b.is_empty() => break, // 结束标记
                    Some(Message::Binary(b)) => {
                        if body_tx.send(Ok(b)).await.is_err() {
                            break;
                        }
                    }
                    Some(_) | None => break,
                }
            }
            // body_tx 在这里被 drop -> reqwest 感知到请求体结束。
        };
        let call_start = Instant::now();
        let send_future = async { tokio::join!(producer, reqwest_req.send()).1 };
        // (message, is_timeout)：未配置 upstreamtimeout 时没有超时分支。
        let outcome = match upstream_call_deadline {
            Some(d) => match tokio::time::timeout(d, send_future).await {
                Ok(Ok(r)) => Ok(r),
                Ok(Err(e)) => Err((e.to_string(), false)),
                Err(_) => Err((
                    format!("upstream timeout after {}ms", config.upstream_timeout),
                    true,
                )),
            },
            None => match send_future.await {
                Ok(r) => Ok(r),
                Err(e) => Err((e.to_string(), false)),
            },
        };
        let resp = match outcome {
            Ok(r) => {
                // 上游响应头已收到，这里只记录"到拿到响应头"的耗时；
                // body 是流式的，后续再慢慢写回，不计入这段日志。
                log::log(format!(
                    "上游调用成功：{} 状态码={} 已耗时={}ms",
                    url,
                    r.status(),
                    call_start.elapsed().as_millis()
                ));
                r
            }
            Err((msg, is_timeout)) => {
                if is_timeout {
                    log::log(format!(
                        "上游调用超时（upstreamtimeout={}ms）：{} 已等待={}ms",
                        config.upstream_timeout,
                        url,
                        call_start.elapsed().as_millis()
                    ));
                } else {
                    log::log(format!(
                        "上游调用失败：{} 错误={} 已耗时={}ms",
                        url,
                        msg,
                        call_start.elapsed().as_millis()
                    ));
                }
                let _ =
                    send_error(&write_tx, &format!("Unable to execute request : {msg}\n")).await;
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
fn check_rules(config: &crate::client::ClientConfig, req: &HttpRequest) -> Option<String> {
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
    write_tx
        .send(Message::binary(Vec::new()))
        .await
        .map_err(|_| ())?;
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

    /// A connection that never became usable (connect failed before the
    /// greeting) must be scored as a failure: `is_stable_lifetime(None)` is
    /// false, so the pool connector backs off instead of hammering an
    /// unreachable server.
    /// Pre-connect ids are process-unique and monotonically increasing; the
    /// authoritative server-assigned number arrives with the handshake
    /// response header and replaces it (see connect()).
    #[test]
    fn provisional_tunnel_ids_are_unique_and_monotonic() {
        let pool = dummy_pool();
        let a = Connection::new(Arc::downgrade(&pool));
        let b = Connection::new(Arc::downgrade(&pool));
        let c = Connection::new(Arc::downgrade(&pool));
        assert!(
            a.id() < b.id() && b.id() < c.id(),
            "provisional ids must be unique monotonic"
        );
        assert!(a.id() >= 1, "ids are 1-based");
    }

    #[test]
    fn outcome_never_connected_is_a_failure() {
        assert!(!is_stable_lifetime(None));
    }

    /// The stability threshold: a usable lifetime below `STABLE_LIFETIME`
    /// (server accepts then immediately closes) is a failure; at or past it
    /// the connection counts as stable and resets the connector backoff.
    #[test]
    fn outcome_threshold_decides_stability() {
        assert!(!is_stable_lifetime(Some(
            STABLE_LIFETIME - Duration::from_millis(1)
        )));
        assert!(is_stable_lifetime(Some(STABLE_LIFETIME)));
        assert!(is_stable_lifetime(Some(STABLE_LIFETIME * 10)));
    }

    /// A just-connected tunnel (elapsed ~0, well below `STABLE_LIFETIME`)
    /// that shuts down right away must also score as a failure — the
    /// accept-then-close churn mode. Exercises the real `shutdown()` path
    /// (this module can write `connected_at` but not the pool's private
    /// backoff state; the pool-side scoring is covered in `pool.rs`).
    #[test]
    fn outcome_fresh_connection_is_a_failure() {
        assert!(!is_stable_lifetime(Some(Instant::now().elapsed())));
    }

    /// Active liveness probes (`Connection::probe`) — the primitive behind
    /// the pool's periodic health round. A probe sends a ping and verifies a
    /// pong arrives afterwards; unlike the passive keepalive reaper (which
    /// waits `livenesstimeout` for ANY frame), a probe settles in ~RTT and
    /// turns "Idle by status" into "verified usable right now". These tests
    /// wire a fake write channel (no real websocket) and simulate the
    /// server's pong the way the driver records it.
    mod probe {
        use super::*;
        use tokio::sync::mpsc;

        /// Attach a write channel to a connection the way `connect()` does,
        /// returning the receiving end (the fake "driver"/server side).
        fn wire_probe_tx(conn: &Arc<Connection>) -> mpsc::Receiver<Message> {
            let (tx, rx) = mpsc::channel::<Message>(8);
            *conn.probe_tx.lock().unwrap() = Some(tx);
            rx
        }

        /// A pong arriving after the probe ping was sent => Ok.
        #[tokio::test(flavor = "current_thread")]
        async fn probe_reports_ok_when_pong_arrives() {
            let pool = dummy_pool();
            let conn = Connection::new(Arc::downgrade(&pool));
            conn.set_status(Status::Idle);
            let mut rx = wire_probe_tx(&conn);

            let probing = conn.clone();
            // 5s deadline: the happy path still settles in ~ms (the responder
            // task runs at the first await); the margin only absorbs a
            // heavily loaded test runner so the case never flakes.
            let probe = tokio::spawn(async move { probing.probe(Duration::from_secs(5)).await });
            // Fake server: the probe's ping arrives, then the pong comes back
            // (the driver records it via record_pong).
            let ping = rx.recv().await.expect("probe must send a ping");
            assert!(
                matches!(ping, Message::Ping(_)),
                "probe must send a ping frame, got {ping:?}"
            );
            conn.record_pong();
            assert_eq!(probe.await.unwrap(), ProbeOutcome::Ok);
        }

        /// No pong within the deadline => Dead.
        #[tokio::test(flavor = "current_thread")]
        async fn probe_reports_dead_when_no_pong_arrives() {
            let pool = dummy_pool();
            let conn = Connection::new(Arc::downgrade(&pool));
            conn.set_status(Status::Idle);
            let _rx = wire_probe_tx(&conn); // never answers
            assert_eq!(
                conn.probe(Duration::from_millis(60)).await,
                ProbeOutcome::Dead
            );
        }

        /// Not Idle => Skipped: a Running tunnel is demonstrably exchanging
        /// data, a Connecting one is not up yet, a Closed one is already gone.
        #[tokio::test(flavor = "current_thread")]
        async fn probe_skips_non_idle_tunnels() {
            let pool = dummy_pool();
            for status in [Status::Running, Status::Connecting, Status::Closed] {
                let conn = Connection::new(Arc::downgrade(&pool));
                conn.set_status(status);
                assert_eq!(
                    conn.probe(Duration::from_millis(20)).await,
                    ProbeOutcome::Skipped,
                    "{status:?} must not be probed"
                );
            }
        }

        /// Idle tunnel with no writer (never went through `connect()`, or the
        /// driver is gone) cannot be verified nor serve a request => Dead.
        #[tokio::test(flavor = "current_thread")]
        async fn probe_reports_dead_without_writer() {
            let pool = dummy_pool();
            let conn = Connection::new(Arc::downgrade(&pool));
            conn.set_status(Status::Idle);
            assert_eq!(
                conn.probe(Duration::from_millis(20)).await,
                ProbeOutcome::Dead
            );
        }

        /// Write queue momentarily full (a streamed response just finished
        /// filling the shared buffer) => inconclusive: Skipped, never killed
        /// on a maybe. The passive keepalive reaper still covers this tunnel.
        #[tokio::test(flavor = "current_thread")]
        async fn probe_skips_when_write_queue_is_full() {
            let pool = dummy_pool();
            let conn = Connection::new(Arc::downgrade(&pool));
            conn.set_status(Status::Idle);
            let (tx, _rx) = mpsc::channel::<Message>(1); // never drained
            *conn.probe_tx.lock().unwrap() = Some(tx.clone());
            tx.try_send(Message::text("fill")).unwrap();
            assert_eq!(
                conn.probe(Duration::from_millis(20)).await,
                ProbeOutcome::Skipped
            );
        }
    }
}
