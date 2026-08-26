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
    cancel: CancellationToken,
    self_weak: Weak<Connection>,
}

impl Connection {
    /// Create a new connection object (status Connecting).
    pub fn new(pool: Weak<Pool>) -> Arc<Self> {
        Arc::new_cyclic(|weak: &Weak<Connection>| Connection {
            pool,
            status: Mutex::new(Status::Connecting),
            cancel: CancellationToken::new(),
            self_weak: weak.clone(),
        })
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

        // Driver: owns the stream, multiplexes reads/writes.
        let driver_conn = this.clone();
        tokio::spawn(async move {
            driver(driver_conn, ws, write_rx, read_tx, driver_cancel).await;
        });

        // Serve: reads requests, executes them, writes responses.
        let serve_conn = this.clone();
        tokio::spawn(serve(serve_conn, read_rx, write_tx.clone(), http_client, config));

        // Keepalive: ping every 30 seconds.
        tokio::spawn(ping_loop(write_tx, cancel));

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
    _conn: Arc<Connection>,
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
                    Some(Ok(Message::Ping(p))) => {
                        pending_pong = Some(p);
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(m)) => {
                        if read_tx.send(m).await.is_err() {
                            break;
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

/// Keepalive: send a ping every 30 seconds (matches Go's keepalive goroutine).
async fn ping_loop(write_tx: mpsc::Sender<Message>, cancel: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                if write_tx.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break;
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
                        if write_tx.send(Message::binary(b.to_vec())).await.is_err() {
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
