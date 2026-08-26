//! A single websocket connection offered by a remote WSP client.
//!
//! The connection owns a background "driver" task that reads messages off the
//! websocket and multiplexes them with outgoing writes coming from an HTTP
//! request handler. The driver delivers incoming Text/Binary messages to the
//! request handler through an mpsc channel (`read_rx`) and accepts outgoing
//! messages through another (`write_tx`). This mirrors the original Go
//! `server.Connection` where a `read()` goroutine hands readers to
//! `proxyRequest` via a rendezvous channel.

use crate::common::{HttpRequest, HttpResponse};
use crate::log;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tokio_util::sync::CancellationToken;

/// Status of a connection. Mirrors Go's `IDLE/BUSY/CLOSED` iota.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Idle = 0,
    Busy = 1,
    Closed = 2,
}

/// Decode a websocket Message into a Text payload, if it is a text message.
pub fn msg_text(msg: &Message) -> Option<String> {
    match msg {
        Message::Text(t) => Some(t.as_str().to_string()),
        _ => None,
    }
}

/// Decode a websocket Message into a Binary payload, if it is a binary message.
pub fn msg_binary(msg: &Message) -> Option<Vec<u8>> {
    match msg {
        Message::Binary(b) => Some(b.to_vec()),
        _ => None,
    }
}

/// A single websocket connection offered by a remote WSP client.
pub struct Connection {
    pub pool_id: String,
    status: Mutex<Status>,
    idle_since: Mutex<Option<Instant>>,
    write_tx: mpsc::Sender<Message>,
    read_rx: tokio::sync::Mutex<mpsc::Receiver<Message>>,
    cancel: CancellationToken,
    idle_tx: Mutex<Option<mpsc::Sender<Arc<Connection>>>>,
    self_weak: Weak<Connection>,
}

impl Connection {
    /// Spawn a connection over an already-upgraded websocket stream. The
    /// `idle_tx` is the server-wide dispatcher queue the connection offers
    /// itself to whenever it becomes idle.
    pub fn new<S>(pool_id: String, stream: WebSocketStream<S>, idle_tx: mpsc::Sender<Arc<Connection>>) -> Arc<Self>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        let (read_tx, read_rx) = mpsc::channel::<Message>(8);
        let (write_tx, write_rx) = mpsc::channel::<Message>(8);
        // The connection owns the parent token; the driver gets a child so
        // that `close()` (cancelling the parent) wakes the driver.
        let cancel = CancellationToken::new();
        let driver_cancel = cancel.child_token();

        let conn: Arc<Connection> = Arc::new_cyclic(|weak: &Weak<Connection>| Connection {
            pool_id: pool_id.clone(),
            status: Mutex::new(Status::Idle),
            idle_since: Mutex::new(Some(Instant::now())),
            write_tx,
            read_rx: tokio::sync::Mutex::new(read_rx),
            cancel,
            idle_tx: Mutex::new(Some(idle_tx)),
            self_weak: weak.clone(),
        });

        let conn_for_driver = conn.clone();
        tokio::spawn(async move {
            driver(conn_for_driver, stream, write_rx, read_tx, driver_cancel).await;
        });

        log::log(format!("Registering new connection from {}", pool_id));

        // Offer the freshly idle connection to the dispatcher immediately,
        // mirroring Go's `connection.Release()` called from `NewConnection`.
        conn.release();
        conn
    }

    /// Whether the connection is currently busy.
    pub fn is_busy(&self) -> bool {
        *self.status.lock().unwrap() == Status::Busy
    }

    /// Whether the connection is closed.
    pub fn is_closed(&self) -> bool {
        *self.status.lock().unwrap() == Status::Closed
    }

    /// Current status (copied out from under the lock).
    pub fn status(&self) -> Status {
        *self.status.lock().unwrap()
    }

    /// Idle-since timestamp (used by the pool cleaner).
    pub fn idle_since(&self) -> Option<Instant> {
        *self.idle_since.lock().unwrap()
    }

    /// Notify that this connection is going to be used. Returns false if the
    /// connection is closed or already busy.
    pub fn take(&self) -> bool {
        let mut s = self.status.lock().unwrap();
        match *s {
            Status::Idle => {
                *s = Status::Busy;
                true
            }
            _ => false,
        }
    }

    /// Notify that this connection is ready to use again and re-offer it to
    /// the dispatcher.
    pub fn release(&self) {
        {
            let mut s = self.status.lock().unwrap();
            if *s == Status::Closed {
                return;
            }
            *s = Status::Idle;
            *self.idle_since.lock().unwrap() = Some(Instant::now());
        }
        self.offer();
    }

    /// Offer an idle connection to the server-wide dispatcher queue (fire and
    /// forget, matching Go's `go func() { pool.idle <- connection }()`).
    fn offer(&self) {
        let idle_tx = self.idle_tx.lock().unwrap().clone();
        if let (Some(idle_tx), Some(arc)) = (idle_tx, self.self_weak.upgrade()) {
            tokio::spawn(async move {
                let _ = idle_tx.send(arc).await;
            });
        }
    }

    /// Close the connection.
    pub fn close(&self) {
        {
            let mut s = self.status.lock().unwrap();
            if *s == Status::Closed {
                return;
            }
            *s = Status::Closed;
        }
        log::log(format!("Closing connection from {}", self.pool_id));
        self.cancel.cancel();
    }

    /// Serialize and send the HTTP request header (a single text message) over
    /// the websocket. The request body is sent separately as a stream of binary
    /// messages terminated by an empty binary end-marker.
    pub async fn send_request_header(&self, req: &HttpRequest) -> Result<(), String> {
        log::log(format!("proxy request to {}", self.pool_id));

        let json_req =
            serde_json::to_string(req).map_err(|e| format!("Unable to serialize request : {}", e))?;
        if self
            .write_tx
            .send(Message::text(json_req))
            .await
            .is_err()
        {
            return Err("Unable to write request".to_string());
        }
        Ok(())
    }

    /// Send one request-body chunk as a binary message.
    pub async fn send_body_chunk(&self, chunk: Bytes) -> Result<(), String> {
        if self.write_tx.send(Message::binary(chunk)).await.is_err() {
            return Err("Unable to pipe request body".to_string());
        }
        Ok(())
    }

    /// Send the empty binary end-marker that terminates the request body
    /// stream (so the remote client knows the body is complete).
    pub async fn send_body_end(&self) -> Result<(), String> {
        if self
            .write_tx
            .send(Message::binary(Bytes::new()))
            .await
            .is_err()
        {
            return Err("Unable to pipe request body (end)".to_string());
        }
        Ok(())
    }

    /// Read the serialized `HttpResponse` (header text message) and deserialize
    /// it. The response body is read separately as a stream of binary messages.
    pub async fn recv_response_header(&self) -> Result<HttpResponse, String> {
        let resp_msg = self
            .recv_msg()
            .await?
            .ok_or_else(|| "Unable to get http response reader".to_string())?;
        let resp_json =
            msg_text(&resp_msg).ok_or_else(|| "Unable to read http response".to_string())?;
        let http_response: HttpResponse = serde_json::from_str(&resp_json)
            .map_err(|e| format!("Unable to unserialize http response : {}", e))?;
        Ok(http_response)
    }

    /// Stream the response body to `body_tx`: each non-empty binary message is
    /// a body chunk, and an empty binary message marks end-of-body (the body
    /// stream is closed by dropping the sender). Returns `Ok(())` on a clean
    /// end marker, or `Err` if the connection broke mid-stream.
    pub async fn drain_response_body(
        &self,
        mut body_tx: http_body_util::channel::Sender<Bytes>,
    ) -> Result<(), String> {
        loop {
            let msg = self
                .recv_msg()
                .await?
                .ok_or_else(|| "Unable to get http response body reader".to_string())?;
            match msg_binary(&msg) {
                Some(chunk) if chunk.is_empty() => {
                    // End-of-body marker.
                    return Ok(());
                }
                Some(chunk) => {
                    if body_tx.send_data(Bytes::from(chunk)).await.is_err() {
                        // The HTTP client went away; stop draining.
                        return Err("client gone".to_string());
                    }
                }
                None => {
                    // Unexpected message type; stop.
                    return Err("unexpected response body message".to_string());
                }
            }
        }
    }

    /// Receive the next message from the driver (the next message read off the
    /// websocket). Returns `None` when the connection has been closed.
    async fn recv_msg(&self) -> Result<Option<Message>, String> {
        let mut rx = self.read_rx.lock().await;
        match rx.recv().await {
            Some(m) => Ok(Some(m)),
            None => Ok(None),
        }
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
        // Flush any pong queued from a received ping before polling again so
        // we never call stream.send() while the stream.next() future is alive
        // inside select!.
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
                    Some(Ok(m @ Message::Text(_))) | Some(Ok(m @ Message::Binary(_))) => {
                        // Wild unexpected message if not currently busy.
                        if !conn.is_busy() {
                            break;
                        }
                        if read_tx.send(m).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(_)) => {}
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
    // Mark the connection closed and drop the read channel so any pending
    // proxyRequest is unblocked.
    {
        let mut s = conn.status.lock().unwrap();
        *s = Status::Closed;
    }
    drop(read_tx);
    drop(write_rx);
}
