//! Server-side connection pool.
//!
//! A `Pool` holds every websocket connection offered by a single remote WSP
//! client (identified by its greeting `id`). Pools are created lazily when a
//! new client id registers and are garbage-collected by the server cleaner
//! once empty.

use crate::log;
use crate::server::connection::{Connection, Status};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;

/// Pool handle: every connection from the same remote client id.
pub struct Pool {
    pub id: String,
    pub size: Mutex<usize>,
    connections: Mutex<Vec<Arc<Connection>>>,
    idle_tx: mpsc::Sender<Arc<Connection>>,
    idle_timeout_ms: i64,
    liveness_timeout_ms: i64,
    done: Mutex<bool>,
}

impl Pool {
    pub fn new(
        id: String,
        idle_tx: mpsc::Sender<Arc<Connection>>,
        idle_timeout_ms: i64,
        liveness_timeout_ms: i64,
    ) -> Arc<Self> {
        Arc::new(Pool {
            id,
            size: Mutex::new(0),
            connections: Mutex::new(Vec::new()),
            idle_tx,
            idle_timeout_ms,
            liveness_timeout_ms,
            done: Mutex::new(false),
        })
    }

    /// Register a new websocket connection into the pool.
    pub fn register<S>(&self, ws: WebSocketStream<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        {
            let done = *self.done.lock().unwrap();
            if done {
                return;
            }
        }
        let conn = Connection::new(self.id.clone(), ws, self.idle_tx.clone());
        let mut conns = self.connections.lock().unwrap();
        conns.push(conn);
    }

    /// Update the pool size advertised by the remote client.
    pub fn set_size(&self, size: usize) {
        *self.size.lock().unwrap() = size;
    }

    /// Remove dead connections from the pool and close idle ones that exceed
    /// the client-advertised pool size once they have been idle for longer than
    /// `IdleTimeout`.
    ///
    /// MUST be called with the connections lock held.
    fn clean_locked(&self, conns: &mut Vec<Arc<Connection>>) {
        let mut idle = 0usize;
        let kept: Vec<Arc<Connection>> = conns
            .drain(..)
            .filter_map(|connection| {
                let st = connection.status();
                if st == Status::Idle {
                    // Liveness: a connection that has received no frame at all
                    // for longer than the liveness timeout is half-open (the
                    // peer or the path is gone). Close it regardless of pool
                    // size so the dispatcher never hands a dead tunnel to a
                    // request. This runs BEFORE the `idle` counter so a dead
                    // link is reaped without inflating the count — otherwise a
                    // dead link could push a healthy one past `size` and get it
                    // wrongly closed as excess idle. The 30-second pings +
                    // this check detect dead links in ~2 minutes instead of
                    // waiting for the OS TCP keepalive (~2 hours).
                    let last_activity_ms =
                        connection.last_activity().elapsed().as_millis() as i64;
                    if last_activity_ms > self.liveness_timeout_ms {
                        log::log(format!(
                            "Reaping half-open connection from {} (no frame for {}ms)",
                            connection.pool_id, last_activity_ms
                        ));
                        connection.close();
                    } else {
                        // Still alive and idle: count it, then close it if it
                        // is an excess connection that has been idle for
                        // longer than IdleTimeout.
                        idle += 1;
                        if idle > *self.size.lock().unwrap() {
                            if let Some(since) = connection.idle_since() {
                                let elapsed_ms = since.elapsed().as_millis() as i64;
                                if elapsed_ms > self.idle_timeout_ms {
                                    log::log(format!(
                                        "Closing excess idle connection from {} (idle for {}ms)",
                                        connection.pool_id, elapsed_ms
                                    ));
                                    connection.close();
                                }
                            }
                        }
                    }
                }
                if connection.is_closed() {
                    None
                } else {
                    Some(connection)
                }
            })
            .collect();
        *conns = kept;
    }

    /// Clean the pool and return true if it is empty.
    pub fn is_empty(&self) -> bool {
        let mut conns = self.connections.lock().unwrap();
        self.clean_locked(&mut conns);
        conns.is_empty()
    }

    /// Shutdown closes every connection in the pool and cleans it.
    pub fn shutdown(&self) {
        {
            let mut done = self.done.lock().unwrap();
            *done = true;
        }
        let mut conns = self.connections.lock().unwrap();
        for conn in conns.iter() {
            conn.close();
        }
        self.clean_locked(&mut conns);
    }

    /// Number of connection in each state in the pool.
    pub fn size_count(&self) -> (usize, usize, usize) {
        let conns = self.connections.lock().unwrap();
        let mut idle = 0;
        let mut busy = 0;
        let mut closed = 0;
        for connection in conns.iter() {
            match connection.status() {
                Status::Idle => idle += 1,
                Status::Busy => busy += 1,
                Status::Closed => closed += 1,
            }
        }
        (idle, busy, closed)
    }
}
