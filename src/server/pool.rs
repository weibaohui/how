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
    busy_liveness_timeout_ms: i64,
    done: Mutex<bool>,
}

impl Pool {
    pub fn new(
        id: String,
        idle_tx: mpsc::Sender<Arc<Connection>>,
        idle_timeout_ms: i64,
        liveness_timeout_ms: i64,
        busy_liveness_timeout_ms: i64,
    ) -> Arc<Self> {
        Arc::new(Pool {
            id,
            size: Mutex::new(0),
            connections: Mutex::new(Vec::new()),
            idle_tx,
            idle_timeout_ms,
            liveness_timeout_ms,
            busy_liveness_timeout_ms,
            done: Mutex::new(false),
        })
    }

    /// Register a new websocket connection into the pool under its
    /// server-assigned `conn_id` (announced to the client at handshake).
    pub fn register<S>(&self, ws: WebSocketStream<S>, conn_id: u64)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
    {
        {
            let done = *self.done.lock().unwrap();
            if done {
                return;
            }
        }
        let conn = Connection::new(self.id.clone(), conn_id, ws, self.idle_tx.clone());
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
                if st == Status::Busy {
                    // Busy fuse: on a live link the client's 30s keepalive
                    // pings keep this watermark fresh in EVERY request phase
                    // (uploads, waiting on upstream headers, SSE gaps). One
                    // that has received nothing for longer than the busy
                    // liveness timeout is therefore not in a "silent phase" —
                    // the link is dead (bidirectional partition, where the
                    // client's close can never arrive), the client is wedged,
                    // or the path is fully backlogged (a caller that stopped
                    // reading). Past the operator-chosen budget, close it:
                    // the hanging request fails fast instead of forever, and
                    // the slot frees up.
                    let silent_ms = connection.last_activity().elapsed().as_millis() as i64;
                    if silent_ms > self.busy_liveness_timeout_ms {
                        log::log(format!(
                            "Reaping stuck busy tunnel#{} from {} (no frame for {}ms)",
                            connection.id, connection.pool_id, silent_ms
                        ));
                        connection.close();
                    }
                }
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
                    let last_activity_ms = connection.last_activity().elapsed().as_millis() as i64;
                    if last_activity_ms > self.liveness_timeout_ms {
                        log::log(format!(
                            "Reaping half-open tunnel#{} from {} (no frame for {}ms)",
                            connection.id, connection.pool_id, last_activity_ms
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

#[cfg(test)]
mod tests {
    //! The cleaner's Busy-connection fuse: on a live link the client's 30s
    //! keepalive pings keep a busy connection's watermark fresh in every
    //! request phase, so one that has received NOTHING for longer than
    //! `busylivenesstimeout` means the link or the client is gone (at worst
    //! a bidirectional partition where the client's close can never arrive)
    //! and must be closed — otherwise a proxied request with no
    //! `upstreamtimeout` hangs forever. Idle-connection behavior must stay
    //! untouched by the fuse.

    use super::*;
    use crate::server::connection::dummy_connection;
    use std::time::{Duration, Instant};

    const IDLE_TIMEOUT_MS: i64 = 60_000;
    const LIVENESS_TIMEOUT_MS: i64 = 120_000;
    const BUSY_LIVENESS_TIMEOUT_MS: i64 = 600_000;

    fn test_pool(idle_tx: mpsc::Sender<Arc<Connection>>) -> Arc<Pool> {
        Pool::new(
            "test-pool".to_string(),
            idle_tx,
            IDLE_TIMEOUT_MS,
            LIVENESS_TIMEOUT_MS,
            BUSY_LIVENESS_TIMEOUT_MS,
        )
    }

    /// A BUSY connection silent past the busy fuse is closed and dropped
    /// from the pool (the request hanging on it fails instead of hanging
    /// forever).
    #[tokio::test(flavor = "current_thread")]
    async fn cleaner_reaps_stuck_busy_connection() {
        let (idle_tx, _idle_rx) = mpsc::channel(8);
        let pool = test_pool(idle_tx.clone());
        let (conn, _peer) = dummy_connection(1, idle_tx).await;
        pool.connections.lock().unwrap().push(conn.clone());
        assert!(conn.take(), "fresh connection starts Idle and takes");
        assert_eq!(conn.status(), Status::Busy);
        let long_ago = Instant::now()
            .checked_sub(Duration::from_millis(
                BUSY_LIVENESS_TIMEOUT_MS as u64 + 60_000,
            ))
            .unwrap_or_else(Instant::now);
        conn.set_last_activity(long_ago);

        assert!(
            pool.is_empty(),
            "a busy connection silent past the fuse must be reaped"
        );
        assert!(conn.is_closed());
    }

    /// A BUSY connection with fresh activity (traffic is flowing) must NOT
    /// be touched by the fuse.
    #[tokio::test(flavor = "current_thread")]
    async fn cleaner_keeps_busy_connection_with_fresh_activity() {
        let (idle_tx, _idle_rx) = mpsc::channel(8);
        let pool = test_pool(idle_tx.clone());
        let (conn, _peer) = dummy_connection(1, idle_tx).await;
        pool.connections.lock().unwrap().push(conn.clone());
        assert!(conn.take());
        // last_activity stays "now" — frames are arriving.

        assert!(
            !pool.is_empty(),
            "an active busy connection must survive the cleaner"
        );
        assert_eq!(conn.status(), Status::Busy);
    }

    /// A busy connection silent BELOW the fuse (still within one ping
    /// cadence of margin) must survive.
    #[tokio::test(flavor = "current_thread")]
    async fn cleaner_keeps_busy_connection_below_the_fuse() {
        let (idle_tx, _idle_rx) = mpsc::channel(8);
        let pool = test_pool(idle_tx.clone());
        let (conn, _peer) = dummy_connection(1, idle_tx).await;
        pool.connections.lock().unwrap().push(conn.clone());
        assert!(conn.take());
        let recent = Instant::now()
            .checked_sub(Duration::from_secs(30))
            .unwrap_or_else(Instant::now);
        conn.set_last_activity(recent); // 30s < 600s fuse

        assert!(
            !pool.is_empty(),
            "a busy connection below the fuse must survive"
        );
        assert_eq!(conn.status(), Status::Busy);
    }

    /// The fuse must not change IDLE handling: a healthy idle connection
    /// (fresh watermark) stays, and one idle past the idle liveness timeout
    /// is still reaped exactly as before.
    #[tokio::test(flavor = "current_thread")]
    async fn cleaner_idle_handling_unchanged_by_the_fuse() {
        let (idle_tx, _idle_rx) = mpsc::channel(8);
        let pool = test_pool(idle_tx.clone());
        pool.set_size(10);
        let (healthy, _p1) = dummy_connection(1, idle_tx.clone()).await;
        let (half_open, _p2) = dummy_connection(2, idle_tx).await;
        let long_ago = Instant::now()
            .checked_sub(Duration::from_millis(LIVENESS_TIMEOUT_MS as u64 + 60_000))
            .unwrap_or_else(Instant::now);
        half_open.set_last_activity(long_ago);
        pool.connections
            .lock()
            .unwrap()
            .extend([healthy.clone(), half_open.clone()]);

        assert!(!pool.is_empty(), "the healthy idle connection must stay");
        assert!(half_open.is_closed(), "the half-open idle one is reaped");
        assert_eq!(healthy.status(), Status::Idle);
    }
}
