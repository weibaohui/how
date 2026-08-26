//! Client-side connection pool to a single WSP server target.

use crate::client::connection::{Connection, Status};
use crate::client::{ClientConfig, ClientInner};
use crate::log;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Pool manage a pool of connection to a remote Server.
pub struct Pool {
    client: Arc<ClientInner>,
    pub target: String,
    secret_key: String,
    connections: Mutex<Vec<Arc<Connection>>>,
    done: CancellationToken,
    self_weak: Weak<Pool>,
}

/// Number of open connections per status.
#[allow(dead_code)]
struct PoolSize {
    connecting: usize,
    idle: usize,
    running: usize,
    total: usize,
}

impl Pool {
    pub(crate) fn new(
        client: Arc<ClientInner>,
        target: String,
        secret_key: String,
        done: CancellationToken,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak: &Weak<Pool>| Pool {
            client,
            target,
            secret_key,
            connections: Mutex::new(Vec::new()),
            done,
            self_weak: weak.clone(),
        })
    }

    /// Start the pool: connect once, then refresh every second.
    pub fn start(self: Arc<Self>) {
        self.connector();
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = this.done.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => this.connector(),
                }
            }
        });
    }

    /// Ensure enough idle connections, up to `PoolMaxSize`. Create only one
    /// connection if the pool is empty. Mirrors Go's `pool.connector`.
    pub fn connector(&self) {
        let mut conns = self.connections.lock().unwrap();
        let size = self.size_locked(&conns);

        let mut to_create = self.client.config.pool_idle_size - size.idle as i64;
        if size.total == 0 {
            to_create = 1;
        }
        if size.total as i64 + to_create > self.client.config.pool_max_size {
            to_create = self.client.config.pool_max_size - size.total as i64;
        }
        if to_create <= 0 {
            return;
        }

        for _ in 0..to_create {
            let conn = Connection::new(self.self_weak.clone());
            conns.push(conn.clone());
            let target = self.target.clone();
            let secret = self.secret_key.clone();
            let config = self.client.config.clone();
            let pool_idle_size = self.client.config.pool_idle_size;
            let proxy_id = self.client.config.id.clone();
            let http_client = self.client.http_client.clone();
            let conn_c = conn.clone();
            tokio::spawn(async move {
                if let Err(e) = conn_c
                    .connect(
                        &target,
                        &secret,
                        pool_idle_size,
                        proxy_id,
                        http_client,
                        config,
                    )
                    .await
                {
                    log::log(format!("Unable to connect to {} : {}", target, e));
                    conn_c.shutdown();
                }
            });
        }
    }

    /// Remove a connection from the pool (by pointer identity).
    pub fn remove(&self, conn: &Connection) {
        let mut conns = self.connections.lock().unwrap();
        let addr = conn as *const Connection;
        conns.retain(|c| Arc::as_ptr(c) != addr);
    }

    /// Shutdown close all connection in the pool.
    pub fn shutdown(&self) {
        self.done.cancel();
        let conns = self.connections.lock().unwrap();
        for conn in conns.iter() {
            conn.shutdown();
        }
    }

    fn size_locked(&self, conns: &[Arc<Connection>]) -> PoolSize {
        let mut connecting = 0;
        let mut idle = 0;
        let mut running = 0;
        for connection in conns {
            match connection.status() {
                Status::Connecting => connecting += 1,
                Status::Idle => idle += 1,
                Status::Running => running += 1,
                Status::Closed => {}
            }
        }
        PoolSize {
            connecting,
            idle,
            running,
            total: conns.len(),
        }
    }

    /// Expose the shared config (used by the connection at connect time).
    pub fn config(&self) -> &ClientConfig {
        &self.client.config
    }
}
