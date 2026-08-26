//! Client-side WSP: connects to a WSP server and executes proxied requests.

pub mod config;
pub mod connection;
pub mod pool;

pub use config::{load_configuration, new_config, Config as ClientConfig};
pub use connection::{Connection, Status};
pub use pool::Pool;

use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Shared client state (config + HTTP client) used by every pool.
pub(crate) struct ClientInner {
    pub config: Arc<ClientConfig>,
    pub http_client: reqwest::Client,
}

/// Client connects to one or more Servers using HTTP websockets. The Server
/// can then send HTTP requests to execute.
pub struct Client {
    inner: Arc<ClientInner>,
    pools: Vec<Arc<Pool>>,
}

impl Client {
    /// Create a new client.
    pub fn new(config: ClientConfig) -> Self {
        let inner = Arc::new(ClientInner {
            config: Arc::new(config),
            http_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::limited(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        });
        Client {
            inner,
            pools: Vec::new(),
        }
    }

    /// Start the client: spawn a pool for each configured target.
    pub fn start(&mut self) {
        for target in &self.inner.config.targets.clone() {
            let pool = Pool::new(
                self.inner.clone(),
                target.clone(),
                self.inner.config.secret_key.clone(),
                CancellationToken::new(),
            );
            self.pools.push(pool.clone());
            pool.clone().start();
        }
    }

    /// Shutdown the client.
    pub fn shutdown(&self) {
        for pool in &self.pools {
            pool.shutdown();
        }
    }
}
