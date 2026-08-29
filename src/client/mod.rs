//! Client-side WSP: connects to a WSP server and executes proxied requests.

pub mod config;
pub mod connection;
pub mod pool;

pub use config::{load_configuration, new_config, Config as ClientConfig};
pub use connection::{Connection, Status};
pub use pool::Pool;

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;
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
    /// 创建一个新的 how-client。
    /// 关键说明：
    /// - `connect_timeout(5s)`：DNS+TCP 握手限时 5 秒。
    ///   常见的"十几秒到几十秒延迟"根因就是：DNS 返回了 IPv6 AAAA 记录，
    ///   但实际网络 IPv6 不通，操作系统要等 20~30 秒 TCP SYN 超时才会回退
    ///   到 IPv4。5 秒的 connect_timeout 会提前打断并让上层（通常是 hyper）
    ///   立即尝试下一个地址族 / 下一个 IP。
    /// - `local_address(0.0.0.0)`：绑定 IPv4 源地址，底层仅解析并尝试 IPv4，
    ///   彻底跳过 IPv6 回退等待。若后续环境确实需要 IPv6，可去掉这一行并
    ///   改依赖 `trust-dns` 实现 Happy Eyeballs。
    /// - `timeout(60s)`：从发出请求到读完响应头+body 的总兜底超时，避免
    ///   慢上游把连接永久占死。按业务需求可再调大。
    /// - `pool_idle_timeout(90s)`：空闲连接超过 90 秒即丢弃，避免连接被
    ///   中间设备静默断开后，复用一条已经半开的连接导致再等一个超时。
    pub fn new(config: ClientConfig) -> Self {
        let inner = Arc::new(ClientInner {
            config: Arc::new(config),
            http_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::limited(10))
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(60))
                .pool_idle_timeout(Duration::from_secs(90))
                .local_address(Some(Ipv4Addr::UNSPECIFIED.into()))
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
