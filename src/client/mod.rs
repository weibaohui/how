//! Client-side WSP: connects to a WSP server and executes proxied requests.

pub mod config;
pub mod connection;
pub mod pool;
pub mod proxy;

pub use config::{load_configuration, new_config, Config as ClientConfig};
pub use connection::{Connection, Status};
pub use pool::Pool;

use crate::log;
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
    /// - 超时全部**显式配置才生效**（`connecttimeout` / `streamidletimeout` /
    ///   `upstreamidletimeout` 等，0/未配置 = 不限时），没有任何隐藏默认值。
    /// - `connecttimeout`：DNS+TCP 握手限时。常见的"十几秒到几十秒延迟"
    ///   根因之一是：DNS 返回了 IPv6 AAAA 记录，但实际网络 IPv6 不通，
    ///   操作系统要等 20~30 秒 TCP SYN 超时才会回退到 IPv4。
    /// - `local_address(0.0.0.0)`：绑定 IPv4 源地址，底层仅解析并尝试 IPv4，
    ///   彻底跳过 IPv6 回退等待。若后续环境确实需要 IPv6，可去掉这一行并
    ///   改依赖 `trust-dns` 实现 Happy Eyeballs。
    /// - `streamidletimeout`（按"每次读取"计的停滞检测，不是总期限）：
    ///   流式响应只要还在持续出数据就永不截断，而彻底停止发送的连接在
    ///   期限内死亡，不会挂死 Running 隧道。总期限会截断超过它的 SSE 长流
    ///   （reqwest 语义是"直到 body 结束"），对 LLM 代理是错误的取舍。
    /// - `upstreamidletimeout`：上游空闲连接超过该时长即丢弃，避免连接被
    ///   中间设备静默断开后，复用一条已经半开的连接导致再等一个超时。
    pub fn new(config: ClientConfig) -> Self {
        // Outbound proxy: decide once, use everywhere. The same pure
        // decision (proxy::decide) feeds the reqwest matcher below and the
        // startup log, so what is printed is exactly how requests route.
        // Previously reqwest silently followed http_proxy/https_proxy/
        // all_proxy (including the uppercase HTTP_PROXY form curl ignores),
        // which is how a proxy-less box ended up detouring upstream traffic.
        let proxy_mode = proxy::ProxyMode::parse(&config.proxy);
        let proxy_env = proxy::ProxyEnv::from_env();
        let noproxy_cfg = config.noproxy.clone();

        log::log(format!("Outbound proxy mode: {}", proxy_mode.describe()));
        log::log(format!(
            "  env: http_proxy={} https_proxy={} all_proxy={} no_proxy={}",
            proxy_env.http.as_deref().unwrap_or("(unset)"),
            proxy_env.https.as_deref().unwrap_or("(unset)"),
            proxy_env.all.as_deref().unwrap_or("(unset)"),
            proxy_env.no_proxy.as_deref().unwrap_or("(unset)"),
        ));
        if matches!(proxy_mode, proxy::ProxyMode::Env) && proxy_env.any_proxy_set() {
            log::log(
                "  WARNING: ambient proxy variables above are ACTIVE because no 'proxy' is set \
                 in the config — upstream requests will go through them. Set 'proxy: none' \
                 (always direct) or 'proxy: <url>' (explicit) to control this."
                    .to_string(),
            );
        }

        let matcher_mode = proxy_mode.clone();
        let matcher_noproxy = noproxy_cfg.clone();
        let matcher_env = proxy_env.clone();
        // Timeouts are explicit-only: an option is applied iff it was
        // configured (> 0). Unset means exactly "no limit" — nothing the
        // operator did not ask for is imposed on the traffic.
        let ms = |v: i64| Duration::from_millis(v as u64);
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::limited(10))
            .local_address(Some(Ipv4Addr::UNSPECIFIED.into()))
            .proxy(reqwest::Proxy::custom(move |url| {
                match proxy::decide(url.as_str(), &matcher_mode, &matcher_noproxy, &matcher_env) {
                    proxy::Decision::Via(p) => Some(p),
                    proxy::Decision::Direct(_) => None,
                }
            }));
        if config.connect_timeout > 0 {
            builder = builder.connect_timeout(ms(config.connect_timeout));
        }
        if config.stream_idle_timeout > 0 {
            // Per-read stall detection, NOT a total deadline: a slow but
            // flowing SSE stream runs as long as it keeps flowing, while a
            // connection that stops sending dies here instead of hanging a
            // Running tunnel forever. (A client-level `timeout()` would cap
            // the WHOLE response — reqwest: "until the response body has
            // finished" — and truncate every stream longer than it.)
            builder = builder.read_timeout(ms(config.stream_idle_timeout));
        }
        // Explicitly None when unconfigured: reqwest's own default is a
        // 90s idle reap, which would violate "unset = no limit".
        builder = builder.pool_idle_timeout(
            (config.upstream_idle_timeout > 0).then(|| ms(config.upstream_idle_timeout)),
        );
        let http_client = match builder.build() {
            Ok(c) => c,
            Err(e) => {
                log::log(format!(
                    "reqwest client build failed ({e}); falling back to a default client — \
                     proxy rules, timeouts and the IPv4 pin are LOST in this mode"
                ));
                reqwest::Client::new()
            }
        };

        // Per-route decisions at startup: each route's upstream, and whether
        // it will be reached via a proxy or directly (and why).
        for (arrival, upstream) in &config.routes {
            match proxy::decide(upstream, &proxy_mode, &noproxy_cfg, &proxy_env) {
                proxy::Decision::Via(p) => {
                    log::log(format!("route {arrival} -> {upstream} : VIA PROXY {p}"));
                }
                proxy::Decision::Direct(reason) => {
                    log::log(format!("route {arrival} -> {upstream} : direct ({reason})"));
                }
            }
        }
        log::log(
            "outbound pinned to IPv4 (local_address 0.0.0.0): a broken-IPv6 environment \
             skips the 20-30s SYN-timeout fallback"
                .to_string(),
        );

        let inner = Arc::new(ClientInner {
            config: Arc::new(config),
            http_client,
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
