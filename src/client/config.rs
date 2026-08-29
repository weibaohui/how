//! Client configuration.

use crate::client::connection::PING_INTERVAL_MS;
use crate::common::Rule;
use crate::log;
use std::collections::HashMap;
use std::path::Path;

/// Minimum sane `livenesstimeout` (ms), derived from the keepalive ping
/// interval (`PING_INTERVAL_MS` in `connection.rs` — change them together).
/// The client judges liveness by pong arrival, and pongs only come in
/// response to the keepalive ping (the server never pongs proactively). A
/// timeout below ~2x the ping interval cannot reliably observe two pongs, so
/// on a healthy idle link the gap between pongs (~30s) would already exceed
/// it and the reaper would false-reap live connections — draining and
/// churning the whole pool. Any configured value below this floor falls back
/// to the default (and logs a warning), so a too-small value degrades to
/// "safe" instead of "broken".
const MIN_LIVENESS_TIMEOUT_MS: i64 = 2 * PING_INTERVAL_MS;

/// Configures a WSP client.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    #[serde(default = "new_id")]
    pub id: String,
    #[serde(default = "default_targets")]
    pub targets: Vec<String>,
    #[serde(rename = "poolidlesize", default = "default_pool_idle_size")]
    pub pool_idle_size: i64,
    #[serde(rename = "poolmaxsize", default = "default_pool_max_size")]
    pub pool_max_size: i64,
    /// Liveness timeout: an idle tunnel that has received no frame at all
    /// (pong/data) from the server for longer than this is considered
    /// half-open (the peer or the path is gone) and is closed so the pool
    /// connector dials a replacement. The client sends a ping every 30s, so
    /// on a live link a pong arrives ~every 30s. The default (90s = 3 ping
    /// periods) tolerates a couple of missed pongs while still reaping dead
    /// links well before the server's `livenesstimeout` (default 120s), so
    /// the client reconnects proactively and the pool stays warm overnight.
    /// Sending pings without verifying pongs cannot detect dead links, so
    /// without this the client would hold a pool of dead connections and the
    /// server would report "no proxy available" after an idle night.
    /// `0` falls back to the default.
    #[serde(rename = "livenesstimeout", default = "default_liveness_timeout")]
    pub liveness_timeout: i64,
    /// Upstream connect timeout (ms): bounds DNS + TCP + TLS for dialing a
    /// route's upstream. Caps the damage of a blackholed address (dropped
    /// SYNs would otherwise stall the request for the OS retransmit ladder).
    /// **0/unset = no limit** (explicit-only: nothing is applied that was
    /// not configured).
    #[serde(rename = "connecttimeout", default)]
    pub connect_timeout: i64,
    /// Upstream call deadline (ms): from sending the request to the upstream
    /// until its response HEADERS arrive (the body then streams unbounded —
    /// a slow but flowing SSE stream is never cut). Bounds the
    /// time-to-first-byte a caller waits. **0/unset = no limit.**
    #[serde(rename = "upstreamtimeout", default)]
    pub upstream_timeout: i64,
    /// Tunnel establishment deadline (ms): bounds each phase of dialing the
    /// WSP server (TCP connect, WebSocket handshake, greeting send). A
    /// blackholed server address would otherwise hang a `Connecting` pool
    /// entry forever, silently consuming pool capacity.
    /// **0/unset = no limit.**
    #[serde(rename = "tunneltimeout", default)]
    pub tunnel_timeout: i64,
    /// Upstream stream idle timeout (ms): per-read stall detection while
    /// receiving an upstream response — a connection that stops sending for
    /// this long is considered dead. A flowing stream is never cut, however
    /// long it runs. **0/unset = no limit** (a truly hung upstream then
    /// holds the request open indefinitely).
    #[serde(rename = "streamidletimeout", default)]
    pub stream_idle_timeout: i64,
    /// Upstream connection pool idle timeout (ms): pooled keep-alive
    /// connections to a route's upstream that stay unused for this long are
    /// closed (a middlebox may have silently cut them in the meantime).
    /// **0/unset = pooled connections are kept forever.**
    #[serde(rename = "upstreamidletimeout", default)]
    pub upstream_idle_timeout: i64,
    #[serde(default)]
    pub whitelist: Vec<Rule>,
    #[serde(default)]
    pub blacklist: Vec<Rule>,
    #[serde(rename = "secretkey", default)]
    pub secret_key: String,
    /// Outbound proxy for upstream requests. Unset (empty) = follow the
    /// ambient http_proxy/https_proxy/all_proxy variables (backwards
    /// compatible; a startup WARNING is logged when any is set — reqwest
    /// honors the uppercase HTTP_PROXY form that curl ignores, a classic
    /// "curl is fast, the client program is slow" trap). "none" = always
    /// connect directly, env ignored. A URL like "http://127.0.0.1:7890" =
    /// use exactly that proxy for every upstream, env ignored. Each route's
    /// decision is printed at startup.
    #[serde(rename = "proxy", default)]
    pub proxy: String,
    /// Upstream hosts that must bypass the proxy even when one is active
    /// (matched against the route's upstream address, not the arrival host):
    /// exact host ("api.corp"), host:port ("api.corp:8443"), or domain
    /// suffix (".corp" / "*.corp" / "corp"). Comma-separated NO_PROXY-style
    /// entries also work.
    #[serde(rename = "noproxy", default)]
    pub noproxy: Vec<String>,
    /// Route map: arrival host (the Host the caller targets on the server,
    /// e.g. "127.0.0.1:8080" or "llm.example.com") -> upstream base URL
    /// (e.g. "https://ecloud.10086.cn/api/query/aigateway"). The client
    /// appends the request path to the upstream base, so the real destination
    /// lives in the client config, not in the caller's request.
    #[serde(default)]
    pub routes: HashMap<String, String>,
}

fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn default_targets() -> Vec<String> {
    vec!["ws://127.0.0.1:8080/register".to_string()]
}
fn default_pool_idle_size() -> i64 {
    10
}
fn default_pool_max_size() -> i64 {
    100
}
fn default_liveness_timeout() -> i64 {
    90000
}

/// Create a new client config with default values (including a fresh UUID).
pub fn new_config() -> Config {
    Config {
        id: new_id(),
        targets: default_targets(),
        pool_idle_size: default_pool_idle_size(),
        pool_max_size: default_pool_max_size(),
        liveness_timeout: default_liveness_timeout(),
        // Timeouts are explicit-only: 0 (= unset) means "no limit". Nothing
        // is applied that the operator did not configure.
        connect_timeout: 0,
        upstream_timeout: 0,
        tunnel_timeout: 0,
        stream_idle_timeout: 0,
        upstream_idle_timeout: 0,
        whitelist: Vec::new(),
        blacklist: Vec::new(),
        secret_key: String::new(),
        proxy: String::new(),
        noproxy: Vec::new(),
        routes: HashMap::new(),
    }
}

/// Load configuration from a YAML file.
pub fn load_configuration(path: &str) -> Result<Config, String> {
    let bytes = std::fs::read(Path::new(path)).map_err(|e| format!("{}", e))?;
    let mut config: Config = serde_yaml::from_slice(&bytes).map_err(|e| format!("{}", e))?;
    if config.id.is_empty() {
        config.id = new_id();
    }
    if config.targets.is_empty() {
        config.targets = default_targets();
    }
    if config.pool_idle_size == 0 {
        config.pool_idle_size = default_pool_idle_size();
    }
    if config.pool_max_size == 0 {
        config.pool_max_size = default_pool_max_size();
    }
    // Treat a non-positive duration as "unset" -> use the default. A bare
    // `== 0` check would let a negative value through and immediately reap
    // every idle connection on the first keepalive pass (`elapsed` is always
    // >= 0 > a negative threshold), silently killing the whole pool.
    if config.liveness_timeout <= 0 {
        config.liveness_timeout = default_liveness_timeout();
    } else if config.liveness_timeout < MIN_LIVENESS_TIMEOUT_MS {
        // The client judges liveness by pong arrival, and pongs only come in
        // response to the 30s ping; a timeout below ~2x the ping interval
        // cannot reliably see two pongs and would false-reap healthy links,
        // churning the pool. Clamp to the default and warn so the operator
        // knows their value was overridden.
        log::log(format!(
            "livenesstimeout {}ms is below the minimum {}ms (must exceed ~2x the {}ms ping \
             interval: pongs only arrive in response to a ping); falling back to default {}ms",
            config.liveness_timeout,
            MIN_LIVENESS_TIMEOUT_MS,
            PING_INTERVAL_MS,
            default_liveness_timeout()
        ));
        config.liveness_timeout = default_liveness_timeout();
    }
    // Timeouts (connecttimeout / upstreamtimeout / tunneltimeout /
    // streamidletimeout / upstreamidletimeout) are explicit-only: absent or
    // 0 means "no limit", a positive value is used as-is. No silent
    // defaults. (livenesstimeout above keeps its floor: a value below ~2x
    // the ping interval would false-reap healthy tunnels.)
    for rule in config.whitelist.iter_mut() {
        rule.compile().map_err(|e| e.to_string())?;
    }
    for rule in config.blacklist.iter_mut() {
        rule.compile().map_err(|e| e.to_string())?;
    }
    // An explicit proxy must be an http(s) URL (socks support is not compiled
    // in). "none"/empty already parsed into a mode by the time this matters;
    // a bogus value would otherwise fail every request at dial time.
    let p = config.proxy.trim();
    if !p.is_empty() && !p.eq_ignore_ascii_case("none") && !p.starts_with("http") {
        log::log(format!(
            "proxy '{p}' does not look like an http(s):// URL; the client only supports \
             HTTP proxies — treating it as-is anyway, expect upstream failures if it is wrong"
        ));
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    //! The client judges liveness by pong arrival, and pongs only come in
    //! response to the 30s ping — so a `livenesstimeout` below ~2x the ping
    //! interval would false-reap healthy links and churn the pool. These
    //! tests pin the load-time floor: too-small / unset / negative values
    //! all fall back to the safe default, while a sane value is preserved.

    use super::*;

    fn load_with(liveness: &str) -> Config {
        let dir = format!(
            "{}/wsp-cfg-{}",
            std::env::temp_dir().to_string_lossy(),
            std::process::id()
        );
        let _ = std::fs::create_dir_all(&dir);
        let path = format!("{dir}/client-{}.cfg", uuid::Uuid::new_v4());
        let yaml =
            format!("---\ntargets:\n - ws://127.0.0.1:8080/register\nsecretkey: k\n{liveness}\n");
        std::fs::write(&path, yaml).unwrap();
        load_configuration(&path).unwrap()
    }

    #[test]
    fn liveness_below_minimum_falls_back_to_default() {
        // 5s is below the 60s floor (cannot see two 30s pings) -> default.
        let c = load_with("livenesstimeout: 5000");
        assert_eq!(c.liveness_timeout, default_liveness_timeout());
    }

    #[test]
    fn liveness_unset_or_non_positive_falls_back_to_default() {
        // Explicit 0 and negative both -> default (no underflow reap).
        let c = load_with("livenesstimeout: 0");
        assert_eq!(c.liveness_timeout, default_liveness_timeout());
        let c = load_with("livenesstimeout: -1");
        assert_eq!(c.liveness_timeout, default_liveness_timeout());
    }

    #[test]
    fn liveness_at_or_above_minimum_is_preserved() {
        // Exactly the floor and a large value are kept as-is.
        let c = load_with(&format!("livenesstimeout: {MIN_LIVENESS_TIMEOUT_MS}"));
        assert_eq!(c.liveness_timeout, MIN_LIVENESS_TIMEOUT_MS);
        let c = load_with("livenesstimeout: 120000");
        assert_eq!(c.liveness_timeout, 120_000);
    }

    #[test]
    fn timeouts_unset_or_non_positive_mean_no_limit() {
        // Explicit-only semantics: absent / 0 / negative all stay as "no
        // limit" (kept as <= 0) — nothing silently becomes a default.
        let c = load_with("");
        assert_eq!(c.connect_timeout, 0);
        assert_eq!(c.upstream_timeout, 0);
        assert_eq!(c.tunnel_timeout, 0);
        assert_eq!(c.stream_idle_timeout, 0);
        assert_eq!(c.upstream_idle_timeout, 0);
        for key in [
            "connecttimeout",
            "upstreamtimeout",
            "tunneltimeout",
            "streamidletimeout",
            "upstreamidletimeout",
        ] {
            let c = load_with(&format!("{key}: 0"));
            let c2 = load_with(&format!("{key}: -5"));
            let (v, v2) = match key {
                "connecttimeout" => (c.connect_timeout, c2.connect_timeout),
                "upstreamtimeout" => (c.upstream_timeout, c2.upstream_timeout),
                "tunneltimeout" => (c.tunnel_timeout, c2.tunnel_timeout),
                "streamidletimeout" => (c.stream_idle_timeout, c2.stream_idle_timeout),
                _ => (c.upstream_idle_timeout, c2.upstream_idle_timeout),
            };
            assert!(v <= 0, "{key}: 0 must stay no-limit");
            assert!(v2 <= 0, "{key}: negative must stay no-limit");
        }
    }

    #[test]
    fn timeouts_positive_values_are_preserved_as_is() {
        let c = load_with(
            "connecttimeout: 2000\nupstreamtimeout: 45000\ntunneltimeout: 8000\n\
             streamidletimeout: 60000\nupstreamidletimeout: 30000",
        );
        assert_eq!(c.connect_timeout, 2000);
        assert_eq!(c.upstream_timeout, 45_000);
        assert_eq!(c.tunnel_timeout, 8000);
        assert_eq!(c.stream_idle_timeout, 60_000);
        assert_eq!(c.upstream_idle_timeout, 30_000);
    }
}
