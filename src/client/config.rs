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
    #[serde(default)]
    pub whitelist: Vec<Rule>,
    #[serde(default)]
    pub blacklist: Vec<Rule>,
    #[serde(rename = "secretkey", default)]
    pub secret_key: String,
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
        whitelist: Vec::new(),
        blacklist: Vec::new(),
        secret_key: String::new(),
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
    for rule in config.whitelist.iter_mut() {
        rule.compile().map_err(|e| e.to_string())?;
    }
    for rule in config.blacklist.iter_mut() {
        rule.compile().map_err(|e| e.to_string())?;
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
}
