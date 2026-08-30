//! Server configuration.

use crate::log;
use std::path::Path;

/// Configures a Server. The server is a transparent catch-all reverse proxy:
/// it forwards every request (except `/register` and `/status`) to a WSP
/// client, which routes it to a configured upstream. There is no
/// caller-provided destination header.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: i32,
    #[serde(default = "default_timeout")]
    pub timeout: i64,
    #[serde(rename = "idletimeout", default = "default_idle_timeout")]
    pub idle_timeout: i64,
    /// Liveness timeout: an idle connection that has received no frame at all
    /// (ping/pong/data) for longer than this is considered half-open and is
    /// closed. Combined with the 30-second ping this detects dead tunnels in
    /// ~2 minutes instead of waiting for the OS TCP keepalive (~2 hours).
    /// `0` falls back to the default.
    #[serde(rename = "livenesstimeout", default = "default_liveness_timeout")]
    pub liveness_timeout: i64,
    /// Dispatch freshness (ms): an idle tunnel that has received no frame at
    /// all for longer than this is NOT handed to a request — the dispatcher
    /// closes it and tries the next one. The client pings every 30s, so a
    /// healthy idle tunnel is never much older than one ping interval; a
    /// stale one is half-open even if its status still says Idle (the client
    /// just has not noticed yet). This bounds the "request sent into a dead
    /// tunnel" window to tunnels that died within the freshness window.
    /// `0` falls back to the default (75s); a value below 40s cannot
    /// reliably see one client ping and falls back to the default with a
    /// warning.
    #[serde(rename = "dispatchfreshness", default = "default_dispatch_freshness")]
    pub dispatch_freshness: i64,
    /// Busy liveness timeout (ms): a BUSY connection that has received no
    /// frame at all for longer than this is closed. This is the last-resort
    /// fuse for a bidirectionally partitioned link: the client's close
    /// cannot reach the server either, and without `upstreamtimeout` the
    /// proxied request would otherwise hang forever. On a LIVE link the
    /// client's 30s keepalive pings keep this watermark fresh in EVERY
    /// request phase (uploads, waiting on upstream headers, SSE gaps all
    /// included), so the fuse only fires when pings stop arriving at all:
    /// the link is dead, the client is wedged, or the path is fully
    /// backlogged (e.g. a caller that stopped reading the response).
    /// `0` falls back to the default (600s); a value below
    /// 60s falls back to the default with a warning.
    #[serde(
        rename = "busylivenesstimeout",
        default = "default_busy_liveness_timeout"
    )]
    pub busy_liveness_timeout: i64,
    /// Upstream roundtrip deadline (ms): from forwarding a request to a WSP
    /// client tunnel until that client returns the upstream's response
    /// headers. Bounds what the HTTP caller waits when a client is stuck on
    /// a slow/hung upstream; the response BODY streams unbounded after the
    /// headers arrive. **0/unset = no limit** (explicit-only).
    #[serde(rename = "upstreamtimeout", default)]
    pub upstream_timeout: i64,
    #[serde(rename = "secretkey", default)]
    pub secret_key: String,
    /// Allowed arrival hostnames (the hostname part of the request's `Host`
    /// header). When non-empty, a request is only accepted if its `Host`
    /// hostname matches one of these; requests addressed by IP (or any
    /// unlisted host) are rejected (HTTP 403). Empty = allow any host.
    #[serde(rename = "allowedhosts", default)]
    pub allowed_hosts: Vec<String>,
    /// Source IP whitelist. When non-empty, only requests from these IPs are
    /// served; any other source IP is rejected (403 "DENY <ip>").
    #[serde(rename = "allowips", default)]
    pub allowips: Vec<String>,
    /// API key whitelist. When non-empty, every proxied request must carry an
    /// `Authorization: Bearer <key>` header whose key is in this list; missing
    /// or non-matching keys are rejected (403). Prevents scanners from pushing
    /// requests through to the backend.
    #[serde(rename = "apikeys", default)]
    pub apikeys: Vec<String>,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}
fn default_port() -> i32 {
    8080
}
fn default_timeout() -> i64 {
    1000
}
fn default_idle_timeout() -> i64 {
    60000
}
fn default_liveness_timeout() -> i64 {
    120000
}
fn default_dispatch_freshness() -> i64 {
    // 2.5x the client's fixed 30s ping cadence: a healthy idle tunnel
    // freshens the server's last-activity watermark ~every 30s, so 75s
    // tolerates one lost ping while still catching half-open tunnels well
    // before the 120s idle liveness reaper would.
    75000
}
fn default_busy_liveness_timeout() -> i64 {
    600000
}

/// Floor for `dispatchfreshness`: below ~1.3x the client ping cadence a
/// healthy idle tunnel would be closed between pings (pure churn — the
/// client would redial it every time). Falls back to the default instead.
const MIN_DISPATCH_FRESHNESS_MS: i64 = 40000;
/// Floor for `busylivenesstimeout`: the client's 30s keepalive pings refresh
/// a busy connection's watermark on any live link, so a value below ~2x that
/// cadence could false-fire on nothing worse than scheduling jitter.
const MIN_BUSY_LIVENESS_TIMEOUT_MS: i64 = 60000;

/// Create a new Server config with default values.
pub fn new_config() -> Config {
    Config {
        host: default_host(),
        port: default_port(),
        timeout: default_timeout(),
        idle_timeout: default_idle_timeout(),
        liveness_timeout: default_liveness_timeout(),
        dispatch_freshness: default_dispatch_freshness(),
        busy_liveness_timeout: default_busy_liveness_timeout(),
        // Explicit-only: 0 (= unset) means "no limit".
        upstream_timeout: 0,
        secret_key: String::new(),
        allowed_hosts: Vec::new(),
        allowips: Vec::new(),
        apikeys: Vec::new(),
    }
}

/// Load configuration from a YAML file.
pub fn load_configuration(path: &str) -> Result<Config, String> {
    let bytes = std::fs::read(Path::new(path)).map_err(|e| format!("{}", e))?;
    let mut config: Config = serde_yaml::from_slice(&bytes).map_err(|e| format!("{}", e))?;
    if config.host.is_empty() {
        config.host = default_host();
    }
    if config.port == 0 {
        config.port = default_port();
    }
    // Treat any non-positive duration as "unset" -> use the default. A bare
    // `== 0` check would let a negative value through and, for liveness/
    // idle, immediately reap every idle connection on the first cleaner pass
    // (`elapsed_ms` is always >= 0 > negative threshold). For `timeout` a
    // negative would underflow to a ~584-million-year wait.
    if config.timeout <= 0 {
        config.timeout = default_timeout();
    }
    if config.idle_timeout <= 0 {
        config.idle_timeout = default_idle_timeout();
    }
    if config.liveness_timeout <= 0 {
        config.liveness_timeout = default_liveness_timeout();
    }
    if config.dispatch_freshness <= 0 {
        config.dispatch_freshness = default_dispatch_freshness();
    } else if config.dispatch_freshness < MIN_DISPATCH_FRESHNESS_MS {
        // Below ~1.3x the client's 30s ping cadence a healthy idle tunnel
        // would be closed between pings — pure churn, the client just
        // redials it. Degrade to the safe default with a warning.
        log::log(format!(
            "dispatchfreshness {}ms is below the minimum {}ms (must exceed the client's 30s \
             ping cadence with margin); falling back to default {}ms",
            config.dispatch_freshness,
            MIN_DISPATCH_FRESHNESS_MS,
            default_dispatch_freshness()
        ));
        config.dispatch_freshness = default_dispatch_freshness();
    }
    if config.busy_liveness_timeout <= 0 {
        config.busy_liveness_timeout = default_busy_liveness_timeout();
    } else if config.busy_liveness_timeout < MIN_BUSY_LIVENESS_TIMEOUT_MS {
        log::log(format!(
            "busylivenesstimeout {}ms is below the minimum {}ms (must exceed the client's 30s \
             ping cadence with margin); falling back to default {}ms",
            config.busy_liveness_timeout,
            MIN_BUSY_LIVENESS_TIMEOUT_MS,
            default_busy_liveness_timeout()
        ));
        config.busy_liveness_timeout = default_busy_liveness_timeout();
    }
    // upstreamtimeout is explicit-only: absent or <= 0 stays "no limit" —
    // nothing is applied that was not configured.
    Ok(config)
}

#[cfg(test)]
mod tests {
    //! `upstreamtimeout` bounds what an HTTP caller waits for a WSP client's
    //! upstream response headers. Explicit-only semantics: absent / 0 /
    //! negative all mean "no limit" (stay <= 0) — no silent default — while
    //! a positive value is preserved as-is.

    use super::*;

    fn load_with(upstream_timeout: &str) -> Config {
        let dir = format!(
            "{}/wsp-srv-cfg-{}",
            std::env::temp_dir().to_string_lossy(),
            std::process::id()
        );
        let _ = std::fs::create_dir_all(&dir);
        let path = format!("{dir}/server-{}.cfg", uuid::Uuid::new_v4());
        let yaml = format!("---\nhost: 127.0.0.1\nport: 18080\n{upstream_timeout}\n");
        std::fs::write(&path, yaml).unwrap();
        load_configuration(&path).unwrap()
    }

    #[test]
    fn upstream_timeout_unset_or_non_positive_means_no_limit() {
        assert!(load_with("").upstream_timeout <= 0);
        assert!(load_with("upstreamtimeout: 0").upstream_timeout <= 0);
        assert!(load_with("upstreamtimeout: -1").upstream_timeout <= 0);
    }

    #[test]
    fn upstream_timeout_positive_value_is_preserved() {
        let c = load_with("upstreamtimeout: 60000");
        assert_eq!(c.upstream_timeout, 60_000);
    }

    #[test]
    fn dispatch_freshness_unset_or_non_positive_falls_back_to_default() {
        assert_eq!(load_with("").dispatch_freshness, 75_000);
        assert_eq!(load_with("dispatchfreshness: 0").dispatch_freshness, 75_000);
        assert_eq!(
            load_with("dispatchfreshness: -5").dispatch_freshness,
            75_000
        );
    }

    #[test]
    fn dispatch_freshness_below_floor_falls_back_to_default() {
        // 30s is below the 40s floor: a healthy idle tunnel would be closed
        // between the client's 30s pings (pure churn) — degrade to default.
        assert_eq!(
            load_with("dispatchfreshness: 30000").dispatch_freshness,
            75_000
        );
    }

    #[test]
    fn dispatch_freshness_sane_value_is_preserved() {
        assert_eq!(
            load_with("dispatchfreshness: 40000").dispatch_freshness,
            40_000
        );
        assert_eq!(
            load_with("dispatchfreshness: 90000").dispatch_freshness,
            90_000
        );
    }

    #[test]
    fn busy_liveness_unset_or_non_positive_falls_back_to_default() {
        assert_eq!(load_with("").busy_liveness_timeout, 600_000);
        assert_eq!(
            load_with("busylivenesstimeout: 0").busy_liveness_timeout,
            600_000
        );
        assert_eq!(
            load_with("busylivenesstimeout: -1").busy_liveness_timeout,
            600_000
        );
    }

    #[test]
    fn busy_liveness_below_floor_falls_back_to_default() {
        assert_eq!(
            load_with("busylivenesstimeout: 5000").busy_liveness_timeout,
            600_000
        );
    }

    #[test]
    fn busy_liveness_sane_value_is_preserved() {
        assert_eq!(
            load_with("busylivenesstimeout: 60000").busy_liveness_timeout,
            60_000
        );
        assert_eq!(
            load_with("busylivenesstimeout: 900000").busy_liveness_timeout,
            900_000
        );
    }
}
