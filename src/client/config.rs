//! Client configuration.

use crate::client::connection::PROBE_DEADLINE;
use crate::common::Rule;
use crate::log;
use std::collections::HashMap;
use std::path::Path;

/// Minimum sane `healthcheckinterval` (ms): a health round probes every idle
/// tunnel and waits up to the probe deadline (`PROBE_DEADLINE`, 10s) for the
/// pong, so an interval below the deadline cannot finish a round before the
/// next one is due — it would just chain rounds back-to-back. Below the
/// floor the value falls back to the default (with a warning), mirroring
/// `livenesstimeout`.
const MIN_HEALTH_CHECK_INTERVAL_MS: i64 = PROBE_DEADLINE.as_millis() as i64;

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
    /// Wedge backstop (ms): normal dead-link detection is the ACTIVE
    /// health-round probe (`healthcheckinterval` cadence, ping -> pong
    /// within its own 10s deadline) — but a tunnel whose write queue is too
    /// full to even send the probe ping cannot be probed. If such a tunnel
    /// has also received no frame at all for longer than this, it is
    /// declared dead (a live driver drains the queue within ~RTT; a
    /// permanently-full queue plus a silent link is a wedged tunnel).
    /// Historical note: this used to be the passive per-connection reaper's
    /// threshold, enforced with a ~2x-ping-interval floor to avoid
    /// false-reaping between pongs; the probe made that floor unnecessary,
    /// so any positive value is now the operator's call. (Caveat: the wedge
    /// branch still inherits the old floor's false-positive mode — right
    /// after a streamed response the queue can be momentarily full while the
    /// last pong is already old, so a value below ~2x the ping cadence can
    /// close a healthy tunnel in that narrow window; the default, 3
    /// cadences, keeps it negligible.)
    /// `0` falls back to the default.
    #[serde(rename = "livenesstimeout", default = "default_liveness_timeout")]
    pub liveness_timeout: i64,
    /// Pool health-check interval (ms): how often the client runs a health
    /// round — actively probe every idle tunnel (ping → pong), close the
    /// unresponsive ones, print each tunnel's status, and refill the pool to
    /// `poolidlesize` immediately. This is what guarantees the SERVER always
    /// has usable tunnels: the passive liveness reaper needs up to
    /// `livenesstimeout` to notice a dead link, and while a half-open tunnel
    /// still LOOKS idle the demand-driven connector dials nothing — during
    /// that window the server has no usable connection and requests fail
    /// until the client is restarted. Must exceed the 10s probe deadline;
    /// below the floor it falls back to the default and logs a warning.
    /// `0` falls back to the default.
    #[serde(
        rename = "healthcheckinterval",
        default = "default_health_check_interval"
    )]
    pub health_check_interval: i64,
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
    /// Log output level: "error", "warn", "info" (default) or "debug".
    /// Messages above the configured level are suppressed. Empty / unset =
    /// "info"; an invalid value falls back to "info" with a warning.
    #[serde(rename = "loglevel", default)]
    pub log_level: String,
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
fn default_health_check_interval() -> i64 {
    30000
}

/// Create a new client config with default values (including a fresh UUID).
pub fn new_config() -> Config {
    Config {
        id: new_id(),
        targets: default_targets(),
        pool_idle_size: default_pool_idle_size(),
        pool_max_size: default_pool_max_size(),
        liveness_timeout: default_liveness_timeout(),
        health_check_interval: default_health_check_interval(),
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
        log_level: String::new(),
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
    // Non-positive pool sizes are "unset" too: a negative idle size would
    // otherwise degenerate the connector (target 0 -> pure +1/tick creep),
    // a negative max would forbid every dial.
    if config.pool_idle_size <= 0 {
        config.pool_idle_size = default_pool_idle_size();
    }
    if config.pool_max_size <= 0 {
        config.pool_max_size = default_pool_max_size();
    }
    // Treat a non-positive duration as "unset" -> use the default (a bare
    // `== 0` check would let a negative value through and turn the probe's
    // wedge backstop into an always-dead verdict).
    if config.liveness_timeout <= 0 {
        config.liveness_timeout = default_liveness_timeout();
    }
    // Same treatment for the health-check cadence: non-positive = "unset" ->
    // default; a value below the probe deadline cannot complete a round
    // before the next is due, so it also falls back (with a warning).
    if config.health_check_interval <= 0 {
        config.health_check_interval = default_health_check_interval();
    } else if config.health_check_interval < MIN_HEALTH_CHECK_INTERVAL_MS {
        log::log_warn(format!(
            "healthcheckinterval {}ms is below the minimum {}ms (a health round waits up to \
             the {}ms probe deadline for every idle tunnel's pong); falling back to default {}ms",
            config.health_check_interval,
            MIN_HEALTH_CHECK_INTERVAL_MS,
            MIN_HEALTH_CHECK_INTERVAL_MS,
            default_health_check_interval()
        ));
        config.health_check_interval = default_health_check_interval();
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
        log::log_warn(format!(
            "proxy '{p}' does not look like an http(s):// URL; the client only supports \
             HTTP proxies — treating it as-is anyway, expect upstream failures if it is wrong"
        ));
    }
    // Apply the configured log level LAST so every load-path warning above
    // is still printed (the level only gates messages logged from here on).
    // Skipped under cfg(test): load_configuration runs in many parallel
    // tests and flipping the process-global level would race them; the
    // application logic itself is covered by src/log.rs unit tests.
    #[cfg(not(test))]
    log::set_level_from_str(&config.log_level);
    Ok(config)
}

#[cfg(test)]
mod tests {
    //! Dead-link detection is the active health-round probe (ping → pong
    //! within its own deadline); `livenesstimeout` is only the staleness
    //! backstop for tunnels that cannot be probed at all (wedged write
    //! queue). Unset / non-positive falls back to the default; any positive
    //! value is the operator's call (there is no ping-cadence floor to
    //! enforce anymore — the probe has its own deadline).

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
    fn liveness_unset_or_non_positive_falls_back_to_default() {
        // Explicit 0 and negative both -> default.
        let c = load_with("livenesstimeout: 0");
        assert_eq!(c.liveness_timeout, default_liveness_timeout());
        let c = load_with("livenesstimeout: -1");
        assert_eq!(c.liveness_timeout, default_liveness_timeout());
    }

    /// Since detection became the active probe, `livenesstimeout` is only the
    /// staleness backstop for tunnels that cannot be probed (wedge case) —
    /// there is no ping-cadence floor anymore, so any positive value is the
    /// operator's call and is preserved as-is.
    #[test]
    fn liveness_any_positive_value_is_preserved() {
        let c = load_with("livenesstimeout: 5000");
        assert_eq!(c.liveness_timeout, 5000);
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
    fn pool_sizes_non_positive_fall_back_to_defaults() {
        // 0 and negative both mean "unset": a negative idle size would
        // degenerate the demand-driven connector into +1/tick creep, so it
        // must fall back instead of passing through.
        let c = load_with("poolidlesize: -5");
        assert_eq!(c.pool_idle_size, default_pool_idle_size());
        let c = load_with("poolidlesize: 0");
        assert_eq!(c.pool_idle_size, default_pool_idle_size());
        let c = load_with("poolmaxsize: -1");
        assert_eq!(c.pool_max_size, default_pool_max_size());
    }

    /// `healthcheckinterval` (client): cadence of the periodic pool health
    /// round. Unset/0/negative -> default; below the 10s floor (a round
    /// cannot meaningfully run faster than its own probe deadline) -> the
    /// default too, with a warning; at/above the floor preserved as-is.
    #[test]
    fn health_check_interval_defaults_floor_and_preservation() {
        let d = default_health_check_interval();
        // Unset / 0 / negative -> default.
        assert_eq!(load_with("").health_check_interval, d);
        assert_eq!(load_with("healthcheckinterval: 0").health_check_interval, d);
        assert_eq!(
            load_with("healthcheckinterval: -1").health_check_interval,
            d
        );
        // Below the floor -> default (probing faster than the probe deadline
        // would just chain rounds back-to-back).
        assert_eq!(
            load_with("healthcheckinterval: 5000").health_check_interval,
            d
        );
        // At the floor and above -> preserved.
        assert_eq!(
            load_with("healthcheckinterval: 10000").health_check_interval,
            10_000
        );
        assert_eq!(
            load_with("healthcheckinterval: 60000").health_check_interval,
            60_000
        );
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

    /// `loglevel` is parsed as a plain string: unset stays empty (the loader
    /// applies the default), a configured value is preserved as-is. Applying
    /// the level is disabled under cfg(test) — see load_configuration — so
    /// only the field itself is asserted here.
    #[test]
    fn loglevel_unset_is_empty_and_configured_value_is_preserved() {
        assert_eq!(load_with("").log_level, "");
        assert_eq!(load_with("loglevel: debug").log_level, "debug");
        assert_eq!(load_with("loglevel: error").log_level, "error");
    }
}
