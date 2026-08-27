//! Server configuration.

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

/// Create a new Server config with default values.
pub fn new_config() -> Config {
    Config {
        host: default_host(),
        port: default_port(),
        timeout: default_timeout(),
        idle_timeout: default_idle_timeout(),
        liveness_timeout: default_liveness_timeout(),
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
    Ok(config)
}
