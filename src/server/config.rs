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
    #[serde(rename = "secretkey", default)]
    pub secret_key: String,
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

/// Create a new Server config with default values.
pub fn new_config() -> Config {
    Config {
        host: default_host(),
        port: default_port(),
        timeout: default_timeout(),
        idle_timeout: default_idle_timeout(),
        secret_key: String::new(),
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
    if config.timeout == 0 {
        config.timeout = default_timeout();
    }
    if config.idle_timeout == 0 {
        config.idle_timeout = default_idle_timeout();
    }
    Ok(config)
}
