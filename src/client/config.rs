//! Client configuration.

use crate::common::Rule;
use std::collections::HashMap;
use std::path::Path;

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

/// Create a new client config with default values (including a fresh UUID).
pub fn new_config() -> Config {
    Config {
        id: new_id(),
        targets: default_targets(),
        pool_idle_size: default_pool_idle_size(),
        pool_max_size: default_pool_max_size(),
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
    for rule in config.whitelist.iter_mut() {
        rule.compile().map_err(|e| e.to_string())?;
    }
    for rule in config.blacklist.iter_mut() {
        rule.compile().map_err(|e| e.to_string())?;
    }
    Ok(config)
}
