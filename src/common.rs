//! Shared types and helpers used by both the server and the client.

use regex::Regex;
use std::collections::HashMap;

/// The handshake response header carrying the server-assigned tunnel number
/// to the client (`/register`'s 101 sets it; the client adopts the number so
/// both ends log the same `tunnel#N` for the same connection). Shared here so
/// the two ends cannot drift on the name.
pub const TUNNEL_ID_HEADER: &str = "X-TUNNEL-ID";

/// A serializable version of `http::Request` (only the useful fields).
///
/// JSON field names mirror the original Go struct so that a Rust server is
/// wire-compatible with a Go client and vice-versa.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HttpRequest {
    #[serde(rename = "Method", default)]
    pub method: String,
    #[serde(rename = "URL", default)]
    pub url: String,
    #[serde(rename = "Header", default)]
    pub header: HashMap<String, Vec<String>>,
    #[serde(rename = "ContentLength", default)]
    pub content_length: i64,
}

/// A serializable version of `http::Response` (only the useful fields).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct HttpResponse {
    #[serde(rename = "StatusCode", default)]
    pub status_code: u16,
    #[serde(rename = "Header", default)]
    pub header: HashMap<String, Vec<String>>,
    #[serde(rename = "ContentLength", default)]
    pub content_length: i64,
}

/// Create a new empty `HttpResponse` (with an initialized header map).
/// Mirrors Go's `common.NewHTTPResponse()`.
pub fn new_http_response() -> HttpResponse {
    HttpResponse::default()
}

/// Rule matches HTTP requests to allow / deny access.
///
/// The YAML shape (lower-cased keys, matching Go's default yaml field naming)
/// is:
/// ```yaml
/// - method: "^GET$"
///   url: "^http(s)?://.*$"
///   headers:
///     X-CUSTOM-HEADER: "^value$"
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Rule {
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,

    // Compiled forms (not serialized).
    #[serde(skip)]
    pub method_re: Option<Regex>,
    #[serde(skip)]
    pub url_re: Option<Regex>,
    #[serde(skip)]
    pub headers_re: HashMap<String, (String, Regex)>,
}

impl Rule {
    /// Create a new Rule and compile its regular expressions.
    pub fn new(method: &str, url: &str, headers: HashMap<String, String>) -> Result<Self, String> {
        let mut rule = Rule {
            method: method.to_string(),
            url: url.to_string(),
            headers,
            method_re: None,
            url_re: None,
            headers_re: HashMap::new(),
        };
        rule.compile().map_err(|e| e.to_string())?;
        Ok(rule)
    }

    /// Compile the regular expressions.
    pub fn compile(&mut self) -> Result<(), regex::Error> {
        if !self.method.is_empty() {
            self.method_re = Some(Regex::new(&self.method)?);
        }
        if !self.url.is_empty() {
            self.url_re = Some(Regex::new(&self.url)?);
        }
        self.headers_re.clear();
        for (header, regex_str) in &self.headers {
            let regex = Regex::new(regex_str)?;
            self.headers_re
                .insert(header.clone(), (regex_str.clone(), regex));
        }
        Ok(())
    }

    /// Returns true if the request matches the rule.
    ///
    /// Header matching is case-insensitive (mirroring Go's
    /// `http.Header.Get`, which canonicalizes the lookup key).
    pub fn matches(&self, method: &str, url: &str, headers: &HashMap<String, Vec<String>>) -> bool {
        if let Some(re) = &self.method_re {
            if !re.is_match(method) {
                return false;
            }
        }
        if let Some(re) = &self.url_re {
            if !re.is_match(url) {
                return false;
            }
        }
        for (name, (_src, re)) in &self.headers_re {
            let val = headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .and_then(|(_, v)| v.first())
                .map(|s| s.as_str())
                .unwrap_or("");
            if !re.is_match(val) {
                return false;
            }
        }
        true
    }
}

impl std::fmt::Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {:?}", self.method, self.url, self.headers)
    }
}

/// HTTP status code used by WSP to signal a proxy error (matches the original
/// Go `common.ProxyError` which uses status 526).
pub const PROXY_ERROR_STATUS: u16 = 526;

/// HTTP status code used by the WSP client to signal that it could not execute
/// a proxied request (matches Go's `connection.error` status 527).
pub const CLIENT_ERROR_STATUS: u16 = 527;

/// Convenience accessor for the proxy error status code.
pub fn proxy_error_status() -> u16 {
    PROXY_ERROR_STATUS
}

/// Convenience accessor for the client error status code.
pub fn client_error_status() -> u16 {
    CLIENT_ERROR_STATUS
}
