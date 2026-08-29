//! Outbound proxy decision logic for upstream requests.
//!
//! reqwest follows the ambient proxy environment variables by default
//! (`http_proxy` / `https_proxy` / `all_proxy`, plus `no_proxy`), which
//! silently reroutes upstream traffic whenever the client happens to be
//! started under such an env — including the uppercase `HTTP_PROXY` form
//! that curl ignores for CGI-safety reasons but reqwest honors. That
//! mismatch is exactly the "curl is fast on this box, the client program
//! is slow" failure mode.
//!
//! This module makes the decision explicit and observable. One pure
//! function [`decide`] determines, for a given upstream URL, whether the
//! request goes via a proxy (and which one) or direct. The same function
//! feeds both the reqwest `Proxy::custom` matcher and the startup log, so
//! what is printed at startup is exactly what happens at runtime.
//!
//! Configuration (client config file):
//!
//! ```yaml
//! # unset       -> follow the ambient env variables (backwards compatible;
//! #                a warning is logged when any is set)
//! # "none"      -> always connect directly, ignore all env variables
//! # "http://…"  -> use exactly this proxy for every upstream, ignore env
//! proxy: none
//!
//! # Upstreams that must bypass the proxy even when one is active. Entries
//! # match the route's upstream host: exact host ("api.corp"), host:port
//! # ("api.corp:8443"), or domain suffix ("*.corp" / ".corp" / "corp").
//! noproxy:
//!  - "127.0.0.1"
//!  - "*.corp.internal"
//! ```

/// The configured proxy mode, parsed from the `proxy` config key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ProxyMode {
    /// `proxy` unset: follow the ambient environment variables. Backwards
    /// compatible with the pre-config behavior; a startup warning is logged
    /// whenever any proxy variable is detected in this mode.
    Env,
    /// `proxy: none`: never use a proxy; env variables are ignored.
    Direct,
    /// `proxy: <url>`: use exactly this proxy URL; env variables are ignored.
    Explicit(String),
}

impl ProxyMode {
    /// Parse the raw config value: empty -> Env, "none" (any case) ->
    /// Direct, anything else -> Explicit (treated as a proxy URL).
    pub(crate) fn parse(raw: &str) -> Self {
        let t = raw.trim();
        if t.is_empty() {
            ProxyMode::Env
        } else if t.eq_ignore_ascii_case("none") {
            ProxyMode::Direct
        } else {
            ProxyMode::Explicit(t.to_string())
        }
    }

    /// Short human-readable name for the startup log.
    pub(crate) fn describe(&self) -> String {
        match self {
            ProxyMode::Env => "env (no 'proxy' set in config)".to_string(),
            ProxyMode::Direct => "none (always direct)".to_string(),
            ProxyMode::Explicit(u) => format!("explicit {u}"),
        }
    }
}

/// The proxy-relevant slice of the environment. Kept as a plain struct so
/// tests can inject values without touching the process env.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProxyEnv {
    pub http: Option<String>,
    pub https: Option<String>,
    pub all: Option<String>,
    pub no_proxy: Option<String>,
}

impl ProxyEnv {
    /// Read the ambient variables, lowercase preferred over uppercase
    /// (mirroring reqwest's lookup order).
    pub(crate) fn from_env() -> Self {
        fn pick(lower: &str, upper: &str) -> Option<String> {
            std::env::var(lower)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .or_else(|| std::env::var(upper).ok().filter(|v| !v.trim().is_empty()))
        }
        ProxyEnv {
            http: pick("http_proxy", "HTTP_PROXY"),
            https: pick("https_proxy", "HTTPS_PROXY"),
            all: pick("all_proxy", "ALL_PROXY"),
            no_proxy: pick("no_proxy", "NO_PROXY"),
        }
    }

    /// True when any proxy variable (not no_proxy) is set — used to decide
    /// whether the startup warning applies.
    pub(crate) fn any_proxy_set(&self) -> bool {
        self.http.is_some() || self.https.is_some() || self.all.is_some()
    }
}

/// The decision for one upstream URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Route the request through this proxy URL.
    Via(String),
    /// Connect directly; the reason explains why (shown in the startup log).
    Direct(String),
}

/// Extract the `(host, port)` of a URL like `scheme://[user@]host[:port]/path`.
/// Port defaults by scheme: 80 for http, 443 for https. Returns `None` for
/// URLs too malformed to inspect (the caller treats those as direct).
pub(crate) fn host_port_of(url: &str) -> Option<(String, u16)> {
    let (scheme, rest) = url.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    let default_port = match scheme {
        "https" | "wss" => 443,
        _ => 80,
    };
    match authority.rsplit_once(':') {
        Some((h, p)) => Some((h.to_string(), p.parse().unwrap_or(default_port))),
        None => Some((authority.to_string(), default_port)),
    }
}

/// Whether `host`[:port] matches any `noproxy` config entry (or a
/// comma-separated `NO_PROXY` value). Returns the matched entry for the
/// log. Semantics follow curl's no_proxy: `*` matches everything; an entry
/// matches on exact host or on dot-boundary suffix; an optional `:port`
/// suffix requires the port to match too.
pub(crate) fn noproxy_matches(host: &str, port: u16, list: &str) -> Option<String> {
    for raw_entry in list.split(',') {
        let entry = raw_entry.trim();
        if entry.is_empty() {
            continue;
        }
        if entry == "*" {
            return Some(entry.to_string());
        }
        // Normalize "*.example.com", ".example.com" and "example.com" all to
        // the bare domain (curl-style suffix match on the dot boundary).
        let entry = entry.strip_prefix("*.").unwrap_or(entry);
        let entry = entry.strip_prefix('.').unwrap_or(entry);
        let (e_host, e_port) = match entry.split_once(':') {
            Some((h, p)) => (h, p.parse::<u16>().ok()),
            None => (entry, None),
        };
        let host_hit = host == e_host || host.ends_with(&format!(".{e_host}"));
        if host_hit && e_port.is_none_or(|p| p == port) {
            return Some(raw_entry.trim().to_string());
        }
    }
    None
}

/// Decide how `upstream` (a route's base URL) is reached. Pure: takes the
/// mode, the config's `noproxy` patterns and the env snapshot; the same
/// result drives the reqwest matcher and the startup log.
pub(crate) fn decide(
    upstream: &str,
    mode: &ProxyMode,
    noproxy_cfg: &[String],
    env: &ProxyEnv,
) -> Decision {
    let Some((host, port)) = host_port_of(upstream) else {
        return Decision::Direct("upstream URL not parseable".to_string());
    };

    if matches!(mode, ProxyMode::Direct) {
        return Decision::Direct("proxy: none".to_string());
    }

    // Config noproxy always wins, in every mode.
    for pattern in noproxy_cfg {
        if let Some(hit) = noproxy_matches(&host, port, pattern) {
            return Decision::Direct(format!("matches noproxy entry '{hit}'"));
        }
    }

    let proxy_url: Option<String> = match mode {
        ProxyMode::Explicit(u) => Some(u.clone()),
        ProxyMode::Direct => unreachable!(),
        ProxyMode::Env => {
            if let Some(list) = env.no_proxy.as_deref() {
                if noproxy_matches(&host, port, list).is_some() {
                    return Decision::Direct("matches NO_PROXY env".to_string());
                }
            }
            let https = upstream.starts_with("https://") || upstream.starts_with("wss://");
            if https {
                env.https.clone().or_else(|| env.all.clone())
            } else {
                env.http.clone().or_else(|| env.all.clone())
            }
        }
    };

    match proxy_url {
        Some(u) => Decision::Via(u),
        None => Decision::Direct(if matches!(mode, ProxyMode::Env) {
            "no proxy configured for this upstream".to_string()
        } else {
            unreachable!()
        }),
    }
}

#[cfg(test)]
mod tests {
    //! The proxy decision must be deterministic and observable: the startup
    //! log prints exactly what decide() returns, and the reqwest matcher
    //! routes by it, so these tests pin the behavior both halves rely on.

    use super::*;

    fn env(http: Option<&str>, https: Option<&str>, all: Option<&str>) -> ProxyEnv {
        ProxyEnv {
            http: http.map(str::to_string),
            https: https.map(str::to_string),
            all: all.map(str::to_string),
            no_proxy: None,
        }
    }

    #[test]
    fn parse_modes() {
        assert_eq!(ProxyMode::parse(""), ProxyMode::Env);
        assert_eq!(ProxyMode::parse("   "), ProxyMode::Env);
        assert_eq!(ProxyMode::parse("none"), ProxyMode::Direct);
        assert_eq!(ProxyMode::parse("NONE"), ProxyMode::Direct);
        assert_eq!(
            ProxyMode::parse(" http://127.0.0.1:7890 "),
            ProxyMode::Explicit("http://127.0.0.1:7890".to_string())
        );
    }

    #[test]
    fn env_mode_without_env_is_direct() {
        let d = decide(
            "http://api.example.com",
            &ProxyMode::Env,
            &[],
            &env(None, None, None),
        );
        assert_eq!(
            d,
            Decision::Direct("no proxy configured for this upstream".to_string())
        );
    }

    #[test]
    fn env_mode_http_proxy_applies_to_http_upstream() {
        let e = env(Some("http://127.0.0.1:7890"), None, None);
        assert_eq!(
            decide("http://api.example.com/v1", &ProxyMode::Env, &[], &e),
            Decision::Via("http://127.0.0.1:7890".to_string())
        );
        // An http-only var must NOT capture an https upstream.
        assert_eq!(
            decide("https://api.example.com", &ProxyMode::Env, &[], &e),
            Decision::Direct("no proxy configured for this upstream".to_string())
        );
    }

    #[test]
    fn env_mode_https_and_all_fallback() {
        let e = env(None, Some("http://p:1"), None);
        assert_eq!(
            decide("https://api.example.com", &ProxyMode::Env, &[], &e),
            Decision::Via("http://p:1".to_string())
        );
        let e = env(None, None, Some("http://p:2"));
        assert_eq!(
            decide("https://api.example.com", &ProxyMode::Env, &[], &e),
            Decision::Via("http://p:2".to_string())
        );
        assert_eq!(
            decide("http://api.example.com", &ProxyMode::Env, &[], &e),
            Decision::Via("http://p:2".to_string())
        );
    }

    #[test]
    fn env_mode_no_proxy_excludes() {
        let mut e = env(Some("http://p:1"), None, None);
        e.no_proxy = Some(".example.com,localhost".to_string());
        assert_eq!(
            decide("http://api.example.com", &ProxyMode::Env, &[], &e),
            Decision::Direct("matches NO_PROXY env".to_string())
        );
        assert_eq!(
            decide("http://other.org", &ProxyMode::Env, &[], &e),
            Decision::Via("http://p:1".to_string())
        );
    }

    #[test]
    fn explicit_mode_uses_configured_proxy_and_ignores_env() {
        let mode = ProxyMode::Explicit("http://cfg-proxy:7890".to_string());
        // Env vars must be ignored entirely.
        let e = env(Some("http://env-proxy:1"), Some("http://env-proxy:2"), None);
        assert_eq!(
            decide("https://api.example.com", &mode, &[], &e),
            Decision::Via("http://cfg-proxy:7890".to_string())
        );
    }

    #[test]
    fn none_mode_is_always_direct_even_with_env() {
        let e = env(Some("http://p:1"), None, None);
        assert_eq!(
            decide("http://api.example.com", &ProxyMode::Direct, &[], &e),
            Decision::Direct("proxy: none".to_string())
        );
    }

    #[test]
    fn config_noproxy_wins_over_explicit_proxy() {
        let mode = ProxyMode::Explicit("http://cfg-proxy:7890".to_string());
        let noproxy = vec!["*.corp.internal".to_string(), "10.0.0.5".to_string()];
        assert_eq!(
            decide(
                "http://api.corp.internal/x",
                &mode,
                &noproxy,
                &ProxyEnv::default()
            ),
            Decision::Direct("matches noproxy entry '*.corp.internal'".to_string())
        );
        assert_eq!(
            decide(
                "http://10.0.0.5:9000/x",
                &mode,
                &noproxy,
                &ProxyEnv::default()
            ),
            Decision::Direct("matches noproxy entry '10.0.0.5'".to_string())
        );
        assert_eq!(
            decide(
                "http://api.example.com",
                &mode,
                &noproxy,
                &ProxyEnv::default()
            ),
            Decision::Via("http://cfg-proxy:7890".to_string())
        );
    }

    #[test]
    fn noproxy_entry_forms() {
        // Exact host, dot-suffix, wildcard-suffix and port-scoped entries.
        assert!(noproxy_matches("api.corp", 80, "api.corp").is_some());
        assert!(noproxy_matches("a.b.corp", 80, ".corp").is_some());
        assert!(noproxy_matches("a.b.corp", 80, "*.corp").is_some());
        assert!(noproxy_matches("x.evil.com", 80, "corp, evil.com").is_some());
        assert!(noproxy_matches("api.corp", 8443, "api.corp:8443").is_some());
        assert!(noproxy_matches("api.corp", 9000, "api.corp:8443").is_none());
        assert!(noproxy_matches("api.corp", 80, "bcorp").is_none());
        assert!(noproxy_matches("anything", 80, "*").is_some());
        // A subdomain must not match a sibling/parent-lookalike entry.
        assert!(noproxy_matches("notcorp.com", 80, "corp.com.example").is_none());
    }

    #[test]
    fn host_port_extraction() {
        assert_eq!(
            host_port_of("http://api.example.com/v1/x?q=1"),
            Some(("api.example.com".to_string(), 80))
        );
        assert_eq!(
            host_port_of("https://api.example.com"),
            Some(("api.example.com".to_string(), 443))
        );
        assert_eq!(
            host_port_of("http://user:pass@10.0.0.5:9000/base"),
            Some(("10.0.0.5".to_string(), 9000))
        );
        assert_eq!(host_port_of("not-a-url"), None);
    }

    #[test]
    fn unparseable_upstream_is_direct() {
        assert_eq!(
            decide(":::", &ProxyMode::Env, &[], &env(None, None, None)),
            Decision::Direct("upstream URL not parseable".to_string())
        );
    }
}
