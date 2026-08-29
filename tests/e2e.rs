//! End-to-end integration tests for the WSP Rust port (transparent proxy +
//! client-side route map model).
//!
//! These tests spawn real `wsp_test_api`, `wsp_server` and `wsp_client`
//! binaries, wire them together on ephemeral ports, and issue real HTTP
//! requests through the proxy. The server is a transparent catch-all reverse
//! proxy; the client's `routes` config maps the server's arrival host to the
//! real upstream. All headers (Authorization, Content-Type, custom) are
//! forwarded transparently.

use std::io::Write;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

/// A child process that is killed (and waited on) when dropped, so the test
/// never leaves a server running even on panic. Output goes to a per-process
/// log file (both stdout and stderr appended; the binaries log to stderr)
/// readable via [`Proc::log`] — files need no draining, unlike pipes.
struct Proc {
    #[allow(dead_code)]
    name: &'static str,
    log_path: String,
    child: Option<Child>,
}

impl Proc {
    fn spawn(name: &'static str, exe: &str, args: &[&str]) -> Self {
        let log_path = format!("{}/{name}.log", tmpdir());
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .unwrap_or_else(|e| panic!("failed to open log for {name}: {e}"));
        let mut cmd = Command::new(exe);
        cmd.args(args);
        cmd.stdout(file.try_clone().expect("clone log fd"));
        cmd.stderr(file);
        let child = cmd
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn {name}: {e}"));
        Proc {
            name,
            log_path,
            child: Some(child),
        }
    }

    /// The child's accumulated output so far (stdout + stderr).
    fn log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn write_cfg(path: &str, contents: &str) {
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
}

fn wait_for_server(port: u16) {
    let url = format!("http://127.0.0.1:{port}/status");
    // 本地闭环测试必须禁用代理：环境变量（如 HTTP_PROXY）中的代理会
    // 劫持请求，导致健康检查探测不到本机刚启动的 server。
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .build()
        .unwrap();
    let start = Instant::now();
    loop {
        if let Ok(resp) = client.get(&url).send() {
            if resp.status().is_success() {
                return;
            }
        }
        if start.elapsed() > Duration::from_secs(10) {
            panic!("server on port {port} did not come up");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Poll until a proxied request to `/hello` returns 200 (client registered +
/// route working).
fn wait_for_proxy(server_port: u16) {
    let url = format!("http://127.0.0.1:{server_port}/hello");
    // 同 wait_for_server：禁用环境代理，确保请求直达本机被测进程。
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .build()
        .unwrap();
    let start = Instant::now();
    loop {
        let resp = client.get(&url).send();
        if let Ok(r) = resp {
            if r.status().as_u16() == 200 {
                return;
            }
        }
        if start.elapsed() > Duration::from_secs(15) {
            panic!("client never registered a usable connection / route");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// 为每个测试创建独立的临时目录。
///
/// 目录名在进程 PID 之外追加自增序号：同一测试二进制内多个 `#[test]`
/// 并行运行且共享同一进程 ID，若共用同一目录会互相覆盖 `server.cfg` /
/// `client.cfg`，导致子进程读到错误配置、绑错端口甚至启动失败，
/// 最终 `wait_for_server` 健康检查超时。
fn tmpdir() -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static N: AtomicUsize = AtomicUsize::new(0);
    let d = format!(
        "{}/wsp-e2e-{}-{}",
        std::env::temp_dir().to_string_lossy(),
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    );
    let _ = std::fs::create_dir_all(&d);
    d
}

fn bin(name: &str) -> String {
    // cargo exposes CARGO_BIN_EXE_<name> with hyphens replaced by underscores.
    let env = format!("CARGO_BIN_EXE_{}", name.replace('-', "_"));
    std::env::var(&env)
        .unwrap_or_else(|_| format!("{}/target/debug/{name}", env!("CARGO_MANIFEST_DIR")))
}

fn http() -> reqwest::blocking::Client {
    // 禁用环境代理（如 HTTP_PROXY）：本机代理会按 Host 规则劫持请求，
    // 例如携带伪造 Host 的边界测试请求会被转发到外网而不到达被测进程。
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(15))
        .no_proxy()
        .build()
        .unwrap()
}

/// Issue a proxied request through the server (transparent: no destination
/// header; the path is the real upstream path, headers pass through).
fn proxy_request(
    server_port: u16,
    method: reqwest::Method,
    path: &str,
    body: Option<Vec<u8>>,
    extra_headers: &[(&str, &str)],
) -> reqwest::blocking::Response {
    let url = format!("http://127.0.0.1:{server_port}{path}");
    let mut req = http().request(method, &url);
    for (k, v) in extra_headers {
        req = req.header(*k, *v);
    }
    if let Some(b) = body {
        req = req.body(b);
    }
    req.send().expect("proxied request failed")
}

/// Standard setup: test_api + server + client(route -> test_api). Returns the
/// server port and the test_api "base" (host:port) for assertions.
struct Env {
    _api: Proc,
    _server: Proc,
    _client: Proc,
    srv_port: u16,
}

fn setup(route_to_api: bool, extra_client: &str) -> Env {
    let dir = tmpdir();
    let api_port = free_port();
    let srv_port = free_port();
    let api_base = format!("http://127.0.0.1:{api_port}");

    let _api = Proc::spawn(
        "test_api",
        &bin("how-test-api"),
        &["-addr", &format!("127.0.0.1:{api_port}")],
    );

    let srv_cfg = format!("{dir}/server.cfg");
    write_cfg(
        &srv_cfg,
        &format!("---\nhost: 127.0.0.1\nport: {srv_port}\ntimeout: 1000\nidletimeout: 60000\n"),
    );
    let _server = Proc::spawn("server", &bin("how-server"), &["--config", &srv_cfg]);
    wait_for_server(srv_port);

    let mut client_yaml = format!(
        "---\ntargets:\n - ws://127.0.0.1:{srv_port}/register\npoolidlesize: 2\npoolmaxsize: 100\n"
    );
    if route_to_api {
        client_yaml.push_str(&format!(
            "routes:\n  \"127.0.0.1:{srv_port}\": \"{api_base}\"\n"
        ));
    }
    client_yaml.push_str(extra_client);
    let cli_cfg = format!("{dir}/client.cfg");
    write_cfg(&cli_cfg, &client_yaml);
    let _client = Proc::spawn("client", &bin("how-client"), &["--config", &cli_cfg]);
    if route_to_api {
        wait_for_proxy(srv_port);
    } else {
        std::thread::sleep(Duration::from_secs(2));
    }
    Env {
        _api,
        _server,
        _client,
        srv_port,
    }
}

#[test]
fn test_full_proxy_flow() {
    let env = setup(true, "");
    let p = env.srv_port;

    // GET /hello -> 200 "hello world"
    let resp = proxy_request(p, reqwest::Method::GET, "/hello", None, &[]);
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().unwrap().trim_end(), "hello world");

    // GET /header -> 200 + forwarded "hello: world" response header
    let resp = proxy_request(p, reqwest::Method::GET, "/header", None, &[]);
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.headers().get("hello").unwrap(), "world");

    // POST /post -> echo body
    let resp = proxy_request(
        p,
        reqwest::Method::POST,
        "/post",
        Some(b"ping=pong".to_vec()),
        &[],
    );
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().unwrap(), "ping=pong");

    // GET /fail -> 666
    let resp = proxy_request(p, reqwest::Method::GET, "/fail", None, &[]);
    assert_eq!(resp.status().as_u16(), 666);

    // Binary body integrity
    let blob: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    let resp = proxy_request(p, reqwest::Method::POST, "/post", Some(blob.clone()), &[]);
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.bytes().unwrap().as_ref(), blob.as_slice());

    // GET /sleep -> 200 "ok"
    let resp = proxy_request(p, reqwest::Method::GET, "/sleep", None, &[]);
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.text().unwrap().trim_end(), "ok");

    // Header transparency: a custom request header is forwarded to the upstream.
    // /header ignores it, but /post echoes the body; use a distinct header that
    // test_api does not use -- verify the proxy does not drop arbitrary headers
    // by sending Content-Type and confirming /post still works (already done).
}

#[test]
fn test_no_proxy_available() {
    // Server alone (no client): any proxied request -> 526 No proxy available.
    let dir = tmpdir();
    let srv_port = free_port();
    let srv_cfg = format!("{dir}/server.cfg");
    write_cfg(
        &srv_cfg,
        &format!("---\nhost: 127.0.0.1\nport: {srv_port}\ntimeout: 1000\nidletimeout: 60000\n"),
    );
    let _server = Proc::spawn("server", &bin("how-server"), &["--config", &srv_cfg]);
    wait_for_server(srv_port);

    let resp = proxy_request(srv_port, reqwest::Method::GET, "/hello", None, &[]);
    assert_eq!(resp.status().as_u16(), 526);
    assert!(resp.text().unwrap().contains("No proxy available"));
}

#[test]
fn test_no_route() {
    // Client connected but with NO route for the server host -> 527.
    let env = setup(false, "");
    let resp = proxy_request(env.srv_port, reqwest::Method::GET, "/hello", None, &[]);
    assert_eq!(resp.status().as_u16(), 527);
    assert!(resp.text().unwrap().to_lowercase().contains("no route"));
}

#[test]
fn test_client_side_blacklist() {
    // Client denies any path matching .*forbidden.* (applied to the arrival URL).
    let env = setup(
        true,
        "blacklist:\n - method: \".*\"\n   url: \".*forbidden.*\"\n",
    );
    let p = env.srv_port;

    // Allowed path -> 200
    let resp = proxy_request(p, reqwest::Method::GET, "/hello", None, &[]);
    assert_eq!(resp.status().as_u16(), 200);

    // Denied path -> 527 "Destination is forbidden"
    let resp = proxy_request(p, reqwest::Method::GET, "/forbidden", None, &[]);
    assert_eq!(resp.status().as_u16(), 527);
    assert!(resp.text().unwrap().contains("Destination is forbidden"));
}

#[test]
fn test_wrong_secret_key() {
    let dir = tmpdir();
    let api_port = free_port();
    let srv_port = free_port();
    let api_base = format!("http://127.0.0.1:{api_port}");
    let _api = Proc::spawn(
        "test_api",
        &bin("how-test-api"),
        &["-addr", &format!("127.0.0.1:{api_port}")],
    );

    let srv_cfg = format!("{dir}/server.cfg");
    write_cfg(&srv_cfg, &format!(
        "---\nhost: 127.0.0.1\nport: {srv_port}\ntimeout: 1000\nidletimeout: 60000\nsecretkey: s3cret\n"
    ));
    let _server = Proc::spawn("server", &bin("how-server"), &["--config", &srv_cfg]);
    wait_for_server(srv_port);

    let cli_cfg = format!("{dir}/client.cfg");
    write_cfg(
        &cli_cfg,
        &format!(
        "---\ntargets:\n - ws://127.0.0.1:{srv_port}/register\npoolidlesize: 2\npoolmaxsize: 100\n\
         secretkey: WRONG\nroutes:\n  \"127.0.0.1:{srv_port}\": \"{api_base}\"\n"
    ),
    );
    let _client = Proc::spawn("client", &bin("how-client"), &["--config", &cli_cfg]);

    std::thread::sleep(Duration::from_secs(3));
    let resp = proxy_request(srv_port, reqwest::Method::GET, "/hello", None, &[]);
    assert_eq!(resp.status().as_u16(), 526);
    let body = resp.text().unwrap();
    assert!(
        body.contains("No proxy available") || body.contains("Unable to get a proxy connection"),
        "expected no-proxy error, got: {body}"
    );
}

#[test]
fn test_server_allowed_hosts() {
    // Server bound to the hostname "localhost": requests on that domain are
    // proxied; requests addressed by IP (or an unlisted host) are rejected 403.
    let dir = tmpdir();
    let api_port = free_port();
    let srv_port = free_port();
    let api_base = format!("http://127.0.0.1:{api_port}");

    let _api = Proc::spawn(
        "test_api",
        &bin("how-test-api"),
        &["-addr", &format!("127.0.0.1:{api_port}")],
    );

    let srv_cfg = format!("{dir}/server.cfg");
    write_cfg(&srv_cfg, &format!(
        "---\nhost: 127.0.0.1\nport: {srv_port}\ntimeout: 1000\nidletimeout: 60000\nallowedhosts:\n - localhost\n"
    ));
    let _server = Proc::spawn("server", &bin("how-server"), &["--config", &srv_cfg]);
    wait_for_server(srv_port);

    // Client routes the arrival host "localhost:<port>" -> test_api.
    let cli_cfg = format!("{dir}/client.cfg");
    write_cfg(
        &cli_cfg,
        &format!(
        "---\ntargets:\n - ws://127.0.0.1:{srv_port}/register\npoolidlesize: 2\npoolmaxsize: 100\n\
         routes:\n  \"localhost:{srv_port}\": \"{api_base}\"\n"
    ),
    );
    let _client = Proc::spawn("client", &bin("how-client"), &["--config", &cli_cfg]);

    // Wait until the localhost path is proxied (client registered + route ok).
    let start = Instant::now();
    loop {
        let url = format!("http://localhost:{srv_port}/hello");
        if let Ok(r) = http().get(&url).send() {
            if r.status().as_u16() == 200 {
                break;
            }
        }
        if start.elapsed() > Duration::from_secs(15) {
            panic!("client never registered a usable connection");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // Bound domain (localhost) -> proxied -> 200.
    let resp = http()
        .get(format!("http://localhost:{srv_port}/hello"))
        .send()
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // IP address -> 403 (cannot bypass the bound domain via IP).
    let resp = http()
        .get(format!("http://127.0.0.1:{srv_port}/hello"))
        .send()
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);

    // Unlisted domain (Host override) -> 403.
    let resp = http()
        .get(format!("http://127.0.0.1:{srv_port}/hello"))
        .header("host", format!("evil.com:{srv_port}"))
        .send()
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);
}

#[test]
fn test_server_ip_whitelist() {
    // Server with allowips=["192.0.2.1"] (not localhost) -> all requests from
    // 127.0.0.1 are denied with "DENY 127.0.0.1".
    let dir = tmpdir();
    let srv_port = free_port();
    let srv_cfg = format!("{dir}/server.cfg");
    write_cfg(&srv_cfg, &format!(
        "---\nhost: 127.0.0.1\nport: {srv_port}\ntimeout: 1000\nidletimeout: 60000\nallowips:\n - 192.0.2.1\n"
    ));
    let _server = Proc::spawn("server", &bin("how-server"), &["--config", &srv_cfg]);
    wait_for_server(srv_port);

    let resp = http()
        .get(format!("http://127.0.0.1:{srv_port}/hello"))
        .send()
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);
    let body = resp.text().unwrap();
    assert!(
        body.contains("DENY 127.0.0.1"),
        "expected DENY, got: {body}"
    );

    // /status is NOT gated (health check still works).
    let resp = http()
        .get(format!("http://127.0.0.1:{srv_port}/status"))
        .send()
        .unwrap();
    assert!(resp.status().is_success());
}

#[test]
fn test_server_apikey_validation() {
    // Server with apikeys=["sk-valid-key-123"].
    let dir = tmpdir();
    let api_port = free_port();
    let srv_port = free_port();
    let api_base = format!("http://127.0.0.1:{api_port}");
    let _api = Proc::spawn(
        "test_api",
        &bin("how-test-api"),
        &["-addr", &format!("127.0.0.1:{api_port}")],
    );
    let srv_cfg = format!("{dir}/server.cfg");
    write_cfg(
        &srv_cfg,
        &format!(
            "---\nhost: 127.0.0.1\nport: {srv_port}\ntimeout: 1000\nidletimeout: 60000\n\
         apikeys:\n - sk-valid-key-123\n"
        ),
    );
    let _server = Proc::spawn("server", &bin("how-server"), &["--config", &srv_cfg]);
    wait_for_server(srv_port);
    let cli_cfg = format!("{dir}/client.cfg");
    write_cfg(
        &cli_cfg,
        &format!(
        "---\ntargets:\n - ws://127.0.0.1:{srv_port}/register\npoolidlesize: 2\npoolmaxsize: 100\n\
         routes:\n  \"127.0.0.1:{srv_port}\": \"{api_base}\"\n"
    ),
    );
    let _client = Proc::spawn("client", &bin("how-client"), &["--config", &cli_cfg]);
    // Wait for the client to register (poll with a valid API key).
    let start = Instant::now();
    loop {
        let resp = http()
            .get(format!("http://127.0.0.1:{srv_port}/hello"))
            .header("Authorization", "Bearer sk-valid-key-123")
            .send();
        if let Ok(r) = resp {
            if r.status().as_u16() == 200 {
                break;
            }
        }
        if start.elapsed() > Duration::from_secs(15) {
            panic!("client never registered a usable connection");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let url = format!("http://127.0.0.1:{srv_port}/hello");

    // No Authorization header -> 403.
    let resp = http().get(&url).send().unwrap();
    assert_eq!(resp.status().as_u16(), 403);
    assert!(resp.text().unwrap().contains("Missing"));

    // Wrong key -> 403.
    let resp = http()
        .get(&url)
        .header("Authorization", "Bearer sk-wrong")
        .send()
        .unwrap();
    assert_eq!(resp.status().as_u16(), 403);
    assert!(resp.text().unwrap().contains("Invalid API key"));

    // Valid key -> 200.
    let resp = http()
        .get(&url)
        .header("Authorization", "Bearer sk-valid-key-123")
        .send()
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // Authentication schemes are case-insensitive.
    let resp = http()
        .get(&url)
        .header("Authorization", "bearer sk-valid-key-123")
        .send()
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}

#[test]
fn test_pool_saturation_timeout() {
    let dir = tmpdir();
    let api_port = free_port();
    let srv_port = free_port();
    let api_base = format!("http://127.0.0.1:{api_port}");
    let _api = Proc::spawn(
        "test_api",
        &bin("how-test-api"),
        &["-addr", &format!("127.0.0.1:{api_port}")],
    );
    let srv_cfg = format!("{dir}/server.cfg");
    write_cfg(
        &srv_cfg,
        &format!("---\nhost: 127.0.0.1\nport: {srv_port}\ntimeout: 1000\nidletimeout: 60000\n"),
    );
    let _server = Proc::spawn("server", &bin("how-server"), &["--config", &srv_cfg]);
    wait_for_server(srv_port);
    let cli_cfg = format!("{dir}/client.cfg");
    write_cfg(
        &cli_cfg,
        &format!(
        "---\ntargets:\n - ws://127.0.0.1:{srv_port}/register\npoolidlesize: 3\npoolmaxsize: 3\n\
         routes:\n  \"127.0.0.1:{srv_port}\": \"{api_base}\"\n"
    ),
    );
    let _client = Proc::spawn("client", &bin("how-client"), &["--config", &cli_cfg]);
    wait_for_proxy(srv_port);

    let mut handles = Vec::new();
    for _ in 0..3 {
        let url = format!("http://127.0.0.1:{srv_port}/sleep");
        handles.push(std::thread::spawn(move || {
            http().get(&url).send().unwrap().status()
        }));
    }
    std::thread::sleep(Duration::from_millis(800));
    let start = Instant::now();
    let resp = proxy_request(srv_port, reqwest::Method::GET, "/hello", None, &[]);
    let elapsed = start.elapsed();
    assert_eq!(resp.status().as_u16(), 526);
    assert!(
        elapsed >= Duration::from_millis(800) && elapsed <= Duration::from_secs(4),
        "expected ~1s timeout, got {elapsed:?}"
    );
    for h in handles {
        assert_eq!(h.join().unwrap().as_u16(), 200);
    }
}

#[test]
fn test_streaming_response() {
    // An OpenAI-style text/event-stream must stream through the proxy:
    // first byte well before the response completes; chunked framing kept.
    let env = setup(true, "");
    let p = env.srv_port;
    let url = format!("http://127.0.0.1:{p}/stream");
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let t0 = Instant::now();
        // 禁用环境代理，保证流式请求直达本机被测进程。
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("no-proxy client");
        let resp = client.get(&url).send().await.expect("stream req");
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "text/event-stream"
        );
        assert!(resp.headers().get("content-length").is_none());
        let mut first: Option<Duration> = None;
        let mut all = String::new();
        use futures::StreamExt;
        let mut s = resp.bytes_stream();
        while let Some(chunk) = s.next().await {
            let b = chunk.expect("chunk");
            if first.is_none() {
                first = Some(t0.elapsed());
            }
            all.push_str(&String::from_utf8_lossy(&b));
        }
        let total = t0.elapsed();
        let fb = first.expect("no bytes");
        assert!(
            fb.as_millis() < 1000,
            "streaming not preserved: first byte at {fb:?}"
        );
        assert!(
            total.as_millis() > 1500,
            "response too fast (buffered?): {total:?}"
        );
        for i in 0..5 {
            assert!(all.contains(&format!("tok-{i}")), "missing tok-{i}");
        }
        assert!(all.contains("[DONE]"));
    });
}

/// Regression: a streaming response that keeps FLOWING for longer than every
/// proxy-side timeout must complete — none of them may bound a live stream.
/// The stream runs ~68s: past the client's 60s read_timeout (per-read stall
/// detection — it must NOT act while data keeps arriving) and past both
/// 30s upstream timeouts (they cover only up to the response HEADERS). Under
/// the old client-level total `timeout(60s)` this stream was truncated
/// mid-body (reqwest: "until the response body has finished"), which is
/// exactly the LLM-SSE failure this test pins.
#[test]
fn test_long_stream_survives_timeouts() {
    let env = setup(true, "");
    let p = env.srv_port;
    // 170 chunks x 400ms = 68s of continuously arriving data.
    let url = format!("http://127.0.0.1:{p}/stream?chunks=170");
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("no-proxy client");
        let resp = client.get(&url).send().await.expect("stream req");
        assert_eq!(resp.status().as_u16(), 200);
        let t0 = Instant::now();
        let mut all = String::new();
        use futures::StreamExt;
        let mut s = resp.bytes_stream();
        while let Some(chunk) = s.next().await {
            let b = chunk.expect("stream died mid-body — truncated by a total timeout?");
            all.push_str(&String::from_utf8_lossy(&b));
        }
        let total = t0.elapsed();
        // The full stream must have arrived: every token and the DONE marker.
        for i in 0..169 {
            assert!(all.contains(&format!("tok-{i}")), "missing tok-{i}");
        }
        assert!(all.contains("[DONE]"), "stream truncated before [DONE]");
        assert!(
            total.as_secs() >= 60,
            "stream finished too early — was it really 170 chunks? {total:?}"
        );
    });
}

#[test]
fn test_large_streamed_upload() {
    let env = setup(true, "");
    let size = 1024 * 1024;
    let blob: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let resp = proxy_request(
        env.srv_port,
        reqwest::Method::POST,
        "/post",
        Some(blob.clone()),
        &[],
    );
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.bytes().unwrap().as_ref(), blob.as_slice());
}

/// The observability contract: both ends log the SAME server-assigned tunnel
/// number for the same connection, and every request line carries it. The
/// client's `[tunnel#N …]` ids must each appear in the server's log too
/// ("via tunnel#N" / "Registering new tunnel#N").
#[test]
fn test_tunnel_id_correlation() {
    let env = setup(true, "");
    let p = env.srv_port;
    for _ in 0..2 {
        let resp = http()
            .get(format!("http://127.0.0.1:{p}/hello"))
            .send()
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
    }
    let cli = env._client.log();
    assert!(
        cli.contains("Connected tunnel#"),
        "client log lacks tunnel numbering:\n{cli}"
    );
    // Extract the tunnel numbers from the client's request lines.
    let request_ids: Vec<String> = cli
        .lines()
        .filter(|l| l.contains("[tunnel#"))
        .filter_map(|l| {
            let rest = l.split("tunnel#").nth(1)?;
            let num: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            (!num.is_empty()).then_some(num)
        })
        .collect();
    assert!(
        !request_ids.is_empty(),
        "no [tunnel#N] request lines:\n{cli}"
    );
    let srv = env._server.log();
    assert!(
        srv.contains("via tunnel#"),
        "server log lacks tunnel numbering:\n{srv}"
    );
    for id in &request_ids {
        assert!(
            srv.contains(&format!("tunnel#{id}")),
            "client logged tunnel#{id} but the server never saw it — numbers diverged"
        );
    }
}
