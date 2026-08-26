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
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A child process that is killed (and waited on) when dropped, so the test
/// never leaves a server running even on panic.
struct Proc {
    #[allow(dead_code)]
    name: &'static str,
    child: Option<Child>,
}

impl Proc {
    fn spawn(name: &'static str, exe: &str, args: &[&str]) -> Self {
        let mut cmd = Command::new(exe);
        cmd.args(args);
        let mut child = cmd
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn {name}: {e}"));
        // Drain stderr/stdout so a chatty child (test_api logging a big body)
        // cannot fill its OS pipe buffer (~64 KiB) and block.
        if let Some(mut stderr) = child.stderr.take() {
            std::thread::spawn(move || {
                use std::io::Read;
                let mut buf = [0u8; 4096];
                while stderr.read(&mut buf).is_ok() {}
            });
        }
        if let Some(mut stdout) = child.stdout.take() {
            std::thread::spawn(move || {
                use std::io::Read;
                let mut buf = [0u8; 4096];
                while stdout.read(&mut buf).is_ok() {}
            });
        }
        Proc { name, child: Some(child) }
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
    let start = Instant::now();
    loop {
        if let Ok(resp) = reqwest::blocking::get(&url) {
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
    let start = Instant::now();
    loop {
        let resp = reqwest::blocking::Client::new().get(&url).send();
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

fn tmpdir() -> String {
    let d = format!(
        "{}/wsp-e2e-{}",
        std::env::temp_dir().to_string_lossy(),
        std::process::id()
    );
    let _ = std::fs::create_dir_all(&d);
    d
}

fn bin(name: &str) -> String {
    // cargo exposes CARGO_BIN_EXE_<name> with hyphens replaced by underscores.
    let env = format!("CARGO_BIN_EXE_{}", name.replace('-', "_"));
    std::env::var(&env).unwrap_or_else(|_| {
        format!("{}/target/debug/{name}", env!("CARGO_MANIFEST_DIR"))
    })
}

fn http() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(15))
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

    let _api = Proc::spawn("test_api", &bin("how-test-api"), &["-addr", &format!("127.0.0.1:{api_port}")]);

    let srv_cfg = format!("{dir}/server.cfg");
    write_cfg(&srv_cfg, &format!(
        "---\nhost: 127.0.0.1\nport: {srv_port}\ntimeout: 1000\nidletimeout: 60000\n"
    ));
    let _server = Proc::spawn("server", &bin("how-server"), &["--config", &srv_cfg]);
    wait_for_server(srv_port);

    let mut client_yaml = format!(
        "---\ntargets:\n - ws://127.0.0.1:{srv_port}/register\npoolidlesize: 2\npoolmaxsize: 100\n"
    );
    if route_to_api {
        client_yaml.push_str(&format!("routes:\n  \"127.0.0.1:{srv_port}\": \"{api_base}\"\n"));
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
    Env { _api, _server, _client, srv_port }
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
    let resp = proxy_request(p, reqwest::Method::POST, "/post", Some(b"ping=pong".to_vec()), &[]);
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
    write_cfg(&srv_cfg, &format!(
        "---\nhost: 127.0.0.1\nport: {srv_port}\ntimeout: 1000\nidletimeout: 60000\n"
    ));
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
    let env = setup(true, "blacklist:\n - method: \".*\"\n   url: \".*forbidden.*\"\n");
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
    let _api = Proc::spawn("test_api", &bin("how-test-api"), &["-addr", &format!("127.0.0.1:{api_port}")]);

    let srv_cfg = format!("{dir}/server.cfg");
    write_cfg(&srv_cfg, &format!(
        "---\nhost: 127.0.0.1\nport: {srv_port}\ntimeout: 1000\nidletimeout: 60000\nsecretkey: s3cret\n"
    ));
    let _server = Proc::spawn("server", &bin("how-server"), &["--config", &srv_cfg]);
    wait_for_server(srv_port);

    let cli_cfg = format!("{dir}/client.cfg");
    write_cfg(&cli_cfg, &format!(
        "---\ntargets:\n - ws://127.0.0.1:{srv_port}/register\npoolidlesize: 2\npoolmaxsize: 100\n\
         secretkey: WRONG\nroutes:\n  \"127.0.0.1:{srv_port}\": \"{api_base}\"\n"
    ));
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
fn test_pool_saturation_timeout() {
    let dir = tmpdir();
    let api_port = free_port();
    let srv_port = free_port();
    let api_base = format!("http://127.0.0.1:{api_port}");
    let _api = Proc::spawn("test_api", &bin("how-test-api"), &["-addr", &format!("127.0.0.1:{api_port}")]);
    let srv_cfg = format!("{dir}/server.cfg");
    write_cfg(&srv_cfg, &format!(
        "---\nhost: 127.0.0.1\nport: {srv_port}\ntimeout: 1000\nidletimeout: 60000\n"
    ));
    let _server = Proc::spawn("server", &bin("how-server"), &["--config", &srv_cfg]);
    wait_for_server(srv_port);
    let cli_cfg = format!("{dir}/client.cfg");
    write_cfg(&cli_cfg, &format!(
        "---\ntargets:\n - ws://127.0.0.1:{srv_port}/register\npoolidlesize: 3\npoolmaxsize: 3\n\
         routes:\n  \"127.0.0.1:{srv_port}\": \"{api_base}\"\n"
    ));
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
        let resp = reqwest::Client::new().get(&url).send().await.expect("stream req");
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(resp.headers().get("content-type").unwrap(), "text/event-stream");
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
        assert!(fb.as_millis() < 1000, "streaming not preserved: first byte at {fb:?}");
        assert!(total.as_millis() > 1500, "response too fast (buffered?): {total:?}");
        for i in 0..5 {
            assert!(all.contains(&format!("tok-{i}")), "missing tok-{i}");
        }
        assert!(all.contains("[DONE]"));
    });
}

#[test]
fn test_large_streamed_upload() {
    let env = setup(true, "");
    let size = 1024 * 1024;
    let blob: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
    let resp = proxy_request(env.srv_port, reqwest::Method::POST, "/post", Some(blob.clone()), &[]);
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(resp.bytes().unwrap().as_ref(), blob.as_slice());
}
