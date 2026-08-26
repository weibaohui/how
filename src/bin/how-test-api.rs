//! A small HTTP API used for end-to-end testing (port of `test_api/test_api.go`)
//! plus an SSE streaming endpoint to exercise streaming-proxy behavior.

use bytes::Bytes;
use futures::stream;
use http_body::Frame;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;

// For testing purpose. Mirrors Go's `flag.String("addr", "localhost:8081", ...)`.
fn addr() -> String {
    how::cli::string_flag("addr", "localhost:8081")
}

type Boxed = http_body_util::combinators::BoxBody<Bytes, Infallible>;

fn boxed(b: Full<Bytes>) -> Boxed {
    b.boxed()
}

async fn hello(_req: Request<Incoming>) -> Result<Response<Boxed>, Infallible> {
    how::log::log_plain("hello");
    Ok(Response::new(boxed(Full::new(Bytes::from_static(b"hello world\n")))))
}

async fn header(_req: Request<Incoming>) -> Result<Response<Boxed>, Infallible> {
    how::log::log_plain("header");
    let mut resp = Response::new(boxed(Full::new(Bytes::from_static(
        b"hello world in header\n",
    ))));
    resp.headers_mut().insert("hello", "world".parse().unwrap());
    Ok(resp)
}

async fn post(req: Request<Incoming>) -> Result<Response<Boxed>, Infallible> {
    let body = req.into_body().collect().await.unwrap().to_bytes();
    how::log::log_plain("post");
    how::log::log_plain(String::from_utf8_lossy(&body).to_string());
    Ok(Response::new(boxed(Full::new(body))))
}

async fn fail(_req: Request<Incoming>) -> Result<Response<Boxed>, Infallible> {
    Ok(Response::builder()
        .status(StatusCode::from_u16(666).unwrap())
        .body(boxed(Full::new(Bytes::from_static(b"GO FUNK YOURSELF\n"))))
        .unwrap())
}

async fn sleep(_req: Request<Incoming>) -> Result<Response<Boxed>, Infallible> {
    tokio::time::sleep(Duration::from_secs(10)).await;
    Ok(Response::new(boxed(Full::new(Bytes::from_static(b"ok")))))
}

/// An SSE streaming endpoint that mimics an OpenAI-style `text/event-stream`
/// response: it emits a `data:` chunk every 400ms and a final `[DONE]`.
async fn stream(_req: Request<Incoming>) -> Result<Response<Boxed>, Infallible> {
    let s = stream::unfold(0u32, |i| async move {
        if i < 6 {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let chunk = if i < 5 {
                format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"tok-{i}\"}}}}]}}\n\n")
            } else {
                "data: [DONE]\n\n".to_string()
            };
            Some((Ok::<_, Infallible>(Frame::data(Bytes::from(chunk))), i + 1))
        } else {
            None
        }
    });
    let body = StreamBody::new(s).boxed();
    let mut resp = Response::new(body);
    resp.headers_mut()
        .insert("content-type", "text/event-stream".parse().unwrap());
    resp.headers_mut()
        .insert("cache-control", "no-cache".parse().unwrap());
    Ok(resp)
}

async fn handle(req: Request<Incoming>) -> Result<Response<Boxed>, Infallible> {
    match req.uri().path() {
        "/hello" => hello(req).await,
        "/header" => header(req).await,
        "/fail" => fail(req).await,
        "/post" => post(req).await,
        "/sleep" => sleep(req).await,
        "/stream" => stream(req).await,
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(boxed(Full::new(Bytes::from_static(b"not found\n"))))
            .unwrap()),
    }
}

#[tokio::main]
async fn main() {
    let addr: SocketAddr = addr()
        .parse()
        .unwrap_or_else(|e| panic!("invalid address : {}", e));
    let listener = TcpListener::bind(addr).await.expect("bind failed");
    how::log::log_plain(format!("test_api listening on {}", addr));
    loop {
        let (stream, _) = listener.accept().await.expect("accept failed");
        let _ = stream.set_nodelay(true);
        let io = TokioIo::new(stream);
        tokio::spawn(async move {
            let _ = http1::Builder::new()
                .serve_connection(io, service_fn(handle))
                .await;
        });
    }
}
