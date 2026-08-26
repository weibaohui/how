//! WSP - Reverse HTTP proxy over WebSockets.
//!
//! Rust port of [github.com/root-gg/wsp](https://github.com/root-gg/wsp).
//!
//! A WSP client runs inside an internal network (alongside the APIs) and
//! connects to a remote WSP server with HTTP websockets. Clients issue HTTP
//! requests to the WSP server `/request` endpoint with an extra
//! `X-PROXY-DESTINATION` header. The server forwards the request to a WSP
//! client over one of the offered websockets; the client executes the HTTP
//! request locally and streams the response back. No buffering of any sort
//! is intended.

pub mod common;
pub mod log;
pub mod cli;
pub mod server;
pub mod client;
