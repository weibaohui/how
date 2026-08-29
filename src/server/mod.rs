//! Server-side WSP: reverse HTTP proxy over WebSockets.

pub mod config;
pub mod connection;
pub mod pool;
// `server::server` shadows the parent module name; the file split mirrors the
// client side (config/connection/pool next to the main type), so keep it.
#[allow(clippy::module_inception)]
pub mod server;

pub use config::{load_configuration, new_config, Config};
pub use server::Server;
