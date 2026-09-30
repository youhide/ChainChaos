//! The chainchaos HTTP JSON-RPC proxy.
//!
//! Requests are forwarded byte-for-byte to a single upstream endpoint and the
//! upstream response is returned unchanged, unless a configured fault says
//! otherwise.

mod http;

pub use http::{ProxyConfig, ProxyError, router, serve};
