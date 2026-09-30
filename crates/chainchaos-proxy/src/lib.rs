//! The chainchaos JSON-RPC proxy.
//!
//! Requests are forwarded byte-for-byte to the upstream (a live endpoint or a
//! recording) and responses are returned unchanged, unless a configured
//! fault says otherwise. WebSocket connections on the same address are
//! proxied to a WebSocket upstream.

mod chain;
mod http;
mod record;
mod upstream;
mod ws;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::post;
use chainchaos_core::recording::{Recording, RecordingError, ReplayIndex};
use chainchaos_core::{FaultConfig, FaultEngine};
use tokio::net::TcpListener;
use tokio::time::Instant;
use url::Url;

pub use record::RecordConfig;

use chain::ChainState;
use record::Recorder;
use upstream::Upstream;

/// Largest request body accepted from clients. Large enough for big
/// `eth_call` payloads and batches; responses are not limited.
const MAX_REQUEST_BODY: usize = 16 * 1024 * 1024;

/// Where requests are served from.
#[derive(Debug, Clone)]
pub enum UpstreamConfig {
    /// A live Ethereum-compatible JSON-RPC endpoint.
    Http { url: Url, timeout: Duration },
    /// Recorded responses (`chainchaos replay`).
    Replay {
        recording: Recording,
        /// Reproduce each response's recorded upstream latency.
        replay_latency: bool,
    },
}

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub upstream: UpstreamConfig,
    /// WebSocket upstream for `eth_subscribe` traffic. WebSocket clients are
    /// rejected when unset.
    pub upstream_ws: Option<Url>,
    pub faults: FaultConfig,
    /// Record traffic to a file (`chainchaos record`).
    pub record: Option<RecordConfig>,
}

impl ProxyConfig {
    /// A transparent HTTP proxy with default settings, handy for tests.
    pub fn http(upstream: Url) -> Self {
        Self {
            upstream: UpstreamConfig::Http {
                url: upstream,
                timeout: Duration::from_secs(30),
            },
            upstream_ws: None,
            faults: FaultConfig::default(),
            record: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("unsupported upstream URL scheme `{0}` (expected http or https)")]
    UnsupportedScheme(String),
    #[error("unsupported WebSocket upstream URL scheme `{0}` (expected ws or wss)")]
    UnsupportedWsScheme(String),
    #[error("failed to build HTTP client: {0}")]
    Client(#[source] reqwest::Error),
    #[error(transparent)]
    Recording(#[from] RecordingError),
    #[error("server error: {0}")]
    Serve(#[source] std::io::Error),
}

#[derive(Debug)]
struct AppState {
    upstream: Upstream,
    upstream_ws: Option<Url>,
    engine: FaultEngine,
    chain: ChainState,
    recorder: Option<Recorder>,
    started: Instant,
    requests: AtomicU64,
    ws_messages: AtomicU64,
    ws_connections: AtomicU64,
}

/// A configured proxy, ready to serve.
#[derive(Debug, Clone)]
pub struct Proxy {
    state: Arc<AppState>,
}

impl Proxy {
    /// Validates the configuration and prepares the proxy. Call this before
    /// binding a listener so configuration errors surface first. Scenario
    /// time (`after: 10s`) is measured from this call.
    pub fn new(config: ProxyConfig) -> Result<Self, ProxyError> {
        let upstream = match config.upstream {
            UpstreamConfig::Http { url, timeout } => {
                let scheme = url.scheme();
                if scheme != "http" && scheme != "https" {
                    return Err(ProxyError::UnsupportedScheme(scheme.to_owned()));
                }
                let client = reqwest::Client::builder()
                    .timeout(timeout)
                    .build()
                    .map_err(ProxyError::Client)?;
                Upstream::Http { client, url }
            }
            UpstreamConfig::Replay {
                recording,
                replay_latency,
            } => Upstream::Replay {
                index: ReplayIndex::new(recording),
                latency: replay_latency,
            },
        };
        if let Some(ws) = &config.upstream_ws {
            if ws.scheme() != "ws" && ws.scheme() != "wss" {
                return Err(ProxyError::UnsupportedWsScheme(ws.scheme().to_owned()));
            }
        }
        let recorder = config.record.as_ref().map(Recorder::create).transpose()?;
        let seed = config.faults.seed;
        Ok(Self {
            state: Arc::new(AppState {
                upstream,
                upstream_ws: config.upstream_ws,
                engine: FaultEngine::new(config.faults),
                chain: ChainState::new(seed),
                recorder,
                started: Instant::now(),
                requests: AtomicU64::new(0),
                ws_messages: AtomicU64::new(0),
                ws_connections: AtomicU64::new(0),
            }),
        })
    }

    /// The axum router: `POST /` for JSON-RPC, `GET /` for WebSocket.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/", post(http::handle).get(ws::upgrade))
            .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY))
            .with_state(self.state.clone())
    }

    /// Serves on `listener` until `shutdown` resolves, then drains in-flight
    /// HTTP requests before returning.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ProxyError> {
        self.spawn_scenario_timer();
        axum::serve(listener, self.router())
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(ProxyError::Serve)
    }

    /// Announces time-based scenario transitions as they happen, even when
    /// no traffic is flowing.
    fn spawn_scenario_timer(&self) {
        let transitions = self.state.engine.time_transitions();
        if transitions.is_empty() {
            return;
        }
        let state = self.state.clone();
        tokio::spawn(async move {
            for at in transitions {
                tokio::time::sleep_until(state.started + at).await;
                state.engine.announce_time_transitions(at);
            }
        });
    }
}
