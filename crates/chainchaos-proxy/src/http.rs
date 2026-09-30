use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use chainchaos_core::{Fault, FaultConfig, FaultEngine, InjectedFault, RpcRequest};
use tokio::net::TcpListener;
use tracing::{Instrument, debug, info, info_span, warn};
use url::Url;

/// Largest request body accepted from clients. Large enough for big
/// `eth_call` payloads and batches; responses are not limited.
const MAX_REQUEST_BODY: usize = 16 * 1024 * 1024;

/// Response header listing faults injected into a request, e.g. `delay`.
/// Only present when at least one fault fired.
const FAULTS_HEADER: HeaderName = HeaderName::from_static("x-chainchaos-faults");

/// Response header set when chainchaos itself generated the response
/// (as opposed to relaying the upstream's).
const ERROR_HEADER: HeaderName = HeaderName::from_static("x-chainchaos-error");

/// JSON-RPC error code used when the upstream cannot be reached.
/// Within the implementation-defined server error range (-32000..=-32099).
const UPSTREAM_UNAVAILABLE_CODE: i64 = -32000;

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub upstream: Url,
    pub upstream_timeout: Duration,
    pub faults: FaultConfig,
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("unsupported upstream URL scheme `{0}` (expected http or https)")]
    UnsupportedScheme(String),
    #[error("failed to build HTTP client: {0}")]
    Client(#[source] reqwest::Error),
    #[error("server error: {0}")]
    Serve(#[source] std::io::Error),
}

#[derive(Debug)]
struct AppState {
    upstream: Url,
    client: reqwest::Client,
    engine: FaultEngine,
    requests: AtomicU64,
}

/// Builds the proxy router, validating the configuration.
///
/// Call this before binding a listener so configuration errors surface
/// before the proxy announces itself.
pub fn router(config: ProxyConfig) -> Result<Router, ProxyError> {
    let scheme = config.upstream.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(ProxyError::UnsupportedScheme(scheme.to_owned()));
    }
    let client = reqwest::Client::builder()
        .timeout(config.upstream_timeout)
        .build()
        .map_err(ProxyError::Client)?;
    let state = Arc::new(AppState {
        upstream: config.upstream,
        client,
        engine: FaultEngine::new(config.faults),
        requests: AtomicU64::new(0),
    });
    Ok(Router::new()
        .route("/", post(handle))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY))
        .with_state(state))
}

/// Serves `app` (from [`router`]) on `listener` until `shutdown` resolves,
/// then drains in-flight requests before returning.
pub async fn serve(
    listener: TcpListener,
    app: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), ProxyError> {
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(ProxyError::Serve)
}

async fn handle(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    let seq = state.requests.fetch_add(1, Ordering::Relaxed) + 1;
    let request = RpcRequest::parse(&body);
    let methods = request
        .as_ref()
        .map_or_else(|| "<unparsed>".to_owned(), RpcRequest::methods_summary);
    let span = info_span!("rpc", seq, method = %methods, id = tracing::field::Empty);
    if let Some(req) = request.as_ref().filter(|r| !r.batch) {
        span.record("id", tracing::field::display(req.error_id()));
    }

    async move {
        let started = Instant::now();
        let faults = state.engine.plan(request.as_ref());
        for injected in &faults {
            apply_pre_forward(injected).await;
        }

        // TODO(phase-1): post-forward faults (receipt_null,
        // transaction_submit_timeout) run here, once the upstream has answered.
        let mut response = match forward(&state, &headers, body).await {
            Ok(response) => response,
            Err(err) => {
                warn!(error = %err, "upstream request failed");
                upstream_error_response(request.as_ref(), &err)
            }
        };

        if !faults.is_empty() {
            let names: Vec<&str> = faults.iter().map(|f| f.fault.name()).collect();
            if let Ok(value) = HeaderValue::from_str(&names.join(",")) {
                response.headers_mut().insert(FAULTS_HEADER, value);
            }
        }
        let status = response.status().as_u16();
        let elapsed_ms = started.elapsed().as_millis() as u64;
        // Untouched requests are only interesting when debugging; requests
        // that received a fault are always worth a line.
        if faults.is_empty() {
            debug!(status, elapsed_ms, "request completed");
        } else {
            info!(
                status,
                elapsed_ms,
                faults = faults.len(),
                "request completed"
            );
        }
        response
    }
    .instrument(span)
    .await
}

/// Realises faults that act before the request reaches the upstream.
async fn apply_pre_forward(injected: &InjectedFault) {
    match &injected.fault {
        Fault::Delay { duration } => {
            info!(
                fault = "delay",
                rule = injected.rule,
                delay_ms = duration.as_millis() as u64,
                "injecting fault"
            );
            tokio::time::sleep(*duration).await;
        }
    }
}

async fn forward(
    state: &AppState,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response, reqwest::Error> {
    let upstream = state
        .client
        .post(state.upstream.clone())
        .headers(forwardable_headers(headers))
        .body(body)
        .send()
        .await?;

    let status = upstream.status();
    let response_headers = forwardable_headers(upstream.headers());
    // Buffer the full body: later phases need to inspect and rewrite it.
    // TODO(phase-3): response mutation must handle a `content-encoding`
    // passed through from the upstream (we currently relay it verbatim).
    let bytes = upstream.bytes().await?;

    let mut response = (status, bytes).into_response();
    *response.headers_mut() = response_headers;
    Ok(response)
}

/// Copies end-to-end headers, dropping hop-by-hop headers and those the HTTP
/// stack recomputes for the new connection.
fn forwardable_headers(headers: &HeaderMap) -> HeaderMap {
    const SKIP: &[HeaderName] = &[
        header::HOST,
        header::CONNECTION,
        header::CONTENT_LENGTH,
        header::TRANSFER_ENCODING,
        header::TE,
        header::TRAILER,
        header::UPGRADE,
        header::PROXY_AUTHORIZATION,
        header::PROXY_AUTHENTICATE,
    ];
    headers
        .iter()
        .filter(|(name, _)| !SKIP.contains(name) && name.as_str() != "keep-alive")
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// A JSON-RPC error produced by chainchaos when the upstream is unreachable,
/// so clients see a well-formed response rather than a bare connection error.
fn upstream_error_response(request: Option<&RpcRequest>, err: &reqwest::Error) -> Response {
    let (status, reason) = if err.is_timeout() {
        (StatusCode::GATEWAY_TIMEOUT, "upstream timed out")
    } else {
        (StatusCode::BAD_GATEWAY, "upstream request failed")
    };
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": request.map_or(serde_json::Value::Null, RpcRequest::error_id),
        "error": {
            "code": UPSTREAM_UNAVAILABLE_CODE,
            "message": format!("chainchaos: {reason}: {err}"),
        },
    });
    let mut response = (status, axum::Json(body)).into_response();
    response
        .headers_mut()
        .insert(ERROR_HEADER, HeaderValue::from_static("upstream"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drops_hop_by_hop_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("localhost"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("close"));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer x"));
        let out = forwardable_headers(&headers);
        assert_eq!(out.len(), 2);
        assert!(out.contains_key(header::CONTENT_TYPE));
        assert!(out.contains_key(header::AUTHORIZATION));
    }

    #[test]
    fn rejects_non_http_upstream() {
        let config = ProxyConfig {
            upstream: Url::parse("ws://127.0.0.1:8546").unwrap(),
            upstream_timeout: Duration::from_secs(1),
            faults: FaultConfig::default(),
        };
        assert!(matches!(
            router(config),
            Err(ProxyError::UnsupportedScheme(_))
        ));
    }
}
