//! HTTP JSON-RPC transport for the shared fault [`Pipeline`].
//!
//! The original request bytes are forwarded unless a fault needs to rewrite
//! them, and the upstream's bytes are returned unless a fault changes them.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use chainchaos_core::{InjectedFault, RpcRequest, Tick};
use serde_json::Value;
use tracing::{Instrument, debug, info, info_span, warn};

use crate::AppState;
use crate::pipeline::{CHAINCHAOS_ERROR_CODE, Early, Pipeline, RATE_LIMIT_CODE, json_error};
use crate::upstream::{ERROR_HEADER, UpstreamError, forwardable_headers};

/// Response header listing faults injected into a request, e.g. `delay`.
/// Only present when at least one fault fired.
const FAULTS_HEADER: HeaderName = HeaderName::from_static("x-chainchaos-faults");

pub(crate) async fn handle(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let seq = state.requests.fetch_add(1, Ordering::Relaxed) + 1;
    let tick = Tick {
        seq,
        elapsed: state.started.elapsed(),
    };
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
        let mut faults = state.engine.plan(request.as_ref(), tick);
        let pipeline = Pipeline {
            state: &state,
            request: request.as_ref(),
            tick,
        };
        state.metrics.faults_planned(&faults);
        let mut response = run(&pipeline, headers, body, &mut faults).await;

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

/// Runs the shared pipeline with HTTP forwarding and HTTP responses.
async fn run(
    pipeline: &Pipeline<'_>,
    headers: HeaderMap,
    body: Bytes,
    faults: &mut [InjectedFault],
) -> Response {
    let request = pipeline.request;
    if let Some(early) = pipeline.before_forward(faults).await {
        return match early {
            Early::HttpError {
                status,
                body,
                retry_after,
            } => http_error_response(request, status, body.as_deref(), retry_after),
            Early::Timeout { duration } => {
                tokio::time::sleep(duration).await;
                timeout_response(request)
            }
        };
    }

    let head = pipeline.head_for_lag(faults).await;
    let forward_body = pipeline.rewrite_request(&body, faults, head);

    let mutate = pipeline.mutates(faults);
    let mut forward_headers = forwardable_headers(&headers);
    if mutate {
        // Response mutation needs a plain body.
        forward_headers.insert(
            header::ACCEPT_ENCODING,
            HeaderValue::from_static("identity"),
        );
    }
    let state = pipeline.state;
    let sent_at = Instant::now();
    let upstream = match state.upstream.send(forward_headers, forward_body).await {
        Ok(response) => response,
        Err(err) => {
            warn!(error = %err, "upstream request failed");
            state.metrics.upstream_error();
            return upstream_error_response(request, &err);
        }
    };
    if let Some(recorder) = &state.recorder {
        recorder.record(
            pipeline.tick.seq,
            pipeline.tick.elapsed,
            sent_at.elapsed(),
            &body,
            upstream.status.as_u16(),
            &upstream.body,
        );
    }

    if let Some((f, duration)) = Pipeline::hold_after_forward(faults) {
        Pipeline::log_hold(f, duration, &upstream.body);
        tokio::time::sleep(duration).await;
        return timeout_response(request);
    }

    let response_body = if mutate && upstream.status.is_success() {
        pipeline
            .mutate_response(&upstream.body, faults, head)
            .unwrap_or(upstream.body)
    } else {
        upstream.body
    };
    let mut response = (upstream.status, response_body).into_response();
    *response.headers_mut() = upstream.headers;
    response
}

fn chainchaos_response(status: StatusCode, body: Value, marker: &'static str) -> Response {
    let mut response = (status, axum::Json(body)).into_response();
    response
        .headers_mut()
        .insert(ERROR_HEADER, HeaderValue::from_static(marker));
    response
}

/// What the client sees when an injected timeout finally ends (most clients
/// will have given up before this).
fn timeout_response(request: Option<&RpcRequest>) -> Response {
    let body = json_error(
        request,
        CHAINCHAOS_ERROR_CODE,
        "chainchaos: injected timeout".into(),
    );
    chainchaos_response(StatusCode::GATEWAY_TIMEOUT, body, "injected")
}

fn http_error_response(
    request: Option<&RpcRequest>,
    status: u16,
    body: Option<&str>,
    retry_after: Option<u64>,
) -> Response {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = match body {
        Some(text) => {
            let content_type = if serde_json::from_str::<Value>(text).is_ok() {
                "application/json"
            } else {
                "text/plain; charset=utf-8"
            };
            (
                status,
                [(header::CONTENT_TYPE, content_type)],
                text.to_owned(),
            )
                .into_response()
        }
        None => {
            let code = if status == StatusCode::TOO_MANY_REQUESTS {
                RATE_LIMIT_CODE
            } else {
                -32603
            };
            let message = format!("chainchaos: injected HTTP {}", status.as_u16());
            (status, axum::Json(json_error(request, code, message))).into_response()
        }
    };
    if let Some(seconds) = retry_after {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
    }
    response
        .headers_mut()
        .insert(ERROR_HEADER, HeaderValue::from_static("injected"));
    response
}

/// A JSON-RPC error produced by chainchaos when the upstream is unreachable,
/// so clients see a well-formed response rather than a bare connection error.
fn upstream_error_response(request: Option<&RpcRequest>, err: &UpstreamError) -> Response {
    let status = match err {
        UpstreamError::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
        UpstreamError::Failed(_) => StatusCode::BAD_GATEWAY,
    };
    let body = json_error(request, CHAINCHAOS_ERROR_CODE, format!("chainchaos: {err}"));
    chainchaos_response(status, body, "upstream")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_error_defaults_to_json_rpc_body() {
        let req = RpcRequest::parse(br#"{"jsonrpc":"2.0","id":4,"method":"eth_call"}"#);
        let response = http_error_response(req.as_ref(), 429, None, Some(3));
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "3");
        assert_eq!(response.headers()[ERROR_HEADER], "injected");
    }
}
