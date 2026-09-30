//! The HTTP JSON-RPC request pipeline.
//!
//! ```text
//! plan faults ─▶ pre-forward faults ─▶ rewrite request ─▶ forward ─▶ post-forward faults ─▶ respond
//!                (delay, http_error,    (lag views,                  (tx-submit timeout,
//!                 timeout, reorg)        reorg hash                   response mutation,
//!                                        translation)                 reorg view)
//! ```
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
use chainchaos_core::{Fault, InjectedFault, ReorgTransactions, RpcCall, RpcRequest, Tick};
use chainchaos_evm::hex::parse_quantity;
use chainchaos_evm::reorg::Rewrite;
use chainchaos_evm::{lag, logs};
use serde_json::{Value, json};
use tracing::{Instrument, debug, info, info_span, warn};

use crate::AppState;
use crate::upstream::{ERROR_HEADER, UpstreamError, forwardable_headers};

/// Response header listing faults injected into a request, e.g. `delay`.
/// Only present when at least one fault fired.
const FAULTS_HEADER: HeaderName = HeaderName::from_static("x-chainchaos-faults");

/// JSON-RPC error code for errors produced by chainchaos itself (upstream
/// unavailable, injected timeouts). Within the implementation-defined server
/// error range (-32000..=-32099).
const CHAINCHAOS_ERROR_CODE: i64 = -32000;

/// JSON-RPC error code commonly used by providers for rate limiting.
const RATE_LIMIT_CODE: i64 = -32005;

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
        let mut response = pipeline.run(headers, body, &mut faults).await;

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

struct Pipeline<'a> {
    state: &'a AppState,
    request: Option<&'a RpcRequest>,
    tick: Tick,
}

impl Pipeline<'_> {
    async fn run(&self, headers: HeaderMap, body: Bytes, faults: &mut [InjectedFault]) -> Response {
        // 1. Faults that act before (or instead of) forwarding.
        for f in faults.iter() {
            match &f.fault {
                Fault::Delay { duration } => {
                    info!(fault = "delay", rule = %f.label, delay_ms = duration.as_millis() as u64, "injecting fault");
                    tokio::time::sleep(*duration).await;
                }
                Fault::HttpError {
                    status,
                    body,
                    retry_after,
                } => {
                    info!(fault = "http_error", rule = %f.label, status, "injecting fault");
                    return http_error_response(
                        self.request,
                        *status,
                        body.as_deref(),
                        *retry_after,
                    );
                }
                Fault::Timeout {
                    duration,
                    forward: false,
                } => {
                    info!(fault = "timeout", rule = %f.label, forwarded = false, hold_ms = duration.as_millis() as u64, "injecting fault");
                    tokio::time::sleep(*duration).await;
                    return timeout_response(self.request);
                }
                Fault::Reorg {
                    depth,
                    head,
                    transactions,
                } => self.apply_reorg(f, *depth, *head, *transactions).await,
                _ => {}
            }
        }

        // 2. The chain head, when a lag view needs it before forwarding.
        let head = self.head_for_lag(faults).await;

        // 3. Request rewriting (lag views, reorg hash translation).
        let forward_body = self.rewrite_request(&body, faults, head);

        // 4. Forward.
        let mutate =
            faults.iter().any(|f| f.fault.mutates_response()) || self.state.chain.reorg_active();
        let mut forward_headers = forwardable_headers(&headers);
        if mutate {
            // Response mutation needs a plain body.
            forward_headers.insert(
                header::ACCEPT_ENCODING,
                HeaderValue::from_static("identity"),
            );
        }
        let sent_at = Instant::now();
        let upstream = match self
            .state
            .upstream
            .send(forward_headers, forward_body)
            .await
        {
            Ok(response) => response,
            Err(err) => {
                warn!(error = %err, "upstream request failed");
                return upstream_error_response(self.request, &err);
            }
        };
        if let Some(recorder) = &self.state.recorder {
            recorder.record(
                self.tick.seq,
                self.tick.elapsed,
                sent_at.elapsed(),
                &body,
                upstream.status.as_u16(),
                &upstream.body,
            );
        }

        // 5. Ambiguous outcome: the upstream answered, the client times out.
        let hold = faults.iter().find_map(|f| match &f.fault {
            Fault::Timeout {
                duration,
                forward: true,
            }
            | Fault::TransactionSubmitTimeout { duration } => Some((f, *duration)),
            _ => None,
        });
        if let Some((f, duration)) = hold {
            info!(
                fault = f.fault.name(),
                rule = %f.label,
                upstream_status = upstream.status.as_u16(),
                upstream_response = %summarize(&upstream.body),
                hold_ms = duration.as_millis() as u64,
                "injecting fault: request reached the upstream, client will see a timeout"
            );
            tokio::time::sleep(duration).await;
            return timeout_response(self.request);
        }

        // 6. Response mutation.
        let response_body = if mutate && upstream.status.is_success() {
            self.mutate_response(&upstream.body, faults, head)
                .unwrap_or(upstream.body)
        } else {
            upstream.body
        };
        let mut response = (upstream.status, response_body).into_response();
        *response.headers_mut() = upstream.headers;
        response
    }

    async fn apply_reorg(
        &self,
        f: &InjectedFault,
        depth: u64,
        head: Option<u64>,
        transactions: ReorgTransactions,
    ) {
        let head = match head {
            Some(head) => head,
            None => match self.fetch_head().await {
                Some(head) => head,
                None => {
                    warn!(fault = "reorg", rule = %f.label, "could not determine the chain head; reorg skipped");
                    return;
                }
            },
        };
        let event = self.state.chain.apply_reorg(head, depth, transactions);
        info!(
            fault = "reorg",
            rule = %f.label,
            first_block = event.first,
            last_block = event.last,
            generation = event.generation,
            transactions = ?transactions,
            "injecting fault"
        );
    }

    async fn fetch_head(&self) -> Option<u64> {
        match self.state.upstream.call("eth_blockNumber", json!([])).await {
            Ok(value) => parse_quantity(&value),
            Err(e) => {
                warn!(error = %e, "failed to query the chain head");
                None
            }
        }
    }

    /// Lag views need the real head before forwarding, unless the request
    /// only asks for `eth_blockNumber` (whose response carries it).
    async fn head_for_lag(&self, faults: &[InjectedFault]) -> Option<u64> {
        let has_lag = faults.iter().any(|f| {
            matches!(
                f.fault,
                Fault::StaleHead { .. } | Fault::InconsistentHead { .. }
            )
        });
        let needs_head = has_lag
            && self
                .request
                .is_some_and(|r| r.methods().any(|m| m != "eth_blockNumber"));
        if needs_head {
            self.fetch_head().await
        } else {
            None
        }
    }

    fn rewrite_request(&self, body: &Bytes, faults: &[InjectedFault], head: Option<u64>) -> Bytes {
        let reorg = self.state.chain.reorg_active();
        let lag_views: Vec<(&InjectedFault, u64)> = faults
            .iter()
            .filter_map(|f| match f.fault {
                Fault::StaleHead { lag } | Fault::InconsistentHead { lag } => {
                    head.map(|h| (f, h.saturating_sub(lag)))
                }
                _ => None,
            })
            .collect();
        if !reorg && lag_views.is_empty() {
            return body.clone();
        }
        let Ok(mut value) = serde_json::from_slice::<Value>(body) else {
            return body.clone();
        };
        let mut changed = false;
        let mut lag_applied = vec![false; lag_views.len()];
        let mut rewrite_call = |call: &mut Value| {
            let Some(obj) = call.as_object_mut() else {
                return;
            };
            let Some(method) = obj.get("method").and_then(Value::as_str).map(str::to_owned) else {
                return;
            };
            let Some(params) = obj.get_mut("params") else {
                return;
            };
            for (i, (f, visible)) in lag_views.iter().enumerate() {
                if f.selects(&method) && lag::rewrite_request(&method, params, *visible) {
                    lag_applied[i] = true;
                    changed = true;
                }
            }
            if reorg {
                changed |= self.state.chain.view().translate_request(&method, params);
            }
        };
        match &mut value {
            Value::Array(calls) => calls.iter_mut().for_each(&mut rewrite_call),
            call => rewrite_call(call),
        }
        for ((f, visible), applied) in lag_views.iter().zip(lag_applied) {
            if applied {
                info!(
                    fault = f.fault.name(),
                    rule = %f.label,
                    visible_head = visible,
                    "injecting fault: block tags rewritten to the lagging head"
                );
            }
        }
        if changed {
            debug!("request rewritten for the simulated chain view");
            Bytes::from(serde_json::to_vec(&value).unwrap_or_default())
        } else {
            body.clone()
        }
    }

    /// Applies response faults and the reorg view. Returns the new body when
    /// anything changed.
    fn mutate_response(
        &self,
        body: &Bytes,
        faults: &mut [InjectedFault],
        head: Option<u64>,
    ) -> Option<Bytes> {
        let request = self.request?;
        let mut value: Value = match serde_json::from_slice(body) {
            Ok(value) => value,
            Err(_) => {
                debug!("upstream response is not JSON; response faults skipped");
                return None;
            }
        };
        let mut applied = vec![false; faults.len()];
        let mut changed = false;
        match &mut value {
            Value::Array(items) => {
                for item in items {
                    changed |= self.mutate_element(request, item, faults, head, &mut applied);
                }
            }
            element => changed |= self.mutate_element(request, element, faults, head, &mut applied),
        }
        for (f, applied) in faults.iter().zip(applied) {
            if !f.fault.mutates_response() {
                continue;
            }
            if applied {
                info!(fault = f.fault.name(), rule = %f.label, "injecting fault");
            } else {
                debug!(fault = f.fault.name(), rule = %f.label, "fault matched but the response had nothing to change");
            }
        }
        changed.then(|| Bytes::from(serde_json::to_vec(&value).unwrap_or_default()))
    }

    fn mutate_element(
        &self,
        request: &RpcRequest,
        element: &mut Value,
        faults: &mut [InjectedFault],
        head: Option<u64>,
        applied: &mut [bool],
    ) -> bool {
        let Some(obj) = element.as_object_mut() else {
            return false;
        };
        let id = obj.get("id").cloned().unwrap_or(Value::Null);
        let Some(call) = request.call_for_id(&id) else {
            return false;
        };
        let Some(method) = call.method.as_deref() else {
            return false;
        };
        let mut changed = false;
        let outcome = {
            // Errors pass through untouched.
            let Some(result) = obj.get_mut("result") else {
                return false;
            };
            // The simulated chain comes first; faults act on top of it.
            let outcome = if self.state.chain.reorg_active() {
                self.state
                    .chain
                    .view()
                    .rewrite_result(method, call.params.as_ref(), result)
            } else {
                Rewrite::Unchanged
            };
            if outcome == Rewrite::Unchanged || outcome == Rewrite::Changed {
                changed |= outcome == Rewrite::Changed;
                for (i, f) in faults.iter_mut().enumerate() {
                    if f.fault.mutates_response() && f.selects(method) {
                        let did = self.apply_response_fault(f, call, method, result, head);
                        applied[i] |= did;
                        changed |= did;
                    }
                }
            }
            outcome
        };
        if let Rewrite::Error { code, message } = outcome {
            obj.remove("result");
            obj.insert(
                "error".to_owned(),
                json!({"code": code, "message": message}),
            );
            changed = true;
        }
        changed
    }

    fn apply_response_fault(
        &self,
        f: &mut InjectedFault,
        call: &RpcCall,
        method: &str,
        result: &mut Value,
        head: Option<u64>,
    ) -> bool {
        let visible = |lag: u64, result: &Value| -> Option<u64> {
            let head = head.or_else(|| {
                (method == "eth_blockNumber")
                    .then(|| parse_quantity(result))
                    .flatten()
            })?;
            Some(head.saturating_sub(lag))
        };
        match f.fault {
            Fault::ReceiptNull | Fault::BlockNull => {
                if result.is_null() {
                    return false;
                }
                *result = Value::Null;
                true
            }
            Fault::StaleHead { lag } => {
                visible(lag, result).is_some_and(|v| lag::rewrite_result(method, result, v, true))
            }
            Fault::InconsistentHead { lag } => {
                visible(lag, result).is_some_and(|v| lag::rewrite_result(method, result, v, false))
            }
            Fault::MissingLogs { ratio } => logs::drop_logs(result, ratio, &mut f.rng),
            Fault::DuplicatedLogs { ratio } => logs::duplicate_logs(result, ratio, &mut f.rng),
            Fault::ReorderedLogs => logs::reorder_logs(result, &mut f.rng),
            Fault::StaleNonce { lag } => logs::lower_nonce(result, lag),
            Fault::ReceiptDisappear { visible_for } => {
                if result.is_null() {
                    return false;
                }
                let Some(hash) = call.param(0).and_then(Value::as_str) else {
                    return false;
                };
                if self.state.chain.count_receipt_view(f.rule, hash) > visible_for {
                    *result = Value::Null;
                    return true;
                }
                false
            }
            _ => false,
        }
    }
}

fn json_error(request: Option<&RpcRequest>, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request.map_or(Value::Null, RpcRequest::error_id),
        "error": {"code": code, "message": message},
    })
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

/// A short, log-friendly excerpt of a response body.
fn summarize(body: &[u8]) -> String {
    const MAX: usize = 200;
    let text = String::from_utf8_lossy(body);
    let text = text.trim();
    if text.len() <= MAX {
        return text.to_owned();
    }
    let mut end = MAX;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summarize_truncates_on_char_boundary() {
        assert_eq!(summarize(b"  short  "), "short");
        let long = "é".repeat(150);
        let out = summarize(long.as_bytes());
        assert!(out.ends_with('…'));
        assert!(out.len() <= 204);
    }

    #[test]
    fn http_error_defaults_to_json_rpc_body() {
        let req = RpcRequest::parse(br#"{"jsonrpc":"2.0","id":4,"method":"eth_call"}"#);
        let response = http_error_response(req.as_ref(), 429, None, Some(3));
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "3");
        assert_eq!(response.headers()[ERROR_HEADER], "injected");
    }
}
