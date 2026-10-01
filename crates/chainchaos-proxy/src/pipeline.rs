//! The transport-neutral fault pipeline shared by HTTP and WebSocket.
//!
//! ```text
//! plan faults ─▶ before_forward ─▶ rewrite_request ─▶ (transport forwards) ─▶ hold / mutate_response
//!                delay, reorg,      lag views,                                 tx-submit timeout,
//!                http_error and     reorg hash                                 reorg view,
//!                timeout (early)    translation                                response faults
//! ```
//!
//! The transport owns forwarding and turns [`Early`] outcomes into its own
//! wire format (HTTP status codes, or JSON-RPC errors on a WebSocket).

use std::time::Duration;

use axum::body::Bytes;
use chainchaos_core::{Fault, InjectedFault, ReorgTransactions, RpcCall, RpcRequest, Tick};
use chainchaos_evm::hex::parse_quantity;
use chainchaos_evm::reorg::Rewrite;
use chainchaos_evm::{lag, logs};
use serde_json::{Value, json};
use tracing::{debug, info, warn};

use crate::AppState;

/// JSON-RPC error code for errors produced by chainchaos itself (upstream
/// unavailable, injected timeouts). Within the implementation-defined server
/// error range (-32000..=-32099).
pub(crate) const CHAINCHAOS_ERROR_CODE: i64 = -32000;

/// JSON-RPC error code commonly used by providers for rate limiting.
pub(crate) const RATE_LIMIT_CODE: i64 = -32005;

/// A fault that answers the client without forwarding the request.
#[derive(Debug, Clone)]
pub(crate) enum Early {
    HttpError {
        status: u16,
        body: Option<String>,
        retry_after: Option<u64>,
    },
    /// Hold for `duration`, then answer with a timeout error.
    Timeout { duration: Duration },
}

pub(crate) struct Pipeline<'a> {
    pub state: &'a AppState,
    pub request: Option<&'a RpcRequest>,
    pub tick: Tick,
}

impl Pipeline<'_> {
    /// Applies faults that act before forwarding (delays, reorg events).
    /// Returns an early outcome when a fault answers instead of the upstream.
    pub async fn before_forward(&self, faults: &[InjectedFault]) -> Option<Early> {
        for f in faults {
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
                    return Some(Early::HttpError {
                        status: *status,
                        body: body.clone(),
                        retry_after: *retry_after,
                    });
                }
                Fault::Timeout {
                    duration,
                    forward: false,
                } => {
                    info!(fault = "timeout", rule = %f.label, forwarded = false, hold_ms = duration.as_millis() as u64, "injecting fault");
                    return Some(Early::Timeout {
                        duration: *duration,
                    });
                }
                Fault::Reorg {
                    depth,
                    head,
                    transactions,
                } => self.apply_reorg(f, *depth, *head, *transactions).await,
                _ => {}
            }
        }
        None
    }

    /// Whether the response may need rewriting (so it must be parsed).
    pub fn mutates(&self, faults: &[InjectedFault]) -> bool {
        faults.iter().any(|f| f.fault.mutates_response()) || self.state.chain.reorg_active()
    }

    /// Faults that let the request reach the upstream, then hold the client
    /// and answer with a timeout: the ambiguous outcome.
    pub fn hold_after_forward(faults: &[InjectedFault]) -> Option<(&InjectedFault, Duration)> {
        faults.iter().find_map(|f| match &f.fault {
            Fault::Timeout {
                duration,
                forward: true,
            }
            | Fault::TransactionSubmitTimeout { duration } => Some((f, *duration)),
            _ => None,
        })
    }

    /// Logs the ambiguous outcome before holding the client.
    pub fn log_hold(f: &InjectedFault, duration: Duration, upstream_response: &[u8]) {
        info!(
            fault = f.fault.name(),
            rule = %f.label,
            upstream_response = %summarize(upstream_response),
            hold_ms = duration.as_millis() as u64,
            "injecting fault: request reached the upstream, client will see a timeout"
        );
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
    pub async fn head_for_lag(&self, faults: &[InjectedFault]) -> Option<u64> {
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

    pub fn rewrite_request(
        &self,
        body: &Bytes,
        faults: &[InjectedFault],
        head: Option<u64>,
    ) -> Bytes {
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
    pub fn mutate_response(
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

pub(crate) fn json_error(request: Option<&RpcRequest>, code: i64, message: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": request.map_or(Value::Null, RpcRequest::error_id),
        "error": {"code": code, "message": message},
    })
}

pub(crate) fn summarize(body: &[u8]) -> String {
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
}
