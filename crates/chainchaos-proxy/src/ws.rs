//! WebSocket proxying with request and subscription faults.
//!
//! Each client connection gets its own upstream connection.
//!
//! - **JSON-RPC requests** sent over the socket go through the same fault
//!   [`Pipeline`] as HTTP requests (and share its request counter). Requests
//!   without faults are forwarded immediately; faulted ones run in their own
//!   task and their response is matched back by id, so a delayed request
//!   does not block the others (JSON-RPC over WebSocket allows out-of-order
//!   responses).
//! - **`eth_subscription` notifications** pass through the reorg view and the
//!   `ws_*` faults, in order.
//! - Everything else is forwarded unchanged.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chainchaos_core::{Fault, InjectedFault, RpcRequest, Tick};
use chainchaos_evm::reorg::LogFate;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message as UpstreamMessage;
use tracing::{Instrument, debug, info, info_span, warn};
use url::Url;

use crate::AppState;
use crate::pipeline::{CHAINCHAOS_ERROR_CODE, Early, Pipeline, RATE_LIMIT_CODE, json_error};

/// Close code sent by `ws_disconnect` with `graceful: true`
/// (1012 = "service restart", which well-behaved clients reconnect after).
const FORCED_RECONNECT_CODE: u16 = 1012;

/// How long a faulted request waits for its upstream response.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) async fn upgrade(State(state): State<Arc<AppState>>, ws: WebSocketUpgrade) -> Response {
    let Some(upstream) = state.upstream_ws.clone() else {
        return (
            StatusCode::BAD_REQUEST,
            "chainchaos: no WebSocket upstream configured (see --upstream-ws)\n",
        )
            .into_response();
    };
    ws.on_upgrade(move |socket| run(socket, state, upstream))
}

/// Subscription bookkeeping for one connection.
#[derive(Debug, Default)]
struct Subscriptions {
    /// `eth_subscribe` request id -> subscription type.
    pending: HashMap<String, String>,
    /// Subscription id -> subscription type.
    active: HashMap<String, String>,
}

impl Subscriptions {
    fn track_request(&mut self, value: &Value) {
        for call in calls(value) {
            if call.get("method").and_then(Value::as_str) != Some("eth_subscribe") {
                continue;
            }
            let (Some(id), Some(kind)) = (
                call.get("id"),
                call.pointer("/params/0").and_then(Value::as_str),
            ) else {
                continue;
            };
            self.pending.insert(id.to_string(), kind.to_owned());
        }
    }

    /// Returns the subscription type for notifications, and records
    /// subscription ids from subscribe responses.
    fn classify(&mut self, value: &Value) -> Classified {
        if value.get("method").and_then(Value::as_str) == Some("eth_subscription") {
            let kind = value
                .pointer("/params/subscription")
                .and_then(Value::as_str)
                .and_then(|id| self.active.get(id))
                .cloned();
            return Classified::Notification(kind);
        }
        if let (Some(id), Some(sub)) =
            (value.get("id"), value.get("result").and_then(Value::as_str))
        {
            if let Some(kind) = self.pending.remove(&id.to_string()) {
                debug!(subscription = sub, kind = %kind, "subscription created");
                self.active.insert(sub.to_owned(), kind);
            }
        }
        Classified::Other
    }
}

enum Classified {
    Notification(Option<String>),
    Other,
}

fn calls(value: &Value) -> Vec<&Value> {
    match value {
        Value::Array(items) => items.iter().collect(),
        other => vec![other],
    }
}

/// Key matching a request to its response: the id, or the sorted ids of a
/// batch. `None` for notifications, which get no response.
fn response_key(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) => {
            let mut ids: Vec<String> = items
                .iter()
                .filter_map(|c| c.get("id"))
                .map(Value::to_string)
                .collect();
            if ids.is_empty() {
                return None;
            }
            ids.sort();
            Some(format!("batch:{}", ids.join(",")))
        }
        other => other.get("id").map(Value::to_string),
    }
}

/// Shared per-connection state.
struct Conn {
    state: Arc<AppState>,
    id: u64,
    subs: Mutex<Subscriptions>,
    /// Faulted requests awaiting their upstream response, by response key.
    awaiting: Mutex<HashMap<String, oneshot::Sender<String>>>,
    to_upstream: mpsc::UnboundedSender<UpstreamMessage>,
    to_client: mpsc::UnboundedSender<Message>,
}

impl Conn {
    fn send_client(&self, text: String) {
        let _ = self.to_client.send(Message::Text(text.into()));
    }

    fn send_upstream(&self, text: String) {
        let _ = self.to_upstream.send(UpstreamMessage::Text(text.into()));
    }

    /// Handles a text message from the client.
    fn on_client_text(self: &Arc<Self>, text: String) {
        let Ok(value) = serde_json::from_str::<Value>(&text) else {
            return self.send_upstream(text);
        };
        lock(&self.subs).track_request(&value);
        let is_request = calls(&value)
            .iter()
            .any(|c| c.get("method").is_some_and(Value::is_string));
        let is_subscription = calls(&value).iter().any(|c| {
            matches!(
                c.get("method").and_then(Value::as_str),
                Some("eth_subscribe" | "eth_unsubscribe")
            )
        });
        if !is_request || is_subscription {
            return self.send_upstream(text);
        }

        let state = &self.state;
        let seq = state.requests.fetch_add(1, Ordering::Relaxed) + 1;
        let tick = Tick {
            seq,
            elapsed: state.started.elapsed(),
        };
        let request = RpcRequest::parse(text.as_bytes());
        let faults = state.engine.plan(request.as_ref(), tick);
        if faults.is_empty() && !state.chain.reorg_active() {
            return self.send_upstream(text);
        }
        state.metrics.faults_planned(&faults);
        let methods = request
            .as_ref()
            .map_or_else(|| "<unparsed>".to_owned(), RpcRequest::methods_summary);
        let span = info_span!("ws_rpc", conn = self.id, seq, method = %methods);
        let key = response_key(&value);
        let conn = self.clone();
        tokio::spawn(
            async move {
                conn.handle_request(text, request, faults, tick, key).await;
            }
            .instrument(span),
        );
    }

    async fn handle_request(
        &self,
        text: String,
        request: Option<RpcRequest>,
        mut faults: Vec<InjectedFault>,
        tick: Tick,
        key: Option<String>,
    ) {
        let pipeline = Pipeline {
            state: &self.state,
            request: request.as_ref(),
            tick,
        };
        let error =
            |code: i64, message: String| json_error(request.as_ref(), code, message).to_string();

        if let Some(early) = pipeline.before_forward(&faults).await {
            let reply = match early {
                // No HTTP status on a socket: report it as a JSON-RPC error.
                Early::HttpError { status, .. } => {
                    let code = if status == 429 {
                        RATE_LIMIT_CODE
                    } else {
                        -32603
                    };
                    error(code, format!("chainchaos: injected HTTP {status}"))
                }
                Early::Timeout { duration } => {
                    tokio::time::sleep(duration).await;
                    error(CHAINCHAOS_ERROR_CODE, "chainchaos: injected timeout".into())
                }
            };
            return self.send_client(reply);
        }

        let head = pipeline.head_for_lag(&faults).await;
        let body = pipeline.rewrite_request(&Bytes::from(text), &faults, head);
        let body = String::from_utf8_lossy(&body).into_owned();
        let Some(key) = key else {
            // A notification: nothing comes back.
            return self.send_upstream(body);
        };

        let (tx, rx) = oneshot::channel();
        lock(&self.awaiting).insert(key.clone(), tx);
        self.send_upstream(body);
        let response = match tokio::time::timeout(RESPONSE_TIMEOUT, rx).await {
            Ok(Ok(response)) => response,
            _ => {
                lock(&self.awaiting).remove(&key);
                warn!("no upstream response on the WebSocket");
                self.state.metrics.upstream_error();
                return self.send_client(error(
                    CHAINCHAOS_ERROR_CODE,
                    "chainchaos: upstream timed out".into(),
                ));
            }
        };

        if let Some((f, duration)) = Pipeline::hold_after_forward(&faults) {
            Pipeline::log_hold(f, duration, response.as_bytes());
            tokio::time::sleep(duration).await;
            return self.send_client(error(
                CHAINCHAOS_ERROR_CODE,
                "chainchaos: injected timeout".into(),
            ));
        }

        let response = if pipeline.mutates(&faults) {
            match pipeline.mutate_response(&Bytes::from(response.clone()), &mut faults, head) {
                Some(mutated) => String::from_utf8_lossy(&mutated).into_owned(),
                None => response,
            }
        } else {
            response
        };
        self.send_client(response);
    }
}

async fn run(client: WebSocket, state: Arc<AppState>, upstream_url: Url) {
    let conn_id = state.ws_connections.fetch_add(1, Ordering::Relaxed) + 1;
    let span = info_span!("ws", conn = conn_id);
    async move {
        let upstream = match tokio_tungstenite::connect_async(upstream_url.as_str()).await {
            Ok((stream, _)) => stream,
            Err(e) => {
                warn!(error = %e, "failed to connect to the WebSocket upstream");
                let mut client = client;
                let _ = client
                    .send(Message::Close(Some(CloseFrame {
                        code: 1011,
                        reason: "chainchaos: upstream unavailable".into(),
                    })))
                    .await;
                return;
            }
        };
        info!("websocket connected");
        let (mut up_sink, mut up_stream) = upstream.split();
        let (mut client_sink, mut client_stream) = client.split();
        let (to_upstream, mut upstream_rx) = mpsc::unbounded_channel::<UpstreamMessage>();
        let (to_client, mut client_rx) = mpsc::unbounded_channel::<Message>();

        let upstream_writer = tokio::spawn(async move {
            while let Some(message) = upstream_rx.recv().await {
                if up_sink.send(message).await.is_err() {
                    break;
                }
            }
            let _ = up_sink.close().await;
        });
        let client_writer = tokio::spawn(async move {
            while let Some(message) = client_rx.recv().await {
                let closing = matches!(message, Message::Close(_));
                if client_sink.send(message).await.is_err() || closing {
                    break;
                }
            }
        });

        let conn = Arc::new(Conn {
            state: state.clone(),
            id: conn_id,
            subs: Mutex::new(Subscriptions::default()),
            awaiting: Mutex::new(HashMap::new()),
            to_upstream,
            to_client,
        });

        let reader_conn = conn.clone();
        let client_reader = async move {
            while let Some(Ok(message)) = client_stream.next().await {
                match message {
                    Message::Text(text) => reader_conn.on_client_text(text.as_str().to_owned()),
                    Message::Binary(bytes) => {
                        let _ = reader_conn.to_upstream.send(UpstreamMessage::Binary(bytes));
                    }
                    Message::Ping(bytes) => {
                        let _ = reader_conn.to_upstream.send(UpstreamMessage::Ping(bytes));
                    }
                    Message::Pong(bytes) => {
                        let _ = reader_conn.to_upstream.send(UpstreamMessage::Pong(bytes));
                    }
                    Message::Close(_) => break,
                }
            }
            Disconnect::Normal
        };

        let upstream_conn = conn.clone();
        let upstream_reader = async move {
            let conn = upstream_conn;
            let mut held: Option<String> = None;
            while let Some(message) = up_stream.next().await {
                let text = match message {
                    Ok(UpstreamMessage::Text(text)) => text.as_str().to_owned(),
                    Ok(UpstreamMessage::Binary(bytes)) => {
                        let _ = conn.to_client.send(Message::Binary(bytes));
                        continue;
                    }
                    Ok(UpstreamMessage::Ping(bytes)) => {
                        let _ = conn.to_client.send(Message::Ping(bytes));
                        continue;
                    }
                    Ok(UpstreamMessage::Close(_)) | Err(_) => break,
                    Ok(_) => continue,
                };
                let Ok(mut value) = serde_json::from_str::<Value>(&text) else {
                    conn.send_client(text);
                    continue;
                };

                // Responses to faulted requests go back to their task.
                if value.get("method").is_none() {
                    if let Some(key) = response_key(&value) {
                        let waiter = lock(&conn.awaiting).remove(&key);
                        if let Some(waiter) = waiter {
                            let _ = waiter.send(text);
                            continue;
                        }
                    }
                }

                let classified = lock(&conn.subs).classify(&value);
                let kind = match classified {
                    Classified::Notification(kind) => kind,
                    Classified::Other => {
                        conn.send_client(text);
                        continue;
                    }
                };

                // The simulated chain comes first.
                let mut outgoing = text;
                if conn.state.chain.reorg_active() {
                    match rewrite_notification(&conn.state, kind.as_deref(), &mut value) {
                        NotificationFate::Unchanged => {}
                        NotificationFate::Changed => outgoing = value.to_string(),
                        NotificationFate::Drop => {
                            debug!("notification removed by the simulated reorg");
                            continue;
                        }
                    }
                }

                let seq = conn.state.ws_messages.fetch_add(1, Ordering::Relaxed) + 1;
                let tick = Tick {
                    seq,
                    elapsed: conn.state.started.elapsed(),
                };
                let mut copies = 1;
                let mut reorder = false;
                let faults = conn.state.engine.plan_ws(kind.as_deref(), tick);
                conn.state.metrics.faults_planned(&faults);
                for f in faults {
                    info!(fault = f.fault.name(), rule = %f.label, subscription = kind.as_deref().unwrap_or("?"), seq, "injecting fault");
                    match f.fault {
                        Fault::WsDisconnect { graceful } => {
                            if graceful {
                                let _ = conn.to_client.send(Message::Close(Some(CloseFrame {
                                    code: FORCED_RECONNECT_CODE,
                                    reason: "chainchaos: forced reconnect".into(),
                                })));
                                return Disconnect::Graceful;
                            }
                            return Disconnect::Abrupt;
                        }
                        Fault::WsDelay { duration } => tokio::time::sleep(duration).await,
                        Fault::WsDuplicate => copies = 2,
                        Fault::WsDrop | Fault::WsStale => copies = 0,
                        Fault::WsReorder => reorder = true,
                        _ => {}
                    }
                }
                if copies == 0 {
                    continue;
                }
                if reorder && held.is_none() {
                    held = Some(outgoing);
                    continue;
                }
                for _ in 0..copies {
                    conn.send_client(outgoing.clone());
                }
                if let Some(previous) = held.take() {
                    conn.send_client(previous);
                }
            }
            Disconnect::Normal
        };

        let outcome = tokio::select! {
            outcome = client_reader => outcome,
            outcome = upstream_reader => outcome,
        };
        // Let a graceful close frame (and queued messages) flush, then tear
        // down both sides.
        drop(conn);
        if outcome == Disconnect::Graceful {
            let _ = tokio::time::timeout(Duration::from_secs(1), client_writer).await;
        } else {
            client_writer.abort();
        }
        upstream_writer.abort();
        info!("websocket closed");
    }
    .instrument(span)
    .await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disconnect {
    Normal,
    Graceful,
    Abrupt,
}

enum NotificationFate {
    Unchanged,
    Changed,
    Drop,
}

/// Applies the reorged view to `newHeads` and `logs` notifications.
fn rewrite_notification(
    state: &AppState,
    kind: Option<&str>,
    value: &mut Value,
) -> NotificationFate {
    let Some(result) = value.pointer_mut("/params/result") else {
        return NotificationFate::Unchanged;
    };
    let is_log = kind == Some("logs") || result.get("logIndex").is_some();
    let is_head = kind == Some("newHeads") || result.get("parentHash").is_some();
    let mut view = state.chain.view();
    if is_log {
        return match view.rewrite_log(result) {
            LogFate::Keep(true) => NotificationFate::Changed,
            LogFate::Keep(false) => NotificationFate::Unchanged,
            LogFate::Drop => NotificationFate::Drop,
        };
    }
    if is_head && view.rewrite_block(result) {
        return NotificationFate::Changed;
    }
    NotificationFate::Unchanged
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn response_keys_match_requests_and_responses() {
        let single = json!({"jsonrpc":"2.0","id":"a","method":"eth_call"});
        assert_eq!(
            response_key(&single),
            response_key(&json!({"id":"a","result":"0x"}))
        );
        let batch = json!([{"id":2,"method":"m"},{"id":1,"method":"m"},{"method":"notify"}]);
        let reply = json!([{"id":1,"result":1},{"id":2,"result":2}]);
        assert_eq!(response_key(&batch), response_key(&reply));
        assert_eq!(response_key(&json!({"method":"notify"})), None);
    }
}
