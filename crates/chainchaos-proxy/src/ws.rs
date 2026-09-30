//! WebSocket proxying with subscription-level faults.
//!
//! Each client connection gets its own upstream connection. Client messages
//! are forwarded unchanged. Upstream messages are forwarded unchanged too,
//! except `eth_subscription` notifications, which pass through the reorg
//! view and the `ws_*` faults.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chainchaos_core::{Fault, Tick};
use chainchaos_evm::reorg::LogFate;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message as UpstreamMessage;
use tracing::{Instrument, debug, info, info_span, warn};
use url::Url;

use crate::AppState;

/// Close code sent by `ws_disconnect` with `graceful: true`
/// (1012 = "service restart", which well-behaved clients reconnect after).
const FORCED_RECONNECT_CODE: u16 = 1012;

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
    fn track_request(&mut self, text: &str) {
        let Ok(value) = serde_json::from_str::<Value>(text) else {
            return;
        };
        let calls = match &value {
            Value::Array(items) => items.iter().collect(),
            other => vec![other],
        };
        for call in calls {
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

    /// Classifies an upstream message: returns the subscription type for
    /// notifications, and records subscription ids from subscribe responses.
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

async fn run(client: WebSocket, state: Arc<AppState>, upstream_url: Url) {
    let conn = state.ws_connections.fetch_add(1, Ordering::Relaxed) + 1;
    let span = info_span!("ws", conn);
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
        let (mut up_tx, mut up_rx) = upstream.split();
        let (mut client_tx, mut client_rx) = client.split();
        let subs = Arc::new(Mutex::new(Subscriptions::default()));

        let client_subs = subs.clone();
        let to_upstream = async move {
            while let Some(Ok(message)) = client_rx.next().await {
                let forwarded = match message {
                    Message::Text(text) => {
                        lock(&client_subs).track_request(text.as_str());
                        UpstreamMessage::Text(text.as_str().into())
                    }
                    Message::Binary(bytes) => UpstreamMessage::Binary(bytes),
                    Message::Ping(bytes) => UpstreamMessage::Ping(bytes),
                    Message::Pong(bytes) => UpstreamMessage::Pong(bytes),
                    Message::Close(_) => break,
                };
                if up_tx.send(forwarded).await.is_err() {
                    break;
                }
            }
            let _ = up_tx.close().await;
        };

        let to_client = async move {
            let mut held: Option<String> = None;
            while let Some(message) = up_rx.next().await {
                let text = match message {
                    Ok(UpstreamMessage::Text(text)) => text.as_str().to_owned(),
                    Ok(UpstreamMessage::Binary(bytes)) => {
                        if client_tx.send(Message::Binary(bytes)).await.is_err() {
                            break;
                        }
                        continue;
                    }
                    Ok(UpstreamMessage::Ping(bytes)) => {
                        let _ = client_tx.send(Message::Ping(bytes)).await;
                        continue;
                    }
                    Ok(UpstreamMessage::Close(_)) | Err(_) => break,
                    Ok(_) => continue,
                };
                let Ok(mut value) = serde_json::from_str::<Value>(&text) else {
                    if client_tx.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                    continue;
                };
                let classified = lock(&subs).classify(&value);
                let kind = match classified {
                    Classified::Notification(kind) => kind,
                    Classified::Other => {
                        if client_tx.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                        continue;
                    }
                };

                // The simulated chain comes first.
                let mut outgoing = text;
                if state.chain.reorg_active() {
                    match rewrite_notification(&state, kind.as_deref(), &mut value) {
                        NotificationFate::Unchanged => {}
                        NotificationFate::Changed => outgoing = value.to_string(),
                        NotificationFate::Drop => {
                            debug!("notification removed by the simulated reorg");
                            continue;
                        }
                    }
                }

                let seq = state.ws_messages.fetch_add(1, Ordering::Relaxed) + 1;
                let tick = Tick {
                    seq,
                    elapsed: state.started.elapsed(),
                };
                let mut copies = 1;
                let mut reorder = false;
                for f in state.engine.plan_ws(kind.as_deref(), tick) {
                    info!(fault = f.fault.name(), rule = %f.label, subscription = kind.as_deref().unwrap_or("?"), seq, "injecting fault");
                    match f.fault {
                        Fault::WsDisconnect { graceful } => {
                            if graceful {
                                let _ = client_tx
                                    .send(Message::Close(Some(CloseFrame {
                                        code: FORCED_RECONNECT_CODE,
                                        reason: "chainchaos: forced reconnect".into(),
                                    })))
                                    .await;
                            }
                            return;
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
                    if client_tx.send(Message::Text(outgoing.clone().into())).await.is_err() {
                        return;
                    }
                }
                if let Some(previous) = held.take() {
                    if client_tx.send(Message::Text(previous.into())).await.is_err() {
                        return;
                    }
                }
            }
        };

        tokio::select! {
            () = to_upstream => {},
            () = to_client => {},
        }
        info!("websocket closed");
    }
    .instrument(span)
    .await
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

fn lock(subs: &Mutex<Subscriptions>) -> std::sync::MutexGuard<'_, Subscriptions> {
    subs.lock().unwrap_or_else(|e| e.into_inner())
}
