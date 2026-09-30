//! Shared test harness: an in-memory fake EVM node and proxy helpers.
//!
//! The fake node serves HTTP JSON-RPC and WebSocket subscriptions on one
//! port, like Anvil. Its chain is fully deterministic: block `n` has hash
//! `block_hash(n)`, one transaction `tx_hash(n)` and two logs.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use chainchaos_core::FaultConfig;
use chainchaos_proxy::{Proxy, ProxyConfig, UpstreamConfig};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use url::Url;

pub const HEAD: u64 = 100;
const BLOCK_BASE: u64 = 0xb10c_0000;
const TX_BASE: u64 = 0x7a00_0000;

pub fn block_hash(n: u64) -> String {
    format!("0x{:064x}", BLOCK_BASE + n)
}

pub fn tx_hash(n: u64) -> String {
    format!("0x{:064x}", TX_BASE + n)
}

fn decode(hash: &str, base: u64) -> Option<u64> {
    let n = u64::from_str_radix(hash.strip_prefix("0x")?, 16).ok()?;
    n.checked_sub(base).filter(|n| *n <= HEAD)
}

fn qty(n: u64) -> String {
    format!("{n:#x}")
}

pub fn logs_in(n: u64) -> Vec<Value> {
    (0..2)
        .map(|i| {
            json!({
                "address": "0x00000000000000000000000000000000000000aa",
                "topics": [],
                "data": "0x",
                "blockNumber": qty(n),
                "blockHash": block_hash(n),
                "transactionHash": tx_hash(n),
                "transactionIndex": "0x0",
                "logIndex": qty(i),
                "removed": false,
            })
        })
        .collect()
}

pub fn transaction(n: u64) -> Value {
    json!({
        "hash": tx_hash(n),
        "blockHash": block_hash(n),
        "blockNumber": qty(n),
        "transactionIndex": "0x0",
        "from": "0x00000000000000000000000000000000000000f0",
        "nonce": "0x0",
    })
}

pub fn block(n: u64, full: bool) -> Value {
    let parent = if n == 0 {
        format!("0x{:064x}", 0)
    } else {
        block_hash(n - 1)
    };
    let txs = if full {
        json!([transaction(n)])
    } else {
        json!([tx_hash(n)])
    };
    json!({
        "number": qty(n),
        "hash": block_hash(n),
        "parentHash": parent,
        "timestamp": qty(1_700_000_000 + n * 12),
        "transactions": txs,
    })
}

pub fn receipt(n: u64) -> Value {
    json!({
        "transactionHash": tx_hash(n),
        "blockHash": block_hash(n),
        "blockNumber": qty(n),
        "status": "0x1",
        "logs": logs_in(n),
    })
}

/// The fake node, with counters tests can inspect.
#[derive(Debug, Default)]
pub struct FakeChain {
    pub requests: AtomicU64,
    pub sent_transactions: Mutex<Vec<String>>,
}

impl FakeChain {
    pub fn sent(&self) -> usize {
        self.sent_transactions.lock().unwrap().len()
    }

    fn block_number(tag: &Value) -> Option<u64> {
        match tag.as_str()? {
            "latest" | "pending" | "safe" | "finalized" => Some(HEAD),
            "earliest" => Some(0),
            hex => u64::from_str_radix(hex.strip_prefix("0x")?, 16).ok(),
        }
    }

    /// Answers one call; `None` for calls needing special HTTP handling.
    fn answer(&self, call: &Value) -> Value {
        let id = call.get("id").cloned().unwrap_or(Value::Null);
        let params = call.get("params").cloned().unwrap_or(json!([]));
        let p = |i: usize| params.get(i).cloned().unwrap_or(Value::Null);
        let result = match call["method"].as_str().unwrap_or_default() {
            "eth_chainId" => json!("0x1"),
            "eth_blockNumber" => json!(qty(HEAD)),
            "eth_getBlockByNumber" => match Self::block_number(&p(0)) {
                Some(n) if n <= HEAD => block(n, p(1).as_bool().unwrap_or(false)),
                _ => Value::Null,
            },
            "eth_getBlockByHash" => match p(0).as_str().and_then(|h| decode(h, BLOCK_BASE)) {
                Some(n) => block(n, p(1).as_bool().unwrap_or(false)),
                None => Value::Null,
            },
            "eth_getLogs" => {
                let filter = p(0);
                if let Some(hash) = filter.get("blockHash").and_then(Value::as_str) {
                    match decode(hash, BLOCK_BASE) {
                        Some(n) => json!(logs_in(n)),
                        None => {
                            return json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":"unknown block"}});
                        }
                    }
                } else {
                    let from = filter
                        .get("fromBlock")
                        .and_then(Self::block_number)
                        .unwrap_or(HEAD);
                    let to = filter
                        .get("toBlock")
                        .and_then(Self::block_number)
                        .unwrap_or(HEAD)
                        .min(HEAD);
                    json!((from..=to).flat_map(logs_in).collect::<Vec<_>>())
                }
            }
            "eth_getTransactionReceipt" => match p(0).as_str().and_then(|h| decode(h, TX_BASE)) {
                Some(n) => receipt(n),
                None => Value::Null,
            },
            "eth_getTransactionByHash" => match p(0).as_str().and_then(|h| decode(h, TX_BASE)) {
                Some(n) => transaction(n),
                None => Value::Null,
            },
            "eth_getTransactionCount" => json!("0x5"),
            "eth_sendRawTransaction" => {
                self.sent_transactions
                    .lock()
                    .unwrap()
                    .push(p(0).as_str().unwrap_or_default().to_owned());
                json!(format!("0x{:064x}", 0xdead_beef_u64))
            }
            method => {
                return json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"code": -32601, "message": format!("the method {method} does not exist/is not available")},
                });
            }
        };
        json!({"jsonrpc": "2.0", "id": id, "result": result})
    }
}

async fn http_handler(
    State(chain): State<Arc<FakeChain>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    chain.requests.fetch_add(1, Ordering::Relaxed);
    let Ok(request) = serde_json::from_slice::<Value>(&body) else {
        let echo = format!("upstream got: {}", String::from_utf8_lossy(&body));
        return (StatusCode::BAD_REQUEST, echo).into_response();
    };
    let mut response = match &request {
        Value::Array(calls) => {
            let answers: Vec<Value> = calls.iter().map(|c| chain.answer(c)).collect();
            (StatusCode::OK, serde_json::to_string(&answers).unwrap()).into_response()
        }
        call if call["method"] == "http_500" => {
            (StatusCode::INTERNAL_SERVER_ERROR, "upstream exploded").into_response()
        }
        call if call["method"] == "slow" => {
            tokio::time::sleep(Duration::from_secs(5)).await;
            (StatusCode::OK, chain.answer(call).to_string()).into_response()
        }
        // Hand-formatted (odd spacing, id first) so re-serialisation by the
        // proxy would be detected.
        call if call["method"] == "eth_blockNumber" => {
            let body = format!(
                r#"{{"id": {},  "jsonrpc":"2.0", "result":"{}"}}"#,
                call["id"],
                qty(HEAD)
            );
            (StatusCode::OK, body).into_response()
        }
        call => (StatusCode::OK, chain.answer(call).to_string()).into_response(),
    };
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    if let Some(auth) = headers.get(header::AUTHORIZATION) {
        response
            .headers_mut()
            .insert("x-seen-authorization", auth.clone());
    }
    response
}

async fn ws_handler(State(chain): State<Arc<FakeChain>>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| ws_session(socket, chain))
}

/// Answers calls; `eth_subscribe` streams five notifications 30ms apart
/// (`newHeads` for blocks 96..=100, or their logs).
async fn ws_session(socket: WebSocket, chain: Arc<FakeChain>) {
    let (mut tx, mut rx) = socket.split();
    let (out_tx, mut out_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let writer = tokio::spawn(async move {
        while let Some(text) = out_rx.recv().await {
            if tx.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });
    while let Some(Ok(Message::Text(text))) = rx.next().await {
        let call: Value = serde_json::from_str(text.as_str()).unwrap();
        if call["method"] == "eth_subscribe" {
            let kind = call["params"][0].as_str().unwrap_or_default().to_owned();
            let sub = format!("0x{}", kind.to_ascii_lowercase());
            let _ = out_tx.send(json!({"jsonrpc":"2.0","id":call["id"],"result":sub}).to_string());
            let out = out_tx.clone();
            tokio::spawn(async move {
                for n in (HEAD - 4)..=HEAD {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    let result = if kind == "logs" {
                        logs_in(n)[0].clone()
                    } else {
                        let mut header = block(n, false);
                        header.as_object_mut().unwrap().remove("transactions");
                        header
                    };
                    let note = json!({"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":sub,"result":result}});
                    if out.send(note.to_string()).is_err() {
                        return;
                    }
                }
            });
        } else {
            let _ = out_tx.send(chain.answer(&call).to_string());
        }
    }
    writer.abort();
}

/// A running fake node.
pub struct Node {
    pub chain: Arc<FakeChain>,
    pub http: Url,
    pub ws: Url,
}

pub async fn spawn_node() -> Node {
    let chain = Arc::new(FakeChain::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/", post(http_handler).get(ws_handler))
        .with_state(chain.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Node {
        chain,
        http: Url::parse(&format!("http://{addr}/")).unwrap(),
        ws: Url::parse(&format!("ws://{addr}/")).unwrap(),
    }
}

/// Starts a proxy and returns its base URL (`http://127.0.0.1:port/`).
pub async fn spawn_proxy(config: ProxyConfig) -> String {
    let proxy = Proxy::new(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { proxy.serve(listener, std::future::pending()).await.unwrap() });
    format!("http://{addr}/")
}

/// A fake node plus a proxy in front of it configured from YAML.
pub async fn setup(yaml: &str) -> (Node, String) {
    let node = spawn_node().await;
    let proxy = spawn_proxy(ProxyConfig {
        upstream: UpstreamConfig::Http {
            url: node.http.clone(),
            timeout: Duration::from_secs(10),
        },
        upstream_ws: Some(node.ws.clone()),
        faults: FaultConfig::from_yaml(yaml).unwrap(),
        record: None,
    })
    .await;
    (node, proxy)
}

pub async fn post_raw(url: &str, body: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer test-token")
        .body(body.to_owned())
        .send()
        .await
        .unwrap()
}

/// Sends one call and returns the full JSON-RPC response object.
pub async fn rpc(url: &str, method: &str, params: Value) -> Value {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).to_string();
    post_raw(url, &body).await.json().await.unwrap()
}

/// Sends one call and returns its `result`, panicking on errors.
pub async fn result(url: &str, method: &str, params: Value) -> Value {
    let response = rpc(url, method, params).await;
    assert!(
        response.get("error").is_none(),
        "{method} failed: {response}"
    );
    response["result"].clone()
}
