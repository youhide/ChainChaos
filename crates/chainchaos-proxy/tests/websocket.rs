//! Phase 5: WebSocket subscriptions.

mod common;

use std::time::Duration;

use common::{HEAD, block_hash, setup};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;

/// What a subscriber observed.
#[derive(Debug, Default)]
struct Observed {
    /// Block numbers (newHeads) or block numbers of logs, in arrival order.
    numbers: Vec<u64>,
    hashes: Vec<String>,
    close_code: Option<u16>,
    closed: bool,
}

/// Subscribes through the proxy and collects notifications until the
/// stream goes quiet or closes.
async fn subscribe(proxy: &str, kind: &str) -> Observed {
    let url = proxy.replacen("http://", "ws://", 1);
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":[kind]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let mut observed = Observed::default();
    loop {
        match tokio::time::timeout(Duration::from_millis(400), socket.next()).await {
            Err(_) => break,
            Ok(None) | Ok(Some(Err(_))) => {
                observed.closed = true;
                break;
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                observed.closed = true;
                observed.close_code = frame.map(|f| f.code.into());
                break;
            }
            Ok(Some(Ok(Message::Text(text)))) => {
                let value: Value = serde_json::from_str(text.as_str()).unwrap();
                if value["method"] == "eth_subscription" {
                    let result = &value["params"]["result"];
                    let field = if kind == "logs" {
                        "blockNumber"
                    } else {
                        "number"
                    };
                    let n = u64::from_str_radix(
                        result[field].as_str().unwrap().trim_start_matches("0x"),
                        16,
                    )
                    .unwrap();
                    observed.numbers.push(n);
                    let hash_field = if kind == "logs" { "blockHash" } else { "hash" };
                    observed
                        .hashes
                        .push(result[hash_field].as_str().unwrap().to_owned());
                } else {
                    assert_eq!(value["id"], 1, "subscribe response");
                }
            }
            Ok(Some(Ok(_))) => {}
        }
    }
    observed
}

const ALL: [u64; 5] = [HEAD - 4, HEAD - 3, HEAD - 2, HEAD - 1, HEAD];

#[tokio::test]
async fn proxies_subscriptions_transparently() {
    let (_, proxy) = setup("{}").await;
    let observed = subscribe(&proxy, "newHeads").await;
    assert_eq!(observed.numbers, ALL);
    assert_eq!(observed.hashes[4], block_hash(HEAD));
}

#[tokio::test]
async fn ws_drop_loses_a_notification() {
    let (_, proxy) = setup("faults:\n  - { type: ws_drop, count: 1 }\n").await;
    assert_eq!(subscribe(&proxy, "newHeads").await.numbers, &ALL[1..]);
}

#[tokio::test]
async fn ws_duplicate_repeats_a_notification() {
    let (_, proxy) = setup("faults:\n  - { type: ws_duplicate, count: 1 }\n").await;
    let observed = subscribe(&proxy, "newHeads").await;
    assert_eq!(
        observed.numbers,
        [HEAD - 4, HEAD - 4, HEAD - 3, HEAD - 2, HEAD - 1, HEAD]
    );
}

#[tokio::test]
async fn ws_reorder_swaps_notifications() {
    let (_, proxy) = setup("faults:\n  - { type: ws_reorder, count: 1 }\n").await;
    let observed = subscribe(&proxy, "newHeads").await;
    assert_eq!(
        observed.numbers,
        [HEAD - 3, HEAD - 4, HEAD - 2, HEAD - 1, HEAD]
    );
}

#[tokio::test]
async fn ws_disconnect_forces_a_reconnect() {
    let (_, proxy) = setup(
        "scenario:\n  - after_requests: 2\n    inject: { type: ws_disconnect, graceful: true }\n",
    )
    .await;
    let observed = subscribe(&proxy, "newHeads").await;
    assert_eq!(observed.numbers, &ALL[..2]);
    assert!(observed.closed);
    assert_eq!(observed.close_code, Some(1012));
}

#[tokio::test]
async fn ws_stale_silences_the_stream_but_keeps_the_connection() {
    let (_, proxy) =
        setup("scenario:\n  - after_requests: 1\n    inject: { type: ws_stale }\n").await;
    let observed = subscribe(&proxy, "newHeads").await;
    assert_eq!(observed.numbers, &ALL[..1]);
    assert!(!observed.closed, "connection stays open");
}

#[tokio::test]
async fn ws_faults_filter_by_subscription_type() {
    let (_, proxy) = setup("faults:\n  - { type: ws_drop, subscription: logs }\n").await;
    assert_eq!(subscribe(&proxy, "newHeads").await.numbers, ALL);
    assert!(subscribe(&proxy, "logs").await.numbers.is_empty());
}

#[tokio::test]
async fn ws_delay_slows_notifications() {
    let (_, proxy) = setup("faults:\n  - { type: ws_delay, duration: 150ms, count: 1 }\n").await;
    let started = std::time::Instant::now();
    let observed = subscribe(&proxy, "newHeads").await;
    assert_eq!(observed.numbers, ALL);
    // The first notification is held 150ms; the 400ms quiet period follows
    // the last one.
    assert!(started.elapsed() >= Duration::from_millis(150 + 400));
}

#[tokio::test]
async fn new_heads_follow_the_reorged_view() {
    // An HTTP request triggers the reorg; subscriptions then see the new branch.
    let (_, proxy) = setup(
        "scenario:\n  - after_requests: 0\n    inject: { type: reorg, depth: 2, head: 100 }\n",
    )
    .await;
    let reorged = common::result(&proxy, "eth_getBlockByNumber", json!(["0x64", false])).await;
    let observed = subscribe(&proxy, "newHeads").await;
    assert_eq!(observed.numbers, ALL);
    assert_eq!(observed.hashes[0], block_hash(HEAD - 4));
    assert_eq!(
        serde_json::Value::String(observed.hashes[4].clone()),
        reorged["hash"]
    );
}

#[tokio::test]
async fn websocket_is_rejected_without_upstream() {
    let proxy = common::spawn_proxy(chainchaos_proxy::ProxyConfig::http(
        url::Url::parse("http://127.0.0.1:1/").unwrap(),
    ))
    .await;
    let url = proxy.replacen("http://", "ws://", 1);
    let err = tokio_tungstenite::connect_async(url).await.unwrap_err();
    assert!(err.to_string().contains("400"), "{err}");
}

// --- JSON-RPC requests over the WebSocket go through the same faults as HTTP.

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(proxy: &str) -> Socket {
    let (socket, _) = tokio_tungstenite::connect_async(proxy.replacen("http://", "ws://", 1))
        .await
        .unwrap();
    socket
}

async fn send(socket: &mut Socket, id: u64, method: &str, params: Value) {
    let call = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
    socket
        .send(Message::Text(call.to_string().into()))
        .await
        .unwrap();
}

/// Reads JSON messages until `n` responses (messages with an id) arrived.
async fn responses(socket: &mut Socket, n: usize) -> Vec<Value> {
    let mut out = Vec::new();
    while out.len() < n {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("response timed out")
            .unwrap()
            .unwrap();
        if let Message::Text(text) = message {
            let value: Value = serde_json::from_str(text.as_str()).unwrap();
            if value.get("id").is_some() {
                out.push(value);
            }
        }
    }
    out
}

#[tokio::test]
async fn ws_requests_get_response_faults() {
    let (_, proxy) = setup("faults:\n  - { type: receipt_null }\n").await;
    let mut socket = connect(&proxy).await;
    send(
        &mut socket,
        7,
        "eth_getTransactionReceipt",
        json!([common::tx_hash(5)]),
    )
    .await;
    let reply = responses(&mut socket, 1).await.remove(0);
    assert_eq!(reply["id"], 7);
    assert_eq!(reply["result"], Value::Null);
}

#[tokio::test]
async fn ws_delayed_requests_do_not_block_others() {
    let (_, proxy) =
        setup("faults:\n  - { type: delay, method: eth_getLogs, duration: 400ms }\n").await;
    let mut socket = connect(&proxy).await;
    send(
        &mut socket,
        1,
        "eth_getLogs",
        json!([{"fromBlock": "0x60"}]),
    )
    .await;
    send(&mut socket, 2, "eth_chainId", json!([])).await;
    let replies = responses(&mut socket, 2).await;
    assert_eq!(
        replies[0]["id"], 2,
        "the fast request overtakes the delayed one"
    );
    assert_eq!(replies[1]["id"], 1);
}

#[tokio::test]
async fn ws_http_error_becomes_a_json_rpc_error() {
    let (_, proxy) = setup("faults:\n  - { type: http_error, status: 429, count: 1 }\n").await;
    let mut socket = connect(&proxy).await;
    send(&mut socket, 3, "eth_chainId", json!([])).await;
    send(&mut socket, 4, "eth_chainId", json!([])).await;
    let replies = responses(&mut socket, 2).await;
    let limited = replies.iter().find(|r| r["id"] == 3).unwrap();
    assert_eq!(limited["error"]["code"], -32005);
    let ok = replies.iter().find(|r| r["id"] == 4).unwrap();
    assert_eq!(ok["result"], "0x1");
}

#[tokio::test]
async fn ws_requests_see_the_reorged_chain() {
    let (_, proxy) = setup(
        "scenario:\n  - after_requests: 0\n    inject: { type: reorg, depth: 1, head: 100 }\n",
    )
    .await;
    let mut socket = connect(&proxy).await;
    send(
        &mut socket,
        1,
        "eth_getBlockByNumber",
        json!(["0x64", false]),
    )
    .await;
    let block = responses(&mut socket, 1).await.remove(0);
    assert_ne!(block["result"]["hash"], json!(block_hash(HEAD)));
    // HTTP and WebSocket agree on the synthetic branch.
    let over_http = common::result(&proxy, "eth_getBlockByNumber", json!(["0x64", false])).await;
    assert_eq!(over_http["hash"], block["result"]["hash"]);
}

#[tokio::test]
async fn ws_transaction_submit_timeout_reaches_the_node() {
    let (node, proxy) =
        setup("faults:\n  - { type: transaction_submit_timeout, duration: 10s }\n").await;
    let mut socket = connect(&proxy).await;
    send(&mut socket, 1, "eth_sendRawTransaction", json!(["0x02ab"])).await;
    send(&mut socket, 2, "eth_chainId", json!([])).await;
    let replies = responses(&mut socket, 1).await;
    assert_eq!(replies[0]["id"], 2, "no answer for the submission yet");
    assert_eq!(
        node.chain.sent(),
        1,
        "but the node already has the transaction"
    );
}
