//! End-to-end tests against a real Anvil node.
//!
//! Each test starts its own `anvil` process. When `anvil` is not on PATH the
//! tests are skipped, unless `CHAINCHAOS_REQUIRE_ANVIL=1` (set in CI), in
//! which case they fail.

mod common;

use std::net::TcpListener as StdListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use chainchaos_core::FaultConfig;
use chainchaos_core::recording::Recording;
use chainchaos_proxy::{ProxyConfig, RecordConfig, UpstreamConfig};
use common::{post_raw, result, rpc, spawn_proxy};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;
use url::Url;

/// Contract creation code that emits one anonymous log (LOG0) in its
/// constructor: PUSH1 0, PUSH1 0, LOG0, STOP.
const LOG_EMITTER_INIT_CODE: &str = "0x60006000a000";

struct Anvil {
    child: Child,
    http: Url,
    ws: Url,
}

impl Drop for Anvil {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn anvil() -> Option<Anvil> {
    let port = StdListener::bind("127.0.0.1:0")
        .ok()?
        .local_addr()
        .ok()?
        .port();
    let child = match Command::new("anvil")
        .args(["--port", &port.to_string(), "--silent"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            if std::env::var("CHAINCHAOS_REQUIRE_ANVIL").is_ok_and(|v| v == "1") {
                panic!("anvil is required but could not be started: {e}");
            }
            eprintln!("skipping: anvil not available ({e})");
            return None;
        }
    };
    let node = Anvil {
        child,
        http: Url::parse(&format!("http://127.0.0.1:{port}/")).unwrap(),
        ws: Url::parse(&format!("ws://127.0.0.1:{port}/")).unwrap(),
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let ready = reqwest::Client::new()
            .post(node.http.as_str())
            .header("content-type", "application/json")
            .body(r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#)
            .send()
            .await
            .is_ok();
        if ready {
            return Some(node);
        }
        assert!(Instant::now() < deadline, "anvil did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn proxy_for(node: &Anvil, yaml: &str) -> String {
    spawn_proxy(ProxyConfig {
        upstream: UpstreamConfig::Http {
            url: node.http.clone(),
            timeout: Duration::from_secs(10),
        },
        upstream_ws: Some(node.ws.clone()),
        faults: FaultConfig::from_yaml(yaml).unwrap(),
        record: None,
    })
    .await
}

fn number(v: &Value) -> u64 {
    u64::from_str_radix(v.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
}

async fn first_account(url: &str) -> String {
    result(url, "eth_accounts", json!([])).await[0]
        .as_str()
        .unwrap()
        .to_owned()
}

/// Polls for a receipt: even with automine, Anvil returns the hash before
/// the block is sealed.
async fn wait_receipt(url: &str, tx_hash: &Value) -> Value {
    for _ in 0..50 {
        let receipt = result(url, "eth_getTransactionReceipt", json!([tx_hash])).await;
        if !receipt.is_null() {
            return receipt;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no receipt for {tx_hash}");
}

async fn mine(url: &str, blocks: u64) {
    result(url, "anvil_mine", json!([format!("{blocks:#x}")])).await;
}

#[tokio::test]
async fn anvil_transparent_proxy_is_byte_exact() {
    let Some(node) = anvil().await else { return };
    let proxy = proxy_for(&node, "{}").await;
    mine(node.http.as_str(), 3).await;
    for body in [
        r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}"#,
        r#"{"jsonrpc":"2.0","id":"x","method":"eth_getBlockByNumber","params":["latest",true]}"#,
        r#"[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"},{"jsonrpc":"2.0","id":2,"method":"eth_gasPrice"}]"#,
        r#"{"jsonrpc":"2.0","id":3,"method":"eth_nope","params":[]}"#,
    ] {
        let direct = post_raw(node.http.as_str(), body)
            .await
            .text()
            .await
            .unwrap();
        let proxied = post_raw(&proxy, body).await.text().await.unwrap();
        assert_eq!(direct, proxied, "body: {body}");
    }
}

#[tokio::test]
async fn anvil_submit_timeout_leaves_an_ambiguous_but_mined_transaction() {
    let Some(node) = anvil().await else { return };
    let proxy = proxy_for(
        &node,
        "faults:\n  - { type: transaction_submit_timeout, method: eth_sendTransaction, duration: 2s }\n",
    )
    .await;
    let from = first_account(&proxy).await;
    let nonce_before =
        number(&result(&proxy, "eth_getTransactionCount", json!([from, "latest"])).await);

    let tx = json!({"jsonrpc":"2.0","id":1,"method":"eth_sendTransaction","params":[{"from": from, "to": from, "value": "0x1"}]});
    let err = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap()
        .post(&proxy)
        .header("content-type", "application/json")
        .body(tx.to_string())
        .send()
        .await
        .unwrap_err();
    assert!(err.is_timeout(), "client must see a timeout: {err}");

    // Anvil automines: the "failed" transaction is on chain.
    let mut nonce_after = nonce_before;
    for _ in 0..40 {
        nonce_after =
            number(&result(&proxy, "eth_getTransactionCount", json!([from, "latest"])).await);
        if nonce_after > nonce_before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(nonce_after, nonce_before + 1);
}

#[tokio::test]
async fn anvil_reorg_rewrites_a_real_block_receipt_and_log() {
    let Some(node) = anvil().await else { return };
    let direct = node.http.as_str();
    mine(direct, 5).await;
    let from = first_account(direct).await;
    let tx_hash = result(
        direct,
        "eth_sendTransaction",
        json!([{"from": from, "data": LOG_EMITTER_INIT_CODE}]),
    )
    .await;
    let receipt = wait_receipt(direct, &tx_hash).await;
    assert_eq!(
        receipt["logs"].as_array().unwrap().len(),
        1,
        "init code emits one log"
    );
    let tx_block = number(&receipt["blockNumber"]);
    mine(direct, 2).await;
    let head = tx_block + 2;

    // Reorg the last 3 blocks (including the transaction's) on the 2nd request.
    let proxy = proxy_for(
        &node,
        &format!("seed: 1\nscenario:\n  - after_requests: 1\n    inject: {{ type: reorg, depth: 3, head: {head} }}\n"),
    )
    .await;
    let block_hex = format!("{tx_block:#x}");
    let before = result(&proxy, "eth_getBlockByNumber", json!([block_hex, false])).await;
    let after = result(&proxy, "eth_getBlockByNumber", json!([block_hex, false])).await;
    assert_ne!(after["hash"], before["hash"], "transaction block replaced");
    let parent = result(
        &proxy,
        "eth_getBlockByNumber",
        json!([format!("{:#x}", tx_block - 1), false]),
    )
    .await;
    assert_eq!(
        after["parentHash"], parent["hash"],
        "fork point is canonical"
    );
    let child = result(
        &proxy,
        "eth_getBlockByNumber",
        json!([format!("{:#x}", tx_block + 1), false]),
    )
    .await;
    assert_eq!(child["parentHash"], after["hash"], "new branch is linked");

    let receipt = result(&proxy, "eth_getTransactionReceipt", json!([tx_hash])).await;
    assert_eq!(receipt["blockHash"], after["hash"]);
    assert_eq!(receipt["logs"][0]["blockHash"], after["hash"]);
    let logs = result(
        &proxy,
        "eth_getLogs",
        json!([{"fromBlock": block_hex, "toBlock": block_hex}]),
    )
    .await;
    assert_eq!(logs[0]["blockHash"], after["hash"]);

    assert_eq!(
        result(&proxy, "eth_getBlockByHash", json!([before["hash"], false])).await,
        Value::Null,
        "old hash is no longer canonical"
    );
    let old_logs = rpc(
        &proxy,
        "eth_getLogs",
        json!([{"blockHash": before["hash"]}]),
    )
    .await;
    assert_eq!(old_logs["error"]["message"], "unknown block");
    let by_new = result(&proxy, "eth_getBlockByHash", json!([after["hash"], false])).await;
    assert_eq!(by_new["number"], json!(block_hex));
}

#[tokio::test]
async fn anvil_stale_head_lags_behind() {
    let Some(node) = anvil().await else { return };
    mine(node.http.as_str(), 10).await;
    let proxy = proxy_for(&node, "faults:\n  - { type: stale_head, lag: 4 }\n").await;
    assert_eq!(
        number(&result(&proxy, "eth_blockNumber", json!([])).await),
        6
    );
    let latest = result(&proxy, "eth_getBlockByNumber", json!(["latest", false])).await;
    assert_eq!(number(&latest["number"]), 6);
    assert_eq!(
        result(&proxy, "eth_getBlockByNumber", json!(["0xa", false])).await,
        Value::Null
    );
}

#[tokio::test]
async fn anvil_websocket_new_heads_with_a_dropped_notification() {
    let Some(node) = anvil().await else { return };
    let proxy = proxy_for(&node, "faults:\n  - { type: ws_drop, count: 1 }\n").await;
    let (mut socket, _) = tokio_tungstenite::connect_async(proxy.replacen("http://", "ws://", 1))
        .await
        .unwrap();
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let ack = socket.next().await.unwrap().unwrap();
    assert!(ack.to_text().unwrap().contains("\"result\""), "{ack}");

    mine(&proxy, 3).await;
    let mut heads = Vec::new();
    while let Ok(Some(Ok(Message::Text(text)))) =
        tokio::time::timeout(Duration::from_millis(1500), socket.next()).await
    {
        let value: Value = serde_json::from_str(text.as_str()).unwrap();
        heads.push(number(&value["params"]["result"]["number"]));
    }
    assert_eq!(heads, [2, 3], "block 1's notification was dropped");
}

#[tokio::test]
async fn anvil_record_then_replay_without_the_node() {
    let Some(node) = anvil().await else { return };
    mine(node.http.as_str(), 2).await;
    let path = std::env::temp_dir().join(format!("chainchaos-anvil-{}.ccr", std::process::id()));
    let recorder = spawn_proxy(ProxyConfig {
        record: Some(RecordConfig {
            output: path.clone(),
            redact_fields: vec![],
            upstream_label: node.http.to_string(),
        }),
        ..ProxyConfig::http(node.http.clone())
    })
    .await;
    let recorded = result(&recorder, "eth_getBlockByNumber", json!(["0x2", false])).await;
    drop(node); // the node is gone

    let replay = spawn_proxy(ProxyConfig {
        upstream: UpstreamConfig::Replay {
            recording: Recording::load(&path).unwrap(),
            replay_latency: false,
        },
        upstream_ws: None,
        faults: FaultConfig::default(),
        record: None,
    })
    .await;
    let replayed = result(&replay, "eth_getBlockByNumber", json!(["0x2", false])).await;
    assert_eq!(replayed, recorded);
    std::fs::remove_file(&path).unwrap();
}
