//! Phases 6 and 7: record, replay, and replay + chaos.

mod common;

use std::path::PathBuf;
use std::time::Duration;

use chainchaos_core::FaultConfig;
use chainchaos_core::recording::{Body, Recording};
use chainchaos_proxy::{ProxyConfig, RecordConfig, UpstreamConfig};
use common::{block_hash, post_raw, result, rpc, spawn_node, spawn_proxy, tx_hash};
use serde_json::{Value, json};

fn temp_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("chainchaos-test-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("session.ccr")
}

/// Records a small indexer-like session and returns the recording path.
async fn record_session(name: &str, redact: &[&str]) -> PathBuf {
    let node = spawn_node().await;
    let path = temp_path(name);
    let recorder = spawn_proxy(ProxyConfig {
        record: Some(RecordConfig {
            output: path.clone(),
            redact_fields: redact.iter().map(|s| (*s).to_owned()).collect(),
            upstream_label: node.http.to_string(),
        }),
        ..ProxyConfig::http(node.http.clone())
    })
    .await;
    result(&recorder, "eth_blockNumber", json!([])).await;
    result(&recorder, "eth_getBlockByNumber", json!(["0x63", false])).await;
    result(&recorder, "eth_getBlockByNumber", json!(["0x64", false])).await;
    result(
        &recorder,
        "eth_getLogs",
        json!([{"fromBlock": "0x63", "toBlock": "0x64"}]),
    )
    .await;
    result(
        &recorder,
        "eth_getTransactionReceipt",
        json!([tx_hash(100)]),
    )
    .await;
    let batch = json!([
        {"jsonrpc":"2.0","id":"x","method":"eth_chainId"},
        {"jsonrpc":"2.0","id":"y","method":"eth_getTransactionCount","params":["0x1","latest"]},
    ]);
    post_raw(&recorder, &batch.to_string()).await;
    post_raw(&recorder, &json!({"jsonrpc":"2.0","id":7,"method":"eth_call","params":[{"to":"0x1","apiKey":"s3cret"}]}).to_string()).await;
    path
}

async fn replay(path: &PathBuf, yaml: &str) -> String {
    spawn_proxy(ProxyConfig {
        upstream: UpstreamConfig::Replay {
            recording: Recording::load(path).unwrap(),
            replay_latency: false,
        },
        upstream_ws: None,
        faults: FaultConfig::from_yaml(yaml).unwrap(),
        record: None,
    })
    .await
}

#[tokio::test]
async fn records_a_versioned_normalised_session() {
    let path = record_session("format", &["apiKey"]).await;
    let recording = Recording::load(&path).unwrap();
    assert_eq!(recording.header.format, "chainchaos-recording");
    assert_eq!(recording.header.version, 1);
    assert_eq!(recording.entries.len(), 7);
    let seqs: Vec<u64> = recording.entries.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=7).collect::<Vec<_>>(), "arrival order preserved");
    for entry in &recording.entries {
        if let Body::Json(Value::Object(req)) = &entry.request {
            assert!(!req.contains_key("id"), "request ids are normalised away");
        }
    }
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(
        !text.contains("s3cret"),
        "redacted fields never reach the file"
    );
    assert!(text.contains("<redacted>"));
}

#[tokio::test]
async fn replays_without_an_upstream_restoring_ids() {
    let path = record_session("replay", &[]).await;
    // The original node is gone as far as replay is concerned.
    let proxy = replay(&path, "{}").await;

    let response = post_raw(&proxy, r#"{"jsonrpc":"2.0","id":"new-id","method":"eth_getBlockByNumber","params":["0x64",false]}"#).await;
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["id"], "new-id");
    assert_eq!(body["result"]["hash"], json!(block_hash(100)));

    // Batches come back in request order with the live ids.
    let batch = json!([
        {"jsonrpc":"2.0","id":100,"method":"eth_chainId"},
        {"jsonrpc":"2.0","id":101,"method":"eth_getTransactionCount","params":["0x1","latest"]},
    ]);
    let body: Value = post_raw(&proxy, &batch.to_string())
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body[0]["id"], 100);
    assert_eq!(body[0]["result"], "0x1");
    assert_eq!(body[1]["id"], 101);
    assert_eq!(body[1]["result"], "0x5");

    // Unknown requests are a clear, well-formed miss.
    let miss = post_raw(
        &proxy,
        r#"{"jsonrpc":"2.0","id":3,"method":"eth_getBalance","params":["0x2","latest"]}"#,
    )
    .await;
    assert_eq!(miss.headers()["x-chainchaos-error"], "replay-miss");
    let body: Value = miss.json().await.unwrap();
    assert_eq!(body["id"], 3);
    assert_eq!(body["error"]["code"], -32001);
}

#[tokio::test]
async fn replay_is_deterministic_across_runs() {
    let path = record_session("determinism", &[]).await;
    let a = replay(&path, "{}").await;
    let b = replay(&path, "{}").await;
    let query = json!([{"fromBlock": "0x63", "toBlock": "0x64"}]);
    assert_eq!(
        result(&a, "eth_getLogs", query.clone()).await,
        result(&b, "eth_getLogs", query).await
    );
}

#[tokio::test]
async fn replay_with_chaos_combines_fixtures_and_faults() {
    let path = record_session("chaos", &[]).await;
    let proxy = replay(
        &path,
        "seed: 3\nfaults:\n  - { type: receipt_null, count: 1 }\nscenario:\n  - after_requests: 1\n    inject: { type: reorg, depth: 2, head: 100 }\n",
    )
    .await;
    // Request 1: receipt hidden by the fault.
    assert_eq!(
        result(&proxy, "eth_getTransactionReceipt", json!([tx_hash(100)])).await,
        Value::Null
    );
    // Request 2 triggers the reorg over recorded data.
    let block = result(&proxy, "eth_getBlockByNumber", json!(["0x64", false])).await;
    assert_ne!(block["hash"], json!(block_hash(100)));
    let receipt = result(&proxy, "eth_getTransactionReceipt", json!([tx_hash(100)])).await;
    assert_eq!(
        receipt["blockHash"], block["hash"],
        "recorded receipt follows the synthetic branch"
    );
    let unaffected = rpc(&proxy, "eth_blockNumber", json!([])).await;
    assert_eq!(unaffected["result"], "0x64");
}

#[tokio::test]
async fn replay_latency_is_optional() {
    let path = record_session("latency", &[]).await;
    let proxy = spawn_proxy(ProxyConfig {
        upstream: UpstreamConfig::Replay {
            recording: Recording::load(&path).unwrap(),
            replay_latency: true,
        },
        upstream_ws: None,
        faults: FaultConfig::default(),
        record: None,
    })
    .await;
    let started = std::time::Instant::now();
    result(&proxy, "eth_blockNumber", json!([])).await;
    assert!(started.elapsed() < Duration::from_secs(2));
}
