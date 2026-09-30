//! Phase 3: blockchain-aware faults.

mod common;

use common::{HEAD, block_hash, post_raw, result, rpc, setup, tx_hash};
use serde_json::{Value, json};

fn number(v: &Value) -> u64 {
    u64::from_str_radix(v.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
}

#[tokio::test]
async fn stale_head_gives_a_coherent_lagging_view() {
    let (_, proxy) = setup("faults:\n  - { type: stale_head, lag: 3 }\n").await;
    let visible = HEAD - 3;
    assert_eq!(
        number(&result(&proxy, "eth_blockNumber", json!([])).await),
        visible
    );

    let latest = result(&proxy, "eth_getBlockByNumber", json!(["latest", false])).await;
    assert_eq!(
        number(&latest["number"]),
        visible,
        "`latest` resolves to the stale head"
    );
    assert_eq!(
        result(
            &proxy,
            "eth_getBlockByNumber",
            json!([format!("{:#x}", HEAD), false])
        )
        .await,
        Value::Null,
        "blocks above the stale head do not exist yet"
    );
    assert_eq!(
        result(
            &proxy,
            "eth_getBlockByHash",
            json!([block_hash(HEAD), false])
        )
        .await,
        Value::Null
    );

    let logs = result(&proxy, "eth_getLogs", json!([{"fromBlock": "0x60"}])).await;
    let max = logs
        .as_array()
        .unwrap()
        .iter()
        .map(|l| number(&l["blockNumber"]))
        .max()
        .unwrap();
    assert_eq!(max, visible);

    assert_eq!(
        result(&proxy, "eth_getTransactionReceipt", json!([tx_hash(HEAD)])).await,
        Value::Null
    );
    assert_eq!(
        result(
            &proxy,
            "eth_getTransactionReceipt",
            json!([tx_hash(visible)])
        )
        .await["status"],
        "0x1"
    );
    let pending = result(&proxy, "eth_getTransactionByHash", json!([tx_hash(HEAD)])).await;
    assert_eq!(
        pending["blockHash"],
        Value::Null,
        "mined above the stale head -> still pending"
    );
}

#[tokio::test]
async fn inconsistent_head_reproduces_lagging_backends() {
    let (_, proxy) = setup("faults:\n  - { type: inconsistent_head, lag: 2 }\n").await;
    // The head is reported correctly...
    let head = number(&result(&proxy, "eth_blockNumber", json!([])).await);
    assert_eq!(head, HEAD);
    // ...but fetching that block fails, as if served by a lagging node.
    assert_eq!(
        result(
            &proxy,
            "eth_getBlockByNumber",
            json!([format!("{head:#x}"), false])
        )
        .await,
        Value::Null
    );
    let logs = result(
        &proxy,
        "eth_getLogs",
        json!([{"fromBlock": format!("{:#x}", HEAD - 3), "toBlock": format!("{head:#x}")}]),
    )
    .await;
    assert!(
        logs.as_array()
            .unwrap()
            .iter()
            .all(|l| number(&l["blockNumber"]) <= HEAD - 2)
    );
}

#[tokio::test]
async fn log_faults_change_presence_multiplicity_and_order() {
    let range = json!([{"fromBlock": "0x5a", "toBlock": "0x5f"}]); // 6 blocks, 12 logs
    let (node, proxy) = setup("seed: 5\nfaults:\n  - { type: missing_logs, ratio: 0.5 }\n").await;
    let real = result(node.http.as_str(), "eth_getLogs", range.clone()).await;
    let missing = result(&proxy, "eth_getLogs", range.clone()).await;
    assert!(missing.as_array().unwrap().len() < real.as_array().unwrap().len());
    assert!(
        missing
            .as_array()
            .unwrap()
            .iter()
            .all(|l| real.as_array().unwrap().contains(l))
    );

    let (_, proxy) = setup("faults:\n  - { type: duplicated_logs, ratio: 0.2 }\n").await;
    let duplicated = result(&proxy, "eth_getLogs", range.clone()).await;
    assert!(duplicated.as_array().unwrap().len() > real.as_array().unwrap().len());

    let (_, proxy) = setup("faults:\n  - { type: reordered_logs }\n").await;
    let reordered = result(&proxy, "eth_getLogs", range.clone()).await;
    assert_ne!(reordered, real);
    let mut sorted = reordered.as_array().unwrap().clone();
    sorted.sort_by_key(|l| (number(&l["blockNumber"]), number(&l["logIndex"])));
    assert_eq!(Value::Array(sorted), real, "same logs, different order");
}

#[tokio::test]
async fn log_faults_are_reproducible_with_seed() {
    let range = json!([{"fromBlock": "0x50", "toBlock": "0x5f"}]);
    let yaml = "seed: 77\nfaults:\n  - { type: missing_logs, ratio: 0.3 }\n";
    let (_, a) = setup(yaml).await;
    let (_, b) = setup(yaml).await;
    assert_eq!(
        result(&a, "eth_getLogs", range.clone()).await,
        result(&b, "eth_getLogs", range.clone()).await
    );
}

#[tokio::test]
async fn stale_nonce_lowers_transaction_count() {
    let (_, proxy) = setup("faults:\n  - { type: stale_nonce, lag: 2 }\n").await;
    assert_eq!(
        result(&proxy, "eth_getTransactionCount", json!(["0x1", "latest"])).await,
        json!("0x3")
    );
}

#[tokio::test]
async fn receipt_disappears_after_being_seen() {
    let (_, proxy) = setup("faults:\n  - { type: receipt_disappear, visible_for: 1 }\n").await;
    let hash = tx_hash(42);
    assert_eq!(
        result(&proxy, "eth_getTransactionReceipt", json!([hash])).await["status"],
        "0x1"
    );
    assert_eq!(
        result(&proxy, "eth_getTransactionReceipt", json!([hash])).await,
        Value::Null
    );
    // Tracked per transaction.
    assert_eq!(
        result(&proxy, "eth_getTransactionReceipt", json!([tx_hash(43)])).await["status"],
        "0x1"
    );
}

#[tokio::test]
async fn block_null_hides_blocks() {
    let (_, proxy) =
        setup("faults:\n  - { type: block_null, method: eth_getBlockByNumber, count: 1 }\n").await;
    assert_eq!(
        result(&proxy, "eth_getBlockByNumber", json!(["0x10", false])).await,
        Value::Null
    );
    assert_eq!(
        result(&proxy, "eth_getBlockByNumber", json!(["0x10", false])).await["hash"],
        json!(block_hash(16))
    );
}

#[tokio::test]
async fn mutated_responses_stay_valid_json_rpc() {
    let (_, proxy) = setup(
        "faults:\n  - { type: stale_head, lag: 1 }\n  - { type: duplicated_logs }\n  - { type: stale_nonce }\n",
    )
    .await;
    let body = json!([
        {"jsonrpc":"2.0","id":"a","method":"eth_blockNumber"},
        {"jsonrpc":"2.0","id":"b","method":"eth_getLogs","params":[{"fromBlock":"0x60"}]},
        {"jsonrpc":"2.0","id":"c","method":"eth_getTransactionCount","params":["0x1","latest"]},
        {"jsonrpc":"2.0","id":"d","method":"eth_nope"},
    ])
    .to_string();
    let response: Value = post_raw(&proxy, &body).await.json().await.unwrap();
    let items = response.as_array().unwrap();
    assert_eq!(items.len(), 4);
    for (item, id) in items.iter().zip(["a", "b", "c", "d"]) {
        assert_eq!(item["jsonrpc"], "2.0");
        assert_eq!(item["id"], id);
    }
    assert_eq!(items[0]["result"], json!(format!("{:#x}", HEAD - 1)));
    assert_eq!(
        items[3]["error"]["code"], -32601,
        "errors pass through untouched"
    );
    let unaffected = rpc(&proxy, "eth_chainId", json!([])).await;
    assert_eq!(unaffected["result"], "0x1");
}
