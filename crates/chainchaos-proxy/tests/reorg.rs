//! Phase 4: reorg simulation, exercised the way an indexer would.

mod common;

use common::{HEAD, block_hash, result, rpc, setup, tx_hash};
use serde_json::{Value, json};

fn hex(n: u64) -> String {
    format!("{n:#x}")
}

async fn block(proxy: &str, n: u64) -> Value {
    result(proxy, "eth_getBlockByNumber", json!([hex(n), false])).await
}

#[tokio::test]
async fn indexer_observes_a_depth_two_reorg() {
    // The reorg happens on the 4th request.
    let (_, proxy) =
        setup("scenario:\n  - after_requests: 3\n    inject: { type: reorg, depth: 2 }\n").await;

    // Indexer syncs blocks 98..=100 (requests 1-3).
    let before: Vec<Value> = vec![
        block(&proxy, 98).await,
        block(&proxy, 99).await,
        block(&proxy, 100).await,
    ];
    assert_eq!(before[2]["hash"], json!(block_hash(100)));

    // Request 4 triggers the reorg; the indexer re-checks its tip.
    let after: Vec<Value> = vec![
        block(&proxy, 98).await,
        block(&proxy, 99).await,
        block(&proxy, 100).await,
    ];
    assert_eq!(after[0], before[0], "block below the reorg is unchanged");
    assert_ne!(after[1]["hash"], before[1]["hash"], "block 99 replaced");
    assert_ne!(after[2]["hash"], before[2]["hash"], "block 100 replaced");
    assert_eq!(
        after[1]["parentHash"], before[0]["hash"],
        "new branch forks from block 98"
    );
    assert_eq!(
        after[2]["parentHash"], after[1]["hash"],
        "new branch is linked"
    );

    // Old hashes are no longer canonical.
    assert_eq!(
        result(
            &proxy,
            "eth_getBlockByHash",
            json!([block_hash(100), false])
        )
        .await,
        Value::Null
    );
    // New hashes resolve.
    let by_new_hash = result(
        &proxy,
        "eth_getBlockByHash",
        json!([after[2]["hash"], false]),
    )
    .await;
    assert_eq!(by_new_hash["hash"], after[2]["hash"]);
    assert_eq!(by_new_hash["number"], json!(hex(100)));

    // Logs and receipts follow the new branch.
    let logs = result(
        &proxy,
        "eth_getLogs",
        json!([{"fromBlock": hex(98), "toBlock": hex(100)}]),
    )
    .await;
    for log in logs.as_array().unwrap() {
        let n = u64::from_str_radix(
            log["blockNumber"]
                .as_str()
                .unwrap()
                .trim_start_matches("0x"),
            16,
        )
        .unwrap();
        let expected = &after[(n - 98) as usize]["hash"];
        assert_eq!(&log["blockHash"], expected, "log in block {n}");
    }
    let receipt = result(&proxy, "eth_getTransactionReceipt", json!([tx_hash(99)])).await;
    assert_eq!(receipt["blockHash"], after[1]["hash"]);
    assert_eq!(receipt["logs"][0]["blockHash"], after[1]["hash"]);

    // Logs queried by an old block hash fail like on a real node.
    let old = rpc(
        &proxy,
        "eth_getLogs",
        json!([{"blockHash": block_hash(99)}]),
    )
    .await;
    assert_eq!(old["error"]["message"], "unknown block");
    let new = result(
        &proxy,
        "eth_getLogs",
        json!([{"blockHash": after[1]["hash"]}]),
    )
    .await;
    assert_eq!(new.as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn dropped_transactions_lose_receipts_and_logs() {
    let (_, proxy) = setup(
        "scenario:\n  - after_requests: 0\n    inject: { type: reorg, depth: 1, transactions: drop }\n",
    )
    .await;
    // The first request triggers the reorg.
    assert_eq!(
        result(&proxy, "eth_getTransactionReceipt", json!([tx_hash(HEAD)])).await,
        Value::Null
    );
    assert_eq!(
        result(
            &proxy,
            "eth_getTransactionReceipt",
            json!([tx_hash(HEAD - 1)])
        )
        .await["status"],
        "0x1"
    );
    let tx = result(&proxy, "eth_getTransactionByHash", json!([tx_hash(HEAD)])).await;
    assert_eq!(
        tx["blockNumber"],
        Value::Null,
        "transaction is pending again"
    );
    let logs = result(
        &proxy,
        "eth_getLogs",
        json!([{"fromBlock": hex(HEAD - 1), "toBlock": hex(HEAD)}]),
    )
    .await;
    assert_eq!(
        logs.as_array().unwrap().len(),
        2,
        "only block 99's logs remain"
    );
    assert_eq!(block(&proxy, HEAD).await["transactions"], json!([]));
}

#[tokio::test]
async fn reorg_is_deterministic_with_seed() {
    let yaml = "seed: 9\nscenario:\n  - after_requests: 0\n    inject: { type: reorg, depth: 3, head: 100 }\n";
    let (_, a) = setup(yaml).await;
    let (_, b) = setup(yaml).await;
    assert_eq!(block(&a, 99).await, block(&b, 99).await);
    let (_, c) = setup(&yaml.replace("seed: 9", "seed: 10")).await;
    assert_ne!(block(&a, 99).await["hash"], block(&c, 99).await["hash"]);
}

#[tokio::test]
async fn repeated_reorgs_replace_the_branch_again() {
    let (_, proxy) = setup(
        "scenario:\n  - after_requests: 0\n    inject: { type: reorg, depth: 1 }\n  - after_requests: 1\n    inject: { type: reorg, depth: 1 }\n",
    )
    .await;
    let first = block(&proxy, HEAD).await; // request 1: first reorg
    let second = block(&proxy, HEAD).await; // request 2: second reorg
    assert_ne!(first["hash"], json!(block_hash(HEAD)));
    assert_ne!(second["hash"], first["hash"]);
    // The first synthetic branch is gone too.
    assert_eq!(
        result(&proxy, "eth_getBlockByHash", json!([first["hash"], false])).await,
        Value::Null
    );
}
