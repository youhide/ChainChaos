//! The example indexer survives a reorg injected by chainchaos on top of a
//! real Anvil node. Skipped when `anvil` is not on PATH, unless
//! `CHAINCHAOS_REQUIRE_ANVIL=1`.

use std::net::TcpListener as StdListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use chainchaos_core::FaultConfig;
use chainchaos_example_indexer::Indexer;
use chainchaos_proxy::{Proxy, ProxyConfig, UpstreamConfig};
use serde_json::{Value, json};
use url::Url;

struct Anvil(Child);

impl Drop for Anvil {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn call(url: &str, method: &str, params: Value) -> Value {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let response: Value = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    response["result"].clone()
}

async fn start_anvil() -> Option<(Anvil, String)> {
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
    let anvil = Anvil(child);
    let url = format!("http://127.0.0.1:{port}/");
    let deadline = Instant::now() + Duration::from_secs(20);
    while reqwest::Client::new()
        .post(&url)
        .body("{}")
        .send()
        .await
        .is_err()
    {
        assert!(Instant::now() < deadline, "anvil did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Some((anvil, url))
}

#[tokio::test]
async fn indexer_rolls_back_and_reindexes_after_a_reorg() {
    let Some((_anvil, node)) = start_anvil().await else {
        return;
    };

    // Chain: blocks 1-7 empty, block 8 holds a contract creation that emits a
    // log, blocks 9-10 empty. Head = 10.
    call(&node, "anvil_mine", json!(["0x7"])).await;
    let from = call(&node, "eth_accounts", json!([])).await[0].clone();
    // PUSH1 0, PUSH1 0, LOG0, STOP: a constructor that emits one log.
    let tx = call(
        &node,
        "eth_sendTransaction",
        json!([{"from": from, "data": "0x60006000a000"}]),
    )
    .await;
    // Automine seals the block asynchronously; wait for it.
    let deadline = Instant::now() + Duration::from_secs(5);
    while call(&node, "eth_getTransactionReceipt", json!([tx]))
        .await
        .is_null()
    {
        assert!(Instant::now() < deadline, "transaction was not mined");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    call(&node, "anvil_mine", json!(["0x2"])).await;
    assert_eq!(
        call(&node, "eth_blockNumber", json!([])).await,
        json!("0xa")
    );

    // chainchaos replaces blocks 8-10 at the start of the indexer's second
    // sync (its first eth_blockNumber after request 1).
    let scenario = "seed: 1\nscenario:\n  - after_requests: 1\n    inject: { type: reorg, depth: 3, method: eth_blockNumber }\n";
    let proxy = Proxy::new(ProxyConfig {
        upstream: UpstreamConfig::Http {
            url: Url::parse(&node).unwrap(),
            timeout: Duration::from_secs(10),
        },
        upstream_ws: None,
        faults: FaultConfig::from_yaml(scenario).unwrap(),
        record: None,
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(proxy.serve(listener, std::future::pending()));

    let mut indexer = Indexer::new(&proxy_url);
    let first = indexer.sync().await.unwrap();
    assert_eq!(first.indexed, 11, "blocks 0-10");
    assert_eq!(indexer.reorgs_detected, 0);
    let before = indexer.blocks().clone();
    assert_eq!(
        before[&8].log_block_hashes.len(),
        1,
        "block 8 has the contract's log"
    );

    let second = indexer.sync().await.unwrap();
    assert_eq!(indexer.reorgs_detected, 1, "the tip check caught the reorg");
    assert_eq!(second.rolled_back, 3, "blocks 8-10 were rolled back");
    assert_eq!(second.indexed, 3, "and re-indexed");

    let after = indexer.blocks();
    assert_eq!(after[&7], before[&7], "the common ancestor is untouched");
    for n in 8..=10 {
        assert_ne!(
            after[&n].hash, before[&n].hash,
            "block {n} is on the new branch"
        );
        let canonical = call(
            &proxy_url,
            "eth_getBlockByNumber",
            json!([format!("{n:#x}"), false]),
        )
        .await;
        assert_eq!(canonical["hash"], json!(after[&n].hash));
    }
    assert_eq!(after[&8].parent_hash, after[&7].hash);
    assert_eq!(after[&9].parent_hash, after[&8].hash);
    assert_eq!(
        after[&8].log_block_hashes,
        vec![after[&8].hash.clone()],
        "the re-indexed log points at the new block"
    );

    // A third sync finds nothing new and no reorg.
    let third = indexer.sync().await.unwrap();
    assert_eq!(third, Default::default());
    assert_eq!(indexer.reorgs_detected, 1);
}
