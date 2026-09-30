//! Phase 1: timeout, http_error, receipt_null, transaction_submit_timeout.

mod common;

use std::time::{Duration, Instant};

use axum::http::StatusCode;
use common::{post_raw, result, rpc, setup, tx_hash};
use serde_json::{Value, json};

fn client_with_timeout(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder().timeout(timeout).build().unwrap()
}

#[tokio::test]
async fn timeout_without_forwarding_makes_client_time_out() {
    let (node, proxy) =
        setup("faults:\n  - { type: timeout, method: eth_chainId, duration: 2s }\n").await;
    let err = client_with_timeout(Duration::from_millis(200))
        .post(&proxy)
        .header("content-type", "application/json")
        .body(r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#)
        .send()
        .await
        .unwrap_err();
    assert!(err.is_timeout(), "{err}");
    assert_eq!(
        node.chain
            .requests
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn timeout_eventually_answers_504_with_request_id() {
    let (node, proxy) =
        setup("faults:\n  - { type: timeout, duration: 150ms, forward: true }\n").await;
    let started = Instant::now();
    let response = post_raw(
        &proxy,
        r#"{"jsonrpc":"2.0","id":"t1","method":"eth_chainId"}"#,
    )
    .await;
    assert!(started.elapsed() >= Duration::from_millis(150));
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(response.headers()["x-chainchaos-faults"], "timeout");
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["id"], "t1");
    assert_eq!(body["error"]["code"], -32000);
    assert_eq!(
        node.chain
            .requests
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "forward: true reaches the upstream"
    );
}

#[tokio::test]
async fn http_error_returns_configured_status_without_forwarding() {
    let (node, proxy) =
        setup("faults:\n  - { type: http_error, status: 429, retry_after: 2, count: 1 }\n").await;
    let response = post_raw(&proxy, r#"{"jsonrpc":"2.0","id":5,"method":"eth_chainId"}"#).await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["retry-after"], "2");
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["id"], 5);
    assert_eq!(body["error"]["code"], -32005);
    assert_eq!(
        node.chain
            .requests
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    // count: 1 -> the next request goes through.
    assert_eq!(result(&proxy, "eth_chainId", json!([])).await, json!("0x1"));
}

#[tokio::test]
async fn http_error_supports_every_common_status_and_custom_bodies() {
    for status in [500u16, 502, 503] {
        let (_, proxy) = setup(&format!(
            "faults:\n  - {{ type: http_error, status: {status} }}\n"
        ))
        .await;
        let response = post_raw(&proxy, r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#).await;
        assert_eq!(response.status().as_u16(), status);
    }
    let (_, proxy) =
        setup("faults:\n  - { type: http_error, status: 503, body: 'upstream connect error' }\n")
            .await;
    let response = post_raw(&proxy, r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#).await;
    assert_eq!(
        response.headers()["content-type"],
        "text/plain; charset=utf-8"
    );
    assert_eq!(response.text().await.unwrap(), "upstream connect error");
}

#[tokio::test]
async fn receipt_null_hides_existing_receipts_temporarily() {
    let (node, proxy) = setup("faults:\n  - { type: receipt_null, count: 2 }\n").await;
    let hash = tx_hash(50);
    assert_eq!(
        result(
            node.http.as_str(),
            "eth_getTransactionReceipt",
            json!([hash])
        )
        .await["status"],
        "0x1"
    );
    for _ in 0..2 {
        assert_eq!(
            result(&proxy, "eth_getTransactionReceipt", json!([hash])).await,
            Value::Null
        );
    }
    assert_eq!(
        result(&proxy, "eth_getTransactionReceipt", json!([hash])).await["status"],
        "0x1"
    );
    // Other methods are unaffected even while active.
    let (_, proxy) = setup("faults:\n  - { type: receipt_null }\n").await;
    assert_eq!(result(&proxy, "eth_chainId", json!([])).await, json!("0x1"));
}

#[tokio::test]
async fn receipt_null_only_touches_receipt_calls_in_a_batch() {
    let (_, proxy) = setup("faults:\n  - { type: receipt_null }\n").await;
    let body = json!([
        {"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":[tx_hash(10)]},
        {"jsonrpc":"2.0","id":2,"method":"eth_getTransactionByHash","params":[tx_hash(10)]},
    ])
    .to_string();
    let response: Value = post_raw(&proxy, &body).await.json().await.unwrap();
    assert_eq!(response[0]["id"], 1);
    assert_eq!(response[0]["result"], Value::Null);
    assert_eq!(response[1]["result"]["hash"], json!(tx_hash(10)));
}

#[tokio::test]
async fn transaction_submit_timeout_forwards_then_times_out() {
    let (node, proxy) =
        setup("faults:\n  - { type: transaction_submit_timeout, duration: 1s }\n").await;
    let err = client_with_timeout(Duration::from_millis(300))
        .post(&proxy)
        .header("content-type", "application/json")
        .body(r#"{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x02f8aa"]}"#)
        .send()
        .await
        .unwrap_err();
    assert!(err.is_timeout(), "{err}");
    // The transaction reached the node even though the client saw a timeout:
    // the ambiguous state an application must handle.
    assert_eq!(node.chain.sent(), 1);
    assert_eq!(node.chain.sent_transactions.lock().unwrap()[0], "0x02f8aa");

    // Reads are untouched.
    assert_eq!(result(&proxy, "eth_chainId", json!([])).await, json!("0x1"));
}

#[tokio::test]
async fn faults_can_be_combined() {
    let (_, proxy) = setup(
        "faults:\n  - { type: delay, method: eth_getTransactionReceipt, duration: 100ms }\n  - { type: receipt_null, count: 1 }\n",
    )
    .await;
    let started = Instant::now();
    let response = post_raw(
        &proxy,
        &json!({"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":[tx_hash(1)]})
            .to_string(),
    )
    .await;
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert_eq!(
        response.headers()["x-chainchaos-faults"],
        "delay,receipt_null"
    );
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["result"], Value::Null);
    let again = rpc(&proxy, "eth_getTransactionReceipt", json!([tx_hash(1)])).await;
    assert_eq!(again["result"]["status"], "0x1");
}
