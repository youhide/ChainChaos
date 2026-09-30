//! Phase 0: transparent forwarding.

mod common;

use std::time::{Duration, Instant};

use axum::http::{StatusCode, header};
use chainchaos_proxy::{Proxy, ProxyConfig, ProxyError, UpstreamConfig};
use common::{post_raw, setup, spawn_node, spawn_proxy};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use url::Url;

async fn snapshot(response: reqwest::Response) -> (u16, String, String) {
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    (status, content_type, response.text().await.unwrap())
}

/// Sends the same request directly and through a transparent proxy.
async fn compare(body: &str) -> ((u16, String, String), (u16, String, String)) {
    let (node, proxy) = setup("{}").await;
    let direct = snapshot(post_raw(node.http.as_str(), body).await).await;
    let proxied = snapshot(post_raw(&proxy, body).await).await;
    (direct, proxied)
}

#[tokio::test]
async fn forwards_requests_transparently() {
    let (direct, proxied) =
        compare(r#"{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}"#).await;
    assert_eq!(direct, proxied);
    assert_eq!(proxied.2, r#"{"id": 1,  "jsonrpc":"2.0", "result":"0x64"}"#);
}

#[tokio::test]
async fn forwards_request_headers() {
    let (_, proxy) = setup("{}").await;
    let response = post_raw(&proxy, r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#).await;
    assert_eq!(
        response.headers()["x-seen-authorization"],
        "Bearer test-token"
    );
    assert!(response.headers().get("x-chainchaos-faults").is_none());
}

#[tokio::test]
async fn preserves_json_rpc_ids() {
    let (_, proxy) = setup("{}").await;
    for id in [
        json!(42),
        json!("req-abc"),
        json!(null),
        json!(18446744073709551615u64),
    ] {
        let body = json!({"jsonrpc": "2.0", "id": id, "method": "eth_chainId"}).to_string();
        let response: Value = post_raw(&proxy, &body).await.json().await.unwrap();
        assert_eq!(response["id"], id, "id {id} was not preserved");
    }
}

#[tokio::test]
async fn preserves_batch_ids_and_order() {
    let body = r#"[{"jsonrpc":"2.0","id":"a","method":"eth_chainId"},{"jsonrpc":"2.0","id":7,"method":"eth_getTransactionCount","params":["0x1","latest"]}]"#;
    let (direct, proxied) = compare(body).await;
    assert_eq!(direct, proxied);
    let parsed: Value = serde_json::from_str(&proxied.2).unwrap();
    assert_eq!(parsed[0]["id"], "a");
    assert_eq!(parsed[1]["id"], 7);
}

#[tokio::test]
async fn preserves_json_rpc_errors() {
    let (direct, proxied) =
        compare(r#"{"jsonrpc":"2.0","id":3,"method":"eth_doesNotExist","params":[]}"#).await;
    assert_eq!(direct, proxied);
    let parsed: Value = serde_json::from_str(&proxied.2).unwrap();
    assert_eq!(parsed["error"]["code"], -32601);
    assert_eq!(parsed["id"], 3);
}

#[tokio::test]
async fn preserves_upstream_http_errors() {
    let (direct, proxied) = compare(r#"{"jsonrpc":"2.0","id":1,"method":"http_500"}"#).await;
    assert_eq!(direct, proxied);
    assert_eq!(proxied.0, 500);
    assert_eq!(proxied.2, "upstream exploded");
}

#[tokio::test]
async fn forwards_non_json_bodies_unchanged() {
    // The proxy must not judge the payload; the upstream decides.
    let (direct, proxied) = compare("not json").await;
    assert_eq!(direct, proxied);
    assert_eq!(proxied.0, 400);
    assert_eq!(proxied.2, "upstream got: not json");
}

#[tokio::test]
async fn unreachable_upstream_returns_json_rpc_error() {
    // Bind then drop a listener to get a port nothing is listening on.
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead.local_addr().unwrap();
    drop(dead);

    let proxy = spawn_proxy(ProxyConfig::http(
        Url::parse(&format!("http://{dead_addr}/")).unwrap(),
    ))
    .await;
    let response = post_raw(
        &proxy,
        r#"{"jsonrpc":"2.0","id":"x1","method":"eth_chainId"}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(response.headers()["x-chainchaos-error"], "upstream");
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["id"], "x1");
    assert_eq!(body["error"]["code"], -32000);
}

#[tokio::test]
async fn upstream_timeout_returns_gateway_timeout() {
    let node = spawn_node().await;
    let proxy = spawn_proxy(ProxyConfig {
        upstream: UpstreamConfig::Http {
            url: node.http.clone(),
            timeout: Duration::from_millis(200),
        },
        ..ProxyConfig::http(node.http.clone())
    })
    .await;
    let response = post_raw(&proxy, r#"{"jsonrpc":"2.0","id":9,"method":"slow"}"#).await;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["id"], 9);
}

#[test]
fn rejects_invalid_upstreams() {
    let config = ProxyConfig::http(Url::parse("ws://127.0.0.1:8546").unwrap());
    assert!(matches!(
        Proxy::new(config),
        Err(ProxyError::UnsupportedScheme(_))
    ));
    let config = ProxyConfig {
        upstream_ws: Some(Url::parse("http://127.0.0.1:8546").unwrap()),
        ..ProxyConfig::http(Url::parse("http://127.0.0.1:8545").unwrap())
    };
    assert!(matches!(
        Proxy::new(config),
        Err(ProxyError::UnsupportedWsScheme(_))
    ));
}

#[tokio::test]
async fn delay_fault_delays_matching_requests() {
    const DELAY: Duration = Duration::from_millis(400);
    let (_, proxy) =
        setup("faults:\n  - type: delay\n    method: eth_blockNumber\n    duration: 400ms\n").await;

    let started = Instant::now();
    let response = post_raw(
        &proxy,
        r#"{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}"#,
    )
    .await;
    let elapsed = started.elapsed();
    assert_eq!(response.headers()["x-chainchaos-faults"], "delay");
    assert_eq!(
        response.text().await.unwrap(),
        r#"{"id": 1,  "jsonrpc":"2.0", "result":"0x64"}"#,
        "delay must not alter the response"
    );
    assert!(elapsed >= DELAY, "expected >= {DELAY:?}, got {elapsed:?}");

    let started = Instant::now();
    let response = post_raw(&proxy, r#"{"jsonrpc":"2.0","id":2,"method":"eth_chainId"}"#).await;
    assert!(response.headers().get("x-chainchaos-faults").is_none());
    assert!(
        started.elapsed() < DELAY,
        "non-matching method should not be delayed"
    );
}

#[tokio::test]
async fn delay_fault_respects_count() {
    let (_, proxy) = setup("faults:\n  - type: delay\n    duration: 50ms\n    count: 1\n").await;
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#;
    let first = post_raw(&proxy, body).await;
    assert_eq!(first.headers()["x-chainchaos-faults"], "delay");
    let second = post_raw(&proxy, body).await;
    assert!(second.headers().get("x-chainchaos-faults").is_none());
}

#[tokio::test]
async fn empty_config_is_transparent_for_every_method() {
    let (node, proxy) = setup("{}").await;
    for body in [
        r#"{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",true]}"#,
        r#"{"jsonrpc":"2.0","id":2,"method":"eth_getLogs","params":[{"fromBlock":"0x60"}]}"#,
        r#"{"jsonrpc":"2.0","id":3,"method":"eth_getTransactionReceipt","params":["0x000000000000000000000000000000000000000000000000000000007a000050"]}"#,
    ] {
        let direct = post_raw(node.http.as_str(), body)
            .await
            .text()
            .await
            .unwrap();
        let proxied = post_raw(&proxy, body).await.text().await.unwrap();
        assert_eq!(direct, proxied);
    }
}
