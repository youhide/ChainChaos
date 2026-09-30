//! End-to-end tests: client -> chainchaos -> fake upstream.
//!
//! The fake upstream is a tiny axum server with canned, deliberately oddly
//! formatted responses, so byte-for-byte transparency can be asserted without
//! running a real node.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use chainchaos_core::FaultConfig;
use chainchaos_proxy::ProxyConfig;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use url::Url;

async fn fake_upstream(headers: HeaderMap, body: Bytes) -> Response {
    let Ok(request) = serde_json::from_slice::<Value>(&body) else {
        let echo = format!("upstream got: {}", String::from_utf8_lossy(&body));
        return (StatusCode::BAD_REQUEST, echo).into_response();
    };
    let mut response = match &request {
        Value::Array(calls) => {
            let parts: Vec<String> = calls.iter().map(answer).collect();
            (StatusCode::OK, format!("[{}]", parts.join(","))).into_response()
        }
        call if call["method"] == "http_500" => {
            (StatusCode::INTERNAL_SERVER_ERROR, "upstream exploded").into_response()
        }
        call if call["method"] == "slow" => {
            tokio::time::sleep(Duration::from_secs(5)).await;
            (StatusCode::OK, answer(call)).into_response()
        }
        call => (StatusCode::OK, answer(call)).into_response(),
    };
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/json".parse().expect("static header"),
    );
    if let Some(auth) = headers.get(header::AUTHORIZATION) {
        response
            .headers_mut()
            .insert("x-seen-authorization", auth.clone());
    }
    response
}

/// Hand-written JSON (odd key order and spacing) so any re-serialisation by
/// the proxy would be detected.
fn answer(call: &Value) -> String {
    let id = &call["id"];
    match call["method"].as_str() {
        Some("eth_blockNumber") => format!(r#"{{"id": {id},  "jsonrpc":"2.0", "result":"0x10"}}"#),
        Some("eth_chainId") => format!(r#"{{"jsonrpc":"2.0","result":"0x1","id":{id}}}"#),
        Some(method) => format!(
            r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":-32601,"message":"the method {method} does not exist/is not available"}}}}"#
        ),
        None => {
            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid request"}}"#
                .to_owned()
        }
    }
}

async fn spawn_upstream() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().route("/", post(fake_upstream));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn spawn_proxy(upstream: Url, faults_yaml: &str) -> String {
    spawn_proxy_with_timeout(upstream, faults_yaml, Duration::from_secs(10)).await
}

async fn spawn_proxy_with_timeout(
    upstream: Url,
    faults_yaml: &str,
    upstream_timeout: Duration,
) -> String {
    let config = ProxyConfig {
        upstream,
        upstream_timeout,
        faults: FaultConfig::from_yaml(faults_yaml).unwrap(),
    };
    let app = chainchaos_proxy::router(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        chainchaos_proxy::serve(listener, app, std::future::pending())
            .await
            .unwrap()
    });
    format!("http://{addr}/")
}

async fn setup(faults_yaml: &str) -> (String, String) {
    let upstream = format!("http://{}/", spawn_upstream().await);
    let proxy = spawn_proxy(Url::parse(&upstream).unwrap(), faults_yaml).await;
    (upstream, proxy)
}

async fn post_raw(url: &str, body: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(url)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer test-token")
        .body(body.to_owned())
        .send()
        .await
        .unwrap()
}

/// Sends the same request directly and through the proxy; returns
/// (direct, proxied) as (status, content-type, body).
async fn compare(body: &str) -> ((u16, String, String), (u16, String, String)) {
    let (upstream, proxy) = setup("{}").await;
    let direct = snapshot(post_raw(&upstream, body).await).await;
    let proxied = snapshot(post_raw(&proxy, body).await).await;
    (direct, proxied)
}

async fn snapshot(response: reqwest::Response) -> (u16, String, String) {
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    (status, content_type, response.text().await.unwrap())
}

#[tokio::test]
async fn forwards_requests_transparently() {
    let (direct, proxied) =
        compare(r#"{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}"#).await;
    assert_eq!(direct, proxied);
    assert_eq!(proxied.2, r#"{"id": 1,  "jsonrpc":"2.0", "result":"0x10"}"#);
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
    let body = r#"[{"jsonrpc":"2.0","id":"a","method":"eth_chainId"},{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber"}]"#;
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

    let proxy = spawn_proxy(Url::parse(&format!("http://{dead_addr}/")).unwrap(), "{}").await;
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
    let upstream = format!("http://{}/", spawn_upstream().await);
    let proxy = spawn_proxy_with_timeout(
        Url::parse(&upstream).unwrap(),
        "{}",
        Duration::from_millis(200),
    )
    .await;
    let response = post_raw(&proxy, r#"{"jsonrpc":"2.0","id":9,"method":"slow"}"#).await;
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["id"], 9);
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
        r#"{"id": 1,  "jsonrpc":"2.0", "result":"0x10"}"#,
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
