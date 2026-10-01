//! Prometheus metrics endpoint.

mod common;

use common::{result, setup, tx_hash};
use serde_json::json;

#[tokio::test]
async fn exposes_request_and_fault_counters() {
    let (_, proxy) = setup("faults:\n  - { type: receipt_null, count: 2 }\n").await;
    for _ in 0..3 {
        result(&proxy, "eth_getTransactionReceipt", json!([tx_hash(1)])).await;
    }
    result(&proxy, "eth_chainId", json!([])).await;

    let response = reqwest::get(format!("{proxy}metrics")).await.unwrap();
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/plain")
    );
    let text = response.text().await.unwrap();
    assert!(
        text.contains("# TYPE chainchaos_requests_total counter"),
        "{text}"
    );
    assert!(text.contains("chainchaos_requests_total 4"), "{text}");
    assert!(
        text.contains(r#"chainchaos_faults_total{fault="receipt_null",rule="faults[0]"} 2"#),
        "{text}"
    );
    assert!(text.contains("chainchaos_reorgs_total 0"));
    assert!(text.contains(&format!(
        r#"chainchaos_build_info{{version="{}"}} 1"#,
        env!("CARGO_PKG_VERSION")
    )));
}
