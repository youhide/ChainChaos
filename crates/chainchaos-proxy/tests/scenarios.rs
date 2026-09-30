//! Phase 2: scenario engine (windows, triggers, seeded probability).

mod common;

use std::time::Duration;

use common::{post_raw, setup};

async fn faulted(proxy: &str) -> bool {
    post_raw(proxy, r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#)
        .await
        .headers()
        .contains_key("x-chainchaos-faults")
}

#[tokio::test]
async fn request_count_windows_are_exact() {
    let (_, proxy) = setup(
        "scenario:\n  - after_requests: 3\n    for_requests: 2\n    inject: { type: http_error, status: 503 }\n",
    )
    .await;
    let mut observed = Vec::new();
    for _ in 0..7 {
        observed.push(faulted(&proxy).await);
    }
    assert_eq!(observed, [false, false, false, true, true, false, false]);
}

#[tokio::test]
async fn time_windows_open_and_close() {
    let (_, proxy) = setup(
        "scenario:\n  - after: 300ms\n    for: 400ms\n    inject: { type: http_error, status: 429 }\n",
    )
    .await;
    assert!(!faulted(&proxy).await, "before the window");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(faulted(&proxy).await, "inside the window");
    tokio::time::sleep(Duration::from_millis(450)).await;
    assert!(!faulted(&proxy).await, "after the window");
}

#[tokio::test]
async fn same_seed_reproduces_the_same_faults() {
    let yaml = "seed: 1234\nfaults:\n  - { type: http_error, status: 503, probability: 0.4 }\n";
    let run = |yaml: String| async move {
        let (_, proxy) = setup(&yaml).await;
        let mut pattern = Vec::new();
        for _ in 0..40 {
            pattern.push(faulted(&proxy).await);
        }
        pattern
    };
    let first = run(yaml.to_owned()).await;
    let second = run(yaml.to_owned()).await;
    assert_eq!(first, second);
    assert!(
        first.iter().any(|f| *f) && first.iter().any(|f| !*f),
        "{first:?}"
    );
    let other = run(yaml.replace("1234", "4321")).await;
    assert_ne!(first, other);
}

#[tokio::test]
async fn ordered_steps_compose() {
    let (_, proxy) = setup(
        r#"
scenario:
  - after_requests: 0
    for_requests: 2
    inject: { type: http_error, status: 429 }
  - after_requests: 2
    for_requests: 1
    inject: { type: http_error, status: 503 }
"#,
    )
    .await;
    let mut statuses = Vec::new();
    for _ in 0..4 {
        let status = post_raw(&proxy, r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#)
            .await
            .status()
            .as_u16();
        statuses.push(status);
    }
    assert_eq!(statuses, [429, 429, 503, 200]);
}
