//! Prometheus metrics, served at `GET /metrics` in the text exposition
//! format. Hand-rolled: a handful of counters does not justify a metrics
//! dependency.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use chainchaos_core::InjectedFault;

use crate::AppState;

#[derive(Debug, Default)]
pub(crate) struct Metrics {
    /// Fired faults by (fault type, rule label).
    faults: Mutex<BTreeMap<(&'static str, String), u64>>,
    upstream_errors: AtomicU64,
}

impl Metrics {
    pub fn faults_planned(&self, faults: &[InjectedFault]) {
        if faults.is_empty() {
            return;
        }
        let mut counts = self.faults.lock().unwrap_or_else(|e| e.into_inner());
        for f in faults {
            *counts.entry((f.fault.name(), f.label.clone())).or_insert(0) += 1;
        }
    }

    pub fn upstream_error(&self) {
        self.upstream_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn fault_counts(&self) -> Vec<((&'static str, String), u64)> {
        let counts = self.faults.lock().unwrap_or_else(|e| e.into_inner());
        counts.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }
}

pub(crate) async fn handle(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        render(&state),
    )
}

fn render(state: &AppState) -> String {
    let mut out = String::new();
    let mut metric = |name: &str, kind: &str, help: &str, samples: &[(String, f64)]| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        for (labels, value) in samples {
            let _ = writeln!(out, "{name}{labels} {value}");
        }
    };
    let counter = |v: u64| vec![(String::new(), v as f64)];

    metric(
        "chainchaos_build_info",
        "gauge",
        "Build information.",
        &[(
            format!("{{version=\"{}\"}}", env!("CARGO_PKG_VERSION")),
            1.0,
        )],
    );
    metric(
        "chainchaos_uptime_seconds",
        "gauge",
        "Seconds since the proxy started.",
        &[(String::new(), state.started.elapsed().as_secs_f64())],
    );
    metric(
        "chainchaos_requests_total",
        "counter",
        "JSON-RPC requests received (HTTP and WebSocket).",
        &counter(state.requests.load(Ordering::Relaxed)),
    );
    metric(
        "chainchaos_ws_connections_total",
        "counter",
        "WebSocket connections accepted.",
        &counter(state.ws_connections.load(Ordering::Relaxed)),
    );
    metric(
        "chainchaos_ws_notifications_total",
        "counter",
        "Subscription notifications received from the upstream.",
        &counter(state.ws_messages.load(Ordering::Relaxed)),
    );
    metric(
        "chainchaos_upstream_errors_total",
        "counter",
        "Requests that failed to reach the upstream or timed out.",
        &counter(state.metrics.upstream_errors.load(Ordering::Relaxed)),
    );
    metric(
        "chainchaos_reorgs_total",
        "counter",
        "Simulated reorgs applied.",
        &counter(state.chain.view().events().len() as u64),
    );
    let faults: Vec<(String, f64)> = state
        .metrics
        .fault_counts()
        .into_iter()
        .map(|((fault, rule), n)| {
            (
                format!("{{fault=\"{}\",rule=\"{}\"}}", escape(fault), escape(&rule)),
                n as f64,
            )
        })
        .collect();
    metric(
        "chainchaos_faults_total",
        "counter",
        "Faults fired, by fault type and rule.",
        &faults,
    );
    out
}

/// Escapes a Prometheus label value.
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_label_values() {
        assert_eq!(escape(r#"a"b\c"#), r#"a\"b\\c"#);
        assert_eq!(escape("faults[0]"), "faults[0]");
    }
}
