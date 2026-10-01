//! YAML fault and scenario configuration.
//!
//! One file format covers both static rules and timed scenarios:
//!
//! ```yaml
//! seed: 42                 # drives every random decision (default 0)
//!
//! faults:                  # rules active from the start (unless windowed)
//!   - type: delay
//!     method: eth_getLogs
//!     duration: 1500ms
//!     probability: 0.5
//!
//! scenario:                # ordered, timed steps
//!   - after: 20s
//!     for: 10s
//!     inject:
//!       type: http_error
//!       status: 429
//! ```
//!
//! Every rule accepts these common fields next to its fault-specific ones:
//! `method`, `count`, `probability`, `subscription` (WebSocket faults) and the
//! window fields `after`, `after_requests`, `for`, `for_requests`. In a
//! scenario step the window fields sit on the step and the fault in `inject`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use serde_yaml_ng::{Mapping, Value};

use crate::duration::parse_duration;
use crate::fault::{
    End, Fault, FaultKind, FaultRule, MethodMatcher, ReorgTransactions, Start, Window,
};

/// Upper bound for simulated reorg depth. Real reorgs deeper than a few
/// blocks are rare; this mostly guards against typos.
pub const MAX_REORG_DEPTH: u64 = 128;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid config{}: {source}", .path.as_ref().map(|p| format!(" in {}", p.display())).unwrap_or_default())]
    Parse {
        path: Option<PathBuf>,
        #[source]
        source: serde_yaml_ng::Error,
    },
    #[error("invalid rule {label}: {message}")]
    Invalid { label: String, message: String },
}

/// A validated configuration: a seed plus fault rules in evaluation order
/// (`faults` first, then `scenario` steps).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FaultConfig {
    pub seed: u64,
    pub rules: Vec<FaultRule>,
}

impl FaultConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::parse(&text, Some(path))
    }

    pub fn from_yaml(text: &str) -> Result<Self, ConfigError> {
        Self::parse(text, None)
    }

    /// Whether any rule is of the given kind.
    pub fn has_kind(&self, kind: FaultKind) -> bool {
        self.rules.iter().any(|r| r.fault.kind() == kind)
    }

    fn parse(text: &str, path: Option<&Path>) -> Result<Self, ConfigError> {
        let parse_err = |source| ConfigError::Parse {
            path: path.map(Path::to_owned),
            source,
        };
        let raw: RawConfig = serde_yaml_ng::from_str(text).map_err(parse_err)?;

        let mut rules = Vec::new();
        for (index, rule) in raw.faults.into_iter().enumerate() {
            rules.push(parse_rule(format!("faults[{index}]"), rule)?);
        }
        for (index, step) in raw.scenario.into_iter().enumerate() {
            let label = format!("scenario[{index}]");
            let merged = merge_step(&label, step)?;
            rules.push(parse_rule(label, merged)?);
        }
        Ok(Self {
            seed: raw.seed.unwrap_or(0),
            rules,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    seed: Option<u64>,
    #[serde(default)]
    faults: Vec<Mapping>,
    #[serde(default)]
    scenario: Vec<Mapping>,
}

/// Moves a step's window fields into its `inject` mapping so steps and
/// rules share one parser.
fn merge_step(label: &str, mut step: Mapping) -> Result<Mapping, ConfigError> {
    let invalid = |message: String| ConfigError::Invalid {
        label: label.to_owned(),
        message,
    };
    let Some(inject) = step.remove("inject") else {
        return Err(invalid("scenario steps need an `inject:` fault".to_owned()));
    };
    let Value::Mapping(mut inject) = inject else {
        return Err(invalid("`inject` must be a mapping".to_owned()));
    };
    for (key, value) in step {
        let name = key.as_str().unwrap_or_default().to_owned();
        if !WINDOW_FIELDS.contains(&name.as_str()) {
            return Err(invalid(format!(
                "unknown step field `{name}` (expected inject, {})",
                WINDOW_FIELDS.join(", ")
            )));
        }
        if inject.contains_key(&key) {
            return Err(invalid(format!(
                "`{name}` is set on both the step and `inject`"
            )));
        }
        inject.insert(key, value);
    }
    Ok(inject)
}

const WINDOW_FIELDS: &[&str] = &["after", "after_requests", "for", "for_requests"];

/// Common rule fields, removed from the mapping before the remainder is
/// parsed as a fault.
#[derive(Debug, Default)]
struct Common {
    method: Option<String>,
    subscription: Option<String>,
    count: Option<u64>,
    probability: Option<f64>,
    after: Option<Duration>,
    after_requests: Option<u64>,
    active_for: Option<Duration>,
    for_requests: Option<u64>,
}

fn parse_rule(label: String, mut map: Mapping) -> Result<FaultRule, ConfigError> {
    let invalid = |message: String| ConfigError::Invalid {
        label: label.clone(),
        message,
    };
    let common = take_common(&mut map).map_err(invalid)?;
    let raw: RawFault =
        serde_yaml_ng::from_value(Value::Mapping(map)).map_err(|e| invalid(e.to_string()))?;
    let fault = raw.into_fault().map_err(invalid)?;
    build_rule(label.clone(), fault, common).map_err(invalid)
}

fn take_common(map: &mut Mapping) -> Result<Common, String> {
    fn take<T: for<'de> Deserialize<'de>>(
        map: &mut Mapping,
        key: &str,
    ) -> Result<Option<T>, String> {
        map.remove(key)
            .map(|v| serde_yaml_ng::from_value(v).map_err(|e| format!("`{key}`: {e}")))
            .transpose()
    }
    fn take_duration(map: &mut Mapping, key: &str) -> Result<Option<Duration>, String> {
        take::<String>(map, key)?
            .map(|s| parse_duration(&s).map_err(|e| format!("`{key}`: {e}")))
            .transpose()
    }
    Ok(Common {
        method: take(map, "method")?,
        subscription: take(map, "subscription")?,
        count: take(map, "count")?,
        probability: take(map, "probability")?,
        after: take_duration(map, "after")?,
        after_requests: take(map, "after_requests")?,
        active_for: take_duration(map, "for")?,
        for_requests: take(map, "for_requests")?,
    })
}

fn build_rule(label: String, fault: Fault, common: Common) -> Result<FaultRule, String> {
    let kind = fault.kind();
    let method = match common.method {
        None => MethodMatcher::Any,
        Some(m) if m.trim().is_empty() => return Err("`method` must not be empty".into()),
        Some(_) if kind == FaultKind::WsMessage => {
            return Err(format!(
                "`method` does not apply to {}; use `subscription` to filter notifications",
                fault.name()
            ));
        }
        Some(m) => {
            if let Some(scope) = fault.scope().filter(|s| !s.contains(&m.as_str())) {
                return Err(format!(
                    "{} only applies to {}, not `{m}`",
                    fault.name(),
                    scope.join(", ")
                ));
            }
            MethodMatcher::Exact(m)
        }
    };
    if common.subscription.is_some() && kind != FaultKind::WsMessage {
        return Err("`subscription` only applies to ws_* faults".into());
    }
    if common.count == Some(0) {
        return Err("`count` must be at least 1 (omit it for unlimited)".into());
    }
    if let Some(p) = common.probability.filter(|p| !(*p > 0.0 && *p <= 1.0)) {
        return Err(format!("`probability` must be in (0, 1], got {p}"));
    }
    let start = match (common.after, common.after_requests) {
        (Some(_), Some(_)) => return Err("use either `after` or `after_requests`, not both".into()),
        (Some(t), None) => Start::AfterTime(t),
        (None, Some(n)) => Start::AfterRequests(n),
        (None, None) => Start::Immediately,
    };
    let end = match (common.active_for, common.for_requests) {
        (Some(_), Some(_)) => return Err("use either `for` or `for_requests`, not both".into()),
        (Some(d), None) if d.is_zero() => return Err("`for` must be longer than 0".into()),
        (Some(d), None) => End::AfterTime(d),
        (None, Some(0)) => return Err("`for_requests` must be at least 1".into()),
        (None, Some(n)) => End::AfterRequests(n),
        (None, None) => End::Never,
    };
    // A reorg is an event, not a condition: by default it happens once.
    let count = match (&fault, common.count) {
        (Fault::Reorg { .. }, None) => Some(1),
        (_, count) => count,
    };
    Ok(FaultRule {
        label,
        fault,
        method,
        subscription: common.subscription,
        count,
        probability: common.probability,
        window: Window { start, end },
    })
}

/// On-disk shape of the fault-specific part of a rule.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum RawFault {
    Delay {
        duration: String,
    },
    Timeout {
        #[serde(default)]
        duration: Option<String>,
        #[serde(default)]
        forward: bool,
    },
    HttpError {
        status: u16,
        body: Option<String>,
        retry_after: Option<u64>,
    },
    ReceiptNull {},
    TransactionSubmitTimeout {
        #[serde(default)]
        duration: Option<String>,
    },
    StaleHead {
        lag: u64,
    },
    InconsistentHead {
        lag: u64,
    },
    MissingLogs {
        #[serde(default = "half")]
        ratio: f64,
    },
    DuplicatedLogs {
        #[serde(default = "half")]
        ratio: f64,
    },
    ReorderedLogs {},
    StaleNonce {
        #[serde(default = "one")]
        lag: u64,
    },
    ReceiptDisappear {
        #[serde(default = "one_u32")]
        visible_for: u32,
    },
    BlockNull {},
    Reorg {
        depth: u64,
        head: Option<u64>,
        #[serde(default)]
        transactions: RawReorgTransactions,
    },
    WsDisconnect {
        #[serde(default)]
        graceful: bool,
    },
    WsDelay {
        duration: String,
    },
    WsDuplicate {},
    WsDrop {},
    WsReorder {},
    WsStale {},
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
enum RawReorgTransactions {
    #[default]
    Reinclude,
    Drop,
}

fn half() -> f64 {
    0.5
}
fn one() -> u64 {
    1
}
fn one_u32() -> u32 {
    1
}

/// Default for faults that hold a connection until the client gives up.
/// Longer than typical client timeouts so the client times out first.
const DEFAULT_HANG: Duration = Duration::from_secs(60);

impl RawFault {
    fn into_fault(self) -> Result<Fault, String> {
        let duration = |raw: Option<String>| -> Result<Duration, String> {
            raw.map_or(Ok(DEFAULT_HANG), |s| parse_duration(&s))
                .map_err(|e| format!("`duration`: {e}"))
        };
        let ratio = |r: f64| -> Result<f64, String> {
            if r > 0.0 && r <= 1.0 {
                Ok(r)
            } else {
                Err(format!("`ratio` must be in (0, 1], got {r}"))
            }
        };
        let lag = |l: u64| -> Result<u64, String> {
            if l == 0 {
                Err("`lag` must be at least 1".into())
            } else {
                Ok(l)
            }
        };
        Ok(match self {
            RawFault::Delay { duration: d } => Fault::Delay {
                duration: duration(Some(d))?,
            },
            RawFault::Timeout {
                duration: d,
                forward,
            } => Fault::Timeout {
                duration: duration(d)?,
                forward,
            },
            RawFault::HttpError {
                status,
                body,
                retry_after,
            } => {
                if !(400..=599).contains(&status) {
                    return Err(format!(
                        "`status` must be an HTTP error (400-599), got {status}"
                    ));
                }
                Fault::HttpError {
                    status,
                    body,
                    retry_after,
                }
            }
            RawFault::ReceiptNull {} => Fault::ReceiptNull,
            RawFault::TransactionSubmitTimeout { duration: d } => Fault::TransactionSubmitTimeout {
                duration: duration(d)?,
            },
            RawFault::StaleHead { lag: l } => Fault::StaleHead { lag: lag(l)? },
            RawFault::InconsistentHead { lag: l } => Fault::InconsistentHead { lag: lag(l)? },
            RawFault::MissingLogs { ratio: r } => Fault::MissingLogs { ratio: ratio(r)? },
            RawFault::DuplicatedLogs { ratio: r } => Fault::DuplicatedLogs { ratio: ratio(r)? },
            RawFault::ReorderedLogs {} => Fault::ReorderedLogs,
            RawFault::StaleNonce { lag: l } => Fault::StaleNonce { lag: lag(l)? },
            RawFault::ReceiptDisappear { visible_for } => Fault::ReceiptDisappear { visible_for },
            RawFault::BlockNull {} => Fault::BlockNull,
            RawFault::Reorg {
                depth,
                head,
                transactions,
            } => {
                if !(1..=MAX_REORG_DEPTH).contains(&depth) {
                    return Err(format!("`depth` must be between 1 and {MAX_REORG_DEPTH}"));
                }
                if head.is_some_and(|h| h + 1 < depth) {
                    return Err("`head` must be at least `depth - 1`".into());
                }
                Fault::Reorg {
                    depth,
                    head,
                    transactions: match transactions {
                        RawReorgTransactions::Reinclude => ReorgTransactions::Reinclude,
                        RawReorgTransactions::Drop => ReorgTransactions::Drop,
                    },
                }
            }
            RawFault::WsDisconnect { graceful } => Fault::WsDisconnect { graceful },
            RawFault::WsDelay { duration: d } => Fault::WsDelay {
                duration: duration(Some(d))?,
            },
            RawFault::WsDuplicate {} => Fault::WsDuplicate,
            RawFault::WsDrop {} => Fault::WsDrop,
            RawFault::WsReorder {} => Fault::WsReorder,
            RawFault::WsStale {} => Fault::WsStale,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(yaml: &str) -> Vec<FaultRule> {
        FaultConfig::from_yaml(yaml).unwrap().rules
    }

    fn error(yaml: &str) -> String {
        FaultConfig::from_yaml(yaml).unwrap_err().to_string()
    }

    #[test]
    fn parses_delay_rule() {
        let rules = rules(
            "faults:\n  - type: delay\n    method: eth_getLogs\n    duration: 1500ms\n    count: 3\n",
        );
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].label, "faults[0]");
        assert_eq!(
            rules[0].fault,
            Fault::Delay {
                duration: Duration::from_millis(1500)
            }
        );
        assert_eq!(rules[0].method, MethodMatcher::Exact("eth_getLogs".into()));
        assert_eq!(rules[0].count, Some(3));
        assert_eq!(rules[0].window, Window::default());
    }

    #[test]
    fn empty_config_is_valid() {
        let config = FaultConfig::from_yaml("{}").unwrap();
        assert_eq!(config.seed, 0);
        assert!(config.rules.is_empty());
    }

    #[test]
    fn parses_scenario_steps() {
        let config = FaultConfig::from_yaml(
            "seed: 42\nscenario:\n  - after: 10s\n    for: 5s\n    inject:\n      type: http_error\n      status: 429\n  - after_requests: 100\n    for_requests: 3\n    inject: { type: receipt_null }\n",
        )
        .unwrap();
        assert_eq!(config.seed, 42);
        assert_eq!(config.rules[0].label, "scenario[0]");
        assert_eq!(
            config.rules[0].window,
            Window {
                start: Start::AfterTime(Duration::from_secs(10)),
                end: End::AfterTime(Duration::from_secs(5)),
            }
        );
        assert_eq!(
            config.rules[1].window,
            Window {
                start: Start::AfterRequests(100),
                end: End::AfterRequests(3),
            }
        );
    }

    #[test]
    fn parses_every_fault_type() {
        let yaml = r#"
faults:
  - { type: delay, duration: 1s }
  - { type: timeout }
  - { type: timeout, duration: 5s, forward: true }
  - { type: http_error, status: 503, retry_after: 2 }
  - { type: receipt_null }
  - { type: transaction_submit_timeout, duration: 2s }
  - { type: stale_head, lag: 3 }
  - { type: inconsistent_head, lag: 2 }
  - { type: missing_logs, ratio: 0.3 }
  - { type: duplicated_logs }
  - { type: reordered_logs }
  - { type: stale_nonce }
  - { type: receipt_disappear, visible_for: 2 }
  - { type: block_null, method: eth_getBlockByHash }
  - { type: reorg, depth: 2, transactions: drop }
  - { type: ws_disconnect, graceful: true }
  - { type: ws_delay, duration: 100ms, subscription: newHeads }
  - { type: ws_duplicate }
  - { type: ws_drop, probability: 0.5 }
  - { type: ws_reorder }
  - { type: ws_stale }
"#;
        let rules = rules(yaml);
        assert_eq!(rules.len(), 21);
        assert_eq!(
            rules[1].fault,
            Fault::Timeout {
                duration: DEFAULT_HANG,
                forward: false
            }
        );
        assert_eq!(rules[14].count, Some(1), "reorg fires once by default");
        assert_eq!(
            rules[14].fault,
            Fault::Reorg {
                depth: 2,
                head: None,
                transactions: ReorgTransactions::Drop
            }
        );
    }

    #[test]
    fn rejects_unknown_fields_and_types() {
        assert!(error("faults:\n  - type: delay\n    duraton: 1s\n").contains("faults[0]"));
        assert!(error("faults:\n  - type: explode\n").contains("explode"));
        assert!(error("fautls: []\n").contains("fautls"));
        assert!(
            error("scenario:\n  - aftr: 1s\n    inject: { type: receipt_null }\n").contains("aftr")
        );
    }

    #[test]
    fn rejects_invalid_values() {
        let cases = [
            (
                "faults:\n  - { type: delay, duration: 1s, count: 0 }\n",
                "count",
            ),
            (
                "faults:\n  - { type: delay, duration: 1s, probability: 1.5 }\n",
                "probability",
            ),
            ("faults:\n  - { type: http_error, status: 200 }\n", "status"),
            (
                "faults:\n  - { type: receipt_null, method: eth_call }\n",
                "only applies",
            ),
            (
                "faults:\n  - { type: ws_drop, method: eth_call }\n",
                "subscription",
            ),
            (
                "faults:\n  - { type: delay, duration: 1s, subscription: logs }\n",
                "ws_",
            ),
            ("faults:\n  - { type: reorg, depth: 0 }\n", "depth"),
            ("faults:\n  - { type: stale_head, lag: 0 }\n", "lag"),
            ("faults:\n  - { type: missing_logs, ratio: 0 }\n", "ratio"),
            (
                "faults:\n  - { type: delay, duration: 1s, after: 1s, after_requests: 2 }\n",
                "either",
            ),
            (
                "faults:\n  - { type: delay, duration: 1s, for_requests: 0 }\n",
                "for_requests",
            ),
            ("scenario:\n  - after: 1s\n", "inject"),
        ];
        for (yaml, needle) in cases {
            let err = error(yaml);
            assert!(err.contains(needle), "expected `{needle}` in: {err}");
        }
    }

    #[test]
    fn bundled_configs_are_valid() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut checked = 0;
        for dir in ["examples", "scenarios"] {
            for entry in std::fs::read_dir(root.join(dir)).unwrap() {
                let path = entry.unwrap().path();
                if path.extension().is_some_and(|e| e == "yaml") {
                    FaultConfig::from_path(&path).unwrap_or_else(|e| panic!("{e}"));
                    checked += 1;
                }
            }
        }
        assert!(checked > 1, "no bundled configs found");
    }
}
