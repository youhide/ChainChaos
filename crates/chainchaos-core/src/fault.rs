//! The fault model: what can go wrong, and which traffic it applies to.

use std::time::Duration;

use crate::rpc::RpcRequest;

/// Methods whose results depend on the chain head, used by the lag faults.
pub const HEAD_DEPENDENT_METHODS: &[&str] = &[
    "eth_blockNumber",
    "eth_getBlockByNumber",
    "eth_getBlockByHash",
    "eth_getBlockReceipts",
    "eth_getLogs",
    "eth_getTransactionReceipt",
    "eth_getTransactionByHash",
    "eth_call",
    "eth_getBalance",
    "eth_getCode",
    "eth_getStorageAt",
    "eth_getTransactionCount",
];
pub const LOG_METHODS: &[&str] = &["eth_getLogs", "eth_getFilterLogs", "eth_getFilterChanges"];
pub const RECEIPT_METHODS: &[&str] = &["eth_getTransactionReceipt"];
pub const SUBMIT_METHODS: &[&str] = &["eth_sendRawTransaction", "eth_sendTransaction"];
pub const BLOCK_METHODS: &[&str] = &["eth_getBlockByNumber", "eth_getBlockByHash"];
pub const NONCE_METHODS: &[&str] = &["eth_getTransactionCount"];

/// What happens to transactions from blocks removed by a simulated reorg.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReorgTransactions {
    /// The new branch re-includes the same transactions (the common case):
    /// receipts and logs survive, but with new block hashes.
    #[default]
    Reinclude,
    /// The new branch drops them: receipts become `null`, logs disappear and
    /// the transactions look pending again.
    Drop,
}

/// A single fault that chainchaos knows how to inject.
///
/// Each variant is an independent module of behaviour; the proxy decides how
/// to realise it. Variants are grouped by the roadmap phase that added them.
#[derive(Debug, Clone, PartialEq)]
pub enum Fault {
    // Phase 1: transport and HTTP faults.
    /// Hold the request for `duration` before forwarding it upstream.
    Delay { duration: Duration },
    /// Make the caller experience a timeout: optionally forward the request,
    /// then hold the connection for `duration` and answer 504.
    Timeout { duration: Duration, forward: bool },
    /// Answer with an HTTP error instead of forwarding.
    HttpError {
        status: u16,
        body: Option<String>,
        retry_after: Option<u64>,
    },
    /// Replace `eth_getTransactionReceipt` results with `null`.
    ReceiptNull,
    /// Forward a transaction submission, discard the upstream answer and time
    /// out the client, leaving it in an ambiguous state.
    TransactionSubmitTimeout { duration: Duration },

    // Phase 3: EVM-aware response faults.
    /// Every head-dependent method sees a chain `lag` blocks behind.
    StaleHead { lag: u64 },
    /// `eth_blockNumber` reports the real head, but block, log and receipt
    /// queries are served by a node `lag` blocks behind.
    InconsistentHead { lag: u64 },
    /// Drop roughly `ratio` of the logs in log query results.
    MissingLogs { ratio: f64 },
    /// Duplicate roughly `ratio` of the logs in log query results.
    DuplicatedLogs { ratio: f64 },
    /// Shuffle the order of logs in log query results.
    ReorderedLogs,
    /// Report `eth_getTransactionCount` results `lag` lower than reality.
    StaleNonce { lag: u64 },
    /// A receipt is visible `visible_for` times, then becomes `null`.
    ReceiptDisappear { visible_for: u32 },
    /// Block queries return `null`, as from a node that has not seen the block.
    BlockNull,

    // Phase 4: chain reorganisation.
    /// Replace the last `depth` blocks (ending at `head`, or the current head)
    /// with a synthetic branch.
    Reorg {
        depth: u64,
        head: Option<u64>,
        transactions: ReorgTransactions,
    },

    // Phase 5: WebSocket subscription faults.
    /// Close the WebSocket connection (with a close frame if `graceful`).
    WsDisconnect { graceful: bool },
    /// Delay a subscription notification.
    WsDelay { duration: Duration },
    /// Deliver a subscription notification twice.
    WsDuplicate,
    /// Drop a subscription notification.
    WsDrop,
    /// Deliver a subscription notification after the next one.
    WsReorder,
    /// Silence the subscription while the rule is active, keeping the
    /// connection open.
    WsStale,
}

/// How a fault interacts with traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// Applies to individual JSON-RPC requests (HTTP).
    Request,
    /// Changes the proxy's view of the chain for all later traffic.
    Chain,
    /// Applies to WebSocket subscription notifications.
    WsMessage,
}

impl Fault {
    /// Stable, snake_case name used in config files, logs and headers.
    pub fn name(&self) -> &'static str {
        match self {
            Fault::Delay { .. } => "delay",
            Fault::Timeout { .. } => "timeout",
            Fault::HttpError { .. } => "http_error",
            Fault::ReceiptNull => "receipt_null",
            Fault::TransactionSubmitTimeout { .. } => "transaction_submit_timeout",
            Fault::StaleHead { .. } => "stale_head",
            Fault::InconsistentHead { .. } => "inconsistent_head",
            Fault::MissingLogs { .. } => "missing_logs",
            Fault::DuplicatedLogs { .. } => "duplicated_logs",
            Fault::ReorderedLogs => "reordered_logs",
            Fault::StaleNonce { .. } => "stale_nonce",
            Fault::ReceiptDisappear { .. } => "receipt_disappear",
            Fault::BlockNull => "block_null",
            Fault::Reorg { .. } => "reorg",
            Fault::WsDisconnect { .. } => "ws_disconnect",
            Fault::WsDelay { .. } => "ws_delay",
            Fault::WsDuplicate => "ws_duplicate",
            Fault::WsDrop => "ws_drop",
            Fault::WsReorder => "ws_reorder",
            Fault::WsStale => "ws_stale",
        }
    }

    pub fn kind(&self) -> FaultKind {
        match self {
            Fault::Reorg { .. } => FaultKind::Chain,
            Fault::WsDisconnect { .. }
            | Fault::WsDelay { .. }
            | Fault::WsDuplicate
            | Fault::WsDrop
            | Fault::WsReorder
            | Fault::WsStale => FaultKind::WsMessage,
            _ => FaultKind::Request,
        }
    }

    /// The JSON-RPC methods this fault can affect, or `None` for any method.
    pub fn scope(&self) -> Option<&'static [&'static str]> {
        match self {
            Fault::ReceiptNull | Fault::ReceiptDisappear { .. } => Some(RECEIPT_METHODS),
            Fault::TransactionSubmitTimeout { .. } => Some(SUBMIT_METHODS),
            Fault::StaleHead { .. } | Fault::InconsistentHead { .. } => {
                Some(HEAD_DEPENDENT_METHODS)
            }
            Fault::MissingLogs { .. } | Fault::DuplicatedLogs { .. } | Fault::ReorderedLogs => {
                Some(LOG_METHODS)
            }
            Fault::StaleNonce { .. } => Some(NONCE_METHODS),
            Fault::BlockNull => Some(BLOCK_METHODS),
            _ => None,
        }
    }

    /// Whether this fault needs to see (and may rewrite) the upstream
    /// response body.
    pub fn mutates_response(&self) -> bool {
        matches!(
            self,
            Fault::ReceiptNull
                | Fault::StaleHead { .. }
                | Fault::InconsistentHead { .. }
                | Fault::MissingLogs { .. }
                | Fault::DuplicatedLogs { .. }
                | Fault::ReorderedLogs
                | Fault::StaleNonce { .. }
                | Fault::ReceiptDisappear { .. }
                | Fault::BlockNull
        )
    }
}

/// Which requests a rule applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MethodMatcher {
    /// Every request (restricted to the fault's scope, if it has one).
    Any,
    /// Requests containing at least one call to this method. For a batch,
    /// transport faults affect the whole batch; response faults only touch
    /// the matching calls.
    Exact(String),
}

impl MethodMatcher {
    /// Whether `method` is selected, taking the fault's scope into account.
    pub fn selects(&self, method: &str, scope: Option<&[&str]>) -> bool {
        let in_scope = scope.is_none_or(|s| s.contains(&method));
        in_scope
            && match self {
                MethodMatcher::Any => true,
                MethodMatcher::Exact(m) => m == method,
            }
    }

    /// Whether the request as a whole is selected. Unparseable requests are
    /// only selected by unscoped rules without a method filter.
    pub fn matches(&self, request: Option<&RpcRequest>, scope: Option<&[&str]>) -> bool {
        match request {
            Some(req) => req.methods().any(|m| self.selects(m, scope)),
            None => scope.is_none() && *self == MethodMatcher::Any,
        }
    }
}

/// When a rule starts being active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Start {
    #[default]
    Immediately,
    /// After this much time since the proxy started.
    AfterTime(Duration),
    /// Once this many requests have been received (the next one is the first
    /// affected).
    AfterRequests(u64),
}

/// When an active rule stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum End {
    #[default]
    Never,
    /// After being active for this long.
    AfterTime(Duration),
    /// After this many requests were received while active.
    AfterRequests(u64),
}

/// The activity window of a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Window {
    pub start: Start,
    pub end: End,
}

impl Window {
    /// True when both boundaries depend only on time, so transitions can be
    /// announced by a timer instead of by traffic.
    pub fn is_time_based(&self) -> bool {
        !matches!(self.start, Start::AfterRequests(_)) && !matches!(self.end, End::AfterRequests(_))
    }
}

/// A configured fault: what to inject, where, when and how often.
#[derive(Debug, Clone, PartialEq)]
pub struct FaultRule {
    /// Where the rule came from, e.g. `faults[0]` or `scenario[2]`.
    pub label: String,
    pub fault: Fault,
    pub method: MethodMatcher,
    /// WebSocket faults only: restrict to one subscription type
    /// (`newHeads`, `logs`, ...).
    pub subscription: Option<String>,
    /// Maximum number of times this rule fires. `None` means unlimited.
    pub count: Option<u64>,
    /// Probability of firing for each matching request, drawn from the
    /// seeded RNG. `None` means always.
    pub probability: Option<f64>,
    pub window: Window,
}

impl FaultRule {
    /// A rule that is always active, for tests and programmatic use.
    pub fn always(fault: Fault) -> Self {
        Self {
            label: "rule".to_owned(),
            fault,
            method: MethodMatcher::Any,
            subscription: None,
            count: None,
            probability: None,
            window: Window::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_matcher_requires_parsed_request() {
        let matcher = MethodMatcher::Exact("eth_call".into());
        assert!(!matcher.matches(None, None));
        let req = RpcRequest::parse(br#"[{"method":"eth_chainId"},{"method":"eth_call"}]"#);
        assert!(matcher.matches(req.as_ref(), None));
    }

    #[test]
    fn any_matcher_respects_scope() {
        assert!(MethodMatcher::Any.matches(None, None));
        assert!(!MethodMatcher::Any.matches(None, Some(RECEIPT_METHODS)));
        let req = RpcRequest::parse(br#"{"method":"eth_chainId"}"#);
        assert!(!MethodMatcher::Any.matches(req.as_ref(), Some(RECEIPT_METHODS)));
        let req = RpcRequest::parse(br#"{"method":"eth_getTransactionReceipt"}"#);
        assert!(MethodMatcher::Any.matches(req.as_ref(), Some(RECEIPT_METHODS)));
    }
}
