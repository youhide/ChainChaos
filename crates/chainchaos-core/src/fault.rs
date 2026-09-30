//! The fault model: what can go wrong, and which requests it applies to.

use std::time::Duration;

use crate::rpc::RpcRequest;

/// A single fault that chainchaos knows how to inject.
///
/// Each variant is an independent module of behaviour. New variants are added
/// here as roadmap phases land; the proxy decides how to realise each one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    /// Hold the request for `duration` before forwarding it upstream.
    Delay { duration: Duration },
    // TODO(phase-1): Timeout { forward: bool }
    // TODO(phase-1): HttpError { status: u16 }
    // TODO(phase-1): ReceiptNull  (eth_getTransactionReceipt -> null)
    // TODO(phase-1): TransactionSubmitTimeout (forward eth_sendRawTransaction, then time out)
    // TODO(phase-3): EVM-aware response mutations (stale head, missing/duplicated logs, ...)
}

impl Fault {
    /// Stable, snake_case name used in config files and logs.
    pub fn name(&self) -> &'static str {
        match self {
            Fault::Delay { .. } => "delay",
        }
    }
}

/// Which requests a rule applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MethodMatcher {
    /// Every request, including bodies that are not valid JSON-RPC.
    Any,
    /// Requests containing at least one call to this method. For a batch this
    /// means the whole batch is affected if any call in it matches.
    Exact(String),
}

impl MethodMatcher {
    pub fn matches(&self, request: Option<&RpcRequest>) -> bool {
        match self {
            MethodMatcher::Any => true,
            MethodMatcher::Exact(method) => {
                request.is_some_and(|req| req.methods().any(|m| m == method))
            }
        }
    }
}

/// A configured fault: what to inject, where, and how often.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultRule {
    pub fault: Fault,
    pub method: MethodMatcher,
    /// Maximum number of times this rule fires. `None` means unlimited.
    pub count: Option<u64>,
    // TODO(phase-2): time windows, request-count triggers, seeded probability.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_matcher_requires_parsed_request() {
        let matcher = MethodMatcher::Exact("eth_call".into());
        assert!(!matcher.matches(None));
        let req = RpcRequest::parse(br#"[{"method":"eth_chainId"},{"method":"eth_call"}]"#);
        assert!(matcher.matches(req.as_ref()));
    }

    #[test]
    fn any_matcher_matches_everything() {
        assert!(MethodMatcher::Any.matches(None));
    }
}
