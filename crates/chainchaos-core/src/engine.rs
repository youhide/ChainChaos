//! Decides which faults apply to a request.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::FaultConfig;
use crate::fault::{Fault, FaultRule};
use crate::rpc::RpcRequest;

/// A fault selected for one specific request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InjectedFault {
    /// Index of the rule in the config (`faults[i]`), for log correlation.
    pub rule: usize,
    pub fault: Fault,
}

/// Evaluates configured rules against incoming requests.
///
/// Shared across all connections; the only mutable state is per-rule fire
/// counters, which are atomic.
#[derive(Debug, Default)]
pub struct FaultEngine {
    rules: Vec<(FaultRule, AtomicU64)>,
}

impl FaultEngine {
    pub fn new(config: FaultConfig) -> Self {
        Self {
            rules: config
                .rules
                .into_iter()
                .map(|rule| (rule, AtomicU64::new(0)))
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Returns the faults to inject for this request, in rule order.
    ///
    /// `request` is `None` when the body could not be parsed as JSON-RPC; only
    /// rules without a method filter can match such requests.
    ///
    /// Every matching rule fires; the proxy applies them in order, so two
    /// matching delays add up.
    pub fn plan(&self, request: Option<&RpcRequest>) -> Vec<InjectedFault> {
        // TODO(phase-2): consult scenario time windows and a seeded RNG here.
        self.rules
            .iter()
            .enumerate()
            .filter(|(_, (rule, _))| rule.method.matches(request))
            .filter(|(_, (rule, fired))| try_consume(rule.count, fired))
            .map(|(index, (rule, _))| InjectedFault {
                rule: index,
                fault: rule.fault.clone(),
            })
            .collect()
    }
}

/// Atomically claims one firing of a rule, respecting its `count` limit.
fn try_consume(limit: Option<u64>, fired: &AtomicU64) -> bool {
    match limit {
        None => {
            fired.fetch_add(1, Ordering::Relaxed);
            true
        }
        Some(limit) => fired
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < limit).then_some(n + 1)
            })
            .is_ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine(yaml: &str) -> FaultEngine {
        FaultEngine::new(FaultConfig::from_yaml(yaml).unwrap())
    }

    fn request(method: &str) -> RpcRequest {
        RpcRequest::parse(format!(r#"{{"id":1,"method":"{method}"}}"#).as_bytes()).unwrap()
    }

    #[test]
    fn matches_by_method() {
        let engine =
            engine("faults:\n  - type: delay\n    method: eth_getLogs\n    duration: 1s\n");
        assert_eq!(engine.plan(Some(&request("eth_getLogs"))).len(), 1);
        assert!(engine.plan(Some(&request("eth_blockNumber"))).is_empty());
        assert!(engine.plan(None).is_empty());
    }

    #[test]
    fn respects_count_limit() {
        let engine = engine("faults:\n  - type: delay\n    duration: 1s\n    count: 2\n");
        let req = request("eth_chainId");
        assert_eq!(engine.plan(Some(&req)).len(), 1);
        assert_eq!(engine.plan(Some(&req)).len(), 1);
        assert!(engine.plan(Some(&req)).is_empty());
    }

    #[test]
    fn all_matching_rules_fire_in_order() {
        let engine = engine(
            "faults:\n  - type: delay\n    duration: 1s\n  - type: delay\n    method: eth_call\n    duration: 2s\n",
        );
        let plan = engine.plan(Some(&request("eth_call")));
        assert_eq!(plan.iter().map(|f| f.rule).collect::<Vec<_>>(), [0, 1]);
    }
}
