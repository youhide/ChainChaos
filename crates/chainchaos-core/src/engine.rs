//! Decides which faults apply to a request or WebSocket message.
//!
//! The engine is deterministic given its inputs: the configured seed, the
//! request sequence number and the elapsed time. It never reads the clock
//! itself, so tests can drive it with synthetic time.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tracing::info;

use crate::config::FaultConfig;
use crate::fault::{End, Fault, FaultKind, FaultRule, Start};
use crate::rng::Rng;
use crate::rpc::RpcRequest;

/// Where a request or message sits in the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tick {
    /// 1-based sequence number (HTTP requests and WebSocket notifications
    /// are counted separately).
    pub seq: u64,
    /// Time since the proxy started.
    pub elapsed: Duration,
}

/// A fault selected for one specific request or message.
#[derive(Debug, Clone)]
pub struct InjectedFault {
    /// Index of the rule in evaluation order.
    pub rule: usize,
    /// Human-readable rule location, e.g. `scenario[1]`.
    pub label: String,
    pub fault: Fault,
    /// Method filter of the rule, for faults that act per call in a batch.
    pub method: crate::fault::MethodMatcher,
    /// Seeded randomness for the fault's own decisions (which logs to drop,
    /// how to shuffle, ...). Deterministic per (seed, rule, seq).
    pub rng: Rng,
}

impl InjectedFault {
    /// Whether this fault should act on a call to `method`.
    pub fn selects(&self, method: &str) -> bool {
        self.method.selects(method, self.fault.scope())
    }
}

#[derive(Debug, Default)]
struct RuleState {
    fired: AtomicU64,
    /// Requests seen while the window was open (for `for_requests`).
    window_requests: AtomicU64,
    /// Elapsed time at which the window opened.
    opened_at: Mutex<Option<Duration>>,
    /// Last announced activity, so transitions are logged once.
    announced_active: AtomicBool,
    announced_closed: AtomicBool,
}

/// Evaluates configured rules against traffic.
///
/// Shared across all connections; per-rule state is atomic or behind short
/// critical sections.
#[derive(Debug, Default)]
pub struct FaultEngine {
    seed: u64,
    rules: Vec<(FaultRule, RuleState)>,
}

impl FaultEngine {
    pub fn new(config: FaultConfig) -> Self {
        Self {
            seed: config.seed,
            rules: config
                .rules
                .into_iter()
                .map(|rule| (rule, RuleState::default()))
                .collect(),
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn rules(&self) -> impl Iterator<Item = &FaultRule> {
        self.rules.iter().map(|(rule, _)| rule)
    }

    /// Faults to inject for an HTTP JSON-RPC request, in rule order.
    ///
    /// `request` is `None` when the body could not be parsed as JSON-RPC.
    /// Every matching rule fires; the proxy applies them in order, so two
    /// matching delays add up.
    pub fn plan(&self, request: Option<&RpcRequest>, tick: Tick) -> Vec<InjectedFault> {
        self.evaluate(
            tick,
            |rule| rule.fault.kind() != FaultKind::WsMessage,
            |rule| rule.method.matches(request, rule.fault.scope()),
        )
    }

    /// Faults to inject for a WebSocket subscription notification.
    /// `subscription` is the subscription type (`newHeads`, `logs`, ...), if
    /// known.
    pub fn plan_ws(&self, subscription: Option<&str>, tick: Tick) -> Vec<InjectedFault> {
        self.evaluate(
            tick,
            |rule| rule.fault.kind() == FaultKind::WsMessage,
            |rule| {
                rule.subscription
                    .as_deref()
                    .is_none_or(|want| Some(want) == subscription)
            },
        )
    }

    /// `applies` selects the rules this kind of traffic can advance (so HTTP
    /// requests never move WebSocket windows and vice versa); `matches`
    /// filters by method or subscription once the window is known to be open.
    fn evaluate(
        &self,
        tick: Tick,
        applies: impl Fn(&FaultRule) -> bool,
        matches: impl Fn(&FaultRule) -> bool,
    ) -> Vec<InjectedFault> {
        let mut planned = Vec::new();
        for (index, (rule, state)) in self.rules.iter().enumerate() {
            if !applies(rule) || !self.window_open(rule, state, tick) || !matches(rule) {
                continue;
            }
            if let Some(p) = rule.probability {
                let mut roll = Rng::derive(self.seed, &[index as u64, tick.seq, 0]);
                if !roll.chance(p) {
                    continue;
                }
            }
            if !try_consume(rule.count, &state.fired) {
                continue;
            }
            planned.push(InjectedFault {
                rule: index,
                label: rule.label.clone(),
                fault: rule.fault.clone(),
                method: rule.method.clone(),
                rng: Rng::derive(self.seed, &[index as u64, tick.seq, 1]),
            });
        }
        planned
    }

    /// Checks (and advances) a rule's window for this tick.
    fn window_open(&self, rule: &FaultRule, state: &RuleState, tick: Tick) -> bool {
        let started = match rule.window.start {
            Start::Immediately => true,
            Start::AfterTime(t) => tick.elapsed >= t,
            Start::AfterRequests(n) => tick.seq > n,
        };
        if !started {
            return false;
        }
        let opened_at = {
            let mut opened = state.opened_at.lock().unwrap_or_else(|e| e.into_inner());
            *opened.get_or_insert(match rule.window.start {
                Start::Immediately => Duration::ZERO,
                Start::AfterTime(t) => t,
                Start::AfterRequests(_) => tick.elapsed,
            })
        };
        let open = match rule.window.end {
            End::Never => true,
            End::AfterTime(d) => tick.elapsed < opened_at + d,
            End::AfterRequests(n) => state.window_requests.fetch_add(1, Ordering::Relaxed) < n,
        };
        announce(rule, state, open, tick);
        open
    }

    /// Instants (since start) at which time-based windows open or close, for
    /// a timer that announces transitions even without traffic.
    pub fn time_transitions(&self) -> Vec<Duration> {
        let mut times: Vec<Duration> = self
            .rules
            .iter()
            .filter(|(rule, _)| rule.window.is_time_based())
            .flat_map(|(rule, _)| {
                let start = match rule.window.start {
                    Start::AfterTime(t) => t,
                    _ => Duration::ZERO,
                };
                let end = match rule.window.end {
                    End::AfterTime(d) => Some(start + d),
                    _ => None,
                };
                [Some(start), end].into_iter().flatten()
            })
            .filter(|t| !t.is_zero())
            .collect();
        times.sort();
        times.dedup();
        times
    }

    /// Announces time-based window transitions at `elapsed`. Called by the
    /// proxy's scenario timer; has no effect on which faults fire.
    pub fn announce_time_transitions(&self, elapsed: Duration) {
        for (rule, state) in &self.rules {
            if !rule.window.is_time_based() {
                continue;
            }
            let start = match rule.window.start {
                Start::AfterTime(t) => t,
                _ => Duration::ZERO,
            };
            let open = elapsed >= start
                && match rule.window.end {
                    End::AfterTime(d) => elapsed < start + d,
                    _ => true,
                };
            let tick = Tick { seq: 0, elapsed };
            announce(rule, state, open, tick);
        }
    }
}

/// Logs a window opening or closing, once per transition.
fn announce(rule: &FaultRule, state: &RuleState, open: bool, tick: Tick) {
    let always_on = rule.window.start == Start::Immediately && rule.window.end == End::Never;
    if always_on {
        return;
    }
    if open {
        if !state.announced_active.swap(true, Ordering::Relaxed) {
            info!(
                rule = %rule.label,
                fault = rule.fault.name(),
                elapsed_ms = tick.elapsed.as_millis() as u64,
                "scenario step activated"
            );
        }
    } else if state.announced_active.load(Ordering::Relaxed)
        && !state.announced_closed.swap(true, Ordering::Relaxed)
    {
        info!(
            rule = %rule.label,
            fault = rule.fault.name(),
            elapsed_ms = tick.elapsed.as_millis() as u64,
            "scenario step deactivated"
        );
    }
}

/// Atomically claims one firing of a rule, respecting its `count` limit.
fn try_consume(limit: Option<u64>, fired: &AtomicU64) -> bool {
    match limit {
        None => {
            fired.fetch_add(1, Ordering::Relaxed);
            true
        }
        // A compare-exchange loop rather than `fetch_update`, which Rust 1.99
        // deprecates in favour of `try_update`; that does not exist in our
        // MSRV (1.85).
        Some(limit) => {
            let mut current = fired.load(Ordering::Relaxed);
            loop {
                if current >= limit {
                    return false;
                }
                match fired.compare_exchange_weak(
                    current,
                    current + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return true,
                    Err(actual) => current = actual,
                }
            }
        }
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

    fn at(seq: u64, secs: u64) -> Tick {
        Tick {
            seq,
            elapsed: Duration::from_secs(secs),
        }
    }

    fn fires(engine: &FaultEngine, method: &str, tick: Tick) -> bool {
        !engine.plan(Some(&request(method)), tick).is_empty()
    }

    #[test]
    fn matches_by_method() {
        let engine =
            engine("faults:\n  - type: delay\n    method: eth_getLogs\n    duration: 1s\n");
        assert!(fires(&engine, "eth_getLogs", at(1, 0)));
        assert!(!fires(&engine, "eth_blockNumber", at(2, 0)));
        assert!(engine.plan(None, at(3, 0)).is_empty());
    }

    #[test]
    fn scoped_faults_only_match_their_methods() {
        let engine = engine("faults:\n  - type: receipt_null\n");
        assert!(fires(&engine, "eth_getTransactionReceipt", at(1, 0)));
        assert!(!fires(&engine, "eth_call", at(2, 0)));
    }

    #[test]
    fn respects_count_limit() {
        let engine = engine("faults:\n  - type: delay\n    duration: 1s\n    count: 2\n");
        assert!(fires(&engine, "eth_chainId", at(1, 0)));
        assert!(fires(&engine, "eth_chainId", at(2, 0)));
        assert!(!fires(&engine, "eth_chainId", at(3, 0)));
    }

    #[test]
    fn all_matching_rules_fire_in_order() {
        let engine = engine(
            "faults:\n  - type: delay\n    duration: 1s\n  - type: delay\n    method: eth_call\n    duration: 2s\n",
        );
        let plan = engine.plan(Some(&request("eth_call")), at(1, 0));
        assert_eq!(plan.iter().map(|f| f.rule).collect::<Vec<_>>(), [0, 1]);
    }

    #[test]
    fn time_windows_open_and_close() {
        let engine = engine(
            "scenario:\n  - after: 10s\n    for: 5s\n    inject: { type: delay, duration: 1s }\n",
        );
        assert!(!fires(&engine, "eth_call", at(1, 9)));
        assert!(fires(&engine, "eth_call", at(2, 10)));
        assert!(fires(&engine, "eth_call", at(3, 14)));
        assert!(!fires(&engine, "eth_call", at(4, 15)));
        assert_eq!(
            engine.time_transitions(),
            [Duration::from_secs(10), Duration::from_secs(15)]
        );
    }

    #[test]
    fn request_windows_open_and_close() {
        let engine = engine(
            "scenario:\n  - after_requests: 2\n    for_requests: 2\n    inject: { type: delay, duration: 1s }\n",
        );
        let fired: Vec<bool> = (1..=6)
            .map(|seq| fires(&engine, "eth_call", at(seq, 0)))
            .collect();
        assert_eq!(fired, [false, false, true, true, false, false]);
        assert!(engine.time_transitions().is_empty());
    }

    #[test]
    fn probability_is_reproducible_with_seed() {
        let yaml = "seed: 7\nfaults:\n  - { type: delay, duration: 1s, probability: 0.3 }\n";
        let run = |yaml: &str| -> Vec<bool> {
            let engine = engine(yaml);
            (1..=200)
                .map(|seq| fires(&engine, "eth_call", at(seq, 0)))
                .collect()
        };
        let first = run(yaml);
        assert_eq!(first, run(yaml), "same seed must reproduce");
        let hits = first.iter().filter(|f| **f).count();
        assert!((30..90).contains(&hits), "{hits}");
        assert_ne!(first, run(&yaml.replace("seed: 7", "seed: 8")));
    }

    #[test]
    fn ws_rules_only_match_ws_plans() {
        let engine = engine(
            "faults:\n  - { type: ws_drop, subscription: newHeads }\n  - { type: delay, duration: 1s }\n",
        );
        let ws = engine.plan_ws(Some("newHeads"), at(1, 0));
        assert_eq!(ws.len(), 1);
        assert_eq!(ws[0].fault, Fault::WsDrop);
        assert!(engine.plan_ws(Some("logs"), at(2, 0)).is_empty());
        let http = engine.plan(Some(&request("eth_call")), at(1, 0));
        assert_eq!(http.len(), 1);
        assert_eq!(http[0].fault.name(), "delay");
    }

    #[test]
    fn ws_traffic_does_not_advance_http_windows() {
        let engine = engine(
            "scenario:\n  - after_requests: 0\n    for_requests: 1\n    inject: { type: delay, duration: 1s }\n",
        );
        for seq in 1..=5 {
            assert!(engine.plan_ws(Some("newHeads"), at(seq, 0)).is_empty());
        }
        assert!(
            fires(&engine, "eth_call", at(1, 0)),
            "window still open for HTTP"
        );
        assert!(!fires(&engine, "eth_call", at(2, 0)));
    }

    #[test]
    fn count_limit_holds_under_contention() {
        let fired = AtomicU64::new(0);
        let claimed = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1_000 {
                        if try_consume(Some(100), &fired) {
                            claimed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(claimed.load(Ordering::Relaxed), 100);
        assert_eq!(fired.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn reorg_fires_once() {
        let engine =
            engine("scenario:\n  - after_requests: 1\n    inject: { type: reorg, depth: 2 }\n");
        assert!(!fires(&engine, "eth_blockNumber", at(1, 0)));
        assert!(fires(&engine, "eth_blockNumber", at(2, 0)));
        assert!(!fires(&engine, "eth_blockNumber", at(3, 0)));
    }
}
