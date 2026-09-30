//! Core building blocks for chainchaos.
//!
//! This crate is transport-agnostic: it knows how to read a JSON-RPC request,
//! how fault rules are configured, and how to decide which faults apply to a
//! given request. Actually injecting a fault (sleeping, dropping a connection,
//! rewriting a response) is the proxy's job.

pub mod config;
pub mod duration;
pub mod engine;
pub mod fault;
pub mod recording;
pub mod rng;
pub mod rpc;

pub use config::{ConfigError, FaultConfig};
pub use engine::{FaultEngine, InjectedFault, Tick};
pub use fault::{Fault, FaultKind, FaultRule, MethodMatcher, ReorgTransactions};
pub use rng::Rng;
pub use rpc::{RpcCall, RpcRequest};
