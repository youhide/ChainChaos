//! EVM-aware response mutations.
//!
//! Everything here operates on `serde_json::Value` rather than typed RPC
//! structs. That is deliberate: mutations touch only the fields they need,
//! and every other field (including chain-specific extensions such as L2
//! receipt fields) passes through untouched.
//!
//! All functions are pure apart from the [`reorg::ReorgView`] bookkeeping,
//! and none of them perform I/O.

pub mod hex;
pub mod lag;
pub mod logs;
pub mod reorg;
