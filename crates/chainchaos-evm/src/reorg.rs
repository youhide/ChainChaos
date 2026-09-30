//! Synthetic chain reorganisations.
//!
//! chainchaos does not fork the upstream chain. Instead, after a reorg of
//! depth `d` at head `H`, it rewrites everything the client observes about
//! blocks `H-d+1 ..= H` so they appear to belong to a different branch:
//!
//! ```text
//! before:  N-1 ── N (0xAAA) ── N+1 (0xBBB)          (real hashes)
//! after:   N-1 ── N (0xCCC) ── N+1 (0xDDD) ── N+2   (synthetic hashes)
//! ```
//!
//! - Block hashes in the range are replaced by deterministic synthetic
//!   hashes; `parentHash` links are rewritten so the new branch is
//!   internally consistent, including blocks mined after the reorg.
//! - Logs, receipts and transactions in the range get the new `blockHash`
//!   (`transactions: reinclude`), or disappear / turn pending
//!   (`transactions: drop`).
//! - Queries by an old (pre-reorg) block hash return `null`, or the
//!   "unknown block" error for `eth_getLogs`, exactly like a node that has
//!   switched branches.
//! - Queries by a synthetic hash are translated back to the real hash
//!   before forwarding.
//!
//! Repeated reorgs of the same height produce a fresh branch each time
//! (tracked by a generation number).

use std::collections::HashMap;

use chainchaos_core::{ReorgTransactions, Rng};
use serde_json::Value;

use crate::hex::{block_number_of, parse_quantity};
use crate::lag::make_pending;

/// One applied reorg.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReorgEvent {
    pub first: u64,
    pub last: u64,
    pub generation: u32,
    pub transactions: ReorgTransactions,
}

impl ReorgEvent {
    fn covers(&self, number: u64) -> bool {
        (self.first..=self.last).contains(&number)
    }
}

/// Outcome of rewriting a result.
#[derive(Debug, Clone, PartialEq)]
pub enum Rewrite {
    Unchanged,
    Changed,
    /// Answer with this JSON-RPC error instead of a result.
    Error {
        code: i64,
        message: String,
    },
}

/// The proxy's synthetic view of the chain after zero or more reorgs.
#[derive(Debug, Clone, Default)]
pub struct ReorgView {
    seed: u64,
    events: Vec<ReorgEvent>,
    /// Synthetic hash -> (real hash, generation), for translating client
    /// queries and recognising hashes from superseded branches.
    reverse: HashMap<String, (String, u32)>,
}

impl ReorgView {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            ..Self::default()
        }
    }

    pub fn is_active(&self) -> bool {
        !self.events.is_empty()
    }

    pub fn events(&self) -> &[ReorgEvent] {
        &self.events
    }

    /// Applies a reorg replacing the `depth` blocks ending at `head`.
    pub fn apply(&mut self, head: u64, depth: u64, transactions: ReorgTransactions) -> ReorgEvent {
        let event = ReorgEvent {
            first: head.saturating_sub(depth.saturating_sub(1)),
            last: head,
            generation: self.events.len() as u32 + 1,
            transactions,
        };
        self.events.push(event.clone());
        event
    }

    /// The most recent reorg covering `number`, if any.
    fn event_for(&self, number: u64) -> Option<&ReorgEvent> {
        self.events.iter().rev().find(|e| e.covers(number))
    }

    fn reorged(&self, number: Option<u64>) -> Option<ReorgEvent> {
        number.and_then(|n| self.event_for(n)).cloned()
    }

    /// Deterministic synthetic hash for a real block hash in a generation.
    pub fn synthetic_hash(&mut self, real: &str, generation: u32) -> String {
        let fingerprint = real.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        });
        let mut rng = Rng::derive(self.seed, &[u64::from(generation), fingerprint]);
        let synthetic = format!(
            "0x{:016x}{:016x}{:016x}{:016x}",
            rng.next_u64(),
            rng.next_u64(),
            rng.next_u64(),
            rng.next_u64()
        );
        self.reverse
            .insert(synthetic.clone(), (real.to_ascii_lowercase(), generation));
        synthetic
    }

    fn real_for(&self, hash: &str) -> Option<&String> {
        self.reverse
            .get(&hash.to_ascii_lowercase())
            .map(|(real, _)| real)
    }

    /// Whether a block hash the client asked for is not on the current
    /// branch for block `number`: either the original (pre-reorg) hash, or
    /// a synthetic hash from a superseded reorg.
    fn is_non_canonical(&self, asked: &str, number: Option<u64>) -> bool {
        let Some(event) = self.reorged(number) else {
            return false;
        };
        match self.reverse.get(&asked.to_ascii_lowercase()) {
            Some((_, generation)) => *generation != event.generation,
            None => asked.len() == 66,
        }
    }

    /// Translates synthetic block hashes in request params back to the real
    /// hashes the upstream knows. Returns whether anything changed.
    pub fn translate_request(&self, method: &str, params: &mut Value) -> bool {
        let slot = match method {
            "eth_getBlockByHash"
            | "eth_getBlockReceipts"
            | "eth_getBlockTransactionCountByHash"
            | "eth_getTransactionByBlockHashAndIndex"
            | "eth_getUncleCountByBlockHash" => params.get_mut(0),
            "eth_getLogs" => params.get_mut(0).and_then(|f| f.get_mut("blockHash")),
            _ => None,
        };
        let Some(slot) = slot else {
            return false;
        };
        match slot.as_str().and_then(|h| self.real_for(h)).cloned() {
            Some(real) => {
                *slot = Value::String(real);
                true
            }
            None => false,
        }
    }

    /// Rewrites a result so it is consistent with the reorged view.
    ///
    /// `original_params` are the params as the client sent them (before
    /// [`translate_request`](Self::translate_request)); they reveal whether
    /// the client asked for an old, now non-canonical block hash.
    pub fn rewrite_result(
        &mut self,
        method: &str,
        original_params: Option<&Value>,
        result: &mut Value,
    ) -> Rewrite {
        if !self.is_active() || result.is_null() {
            return Rewrite::Unchanged;
        }
        let asked_hash = original_params
            .and_then(|p| p.get(0))
            .and_then(Value::as_str)
            .map(str::to_owned);

        match method {
            "eth_getBlockByNumber" | "eth_getBlockByHash" => {
                let number = result.get("number").and_then(parse_quantity);
                let stale = asked_hash
                    .as_deref()
                    .is_some_and(|h| self.is_non_canonical(h, number));
                if method == "eth_getBlockByHash" && stale {
                    *result = Value::Null;
                    return Rewrite::Changed;
                }
                changed(self.rewrite_block(result))
            }
            "eth_getBlockReceipts" => {
                let Some(receipts) = result.as_array_mut() else {
                    return Rewrite::Unchanged;
                };
                let number = receipts.first().and_then(block_number_of);
                let Some(event) = self.reorged(number) else {
                    return Rewrite::Unchanged;
                };
                if asked_hash
                    .as_deref()
                    .is_some_and(|h| self.is_non_canonical(h, number))
                {
                    *result = Value::Null;
                    return Rewrite::Changed;
                }
                if event.transactions == ReorgTransactions::Drop {
                    receipts.clear();
                    return Rewrite::Changed;
                }
                let mut any = false;
                for receipt in receipts.iter_mut() {
                    any |= self.rewrite_receipt(receipt);
                }
                changed(any)
            }
            "eth_getLogs" | "eth_getFilterLogs" | "eth_getFilterChanges" => {
                let asked_block = original_params
                    .and_then(|p| p.get(0))
                    .and_then(|f| f.get("blockHash"))
                    .and_then(Value::as_str)
                    .filter(|_| method == "eth_getLogs")
                    .map(str::to_owned);
                let Some(logs) = result.as_array_mut() else {
                    return Rewrite::Unchanged;
                };
                let number = logs.first().and_then(block_number_of);
                if asked_block
                    .as_deref()
                    .is_some_and(|h| self.is_non_canonical(h, number))
                {
                    return Rewrite::Error {
                        code: -32000,
                        message: "unknown block".to_owned(),
                    };
                }
                let mut any = false;
                let original = std::mem::take(logs);
                for mut log in original {
                    match self.rewrite_log(&mut log) {
                        LogFate::Keep(c) => {
                            any |= c;
                            logs.push(log);
                        }
                        LogFate::Drop => any = true,
                    }
                }
                changed(any)
            }
            "eth_getTransactionReceipt" => changed(self.rewrite_receipt(result)),
            "eth_getTransactionByHash"
            | "eth_getTransactionByBlockHashAndIndex"
            | "eth_getTransactionByBlockNumberAndIndex" => {
                changed(self.rewrite_transaction(result))
            }
            _ => Rewrite::Unchanged,
        }
    }

    /// Rewrites a block (or block header, as in `newHeads`).
    pub fn rewrite_block(&mut self, block: &mut Value) -> bool {
        let Some(number) = block.get("number").and_then(parse_quantity) else {
            return false;
        };
        let mut any = false;
        if let Some(event) = self.reorged(Some(number)) {
            if let Some(hash) = block.get("hash").and_then(Value::as_str).map(str::to_owned) {
                let synthetic = self.synthetic_hash(&hash, event.generation);
                block["hash"] = Value::String(synthetic.clone());
                any = true;
                if let Some(txs) = block.get_mut("transactions").and_then(Value::as_array_mut) {
                    if event.transactions == ReorgTransactions::Drop {
                        txs.clear();
                    } else {
                        for tx in txs.iter_mut().filter(|t| t.is_object()) {
                            tx["blockHash"] = Value::String(synthetic.clone());
                        }
                    }
                }
            }
        }
        if let Some(event) = number
            .checked_sub(1)
            .and_then(|p| self.event_for(p))
            .cloned()
        {
            if let Some(parent) = block
                .get("parentHash")
                .and_then(Value::as_str)
                .map(str::to_owned)
            {
                block["parentHash"] = Value::String(self.synthetic_hash(&parent, event.generation));
                any = true;
            }
        }
        any
    }

    /// Rewrites a log; logs from dropped transactions disappear.
    pub fn rewrite_log(&mut self, log: &mut Value) -> LogFate {
        let Some(event) = self.reorged(block_number_of(log)) else {
            return LogFate::Keep(false);
        };
        if event.transactions == ReorgTransactions::Drop {
            return LogFate::Drop;
        }
        LogFate::Keep(self.rewrite_block_hash_field(log, event.generation))
    }

    fn rewrite_receipt(&mut self, receipt: &mut Value) -> bool {
        let Some(event) = self.reorged(block_number_of(receipt)) else {
            return false;
        };
        if event.transactions == ReorgTransactions::Drop {
            *receipt = Value::Null;
            return true;
        }
        let mut any = self.rewrite_block_hash_field(receipt, event.generation);
        if let Some(logs) = receipt.get_mut("logs").and_then(Value::as_array_mut) {
            let mut logs = std::mem::take(logs);
            for log in &mut logs {
                any |= self.rewrite_block_hash_field(log, event.generation);
            }
            receipt["logs"] = Value::Array(logs);
        }
        any
    }

    fn rewrite_transaction(&mut self, tx: &mut Value) -> bool {
        let Some(event) = self.reorged(block_number_of(tx)) else {
            return false;
        };
        if event.transactions == ReorgTransactions::Drop {
            make_pending(tx);
            return true;
        }
        self.rewrite_block_hash_field(tx, event.generation)
    }

    fn rewrite_block_hash_field(&mut self, object: &mut Value, generation: u32) -> bool {
        match object
            .get("blockHash")
            .and_then(Value::as_str)
            .map(str::to_owned)
        {
            Some(hash) => {
                object["blockHash"] = Value::String(self.synthetic_hash(&hash, generation));
                true
            }
            None => false,
        }
    }
}

/// What happens to a log under the reorged view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFate {
    /// Keep it; `true` if it was modified.
    Keep(bool),
    Drop,
}

fn changed(any: bool) -> Rewrite {
    if any {
        Rewrite::Changed
    } else {
        Rewrite::Unchanged
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn h(n: u64) -> String {
        format!("0x{n:064x}")
    }

    fn block(n: u64) -> Value {
        json!({
            "number": format!("{n:#x}"),
            "hash": h(0xb000 + n),
            "parentHash": h(0xb000 + n - 1),
            "transactions": [h(0x7000 + n)],
        })
    }

    fn log(n: u64) -> Value {
        json!({"blockNumber": format!("{n:#x}"), "blockHash": h(0xb000 + n), "logIndex": "0x0"})
    }

    #[test]
    fn inactive_view_changes_nothing() {
        let mut view = ReorgView::new(1);
        let mut b = block(100);
        assert_eq!(
            view.rewrite_result("eth_getBlockByNumber", None, &mut b),
            Rewrite::Unchanged
        );
        assert_eq!(b, block(100));
    }

    #[test]
    fn reorg_rewrites_range_and_links() {
        let mut view = ReorgView::new(1);
        let event = view.apply(100, 2, ReorgTransactions::Reinclude);
        assert_eq!((event.first, event.last), (99, 100));

        let mut b98 = block(98);
        view.rewrite_result("eth_getBlockByNumber", None, &mut b98);
        assert_eq!(b98, block(98), "blocks below the range are untouched");

        let mut b99 = block(99);
        let mut b100 = block(100);
        let mut b101 = block(101);
        for b in [&mut b99, &mut b100, &mut b101] {
            view.rewrite_result("eth_getBlockByNumber", None, b);
        }
        assert_ne!(b99["hash"], block(99)["hash"]);
        assert_eq!(
            b99["parentHash"],
            block(99)["parentHash"],
            "fork point parent is canonical"
        );
        assert_eq!(b100["parentHash"], b99["hash"]);
        assert_eq!(b101["parentHash"], b100["hash"]);
        assert_eq!(b101["hash"], block(101)["hash"]);

        // Deterministic across calls.
        let mut again = block(100);
        view.rewrite_result("eth_getBlockByNumber", None, &mut again);
        assert_eq!(again, b100);

        // Logs follow the new branch.
        let mut logs = json!([log(98), log(99)]);
        assert_eq!(
            view.rewrite_result("eth_getLogs", Some(&json!([{}])), &mut logs),
            Rewrite::Changed
        );
        assert_eq!(logs[0], log(98));
        assert_eq!(logs[1]["blockHash"], b99["hash"]);
    }

    #[test]
    fn old_hashes_become_non_canonical_and_new_ones_resolve() {
        let mut view = ReorgView::new(1);
        view.apply(100, 1, ReorgTransactions::Reinclude);

        // Client asks by the old real hash: the node no longer knows it.
        let old = json!([h(0xb000 + 100), false]);
        let mut b = block(100);
        assert_eq!(
            view.rewrite_result("eth_getBlockByHash", Some(&old), &mut b),
            Rewrite::Changed
        );
        assert_eq!(b, Value::Null);

        // Learn the synthetic hash, then query by it.
        let mut b = block(100);
        view.rewrite_result("eth_getBlockByNumber", None, &mut b);
        let synthetic = b["hash"].as_str().unwrap().to_owned();
        let original = json!([synthetic.clone(), false]);
        let mut params = original.clone();
        assert!(view.translate_request("eth_getBlockByHash", &mut params));
        assert_eq!(params[0], json!(h(0xb000 + 100)));
        let mut b = block(100);
        view.rewrite_result("eth_getBlockByHash", Some(&original), &mut b);
        assert_eq!(b["hash"], json!(synthetic));

        // eth_getLogs by the old hash fails like on a real node.
        let old_filter = json!([{"blockHash": h(0xb000 + 100)}]);
        let mut logs = json!([log(100)]);
        assert!(matches!(
            view.rewrite_result("eth_getLogs", Some(&old_filter), &mut logs),
            Rewrite::Error { .. }
        ));
    }

    #[test]
    fn dropped_transactions_disappear() {
        let mut view = ReorgView::new(1);
        view.apply(100, 1, ReorgTransactions::Drop);
        let mut receipt = json!({"blockNumber": "0x64", "blockHash": h(0xb064), "logs": []});
        view.rewrite_result("eth_getTransactionReceipt", None, &mut receipt);
        assert_eq!(receipt, Value::Null);

        let mut tx = json!({"hash": h(0x7064), "blockNumber": "0x64", "blockHash": h(0xb064), "transactionIndex": "0x0"});
        view.rewrite_result("eth_getTransactionByHash", None, &mut tx);
        assert_eq!(tx["blockHash"], Value::Null);

        let mut logs = json!([log(99), log(100)]);
        view.rewrite_result("eth_getLogs", Some(&json!([{}])), &mut logs);
        assert_eq!(logs, json!([log(99)]));

        let mut b = block(100);
        view.rewrite_result("eth_getBlockByNumber", None, &mut b);
        assert_eq!(b["transactions"], json!([]));
    }

    #[test]
    fn repeated_reorgs_create_new_branches() {
        let mut view = ReorgView::new(1);
        view.apply(100, 1, ReorgTransactions::Reinclude);
        let mut first = block(100);
        view.rewrite_block(&mut first);
        view.apply(100, 1, ReorgTransactions::Reinclude);
        let mut second = block(100);
        view.rewrite_block(&mut second);
        assert_ne!(first["hash"], second["hash"]);
    }

    #[test]
    fn superseded_synthetic_hashes_are_non_canonical() {
        let mut view = ReorgView::new(1);
        view.apply(100, 1, ReorgTransactions::Reinclude);
        let mut first = block(100);
        view.rewrite_block(&mut first);
        view.apply(100, 1, ReorgTransactions::Reinclude);
        let asked = json!([first["hash"], false]);
        let mut params = asked.clone();
        assert!(view.translate_request("eth_getBlockByHash", &mut params));
        let mut b = block(100);
        view.rewrite_result("eth_getBlockByHash", Some(&asked), &mut b);
        assert_eq!(b, Value::Null);
    }

    #[test]
    fn receipts_get_new_block_hash_including_logs() {
        let mut view = ReorgView::new(1);
        view.apply(100, 1, ReorgTransactions::Reinclude);
        let mut receipt =
            json!({"blockNumber": "0x64", "blockHash": h(0xb064), "logs": [log(100)]});
        view.rewrite_result("eth_getTransactionReceipt", None, &mut receipt);
        assert_ne!(receipt["blockHash"], json!(h(0xb064)));
        assert_eq!(receipt["logs"][0]["blockHash"], receipt["blockHash"]);
    }
}
