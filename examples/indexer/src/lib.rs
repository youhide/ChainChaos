//! A deliberately small, reorg-aware block and log indexer.
//!
//! It exists to show what chainchaos is for: the code below has to survive
//! chain reorganisations, and `tests/reorg.rs` proves it does by running it
//! through chainchaos with an injected reorg.
//!
//! Reorg handling, in two places:
//!
//! 1. **Tip check.** Before extending, re-fetch the last indexed block. If its
//!    hash changed, the chain reorganised under us.
//! 2. **Parent check.** While extending, every new block's `parentHash` must
//!    equal the stored hash of the previous block.
//!
//! On a mismatch the indexer walks back until a stored block still matches
//! the chain (the common ancestor), deletes everything above it, and
//! re-indexes from there.

use std::collections::BTreeMap;

use serde_json::{Value, json};

/// What the indexer stores per block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedBlock {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    /// `blockHash` of every log in the block, as returned by `eth_getLogs`.
    pub log_block_hashes: Vec<String>,
}

/// Outcome of one [`Indexer::sync`] call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub indexed: u64,
    /// Blocks rolled back because they were no longer canonical.
    pub rolled_back: u64,
}

#[derive(Debug)]
pub struct Indexer {
    client: reqwest::Client,
    url: String,
    blocks: BTreeMap<u64, IndexedBlock>,
    pub reorgs_detected: u32,
}

impl Indexer {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
            blocks: BTreeMap::new(),
            reorgs_detected: 0,
        }
    }

    pub fn blocks(&self) -> &BTreeMap<u64, IndexedBlock> {
        &self.blocks
    }

    /// Indexes up to the current head, handling reorgs on the way.
    pub async fn sync(&mut self) -> Result<SyncReport, String> {
        let mut report = SyncReport::default();
        let head = quantity(&self.call("eth_blockNumber", json!([])).await?)?;

        // 1. Tip check.
        if let Some((&number, tip)) = self.blocks.iter().next_back() {
            let current = self.block(number).await?;
            if current.get("hash") != Some(&json!(tip.hash)) {
                report.rolled_back += self.rollback().await?;
            }
        }

        // 2. Extend, checking parent links.
        let mut next = self.blocks.keys().next_back().map_or(0, |n| n + 1);
        while next <= head {
            let block = self.block(next).await?;
            if block.is_null() {
                // The node does not have it yet (e.g. a lagging backend).
                break;
            }
            let parent_hash = string(&block["parentHash"])?;
            if let Some(previous) = self.blocks.get(&next.wrapping_sub(1)) {
                if previous.hash != parent_hash {
                    report.rolled_back += self.rollback().await?;
                    next = self.blocks.keys().next_back().map_or(0, |n| n + 1);
                    continue;
                }
            }
            let hash = string(&block["hash"])?;
            let logs = self
                .call("eth_getLogs", json!([{"blockHash": hash}]))
                .await?;
            let log_block_hashes = logs
                .as_array()
                .ok_or("eth_getLogs did not return an array")?
                .iter()
                .map(|log| string(&log["blockHash"]))
                .collect::<Result<_, _>>()?;
            self.blocks.insert(
                next,
                IndexedBlock {
                    number: next,
                    hash,
                    parent_hash,
                    log_block_hashes,
                },
            );
            report.indexed += 1;
            next += 1;
        }
        Ok(report)
    }

    /// Walks back to the common ancestor and deletes everything above it.
    /// Returns how many blocks were removed.
    async fn rollback(&mut self) -> Result<u64, String> {
        self.reorgs_detected += 1;
        let mut removed = 0;
        while let Some((&number, stored)) = self.blocks.iter().next_back() {
            let canonical = self.block(number).await?;
            if canonical.get("hash") == Some(&json!(stored.hash)) {
                break;
            }
            self.blocks.remove(&number);
            removed += 1;
        }
        Ok(removed)
    }

    async fn block(&self, number: u64) -> Result<Value, String> {
        self.call(
            "eth_getBlockByNumber",
            json!([format!("{number:#x}"), false]),
        )
        .await
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let response: Value = self
            .client
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{method}: {e}"))?
            .json()
            .await
            .map_err(|e| format!("{method}: {e}"))?;
        if let Some(error) = response.get("error") {
            return Err(format!("{method}: {error}"));
        }
        Ok(response["result"].clone())
    }
}

fn quantity(value: &Value) -> Result<u64, String> {
    let s = string(value)?;
    u64::from_str_radix(s.trim_start_matches("0x"), 16)
        .map_err(|e| format!("bad quantity {s}: {e}"))
}

fn string(value: &Value) -> Result<String, String> {
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("expected a string, got {value}"))
}
