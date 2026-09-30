//! Chain-level state shared by all connections: the reorged view and
//! per-rule bookkeeping for stateful faults.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use chainchaos_core::ReorgTransactions;
use chainchaos_evm::reorg::{ReorgEvent, ReorgView};

#[derive(Debug)]
pub(crate) struct ChainState {
    reorg: Mutex<ReorgView>,
    /// Fast path: avoids locking and parsing when no reorg has happened.
    reorg_active: AtomicBool,
    /// `receipt_disappear`: how many times each (rule, tx hash) receipt was
    /// served.
    receipt_views: Mutex<HashMap<(usize, String), u32>>,
}

impl ChainState {
    pub fn new(seed: u64) -> Self {
        Self {
            reorg: Mutex::new(ReorgView::new(seed)),
            reorg_active: AtomicBool::new(false),
            receipt_views: Mutex::new(HashMap::new()),
        }
    }

    pub fn reorg_active(&self) -> bool {
        self.reorg_active.load(Ordering::Acquire)
    }

    pub fn apply_reorg(
        &self,
        head: u64,
        depth: u64,
        transactions: ReorgTransactions,
    ) -> ReorgEvent {
        let event = self.view().apply(head, depth, transactions);
        self.reorg_active.store(true, Ordering::Release);
        event
    }

    /// Locks the reorged view. Critical sections are short and never await.
    pub fn view(&self) -> std::sync::MutexGuard<'_, ReorgView> {
        self.reorg.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Counts one more view of a receipt and returns the new count.
    pub fn count_receipt_view(&self, rule: usize, tx_hash: &str) -> u32 {
        let mut views = self.receipt_views.lock().unwrap_or_else(|e| e.into_inner());
        let count = views
            .entry((rule, tx_hash.to_ascii_lowercase()))
            .or_insert(0);
        *count += 1;
        *count
    }
}
