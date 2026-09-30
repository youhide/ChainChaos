//! Log-list and nonce mutations.
//!
//! Log mutations keep every individual log object intact; they only change
//! which logs are present, how often, and in what order. That mirrors real
//! provider bugs (partial range results, duplicated pages, unsorted merges).

use chainchaos_core::Rng;
use serde_json::Value;

use crate::hex::{parse_quantity, quantity};

/// Drops roughly `ratio` of the logs. Always drops at least one log from a
/// non-empty list so the fault is observable. Returns whether anything
/// changed.
pub fn drop_logs(result: &mut Value, ratio: f64, rng: &mut Rng) -> bool {
    let Some(logs) = result.as_array_mut() else {
        return false;
    };
    if logs.is_empty() {
        return false;
    }
    let mut keep: Vec<bool> = logs.iter().map(|_| !rng.chance(ratio)).collect();
    if keep.iter().all(|k| *k) {
        let victim = rng.below(logs.len() as u64) as usize;
        keep[victim] = false;
    }
    let mut flags = keep.into_iter();
    logs.retain(|_| flags.next().unwrap_or(true));
    true
}

/// Duplicates roughly `ratio` of the logs, placing each copy right after its
/// original. Always duplicates at least one log from a non-empty list.
pub fn duplicate_logs(result: &mut Value, ratio: f64, rng: &mut Rng) -> bool {
    let Some(logs) = result.as_array_mut() else {
        return false;
    };
    if logs.is_empty() {
        return false;
    }
    let mut dup: Vec<bool> = logs.iter().map(|_| rng.chance(ratio)).collect();
    if !dup.iter().any(|d| *d) {
        let pick = rng.below(logs.len() as u64) as usize;
        dup[pick] = true;
    }
    let original = std::mem::take(logs);
    for (log, dup) in original.into_iter().zip(dup) {
        if dup {
            logs.push(log.clone());
        }
        logs.push(log);
    }
    true
}

/// Shuffles logs. Lists with two or more logs are guaranteed to come back
/// in a different order.
pub fn reorder_logs(result: &mut Value, rng: &mut Rng) -> bool {
    let Some(logs) = result.as_array_mut() else {
        return false;
    };
    if logs.len() < 2 {
        return false;
    }
    let before = logs.clone();
    rng.shuffle(logs);
    if *logs == before {
        logs.reverse();
    }
    // Identical logs (duplicates) can make every permutation equal.
    *logs != before
}

/// Lowers an `eth_getTransactionCount` result by `lag` (saturating at 0).
pub fn lower_nonce(result: &mut Value, lag: u64) -> bool {
    match parse_quantity(result) {
        Some(n) if n > 0 => {
            *result = quantity(n.saturating_sub(lag));
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn logs(n: u64) -> Value {
        Value::Array((0..n).map(|i| json!({"logIndex": quantity(i)})).collect())
    }

    #[test]
    fn drop_removes_at_least_one() {
        let mut r = logs(10);
        assert!(drop_logs(&mut r, 0.01, &mut Rng::new(1)));
        assert!(r.as_array().unwrap().len() < 10);
        let mut all = logs(10);
        assert!(drop_logs(&mut all, 1.0, &mut Rng::new(1)));
        assert_eq!(all, json!([]));
        let mut empty = logs(0);
        assert!(!drop_logs(&mut empty, 1.0, &mut Rng::new(1)));
    }

    #[test]
    fn duplicate_places_copies_next_to_originals() {
        let mut r = logs(4);
        assert!(duplicate_logs(&mut r, 1.0, &mut Rng::new(3)));
        let indices: Vec<&str> = r
            .as_array()
            .unwrap()
            .iter()
            .map(|l| l["logIndex"].as_str().unwrap())
            .collect();
        assert_eq!(
            indices,
            ["0x0", "0x0", "0x1", "0x1", "0x2", "0x2", "0x3", "0x3"]
        );
    }

    #[test]
    fn reorder_always_changes_order() {
        for seed in 0..50 {
            let mut r = logs(2);
            assert!(reorder_logs(&mut r, &mut Rng::new(seed)));
            assert_eq!(r, json!([{"logIndex": "0x1"}, {"logIndex": "0x0"}]));
        }
        let mut single = logs(1);
        assert!(!reorder_logs(&mut single, &mut Rng::new(0)));
    }

    #[test]
    fn same_seed_same_mutation() {
        let run = |seed| {
            let mut r = logs(20);
            drop_logs(&mut r, 0.5, &mut Rng::new(seed));
            r
        };
        assert_eq!(run(9), run(9));
        assert_ne!(run(9), run(10));
    }

    #[test]
    fn nonce_saturates() {
        let mut r = json!("0x5");
        assert!(lower_nonce(&mut r, 2));
        assert_eq!(r, json!("0x3"));
        let mut r = json!("0x1");
        assert!(lower_nonce(&mut r, 5));
        assert_eq!(r, json!("0x0"));
        let mut r = json!("0x0");
        assert!(!lower_nonce(&mut r, 1));
    }
}
