//! A coherent view of the chain as seen by a node that lags behind.
//!
//! Used by `stale_head` (every method lags, including `eth_blockNumber`) and
//! `inconsistent_head` (`eth_blockNumber` reports the real head while data
//! methods lag), which reproduces load-balanced providers whose backends
//! disagree.

use serde_json::Value;

use crate::hex::{block_number_of, parse_quantity, quantity};

/// Position of the block parameter for methods that take one.
fn block_param_index(method: &str) -> Option<usize> {
    match method {
        "eth_getBlockByNumber" | "eth_getBlockReceipts" => Some(0),
        "eth_call" | "eth_getBalance" | "eth_getCode" | "eth_getTransactionCount" => Some(1),
        "eth_getStorageAt" => Some(2),
        _ => None,
    }
}

fn is_head_tag(value: &Value) -> bool {
    matches!(value.as_str(), Some("latest" | "pending"))
}

/// Rewrites `latest` / `pending` block tags in request params to the
/// visible head, so state reads resolve against the lagging view.
/// Returns whether anything changed.
pub fn rewrite_request(method: &str, params: &mut Value, visible_head: u64) -> bool {
    let Some(params) = params.as_array_mut() else {
        return false;
    };
    if method == "eth_getLogs" {
        let Some(filter) = params.first_mut().and_then(Value::as_object_mut) else {
            return false;
        };
        if filter.contains_key("blockHash") {
            return false;
        }
        let mut changed = false;
        for key in ["fromBlock", "toBlock"] {
            let entry = filter.entry(key).or_insert(Value::String("latest".into()));
            if is_head_tag(entry) {
                *entry = quantity(visible_head);
                changed = true;
            }
        }
        return changed;
    }
    let Some(index) = block_param_index(method) else {
        return false;
    };
    let len = params.len();
    match params.get_mut(index) {
        Some(tag) if is_head_tag(tag) => {
            *tag = quantity(visible_head);
            true
        }
        Some(_) => false,
        // The block parameter defaults to `latest` when omitted.
        None if len == index && method != "eth_getBlockByNumber" => {
            params.push(quantity(visible_head));
            true
        }
        None => false,
    }
}

/// Hides everything above the visible head in a result. When
/// `include_block_number` is false, `eth_blockNumber` is left alone.
/// Returns whether anything changed.
pub fn rewrite_result(
    method: &str,
    result: &mut Value,
    visible_head: u64,
    include_block_number: bool,
) -> bool {
    let above = |n: Option<u64>| n.is_some_and(|n| n > visible_head);
    match method {
        "eth_blockNumber" if include_block_number => match parse_quantity(result) {
            Some(head) if head > visible_head => {
                *result = quantity(visible_head);
                true
            }
            _ => false,
        },
        "eth_getBlockByNumber" | "eth_getBlockByHash" => {
            if above(result.get("number").and_then(parse_quantity)) {
                *result = Value::Null;
                return true;
            }
            false
        }
        "eth_getBlockReceipts" => {
            let hidden = result
                .as_array()
                .is_some_and(|rs| rs.iter().any(|r| above(block_number_of(r))));
            if hidden {
                *result = Value::Null;
            }
            hidden
        }
        "eth_getLogs" | "eth_getFilterLogs" | "eth_getFilterChanges" => {
            let Some(logs) = result.as_array_mut() else {
                return false;
            };
            let before = logs.len();
            logs.retain(|log| !above(block_number_of(log)));
            logs.len() != before
        }
        "eth_getTransactionReceipt" => {
            if above(block_number_of(result)) {
                *result = Value::Null;
                return true;
            }
            false
        }
        "eth_getTransactionByHash" => {
            if above(block_number_of(result)) {
                make_pending(result);
                return true;
            }
            false
        }
        _ => false,
    }
}

/// Turns a mined transaction object into its pending shape.
pub fn make_pending(tx: &mut Value) {
    if let Some(obj) = tx.as_object_mut() {
        for key in [
            "blockHash",
            "blockNumber",
            "transactionIndex",
            "blockTimestamp",
        ] {
            if obj.contains_key(key) {
                obj.insert(key.to_owned(), Value::Null);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rewrites_head_tags() {
        let mut p = json!(["latest", false]);
        assert!(rewrite_request("eth_getBlockByNumber", &mut p, 97));
        assert_eq!(p, json!(["0x61", false]));

        let mut p = json!([{"to": "0x1"}]);
        assert!(rewrite_request("eth_call", &mut p, 97));
        assert_eq!(p, json!([{"to": "0x1"}, "0x61"]));

        let mut p = json!([{"address": "0x1", "fromBlock": "0x10"}]);
        assert!(rewrite_request("eth_getLogs", &mut p, 97));
        assert_eq!(
            p,
            json!([{"address": "0x1", "fromBlock": "0x10", "toBlock": "0x61"}])
        );

        let mut p = json!([{"blockHash": "0xabc"}]);
        assert!(!rewrite_request("eth_getLogs", &mut p, 97));
        let mut p = json!(["0x10", false]);
        assert!(!rewrite_request("eth_getBlockByNumber", &mut p, 97));
    }

    #[test]
    fn hides_data_above_visible_head() {
        let mut r = json!("0x64");
        assert!(rewrite_result("eth_blockNumber", &mut r, 97, true));
        assert_eq!(r, json!("0x61"));
        let mut r = json!("0x64");
        assert!(!rewrite_result("eth_blockNumber", &mut r, 97, false));

        let mut r = json!({"number": "0x62", "hash": "0x1"});
        assert!(rewrite_result("eth_getBlockByHash", &mut r, 97, true));
        assert_eq!(r, Value::Null);

        let mut r = json!([{"blockNumber": "0x60"}, {"blockNumber": "0x62"}]);
        assert!(rewrite_result("eth_getLogs", &mut r, 97, true));
        assert_eq!(r, json!([{"blockNumber": "0x60"}]));

        let mut r = json!({"blockNumber": "0x62", "status": "0x1"});
        assert!(rewrite_result(
            "eth_getTransactionReceipt",
            &mut r,
            97,
            true
        ));
        assert_eq!(r, Value::Null);

        let mut r = json!({"hash": "0xt", "blockNumber": "0x62", "blockHash": "0xb", "transactionIndex": "0x0"});
        assert!(rewrite_result("eth_getTransactionByHash", &mut r, 97, true));
        assert_eq!(
            r,
            json!({"hash": "0xt", "blockNumber": null, "blockHash": null, "transactionIndex": null})
        );

        let mut r = json!({"blockNumber": "0x61"});
        assert!(!rewrite_result(
            "eth_getTransactionReceipt",
            &mut r,
            97,
            true
        ));
    }
}
