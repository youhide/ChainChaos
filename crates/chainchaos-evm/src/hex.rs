//! JSON-RPC hex quantities (`"0x1a"`).

use serde_json::Value;

/// Parses a hex quantity such as `"0x1a"`.
pub fn parse_quantity(value: &Value) -> Option<u64> {
    let s = value.as_str()?.strip_prefix("0x")?;
    if s.is_empty() {
        return None;
    }
    u64::from_str_radix(s, 16).ok()
}

/// Formats a number as a hex quantity.
pub fn quantity(n: u64) -> Value {
    Value::String(format!("{n:#x}"))
}

/// Reads the `blockNumber` field of a log, receipt or transaction.
pub fn block_number_of(object: &Value) -> Option<u64> {
    object.get("blockNumber").and_then(parse_quantity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trips() {
        assert_eq!(parse_quantity(&json!("0x1a")), Some(26));
        assert_eq!(parse_quantity(&json!("0x0")), Some(0));
        assert_eq!(parse_quantity(&json!("0x")), None);
        assert_eq!(parse_quantity(&json!("26")), None);
        assert_eq!(parse_quantity(&json!(26)), None);
        assert_eq!(quantity(26), json!("0x1a"));
        assert_eq!(quantity(0), json!("0x0"));
    }
}
