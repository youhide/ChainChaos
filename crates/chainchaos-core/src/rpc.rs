//! Minimal, read-only inspection of JSON-RPC 2.0 requests.
//!
//! The proxy forwards the original request bytes untouched. This module only
//! extracts what fault matching and logging need: the method name and the id
//! of each call. Anything that does not look like JSON-RPC is simply reported
//! as "not inspectable" and still forwarded.

use serde_json::Value;

/// A single call inside a JSON-RPC request (or one element of a batch).
#[derive(Debug, Clone, PartialEq)]
pub struct RpcCall {
    /// The `method` field, if present and a string.
    pub method: Option<String>,
    /// The raw `id` field. `None` means the field was absent (a notification).
    pub id: Option<Value>,
}

/// The inspectable parts of an incoming JSON-RPC request.
#[derive(Debug, Clone, PartialEq)]
pub struct RpcRequest {
    pub calls: Vec<RpcCall>,
    pub batch: bool,
}

impl RpcRequest {
    /// Parses a request body. Returns `None` if the body is not a JSON object
    /// or array of objects; the caller should forward such bodies unchanged.
    pub fn parse(body: &[u8]) -> Option<Self> {
        match serde_json::from_slice::<Value>(body).ok()? {
            Value::Object(obj) => Some(Self {
                calls: vec![call_from_object(&obj)],
                batch: false,
            }),
            Value::Array(items) => {
                let calls = items
                    .iter()
                    .map(|item| item.as_object().map(call_from_object))
                    .collect::<Option<Vec<_>>>()?;
                Some(Self { calls, batch: true })
            }
            _ => None,
        }
    }

    /// Iterates over the method names in this request, skipping calls
    /// without a string method.
    pub fn methods(&self) -> impl Iterator<Item = &str> {
        self.calls.iter().filter_map(|c| c.method.as_deref())
    }

    /// A compact human-readable summary for logs, e.g. `eth_blockNumber` or
    /// `[eth_call, eth_getBalance]`.
    pub fn methods_summary(&self) -> String {
        let methods: Vec<&str> = self
            .calls
            .iter()
            .map(|c| c.method.as_deref().unwrap_or("<none>"))
            .collect();
        if self.batch {
            format!("[{}]", methods.join(", "))
        } else {
            methods.join(", ")
        }
    }

    /// The id to use when chainchaos itself must produce an error response.
    ///
    /// For a single call this is the call's id (or `null`). For a batch the
    /// JSON-RPC spec allows a single error object with a `null` id when the
    /// batch as a whole fails, which is what chainchaos returns.
    pub fn error_id(&self) -> Value {
        match (self.batch, self.calls.first()) {
            (false, Some(call)) => call.id.clone().unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }
}

fn call_from_object(obj: &serde_json::Map<String, Value>) -> RpcCall {
    RpcCall {
        method: obj.get("method").and_then(Value::as_str).map(str::to_owned),
        id: obj.get("id").cloned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_single_call() {
        let req = RpcRequest::parse(
            br#"{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}"#,
        )
        .unwrap();
        assert!(!req.batch);
        assert_eq!(req.methods().collect::<Vec<_>>(), ["eth_blockNumber"]);
        assert_eq!(req.error_id(), json!(7));
    }

    #[test]
    fn parses_batch() {
        let req = RpcRequest::parse(
            br#"[{"jsonrpc":"2.0","id":"a","method":"eth_call"},{"jsonrpc":"2.0","id":2,"method":"eth_getBalance"}]"#,
        )
        .unwrap();
        assert!(req.batch);
        assert_eq!(req.methods_summary(), "[eth_call, eth_getBalance]");
        assert_eq!(req.error_id(), Value::Null);
    }

    #[test]
    fn notification_has_no_id() {
        let req = RpcRequest::parse(br#"{"jsonrpc":"2.0","method":"eth_foo"}"#).unwrap();
        assert_eq!(req.calls[0].id, None);
        assert_eq!(req.error_id(), Value::Null);
    }

    #[test]
    fn rejects_non_rpc_bodies() {
        assert!(RpcRequest::parse(b"not json").is_none());
        assert!(RpcRequest::parse(b"42").is_none());
        assert!(RpcRequest::parse(b"[1, 2]").is_none());
    }
}
