//! Where requests go: a live JSON-RPC endpoint, or a recording.

use std::time::Duration;

use axum::body::Bytes;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use chainchaos_core::recording::{Body, Ids, ReplayIndex, restore_response};
use serde_json::{Value, json};
use url::Url;

/// Response header set when chainchaos itself generated the response
/// (as opposed to relaying the upstream's).
pub(crate) const ERROR_HEADER: HeaderName = HeaderName::from_static("x-chainchaos-error");

/// JSON-RPC error code for requests missing from a recording.
pub(crate) const REPLAY_MISS_CODE: i64 = -32001;

#[derive(Debug)]
pub(crate) struct UpstreamResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpstreamError {
    #[error("upstream timed out: {0}")]
    Timeout(String),
    #[error("upstream request failed: {0}")]
    Failed(String),
}

#[derive(Debug)]
pub(crate) enum Upstream {
    Http {
        client: reqwest::Client,
        url: Url,
    },
    Replay {
        index: ReplayIndex,
        /// Reproduce recorded upstream latency.
        latency: bool,
    },
}

impl Upstream {
    pub async fn send(
        &self,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<UpstreamResponse, UpstreamError> {
        match self {
            Upstream::Http { client, url } => {
                let response = client
                    .post(url.clone())
                    .headers(headers)
                    .body(body)
                    .send()
                    .await
                    .map_err(classify)?;
                let status = response.status();
                let headers = forwardable_headers(response.headers());
                let body = response.bytes().await.map_err(classify)?;
                Ok(UpstreamResponse {
                    status,
                    headers,
                    body,
                })
            }
            Upstream::Replay { index, latency } => Ok(replay(index, *latency, &body).await),
        }
    }

    /// Issues a read-only JSON-RPC call on chainchaos' own behalf (for
    /// example to learn the current head). Never used for transactions.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let body =
            json!({"jsonrpc": "2.0", "id": "chainchaos", "method": method, "params": params});
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let response = self
            .send(headers, Bytes::from(body.to_string()))
            .await
            .map_err(|e| e.to_string())?;
        let value: Value = serde_json::from_slice(&response.body)
            .map_err(|e| format!("{method}: invalid JSON response: {e}"))?;
        if let Some(error) = value.get("error") {
            return Err(format!("{method}: {error}"));
        }
        value
            .get("result")
            .cloned()
            .ok_or_else(|| format!("{method}: response has no result"))
    }
}

fn classify(err: reqwest::Error) -> UpstreamError {
    if err.is_timeout() {
        UpstreamError::Timeout(err.to_string())
    } else {
        UpstreamError::Failed(err.to_string())
    }
}

async fn replay(index: &ReplayIndex, latency: bool, body: &[u8]) -> UpstreamResponse {
    let request = index.normalize(body);
    let mut headers = HeaderMap::new();
    let Some(entry) = index.next(&request) else {
        tracing::warn!("no recorded response for request");
        let id = match &request.ids {
            Ids::Single(id) => id.clone().unwrap_or(Value::Null),
            _ => Value::Null,
        };
        let body = json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": REPLAY_MISS_CODE, "message": "chainchaos: no recorded response for this request"},
        });
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(ERROR_HEADER, HeaderValue::from_static("replay-miss"));
        return UpstreamResponse {
            status: StatusCode::OK,
            headers,
            body: Bytes::from(body.to_string()),
        };
    };
    if latency && entry.latency_ms > 0 {
        tokio::time::sleep(Duration::from_millis(entry.latency_ms)).await;
    }
    let content_type = match entry.response {
        Body::Json(_) => "application/json",
        Body::Text(_) => "text/plain; charset=utf-8",
    };
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    UpstreamResponse {
        status: StatusCode::from_u16(entry.status).unwrap_or(StatusCode::OK),
        headers,
        body: Bytes::from(restore_response(&entry.response, &request.ids)),
    }
}

/// Copies end-to-end headers, dropping hop-by-hop headers and those the HTTP
/// stack recomputes for the new connection.
pub(crate) fn forwardable_headers(headers: &HeaderMap) -> HeaderMap {
    const SKIP: &[HeaderName] = &[
        header::HOST,
        header::CONNECTION,
        header::CONTENT_LENGTH,
        header::TRANSFER_ENCODING,
        header::TE,
        header::TRAILER,
        header::UPGRADE,
        header::PROXY_AUTHORIZATION,
        header::PROXY_AUTHENTICATE,
    ];
    headers
        .iter()
        .filter(|(name, _)| !SKIP.contains(name) && name.as_str() != "keep-alive")
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drops_hop_by_hop_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("localhost"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("close"));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer x"));
        let out = forwardable_headers(&headers);
        assert_eq!(out.len(), 2);
        assert!(out.contains_key(header::CONTENT_TYPE));
        assert!(out.contains_key(header::AUTHORIZATION));
    }
}
