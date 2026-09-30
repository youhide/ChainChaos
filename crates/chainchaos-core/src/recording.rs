//! The `.ccr` recording format used by `chainchaos record` and `replay`.
//!
//! A recording is JSON Lines: one header line, then one line per recorded
//! exchange.
//!
//! ```text
//! {"format":"chainchaos-recording","version":1,"recorded_at_unix_ms":…,"upstream":"http://127.0.0.1:8545/","redacted_fields":[]}
//! {"seq":1,"offset_ms":0,"latency_ms":3,"request":{"json":{…}},"status":200,"response":{"json":{…}}}
//! ```
//!
//! Design notes:
//!
//! - **Request ids are normalised away.** Ids are client-chosen and differ
//!   between runs, so they are stripped from stored requests and responses
//!   and restored from the live request at replay time.
//! - **Matching is by content.** A request is keyed by its canonical JSON
//!   (sorted keys, ids removed). Repeated identical requests replay their
//!   recorded responses in order; the last one repeats once exhausted. This
//!   keeps replay correct under concurrency, where arrival order can differ.
//! - **Wall-clock data is informational.** `recorded_at_unix_ms`, `offset_ms`
//!   and `latency_ms` are kept for inspection and optional latency replay,
//!   but never affect matching.
//! - **Redaction happens before keying**, on both sides, so redacted fields
//!   still match at replay time.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const FORMAT: &str = "chainchaos-recording";
pub const VERSION: u32 = 1;
pub const REDACTED: &str = "<redacted>";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Header {
    pub format: String,
    pub version: u32,
    pub recorded_at_unix_ms: u64,
    /// Upstream URL with credentials removed.
    pub upstream: String,
    #[serde(default)]
    pub redacted_fields: Vec<String>,
}

impl Header {
    pub fn new(recorded_at_unix_ms: u64, upstream: String, redacted_fields: Vec<String>) -> Self {
        Self {
            format: FORMAT.to_owned(),
            version: VERSION,
            recorded_at_unix_ms,
            upstream,
            redacted_fields,
        }
    }
}

/// A request or response body: JSON when it parses, text otherwise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Body {
    Json(Value),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub seq: u64,
    /// When the request arrived, relative to the start of recording.
    pub offset_ms: u64,
    /// How long the upstream took to answer.
    pub latency_ms: u64,
    pub request: Body,
    pub status: u16,
    pub response: Body,
}

#[derive(Debug, thiserror::Error)]
pub enum RecordingError {
    #[error("failed to access recording {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid recording {path} at line {line}: {message}")]
    Invalid {
        path: PathBuf,
        line: usize,
        message: String,
    },
}

/// Replaces the values of configured object keys (case-insensitive,
/// anywhere in the document) with a placeholder.
#[derive(Debug, Clone, Default)]
pub struct Redactor {
    fields: Vec<String>,
}

impl Redactor {
    pub fn new(fields: &[String]) -> Self {
        Self {
            fields: fields.iter().map(|f| f.to_ascii_lowercase()).collect(),
        }
    }

    pub fn apply(&self, value: &mut Value) {
        if self.fields.is_empty() {
            return;
        }
        match value {
            Value::Object(map) => {
                for (key, v) in map.iter_mut() {
                    if self.fields.contains(&key.to_ascii_lowercase()) {
                        *v = Value::String(REDACTED.to_owned());
                    } else {
                        self.apply(v);
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|v| self.apply(v)),
            _ => {}
        }
    }
}

/// The ids removed from a request, needed to restore a response.
#[derive(Debug, Clone, PartialEq)]
pub enum Ids {
    /// The body was not JSON.
    None,
    Single(Option<Value>),
    Batch(Vec<Option<Value>>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedRequest {
    pub body: Body,
    /// Canonical matching key.
    pub key: String,
    pub ids: Ids,
}

pub fn normalize_request(bytes: &[u8], redactor: &Redactor) -> NormalizedRequest {
    let Ok(mut value) = serde_json::from_slice::<Value>(bytes) else {
        let text = String::from_utf8_lossy(bytes).into_owned();
        return NormalizedRequest {
            key: format!("text:{text}"),
            body: Body::Text(text),
            ids: Ids::None,
        };
    };
    let ids = match &mut value {
        Value::Object(obj) => Ids::Single(obj.remove("id")),
        Value::Array(items) => Ids::Batch(
            items
                .iter_mut()
                .map(|item| item.as_object_mut().and_then(|o| o.remove("id")))
                .collect(),
        ),
        _ => Ids::Single(None),
    };
    redactor.apply(&mut value);
    NormalizedRequest {
        key: canonical(&value),
        body: Body::Json(value),
        ids,
    }
}

/// Strips ids from a response. Batch responses are reordered to follow the
/// request order, so ids can be restored by position.
pub fn normalize_response(bytes: &[u8], ids: &Ids, redactor: &Redactor) -> Body {
    let Ok(mut value) = serde_json::from_slice::<Value>(bytes) else {
        return Body::Text(String::from_utf8_lossy(bytes).into_owned());
    };
    match (&mut value, ids) {
        (Value::Object(obj), _) => {
            obj.remove("id");
        }
        (Value::Array(items), Ids::Batch(request_ids)) => {
            let mut remaining: Vec<Value> = std::mem::take(items);
            let mut ordered = Vec::with_capacity(remaining.len());
            for id in request_ids.iter().flatten() {
                if let Some(pos) = remaining.iter().position(|r| r.get("id") == Some(id)) {
                    ordered.push(remaining.remove(pos));
                }
            }
            ordered.extend(remaining);
            for item in &mut ordered {
                if let Some(obj) = item.as_object_mut() {
                    obj.remove("id");
                }
            }
            *items = ordered;
        }
        _ => {}
    }
    redactor.apply(&mut value);
    Body::Json(value)
}

/// Rebuilds a response body for a live request, restoring its ids.
pub fn restore_response(body: &Body, ids: &Ids) -> Vec<u8> {
    match body {
        Body::Text(text) => text.clone().into_bytes(),
        Body::Json(value) => {
            let mut value = value.clone();
            match (&mut value, ids) {
                (Value::Object(obj), Ids::Single(id)) => {
                    obj.insert("id".to_owned(), id.clone().unwrap_or(Value::Null));
                }
                (Value::Array(items), Ids::Batch(request_ids)) => {
                    let mut live = request_ids.iter().flatten();
                    for item in items.iter_mut() {
                        if let Some(obj) = item.as_object_mut() {
                            let id = live.next().cloned().unwrap_or(Value::Null);
                            obj.insert("id".to_owned(), id);
                        }
                    }
                }
                _ => {}
            }
            serde_json::to_vec(&value).unwrap_or_default()
        }
    }
}

/// Serialises JSON with object keys sorted, independent of serde_json's
/// `preserve_order` feature.
pub fn canonical(value: &Value) -> String {
    fn sort(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                let sorted: Map<String, Value> = keys
                    .into_iter()
                    .map(|k| (k.clone(), sort(&map[k])))
                    .collect();
                Value::Object(sorted)
            }
            Value::Array(items) => Value::Array(items.iter().map(sort).collect()),
            other => other.clone(),
        }
    }
    sort(value).to_string()
}

/// Appends entries to a recording file, flushing each line so an
/// interrupted recording is still usable.
#[derive(Debug)]
pub struct RecordingWriter {
    path: PathBuf,
    file: Mutex<BufWriter<File>>,
}

impl RecordingWriter {
    pub fn create(path: impl AsRef<Path>, header: &Header) -> Result<Self, RecordingError> {
        let path = path.as_ref().to_owned();
        let io = |source| RecordingError::Io {
            path: path.clone(),
            source,
        };
        let file = File::create(&path).map_err(io)?;
        let writer = Self {
            path: path.clone(),
            file: Mutex::new(BufWriter::new(file)),
        };
        writer.write_line(&serde_json::to_string(header).unwrap_or_default())?;
        Ok(writer)
    }

    pub fn append(&self, entry: &Entry) -> Result<(), RecordingError> {
        self.write_line(&serde_json::to_string(entry).unwrap_or_default())
    }

    fn write_line(&self, line: &str) -> Result<(), RecordingError> {
        let mut file = self.file.lock().unwrap_or_else(|e| e.into_inner());
        writeln!(file, "{line}")
            .and_then(|()| file.flush())
            .map_err(|source| RecordingError::Io {
                path: self.path.clone(),
                source,
            })
    }
}

/// A loaded recording.
#[derive(Debug, Clone, PartialEq)]
pub struct Recording {
    pub header: Header,
    pub entries: Vec<Entry>,
}

impl Recording {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, RecordingError> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|source| RecordingError::Io {
            path: path.to_owned(),
            source,
        })?;
        let invalid = |line: usize, message: String| RecordingError::Invalid {
            path: path.to_owned(),
            line,
            message,
        };
        let mut lines = BufReader::new(file).lines().enumerate();
        let header_line = match lines.next() {
            Some((_, Ok(line))) => line,
            Some((_, Err(source))) => {
                return Err(RecordingError::Io {
                    path: path.to_owned(),
                    source,
                });
            }
            None => return Err(invalid(1, "empty file".into())),
        };
        let header: Header =
            serde_json::from_str(&header_line).map_err(|e| invalid(1, e.to_string()))?;
        if header.format != FORMAT {
            return Err(invalid(
                1,
                format!("not a chainchaos recording (format `{}`)", header.format),
            ));
        }
        if header.version != VERSION {
            return Err(invalid(
                1,
                format!(
                    "unsupported recording version {} (this build reads version {VERSION})",
                    header.version
                ),
            ));
        }
        let mut entries = Vec::new();
        for (index, line) in lines {
            let line = line.map_err(|source| RecordingError::Io {
                path: path.to_owned(),
                source,
            })?;
            if line.trim().is_empty() {
                continue;
            }
            let entry: Entry =
                serde_json::from_str(&line).map_err(|e| invalid(index + 1, e.to_string()))?;
            entries.push(entry);
        }
        // Concurrent requests may finish out of order; replay by arrival.
        entries.sort_by_key(|e| e.seq);
        Ok(Self { header, entries })
    }
}

/// Looks up recorded responses by request content.
#[derive(Debug)]
pub struct ReplayIndex {
    recording: Recording,
    redactor: Redactor,
    by_key: HashMap<String, (Vec<usize>, AtomicUsize)>,
}

impl ReplayIndex {
    pub fn new(recording: Recording) -> Self {
        let redactor = Redactor::new(&recording.header.redacted_fields);
        let mut by_key: HashMap<String, (Vec<usize>, AtomicUsize)> = HashMap::new();
        for (index, entry) in recording.entries.iter().enumerate() {
            let key = match &entry.request {
                Body::Json(value) => canonical(value),
                Body::Text(text) => format!("text:{text}"),
            };
            by_key
                .entry(key)
                .or_insert_with(|| (Vec::new(), AtomicUsize::new(0)))
                .0
                .push(index);
        }
        Self {
            recording,
            redactor,
            by_key,
        }
    }

    pub fn header(&self) -> &Header {
        &self.recording.header
    }

    pub fn len(&self) -> usize {
        self.recording.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.recording.entries.is_empty()
    }

    /// Normalises a live request with the recording's redaction settings.
    pub fn normalize(&self, bytes: &[u8]) -> NormalizedRequest {
        normalize_request(bytes, &self.redactor)
    }

    /// The next recorded response for this request, if any.
    pub fn next(&self, request: &NormalizedRequest) -> Option<&Entry> {
        let (indices, cursor) = self.by_key.get(&request.key)?;
        let n = cursor
            .fetch_add(1, Ordering::Relaxed)
            .min(indices.len() - 1);
        Some(&self.recording.entries[indices[n]])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keys_ignore_ids_and_key_order() {
        let r = Redactor::default();
        let a = normalize_request(
            br#"{"id":1,"method":"eth_call","params":[{"to":"0x1","data":"0x"}]}"#,
            &r,
        );
        let b = normalize_request(
            br#"{"params":[{"data":"0x","to":"0x1"}],"method":"eth_call","id":"x"}"#,
            &r,
        );
        assert_eq!(a.key, b.key);
        assert_eq!(a.ids, Ids::Single(Some(json!(1))));
    }

    #[test]
    fn restores_single_and_batch_ids() {
        let r = Redactor::default();
        let req = normalize_request(br#"[{"id":"a","method":"m1"},{"id":2,"method":"m2"}]"#, &r);
        // Upstream answered out of order.
        let body = normalize_response(
            br#"[{"id":2,"result":"two"},{"id":"a","result":"one"}]"#,
            &req.ids,
            &r,
        );
        let live = normalize_request(br#"[{"id":10,"method":"m1"},{"id":11,"method":"m2"}]"#, &r);
        let restored: Value = serde_json::from_slice(&restore_response(&body, &live.ids)).unwrap();
        assert_eq!(
            restored,
            json!([{"id":10,"result":"one"},{"id":11,"result":"two"}])
        );

        let single = normalize_request(br#"{"id":5,"method":"m"}"#, &r);
        let body = normalize_response(
            br#"{"jsonrpc":"2.0","id":5,"result":"0x1"}"#,
            &single.ids,
            &r,
        );
        let live = normalize_request(br#"{"id":"z","method":"m"}"#, &r);
        let restored: Value = serde_json::from_slice(&restore_response(&body, &live.ids)).unwrap();
        assert_eq!(restored, json!({"jsonrpc":"2.0","id":"z","result":"0x1"}));
    }

    #[test]
    fn redaction_applies_before_keying() {
        let r = Redactor::new(&["apiKey".to_owned()]);
        let a = normalize_request(
            br#"{"id":1,"method":"m","params":[{"apikey":"secret1"}]}"#,
            &r,
        );
        let b = normalize_request(
            br#"{"id":1,"method":"m","params":[{"apikey":"secret2"}]}"#,
            &r,
        );
        assert_eq!(a.key, b.key);
        assert!(!a.key.contains("secret"));
    }

    #[test]
    fn replay_index_serves_in_order_then_repeats() {
        let r = Redactor::default();
        let mk = |seq, result: &str| Entry {
            seq,
            offset_ms: 0,
            latency_ms: 0,
            request: normalize_request(br#"{"id":1,"method":"eth_blockNumber"}"#, &r).body,
            status: 200,
            response: Body::Json(json!({"jsonrpc":"2.0","result":result})),
        };
        let index = ReplayIndex::new(Recording {
            header: Header::new(0, "http://x/".into(), vec![]),
            entries: vec![mk(2, "0x2"), mk(1, "0x1")],
        });
        let req = index.normalize(br#"{"id":9,"method":"eth_blockNumber"}"#);
        let results: Vec<Value> = (0..3)
            .map(|_| match &index.next(&req).unwrap().response {
                Body::Json(v) => v["result"].clone(),
                Body::Text(_) => unreachable!(),
            })
            .collect();
        // Recording::load sorts by seq; ReplayIndex::new keeps given order.
        assert_eq!(results, [json!("0x2"), json!("0x1"), json!("0x1")]);
        assert!(
            index
                .next(&index.normalize(br#"{"id":1,"method":"other"}"#))
                .is_none()
        );
    }

    #[test]
    fn writes_and_loads_round_trip() {
        let dir = std::env::temp_dir().join(format!("chainchaos-rec-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.ccr");
        let header = Header::new(1, "http://127.0.0.1:8545/".into(), vec!["apiKey".into()]);
        let writer = RecordingWriter::create(&path, &header).unwrap();
        let entry = Entry {
            seq: 1,
            offset_ms: 5,
            latency_ms: 2,
            request: Body::Json(json!({"method":"eth_chainId"})),
            status: 200,
            response: Body::Text("ok".into()),
        };
        writer.append(&entry).unwrap();
        drop(writer);
        let loaded = Recording::load(&path).unwrap();
        assert_eq!(loaded.header, header);
        assert_eq!(loaded.entries, vec![entry]);

        std::fs::write(
            &path,
            "{\"format\":\"other\",\"version\":1,\"recorded_at_unix_ms\":0,\"upstream\":\"\"}\n",
        )
        .unwrap();
        assert!(
            Recording::load(&path)
                .unwrap_err()
                .to_string()
                .contains("not a chainchaos recording")
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
