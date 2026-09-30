//! Recording of live traffic for `chainchaos record`.

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chainchaos_core::recording::{
    Entry, Header, RecordingError, RecordingWriter, Redactor, normalize_request, normalize_response,
};
use tracing::warn;

/// Settings for recording traffic to a `.ccr` file.
#[derive(Debug, Clone)]
pub struct RecordConfig {
    pub output: PathBuf,
    /// Object keys whose values are replaced before writing.
    pub redact_fields: Vec<String>,
    /// Upstream description stored in the header (credentials removed).
    pub upstream_label: String,
}

#[derive(Debug)]
pub(crate) struct Recorder {
    writer: RecordingWriter,
    redactor: Redactor,
}

impl Recorder {
    pub fn create(config: &RecordConfig) -> Result<Self, RecordingError> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);
        let header = Header::new(
            now_ms,
            config.upstream_label.clone(),
            config.redact_fields.clone(),
        );
        Ok(Self {
            writer: RecordingWriter::create(&config.output, &header)?,
            redactor: Redactor::new(&config.redact_fields),
        })
    }

    pub fn record(
        &self,
        seq: u64,
        offset: Duration,
        latency: Duration,
        request: &[u8],
        status: u16,
        response: &[u8],
    ) {
        let normalized = normalize_request(request, &self.redactor);
        let entry = Entry {
            seq,
            offset_ms: offset.as_millis() as u64,
            latency_ms: latency.as_millis() as u64,
            response: normalize_response(response, &normalized.ids, &self.redactor),
            request: normalized.body,
            status,
        };
        if let Err(e) = self.writer.append(&entry) {
            warn!(error = %e, "failed to write recording entry");
        }
    }
}
