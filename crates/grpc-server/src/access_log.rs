//! Emit-only data-plane access records (ADR-0009).
//!
//! An [`AccessRecord`] is an unsigned, non-authoritative candidate event: it
//! becomes audit, if anywhere, inside a governance platform's own ledger.
//! This module never stores audit, never chains hashes, never exposes the
//! records over any API, and never blocks the data plane — the spool is a
//! bounded delivery buffer, not a store, and a full queue or a failed write
//! drops the record and counts the drop.
//!
//! Identity fields are exactly as verified by the auth interceptor: when
//! `principal_id` is `null` the request presented no principal credential
//! (legacy token, disabled auth, or unauthenticated loopback), and in that
//! case `workspace_id`/`agent_id` are client-asserted header values, not
//! verified identity.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use akidb_common::config::AccessLogConfig;
use hmac::{Hmac, Mac};
use serde::Serialize;
use sha2::Sha256;
use tokio::sync::mpsc;
use tracing::{error, warn};

/// Environment variable holding the per-deployment HMAC key for query
/// digests. When unset a random per-process key is used: digests stay
/// correlatable within the process lifetime but reveal nothing to an
/// offline reader of the spool.
pub const HASH_KEY_ENV: &str = "AKIDB_ACCESS_LOG_HASH_KEY";

/// One completed data-plane request, keyed-hashed and content-free.
#[derive(Debug, Clone, Serialize)]
pub struct AccessRecord {
    pub record_id: String,
    /// Correlation id shared by every record produced for one client
    /// request (fan-out shards record the same id).
    pub request_id: String,
    pub occurred_at_ms: u64,
    /// Principal id when a principal credential was presented; `null` for the
    /// legacy token, disabled auth, or unauthenticated loopback.
    pub principal_id: Option<String>,
    /// Credential id of the presented principal credential.
    pub credential_id: Option<String>,
    /// Reserved for externally-issued signed tokens (ADR-0010); always
    /// `null` for static-token credentials.
    pub issuer: Option<String>,
    /// Reserved for externally-issued signed tokens (ADR-0010); lets a
    /// governance platform join access records to its issuance ledger.
    pub jti: Option<String>,
    pub authenticated: bool,
    pub workspace_id: String,
    pub agent_id: Option<String>,
    pub collection: String,
    /// RPC name, e.g. `Search`, `TextSearch`, `Insert`.
    pub operation: String,
    /// `ok`, `denied`, or `error`.
    pub outcome: String,
    /// gRPC status code name (`OK`, `NOT_FOUND`, ...), `OK` on success.
    pub status_code: String,
    /// Keyed HMAC-SHA256 over the query material (vector bytes or text plus
    /// filter); never raw content, and not a dictionary-attack channel for
    /// low-entropy query text because the key never leaves the deployment.
    pub query_hash: Option<String>,
    pub hit_count: Option<u32>,
    pub latency_us: u64,
    /// Serving-generation identity when the request was served from a
    /// generation; mirrors the `ServingGenerationEvidence` on the response.
    pub generation_id: Option<String>,
    pub manifest_sha256: Option<String>,
}

impl AccessRecord {
    pub fn new(operation: &str, ctx: &crate::auth::AuthContext, collection: &str) -> Self {
        Self {
            record_id: format!("ar_{}", uuid::Uuid::now_v7().simple()),
            request_id: format!("rq_{}", uuid::Uuid::now_v7().simple()),
            occurred_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0),
            principal_id: ctx.principal_id.clone(),
            credential_id: ctx.credential_id.clone(),
            issuer: None,
            jti: None,
            authenticated: ctx.authenticated,
            workspace_id: ctx.workspace_id.clone(),
            agent_id: ctx.agent_id.clone(),
            collection: collection.to_string(),
            operation: operation.to_string(),
            outcome: "ok".to_string(),
            status_code: "OK".to_string(),
            query_hash: None,
            hit_count: None,
            latency_us: 0,
            generation_id: None,
            manifest_sha256: None,
        }
    }

    pub fn with_query_hash(mut self, query_hash: String) -> Self {
        self.query_hash = Some(query_hash);
        self
    }

    pub fn finish_ok(mut self, hit_count: Option<u32>, latency_us: u64) -> Self {
        self.outcome = "ok".to_string();
        self.status_code = "OK".to_string();
        self.hit_count = hit_count;
        self.latency_us = latency_us;
        self
    }

    pub fn finish_error(mut self, status: &tonic::Code, latency_us: u64) -> Self {
        self.outcome = match status {
            tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => "denied".to_string(),
            _ => "error".to_string(),
        };
        self.status_code = format!("{status:?}").to_uppercase();
        self.latency_us = latency_us;
        self
    }
}

/// Data-plane access-record emitter. `Disabled` is the shipped default and a
/// no-op branch on emit.
#[derive(Clone)]
pub struct AccessLog {
    sender: Option<mpsc::Sender<AccessRecord>>,
    dropped: Arc<AtomicU64>,
    digest_key: Arc<Vec<u8>>,
}

impl AccessLog {
    pub fn disabled() -> Self {
        Self {
            sender: None,
            dropped: Arc::new(AtomicU64::new(0)),
            digest_key: Arc::new(Vec::new()),
        }
    }

    /// Build the emitter from config. Spawns the background writer task; the
    /// task lives as long as the runtime. Returns the disabled emitter when
    /// the config switch is off.
    pub fn spawn(config: &AccessLogConfig) -> std::io::Result<Self> {
        if !config.enabled {
            return Ok(Self::disabled());
        }
        fs::create_dir_all(&config.directory)?;
        let (sender, receiver) = mpsc::channel::<AccessRecord>(config.queue_capacity.max(1));
        let writer = SpoolWriter::new(config, receiver)?;
        tokio::spawn(writer.run());
        Ok(Self {
            sender: Some(sender),
            dropped: Arc::new(AtomicU64::new(0)),
            digest_key: Arc::new(load_digest_key()),
        })
    }

    /// Non-blocking emit. A full queue or a shut-down writer drops the record
    /// and counts the drop; the data plane is never slowed or failed by the
    /// access log.
    pub fn emit(&self, record: AccessRecord) {
        if let Some(sender) = &self.sender {
            if sender.try_send(record).is_err() {
                let total = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if total == 1 || total.is_multiple_of(1024) {
                    warn!(dropped = total, "access-record spool is dropping records");
                }
            }
        }
    }

    /// Keyed HMAC-SHA256 digest of query material. This is the only
    /// representation of query content allowed to leave the process in an
    /// access record. Length-prefixing makes concatenation boundaries
    /// unambiguous.
    pub fn query_digest(&self, parts: &[&[u8]]) -> String {
        let mut mac =
            <Hmac<Sha256> as Mac>::new_from_slice(&self.digest_key).expect("HMAC takes any key");
        for part in parts {
            mac.update(&(part.len() as u64).to_le_bytes());
            mac.update(part);
        }
        format!("{:x}", mac.finalize().into_bytes())
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Whether records will actually be emitted; handlers skip building them
    /// entirely when this is false.
    pub fn is_enabled(&self) -> bool {
        self.sender.is_some()
    }
}

impl Default for AccessLog {
    fn default() -> Self {
        Self::disabled()
    }
}

fn load_digest_key() -> Vec<u8> {
    if let Ok(key) = std::env::var(HASH_KEY_ENV) {
        let key = key.into_bytes();
        if !key.is_empty() {
            return key;
        }
    }
    // No deployment key configured: fall back to a random per-process key so
    // the spool still resists offline dictionary attacks, at the cost of
    // cross-restart correlation.
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    key.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    key
}

struct SpoolWriter {
    directory: PathBuf,
    max_file_bytes: u64,
    max_files: u32,
    receiver: mpsc::Receiver<AccessRecord>,
    current: Option<BufWriter<File>>,
    current_path: PathBuf,
    current_bytes: u64,
    sequence: u64,
}

impl SpoolWriter {
    fn new(
        config: &AccessLogConfig,
        receiver: mpsc::Receiver<AccessRecord>,
    ) -> std::io::Result<Self> {
        let directory = PathBuf::from(&config.directory);
        // Resume after the highest existing spool index so a restart never
        // truncates records written before it.
        let mut sequence = 0u64;
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(index) = name
                .strip_prefix("access-")
                .and_then(|rest| rest.strip_suffix(".jsonl"))
                .and_then(|digits| digits.parse::<u64>().ok())
            {
                sequence = sequence.max(index);
            }
        }
        let mut writer = Self {
            directory,
            max_file_bytes: config.max_file_bytes.max(1024),
            max_files: config.max_files.max(1),
            receiver,
            current: None,
            current_path: PathBuf::new(),
            current_bytes: 0,
            sequence,
        };
        writer.roll()?;
        Ok(writer)
    }

    async fn run(mut self) {
        while let Some(record) = self.receiver.recv().await {
            if let Err(error) = self.append(&record) {
                error!(%error, "access-record spool write failed; record dropped");
            }
        }
        if let Some(file) = self.current.as_mut() {
            let _ = file.flush();
        }
    }

    fn append(&mut self, record: &AccessRecord) -> std::io::Result<()> {
        if self.current_bytes >= self.max_file_bytes {
            self.roll()?;
        }
        let line = serde_json::to_vec(record)?;
        let file = self
            .current
            .as_mut()
            .ok_or_else(|| std::io::Error::other("spool not open"))?;
        file.write_all(&line)?;
        file.write_all(b"\n")?;
        file.flush()?;
        self.current_bytes += line.len() as u64 + 1;
        Ok(())
    }

    fn roll(&mut self) -> std::io::Result<()> {
        if let Some(file) = self.current.as_mut() {
            let _ = file.flush();
        }
        self.sequence += 1;
        self.current_path = self
            .directory
            .join(format!("access-{:06}.jsonl", self.sequence));
        // The spool holds access metadata (principal, workspace, query
        // digests): owner-read/write only, never group/world readable.
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&self.current_path)?;
        self.current_bytes = file.metadata()?.len();
        self.current = Some(BufWriter::new(file));
        self.enforce_retention()?;
        Ok(())
    }

    /// The spool is a delivery buffer, not a store: keep at most `max_files`
    /// rotated files and delete the oldest beyond that bound.
    fn enforce_retention(&self) -> std::io::Result<()> {
        let mut spools: Vec<PathBuf> = Vec::new();
        for entry in fs::read_dir(&self.directory)? {
            let path = entry?.path();
            let name = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            if name.starts_with("access-") && name.ends_with(".jsonl") {
                spools.push(path);
            }
        }
        spools.sort();
        while spools.len() > self.max_files as usize {
            let oldest = spools.remove(0);
            // Never delete the file currently being written.
            if oldest == self.current_path {
                continue;
            }
            fs::remove_file(&oldest)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthContext;
    use std::path::Path;

    fn test_context() -> AuthContext {
        AuthContext {
            workspace_id: "ops".to_string(),
            agent_id: Some("agent-7".to_string()),
            authenticated: true,
            principal_id: Some("principal-1".to_string()),
            credential_id: Some("cred-1".to_string()),
        }
    }

    fn read_spool_lines(directory: &Path) -> Vec<String> {
        let mut lines = Vec::new();
        for entry in fs::read_dir(directory).unwrap() {
            let content = fs::read_to_string(entry.unwrap().path()).unwrap();
            lines.extend(content.lines().map(|line| line.to_string()));
        }
        lines
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spool_writes_emitted_records_as_jsonl() {
        let temporary = tempfile::tempdir().unwrap();
        let config = AccessLogConfig {
            enabled: true,
            directory: temporary.path().display().to_string(),
            ..Default::default()
        };
        let log = AccessLog::spawn(&config).unwrap();

        let record = AccessRecord::new("Search", &test_context(), "docs").finish_ok(Some(5), 1234);
        let record_id = record.record_id.clone();
        log.emit(record);

        // Wait until the writer has drained the queue.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let found = read_spool_lines(temporary.path())
                .iter()
                .any(|line| line.contains(&record_id));
            if found || std::time::Instant::now() > deadline {
                assert!(found, "record {record_id} never reached the spool");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        let line = read_spool_lines(temporary.path())
            .into_iter()
            .find(|line| line.contains(&record_id))
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(parsed["principal_id"], "principal-1");
        assert_eq!(parsed["credential_id"], "cred-1");
        assert_eq!(parsed["workspace_id"], "ops");
        assert_eq!(parsed["collection"], "docs");
        assert_eq!(parsed["operation"], "Search");
        assert_eq!(parsed["outcome"], "ok");
        assert_eq!(parsed["hit_count"], 5);
        assert_eq!(parsed["latency_us"], 1234);
        assert!(parsed["request_id"].as_str().unwrap().starts_with("rq_"));
        assert_eq!(log.dropped_count(), 0);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(log_file_path(temporary.path()))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "spool file must be owner-only");
        }
    }

    #[cfg(unix)]
    fn log_file_path(directory: &Path) -> PathBuf {
        fs::read_dir(directory)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disabled_log_is_a_noop() {
        let log = AccessLog::spawn(&AccessLogConfig::default()).unwrap();
        log.emit(AccessRecord::new("Get", &test_context(), "docs").finish_ok(Some(1), 10));
        assert_eq!(log.dropped_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn full_queue_drops_and_counts_instead_of_blocking() {
        let temporary = tempfile::tempdir().unwrap();
        let config = AccessLogConfig {
            enabled: true,
            directory: temporary.path().display().to_string(),
            queue_capacity: 1,
            ..Default::default()
        };
        let log = AccessLog::spawn(&config).unwrap();
        for _ in 0..100_000 {
            log.emit(AccessRecord::new("Search", &test_context(), "docs").finish_ok(None, 1));
        }
        // 100k records through a capacity-1 channel cannot all fit; drops are
        // the designed behavior and must be counted.
        assert!(log.dropped_count() > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_digest_is_keyed_deterministic_and_length_prefixed() {
        let log = AccessLog::spawn(&AccessLogConfig {
            enabled: true,
            directory: tempfile::tempdir().unwrap().path().display().to_string(),
            ..Default::default()
        })
        .unwrap();
        let first = log.query_digest(&[b"ab".as_slice(), b"c".as_slice()]);
        let second = log.query_digest(&[b"a".as_slice(), b"bc".as_slice()]);
        assert_ne!(first, second);
        assert_eq!(
            first,
            log.query_digest(&[b"ab".as_slice(), b"c".as_slice()])
        );
        assert_eq!(first.len(), 64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_digest_differs_across_keys() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let make = |dir: &Path| {
            AccessLog::spawn(&AccessLogConfig {
                enabled: true,
                directory: dir.display().to_string(),
                ..Default::default()
            })
            .unwrap()
        };
        // Without AKIDB_ACCESS_LOG_HASH_KEY each process instance draws a
        // random key, so two independent logs must not produce the same
        // digest for identical input.
        std::env::remove_var(HASH_KEY_ENV);
        let first = make(dir_a.path());
        let second = make(dir_b.path());
        assert_ne!(
            first.query_digest(&[b"same".as_slice()]),
            second.query_digest(&[b"same".as_slice()])
        );
    }

    #[test]
    fn error_outcome_classifies_denials() {
        let record = AccessRecord::new("Get", &test_context(), "docs")
            .finish_error(&tonic::Code::PermissionDenied, 7);
        assert_eq!(record.outcome, "denied");
        assert_eq!(record.status_code, "PERMISSIONDENIED");
        let record = AccessRecord::new("Get", &test_context(), "docs")
            .finish_error(&tonic::Code::Internal, 7);
        assert_eq!(record.outcome, "error");
    }
}
