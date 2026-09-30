//! Shard-side bounded admission for synchronous index/storage work.
//!
//! Data-plane operations execute USearch HNSW search, RocksDB reads/writes,
//! BM25, and graph expansion inline on tokio worker threads. Under overload,
//! unbounded concurrency pins every worker behind synchronous work and
//! stalls health probes and deadline handling. [`AdmissionController`] bounds
//! concurrent executions with class permits held for the full synchronous
//! lifetime of each operation; excess executions fail fast with
//! RESOURCE_EXHAUSTED (matching the coordinator's admission behavior)
//! instead of queueing unbounded work.
//!
//! This implements the follow-up identified by the ANN service-load
//! investigation (`.internal/analysis/ann-service-load-report.md`): permits
//! are held for the actual work lifetime and the client deadline is checked
//! before expensive work starts. Timing out an async wrapper around blocking
//! work is deliberately not used — a timed-out task would keep running after
//! the caller already gave up.

use std::sync::Arc;
use std::time::{Duration, Instant};

use akidb_common::config::AdmissionConfig;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Code, Status};

use crate::metrics::metrics;

/// gRPC deadline metadata header (`grpc-timeout`), sent by clients per the
/// gRPC protocol. tonic surfaces it as request metadata; the shard checks it
/// before starting expensive synchronous work.
pub const GRPC_TIMEOUT_HEADER: &str = "grpc-timeout";

/// Data-plane permit classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionClass {
    /// Reads: Search / SearchBatch / TextSearch / Get.
    Search,
    /// Mutations: Insert / InsertBatch / Update / Delete.
    Mutation,
}

impl AdmissionClass {
    fn label(self) -> &'static str {
        match self {
            Self::Search => "search",
            Self::Mutation => "mutation",
        }
    }

    fn saturation_message(self) -> &'static str {
        match self {
            Self::Search => "shard admission limit reached for read operations",
            Self::Mutation => "shard admission limit reached for mutation operations",
        }
    }
}

/// Bounds concurrent synchronous data-plane executions on this shard.
///
/// A disabled controller performs no semaphore accounting at all, so the
/// admit path stays branch-cheap and never rejects; this preserves the
/// pre-admission behavior for deployments that have not opted in.
#[derive(Debug, Clone)]
pub struct AdmissionController {
    enabled: bool,
    search: Arc<Semaphore>,
    mutation: Arc<Semaphore>,
    acquire_timeout: Duration,
}

impl Default for AdmissionController {
    fn default() -> Self {
        Self::disabled()
    }
}

impl AdmissionController {
    /// Controller that never rejects (the shipped default).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            search: Arc::new(Semaphore::new(1)),
            mutation: Arc::new(Semaphore::new(1)),
            acquire_timeout: Duration::ZERO,
        }
    }

    /// Controller sized from operator configuration.
    pub fn new(config: &AdmissionConfig) -> Self {
        if !config.enabled {
            return Self::disabled();
        }
        Self {
            enabled: true,
            search: Arc::new(Semaphore::new(config.search_permits.max(1))),
            mutation: Arc::new(Semaphore::new(config.mutation_permits.max(1))),
            acquire_timeout: Duration::from_millis(config.acquire_timeout_ms),
        }
    }

    /// Whether admission enforcement is active (false = no accounting).
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn semaphore(&self, class: AdmissionClass) -> &Arc<Semaphore> {
        match class {
            AdmissionClass::Search => &self.search,
            AdmissionClass::Mutation => &self.mutation,
        }
    }

    /// Acquire a permit for `class`, rejecting with RESOURCE_EXHAUSTED when
    /// the class is saturated and no permit frees within the configured wait.
    ///
    /// The returned guard must be held until the synchronous work finishes so
    /// the permit tracks the actual work lifetime, not a timeout boundary.
    pub async fn acquire(&self, class: AdmissionClass) -> Result<AdmissionGuard, Status> {
        if !self.enabled {
            return Ok(AdmissionGuard { inner: None });
        }
        let semaphore = self.semaphore(class);
        let permit = if self.acquire_timeout.is_zero() {
            semaphore
                .clone()
                .try_acquire_owned()
                .map_err(|_| reject(class))?
        } else {
            match tokio::time::timeout(self.acquire_timeout, semaphore.clone().acquire_owned())
                .await
            {
                Ok(Ok(permit)) => permit,
                // The semaphores are never closed; treat a closed error as a
                // defensive rejection rather than panicking.
                Ok(Err(_)) | Err(_) => return Err(reject(class)),
            }
        };
        metrics()
            .admission_in_flight
            .with_label_values(&[class.label()])
            .inc();
        Ok(AdmissionGuard {
            inner: Some(GuardInner { permit, class }),
        })
    }
}

#[derive(Debug)]
struct GuardInner {
    #[allow(dead_code)]
    permit: OwnedSemaphorePermit,
    class: AdmissionClass,
}

/// Permit held for the entire synchronous lifetime of an admitted operation.
/// A guard from a disabled controller carries no permit and does no metric
/// accounting.
#[derive(Debug)]
pub struct AdmissionGuard {
    inner: Option<GuardInner>,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            metrics()
                .admission_in_flight
                .with_label_values(&[inner.class.label()])
                .dec();
        }
    }
}

fn reject(class: AdmissionClass) -> Status {
    metrics()
        .admission_rejections_total
        .with_label_values(&[class.label()])
        .inc();
    Status::new(Code::ResourceExhausted, class.saturation_message())
}

/// Run synchronous work without starving the tokio event loop.
///
/// On a multi-thread runtime the closure runs under `block_in_place` so the
/// runtime moves the remaining task queue off this worker while the
/// synchronous section executes; combined with the admission permits this is
/// the bounded executor for synchronous ANN/storage work. On current-thread
/// runtimes (unit tests) or outside a runtime the closure runs inline — there
/// is no event loop to starve.
pub fn run_blocking<T>(work: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(work),
        _ => work(),
    }
}

/// Client-declared gRPC timeout carried on the request metadata, if present
/// and parseable.
///
/// The wire format is `TimeoutValue Unit` with 1-8 ASCII digits and a unit of
/// `H`, `M`, `S`, `m`, `u`, or `n`. Anything else — absent, malformed, or
/// overflowing — yields `None`: admission treats an unreadable deadline as no
/// deadline rather than guessing.
pub fn client_timeout(metadata: &tonic::metadata::MetadataMap) -> Option<Duration> {
    let header = metadata.get(GRPC_TIMEOUT_HEADER)?.to_str().ok()?;
    let (unit_byte, digit_bytes) = header.as_bytes().split_last()?;
    if digit_bytes.is_empty()
        || digit_bytes.len() > 8
        || !digit_bytes.iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    let value: u64 = std::str::from_utf8(digit_bytes).ok()?.parse().ok()?;
    let nanos_per_unit: u64 = match unit_byte {
        b'H' => 3_600_000_000_000,
        b'M' => 60_000_000_000,
        b'S' => 1_000_000_000,
        b'm' => 1_000_000,
        b'u' => 1_000,
        b'n' => 1,
        _ => return None,
    };
    Some(Duration::from_nanos(value.saturating_mul(nanos_per_unit)))
}

/// Reject with DEADLINE_EXCEEDED once a request can no longer beat its
/// client-declared timeout.
///
/// `received` is the handler-entry instant. Because the client sets its
/// deadline no later than transmission, deadline ≤ received + timeout; when
/// `received.elapsed()` reaches the timeout, the deadline has certainly
/// passed. The check never rejects a request that could still make its
/// deadline — it only under-detects expiration that happened in transit.
pub fn check_deadline(received: Instant, timeout: Option<Duration>) -> Result<(), Status> {
    match timeout {
        Some(timeout) if received.elapsed() >= timeout => Err(Status::new(
            Code::DeadlineExceeded,
            "grpc deadline expired before expensive work could start",
        )),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataMap;

    fn metadata_with_timeout(value: &str) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert(GRPC_TIMEOUT_HEADER, value.parse().unwrap());
        metadata
    }

    #[test]
    fn disabled_controller_never_rejects() {
        let controller = AdmissionController::disabled();
        assert!(!controller.is_enabled());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let _first = controller.acquire(AdmissionClass::Search).await.unwrap();
            let _second = controller.acquire(AdmissionClass::Search).await.unwrap();
            let _mutation = controller.acquire(AdmissionClass::Mutation).await.unwrap();
        });
    }

    #[tokio::test]
    async fn fail_fast_rejects_when_class_is_saturated() {
        let controller = AdmissionController::new(&AdmissionConfig {
            enabled: true,
            search_permits: 1,
            mutation_permits: 1,
            acquire_timeout_ms: 0,
        });
        let _held = controller.acquire(AdmissionClass::Search).await.unwrap();
        let err = controller
            .acquire(AdmissionClass::Search)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
        // Other class is independent.
        let _mutation = controller.acquire(AdmissionClass::Mutation).await.unwrap();
    }

    #[tokio::test]
    async fn bounded_wait_acquires_after_release() {
        let controller = AdmissionController::new(&AdmissionConfig {
            enabled: true,
            search_permits: 1,
            mutation_permits: 1,
            acquire_timeout_ms: 5_000,
        });
        let guard = controller.acquire(AdmissionClass::Search).await.unwrap();
        let pending_controller = controller.clone();
        let pending = tokio::spawn(async move {
            pending_controller
                .acquire(AdmissionClass::Search)
                .await
                .unwrap()
        });
        tokio::task::yield_now().await;
        drop(guard);
        let _permit = pending.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_wait_times_out_with_resource_exhausted() {
        let controller = AdmissionController::new(&AdmissionConfig {
            enabled: true,
            search_permits: 1,
            mutation_permits: 1,
            acquire_timeout_ms: 25,
        });
        let _held = controller.acquire(AdmissionClass::Search).await.unwrap();
        let err = controller
            .acquire(AdmissionClass::Search)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
    }

    #[test]
    fn client_timeout_parses_all_units() {
        assert_eq!(
            client_timeout(&metadata_with_timeout("1H")),
            Some(Duration::from_secs(3600))
        );
        assert_eq!(
            client_timeout(&metadata_with_timeout("2M")),
            Some(Duration::from_secs(120))
        );
        assert_eq!(
            client_timeout(&metadata_with_timeout("3S")),
            Some(Duration::from_secs(3))
        );
        assert_eq!(
            client_timeout(&metadata_with_timeout("4m")),
            Some(Duration::from_millis(4))
        );
        assert_eq!(
            client_timeout(&metadata_with_timeout("5u")),
            Some(Duration::from_micros(5))
        );
        assert_eq!(
            client_timeout(&metadata_with_timeout("6n")),
            Some(Duration::from_nanos(6))
        );
    }

    #[test]
    fn client_timeout_ignores_malformed_values() {
        assert_eq!(client_timeout(&MetadataMap::new()), None);
        assert_eq!(client_timeout(&metadata_with_timeout("")), None);
        assert_eq!(client_timeout(&metadata_with_timeout("S")), None);
        assert_eq!(client_timeout(&metadata_with_timeout("123456789S")), None);
        assert_eq!(client_timeout(&metadata_with_timeout("10x")), None);
        assert_eq!(client_timeout(&metadata_with_timeout("-5S")), None);
        assert_eq!(client_timeout(&metadata_with_timeout("5.5S")), None);
    }

    #[test]
    fn check_deadline_rejects_only_certain_expiration() {
        let received = Instant::now();
        assert!(check_deadline(received, None).is_ok());
        assert!(check_deadline(received, Some(Duration::ZERO)).is_err());
        assert!(check_deadline(received, Some(Duration::from_secs(30))).is_ok());
    }
}
