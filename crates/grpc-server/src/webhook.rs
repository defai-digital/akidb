//! Webhook alerting for background task events
//!
//! Provides webhook notifications for:
//! - Task lifecycle events (started, completed, failed)
//! - Snapshot and rebuild completions
//! - SLO breaches and resource exhaustion

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

/// One queued webhook delivery attempt.
#[derive(Debug, Clone)]
struct PendingWebhook {
    payload: WebhookPayload,
    attempts: u32,
    /// Earliest instant at which this item may be delivered (retry backoff).
    ready_at: Instant,
}

/// Maximum retry attempts for webhook delivery
const MAX_RETRIES: u32 = 3;
/// Retry backoff base delay
const RETRY_BACKOFF_BASE_MS: u64 = 1000;
/// Maximum queue size for pending webhooks
const MAX_QUEUE_SIZE: usize = 1000;
/// Default timeout for webhook requests
const DEFAULT_TIMEOUT_SECS: u64 = 10;

/// Webhook event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEventType {
    TaskStarted,
    TaskCompleted,
    TaskFailed,
    SnapshotCompleted,
    SnapshotFailed,
    RebuildCompleted,
    RebuildFailed,
    SloBreachSoft,
    SloBreachHard,
    ResourceExhausted,
    GovernorCooldown,
}

/// Webhook payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookPayload {
    /// Event type
    pub event: WebhookEventType,
    /// Unix timestamp in milliseconds
    pub timestamp_ms: u64,
    /// Task type (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_type: Option<String>,
    /// Task ID (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Execution ID (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    /// Duration in milliseconds (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Error message (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Additional details
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    /// Shard ID (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<String>,
    /// Node ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
}

impl WebhookPayload {
    /// Create a new payload with required fields
    pub fn new(event: WebhookEventType) -> Self {
        Self {
            event,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            task_type: None,
            task_id: None,
            execution_id: None,
            duration_ms: None,
            error: None,
            details: None,
            shard_id: None,
            node_id: None,
        }
    }

    /// Set task info
    pub fn with_task(mut self, task_type: &str, task_id: &str) -> Self {
        self.task_type = Some(task_type.to_string());
        self.task_id = Some(task_id.to_string());
        self
    }

    /// Set execution ID
    pub fn with_execution(mut self, execution_id: &str) -> Self {
        self.execution_id = Some(execution_id.to_string());
        self
    }

    /// Set duration
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration_ms = Some(duration.as_millis() as u64);
        self
    }

    /// Set error message
    pub fn with_error(mut self, error: &str) -> Self {
        self.error = Some(error.to_string());
        self
    }

    /// Set additional details
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    /// Set shard ID
    pub fn with_shard(mut self, shard_id: &str) -> Self {
        self.shard_id = Some(shard_id.to_string());
        self
    }

    /// Set node ID
    pub fn with_node(mut self, node_id: &str) -> Self {
        self.node_id = Some(node_id.to_string());
        self
    }
}

/// Webhook configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Webhook URL
    pub url: String,
    /// Events to send
    pub events: Vec<WebhookEventType>,
    /// Optional HMAC secret for signing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    /// Whether webhooks are enabled
    pub enabled: bool,
    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Custom headers
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}

fn default_timeout() -> u64 {
    DEFAULT_TIMEOUT_SECS
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            events: vec![
                WebhookEventType::TaskFailed,
                WebhookEventType::SnapshotFailed,
                WebhookEventType::RebuildFailed,
                WebhookEventType::SloBreachHard,
            ],
            secret: None,
            enabled: false,
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            headers: std::collections::HashMap::new(),
        }
    }
}

/// Delivery status for a webhook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryStatus {
    /// Payload that was sent
    pub payload: WebhookPayload,
    /// Whether delivery was successful
    pub success: bool,
    /// HTTP status code (if received)
    pub status_code: Option<u16>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Number of attempts made
    pub attempts: u32,
    /// Timestamp of last attempt
    pub last_attempt_ms: u64,
}

/// Webhook delivery stats
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WebhookStats {
    /// Total webhooks sent
    pub total_sent: u64,
    /// Total successful deliveries
    pub successful: u64,
    /// Total failed deliveries
    pub failed: u64,
    /// Total retries
    pub retries: u64,
    /// Last successful delivery timestamp
    pub last_success_ms: Option<u64>,
    /// Last failed delivery timestamp
    pub last_failure_ms: Option<u64>,
}

/// Webhook sender for background task events
pub struct WebhookSender {
    /// Configuration
    config: Arc<RwLock<WebhookConfig>>,
    /// HTTP client (behind RwLock so timeout changes in update_config take effect)
    client: RwLock<reqwest::Client>,
    /// Pending webhooks queue
    pending: Arc<RwLock<VecDeque<PendingWebhook>>>,
    /// Recent delivery statuses (for debugging)
    recent_deliveries: Arc<RwLock<VecDeque<DeliveryStatus>>>,
    /// Stats
    stats: Arc<RwLock<WebhookStats>>,
    /// Node ID for this instance
    node_id: Option<String>,
}

impl WebhookSender {
    /// Create a new webhook sender
    pub fn new(config: WebhookConfig, node_id: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .unwrap_or_default();

        Self {
            config: Arc::new(RwLock::new(config)),
            client: RwLock::new(client),
            pending: Arc::new(RwLock::new(VecDeque::new())),
            recent_deliveries: Arc::new(RwLock::new(VecDeque::new())),
            stats: Arc::new(RwLock::new(WebhookStats::default())),
            node_id,
        }
    }

    /// Update configuration
    pub fn update_config(&self, config: WebhookConfig) {
        // FIX BUG: rebuild the HTTP client so timeout changes take effect
        let new_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .unwrap_or_default();
        *self.client.write() = new_client;
        *self.config.write() = config;
    }

    /// Get current configuration
    pub fn get_config(&self) -> WebhookConfig {
        self.config.read().clone()
    }

    /// Get delivery statistics
    pub fn get_stats(&self) -> WebhookStats {
        self.stats.read().clone()
    }

    /// Get recent delivery statuses
    pub fn get_recent_deliveries(&self, limit: usize) -> Vec<DeliveryStatus> {
        let deliveries = self.recent_deliveries.read();
        deliveries.iter().rev().take(limit).cloned().collect()
    }

    /// Send a webhook payload
    pub async fn send(&self, mut payload: WebhookPayload) {
        // Add node ID if configured
        if payload.node_id.is_none() {
            payload.node_id = self.node_id.clone();
        }

        let config = self.config.read().clone();

        // Check if enabled and event type is subscribed
        if !config.enabled {
            debug!("Webhooks disabled, skipping delivery");
            return;
        }

        if !config.events.contains(&payload.event) {
            debug!(event = ?payload.event, "Event type not subscribed, skipping");
            return;
        }

        // Queue for delivery
        {
            let mut pending = self.pending.write();
            if pending.len() >= MAX_QUEUE_SIZE {
                warn!("Webhook queue full, dropping oldest event");
                pending.pop_front();
            }
            pending.push_back(PendingWebhook {
                payload: payload.clone(),
                attempts: 0,
                ready_at: Instant::now(),
            });
        }

        // Attempt immediate delivery
        self.process_pending().await;
    }

    /// Process pending webhooks that are ready for delivery.
    ///
    /// Retry backoff is stored as `ready_at` on the queue item so this loop
    /// never sleeps while holding up other ready events.
    pub async fn process_pending(&self) {
        let mut deferred: VecDeque<PendingWebhook> = VecDeque::new();
        let now = Instant::now();

        loop {
            // Get next ready item from queue; defer not-yet-ready retries.
            let item = {
                let mut pending = self.pending.write();
                loop {
                    match pending.pop_front() {
                        None => break None,
                        Some(item) if item.ready_at > now => {
                            deferred.push_back(item);
                        }
                        Some(item) => break Some(item),
                    }
                }
            };

            let Some(PendingWebhook {
                payload,
                attempts,
                ..
            }) = item
            else {
                break;
            };

            let config = self.config.read().clone();
            let result = self.deliver(&config, &payload).await;

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            let status = DeliveryStatus {
                payload: payload.clone(),
                success: result.is_ok(),
                status_code: result.as_ref().ok().copied(),
                error: result.as_ref().err().map(|e| e.to_string()),
                attempts: attempts + 1,
                last_attempt_ms: now_ms,
            };

            // Update stats
            {
                let mut stats = self.stats.write();
                stats.total_sent += 1;
                if status.success {
                    stats.successful += 1;
                    stats.last_success_ms = Some(now_ms);
                } else {
                    stats.failed += 1;
                    stats.last_failure_ms = Some(now_ms);
                }
            }

            // Store recent delivery
            {
                let mut deliveries = self.recent_deliveries.write();
                if deliveries.len() >= 100 {
                    deliveries.pop_front();
                }
                deliveries.push_back(status.clone());
            }

            // Handle retry if failed: schedule for later without blocking the drain.
            if !status.success && attempts < MAX_RETRIES {
                {
                    let mut stats = self.stats.write();
                    stats.retries += 1;
                }

                let backoff_ms = RETRY_BACKOFF_BASE_MS * 2u64.pow(attempts);
                deferred.push_back(PendingWebhook {
                    payload,
                    attempts: attempts + 1,
                    ready_at: Instant::now() + Duration::from_millis(backoff_ms),
                });
            }
        }

        if !deferred.is_empty() {
            let mut pending = self.pending.write();
            for item in deferred {
                pending.push_back(item);
            }
        }
    }

    /// Deliver a single webhook
    async fn deliver(
        &self,
        config: &WebhookConfig,
        payload: &WebhookPayload,
    ) -> Result<u16, String> {
        // Serialize once so the HMAC covers the exact bytes we put on the wire.
        // Using `.json(payload)` would re-serialize and can diverge in key order.
        let body = serde_json::to_vec(payload).map_err(|e| e.to_string())?;

        let mut request = self
            .client
            .read()
            .post(&config.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.clone());

        // Add custom headers
        for (key, value) in &config.headers {
            request = request.header(key.as_str(), value.as_str());
        }

        // Add HMAC signature if secret is configured
        if let Some(ref secret) = config.secret {
            let signature = compute_hmac_signature(secret, &body);
            request = request.header("X-Webhook-Signature", signature);
        }

        let response = request.send().await.map_err(|e| e.to_string())?;

        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            info!(url = %config.url, status, event = ?payload.event, "Webhook delivered successfully");
            Ok(status)
        } else {
            let error = format!("HTTP {}", status);
            error!(url = %config.url, status, event = ?payload.event, "Webhook delivery failed");
            Err(error)
        }
    }

    // ============================================
    // Convenience methods for common events
    // ============================================

    /// Send task started event
    pub async fn task_started(&self, task_type: &str, task_id: &str, execution_id: &str) {
        let payload = WebhookPayload::new(WebhookEventType::TaskStarted)
            .with_task(task_type, task_id)
            .with_execution(execution_id);
        self.send(payload).await;
    }

    /// Send task completed event
    pub async fn task_completed(
        &self,
        task_type: &str,
        task_id: &str,
        execution_id: &str,
        duration: Duration,
    ) {
        let payload = WebhookPayload::new(WebhookEventType::TaskCompleted)
            .with_task(task_type, task_id)
            .with_execution(execution_id)
            .with_duration(duration);
        self.send(payload).await;
    }

    /// Send task failed event
    pub async fn task_failed(
        &self,
        task_type: &str,
        task_id: &str,
        execution_id: &str,
        error: &str,
        duration: Duration,
    ) {
        let payload = WebhookPayload::new(WebhookEventType::TaskFailed)
            .with_task(task_type, task_id)
            .with_execution(execution_id)
            .with_error(error)
            .with_duration(duration);
        self.send(payload).await;
    }

    /// Send snapshot completed event
    pub async fn snapshot_completed(&self, snapshot_id: &str, duration: Duration, size_bytes: u64) {
        let payload = WebhookPayload::new(WebhookEventType::SnapshotCompleted)
            .with_task("snapshot", snapshot_id)
            .with_duration(duration)
            .with_details(serde_json::json!({
                "size_bytes": size_bytes
            }));
        self.send(payload).await;
    }

    /// Send snapshot failed event
    pub async fn snapshot_failed(&self, snapshot_id: &str, error: &str, duration: Duration) {
        let payload = WebhookPayload::new(WebhookEventType::SnapshotFailed)
            .with_task("snapshot", snapshot_id)
            .with_error(error)
            .with_duration(duration);
        self.send(payload).await;
    }

    /// Send rebuild completed event
    pub async fn rebuild_completed(
        &self,
        rebuild_id: &str,
        duration: Duration,
        vectors_processed: u64,
    ) {
        let payload = WebhookPayload::new(WebhookEventType::RebuildCompleted)
            .with_task("rebuild", rebuild_id)
            .with_duration(duration)
            .with_details(serde_json::json!({
                "vectors_processed": vectors_processed
            }));
        self.send(payload).await;
    }

    /// Send rebuild failed event
    pub async fn rebuild_failed(&self, rebuild_id: &str, error: &str, duration: Duration) {
        let payload = WebhookPayload::new(WebhookEventType::RebuildFailed)
            .with_task("rebuild", rebuild_id)
            .with_error(error)
            .with_duration(duration);
        self.send(payload).await;
    }

    /// Send SLO breach event
    pub async fn slo_breach(&self, breach_type: &str, latency_ms: u64, threshold_ms: u64) {
        let event = if breach_type == "hard" {
            WebhookEventType::SloBreachHard
        } else {
            WebhookEventType::SloBreachSoft
        };

        let payload = WebhookPayload::new(event).with_details(serde_json::json!({
            "breach_type": breach_type,
            "latency_ms": latency_ms,
            "threshold_ms": threshold_ms
        }));
        self.send(payload).await;
    }

    /// Send resource exhausted event
    pub async fn resource_exhausted(&self, reason: &str, current_value: f64, limit: f64) {
        let payload = WebhookPayload::new(WebhookEventType::ResourceExhausted).with_details(
            serde_json::json!({
                "reason": reason,
                "current_value": current_value,
                "limit": limit
            }),
        );
        self.send(payload).await;
    }

    /// Send governor cooldown event
    pub async fn governor_cooldown(&self, p95_latency_ms: u64, cooldown_ms: u64) {
        let payload = WebhookPayload::new(WebhookEventType::GovernorCooldown).with_details(
            serde_json::json!({
                "p95_latency_ms": p95_latency_ms,
                "cooldown_ms": cooldown_ms
            }),
        );
        self.send(payload).await;
    }
}

/// Compute HMAC-SHA256 signature for webhook payload body bytes
fn compute_hmac_signature(secret: &str, body: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(body);
    let result = mac.finalize();

    format!("sha256={}", hex::encode(result.into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_webhook_payload_builder() {
        let payload = WebhookPayload::new(WebhookEventType::TaskFailed)
            .with_task("snapshot", "daily-backup")
            .with_execution("exec-123")
            .with_error("Connection timeout")
            .with_duration(Duration::from_secs(120));

        assert_eq!(payload.event, WebhookEventType::TaskFailed);
        assert_eq!(payload.task_type, Some("snapshot".to_string()));
        assert_eq!(payload.task_id, Some("daily-backup".to_string()));
        assert_eq!(payload.execution_id, Some("exec-123".to_string()));
        assert_eq!(payload.error, Some("Connection timeout".to_string()));
        assert_eq!(payload.duration_ms, Some(120000));
    }

    #[test]
    fn test_webhook_config_default() {
        let config = WebhookConfig::default();
        assert!(!config.enabled);
        assert!(config.url.is_empty());
        assert!(config.events.contains(&WebhookEventType::TaskFailed));
        assert!(config.events.contains(&WebhookEventType::SloBreachHard));
    }

    #[test]
    fn test_webhook_sender_disabled() {
        let config = WebhookConfig {
            url: "https://example.com/webhook".to_string(),
            enabled: false,
            ..Default::default()
        };
        let sender = WebhookSender::new(config, None);

        // Should not panic when disabled
        let stats = sender.get_stats();
        assert_eq!(stats.total_sent, 0);
    }

    #[test]
    fn test_hmac_signature() {
        let signature = compute_hmac_signature("secret", b"test body");
        assert!(signature.starts_with("sha256="));
        assert_eq!(signature.len(), 7 + 64); // "sha256=" + 64 hex chars
    }

    #[test]
    fn hmac_signature_matches_serialized_body_bytes() {
        let payload = WebhookPayload::new(WebhookEventType::TaskCompleted)
            .with_task("rebuild", "r1")
            .with_execution("exec-1");
        let body = serde_json::to_vec(&payload).unwrap();
        let signature = compute_hmac_signature("webhook-secret", &body);

        // Recompute over the same bytes that would be sent on the wire.
        let again = compute_hmac_signature("webhook-secret", &body);
        assert_eq!(signature, again);

        // A re-serialized copy with the same logical content must still match
        // when we use the canonical body we actually transmit.
        let body2 = serde_json::to_vec(&payload).unwrap();
        assert_eq!(body, body2);
        assert_eq!(
            compute_hmac_signature("webhook-secret", &body2),
            signature
        );
    }

    #[test]
    fn test_payload_serialization() {
        let payload = WebhookPayload::new(WebhookEventType::SnapshotCompleted)
            .with_task("snapshot", "snap-123")
            .with_duration(Duration::from_secs(60));

        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"event\":\"snapshot_completed\""));
        assert!(json.contains("\"task_type\":\"snapshot\""));
        assert!(json.contains("\"duration_ms\":60000"));
    }

    #[test]
    fn process_pending_defers_not_ready_retries_without_blocking_queue() {
        let sender = WebhookSender::new(WebhookConfig::default(), None);
        let now = Instant::now();

        {
            let mut pending = sender.pending.write();
            pending.push_back(PendingWebhook {
                payload: WebhookPayload::new(WebhookEventType::TaskFailed).with_task("t", "late"),
                attempts: 1,
                ready_at: now + Duration::from_secs(60),
            });
            pending.push_back(PendingWebhook {
                payload: WebhookPayload::new(WebhookEventType::TaskCompleted)
                    .with_task("t", "ready"),
                attempts: 0,
                ready_at: now,
            });
        }

        // Drain with webhooks disabled: ready items are attempted (stats),
        // not-ready items remain queued without a sleep in the drain path.
        // Disabled config still short-circuits in send(), but process_pending
        // always tries deliver; use enabled=false and empty URL to fail fast.
        {
            let mut config = sender.get_config();
            config.enabled = true;
            config.url = "http://127.0.0.1:1/webhook-unreachable".to_string();
            config.events = vec![
                WebhookEventType::TaskCompleted,
                WebhookEventType::TaskFailed,
            ];
            sender.update_config(config);
        }

        let start = Instant::now();
        // block_on process_pending — use a tiny runtime for the unit test.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(sender.process_pending());
        let elapsed = start.elapsed();

        // Must not have slept the 60s deferred backoff.
        assert!(
            elapsed < Duration::from_secs(2),
            "drain loop slept on deferred retry: {elapsed:?}"
        );

        let pending = sender.pending.read();
        // The not-ready item remains; the ready item is either delivered or
        // requeued with a future ready_at after failure.
        assert!(
            pending.iter().any(|p| p.payload.task_id.as_deref() == Some("late")),
            "deferred not-ready item must stay queued"
        );
        assert!(
            pending
                .iter()
                .any(|p| p.payload.task_id.as_deref() == Some("ready") && p.attempts >= 1),
            "failed ready item should be requeued with attempts incremented"
        );
        let ready_item = pending
            .iter()
            .find(|p| p.payload.task_id.as_deref() == Some("ready"))
            .unwrap();
        assert!(
            ready_item.ready_at > Instant::now() - Duration::from_millis(100),
            "retry must schedule future ready_at rather than inline sleep"
        );
    }

    #[test]
    fn test_recent_deliveries_returns_newest_first() {
        let sender = WebhookSender::new(WebhookConfig::default(), None);

        {
            let mut deliveries = sender.recent_deliveries.write();
            for task_id in ["oldest", "middle", "newest"] {
                deliveries.push_back(DeliveryStatus {
                    payload: WebhookPayload::new(WebhookEventType::TaskCompleted)
                        .with_task("task", task_id),
                    success: true,
                    status_code: Some(200),
                    error: None,
                    attempts: 1,
                    last_attempt_ms: 1,
                });
            }
        }

        let task_ids: Vec<String> = sender
            .get_recent_deliveries(2)
            .into_iter()
            .map(|delivery| delivery.payload.task_id.unwrap())
            .collect();

        assert_eq!(task_ids, vec!["newest", "middle"]);
    }
}
