//! Embedding service abstraction with caching and fallback
//!
//! This module provides:
//! - Trait-based embedding service abstraction
//! - LRU caching layer for embedding reuse
//! - Fallback chain (ax-engine → Mock)
//! - Batch processing for efficiency

pub mod ax_engine;

use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
// FIX BUG-H032: Removed std::hash imports - now using deterministic FNV-1a hash
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

/// Embedding service error
#[derive(Debug, thiserror::Error)]
pub enum EmbeddingError {
    #[error("Model not loaded: {0}")]
    ModelNotLoaded(String),
    #[error("Dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
    #[error("Batch too large: {size} > {max}")]
    BatchTooLarge { size: usize, max: usize },
    #[error("Timeout after {0:?}")]
    Timeout(Duration),
    #[error("Backend error: {0}")]
    BackendError(String),
    #[error("All backends failed")]
    AllBackendsFailed,
}

pub type EmbeddingResult<T> = Result<T, EmbeddingError>;

/// Embedding service trait
pub trait EmbeddingService: Send + Sync {
    /// Get embedding dimensions
    fn dimensions(&self) -> usize;

    /// Generate embedding for a single text
    fn embed(&self, text: &str) -> EmbeddingResult<Vec<f32>>;

    /// Generate embeddings for multiple texts (batch)
    fn embed_batch(&self, texts: &[&str]) -> EmbeddingResult<Vec<Vec<f32>>>;

    /// Check if the service is ready
    fn is_ready(&self) -> bool;

    /// Get service name
    fn name(&self) -> &str;

    /// Get maximum batch size
    fn max_batch_size(&self) -> usize;
}

/// Embedding service configuration
#[derive(Debug, Clone)]
pub struct EmbeddingConfig {
    /// Model dimensions
    pub dimensions: usize,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Timeout for embedding operations
    pub timeout: Duration,
    /// Cache size (number of entries)
    pub cache_size: usize,
    /// Cache TTL
    pub cache_ttl: Duration,
    /// Enable caching
    pub cache_enabled: bool,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            dimensions: 768,
            max_batch_size: 32,
            timeout: Duration::from_secs(10),
            cache_size: 10_000,
            cache_ttl: Duration::from_secs(3600), // 1 hour
            cache_enabled: true,
        }
    }
}

/// Cache entry for embeddings
/// FIX BUG-004: Store original text to detect hash collisions
#[derive(Clone)]
struct CacheEntry {
    text: String,
    embedding: Vec<f32>,
    created_at: Instant,
}

/// FIX BUG-H032: Use deterministic FNV-1a hash for text (for cache key)
///
/// DefaultHasher uses SipHash which is randomized per-process for DoS protection.
/// This causes cache misses after restart since the same text hashes differently.
/// FNV-1a is fast, deterministic, and suitable for hash table keys.
fn hash_text(text: &str) -> u64 {
    // FNV-1a 64-bit hash
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in text.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Atomically increment a u64 counter without wrapping at u64::MAX.
///
/// A plain `fetch_add(1)` silently wraps from `u64::MAX` to 0, which corrupts
/// monitoring metrics (hit/miss rates). This helper saturates at `u64::MAX`.
fn saturating_increment_u64(counter: &AtomicU64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(1);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn hit_rate(hits: u64, misses: u64) -> f64 {
    if hits == 0 && misses == 0 {
        return 0.0;
    }

    hits as f64 / (hits as f64 + misses as f64)
}

/// LRU-based embedding cache
/// FIX BUG-002: Use consistent lock ordering (entries first, then access_order)
/// FIX BUG-008: Use VecDeque for O(1) pop_front instead of Vec::remove(0)
pub struct EmbeddingCache {
    entries: RwLock<HashMap<u64, CacheEntry>>,
    access_order: RwLock<VecDeque<u64>>,
    max_size: usize,
    ttl: Duration,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl EmbeddingCache {
    /// Create a new embedding cache
    pub fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(HashMap::with_capacity(max_size)),
            access_order: RwLock::new(VecDeque::with_capacity(max_size)),
            max_size,
            ttl,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Get cached embedding
    /// FIX BUG-002: Release entries lock before updating access_order to avoid contention
    /// FIX BUG-004: Verify text matches to detect hash collisions
    pub fn get(&self, text: &str) -> Option<Vec<f32>> {
        let key = hash_text(text);

        // First, try to get the entry (short read lock)
        let result = {
            let entries = self.entries.read();
            if let Some(entry) = entries.get(&key) {
                // FIX BUG-004: Verify text matches (hash collision detection)
                if entry.text != text {
                    // Hash collision - treat as miss
                    None
                } else if entry.created_at.elapsed() < self.ttl {
                    Some(entry.embedding.clone())
                } else {
                    None // Expired
                }
            } else {
                None
            }
        };
        // entries lock is released here

        if let Some(embedding) = result {
            saturating_increment_u64(&self.hits);
            // Update access order (separate lock acquisition)
            // FIX BUG-039: Only perform O(n) retain if key isn't already at back
            // FIX BUG-069: Verify entry still exists before updating access_order
            // This prevents adding stale keys if another thread evicted the entry
            {
                let entries = self.entries.read();
                let mut order = self.access_order.write();
                // Only update if entry still exists (wasn't evicted by another thread)
                if entries.contains_key(&key) && order.back() != Some(&key) {
                    order.retain(|&k| k != key);
                    order.push_back(key);
                }
            }
            return Some(embedding);
        }

        saturating_increment_u64(&self.misses);
        None
    }

    /// Put embedding in cache
    pub fn put(&self, text: &str, embedding: Vec<f32>) {
        let key = hash_text(text);

        let mut entries = self.entries.write();
        let mut order = self.access_order.write();

        // FIX BUG-009: Evict expired entries first, then by LRU
        // First pass: remove expired entries
        // FIX BUG-039: Use HashSet for O(1) contains check instead of O(m) Vec lookup
        let expired_keys: HashSet<u64> = entries
            .iter()
            .filter(|(_, entry)| entry.created_at.elapsed() >= self.ttl)
            .map(|(&k, _)| k)
            .collect();
        for k in &expired_keys {
            entries.remove(k);
        }
        order.retain(|k| !expired_keys.contains(k));

        // FIX BUG-008: Use VecDeque::pop_front() for O(1) eviction
        while entries.len() >= self.max_size && !order.is_empty() {
            if let Some(oldest) = order.pop_front() {
                entries.remove(&oldest);
            }
        }

        // FIX BUG-H047: Remove key from access_order before re-adding to prevent duplicates
        // When updating an existing entry, the old key position would remain in access_order,
        // causing memory growth and incorrect LRU eviction behavior.
        if entries.contains_key(&key) {
            order.retain(|&k| k != key);
        }

        // FIX BUG-004: Store text for collision detection
        entries.insert(
            key,
            CacheEntry {
                text: text.to_string(),
                embedding,
                created_at: Instant::now(),
            },
        );
        order.push_back(key);
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);

        CacheStats {
            hits,
            misses,
            hit_rate: hit_rate(hits, misses),
            size: self.entries.read().len(),
            max_size: self.max_size,
        }
    }

    /// Clear the cache
    pub fn clear(&self) {
        let mut entries = self.entries.write();
        let mut order = self.access_order.write();
        entries.clear();
        order.clear();
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
    pub size: usize,
    pub max_size: usize,
}

/// Mock embedding service for testing
pub struct MockEmbeddingService {
    config: EmbeddingConfig,
    ready: bool,
}

impl MockEmbeddingService {
    /// Create a new mock embedding service
    pub fn new(config: EmbeddingConfig) -> Self {
        Self {
            config,
            ready: true,
        }
    }

    /// Generate deterministic embedding from text
    fn generate_embedding(&self, text: &str) -> Vec<f32> {
        let hash = hash_text(text);
        let mut embedding = Vec::with_capacity(self.config.dimensions);

        // Generate deterministic pseudo-random values
        let mut state = hash;
        for _ in 0..self.config.dimensions {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let value = ((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0;
            embedding.push(value);
        }

        // Normalize
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut embedding {
                *x /= norm;
            }
        }

        embedding
    }
}

impl EmbeddingService for MockEmbeddingService {
    fn dimensions(&self) -> usize {
        self.config.dimensions
    }

    fn embed(&self, text: &str) -> EmbeddingResult<Vec<f32>> {
        if !self.ready {
            return Err(EmbeddingError::ModelNotLoaded("Mock".to_string()));
        }
        Ok(self.generate_embedding(text))
    }

    fn embed_batch(&self, texts: &[&str]) -> EmbeddingResult<Vec<Vec<f32>>> {
        if texts.len() > self.config.max_batch_size {
            return Err(EmbeddingError::BatchTooLarge {
                size: texts.len(),
                max: self.config.max_batch_size,
            });
        }
        texts.iter().map(|t| self.embed(t)).collect()
    }

    fn is_ready(&self) -> bool {
        self.ready
    }

    fn name(&self) -> &str {
        "mock"
    }

    fn max_batch_size(&self) -> usize {
        self.config.max_batch_size
    }
}

/// Cached embedding service wrapper
pub struct CachedEmbeddingService<S: EmbeddingService> {
    inner: S,
    cache: EmbeddingCache,
    enabled: bool,
}

impl<S: EmbeddingService> CachedEmbeddingService<S> {
    /// Create a new cached embedding service
    pub fn new(inner: S, cache_size: usize, cache_ttl: Duration) -> Self {
        Self {
            inner,
            cache: EmbeddingCache::new(cache_size, cache_ttl),
            enabled: true,
        }
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        self.cache.stats()
    }

    /// Enable/disable caching
    pub fn set_cache_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Clear the cache
    pub fn clear_cache(&self) {
        self.cache.clear();
    }
}

impl<S: EmbeddingService> EmbeddingService for CachedEmbeddingService<S> {
    fn dimensions(&self) -> usize {
        self.inner.dimensions()
    }

    fn embed(&self, text: &str) -> EmbeddingResult<Vec<f32>> {
        if self.enabled {
            if let Some(cached) = self.cache.get(text) {
                return Ok(cached);
            }
        }

        let embedding = self.inner.embed(text)?;

        if self.enabled {
            self.cache.put(text, embedding.clone());
        }

        Ok(embedding)
    }

    fn embed_batch(&self, texts: &[&str]) -> EmbeddingResult<Vec<Vec<f32>>> {
        if !self.enabled {
            return self.inner.embed_batch(texts);
        }

        let mut results = vec![None; texts.len()];
        let mut uncached_indices = Vec::new();
        let mut uncached_texts = Vec::new();

        // Check cache for each text
        for (i, text) in texts.iter().enumerate() {
            if let Some(cached) = self.cache.get(text) {
                results[i] = Some(cached);
            } else {
                uncached_indices.push(i);
                uncached_texts.push(*text);
            }
        }

        // Batch generate uncached embeddings
        if !uncached_texts.is_empty() {
            let new_embeddings = self.inner.embed_batch(&uncached_texts)?;
            if new_embeddings.len() != uncached_texts.len() {
                return Err(EmbeddingError::BackendError(format!(
                    "{} returned {} embeddings for {} uncached texts",
                    self.inner.name(),
                    new_embeddings.len(),
                    uncached_texts.len()
                )));
            }

            for (idx, embedding) in uncached_indices.into_iter().zip(new_embeddings) {
                self.cache.put(texts[idx], embedding.clone());
                results[idx] = Some(embedding);
            }
        }

        results
            .into_iter()
            .enumerate()
            .map(|(idx, embedding)| {
                embedding.ok_or_else(|| {
                    EmbeddingError::BackendError(format!(
                        "{} did not produce embedding for batch item {}",
                        self.inner.name(),
                        idx
                    ))
                })
            })
            .collect()
    }

    fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn max_batch_size(&self) -> usize {
        self.inner.max_batch_size()
    }
}

/// Embedding service with fallback chain
pub struct FallbackEmbeddingService {
    backends: Vec<Arc<dyn EmbeddingService>>,
    dimensions: usize,
}

impl FallbackEmbeddingService {
    /// Create a new fallback embedding service
    pub fn new(backends: Vec<Arc<dyn EmbeddingService>>) -> EmbeddingResult<Self> {
        if backends.is_empty() {
            return Err(EmbeddingError::BackendError(
                "At least one backend required".to_string(),
            ));
        }

        let dimensions = backends[0].dimensions();

        // Validate all backends have same dimensions
        for backend in &backends {
            if backend.dimensions() != dimensions {
                return Err(EmbeddingError::DimensionMismatch {
                    expected: dimensions,
                    actual: backend.dimensions(),
                });
            }
        }

        Ok(Self {
            backends,
            dimensions,
        })
    }

    /// Get available backend names
    pub fn available_backends(&self) -> Vec<&str> {
        self.backends
            .iter()
            .filter(|b| b.is_ready())
            .map(|b| b.name())
            .collect()
    }
}

impl EmbeddingService for FallbackEmbeddingService {
    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn embed(&self, text: &str) -> EmbeddingResult<Vec<f32>> {
        for backend in &self.backends {
            if backend.is_ready() {
                match backend.embed(text) {
                    Ok(embedding) => return Ok(embedding),
                    Err(e) => {
                        warn!(
                            backend = backend.name(),
                            error = %e,
                            "Backend failed, trying next"
                        );
                    }
                }
            }
        }
        Err(EmbeddingError::AllBackendsFailed)
    }

    fn embed_batch(&self, texts: &[&str]) -> EmbeddingResult<Vec<Vec<f32>>> {
        for backend in &self.backends {
            if backend.is_ready() {
                match backend.embed_batch(texts) {
                    Ok(embeddings) => return Ok(embeddings),
                    Err(e) => {
                        warn!(
                            backend = backend.name(),
                            error = %e,
                            "Backend failed, trying next"
                        );
                    }
                }
            }
        }
        Err(EmbeddingError::AllBackendsFailed)
    }

    fn is_ready(&self) -> bool {
        self.backends.iter().any(|b| b.is_ready())
    }

    fn name(&self) -> &str {
        "fallback"
    }

    fn max_batch_size(&self) -> usize {
        // FIX BUG-H049: Don't return batch size 1 when no backends are healthy
        // Returning 1 causes 100x performance degradation (single-vector batches).
        // Instead, use the minimum of all backends' batch sizes (even unhealthy ones)
        // as a reasonable fallback, or a sensible default.
        let healthy_max = self
            .backends
            .iter()
            .filter(|b| b.is_ready())
            .map(|b| b.max_batch_size())
            .min();

        if let Some(max) = healthy_max {
            return max;
        }

        // No healthy backends - use the min batch size from ANY backend as fallback
        // This preserves the expected batch size for when backends recover
        let any_max = self.backends.iter().map(|b| b.max_batch_size()).min();

        if let Some(max) = any_max {
            warn!(
                batch_size = max,
                "No healthy embedding backends, using fallback batch size from unhealthy backend"
            );
            return max;
        }

        // No backends at all (shouldn't happen due to new() validation) - use reasonable default
        warn!("No embedding backends configured, using default batch size of 32");
        32
    }
}

/// Embedding service statistics
#[derive(Debug, Clone, Default)]
pub struct EmbeddingStats {
    pub total_requests: u64,
    pub total_texts: u64,
    pub total_latency_us: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub errors: u64,
}

impl EmbeddingStats {
    pub fn avg_latency_us(&self) -> f64 {
        if self.total_requests > 0 {
            self.total_latency_us as f64 / self.total_requests as f64
        } else {
            0.0
        }
    }

    pub fn cache_hit_rate(&self) -> f64 {
        hit_rate(self.cache_hits, self.cache_misses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_mock_service() -> MockEmbeddingService {
        MockEmbeddingService::new(EmbeddingConfig {
            dimensions: 128,
            max_batch_size: 10,
            ..Default::default()
        })
    }

    #[test]
    fn test_mock_embedding() {
        let service = create_mock_service();

        let embedding = service.embed("hello world").unwrap();
        assert_eq!(embedding.len(), 128);

        // Check normalization
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_mock_deterministic() {
        let service = create_mock_service();

        let e1 = service.embed("test text").unwrap();
        let e2 = service.embed("test text").unwrap();

        assert_eq!(e1, e2);
    }

    #[test]
    fn test_mock_batch() {
        let service = create_mock_service();

        let texts = vec!["hello", "world", "test"];
        let embeddings = service.embed_batch(&texts).unwrap();

        assert_eq!(embeddings.len(), 3);
        for e in &embeddings {
            assert_eq!(e.len(), 128);
        }
    }

    #[test]
    fn test_batch_too_large() {
        let service = create_mock_service();

        let texts: Vec<&str> = (0..20).map(|_| "text").collect();
        let result = service.embed_batch(&texts);

        assert!(matches!(result, Err(EmbeddingError::BatchTooLarge { .. })));
    }

    #[test]
    fn test_cache() {
        let cache = EmbeddingCache::new(10, Duration::from_secs(3600));

        // Miss
        assert!(cache.get("hello").is_none());

        // Put
        cache.put("hello", vec![1.0, 2.0, 3.0]);

        // Hit
        let cached = cache.get("hello").unwrap();
        assert_eq!(cached, vec![1.0, 2.0, 3.0]);

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cache_eviction() {
        let cache = EmbeddingCache::new(3, Duration::from_secs(3600));

        cache.put("a", vec![1.0]);
        cache.put("b", vec![2.0]);
        cache.put("c", vec![3.0]);
        cache.put("d", vec![4.0]); // Should evict "a"

        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_some());
        assert!(cache.get("c").is_some());
        assert!(cache.get("d").is_some());
    }

    #[test]
    fn test_cached_service() {
        let mock = MockEmbeddingService::new(EmbeddingConfig {
            dimensions: 64,
            ..Default::default()
        });
        let cached = CachedEmbeddingService::new(mock, 100, Duration::from_secs(3600));

        // First call - miss
        let e1 = cached.embed("hello").unwrap();

        // Second call - hit
        let e2 = cached.embed("hello").unwrap();

        assert_eq!(e1, e2);

        let stats = cached.cache_stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cached_batch() {
        let mock = MockEmbeddingService::new(EmbeddingConfig {
            dimensions: 64,
            max_batch_size: 10,
            ..Default::default()
        });
        let cached = CachedEmbeddingService::new(mock, 100, Duration::from_secs(3600));

        // Pre-cache one
        let _ = cached.embed("hello").unwrap();

        // Batch with one cached, two uncached
        let texts = vec!["hello", "world", "test"];
        let embeddings = cached.embed_batch(&texts).unwrap();

        assert_eq!(embeddings.len(), 3);

        // Verify "hello" was from cache
        let stats = cached.cache_stats();
        assert!(stats.hits >= 1);
    }

    struct ShortBatchEmbeddingService;

    impl EmbeddingService for ShortBatchEmbeddingService {
        fn dimensions(&self) -> usize {
            2
        }

        fn embed(&self, _text: &str) -> EmbeddingResult<Vec<f32>> {
            Ok(vec![1.0, 0.0])
        }

        fn embed_batch(&self, _texts: &[&str]) -> EmbeddingResult<Vec<Vec<f32>>> {
            Ok(vec![vec![1.0, 0.0]])
        }

        fn is_ready(&self) -> bool {
            true
        }

        fn name(&self) -> &str {
            "short-batch"
        }

        fn max_batch_size(&self) -> usize {
            16
        }
    }

    #[test]
    fn test_cached_batch_rejects_short_backend_response() {
        let cached =
            CachedEmbeddingService::new(ShortBatchEmbeddingService, 100, Duration::from_secs(3600));

        let result = cached.embed_batch(&["a", "b"]);

        assert!(
            matches!(result, Err(EmbeddingError::BackendError(message)) if message.contains("returned 1 embeddings for 2 uncached texts"))
        );
    }

    #[test]
    fn test_fallback_service() {
        let backend1 = Arc::new(MockEmbeddingService::new(EmbeddingConfig {
            dimensions: 128,
            ..Default::default()
        }));
        let backend2 = Arc::new(MockEmbeddingService::new(EmbeddingConfig {
            dimensions: 128,
            ..Default::default()
        }));

        let fallback = FallbackEmbeddingService::new(vec![backend1, backend2]).unwrap();

        assert!(fallback.is_ready());
        assert_eq!(fallback.available_backends().len(), 2);

        let embedding = fallback.embed("test").unwrap();
        assert_eq!(embedding.len(), 128);
    }

    #[test]
    fn test_fallback_dimension_mismatch() {
        let backend1 = Arc::new(MockEmbeddingService::new(EmbeddingConfig {
            dimensions: 128,
            ..Default::default()
        }));
        let backend2 = Arc::new(MockEmbeddingService::new(EmbeddingConfig {
            dimensions: 256,
            ..Default::default()
        }));

        let result = FallbackEmbeddingService::new(vec![backend1, backend2]);
        assert!(matches!(
            result,
            Err(EmbeddingError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn test_cache_hit_miss_counters_saturate_without_wrapping() {
        let cache = EmbeddingCache::new(10, Duration::from_secs(60));
        cache.hits.store(u64::MAX, Ordering::Relaxed);
        cache.misses.store(u64::MAX, Ordering::Relaxed);

        // A miss on an empty cache exercises the miss counter path.
        assert!(cache.get("absent").is_none());

        // A hit exercises the hit counter path.
        cache.put("present", vec![0.0]);
        assert!(cache.get("present").is_some());

        // Both counters must saturate at u64::MAX rather than wrap to 0,
        // which would otherwise corrupt the reported hit_rate metric.
        let stats = cache.stats();
        assert_eq!(stats.hits, u64::MAX);
        assert_eq!(stats.misses, u64::MAX);
        assert!((stats.hit_rate - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_embedding_stats_hit_rate_does_not_overflow() {
        let stats = EmbeddingStats {
            cache_hits: u64::MAX,
            cache_misses: u64::MAX,
            ..Default::default()
        };

        assert!((stats.cache_hit_rate() - 0.5).abs() < f64::EPSILON);
    }
}
