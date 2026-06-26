# AkiDB Thor Edition - Architecture Decision Records (ADR)
## Version 1.4 (Final)

**Version:** 1.4
**Date:** 2026-01-21
**Status:** Approved (Final for v1.5 Release)
**Changes from v1.3:** Added ADR-019 (NATS Cluster Sizing), ADR-020 (Resilience Patterns), comprehensive v1.5 checklist
**Review:** Multi-model synthesis (Claude, Gemini, Grok) - Final validation

---

## Change Log from v1.3

| Section | Change | Rationale |
|---------|--------|-----------|
| ADR-019 | NEW: NATS Cluster Sizing (3-node) | 4-node Raft anti-pattern identified |
| ADR-020 | NEW: Resilience Patterns | Circuit breaker, backpressure, memory coordination |
| ADR-018 | Updated format routing | Added XLSX to Rust (calamine) |
| Checklist | Comprehensive v1.5 checklist | Production readiness gate |

---

## Table of Contents

- [ADR-002: Vector Index Strategy](#adr-002) *(unchanged)*
- [ADR-009: Index Lifecycle](#adr-009) *(unchanged)*
- [ADR-015: ID Management Contract](#adr-015) *(unchanged)*
- [ADR-016: Consistency Guarantees](#adr-016) *(unchanged)*
- [ADR-017: Container Orchestration Strategy](#adr-017) *(unchanged)*
- [ADR-018: Hybrid Ingestion Pipeline (UPDATED)](#adr-018-hybrid-ingestion-pipeline-updated)
- [ADR-019: NATS Cluster Sizing (NEW)](#adr-019-nats-cluster-sizing)
- [ADR-020: Resilience Patterns (NEW)](#adr-020-resilience-patterns)

---

## ADR-018: Hybrid Ingestion Pipeline (UPDATED)

### Updates from v1.3

1. **Format Routing Updated:** XLSX moved to Rust (calamine crate)
2. **Semantic Chunking Added:** Sentence-boundary detection
3. **Dynamic Batching Added:** 16-64 range based on queue depth

### Updated Format Routing

| Format | Extension | Parser Location | Library | Change |
|--------|-----------|-----------------|---------|--------|
| JSON | .json | Rust | serde_json | - |
| CSV | .csv | Rust | csv crate | - |
| HTML | .html, .htm | Rust | scraper | - |
| XML | .xml | Rust | quick-xml | - |
| **XLSX** | **.xlsx, .xls** | **Rust** | **calamine** | **MOVED from Python** |
| PDF | .pdf | Python | pdfplumber | - |
| DOCX (simple) | .docx | Rust | docx-rs | NEW (no macros/embedded) |
| DOCX (complex) | .docx | Python | python-docx | - |
| ENL | .enl | Python | Custom | - |

**Net Result:** ~60-70% of documents now parsed in Rust (up from 40-60%)

### Semantic Chunking Strategy

```rust
// Sentence-boundary-aware chunking
pub struct SemanticChunker {
    target_tokens: usize,     // 512
    min_overlap_tokens: usize, // 20
    max_overlap_tokens: usize, // 50
}

impl SemanticChunker {
    pub fn chunk(&self, text: &str) -> Vec<Chunk> {
        // 1. Split into sentences (rust-punkt or unicode-segmentation)
        let sentences = self.split_sentences(text);

        // 2. Group sentences into chunks near target_tokens
        let mut chunks = Vec::new();
        let mut current_chunk = Vec::new();
        let mut current_tokens = 0;

        for sentence in sentences {
            let sentence_tokens = self.count_tokens(&sentence);

            if current_tokens + sentence_tokens > self.target_tokens
               && !current_chunk.is_empty() {
                // Finalize current chunk
                chunks.push(self.create_chunk(&current_chunk));

                // Start new chunk with overlap
                current_chunk = self.get_overlap_sentences(&current_chunk);
                current_tokens = self.count_tokens_vec(&current_chunk);
            }

            current_chunk.push(sentence);
            current_tokens += sentence_tokens;
        }

        // Handle final chunk
        if !current_chunk.is_empty() {
            chunks.push(self.create_chunk(&current_chunk));
        }

        chunks
    }
}
```

### Dynamic Embedding Batching

```rust
pub struct DynamicBatcher {
    min_batch: usize,  // 16
    max_batch: usize,  // 64
    queue_depth_low: usize,   // <100 messages
    queue_depth_high: usize,  // >1000 messages
}

impl DynamicBatcher {
    pub fn optimal_batch_size(&self, queue_depth: usize, gpu_util: f32) -> usize {
        let base = if queue_depth < self.queue_depth_low {
            self.min_batch  // Low load: smaller batches, lower latency
        } else if queue_depth > self.queue_depth_high {
            self.max_batch  // High load: maximize throughput
        } else {
            // Linear interpolation
            self.min_batch + (queue_depth - self.queue_depth_low)
                * (self.max_batch - self.min_batch)
                / (self.queue_depth_high - self.queue_depth_low)
        };

        // Reduce if GPU memory pressure
        if gpu_util > 0.8 {
            (base as f32 * 0.5) as usize
        } else {
            base
        }
    }
}
```

---

## ADR-019: NATS Cluster Sizing (NEW)

### Status
**Accepted**

### Context

The original architecture specified a 4-node NATS JetStream cluster for the 4-node Thor deployment. Multi-model review identified this as an anti-pattern.

### Problem

A 4-node Raft cluster:
- Requires quorum of 3 nodes (⌈4/2⌉ + 1 = 3)
- Can only tolerate 1 node failure
- Has the same fault tolerance as a 3-node cluster
- Adds operational overhead without benefit

```
4-node cluster: ████ (lose 1) = ███ (still works)
                ████ (lose 2) = ██  (no quorum, FAIL)

3-node cluster: ███  (lose 1) = ██  (still works)
                ███  (lose 2) = █   (no quorum, FAIL)

Same fault tolerance, less overhead!
```

### Decision

Deploy NATS JetStream as a **3-node cluster** on Thor nodes 1-3.

```
┌─────────────────────────────────────────────────────────────┐
│                    NATS TOPOLOGY (v1.5)                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Thor 1          Thor 2          Thor 3          Thor 4     │
│  ┌─────┐         ┌─────┐         ┌─────┐         ┌─────┐   │
│  │NATS │◄───────►│NATS │◄───────►│NATS │         │     │   │
│  │ R1  │         │ R2  │         │ R3  │         │     │   │
│  └─────┘         └─────┘         └─────┘         └─────┘   │
│     │               │               │               │       │
│  ┌─────┐         ┌─────┐         ┌─────┐         ┌─────┐   │
│  │Shard│         │Shard│         │Shard│         │Coord│   │
│  └─────┘         └─────┘         └─────┘         └─────┘   │
│                                                             │
│  Quorum: 2 of 3 (can lose 1 node)                          │
│  Leader: Auto-elected                                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Configuration

```hcl
# /etc/nats/nats.conf
server_name: nats-thor-${NODE_ID}

port: 4222
http_port: 8222

jetstream {
  store_dir: /var/lib/nats/jetstream
  max_memory_store: 256MB
  max_file_store: 10GB
}

cluster {
  name: akidb-nats
  listen: 0.0.0.0:6222

  routes: [
    nats-route://thor1:6222
    nats-route://thor2:6222
    nats-route://thor3:6222
  ]
}
```

### Stream Configuration

```yaml
# AKIDB_INGEST stream
name: AKIDB_INGEST
subjects:
  - "akidb.uploads.*"
retention: WorkQueue
max_age: 24h           # Bound storage
max_deliver: 3         # Prevent infinite retries
replicas: 3            # Full replication across 3 nodes
storage: file
```

### Consequences

**Positive:**
- Same fault tolerance (1 node) with less overhead
- Simpler operations (3 nodes to manage)
- Thor 4 (coordinator) has more resources for other services

**Negative:**
- Thor 4 has no local NATS (must connect to Thor 1-3)
- Slightly higher network latency for Thor 4

---

## ADR-020: Resilience Patterns (NEW)

### Status
**Accepted**

### Context

Multi-model review identified three critical resilience gaps:
1. No circuit breaker for Python parser service
2. No backpressure propagation from AkiDB to NATS
3. No memory coordination for unified memory contention

### Decision 1: Circuit Breaker for Python Parser

```rust
pub struct ParserCircuitBreaker {
    state: AtomicU8,  // 0=Closed, 1=Open, 2=HalfOpen
    failure_count: AtomicUsize,
    last_failure: AtomicU64,

    // Configuration
    failure_threshold: usize,    // 3 consecutive failures
    reset_timeout_ms: u64,       // 30 seconds
    half_open_max_calls: usize,  // 1 test call
}

impl ParserCircuitBreaker {
    pub async fn call<F, T>(&self, f: F) -> Result<T, CircuitBreakerError>
    where
        F: Future<Output = Result<T, ParseError>>,
    {
        match self.state.load(Ordering::SeqCst) {
            0 => { // Closed - normal operation
                match f.await {
                    Ok(result) => {
                        self.reset_failures();
                        Ok(result)
                    }
                    Err(e) => {
                        self.record_failure();
                        if self.failure_count.load(Ordering::SeqCst) >= self.failure_threshold {
                            self.open();
                        }
                        Err(CircuitBreakerError::ServiceError(e))
                    }
                }
            }
            1 => { // Open - fail fast
                if self.should_attempt_reset() {
                    self.half_open();
                    // Recursive call in half-open state
                    self.call(f).await
                } else {
                    Err(CircuitBreakerError::CircuitOpen)
                }
            }
            2 => { // Half-open - test one call
                match f.await {
                    Ok(result) => {
                        self.close();
                        Ok(result)
                    }
                    Err(e) => {
                        self.open();
                        Err(CircuitBreakerError::ServiceError(e))
                    }
                }
            }
            _ => unreachable!(),
        }
    }
}
```

**State Transitions:**
```
         ┌─────────────────────────────────────┐
         │                                     │
         ▼                                     │
┌─────────────────┐    3 failures    ┌─────────────────┐
│     CLOSED      │ ─────────────────►│      OPEN       │
│  (normal ops)   │                   │  (fail fast)    │
└─────────────────┘                   └────────┬────────┘
         ▲                                     │
         │                              30s timeout
         │                                     │
         │    success                          ▼
         │                            ┌─────────────────┐
         └────────────────────────────│    HALF-OPEN    │
                                      │  (test 1 call)  │
                                      └─────────────────┘
                                              │
                                        failure│
                                              │
                                              ▼
                                      ┌─────────────────┐
                                      │      OPEN       │
                                      └─────────────────┘
```

### Decision 2: Backpressure Propagation

```rust
pub struct BackpressureController {
    nats_subscription: Subscription,
    akidb_client: AkiDBClient,

    // Thresholds
    insert_latency_threshold_ms: u64,   // 500ms
    queue_depth_high_water: usize,      // 10000 messages
    pause_duration_ms: u64,             // 5000ms
}

impl BackpressureController {
    pub async fn process_with_backpressure(&self) {
        loop {
            // Check AkiDB health
            let insert_latency = self.akidb_client.last_insert_latency_ms();
            let queue_depth = self.nats_subscription.pending_count();

            if insert_latency > self.insert_latency_threshold_ms {
                tracing::warn!(
                    latency_ms = insert_latency,
                    "AkiDB insert latency high, applying backpressure"
                );
                // Pause NATS consumption
                tokio::time::sleep(Duration::from_millis(self.pause_duration_ms)).await;
                continue;
            }

            if queue_depth > self.queue_depth_high_water {
                tracing::warn!(
                    queue_depth = queue_depth,
                    "NATS queue depth high, throttling"
                );
                // Process but with delay
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            // Normal processing
            match self.nats_subscription.next().await {
                Some(msg) => self.process_message(msg).await,
                None => break,
            }
        }
    }
}
```

### Decision 3: Memory Coordination

```rust
pub struct MemoryCoordinator {
    unified_memory_limit_mb: usize,  // 64000 (64GB)
    ingestion_budget_pct: f32,       // 0.05 (5% = 3.2GB)
    pause_threshold_pct: f32,        // 0.70 (pause at 70%)
}

impl MemoryCoordinator {
    pub async fn check_memory_pressure(&self) -> MemoryState {
        // Read unified memory usage via tegrastats or /proc/meminfo
        let used_mb = self.read_unified_memory_usage().await;
        let used_pct = used_mb as f32 / self.unified_memory_limit_mb as f32;

        if used_pct > self.pause_threshold_pct {
            MemoryState::Critical  // Pause ingestion
        } else if used_pct > 0.5 {
            MemoryState::Warning   // Reduce batch size
        } else {
            MemoryState::Normal    // Full speed
        }
    }

    async fn read_unified_memory_usage(&self) -> usize {
        // On Jetson Thor, use tegrastats or /sys/kernel/debug/nvmap
        // Fallback to /proc/meminfo for CPU memory
        let output = Command::new("tegrastats")
            .arg("--interval")
            .arg("0")
            .output()
            .await?;

        // Parse: RAM 12345/64000MB (lfb 5x4MB)
        self.parse_tegrastats(&output.stdout)
    }
}
```

### Consequences

**Positive:**
- Pipeline continues operating when Python parser is slow/crashed
- AkiDB saturation doesn't cause unbounded queueing
- Memory pressure detected before OOM
- Clear observability into resilience states

**Negative:**
- Additional complexity in orchestrator
- Requires tuning thresholds for specific workloads
- tegrastats dependency for Jetson-specific memory monitoring

---

## v1.5 Production Readiness Checklist

### Critical (Must Pass)

| ID | Item | Owner | Validation |
|----|------|-------|------------|
| C-01 | NATS 3-node cluster deployed | Infra | `nats cluster list` shows 3 nodes |
| C-02 | Circuit breaker implemented | Dev | Unit tests for all state transitions |
| C-03 | Backpressure tested | QA | Load test: AkiDB saturated → queue bounded |
| C-04 | Memory coordinator active | Dev | tegrastats integration verified |
| C-05 | Core metrics exported | Ops | Prometheus scraping all targets |

### High Priority (Strongly Recommended)

| ID | Item | Owner | Validation |
|----|------|-------|------------|
| H-01 | Semantic chunking | Dev | A/B test vs fixed chunking on retrieval quality |
| H-02 | Dynamic batching | Dev | Queue depth → batch size correlation logged |
| H-03 | XLSX in Rust (calamine) | Dev | Parse 1000 XLSX files without errors |
| H-04 | Idempotency layer | Dev | Duplicate detection test suite |
| H-05 | Document state tracking | Dev | Query `GET /status/{id}` returns full history |
| H-06 | Pre-signed URL hardening | Security | Penetration test passed |
| H-07 | GPU metrics via DCGM | Ops | Grafana dashboard showing GPU utilization |

### Medium Priority (Recommended)

| ID | Item | Owner | Validation |
|----|------|-------|------------|
| M-01 | Simple DOCX in Rust | Dev | Route simple DOCX to docx-rs |
| M-02 | DLQ auto-recovery | Ops | Cron job retries DLQ messages |
| M-03 | Thermal throttling | Dev | Batch size reduces at high temps |
| M-04 | Cold start handling | Dev | 503 returned until FAISS loaded |

### Hardware Validation

| ID | Item | Validation |
|----|------|------------|
| HW-01 | Thor node specs | 64GB unified memory confirmed |
| HW-02 | GPU passthrough | nvidia-smi inside container |
| HW-03 | Network latency | <1ms inter-node latency |
| HW-04 | NVMe throughput | >500MB/s sequential read |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial ADRs |
| 1.1 | 2025-01-20 | AkiDB Team | cuVS gate, SLO boundaries |
| 1.2 | 2026-01-21 | AkiDB Team | Container orchestration (Podman + quadlets) |
| 1.3 | 2026-01-21 | AkiDB Team | Hybrid ingestion pipeline |
| 1.4 | 2026-01-21 | AkiDB Team | NATS 3-node, resilience patterns, v1.5 checklist (Final) |

---

*End of ADR v1.4 (Final)*
