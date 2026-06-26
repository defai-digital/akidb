# Bug Hunting Report - AkiDB Thor Edition

**Generated:** 2026-01-21
**Agent:** bug-hunter (automatosx)
**Status:** Bugs Fixed (Round 2 Complete)

---

## Executive Summary

| Severity | Found | Fixed |
|----------|-------|-------|
| **CRITICAL** | 3 | 3 |
| **HIGH** | 5 | 5 |
| **MEDIUM** | 7 | 1 |
| **LOW** | 4 | 0 |
| **SECURITY** | 1 | 1 |

---

## Fixed Bugs (Round 1)

### Bug #1: StateTracker SQLite Not Thread-Safe (CRITICAL) - FIXED
**Location:** `crates/ingestion-orchestrator/src/state.rs:77-79`
**Issue:** `StateTracker` held raw `rusqlite::Connection` without `Mutex`. Concurrent async tasks caused "database is locked" errors.
**Fix:** Wrapped `Connection` in `Mutex<Connection>` and added proper lock acquisition in all methods.

### Bug #2: tegrastats Hangs Forever (CRITICAL) - FIXED
**Location:** `crates/ingestion-orchestrator/src/memory.rs:128-145`
**Issue:** `Command::output()` on continuous `tegrastats --interval` process blocked indefinitely.
**Fix:** Changed to `spawn()` + read one line + `kill()` pattern. Falls back to `/proc/meminfo` on failure.

### Bug #3: Embedding Service Missing (CRITICAL) - FIXED
**Location:** `deploy/compose/docker-compose.yml:241-259`
**Issue:** Embedding service was commented out, but ingestion expected `http://embedding:8000`.
**Fix:** Uncommented embedding service, added to GPU profile (`--profile gpu`), added health check.

### Bug #8: Missing Docker Health Conditions (HIGH) - FIXED
**Location:** `deploy/compose/docker-compose.yml:233-239`
**Issue:** Ingestion service had no `condition: service_healthy` dependencies, causing race conditions.
**Fix:** Added proper health conditions for nats-1, doc-parser, and minio dependencies.

### Bug #11: GPU Threshold Wrong (MEDIUM) - FIXED
**Location:** `crates/ingestion-orchestrator/src/batcher/dynamic.rs:51`
**Issue:** Compared `gpu_util > 80.0` but value was 0.0-1.0 after division. GPU protection never triggered.
**Fix:** Changed threshold to `0.8` and updated interface documentation to clarify 0.0-1.0 range.

### Bug #13: NATS Subject Mismatch (HIGH) - FIXED
**Location:** `crates/ingestion-orchestrator/src/nats/consumer.rs:50`
**Issue:** MinIO published to `minio.uploads`, but stream subscribed only to `minio.uploads.>` (wildcard doesn't match exact).
**Fix:** Added both `minio.uploads` (exact) and `minio.uploads.>` (wildcard) to stream subjects.

---

## Fixed Bugs (Round 2)

### BUG-001: Idempotency State Lost on Restart (CRITICAL) - FIXED
**Location:** `crates/ingestion-orchestrator/src/idempotency.rs`
**Issue:** In-memory `IndexSet` loses all hashes on restart, causing duplicate document processing.
**Fix:** Added SQLite persistence with `new_persistent()` constructor. Hashes are loaded on startup and persisted on each insert. Falls back to in-memory if database creation fails.

### BUG-002: Unbounded Memory on Large Document Fetch (CRITICAL) - FIXED
**Location:** `crates/ingestion-orchestrator/src/storage.rs`
**Issue:** `fetch()` loaded entire document into memory without size check, risking OOM on large files.
**Fix:** Added `MAX_DOCUMENT_SIZE` constant (100MB) and HEAD request size check before fetching. Documents exceeding limit are rejected with descriptive error.

### BUG-003: DLQ Publisher Never Called (HIGH) - FIXED
**Location:** `crates/ingestion-orchestrator/src/pipeline.rs:136-141`
**Issue:** Failed documents were only `nack()`'d, never sent to Dead Letter Queue for investigation.
**Fix:** Wired up DLQ publisher in error handler. Failed documents now publish to DLQ with error details, then terminate (not redeliver).

### BUG-005: Python/Rust Response Model Mismatch (HIGH) - FIXED
**Location:** `crates/ingestion-orchestrator/src/python_client/http.rs`
**Issue:** Rust `ParseResponse` expected legacy fields (`title`, `author`, `success`) but Python returned `ParsedDocument` model (`format`, `page_count`, `metadata`, `tables`, `images`).
**Fix:** Updated Rust `ParseResponse` to match Python's `ParsedDocument` model. Added `TableData`, `ImageRef`, and `ParseErrorResponse` types. Updated parse logic to extract metadata fields.

### BUG-007: XXE Vulnerability in ENL Parser (SECURITY) - FIXED
**Location:** `services/doc-parser/parser/parsers/enl.py`
**Issue:** Used vulnerable `xml.etree.ElementTree` which allows XML External Entity attacks.
**Fix:** Replaced with `defusedxml.ElementTree` which blocks XXE attacks. Added `defusedxml>=0.7.1` to pyproject.toml dependencies.

### BUG-008: Embedding Client No Retry Logic (HIGH) - FIXED
**Location:** `crates/ingestion-orchestrator/src/embedding.rs`
**Issue:** Single HTTP request failure caused document processing to fail without retry.
**Fix:** Added exponential backoff retry logic with jitter. Retries on server errors (5xx) and rate limiting (429). Configurable max retries (default: 3).

---

## Remaining Bugs (Lower Priority)

### MEDIUM Priority

- **Bug #7:** Duplicate NATS connections (consumer.rs & publisher.rs create separate connections)
- **Bug #9:** PDF parser double BytesIO (2x memory)
- **Bug #10:** Chunker overlap offset issues with multi-byte UTF-8
- **Bug #14:** Health check uses curl (may not exist in slim images)
- **Bug #15:** Circuit breaker tracks "allowed" not "succeeded"

### LOW Priority

- **Bug #16:** 60s Python timeout blocks pipeline
- **Bug #17:** `estimate_tokens_fast` unused dead code
- **Bug #18:** `max_retries` config not implemented
- **Bug #19:** StateTracker silently falls back to in-memory

---

## Files Modified (Round 1)

| File | Changes |
|------|---------|
| `crates/ingestion-orchestrator/src/state.rs` | Added Mutex wrapper for thread-safety |
| `crates/ingestion-orchestrator/src/memory.rs` | Fixed tegrastats spawn/kill pattern |
| `crates/ingestion-orchestrator/src/batcher/dynamic.rs` | Fixed GPU threshold (0.8 vs 80.0) |
| `crates/ingestion-orchestrator/src/nats/consumer.rs` | Added both exact and wildcard NATS subjects |
| `deploy/compose/docker-compose.yml` | Enabled embedding service, added health conditions |

## Files Modified (Round 2)

| File | Changes |
|------|---------|
| `crates/ingestion-orchestrator/src/idempotency.rs` | Added SQLite persistence for hash storage |
| `crates/ingestion-orchestrator/src/storage.rs` | Added 100MB size limit with HEAD check |
| `crates/ingestion-orchestrator/src/pipeline.rs` | Wired up DLQ publisher, added chrono |
| `crates/ingestion-orchestrator/src/python_client/http.rs` | Fixed response model to match Python API |
| `crates/ingestion-orchestrator/src/embedding.rs` | Added retry logic with exponential backoff |
| `crates/ingestion-orchestrator/Cargo.toml` | Added chrono dependency |
| `services/doc-parser/parser/parsers/enl.py` | Replaced xml.etree with defusedxml |
| `services/doc-parser/pyproject.toml` | Added defusedxml dependency |

---

## Verification Commands

```bash
# Run Rust tests to verify fixes
cd crates/ingestion-orchestrator
cargo test

# Verify docker-compose syntax
docker compose -f deploy/compose/docker-compose.yml config

# Start with GPU profile (includes embedding service)
docker compose -f deploy/compose/docker-compose.yml --profile gpu up -d

# Verify Python dependencies
cd services/doc-parser
pip install -e .
python -c "import defusedxml; print('defusedxml OK')"
```

---

*Report generated by bug-hunter agent (automatosx)*
*Round 2 fixes completed by Claude Code*
