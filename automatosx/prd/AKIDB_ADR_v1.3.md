# AkiDB Thor Edition - Architecture Decision Records (ADR)
## Version 1.3

**Version:** 1.3
**Date:** 2026-01-21
**Status:** Approved
**Changes from v1.2:** Added ADR-018 Hybrid Ingestion Pipeline Architecture
**Review:** Multi-model synthesis (Claude, Gemini, Grok) addressing ingestion architecture

---

## Change Log from v1.2

| Section | Change | Rationale |
|---------|--------|-----------|
| ADR-018 | NEW: Hybrid Ingestion Pipeline Architecture | Document ingestion strategy decision |
| ADR-018 | Rust orchestration + Python parsing selected | Memory efficiency + library maturity |
| ADR-018 | Format-aware routing defined | Optimize simple formats in Rust |

---

## Table of Contents

- [ADR-002: Vector Index Strategy (FAISS GPU IVF-Flat)](#adr-002-vector-index-strategy-revised) *(unchanged)*
- [ADR-009: Index Lifecycle - Delete, Update, Rebuild](#adr-009-index-lifecycle-revised) *(unchanged)*
- [ADR-015: ID Management Contract](#adr-015-id-management-contract) *(unchanged)*
- [ADR-016: Consistency and Visibility Guarantees](#adr-016-consistency-guarantees) *(unchanged)*
- [ADR-017: Container Orchestration Strategy](#adr-017-container-orchestration-strategy) *(unchanged)*
- [ADR-018: Hybrid Ingestion Pipeline Architecture (NEW)](#adr-018-hybrid-ingestion-pipeline-architecture)

*Note: ADRs 002, 009, 015, 016, 017 remain unchanged from v1.2. Only new ADR-018 included below.*

---

## ADR-018: Hybrid Ingestion Pipeline Architecture (NEW)

### Status
**Accepted**

### Context

AkiDB Thor Edition requires a document ingestion pipeline to convert uploaded files (PDF, DOCX, XLSX, CSV, HTML, XML, JSON, ENL) into searchable vectors. The architecture must balance:

1. **Memory efficiency**: Unified memory (64GB) shared between CPU and GPU; ingestion competes with FAISS/TensorRT-LLM
2. **Library maturity**: Python has mature document parsing libraries; Rust alternatives are limited for complex formats
3. **Development velocity**: 30-minute batch SLO provides slack; operational simplicity preferred over raw performance
4. **Fault isolation**: Parsing failures should not crash the entire ingestion pipeline
5. **Migration path**: Ability to incrementally adopt Rust for hot paths as crates mature

### Options Considered

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| **A: Pure Python** | FastAPI + Python NATS consumer + all parsing in Python | Fastest dev, mature libs | Memory unpredictable, GIL limits concurrency |
| **B: Hybrid (Rust + Python HTTP)** | Rust orchestrator + Python parser service | Memory efficient, fault isolation | Two services, IPC overhead |
| **C: Hybrid (PyO3)** | Rust binary with embedded Python via PyO3 | Single binary, no network IPC | Complex builds, ARM64 issues, hard debugging |

### Decision

We adopt **Option B: Hybrid (Rust Orchestration + Python Parser Service)** with **format-aware routing**.

```
┌─────────────────────────────────────────────────────────────────┐
│               INGESTION ARCHITECTURE DECISION                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  SELECTED: Hybrid (Rust Orchestration + Python Parsing)         │
│                                                                 │
│  Key Design Principles:                                         │
│  • Rust orchestrator for memory-efficient, long-running core   │
│  • Python parser service isolated for complex document formats │
│  • Format-aware routing: simple formats parsed directly in Rust│
│  • Process isolation: parsing crashes don't affect orchestrator│
│  • Clear migration path: replace Python parsers incrementally  │
│                                                                 │
│  Trade-offs accepted:                                           │
│  • Two services to deploy and monitor                          │
│  • HTTP IPC adds ~10-50ms latency (acceptable for batch)       │
│  • Python dependency management on ARM64                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         HYBRID INGESTION PIPELINE                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  MinIO Event (s3:ObjectCreated:*)                                           │
│         │                                                                   │
│         ▼                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    NATS JetStream                                    │   │
│  │                    Stream: AKIDB_INGEST                              │   │
│  └──────────────────────────────┬──────────────────────────────────────┘   │
│                                 │                                           │
│                                 ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    RUST ORCHESTRATOR (tokio)                         │   │
│  │                                                                      │   │
│  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐    │   │
│  │  │    NATS      │   │   Format     │   │   TensorRT-LLM       │    │   │
│  │  │  Consumer    │──►│   Router     │──►│   Embedding Client   │    │   │
│  │  │ (async-nats) │   │              │   │   (batch 32-64)      │    │   │
│  │  └──────────────┘   └──────┬───────┘   └──────────┬───────────┘    │   │
│  │                            │                       │                │   │
│  │               ┌────────────┴────────────┐         │                │   │
│  │               │                         │         │                │   │
│  │               ▼                         ▼         ▼                │   │
│  │  ┌─────────────────────┐  ┌─────────────────────────────────────┐ │   │
│  │  │   RUST PARSERS      │  │   PYTHON PARSER SERVICE (HTTP)     │ │   │
│  │  │   (simple formats)  │  │   (complex formats)                │ │   │
│  │  │                     │  │                                     │ │   │
│  │  │ • JSON (serde_json) │  │ • PDF (pdfplumber)                 │ │   │
│  │  │ • CSV (csv crate)   │  │ • DOCX (python-docx)               │ │   │
│  │  │ • HTML (scraper)    │  │ • XLSX (openpyxl)                  │ │   │
│  │  │ • XML (quick-xml)   │  │ • ENL (custom)                     │ │   │
│  │  └─────────────────────┘  └─────────────────────────────────────┘ │   │
│  │                            │                       │                │   │
│  │                            └───────────┬───────────┘                │   │
│  │                                        │                            │   │
│  │                                        ▼                            │   │
│  │                            ┌──────────────────────┐                │   │
│  │                            │   Text Chunking      │                │   │
│  │                            │   (512 tokens,       │                │   │
│  │                            │    50 overlap)       │                │   │
│  │                            └──────────┬───────────┘                │   │
│  │                                       │                             │   │
│  │                                       ▼                             │   │
│  │                            ┌──────────────────────┐                │   │
│  │                            │   AkiDB gRPC Client  │                │   │
│  │                            │   (tonic)            │                │   │
│  │                            └──────────────────────┘                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Format-Aware Routing

| Format | Extension | Parser | Location | Rationale |
|--------|-----------|--------|----------|-----------|
| JSON | .json | serde_json | Rust | Trivial, zero-copy |
| CSV | .csv | csv crate | Rust | Streaming, memory-efficient |
| HTML | .html, .htm | scraper | Rust | DOM parsing well-supported |
| XML | .xml | quick-xml | Rust | 2-3x faster than lxml |
| PDF | .pdf | pdfplumber | Python | Table extraction, OCR support |
| DOCX | .docx | python-docx | Python | Complex XML, embedded objects |
| XLSX | .xlsx | openpyxl | Python | Formulas, charts |
| ENL | .enl | custom | Python | No Rust alternative |

**Routing Logic:**
```rust
fn route_document(filename: &str) -> ParserTarget {
    match Path::new(filename).extension().and_then(|e| e.to_str()) {
        Some("json") | Some("csv") | Some("html") | Some("htm") | Some("xml")
            => ParserTarget::RustNative,
        Some("pdf") | Some("docx") | Some("xlsx") | Some("xls") | Some("enl")
            => ParserTarget::PythonService,
        _ => ParserTarget::PythonService, // Default to Python for unknown
    }
}
```

### Component Specifications

#### Rust Orchestrator

| Aspect | Specification |
|--------|---------------|
| Runtime | tokio async |
| NATS Client | async-nats 0.33+ |
| gRPC Client | tonic 0.11+ |
| Simple Parsers | serde_json, csv, quick-xml, scraper |
| Memory Target | <50MB baseline |
| Container | ghcr.io/akidb/ingestion-orchestrator:arm64 |

**Rust Orchestrator Responsibilities:**
1. Consume MinIO events from NATS JetStream
2. Download files from MinIO
3. Route by format (Rust native or Python service)
4. Parse simple formats directly
5. Manage retry logic with exponential backoff
6. Batch embedding requests (32-64 chunks)
7. Insert vectors via AkiDB gRPC
8. Handle dead-letter queue

#### Python Parser Service

| Aspect | Specification |
|--------|---------------|
| Framework | FastAPI |
| Endpoint | POST /parse (accepts bytes, returns text + metadata) |
| Libraries | pdfplumber, python-docx, openpyxl |
| Container Limits | 2GB memory, 1 CPU |
| Container | ghcr.io/akidb/document-parser:arm64 |

**Python Parser Responsibilities:**
1. Accept document bytes via HTTP
2. Parse complex formats (PDF, DOCX, XLSX, ENL)
3. Return extracted text with page/section metadata
4. Timeout: 60s per document (fail fast on malformed docs)

#### Interface Contract

**Request:**
```http
POST /parse
Content-Type: application/octet-stream
X-Filename: report.pdf
X-Correlation-Id: abc-123

<binary document content>
```

**Response:**
```json
{
  "success": true,
  "pages": [
    {
      "page_number": 1,
      "text": "Extracted text content...",
      "tables": "| Header | Value |\n| --- | --- |\n...",
      "metadata": {"chars": 1500}
    }
  ],
  "total_pages": 42,
  "file_type": "pdf",
  "parse_time_ms": 1250
}
```

### Memory Analysis

| Component | Memory Usage | Justification |
|-----------|--------------|---------------|
| Rust Orchestrator | ~50MB | Tokio runtime, async buffers |
| Python Parser (idle) | ~150MB | Python interpreter, libs loaded |
| Python Parser (active) | ~500MB-2GB | Document in memory during parse |
| **Total Ingestion** | **~200MB-2GB** | Peak during large document parse |
| **FAISS + TensorRT** | **~40GB** | Primary GPU/memory consumers |

The hybrid approach ensures:
- Rust orchestrator's predictable 50MB footprint for long-running process
- Python parser's variable memory is isolated and capped at 2GB
- Peak ingestion memory (2GB) is <5% of unified memory budget

### Fault Isolation

```
┌─────────────────────────────────────────────────────────────────┐
│                     FAULT ISOLATION MODEL                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Scenario: Malformed PDF causes Python parser crash             │
│                                                                 │
│  ┌───────────────────┐        ┌───────────────────┐            │
│  │ Rust Orchestrator │───X───►│ Python Parser     │ ← crash    │
│  │ (still running)   │        │ (container restart)│            │
│  └───────────────────┘        └───────────────────┘            │
│           │                                                     │
│           ▼                                                     │
│  • Detects HTTP timeout after 60s                              │
│  • Logs error with correlation ID                              │
│  • Moves message to dead-letter queue                          │
│  • Continues processing next document                          │
│  • Python container auto-restarts via systemd                  │
│                                                                 │
│  Result: Single document fails, pipeline continues              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Migration Path

**Phase 1: Initial Deployment**
- Rust orchestrator + Python parser service
- All complex formats via Python
- Simple formats (JSON, CSV, HTML, XML) in Rust

**Phase 2: Profile and Optimize**
- Run profiling on production traffic
- Identify actual bottlenecks (likely TensorRT-LLM, not parsing)
- If parsing becomes bottleneck for specific formats, migrate incrementally

**Phase 3: Incremental Rust Adoption**
- XLSX → calamine crate (when mature)
- DOCX → docx-rs crate (when table support added)
- PDF → keep in Python (pdfplumber features unmatched)
- ENL → keep in Python (custom parser, no alternative)

### Deployment

#### Quadlet: Rust Orchestrator

```ini
# /etc/containers/systemd/ingestion-orchestrator.container
[Unit]
Description=AkiDB Ingestion Orchestrator
After=network-online.target nats.service akidb-shard.service document-parser.service
Wants=network-online.target

[Container]
Image=ghcr.io/akidb/ingestion-orchestrator:latest
ContainerName=ingestion-orchestrator
Environment=NATS_URL=nats://localhost:4222
Environment=MINIO_ENDPOINT=localhost:9000
Environment=AKIDB_COORDINATOR=localhost:50051
Environment=PARSER_SERVICE_URL=http://localhost:8001
Environment=TENSORRT_URL=http://localhost:8000
Environment=RUST_LOG=info
Volume=/etc/akidb/secrets:/run/secrets:ro,Z
Network=host
HealthCmd=curl -f http://localhost:9091/health
HealthInterval=30s

[Service]
Restart=always
RestartSec=5
MemoryMax=512M

[Install]
WantedBy=multi-user.target
```

#### Quadlet: Python Parser Service

```ini
# /etc/containers/systemd/document-parser.container
[Unit]
Description=AkiDB Document Parser Service
After=network-online.target

[Container]
Image=ghcr.io/akidb/document-parser:latest
ContainerName=document-parser
Environment=LOG_LEVEL=info
Environment=TIMEOUT_SECONDS=60
PublishPort=8001:8001
Network=host
HealthCmd=curl -f http://localhost:8001/health
HealthInterval=30s
HealthTimeout=10s

[Service]
Restart=always
RestartSec=5
MemoryMax=2G

[Install]
WantedBy=multi-user.target
```

### Rejected Alternatives

#### Option A: Pure Python

**Why rejected:**
- Memory usage unpredictable for long-running orchestrator
- GIL limits concurrent NATS consumption
- No process isolation for parsing failures
- Less memory-efficient in unified memory environment

**When appropriate:**
- Rapid prototyping
- Non-edge deployments with abundant memory

#### Option C: PyO3 (Embedded Python)

**Why rejected:**
- Complex cross-compilation for ARM64
- Python version must match exactly across updates
- Debugging FFI crashes is difficult on edge devices
- GIL still applies, requiring multiple processes

**When appropriate:**
- After profiling shows IPC is a bottleneck
- Single-binary deployment is operationally required
- Team has PyO3 expertise

### Consequences

**Positive:**
- Memory-efficient Rust orchestrator for core path
- Mature Python libraries for complex document parsing
- Fault isolation prevents parsing crashes from affecting pipeline
- Clear interface contract for incremental Rust migration
- 40-60% of typical documents parsed in Rust (JSON, CSV, HTML, XML)

**Negative:**
- Two services to deploy and monitor
- HTTP IPC adds latency (~10-50ms per document)
- Python dependency management on ARM64 requires containerization
- Team needs both Rust and Python expertise

**Neutral:**
- 30-minute SLO easily achievable with either approach
- Monitoring unchanged (both export Prometheus metrics)
- Container orchestration via quadlets same as other services

---

## Validation Checklist for v1.3

Before signing off on architecture:

- [ ] **Hardware:** Jetson Thor acquired and operational
- [ ] **FAISS:** GPU IVF-Flat benchmark at reference config
- [ ] **SLO:** Actual latency/recall documented
- [ ] **cuVS:** 24h shadow mode (if pursuing)
- [ ] **Delete:** Tombstone filtering validated
- [ ] **Rebuild:** Dual-index swap tested with concurrent ingest
- [ ] **Consistency:** Read-your-writes <100ms validated
- [ ] **Containers:** Podman + quadlets deployed on Thor
- [ ] **GPU passthrough:** CDI working with FAISS GPU
- [ ] **Rolling updates:** Script tested
- [ ] **Ingestion:** Rust orchestrator processing NATS events (NEW)
- [ ] **Parsing:** Python service handling PDF/DOCX/XLSX (NEW)
- [ ] **Format routing:** Simple formats parsed in Rust (NEW)
- [ ] **30-min SLO:** End-to-end ingestion validated (NEW)

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial ADRs |
| 1.1 | 2025-01-20 | AkiDB Team | cuVS gate, SLO boundaries, delete/update contract |
| 1.2 | 2026-01-21 | AkiDB Team | Container orchestration (Podman + quadlets) |
| 1.3 | 2026-01-21 | AkiDB Team | Hybrid ingestion pipeline (Rust + Python) |

---

*End of ADR v1.3*
