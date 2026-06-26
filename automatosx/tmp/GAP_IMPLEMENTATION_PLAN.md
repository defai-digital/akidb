# AkiDB Thor Edition - Gap Implementation Plan

**Created:** 2026-01-21
**Status:** Ready for Implementation
**Reference:** IMPLEMENTATION_GAP_REPORT.md

---

## Overview

This document provides detailed implementation plans for the remaining gaps identified in the gap analysis. Gaps are organized by priority and include step-by-step instructions.

---

## Priority P0 - Critical (Must Complete This Sprint)

### G-02: akidb-server Standalone Dockerfile

**Objective:** Create a standalone Dockerfile for the AkiDB shard server for CI/CD pipelines.

**Location:** `deploy/docker/akidb-server.Dockerfile`

**Implementation Steps:**

1. **Create multi-stage Dockerfile**
   ```dockerfile
   # Stage 1: Build
   FROM rust:1.75-bookworm AS builder

   WORKDIR /build
   COPY Cargo.toml Cargo.lock ./
   COPY crates/ crates/

   # Build release binary
   RUN cargo build --release --bin akidb-server

   # Stage 2: Runtime
   FROM debian:bookworm-slim

   # Install runtime dependencies
   RUN apt-get update && apt-get install -y \
       libssl3 \
       ca-certificates \
       && rm -rf /var/lib/apt/lists/*

   # Create non-root user (ADR-021)
   RUN groupadd -r akidb && useradd -r -g akidb akidb

   # Copy binary
   COPY --from=builder /build/target/release/akidb-server /usr/local/bin/

   # Set permissions
   RUN chown akidb:akidb /usr/local/bin/akidb-server

   USER akidb

   EXPOSE 50051 9090

   ENTRYPOINT ["akidb-server"]
   ```

2. **Add to docker-compose.yml**
   ```yaml
   akidb-server:
     build:
       context: ../..
       dockerfile: deploy/docker/akidb-server.Dockerfile
     user: "1000:1000"
     security_opt:
       - no-new-privileges:true
     cap_drop:
       - ALL
   ```

3. **Create CI/CD workflow** (`.github/workflows/build-server.yml`)

**Acceptance Criteria:**
- [ ] Dockerfile builds successfully
- [ ] Binary runs as non-root user
- [ ] Image size < 200MB
- [ ] Passes security scan (trivy/grype)

---

### G-03: akidb-coordinator Standalone Dockerfile

**Objective:** Create a standalone Dockerfile for the AkiDB coordinator for CI/CD pipelines.

**Location:** `deploy/docker/akidb-coordinator.Dockerfile`

**Implementation Steps:**

1. **Create multi-stage Dockerfile**
   ```dockerfile
   # Stage 1: Build
   FROM rust:1.75-bookworm AS builder

   WORKDIR /build
   COPY Cargo.toml Cargo.lock ./
   COPY crates/ crates/

   # Build release binary
   RUN cargo build --release --bin akidb-coordinator

   # Stage 2: Runtime
   FROM debian:bookworm-slim

   # Install runtime dependencies
   RUN apt-get update && apt-get install -y \
       libssl3 \
       ca-certificates \
       && rm -rf /var/lib/apt/lists/*

   # Create non-root user (ADR-021)
   RUN groupadd -r akidb && useradd -r -g akidb akidb

   # Copy binary
   COPY --from=builder /build/target/release/akidb-coordinator /usr/local/bin/

   # Set permissions
   RUN chown akidb:akidb /usr/local/bin/akidb-coordinator

   USER akidb

   EXPOSE 50052 9091

   ENTRYPOINT ["akidb-coordinator"]
   ```

2. **Add health check endpoint** in coordinator code if not present

3. **Create CI/CD workflow** (`.github/workflows/build-coordinator.yml`)

**Acceptance Criteria:**
- [ ] Dockerfile builds successfully
- [ ] Binary runs as non-root user
- [ ] Image size < 200MB
- [ ] Passes security scan

---

## Priority P1 - High (Next Sprint)

### G-04: DOCX-simple Parser in Rust

**Objective:** Implement a Rust-native DOCX parser for simple documents (no macros/complex formatting).

**Location:** `crates/ingestion-orchestrator/src/parsers/docx.rs`

**Dependencies:** Add to `Cargo.toml`:
```toml
docx-rs = "0.4"
zip = "0.6"
```

**Implementation Steps:**

1. **Create parser module**
   ```rust
   //! Simple DOCX parser using docx-rs
   //! Routes complex DOCX (macros, forms) to Python sidecar

   use docx_rs::*;
   use crate::{ParseResult, IngestionError};

   pub struct DocxParser;

   impl DocxParser {
       /// Check if DOCX is simple enough for Rust parsing
       pub fn is_simple(data: &[u8]) -> bool {
           // Check for:
           // - No macros (vbaProject.bin)
           // - No ActiveX controls
           // - No embedded OLE objects
           let reader = std::io::Cursor::new(data);
           if let Ok(archive) = zip::ZipArchive::new(reader) {
               for i in 0..archive.len() {
                   if let Ok(file) = archive.by_index(i) {
                       let name = file.name();
                       if name.contains("vbaProject") ||
                          name.contains("activeX") ||
                          name.contains("oleObject") {
                           return false;
                       }
                   }
               }
               return true;
           }
           false
       }

       /// Parse simple DOCX to text
       pub fn parse(data: &[u8]) -> Result<ParseResult, IngestionError> {
           let docx = Docx::from_bytes(data)
               .map_err(|e| IngestionError::Parse(e.to_string()))?;

           let mut text = String::new();

           for child in docx.document.children {
               if let DocumentChild::Paragraph(p) = child {
                   for child in p.children {
                       if let ParagraphChild::Run(r) = child {
                           for child in r.children {
                               if let RunChild::Text(t) = child {
                                   text.push_str(&t.text);
                               }
                           }
                       }
                   }
                   text.push('\n');
               }
           }

           Ok(ParseResult {
               text,
               format: "docx".to_string(),
               metadata: Default::default(),
           })
       }
   }
   ```

2. **Update format router** in `src/parsers/mod.rs`:
   ```rust
   pub fn is_rust_native(ext: &str) -> bool {
       matches!(ext, "json" | "csv" | "html" | "xml" | "xlsx" | "docx")
   }

   pub async fn parse(ext: &str, data: &[u8]) -> Result<ParseResult, IngestionError> {
       match ext {
           "docx" => {
               if DocxParser::is_simple(data) {
                   DocxParser::parse(data)
               } else {
                   // Route to Python for complex DOCX
                   python_client::parse(data, "docx").await
               }
           }
           // ... other formats
       }
   }
   ```

3. **Add tests** in `src/parsers/docx_tests.rs`

**Acceptance Criteria:**
- [ ] Parses simple .docx files correctly
- [ ] Routes complex DOCX (macros) to Python
- [ ] Benchmark shows 3x faster than Python for simple docs
- [ ] Unit tests pass

---

### G-06: DCGM GPU Metrics

**Objective:** Add NVIDIA DCGM GPU metrics to Prometheus monitoring.

**Location:** `deploy/compose/monitoring/prometheus.yml`

**Implementation Steps:**

1. **Add dcgm-exporter service** to docker-compose.yml:
   ```yaml
   dcgm-exporter:
     image: nvidia/dcgm-exporter:3.3.0-3.2.0-ubuntu22.04
     container_name: akidb-dcgm-exporter
     runtime: nvidia
     environment:
       DCGM_EXPORTER_LISTEN: ":9400"
       DCGM_EXPORTER_KUBERNETES: "false"
     ports:
       - "9400:9400"
     networks:
       - akidb-net
     security_opt:
       - no-new-privileges:true
     deploy:
       resources:
         reservations:
           devices:
             - driver: nvidia
               count: all
               capabilities: [gpu]
   ```

2. **Update Prometheus config** (`monitoring/prometheus.yml`):
   ```yaml
   scrape_configs:
     - job_name: 'dcgm-exporter'
       static_configs:
         - targets: ['dcgm-exporter:9400']
       scrape_interval: 5s
       metrics_path: /metrics
   ```

3. **Key metrics to monitor:**
   - `DCGM_FI_DEV_GPU_UTIL` - GPU utilization
   - `DCGM_FI_DEV_MEM_COPY_UTIL` - Memory bandwidth utilization
   - `DCGM_FI_DEV_FB_USED` - Framebuffer memory used
   - `DCGM_FI_DEV_POWER_USAGE` - Power consumption
   - `DCGM_FI_DEV_GPU_TEMP` - GPU temperature

**Acceptance Criteria:**
- [ ] DCGM exporter runs on Jetson Thor
- [ ] Metrics appear in Prometheus
- [ ] No performance impact on GPU workloads

---

### G-07: Grafana Dashboards

**Objective:** Create 4 Grafana dashboards per PRD requirements.

**Location:** `deploy/compose/grafana/dashboards/`

**Dashboards to Create:**

#### Dashboard 1: Ingestion Pipeline Overview
**File:** `ingestion-overview.json`

**Panels:**
1. Documents ingested (counter) - `ingestion_documents_total`
2. Ingestion rate (gauge) - `rate(ingestion_documents_total[5m])`
3. Format distribution (pie chart)
4. Error rate by format
5. Pipeline latency histogram
6. Active documents in pipeline

#### Dashboard 2: Resilience Patterns
**File:** `resilience-patterns.json`

**Panels:**
1. Circuit breaker state timeline - `circuit_breaker_state`
2. Backpressure active indicator - `backpressure_active`
3. Memory pressure gauge - `memory_pressure_level`
4. Unified memory usage - `unified_memory_used_bytes`
5. Python sidecar latency
6. Retry count by service

#### Dashboard 3: GPU Performance
**File:** `gpu-performance.json`

**Panels:**
1. GPU utilization - `DCGM_FI_DEV_GPU_UTIL`
2. Memory bandwidth - `DCGM_FI_DEV_MEM_COPY_UTIL`
3. Framebuffer usage - `DCGM_FI_DEV_FB_USED`
4. Power consumption - `DCGM_FI_DEV_POWER_USAGE`
5. Temperature - `DCGM_FI_DEV_GPU_TEMP`
6. Embedding throughput

#### Dashboard 4: AkiDB Cluster Health
**File:** `cluster-health.json`

**Panels:**
1. Shard status (up/down)
2. Query latency P50/P95/P99
3. Index size per shard
4. Replication lag
5. Coordinator fanout latency
6. Connection pool status

**Implementation Steps:**

1. **Create dashboard provisioning config:**
   ```yaml
   # deploy/compose/grafana/provisioning/dashboards/dashboards.yml
   apiVersion: 1
   providers:
     - name: 'AkiDB'
       folder: 'AkiDB Thor Edition'
       type: file
       options:
         path: /var/lib/grafana/dashboards
   ```

2. **Create each dashboard JSON file**

3. **Update docker-compose.yml:**
   ```yaml
   grafana:
     volumes:
       - ./grafana/provisioning:/etc/grafana/provisioning:ro
       - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
   ```

**Acceptance Criteria:**
- [ ] All 4 dashboards load without errors
- [ ] All panels show real data
- [ ] Dashboards are auto-provisioned on startup

---

## Priority P2 - Medium (Future Sprint)

### G-05: ENL Parser (Python)

**Objective:** Add EndNote library format (.enl) parsing support.

**Location:** `services/doc-parser/parser/parsers/enl.py`

**Implementation Steps:**

1. **Research ENL format** - EndNote uses XML-based format inside .enlx (ZIP archive)

2. **Create parser class:**
   ```python
   """EndNote library parser."""

   import zipfile
   import xml.etree.ElementTree as ET
   from pathlib import Path
   from typing import Any

   from parser.parsers.base import BaseParser, ParseResult


   class EnlParser(BaseParser):
       """Parser for EndNote library files (.enl, .enlx)."""

       SUPPORTED_EXTENSIONS = {".enl", ".enlx", ".enlp"}

       def parse(self, file_path: Path) -> ParseResult:
           """Parse EndNote library to extract references."""
           if file_path.suffix.lower() == ".enlx":
               return self._parse_enlx(file_path)
           else:
               return self._parse_enl(file_path)

       def _parse_enlx(self, file_path: Path) -> ParseResult:
           """Parse compressed EndNote library."""
           text_parts = []
           metadata = {"references": []}

           with zipfile.ZipFile(file_path, 'r') as zf:
               for name in zf.namelist():
                   if name.endswith('.xml'):
                       with zf.open(name) as f:
                           refs = self._parse_refs_xml(f.read())
                           metadata["references"].extend(refs)
                           text_parts.extend(
                               self._ref_to_text(r) for r in refs
                           )

           return ParseResult(
               text="\n\n".join(text_parts),
               format="enl",
               metadata=metadata,
           )

       def _parse_refs_xml(self, xml_data: bytes) -> list[dict]:
           """Parse EndNote references XML."""
           refs = []
           root = ET.fromstring(xml_data)

           for record in root.findall('.//record'):
               ref = {
                   "title": self._get_text(record, 'title'),
                   "authors": [a.text for a in record.findall('.//author')],
                   "year": self._get_text(record, 'year'),
                   "journal": self._get_text(record, 'secondary-title'),
                   "abstract": self._get_text(record, 'abstract'),
                   "doi": self._get_text(record, 'electronic-resource-num'),
               }
               refs.append(ref)

           return refs

       def _ref_to_text(self, ref: dict) -> str:
           """Convert reference to searchable text."""
           parts = []
           if ref.get("title"):
               parts.append(f"Title: {ref['title']}")
           if ref.get("authors"):
               parts.append(f"Authors: {', '.join(ref['authors'])}")
           if ref.get("year"):
               parts.append(f"Year: {ref['year']}")
           if ref.get("abstract"):
               parts.append(f"Abstract: {ref['abstract']}")
           return "\n".join(parts)
   ```

3. **Register parser** in `parsers/__init__.py`

4. **Add tests** with sample .enlx files

**Acceptance Criteria:**
- [ ] Parses .enl, .enlx, .enlp files
- [ ] Extracts reference metadata (title, authors, year, abstract)
- [ ] Returns searchable text output

---

### G-08: cuVS Evaluation

**Objective:** Evaluate NVIDIA cuVS as potential FAISS GPU replacement.

**Location:** `docs/evaluation/cuvs-evaluation.md`

**Evaluation Criteria:**

1. **Performance Benchmarks**
   - Indexing throughput (vectors/sec)
   - Search latency (P50, P95, P99)
   - Memory efficiency
   - GPU utilization

2. **Feature Comparison**
   | Feature | FAISS GPU | cuVS |
   |---------|-----------|------|
   | IVF-Flat | ✅ | ? |
   | IVF-PQ | ✅ | ? |
   | HNSW | CPU only | ? |
   | Unified memory | Manual | ? |
   | Multi-GPU | ✅ | ? |

3. **API Compatibility**
   - Rust bindings availability
   - Migration effort estimate

4. **Recommendation criteria:**
   - Switch if: >30% latency improvement OR >2x throughput
   - Stay with FAISS if: <10% improvement

**Implementation Steps:**

1. **Set up cuVS benchmark environment**
2. **Run comparative benchmarks**
3. **Document findings**
4. **Make recommendation**

---

## Priority P3 - Low (Future Enhancements)

### G-09: OCR for Scanned PDFs

**Objective:** Add OCR capability for scanned/image-based PDFs.

**Approach:** Use Tesseract or PaddleOCR via Python sidecar.

**Deferred:** Implement when use case arises.

---

### G-10: Malware Scanning

**Objective:** Scan uploaded files for malware before processing.

**Approach:** Integrate ClamAV or YARA rules.

**Deferred:** Implement based on security requirements.

---

## Implementation Timeline

| Week | Tasks |
|------|-------|
| 1 | G-02: akidb-server Dockerfile |
| 1 | G-03: akidb-coordinator Dockerfile |
| 2 | G-06: DCGM GPU metrics |
| 2-3 | G-04: DOCX-simple parser |
| 3-4 | G-07: Grafana dashboards (4) |
| 5 | G-05: ENL parser (if needed) |
| 6+ | G-08: cuVS evaluation |

---

## Dependencies & Prerequisites

### For G-02, G-03 (Dockerfiles):
- Rust 1.75+ installed
- Docker BuildKit enabled
- Access to container registry

### For G-04 (DOCX parser):
- `docx-rs` crate compatibility verified
- Sample DOCX test files

### For G-06 (DCGM):
- NVIDIA GPU available
- `nvidia-docker` or `--runtime=nvidia`
- DCGM driver installed

### For G-07 (Dashboards):
- Prometheus metrics endpoints working
- Grafana provisioning configured

---

## Testing Requirements

| Gap | Unit Tests | Integration Tests | E2E Tests |
|-----|------------|-------------------|-----------|
| G-02 | N/A | Docker build | Health check |
| G-03 | N/A | Docker build | Health check |
| G-04 | Parser tests | Route to Python | Full pipeline |
| G-05 | Parser tests | API endpoint | Full pipeline |
| G-06 | N/A | Metrics scrape | Dashboard load |
| G-07 | N/A | Provisioning | Panel queries |

---

*End of Gap Implementation Plan*
