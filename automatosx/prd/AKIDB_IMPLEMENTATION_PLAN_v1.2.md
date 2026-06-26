# AkiDB Thor Edition - Implementation Plan

**Version:** 1.2
**Date:** 2026-01-21
**Status:** Approved
**Based On:** ADR v1.2, PRD v1.3
**Review:** Multi-model synthesis (Claude, Gemini, Grok)
**Changes from v1.1:** Added Python Ingestion Service, NATS JetStream, Upload Gateway

---

## Change Log from v1.1

| Section | Change | Rationale |
|---------|--------|-----------|
| Phase 2 | Added Python Ingestion Service tasks | Document-to-vector pipeline |
| Phase 2 | Added NATS JetStream setup | Event-driven ingestion |
| Phase 2 | Added Upload Gateway service | Pre-signed URL uploads |
| Timeline | Extended Phase 2 by 2 weeks | Ingestion development |
| Dependencies | Added Python/FastAPI/NATS to stack | Ingestion requirements |

---

## Executive Summary

This implementation plan covers the development of AkiDB Thor Edition over **~24 weeks (~6 months)** across 4 phases plus a validation sprint. The plan now includes a **Python Ingestion Pipeline** for document parsing and chunking.

**Key Updates in v1.2:**
- Python sidecar services for document ingestion
- NATS JetStream for event-driven processing
- Upload Gateway with pre-signed URLs
- 30-minute batch SLO for document-to-searchable

---

## Timeline Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        AKIDB THOR IMPLEMENTATION TIMELINE v1.2                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Week 0       │ Weeks 1-6     │ Weeks 7-14     │ Weeks 15-20  │ Weeks 21-24    │
│  ┌──────────┐ │ ┌───────────┐ │ ┌────────────┐ │ ┌──────────┐ │ ┌────────────┐ │
│  │VALIDATION│ │ │  PHASE 1  │ │ │  PHASE 2   │ │ │ PHASE 3  │ │ │  PHASE 4   │ │
│  │  SPRINT  │ │ │Foundation │ │ │Distribution│ │ │Optimize  │ │ │ Production │ │
│  │ (1 week) │ │ │ (6 weeks) │ │ │+ INGESTION │ │ │(6 weeks) │ │ │  (4 weeks) │ │
│  └──────────┘ │ └───────────┘ │ │ (8 weeks)  │ │ └──────────┘ │ └────────────┘ │
│               │               │ └────────────┘ │              │                │
│  Hardware     │ Single-node   │ Multi-node     │ TensorRT     │ cuVS           │
│  Podman + CDI │ FAISS GPU     │ Fan-out        │ Rebuild      │ Production     │
│  CI/CD        │ gRPC + RocksDB│ +INGESTION     │ Performance  │ Quadlets       │
│  Dockerfile   │               │ +NATS          │              │                │
│               │               │ +Upload GW     │              │                │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Total Duration:** 24-25 weeks (~6 months) - *Extended by 2 weeks for ingestion*

---

## Phase 0: Validation Sprint (Week 0)

*(Unchanged from v1.1 - see previous version)*

### Additional Validation Tasks

| ID | Task | Owner | Duration | Exit Criteria |
|----|------|-------|----------|---------------|
| **V-12** | **Validate Python 3.11 on Thor** | **DevOps** | **0.5 day** | **python3 --version succeeds** |
| **V-13** | **Test NATS on ARM64** | **DevOps** | **0.5 day** | **NATS server runs on Thor** |

---

## Phase 1: Foundation (Weeks 1-6)

*(Unchanged from v1.1 - see previous version)*

---

## Phase 2: Distribution + Ingestion (Weeks 7-14) - SIGNIFICANTLY UPDATED

### Objectives
- Establish distributed coordination with fan-out
- **Implement Python Ingestion Pipeline (NEW)**
- **Deploy NATS JetStream (NEW)**
- **Create Upload Gateway (NEW)**
- Implement tombstone deletes

### Sprint Breakdown

#### Sprint 4 (Weeks 7-8): Fan-out Coordinator + NATS Setup

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-01 | Coordinator service with shard discovery | P0 | 3d | Phase 1 |
| P2-02 | Fan-out search (parallel shard queries) | P0 | 4d | P2-01 |
| P2-03 | Result aggregation (merge + dedup) | P0 | 2d | P2-02 |
| P2-04 | Consistent hashing for shard routing | P0 | 2d | P2-01 |
| **P2-05** | **Deploy NATS JetStream (single node)** | **P0** | **1d** | **-** |
| **P2-06** | **Create NATS quadlet file** | **P0** | **0.5d** | **P2-05** |

**Sprint 4 Exit Criteria:**
- [ ] Fan-out search works across 3 shards
- [ ] **NATS JetStream running on Thor 4 (NEW)**
- [ ] Result deduplication functional

#### Sprint 5 (Weeks 9-10): Tombstones + Upload Gateway

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| P2-07 | Tombstone delete implementation | P0 | 3d | Phase 1 |
| P2-08 | GPU bitset for tombstone filtering | P0 | 4d | P2-07 |
| P2-09 | Tombstone persistence in RocksDB | P0 | 2d | P2-07 |
| **P2-10** | **Create Upload Gateway (FastAPI)** | **P0** | **3d** | **-** |
| **P2-11** | **Pre-signed URL generation** | **P0** | **1d** | **P2-10** |
| **P2-12** | **MinIO event notification to NATS** | **P0** | **1d** | **P2-05** |

**Sprint 5 Exit Criteria:**
- [ ] Deletes don't appear in search results
- [ ] **Upload Gateway accepts HTTP uploads (NEW)**
- [ ] **MinIO triggers NATS events on upload (NEW)**

#### Sprint 6 (Weeks 11-12): Ingestion Worker (Core)

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| **P2-13** | **Create Ingestion Worker (Python)** | **P0** | **2d** | **-** |
| **P2-14** | **Implement document parsers (PDF, DOCX)** | **P0** | **3d** | **P2-13** |
| **P2-15** | **Implement document parsers (XLSX, CSV)** | **P0** | **2d** | **P2-13** |
| **P2-16** | **Implement document parsers (HTML, XML, JSON)** | **P1** | **2d** | **P2-13** |
| **P2-17** | **Text chunking with LangChain** | **P0** | **2d** | **P2-14** |
| **P2-18** | **TensorRT embedding client** | **P0** | **2d** | **P2-13** |

**Sprint 6 Exit Criteria:**
- [ ] **PDF, DOCX parsing works (NEW)**
- [ ] **Chunking produces 512-token chunks (NEW)**
- [ ] **Embeddings generated via TensorRT (NEW)**

#### Sprint 7 (Weeks 13-14): Ingestion Integration + Testing

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| **P2-19** | **AkiDB gRPC client (Python)** | **P0** | **2d** | **P2-13** |
| **P2-20** | **End-to-end ingestion pipeline** | **P0** | **3d** | **P2-19** |
| **P2-21** | **Upload status tracking (RocksDB)** | **P1** | **2d** | **P2-10** |
| **P2-22** | **Dead letter queue handling** | **P1** | **1d** | **P2-20** |
| **P2-23** | **Ingestion metrics (Prometheus)** | **P0** | **1d** | **P2-20** |
| P2-24 | Distributed load testing | P0 | 2d | All |
| **P2-25** | **Ingestion Worker Dockerfile** | **P0** | **1d** | **P2-20** |
| **P2-26** | **Upload Gateway Dockerfile** | **P0** | **1d** | **P2-10** |

**Sprint 7 Exit Criteria:**
- [ ] **Upload → Parse → Chunk → Embed → Search works (NEW)**
- [ ] **30-minute SLO validated (NEW)**
- [ ] **Ingestion metrics in Prometheus (NEW)**
- [ ] 100 QPS distributed search achieved

### Ingestion Pipeline Implementation Details (NEW)

#### P2-10: Upload Gateway (FastAPI)

```
services/upload-gateway/
├── Dockerfile
├── requirements.txt
├── app/
│   ├── __init__.py
│   ├── main.py           # FastAPI app
│   ├── config.py         # Settings
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── upload.py     # POST /upload
│   │   └── status.py     # GET /status/{job_id}
│   ├── services/
│   │   ├── __init__.py
│   │   ├── minio.py      # MinIO client
│   │   └── presigned.py  # Pre-signed URL generation
│   └── models/
│       ├── __init__.py
│       └── upload.py     # Pydantic models
└── tests/
    └── test_upload.py
```

**requirements.txt:**
```
fastapi==0.109.0
uvicorn[standard]==0.27.0
minio==7.2.3
pydantic==2.5.3
python-multipart==0.0.6
httpx==0.26.0
prometheus-client==0.19.0
```

#### P2-13: Ingestion Worker (Python)

```
services/ingestion-worker/
├── Dockerfile
├── requirements.txt
├── worker/
│   ├── __init__.py
│   ├── main.py           # Entry point
│   ├── config.py         # Settings
│   ├── consumer.py       # NATS consumer
│   ├── pipeline.py       # Orchestration
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── base.py       # Abstract parser
│   │   ├── pdf.py        # PDF parser
│   │   ├── docx.py       # DOCX parser
│   │   ├── xlsx.py       # XLSX parser
│   │   ├── csv_parser.py # CSV parser
│   │   ├── html.py       # HTML parser
│   │   ├── xml.py        # XML parser
│   │   ├── json_parser.py # JSON parser
│   │   └── enl.py        # EndNote parser
│   ├── chunking/
│   │   ├── __init__.py
│   │   └── splitter.py   # Text chunking
│   ├── embedding/
│   │   ├── __init__.py
│   │   └── tensorrt.py   # TensorRT client
│   └── storage/
│       ├── __init__.py
│       ├── minio.py      # MinIO client
│       └── akidb.py      # AkiDB gRPC client
└── tests/
    ├── test_parsers.py
    ├── test_chunking.py
    └── test_pipeline.py
```

**requirements.txt:**
```
# NATS
nats-py==2.6.0

# Document parsing
pypdf==3.17.4
pdfplumber==0.10.3
python-docx==1.1.0
openpyxl==3.1.2
pandas==2.1.4
beautifulsoup4==4.12.3
lxml==5.1.0

# Text processing
langchain==0.1.4
langchain-text-splitters==0.0.1
tiktoken==0.5.2

# Clients
httpx==0.26.0
grpcio==1.60.0
grpcio-tools==1.60.0
minio==7.2.3

# Observability
prometheus-client==0.19.0
structlog==24.1.0

# Core
pydantic==2.5.3
pydantic-settings==2.1.0
```

#### P2-14: Document Parser Example (PDF)

```python
# worker/parsers/pdf.py
from typing import List
import pdfplumber
from .base import BaseParser, ParsedDocument, ParsedPage

class PDFParser(BaseParser):
    """PDF document parser using pdfplumber."""

    supported_extensions = ['.pdf']

    async def parse(self, file_path: str) -> ParsedDocument:
        pages: List[ParsedPage] = []

        with pdfplumber.open(file_path) as pdf:
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                tables = page.extract_tables()

                # Convert tables to text
                table_text = self._tables_to_text(tables)

                pages.append(ParsedPage(
                    page_number=i + 1,
                    text=text,
                    tables=table_text,
                    metadata={"width": page.width, "height": page.height}
                ))

        return ParsedDocument(
            pages=pages,
            total_pages=len(pages),
            metadata={"parser": "pdfplumber"}
        )

    def _tables_to_text(self, tables: List) -> str:
        """Convert extracted tables to markdown format."""
        if not tables:
            return ""

        result = []
        for table in tables:
            if table:
                # Convert to markdown table
                header = " | ".join(str(cell) for cell in table[0])
                separator = " | ".join("---" for _ in table[0])
                rows = [" | ".join(str(cell) for cell in row) for row in table[1:]]
                result.append(f"{header}\n{separator}\n" + "\n".join(rows))

        return "\n\n".join(result)
```

#### P2-17: Text Chunking

```python
# worker/chunking/splitter.py
from typing import List
from langchain.text_splitter import RecursiveCharacterTextSplitter
import tiktoken

class DocumentChunker:
    """Chunk documents into embedding-sized pieces."""

    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        model: str = "cl100k_base"
    ):
        self.encoding = tiktoken.get_encoding(model)
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=self._token_counter,
            separators=["\n\n", "\n", ". ", " ", ""]
        )

    def _token_counter(self, text: str) -> int:
        return len(self.encoding.encode(text))

    def chunk(self, text: str, metadata: dict = None) -> List[dict]:
        """Split text into chunks with metadata."""
        chunks = self.splitter.split_text(text)

        return [
            {
                "text": chunk,
                "chunk_index": i,
                "total_chunks": len(chunks),
                "token_count": self._token_counter(chunk),
                **(metadata or {})
            }
            for i, chunk in enumerate(chunks)
        ]
```

#### P2-18: TensorRT Embedding Client

```python
# worker/embedding/tensorrt.py
from typing import List
import httpx
from pydantic import BaseModel

class EmbeddingResponse(BaseModel):
    embeddings: List[List[float]]

class TensorRTEmbeddingClient:
    """Client for TensorRT-LLM embedding service."""

    def __init__(self, base_url: str, model: str = "bge-base-en-v1.5"):
        self.base_url = base_url
        self.model = model
        self.client = httpx.AsyncClient(timeout=30.0)

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a batch of texts."""
        response = await self.client.post(
            f"{self.base_url}/v1/embeddings",
            json={
                "input": texts,
                "model": self.model
            }
        )
        response.raise_for_status()

        data = response.json()
        return [item["embedding"] for item in data["data"]]

    async def close(self):
        await self.client.aclose()
```

#### P2-20: End-to-End Pipeline

```python
# worker/pipeline.py
import asyncio
from typing import Optional
import structlog
from .parsers import get_parser
from .chunking import DocumentChunker
from .embedding import TensorRTEmbeddingClient
from .storage import MinIOClient, AkiDBClient

logger = structlog.get_logger()

class IngestionPipeline:
    """Orchestrates document ingestion."""

    def __init__(self, config):
        self.minio = MinIOClient(config.minio)
        self.chunker = DocumentChunker(
            chunk_size=config.chunk_size,
            chunk_overlap=config.chunk_overlap
        )
        self.embedder = TensorRTEmbeddingClient(
            base_url=config.tensorrt_url,
            model=config.embedding_model
        )
        self.akidb = AkiDBClient(config.akidb_coordinator)

    async def process(self, event: dict) -> dict:
        """Process a single document upload event."""
        bucket = event["bucket"]
        key = event["key"]
        correlation_id = event.get("correlation_id", "unknown")

        log = logger.bind(
            bucket=bucket,
            key=key,
            correlation_id=correlation_id
        )

        try:
            # 1. Download file
            log.info("Fetching document from MinIO")
            file_path = await self.minio.download(bucket, key)

            # 2. Parse document
            log.info("Parsing document")
            parser = get_parser(key)
            document = await parser.parse(file_path)

            # 3. Chunk text
            log.info("Chunking document", pages=document.total_pages)
            all_chunks = []
            for page in document.pages:
                chunks = self.chunker.chunk(
                    page.text,
                    metadata={
                        "source_file": key,
                        "page_number": page.page_number,
                        "correlation_id": correlation_id
                    }
                )
                all_chunks.extend(chunks)

            # 4. Generate embeddings (batch)
            log.info("Generating embeddings", chunks=len(all_chunks))
            texts = [c["text"] for c in all_chunks]
            embeddings = await self.embedder.embed(texts)

            # 5. Insert into AkiDB
            log.info("Inserting vectors into AkiDB")
            vectors = [
                {
                    "embedding": emb,
                    "metadata": {
                        **chunk,
                        "text_preview": chunk["text"][:200]
                    }
                }
                for emb, chunk in zip(embeddings, all_chunks)
            ]
            await self.akidb.insert_batch(vectors)

            # 6. Cleanup
            log.info("Cleaning up source file")
            await self.minio.delete(bucket, key)

            log.info(
                "Ingestion complete",
                chunks=len(all_chunks),
                vectors=len(vectors)
            )

            return {
                "status": "success",
                "chunks": len(all_chunks),
                "vectors": len(vectors)
            }

        except Exception as e:
            log.error("Ingestion failed", error=str(e))
            raise
```

### Phase 2 Deliverables (UPDATED)

- [ ] Distributed coordinator with fan-out search
- [ ] Tombstone deletes with GPU bitset filtering
- [ ] **Upload Gateway (FastAPI) (NEW)**
- [ ] **NATS JetStream deployment (NEW)**
- [ ] **Ingestion Worker with parsers (NEW)**
- [ ] **End-to-end document → vector pipeline (NEW)**
- [ ] Load test report (100 QPS target)
- [ ] **Ingestion metrics dashboard (NEW)**

### Phase 2 Exit Gate (UPDATED)

| Criteria | Target | Validated |
|----------|--------|-----------|
| Fan-out search P95 | < 50ms | [ ] |
| Tombstone filtering | 100% accurate | [ ] |
| Distributed throughput | 100 QPS | [ ] |
| **Upload → Search SLO** | < 30 min | [ ] |
| **PDF parsing success** | > 95% | [ ] |
| **DOCX parsing success** | > 95% | [ ] |
| **Ingestion metrics** | Exported | [ ] |

---

## Phase 3: Optimization (Weeks 15-20)

*(Largely unchanged from v1.1)*

### Additional Tasks

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| **P3-20** | **Optimize ingestion batch size** | **P1** | **2d** | **Phase 2** |
| **P3-21** | **Add ENL (EndNote) parser** | **P2** | **2d** | **Phase 2** |
| **P3-22** | **Ingestion load testing (1000 docs/hr)** | **P1** | **2d** | **P3-20** |

---

## Phase 4: Production (Weeks 21-24)

*(Largely unchanged from v1.1)*

### Additional Deployment Tasks

| ID | Task | Priority | Estimate | Dependencies |
|----|------|----------|----------|--------------|
| **P4-15** | **Create upload-gateway.container quadlet** | **P0** | **0.5d** | **Phase 2** |
| **P4-16** | **Create ingestion-worker.container quadlet** | **P0** | **0.5d** | **Phase 2** |
| **P4-17** | **Create nats.container quadlet** | **P0** | **0.5d** | **Phase 2** |
| **P4-18** | **Update Ansible playbook for ingestion services** | **P0** | **1d** | **P4-15, P4-16, P4-17** |

### Ingestion Quadlet Files (NEW)

#### upload-gateway.container

```ini
[Unit]
Description=AkiDB Upload Gateway
After=network-online.target minio.service
Wants=network-online.target

[Container]
Image=ghcr.io/akidb/upload-gateway:latest
ContainerName=upload-gateway
Environment=MINIO_ENDPOINT=localhost:9000
Environment=MINIO_ACCESS_KEY_FILE=/run/secrets/minio-access-key
Environment=MINIO_SECRET_KEY_FILE=/run/secrets/minio-secret-key
Environment=UPLOAD_BUCKET=uploads
Environment=PRESIGNED_URL_EXPIRY=900
Volume=/etc/akidb/secrets:/run/secrets:ro,Z
Network=host
HealthCmd=curl -f http://localhost:8000/health
HealthInterval=10s
HealthTimeout=5s
HealthRetries=3

[Service]
Restart=always
RestartSec=5
TimeoutStartSec=60

[Install]
WantedBy=multi-user.target
```

#### ingestion-worker.container

```ini
[Unit]
Description=AkiDB Ingestion Worker
After=network-online.target nats.service akidb-shard.service
Wants=network-online.target

[Container]
Image=ghcr.io/akidb/ingestion-worker:latest
ContainerName=ingestion-worker
Environment=NATS_URL=nats://localhost:4222
Environment=MINIO_ENDPOINT=localhost:9000
Environment=MINIO_ACCESS_KEY_FILE=/run/secrets/minio-access-key
Environment=MINIO_SECRET_KEY_FILE=/run/secrets/minio-secret-key
Environment=AKIDB_COORDINATOR=localhost:50051
Environment=TENSORRT_URL=http://localhost:8001
Environment=WORKER_CONCURRENCY=4
Volume=/etc/akidb/secrets:/run/secrets:ro,Z
Volume=/tmp/ingestion:/tmp/ingestion:Z
Network=host
HealthCmd=python -c "import nats; print('ok')"
HealthInterval=30s
HealthTimeout=10s
HealthRetries=3

[Service]
Restart=always
RestartSec=10
TimeoutStartSec=120
# Allow sufficient memory for document parsing
MemoryMax=2G

[Install]
WantedBy=multi-user.target
```

#### nats.container

```ini
[Unit]
Description=NATS JetStream Server
After=network-online.target

[Container]
Image=nats:2.10-alpine
ContainerName=nats
PublishPort=4222:4222
PublishPort=8222:8222
Volume=/var/lib/nats:/data:Z
Environment=NATS_CONFIG=/etc/nats/nats.conf
Network=host
HealthCmd=wget -q --spider http://localhost:8222/healthz
HealthInterval=10s
HealthTimeout=5s
HealthRetries=3

[Service]
Restart=always
RestartSec=5
TimeoutStartSec=30

[Install]
WantedBy=multi-user.target
```

---

## Critical Path Dependencies (UPDATED)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                   CRITICAL PATH DEPENDENCY DAG v1.2                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Week 0: Hardware + Podman/CDI + Python/NATS Validation                     │
│              │                                                              │
│              ▼                                                              │
│  Phase 1: FAISS-rs GPU ──► gRPC ──► Dockerfile                             │
│              │                                                              │
│              ▼                                                              │
│  Phase 2: ┌──────────────────────────────────────────────┐                 │
│           │                                              │                 │
│           │  Coordinator ◄─────────────────┐             │                 │
│           │      │                         │             │                 │
│           │      ├──► Tombstones          │             │                 │
│           │      │                         │             │                 │
│           │      └──► Fan-out             │             │                 │
│           │                                │             │                 │
│           │  NATS ──► Upload GW ──► Ingestion Worker    │  (PARALLEL)     │
│           │              │              │                │                 │
│           │              └──────────────┘                │                 │
│           │                     │                        │                 │
│           │                     ▼                        │                 │
│           │           End-to-End Pipeline                │                 │
│           │                                              │                 │
│           └──────────────────────────────────────────────┘                 │
│              │                                                              │
│              ▼                                                              │
│  Phase 3: TensorRT ──► Rebuild ──► Ingestion Optimization                  │
│              │                                                              │
│              ▼                                                              │
│  Phase 4: cuVS + Quadlets (Shard + Coordinator + Ingestion)                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Team Allocation (UPDATED)

| Role | Phase 0 | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|------|---------|---------|---------|---------|---------|
| **Rust Engineer 1** | FAISS bench | FAISS wrapper | Coordinator | Rebuild | cuVS |
| **Rust Engineer 2** | - | gRPC, RocksDB | Tombstones | WAL | Hardening |
| **ML Engineer** | CUDA compat | - | - | TensorRT | cuVS |
| **DevOps** | Podman/CDI, **NATS** | Dockerfile | Load testing, **Ingestion quadlets** | Automation | Quadlets |
| **Python Engineer (NEW)** | - | - | **Upload GW, Ingestion Worker, Parsers** | **Optimization** | **Testing** |

---

## Technology Stack (UPDATED)

### Core (Rust)
- FAISS 1.8+ (GPU IVF-Flat)
- RocksDB 7.8+
- Tonic (gRPC)
- Tokio (async runtime)

### Ingestion (Python) - NEW
- Python 3.11
- FastAPI 0.109+
- NATS.py 2.6+
- LangChain 0.1+
- pdfplumber 0.10+
- python-docx 1.1+
- openpyxl 3.1+

### Infrastructure
- Podman 4.0+
- NATS JetStream 2.10+
- MinIO (distributed)
- Prometheus + Grafana

---

## Deliverables Summary (UPDATED)

### Ingestion Deliverables (NEW)

| Phase | Deliverable | Description |
|-------|-------------|-------------|
| 0 | Python/NATS validated | Runtime confirmed on Thor |
| 2 | Upload Gateway | FastAPI service with pre-signed URLs |
| 2 | Ingestion Worker | Document parsing + chunking + embedding |
| 2 | NATS JetStream | Event-driven message queue |
| 2 | Parsers | PDF, DOCX, XLSX, CSV, HTML, XML, JSON |
| 3 | ENL parser | EndNote format support |
| 4 | Ingestion quadlets | Production deployment |

---

## Open Questions (UPDATED)

### Resolved in v1.2

| ID | Question | Resolution |
|----|----------|------------|
| Q7 | Document parsing approach? | Python sidecar (not Rust) |
| Q8 | Message queue? | NATS JetStream |
| Q9 | Chunking strategy? | LangChain RecursiveCharacterTextSplitter |

### Remaining Questions

| ID | Question | Options | Decision By |
|----|----------|---------|-------------|
| Q10 | TensorRT vs vLLM for embedding? | TensorRT (primary) | Phase 2 |
| Q11 | Malware scanning for uploads? | ClamAV vs cloud API | Phase 3 |
| Q12 | OCR for scanned PDFs? | Tesseract vs cloud | Phase 3 |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-20 | AkiDB Team | Initial implementation plan |
| 1.1 | 2026-01-21 | AkiDB Team | Added Podman + quadlets deployment |
| 1.2 | 2026-01-21 | AkiDB Team | Added Python Ingestion Service, NATS, Upload Gateway |

---

*End of Implementation Plan v1.2*
