# AkiDB Thor Edition - Python Ingestion Service Design

**Version:** 1.0
**Date:** 2026-01-21
**Author:** AkiDB Team
**Status:** Approved
**Based On:** PRD v1.3, Implementation Plan v1.2

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Component Details](#3-component-details)
4. [Document Parsers](#4-document-parsers)
5. [Text Chunking](#5-text-chunking)
6. [Embedding Integration](#6-embedding-integration)
7. [Error Handling](#7-error-handling)
8. [Observability](#8-observability)
9. [Deployment](#9-deployment)
10. [API Reference](#10-api-reference)

---

## 1. Overview

### 1.1 Purpose

The Python Ingestion Service converts uploaded documents into searchable vectors in AkiDB. It handles:

- Document upload via pre-signed URLs
- Multi-format document parsing (PDF, DOCX, XLSX, CSV, HTML, XML, JSON, ENL)
- Text chunking with configurable overlap
- Embedding generation via TensorRT-LLM
- Vector insertion into AkiDB

### 1.2 Design Principles

1. **Event-driven:** Triggered by MinIO bucket notifications via NATS
2. **Stateless workers:** Horizontal scaling without coordination
3. **At-least-once delivery:** With idempotent processing
4. **Graceful degradation:** Parser fallbacks, circuit breakers
5. **Observable:** Metrics, tracing, structured logging

### 1.3 SLOs

| Metric | Target |
|--------|--------|
| Upload to Searchable | < 30 minutes (P95) |
| Parse success rate | > 95% |
| Worker availability | > 99.5% |

---

## 2. Architecture

### 2.1 System Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         INGESTION SERVICE ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│                           ┌─────────────────────┐                           │
│                           │      Client         │                           │
│                           │   (Browser/CLI)     │                           │
│                           └──────────┬──────────┘                           │
│                                      │                                      │
│                              HTTP POST /upload                              │
│                                      │                                      │
│                                      ▼                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                         UPLOAD GATEWAY                                 │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │ │
│  │  │  FastAPI Application                                            │  │ │
│  │  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │  │ │
│  │  │  │   /upload   │ │  /status/   │ │  /health    │               │  │ │
│  │  │  │   (POST)    │ │  {job_id}   │ │   (GET)     │               │  │ │
│  │  │  └──────┬──────┘ └──────┬──────┘ └─────────────┘               │  │ │
│  │  │         │               │                                       │  │ │
│  │  │         ▼               ▼                                       │  │ │
│  │  │  ┌─────────────────────────────┐                               │  │ │
│  │  │  │    Pre-signed URL Service   │                               │  │ │
│  │  │  │  - Validate file type       │                               │  │ │
│  │  │  │  - Generate upload URL      │                               │  │ │
│  │  │  │  - Track job status         │                               │  │ │
│  │  │  └──────────────┬──────────────┘                               │  │ │
│  │  └─────────────────│──────────────────────────────────────────────┘  │ │
│  └────────────────────│─────────────────────────────────────────────────┘ │
│                       │                                                    │
│          Pre-signed URL returned to client                                │
│                       │                                                    │
│                       ▼                                                    │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                              MINIO                                     │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │ │
│  │  │  Bucket: uploads/                                               │  │ │
│  │  │  ┌─────────┐ ┌─────────┐ ┌─────────┐                           │  │ │
│  │  │  │ doc1.pdf│ │ doc2.doc│ │data.xlsx│                           │  │ │
│  │  │  └─────────┘ └─────────┘ └─────────┘                           │  │ │
│  │  │                                                                 │  │ │
│  │  │  Event Notification: s3:ObjectCreated:*                        │  │ │
│  │  └──────────────────────────┬──────────────────────────────────────┘  │ │
│  └─────────────────────────────│─────────────────────────────────────────┘ │
│                                │                                           │
│                     S3 Event (JSON)                                        │
│                                │                                           │
│                                ▼                                           │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                         NATS JETSTREAM                                 │ │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │ │
│  │  │  Stream: AKIDB_INGEST                                           │  │ │
│  │  │  ┌─────────────────────────────────────────────────────┐        │  │ │
│  │  │  │  Subject: akidb.uploads.created                     │        │  │ │
│  │  │  │  ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐           │        │  │ │
│  │  │  │  │ msg 1 │ │ msg 2 │ │ msg 3 │ │ msg 4 │  ...      │        │  │ │
│  │  │  │  └───────┘ └───────┘ └───────┘ └───────┘           │        │  │ │
│  │  │  └─────────────────────────────────────────────────────┘        │  │ │
│  │  │                                                                 │  │ │
│  │  │  Subject: akidb.uploads.dlq (Dead Letter Queue)                │  │ │
│  │  │  ┌───────┐                                                      │  │ │
│  │  │  │failed │                                                      │  │ │
│  │  │  └───────┘                                                      │  │ │
│  │  └─────────────────────────────────────────────────────────────────┘  │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                │                                           │
│                    Pull Subscription                                       │
│                                │                                           │
│                                ▼                                           │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                      INGESTION WORKERS                                 │ │
│  │                                                                        │ │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐    │ │
│  │  │    Worker 1      │  │    Worker 2      │  │    Worker 3      │    │ │
│  │  │    (Thor 1)      │  │    (Thor 2)      │  │    (Thor 3)      │    │ │
│  │  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘    │ │
│  │           │                     │                     │              │ │
│  │           └─────────────────────┴─────────────────────┘              │ │
│  │                                 │                                     │ │
│  │  ┌──────────────────────────────┼──────────────────────────────────┐ │ │
│  │  │                              │                                  │ │ │
│  │  │  ┌───────────────────────────▼───────────────────────────────┐ │ │ │
│  │  │  │                  PROCESSING PIPELINE                       │ │ │ │
│  │  │  │                                                            │ │ │ │
│  │  │  │  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐   │ │ │ │
│  │  │  │  │ 1.FETCH │──►│ 2.PARSE │──►│ 3.CHUNK │──►│ 4.EMBED │   │ │ │ │
│  │  │  │  │  MinIO  │   │  Text   │   │  Split  │   │TensorRT │   │ │ │ │
│  │  │  │  └─────────┘   └─────────┘   └─────────┘   └────┬────┘   │ │ │ │
│  │  │  │                                                  │        │ │ │ │
│  │  │  │  ┌─────────┐   ┌─────────┐                      │        │ │ │ │
│  │  │  │  │6.CLEANUP│◄──│5.INSERT │◄─────────────────────┘        │ │ │ │
│  │  │  │  │  MinIO  │   │  AkiDB  │                               │ │ │ │
│  │  │  │  └─────────┘   └─────────┘                               │ │ │ │
│  │  │  │                                                            │ │ │ │
│  │  │  └────────────────────────────────────────────────────────────┘ │ │ │
│  │  └─────────────────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                │                                           │
│                                ▼                                           │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │                          AKIDB CLUSTER                                 │ │
│  │  ┌─────────────┐                                                       │ │
│  │  │ Coordinator │ ────► Shard 0 / Shard 1 / Shard 2                    │ │
│  │  └─────────────┘                                                       │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow

```
1. Client requests upload URL        → Upload Gateway
2. Gateway generates pre-signed URL  → Returns to client
3. Client uploads file directly      → MinIO
4. MinIO emits S3 event             → NATS JetStream
5. Worker pulls message              → NATS subscription
6. Worker downloads file             → MinIO
7. Worker parses document            → Text extraction
8. Worker chunks text                → LangChain splitter
9. Worker generates embeddings       → TensorRT-LLM
10. Worker inserts vectors           → AkiDB gRPC
11. Worker deletes source file       → MinIO cleanup
12. Worker acknowledges message      → NATS
```

---

## 3. Component Details

### 3.1 Upload Gateway

**Purpose:** HTTP API for document uploads with pre-signed URLs.

**Technology Stack:**
- FastAPI 0.109+
- Pydantic 2.5+
- MinIO Python SDK 7.2+
- httpx (async HTTP client)

**Directory Structure:**
```
services/upload-gateway/
├── Dockerfile
├── requirements.txt
├── pyproject.toml
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI application entry
│   ├── config.py            # Pydantic settings
│   ├── dependencies.py      # Dependency injection
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── upload.py        # POST /upload
│   │   ├── status.py        # GET /status/{job_id}
│   │   └── health.py        # GET /health
│   ├── services/
│   │   ├── __init__.py
│   │   ├── minio_service.py # MinIO operations
│   │   ├── presigned.py     # Pre-signed URL generation
│   │   └── job_tracker.py   # Job status tracking
│   ├── models/
│   │   ├── __init__.py
│   │   ├── upload.py        # Upload request/response models
│   │   └── status.py        # Status models
│   └── middleware/
│       ├── __init__.py
│       └── metrics.py       # Prometheus metrics
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_upload.py
    └── test_status.py
```

### 3.2 Ingestion Worker

**Purpose:** Async worker that processes uploaded documents.

**Technology Stack:**
- NATS.py 2.6+ (async NATS client)
- LangChain 0.1+ (text splitting)
- pdfplumber, python-docx, openpyxl, etc. (parsers)
- grpcio (AkiDB client)
- structlog (structured logging)

**Directory Structure:**
```
services/ingestion-worker/
├── Dockerfile
├── requirements.txt
├── pyproject.toml
├── worker/
│   ├── __init__.py
│   ├── main.py              # Entry point
│   ├── config.py            # Pydantic settings
│   ├── consumer.py          # NATS JetStream consumer
│   ├── pipeline.py          # Processing orchestration
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── base.py          # Abstract parser interface
│   │   ├── registry.py      # Parser registry
│   │   ├── pdf.py           # PDF parser
│   │   ├── docx.py          # DOCX parser
│   │   ├── xlsx.py          # XLSX parser
│   │   ├── csv_parser.py    # CSV parser
│   │   ├── html.py          # HTML parser
│   │   ├── xml_parser.py    # XML parser
│   │   ├── json_parser.py   # JSON parser
│   │   └── enl.py           # EndNote parser
│   ├── chunking/
│   │   ├── __init__.py
│   │   ├── splitter.py      # Text chunking
│   │   └── strategies.py    # Chunking strategies
│   ├── embedding/
│   │   ├── __init__.py
│   │   ├── client.py        # Embedding client interface
│   │   └── tensorrt.py      # TensorRT-LLM client
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── minio_client.py  # MinIO operations
│   │   └── akidb_client.py  # AkiDB gRPC client
│   └── proto/
│       └── akidb_pb2.py     # Generated protobuf
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_parsers/
    ├── test_chunking.py
    ├── test_embedding.py
    └── test_pipeline.py
```

---

## 4. Document Parsers

### 4.1 Parser Interface

```python
# worker/parsers/base.py
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

@dataclass
class ParsedPage:
    """Represents a single page/section of a document."""
    page_number: int
    text: str
    tables: Optional[str] = None
    metadata: Dict[str, Any] = None

@dataclass
class ParsedDocument:
    """Represents a fully parsed document."""
    pages: List[ParsedPage]
    total_pages: int
    metadata: Dict[str, Any]
    file_type: str

class BaseParser(ABC):
    """Abstract base class for document parsers."""

    supported_extensions: List[str] = []

    @abstractmethod
    async def parse(self, file_path: str) -> ParsedDocument:
        """Parse a document and return structured text."""
        pass

    @classmethod
    def can_parse(cls, filename: str) -> bool:
        """Check if this parser can handle the file."""
        ext = Path(filename).suffix.lower()
        return ext in cls.supported_extensions
```

### 4.2 Parser Implementations

#### PDF Parser

```python
# worker/parsers/pdf.py
import pdfplumber
from pathlib import Path
from typing import List
from .base import BaseParser, ParsedDocument, ParsedPage

class PDFParser(BaseParser):
    """PDF document parser using pdfplumber."""

    supported_extensions = ['.pdf']

    def __init__(self, extract_tables: bool = True):
        self.extract_tables = extract_tables

    async def parse(self, file_path: str) -> ParsedDocument:
        pages: List[ParsedPage] = []

        with pdfplumber.open(file_path) as pdf:
            for i, page in enumerate(pdf.pages):
                # Extract text
                text = page.extract_text() or ""

                # Extract tables if enabled
                tables_text = ""
                if self.extract_tables:
                    tables = page.extract_tables()
                    tables_text = self._tables_to_markdown(tables)

                pages.append(ParsedPage(
                    page_number=i + 1,
                    text=text,
                    tables=tables_text,
                    metadata={
                        "width": page.width,
                        "height": page.height,
                        "chars": len(text)
                    }
                ))

        return ParsedDocument(
            pages=pages,
            total_pages=len(pages),
            metadata={
                "parser": "pdfplumber",
                "filename": Path(file_path).name
            },
            file_type="pdf"
        )

    def _tables_to_markdown(self, tables: List) -> str:
        """Convert tables to markdown format."""
        if not tables:
            return ""

        result = []
        for table in tables:
            if not table or len(table) < 2:
                continue

            # Header row
            header = " | ".join(str(cell or "") for cell in table[0])
            separator = " | ".join("---" for _ in table[0])

            # Data rows
            rows = []
            for row in table[1:]:
                rows.append(" | ".join(str(cell or "") for cell in row))

            result.append(f"\n{header}\n{separator}\n" + "\n".join(rows))

        return "\n".join(result)
```

#### DOCX Parser

```python
# worker/parsers/docx.py
from docx import Document
from pathlib import Path
from typing import List
from .base import BaseParser, ParsedDocument, ParsedPage

class DOCXParser(BaseParser):
    """Microsoft Word document parser."""

    supported_extensions = ['.docx', '.doc']

    async def parse(self, file_path: str) -> ParsedDocument:
        doc = Document(file_path)
        pages: List[ParsedPage] = []

        # Group paragraphs by approximate pages (rough heuristic)
        current_text = []
        page_num = 1

        for para in doc.paragraphs:
            current_text.append(para.text)

            # Rough page break detection (could be improved)
            if len("\n".join(current_text)) > 3000:
                pages.append(ParsedPage(
                    page_number=page_num,
                    text="\n".join(current_text),
                    metadata={"paragraphs": len(current_text)}
                ))
                current_text = []
                page_num += 1

        # Add remaining text
        if current_text:
            pages.append(ParsedPage(
                page_number=page_num,
                text="\n".join(current_text),
                metadata={"paragraphs": len(current_text)}
            ))

        # Extract tables
        tables_text = self._extract_tables(doc)
        if tables_text:
            pages.append(ParsedPage(
                page_number=page_num + 1,
                text=tables_text,
                tables=tables_text,
                metadata={"type": "tables"}
            ))

        return ParsedDocument(
            pages=pages,
            total_pages=len(pages),
            metadata={
                "parser": "python-docx",
                "filename": Path(file_path).name,
                "core_properties": {
                    "author": doc.core_properties.author,
                    "title": doc.core_properties.title
                }
            },
            file_type="docx"
        )

    def _extract_tables(self, doc: Document) -> str:
        """Extract all tables from document."""
        tables_text = []
        for table in doc.tables:
            rows = []
            for row in table.rows:
                cells = [cell.text for cell in row.cells]
                rows.append(" | ".join(cells))
            if rows:
                tables_text.append("\n".join(rows))
        return "\n\n".join(tables_text)
```

#### XLSX Parser

```python
# worker/parsers/xlsx.py
from openpyxl import load_workbook
from pathlib import Path
from typing import List
from .base import BaseParser, ParsedDocument, ParsedPage

class XLSXParser(BaseParser):
    """Excel spreadsheet parser."""

    supported_extensions = ['.xlsx', '.xls']

    def __init__(self, max_rows: int = 10000):
        self.max_rows = max_rows

    async def parse(self, file_path: str) -> ParsedDocument:
        wb = load_workbook(file_path, read_only=True, data_only=True)
        pages: List[ParsedPage] = []

        for sheet_name in wb.sheetnames:
            sheet = wb[sheet_name]
            rows_text = []

            # Get header row
            header = None
            row_count = 0

            for row in sheet.iter_rows(values_only=True):
                if row_count >= self.max_rows:
                    break

                if header is None:
                    header = [str(cell or "") for cell in row]
                    rows_text.append(" | ".join(header))
                    rows_text.append(" | ".join("---" for _ in header))
                else:
                    row_values = [str(cell or "") for cell in row]
                    rows_text.append(" | ".join(row_values))

                row_count += 1

            if rows_text:
                pages.append(ParsedPage(
                    page_number=wb.sheetnames.index(sheet_name) + 1,
                    text="\n".join(rows_text),
                    tables="\n".join(rows_text),
                    metadata={
                        "sheet_name": sheet_name,
                        "rows": row_count
                    }
                ))

        wb.close()

        return ParsedDocument(
            pages=pages,
            total_pages=len(pages),
            metadata={
                "parser": "openpyxl",
                "filename": Path(file_path).name,
                "sheets": wb.sheetnames
            },
            file_type="xlsx"
        )
```

#### CSV Parser

```python
# worker/parsers/csv_parser.py
import pandas as pd
from pathlib import Path
from typing import List
from .base import BaseParser, ParsedDocument, ParsedPage

class CSVParser(BaseParser):
    """CSV file parser using pandas."""

    supported_extensions = ['.csv']

    def __init__(self, max_rows: int = 50000, chunk_rows: int = 1000):
        self.max_rows = max_rows
        self.chunk_rows = chunk_rows

    async def parse(self, file_path: str) -> ParsedDocument:
        # Read CSV with pandas
        df = pd.read_csv(file_path, nrows=self.max_rows)
        pages: List[ParsedPage] = []

        # Split into chunks for manageable page sizes
        for i in range(0, len(df), self.chunk_rows):
            chunk = df.iloc[i:i + self.chunk_rows]

            # Convert to markdown table
            header = " | ".join(df.columns)
            separator = " | ".join("---" for _ in df.columns)
            rows = []
            for _, row in chunk.iterrows():
                rows.append(" | ".join(str(v) for v in row.values))

            text = f"{header}\n{separator}\n" + "\n".join(rows)

            pages.append(ParsedPage(
                page_number=i // self.chunk_rows + 1,
                text=text,
                tables=text,
                metadata={
                    "rows": len(chunk),
                    "start_row": i
                }
            ))

        return ParsedDocument(
            pages=pages,
            total_pages=len(pages),
            metadata={
                "parser": "pandas",
                "filename": Path(file_path).name,
                "columns": list(df.columns),
                "total_rows": len(df)
            },
            file_type="csv"
        )
```

#### HTML Parser

```python
# worker/parsers/html.py
from bs4 import BeautifulSoup
from pathlib import Path
from typing import List
from .base import BaseParser, ParsedDocument, ParsedPage

class HTMLParser(BaseParser):
    """HTML document parser."""

    supported_extensions = ['.html', '.htm']

    def __init__(self, extract_links: bool = False):
        self.extract_links = extract_links

    async def parse(self, file_path: str) -> ParsedDocument:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        soup = BeautifulSoup(content, 'lxml')

        # Remove script and style elements
        for element in soup(['script', 'style', 'nav', 'footer', 'header']):
            element.decompose()

        # Extract main content
        main_content = soup.find('main') or soup.find('article') or soup.body

        if not main_content:
            return ParsedDocument(
                pages=[],
                total_pages=0,
                metadata={"parser": "beautifulsoup4"},
                file_type="html"
            )

        pages: List[ParsedPage] = []

        # Split by major sections
        sections = main_content.find_all(['section', 'div', 'article'], recursive=False)

        if not sections:
            sections = [main_content]

        for i, section in enumerate(sections):
            text = section.get_text(separator='\n', strip=True)
            if text:
                pages.append(ParsedPage(
                    page_number=i + 1,
                    text=text,
                    metadata={"tag": section.name}
                ))

        # Extract title
        title = soup.find('title')
        title_text = title.string if title else None

        return ParsedDocument(
            pages=pages,
            total_pages=len(pages),
            metadata={
                "parser": "beautifulsoup4",
                "filename": Path(file_path).name,
                "title": title_text
            },
            file_type="html"
        )
```

#### JSON Parser

```python
# worker/parsers/json_parser.py
import json
from pathlib import Path
from typing import List, Any
from .base import BaseParser, ParsedDocument, ParsedPage

class JSONParser(BaseParser):
    """JSON file parser with key-value flattening."""

    supported_extensions = ['.json']

    def __init__(self, max_depth: int = 5):
        self.max_depth = max_depth

    async def parse(self, file_path: str) -> ParsedDocument:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Flatten JSON to text
        flattened = self._flatten(data)
        text = "\n".join(f"{k}: {v}" for k, v in flattened.items())

        # Split into pages if large
        pages: List[ParsedPage] = []
        lines = text.split("\n")
        chunk_size = 100  # lines per page

        for i in range(0, len(lines), chunk_size):
            chunk = lines[i:i + chunk_size]
            pages.append(ParsedPage(
                page_number=i // chunk_size + 1,
                text="\n".join(chunk),
                metadata={"lines": len(chunk)}
            ))

        return ParsedDocument(
            pages=pages,
            total_pages=len(pages),
            metadata={
                "parser": "json",
                "filename": Path(file_path).name,
                "keys": len(flattened)
            },
            file_type="json"
        )

    def _flatten(self, obj: Any, prefix: str = "", depth: int = 0) -> dict:
        """Recursively flatten nested JSON."""
        if depth > self.max_depth:
            return {prefix: str(obj)}

        result = {}

        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{prefix}.{k}" if prefix else k
                result.update(self._flatten(v, new_key, depth + 1))
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                new_key = f"{prefix}[{i}]"
                result.update(self._flatten(item, new_key, depth + 1))
        else:
            result[prefix] = str(obj)

        return result
```

### 4.3 Parser Registry

```python
# worker/parsers/registry.py
from typing import Dict, Type, Optional
from pathlib import Path
from .base import BaseParser
from .pdf import PDFParser
from .docx import DOCXParser
from .xlsx import XLSXParser
from .csv_parser import CSVParser
from .html import HTMLParser
from .xml_parser import XMLParser
from .json_parser import JSONParser

class ParserRegistry:
    """Registry for document parsers."""

    _parsers: Dict[str, Type[BaseParser]] = {}

    @classmethod
    def register(cls, parser_class: Type[BaseParser]):
        """Register a parser for its supported extensions."""
        for ext in parser_class.supported_extensions:
            cls._parsers[ext.lower()] = parser_class

    @classmethod
    def get_parser(cls, filename: str) -> Optional[BaseParser]:
        """Get a parser instance for the given filename."""
        ext = Path(filename).suffix.lower()
        parser_class = cls._parsers.get(ext)
        if parser_class:
            return parser_class()
        return None

    @classmethod
    def supported_extensions(cls) -> list:
        """List all supported file extensions."""
        return list(cls._parsers.keys())

# Register all parsers
ParserRegistry.register(PDFParser)
ParserRegistry.register(DOCXParser)
ParserRegistry.register(XLSXParser)
ParserRegistry.register(CSVParser)
ParserRegistry.register(HTMLParser)
ParserRegistry.register(XMLParser)
ParserRegistry.register(JSONParser)


def get_parser(filename: str) -> BaseParser:
    """Convenience function to get a parser."""
    parser = ParserRegistry.get_parser(filename)
    if not parser:
        raise ValueError(f"Unsupported file type: {filename}")
    return parser
```

---

## 5. Text Chunking

### 5.1 Chunking Strategy

```python
# worker/chunking/splitter.py
from typing import List, Dict, Any
from langchain.text_splitter import RecursiveCharacterTextSplitter
import tiktoken

class DocumentChunker:
    """
    Chunk documents into embedding-sized pieces.

    Uses LangChain's RecursiveCharacterTextSplitter with tiktoken
    for accurate token counting.
    """

    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        min_chunk_size: int = 100,
        model: str = "cl100k_base"
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size
        self.encoding = tiktoken.get_encoding(model)

        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=self._token_counter,
            separators=[
                "\n\n",      # Paragraph breaks
                "\n",        # Line breaks
                ". ",        # Sentences
                "? ",        # Questions
                "! ",        # Exclamations
                "; ",        # Semi-colons
                ", ",        # Commas
                " ",         # Words
                ""           # Characters (fallback)
            ]
        )

    def _token_counter(self, text: str) -> int:
        """Count tokens using tiktoken."""
        return len(self.encoding.encode(text))

    def chunk(
        self,
        text: str,
        metadata: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Split text into chunks with metadata.

        Args:
            text: The text to split
            metadata: Base metadata to include with each chunk

        Returns:
            List of chunk dictionaries with text and metadata
        """
        if not text.strip():
            return []

        # Split text
        chunks = self.splitter.split_text(text)

        # Filter out tiny chunks
        chunks = [c for c in chunks if self._token_counter(c) >= self.min_chunk_size]

        # Build chunk objects
        result = []
        for i, chunk_text in enumerate(chunks):
            chunk_data = {
                "text": chunk_text,
                "chunk_index": i,
                "total_chunks": len(chunks),
                "token_count": self._token_counter(chunk_text),
                "char_count": len(chunk_text),
            }

            # Merge with provided metadata
            if metadata:
                chunk_data.update(metadata)

            result.append(chunk_data)

        return result

    def chunk_document(
        self,
        pages: List[Dict[str, str]],
        document_metadata: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Chunk a multi-page document.

        Args:
            pages: List of page dicts with 'text' and optionally 'page_number'
            document_metadata: Metadata to include with all chunks

        Returns:
            List of chunk dictionaries
        """
        all_chunks = []

        for page in pages:
            page_text = page.get("text", "")
            page_num = page.get("page_number", 0)

            page_metadata = {
                "page_number": page_num,
                **(document_metadata or {})
            }

            chunks = self.chunk(page_text, page_metadata)
            all_chunks.extend(chunks)

        # Re-index chunks across entire document
        for i, chunk in enumerate(all_chunks):
            chunk["document_chunk_index"] = i
            chunk["document_total_chunks"] = len(all_chunks)

        return all_chunks
```

### 5.2 Chunking Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| chunk_size | 512 | Target tokens per chunk |
| chunk_overlap | 50 | Overlap tokens between chunks |
| min_chunk_size | 100 | Minimum tokens (discard smaller) |
| model | cl100k_base | Tokenizer model (GPT-4 compatible) |

---

## 6. Embedding Integration

### 6.1 TensorRT-LLM Client

```python
# worker/embedding/tensorrt.py
from typing import List, Optional
import httpx
from pydantic import BaseModel
import structlog

logger = structlog.get_logger()

class EmbeddingRequest(BaseModel):
    input: List[str]
    model: str

class EmbeddingData(BaseModel):
    embedding: List[float]
    index: int

class EmbeddingResponse(BaseModel):
    data: List[EmbeddingData]
    model: str
    usage: dict

class TensorRTEmbeddingClient:
    """
    Client for TensorRT-LLM embedding service.

    Compatible with OpenAI embeddings API format.
    """

    def __init__(
        self,
        base_url: str,
        model: str = "bge-base-en-v1.5",
        timeout: float = 30.0,
        max_batch_size: int = 32
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.max_batch_size = max_batch_size
        self.client = httpx.AsyncClient(timeout=timeout)

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a batch of texts.

        Automatically batches large requests.
        """
        if not texts:
            return []

        all_embeddings = []

        # Process in batches
        for i in range(0, len(texts), self.max_batch_size):
            batch = texts[i:i + self.max_batch_size]
            embeddings = await self._embed_batch(batch)
            all_embeddings.extend(embeddings)

        return all_embeddings

    async def _embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed a single batch."""
        try:
            response = await self.client.post(
                f"{self.base_url}/v1/embeddings",
                json={
                    "input": texts,
                    "model": self.model
                }
            )
            response.raise_for_status()

            data = response.json()
            # Sort by index to maintain order
            sorted_data = sorted(data["data"], key=lambda x: x["index"])
            return [item["embedding"] for item in sorted_data]

        except httpx.HTTPError as e:
            logger.error("Embedding request failed", error=str(e))
            raise

    async def embed_single(self, text: str) -> List[float]:
        """Embed a single text."""
        results = await self.embed([text])
        return results[0]

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
```

### 6.2 Embedding Model Specifications

| Model | Dimensions | Max Tokens | Latency (P95) |
|-------|------------|------------|---------------|
| BGE-base-en-v1.5 | 768 | 512 | < 10ms |
| E5-base-v2 | 768 | 512 | < 10ms |
| BGE-large-en-v1.5 | 1024 | 512 | < 15ms |

---

## 7. Error Handling

### 7.1 Error Categories

| Category | Examples | Handling |
|----------|----------|----------|
| **Transient** | Network timeout, TensorRT busy | Retry with backoff |
| **Permanent** | Invalid file format, corrupt file | Move to DLQ |
| **Resource** | OOM, disk full | Alert + pause worker |

### 7.2 Retry Policy

```python
# worker/retry.py
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
import httpx

# Retry configuration
RETRY_CONFIG = {
    "stop": stop_after_attempt(3),
    "wait": wait_exponential(multiplier=1, min=1, max=30),
    "retry": retry_if_exception_type((
        httpx.TimeoutException,
        httpx.NetworkError,
        ConnectionError
    ))
}

@retry(**RETRY_CONFIG)
async def embed_with_retry(client, texts):
    return await client.embed(texts)
```

### 7.3 Dead Letter Queue

```python
# worker/dlq.py
import nats
from nats.js.api import ConsumerConfig
import json
import structlog

logger = structlog.get_logger()

class DeadLetterQueue:
    """Handle failed messages."""

    def __init__(self, js: nats.js.JetStreamContext, subject: str = "akidb.uploads.dlq"):
        self.js = js
        self.subject = subject

    async def send(self, original_message: dict, error: str, attempts: int):
        """Send a failed message to the DLQ."""
        dlq_message = {
            "original": original_message,
            "error": str(error),
            "attempts": attempts,
            "failed_at": datetime.utcnow().isoformat()
        }

        await self.js.publish(
            self.subject,
            json.dumps(dlq_message).encode()
        )

        logger.warning(
            "Message sent to DLQ",
            subject=self.subject,
            error=error
        )
```

---

## 8. Observability

### 8.1 Metrics

```python
# worker/metrics.py
from prometheus_client import Counter, Histogram, Gauge

# Counters
DOCUMENTS_PROCESSED = Counter(
    'akidb_ingestion_documents_total',
    'Total documents processed',
    ['file_type', 'status']
)

CHUNKS_GENERATED = Counter(
    'akidb_ingestion_chunks_total',
    'Total chunks generated',
    ['file_type']
)

VECTORS_INSERTED = Counter(
    'akidb_ingestion_vectors_total',
    'Total vectors inserted'
)

# Histograms
PARSE_DURATION = Histogram(
    'akidb_ingestion_parse_seconds',
    'Document parsing duration',
    ['file_type'],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60]
)

EMBED_DURATION = Histogram(
    'akidb_ingestion_embed_seconds',
    'Embedding generation duration',
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 2, 5]
)

PIPELINE_DURATION = Histogram(
    'akidb_ingestion_pipeline_seconds',
    'Total pipeline duration',
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800]
)

# Gauges
QUEUE_DEPTH = Gauge(
    'akidb_ingestion_queue_depth',
    'Current queue depth'
)

WORKER_ACTIVE = Gauge(
    'akidb_ingestion_worker_active',
    'Number of active workers'
)
```

### 8.2 Structured Logging

```python
# worker/logging.py
import structlog

def configure_logging(log_level: str = "INFO"):
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True
    )
```

### 8.3 Tracing

```python
# worker/tracing.py
from opentelemetry import trace
from opentelemetry.trace import SpanKind

tracer = trace.get_tracer("akidb.ingestion")

async def process_with_tracing(event: dict):
    with tracer.start_as_current_span(
        "process_document",
        kind=SpanKind.CONSUMER,
        attributes={
            "document.key": event["key"],
            "document.bucket": event["bucket"]
        }
    ) as span:
        try:
            result = await pipeline.process(event)
            span.set_attribute("chunks", result["chunks"])
            span.set_attribute("vectors", result["vectors"])
        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.StatusCode.ERROR)
            raise
```

---

## 9. Deployment

### 9.1 Dockerfile (Upload Gateway)

```dockerfile
# services/upload-gateway/Dockerfile
FROM python:3.11-slim-bookworm

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app/ app/

# Create non-root user
RUN useradd -r -u 1000 appuser
USER appuser

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### 9.2 Dockerfile (Ingestion Worker)

```dockerfile
# services/ingestion-worker/Dockerfile
FROM python:3.11-slim-bookworm

WORKDIR /app

# Install system dependencies for document parsing
RUN apt-get update && apt-get install -y \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY worker/ worker/

# Create non-root user
RUN useradd -r -u 1000 appuser
RUN mkdir -p /tmp/ingestion && chown appuser:appuser /tmp/ingestion
USER appuser

CMD ["python", "-m", "worker.main"]
```

### 9.3 NATS Configuration

```hcl
# /etc/nats/nats.conf
server_name: akidb-nats

# Client connections
port: 4222

# HTTP monitoring
http_port: 8222

# JetStream
jetstream {
  store_dir: /data/jetstream
  max_memory_store: 256MB
  max_file_store: 10GB
}

# Logging
debug: false
trace: false
logtime: true
```

### 9.4 NATS Stream Configuration

```python
# Setup script for NATS streams
import asyncio
import nats
from nats.js.api import StreamConfig, RetentionPolicy, AckPolicy

async def setup_streams():
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Main ingestion stream
    await js.add_stream(
        StreamConfig(
            name="AKIDB_INGEST",
            subjects=["akidb.uploads.*"],
            retention=RetentionPolicy.WORK_QUEUE,
            max_age=86400 * 1e9,  # 24 hours in nanoseconds
            max_msgs=-1,
            max_bytes=-1,
            storage="file",
            num_replicas=1
        )
    )

    # Dead letter queue stream
    await js.add_stream(
        StreamConfig(
            name="AKIDB_INGEST_DLQ",
            subjects=["akidb.uploads.dlq"],
            retention=RetentionPolicy.LIMITS,
            max_age=604800 * 1e9,  # 7 days
            storage="file"
        )
    )

    await nc.close()

if __name__ == "__main__":
    asyncio.run(setup_streams())
```

---

## 10. API Reference

### 10.1 Upload Gateway API

#### POST /upload

Request a pre-signed URL for file upload.

**Request:**
```json
{
  "filename": "report.pdf",
  "content_type": "application/pdf",
  "metadata": {
    "user_id": "user123",
    "project": "research"
  }
}
```

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "upload_url": "https://minio:9000/uploads/...",
  "expires_at": "2026-01-21T11:00:00Z",
  "max_size_bytes": 104857600
}
```

#### GET /status/{job_id}

Get the status of an ingestion job.

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "filename": "report.pdf",
  "chunks": 42,
  "vectors": 42,
  "started_at": "2026-01-21T10:30:00Z",
  "completed_at": "2026-01-21T10:31:15Z",
  "error": null
}
```

**Status Values:**
- `pending` - Upload not yet received
- `uploaded` - File uploaded, awaiting processing
- `processing` - Currently being processed
- `completed` - Successfully ingested
- `failed` - Processing failed (see error field)

#### GET /health

Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "minio": "connected",
  "version": "1.0.0"
}
```

---

## Appendix A: Dependencies

### Upload Gateway

```
# requirements.txt
fastapi==0.109.0
uvicorn[standard]==0.27.0
minio==7.2.3
pydantic==2.5.3
pydantic-settings==2.1.0
python-multipart==0.0.6
httpx==0.26.0
prometheus-client==0.19.0
structlog==24.1.0
```

### Ingestion Worker

```
# requirements.txt
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

# Retry/resilience
tenacity==8.2.3

# Observability
prometheus-client==0.19.0
structlog==24.1.0
opentelemetry-api==1.22.0
opentelemetry-sdk==1.22.0

# Core
pydantic==2.5.3
pydantic-settings==2.1.0
```

---

*End of Ingestion Service Design v1.0*
