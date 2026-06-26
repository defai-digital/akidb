"""FastAPI application for document parsing service."""

import base64
import time
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, HTTPException, status
from prometheus_client import Counter, Histogram, generate_latest

from parser import __version__
from parser.config import settings
from parser.models import (
    DocumentFormat,
    HealthResponse,
    ParsedDocument,
    ParseError,
    ParseRequest,
)
from parser.parsers.base import detect_format, get_parser
from parser.parsers.pdf import PdfParser
from parser.parsers.docx import DocxParser
from parser.parsers.enl import EnlParser

logger = structlog.get_logger()

# Prometheus metrics
PARSE_REQUESTS = Counter(
    "doc_parser_requests_total",
    "Total parse requests",
    ["format", "status"],
)
PARSE_LATENCY = Histogram(
    "doc_parser_latency_seconds",
    "Parse latency in seconds",
    ["format"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
)
PARSE_SIZE = Histogram(
    "doc_parser_size_bytes",
    "Document size in bytes",
    ["format"],
    buckets=[1e4, 1e5, 1e6, 5e6, 1e7, 5e7, 1e8],
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger.info(
        "doc_parser_starting",
        version=__version__,
        host=settings.host,
        port=settings.port,
    )
    yield
    logger.info("doc_parser_shutdown")


app = FastAPI(
    title="AkiDB Document Parser",
    description="Document parsing service for complex formats (PDF, DOCX)",
    version=__version__,
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Health check endpoint."""
    pdf_parser = PdfParser()
    docx_parser = DocxParser()
    enl_parser = EnlParser()

    return HealthResponse(
        status="healthy",
        version=__version__,
        parsers={
            "pdf": pdf_parser.is_available(),
            "docx": docx_parser.is_available(),
            "enl": enl_parser.is_available(),
        },
    )


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    from fastapi.responses import Response

    return Response(content=generate_latest(), media_type="text/plain")


@app.post("/parse", response_model=ParsedDocument, responses={400: {"model": ParseError}})
async def parse_document(request: ParseRequest) -> ParsedDocument:
    """Parse a document and extract text content.

    The document content should be base64-encoded.
    Format is auto-detected from filename if not provided.
    """
    start_time = time.time()

    # Detect format
    doc_format = request.format or detect_format(request.filename)
    if doc_format == DocumentFormat.UNKNOWN:
        PARSE_REQUESTS.labels(format="unknown", status="error").inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported document format: {request.filename}",
        )

    # Decode content
    try:
        content = base64.b64decode(request.content_base64)
    except Exception as e:
        PARSE_REQUESTS.labels(format=doc_format.value, status="error").inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid base64 content: {e}",
        )

    # Check size
    size_mb = len(content) / (1024 * 1024)
    if size_mb > settings.max_file_size_mb:
        PARSE_REQUESTS.labels(format=doc_format.value, status="error").inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Document too large: {size_mb:.1f}MB (max: {settings.max_file_size_mb}MB)",
        )

    PARSE_SIZE.labels(format=doc_format.value).observe(len(content))

    # Get parser
    parser = get_parser(doc_format)
    if parser is None:
        PARSE_REQUESTS.labels(format=doc_format.value, status="error").inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"No parser available for format: {doc_format.value}",
        )

    if not parser.is_available():
        PARSE_REQUESTS.labels(format=doc_format.value, status="error").inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Parser for {doc_format.value} is not available (missing dependencies)",
        )

    # Parse document
    try:
        result = parser.parse(content, request.filename)
        PARSE_REQUESTS.labels(format=doc_format.value, status="success").inc()
        PARSE_LATENCY.labels(format=doc_format.value).observe(time.time() - start_time)
        return result
    except Exception as e:
        logger.error(
            "parse_failed",
            format=doc_format.value,
            filename=request.filename,
            error=str(e),
        )
        PARSE_REQUESTS.labels(format=doc_format.value, status="error").inc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Parse failed: {e}",
        )


@app.get("/formats")
async def list_formats() -> dict:
    """List supported document formats and their availability."""
    pdf_parser = PdfParser()
    docx_parser = DocxParser()
    enl_parser = EnlParser()

    return {
        "formats": [
            {
                "format": "pdf",
                "extensions": [".pdf"],
                "available": pdf_parser.is_available(),
            },
            {
                "format": "docx",
                "extensions": [".docx", ".doc"],
                "available": docx_parser.is_available(),
            },
            {
                "format": "enl",
                "extensions": [".enl", ".enlx", ".enlp"],
                "available": enl_parser.is_available(),
                "description": "EndNote library files",
            },
        ]
    }
