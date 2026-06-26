"""FastAPI application for upload gateway service."""

import hashlib
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

import structlog
from fastapi import FastAPI, File, HTTPException, UploadFile, status
from prometheus_client import Counter, Histogram, generate_latest

from gateway import __version__
from gateway.config import settings
from gateway.events import close_event_publisher, get_event_publisher
from gateway.models import (
    BucketInfo,
    HealthResponse,
    UploadEvent,
    UploadResponse,
)
from gateway.storage import get_storage_client

logger = structlog.get_logger()

# Prometheus metrics
UPLOAD_REQUESTS = Counter(
    "upload_gateway_requests_total",
    "Total upload requests",
    ["status", "extension"],
)
UPLOAD_LATENCY = Histogram(
    "upload_gateway_latency_seconds",
    "Upload latency in seconds",
    ["extension"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)
UPLOAD_SIZE = Histogram(
    "upload_gateway_size_bytes",
    "Upload size in bytes",
    ["extension"],
    buckets=[1e4, 1e5, 1e6, 5e6, 1e7, 5e7, 1e8],
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger.info(
        "upload_gateway_starting",
        version=__version__,
        host=settings.host,
        port=settings.port,
    )

    # Initialize storage
    storage = get_storage_client()
    storage.ensure_bucket()

    # Initialize NATS
    publisher = await get_event_publisher()

    yield

    # Cleanup
    await close_event_publisher()
    logger.info("upload_gateway_shutdown")


app = FastAPI(
    title="AkiDB Upload Gateway",
    description="Upload gateway for document ingestion",
    version=__version__,
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Health check endpoint."""
    storage = get_storage_client()
    publisher = await get_event_publisher()

    return HealthResponse(
        status="healthy",
        version=__version__,
        minio_connected=storage.is_connected(),
        nats_connected=publisher.is_connected(),
    )


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    from fastapi.responses import Response

    return Response(content=generate_latest(), media_type="text/plain")


@app.get("/bucket", response_model=BucketInfo)
async def get_bucket_info() -> BucketInfo:
    """Get information about the upload bucket."""
    storage = get_storage_client()
    info = storage.get_bucket_info()
    return BucketInfo(**info)


@app.post("/upload", response_model=UploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    prefix: str = "",
) -> UploadResponse:
    """Upload a document for processing.

    The document will be stored in MinIO and an event will be published
    to NATS for the ingestion pipeline to process.

    Args:
        file: The document file to upload
        prefix: Optional path prefix for the object key
    """
    start_time = time.time()

    # Validate file
    if not file.filename:
        UPLOAD_REQUESTS.labels(status="error", extension="unknown").inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Filename is required",
        )

    # Check extension
    ext = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else ""
    if ext not in settings.allowed_extensions_list:
        UPLOAD_REQUESTS.labels(status="error", extension=ext).inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File extension '{ext}' not allowed. Allowed: {settings.allowed_extensions}",
        )

    # Read file content
    content = await file.read()
    size_mb = len(content) / (1024 * 1024)

    # Check size
    if size_mb > settings.max_file_size_mb:
        UPLOAD_REQUESTS.labels(status="error", extension=ext).inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File too large: {size_mb:.1f}MB (max: {settings.max_file_size_mb}MB)",
        )

    UPLOAD_SIZE.labels(extension=ext).observe(len(content))

    # Generate unique key
    content_hash = hashlib.sha256(content).hexdigest()[:16]
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]

    if prefix:
        key = f"{prefix.strip('/')}/{timestamp}_{content_hash}_{unique_id}_{file.filename}"
    else:
        key = f"{timestamp}_{content_hash}_{unique_id}_{file.filename}"

    # Upload to MinIO
    storage = get_storage_client()
    try:
        storage.upload(
            key=key,
            data=content,
            content_type=file.content_type,
            metadata={
                "original_filename": file.filename,
                "upload_timestamp": datetime.utcnow().isoformat(),
            },
        )
    except Exception as e:
        UPLOAD_REQUESTS.labels(status="error", extension=ext).inc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Storage upload failed: {e}",
        )

    # Publish event to NATS
    publisher = await get_event_publisher()
    event = UploadEvent(
        bucket=settings.minio_bucket,
        key=key,
        size=len(content),
        content_type=file.content_type,
        metadata={
            "original_filename": file.filename,
            "extension": ext,
        },
    )
    event_published = await publisher.publish(event)

    UPLOAD_REQUESTS.labels(status="success", extension=ext).inc()
    UPLOAD_LATENCY.labels(extension=ext).observe(time.time() - start_time)

    logger.info(
        "document_uploaded",
        key=key,
        size=len(content),
        extension=ext,
        event_published=event_published,
    )

    return UploadResponse(
        key=key,
        bucket=settings.minio_bucket,
        size=len(content),
        content_type=file.content_type,
        event_published=event_published,
    )


@app.get("/extensions")
async def list_extensions() -> dict:
    """List allowed file extensions."""
    return {
        "allowed_extensions": settings.allowed_extensions_list,
        "max_file_size_mb": settings.max_file_size_mb,
    }
