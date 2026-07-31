"""FastAPI application for upload gateway service."""

import hashlib
import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import structlog
from fastapi import FastAPI, File, HTTPException, Response, UploadFile, status
from prometheus_client import Counter, Histogram, generate_latest
from starlette.concurrency import run_in_threadpool

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


def safe_filename(filename: str) -> str:
    """Return a client filename without path components."""
    name = filename.replace("\\", "/").rsplit("/", 1)[-1].strip()
    if not name or name in {".", ".."}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Filename is invalid",
        )
    return name


def safe_prefix(prefix: str) -> str:
    """Normalize an object prefix and reject traversal-like segments."""
    normalized = prefix.replace("\\", "/").strip("/")
    if not normalized:
        return ""
    parts = normalized.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Prefix contains an invalid path segment",
        )
    return "/".join(parts)


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
    await run_in_threadpool(storage.ensure_bucket)

    # Initialize NATS
    await get_event_publisher()

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
async def health_check(response: Response) -> HealthResponse:
    """Health check endpoint."""
    storage = get_storage_client()
    publisher = await get_event_publisher()
    minio_connected = await run_in_threadpool(storage.is_connected)
    nats_connected = publisher.is_connected()
    healthy = minio_connected and nats_connected
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(
        status="healthy" if healthy else "degraded",
        version=__version__,
        minio_connected=minio_connected,
        nats_connected=nats_connected,
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
    info = await run_in_threadpool(storage.get_bucket_info)
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
    filename = safe_filename(file.filename)
    object_prefix = safe_prefix(prefix)

    # Check extension
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in settings.allowed_extensions_list:
        UPLOAD_REQUESTS.labels(status="error", extension=ext).inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File extension '{ext}' not allowed. Allowed: {settings.allowed_extensions}",
        )

    # Read at most one byte beyond the configured limit so oversized uploads
    # cannot force an unbounded in-memory allocation before validation.
    max_size_bytes = settings.max_file_size_mb * 1024 * 1024
    content = await file.read(max_size_bytes + 1)
    if not content:
        UPLOAD_REQUESTS.labels(status="error", extension=ext).inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File is empty",
        )
    # Check size
    if len(content) > max_size_bytes:
        UPLOAD_REQUESTS.labels(status="error", extension=ext).inc()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File exceeds the {settings.max_file_size_mb}MB limit",
        )

    UPLOAD_SIZE.labels(extension=ext).observe(len(content))

    # Generate unique key
    content_hash = hashlib.sha256(content).hexdigest()[:16]
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]

    if object_prefix:
        key = f"{object_prefix}/{timestamp}_{content_hash}_{unique_id}_{filename}"
    else:
        key = f"{timestamp}_{content_hash}_{unique_id}_{filename}"

    # Upload to MinIO
    storage = get_storage_client()
    try:
        await run_in_threadpool(
            storage.upload,
            key=key,
            data=content,
            content_type=file.content_type,
            metadata={
                "original_filename": filename,
                "upload_timestamp": datetime.now(UTC).isoformat(),
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
            "original_filename": filename,
            "extension": ext,
        },
    )
    event_published = await publisher.publish(event)

    logger.info(
        "document_uploaded",
        key=key,
        size=len(content),
        extension=ext,
        event_published=event_published,
    )
    if not event_published:
        UPLOAD_REQUESTS.labels(status="error", extension=ext).inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Event publication failed after object storage",
                "bucket": settings.minio_bucket,
                "key": key,
            },
        )

    UPLOAD_REQUESTS.labels(status="success", extension=ext).inc()
    UPLOAD_LATENCY.labels(extension=ext).observe(time.time() - start_time)

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
