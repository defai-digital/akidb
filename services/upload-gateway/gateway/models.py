"""Data models for the upload gateway service."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class UploadEvent(BaseModel):
    """Event published to NATS when a document is uploaded."""

    bucket: str = Field(..., description="MinIO bucket name")
    key: str = Field(..., description="Object key (file path)")
    size: int = Field(..., description="File size in bytes")
    content_type: str | None = Field(None, description="MIME content type")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    metadata: dict[str, Any] = Field(default_factory=dict)


class UploadResponse(BaseModel):
    """Response after successful upload."""

    key: str
    bucket: str
    size: int
    content_type: str | None
    event_published: bool


class UploadError(BaseModel):
    """Error response from upload."""

    error: str
    details: dict[str, Any] | None = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str
    minio_connected: bool
    nats_connected: bool


class BucketInfo(BaseModel):
    """Information about the upload bucket."""

    name: str
    exists: bool
    object_count: int | None = None
