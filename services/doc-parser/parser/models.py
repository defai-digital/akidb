"""Data models for the document parser service."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class DocumentFormat(str, Enum):
    """Supported document formats."""

    PDF = "pdf"
    DOCX = "docx"
    DOC = "doc"
    PPTX = "pptx"
    ENL = "enl"  # EndNote
    UNKNOWN = "unknown"


class ParseRequest(BaseModel):
    """Request to parse a document."""

    content_base64: str = Field(..., description="Base64-encoded document content")
    filename: str = Field(..., description="Original filename for format detection")
    format: DocumentFormat | None = Field(
        None,
        description="Document format (auto-detected if not provided)",
    )


class TableData(BaseModel):
    """Extracted table data."""

    headers: list[str] = Field(default_factory=list)
    rows: list[list[str]] = Field(default_factory=list)
    page: int | None = None


class ImageRef(BaseModel):
    """Reference to an extracted image."""

    index: int
    page: int | None = None
    width: int | None = None
    height: int | None = None
    alt_text: str | None = None


class ParsedDocument(BaseModel):
    """Result of document parsing."""

    text: str = Field(..., description="Extracted plain text")
    format: DocumentFormat
    page_count: int = Field(0, description="Number of pages")
    metadata: dict[str, Any] = Field(default_factory=dict)
    tables: list[TableData] = Field(default_factory=list)
    images: list[ImageRef] = Field(default_factory=list)
    parse_time_ms: float = Field(0.0, description="Time taken to parse in milliseconds")


class ParseError(BaseModel):
    """Error response from parsing."""

    error: str
    error_type: str
    details: dict[str, Any] | None = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str
    parsers: dict[str, bool]
