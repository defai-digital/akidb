"""Tests for the document parser API."""

import base64

from fastapi.testclient import TestClient

from parser.api import app
from parser.models import DocumentFormat
from parser.parsers.base import detect_format

client = TestClient(app)


def test_health_check():
    """Test health endpoint returns healthy status."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "parsers" in data


def test_list_formats():
    """Test formats endpoint returns supported formats."""
    response = client.get("/formats")
    assert response.status_code == 200
    data = response.json()
    assert "formats" in data
    assert len(data["formats"]) >= 2


def test_detect_format_pdf():
    """Test PDF format detection."""
    assert detect_format("document.pdf") == DocumentFormat.PDF
    assert detect_format("file.PDF") == DocumentFormat.PDF


def test_detect_format_docx():
    """Test DOCX format detection."""
    assert detect_format("document.docx") == DocumentFormat.DOCX
    assert detect_format("file.DOCX") == DocumentFormat.DOCX
    assert detect_format("legacy.doc") == DocumentFormat.UNKNOWN


def test_detect_format_accepts_paths_and_url_suffixes():
    """Test format detection with paths, query strings, and fragments."""
    assert detect_format("contracts/2025/HGC.CONTRACT.PDF") == DocumentFormat.PDF
    assert (
        detect_format("https://example.test/docs/report.docx?download=1")
        == DocumentFormat.DOCX
    )
    assert detect_format(r"C:\\docs\\library.enlx#record") == DocumentFormat.ENL


def test_detect_format_unknown():
    """Test unknown format detection."""
    assert detect_format("file.xyz") == DocumentFormat.UNKNOWN
    assert detect_format("noextension") == DocumentFormat.UNKNOWN


def test_parse_invalid_base64():
    """Test parsing with invalid base64 content."""
    response = client.post(
        "/parse",
        json={
            "content_base64": "not-valid-base64!!!",
            "filename": "test.pdf",
        },
    )
    assert response.status_code == 400


def test_parse_unsupported_format():
    """Test parsing with unsupported format."""
    response = client.post(
        "/parse",
        json={
            "content_base64": base64.b64encode(b"test content").decode(),
            "filename": "test.xyz",
        },
    )
    assert response.status_code == 400


def test_metrics_endpoint():
    """Test metrics endpoint returns Prometheus format."""
    response = client.get("/metrics")
    assert response.status_code == 200
    assert b"doc_parser" in response.content
