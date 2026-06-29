"""Tests for the EndNote library parser."""

import base64
import io
import zipfile

import pytest
from fastapi.testclient import TestClient

from parser.api import app
from parser.models import DocumentFormat
from parser.parsers.base import detect_format
from parser.parsers.enl import EnlParser

client = TestClient(app)


# Sample EndNote XML content
SAMPLE_ENL_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<xml>
    <records>
        <record>
            <ref-type>1</ref-type>
            <contributors>
                <author>Smith, John</author>
                <author>Doe, Jane</author>
            </contributors>
            <title>Machine Learning for Vector Databases</title>
            <year>2025</year>
            <journal>Journal of Database Systems</journal>
            <volume>42</volume>
            <issue>3</issue>
            <pages>123-145</pages>
            <doi>10.1234/jds.2025.001</doi>
            <abstract>This paper presents a novel approach to machine learning for vector
            databases, focusing on efficient similarity search algorithms.</abstract>
            <keywords>machine learning; vector database; similarity search</keywords>
        </record>
        <record>
            <ref-type>2</ref-type>
            <contributors>
                <author>Johnson, Robert</author>
            </contributors>
            <title>Introduction to Neural Networks</title>
            <year>2024</year>
            <publisher>Academic Press</publisher>
            <isbn>978-1234567890</isbn>
        </record>
    </records>
</xml>
"""

SAMPLE_RIS_FORMAT = b"""TY  - JOUR
AU  - Smith, John
AU  - Doe, Jane
TI  - Machine Learning for Vector Databases
PY  - 2025
JO  - Journal of Database Systems
VL  - 42
IS  - 3
SP  - 123
EP  - 145
DO  - 10.1234/jds.2025.001
AB  - This paper presents a novel approach to machine learning for vector databases.
ER  -

TY  - BOOK
AU  - Johnson, Robert
TI  - Introduction to Neural Networks
PY  - 2024
PB  - Academic Press
ER  -
"""


def test_detect_format_enl():
    """Test ENL format detection."""
    assert detect_format("library.enl") == DocumentFormat.ENL
    assert detect_format("references.ENL") == DocumentFormat.ENL


def test_detect_format_enlx():
    """Test ENLX format detection."""
    assert detect_format("library.enlx") == DocumentFormat.ENL
    assert detect_format("references.ENLX") == DocumentFormat.ENL


def test_detect_format_enlp():
    """Test ENLP format detection."""
    assert detect_format("library.enlp") == DocumentFormat.ENL
    assert detect_format("references.ENLP") == DocumentFormat.ENL


def test_enl_parser_is_available():
    """Test that ENL parser is always available (uses stdlib)."""
    parser = EnlParser()
    assert parser.is_available() is True


def test_enl_parser_format():
    """Test ENL parser returns correct format."""
    parser = EnlParser()
    assert parser.format == DocumentFormat.ENL


def test_parse_enl_xml():
    """Test parsing EndNote XML format."""
    parser = EnlParser()
    result = parser.parse(SAMPLE_ENL_XML, "library.enl")

    assert result.format == DocumentFormat.ENL
    assert result.metadata["reference_count"] == 2

    # Check that references were extracted
    assert "Machine Learning for Vector Databases" in result.text
    assert "Introduction to Neural Networks" in result.text
    assert "Smith, John" in result.text
    assert "2025" in result.metadata["years"]
    assert "2024" in result.metadata["years"]


def test_parse_enl_ris_fallback():
    """Test parsing RIS-style EndNote format."""
    parser = EnlParser()
    result = parser.parse(SAMPLE_RIS_FORMAT, "library.enl")

    assert result.format == DocumentFormat.ENL
    # RIS format should be parsed by fallback
    assert result.metadata["reference_count"] >= 1


def test_parse_enlx_archive():
    """Test parsing ENLX (ZIP archive) format."""
    # Create a mock ENLX file (ZIP containing ENL)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("library.enl", SAMPLE_ENL_XML)

    parser = EnlParser()
    result = parser.parse(buffer.getvalue(), "library.enlx")

    assert result.format == DocumentFormat.ENL
    assert result.metadata["reference_count"] == 2
    assert "Machine Learning for Vector Databases" in result.text


def test_parse_enl_via_api():
    """Test parsing ENL via the API endpoint."""
    content_b64 = base64.b64encode(SAMPLE_ENL_XML).decode()

    response = client.post(
        "/parse",
        json={
            "content_base64": content_b64,
            "filename": "library.enl",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["format"] == "enl"
    assert data["metadata"]["reference_count"] == 2


def test_parse_enlx_via_api():
    """Test parsing ENLX via the API endpoint."""
    # Create a mock ENLX file
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("library.enl", SAMPLE_ENL_XML)

    content_b64 = base64.b64encode(buffer.getvalue()).decode()

    response = client.post(
        "/parse",
        json={
            "content_base64": content_b64,
            "filename": "library.enlx",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["format"] == "enl"


def test_enl_metadata_extraction():
    """Test that metadata is properly extracted from references."""
    parser = EnlParser()
    result = parser.parse(SAMPLE_ENL_XML, "library.enl")

    # Check metadata aggregation
    assert "Journal of Database Systems" in result.metadata["journals"]
    assert "Journal Article" in result.metadata["types"]
    assert "Book" in result.metadata["types"]


def test_enl_empty_library():
    """Test parsing an empty EndNote library."""
    empty_xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <xml>
        <records>
        </records>
    </xml>
    """
    parser = EnlParser()
    result = parser.parse(empty_xml, "empty.enl")

    assert result.format == DocumentFormat.ENL
    assert result.metadata["reference_count"] == 0
    assert result.text == ""


def test_enl_invalid_archive():
    """Test handling of invalid ENLX archive."""
    parser = EnlParser()

    with pytest.raises(Exception):
        parser.parse(b"not a valid zip file", "invalid.enlx")


def test_enl_health_check_includes_parser():
    """Test that health check includes ENL parser status."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "enl" in data["parsers"] or "ENL" in str(data["parsers"])
