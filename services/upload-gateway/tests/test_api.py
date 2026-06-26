"""Tests for the upload gateway API."""

import pytest

from gateway.config import settings


def test_allowed_extensions_list():
    """Test that allowed extensions are parsed correctly."""
    extensions = settings.allowed_extensions_list
    assert "pdf" in extensions
    assert "docx" in extensions
    assert "csv" in extensions


def test_settings_defaults():
    """Test that settings have sensible defaults."""
    assert settings.port == 8081
    assert settings.max_file_size_mb > 0
    assert settings.minio_bucket == "akidb-documents"
