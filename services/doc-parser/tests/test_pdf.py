"""Tests for the PDF parser."""

import sys
from types import SimpleNamespace

from parser.parsers.pdf import PdfParser


class FakePage:
    images = []

    def extract_text(self):
        return None

    def extract_tables(self):
        return [[["customer", "year", "contract_amount"], ["HGC", "2025", "1200"]]]


class FakePdf:
    pages = [FakePage()]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class FakePdfReader:
    metadata = {}
    pages = [object()]

    def __init__(self, _stream):
        pass


def test_pdf_table_content_is_included_in_retrieval_text(monkeypatch):
    monkeypatch.setitem(
        sys.modules,
        "pdfplumber",
        SimpleNamespace(open=lambda _stream: FakePdf()),
    )
    monkeypatch.setitem(sys.modules, "pypdf", SimpleNamespace(PdfReader=FakePdfReader))

    result = PdfParser().parse(b"fake pdf bytes", "contract.pdf")

    assert "customer HGC" in result.text
    assert "year 2025" in result.text
    assert "contract_amount 1200" in result.text
    assert result.tables[0].headers == ["customer", "year", "contract_amount"]


def test_pdf_is_available_accepts_loaded_modules(monkeypatch):
    monkeypatch.setitem(sys.modules, "pdfplumber", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "pypdf", SimpleNamespace())

    assert PdfParser().is_available()
