"""Document parsers."""

from parser.parsers.base import BaseParser, get_parser
from parser.parsers.docx import DocxParser
from parser.parsers.enl import EnlParser
from parser.parsers.pdf import PdfParser

__all__ = ["BaseParser", "PdfParser", "DocxParser", "EnlParser", "get_parser"]
