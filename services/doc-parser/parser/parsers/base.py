"""Base parser interface and factory."""

from abc import ABC, abstractmethod

from parser.models import DocumentFormat, ParsedDocument


class BaseParser(ABC):
    """Abstract base class for document parsers."""

    @property
    @abstractmethod
    def format(self) -> DocumentFormat:
        """Return the document format this parser handles."""
        ...

    @abstractmethod
    def parse(self, content: bytes, filename: str) -> ParsedDocument:
        """Parse document content and return extracted text and metadata."""
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """Check if the parser's dependencies are available."""
        ...


def get_parser(format: DocumentFormat) -> BaseParser | None:
    """Get the appropriate parser for a document format."""
    from parser.parsers.docx import DocxParser
    from parser.parsers.enl import EnlParser
    from parser.parsers.pdf import PdfParser

    parsers: dict[DocumentFormat, type[BaseParser]] = {
        DocumentFormat.PDF: PdfParser,
        DocumentFormat.DOCX: DocxParser,
        DocumentFormat.DOC: DocxParser,  # Try docx parser for .doc
        DocumentFormat.ENL: EnlParser,
    }

    parser_class = parsers.get(format)
    if parser_class is None:
        return None

    return parser_class()


def detect_format(filename: str) -> DocumentFormat:
    """Detect document format from filename."""
    normalized = (
        filename.strip()
        .split("?", 1)[0]
        .split("#", 1)[0]
        .rstrip("/\\")
        .replace("\\", "/")
    )
    basename = normalized.rsplit("/", 1)[-1]
    ext = basename.rsplit(".", 1)[-1].lower() if "." in basename else ""

    format_map = {
        "pdf": DocumentFormat.PDF,
        "docx": DocumentFormat.DOCX,
        "doc": DocumentFormat.DOC,
        "pptx": DocumentFormat.PPTX,
        "enl": DocumentFormat.ENL,
        "enlx": DocumentFormat.ENL,
        "enlp": DocumentFormat.ENL,
    }

    return format_map.get(ext, DocumentFormat.UNKNOWN)
