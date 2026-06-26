"""PDF document parser using pdfplumber and pypdf."""

import io
import time
from typing import Any

import structlog

from parser.models import DocumentFormat, ImageRef, ParsedDocument, TableData
from parser.parsers.base import BaseParser

logger = structlog.get_logger()


class PdfParser(BaseParser):
    """Parser for PDF documents using pdfplumber."""

    @property
    def format(self) -> DocumentFormat:
        return DocumentFormat.PDF

    def is_available(self) -> bool:
        """Check if pdfplumber is available."""
        try:
            import pdfplumber
            import pypdf

            return True
        except ImportError:
            return False

    def parse(self, content: bytes, filename: str) -> ParsedDocument:
        """Parse PDF content and extract text, tables, and metadata."""
        import pdfplumber
        from pypdf import PdfReader

        start_time = time.time()
        text_parts: list[str] = []
        tables: list[TableData] = []
        images: list[ImageRef] = []
        metadata: dict[str, Any] = {}
        page_count = 0

        try:
            # Use pypdf for metadata
            pdf_reader = PdfReader(io.BytesIO(content))
            page_count = len(pdf_reader.pages)

            if pdf_reader.metadata:
                metadata = {
                    "title": pdf_reader.metadata.get("/Title"),
                    "author": pdf_reader.metadata.get("/Author"),
                    "subject": pdf_reader.metadata.get("/Subject"),
                    "creator": pdf_reader.metadata.get("/Creator"),
                    "producer": pdf_reader.metadata.get("/Producer"),
                }
                # Remove None values
                metadata = {k: v for k, v in metadata.items() if v}

            # Use pdfplumber for text and table extraction
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    # Extract text
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)

                    # Extract tables
                    page_tables = page.extract_tables()
                    for table in page_tables:
                        if table and len(table) > 0:
                            headers = [str(cell) if cell else "" for cell in table[0]]
                            rows = [
                                [str(cell) if cell else "" for cell in row]
                                for row in table[1:]
                            ]
                            tables.append(
                                TableData(headers=headers, rows=rows, page=page_num + 1)
                            )

                    # Track images
                    for img_idx, img in enumerate(page.images):
                        images.append(
                            ImageRef(
                                index=len(images),
                                page=page_num + 1,
                                width=int(img.get("width", 0)),
                                height=int(img.get("height", 0)),
                            )
                        )

        except Exception as e:
            logger.error("pdf_parse_error", error=str(e), filename=filename)
            raise

        parse_time_ms = (time.time() - start_time) * 1000

        logger.info(
            "pdf_parsed",
            filename=filename,
            pages=page_count,
            tables=len(tables),
            images=len(images),
            parse_time_ms=round(parse_time_ms, 2),
        )

        return ParsedDocument(
            text="\n\n".join(text_parts),
            format=DocumentFormat.PDF,
            page_count=page_count,
            metadata=metadata,
            tables=tables,
            images=images,
            parse_time_ms=parse_time_ms,
        )
