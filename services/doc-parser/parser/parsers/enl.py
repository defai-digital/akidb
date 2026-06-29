"""EndNote library parser for .enl, .enlx, and .enlp files."""

import io
import re
import time
import zipfile
from typing import Any
from xml.etree.ElementTree import Element

import structlog
from defusedxml import ElementTree

from parser.models import DocumentFormat, ParsedDocument
from parser.parsers.base import BaseParser

logger = structlog.get_logger()


class EnlParser(BaseParser):
    """Parser for EndNote library files.

    Supports:
    - .enl: XML-based EndNote library
    - .enlx: Compressed EndNote library (ZIP archive)
    - .enlp: Portable EndNote library (ZIP archive)

    Extracts bibliographic references including title, authors, year, abstract,
    journal, DOI, and other metadata fields.
    """

    # EndNote XML namespace
    NS = {"enl": "http://www.endnote.com/export/enl"}

    # Reference type mapping from EndNote codes
    REFERENCE_TYPES = {
        "0": "Generic",
        "1": "Journal Article",
        "2": "Book",
        "3": "Book Section",
        "4": "Manuscript",
        "5": "Edited Book",
        "6": "Magazine Article",
        "7": "Newspaper Article",
        "8": "Conference Proceedings",
        "9": "Thesis",
        "10": "Report",
        "11": "Personal Communication",
        "12": "Computer Program",
        "13": "Electronic Source",
        "14": "Audiovisual Material",
        "15": "Film or Broadcast",
        "16": "Artwork",
        "17": "Patent",
        "18": "Map",
        "19": "Hearing",
        "20": "Bill",
        "21": "Statute",
        "22": "Case",
        "23": "Government Document",
        "24": "Conference Paper",
        "25": "Online Database",
        "26": "Blog",
        "27": "Podcast",
        "28": "Web Page",
        "29": "Standard",
        "30": "Grant",
        "31": "Chart or Table",
        "32": "Equation",
        "33": "Figure",
        "34": "Music",
        "35": "Legal Rule or Regulation",
        "36": "Unpublished Work",
    }

    @property
    def format(self) -> DocumentFormat:
        return DocumentFormat.ENL

    def is_available(self) -> bool:
        """ENL parser uses defusedxml for secure XML parsing."""
        return True

    def parse(self, content: bytes, filename: str) -> ParsedDocument:
        """Parse EndNote library and extract references."""
        start_time = time.time()
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

        try:
            if ext in ("enlx", "enlp"):
                xml_content = self._extract_from_archive(content)
            else:
                xml_content = content

            references = self._parse_xml(xml_content)
            text_parts, metadata = self._format_references(references)

        except Exception as e:
            logger.error("enl_parse_error", error=str(e), filename=filename)
            raise

        parse_time_ms = (time.time() - start_time) * 1000

        logger.info(
            "enl_parsed",
            filename=filename,
            references=len(references),
            parse_time_ms=round(parse_time_ms, 2),
        )

        return ParsedDocument(
            text="\n\n".join(text_parts),
            format=DocumentFormat.ENL,
            page_count=0,
            metadata=metadata,
            tables=[],
            images=[],
            parse_time_ms=parse_time_ms,
        )

    def _extract_from_archive(self, content: bytes) -> bytes:
        """Extract .enl file from ENLX/ENLP archive."""
        with zipfile.ZipFile(io.BytesIO(content), "r") as zf:
            # Look for the main .enl file in the archive
            for name in zf.namelist():
                if name.endswith(".enl") or name.endswith(".xml"):
                    return zf.read(name)

            # Try to find any XML file that might be the library
            for name in zf.namelist():
                if "library" in name.lower() or "references" in name.lower():
                    return zf.read(name)

            # Fall back to first XML file
            for name in zf.namelist():
                if name.endswith(".xml"):
                    return zf.read(name)

            raise ValueError("No EndNote library found in archive")

    def _parse_xml(self, content: bytes) -> list[dict[str, Any]]:
        """Parse EndNote XML and extract references."""
        references = []

        try:
            # Try to parse as XML
            root = ElementTree.fromstring(content)
        except ElementTree.ParseError:
            # Try decoding and cleaning up the content
            try:
                text = content.decode("utf-8", errors="ignore")
                # Remove any BOM or invalid characters
                text = text.lstrip("\ufeff")
                root = ElementTree.fromstring(text)
            except ElementTree.ParseError:
                # Try to extract references using regex as fallback
                return self._parse_text_fallback(content)

        # Find all record elements (EndNote uses various structures)
        record_paths = [
            ".//record",
            ".//RECORD",
            ".//ref",
            ".//reference",
            ".//enl:record",
            ".//{http://www.endnote.com}record",
        ]

        records = []
        for path in record_paths:
            try:
                found = root.findall(path)
                if found:
                    records = found
                    break
            except (SyntaxError, ElementTree.ParseError):
                continue

        # If no records found, try parsing the whole document as a single reference
        if not records:
            ref = self._extract_reference_from_element(root)
            if ref and ref.get("title"):
                references.append(ref)
        else:
            for record in records:
                ref = self._extract_reference_from_element(record)
                if ref:
                    references.append(ref)

        return references

    def _extract_reference_from_element(self, elem: Element) -> dict[str, Any]:
        """Extract reference data from an XML element."""
        ref: dict[str, Any] = {}

        # Field mappings: element_names -> output_key
        field_mappings = {
            ("title", "TITLE", "primary-title", "article-title"): "title",
            ("author", "AUTHORS", "contributors", "author-list"): "authors",
            ("year", "YEAR", "pub-dates", "publication-year"): "year",
            ("abstract", "ABSTRACT", "notes"): "abstract",
            (
                "journal",
                "JOURNAL",
                "secondary-title",
                "periodical",
                "full-title",
            ): "journal",
            ("volume", "VOLUME"): "volume",
            ("issue", "ISSUE", "number"): "issue",
            ("pages", "PAGES", "page-range"): "pages",
            ("doi", "DOI", "electronic-resource-num"): "doi",
            ("isbn", "ISBN"): "isbn",
            ("issn", "ISSN"): "issn",
            ("keywords", "KEYWORDS", "keyword-list"): "keywords",
            ("url", "URL", "web-urls", "related-urls"): "url",
            ("publisher", "PUBLISHER"): "publisher",
            ("ref-type", "REF-TYPE", "reference-type", "type-of-reference"): "ref_type",
        }

        for elem_names, output_key in field_mappings.items():
            for elem_name in elem_names:
                value = self._find_text(elem, elem_name)
                if value:
                    ref[output_key] = value
                    break

        # Handle authors specially - they might be nested
        if "authors" not in ref:
            authors = self._extract_authors(elem)
            if authors:
                ref["authors"] = authors

        # Handle reference type
        if "ref_type" in ref and ref["ref_type"] in self.REFERENCE_TYPES:
            ref["ref_type"] = self.REFERENCE_TYPES[ref["ref_type"]]

        return ref

    def _find_text(self, elem: Element, tag: str) -> str | None:
        """Find text content of a tag, handling nested structures."""
        # Direct child
        child = elem.find(tag)
        if child is not None:
            text = self._get_all_text(child)
            if text:
                return text.strip()

        # Try case-insensitive search
        for child in elem:
            if child.tag.lower() == tag.lower():
                text = self._get_all_text(child)
                if text:
                    return text.strip()

        return None

    def _get_all_text(self, elem: Element) -> str:
        """Get all text content from an element, including nested elements."""
        text_parts = []
        if elem.text:
            text_parts.append(elem.text)
        for child in elem:
            child_text = self._get_all_text(child)
            if child_text:
                text_parts.append(child_text)
            if child.tail:
                text_parts.append(child.tail)
        return " ".join(text_parts)

    def _extract_authors(self, elem: Element) -> list[str]:
        """Extract author names from various EndNote structures."""
        authors = []

        # Look for author containers
        author_containers = [
            "contributors",
            "authors",
            "author-list",
            "CONTRIBUTORS",
            "AUTHORS",
        ]

        container = None
        for name in author_containers:
            container = elem.find(f".//{name}")
            if container is not None:
                break

        if container is None:
            container = elem

        # Look for individual author elements
        author_elements = ["author", "name", "AUTHOR", "person"]
        for author_elem_name in author_elements:
            for author_elem in container.findall(f".//{author_elem_name}"):
                text = self._get_all_text(author_elem)
                if text and text.strip():
                    authors.append(text.strip())

        return authors

    def _parse_text_fallback(self, content: bytes) -> list[dict[str, Any]]:
        """Fallback parser for non-standard EndNote formats."""
        references = []
        text = content.decode("utf-8", errors="ignore")

        # Try to extract references using common patterns
        # EndNote RIS-like format
        ref_blocks = re.split(r"\n(?=TY\s+-|%0\s+)", text)

        for block in ref_blocks:
            if not block.strip():
                continue

            ref: dict[str, Any] = {}

            # RIS format patterns
            patterns = {
                "title": r"(?:TI|T1)\s+-\s+(.+)",
                "authors": r"(?:AU|A1)\s+-\s+(.+)",
                "year": r"(?:PY|Y1)\s+-\s+(\d{4})",
                "abstract": r"(?:AB|N2)\s+-\s+(.+)",
                "journal": r"(?:JO|JF|T2)\s+-\s+(.+)",
                "volume": r"VL\s+-\s+(.+)",
                "issue": r"IS\s+-\s+(.+)",
                "pages": r"(?:SP|EP)\s+-\s+(.+)",
                "doi": r"DO\s+-\s+(.+)",
            }

            for field, pattern in patterns.items():
                matches = re.findall(pattern, block, re.MULTILINE)
                if matches:
                    if field == "authors":
                        ref[field] = [m.strip() for m in matches]
                    else:
                        ref[field] = " ".join(matches).strip()

            if ref.get("title"):
                references.append(ref)

        return references

    def _format_references(
        self, references: list[dict[str, Any]]
    ) -> tuple[list[str], dict[str, Any]]:
        """Format references as readable text and aggregate metadata."""
        text_parts = []
        metadata: dict[str, Any] = {
            "reference_count": len(references),
            "years": set(),
            "journals": set(),
            "types": set(),
        }

        for i, ref in enumerate(references, 1):
            parts = [f"[{i}]"]

            # Authors
            if "authors" in ref:
                authors = ref["authors"]
                if isinstance(authors, list):
                    author_str = ", ".join(authors)
                else:
                    author_str = str(authors)
                parts.append(f"Authors: {author_str}")

            # Title
            if "title" in ref:
                parts.append(f"Title: {ref['title']}")

            # Year
            if "year" in ref:
                parts.append(f"Year: {ref['year']}")
                metadata["years"].add(str(ref["year"]))

            # Journal
            if "journal" in ref:
                journal_info = ref["journal"]
                if "volume" in ref:
                    journal_info += f", Vol. {ref['volume']}"
                if "issue" in ref:
                    journal_info += f"({ref['issue']})"
                if "pages" in ref:
                    journal_info += f", pp. {ref['pages']}"
                parts.append(f"Journal: {journal_info}")
                metadata["journals"].add(ref["journal"])

            # DOI
            if "doi" in ref:
                parts.append(f"DOI: {ref['doi']}")

            # Abstract
            if "abstract" in ref:
                parts.append(f"Abstract: {ref['abstract']}")

            # Reference type
            if "ref_type" in ref:
                parts.append(f"Type: {ref['ref_type']}")
                metadata["types"].add(ref["ref_type"])

            text_parts.append("\n".join(parts))

        # Convert sets to sorted lists for JSON serialization
        metadata["years"] = sorted(metadata["years"])
        metadata["journals"] = sorted(metadata["journals"])
        metadata["types"] = sorted(metadata["types"])

        return text_parts, metadata
