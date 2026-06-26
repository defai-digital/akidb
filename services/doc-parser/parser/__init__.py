"""AkiDB Document Parser Service.

Handles complex document parsing (PDF, DOCX) that require Python libraries.
Called by the Rust ingestion orchestrator via HTTP.
"""

__version__ = "0.1.0"

from parser.api import app
from parser.config import settings

__all__ = ["app", "settings", "__version__"]
