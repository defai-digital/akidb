"""AkiDB Python SDK — a typed client for the AkiDB retrieval/memory engine."""

from .client import AkiDBClient, SearchHit, TextSearchResult

__all__ = ["AkiDBClient", "SearchHit", "TextSearchResult"]
__version__ = "0.5.0"
