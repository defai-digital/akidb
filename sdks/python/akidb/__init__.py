"""AkiDB Python SDK — a typed client for the AkiDB retrieval/memory engine."""

from .client import (
    AkiDBClient,
    GetResult,
    SearchHit,
    TextSearchResult,
    VectorInput,
    build_memory_metadata,
)
from .errors import (
    AkiDBError,
    AlreadyExistsError,
    DeadlineExceededError,
    FailedPreconditionError,
    InternalError,
    InvalidArgumentError,
    NotFoundError,
    PermissionDeniedError,
    ResourceExhaustedError,
    UnauthenticatedError,
    UnavailableError,
)

__all__ = [
    "AkiDBClient",
    "GetResult",
    "SearchHit",
    "TextSearchResult",
    "VectorInput",
    "build_memory_metadata",
    "AkiDBError",
    "AlreadyExistsError",
    "DeadlineExceededError",
    "FailedPreconditionError",
    "InternalError",
    "InvalidArgumentError",
    "NotFoundError",
    "PermissionDeniedError",
    "ResourceExhaustedError",
    "UnauthenticatedError",
    "UnavailableError",
]
__version__ = "0.6.0"
