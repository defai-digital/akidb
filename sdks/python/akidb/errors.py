"""Typed exception hierarchy for the AkiDB client.

gRPC status codes are mapped to specific exception types so callers can handle
failures by kind (e.g. catch ``NotFoundError``) instead of inspecting raw status
codes. Every mapped error keeps the originating ``code`` (a ``grpc.StatusCode``)
and ``details`` string for inspection.
"""

from __future__ import annotations

from typing import Optional

import grpc


class AkiDBError(Exception):
    """Base class for all AkiDB client errors."""

    def __init__(self, message: str, *, code: Optional[grpc.StatusCode] = None, details: str = ""):
        super().__init__(message)
        self.code = code
        self.details = details


class InvalidArgumentError(AkiDBError):
    """The request was malformed or violated a precondition on its arguments."""


class NotFoundError(AkiDBError):
    """The requested entity does not exist."""


class AlreadyExistsError(AkiDBError):
    """The entity the caller attempted to create already exists."""


class PermissionDeniedError(AkiDBError):
    """The caller is authenticated but not allowed to perform the operation."""


class UnauthenticatedError(AkiDBError):
    """The request lacks valid authentication credentials."""


class FailedPreconditionError(AkiDBError):
    """The system is not in a state required for the operation."""


class ResourceExhaustedError(AkiDBError):
    """A resource (e.g. backpressure / quota) has been exhausted. Retryable."""


class UnavailableError(AkiDBError):
    """The service is unavailable (often transient). Retryable."""


class DeadlineExceededError(AkiDBError):
    """The deadline elapsed before the operation completed. Retryable."""


class InternalError(AkiDBError):
    """An internal server error occurred."""


# gRPC status codes that are safe to retry. AkiDB's writes are id-keyed upserts,
# so retrying a unary call does not duplicate data.
RETRYABLE_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }
)

_CODE_MAP = {
    grpc.StatusCode.INVALID_ARGUMENT: InvalidArgumentError,
    grpc.StatusCode.NOT_FOUND: NotFoundError,
    grpc.StatusCode.ALREADY_EXISTS: AlreadyExistsError,
    grpc.StatusCode.PERMISSION_DENIED: PermissionDeniedError,
    grpc.StatusCode.UNAUTHENTICATED: UnauthenticatedError,
    grpc.StatusCode.FAILED_PRECONDITION: FailedPreconditionError,
    grpc.StatusCode.RESOURCE_EXHAUSTED: ResourceExhaustedError,
    grpc.StatusCode.UNAVAILABLE: UnavailableError,
    grpc.StatusCode.DEADLINE_EXCEEDED: DeadlineExceededError,
    grpc.StatusCode.INTERNAL: InternalError,
}


def map_grpc_error(exc: grpc.RpcError) -> AkiDBError:
    """Convert a ``grpc.RpcError`` into the appropriate :class:`AkiDBError`."""
    code = exc.code() if hasattr(exc, "code") else None
    details = exc.details() if hasattr(exc, "details") else str(exc)
    cls = _CODE_MAP.get(code, AkiDBError)
    name = code.name if code is not None else "UNKNOWN"
    return cls(f"{name}: {details}", code=code, details=details)
