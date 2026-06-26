"""Configuration for the upload gateway service."""

import os
from pathlib import Path

from pydantic import model_validator
from pydantic_settings import BaseSettings


def _read_secret_file(file_path: str | None) -> str | None:
    """Read secret from file if path is provided and file exists."""
    if file_path and Path(file_path).is_file():
        return Path(file_path).read_text().strip()
    return None


class Settings(BaseSettings):
    """Service configuration loaded from environment variables.

    Supports Docker secrets via _FILE suffix environment variables (ADR-021).
    Example: UPLOAD_GATEWAY_MINIO_ACCESS_KEY_FILE=/run/secrets/minio_access_key
    """

    # Service settings
    host: str = "0.0.0.0"
    port: int = 8081
    workers: int = 4

    # MinIO settings
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False
    minio_bucket: str = "akidb-documents"

    # NATS settings
    nats_url: str = "nats://nats:4222"
    nats_stream: str = "INGESTION"
    nats_subject: str = "minio.uploads.document"

    # Upload settings
    max_file_size_mb: int = 100
    allowed_extensions: str = "pdf,docx,doc,csv,json,xml,html,xlsx,txt"

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"

    # Metrics
    metrics_enabled: bool = True

    class Config:
        env_prefix = "UPLOAD_GATEWAY_"
        case_sensitive = False

    @model_validator(mode="after")
    def load_secrets_from_files(self) -> "Settings":
        """Load secrets from _FILE environment variables (Docker secrets support)."""
        prefix = "UPLOAD_GATEWAY_"

        # Check for _FILE variants and load secrets
        access_key_file = os.environ.get(f"{prefix}MINIO_ACCESS_KEY_FILE")
        if access_key_file:
            secret = _read_secret_file(access_key_file)
            if secret:
                object.__setattr__(self, "minio_access_key", secret)

        secret_key_file = os.environ.get(f"{prefix}MINIO_SECRET_KEY_FILE")
        if secret_key_file:
            secret = _read_secret_file(secret_key_file)
            if secret:
                object.__setattr__(self, "minio_secret_key", secret)

        return self

    @property
    def allowed_extensions_list(self) -> list[str]:
        """Get allowed extensions as a list."""
        return [ext.strip().lower() for ext in self.allowed_extensions.split(",")]


settings = Settings()
