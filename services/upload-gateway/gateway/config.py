"""Configuration for the upload gateway service."""

import os
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _read_secret_file(file_path: str | None) -> str | None:
    """Read a configured secret file, failing closed on invalid input."""
    if not file_path:
        return None

    path = Path(file_path)
    try:
        secret = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise ValueError(f"cannot read configured secret file: {path}") from error
    if not secret:
        raise ValueError(f"configured secret file is empty: {path}")
    return secret


class Settings(BaseSettings):
    """Service configuration loaded from environment variables.

    Supports Docker secrets via _FILE suffix environment variables (ADR-021).
    Example: UPLOAD_GATEWAY_MINIO_ACCESS_KEY_FILE=/run/secrets/minio_access_key
    """

    model_config = SettingsConfigDict(
        env_prefix="UPLOAD_GATEWAY_",
        case_sensitive=False,
    )

    # Service settings
    host: str = "0.0.0.0"
    port: int = 8081
    workers: int = 4

    # MinIO settings
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = Field(default="minioadmin", min_length=1)
    minio_secret_key: str = Field(default="minioadmin", min_length=1)
    minio_secure: bool = False
    minio_bucket: str = "akidb-documents"

    # NATS settings
    nats_url: str = "nats://nats:4222"
    nats_stream: str = Field(
        default="INGESTION",
        pattern=r"^[^.*>\s/\\]+$",
    )
    nats_subject: str = Field(
        default="minio.uploads.document",
        pattern=r"^[^.*>\s]+(?:\.[^.*>\s]+)*$",
    )
    nats_replicas: int = Field(default=1, ge=1, le=5)

    # Upload settings
    max_file_size_mb: int = Field(default=100, ge=1)
    allowed_extensions: str = (
        "pdf,docx,csv,tsv,json,xml,html,htm,"
        "xlsx,xlsm,txt,text,md,enl,enlx,enlp"
    )

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"

    # Metrics
    metrics_enabled: bool = True

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

        if not self.minio_access_key.strip():
            raise ValueError("minio_access_key must not be blank")
        if not self.minio_secret_key.strip():
            raise ValueError("minio_secret_key must not be blank")

        return self

    @property
    def allowed_extensions_list(self) -> list[str]:
        """Get allowed extensions as a list."""
        return [ext.strip().lower() for ext in self.allowed_extensions.split(",")]


settings = Settings()
