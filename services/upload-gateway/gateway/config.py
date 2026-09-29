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
    Example: UPLOAD_GATEWAY_SEAWEEDFS_ACCESS_KEY_FILE=/run/secrets/seaweedfs_access_key
    """

    model_config = SettingsConfigDict(
        env_prefix="UPLOAD_GATEWAY_",
        case_sensitive=False,
    )

    # Service settings
    host: str = "0.0.0.0"
    port: int = 8081
    workers: int = 4

    # SeaweedFS settings. The endpoint is a bare `host:port` or a full URL;
    # a full URL wins over `seaweedfs_secure`.
    seaweedfs_endpoint: str = "seaweedfs:8333"
    seaweedfs_access_key: str = Field(default="akidb-admin", min_length=1)
    seaweedfs_secret_key: str = Field(default="akidb-secret-key", min_length=1)
    seaweedfs_secure: bool = False
    seaweedfs_bucket: str = "akidb-documents"

    # NATS settings
    nats_url: str = "nats://nats:4222"
    nats_stream: str = Field(
        default="INGESTION",
        pattern=r"^[^.*>\s/\\]+$",
    )
    nats_subject: str = Field(
        default="seaweedfs.uploads.document",
        pattern=r"^[^.*>\s]+(?:\.[^.*>\s]+)*$",
    )
    nats_replicas: int = Field(default=1, ge=1, le=5)

    # NATS authentication (optional; when all are unset the client connects
    # anonymously, preserving historical loopback/compose behavior).
    # Precedence on connect: credentials file > token > user+password.
    nats_token: str | None = None
    nats_user: str | None = None
    nats_password: str | None = None
    nats_credentials_file: str | None = None

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
        access_key_file = os.environ.get(f"{prefix}SEAWEEDFS_ACCESS_KEY_FILE")
        if access_key_file:
            secret = _read_secret_file(access_key_file)
            if secret:
                object.__setattr__(self, "seaweedfs_access_key", secret)

        secret_key_file = os.environ.get(f"{prefix}SEAWEEDFS_SECRET_KEY_FILE")
        if secret_key_file:
            secret = _read_secret_file(secret_key_file)
            if secret:
                object.__setattr__(self, "seaweedfs_secret_key", secret)

        nats_token_file = os.environ.get(f"{prefix}NATS_TOKEN_FILE")
        if nats_token_file:
            secret = _read_secret_file(nats_token_file)
            if secret:
                object.__setattr__(self, "nats_token", secret)

        nats_password_file = os.environ.get(f"{prefix}NATS_PASSWORD_FILE")
        if nats_password_file:
            secret = _read_secret_file(nats_password_file)
            if secret:
                object.__setattr__(self, "nats_password", secret)

        if not self.seaweedfs_access_key.strip():
            raise ValueError("seaweedfs_access_key must not be blank")
        if not self.seaweedfs_secret_key.strip():
            raise ValueError("seaweedfs_secret_key must not be blank")

        return self

    @property
    def allowed_extensions_list(self) -> list[str]:
        """Get allowed extensions as a list."""
        return [ext.strip().lower() for ext in self.allowed_extensions.split(",")]


settings = Settings()
