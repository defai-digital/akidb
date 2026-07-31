"""Configuration for the document parser service."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="DOC_PARSER_",
        case_sensitive=False,
    )

    # Service settings
    host: str = "0.0.0.0"
    port: int = 8080
    workers: int = 4

    # Parsing settings
    max_file_size_mb: int = 100
    max_pages: int = 1000
    timeout_seconds: int = 300

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"

    # Metrics
    metrics_enabled: bool = True
    metrics_port: int = 9090

settings = Settings()
