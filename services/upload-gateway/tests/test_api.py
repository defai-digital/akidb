"""Tests for the upload gateway API."""

import io
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Response, UploadFile, status
from nats.js.api import StreamConfig

from gateway import api, events
from gateway.config import Settings, settings
from gateway.events import EventPublisher
from gateway.models import UploadEvent


def test_allowed_extensions_list():
    """Test that allowed extensions are parsed correctly."""
    extensions = settings.allowed_extensions_list
    assert "pdf" in extensions
    assert "docx" in extensions
    assert "csv" in extensions
    assert "tsv" in extensions
    assert "enl" in extensions
    assert "doc" not in extensions
    assert "docm" not in extensions
    assert "dotx" not in extensions
    assert "dotm" not in extensions
    assert "xls" not in extensions
    assert "xlsb" not in extensions
    assert "ods" not in extensions


def test_settings_defaults():
    """Test that settings have sensible defaults."""
    assert settings.port == 8081
    assert settings.max_file_size_mb > 0
    assert settings.minio_bucket == "akidb-documents"


def test_invalid_publish_subject_is_rejected():
    with pytest.raises(ValueError, match="nats_subject"):
        Settings(nats_subject="minio.uploads.>")


def test_invalid_stream_name_and_blank_credentials_are_rejected():
    with pytest.raises(ValueError, match="nats_stream"):
        Settings(nats_stream="INGESTION.bad")
    with pytest.raises(ValueError, match="minio_access_key"):
        Settings(minio_access_key="   ")
    with pytest.raises(ValueError, match="minio_secret_key"):
        Settings(minio_secret_key="\t")


def test_nats_stream_accepts_gateway_and_minio_events():
    """The shared stream must cover both canonical and MinIO subjects."""
    config = EventPublisher()._stream_config()

    assert config.subjects == ["minio.uploads", "minio.uploads.>"]
    assert config.num_replicas == settings.nats_replicas


def test_custom_nats_subject_is_added_to_stream(monkeypatch):
    """A configured subject outside minio.uploads.> must remain publishable."""
    monkeypatch.setattr(settings, "nats_subject", "akidb.uploads.document")

    config = EventPublisher()._stream_config()

    assert "akidb.uploads.document" in config.subjects


@pytest.mark.asyncio
async def test_existing_nats_stream_is_reconciled(monkeypatch):
    """An existing wildcard-only stream must be updated for exact MinIO events."""

    class JetStream:
        updated = None

        async def stream_info(self, _name):
            return SimpleNamespace(
                config=StreamConfig(
                    name="INGESTION",
                    subjects=["minio.uploads.>", "custom.>"],
                    num_replicas=1,
                )
            )

        async def update_stream(self, *, config):
            self.updated = config

    publisher = EventPublisher()
    publisher.js = JetStream()
    monkeypatch.setattr(settings, "nats_replicas", 3)

    await publisher._ensure_stream()

    assert publisher.js.updated.subjects == [
        "minio.uploads.>",
        "custom.>",
        "minio.uploads",
    ]
    assert publisher.js.updated.num_replicas == 3


@pytest.mark.asyncio
async def test_failed_stream_setup_is_disconnected_and_can_retry(monkeypatch):
    """A partial NATS connection must not be healthy or poison future retries."""

    class Connection:
        def __init__(self, stream):
            self.stream = stream
            self.is_connected = True
            self.closed = False

        def jetstream(self):
            return self.stream

        async def close(self):
            self.closed = True
            self.is_connected = False

    class BrokenJetStream:
        async def stream_info(self, _name):
            raise RuntimeError("JetStream unavailable")

    class ReadyJetStream:
        async def stream_info(self, _name):
            return SimpleNamespace(
                config=StreamConfig(
                    name="INGESTION",
                    subjects=["minio.uploads", "minio.uploads.>"],
                    num_replicas=settings.nats_replicas,
                )
            )

    connections = [Connection(BrokenJetStream()), Connection(ReadyJetStream())]

    async def connect(_url):
        return connections.pop(0)

    monkeypatch.setattr(events.nats, "connect", connect)
    publisher = EventPublisher()

    assert not await publisher.connect()
    assert not publisher.is_connected()
    assert await publisher.connect()
    assert publisher.is_connected()


@pytest.mark.asyncio
async def test_publish_failure_marks_stream_unready_for_reconnect():
    """A publish failure must make the next request re-run stream setup."""

    class Connection:
        is_connected = True

    class JetStream:
        async def publish(self, _subject, _payload):
            raise RuntimeError("stream was removed")

    publisher = EventPublisher()
    publisher.nc = Connection()
    publisher.js = JetStream()
    publisher._stream_ready = True

    assert not await publisher.publish(
        UploadEvent(bucket="bucket", key="key", size=1)
    )
    assert not publisher.is_connected()


def test_configured_secret_files_fail_closed(monkeypatch, tmp_path):
    """Missing or empty Docker secrets must not fall back to default credentials."""
    missing = tmp_path / "missing"
    monkeypatch.setenv(
        "UPLOAD_GATEWAY_MINIO_ACCESS_KEY_FILE",
        str(missing),
    )
    with pytest.raises(ValueError, match="cannot read configured secret file"):
        Settings()

    empty = tmp_path / "empty"
    empty.write_text("", encoding="utf-8")
    monkeypatch.setenv(
        "UPLOAD_GATEWAY_MINIO_ACCESS_KEY_FILE",
        str(empty),
    )
    with pytest.raises(ValueError, match="configured secret file is empty"):
        Settings()


@pytest.mark.asyncio
async def test_health_is_unavailable_when_a_dependency_is_down(monkeypatch):
    """HTTP health must fail closed when MinIO or NATS is unavailable."""

    class Storage:
        @staticmethod
        def is_connected():
            return False

    class Publisher:
        @staticmethod
        def is_connected():
            return True

    async def get_publisher():
        return Publisher()

    monkeypatch.setattr(api, "get_storage_client", Storage)
    monkeypatch.setattr(api, "get_event_publisher", get_publisher)
    response = Response()

    health = await api.health_check(response)

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert health.status == "degraded"
    assert not health.minio_connected
    assert health.nats_connected


@pytest.mark.asyncio
async def test_upload_fails_when_event_cannot_be_published(monkeypatch):
    """A stored object without a NATS event must not be reported as successful."""

    class Storage:
        @staticmethod
        def upload(**_kwargs):
            return True

    class Publisher:
        @staticmethod
        async def publish(_event):
            return False

    async def get_publisher():
        return Publisher()

    monkeypatch.setattr(api, "get_storage_client", Storage)
    monkeypatch.setattr(api, "get_event_publisher", get_publisher)
    upload = UploadFile(filename="test.txt", file=io.BytesIO(b"hello"))

    with pytest.raises(HTTPException) as raised:
        await api.upload_document(upload)

    assert raised.value.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert raised.value.detail["error"].startswith("Event publication failed")


@pytest.mark.asyncio
async def test_empty_upload_is_rejected_before_storage(monkeypatch):
    class Storage:
        @staticmethod
        def upload(**_kwargs):
            raise AssertionError("empty content must not reach storage")

    monkeypatch.setattr(api, "get_storage_client", Storage)
    upload = UploadFile(filename="empty.txt", file=io.BytesIO())

    with pytest.raises(HTTPException) as raised:
        await api.upload_document(upload)

    assert raised.value.status_code == status.HTTP_400_BAD_REQUEST
    assert raised.value.detail == "File is empty"


@pytest.mark.asyncio
async def test_oversized_upload_is_bounded_and_rejected(monkeypatch):
    class Storage:
        @staticmethod
        def upload(**_kwargs):
            raise AssertionError("oversized content must not reach storage")

    monkeypatch.setattr(api, "get_storage_client", Storage)
    monkeypatch.setattr(settings, "max_file_size_mb", 1)
    upload = UploadFile(
        filename="large.txt",
        file=io.BytesIO(b"x" * (1024 * 1024 + 10)),
    )

    with pytest.raises(HTTPException) as raised:
        await api.upload_document(upload)

    assert raised.value.status_code == status.HTTP_400_BAD_REQUEST
    assert raised.value.detail == "File exceeds the 1MB limit"
    assert upload.file.tell() == 1024 * 1024 + 1


@pytest.mark.asyncio
async def test_upload_sanitizes_filename_and_rejects_unsafe_prefix(monkeypatch):
    captured = {}

    class Storage:
        @staticmethod
        def upload(**kwargs):
            captured.update(kwargs)
            return True

    class Publisher:
        @staticmethod
        async def publish(_event):
            return True

    async def get_publisher():
        return Publisher()

    monkeypatch.setattr(api, "get_storage_client", Storage)
    monkeypatch.setattr(api, "get_event_publisher", get_publisher)
    upload = UploadFile(
        filename="../../report.txt",
        file=io.BytesIO(b"safe content"),
    )

    result = await api.upload_document(upload, prefix="tenant/reports")

    assert result.key.startswith("tenant/reports/")
    assert result.key.endswith("_report.txt")
    assert ".." not in result.key
    assert captured["metadata"]["original_filename"] == "report.txt"

    unsafe = UploadFile(filename="report.txt", file=io.BytesIO(b"content"))
    with pytest.raises(HTTPException) as raised:
        await api.upload_document(unsafe, prefix="../escape")
    assert raised.value.status_code == status.HTTP_400_BAD_REQUEST
