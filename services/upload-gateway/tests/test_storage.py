"""Tests for SeaweedFS storage health and bucket reporting."""

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from gateway import storage
from gateway.config import settings
from gateway.storage import StorageClient


def s3_error(code: str, operation: str = "HeadBucket") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class FakeS3:
    """Minimal boto3 S3 client double."""

    def __init__(self, *, exists: bool = True, pages: list[dict] | None = None) -> None:
        self.exists = exists
        self.pages = pages or []
        self.head_calls: list[str] = []
        self.list_calls: list[dict] = []

    def head_bucket(self, **kwargs) -> dict:
        self.head_calls.append(kwargs["Bucket"])
        if not self.exists:
            raise s3_error("404")
        return {}

    def list_objects_v2(self, **kwargs) -> dict:
        self.list_calls.append(kwargs)
        index = len(self.list_calls) - 1
        if index < len(self.pages):
            return self.pages[index]
        return {"Contents": []}


def storage_with(client) -> StorageClient:
    storage_client = StorageClient.__new__(StorageClient)
    storage_client.client = client
    storage_client.bucket = "documents"
    return storage_client


def test_client_uses_path_style_sigv4_and_required_checksums(monkeypatch) -> None:
    captured: dict = {}

    def fake_client(service_name, **kwargs):
        captured["service_name"] = service_name
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(storage.boto3, "client", fake_client)

    StorageClient()

    assert captured["service_name"] == "s3"
    assert captured["endpoint_url"] == "http://seaweedfs:8333"
    assert captured["aws_access_key_id"] == settings.seaweedfs_access_key
    assert captured["aws_secret_access_key"] == settings.seaweedfs_secret_key
    assert captured["region_name"] == "us-east-1"
    config = captured["config"]
    assert config.signature_version == "s3v4"
    assert config.s3["addressing_style"] == "path"
    assert config.request_checksum_calculation == "when_required"
    assert config.response_checksum_validation == "when_required"
    assert config.retries["max_attempts"] == 3


def test_secure_endpoint_uses_https(monkeypatch) -> None:
    captured: dict = {}

    def fake_client(_service_name, **kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(storage.boto3, "client", fake_client)
    monkeypatch.setattr(settings, "seaweedfs_secure", True)

    StorageClient()

    assert captured["endpoint_url"] == "https://seaweedfs:8333"


def test_full_url_endpoint_is_used_as_given(monkeypatch) -> None:
    captured: dict = {}

    def fake_client(_service_name, **kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(storage.boto3, "client", fake_client)
    monkeypatch.setattr(settings, "seaweedfs_endpoint", "http://seaweedfs:8333/")
    monkeypatch.setattr(settings, "seaweedfs_secure", True)

    StorageClient()

    assert captured["endpoint_url"] == "http://seaweedfs:8333"


def test_health_requires_the_configured_bucket() -> None:
    assert storage_with(FakeS3(exists=True)).is_connected()
    assert not storage_with(FakeS3(exists=False)).is_connected()


def test_health_is_false_when_the_client_fails() -> None:
    class Broken:
        @staticmethod
        def head_bucket(**_kwargs):
            raise s3_error("AccessDenied")

    assert not storage_with(Broken()).is_connected()


def test_bucket_info_counts_objects_across_pages() -> None:
    client = FakeS3(
        pages=[
            {
                "Contents": [object(), object()],
                "IsTruncated": True,
                "NextContinuationToken": "page-2",
            },
            {"Contents": [object()], "IsTruncated": False},
        ]
    )

    info = storage_with(client).get_bucket_info()

    assert info == {
        "name": "documents",
        "exists": True,
        "object_count": 3,
    }
    assert client.head_calls == ["documents"]
    assert client.list_calls == [
        {"Bucket": "documents"},
        {"Bucket": "documents", "ContinuationToken": "page-2"},
    ]


def test_bucket_info_reports_missing_bucket_without_listing() -> None:
    client = FakeS3(exists=False)

    info = storage_with(client).get_bucket_info()

    assert info == {
        "name": "documents",
        "exists": False,
        "object_count": None,
    }
    assert client.list_calls == []


def test_ensure_bucket_creates_a_missing_bucket() -> None:
    class Client:
        def __init__(self) -> None:
            self.created: list[str] = []

        @staticmethod
        def head_bucket(**_kwargs):
            raise s3_error("404")

        def create_bucket(self, **kwargs) -> dict:
            self.created.append(kwargs["Bucket"])
            return {}

    client = Client()

    assert storage_with(client).ensure_bucket()
    assert client.created == ["documents"]


def test_ensure_bucket_accepts_an_already_existing_bucket() -> None:
    class Existing:
        @staticmethod
        def head_bucket(**_kwargs):
            return {}

    class Race:
        @staticmethod
        def head_bucket(**_kwargs):
            raise s3_error("404")

        @staticmethod
        def create_bucket(**_kwargs):
            raise s3_error("BucketAlreadyOwnedByYou", "CreateBucket")

    assert storage_with(Existing()).ensure_bucket()
    assert storage_with(Race()).ensure_bucket()


def test_ensure_bucket_fails_closed_on_errors() -> None:
    class Denied:
        @staticmethod
        def head_bucket(**_kwargs):
            raise s3_error("404")

        @staticmethod
        def create_bucket(**_kwargs):
            raise s3_error("AccessDenied", "CreateBucket")

    class Broken:
        @staticmethod
        def head_bucket(**_kwargs):
            raise BotoCoreError()

    assert not storage_with(Denied()).ensure_bucket()
    assert not storage_with(Broken()).ensure_bucket()


def test_upload_maps_content_type_and_metadata() -> None:
    recorded: dict = {}

    class Client:
        @staticmethod
        def put_object(**kwargs) -> dict:
            recorded.update(kwargs)
            return {}

    assert storage_with(Client()).upload("key", b"payload", "text/plain", {"a": "b"})
    assert recorded["Bucket"] == "documents"
    assert recorded["Key"] == "key"
    assert recorded["ContentType"] == "text/plain"
    assert recorded["Metadata"] == {"a": "b"}
    assert recorded["Body"].read() == b"payload"


def test_upload_defaults_content_type_and_metadata() -> None:
    recorded: dict = {}

    class Client:
        @staticmethod
        def put_object(**kwargs) -> dict:
            recorded.update(kwargs)
            return {}

    assert storage_with(Client()).upload("key", b"payload")
    assert recorded["ContentType"] == "application/octet-stream"
    assert recorded["Metadata"] == {}


def test_upload_errors_are_raised_after_logging() -> None:
    class Client:
        @staticmethod
        def put_object(**_kwargs):
            raise s3_error("AccessDenied", "PutObject")

    with pytest.raises(ClientError):
        storage_with(Client()).upload("key", b"payload")


def test_get_storage_client_is_a_singleton(monkeypatch) -> None:
    created = []

    class FakeStorage:
        def __init__(self) -> None:
            created.append(self)

    monkeypatch.setattr(storage, "StorageClient", FakeStorage)
    monkeypatch.setattr(storage, "storage_client", None)

    client = storage.get_storage_client()

    assert client is created[0]
    assert storage.get_storage_client() is client
    assert len(created) == 1
