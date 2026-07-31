"""Tests for MinIO storage health and bucket reporting."""

from gateway.storage import StorageClient


class FakeMinio:
    def __init__(self, *, exists: bool = True) -> None:
        self.exists = exists
        self.list_calls: list[tuple[str, bool]] = []

    def bucket_exists(self, bucket: str) -> bool:
        assert bucket == "documents"
        return self.exists

    def list_objects(self, bucket: str, *, recursive: bool = False):
        self.list_calls.append((bucket, recursive))
        return iter([object(), object()])


def storage_with(client: FakeMinio) -> StorageClient:
    storage = StorageClient.__new__(StorageClient)
    storage.client = client
    storage.bucket = "documents"
    return storage


def test_health_requires_the_configured_bucket() -> None:
    assert storage_with(FakeMinio(exists=True)).is_connected()
    assert not storage_with(FakeMinio(exists=False)).is_connected()


def test_bucket_info_counts_objects_recursively() -> None:
    client = FakeMinio()

    info = storage_with(client).get_bucket_info()

    assert info == {
        "name": "documents",
        "exists": True,
        "object_count": 2,
    }
    assert client.list_calls == [("documents", True)]
