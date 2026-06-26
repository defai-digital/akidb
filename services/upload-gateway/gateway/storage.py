"""MinIO storage client."""

import io
from typing import Any

import structlog
from minio import Minio
from minio.error import S3Error

from gateway.config import settings

logger = structlog.get_logger()


class StorageClient:
    """Client for MinIO object storage."""

    def __init__(self):
        """Initialize MinIO client."""
        self.client = Minio(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )
        self.bucket = settings.minio_bucket

    def ensure_bucket(self) -> bool:
        """Ensure the upload bucket exists."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info("bucket_created", bucket=self.bucket)
            return True
        except S3Error as e:
            logger.error("bucket_creation_failed", bucket=self.bucket, error=str(e))
            return False

    def upload(
        self,
        key: str,
        data: bytes,
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Upload a file to MinIO.

        Args:
            key: Object key (file path)
            data: File content
            content_type: MIME type
            metadata: Additional metadata

        Returns:
            True if upload succeeded
        """
        try:
            self.client.put_object(
                bucket_name=self.bucket,
                object_name=key,
                data=io.BytesIO(data),
                length=len(data),
                content_type=content_type or "application/octet-stream",
                metadata=metadata or {},
            )
            logger.info(
                "file_uploaded",
                bucket=self.bucket,
                key=key,
                size=len(data),
                content_type=content_type,
            )
            return True
        except S3Error as e:
            logger.error(
                "upload_failed",
                bucket=self.bucket,
                key=key,
                error=str(e),
            )
            raise

    def is_connected(self) -> bool:
        """Check if MinIO is reachable."""
        try:
            self.client.list_buckets()
            return True
        except Exception:
            return False

    def get_bucket_info(self) -> dict:
        """Get information about the upload bucket."""
        try:
            exists = self.client.bucket_exists(self.bucket)
            object_count = None
            if exists:
                objects = list(self.client.list_objects(self.bucket))
                object_count = len(objects)
            return {
                "name": self.bucket,
                "exists": exists,
                "object_count": object_count,
            }
        except Exception as e:
            logger.error("bucket_info_failed", error=str(e))
            return {
                "name": self.bucket,
                "exists": False,
                "object_count": None,
            }


# Global client instance
storage_client: StorageClient | None = None


def get_storage_client() -> StorageClient:
    """Get or create the storage client."""
    global storage_client
    if storage_client is None:
        storage_client = StorageClient()
    return storage_client
