"""SeaweedFS S3 storage client."""

import io
from typing import Any

import boto3
import structlog
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from gateway.config import settings

logger = structlog.get_logger()

# SeaweedFS ignores the region, but SigV4 signing requires one.
S3_REGION = "us-east-1"
S3_MAX_ATTEMPTS = 3

_MISSING_BUCKET_CODES = {"404", "NoSuchBucket", "NotFound"}
_BUCKET_EXISTS_CODES = {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}


def _endpoint_url() -> str:
    """Build the S3 endpoint URL from the configured endpoint.

    A full URL is used as given; a bare ``host:port`` takes its scheme from
    ``seaweedfs_secure``. Deployments pass a URL (matching the Rust config
    style), while local overrides may pass ``host:port``.
    """
    endpoint = settings.seaweedfs_endpoint.strip().rstrip("/")
    if "://" in endpoint:
        return endpoint
    scheme = "https" if settings.seaweedfs_secure else "http"
    return f"{scheme}://{endpoint}"


def _client_config() -> Config:
    """Build a bounded botocore config for the SeaweedFS S3 gateway.

    Path-style addressing is required: the SeaweedFS gateway must not be used
    in virtual-host mode. Checksums are requested only when the operation
    requires them, since SeaweedFS verifies ``x-amz-checksum-*`` values.
    """
    return Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
        retries={"max_attempts": S3_MAX_ATTEMPTS, "mode": "standard"},
    )


def _error_code(error: ClientError) -> str:
    """Return the S3 error code carried by a botocore client error."""
    return error.response.get("Error", {}).get("Code", "")


def _is_missing_bucket(error: ClientError) -> bool:
    """Return whether a client error means the bucket does not exist."""
    if _error_code(error) in _MISSING_BUCKET_CODES:
        return True
    status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return status_code == 404


class StorageClient:
    """Client for the SeaweedFS object storage gateway."""

    def __init__(self):
        """Initialize the SeaweedFS S3 client."""
        self.client = boto3.client(
            "s3",
            endpoint_url=_endpoint_url(),
            aws_access_key_id=settings.seaweedfs_access_key,
            aws_secret_access_key=settings.seaweedfs_secret_key,
            region_name=S3_REGION,
            config=_client_config(),
        )
        self.bucket = settings.seaweedfs_bucket

    def _bucket_exists(self) -> bool:
        """Return whether the configured bucket exists."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return True
        except ClientError as error:
            if _is_missing_bucket(error):
                return False
            raise

    def _count_objects(self) -> int:
        """Count objects in the bucket with paginated ListObjectsV2 calls."""
        count = 0
        continuation_token: str | None = None
        while True:
            request: dict[str, Any] = {"Bucket": self.bucket}
            if continuation_token:
                request["ContinuationToken"] = continuation_token
            response = self.client.list_objects_v2(**request)
            count += len(response.get("Contents", []))
            if not response.get("IsTruncated"):
                return count
            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                return count

    def ensure_bucket(self) -> bool:
        """Ensure the upload bucket exists."""
        try:
            if not self._bucket_exists():
                self.client.create_bucket(Bucket=self.bucket)
                logger.info("bucket_created", bucket=self.bucket)
            return True
        except ClientError as error:
            if _error_code(error) in _BUCKET_EXISTS_CODES:
                return True
            logger.error("bucket_creation_failed", bucket=self.bucket, error=str(error))
            return False
        except BotoCoreError as error:
            logger.error("bucket_creation_failed", bucket=self.bucket, error=str(error))
            return False

    def upload(
        self,
        key: str,
        data: bytes,
        content_type: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Upload a file to SeaweedFS.

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
                Bucket=self.bucket,
                Key=key,
                Body=io.BytesIO(data),
                ContentType=content_type or "application/octet-stream",
                Metadata=metadata or {},
            )
            logger.info(
                "file_uploaded",
                bucket=self.bucket,
                key=key,
                size=len(data),
                content_type=content_type,
            )
            return True
        except (ClientError, BotoCoreError) as error:
            logger.error(
                "upload_failed",
                bucket=self.bucket,
                key=key,
                error=str(error),
            )
            raise

    def is_connected(self) -> bool:
        """Check that the configured bucket is reachable."""
        try:
            return self._bucket_exists()
        except Exception:
            return False

    def get_bucket_info(self) -> dict:
        """Get information about the upload bucket."""
        try:
            exists = self._bucket_exists()
            object_count = None
            if exists:
                object_count = self._count_objects()
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
