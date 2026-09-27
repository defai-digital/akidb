# AkiDB Upload Gateway

Upload gateway service for AkiDB ingestion pipeline.

## Features
- File upload to SeaweedFS through the S3 API (boto3, path-style SigV4)
- Event publishing to NATS
- Health monitoring

## Configuration

Settings are read from `UPLOAD_GATEWAY_`-prefixed environment variables.

| Setting | Environment variable | Default |
|---|---|---|
| `seaweedfs_endpoint` | `UPLOAD_GATEWAY_SEAWEEDFS_ENDPOINT` | `seaweedfs:8333` |
| `seaweedfs_access_key` | `UPLOAD_GATEWAY_SEAWEEDFS_ACCESS_KEY` | `akidb-admin` |
| `seaweedfs_secret_key` | `UPLOAD_GATEWAY_SEAWEEDFS_SECRET_KEY` | `akidb-secret-key` |
| `seaweedfs_secure` | `UPLOAD_GATEWAY_SEAWEEDFS_SECURE` | `false` |
| `seaweedfs_bucket` | `UPLOAD_GATEWAY_SEAWEEDFS_BUCKET` | `akidb-documents` |

`seaweedfs_endpoint` is a `host:port` pair without a scheme; the scheme is
`http`, or `https` when `seaweedfs_secure` is true.

Credentials can be supplied as Docker secrets through
`UPLOAD_GATEWAY_SEAWEEDFS_ACCESS_KEY_FILE` and
`UPLOAD_GATEWAY_SEAWEEDFS_SECRET_KEY_FILE`. A configured secret file must
exist and be non-empty, and blank credentials are rejected at startup.

The gateway reaches the SeaweedFS S3 gateway in path-style addressing only;
virtual-host-style requests are not supported. SeaweedFS does not support S3
bucket notifications, so the gateway's own NATS publish is the upload event
path, and the ingestion orchestrator's scheduled bucket sync remains the
recovery path for objects written by other means.
