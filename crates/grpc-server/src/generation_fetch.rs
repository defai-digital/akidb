//! Authorized immutable-object fetch boundary for generation publication.

#[cfg(feature = "generation-s3")]
use std::collections::HashSet;
#[cfg(feature = "generation-s3")]
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
#[cfg(feature = "generation-s3")]
use std::time::Duration;

use akidb_contracts::ImmutableObjectReference;
use async_trait::async_trait;
#[cfg(feature = "generation-s3")]
use aws_config::BehaviorVersion;
#[cfg(feature = "generation-s3")]
use aws_sdk_s3::config::{Builder as S3ClientConfigBuilder, Credentials, Region};
use thiserror::Error;
#[cfg(feature = "generation-s3")]
use tokio::io::AsyncWriteExt;
#[cfg(feature = "generation-s3")]
use url::Url;

#[derive(Debug, Error)]
pub enum GenerationFetchError {
    #[error("generation object reference is not authorized: {0}")]
    Unauthorized(String),
    #[error("generation object is unavailable: {0}")]
    Unavailable(String),
    #[error("generation object fetch failed: {0}")]
    Transport(String),
    #[error("generation object {operation} returned HTTP {status}")]
    Remote {
        operation: &'static str,
        status: u16,
    },
    #[error("generation object {0} deadline exceeded")]
    Timeout(&'static str),
    #[error("generation object temporary-file error: {0}")]
    Io(#[from] std::io::Error),
    #[error("generation object fetch was rejected: {0}")]
    Rejected(String),
}

/// A fetched object held in a regular, non-symlink temporary file.
///
/// The generation store independently verifies exact size and SHA-256 while
/// streaming this file. Dropping the handle removes only this exact file.
#[derive(Debug)]
pub struct FetchedGenerationBundle {
    path: PathBuf,
    remove_on_drop: bool,
}

impl FetchedGenerationBundle {
    pub fn temporary(path: impl Into<PathBuf>) -> Result<Self, GenerationFetchError> {
        let path = path.into();
        validate_regular_file(&path)?;
        Ok(Self {
            path,
            remove_on_drop: true,
        })
    }

    pub fn retained(path: impl Into<PathBuf>) -> Result<Self, GenerationFetchError> {
        let path = path.into();
        validate_regular_file(&path)?;
        Ok(Self {
            path,
            remove_on_drop: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn open(&self) -> Result<File, GenerationFetchError> {
        validate_regular_file(&self.path)?;
        File::open(&self.path).map_err(Into::into)
    }
}

impl Drop for FetchedGenerationBundle {
    fn drop(&mut self) {
        if self.remove_on_drop {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[async_trait]
pub trait GenerationBundleFetcher: Send + Sync {
    /// Fetch the already-authorized immutable reference into a local regular
    /// file. Implementations enforce bucket/host policy and a bounded download;
    /// the generation store independently rechecks size and checksum.
    async fn fetch(
        &self,
        reference: &ImmutableObjectReference,
    ) -> Result<FetchedGenerationBundle, GenerationFetchError>;
}

#[cfg(feature = "generation-s3")]
#[derive(Debug, Clone)]
pub struct S3GenerationBundleFetcherConfig {
    pub allowed_buckets: HashSet<String>,
    pub download_directory: PathBuf,
    pub max_bundle_size_bytes: u64,
    pub require_version_or_digest_key: bool,
}

/// Bounded, streaming S3/SeaweedFS fetcher using an already-configured SDK client.
///
/// The SDK client fixes the endpoint and credentials. This layer additionally
/// restricts buckets, URI query parameters, object immutability, and bytes
/// written to a private local directory.
#[cfg(feature = "generation-s3")]
pub struct S3GenerationBundleFetcher {
    client: aws_sdk_s3::Client,
    config: S3GenerationBundleFetcherConfig,
    deadlines: FetchDeadlines,
}

#[cfg(feature = "generation-s3")]
#[derive(Clone, Copy)]
struct FetchDeadlines {
    request: Duration,
    idle: Duration,
    total: Duration,
}

#[cfg(feature = "generation-s3")]
impl Default for FetchDeadlines {
    fn default() -> Self {
        Self {
            request: Duration::from_secs(30),
            idle: Duration::from_secs(30),
            total: Duration::from_secs(120),
        }
    }
}

#[cfg(feature = "generation-s3")]
impl S3GenerationBundleFetcher {
    pub fn new(
        client: aws_sdk_s3::Client,
        mut config: S3GenerationBundleFetcherConfig,
    ) -> Result<Self, GenerationFetchError> {
        if config.allowed_buckets.is_empty()
            || config
                .allowed_buckets
                .iter()
                .any(|bucket| bucket.trim().is_empty() || bucket.trim() != bucket)
        {
            return Err(GenerationFetchError::Rejected(
                "at least one valid allowed S3 bucket is required".to_string(),
            ));
        }
        if config.max_bundle_size_bytes == 0 {
            return Err(GenerationFetchError::Rejected(
                "max_bundle_size_bytes must be greater than zero".to_string(),
            ));
        }
        create_private_download_directory(&config.download_directory)?;
        config.download_directory = fs::canonicalize(&config.download_directory)?;
        Ok(Self {
            client,
            config,
            deadlines: FetchDeadlines::default(),
        })
    }

    pub fn for_seaweedfs(
        seaweedfs: &akidb_common::config::SeaweedFsConfig,
        region: impl Into<String>,
        config: S3GenerationBundleFetcherConfig,
    ) -> Result<Self, GenerationFetchError> {
        let endpoint = seaweedfs
            .normalized_endpoint()
            .map_err(GenerationFetchError::Rejected)?;
        let (access_key, secret_key) = seaweedfs
            .credentials()
            .map_err(GenerationFetchError::Rejected)?;
        let credentials = Credentials::new(access_key, secret_key, None, None, "akidb-generation");
        let sdk_config = S3ClientConfigBuilder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.into()))
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .force_path_style(true)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(3))
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(30))
                    .operation_attempt_timeout(Duration::from_secs(10))
                    .build(),
            )
            .build();
        Self::new(aws_sdk_s3::Client::from_conf(sdk_config), config)
    }

    fn address(
        &self,
        reference: &ImmutableObjectReference,
    ) -> Result<S3ObjectAddress, GenerationFetchError> {
        reference
            .validate()
            .map_err(|error| GenerationFetchError::Rejected(error.to_string()))?;
        parse_s3_address(reference, &self.config)
    }
}

#[cfg(feature = "generation-s3")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct S3ObjectAddress {
    bucket: String,
    key: String,
    version_id: Option<String>,
}

#[cfg(feature = "generation-s3")]
#[async_trait]
impl GenerationBundleFetcher for S3GenerationBundleFetcher {
    async fn fetch(
        &self,
        reference: &ImmutableObjectReference,
    ) -> Result<FetchedGenerationBundle, GenerationFetchError> {
        tokio::time::timeout(
            self.deadlines.total.saturating_add(Duration::from_secs(
                (reference.size_bytes / (1024 * 1024)).min(86_400),
            )),
            self.fetch_inner(reference),
        )
        .await
        .map_err(|_| GenerationFetchError::Timeout("total fetch"))?
    }
}

#[cfg(feature = "generation-s3")]
impl S3GenerationBundleFetcher {
    async fn fetch_inner(
        &self,
        reference: &ImmutableObjectReference,
    ) -> Result<FetchedGenerationBundle, GenerationFetchError> {
        if reference.size_bytes > self.config.max_bundle_size_bytes {
            return Err(GenerationFetchError::Rejected(format!(
                "bundle size {} exceeds configured maximum {}",
                reference.size_bytes, self.config.max_bundle_size_bytes
            )));
        }
        let address = self.address(reference)?;
        let mut head = self
            .client
            .head_object()
            .bucket(&address.bucket)
            .key(&address.key);
        if let Some(version_id) = &address.version_id {
            head = head.version_id(version_id);
        }
        let head = tokio::time::timeout(self.deadlines.request, head.send())
            .await
            .map_err(|_| GenerationFetchError::Timeout("HEAD"))?
            .map_err(|error| match &error {
                aws_sdk_s3::error::SdkError::TimeoutError(_) => {
                    GenerationFetchError::Timeout("HEAD")
                }
                aws_sdk_s3::error::SdkError::DispatchFailure(failure) if failure.is_timeout() => {
                    GenerationFetchError::Timeout("HEAD")
                }
                _ => remote_error("HEAD", error.raw_response().map(|r| r.status().as_u16())),
            })?;
        let head_length = checked_content_length(head.content_length())?;
        if head_length != reference.size_bytes {
            return Err(GenerationFetchError::Rejected(format!(
                "S3 object size changed: manifest {}, HEAD {}",
                reference.size_bytes, head_length
            )));
        }

        let mut get = self
            .client
            .get_object()
            .bucket(&address.bucket)
            .key(&address.key);
        if let Some(version_id) = &address.version_id {
            get = get.version_id(version_id);
        }
        let response = tokio::time::timeout(self.deadlines.request, get.send())
            .await
            .map_err(|_| GenerationFetchError::Timeout("GET"))?
            .map_err(|error| match &error {
                aws_sdk_s3::error::SdkError::TimeoutError(_) => {
                    GenerationFetchError::Timeout("GET")
                }
                aws_sdk_s3::error::SdkError::DispatchFailure(failure) if failure.is_timeout() => {
                    GenerationFetchError::Timeout("GET")
                }
                _ => remote_error("GET", error.raw_response().map(|r| r.status().as_u16())),
            })?;
        let response_length = checked_content_length(response.content_length())?;
        if response_length != reference.size_bytes {
            return Err(GenerationFetchError::Rejected(format!(
                "S3 response size changed: manifest {}, GET {}",
                reference.size_bytes, response_length
            )));
        }

        let path = self
            .config
            .download_directory
            .join(format!(".generation-{}.partial", uuid::Uuid::new_v4()));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        // Own cleanup before the first await: timeout, task abort, stream errors,
        // and successful handoff all retain the same exact-file ownership.
        let fetched = FetchedGenerationBundle {
            path,
            remove_on_drop: true,
        };
        let mut file = tokio::fs::File::from_std(file);
        let mut body = response.body;
        let mut observed = 0u64;
        let write_result: Result<(), GenerationFetchError> = async {
            while let Some(bytes) = tokio::time::timeout(self.deadlines.idle, body.try_next())
                .await
                .map_err(|_| GenerationFetchError::Timeout("body idle"))?
                .map_err(|_| {
                    GenerationFetchError::Transport("S3 response stream failed".to_string())
                })?
            {
                let chunk_len = u64::try_from(bytes.len()).map_err(|_| {
                    GenerationFetchError::Rejected(
                        "S3 response chunk cannot fit the platform".to_string(),
                    )
                })?;
                observed = observed.checked_add(chunk_len).ok_or_else(|| {
                    GenerationFetchError::Rejected("S3 response size overflow".to_string())
                })?;
                if observed > reference.size_bytes || observed > self.config.max_bundle_size_bytes {
                    return Err(GenerationFetchError::Rejected(
                        "S3 response exceeded the authorized byte count".to_string(),
                    ));
                }
                file.write_all(&bytes).await?;
            }
            if observed != reference.size_bytes {
                return Err(GenerationFetchError::Rejected(format!(
                    "S3 response was truncated: expected {}, observed {}",
                    reference.size_bytes, observed
                )));
            }
            file.flush().await?;
            file.sync_all().await?;
            Ok(())
        }
        .await;
        drop(file);
        write_result?;
        Ok(fetched)
    }
}

#[cfg(feature = "generation-s3")]
fn remote_error(operation: &'static str, status: Option<u16>) -> GenerationFetchError {
    // Never retain SDK diagnostics: they can contain signed URLs or credentials.
    match status {
        Some(status) => GenerationFetchError::Remote { operation, status },
        None => GenerationFetchError::Transport(format!("S3 {operation} transport failed")),
    }
}

#[cfg(feature = "generation-s3")]
fn parse_s3_address(
    reference: &ImmutableObjectReference,
    config: &S3GenerationBundleFetcherConfig,
) -> Result<S3ObjectAddress, GenerationFetchError> {
    let uri = Url::parse(&reference.uri)
        .map_err(|_| GenerationFetchError::Rejected("invalid S3 URI".to_string()))?;
    if uri.scheme() != "s3" {
        return Err(GenerationFetchError::Unauthorized(
            "only s3:// generation objects are enabled".to_string(),
        ));
    }
    let bucket = uri
        .host_str()
        .ok_or_else(|| GenerationFetchError::Rejected("S3 bucket is missing".to_string()))?
        .to_string();
    if !config.allowed_buckets.contains(&bucket) {
        return Err(GenerationFetchError::Unauthorized(format!(
            "S3 bucket {bucket} is not allowed"
        )));
    }
    let encoded_key = uri.path().strip_prefix('/').unwrap_or(uri.path());
    let key = percent_encoding::percent_decode_str(encoded_key)
        .decode_utf8()
        .map_err(|_| GenerationFetchError::Rejected("S3 key is not valid UTF-8".to_string()))?
        .into_owned();
    if key.is_empty() || key.chars().any(char::is_control) {
        return Err(GenerationFetchError::Rejected(
            "S3 object key is invalid".to_string(),
        ));
    }

    let mut version_id = None;
    for (name, value) in uri.query_pairs() {
        if name != "versionId" || version_id.is_some() || value.trim().is_empty() {
            return Err(GenerationFetchError::Rejected(
                "S3 URI permits at most one non-empty versionId query parameter".to_string(),
            ));
        }
        version_id = Some(value.into_owned());
    }
    if config.require_version_or_digest_key
        && version_id.is_none()
        && !key.contains(&reference.sha256)
    {
        return Err(GenerationFetchError::Unauthorized(
            "unversioned S3 key must contain the authorized SHA-256 digest".to_string(),
        ));
    }
    Ok(S3ObjectAddress {
        bucket,
        key,
        version_id,
    })
}

#[cfg(feature = "generation-s3")]
fn checked_content_length(length: Option<i64>) -> Result<u64, GenerationFetchError> {
    let length = length.ok_or_else(|| {
        GenerationFetchError::Rejected("S3 response omitted content length".to_string())
    })?;
    u64::try_from(length)
        .map_err(|_| GenerationFetchError::Rejected("S3 content length is negative".to_string()))
}

#[cfg(feature = "generation-s3")]
fn create_private_download_directory(path: &Path) -> Result<(), GenerationFetchError> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() {
            return Err(GenerationFetchError::Rejected(format!(
                "download directory is a symbolic link: {}",
                path.display()
            )));
        }
        if !metadata.is_dir() {
            return Err(GenerationFetchError::Rejected(format!(
                "download path is not a directory: {}",
                path.display()
            )));
        }
    } else {
        fs::create_dir_all(path)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn validate_regular_file(path: &Path) -> Result<(), GenerationFetchError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(GenerationFetchError::Rejected(format!(
            "symbolic link is not allowed at {}",
            path.display()
        )));
    }
    if !metadata.is_file() {
        return Err(GenerationFetchError::Rejected(format!(
            "expected a regular file at {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "generation-s3")]
    use std::collections::HashSet;

    #[test]
    fn temporary_bundle_is_deleted_on_drop() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("bundle");
        std::fs::write(&path, b"bundle").unwrap();
        let fetched = FetchedGenerationBundle::temporary(path.clone()).unwrap();
        assert_eq!(fetched.open().unwrap().metadata().unwrap().len(), 6);
        drop(fetched);
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_link_is_rejected() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target");
        let link = directory.path().join("link");
        std::fs::write(&target, b"bundle").unwrap();
        symlink(&target, &link).unwrap();
        let error = FetchedGenerationBundle::retained(link).unwrap_err();
        assert!(error.to_string().contains("symbolic link"));
    }

    #[cfg(feature = "generation-s3")]
    fn s3_config(directory: &Path) -> S3GenerationBundleFetcherConfig {
        S3GenerationBundleFetcherConfig {
            allowed_buckets: HashSet::from(["knowledge".to_string()]),
            download_directory: directory.to_path_buf(),
            max_bundle_size_bytes: 1024,
            require_version_or_digest_key: true,
        }
    }

    #[cfg(feature = "generation-s3")]
    fn reference(uri: String) -> ImmutableObjectReference {
        ImmutableObjectReference {
            uri,
            sha256: "a".repeat(64),
            size_bytes: 100,
        }
    }

    #[cfg(feature = "generation-s3")]
    #[test]
    fn s3_uri_requires_allowed_bucket_and_immutable_identity() {
        let directory = tempfile::tempdir().unwrap();
        let config = s3_config(directory.path());
        let versioned = reference("s3://knowledge/generations/bundle?versionId=v1".to_string());
        assert_eq!(
            parse_s3_address(&versioned, &config).unwrap(),
            S3ObjectAddress {
                bucket: "knowledge".to_string(),
                key: "generations/bundle".to_string(),
                version_id: Some("v1".to_string()),
            }
        );

        let digest_key = reference(format!(
            "s3://knowledge/generations/{}/bundle",
            "a".repeat(64)
        ));
        assert!(parse_s3_address(&digest_key, &config).is_ok());
        assert!(parse_s3_address(
            &reference("s3://other/generations/bundle?versionId=v1".to_string()),
            &config
        )
        .unwrap_err()
        .to_string()
        .contains("not allowed"));
        assert!(parse_s3_address(
            &reference("s3://knowledge/generations/mutable".to_string()),
            &config
        )
        .unwrap_err()
        .to_string()
        .contains("unversioned"));
    }

    #[cfg(feature = "generation-s3")]
    #[test]
    fn s3_uri_rejects_unexpected_or_duplicate_query_parameters() {
        let directory = tempfile::tempdir().unwrap();
        let config = s3_config(directory.path());
        for uri in [
            "s3://knowledge/key?token=secret",
            "s3://knowledge/key?versionId=v1&versionId=v2",
            "s3://knowledge/key?versionId=",
        ] {
            assert!(parse_s3_address(&reference(uri.to_string()), &config).is_err());
        }
    }

    #[cfg(feature = "generation-s3")]
    #[test]
    fn seaweedfs_credentials_are_required() {
        let seaweedfs = akidb_common::config::SeaweedFsConfig {
            endpoint: "http://seaweedfs.internal:8333".to_string(),
            bucket: "knowledge".to_string(),
            ..Default::default()
        };

        // SeaweedFS serves every operation anonymously without an identity
        // configuration, so the fetcher must refuse to be built credential-less.
        assert!(seaweedfs.credentials().is_err());
    }
    #[cfg(feature = "generation-s3")]
    async fn scripted_fetcher(
        directory: &Path,
        responses: Vec<(String, Duration)>,
    ) -> (S3GenerationBundleFetcher, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            for (response, delay) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 8192];
                let _ = stream.read(&mut request).await;
                if !response.is_empty() {
                    let _ = stream.write_all(response.as_bytes()).await;
                }
                tokio::time::sleep(delay).await;
            }
        });
        let config = S3ClientConfigBuilder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint)
            .credentials_provider(Credentials::new(
                "test-key",
                "test-secret",
                None,
                None,
                "test",
            ))
            .force_path_style(true)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::standard().with_max_attempts(1))
            .build();
        let fetcher = S3GenerationBundleFetcher::new(
            aws_sdk_s3::Client::from_conf(config),
            s3_config(directory),
        )
        .unwrap();
        (fetcher, task)
    }

    #[cfg(feature = "generation-s3")]
    fn tiny_reference() -> ImmutableObjectReference {
        let mut value = reference(format!("s3://knowledge/{}/bundle", "a".repeat(64)));
        value.size_bytes = 2;
        value
    }

    #[cfg(feature = "generation-s3")]
    fn ok_headers() -> String {
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n".to_string()
    }

    #[cfg(feature = "generation-s3")]
    #[tokio::test]
    async fn s3_status_is_classified_without_sdk_secrets() {
        for status in [403, 404, 429, 503] {
            let directory = tempfile::tempdir().unwrap();
            let (fetcher, server) = scripted_fetcher(directory.path(), vec![(format!("HTTP/1.1 {status} Failure\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"), Duration::ZERO)]).await;
            let error = fetcher.fetch(&tiny_reference()).await.unwrap_err();
            assert!(
                matches!(error, GenerationFetchError::Remote { operation: "HEAD", status: actual } if actual == status)
            );
            assert!(!error.to_string().contains("test-secret"));
            assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
            server.abort();
        }
    }

    #[cfg(feature = "generation-s3")]
    #[tokio::test]
    async fn request_and_body_deadlines_remove_partial_files() {
        for mode in ["request", "idle", "total", "truncated"] {
            let directory = tempfile::tempdir().unwrap();
            let responses = if mode == "request" {
                vec![(String::new(), Duration::from_secs(10))]
            } else {
                vec![
                    (ok_headers(), Duration::ZERO),
                    (
                        format!("{}x", ok_headers()),
                        if mode == "truncated" {
                            Duration::ZERO
                        } else {
                            Duration::from_secs(10)
                        },
                    ),
                ]
            };
            let (mut fetcher, server) = scripted_fetcher(directory.path(), responses).await;
            fetcher.deadlines = FetchDeadlines {
                request: if mode == "request" {
                    Duration::from_millis(100)
                } else {
                    Duration::from_secs(2)
                },
                idle: if mode == "idle" {
                    Duration::from_millis(100)
                } else {
                    Duration::from_secs(2)
                },
                total: if mode == "total" {
                    Duration::from_millis(100)
                } else {
                    Duration::from_secs(5)
                },
            };
            let error = fetcher.fetch(&tiny_reference()).await.unwrap_err();
            if mode == "truncated" {
                assert!(matches!(
                    error,
                    GenerationFetchError::Transport(_) | GenerationFetchError::Rejected(_)
                ));
            } else {
                assert!(
                    matches!(error, GenerationFetchError::Timeout(_)),
                    "{mode}: {error}"
                );
            }
            assert_eq!(
                std::fs::read_dir(directory.path()).unwrap().count(),
                0,
                "{mode}"
            );
            server.abort();
        }
    }

    #[cfg(feature = "generation-s3")]
    #[tokio::test]
    async fn sdk_request_timeout_preserves_timeout_classification() {
        let directory = tempfile::tempdir().unwrap();
        let (mut fetcher, server) = scripted_fetcher(
            directory.path(),
            vec![(String::new(), Duration::from_secs(5))],
        )
        .await;
        fetcher.client = aws_sdk_s3::Client::from_conf(
            fetcher
                .client
                .config()
                .to_builder()
                .timeout_config(
                    aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                        .operation_timeout(Duration::from_millis(50))
                        .build(),
                )
                .build(),
        );
        assert!(matches!(
            fetcher.fetch(&tiny_reference()).await.unwrap_err(),
            GenerationFetchError::Timeout("HEAD")
        ));
        server.abort();
    }

    #[cfg(feature = "generation-s3")]
    #[tokio::test]
    async fn abort_during_body_cleans_partial_download() {
        let directory = tempfile::tempdir().unwrap();
        let (fetcher, server) = scripted_fetcher(
            directory.path(),
            vec![
                (ok_headers(), Duration::ZERO),
                (format!("{}x", ok_headers()), Duration::from_secs(10)),
            ],
        )
        .await;
        let download = tokio::spawn(async move { fetcher.fetch(&tiny_reference()).await });
        tokio::time::timeout(Duration::from_secs(3), async {
            while std::fs::read_dir(directory.path()).unwrap().count() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        download.abort();
        assert!(download.await.unwrap_err().is_cancelled());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        server.abort();
    }

    #[cfg(feature = "generation-s3")]
    #[tokio::test]
    async fn successful_download_retains_file_until_consumer_drops_it() {
        let directory = tempfile::tempdir().unwrap();
        let (fetcher, server) = scripted_fetcher(
            directory.path(),
            vec![
                (ok_headers(), Duration::ZERO),
                (format!("{}ok", ok_headers()), Duration::ZERO),
            ],
        )
        .await;
        let fetched = fetcher.fetch(&tiny_reference()).await.unwrap();
        assert_eq!(std::fs::read(fetched.path()).unwrap(), b"ok");
        drop(fetched);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        server.abort();
    }
}
