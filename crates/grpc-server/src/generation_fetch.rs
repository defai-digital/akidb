//! Authorized immutable-object fetch boundary for generation publication.

#[cfg(feature = "generation-s3")]
use std::collections::HashSet;
#[cfg(feature = "generation-s3")]
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

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

/// Bounded, streaming S3/MinIO fetcher using an already-configured SDK client.
///
/// The SDK client fixes the endpoint and credentials. This layer additionally
/// restricts buckets, URI query parameters, object immutability, and bytes
/// written to a private local directory.
#[cfg(feature = "generation-s3")]
pub struct S3GenerationBundleFetcher {
    client: aws_sdk_s3::Client,
    config: S3GenerationBundleFetcherConfig,
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
        Ok(Self { client, config })
    }

    pub fn for_minio(
        minio: &akidb_common::config::MinioConfig,
        region: impl Into<String>,
        config: S3GenerationBundleFetcherConfig,
    ) -> Result<Self, GenerationFetchError> {
        let endpoint = normalized_minio_endpoint(&minio.endpoint, minio.use_ssl)?;
        let credentials = Credentials::new(
            &minio.access_key,
            &minio.secret_key,
            None,
            None,
            "akidb-generation",
        );
        let sdk_config = S3ClientConfigBuilder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.into()))
            .endpoint_url(endpoint)
            .credentials_provider(credentials)
            .force_path_style(true)
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
        let head = head
            .send()
            .await
            .map_err(|_| GenerationFetchError::Unavailable("S3 object HEAD failed".to_string()))?;
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
        let response = get
            .send()
            .await
            .map_err(|_| GenerationFetchError::Unavailable("S3 object GET failed".to_string()))?;
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
        let mut file = tokio::fs::File::from_std(file);
        let mut body = response.body;
        let mut observed = 0u64;
        let write_result: Result<(), GenerationFetchError> = async {
            while let Some(bytes) = body.try_next().await.map_err(|_| {
                GenerationFetchError::Transport("S3 response stream failed".to_string())
            })? {
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
        if let Err(error) = write_result {
            let _ = tokio::fs::remove_file(&path).await;
            return Err(error);
        }
        FetchedGenerationBundle::temporary(path)
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

#[cfg(feature = "generation-s3")]
fn normalized_minio_endpoint(
    endpoint: &str,
    use_ssl: bool,
) -> Result<String, GenerationFetchError> {
    let endpoint = endpoint.trim().trim_end_matches('/');
    if endpoint.is_empty() {
        return Err(GenerationFetchError::Rejected(
            "MinIO endpoint must not be empty".to_string(),
        ));
    }
    let endpoint = if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("{}://{endpoint}", if use_ssl { "https" } else { "http" })
    };
    let parsed = Url::parse(&endpoint)
        .map_err(|_| GenerationFetchError::Rejected("invalid MinIO endpoint".to_string()))?;
    let required_scheme = if use_ssl { "https" } else { "http" };
    if parsed.scheme() != required_scheme
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || !matches!(parsed.path(), "" | "/")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(GenerationFetchError::Rejected(format!(
            "MinIO endpoint must be a credential-free {required_scheme} origin"
        )));
    }
    Ok(endpoint)
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
    fn minio_endpoint_scheme_must_match_tls_configuration() {
        assert_eq!(
            normalized_minio_endpoint("minio.internal:9000", true).unwrap(),
            "https://minio.internal:9000"
        );
        assert!(normalized_minio_endpoint("http://minio.internal:9000", true).is_err());
        assert!(normalized_minio_endpoint("https://user:secret@minio.internal", true).is_err());
    }
}
