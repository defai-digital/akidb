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
#[cfg(feature = "generation-s3")]
use sha2::{Digest, Sha256};
use thiserror::Error;
#[cfg(feature = "generation-s3")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

/// A private digest-addressed mirror for edge nodes with no object-store route.
#[cfg(feature = "generation-s3")]
#[derive(Debug, Clone)]
pub struct LocalMirrorGenerationBundleFetcherConfig {
    pub mirror_directory: PathBuf,
    pub download_directory: PathBuf,
    pub allowed_buckets: HashSet<String>,
    pub max_bundle_size_bytes: u64,
    pub require_version_or_digest_key: bool,
}

#[cfg(feature = "generation-s3")]
pub struct LocalMirrorGenerationBundleFetcher {
    config: LocalMirrorGenerationBundleFetcherConfig,
    bundle_directory: PathBuf,
}

#[cfg(feature = "generation-s3")]
impl LocalMirrorGenerationBundleFetcher {
    pub fn new(
        mut config: LocalMirrorGenerationBundleFetcherConfig,
    ) -> Result<Self, GenerationFetchError> {
        validate_object_policy(&config.allowed_buckets, config.max_bundle_size_bytes)?;
        create_private_download_directory(&config.mirror_directory)?;
        let digest_directory = config.mirror_directory.join("sha256");
        create_private_download_directory(&digest_directory)?;
        create_private_download_directory(&config.download_directory)?;
        let bundle_directory = fs::canonicalize(digest_directory)?;
        config.mirror_directory = fs::canonicalize(&config.mirror_directory)?;
        config.download_directory = fs::canonicalize(&config.download_directory)?;
        if config
            .download_directory
            .starts_with(&config.mirror_directory)
            || config
                .mirror_directory
                .starts_with(&config.download_directory)
        {
            return Err(GenerationFetchError::Rejected(
                "download directory must not overlap the immutable bundle mirror".to_string(),
            ));
        }
        Ok(Self {
            config,
            bundle_directory,
        })
    }
}

#[cfg(feature = "generation-s3")]
#[async_trait]
impl GenerationBundleFetcher for LocalMirrorGenerationBundleFetcher {
    async fn fetch(
        &self,
        reference: &ImmutableObjectReference,
    ) -> Result<FetchedGenerationBundle, GenerationFetchError> {
        reference
            .validate()
            .map_err(|error| GenerationFetchError::Rejected(error.to_string()))?;
        if reference.size_bytes > self.config.max_bundle_size_bytes {
            return Err(GenerationFetchError::Rejected(format!(
                "bundle size {} exceeds configured maximum {}",
                reference.size_bytes, self.config.max_bundle_size_bytes
            )));
        }
        parse_s3_address(
            reference,
            &self.config.allowed_buckets,
            self.config.require_version_or_digest_key,
        )?;

        let path = self.bundle_directory.join(&reference.sha256);
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                GenerationFetchError::Unavailable(
                    "bundle digest is absent from local mirror".to_string(),
                )
            } else {
                GenerationFetchError::Io(error)
            }
        })?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(GenerationFetchError::Rejected(
                "local mirror bundle must be a regular file without symlinks".to_string(),
            ));
        }
        if metadata.len() != reference.size_bytes {
            return Err(GenerationFetchError::Rejected(format!(
                "local mirror bundle size changed: manifest {}, file {}",
                reference.size_bytes,
                metadata.len()
            )));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let source = options.open(&path)?;
        if !source.metadata()?.is_file() {
            return Err(GenerationFetchError::Rejected(
                "local mirror bundle changed during open".to_string(),
            ));
        }
        let mut source = tokio::fs::File::from_std(source);
        let download_path = self
            .config
            .download_directory
            .join(format!(".generation-{}.partial", uuid::Uuid::new_v4()));
        let destination = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&download_path)?;
        let mut destination = tokio::fs::File::from_std(destination);
        let copied: Result<(), GenerationFetchError> = async {
            let mut digest = Sha256::new();
            let mut observed = 0u64;
            let mut buffer = [0u8; 64 * 1024];
            loop {
                let count = source.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                observed = observed.checked_add(count as u64).ok_or_else(|| {
                    GenerationFetchError::Rejected("local mirror bundle size overflow".to_string())
                })?;
                if observed > reference.size_bytes || observed > self.config.max_bundle_size_bytes {
                    return Err(GenerationFetchError::Rejected(
                        "local mirror bundle exceeded the authorized byte count".to_string(),
                    ));
                }
                digest.update(&buffer[..count]);
                destination.write_all(&buffer[..count]).await?;
            }
            if observed != reference.size_bytes
                || format!("{:x}", digest.finalize()) != reference.sha256
            {
                return Err(GenerationFetchError::Rejected(
                    "local mirror bundle size or SHA-256 does not match publication".to_string(),
                ));
            }
            destination.flush().await?;
            destination.sync_all().await?;
            Ok(())
        }
        .await;
        drop(destination);
        if let Err(error) = copied {
            let _ = tokio::fs::remove_file(&download_path).await;
            return Err(error);
        }
        FetchedGenerationBundle::temporary(download_path)
    }
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
}

#[cfg(feature = "generation-s3")]
impl S3GenerationBundleFetcher {
    pub fn new(
        client: aws_sdk_s3::Client,
        mut config: S3GenerationBundleFetcherConfig,
    ) -> Result<Self, GenerationFetchError> {
        validate_object_policy(&config.allowed_buckets, config.max_bundle_size_bytes)?;
        create_private_download_directory(&config.download_directory)?;
        config.download_directory = fs::canonicalize(&config.download_directory)?;
        Ok(Self { client, config })
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
        parse_s3_address(
            reference,
            &self.config.allowed_buckets,
            self.config.require_version_or_digest_key,
        )
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
    allowed_buckets: &HashSet<String>,
    require_version_or_digest_key: bool,
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
    if !allowed_buckets.contains(&bucket) {
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
    if require_version_or_digest_key && version_id.is_none() && !key.contains(&reference.sha256) {
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
fn validate_object_policy(
    allowed_buckets: &HashSet<String>,
    max_bundle_size_bytes: u64,
) -> Result<(), GenerationFetchError> {
    if allowed_buckets.is_empty()
        || allowed_buckets
            .iter()
            .any(|bucket| bucket.trim().is_empty() || bucket.trim() != bucket)
    {
        return Err(GenerationFetchError::Rejected(
            "at least one valid allowed S3 bucket is required".to_string(),
        ));
    }
    if max_bundle_size_bytes == 0 {
        return Err(GenerationFetchError::Rejected(
            "max_bundle_size_bytes must be greater than zero".to_string(),
        ));
    }
    Ok(())
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
    fn parse_with_config(
        reference: &ImmutableObjectReference,
        config: &S3GenerationBundleFetcherConfig,
    ) -> Result<S3ObjectAddress, GenerationFetchError> {
        parse_s3_address(
            reference,
            &config.allowed_buckets,
            config.require_version_or_digest_key,
        )
    }

    #[cfg(feature = "generation-s3")]
    #[test]
    fn s3_uri_requires_allowed_bucket_and_immutable_identity() {
        let directory = tempfile::tempdir().unwrap();
        let config = s3_config(directory.path());
        let versioned = reference("s3://knowledge/generations/bundle?versionId=v1".to_string());
        assert_eq!(
            parse_with_config(&versioned, &config).unwrap(),
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
        assert!(parse_with_config(&digest_key, &config).is_ok());
        assert!(parse_with_config(
            &reference("s3://other/generations/bundle?versionId=v1".to_string()),
            &config
        )
        .unwrap_err()
        .to_string()
        .contains("not allowed"));
        assert!(parse_with_config(
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
            assert!(parse_with_config(&reference(uri.to_string()), &config).is_err());
        }
    }

    #[cfg(feature = "generation-s3")]
    fn mirror_config(directory: &Path) -> LocalMirrorGenerationBundleFetcherConfig {
        LocalMirrorGenerationBundleFetcherConfig {
            mirror_directory: directory.join("mirror"),
            download_directory: directory.join("downloads"),
            allowed_buckets: HashSet::from(["knowledge".to_string()]),
            max_bundle_size_bytes: 1024,
            require_version_or_digest_key: true,
        }
    }

    #[cfg(feature = "generation-s3")]
    #[test]
    fn local_mirror_rejects_overlapping_download_path() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = mirror_config(directory.path());
        config.download_directory = config.mirror_directory.join("downloads");
        assert!(LocalMirrorGenerationBundleFetcher::new(config).is_err());
    }

    #[cfg(feature = "generation-s3")]
    #[tokio::test]
    async fn local_mirror_fetches_only_the_authorized_digest() {
        let directory = tempfile::tempdir().unwrap();
        let config = mirror_config(directory.path());
        let fetcher = LocalMirrorGenerationBundleFetcher::new(config.clone()).unwrap();
        let bytes = b"immutable logical bundle";
        let sha256 = format!("{:x}", Sha256::digest(bytes));
        let mirror_path = config.mirror_directory.join("sha256").join(&sha256);
        std::fs::write(&mirror_path, bytes).unwrap();
        let reference = ImmutableObjectReference {
            uri: format!("s3://knowledge/generations/{sha256}/bundle"),
            sha256: sha256.clone(),
            size_bytes: bytes.len() as u64,
        };

        let fetched = fetcher.fetch(&reference).await.unwrap();
        assert_eq!(std::fs::read(fetched.path()).unwrap(), bytes);
        assert_ne!(fetched.path(), mirror_path);
        std::fs::write(&mirror_path, b"changed after fetch").unwrap();
        assert_eq!(std::fs::read(fetched.path()).unwrap(), bytes);
        assert!(fetcher.fetch(&reference).await.is_err());
    }

    #[cfg(feature = "generation-s3")]
    #[tokio::test]
    async fn local_mirror_rejects_missing_tampered_and_unauthorized_bundles() {
        let directory = tempfile::tempdir().unwrap();
        let config = mirror_config(directory.path());
        let fetcher = LocalMirrorGenerationBundleFetcher::new(config.clone()).unwrap();
        let bytes = b"immutable logical bundle";
        let sha256 = format!("{:x}", Sha256::digest(bytes));
        let mirror_path = config.mirror_directory.join("sha256").join(&sha256);
        let reference = ImmutableObjectReference {
            uri: format!("s3://knowledge/generations/{sha256}/bundle"),
            sha256: sha256.clone(),
            size_bytes: bytes.len() as u64,
        };

        assert!(matches!(
            fetcher.fetch(&reference).await,
            Err(GenerationFetchError::Unavailable(_))
        ));
        std::fs::write(&mirror_path, b"different logical bundle").unwrap();
        assert!(matches!(
            fetcher.fetch(&reference).await,
            Err(GenerationFetchError::Rejected(_))
        ));
        std::fs::write(&mirror_path, bytes).unwrap();
        let wrong_bucket = ImmutableObjectReference {
            uri: format!("s3://other/generations/{sha256}/bundle"),
            ..reference.clone()
        };
        assert!(matches!(
            fetcher.fetch(&wrong_bucket).await,
            Err(GenerationFetchError::Unauthorized(_))
        ));
        #[cfg(unix)]
        {
            std::fs::remove_file(&mirror_path).unwrap();
            std::os::unix::fs::symlink(directory.path().join("target"), &mirror_path).unwrap();
            assert!(matches!(
                fetcher.fetch(&reference).await,
                Err(GenerationFetchError::Rejected(_))
            ));
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
}
