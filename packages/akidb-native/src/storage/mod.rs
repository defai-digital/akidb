//! Storage backend — file-system backed object storage.
//!
//!   - `put_object` / `get_object` — store and retrieve blobs
//!   - Optional SHA-256 sidecar checksums on write/read

use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::error::{AkiDbError, Result};

/// Configuration for the local file-system backend.
pub struct LocalFsOptions {
    pub root_dir: PathBuf,
    pub checksum_on_write: bool,
    pub checksum_on_read: bool,
}

impl Default for LocalFsOptions {
    fn default() -> Self {
        Self {
            root_dir: PathBuf::from("."),
            checksum_on_write: true,
            checksum_on_read: true,
        }
    }
}

/// File-system backed storage for segment binaries, WAL files, etc.
pub struct LocalFsBackend {
    root: PathBuf,
    checksum_on_write: bool,
    checksum_on_read: bool,
}

impl LocalFsBackend {
    pub fn new(opts: LocalFsOptions) -> Self {
        let root = fs::canonicalize(&opts.root_dir).unwrap_or_else(|_| opts.root_dir.clone());
        Self {
            root,
            checksum_on_write: opts.checksum_on_write,
            checksum_on_read: opts.checksum_on_read,
        }
    }

    /// Convenience constructor with defaults.
    pub fn open(root_dir: impl Into<PathBuf>) -> Result<Self> {
        let root_dir = root_dir.into();
        fs::create_dir_all(&root_dir)?;
        Ok(Self::new(LocalFsOptions {
            root_dir,
            checksum_on_write: true,
            checksum_on_read: true,
        }))
    }

    // ── Public API ──────────────────────────────────────────────────────────

    /// Persist an opaque blob under `key`. Overwrites if the key already exists.
    pub fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let abs_path = self.key_to_path(key)?;
        if let Some(parent) = abs_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&abs_path, data)?;

        if self.checksum_on_write {
            let digest = sha256_hex(data);
            fs::write(format!("{}.sha256", abs_path.display()), digest)?;
        }
        Ok(())
    }

    /// Delete the blob stored under `key`. No-ops silently if the object does not exist.
    /// Also removes the SHA-256 sidecar file if present.
    pub fn delete_object(&self, key: &str) -> Result<()> {
        let abs_path = self.key_to_path(key)?;
        if abs_path.exists() {
            fs::remove_file(&abs_path)?;
        }
        // Best-effort: remove sidecar even if the main file was already gone.
        let sidecar = PathBuf::from(format!("{}.sha256", abs_path.display()));
        if sidecar.exists() {
            let _ = fs::remove_file(sidecar);
        }
        Ok(())
    }

    /// Retrieve the blob stored under `key`.
    pub fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let abs_path = self.key_to_path(key)?;
        let data = fs::read(&abs_path).map_err(|_| {
            AkiDbError::Storage(format!("Object not found: \"{key}\""))
        })?;

        if self.checksum_on_read {
            self.validate_checksum(&abs_path, &data, key)?;
        }
        Ok(data)
    }

    // ── Internal helpers ────────────────────────────────────────────────────

    /// Convert a logical key to an absolute path, guarding against path traversal.
    fn key_to_path(&self, key: &str) -> Result<PathBuf> {
        let resolved = self.root.join(key);
        // Canonicalize may fail if path doesn't exist yet — use starts_with on the
        // joined path components instead.
        let normalized = normalize_path(&resolved);
        let root_normalized = normalize_path(&self.root);
        if !normalized.starts_with(&root_normalized) {
            return Err(AkiDbError::Storage(format!(
                "Key \"{key}\" resolves outside root directory"
            )));
        }
        Ok(resolved)
    }

    fn validate_checksum(&self, abs_path: &Path, data: &[u8], _key: &str) -> Result<()> {
        let sidecar = format!("{}.sha256", abs_path.display());
        let expected = match fs::read_to_string(&sidecar) {
            Ok(s) => s.trim().to_string(),
            Err(_) => return Ok(()), // no sidecar — nothing to validate
        };
        let actual = sha256_hex(data);
        if actual != expected {
            return Err(AkiDbError::ChecksumMismatch { expected, actual });
        }
        Ok(())
    }

}

// ── Free functions ──────────────────────────────────────────────────────────

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Normalize a path by resolving `.` and `..` without hitting the filesystem.
fn normalize_path(path: &Path) -> PathBuf {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                components.pop();
            }
            std::path::Component::CurDir => {}
            c => components.push(c),
        }
    }
    components.iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_backend() -> (tempfile::TempDir, LocalFsBackend) {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFsBackend::open(dir.path()).unwrap();
        (dir, backend)
    }

    #[test]
    fn put_and_get() {
        let (_dir, backend) = test_backend();
        backend.put_object("test/a.bin", b"hello world").unwrap();
        let data = backend.get_object("test/a.bin").unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn get_missing_returns_error() {
        let (_dir, backend) = test_backend();
        let result = backend.get_object("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn checksum_validates_on_read() {
        let (_dir, backend) = test_backend();
        backend.put_object("chk.bin", b"original").unwrap();

        // Tamper with the data file but not the sidecar.
        let path = backend.root.join("chk.bin");
        fs::write(&path, b"tampered").unwrap();

        let result = backend.get_object("chk.bin");
        assert!(matches!(result, Err(AkiDbError::ChecksumMismatch { .. })));
    }

    #[test]
    fn path_traversal_blocked() {
        let (_dir, backend) = test_backend();
        let result = backend.put_object("../../etc/passwd", b"nope");
        assert!(result.is_err());
    }
}
