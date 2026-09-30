//! Atomic import of immutable logical bundles into a digest-addressed mirror.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Subcommand;
use sha2::{Digest, Sha256};

#[derive(clap::Args, Debug)]
pub struct BundleArgs {
    #[command(subcommand)]
    command: BundleCommand,
}

#[derive(Subcommand, Debug)]
enum BundleCommand {
    /// Verify and atomically store a published logical bundle by SHA-256.
    Import {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        mirror: PathBuf,
        /// Expected SHA-256 from the authorized publication manifest.
        #[arg(long)]
        sha256: String,
        /// Expected byte count from the authorized publication manifest.
        #[arg(long)]
        size_bytes: u64,
    },
}

pub fn run(args: BundleArgs) -> Result<()> {
    match args.command {
        BundleCommand::Import {
            file,
            mirror,
            sha256,
            size_bytes,
        } => {
            let (path, installed) = import_bundle(&file, &mirror, &sha256, size_bytes)?;
            println!(
                "{}",
                serde_json::json!({
                    "path": path,
                    "sha256": sha256,
                    "size_bytes": size_bytes,
                    "installed": installed,
                })
            );
            Ok(())
        }
    }
}

fn import_bundle(
    source: &Path,
    mirror: &Path,
    expected_sha256: &str,
    expected_size: u64,
) -> Result<(PathBuf, bool)> {
    if expected_sha256.len() != 64
        || !expected_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("sha256 must be 64 lowercase hexadecimal characters");
    }
    if expected_size == 0 {
        bail!("size_bytes must be greater than zero");
    }
    let source_metadata = fs::symlink_metadata(source).context("cannot inspect bundle source")?;
    if !source_metadata.is_file() || source_metadata.file_type().is_symlink() {
        bail!("bundle source must be a regular file without symlinks");
    }
    if source_metadata.len() != expected_size {
        bail!("bundle source size differs from the publication manifest");
    }

    ensure_private_directory(mirror)?;
    let digest_directory = mirror.join("sha256");
    ensure_private_directory(&digest_directory)?;
    let target = digest_directory.join(expected_sha256);
    let mut input = open_regular_file(source)?;
    let mut staged = tempfile::NamedTempFile::new_in(&digest_directory)?;
    let mut hasher = Sha256::new();
    let mut observed = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        observed = observed
            .checked_add(count as u64)
            .context("bundle size overflow")?;
        if observed > expected_size {
            bail!("bundle source exceeded the publication byte count");
        }
        hasher.update(&buffer[..count]);
        staged.write_all(&buffer[..count])?;
    }
    if observed != expected_size || format!("{:x}", hasher.finalize()) != expected_sha256 {
        bail!("bundle source does not match the publication size and SHA-256");
    }
    if target.exists() {
        verify_file(&target, expected_sha256, expected_size)?;
        return Ok((target, false));
    }
    staged.as_file().sync_all()?;
    match staged.persist_noclobber(&target) {
        Ok(_) => {
            #[cfg(unix)]
            File::open(&digest_directory)?.sync_all()?;
            Ok((target, true))
        }
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            verify_file(&target, expected_sha256, expected_size)?;
            Ok((target, false))
        }
        Err(error) => Err(error.error.into()),
    }
}

fn verify_file(path: &Path, expected_sha256: &str, expected_size: u64) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != expected_size {
        bail!("existing mirror bundle is not the published regular file");
    }
    let mut input = open_regular_file(path)?;
    let mut hasher = Sha256::new();
    let mut observed = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        observed = observed
            .checked_add(count as u64)
            .context("bundle size overflow")?;
        if observed > expected_size {
            bail!("existing mirror bundle exceeded the publication byte count");
        }
        hasher.update(&buffer[..count]);
    }
    if observed != expected_size || format!("{:x}", hasher.finalize()) != expected_sha256 {
        bail!("existing mirror bundle does not match the publication SHA-256");
    }
    Ok(())
}

fn open_regular_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        bail!("bundle changed to a non-regular file during open");
    }
    Ok(file)
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            bail!("mirror path must be a directory without symlinks");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_is_verified_atomic_and_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("bundle.ndjson");
        let mirror = directory.path().join("mirror");
        let bytes = b"published bundle";
        fs::write(&source, bytes).unwrap();
        let digest = format!("{:x}", Sha256::digest(bytes));

        let (path, installed) =
            import_bundle(&source, &mirror, &digest, bytes.len() as u64).unwrap();
        assert!(installed);
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert!(
            !import_bundle(&source, &mirror, &digest, bytes.len() as u64)
                .unwrap()
                .1
        );
        fs::write(&source, b"corrupted bundle").unwrap();
        assert!(import_bundle(&source, &mirror, &digest, bytes.len() as u64).is_err());
        fs::write(&source, bytes).unwrap();
        fs::write(&path, b"corrupt mirror!!").unwrap();
        assert!(import_bundle(&source, &mirror, &digest, bytes.len() as u64).is_err());
    }

    #[test]
    fn rejected_import_never_publishes_a_blob() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("bundle.ndjson");
        let mirror = directory.path().join("mirror");
        let bytes = b"published bundle";
        fs::write(&source, bytes).unwrap();
        let wrong_digest = "a".repeat(64);

        assert!(import_bundle(&source, &mirror, &wrong_digest, bytes.len() as u64).is_err());
        assert!(!mirror.join("sha256").join(wrong_digest).exists());
    }
}
