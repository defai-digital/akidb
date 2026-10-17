//! Opt-in local source checks, confined to a caller-selected directory.
//! No URL is fetched and no source path from a response is trusted implicitly.

use std::io::Read;
use std::path::{Component, Path};

use akidb_proto::ContextPackItemV1;
use sha2::{Digest, Sha256};

const MAX_SOURCE_BYTES: u64 = 16 * 1024 * 1024;

/// Open each component relative to a pinned directory descriptor. Refusing
/// symlinks at every step also prevents ancestor replacement from escaping the
/// selected directory during verification.
#[cfg(unix)]
pub(crate) fn open_source(root: &Path, relative: &Path) -> std::io::Result<std::fs::File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::OpenOptionsExt;

    let mut current = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root)?;
    let mut parts = relative.components().peekable();
    if parts.peek().is_none() {
        return Err(std::io::ErrorKind::InvalidInput.into());
    }
    while let Some(part) = parts.next() {
        let Component::Normal(name) = part else {
            return Err(std::io::ErrorKind::InvalidInput.into());
        };
        let name = CString::new(name.as_bytes())
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let mut flags = libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK;
        if parts.peek().is_some() {
            flags |= libc::O_DIRECTORY;
        }
        // SAFETY: current owns a live fd; name is a NUL-terminated CString.
        let fd = unsafe { libc::openat(current.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: openat returned a newly owned fd; File closes it on drop.
        current = unsafe { std::fs::File::from_raw_fd(fd) };
    }
    Ok(current)
}

#[cfg(not(unix))]
pub(crate) fn open_source(_root: &Path, _relative: &Path) -> std::io::Result<std::fs::File> {
    Err(std::io::ErrorKind::Unsupported.into())
}

fn hash_matches(bytes: &[u8], expected: &str) -> bool {
    // The original folder indexer used a 16-character SHA-256 prefix. Accept
    // that documented legacy format as well as full SHA-256, never arbitrary
    // short prefixes or opaque source-version labels.
    if !matches!(expected.len(), 16 | 64) || !expected.bytes().all(|b| b.is_ascii_hexdigit()) {
        return false;
    }
    let actual = format!("{:x}", Sha256::digest(bytes));
    actual[..expected.len()].eq_ignore_ascii_case(expected)
}

/// Verify the bytes in one citation against the current local file. This does
/// not validate the truth of the source or the correctness of an agent answer.
pub fn verify(item: &ContextPackItemV1, root: &Path) -> &'static str {
    let Some(citation) = &item.citation else {
        return "missing_citation";
    };
    if citation.chunk_id != item.chunk_id {
        return "chunk_id_mismatch";
    }
    if citation.source_uri.is_empty() {
        return "missing_source";
    }
    let Ok(uri) = url::Url::parse(&citation.source_uri) else {
        return "unsupported_source";
    };
    if uri.scheme() != "file" || uri.query().is_some() || uri.fragment().is_some() {
        return "unsupported_source";
    }
    let Ok(path) = uri.to_file_path() else {
        return "unsupported_source";
    };
    let Ok(relative) = path.strip_prefix(root) else {
        return "outside_allowed_root";
    };
    let Ok(file) = open_source(root, relative) else {
        return "source_unavailable";
    };
    let Ok(meta) = file.metadata() else {
        return "source_unavailable";
    };
    if !meta.is_file() || meta.len() > MAX_SOURCE_BYTES {
        return "unsupported_source_size_or_type";
    }
    let mut bytes = Vec::new();
    if file
        .take(MAX_SOURCE_BYTES + 1)
        .read_to_end(&mut bytes)
        .is_err()
    {
        return "source_unavailable";
    }
    if bytes.len() as u64 > MAX_SOURCE_BYTES {
        return "unsupported_source_size_or_type";
    }
    let (Some(start), Some(end)) = (citation.start_offset, citation.end_offset) else {
        return "missing_offsets";
    };
    if start > end || end > bytes.len() as u64 {
        return "invalid_offsets";
    }
    let span = &bytes[start as usize..end as usize];
    if span != item.text.as_bytes() {
        return "text_mismatch";
    }
    if !hash_matches(span, &citation.content_hash) {
        return "content_hash_mismatch_or_missing";
    }
    if !hash_matches(&bytes, &citation.document_version) {
        return "document_version_mismatch_or_unsupported";
    }
    "verified"
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_proto::RetrievalCitationV1;

    fn sample(root: &Path) -> ContextPackItemV1 {
        let path = root.join("source #1.txt");
        let text = "prefix\ncaf\u{e9}\n";
        std::fs::write(&path, text).unwrap();
        ContextPackItemV1 {
            text: "caf\u{e9}\n".into(),
            citation: Some(RetrievalCitationV1 {
                source_uri: url::Url::from_file_path(path).unwrap().to_string(),
                start_offset: Some(7),
                end_offset: Some(text.len() as u64),
                content_hash: format!("{:x}", Sha256::digest("caf\u{e9}\n".as_bytes())),
                document_version: format!("{:x}", Sha256::digest(text.as_bytes())),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn verifies_encoded_paths_utf8_and_detects_changes_outside_the_passage() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let item = sample(&root);
        assert_eq!(verify(&item, &root), "verified");
        std::fs::write(root.join("source #1.txt"), "PREFIX\ncaf\u{e9}\n").unwrap();
        assert_eq!(
            verify(&item, &root),
            "document_version_mismatch_or_unsupported"
        );
    }

    #[test]
    fn rejects_bad_ranges_hashes_and_external_sources() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let mut item = sample(&root);
        item.citation.as_mut().unwrap().chunk_id = "other-chunk".into();
        assert_eq!(verify(&item, &root), "chunk_id_mismatch");
        let mut item = sample(&root);
        item.citation.as_mut().unwrap().end_offset = Some(u64::MAX);
        assert_eq!(verify(&item, &root), "invalid_offsets");
        let mut item = sample(&root);
        item.citation.as_mut().unwrap().content_hash = "ab".into();
        assert_eq!(verify(&item, &root), "content_hash_mismatch_or_missing");
        item.citation.as_mut().unwrap().source_uri = "https://example.com/private".into();
        assert_eq!(verify(&item, &root), "unsupported_source");
        let other = tempfile::tempdir().unwrap();
        assert_eq!(
            verify(&sample(&root), &other.path().canonicalize().unwrap()),
            "outside_allowed_root"
        );
    }

    #[cfg(unix)]
    #[test]
    fn refuses_symlink_sources_and_accepts_legacy_hashes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let mut item = sample(&root);
        let c = item.citation.as_mut().unwrap();
        c.content_hash.truncate(16);
        c.document_version.truncate(16);
        assert_eq!(verify(&item, &root), "verified");
        std::os::unix::fs::symlink(root.join("source #1.txt"), root.join("alias.txt")).unwrap();
        item.citation.as_mut().unwrap().source_uri =
            url::Url::from_file_path(root.join("alias.txt"))
                .unwrap()
                .to_string();
        assert_eq!(verify(&item, &root), "source_unavailable");
        std::os::unix::fs::symlink(&root, root.join("linked-dir")).unwrap();
        item.citation.as_mut().unwrap().source_uri =
            url::Url::from_file_path(root.join("linked-dir/source #1.txt"))
                .unwrap()
                .to_string();
        assert_eq!(verify(&item, &root), "source_unavailable");
    }
}
