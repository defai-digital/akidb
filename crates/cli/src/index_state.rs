//! Local state for incremental `akidb index` runs.
//!
//! The server has no way to list a collection, so the CLI remembers what it
//! last stored: per absolute file path, the content hash (`document_version`),
//! the chunk count, and the settings the chunks were built with. A later run
//! skips a file whose entry still matches, and deletes the chunks of a file
//! that is gone from disk. The state is only a hint: before skipping, the
//! caller checks the server document head and every durable chunk, so a
//! wiped or restored server is re-indexed rather than trusted.
//!
//! The file lives outside the indexed folder (default
//! `~/.akidb/index-state/`) so indexing never writes into a user's project.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const STATE_SCHEMA: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    pub document_version: String,
    pub chunk_count: usize,
    /// Embedding model, dimensions, chunk size and chunker version.
    pub settings: String,
}

#[derive(Debug)]
pub struct IndexState {
    path: PathBuf,
    files: BTreeMap<String, FileEntry>,
    dirty: bool,
}

/// Default state directory: `$AKIDB_INDEX_STATE_DIR`, else `~/.akidb/index-state`.
pub fn default_state_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("AKIDB_INDEX_STATE_DIR") {
        if !dir.is_empty() {
            return Some(PathBuf::from(dir));
        }
    }
    let home = std::env::var_os("HOME").filter(|h| !h.is_empty())?;
    Some(PathBuf::from(home).join(".akidb").join("index-state"))
}

/// Settings fingerprint stored with each file entry.
pub fn settings_fingerprint(
    model: &str,
    dimensions: usize,
    chunk_chars: usize,
    chunker: &str,
    symbols: bool,
) -> String {
    format!("{model}|{dimensions}|{chunk_chars}|{chunker}|{symbols}")
}

impl IndexState {
    /// Load (or start) the state for one server and collection.
    pub fn load(dir: &Path, server: &str, collection: &str) -> Result<Self> {
        let digest = Sha256::digest(format!("{server}\n{collection}").as_bytes());
        let name: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
        let path = dir.join(format!("{name}.json"));
        let mut files = BTreeMap::new();
        match std::fs::read_to_string(&path) {
            Ok(raw) => {
                // A corrupt or newer-schema file is ignored: the worst case
                // is a full re-index of present files. Removed paths cannot be
                // pruned without their local receipt.
                if let Ok(value) = serde_json::from_str::<Value>(&raw) {
                    if value["schema"].as_u64() == Some(STATE_SCHEMA) {
                        if let Some(map) = value["files"].as_object() {
                            for (file, entry) in map {
                                if let (Some(v), Some(n), Some(s)) = (
                                    entry["document_version"].as_str(),
                                    entry["chunk_count"].as_u64(),
                                    entry["settings"].as_str(),
                                ) {
                                    files.insert(
                                        file.clone(),
                                        FileEntry {
                                            document_version: v.to_string(),
                                            chunk_count: n as usize,
                                            settings: s.to_string(),
                                        },
                                    );
                                }
                            }
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).with_context(|| format!("cannot read {}", path.display()));
            }
        }
        Ok(Self {
            path,
            files,
            dirty: false,
        })
    }

    pub fn get(&self, file: &str) -> Option<&FileEntry> {
        self.files.get(file)
    }

    pub fn set(&mut self, file: String, entry: FileEntry) {
        if self.files.get(&file) != Some(&entry) {
            self.files.insert(file, entry);
            self.dirty = true;
        }
    }

    pub fn remove(&mut self, file: &str) {
        if self.files.remove(file).is_some() {
            self.dirty = true;
        }
    }

    /// Entries at or below `root` that are not in `present`: files that were
    /// indexed earlier and are no longer indexable on disk.
    pub fn missing_under(
        &self,
        root: &Path,
        present: &std::collections::BTreeSet<String>,
    ) -> Vec<(String, FileEntry)> {
        self.files
            .iter()
            .filter(|(file, _)| Path::new(file.as_str()).starts_with(root))
            .filter(|(file, _)| !present.contains(file.as_str()))
            .map(|(file, entry)| (file.clone(), entry.clone()))
            .collect()
    }

    /// Persist atomically (temp file, then rename). No-op when unchanged.
    pub fn save(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }
        let files: serde_json::Map<String, Value> = self
            .files
            .iter()
            .map(|(file, e)| {
                (
                    file.clone(),
                    json!({
                        "document_version": e.document_version,
                        "chunk_count": e.chunk_count,
                        "settings": e.settings,
                    }),
                )
            })
            .collect();
        let body = serde_json::to_string_pretty(&json!({
            "schema": STATE_SCHEMA,
            "files": files,
        }))?;
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, body).with_context(|| format!("cannot write {}", tmp.display()))?;
        std::fs::rename(&tmp, &self.path)
            .with_context(|| format!("cannot replace {}", self.path.display()))?;
        self.dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn entry(v: &str, n: usize) -> FileEntry {
        FileEntry {
            document_version: v.into(),
            chunk_count: n,
            settings: "m|4|100|sym1|true".into(),
        }
    }

    #[test]
    fn round_trips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = IndexState::load(dir.path(), "127.0.0.1:50051", "default").unwrap();
        assert!(state.get("/a/b.rs").is_none());
        state.set("/a/b.rs".into(), entry("v1", 3));
        state.save().unwrap();
        let again = IndexState::load(dir.path(), "127.0.0.1:50051", "default").unwrap();
        assert_eq!(again.get("/a/b.rs"), Some(&entry("v1", 3)));
        // Another collection has its own state.
        let other = IndexState::load(dir.path(), "127.0.0.1:50051", "other").unwrap();
        assert!(other.get("/a/b.rs").is_none());
    }

    #[test]
    fn corrupt_state_is_ignored_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = IndexState::load(dir.path(), "s", "c").unwrap();
        state.set("/x".into(), entry("v", 1));
        state.save().unwrap();
        let path = std::fs::read_dir(dir.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::write(&path, "{ not json").unwrap();
        let reloaded = IndexState::load(dir.path(), "s", "c").unwrap();
        assert!(reloaded.get("/x").is_none());
    }

    #[test]
    fn missing_under_is_scoped_to_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = IndexState::load(dir.path(), "s", "c").unwrap();
        state.set("/proj/a.rs".into(), entry("1", 1));
        state.set("/proj/b.rs".into(), entry("2", 1));
        state.set("/projx/c.rs".into(), entry("3", 1));
        state.set("/other/d.rs".into(), entry("4", 1));
        let present: BTreeSet<String> = ["/proj/a.rs".to_string()].into();
        let gone = state.missing_under(Path::new("/proj"), &present);
        let names: Vec<_> = gone.iter().map(|(f, _)| f.as_str()).collect();
        // `/projx` shares a string prefix but is a different directory.
        assert_eq!(names, vec!["/proj/b.rs"]);
    }

    #[test]
    fn unchanged_state_is_not_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = IndexState::load(dir.path(), "s", "c").unwrap();
        state.set("/x".into(), entry("v", 1));
        state.save().unwrap();
        let mut again = IndexState::load(dir.path(), "s", "c").unwrap();
        again.set("/x".into(), entry("v", 1));
        assert!(!again.dirty);
    }

    #[test]
    fn settings_fingerprint_changes_with_each_input() {
        let base = settings_fingerprint("m", 4, 100, "sym1", true);
        assert_ne!(base, settings_fingerprint("m2", 4, 100, "sym1", true));
        assert_ne!(base, settings_fingerprint("m", 8, 100, "sym1", true));
        assert_ne!(base, settings_fingerprint("m", 4, 200, "sym1", true));
        assert_ne!(base, settings_fingerprint("m", 4, 100, "sym2", true));
        assert_ne!(base, settings_fingerprint("m", 4, 100, "sym1", false));
    }
}
