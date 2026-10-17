//! Explicit, additive exclusions for folder indexing. Rules are literal relative
//! paths, not shell globs. Basenames match at any depth; paths match from root.

use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};

const SKIP_DIRS: &[&str] = &[
    "target",
    "node_modules",
    "dist",
    "build",
    "__pycache__",
    "venv",
    "vendor",
];

#[derive(Default)]
pub struct Selection {
    pub files: Vec<PathBuf>,
    pub excluded: Vec<(PathBuf, &'static str)>,
}

pub struct Exclusions(Vec<PathBuf>);

impl Exclusions {
    pub fn load(base: &Path, extra: &[String]) -> Result<Self> {
        let ignore_path = base.join(".akidbignore");
        let mut lines = extra.to_vec();
        match std::fs::symlink_metadata(&ignore_path) {
            Ok(meta) => {
                if !meta.is_file() || meta.file_type().is_symlink() || meta.len() > 65_536 {
                    bail!(".akidbignore must be a regular file of at most 64 KiB");
                }
                let text = super::index::read_bounded_utf8(&ignore_path, 65_536)?
                    .context(".akidbignore must be UTF-8 and at most 64 KiB")?;
                lines.extend(text.lines().map(str::to_string));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).context("cannot inspect .akidbignore"),
        }
        let mut rules = Vec::new();
        for line in lines {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let path = PathBuf::from(line);
            if path.is_absolute()
                || path
                    .components()
                    .any(|c| !matches!(c, Component::Normal(_)))
                || line.contains(['*', '?', '[', ']', '!', '\\'])
            {
                bail!(
                    "exclusions must be literal relative paths without globs or parent components"
                );
            }
            rules.push(path);
        }
        Ok(Self(rules))
    }

    fn matches(&self, relative: &Path) -> bool {
        self.0.iter().any(|rule| {
            if rule.components().count() == 1 {
                relative
                    .components()
                    .any(|part| part.as_os_str() == rule.as_os_str())
            } else {
                relative.starts_with(rule)
            }
        })
    }
}

fn sensitive_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    let stem = name.split('.').next().unwrap_or_default();
    matches!(stem, "secret" | "secrets" | "credential" | "credentials")
        || name == "auth.token"
        || name == "known_hosts"
        || name == "authorized_keys"
        || ["id_rsa", "id_dsa", "id_ecdsa", "id_ed25519"]
            .iter()
            .any(|prefix| name == *prefix || name.starts_with(&format!("{prefix}.")))
        || name.ends_with(".key")
        || name.ends_with(".pem")
        || name.ends_with(".p12")
        || name.ends_with(".pfx")
}

pub fn collect(root: &Path, base: &Path, rules: &Exclusions) -> Result<Selection> {
    let mut selection = Selection::default();
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let meta = std::fs::symlink_metadata(&path)
            .with_context(|| format!("cannot inspect {}", path.display()))?;
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        let relative = path.strip_prefix(base).unwrap_or(&path);
        // An explicitly selected directory is indexed even when its own name is
        // on a default skip list (`akidb index ./build`): the operator asked for
        // it. Its contents are still filtered below. An explicitly selected
        // *file* keeps every refusal, including the sensitive-name one.
        let selected_root_directory = meta.is_dir() && path.as_path() == root;
        let reason = if meta.file_type().is_symlink() {
            Some("symlink")
        } else if !relative.as_os_str().is_empty() && name.starts_with('.') {
            Some("hidden")
        } else if !selected_root_directory && sensitive_name(&name) {
            Some("sensitive filename")
        } else if rules.matches(relative) {
            Some("excluded by rule")
        } else if !selected_root_directory && meta.is_dir() && SKIP_DIRS.contains(&name.as_ref()) {
            Some("dependency or build directory")
        } else {
            None
        };
        if let Some(reason) = reason {
            selection.excluded.push((path, reason));
        } else if meta.is_dir() {
            for entry in std::fs::read_dir(&path)? {
                stack.push(entry?.path());
            }
        } else if meta.is_file() && super::index::is_indexable(&path) {
            selection.files.push(path);
        } else {
            selection.excluded.push((path, "unsupported file type"));
        }
    }
    selection.files.sort();
    selection.excluded.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(selection)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_and_sensitive_names_apply_to_explicit_files_and_directories() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["keep.md", "credentials.json", "private.txt", ".hidden.md"] {
            std::fs::write(dir.path().join(name), "example").unwrap();
        }
        std::fs::create_dir(dir.path().join("nested")).unwrap();
        std::fs::write(dir.path().join("nested/private.txt"), "example").unwrap();
        std::fs::write(
            dir.path().join(".akidbignore"),
            "# local exclusions\nprivate.txt\n",
        )
        .unwrap();
        let rules = Exclusions::load(dir.path(), &[]).unwrap();
        let result = collect(dir.path(), dir.path(), &rules).unwrap();
        assert_eq!(result.files, [dir.path().join("keep.md")]);
        for name in ["credentials.json", "private.txt", ".hidden.md"] {
            assert!(collect(&dir.path().join(name), dir.path(), &rules)
                .unwrap()
                .files
                .is_empty());
        }
    }

    #[test]
    fn rooted_rules_do_not_match_other_directories_or_prefixes() {
        let dir = tempfile::tempdir().unwrap();
        let rules = Exclusions::load(dir.path(), &["docs/private".into()]).unwrap();
        assert!(rules.matches(Path::new("docs/private/key.txt")));
        assert!(!rules.matches(Path::new("docs/private-notes.txt")));
        assert!(!rules.matches(Path::new("other/docs/private/key.txt")));
        for bad in ["../secret", "/tmp/secret", "*.json", "!keep.txt"] {
            assert!(Exclusions::load(dir.path(), &[bad.into()]).is_err());
        }
    }

    #[test]
    fn explicitly_selected_directory_overrides_default_skip_list() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("target");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("notes.md"), "example").unwrap();
        std::fs::create_dir(root.join("nested")).unwrap();
        std::fs::write(root.join("nested/more.md"), "example").unwrap();
        let rules = Exclusions::load(&root, &[]).unwrap();

        let result = collect(&root, &root, &rules).unwrap();
        assert_eq!(
            result.files,
            [root.join("nested/more.md"), root.join("notes.md")]
        );

        // Its contents are still filtered: a nested skip-listed directory and a
        // secret file stay out.
        std::fs::create_dir(root.join("node_modules")).unwrap();
        std::fs::write(root.join("node_modules/pkg.md"), "example").unwrap();
        std::fs::write(root.join("credentials.json"), "example").unwrap();
        let result = collect(&root, &root, &rules).unwrap();
        assert_eq!(
            result.files,
            [root.join("nested/more.md"), root.join("notes.md")]
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_not_followed_even_for_explicit_inputs() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("keep.md"), "example").unwrap();
        let link = dir.path().join("link.md");
        std::os::unix::fs::symlink(dir.path().join("keep.md"), &link).unwrap();
        let rules = Exclusions::load(dir.path(), &[]).unwrap();
        assert!(collect(&link, dir.path(), &rules).unwrap().files.is_empty());
        std::os::unix::fs::symlink(dir.path().join("keep.md"), dir.path().join(".akidbignore"))
            .unwrap();
        assert!(Exclusions::load(dir.path(), &[]).is_err());
    }
}
