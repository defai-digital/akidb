//! Code-aware segmentation for `akidb index`.
//!
//! Source files in a language the retrieval crate can scan (Rust, Python,
//! TypeScript/JavaScript, Go) are split at top-level symbol boundaries so a
//! signature stays with its body and doc comments. Everything else, and any
//! file the scanner cannot handle, falls back to line-aligned text chunks.
//!
//! The scanner is a dependency-free heuristic, not a full parse. If it
//! panics on unusual input the file falls back to plain text chunks, so one
//! odd file never aborts an index run.

use akidb_retrieval::code::{chunk_code, Language};

use crate::index::{chunk_text, Chunk};

/// Bump when the segmentation rules change so `akidb index` re-embeds files
/// that were chunked by an earlier rule set.
pub const CHUNKER_VERSION: &str = "sym2";

/// Whether `ext` (without dot) is chunked by symbol.
pub fn is_symbol_language(ext: &str) -> bool {
    Language::from_extension(ext) != Language::Other
}

/// Split `text` into chunks of at most roughly `max_chars` characters.
///
/// With `symbols` set and a supported language, chunks follow symbol
/// boundaries: adjacent small symbols are packed together up to `max_chars`,
/// an oversized symbol is split by lines, and non-symbol text between symbols
/// (imports, module docs, top-level statements) is kept rather than dropped.
pub fn segment(text: &str, extension: &str, max_chars: usize, symbols: bool) -> Vec<Chunk> {
    if !symbols || !is_symbol_language(extension) {
        return chunk_text(text, max_chars);
    }
    let lang = Language::from_extension(extension);
    let source = text.to_string();
    let scanned = std::panic::catch_unwind(move || chunk_code(&source, lang));
    let Ok(code_chunks) = scanned else {
        return chunk_text(text, max_chars);
    };
    if code_chunks.is_empty() {
        return chunk_text(text, max_chars);
    }

    // Byte ranges of symbols, sorted and clamped so they never overlap.
    let mut spans: Vec<(usize, usize, Option<String>)> = Vec::new();
    let mut cursor = 0usize;
    for c in &code_chunks {
        let start = c.start_byte.max(cursor);
        let end = c.end_byte.min(text.len());
        if start >= end || !text.is_char_boundary(start) || !text.is_char_boundary(end) {
            continue;
        }
        spans.push((start, end, c.name.clone()));
        cursor = end;
    }
    if spans.is_empty() {
        return chunk_text(text, max_chars);
    }

    // Segments: gaps between symbols plus the symbols themselves.
    let mut segments: Vec<(usize, usize, Option<String>)> = Vec::new();
    let mut at = 0usize;
    for (start, end, name) in spans {
        if start > at {
            segments.push((at, start, None));
        }
        segments.push((start, end, name));
        at = end;
    }
    if at < text.len() {
        segments.push((at, text.len(), None));
    }

    let mut out: Vec<Chunk> = Vec::new();
    let mut pending: Option<(usize, usize, usize)> = None; // start, end, chars
    let mut pending_names: Vec<String> = Vec::new();
    let flush = |pending: &mut Option<(usize, usize, usize)>,
                 names: &mut Vec<String>,
                 out: &mut Vec<Chunk>| {
        if let Some((start, end, _)) = pending.take() {
            let slice = &text[start..end];
            if !slice.trim().is_empty() {
                out.push(Chunk {
                    start,
                    end,
                    text: slice.to_string(),
                    symbol: join_names(names),
                });
            }
        }
        names.clear();
    };

    for (start, end, name) in segments {
        let slice = &text[start..end];
        if slice.trim().is_empty() {
            // Blank gap: let it ride along with a pending chunk only, and only
            // while it fits. An unbounded run of blank lines would otherwise
            // produce a chunk as large as the file, which blows the embedding
            // context and dilutes retrieval.
            let gap_chars = slice.chars().count();
            let fits = pending
                .as_ref()
                .is_some_and(|(_, _, chars)| *chars + gap_chars <= max_chars);
            if fits {
                if let Some((_, e, chars)) = pending.as_mut() {
                    *e = end;
                    *chars += gap_chars;
                }
            } else if pending.is_some() {
                // Close the chunk at the last symbol; the whitespace adds
                // nothing to retrieval and the next symbol opens a new chunk.
                flush(&mut pending, &mut pending_names, &mut out);
            }
            continue;
        }
        let chars = slice.chars().count();
        if chars > max_chars {
            flush(&mut pending, &mut pending_names, &mut out);
            let label = name.clone();
            for mut piece in chunk_text(slice, max_chars) {
                piece.start += start;
                piece.end += start;
                piece.symbol = label.clone();
                out.push(piece);
            }
            continue;
        }
        match pending.as_mut() {
            Some((_, e, c)) if *c + chars <= max_chars => {
                // Keep the pack contiguous: extend through any blank lines.
                *e = end;
                *c += chars;
                if let Some(n) = name {
                    pending_names.push(n);
                }
            }
            _ => {
                flush(&mut pending, &mut pending_names, &mut out);
                pending = Some((start, end, chars));
                if let Some(n) = name {
                    pending_names.push(n);
                }
            }
        }
    }
    flush(&mut pending, &mut pending_names, &mut out);
    if out.is_empty() {
        return chunk_text(text, max_chars);
    }
    out
}

fn join_names(names: &[String]) -> Option<String> {
    if names.is_empty() {
        return None;
    }
    let mut joined = names.iter().take(4).cloned().collect::<Vec<_>>().join(", ");
    if names.len() > 4 {
        joined.push_str(&format!(", +{} more", names.len() - 4));
    }
    Some(joined)
}

#[cfg(test)]
mod tests {
    use super::*;

    const RUST: &str = "use std::fmt;\n\n/// Adds.\npub fn add(a: i32, b: i32) -> i32 {\n    a + b\n}\n\npub fn sub(a: i32, b: i32) -> i32 {\n    a - b\n}\n";

    #[test]
    fn small_symbols_pack_together_and_keep_imports() {
        let chunks = segment(RUST, "rs", 1200, true);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].text.contains("use std::fmt"));
        assert!(chunks[0].text.contains("pub fn sub"));
        let sym = chunks[0].symbol.as_deref().unwrap();
        assert!(sym.contains("add") && sym.contains("sub"));
    }

    #[test]
    fn blank_gaps_do_not_grow_a_chunk_past_the_budget() {
        let text = format!(
            "fn a() -> i32 {{ 1 }}\n{}\nfn b() -> i32 {{ 2 }}\n",
            "\n".repeat(300)
        );
        let chunks = segment(&text, "rs", 120, true);

        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.text.chars().count() <= 120),
            "a blank gap must not push a chunk over the budget: {chunks:?}"
        );
        assert!(chunks.iter().any(|chunk| chunk.text.contains("fn a")));
        assert!(chunks.iter().any(|chunk| chunk.text.contains("fn b")));
    }

    #[test]
    fn symbols_split_at_boundaries_when_budget_is_small() {
        let chunks = segment(RUST, "rs", 60, true);
        assert!(chunks.len() >= 3, "{chunks:?}");
        // No chunk starts in the middle of a function body.
        for c in &chunks {
            assert!(c.text.matches('{').count() == c.text.matches('}').count());
            assert_eq!(&RUST[c.start..c.end], c.text);
        }
        assert!(chunks.iter().any(|c| c.text.starts_with("/// Adds.")));
    }

    #[test]
    fn offsets_always_slice_back_to_the_chunk_text() {
        let big = format!(
            "fn long() {{\n{}}}\n\nfn tail() {{}}\n",
            "    let x = 1;\n".repeat(40)
        );
        let chunks = segment(&big, "rs", 100, true);
        assert!(chunks.len() > 2);
        for c in &chunks {
            assert_eq!(&big[c.start..c.end], c.text);
            assert!(c.text.chars().count() <= 100);
        }
        assert_eq!(chunks[0].symbol.as_deref(), Some("long"));
    }

    #[test]
    fn python_functions_split_by_symbol() {
        let py = "import os\n\ndef a():\n    return 1\n\ndef b():\n    return 2\n";
        let chunks = segment(py, "py", 30, true);
        assert!(chunks.len() >= 2);
        for c in &chunks {
            assert_eq!(&py[c.start..c.end], c.text);
        }
    }

    #[test]
    fn other_languages_and_disabled_mode_use_text_chunks() {
        let md = "# Title\n\nbody\n";
        assert_eq!(segment(md, "md", 100, true), chunk_text(md, 100));
        assert_eq!(segment(RUST, "rs", 100, false), chunk_text(RUST, 100));
        assert!(segment(RUST, "rs", 100, false)
            .iter()
            .all(|c| c.symbol.is_none()));
    }

    #[test]
    fn multibyte_source_never_panics() {
        let src = "fn héllo() {\n    let s = \"日本語🙂\";\n}\n\nfn wörld() {}\n";
        let chunks = segment(src, "rs", 20, true);
        for c in &chunks {
            assert_eq!(&src[c.start..c.end], c.text);
        }
    }

    #[test]
    fn blank_source_yields_nothing() {
        assert!(segment("  \n\n", "rs", 100, true).is_empty());
    }
}
