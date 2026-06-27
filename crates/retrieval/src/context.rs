//! Contextual chunking (CHUNK-004).
//!
//! Standalone chunks often lack the context needed to retrieve them well — e.g.
//! "Revenue increased 18% year over year." with no document or section attached.
//! Prepending document/section context to a chunk before embedding and lexical
//! indexing (Anthropic's contextual retrieval) materially reduces failed
//! retrievals.
//!
//! This module produces the *contextualized text to index*. The `headings` mode
//! is dependency-free (it stitches the provided heading path onto the chunk). The
//! `local_llm` mode (generating a per-chunk summary with a local model) is a
//! deferred extension and is intentionally not implemented here.

/// How much context to prepend to a chunk before indexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Contextualization {
    /// Index the chunk text as-is.
    #[default]
    Off,
    /// Prepend the document title and heading path.
    Headings,
}

/// Build the text to index for a chunk under the given contextualization mode.
///
/// For [`Contextualization::Headings`], a single context line is prepended:
/// `"<title> > <h1> > <h2>: \n<chunk>"`, skipping any empty parts. With no title
/// and no headings, or in [`Contextualization::Off`], the chunk is returned
/// unchanged.
pub fn contextualize(
    chunk_text: &str,
    doc_title: Option<&str>,
    headings: &[String],
    mode: Contextualization,
) -> String {
    match mode {
        Contextualization::Off => chunk_text.to_string(),
        Contextualization::Headings => {
            let mut parts: Vec<&str> = Vec::new();
            if let Some(t) = doc_title {
                if !t.trim().is_empty() {
                    parts.push(t.trim());
                }
            }
            for h in headings {
                let h = h.trim();
                if !h.is_empty() {
                    parts.push(h);
                }
            }
            if parts.is_empty() {
                chunk_text.to_string()
            } else {
                format!("{}:\n{}", parts.join(" > "), chunk_text)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_off_returns_chunk_unchanged() {
        let out = contextualize("body", Some("Doc"), &["H1".into()], Contextualization::Off);
        assert_eq!(out, "body");
    }

    #[test]
    fn test_headings_prepends_title_and_path() {
        let out = contextualize(
            "Revenue increased 18% year over year.",
            Some("2025 Annual Report"),
            &["Financials".into(), "Revenue".into()],
            Contextualization::Headings,
        );
        assert_eq!(
            out,
            "2025 Annual Report > Financials > Revenue:\nRevenue increased 18% year over year."
        );
    }

    #[test]
    fn test_headings_with_no_context_returns_chunk() {
        let out = contextualize("body", None, &[], Contextualization::Headings);
        assert_eq!(out, "body");
    }

    #[test]
    fn test_headings_skips_empty_parts() {
        let out = contextualize(
            "body",
            Some("  "),
            &["".into(), " Section ".into()],
            Contextualization::Headings,
        );
        assert_eq!(out, "Section:\nbody");
    }

    #[test]
    fn test_title_only() {
        let out = contextualize("body", Some("Doc"), &[], Contextualization::Headings);
        assert_eq!(out, "Doc:\nbody");
    }
}
