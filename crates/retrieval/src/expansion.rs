//! Parent-child context expansion (Chunk Intelligence, CHUNK-003).
//!
//! A common RAG failure: the retriever finds the right small chunk, but the LLM
//! needs the surrounding parent (paragraph / section / function) to answer. The
//! fix is to *search* small child chunks but *return* the larger parent for
//! answering. This module performs that expansion at query time, after retrieval
//! and before packing.
//!
//! It is deliberately storage-agnostic: parent text is supplied via a closure,
//! so the same logic serves the in-memory document store today and any future
//! backing store. Children that share a parent collapse to a single parent
//! passage (deduplicated, keeping the best-ranked occurrence's score).

use akidb_common::VectorId;

use crate::packer::{Citation, Passage};

/// A matched child chunk to be expanded toward its parent.
#[derive(Debug, Clone)]
pub struct MatchedChunk {
    /// The child chunk's id.
    pub id: VectorId,
    /// The parent's id, if this chunk has one.
    pub parent_id: Option<String>,
    /// The child's own text (fallback when no parent is available).
    pub text: String,
    /// The child's retrieval score.
    pub score: f32,
}

impl MatchedChunk {
    pub fn new(
        id: VectorId,
        parent_id: Option<String>,
        text: impl Into<String>,
        score: f32,
    ) -> Self {
        Self {
            id,
            parent_id,
            text: text.into(),
            score,
        }
    }
}

/// Expand matched children to parent context for packing.
///
/// For each match in rank order:
/// - if it has a `parent_id` whose text `parent_text` resolves, emit one passage
///   for that parent (the first, best-ranked occurrence wins; later children of
///   the same parent are skipped to avoid duplicate context);
/// - otherwise emit a passage from the child's own text.
///
/// Rank order is preserved. Each emitted passage carries a citation to whatever
/// it represents (parent id or child id).
pub fn expand_to_parents<F>(matched: &[MatchedChunk], parent_text: F) -> Vec<Passage>
where
    F: Fn(&str) -> Option<String>,
{
    let mut out: Vec<Passage> = Vec::new();
    // Dedup by the *resolved* passage id, so siblings collapse to one parent and
    // a parent that is itself retrieved isn't emitted twice (as parent + self).
    let mut seen: Vec<String> = Vec::new();

    for m in matched {
        // Resolve each match to the (id, text) it should contribute: its parent
        // when one resolves, otherwise the child itself.
        let (resolved_id, text) = match m.parent_id.as_deref().and_then(|pid| {
            parent_text(pid).map(|t| (pid.to_string(), t))
        }) {
            Some(parent) => parent,
            None => (m.id.to_string(), m.text.clone()),
        };

        if seen.iter().any(|s| s == &resolved_id) {
            continue; // already represented (best-ranked occurrence wins)
        }
        seen.push(resolved_id.clone());
        out.push(Passage::new(
            VectorId::new(resolved_id.clone()),
            text,
            m.score,
            Citation::new(resolved_id),
        ));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn id(s: &str) -> VectorId {
        VectorId::new(s)
    }

    fn parents() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("P1".to_string(), "full parent one context".to_string());
        m.insert("P2".to_string(), "full parent two context".to_string());
        m
    }

    #[test]
    fn test_child_expands_to_parent() {
        let p = parents();
        let matched = [MatchedChunk::new(id("c1"), Some("P1".into()), "child snippet", 0.9)];
        let out = expand_to_parents(&matched, |pid| p.get(pid).cloned());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, id("P1"));
        assert_eq!(out[0].text, "full parent one context");
        assert_eq!(out[0].citation.source_uri, "P1");
        assert_eq!(out[0].score, 0.9);
    }

    #[test]
    fn test_siblings_dedup_to_single_parent_best_score() {
        let p = parents();
        let matched = [
            MatchedChunk::new(id("c1"), Some("P1".into()), "snippet a", 0.9),
            MatchedChunk::new(id("c2"), Some("P1".into()), "snippet b", 0.7),
        ];
        let out = expand_to_parents(&matched, |pid| p.get(pid).cloned());
        assert_eq!(out.len(), 1, "siblings collapse to one parent");
        assert_eq!(out[0].id, id("P1"));
        assert_eq!(out[0].score, 0.9, "keeps the best-ranked occurrence's score");
    }

    #[test]
    fn test_no_parent_uses_child_text() {
        let p = parents();
        let matched = [MatchedChunk::new(id("solo"), None, "standalone text", 0.5)];
        let out = expand_to_parents(&matched, |pid| p.get(pid).cloned());
        assert_eq!(out[0].id, id("solo"));
        assert_eq!(out[0].text, "standalone text");
    }

    #[test]
    fn test_missing_parent_text_falls_back_to_child() {
        let p = parents();
        let matched = [MatchedChunk::new(id("c9"), Some("UNKNOWN".into()), "child only", 0.4)];
        let out = expand_to_parents(&matched, |pid| p.get(pid).cloned());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, id("c9"));
        assert_eq!(out[0].text, "child only");
    }

    #[test]
    fn test_order_preserved_across_mixed_matches() {
        let p = parents();
        let matched = [
            MatchedChunk::new(id("c1"), Some("P2".into()), "a", 0.9),
            MatchedChunk::new(id("solo"), None, "b", 0.8),
            MatchedChunk::new(id("c2"), Some("P1".into()), "c", 0.7),
            MatchedChunk::new(id("c3"), Some("P2".into()), "d", 0.6), // dup parent P2
        ];
        let out = expand_to_parents(&matched, |pid| p.get(pid).cloned());
        let ids: Vec<&str> = out.iter().map(|x| x.id.as_str()).collect();
        assert_eq!(ids, vec!["P2", "solo", "P1"]);
    }
}
