//! Source-grounded context packing.
//!
//! Retrieval returns ranked ids and scores; an LLM/agent wants a single,
//! token-budget-aware block of grounded text with citations it can trust. This
//! module turns ranked [`Passage`]s into a [`ContextPack`]: it greedily fills a
//! token budget in rank order, formats each included passage according to a
//! [`PackStrategy`], and guarantees every included span carries a [`Citation`]
//! back to its source (PACK-001/002/004).
//!
//! Token counting here is a deterministic whitespace-word heuristic, not a
//! model-specific tokenizer; it is intentionally simple and conservative. A
//! pluggable model tokenizer can replace [`estimate_tokens`] later without
//! changing the packing logic.

use akidb_common::VectorId;

/// A traceable pointer from a packed span back to its origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Citation {
    /// Origin document/source identifier (URI, path, etc.).
    pub source_uri: String,
    /// Optional character span `[start, end)` within the source.
    pub span: Option<(usize, usize)>,
    /// Optional source version / commit / epoch for reproducibility.
    pub version: Option<String>,
}

impl Citation {
    pub fn new(source_uri: impl Into<String>) -> Self {
        Self {
            source_uri: source_uri.into(),
            span: None,
            version: None,
        }
    }

    pub fn with_span(mut self, start: usize, end: usize) -> Self {
        self.span = Some((start, end));
        self
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Compact human/LLM-readable marker, e.g. `file.rs@v2#10:42`.
    fn marker(&self) -> String {
        let mut m = self.source_uri.clone();
        if let Some(v) = &self.version {
            m.push('@');
            m.push_str(v);
        }
        if let Some((s, e)) = self.span {
            m.push('#');
            m.push_str(&s.to_string());
            m.push(':');
            m.push_str(&e.to_string());
        }
        m
    }
}

/// One ranked retrieval result to be packed.
#[derive(Debug, Clone)]
pub struct Passage {
    pub id: VectorId,
    pub text: String,
    pub score: f32,
    pub citation: Citation,
}

impl Passage {
    pub fn new(id: VectorId, text: impl Into<String>, score: f32, citation: Citation) -> Self {
        Self {
            id,
            text: text.into(),
            score: if score.is_finite() { score } else { 0.0 },
            citation,
        }
    }
}

/// How each passage is rendered into the packed context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackStrategy {
    /// Text prefixed with a compact inline citation marker.
    Citation,
    /// Text only, no markers (most token-efficient).
    Compact,
    /// Text followed by a full citation line (source, version, span).
    Full,
}

/// Packing configuration.
#[derive(Debug, Clone)]
pub struct PackerConfig {
    /// Maximum tokens (per [`estimate_tokens`]) the assembled context may use.
    pub token_budget: usize,
    pub strategy: PackStrategy,
    /// Separator inserted between passages.
    pub separator: String,
}

impl Default for PackerConfig {
    fn default() -> Self {
        Self {
            token_budget: 1024,
            strategy: PackStrategy::Citation,
            separator: "\n\n".to_string(),
        }
    }
}

impl PackerConfig {
    pub fn new(token_budget: usize) -> Self {
        Self {
            token_budget,
            ..Default::default()
        }
    }

    pub fn with_strategy(mut self, strategy: PackStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn with_separator(mut self, separator: impl Into<String>) -> Self {
        self.separator = separator.into();
        self
    }
}

/// The assembled, LLM-ready context plus its provenance.
#[derive(Debug, Clone)]
pub struct ContextPack {
    /// Assembled context text, ready to drop into a prompt.
    pub text: String,
    /// Citations for included passages, in inclusion order.
    pub citations: Vec<Citation>,
    /// Ids of passages included, in order.
    pub included: Vec<VectorId>,
    /// Ids of passages dropped because they did not fit the budget.
    pub dropped: Vec<VectorId>,
    /// Estimated tokens used by the assembled context.
    pub used_tokens: usize,
}

/// Deterministic token estimate: count of whitespace-separated words.
///
/// This is a heuristic stand-in for a model tokenizer — adequate for budgeting
/// and fully reproducible in tests.
pub fn estimate_tokens(text: &str) -> usize {
    text.split_whitespace().count()
}

/// Render a single passage according to the strategy.
fn render(passage: &Passage, strategy: PackStrategy) -> String {
    match strategy {
        PackStrategy::Compact => passage.text.clone(),
        PackStrategy::Citation => format!("[{}] {}", passage.citation.marker(), passage.text),
        PackStrategy::Full => {
            let mut s = passage.text.clone();
            s.push_str("\n— source: ");
            s.push_str(&passage.citation.source_uri);
            if let Some(v) = &passage.citation.version {
                s.push_str(" (");
                s.push_str(v);
                s.push(')');
            }
            if let Some((start, end)) = passage.citation.span {
                s.push_str(&format!(" [{start}:{end}]"));
            }
            s
        }
    }
}

/// Pack ranked `passages` into a single context within the token budget.
///
/// Passages are considered in the given (rank) order. Each is included if it
/// fits the remaining budget; otherwise it is dropped and packing continues, so
/// a smaller later passage can still fill remaining space. Inclusion order is
/// preserved. Every included passage contributes a citation; dropped ids are
/// reported for transparency.
pub fn pack(passages: &[Passage], config: &PackerConfig) -> ContextPack {
    let sep_tokens = estimate_tokens(&config.separator);

    let mut parts: Vec<String> = Vec::new();
    let mut citations = Vec::new();
    let mut included = Vec::new();
    let mut dropped = Vec::new();
    let mut used_tokens = 0usize;

    for passage in passages {
        if passage.text.trim().is_empty() {
            dropped.push(passage.id.clone());
            continue;
        }

        let rendered = render(passage, config.strategy);
        let rendered_tokens = estimate_tokens(&rendered);
        // The separator only costs tokens when joining to an existing part.
        let added = if parts.is_empty() {
            rendered_tokens
        } else {
            rendered_tokens + sep_tokens
        };

        if used_tokens + added <= config.token_budget {
            used_tokens += added;
            parts.push(rendered);
            citations.push(passage.citation.clone());
            included.push(passage.id.clone());
        } else {
            dropped.push(passage.id.clone());
        }
    }

    ContextPack {
        text: parts.join(&config.separator),
        citations,
        included,
        dropped,
        used_tokens,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn passage(id: &str, text: &str) -> Passage {
        Passage::new(
            VectorId::new(id),
            text,
            1.0,
            Citation::new(format!("{id}.txt")),
        )
    }

    #[test]
    fn test_empty_input_produces_empty_pack() {
        let pack = pack(&[], &PackerConfig::default());
        assert!(pack.text.is_empty());
        assert!(pack.citations.is_empty());
        assert!(pack.included.is_empty());
        assert!(pack.dropped.is_empty());
        assert_eq!(pack.used_tokens, 0);
    }

    #[test]
    fn test_blank_passages_are_dropped() {
        let passages = [
            passage("blank", "   \n\t  "),
            passage("content", "useful text"),
        ];
        let cfg = PackerConfig::new(100).with_strategy(PackStrategy::Compact);

        let out = pack(&passages, &cfg);

        assert_eq!(out.included, vec![VectorId::new("content")]);
        assert_eq!(out.dropped, vec![VectorId::new("blank")]);
        assert_eq!(out.text, "useful text");
        assert_eq!(out.citations.len(), 1);
    }

    #[test]
    fn test_passage_new_sanitizes_non_finite_scores() {
        let nan = Passage::new(VectorId::new("nan"), "text", f32::NAN, Citation::new("nan"));
        let pos_inf = Passage::new(
            VectorId::new("pos-inf"),
            "text",
            f32::INFINITY,
            Citation::new("pos-inf"),
        );
        let neg_inf = Passage::new(
            VectorId::new("neg-inf"),
            "text",
            f32::NEG_INFINITY,
            Citation::new("neg-inf"),
        );

        assert_eq!(nan.score, 0.0);
        assert_eq!(pos_inf.score, 0.0);
        assert_eq!(neg_inf.score, 0.0);
    }

    #[test]
    fn test_all_fit_within_budget() {
        let passages = [passage("a", "alpha beta"), passage("b", "gamma delta")];
        let cfg = PackerConfig::new(100).with_strategy(PackStrategy::Compact);
        let out = pack(&passages, &cfg);
        assert_eq!(out.included, vec![VectorId::new("a"), VectorId::new("b")]);
        assert!(out.dropped.is_empty());
        assert_eq!(out.text, "alpha beta\n\ngamma delta");
        assert_eq!(out.citations.len(), 2);
    }

    #[test]
    fn test_budget_drops_overflowing_passages_preserving_order() {
        // Compact strategy, budget 4 words, separator "\n\n" (0 word-tokens).
        let passages = [
            passage("a", "one two"),             // 2 tokens
            passage("b", "three four five six"), // 4 tokens -> would exceed (2+4=6>4)
            passage("c", "seven two"),           // 2 tokens -> fits (2+2=4)
        ];
        let cfg = PackerConfig::new(4).with_strategy(PackStrategy::Compact);
        let out = pack(&passages, &cfg);
        assert_eq!(out.included, vec![VectorId::new("a"), VectorId::new("c")]);
        assert_eq!(out.dropped, vec![VectorId::new("b")]);
        assert_eq!(out.used_tokens, 4);
        assert_eq!(out.text, "one two\n\nseven two");
    }

    #[test]
    fn test_zero_budget_drops_everything() {
        let passages = [passage("a", "x"), passage("b", "y")];
        let out = pack(&passages, &PackerConfig::new(0));
        assert!(out.included.is_empty());
        assert_eq!(out.dropped, vec![VectorId::new("a"), VectorId::new("b")]);
        assert!(out.text.is_empty());
    }

    #[test]
    fn test_citation_strategy_includes_markers() {
        let p = Passage::new(
            VectorId::new("doc"),
            "the answer",
            0.9,
            Citation::new("guide.md")
                .with_version("v3")
                .with_span(10, 20),
        );
        let cfg = PackerConfig::new(100).with_strategy(PackStrategy::Citation);
        let out = pack(std::slice::from_ref(&p), &cfg);
        assert_eq!(out.text, "[guide.md@v3#10:20] the answer");
        assert_eq!(out.citations[0], p.citation);
    }

    #[test]
    fn test_full_strategy_appends_source_line() {
        let p = Passage::new(
            VectorId::new("doc"),
            "body text",
            0.5,
            Citation::new("file.rs").with_version("abc123"),
        );
        let cfg = PackerConfig::new(100).with_strategy(PackStrategy::Full);
        let out = pack(std::slice::from_ref(&p), &cfg);
        assert!(out.text.starts_with("body text"));
        assert!(out.text.contains("source: file.rs"));
        assert!(out.text.contains("abc123"));
    }

    #[test]
    fn test_every_included_passage_has_a_citation() {
        let passages = [
            passage("a", "one"),
            passage("b", "two"),
            passage("c", "three"),
        ];
        let cfg = PackerConfig::new(100).with_strategy(PackStrategy::Compact);
        let out = pack(&passages, &cfg);
        assert_eq!(out.included.len(), out.citations.len());
        for (id, cite) in out.included.iter().zip(out.citations.iter()) {
            assert_eq!(cite.source_uri, format!("{}.txt", id.as_str()));
        }
    }

    #[test]
    fn test_separator_tokens_count_against_budget() {
        // Separator " AND " is one word-token; with two 1-token passages the
        // total is 1 + 1 (sep) + 1 = 3.
        let passages = [passage("a", "x"), passage("b", "y")];
        let cfg = PackerConfig::new(3)
            .with_strategy(PackStrategy::Compact)
            .with_separator(" AND ");
        let out = pack(&passages, &cfg);
        assert_eq!(out.included.len(), 2);
        assert_eq!(out.used_tokens, 3);

        // Budget of 2 cannot afford the separator, so only the first fits.
        let cfg2 = PackerConfig::new(2)
            .with_strategy(PackStrategy::Compact)
            .with_separator(" AND ");
        let out2 = pack(&passages, &cfg2);
        assert_eq!(out2.included, vec![VectorId::new("a")]);
        assert_eq!(out2.dropped, vec![VectorId::new("b")]);
    }

    #[test]
    fn test_estimate_tokens_counts_words() {
        assert_eq!(estimate_tokens(""), 0);
        assert_eq!(estimate_tokens("   "), 0);
        assert_eq!(estimate_tokens("one"), 1);
        assert_eq!(estimate_tokens("one two   three"), 3);
    }
}
