//! In-memory BM25 lexical index.
//!
//! This is the lexical (keyword / exact-identifier) half of AkiDB's hybrid
//! retrieval. Dense vector search (`usearch` HNSW) is great at semantic
//! similarity but misses exact identifiers, rare tokens, and keyword-precise
//! queries — exactly the cases that matter for code and grounded RAG. BM25
//! covers those.
//!
//! The implementation is a classic inverted index with Okapi BM25 scoring. It is
//! intentionally dependency-free (std collections only) so it is easy to audit,
//! test, and run on Apple Silicon without pulling in a full-text engine. A
//! heavier backend (e.g. tantivy) can replace it later behind the same API; that
//! trade-off is tracked as an open question in the PRD.

use std::collections::HashMap;

use akidb_common::VectorId;

use crate::ScoredId;

/// Default BM25 term-frequency saturation parameter.
pub const DEFAULT_K1: f32 = 1.2;
/// Default BM25 length-normalization parameter.
pub const DEFAULT_B: f32 = 0.75;

fn normalize_k1(k1: f32) -> f32 {
    if k1.is_finite() && k1 >= 0.0 {
        k1
    } else {
        DEFAULT_K1
    }
}

fn normalize_b(b: f32) -> f32 {
    if b.is_finite() {
        b.clamp(0.0, 1.0)
    } else {
        DEFAULT_B
    }
}

fn finite_f32(score: f64) -> f32 {
    if !score.is_finite() {
        0.0
    } else if score > f32::MAX as f64 {
        f32::MAX
    } else {
        score as f32
    }
}

/// Tokenize text into lowercase identifier-aware terms.
///
/// Splitting on any non-identifier character keeps identifiers like
/// `tokenRefresh` as exact tokens while breaking `foo.bar(baz)` into
/// `foo`, `bar`, `baz`. Snake_case, kebab-case, Rust paths, and camelCase
/// identifiers keep their exact token and also emit split subterms so plain-word
/// queries can still recall them.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    for raw in text
        .split(|c: char| !is_token_char(c))
        .filter(|t| contains_semantic_token_char(t))
    {
        let token = raw.to_lowercase();
        tokens.push(token.clone());
        for part in identifier_subterms(raw) {
            let part = part.to_lowercase();
            if part != token && !part.is_empty() {
                tokens.push(part);
            }
        }
    }
    tokens
}

fn is_token_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '-' | '+' | '#' | ':')
}

fn contains_semantic_token_char(token: &str) -> bool {
    token.chars().any(char::is_alphanumeric)
}

fn identifier_subterms(raw: &str) -> Vec<String> {
    raw.split(['_', '-', '+', '#', ':'])
        .filter(|part| !part.is_empty())
        .flat_map(split_camel_case)
        .collect()
}

fn split_camel_case(segment: &str) -> Vec<String> {
    if segment.is_empty() {
        return Vec::new();
    }

    let chars: Vec<(usize, char)> = segment.char_indices().collect();
    let mut parts = Vec::new();
    let mut start = 0;

    for i in 1..chars.len() {
        let prev = chars[i - 1].1;
        let cur = chars[i].1;
        let next = chars.get(i + 1).map(|(_, c)| *c);

        let boundary = (cur.is_uppercase()
            && (prev.is_lowercase()
                || prev.is_numeric()
                || (prev.is_uppercase() && next.is_some_and(|c| c.is_lowercase()))))
            || (cur.is_numeric() && !prev.is_numeric())
            || (!cur.is_numeric() && prev.is_numeric());
        if boundary {
            let idx = chars[i].0;
            if start < idx {
                parts.push(segment[start..idx].to_string());
            }
            start = idx;
        }
    }

    if start < segment.len() {
        parts.push(segment[start..].to_string());
    }
    parts
}

/// An in-memory BM25 inverted index keyed by external [`VectorId`].
///
/// Documents are upserted by id: re-inserting an existing id replaces its text.
/// Scores are produced by [`Bm25Index::search`], highest first, with ties broken
/// by id for deterministic ordering.
#[derive(Debug, Clone)]
pub struct Bm25Index {
    k1: f32,
    b: f32,
    /// term -> (doc id -> term frequency in that doc)
    postings: HashMap<String, HashMap<VectorId, u32>>,
    /// doc id -> total token count (document length)
    doc_len: HashMap<VectorId, u32>,
    /// doc id -> the distinct terms it contains (so removal is O(unique terms))
    doc_terms: HashMap<VectorId, Vec<String>>,
    /// sum of all document lengths, for the average-document-length term
    total_len: u64,
}

impl Default for Bm25Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Bm25Index {
    /// Create an index with the default BM25 parameters (`k1=1.2`, `b=0.75`).
    pub fn new() -> Self {
        Self::with_params(DEFAULT_K1, DEFAULT_B)
    }

    /// Create an index with explicit BM25 parameters.
    ///
    /// `k1` controls term-frequency saturation; `b` controls how strongly long
    /// documents are penalized (`b=0` disables length normalization).
    pub fn with_params(k1: f32, b: f32) -> Self {
        Self {
            k1: normalize_k1(k1),
            b: normalize_b(b),
            postings: HashMap::new(),
            doc_len: HashMap::new(),
            doc_terms: HashMap::new(),
            total_len: 0,
        }
    }

    /// Number of documents currently indexed.
    pub fn len(&self) -> usize {
        self.doc_len.len()
    }

    /// Whether the index holds no documents.
    pub fn is_empty(&self) -> bool {
        self.doc_len.is_empty()
    }

    /// Whether a document with this id is indexed.
    pub fn contains(&self, id: &VectorId) -> bool {
        self.doc_len.contains_key(id)
    }

    /// Average document length across the index (`0.0` when empty).
    pub fn avgdl(&self) -> f32 {
        let n = self.doc_len.len();
        if n == 0 {
            0.0
        } else {
            self.total_len as f32 / n as f32
        }
    }

    /// Insert or replace the text indexed under `id`.
    ///
    /// Re-inserting an existing id is an upsert: the previous content is removed
    /// first, so document statistics stay correct.
    pub fn insert(&mut self, id: VectorId, text: &str) {
        if self.contains(&id) {
            self.remove(&id);
        }

        let tokens = tokenize(text);
        if tokens.is_empty() {
            return;
        }
        let len = tokens.len() as u32;

        // Build the per-document term-frequency map.
        let mut tf: HashMap<String, u32> = HashMap::new();
        for tok in tokens {
            *tf.entry(tok).or_insert(0) += 1;
        }

        let mut terms = Vec::with_capacity(tf.len());
        for (term, freq) in tf {
            self.postings
                .entry(term.clone())
                .or_default()
                .insert(id.clone(), freq);
            terms.push(term);
        }

        self.doc_terms.insert(id.clone(), terms);
        self.doc_len.insert(id, len);
        self.total_len += len as u64;
    }

    /// Remove a document from the index. Returns `true` if it was present.
    pub fn remove(&mut self, id: &VectorId) -> bool {
        let Some(len) = self.doc_len.remove(id) else {
            return false;
        };
        self.total_len -= len as u64;

        if let Some(terms) = self.doc_terms.remove(id) {
            for term in terms {
                if let Some(docs) = self.postings.get_mut(&term) {
                    docs.remove(id);
                    if docs.is_empty() {
                        self.postings.remove(&term);
                    }
                }
            }
        }
        true
    }

    /// Inverse document frequency for a term using the BM25 "plus-one" smoothing,
    /// `ln(1 + (N - df + 0.5) / (df + 0.5))`, which is always non-negative and so
    /// avoids the negative-IDF pathology of the raw Robertson/Spärck-Jones form.
    fn idf(&self, df: usize) -> f64 {
        let n = self.doc_len.len() as f64;
        let df = df as f64;
        (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
    }

    /// Score `query` against the index and return the top `top_k` documents,
    /// highest score first. Ties are broken by ascending id for determinism.
    ///
    /// Only documents containing at least one query term are scored; documents
    /// with no overlap never appear in the results.
    pub fn search(&self, query: &str, top_k: usize) -> Vec<ScoredId> {
        if self.is_empty() || top_k == 0 {
            return Vec::new();
        }

        let query_terms = tokenize(query);
        if query_terms.is_empty() {
            return Vec::new();
        }

        let avgdl = self.avgdl() as f64;
        let k1 = self.k1 as f64;
        let b = self.b as f64;

        // Accumulate BM25 score per document over the query terms.
        let mut scores: HashMap<VectorId, f64> = HashMap::new();
        for term in &query_terms {
            let Some(docs) = self.postings.get(term) else {
                continue;
            };
            let idf = self.idf(docs.len());
            for (doc_id, &tf) in docs {
                let tf = tf as f64;
                let dl = self.doc_len[doc_id] as f64;
                let denom = tf + k1 * (1.0 - b + b * dl / avgdl);
                let contribution = idf * (tf * (k1 + 1.0)) / denom;
                if !contribution.is_finite() {
                    continue;
                }
                *scores.entry(doc_id.clone()).or_insert(0.0) += contribution;
            }
        }

        let mut results: Vec<ScoredId> = scores
            .into_iter()
            .map(|(id, score)| ScoredId::new(id, finite_f32(score)))
            .collect();

        // Sort by score descending, then by id ascending for stable tie-breaking.
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.as_str().cmp(b.id.as_str()))
        });
        results.truncate(top_k);
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(s: &str) -> VectorId {
        VectorId::new(s)
    }

    #[test]
    fn test_tokenize_lowercases_and_splits_on_non_alphanumeric() {
        assert_eq!(tokenize("Hello, World!"), vec!["hello", "world"]);
        assert_eq!(tokenize("foo.bar(baz)"), vec!["foo", "bar", "baz"]);
        // identifiers without separators keep the exact lowercase token
        assert!(tokenize("tokenRefresh").contains(&"tokenrefresh".to_string()));
        assert_eq!(tokenize("   "), Vec::<String>::new());
        assert_eq!(tokenize(""), Vec::<String>::new());
    }

    #[test]
    fn test_tokenize_preserves_camel_case_identifier_and_parts() {
        assert_eq!(
            tokenize("tokenRefresh"),
            vec!["tokenrefresh", "token", "refresh"]
        );
    }

    #[test]
    fn test_tokenize_preserves_digit_boundary_identifier_parts() {
        assert_eq!(
            tokenize("Gemma2Model"),
            vec!["gemma2model", "gemma", "2", "model"]
        );
    }

    #[test]
    fn test_tokenize_preserves_acronym_digit_identifier_parts() {
        assert_eq!(
            tokenize("HTTP2Server"),
            vec!["http2server", "http", "2", "server"]
        );
    }

    #[test]
    fn test_tokenize_preserves_snake_case_identifier_and_parts() {
        assert_eq!(
            tokenize("contract_amount"),
            vec!["contract_amount", "contract", "amount"]
        );
    }

    #[test]
    fn test_tokenize_preserves_kebab_case_identifier_and_parts() {
        assert_eq!(tokenize("ax-code"), vec!["ax-code", "ax", "code"]);
    }

    #[test]
    fn test_tokenize_preserves_rust_path_identifier_and_parts() {
        assert_eq!(
            tokenize("draft_model::decode"),
            vec!["draft_model::decode", "draft", "model", "decode"]
        );
    }

    #[test]
    fn test_tokenize_preserves_code_language_identifiers() {
        assert_eq!(tokenize("C++"), vec!["c++", "c"]);
        assert_eq!(tokenize("C#"), vec!["c#", "c"]);
    }

    #[test]
    fn test_tokenize_drops_hyphen_only_runs() {
        assert_eq!(tokenize("---"), Vec::<String>::new());
        assert_eq!(tokenize("alpha --- beta"), vec!["alpha", "beta"]);
    }

    #[test]
    fn test_tokenize_drops_underscore_only_runs() {
        assert_eq!(tokenize("___"), Vec::<String>::new());
        assert_eq!(tokenize("alpha ___ beta"), vec!["alpha", "beta"]);
        assert_eq!(tokenize("__init__"), vec!["__init__", "init"]);
    }

    #[test]
    fn test_empty_index_returns_no_results() {
        let index = Bm25Index::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert_eq!(index.avgdl(), 0.0);
        assert!(index.search("anything", 10).is_empty());
    }

    #[test]
    fn test_top_k_zero_or_empty_query_returns_empty() {
        let mut index = Bm25Index::new();
        index.insert(id("a"), "the quick brown fox");
        assert!(index.search("fox", 0).is_empty());
        assert!(index.search("", 10).is_empty());
        assert!(index.search("   !!!  ", 10).is_empty());
    }

    #[test]
    fn test_single_document_match_and_miss() {
        let mut index = Bm25Index::new();
        index.insert(id("doc1"), "the quick brown fox");

        let hits = index.search("fox", 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, id("doc1"));
        assert!(hits[0].score > 0.0);

        // A query term absent from every document yields no results.
        assert!(index.search("elephant", 10).is_empty());
    }

    #[test]
    fn test_empty_document_is_not_indexed_and_upsert_removes_existing() {
        let mut index = Bm25Index::new();

        index.insert(id("empty"), "   !!!   ");
        assert_eq!(index.len(), 0);
        assert!(!index.contains(&id("empty")));
        assert_eq!(index.avgdl(), 0.0);

        index.insert(id("doc"), "alpha beta");
        assert_eq!(index.len(), 1);
        assert!(index
            .search("alpha", 10)
            .iter()
            .any(|hit| hit.id == id("doc")));

        index.insert(id("doc"), " \n\t ");
        assert_eq!(index.len(), 0);
        assert!(!index.contains(&id("doc")));
        assert!(index.search("alpha", 10).is_empty());
    }

    #[test]
    fn test_hyphen_only_document_is_not_indexed() {
        let mut index = Bm25Index::new();

        index.insert(id("separator"), "--- ---");

        assert!(index.is_empty());
        assert!(index.search("---", 10).is_empty());
    }

    #[test]
    fn test_underscore_only_document_is_not_indexed() {
        let mut index = Bm25Index::new();

        index.insert(id("separator"), "___ ___");

        assert!(index.is_empty());
        assert!(index.search("___", 10).is_empty());
    }

    #[test]
    fn test_higher_term_frequency_scores_higher() {
        let mut index = Bm25Index::new();
        // Same length-ish docs; doc_many mentions "vector" more often.
        index.insert(id("doc_many"), "vector vector vector search");
        index.insert(id("doc_few"), "vector search engine here");

        let hits = index.search("vector", 10);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, id("doc_many"));
        assert_eq!(hits[1].id, id("doc_few"));
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn test_exact_snake_case_identifier_ranks_above_split_words() {
        let mut index = Bm25Index::new();
        index.insert(id("z_exact"), "contract_amount");
        index.insert(id("a_words"), "contract amount");

        let hits = index.search("contract_amount", 10);

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, id("z_exact"));
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn test_exact_kebab_case_identifier_ranks_above_split_words() {
        let mut index = Bm25Index::new();
        index.insert(id("z_exact"), "ax-code");
        index.insert(id("a_words"), "ax code");

        let hits = index.search("ax-code", 10);

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, id("z_exact"));
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn test_exact_rust_path_identifier_ranks_above_split_words() {
        let mut index = Bm25Index::new();
        index.insert(id("z_exact"), "draft_model::decode");
        index.insert(id("a_words"), "draft model decode");

        let hits = index.search("draft_model::decode", 10);

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, id("z_exact"));
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn test_exact_cpp_identifier_ranks_above_plain_c() {
        let mut index = Bm25Index::new();
        index.insert(id("z_cpp"), "C++ parser bindings");
        index.insert(id("a_c"), "C parser bindings");

        let hits = index.search("C++", 10);

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, id("z_cpp"));
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn test_camel_case_identifier_matches_split_word_query() {
        let mut index = Bm25Index::new();
        index.insert(id("doc"), "tokenRefresh");

        let hits = index.search("token refresh", 10);

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, id("doc"));
    }

    #[test]
    fn test_kebab_case_identifier_matches_split_word_query() {
        let mut index = Bm25Index::new();
        index.insert(id("doc"), "upload-gateway");

        let hits = index.search("upload gateway", 10);

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, id("doc"));
    }

    #[test]
    fn test_digit_boundary_identifier_matches_split_word_query() {
        let mut index = Bm25Index::new();
        index.insert(id("z_exact"), "Gemma2Model");
        index.insert(id("a_partial"), "generic model");

        let hits = index.search("gemma 2 model", 10);

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, id("z_exact"));
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn test_acronym_digit_identifier_matches_split_word_query() {
        let mut index = Bm25Index::new();
        index.insert(id("z_exact"), "HTTP2Server");
        index.insert(id("a_partial"), "generic server");

        let hits = index.search("http 2 server", 10);

        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].id, id("z_exact"));
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn test_rare_term_outweighs_common_term_via_idf() {
        let mut index = Bm25Index::new();
        // "common" appears in all docs (low IDF); "rare" appears in one (high IDF).
        index.insert(id("d1"), "common token rare token");
        index.insert(id("d2"), "common token filler words");
        index.insert(id("d3"), "common token more filler");

        // Query both terms; the doc with the rare term should win clearly.
        let hits = index.search("common rare", 10);
        assert_eq!(hits[0].id, id("d1"));

        // The rare term alone only matches d1.
        let rare_only = index.search("rare", 10);
        assert_eq!(rare_only.len(), 1);
        assert_eq!(rare_only[0].id, id("d1"));
    }

    #[test]
    fn test_length_normalization_prefers_shorter_doc() {
        let mut index = Bm25Index::new();
        // Both contain "needle" once, but doc_long is padded with filler.
        index.insert(id("short"), "needle haystack");
        index.insert(
            id("long"),
            "needle haystack filler filler filler filler filler filler filler filler",
        );

        let hits = index.search("needle", 10);
        assert_eq!(hits[0].id, id("short"));
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn test_top_k_limits_results() {
        let mut index = Bm25Index::new();
        for i in 0..5 {
            index.insert(id(&format!("doc{i}")), "alpha beta gamma");
        }
        let hits = index.search("alpha", 3);
        assert_eq!(hits.len(), 3);
    }

    #[test]
    fn test_tie_break_is_deterministic_by_id() {
        let mut index = Bm25Index::new();
        // Identical content => identical scores => deterministic id ordering.
        index.insert(id("c"), "same same content");
        index.insert(id("a"), "same same content");
        index.insert(id("b"), "same same content");

        let hits = index.search("content", 10);
        let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_invalid_bm25_params_are_normalized() {
        let mut index = Bm25Index::with_params(f32::NAN, f32::INFINITY);
        index.insert(id("a"), "alpha alpha");
        index.insert(id("b"), "alpha beta");

        let hits = index.search("alpha", 10);
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().all(|hit| hit.score.is_finite()));

        let mut clamped = Bm25Index::with_params(1.2, -10.0);
        clamped.insert(id("a"), "needle short");
        clamped.insert(id("b"), "needle long long long long");
        let clamped_hits = clamped.search("needle", 10);
        assert_eq!(clamped_hits.len(), 2);
        assert!(clamped_hits.iter().all(|hit| hit.score.is_finite()));
    }

    #[test]
    fn test_bm25_score_conversion_stays_finite() {
        assert_eq!(finite_f32(f64::INFINITY), 0.0);
        assert_eq!(finite_f32(f32::MAX as f64 * 2.0), f32::MAX);
        assert_eq!(finite_f32(1.25), 1.25);
    }

    #[test]
    fn test_upsert_replaces_document_content() {
        let mut index = Bm25Index::new();
        index.insert(id("doc"), "alpha alpha alpha");
        assert_eq!(index.len(), 1);

        // Re-insert with new content; old terms must no longer match.
        index.insert(id("doc"), "beta gamma");
        assert_eq!(index.len(), 1, "upsert must not create a second document");
        assert!(index.search("alpha", 10).is_empty());

        let hits = index.search("beta", 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, id("doc"));
    }

    #[test]
    fn test_remove_updates_index_and_stats() {
        let mut index = Bm25Index::new();
        index.insert(id("d1"), "one two three");
        index.insert(id("d2"), "four five six seven");
        assert_eq!(index.len(), 2);
        let avg_before = index.avgdl();
        assert!((avg_before - 3.5).abs() < 1e-6);

        assert!(index.remove(&id("d2")));
        assert_eq!(index.len(), 1);
        assert!((index.avgdl() - 3.0).abs() < 1e-6);
        assert!(index.search("four", 10).is_empty());
        assert!(!index.contains(&id("d2")));

        // Removing a missing id is a no-op returning false.
        assert!(!index.remove(&id("missing")));
    }

    #[test]
    fn test_postings_pruned_when_last_doc_removed() {
        let mut index = Bm25Index::new();
        index.insert(id("only"), "unique_term shared");
        index.insert(id("other"), "shared word");
        index.remove(&id("only"));

        // The term that only lived in the removed doc must be gone, so a query
        // for it returns nothing rather than touching a stale posting.
        assert!(index.search("unique_term", 10).is_empty());
        // Shared term still works for the surviving doc.
        let hits = index.search("shared", 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, id("other"));
    }

    #[test]
    fn test_repeated_query_terms_do_not_double_count_incorrectly() {
        // Repeating a query term increases its weight (standard BM25 behavior),
        // but must remain finite and ordered sensibly.
        let mut index = Bm25Index::new();
        index.insert(id("d1"), "rust rust rust");
        index.insert(id("d2"), "rust language");

        let single = index.search("rust", 10);
        let doubled = index.search("rust rust", 10);
        assert_eq!(single[0].id, id("d1"));
        assert_eq!(doubled[0].id, id("d1"));
        // Doubling the query term roughly doubles each document's score.
        assert!(doubled[0].score > single[0].score);
    }
}
