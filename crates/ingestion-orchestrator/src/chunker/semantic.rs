//! Semantic Chunker with Sentence Boundary Awareness
//!
//! Splits text into chunks at sentence boundaries, targeting a specific
//! token count with configurable overlap.

use std::sync::OnceLock;
use tiktoken_rs::CoreBPE;
use unicode_segmentation::UnicodeSegmentation;

use crate::chunker::Chunk;
use crate::config::ChunkerConfig;

/// Global tokenizer instance (cl100k_base - used by text-embedding-ada-002 and GPT-4)
static TOKENIZER: OnceLock<CoreBPE> = OnceLock::new();

/// Get or initialize the tokenizer
fn get_tokenizer() -> &'static CoreBPE {
    TOKENIZER
        .get_or_init(|| tiktoken_rs::cl100k_base().expect("Failed to load cl100k_base tokenizer"))
}

/// Semantic chunker that respects sentence boundaries
pub struct SemanticChunker {
    config: ChunkerConfig,
}

impl SemanticChunker {
    /// Create a new semantic chunker with the given configuration
    pub fn new(config: ChunkerConfig) -> Self {
        Self { config }
    }

    /// Create with default configuration
    pub fn default_config() -> Self {
        Self::new(ChunkerConfig::default())
    }

    /// Chunk text into semantically meaningful pieces
    pub fn chunk(&self, text: &str) -> Vec<Chunk> {
        if text.is_empty() {
            return Vec::new();
        }

        // Split into sentences
        let sentences: Vec<&str> = text.unicode_sentences().collect();

        if sentences.is_empty() {
            // Fallback: treat entire text as one chunk
            return vec![Chunk {
                text: text.to_string(),
                start_offset: 0,
                end_offset: text.len(),
                token_count: count_tokens(text),
                index: 0,
            }];
        }

        let mut chunks = Vec::new();
        let mut current_chunk = String::new();
        let mut current_start: usize = 0; // Byte offset in original text
        let mut current_end: usize = 0; // Byte offset of end of current chunk content
        let mut chunk_index = 0;

        // FIX: Track actual byte offsets in original text using find()
        let mut search_start: usize = 0;

        for sentence in sentences.iter() {
            // Find actual position of this sentence in original text
            let sentence_offset = match text[search_start..].find(sentence) {
                Some(offset) => search_start + offset,
                None => search_start, // Fallback if not found (shouldn't happen)
            };
            let sentence_end = sentence_offset + sentence.len();
            search_start = sentence_end; // Move search position past this sentence

            let sentence_tokens = count_tokens(sentence);
            let current_tokens = count_tokens(&current_chunk);

            // If adding this sentence would exceed target, create a chunk
            if current_tokens > 0 && current_tokens + sentence_tokens > self.config.target_tokens {
                // Create chunk from current content
                let chunk_text = current_chunk.trim().to_string();
                if !chunk_text.is_empty() {
                    chunks.push(Chunk {
                        text: chunk_text.clone(),
                        start_offset: current_start,
                        end_offset: current_end, // FIX: Use tracked end offset
                        token_count: count_tokens(&chunk_text),
                        index: chunk_index,
                    });
                    chunk_index += 1;
                }

                // Start new chunk with overlap. Keep the overlap as an exact
                // substring of the original text so citation offsets remain valid.
                let overlap = get_overlap_span(
                    &current_chunk,
                    self.config.min_overlap,
                    self.config.max_overlap,
                );

                match overlap {
                    Some((overlap_start, overlap_text)) => {
                        current_start += overlap_start;
                        current_chunk = overlap_text;
                    }
                    None => {
                        current_chunk.clear();
                        current_start = sentence_offset;
                    }
                }
            }

            // If this is the start of a new chunk (empty), set start offset
            if current_chunk.is_empty() {
                current_start = sentence_offset;
            }

            let append_start = if current_chunk.is_empty() {
                sentence_offset
            } else {
                current_end
            };
            current_chunk.push_str(&text[append_start..sentence_end]);
            current_end = sentence_end; // FIX: Track actual end offset
        }

        // Don't forget the last chunk
        let chunk_text = current_chunk.trim().to_string();
        if !chunk_text.is_empty() {
            chunks.push(Chunk {
                text: chunk_text.clone(),
                start_offset: current_start,
                end_offset: current_end, // FIX: Use tracked end offset instead of text.len()
                token_count: count_tokens(&chunk_text),
                index: chunk_index,
            });
        }

        chunks
    }
}

/// Count tokens using tiktoken cl100k_base tokenizer
fn count_tokens(text: &str) -> usize {
    if text.is_empty() {
        return 0;
    }
    get_tokenizer().encode_with_special_tokens(text).len()
}

/// Fallback: Estimate token count (rough approximation: ~4 chars per token)
/// Used when quick estimation is needed and accuracy is less critical
fn estimate_tokens_fast(text: &str) -> usize {
    (text.len() + 3) / 4
}

/// Get overlap text from the end of current chunk.
///
/// Returns the byte offset within `text` and the exact substring to preserve
/// original spacing for source-span citations.
fn get_overlap_span(text: &str, min_tokens: usize, max_tokens: usize) -> Option<(usize, String)> {
    let sentences: Vec<&str> = text.unicode_sentences().collect();
    if sentences.is_empty() {
        return None;
    }

    let mut sentence_spans = Vec::with_capacity(sentences.len());
    let mut search_start = 0;
    for sentence in sentences {
        let sentence_start = match text[search_start..].find(sentence) {
            Some(offset) => search_start + offset,
            None => search_start,
        };
        let sentence_end = sentence_start + sentence.len();
        sentence_spans.push((sentence_start, sentence_end, sentence));
        search_start = sentence_end;
    }

    let mut overlap_tokens = 0;
    let mut overlap_start = None;
    let mut overlap_end = None;

    // Take sentences from the end until we have enough overlap
    for (sentence_start, sentence_end, sentence) in sentence_spans.iter().rev() {
        let sentence_tokens = count_tokens(sentence);
        if overlap_tokens + sentence_tokens > max_tokens {
            break;
        }
        overlap_start = Some(*sentence_start);
        overlap_end.get_or_insert(*sentence_end);
        overlap_tokens += sentence_tokens;
        if overlap_tokens >= min_tokens {
            break;
        }
    }

    match (overlap_start, overlap_end) {
        (Some(start), Some(end)) => Some((start, text[start..end].to_string())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_text() {
        let chunker = SemanticChunker::default_config();
        let chunks = chunker.chunk("");
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_single_sentence() {
        let chunker = SemanticChunker::default_config();
        let chunks = chunker.chunk("This is a single sentence.");
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].text.contains("single sentence"));
    }

    #[test]
    fn test_multiple_sentences() {
        let chunker = SemanticChunker::new(ChunkerConfig {
            target_tokens: 20, // Small for testing
            min_overlap: 5,
            max_overlap: 10,
        });

        let text = "First sentence here. Second sentence follows. Third one too. And a fourth.";
        let chunks = chunker.chunk(text);

        // Should create multiple chunks
        assert!(chunks.len() >= 1);

        // Each chunk should have valid indices
        for chunk in &chunks {
            assert!(!chunk.text.is_empty());
            assert!(chunk.token_count > 0);
        }
    }

    #[test]
    fn test_offsets_round_trip_repeated_unicode_sentences() {
        let chunker = SemanticChunker::new(ChunkerConfig {
            target_tokens: 12,
            min_overlap: 0,
            max_overlap: 0,
        });

        let text = "Alpha repeats.  Alpha repeats.\nBeta uses unicode 雪. Gamma closes.";
        let chunks = chunker.chunk(text);

        assert!(chunks.len() > 1);
        for chunk in chunks {
            assert_eq!(
                text[chunk.start_offset..chunk.end_offset].trim(),
                chunk.text,
                "chunk {} offsets should round-trip to original text",
                chunk.index
            );
        }
    }

    #[test]
    fn test_offsets_round_trip_with_overlap() {
        let chunker = SemanticChunker::new(ChunkerConfig {
            target_tokens: 8,
            min_overlap: 1,
            max_overlap: 8,
        });

        let text = "Alpha one. Beta two. Gamma three. Delta four.";
        let chunks = chunker.chunk(text);

        assert!(chunks.len() > 1);
        for chunk in chunks {
            assert_eq!(
                text[chunk.start_offset..chunk.end_offset].trim(),
                chunk.text,
                "chunk {} offsets should round-trip when overlap is enabled",
                chunk.index
            );
        }
    }

    #[test]
    fn test_count_tokens() {
        assert_eq!(count_tokens(""), 0);
        assert!(count_tokens("test") > 0);
        // tiktoken gives accurate token counts
        let tokens = count_tokens("This is a test sentence.");
        assert!(tokens > 0 && tokens < 20); // Should be around 5-7 tokens
    }

    #[test]
    fn test_estimate_tokens_fast() {
        assert_eq!(estimate_tokens_fast(""), 0);
        assert_eq!(estimate_tokens_fast("test"), 1);
        assert_eq!(estimate_tokens_fast("this is a test"), 4); // 14 chars / 4 = ~4 tokens
    }
}
