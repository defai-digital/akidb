//! Streaming validation for logical knowledge-generation bundles.
//!
//! The reader deliberately consumes logical NDJSON records rather than
//! extracting an archive of engine files. This removes archive path traversal
//! and symlink classes of bugs and keeps generations rebuildable across AkiDB
//! versions and CPU architectures.

use akidb_contracts::{
    ContractViolation, KnowledgeBundleCompression, KnowledgeBundleEntry, KnowledgeBundleFormat,
    KnowledgeBundleHeader, KnowledgeGenerationManifest,
};
use std::io::{BufRead, BufReader, Read};
use thiserror::Error;

const MIB: u64 = 1024 * 1024;

/// Resource limits applied after the immutable object's compressed size and
/// checksum have been verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KnowledgeBundleReadLimits {
    /// Maximum bytes in one decoded NDJSON line, excluding the LF delimiter.
    pub max_line_bytes: usize,
    /// Absolute cap on decoded bytes.
    pub max_decoded_bytes: u64,
    /// Maximum decoded/compressed expansion ratio for zstd streams.
    pub max_expansion_ratio: u64,
    /// Maximum total logical records, nodes, and edges.
    pub max_entries: u64,
}

impl Default for KnowledgeBundleReadLimits {
    fn default() -> Self {
        Self {
            max_line_bytes: 32 * MIB as usize,
            max_decoded_bytes: 1024 * 1024 * MIB,
            max_expansion_ratio: 256,
            max_entries: 100_000_000,
        }
    }
}

/// Counts observed while validating a complete bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeBundleSummary {
    pub header: KnowledgeBundleHeader,
    pub record_count: u64,
    pub node_count: u64,
    pub edge_count: u64,
    pub decoded_bytes: u64,
}

#[derive(Debug, Error)]
pub enum KnowledgeBundleReadError {
    #[error("unsupported knowledge bundle format")]
    UnsupportedFormat,

    #[error("invalid knowledge bundle limits: {0}")]
    InvalidLimits(String),

    #[error("knowledge bundle I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("knowledge bundle line {line} exceeds the {maximum}-byte limit")]
    LineTooLong { line: u64, maximum: usize },

    #[error("knowledge bundle exceeds the {maximum}-byte decoded limit")]
    DecodedSizeExceeded { maximum: u64 },

    #[error("knowledge bundle line {line} is empty")]
    EmptyLine { line: u64 },

    #[error("knowledge bundle line {line} must end with LF")]
    MissingLineFeed { line: u64 },

    #[error("knowledge bundle line {line} uses CRLF; canonical bundles require LF")]
    NonCanonicalLineEnding { line: u64 },

    #[error("invalid JSON on knowledge bundle line {line}: {source}")]
    Json {
        line: u64,
        #[source]
        source: serde_json::Error,
    },

    #[error("invalid contract on knowledge bundle line {line}: {source}")]
    Contract {
        line: u64,
        #[source]
        source: ContractViolation,
    },

    #[error("knowledge bundle header must be the first and only header")]
    HeaderOrder,

    #[error("knowledge bundle has no header")]
    MissingHeader,

    #[error(
        "knowledge bundle entry order regressed on line {line}; expected records, then nodes, then edges"
    )]
    EntryOrder { line: u64 },

    #[error(
        "knowledge bundle {kind} IDs must be strictly sorted; line {line} has {actual} after {previous}"
    )]
    IdOrder {
        line: u64,
        kind: &'static str,
        previous: String,
        actual: String,
    },

    #[error("knowledge bundle {kind} count exceeds declared count {declared} on line {line}")]
    DeclaredCountExceeded {
        line: u64,
        kind: &'static str,
        declared: u64,
    },

    #[error("knowledge bundle entry count exceeds configured maximum {maximum}")]
    EntryLimitExceeded { maximum: u64 },

    #[error("knowledge bundle {kind} count mismatch: declared {declared}, observed {observed}")]
    CountMismatch {
        kind: &'static str,
        declared: u64,
        observed: u64,
    },

    #[error("knowledge bundle consumer rejected line {line}: {message}")]
    Consumer { line: u64, message: String },
}

/// Validate and consume one bundle with conservative default resource limits.
pub fn consume_knowledge_bundle<R, F>(
    reader: R,
    manifest: &KnowledgeGenerationManifest,
    consume: F,
) -> Result<KnowledgeBundleSummary, KnowledgeBundleReadError>
where
    R: Read,
    F: FnMut(KnowledgeBundleEntry) -> Result<(), String>,
{
    consume_knowledge_bundle_with_limits(
        reader,
        manifest,
        KnowledgeBundleReadLimits::default(),
        consume,
    )
}

/// Validate and consume one bundle without loading it fully into memory.
///
/// The immutable object's byte length and SHA-256 must be checked before this
/// function is called. The consumer is invoked only after each line passes
/// schema, cross-manifest, ordering, and resource validation.
pub fn consume_knowledge_bundle_with_limits<R, F>(
    reader: R,
    manifest: &KnowledgeGenerationManifest,
    limits: KnowledgeBundleReadLimits,
    consume: F,
) -> Result<KnowledgeBundleSummary, KnowledgeBundleReadError>
where
    R: Read,
    F: FnMut(KnowledgeBundleEntry) -> Result<(), String>,
{
    validate_limits(limits)?;
    manifest
        .validate()
        .map_err(|source| KnowledgeBundleReadError::Contract { line: 0, source })?;
    if manifest.bundle_format != KnowledgeBundleFormat::NdjsonV1 {
        return Err(KnowledgeBundleReadError::UnsupportedFormat);
    }

    let decoded_limit = decoded_limit(manifest, limits)?;
    match manifest.bundle_compression {
        KnowledgeBundleCompression::None => consume_lines(
            BufReader::new(reader),
            manifest,
            limits,
            decoded_limit,
            consume,
        ),
        KnowledgeBundleCompression::Zstd => {
            let decoder = zstd::stream::read::Decoder::new(reader)?;
            consume_lines(
                BufReader::new(decoder),
                manifest,
                limits,
                decoded_limit,
                consume,
            )
        }
    }
}

fn validate_limits(limits: KnowledgeBundleReadLimits) -> Result<(), KnowledgeBundleReadError> {
    if limits.max_line_bytes == 0 {
        return Err(KnowledgeBundleReadError::InvalidLimits(
            "max_line_bytes must be greater than zero".to_string(),
        ));
    }
    if limits.max_decoded_bytes == 0 {
        return Err(KnowledgeBundleReadError::InvalidLimits(
            "max_decoded_bytes must be greater than zero".to_string(),
        ));
    }
    if limits.max_expansion_ratio == 0 {
        return Err(KnowledgeBundleReadError::InvalidLimits(
            "max_expansion_ratio must be greater than zero".to_string(),
        ));
    }
    if limits.max_entries == 0 {
        return Err(KnowledgeBundleReadError::InvalidLimits(
            "max_entries must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn decoded_limit(
    manifest: &KnowledgeGenerationManifest,
    limits: KnowledgeBundleReadLimits,
) -> Result<u64, KnowledgeBundleReadError> {
    let object_bound = match manifest.bundle_compression {
        KnowledgeBundleCompression::None => manifest.bundle.size_bytes,
        KnowledgeBundleCompression::Zstd => manifest
            .bundle
            .size_bytes
            .checked_mul(limits.max_expansion_ratio)
            .ok_or(KnowledgeBundleReadError::DecodedSizeExceeded {
                maximum: limits.max_decoded_bytes,
            })?,
    };
    Ok(object_bound.min(limits.max_decoded_bytes))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Section {
    Record,
    Node,
    Edge,
}

fn consume_lines<B, F>(
    mut reader: B,
    manifest: &KnowledgeGenerationManifest,
    limits: KnowledgeBundleReadLimits,
    decoded_limit: u64,
    mut consume: F,
) -> Result<KnowledgeBundleSummary, KnowledgeBundleReadError>
where
    B: BufRead,
    F: FnMut(KnowledgeBundleEntry) -> Result<(), String>,
{
    let mut line_number = 0_u64;
    let mut decoded_bytes = 0_u64;
    let mut header: Option<KnowledgeBundleHeader> = None;
    let mut current_section: Option<Section> = None;
    let mut last_record_id: Option<String> = None;
    let mut last_node_id: Option<String> = None;
    let mut last_edge_id: Option<String> = None;
    let mut record_count = 0_u64;
    let mut node_count = 0_u64;
    let mut edge_count = 0_u64;

    loop {
        let mut line = Vec::new();
        let maximum_read = limits.max_line_bytes.saturating_add(2) as u64;
        let read = std::io::Read::by_ref(&mut reader)
            .take(maximum_read)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        line_number = line_number.saturating_add(1);
        decoded_bytes = decoded_bytes.checked_add(read as u64).ok_or(
            KnowledgeBundleReadError::DecodedSizeExceeded {
                maximum: decoded_limit,
            },
        )?;
        if decoded_bytes > decoded_limit {
            return Err(KnowledgeBundleReadError::DecodedSizeExceeded {
                maximum: decoded_limit,
            });
        }
        if line.last() != Some(&b'\n') {
            if line.len() > limits.max_line_bytes {
                return Err(KnowledgeBundleReadError::LineTooLong {
                    line: line_number,
                    maximum: limits.max_line_bytes,
                });
            }
            return Err(KnowledgeBundleReadError::MissingLineFeed { line: line_number });
        }
        line.pop();
        if line.last() == Some(&b'\r') {
            return Err(KnowledgeBundleReadError::NonCanonicalLineEnding { line: line_number });
        }
        if line.len() > limits.max_line_bytes {
            return Err(KnowledgeBundleReadError::LineTooLong {
                line: line_number,
                maximum: limits.max_line_bytes,
            });
        }
        if line.is_empty() {
            return Err(KnowledgeBundleReadError::EmptyLine { line: line_number });
        }

        let entry: KnowledgeBundleEntry =
            serde_json::from_slice(&line).map_err(|source| KnowledgeBundleReadError::Json {
                line: line_number,
                source,
            })?;

        match &entry {
            KnowledgeBundleEntry::Header {
                header: bundle_header,
            } => {
                if line_number != 1 || header.is_some() {
                    return Err(KnowledgeBundleReadError::HeaderOrder);
                }
                bundle_header.validate_against(manifest).map_err(|source| {
                    KnowledgeBundleReadError::Contract {
                        line: line_number,
                        source,
                    }
                })?;
                let total_entries = bundle_header
                    .record_count
                    .checked_add(bundle_header.node_count)
                    .and_then(|value| value.checked_add(bundle_header.edge_count))
                    .ok_or(KnowledgeBundleReadError::EntryLimitExceeded {
                        maximum: limits.max_entries,
                    })?;
                if total_entries > limits.max_entries {
                    return Err(KnowledgeBundleReadError::EntryLimitExceeded {
                        maximum: limits.max_entries,
                    });
                }
                header = Some(bundle_header.clone());
            }
            KnowledgeBundleEntry::Record { record } => {
                let bundle_header = header
                    .as_ref()
                    .ok_or(KnowledgeBundleReadError::HeaderOrder)?;
                ensure_section(&mut current_section, Section::Record, line_number)?;
                ensure_id_order(&mut last_record_id, &record.chunk_id, "record", line_number)?;
                record.validate(bundle_header).map_err(|source| {
                    KnowledgeBundleReadError::Contract {
                        line: line_number,
                        source,
                    }
                })?;
                record_count = increment_count(
                    record_count,
                    bundle_header.record_count,
                    "record",
                    line_number,
                )?;
            }
            KnowledgeBundleEntry::Node { node } => {
                let bundle_header = header
                    .as_ref()
                    .ok_or(KnowledgeBundleReadError::HeaderOrder)?;
                ensure_section(&mut current_section, Section::Node, line_number)?;
                ensure_id_order(&mut last_node_id, &node.node_id, "node", line_number)?;
                node.validate()
                    .map_err(|source| KnowledgeBundleReadError::Contract {
                        line: line_number,
                        source,
                    })?;
                node_count =
                    increment_count(node_count, bundle_header.node_count, "node", line_number)?;
            }
            KnowledgeBundleEntry::Edge { edge } => {
                let bundle_header = header
                    .as_ref()
                    .ok_or(KnowledgeBundleReadError::HeaderOrder)?;
                ensure_section(&mut current_section, Section::Edge, line_number)?;
                ensure_id_order(&mut last_edge_id, &edge.edge_id, "edge", line_number)?;
                edge.validate()
                    .map_err(|source| KnowledgeBundleReadError::Contract {
                        line: line_number,
                        source,
                    })?;
                edge_count =
                    increment_count(edge_count, bundle_header.edge_count, "edge", line_number)?;
            }
        }

        consume(entry).map_err(|message| KnowledgeBundleReadError::Consumer {
            line: line_number,
            message,
        })?;
    }

    let header = header.ok_or(KnowledgeBundleReadError::MissingHeader)?;
    ensure_count("record", header.record_count, record_count)?;
    ensure_count("node", header.node_count, node_count)?;
    ensure_count("edge", header.edge_count, edge_count)?;
    Ok(KnowledgeBundleSummary {
        header,
        record_count,
        node_count,
        edge_count,
        decoded_bytes,
    })
}

fn ensure_section(
    current: &mut Option<Section>,
    next: Section,
    line: u64,
) -> Result<(), KnowledgeBundleReadError> {
    if current.is_some_and(|section| next < section) {
        return Err(KnowledgeBundleReadError::EntryOrder { line });
    }
    *current = Some(next);
    Ok(())
}

fn ensure_id_order(
    previous: &mut Option<String>,
    actual: &str,
    kind: &'static str,
    line: u64,
) -> Result<(), KnowledgeBundleReadError> {
    if let Some(previous_id) = previous {
        if previous_id.as_str() >= actual {
            return Err(KnowledgeBundleReadError::IdOrder {
                line,
                kind,
                previous: previous_id.clone(),
                actual: actual.to_string(),
            });
        }
    }
    *previous = Some(actual.to_string());
    Ok(())
}

fn increment_count(
    observed: u64,
    declared: u64,
    kind: &'static str,
    line: u64,
) -> Result<u64, KnowledgeBundleReadError> {
    let next = observed
        .checked_add(1)
        .ok_or(KnowledgeBundleReadError::DeclaredCountExceeded {
            line,
            kind,
            declared,
        })?;
    if next > declared {
        return Err(KnowledgeBundleReadError::DeclaredCountExceeded {
            line,
            kind,
            declared,
        });
    }
    Ok(next)
}

fn ensure_count(
    kind: &'static str,
    declared: u64,
    observed: u64,
) -> Result<(), KnowledgeBundleReadError> {
    if declared != observed {
        return Err(KnowledgeBundleReadError::CountMismatch {
            kind,
            declared,
            observed,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use akidb_contracts::{
        ImmutableObjectReference, KnowledgeAssertionState, KnowledgeBundleEdge,
        KnowledgeBundleNode, KnowledgeBundleRecord, KnowledgeEdgeKind, KnowledgeNodeKind,
        KNOWLEDGE_SCHEMA_VERSION,
    };
    use serde_json::{Map, Value};
    use sha2::{Digest, Sha256};

    const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const GOLDEN_BUNDLE: &[u8] =
        include_bytes!("../../../contracts/fixtures/knowledge/v1/valid/bundle.ndjson");
    const GOLDEN_MANIFEST: &str =
        include_str!("../../../contracts/fixtures/knowledge/v1/valid/bundle-manifest.json");
    const INVALID_DIMENSIONS: &[u8] = include_bytes!(
        "../../../contracts/fixtures/knowledge/v1/invalid/bundle-dimension-mismatch.ndjson"
    );
    const INVALID_ORDER: &[u8] = include_bytes!(
        "../../../contracts/fixtures/knowledge/v1/invalid/bundle-wrong-order.ndjson"
    );

    fn entries() -> Vec<KnowledgeBundleEntry> {
        vec![
            KnowledgeBundleEntry::Header {
                header: KnowledgeBundleHeader {
                    schema_version: KNOWLEDGE_SCHEMA_VERSION,
                    workspace_id: "workspace-a".to_string(),
                    collection: "knowledge".to_string(),
                    generation_id: "generation-a".to_string(),
                    embedding_model_id: "model@revision".to_string(),
                    embedding_dimensions: 2,
                    graph_schema_version: "ax.knowledge-graph.v1".to_string(),
                    base_sequence: 10,
                    record_count: 1,
                    node_count: 2,
                    edge_count: 1,
                },
            },
            KnowledgeBundleEntry::Record {
                record: KnowledgeBundleRecord {
                    chunk_id: "chunk-a".to_string(),
                    doc_id: "doc-a".to_string(),
                    doc_version: "version-a".to_string(),
                    chunk_hash: DIGEST.to_string(),
                    pipeline_signature: "pipeline-v1".to_string(),
                    embedding_model_id: "model@revision".to_string(),
                    vector: vec![0.1, 0.2],
                    metadata: Map::from_iter([(
                        "source_uri".to_string(),
                        Value::String("s3://knowledge/documents/doc-a".to_string()),
                    )]),
                    chunk_text: "grounded text".to_string(),
                    context_headings: None,
                },
            },
            KnowledgeBundleEntry::Node {
                node: KnowledgeBundleNode {
                    node_id: "chunk-a".to_string(),
                    kind: KnowledgeNodeKind::Chunk,
                    properties: Map::new(),
                },
            },
            KnowledgeBundleEntry::Node {
                node: KnowledgeBundleNode {
                    node_id: "entity-a".to_string(),
                    kind: KnowledgeNodeKind::Entity,
                    properties: Map::from_iter([(
                        "entity_type".to_string(),
                        Value::String("product".to_string()),
                    )]),
                },
            },
            KnowledgeBundleEntry::Edge {
                edge: KnowledgeBundleEdge {
                    edge_id: "edge-a".to_string(),
                    from_node_id: "chunk-a".to_string(),
                    to_node_id: "entity-a".to_string(),
                    kind: KnowledgeEdgeKind::RelatedTo,
                    predicate: Some("mentions_product".to_string()),
                    weight: 0.9,
                    confidence: 0.95,
                    assertion_state: KnowledgeAssertionState::Extracted,
                    source_uri: "s3://knowledge/documents/doc-a".to_string(),
                    source_version: "version-a".to_string(),
                    evidence_chunk_ids: vec!["chunk-a".to_string()],
                    extractor: "ner-v1".to_string(),
                    valid_from_ms: None,
                    valid_to_ms: None,
                    observed_at_ms: 1_784_995_200_000,
                    properties: Map::new(),
                },
            },
        ]
    }

    fn encode(entries: &[KnowledgeBundleEntry]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for entry in entries {
            serde_json::to_writer(&mut bytes, entry).unwrap();
            bytes.push(b'\n');
        }
        bytes
    }

    fn manifest(
        bytes: &[u8],
        compression: KnowledgeBundleCompression,
    ) -> KnowledgeGenerationManifest {
        let object_bytes = match compression {
            KnowledgeBundleCompression::None => bytes.to_vec(),
            KnowledgeBundleCompression::Zstd => zstd::stream::encode_all(bytes, 1).unwrap(),
        };
        KnowledgeGenerationManifest {
            schema_version: KNOWLEDGE_SCHEMA_VERSION,
            workspace_id: "workspace-a".to_string(),
            collection: "knowledge".to_string(),
            generation_id: "generation-a".to_string(),
            parent_generation_id: None,
            created_at_ms: 1_784_995_200_000,
            embedding_model_id: "model@revision".to_string(),
            embedding_dimensions: 2,
            graph_schema_version: "ax.knowledge-graph.v1".to_string(),
            bundle_format: KnowledgeBundleFormat::NdjsonV1,
            bundle_compression: compression,
            bundle: ImmutableObjectReference {
                uri: "s3://knowledge/generations/generation-a/bundle.ndjson".to_string(),
                sha256: format!("{:x}", Sha256::digest(&object_bytes)),
                size_bytes: object_bytes.len() as u64,
            },
            base_sequence: 10,
            target_sequence: 10,
            expected_vector_count: 1,
            expected_edge_count: 1,
        }
    }

    #[test]
    fn validates_and_streams_a_complete_bundle() {
        let bytes = encode(&entries());
        let manifest = manifest(&bytes, KnowledgeBundleCompression::None);
        let mut seen = Vec::new();
        let summary = consume_knowledge_bundle(bytes.as_slice(), &manifest, |entry| {
            seen.push(entry);
            Ok(())
        })
        .unwrap();
        assert_eq!(seen.len(), 5);
        assert_eq!(summary.record_count, 1);
        assert_eq!(summary.node_count, 2);
        assert_eq!(summary.edge_count, 1);
        assert_eq!(summary.decoded_bytes, bytes.len() as u64);
    }

    #[test]
    fn shared_golden_bundle_has_exact_bytes_digest_and_contract() {
        let manifest: KnowledgeGenerationManifest = serde_json::from_str(GOLDEN_MANIFEST).unwrap();
        assert_eq!(GOLDEN_BUNDLE.len() as u64, manifest.bundle.size_bytes);
        assert_eq!(
            format!("{:x}", Sha256::digest(GOLDEN_BUNDLE)),
            manifest.bundle.sha256
        );
        let summary = consume_knowledge_bundle(GOLDEN_BUNDLE, &manifest, |_| Ok(())).unwrap();
        assert_eq!(summary.record_count, 1);
        assert_eq!(summary.node_count, 2);
        assert_eq!(summary.edge_count, 1);

        assert!(matches!(
            consume_knowledge_bundle(INVALID_DIMENSIONS, &manifest, |_| Ok(())),
            Err(KnowledgeBundleReadError::Contract { line: 2, .. })
        ));
        assert!(matches!(
            consume_knowledge_bundle(INVALID_ORDER, &manifest, |_| Ok(())),
            Err(KnowledgeBundleReadError::EntryOrder { line: 3 })
        ));
    }

    #[test]
    fn validates_a_zstd_stream_without_archive_extraction() {
        let bytes = encode(&entries());
        let manifest = manifest(&bytes, KnowledgeBundleCompression::Zstd);
        let compressed = zstd::stream::encode_all(bytes.as_slice(), 1).unwrap();
        let summary =
            consume_knowledge_bundle(compressed.as_slice(), &manifest, |_| Ok(())).unwrap();
        assert_eq!(summary.record_count, 1);
        assert_eq!(summary.decoded_bytes, bytes.len() as u64);
    }

    #[test]
    fn rejects_noncanonical_order_and_missing_final_lf() {
        let mut unordered = entries();
        unordered.swap(1, 2);
        let bytes = encode(&unordered);
        let canonical_manifest = manifest(&encode(&entries()), KnowledgeBundleCompression::None);
        assert!(matches!(
            consume_knowledge_bundle(bytes.as_slice(), &canonical_manifest, |_| Ok(())),
            Err(KnowledgeBundleReadError::EntryOrder { .. })
        ));

        let mut missing_lf = encode(&entries());
        missing_lf.pop();
        let missing_lf_manifest = manifest(&missing_lf, KnowledgeBundleCompression::None);
        assert!(matches!(
            consume_knowledge_bundle(missing_lf.as_slice(), &missing_lf_manifest, |_| Ok(())),
            Err(KnowledgeBundleReadError::MissingLineFeed { .. })
        ));
    }

    #[test]
    fn rejects_declared_count_mismatch_before_consuming_excess() {
        let mut entries = entries();
        if let KnowledgeBundleEntry::Header { header } = &mut entries[0] {
            header.node_count = 1;
        }
        let bytes = encode(&entries);
        let mut manifest = manifest(&bytes, KnowledgeBundleCompression::None);
        manifest.bundle.size_bytes = bytes.len() as u64;
        assert!(matches!(
            consume_knowledge_bundle(bytes.as_slice(), &manifest, |_| Ok(())),
            Err(KnowledgeBundleReadError::DeclaredCountExceeded { kind: "node", .. })
        ));
    }

    #[test]
    fn enforces_decoded_size_and_consumer_failures() {
        let bytes = encode(&entries());
        let manifest = manifest(&bytes, KnowledgeBundleCompression::None);
        let limits = KnowledgeBundleReadLimits {
            max_decoded_bytes: 8,
            ..KnowledgeBundleReadLimits::default()
        };
        assert!(matches!(
            consume_knowledge_bundle_with_limits(bytes.as_slice(), &manifest, limits, |_| Ok(())),
            Err(KnowledgeBundleReadError::DecodedSizeExceeded { .. })
        ));

        assert!(matches!(
            consume_knowledge_bundle(bytes.as_slice(), &manifest, |_| {
                Err("injected build failure".to_string())
            }),
            Err(KnowledgeBundleReadError::Consumer { line: 1, .. })
        ));
    }
}
