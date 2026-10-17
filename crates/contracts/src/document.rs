//! Bounds for atomic standalone document replacement.

use crate::{ContractViolation, ContractViolationKind};

pub const MAX_DOCUMENT_CHUNKS: usize = 2048;
pub const MAX_DOCUMENT_REQUEST_BYTES: usize = 3 * 1024 * 1024;
pub const DOCUMENT_CHUNK_PREFIX: &str = "adoc:";

pub fn validate_hash(field: &'static str, value: &str) -> Result<(), ContractViolation> {
    if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ContractViolation::new(
            field,
            "expected a SHA-256 hex digest",
            ContractViolationKind::InvalidFormat,
        ));
    }
    Ok(())
}

pub fn validate_document_bounds(chunks: usize, bytes: usize) -> Result<(), ContractViolation> {
    if chunks > MAX_DOCUMENT_CHUNKS {
        return Err(ContractViolation::exceeds_maximum(
            "chunks",
            chunks,
            MAX_DOCUMENT_CHUNKS,
        ));
    }
    if bytes > MAX_DOCUMENT_REQUEST_BYTES {
        return Err(ContractViolation::exceeds_maximum(
            "request",
            bytes,
            MAX_DOCUMENT_REQUEST_BYTES,
        ));
    }
    Ok(())
}
