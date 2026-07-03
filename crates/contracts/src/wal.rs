//! WAL (Write-Ahead Log) entry contracts
//!
//! These contracts define the valid bounds for data written to the WAL.
//! They should be checked at the system boundary before data enters the WAL.

use crate::error::{ContractResult, ContractViolation};
use akidb_common::metrics::record_contract_violation;

/// Maximum vector dimensions allowed
///
/// 16K dimensions supports most embedding models:
/// - OpenAI text-embedding-3-large: 3072
/// - Cohere embed-v3: 1024
/// - BGE-large: 1024
///
/// This limit prevents DoS attacks via oversized vectors.
pub const MAX_VECTOR_DIMENSIONS: usize = 16_384;

/// Maximum metadata size in bytes (1MB)
///
/// Metadata typically contains JSON or key-value pairs.
/// 1MB is generous for typical use cases while preventing abuse.
pub const MAX_METADATA_BYTES: usize = 1_048_576;

/// Maximum external ID length in bytes
pub const MAX_EXTERNAL_ID_BYTES: usize = 1024;

/// WAL entry contract validation
///
/// Provides validation methods for data entering the WAL.
/// Use these at system boundaries (gRPC handlers, API endpoints).
pub struct WalContract;

impl WalContract {
    /// Validate vector dimensions
    ///
    /// # Errors
    /// Returns `ContractViolation` if:
    /// - Vector is empty
    /// - Vector exceeds MAX_VECTOR_DIMENSIONS
    pub fn validate_vector(vector: &[f32]) -> ContractResult<()> {
        if vector.is_empty() {
            record_contract_violation("wal_entry", "vector", "empty");
            return Err(ContractViolation::empty("vector"));
        }

        if vector.len() > MAX_VECTOR_DIMENSIONS {
            record_contract_violation("wal_entry", "vector", "exceeds_maximum");
            return Err(ContractViolation::exceeds_maximum(
                "vector",
                vector.len(),
                MAX_VECTOR_DIMENSIONS,
            ));
        }

        // Check for NaN/Inf values that would corrupt similarity calculations
        for (i, &v) in vector.iter().enumerate() {
            if !v.is_finite() {
                record_contract_violation("wal_entry", "vector", "invalid_number");
                return Err(ContractViolation::invalid_number("vector", i));
            }
        }

        Ok(())
    }

    /// Validate metadata size
    ///
    /// # Errors
    /// Returns `ContractViolation` if metadata exceeds MAX_METADATA_BYTES
    pub fn validate_metadata(metadata: Option<&[u8]>) -> ContractResult<()> {
        if let Some(m) = metadata {
            if m.len() > MAX_METADATA_BYTES {
                record_contract_violation("wal_entry", "metadata", "exceeds_maximum");
                return Err(ContractViolation::exceeds_maximum(
                    "metadata",
                    m.len(),
                    MAX_METADATA_BYTES,
                ));
            }
        }
        Ok(())
    }

    /// Validate external ID
    ///
    /// # Errors
    /// Returns `ContractViolation` if:
    /// - ID is empty
    /// - ID exceeds MAX_EXTERNAL_ID_BYTES
    pub fn validate_external_id(id: &str) -> ContractResult<()> {
        if id.is_empty() {
            record_contract_violation("wal_entry", "external_id", "empty");
            return Err(ContractViolation::empty("external_id"));
        }

        if id.len() > MAX_EXTERNAL_ID_BYTES {
            record_contract_violation("wal_entry", "external_id", "exceeds_maximum");
            return Err(ContractViolation::exceeds_maximum(
                "external_id",
                id.len(),
                MAX_EXTERNAL_ID_BYTES,
            ));
        }

        Ok(())
    }

    /// Validate a complete WAL insert entry
    ///
    /// Validates all components of an insert operation.
    pub fn validate_insert(
        external_id: &str,
        vector: &[f32],
        metadata: Option<&[u8]>,
    ) -> ContractResult<()> {
        Self::validate_external_id(external_id)?;
        Self::validate_vector(vector)?;
        Self::validate_metadata(metadata)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_vector_valid() {
        let vector = vec![1.0f32; 1024];
        assert!(WalContract::validate_vector(&vector).is_ok());
    }

    #[test]
    fn test_validate_vector_empty() {
        let vector: Vec<f32> = vec![];
        let result = WalContract::validate_vector(&vector);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("empty"));
    }

    #[test]
    fn test_validate_vector_too_large() {
        let vector = vec![1.0f32; MAX_VECTOR_DIMENSIONS + 1];
        let result = WalContract::validate_vector(&vector);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("exceeds"));
    }

    #[test]
    fn test_validate_vector_nan() {
        let vector = vec![1.0, f32::NAN, 3.0];
        let result = WalContract::validate_vector(&vector);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("NaN"));
    }

    #[test]
    fn test_validate_vector_infinity() {
        let vector = vec![1.0, f32::INFINITY, 3.0];
        let result = WalContract::validate_vector(&vector);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_metadata_valid() {
        let metadata = vec![0u8; 1000];
        assert!(WalContract::validate_metadata(Some(&metadata)).is_ok());
    }

    #[test]
    fn test_validate_metadata_none() {
        assert!(WalContract::validate_metadata(None).is_ok());
    }

    #[test]
    fn test_validate_metadata_too_large() {
        let metadata = vec![0u8; MAX_METADATA_BYTES + 1];
        let result = WalContract::validate_metadata(Some(&metadata));
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_external_id_valid() {
        assert!(WalContract::validate_external_id("vec-123").is_ok());
    }

    #[test]
    fn test_validate_external_id_empty() {
        let result = WalContract::validate_external_id("");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_external_id_too_long() {
        let id = "x".repeat(MAX_EXTERNAL_ID_BYTES + 1);
        let result = WalContract::validate_external_id(&id);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_insert_complete() {
        let vector = vec![1.0f32; 128];
        let metadata = b"test metadata";
        assert!(WalContract::validate_insert("vec-1", &vector, Some(metadata)).is_ok());
    }
}

/// Property-based tests using proptest
///
/// These tests verify contract properties hold across a wide range of inputs.
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Strategy for generating valid vectors (non-empty, within dimension limit, finite values)
    fn valid_vector_strategy() -> impl Strategy<Value = Vec<f32>> {
        // Generate vectors of 1 to 1024 dimensions with finite values
        prop::collection::vec(-1e6f32..1e6f32, 1..1024)
    }

    /// Strategy for valid external IDs
    fn valid_id_strategy() -> impl Strategy<Value = String> {
        // Generate non-empty strings within length limit
        "[a-zA-Z0-9_-]{1,100}".prop_map(String::from)
    }

    proptest! {
        /// Property: Valid vectors always pass validation
        #[test]
        fn prop_valid_vector_passes(vector in valid_vector_strategy()) {
            prop_assert!(WalContract::validate_vector(&vector).is_ok());
        }

        /// Property: Empty vectors always fail
        #[test]
        fn prop_empty_vector_fails(size in 0usize..1) {
            let vector: Vec<f32> = vec![0.0; size];
            if size == 0 {
                prop_assert!(WalContract::validate_vector(&vector).is_err());
            }
        }

        /// Property: Vectors exceeding max dimensions always fail
        #[test]
        fn prop_oversized_vector_fails(extra in 1usize..100) {
            let vector = vec![1.0f32; MAX_VECTOR_DIMENSIONS + extra];
            let result = WalContract::validate_vector(&vector);
            prop_assert!(result.is_err());
        }

        /// Property: Vectors with NaN always fail
        #[test]
        fn prop_nan_vector_fails(prefix_len in 0usize..100, suffix_len in 0usize..100) {
            let mut vector = vec![1.0f32; prefix_len + 1 + suffix_len];
            vector[prefix_len] = f32::NAN;
            let result = WalContract::validate_vector(&vector);
            prop_assert!(result.is_err());
        }

        /// Property: Valid external IDs pass validation
        #[test]
        fn prop_valid_id_passes(id in valid_id_strategy()) {
            prop_assert!(WalContract::validate_external_id(&id).is_ok());
        }

        /// Property: Empty IDs always fail
        #[test]
        fn prop_empty_id_fails(_dummy in Just(())) {
            prop_assert!(WalContract::validate_external_id("").is_err());
        }

        /// Property: IDs exceeding max length always fail
        #[test]
        fn prop_oversized_id_fails(extra in 1usize..100) {
            let id = "x".repeat(MAX_EXTERNAL_ID_BYTES + extra);
            prop_assert!(WalContract::validate_external_id(&id).is_err());
        }

        /// Property: Metadata within size limit passes
        #[test]
        fn prop_valid_metadata_passes(size in 0usize..1000) {
            let metadata = vec![0u8; size];
            prop_assert!(WalContract::validate_metadata(Some(&metadata)).is_ok());
        }

        /// Property: Metadata exceeding size limit fails
        #[test]
        fn prop_oversized_metadata_fails(extra in 1usize..100) {
            let metadata = vec![0u8; MAX_METADATA_BYTES + extra];
            prop_assert!(WalContract::validate_metadata(Some(&metadata)).is_err());
        }

        /// Property: None metadata always passes
        #[test]
        fn prop_none_metadata_passes(_dummy in Just(())) {
            prop_assert!(WalContract::validate_metadata(None).is_ok());
        }

        /// Property: validate_insert is conjunction of individual validations
        #[test]
        fn prop_validate_insert_conjunction(
            id in valid_id_strategy(),
            vector in valid_vector_strategy()
        ) {
            // If all individual validations pass, the combined validation passes
            let id_valid = WalContract::validate_external_id(&id).is_ok();
            let vec_valid = WalContract::validate_vector(&vector).is_ok();
            let meta_valid = WalContract::validate_metadata(None).is_ok();

            let insert_valid = WalContract::validate_insert(&id, &vector, None).is_ok();

            prop_assert_eq!(id_valid && vec_valid && meta_valid, insert_valid);
        }
    }
}
