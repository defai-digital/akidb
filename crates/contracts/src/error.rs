//! Contract violation error types

use thiserror::Error;

/// Kind of contract violation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContractViolationKind {
    /// Value exceeds maximum allowed
    ExceedsMaximum,
    /// Value is below minimum allowed
    BelowMinimum,
    /// Value is empty when non-empty required
    Empty,
    /// Value contains invalid characters or format
    InvalidFormat,
    /// Value is NaN or infinite
    InvalidNumber,
}

/// Contract violation error
///
/// Returned when input data fails validation at system boundaries.
#[derive(Error, Debug, Clone)]
#[error("Contract violation in {field}: {message}")]
pub struct ContractViolation {
    /// The field that failed validation
    pub field: &'static str,
    /// Human-readable description of the violation
    pub message: String,
    /// Kind of violation
    pub kind: ContractViolationKind,
}

impl ContractViolation {
    /// Create a new contract violation
    pub fn new(field: &'static str, message: impl Into<String>, kind: ContractViolationKind) -> Self {
        Self {
            field,
            message: message.into(),
            kind,
        }
    }

    /// Create a violation for exceeding maximum
    pub fn exceeds_maximum(field: &'static str, actual: usize, max: usize) -> Self {
        Self::new(
            field,
            format!("{} ({}) exceeds maximum ({})", field, actual, max),
            ContractViolationKind::ExceedsMaximum,
        )
    }

    /// Create a violation for empty value
    pub fn empty(field: &'static str) -> Self {
        Self::new(
            field,
            format!("{} cannot be empty", field),
            ContractViolationKind::Empty,
        )
    }

    /// Create a violation for invalid number (NaN or infinite)
    pub fn invalid_number(field: &'static str, index: usize) -> Self {
        Self::new(
            field,
            format!("{} contains NaN or infinite value at index {}", field, index),
            ContractViolationKind::InvalidNumber,
        )
    }
}

/// Result type for contract validation
pub type ContractResult<T> = Result<T, ContractViolation>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contract_violation_display() {
        let violation = ContractViolation::exceeds_maximum("vector", 20000, 16384);
        let msg = violation.to_string();
        assert!(msg.contains("vector"));
        assert!(msg.contains("20000"));
        assert!(msg.contains("16384"));
    }

    #[test]
    fn test_contract_violation_kinds() {
        let v1 = ContractViolation::empty("id");
        assert_eq!(v1.kind, ContractViolationKind::Empty);

        let v2 = ContractViolation::exceeds_maximum("data", 100, 50);
        assert_eq!(v2.kind, ContractViolationKind::ExceedsMaximum);
    }
}
