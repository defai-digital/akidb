//! AkiDB Observability Metrics
//!
//! This module provides Prometheus metrics for monitoring AkiDB operations,
//! particularly focused on AutomatosX principles:
//!
//! - **Invariant violations**: Track when critical invariants are violated
//! - **Guard failures**: Track rebuild state machine guard failures
//! - **Contract violations**: Track validation failures at system boundaries
//!
//! # Usage
//!
//! ```rust
//! use akidb_common::metrics::{INVARIANT_VIOLATIONS, GUARD_FAILURES};
//!
//! // Record an invariant violation
//! INVARIANT_VIOLATIONS
//!     .with_label_values(&["id_mapping_bijectivity", "critical"])
//!     .inc();
//!
//! // Record a guard failure
//! GUARD_FAILURES
//!     .with_label_values(&["preparing", "building", "shadow_not_ready"])
//!     .inc();
//! ```

use lazy_static::lazy_static;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge_vec,
    HistogramVec, IntCounterVec, IntGaugeVec,
};

lazy_static! {
    /// Counter for invariant violations
    ///
    /// Labels:
    /// - `invariant_id`: Identifier for the invariant (e.g., "id_mapping_bijectivity")
    /// - `severity`: "debug", "warning", or "critical"
    pub static ref INVARIANT_VIOLATIONS: IntCounterVec = register_int_counter_vec!(
        "akidb_invariant_violations_total",
        "Total count of invariant violations detected",
        &["invariant_id", "severity"]
    ).expect("Failed to create INVARIANT_VIOLATIONS metric");

    /// Counter for guard check failures
    ///
    /// Labels:
    /// - `from_state`: The state we're transitioning from
    /// - `to_state`: The state we're transitioning to
    /// - `guard_id`: Identifier for the failed guard
    pub static ref GUARD_FAILURES: IntCounterVec = register_int_counter_vec!(
        "akidb_guard_failures_total",
        "Total count of state transition guard failures",
        &["from_state", "to_state", "guard_id"]
    ).expect("Failed to create GUARD_FAILURES metric");

    /// Counter for contract violations at system boundaries
    ///
    /// Labels:
    /// - `contract_id`: Identifier for the contract (e.g., "wal_entry", "grpc_request")
    /// - `field`: The field that violated the contract
    /// - `kind`: Kind of violation (e.g., "exceeds_maximum", "empty", "invalid_format")
    pub static ref CONTRACT_VIOLATIONS: IntCounterVec = register_int_counter_vec!(
        "akidb_contract_violations_total",
        "Total count of contract violations at system boundaries",
        &["contract_id", "field", "kind"]
    ).expect("Failed to create CONTRACT_VIOLATIONS metric");

    /// Gauge for rebuild state machine
    ///
    /// Labels:
    /// - `state`: Current rebuild state (idle, preparing, building, etc.)
    pub static ref REBUILD_STATE: IntGaugeVec = register_int_gauge_vec!(
        "akidb_rebuild_state",
        "Current rebuild state (1 = active, 0 = inactive)",
        &["state"]
    ).expect("Failed to create REBUILD_STATE metric");

    /// Histogram for rebuild operation duration
    ///
    /// Labels:
    /// - `phase`: The phase of rebuild (preparing, building, replaying, swapping, cleaning)
    pub static ref REBUILD_PHASE_DURATION: HistogramVec = register_histogram_vec!(
        "akidb_rebuild_phase_duration_seconds",
        "Duration of each rebuild phase in seconds",
        &["phase"],
        vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]
    ).expect("Failed to create REBUILD_PHASE_DURATION metric");

    /// Counter for successful operations by type
    ///
    /// Labels:
    /// - `operation`: Type of operation (insert, search, delete, rebuild)
    pub static ref OPERATIONS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "akidb_operations_total",
        "Total count of operations by type",
        &["operation"]
    ).expect("Failed to create OPERATIONS_TOTAL metric");

    /// Counter for operation errors by type
    ///
    /// Labels:
    /// - `operation`: Type of operation
    /// - `error_type`: Type of error encountered
    pub static ref OPERATION_ERRORS: IntCounterVec = register_int_counter_vec!(
        "akidb_operation_errors_total",
        "Total count of operation errors by type",
        &["operation", "error_type"]
    ).expect("Failed to create OPERATION_ERRORS metric");
}

/// Record an invariant violation
pub fn record_invariant_violation(invariant_id: &str, severity: &str) {
    INVARIANT_VIOLATIONS
        .with_label_values(&[invariant_id, severity])
        .inc();
}

/// Record a guard failure
pub fn record_guard_failure(from_state: &str, to_state: &str, guard_id: &str) {
    GUARD_FAILURES
        .with_label_values(&[from_state, to_state, guard_id])
        .inc();
}

/// Record a contract violation
pub fn record_contract_violation(contract_id: &str, field: &str, kind: &str) {
    CONTRACT_VIOLATIONS
        .with_label_values(&[contract_id, field, kind])
        .inc();
}

/// Update rebuild state gauge
pub fn set_rebuild_state(state: &str, active: bool) {
    REBUILD_STATE
        .with_label_values(&[state])
        .set(if active { 1 } else { 0 });
}

/// Record rebuild phase completion time
pub fn record_rebuild_phase_duration(phase: &str, duration_secs: f64) {
    REBUILD_PHASE_DURATION
        .with_label_values(&[phase])
        .observe(duration_secs);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registration() {
        // Just verify metrics can be accessed without panicking
        INVARIANT_VIOLATIONS
            .with_label_values(&["test_invariant", "debug"])
            .inc();

        GUARD_FAILURES
            .with_label_values(&["idle", "preparing", "test_guard"])
            .inc();

        CONTRACT_VIOLATIONS
            .with_label_values(&["wal_entry", "vector", "exceeds_maximum"])
            .inc();

        set_rebuild_state("building", true);
        set_rebuild_state("building", false);

        record_rebuild_phase_duration("building", 5.5);
    }

    #[test]
    fn test_helper_functions() {
        record_invariant_violation("test", "critical");
        record_guard_failure("idle", "preparing", "test_guard");
        record_contract_violation("wal", "vector", "empty");
    }
}
