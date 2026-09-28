//! Generated protobuf and gRPC bindings for AkiDB.
//!
//! This crate is intentionally transport-contract only. Server implementations,
//! storage, indexes, and retrieval logic live in higher-level crates.

// tonic-generated servers return Result<_, tonic::Status>, and Status exceeds
// the result_large_err threshold on newer clippy versions. The crate contains
// no hand-written logic, so allow the lint rather than churn generated code.
#![allow(clippy::result_large_err)]

tonic::include_proto!("akidb.v1");
