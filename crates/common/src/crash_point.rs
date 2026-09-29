//! Test-only crash-point injection for durability qualification.
//!
//! Setting the `AKIDB_CRASH_POINT` environment variable to a stage name makes
//! the process abort (SIGABRT, like a `kill -9` for crash-recovery purposes)
//! the next time the write path reaches that stage. This exists so crash
//! recovery can be qualified deterministically without external process
//! orchestration. It is a no-op when the variable is unset — never set it in
//! production.
//!
//! Known stages (write path, `akidb-grpc` service):
//! - `insert.after_index_insert` — after the in-memory HNSW insert, before the
//!   synced durable batch
//! - `insert.after_durable_write` — after the synced RocksDB batch, before the
//!   graph/SQL projections
//! - `insert_batch.after_durable_write` — same point in the batch path
//! - `delete.after_durable_write` — after the synced tombstone batch, before
//!   the in-memory tombstone

use std::sync::OnceLock;

/// Environment variable selecting the crash-point stage.
pub const CRASH_POINT_ENV: &str = "AKIDB_CRASH_POINT";

fn configured_stage() -> Option<&'static str> {
    static STAGE: OnceLock<Option<String>> = OnceLock::new();
    STAGE
        .get_or_init(|| {
            std::env::var(CRASH_POINT_ENV)
                .ok()
                .filter(|value| !value.is_empty())
        })
        .as_deref()
}

/// Abort the process when `stage` matches `AKIDB_CRASH_POINT`.
///
/// Cheap no-op when the variable is unset: one static lookup and one string
/// comparison per call.
pub fn crash_point(stage: &str) {
    if configured_stage() == Some(stage) {
        eprintln!("AKIDB_CRASH_POINT triggered at stage '{stage}'; aborting process");
        std::process::abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_env_is_a_noop() {
        // The test harness does not set AKIDB_CRASH_POINT; matching and
        // non-matching stages must both pass through.
        crash_point("insert.after_index_insert");
        crash_point("anything");
    }
}
