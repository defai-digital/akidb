//! AkiDB Invariants - Debug assertions and runtime invariant checking
//!
//! This crate provides macros for asserting invariants in AkiDB code.
//!
//! # Philosophy
//!
//! Invariants are assumptions about system state that must always hold.
//! When an invariant is violated, it indicates a bug in the code.
//!
//! We provide two levels of invariant checking:
//!
//! 1. **Debug invariants** (`debug_invariant!`): Checked only in debug builds.
//!    Zero cost in release builds. Use for expensive checks on hot paths.
//!
//! 2. **Critical invariants** (`critical_invariant!`): Always checked, with
//!    metrics emission. Use sparingly for critical safety properties.
//!
//! # Example
//!
//! ```rust
//! use akidb_invariants::{debug_invariant, critical_invariant};
//!
//! fn merge_results(results: &mut Vec<f32>, capacity: usize) {
//!     // Debug-only check (zero cost in release)
//!     debug_invariant!(
//!         results.len() <= capacity * 2,
//!         "Heap size {} exceeds 2x capacity {}",
//!         results.len(),
//!         capacity
//!     );
//!
//!     // ... merge logic ...
//!
//!     // Debug-only post-condition
//!     debug_invariant!(
//!         results.windows(2).all(|w| w[0] >= w[1]),
//!         "Results must be sorted descending"
//!     );
//! }
//! ```

/// Debug-only invariant check.
///
/// Panics in debug builds if the condition is false.
/// Compiles to nothing in release builds (zero runtime cost).
///
/// Use this for:
/// - Expensive checks that would impact performance
/// - Checks on hot paths (search, insert)
/// - Pre/post conditions during development
///
/// # Example
///
/// ```rust
/// use akidb_invariants::debug_invariant;
///
/// fn process(data: &[u8], max_size: usize) {
///     debug_invariant!(data.len() <= max_size, "Data exceeds max size");
///     // ... processing ...
/// }
/// ```
#[macro_export]
macro_rules! debug_invariant {
    ($cond:expr, $msg:literal) => {
        #[cfg(debug_assertions)]
        {
            if !$cond {
                panic!("INVARIANT VIOLATED: {}", $msg);
            }
        }
    };
    ($cond:expr, $fmt:literal, $($arg:tt)*) => {
        #[cfg(debug_assertions)]
        {
            if !$cond {
                panic!("INVARIANT VIOLATED: {}", format!($fmt, $($arg)*));
            }
        }
    };
}

/// Critical invariant check with logging.
///
/// Always checked (even in release builds). Logs a warning but does not panic.
/// Use this for invariants where violation is serious but shouldn't crash.
///
/// # When to use
///
/// - Safety-critical properties that must be monitored in production
/// - Invariants whose violation indicates data corruption risk
/// - Properties that should trigger alerts but not crashes
///
/// # Example
///
/// ```rust
/// use akidb_invariants::critical_invariant;
///
/// fn verify_rebuild(old_count: usize, new_count: usize, tombstones: usize) -> bool {
///     let expected = old_count.saturating_sub(tombstones);
///     critical_invariant!(
///         new_count >= expected,
///         "rebuild_data_loss",
///         "New index has {} vectors, expected at least {}",
///         new_count,
///         expected
///     );
///     new_count >= expected
/// }
/// ```
#[macro_export]
macro_rules! critical_invariant {
    ($cond:expr, $invariant_id:literal, $msg:literal) => {
        if !$cond {
            tracing::error!(
                invariant_id = $invariant_id,
                "CRITICAL INVARIANT VIOLATED: {}",
                $msg
            );
        }
    };
    ($cond:expr, $invariant_id:literal, $fmt:literal, $($arg:tt)*) => {
        if !$cond {
            tracing::error!(
                invariant_id = $invariant_id,
                "CRITICAL INVARIANT VIOLATED: {}",
                format!($fmt, $($arg)*)
            );
        }
    };
}

/// Assert invariant and return Result on violation.
///
/// Unlike `debug_invariant!` which panics, this returns an error,
/// allowing the caller to handle the violation gracefully.
///
/// # Example
///
/// ```rust
/// use akidb_invariants::ensure_invariant;
///
/// fn validate_state(count: usize, max: usize) -> Result<(), String> {
///     ensure_invariant!(count <= max, "Count {} exceeds max {}", count, max);
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! ensure_invariant {
    ($cond:expr, $msg:literal) => {
        if !$cond {
            return Err(format!("Invariant violated: {}", $msg));
        }
    };
    ($cond:expr, $fmt:literal, $($arg:tt)*) => {
        if !$cond {
            return Err(format!("Invariant violated: {}", format!($fmt, $($arg)*)));
        }
    };
}

/// Check invariant and execute recovery action on violation.
///
/// This is useful when you want to detect violations and take
/// corrective action rather than failing.
///
/// # Example
///
/// ```rust
/// use akidb_invariants::invariant_or;
///
/// fn cleanup_if_needed(items: &mut Vec<i32>, max: usize) {
///     invariant_or!(
///         items.len() <= max,
///         {
///             // Recovery: truncate to max
///             items.truncate(max);
///             tracing::warn!("Truncated items to max size");
///         },
///         "Items exceeded max size"
///     );
/// }
/// ```
#[macro_export]
macro_rules! invariant_or {
    ($cond:expr, $recovery:block, $msg:literal) => {
        if !$cond {
            tracing::warn!("Invariant violated (recovering): {}", $msg);
            $recovery
        }
    };
    ($cond:expr, $recovery:block, $fmt:literal, $($arg:tt)*) => {
        if !$cond {
            tracing::warn!("Invariant violated (recovering): {}", format!($fmt, $($arg)*));
            $recovery
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_invariant_passes() {
        debug_invariant!(true, "This should not panic");
        debug_invariant!(1 + 1 == 2, "Math still works");
    }

    #[test]
    #[should_panic(expected = "INVARIANT VIOLATED")]
    #[cfg(debug_assertions)]
    fn test_debug_invariant_fails() {
        debug_invariant!(false, "This should panic");
    }

    #[test]
    fn test_debug_invariant_with_format() {
        let x = 5;
        let max = 10;
        debug_invariant!(x <= max, "Value {} exceeds max {}", x, max);
    }

    #[test]
    #[should_panic(expected = "Value 15 exceeds max 10")]
    #[cfg(debug_assertions)]
    fn test_debug_invariant_format_in_panic() {
        let x = 15;
        let max = 10;
        debug_invariant!(x <= max, "Value {} exceeds max {}", x, max);
    }

    #[test]
    fn test_ensure_invariant_passes() {
        fn check() -> Result<(), String> {
            ensure_invariant!(true, "Should pass");
            Ok(())
        }
        assert!(check().is_ok());
    }

    #[test]
    fn test_ensure_invariant_fails() {
        fn check() -> Result<(), String> {
            ensure_invariant!(false, "Should fail");
            Ok(())
        }
        let result = check();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Should fail"));
    }

    #[test]
    fn test_invariant_or_passes() {
        let mut recovered = false;
        invariant_or!(
            true,
            { recovered = true; },
            "Should not recover"
        );
        assert!(!recovered);
    }

    #[test]
    fn test_invariant_or_recovers() {
        let mut recovered = false;
        invariant_or!(
            false,
            { recovered = true; },
            "Should recover"
        );
        assert!(recovered);
    }
}
