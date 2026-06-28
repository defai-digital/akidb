//! Query workflow with explicit state management
//!
//! This module implements the QueryWorkflow pattern from AutomatosX principles:
//! - Explicit state transitions for query lifecycle
//! - Timeout budgeting across phases
//! - Coverage reporting for partial results
//!
//! # Workflow States
//!
//! 1. **Pending**: Query submitted, not yet started
//! 2. **Routing**: Determining which shards to query
//! 3. **Executing**: Fan-out search in progress
//! 4. **Merging**: Collecting and merging results
//! 5. **Completed**: Successfully finished
//! 6. **TimedOut**: Exceeded deadline with partial results
//! 7. **Failed**: Unrecoverable error
//!
//! # Example
//!
//! ```ignore
//! let workflow = QueryWorkflow::new(query, top_k, Duration::from_secs(5));
//! let result = workflow.execute(&fanout_executor).await;
//!
//! match result.state {
//!     QueryState::Completed => { /* full results */ }
//!     QueryState::TimedOut => {
//!         println!("Partial results: {}% coverage", result.coverage * 100.0);
//!     }
//!     QueryState::Failed => { /* handle error */ }
//! }
//! ```

use crate::fanout::{FanoutExecutor, FanoutResult};
use akidb_common::{Result, SearchResult};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Query workflow states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryState {
    /// Query submitted, not yet started
    Pending,
    /// Determining which shards to query
    Routing,
    /// Fan-out search in progress
    Executing,
    /// Collecting and merging results
    Merging,
    /// Successfully completed
    Completed,
    /// Exceeded deadline (may have partial results)
    TimedOut,
    /// Unrecoverable error
    Failed,
}

impl QueryState {
    /// Get human-readable state name
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryState::Pending => "pending",
            QueryState::Routing => "routing",
            QueryState::Executing => "executing",
            QueryState::Merging => "merging",
            QueryState::Completed => "completed",
            QueryState::TimedOut => "timed_out",
            QueryState::Failed => "failed",
        }
    }

    /// Check if this is a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            QueryState::Completed | QueryState::TimedOut | QueryState::Failed
        )
    }
}

/// Query coverage information
#[derive(Debug, Clone)]
pub struct QueryCoverage {
    /// Number of shards that responded
    pub responding_shards: usize,
    /// Total number of shards queried
    pub total_shards: usize,
    /// Coverage ratio (0.0 to 1.0)
    pub ratio: f32,
    /// List of missing shard IDs
    pub missing_shards: Vec<String>,
}

impl QueryCoverage {
    /// Create coverage from fanout result
    pub fn from_fanout(result: &FanoutResult) -> Self {
        Self {
            responding_shards: result.responding_shards.len(),
            total_shards: result.total_shards,
            ratio: result.coverage(),
            missing_shards: result.missing_shards.clone(),
        }
    }

    /// Check if coverage is complete
    pub fn is_complete(&self) -> bool {
        self.missing_shards.is_empty() && self.responding_shards == self.total_shards
    }

    /// Check if coverage meets minimum threshold
    pub fn meets_threshold(&self, threshold: f32) -> bool {
        self.ratio >= threshold
    }
}

/// Timing information for query phases
#[derive(Debug, Clone)]
pub struct QueryTiming {
    /// Total elapsed time
    pub total_elapsed: Duration,
    /// Time spent in routing phase
    pub routing_duration: Option<Duration>,
    /// Time spent in execution phase
    pub execution_duration: Option<Duration>,
    /// Time spent in merging phase
    pub merging_duration: Option<Duration>,
    /// Original deadline
    pub deadline: Duration,
    /// Whether the query timed out
    pub timed_out: bool,
}

impl QueryTiming {
    fn new(deadline: Duration) -> Self {
        Self {
            total_elapsed: Duration::ZERO,
            routing_duration: None,
            execution_duration: None,
            merging_duration: None,
            deadline,
            timed_out: false,
        }
    }

    /// Check remaining time budget
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_sub(self.total_elapsed)
    }

    /// Check if deadline has been exceeded
    pub fn is_expired(&self) -> bool {
        self.total_elapsed >= self.deadline
    }
}

/// Result of a query workflow execution
#[derive(Debug)]
pub struct QueryWorkflowResult {
    /// Final state
    pub state: QueryState,
    /// Search results (may be partial if timed out)
    pub results: Vec<SearchResult>,
    /// Coverage information
    pub coverage: QueryCoverage,
    /// Timing breakdown
    pub timing: QueryTiming,
    /// Error message if failed
    pub error: Option<String>,
}

impl QueryWorkflowResult {
    /// Check if the query completed successfully with full coverage
    pub fn is_success(&self) -> bool {
        self.state == QueryState::Completed && self.coverage.is_complete()
    }

    /// Check if results are partial (timeout or missing shards)
    pub fn is_partial(&self) -> bool {
        !self.coverage.is_complete() || self.state == QueryState::TimedOut
    }

    /// Get coverage percentage
    pub fn coverage_percent(&self) -> f32 {
        self.coverage.ratio * 100.0
    }
}

/// Query workflow executor
///
/// Manages the lifecycle of a search query with explicit state transitions,
/// timeout budgeting, and coverage reporting.
pub struct QueryWorkflow {
    /// Collection to query
    collection: String,
    /// Query vector
    query: Vec<f32>,
    /// Number of results to return
    top_k: usize,
    /// Number of probes for FAISS
    nprobe: u32,
    /// Deadline for entire query
    deadline: Duration,
    /// Minimum acceptable coverage (0.0 to 1.0)
    min_coverage: f32,
    /// Current state
    state: QueryState,
}

impl QueryWorkflow {
    /// Create a new query workflow
    pub fn new(query: Vec<f32>, top_k: usize, deadline: Duration) -> Self {
        Self {
            collection: "default".to_string(),
            query,
            top_k,
            nprobe: 10, // default nprobe
            deadline,
            min_coverage: 0.0, // accept any coverage by default
            state: QueryState::Pending,
        }
    }

    /// Set the collection to query.
    pub fn with_collection(mut self, collection: impl Into<String>) -> Self {
        self.collection = collection.into();
        self
    }

    /// Set the nprobe parameter for FAISS
    pub fn with_nprobe(mut self, nprobe: u32) -> Self {
        self.nprobe = nprobe;
        self
    }

    /// Set minimum acceptable coverage
    ///
    /// If coverage falls below this threshold, the result will be marked
    /// as a failure rather than partial success.
    pub fn with_min_coverage(mut self, min_coverage: f32) -> Self {
        self.min_coverage = min_coverage.clamp(0.0, 1.0);
        self
    }

    /// Get current state
    pub fn current_state(&self) -> QueryState {
        self.state
    }

    /// Execute the query workflow
    pub async fn execute(mut self, executor: &FanoutExecutor) -> QueryWorkflowResult {
        let start = Instant::now();
        let mut timing = QueryTiming::new(self.deadline);

        debug!(
            "Starting query workflow: top_k={}, nprobe={}, deadline={:?}",
            self.top_k, self.nprobe, self.deadline
        );

        // Transition: Pending -> Routing
        self.state = QueryState::Routing;
        let routing_start = Instant::now();

        // Check deadline
        if start.elapsed() >= self.deadline {
            warn!("Query timed out before routing");
            timing.total_elapsed = start.elapsed();
            timing.timed_out = true;
            return QueryWorkflowResult {
                state: QueryState::TimedOut,
                results: vec![],
                coverage: QueryCoverage {
                    responding_shards: 0,
                    total_shards: 0,
                    ratio: 0.0,
                    missing_shards: vec![],
                },
                timing,
                error: Some("Timed out before routing".to_string()),
            };
        }

        timing.routing_duration = Some(routing_start.elapsed());

        // Transition: Routing -> Executing
        self.state = QueryState::Executing;
        let execution_start = Instant::now();

        // Calculate remaining time budget for execution
        let remaining = self.deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            warn!("Query timed out before execution");
            timing.total_elapsed = start.elapsed();
            timing.timed_out = true;
            return QueryWorkflowResult {
                state: QueryState::TimedOut,
                results: vec![],
                coverage: QueryCoverage {
                    responding_shards: 0,
                    total_shards: 0,
                    ratio: 0.0,
                    missing_shards: vec![],
                },
                timing,
                error: Some("Timed out before execution".to_string()),
            };
        }

        // Execute fan-out search with timeout
        let search_result = tokio::time::timeout(
            remaining,
            executor.search(&self.collection, &self.query, self.top_k, self.nprobe),
        )
        .await;

        timing.execution_duration = Some(execution_start.elapsed());

        match search_result {
            Ok(Ok(fanout_result)) => {
                // Transition: Executing -> Merging
                self.state = QueryState::Merging;
                let merging_start = Instant::now();

                let coverage = QueryCoverage::from_fanout(&fanout_result);

                timing.merging_duration = Some(merging_start.elapsed());
                timing.total_elapsed = start.elapsed();

                // Determine final state based on coverage
                if !coverage.meets_threshold(self.min_coverage) {
                    let coverage_pct = coverage.ratio * 100.0;
                    let min_pct = self.min_coverage * 100.0;
                    warn!(
                        "Query coverage {:.1}% below minimum threshold {:.1}%",
                        coverage_pct, min_pct
                    );
                    self.state = QueryState::Failed;
                    return QueryWorkflowResult {
                        state: QueryState::Failed,
                        results: fanout_result.results,
                        coverage,
                        timing,
                        error: Some(format!(
                            "Coverage {:.1}% below minimum {:.1}%",
                            coverage_pct, min_pct
                        )),
                    };
                }

                // Transition: Merging -> Completed
                self.state = QueryState::Completed;

                info!(
                    "Query workflow completed: {} results, {:.1}% coverage, {:?}",
                    fanout_result.results.len(),
                    coverage.ratio * 100.0,
                    timing.total_elapsed
                );

                QueryWorkflowResult {
                    state: QueryState::Completed,
                    results: fanout_result.results,
                    coverage,
                    timing,
                    error: None,
                }
            }
            Ok(Err(e)) => {
                // Execution error
                self.state = QueryState::Failed;
                timing.total_elapsed = start.elapsed();

                warn!("Query workflow failed: {}", e);

                QueryWorkflowResult {
                    state: QueryState::Failed,
                    results: vec![],
                    coverage: QueryCoverage {
                        responding_shards: 0,
                        total_shards: 0,
                        ratio: 0.0,
                        missing_shards: vec![],
                    },
                    timing,
                    error: Some(e.to_string()),
                }
            }
            Err(_) => {
                // Timeout during execution
                self.state = QueryState::TimedOut;
                timing.total_elapsed = start.elapsed();
                timing.timed_out = true;

                warn!(
                    "Query workflow timed out after {:?}",
                    timing.total_elapsed
                );

                QueryWorkflowResult {
                    state: QueryState::TimedOut,
                    results: vec![],
                    coverage: QueryCoverage {
                        responding_shards: 0,
                        total_shards: 0,
                        ratio: 0.0,
                        missing_shards: vec![],
                    },
                    timing,
                    error: Some("Query execution timed out".to_string()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_state_terminal() {
        assert!(!QueryState::Pending.is_terminal());
        assert!(!QueryState::Routing.is_terminal());
        assert!(!QueryState::Executing.is_terminal());
        assert!(!QueryState::Merging.is_terminal());
        assert!(QueryState::Completed.is_terminal());
        assert!(QueryState::TimedOut.is_terminal());
        assert!(QueryState::Failed.is_terminal());
    }

    #[test]
    fn test_query_coverage_threshold() {
        let coverage = QueryCoverage {
            responding_shards: 3,
            total_shards: 4,
            ratio: 0.75,
            missing_shards: vec!["shard-4".to_string()],
        };

        assert!(coverage.meets_threshold(0.5));
        assert!(coverage.meets_threshold(0.75));
        assert!(!coverage.meets_threshold(0.76));
        assert!(!coverage.meets_threshold(1.0));
        assert!(!coverage.is_complete());
    }

    #[test]
    fn test_query_coverage_is_incomplete_when_responding_count_is_short() {
        let coverage = QueryCoverage {
            responding_shards: 1,
            total_shards: 2,
            ratio: 0.5,
            missing_shards: vec![],
        };

        assert!(!coverage.is_complete());
    }

    #[test]
    fn test_query_timing_remaining() {
        let mut timing = QueryTiming::new(Duration::from_secs(10));
        assert_eq!(timing.remaining(), Duration::from_secs(10));

        timing.total_elapsed = Duration::from_secs(3);
        assert_eq!(timing.remaining(), Duration::from_secs(7));
        assert!(!timing.is_expired());

        timing.total_elapsed = Duration::from_secs(10);
        assert_eq!(timing.remaining(), Duration::ZERO);
        assert!(timing.is_expired());

        timing.total_elapsed = Duration::from_secs(15);
        assert_eq!(timing.remaining(), Duration::ZERO);
        assert!(timing.is_expired());
    }

    #[test]
    fn test_workflow_builder() {
        let query = vec![1.0f32; 128];
        let workflow = QueryWorkflow::new(query.clone(), 10, Duration::from_secs(5))
            .with_collection("tenant-a")
            .with_nprobe(20)
            .with_min_coverage(0.8);

        assert_eq!(workflow.collection, "tenant-a");
        assert_eq!(workflow.query, query);
        assert_eq!(workflow.top_k, 10);
        assert_eq!(workflow.nprobe, 20);
        assert_eq!(workflow.deadline, Duration::from_secs(5));
        assert_eq!(workflow.min_coverage, 0.8);
        assert_eq!(workflow.state, QueryState::Pending);
    }

    #[test]
    fn test_min_coverage_clamping() {
        let query = vec![1.0f32; 128];

        let workflow = QueryWorkflow::new(query.clone(), 10, Duration::from_secs(5))
            .with_min_coverage(1.5); // Above 1.0
        assert_eq!(workflow.min_coverage, 1.0);

        let workflow = QueryWorkflow::new(query.clone(), 10, Duration::from_secs(5))
            .with_min_coverage(-0.5); // Below 0.0
        assert_eq!(workflow.min_coverage, 0.0);
    }
}
