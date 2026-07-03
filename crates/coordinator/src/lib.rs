//! AkiDB Coordinator - Fan-out search coordination
//!
//! The coordinator handles:
//! - Routing queries to appropriate shards
//! - Fan-out parallel search across shards
//! - Result merging with min-heap
//! - Partial result handling when shards are unavailable
//! - Read-your-writes consistency
//! - Backpressure and rate limiting
//! - Automatic compaction scheduling
//! - SLO estimation and monitoring
//! - Embedding service with caching and fallback
//! - Auto-discovery of coordinators and shards (optional, with `discovery` feature)

#![allow(clippy::result_large_err)]

pub mod backpressure;
pub mod batch;
pub mod compaction;
pub mod consistency;
pub mod discovery;
pub mod embedding;
pub mod fanout;
pub mod merger;
pub mod metrics;
pub mod router;
pub mod server;
pub mod slo;
pub mod workflow;

pub use backpressure::{
    BackpressureConfig, BackpressureController, BackpressureError, BackpressureStats,
};
pub use batch::{BatchConfig, BatchProcessor, BatchResult};
pub use compaction::{CompactionConfig, CompactionScheduler, CompactionStats, CompactionTrigger};
pub use consistency::{ConsistencyConfig, ConsistencyStats, ConsistencyTracker, WriteEntry};
pub use discovery::{
    ClusterState, ClusterStateMessage, CoordinatorAnnouncement, CoordinatorMode, DiscoveryConfig,
    DiscoveryService, ShardAnnouncement,
};
pub use embedding::{
    ax_engine::AxEngineEmbedding, CacheStats, CachedEmbeddingService, EmbeddingConfig,
    EmbeddingError, EmbeddingService, FallbackEmbeddingService, MockEmbeddingService,
};
pub use fanout::{
    BroadcastDeleteResult, BroadcastUpdateResult, FanoutExecutor, FanoutResult, PoolStats,
};
pub use merger::ResultMerger;
pub use metrics::{export_metrics, metrics as coordinator_metrics, CoordinatorMetrics};
pub use router::{DistributionStats, ShardInfo, ShardRouter};
pub use server::{run as run_server, Args as ServerArgs, CoordinatorService};
pub use slo::{SloConfig, SloEstimate, SloEstimator, SloStats};
pub use workflow::{QueryCoverage, QueryState, QueryTiming, QueryWorkflow, QueryWorkflowResult};
