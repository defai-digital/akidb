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
//! - Static coordinator and shard routing

#![allow(clippy::result_large_err)]

mod backpressure;
mod batch;
mod compaction;
mod consistency;
mod discovery;
mod fanout;
mod merger;
mod metrics;
mod router;
mod server;
mod slo;
mod workflow;

pub use akidb_embedding::{
    ax_engine::AxEngineEmbedding, CacheStats, CachedEmbeddingService, EmbeddingConfig,
    EmbeddingError, EmbeddingService, EmbeddingStats, FallbackEmbeddingService,
    MockEmbeddingService,
};
pub use backpressure::{
    BackpressureConfig, BackpressureController, BackpressureError, BackpressureStats,
};
pub use batch::{BatchConfig, BatchProcessor, BatchResult};
pub use compaction::{CompactionConfig, CompactionScheduler, CompactionStats, CompactionTrigger};
pub use consistency::{ConsistencyConfig, ConsistencyStats, ConsistencyTracker, WriteEntry};
pub use discovery::{
    ClusterState, ClusterStateMessage, CoordinatorAnnouncement, CoordinatorMode, DiscoveryConfig,
    DiscoveryEvent, DiscoveryService, GossipEvent, MetricsMessage, NodeType, PeerInfo,
    ShardAnnouncement,
};
pub use fanout::{
    BroadcastDeleteResult, BroadcastUpdateResult, FanoutExecutor, FanoutResult,
    FanoutSearchOptions, PoolStats,
};
pub use merger::ResultMerger;
pub use metrics::{export_metrics, metrics as coordinator_metrics, CoordinatorMetrics};
pub use router::{DistributionStats, ShardInfo, ShardRouter};
pub use server::{run as run_server, Args as ServerArgs, CoordinatorService};
pub use slo::{SloConfig, SloEstimate, SloEstimator, SloStats};
pub use workflow::{QueryCoverage, QueryState, QueryTiming, QueryWorkflow, QueryWorkflowResult};
