//! AkiDB TUI Dashboard
//!
//! Terminal User Interface for monitoring AkiDB deployments.

pub mod app;
pub mod client;
pub mod config;
pub mod events;
pub mod theme;
pub mod ui;

pub use app::{App, ClusterState, MetricsState};
pub use client::CoordinatorClient;
pub use config::TuiConfig;
pub use events::{Event, EventHandler};
pub use theme::Theme;
