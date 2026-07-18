//! AkiDB TUI Dashboard
//!
//! Terminal User Interface for monitoring AkiDB deployments.

pub mod action;
pub mod app;
pub mod client;
pub mod config;
pub mod effect;
pub mod events;
pub mod model;
pub mod runtime;
pub mod theme;
pub mod ui;

pub use app::{App, ClusterState, MetricsState};
pub use client::CoordinatorClient;
pub use config::TuiConfig;
pub use events::{Event, EventHandler};
pub use runtime::{run, Args};
pub use theme::Theme;
