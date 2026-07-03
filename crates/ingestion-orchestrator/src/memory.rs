//! Memory Coordinator for local memory pressure
//!
//! Monitors memory usage and pauses ingestion when pressure is detected.

use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use crate::config::MemoryConfig;

/// Memory coordinator for local memory monitoring
pub struct MemoryCoordinator {
    /// Whether memory pressure pause is active
    paused: AtomicBool,

    /// Current memory usage percentage (stored as f32 bits)
    /// FIX: Use AtomicU32 to match f32 bit width exactly, avoiding unnecessary casts
    usage_pct: AtomicU32,

    /// Total memory in MB
    total_mb: AtomicU64,

    /// Used memory in MB
    used_mb: AtomicU64,

    /// Configuration
    config: MemoryConfig,
}

impl MemoryCoordinator {
    /// Create a new memory coordinator
    pub fn new(config: MemoryConfig) -> Self {
        let config = normalize_config(config);
        Self {
            paused: AtomicBool::new(false),
            usage_pct: AtomicU32::new(0.0_f32.to_bits()),
            total_mb: AtomicU64::new(0),
            used_mb: AtomicU64::new(0),
            config,
        }
    }

    /// Check if ingestion is paused due to memory pressure
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Get current memory usage percentage
    /// FIX: Direct conversion without unnecessary u64->u32 cast
    pub fn usage_percent(&self) -> f32 {
        f32::from_bits(self.usage_pct.load(Ordering::SeqCst))
    }

    /// Start the background monitoring task (must be called on `Arc<Self>`)
    pub fn start_monitoring(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = Arc::clone(self);

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(this.config.poll_interval_ms));

            loop {
                ticker.tick().await;

                match parse_system_memory() {
                    Ok((used, total)) => {
                        let pct = (used as f32 / total as f32) * 100.0;

                        this.total_mb.store(total, Ordering::SeqCst);
                        this.used_mb.store(used, Ordering::SeqCst);
                        // FIX: Direct store without unnecessary u32->u64 cast
                        this.usage_pct.store(pct.to_bits(), Ordering::SeqCst);

                        debug!(used_mb = used, total_mb = total, pct, "Memory stats");

                        if pct >= this.config.pause_threshold_pct {
                            if !this.paused.swap(true, Ordering::SeqCst) {
                                warn!(
                                    used_mb = used,
                                    total_mb = total,
                                    pct,
                                    threshold = this.config.pause_threshold_pct,
                                    "Pausing ingestion due to memory pressure"
                                );
                            }
                        } else if pct <= this.config.resume_threshold_pct
                            && this.paused.swap(false, Ordering::SeqCst)
                        {
                            info!(
                                used_mb = used,
                                total_mb = total,
                                pct,
                                "Resuming ingestion, memory recovered"
                            );
                        }
                    }
                    Err(e) => {
                        error!(?e, "Failed to parse system memory");
                    }
                }
            }
        })
    }

    /// Get current statistics
    pub fn stats(&self) -> MemoryStats {
        MemoryStats {
            paused: self.is_paused(),
            usage_pct: self.usage_percent(),
            total_mb: self.total_mb.load(Ordering::SeqCst),
            used_mb: self.used_mb.load(Ordering::SeqCst),
        }
    }
}

fn normalize_config(config: MemoryConfig) -> MemoryConfig {
    MemoryConfig {
        poll_interval_ms: config.poll_interval_ms.max(1),
        ..config
    }
}

/// Memory statistics
#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub paused: bool,
    pub usage_pct: f32,
    pub total_mb: u64,
    pub used_mb: u64,
}

/// Parse system memory usage in MB.
fn parse_system_memory() -> Result<(u64, u64), String> {
    parse_macos_memory().or_else(|_| parse_proc_meminfo())
}

/// Parse macOS memory counters from sysctl/vm_stat.
fn parse_macos_memory() -> Result<(u64, u64), String> {
    let total_output = Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .map_err(|e| format!("Failed to run sysctl: {}", e))?;
    if !total_output.status.success() {
        return Err("sysctl hw.memsize failed".to_string());
    }

    let total_bytes = String::from_utf8_lossy(&total_output.stdout)
        .trim()
        .parse::<u64>()
        .map_err(|e| format!("Failed to parse hw.memsize: {}", e))?;

    let vm_output = Command::new("vm_stat")
        .output()
        .map_err(|e| format!("Failed to run vm_stat: {}", e))?;
    if !vm_output.status.success() {
        return Err("vm_stat failed".to_string());
    }

    let vm_stat = String::from_utf8_lossy(&vm_output.stdout);
    parse_vm_stat(&vm_stat, total_bytes)
}

fn parse_vm_stat(output: &str, total_bytes: u64) -> Result<(u64, u64), String> {
    let mut page_size = 4096u64;
    let mut free_pages = 0u64;
    let mut speculative_pages = 0u64;

    for line in output.lines() {
        if let Some(start) = line.find("page size of ") {
            let size_part = &line[start + "page size of ".len()..];
            if let Some(end) = size_part.find(" bytes") {
                page_size = size_part[..end]
                    .parse::<u64>()
                    .map_err(|e| format!("Failed to parse page size: {}", e))?;
            }
        } else if line.starts_with("Pages free:") {
            free_pages = parse_vm_stat_pages(line)?;
        } else if line.starts_with("Pages speculative:") {
            speculative_pages = parse_vm_stat_pages(line)?;
        }
    }

    let available_bytes = free_pages
        .saturating_add(speculative_pages)
        .saturating_mul(page_size);
    let used_bytes = total_bytes.saturating_sub(available_bytes);

    Ok((used_bytes / 1024 / 1024, total_bytes / 1024 / 1024))
}

fn parse_vm_stat_pages(line: &str) -> Result<u64, String> {
    line.split(':')
        .nth(1)
        .ok_or_else(|| "Invalid vm_stat line".to_string())?
        .trim()
        .trim_end_matches('.')
        .replace('.', "")
        .parse::<u64>()
        .map_err(|e| format!("Failed to parse vm_stat pages: {}", e))
}

/// Fallback: parse /proc/meminfo
fn parse_proc_meminfo() -> Result<(u64, u64), String> {
    let content = std::fs::read_to_string("/proc/meminfo")
        .map_err(|e| format!("Failed to read /proc/meminfo: {}", e))?;

    let mut total_kb = 0u64;
    let mut available_kb = 0u64;

    for line in content.lines() {
        if line.starts_with("MemTotal:") {
            total_kb = parse_meminfo_value(line)?;
        } else if line.starts_with("MemAvailable:") {
            available_kb = parse_meminfo_value(line)?;
        }
    }

    if total_kb == 0 {
        return Err("Could not find MemTotal".to_string());
    }

    let used_kb = total_kb.saturating_sub(available_kb);
    Ok((used_kb / 1024, total_kb / 1024)) // Convert to MB
}

fn parse_meminfo_value(line: &str) -> Result<u64, String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() >= 2 {
        parts[1]
            .parse::<u64>()
            .map_err(|e| format!("Failed to parse meminfo value: {}", e))
    } else {
        Err("Invalid meminfo line".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_vm_stat() {
        let output = "\
Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                               1000.
Pages speculative:                         500.
Pages active:                            10000.
";
        let result = parse_vm_stat(output, 32 * 1024 * 1024);
        assert!(result.is_ok());
        let (used, total) = result.unwrap();
        assert_eq!(used, 8);
        assert_eq!(total, 32);
    }

    #[test]
    fn test_memory_coordinator_initial_state() {
        let config = MemoryConfig {
            pause_threshold_pct: 70.0,
            resume_threshold_pct: 60.0,
            poll_interval_ms: 1000,
            // FIX BUG-H052: Include max pause duration in test config
            max_pause_duration_secs: 300,
        };
        let mc = MemoryCoordinator::new(config);
        assert!(!mc.is_paused());
    }

    #[tokio::test]
    async fn test_zero_poll_interval_does_not_panic_monitoring_task() {
        let config = MemoryConfig {
            pause_threshold_pct: 70.0,
            resume_threshold_pct: 60.0,
            poll_interval_ms: 0,
            max_pause_duration_secs: 300,
        };
        let mc = Arc::new(MemoryCoordinator::new(config));

        let handle = mc.start_monitoring();
        tokio::task::yield_now().await;

        assert!(
            !handle.is_finished(),
            "memory monitoring task should keep running with a normalized poll interval"
        );
        handle.abort();
    }
}
