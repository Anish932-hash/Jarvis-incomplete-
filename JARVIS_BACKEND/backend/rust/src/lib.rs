#![recursion_limit = "256"]

pub mod audio_utils;
pub mod automation_engine;
pub mod file_access;
pub mod input_analyzer;
pub mod ipc_bridge;
pub mod safety_guard;
pub mod surface_router;
pub mod system_monitor;

#[cfg(target_os = "windows")]
pub mod windows_control;

use crate::safety_guard::SafetyGuard;
use crate::system_monitor::SystemMonitor;

pub async fn bootstrap() -> anyhow::Result<()> {
    SafetyGuard::enforce_global_safety()?;

    eprintln!("[BOOT] Initializing Rust subsystems...");

    // Optional background monitor task. Disabled by default so stdio RPC
    // callers do not receive noisy logs unless explicitly enabled.
    let monitor_enabled = std::env::var("JARVIS_RUST_BOOT_MONITOR")
        .map(|value| {
            let lowered = value.trim().to_ascii_lowercase();
            matches!(lowered.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false);
    if monitor_enabled {
        tokio::spawn(async {
            let mut monitor = SystemMonitor::new();
            loop {
                if let Err(e) = monitor.refresh().await {
                    eprintln!("[SystemMonitor] Error: {:?}", e);
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        });
    }

    eprintln!("[BOOT] Rust subsystem ready.");
    Ok(())
}
