use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;

static INIT: Once = Once::new();
static SAFE_MODE: AtomicBool = AtomicBool::new(true);

pub struct SafetyGuard;

impl SafetyGuard {
    pub fn enforce_global_safety() -> anyhow::Result<()> {
        INIT.call_once(|| {
            SAFE_MODE.store(true, Ordering::SeqCst);
            eprintln!("[SAFETY] Global safety mode enabled.");
        });

        if !SAFE_MODE.load(Ordering::SeqCst) {
            anyhow::bail!("Safety subsystem reported corrupted state.");
        }

        Self::validate_platform()?;
        Ok(())
    }

    fn validate_platform() -> anyhow::Result<()> {
        let os = std::env::consts::OS;

        // All features must work on Windows/Linux/Mac only
        match os {
            "windows" | "linux" | "macos" => Ok(()),
            _ => anyhow::bail!("Unsupported OS for backend."),
        }
    }

    pub fn safety_status() -> bool {
        SAFE_MODE.load(Ordering::SeqCst)
    }
}
