use serde_json::{json, Value};
use sysinfo::System;

pub struct SystemMonitor {
    sys: System,
}

impl SystemMonitor {
    pub fn new() -> Self {
        let mut sys = System::new_all();
        sys.refresh_all();
        SystemMonitor { sys }
    }

    pub async fn refresh(&mut self) -> anyhow::Result<()> {
        self.sys.refresh_cpu();
        self.sys.refresh_memory();

        let emit_logs = std::env::var("JARVIS_RUST_MONITOR_LOG")
            .map(|value| {
                let lowered = value.trim().to_ascii_lowercase();
                matches!(lowered.as_str(), "1" | "true" | "yes" | "on")
            })
            .unwrap_or(false);
        if emit_logs {
            let snapshot = self.build_snapshot();
            eprintln!("[SYSTEM] {}", snapshot);
        }

        Ok(())
    }

    pub async fn snapshot(&mut self) -> anyhow::Result<Value> {
        self.refresh().await?;
        Ok(self.build_snapshot())
    }

    fn build_snapshot(&self) -> Value {
        let total_mem = self.sys.total_memory();
        let used_mem = self.sys.used_memory();
        let mem_percent = if total_mem > 0 {
            (used_mem as f64 / total_mem as f64) * 100.0
        } else {
            0.0
        };

        let cpu_load: Vec<f32> = self.sys.cpus().iter().map(|c| c.cpu_usage()).collect();

        let json_data = json!({
            "memory": {
                "used": used_mem,
                "total": total_mem,
                "percent": format!("{:.2}", mem_percent)
            },
            "cpu": {
                "cores": cpu_load
            },
            "temperature": []
        });

        json_data
    }
}
