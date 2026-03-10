use lazy_static::lazy_static;
use serde_json::json;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

lazy_static! {
    static ref INPUT_DATA: Mutex<InputAnalyzer> = Mutex::new(InputAnalyzer::new());
}

pub struct InputAnalyzer {
    last_input_time: Instant,
    key_count: u64,
    session_start: Instant,
}

impl InputAnalyzer {
    pub fn new() -> Self {
        Self {
            last_input_time: Instant::now(),
            key_count: 0,
            session_start: Instant::now(),
        }
    }

    pub fn register_keystroke(&mut self) {
        self.key_count += 1;
        self.last_input_time = Instant::now();
    }

    pub fn idle_duration(&self) -> Duration {
        Instant::now().duration_since(self.last_input_time)
    }

    pub fn session_duration(&self) -> Duration {
        Instant::now().duration_since(self.session_start)
    }

    pub fn typing_speed(&self) -> f64 {
        let secs = self.session_duration().as_secs_f64();
        if secs == 0.0 {
            return 0.0;
        }
        self.key_count as f64 / secs
    }

    pub async fn snapshot() -> serde_json::Value {
        let data = INPUT_DATA.lock().await;

        json!({
            "keys_pressed": data.key_count,
            "typing_speed_keys_per_sec": data.typing_speed(),
            "idle_ms": data.idle_duration().as_millis(),
            "session_secs": data.session_duration().as_secs()
        })
    }
}
