use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use jarvis_backend::audio_utils::AudioUtils;
use jarvis_backend::automation_engine::{
    AutomationEngine, AutomationExecutionOptions, AutomationTaskSpec,
};
use jarvis_backend::bootstrap;
use jarvis_backend::file_access::FileAccess;
use jarvis_backend::input_analyzer::InputAnalyzer;
use jarvis_backend::ipc_bridge::IpcBridge;
use jarvis_backend::safety_guard::SafetyGuard;
use jarvis_backend::system_monitor::SystemMonitor;
#[cfg(target_os = "windows")]
use jarvis_backend::windows_control::WindowsControl;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Deserialize)]
struct RpcRequest {
    event: String,
    #[serde(default)]
    payload: Value,
}

#[derive(Debug, Serialize)]
struct RpcResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_to: Option<String>,
    event: String,
    status: String,
    data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

struct RuntimeState {
    started_at: Instant,
    monitor: Mutex<SystemMonitor>,
    max_concurrent: usize,
    max_queue: usize,
    limiter: Arc<Semaphore>,
    inflight: Mutex<HashMap<String, InFlightRequest>>,
    queued_count: AtomicUsize,
    accepted_total: AtomicU64,
    completed_total: AtomicU64,
    backpressure_rejected_total: AtomicU64,
    cancel_requested_total: AtomicU64,
    cancel_applied_total: AtomicU64,
}

#[derive(Clone)]
struct InFlightRequest {
    event: String,
    started_at: Instant,
    cancel_token: CancellationToken,
}

impl RuntimeState {
    fn env_usize(name: &str, default: usize, minimum: usize, maximum: usize) -> usize {
        let raw = std::env::var(name).unwrap_or_default();
        let parsed = raw
            .trim()
            .parse::<usize>()
            .ok()
            .unwrap_or(default)
            .clamp(minimum, maximum);
        parsed
    }

    fn new() -> Self {
        let max_concurrent = Self::env_usize("JARVIS_RUST_MAX_CONCURRENT", 4, 1, 64);
        let max_queue = Self::env_usize("JARVIS_RUST_MAX_QUEUE", 24, 0, 5000);
        Self {
            started_at: Instant::now(),
            monitor: Mutex::new(SystemMonitor::new()),
            max_concurrent,
            max_queue,
            limiter: Arc::new(Semaphore::new(max_concurrent)),
            inflight: Mutex::new(HashMap::new()),
            queued_count: AtomicUsize::new(0),
            accepted_total: AtomicU64::new(0),
            completed_total: AtomicU64::new(0),
            backpressure_rejected_total: AtomicU64::new(0),
            cancel_requested_total: AtomicU64::new(0),
            cancel_applied_total: AtomicU64::new(0),
        }
    }

    fn limiter(&self) -> Arc<Semaphore> {
        Arc::clone(&self.limiter)
    }

    fn should_reject_backpressure(&self) -> bool {
        self.limiter.available_permits() == 0
            && self.queued_count.load(Ordering::Relaxed) >= self.max_queue
    }

    async fn register_inflight(&self, request_id: String, event: String, cancel_token: CancellationToken) {
        let mut inflight = self.inflight.lock().await;
        inflight.insert(
            request_id,
            InFlightRequest {
                event,
                started_at: Instant::now(),
                cancel_token,
            },
        );
    }

    async fn unregister_inflight(&self, request_id: &str) {
        let mut inflight = self.inflight.lock().await;
        inflight.remove(request_id);
    }

    async fn cancel_inflight(&self, request_id: &str, reason: &str) -> Value {
        self.cancel_requested_total.fetch_add(1, Ordering::Relaxed);
        let mut event = String::new();
        let mut running_ms = 0u128;
        let cancelled = {
            let inflight = self.inflight.lock().await;
            if let Some(entry) = inflight.get(request_id) {
                event = entry.event.clone();
                running_ms = entry.started_at.elapsed().as_millis();
                entry.cancel_token.cancel();
                true
            } else {
                false
            }
        };
        if cancelled {
            self.cancel_applied_total.fetch_add(1, Ordering::Relaxed);
        }
        json!({
            "status": "success",
            "target_request_id": request_id,
            "cancelled": cancelled,
            "reason": reason,
            "event": event,
            "running_ms": running_ms,
            "known_inflight": cancelled,
        })
    }

    async fn runtime_load_snapshot(&self) -> Value {
        let queued = self.queued_count.load(Ordering::Relaxed);
        let available_permits = self.limiter.available_permits();
        let mut inflight_items: Vec<Value> = Vec::new();
        let mut event_class_counts: HashMap<String, usize> = HashMap::new();
        let running = {
            let inflight = self.inflight.lock().await;
            for (request_id, row) in inflight.iter().take(24) {
                let class_name = event_class_name(&row.event).to_string();
                *event_class_counts.entry(class_name).or_insert(0) += 1;
                inflight_items.push(json!({
                    "request_id": request_id,
                    "event": row.event,
                    "running_ms": row.started_at.elapsed().as_millis(),
                }));
            }
            inflight.len()
        };
        let overloaded = available_permits == 0 && queued >= self.max_queue;
        let utilization = if self.max_concurrent > 0 {
            (running as f64) / (self.max_concurrent as f64)
        } else {
            0.0
        };
        let queue_ratio = if self.max_queue > 0 {
            (queued as f64) / (self.max_queue as f64)
        } else {
            0.0
        };
        let mut pressure_score = (utilization * 0.58) + (queue_ratio * 0.34);
        if overloaded {
            pressure_score += 0.22;
        } else if available_permits == 0 {
            pressure_score += 0.1;
        }
        pressure_score = pressure_score.clamp(0.0, 1.0);

        let mut recommended_parallel_cap = self.max_concurrent.max(1);
        if queued > 0 {
            let queue_penalty = ((queued as f64) * 0.34).ceil() as usize;
            recommended_parallel_cap = recommended_parallel_cap.saturating_sub(queue_penalty).max(1);
        }
        if pressure_score >= 0.9 {
            recommended_parallel_cap = 1;
        } else if pressure_score >= 0.74 {
            recommended_parallel_cap = recommended_parallel_cap.min(2).max(1);
        } else if pressure_score >= 0.58 {
            recommended_parallel_cap = recommended_parallel_cap.min(3).max(1);
        }
        if available_permits > 0 {
            recommended_parallel_cap = recommended_parallel_cap.min(available_permits.max(1));
        }
        let suggested_retry_mode = if pressure_score >= 0.9 {
            "stabilize"
        } else if pressure_score >= 0.66 {
            "adaptive_backoff"
        } else if pressure_score >= 0.4 {
            "probe_then_backoff"
        } else {
            "immediate"
        };
        let suggested_retry_delay_s = if suggested_retry_mode == "immediate" {
            0.0
        } else {
            (1.2 + (pressure_score * 6.4) + ((queued as f64) * 0.12)).clamp(0.0, 120.0)
        };

        let mut event_pressure_rows: Vec<Value> = event_class_counts
            .iter()
            .map(|(class_name, count)| {
                let class_share = if running > 0 {
                    (*count as f64) / (running as f64)
                } else {
                    0.0
                };
                let class_pressure = (class_share * pressure_score).clamp(0.0, 1.0);
                json!({
                    "class": class_name,
                    "running": *count,
                    "share": class_share,
                    "pressure": class_pressure,
                })
            })
            .collect();
        event_pressure_rows.sort_by(|left, right| {
            let lp = left.get("pressure").and_then(Value::as_f64).unwrap_or(0.0);
            let rp = right.get("pressure").and_then(Value::as_f64).unwrap_or(0.0);
            rp.partial_cmp(&lp).unwrap_or(std::cmp::Ordering::Equal)
        });
        let pressure_band = if pressure_score >= 0.86 {
            "critical"
        } else if pressure_score >= 0.66 {
            "high"
        } else if pressure_score >= 0.4 {
            "medium"
        } else {
            "low"
        };
        let policy_hint = json!({
            "pressure_score": pressure_score,
            "pressure_band": pressure_band,
            "recommended_parallel_cap": recommended_parallel_cap,
            "suggested_retry_mode": suggested_retry_mode,
            "suggested_retry_delay_s": suggested_retry_delay_s,
            "queue_ratio": queue_ratio.clamp(0.0, 1.0),
            "event_class_pressure": event_pressure_rows,
        });
        json!({
            "status": "success",
            "max_concurrent": self.max_concurrent,
            "max_queue": self.max_queue,
            "running": running,
            "queued": queued,
            "available_permits": available_permits,
            "utilization": utilization,
            "queue_ratio": queue_ratio.clamp(0.0, 1.0),
            "overloaded": overloaded,
            "accepted_total": self.accepted_total.load(Ordering::Relaxed),
            "completed_total": self.completed_total.load(Ordering::Relaxed),
            "backpressure_rejected_total": self.backpressure_rejected_total.load(Ordering::Relaxed),
            "cancel_requested_total": self.cancel_requested_total.load(Ordering::Relaxed),
            "cancel_applied_total": self.cancel_applied_total.load(Ordering::Relaxed),
            "inflight": inflight_items,
            "policy_hint": policy_hint,
            "uptime_s": self.started_at.elapsed().as_secs_f64(),
        })
    }
}

fn request_id(payload: &Value) -> Option<String> {
    payload
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn event_class_name(event: &str) -> &'static str {
    let clean = event.trim().to_ascii_lowercase();
    if clean.is_empty() {
        return "unknown";
    }
    if clean.contains("file_") {
        return "file_io";
    }
    if clean.contains("window") || clean.contains("desktop") || clean.contains("input_") {
        return "desktop_context";
    }
    if clean.contains("automation") || clean.contains("batch_") {
        return "automation";
    }
    if clean.contains("audio") {
        return "audio";
    }
    if clean.contains("runtime") || clean.contains("health") || clean.contains("capabilities") {
        return "control_plane";
    }
    "general"
}

fn response_ok(reply_to: Option<String>, event: &str, data: Value) -> RpcResponse {
    RpcResponse {
        reply_to,
        event: event.to_string(),
        status: "success".to_string(),
        data,
        message: None,
    }
}

fn response_error(reply_to: Option<String>, event: &str, message: String) -> RpcResponse {
    RpcResponse {
        reply_to,
        event: event.to_string(),
        status: "error".to_string(),
        data: Value::Null,
        message: Some(message),
    }
}

fn required_str<'a>(payload: &'a Value, key: &str) -> Result<&'a str, String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("payload.{key} is required"))
}

fn parse_automation_tasks(payload: &Value) -> Result<Vec<AutomationTaskSpec>, String> {
    let tasks_value = payload
        .get("tasks")
        .cloned()
        .ok_or_else(|| "payload.tasks is required".to_string())?;
    serde_json::from_value::<Vec<AutomationTaskSpec>>(tasks_value).map_err(|err| err.to_string())
}

fn parse_automation_options(payload: &Value) -> Result<AutomationExecutionOptions, String> {
    let options_value = payload.get("options").cloned().unwrap_or_else(|| json!({}));
    serde_json::from_value::<AutomationExecutionOptions>(options_value)
        .map_err(|err| format!("payload.options is invalid: {err}"))
}

fn parse_batch_requests(payload: &Value) -> Result<Vec<(String, Value)>, String> {
    let rows = payload
        .get("requests")
        .and_then(Value::as_array)
        .ok_or_else(|| "payload.requests must be an array".to_string())?;
    if rows.is_empty() {
        return Err("payload.requests cannot be empty".to_string());
    }

    let mut parsed = Vec::with_capacity(rows.len());
    for (index, item) in rows.iter().enumerate() {
        let event = item
            .get("event")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("payload.requests[{index}].event is required"))?;
        let item_payload = item.get("payload").cloned().unwrap_or_else(|| json!({}));
        parsed.push((event.to_string(), item_payload));
    }
    Ok(parsed)
}

#[async_recursion::async_recursion]
async fn dispatch_internal(
    runtime: &RuntimeState,
    event: &str,
    payload: &Value,
    depth: usize,
    cancel_token: &CancellationToken,
) -> Result<Value, String> {
    if cancel_token.is_cancelled() {
        return Err("Request cancelled by control plane.".to_string());
    }
    if depth > 4 {
        return Err("Dispatch recursion limit exceeded.".to_string());
    }

    match event {
        "health_check" => Ok(json!({
            "status": "ok",
            "service": "jarvis-rust-backend",
            "version": env!("CARGO_PKG_VERSION"),
            "safety_enabled": SafetyGuard::safety_status(),
            "uptime_s": runtime.started_at.elapsed().as_secs_f64(),
        })),
        "capabilities" => Ok(json!({
            "supported_events": [
                "health_check",
                "safety_status",
                "capabilities",
                "bridge_runtime_snapshot",
                "runtime_load",
                "runtime_policy_snapshot",
                "cancel_request",
                "echo",
                "system_snapshot",
                "desktop_context",
                "batch_execute",
                "audio_probe",
                "automation_plan_execute",
                "active_window",
                "list_windows",
                "input_snapshot",
                "file_hash",
                "file_read_json",
                "file_write_json",
                "file_read_text",
                "file_write_text",
            ]
        })),
        "safety_status" => Ok(json!({
            "safe_mode": SafetyGuard::safety_status(),
            "platform": std::env::consts::OS,
        })),
        "bridge_runtime_snapshot" => {
            let runtime_load = runtime.runtime_load_snapshot().await;
            Ok(json!({
                "status": "success",
                "service": "jarvis-rust-backend",
                "version": env!("CARGO_PKG_VERSION"),
                "platform": std::env::consts::OS,
                "safe_mode": SafetyGuard::safety_status(),
                "uptime_s": runtime.started_at.elapsed().as_secs_f64(),
                "runtime_load": runtime_load,
                "core_events": [
                    "health_check",
                    "safety_status",
                    "capabilities",
                    "bridge_runtime_snapshot",
                    "runtime_load",
                    "runtime_policy_snapshot",
                    "cancel_request",
                    "system_snapshot",
                    "desktop_context",
                    "batch_execute",
                    "automation_plan_execute",
                    "input_snapshot",
                    "active_window",
                    "list_windows",
                ],
            }))
        }
        "runtime_load" => Ok(runtime.runtime_load_snapshot().await),
        "runtime_policy_snapshot" => {
            let runtime_load = runtime.runtime_load_snapshot().await;
            let policy_hint = runtime_load
                .get("policy_hint")
                .cloned()
                .unwrap_or_else(|| json!({}));
            Ok(json!({
                "status": "success",
                "runtime_load": runtime_load,
                "policy_hint": policy_hint,
            }))
        }
        "cancel_request" => {
            let target_request_id = required_str(payload, "target_request_id")?;
            let reason = payload
                .get("reason")
                .and_then(Value::as_str)
                .map(str::trim)
                .unwrap_or("");
            Ok(runtime.cancel_inflight(target_request_id, reason).await)
        }
        "echo" => Ok(payload.clone()),
        "system_snapshot" => {
            let mut monitor = runtime.monitor.lock().await;
            monitor.snapshot().await.map_err(|err| err.to_string())
        }
        "desktop_context" => {
            let started = Instant::now();
            let system =
                dispatch_internal(runtime, "system_snapshot", &json!({}), depth + 1, cancel_token)
                    .await?;
            let input =
                dispatch_internal(runtime, "input_snapshot", &json!({}), depth + 1, cancel_token)
                    .await?;

            #[cfg(target_os = "windows")]
            let window =
                match dispatch_internal(
                    runtime,
                    "active_window",
                    &json!({}),
                    depth + 1,
                    cancel_token,
                )
                .await
                {
                    Ok(value) => json!({"status": "success", "data": value}),
                    Err(message) => json!({"status": "error", "message": message}),
                };
            #[cfg(not(target_os = "windows"))]
            let window =
                json!({"status":"error","message":"active_window is only supported on Windows"});

            #[cfg(target_os = "windows")]
            let windows =
                match dispatch_internal(
                    runtime,
                    "list_windows",
                    &json!({}),
                    depth + 1,
                    cancel_token,
                )
                .await
                {
                    Ok(value) => {
                        let count = value.get("count").and_then(Value::as_u64).unwrap_or(0);
                        json!({"status":"success","count": count})
                    }
                    Err(message) => json!({"status":"error","message": message}),
                };
            #[cfg(not(target_os = "windows"))]
            let windows =
                json!({"status":"error","message":"list_windows is only supported on Windows"});

            Ok(json!({
                "status": "success",
                "uptime_s": runtime.started_at.elapsed().as_secs_f64(),
                "collected_in_ms": started.elapsed().as_millis(),
                "system": system,
                "input": input,
                "window": window,
                "windows": windows,
            }))
        }
        "batch_execute" => {
            let requests = parse_batch_requests(payload)?;
            let continue_on_error = payload
                .get("continue_on_error")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let include_timing = payload
                .get("include_timing")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let max_steps = payload
                .get("max_steps")
                .and_then(Value::as_u64)
                .unwrap_or(64)
                .clamp(1, 256) as usize;

            if requests.len() > max_steps {
                return Err(format!(
                    "payload.requests length {} exceeds max_steps {}",
                    requests.len(),
                    max_steps
                ));
            }

            let mut results: Vec<Value> = Vec::with_capacity(requests.len());
            let mut success_count = 0usize;
            let mut error_count = 0usize;

            for (index, (step_event, step_payload)) in requests.iter().enumerate() {
                if cancel_token.is_cancelled() {
                    return Err("Request cancelled by control plane.".to_string());
                }
                if step_event == "batch_execute" {
                    let item = json!({
                        "index": index,
                        "event": step_event,
                        "status": "error",
                        "message": "Nested batch_execute is not allowed.",
                    });
                    results.push(item);
                    error_count += 1;
                    if !continue_on_error {
                        break;
                    }
                    continue;
                }

                let step_started = Instant::now();
                match dispatch_internal(runtime, step_event, step_payload, depth + 1, cancel_token).await {
                    Ok(data) => {
                        let mut item = json!({
                            "index": index,
                            "event": step_event,
                            "status": "success",
                            "data": data,
                        });
                        if include_timing {
                            item["elapsed_ms"] = json!(step_started.elapsed().as_millis());
                        }
                        results.push(item);
                        success_count += 1;
                    }
                    Err(message) => {
                        let mut item = json!({
                            "index": index,
                            "event": step_event,
                            "status": "error",
                            "message": message,
                        });
                        if include_timing {
                            item["elapsed_ms"] = json!(step_started.elapsed().as_millis());
                        }
                        results.push(item);
                        error_count += 1;
                        if !continue_on_error {
                            break;
                        }
                    }
                }
            }

            let status = if error_count == 0 {
                "success"
            } else if success_count > 0 {
                "partial"
            } else {
                "failed"
            };

            Ok(json!({
                "status": status,
                "count": results.len(),
                "success_count": success_count,
                "error_count": error_count,
                "continue_on_error": continue_on_error,
                "results": results,
            }))
        }
        "audio_probe" => {
            let path = required_str(payload, "path")?;
            AudioUtils::probe_audio_file(path).map_err(|err| err.to_string())
        }
        "automation_plan_execute" => {
            let tasks = parse_automation_tasks(payload)?;
            let options = parse_automation_options(payload)?;
            let engine = AutomationEngine::new();
            let report = tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Err("Request cancelled by control plane.".to_string());
                }
                report = engine.execute_plan_with_options(tasks, options) => report,
            };
            serde_json::to_value(report).map_err(|err| err.to_string())
        }
        "input_snapshot" => Ok(InputAnalyzer::snapshot().await),
        "file_hash" => {
            let path = required_str(payload, "path")?;
            let hash = FileAccess::sha256_of_file(path)
                .await
                .map_err(|err| err.to_string())?;
            Ok(json!({"path": path, "sha256": hash}))
        }
        "file_read_json" => {
            let path = required_str(payload, "path")?;
            let value = FileAccess::read_json::<Value>(path)
                .await
                .map_err(|err| err.to_string())?;
            Ok(json!({"path": path, "value": value}))
        }
        "file_write_json" => {
            let path = required_str(payload, "path")?;
            let value = payload
                .get("value")
                .cloned()
                .or_else(|| payload.get("json").cloned())
                .ok_or_else(|| "payload.value is required".to_string())?;
            FileAccess::write_json(path, &value)
                .await
                .map_err(|err| err.to_string())?;
            Ok(json!({"path": path, "status": "written"}))
        }
        "file_read_text" => {
            let path = required_str(payload, "path")?;
            let max_bytes = payload
                .get("max_bytes")
                .and_then(Value::as_u64)
                .unwrap_or(128_000)
                .clamp(128, 2_000_000) as usize;
            let mut text = FileAccess::read_text(path)
                .await
                .map_err(|err| err.to_string())?;
            let truncated = text.len() > max_bytes;
            if truncated {
                text.truncate(max_bytes);
            }
            let bytes = text.len();
            Ok(json!({
                "path": path,
                "text": text,
                "bytes": bytes,
                "truncated": truncated,
            }))
        }
        "file_write_text" => {
            let path = required_str(payload, "path")?;
            let text = payload
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    payload
                        .get("data")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .ok_or_else(|| "payload.text is required".to_string())?;
            FileAccess::write_text(path, &text)
                .await
                .map_err(|err| err.to_string())?;
            Ok(json!({
                "path": path,
                "bytes": text.len(),
                "status": "written",
            }))
        }
        "active_window" => {
            #[cfg(target_os = "windows")]
            {
                WindowsControl::get_active_window().map_err(|err| err.to_string())
            }
            #[cfg(not(target_os = "windows"))]
            {
                Err("active_window is only supported on Windows".to_string())
            }
        }
        "list_windows" => {
            #[cfg(target_os = "windows")]
            {
                let rows = WindowsControl::list_windows().map_err(|err| err.to_string())?;
                Ok(json!({"items": rows, "count": rows.len()}))
            }
            #[cfg(not(target_os = "windows"))]
            {
                Err("list_windows is only supported on Windows".to_string())
            }
        }
        _ => Err(format!("Unsupported event '{event}'")),
    }
}

async fn dispatch(
    runtime: &RuntimeState,
    event: &str,
    payload: &Value,
    cancel_token: &CancellationToken,
) -> Result<Value, String> {
    dispatch_internal(runtime, event, payload, 0, cancel_token).await
}

async fn write_response(stdout: &mut io::Stdout, response: &RpcResponse) -> anyhow::Result<()> {
    let mut encoded = serde_json::to_vec(response)?;
    encoded.push(b'\n');
    stdout.write_all(&encoded).await?;
    stdout.flush().await?;
    Ok(())
}

async fn write_response_shared(
    stdout: &Arc<Mutex<io::Stdout>>,
    response: &RpcResponse,
) -> anyhow::Result<()> {
    let mut guard = stdout.lock().await;
    write_response(&mut *guard, response).await
}

async fn handle_request(
    runtime: &RuntimeState,
    request: RpcRequest,
    cancel_token: CancellationToken,
) -> RpcResponse {
    let reply_to = request_id(&request.payload);
    match dispatch(runtime, request.event.as_str(), &request.payload, &cancel_token).await {
        Ok(data) => response_ok(reply_to, request.event.as_str(), data),
        Err(message) => response_error(reply_to, request.event.as_str(), message),
    }
}

async fn run_stdio_loop(runtime: Arc<RuntimeState>) -> anyhow::Result<()> {
    let stdin = BufReader::new(io::stdin());
    let mut lines = stdin.lines();
    let stdout = Arc::new(Mutex::new(io::stdout()));

    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parsed: Result<RpcRequest, _> = serde_json::from_str(trimmed);
        let request = match parsed {
            Ok(request) => request,
            Err(err) => {
                let response = response_error(None, "invalid_request", err.to_string());
                write_response_shared(&stdout, &response).await?;
                continue;
            }
        };

        let event_name = request.event.trim().to_ascii_lowercase();
        if event_name == "cancel_request"
            || event_name == "runtime_load"
            || event_name == "runtime_policy_snapshot"
        {
            let response = handle_request(runtime.as_ref(), request, CancellationToken::new()).await;
            write_response_shared(&stdout, &response).await?;
            continue;
        }

        if runtime.should_reject_backpressure() {
            runtime
                .backpressure_rejected_total
                .fetch_add(1, Ordering::Relaxed);
            let mut response = response_error(
                request_id(&request.payload),
                request.event.as_str(),
                "Rust runtime overloaded; backpressure queue is full.".to_string(),
            );
            response.data = json!({
                "error_code": "runtime_overloaded",
                "load": runtime.runtime_load_snapshot().await,
            });
            write_response_shared(&stdout, &response).await?;
            continue;
        }

        runtime.queued_count.fetch_add(1, Ordering::Relaxed);
        let runtime_clone = Arc::clone(&runtime);
        let stdout_clone = Arc::clone(&stdout);
        tokio::spawn(async move {
            let permit = runtime_clone.limiter().acquire_owned().await;
            runtime_clone.queued_count.fetch_sub(1, Ordering::Relaxed);
            let permit = match permit {
                Ok(permit) => permit,
                Err(_) => {
                    let response = response_error(
                        request_id(&request.payload),
                        request.event.as_str(),
                        "Runtime limiter is shutting down.".to_string(),
                    );
                    let _ = write_response_shared(&stdout_clone, &response).await;
                    return;
                }
            };

            runtime_clone
                .accepted_total
                .fetch_add(1, Ordering::Relaxed);

            let request_identifier = request_id(&request.payload);
            let cancel_token = CancellationToken::new();
            if let Some(request_id_value) = request_identifier.clone() {
                runtime_clone
                    .register_inflight(request_id_value, request.event.clone(), cancel_token.clone())
                    .await;
            }

            let response = handle_request(runtime_clone.as_ref(), request, cancel_token).await;

            if let Some(request_id_value) = request_identifier {
                runtime_clone.unregister_inflight(&request_id_value).await;
            }
            runtime_clone
                .completed_total
                .fetch_add(1, Ordering::Relaxed);
            drop(permit);
            let _ = write_response_shared(&stdout_clone, &response).await;
        });
    }

    Ok(())
}

async fn run_tcp_loop(runtime: Arc<RuntimeState>) -> anyhow::Result<()> {
    let addr =
        std::env::var("JARVIS_RUST_TCP_ADDR").unwrap_or_else(|_| "127.0.0.1:7654".to_string());
    let listener = IpcBridge::listen(&addr).await?;
    eprintln!("[RUST BACKEND] TCP IPC listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        eprintln!("[RUST BACKEND] TCP client connected: {peer}");
        let mut bridge = IpcBridge::from_stream(stream);

        loop {
            let message = match bridge.receive_message().await {
                Ok(msg) => msg,
                Err(err) => {
                    eprintln!("[RUST BACKEND] TCP receive error: {err}");
                    break;
                }
            };

            let parsed = serde_json::from_value::<RpcRequest>(message);
            let response = match parsed {
                Ok(request) => {
                    let request_identifier = request_id(&request.payload);
                    let cancel_token = CancellationToken::new();
                    runtime
                        .accepted_total
                        .fetch_add(1, Ordering::Relaxed);
                    if let Some(request_id_value) = request_identifier.clone() {
                        runtime
                            .register_inflight(
                                request_id_value,
                                request.event.clone(),
                                cancel_token.clone(),
                            )
                            .await;
                    }
                    let response =
                        handle_request(runtime.as_ref(), request, cancel_token).await;
                    if let Some(request_id_value) = request_identifier {
                        runtime.unregister_inflight(&request_id_value).await;
                    }
                    runtime
                        .completed_total
                        .fetch_add(1, Ordering::Relaxed);
                    response
                }
                Err(err) => response_error(None, "invalid_request", err.to_string()),
            };

            let response_json =
                serde_json::to_value(response).map_err(|err| anyhow::anyhow!(err.to_string()))?;
            if let Err(err) = bridge.send(&response_json).await {
                eprintln!("[RUST BACKEND] TCP send error: {err}");
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    SafetyGuard::enforce_global_safety()?;
    eprintln!("[RUST BACKEND] Starting runtime...");

    bootstrap().await?;

    let runtime = Arc::new(RuntimeState::new());
    let ipc_mode = std::env::var("JARVIS_RUST_IPC_MODE").unwrap_or_else(|_| "stdio".to_string());
    if ipc_mode.trim().eq_ignore_ascii_case("tcp") {
        run_tcp_loop(Arc::clone(&runtime)).await
    } else {
        run_stdio_loop(Arc::clone(&runtime)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_id_is_extracted_from_payload() {
        let payload = json!({"request_id": "abc-123"});
        assert_eq!(request_id(&payload), Some("abc-123".to_string()));
    }

    #[tokio::test]
    async fn capabilities_event_is_supported() {
        let runtime = RuntimeState::new();
        let output = dispatch(
            &runtime,
            "capabilities",
            &json!({}),
            &CancellationToken::new(),
        )
            .await
            .expect("capabilities should succeed");
        let events = output
            .get("supported_events")
            .and_then(Value::as_array)
            .expect("supported_events should be an array");
        assert!(events
            .iter()
            .any(|item| item.as_str() == Some("health_check")));
    }

    #[tokio::test]
    async fn bridge_runtime_snapshot_event_is_supported() {
        let runtime = RuntimeState::new();
        let output = dispatch(
            &runtime,
            "bridge_runtime_snapshot",
            &json!({}),
            &CancellationToken::new(),
        )
            .await
            .expect("bridge_runtime_snapshot should succeed");
        assert_eq!(
            output.get("status").and_then(Value::as_str),
            Some("success")
        );
        assert!(output.get("core_events").and_then(Value::as_array).is_some());
    }

    #[tokio::test]
    async fn runtime_load_event_returns_capacity_snapshot() {
        let runtime = RuntimeState::new();
        let output = dispatch(
            &runtime,
            "runtime_load",
            &json!({}),
            &CancellationToken::new(),
        )
        .await
        .expect("runtime_load should succeed");
        assert!(output.get("max_concurrent").and_then(Value::as_u64).is_some());
        assert!(output.get("queued").and_then(Value::as_u64).is_some());
        assert!(output.get("policy_hint").is_some());
    }

    #[tokio::test]
    async fn runtime_policy_snapshot_event_returns_policy_payload() {
        let runtime = RuntimeState::new();
        let output = dispatch(
            &runtime,
            "runtime_policy_snapshot",
            &json!({}),
            &CancellationToken::new(),
        )
        .await
        .expect("runtime_policy_snapshot should succeed");
        assert_eq!(
            output.get("status").and_then(Value::as_str),
            Some("success")
        );
        assert!(output.get("runtime_load").is_some());
        assert!(output.get("policy_hint").is_some());
    }

    #[tokio::test]
    async fn cancel_request_for_unknown_id_returns_not_inflight() {
        let runtime = RuntimeState::new();
        let output = dispatch(
            &runtime,
            "cancel_request",
            &json!({"target_request_id": "missing-id", "reason": "test"}),
            &CancellationToken::new(),
        )
        .await
        .expect("cancel_request should return status payload");
        assert_eq!(
            output.get("cancelled").and_then(Value::as_bool),
            Some(false)
        );
    }

    #[tokio::test]
    async fn unknown_event_returns_error() {
        let runtime = RuntimeState::new();
        let error = dispatch(
            &runtime,
            "unknown_event",
            &json!({}),
            &CancellationToken::new(),
        )
            .await
            .expect_err("unknown event should fail");
        assert!(error.contains("Unsupported event"));
    }

    #[tokio::test]
    async fn automation_plan_execute_runs_tasks() {
        let runtime = RuntimeState::new();
        let output = dispatch(
            &runtime,
            "automation_plan_execute",
            &json!({
                "tasks": [
                    {"id": "t1", "depends_on": [], "simulate_ms": 10},
                    {"id": "t2", "depends_on": ["t1"], "simulate_ms": 10}
                ]
            }),
            &CancellationToken::new(),
        )
        .await
        .expect("automation plan should execute");
        assert_eq!(
            output.get("status").and_then(Value::as_str),
            Some("success")
        );
        assert_eq!(output.get("completed").and_then(Value::as_u64), Some(2));
    }

    #[tokio::test]
    async fn automation_plan_execute_requires_tasks_payload() {
        let runtime = RuntimeState::new();
        let error = dispatch(
            &runtime,
            "automation_plan_execute",
            &json!({}),
            &CancellationToken::new(),
        )
            .await
            .expect_err("automation plan without tasks should fail");
        assert!(error.contains("payload.tasks"));
    }

    #[tokio::test]
    async fn safety_status_event_returns_safe_mode() {
        let runtime = RuntimeState::new();
        let output = dispatch(
            &runtime,
            "safety_status",
            &json!({}),
            &CancellationToken::new(),
        )
            .await
            .expect("safety_status should succeed");
        assert_eq!(output.get("safe_mode").and_then(Value::as_bool), Some(true));
        assert!(output.get("platform").and_then(Value::as_str).is_some());
    }

    #[tokio::test]
    async fn file_write_and_read_json_roundtrip() {
        let runtime = RuntimeState::new();
        let path = std::env::temp_dir().join(format!("jarvis-rust-{}.json", uuid::Uuid::new_v4()));
        let path_str = path.to_string_lossy().to_string();

        let written = dispatch(
            &runtime,
            "file_write_json",
            &json!({
                "path": path_str,
                "value": {"hello": "world", "count": 2}
            }),
            &CancellationToken::new(),
        )
        .await
        .expect("file_write_json should succeed");
        assert_eq!(
            written.get("status").and_then(Value::as_str),
            Some("written")
        );

        let read_back = dispatch(
            &runtime,
            "file_read_json",
            &json!({
                "path": path.to_string_lossy().to_string()
            }),
            &CancellationToken::new(),
        )
        .await
        .expect("file_read_json should succeed");
        assert_eq!(
            read_back
                .get("value")
                .and_then(|value| value.get("hello"))
                .and_then(Value::as_str),
            Some("world")
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn desktop_context_event_includes_core_sections() {
        let runtime = RuntimeState::new();
        let output = dispatch(
            &runtime,
            "desktop_context",
            &json!({}),
            &CancellationToken::new(),
        )
            .await
            .expect("desktop_context should succeed");

        assert_eq!(
            output.get("status").and_then(Value::as_str),
            Some("success")
        );
        assert!(output.get("system").is_some());
        assert!(output.get("input").is_some());
        assert!(output.get("window").is_some());
        assert!(output.get("windows").is_some());
    }

    #[tokio::test]
    async fn batch_execute_can_continue_on_error() {
        let runtime = RuntimeState::new();
        let output = dispatch(
            &runtime,
            "batch_execute",
            &json!({
                "continue_on_error": true,
                "requests": [
                    {"event": "echo", "payload": {"value": 1}},
                    {"event": "unsupported_event", "payload": {}},
                    {"event": "safety_status", "payload": {}}
                ]
            }),
            &CancellationToken::new(),
        )
        .await
        .expect("batch_execute should return report");

        assert_eq!(
            output.get("status").and_then(Value::as_str),
            Some("partial")
        );
        assert_eq!(output.get("count").and_then(Value::as_u64), Some(3));
        assert_eq!(output.get("success_count").and_then(Value::as_u64), Some(2));
        assert_eq!(output.get("error_count").and_then(Value::as_u64), Some(1));
    }

    #[tokio::test]
    async fn batch_execute_rejects_nested_batch() {
        let runtime = RuntimeState::new();
        let output = dispatch(
            &runtime,
            "batch_execute",
            &json!({
                "continue_on_error": false,
                "requests": [
                    {"event": "batch_execute", "payload": {"requests":[{"event":"echo","payload":{}}]}}
                ]
            }),
            &CancellationToken::new(),
        )
        .await
        .expect("batch_execute should return report");

        assert_eq!(output.get("status").and_then(Value::as_str), Some("failed"));
        assert_eq!(output.get("error_count").and_then(Value::as_u64), Some(1));
    }
}
