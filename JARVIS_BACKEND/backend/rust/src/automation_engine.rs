use std::collections::{HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskState {
    Pending,
    Running,
    Completed,
    Failed(String),
    Skipped(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationTaskSpec {
    pub id: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub payload: Value,
    #[serde(default = "default_simulate_ms")]
    pub simulate_ms: u64,
    #[serde(default)]
    pub should_fail: bool,
    #[serde(default)]
    pub fail_message: String,
    #[serde(default)]
    pub max_retries: Option<u32>,
    #[serde(default)]
    pub retry_backoff_ms: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub priority: i32,
}

fn default_simulate_ms() -> u64 {
    150
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAttemptRecord {
    pub attempt: u32,
    pub status: String,
    pub message: String,
    pub elapsed_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationTaskResult {
    pub id: String,
    pub state: TaskState,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub attempts: Vec<TaskAttemptRecord>,
    pub retry_count: u32,
    pub elapsed_ms: u128,
    pub started_at_ms: u128,
    pub finished_at_ms: u128,
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationExecutionOptions {
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: usize,
    #[serde(default)]
    pub fail_fast: bool,
    #[serde(default = "default_default_max_retries")]
    pub default_max_retries: u32,
    #[serde(default = "default_default_retry_backoff_ms")]
    pub default_retry_backoff_ms: u64,
    #[serde(default = "default_default_timeout_ms")]
    pub default_timeout_ms: u64,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

fn default_max_concurrency() -> usize {
    3
}

fn default_default_max_retries() -> u32 {
    1
}

fn default_default_retry_backoff_ms() -> u64 {
    120
}

fn default_default_timeout_ms() -> u64 {
    4_000
}

fn default_backoff_multiplier() -> f64 {
    1.8
}

fn default_max_backoff_ms() -> u64 {
    5_000
}

impl Default for AutomationExecutionOptions {
    fn default() -> Self {
        Self {
            max_concurrency: default_max_concurrency(),
            fail_fast: false,
            default_max_retries: default_default_max_retries(),
            default_retry_backoff_ms: default_default_retry_backoff_ms(),
            default_timeout_ms: default_default_timeout_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            max_backoff_ms: default_max_backoff_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutomationExecutionReport {
    pub status: String,
    pub total: usize,
    pub completed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub pending: usize,
    #[serde(default)]
    pub execution_order: Vec<String>,
    #[serde(default)]
    pub errors: Vec<String>,
    #[serde(default)]
    pub results: Vec<AutomationTaskResult>,
    pub duration_ms: u128,
    pub throughput_tasks_per_s: f64,
    pub fail_fast_triggered: bool,
    pub options: AutomationExecutionOptions,
}

#[derive(Debug, Clone)]
struct TaskExecutionEnvelope {
    id: String,
    state: TaskState,
    attempts: Vec<TaskAttemptRecord>,
    retry_count: u32,
    elapsed_ms: u128,
    started_at_ms: u128,
    finished_at_ms: u128,
}

impl TaskExecutionEnvelope {
    fn skipped(id: &str, reason: String, started_at_ms: u128) -> Self {
        Self {
            id: id.to_string(),
            state: TaskState::Skipped(reason),
            attempts: Vec::new(),
            retry_count: 0,
            elapsed_ms: 0,
            started_at_ms,
            finished_at_ms: started_at_ms,
        }
    }
}

fn pop_highest_priority_ready(
    ready: &mut Vec<String>,
    specs: &HashMap<String, AutomationTaskSpec>,
) -> Option<String> {
    let mut best_index: Option<usize> = None;
    for (index, id) in ready.iter().enumerate() {
        let priority = specs.get(id).map(|item| item.priority).unwrap_or(0);
        if let Some(current) = best_index {
            let current_id = &ready[current];
            let current_priority = specs.get(current_id).map(|item| item.priority).unwrap_or(0);
            if priority > current_priority
                || (priority == current_priority && id.as_str() < current_id.as_str())
            {
                best_index = Some(index);
            }
        } else {
            best_index = Some(index);
        }
    }
    best_index.map(|index| ready.swap_remove(index))
}

fn unlock_dependents(
    task_id: &str,
    dependents: &HashMap<String, Vec<String>>,
    remaining_dependencies: &mut HashMap<String, usize>,
) -> Vec<String> {
    let mut unlocked = Vec::new();
    if let Some(children) = dependents.get(task_id) {
        for child in children {
            if let Some(remaining) = remaining_dependencies.get_mut(child) {
                if *remaining > 0 {
                    *remaining -= 1;
                }
                if *remaining == 0 {
                    unlocked.push(child.clone());
                }
            }
        }
    }
    unlocked
}

fn is_failure(state: &TaskState) -> bool {
    matches!(state, TaskState::Failed(_))
}

fn effective_retry_count(spec: &AutomationTaskSpec, options: &AutomationExecutionOptions) -> u32 {
    spec.max_retries
        .unwrap_or(options.default_max_retries)
        .min(12)
}

fn effective_backoff_ms(spec: &AutomationTaskSpec, options: &AutomationExecutionOptions) -> u64 {
    spec.retry_backoff_ms
        .unwrap_or(options.default_retry_backoff_ms)
        .min(60_000)
}

fn effective_timeout_ms(spec: &AutomationTaskSpec, options: &AutomationExecutionOptions) -> u64 {
    spec.timeout_ms
        .unwrap_or(options.default_timeout_ms)
        .min(180_000)
}

fn failure_message(spec: &AutomationTaskSpec, default_value: &str) -> String {
    let configured = spec.fail_message.trim();
    if !configured.is_empty() {
        return configured.to_string();
    }
    if let Some(from_payload) = spec.payload.get("fail_message").and_then(Value::as_str) {
        let value = from_payload.trim();
        if !value.is_empty() {
            return value.to_string();
        }
    }
    default_value.to_string()
}

async fn execute_task(
    spec: AutomationTaskSpec,
    options: AutomationExecutionOptions,
    started_at_ms: u128,
) -> TaskExecutionEnvelope {
    let max_retries = effective_retry_count(&spec, &options);
    let timeout_ms = effective_timeout_ms(&spec, &options);
    let base_backoff_ms = effective_backoff_ms(&spec, &options);
    let backoff_multiplier = options.backoff_multiplier.clamp(1.0, 5.0);
    let max_backoff_ms = options.max_backoff_ms.clamp(0, 60_000);
    let fail_attempts = spec
        .payload
        .get("fail_attempts")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .min(32) as u32;

    let started = Instant::now();
    let mut attempts = Vec::new();
    let mut final_state = TaskState::Completed;

    for attempt in 1..=(max_retries + 1) {
        let attempt_started = Instant::now();
        let simulated_ms = spec.simulate_ms.clamp(5, 120_000);
        let delay = Duration::from_millis(simulated_ms);
        let timed_out = if timeout_ms > 0 {
            tokio::time::timeout(Duration::from_millis(timeout_ms), tokio::time::sleep(delay))
                .await
                .is_err()
        } else {
            tokio::time::sleep(delay).await;
            false
        };

        if timed_out {
            let message = format!(
                "Task timed out at attempt {attempt} after {}ms (timeout={}ms).",
                simulated_ms, timeout_ms
            );
            attempts.push(TaskAttemptRecord {
                attempt,
                status: "timeout".to_string(),
                message: message.clone(),
                elapsed_ms: attempt_started.elapsed().as_millis(),
            });
            final_state = TaskState::Failed(message);
        } else {
            let should_fail = spec.should_fail || attempt <= fail_attempts;
            if should_fail {
                let message = failure_message(
                    &spec,
                    &format!("Task '{}' failed at attempt {}.", spec.id, attempt),
                );
                attempts.push(TaskAttemptRecord {
                    attempt,
                    status: "failed".to_string(),
                    message: message.clone(),
                    elapsed_ms: attempt_started.elapsed().as_millis(),
                });
                final_state = TaskState::Failed(message);
            } else {
                attempts.push(TaskAttemptRecord {
                    attempt,
                    status: "success".to_string(),
                    message: "Task completed.".to_string(),
                    elapsed_ms: attempt_started.elapsed().as_millis(),
                });
                final_state = TaskState::Completed;
                break;
            }
        }

        if attempt <= max_retries {
            let exponent = (attempt - 1) as i32;
            let scaled = (base_backoff_ms as f64 * backoff_multiplier.powi(exponent)).round();
            let backoff_ms = scaled.clamp(0.0, max_backoff_ms as f64) as u64;
            if backoff_ms > 0 {
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }

    let elapsed_ms = started.elapsed().as_millis();
    let retry_count = attempts.len().saturating_sub(1) as u32;
    TaskExecutionEnvelope {
        id: spec.id,
        state: final_state,
        attempts,
        retry_count,
        elapsed_ms,
        started_at_ms,
        finished_at_ms: started_at_ms + elapsed_ms,
    }
}

pub struct AutomationEngine;

impl AutomationEngine {
    pub fn new() -> Self {
        Self
    }

    pub async fn execute_plan(&self, tasks: Vec<AutomationTaskSpec>) -> AutomationExecutionReport {
        self.execute_plan_with_options(tasks, AutomationExecutionOptions::default())
            .await
    }

    pub async fn execute_plan_with_options(
        &self,
        tasks: Vec<AutomationTaskSpec>,
        options: AutomationExecutionOptions,
    ) -> AutomationExecutionReport {
        let started = Instant::now();
        let bounded_options = AutomationExecutionOptions {
            max_concurrency: options.max_concurrency.clamp(1, 32),
            fail_fast: options.fail_fast,
            default_max_retries: options.default_max_retries.min(12),
            default_retry_backoff_ms: options.default_retry_backoff_ms.min(60_000),
            default_timeout_ms: options.default_timeout_ms.min(180_000),
            backoff_multiplier: options.backoff_multiplier.clamp(1.0, 5.0),
            max_backoff_ms: options.max_backoff_ms.min(60_000),
        };

        let total = tasks.len();
        if tasks.is_empty() {
            return AutomationExecutionReport {
                status: "success".to_string(),
                total: 0,
                completed: 0,
                failed: 0,
                skipped: 0,
                pending: 0,
                execution_order: Vec::new(),
                errors: Vec::new(),
                results: Vec::new(),
                duration_ms: 0,
                throughput_tasks_per_s: 0.0,
                fail_fast_triggered: false,
                options: bounded_options,
            };
        }

        let mut errors: Vec<String> = Vec::new();
        let mut specs: HashMap<String, AutomationTaskSpec> = HashMap::new();
        let mut order: Vec<String> = Vec::new();

        for task in tasks {
            let id = task.id.trim().to_string();
            if id.is_empty() {
                errors.push("Task id cannot be empty.".to_string());
                continue;
            }
            if specs.contains_key(&id) {
                errors.push(format!("Duplicate task id '{id}'."));
                continue;
            }

            let depends_on = task
                .depends_on
                .into_iter()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();

            order.push(id.clone());
            specs.insert(
                id.clone(),
                AutomationTaskSpec {
                    id,
                    depends_on,
                    payload: task.payload,
                    simulate_ms: task.simulate_ms,
                    should_fail: task.should_fail,
                    fail_message: task.fail_message,
                    max_retries: task.max_retries,
                    retry_backoff_ms: task.retry_backoff_ms,
                    timeout_ms: task.timeout_ms,
                    priority: task.priority,
                },
            );
        }

        let mut remaining_dependencies: HashMap<String, usize> = HashMap::new();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
        for id in &order {
            remaining_dependencies.insert(id.clone(), 0);
            dependents.insert(id.clone(), Vec::new());
        }

        for id in &order {
            if let Some(task) = specs.get(id) {
                for dependency in &task.depends_on {
                    if !specs.contains_key(dependency) {
                        errors.push(format!(
                            "Task '{}' depends on missing task '{}'.",
                            id, dependency
                        ));
                        continue;
                    }
                    if dependency == id {
                        errors.push(format!("Task '{}' cannot depend on itself.", id));
                        continue;
                    }
                    let entry = remaining_dependencies.entry(id.clone()).or_insert(0);
                    *entry += 1;
                    if let Some(rows) = dependents.get_mut(dependency) {
                        rows.push(id.clone());
                    }
                }
            }
        }

        if !errors.is_empty() {
            let results = order
                .iter()
                .filter_map(|id| specs.get(id))
                .map(|task| AutomationTaskResult {
                    id: task.id.clone(),
                    state: TaskState::Pending,
                    depends_on: task.depends_on.clone(),
                    payload: task.payload.clone(),
                    attempts: Vec::new(),
                    retry_count: 0,
                    elapsed_ms: 0,
                    started_at_ms: 0,
                    finished_at_ms: 0,
                    priority: task.priority,
                })
                .collect::<Vec<_>>();
            return AutomationExecutionReport {
                status: "invalid_plan".to_string(),
                total,
                completed: 0,
                failed: 0,
                skipped: 0,
                pending: results.len(),
                execution_order: Vec::new(),
                errors,
                results,
                duration_ms: started.elapsed().as_millis(),
                throughput_tasks_per_s: 0.0,
                fail_fast_triggered: false,
                options: bounded_options,
            };
        }

        let mut ready: Vec<String> = Vec::new();
        let mut ready_set: HashSet<String> = HashSet::new();
        for id in &order {
            if remaining_dependencies.get(id).copied().unwrap_or(0) == 0 {
                ready.push(id.clone());
                ready_set.insert(id.clone());
            }
        }

        let mut join_set: JoinSet<TaskExecutionEnvelope> = JoinSet::new();
        let mut running_ids: HashSet<String> = HashSet::new();
        let mut final_results: HashMap<String, TaskExecutionEnvelope> = HashMap::new();
        let mut execution_order: Vec<String> = Vec::new();
        let mut failed_any = false;
        let mut started_slots = 0u64;

        while !ready.is_empty() || !running_ids.is_empty() {
            while running_ids.len() < bounded_options.max_concurrency
                && !ready.is_empty()
                && !(bounded_options.fail_fast && failed_any)
            {
                let Some(next_id) = pop_highest_priority_ready(&mut ready, &specs) else {
                    break;
                };
                ready_set.remove(&next_id);
                if running_ids.contains(&next_id) || final_results.contains_key(&next_id) {
                    continue;
                }

                let Some(task) = specs.get(&next_id).cloned() else {
                    continue;
                };
                let started_at_ms = started.elapsed().as_millis();
                let options_clone = bounded_options.clone();
                running_ids.insert(next_id.clone());
                started_slots += 1;
                let _ = started_slots; // explicit counter kept for diagnostics compatibility
                join_set
                    .spawn(async move { execute_task(task, options_clone, started_at_ms).await });
            }

            if running_ids.is_empty() {
                break;
            }

            let Some(joined) = join_set.join_next().await else {
                break;
            };
            let message = match joined {
                Ok(item) => item,
                Err(join_error) => {
                    errors.push(format!("Task join error: {join_error}"));
                    continue;
                }
            };

            running_ids.remove(&message.id);
            if is_failure(&message.state) {
                failed_any = true;
            }
            let completed_id = message.id.clone();
            execution_order.push(completed_id.clone());
            final_results.insert(completed_id.clone(), message);

            let mut evaluation_queue: VecDeque<String> =
                unlock_dependents(&completed_id, &dependents, &mut remaining_dependencies).into();

            while let Some(candidate_id) = evaluation_queue.pop_front() {
                if final_results.contains_key(&candidate_id)
                    || running_ids.contains(&candidate_id)
                    || ready_set.contains(&candidate_id)
                {
                    continue;
                }

                let Some(candidate_spec) = specs.get(&candidate_id) else {
                    continue;
                };
                let dependencies_ok = candidate_spec.depends_on.iter().all(|dependency| {
                    matches!(
                        final_results.get(dependency).map(|item| &item.state),
                        Some(TaskState::Completed)
                    )
                });

                if dependencies_ok && !(bounded_options.fail_fast && failed_any) {
                    ready.push(candidate_id.clone());
                    ready_set.insert(candidate_id);
                    continue;
                }

                let reason = if bounded_options.fail_fast && failed_any {
                    "Skipped due to fail_fast after previous failure.".to_string()
                } else {
                    "Dependency did not complete successfully.".to_string()
                };
                let skipped = TaskExecutionEnvelope::skipped(
                    &candidate_spec.id,
                    reason,
                    started.elapsed().as_millis(),
                );
                execution_order.push(candidate_spec.id.clone());
                final_results.insert(candidate_spec.id.clone(), skipped);
                let unlocked =
                    unlock_dependents(&candidate_spec.id, &dependents, &mut remaining_dependencies);
                for item in unlocked {
                    evaluation_queue.push_back(item);
                }
            }
        }

        let mut fail_fast_triggered = false;
        if bounded_options.fail_fast && failed_any {
            fail_fast_triggered = true;
            for id in &order {
                if !final_results.contains_key(id) {
                    let skipped = TaskExecutionEnvelope::skipped(
                        id,
                        "Skipped because fail_fast was triggered.".to_string(),
                        started.elapsed().as_millis(),
                    );
                    execution_order.push(id.clone());
                    final_results.insert(id.clone(), skipped);
                }
            }
        }

        if final_results.len() < order.len() {
            errors.push("Dependency cycle detected in automation plan.".to_string());
            for id in &order {
                if !final_results.contains_key(id) {
                    final_results.insert(
                        id.clone(),
                        TaskExecutionEnvelope {
                            id: id.clone(),
                            state: TaskState::Pending,
                            attempts: Vec::new(),
                            retry_count: 0,
                            elapsed_ms: 0,
                            started_at_ms: 0,
                            finished_at_ms: 0,
                        },
                    );
                }
            }
        }

        let mut completed = 0usize;
        let mut failed = 0usize;
        let mut skipped = 0usize;
        let mut pending = 0usize;
        let results = order
            .iter()
            .filter_map(|id| specs.get(id).map(|task| (id, task)))
            .map(|(id, task)| {
                let envelope = final_results
                    .get(id)
                    .cloned()
                    .unwrap_or(TaskExecutionEnvelope {
                        id: id.clone(),
                        state: TaskState::Pending,
                        attempts: Vec::new(),
                        retry_count: 0,
                        elapsed_ms: 0,
                        started_at_ms: 0,
                        finished_at_ms: 0,
                    });
                match &envelope.state {
                    TaskState::Completed => completed += 1,
                    TaskState::Failed(_) => failed += 1,
                    TaskState::Skipped(_) => skipped += 1,
                    TaskState::Pending | TaskState::Running => pending += 1,
                }
                AutomationTaskResult {
                    id: task.id.clone(),
                    state: envelope.state,
                    depends_on: task.depends_on.clone(),
                    payload: task.payload.clone(),
                    attempts: envelope.attempts,
                    retry_count: envelope.retry_count,
                    elapsed_ms: envelope.elapsed_ms,
                    started_at_ms: envelope.started_at_ms,
                    finished_at_ms: envelope.finished_at_ms,
                    priority: task.priority,
                }
            })
            .collect::<Vec<_>>();

        let status = if !errors.is_empty() || failed > 0 {
            if completed > 0 {
                "partial"
            } else {
                "failed"
            }
        } else {
            "success"
        };

        let duration_ms = started.elapsed().as_millis();
        let throughput_tasks_per_s = if duration_ms > 0 {
            (completed as f64) / (duration_ms as f64 / 1000.0)
        } else {
            completed as f64
        };

        AutomationExecutionReport {
            status: status.to_string(),
            total,
            completed,
            failed,
            skipped,
            pending,
            execution_order,
            errors,
            results,
            duration_ms,
            throughput_tasks_per_s,
            fail_fast_triggered,
            options: bounded_options,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn execute_plan_completes_dependencies() {
        let engine = AutomationEngine::new();
        let report = engine
            .execute_plan(vec![
                AutomationTaskSpec {
                    id: "a".to_string(),
                    depends_on: vec![],
                    payload: json!({"name": "first"}),
                    simulate_ms: 10,
                    should_fail: false,
                    fail_message: String::new(),
                    max_retries: None,
                    retry_backoff_ms: None,
                    timeout_ms: None,
                    priority: 0,
                },
                AutomationTaskSpec {
                    id: "b".to_string(),
                    depends_on: vec!["a".to_string()],
                    payload: json!({"name": "second"}),
                    simulate_ms: 10,
                    should_fail: false,
                    fail_message: String::new(),
                    max_retries: None,
                    retry_backoff_ms: None,
                    timeout_ms: None,
                    priority: 0,
                },
            ])
            .await;

        assert_eq!(report.status, "success");
        assert_eq!(report.completed, 2);
        assert_eq!(report.failed, 0);
    }

    #[tokio::test]
    async fn execute_plan_detects_invalid_dependencies() {
        let engine = AutomationEngine::new();
        let report = engine
            .execute_plan(vec![AutomationTaskSpec {
                id: "a".to_string(),
                depends_on: vec!["missing".to_string()],
                payload: json!({}),
                simulate_ms: 10,
                should_fail: false,
                fail_message: String::new(),
                max_retries: None,
                retry_backoff_ms: None,
                timeout_ms: None,
                priority: 0,
            }])
            .await;

        assert_eq!(report.status, "invalid_plan");
        assert!(!report.errors.is_empty());
    }

    #[tokio::test]
    async fn execute_plan_retries_until_success() {
        let engine = AutomationEngine::new();
        let report = engine
            .execute_plan_with_options(
                vec![AutomationTaskSpec {
                    id: "retry_task".to_string(),
                    depends_on: vec![],
                    payload: json!({"fail_attempts": 2}),
                    simulate_ms: 10,
                    should_fail: false,
                    fail_message: String::new(),
                    max_retries: Some(3),
                    retry_backoff_ms: Some(5),
                    timeout_ms: Some(200),
                    priority: 0,
                }],
                AutomationExecutionOptions {
                    max_concurrency: 1,
                    fail_fast: false,
                    default_max_retries: 0,
                    default_retry_backoff_ms: 1,
                    default_timeout_ms: 1000,
                    backoff_multiplier: 1.1,
                    max_backoff_ms: 100,
                },
            )
            .await;

        assert_eq!(report.status, "success");
        assert_eq!(report.completed, 1);
        let row = &report.results[0];
        assert_eq!(row.retry_count, 2);
        assert_eq!(row.attempts.len(), 3);
    }

    #[tokio::test]
    async fn execute_plan_fail_fast_skips_remaining_tasks() {
        let engine = AutomationEngine::new();
        let report = engine
            .execute_plan_with_options(
                vec![
                    AutomationTaskSpec {
                        id: "a".to_string(),
                        depends_on: vec![],
                        payload: json!({}),
                        simulate_ms: 10,
                        should_fail: true,
                        fail_message: "forced".to_string(),
                        max_retries: Some(0),
                        retry_backoff_ms: Some(1),
                        timeout_ms: Some(100),
                        priority: 5,
                    },
                    AutomationTaskSpec {
                        id: "b".to_string(),
                        depends_on: vec![],
                        payload: json!({}),
                        simulate_ms: 10,
                        should_fail: false,
                        fail_message: String::new(),
                        max_retries: Some(0),
                        retry_backoff_ms: Some(1),
                        timeout_ms: Some(100),
                        priority: 1,
                    },
                ],
                AutomationExecutionOptions {
                    max_concurrency: 1,
                    fail_fast: true,
                    default_max_retries: 0,
                    default_retry_backoff_ms: 1,
                    default_timeout_ms: 100,
                    backoff_multiplier: 1.2,
                    max_backoff_ms: 10,
                },
            )
            .await;

        assert!(report.fail_fast_triggered);
        assert_eq!(report.failed, 1);
        assert_eq!(report.skipped, 1);
    }
}
