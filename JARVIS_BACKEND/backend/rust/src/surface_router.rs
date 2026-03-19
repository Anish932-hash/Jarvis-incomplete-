use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[cfg(target_os = "windows")]
use crate::windows_control::WindowsControl;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SurfaceExplorationSelectionRow {
    #[serde(default)]
    pub selection_key: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub candidate_id: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub selected_action: String,
    #[serde(default)]
    pub confidence: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SurfaceExplorationBranchEntry {
    #[serde(default)]
    pub transition_kind: String,
    #[serde(default)]
    pub selected_action: String,
    #[serde(default)]
    pub selected_candidate_id: String,
    #[serde(default)]
    pub selected_candidate_label: String,
    #[serde(default)]
    pub window_title: String,
    #[serde(default)]
    pub surface_path_tail: Vec<String>,
    #[serde(default)]
    pub topology_branch_family_signature: String,
    #[serde(default)]
    pub occurrences: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SurfaceExplorationRouterInput {
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub surface_mode: String,
    #[serde(default)]
    pub current_window_title: String,
    #[serde(default)]
    pub current_window_app_name: String,
    #[serde(default)]
    pub current_surface_path: Vec<String>,
    #[serde(default)]
    pub current_dialog_visible: bool,
    #[serde(default)]
    pub current_reacquired_title: String,
    #[serde(default)]
    pub current_reacquired_app_name: String,
    #[serde(default)]
    pub current_reacquired_hwnd: u64,
    #[serde(default)]
    pub native_same_process_window_count: u32,
    #[serde(default)]
    pub native_related_window_count: u32,
    #[serde(default)]
    pub native_owner_link_count: u32,
    #[serde(default)]
    pub native_owner_chain_visible: bool,
    #[serde(default)]
    pub native_same_root_owner_window_count: u32,
    #[serde(default)]
    pub native_same_root_owner_dialog_like_count: u32,
    #[serde(default)]
    pub native_direct_child_window_count: u32,
    #[serde(default)]
    pub native_direct_child_dialog_like_count: u32,
    #[serde(default)]
    pub native_active_owner_chain_depth: u32,
    #[serde(default)]
    pub native_max_owner_chain_depth: u32,
    #[serde(default)]
    pub native_descendant_chain_depth: u32,
    #[serde(default)]
    pub native_descendant_dialog_chain_depth: u32,
    #[serde(default)]
    pub native_descendant_query_match_count: u32,
    #[serde(default)]
    pub native_descendant_adoption_available: bool,
    #[serde(default)]
    pub native_descendant_adoption_match_score: f64,
    #[serde(default)]
    pub native_descendant_chain_titles: Vec<String>,
    #[serde(default)]
    pub preferred_descendant_title: String,
    #[serde(default)]
    pub preferred_descendant_hwnd: u64,
    #[serde(default)]
    pub native_child_dialog_like_visible: bool,
    #[serde(default)]
    pub native_topology_signature: String,
    #[serde(default)]
    pub native_modal_chain_signature: String,
    #[serde(default)]
    pub native_child_chain_signature: String,
    #[serde(default)]
    pub native_branch_family_signature: String,
    #[serde(default)]
    pub branch_family_repeat_count: u32,
    #[serde(default)]
    pub branch_family_switch_count: u32,
    #[serde(default)]
    pub branch_family_continuity: bool,
    #[serde(default)]
    pub branch_cascade_count: u32,
    #[serde(default)]
    pub branch_cascade_kind_count: u32,
    #[serde(default)]
    pub branch_cascade_signature: String,
    #[serde(default)]
    pub benchmark_ready: bool,
    #[serde(default)]
    pub benchmark_focus_summary: Vec<String>,
    #[serde(default)]
    pub benchmark_weakest_pack: String,
    #[serde(default)]
    pub benchmark_weakest_capability: String,
    #[serde(default)]
    pub benchmark_dialog_pressure: f64,
    #[serde(default)]
    pub benchmark_descendant_focus_pressure: f64,
    #[serde(default)]
    pub benchmark_navigation_pressure: f64,
    #[serde(default)]
    pub benchmark_reacquire_pressure: f64,
    #[serde(default)]
    pub benchmark_loop_guard_pressure: f64,
    #[serde(default)]
    pub benchmark_native_focus_pressure: f64,
    #[serde(default)]
    pub benchmark_target_app_name: String,
    #[serde(default)]
    pub benchmark_target_app_matched: bool,
    #[serde(default)]
    pub benchmark_target_app_match_score: f64,
    #[serde(default)]
    pub benchmark_target_query_hints: Vec<String>,
    #[serde(default)]
    pub benchmark_target_descendant_title_hints: Vec<String>,
    #[serde(default)]
    pub benchmark_target_descendant_hint_query: String,
    #[serde(default)]
    pub benchmark_target_preferred_window_title: String,
    #[serde(default)]
    pub benchmark_target_hint_query: String,
    #[serde(default)]
    pub benchmark_target_priority: f64,
    #[serde(default)]
    pub benchmark_target_max_horizon_steps: u32,
    #[serde(default)]
    pub benchmark_target_replay_pressure: f64,
    #[serde(default)]
    pub benchmark_target_replay_session_count: u32,
    #[serde(default)]
    pub benchmark_target_replay_pending_count: u32,
    #[serde(default)]
    pub benchmark_target_replay_failed_count: u32,
    #[serde(default)]
    pub benchmark_target_replay_completed_count: u32,
    #[serde(default)]
    pub benchmark_target_campaign_count: u32,
    #[serde(default)]
    pub benchmark_target_campaign_sweep_count: u32,
    #[serde(default)]
    pub benchmark_target_campaign_pending_session_count: u32,
    #[serde(default)]
    pub benchmark_target_campaign_attention_session_count: u32,
    #[serde(default)]
    pub benchmark_target_campaign_pending_app_target_count: u32,
    #[serde(default)]
    pub benchmark_target_campaign_regression_cycle_count: u32,
    #[serde(default)]
    pub benchmark_target_campaign_long_horizon_pending_count: u32,
    #[serde(default)]
    pub benchmark_target_campaign_pressure: f64,
    #[serde(default)]
    pub benchmark_target_campaign_hint_query: String,
    #[serde(default)]
    pub benchmark_target_campaign_descendant_title_hints: Vec<String>,
    #[serde(default)]
    pub benchmark_target_campaign_descendant_hint_query: String,
    #[serde(default)]
    pub benchmark_target_campaign_preferred_window_title: String,
    #[serde(default)]
    pub benchmark_target_campaign_latest_sweep_status: String,
    #[serde(default)]
    pub benchmark_target_campaign_latest_sweep_regression_status: String,
    #[serde(default)]
    pub benchmark_target_session_cycle_count: u32,
    #[serde(default)]
    pub benchmark_target_regression_cycle_count: u32,
    #[serde(default)]
    pub benchmark_target_long_horizon_pending_count: u32,
    #[serde(default)]
    pub benchmark_target_dialog_pressure: f64,
    #[serde(default)]
    pub benchmark_target_descendant_focus_pressure: f64,
    #[serde(default)]
    pub benchmark_target_navigation_pressure: f64,
    #[serde(default)]
    pub benchmark_target_reacquire_pressure: f64,
    #[serde(default)]
    pub benchmark_target_loop_guard_pressure: f64,
    #[serde(default)]
    pub benchmark_target_native_focus_pressure: f64,
    #[serde(default)]
    pub selection_rows: Vec<SurfaceExplorationSelectionRow>,
    #[serde(default)]
    pub branch_history: Vec<SurfaceExplorationBranchEntry>,
}

fn normalize_text(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_ascii_lowercase()
}

fn tokenize(value: &str) -> Vec<String> {
    let mut normalized = normalize_text(value);
    for token in [
        "|", "-", "_", "/", "\\", "(", ")", "[", "]", "{", "}", ".", ",", ":",
    ] {
        normalized = normalized.replace(token, " ");
    }
    normalized
        .split_whitespace()
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

fn token_overlap(left: &[String], right: &[String]) -> usize {
    left.iter()
        .filter(|token| !token.is_empty() && right.iter().any(|candidate| candidate == *token))
        .count()
}

fn window_titles_from_topology(topology: &Value) -> Vec<String> {
    topology
        .get("window_title_tail")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn active_window_from_topology(topology: &Value) -> Value {
    topology
        .get("active_window")
        .cloned()
        .filter(|value| value.is_object())
        .unwrap_or_else(|| json!({}))
}

fn topology_dialog_visible(topology: &Value) -> bool {
    active_window_from_topology(topology)
        .get("surface_hints")
        .and_then(|value| value.get("dialog_like"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || topology
            .get("dialog_like_count")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            > 0
}

fn build_topology_signature(
    rows: &[String],
    active_title: &str,
    visible_count: usize,
    dialog_count: usize,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(active_title.as_bytes());
    hasher.update(b"|");
    hasher.update(visible_count.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(dialog_count.to_string().as_bytes());
    for title in rows.iter().take(6) {
        hasher.update(b"|");
        hasher.update(title.as_bytes());
    }
    let digest = hasher.finalize();
    format!("{:x}", digest)[..16].to_string()
}

fn branch_family_signature_from_modal(signature: &str) -> String {
    let parts = signature
        .split('|')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() < 2 {
        return String::new();
    }
    let mut stable = vec![parts[0].to_string(), parts[1].to_string()];
    stable.extend(parts.iter().skip(3).map(|part| part.to_string()));
    stable.join("|")
}

#[cfg(target_os = "windows")]
pub fn window_topology_snapshot(query: &str) -> anyhow::Result<Value> {
    let clean_query = query.trim();
    let query_tokens = tokenize(clean_query);
    let active_window = WindowsControl::get_active_window().unwrap_or_else(|_| json!({}));
    let windows = WindowsControl::list_windows().unwrap_or_default();
    let active_pid = active_window
        .get("process_id")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let active_hwnd = active_window
        .get("hwnd")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let active_root_owner_hwnd = active_window
        .get("root_owner_hwnd")
        .and_then(Value::as_u64)
        .unwrap_or(active_hwnd);
    let active_owner_chain_depth = active_window
        .get("owner_chain_depth")
        .and_then(Value::as_u64)
        .unwrap_or(0) as u32;
    let active_title = active_window
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("");

    let mut dialog_like_count = 0usize;
    let mut same_process_window_count = 0usize;
    let mut query_window_match_count = 0usize;
    let mut owner_link_count = 0usize;
    let mut same_root_owner_window_count = 0usize;
    let mut same_root_owner_dialog_like_count = 0usize;
    let mut max_owner_chain_depth = active_owner_chain_depth as usize;
    let mut window_title_tail: Vec<String> = Vec::new();
    let mut class_name_tail: Vec<String> = Vec::new();
    let mut owner_title_tail: Vec<String> = Vec::new();
    let mut sample_windows: Vec<Value> = Vec::new();

    for row in &windows {
        if row
            .get("surface_hints")
            .and_then(|value| value.get("dialog_like"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            dialog_like_count += 1;
        }
        if active_pid > 0
            && row.get("process_id").and_then(Value::as_u64).unwrap_or(0) == active_pid
        {
            same_process_window_count += 1;
        }
        let row_owner_hwnd = row.get("owner_hwnd").and_then(Value::as_u64).unwrap_or(0);
        if row_owner_hwnd > 0 {
            owner_link_count += 1;
        }
        let row_root_owner_hwnd = row
            .get("root_owner_hwnd")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| row.get("hwnd").and_then(Value::as_u64).unwrap_or(0));
        let row_owner_chain_depth = row
            .get("owner_chain_depth")
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize;
        if row_owner_chain_depth > max_owner_chain_depth {
            max_owner_chain_depth = row_owner_chain_depth;
        }
        let title = row
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if !title.is_empty() {
            window_title_tail.push(title.to_string());
            let title_tokens = tokenize(title);
            if !query_tokens.is_empty() && token_overlap(&query_tokens, &title_tokens) > 0 {
                query_window_match_count += 1;
            }
            if active_root_owner_hwnd > 0 && row_root_owner_hwnd == active_root_owner_hwnd {
                same_root_owner_window_count += 1;
                owner_title_tail.push(title.to_string());
                if row
                    .get("surface_hints")
                    .and_then(|value| value.get("dialog_like"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    same_root_owner_dialog_like_count += 1;
                }
            }
        }
        let class_name = row
            .get("class_name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if !class_name.is_empty() {
            class_name_tail.push(class_name.to_string());
        }
        if sample_windows.len() < 8 {
            sample_windows.push(row.clone());
        }
    }

    let topology_signature = build_topology_signature(
        &window_title_tail,
        active_title,
        windows.len(),
        dialog_like_count,
    );
    let modal_chain_signature = if active_root_owner_hwnd > 0 || same_root_owner_window_count > 0 {
        format!(
            "{}|{}|{}|{}",
            active_root_owner_hwnd,
            same_root_owner_window_count,
            same_root_owner_dialog_like_count,
            max_owner_chain_depth
        )
    } else {
        String::new()
    };

    Ok(json!({
        "status": "success",
        "query": clean_query,
        "query_tokens": query_tokens,
        "active_window": active_window,
        "visible_window_count": windows.len(),
        "dialog_like_count": dialog_like_count,
        "same_process_window_count": same_process_window_count,
        "query_window_match_count": query_window_match_count,
        "owner_link_count": owner_link_count,
        "owner_chain_visible": owner_link_count > 0 || active_owner_chain_depth > 0,
        "same_root_owner_window_count": same_root_owner_window_count,
        "same_root_owner_dialog_like_count": same_root_owner_dialog_like_count,
        "active_owner_chain_depth": active_owner_chain_depth,
        "max_owner_chain_depth": max_owner_chain_depth,
        "window_title_tail": window_title_tail.into_iter().take(8).collect::<Vec<_>>(),
        "owner_title_tail": owner_title_tail.into_iter().take(8).collect::<Vec<_>>(),
        "class_name_tail": class_name_tail.into_iter().take(8).collect::<Vec<_>>(),
        "windows": sample_windows,
        "topology_signature": topology_signature,
        "modal_chain_signature": modal_chain_signature,
    }))
}

#[cfg(not(target_os = "windows"))]
pub fn window_topology_snapshot(query: &str) -> anyhow::Result<Value> {
    Ok(json!({
        "status": "error",
        "message": "window_topology_snapshot is only supported on Windows",
        "query": query.trim(),
    }))
}

pub fn route_surface_exploration(payload: &Value) -> anyhow::Result<Value> {
    let input: SurfaceExplorationRouterInput = serde_json::from_value(payload.clone())?;
    let topology = window_topology_snapshot(&input.query)?;
    let topology_status = topology
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("error");
    let query_tokens = tokenize(&input.query);
    let current_window_tokens = tokenize(&input.current_window_title);
    let current_window_app_tokens = tokenize(&input.current_window_app_name);
    let current_reacquired_tokens = tokenize(&input.current_reacquired_title);
    let current_reacquired_app_tokens = tokenize(&input.current_reacquired_app_name);
    let current_surface_tokens = input
        .current_surface_path
        .iter()
        .flat_map(|value| tokenize(value))
        .collect::<Vec<_>>();
    let topology_window_tokens = window_titles_from_topology(&topology)
        .into_iter()
        .flat_map(|value| tokenize(&value))
        .collect::<Vec<_>>();
    let active_window = active_window_from_topology(&topology);
    let active_window_title = active_window
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("");
    let active_dialog_visible = topology_dialog_visible(&topology);
    let native_same_process_window_count = input.native_same_process_window_count.max(
        topology
            .get("same_process_window_count")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32,
    );
    let native_related_window_count = input.native_related_window_count.max(
        topology
            .get("query_window_match_count")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32,
    );
    let native_owner_link_count = input.native_owner_link_count.max(
        topology
            .get("owner_link_count")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32,
    );
    let native_owner_chain_visible = input.native_owner_chain_visible
        || topology
            .get("owner_chain_visible")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    let native_same_root_owner_window_count = input.native_same_root_owner_window_count.max(
        topology
            .get("same_root_owner_window_count")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32,
    );
    let native_same_root_owner_dialog_like_count =
        input.native_same_root_owner_dialog_like_count.max(
            topology
                .get("same_root_owner_dialog_like_count")
                .and_then(Value::as_u64)
                .unwrap_or(0) as u32,
        );
    let native_direct_child_window_count = input.native_direct_child_window_count;
    let native_direct_child_dialog_like_count = input.native_direct_child_dialog_like_count;
    let native_active_owner_chain_depth = input.native_active_owner_chain_depth.max(
        topology
            .get("active_owner_chain_depth")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32,
    );
    let native_max_owner_chain_depth = input.native_max_owner_chain_depth.max(
        topology
            .get("max_owner_chain_depth")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32,
    );
    let native_descendant_chain_depth = input.native_descendant_chain_depth;
    let native_descendant_dialog_chain_depth = input.native_descendant_dialog_chain_depth;
    let native_descendant_query_match_count = input.native_descendant_query_match_count;
    let native_descendant_adoption_available = input.native_descendant_adoption_available;
    let native_descendant_adoption_match_score =
        input.native_descendant_adoption_match_score.clamp(0.0, 1.0);
    let native_descendant_chain_titles = input
        .native_descendant_chain_titles
        .iter()
        .map(|value| normalize_text(value))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let native_descendant_title_tokens = native_descendant_chain_titles
        .iter()
        .flat_map(|value| tokenize(value))
        .collect::<Vec<_>>();
    let preferred_descendant_title = normalize_text(&input.preferred_descendant_title);
    let preferred_descendant_hwnd = input.preferred_descendant_hwnd;
    let preferred_descendant_tokens = tokenize(&preferred_descendant_title);
    let native_child_dialog_visible = input.native_child_dialog_like_visible
        || topology
            .get("dialog_like_count")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            > 0;
    let native_modal_chain_signature = if !input.native_modal_chain_signature.trim().is_empty() {
        input.native_modal_chain_signature.trim().to_string()
    } else {
        topology
            .get("modal_chain_signature")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string()
    };
    let native_child_chain_signature = input.native_child_chain_signature.trim().to_string();
    let native_branch_family_signature = if !input.native_branch_family_signature.trim().is_empty()
    {
        input.native_branch_family_signature.trim().to_string()
    } else {
        branch_family_signature_from_modal(&native_modal_chain_signature)
    };
    let latest_branch = input.branch_history.last().cloned().unwrap_or_default();
    let latest_transition = normalize_text(&latest_branch.transition_kind);
    let latest_branch_family_signature = if !latest_branch
        .topology_branch_family_signature
        .trim()
        .is_empty()
    {
        latest_branch
            .topology_branch_family_signature
            .trim()
            .to_string()
    } else {
        String::new()
    };
    let latest_occurrences = latest_branch.occurrences.max(1);
    let branch_family_repeat_count = input.branch_family_repeat_count;
    let branch_family_switch_count = input.branch_family_switch_count;
    let branch_family_continuity = input.branch_family_continuity
        || (!native_branch_family_signature.is_empty()
            && !latest_branch_family_signature.is_empty()
            && native_branch_family_signature == latest_branch_family_signature);
    let branch_cascade_count = input.branch_cascade_count;
    let branch_cascade_kind_count = input.branch_cascade_kind_count;
    let branch_cascade_signature = normalize_text(&input.branch_cascade_signature);
    let benchmark_dialog_pressure = input.benchmark_dialog_pressure.clamp(0.0, 1.0);
    let benchmark_descendant_focus_pressure =
        input.benchmark_descendant_focus_pressure.clamp(0.0, 1.0);
    let benchmark_navigation_pressure = input.benchmark_navigation_pressure.clamp(0.0, 1.0);
    let benchmark_reacquire_pressure = input.benchmark_reacquire_pressure.clamp(0.0, 1.0);
    let benchmark_loop_guard_pressure = input.benchmark_loop_guard_pressure.clamp(0.0, 1.0);
    let benchmark_native_focus_pressure = input.benchmark_native_focus_pressure.clamp(0.0, 1.0);
    let benchmark_target_app_name = normalize_text(&input.benchmark_target_app_name);
    let benchmark_target_app_tokens = tokenize(&benchmark_target_app_name);
    let benchmark_target_app_matched =
        input.benchmark_target_app_matched && !benchmark_target_app_name.is_empty();
    let benchmark_target_app_match_score = input.benchmark_target_app_match_score.clamp(0.0, 1.0);
    let benchmark_target_query_hint_tokens = input
        .benchmark_target_query_hints
        .iter()
        .flat_map(|value| tokenize(value))
        .collect::<Vec<_>>();
    let benchmark_target_descendant_title_hint_tokens = input
        .benchmark_target_descendant_title_hints
        .iter()
        .flat_map(|value| tokenize(value))
        .collect::<Vec<_>>();
    let benchmark_target_descendant_hint_query =
        normalize_text(&input.benchmark_target_descendant_hint_query);
    let benchmark_target_descendant_hint_query_tokens =
        tokenize(&benchmark_target_descendant_hint_query);
    let benchmark_target_preferred_window_title =
        normalize_text(&input.benchmark_target_preferred_window_title);
    let benchmark_target_preferred_window_tokens =
        tokenize(&benchmark_target_preferred_window_title);
    let benchmark_target_hint_query = normalize_text(&input.benchmark_target_hint_query);
    let benchmark_target_hint_query_tokens = tokenize(&benchmark_target_hint_query);
    let benchmark_target_priority = input.benchmark_target_priority.max(0.0);
    let benchmark_target_max_horizon_steps = input.benchmark_target_max_horizon_steps;
    let benchmark_target_replay_pressure = input.benchmark_target_replay_pressure.max(0.0);
    let benchmark_target_replay_session_count = input.benchmark_target_replay_session_count;
    let benchmark_target_replay_pending_count = input.benchmark_target_replay_pending_count;
    let benchmark_target_replay_failed_count = input.benchmark_target_replay_failed_count;
    let benchmark_target_replay_completed_count = input.benchmark_target_replay_completed_count;
    let benchmark_target_campaign_count = input.benchmark_target_campaign_count;
    let benchmark_target_campaign_sweep_count = input.benchmark_target_campaign_sweep_count;
    let benchmark_target_campaign_pending_session_count =
        input.benchmark_target_campaign_pending_session_count;
    let benchmark_target_campaign_attention_session_count =
        input.benchmark_target_campaign_attention_session_count;
    let benchmark_target_campaign_pending_app_target_count =
        input.benchmark_target_campaign_pending_app_target_count;
    let benchmark_target_campaign_regression_cycle_count =
        input.benchmark_target_campaign_regression_cycle_count;
    let benchmark_target_campaign_long_horizon_pending_count =
        input.benchmark_target_campaign_long_horizon_pending_count;
    let benchmark_target_campaign_pressure =
        input.benchmark_target_campaign_pressure.max(0.0);
    let benchmark_target_campaign_hint_query =
        normalize_text(&input.benchmark_target_campaign_hint_query);
    let benchmark_target_campaign_hint_query_tokens =
        tokenize(&benchmark_target_campaign_hint_query);
    let benchmark_target_campaign_descendant_title_hint_tokens = input
        .benchmark_target_campaign_descendant_title_hints
        .iter()
        .flat_map(|value| tokenize(value))
        .collect::<Vec<_>>();
    let benchmark_target_campaign_descendant_hint_query =
        normalize_text(&input.benchmark_target_campaign_descendant_hint_query);
    let benchmark_target_campaign_descendant_hint_query_tokens =
        tokenize(&benchmark_target_campaign_descendant_hint_query);
    let benchmark_target_campaign_preferred_window_title =
        normalize_text(&input.benchmark_target_campaign_preferred_window_title);
    let benchmark_target_campaign_preferred_window_tokens =
        tokenize(&benchmark_target_campaign_preferred_window_title);
    let benchmark_target_campaign_latest_sweep_status =
        normalize_text(&input.benchmark_target_campaign_latest_sweep_status);
    let benchmark_target_campaign_latest_sweep_regression_status =
        normalize_text(&input.benchmark_target_campaign_latest_sweep_regression_status);
    let benchmark_target_session_cycle_count = input.benchmark_target_session_cycle_count;
    let benchmark_target_regression_cycle_count =
        input.benchmark_target_regression_cycle_count;
    let benchmark_target_long_horizon_pending_count =
        input.benchmark_target_long_horizon_pending_count;
    let benchmark_target_dialog_pressure = input.benchmark_target_dialog_pressure.clamp(0.0, 1.0);
    let benchmark_target_descendant_focus_pressure =
        input.benchmark_target_descendant_focus_pressure.clamp(0.0, 1.0);
    let benchmark_target_navigation_pressure =
        input.benchmark_target_navigation_pressure.clamp(0.0, 1.0);
    let benchmark_target_reacquire_pressure =
        input.benchmark_target_reacquire_pressure.clamp(0.0, 1.0);
    let benchmark_target_loop_guard_pressure =
        input.benchmark_target_loop_guard_pressure.clamp(0.0, 1.0);
    let benchmark_target_native_focus_pressure =
        input.benchmark_target_native_focus_pressure.clamp(0.0, 1.0);
    let prefer_nested_branch = matches!(
        latest_transition.as_str(),
        "child_window" | "dialog_shift" | "drilldown" | "pane_shift"
    );
    let prefer_branch_cascade = branch_cascade_count > 0 && branch_cascade_kind_count > 0;
    let mixed_branch_cascade = branch_cascade_kind_count > 1;

    let mut ranked_candidates: Vec<Value> = Vec::new();
    for row in &input.selection_rows {
        let label_tokens = tokenize(&row.label);
        let selected_action = normalize_text(&row.selected_action);
        let focus_like_action =
            matches!(selected_action.as_str(), "focus" | "focus_related_window");
        let kind = normalize_text(&row.kind);
        let mut rust_score = 0.0_f64;
        let mut reasons: Vec<String> = Vec::new();

        let query_overlap = token_overlap(&query_tokens, &label_tokens);
        if query_overlap > 0 {
            let boost = (query_overlap as f64 * 0.04).min(0.14);
            rust_score += boost;
            reasons.push(format!("label_matches_query:{query_overlap}"));
        }

        let path_overlap = token_overlap(&current_surface_tokens, &label_tokens);
        if path_overlap > 0
            && matches!(
                selected_action.as_str(),
                "select_sidebar_item"
                    | "select_tab_page"
                    | "select_list_item"
                    | "focus_input_field"
            )
        {
            rust_score += (path_overlap as f64 * 0.03).min(0.09);
            reasons.push(format!("path_continuation:{path_overlap}"));
        }

        let window_overlap = token_overlap(&current_window_tokens, &label_tokens)
            + token_overlap(&topology_window_tokens, &label_tokens);
        if window_overlap > 0
            && matches!(
                selected_action.as_str(),
                "click" | "press_dialog_button" | "select_tab_page"
            )
        {
            rust_score += (window_overlap as f64 * 0.02).min(0.06);
            reasons.push(format!("window_context_overlap:{window_overlap}"));
        }
        let reacquired_overlap = token_overlap(&current_reacquired_tokens, &label_tokens);
        if reacquired_overlap > 0
            && matches!(
                selected_action.as_str(),
                "press_dialog_button" | "select_tab_page" | "select_list_item" | "select_tree_item"
            )
        {
            rust_score += (reacquired_overlap as f64 * 0.03).min(0.09);
            reasons.push(format!("reacquired_window_overlap:{reacquired_overlap}"));
        }
        let descendant_title_overlap =
            token_overlap(&native_descendant_title_tokens, &label_tokens);
        let target_app_overlap = token_overlap(&benchmark_target_app_tokens, &label_tokens)
            + token_overlap(&benchmark_target_app_tokens, &current_window_app_tokens)
            + token_overlap(&benchmark_target_app_tokens, &current_reacquired_app_tokens);
        let target_hint_overlap = token_overlap(&benchmark_target_query_hint_tokens, &label_tokens);
        let target_descendant_hint_overlap =
            token_overlap(&benchmark_target_descendant_title_hint_tokens, &label_tokens)
                + token_overlap(&benchmark_target_descendant_hint_query_tokens, &label_tokens);
        let target_hint_query_overlap =
            token_overlap(&benchmark_target_hint_query_tokens, &label_tokens);
        let target_preferred_window_overlap =
            token_overlap(&benchmark_target_preferred_window_tokens, &label_tokens);
        let campaign_hint_overlap =
            token_overlap(&benchmark_target_campaign_hint_query_tokens, &label_tokens);
        let campaign_descendant_hint_overlap =
            token_overlap(
                &benchmark_target_campaign_descendant_title_hint_tokens,
                &label_tokens,
            ) + token_overlap(
                &benchmark_target_campaign_descendant_hint_query_tokens,
                &label_tokens,
            );
        let campaign_preferred_window_overlap =
            token_overlap(&benchmark_target_campaign_preferred_window_tokens, &label_tokens);
        if descendant_title_overlap > 0 {
            rust_score += (descendant_title_overlap as f64 * 0.04).min(0.12);
            reasons.push(format!(
                "descendant_chain_overlap:{descendant_title_overlap}"
            ));
        }
        if benchmark_target_app_matched && target_app_overlap > 0 {
            rust_score += (target_app_overlap as f64 * 0.03).min(0.12);
            reasons.push(format!("benchmark_target_app_overlap:{target_app_overlap}"));
        }
        if benchmark_target_app_matched && target_hint_overlap > 0 {
            rust_score += (target_hint_overlap as f64 * 0.04).min(0.14);
            reasons.push(format!("benchmark_target_query_hint:{target_hint_overlap}"));
        }
        if benchmark_target_app_matched && target_descendant_hint_overlap > 0 {
            rust_score += (target_descendant_hint_overlap as f64 * 0.045).min(0.16);
            reasons.push(format!(
                "benchmark_target_descendant_hint:{}",
                target_descendant_hint_overlap
            ));
        }
        if benchmark_target_app_matched && target_hint_query_overlap > 0 {
            rust_score += (target_hint_query_overlap as f64 * 0.045).min(0.14);
            reasons.push(format!(
                "benchmark_target_hint_query:{target_hint_query_overlap}"
            ));
        }
        if benchmark_target_app_matched && target_preferred_window_overlap > 0 {
            rust_score += (target_preferred_window_overlap as f64 * 0.04).min(0.12);
            reasons.push(format!(
                "benchmark_target_preferred_window:{}",
                target_preferred_window_overlap
            ));
        }
        if benchmark_target_app_matched && campaign_hint_overlap > 0 {
            rust_score += (campaign_hint_overlap as f64 * 0.04).min(0.14);
            reasons.push(format!("benchmark_campaign_hint:{}", campaign_hint_overlap));
        }
        if benchmark_target_app_matched && campaign_descendant_hint_overlap > 0 {
            rust_score += (campaign_descendant_hint_overlap as f64 * 0.045).min(0.16);
            reasons.push(format!(
                "benchmark_campaign_descendant_hint:{}",
                campaign_descendant_hint_overlap
            ));
        }
        if benchmark_target_app_matched && campaign_preferred_window_overlap > 0 {
            rust_score += (campaign_preferred_window_overlap as f64 * 0.04).min(0.12);
            reasons.push(format!(
                "benchmark_campaign_preferred_window:{}",
                campaign_preferred_window_overlap
            ));
        }
        let preferred_descendant_overlap =
            token_overlap(&preferred_descendant_tokens, &label_tokens);
        let preferred_descendant_focus = kind == "branch_action"
            && focus_like_action
            && ((!preferred_descendant_title.is_empty() && preferred_descendant_overlap > 0)
                || (preferred_descendant_hwnd > 0
                    && row.candidate_id.trim() == preferred_descendant_hwnd.to_string()));
        if preferred_descendant_focus {
            rust_score += 0.12;
            reasons.push("preferred_descendant_focus".to_string());
            if preferred_descendant_overlap > 0 {
                rust_score += (preferred_descendant_overlap as f64 * 0.03).min(0.09);
                reasons.push(format!(
                    "preferred_descendant_overlap:{}",
                    preferred_descendant_overlap
                ));
            }
        }
        if benchmark_dialog_pressure > 0.0 && selected_action == "press_dialog_button" {
            let boost = 0.05 + (benchmark_dialog_pressure * 0.16);
            rust_score += boost;
            reasons.push(format!(
                "benchmark_dialog_pressure:{:.2}",
                benchmark_dialog_pressure
            ));
        } else if benchmark_dialog_pressure >= 0.6 && kind == "branch_action" {
            rust_score -= benchmark_dialog_pressure * 0.04;
        }
        if benchmark_descendant_focus_pressure > 0.0 && preferred_descendant_focus {
            let boost = 0.04 + (benchmark_descendant_focus_pressure * 0.16);
            rust_score += boost;
            reasons.push(format!(
                "benchmark_descendant_focus_pressure:{:.2}",
                benchmark_descendant_focus_pressure
            ));
        }
        if benchmark_navigation_pressure > 0.0
            && matches!(
                selected_action.as_str(),
                "select_sidebar_item"
                    | "select_tab_page"
                    | "select_list_item"
                    | "select_tree_item"
                    | "focus_input_field"
                    | "open_dropdown"
            )
        {
            let boost = 0.04 + (benchmark_navigation_pressure * 0.14);
            rust_score += boost;
            reasons.push(format!(
                "benchmark_navigation_pressure:{:.2}",
                benchmark_navigation_pressure
            ));
        }
        if benchmark_reacquire_pressure > 0.0 && focus_like_action {
            let boost = 0.04 + (benchmark_reacquire_pressure * 0.16);
            rust_score += boost;
            reasons.push(format!(
                "benchmark_reacquire_pressure:{:.2}",
                benchmark_reacquire_pressure
            ));
        }
        if benchmark_native_focus_pressure > 0.0 && preferred_descendant_focus {
            let boost = 0.03 + (benchmark_native_focus_pressure * 0.12);
            rust_score += boost;
            reasons.push(format!(
                "benchmark_native_focus_pressure:{:.2}",
                benchmark_native_focus_pressure
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_dialog_pressure > 0.0
            && selected_action == "press_dialog_button"
        {
            let boost = 0.04 + (benchmark_target_dialog_pressure * 0.14);
            rust_score += boost;
            reasons.push(format!(
                "benchmark_target_dialog_pressure:{:.2}",
                benchmark_target_dialog_pressure
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_descendant_focus_pressure > 0.0
            && preferred_descendant_focus
        {
            let boost = 0.04 + (benchmark_target_descendant_focus_pressure * 0.14);
            rust_score += boost;
            reasons.push(format!(
                "benchmark_target_descendant_focus_pressure:{:.2}",
                benchmark_target_descendant_focus_pressure
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_navigation_pressure > 0.0
            && matches!(
                selected_action.as_str(),
                "select_sidebar_item"
                    | "select_tab_page"
                    | "select_list_item"
                    | "select_tree_item"
                    | "focus_input_field"
                    | "open_dropdown"
            )
        {
            let boost = 0.03 + (benchmark_target_navigation_pressure * 0.14);
            rust_score += boost;
            reasons.push(format!(
                "benchmark_target_navigation_pressure:{:.2}",
                benchmark_target_navigation_pressure
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_reacquire_pressure > 0.0
            && focus_like_action
        {
            let boost = 0.04 + (benchmark_target_reacquire_pressure * 0.14);
            rust_score += boost;
            reasons.push(format!(
                "benchmark_target_reacquire_pressure:{:.2}",
                benchmark_target_reacquire_pressure
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_native_focus_pressure > 0.0
            && preferred_descendant_focus
        {
            let boost = 0.03 + (benchmark_target_native_focus_pressure * 0.12);
            rust_score += boost;
            reasons.push(format!(
                "benchmark_target_native_focus_pressure:{:.2}",
                benchmark_target_native_focus_pressure
            ));
        }
        if benchmark_target_app_matched && benchmark_target_replay_pressure > 0.0 {
            let mut replay_boost = (0.02 * benchmark_target_replay_pressure.min(5.0))
                + (0.025 * benchmark_target_replay_failed_count.min(3) as f64)
                + (0.015 * benchmark_target_replay_pending_count.min(3) as f64)
                + (0.01 * benchmark_target_replay_session_count.min(3) as f64);
            if preferred_descendant_focus {
                replay_boost += (0.03 + (target_hint_query_overlap as f64 * 0.05)).min(0.09);
            } else if selected_action == "press_dialog_button" {
                replay_boost += (0.02 + (target_hint_overlap as f64 * 0.04)).min(0.07);
            } else if focus_like_action {
                replay_boost += (0.02 + (target_hint_query_overlap as f64 * 0.03)).min(0.06);
            }
            rust_score += replay_boost.min(0.26);
            reasons.push(format!(
                "benchmark_target_replay_pressure:{:.2}",
                benchmark_target_replay_pressure
            ));
        }
        if benchmark_target_app_matched && benchmark_target_campaign_pressure > 0.0 {
            let mut campaign_boost = (0.02 * benchmark_target_campaign_pressure.min(5.0))
                + (0.02 * benchmark_target_campaign_attention_session_count.min(3) as f64)
                + (0.015 * benchmark_target_campaign_pending_session_count.min(3) as f64)
                + (0.02 * benchmark_target_campaign_pending_app_target_count.min(3) as f64)
                + (0.02 * benchmark_target_campaign_regression_cycle_count.min(3) as f64)
                + (0.012 * benchmark_target_campaign_sweep_count.min(4) as f64)
                + (0.008 * benchmark_target_campaign_count.min(4) as f64);
            if preferred_descendant_focus {
                campaign_boost +=
                    (0.03 + (campaign_descendant_hint_overlap as f64 * 0.05)).min(0.09);
            } else if selected_action == "press_dialog_button" {
                campaign_boost +=
                    (0.02 + (campaign_hint_overlap as f64 * 0.04)).min(0.07);
            } else if focus_like_action {
                campaign_boost +=
                    (0.02 + (campaign_preferred_window_overlap as f64 * 0.03)).min(0.06);
            }
            rust_score += campaign_boost.min(0.28);
            reasons.push(format!(
                "benchmark_campaign_pressure:{:.2}",
                benchmark_target_campaign_pressure
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_session_cycle_count > 0
            && target_descendant_hint_overlap > 0
        {
            rust_score += (benchmark_target_session_cycle_count.min(4) as f64 * 0.015).min(0.06);
            reasons.push(format!(
                "benchmark_target_session_cycles:{}",
                benchmark_target_session_cycle_count
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_regression_cycle_count > 0
            && (preferred_descendant_focus || target_descendant_hint_overlap > 0)
        {
            let regression_boost =
                (benchmark_target_regression_cycle_count.min(4) as f64 * 0.02).min(0.08);
            rust_score += regression_boost;
            reasons.push(format!(
                "benchmark_target_regression_cycles:{}",
                benchmark_target_regression_cycle_count
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_campaign_attention_session_count > 0
            && (campaign_descendant_hint_overlap > 0 || campaign_preferred_window_overlap > 0)
        {
            rust_score +=
                (benchmark_target_campaign_attention_session_count.min(4) as f64 * 0.018)
                    .min(0.08);
            reasons.push(format!(
                "benchmark_campaign_attention:{}",
                benchmark_target_campaign_attention_session_count
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_campaign_regression_cycle_count > 0
            && (preferred_descendant_focus || campaign_descendant_hint_overlap > 0)
        {
            rust_score +=
                (benchmark_target_campaign_regression_cycle_count.min(4) as f64 * 0.02)
                    .min(0.08);
            reasons.push(format!(
                "benchmark_campaign_regression:{}",
                benchmark_target_campaign_regression_cycle_count
            ));
        }
        if benchmark_target_app_matched && benchmark_target_campaign_pending_app_target_count > 0 {
            if preferred_descendant_focus || focus_like_action {
                rust_score +=
                    (benchmark_target_campaign_pending_app_target_count.min(3) as f64 * 0.02)
                        .min(0.06);
                reasons.push(format!(
                    "benchmark_campaign_pending_apps:{}",
                    benchmark_target_campaign_pending_app_target_count
                ));
            }
        }
        if native_descendant_adoption_available && focus_like_action {
            rust_score += (0.03 + (native_descendant_adoption_match_score * 0.08)).min(0.1);
            reasons.push(format!(
                "native_descendant_adoption_ready:{:.2}",
                native_descendant_adoption_match_score
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_campaign_long_horizon_pending_count > 0
            && (preferred_descendant_focus || selected_action == "press_dialog_button")
        {
            rust_score +=
                (benchmark_target_campaign_long_horizon_pending_count.min(3) as f64 * 0.02)
                    .min(0.06);
            reasons.push(format!(
                "benchmark_campaign_long_horizon:{}",
                benchmark_target_campaign_long_horizon_pending_count
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_campaign_latest_sweep_status == "failed"
            && (campaign_descendant_hint_overlap > 0 || campaign_preferred_window_overlap > 0)
        {
            rust_score += 0.05;
            reasons.push("benchmark_campaign_latest_sweep_failed".to_string());
        }
        if benchmark_target_app_matched
            && matches!(
                benchmark_target_campaign_latest_sweep_regression_status.as_str(),
                "regression" | "failed"
            )
            && (campaign_descendant_hint_overlap > 0 || campaign_preferred_window_overlap > 0)
        {
            rust_score += 0.06;
            reasons.push("benchmark_campaign_latest_regression".to_string());
        }
        if benchmark_target_app_matched && benchmark_target_long_horizon_pending_count > 0 {
            if preferred_descendant_focus || selected_action == "press_dialog_button" {
                rust_score +=
                    (benchmark_target_long_horizon_pending_count.min(3) as f64 * 0.02).min(0.06)
                        + 0.02;
            } else if matches!(
                selected_action.as_str(),
                "select_sidebar_item"
                    | "select_tab_page"
                    | "select_list_item"
                    | "select_tree_item"
                    | "focus_input_field"
                    | "open_dropdown"
            ) {
                rust_score +=
                    (benchmark_target_long_horizon_pending_count.min(3) as f64 * 0.015).min(0.045)
                        + 0.01;
            }
            reasons.push(format!(
                "benchmark_target_long_horizon_pending:{}",
                benchmark_target_long_horizon_pending_count
            ));
        }
        if benchmark_target_app_matched
            && benchmark_target_loop_guard_pressure > 0.0
            && kind == "branch_action"
            && !preferred_descendant_focus
        {
            rust_score -= 0.02 + (benchmark_target_loop_guard_pressure * 0.05);
        }
        if benchmark_target_app_matched && benchmark_target_priority > 0.0 {
            rust_score += (0.01 + (benchmark_target_priority * 0.01)).min(0.08);
            reasons.push("benchmark_target_priority".to_string());
        }
        if benchmark_target_app_matched && benchmark_target_app_match_score >= 0.95 {
            rust_score += 0.03;
            reasons.push("benchmark_target_exact_app_match".to_string());
        }
        if benchmark_target_app_matched && benchmark_target_max_horizon_steps >= 4 && kind == "hypothesis" {
            rust_score += 0.02;
            reasons.push(format!(
                "benchmark_target_horizon_steps:{}",
                benchmark_target_max_horizon_steps
            ));
        }
        if benchmark_target_app_matched && benchmark_target_replay_completed_count > 0 && kind == "hypothesis" {
            rust_score += (benchmark_target_replay_completed_count.min(4) as f64 * 0.01).min(0.04);
            reasons.push(format!(
                "benchmark_target_replay_completed:{}",
                benchmark_target_replay_completed_count
            ));
        }

        if input.current_dialog_visible || active_dialog_visible || native_child_dialog_visible {
            if selected_action == "press_dialog_button" {
                rust_score += 0.16;
                reasons.push("dialog_resolution_visible".to_string());
            } else {
                rust_score -= 0.05;
            }
        }
        if native_child_dialog_visible
            && matches!(latest_transition.as_str(), "child_window" | "dialog_shift")
        {
            if selected_action == "press_dialog_button" {
                rust_score += 0.08;
                reasons.push("native_child_dialog_cluster".to_string());
            } else {
                rust_score -= 0.04;
            }
        }

        if prefer_nested_branch {
            match latest_transition.as_str() {
                "child_window" | "dialog_shift" => {
                    if selected_action == "press_dialog_button" {
                        rust_score += 0.12;
                        reasons.push(format!("nested_dialog_branch:{latest_transition}"));
                    } else if kind == "branch_action" {
                        rust_score += 0.04;
                    }
                }
                "drilldown" | "pane_shift" => {
                    if matches!(
                        selected_action.as_str(),
                        "select_sidebar_item"
                            | "select_tab_page"
                            | "select_list_item"
                            | "focus_input_field"
                    ) {
                        rust_score += 0.1;
                        reasons.push(format!("nested_navigation_branch:{latest_transition}"));
                    }
                }
                _ => {}
            }
        }
        if prefer_branch_cascade {
            match latest_transition.as_str() {
                "child_window_chain" | "dialog_shift" => {
                    if selected_action == "press_dialog_button" {
                        rust_score += if mixed_branch_cascade { 0.08 } else { 0.05 };
                        reasons.push(format!(
                            "branch_cascade_dialog_resolution:{branch_cascade_count}"
                        ));
                    } else if kind == "branch_action" && mixed_branch_cascade {
                        rust_score -= 0.04;
                    }
                }
                "drilldown" | "pane_shift" => {
                    if matches!(
                        selected_action.as_str(),
                        "select_sidebar_item"
                            | "select_tab_page"
                            | "select_list_item"
                            | "select_tree_item"
                            | "focus_input_field"
                            | "open_dropdown"
                    ) {
                        rust_score += if mixed_branch_cascade { 0.08 } else { 0.05 };
                        reasons.push(format!("branch_cascade_navigation:{branch_cascade_count}"));
                    } else if kind == "branch_action" && mixed_branch_cascade {
                        rust_score -= 0.03;
                    }
                }
                _ => {}
            }
        }
        if !branch_cascade_signature.is_empty()
            && branch_cascade_signature.contains("dialog_shift")
            && selected_action == "press_dialog_button"
        {
            rust_score += 0.03;
            reasons.push("branch_cascade_signature_dialog".to_string());
        }
        if native_same_process_window_count > 1 && selected_action == "press_dialog_button" {
            rust_score += 0.05;
            reasons.push(format!(
                "same_process_cluster:{}",
                native_same_process_window_count
            ));
        }
        if native_owner_chain_visible {
            if selected_action == "press_dialog_button" {
                rust_score += 0.09;
                reasons.push("owner_chain_visible".to_string());
            } else if matches!(
                selected_action.as_str(),
                "select_sidebar_item"
                    | "select_tab_page"
                    | "select_list_item"
                    | "select_tree_item"
                    | "focus_input_field"
            ) {
                rust_score += 0.04;
                reasons.push("owner_chain_navigation".to_string());
            }
        }
        if native_owner_link_count > 1 && selected_action == "press_dialog_button" {
            rust_score += 0.05;
            reasons.push(format!("owner_link_cluster:{}", native_owner_link_count));
        }
        if native_same_root_owner_window_count > 1 && selected_action == "press_dialog_button" {
            rust_score += 0.07;
            reasons.push(format!(
                "same_root_owner_cluster:{}",
                native_same_root_owner_window_count
            ));
        }
        if native_same_root_owner_dialog_like_count > 1 && selected_action == "press_dialog_button"
        {
            rust_score += 0.08;
            reasons.push(format!(
                "same_root_owner_dialog_cluster:{}",
                native_same_root_owner_dialog_like_count
            ));
        }
        if native_direct_child_window_count > 0 {
            if selected_action == "press_dialog_button"
                && matches!(
                    latest_transition.as_str(),
                    "child_window" | "child_window_chain" | "dialog_shift"
                )
            {
                rust_score += 0.05;
                reasons.push(format!(
                    "direct_child_cluster:{}",
                    native_direct_child_window_count
                ));
            } else if preferred_descendant_focus {
                rust_score += 0.04;
                reasons.push(format!(
                    "direct_child_focus:{}",
                    native_direct_child_window_count
                ));
            } else if matches!(
                selected_action.as_str(),
                "select_sidebar_item"
                    | "select_tab_page"
                    | "select_list_item"
                    | "select_tree_item"
                    | "focus_input_field"
            ) {
                rust_score += 0.02;
                reasons.push(format!(
                    "direct_child_navigation:{}",
                    native_direct_child_window_count
                ));
            }
        }
        if native_direct_child_dialog_like_count > 0 && selected_action == "press_dialog_button" {
            rust_score += 0.04;
            reasons.push(format!(
                "direct_child_dialog_cluster:{}",
                native_direct_child_dialog_like_count
            ));
        }
        if native_active_owner_chain_depth > 0 && selected_action == "press_dialog_button" {
            rust_score += (native_active_owner_chain_depth as f64 * 0.03).min(0.12);
            reasons.push(format!(
                "active_owner_chain_depth:{}",
                native_active_owner_chain_depth
            ));
        }
        if native_max_owner_chain_depth > native_active_owner_chain_depth
            && selected_action == "press_dialog_button"
        {
            rust_score += ((native_max_owner_chain_depth - native_active_owner_chain_depth) as f64
                * 0.04)
                .min(0.16);
            reasons.push(format!(
                "deeper_owner_chain_available:{}",
                native_max_owner_chain_depth
            ));
        }
        if native_descendant_chain_depth > 0 {
            if selected_action == "press_dialog_button"
                && matches!(
                    latest_transition.as_str(),
                    "child_window" | "child_window_chain" | "dialog_shift"
                )
            {
                rust_score += (native_descendant_chain_depth as f64 * 0.03).min(0.12);
                reasons.push(format!(
                    "descendant_chain_depth:{}",
                    native_descendant_chain_depth
                ));
            } else if preferred_descendant_focus {
                rust_score += (native_descendant_chain_depth as f64 * 0.03).min(0.12);
                reasons.push(format!(
                    "descendant_focus_chain_depth:{}",
                    native_descendant_chain_depth
                ));
            } else if matches!(
                selected_action.as_str(),
                "select_sidebar_item"
                    | "select_tab_page"
                    | "select_list_item"
                    | "select_tree_item"
                    | "focus_input_field"
                    | "open_dropdown"
            ) {
                rust_score += 0.03;
                reasons.push(format!(
                    "descendant_chain_navigation:{}",
                    native_descendant_chain_depth
                ));
            }
        }
        if native_descendant_dialog_chain_depth > 0 && selected_action == "press_dialog_button" {
            rust_score += (native_descendant_dialog_chain_depth as f64 * 0.03).min(0.1);
            reasons.push(format!(
                "descendant_chain_dialog_depth:{}",
                native_descendant_dialog_chain_depth
            ));
        } else if native_descendant_dialog_chain_depth > 0 && preferred_descendant_focus {
            rust_score += (native_descendant_dialog_chain_depth as f64 * 0.02).min(0.08);
            reasons.push(format!(
                "descendant_focus_dialog_depth:{}",
                native_descendant_dialog_chain_depth
            ));
        }
        if native_descendant_query_match_count > 0 && kind == "hypothesis" {
            rust_score += (native_descendant_query_match_count as f64 * 0.02).min(0.08);
            reasons.push(format!(
                "descendant_query_matches:{}",
                native_descendant_query_match_count
            ));
        } else if native_descendant_query_match_count > 0 && preferred_descendant_focus {
            rust_score += (native_descendant_query_match_count as f64 * 0.02).min(0.06);
            reasons.push(format!(
                "descendant_focus_query_matches:{}",
                native_descendant_query_match_count
            ));
        }
        if native_related_window_count > 1
            && matches!(
                selected_action.as_str(),
                "select_sidebar_item" | "select_tab_page" | "select_list_item" | "select_tree_item"
            )
        {
            rust_score += 0.03;
            reasons.push(format!(
                "related_window_cluster:{}",
                native_related_window_count
            ));
        }
        if !native_modal_chain_signature.is_empty()
            && matches!(
                latest_transition.as_str(),
                "child_window" | "dialog_shift" | "child_window_chain"
            )
        {
            if selected_action == "press_dialog_button" {
                rust_score += 0.05;
                reasons.push("modal_chain_signature".to_string());
            } else {
                rust_score -= 0.03;
            }
        }
        if !native_child_chain_signature.is_empty()
            && matches!(
                latest_transition.as_str(),
                "child_window" | "dialog_shift" | "child_window_chain"
            )
        {
            if selected_action == "press_dialog_button" {
                rust_score += 0.05;
                reasons.push("child_chain_signature".to_string());
            } else if preferred_descendant_focus {
                rust_score += 0.06;
                reasons.push("preferred_descendant_child_chain".to_string());
            } else {
                rust_score -= 0.02;
            }
        }
        if branch_family_continuity {
            match latest_transition.as_str() {
                "child_window" | "child_window_chain" | "dialog_shift" => {
                    if selected_action == "press_dialog_button" {
                        rust_score += if branch_family_repeat_count >= 2 {
                            0.12
                        } else {
                            0.08
                        };
                        reasons.push(format!(
                            "branch_family_dialog_continuity:{}",
                            branch_family_repeat_count.max(1)
                        ));
                    } else if preferred_descendant_focus {
                        rust_score += if branch_family_repeat_count >= 2 {
                            0.09
                        } else {
                            0.06
                        };
                        reasons.push(format!(
                            "preferred_descendant_branch_family:{}",
                            branch_family_repeat_count.max(1)
                        ));
                    } else if kind == "branch_action" {
                        rust_score -= 0.03;
                    }
                }
                "drilldown" | "pane_shift" => {
                    if matches!(
                        selected_action.as_str(),
                        "select_sidebar_item"
                            | "select_tab_page"
                            | "select_list_item"
                            | "select_tree_item"
                            | "focus_input_field"
                            | "open_dropdown"
                    ) {
                        rust_score += if branch_family_repeat_count >= 2 {
                            0.08
                        } else {
                            0.06
                        };
                        reasons.push(format!(
                            "branch_family_navigation_continuity:{}",
                            branch_family_repeat_count.max(1)
                        ));
                    }
                }
                _ => {}
            }
        } else if !native_branch_family_signature.is_empty()
            && !latest_branch_family_signature.is_empty()
            && native_branch_family_signature != latest_branch_family_signature
        {
            if kind == "branch_action" {
                rust_score -= 0.04;
                reasons.push("branch_family_switch_branch_action".to_string());
            } else if matches!(
                latest_transition.as_str(),
                "child_window" | "child_window_chain" | "dialog_shift"
            ) && selected_action != "press_dialog_button"
            {
                rust_score -= 0.03;
                reasons.push("branch_family_switch_pressure".to_string());
            }
        }
        if branch_family_switch_count >= 2 && kind == "branch_action" {
            rust_score -= 0.03;
            reasons.push(format!(
                "branch_family_switch_count:{}",
                branch_family_switch_count
            ));
        }

        if latest_occurrences >= 2
            && !latest_branch.selected_action.is_empty()
            && normalize_text(&latest_branch.selected_action) == selected_action
            && ((!latest_branch.selected_candidate_id.is_empty()
                && latest_branch.selected_candidate_id == row.candidate_id)
                || (!latest_branch.selected_candidate_label.is_empty()
                    && normalize_text(&latest_branch.selected_candidate_label)
                        == normalize_text(&row.label)))
        {
            rust_score -= 0.24;
            reasons.push(format!("repeat_branch_penalty:{latest_occurrences}"));
        } else if latest_occurrences >= 2 && kind == "branch_action" {
            rust_score -= 0.08 + (benchmark_loop_guard_pressure * 0.08);
            reasons.push("branch_repeat_pressure".to_string());
        }
        if benchmark_loop_guard_pressure > 0.0
            && kind == "branch_action"
            && branch_family_switch_count >= 1
            && !preferred_descendant_focus
        {
            rust_score -= benchmark_loop_guard_pressure * 0.06;
            reasons.push(format!(
                "benchmark_loop_guard_pressure:{:.2}",
                benchmark_loop_guard_pressure
            ));
        }

        if active_window_title.eq_ignore_ascii_case(&input.current_window_title)
            && selected_action == "press_dialog_button"
            && topology
                .get("same_process_window_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                > 1
        {
            rust_score += 0.05;
            reasons.push("same_process_child_window_bias".to_string());
        }

        let router_hint = if branch_family_continuity
            && matches!(
                latest_transition.as_str(),
                "child_window" | "child_window_chain" | "dialog_shift"
            )
            && selected_action == "press_dialog_button"
        {
            "prefer_branch_family_dialog"
        } else if preferred_descendant_focus {
            "prefer_descendant_surface_adoption"
        } else if native_descendant_dialog_chain_depth > 0
            && selected_action == "press_dialog_button"
        {
            "prefer_descendant_dialog_chain"
        } else if branch_family_continuity
            && matches!(latest_transition.as_str(), "drilldown" | "pane_shift")
            && matches!(
                selected_action.as_str(),
                "select_sidebar_item" | "select_tab_page" | "select_list_item"
            )
        {
            "prefer_branch_family_navigation"
        } else if (native_max_owner_chain_depth >= 2
            || native_same_root_owner_dialog_like_count > 1)
            && selected_action == "press_dialog_button"
        {
            "prefer_modal_chain_resolution"
        } else if (input.current_dialog_visible || active_dialog_visible)
            && selected_action == "press_dialog_button"
        {
            "prefer_dialog_resolution"
        } else if prefer_nested_branch
            && matches!(
                selected_action.as_str(),
                "select_sidebar_item" | "select_tab_page" | "select_list_item"
            )
        {
            "prefer_nested_navigation"
        } else if latest_occurrences >= 2 && rust_score < 0.0 {
            "avoid_branch_repeat"
        } else {
            "balanced"
        };

        ranked_candidates.push(json!({
            "selection_key": row.selection_key,
            "candidate_id": row.candidate_id,
            "label": row.label,
            "selected_action": row.selected_action,
            "base_confidence": row.confidence,
            "rust_score": (rust_score * 1000.0).round() / 1000.0,
            "router_hint": router_hint,
            "reasons": reasons,
            "total_score": (((row.confidence.max(0.0)) + rust_score) * 1000.0).round() / 1000.0,
        }));
    }

    ranked_candidates.sort_by(|left, right| {
        let left_total = left
            .get("total_score")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let right_total = right
            .get("total_score")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        right_total
            .partial_cmp(&left_total)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for (index, row) in ranked_candidates.iter_mut().enumerate() {
        if let Some(target) = row.as_object_mut() {
            target.insert("rank".to_string(), json!(index + 1));
        }
    }

    let loop_risk = latest_occurrences >= 2
        && ranked_candidates
            .first()
            .and_then(|row| row.get("rust_score"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
            <= 0.02;
    let router_hint = ranked_candidates
        .first()
        .and_then(|row| row.get("router_hint"))
        .and_then(Value::as_str)
        .unwrap_or(if prefer_nested_branch {
            "prefer_nested_branch"
        } else {
            "balanced"
        });

    Ok(json!({
        "status": if topology_status == "success" { "success" } else { "partial" },
        "router_hint": router_hint,
        "prefer_nested_branch": prefer_nested_branch,
        "loop_risk": loop_risk,
        "native_topology_signature": input.native_topology_signature,
        "native_owner_chain_visible": native_owner_chain_visible,
        "native_owner_link_count": native_owner_link_count,
        "native_same_root_owner_window_count": native_same_root_owner_window_count,
        "native_same_root_owner_dialog_like_count": native_same_root_owner_dialog_like_count,
        "native_direct_child_window_count": native_direct_child_window_count,
        "native_direct_child_dialog_like_count": native_direct_child_dialog_like_count,
        "native_active_owner_chain_depth": native_active_owner_chain_depth,
        "native_max_owner_chain_depth": native_max_owner_chain_depth,
        "native_descendant_chain_depth": native_descendant_chain_depth,
        "native_descendant_dialog_chain_depth": native_descendant_dialog_chain_depth,
        "native_descendant_query_match_count": native_descendant_query_match_count,
        "native_descendant_adoption_available": native_descendant_adoption_available,
        "native_descendant_adoption_match_score": native_descendant_adoption_match_score,
        "native_descendant_chain_titles": native_descendant_chain_titles,
        "native_modal_chain_signature": native_modal_chain_signature,
        "native_child_chain_signature": native_child_chain_signature,
        "native_branch_family_signature": native_branch_family_signature,
        "branch_family_repeat_count": branch_family_repeat_count,
        "branch_family_switch_count": branch_family_switch_count,
        "branch_family_continuity": branch_family_continuity,
        "topology": topology,
        "ranked_candidates": ranked_candidates,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_surface_exploration_flags_repeated_branch_loops_without_ignoring_visible_dialogs() {
        let payload = json!({
            "query": "Bluetooth",
            "current_dialog_visible": true,
            "selection_rows": [
                {
                    "selection_key": "branch_action||press_dialog_button|ok",
                    "kind": "branch_action",
                    "candidate_id": "",
                    "label": "OK",
                    "selected_action": "press_dialog_button",
                    "confidence": 0.61
                },
                {
                    "selection_key": "hypothesis|list_bluetooth|select_list_item|bluetooth",
                    "kind": "hypothesis",
                    "candidate_id": "list_bluetooth",
                    "label": "Bluetooth",
                    "selected_action": "select_list_item",
                    "confidence": 0.58
                }
            ],
            "branch_history": [
                {
                    "transition_kind": "dialog_shift",
                    "selected_action": "press_dialog_button",
                    "selected_candidate_label": "OK",
                    "occurrences": 2
                }
            ]
        });

        let result = route_surface_exploration(&payload).expect("router payload should parse");
        let rows = result
            .get("ranked_candidates")
            .and_then(Value::as_array)
            .expect("ranked candidates should be present");
        let reasons = rows
            .first()
            .and_then(|row| row.get("reasons"))
            .and_then(Value::as_array)
            .expect("top-ranked row should expose reasons");
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("repeat_branch_penalty:2")));
        assert_eq!(
            rows.first()
                .and_then(|row| row.get("selected_action"))
                .and_then(Value::as_str),
            Some("press_dialog_button")
        );
    }

    #[test]
    fn route_surface_exploration_prefers_dialog_resolution_when_dialog_is_visible() {
        let payload = json!({
            "query": "Continue",
            "current_dialog_visible": true,
            "selection_rows": [
                {
                    "selection_key": "branch_action||press_dialog_button|continue",
                    "kind": "branch_action",
                    "candidate_id": "",
                    "label": "Continue",
                    "selected_action": "press_dialog_button",
                    "confidence": 0.42
                },
                {
                    "selection_key": "hypothesis|settings_sidebar|select_sidebar_item|settings",
                    "kind": "hypothesis",
                    "candidate_id": "settings_sidebar",
                    "label": "Settings",
                    "selected_action": "select_sidebar_item",
                    "confidence": 0.42
                }
            ]
        });

        let result = route_surface_exploration(&payload).expect("router payload should parse");
        let rows = result
            .get("ranked_candidates")
            .and_then(Value::as_array)
            .expect("ranked candidates should be present");
        assert_eq!(
            rows.first()
                .and_then(|row| row.get("selected_action"))
                .and_then(Value::as_str),
            Some("press_dialog_button")
        );
    }

    #[test]
    fn route_surface_exploration_uses_native_child_dialog_cluster_for_nested_branch_bias() {
        let payload = json!({
            "query": "Bluetooth",
            "current_dialog_visible": false,
            "current_reacquired_title": "Pair device",
            "native_same_process_window_count": 3,
            "native_related_window_count": 2,
            "native_child_dialog_like_visible": true,
            "native_topology_signature": "settings|3|2",
            "selection_rows": [
                {
                    "selection_key": "hypothesis|list_bluetooth|select_list_item|bluetooth",
                    "kind": "hypothesis",
                    "candidate_id": "list_bluetooth",
                    "label": "Bluetooth",
                    "selected_action": "select_list_item",
                    "confidence": 0.78
                },
                {
                    "selection_key": "hypothesis|dialog_ok|press_dialog_button|ok",
                    "kind": "hypothesis",
                    "candidate_id": "dialog_ok",
                    "label": "OK",
                    "selected_action": "press_dialog_button",
                    "confidence": 0.69
                }
            ],
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "select_list_item",
                    "selected_candidate_id": "list_bluetooth",
                    "selected_candidate_label": "Bluetooth",
                    "window_title": "Bluetooth & devices",
                    "occurrences": 1
                }
            ]
        });

        let result = route_surface_exploration(&payload).expect("router payload should parse");
        let rows = result
            .get("ranked_candidates")
            .and_then(Value::as_array)
            .expect("ranked candidates should be present");
        let reasons = rows
            .first()
            .and_then(|row| row.get("reasons"))
            .and_then(Value::as_array)
            .expect("top-ranked row should expose reasons");
        assert_eq!(
            rows.first()
                .and_then(|row| row.get("selected_action"))
                .and_then(Value::as_str),
            Some("press_dialog_button")
        );
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("native_child_dialog_cluster")));
    }

    #[test]
    fn route_surface_exploration_uses_owner_chain_cluster_for_dialog_bias() {
        let payload = json!({
            "query": "Continue",
            "current_dialog_visible": false,
            "native_owner_link_count": 2,
            "native_owner_chain_visible": true,
            "native_same_root_owner_window_count": 3,
            "native_same_root_owner_dialog_like_count": 2,
            "native_active_owner_chain_depth": 1,
            "native_max_owner_chain_depth": 2,
            "native_modal_chain_signature": "5000|3|2|2",
            "selection_rows": [
                {
                    "selection_key": "branch_action||press_dialog_button|continue",
                    "kind": "branch_action",
                    "candidate_id": "",
                    "label": "Continue",
                    "selected_action": "press_dialog_button",
                    "confidence": 0.48
                },
                {
                    "selection_key": "hypothesis|settings_sidebar|select_sidebar_item|settings",
                    "kind": "hypothesis",
                    "candidate_id": "settings_sidebar",
                    "label": "Settings",
                    "selected_action": "select_sidebar_item",
                    "confidence": 0.51
                }
            ],
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "select_sidebar_item",
                    "selected_candidate_label": "Settings",
                    "occurrences": 1
                }
            ]
        });

        let result = route_surface_exploration(&payload).expect("router payload should parse");
        let rows = result
            .get("ranked_candidates")
            .and_then(Value::as_array)
            .expect("ranked candidates should be present");
        let reasons = rows
            .first()
            .and_then(|row| row.get("reasons"))
            .and_then(Value::as_array)
            .expect("top-ranked row should expose reasons");
        assert_eq!(
            rows.first()
                .and_then(|row| row.get("selected_action"))
                .and_then(Value::as_str),
            Some("press_dialog_button")
        );
        assert_eq!(
            result.get("router_hint").and_then(Value::as_str),
            Some("prefer_modal_chain_resolution")
        );
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("owner_chain_visible")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("same_root_owner_cluster:3")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("same_root_owner_dialog_cluster:2")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("active_owner_chain_depth:1")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("deeper_owner_chain_available:2")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("modal_chain_signature")));
        assert_eq!(
            result
                .get("native_same_root_owner_dialog_like_count")
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            result
                .get("native_modal_chain_signature")
                .and_then(Value::as_str),
            Some("5000|3|2|2")
        );
    }

    #[test]
    fn route_surface_exploration_prefers_descendant_dialog_chain_resolution() {
        let payload = json!({
            "query": "Confirm pairing",
            "current_dialog_visible": false,
            "current_reacquired_title": "Pair device",
            "native_direct_child_window_count": 1,
            "native_direct_child_dialog_like_count": 1,
            "native_descendant_chain_depth": 2,
            "native_descendant_dialog_chain_depth": 2,
            "native_descendant_query_match_count": 1,
            "native_descendant_chain_titles": ["Pair device", "Confirm pairing"],
            "native_child_chain_signature": "5001|1|2|Pair device|Confirm pairing",
            "selection_rows": [
                {
                    "selection_key": "hypothesis|row_previous|select_list_item|previous_page",
                    "kind": "hypothesis",
                    "candidate_id": "row_previous",
                    "label": "Previous page",
                    "selected_action": "select_list_item",
                    "confidence": 0.73
                },
                {
                    "selection_key": "hypothesis|dialog_confirm|press_dialog_button|confirm pairing",
                    "kind": "hypothesis",
                    "candidate_id": "dialog_confirm",
                    "label": "Confirm pairing",
                    "selected_action": "press_dialog_button",
                    "confidence": 0.64
                }
            ],
            "branch_history": [
                {
                    "transition_kind": "child_window_chain",
                    "selected_action": "press_dialog_button",
                    "selected_candidate_id": "dialog_continue",
                    "selected_candidate_label": "Continue",
                    "occurrences": 1
                }
            ]
        });

        let result = route_surface_exploration(&payload).expect("router payload should parse");
        let rows = result
            .get("ranked_candidates")
            .and_then(Value::as_array)
            .expect("ranked candidates should be present");
        let reasons = rows
            .first()
            .and_then(|row| row.get("reasons"))
            .and_then(Value::as_array)
            .expect("top-ranked row should expose reasons");
        assert_eq!(
            rows.first()
                .and_then(|row| row.get("selected_action"))
                .and_then(Value::as_str),
            Some("press_dialog_button")
        );
        assert_eq!(
            result.get("router_hint").and_then(Value::as_str),
            Some("prefer_descendant_dialog_chain")
        );
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("descendant_chain_overlap:2")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("descendant_chain_dialog_depth:2")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("child_chain_signature")));
    }

    #[test]
    fn route_surface_exploration_prefers_preferred_descendant_focus_adoption() {
        let payload = json!({
            "query": "Pair device",
            "current_window_title": "Bluetooth & devices",
            "current_reacquired_title": "Bluetooth & devices",
            "native_direct_child_window_count": 1,
            "native_descendant_chain_depth": 2,
            "native_descendant_dialog_chain_depth": 1,
            "native_descendant_query_match_count": 1,
            "native_descendant_adoption_available": true,
            "native_descendant_adoption_match_score": 0.84,
            "native_descendant_chain_titles": ["Pair device", "Confirm pairing"],
            "preferred_descendant_title": "Pair device",
            "preferred_descendant_hwnd": 5002,
            "native_child_chain_signature": "5001|1|2|Pair device|Confirm pairing",
            "selection_rows": [
                {
                    "selection_key": "branch_action|5002|focus_related_window|adopt child surface: pair device",
                    "kind": "branch_action",
                    "candidate_id": "5002",
                    "label": "Adopt child surface: Pair device",
                    "selected_action": "focus_related_window",
                    "confidence": 0.79
                },
                {
                    "selection_key": "branch_action||click|open bluetooth settings",
                    "kind": "branch_action",
                    "candidate_id": "",
                    "label": "Open Bluetooth settings",
                    "selected_action": "click",
                    "confidence": 0.76
                }
            ],
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "select_list_item",
                    "selected_candidate_id": "list_bluetooth",
                    "selected_candidate_label": "Bluetooth",
                    "window_title": "Bluetooth & devices",
                    "surface_path_tail": ["Devices", "Bluetooth"],
                    "occurrences": 1
                }
            ]
        });

        let result = route_surface_exploration(&payload).expect("router payload should parse");
        let rows = result
            .get("ranked_candidates")
            .and_then(Value::as_array)
            .expect("ranked candidates should be present");
        let reasons = rows
            .first()
            .and_then(|row| row.get("reasons"))
            .and_then(Value::as_array)
            .expect("top-ranked row should expose reasons");
        assert_eq!(
            rows.first()
                .and_then(|row| row.get("selected_action"))
                .and_then(Value::as_str),
            Some("focus_related_window")
        );
        assert_eq!(
            result.get("router_hint").and_then(Value::as_str),
            Some("prefer_descendant_surface_adoption")
        );
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("preferred_descendant_focus")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("preferred_descendant_child_chain")));
        assert!(reasons.iter().any(|value| {
            value
                .as_str()
                .map(|reason| reason.starts_with("native_descendant_adoption_ready:"))
                .unwrap_or(false)
        }));
    }

    #[test]
    fn route_surface_exploration_uses_benchmark_dialog_pressure_to_break_ties() {
        let payload = json!({
            "query": "Continue",
            "benchmark_ready": true,
            "benchmark_weakest_pack": "unsupported_and_recovery",
            "benchmark_dialog_pressure": 0.95,
            "selection_rows": [
                {
                    "selection_key": "branch_action||press_dialog_button|continue",
                    "kind": "branch_action",
                    "candidate_id": "",
                    "label": "Continue",
                    "selected_action": "press_dialog_button",
                    "confidence": 0.44
                },
                {
                    "selection_key": "hypothesis|settings_sidebar|select_sidebar_item|settings",
                    "kind": "hypothesis",
                    "candidate_id": "settings_sidebar",
                    "label": "Settings",
                    "selected_action": "select_sidebar_item",
                    "confidence": 0.53
                }
            ],
            "branch_history": [
                {
                    "transition_kind": "dialog_shift",
                    "selected_action": "press_dialog_button",
                    "selected_candidate_label": "Continue",
                    "occurrences": 1
                }
            ]
        });

        let result = route_surface_exploration(&payload).expect("router payload should parse");
        let rows = result
            .get("ranked_candidates")
            .and_then(Value::as_array)
            .expect("ranked candidates should be present");
        let reasons = rows
            .first()
            .and_then(|row| row.get("reasons"))
            .and_then(Value::as_array)
            .expect("top-ranked row should expose reasons");
        assert_eq!(
            rows.first()
                .and_then(|row| row.get("selected_action"))
                .and_then(Value::as_str),
            Some("press_dialog_button")
        );
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("benchmark_dialog_pressure:0.95")));
    }

    #[test]
    fn route_surface_exploration_uses_benchmark_target_app_pressure_for_descendant_focus() {
        let payload = json!({
            "query": "Pair device",
            "current_window_title": "Bluetooth & devices",
            "current_window_app_name": "settings",
            "current_reacquired_title": "Pair device",
            "current_reacquired_app_name": "settings",
            "native_descendant_chain_depth": 1,
            "native_descendant_dialog_chain_depth": 1,
            "native_descendant_chain_titles": ["Pair device", "Confirm pairing"],
            "preferred_descendant_title": "Pair device",
            "preferred_descendant_hwnd": 5002,
            "native_child_chain_signature": "5001|1|1|Pair device",
            "benchmark_target_app_name": "settings",
            "benchmark_target_app_matched": true,
            "benchmark_target_app_match_score": 1.0,
            "benchmark_target_query_hints": ["pair device", "confirm pairing"],
            "benchmark_target_descendant_title_hints": ["Pair device", "Confirm pairing"],
            "benchmark_target_descendant_hint_query": "pair device | confirm pairing",
            "benchmark_target_preferred_window_title": "Pair device",
            "benchmark_target_hint_query": "pair device | confirm pairing",
            "benchmark_target_priority": 2.4,
            "benchmark_target_max_horizon_steps": 5,
            "benchmark_target_replay_pressure": 1.65,
            "benchmark_target_replay_session_count": 1,
            "benchmark_target_replay_pending_count": 1,
            "benchmark_target_replay_failed_count": 1,
            "benchmark_target_replay_completed_count": 0,
            "benchmark_target_session_cycle_count": 3,
            "benchmark_target_regression_cycle_count": 2,
            "benchmark_target_long_horizon_pending_count": 1,
            "benchmark_target_descendant_focus_pressure": 0.94,
            "benchmark_target_native_focus_pressure": 0.91,
            "benchmark_target_reacquire_pressure": 0.88,
            "selection_rows": [
                {
                    "selection_key": "branch_action|5002|focus|adopt child surface: pair device",
                    "kind": "branch_action",
                    "candidate_id": "5002",
                    "label": "Adopt child surface: Pair device",
                    "selected_action": "focus",
                    "confidence": 0.72
                },
                {
                    "selection_key": "branch_action||click|open bluetooth settings",
                    "kind": "branch_action",
                    "candidate_id": "",
                    "label": "Open Bluetooth settings",
                    "selected_action": "click",
                    "confidence": 0.79
                }
            ],
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "select_list_item",
                    "selected_candidate_label": "Bluetooth",
                    "topology_branch_family_signature": "5000|2|Pair device",
                    "occurrences": 1
                }
            ]
        });

        let result = route_surface_exploration(&payload).expect("router payload should parse");
        let rows = result
            .get("ranked_candidates")
            .and_then(Value::as_array)
            .expect("ranked candidates should be present");
        let reasons = rows
            .first()
            .and_then(|row| row.get("reasons"))
            .and_then(Value::as_array)
            .expect("top-ranked row should expose reasons");
        assert_eq!(
            rows.first()
                .and_then(|row| row.get("selected_action"))
                .and_then(Value::as_str),
            Some("focus")
        );
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("benchmark_target_descendant_focus_pressure:0.94")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("benchmark_target_exact_app_match")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("benchmark_target_replay_pressure:1.65")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("benchmark_target_descendant_hint:4")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("benchmark_target_regression_cycles:2")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("benchmark_target_long_horizon_pending:1")));
    }

    #[test]
    fn route_surface_exploration_prefers_branch_family_dialog_continuity() {
        let payload = json!({
            "query": "OK",
            "current_dialog_visible": true,
            "native_owner_chain_visible": true,
            "native_modal_chain_signature": "2410|2|2|Pair device|Confirm pairing",
            "native_branch_family_signature": "2410|2|Bluetooth & devices|Pair device",
            "branch_family_repeat_count": 2,
            "branch_family_switch_count": 0,
            "branch_family_continuity": true,
            "selection_rows": [
                {
                    "selection_key": "hypothesis|dialog_ok|press_dialog_button|ok",
                    "kind": "hypothesis",
                    "candidate_id": "dialog_ok",
                    "label": "OK",
                    "selected_action": "press_dialog_button",
                    "confidence": 0.63
                },
                {
                    "selection_key": "hypothesis|row_previous|select_list_item|previous_page",
                    "kind": "hypothesis",
                    "candidate_id": "row_previous",
                    "label": "Previous page",
                    "selected_action": "select_list_item",
                    "confidence": 0.68
                }
            ],
            "branch_history": [
                {
                    "transition_kind": "child_window_chain",
                    "selected_action": "press_dialog_button",
                    "selected_candidate_id": "dialog_continue",
                    "selected_candidate_label": "Continue",
                    "window_title": "Add a device",
                    "surface_path_tail": ["Devices", "Bluetooth", "Add a device"],
                    "topology_branch_family_signature": "2410|2|Bluetooth & devices|Pair device",
                    "occurrences": 1
                },
                {
                    "transition_kind": "dialog_shift",
                    "selected_action": "press_dialog_button",
                    "selected_candidate_id": "dialog_confirm",
                    "selected_candidate_label": "Confirm",
                    "window_title": "Pair device",
                    "surface_path_tail": ["Devices", "Bluetooth", "Pair device"],
                    "topology_branch_family_signature": "2410|2|Bluetooth & devices|Pair device",
                    "occurrences": 1
                }
            ]
        });

        let result = route_surface_exploration(&payload).expect("router payload should parse");
        let rows = result
            .get("ranked_candidates")
            .and_then(Value::as_array)
            .expect("ranked candidates should be present");
        let reasons = rows
            .first()
            .and_then(|row| row.get("reasons"))
            .and_then(Value::as_array)
            .expect("top-ranked row should expose reasons");
        assert_eq!(
            rows.first()
                .and_then(|row| row.get("selected_action"))
                .and_then(Value::as_str),
            Some("press_dialog_button")
        );
        assert_eq!(
            result.get("router_hint").and_then(Value::as_str),
            Some("prefer_branch_family_dialog")
        );
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("branch_family_dialog_continuity:2")));
    }

    #[test]
    fn route_surface_exploration_uses_campaign_pressure_for_descendant_focus() {
        let payload = json!({
            "query": "Confirm pairing",
            "current_window_title": "Bluetooth & devices",
            "current_window_app_name": "settings",
            "current_reacquired_title": "Pair device",
            "current_reacquired_app_name": "settings",
            "preferred_descendant_title": "Confirm pairing",
            "preferred_descendant_hwnd": 5003,
            "benchmark_target_app_name": "settings",
            "benchmark_target_app_matched": true,
            "benchmark_target_app_match_score": 1.0,
            "benchmark_target_campaign_pressure": 1.9,
            "benchmark_target_campaign_count": 1,
            "benchmark_target_campaign_sweep_count": 2,
            "benchmark_target_campaign_attention_session_count": 1,
            "benchmark_target_campaign_pending_app_target_count": 1,
            "benchmark_target_campaign_regression_cycle_count": 2,
            "benchmark_target_campaign_long_horizon_pending_count": 1,
            "benchmark_target_campaign_hint_query": "pair device | confirm pairing",
            "benchmark_target_campaign_descendant_title_hints": ["Pair device", "Confirm pairing"],
            "benchmark_target_campaign_descendant_hint_query": "pair device | confirm pairing",
            "benchmark_target_campaign_preferred_window_title": "Confirm pairing",
            "benchmark_target_campaign_latest_sweep_regression_status": "regression",
            "selection_rows": [
                {
                    "selection_key": "branch_action|5003|focus|adopt child surface: confirm pairing",
                    "kind": "branch_action",
                    "candidate_id": "5003",
                    "label": "Adopt child surface: Confirm pairing",
                    "selected_action": "focus",
                    "confidence": 0.74
                },
                {
                    "selection_key": "branch_action||click|back to bluetooth",
                    "kind": "branch_action",
                    "candidate_id": "",
                    "label": "Back to Bluetooth",
                    "selected_action": "click",
                    "confidence": 0.8
                }
            ],
            "branch_history": [
                {
                    "transition_kind": "child_window_chain",
                    "selected_action": "press_dialog_button",
                    "selected_candidate_id": "dialog_continue",
                    "selected_candidate_label": "Continue",
                    "window_title": "Pair device",
                    "occurrences": 1
                }
            ]
        });

        let result = route_surface_exploration(&payload).expect("router payload should parse");
        let rows = result
            .get("ranked_candidates")
            .and_then(Value::as_array)
            .expect("ranked candidates should be present");
        let reasons = rows
            .first()
            .and_then(|row| row.get("reasons"))
            .and_then(Value::as_array)
            .expect("top-ranked row should expose reasons");
        assert_eq!(
            rows.first()
                .and_then(|row| row.get("selected_action"))
                .and_then(Value::as_str),
            Some("focus")
        );
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("benchmark_campaign_pressure:1.90")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("benchmark_campaign_descendant_hint:4")));
        assert!(reasons
            .iter()
            .any(|value| value.as_str() == Some("benchmark_campaign_regression:2")));
    }
}
