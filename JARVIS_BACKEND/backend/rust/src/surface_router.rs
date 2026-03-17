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
    pub current_surface_path: Vec<String>,
    #[serde(default)]
    pub current_dialog_visible: bool,
    #[serde(default)]
    pub current_reacquired_title: String,
    #[serde(default)]
    pub current_reacquired_hwnd: u64,
    #[serde(default)]
    pub native_same_process_window_count: u32,
    #[serde(default)]
    pub native_related_window_count: u32,
    #[serde(default)]
    pub native_child_dialog_like_visible: bool,
    #[serde(default)]
    pub native_topology_signature: String,
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
    for token in ["|", "-", "_", "/", "\\", "(", ")", "[", "]", "{", "}", ".", ",", ":"] {
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

fn build_topology_signature(rows: &[String], active_title: &str, visible_count: usize, dialog_count: usize) -> String {
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
    let active_title = active_window
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("");

    let mut dialog_like_count = 0usize;
    let mut same_process_window_count = 0usize;
    let mut query_window_match_count = 0usize;
    let mut window_title_tail: Vec<String> = Vec::new();
    let mut class_name_tail: Vec<String> = Vec::new();
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
            && row
                .get("process_id")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                == active_pid
        {
            same_process_window_count += 1;
        }
        let title = row.get("title").and_then(Value::as_str).unwrap_or("").trim();
        if !title.is_empty() {
            window_title_tail.push(title.to_string());
            let title_tokens = tokenize(title);
            if !query_tokens.is_empty() && token_overlap(&query_tokens, &title_tokens) > 0 {
                query_window_match_count += 1;
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

    Ok(json!({
        "status": "success",
        "query": clean_query,
        "query_tokens": query_tokens,
        "active_window": active_window,
        "visible_window_count": windows.len(),
        "dialog_like_count": dialog_like_count,
        "same_process_window_count": same_process_window_count,
        "query_window_match_count": query_window_match_count,
        "window_title_tail": window_title_tail.into_iter().take(8).collect::<Vec<_>>(),
        "class_name_tail": class_name_tail.into_iter().take(8).collect::<Vec<_>>(),
        "windows": sample_windows,
        "topology_signature": topology_signature,
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
    let current_reacquired_tokens = tokenize(&input.current_reacquired_title);
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
    let native_child_dialog_visible = input.native_child_dialog_like_visible
        || topology
            .get("dialog_like_count")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            > 0;
    let latest_branch = input.branch_history.last().cloned().unwrap_or_default();
    let latest_transition = normalize_text(&latest_branch.transition_kind);
    let latest_occurrences = latest_branch.occurrences.max(1);
    let prefer_nested_branch = matches!(
        latest_transition.as_str(),
        "child_window" | "dialog_shift" | "drilldown" | "pane_shift"
    );

    let mut ranked_candidates: Vec<Value> = Vec::new();
    for row in &input.selection_rows {
        let label_tokens = tokenize(&row.label);
        let selected_action = normalize_text(&row.selected_action);
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
                "select_sidebar_item" | "select_tab_page" | "select_list_item" | "focus_input_field"
            )
        {
            rust_score += (path_overlap as f64 * 0.03).min(0.09);
            reasons.push(format!("path_continuation:{path_overlap}"));
        }

        let window_overlap = token_overlap(&current_window_tokens, &label_tokens)
            + token_overlap(&topology_window_tokens, &label_tokens);
        if window_overlap > 0
            && matches!(selected_action.as_str(), "click" | "press_dialog_button" | "select_tab_page")
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

        if input.current_dialog_visible || active_dialog_visible || native_child_dialog_visible {
            if selected_action == "press_dialog_button" {
                rust_score += 0.16;
                reasons.push("dialog_resolution_visible".to_string());
            } else {
                rust_score -= 0.05;
            }
        }
        if native_child_dialog_visible && matches!(latest_transition.as_str(), "child_window" | "dialog_shift") {
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
                        "select_sidebar_item" | "select_tab_page" | "select_list_item" | "focus_input_field"
                    ) {
                        rust_score += 0.1;
                        reasons.push(format!("nested_navigation_branch:{latest_transition}"));
                    }
                }
                _ => {}
            }
        }
        if native_same_process_window_count > 1 && selected_action == "press_dialog_button" {
            rust_score += 0.05;
            reasons.push(format!("same_process_cluster:{}", native_same_process_window_count));
        }
        if native_related_window_count > 1
            && matches!(
                selected_action.as_str(),
                "select_sidebar_item" | "select_tab_page" | "select_list_item" | "select_tree_item"
            )
        {
            rust_score += 0.03;
            reasons.push(format!("related_window_cluster:{}", native_related_window_count));
        }

        if latest_occurrences >= 2
            && !latest_branch.selected_action.is_empty()
            && normalize_text(&latest_branch.selected_action) == selected_action
            && ((!latest_branch.selected_candidate_id.is_empty() && latest_branch.selected_candidate_id == row.candidate_id)
                || (!latest_branch.selected_candidate_label.is_empty()
                    && normalize_text(&latest_branch.selected_candidate_label) == normalize_text(&row.label)))
        {
            rust_score -= 0.24;
            reasons.push(format!("repeat_branch_penalty:{latest_occurrences}"));
        } else if latest_occurrences >= 2 && kind == "branch_action" {
            rust_score -= 0.08;
            reasons.push("branch_repeat_pressure".to_string());
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

        let router_hint = if (input.current_dialog_visible || active_dialog_visible)
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
        let left_total = left.get("total_score").and_then(Value::as_f64).unwrap_or(0.0);
        let right_total = right.get("total_score").and_then(Value::as_f64).unwrap_or(0.0);
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
        assert!(reasons.iter().any(|value| value.as_str() == Some("repeat_branch_penalty:2")));
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
        assert!(reasons.iter().any(|value| value.as_str() == Some("native_child_dialog_cluster")));
    }
}
