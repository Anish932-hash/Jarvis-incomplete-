from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List


class DesktopAppMemory:
    _DEFAULT_INSTANCE: "DesktopAppMemory | None" = None
    _DEFAULT_LOCK = RLock()

    def __init__(
        self,
        *,
        store_path: str = "data/desktop_app_memory.json",
        max_entries: int = 2500,
        max_controls_per_entry: int = 320,
        max_history_per_entry: int = 10,
    ) -> None:
        self.store_path = Path(store_path)
        self.max_entries = self._coerce_int(max_entries, minimum=100, maximum=100_000, default=2500)
        self.max_controls_per_entry = self._coerce_int(max_controls_per_entry, minimum=32, maximum=2000, default=320)
        self.max_history_per_entry = self._coerce_int(max_history_per_entry, minimum=2, maximum=64, default=10)
        self._lock = RLock()
        self._entries: Dict[str, Dict[str, Any]] = {}
        self._updates_since_save = 0
        self._last_save_monotonic = 0.0
        self._load()

    @classmethod
    def default(cls) -> "DesktopAppMemory":
        with cls._DEFAULT_LOCK:
            if cls._DEFAULT_INSTANCE is None:
                cls._DEFAULT_INSTANCE = cls()
            return cls._DEFAULT_INSTANCE

    def record_survey(
        self,
        *,
        app_name: str = "",
        window_title: str = "",
        query: str = "",
        app_profile: Dict[str, Any] | None = None,
        launch_result: Dict[str, Any] | None = None,
        snapshot: Dict[str, Any] | None = None,
        exploration_plan: Dict[str, Any] | None = None,
        probe_report: Dict[str, Any] | None = None,
        survey_status: str = "success",
        error_message: str = "",
        source: str = "manual",
    ) -> Dict[str, Any]:
        profile = dict(app_profile) if isinstance(app_profile, dict) else {}
        launch_payload = dict(launch_result) if isinstance(launch_result, dict) else {}
        snapshot_payload = dict(snapshot) if isinstance(snapshot, dict) else {}
        exploration_payload = dict(exploration_plan) if isinstance(exploration_plan, dict) else {}
        probe_payload = dict(probe_report) if isinstance(probe_report, dict) else {}
        target_window = (
            snapshot_payload.get("target_window", {})
            if isinstance(snapshot_payload.get("target_window", {}), dict)
            else {}
        )
        active_window = (
            snapshot_payload.get("active_window", {})
            if isinstance(snapshot_payload.get("active_window", {}), dict)
            else {}
        )
        summary = (
            snapshot_payload.get("surface_summary", {})
            if isinstance(snapshot_payload.get("surface_summary", {}), dict)
            else {}
        )
        observation_payload = (
            snapshot_payload.get("observation", {})
            if isinstance(snapshot_payload.get("observation", {}), dict)
            else {}
        )
        intelligence = (
            snapshot_payload.get("surface_intelligence", {})
            if isinstance(snapshot_payload.get("surface_intelligence", {}), dict)
            else {}
        )
        native_window_topology = (
            snapshot_payload.get("native_window_topology", {})
            if isinstance(snapshot_payload.get("native_window_topology", {}), dict)
            else {}
        )
        window_reacquisition = (
            snapshot_payload.get("window_reacquisition", {})
            if isinstance(snapshot_payload.get("window_reacquisition", {}), dict)
            else {}
        )
        elements_payload = (
            snapshot_payload.get("elements", {})
            if isinstance(snapshot_payload.get("elements", {}), dict)
            else {}
        )
        element_rows = [
            dict(row)
            for row in elements_payload.get("items", [])
            if isinstance(row, dict)
        ]
        app_label = self._display_app_name(
            explicit_app_name=app_name,
            explicit_window_title=window_title,
            app_profile=profile,
            target_window=target_window,
            active_window=active_window,
            launch_result=launch_payload,
        )
        key = self._entry_key(
            app_name=app_label,
            app_profile=profile,
            target_window=target_window,
            active_window=active_window,
        )
        now = datetime.now(timezone.utc).isoformat()
        clean_query = str(query or "").strip()
        clean_survey_status = self._normalize_text(survey_status) or "success"
        clean_error_message = str(error_message or snapshot_payload.get("message", "") or "").strip()
        clean_source = self._normalize_text(source) or "manual"
        surface_fingerprint = str(snapshot_payload.get("surface_fingerprint", "") or "").strip() or self._surface_fingerprint(
            app_name=app_label,
            profile_id=self._profile_id(profile),
            target_window=target_window,
            active_window=active_window,
            summary=summary,
            intelligence=intelligence,
            observation=observation_payload,
        )
        with self._lock:
            entry = dict(self._entries.get(key, {}))
            entry["key"] = key
            entry["app_name"] = app_label
            entry["profile_id"] = self._profile_id(profile)
            entry["profile_name"] = str(profile.get("name", "") or "").strip()
            entry["category"] = self._normalize_text(profile.get("category", ""))
            entry["window_title"] = (
                str(target_window.get("title", "") or active_window.get("title", "") or window_title or "").strip()
            )
            entry["updated_at"] = now
            entry["last_surface_fingerprint"] = surface_fingerprint

            metrics = entry.get("metrics", {}) if isinstance(entry.get("metrics", {}), dict) else {}
            metrics["survey_count"] = self._coerce_int(metrics.get("survey_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            if clean_survey_status in {"success", "partial"}:
                metrics["survey_success_count"] = self._coerce_int(metrics.get("survey_success_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            else:
                metrics["survey_failure_count"] = self._coerce_int(metrics.get("survey_failure_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            metrics["launch_attempt_count"] = self._coerce_int(metrics.get("launch_attempt_count", 0), minimum=0, maximum=10_000_000, default=0) + (1 if launch_payload else 0)
            if str(launch_payload.get("status", "") or "").strip().lower() == "success":
                metrics["launch_success_count"] = self._coerce_int(metrics.get("launch_success_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            elif launch_payload:
                metrics["launch_failure_count"] = self._coerce_int(metrics.get("launch_failure_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            if element_rows:
                metrics["surface_success_count"] = self._coerce_int(metrics.get("surface_success_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            metrics["element_observation_count"] = self._coerce_int(metrics.get("element_observation_count", 0), minimum=0, maximum=10_000_000, default=0) + len(element_rows)
            metrics["control_inventory_count"] = self._coerce_int(metrics.get("control_inventory_count", 0), minimum=0, maximum=10_000_000, default=0) + len([row for row in summary.get("control_inventory", []) if isinstance(row, dict)])
            metrics["query_candidate_count"] = self._coerce_int(metrics.get("query_candidate_count", 0), minimum=0, maximum=10_000_000, default=0) + len([row for row in summary.get("query_candidates", []) if isinstance(row, dict)])
            metrics["workflow_surface_count"] = self._coerce_int(metrics.get("workflow_surface_count", 0), minimum=0, maximum=10_000_000, default=0) + len([row for row in snapshot_payload.get("workflow_surfaces", []) if isinstance(row, dict)])
            metrics["branch_action_count"] = self._coerce_int(metrics.get("branch_action_count", 0), minimum=0, maximum=10_000_000, default=0) + len([row for row in exploration_payload.get("branch_actions", []) if isinstance(row, dict)])
            metrics["top_hypothesis_count"] = self._coerce_int(metrics.get("top_hypothesis_count", 0), minimum=0, maximum=10_000_000, default=0) + len([row for row in exploration_payload.get("top_hypotheses", []) if isinstance(row, dict)])
            metrics["ocr_target_count"] = self._coerce_int(metrics.get("ocr_target_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("ocr_target_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["probe_attempt_count"] = self._coerce_int(metrics.get("probe_attempt_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("attempted_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["probe_success_count"] = self._coerce_int(metrics.get("probe_success_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("successful_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["probe_blocked_count"] = self._coerce_int(metrics.get("probe_blocked_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["probe_error_count"] = self._coerce_int(metrics.get("probe_error_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("error_count", 0), minimum=0, maximum=10_000_000, default=0)
            if clean_source == "daemon":
                metrics["background_survey_count"] = self._coerce_int(metrics.get("background_survey_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            elif clean_source == "batch":
                metrics["batch_survey_count"] = self._coerce_int(metrics.get("batch_survey_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            else:
                metrics["manual_survey_count"] = self._coerce_int(metrics.get("manual_survey_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            entry["metrics"] = metrics

            self._increment_count(entry.setdefault("window_title_counts", {}), str(entry.get("window_title", "") or "").strip())
            self._increment_count(entry.setdefault("surface_role_counts", {}), str(intelligence.get("surface_role", "") or "").strip())
            self._increment_count(entry.setdefault("interaction_mode_counts", {}), str(intelligence.get("interaction_mode", "") or "").strip())
            self._increment_count(entry.setdefault("survey_status_counts", {}), clean_survey_status)
            self._increment_count(entry.setdefault("survey_source_counts", {}), clean_source)
            self._increment_count(entry.setdefault("surface_fingerprint_counts", {}), surface_fingerprint)
            self._increment_count(
                entry.setdefault("surface_signature_counts", {}),
                str(native_window_topology.get("signature", "") or target_window.get("window_signature", "") or active_window.get("window_signature", "") or "").strip(),
            )
            if clean_error_message:
                self._increment_count(entry.setdefault("failure_reason_counts", {}), clean_error_message)
            entry["last_survey_status"] = clean_survey_status
            entry["last_survey_source"] = clean_source
            entry["last_error_message"] = clean_error_message
            entry["last_probe_summary"] = {
                "attempted_count": self._coerce_int(probe_payload.get("attempted_count", 0), minimum=0, maximum=10_000_000, default=0),
                "successful_count": self._coerce_int(probe_payload.get("successful_count", 0), minimum=0, maximum=10_000_000, default=0),
                "blocked_count": self._coerce_int(probe_payload.get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0),
                "error_count": self._coerce_int(probe_payload.get("error_count", 0), minimum=0, maximum=10_000_000, default=0),
                "ocr_target_count": self._coerce_int(probe_payload.get("ocr_target_count", 0), minimum=0, maximum=10_000_000, default=0),
                "candidate_count": self._coerce_int(probe_payload.get("candidate_count", 0), minimum=0, maximum=10_000_000, default=0),
                "status": str(probe_payload.get("status", "") or "").strip(),
                "updated_at": now,
            }

            for label_row in summary.get("top_labels", []):
                if not isinstance(label_row, dict):
                    continue
                label = str(label_row.get("label", "") or "").strip()
                count = self._coerce_int(label_row.get("count", 1), minimum=1, maximum=1000, default=1)
                for _ in range(count):
                    self._increment_count(entry.setdefault("top_label_counts", {}), label)

            for control_type, count in self._normalize_count_map(summary.get("control_counts", {})).items():
                current = entry.setdefault("control_type_counts", {})
                current[control_type] = self._coerce_int(current.get(control_type, 0), minimum=0, maximum=10_000_000, default=0) + count

            for row in element_rows:
                self._record_control(entry=entry, row=row, observed_at=now, query=clean_query)
                self._record_command_harvest(
                    entry=entry,
                    label=str(row.get("name", "") or row.get("automation_id", "") or "").strip(),
                    control_type=str(row.get("control_type", "") or "").strip(),
                    source="element",
                    hotkeys=[
                        str(row.get("accelerator_key", "") or "").strip(),
                        str(row.get("access_key", "") or "").strip(),
                    ],
                    aliases=self._control_aliases(row),
                )

            for row in summary.get("query_candidates", []):
                if not isinstance(row, dict):
                    continue
                self._increment_count(entry.setdefault("command_candidate_counts", {}), self._candidate_label(row))
                self._record_command_harvest(
                    entry=entry,
                    label=self._candidate_label(row),
                    control_type=str(row.get("control_type", "") or "").strip(),
                    source="query_candidate",
                    aliases=[
                        str(row.get("name", "") or "").strip(),
                        str(row.get("automation_id", "") or "").strip(),
                        str(row.get("label", "") or "").strip(),
                    ],
                )

            for action_name in snapshot_payload.get("recommended_actions", []):
                self._increment_count(entry.setdefault("recommended_action_counts", {}), action_name)
            for action_name in summary.get("recommended_actions", []):
                self._increment_count(entry.setdefault("recommended_action_counts", {}), action_name)

            for action_name in summary.get("confirmation_candidates", []):
                self._increment_count(entry.setdefault("confirmation_candidate_counts", {}), action_name)
            for action_name in summary.get("destructive_candidates", []):
                self._increment_count(entry.setdefault("destructive_candidate_counts", {}), action_name)

            for workflow in snapshot_payload.get("workflow_surfaces", []):
                if not isinstance(workflow, dict):
                    continue
                action_name = str(workflow.get("action", "") or "").strip()
                self._increment_count(entry.setdefault("workflow_action_counts", {}), action_name)
                hotkeys = [
                    str(item).strip()
                    for item in workflow.get("primary_hotkey", [])
                    if str(item).strip()
                ] if isinstance(workflow.get("primary_hotkey", []), list) else []
                if hotkeys:
                    shortcut_actions = entry.setdefault("shortcut_actions", {})
                    shortcut_state = shortcut_actions.get(action_name, {}) if isinstance(shortcut_actions.get(action_name, {}), dict) else {}
                    shortcut_state = dict(shortcut_state)
                    shortcut_state["action"] = action_name
                    shortcut_state["sample_count"] = self._coerce_int(shortcut_state.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
                    known = [str(item).strip() for item in shortcut_state.get("hotkeys", []) if str(item).strip()]
                    for hotkey in hotkeys:
                        if hotkey not in known:
                            known.append(hotkey)
                    shortcut_state["hotkeys"] = known[:12]
                    shortcut_actions[action_name] = shortcut_state
                    self._record_command_harvest(
                        entry=entry,
                        label=action_name.replace("_", " "),
                        control_type="workflow_action",
                        source="workflow",
                        hotkeys=hotkeys,
                        aliases=[action_name, str(workflow.get("title", "") or "").strip()],
                    )

            for branch in exploration_payload.get("branch_actions", []):
                if not isinstance(branch, dict):
                    continue
                self._increment_count(entry.setdefault("branch_action_counts", {}), str(branch.get("action", "") or branch.get("branch_action", "") or branch.get("label", "") or "").strip())
            for hypothesis in exploration_payload.get("top_hypotheses", []):
                if not isinstance(hypothesis, dict):
                    continue
                self._increment_count(entry.setdefault("exploration_target_counts", {}), str(hypothesis.get("label", "") or hypothesis.get("target_name", "") or "").strip())
            for probe_item in probe_payload.get("items", []):
                if not isinstance(probe_item, dict):
                    continue
                self._record_probe_result(
                    entry=entry,
                    row=probe_item,
                    observed_at=now,
                    default_surface_fingerprint=surface_fingerprint,
                )

            self._record_surface_node(
                entry=entry,
                observed_at=now,
                surface_fingerprint=surface_fingerprint,
                snapshot_payload=snapshot_payload,
                app_profile=profile,
                probe_payload=probe_payload,
            )
            self._record_capability_profile(
                entry=entry,
                summary=summary,
                intelligence=intelligence,
                workflow_surfaces=snapshot_payload.get("workflow_surfaces", []),
                probe_payload=probe_payload,
                observation=observation_payload,
            )

            native_summary = entry.get("native_summary", {}) if isinstance(entry.get("native_summary", {}), dict) else {}
            native_summary["last_signature"] = str(native_window_topology.get("signature", "") or "").strip()
            native_summary["max_descendant_chain_depth"] = max(self._coerce_int(native_summary.get("max_descendant_chain_depth", 0), minimum=0, maximum=1000, default=0), self._coerce_int(native_window_topology.get("descendant_chain_depth", window_reacquisition.get("descendant_chain_depth", 0)), minimum=0, maximum=1000, default=0))
            native_summary["max_descendant_dialog_chain_depth"] = max(self._coerce_int(native_summary.get("max_descendant_dialog_chain_depth", 0), minimum=0, maximum=1000, default=0), self._coerce_int(native_window_topology.get("descendant_dialog_chain_depth", window_reacquisition.get("descendant_dialog_chain_depth", 0)), minimum=0, maximum=1000, default=0))
            native_summary["max_same_process_window_count"] = max(self._coerce_int(native_summary.get("max_same_process_window_count", 0), minimum=0, maximum=10_000, default=0), self._coerce_int(native_window_topology.get("same_process_window_count", 0), minimum=0, maximum=10_000, default=0))
            native_summary["max_related_window_count"] = max(self._coerce_int(native_summary.get("max_related_window_count", 0), minimum=0, maximum=10_000, default=0), self._coerce_int(native_window_topology.get("related_window_count", 0), minimum=0, maximum=10_000, default=0))
            native_summary["max_dialog_like_window_count"] = max(self._coerce_int(native_summary.get("max_dialog_like_window_count", 0), minimum=0, maximum=10_000, default=0), self._coerce_int(native_window_topology.get("dialog_like_window_count", 0), minimum=0, maximum=10_000, default=0))
            native_summary["last_reacquired_title"] = str(dict(window_reacquisition.get("candidate", {})).get("title", "") if isinstance(window_reacquisition.get("candidate", {}), dict) else "").strip()
            native_summary["updated_at"] = now
            entry["native_summary"] = native_summary

            survey_record = {
                "recorded_at": now,
                "status": clean_survey_status,
                "source": clean_source,
                "error_message": clean_error_message,
                "query": clean_query,
                "window_title": str(entry.get("window_title", "") or "").strip(),
                "launch_status": str(launch_payload.get("status", "") or "").strip(),
                "launch_method": str(launch_payload.get("launch_method", "") or launch_payload.get("resolution", "") or "").strip(),
                "element_count": len(element_rows),
                "surface_role": str(intelligence.get("surface_role", "") or "").strip(),
                "interaction_mode": str(intelligence.get("interaction_mode", "") or "").strip(),
                "surface_fingerprint": surface_fingerprint,
                "recommended_actions": [str(item).strip() for item in snapshot_payload.get("recommended_actions", []) if str(item).strip()][:8],
                "command_candidates": self._top_count_rows(entry.get("command_candidate_counts", {}), limit=6),
                "top_controls": self._top_controls(entry.get("controls", {}), limit=6),
                "surface_nodes": self._top_surface_nodes(entry.get("surface_nodes", {}), limit=4),
                "surface_transitions": self._top_surface_transitions(entry.get("surface_transitions", {}), limit=4),
                "learned_commands": self._top_commands(entry.get("learned_commands", {}), limit=8),
                "capability_profile": self._capability_profile_snapshot(entry),
                "branch_actions": self._top_count_rows(entry.get("branch_action_counts", {}), limit=4),
                "exploration_targets": self._top_count_rows(entry.get("exploration_target_counts", {}), limit=4),
                "probe_summary": {
                    "attempted_count": self._coerce_int(probe_payload.get("attempted_count", 0), minimum=0, maximum=10_000_000, default=0),
                    "successful_count": self._coerce_int(probe_payload.get("successful_count", 0), minimum=0, maximum=10_000_000, default=0),
                    "blocked_count": self._coerce_int(probe_payload.get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0),
                    "error_count": self._coerce_int(probe_payload.get("error_count", 0), minimum=0, maximum=10_000_000, default=0),
                    "ocr_target_count": self._coerce_int(probe_payload.get("ocr_target_count", 0), minimum=0, maximum=10_000_000, default=0),
                },
                "native_summary": dict(native_summary),
            }
            survey_history = [dict(item) for item in entry.get("survey_history", []) if isinstance(item, dict)]
            survey_history.append(survey_record)
            entry["survey_history"] = survey_history[-self.max_history_per_entry :]

            self._trim_entry_locked(entry)
            self._entries[key] = entry
            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=False)
            return self._snapshot_item(dict(entry))

    def snapshot(
        self,
        *,
        limit: int = 200,
        app_name: str = "",
        profile_id: str = "",
        category: str = "",
    ) -> Dict[str, Any]:
        bounded = self._coerce_int(limit, minimum=1, maximum=5000, default=200)
        clean_app_name = self._normalize_text(app_name)
        clean_profile_id = self._normalize_text(profile_id)
        clean_category = self._normalize_text(category)
        with self._lock:
            rows = [dict(row) for row in self._entries.values()]
        if clean_app_name:
            rows = [
                row
                for row in rows
                if clean_app_name in self._normalize_text(row.get("app_name", ""))
                or clean_app_name in self._normalize_text(row.get("window_title", ""))
                or clean_app_name in self._normalize_text(row.get("profile_name", ""))
            ]
        if clean_profile_id:
            rows = [row for row in rows if self._normalize_text(row.get("profile_id", "")) == clean_profile_id]
        if clean_category:
            rows = [row for row in rows if self._normalize_text(row.get("category", "")) == clean_category]
        rows.sort(key=lambda row: str(row.get("updated_at", "")), reverse=True)
        items = [self._snapshot_item(row) for row in rows[:bounded]]
        return {
            "status": "success",
            "count": min(len(rows), bounded),
            "total": len(rows),
            "items": items,
            "latest_entry": items[0] if items else {},
            "filters": {
                "app_name": clean_app_name,
                "profile_id": clean_profile_id,
                "category": clean_category,
            },
            "summary": self._snapshot_summary(rows),
        }

    def reset(
        self,
        *,
        app_name: str = "",
        profile_id: str = "",
        category: str = "",
    ) -> Dict[str, Any]:
        clean_app_name = self._normalize_text(app_name)
        clean_profile_id = self._normalize_text(profile_id)
        clean_category = self._normalize_text(category)
        with self._lock:
            removed = 0
            if not any((clean_app_name, clean_profile_id, clean_category)):
                removed = len(self._entries)
                self._entries = {}
            else:
                kept: Dict[str, Dict[str, Any]] = {}
                for key, row in self._entries.items():
                    app_match = bool(clean_app_name) and (
                        clean_app_name in self._normalize_text(row.get("app_name", ""))
                        or clean_app_name in self._normalize_text(row.get("window_title", ""))
                        or clean_app_name in self._normalize_text(row.get("profile_name", ""))
                    )
                    profile_match = bool(clean_profile_id) and self._normalize_text(row.get("profile_id", "")) == clean_profile_id
                    category_match = bool(clean_category) and self._normalize_text(row.get("category", "")) == clean_category
                    if app_match or profile_match or category_match:
                        removed += 1
                        continue
                    kept[key] = row
                self._entries = kept
            self._maybe_save_locked(force=True)
        return {
            "status": "success",
            "removed": removed,
            "filters": {
                "app_name": clean_app_name,
                "profile_id": clean_profile_id,
                "category": clean_category,
            },
        }

    def _record_control(self, *, entry: Dict[str, Any], row: Dict[str, Any], observed_at: str, query: str) -> None:
        identity = self._control_identity(row)
        if not identity:
            return
        controls = entry.setdefault("controls", {})
        current = controls.get(identity, {}) if isinstance(controls.get(identity, {}), dict) else {}
        current["identity"] = identity
        current["label"] = str(row.get("name", "") or row.get("automation_id", "") or "").strip()
        current["control_type"] = self._normalize_text(row.get("control_type", "")) or "unknown"
        current["automation_id"] = str(row.get("automation_id", "") or "").strip()
        current["element_id"] = str(row.get("element_id", "") or "").strip()
        current["class_name"] = str(row.get("class_name", "") or "").strip()
        current["sample_count"] = self._coerce_int(current.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        current["last_seen_at"] = observed_at
        current["root_window_title"] = str(row.get("root_window_title", "") or row.get("window_title", "") or "").strip()
        current["state_text"] = str(row.get("state_text", "") or "").strip()
        current["access_key"] = str(row.get("access_key", "") or "").strip()
        current["accelerator_key"] = str(row.get("accelerator_key", "") or "").strip()
        current["command_aliases"] = self._merge_recent_strings(
            current.get("command_aliases", []),
            self._control_aliases(row),
            limit=12,
        )
        current["label_variants"] = self._merge_recent_strings(
            current.get("label_variants", []),
            [str(row.get("name", "") or "").strip(), str(row.get("automation_id", "") or "").strip()],
            limit=8,
        )
        current["state_samples"] = self._merge_recent_strings(
            current.get("state_samples", []),
            [str(row.get("state_text", "") or "").strip()],
            limit=6,
        )
        seen_queries = [str(item).strip() for item in current.get("query_examples", []) if str(item).strip()]
        if query and query not in seen_queries:
            seen_queries.append(query)
        current["query_examples"] = seen_queries[-6:]
        controls[identity] = current

    def _record_probe_result(
        self,
        *,
        entry: Dict[str, Any],
        row: Dict[str, Any],
        observed_at: str,
        default_surface_fingerprint: str = "",
    ) -> None:
        identity = self._control_identity(row) or self._normalize_text(row.get("label", "") or row.get("query", "") or row.get("expected_text", ""))
        if not identity:
            return
        controls = entry.setdefault("controls", {})
        current = controls.get(identity, {}) if isinstance(controls.get(identity, {}), dict) else {}
        current["identity"] = identity
        current["label"] = str(row.get("label", "") or current.get("label", "") or "").strip()
        current["control_type"] = self._normalize_text(row.get("control_type", "") or current.get("control_type", "")) or "unknown"
        current["element_id"] = str(row.get("element_id", "") or current.get("element_id", "") or "").strip()
        current["automation_id"] = str(row.get("automation_id", "") or current.get("automation_id", "") or "").strip()
        current["probe_count"] = self._coerce_int(current.get("probe_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        probe_status = self._normalize_text(row.get("probe_status", ""))
        if probe_status == "success":
            current["probe_success_count"] = self._coerce_int(current.get("probe_success_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        elif probe_status in {"blocked", "skipped"}:
            current["probe_blocked_count"] = self._coerce_int(current.get("probe_blocked_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        elif probe_status:
            current["probe_error_count"] = self._coerce_int(current.get("probe_error_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        current["last_probe_status"] = probe_status
        current["last_probe_at"] = observed_at
        current["last_probe_method"] = str(row.get("method", "") or "").strip()
        current["last_probe_effect"] = str(row.get("effect_kind", "") or "").strip()
        current["learned_role"] = str(row.get("semantic_role", "") or current.get("learned_role", "") or "").strip()
        current["last_probe_summary"] = str(row.get("effect_summary", "") or row.get("message", "") or "").strip()
        current["expected_text"] = str(row.get("expected_text", "") or current.get("expected_text", "") or "").strip()
        current["last_post_surface_fingerprint"] = str(
            row.get("post_surface_fingerprint", "") or current.get("last_post_surface_fingerprint", "") or ""
        ).strip()
        current["vision_labels"] = self._merge_recent_strings(
            current.get("vision_labels", []),
            [str(item).strip() for item in row.get("vision_labels", []) if str(item).strip()] if isinstance(row.get("vision_labels", []), list) else [],
            limit=10,
        )
        controls[identity] = current
        self._increment_count(entry.setdefault("probe_status_counts", {}), probe_status)
        self._increment_count(entry.setdefault("probe_effect_counts", {}), str(row.get("effect_kind", "") or "").strip())
        self._increment_count(entry.setdefault("probe_role_counts", {}), str(row.get("semantic_role", "") or "").strip())
        self._increment_count(entry.setdefault("tested_control_counts", {}), current.get("label", identity))
        from_surface = str(row.get("pre_surface_fingerprint", "") or default_surface_fingerprint or "").strip()
        to_surface = str(row.get("post_surface_fingerprint", "") or from_surface or "").strip()
        if from_surface and to_surface:
            self._record_surface_transition(
                entry=entry,
                observed_at=observed_at,
                from_surface_fingerprint=from_surface,
                to_surface_fingerprint=to_surface,
                label=current.get("label", identity),
                effect_kind=str(row.get("effect_kind", "") or "").strip(),
                semantic_role=str(row.get("semantic_role", "") or "").strip(),
            )

    def _trim_entry_locked(self, entry: Dict[str, Any]) -> None:
        entry["window_title_counts"] = self._trim_count_map(entry.get("window_title_counts", {}), limit=24)
        entry["surface_role_counts"] = self._trim_count_map(entry.get("surface_role_counts", {}), limit=16)
        entry["interaction_mode_counts"] = self._trim_count_map(entry.get("interaction_mode_counts", {}), limit=16)
        entry["survey_status_counts"] = self._trim_count_map(entry.get("survey_status_counts", {}), limit=8)
        entry["survey_source_counts"] = self._trim_count_map(entry.get("survey_source_counts", {}), limit=8)
        entry["failure_reason_counts"] = self._trim_count_map(entry.get("failure_reason_counts", {}), limit=16, skip_empty=True)
        entry["surface_signature_counts"] = self._trim_count_map(entry.get("surface_signature_counts", {}), limit=16)
        entry["surface_fingerprint_counts"] = self._trim_count_map(entry.get("surface_fingerprint_counts", {}), limit=16, skip_empty=True)
        entry["control_type_counts"] = self._trim_count_map(entry.get("control_type_counts", {}), limit=24)
        entry["top_label_counts"] = self._trim_count_map(entry.get("top_label_counts", {}), limit=80, skip_empty=True)
        entry["command_candidate_counts"] = self._trim_count_map(entry.get("command_candidate_counts", {}), limit=32)
        entry["recommended_action_counts"] = self._trim_count_map(entry.get("recommended_action_counts", {}), limit=32)
        entry["confirmation_candidate_counts"] = self._trim_count_map(entry.get("confirmation_candidate_counts", {}), limit=24)
        entry["destructive_candidate_counts"] = self._trim_count_map(entry.get("destructive_candidate_counts", {}), limit=24)
        entry["workflow_action_counts"] = self._trim_count_map(entry.get("workflow_action_counts", {}), limit=32)
        entry["branch_action_counts"] = self._trim_count_map(entry.get("branch_action_counts", {}), limit=24)
        entry["exploration_target_counts"] = self._trim_count_map(entry.get("exploration_target_counts", {}), limit=24, skip_empty=True)
        entry["probe_status_counts"] = self._trim_count_map(entry.get("probe_status_counts", {}), limit=12, skip_empty=True)
        entry["probe_effect_counts"] = self._trim_count_map(entry.get("probe_effect_counts", {}), limit=24, skip_empty=True)
        entry["probe_role_counts"] = self._trim_count_map(entry.get("probe_role_counts", {}), limit=24, skip_empty=True)
        entry["tested_control_counts"] = self._trim_count_map(entry.get("tested_control_counts", {}), limit=32, skip_empty=True)
        learned_commands = entry.get("learned_commands", {}) if isinstance(entry.get("learned_commands", {}), dict) else {}
        if len(learned_commands) > 64:
            ordered_commands = sorted(
                learned_commands.items(),
                key=lambda item: (
                    self._coerce_int(item[1].get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                    str(item[0]),
                ),
                reverse=True,
            )
            entry["learned_commands"] = {key: value for key, value in ordered_commands[:64]}
        surface_nodes = entry.get("surface_nodes", {}) if isinstance(entry.get("surface_nodes", {}), dict) else {}
        if len(surface_nodes) > 32:
            ordered_nodes = sorted(
                surface_nodes.items(),
                key=lambda item: (
                    self._coerce_int(item[1].get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                    str(item[1].get("last_seen_at", "")),
                    str(item[0]),
                ),
                reverse=True,
            )
            entry["surface_nodes"] = {key: value for key, value in ordered_nodes[:32]}
        surface_transitions = entry.get("surface_transitions", {}) if isinstance(entry.get("surface_transitions", {}), dict) else {}
        if len(surface_transitions) > 64:
            ordered_transitions = sorted(
                surface_transitions.items(),
                key=lambda item: (
                    self._coerce_int(item[1].get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                    str(item[1].get("last_seen_at", "")),
                    str(item[0]),
                ),
                reverse=True,
            )
            entry["surface_transitions"] = {key: value for key, value in ordered_transitions[:64]}
        shortcut_actions = entry.get("shortcut_actions", {}) if isinstance(entry.get("shortcut_actions", {}), dict) else {}
        if len(shortcut_actions) > 40:
            ordered_shortcuts = sorted(
                shortcut_actions.items(),
                key=lambda item: (
                    self._coerce_int(item[1].get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                    str(item[0]),
                ),
                reverse=True,
            )
            entry["shortcut_actions"] = {key: value for key, value in ordered_shortcuts[:40]}
        controls = entry.get("controls", {}) if isinstance(entry.get("controls", {}), dict) else {}
        if len(controls) > self.max_controls_per_entry:
            ordered_controls = sorted(
                controls.items(),
                key=lambda item: (
                    self._coerce_int(item[1].get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                    str(item[1].get("last_seen_at", "")),
                    str(item[0]),
                ),
                reverse=True,
            )
            entry["controls"] = {key: value for key, value in ordered_controls[: self.max_controls_per_entry]}

    def _snapshot_item(self, row: Dict[str, Any]) -> Dict[str, Any]:
        item = dict(row)
        item["control_type_counts"] = self._trim_count_map(row.get("control_type_counts", {}), limit=24)
        item["top_labels"] = self._top_count_rows(row.get("top_label_counts", {}), limit=12, label_field="label")
        item["top_controls"] = self._top_controls(row.get("controls", {}), limit=12)
        item["command_candidates"] = self._top_count_rows(row.get("command_candidate_counts", {}), limit=10)
        item["recommended_actions"] = self._top_count_rows(row.get("recommended_action_counts", {}), limit=10)
        item["confirmation_candidates"] = self._top_count_rows(row.get("confirmation_candidate_counts", {}), limit=8)
        item["destructive_candidates"] = self._top_count_rows(row.get("destructive_candidate_counts", {}), limit=8)
        item["workflow_actions"] = self._top_count_rows(row.get("workflow_action_counts", {}), limit=8)
        item["branch_actions"] = self._top_count_rows(row.get("branch_action_counts", {}), limit=8)
        item["exploration_targets"] = self._top_count_rows(row.get("exploration_target_counts", {}), limit=8)
        item["probe_statuses"] = self._top_count_rows(row.get("probe_status_counts", {}), limit=8)
        item["tested_controls"] = self._top_count_rows(row.get("tested_control_counts", {}), limit=8, label_field="label")
        item["probe_effects"] = self._top_count_rows(row.get("probe_effect_counts", {}), limit=8)
        item["probe_roles"] = self._top_count_rows(row.get("probe_role_counts", {}), limit=8)
        item["surface_roles"] = self._top_count_rows(row.get("surface_role_counts", {}), limit=6)
        item["interaction_modes"] = self._top_count_rows(row.get("interaction_mode_counts", {}), limit=6)
        item["survey_statuses"] = self._top_count_rows(row.get("survey_status_counts", {}), limit=6)
        item["survey_sources"] = self._top_count_rows(row.get("survey_source_counts", {}), limit=6)
        item["failure_reasons"] = self._top_count_rows(row.get("failure_reason_counts", {}), limit=6)
        item["window_titles"] = self._top_count_rows(row.get("window_title_counts", {}), limit=6)
        item["surface_fingerprints"] = self._top_count_rows(row.get("surface_fingerprint_counts", {}), limit=6)
        item["surface_signatures"] = self._top_count_rows(row.get("surface_signature_counts", {}), limit=4)
        item["metrics"] = self._normalize_metrics(row.get("metrics", {}))
        item["native_summary"] = dict(row.get("native_summary", {})) if isinstance(row.get("native_summary", {}), dict) else {}
        item["probe_summary"] = (
            dict(row.get("last_probe_summary", {}))
            if isinstance(row.get("last_probe_summary", {}), dict)
            else {}
        )
        item["surface_nodes"] = self._top_surface_nodes(row.get("surface_nodes", {}), limit=8)
        item["surface_transitions"] = self._top_surface_transitions(row.get("surface_transitions", {}), limit=8)
        item["learned_commands"] = self._top_commands(row.get("learned_commands", {}), limit=10)
        item["capability_profile"] = self._capability_profile_snapshot(row)
        item["learning_health"] = self._learning_health_snapshot(row)
        history_rows = [dict(entry) for entry in row.get("survey_history", []) if isinstance(entry, dict)]
        item["survey_history"] = history_rows[-self.max_history_per_entry :]
        item["latest_survey"] = item["survey_history"][-1] if item["survey_history"] else {}
        item["discovered_control_count"] = len(row.get("controls", {})) if isinstance(row.get("controls", {}), dict) else 0
        item["shortcut_actions"] = [
            {
                "action": str(action_name).strip(),
                "sample_count": self._coerce_int(details.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                "hotkeys": [str(value).strip() for value in details.get("hotkeys", []) if str(value).strip()][:12],
            }
            for action_name, details in (
                row.get("shortcut_actions", {}).items() if isinstance(row.get("shortcut_actions", {}), dict) else []
            )
            if str(action_name).strip() and isinstance(details, dict)
        ][:12]
        return item

    def _snapshot_summary(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        category_counts: Dict[str, int] = {}
        profile_counts: Dict[str, int] = {}
        surface_role_counts: Dict[str, int] = {}
        interaction_mode_counts: Dict[str, int] = {}
        survey_status_counts: Dict[str, int] = {}
        survey_source_counts: Dict[str, int] = {}
        control_type_counts: Dict[str, int] = {}
        survey_count_total = 0
        survey_failure_total = 0
        probe_blocked_total = 0
        probe_error_total = 0
        ocr_target_total = 0
        probe_attempt_total = 0
        probe_success_total = 0
        surface_node_total = 0
        surface_transition_total = 0
        learned_command_total = 0
        discovered_control_total = 0
        command_candidate_total = 0
        healthy_app_count = 0
        degraded_app_count = 0
        apps: List[Dict[str, Any]] = []
        for row in rows:
            self._increment_count(category_counts, row.get("category", ""))
            self._increment_count(profile_counts, row.get("profile_id", ""))
            for key, count in self._normalize_count_map(row.get("surface_role_counts", {})).items():
                surface_role_counts[key] = self._coerce_int(surface_role_counts.get(key, 0), minimum=0, maximum=10_000_000, default=0) + count
            for key, count in self._normalize_count_map(row.get("interaction_mode_counts", {})).items():
                interaction_mode_counts[key] = self._coerce_int(interaction_mode_counts.get(key, 0), minimum=0, maximum=10_000_000, default=0) + count
            for key, count in self._normalize_count_map(row.get("survey_status_counts", {})).items():
                survey_status_counts[key] = self._coerce_int(survey_status_counts.get(key, 0), minimum=0, maximum=10_000_000, default=0) + count
            for key, count in self._normalize_count_map(row.get("survey_source_counts", {})).items():
                survey_source_counts[key] = self._coerce_int(survey_source_counts.get(key, 0), minimum=0, maximum=10_000_000, default=0) + count
            for key, count in self._normalize_count_map(row.get("control_type_counts", {})).items():
                control_type_counts[key] = self._coerce_int(control_type_counts.get(key, 0), minimum=0, maximum=10_000_000, default=0) + count
            metrics = self._normalize_metrics(row.get("metrics", {}))
            survey_count_total += self._coerce_int(metrics.get("survey_count", 0), minimum=0, maximum=10_000_000, default=0)
            survey_failure_total += self._coerce_int(metrics.get("survey_failure_count", 0), minimum=0, maximum=10_000_000, default=0)
            probe_blocked_total += self._coerce_int(metrics.get("probe_blocked_count", 0), minimum=0, maximum=10_000_000, default=0)
            probe_error_total += self._coerce_int(metrics.get("probe_error_count", 0), minimum=0, maximum=10_000_000, default=0)
            ocr_target_total += self._coerce_int(metrics.get("ocr_target_count", 0), minimum=0, maximum=10_000_000, default=0)
            probe_attempt_total += self._coerce_int(metrics.get("probe_attempt_count", 0), minimum=0, maximum=10_000_000, default=0)
            probe_success_total += self._coerce_int(metrics.get("probe_success_count", 0), minimum=0, maximum=10_000_000, default=0)
            surface_node_total += len(row.get("surface_nodes", {})) if isinstance(row.get("surface_nodes", {}), dict) else 0
            surface_transition_total += len(row.get("surface_transitions", {})) if isinstance(row.get("surface_transitions", {}), dict) else 0
            learned_command_total += len(row.get("learned_commands", {})) if isinstance(row.get("learned_commands", {}), dict) else 0
            discovered_control_total += len(row.get("controls", {})) if isinstance(row.get("controls", {}), dict) else 0
            command_candidate_total += len(self._trim_count_map(row.get("command_candidate_counts", {}), limit=32))
            learning_health = self._learning_health_snapshot(row)
            if str(learning_health.get("status", "") or "") == "healthy":
                healthy_app_count += 1
            if str(learning_health.get("status", "") or "") in {"degraded", "attention"}:
                degraded_app_count += 1
            apps.append(
                {
                    "app_name": str(row.get("app_name", "") or "").strip(),
                    "profile_id": str(row.get("profile_id", "") or "").strip(),
                    "survey_count": metrics.get("survey_count", 0),
                    "discovered_control_count": len(row.get("controls", {})) if isinstance(row.get("controls", {}), dict) else 0,
                    "learning_status": str(learning_health.get("status", "") or "").strip(),
                    "updated_at": str(row.get("updated_at", "") or ""),
                }
            )
        apps.sort(
            key=lambda item: (
                -self._coerce_int(item.get("survey_count", 0), minimum=0, maximum=10_000_000, default=0),
                -self._coerce_int(item.get("discovered_control_count", 0), minimum=0, maximum=10_000_000, default=0),
                str(item.get("updated_at", "")),
            )
        )
        return {
            "status": "success",
            "entry_count": len(rows),
            "survey_count_total": survey_count_total,
            "survey_failure_total": survey_failure_total,
            "probe_blocked_total": probe_blocked_total,
            "probe_error_total": probe_error_total,
            "ocr_target_total": ocr_target_total,
            "probe_attempt_total": probe_attempt_total,
            "probe_success_total": probe_success_total,
            "surface_node_total": surface_node_total,
            "surface_transition_total": surface_transition_total,
            "learned_command_total": learned_command_total,
            "discovered_control_total": discovered_control_total,
            "command_candidate_total": command_candidate_total,
            "healthy_app_count": healthy_app_count,
            "degraded_app_count": degraded_app_count,
            "category_counts": self._trim_count_map(category_counts, limit=16),
            "profile_counts": self._trim_count_map(profile_counts, limit=16),
            "surface_role_counts": self._trim_count_map(surface_role_counts, limit=16),
            "interaction_mode_counts": self._trim_count_map(interaction_mode_counts, limit=16),
            "survey_status_counts": self._trim_count_map(survey_status_counts, limit=8),
            "survey_source_counts": self._trim_count_map(survey_source_counts, limit=8),
            "control_type_counts": self._trim_count_map(control_type_counts, limit=24),
            "top_apps": apps[:8],
        }

    def _load(self) -> None:
        try:
            raw = self.store_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        except Exception:
            return
        try:
            payload = json.loads(raw)
        except Exception:
            return
        entries = payload.get("entries", {}) if isinstance(payload, dict) else {}
        if isinstance(entries, dict):
            self._entries = {
                str(key).strip(): dict(value)
                for key, value in entries.items()
                if str(key).strip() and isinstance(value, dict)
            }

    def _maybe_save_locked(self, *, force: bool) -> None:
        if not force and self._updates_since_save < 4 and (time.monotonic() - self._last_save_monotonic) < 4.0:
            return
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": "1.0",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "entries": self._entries,
        }
        self.store_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        self._updates_since_save = 0
        self._last_save_monotonic = time.monotonic()

    def _trim_locked(self) -> None:
        if len(self._entries) > self.max_entries:
            ordered = sorted(
                self._entries.items(),
                key=lambda item: str(item[1].get("updated_at", "")),
                reverse=True,
            )
            self._entries = {key: value for key, value in ordered[: self.max_entries]}

    @classmethod
    def _entry_key(
        cls,
        *,
        app_name: str,
        app_profile: Dict[str, Any] | None,
        target_window: Dict[str, Any] | None,
        active_window: Dict[str, Any] | None,
    ) -> str:
        profile_id = cls._profile_id(app_profile)
        app_hint = cls._normalize_text(app_name)
        window_hint = cls._normalize_text((target_window or {}).get("title", "")) or cls._normalize_text((active_window or {}).get("title", ""))
        return "|".join(part for part in [profile_id, app_hint or window_hint or "desktop"] if part)

    @classmethod
    def _display_app_name(
        cls,
        *,
        explicit_app_name: str,
        explicit_window_title: str,
        app_profile: Dict[str, Any],
        target_window: Dict[str, Any],
        active_window: Dict[str, Any],
        launch_result: Dict[str, Any],
    ) -> str:
        return (
            str(explicit_app_name or "").strip()
            or str(app_profile.get("name", "") or "").strip()
            or str(launch_result.get("requested_app", "") or "").strip()
            or str(target_window.get("app_name", "") or "").strip()
            or str(active_window.get("app_name", "") or "").strip()
            or str(target_window.get("title", "") or active_window.get("title", "") or explicit_window_title or "").strip()
            or "desktop"
        )

    @staticmethod
    def _profile_id(app_profile: Dict[str, Any] | None) -> str:
        profile = app_profile if isinstance(app_profile, dict) else {}
        return (
            DesktopAppMemory._normalize_text(profile.get("profile_id", ""))
            or DesktopAppMemory._normalize_text(profile.get("name", ""))
            or DesktopAppMemory._normalize_text(profile.get("category", ""))
            or "generic"
        )

    @staticmethod
    def _candidate_label(row: Dict[str, Any]) -> str:
        return str(row.get("name", "") or row.get("automation_id", "") or row.get("label", "") or "").strip()

    @staticmethod
    def _control_identity(row: Dict[str, Any]) -> str:
        explicit = str(row.get("element_id", "") or "").strip()
        if explicit:
            return explicit
        parts = [
            str(row.get("automation_id", "") or "").strip(),
            str(row.get("name", "") or "").strip().lower(),
            str(row.get("control_type", "") or "").strip().lower(),
            str(row.get("class_name", "") or "").strip().lower(),
        ]
        return "|".join(part for part in parts if part)

    @staticmethod
    def _normalize_text(value: Any) -> str:
        return " ".join(str(value or "").strip().lower().split())

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            numeric = int(value)
        except Exception:
            numeric = default
        return max(minimum, min(maximum, numeric))

    @staticmethod
    def _increment_count(mapping: Any, key: Any) -> None:
        if not isinstance(mapping, dict):
            return
        clean = str(key or "").strip().lower()
        if not clean:
            return
        mapping[clean] = DesktopAppMemory._coerce_int(mapping.get(clean, 0), minimum=0, maximum=10_000_000, default=0) + 1

    @staticmethod
    def _normalize_count_map(raw: Any) -> Dict[str, int]:
        if not isinstance(raw, dict):
            return {}
        normalized: Dict[str, int] = {}
        for key, value in raw.items():
            clean = str(key or "").strip().lower()
            if not clean:
                continue
            normalized[clean] = DesktopAppMemory._coerce_int(value, minimum=0, maximum=10_000_000, default=0)
        return normalized

    @staticmethod
    def _trim_count_map(raw: Any, *, limit: int, skip_empty: bool = False) -> Dict[str, int]:
        rows = DesktopAppMemory._normalize_count_map(raw)
        if skip_empty:
            rows = {key: value for key, value in rows.items() if key}
        ordered = sorted(rows.items(), key=lambda item: (item[1], item[0]), reverse=True)
        return {key: value for key, value in ordered[: max(1, int(limit or 1))]}

    @staticmethod
    def _top_count_rows(raw: Any, *, limit: int, label_field: str = "value") -> List[Dict[str, Any]]:
        trimmed = DesktopAppMemory._trim_count_map(raw, limit=limit)
        return [{label_field: key, "count": value} for key, value in trimmed.items()]

    @staticmethod
    def _top_controls(raw: Any, *, limit: int) -> List[Dict[str, Any]]:
        rows = [
            dict(row)
            for row in raw.values()
            if isinstance(raw, dict) and isinstance(row, dict)
        ] if isinstance(raw, dict) else []
        rows.sort(
            key=lambda row: (
                DesktopAppMemory._coerce_int(row.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("last_seen_at", "")),
                str(row.get("label", "")),
            ),
            reverse=True,
        )
        return rows[: max(1, int(limit or 1))]

    @staticmethod
    def _normalize_metrics(raw: Any) -> Dict[str, int]:
        metrics = raw if isinstance(raw, dict) else {}
        return {
            "survey_count": DesktopAppMemory._coerce_int(metrics.get("survey_count", 0), minimum=0, maximum=10_000_000, default=0),
            "survey_success_count": DesktopAppMemory._coerce_int(metrics.get("survey_success_count", 0), minimum=0, maximum=10_000_000, default=0),
            "survey_failure_count": DesktopAppMemory._coerce_int(metrics.get("survey_failure_count", 0), minimum=0, maximum=10_000_000, default=0),
            "launch_attempt_count": DesktopAppMemory._coerce_int(metrics.get("launch_attempt_count", 0), minimum=0, maximum=10_000_000, default=0),
            "launch_success_count": DesktopAppMemory._coerce_int(metrics.get("launch_success_count", 0), minimum=0, maximum=10_000_000, default=0),
            "launch_failure_count": DesktopAppMemory._coerce_int(metrics.get("launch_failure_count", 0), minimum=0, maximum=10_000_000, default=0),
            "surface_success_count": DesktopAppMemory._coerce_int(metrics.get("surface_success_count", 0), minimum=0, maximum=10_000_000, default=0),
            "element_observation_count": DesktopAppMemory._coerce_int(metrics.get("element_observation_count", 0), minimum=0, maximum=10_000_000, default=0),
            "control_inventory_count": DesktopAppMemory._coerce_int(metrics.get("control_inventory_count", 0), minimum=0, maximum=10_000_000, default=0),
            "query_candidate_count": DesktopAppMemory._coerce_int(metrics.get("query_candidate_count", 0), minimum=0, maximum=10_000_000, default=0),
            "workflow_surface_count": DesktopAppMemory._coerce_int(metrics.get("workflow_surface_count", 0), minimum=0, maximum=10_000_000, default=0),
            "branch_action_count": DesktopAppMemory._coerce_int(metrics.get("branch_action_count", 0), minimum=0, maximum=10_000_000, default=0),
            "top_hypothesis_count": DesktopAppMemory._coerce_int(metrics.get("top_hypothesis_count", 0), minimum=0, maximum=10_000_000, default=0),
            "ocr_target_count": DesktopAppMemory._coerce_int(metrics.get("ocr_target_count", 0), minimum=0, maximum=10_000_000, default=0),
            "probe_attempt_count": DesktopAppMemory._coerce_int(metrics.get("probe_attempt_count", 0), minimum=0, maximum=10_000_000, default=0),
            "probe_success_count": DesktopAppMemory._coerce_int(metrics.get("probe_success_count", 0), minimum=0, maximum=10_000_000, default=0),
            "probe_blocked_count": DesktopAppMemory._coerce_int(metrics.get("probe_blocked_count", 0), minimum=0, maximum=10_000_000, default=0),
            "probe_error_count": DesktopAppMemory._coerce_int(metrics.get("probe_error_count", 0), minimum=0, maximum=10_000_000, default=0),
            "manual_survey_count": DesktopAppMemory._coerce_int(metrics.get("manual_survey_count", 0), minimum=0, maximum=10_000_000, default=0),
            "batch_survey_count": DesktopAppMemory._coerce_int(metrics.get("batch_survey_count", 0), minimum=0, maximum=10_000_000, default=0),
            "background_survey_count": DesktopAppMemory._coerce_int(metrics.get("background_survey_count", 0), minimum=0, maximum=10_000_000, default=0),
        }

    @classmethod
    def _control_aliases(cls, row: Dict[str, Any]) -> List[str]:
        aliases = [
            str(row.get("name", "") or "").strip(),
            str(row.get("automation_id", "") or "").strip(),
            str(row.get("access_key", "") or "").strip(),
            str(row.get("accelerator_key", "") or "").strip(),
        ]
        normalized: List[str] = []
        seen: set[str] = set()
        for alias in aliases:
            clean = cls._normalize_text(alias)
            if not clean or clean in seen:
                continue
            seen.add(clean)
            normalized.append(clean)
        return normalized

    @staticmethod
    def _merge_recent_strings(existing: Any, additions: List[str], *, limit: int) -> List[str]:
        values = [str(item).strip() for item in existing if str(item).strip()] if isinstance(existing, list) else []
        for addition in additions:
            clean = str(addition or "").strip()
            if not clean:
                continue
            if clean in values:
                continue
            values.append(clean)
        return values[-max(1, int(limit or 1)) :]

    @classmethod
    def _learning_health_snapshot(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        metrics = cls._normalize_metrics(row.get("metrics", {}))
        survey_count = cls._coerce_int(metrics.get("survey_count", 0), minimum=0, maximum=10_000_000, default=0)
        success_count = cls._coerce_int(metrics.get("survey_success_count", 0), minimum=0, maximum=10_000_000, default=0)
        failure_count = cls._coerce_int(metrics.get("survey_failure_count", 0), minimum=0, maximum=10_000_000, default=0)
        probe_attempt_count = cls._coerce_int(metrics.get("probe_attempt_count", 0), minimum=0, maximum=10_000_000, default=0)
        probe_success_count = cls._coerce_int(metrics.get("probe_success_count", 0), minimum=0, maximum=10_000_000, default=0)
        probe_blocked_count = cls._coerce_int(metrics.get("probe_blocked_count", 0), minimum=0, maximum=10_000_000, default=0)
        probe_error_count = cls._coerce_int(metrics.get("probe_error_count", 0), minimum=0, maximum=10_000_000, default=0)
        last_status = cls._normalize_text(row.get("last_survey_status", "")) or "unknown"
        success_rate = round(float(success_count) / float(survey_count), 4) if survey_count > 0 else 0.0
        probe_success_rate = round(float(probe_success_count) / float(probe_attempt_count), 4) if probe_attempt_count > 0 else 0.0
        status = "learning"
        if survey_count <= 0:
            status = "idle"
        elif failure_count <= 0 and last_status in {"success", "partial"} and success_rate >= 0.6:
            status = "healthy"
        elif failure_count > success_count or last_status == "error":
            status = "degraded"
        elif failure_count > 0:
            status = "attention"
        return {
            "status": status,
            "survey_count": survey_count,
            "success_count": success_count,
            "failure_count": failure_count,
            "success_rate": success_rate,
            "probe_attempt_count": probe_attempt_count,
            "probe_success_count": probe_success_count,
            "probe_blocked_count": probe_blocked_count,
            "probe_error_count": probe_error_count,
            "probe_success_rate": probe_success_rate,
            "last_status": last_status,
            "last_source": str(row.get("last_survey_source", "") or "").strip(),
            "last_error_message": str(row.get("last_error_message", "") or "").strip(),
        }

    @classmethod
    def _surface_fingerprint(
        cls,
        *,
        app_name: str,
        profile_id: str,
        target_window: Dict[str, Any],
        active_window: Dict[str, Any],
        summary: Dict[str, Any],
        intelligence: Dict[str, Any],
        observation: Dict[str, Any],
    ) -> str:
        control_counts = cls._normalize_count_map(summary.get("control_counts", {}))
        dominant_controls = [
            key
            for key, _ in sorted(control_counts.items(), key=lambda item: (item[1], item[0]), reverse=True)[:4]
        ]
        top_labels = [
            cls._normalize_text(dict(row).get("label", ""))
            for row in summary.get("top_labels", [])
            if isinstance(row, dict)
        ][:3]
        ocr_terms = [
            cls._normalize_text(dict(row).get("text", ""))
            for row in observation.get("targets", [])
            if isinstance(row, dict)
        ][:3]
        parts = [
            cls._normalize_text(profile_id),
            cls._normalize_text(app_name),
            cls._normalize_text(intelligence.get("surface_role", "")),
            cls._normalize_text(intelligence.get("interaction_mode", "")),
            cls._normalize_text(target_window.get("class_name", "") or active_window.get("class_name", "")),
            cls._normalize_text(target_window.get("window_signature", "") or active_window.get("window_signature", "")),
            *[part for part in top_labels if part],
            *[part for part in dominant_controls if part],
            *[part for part in ocr_terms if part],
        ]
        return "|".join(part for part in parts if part)[:320] or "generic|surface"

    def _record_surface_node(
        self,
        *,
        entry: Dict[str, Any],
        observed_at: str,
        surface_fingerprint: str,
        snapshot_payload: Dict[str, Any],
        app_profile: Dict[str, Any],
        probe_payload: Dict[str, Any],
    ) -> None:
        if not surface_fingerprint:
            return
        summary = snapshot_payload.get("surface_summary", {}) if isinstance(snapshot_payload.get("surface_summary", {}), dict) else {}
        intelligence = snapshot_payload.get("surface_intelligence", {}) if isinstance(snapshot_payload.get("surface_intelligence", {}), dict) else {}
        observation = snapshot_payload.get("observation", {}) if isinstance(snapshot_payload.get("observation", {}), dict) else {}
        node_map = entry.setdefault("surface_nodes", {})
        current = node_map.get(surface_fingerprint, {}) if isinstance(node_map.get(surface_fingerprint, {}), dict) else {}
        current = dict(current)
        current["fingerprint"] = surface_fingerprint
        current["sample_count"] = self._coerce_int(current.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        current["last_seen_at"] = observed_at
        current["surface_role"] = str(intelligence.get("surface_role", "") or current.get("surface_role", "") or "").strip()
        current["interaction_mode"] = str(intelligence.get("interaction_mode", "") or current.get("interaction_mode", "") or "").strip()
        current["summary"] = str(summary.get("summary", "") or current.get("summary", "") or "").strip()
        current["profile_id"] = self._profile_id(app_profile)
        current["window_titles"] = self._merge_recent_strings(
            current.get("window_titles", []),
            [
                str(snapshot_payload.get("target_window", {}).get("title", "") if isinstance(snapshot_payload.get("target_window", {}), dict) else "").strip(),
                str(snapshot_payload.get("active_window", {}).get("title", "") if isinstance(snapshot_payload.get("active_window", {}), dict) else "").strip(),
            ],
            limit=8,
        )
        current["top_labels"] = self._merge_recent_strings(
            current.get("top_labels", []),
            [
                str(dict(row).get("label", "") or "").strip()
                for row in summary.get("top_labels", [])
                if isinstance(row, dict)
            ],
            limit=10,
        )
        current["ocr_keywords"] = self._merge_recent_strings(
            current.get("ocr_keywords", []),
            [
                str(dict(row).get("text", "") or "").strip()
                for row in observation.get("targets", [])
                if isinstance(row, dict)
            ],
            limit=10,
        )
        current["recommended_actions"] = self._merge_recent_strings(
            current.get("recommended_actions", []),
            [str(item).strip() for item in summary.get("recommended_actions", []) if str(item).strip()],
            limit=10,
        )
        current["query_examples"] = self._merge_recent_strings(
            current.get("query_examples", []),
            [str(item.get("query", "") or "").strip() for item in entry.get("survey_history", []) if isinstance(item, dict)],
            limit=6,
        )
        current["control_counts"] = summary.get("control_counts", {}) if isinstance(summary.get("control_counts", {}), dict) else {}
        current["probe_success_count"] = self._coerce_int(current.get("probe_success_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("successful_count", 0), minimum=0, maximum=10_000_000, default=0)
        current["probe_attempt_count"] = self._coerce_int(current.get("probe_attempt_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("attempted_count", 0), minimum=0, maximum=10_000_000, default=0)
        node_map[surface_fingerprint] = current

    def _record_surface_transition(
        self,
        *,
        entry: Dict[str, Any],
        observed_at: str,
        from_surface_fingerprint: str,
        to_surface_fingerprint: str,
        label: str,
        effect_kind: str,
        semantic_role: str,
    ) -> None:
        clean_from = str(from_surface_fingerprint or "").strip()
        clean_to = str(to_surface_fingerprint or "").strip()
        clean_label = str(label or "").strip()
        if not clean_from or not clean_to or not clean_label:
            return
        transition_key = "|".join(
            part for part in [clean_from, self._normalize_text(clean_label), clean_to] if part
        )
        transitions = entry.setdefault("surface_transitions", {})
        current = transitions.get(transition_key, {}) if isinstance(transitions.get(transition_key, {}), dict) else {}
        current = dict(current)
        current["transition_key"] = transition_key
        current["from_surface_fingerprint"] = clean_from
        current["to_surface_fingerprint"] = clean_to
        current["label"] = clean_label
        current["effect_kind"] = str(effect_kind or "").strip()
        current["semantic_role"] = str(semantic_role or "").strip()
        current["sample_count"] = self._coerce_int(current.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        current["last_seen_at"] = observed_at
        transitions[transition_key] = current

    def _record_command_harvest(
        self,
        *,
        entry: Dict[str, Any],
        label: str,
        control_type: str,
        source: str,
        hotkeys: List[str] | None = None,
        aliases: List[str] | None = None,
    ) -> None:
        clean_label = str(label or "").strip()
        if not clean_label:
            return
        key = self._normalize_text(clean_label)
        if not key:
            return
        command_map = entry.setdefault("learned_commands", {})
        current = command_map.get(key, {}) if isinstance(command_map.get(key, {}), dict) else {}
        current = dict(current)
        current["label"] = clean_label
        current["sample_count"] = self._coerce_int(current.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        current["control_types"] = self._merge_recent_strings(
            current.get("control_types", []),
            [str(control_type or "").strip()],
            limit=6,
        )
        current["sources"] = self._merge_recent_strings(
            current.get("sources", []),
            [str(source or "").strip()],
            limit=8,
        )
        current["aliases"] = self._merge_recent_strings(
            current.get("aliases", []),
            [str(item).strip() for item in (aliases or []) if str(item).strip()],
            limit=12,
        )
        current["hotkeys"] = self._merge_recent_strings(
            current.get("hotkeys", []),
            [str(item).strip() for item in (hotkeys or []) if str(item).strip()],
            limit=12,
        )
        command_map[key] = current

    def _record_capability_profile(
        self,
        *,
        entry: Dict[str, Any],
        summary: Dict[str, Any],
        intelligence: Dict[str, Any],
        workflow_surfaces: Any,
        probe_payload: Dict[str, Any],
        observation: Dict[str, Any],
    ) -> None:
        flags = summary.get("surface_flags", {}) if isinstance(summary.get("surface_flags", {}), dict) else {}
        workflow_rows = [dict(row) for row in workflow_surfaces if isinstance(row, dict)] if isinstance(workflow_surfaces, list) else []
        capabilities = entry.setdefault("capability_profile_counts", {})
        feature_values = {
            "search_surface": bool(flags.get("search_visible", False) or any(str(row.get("action", "")).strip() == "search" for row in workflow_rows)),
            "command_surface": any(str(row.get("action", "")).strip() == "command" for row in workflow_rows),
            "navigation_tree": bool(flags.get("navigation_tree_visible", False)),
            "list_surface": bool(flags.get("list_surface_visible", False)),
            "data_table": bool(flags.get("data_table_visible", False)),
            "form_surface": bool(flags.get("form_surface_visible", False)),
            "dialog_surface": bool(flags.get("dialog_visible", False)),
            "wizard_surface": bool(flags.get("wizard_surface_visible", False)),
            "keyboard_shortcuts": any(
                isinstance(row.get("primary_hotkey", []), list) and bool(row.get("primary_hotkey", []))
                for row in workflow_rows
            ),
            "vision_grounded": bool(observation.get("targets")),
            "safe_probe_ready": self._coerce_int(probe_payload.get("attempted_count", 0), minimum=0, maximum=10_000_000, default=0) > 0,
            "navigator_role": self._normalize_text(intelligence.get("surface_role", "")) in {"navigator", "file_manager", "browser"},
        }
        for feature, enabled in feature_values.items():
            if enabled:
                self._increment_count(capabilities, feature)

    @staticmethod
    def _top_surface_nodes(raw: Any, *, limit: int) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in raw.values() if isinstance(raw, dict) and isinstance(row, dict)] if isinstance(raw, dict) else []
        rows.sort(
            key=lambda row: (
                DesktopAppMemory._coerce_int(row.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("last_seen_at", "")),
                str(row.get("fingerprint", "")),
            ),
            reverse=True,
        )
        return rows[: max(1, int(limit or 1))]

    @staticmethod
    def _top_surface_transitions(raw: Any, *, limit: int) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in raw.values() if isinstance(raw, dict) and isinstance(row, dict)] if isinstance(raw, dict) else []
        rows.sort(
            key=lambda row: (
                DesktopAppMemory._coerce_int(row.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("last_seen_at", "")),
                str(row.get("transition_key", "")),
            ),
            reverse=True,
        )
        return rows[: max(1, int(limit or 1))]

    @staticmethod
    def _top_commands(raw: Any, *, limit: int) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in raw.values() if isinstance(raw, dict) and isinstance(row, dict)] if isinstance(raw, dict) else []
        rows.sort(
            key=lambda row: (
                DesktopAppMemory._coerce_int(row.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("label", "")),
            ),
            reverse=True,
        )
        return rows[: max(1, int(limit or 1))]

    @classmethod
    def _capability_profile_snapshot(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        counts = cls._normalize_count_map(row.get("capability_profile_counts", {}))
        return {
            "status": "success",
            "features": counts,
            "top_features": [{ "value": key, "count": value } for key, value in list(cls._trim_count_map(counts, limit=10).items())],
        }

    def surface_hint(
        self,
        *,
        app_name: str = "",
        profile_id: str = "",
        surface_fingerprint: str = "",
    ) -> Dict[str, Any]:
        clean_app_name = self._normalize_text(app_name)
        clean_profile_id = self._normalize_text(profile_id)
        clean_surface_fingerprint = str(surface_fingerprint or "").strip()
        with self._lock:
            rows = [dict(row) for row in self._entries.values()]
        rows.sort(key=lambda row: str(row.get("updated_at", "")), reverse=True)
        for row in rows:
            if clean_app_name and clean_app_name not in self._normalize_text(row.get("app_name", "")) and clean_app_name not in self._normalize_text(row.get("window_title", "")):
                continue
            if clean_profile_id and clean_profile_id != self._normalize_text(row.get("profile_id", "")):
                continue
            nodes = row.get("surface_nodes", {}) if isinstance(row.get("surface_nodes", {}), dict) else {}
            node = dict(nodes.get(clean_surface_fingerprint, {})) if clean_surface_fingerprint and isinstance(nodes.get(clean_surface_fingerprint, {}), dict) else {}
            return {
                "status": "success",
                "known": bool(node),
                "surface_fingerprint": clean_surface_fingerprint,
                "surface_node": node,
                "capability_profile": self._capability_profile_snapshot(row),
                "learned_commands": self._top_commands(row.get("learned_commands", {}), limit=8),
                "shortcut_actions": self._snapshot_item(row).get("shortcut_actions", []),
            }
        return {
            "status": "success",
            "known": False,
            "surface_fingerprint": clean_surface_fingerprint,
            "surface_node": {},
            "capability_profile": {"status": "success", "features": {}, "top_features": []},
            "learned_commands": [],
            "shortcut_actions": [],
        }
