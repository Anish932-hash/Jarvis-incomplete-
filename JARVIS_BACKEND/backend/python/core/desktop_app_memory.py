from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List


class DesktopAppMemory:
    _DEFAULT_INSTANCE: "DesktopAppMemory | None" = None
    _DEFAULT_LOCK = RLock()
    _HOTKEY_PATTERN = re.compile(
        r"(?i)\b(?:ctrl|control|alt|shift|cmd|command|win|windows)"
        r"(?:\s*\+\s*(?:ctrl|control|alt|shift|cmd|command|win|windows))*"
        r"\s*\+\s*(?:[a-z0-9]|f(?:1[0-2]|[1-9])|tab|enter|esc|space|delete|backspace|up|down|left|right)\b"
    )

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
        wave_report: Dict[str, Any] | None = None,
        survey_status: str = "success",
        error_message: str = "",
        source: str = "manual",
    ) -> Dict[str, Any]:
        profile = dict(app_profile) if isinstance(app_profile, dict) else {}
        launch_payload = dict(launch_result) if isinstance(launch_result, dict) else {}
        snapshot_payload = dict(snapshot) if isinstance(snapshot, dict) else {}
        exploration_payload = dict(exploration_plan) if isinstance(exploration_plan, dict) else {}
        probe_payload = dict(probe_report) if isinstance(probe_report, dict) else {}
        wave_payload = dict(wave_report) if isinstance(wave_report, dict) else {}
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
        vision_fusion = (
            snapshot_payload.get("vision_fusion", {})
            if isinstance(snapshot_payload.get("vision_fusion", {}), dict)
            else {}
        )
        native_learning_signals = (
            snapshot_payload.get("native_learning_signals", {})
            if isinstance(snapshot_payload.get("native_learning_signals", {}), dict)
            else {}
        )
        safe_traversal_plan = (
            snapshot_payload.get("safe_traversal_plan", {})
            if isinstance(snapshot_payload.get("safe_traversal_plan", {}), dict)
            else {}
        )
        vision_runtime_profile = (
            snapshot_payload.get("vision_runtime_profile", {})
            if isinstance(snapshot_payload.get("vision_runtime_profile", {}), dict)
            else {}
        )
        vision_learning_route = (
            snapshot_payload.get("vision_learning_route", {})
            if isinstance(snapshot_payload.get("vision_learning_route", {}), dict)
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
        version_profile = self._version_profile_snapshot(
            app_label=app_label,
            app_profile=profile,
            target_window=target_window,
            active_window=active_window,
            launch_result=launch_payload,
            surface_fingerprint=surface_fingerprint,
            native_window_topology=native_window_topology,
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
            metrics["probe_verified_count"] = self._coerce_int(metrics.get("probe_verified_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("verified_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["probe_uncertain_count"] = self._coerce_int(metrics.get("probe_uncertain_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("uncertain_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["probe_blocked_count"] = self._coerce_int(metrics.get("probe_blocked_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["probe_error_count"] = self._coerce_int(metrics.get("probe_error_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("error_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["wave_attempt_count"] = self._coerce_int(metrics.get("wave_attempt_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(wave_payload.get("attempted_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["wave_success_count"] = self._coerce_int(metrics.get("wave_success_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(wave_payload.get("learned_surface_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["wave_known_surface_hit_count"] = self._coerce_int(metrics.get("wave_known_surface_hit_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(wave_payload.get("known_surface_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["vision_surface_count"] = self._coerce_int(metrics.get("vision_surface_count", 0), minimum=0, maximum=10_000_000, default=0) + (1 if vision_fusion else 0)
            metrics["safe_traversal_candidate_count"] = self._coerce_int(metrics.get("safe_traversal_candidate_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(safe_traversal_plan.get("candidate_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["custom_surface_count"] = self._coerce_int(metrics.get("custom_surface_count", 0), minimum=0, maximum=10_000_000, default=0) + (1 if bool(native_learning_signals.get("custom_surface_suspected", False)) else 0)
            metrics["reparenting_risk_count"] = self._coerce_int(metrics.get("reparenting_risk_count", 0), minimum=0, maximum=10_000_000, default=0) + (1 if float(native_learning_signals.get("reparenting_risk", 0.0) or 0.0) >= 0.45 else 0)
            metrics["vision_local_runtime_count"] = self._coerce_int(metrics.get("vision_local_runtime_count", 0), minimum=0, maximum=10_000_000, default=0) + (1 if bool(vision_learning_route.get("local_runtime_ready", False)) else 0)
            metrics["vision_api_assist_count"] = self._coerce_int(metrics.get("vision_api_assist_count", 0), minimum=0, maximum=10_000_000, default=0) + (1 if bool(vision_learning_route.get("api_assist_recommended", False)) else 0)
            metrics["native_stabilization_count"] = self._coerce_int(metrics.get("native_stabilization_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("stabilized_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(wave_payload.get("stabilized_count", 0), minimum=0, maximum=10_000_000, default=0)
            metrics["weird_app_surface_count"] = self._coerce_int(metrics.get("weird_app_surface_count", 0), minimum=0, maximum=10_000_000, default=0) + (1 if float(vision_learning_route.get("weird_app_pressure", 0.0) or 0.0) >= 0.55 else 0)
            if clean_source == "daemon":
                metrics["background_survey_count"] = self._coerce_int(metrics.get("background_survey_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            elif clean_source == "batch":
                metrics["batch_survey_count"] = self._coerce_int(metrics.get("batch_survey_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
            elif "wave" in clean_source:
                metrics["wave_survey_count"] = self._coerce_int(metrics.get("wave_survey_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
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
                "stabilized_count": self._coerce_int(probe_payload.get("stabilized_count", 0), minimum=0, maximum=10_000_000, default=0),
                "status": str(probe_payload.get("status", "") or "").strip(),
                "updated_at": now,
            }
            entry["last_wave_summary"] = {
                "attempted_count": self._coerce_int(wave_payload.get("attempted_count", 0), minimum=0, maximum=10_000_000, default=0),
                "learned_surface_count": self._coerce_int(wave_payload.get("learned_surface_count", 0), minimum=0, maximum=10_000_000, default=0),
                "known_surface_count": self._coerce_int(wave_payload.get("known_surface_count", 0), minimum=0, maximum=10_000_000, default=0),
                "stabilized_count": self._coerce_int(wave_payload.get("stabilized_count", 0), minimum=0, maximum=10_000_000, default=0),
                "stop_reason": str(wave_payload.get("stop_reason", "") or "").strip(),
                "recommended_next_actions": [
                    str(item).strip()
                    for item in wave_payload.get("recommended_next_actions", [])
                    if str(item).strip()
                ][:8] if isinstance(wave_payload.get("recommended_next_actions", []), list) else [],
                "updated_at": now,
            }
            entry["last_vision_summary"] = {
                "model_mode": str(vision_fusion.get("model_mode", "") or "").strip(),
                "confidence": round(max(0.0, min(float(vision_fusion.get("confidence", 0.0) or 0.0), 1.0)), 4),
                "top_labels": [str(item).strip() for item in vision_fusion.get("top_labels", []) if str(item).strip()][:8] if isinstance(vision_fusion.get("top_labels", []), list) else [],
                "ocr_terms": [str(item).strip() for item in vision_fusion.get("ocr_terms", []) if str(item).strip()][:10] if isinstance(vision_fusion.get("ocr_terms", []), list) else [],
                "command_terms": [str(item).strip() for item in vision_fusion.get("command_terms", []) if str(item).strip()][:10] if isinstance(vision_fusion.get("command_terms", []), list) else [],
                "native_attention": bool(vision_fusion.get("native_attention", False)),
                "updated_at": now,
            }
            entry["last_vision_learning_route"] = {
                "route_profile": str(vision_learning_route.get("route_profile", "") or "").strip(),
                "model_preference": str(vision_learning_route.get("model_preference", "") or "").strip(),
                "preferred_probe_mode": str(vision_learning_route.get("preferred_probe_mode", "") or "").strip(),
                "preferred_wave_mode": str(vision_learning_route.get("preferred_wave_mode", "") or "").strip(),
                "preferred_target_mode": str(vision_learning_route.get("preferred_target_mode", "") or "").strip(),
                "preferred_verification_mode": str(vision_learning_route.get("preferred_verification_mode", "") or "").strip(),
                "local_runtime_ready": bool(vision_learning_route.get("local_runtime_ready", False)),
                "api_assist_recommended": bool(vision_learning_route.get("api_assist_recommended", False)),
                "needs_native_stabilization": bool(vision_learning_route.get("needs_native_stabilization", False)),
                "native_recovery_mode": str(vision_learning_route.get("native_recovery_mode", "") or "").strip(),
                "weird_app_pressure": round(max(0.0, min(float(vision_learning_route.get("weird_app_pressure", 0.0) or 0.0), 1.0)), 4),
                "preferred_container_roles": [str(item).strip() for item in vision_learning_route.get("preferred_container_roles", []) if str(item).strip()][:8] if isinstance(vision_learning_route.get("preferred_container_roles", []), list) else [],
                "reason_codes": [str(item).strip() for item in vision_learning_route.get("reason_codes", []) if str(item).strip()][:10] if isinstance(vision_learning_route.get("reason_codes", []), list) else [],
                "runtime_profile": {
                    "provider_mode": str(vision_runtime_profile.get("provider_mode", vision_learning_route.get("runtime_profile", {}).get("provider_mode", "")) or "").strip()
                    if isinstance(vision_learning_route.get("runtime_profile", {}), dict)
                    else str(vision_runtime_profile.get("provider_mode", "") or "").strip(),
                    "runtime_status": str(vision_runtime_profile.get("runtime_status", vision_learning_route.get("runtime_profile", {}).get("runtime_status", "")) or "").strip()
                    if isinstance(vision_learning_route.get("runtime_profile", {}), dict)
                    else str(vision_runtime_profile.get("runtime_status", "") or "").strip(),
                    "loaded_count": self._coerce_int(
                        vision_runtime_profile.get("loaded_count", vision_learning_route.get("runtime_profile", {}).get("loaded_count", 0))
                        if isinstance(vision_learning_route.get("runtime_profile", {}), dict)
                        else vision_runtime_profile.get("loaded_count", 0),
                        minimum=0,
                        maximum=128,
                        default=0,
                    ),
                },
                "updated_at": now,
            }
            entry["last_native_learning_signals"] = {
                "custom_surface_suspected": bool(native_learning_signals.get("custom_surface_suspected", False)),
                "reparenting_risk": round(max(0.0, min(float(native_learning_signals.get("reparenting_risk", 0.0) or 0.0), 1.0)), 4),
                "descendant_chain_depth": self._coerce_int(native_learning_signals.get("descendant_chain_depth", 0), minimum=0, maximum=1000, default=0),
                "dialog_chain_depth": self._coerce_int(native_learning_signals.get("dialog_chain_depth", 0), minimum=0, maximum=1000, default=0),
                "owner_chain_depth": self._coerce_int(native_learning_signals.get("owner_chain_depth", 0), minimum=0, maximum=1000, default=0),
                "anomaly_flags": [str(item).strip() for item in native_learning_signals.get("anomaly_flags", []) if str(item).strip()][:8] if isinstance(native_learning_signals.get("anomaly_flags", []), list) else [],
                "updated_at": now,
            }
            entry["last_safe_traversal_summary"] = {
                "candidate_count": self._coerce_int(safe_traversal_plan.get("candidate_count", 0), minimum=0, maximum=10_000_000, default=0),
                "container_counts": dict(safe_traversal_plan.get("container_counts", {})) if isinstance(safe_traversal_plan.get("container_counts", {}), dict) else {},
                "recommended_paths": [str(item).strip() for item in safe_traversal_plan.get("recommended_paths", []) if str(item).strip()][:8] if isinstance(safe_traversal_plan.get("recommended_paths", []), list) else [],
                "recursive_depth_limit": self._coerce_int(safe_traversal_plan.get("recursive_depth_limit", 0), minimum=0, maximum=16, default=0),
                "updated_at": now,
            }
            entry["last_verification_summary"] = self._probe_verification_snapshot(probe_payload=probe_payload, observed_at=now)
            entry["last_native_stabilization_summary"] = self._probe_stabilization_snapshot(probe_payload=probe_payload, wave_payload=wave_payload, observed_at=now)
            entry["version_profile"] = version_profile
            entry["staleness"] = self._staleness_snapshot(
                updated_at=now,
                version_signature=str(version_profile.get("signature", "") or "").strip(),
            )
            harvest_tracker: Dict[str, set[str]] = {
                "menu_command": set(),
                "toolbar_action": set(),
                "ribbon_action": set(),
                "navigation_command": set(),
                "ocr_command_phrase": set(),
                "harvested_hotkey": set(),
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
                row_label, row_hotkeys = self._command_phrase(
                    str(row.get("name", "") or row.get("automation_id", "") or "").strip()
                )
                semantic_roles, container_roles = self._command_semantics_from_row(row)
                self._record_command_harvest(
                    entry=entry,
                    label=row_label or str(row.get("name", "") or row.get("automation_id", "") or "").strip(),
                    control_type=str(row.get("control_type", "") or "").strip(),
                    source="element",
                    hotkeys=[
                        *row_hotkeys,
                        self._normalize_hotkey(row.get("accelerator_key", "")),
                        self._normalize_hotkey(row.get("access_key", ""), treat_as_access_key=True),
                    ],
                    aliases=self._control_aliases(row),
                    semantic_roles=semantic_roles,
                    container_roles=container_roles,
                )
                self._record_harvest_classification(
                    entry=entry,
                    label=row_label or str(row.get("name", "") or row.get("automation_id", "") or "").strip(),
                    semantic_roles=semantic_roles,
                    hotkeys=[
                        *row_hotkeys,
                        self._normalize_hotkey(row.get("accelerator_key", "")),
                        self._normalize_hotkey(row.get("access_key", ""), treat_as_access_key=True),
                    ],
                    tracker=harvest_tracker,
                )

            for row in summary.get("query_candidates", []):
                if not isinstance(row, dict):
                    continue
                self._increment_count(entry.setdefault("command_candidate_counts", {}), self._candidate_label(row))
                candidate_label, candidate_hotkeys = self._command_phrase(self._candidate_label(row))
                self._record_command_harvest(
                    entry=entry,
                    label=candidate_label or self._candidate_label(row),
                    control_type=str(row.get("control_type", "") or "").strip(),
                    source="query_candidate",
                    hotkeys=candidate_hotkeys,
                    aliases=[
                        str(row.get("name", "") or "").strip(),
                        str(row.get("automation_id", "") or "").strip(),
                        str(row.get("label", "") or "").strip(),
                    ],
                    semantic_roles=["query_candidate"],
                    container_roles=["surface"],
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
                        semantic_roles=["workflow_action"],
                        container_roles=["workflow"],
                    )
                    self._record_harvest_classification(
                        entry=entry,
                        label=action_name.replace("_", " "),
                        semantic_roles=["workflow_action"],
                        hotkeys=hotkeys,
                        tracker=harvest_tracker,
                    )

            for target in observation_payload.get("targets", []):
                if not isinstance(target, dict):
                    continue
                target_label, target_hotkeys = self._command_phrase(str(target.get("text", "") or "").strip())
                if not target_hotkeys and not self._looks_like_command_phrase(target_label):
                    continue
                self._record_command_harvest(
                    entry=entry,
                    label=target_label,
                    control_type="ocr_text",
                    source="ocr",
                    hotkeys=target_hotkeys,
                    aliases=[
                        str(target.get("text", "") or "").strip(),
                        str(target.get("label", "") or "").strip(),
                        str(target.get("match_text", "") or "").strip(),
                    ],
                    semantic_roles=["ocr_command_phrase"],
                    container_roles=["vision"],
                )
                self._record_harvest_classification(
                    entry=entry,
                    label=target_label,
                    semantic_roles=["ocr_command_phrase"],
                    hotkeys=target_hotkeys,
                    tracker=harvest_tracker,
                )

            harvest_summary = {
                "menu_command_count": len(harvest_tracker["menu_command"]),
                "toolbar_action_count": len(harvest_tracker["toolbar_action"]),
                "ribbon_action_count": len(harvest_tracker["ribbon_action"]),
                "navigation_command_count": len(harvest_tracker["navigation_command"]),
                "ocr_command_phrase_count": len(harvest_tracker["ocr_command_phrase"]),
                "harvested_hotkey_count": len(harvest_tracker["harvested_hotkey"]),
                "updated_at": now,
            }
            metrics["menu_command_count"] = self._coerce_int(metrics.get("menu_command_count", 0), minimum=0, maximum=10_000_000, default=0) + harvest_summary["menu_command_count"]
            metrics["toolbar_action_count"] = self._coerce_int(metrics.get("toolbar_action_count", 0), minimum=0, maximum=10_000_000, default=0) + harvest_summary["toolbar_action_count"]
            metrics["ribbon_action_count"] = self._coerce_int(metrics.get("ribbon_action_count", 0), minimum=0, maximum=10_000_000, default=0) + harvest_summary["ribbon_action_count"]
            metrics["navigation_command_count"] = self._coerce_int(metrics.get("navigation_command_count", 0), minimum=0, maximum=10_000_000, default=0) + harvest_summary["navigation_command_count"]
            metrics["ocr_command_phrase_count"] = self._coerce_int(metrics.get("ocr_command_phrase_count", 0), minimum=0, maximum=10_000_000, default=0) + harvest_summary["ocr_command_phrase_count"]
            metrics["harvested_hotkey_count"] = self._coerce_int(metrics.get("harvested_hotkey_count", 0), minimum=0, maximum=10_000_000, default=0) + harvest_summary["harvested_hotkey_count"]
            entry["metrics"] = metrics
            entry["last_harvest_summary"] = harvest_summary

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
            self._increment_count(entry.setdefault("wave_stop_reason_counts", {}), str(wave_payload.get("stop_reason", "") or "").strip())
            for wave_item in wave_payload.get("items", []):
                if not isinstance(wave_item, dict):
                    continue
                self._record_wave_strategy(
                    entry=entry,
                    row=wave_item,
                    observed_at=now,
                    status="success",
                )
            for wave_item in wave_payload.get("skipped", []):
                if not isinstance(wave_item, dict):
                    continue
                self._record_wave_strategy(
                    entry=entry,
                    row=wave_item,
                    observed_at=now,
                    status=str(wave_item.get("status", "") or "skipped").strip() or "skipped",
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
            native_summary["custom_surface_count"] = self._coerce_int(native_summary.get("custom_surface_count", 0), minimum=0, maximum=10_000_000, default=0) + (1 if bool(native_learning_signals.get("custom_surface_suspected", False)) else 0)
            native_summary["reparenting_risk"] = round(max(float(native_summary.get("reparenting_risk", 0.0) or 0.0), float(native_learning_signals.get("reparenting_risk", 0.0) or 0.0)), 4)
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
                "harvest_summary": dict(harvest_summary),
                "menu_commands": self._top_count_rows(entry.get("menu_command_counts", {}), limit=6, label_field="label"),
                "toolbar_actions": self._top_count_rows(entry.get("toolbar_action_counts", {}), limit=6, label_field="label"),
                "ribbon_actions": self._top_count_rows(entry.get("ribbon_action_counts", {}), limit=6, label_field="label"),
                "navigation_commands": self._top_count_rows(entry.get("navigation_command_counts", {}), limit=6, label_field="label"),
                "ocr_command_phrases": self._top_count_rows(entry.get("ocr_command_phrase_counts", {}), limit=6, label_field="label"),
                "harvested_hotkeys": self._top_count_rows(entry.get("harvested_hotkey_counts", {}), limit=8, label_field="hotkey"),
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
                "wave_summary": dict(entry.get("last_wave_summary", {})) if isinstance(entry.get("last_wave_summary", {}), dict) else {},
                "wave_strategies": self._top_wave_strategies(entry.get("wave_strategies", {}), limit=6),
                "native_summary": dict(native_summary),
                "vision_summary": dict(entry.get("last_vision_summary", {})) if isinstance(entry.get("last_vision_summary", {}), dict) else {},
                "vision_learning_route": dict(entry.get("last_vision_learning_route", {})) if isinstance(entry.get("last_vision_learning_route", {}), dict) else {},
                "verification_summary": dict(entry.get("last_verification_summary", {})) if isinstance(entry.get("last_verification_summary", {}), dict) else {},
                "native_stabilization_summary": dict(entry.get("last_native_stabilization_summary", {})) if isinstance(entry.get("last_native_stabilization_summary", {}), dict) else {},
                "safe_traversal_summary": dict(entry.get("last_safe_traversal_summary", {})) if isinstance(entry.get("last_safe_traversal_summary", {}), dict) else {},
                "version_profile": dict(version_profile),
                "staleness": dict(entry.get("staleness", {})) if isinstance(entry.get("staleness", {}), dict) else {},
                "failure_memory": self._failure_memory_summary(entry),
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
        current["native_stabilized"] = bool(row.get("native_stabilized", False))
        current["last_verification_confidence"] = round(
            max(0.0, min(float(row.get("verification_confidence", 0.0) or 0.0), 1.0)),
            4,
        )
        current["last_verification_summary"] = (
            dict(row.get("verification_summary", {}))
            if isinstance(row.get("verification_summary", {}), dict)
            else {}
        )
        current["last_vision_learning_route"] = (
            dict(row.get("vision_learning_route", {}))
            if isinstance(row.get("vision_learning_route", {}), dict)
            else {}
        )
        current["last_stabilization_summary"] = (
            dict(row.get("stabilization_summary", {}))
            if isinstance(row.get("stabilization_summary", {}), dict)
            else {}
        )
        if bool(row.get("verified_effect", False)):
            current["verified_effect_count"] = self._coerce_int(
                current.get("verified_effect_count", 0),
                minimum=0,
                maximum=10_000_000,
                default=0,
            ) + 1
        elif probe_status:
            current["uncertain_effect_count"] = self._coerce_int(
                current.get("uncertain_effect_count", 0),
                minimum=0,
                maximum=10_000_000,
                default=0,
            ) + 1
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
        if probe_status != "success" or not bool(row.get("verified_effect", False)):
            self._record_failure_memory(
                entry=entry,
                row=row,
                observed_at=observed_at,
                channel="probe",
            )

    def _record_failure_memory(
        self,
        *,
        entry: Dict[str, Any],
        row: Dict[str, Any],
        observed_at: str,
        channel: str,
    ) -> None:
        action_name = self._normalize_text(
            row.get("action", "")
            or row.get("element_id", "")
            or row.get("automation_id", "")
            or row.get("label", "")
            or row.get("query", "")
            or row.get("title", "")
        )
        if not action_name:
            return
        failures = entry.setdefault("failure_memory", {})
        current = failures.get(action_name, {}) if isinstance(failures.get(action_name, {}), dict) else {}
        current = dict(current)
        status = self._normalize_text(
            row.get("probe_status", "")
            or row.get("status", "")
            or row.get("last_status", "")
            or "unknown"
        ) or "unknown"
        current["action"] = action_name
        current["title"] = str(
            row.get("title", "")
            or row.get("label", "")
            or row.get("query", "")
            or action_name.replace("_", " ")
        ).strip()
        current["channel"] = str(channel or "").strip()
        current["sample_count"] = self._coerce_int(current.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        if status in {"blocked", "skipped", "duplicate_surface", "duplicate"}:
            current["blocked_count"] = self._coerce_int(current.get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        elif status == "success" and not bool(row.get("verified_effect", False)):
            current["uncertain_count"] = self._coerce_int(current.get("uncertain_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        else:
            current["error_count"] = self._coerce_int(current.get("error_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        current["last_status"] = status
        current["last_seen_at"] = observed_at
        current["last_message"] = str(row.get("message", "") or row.get("effect_summary", "") or "").strip()
        current["container_role"] = str(row.get("container_role", "") or "").strip()
        current["source"] = str(row.get("source", "") or "").strip()
        current["verification_confidence"] = round(
            max(0.0, min(float(row.get("verification_confidence", 0.0) or 0.0), 1.0)),
            4,
        )
        current["surface_fingerprints"] = self._merge_recent_strings(
            current.get("surface_fingerprints", []),
            [
                str(row.get("pre_surface_fingerprint", "") or "").strip(),
                str(row.get("post_surface_fingerprint", "") or "").strip(),
                str(row.get("surface_fingerprint", "") or "").strip(),
            ],
            limit=10,
        )
        failures[action_name] = current

    def _record_wave_strategy(
        self,
        *,
        entry: Dict[str, Any],
        row: Dict[str, Any],
        observed_at: str,
        status: str,
    ) -> None:
        action_name = self._normalize_text(row.get("action", "") or row.get("element_id", "") or row.get("automation_id", ""))
        title = str(row.get("title", "") or row.get("label", "") or action_name.replace("_", " ")).strip()
        if not action_name:
            return
        strategies = entry.setdefault("wave_strategies", {})
        current = strategies.get(action_name, {}) if isinstance(strategies.get(action_name, {}), dict) else {}
        current = dict(current)
        current["action"] = action_name
        current["title"] = title or action_name.replace("_", " ")
        current["sample_count"] = self._coerce_int(current.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        clean_status = self._normalize_text(status) or "unknown"
        if clean_status == "success":
            current["success_count"] = self._coerce_int(current.get("success_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        elif clean_status in {"skipped", "duplicate_surface", "duplicate"}:
            current["skipped_count"] = self._coerce_int(current.get("skipped_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        else:
            current["error_count"] = self._coerce_int(current.get("error_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        if bool(row.get("known_surface", False)):
            current["known_surface_count"] = self._coerce_int(current.get("known_surface_count", 0), minimum=0, maximum=10_000_000, default=0) + 1
        current["last_status"] = clean_status
        current["last_seen_at"] = observed_at
        current["last_message"] = str(row.get("message", "") or "").strip()
        current["last_surface_fingerprint"] = str(
            row.get("surface_fingerprint", "")
            or row.get("post_surface_fingerprint", "")
            or current.get("last_surface_fingerprint", "")
            or ""
        ).strip()
        current["from_surface_fingerprints"] = self._merge_recent_strings(
            current.get("from_surface_fingerprints", []),
            [str(row.get("pre_surface_fingerprint", "") or "").strip()],
            limit=8,
        )
        current["surface_fingerprints"] = self._merge_recent_strings(
            current.get("surface_fingerprints", []),
            [
                str(row.get("surface_fingerprint", "") or "").strip(),
                str(row.get("post_surface_fingerprint", "") or "").strip(),
            ],
            limit=10,
        )
        current["hotkeys"] = self._merge_recent_strings(
            current.get("hotkeys", []),
            [str(item).strip() for item in row.get("hotkeys", []) if str(item).strip()] if isinstance(row.get("hotkeys", []), list) else [],
            limit=10,
        )
        recommended_followups = [
            str(item).strip().lower()
            for item in row.get("recommended_followups", [])
            if str(item).strip()
        ] if isinstance(row.get("recommended_followups", []), list) else []
        current["recommended_followups"] = self._merge_recent_strings(
            current.get("recommended_followups", []),
            recommended_followups,
            limit=10,
        )
        current["stop_reasons"] = self._merge_recent_strings(
            current.get("stop_reasons", []),
            [str(row.get("stop_reason", "") or "").strip()],
            limit=6,
        )
        strategies[action_name] = current
        self._increment_count(entry.setdefault("wave_action_counts", {}), action_name)
        if clean_status == "success":
            self._increment_count(entry.setdefault("wave_success_action_counts", {}), action_name)
        if bool(row.get("known_surface", False)):
            self._increment_count(entry.setdefault("wave_known_surface_action_counts", {}), action_name)
        for followup in recommended_followups:
            self._increment_count(entry.setdefault("wave_followup_counts", {}), followup)

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
        entry["wave_stop_reason_counts"] = self._trim_count_map(entry.get("wave_stop_reason_counts", {}), limit=12, skip_empty=True)
        entry["menu_command_counts"] = self._trim_count_map(entry.get("menu_command_counts", {}), limit=32, skip_empty=True)
        entry["toolbar_action_counts"] = self._trim_count_map(entry.get("toolbar_action_counts", {}), limit=32, skip_empty=True)
        entry["ribbon_action_counts"] = self._trim_count_map(entry.get("ribbon_action_counts", {}), limit=32, skip_empty=True)
        entry["navigation_command_counts"] = self._trim_count_map(entry.get("navigation_command_counts", {}), limit=32, skip_empty=True)
        entry["ocr_command_phrase_counts"] = self._trim_count_map(entry.get("ocr_command_phrase_counts", {}), limit=32, skip_empty=True)
        entry["harvested_hotkey_counts"] = self._trim_count_map(entry.get("harvested_hotkey_counts", {}), limit=32, skip_empty=True)
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
        wave_strategies = entry.get("wave_strategies", {}) if isinstance(entry.get("wave_strategies", {}), dict) else {}
        if len(wave_strategies) > 32:
            ordered_wave_strategies = sorted(
                wave_strategies.items(),
                key=lambda item: (
                    self._coerce_int(item[1].get("success_count", 0), minimum=0, maximum=10_000_000, default=0),
                    self._coerce_int(item[1].get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                    str(item[1].get("last_seen_at", "")),
                    str(item[0]),
                ),
                reverse=True,
            )
            entry["wave_strategies"] = {key: value for key, value in ordered_wave_strategies[:32]}
        failure_memory = entry.get("failure_memory", {}) if isinstance(entry.get("failure_memory", {}), dict) else {}
        if len(failure_memory) > 48:
            ordered_failures = sorted(
                failure_memory.items(),
                key=lambda item: (
                    self._coerce_int(item[1].get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                    self._coerce_int(item[1].get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0),
                    self._coerce_int(item[1].get("error_count", 0), minimum=0, maximum=10_000_000, default=0),
                    str(item[1].get("last_seen_at", "")),
                    str(item[0]),
                ),
                reverse=True,
            )
            entry["failure_memory"] = {key: value for key, value in ordered_failures[:48]}
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
        item["tested_controls"] = self._top_tested_controls(row.get("controls", {}), limit=8)
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
        item["wave_summary"] = (
            dict(row.get("last_wave_summary", {}))
            if isinstance(row.get("last_wave_summary", {}), dict)
            else {}
        )
        item["surface_nodes"] = self._top_surface_nodes(row.get("surface_nodes", {}), limit=8)
        item["surface_transitions"] = self._top_surface_transitions(row.get("surface_transitions", {}), limit=8)
        item["learned_commands"] = self._top_commands(row.get("learned_commands", {}), limit=10)
        item["wave_strategies"] = self._top_wave_strategies(row.get("wave_strategies", {}), limit=8)
        item["wave_strategy_summary"] = self._wave_strategy_summary(row)
        item["menu_commands"] = self._top_count_rows(row.get("menu_command_counts", {}), limit=8, label_field="label")
        item["toolbar_actions"] = self._top_count_rows(row.get("toolbar_action_counts", {}), limit=8, label_field="label")
        item["ribbon_actions"] = self._top_count_rows(row.get("ribbon_action_counts", {}), limit=8, label_field="label")
        item["navigation_commands"] = self._top_count_rows(row.get("navigation_command_counts", {}), limit=8, label_field="label")
        item["ocr_command_phrases"] = self._top_count_rows(row.get("ocr_command_phrase_counts", {}), limit=8, label_field="label")
        item["harvested_hotkeys"] = self._top_count_rows(row.get("harvested_hotkey_counts", {}), limit=10, label_field="hotkey")
        item["harvest_summary"] = (
            dict(row.get("last_harvest_summary", {}))
            if isinstance(row.get("last_harvest_summary", {}), dict)
            else {
                "menu_command_count": len(row.get("menu_command_counts", {})) if isinstance(row.get("menu_command_counts", {}), dict) else 0,
                "toolbar_action_count": len(row.get("toolbar_action_counts", {})) if isinstance(row.get("toolbar_action_counts", {}), dict) else 0,
                "ribbon_action_count": len(row.get("ribbon_action_counts", {})) if isinstance(row.get("ribbon_action_counts", {}), dict) else 0,
                "navigation_command_count": len(row.get("navigation_command_counts", {})) if isinstance(row.get("navigation_command_counts", {}), dict) else 0,
                "ocr_command_phrase_count": len(row.get("ocr_command_phrase_counts", {})) if isinstance(row.get("ocr_command_phrase_counts", {}), dict) else 0,
                "harvested_hotkey_count": len(row.get("harvested_hotkey_counts", {})) if isinstance(row.get("harvested_hotkey_counts", {}), dict) else 0,
            }
        )
        item["vision_summary"] = (
            dict(row.get("last_vision_summary", {}))
            if isinstance(row.get("last_vision_summary", {}), dict)
            else {}
        )
        item["vision_learning_route"] = (
            dict(row.get("last_vision_learning_route", {}))
            if isinstance(row.get("last_vision_learning_route", {}), dict)
            else {}
        )
        item["native_learning_signals"] = (
            dict(row.get("last_native_learning_signals", {}))
            if isinstance(row.get("last_native_learning_signals", {}), dict)
            else {}
        )
        item["safe_traversal_summary"] = (
            dict(row.get("last_safe_traversal_summary", {}))
            if isinstance(row.get("last_safe_traversal_summary", {}), dict)
            else {}
        )
        item["verification_summary"] = (
            dict(row.get("last_verification_summary", {}))
            if isinstance(row.get("last_verification_summary", {}), dict)
            else {}
        )
        item["native_stabilization_summary"] = (
            dict(row.get("last_native_stabilization_summary", {}))
            if isinstance(row.get("last_native_stabilization_summary", {}), dict)
            else {}
        )
        item["version_profile"] = (
            dict(row.get("version_profile", {}))
            if isinstance(row.get("version_profile", {}), dict)
            else {}
        )
        item["staleness"] = (
            dict(row.get("staleness", {}))
            if isinstance(row.get("staleness", {}), dict)
            else {}
        )
        item["failure_memory"] = self._top_failure_memory(row.get("failure_memory", {}), limit=8)
        item["failure_memory_summary"] = self._failure_memory_summary(row)
        item["discouraged_wave_actions"] = list(dict(item.get("failure_memory_summary", {})).get("discouraged_actions", []))
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
        wave_survey_total = 0
        wave_attempt_total = 0
        wave_success_total = 0
        wave_known_surface_total = 0
        surface_node_total = 0
        surface_transition_total = 0
        learned_command_total = 0
        wave_strategy_total = 0
        discovered_control_total = 0
        command_candidate_total = 0
        menu_command_total = 0
        toolbar_action_total = 0
        ribbon_action_total = 0
        navigation_command_total = 0
        ocr_command_phrase_total = 0
        harvested_hotkey_total = 0
        verification_event_total = 0
        verified_effect_total = 0
        uncertain_effect_total = 0
        safe_traversal_candidate_total = 0
        custom_surface_total = 0
        reparenting_risk_total = 0
        vision_local_runtime_total = 0
        vision_api_assist_total = 0
        native_stabilization_total = 0
        weird_app_surface_total = 0
        stale_entry_total = 0
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
            verification_event_total += self._coerce_int(metrics.get("probe_attempt_count", 0), minimum=0, maximum=10_000_000, default=0)
            verified_effect_total += self._coerce_int(metrics.get("probe_verified_count", 0), minimum=0, maximum=10_000_000, default=0)
            uncertain_effect_total += self._coerce_int(metrics.get("probe_uncertain_count", 0), minimum=0, maximum=10_000_000, default=0)
            wave_survey_total += self._coerce_int(metrics.get("wave_survey_count", 0), minimum=0, maximum=10_000_000, default=0)
            wave_attempt_total += self._coerce_int(metrics.get("wave_attempt_count", 0), minimum=0, maximum=10_000_000, default=0)
            wave_success_total += self._coerce_int(metrics.get("wave_success_count", 0), minimum=0, maximum=10_000_000, default=0)
            wave_known_surface_total += self._coerce_int(metrics.get("wave_known_surface_hit_count", 0), minimum=0, maximum=10_000_000, default=0)
            safe_traversal_candidate_total += self._coerce_int(metrics.get("safe_traversal_candidate_count", 0), minimum=0, maximum=10_000_000, default=0)
            custom_surface_total += self._coerce_int(metrics.get("custom_surface_count", 0), minimum=0, maximum=10_000_000, default=0)
            reparenting_risk_total += self._coerce_int(metrics.get("reparenting_risk_count", 0), minimum=0, maximum=10_000_000, default=0)
            vision_local_runtime_total += self._coerce_int(metrics.get("vision_local_runtime_count", 0), minimum=0, maximum=10_000_000, default=0)
            vision_api_assist_total += self._coerce_int(metrics.get("vision_api_assist_count", 0), minimum=0, maximum=10_000_000, default=0)
            native_stabilization_total += self._coerce_int(metrics.get("native_stabilization_count", 0), minimum=0, maximum=10_000_000, default=0)
            weird_app_surface_total += self._coerce_int(metrics.get("weird_app_surface_count", 0), minimum=0, maximum=10_000_000, default=0)
            surface_node_total += len(row.get("surface_nodes", {})) if isinstance(row.get("surface_nodes", {}), dict) else 0
            surface_transition_total += len(row.get("surface_transitions", {})) if isinstance(row.get("surface_transitions", {}), dict) else 0
            learned_command_total += len(row.get("learned_commands", {})) if isinstance(row.get("learned_commands", {}), dict) else 0
            wave_strategy_total += len(row.get("wave_strategies", {})) if isinstance(row.get("wave_strategies", {}), dict) else 0
            discovered_control_total += len(row.get("controls", {})) if isinstance(row.get("controls", {}), dict) else 0
            command_candidate_total += len(self._trim_count_map(row.get("command_candidate_counts", {}), limit=32))
            menu_command_total += len(row.get("menu_command_counts", {})) if isinstance(row.get("menu_command_counts", {}), dict) else 0
            toolbar_action_total += len(row.get("toolbar_action_counts", {})) if isinstance(row.get("toolbar_action_counts", {}), dict) else 0
            ribbon_action_total += len(row.get("ribbon_action_counts", {})) if isinstance(row.get("ribbon_action_counts", {}), dict) else 0
            navigation_command_total += len(row.get("navigation_command_counts", {})) if isinstance(row.get("navigation_command_counts", {}), dict) else 0
            ocr_command_phrase_total += len(row.get("ocr_command_phrase_counts", {})) if isinstance(row.get("ocr_command_phrase_counts", {}), dict) else 0
            harvested_hotkey_total += len(row.get("harvested_hotkey_counts", {})) if isinstance(row.get("harvested_hotkey_counts", {}), dict) else 0
            if bool(dict(row.get("staleness", {})).get("stale", False)) if isinstance(row.get("staleness", {}), dict) else False:
                stale_entry_total += 1
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
            "verification_event_total": verification_event_total,
            "verified_effect_total": verified_effect_total,
            "uncertain_effect_total": uncertain_effect_total,
            "wave_survey_total": wave_survey_total,
            "wave_attempt_total": wave_attempt_total,
            "wave_success_total": wave_success_total,
            "wave_known_surface_total": wave_known_surface_total,
            "safe_traversal_candidate_total": safe_traversal_candidate_total,
            "custom_surface_total": custom_surface_total,
            "reparenting_risk_total": reparenting_risk_total,
            "vision_local_runtime_total": vision_local_runtime_total,
            "vision_api_assist_total": vision_api_assist_total,
            "native_stabilization_total": native_stabilization_total,
            "weird_app_surface_total": weird_app_surface_total,
            "stale_entry_total": stale_entry_total,
            "surface_node_total": surface_node_total,
            "surface_transition_total": surface_transition_total,
            "learned_command_total": learned_command_total,
            "wave_strategy_total": wave_strategy_total,
            "discovered_control_total": discovered_control_total,
            "command_candidate_total": command_candidate_total,
            "menu_command_total": menu_command_total,
            "toolbar_action_total": toolbar_action_total,
            "ribbon_action_total": ribbon_action_total,
            "navigation_command_total": navigation_command_total,
            "ocr_command_phrase_total": ocr_command_phrase_total,
            "harvested_hotkey_total": harvested_hotkey_total,
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
    def _top_tested_controls(raw: Any, *, limit: int) -> List[Dict[str, Any]]:
        rows = [
            dict(row)
            for row in raw.values()
            if isinstance(raw, dict) and isinstance(row, dict)
            and DesktopAppMemory._coerce_int(row.get("probe_count", 0), minimum=0, maximum=10_000_000, default=0) > 0
        ] if isinstance(raw, dict) else []
        rows.sort(
            key=lambda row: (
                DesktopAppMemory._coerce_int(row.get("probe_count", 0), minimum=0, maximum=10_000_000, default=0),
                DesktopAppMemory._coerce_int(row.get("verified_effect_count", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("last_probe_at", "")),
                str(row.get("label", "")),
            ),
            reverse=True,
        )
        trimmed: List[Dict[str, Any]] = []
        for row in rows[: max(1, int(limit or 1))]:
            trimmed.append(
                {
                    "label": DesktopAppMemory._normalize_text(
                        row.get("label", "") or row.get("identity", "") or ""
                    ),
                    "count": DesktopAppMemory._coerce_int(row.get("probe_count", 0), minimum=0, maximum=10_000_000, default=0),
                    "native_stabilized": bool(row.get("native_stabilized", False)),
                    "last_probe_method": str(row.get("last_probe_method", "") or "").strip(),
                    "last_probe_effect": str(row.get("last_probe_effect", "") or "").strip(),
                    "last_probe_summary": str(row.get("last_probe_summary", "") or "").strip(),
                    "verification_confidence": round(
                        max(0.0, min(float(row.get("last_verification_confidence", 0.0) or 0.0), 1.0)),
                        4,
                    ),
                    "verification_summary": (
                        dict(row.get("last_verification_summary", {}))
                        if isinstance(row.get("last_verification_summary", {}), dict)
                        else {}
                    ),
                    "vision_learning_route": (
                        dict(row.get("last_vision_learning_route", {}))
                        if isinstance(row.get("last_vision_learning_route", {}), dict)
                        else {}
                    ),
                    "stabilization_summary": (
                        dict(row.get("last_stabilization_summary", {}))
                        if isinstance(row.get("last_stabilization_summary", {}), dict)
                        else {}
                    ),
                }
            )
        return trimmed

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
            "probe_verified_count": DesktopAppMemory._coerce_int(metrics.get("probe_verified_count", 0), minimum=0, maximum=10_000_000, default=0),
            "probe_uncertain_count": DesktopAppMemory._coerce_int(metrics.get("probe_uncertain_count", 0), minimum=0, maximum=10_000_000, default=0),
            "probe_blocked_count": DesktopAppMemory._coerce_int(metrics.get("probe_blocked_count", 0), minimum=0, maximum=10_000_000, default=0),
            "probe_error_count": DesktopAppMemory._coerce_int(metrics.get("probe_error_count", 0), minimum=0, maximum=10_000_000, default=0),
            "manual_survey_count": DesktopAppMemory._coerce_int(metrics.get("manual_survey_count", 0), minimum=0, maximum=10_000_000, default=0),
            "batch_survey_count": DesktopAppMemory._coerce_int(metrics.get("batch_survey_count", 0), minimum=0, maximum=10_000_000, default=0),
            "background_survey_count": DesktopAppMemory._coerce_int(metrics.get("background_survey_count", 0), minimum=0, maximum=10_000_000, default=0),
            "wave_survey_count": DesktopAppMemory._coerce_int(metrics.get("wave_survey_count", 0), minimum=0, maximum=10_000_000, default=0),
            "wave_attempt_count": DesktopAppMemory._coerce_int(metrics.get("wave_attempt_count", 0), minimum=0, maximum=10_000_000, default=0),
            "wave_success_count": DesktopAppMemory._coerce_int(metrics.get("wave_success_count", 0), minimum=0, maximum=10_000_000, default=0),
            "wave_known_surface_hit_count": DesktopAppMemory._coerce_int(metrics.get("wave_known_surface_hit_count", 0), minimum=0, maximum=10_000_000, default=0),
            "vision_surface_count": DesktopAppMemory._coerce_int(metrics.get("vision_surface_count", 0), minimum=0, maximum=10_000_000, default=0),
            "safe_traversal_candidate_count": DesktopAppMemory._coerce_int(metrics.get("safe_traversal_candidate_count", 0), minimum=0, maximum=10_000_000, default=0),
            "custom_surface_count": DesktopAppMemory._coerce_int(metrics.get("custom_surface_count", 0), minimum=0, maximum=10_000_000, default=0),
            "reparenting_risk_count": DesktopAppMemory._coerce_int(metrics.get("reparenting_risk_count", 0), minimum=0, maximum=10_000_000, default=0),
            "vision_local_runtime_count": DesktopAppMemory._coerce_int(metrics.get("vision_local_runtime_count", 0), minimum=0, maximum=10_000_000, default=0),
            "vision_api_assist_count": DesktopAppMemory._coerce_int(metrics.get("vision_api_assist_count", 0), minimum=0, maximum=10_000_000, default=0),
            "native_stabilization_count": DesktopAppMemory._coerce_int(metrics.get("native_stabilization_count", 0), minimum=0, maximum=10_000_000, default=0),
            "weird_app_surface_count": DesktopAppMemory._coerce_int(metrics.get("weird_app_surface_count", 0), minimum=0, maximum=10_000_000, default=0),
            "menu_command_count": DesktopAppMemory._coerce_int(metrics.get("menu_command_count", 0), minimum=0, maximum=10_000_000, default=0),
            "toolbar_action_count": DesktopAppMemory._coerce_int(metrics.get("toolbar_action_count", 0), minimum=0, maximum=10_000_000, default=0),
            "ribbon_action_count": DesktopAppMemory._coerce_int(metrics.get("ribbon_action_count", 0), minimum=0, maximum=10_000_000, default=0),
            "navigation_command_count": DesktopAppMemory._coerce_int(metrics.get("navigation_command_count", 0), minimum=0, maximum=10_000_000, default=0),
            "ocr_command_phrase_count": DesktopAppMemory._coerce_int(metrics.get("ocr_command_phrase_count", 0), minimum=0, maximum=10_000_000, default=0),
            "harvested_hotkey_count": DesktopAppMemory._coerce_int(metrics.get("harvested_hotkey_count", 0), minimum=0, maximum=10_000_000, default=0),
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
    def _normalize_hotkey(cls, value: Any, *, treat_as_access_key: bool = False) -> str:
        clean = str(value or "").strip()
        if not clean:
            return ""
        normalized = clean.lower().replace("control", "ctrl").replace("command", "cmd").replace("windows", "win")
        normalized = normalized.replace("escape", "esc").replace("spacebar", "space")
        normalized = re.sub(r"\s+", "", normalized)
        if treat_as_access_key and "+" not in normalized and len(normalized) == 1 and normalized.isalnum():
            return f"alt+{normalized}"
        return normalized

    @classmethod
    def _extract_hotkeys_from_text(cls, text: Any) -> List[str]:
        raw = str(text or "").strip()
        if not raw:
            return []
        hotkeys: List[str] = []
        for match in cls._HOTKEY_PATTERN.finditer(raw):
            normalized = cls._normalize_hotkey(match.group(0))
            if normalized and normalized not in hotkeys:
                hotkeys.append(normalized)
        return hotkeys[:8]

    @classmethod
    def _command_phrase(cls, text: Any) -> tuple[str, List[str]]:
        raw = str(text or "").replace("\u2026", "...").strip()
        if not raw:
            return ("", [])
        hotkeys = cls._extract_hotkeys_from_text(raw)
        label = cls._HOTKEY_PATTERN.sub("", raw)
        if "\t" in label:
            label = label.split("\t", 1)[0]
        label = label.replace("&", " ")
        label = re.sub(r"\s+", " ", label)
        label = label.strip(" -:>\t")
        while label.endswith("..."):
            label = label[:-3].rstrip()
        return (label, hotkeys)

    @classmethod
    def _looks_like_command_phrase(cls, text: Any) -> bool:
        clean = str(text or "").strip()
        if not clean:
            return False
        if len(clean) > 48:
            return False
        words = [part for part in re.split(r"\s+", clean) if part]
        if not words or len(words) > 6:
            return False
        return any(any(char.isalpha() for char in word) for word in words)

    @classmethod
    def _command_semantics_from_row(cls, row: Dict[str, Any]) -> tuple[List[str], List[str]]:
        control_type = cls._normalize_text(row.get("control_type", ""))
        class_name = cls._normalize_text(row.get("class_name", ""))
        automation_id = cls._normalize_text(row.get("automation_id", ""))
        root_window_title = cls._normalize_text(row.get("root_window_title", "") or row.get("window_title", ""))
        haystack = " ".join(part for part in [class_name, automation_id, root_window_title] if part)
        semantic_roles: List[str] = []
        container_roles: List[str] = []
        if control_type == "menuitem":
            semantic_roles.append("menu_command")
            container_roles.append("menu")
        elif control_type in {"button", "splitbutton", "togglebutton"}:
            if "ribbon" in haystack:
                semantic_roles.append("ribbon_action")
                container_roles.append("ribbon")
            elif any(marker in haystack for marker in ("toolbar", "commandbar", "command bar", "quick access", "quickaccess")):
                semantic_roles.append("toolbar_action")
                container_roles.append("toolbar")
            else:
                semantic_roles.append("surface_action")
        elif control_type in {"treeitem", "listitem"}:
            semantic_roles.append("navigation_command")
            container_roles.append("navigation")
        elif control_type == "tabitem":
            semantic_roles.append("tab_action")
            container_roles.append("tabs")
            if "ribbon" in haystack:
                container_roles.append("ribbon")
        elif control_type == "hyperlink":
            semantic_roles.append("link_action")
        return (semantic_roles, cls._merge_recent_strings([], container_roles, limit=4))

    def _record_harvest_classification(
        self,
        *,
        entry: Dict[str, Any],
        label: str,
        semantic_roles: List[str],
        hotkeys: List[str],
        tracker: Dict[str, set[str]],
    ) -> None:
        clean_label = str(label or "").strip()
        normalized_label = self._normalize_text(clean_label)
        role_to_field = {
            "menu_command": "menu_command_counts",
            "toolbar_action": "toolbar_action_counts",
            "ribbon_action": "ribbon_action_counts",
            "navigation_command": "navigation_command_counts",
            "ocr_command_phrase": "ocr_command_phrase_counts",
        }
        for role in semantic_roles:
            field_name = role_to_field.get(self._normalize_text(role))
            if not field_name or not clean_label:
                continue
            self._increment_count(entry.setdefault(field_name, {}), clean_label)
            role_tracker = tracker.get(self._normalize_text(role))
            if isinstance(role_tracker, set) and normalized_label:
                role_tracker.add(normalized_label)
        for hotkey in hotkeys:
            normalized_hotkey = self._normalize_hotkey(hotkey)
            if not normalized_hotkey:
                continue
            self._increment_count(entry.setdefault("harvested_hotkey_counts", {}), normalized_hotkey)
            tracker.setdefault("harvested_hotkey", set()).add(normalized_hotkey)

    @classmethod
    def _learning_health_snapshot(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        metrics = cls._normalize_metrics(row.get("metrics", {}))
        survey_count = cls._coerce_int(metrics.get("survey_count", 0), minimum=0, maximum=10_000_000, default=0)
        success_count = cls._coerce_int(metrics.get("survey_success_count", 0), minimum=0, maximum=10_000_000, default=0)
        failure_count = cls._coerce_int(metrics.get("survey_failure_count", 0), minimum=0, maximum=10_000_000, default=0)
        probe_attempt_count = cls._coerce_int(metrics.get("probe_attempt_count", 0), minimum=0, maximum=10_000_000, default=0)
        probe_success_count = cls._coerce_int(metrics.get("probe_success_count", 0), minimum=0, maximum=10_000_000, default=0)
        probe_verified_count = cls._coerce_int(metrics.get("probe_verified_count", 0), minimum=0, maximum=10_000_000, default=0)
        probe_uncertain_count = cls._coerce_int(metrics.get("probe_uncertain_count", 0), minimum=0, maximum=10_000_000, default=0)
        probe_blocked_count = cls._coerce_int(metrics.get("probe_blocked_count", 0), minimum=0, maximum=10_000_000, default=0)
        probe_error_count = cls._coerce_int(metrics.get("probe_error_count", 0), minimum=0, maximum=10_000_000, default=0)
        last_status = cls._normalize_text(row.get("last_survey_status", "")) or "unknown"
        success_rate = round(float(success_count) / float(survey_count), 4) if survey_count > 0 else 0.0
        probe_success_rate = round(float(probe_success_count) / float(probe_attempt_count), 4) if probe_attempt_count > 0 else 0.0
        verification_rate = round(float(probe_verified_count) / float(probe_attempt_count), 4) if probe_attempt_count > 0 else 0.0
        status = "learning"
        if survey_count <= 0:
            status = "idle"
        elif failure_count <= 0 and last_status in {"success", "partial"} and success_rate >= 0.6 and (probe_attempt_count <= 0 or verification_rate >= 0.4):
            status = "healthy"
        elif failure_count > success_count or last_status == "error" or (probe_attempt_count >= 2 and probe_uncertain_count > probe_verified_count):
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
            "probe_verified_count": probe_verified_count,
            "probe_uncertain_count": probe_uncertain_count,
            "probe_blocked_count": probe_blocked_count,
            "probe_error_count": probe_error_count,
            "probe_success_rate": probe_success_rate,
            "verification_rate": verification_rate,
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
        vision_fusion = snapshot_payload.get("vision_fusion", {}) if isinstance(snapshot_payload.get("vision_fusion", {}), dict) else {}
        vision_learning_route = snapshot_payload.get("vision_learning_route", {}) if isinstance(snapshot_payload.get("vision_learning_route", {}), dict) else {}
        native_learning_signals = snapshot_payload.get("native_learning_signals", {}) if isinstance(snapshot_payload.get("native_learning_signals", {}), dict) else {}
        safe_traversal_plan = snapshot_payload.get("safe_traversal_plan", {}) if isinstance(snapshot_payload.get("safe_traversal_plan", {}), dict) else {}
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
        current["harvested_hotkeys"] = self._merge_recent_strings(
            current.get("harvested_hotkeys", []),
            [
                str(row.get("hotkey", "") or "").strip()
                for row in self._top_count_rows(entry.get("harvested_hotkey_counts", {}), limit=6, label_field="hotkey")
                if isinstance(row, dict)
            ],
            limit=10,
        )
        current["query_examples"] = self._merge_recent_strings(
            current.get("query_examples", []),
            [str(item.get("query", "") or "").strip() for item in entry.get("survey_history", []) if isinstance(item, dict)],
            limit=6,
        )
        current["control_counts"] = summary.get("control_counts", {}) if isinstance(summary.get("control_counts", {}), dict) else {}
        current["harvest_summary"] = dict(entry.get("last_harvest_summary", {})) if isinstance(entry.get("last_harvest_summary", {}), dict) else {}
        current["probe_success_count"] = self._coerce_int(current.get("probe_success_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("successful_count", 0), minimum=0, maximum=10_000_000, default=0)
        current["probe_attempt_count"] = self._coerce_int(current.get("probe_attempt_count", 0), minimum=0, maximum=10_000_000, default=0) + self._coerce_int(probe_payload.get("attempted_count", 0), minimum=0, maximum=10_000_000, default=0)
        current["vision_summary"] = {
            "model_mode": str(vision_fusion.get("model_mode", "") or "").strip(),
            "confidence": round(max(0.0, min(float(vision_fusion.get("confidence", 0.0) or 0.0), 1.0)), 4),
            "top_labels": [str(item).strip() for item in vision_fusion.get("top_labels", []) if str(item).strip()][:8] if isinstance(vision_fusion.get("top_labels", []), list) else [],
            "ocr_terms": [str(item).strip() for item in vision_fusion.get("ocr_terms", []) if str(item).strip()][:10] if isinstance(vision_fusion.get("ocr_terms", []), list) else [],
        }
        current["vision_learning_route"] = {
            "route_profile": str(vision_learning_route.get("route_profile", "") or "").strip(),
            "preferred_probe_mode": str(vision_learning_route.get("preferred_probe_mode", "") or "").strip(),
            "preferred_wave_mode": str(vision_learning_route.get("preferred_wave_mode", "") or "").strip(),
            "model_preference": str(vision_learning_route.get("model_preference", "") or "").strip(),
            "native_recovery_mode": str(vision_learning_route.get("native_recovery_mode", "") or "").strip(),
            "weird_app_pressure": round(max(0.0, min(float(vision_learning_route.get("weird_app_pressure", 0.0) or 0.0), 1.0)), 4),
            "local_runtime_ready": bool(vision_learning_route.get("local_runtime_ready", False)),
            "api_assist_recommended": bool(vision_learning_route.get("api_assist_recommended", False)),
        }
        current["native_learning_signals"] = {
            "custom_surface_suspected": bool(native_learning_signals.get("custom_surface_suspected", False)),
            "reparenting_risk": round(max(0.0, min(float(native_learning_signals.get("reparenting_risk", 0.0) or 0.0), 1.0)), 4),
            "anomaly_flags": [str(item).strip() for item in native_learning_signals.get("anomaly_flags", []) if str(item).strip()][:8] if isinstance(native_learning_signals.get("anomaly_flags", []), list) else [],
        }
        current["safe_traversal_summary"] = {
            "candidate_count": self._coerce_int(safe_traversal_plan.get("candidate_count", 0), minimum=0, maximum=10_000_000, default=0),
            "recommended_paths": [str(item).strip() for item in safe_traversal_plan.get("recommended_paths", []) if str(item).strip()][:8] if isinstance(safe_traversal_plan.get("recommended_paths", []), list) else [],
        }
        current["native_stabilization_summary"] = self._probe_stabilization_snapshot(
            probe_payload=probe_payload,
            wave_payload={},
            observed_at=observed_at,
        )
        current["version_signature"] = str(dict(entry.get("version_profile", {})).get("signature", "") if isinstance(entry.get("version_profile", {}), dict) else "").strip()
        current["staleness"] = dict(entry.get("staleness", {})) if isinstance(entry.get("staleness", {}), dict) else {}
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
        semantic_roles: List[str] | None = None,
        container_roles: List[str] | None = None,
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
        current["semantic_roles"] = self._merge_recent_strings(
            current.get("semantic_roles", []),
            [str(item).strip() for item in (semantic_roles or []) if str(item).strip()],
            limit=8,
        )
        current["container_roles"] = self._merge_recent_strings(
            current.get("container_roles", []),
            [str(item).strip() for item in (container_roles or []) if str(item).strip()],
            limit=8,
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

    @staticmethod
    def _top_wave_strategies(raw: Any, *, limit: int) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in raw.values() if isinstance(raw, dict) and isinstance(row, dict)] if isinstance(raw, dict) else []
        rows.sort(
            key=lambda row: (
                DesktopAppMemory._coerce_int(row.get("success_count", 0), minimum=0, maximum=10_000_000, default=0),
                DesktopAppMemory._coerce_int(row.get("known_surface_count", 0), minimum=0, maximum=10_000_000, default=0),
                DesktopAppMemory._coerce_int(row.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("last_seen_at", "")),
                str(row.get("action", "")),
            ),
            reverse=True,
        )
        return rows[: max(1, int(limit or 1))]

    @classmethod
    def _wave_strategy_summary(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        strategies = cls._top_wave_strategies(row.get("wave_strategies", {}), limit=8)
        recommended_actions = [
            str(item.get("action", "") or "").strip()
            for item in strategies
            if cls._coerce_int(item.get("success_count", 0), minimum=0, maximum=10_000_000, default=0) > 0
        ][:6]
        return {
            "total_actions": len(row.get("wave_strategies", {})) if isinstance(row.get("wave_strategies", {}), dict) else 0,
            "recommended_actions": recommended_actions,
            "top_actions": strategies[:4],
            "stop_reasons": cls._top_count_rows(row.get("wave_stop_reason_counts", {}), limit=6),
        }

    @classmethod
    def _capability_profile_snapshot(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        counts = cls._normalize_count_map(row.get("capability_profile_counts", {}))
        return {
            "status": "success",
            "features": counts,
            "top_features": [{ "value": key, "count": value } for key, value in list(cls._trim_count_map(counts, limit=10).items())],
        }

    @classmethod
    def _top_failure_memory(cls, raw: Any, *, limit: int) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in raw.values() if isinstance(raw, dict) and isinstance(row, dict)] if isinstance(raw, dict) else []
        rows.sort(
            key=lambda row: (
                cls._coerce_int(row.get("sample_count", 0), minimum=0, maximum=10_000_000, default=0),
                cls._coerce_int(row.get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0),
                cls._coerce_int(row.get("error_count", 0), minimum=0, maximum=10_000_000, default=0),
                str(row.get("last_seen_at", "")),
                str(row.get("action", "")),
            ),
            reverse=True,
        )
        return rows[: max(1, int(limit or 1))]

    @classmethod
    def _failure_memory_summary(cls, row: Dict[str, Any]) -> Dict[str, Any]:
        failures = row.get("failure_memory", {}) if isinstance(row.get("failure_memory", {}), dict) else {}
        top_failures = cls._top_failure_memory(failures, limit=8)
        discouraged_actions = [
            str(item.get("action", "") or "").strip()
            for item in top_failures
            if (
                cls._coerce_int(item.get("blocked_count", 0), minimum=0, maximum=10_000_000, default=0) > 0
                or cls._coerce_int(item.get("error_count", 0), minimum=0, maximum=10_000_000, default=0) >= 2
                or cls._coerce_int(item.get("uncertain_count", 0), minimum=0, maximum=10_000_000, default=0) >= 2
                or (
                    cls._coerce_int(item.get("error_count", 0), minimum=0, maximum=10_000_000, default=0) >= 1
                    and cls._normalize_text(item.get("container_role", "")) in {"dialog", "tree", "sidebar", "table"}
                    and max(0.0, min(float(item.get("verification_confidence", 0.0) or 0.0), 1.0)) <= 0.35
                )
            )
        ][:8]
        return {
            "entry_count": len(failures),
            "top_failures": top_failures[:4],
            "discouraged_actions": discouraged_actions,
        }

    @classmethod
    def _probe_verification_snapshot(cls, *, probe_payload: Dict[str, Any], observed_at: str) -> Dict[str, Any]:
        items = [
            dict(item)
            for item in probe_payload.get("items", [])
            if isinstance(probe_payload.get("items", []), list) and isinstance(item, dict)
        ]
        verified_count = cls._coerce_int(probe_payload.get("verified_count", 0), minimum=0, maximum=10_000_000, default=0)
        uncertain_count = cls._coerce_int(probe_payload.get("uncertain_count", 0), minimum=0, maximum=10_000_000, default=0)
        max_confidence = 0.0
        confidence_sum = 0.0
        custom_surface_count = 0
        reparenting_risk_max = 0.0
        for item in items:
            confidence = max(0.0, min(float(item.get("verification_confidence", 0.0) or 0.0), 1.0))
            max_confidence = max(max_confidence, confidence)
            confidence_sum += confidence
            native_signals = item.get("native_learning_signals", {}) if isinstance(item.get("native_learning_signals", {}), dict) else {}
            if bool(native_signals.get("custom_surface_suspected", False)):
                custom_surface_count += 1
            reparenting_risk_max = max(reparenting_risk_max, float(native_signals.get("reparenting_risk", 0.0) or 0.0))
        average_confidence = round(confidence_sum / len(items), 4) if items else 0.0
        return {
            "attempted_count": cls._coerce_int(probe_payload.get("attempted_count", 0), minimum=0, maximum=10_000_000, default=0),
            "verified_count": verified_count,
            "uncertain_count": uncertain_count,
            "verification_rate": round(float(verified_count) / float(len(items)), 4) if items else 0.0,
            "average_confidence": average_confidence,
            "max_confidence": round(max_confidence, 4),
            "custom_surface_count": custom_surface_count,
            "reparenting_risk_max": round(max(0.0, min(reparenting_risk_max, 1.0)), 4),
            "updated_at": observed_at,
        }

    @classmethod
    def _probe_stabilization_snapshot(
        cls,
        *,
        probe_payload: Dict[str, Any],
        wave_payload: Dict[str, Any],
        observed_at: str,
    ) -> Dict[str, Any]:
        probe_items = [
            dict(item)
            for item in probe_payload.get("items", [])
            if isinstance(probe_payload.get("items", []), list) and isinstance(item, dict)
        ]
        wave_items = [
            dict(item)
            for item in wave_payload.get("items", [])
            if isinstance(wave_payload.get("items", []), list) and isinstance(item, dict)
        ]
        stabilized_probe_count = cls._coerce_int(probe_payload.get("stabilized_count", 0), minimum=0, maximum=10_000_000, default=0)
        stabilized_wave_count = cls._coerce_int(wave_payload.get("stabilized_count", 0), minimum=0, maximum=10_000_000, default=0)
        recovery_methods = cls._merge_recent_strings(
            [],
            [
                str(dict(item.get("stabilization_summary", {})).get("recovery_method", "") or "").strip()
                for item in [*probe_items, *wave_items]
                if isinstance(item.get("stabilization_summary", {}), dict)
            ],
            limit=8,
        )
        route_profiles = cls._merge_recent_strings(
            [],
            [
                str(dict(item.get("vision_learning_route", {})).get("route_profile", "") or "").strip()
                for item in probe_items
                if isinstance(item.get("vision_learning_route", {}), dict)
            ],
            limit=8,
        )
        return {
            "stabilized_probe_count": stabilized_probe_count,
            "stabilized_wave_count": stabilized_wave_count,
            "stabilized_total": stabilized_probe_count + stabilized_wave_count,
            "recovery_methods": recovery_methods,
            "route_profiles": route_profiles,
            "updated_at": observed_at,
        }

    @classmethod
    def _version_profile_snapshot(
        cls,
        *,
        app_label: str,
        app_profile: Dict[str, Any],
        target_window: Dict[str, Any],
        active_window: Dict[str, Any],
        launch_result: Dict[str, Any],
        surface_fingerprint: str,
        native_window_topology: Dict[str, Any],
    ) -> Dict[str, Any]:
        signature_parts = [
            cls._normalize_text(app_profile.get("profile_id", "")),
            cls._normalize_text(app_profile.get("name", "")),
            cls._normalize_text(app_label),
            cls._normalize_text(target_window.get("app_name", "") or active_window.get("app_name", "")),
            cls._normalize_text(target_window.get("class_name", "") or active_window.get("class_name", "")),
            cls._normalize_text(target_window.get("window_signature", "") or active_window.get("window_signature", "")),
            cls._normalize_text(native_window_topology.get("signature", "")),
        ]
        signature = "|".join(part for part in signature_parts if part)[:320] or "generic|app"
        return {
            "profile_id": cls._normalize_text(app_profile.get("profile_id", "")),
            "profile_name": str(app_profile.get("name", "") or "").strip(),
            "category": cls._normalize_text(app_profile.get("category", "")),
            "app_name": str(app_label or "").strip(),
            "launch_method": str(launch_result.get("launch_method", "") or launch_result.get("resolution", "") or "").strip(),
            "window_class": str(target_window.get("class_name", "") or active_window.get("class_name", "") or "").strip(),
            "window_signature": str(target_window.get("window_signature", "") or active_window.get("window_signature", "") or "").strip(),
            "native_signature": str(native_window_topology.get("signature", "") or "").strip(),
            "surface_fingerprint": str(surface_fingerprint or "").strip(),
            "signature": signature,
        }

    @classmethod
    def _staleness_snapshot(
        cls,
        *,
        updated_at: str,
        version_signature: str,
        stale_after_hours: float = 72.0,
    ) -> Dict[str, Any]:
        age_hours = 0.0
        if str(updated_at or "").strip():
            try:
                parsed = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                age_hours = max(0.0, (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds() / 3600.0)
            except Exception:
                age_hours = 0.0
        threshold = max(1.0, float(stale_after_hours or 72.0))
        return {
            "age_hours": round(age_hours, 4),
            "stale_after_hours": round(threshold, 4),
            "stale": age_hours >= threshold,
            "version_signature": str(version_signature or "").strip(),
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
        matched_row: Dict[str, Any] | None = None
        matched_node: Dict[str, Any] = {}
        for row in rows:
            if clean_app_name and clean_app_name not in self._normalize_text(row.get("app_name", "")) and clean_app_name not in self._normalize_text(row.get("window_title", "")):
                continue
            if clean_profile_id and clean_profile_id != self._normalize_text(row.get("profile_id", "")):
                continue
            matched_row = row
            nodes = row.get("surface_nodes", {}) if isinstance(row.get("surface_nodes", {}), dict) else {}
            matched_node = (
                dict(nodes.get(clean_surface_fingerprint, {}))
                if clean_surface_fingerprint and isinstance(nodes.get(clean_surface_fingerprint, {}), dict)
                else {}
            )
            break
        if matched_row is not None:
            wave_strategy_summary = self._wave_strategy_summary(matched_row)
            failure_memory_summary = self._failure_memory_summary(matched_row)
            return {
                "status": "success",
                "known": bool(matched_node),
                "surface_fingerprint": clean_surface_fingerprint,
                "surface_node": matched_node,
                "capability_profile": self._capability_profile_snapshot(matched_row),
                "learned_commands": self._top_commands(matched_row.get("learned_commands", {}), limit=8),
                "wave_strategies": self._top_wave_strategies(matched_row.get("wave_strategies", {}), limit=6),
                "wave_strategy_summary": wave_strategy_summary,
                "recommended_wave_actions": wave_strategy_summary.get("recommended_actions", []),
                "menu_commands": self._top_count_rows(matched_row.get("menu_command_counts", {}), limit=6, label_field="label"),
                "toolbar_actions": self._top_count_rows(matched_row.get("toolbar_action_counts", {}), limit=6, label_field="label"),
                "ribbon_actions": self._top_count_rows(matched_row.get("ribbon_action_counts", {}), limit=6, label_field="label"),
                "navigation_commands": self._top_count_rows(matched_row.get("navigation_command_counts", {}), limit=6, label_field="label"),
                "harvested_hotkeys": self._top_count_rows(matched_row.get("harvested_hotkey_counts", {}), limit=8, label_field="hotkey"),
                "harvest_summary": (
                    dict(matched_row.get("last_harvest_summary", {}))
                    if isinstance(matched_row.get("last_harvest_summary", {}), dict)
                    else {}
                ),
                "vision_summary": (
                    dict(matched_row.get("last_vision_summary", {}))
                    if isinstance(matched_row.get("last_vision_summary", {}), dict)
                    else {}
                ),
                "vision_learning_route": (
                    dict(matched_row.get("last_vision_learning_route", {}))
                    if isinstance(matched_row.get("last_vision_learning_route", {}), dict)
                    else {}
                ),
                "native_learning_signals": (
                    dict(matched_row.get("last_native_learning_signals", {}))
                    if isinstance(matched_row.get("last_native_learning_signals", {}), dict)
                    else {}
                ),
                "safe_traversal_summary": (
                    dict(matched_row.get("last_safe_traversal_summary", {}))
                    if isinstance(matched_row.get("last_safe_traversal_summary", {}), dict)
                    else {}
                ),
                "verification_state": (
                    dict(matched_row.get("last_verification_summary", {}))
                    if isinstance(matched_row.get("last_verification_summary", {}), dict)
                    else {}
                ),
                "native_stabilization_summary": (
                    dict(matched_row.get("last_native_stabilization_summary", {}))
                    if isinstance(matched_row.get("last_native_stabilization_summary", {}), dict)
                    else {}
                ),
                "version_profile": (
                    dict(matched_row.get("version_profile", {}))
                    if isinstance(matched_row.get("version_profile", {}), dict)
                    else {}
                ),
                "staleness": (
                    dict(matched_row.get("staleness", {}))
                    if isinstance(matched_row.get("staleness", {}), dict)
                    else {}
                ),
                "failure_memory_summary": failure_memory_summary,
                "discouraged_wave_actions": list(failure_memory_summary.get("discouraged_actions", [])),
                "shortcut_actions": self._snapshot_item(matched_row).get("shortcut_actions", []),
            }
        return {
            "status": "success",
            "known": False,
            "surface_fingerprint": clean_surface_fingerprint,
            "surface_node": {},
            "capability_profile": {"status": "success", "features": {}, "top_features": []},
            "learned_commands": [],
            "wave_strategies": [],
            "wave_strategy_summary": {"total_actions": 0, "recommended_actions": [], "top_actions": [], "stop_reasons": []},
            "recommended_wave_actions": [],
            "menu_commands": [],
            "toolbar_actions": [],
            "ribbon_actions": [],
            "navigation_commands": [],
            "harvested_hotkeys": [],
            "harvest_summary": {},
            "vision_summary": {},
            "vision_learning_route": {},
            "native_learning_signals": {},
            "safe_traversal_summary": {},
            "verification_state": {},
            "native_stabilization_summary": {},
            "version_profile": {},
            "staleness": {},
            "failure_memory_summary": {"entry_count": 0, "top_failures": [], "discouraged_actions": []},
            "discouraged_wave_actions": [],
            "shortcut_actions": [],
        }
