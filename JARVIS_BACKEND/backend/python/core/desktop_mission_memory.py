from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List


class DesktopMissionMemory:
    _DEFAULT_INSTANCE: "DesktopMissionMemory | None" = None
    _DEFAULT_LOCK = RLock()

    def __init__(
        self,
        *,
        store_path: str = "data/desktop_mission_memory.json",
        max_entries: int = 2000,
        max_page_history: int = 12,
    ) -> None:
        self.store_path = Path(store_path)
        self.max_entries = self._coerce_int(max_entries, minimum=100, maximum=100_000, default=2000)
        self.max_page_history = self._coerce_int(max_page_history, minimum=2, maximum=128, default=12)
        self._lock = RLock()
        self._missions: Dict[str, Dict[str, Any]] = {}
        self._updates_since_save = 0
        self._last_save_monotonic = 0.0
        self._load()

    @classmethod
    def default(cls) -> "DesktopMissionMemory":
        with cls._DEFAULT_LOCK:
            if cls._DEFAULT_INSTANCE is None:
                cls._DEFAULT_INSTANCE = cls()
            return cls._DEFAULT_INSTANCE

    def save_paused_mission(
        self,
        *,
        mission_kind: str,
        args: Dict[str, Any] | None,
        resume_contract: Dict[str, Any] | None,
        blocking_surface: Dict[str, Any] | None,
        mission_payload: Dict[str, Any] | None,
        message: str = "",
        warnings: List[str] | None = None,
    ) -> Dict[str, Any]:
        clean_kind = self._normalize_text(mission_kind)
        if clean_kind not in {"wizard", "form", "exploration"}:
            return {"status": "error", "message": "unsupported mission kind"}
        runtime_args = args if isinstance(args, dict) else {}
        contract = dict(resume_contract) if isinstance(resume_contract, dict) else {}
        surface = dict(blocking_surface) if isinstance(blocking_surface, dict) else {}
        payload = dict(mission_payload) if isinstance(mission_payload, dict) else {}
        now = datetime.now(timezone.utc).isoformat()
        mission_id = str(
            runtime_args.get("mission_id", "") or contract.get("mission_id", "") or surface.get("mission_id", "")
        ).strip()
        resume_signature = str(contract.get("resume_signature", "") or "").strip()
        surface_signature = str(surface.get("surface_signature", "") or "").strip()
        with self._lock:
            existing = dict(self._missions.get(mission_id, {})) if mission_id and isinstance(self._missions.get(mission_id), dict) else {}
            if not existing and (resume_signature or surface_signature):
                for row in self._missions.values():
                    if not isinstance(row, dict):
                        continue
                    if str(row.get("status", "") or "").strip().lower() not in {"paused", "resuming"}:
                        continue
                    if resume_signature and str(row.get("resume_signature", "") or "").strip() == resume_signature:
                        existing = dict(row)
                        break
                    if surface_signature and str(row.get("surface_signature", "") or "").strip() == surface_signature:
                        existing = dict(row)
                        break
            if existing:
                mission_id = str(existing.get("mission_id", "") or mission_id).strip()
            if not mission_id:
                mission_id = self._new_mission_id(
                    mission_kind=clean_kind,
                    app_name=str(runtime_args.get("app_name", "") or contract.get("anchor_app_name", "")).strip(),
                    resume_signature=resume_signature,
                    surface_signature=surface_signature,
                )
            page_history = payload.get("page_history", []) if isinstance(payload.get("page_history", []), list) else []
            row = {
                "mission_id": mission_id,
                "status": "paused",
                "mission_kind": clean_kind,
                "resume_action": str(contract.get("resume_action", "") or "").strip(),
                "resume_signature": resume_signature,
                "surface_signature": surface_signature,
                "app_name": str(runtime_args.get("app_name", "") or contract.get("anchor_app_name", "") or "").strip(),
                "anchor_window_title": str(contract.get("anchor_window_title", "") or "").strip(),
                "blocking_window_title": str(surface.get("window_title", "") or contract.get("blocking_window_title", "") or "").strip(),
                "stop_reason_code": str(payload.get("stop_reason_code", "") or surface.get("stop_reason_code", "") or "").strip(),
                "stop_reason": str(payload.get("stop_reason", "") or message or "").strip(),
                "approval_kind": str(surface.get("approval_kind", "") or "").strip(),
                "dialog_kind": str(surface.get("dialog_kind", "") or "").strip(),
                "risk_level": str(payload.get("risk_level", "") or "").strip(),
                "page_count": self._coerce_int(payload.get("page_count", 0), minimum=0, maximum=100_000, default=0),
                "pages_completed": self._coerce_int(payload.get("pages_completed", 0), minimum=0, maximum=100_000, default=0),
                "requested_target_count": self._coerce_int(payload.get("requested_target_count", 0), minimum=0, maximum=100_000, default=0),
                "resolved_target_count": self._coerce_int(payload.get("resolved_target_count", 0), minimum=0, maximum=100_000, default=0),
                "remaining_target_count": self._coerce_int(payload.get("remaining_target_count", 0), minimum=0, maximum=100_000, default=0),
                "surface_mode": str(payload.get("surface_mode", "") or "").strip(),
                "exploration_query": str(payload.get("exploration_query", "") or runtime_args.get("query", "") or "").strip(),
                "hypothesis_count": self._coerce_int(payload.get("hypothesis_count", 0), minimum=0, maximum=100_000, default=0),
                "branch_action_count": self._coerce_int(payload.get("branch_action_count", 0), minimum=0, maximum=100_000, default=0),
                "attempted_target_count": self._coerce_int(payload.get("attempted_target_count", 0), minimum=0, maximum=100_000, default=0),
                "alternative_target_count": self._coerce_int(payload.get("alternative_target_count", 0), minimum=0, maximum=100_000, default=0),
                "alternative_hypothesis_count": self._coerce_int(payload.get("alternative_hypothesis_count", 0), minimum=0, maximum=100_000, default=0),
                "alternative_branch_action_count": self._coerce_int(payload.get("alternative_branch_action_count", 0), minimum=0, maximum=100_000, default=0),
                "step_count": self._coerce_int(payload.get("step_count", payload.get("page_count", 0)), minimum=0, maximum=100_000, default=0),
                "steps_completed": self._coerce_int(payload.get("steps_completed", payload.get("pages_completed", 0)), minimum=0, maximum=100_000, default=0),
                "max_steps": self._coerce_int(payload.get("max_steps", 0), minimum=0, maximum=100_000, default=0),
                "max_descendant_chain_steps": self._coerce_int(payload.get("max_descendant_chain_steps", 0), minimum=0, maximum=100_000, default=0),
                "max_branch_cascade_steps": self._coerce_int(payload.get("max_branch_cascade_steps", 0), minimum=0, maximum=100_000, default=0),
                "max_branch_family_switches": self._coerce_int(payload.get("max_branch_family_switches", 0), minimum=0, maximum=100_000, default=0),
                "auto_continued": bool(payload.get("auto_continued", False)),
                "selected_action": str(payload.get("selected_action", "") or "").strip(),
                "selected_candidate_id": str(payload.get("selected_candidate_id", "") or "").strip(),
                "selected_candidate_label": str(payload.get("selected_candidate_label", "") or "").strip(),
                "rust_router_hint": str(payload.get("rust_router_hint", "") or "").strip(),
                "rust_loop_risk": bool(payload.get("rust_loop_risk", False)),
                "surface_topology_signature": str(payload.get("surface_topology_signature", "") or "").strip(),
                "topology_visible_window_count": self._coerce_int(payload.get("topology_visible_window_count", 0), minimum=0, maximum=100_000, default=0),
                "topology_dialog_like_count": self._coerce_int(payload.get("topology_dialog_like_count", 0), minimum=0, maximum=100_000, default=0),
                "topology_same_process_window_count": self._coerce_int(payload.get("topology_same_process_window_count", 0), minimum=0, maximum=100_000, default=0),
                "topology_owner_link_count": self._coerce_int(payload.get("topology_owner_link_count", 0), minimum=0, maximum=100_000, default=0),
                "topology_owner_chain_visible": bool(payload.get("topology_owner_chain_visible", False)),
                "topology_same_root_owner_window_count": self._coerce_int(payload.get("topology_same_root_owner_window_count", 0), minimum=0, maximum=100_000, default=0),
                "topology_same_root_owner_dialog_like_count": self._coerce_int(payload.get("topology_same_root_owner_dialog_like_count", 0), minimum=0, maximum=100_000, default=0),
                "topology_active_owner_chain_depth": self._coerce_int(payload.get("topology_active_owner_chain_depth", 0), minimum=0, maximum=100_000, default=0),
                "topology_max_owner_chain_depth": self._coerce_int(payload.get("topology_max_owner_chain_depth", 0), minimum=0, maximum=100_000, default=0),
                "topology_direct_child_window_count": self._coerce_int(payload.get("topology_direct_child_window_count", 0), minimum=0, maximum=100_000, default=0),
                "topology_direct_child_dialog_like_count": self._coerce_int(payload.get("topology_direct_child_dialog_like_count", 0), minimum=0, maximum=100_000, default=0),
                "topology_descendant_chain_depth": self._coerce_int(payload.get("topology_descendant_chain_depth", 0), minimum=0, maximum=100_000, default=0),
                "topology_descendant_dialog_chain_depth": self._coerce_int(payload.get("topology_descendant_dialog_chain_depth", 0), minimum=0, maximum=100_000, default=0),
                "topology_descendant_query_match_count": self._coerce_int(payload.get("topology_descendant_query_match_count", 0), minimum=0, maximum=100_000, default=0),
                "topology_modal_chain_signature": str(payload.get("topology_modal_chain_signature", "") or "").strip(),
                "topology_branch_family_signature": str(payload.get("topology_branch_family_signature", "") or "").strip(),
                "topology_child_chain_signature": str(payload.get("topology_child_chain_signature", "") or "").strip(),
                "transition_kind": str(payload.get("transition_kind", "") or "").strip(),
                "nested_surface_progressed": bool(payload.get("nested_surface_progressed", False)),
                "child_window_adopted": bool(payload.get("child_window_adopted", False)),
                "surface_path_tail": [
                    str(item).strip()
                    for item in payload.get("surface_path_tail", [])
                    if str(item).strip()
                ][:8] if isinstance(payload.get("surface_path_tail", []), list) else [],
                "window_title_history_tail": [
                    str(item).strip()
                    for item in payload.get("window_title_history_tail", [])
                    if str(item).strip()
                ][:8] if isinstance(payload.get("window_title_history_tail", []), list) else [],
                "last_branch_kind": str(payload.get("last_branch_kind", "") or "").strip(),
                "branch_transition_count": self._coerce_int(
                    payload.get(
                        "branch_transition_count",
                        len(payload.get("branch_history", []))
                        if isinstance(payload.get("branch_history", []), list)
                        else 0,
                    ),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "branch_repeat_count": self._coerce_int(
                    payload.get("branch_repeat_count", 0),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "branch_family_signature": str(payload.get("branch_family_signature", "") or "").strip(),
                "branch_family_repeat_count": self._coerce_int(payload.get("branch_family_repeat_count", 0), minimum=0, maximum=100_000, default=0),
                "branch_family_switch_count": self._coerce_int(payload.get("branch_family_switch_count", 0), minimum=0, maximum=100_000, default=0),
                "branch_family_continuity": bool(payload.get("branch_family_continuity", False)),
                "descendant_chain_repeat_count": self._coerce_int(payload.get("descendant_chain_repeat_count", 0), minimum=0, maximum=100_000, default=0),
                "descendant_chain_continuity": bool(payload.get("descendant_chain_continuity", False)),
                "surface_path_depth": self._coerce_int(
                    payload.get(
                        "surface_path_depth",
                        len(payload.get("surface_path_tail", []))
                        if isinstance(payload.get("surface_path_tail", []), list)
                        else 0,
                    ),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "nested_chain_count": self._coerce_int(payload.get("nested_chain_count", 0), minimum=0, maximum=100_000, default=0),
                "child_window_chain_count": self._coerce_int(payload.get("child_window_chain_count", 0), minimum=0, maximum=100_000, default=0),
                "dialog_cascade_count": self._coerce_int(payload.get("dialog_cascade_count", 0), minimum=0, maximum=100_000, default=0),
                "pane_cascade_count": self._coerce_int(payload.get("pane_cascade_count", 0), minimum=0, maximum=100_000, default=0),
                "drilldown_cascade_count": self._coerce_int(payload.get("drilldown_cascade_count", 0), minimum=0, maximum=100_000, default=0),
                "branch_cascade_count": self._coerce_int(payload.get("branch_cascade_count", 0), minimum=0, maximum=100_000, default=0),
                "branch_cascade_kind_count": self._coerce_int(payload.get("branch_cascade_kind_count", 0), minimum=0, maximum=100_000, default=0),
                "branch_cascade_signature": str(payload.get("branch_cascade_signature", "") or "").strip(),
                "branch_history_tail": [
                    dict(item)
                    for item in payload.get("branch_history", [])[-8:]
                    if isinstance(item, dict)
                ] if isinstance(payload.get("branch_history", []), list) else [],
                "nested_progress_count": self._coerce_int(
                    payload.get(
                        "nested_progress_count",
                        sum(
                            1
                            for item in payload.get("attempted_targets", [])
                            if isinstance(item, dict) and bool(item.get("nested_surface_progressed", item.get("progressed", False)))
                        ) if isinstance(payload.get("attempted_targets", []), list) else 0,
                    ),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "attempted_targets_tail": [
                    dict(item)
                    for item in payload.get("attempted_targets", [])[-8:]
                    if isinstance(item, dict)
                ] if isinstance(payload.get("attempted_targets", []), list) else [],
                "surface_signature_history": [
                    str(item).strip()
                    for item in payload.get("surface_signature_history", [])
                    if str(item).strip()
                ][:16] if isinstance(payload.get("surface_signature_history", []), list) else [],
                "pause_count": self._coerce_int(existing.get("pause_count", 0), minimum=0, maximum=100_000, default=0) + 1,
                "resume_attempts": self._coerce_int(existing.get("resume_attempts", 0), minimum=0, maximum=100_000, default=0),
                "last_resume_at": str(existing.get("last_resume_at", "") or "").strip(),
                "latest_result_status": str(payload.get("status", "partial") or "partial").strip().lower() or "partial",
                "latest_result_message": str(message or payload.get("message", "") or "").strip(),
                "warnings": self._dedupe_strings([str(item).strip() for item in (warnings or []) if str(item).strip()])[:16],
                "recommended_actions": self._dedupe_strings([str(item).strip() for item in surface.get("recommended_actions", []) if str(item).strip()])[:12] if isinstance(surface.get("recommended_actions", []), list) else [],
                "resume_contract": {
                    **contract,
                    "mission_id": mission_id,
                },
                "blocking_surface": {
                    **surface,
                    "mission_id": mission_id,
                },
                "final_page": dict(payload.get("final_page", {})) if isinstance(payload.get("final_page", {}), dict) else {},
                "page_history_tail": [dict(item) for item in page_history[-self.max_page_history:] if isinstance(item, dict)],
                "created_at": str(existing.get("created_at", "") or now),
                "updated_at": now,
            }
            self._missions[mission_id] = row
            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
            return {"status": "success", "mission": self._public_row(row)}

    def mark_resumed(
        self,
        *,
        mission_id: str,
        outcome_status: str,
        message: str = "",
        completed: bool = False,
        mission_payload: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_id = str(mission_id or "").strip()
        if not clean_id:
            return {"status": "error", "message": "mission_id is required"}
        outcome = str(outcome_status or "").strip().lower() or "unknown"
        payload = dict(mission_payload) if isinstance(mission_payload, dict) else {}
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            row = dict(self._missions.get(clean_id, {}))
            if not row:
                return {"status": "missing", "mission_id": clean_id}
            row["resume_attempts"] = self._coerce_int(row.get("resume_attempts", 0), minimum=0, maximum=100_000, default=0) + 1
            row["last_resume_at"] = now
            row["updated_at"] = now
            row["latest_result_status"] = outcome
            row["latest_result_message"] = str(message or payload.get("message", "") or "").strip()
            for field_name in (
                "attempted_target_count",
                "alternative_target_count",
                "alternative_hypothesis_count",
                "alternative_branch_action_count",
                "step_count",
                "steps_completed",
                "max_steps",
            ):
                if field_name in payload:
                    row[field_name] = self._coerce_int(payload.get(field_name, row.get(field_name, 0)), minimum=0, maximum=100_000, default=0)
            for field_name in (
                "transition_kind",
                "selected_action",
                "selected_candidate_id",
                "selected_candidate_label",
                "last_branch_kind",
                "rust_router_hint",
                "surface_topology_signature",
                "topology_modal_chain_signature",
                "topology_branch_family_signature",
                "branch_family_signature",
            ):
                if field_name in payload:
                    row[field_name] = str(payload.get(field_name, row.get(field_name, "")) or "").strip()
            for field_name in (
                "nested_surface_progressed",
                "child_window_adopted",
                "auto_continued",
                "rust_loop_risk",
                "topology_owner_chain_visible",
                "branch_family_continuity",
            ):
                if field_name in payload:
                    row[field_name] = bool(payload.get(field_name, row.get(field_name, False)))
            for field_name in (
                "topology_visible_window_count",
                "topology_dialog_like_count",
                "topology_same_process_window_count",
                "topology_owner_link_count",
                "topology_same_root_owner_window_count",
                "topology_same_root_owner_dialog_like_count",
                "topology_active_owner_chain_depth",
                "topology_max_owner_chain_depth",
                "nested_chain_count",
                "child_window_chain_count",
                "dialog_cascade_count",
                "pane_cascade_count",
                "drilldown_cascade_count",
                "branch_cascade_count",
                "branch_cascade_kind_count",
                "max_branch_cascade_steps",
                "max_branch_family_switches",
                "branch_family_repeat_count",
                "branch_family_switch_count",
            ):
                if field_name in payload:
                    row[field_name] = self._coerce_int(
                        payload.get(field_name, row.get(field_name, 0)),
                        minimum=0,
                        maximum=100_000,
                        default=0,
                    )
            if isinstance(payload.get("surface_path_tail", []), list):
                row["surface_path_tail"] = [
                    str(item).strip()
                    for item in payload.get("surface_path_tail", [])
                    if str(item).strip()
                ][:8]
            if isinstance(payload.get("window_title_history_tail", []), list):
                row["window_title_history_tail"] = [
                    str(item).strip()
                    for item in payload.get("window_title_history_tail", [])
                    if str(item).strip()
                ][:8]
            if isinstance(payload.get("branch_history", []), list):
                row["branch_history_tail"] = [
                    dict(item)
                    for item in payload.get("branch_history", [])[-8:]
                    if isinstance(item, dict)
                ]
            if "branch_transition_count" in payload:
                row["branch_transition_count"] = self._coerce_int(
                    payload.get("branch_transition_count", row.get("branch_transition_count", 0)),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                )
            if "branch_repeat_count" in payload:
                row["branch_repeat_count"] = self._coerce_int(
                    payload.get("branch_repeat_count", row.get("branch_repeat_count", 0)),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                )
            if "surface_path_depth" in payload:
                row["surface_path_depth"] = self._coerce_int(
                    payload.get("surface_path_depth", row.get("surface_path_depth", 0)),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                )
            for field_name in (
                "nested_chain_count",
                "child_window_chain_count",
                "dialog_cascade_count",
                "pane_cascade_count",
                "drilldown_cascade_count",
                "branch_cascade_count",
                "branch_cascade_kind_count",
                "max_branch_cascade_steps",
                "max_branch_family_switches",
            ):
                if field_name in payload:
                    row[field_name] = self._coerce_int(
                        payload.get(field_name, row.get(field_name, 0)),
                        minimum=0,
                        maximum=100_000,
                        default=0,
                    )
            if "branch_cascade_signature" in payload:
                row["branch_cascade_signature"] = str(payload.get("branch_cascade_signature", row.get("branch_cascade_signature", "")) or "").strip()
            if "nested_progress_count" in payload:
                row["nested_progress_count"] = self._coerce_int(
                    payload.get("nested_progress_count", row.get("nested_progress_count", 0)),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                )
            if isinstance(payload.get("attempted_targets", []), list):
                row["attempted_targets_tail"] = [
                    dict(item)
                    for item in payload.get("attempted_targets", [])[-8:]
                    if isinstance(item, dict)
                ]
            if isinstance(payload.get("surface_signature_history", []), list):
                row["surface_signature_history"] = [
                    str(item).strip()
                    for item in payload.get("surface_signature_history", [])
                    if str(item).strip()
                ][:16]
            if completed or outcome == "success":
                row["status"] = "completed"
                row["completed_at"] = now
            elif outcome in {"partial", "blocked"}:
                row["status"] = "paused"
            elif outcome == "error":
                row["status"] = "error"
            else:
                row["status"] = "resuming"
            if isinstance(payload.get("final_page", {}), dict) and payload.get("final_page"):
                row["final_page"] = dict(payload.get("final_page", {}))
            self._missions[clean_id] = row
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
            return {"status": "success", "mission": self._public_row(row)}

    def resolve_resume_reference(
        self,
        *,
        mission_id: str = "",
        mission_kind: str = "",
        app_name: str = "",
    ) -> Dict[str, Any]:
        clean_id = str(mission_id or "").strip()
        clean_kind = self._normalize_text(mission_kind)
        clean_app = self._normalize_text(app_name)
        with self._lock:
            rows = [dict(row) for row in self._missions.values()]
        if clean_id:
            row = next((item for item in rows if str(item.get("mission_id", "") or "").strip() == clean_id), {})
            return {"status": "success", "mission": self._public_row(row)} if row else {"status": "missing", "mission_id": clean_id}
        rows = [row for row in rows if str(row.get("status", "") or "").strip().lower() == "paused"]
        if clean_kind:
            rows = [row for row in rows if self._normalize_text(row.get("mission_kind", "")) == clean_kind]
        if clean_app:
            rows = [row for row in rows if self._row_matches_app(row, clean_app)]
        rows.sort(key=lambda row: str(row.get("updated_at", "")), reverse=True)
        if not rows:
            return {"status": "empty", "filters": {"mission_kind": clean_kind, "app_name": clean_app}}
        return {"status": "success", "mission": self._public_row(rows[0])}

    def snapshot(
        self,
        *,
        limit: int = 200,
        mission_id: str = "",
        status: str = "",
        mission_kind: str = "",
        app_name: str = "",
        stop_reason_code: str = "",
    ) -> Dict[str, Any]:
        bounded = self._coerce_int(limit, minimum=1, maximum=5000, default=200)
        clean_id = str(mission_id or "").strip()
        clean_status = self._normalize_text(status)
        clean_kind = self._normalize_text(mission_kind)
        clean_app = self._normalize_text(app_name)
        clean_stop_reason = self._normalize_text(stop_reason_code)
        with self._lock:
            rows = [dict(row) for row in self._missions.values()]
        if clean_id:
            rows = [row for row in rows if str(row.get("mission_id", "") or "").strip() == clean_id]
        if clean_status:
            rows = [row for row in rows if self._normalize_text(row.get("status", "")) == clean_status]
        if clean_kind:
            rows = [row for row in rows if self._normalize_text(row.get("mission_kind", "")) == clean_kind]
        if clean_app:
            rows = [row for row in rows if self._row_matches_app(row, clean_app)]
        if clean_stop_reason:
            rows = [row for row in rows if self._normalize_text(row.get("stop_reason_code", "")) == clean_stop_reason]
        rows.sort(key=lambda row: str(row.get("updated_at", "")), reverse=True)
        public_rows = [self._public_row(row) for row in rows[:bounded]]
        status_counts: Dict[str, int] = {}
        mission_kind_counts: Dict[str, int] = {}
        approval_kind_counts: Dict[str, int] = {}
        recovery_profile_counts: Dict[str, int] = {}
        app_counts: Dict[str, int] = {}
        stop_reason_counts: Dict[str, int] = {}
        resume_ready_count = 0
        manual_attention_count = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            status_key = self._normalize_text(row.get("status", ""))
            if status_key:
                status_counts[status_key] = int(status_counts.get(status_key, 0)) + 1
            mission_kind_key = self._normalize_text(row.get("mission_kind", ""))
            if mission_kind_key:
                mission_kind_counts[mission_kind_key] = int(mission_kind_counts.get(mission_kind_key, 0)) + 1
            approval_kind_key = self._normalize_text(row.get("approval_kind", ""))
            if approval_kind_key:
                approval_kind_counts[approval_kind_key] = int(approval_kind_counts.get(approval_kind_key, 0)) + 1
            recovery_details = self._recovery_profile_details(row)
            recovery_profile_key = str(recovery_details.get("recovery_profile", "") or "").strip().lower()
            if recovery_profile_key:
                recovery_profile_counts[recovery_profile_key] = int(recovery_profile_counts.get(recovery_profile_key, 0)) + 1
            if bool(recovery_details.get("resume_ready", False)):
                resume_ready_count += 1
            if bool(recovery_details.get("manual_attention_required", False)):
                manual_attention_count += 1
            app_key = self._app_bucket(row)
            if app_key:
                app_counts[app_key] = int(app_counts.get(app_key, 0)) + 1
            stop_reason_key = self._normalize_text(row.get("stop_reason_code", ""))
            if stop_reason_key:
                stop_reason_counts[stop_reason_key] = int(stop_reason_counts.get(stop_reason_key, 0)) + 1
        latest_paused = next(
            (
                self._public_row(row)
                for row in rows
                if self._normalize_text(row.get("status", "")) in {"paused", "resuming"}
            ),
            None,
        )
        return {
            "status": "success",
            "count": len(public_rows),
            "total": len(rows),
            "items": public_rows,
            "status_counts": status_counts,
            "mission_kind_counts": mission_kind_counts,
            "approval_kind_counts": approval_kind_counts,
            "recovery_profile_counts": recovery_profile_counts,
            "app_counts": app_counts,
            "stop_reason_counts": stop_reason_counts,
            "resume_ready_count": resume_ready_count,
            "manual_attention_count": manual_attention_count,
            "latest_paused": latest_paused,
            "filters": {
                "mission_id": clean_id,
                "status": clean_status,
                "mission_kind": clean_kind,
                "app_name": clean_app,
                "stop_reason_code": clean_stop_reason,
            },
        }

    def reset(
        self,
        *,
        mission_id: str = "",
        status: str = "",
        mission_kind: str = "",
        app_name: str = "",
    ) -> Dict[str, Any]:
        clean_id = str(mission_id or "").strip()
        clean_status = self._normalize_text(status)
        clean_kind = self._normalize_text(mission_kind)
        clean_app = self._normalize_text(app_name)
        removed = 0
        with self._lock:
            if clean_id:
                if clean_id in self._missions:
                    del self._missions[clean_id]
                    removed = 1
            else:
                keep: Dict[str, Dict[str, Any]] = {}
                for mission_key, row in self._missions.items():
                    row_status = self._normalize_text(row.get("status", ""))
                    row_kind = self._normalize_text(row.get("mission_kind", ""))
                    should_remove = (
                        (clean_status and row_status == clean_status)
                        or (clean_kind and row_kind == clean_kind)
                        or (clean_app and self._row_matches_app(row, clean_app))
                    )
                    if should_remove:
                        removed += 1
                        continue
                    keep[mission_key] = row
                if not any([clean_status, clean_kind, clean_app]):
                    removed = len(self._missions)
                    keep = {}
                self._missions = keep
            if removed > 0:
                self._updates_since_save += removed
                self._maybe_save_locked(force=True)
        return {
            "status": "success",
            "removed": removed,
            "filters": {
                "mission_id": clean_id,
                "status": clean_status,
                "mission_kind": clean_kind,
                "app_name": clean_app,
            },
        }

    def _new_mission_id(self, *, mission_kind: str, app_name: str, resume_signature: str, surface_signature: str) -> str:
        raw = "|".join(
            [
                mission_kind,
                app_name,
                resume_signature,
                surface_signature,
                str(time.time_ns()),
            ]
        )
        return f"dm_{hashlib.sha1(raw.encode('utf-8')).hexdigest()[:14]}"

    def _trim_locked(self) -> None:
        if len(self._missions) <= self.max_entries:
            return
        rows = sorted(
            self._missions.values(),
            key=lambda row: (
                str(row.get("updated_at", "")),
                str(row.get("mission_id", "")),
            ),
            reverse=True,
        )
        keep_ids = {
            str(row.get("mission_id", "") or "").strip()
            for row in rows[: self.max_entries]
            if isinstance(row, dict)
        }
        self._missions = {
            mission_id: row
            for mission_id, row in self._missions.items()
            if mission_id in keep_ids
        }

    def _load(self) -> None:
        if not self.store_path.exists():
            return
        try:
            payload = json.loads(self.store_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return
        if not isinstance(payload, dict):
            return
        missions = payload.get("missions", {}) if isinstance(payload.get("missions", {}), dict) else {}
        self._missions = {
            str(key): dict(value)
            for key, value in missions.items()
            if str(key).strip() and isinstance(value, dict)
        }

    def _save_locked(self) -> None:
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "mission_count": len(self._missions),
            "missions": self._missions,
        }
        temp_path = self.store_path.with_suffix(self.store_path.suffix + ".tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
        temp_path.replace(self.store_path)
        self._updates_since_save = 0
        self._last_save_monotonic = time.monotonic()

    def _maybe_save_locked(self, *, force: bool = False) -> None:
        if not force and self._updates_since_save < 2 and (time.monotonic() - self._last_save_monotonic) < 4.0:
            return
        self._save_locked()

    def _public_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(row, dict):
            return {}
        recovery_details = self._recovery_profile_details(row)
        return {
            "mission_id": str(row.get("mission_id", "") or "").strip(),
            "status": str(row.get("status", "") or "").strip(),
            "mission_kind": str(row.get("mission_kind", "") or "").strip(),
            "resume_action": str(row.get("resume_action", "") or "").strip(),
            "resume_signature": str(row.get("resume_signature", "") or "").strip(),
            "surface_signature": str(row.get("surface_signature", "") or "").strip(),
            "app_name": str(row.get("app_name", "") or "").strip(),
            "anchor_window_title": str(row.get("anchor_window_title", "") or "").strip(),
            "blocking_window_title": str(row.get("blocking_window_title", "") or "").strip(),
            "stop_reason_code": str(row.get("stop_reason_code", "") or "").strip(),
            "stop_reason": str(row.get("stop_reason", "") or "").strip(),
            "approval_kind": str(row.get("approval_kind", "") or "").strip(),
            "dialog_kind": str(row.get("dialog_kind", "") or "").strip(),
            "risk_level": str(row.get("risk_level", "") or "").strip(),
            "page_count": self._coerce_int(row.get("page_count", 0), minimum=0, maximum=100_000, default=0),
            "pages_completed": self._coerce_int(row.get("pages_completed", 0), minimum=0, maximum=100_000, default=0),
            "requested_target_count": self._coerce_int(row.get("requested_target_count", 0), minimum=0, maximum=100_000, default=0),
            "resolved_target_count": self._coerce_int(row.get("resolved_target_count", 0), minimum=0, maximum=100_000, default=0),
            "remaining_target_count": self._coerce_int(row.get("remaining_target_count", 0), minimum=0, maximum=100_000, default=0),
            "surface_mode": str(row.get("surface_mode", "") or "").strip(),
            "exploration_query": str(row.get("exploration_query", "") or "").strip(),
            "hypothesis_count": self._coerce_int(row.get("hypothesis_count", 0), minimum=0, maximum=100_000, default=0),
            "branch_action_count": self._coerce_int(row.get("branch_action_count", 0), minimum=0, maximum=100_000, default=0),
            "attempted_target_count": self._coerce_int(row.get("attempted_target_count", 0), minimum=0, maximum=100_000, default=0),
            "alternative_target_count": self._coerce_int(row.get("alternative_target_count", 0), minimum=0, maximum=100_000, default=0),
            "alternative_hypothesis_count": self._coerce_int(row.get("alternative_hypothesis_count", 0), minimum=0, maximum=100_000, default=0),
            "alternative_branch_action_count": self._coerce_int(row.get("alternative_branch_action_count", 0), minimum=0, maximum=100_000, default=0),
            "step_count": self._coerce_int(row.get("step_count", 0), minimum=0, maximum=100_000, default=0),
            "steps_completed": self._coerce_int(row.get("steps_completed", 0), minimum=0, maximum=100_000, default=0),
            "max_steps": self._coerce_int(row.get("max_steps", 0), minimum=0, maximum=100_000, default=0),
            "max_descendant_chain_steps": self._coerce_int(row.get("max_descendant_chain_steps", 0), minimum=0, maximum=100_000, default=0),
            "max_branch_cascade_steps": self._coerce_int(row.get("max_branch_cascade_steps", 0), minimum=0, maximum=100_000, default=0),
            "max_branch_family_switches": self._coerce_int(row.get("max_branch_family_switches", 0), minimum=0, maximum=100_000, default=0),
            "auto_continued": bool(row.get("auto_continued", False)),
            "selected_action": str(row.get("selected_action", "") or "").strip(),
            "selected_candidate_id": str(row.get("selected_candidate_id", "") or "").strip(),
            "selected_candidate_label": str(row.get("selected_candidate_label", "") or "").strip(),
            "rust_router_hint": str(row.get("rust_router_hint", "") or "").strip(),
            "rust_loop_risk": bool(row.get("rust_loop_risk", False)),
            "surface_topology_signature": str(row.get("surface_topology_signature", "") or "").strip(),
            "topology_visible_window_count": self._coerce_int(row.get("topology_visible_window_count", 0), minimum=0, maximum=100_000, default=0),
            "topology_dialog_like_count": self._coerce_int(row.get("topology_dialog_like_count", 0), minimum=0, maximum=100_000, default=0),
            "topology_same_process_window_count": self._coerce_int(row.get("topology_same_process_window_count", 0), minimum=0, maximum=100_000, default=0),
            "topology_owner_link_count": self._coerce_int(row.get("topology_owner_link_count", 0), minimum=0, maximum=100_000, default=0),
            "topology_owner_chain_visible": bool(row.get("topology_owner_chain_visible", False)),
            "topology_same_root_owner_window_count": self._coerce_int(row.get("topology_same_root_owner_window_count", 0), minimum=0, maximum=100_000, default=0),
            "topology_same_root_owner_dialog_like_count": self._coerce_int(row.get("topology_same_root_owner_dialog_like_count", 0), minimum=0, maximum=100_000, default=0),
            "topology_active_owner_chain_depth": self._coerce_int(row.get("topology_active_owner_chain_depth", 0), minimum=0, maximum=100_000, default=0),
            "topology_max_owner_chain_depth": self._coerce_int(row.get("topology_max_owner_chain_depth", 0), minimum=0, maximum=100_000, default=0),
            "topology_direct_child_window_count": self._coerce_int(row.get("topology_direct_child_window_count", 0), minimum=0, maximum=100_000, default=0),
            "topology_direct_child_dialog_like_count": self._coerce_int(row.get("topology_direct_child_dialog_like_count", 0), minimum=0, maximum=100_000, default=0),
            "topology_descendant_chain_depth": self._coerce_int(row.get("topology_descendant_chain_depth", 0), minimum=0, maximum=100_000, default=0),
            "topology_descendant_dialog_chain_depth": self._coerce_int(row.get("topology_descendant_dialog_chain_depth", 0), minimum=0, maximum=100_000, default=0),
            "topology_descendant_query_match_count": self._coerce_int(row.get("topology_descendant_query_match_count", 0), minimum=0, maximum=100_000, default=0),
            "topology_modal_chain_signature": str(row.get("topology_modal_chain_signature", "") or "").strip(),
            "topology_branch_family_signature": str(row.get("topology_branch_family_signature", "") or "").strip(),
            "topology_child_chain_signature": str(row.get("topology_child_chain_signature", "") or "").strip(),
            "transition_kind": str(row.get("transition_kind", "") or "").strip(),
            "nested_surface_progressed": bool(row.get("nested_surface_progressed", False)),
            "child_window_adopted": bool(row.get("child_window_adopted", False)),
            "surface_path_tail": [str(item).strip() for item in row.get("surface_path_tail", []) if str(item).strip()] if isinstance(row.get("surface_path_tail", []), list) else [],
            "window_title_history_tail": [str(item).strip() for item in row.get("window_title_history_tail", []) if str(item).strip()] if isinstance(row.get("window_title_history_tail", []), list) else [],
            "last_branch_kind": str(row.get("last_branch_kind", "") or "").strip(),
            "branch_transition_count": self._coerce_int(row.get("branch_transition_count", 0), minimum=0, maximum=100_000, default=0),
            "branch_repeat_count": self._coerce_int(row.get("branch_repeat_count", 0), minimum=0, maximum=100_000, default=0),
            "branch_family_signature": str(row.get("branch_family_signature", "") or "").strip(),
            "branch_family_repeat_count": self._coerce_int(row.get("branch_family_repeat_count", 0), minimum=0, maximum=100_000, default=0),
            "branch_family_switch_count": self._coerce_int(row.get("branch_family_switch_count", 0), minimum=0, maximum=100_000, default=0),
            "branch_family_continuity": bool(row.get("branch_family_continuity", False)),
            "descendant_chain_repeat_count": self._coerce_int(row.get("descendant_chain_repeat_count", 0), minimum=0, maximum=100_000, default=0),
            "descendant_chain_continuity": bool(row.get("descendant_chain_continuity", False)),
            "surface_path_depth": self._coerce_int(row.get("surface_path_depth", 0), minimum=0, maximum=100_000, default=0),
            "nested_chain_count": self._coerce_int(row.get("nested_chain_count", 0), minimum=0, maximum=100_000, default=0),
            "child_window_chain_count": self._coerce_int(row.get("child_window_chain_count", 0), minimum=0, maximum=100_000, default=0),
            "dialog_cascade_count": self._coerce_int(row.get("dialog_cascade_count", 0), minimum=0, maximum=100_000, default=0),
            "pane_cascade_count": self._coerce_int(row.get("pane_cascade_count", 0), minimum=0, maximum=100_000, default=0),
            "drilldown_cascade_count": self._coerce_int(row.get("drilldown_cascade_count", 0), minimum=0, maximum=100_000, default=0),
            "branch_cascade_count": self._coerce_int(row.get("branch_cascade_count", 0), minimum=0, maximum=100_000, default=0),
            "branch_cascade_kind_count": self._coerce_int(row.get("branch_cascade_kind_count", 0), minimum=0, maximum=100_000, default=0),
            "branch_cascade_signature": str(row.get("branch_cascade_signature", "") or "").strip(),
            "branch_history_tail": [dict(item) for item in row.get("branch_history_tail", []) if isinstance(item, dict)] if isinstance(row.get("branch_history_tail", []), list) else [],
            "nested_progress_count": self._coerce_int(row.get("nested_progress_count", 0), minimum=0, maximum=100_000, default=0),
            "attempted_targets_tail": [dict(item) for item in row.get("attempted_targets_tail", []) if isinstance(item, dict)] if isinstance(row.get("attempted_targets_tail", []), list) else [],
            "surface_signature_history": [str(item).strip() for item in row.get("surface_signature_history", []) if str(item).strip()] if isinstance(row.get("surface_signature_history", []), list) else [],
            "pause_count": self._coerce_int(row.get("pause_count", 0), minimum=0, maximum=100_000, default=0),
            "resume_attempts": self._coerce_int(row.get("resume_attempts", 0), minimum=0, maximum=100_000, default=0),
            "latest_result_status": str(row.get("latest_result_status", "") or "").strip(),
            "latest_result_message": str(row.get("latest_result_message", "") or "").strip(),
            "warnings": [str(item).strip() for item in row.get("warnings", []) if str(item).strip()] if isinstance(row.get("warnings", []), list) else [],
            "recommended_actions": [str(item).strip() for item in row.get("recommended_actions", []) if str(item).strip()] if isinstance(row.get("recommended_actions", []), list) else [],
            "recovery_profile": str(recovery_details.get("recovery_profile", "") or "").strip(),
            "recovery_hint": str(recovery_details.get("recovery_hint", "") or "").strip(),
            "recovery_priority": self._coerce_int(
                recovery_details.get("recovery_priority", 0),
                minimum=0,
                maximum=1000,
                default=0,
            ),
            "resume_ready": bool(recovery_details.get("resume_ready", False)),
            "manual_attention_required": bool(recovery_details.get("manual_attention_required", False)),
            "approval_blocked": bool(recovery_details.get("approval_blocked", False)),
            "resume_contract": dict(row.get("resume_contract", {})) if isinstance(row.get("resume_contract", {}), dict) else {},
            "blocking_surface": dict(row.get("blocking_surface", {})) if isinstance(row.get("blocking_surface", {}), dict) else {},
            "final_page": dict(row.get("final_page", {})) if isinstance(row.get("final_page", {}), dict) else {},
            "page_history_tail": [dict(item) for item in row.get("page_history_tail", []) if isinstance(item, dict)] if isinstance(row.get("page_history_tail", []), list) else [],
            "created_at": str(row.get("created_at", "") or "").strip(),
            "updated_at": str(row.get("updated_at", "") or "").strip(),
            "last_resume_at": str(row.get("last_resume_at", "") or "").strip(),
            "completed_at": str(row.get("completed_at", "") or "").strip(),
        }

    def _recovery_profile_details(self, row: Dict[str, Any]) -> Dict[str, Any]:
        status = self._normalize_text(row.get("status", ""))
        approval_kind = self._normalize_text(row.get("approval_kind", ""))
        dialog_kind = self._normalize_text(row.get("dialog_kind", ""))
        stop_reason_code = self._normalize_text(row.get("stop_reason_code", ""))
        blocking_surface = (
            dict(row.get("blocking_surface", {}))
            if isinstance(row.get("blocking_surface", {}), dict)
            else {}
        )
        secure_desktop_likely = bool(blocking_surface.get("secure_desktop_likely", False))

        recovery_profile = "unknown"
        recovery_hint = "Inspect the stored desktop mission before resuming it."
        recovery_priority = 15
        approval_blocked = False
        manual_attention_required = False

        if status == "completed":
            recovery_profile = "completed"
            recovery_hint = "This desktop mission is already complete."
            recovery_priority = 5
        elif status == "error":
            recovery_profile = "failed_retry"
            recovery_hint = "Inspect the last failure and validate the target surface before retrying."
            recovery_priority = 35
            manual_attention_required = True
        elif approval_kind in {"elevation_consent", "elevation_credentials"} or secure_desktop_likely:
            recovery_profile = "admin_review"
            recovery_hint = "Administrator approval is still likely required before JARVIS can continue."
            recovery_priority = 70
            approval_blocked = True
            manual_attention_required = True
        elif approval_kind == "credential_input":
            recovery_profile = "credential_review"
            recovery_hint = "Credentials are likely required before this mission can resume."
            recovery_priority = 74
            approval_blocked = True
            manual_attention_required = True
        elif approval_kind == "permission_review":
            recovery_profile = "permission_review"
            recovery_hint = "Review the permission surface, then let JARVIS resume the mission."
            recovery_priority = 78
            approval_blocked = True
            manual_attention_required = True
        elif any(
            marker in dialog_kind or marker in stop_reason_code
            for marker in ("review", "warning", "destructive", "confirm")
        ):
            recovery_profile = "operator_review"
            recovery_hint = "An operator review surface is likely still in the way of autonomous progress."
            recovery_priority = 72
            approval_blocked = True
            manual_attention_required = True
        elif stop_reason_code in {
            "exploration_followup_available",
            "exploration_nested_branch_available",
            "exploration_step_limit_reached",
            "exploration_nested_branch_limit_reached",
            "exploration_nested_chain_limit_reached",
            "exploration_descendant_chain_limit_reached",
            "exploration_branch_cascade_limit_reached",
            "exploration_branch_family_switch_limit_reached",
        }:
            recovery_profile = "resume_ready"
            recovery_hint = (
                "JARVIS found another safe surface-recon step and can continue exploring this app."
                if stop_reason_code == "exploration_followup_available"
                else (
                    "JARVIS advanced into a deeper nested surface and is ready to continue that exploration branch."
                    if stop_reason_code == "exploration_nested_branch_available"
                    else (
                        "JARVIS paused at the configured recon step limit and is ready to continue this exploration flow."
                        if stop_reason_code == "exploration_step_limit_reached"
                        else (
                            "JARVIS paused at the configured recon step limit after advancing into a deeper nested branch."
                            if stop_reason_code == "exploration_nested_branch_limit_reached"
                            else (
                            "JARVIS paused after a deeper nested window chain and is ready to continue in another bounded wave."
                            if stop_reason_code == "exploration_nested_chain_limit_reached"
                            else (
                                    "JARVIS paused after a stable descendant child-window chain and is ready to continue that deeper modal path."
                                    if stop_reason_code == "exploration_descendant_chain_limit_reached"
                                    else (
                                        "JARVIS paused after a deeper branch cascade and is ready to continue that unsupported-app recovery path."
                                        if stop_reason_code == "exploration_branch_cascade_limit_reached"
                                        else "JARVIS paused after switching across sibling branch families and is ready to continue from the latest stable anchor."
                                    )
                                )
                            )
                        )
                    )
                )
            )
            recovery_priority = {
                "exploration_followup_available": 88,
                "exploration_nested_branch_available": 89,
                "exploration_step_limit_reached": 86,
                "exploration_nested_branch_limit_reached": 87,
                "exploration_nested_chain_limit_reached": 90,
                "exploration_descendant_chain_limit_reached": 93,
                "exploration_branch_cascade_limit_reached": 91,
                "exploration_branch_family_switch_limit_reached": 92,
            }.get(stop_reason_code, 86)
        elif stop_reason_code == "exploration_nested_branch_loop_guard":
            recovery_profile = "surface_review"
            recovery_hint = "JARVIS is revisiting the same nested branch, so inspect the child surface before resuming."
            recovery_priority = 77
            manual_attention_required = True
        elif stop_reason_code in {
            "exploration_manual_review_required",
            "exploration_no_safe_path",
            "exploration_no_progress",
            "exploration_route_unavailable",
        }:
            recovery_profile = "surface_review"
            recovery_hint = "The current unsupported-app surface still needs human review before safe exploration can continue."
            recovery_priority = 76
            manual_attention_required = True
        elif status in {"paused", "resuming"}:
            recovery_profile = "resume_ready"
            recovery_hint = "This mission looks ready for a resume attempt."
            recovery_priority = 90 if status == "paused" else 82
        else:
            recovery_profile = status or "unknown"

        resume_ready = bool(status in {"paused", "resuming"} and not approval_blocked and not manual_attention_required)
        return {
            "recovery_profile": recovery_profile,
            "recovery_hint": recovery_hint,
            "recovery_priority": recovery_priority,
            "resume_ready": resume_ready,
            "manual_attention_required": manual_attention_required,
            "approval_blocked": approval_blocked,
        }

    def _app_bucket(self, row: Dict[str, Any]) -> str:
        candidates = [
            row.get("app_name", ""),
            row.get("anchor_window_title", ""),
            row.get("blocking_window_title", ""),
        ]
        for candidate in candidates:
            clean = self._normalize_text(candidate)
            if clean:
                return clean
        return ""

    def _row_matches_app(self, row: Dict[str, Any], clean_app: str) -> bool:
        if not clean_app:
            return False
        for candidate in (
            row.get("app_name", ""),
            row.get("anchor_window_title", ""),
            row.get("blocking_window_title", ""),
        ):
            if clean_app in self._normalize_text(candidate):
                return True
        return False

    @staticmethod
    def _normalize_text(value: Any) -> str:
        return " ".join(str(value or "").strip().lower().split())

    @staticmethod
    def _dedupe_strings(values: List[str]) -> List[str]:
        rows: List[str] = []
        seen: set[str] = set()
        for value in values:
            clean = str(value or "").strip()
            if not clean:
                continue
            key = clean.lower()
            if key in seen:
                continue
            seen.add(key)
            rows.append(clean)
        return rows

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            number = int(value)
        except Exception:  # noqa: BLE001
            number = default
        return max(minimum, min(maximum, number))
