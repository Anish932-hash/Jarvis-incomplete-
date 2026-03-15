from __future__ import annotations

import copy
import hashlib
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from backend.python.database.local_store import LocalStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ModelSetupMissionMemory:
    def __init__(
        self,
        *,
        state_path: str = "data/model_setup_missions.json",
        keep_missions: int = 48,
        history_limit: int = 8,
    ) -> None:
        self._store = LocalStore(state_path)
        self._keep_missions = self._coerce_int(keep_missions, minimum=4, maximum=512, default=48)
        self._history_limit = self._coerce_int(history_limit, minimum=2, maximum=32, default=8)
        self._lock = threading.RLock()
        self._missions: Dict[str, Dict[str, Any]] = {}
        self._load()

    def record(
        self,
        *,
        mission_payload: Dict[str, Any],
        launch_payload: Optional[Dict[str, Any]] = None,
        selected_action_ids: Optional[Iterable[str]] = None,
        source: str = "launch",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        mission = dict(mission_payload) if isinstance(mission_payload, dict) else {}
        launch = dict(launch_payload) if isinstance(launch_payload, dict) else {}
        workspace = self._workspace_payload(mission)
        workspace_root = str(workspace.get("workspace_root", "") or "").strip()
        manifest_path = str(workspace.get("manifest_path", "") or "").strip()
        mission_id = self._mission_id(workspace_root=workspace_root, manifest_path=manifest_path)
        now = _utc_now_iso()
        pending = self._pending_action_state(mission)
        launch_summary = self._launch_summary(
            launch_payload=launch,
            selected_action_ids=selected_action_ids,
            dry_run=bool(dry_run),
        )
        run_telemetry = pending.get("run_telemetry", {}) if isinstance(pending.get("run_telemetry", {}), dict) else {}
        with self._lock:
            existing = dict(self._missions.get(mission_id, {}))
            launch_history = [
                dict(item)
                for item in existing.get("launch_history_tail", [])
                if isinstance(item, dict)
            ]
            if launch_summary:
                launch_history.append(launch_summary)
            launch_history = launch_history[-self._history_limit :]
            status = self._derive_status(mission=mission, pending=pending, launch_summary=launch_summary)
            record = {
                "mission_id": mission_id,
                "mission_scope": self._scope_key(workspace_root=workspace_root, manifest_path=manifest_path),
                "workspace_root": workspace_root,
                "manifest_path": manifest_path,
                "status": status,
                "mission_status": str(mission.get("mission_status", "") or "").strip().lower(),
                "latest_source": str(source or "launch").strip().lower() or "launch",
                "dry_run": bool(dry_run),
                "launch_count": self._coerce_int(existing.get("launch_count", 0), minimum=0, maximum=1_000_000, default=0)
                + (1 if launch_summary and not bool(dry_run) else 0),
                "resume_count": self._coerce_int(existing.get("resume_count", 0), minimum=0, maximum=1_000_000, default=0)
                + (1 if str(source or "").strip().lower() == "resume" and launch_summary and not bool(dry_run) else 0),
                "preview_count": self._coerce_int(existing.get("preview_count", 0), minimum=0, maximum=1_000_000, default=0)
                + (1 if bool(dry_run) else 0),
                "action_count": pending["action_count"],
                "ready_action_count": pending["ready_action_count"],
                "manual_action_count": pending["manual_action_count"],
                "blocked_action_count": pending["blocked_action_count"],
                "in_progress_count": pending["in_progress_count"],
                "pending_auto_action_ids": pending["pending_auto_action_ids"],
                "pending_manual_action_ids": pending["pending_manual_action_ids"],
                "pending_blocked_action_ids": pending["pending_blocked_action_ids"],
                "in_progress_action_ids": pending["in_progress_action_ids"],
                "pending_auto_action_count": len(pending["pending_auto_action_ids"]),
                "pending_manual_action_count": len(pending["pending_manual_action_ids"]),
                "pending_blocked_action_count": len(pending["pending_blocked_action_ids"]),
                "active_install_run_ids": pending["active_install_run_ids"],
                "active_manual_run_ids": pending["active_manual_run_ids"],
                "active_run_count": len(pending["active_install_run_ids"]) + len(pending["active_manual_run_ids"]),
                "active_install_runs": [
                    dict(item)
                    for item in run_telemetry.get("active_install_runs", [])
                    if isinstance(item, dict)
                ],
                "active_manual_runs": [
                    dict(item)
                    for item in run_telemetry.get("active_manual_runs", [])
                    if isinstance(item, dict)
                ],
                "active_run_summary": (
                    copy.deepcopy(run_telemetry.get("summary", {}))
                    if isinstance(run_telemetry.get("summary", {}), dict)
                    else {}
                ),
                "stalled_run_count": self._coerce_int(
                    pending.get("stalled_run_count", 0),
                    minimum=0,
                    maximum=1_000_000,
                    default=0,
                ),
                "waiting_run_count": self._coerce_int(
                    pending.get("waiting_run_count", 0),
                    minimum=0,
                    maximum=1_000_000,
                    default=0,
                ),
                "active_run_health": str(pending.get("active_run_health", "") or "").strip().lower(),
                "watch_active_runs": bool(pending.get("watch_active_runs", False)),
                "next_poll_s": self._coerce_int(
                    pending.get("next_poll_s", 0),
                    minimum=0,
                    maximum=86_400,
                    default=0,
                ),
                "workspace_ready": bool(self._summary_value(mission, "workspace_ready", False)),
                "stack_ready": bool(self._summary_value(mission, "stack_ready", False)),
                "readiness_score": self._coerce_int(
                    self._summary_value(mission, "readiness_score", 0),
                    minimum=0,
                    maximum=100,
                    default=0,
                ),
                "resume_ready": bool(pending["resume_ready"]),
                "manual_attention_required": bool(pending["manual_attention_required"]),
                "recovery_profile": pending["recovery_profile"],
                "recovery_hint": pending["recovery_hint"],
                "recovery_priority": pending["recovery_priority"],
                "auto_resume_candidate": bool(pending.get("auto_resume_candidate", False)),
                "resume_trigger": str(pending.get("resume_trigger", "") or "").strip(),
                "resume_blockers": self._string_list(pending.get("resume_blockers", []), limit=8),
                "auto_resume_reason": str(pending.get("auto_resume_reason", "") or "").strip(),
                "latest_result_status": str(
                    (launch_summary or {}).get("status", mission.get("mission_status", "unknown")) or "unknown"
                ).strip().lower(),
                "latest_result_message": str(
                    (launch_summary or {}).get("message", pending["recovery_hint"]) or pending["recovery_hint"]
                ).strip(),
                "recommendations": self._string_list(mission.get("recommendations", []), limit=8),
                "latest_launch": launch_summary,
                "launch_history_tail": launch_history,
                "stored_mission": self._trim_mission_payload(mission),
                "created_at": str(existing.get("created_at", "") or now),
                "updated_at": now,
                "completed_at": now if status == "completed" else str(existing.get("completed_at", "") or ""),
            }
            self._missions[mission_id] = record
            self._trim_locked()
            self._persist_locked()
            return {"status": "success", "mission": self._public_row(record)}

    def resolve_resume_reference(
        self,
        *,
        mission_id: str = "",
        workspace_root: str = "",
        manifest_path: str = "",
    ) -> Dict[str, Any]:
        clean_id = str(mission_id or "").strip()
        clean_root = self._normalize_text(workspace_root)
        clean_manifest = self._normalize_text(manifest_path)
        with self._lock:
            rows = [dict(row) for row in self._missions.values()]
        if clean_id:
            row = next((item for item in rows if str(item.get("mission_id", "") or "").strip() == clean_id), {})
            return {"status": "success", "mission": self._public_row(row)} if row else {"status": "missing", "mission_id": clean_id}
        rows = [row for row in rows if self._scope_matches(row, workspace_root=clean_root, manifest_path=clean_manifest)]
        if not rows:
            return {"status": "empty", "filters": {"workspace_root": clean_root, "manifest_path": clean_manifest}}
        rows.sort(key=self._resume_priority_sort_key, reverse=True)
        return {"status": "success", "mission": self._public_row(rows[0])}

    def snapshot(
        self,
        *,
        limit: int = 20,
        mission_id: str = "",
        status: str = "",
        recovery_profile: str = "",
        workspace_root: str = "",
        manifest_path: str = "",
    ) -> Dict[str, Any]:
        bounded = self._coerce_int(limit, minimum=1, maximum=500, default=20)
        clean_id = str(mission_id or "").strip()
        clean_status = self._normalize_text(status)
        clean_profile = self._normalize_text(recovery_profile)
        clean_root = self._normalize_text(workspace_root)
        clean_manifest = self._normalize_text(manifest_path)
        with self._lock:
            rows = [dict(row) for row in self._missions.values()]
        if clean_id:
            rows = [row for row in rows if str(row.get("mission_id", "") or "").strip() == clean_id]
        if clean_status:
            rows = [row for row in rows if self._normalize_text(row.get("status", "")) == clean_status]
        if clean_profile:
            rows = [row for row in rows if self._normalize_text(row.get("recovery_profile", "")) == clean_profile]
        if clean_root or clean_manifest:
            rows = [row for row in rows if self._scope_matches(row, workspace_root=clean_root, manifest_path=clean_manifest)]
        rows.sort(key=lambda row: str(row.get("updated_at", "")), reverse=True)
        public_rows = [self._public_row(row) for row in rows[:bounded]]
        status_counts: Dict[str, int] = {}
        recovery_counts: Dict[str, int] = {}
        resume_ready_count = 0
        manual_attention_count = 0
        running_count = 0
        auto_resume_candidate_count = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            status_key = self._normalize_text(row.get("status", ""))
            if status_key:
                status_counts[status_key] = int(status_counts.get(status_key, 0)) + 1
            recovery_key = self._normalize_text(row.get("recovery_profile", ""))
            if recovery_key:
                recovery_counts[recovery_key] = int(recovery_counts.get(recovery_key, 0)) + 1
            if bool(row.get("resume_ready", False)):
                resume_ready_count += 1
            if bool(row.get("manual_attention_required", False)):
                manual_attention_count += 1
            if status_key == "running":
                running_count += 1
            if bool(row.get("auto_resume_candidate", False)):
                auto_resume_candidate_count += 1
        return {
            "status": "success",
            "count": len(public_rows),
            "total": len(rows),
            "items": public_rows,
            "status_counts": status_counts,
            "recovery_profile_counts": recovery_counts,
            "resume_ready_count": resume_ready_count,
            "manual_attention_count": manual_attention_count,
            "running_count": running_count,
            "auto_resume_candidate_count": auto_resume_candidate_count,
            "latest_resume_ready": next((item for item in public_rows if bool(item.get("resume_ready", False))), None),
            "latest_attention_required": next(
                (item for item in public_rows if bool(item.get("manual_attention_required", False))),
                None,
            ),
            "latest_running": next(
                (item for item in public_rows if str(item.get("status", "")).strip().lower() == "running"),
                None,
            ),
            "latest_auto_resume_candidate": next(
                (item for item in public_rows if bool(item.get("auto_resume_candidate", False))),
                None,
            ),
            "filters": {
                "mission_id": clean_id,
                "status": clean_status,
                "recovery_profile": clean_profile,
                "workspace_root": clean_root,
                "manifest_path": clean_manifest,
            },
        }

    def reset(
        self,
        *,
        mission_id: str = "",
        status: str = "",
        workspace_root: str = "",
        manifest_path: str = "",
    ) -> Dict[str, Any]:
        clean_id = str(mission_id or "").strip()
        clean_status = self._normalize_text(status)
        clean_root = self._normalize_text(workspace_root)
        clean_manifest = self._normalize_text(manifest_path)
        removed = 0
        with self._lock:
            if clean_id:
                if clean_id in self._missions:
                    del self._missions[clean_id]
                    removed = 1
            else:
                keep: Dict[str, Dict[str, Any]] = {}
                for row_id, row in self._missions.items():
                    row_status = self._normalize_text(row.get("status", ""))
                    matches_scope = self._scope_matches(row, workspace_root=clean_root, manifest_path=clean_manifest)
                    should_remove = False
                    if clean_status and row_status == clean_status:
                        should_remove = True
                    if (clean_root or clean_manifest) and matches_scope:
                        should_remove = True
                    if not any([clean_status, clean_root, clean_manifest]):
                        should_remove = True
                    if should_remove:
                        removed += 1
                        continue
                    keep[row_id] = row
                self._missions = keep
            if removed > 0:
                self._persist_locked()
        return {
            "status": "success",
            "removed": removed,
            "filters": {
                "mission_id": clean_id,
                "status": clean_status,
                "workspace_root": clean_root,
                "manifest_path": clean_manifest,
            },
        }

    def _load(self) -> None:
        payload = self._store.get("missions", {})
        data = dict(payload) if isinstance(payload, dict) else {}
        items = data.get("items", {}) if isinstance(data.get("items", {}), dict) else {}
        self._missions = {
            str(key): dict(value)
            for key, value in items.items()
            if str(key).strip() and isinstance(value, dict)
        }

    def _persist_locked(self) -> None:
        self._store.set(
            "missions",
            {
                "updated_at": _utc_now_iso(),
                "count": len(self._missions),
                "items": self._missions,
            },
        )

    def _trim_locked(self) -> None:
        if len(self._missions) <= self._keep_missions:
            return
        rows = sorted(
            self._missions.values(),
            key=lambda row: str(row.get("updated_at", "")),
            reverse=True,
        )
        keep_ids = {
            str(row.get("mission_id", "") or "").strip()
            for row in rows[: self._keep_missions]
            if isinstance(row, dict)
        }
        self._missions = {
            mission_id: row
            for mission_id, row in self._missions.items()
            if mission_id in keep_ids
        }

    def _trim_mission_payload(self, mission: Dict[str, Any]) -> Dict[str, Any]:
        actions = [
            {
                "id": str(action.get("id", "") or "").strip(),
                "kind": str(action.get("kind", "") or "").strip(),
                "stage": str(action.get("stage", "") or "").strip(),
                "title": str(action.get("title", "") or "").strip(),
                "status": str(action.get("status", "") or "").strip(),
                "auto_runnable": bool(action.get("auto_runnable", False)),
                "provider": str(action.get("provider", "") or "").strip(),
                "task": str(action.get("task", "") or "").strip(),
                "blockers": self._string_list(action.get("blockers", []), limit=3),
            }
            for action in mission.get("actions", [])
            if isinstance(action, dict)
        ]
        return {
            "status": str(mission.get("status", "") or "").strip(),
            "generated_at": str(mission.get("generated_at", "") or "").strip(),
            "mission_status": str(mission.get("mission_status", "") or "").strip(),
            "summary": copy.deepcopy(mission.get("summary", {})) if isinstance(mission.get("summary", {}), dict) else {},
            "actions": actions,
            "recommendations": self._string_list(mission.get("recommendations", []), limit=8),
            "workspace": self._workspace_payload(mission),
        }

    def _pending_action_state(self, mission: Dict[str, Any]) -> Dict[str, Any]:
        actions = [dict(action) for action in mission.get("actions", []) if isinstance(action, dict)]
        workspace_payload = self._workspace_payload(mission)
        required_providers = [
            dict(item)
            for item in workspace_payload.get("required_providers", [])
            if isinstance(item, dict)
        ]
        provider_ready_map = {
            self._normalize_text(item.get("provider", "")): bool(item.get("ready", False))
            for item in required_providers
            if self._normalize_text(item.get("provider", ""))
        }
        all_declared_providers_ready = bool(provider_ready_map) and all(provider_ready_map.values())
        preflight_payload = mission.get("preflight", {}) if isinstance(mission.get("preflight", {}), dict) else {}
        preflight_items = [
            dict(item)
            for item in preflight_payload.get("items", [])
            if isinstance(item, dict)
        ]
        preflight_summary = preflight_payload.get("summary", {}) if isinstance(preflight_payload.get("summary", {}), dict) else {}
        preflight_has_blockers = bool(
            self._coerce_int(preflight_summary.get("blocked_count", 0), minimum=0, maximum=1_000_000, default=0) > 0
            or any(self._normalize_text(item.get("status", "")) == "blocked" for item in preflight_items)
        )
        manual_pipeline_payload = mission.get("manual_pipeline", {}) if isinstance(mission.get("manual_pipeline", {}), dict) else {}
        manual_pipeline_items = [
            dict(item)
            for item in manual_pipeline_payload.get("items", [])
            if isinstance(item, dict)
        ]
        manual_pipeline_has_blockers = any(
            self._normalize_text(item.get("status", "")) == "blocked"
            for item in manual_pipeline_items
        )
        pending_auto_ids: List[str] = []
        pending_manual_ids: List[str] = []
        pending_blocked_ids: List[str] = []
        in_progress_ids: List[str] = []
        ready_count = 0
        manual_count = 0
        blocked_count = 0
        in_progress_count = 0
        first_ready_title = ""
        first_attention_title = ""
        provider_blocked = False
        preflight_blocked = False
        manual_pipeline_blocked = False
        workspace_ready = False
        run_telemetry = self._active_run_telemetry(mission)
        run_summary = run_telemetry.get("summary", {}) if isinstance(run_telemetry.get("summary", {}), dict) else {}
        top_run_label = str(run_summary.get("top_label", "") or "").strip()
        top_run_message = str(run_summary.get("top_message", "") or "").strip()
        top_run_health = str(run_summary.get("top_health", "") or "").strip().lower()
        stalled_run_count = self._coerce_int(run_summary.get("stalled_count", 0), minimum=0, maximum=1_000_000, default=0)
        waiting_run_count = self._coerce_int(run_summary.get("waiting_count", 0), minimum=0, maximum=1_000_000, default=0)
        next_poll_s = self._coerce_int(run_summary.get("next_poll_s", 0), minimum=0, maximum=86_400, default=0)
        for action in actions:
            action_id = str(action.get("id", "") or "").strip().lower()
            status_name = str(action.get("status", "") or "").strip().lower()
            stage = str(action.get("stage", "") or "").strip().lower()
            kind = str(action.get("kind", "") or "").strip().lower()
            title = str(action.get("title", "") or "").strip()
            provider_name = self._normalize_text(action.get("provider", ""))
            if not provider_name and kind == "configure_provider_credentials" and ":" in action_id:
                provider_name = self._normalize_text(action_id.split(":", 1)[1])
            if status_name == "ready" and bool(action.get("auto_runnable", False)) and action_id:
                pending_auto_ids.append(action_id)
                ready_count += 1
                if not first_ready_title:
                    first_ready_title = title
                if stage == "workspace":
                    workspace_ready = True
            elif status_name == "manual" and action_id:
                if kind == "configure_provider_credentials":
                    provider_already_ready = bool(
                        provider_ready_map.get(provider_name, False)
                        if provider_name
                        else all_declared_providers_ready
                    )
                    if provider_already_ready:
                        continue
                if kind == "review_manual_pipeline_blockers" and not manual_pipeline_has_blockers:
                    continue
                pending_manual_ids.append(action_id)
                manual_count += 1
                if not first_attention_title:
                    first_attention_title = title
                if kind == "configure_provider_credentials":
                    provider_blocked = True
            elif status_name == "blocked" and action_id:
                if stage == "preflight" and not preflight_has_blockers:
                    continue
                if kind == "review_manual_pipeline_blockers" and not manual_pipeline_has_blockers:
                    continue
                pending_blocked_ids.append(action_id)
                blocked_count += 1
                if not first_attention_title:
                    first_attention_title = title
                if stage == "preflight":
                    preflight_blocked = True
                if kind == "review_manual_pipeline_blockers":
                    manual_pipeline_blocked = True
            elif status_name == "in_progress" and action_id:
                in_progress_ids.append(action_id)
                in_progress_count += 1

        install_run_ids = self._active_run_ids(mission.get("install_runs", {}), key="run_id")
        manual_run_ids = self._active_run_ids(mission.get("manual_runs", {}), key="run_id")
        active_run_count = len(install_run_ids) + len(manual_run_ids)
        resume_ready = bool(pending_auto_ids) and active_run_count <= 0
        manual_attention_required = bool(pending_manual_ids or pending_blocked_ids)
        attention_blockers: List[str] = []
        attention_reason = ""
        if provider_blocked:
            attention_blockers = ["provider_credentials"]
            attention_reason = "Provider credentials still need to be configured and verified."
        elif preflight_blocked:
            attention_blockers = ["preflight_blocked"]
            attention_reason = "Preflight blockers still need operator fixes before setup can continue."
        elif manual_pipeline_blocked:
            attention_blockers = ["manual_conversion"]
            attention_reason = "Manual conversion blockers still need operator attention."
        elif manual_attention_required:
            attention_blockers = ["manual_review"]
            attention_reason = "Manual review is still required before setup can continue."

        recovery_profile = "complete"
        recovery_hint = "The current local model setup mission is complete."
        recovery_priority = 5
        auto_resume_candidate = False
        resume_trigger = "none"
        resume_blockers: List[str] = []
        auto_resume_reason = recovery_hint
        watch_active_runs = active_run_count > 0
        if install_run_ids:
            recovery_profile = "install_running"
            recovery_hint = "Auto-install tasks are still running."
            if stalled_run_count > 0:
                recovery_profile = "install_stalled"
                recovery_hint = "Auto-install tasks appear stalled and should be reviewed."
            if top_run_label:
                recovery_hint = f"{recovery_hint} Active: {top_run_label}."
            if top_run_message:
                recovery_hint = f"{recovery_hint} {top_run_message}"
            recovery_priority = 90 if stalled_run_count > 0 else 88
            if manual_attention_required and attention_blockers:
                resume_trigger = "manual_attention"
                resume_blockers = attention_blockers
                auto_resume_reason = attention_reason or recovery_hint
            elif stalled_run_count > 0:
                resume_trigger = "stalled_active_runs"
                resume_blockers = ["stalled_runs"]
                auto_resume_reason = "At least one auto-install run has not reported progress recently."
            elif bool(pending_auto_ids) and not manual_attention_required:
                auto_resume_candidate = True
                resume_trigger = "after_active_runs"
                resume_blockers = ["active_runs"]
                auto_resume_reason = "Resume after the active auto-install run completes."
        elif manual_run_ids:
            recovery_profile = "manual_pipeline_running"
            recovery_hint = "Manual conversion tasks are still running."
            if stalled_run_count > 0:
                recovery_profile = "manual_pipeline_stalled"
                recovery_hint = "Manual conversion tasks appear stalled and should be reviewed."
            if top_run_label:
                recovery_hint = f"{recovery_hint} Active: {top_run_label}."
            if top_run_message:
                recovery_hint = f"{recovery_hint} {top_run_message}"
            recovery_priority = 86 if stalled_run_count > 0 else 84
            if manual_attention_required and attention_blockers:
                resume_trigger = "manual_attention"
                resume_blockers = attention_blockers
                auto_resume_reason = attention_reason or recovery_hint
            elif stalled_run_count > 0:
                resume_trigger = "stalled_active_runs"
                resume_blockers = ["stalled_runs"]
                auto_resume_reason = "At least one manual conversion run has not reported progress recently."
            elif bool(pending_auto_ids) and not manual_attention_required:
                auto_resume_candidate = True
                resume_trigger = "after_active_runs"
                resume_blockers = ["active_runs"]
                auto_resume_reason = "Resume after the active manual conversion run completes."
        elif provider_blocked:
            recovery_profile = "provider_credentials"
            recovery_hint = first_attention_title or "Provider credentials still need to be configured."
            recovery_priority = 78
            resume_trigger = "manual_attention"
            resume_blockers = ["provider_credentials"]
            auto_resume_reason = "Provider credentials still need to be configured and verified."
        elif preflight_blocked:
            recovery_profile = "preflight_blocked"
            recovery_hint = first_attention_title or "Preflight blockers must be fixed before setup can continue."
            recovery_priority = 82
            resume_trigger = "manual_attention"
            resume_blockers = ["preflight_blocked"]
            auto_resume_reason = "Preflight blockers still need operator fixes before setup can continue."
        elif manual_pipeline_blocked:
            recovery_profile = "manual_conversion"
            recovery_hint = first_attention_title or "Manual conversion blockers still need operator attention."
            recovery_priority = 74
            resume_trigger = "manual_attention"
            resume_blockers = ["manual_conversion"]
            auto_resume_reason = "Manual conversion blockers still need operator attention."
        elif workspace_ready:
            recovery_profile = "workspace_scaffold"
            recovery_hint = first_ready_title or "Scaffold the declared local-model workspace directories first."
            recovery_priority = 91
            auto_resume_candidate = True
            resume_trigger = "ready_now"
            auto_resume_reason = "The workspace scaffold can run immediately."
        elif resume_ready:
            recovery_profile = "resume_ready"
            recovery_hint = first_ready_title or "Resume the next auto-runnable local-model setup actions."
            recovery_priority = 86
            auto_resume_candidate = True
            resume_trigger = "ready_now"
            auto_resume_reason = "Auto-runnable setup actions are ready right now."
        elif manual_attention_required:
            recovery_profile = "manual_review"
            recovery_hint = first_attention_title or "Review the remaining manual setup actions before continuing."
            recovery_priority = 70
            resume_trigger = "manual_attention"
            resume_blockers = ["manual_review"]
            auto_resume_reason = "Manual review is still required before setup can continue."

        if not auto_resume_candidate and not resume_blockers and manual_attention_required:
            resume_blockers = ["manual_attention"]

        return {
            "action_count": len(actions),
            "ready_action_count": ready_count,
            "manual_action_count": manual_count,
            "blocked_action_count": blocked_count,
            "in_progress_count": in_progress_count + active_run_count,
            "pending_auto_action_ids": pending_auto_ids,
            "pending_manual_action_ids": pending_manual_ids,
            "pending_blocked_action_ids": pending_blocked_ids,
            "in_progress_action_ids": in_progress_ids,
            "active_install_run_ids": install_run_ids,
            "active_manual_run_ids": manual_run_ids,
            "resume_ready": resume_ready,
            "manual_attention_required": manual_attention_required,
            "stalled_run_count": stalled_run_count,
            "waiting_run_count": waiting_run_count,
            "active_run_health": top_run_health,
            "watch_active_runs": watch_active_runs,
            "next_poll_s": next_poll_s,
            "recovery_profile": recovery_profile,
            "recovery_hint": recovery_hint,
            "recovery_priority": recovery_priority,
            "auto_resume_candidate": auto_resume_candidate,
            "resume_trigger": resume_trigger,
            "resume_blockers": resume_blockers,
            "auto_resume_reason": auto_resume_reason,
            "run_telemetry": run_telemetry,
        }

    def _derive_status(
        self,
        *,
        mission: Dict[str, Any],
        pending: Dict[str, Any],
        launch_summary: Optional[Dict[str, Any]],
    ) -> str:
        launch_status = str((launch_summary or {}).get("status", "") or "").strip().lower()
        if launch_status == "error":
            return "error"
        if pending["active_install_run_ids"] or pending["active_manual_run_ids"] or pending["in_progress_action_ids"]:
            return "running"
        if bool(pending["resume_ready"]):
            return "resume_ready"
        if bool(pending["manual_attention_required"]):
            return "blocked"
        if pending["action_count"] <= 0 or str(mission.get("mission_status", "") or "").strip().lower() in {"complete", "completed"}:
            return "completed"
        return str(mission.get("mission_status", "unknown") or "unknown").strip().lower()

    def _launch_summary(
        self,
        *,
        launch_payload: Dict[str, Any],
        selected_action_ids: Optional[Iterable[str]],
        dry_run: bool,
    ) -> Optional[Dict[str, Any]]:
        if not launch_payload:
            return None
        items = [dict(item) for item in launch_payload.get("items", []) if isinstance(item, dict)]
        item_status_counts: Dict[str, int] = {}
        for item in items:
            status_name = str(item.get("status", "") or "").strip().lower()
            if status_name:
                item_status_counts[status_name] = int(item_status_counts.get(status_name, 0)) + 1
        accepted_install_run_ids: List[str] = []
        accepted_manual_run_ids: List[str] = []
        for item in items:
            kind = str(item.get("kind", "") or "").strip().lower()
            result_payload = item.get("result", {}) if isinstance(item.get("result", {}), dict) else {}
            run_payload = result_payload.get("run", {}) if isinstance(result_payload.get("run", {}), dict) else {}
            run_id = str(run_payload.get("run_id", "") or "").strip()
            if not run_id:
                continue
            if kind == "launch_setup_install":
                accepted_install_run_ids.append(run_id)
            elif kind == "launch_manual_pipeline":
                accepted_manual_run_ids.append(run_id)
        message = ""
        for item in items:
            if bool(item.get("ok", False)):
                continue
            message = str(item.get("reason", "") or "").strip()
            if not message and isinstance(item.get("result", {}), dict):
                message = str(item["result"].get("message", "") or "").strip()
            if message:
                break
        if not message:
            if bool(dry_run):
                message = "Mission preview prepared."
            elif str(launch_payload.get("status", "") or "").strip().lower() == "success":
                message = "Mission execution completed successfully."
            else:
                message = "Mission execution updated."
        return {
            "status": str(launch_payload.get("status", "unknown") or "unknown").strip().lower(),
            "generated_at": str(launch_payload.get("generated_at", "") or "").strip(),
            "dry_run": bool(launch_payload.get("dry_run", dry_run)),
            "executed_count": self._coerce_int(launch_payload.get("executed_count", 0), minimum=0, maximum=1_000_000, default=0),
            "skipped_count": self._coerce_int(launch_payload.get("skipped_count", 0), minimum=0, maximum=1_000_000, default=0),
            "error_count": self._coerce_int(launch_payload.get("error_count", 0), minimum=0, maximum=1_000_000, default=0),
            "selected_action_ids": self._string_list(selected_action_ids or launch_payload.get("selected_action_ids", []), limit=32),
            "item_status_counts": item_status_counts,
            "accepted_install_run_ids": self._string_list(accepted_install_run_ids, limit=16),
            "accepted_manual_run_ids": self._string_list(accepted_manual_run_ids, limit=16),
            "accepted_install_run_count": len(accepted_install_run_ids),
            "accepted_manual_run_count": len(accepted_manual_run_ids),
            "message": message,
        }

    def _public_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(row, dict):
            return {}
        return {
            "mission_id": str(row.get("mission_id", "") or "").strip(),
            "mission_scope": str(row.get("mission_scope", "") or "").strip(),
            "workspace_root": str(row.get("workspace_root", "") or "").strip(),
            "manifest_path": str(row.get("manifest_path", "") or "").strip(),
            "status": str(row.get("status", "") or "").strip(),
            "mission_status": str(row.get("mission_status", "") or "").strip(),
            "latest_source": str(row.get("latest_source", "") or "").strip(),
            "dry_run": bool(row.get("dry_run", False)),
            "launch_count": self._coerce_int(row.get("launch_count", 0), minimum=0, maximum=1_000_000, default=0),
            "resume_count": self._coerce_int(row.get("resume_count", 0), minimum=0, maximum=1_000_000, default=0),
            "preview_count": self._coerce_int(row.get("preview_count", 0), minimum=0, maximum=1_000_000, default=0),
            "action_count": self._coerce_int(row.get("action_count", 0), minimum=0, maximum=1_000_000, default=0),
            "ready_action_count": self._coerce_int(row.get("ready_action_count", 0), minimum=0, maximum=1_000_000, default=0),
            "manual_action_count": self._coerce_int(row.get("manual_action_count", 0), minimum=0, maximum=1_000_000, default=0),
            "blocked_action_count": self._coerce_int(row.get("blocked_action_count", 0), minimum=0, maximum=1_000_000, default=0),
            "in_progress_count": self._coerce_int(row.get("in_progress_count", 0), minimum=0, maximum=1_000_000, default=0),
            "pending_auto_action_ids": self._string_list(row.get("pending_auto_action_ids", []), limit=64),
            "pending_manual_action_ids": self._string_list(row.get("pending_manual_action_ids", []), limit=64),
            "pending_blocked_action_ids": self._string_list(row.get("pending_blocked_action_ids", []), limit=64),
            "in_progress_action_ids": self._string_list(row.get("in_progress_action_ids", []), limit=64),
            "active_install_run_ids": self._string_list(row.get("active_install_run_ids", []), limit=32),
            "active_manual_run_ids": self._string_list(row.get("active_manual_run_ids", []), limit=32),
            "active_run_count": self._coerce_int(row.get("active_run_count", 0), minimum=0, maximum=1_000_000, default=0),
            "active_install_runs": [
                dict(item)
                for item in row.get("active_install_runs", [])
                if isinstance(item, dict)
            ] if isinstance(row.get("active_install_runs", []), list) else [],
            "active_manual_runs": [
                dict(item)
                for item in row.get("active_manual_runs", [])
                if isinstance(item, dict)
            ] if isinstance(row.get("active_manual_runs", []), list) else [],
            "active_run_summary": (
                copy.deepcopy(row.get("active_run_summary", {}))
                if isinstance(row.get("active_run_summary", {}), dict)
                else {}
            ),
            "stalled_run_count": self._coerce_int(row.get("stalled_run_count", 0), minimum=0, maximum=1_000_000, default=0),
            "waiting_run_count": self._coerce_int(row.get("waiting_run_count", 0), minimum=0, maximum=1_000_000, default=0),
            "active_run_health": str(row.get("active_run_health", "") or "").strip(),
            "watch_active_runs": bool(row.get("watch_active_runs", False)),
            "next_poll_s": self._coerce_int(row.get("next_poll_s", 0), minimum=0, maximum=86_400, default=0),
            "workspace_ready": bool(row.get("workspace_ready", False)),
            "stack_ready": bool(row.get("stack_ready", False)),
            "readiness_score": self._coerce_int(row.get("readiness_score", 0), minimum=0, maximum=100, default=0),
            "resume_ready": bool(row.get("resume_ready", False)),
            "manual_attention_required": bool(row.get("manual_attention_required", False)),
            "recovery_profile": str(row.get("recovery_profile", "") or "").strip(),
            "recovery_hint": str(row.get("recovery_hint", "") or "").strip(),
            "recovery_priority": self._coerce_int(row.get("recovery_priority", 0), minimum=0, maximum=1000, default=0),
            "auto_resume_candidate": bool(row.get("auto_resume_candidate", False)),
            "resume_trigger": str(row.get("resume_trigger", "") or "").strip(),
            "resume_blockers": self._string_list(row.get("resume_blockers", []), limit=8),
            "auto_resume_reason": str(row.get("auto_resume_reason", "") or "").strip(),
            "latest_result_status": str(row.get("latest_result_status", "") or "").strip(),
            "latest_result_message": str(row.get("latest_result_message", "") or "").strip(),
            "recommendations": self._string_list(row.get("recommendations", []), limit=8),
            "latest_launch": copy.deepcopy(row.get("latest_launch", {})) if isinstance(row.get("latest_launch", {}), dict) else {},
            "launch_history_tail": [
                dict(item)
                for item in row.get("launch_history_tail", [])
                if isinstance(item, dict)
            ] if isinstance(row.get("launch_history_tail", []), list) else [],
            "stored_mission": copy.deepcopy(row.get("stored_mission", {})) if isinstance(row.get("stored_mission", {}), dict) else {},
            "created_at": str(row.get("created_at", "") or "").strip(),
            "updated_at": str(row.get("updated_at", "") or "").strip(),
            "completed_at": str(row.get("completed_at", "") or "").strip(),
        }

    def _workspace_payload(self, mission: Dict[str, Any]) -> Dict[str, Any]:
        workspace = mission.get("workspace", {}) if isinstance(mission.get("workspace", {}), dict) else {}
        return {
            "workspace_root": str(workspace.get("workspace_root", "") or "").strip(),
            "manifest_path": str(workspace.get("manifest_path", "") or "").strip(),
            "summary": copy.deepcopy(workspace.get("summary", {})) if isinstance(workspace.get("summary", {}), dict) else {},
            "required_providers": [
                copy.deepcopy(item)
                for item in workspace.get("required_providers", [])
                if isinstance(item, dict)
            ],
        }

    def _scope_matches(self, row: Dict[str, Any], *, workspace_root: str, manifest_path: str) -> bool:
        if not workspace_root and not manifest_path:
            return True
        row_root = self._normalize_text(row.get("workspace_root", ""))
        row_manifest = self._normalize_text(row.get("manifest_path", ""))
        if workspace_root and workspace_root != row_root:
            return False
        if manifest_path and manifest_path != row_manifest:
            return False
        return True

    def _resume_priority_sort_key(self, row: Dict[str, Any]) -> tuple[int, int, str]:
        status_order = {
            "resume_ready": 5,
            "running": 4,
            "blocked": 3,
            "error": 2,
            "completed": 1,
        }
        status_name = self._normalize_text(row.get("status", ""))
        recovery_priority = self._coerce_int(row.get("recovery_priority", 0), minimum=0, maximum=1000, default=0)
        return (
            status_order.get(status_name, 0),
            recovery_priority,
            str(row.get("updated_at", "") or ""),
        )

    def _active_run_ids(self, payload: Any, *, key: str) -> List[str]:
        data = payload if isinstance(payload, dict) else {}
        rows = data.get("items", []) if isinstance(data.get("items", []), list) else []
        values: List[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            status_name = str(row.get("status", "") or "").strip().lower()
            if status_name not in {"queued", "running", "cancelling", "accepted"}:
                continue
            clean = str(row.get(key, "") or "").strip()
            if clean:
                values.append(clean)
        return self._string_list(values, limit=16)

    def _active_run_telemetry(self, mission: Dict[str, Any]) -> Dict[str, Any]:
        install_rows = self._active_run_rows(mission.get("install_runs", {}), run_kind="install")
        manual_rows = self._active_run_rows(mission.get("manual_runs", {}), run_kind="manual")
        active_rows = install_rows + manual_rows
        top_row = self._top_active_run(active_rows)
        percent_values = [
            float(row.get("percent", 0.0) or 0.0)
            for row in active_rows
            if isinstance(row.get("percent"), (int, float))
        ]
        average_percent = round(sum(percent_values) / len(percent_values), 3) if percent_values else 0.0
        health_counts: Dict[str, int] = {}
        stalled_run_ids: List[str] = []
        latest_progress_at = ""
        for row in active_rows:
            health_name = str(row.get("health", "") or "").strip().lower() or "unknown"
            health_counts[health_name] = int(health_counts.get(health_name, 0)) + 1
            if health_name == "stalled":
                run_id = str(row.get("run_id", "") or "").strip()
                if run_id:
                    stalled_run_ids.append(run_id)
            row_progress_at = str(row.get("last_progress_at", "") or "").strip()
            if row_progress_at and row_progress_at > latest_progress_at:
                latest_progress_at = row_progress_at
        stalled_count = int(health_counts.get("stalled", 0))
        waiting_count = int(health_counts.get("waiting", 0))
        queued_count = int(health_counts.get("queued", 0))
        cancelling_count = int(health_counts.get("cancelling", 0))
        active_count = int(health_counts.get("active", 0))
        summary = {
            "active_count": len(active_rows),
            "install_count": len(install_rows),
            "manual_count": len(manual_rows),
            "percent": average_percent,
            "top_run_id": str(top_row.get("run_id", "") or "").strip(),
            "top_run_kind": str(top_row.get("run_kind", "") or "").strip(),
            "top_status": str(top_row.get("status", "") or "").strip(),
            "top_label": str(top_row.get("label", "") or "").strip(),
            "top_message": str(top_row.get("message", "") or "").strip(),
            "top_health": str(top_row.get("health", "") or "").strip(),
            "top_idle_s": self._coerce_int(top_row.get("idle_s", 0), minimum=0, maximum=86_400, default=0),
            "top_last_event_name": str(top_row.get("last_event_name", "") or "").strip(),
            "latest_updated_at": str(top_row.get("updated_at", "") or "").strip(),
            "latest_progress_at": latest_progress_at,
            "active_running_count": active_count,
            "waiting_count": waiting_count,
            "queued_count": queued_count,
            "cancelling_count": cancelling_count,
            "stalled_count": stalled_count,
            "stalled_run_ids": self._string_list(stalled_run_ids, limit=16),
            "watch_recommended": len(active_rows) > 0,
            "next_poll_s": self._recommended_run_poll_seconds(
                active_count=len(active_rows),
                stalled_count=stalled_count,
                waiting_count=waiting_count,
                queued_count=queued_count,
            ),
        }
        return {
            "active_install_runs": install_rows,
            "active_manual_runs": manual_rows,
            "summary": summary,
        }

    def _active_run_rows(self, payload: Any, *, run_kind: str) -> List[Dict[str, Any]]:
        data = payload if isinstance(payload, dict) else {}
        rows = data.get("items", []) if isinstance(data.get("items", []), list) else []
        active_rows: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            status_name = str(row.get("status", "") or "").strip().lower()
            if status_name not in {"queued", "running", "cancelling", "accepted"}:
                continue
            progress = row.get("progress", {}) if isinstance(row.get("progress", {}), dict) else {}
            current_item_name = str(progress.get("current_item_name", "") or "").strip()
            current_item_key = str(progress.get("current_item_key", "") or "").strip()
            current_step_id = str(progress.get("current_step_id", "") or "").strip()
            total_items = self._coerce_int(
                progress.get("total_items", row.get("selected_count", 0)),
                minimum=0,
                maximum=1_000_000,
                default=0,
            )
            completed_items = self._coerce_int(
                progress.get("completed_items", 0),
                minimum=0,
                maximum=1_000_000,
                default=0,
            )
            percent_value = progress.get("percent", 0.0)
            try:
                percent = round(float(percent_value), 3)
            except Exception:
                percent = 0.0
            progress_message = str(progress.get("message", row.get("message", "")) or "").strip()
            last_progress_at = str(row.get("last_progress_at", row.get("updated_at", "")) or "").strip()
            idle_s = self._coerce_int(
                self._seconds_since(last_progress_at or str(row.get("updated_at", "") or "").strip()),
                minimum=0,
                maximum=86_400,
                default=0,
            )
            health = self._classify_active_run_health(status_name=status_name, idle_s=idle_s)
            label = current_item_name or current_item_key or current_step_id or str(row.get("task", "") or "").strip() or str(row.get("run_id", "") or "").strip()
            active_rows.append(
                {
                    "run_id": str(row.get("run_id", "") or "").strip(),
                    "run_kind": str(run_kind or "").strip(),
                    "status": status_name,
                    "task": str(row.get("task", "") or "").strip(),
                    "message": progress_message,
                    "progress_message": progress_message,
                    "label": label,
                    "current_item_name": current_item_name,
                    "current_item_key": current_item_key,
                    "current_step_id": current_step_id,
                    "completed_items": completed_items,
                    "total_items": total_items,
                    "percent": percent,
                    "selected_count": self._coerce_int(row.get("selected_count", 0), minimum=0, maximum=1_000_000, default=0),
                    "progress_event_count": self._coerce_int(row.get("progress_event_count", 0), minimum=0, maximum=1_000_000, default=0),
                    "last_event_name": str(row.get("last_event_name", "") or "").strip(),
                    "last_progress_at": last_progress_at,
                    "idle_s": idle_s,
                    "health": health,
                    "updated_at": str(row.get("updated_at", "") or "").strip(),
                    "started_at": str(row.get("started_at", "") or "").strip(),
                }
            )
        return active_rows[:8]

    @staticmethod
    def _top_active_run(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not rows:
            return {}
        status_order = {
            "running": 4,
            "accepted": 3,
            "queued": 2,
            "cancelling": 1,
        }
        ranked = sorted(
            rows,
            key=lambda row: (
                status_order.get(str(row.get("status", "") or "").strip().lower(), 0),
                float(row.get("percent", 0.0) or 0.0),
                str(row.get("updated_at", "") or ""),
            ),
            reverse=True,
        )
        return dict(ranked[0]) if ranked else {}

    @staticmethod
    def _classify_active_run_health(*, status_name: str, idle_s: int) -> str:
        normalized_status = str(status_name or "").strip().lower()
        if normalized_status == "cancelling":
            return "cancelling"
        if normalized_status in {"accepted", "queued"}:
            if idle_s >= 120:
                return "stalled"
            if idle_s >= 25:
                return "waiting"
            return "queued"
        if idle_s >= 240:
            return "stalled"
        if idle_s >= 45:
            return "waiting"
        return "active"

    @staticmethod
    def _recommended_run_poll_seconds(*, active_count: int, stalled_count: int, waiting_count: int, queued_count: int) -> int:
        if active_count <= 0:
            return 0
        if stalled_count > 0:
            return 6
        if waiting_count > 0:
            return 10
        if queued_count > 0:
            return 8
        return 14

    @staticmethod
    def _seconds_since(timestamp: str) -> int:
        clean = str(timestamp or "").strip()
        if not clean:
            return 0
        try:
            parsed = datetime.fromisoformat(clean.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)
            return max(0, int(delta.total_seconds()))
        except Exception:
            return 0

    def _mission_id(self, *, workspace_root: str, manifest_path: str) -> str:
        scope = self._scope_key(workspace_root=workspace_root, manifest_path=manifest_path)
        digest = hashlib.sha1(scope.encode("utf-8")).hexdigest()[:16]
        return f"msm_{digest}"

    @staticmethod
    def _scope_key(*, workspace_root: str, manifest_path: str) -> str:
        return "|".join(
            [
                str(workspace_root or "").strip().lower(),
                str(manifest_path or "").strip().lower(),
            ]
        )

    @staticmethod
    def _summary_value(payload: Dict[str, Any], key: str, default: Any = None) -> Any:
        summary = payload.get("summary", {}) if isinstance(payload.get("summary", {}), dict) else {}
        return summary.get(key, default)

    @staticmethod
    def _normalize_text(value: Any) -> str:
        return " ".join(str(value or "").strip().lower().split())

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            number = int(value)
        except Exception:
            number = default
        return max(minimum, min(maximum, number))

    @staticmethod
    def _string_list(values: Any, *, limit: int) -> List[str]:
        rows: List[str] = []
        seen: set[str] = set()
        iterable = values if isinstance(values, (list, tuple, set)) else [values]
        for value in iterable:
            clean = str(value or "").strip()
            if not clean:
                continue
            key = clean.lower()
            if key in seen:
                continue
            seen.add(key)
            rows.append(clean)
            if len(rows) >= limit:
                break
        return rows
