from __future__ import annotations

import copy
import hashlib
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List

from backend.python.database.local_store import LocalStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ModelSetupRecoveryWatchdogMemory:
    def __init__(
        self,
        *,
        state_path: str = "data/model_setup_watchdog_runs.json",
        keep_runs: int = 96,
    ) -> None:
        self._store = LocalStore(state_path)
        self._keep_runs = self._coerce_int(keep_runs, minimum=8, maximum=512, default=96)
        self._lock = threading.RLock()
        self._runs: Dict[str, Dict[str, Any]] = {}
        self._load()

    def record(
        self,
        *,
        watchdog_payload: Dict[str, Any],
        source: str = "manual",
    ) -> Dict[str, Any]:
        payload = dict(watchdog_payload) if isinstance(watchdog_payload, dict) else {}
        now = _utc_now_iso()
        scope = self._scope_from_payload(payload)
        workspace_root = str(scope.get("workspace_root", "") or "").strip()
        manifest_path = str(scope.get("manifest_path", "") or "").strip()
        run_id = self._run_id(
            workspace_root=workspace_root,
            manifest_path=manifest_path,
            status=str(payload.get("status", "") or "").strip().lower(),
            created_at=now,
        )
        latest_triggered = (
            copy.deepcopy(payload.get("latest_triggered_payload", {}))
            if isinstance(payload.get("latest_triggered_payload", {}), dict)
            else {}
        )
        latest_triggered_scope = self._scope_from_payload(latest_triggered)
        with self._lock:
            self._runs[run_id] = {
                "run_id": run_id,
                "workspace_root": workspace_root,
                "manifest_path": manifest_path,
                "scope_label": self._scope_label(scope),
                "status": str(payload.get("status", "") or "").strip().lower(),
                "message": str(payload.get("message", "") or "").strip(),
                "source": str(source or "manual").strip().lower() or "manual",
                "dry_run": bool(payload.get("dry_run", False)),
                "current_scope": bool(payload.get("current_scope", False)),
                "continue_on_error": bool(payload.get("continue_on_error", True)),
                "continue_followup_actions_requested": bool(payload.get("continue_followup_actions_requested", False)),
                "max_missions": self._coerce_int(payload.get("max_missions", 0), minimum=0, maximum=512, default=0),
                "max_auto_resumes": self._coerce_int(payload.get("max_auto_resumes", 0), minimum=0, maximum=512, default=0),
                "max_followup_waves": self._coerce_int(payload.get("max_followup_waves", 0), minimum=0, maximum=64, default=0),
                "evaluated_count": self._coerce_int(payload.get("evaluated_count", 0), minimum=0, maximum=100_000, default=0),
                "auto_resume_attempted_count": self._coerce_int(payload.get("auto_resume_attempted_count", 0), minimum=0, maximum=100_000, default=0),
                "auto_resume_triggered_count": self._coerce_int(payload.get("auto_resume_triggered_count", 0), minimum=0, maximum=100_000, default=0),
                "ready_count": self._coerce_int(payload.get("ready_count", 0), minimum=0, maximum=100_000, default=0),
                "watch_count": self._coerce_int(payload.get("watch_count", 0), minimum=0, maximum=100_000, default=0),
                "stalled_count": self._coerce_int(payload.get("stalled_count", 0), minimum=0, maximum=100_000, default=0),
                "blocked_count": self._coerce_int(payload.get("blocked_count", 0), minimum=0, maximum=100_000, default=0),
                "idle_count": self._coerce_int(payload.get("idle_count", 0), minimum=0, maximum=100_000, default=0),
                "complete_count": self._coerce_int(payload.get("complete_count", 0), minimum=0, maximum=100_000, default=0),
                "error_count": self._coerce_int(payload.get("error_count", 0), minimum=0, maximum=100_000, default=0),
                "stop_reason": str(payload.get("stop_reason", "") or "").strip().lower(),
                "triggered_mission_ids": self._string_list(payload.get("triggered_mission_ids", []), limit=64),
                "ready_mission_ids": self._string_list(payload.get("ready_mission_ids", []), limit=64),
                "watched_mission_ids": self._string_list(payload.get("watched_mission_ids", []), limit=64),
                "stalled_mission_ids": self._string_list(payload.get("stalled_mission_ids", []), limit=64),
                "blocked_mission_ids": self._string_list(payload.get("blocked_mission_ids", []), limit=64),
                "scope_counts": (
                    copy.deepcopy(payload.get("scope_counts", {}))
                    if isinstance(payload.get("scope_counts", {}), dict)
                    else {}
                ),
                "latest_triggered_scope": latest_triggered_scope,
                "latest_triggered_scope_label": self._scope_label(latest_triggered_scope),
                "latest_triggered_status": str(latest_triggered.get("status", "") or "").strip().lower(),
                "latest_triggered_message": str(latest_triggered.get("message", "") or "").strip(),
                "latest_triggered_mission_id": (
                    self._string_list(payload.get("triggered_mission_ids", []), limit=1)[0]
                    if self._string_list(payload.get("triggered_mission_ids", []), limit=1)
                    else ""
                ),
                "created_at": now,
                "updated_at": now,
            }
            self._trim_locked()
            self._persist_locked()
            return {"status": "success", "run": self._public_row(self._runs[run_id])}

    def snapshot(
        self,
        *,
        limit: int = 20,
        status: str = "",
        workspace_root: str = "",
        manifest_path: str = "",
    ) -> Dict[str, Any]:
        bounded = self._coerce_int(limit, minimum=1, maximum=500, default=20)
        clean_status = self._normalize_text(status)
        clean_root = self._normalize_text(workspace_root)
        clean_manifest = self._normalize_text(manifest_path)
        with self._lock:
            rows = [dict(row) for row in self._runs.values()]
        if clean_status:
            rows = [row for row in rows if self._normalize_text(row.get("status", "")) == clean_status]
        if clean_root or clean_manifest:
            rows = [row for row in rows if self._scope_matches(row, workspace_root=clean_root, manifest_path=clean_manifest)]
        rows.sort(key=lambda row: str(row.get("updated_at", "") or ""), reverse=True)
        public_rows = [self._public_row(row) for row in rows[:bounded]]
        status_counts: Dict[str, int] = {}
        triggered_run_count = 0
        watch_run_count = 0
        stalled_run_count = 0
        error_run_count = 0
        for row in rows:
            status_key = self._normalize_text(row.get("status", ""))
            if status_key:
                status_counts[status_key] = int(status_counts.get(status_key, 0)) + 1
            if self._coerce_int(row.get("auto_resume_triggered_count", 0), minimum=0, maximum=100_000, default=0) > 0:
                triggered_run_count += 1
            if self._coerce_int(row.get("watch_count", 0), minimum=0, maximum=100_000, default=0) > 0:
                watch_run_count += 1
            if self._coerce_int(row.get("stalled_count", 0), minimum=0, maximum=100_000, default=0) > 0:
                stalled_run_count += 1
            if self._coerce_int(row.get("error_count", 0), minimum=0, maximum=100_000, default=0) > 0:
                error_run_count += 1
        return {
            "status": "success",
            "count": len(public_rows),
            "total": len(rows),
            "items": public_rows,
            "status_counts": status_counts,
            "triggered_run_count": triggered_run_count,
            "watch_run_count": watch_run_count,
            "stalled_run_count": stalled_run_count,
            "error_run_count": error_run_count,
            "latest_run": public_rows[0] if public_rows else None,
            "latest_triggered_run": next(
                (item for item in public_rows if self._coerce_int(item.get("auto_resume_triggered_count", 0), minimum=0, maximum=100_000, default=0) > 0),
                None,
            ),
            "latest_watch_run": next(
                (item for item in public_rows if self._coerce_int(item.get("watch_count", 0), minimum=0, maximum=100_000, default=0) > 0),
                None,
            ),
            "latest_stalled_run": next(
                (item for item in public_rows if self._coerce_int(item.get("stalled_count", 0), minimum=0, maximum=100_000, default=0) > 0),
                None,
            ),
            "latest_error_run": next(
                (item for item in public_rows if self._coerce_int(item.get("error_count", 0), minimum=0, maximum=100_000, default=0) > 0),
                None,
            ),
            "filters": {
                "status": clean_status,
                "workspace_root": clean_root,
                "manifest_path": clean_manifest,
            },
        }

    def reset(
        self,
        *,
        run_id: str = "",
        status: str = "",
        workspace_root: str = "",
        manifest_path: str = "",
    ) -> Dict[str, Any]:
        clean_id = str(run_id or "").strip()
        clean_status = self._normalize_text(status)
        clean_root = self._normalize_text(workspace_root)
        clean_manifest = self._normalize_text(manifest_path)
        removed = 0
        with self._lock:
            if clean_id:
                if clean_id in self._runs:
                    del self._runs[clean_id]
                    removed = 1
            else:
                keep: Dict[str, Dict[str, Any]] = {}
                for row_id, row in self._runs.items():
                    should_remove = False
                    if clean_status and self._normalize_text(row.get("status", "")) == clean_status:
                        should_remove = True
                    if (clean_root or clean_manifest) and self._scope_matches(
                        row,
                        workspace_root=clean_root,
                        manifest_path=clean_manifest,
                    ):
                        should_remove = True
                    if not any([clean_status, clean_root, clean_manifest]):
                        should_remove = True
                    if should_remove:
                        removed += 1
                        continue
                    keep[row_id] = row
                self._runs = keep
            if removed > 0:
                self._persist_locked()
        return {
            "status": "success",
            "removed": removed,
            "filters": {
                "run_id": clean_id,
                "status": clean_status,
                "workspace_root": clean_root,
                "manifest_path": clean_manifest,
            },
        }

    def _load(self) -> None:
        payload = self._store.get("runs", {})
        rows = payload if isinstance(payload, dict) else {}
        self._runs = {
            str(run_id): dict(row)
            for run_id, row in rows.items()
            if str(run_id).strip() and isinstance(row, dict)
        }

    def _persist_locked(self) -> None:
        self._store.set("runs", self._runs)

    def _trim_locked(self) -> None:
        if len(self._runs) <= self._keep_runs:
            return
        ordered = sorted(
            self._runs.items(),
            key=lambda item: str(item[1].get("updated_at", "") or ""),
            reverse=True,
        )
        self._runs = dict(ordered[: self._keep_runs])

    def _public_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        payload = row if isinstance(row, dict) else {}
        return {
            "run_id": str(payload.get("run_id", "") or "").strip(),
            "workspace_root": str(payload.get("workspace_root", "") or "").strip(),
            "manifest_path": str(payload.get("manifest_path", "") or "").strip(),
            "scope_label": str(payload.get("scope_label", "") or "").strip(),
            "status": str(payload.get("status", "") or "").strip(),
            "message": str(payload.get("message", "") or "").strip(),
            "source": str(payload.get("source", "") or "").strip(),
            "dry_run": bool(payload.get("dry_run", False)),
            "current_scope": bool(payload.get("current_scope", False)),
            "continue_on_error": bool(payload.get("continue_on_error", True)),
            "continue_followup_actions_requested": bool(payload.get("continue_followup_actions_requested", False)),
            "max_missions": self._coerce_int(payload.get("max_missions", 0), minimum=0, maximum=512, default=0),
            "max_auto_resumes": self._coerce_int(payload.get("max_auto_resumes", 0), minimum=0, maximum=512, default=0),
            "max_followup_waves": self._coerce_int(payload.get("max_followup_waves", 0), minimum=0, maximum=64, default=0),
            "evaluated_count": self._coerce_int(payload.get("evaluated_count", 0), minimum=0, maximum=100_000, default=0),
            "auto_resume_attempted_count": self._coerce_int(payload.get("auto_resume_attempted_count", 0), minimum=0, maximum=100_000, default=0),
            "auto_resume_triggered_count": self._coerce_int(payload.get("auto_resume_triggered_count", 0), minimum=0, maximum=100_000, default=0),
            "ready_count": self._coerce_int(payload.get("ready_count", 0), minimum=0, maximum=100_000, default=0),
            "watch_count": self._coerce_int(payload.get("watch_count", 0), minimum=0, maximum=100_000, default=0),
            "stalled_count": self._coerce_int(payload.get("stalled_count", 0), minimum=0, maximum=100_000, default=0),
            "blocked_count": self._coerce_int(payload.get("blocked_count", 0), minimum=0, maximum=100_000, default=0),
            "idle_count": self._coerce_int(payload.get("idle_count", 0), minimum=0, maximum=100_000, default=0),
            "complete_count": self._coerce_int(payload.get("complete_count", 0), minimum=0, maximum=100_000, default=0),
            "error_count": self._coerce_int(payload.get("error_count", 0), minimum=0, maximum=100_000, default=0),
            "stop_reason": str(payload.get("stop_reason", "") or "").strip(),
            "triggered_mission_ids": self._string_list(payload.get("triggered_mission_ids", []), limit=64),
            "ready_mission_ids": self._string_list(payload.get("ready_mission_ids", []), limit=64),
            "watched_mission_ids": self._string_list(payload.get("watched_mission_ids", []), limit=64),
            "stalled_mission_ids": self._string_list(payload.get("stalled_mission_ids", []), limit=64),
            "blocked_mission_ids": self._string_list(payload.get("blocked_mission_ids", []), limit=64),
            "scope_counts": copy.deepcopy(payload.get("scope_counts", {})) if isinstance(payload.get("scope_counts", {}), dict) else {},
            "latest_triggered_scope": copy.deepcopy(payload.get("latest_triggered_scope", {})) if isinstance(payload.get("latest_triggered_scope", {}), dict) else {},
            "latest_triggered_scope_label": str(payload.get("latest_triggered_scope_label", "") or "").strip(),
            "latest_triggered_status": str(payload.get("latest_triggered_status", "") or "").strip(),
            "latest_triggered_message": str(payload.get("latest_triggered_message", "") or "").strip(),
            "latest_triggered_mission_id": str(payload.get("latest_triggered_mission_id", "") or "").strip(),
            "created_at": str(payload.get("created_at", "") or "").strip(),
            "updated_at": str(payload.get("updated_at", "") or "").strip(),
        }

    def _scope_from_payload(self, payload: Dict[str, Any]) -> Dict[str, str]:
        data = payload if isinstance(payload, dict) else {}
        filters = data.get("filters", {}) if isinstance(data.get("filters", {}), dict) else {}
        history_after = data.get("history_after", {}) if isinstance(data.get("history_after", {}), dict) else {}
        history_before = data.get("history_before", {}) if isinstance(data.get("history_before", {}), dict) else {}
        latest_triggered = data.get("latest_triggered_payload", {}) if isinstance(data.get("latest_triggered_payload", {}), dict) else {}
        latest_workspace = latest_triggered.get("workspace", {}) if isinstance(latest_triggered.get("workspace", {}), dict) else {}
        for candidate in (
            filters,
            history_after.get("filters", {}) if isinstance(history_after.get("filters", {}), dict) else {},
            history_before.get("filters", {}) if isinstance(history_before.get("filters", {}), dict) else {},
            latest_workspace,
            data,
        ):
            if not isinstance(candidate, dict):
                continue
            workspace_root = str(candidate.get("workspace_root", "") or "").strip()
            manifest_path = str(candidate.get("manifest_path", "") or "").strip()
            if workspace_root or manifest_path:
                return {
                    "workspace_root": workspace_root,
                    "manifest_path": manifest_path,
                }
        return {"workspace_root": "", "manifest_path": ""}

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

    @staticmethod
    def _scope_label(scope: Dict[str, Any]) -> str:
        payload = scope if isinstance(scope, dict) else {}
        manifest_path = str(payload.get("manifest_path", "") or "").strip().replace("\\", "/")
        workspace_root = str(payload.get("workspace_root", "") or "").strip().replace("\\", "/")
        manifest_name = manifest_path.rsplit("/", 1)[-1] if manifest_path else ""
        workspace_name = workspace_root.rsplit("/", 1)[-1] if workspace_root else ""
        if workspace_name and manifest_name:
            return f"{workspace_name}::{manifest_name}"
        return manifest_name or workspace_name or "default"

    @staticmethod
    def _run_id(*, workspace_root: str, manifest_path: str, status: str, created_at: str) -> str:
        digest = hashlib.sha1(
            "|".join([workspace_root.strip().lower(), manifest_path.strip().lower(), status.strip().lower(), created_at.strip()]).encode("utf-8")
        ).hexdigest()
        return f"mswd_{digest[:12]}"

    @staticmethod
    def _normalize_text(value: Any) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            result = int(value)
        except Exception:  # noqa: BLE001
            return default
        return max(minimum, min(maximum, result))

    @staticmethod
    def _string_list(values: Any, *, limit: int) -> List[str]:
        if not isinstance(values, list):
            return []
        clean: List[str] = []
        seen: set[str] = set()
        for item in values:
            text = str(item or "").strip()
            if not text:
                continue
            lowered = text.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            clean.append(text)
            if len(clean) >= limit:
                break
        return clean
