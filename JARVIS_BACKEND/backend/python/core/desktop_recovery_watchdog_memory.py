from __future__ import annotations

import copy
import hashlib
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List

from backend.python.database.local_store import LocalStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DesktopRecoveryWatchdogMemory:
    def __init__(
        self,
        *,
        state_path: str = "data/desktop_recovery_watchdog_runs.json",
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
        filters = dict(payload.get("filters", {})) if isinstance(payload.get("filters", {}), dict) else {}
        trigger_source = str(payload.get("trigger_source", source) or source).strip().lower() or "manual"
        status_name = str(payload.get("status", "") or "").strip().lower()
        created_at = _utc_now_iso()
        app_name = str(filters.get("app_name", payload.get("app_name", "")) or "").strip()
        mission_kind = str(filters.get("mission_kind", payload.get("mission_kind", "")) or "").strip()
        stop_reason_code = str(
            filters.get("stop_reason_code", payload.get("stop_reason_code", payload.get("stop_reason", ""))) or ""
        ).strip().lower()
        run_id = self._run_id(
            status=status_name,
            app_name=app_name,
            mission_kind=mission_kind,
            created_at=created_at,
        )
        with self._lock:
            row = {
                "run_id": run_id,
                "status": status_name,
                "message": str(payload.get("message", "") or "").strip(),
                "source": str(source or "manual").strip().lower() or "manual",
                "trigger_source": trigger_source,
                "limit": self._coerce_int(payload.get("limit", 0), minimum=0, maximum=2000, default=0),
                "max_auto_resumes": self._coerce_int(
                    payload.get("max_auto_resumes", 0),
                    minimum=0,
                    maximum=256,
                    default=0,
                ),
                "mission_status": str(filters.get("status", payload.get("mission_status", "")) or "").strip().lower(),
                "mission_kind": mission_kind,
                "app_name": app_name,
                "stop_reason_code": stop_reason_code,
                "resume_force": bool(payload.get("resume_force", False)),
                "evaluated_count": self._coerce_int(payload.get("evaluated_count", 0), minimum=0, maximum=100_000, default=0),
                "auto_resume_attempted_count": self._coerce_int(
                    payload.get("auto_resume_attempted_count", 0),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "auto_resume_triggered_count": self._coerce_int(
                    payload.get("auto_resume_triggered_count", 0),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "resume_ready_count": self._coerce_int(payload.get("resume_ready_count", 0), minimum=0, maximum=100_000, default=0),
                "manual_attention_count": self._coerce_int(
                    payload.get("manual_attention_count", 0),
                    minimum=0,
                    maximum=100_000,
                    default=0,
                ),
                "blocked_count": self._coerce_int(payload.get("blocked_count", 0), minimum=0, maximum=100_000, default=0),
                "idle_count": self._coerce_int(payload.get("idle_count", 0), minimum=0, maximum=100_000, default=0),
                "error_count": self._coerce_int(payload.get("error_count", 0), minimum=0, maximum=100_000, default=0),
                "stop_reason": str(payload.get("stop_reason", "") or "").strip().lower(),
                "triggered_mission_ids": self._string_list(payload.get("triggered_mission_ids", []), limit=64),
                "ready_mission_ids": self._string_list(payload.get("ready_mission_ids", []), limit=64),
                "blocked_mission_ids": self._string_list(payload.get("blocked_mission_ids", []), limit=64),
                "latest_triggered_mission_id": self._string_list(payload.get("triggered_mission_ids", []), limit=1)[0]
                if self._string_list(payload.get("triggered_mission_ids", []), limit=1)
                else "",
                "filters": {
                    "status": str(filters.get("status", "") or "").strip().lower(),
                    "mission_kind": mission_kind,
                    "app_name": app_name,
                    "stop_reason_code": stop_reason_code,
                },
                "created_at": created_at,
                "updated_at": created_at,
            }
            self._runs[run_id] = row
            self._trim_locked()
            self._persist_locked()
            return {"status": "success", "run": self._public_row(row)}

    def snapshot(
        self,
        *,
        limit: int = 20,
        status: str = "",
        source: str = "",
        app_name: str = "",
        mission_kind: str = "",
    ) -> Dict[str, Any]:
        bounded = self._coerce_int(limit, minimum=1, maximum=500, default=20)
        clean_status = self._normalize_text(status)
        clean_source = self._normalize_text(source)
        clean_app = self._normalize_text(app_name)
        clean_kind = self._normalize_text(mission_kind)
        with self._lock:
            rows = [dict(row) for row in self._runs.values()]
        if clean_status:
            rows = [row for row in rows if self._normalize_text(row.get("status", "")) == clean_status]
        if clean_source:
            rows = [row for row in rows if self._normalize_text(row.get("source", "")) == clean_source]
        if clean_app:
            rows = [row for row in rows if self._normalize_text(row.get("app_name", "")) == clean_app]
        if clean_kind:
            rows = [row for row in rows if self._normalize_text(row.get("mission_kind", "")) == clean_kind]
        rows.sort(key=lambda row: str(row.get("updated_at", "") or ""), reverse=True)
        public_rows = [self._public_row(row) for row in rows[:bounded]]

        status_counts: Dict[str, int] = {}
        source_counts: Dict[str, int] = {}
        app_counts: Dict[str, int] = {}
        mission_kind_counts: Dict[str, int] = {}
        triggered_run_count = 0
        blocked_run_count = 0
        error_run_count = 0
        for row in rows:
            status_key = self._normalize_text(row.get("status", ""))
            if status_key:
                status_counts[status_key] = int(status_counts.get(status_key, 0)) + 1
            source_key = self._normalize_text(row.get("source", ""))
            if source_key:
                source_counts[source_key] = int(source_counts.get(source_key, 0)) + 1
            app_key = str(row.get("app_name", "") or "").strip()
            if app_key:
                app_counts[app_key] = int(app_counts.get(app_key, 0)) + 1
            mission_key = self._normalize_text(row.get("mission_kind", ""))
            if mission_key:
                mission_kind_counts[mission_key] = int(mission_kind_counts.get(mission_key, 0)) + 1
            if self._coerce_int(row.get("auto_resume_triggered_count", 0), minimum=0, maximum=100_000, default=0) > 0:
                triggered_run_count += 1
            if self._coerce_int(row.get("blocked_count", 0), minimum=0, maximum=100_000, default=0) > 0:
                blocked_run_count += 1
            if self._coerce_int(row.get("error_count", 0), minimum=0, maximum=100_000, default=0) > 0:
                error_run_count += 1
        return {
            "status": "success",
            "count": len(public_rows),
            "total": len(rows),
            "items": public_rows,
            "status_counts": status_counts,
            "source_counts": source_counts,
            "app_counts": app_counts,
            "mission_kind_counts": mission_kind_counts,
            "triggered_run_count": triggered_run_count,
            "blocked_run_count": blocked_run_count,
            "error_run_count": error_run_count,
            "latest_run": self._public_optional_row(public_rows[0] if public_rows else None),
            "latest_triggered_run": self._public_optional_row(
                next(
                    (
                        item
                        for item in public_rows
                        if self._coerce_int(item.get("auto_resume_triggered_count", 0), minimum=0, maximum=100_000, default=0) > 0
                    ),
                    None,
                )
            ),
            "latest_blocked_run": self._public_optional_row(
                next(
                    (
                        item
                        for item in public_rows
                        if self._coerce_int(item.get("blocked_count", 0), minimum=0, maximum=100_000, default=0) > 0
                    ),
                    None,
                )
            ),
            "latest_error_run": self._public_optional_row(
                next(
                    (
                        item
                        for item in public_rows
                        if self._coerce_int(item.get("error_count", 0), minimum=0, maximum=100_000, default=0) > 0
                    ),
                    None,
                )
            ),
            "filters": {
                "status": clean_status,
                "source": clean_source,
                "app_name": clean_app,
                "mission_kind": clean_kind,
            },
        }

    def reset(
        self,
        *,
        run_id: str = "",
        status: str = "",
        source: str = "",
        app_name: str = "",
        mission_kind: str = "",
    ) -> Dict[str, Any]:
        clean_id = str(run_id or "").strip()
        clean_status = self._normalize_text(status)
        clean_source = self._normalize_text(source)
        clean_app = self._normalize_text(app_name)
        clean_kind = self._normalize_text(mission_kind)
        removed = 0
        with self._lock:
            if clean_id:
                if clean_id in self._runs:
                    del self._runs[clean_id]
                    removed = 1
            else:
                keep: Dict[str, Dict[str, Any]] = {}
                for row_id, row in self._runs.items():
                    should_remove = True
                    if clean_status:
                        should_remove = should_remove and self._normalize_text(row.get("status", "")) == clean_status
                    if clean_source:
                        should_remove = should_remove and self._normalize_text(row.get("source", "")) == clean_source
                    if clean_app:
                        should_remove = should_remove and self._normalize_text(row.get("app_name", "")) == clean_app
                    if clean_kind:
                        should_remove = should_remove and self._normalize_text(row.get("mission_kind", "")) == clean_kind
                    if not any([clean_status, clean_source, clean_app, clean_kind]):
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
                "source": clean_source,
                "app_name": clean_app,
                "mission_kind": clean_kind,
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
            "status": str(payload.get("status", "") or "").strip(),
            "message": str(payload.get("message", "") or "").strip(),
            "source": str(payload.get("source", "") or "").strip(),
            "trigger_source": str(payload.get("trigger_source", "") or "").strip(),
            "limit": self._coerce_int(payload.get("limit", 0), minimum=0, maximum=2000, default=0),
            "max_auto_resumes": self._coerce_int(payload.get("max_auto_resumes", 0), minimum=0, maximum=256, default=0),
            "mission_status": str(payload.get("mission_status", "") or "").strip(),
            "mission_kind": str(payload.get("mission_kind", "") or "").strip(),
            "app_name": str(payload.get("app_name", "") or "").strip(),
            "stop_reason_code": str(payload.get("stop_reason_code", "") or "").strip(),
            "resume_force": bool(payload.get("resume_force", False)),
            "evaluated_count": self._coerce_int(payload.get("evaluated_count", 0), minimum=0, maximum=100_000, default=0),
            "auto_resume_attempted_count": self._coerce_int(
                payload.get("auto_resume_attempted_count", 0),
                minimum=0,
                maximum=100_000,
                default=0,
            ),
            "auto_resume_triggered_count": self._coerce_int(
                payload.get("auto_resume_triggered_count", 0),
                minimum=0,
                maximum=100_000,
                default=0,
            ),
            "resume_ready_count": self._coerce_int(payload.get("resume_ready_count", 0), minimum=0, maximum=100_000, default=0),
            "manual_attention_count": self._coerce_int(
                payload.get("manual_attention_count", 0),
                minimum=0,
                maximum=100_000,
                default=0,
            ),
            "blocked_count": self._coerce_int(payload.get("blocked_count", 0), minimum=0, maximum=100_000, default=0),
            "idle_count": self._coerce_int(payload.get("idle_count", 0), minimum=0, maximum=100_000, default=0),
            "error_count": self._coerce_int(payload.get("error_count", 0), minimum=0, maximum=100_000, default=0),
            "stop_reason": str(payload.get("stop_reason", "") or "").strip(),
            "triggered_mission_ids": self._string_list(payload.get("triggered_mission_ids", []), limit=64),
            "ready_mission_ids": self._string_list(payload.get("ready_mission_ids", []), limit=64),
            "blocked_mission_ids": self._string_list(payload.get("blocked_mission_ids", []), limit=64),
            "latest_triggered_mission_id": str(payload.get("latest_triggered_mission_id", "") or "").strip(),
            "filters": copy.deepcopy(payload.get("filters", {})) if isinstance(payload.get("filters", {}), dict) else {},
            "created_at": str(payload.get("created_at", "") or "").strip(),
            "updated_at": str(payload.get("updated_at", "") or "").strip(),
        }

    @staticmethod
    def _public_optional_row(row: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if not isinstance(row, dict):
            return None
        return row

    @staticmethod
    def _normalize_text(value: Any) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            numeric = int(value)
        except Exception:  # noqa: BLE001
            return default
        return max(minimum, min(maximum, numeric))

    @staticmethod
    def _string_list(values: Any, *, limit: int) -> List[str]:
        if not isinstance(values, list):
            return []
        items: List[str] = []
        for value in values:
            text = str(value or "").strip()
            if not text or text in items:
                continue
            items.append(text)
            if len(items) >= limit:
                break
        return items

    @staticmethod
    def _run_id(*, status: str, app_name: str, mission_kind: str, created_at: str) -> str:
        digest = hashlib.sha1(
            f"{status}|{app_name}|{mission_kind}|{created_at}".encode("utf-8"),
            usedforsecurity=False,
        ).hexdigest()[:12]
        return f"desktop_watchdog_{digest}"
