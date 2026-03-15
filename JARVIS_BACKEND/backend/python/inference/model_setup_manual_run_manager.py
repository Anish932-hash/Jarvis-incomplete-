from __future__ import annotations

import copy
import threading
import uuid
from typing import Any, Callable, Dict, List, Optional

from backend.python.database.local_store import LocalStore
from backend.python.inference.model_setup_manual_runner import ModelSetupManualRunner, _iso_now, select_manual_pipeline_items


class ModelSetupManualRunManager:
    _ACTIVE_STATUSES = {"queued", "running", "cancelling"}

    @staticmethod
    def _scope_key(*, manifest_path: str = "", workspace_root: str = "") -> str:
        clean_manifest = str(manifest_path or "").strip().lower()
        clean_root = str(workspace_root or "").strip().lower()
        return f"{clean_root}::{clean_manifest}".strip(":")

    def __init__(
        self,
        runner: Optional[ModelSetupManualRunner] = None,
        *,
        state_path: str = "data/model_setup_manual_runs.json",
        keep_runs: int = 60,
        completion_callback: Optional[Callable[..., Dict[str, Any]]] = None,
    ) -> None:
        self._runner = runner or ModelSetupManualRunner()
        self._store = LocalStore(state_path)
        self._keep_runs = max(10, min(int(keep_runs), 200))
        self._completion_callback = completion_callback
        self._lock = threading.RLock()
        self._runs: Dict[str, Dict[str, Any]] = {}
        self._load()

    def start(
        self,
        *,
        pipeline_payload: Dict[str, Any],
        item_keys: Optional[List[str]] = None,
        dry_run: bool = False,
        force: bool = False,
        task: str = "",
        step_ids: Optional[List[str]] = None,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        selected = select_manual_pipeline_items(pipeline_payload, item_keys=item_keys)
        if not selected and not item_keys:
            return {"status": "error", "message": "no runnable manual pipeline items selected"}
        setup_plan = pipeline_payload.get("setup_plan", {}) if isinstance(pipeline_payload.get("setup_plan", {}), dict) else {}
        manifest = setup_plan.get("manifest", {}) if isinstance(setup_plan.get("manifest", {}), dict) else (
            pipeline_payload.get("manifest", {}) if isinstance(pipeline_payload.get("manifest", {}), dict) else {}
        )
        clean_manifest_path = str(manifest_path or manifest.get("path", "") or "").strip()
        clean_workspace_root = str(workspace_root or manifest.get("workspace_root", "") or "").strip()
        run_id = uuid.uuid4().hex
        cancel_event = threading.Event()
        now_iso = _iso_now()
        row: Dict[str, Any] = {
            "run_id": run_id,
            "task": str(task or "").strip().lower(),
            "status": "queued",
            "dry_run": bool(dry_run),
            "force": bool(force),
            "selected_count": len(selected) if selected else len(item_keys or []),
            "selected_item_keys": [str(item).strip().lower() for item in (item_keys or []) if str(item).strip()],
            "selected_step_ids": [str(item).strip().lower() for item in (step_ids or []) if str(item).strip()],
            "manifest_path": clean_manifest_path,
            "workspace_root": clean_workspace_root,
            "scope_key": self._scope_key(
                manifest_path=clean_manifest_path,
                workspace_root=clean_workspace_root,
            ),
            "planned_count": 0,
            "success_count": 0,
            "warning_count": 0,
            "error_count": 0,
            "blocked_count": 0,
            "cancelled_count": 0,
            "step_success_count": 0,
            "step_error_count": 0,
            "step_skipped_count": 0,
            "created_at": now_iso,
            "updated_at": now_iso,
            "started_at": "",
            "completed_at": "",
            "duration_s": 0.0,
            "message": "queued",
            "progress_event_count": 0,
            "last_event_name": "queued",
            "last_progress_at": now_iso,
            "progress": {
                "total_items": len(selected) if selected else len(item_keys or []),
                "completed_items": 0,
                "current_item_key": "",
                "current_item_name": "",
                "current_step_id": "",
                "percent": 0.0,
                "message": "queued",
                "phase": "queued",
            },
            "items": [],
            "result": {},
            "activation": {},
            "cancel_requested_at": "",
            "cancel_reason": "",
            "_cancel_event": cancel_event,
            "_thread": None,
        }
        worker = threading.Thread(
            target=self._worker,
            kwargs={
                "run_id": run_id,
                "pipeline_payload": copy.deepcopy(pipeline_payload),
                "item_keys": list(item_keys or []),
                "dry_run": bool(dry_run),
                "force": bool(force),
                "task": str(task or "").strip().lower(),
                "step_ids": list(step_ids or []),
            },
            name=f"jarvis-manual-pipeline-{run_id[:8]}",
            daemon=True,
        )
        row["_thread"] = worker
        with self._lock:
            self._runs[run_id] = row
            self._persist_locked()
        worker.start()
        return {"status": "accepted", "run": self._sanitize(row)}

    def list_runs(
        self,
        *,
        limit: int = 20,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 200))
        clean_manifest_path = str(manifest_path or "").strip().lower()
        clean_workspace_root = str(workspace_root or "").strip().lower()
        with self._lock:
            rows = sorted(self._runs.values(), key=lambda row: str(row.get("updated_at", "")), reverse=True)
            if clean_manifest_path or clean_workspace_root:
                rows = [
                    row
                    for row in rows
                    if (
                        (not clean_manifest_path or str(row.get("manifest_path", "") or "").strip().lower() == clean_manifest_path)
                        and (not clean_workspace_root or str(row.get("workspace_root", "") or "").strip().lower() == clean_workspace_root)
                    )
                ]
            payload = [self._sanitize(row) for row in rows[:bounded]]
            active_count = sum(1 for row in rows if str(row.get("status", "") or "").strip().lower() in self._ACTIVE_STATUSES)
        return {
            "status": "success",
            "items": payload,
            "count": len(payload),
            "total": len(rows),
            "active_count": active_count,
            "filters": {
                "manifest_path": str(manifest_path or "").strip(),
                "workspace_root": str(workspace_root or "").strip(),
            },
        }

    def get_run(self, run_id: str) -> Dict[str, Any]:
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return {"status": "error", "message": "run_id is required"}
        with self._lock:
            row = self._runs.get(clean_run_id)
            if not isinstance(row, dict):
                return {"status": "error", "message": "run not found", "run_id": clean_run_id}
            return {"status": "success", "run": self._sanitize(row)}

    def cancel(self, run_id: str, *, reason: str = "cancelled_by_user") -> Dict[str, Any]:
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return {"status": "error", "message": "run_id is required"}
        with self._lock:
            row = self._runs.get(clean_run_id)
            if not isinstance(row, dict):
                return {"status": "error", "message": "run not found", "run_id": clean_run_id}
            cancel_event = row.get("_cancel_event")
            if isinstance(cancel_event, threading.Event):
                cancel_event.set()
            row["cancel_requested_at"] = _iso_now()
            row["cancel_reason"] = str(reason or "cancelled_by_user").strip() or "cancelled_by_user"
            if str(row.get("status", "") or "").strip().lower() in {"queued", "running"}:
                row["status"] = "cancelling"
                row["message"] = row["cancel_reason"]
            self._runs[clean_run_id] = row
            self._persist_locked()
        return {"status": "success", "run": self._sanitize(row)}

    def _worker(
        self,
        *,
        run_id: str,
        pipeline_payload: Dict[str, Any],
        item_keys: List[str],
        dry_run: bool,
        force: bool,
        task: str,
        step_ids: List[str],
    ) -> None:
        with self._lock:
            row = self._runs.get(run_id)
            if not isinstance(row, dict):
                return
            row["status"] = "running"
            row["started_at"] = _iso_now()
            row["updated_at"] = row["started_at"]
            row["message"] = "starting manual pipeline"
            row["last_event_name"] = "run_started"
            row["last_progress_at"] = row["started_at"]
            progress = row.get("progress") if isinstance(row.get("progress"), dict) else {}
            progress["message"] = "starting manual pipeline"
            progress["phase"] = "starting"
            row["progress"] = progress
            cancel_event = row.get("_cancel_event") if isinstance(row.get("_cancel_event"), threading.Event) else threading.Event()
            self._runs[run_id] = row
            self._persist_locked()
        try:
            result = self._runner.run(
                pipeline_payload=pipeline_payload,
                item_keys=item_keys or None,
                dry_run=bool(dry_run),
                force=bool(force),
                run_id=run_id,
                cancel_event=cancel_event,
                progress_callback=lambda event: self._handle_progress(run_id, event),
                step_ids=step_ids or None,
            )
            activation_payload: Dict[str, Any] = {}
            if callable(self._completion_callback):
                try:
                    activation_payload = self._completion_callback(
                        source="manual_pipeline",
                        task=task,
                        run_payload=copy.deepcopy(result),
                    )
                except Exception as exc:  # noqa: BLE001
                    activation_payload = {"status": "error", "message": str(exc), "source": "manual_pipeline"}
            if activation_payload:
                result["activation"] = copy.deepcopy(activation_payload)
            with self._lock:
                row = self._runs.get(run_id)
                if not isinstance(row, dict):
                    return
                row["status"] = str(result.get("status", "error") or "error")
                row["task"] = task
                row["manifest_path"] = str(result.get("manifest_path", row.get("manifest_path", "")) or row.get("manifest_path", ""))
                row["workspace_root"] = str(result.get("workspace_root", row.get("workspace_root", "")) or row.get("workspace_root", ""))
                row["scope_key"] = self._scope_key(
                    manifest_path=str(row.get("manifest_path", "") or ""),
                    workspace_root=str(row.get("workspace_root", "") or ""),
                )
                row["completed_at"] = str(result.get("completed_at", "") or _iso_now())
                row["updated_at"] = row["completed_at"]
                row["message"] = str(result.get("message", row.get("status", "completed")) or row.get("status", "completed"))
                row["result"] = copy.deepcopy(result)
                row["items"] = copy.deepcopy(result.get("items", [])) if isinstance(result.get("items", []), list) else []
                row["planned_count"] = int(result.get("planned_count", 0) or 0)
                row["success_count"] = int(result.get("success_count", 0) or 0)
                row["warning_count"] = int(result.get("warning_count", 0) or 0)
                row["error_count"] = int(result.get("error_count", 0) or 0)
                row["blocked_count"] = int(result.get("blocked_count", 0) or 0)
                row["cancelled_count"] = int(result.get("cancelled_count", 0) or 0)
                row["step_success_count"] = int(result.get("step_success_count", 0) or 0)
                row["step_error_count"] = int(result.get("step_error_count", 0) or 0)
                row["step_skipped_count"] = int(result.get("step_skipped_count", 0) or 0)
                row["duration_s"] = float(result.get("duration_s", 0.0) or 0.0)
                row["activation"] = copy.deepcopy(result.get("activation", {})) if isinstance(result.get("activation", {}), dict) else {}
                self._runs[run_id] = row
                self._persist_locked()
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                row = self._runs.get(run_id)
                if not isinstance(row, dict):
                    return
                row["status"] = "error"
                row["completed_at"] = _iso_now()
                row["updated_at"] = row["completed_at"]
                row["message"] = str(exc)
                self._runs[run_id] = row
                self._persist_locked()

    def _handle_progress(self, run_id: str, event: Dict[str, Any]) -> None:
        event_name = str(event.get("event", "") or "").strip().lower()
        with self._lock:
            row = self._runs.get(run_id)
            if not isinstance(row, dict):
                return
            progress = row.get("progress") if isinstance(row.get("progress"), dict) else {}
            if event_name == "run_started":
                progress["total_items"] = max(
                    0,
                    int(event.get("selected_count", progress.get("total_items", row.get("selected_count", 0))) or 0),
                )
                progress["message"] = "starting manual pipeline"
                progress["phase"] = "starting"
            elif event_name == "item_started":
                item = event.get("item") if isinstance(event.get("item"), dict) else {}
                progress["total_items"] = max(
                    int(progress.get("total_items", row.get("selected_count", 0)) or 0),
                    int(event.get("total_items", progress.get("total_items", row.get("selected_count", 0))) or 0),
                )
                progress["current_item_key"] = str(item.get("key", "") or "")
                progress["current_item_name"] = str(item.get("name", "") or "")
                progress["completed_items"] = max(0, int(event.get("index", 1) or 1) - 1)
                progress["message"] = f"running {progress['current_item_name'] or progress['current_item_key'] or 'item'}"
                progress["phase"] = "item_running"
            elif event_name == "step_started":
                progress["current_step_id"] = str(event.get("step_id", "") or "")
                progress["message"] = str(event.get("title", progress.get("message", "running")) or progress.get("message", "running"))
                progress["phase"] = "step_running"
            elif event_name == "item_completed":
                item = event.get("item") if isinstance(event.get("item"), dict) else {}
                progress["total_items"] = max(
                    int(progress.get("total_items", row.get("selected_count", 0)) or 0),
                    int(event.get("total_items", progress.get("total_items", row.get("selected_count", 0))) or 0),
                )
                progress["completed_items"] = max(0, int(event.get("index", 0) or 0))
                progress["current_item_key"] = str(item.get("key", progress.get("current_item_key", "")) or "")
                progress["current_item_name"] = str(item.get("name", progress.get("current_item_name", "")) or "")
                progress["current_step_id"] = ""
                progress["message"] = str(item.get("message", item.get("status", "completed")) or "completed")
                progress["phase"] = "item_completed"
                row["items"] = [dict(entry) for entry in row.get("items", []) if isinstance(entry, dict)]
                row["items"].append(copy.deepcopy(item))
            elif event_name == "run_completed":
                payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
                progress["completed_items"] = max(progress.get("completed_items", 0), len(payload.get("items", [])) if isinstance(payload.get("items", []), list) else 0)
                progress["current_step_id"] = ""
                progress["message"] = str(payload.get("status", progress.get("message", "completed")) or progress.get("message", "completed"))
                progress["phase"] = "completed"
            total_items = max(0, int(progress.get("total_items", row.get("selected_count", 0)) or row.get("selected_count", 0) or 0))
            completed_items = max(0, int(progress.get("completed_items", 0) or 0))
            progress["percent"] = round((float(completed_items) / float(total_items)) * 100.0, 3) if total_items > 0 else 0.0
            row["progress"] = progress
            event_iso = _iso_now()
            row["progress_event_count"] = int(row.get("progress_event_count", 0) or 0) + 1
            row["last_event_name"] = event_name or str(row.get("last_event_name", "") or "")
            row["last_progress_at"] = event_iso
            row["updated_at"] = event_iso
            self._runs[run_id] = row
            self._persist_locked()

    def _load(self) -> None:
        rows = self._store.get("runs", [])
        if not isinstance(rows, list):
            return
        with self._lock:
            self._runs = {
                str(row.get("run_id", "") or "").strip(): dict(row)
                for row in rows
                if isinstance(row, dict) and str(row.get("run_id", "") or "").strip()
            }

    def _persist_locked(self) -> None:
        persisted_rows = sorted(
            (self._sanitize(row) for row in self._runs.values()),
            key=lambda row: str(row.get("updated_at", "")),
            reverse=True,
        )[: self._keep_runs]
        keep_ids = {
            str(row.get("run_id", "") or "").strip()
            for row in persisted_rows
            if str(row.get("run_id", "") or "").strip()
        }
        self._runs = {run_id: row for run_id, row in self._runs.items() if run_id in keep_ids}
        self._store.set("runs", persisted_rows)

    @staticmethod
    def _sanitize(row: Dict[str, Any]) -> Dict[str, Any]:
        return {key: copy.deepcopy(value) for key, value in row.items() if not str(key).startswith("_")}
