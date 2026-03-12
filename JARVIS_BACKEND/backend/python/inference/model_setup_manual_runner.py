from __future__ import annotations

import os
import shutil
import subprocess
import threading
import time
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from backend.python.core.provider_credentials import ProviderCredentialManager
from backend.python.database.local_store import LocalStore


ProgressCallback = Callable[[Dict[str, Any]], None]


class ModelSetupManualPipelineCancelled(RuntimeError):
    pass


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _path_size(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        if path.is_file():
            return int(path.stat().st_size)
        return int(sum(item.stat().st_size for item in path.rglob("*") if item.is_file()))
    except Exception:
        return 0


def select_manual_pipeline_items(pipeline_payload: Dict[str, Any], item_keys: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    items = [
        dict(item)
        for item in pipeline_payload.get("items", [])
        if isinstance(item, dict)
    ]
    selected_keys = {
        str(item).strip().lower()
        for item in (item_keys or [])
        if str(item).strip()
    }
    if selected_keys:
        return [
            item
            for item in items
            if str(item.get("key", "") or "").strip().lower() in selected_keys
        ]
    return [item for item in items if _item_default_runnable(item)]


def _item_default_runnable(item: Dict[str, Any]) -> bool:
    status = str(item.get("status", "") or "").strip().lower()
    if status not in {"ready", "warning"}:
        return False
    if str(item.get("pipeline_kind", "") or "").strip().lower() == "unresolved_source":
        return False
    steps = item.get("steps", []) if isinstance(item.get("steps"), list) else []
    return any(
        isinstance(step, dict)
        and isinstance(step.get("commands"), list)
        and any(str(command).strip() for command in step.get("commands", []))
        for step in steps
    )


class ModelSetupManualRunner:
    def __init__(
        self,
        *,
        history_path: str = "data/model_setup_manual_history.json",
        log_root: str = "data/model_setup_manual_logs",
        provider_credentials: Optional[ProviderCredentialManager] = None,
    ) -> None:
        self._history_path = self._resolve_path(history_path)
        self._store = LocalStore(self._history_path)
        self._log_root = Path(self._resolve_path(log_root))
        self._provider_credentials = provider_credentials

    def history(self, *, limit: int = 20) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 200))
        runs = self._runs()
        return {
            "status": "success",
            "count": min(len(runs), bounded),
            "total": len(runs),
            "items": runs[:bounded],
            "history_path": self._history_path,
            "log_root": str(self._log_root),
        }

    def run(
        self,
        *,
        pipeline_payload: Dict[str, Any],
        item_keys: Optional[List[str]] = None,
        dry_run: bool = False,
        force: bool = False,
        run_id: str = "",
        progress_callback: Optional[ProgressCallback] = None,
        cancel_event: Optional[threading.Event] = None,
        step_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        items = select_manual_pipeline_items(pipeline_payload, item_keys=item_keys)
        requested_keys = {
            str(item).strip().lower()
            for item in (item_keys or [])
            if str(item).strip()
        }
        selected_step_ids = {
            str(item).strip().lower()
            for item in (step_ids or [])
            if str(item).strip()
        }
        run_started = time.time()
        clean_run_id = str(run_id or "").strip() or uuid.uuid4().hex
        payload: Dict[str, Any] = {
            "status": "success",
            "run_id": clean_run_id,
            "dry_run": bool(dry_run),
            "force": bool(force),
            "selected_count": len(items),
            "success_count": 0,
            "planned_count": 0,
            "warning_count": 0,
            "error_count": 0,
            "blocked_count": 0,
            "cancelled_count": 0,
            "step_success_count": 0,
            "step_error_count": 0,
            "step_skipped_count": 0,
            "requested_item_keys": sorted(requested_keys),
            "requested_step_ids": sorted(selected_step_ids),
            "started_at": _iso_now(),
            "completed_at": "",
            "duration_s": 0.0,
            "items": [],
            "history_path": self._history_path,
            "log_root": str(self._log_root),
            "pipeline_root": str(pipeline_payload.get("pipeline_root", "") or ""),
        }
        self._emit(progress_callback, {"event": "run_started", "run_id": clean_run_id, "selected_count": len(items)})
        if not items:
            payload["status"] = "error"
            payload["message"] = "no manual pipeline items selected"
            payload["completed_at"] = _iso_now()
            payload["duration_s"] = round(time.time() - run_started, 6)
            self._append_run(payload)
            return payload

        for index, item in enumerate(items, start=1):
            if isinstance(cancel_event, threading.Event) and cancel_event.is_set():
                result = {
                    "key": str(item.get("key", "") or ""),
                    "name": str(item.get("name", "") or ""),
                    "task": str(item.get("task", "") or ""),
                    "status": "cancelled",
                    "message": "cancelled",
                    "started_at": _iso_now(),
                    "completed_at": _iso_now(),
                    "duration_s": 0.0,
                    "steps": [],
                }
            else:
                result = self._run_item(
                    item=item,
                    index=index,
                    total_items=len(items),
                    dry_run=bool(dry_run),
                    force=bool(force),
                    selected_step_ids=selected_step_ids,
                    progress_callback=progress_callback,
                    cancel_event=cancel_event,
                    run_id=clean_run_id,
                )
            payload["items"].append(result)
            status_name = str(result.get("status", "error") or "error").strip().lower()
            if status_name == "success":
                payload["success_count"] = int(payload.get("success_count", 0) or 0) + 1
            elif status_name == "planned":
                payload["planned_count"] = int(payload.get("planned_count", 0) or 0) + 1
            elif status_name == "warning":
                payload["warning_count"] = int(payload.get("warning_count", 0) or 0) + 1
            elif status_name == "blocked":
                payload["blocked_count"] = int(payload.get("blocked_count", 0) or 0) + 1
            elif status_name == "cancelled":
                payload["cancelled_count"] = int(payload.get("cancelled_count", 0) or 0) + 1
            else:
                payload["error_count"] = int(payload.get("error_count", 0) or 0) + 1
            payload["step_success_count"] = int(payload.get("step_success_count", 0) or 0) + int(result.get("step_success_count", 0) or 0)
            payload["step_error_count"] = int(payload.get("step_error_count", 0) or 0) + int(result.get("step_error_count", 0) or 0)
            payload["step_skipped_count"] = int(payload.get("step_skipped_count", 0) or 0) + int(result.get("step_skipped_count", 0) or 0)
            self._emit(progress_callback, {"event": "item_completed", "run_id": clean_run_id, "index": index, "total_items": len(items), "item": result})
            if status_name == "cancelled":
                break

        if bool(dry_run):
            if int(payload.get("error_count", 0) or 0) > 0:
                payload["status"] = "partial" if int(payload.get("planned_count", 0) or 0) > 0 else "error"
            elif int(payload.get("cancelled_count", 0) or 0) > 0:
                payload["status"] = "partial" if int(payload.get("planned_count", 0) or 0) > 0 else "cancelled"
            elif int(payload.get("blocked_count", 0) or 0) > 0:
                payload["status"] = "partial"
            else:
                payload["status"] = "planned"
        elif int(payload.get("error_count", 0) or 0) > 0:
            payload["status"] = "partial" if int(payload.get("success_count", 0) or 0) > 0 else "error"
        elif int(payload.get("cancelled_count", 0) or 0) > 0:
            payload["status"] = "partial" if int(payload.get("success_count", 0) or 0) > 0 else "cancelled"
        elif int(payload.get("warning_count", 0) or 0) > 0 or int(payload.get("blocked_count", 0) or 0) > 0:
            payload["status"] = "partial"
        payload["completed_at"] = _iso_now()
        payload["duration_s"] = round(time.time() - run_started, 6)
        self._emit(progress_callback, {"event": "run_completed", "run_id": clean_run_id, "payload": dict(payload)})
        self._append_run(payload)
        return payload

    def _run_item(
        self,
        *,
        item: Dict[str, Any],
        index: int,
        total_items: int,
        dry_run: bool,
        force: bool,
        selected_step_ids: set[str],
        progress_callback: Optional[ProgressCallback],
        cancel_event: Optional[threading.Event],
        run_id: str,
    ) -> Dict[str, Any]:
        started = time.time()
        item_key = str(item.get("key", "") or "").strip().lower()
        item_status = str(item.get("status", "unknown") or "unknown").strip().lower()
        pipeline_root = Path(str(item.get("pipeline_root", "") or self._log_root.parent)).expanduser()
        target_path = Path(str(item.get("path", "") or "")).expanduser()
        steps = [
            dict(step)
            for step in (item.get("steps", []) if isinstance(item.get("steps"), list) else [])
            if isinstance(step, dict)
        ]
        selected_steps = [
            step for step in steps if not selected_step_ids or str(step.get("id", "") or "").strip().lower() in selected_step_ids
        ]
        result: Dict[str, Any] = {
            "key": item_key,
            "name": str(item.get("name", "") or target_path.name or item_key),
            "task": str(item.get("task", "") or "").strip().lower(),
            "path": str(target_path),
            "status": "",
            "message": "",
            "started_at": _iso_now(),
            "completed_at": "",
            "duration_s": 0.0,
            "steps": [],
            "step_success_count": 0,
            "step_error_count": 0,
            "step_skipped_count": 0,
            "log_root": str(self._log_root / run_id / item_key),
        }
        if item_status == "blocked" and not force:
            result["status"] = "blocked"
            result["message"] = str(item.get("recommended_next_action", "") or "manual pipeline is blocked")
            result["completed_at"] = _iso_now()
            result["duration_s"] = round(time.time() - started, 6)
            return result
        if not selected_steps:
            result["status"] = "blocked"
            result["message"] = "no runnable steps selected"
            result["completed_at"] = _iso_now()
            result["duration_s"] = round(time.time() - started, 6)
            return result

        self._emit(progress_callback, {"event": "item_started", "run_id": run_id, "index": index, "total_items": total_items, "item": {"key": item_key, "name": result["name"], "task": result["task"]}})

        for step_index, step in enumerate(selected_steps, start=1):
            if isinstance(cancel_event, threading.Event) and cancel_event.is_set():
                result["status"] = "cancelled"
                result["message"] = "cancelled"
                break
            step_result = self._run_step(
                item=item,
                step=step,
                step_index=step_index,
                total_steps=len(selected_steps),
                dry_run=dry_run,
                force=force,
                pipeline_root=pipeline_root,
                cancel_event=cancel_event,
                progress_callback=progress_callback,
                run_id=run_id,
                item_key=item_key,
            )
            result["steps"].append(step_result)
            step_status = str(step_result.get("status", "error") or "error").strip().lower()
            if step_status in {"success", "planned"}:
                result["step_success_count"] = int(result.get("step_success_count", 0) or 0) + 1
            elif step_status == "skipped":
                result["step_skipped_count"] = int(result.get("step_skipped_count", 0) or 0) + 1
            elif step_status == "blocked":
                result["step_skipped_count"] = int(result.get("step_skipped_count", 0) or 0) + 1
            elif step_status == "cancelled":
                result["status"] = "cancelled"
                result["message"] = "cancelled"
                break
            else:
                result["step_error_count"] = int(result.get("step_error_count", 0) or 0) + 1
                result["status"] = "error"
                result["message"] = str(step_result.get("message", "step failed") or "step failed")
                break

        if not str(result.get("status", "") or "").strip():
            if bool(dry_run):
                result["status"] = "planned"
                result["message"] = "manual pipeline dry-run completed"
            else:
                verification = self._verify_target_path(target_path)
                result["artifact"] = verification
                if bool(verification.get("exists", False)):
                    result["status"] = "warning" if item_status == "warning" else "success"
                    result["message"] = "manual pipeline completed"
                else:
                    result["status"] = "error"
                    result["message"] = "final artifact was not created at the manifest path"
        result["completed_at"] = _iso_now()
        result["duration_s"] = round(time.time() - started, 6)
        return result

    def _run_step(
        self,
        *,
        item: Dict[str, Any],
        step: Dict[str, Any],
        step_index: int,
        total_steps: int,
        dry_run: bool,
        force: bool,
        pipeline_root: Path,
        cancel_event: Optional[threading.Event],
        progress_callback: Optional[ProgressCallback],
        run_id: str,
        item_key: str,
    ) -> Dict[str, Any]:
        started = time.time()
        step_id = str(step.get("id", "") or f"step-{step_index}").strip().lower()
        step_title = str(step.get("title", "") or step_id).strip()
        step_status = str(step.get("status", "ready") or "ready").strip().lower()
        commands = [
            str(command).strip()
            for command in (step.get("commands", []) if isinstance(step.get("commands"), list) else [])
            if str(command).strip()
        ]
        result: Dict[str, Any] = {
            "id": step_id,
            "title": step_title,
            "status": "error",
            "message": "",
            "started_at": _iso_now(),
            "completed_at": "",
            "duration_s": 0.0,
            "command_count": len(commands),
            "commands": commands,
            "logs": [],
        }
        if step_status == "blocked" and not force:
            result["status"] = "blocked"
            result["message"] = "; ".join(
                str(item).strip()
                for item in (step.get("blockers", []) if isinstance(step.get("blockers"), list) else [])
                if str(item).strip()
            ) or f"{step_title} is blocked"
            result["completed_at"] = _iso_now()
            result["duration_s"] = round(time.time() - started, 6)
            return result
        if not commands:
            result["status"] = "skipped"
            result["message"] = "no commands generated for this step"
            result["completed_at"] = _iso_now()
            result["duration_s"] = round(time.time() - started, 6)
            return result

        self._emit(progress_callback, {"event": "step_started", "run_id": run_id, "item_key": item_key, "step_id": step_id, "step_index": step_index, "total_steps": total_steps, "title": step_title})
        if dry_run:
            result["status"] = "planned"
            result["message"] = "dry-run"
            result["completed_at"] = _iso_now()
            result["duration_s"] = round(time.time() - started, 6)
            self._emit(progress_callback, {"event": "step_completed", "run_id": run_id, "item_key": item_key, "step": result})
            return result

        env = self._command_env()
        for command_index, command in enumerate(commands, start=1):
            log_payload = self._run_command(
                run_id=run_id,
                item_key=item_key,
                step_id=step_id,
                command_index=command_index,
                command=command,
                cwd=pipeline_root,
                env=env,
                cancel_event=cancel_event,
            )
            result["logs"].append(log_payload)
            if str(log_payload.get("status", "error") or "error").strip().lower() != "success":
                result["status"] = str(log_payload.get("status", "error") or "error").strip().lower()
                result["message"] = str(log_payload.get("message", "command failed") or "command failed")
                result["completed_at"] = _iso_now()
                result["duration_s"] = round(time.time() - started, 6)
                self._emit(progress_callback, {"event": "step_completed", "run_id": run_id, "item_key": item_key, "step": result})
                return result

        result["status"] = "success"
        result["message"] = "completed"
        result["completed_at"] = _iso_now()
        result["duration_s"] = round(time.time() - started, 6)
        self._emit(progress_callback, {"event": "step_completed", "run_id": run_id, "item_key": item_key, "step": result})
        return result

    def _run_command(
        self,
        *,
        run_id: str,
        item_key: str,
        step_id: str,
        command_index: int,
        command: str,
        cwd: Path,
        env: Dict[str, str],
        cancel_event: Optional[threading.Event],
    ) -> Dict[str, Any]:
        started = time.time()
        log_dir = self._log_root / run_id / item_key
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{step_id}-{command_index:02d}.log"
        shell_path = shutil.which("powershell") or shutil.which("pwsh") or "powershell"
        command_args = [shell_path, "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command]
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        with log_path.open("w", encoding="utf-8", errors="replace") as log_handle:
            proc = subprocess.Popen(
                command_args,
                cwd=str(cwd),
                env=env,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                creationflags=creationflags,
            )
            while proc.poll() is None:
                if isinstance(cancel_event, threading.Event) and cancel_event.is_set():
                    proc.terminate()
                    try:
                        proc.wait(timeout=5.0)
                    except Exception:
                        proc.kill()
                    return {
                        "status": "cancelled",
                        "message": "cancelled",
                        "command": command,
                        "log_path": str(log_path),
                        "duration_s": round(time.time() - started, 6),
                        "output_tail": self._read_tail(log_path),
                    }
                time.sleep(0.2)
        returncode = int(proc.returncode or 0)
        output_tail = self._read_tail(log_path)
        return {
            "status": "success" if returncode == 0 else "error",
            "message": "completed" if returncode == 0 else f"command failed with exit code {returncode}",
            "returncode": returncode,
            "command": command,
            "log_path": str(log_path),
            "duration_s": round(time.time() - started, 6),
            "output_tail": output_tail,
        }

    def _command_env(self) -> Dict[str, str]:
        env = dict(os.environ)
        env.setdefault("PYTHONIOENCODING", "utf-8")
        manager = self._provider_credentials
        if manager is not None:
            hf_token = manager.get_api_key("huggingface")
            if hf_token:
                env.setdefault("HUGGINGFACE_HUB_TOKEN", hf_token)
                env.setdefault("HF_TOKEN", hf_token)
        return env

    @staticmethod
    def _verify_target_path(path: Path) -> Dict[str, Any]:
        exists = path.exists()
        payload = {
            "path": str(path),
            "exists": exists,
            "kind": "directory" if exists and path.is_dir() else "file",
            "size_bytes": _path_size(path) if exists else 0,
        }
        if exists and path.is_dir():
            payload["file_count"] = sum(1 for item in path.rglob("*") if item.is_file())
        return payload

    @staticmethod
    def _read_tail(path: Path, *, max_chars: int = 8000) -> str:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return ""
        if len(text) <= max_chars:
            return text
        return text[-max_chars:]

    @staticmethod
    def _emit(callback: Optional[ProgressCallback], event: Dict[str, Any]) -> None:
        if not callable(callback):
            return
        try:
            callback(dict(event))
        except Exception:
            pass

    def _runs(self) -> List[Dict[str, Any]]:
        rows = self._store.get("runs", [])
        return [dict(item) for item in rows if isinstance(item, dict)] if isinstance(rows, list) else []

    def _append_run(self, payload: Dict[str, Any]) -> None:
        rows = self._runs()
        rows.insert(0, deepcopy(payload))
        self._store.set("runs", rows[:80])

    @staticmethod
    def _resolve_path(path_value: str) -> str:
        candidate = Path(str(path_value or "").strip() or ".")
        if candidate.is_absolute():
            return str(candidate)
        return str((Path(__file__).resolve().parents[4] / candidate).resolve())
