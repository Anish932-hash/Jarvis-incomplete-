from __future__ import annotations

import copy
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from backend.python.database.local_store import LocalStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso_from_ts(value: float) -> str:
    try:
        numeric = float(value)
    except Exception:  # noqa: BLE001
        return ""
    if numeric <= 0:
        return ""
    return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat()


class ModelSetupRecoveryWatchdogSupervisor:
    def __init__(
        self,
        *,
        state_path: str = "data/model_setup_watchdog_supervisor.json",
        enabled: bool = False,
        interval_s: float = 45.0,
        max_missions: int = 6,
        max_auto_resumes: int = 2,
        continue_followup_actions: bool = True,
        max_followup_waves: int = 3,
        current_scope: bool = False,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> None:
        self._store = LocalStore(state_path)
        self._lock = threading.RLock()
        self._wakeup = threading.Event()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._execute_callback: Optional[Callable[..., Dict[str, Any]]] = None
        self._config = self._default_config(
            enabled=enabled,
            interval_s=interval_s,
            max_missions=max_missions,
            max_auto_resumes=max_auto_resumes,
            continue_followup_actions=continue_followup_actions,
            max_followup_waves=max_followup_waves,
            current_scope=current_scope,
            manifest_path=manifest_path,
            workspace_root=workspace_root,
        )
        self._runtime = self._default_runtime()
        self._load()

    def start(self, execute_callback: Callable[..., Dict[str, Any]]) -> None:
        with self._lock:
            self._execute_callback = execute_callback
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._wakeup.clear()
            self._thread = threading.Thread(
                target=self._worker,
                name="model-setup-watchdog-supervisor",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            thread = self._thread
        if thread is None:
            return
        self._stop_event.set()
        self._wakeup.set()
        thread.join(timeout=5)
        with self._lock:
            self._thread = None

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return self._public_status_locked()

    def configure(
        self,
        *,
        enabled: Optional[bool] = None,
        interval_s: Optional[float] = None,
        max_missions: Optional[int] = None,
        max_auto_resumes: Optional[int] = None,
        continue_followup_actions: Optional[bool] = None,
        max_followup_waves: Optional[int] = None,
        current_scope: Optional[bool] = None,
        manifest_path: Optional[str] = None,
        workspace_root: Optional[str] = None,
        source: str = "manual",
    ) -> Dict[str, Any]:
        with self._lock:
            if enabled is not None:
                self._config["enabled"] = bool(enabled)
            if interval_s is not None:
                self._config["interval_s"] = self._coerce_float(interval_s, minimum=5.0, maximum=3600.0, default=45.0)
            if max_missions is not None:
                self._config["max_missions"] = self._coerce_int(max_missions, minimum=1, maximum=64, default=6)
            if max_auto_resumes is not None:
                self._config["max_auto_resumes"] = self._coerce_int(
                    max_auto_resumes,
                    minimum=0,
                    maximum=max(1, int(self._config.get("max_missions", 6) or 6)),
                    default=2,
                )
            if continue_followup_actions is not None:
                self._config["continue_followup_actions"] = bool(continue_followup_actions)
            if max_followup_waves is not None:
                self._config["max_followup_waves"] = self._coerce_int(max_followup_waves, minimum=0, maximum=8, default=3)
            if current_scope is not None:
                self._config["current_scope"] = bool(current_scope)
            if manifest_path is not None:
                self._config["manifest_path"] = str(manifest_path or "").strip()
            if workspace_root is not None:
                self._config["workspace_root"] = str(workspace_root or "").strip()
            self._runtime["last_config_source"] = str(source or "manual").strip().lower() or "manual"
            self._runtime["updated_at"] = _utc_now_iso()
            self._persist_locked()
            status = self._public_status_locked()
        self._wakeup.set()
        return status

    def trigger_now(
        self,
        *,
        source: str = "manual",
        dry_run: Optional[bool] = None,
        current_scope: Optional[bool] = None,
        manifest_path: Optional[str] = None,
        workspace_root: Optional[str] = None,
        max_missions: Optional[int] = None,
        max_auto_resumes: Optional[int] = None,
        continue_followup_actions: Optional[bool] = None,
        max_followup_waves: Optional[int] = None,
    ) -> Dict[str, Any]:
        overrides = {
            "dry_run": dry_run,
            "current_scope": current_scope,
            "manifest_path": manifest_path,
            "workspace_root": workspace_root,
            "max_missions": max_missions,
            "max_auto_resumes": max_auto_resumes,
            "continue_followup_actions": continue_followup_actions,
            "max_followup_waves": max_followup_waves,
        }
        return self._execute_once(source=str(source or "manual").strip().lower() or "manual", overrides=overrides)

    def _worker(self) -> None:
        while not self._stop_event.is_set():
            wait_s = self._compute_wait_s()
            if self._wakeup.wait(timeout=wait_s):
                self._wakeup.clear()
                continue
            if self._stop_event.is_set():
                break
            with self._lock:
                if not bool(self._config.get("enabled", False)):
                    continue
            self._execute_once(source="daemon")

    def _compute_wait_s(self) -> float:
        with self._lock:
            if not bool(self._config.get("enabled", False)):
                return 1.0
            interval_s = self._coerce_float(self._config.get("interval_s", 45.0), minimum=5.0, maximum=3600.0, default=45.0)
            last_tick_ts = float(self._runtime.get("last_tick_ts", 0.0) or 0.0)
            now = time.time()
            if last_tick_ts <= 0:
                return 0.0
            return max(0.0, (last_tick_ts + interval_s) - now)

    def _execute_once(self, *, source: str, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        with self._lock:
            if bool(self._runtime.get("inflight", False)):
                return {
                    "status": "busy",
                    "message": "watchdog supervisor tick already in progress",
                    "supervisor": self._public_status_locked(),
                }
            callback = self._execute_callback
            if callback is None:
                return {
                    "status": "unavailable",
                    "message": "watchdog supervisor callback unavailable",
                    "supervisor": self._public_status_locked(),
                }
            self._runtime["inflight"] = True
            self._runtime["last_trigger_source"] = str(source or "manual").strip().lower() or "manual"
            self._runtime["last_trigger_at"] = _utc_now_iso()
            self._persist_locked()
            config = dict(self._config)
        effective = self._apply_overrides(config, overrides or {})
        start_ts = time.time()
        try:
            result = callback(
                dry_run=bool(effective.get("dry_run", False)),
                continue_on_error=True,
                current_scope=bool(effective.get("current_scope", False)),
                max_missions=self._coerce_int(effective.get("max_missions", 6), minimum=1, maximum=64, default=6),
                max_auto_resumes=self._coerce_int(effective.get("max_auto_resumes", 2), minimum=0, maximum=64, default=2),
                continue_followup_actions=bool(effective.get("continue_followup_actions", True)),
                max_followup_waves=self._coerce_int(effective.get("max_followup_waves", 3), minimum=0, maximum=8, default=3),
                manifest_path=str(effective.get("manifest_path", "") or "").strip(),
                workspace_root=str(effective.get("workspace_root", "") or "").strip(),
            )
        except Exception as exc:  # noqa: BLE001
            result = {"status": "error", "message": str(exc)}
        duration_ms = max(0.0, (time.time() - start_ts) * 1000.0)
        result_payload = dict(result) if isinstance(result, dict) else {"status": "error", "message": "invalid supervisor result"}
        with self._lock:
            self._runtime["inflight"] = False
            self._runtime["last_tick_ts"] = time.time()
            self._runtime["last_tick_at"] = _iso_from_ts(self._runtime["last_tick_ts"])
            self._runtime["last_duration_ms"] = round(duration_ms, 2)
            self._runtime["last_result_status"] = str(result_payload.get("status", "") or "").strip().lower()
            self._runtime["last_result_message"] = str(result_payload.get("message", "") or "").strip()
            self._runtime["last_summary"] = self._result_summary(result_payload)
            self._runtime["run_count"] = self._coerce_int(self._runtime.get("run_count", 0), minimum=0, maximum=1_000_000, default=0) + 1
            if source == "daemon":
                self._runtime["auto_trigger_count"] = self._coerce_int(
                    self._runtime.get("auto_trigger_count", 0), minimum=0, maximum=1_000_000, default=0
                ) + 1
            else:
                self._runtime["manual_trigger_count"] = self._coerce_int(
                    self._runtime.get("manual_trigger_count", 0), minimum=0, maximum=1_000_000, default=0
                ) + 1
            if str(result_payload.get("status", "") or "").strip().lower() == "error":
                self._runtime["last_error_at"] = self._runtime["last_tick_at"]
                self._runtime["consecutive_error_count"] = self._coerce_int(
                    self._runtime.get("consecutive_error_count", 0), minimum=0, maximum=1_000_000, default=0
                ) + 1
            else:
                self._runtime["last_success_at"] = self._runtime["last_tick_at"]
                self._runtime["consecutive_error_count"] = 0
            self._runtime["updated_at"] = _utc_now_iso()
            self._persist_locked()
            status = self._public_status_locked()
        return {
            "status": str(result_payload.get("status", "") or "").strip().lower() or "success",
            "message": str(result_payload.get("message", "") or "").strip(),
            "result": copy.deepcopy(result_payload),
            "supervisor": status,
        }

    def _public_status_locked(self) -> Dict[str, Any]:
        interval_s = self._coerce_float(self._config.get("interval_s", 45.0), minimum=5.0, maximum=3600.0, default=45.0)
        last_tick_ts = float(self._runtime.get("last_tick_ts", 0.0) or 0.0)
        next_due_at = _iso_from_ts(last_tick_ts + interval_s) if bool(self._config.get("enabled", False)) and last_tick_ts > 0 else ""
        return {
            "status": "success",
            "active": bool(self._thread and self._thread.is_alive()),
            "enabled": bool(self._config.get("enabled", False)),
            "inflight": bool(self._runtime.get("inflight", False)),
            "interval_s": interval_s,
            "current_scope": bool(self._config.get("current_scope", False)),
            "manifest_path": str(self._config.get("manifest_path", "") or "").strip(),
            "workspace_root": str(self._config.get("workspace_root", "") or "").strip(),
            "max_missions": self._coerce_int(self._config.get("max_missions", 6), minimum=1, maximum=64, default=6),
            "max_auto_resumes": self._coerce_int(self._config.get("max_auto_resumes", 2), minimum=0, maximum=64, default=2),
            "continue_followup_actions": bool(self._config.get("continue_followup_actions", True)),
            "max_followup_waves": self._coerce_int(self._config.get("max_followup_waves", 3), minimum=0, maximum=8, default=3),
            "last_tick_at": str(self._runtime.get("last_tick_at", "") or "").strip(),
            "last_success_at": str(self._runtime.get("last_success_at", "") or "").strip(),
            "last_error_at": str(self._runtime.get("last_error_at", "") or "").strip(),
            "last_duration_ms": float(self._runtime.get("last_duration_ms", 0.0) or 0.0),
            "last_result_status": str(self._runtime.get("last_result_status", "") or "").strip(),
            "last_result_message": str(self._runtime.get("last_result_message", "") or "").strip(),
            "last_trigger_source": str(self._runtime.get("last_trigger_source", "") or "").strip(),
            "last_trigger_at": str(self._runtime.get("last_trigger_at", "") or "").strip(),
            "last_config_source": str(self._runtime.get("last_config_source", "") or "").strip(),
            "next_due_at": next_due_at,
            "run_count": self._coerce_int(self._runtime.get("run_count", 0), minimum=0, maximum=1_000_000, default=0),
            "manual_trigger_count": self._coerce_int(self._runtime.get("manual_trigger_count", 0), minimum=0, maximum=1_000_000, default=0),
            "auto_trigger_count": self._coerce_int(self._runtime.get("auto_trigger_count", 0), minimum=0, maximum=1_000_000, default=0),
            "consecutive_error_count": self._coerce_int(self._runtime.get("consecutive_error_count", 0), minimum=0, maximum=1_000_000, default=0),
            "last_summary": copy.deepcopy(self._runtime.get("last_summary", {}))
            if isinstance(self._runtime.get("last_summary", {}), dict)
            else {},
            "updated_at": str(self._runtime.get("updated_at", "") or "").strip(),
        }

    def _load(self) -> None:
        config = self._store.get("config", {})
        runtime = self._store.get("runtime", {})
        if isinstance(config, dict):
            self._config.update(self._apply_overrides(self._config, config))
        if isinstance(runtime, dict):
            self._runtime.update(runtime)

    def _persist_locked(self) -> None:
        self._store.set("config", self._config)
        self._store.set("runtime", self._runtime)

    @staticmethod
    def _result_summary(result_payload: Dict[str, Any]) -> Dict[str, Any]:
        payload = result_payload if isinstance(result_payload, dict) else {}
        return {
            "status": str(payload.get("status", "") or "").strip().lower(),
            "message": str(payload.get("message", "") or "").strip(),
            "auto_resume_triggered_count": ModelSetupRecoveryWatchdogSupervisor._coerce_int(
                payload.get("auto_resume_triggered_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "watch_count": ModelSetupRecoveryWatchdogSupervisor._coerce_int(
                payload.get("watch_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "stalled_count": ModelSetupRecoveryWatchdogSupervisor._coerce_int(
                payload.get("stalled_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "blocked_count": ModelSetupRecoveryWatchdogSupervisor._coerce_int(
                payload.get("blocked_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "idle_count": ModelSetupRecoveryWatchdogSupervisor._coerce_int(
                payload.get("idle_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "complete_count": ModelSetupRecoveryWatchdogSupervisor._coerce_int(
                payload.get("complete_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "error_count": ModelSetupRecoveryWatchdogSupervisor._coerce_int(
                payload.get("error_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "stop_reason": str(payload.get("stop_reason", "") or "").strip().lower(),
        }

    @staticmethod
    def _default_config(
        *,
        enabled: bool,
        interval_s: float,
        max_missions: int,
        max_auto_resumes: int,
        continue_followup_actions: bool,
        max_followup_waves: int,
        current_scope: bool,
        manifest_path: str,
        workspace_root: str,
    ) -> Dict[str, Any]:
        return {
            "enabled": bool(enabled),
            "interval_s": ModelSetupRecoveryWatchdogSupervisor._coerce_float(interval_s, minimum=5.0, maximum=3600.0, default=45.0),
            "max_missions": ModelSetupRecoveryWatchdogSupervisor._coerce_int(max_missions, minimum=1, maximum=64, default=6),
            "max_auto_resumes": ModelSetupRecoveryWatchdogSupervisor._coerce_int(max_auto_resumes, minimum=0, maximum=64, default=2),
            "continue_followup_actions": bool(continue_followup_actions),
            "max_followup_waves": ModelSetupRecoveryWatchdogSupervisor._coerce_int(max_followup_waves, minimum=0, maximum=8, default=3),
            "current_scope": bool(current_scope),
            "manifest_path": str(manifest_path or "").strip(),
            "workspace_root": str(workspace_root or "").strip(),
        }

    @staticmethod
    def _default_runtime() -> Dict[str, Any]:
        return {
            "inflight": False,
            "last_tick_ts": 0.0,
            "last_tick_at": "",
            "last_success_at": "",
            "last_error_at": "",
            "last_duration_ms": 0.0,
            "last_result_status": "",
            "last_result_message": "",
            "last_trigger_source": "",
            "last_trigger_at": "",
            "last_config_source": "",
            "run_count": 0,
            "manual_trigger_count": 0,
            "auto_trigger_count": 0,
            "consecutive_error_count": 0,
            "last_summary": {},
            "updated_at": "",
        }

    @staticmethod
    def _apply_overrides(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(base)
        if not isinstance(overrides, dict):
            return payload
        for key in (
            "enabled",
            "continue_followup_actions",
            "current_scope",
            "manifest_path",
            "workspace_root",
            "interval_s",
            "max_missions",
            "max_auto_resumes",
            "max_followup_waves",
            "dry_run",
        ):
            if key in overrides and overrides[key] is not None:
                payload[key] = overrides[key]
        return payload

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            result = int(value)
        except Exception:  # noqa: BLE001
            return default
        return max(minimum, min(maximum, result))

    @staticmethod
    def _coerce_float(value: Any, *, minimum: float, maximum: float, default: float) -> float:
        try:
            result = float(value)
        except Exception:  # noqa: BLE001
            return default
        return max(minimum, min(maximum, result))
