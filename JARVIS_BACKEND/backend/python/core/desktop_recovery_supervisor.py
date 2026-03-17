from __future__ import annotations

import copy
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from backend.python.core.desktop_governance_policy import desktop_governance_profile_defaults
from backend.python.core.desktop_governance_policy import normalize_desktop_governance_profile
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


class DesktopRecoverySupervisor:
    def __init__(
        self,
        *,
        state_path: str = "data/desktop_recovery_supervisor.json",
        enabled: bool = False,
        interval_s: float = 45.0,
        limit: int = 12,
        max_auto_resumes: int = 2,
        policy_profile: str = "balanced",
        allow_high_risk: bool | None = None,
        allow_critical_risk: bool | None = None,
        allow_admin_clearance: bool | None = None,
        allow_destructive: bool | None = None,
        mission_status: str = "paused",
        mission_kind: str = "",
        app_name: str = "",
        stop_reason_code: str = "",
        resume_force: bool = False,
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
            limit=limit,
            max_auto_resumes=max_auto_resumes,
            policy_profile=policy_profile,
            allow_high_risk=allow_high_risk,
            allow_critical_risk=allow_critical_risk,
            allow_admin_clearance=allow_admin_clearance,
            allow_destructive=allow_destructive,
            mission_status=mission_status,
            mission_kind=mission_kind,
            app_name=app_name,
            stop_reason_code=stop_reason_code,
            resume_force=resume_force,
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
                name="desktop-recovery-supervisor",
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
        limit: Optional[int] = None,
        max_auto_resumes: Optional[int] = None,
        policy_profile: Optional[str] = None,
        allow_high_risk: Optional[bool] = None,
        allow_critical_risk: Optional[bool] = None,
        allow_admin_clearance: Optional[bool] = None,
        allow_destructive: Optional[bool] = None,
        mission_status: Optional[str] = None,
        mission_kind: Optional[str] = None,
        app_name: Optional[str] = None,
        stop_reason_code: Optional[str] = None,
        resume_force: Optional[bool] = None,
        source: str = "manual",
    ) -> Dict[str, Any]:
        with self._lock:
            if enabled is not None:
                self._config["enabled"] = bool(enabled)
            if interval_s is not None:
                self._config["interval_s"] = self._coerce_float(interval_s, minimum=5.0, maximum=3600.0, default=45.0)
            if limit is not None:
                self._config["limit"] = self._coerce_int(limit, minimum=1, maximum=200, default=12)
            if max_auto_resumes is not None:
                self._config["max_auto_resumes"] = self._coerce_int(max_auto_resumes, minimum=0, maximum=32, default=2)
            if policy_profile is not None:
                self._config["policy_profile"] = self._normalize_policy_profile(policy_profile)
                self._apply_policy_profile_defaults_locked(force=True)
            if allow_high_risk is not None:
                self._config["allow_high_risk"] = bool(allow_high_risk)
                self._config["policy_profile"] = "custom"
            if allow_critical_risk is not None:
                self._config["allow_critical_risk"] = bool(allow_critical_risk)
                self._config["policy_profile"] = "custom"
            if allow_admin_clearance is not None:
                self._config["allow_admin_clearance"] = bool(allow_admin_clearance)
                self._config["policy_profile"] = "custom"
            if allow_destructive is not None:
                self._config["allow_destructive"] = bool(allow_destructive)
                self._config["policy_profile"] = "custom"
            if mission_status is not None:
                self._config["mission_status"] = str(mission_status or "").strip()
            if mission_kind is not None:
                self._config["mission_kind"] = str(mission_kind or "").strip()
            if app_name is not None:
                self._config["app_name"] = str(app_name or "").strip()
            if stop_reason_code is not None:
                self._config["stop_reason_code"] = str(stop_reason_code or "").strip()
            if resume_force is not None:
                self._config["resume_force"] = bool(resume_force)
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
        limit: Optional[int] = None,
        max_auto_resumes: Optional[int] = None,
        policy_profile: Optional[str] = None,
        allow_high_risk: Optional[bool] = None,
        allow_critical_risk: Optional[bool] = None,
        allow_admin_clearance: Optional[bool] = None,
        allow_destructive: Optional[bool] = None,
        mission_status: Optional[str] = None,
        mission_kind: Optional[str] = None,
        app_name: Optional[str] = None,
        stop_reason_code: Optional[str] = None,
        resume_force: Optional[bool] = None,
    ) -> Dict[str, Any]:
        overrides = {
            "limit": limit,
            "max_auto_resumes": max_auto_resumes,
            "policy_profile": policy_profile,
            "allow_high_risk": allow_high_risk,
            "allow_critical_risk": allow_critical_risk,
            "allow_admin_clearance": allow_admin_clearance,
            "allow_destructive": allow_destructive,
            "mission_status": mission_status,
            "mission_kind": mission_kind,
            "app_name": app_name,
            "stop_reason_code": stop_reason_code,
            "resume_force": resume_force,
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
                    "message": "desktop recovery daemon tick already in progress",
                    "supervisor": self._public_status_locked(),
                }
            callback = self._execute_callback
            if callback is None:
                return {
                    "status": "unavailable",
                    "message": "desktop recovery daemon callback unavailable",
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
                limit=self._coerce_int(effective.get("limit", 12), minimum=1, maximum=200, default=12),
                max_auto_resumes=self._coerce_int(effective.get("max_auto_resumes", 2), minimum=0, maximum=32, default=2),
                policy_profile=str(effective.get("policy_profile", "") or "").strip(),
                allow_high_risk=bool(effective.get("allow_high_risk", False)),
                allow_critical_risk=bool(effective.get("allow_critical_risk", False)),
                allow_admin_clearance=bool(effective.get("allow_admin_clearance", False)),
                allow_destructive=bool(effective.get("allow_destructive", False)),
                mission_status=str(effective.get("mission_status", "") or "").strip(),
                mission_kind=str(effective.get("mission_kind", "") or "").strip(),
                app_name=str(effective.get("app_name", "") or "").strip(),
                stop_reason_code=str(effective.get("stop_reason_code", "") or "").strip(),
                resume_force=bool(effective.get("resume_force", False)),
                trigger_source=str(source or "manual").strip().lower() or "manual",
            )
        except Exception as exc:  # noqa: BLE001
            result = {"status": "error", "message": str(exc)}
        duration_ms = max(0.0, (time.time() - start_ts) * 1000.0)
        result_payload = dict(result) if isinstance(result, dict) else {"status": "error", "message": "invalid desktop supervisor result"}
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
            "limit": self._coerce_int(self._config.get("limit", 12), minimum=1, maximum=200, default=12),
            "max_auto_resumes": self._coerce_int(self._config.get("max_auto_resumes", 2), minimum=0, maximum=32, default=2),
            "policy_profile": str(self._config.get("policy_profile", "balanced") or "balanced").strip(),
            "allow_high_risk": bool(self._config.get("allow_high_risk", False)),
            "allow_critical_risk": bool(self._config.get("allow_critical_risk", False)),
            "allow_admin_clearance": bool(self._config.get("allow_admin_clearance", False)),
            "allow_destructive": bool(self._config.get("allow_destructive", False)),
            "mission_status": str(self._config.get("mission_status", "") or "").strip(),
            "mission_kind": str(self._config.get("mission_kind", "") or "").strip(),
            "app_name": str(self._config.get("app_name", "") or "").strip(),
            "stop_reason_code": str(self._config.get("stop_reason_code", "") or "").strip(),
            "resume_force": bool(self._config.get("resume_force", False)),
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
            self._apply_policy_profile_defaults_locked(force=False)
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
            "evaluated_count": DesktopRecoverySupervisor._coerce_int(
                payload.get("evaluated_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "auto_resume_attempted_count": DesktopRecoverySupervisor._coerce_int(
                payload.get("auto_resume_attempted_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "auto_resume_triggered_count": DesktopRecoverySupervisor._coerce_int(
                payload.get("auto_resume_triggered_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "resume_ready_count": DesktopRecoverySupervisor._coerce_int(
                payload.get("resume_ready_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "manual_attention_count": DesktopRecoverySupervisor._coerce_int(
                payload.get("manual_attention_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "blocked_count": DesktopRecoverySupervisor._coerce_int(
                payload.get("blocked_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "policy_blocked_count": DesktopRecoverySupervisor._coerce_int(
                payload.get("policy_blocked_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "idle_count": DesktopRecoverySupervisor._coerce_int(
                payload.get("idle_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "error_count": DesktopRecoverySupervisor._coerce_int(
                payload.get("error_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "stop_reason": str(payload.get("stop_reason", "") or "").strip().lower(),
        }

    @staticmethod
    def _default_config(
        *,
        enabled: bool,
        interval_s: float,
        limit: int,
        max_auto_resumes: int,
        policy_profile: str,
        allow_high_risk: bool | None,
        allow_critical_risk: bool | None,
        allow_admin_clearance: bool | None,
        allow_destructive: bool | None,
        mission_status: str,
        mission_kind: str,
        app_name: str,
        stop_reason_code: str,
        resume_force: bool,
    ) -> Dict[str, Any]:
        normalized_profile = DesktopRecoverySupervisor._normalize_policy_profile(policy_profile)
        config = {
            "enabled": bool(enabled),
            "interval_s": DesktopRecoverySupervisor._coerce_float(interval_s, minimum=5.0, maximum=3600.0, default=45.0),
            "limit": DesktopRecoverySupervisor._coerce_int(limit, minimum=1, maximum=200, default=12),
            "max_auto_resumes": DesktopRecoverySupervisor._coerce_int(max_auto_resumes, minimum=0, maximum=32, default=2),
            "policy_profile": normalized_profile,
            "mission_status": str(mission_status or "paused").strip() or "paused",
            "mission_kind": str(mission_kind or "").strip(),
            "app_name": str(app_name or "").strip(),
            "stop_reason_code": str(stop_reason_code or "").strip(),
            "resume_force": bool(resume_force),
        }
        profile_defaults = DesktopRecoverySupervisor._policy_profile_defaults(normalized_profile)
        config["allow_high_risk"] = bool(profile_defaults["allow_high_risk"] if allow_high_risk is None else allow_high_risk)
        config["allow_critical_risk"] = bool(profile_defaults["allow_critical_risk"] if allow_critical_risk is None else allow_critical_risk)
        config["allow_admin_clearance"] = bool(profile_defaults["allow_admin_clearance"] if allow_admin_clearance is None else allow_admin_clearance)
        config["allow_destructive"] = bool(profile_defaults["allow_destructive"] if allow_destructive is None else allow_destructive)
        if any(value is not None for value in (allow_high_risk, allow_critical_risk, allow_admin_clearance, allow_destructive)):
            config["policy_profile"] = "custom"
        return config

    @staticmethod
    def _normalize_policy_profile(value: object) -> str:
        return normalize_desktop_governance_profile(value)

    @staticmethod
    def _policy_profile_defaults(profile: str) -> Dict[str, bool]:
        defaults = desktop_governance_profile_defaults(profile)
        return {
            "allow_high_risk": bool(defaults.get("allow_high_risk", False)),
            "allow_critical_risk": bool(defaults.get("allow_critical_risk", False)),
            "allow_admin_clearance": bool(defaults.get("allow_admin_clearance", False)),
            "allow_destructive": bool(defaults.get("allow_destructive", False)),
        }

    def _apply_policy_profile_defaults_locked(self, *, force: bool = False) -> None:
        profile = self._normalize_policy_profile(self._config.get("policy_profile", "balanced"))
        self._config["policy_profile"] = profile
        if profile == "custom" and not force:
            return
        defaults = self._policy_profile_defaults(profile)
        self._config["allow_high_risk"] = bool(defaults["allow_high_risk"])
        self._config["allow_critical_risk"] = bool(defaults["allow_critical_risk"])
        self._config["allow_admin_clearance"] = bool(defaults["allow_admin_clearance"])
        self._config["allow_destructive"] = bool(defaults["allow_destructive"])

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
        explicit_policy_profile = overrides.get("policy_profile") if overrides.get("policy_profile") is not None else None
        normalized_policy_profile = (
            DesktopRecoverySupervisor._normalize_policy_profile(explicit_policy_profile)
            if explicit_policy_profile is not None
            else ""
        )
        explicit_policy_override = False
        for key in (
            "enabled",
            "interval_s",
            "limit",
            "max_auto_resumes",
            "policy_profile",
            "allow_high_risk",
            "allow_critical_risk",
            "allow_admin_clearance",
            "allow_destructive",
            "mission_status",
            "mission_kind",
            "app_name",
            "stop_reason_code",
            "resume_force",
        ):
            if key in overrides and overrides[key] is not None:
                payload[key] = overrides[key]
                if key in {
                    "allow_high_risk",
                    "allow_critical_risk",
                    "allow_admin_clearance",
                    "allow_destructive",
                }:
                    explicit_policy_override = True
        if explicit_policy_profile is not None:
            payload["policy_profile"] = normalized_policy_profile
            if payload["policy_profile"] != "custom":
                defaults = DesktopRecoverySupervisor._policy_profile_defaults(payload["policy_profile"])
                for key, value in defaults.items():
                    if overrides.get(key) is None:
                        payload[key] = bool(value)
        if explicit_policy_override:
            if not normalized_policy_profile:
                payload["policy_profile"] = "custom"
            elif normalized_policy_profile == "custom":
                payload["policy_profile"] = "custom"
            else:
                defaults = DesktopRecoverySupervisor._policy_profile_defaults(normalized_policy_profile)
                if any(
                    overrides.get(key) is not None and bool(overrides.get(key)) != bool(defaults[key])
                    for key in defaults
                ):
                    payload["policy_profile"] = "custom"
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
