from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


class LocalNeuralTtsBridge:
    """
    Supervises an optional local neural TTS server process and probes its HTTP endpoint.
    """

    _shared: Optional["LocalNeuralTtsBridge"] = None
    _shared_lock = threading.Lock()

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._process: Optional[subprocess.Popen[Any]] = None
        self._runtime_overrides: Dict[str, Any] = {}
        self._active_profile_id = ""
        self._active_template_id = ""
        self._state: Dict[str, Any] = {
            "last_start_at": 0.0,
            "last_stop_at": 0.0,
            "last_probe_at": 0.0,
            "last_probe_ok": False,
            "last_error": "",
            "last_message": "",
            "last_pid": 0,
            "last_exit_code": None,
            "start_attempts": 0,
            "probe_attempts": 0,
            "restart_count": 0,
            "cooldown_until": 0.0,
            "last_start_reason": "",
            "last_stop_reason": "",
            "last_probe_url": "",
        }

    @classmethod
    def shared(cls) -> "LocalNeuralTtsBridge":
        with cls._shared_lock:
            if cls._shared is None:
                cls._shared = cls()
            return cls._shared

    def status(self, *, probe: bool = False) -> Dict[str, Any]:
        with self._lock:
            self._refresh_process_state_locked()
            payload = self._status_payload_locked()
        if probe and bool(payload.get("endpoint_configured", False)):
            return self.probe(force=True)
        return payload

    def set_runtime_overrides(
        self,
        *,
        updates: Dict[str, Any],
        profile_id: str = "",
        template_id: str = "",
        replace: bool = False,
    ) -> Dict[str, Any]:
        clean_updates = dict(updates or {})
        with self._lock:
            if replace:
                self._runtime_overrides = {}
            for key, value in clean_updates.items():
                normalized = self._normalize_override_value(key, value)
                if normalized is None:
                    self._runtime_overrides.pop(str(key or "").strip(), None)
                else:
                    self._runtime_overrides[str(key or "").strip()] = normalized
            self._active_profile_id = str(profile_id or self._active_profile_id or "").strip()
            self._active_template_id = str(template_id or self._active_template_id or "").strip()
            self._refresh_process_state_locked()
            payload = self._status_payload_locked()
        payload["applied_profile_id"] = self._active_profile_id
        payload["applied_template_id"] = self._active_template_id
        payload["message"] = (
            f"Applied local neural TTS template '{self._active_template_id}' for profile '{self._active_profile_id}'."
            if self._active_profile_id and self._active_template_id
            else (
                f"Applied local neural TTS profile '{self._active_profile_id}'."
                if self._active_profile_id
                else "Applied local neural TTS runtime overrides."
            )
        )
        return payload

    def clear_runtime_overrides(self, *, keys: Optional[List[str]] = None) -> Dict[str, Any]:
        with self._lock:
            if isinstance(keys, list) and keys:
                for key in keys:
                    clean_key = str(key or "").strip()
                    if clean_key:
                        self._runtime_overrides.pop(clean_key, None)
            else:
                self._runtime_overrides = {}
                self._active_profile_id = ""
                self._active_template_id = ""
            self._refresh_process_state_locked()
            payload = self._status_payload_locked()
        payload["message"] = "Cleared local neural TTS runtime overrides."
        return payload

    def probe(self, *, force: bool = True) -> Dict[str, Any]:
        with self._lock:
            self._refresh_process_state_locked()
            config = self._load_config_locked()
            self._state["probe_attempts"] = int(self._state.get("probe_attempts", 0) or 0) + 1
        if not bool(config.get("enabled", False)):
            with self._lock:
                self._state["last_probe_at"] = time.time()
                self._state["last_probe_ok"] = False
                self._state["last_message"] = "Local neural TTS bridge is disabled."
            return self.status(probe=False)
        if not bool(config.get("endpoint_configured", False)):
            with self._lock:
                self._state["last_probe_at"] = time.time()
                self._state["last_probe_ok"] = False
                self._state["last_message"] = "No neural TTS endpoint is configured."
            return self.status(probe=False)

        result = self._perform_probe(config)
        with self._lock:
            self._state["last_probe_at"] = time.time()
            self._state["last_probe_ok"] = bool(result.get("ready", False))
            self._state["last_message"] = str(result.get("message", "")).strip()
            self._state["last_error"] = str(result.get("error", "")).strip()
            self._state["last_probe_url"] = str(result.get("probe_url", "")).strip()
            if bool(result.get("ready", False)):
                self._state["cooldown_until"] = 0.0
            elif force and bool(config.get("managed", False)):
                cooldown_s = float(config.get("cooldown_s", 12.0) or 12.0)
                self._state["cooldown_until"] = max(float(self._state.get("cooldown_until", 0.0) or 0.0), time.time() + cooldown_s)
        return self.status(probe=False)

    def ensure_started(
        self,
        *,
        reason: str = "auto",
        wait_ready: bool = True,
        timeout_s: Optional[float] = None,
    ) -> Dict[str, Any]:
        current = self.status(probe=True)
        if bool(current.get("ready", False)):
            return current
        if not bool(current.get("managed", False)):
            return current
        if not (bool(current.get("autostart", False)) or str(reason or "").strip().lower().startswith("manual")):
            return current
        return self.start(wait_ready=wait_ready, timeout_s=timeout_s, reason=reason)

    def start(
        self,
        *,
        wait_ready: bool = True,
        timeout_s: Optional[float] = None,
        reason: str = "manual_start",
        force: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            self._refresh_process_state_locked()
            config = self._load_config_locked()
            if not bool(config.get("enabled", False)):
                self._state["last_message"] = "Local neural TTS bridge is disabled."
                return self._status_payload_locked()
            if not bool(config.get("managed", False)):
                self._state["last_message"] = "No local neural TTS bridge command is configured."
                return self._status_payload_locked()
            cooldown_until = float(self._state.get("cooldown_until", 0.0) or 0.0)
            if not force and cooldown_until > time.time():
                self._state["last_message"] = "Bridge start is in cooldown after a failed probe."
                return self._status_payload_locked()
            if self._process is not None and self._process.poll() is None:
                return self._status_payload_locked()

            command = str(config.get("server_command", "")).strip()
            cwd = str(config.get("server_cwd", "")).strip() or None
            creationflags = 0
            if os.name == "nt":
                creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
            try:
                self._process = subprocess.Popen(  # noqa: S603
                    command,
                    shell=True,
                    cwd=cwd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=creationflags,
                )
            except Exception as exc:  # noqa: BLE001
                self._process = None
                self._state["last_error"] = str(exc)
                self._state["last_message"] = f"Failed to start local neural TTS bridge: {exc}"
                cooldown_s = float(config.get("cooldown_s", 12.0) or 12.0)
                self._state["cooldown_until"] = time.time() + cooldown_s
                return self._status_payload_locked()

            self._state["start_attempts"] = int(self._state.get("start_attempts", 0) or 0) + 1
            self._state["last_start_at"] = time.time()
            self._state["last_start_reason"] = str(reason or "").strip()
            self._state["last_pid"] = int(getattr(self._process, "pid", 0) or 0)
            self._state["last_exit_code"] = None
            self._state["last_error"] = ""
            self._state["last_message"] = "Bridge process started."
            if int(self._state["start_attempts"]) > 1:
                self._state["restart_count"] = int(self._state.get("restart_count", 0) or 0) + 1

        if wait_ready:
            deadline = time.time() + float(timeout_s or config.get("startup_timeout_s", 15.0) or 15.0)
            while time.time() < deadline:
                status = self.probe(force=True)
                if bool(status.get("ready", False)):
                    return status
                with self._lock:
                    self._refresh_process_state_locked()
                    if self._process is not None and self._process.poll() is not None:
                        self._state["last_message"] = "Bridge process exited before becoming ready."
                        return self._status_payload_locked()
                time.sleep(0.45)
            with self._lock:
                self._state["last_message"] = "Bridge start timed out before the endpoint became ready."
                self._state["last_error"] = self._state["last_message"]
            return self.status(probe=False)

        return self.status(probe=False)

    def stop(self, *, reason: str = "manual_stop") -> Dict[str, Any]:
        with self._lock:
            self._refresh_process_state_locked()
            process = self._process
            if process is None or process.poll() is not None:
                self._process = None
                self._state["last_message"] = "Bridge process is not running."
                return self._status_payload_locked()
            try:
                process.terminate()
                process.wait(timeout=5.0)
            except Exception:  # noqa: BLE001
                try:
                    process.kill()
                    process.wait(timeout=3.0)
                except Exception as exc:  # noqa: BLE001
                    self._state["last_error"] = str(exc)
                    self._state["last_message"] = f"Failed to stop bridge process: {exc}"
                    return self._status_payload_locked()
            finally:
                self._state["last_stop_at"] = time.time()
                self._state["last_stop_reason"] = str(reason or "").strip()
                self._state["last_exit_code"] = process.poll()
                self._process = None
                self._state["last_pid"] = 0
                self._state["last_probe_ok"] = False
            self._state["last_message"] = "Bridge process stopped."
            return self._status_payload_locked()

    def _perform_probe(self, config: Dict[str, Any]) -> Dict[str, Any]:
        timeout_s = float(config.get("probe_timeout_s", 3.5) or 3.5)
        probe_candidates = list(config.get("probe_candidates", []))
        last_error = ""
        last_message = ""
        for candidate in probe_candidates:
            url = str(candidate or "").strip()
            if not url:
                continue
            request = urllib.request.Request(
                url,
                headers={"Accept": "application/json", "User-Agent": "JARVIS-Local-TTS-Bridge/1.0"},
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout_s) as response:
                    body = response.read(4096)
                    message = ""
                    parsed: Any = None
                    if body:
                        try:
                            parsed = json.loads(body.decode("utf-8"))
                        except Exception:
                            parsed = None
                    if isinstance(parsed, dict):
                        message = str(parsed.get("message", parsed.get("status", ""))).strip()
                    status_code = int(getattr(response, "status", response.getcode()) or 200)
                    if 200 <= status_code < 400:
                        return {
                            "ready": True,
                            "message": message or "Endpoint probe succeeded.",
                            "probe_url": url,
                            "probe_status": status_code,
                            "probe_payload": parsed if isinstance(parsed, dict) else {},
                        }
                    last_message = f"HTTP {status_code} from {url}"
            except urllib.error.HTTPError as exc:
                last_error = f"HTTP {exc.code} from {url}"
                last_message = last_error
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                last_message = f"Probe failed for {url}: {exc}"
        return {
            "ready": False,
            "message": last_message or "Unable to reach local neural TTS endpoint.",
            "error": last_error or last_message or "probe_failed",
            "probe_url": probe_candidates[0] if probe_candidates else "",
        }

    def _status_payload_locked(self) -> Dict[str, Any]:
        config = self._load_config_locked()
        process = self._process
        running = process is not None and process.poll() is None
        now = time.time()
        return {
            "status": "success",
            "enabled": bool(config.get("enabled", False)),
            "configured": bool(config.get("configured", False)),
            "managed": bool(config.get("managed", False)),
            "autostart": bool(config.get("autostart", False)),
            "server_command_configured": bool(config.get("managed", False)),
            "server_cwd": str(config.get("server_cwd", "")).strip(),
            "endpoint": str(config.get("endpoint", "")).strip(),
            "endpoint_configured": bool(config.get("endpoint_configured", False)),
            "healthcheck_url": str(config.get("healthcheck_url", "")).strip(),
            "probe_candidates": list(config.get("probe_candidates", [])),
            "active_profile_id": str(self._active_profile_id or "").strip(),
            "active_template_id": str(self._active_template_id or "").strip(),
            "runtime_overrides": dict(self._runtime_overrides),
            "running": running,
            "pid": int(getattr(process, "pid", 0) or 0) if running else 0,
            "ready": bool(self._state.get("last_probe_ok", False)),
            "message": str(self._state.get("last_message", "")).strip(),
            "last_error": str(self._state.get("last_error", "")).strip(),
            "last_probe_at": float(self._state.get("last_probe_at", 0.0) or 0.0),
            "last_probe_url": str(self._state.get("last_probe_url", "")).strip(),
            "last_start_at": float(self._state.get("last_start_at", 0.0) or 0.0),
            "last_stop_at": float(self._state.get("last_stop_at", 0.0) or 0.0),
            "last_exit_code": self._state.get("last_exit_code"),
            "last_pid": int(self._state.get("last_pid", 0) or 0),
            "start_attempts": int(self._state.get("start_attempts", 0) or 0),
            "probe_attempts": int(self._state.get("probe_attempts", 0) or 0),
            "restart_count": int(self._state.get("restart_count", 0) or 0),
            "cooldown_until": float(self._state.get("cooldown_until", 0.0) or 0.0),
            "cooldown_remaining_s": round(max(0.0, float(self._state.get("cooldown_until", 0.0) or 0.0) - now), 3),
            "last_start_reason": str(self._state.get("last_start_reason", "")).strip(),
            "last_stop_reason": str(self._state.get("last_stop_reason", "")).strip(),
        }

    def _refresh_process_state_locked(self) -> None:
        process = self._process
        if process is None:
            return
        return_code = process.poll()
        if return_code is None:
            return
        self._state["last_exit_code"] = return_code
        self._state["last_pid"] = int(getattr(process, "pid", 0) or 0)
        self._process = None

    def _load_config_locked(self) -> Dict[str, Any]:
        endpoint = str(os.getenv("JARVIS_LOCAL_NEURAL_TTS_HTTP_ENDPOINT", "") or "").strip()
        managed_command = str(
            os.getenv(
                "JARVIS_LOCAL_NEURAL_TTS_SERVER_COMMAND",
                os.getenv("JARVIS_LOCAL_NEURAL_TTS_BRIDGE_COMMAND", ""),
            )
            or ""
        ).strip()
        server_cwd = str(
            os.getenv(
                "JARVIS_LOCAL_NEURAL_TTS_SERVER_CWD",
                os.getenv("JARVIS_LOCAL_NEURAL_TTS_BRIDGE_CWD", ""),
            )
            or ""
        ).strip()
        if not server_cwd:
            model_path = str(os.getenv("JARVIS_LOCAL_NEURAL_TTS_MODEL_PATH", "") or "").strip()
            if model_path:
                try:
                    server_cwd = str(Path(model_path).resolve().parent)
                except Exception:
                    server_cwd = str(Path(model_path).parent)
        healthcheck_url = str(
            os.getenv(
                "JARVIS_LOCAL_NEURAL_TTS_SERVER_HEALTHCHECK_URL",
                os.getenv("JARVIS_LOCAL_NEURAL_TTS_BRIDGE_HEALTHCHECK_URL", ""),
            )
            or ""
        ).strip()
        enabled_default = bool(endpoint or managed_command)
        enabled = self._as_bool(os.getenv("JARVIS_LOCAL_NEURAL_TTS_BRIDGE_ENABLED", str(enabled_default)), default=enabled_default)
        autostart_default = bool(endpoint and managed_command)
        autostart = self._as_bool(
            os.getenv("JARVIS_LOCAL_NEURAL_TTS_SERVER_AUTO_START", os.getenv("JARVIS_LOCAL_NEURAL_TTS_BRIDGE_AUTO_START", str(autostart_default))),
            default=autostart_default,
        )
        config = {
            "enabled": enabled,
            "autostart": autostart,
            "server_command": managed_command,
            "server_cwd": server_cwd,
            "endpoint": endpoint,
            "healthcheck_url": healthcheck_url,
            "startup_timeout_s": self._env_float(
                "JARVIS_LOCAL_NEURAL_TTS_SERVER_STARTUP_TIMEOUT_S",
                18.0,
                minimum=2.0,
                maximum=240.0,
            ),
            "probe_timeout_s": self._env_float(
                "JARVIS_LOCAL_NEURAL_TTS_SERVER_PROBE_TIMEOUT_S",
                3.5,
                minimum=0.2,
                maximum=30.0,
            ),
            "cooldown_s": self._env_float(
                "JARVIS_LOCAL_NEURAL_TTS_SERVER_COOLDOWN_S",
                10.0,
                minimum=1.0,
                maximum=300.0,
            ),
        }
        self._apply_runtime_overrides_locked(config)
        endpoint = str(config.get("endpoint", "") or "").strip()
        managed_command = str(config.get("server_command", "") or "").strip()
        healthcheck_url = str(config.get("healthcheck_url", "") or "").strip()
        config["enabled"] = bool(config.get("enabled", enabled))
        config["configured"] = bool(endpoint or managed_command)
        config["managed"] = bool(managed_command)
        config["autostart"] = bool(config.get("autostart", autostart))
        config["endpoint"] = endpoint
        config["server_command"] = managed_command
        config["healthcheck_url"] = healthcheck_url
        config["endpoint_configured"] = bool(endpoint)
        config["probe_candidates"] = self._probe_candidates(endpoint=endpoint, healthcheck_url=healthcheck_url)
        return config

    def _apply_runtime_overrides_locked(self, config: Dict[str, Any]) -> None:
        for key, value in self._runtime_overrides.items():
            normalized = self._normalize_override_value(key, value)
            if normalized is None:
                continue
            config[str(key)] = normalized

    def _normalize_override_value(self, key: str, value: Any) -> Any:
        clean_key = str(key or "").strip()
        if not clean_key:
            return None
        if value is None:
            return None
        if clean_key in {"enabled", "autostart", "retain_output"}:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return self._as_bool(value, default=False)
            return bool(value)
        if clean_key in {"startup_timeout_s", "probe_timeout_s", "cooldown_s", "timeout_s"}:
            try:
                numeric = float(value)
            except Exception:
                return None
            limits = {
                "startup_timeout_s": (2.0, 240.0),
                "probe_timeout_s": (0.2, 30.0),
                "cooldown_s": (1.0, 300.0),
                "timeout_s": (5.0, 900.0),
            }
            minimum, maximum = limits[clean_key]
            return max(minimum, min(maximum, numeric))
        if clean_key in {
            "server_command",
            "server_cwd",
            "endpoint",
            "http_endpoint",
            "healthcheck_url",
            "model_path",
            "config_path",
            "backend",
            "execution_backend",
            "command_template",
            "http_model",
            "coqui_model_name",
            "model_label",
            "voice",
            "output_format",
        }:
            text = str(value or "").strip()
            return text or None
        return None

    @staticmethod
    def _probe_candidates(*, endpoint: str, healthcheck_url: str) -> List[str]:
        values: List[str] = []
        if healthcheck_url:
            values.append(healthcheck_url)
        if endpoint:
            parsed = urlparse(endpoint)
            if parsed.scheme and parsed.netloc:
                base = f"{parsed.scheme}://{parsed.netloc}"
                values.append(f"{base}/health")
                values.append(base)
            values.append(endpoint)
        deduped: List[str] = []
        seen = set()
        for item in values:
            clean = str(item or "").strip()
            if clean and clean not in seen:
                seen.add(clean)
                deduped.append(clean)
        return deduped

    @staticmethod
    def _as_bool(raw: Optional[str], *, default: bool) -> bool:
        value = str(raw or "").strip().lower()
        if not value:
            return default
        if value in {"1", "true", "yes", "on"}:
            return True
        if value in {"0", "false", "no", "off"}:
            return False
        return default

    @staticmethod
    def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
        raw = str(os.getenv(name, "") or "").strip()
        if not raw:
            return default
        try:
            value = float(raw)
        except Exception:
            return default
        return max(minimum, min(maximum, value))
