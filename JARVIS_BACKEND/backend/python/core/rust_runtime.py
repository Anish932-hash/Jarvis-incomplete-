from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any, Dict, Optional


class RustRuntimeBridge:
    """
    Thin stdio JSON-RPC bridge for the Rust backend binary.

    Request protocol:
      {"event": "<name>", "payload": {"request_id": "<id>", ...}}\n
    Response protocol:
      {"reply_to": "<id>", "status": "success|error", "data": ..., "message": "..."}\n
    """

    def __init__(self, *, logger: Any, binary_path: str = "") -> None:
        self.log = logger
        self._binary_path = self._resolve_binary_path(binary_path)
        self._project_root = self._binary_path.parents[4] if len(self._binary_path.parents) >= 5 else self._binary_path.parent
        self._launch_disabled = self._env_bool("JARVIS_RUST_DISABLED", False)

        self._process: Optional[subprocess.Popen[str]] = None
        self._stdout_queue: queue.Queue[Dict[str, Any]] = queue.Queue()
        self._stderr_tail: deque[str] = deque(maxlen=120)

        self._state_lock = threading.RLock()
        self._request_lock = threading.Lock()
        self._stats_lock = threading.RLock()
        self._stdout_thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None

        self._stats: Dict[str, Any] = {
            "requests_total": 0,
            "requests_success": 0,
            "requests_error": 0,
            "timeouts": 0,
            "spawn_count": 0,
            "restart_count": 0,
            "last_event": "",
            "last_status": "",
            "last_error": "",
            "last_roundtrip_ms": 0.0,
            "avg_roundtrip_ms": 0.0,
            "max_roundtrip_ms": 0.0,
            "updated_at": 0.0,
        }
        self._capabilities_cache: Dict[str, Any] = {}
        self._capabilities_cached_at_monotonic = 0.0
        self._capabilities_cache_ttl_s = 20.0
        self._cancel_grace_default_s = self._env_float("JARVIS_RUST_CANCEL_GRACE_S", 2.8, minimum=0.2, maximum=12.0)
        self._control_plane_events = {
            "health_check",
            "capabilities",
            "bridge_runtime_snapshot",
            "runtime_load",
            "runtime_policy_snapshot",
            "cancel_request",
        }

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        raw = str(os.getenv(name, "")).strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            return True
        if raw in {"0", "false", "no", "off"}:
            return False
        return default

    @staticmethod
    def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
        raw = str(os.getenv(name, "")).strip()
        try:
            value = float(raw)
        except Exception:
            value = float(default)
        if value < minimum:
            value = minimum
        if value > maximum:
            value = maximum
        return float(value)

    @staticmethod
    def _resolve_binary_path(explicit_path: str) -> Path:
        explicit = str(explicit_path or "").strip()
        if explicit:
            return Path(explicit).expanduser().resolve()

        env_path = str(os.getenv("JARVIS_RUST_BINARY", "")).strip()
        if env_path:
            return Path(env_path).expanduser().resolve()

        root = Path(__file__).resolve().parents[3]  # JARVIS_BACKEND/
        release_dir = root / "backend" / "rust" / "target" / "release"
        windows_candidate = release_dir / "jarvis_backend_bin.exe"
        unix_candidate = release_dir / "jarvis_backend_bin"
        if windows_candidate.exists():
            return windows_candidate
        if unix_candidate.exists():
            return unix_candidate
        return windows_candidate if os.name == "nt" else unix_candidate

    def _is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def _clear_stdout_queue(self) -> None:
        while True:
            try:
                self._stdout_queue.get_nowait()
            except queue.Empty:
                break

    def _start_process_locked(self) -> Dict[str, Any]:
        if self._launch_disabled:
            return {
                "status": "error",
                "error_code": "runtime_disabled",
                "message": "Rust runtime is disabled via JARVIS_RUST_DISABLED.",
            }

        if self._is_running():
            return {"status": "success", "message": "already_running"}

        if not self._binary_path.exists():
            return {
                "status": "error",
                "error_code": "runtime_missing",
                "message": f"Rust binary not found: {self._binary_path}",
            }

        try:
            creation_flags = 0
            if os.name == "nt":
                creation_flags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
            self._process = subprocess.Popen(
                [str(self._binary_path)],
                cwd=str(self._project_root),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=creation_flags,
            )
        except Exception as exc:  # noqa: BLE001
            self._process = None
            return {
                "status": "error",
                "error_code": "runtime_spawn_failed",
                "message": f"Failed to launch Rust runtime: {exc}",
            }

        self._clear_stdout_queue()
        self._stdout_thread = threading.Thread(target=self._stdout_reader, name="rust-stdout-reader", daemon=True)
        self._stderr_thread = threading.Thread(target=self._stderr_reader, name="rust-stderr-reader", daemon=True)
        self._stdout_thread.start()
        self._stderr_thread.start()

        with self._stats_lock:
            previous_spawns = int(self._stats.get("spawn_count", 0) or 0)
            self._stats["spawn_count"] = previous_spawns + 1
            if previous_spawns > 0:
                self._stats["restart_count"] = int(self._stats.get("restart_count", 0) or 0) + 1
            self._stats["updated_at"] = time.time()
        return {"status": "success", "message": "started"}

    def _stdout_reader(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return
        try:
            for raw in process.stdout:
                line = str(raw or "").strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except Exception as exc:  # noqa: BLE001
                    self._stdout_queue.put(
                        {
                            "event": "_decode_error",
                            "status": "error",
                            "message": f"Invalid JSON from Rust runtime: {exc}",
                            "raw": line,
                        }
                    )
                    continue

                if isinstance(parsed, dict):
                    self._stdout_queue.put(parsed)
                else:
                    self._stdout_queue.put({"event": "_non_object", "status": "error", "data": parsed})
        except Exception as exc:  # noqa: BLE001
            self._stdout_queue.put({"event": "_stdout_error", "status": "error", "message": str(exc)})
        finally:
            code = process.poll() if process else None
            self._stdout_queue.put({"event": "_process_exit", "status": "error", "code": code})

    def _stderr_reader(self) -> None:
        process = self._process
        if process is None or process.stderr is None:
            return
        try:
            for raw in process.stderr:
                line = str(raw or "").rstrip()
                if not line:
                    continue
                self._stderr_tail.append(line)
                try:
                    self.log.debug(f"[rust] {line}")
                except Exception:  # noqa: BLE001
                    pass
        except Exception as exc:  # noqa: BLE001
            self._stderr_tail.append(f"stderr reader failed: {exc}")

    def _tail_stderr(self, limit: int = 12) -> list[str]:
        if limit <= 0:
            return []
        rows = list(self._stderr_tail)
        return rows[-limit:]

    def _record_request_stat(self, *, event: str, status: str, elapsed_ms: float, error_message: str = "") -> None:
        with self._stats_lock:
            self._stats["requests_total"] = int(self._stats.get("requests_total", 0) or 0) + 1
            if status == "success":
                self._stats["requests_success"] = int(self._stats.get("requests_success", 0) or 0) + 1
            else:
                self._stats["requests_error"] = int(self._stats.get("requests_error", 0) or 0) + 1
                if "timeout" in error_message.lower():
                    self._stats["timeouts"] = int(self._stats.get("timeouts", 0) or 0) + 1
            self._stats["last_event"] = event
            self._stats["last_status"] = status
            self._stats["last_error"] = error_message
            self._stats["last_roundtrip_ms"] = round(float(elapsed_ms), 3)

            success = int(self._stats.get("requests_success", 0) or 0)
            errors = int(self._stats.get("requests_error", 0) or 0)
            count = success + errors
            previous_avg = float(self._stats.get("avg_roundtrip_ms", 0.0) or 0.0)
            if count <= 1:
                self._stats["avg_roundtrip_ms"] = round(float(elapsed_ms), 3)
            else:
                new_avg = ((previous_avg * (count - 1)) + float(elapsed_ms)) / count
                self._stats["avg_roundtrip_ms"] = round(new_avg, 3)

            self._stats["max_roundtrip_ms"] = round(
                max(float(self._stats.get("max_roundtrip_ms", 0.0) or 0.0), float(elapsed_ms)),
                3,
            )
            self._stats["updated_at"] = time.time()

    def diagnostics(self) -> Dict[str, Any]:
        with self._stats_lock:
            stats = dict(self._stats)
        stats["running"] = bool(self._is_running())
        stats["binary_path"] = str(self._binary_path)
        stats["disabled"] = bool(self._launch_disabled)
        stats["stderr_tail"] = self._tail_stderr(limit=10)
        if isinstance(self._capabilities_cache, dict) and self._capabilities_cache:
            supported = self._capabilities_cache.get("supported_events")
            if isinstance(supported, list):
                stats["capabilities_cached"] = True
                stats["cached_supported_events"] = list(supported)
                stats["cached_supported_count"] = len(supported)
        if bool(self._is_running()):
            try:
                load_payload = self.runtime_load(timeout_s=1.2)
                if str(load_payload.get("status", "")).strip().lower() == "success":
                    data = load_payload.get("data")
                    if isinstance(data, dict):
                        stats["runtime_load"] = data
                        policy_hint = data.get("policy_hint")
                        if isinstance(policy_hint, dict) and policy_hint:
                            stats["runtime_policy_hint"] = policy_hint
                if "runtime_policy_hint" not in stats:
                    policy_payload = self.runtime_policy_snapshot(timeout_s=1.2)
                    if str(policy_payload.get("status", "")).strip().lower() == "success":
                        policy_data = policy_payload.get("data")
                        if isinstance(policy_data, dict):
                            stats["runtime_policy"] = policy_data
            except Exception:
                pass
        return {"status": "success", "runtime": stats}

    def stop(self) -> None:
        with self._state_lock:
            process = self._process
            self._process = None

        if process is None:
            return
        try:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=2.5)
        except Exception:  # noqa: BLE001
            try:
                process.kill()
            except Exception:  # noqa: BLE001
                pass

    def health(self) -> Dict[str, Any]:
        binary_exists = self._binary_path.exists()
        if self._launch_disabled:
            return {
                "status": "success",
                "available": False,
                "running": False,
                "disabled": True,
                "binary_path": str(self._binary_path),
                "message": "Disabled via JARVIS_RUST_DISABLED.",
            }

        if not binary_exists:
            return {
                "status": "success",
                "available": False,
                "running": False,
                "disabled": False,
                "binary_path": str(self._binary_path),
                "message": "Rust binary not found.",
            }

        probe = self.request("health_check", payload={}, timeout_s=3.0)
        return {
            "status": "success",
            "available": True,
            "running": bool(self._is_running()),
            "disabled": False,
            "binary_path": str(self._binary_path),
            "probe": probe,
            "stderr_tail": self._tail_stderr(limit=8),
            "diagnostics": self.diagnostics().get("runtime", {}),
        }

    def request(
        self,
        event: str,
        payload: Optional[Dict[str, Any]] = None,
        timeout_s: float = 8.0,
        *,
        cancel_event: Optional[threading.Event] = None,
        cancel_reason: str = "cancelled_by_control",
        cancel_grace_s: float = 0.0,
    ) -> Dict[str, Any]:
        event_name = str(event or "").strip()
        if not event_name:
            return {"status": "error", "error_code": "invalid_event", "message": "event is required"}

        request_payload = dict(payload) if isinstance(payload, dict) else {}
        timeout = max(0.8, min(float(timeout_s), 45.0))
        cancellation_grace = (
            max(0.2, min(float(cancel_grace_s), 12.0))
            if float(cancel_grace_s or 0.0) > 0.0
            else float(self._cancel_grace_default_s)
        )
        clean_cancel_reason = str(cancel_reason or "").strip() or "cancelled_by_control"
        started = time.perf_counter()

        with self._request_lock:
            with self._state_lock:
                start_result = self._start_process_locked()
            if start_result.get("status") != "success":
                self._record_request_stat(
                    event=event_name,
                    status="error",
                    elapsed_ms=(time.perf_counter() - started) * 1000.0,
                    error_message=str(start_result.get("message", "runtime start failed")),
                )
                return start_result

            process = self._process
            if process is None or process.stdin is None:
                self._record_request_stat(
                    event=event_name,
                    status="error",
                    elapsed_ms=(time.perf_counter() - started) * 1000.0,
                    error_message="Rust runtime process is not available.",
                )
                return {
                    "status": "error",
                    "error_code": "runtime_unavailable",
                    "message": "Rust runtime process is not available.",
                }

            request_id = f"pyreq_{uuid.uuid4().hex}"
            outbound = {
                "event": event_name,
                "payload": {**request_payload, "request_id": request_id},
            }

            try:
                process.stdin.write(json.dumps(outbound, ensure_ascii=True) + "\n")
                process.stdin.flush()
            except Exception as exc:  # noqa: BLE001
                self.stop()
                self._record_request_stat(
                    event=event_name,
                    status="error",
                    elapsed_ms=(time.perf_counter() - started) * 1000.0,
                    error_message=str(exc),
                )
                return {
                    "status": "error",
                    "error_code": "runtime_write_failed",
                    "message": f"Failed to write request to Rust runtime: {exc}",
                    "stderr_tail": self._tail_stderr(limit=8),
                }

            cancel_sent = False
            cancel_request_id = ""
            cancel_ack: Dict[str, Any] = {}
            cancel_deadline = 0.0

            deadline = time.time() + timeout
            while time.time() < deadline:
                if (
                    not cancel_sent
                    and isinstance(cancel_event, threading.Event)
                    and cancel_event.is_set()
                    and event_name not in self._control_plane_events
                ):
                    cancel_request_id = f"pycancel_{uuid.uuid4().hex}"
                    cancellation_packet = {
                        "event": "cancel_request",
                        "payload": {
                            "request_id": cancel_request_id,
                            "target_request_id": request_id,
                            "reason": clean_cancel_reason,
                        },
                    }
                    try:
                        process.stdin.write(json.dumps(cancellation_packet, ensure_ascii=True) + "\n")
                        process.stdin.flush()
                        cancel_sent = True
                        cancel_deadline = time.time() + cancellation_grace
                    except Exception as exc:  # noqa: BLE001
                        self.stop()
                        elapsed_ms = (time.perf_counter() - started) * 1000.0
                        self._record_request_stat(
                            event=event_name,
                            status="error",
                            elapsed_ms=elapsed_ms,
                            error_message=f"cancel_write_failed:{exc}",
                        )
                        return {
                            "status": "cancelled",
                            "error_code": "cancel_signal_failed",
                            "event": event_name,
                            "message": f"Request cancellation failed while sending signal: {exc}",
                            "stderr_tail": self._tail_stderr(limit=10),
                        }

                if cancel_sent and cancel_deadline > 0.0 and time.time() >= cancel_deadline:
                    elapsed_ms = (time.perf_counter() - started) * 1000.0
                    self._record_request_stat(
                        event=event_name,
                        status="error",
                        elapsed_ms=elapsed_ms,
                        error_message="cancelled_by_control",
                    )
                    return {
                        "status": "cancelled",
                        "error_code": "cancelled",
                        "event": event_name,
                        "message": "Request cancelled by control plane.",
                        "cancel_ack": cancel_ack,
                    }

                remaining = max(0.01, deadline - time.time())
                try:
                    packet = self._stdout_queue.get(timeout=remaining)
                except queue.Empty:
                    break

                if not isinstance(packet, dict):
                    continue

                if str(packet.get("event", "")).strip() == "_process_exit":
                    self._record_request_stat(
                        event=event_name,
                        status="error",
                        elapsed_ms=(time.perf_counter() - started) * 1000.0,
                        error_message="Rust runtime exited before replying.",
                    )
                    return {
                        "status": "error",
                        "error_code": "runtime_exited",
                        "message": "Rust runtime exited before replying.",
                        "packet": packet,
                        "stderr_tail": self._tail_stderr(limit=10),
                    }

                reply_to = str(packet.get("reply_to", "")).strip()
                if reply_to != request_id:
                    if cancel_sent and cancel_request_id and reply_to == cancel_request_id:
                        if isinstance(packet, dict):
                            cancel_ack = dict(packet)
                    continue

                status = str(packet.get("status", "success")).strip().lower()
                if status == "success":
                    self._record_request_stat(
                        event=event_name,
                        status="success",
                        elapsed_ms=(time.perf_counter() - started) * 1000.0,
                    )
                    if cancel_sent and isinstance(cancel_event, threading.Event) and cancel_event.is_set():
                        return {
                            "status": "cancelled",
                            "error_code": "cancelled",
                            "event": event_name,
                            "message": "Request cancelled by control plane.",
                            "data": packet.get("data"),
                            "packet": packet,
                            "cancel_ack": cancel_ack,
                        }
                    return {
                        "status": "success",
                        "event": event_name,
                        "data": packet.get("data"),
                        "packet": packet,
                    }

                error_message = str(packet.get("message", "Rust runtime returned an error"))
                self._record_request_stat(
                    event=event_name,
                    status="error",
                    elapsed_ms=(time.perf_counter() - started) * 1000.0,
                    error_message=error_message,
                )
                if cancel_sent and "cancel" in error_message.lower():
                    return {
                        "status": "cancelled",
                        "error_code": "cancelled",
                        "event": event_name,
                        "message": error_message,
                        "data": packet.get("data"),
                        "packet": packet,
                        "cancel_ack": cancel_ack,
                    }
                return {
                    "status": "error",
                    "error_code": "runtime_error",
                    "event": event_name,
                    "message": error_message,
                    "data": packet.get("data"),
                    "packet": packet,
                }

            timeout_message = f"Timed out waiting for Rust runtime reply ({timeout:.1f}s)."
            self._record_request_stat(
                event=event_name,
                status="error",
                elapsed_ms=(time.perf_counter() - started) * 1000.0,
                error_message=timeout_message,
            )
            if cancel_sent:
                return {
                    "status": "cancelled",
                    "error_code": "cancelled",
                    "event": event_name,
                    "message": "Request cancellation requested; runtime reply was not received before timeout.",
                    "cancel_ack": cancel_ack,
                    "stderr_tail": self._tail_stderr(limit=10),
                }
            return {
                "status": "error",
                "error_code": "timeout",
                "event": event_name,
                "message": timeout_message,
                "stderr_tail": self._tail_stderr(limit=10),
            }

    def desktop_context(self, timeout_s: float = 8.0) -> Dict[str, Any]:
        return self.request("desktop_context", payload={}, timeout_s=timeout_s)

    def window_topology_snapshot(
        self,
        *,
        query: str = "",
        timeout_s: float = 4.0,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        clean_query = str(query or "").strip()
        if clean_query:
            payload["query"] = clean_query
        return self.request(
            "window_topology_snapshot",
            payload=payload,
            timeout_s=max(0.8, min(float(timeout_s), 15.0)),
        )

    def surface_exploration_router(
        self,
        *,
        payload: Optional[Dict[str, Any]] = None,
        timeout_s: float = 5.0,
    ) -> Dict[str, Any]:
        safe_payload = dict(payload) if isinstance(payload, dict) else {}
        return self.request(
            "surface_exploration_router",
            payload=safe_payload,
            timeout_s=max(0.8, min(float(timeout_s), 20.0)),
        )

    def runtime_load(self, *, timeout_s: float = 2.0) -> Dict[str, Any]:
        return self.request(
            "runtime_load",
            payload={},
            timeout_s=max(0.5, min(float(timeout_s), 8.0)),
        )

    def runtime_policy_snapshot(self, *, timeout_s: float = 2.5) -> Dict[str, Any]:
        return self.request(
            "runtime_policy_snapshot",
            payload={},
            timeout_s=max(0.5, min(float(timeout_s), 10.0)),
        )

    def cancel_request(
        self,
        *,
        target_request_id: str,
        reason: str = "",
        timeout_s: float = 2.5,
    ) -> Dict[str, Any]:
        clean_target = str(target_request_id or "").strip()
        if not clean_target:
            return {
                "status": "error",
                "error_code": "invalid_request_id",
                "message": "target_request_id is required",
            }
        payload: Dict[str, Any] = {"target_request_id": clean_target}
        clean_reason = str(reason or "").strip()
        if clean_reason:
            payload["reason"] = clean_reason
        return self.request(
            "cancel_request",
            payload=payload,
            timeout_s=max(0.5, min(float(timeout_s), 8.0)),
        )

    def capabilities(self, *, timeout_s: float = 4.0, refresh: bool = False) -> Dict[str, Any]:
        now = time.monotonic()
        if (
            not refresh
            and isinstance(self._capabilities_cache, dict)
            and self._capabilities_cache
            and (now - self._capabilities_cached_at_monotonic) <= self._capabilities_cache_ttl_s
        ):
            payload = dict(self._capabilities_cache)
            payload["cache_hit"] = True
            payload["cache_age_s"] = round(max(0.0, now - self._capabilities_cached_at_monotonic), 3)
            return payload

        result = self.request("capabilities", payload={}, timeout_s=max(0.8, min(float(timeout_s), 20.0)))
        if str(result.get("status", "")).strip().lower() != "success":
            result["cache_hit"] = False
            return result

        data = result.get("data")
        if not isinstance(data, dict):
            return {
                "status": "error",
                "error_code": "invalid_capabilities_payload",
                "message": "Rust runtime capabilities payload is invalid.",
                "cache_hit": False,
            }

        supported_events_raw = data.get("supported_events")
        if not isinstance(supported_events_raw, list):
            return {
                "status": "error",
                "error_code": "invalid_capabilities_payload",
                "message": "Rust runtime capabilities did not include supported_events.",
                "cache_hit": False,
            }

        supported_events: list[str] = []
        seen: set[str] = set()
        for item in supported_events_raw:
            clean = str(item or "").strip()
            if not clean:
                continue
            lowered = clean.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            supported_events.append(clean)

        capability_payload = {
            "status": "success",
            "supported_events": supported_events,
            "supported_count": len(supported_events),
            "cache_hit": False,
            "fetched_at": time.time(),
        }
        self._capabilities_cache = dict(capability_payload)
        self._capabilities_cached_at_monotonic = now
        return capability_payload

    def supports_event(self, event: str, *, timeout_s: float = 4.0) -> bool:
        clean_event = str(event or "").strip()
        if not clean_event:
            return False
        caps = self.capabilities(timeout_s=timeout_s, refresh=False)
        if str(caps.get("status", "")).strip().lower() != "success":
            return False
        rows = caps.get("supported_events")
        if not isinstance(rows, list):
            return False
        return any(str(item).strip() == clean_event for item in rows)

    def bridge_preflight(self, *, include_context: bool = False, timeout_s: float = 8.0) -> Dict[str, Any]:
        bounded_timeout = max(1.0, min(float(timeout_s), 45.0))
        health_timeout = min(6.0, max(1.0, bounded_timeout * 0.4))
        caps_timeout = min(6.0, max(1.0, bounded_timeout * 0.35))
        context_timeout = max(1.5, min(20.0, bounded_timeout * 0.75))

        health = self.health()
        capabilities = self.capabilities(timeout_s=caps_timeout, refresh=False)
        diagnostics = self.diagnostics()

        checks: list[Dict[str, Any]] = []
        score = 1.0

        probe = health.get("probe") if isinstance(health.get("probe"), dict) else {}
        probe_ok = str(probe.get("status", "")).strip().lower() == "success"
        available = bool(health.get("available"))
        running = bool(health.get("running"))
        if not available:
            score -= 0.7
            checks.append({"name": "runtime_available", "status": "error", "message": "Rust runtime binary is unavailable."})
        else:
            checks.append({"name": "runtime_available", "status": "success"})
        if not running:
            score -= 0.25
            checks.append({"name": "runtime_running", "status": "warn", "message": "Rust runtime is not currently running."})
        else:
            checks.append({"name": "runtime_running", "status": "success"})
        if not probe_ok:
            score -= 0.4
            checks.append(
                {
                    "name": "health_probe",
                    "status": "error",
                    "message": str(probe.get("message", "health_check probe failed")),
                }
            )
        else:
            checks.append({"name": "health_probe", "status": "success"})

        supported_events: list[str] = []
        supported_set: set[str] = set()
        if str(capabilities.get("status", "")).strip().lower() == "success":
            rows = capabilities.get("supported_events")
            if isinstance(rows, list):
                supported_events = [str(item) for item in rows if str(item).strip()]
                supported_set = {str(item).strip() for item in supported_events if str(item).strip()}
            checks.append(
                {
                    "name": "capabilities",
                    "status": "success",
                    "count": len(supported_events),
                    "cache_hit": bool(capabilities.get("cache_hit", False)),
                }
            )
        else:
            score -= 0.35
            checks.append(
                {
                    "name": "capabilities",
                    "status": "error",
                    "message": str(capabilities.get("message", "capabilities probe failed")),
                }
            )

        required_events = {
            "health_check",
            "capabilities",
            "bridge_runtime_snapshot",
            "runtime_load",
            "runtime_policy_snapshot",
            "cancel_request",
            "desktop_context",
            "batch_execute",
            "automation_plan_execute",
        }
        if supported_set:
            missing_required = sorted(item for item in required_events if item not in supported_set)
            if missing_required:
                score -= min(0.36, 0.06 * len(missing_required))
                checks.append(
                    {
                        "name": "required_events",
                        "status": "warn",
                        "missing": missing_required,
                        "message": "Some required bridge events are missing from Rust capabilities.",
                    }
                )
            else:
                checks.append({"name": "required_events", "status": "success", "count": len(required_events)})
            if "runtime_policy_snapshot" in supported_set:
                try:
                    policy_payload = self.runtime_policy_snapshot(timeout_s=min(4.0, bounded_timeout * 0.35))
                    if str(policy_payload.get("status", "")).strip().lower() == "success":
                        checks.append({"name": "runtime_policy_snapshot", "status": "success"})
                    else:
                        score -= 0.12
                        checks.append(
                            {
                                "name": "runtime_policy_snapshot",
                                "status": "warn",
                                "message": str(policy_payload.get("message", "runtime policy snapshot failed")),
                            }
                        )
                except Exception as exc:  # noqa: BLE001
                    score -= 0.12
                    checks.append(
                        {
                            "name": "runtime_policy_snapshot",
                            "status": "warn",
                            "message": str(exc),
                        }
                    )

        context_payload: Dict[str, Any] = {}
        if include_context:
            context_payload = self.desktop_context(timeout_s=context_timeout)
            context_status = str(context_payload.get("status", "")).strip().lower()
            if context_status == "success":
                checks.append({"name": "desktop_context", "status": "success"})
            else:
                score -= 0.2
                checks.append(
                    {
                        "name": "desktop_context",
                        "status": "warn",
                        "message": str(context_payload.get("message", "desktop context probe failed")),
                    }
                )

        final_score = round(max(0.0, min(1.0, score)), 3)
        if final_score >= 0.8:
            readiness = "ready"
        elif final_score >= 0.5:
            readiness = "degraded"
        else:
            readiness = "blocked"

        payload: Dict[str, Any] = {
            "status": "success",
            "readiness": readiness,
            "score": final_score,
            "checks": checks,
            "health": health,
            "capabilities": capabilities,
            "supported_events": supported_events,
            "diagnostics": diagnostics.get("runtime", diagnostics),
            "timeouts": {
                "total_s": bounded_timeout,
                "health_s": health_timeout,
                "capabilities_s": caps_timeout,
                "desktop_context_s": context_timeout if include_context else 0.0,
            },
        }
        if include_context:
            payload["desktop_context"] = context_payload
        return payload

    def batch_execute(
        self,
        *,
        requests: list[Dict[str, Any]],
        continue_on_error: bool = False,
        include_timing: bool = True,
        max_steps: int = 64,
        timeout_s: float = 15.0,
    ) -> Dict[str, Any]:
        payload = {
            "requests": requests,
            "continue_on_error": bool(continue_on_error),
            "include_timing": bool(include_timing),
            "max_steps": max(1, min(int(max_steps), 256)),
        }
        return self.request("batch_execute", payload=payload, timeout_s=max(1.0, min(float(timeout_s), 45.0)))

    def automation_plan_execute(
        self,
        *,
        tasks: list[Dict[str, Any]],
        options: Optional[Dict[str, Any]] = None,
        timeout_s: float = 25.0,
    ) -> Dict[str, Any]:
        payload = {"tasks": tasks}
        if isinstance(options, dict) and options:
            payload["options"] = options
        return self.request(
            "automation_plan_execute",
            payload=payload,
            timeout_s=max(2.0, min(float(timeout_s), 60.0)),
        )
