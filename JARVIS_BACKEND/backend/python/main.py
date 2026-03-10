import argparse
import asyncio
import hashlib
import json
import os
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.python.core.kernel import AgentKernel
from backend.python.desktop_api import DesktopBackendService, JarvisAPIHandler, JarvisHTTPServer
from backend.python.event_bus import EventBus
from backend.python.router import Router
from backend.python.settings import Settings
from backend.python.utils.error_handler import global_exception_handler
from backend.python.utils.logger import Logger


class AIKernelRuntime:
    def __init__(self) -> None:
        self.settings = Settings(config_path="configs/jarvis.yaml")
        self.logger = Logger.get_logger("AIKernelRuntime")
        self.event_bus = EventBus(autostart=True)
        self.router = Router(self.event_bus, self.settings)
        self.agent = AgentKernel()
        self._shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._started = False
        self._state = "idle"
        self._last_error = ""
        self._started_at_monotonic = 0.0
        self._last_transition_at = _utc_now_iso()
        self._include_diagnostics = _env_bool("JARVIS_RUNTIME_KERNEL_INCLUDE_DIAGNOSTICS", False)
        self._diagnostics_refresh_s = _env_float("JARVIS_RUNTIME_KERNEL_DIAGNOSTICS_REFRESH_S", 8.0, minimum=1.0, maximum=300.0)
        self._diagnostics_limit = _env_int("JARVIS_RUNTIME_KERNEL_DIAGNOSTICS_LIMIT", 180, minimum=20, maximum=5000)
        self._diagnostics_cached: dict[str, Any] = {}
        self._diagnostics_last_fetch_monotonic = 0.0

    def _set_state(self, state: str, *, error: str = "") -> None:
        self._state = str(state or "unknown").strip().lower() or "unknown"
        self._last_transition_at = _utc_now_iso()
        if error:
            self._last_error = str(error).strip()

    async def boot(self, *, timeout_s: float = 90.0) -> None:
        async with self._lock:
            if self._started:
                return
            self.logger.info("Booting AI kernel runtime...")
            self._set_state("starting")
            timeout = max(5.0, min(float(timeout_s), 1200.0))

            async def _boot_sequence() -> None:
                self.event_bus.set_router(self.router)
                await self.router.load_routes()
                await self.agent.start()

            try:
                await asyncio.wait_for(_boot_sequence(), timeout=timeout)
            except Exception as exc:  # noqa: BLE001
                message = f"Kernel boot failed: {_safe_error(exc)}"
                self._set_state("failed", error=message)
                self.logger.error(message)
                raise

            self._started = True
            self._started_at_monotonic = time.monotonic()
            self._set_state("online")
            self.logger.info("AI kernel runtime is online.")

    async def wait(self) -> None:
        await self._shutdown_event.wait()

    async def run_forever(self) -> None:
        await self.boot()
        await self.wait()

    async def shutdown(self, *, reason: str = "shutdown", timeout_s: float = 45.0) -> None:
        async with self._lock:
            if not self._started:
                self._set_state("offline")
                self._shutdown_event.set()
                return
            self.logger.info(f"Shutting down AI kernel runtime ({reason})...")
            self._set_state("stopping")
            timeout = max(2.0, min(float(timeout_s), 600.0))
            shutdown_errors: list[str] = []
            try:
                await asyncio.wait_for(self.agent.stop(), timeout=timeout)
            except Exception as exc:  # noqa: BLE001
                shutdown_errors.append(f"agent.stop: {_safe_error(exc)}")
            try:
                await asyncio.wait_for(self.event_bus.shutdown(), timeout=timeout)
            except Exception as exc:  # noqa: BLE001
                shutdown_errors.append(f"event_bus.shutdown: {_safe_error(exc)}")
            self._started = False
            self._started_at_monotonic = 0.0
            self._shutdown_event.set()
            if shutdown_errors:
                message = "; ".join(shutdown_errors)
                self._set_state("failed", error=message)
                self.logger.error(f"Kernel shutdown completed with errors: {message}")
            else:
                self._set_state("offline")
                self.logger.info("AI kernel runtime shutdown complete.")

    def snapshot(self) -> dict[str, Any]:
        worker = getattr(self.agent, "_worker", None)
        worker_alive = bool(worker is not None and not worker.done())
        running = bool(getattr(self.agent, "_running", False))
        uptime_s = 0.0
        if self._started and self._started_at_monotonic > 0:
            uptime_s = max(0.0, time.monotonic() - self._started_at_monotonic)
        payload = {
            "type": "kernel",
            "state": self._state,
            "started": bool(self._started),
            "running": running,
            "worker_alive": worker_alive,
            "uptime_s": round(uptime_s, 3),
            "last_error": self._last_error,
            "last_transition_at": self._last_transition_at,
        }
        if self._include_diagnostics:
            diagnostics = self._diagnostics_snapshot()
            payload["diagnostics"] = diagnostics
        return payload

    def _diagnostics_snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        if (
            isinstance(self._diagnostics_cached, dict)
            and self._diagnostics_cached
            and (now - self._diagnostics_last_fetch_monotonic) < self._diagnostics_refresh_s
        ):
            return dict(self._diagnostics_cached)
        try:
            bundle_fn = getattr(self.agent, "runtime_diagnostics_bundle", None)
            if callable(bundle_fn):
                payload = bundle_fn(limit=self._diagnostics_limit)
                if isinstance(payload, dict):
                    readiness = payload.get("readiness", {})
                    compact = {
                        "status": str(payload.get("status", "success")),
                        "readiness": readiness if isinstance(readiness, dict) else {},
                        "queue_length": int(payload.get("queue", {}).get("queue_length", 0))
                        if isinstance(payload.get("queue", {}), dict)
                        else 0,
                        "open_breakers": int(payload.get("circuit_breakers", {}).get("open_count", 0))
                        if isinstance(payload.get("circuit_breakers", {}), dict)
                        else 0,
                        "degraded_providers": int(payload.get("external_reliability", {}).get("degraded_count", 0))
                        if isinstance(payload.get("external_reliability", {}), dict)
                        else 0,
                        "pressure_score": float(payload.get("pressure", {}).get("score", 0.0))
                        if isinstance(payload.get("pressure", {}), dict)
                        else 0.0,
                        "contract_pressure": float(payload.get("pressure", {}).get("contract", 0.0))
                        if isinstance(payload.get("pressure", {}), dict)
                        else 0.0,
                        "alert_count": len(payload.get("alerts", [])) if isinstance(payload.get("alerts", []), list) else 0,
                        "top_recommendation": str(payload.get("recommendations", [""])[0] if isinstance(payload.get("recommendations", []), list) and payload.get("recommendations", []) else "").strip(),
                    }
                    self._diagnostics_cached = compact
                    self._diagnostics_last_fetch_monotonic = now
                    return dict(compact)
        except Exception as exc:  # noqa: BLE001
            fallback = {"status": "error", "message": _safe_error(exc)}
            self._diagnostics_cached = fallback
            self._diagnostics_last_fetch_monotonic = now
            return fallback
        return {"status": "unavailable"}

    def liveness(self) -> tuple[bool, str]:
        if not self._started:
            return True, "not_started"
        if not bool(getattr(self.agent, "_running", False)):
            return False, "agent_not_running"
        worker = getattr(self.agent, "_worker", None)
        if worker is not None and worker.done():
            return False, "worker_task_done"
        return True, "ok"


class DesktopAPIRuntime:
    def __init__(self, *, host: str, port: int) -> None:
        self.host = str(host or "127.0.0.1").strip() or "127.0.0.1"
        self.port = max(1, min(65535, int(port)))
        self.logger = Logger.get_logger("DesktopAPI")
        self.service: Optional[DesktopBackendService] = None
        self.server: Optional[JarvisHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._started = False
        self._state = "idle"
        self._last_error = ""
        self._started_at_monotonic = 0.0
        self._last_transition_at = _utc_now_iso()

    def _set_state(self, state: str, *, error: str = "") -> None:
        self._state = str(state or "unknown").strip().lower() or "unknown"
        self._last_transition_at = _utc_now_iso()
        if error:
            self._last_error = str(error).strip()

    def start(self, *, timeout_s: float = 45.0) -> None:
        with self._lock:
            if self._started:
                return
            self._set_state("starting")
        service: Optional[DesktopBackendService] = None
        server: Optional[JarvisHTTPServer] = None
        thread: Optional[threading.Thread] = None
        try:
            service = DesktopBackendService()
            service.start()
            server = JarvisHTTPServer((self.host, self.port), JarvisAPIHandler, service)
            thread = threading.Thread(
                target=server.serve_forever,
                kwargs={"poll_interval": 0.5},
                name="desktop-api-http",
                daemon=True,
            )
            thread.start()
            deadline = time.monotonic() + max(2.0, min(float(timeout_s), 180.0))
            while not thread.is_alive() and time.monotonic() < deadline:
                time.sleep(0.02)
            if not thread.is_alive():
                raise RuntimeError("desktop-api-http thread did not enter serving state")
        except Exception as exc:  # noqa: BLE001
            self._safe_failed_start_cleanup(server=server, service=service, thread=thread)
            message = f"Desktop API boot failed: {_safe_error(exc)}"
            with self._lock:
                self._started = False
                self.service = None
                self.server = None
                self._thread = None
                self._started_at_monotonic = 0.0
                self._set_state("failed", error=message)
            self.logger.error(message)
            raise

        with self._lock:
            self.service = service
            self.server = server
            self._thread = thread
            self._started = True
            self._started_at_monotonic = time.monotonic()
            self._set_state("online")
        self.logger.info(f"Desktop API listening on http://{self.host}:{self.port}")

    def _safe_failed_start_cleanup(
        self,
        *,
        server: Optional[JarvisHTTPServer],
        service: Optional[DesktopBackendService],
        thread: Optional[threading.Thread],
    ) -> None:
        try:
            if server is not None:
                server.shutdown()
        except Exception:
            pass
        try:
            if thread is not None and thread.is_alive():
                thread.join(timeout=2.0)
        except Exception:
            pass
        try:
            if server is not None:
                server.server_close()
        except Exception:
            pass
        try:
            if service is not None:
                service.stop()
        except Exception:
            pass

    def stop(self, *, reason: str = "shutdown", timeout_s: float = 20.0) -> None:
        with self._lock:
            if not self._started:
                self._set_state("offline")
                return
            self.logger.info(f"Stopping desktop API ({reason})...")
            self._set_state("stopping")
            server = self.server
            service = self.service
            thread = self._thread
            self._started = False
            self.server = None
            self.service = None
            self._thread = None
            self._started_at_monotonic = 0.0

        timeout = max(2.0, min(float(timeout_s), 180.0))
        stop_errors: list[str] = []

        try:
            if server is not None:
                server.shutdown()
        except Exception as exc:  # noqa: BLE001
            stop_errors.append(f"server.shutdown: {_safe_error(exc)}")
            self.logger.warning(f"Desktop API shutdown signal failed: {exc}")

        try:
            if thread is not None:
                thread.join(timeout=timeout)
                if thread.is_alive():
                    stop_errors.append("thread.join timeout")
        except Exception as exc:  # noqa: BLE001
            stop_errors.append(f"thread.join: {_safe_error(exc)}")
            self.logger.warning(f"Desktop API thread join failed: {exc}")

        try:
            if server is not None:
                server.server_close()
        except Exception as exc:  # noqa: BLE001
            stop_errors.append(f"server.server_close: {_safe_error(exc)}")
            self.logger.warning(f"Desktop API socket close failed: {exc}")

        try:
            if service is not None:
                service.stop()
        except Exception as exc:  # noqa: BLE001
            stop_errors.append(f"service.stop: {_safe_error(exc)}")
            self.logger.error(f"Desktop backend service stop failed: {exc}")

        if stop_errors:
            message = "; ".join(stop_errors)
            with self._lock:
                self._set_state("failed", error=message)
            self.logger.error(f"Desktop API shutdown completed with errors: {message}")
        else:
            with self._lock:
                self._set_state("offline")

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            thread = self._thread
            thread_alive = bool(thread is not None and thread.is_alive())
            started = bool(self._started)
            state = self._state
            last_error = self._last_error
            last_transition_at = self._last_transition_at
            uptime_s = 0.0
            if started and self._started_at_monotonic > 0:
                uptime_s = max(0.0, time.monotonic() - self._started_at_monotonic)
        return {
            "type": "desktop-api",
            "state": state,
            "started": started,
            "thread_alive": thread_alive,
            "host": self.host,
            "port": self.port,
            "uptime_s": round(uptime_s, 3),
            "last_error": last_error,
            "last_transition_at": last_transition_at,
        }

    def liveness(self) -> tuple[bool, str]:
        with self._lock:
            if not self._started:
                return True, "not_started"
            thread = self._thread
            if thread is None:
                return False, "http_thread_missing"
            if not thread.is_alive():
                return False, "http_thread_dead"
            if self.server is None:
                return False, "server_missing"
            if self.service is None:
                return False, "service_missing"
            return True, "ok"


class RuntimeLauncher:
    def __init__(
        self,
        *,
        mode: str,
        host: str,
        port: int,
        ready_file: str = "",
        startup_timeout_s: float = 90.0,
        shutdown_timeout_s: float = 45.0,
        health_interval_s: float = 2.0,
        max_health_failures: int = 3,
        fail_fast: bool = True,
        ready_write_interval_s: float = 2.0,
        auto_recover: bool = True,
        auto_recover_max_attempts: int = 3,
        auto_recover_cooldown_s: float = 30.0,
        auto_recover_startup_timeout_s: float = 45.0,
    ) -> None:
        self.mode = self._normalize_mode(mode)
        self.logger = Logger.get_logger("RuntimeLauncher")
        self.kernel_runtime = AIKernelRuntime() if self.mode in {"kernel", "both"} else None
        self.desktop_runtime = (
            DesktopAPIRuntime(host=host, port=port)
            if self.mode in {"desktop-api", "both"}
            else None
        )
        self.ready_file = str(ready_file or "").strip()
        self.startup_timeout_s = max(10.0, min(float(startup_timeout_s), 1200.0))
        self.shutdown_timeout_s = max(2.0, min(float(shutdown_timeout_s), 600.0))
        self.health_interval_s = max(0.5, min(float(health_interval_s), 120.0))
        self.max_health_failures = max(1, min(int(max_health_failures), 1000))
        self.fail_fast = bool(fail_fast)
        self.ready_write_interval_s = max(0.2, min(float(ready_write_interval_s), 120.0))
        self.auto_recover = bool(auto_recover)
        self.auto_recover_max_attempts = max(1, min(int(auto_recover_max_attempts), 100))
        self.auto_recover_cooldown_s = max(1.0, min(float(auto_recover_cooldown_s), 900.0))
        self.auto_recover_startup_timeout_s = max(5.0, min(float(auto_recover_startup_timeout_s), 300.0))
        self._shutdown_event = asyncio.Event()
        self._shutdown_lock = asyncio.Lock()
        self._started = False
        self._state = "idle"
        self._last_error = ""
        self._started_at_monotonic = 0.0
        self._last_transition_at = _utc_now_iso()
        self._monitor_task: Optional[asyncio.Task[None]] = None
        self._consecutive_health_failures = 0
        self._last_ready_write_monotonic = 0.0
        self._last_ready_status = ""
        self._health_journal: list[dict[str, Any]] = []
        self._recovery_attempts = 0
        self._recovery_successes = 0
        self._last_recovery_monotonic = 0.0
        self._last_recovery_at = ""
        self._last_recovery_error = ""
        self._last_recovery_issues: list[str] = []

    @staticmethod
    def _normalize_mode(mode: str) -> str:
        clean = str(mode or "").strip().lower()
        if clean in {"kernel", "desktop-api", "both"}:
            return clean
        return "kernel"

    def _set_state(self, state: str, *, error: str = "") -> None:
        self._state = str(state or "unknown").strip().lower() or "unknown"
        self._last_transition_at = _utc_now_iso()
        if error:
            self._last_error = str(error).strip()

    def _collect_runtime_status(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.kernel_runtime is not None:
            try:
                payload["kernel"] = self.kernel_runtime.snapshot()
            except Exception as exc:  # noqa: BLE001
                payload["kernel"] = {"state": "error", "message": _safe_error(exc)}
        if self.desktop_runtime is not None:
            try:
                payload["desktop_api"] = self.desktop_runtime.snapshot()
            except Exception as exc:  # noqa: BLE001
                payload["desktop_api"] = {"state": "error", "message": _safe_error(exc)}
        return payload

    def _runtime_signature(self, payload: dict[str, Any]) -> str:
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def _runtime_health_details(runtimes: dict[str, Any], *, failures: int, max_failures: int) -> dict[str, Any]:
        score = 1.0
        reasons: list[str] = []
        if max_failures > 0:
            failure_penalty = min(0.45, float(failures) / float(max_failures) * 0.45)
            score -= failure_penalty
            if failure_penalty > 0.08:
                reasons.append(f"supervisor_failures:{failures}/{max_failures}")
        for runtime_name, row in runtimes.items():
            if not isinstance(row, dict):
                continue
            state = str(row.get("state", "")).strip().lower()
            if state in {"failed", "error"}:
                score -= 0.35
                reasons.append(f"{runtime_name}:state={state}")
            elif state in {"degraded", "stopping", "starting"}:
                score -= 0.14
                reasons.append(f"{runtime_name}:state={state}")
            if row.get("type") == "desktop-api" and not bool(row.get("thread_alive", True)):
                score -= 0.18
                reasons.append(f"{runtime_name}:thread_not_alive")
            if row.get("type") == "kernel" and not bool(row.get("worker_alive", True)):
                score -= 0.18
                reasons.append(f"{runtime_name}:worker_not_alive")

            diagnostics = row.get("diagnostics")
            if isinstance(diagnostics, dict):
                readiness = diagnostics.get("readiness", {})
                readiness_score = 1.0
                if isinstance(readiness, dict):
                    try:
                        readiness_score = float(readiness.get("score", 1.0))
                    except Exception:
                        readiness_score = 1.0
                readiness_penalty = min(0.24, max(0.0, 1.0 - readiness_score) * 0.24)
                if readiness_penalty > 0.0:
                    score -= readiness_penalty
                if readiness_score < 0.62:
                    reasons.append(f"{runtime_name}:diagnostics_readiness={readiness_score:.3f}")
        compact_reasons = []
        for item in reasons:
            clean = str(item or "").strip()
            if clean and clean not in compact_reasons:
                compact_reasons.append(clean)
        return {"score": max(0.0, min(1.0, score)), "reasons": compact_reasons[:16]}

    @staticmethod
    def _runtime_health_score(runtimes: dict[str, Any], *, failures: int, max_failures: int) -> float:
        details = RuntimeLauncher._runtime_health_details(
            runtimes,
            failures=int(failures),
            max_failures=int(max_failures),
        )
        return float(details.get("score", 0.0))

    @staticmethod
    def _recovery_targets_from_issues(issues: list[str]) -> list[str]:
        targets: list[str] = []
        for issue in issues:
            clean = str(issue or "").strip().lower()
            if clean.startswith("kernel:"):
                if "kernel" not in targets:
                    targets.append("kernel")
                continue
            if clean.startswith("desktop-api:"):
                if "desktop-api" not in targets:
                    targets.append("desktop-api")
                continue
        return targets

    async def _attempt_runtime_recovery(self, *, issues: list[str]) -> dict[str, Any]:
        now = time.monotonic()
        if not self.auto_recover:
            return {"status": "skipped", "reason": "auto_recover_disabled"}
        if self._recovery_attempts >= self.auto_recover_max_attempts:
            return {
                "status": "skipped",
                "reason": "max_recovery_attempts_exhausted",
                "attempts": int(self._recovery_attempts),
                "max_attempts": int(self.auto_recover_max_attempts),
            }
        if (now - self._last_recovery_monotonic) < self.auto_recover_cooldown_s:
            return {
                "status": "skipped",
                "reason": "recovery_cooldown_active",
                "cooldown_remaining_s": round(
                    max(0.0, self.auto_recover_cooldown_s - (now - self._last_recovery_monotonic)),
                    3,
                ),
            }

        targets = self._recovery_targets_from_issues(issues)
        if not targets:
            if self.kernel_runtime is not None:
                alive, _ = self.kernel_runtime.liveness()
                if not alive:
                    targets.append("kernel")
            if self.desktop_runtime is not None:
                alive, _ = self.desktop_runtime.liveness()
                if not alive:
                    targets.append("desktop-api")
        if not targets:
            return {"status": "skipped", "reason": "no_recovery_targets"}

        self._recovery_attempts += 1
        self._last_recovery_monotonic = now
        self._last_recovery_at = _utc_now_iso()
        self._last_recovery_issues = [str(item).strip() for item in issues if str(item).strip()][:16]
        self._last_recovery_error = ""

        timeout_s = min(self.startup_timeout_s, self.auto_recover_startup_timeout_s)
        recover_errors: list[str] = []

        if "kernel" in targets and self.kernel_runtime is not None:
            try:
                await asyncio.wait_for(
                    self.kernel_runtime.shutdown(reason="health_recovery", timeout_s=self.shutdown_timeout_s),
                    timeout=self.shutdown_timeout_s + 5.0,
                )
            except Exception as exc:  # noqa: BLE001
                recover_errors.append(f"kernel.shutdown: {_safe_error(exc)}")
            try:
                await asyncio.wait_for(
                    self.kernel_runtime.boot(timeout_s=timeout_s),
                    timeout=timeout_s + 5.0,
                )
            except Exception as exc:  # noqa: BLE001
                recover_errors.append(f"kernel.boot: {_safe_error(exc)}")

        if "desktop-api" in targets and self.desktop_runtime is not None:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(
                        self.desktop_runtime.stop,
                        reason="health_recovery",
                        timeout_s=self.shutdown_timeout_s,
                    ),
                    timeout=self.shutdown_timeout_s + 5.0,
                )
            except Exception as exc:  # noqa: BLE001
                recover_errors.append(f"desktop.stop: {_safe_error(exc)}")
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(self.desktop_runtime.start, timeout_s=timeout_s),
                    timeout=timeout_s + 5.0,
                )
            except Exception as exc:  # noqa: BLE001
                recover_errors.append(f"desktop.start: {_safe_error(exc)}")

        if self.kernel_runtime is not None:
            alive, reason = self.kernel_runtime.liveness()
            if not alive:
                recover_errors.append(f"kernel.liveness:{reason}")
        if self.desktop_runtime is not None:
            alive, reason = self.desktop_runtime.liveness()
            if not alive:
                recover_errors.append(f"desktop-api.liveness:{reason}")

        if recover_errors:
            merged = "; ".join(recover_errors)
            self._last_recovery_error = merged
            return {
                "status": "failed",
                "attempt": int(self._recovery_attempts),
                "targets": targets,
                "issues": list(self._last_recovery_issues),
                "error": merged,
            }

        self._recovery_successes += 1
        return {
            "status": "recovered",
            "attempt": int(self._recovery_attempts),
            "targets": targets,
            "issues": list(self._last_recovery_issues),
        }

    async def _stop_runtimes(self, *, reason: str) -> list[str]:
        stop_errors: list[str] = []
        if self.desktop_runtime is not None:
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(
                        self.desktop_runtime.stop,
                        reason=reason,
                        timeout_s=self.shutdown_timeout_s,
                    ),
                    timeout=self.shutdown_timeout_s + 5.0,
                )
            except Exception as exc:  # noqa: BLE001
                stop_errors.append(f"desktop_runtime.stop: {_safe_error(exc)}")
        if self.kernel_runtime is not None:
            try:
                await asyncio.wait_for(
                    self.kernel_runtime.shutdown(reason=reason, timeout_s=self.shutdown_timeout_s),
                    timeout=self.shutdown_timeout_s + 5.0,
                )
            except Exception as exc:  # noqa: BLE001
                stop_errors.append(f"kernel_runtime.shutdown: {_safe_error(exc)}")
        return stop_errors

    async def _stop_health_supervisor(self) -> None:
        monitor_task = self._monitor_task
        if monitor_task is None:
            return
        if monitor_task is asyncio.current_task():
            return
        if monitor_task.done():
            self._monitor_task = None
            return
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(f"Health supervisor stop failed: {exc}")
        self._monitor_task = None

    async def _health_supervisor_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.health_interval_s)
            if self._shutdown_event.is_set():
                break
            issues: list[str] = []
            if self.kernel_runtime is not None:
                alive, reason = self.kernel_runtime.liveness()
                if not alive:
                    issues.append(f"kernel:{reason}")
            if self.desktop_runtime is not None:
                alive, reason = self.desktop_runtime.liveness()
                if not alive:
                    issues.append(f"desktop-api:{reason}")

            if issues:
                self._consecutive_health_failures += 1
                issue_text = "; ".join(issues)
                self._set_state("degraded", error=issue_text)
                self.logger.warning(
                    f"Runtime health degraded ({self._consecutive_health_failures}/{self.max_health_failures}): {issue_text}"
                )
                if self._consecutive_health_failures >= self.max_health_failures:
                    recovery = await self._attempt_runtime_recovery(issues=issues)
                    if str(recovery.get("status", "")).strip().lower() == "recovered":
                        self.logger.warning(
                            "Runtime health auto-recovery succeeded; resetting failure streak."
                        )
                        self._consecutive_health_failures = 0
                        self._set_state("online")
                        self._write_ready_file(status="online", force=True)
                        continue
                    self._set_state("failed", error=issue_text)
                    self._write_ready_file(status="failed", force=True)
                    recovery_state = str(recovery.get("status", "")).strip().lower()
                    recovery_reason = str(
                        recovery.get("error", recovery.get("reason", ""))
                    ).strip()
                    if recovery_state in {"failed", "skipped"} and recovery_reason:
                        self.logger.error(
                            f"Runtime health failed permanently: {issue_text} (recovery={recovery_state}:{recovery_reason})"
                        )
                    else:
                        self.logger.error(f"Runtime health failed permanently: {issue_text}")
                    if self.fail_fast:
                        asyncio.create_task(self.shutdown(reason="health_fail_fast"))
                        return
                else:
                    self._write_ready_file(status="degraded")
                continue

            if self._consecutive_health_failures > 0:
                self.logger.info("Runtime health recovered.")
            self._consecutive_health_failures = 0
            if self._state != "online":
                self._set_state("online")
            self._write_ready_file(status="online")

    async def start(self) -> None:
        if self._started:
            await self._shutdown_event.wait()
            return
        global_exception_handler(self.logger)
        self.logger.info(f"Starting runtime launcher in mode='{self.mode}'...")
        self._set_state("initializing")

        try:
            if self.kernel_runtime is not None:
                await asyncio.wait_for(
                    self.kernel_runtime.boot(timeout_s=self.startup_timeout_s),
                    timeout=self.startup_timeout_s + 5.0,
                )
            if self.desktop_runtime is not None:
                await asyncio.wait_for(
                    asyncio.to_thread(self.desktop_runtime.start, timeout_s=self.startup_timeout_s),
                    timeout=self.startup_timeout_s + 5.0,
                )
            self._started = True
            self._started_at_monotonic = time.monotonic()
            self._set_state("online")
            self._consecutive_health_failures = 0
            self._write_ready_file(status="online", force=True)
            self._monitor_task = asyncio.create_task(
                self._health_supervisor_loop(),
                name="runtime-health-supervisor",
            )
            self.logger.info("Runtime launcher is online.")
            await self._shutdown_event.wait()
        except Exception as exc:  # noqa: BLE001
            startup_message = f"Runtime startup failed: {_safe_error(exc)}"
            self._set_state("failed", error=startup_message)
            self.logger.error(startup_message)
            stop_errors = await self._stop_runtimes(reason="startup_failure")
            if stop_errors:
                merged = f"{startup_message}; {'; '.join(stop_errors)}"
                self._set_state("failed", error=merged)
            self._write_ready_file(status="failed", force=True)
            self._shutdown_event.set()
            raise

    async def shutdown(self, *, reason: str = "shutdown") -> None:
        async with self._shutdown_lock:
            if self._shutdown_event.is_set():
                return
            self.logger.info(f"Stopping runtime launcher ({reason})...")
            self._set_state("stopping")
            self._write_ready_file(status="stopping", force=True)
            await self._stop_health_supervisor()
            stop_errors = await self._stop_runtimes(reason=reason)
            self._started = False
            self._started_at_monotonic = 0.0
            self._shutdown_event.set()
            if stop_errors:
                merged = "; ".join(stop_errors)
                self._set_state("failed", error=merged)
                self._write_ready_file(status="failed", force=True)
                self.logger.error(f"Runtime launcher stopped with errors: {merged}")
            else:
                self._set_state("offline")
                self._write_ready_file(status="offline", force=True)
                self.logger.info("Runtime launcher shutdown complete.")

    def _write_ready_file(self, *, status: str, force: bool = False) -> None:
        path_text = str(self.ready_file or "").strip()
        if not path_text:
            return
        clean_status = str(status or "").strip().lower() or self._state
        now_mono = time.monotonic()
        should_skip = (
            not force
            and clean_status == self._last_ready_status
            and (now_mono - self._last_ready_write_monotonic) < self.ready_write_interval_s
        )
        if should_skip:
            return
        path = Path(path_text)
        try:
            if clean_status == "offline":
                if path.exists():
                    path.unlink()
                self._last_ready_status = clean_status
                self._last_ready_write_monotonic = now_mono
                return
            uptime_s = 0.0
            if self._started and self._started_at_monotonic > 0:
                uptime_s = max(0.0, time.monotonic() - self._started_at_monotonic)
            payload = {
                "status": clean_status,
                "mode": self.mode,
                "pid": os.getpid(),
                "state": self._state,
                "last_error": self._last_error,
                "last_transition_at": self._last_transition_at,
                "updated_at": _utc_now_iso(),
                "uptime_s": round(uptime_s, 3),
                "health": {
                    "consecutive_failures": int(self._consecutive_health_failures),
                    "max_failures": int(self.max_health_failures),
                    "interval_s": float(self.health_interval_s),
                    "fail_fast": bool(self.fail_fast),
                    "recovery": {
                        "enabled": bool(self.auto_recover),
                        "attempts": int(self._recovery_attempts),
                        "successes": int(self._recovery_successes),
                        "max_attempts": int(self.auto_recover_max_attempts),
                        "cooldown_s": float(self.auto_recover_cooldown_s),
                        "last_at": str(self._last_recovery_at),
                        "last_error": str(self._last_recovery_error),
                        "last_issues": list(self._last_recovery_issues[:16]),
                    },
                },
                "runtimes": self._collect_runtime_status(),
            }
            runtime_rows = payload.get("runtimes", {}) if isinstance(payload.get("runtimes", {}), dict) else {}
            health_details = self._runtime_health_details(
                runtime_rows,
                failures=int(self._consecutive_health_failures),
                max_failures=int(self.max_health_failures),
            )
            health_score = float(health_details.get("score", 0.0))
            payload["health"]["score"] = round(health_score, 6)
            payload["health"]["level"] = "healthy" if health_score >= 0.75 else ("degraded" if health_score >= 0.45 else "critical")
            payload["health"]["reasons"] = list(health_details.get("reasons", []))
            payload["signature"] = self._runtime_signature(payload)

            journal_row = {
                "at": payload["updated_at"],
                "status": clean_status,
                "state": self._state,
                "health_score": payload["health"]["score"],
                "signature": payload["signature"][:16],
            }
            self._health_journal.append(journal_row)
            if len(self._health_journal) > 180:
                self._health_journal = self._health_journal[-180:]
            payload["health"]["journal_tail"] = self._health_journal[-24:]
            path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = path.with_name(f".{path.name}.tmp")
            temp_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
            temp_path.replace(path)
            self._last_ready_status = clean_status
            self._last_ready_write_monotonic = now_mono
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(f"Unable to update ready file '{path_text}': {exc}")


def _safe_error(exc: BaseException) -> str:
    text = str(exc).strip()
    return text or exc.__class__.__name__


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = str(os.getenv(name, "")).strip()
    try:
        value = int(raw) if raw else int(default)
    except Exception:
        value = int(default)
    return max(minimum, min(value, maximum))


def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = str(os.getenv(name, "")).strip()
    try:
        value = float(raw) if raw else float(default)
    except Exception:
        value = float(default)
    return max(minimum, min(value, maximum))


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    port_default = _env_int("JARVIS_DESKTOP_API_PORT", 8765, minimum=1, maximum=65535)
    startup_timeout_default = _env_float("JARVIS_RUNTIME_STARTUP_TIMEOUT_S", 90.0, minimum=10.0, maximum=1200.0)
    shutdown_timeout_default = _env_float("JARVIS_RUNTIME_SHUTDOWN_TIMEOUT_S", 45.0, minimum=2.0, maximum=600.0)
    health_interval_default = _env_float("JARVIS_RUNTIME_HEALTH_INTERVAL_S", 2.0, minimum=0.5, maximum=120.0)
    max_health_failures_default = _env_int("JARVIS_RUNTIME_MAX_HEALTH_FAILURES", 3, minimum=1, maximum=1000)
    fail_fast_default = _env_bool("JARVIS_RUNTIME_FAIL_FAST", True)
    auto_recover_default = _env_bool("JARVIS_RUNTIME_AUTO_RECOVER", True)
    auto_recover_max_attempts_default = _env_int("JARVIS_RUNTIME_AUTO_RECOVER_MAX_ATTEMPTS", 3, minimum=1, maximum=100)
    auto_recover_cooldown_default = _env_float("JARVIS_RUNTIME_AUTO_RECOVER_COOLDOWN_S", 30.0, minimum=1.0, maximum=900.0)
    auto_recover_startup_timeout_default = _env_float(
        "JARVIS_RUNTIME_AUTO_RECOVER_STARTUP_TIMEOUT_S",
        45.0,
        minimum=5.0,
        maximum=300.0,
    )
    ready_write_interval_default = _env_float(
        "JARVIS_RUNTIME_READY_WRITE_INTERVAL_S",
        2.0,
        minimum=0.2,
        maximum=120.0,
    )

    parser = argparse.ArgumentParser(description="Run JARVIS kernel and/or desktop API runtime.")
    parser.add_argument(
        "--mode",
        default=os.getenv("JARVIS_RUNTIME_MODE", "kernel"),
        choices=["kernel", "desktop-api", "both"],
        help="kernel=agent kernel only, desktop-api=HTTP API only, both=run both runtimes.",
    )
    parser.add_argument("--host", default=os.getenv("JARVIS_DESKTOP_API_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=port_default)
    parser.add_argument("--startup-timeout-s", type=float, default=startup_timeout_default)
    parser.add_argument("--shutdown-timeout-s", type=float, default=shutdown_timeout_default)
    parser.add_argument("--health-interval-s", type=float, default=health_interval_default)
    parser.add_argument("--max-health-failures", type=int, default=max_health_failures_default)
    parser.add_argument("--ready-write-interval-s", type=float, default=ready_write_interval_default)
    parser.add_argument("--fail-fast", dest="fail_fast", action="store_true")
    parser.add_argument("--no-fail-fast", dest="fail_fast", action="store_false")
    parser.set_defaults(fail_fast=fail_fast_default)
    parser.add_argument("--auto-recover", dest="auto_recover", action="store_true")
    parser.add_argument("--no-auto-recover", dest="auto_recover", action="store_false")
    parser.set_defaults(auto_recover=auto_recover_default)
    parser.add_argument("--auto-recover-max-attempts", type=int, default=auto_recover_max_attempts_default)
    parser.add_argument("--auto-recover-cooldown-s", type=float, default=auto_recover_cooldown_default)
    parser.add_argument(
        "--auto-recover-startup-timeout-s",
        type=float,
        default=auto_recover_startup_timeout_default,
    )
    parser.add_argument(
        "--ready-file",
        default=os.getenv("JARVIS_RUNTIME_READY_FILE", ""),
        help="Optional JSON status file path for process supervisors.",
    )
    return parser.parse_args(argv)


async def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    launcher = RuntimeLauncher(
        mode=args.mode,
        host=args.host,
        port=int(args.port),
        ready_file=args.ready_file,
        startup_timeout_s=float(args.startup_timeout_s),
        shutdown_timeout_s=float(args.shutdown_timeout_s),
        health_interval_s=float(args.health_interval_s),
        max_health_failures=int(args.max_health_failures),
        fail_fast=bool(args.fail_fast),
        ready_write_interval_s=float(args.ready_write_interval_s),
        auto_recover=bool(args.auto_recover),
        auto_recover_max_attempts=int(args.auto_recover_max_attempts),
        auto_recover_cooldown_s=float(args.auto_recover_cooldown_s),
        auto_recover_startup_timeout_s=float(args.auto_recover_startup_timeout_s),
    )
    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        asyncio.create_task(launcher.shutdown(reason="signal"))

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except (NotImplementedError, RuntimeError, ValueError):
            # Some Windows event loop policies do not support signal handlers.
            try:
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(_request_shutdown))
            except Exception:
                pass

    try:
        await launcher.start()
    finally:
        await launcher.shutdown(reason="main_exit")


if __name__ == "__main__":
    asyncio.run(main())
