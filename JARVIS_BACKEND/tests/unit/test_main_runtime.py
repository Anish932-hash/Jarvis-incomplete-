from __future__ import annotations

import json
import asyncio

from backend.python.main import RuntimeLauncher, parse_args


def test_parse_args_accepts_runtime_mode_and_desktop_host_port() -> None:
    args = parse_args(
        [
            "--mode",
            "desktop-api",
            "--host",
            "0.0.0.0",
            "--port",
            "9988",
            "--ready-file",
            "data/runtime_ready.json",
        ]
    )

    assert str(args.mode) == "desktop-api"
    assert str(args.host) == "0.0.0.0"
    assert int(args.port) == 9988
    assert str(args.ready_file).endswith("runtime_ready.json")


def test_parse_args_accepts_runtime_supervision_controls() -> None:
    args = parse_args(
        [
            "--mode",
            "both",
            "--startup-timeout-s",
            "120",
            "--shutdown-timeout-s",
            "30",
            "--health-interval-s",
            "1.5",
            "--max-health-failures",
            "5",
            "--ready-write-interval-s",
            "3",
            "--no-fail-fast",
        ]
    )

    assert str(args.mode) == "both"
    assert float(args.startup_timeout_s) == 120.0
    assert float(args.shutdown_timeout_s) == 30.0
    assert float(args.health_interval_s) == 1.5
    assert int(args.max_health_failures) == 5
    assert float(args.ready_write_interval_s) == 3.0
    assert bool(args.fail_fast) is False


def test_parse_args_accepts_runtime_auto_recover_controls() -> None:
    args = parse_args(
        [
            "--mode",
            "desktop-api",
            "--auto-recover",
            "--auto-recover-max-attempts",
            "7",
            "--auto-recover-cooldown-s",
            "22",
            "--auto-recover-startup-timeout-s",
            "35",
        ]
    )

    assert bool(args.auto_recover) is True
    assert int(args.auto_recover_max_attempts) == 7
    assert float(args.auto_recover_cooldown_s) == 22.0
    assert float(args.auto_recover_startup_timeout_s) == 35.0


def test_runtime_launcher_ready_file_lifecycle(tmp_path) -> None:
    ready_path = tmp_path / "runtime_ready.json"
    launcher = RuntimeLauncher(
        mode="desktop-api",
        host="127.0.0.1",
        port=8765,
        ready_file=str(ready_path),
    )

    launcher._write_ready_file(status="online")  # noqa: SLF001
    assert ready_path.exists()
    payload = json.loads(ready_path.read_text(encoding="utf-8"))
    assert payload["status"] == "online"
    assert payload["mode"] == "desktop-api"
    assert isinstance(payload.get("health"), dict)
    assert isinstance(payload.get("health", {}).get("reasons"), list)
    assert isinstance(payload.get("runtimes"), dict)

    launcher._write_ready_file(status="offline")  # noqa: SLF001
    assert not ready_path.exists()


def test_runtime_health_details_include_kernel_diagnostic_reasons() -> None:
    details = RuntimeLauncher._runtime_health_details(  # noqa: SLF001
        {
            "kernel": {
                "type": "kernel",
                "state": "online",
                "running": True,
                "worker_alive": True,
                "diagnostics": {"readiness": {"score": 0.4}},
            }
        },
        failures=0,
        max_failures=3,
    )
    assert 0.0 <= float(details.get("score", 0.0)) < 1.0
    reasons = details.get("reasons", [])
    assert isinstance(reasons, list)
    assert any("diagnostics_readiness" in str(item) for item in reasons)


def test_runtime_auto_recovery_restarts_failed_desktop_runtime() -> None:
    class _DesktopRuntimeStub:
        def __init__(self) -> None:
            self.started = False
            self.start_calls = 0
            self.stop_calls = 0

        def liveness(self) -> tuple[bool, str]:
            if self.started:
                return (True, "ok")
            return (False, "http_thread_dead")

        def stop(self, *, reason: str = "shutdown", timeout_s: float = 20.0) -> None:
            _ = (reason, timeout_s)
            self.stop_calls += 1
            self.started = False

        def start(self, *, timeout_s: float = 45.0) -> None:
            _ = timeout_s
            self.start_calls += 1
            self.started = True

    launcher = RuntimeLauncher(
        mode="desktop-api",
        host="127.0.0.1",
        port=8765,
        auto_recover=True,
        auto_recover_max_attempts=3,
        auto_recover_cooldown_s=0.1,
    )
    runtime = _DesktopRuntimeStub()
    launcher.desktop_runtime = runtime

    result = asyncio.run(
        launcher._attempt_runtime_recovery(issues=["desktop-api:http_thread_dead"])  # noqa: SLF001
    )

    assert str(result.get("status", "")) == "recovered"
    assert runtime.stop_calls >= 1
    assert runtime.start_calls >= 1
    assert launcher._recovery_attempts == 1  # noqa: SLF001
    assert launcher._recovery_successes == 1  # noqa: SLF001


def test_runtime_auto_recovery_honors_cooldown_between_attempts() -> None:
    class _DesktopRuntimeStub:
        def __init__(self) -> None:
            self.started = False

        def liveness(self) -> tuple[bool, str]:
            if self.started:
                return (True, "ok")
            return (False, "http_thread_dead")

        def stop(self, *, reason: str = "shutdown", timeout_s: float = 20.0) -> None:
            _ = (reason, timeout_s)
            self.started = False

        def start(self, *, timeout_s: float = 45.0) -> None:
            _ = timeout_s
            self.started = True

    launcher = RuntimeLauncher(
        mode="desktop-api",
        host="127.0.0.1",
        port=8765,
        auto_recover=True,
        auto_recover_max_attempts=4,
        auto_recover_cooldown_s=30.0,
    )
    launcher.desktop_runtime = _DesktopRuntimeStub()

    first = asyncio.run(
        launcher._attempt_runtime_recovery(issues=["desktop-api:http_thread_dead"])  # noqa: SLF001
    )
    second = asyncio.run(
        launcher._attempt_runtime_recovery(issues=["desktop-api:http_thread_dead"])  # noqa: SLF001
    )

    assert str(first.get("status", "")) == "recovered"
    assert str(second.get("status", "")) == "skipped"
    assert str(second.get("reason", "")) == "recovery_cooldown_active"
