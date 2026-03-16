from __future__ import annotations

import time

from backend.python.inference.model_setup_watchdog_supervisor import ModelSetupRecoveryWatchdogSupervisor


def test_watchdog_supervisor_persists_configuration(tmp_path) -> None:
    state_path = tmp_path / "watchdog_supervisor.json"
    supervisor = ModelSetupRecoveryWatchdogSupervisor(state_path=str(state_path), enabled=False, interval_s=12.0)
    status = supervisor.configure(
        enabled=True,
        interval_s=30.0,
        max_missions=7,
        max_auto_resumes=3,
        continue_followup_actions=False,
        max_followup_waves=2,
        current_scope=False,
        manifest_path="E:/Scope/Models to Download.txt",
        workspace_root="E:/Scope",
        source="test",
    )
    assert status["enabled"] is True
    assert status["interval_s"] == 30.0
    assert status["max_missions"] == 7
    assert status["max_auto_resumes"] == 3
    assert status["continue_followup_actions"] is False

    reloaded = ModelSetupRecoveryWatchdogSupervisor(state_path=str(state_path))
    reloaded_status = reloaded.status()
    assert reloaded_status["enabled"] is True
    assert reloaded_status["interval_s"] == 30.0
    assert reloaded_status["manifest_path"] == "E:/Scope/Models to Download.txt"
    assert reloaded_status["workspace_root"] == "E:/Scope"


def test_watchdog_supervisor_manual_trigger_updates_runtime(tmp_path) -> None:
    state_path = tmp_path / "watchdog_supervisor.json"
    calls: list[dict] = []

    def _callback(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return {
            "status": "success",
            "message": "tick complete",
            "auto_resume_triggered_count": 1,
            "watch_count": 0,
            "stalled_count": 0,
            "blocked_count": 0,
            "idle_count": 1,
            "complete_count": 0,
            "error_count": 0,
            "stop_reason": "auto_resume_triggered",
        }

    supervisor = ModelSetupRecoveryWatchdogSupervisor(state_path=str(state_path), enabled=False, interval_s=8.0)
    supervisor.start(_callback)
    try:
        payload = supervisor.trigger_now(source="manual_test", max_missions=5, max_auto_resumes=1)
        assert payload["status"] == "success"
        assert calls[0]["max_missions"] == 5
        assert calls[0]["max_auto_resumes"] == 1
        status = supervisor.status()
        assert status["run_count"] == 1
        assert status["manual_trigger_count"] == 1
        assert status["last_result_status"] == "success"
        assert status["last_summary"]["auto_resume_triggered_count"] == 1
    finally:
        supervisor.stop()


def test_watchdog_supervisor_daemon_runs_when_enabled(tmp_path) -> None:
    state_path = tmp_path / "watchdog_supervisor.json"
    calls: list[dict] = []

    def _callback(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return {"status": "watch", "message": "watching", "watch_count": 1}

    supervisor = ModelSetupRecoveryWatchdogSupervisor(state_path=str(state_path), enabled=True, interval_s=5.0)
    supervisor.start(_callback)
    try:
        supervisor.configure(enabled=True, interval_s=5.0)
        deadline = time.time() + 6.5
        while time.time() < deadline:
            if len(calls) >= 1:
                break
            time.sleep(0.15)
        assert len(calls) >= 1
        status = supervisor.status()
        assert status["auto_trigger_count"] >= 1
        assert status["last_result_status"] == "watch"
    finally:
        supervisor.stop()
