from __future__ import annotations

import time

from backend.python.core.desktop_recovery_supervisor import DesktopRecoverySupervisor


def test_desktop_recovery_supervisor_persists_configuration(tmp_path) -> None:
    state_path = tmp_path / "desktop_recovery_supervisor.json"
    supervisor = DesktopRecoverySupervisor(state_path=str(state_path), enabled=False, interval_s=12.0)
    status = supervisor.configure(
        enabled=True,
        interval_s=30.0,
        limit=9,
        max_auto_resumes=3,
        policy_profile="power",
        mission_status="paused",
        mission_kind="wizard",
        app_name="installer",
        stop_reason_code="elevation_consent_required",
        resume_force=True,
        source="test",
    )
    assert status["enabled"] is True
    assert status["interval_s"] == 30.0
    assert status["limit"] == 9
    assert status["max_auto_resumes"] == 3
    assert status["policy_profile"] == "power"
    assert status["allow_high_risk"] is True
    assert status["allow_critical_risk"] is True
    assert status["mission_kind"] == "wizard"
    assert status["app_name"] == "installer"
    assert status["resume_force"] is True

    reloaded = DesktopRecoverySupervisor(state_path=str(state_path))
    reloaded_status = reloaded.status()
    assert reloaded_status["enabled"] is True
    assert reloaded_status["interval_s"] == 30.0
    assert reloaded_status["limit"] == 9
    assert reloaded_status["max_auto_resumes"] == 3
    assert reloaded_status["policy_profile"] == "power"
    assert reloaded_status["allow_high_risk"] is True
    assert reloaded_status["allow_critical_risk"] is True
    assert reloaded_status["mission_kind"] == "wizard"
    assert reloaded_status["app_name"] == "installer"
    assert reloaded_status["resume_force"] is True


def test_desktop_recovery_supervisor_custom_policy_overrides_persist(tmp_path) -> None:
    state_path = tmp_path / "desktop_recovery_supervisor.json"
    supervisor = DesktopRecoverySupervisor(state_path=str(state_path), enabled=False, interval_s=12.0)
    status = supervisor.configure(
        policy_profile="balanced",
        allow_high_risk=False,
        allow_critical_risk=False,
        allow_admin_clearance=False,
        allow_destructive=True,
        source="test",
    )

    assert status["policy_profile"] == "custom"
    assert status["allow_high_risk"] is False
    assert status["allow_destructive"] is True

    reloaded = DesktopRecoverySupervisor(state_path=str(state_path))
    reloaded_status = reloaded.status()
    assert reloaded_status["policy_profile"] == "custom"
    assert reloaded_status["allow_high_risk"] is False
    assert reloaded_status["allow_destructive"] is True


def test_desktop_recovery_supervisor_manual_trigger_updates_runtime(tmp_path) -> None:
    state_path = tmp_path / "desktop_recovery_supervisor.json"
    calls: list[dict] = []

    def _callback(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return {
            "status": "success",
            "message": "desktop tick complete",
            "evaluated_count": 2,
            "auto_resume_attempted_count": 1,
            "auto_resume_triggered_count": 1,
            "resume_ready_count": 1,
            "manual_attention_count": 0,
            "blocked_count": 0,
            "idle_count": 1,
            "error_count": 0,
            "stop_reason": "auto_resume_triggered",
        }

    supervisor = DesktopRecoverySupervisor(state_path=str(state_path), enabled=False, interval_s=8.0)
    supervisor.start(_callback)
    try:
        payload = supervisor.trigger_now(
            source="manual_test",
            limit=5,
            max_auto_resumes=1,
            mission_status="paused",
            mission_kind="wizard",
        )
        assert payload["status"] == "success"
        assert calls[0]["limit"] == 5
        assert calls[0]["max_auto_resumes"] == 1
        assert calls[0]["mission_kind"] == "wizard"
        status = supervisor.status()
        assert status["run_count"] == 1
        assert status["manual_trigger_count"] == 1
        assert status["last_result_status"] == "success"
        assert status["last_summary"]["auto_resume_triggered_count"] == 1
    finally:
        supervisor.stop()


def test_desktop_recovery_supervisor_trigger_profile_applies_defaults(tmp_path) -> None:
    state_path = tmp_path / "desktop_recovery_supervisor.json"
    calls: list[dict] = []

    def _callback(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return {
            "status": "blocked",
            "message": "desktop tick deferred by policy",
            "evaluated_count": 1,
            "auto_resume_attempted_count": 0,
            "auto_resume_triggered_count": 0,
            "resume_ready_count": 1,
            "manual_attention_count": 0,
            "blocked_count": 1,
            "policy_blocked_count": 1,
            "idle_count": 0,
            "error_count": 0,
            "stop_reason": "policy_blocked",
        }

    supervisor = DesktopRecoverySupervisor(state_path=str(state_path), enabled=False, interval_s=8.0)
    supervisor.start(_callback)
    try:
        payload = supervisor.trigger_now(
            source="manual_test",
            limit=4,
            max_auto_resumes=1,
            policy_profile="power",
            mission_status="paused",
            mission_kind="wizard",
        )
        assert payload["status"] == "blocked"
        assert calls[0]["policy_profile"] == "power"
        assert calls[0]["allow_high_risk"] is True
        assert calls[0]["allow_critical_risk"] is True
        assert calls[0]["allow_admin_clearance"] is False
        assert calls[0]["allow_destructive"] is False
        status = supervisor.status()
        assert status["last_summary"]["policy_blocked_count"] == 1
    finally:
        supervisor.stop()


def test_desktop_recovery_supervisor_daemon_runs_when_enabled(tmp_path) -> None:
    state_path = tmp_path / "desktop_recovery_supervisor.json"
    calls: list[dict] = []

    def _callback(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return {"status": "ready", "message": "waiting on resume-safe missions", "resume_ready_count": 1}

    supervisor = DesktopRecoverySupervisor(state_path=str(state_path), enabled=True, interval_s=5.0)
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
        assert status["last_result_status"] == "ready"
    finally:
        supervisor.stop()
