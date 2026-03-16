from __future__ import annotations

from backend.python.core.desktop_recovery_watchdog_memory import DesktopRecoveryWatchdogMemory


def test_desktop_recovery_watchdog_memory_records_and_summarizes_runs(tmp_path) -> None:
    memory = DesktopRecoveryWatchdogMemory(
        state_path=str(tmp_path / "desktop_watchdog_runs.json"),
        keep_runs=16,
    )

    recorded = memory.record(
        watchdog_payload={
            "status": "success",
            "message": "Desktop recovery daemon resumed 1 paused mission.",
            "limit": 8,
            "max_auto_resumes": 2,
            "mission_status": "paused",
            "mission_kind": "wizard",
            "app_name": "installer",
            "stop_reason_code": "elevation_consent_required",
            "resume_force": False,
            "evaluated_count": 3,
            "auto_resume_attempted_count": 1,
            "auto_resume_triggered_count": 1,
            "resume_ready_count": 1,
            "manual_attention_count": 1,
            "blocked_count": 1,
            "idle_count": 1,
            "error_count": 0,
            "triggered_mission_ids": ["dm_ready_1"],
            "ready_mission_ids": ["dm_ready_2"],
            "blocked_mission_ids": ["dm_blocked_1"],
            "filters": {
                "status": "paused",
                "mission_kind": "wizard",
                "app_name": "installer",
                "stop_reason_code": "elevation_consent_required",
            },
            "trigger_source": "manual_api",
        },
        source="manual_api",
    )

    run = recorded["run"]
    assert run["status"] == "success"
    assert run["source"] == "manual_api"
    assert run["trigger_source"] == "manual_api"
    assert run["mission_kind"] == "wizard"
    assert run["app_name"] == "installer"
    assert run["auto_resume_triggered_count"] == 1
    assert run["blocked_count"] == 1

    snapshot = memory.snapshot(limit=8)

    assert snapshot["status"] == "success"
    assert snapshot["count"] == 1
    assert snapshot["triggered_run_count"] == 1
    assert snapshot["blocked_run_count"] == 1
    assert snapshot["error_run_count"] == 0
    assert snapshot["latest_run"]["run_id"] == run["run_id"]
    assert snapshot["latest_triggered_run"]["run_id"] == run["run_id"]
    assert snapshot["latest_blocked_run"]["run_id"] == run["run_id"]


def test_desktop_recovery_watchdog_memory_resets_by_app_and_kind(tmp_path) -> None:
    memory = DesktopRecoveryWatchdogMemory(
        state_path=str(tmp_path / "desktop_watchdog_runs.json"),
        keep_runs=16,
    )

    memory.record(
        watchdog_payload={
            "status": "blocked",
            "message": "Waiting on admin review.",
            "filters": {
                "status": "paused",
                "mission_kind": "wizard",
                "app_name": "installer",
            },
            "blocked_count": 1,
        },
        source="daemon",
    )
    memory.record(
        watchdog_payload={
            "status": "idle",
            "message": "No resumable missions.",
            "filters": {
                "status": "paused",
                "mission_kind": "form",
                "app_name": "settings",
            },
        },
        source="daemon",
    )

    reset_payload = memory.reset(app_name="installer", mission_kind="wizard")
    assert reset_payload["status"] == "success"
    assert reset_payload["removed"] == 1

    snapshot = memory.snapshot(limit=8)
    assert snapshot["count"] == 1
    assert snapshot["latest_run"]["app_name"] == "settings"
