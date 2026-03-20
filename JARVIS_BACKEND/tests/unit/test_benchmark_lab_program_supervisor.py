from __future__ import annotations

import time

from backend.python.evaluation.benchmark_lab_program_supervisor import DesktopBenchmarkLabProgramSupervisor


def test_benchmark_program_supervisor_persists_configuration(tmp_path) -> None:
    state_path = tmp_path / "benchmark_program_supervisor.json"
    supervisor = DesktopBenchmarkLabProgramSupervisor(state_path=str(state_path), enabled=False, interval_s=30.0)
    status = supervisor.configure(
        enabled=True,
        interval_s=300.0,
        max_programs=4,
        max_campaigns_per_program=5,
        max_sweeps_per_campaign=3,
        max_sessions=3,
        max_replays_per_session=2,
        history_limit=10,
        program_status="ready",
        pack="long_horizon_and_replay",
        app_name="settings",
        source="test",
    )
    assert status["enabled"] is True
    assert status["interval_s"] == 300.0
    assert status["max_programs"] == 4
    assert status["max_campaigns_per_program"] == 5
    assert status["program_status"] == "ready"

    reloaded = DesktopBenchmarkLabProgramSupervisor(state_path=str(state_path))
    reloaded_status = reloaded.status()
    assert reloaded_status["enabled"] is True
    assert reloaded_status["interval_s"] == 300.0
    assert reloaded_status["max_programs"] == 4
    assert reloaded_status["max_campaigns_per_program"] == 5
    assert reloaded_status["pack"] == "long_horizon_and_replay"
    assert reloaded_status["app_name"] == "settings"


def test_benchmark_program_supervisor_manual_trigger_updates_runtime(tmp_path) -> None:
    state_path = tmp_path / "benchmark_program_supervisor.json"
    calls: list[dict] = []

    def _callback(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return {
            "status": "success",
            "message": "program watchdog executed 2 program(s)",
            "targeted_program_count": 2,
            "executed_program_count": 2,
            "executed_campaign_count": 4,
            "executed_sweep_count": 7,
            "stable_program_count": 1,
            "regression_program_count": 1,
            "pending_campaign_count": 2,
            "attention_campaign_count": 1,
            "pending_session_count": 3,
            "pending_app_target_count": 2,
            "long_horizon_pending_count": 4,
            "error_count": 0,
            "latest_program_label": "settings replay program",
            "auto_created_program_count": 1,
            "cycle_stop_reason_counts": {"stable": 1, "max_campaigns_reached": 1},
            "trend_direction_counts": {"improving": 1, "regressing": 1},
        }

    supervisor = DesktopBenchmarkLabProgramSupervisor(state_path=str(state_path), enabled=False, interval_s=30.0)
    supervisor.start(_callback)
    try:
        payload = supervisor.trigger_now(
            source="manual_test",
            max_programs=3,
            max_campaigns_per_program=4,
            max_sweeps_per_campaign=3,
            max_sessions=2,
            max_replays_per_session=2,
            history_limit=6,
            pack="long_horizon_and_replay",
            app_name="settings",
        )
        assert payload["status"] == "success"
        assert calls[0]["max_programs"] == 3
        assert calls[0]["max_campaigns_per_program"] == 4
        status = supervisor.status()
        assert status["run_count"] == 1
        assert status["manual_trigger_count"] == 1
        assert status["last_result_status"] == "success"
        assert status["last_summary"]["executed_program_count"] == 2
        assert status["last_summary"]["executed_campaign_count"] == 4
        assert status["last_summary"]["auto_created_program_count"] == 1
    finally:
        supervisor.stop()


def test_benchmark_program_supervisor_daemon_runs_when_enabled(tmp_path) -> None:
    state_path = tmp_path / "benchmark_program_supervisor.json"
    calls: list[dict] = []

    def _callback(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return {"status": "idle", "message": "program watchdog found no executable replay programs"}

    supervisor = DesktopBenchmarkLabProgramSupervisor(state_path=str(state_path), enabled=True, interval_s=5.0)
    supervisor.start(_callback)
    try:
        supervisor.configure(enabled=True, interval_s=5.0)
        deadline = time.time() + 6.5
        while time.time() < deadline:
            if calls:
                break
            time.sleep(0.15)
        assert len(calls) >= 1
        status = supervisor.status()
        assert status["auto_trigger_count"] >= 1
        assert status["last_result_status"] == "idle"
    finally:
        supervisor.stop()


def test_benchmark_program_supervisor_history_persists_and_resets(tmp_path) -> None:
    state_path = tmp_path / "benchmark_program_supervisor.json"

    def _callback(**kwargs):  # noqa: ANN001
        trigger_source = str(kwargs.get("trigger_source", "") or "").strip().lower()
        return {
            "status": "success",
            "message": f"program watchdog executed 1 program(s) from {trigger_source}",
            "targeted_program_count": 1,
            "executed_program_count": 1,
            "executed_campaign_count": 2,
            "executed_sweep_count": 3,
            "stable_program_count": 1,
            "regression_program_count": 0,
            "pending_campaign_count": 1,
            "attention_campaign_count": 0,
            "pending_session_count": 1,
            "pending_app_target_count": 1,
            "long_horizon_pending_count": 2,
            "error_count": 0,
            "latest_program_label": f"{trigger_source} replay program",
            "auto_created_program_count": 1 if trigger_source == "manual_test" else 0,
        }

    supervisor = DesktopBenchmarkLabProgramSupervisor(state_path=str(state_path), enabled=False, interval_s=30.0)
    supervisor.start(_callback)
    try:
        supervisor.trigger_now(source="manual_test", pack="long_horizon_and_replay")
        supervisor.trigger_now(source="ops_test", pack="installer_and_governance")
        history = supervisor.history(limit=4)
        assert history["count"] == 2
        assert history["summary"]["executed_program_total"] == 2
        assert history["summary"]["executed_campaign_total"] == 4
        assert history["latest_run"]["source"] == "ops_test"

        filtered = supervisor.history(limit=4, source="manual_test")
        assert filtered["count"] == 1
        assert filtered["items"][0]["auto_created_program_count"] == 1

        reloaded = DesktopBenchmarkLabProgramSupervisor(state_path=str(state_path))
        persisted = reloaded.history(limit=4)
        assert persisted["count"] == 2

        reset = reloaded.reset_history(source="manual_test")
        assert reset["removed_count"] == 1
        assert reset["remaining_count"] == 1
    finally:
        supervisor.stop()
