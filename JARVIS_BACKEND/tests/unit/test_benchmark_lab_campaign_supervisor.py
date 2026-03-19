from __future__ import annotations

import time

from backend.python.evaluation.benchmark_lab_campaign_supervisor import DesktopBenchmarkLabCampaignSupervisor


def test_benchmark_campaign_supervisor_persists_configuration(tmp_path) -> None:
    state_path = tmp_path / "benchmark_campaign_supervisor.json"
    supervisor = DesktopBenchmarkLabCampaignSupervisor(state_path=str(state_path), enabled=False, interval_s=30.0)
    status = supervisor.configure(
        enabled=True,
        interval_s=240.0,
        max_campaigns=4,
        max_sweeps_per_campaign=3,
        max_sessions=3,
        max_replays_per_session=2,
        history_limit=10,
        campaign_status="ready",
        pack="long_horizon_and_replay",
        app_name="settings",
        source="test",
    )
    assert status["enabled"] is True
    assert status["interval_s"] == 240.0
    assert status["max_campaigns"] == 4
    assert status["max_sweeps_per_campaign"] == 3
    assert status["pack"] == "long_horizon_and_replay"
    assert status["app_name"] == "settings"

    reloaded = DesktopBenchmarkLabCampaignSupervisor(state_path=str(state_path))
    reloaded_status = reloaded.status()
    assert reloaded_status["enabled"] is True
    assert reloaded_status["interval_s"] == 240.0
    assert reloaded_status["max_campaigns"] == 4
    assert reloaded_status["max_sweeps_per_campaign"] == 3
    assert reloaded_status["campaign_status"] == "ready"
    assert reloaded_status["pack"] == "long_horizon_and_replay"
    assert reloaded_status["app_name"] == "settings"


def test_benchmark_campaign_supervisor_manual_trigger_updates_runtime(tmp_path) -> None:
    state_path = tmp_path / "benchmark_campaign_supervisor.json"
    calls: list[dict] = []

    def _callback(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return {
            "status": "success",
            "message": "campaign watchdog executed 2 campaign(s)",
            "targeted_campaign_count": 2,
            "executed_campaign_count": 2,
            "executed_sweep_count": 4,
            "stable_campaign_count": 1,
            "regression_campaign_count": 1,
            "pending_session_count": 3,
            "attention_session_count": 1,
            "pending_app_target_count": 2,
            "long_horizon_pending_count": 4,
            "error_count": 0,
            "latest_campaign_label": "settings replay campaign",
            "auto_created_campaign_count": 1,
            "cycle_stop_reason_counts": {"stable": 1, "max_sweeps_reached": 1},
            "trend_direction_counts": {"improving": 1, "regressing": 1},
        }

    supervisor = DesktopBenchmarkLabCampaignSupervisor(state_path=str(state_path), enabled=False, interval_s=30.0)
    supervisor.start(_callback)
    try:
        payload = supervisor.trigger_now(
            source="manual_test",
            max_campaigns=3,
            max_sweeps_per_campaign=4,
            max_sessions=2,
            max_replays_per_session=2,
            history_limit=6,
            pack="long_horizon_and_replay",
            app_name="settings",
        )
        assert payload["status"] == "success"
        assert calls[0]["max_campaigns"] == 3
        assert calls[0]["max_sweeps_per_campaign"] == 4
        assert calls[0]["pack"] == "long_horizon_and_replay"
        status = supervisor.status()
        assert status["run_count"] == 1
        assert status["manual_trigger_count"] == 1
        assert status["last_result_status"] == "success"
        assert status["last_summary"]["executed_campaign_count"] == 2
        assert status["last_summary"]["executed_sweep_count"] == 4
        assert status["last_summary"]["stable_campaign_count"] == 1
        assert status["last_summary"]["latest_campaign_label"] == "settings replay campaign"
        assert status["last_summary"]["auto_created_campaign_count"] == 1
    finally:
        supervisor.stop()


def test_benchmark_campaign_supervisor_daemon_runs_when_enabled(tmp_path) -> None:
    state_path = tmp_path / "benchmark_campaign_supervisor.json"
    calls: list[dict] = []

    def _callback(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return {"status": "idle", "message": "campaign watchdog found no executable replay campaigns"}

    supervisor = DesktopBenchmarkLabCampaignSupervisor(state_path=str(state_path), enabled=True, interval_s=5.0)
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
        assert status["last_result_status"] == "idle"
    finally:
        supervisor.stop()


def test_benchmark_campaign_supervisor_history_persists_and_resets(tmp_path) -> None:
    state_path = tmp_path / "benchmark_campaign_supervisor.json"

    def _callback(**kwargs):  # noqa: ANN001
        trigger_source = str(kwargs.get("trigger_source", "") or "").strip().lower()
        return {
            "status": "success",
            "message": f"campaign watchdog executed 1 campaign(s) from {trigger_source}",
            "targeted_campaign_count": 1,
            "executed_campaign_count": 1,
            "executed_sweep_count": 2,
            "stable_campaign_count": 1,
            "regression_campaign_count": 0,
            "pending_session_count": 1,
            "attention_session_count": 0,
            "pending_app_target_count": 1,
            "long_horizon_pending_count": 2,
            "error_count": 0,
            "latest_campaign_label": f"{trigger_source} replay campaign",
            "auto_created_campaign_count": 1 if trigger_source == "manual_test" else 0,
        }

    supervisor = DesktopBenchmarkLabCampaignSupervisor(state_path=str(state_path), enabled=False, interval_s=30.0)
    supervisor.start(_callback)
    try:
        supervisor.trigger_now(source="manual_test", pack="long_horizon_and_replay")
        supervisor.trigger_now(source="ops_test", pack="installer_and_governance")
        status = supervisor.status()
        assert status["history_count"] == 2
        assert isinstance(status.get("latest_history_run"), dict)
        assert status["latest_history_run"]["source"] == "ops_test"

        history = supervisor.history(limit=4)
        assert history["status"] == "success"
        assert history["count"] == 2
        assert history["latest_run"]["source"] == "ops_test"
        assert history["summary"]["executed_sweep_total"] == 4
        assert history["summary"]["stable_campaign_total"] == 2

        filtered = supervisor.history(limit=4, source="manual_test")
        assert filtered["count"] == 1
        assert filtered["items"][0]["auto_created_campaign_count"] == 1

        reloaded = DesktopBenchmarkLabCampaignSupervisor(state_path=str(state_path))
        persisted = reloaded.history(limit=4)
        assert persisted["count"] == 2
        assert persisted["latest_run"]["source"] == "ops_test"

        reset = reloaded.reset_history(source="manual_test")
        assert reset["removed_count"] == 1
        assert reset["remaining_count"] == 1
        assert reset["latest_run"]["source"] == "ops_test"
    finally:
        supervisor.stop()
