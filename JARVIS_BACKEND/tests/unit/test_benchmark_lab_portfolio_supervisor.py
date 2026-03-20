from __future__ import annotations

import time

from backend.python.evaluation.benchmark_lab_portfolio_supervisor import (
    DesktopBenchmarkLabPortfolioSupervisor,
)


def test_benchmark_portfolio_supervisor_persists_configuration(tmp_path) -> None:
    state_path = tmp_path / "benchmark_portfolio_supervisor.json"
    supervisor = DesktopBenchmarkLabPortfolioSupervisor(
        state_path=str(state_path),
        enabled=False,
        interval_s=30.0,
    )
    status = supervisor.configure(
        enabled=True,
        interval_s=420.0,
        max_portfolios=3,
        max_programs_per_portfolio=4,
        max_campaigns_per_program=5,
        max_sweeps_per_campaign=3,
        max_sessions=3,
        max_replays_per_session=2,
        history_limit=10,
        portfolio_status="ready",
        pack="long_horizon_and_replay",
        app_name="settings",
        source="test",
    )
    assert status["enabled"] is True
    assert status["interval_s"] == 420.0
    assert status["max_portfolios"] == 3
    assert status["max_programs_per_portfolio"] == 4
    assert status["portfolio_status"] == "ready"

    reloaded = DesktopBenchmarkLabPortfolioSupervisor(state_path=str(state_path))
    reloaded_status = reloaded.status()
    assert reloaded_status["enabled"] is True
    assert reloaded_status["interval_s"] == 420.0
    assert reloaded_status["max_portfolios"] == 3
    assert reloaded_status["max_programs_per_portfolio"] == 4
    assert reloaded_status["pack"] == "long_horizon_and_replay"
    assert reloaded_status["app_name"] == "settings"


def test_benchmark_portfolio_supervisor_manual_trigger_updates_runtime(tmp_path) -> None:
    state_path = tmp_path / "benchmark_portfolio_supervisor.json"
    calls: list[dict] = []

    def _callback(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return {
            "status": "success",
            "message": "portfolio watchdog executed 2 portfolio(s)",
            "targeted_portfolio_count": 2,
            "executed_portfolio_count": 2,
            "executed_program_count": 4,
            "executed_campaign_count": 7,
            "executed_sweep_count": 9,
            "stable_portfolio_count": 1,
            "regression_portfolio_count": 1,
            "pending_program_count": 2,
            "attention_program_count": 1,
            "pending_campaign_count": 3,
            "pending_session_count": 4,
            "pending_app_target_count": 2,
            "long_horizon_pending_count": 5,
            "error_count": 0,
            "latest_portfolio_label": "settings replay portfolio",
            "auto_created_portfolio_count": 1,
            "wave_stop_reason_counts": {"stable": 1, "max_programs_reached": 1},
            "trend_direction_counts": {"improving": 1, "regressing": 1},
        }

    supervisor = DesktopBenchmarkLabPortfolioSupervisor(
        state_path=str(state_path),
        enabled=False,
        interval_s=30.0,
    )
    supervisor.start(_callback)
    try:
        payload = supervisor.trigger_now(
            source="manual_test",
            max_portfolios=3,
            max_programs_per_portfolio=4,
            max_campaigns_per_program=3,
            max_sweeps_per_campaign=2,
            max_sessions=2,
            max_replays_per_session=2,
            history_limit=6,
            pack="long_horizon_and_replay",
            app_name="settings",
        )
        assert payload["status"] == "success"
        assert calls[0]["max_portfolios"] == 3
        assert calls[0]["max_programs_per_portfolio"] == 4
        status = supervisor.status()
        assert status["run_count"] == 1
        assert status["manual_trigger_count"] == 1
        assert status["last_result_status"] == "success"
        assert status["last_summary"]["executed_portfolio_count"] == 2
        assert status["last_summary"]["executed_program_count"] == 4
        assert status["last_summary"]["auto_created_portfolio_count"] == 1
    finally:
        supervisor.stop()


def test_benchmark_portfolio_supervisor_daemon_runs_when_enabled(tmp_path) -> None:
    state_path = tmp_path / "benchmark_portfolio_supervisor.json"
    calls: list[dict] = []

    def _callback(**kwargs):  # noqa: ANN001
        calls.append(dict(kwargs))
        return {"status": "idle", "message": "portfolio watchdog found no executable replay portfolios"}

    supervisor = DesktopBenchmarkLabPortfolioSupervisor(
        state_path=str(state_path),
        enabled=True,
        interval_s=5.0,
    )
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


def test_benchmark_portfolio_supervisor_history_persists_and_resets(tmp_path) -> None:
    state_path = tmp_path / "benchmark_portfolio_supervisor.json"

    def _callback(**kwargs):  # noqa: ANN001
        trigger_source = str(kwargs.get("trigger_source", "") or "").strip().lower()
        return {
            "status": "success",
            "message": f"portfolio watchdog executed 1 portfolio(s) from {trigger_source}",
            "targeted_portfolio_count": 1,
            "executed_portfolio_count": 1,
            "executed_program_count": 2,
            "executed_campaign_count": 3,
            "executed_sweep_count": 4,
            "stable_portfolio_count": 1,
            "regression_portfolio_count": 0,
            "pending_program_count": 1,
            "attention_program_count": 0,
            "pending_campaign_count": 1,
            "pending_session_count": 1,
            "pending_app_target_count": 1,
            "long_horizon_pending_count": 2,
            "error_count": 0,
            "latest_portfolio_label": f"{trigger_source} replay portfolio",
            "auto_created_portfolio_count": 1 if trigger_source == "manual_test" else 0,
        }

    supervisor = DesktopBenchmarkLabPortfolioSupervisor(
        state_path=str(state_path),
        enabled=False,
        interval_s=30.0,
    )
    supervisor.start(_callback)
    try:
        supervisor.trigger_now(source="manual_test", pack="long_horizon_and_replay")
        supervisor.trigger_now(source="ops_test", pack="installer_and_governance")
        history = supervisor.history(limit=4)
        assert history["count"] == 2
        assert history["summary"]["executed_portfolio_total"] == 2
        assert history["summary"]["executed_program_total"] == 4
        assert history["latest_run"]["source"] == "ops_test"

        filtered = supervisor.history(limit=4, source="manual_test")
        assert filtered["count"] == 1
        assert filtered["items"][0]["auto_created_portfolio_count"] == 1

        reloaded = DesktopBenchmarkLabPortfolioSupervisor(state_path=str(state_path))
        persisted = reloaded.history(limit=4)
        assert persisted["count"] == 2

        reset = reloaded.reset_history(source="manual_test")
        assert reset["removed_count"] == 1
        assert reset["remaining_count"] == 1
    finally:
        supervisor.stop()
