from __future__ import annotations

from backend.python.evaluation.benchmark_lab_memory import DesktopBenchmarkLabMemory


def test_benchmark_lab_memory_records_and_lists_sessions(tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_memory.json"))
    created = memory.record_session(
        filters={"pack": "unsupported_and_recovery", "app": "settings", "limit": 8},
        lab_payload={
            "latest_summary": {"weighted_score": 0.74, "weighted_pass_rate": 0.72},
            "history_trend": {"direction": "regressing", "run_count": 3},
            "replay_candidates": [
                {
                    "scenario": "unsupported_child_dialog_chain",
                    "replay_query": {"scenario_name": "unsupported_child_dialog_chain", "limit": 1},
                    "apps": ["settings"],
                }
            ],
        },
        native_targets_payload={
            "focus_summary": ["unsupported_and_recovery", "surface_exploration"],
            "target_apps": [{"app_name": "settings"}],
            "strongest_tactics": {"descendant_focus": 0.88},
            "coverage_gap_apps": ["outlook"],
        },
        guidance_payload={"focus_summary": ["surface_exploration"]},
        source="unit_test",
        label="unsupported benchmark lab",
    )

    assert created["status"] == "success"
    session = created["session"]
    assert session["label"] == "unsupported benchmark lab"
    assert session["target_app_count"] == 1
    assert session["pending_replay_count"] == 1
    assert session["cycle_count"] == 1
    assert session["latest_cycle_status"] == "success"

    history = memory.session_history(limit=5)
    assert history["status"] == "success"
    assert history["count"] == 1
    assert history["latest_session"]["session_id"] == session["session_id"]
    assert history["summary"]["pending_replays"] == 1
    assert history["summary"]["cycle_count"] == 1


def test_benchmark_lab_memory_records_replay_results(tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_memory.json"))
    created = memory.record_session(
        filters={"pack": "long_horizon_and_replay", "app": "vscode", "limit": 8},
        lab_payload={
            "latest_summary": {"weighted_score": 0.8, "weighted_pass_rate": 0.8},
            "replay_candidates": [
                {
                    "scenario": "vscode_long_horizon_debug_loop",
                    "replay_query": {"scenario_name": "vscode_long_horizon_debug_loop", "limit": 1},
                    "apps": ["vscode"],
                    "capabilities": ["desktop_workflow", "quick_open"],
                }
            ],
        },
        native_targets_payload={"target_apps": [{"app_name": "vscode"}]},
    )
    session_id = str(created["session"]["session_id"])

    updated = memory.record_replay_result(
        session_id=session_id,
        scenario_name="vscode_long_horizon_debug_loop",
        replay_payload={
            "status": "success",
            "items": [{"passed": True}],
            "summary": {"weighted_score": 0.92, "weighted_pass_rate": 1.0},
            "regression": {"status": "stable"},
        },
        replay_query={"scenario_name": "vscode_long_horizon_debug_loop", "limit": 1},
        lab_payload={"latest_summary": {"weighted_score": 0.92, "weighted_pass_rate": 1.0}},
        native_targets_payload={"target_apps": [{"app_name": "vscode"}]},
    )

    assert updated["status"] == "success"
    assert updated["updated_candidate"]["replay_status"] == "completed"
    assert updated["session"]["completed_replay_count"] == 1
    assert updated["session"]["pending_replay_count"] == 0
    assert updated["session"]["cycle_count"] == 1


def test_benchmark_lab_memory_records_run_cycles(tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_memory.json"))
    created = memory.record_session(
        filters={"pack": "long_horizon_and_replay", "app": "settings", "limit": 8},
        lab_payload={
            "latest_run": {"status": "success", "executed_at": "2026-03-18T10:15:00+00:00"},
            "latest_summary": {"weighted_score": 0.71, "weighted_pass_rate": 0.7},
            "latest_regression": {"status": "baseline"},
            "coverage": {"long_horizon": {"count": 2, "ratio": 0.5}},
            "replay_candidates": [
                {
                    "scenario": "settings_long_horizon_replay",
                    "replay_query": {"scenario_name": "settings_long_horizon_replay", "limit": 1},
                    "apps": ["settings"],
                    "horizon_steps": 6,
                }
            ],
        },
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
    )
    session_id = str(created["session"]["session_id"])

    updated = memory.record_run_cycle(
        session_id=session_id,
        cycle_payload={
            "status": "success",
            "executed_at": "2026-03-18T10:20:00+00:00",
            "summary": {"weighted_score": 0.84, "weighted_pass_rate": 0.86},
            "regression": {"status": "stable"},
            "scenario_count": 4,
        },
        cycle_query={"pack": "long_horizon_and_replay", "app": "settings", "limit": 8},
        lab_payload={
            "latest_summary": {"weighted_score": 0.84, "weighted_pass_rate": 0.86},
            "latest_regression": {"status": "stable"},
            "coverage": {"long_horizon": {"count": 3, "ratio": 0.75}},
            "replay_candidates": [
                {
                    "scenario": "settings_long_horizon_replay",
                    "replay_query": {"scenario_name": "settings_long_horizon_replay", "limit": 1},
                    "apps": ["settings"],
                    "horizon_steps": 6,
                }
            ],
        },
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
    )

    assert updated["status"] == "success"
    assert updated["session"]["cycle_count"] == 2
    assert updated["session"]["regression_cycle_count"] == 0
    assert updated["session"]["latest_cycle_regression_status"] == "stable"
    assert updated["session"]["long_horizon_pending_count"] == 1
    assert updated["cycle"]["scenario_count"] == 4
