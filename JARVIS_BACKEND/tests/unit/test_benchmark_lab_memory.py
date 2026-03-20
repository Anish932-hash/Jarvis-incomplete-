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


def test_benchmark_lab_memory_records_campaigns(tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_memory.json"))
    first_session = memory.record_session(
        filters={"pack": "long_horizon_and_replay", "app": "settings", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.8}, "replay_candidates": [{"scenario": "settings_long_horizon", "apps": ["settings"]}]},
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
    )["session"]
    second_session = memory.record_session(
        filters={"pack": "long_horizon_and_replay", "app": "vscode", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.76}, "replay_candidates": [{"scenario": "vscode_long_horizon", "apps": ["vscode"]}]},
        native_targets_payload={"target_apps": [{"app_name": "vscode"}]},
    )["session"]

    created = memory.record_campaign(
        filters={"pack": "long_horizon_and_replay", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.78}},
        native_targets_payload={"target_apps": [{"app_name": "settings"}, {"app_name": "vscode"}]},
        session_ids=[str(first_session["session_id"]), str(second_session["session_id"])],
        app_targets=["settings", "vscode"],
        session_rows=[dict(first_session), dict(second_session)],
        source="unit_test",
        label="desktop replay campaign",
    )

    assert created["status"] == "success"
    campaign = created["campaign"]
    assert campaign["label"] == "desktop replay campaign"
    assert campaign["session_count"] == 2
    assert campaign["target_app_count"] == 2
    assert campaign["pending_app_target_count"] == 0

    history = memory.campaign_history(limit=5)
    assert history["status"] == "success"
    assert history["count"] == 1
    assert history["latest_campaign"]["campaign_id"] == campaign["campaign_id"]
    assert history["summary"]["pending_sessions"] >= 1


def test_benchmark_lab_memory_records_campaign_sweeps(tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_memory.json"))
    session = memory.record_session(
        filters={"pack": "unsupported_and_recovery", "app": "settings", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.72}, "replay_candidates": [{"scenario": "unsupported_child_dialog_chain", "apps": ["settings"], "horizon_steps": 5}]},
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
    )["session"]
    campaign = memory.record_campaign(
        filters={"pack": "unsupported_and_recovery", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.72}},
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
        session_ids=[str(session["session_id"])],
        app_targets=["settings"],
        session_rows=[dict(session)],
    )["campaign"]

    updated = memory.record_campaign_sweep(
        campaign_id=str(campaign["campaign_id"]),
        sweep_payload={
            "status": "success",
            "regression_status": "stable",
            "executed_session_count": 1,
            "created_session_count": 0,
            "pending_session_count": 1,
            "attention_session_count": 0,
            "long_horizon_pending_count": 1,
            "pending_app_target_count": 0,
        },
        lab_payload={"latest_summary": {"weighted_score": 0.81}},
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
        session_rows=[dict(session)],
    )

    assert updated["status"] == "success"
    assert updated["campaign"]["sweep_count"] == 1
    assert updated["campaign"]["latest_sweep_status"] == "success"
    assert updated["sweep"]["executed_session_count"] == 1


def test_benchmark_lab_memory_tracks_campaign_trends_and_priority(tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_memory.json"))
    session = memory.record_session(
        filters={"pack": "long_horizon_and_replay", "app": "settings", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.72}, "replay_candidates": [{"scenario": "settings_long_horizon", "apps": ["settings"], "horizon_steps": 6}]},
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
    )["session"]
    campaign = memory.record_campaign(
        filters={"pack": "long_horizon_and_replay", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.72}},
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
        session_ids=[str(session["session_id"])],
        app_targets=["settings"],
        session_rows=[dict(session)],
    )["campaign"]

    memory.record_campaign_sweep(
        campaign_id=str(campaign["campaign_id"]),
        sweep_payload={
            "status": "success",
            "regression_status": "regression",
            "executed_session_count": 1,
            "pending_session_count": 2,
            "attention_session_count": 1,
            "long_horizon_pending_count": 2,
            "pending_app_target_count": 1,
            "weighted_score": 0.63,
            "weighted_pass_rate": 0.58,
            "history_direction": "regressing",
        },
        lab_payload={"latest_summary": {"weighted_score": 0.63, "weighted_pass_rate": 0.58}, "history_trend": {"direction": "regressing"}},
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
        session_rows=[{**dict(session), "status": "attention", "pending_replay_count": 2, "failed_replay_count": 1}],
    )
    updated = memory.record_campaign_sweep(
        campaign_id=str(campaign["campaign_id"]),
        sweep_payload={
            "status": "success",
            "regression_status": "stable",
            "executed_session_count": 1,
            "pending_session_count": 0,
            "attention_session_count": 0,
            "long_horizon_pending_count": 1,
            "pending_app_target_count": 0,
            "weighted_score": 0.84,
            "weighted_pass_rate": 0.87,
            "history_direction": "improving",
        },
        lab_payload={"latest_summary": {"weighted_score": 0.84, "weighted_pass_rate": 0.87}, "history_trend": {"direction": "improving"}},
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
        session_rows=[{**dict(session), "status": "complete", "pending_replay_count": 0, "failed_replay_count": 0}],
    )

    trend_summary = updated["campaign"]["trend_summary"]
    assert updated["campaign"]["sweep_count"] == 2
    assert updated["campaign"]["completed_sweep_count"] == 2
    assert updated["campaign"]["latest_sweep_score"] == 0.84
    assert trend_summary["direction"] in {"improving", "volatile"}
    assert updated["campaign"]["campaign_priority"] in {"stable", "steady", "elevated", "critical"}


def test_benchmark_lab_memory_records_programs_and_program_cycles(tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_memory.json"))
    first_session = memory.record_session(
        filters={"pack": "long_horizon_and_replay", "app": "settings", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.78}, "replay_candidates": [{"scenario": "settings_long_horizon", "apps": ["settings"], "horizon_steps": 6}]},
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
    )["session"]
    second_session = memory.record_session(
        filters={"pack": "long_horizon_and_replay", "app": "vscode", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.74}, "replay_candidates": [{"scenario": "vscode_long_horizon", "apps": ["vscode"], "horizon_steps": 5}]},
        native_targets_payload={"target_apps": [{"app_name": "vscode"}]},
    )["session"]
    first_campaign = memory.record_campaign(
        filters={"pack": "long_horizon_and_replay", "app": "settings", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.78}},
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
        session_ids=[str(first_session["session_id"])],
        app_targets=["settings"],
        session_rows=[dict(first_session)],
    )["campaign"]
    second_campaign = memory.record_campaign(
        filters={"pack": "long_horizon_and_replay", "app": "vscode", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.74}},
        native_targets_payload={"target_apps": [{"app_name": "vscode"}]},
        session_ids=[str(second_session["session_id"])],
        app_targets=["vscode"],
        session_rows=[dict(second_session)],
    )["campaign"]

    created = memory.record_program(
        filters={"pack": "long_horizon_and_replay", "limit": 8},
        lab_payload={"latest_summary": {"weighted_score": 0.76}},
        native_targets_payload={"target_apps": [{"app_name": "settings"}, {"app_name": "vscode"}]},
        campaign_ids=[str(first_campaign["campaign_id"]), str(second_campaign["campaign_id"])],
        app_targets=["settings", "vscode"],
        campaign_rows=[dict(first_campaign), dict(second_campaign)],
        source="unit_test",
        label="desktop replay program",
    )

    assert created["status"] == "success"
    program = created["program"]
    assert program["label"] == "desktop replay program"
    assert program["campaign_count"] == 2
    assert program["target_app_count"] == 2

    updated = memory.record_program_cycle(
        program_id=str(program["program_id"]),
        cycle_payload={
            "status": "success",
            "stop_reason": "stable",
            "executed_campaign_count": 2,
            "created_campaign_count": 0,
            "executed_sweep_count": 3,
            "stable_campaign_count": 2,
            "regression_campaign_count": 0,
            "pending_session_count": 0,
            "attention_session_count": 0,
            "pending_app_target_count": 0,
            "long_horizon_pending_count": 1,
            "weighted_score": 0.88,
            "weighted_pass_rate": 0.9,
            "trend_direction": "stable",
        },
        lab_payload={"latest_summary": {"weighted_score": 0.88, "weighted_pass_rate": 0.9}},
        native_targets_payload={"target_apps": [{"app_name": "settings"}, {"app_name": "vscode"}]},
        campaign_rows=[dict(first_campaign), dict(second_campaign)],
    )

    assert updated["status"] == "success"
    assert updated["program"]["cycle_count"] == 1
    assert updated["program"]["latest_cycle_stop_reason"] == "stable"
    assert updated["program"]["program_priority"] in {"steady", "active", "elevated", "critical"}

    history = memory.program_history(limit=5)
    assert history["status"] == "success"
    assert history["count"] == 1
    assert history["latest_program"]["program_id"] == program["program_id"]
    assert history["summary"]["campaign_count"] == 2
