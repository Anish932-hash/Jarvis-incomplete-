from __future__ import annotations

import asyncio

from backend.python.core.contracts import ExecutionPlan, PlanStep
from backend.python.evaluation.benchmark_lab_memory import DesktopBenchmarkLabMemory
from backend.python.evaluation.runner import EvaluationRunner
from backend.python.evaluation.scenarios import Scenario


def test_evaluation_runner_produces_weighted_summary(monkeypatch) -> None:
    runner = EvaluationRunner()

    async def _build_plan(goal, context):  # noqa: ANN001
        del context
        text = str(goal.request.text)
        if text == "strict":
            steps = [
                PlanStep(step_id="s1", action="open_app"),
                PlanStep(step_id="s2", action="tts_speak"),
            ]
        else:
            steps = [
                PlanStep(step_id="s1", action="external_email_send"),
                PlanStep(step_id="s2", action="time_now"),
            ]
        return ExecutionPlan(plan_id="plan-1", goal_id=goal.goal_id, intent="test", steps=steps)

    monkeypatch.setattr(runner.planner, "build_plan", _build_plan)
    scenarios = [
        Scenario(
            "strict_case",
            "strict",
            ["open_app", "tts_speak"],
            weight=2.0,
            strict_order=True,
            category="desktop_basics",
            capabilities=["launch", "speech"],
        ),
        Scenario(
            "flex_case",
            "flex",
            ["external_email_send"],
            weight=1.0,
            strict_order=False,
            required_actions=["external_email_send"],
            category="communication",
            capabilities=["connectors", "mail"],
            risk_level="guarded",
        ),
    ]

    payload = runner.run_with_summary(scenarios)
    items = payload["items"]
    summary = payload["summary"]

    assert len(items) == 2
    assert all(bool(item["passed"]) for item in items)
    assert float(summary["weighted_pass_rate"]) == 1.0
    assert float(summary["weighted_score"]) > 0.85
    assert any(row["name"] == "desktop_basics" for row in summary["category_breakdown"])
    assert any(row["name"] == "launch" for row in summary["capability_coverage"])
    assert any(row["name"] == "guarded" for row in summary["risk_breakdown"])


def test_evaluation_runner_lcs_metrics_capture_unexpected_actions() -> None:
    runner = EvaluationRunner()
    metrics = runner._scenario_metrics(  # noqa: SLF001
        expected=["open_app", "write_file"],
        actual=["open_app", "time_now", "write_file"],
        required=[],
        strict_order=False,
    )
    assert float(metrics["precision"]) < 1.0
    assert "time_now" in list(metrics["unexpected_actions"])
    assert float(metrics["recall"]) >= 1.0
    assert "write_file" not in list(metrics["missing_expected"])


def test_evaluation_catalog_summary_tracks_phase4_dimensions() -> None:
    runner = EvaluationRunner()
    payload = runner.catalog(
        [
            Scenario(
                "unsupported_chain",
                "Continue through a child dialog chain in settings",
                ["desktop_interact"],
                strict_order=False,
                required_actions=["desktop_interact"],
                category="unsupported_app",
                capabilities=["surface_exploration", "recovery"],
                risk_level="guarded",
                pack="unsupported_and_recovery",
                mission_family="exploration",
                autonomy_tier="autonomous",
                apps=["settings"],
                recovery_expected=True,
                native_hybrid_focus=True,
            ),
            Scenario(
                "installer_resume",
                "Resume blocked installer after approval",
                ["desktop_interact"],
                strict_order=False,
                required_actions=["desktop_interact"],
                category="installer",
                capabilities=["wizard_mission", "governance"],
                risk_level="high",
                pack="installer_and_governance",
                mission_family="recovery",
                autonomy_tier="guardrailed",
                apps=["installer"],
                recovery_expected=True,
                native_hybrid_focus=False,
            ),
        ]
    )

    assert payload["status"] == "success"
    assert payload["count"] == 2
    summary = payload["summary"]
    assert summary["pack_counts"]["installer_and_governance"] == 1
    assert summary["pack_counts"]["unsupported_and_recovery"] == 1
    assert summary["autonomy_tier_counts"]["autonomous"] == 1
    assert summary["autonomy_tier_counts"]["guardrailed"] == 1
    assert summary["mission_family_counts"]["exploration"] == 1
    assert summary["mission_family_counts"]["recovery"] == 1
    assert summary["recovery_expected_count"] == 2
    assert summary["native_hybrid_focus_count"] == 1
    assert summary["app_counts"]["installer"] == 1
    assert summary["app_counts"]["settings"] == 1


def test_evaluation_runner_reports_regressions_against_previous_run(monkeypatch) -> None:
    runner = EvaluationRunner()
    state = {"regressed": False}

    async def _build_plan(goal, context):  # noqa: ANN001
        del context
        text = str(goal.request.text).lower()
        if "installer" in text and state["regressed"]:
            steps = [PlanStep(step_id="s1", action="time_now")]
        else:
            steps = [PlanStep(step_id="s1", action="desktop_interact")]
        return ExecutionPlan(plan_id="plan-1", goal_id=goal.goal_id, intent="test", steps=steps)

    monkeypatch.setattr(runner.planner, "build_plan", _build_plan)
    scenarios = [
        Scenario(
            "installer_resume_after_prompt",
            "Resume the blocked installer after approval is completed",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="installer",
            capabilities=["wizard_mission", "desktop_recovery", "governance"],
            risk_level="high",
            pack="installer_and_governance",
            mission_family="recovery",
            autonomy_tier="autonomous",
            apps=["installer"],
            recovery_expected=True,
            native_hybrid_focus=True,
        )
    ]

    baseline = runner.run_with_summary(scenarios)
    assert baseline["regression"]["status"] == "baseline"

    state["regressed"] = True
    payload = runner.run_with_summary(scenarios)
    regression = payload["regression"]

    assert regression["status"] == "regression"
    assert float(regression["weighted_score_delta"]) < 0.0
    assert float(regression["weighted_pass_rate_delta"]) < 0.0
    assert regression["scenario_regressions"][0]["scenario"] == "installer_resume_after_prompt"
    assert regression["pack_regressions"][0]["name"] == "installer_and_governance"
    assert regression["category_regressions"][0]["name"] == "installer"
    assert regression["capability_regressions"][0]["name"] == "desktop_recovery"


def test_evaluation_runner_history_and_improvement_candidates(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4)

    async def _build_plan(goal, context):  # noqa: ANN001
        del context
        text = str(goal.request.text).lower()
        if "installer" in text:
            steps = [PlanStep(step_id="s1", action="time_now")]
        else:
            steps = [PlanStep(step_id="s1", action="desktop_interact")]
        return ExecutionPlan(plan_id="plan-1", goal_id=goal.goal_id, intent="test", steps=steps)

    monkeypatch.setattr(runner.planner, "build_plan", _build_plan)
    scenarios = [
        Scenario(
            "settings_autonomy",
            "Open settings and apply settings changes",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="settings",
            capabilities=["form_mission", "desktop_recovery"],
            risk_level="guarded",
            pack="settings_and_admin",
            mission_family="form",
            autonomy_tier="autonomous",
            apps=["settings"],
            recovery_expected=True,
            native_hybrid_focus=True,
        ),
        Scenario(
            "installer_autonomy",
            "Resume blocked installer after approval",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="installer",
            capabilities=["wizard_mission", "desktop_recovery"],
            risk_level="high",
            pack="installer_and_governance",
            mission_family="recovery",
            autonomy_tier="autonomous",
            apps=["installer"],
            recovery_expected=True,
            native_hybrid_focus=True,
        ),
    ]

    payload = runner.run_with_summary(scenarios)
    summary = payload["summary"]
    candidates = summary["improvement_candidates"]

    assert payload["latest_run"]["status"] == "success"
    assert candidates["packs"][0]["name"] == "installer_and_governance"
    assert candidates["categories"][0]["name"] == "installer"
    assert candidates["capabilities"][0]["name"] == "wizard_mission"
    assert candidates["recovery_focus"]["target"] == "recovery_readiness"
    assert candidates["native_hybrid_focus"]["target"] == "native_hybrid_coverage"

    history = runner.history(limit=2)
    assert history["status"] == "success"
    assert history["count"] == 1
    assert history["items"][0]["scenario_count"] == 2


def test_evaluation_runner_control_guidance_uses_latest_summary(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4)

    async def _build_plan(goal, context):  # noqa: ANN001
        del context
        text = str(goal.request.text).lower()
        if "installer" in text:
            steps = [PlanStep(step_id="s1", action="time_now")]
        else:
            steps = [PlanStep(step_id="s1", action="desktop_interact")]
        return ExecutionPlan(plan_id="plan-1", goal_id=goal.goal_id, intent="test", steps=steps)

    monkeypatch.setattr(runner.planner, "build_plan", _build_plan)
    scenarios = [
        Scenario(
            "unsupported_child_dialog_chain",
            "Recover the unsupported child dialog chain",
            ["desktop_interact"],
            required_actions=["desktop_interact"],
            category="unsupported_app",
            capabilities=["surface_exploration", "child_window_adoption"],
            risk_level="guarded",
            pack="unsupported_and_recovery",
            mission_family="exploration",
            autonomy_tier="autonomous",
            apps=["settings"],
            recovery_expected=True,
            native_hybrid_focus=True,
        ),
        Scenario(
            "installer_resume_after_prompt",
            "Resume blocked installer after approval",
            ["time_now"],
            required_actions=["desktop_interact"],
            category="installer",
            capabilities=["wizard_mission", "desktop_recovery"],
            risk_level="high",
            pack="installer_and_governance",
            mission_family="recovery",
            autonomy_tier="autonomous",
            apps=["installer"],
            recovery_expected=True,
            native_hybrid_focus=True,
        ),
    ]

    runner.run_with_summary(scenarios)
    guidance = runner.control_guidance()

    assert guidance["status"] == "success"
    assert guidance["benchmark_ready"] is True
    assert guidance["weakest_pack"] in {"unsupported_and_recovery", "installer_and_governance"}
    assert guidance["weakest_capability"] in {
        "surface_exploration",
        "child_window_adoption",
        "wizard_mission",
        "desktop_recovery",
    }
    assert isinstance(guidance["focus_summary"], list)
    assert guidance["history_size"] == 1
    control_biases = guidance["control_biases"]
    assert float(control_biases["dialog_resolution"]) > 0.12
    assert float(control_biases["recovery_reacquire"]) > 0.1


def test_evaluation_catalog_summary_tracks_phase5_lab_dimensions() -> None:
    runner = EvaluationRunner()
    payload = runner.catalog(
        [
            Scenario(
                "long_horizon_settings",
                "Open settings and apply privacy changes",
                ["desktop_interact"],
                strict_order=False,
                required_actions=["desktop_interact"],
                category="settings",
                capabilities=["settings_control", "recovery"],
                risk_level="guarded",
                pack="long_horizon_and_replay",
                mission_family="form",
                autonomy_tier="autonomous",
                apps=["settings"],
                recovery_expected=True,
                native_hybrid_focus=True,
                replayable=True,
                horizon_steps=6,
            ),
            Scenario(
                "fast_status",
                "Check defender status",
                ["defender_status"],
                category="system_ops",
                capabilities=["system_status"],
                apps=["defender"],
                replayable=True,
                horizon_steps=1,
            ),
        ]
    )

    summary = payload["summary"]
    assert summary["replayable_count"] == 2
    assert summary["long_horizon_count"] == 1
    assert float(summary["avg_horizon_steps"]) > 3.0
    assert summary["max_horizon_steps"] == 6


def test_evaluation_runner_lab_reports_replay_candidates_and_installed_app_coverage(monkeypatch) -> None:
    installed_provider_payload = {
        "status": "success",
        "count": 4,
        "total": 4,
        "items": [
            {"name": "Settings", "profile_id": "settings", "aliases": ["settings"], "category": "system"},
            {"name": "Installer", "profile_id": "installer", "aliases": ["installer"], "category": "system"},
            {"name": "Visual Studio Code", "profile_id": "vscode", "aliases": ["vscode"], "category": "developer"},
            {"name": "Clipchamp", "profile_id": "clipchamp", "aliases": ["clipchamp"], "category": "media"},
        ],
    }
    runner = EvaluationRunner(installed_app_catalog_provider=lambda **_: installed_provider_payload)

    async def _build_plan(goal, context):  # noqa: ANN001
        del context
        text = str(goal.request.text).lower()
        if "installer" in text:
            steps = [PlanStep(step_id="s1", action="time_now")]
        else:
            steps = [PlanStep(step_id="s1", action="desktop_interact")]
        return ExecutionPlan(plan_id="plan-1", goal_id=goal.goal_id, intent="test", steps=steps)

    monkeypatch.setattr(runner.planner, "build_plan", _build_plan)
    scenarios = [
        Scenario(
            "unsupported_child_dialog_chain",
            "Explore surface for add bluetooth device in settings and continue through the child dialog chain",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="unsupported_app",
            capabilities=["surface_exploration", "child_window_adoption", "recovery"],
            risk_level="guarded",
            pack="unsupported_and_recovery",
            mission_family="exploration",
            autonomy_tier="autonomous",
            apps=["settings"],
            recovery_expected=True,
            native_hybrid_focus=True,
            replayable=True,
            horizon_steps=5,
        ),
        Scenario(
            "installer_resume_after_prompt",
            "Resume the blocked installer after approval is completed",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="installer",
            capabilities=["wizard_mission", "desktop_recovery", "governance"],
            risk_level="high",
            pack="installer_and_governance",
            mission_family="recovery",
            autonomy_tier="autonomous",
            apps=["installer"],
            recovery_expected=True,
            native_hybrid_focus=True,
            replayable=True,
            horizon_steps=5,
        ),
        Scenario(
            "vscode_long_horizon_debug_loop",
            "Open vscode, run npm test in the terminal, inspect failures, and reopen the failing file with quick open",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="editor_workflow",
            capabilities=["editor", "terminal", "quick_open", "desktop_workflow", "command_execution"],
            pack="long_horizon_and_replay",
            mission_family="workflow",
            autonomy_tier="autonomous",
            apps=["vscode"],
            native_hybrid_focus=True,
            replayable=True,
            horizon_steps=6,
        ),
    ]

    payload = runner.run_with_summary(scenarios)
    assert payload["status"] == "success"

    lab = runner.lab(history_limit=4)
    assert lab["status"] == "success"
    assert lab["coverage"]["long_horizon"]["count"] >= 3
    assert lab["history_trend"]["run_count"] == 1
    assert lab["installed_app_coverage"]["benchmarked_installed_app_count"] == 3
    assert "Clipchamp" in lab["installed_app_coverage"]["missing_apps"]
    assert lab["replay_candidates"][0]["scenario"] == "installer_resume_after_prompt"
    assert lab["replay_candidates"][0]["replay_query"]["scenario_name"] == "installer_resume_after_prompt"


def test_evaluation_runner_native_control_targets_aggregates_app_tactics(monkeypatch) -> None:
    installed_provider_payload = {
        "status": "success",
        "count": 3,
        "total": 3,
        "items": [
            {"name": "Settings", "profile_id": "settings", "aliases": ["settings"], "category": "system"},
            {"name": "Installer", "profile_id": "installer", "aliases": ["installer"], "category": "system"},
            {"name": "Visual Studio Code", "profile_id": "vscode", "aliases": ["vscode"], "category": "developer"},
        ],
    }
    runner = EvaluationRunner(installed_app_catalog_provider=lambda **_: installed_provider_payload)

    async def _build_plan(goal, context):  # noqa: ANN001
        del context
        text = str(goal.request.text).lower()
        if "installer" in text:
            steps = [PlanStep(step_id="s1", action="time_now")]
        else:
            steps = [PlanStep(step_id="s1", action="desktop_interact")]
        return ExecutionPlan(plan_id="plan-1", goal_id=goal.goal_id, intent="test", steps=steps)

    monkeypatch.setattr(runner.planner, "build_plan", _build_plan)
    scenarios = [
        Scenario(
            "settings_child_dialog_chain",
            "Open settings and continue through the child dialog chain",
            ["desktop_interact"],
            required_actions=["desktop_interact"],
            category="unsupported_app",
            capabilities=["surface_exploration", "child_window_adoption", "recovery"],
            pack="unsupported_and_recovery",
            mission_family="exploration",
            autonomy_tier="autonomous",
            apps=["settings"],
            recovery_expected=True,
            native_hybrid_focus=True,
            replayable=True,
            horizon_steps=5,
        ),
        Scenario(
            "installer_resume_after_prompt",
            "Resume the blocked installer after approval is completed",
            ["desktop_interact"],
            required_actions=["desktop_interact"],
            category="installer",
            capabilities=["wizard_mission", "desktop_recovery", "governance"],
            risk_level="high",
            pack="installer_and_governance",
            mission_family="recovery",
            autonomy_tier="autonomous",
            apps=["installer"],
            recovery_expected=True,
            native_hybrid_focus=True,
            replayable=True,
            horizon_steps=5,
        ),
    ]

    payload = runner.run_with_summary(scenarios)
    assert payload["status"] == "success"

    native_targets = runner.native_control_targets(history_limit=4)
    assert native_targets["status"] == "success"
    assert native_targets["benchmark_ready"] is True
    assert native_targets["target_apps"][0]["app_name"] == "installer"
    assert native_targets["target_app_biases"]["installer"]["recovery_reacquire"] > 0.0
    assert native_targets["replay_session_summary"]["session_count"] == 0
    assert "Visual Studio Code" in native_targets["coverage_gap_apps"]


def test_evaluation_runner_persists_lab_sessions_and_replays_them(monkeypatch, tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_memory.json"))
    runner = EvaluationRunner(history_limit=6, lab_memory=memory)

    async def _build_plan(goal, context):  # noqa: ANN001
        del context
        text = str(goal.request.text).lower()
        if "installer" in text:
            steps = [PlanStep(step_id="s1", action="time_now")]
        else:
            steps = [PlanStep(step_id="s1", action="desktop_interact")]
        return ExecutionPlan(plan_id="plan-1", goal_id=goal.goal_id, intent="test", steps=steps)

    monkeypatch.setattr(runner.planner, "build_plan", _build_plan)
    scenarios = [
        Scenario(
            "unsupported_child_dialog_chain",
            "Explore surface for add bluetooth device in settings and continue through the child dialog chain",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="unsupported_app",
            capabilities=["surface_exploration", "child_window_adoption", "recovery"],
            risk_level="guarded",
            pack="unsupported_and_recovery",
            mission_family="exploration",
            autonomy_tier="autonomous",
            apps=["settings"],
            recovery_expected=True,
            native_hybrid_focus=True,
            replayable=True,
            horizon_steps=5,
        )
    ]

    payload = runner.run_with_summary(scenarios)
    assert payload["status"] == "success"

    created = runner.create_lab_session(pack="unsupported_and_recovery", app="settings", history_limit=4)
    assert created["status"] == "success"
    session = created["session"]
    assert session["replay_candidate_count"] >= 1
    assert session["target_app_count"] >= 1
    assert session["cycle_count"] == 1

    history = runner.lab_sessions(limit=4)
    assert history["status"] == "success"
    assert history["count"] == 1
    assert history["latest_session"]["session_id"] == session["session_id"]

    replayed = runner.replay_lab_session(
        session_id=str(session["session_id"]),
        scenario_name="unsupported_child_dialog_chain",
    )
    assert replayed["status"] == "success"
    assert replayed["updated_candidate"]["scenario"] == "unsupported_child_dialog_chain"
    assert replayed["updated_candidate"]["replay_status"] in {"completed", "failed"}
    assert replayed["session"]["session_id"] == session["session_id"]

    native_targets = runner.native_control_targets(pack="unsupported_and_recovery", app="settings", history_limit=4)
    assert native_targets["status"] == "success"
    assert native_targets["replay_session_summary"]["session_count"] == 1
    assert native_targets["replay_session_summary"]["latest_session_id"] == session["session_id"]
    target_row = next(item for item in native_targets["target_apps"] if str(item.get("app_name", "")) == "settings")
    assert str(target_row["hint_query"]).strip()
    assert float(target_row["replay_pressure"]) > 0.0
    assert int(target_row["replay_session_count"]) == 1
    assert int(target_row["replay_pending_count"]) >= 0
    assert int(target_row["replay_failed_count"]) >= 0
    assert "unsupported_child_dialog_chain" in list(target_row["replay_scenarios"])


def test_evaluation_runner_runs_lab_session_cycles_and_batches(monkeypatch, tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_cycles.json"))
    runner = EvaluationRunner(history_limit=8, lab_memory=memory)

    async def _build_plan(goal, context):  # noqa: ANN001
        del context
        return ExecutionPlan(
            plan_id="plan-1",
            goal_id=goal.goal_id,
            intent="test",
            steps=[PlanStep(step_id="s1", action="desktop_interact")],
        )

    monkeypatch.setattr(runner.planner, "build_plan", _build_plan)
    scenarios = [
        Scenario(
            "settings_privacy_long_horizon",
            "Open settings and continue through a longer privacy workflow",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="settings",
            capabilities=["settings_control", "recovery"],
            risk_level="guarded",
            pack="long_horizon_and_replay",
            mission_family="workflow",
            autonomy_tier="autonomous",
            apps=["settings"],
            recovery_expected=True,
            native_hybrid_focus=True,
            replayable=True,
            horizon_steps=6,
        ),
        Scenario(
            "settings_bluetooth_chain",
            "Explore the bluetooth child dialog chain in settings",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="unsupported_app",
            capabilities=["surface_exploration", "child_window_adoption"],
            risk_level="guarded",
            pack="unsupported_and_recovery",
            mission_family="exploration",
            autonomy_tier="autonomous",
            apps=["settings"],
            recovery_expected=True,
            native_hybrid_focus=True,
            replayable=True,
            horizon_steps=5,
        ),
    ]

    payload = runner.run_with_summary(scenarios)
    assert payload["status"] == "success"
    created = runner.create_lab_session(app="settings", limit=12, history_limit=6)
    session = created["session"]

    cycled = runner.run_lab_session_cycle(session_id=str(session["session_id"]), history_limit=6)
    assert cycled["status"] == "success"
    assert cycled["session"]["cycle_count"] == 2
    assert cycled["session"]["latest_cycle_score"] >= 0.0
    assert cycled["cycle"]["scenario_count"] >= 2

    advanced = runner.advance_lab_session(session_id=str(session["session_id"]), max_replays=2)
    assert advanced["status"] == "success"
    assert advanced["batch_count"] >= 1
    assert len(advanced["replayed_scenarios"]) >= 1
    assert advanced["session"]["cycle_count"] == 2
    assert int(advanced["session"]["pending_replay_count"]) >= 0

    native_targets = runner.native_control_targets(app="settings", history_limit=6)
    assert native_targets["status"] == "success"
    assert native_targets["replay_session_summary"]["cycle_count"] >= 2
    assert native_targets["replay_session_summary"]["long_horizon_pending_count"] >= 0
    settings_row = next(item for item in native_targets["target_apps"] if str(item.get("app_name", "")) == "settings")
    assert int(settings_row["session_cycle_count"]) >= 2


def test_evaluation_runner_creates_lab_campaigns_and_sweeps(monkeypatch, tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_campaigns.json"))
    runner = EvaluationRunner(history_limit=8, lab_memory=memory)

    async def _build_plan(goal, context):  # noqa: ANN001
        del context
        text = str(goal.request.text).lower()
        action = "desktop_interact" if "settings" in text or "vscode" in text else "time_now"
        return ExecutionPlan(
            plan_id="plan-1",
            goal_id=goal.goal_id,
            intent="test",
            steps=[PlanStep(step_id="s1", action=action)],
        )

    monkeypatch.setattr(runner.planner, "build_plan", _build_plan)

    scenarios = [
        Scenario(
            "settings_long_horizon_replay",
            "Continue settings exploration through a long horizon flow",
            ["desktop_interact"],
            required_actions=["desktop_interact"],
            category="settings",
            capabilities=["surface_exploration", "desktop_recovery"],
            pack="long_horizon_and_replay",
            mission_family="exploration",
            autonomy_tier="autonomous",
            apps=["settings"],
            horizon_steps=5,
            replayable=True,
            recovery_expected=True,
            native_hybrid_focus=True,
        ),
        Scenario(
            "vscode_long_horizon_debug_loop",
            "Recover VS Code workflow and continue a debug loop",
            ["desktop_interact"],
            required_actions=["desktop_interact"],
            category="editor_workflow",
            capabilities=["desktop_workflow", "quick_open"],
            pack="long_horizon_and_replay",
            mission_family="workflow",
            autonomy_tier="autonomous",
            apps=["vscode"],
            horizon_steps=6,
            replayable=True,
            recovery_expected=True,
            native_hybrid_focus=True,
        ),
    ]

    runner.run_with_summary(scenarios)
    created = runner.create_lab_campaign(pack="long_horizon_and_replay", limit=12, history_limit=6, max_sessions=2)
    assert created["status"] == "success"
    campaign = created["campaign"]
    assert int(campaign["session_count"]) >= 1
    assert int(created["created_session_count"]) >= 1

    history = runner.lab_campaigns(limit=4)
    assert history["status"] == "success"
    assert history["count"] == 1
    assert history["latest_campaign"]["campaign_id"] == campaign["campaign_id"]

    swept = runner.run_lab_campaign_sweep(
        campaign_id=str(campaign["campaign_id"]),
        max_sessions=2,
        max_replays_per_session=2,
        history_limit=6,
    )
    assert swept["status"] == "success"
    assert swept["campaign"]["campaign_id"] == campaign["campaign_id"]
    assert int(swept["campaign"]["sweep_count"]) >= 1
    assert isinstance(swept["results"], list)

    native_targets = runner.native_control_targets(pack="long_horizon_and_replay", history_limit=6)
    assert native_targets["status"] == "success"
    assert int(native_targets["replay_campaign_summary"]["campaign_count"]) >= 1
    assert int(native_targets["replay_campaign_summary"]["sweep_count"]) >= 1
    settings_like_row = next(
        item
        for item in native_targets["target_apps"]
        if str(item.get("app_name", "")) in {"settings", "vscode"}
    )
    assert int(settings_like_row["campaign_count"]) >= 1
    assert float(settings_like_row["campaign_pressure"]) > 0.0
    assert str(settings_like_row["campaign_hint_query"]).strip()
    assert str(settings_like_row["campaign_preferred_window_title"]).strip()
    assert settings_like_row["descendant_title_sequence"]
    assert settings_like_row["campaign_descendant_title_sequence"]


def test_evaluation_runner_campaign_cycle_runs_multiple_sweeps_until_stable(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4, lab_memory=DesktopBenchmarkLabMemory())
    sweep_calls: list[str] = []
    states = [
        {
            "campaign_id": "camp-settings",
            "label": "Settings replay campaign",
            "pending_session_count": 2,
            "attention_session_count": 1,
            "pending_app_target_count": 1,
            "long_horizon_pending_count": 2,
            "latest_sweep_regression_status": "stable",
            "campaign_priority": "elevated",
            "trend_summary": {"direction": "improving"},
            "history_direction": "improving",
        },
        {
            "campaign_id": "camp-settings",
            "label": "Settings replay campaign",
            "pending_session_count": 0,
            "attention_session_count": 0,
            "pending_app_target_count": 0,
            "long_horizon_pending_count": 1,
            "latest_sweep_regression_status": "stable",
            "campaign_priority": "stable",
            "trend_summary": {"direction": "improving"},
            "history_direction": "improving",
        },
    ]

    def _run_lab_campaign_sweep(**kwargs):  # noqa: ANN001
        sweep_calls.append(str(kwargs.get("campaign_id", "") or ""))
        current = dict(states[min(len(sweep_calls) - 1, len(states) - 1)])
        current["sweep_count"] = len(sweep_calls)
        return {
            "status": "success",
            "campaign": current,
            "sweep": {
                "executed_session_count": 1,
                "weighted_score": 0.81 + (0.05 * (len(sweep_calls) - 1)),
                "weighted_pass_rate": 0.78 + (0.1 * (len(sweep_calls) - 1)),
            },
            "created_session_count": 0,
            "lab": {"latest_summary": {"weighted_score": 0.86, "weighted_pass_rate": 0.88}},
            "native_targets": {"status": "success"},
            "guidance": {"status": "success"},
        }

    monkeypatch.setattr(runner, "run_lab_campaign_sweep", _run_lab_campaign_sweep)

    payload = runner.run_lab_campaign_cycle(
        campaign_id="camp-settings",
        max_sweeps=3,
        max_sessions=2,
        max_replays_per_session=2,
        history_limit=6,
        stop_on_stable=True,
    )

    assert payload["status"] == "success"
    assert payload["cycle"]["executed_sweep_count"] == 2
    assert payload["cycle"]["stop_reason"] == "stable"
    assert payload["cycle"]["stable"] is True
    assert len(payload["results"]) == 2
    assert sweep_calls == ["camp-settings", "camp-settings"]


def test_evaluation_runner_native_targets_fallback_to_latest_rows(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4)

    async def _build_plan(goal, context):  # noqa: ANN001
        del context
        return ExecutionPlan(
            plan_id="plan-1",
            goal_id=goal.goal_id,
            intent="test",
            steps=[PlanStep(step_id="s1", action="desktop_interact")],
        )

    monkeypatch.setattr(runner.planner, "build_plan", _build_plan)
    scenarios = [
        Scenario(
            "custom_vscode_replay",
            "Run npm test in vscode and reopen the failing file",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="editor_workflow",
            capabilities=["desktop_workflow", "quick_open", "command_execution"],
            risk_level="standard",
            pack="long_horizon_and_replay",
            mission_family="workflow",
            autonomy_tier="autonomous",
            apps=["vscode"],
            native_hybrid_focus=True,
            replayable=True,
            horizon_steps=6,
            tags=["editor", "long_horizon"],
        )
    ]

    runner.run_with_summary(scenarios)
    runner.last_items = [
        {
            "scenario": "custom_vscode_replay",
            "user_text": "Run npm test in vscode and reopen the failing file",
            "pack": "long_horizon_and_replay",
            "category": "editor_workflow",
            "mission_family": "workflow",
            "capabilities": ["desktop_workflow", "quick_open", "command_execution"],
            "apps": ["vscode"],
            "native_hybrid_focus": True,
            "recovery_expected": False,
            "horizon_steps": 6,
            "score": 0.61,
            "weight": 1.4,
            "replayable": True,
        }
    ]
    lab_payload = runner.lab(pack="long_horizon_and_replay", app="vscode")
    custom_candidates = list(lab_payload.get("replay_candidates", []))
    custom_candidates.insert(
        0,
        {
            "scenario": "custom_vscode_replay",
            "pack": "long_horizon_and_replay",
            "category": "editor_workflow",
            "mission_family": "workflow",
            "apps": ["vscode"],
            "capabilities": ["desktop_workflow", "quick_open", "command_execution"],
            "score": 0.5,
            "weight": 1.5,
            "replayable": True,
            "horizon_steps": 6,
            "reasons": ["custom_latest_row"],
            "replay_query": {"scenario_name": "custom_vscode_replay", "limit": 1},
        },
    )
    monkeypatch.setattr(runner, "lab", lambda **kwargs: {**lab_payload, "replay_candidates": custom_candidates})

    payload = runner.native_control_targets(pack="long_horizon_and_replay", app="vscode")
    assert payload["status"] == "success"
    assert payload["benchmark_ready"] is True
    vscode_row = next(item for item in payload["target_apps"] if str(item.get("app_name", "")) == "vscode")
    assert str(vscode_row["hint_query"]).strip()
    assert vscode_row["descendant_title_hints"]
    assert vscode_row["descendant_title_sequence"]
    assert str(vscode_row["descendant_hint_query"]).strip()
    assert str(vscode_row["preferred_window_title"]).strip()
    assert float(vscode_row["replay_pressure"]) > 0.0


def test_evaluation_runner_campaign_watchdog_prioritizes_attention_and_regressions(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4, lab_memory=DesktopBenchmarkLabMemory())
    cycle_calls: list[str] = []

    campaigns_payload = {
        "status": "success",
        "count": 3,
        "items": [
            {
                "campaign_id": "camp-settings",
                "label": "Settings replay campaign",
                "status": "ready",
                "attention_session_count": 2,
                "pending_session_count": 3,
                "pending_app_target_count": 1,
                "long_horizon_pending_count": 4,
                "regression_cycle_count": 0,
                "latest_sweep_regression_status": "stable",
                "filters": {"pack": "long_horizon_and_replay", "app": "settings"},
                "target_apps": ["settings"],
            },
            {
                "campaign_id": "camp-installer",
                "label": "Installer replay campaign",
                "status": "ready",
                "attention_session_count": 0,
                "pending_session_count": 4,
                "pending_app_target_count": 2,
                "long_horizon_pending_count": 3,
                "regression_cycle_count": 2,
                "latest_sweep_regression_status": "regression",
                "filters": {"pack": "long_horizon_and_replay", "app": "installer"},
                "target_apps": ["installer"],
            },
            {
                "campaign_id": "camp-vscode",
                "label": "VS Code replay campaign",
                "status": "ready",
                "attention_session_count": 0,
                "pending_session_count": 1,
                "pending_app_target_count": 0,
                "long_horizon_pending_count": 1,
                "regression_cycle_count": 0,
                "latest_sweep_regression_status": "stable",
                "filters": {"pack": "long_horizon_and_replay", "app": "vscode"},
                "target_apps": ["vscode"],
            },
        ],
    }

    monkeypatch.setattr(runner, "lab_campaigns", lambda **_: campaigns_payload)

    def _run_lab_campaign_cycle(**kwargs):  # noqa: ANN001
        campaign_id = str(kwargs.get("campaign_id", "") or "")
        cycle_calls.append(campaign_id)
        if campaign_id == "camp-settings":
            return {
                "status": "success",
                "campaign": {
                    "campaign_id": campaign_id,
                    "label": "Settings replay campaign",
                    "pending_session_count": 2,
                    "attention_session_count": 1,
                    "pending_app_target_count": 1,
                    "long_horizon_pending_count": 2,
                    "latest_sweep_regression_status": "stable",
                    "campaign_priority": "elevated",
                    "trend_summary": {"direction": "improving"},
                },
                "cycle": {"executed_sweep_count": 2, "executed_session_count": 2, "created_session_count": 0, "stop_reason": "stable", "stable": True},
            }
        return {
            "status": "success",
            "campaign": {
                "campaign_id": campaign_id,
                "label": "Installer replay campaign",
                "pending_session_count": 3,
                "attention_session_count": 0,
                "pending_app_target_count": 1,
                "long_horizon_pending_count": 1,
                "latest_sweep_regression_status": "regression",
                "campaign_priority": "critical",
                "trend_summary": {"direction": "regressing"},
            },
            "cycle": {"executed_sweep_count": 2, "executed_session_count": 1, "created_session_count": 1, "stop_reason": "max_sweeps_reached", "stable": False},
        }

    monkeypatch.setattr(runner, "run_lab_campaign_cycle", _run_lab_campaign_cycle)
    monkeypatch.setattr(runner, "native_control_targets", lambda **_: {"status": "success", "target_apps": []})
    monkeypatch.setattr(runner, "control_guidance", lambda: {"status": "success", "focus_summary": ["campaign_watchdog"]})

    payload = runner.run_lab_campaign_watchdog(
        max_campaigns=2,
        max_sweeps_per_campaign=2,
        max_sessions=2,
        max_replays_per_session=2,
        history_limit=6,
        pack="long_horizon_and_replay",
    )

    assert payload["status"] == "success"
    assert payload["targeted_campaign_count"] == 2
    assert payload["executed_campaign_count"] == 2
    assert payload["executed_sweep_count"] == 4
    assert payload["stable_campaign_count"] == 1
    assert payload["regression_campaign_count"] == 1
    assert payload["cycle_stop_reason_counts"]["stable"] == 1
    assert payload["cycle_stop_reason_counts"]["max_sweeps_reached"] == 1
    assert payload["trend_direction_counts"]["improving"] == 1
    assert payload["trend_direction_counts"]["regressing"] == 1
    assert cycle_calls == ["camp-settings", "camp-installer"]
    assert payload["results"][0]["campaign_id"] == "camp-settings"
    assert payload["results"][1]["campaign_id"] == "camp-installer"


def test_evaluation_runner_campaign_watchdog_returns_idle_for_no_matching_campaigns(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4, lab_memory=DesktopBenchmarkLabMemory())
    monkeypatch.setattr(
        runner,
        "lab_campaigns",
        lambda **_: {
            "status": "success",
            "count": 1,
            "items": [
                {
                    "campaign_id": "camp-installer",
                    "label": "Installer replay campaign",
                    "status": "ready",
                    "filters": {"pack": "installer_and_governance", "app": "installer"},
                    "target_apps": ["installer"],
                }
            ],
        },
    )
    monkeypatch.setattr(runner, "native_control_targets", lambda **_: {"status": "success", "target_apps": []})
    monkeypatch.setattr(runner, "control_guidance", lambda: {"status": "success"})

    payload = runner.run_lab_campaign_watchdog(pack="long_horizon_and_replay", app_name="settings")

    assert payload["status"] == "idle"
    assert payload["targeted_campaign_count"] == 0
    assert payload["executed_campaign_count"] == 0
    assert payload["results"] == []


def test_evaluation_runner_campaign_watchdog_auto_creates_campaigns_from_native_targets(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4, lab_memory=DesktopBenchmarkLabMemory())
    campaign_rows: list[dict] = []
    create_calls: list[dict] = []
    cycle_calls: list[str] = []

    def _lab_campaigns(**kwargs):  # noqa: ANN001
        normalized_status = str(kwargs.get("status", "") or "").strip().lower()
        items = [dict(item) for item in campaign_rows]
        if normalized_status:
            items = [
                item
                for item in items
                if str(item.get("status", "") or "").strip().lower() == normalized_status
            ]
        return {"status": "success", "count": len(items), "items": items}

    def _create_lab_campaign(**kwargs):  # noqa: ANN001
        create_calls.append(dict(kwargs))
        target_app = str(kwargs.get("app", "") or "").strip().lower() or "settings"
        pack = str(kwargs.get("pack", "") or "").strip() or "long_horizon_and_replay"
        campaign = {
            "campaign_id": f"camp-{target_app}",
            "label": f"{target_app} replay campaign",
            "status": "ready",
            "filters": {"pack": pack, "app": target_app},
            "target_apps": [target_app],
            "pending_session_count": 1,
            "attention_session_count": 0,
            "pending_app_target_count": 1,
            "long_horizon_pending_count": 2,
            "regression_cycle_count": 0,
            "sweep_count": 0,
        }
        campaign_rows.append(campaign)
        return {"status": "success", "campaign": campaign, "created_session_count": 1}

    def _run_lab_campaign_cycle(**kwargs):  # noqa: ANN001
        campaign_id = str(kwargs.get("campaign_id", "") or "").strip()
        cycle_calls.append(campaign_id)
        updated = next(
            dict(item)
            for item in campaign_rows
            if str(item.get("campaign_id", "") or "").strip() == campaign_id
        )
        updated["pending_session_count"] = 0
        updated["pending_app_target_count"] = 0
        updated["long_horizon_pending_count"] = 1
        updated["sweep_count"] = 1
        updated["latest_sweep_regression_status"] = "stable"
        for index, item in enumerate(campaign_rows):
            if str(item.get("campaign_id", "") or "").strip() == campaign_id:
                campaign_rows[index] = dict(updated)
                break
        return {
            "status": "success",
            "campaign": updated,
            "cycle": {"executed_sweep_count": 1, "executed_session_count": 1, "created_session_count": 0, "stop_reason": "stable", "stable": True},
        }

    monkeypatch.setattr(runner, "lab_campaigns", _lab_campaigns)
    monkeypatch.setattr(runner, "create_lab_campaign", _create_lab_campaign)
    monkeypatch.setattr(runner, "run_lab_campaign_cycle", _run_lab_campaign_cycle)
    monkeypatch.setattr(
        runner,
        "native_control_targets",
        lambda **_: {
            "status": "success",
            "target_apps": [
                {
                    "app_name": "settings",
                    "packs": ["long_horizon_and_replay"],
                    "campaign_pressure": 1.3,
                    "replay_pressure": 0.9,
                    "max_horizon_steps": 5,
                }
            ],
        },
    )
    monkeypatch.setattr(runner, "control_guidance", lambda: {"status": "success", "focus_summary": ["campaign_auto_create"]})

    payload = runner.run_lab_campaign_watchdog(
        max_campaigns=1,
        max_sessions=2,
        max_replays_per_session=2,
        history_limit=6,
        pack="long_horizon_and_replay",
        trigger_source="daemon",
    )

    assert payload["status"] == "success"
    assert payload["auto_created_campaign_count"] == 1
    assert payload["executed_campaign_count"] == 1
    assert payload["executed_sweep_count"] == 1
    assert payload["stable_campaign_count"] == 1
    assert payload["auto_created_app_names"] == ["settings"]
    assert "auto-created 1 replay campaign(s)" in str(payload.get("message", ""))
    assert create_calls and create_calls[0]["app"] == "settings"
    assert cycle_calls == ["camp-settings"]


def test_evaluation_runner_program_watchdog_prioritizes_attention_and_regressions(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4, lab_memory=DesktopBenchmarkLabMemory())
    cycle_calls: list[str] = []

    programs_payload = {
        "status": "success",
        "count": 3,
        "items": [
            {
                "program_id": "prog-settings",
                "label": "Settings replay program",
                "status": "ready",
                "program_pressure_score": 2.1,
                "attention_campaign_count": 2,
                "pending_campaign_count": 3,
                "pending_session_count": 4,
                "pending_app_target_count": 1,
                "long_horizon_pending_count": 5,
                "regression_cycle_count": 0,
                "latest_cycle_status": "stable",
                "filters": {"pack": "long_horizon_and_replay", "app": "settings"},
                "target_apps": ["settings"],
            },
            {
                "program_id": "prog-installer",
                "label": "Installer replay program",
                "status": "ready",
                "program_pressure_score": 1.9,
                "attention_campaign_count": 0,
                "pending_campaign_count": 4,
                "pending_session_count": 3,
                "pending_app_target_count": 2,
                "long_horizon_pending_count": 2,
                "regression_cycle_count": 2,
                "latest_cycle_status": "regression",
                "filters": {"pack": "long_horizon_and_replay", "app": "installer"},
                "target_apps": ["installer"],
            },
            {
                "program_id": "prog-vscode",
                "label": "VS Code replay program",
                "status": "ready",
                "program_pressure_score": 0.7,
                "attention_campaign_count": 0,
                "pending_campaign_count": 1,
                "pending_session_count": 1,
                "pending_app_target_count": 0,
                "long_horizon_pending_count": 1,
                "regression_cycle_count": 0,
                "latest_cycle_status": "stable",
                "filters": {"pack": "long_horizon_and_replay", "app": "vscode"},
                "target_apps": ["vscode"],
            },
        ],
    }

    monkeypatch.setattr(runner, "lab_programs", lambda **_: programs_payload)
    monkeypatch.setattr(runner, "lab_campaigns", lambda **_: {"status": "success", "count": 0, "items": []})

    def _run_lab_program_cycle(**kwargs):  # noqa: ANN001
        program_id = str(kwargs.get("program_id", "") or "")
        cycle_calls.append(program_id)
        if program_id == "prog-settings":
            return {
                "status": "success",
                "program": {
                    "program_id": program_id,
                    "label": "Settings replay program",
                    "pending_campaign_count": 1,
                    "attention_campaign_count": 1,
                    "pending_session_count": 2,
                    "pending_app_target_count": 1,
                    "long_horizon_pending_count": 2,
                    "latest_cycle_status": "stable",
                    "program_priority": "elevated",
                    "trend_summary": {"direction": "improving"},
                },
                "cycle": {"executed_campaign_count": 2, "executed_sweep_count": 3, "stop_reason": "stable"},
            }
        return {
            "status": "success",
            "program": {
                "program_id": program_id,
                "label": "Installer replay program",
                "pending_campaign_count": 2,
                "attention_campaign_count": 0,
                "pending_session_count": 2,
                "pending_app_target_count": 1,
                "long_horizon_pending_count": 1,
                "latest_cycle_status": "regression",
                "program_priority": "critical",
                "trend_summary": {"direction": "regressing"},
            },
            "cycle": {"executed_campaign_count": 3, "executed_sweep_count": 4, "stop_reason": "max_campaigns_reached"},
        }

    monkeypatch.setattr(runner, "run_lab_program_cycle", _run_lab_program_cycle)
    monkeypatch.setattr(runner, "native_control_targets", lambda **_: {"status": "success", "target_apps": []})
    monkeypatch.setattr(runner, "control_guidance", lambda: {"status": "success", "focus_summary": ["program_watchdog"]})

    payload = runner.run_lab_program_watchdog(
        max_programs=2,
        max_campaigns_per_program=3,
        max_sweeps_per_campaign=2,
        max_sessions=2,
        max_replays_per_session=2,
        history_limit=6,
        pack="long_horizon_and_replay",
    )

    assert payload["status"] == "success"
    assert payload["targeted_program_count"] == 2
    assert payload["executed_program_count"] == 2
    assert payload["executed_campaign_count"] == 5
    assert payload["executed_sweep_count"] == 7
    assert payload["stable_program_count"] == 1
    assert payload["regression_program_count"] == 1
    assert payload["cycle_stop_reason_counts"]["stable"] == 1
    assert payload["cycle_stop_reason_counts"]["max_campaigns_reached"] == 1
    assert payload["trend_direction_counts"]["improving"] == 1
    assert payload["trend_direction_counts"]["regressing"] == 1
    assert cycle_calls == ["prog-settings", "prog-installer"]


def test_evaluation_runner_program_watchdog_auto_creates_programs_from_native_targets(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4, lab_memory=DesktopBenchmarkLabMemory())
    program_rows: list[dict] = []
    create_calls: list[dict] = []
    cycle_calls: list[str] = []

    def _lab_programs(**kwargs):  # noqa: ANN001
        normalized_status = str(kwargs.get("status", "") or "").strip().lower()
        items = [dict(item) for item in program_rows]
        if normalized_status:
            items = [item for item in items if str(item.get("status", "") or "").strip().lower() == normalized_status]
        return {"status": "success", "count": len(items), "items": items}

    def _create_lab_program(**kwargs):  # noqa: ANN001
        create_calls.append(dict(kwargs))
        target_app = str(kwargs.get("app", "") or "").strip().lower() or "settings"
        pack = str(kwargs.get("pack", "") or "").strip() or "long_horizon_and_replay"
        program = {
            "program_id": f"prog-{target_app}",
            "label": f"{target_app} replay program",
            "status": "ready",
            "filters": {"pack": pack, "app": target_app},
            "target_apps": [target_app],
            "pending_campaign_count": 1,
            "attention_campaign_count": 0,
            "pending_session_count": 1,
            "pending_app_target_count": 1,
            "long_horizon_pending_count": 2,
            "program_pressure_score": 1.2,
            "cycle_count": 0,
        }
        program_rows.append(program)
        return {"status": "success", "program": program, "created_campaign_count": 1, "created_session_count": 1}

    def _run_lab_program_cycle(**kwargs):  # noqa: ANN001
        program_id = str(kwargs.get("program_id", "") or "").strip()
        cycle_calls.append(program_id)
        updated = next(dict(item) for item in program_rows if str(item.get("program_id", "") or "").strip() == program_id)
        updated["pending_campaign_count"] = 0
        updated["pending_session_count"] = 0
        updated["pending_app_target_count"] = 0
        updated["long_horizon_pending_count"] = 1
        updated["latest_cycle_status"] = "stable"
        for index, item in enumerate(program_rows):
            if str(item.get("program_id", "") or "").strip() == program_id:
                program_rows[index] = dict(updated)
                break
        return {
            "status": "success",
            "program": updated,
            "cycle": {"executed_campaign_count": 1, "executed_sweep_count": 1, "stop_reason": "stable"},
        }

    monkeypatch.setattr(runner, "lab_programs", _lab_programs)
    monkeypatch.setattr(runner, "lab_campaigns", lambda **_: {"status": "success", "count": 0, "items": []})
    monkeypatch.setattr(runner, "create_lab_program", _create_lab_program)
    monkeypatch.setattr(runner, "run_lab_program_cycle", _run_lab_program_cycle)
    monkeypatch.setattr(
        runner,
        "native_control_targets",
        lambda **_: {
            "status": "success",
            "target_apps": [
                {
                    "app_name": "settings",
                    "packs": ["long_horizon_and_replay"],
                    "campaign_pressure": 1.4,
                    "replay_pressure": 0.9,
                    "max_horizon_steps": 5,
                }
            ],
        },
    )
    monkeypatch.setattr(runner, "control_guidance", lambda: {"status": "success", "focus_summary": ["program_auto_create"]})

    payload = runner.run_lab_program_watchdog(
        max_programs=1,
        max_campaigns_per_program=2,
        max_sweeps_per_campaign=2,
        max_sessions=2,
        max_replays_per_session=2,
        history_limit=6,
        pack="long_horizon_and_replay",
        trigger_source="daemon",
    )

    assert payload["status"] == "success"
    assert payload["auto_created_program_count"] == 1
    assert payload["executed_program_count"] == 1
    assert payload["executed_campaign_count"] == 1
    assert payload["executed_sweep_count"] == 1
    assert payload["stable_program_count"] == 1
    assert payload["auto_created_app_names"] == ["settings"]
    assert create_calls and create_calls[0]["app"] == "settings"
    assert cycle_calls == ["prog-settings"]


def test_evaluation_runner_creates_and_cycles_lab_program(monkeypatch, tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_memory.json"))
    runner = EvaluationRunner(history_limit=4, lab_memory=memory)

    def _lab(**kwargs):  # noqa: ANN001
        filters = {key: value for key, value in kwargs.items() if value not in {"", None}}
        return {
            "status": "success",
            "filters": filters,
            "latest_summary": {"weighted_score": 0.78, "weighted_pass_rate": 0.8},
            "latest_regression": {"status": "stable"},
            "latest_run": {"executed_at": "2026-03-20T10:00:00+00:00"},
            "catalog_summary": {"scenario_count": 4},
            "coverage": {"long_horizon": {"count": 2, "ratio": 0.5}},
            "history_trend": {"direction": "warming", "run_count": 3},
            "replay_candidates": [
                {
                    "scenario": "settings_long_horizon_replay",
                    "apps": ["settings"],
                    "horizon_steps": 6,
                    "replay_status": "pending",
                    "replay_query": {"scenario_name": "settings_long_horizon_replay", "limit": 1},
                }
            ],
        }

    def _native_targets(**kwargs):  # noqa: ANN001
        app_name = str(kwargs.get("app", "") or kwargs.get("app_name", "") or "").strip()
        if app_name:
            target_apps = [{"app_name": app_name, "priority": 1.0}]
        else:
            target_apps = [{"app_name": "settings", "priority": 1.0}, {"app_name": "vscode", "priority": 0.9}]
        return {
            "status": "success",
            "focus_summary": ["long_horizon_and_replay", "desktop_workflow"],
            "target_apps": target_apps,
            "strongest_tactics": {"descendant_focus": 0.86, "native_focus": 0.72},
            "coverage_gap_apps": ["outlook"],
        }

    monkeypatch.setattr(runner, "lab", _lab)
    monkeypatch.setattr(runner, "native_control_targets", _native_targets)
    monkeypatch.setattr(
        runner,
        "control_guidance",
        lambda: {"status": "success", "focus_summary": ["desktop_workflow"], "control_biases": {"native_focus": 0.7}},
    )

    created = runner.create_lab_program(
        pack="long_horizon_and_replay",
        source="unit_test",
        label="desktop replay program",
        max_campaigns=2,
        max_sessions_per_campaign=1,
    )

    assert created["status"] == "success"
    assert created["created_campaign_count"] == 2
    program = created["program"]
    assert program["campaign_count"] == 2
    assert program["target_app_count"] == 2

    def _cycle_campaign(*, campaign_id: str, **kwargs):  # noqa: ANN001
        del kwargs
        campaign = memory.get_campaign(campaign_id)["campaign"]
        return {
            "status": "success",
            "campaign": {
                **dict(campaign),
                "status": "complete",
                "pending_session_count": 0,
                "attention_session_count": 0,
                "pending_app_target_count": 0,
                "latest_sweep_score": 0.9,
                "latest_sweep_pass_rate": 0.92,
                "latest_sweep_regression_status": "stable",
            },
            "cycle": {"executed_sweep_count": 2, "stop_reason": "stable"},
        }

    monkeypatch.setattr(runner, "run_lab_campaign_cycle", _cycle_campaign)

    cycled = runner.run_lab_program_cycle(
        program_id=str(program["program_id"]),
        max_campaigns=2,
        max_sweeps_per_campaign=2,
        max_sessions=1,
        max_replays_per_session=1,
        history_limit=4,
    )

    assert cycled["status"] == "success"
    assert cycled["created_campaign_count"] == 0
    assert len(cycled["results"]) == 2
    assert cycled["program"]["cycle_count"] == 1
    assert cycled["program"]["latest_cycle_stop_reason"] in {"stable", "max_campaigns_reached"}
    assert cycled["cycle"]["executed_campaign_count"] == 2


def test_evaluation_runner_creates_and_cycles_lab_portfolio(monkeypatch, tmp_path) -> None:
    memory = DesktopBenchmarkLabMemory(store_path=str(tmp_path / "benchmark_lab_memory.json"))
    runner = EvaluationRunner(history_limit=4, lab_memory=memory)

    def _lab(**kwargs):  # noqa: ANN001
        filters = {key: value for key, value in kwargs.items() if value not in {"", None}}
        return {
            "status": "success",
            "filters": filters,
            "latest_summary": {"weighted_score": 0.8, "weighted_pass_rate": 0.82},
            "latest_regression": {"status": "stable"},
            "catalog_summary": {"scenario_count": 4},
            "history_trend": {"direction": "warming", "run_count": 4},
            "replay_candidates": [
                {
                    "scenario": "settings_long_horizon_replay",
                    "apps": ["settings"],
                    "horizon_steps": 6,
                    "replay_status": "pending",
                    "replay_query": {"scenario_name": "settings_long_horizon_replay", "limit": 1},
                }
            ],
        }

    def _native_targets(**kwargs):  # noqa: ANN001
        app_name = str(kwargs.get("app", "") or kwargs.get("app_name", "") or "").strip()
        if app_name:
            target_apps = [{"app_name": app_name, "priority": 1.0}]
        else:
            target_apps = [{"app_name": "settings", "priority": 1.0}, {"app_name": "vscode", "priority": 0.9}]
        return {
            "status": "success",
            "focus_summary": ["long_horizon_and_replay", "desktop_workflow"],
            "target_apps": target_apps,
            "strongest_tactics": {"descendant_focus": 0.86, "native_focus": 0.74},
            "coverage_gap_apps": ["outlook"],
        }

    monkeypatch.setattr(runner, "lab", _lab)
    monkeypatch.setattr(runner, "native_control_targets", _native_targets)
    monkeypatch.setattr(
        runner,
        "control_guidance",
        lambda: {"status": "success", "focus_summary": ["desktop_workflow"], "control_biases": {"native_focus": 0.72}},
    )

    created = runner.create_lab_portfolio(
        pack="long_horizon_and_replay",
        source="unit_test",
        label="desktop replay portfolio",
        max_programs=2,
        max_campaigns_per_program=2,
        max_sessions_per_campaign=1,
    )

    assert created["status"] == "success"
    assert created["created_program_count"] == 2
    portfolio = created["portfolio"]
    assert portfolio["program_count"] == 2
    assert portfolio["target_app_count"] == 2

    def _cycle_program(*, program_id: str, **kwargs):  # noqa: ANN001
        del kwargs
        program = memory.get_program(program_id)["program"]
        return {
            "status": "success",
            "program": {
                **dict(program),
                "status": "complete",
                "pending_campaign_count": 0,
                "attention_campaign_count": 0,
                "pending_session_count": 0,
                "pending_app_target_count": 0,
                "latest_cycle_status": "stable",
                "trend_summary": {"direction": "improving"},
            },
            "cycle": {"executed_campaign_count": 2, "executed_sweep_count": 3, "stop_reason": "stable", "stable": True},
        }

    monkeypatch.setattr(runner, "run_lab_program_cycle", _cycle_program)

    cycled = runner.run_lab_portfolio_cycle(
        portfolio_id=str(portfolio["portfolio_id"]),
        max_programs=2,
        max_campaigns_per_program=2,
        max_sweeps_per_campaign=2,
        max_sessions=1,
        max_replays_per_session=1,
        history_limit=4,
    )

    assert cycled["status"] == "success"
    assert cycled["created_program_count"] == 0
    assert len(cycled["results"]) == 2
    assert cycled["portfolio"]["wave_count"] == 1
    assert cycled["portfolio"]["latest_wave_stop_reason"] in {"stable", "max_programs_reached"}
    assert cycled["wave"]["executed_program_count"] == 2


def test_evaluation_runner_portfolio_campaign_records_multi_wave_progress(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4, lab_memory=DesktopBenchmarkLabMemory())
    created = runner.lab_memory.record_portfolio(
        filters={"pack": "long_horizon_and_replay", "app": "settings"},
        lab_payload={"latest_summary": {"weighted_score": 0.7}},
        native_targets_payload={"target_apps": [{"app_name": "settings"}]},
        guidance_payload={"focus_summary": ["portfolio_campaign"]},
        source="unit_test",
        label="settings replay portfolio",
        program_ids=[],
        app_targets=["settings"],
        program_rows=[],
    )
    portfolio_id = str(created["portfolio"]["portfolio_id"])
    cycle_calls: list[str] = []
    pending_program_values = [1, 0]

    def _run_lab_portfolio_cycle(**kwargs):  # noqa: ANN001
        cycle_calls.append(str(kwargs.get("portfolio_id", "")))
        pending_program_count = pending_program_values.pop(0)
        return {
            "status": "success",
            "portfolio": {
                "portfolio_id": portfolio_id,
                "label": "settings replay portfolio",
                "status": "complete" if pending_program_count == 0 else "attention",
                "filters": {"pack": "long_horizon_and_replay", "app": "settings"},
                "program_ids": [],
                "programs": [],
                "target_apps": ["settings"],
                "pending_program_count": pending_program_count,
                "attention_program_count": 0,
                "pending_campaign_count": pending_program_count,
                "attention_campaign_count": 0,
                "pending_session_count": pending_program_count,
                "pending_app_target_count": pending_program_count,
                "long_horizon_pending_count": pending_program_count,
                "latest_wave_status": "success",
                "latest_wave_stop_reason": "stable" if pending_program_count == 0 else "max_programs_reached",
                "latest_wave_trend_direction": "improving" if pending_program_count == 0 else "warming",
                "lab_snapshot": {"latest_summary": {"weighted_score": 0.85}},
                "native_targets_snapshot": {"target_apps": [{"app_name": "settings"}]},
                "guidance_snapshot": {"focus_summary": ["portfolio_campaign"]},
            },
            "wave": {
                "executed_at": "2026-01-01T00:00:00+00:00",
                "executed_program_count": 1,
                "executed_campaign_count": 2,
                "executed_sweep_count": 3,
                "weighted_score": 0.84 if pending_program_count == 0 else 0.72,
                "weighted_pass_rate": 0.88 if pending_program_count == 0 else 0.74,
                "stop_reason": "stable" if pending_program_count == 0 else "max_programs_reached",
                "trend_direction": "improving" if pending_program_count == 0 else "warming",
            },
            "lab": {"latest_summary": {"weighted_score": 0.85}},
            "native_targets": {"target_apps": [{"app_name": "settings"}]},
            "guidance": {"focus_summary": ["portfolio_campaign"]},
        }

    monkeypatch.setattr(runner, "run_lab_portfolio_cycle", _run_lab_portfolio_cycle)

    payload = runner.run_lab_portfolio_campaign(
        portfolio_id=portfolio_id,
        max_waves=3,
        max_programs=2,
        max_campaigns_per_program=2,
        max_sweeps_per_campaign=2,
        max_sessions=1,
        max_replays_per_session=1,
        history_limit=4,
    )

    assert payload["status"] == "success"
    assert payload["executed_wave_count"] == 2
    assert payload["stop_reason"] == "stable"
    assert payload["portfolio"]["campaign_count"] == 1
    assert payload["portfolio"]["latest_campaign_stop_reason"] == "stable"
    assert payload["campaign"]["executed_wave_count"] == 2
    assert cycle_calls == [portfolio_id, portfolio_id]


def test_evaluation_runner_portfolio_watchdog_auto_creates_portfolios(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4, lab_memory=DesktopBenchmarkLabMemory())
    portfolio_rows: list[dict] = []
    create_calls: list[dict] = []
    cycle_calls: list[str] = []

    def _lab_portfolios(**kwargs):  # noqa: ANN001
        normalized_status = str(kwargs.get("status", "") or "").strip().lower()
        items = [dict(item) for item in portfolio_rows]
        if normalized_status:
            items = [item for item in items if str(item.get("status", "") or "").strip().lower() == normalized_status]
        return {"status": "success", "count": len(items), "items": items}

    def _create_lab_portfolio(**kwargs):  # noqa: ANN001
        create_calls.append(dict(kwargs))
        target_app = str(kwargs.get("app", "") or "").strip().lower() or "settings"
        pack = str(kwargs.get("pack", "") or "").strip() or "long_horizon_and_replay"
        portfolio = {
            "portfolio_id": f"portfolio-{target_app}",
            "label": f"{target_app} replay portfolio",
            "status": "ready",
            "filters": {"pack": pack, "app": target_app},
            "target_apps": [target_app],
            "pending_program_count": 1,
            "attention_program_count": 0,
            "pending_campaign_count": 1,
            "pending_session_count": 2,
            "pending_app_target_count": 1,
            "long_horizon_pending_count": 2,
            "portfolio_pressure_score": 1.4,
            "wave_count": 0,
        }
        portfolio_rows.append(portfolio)
        return {"status": "success", "portfolio": portfolio, "created_program_count": 1, "created_campaign_count": 1, "created_session_count": 1}

    def _run_lab_portfolio_cycle(**kwargs):  # noqa: ANN001
        portfolio_id = str(kwargs.get("portfolio_id", "") or "").strip()
        cycle_calls.append(portfolio_id)
        updated = next(dict(item) for item in portfolio_rows if str(item.get("portfolio_id", "") or "").strip() == portfolio_id)
        updated["pending_program_count"] = 0
        updated["pending_campaign_count"] = 0
        updated["pending_session_count"] = 0
        updated["pending_app_target_count"] = 0
        updated["long_horizon_pending_count"] = 1
        updated["latest_wave_status"] = "stable"
        for index, item in enumerate(portfolio_rows):
            if str(item.get("portfolio_id", "") or "").strip() == portfolio_id:
                portfolio_rows[index] = dict(updated)
                break
        return {
            "status": "success",
            "portfolio": updated,
            "wave": {"executed_program_count": 1, "executed_campaign_count": 1, "executed_sweep_count": 1, "stop_reason": "stable"},
        }

    monkeypatch.setattr(runner, "lab_portfolios", _lab_portfolios)
    monkeypatch.setattr(runner, "create_lab_portfolio", _create_lab_portfolio)
    monkeypatch.setattr(runner, "run_lab_portfolio_cycle", _run_lab_portfolio_cycle)
    monkeypatch.setattr(
        runner,
        "native_control_targets",
        lambda **_: {
            "status": "success",
            "target_apps": [
                {
                    "app_name": "settings",
                    "packs": ["long_horizon_and_replay"],
                    "campaign_pressure": 1.4,
                    "program_pressure": 1.1,
                    "portfolio_pressure": 0.9,
                    "max_horizon_steps": 5,
                }
            ],
        },
    )
    monkeypatch.setattr(runner, "control_guidance", lambda: {"status": "success", "focus_summary": ["portfolio_auto_create"]})

    payload = runner.run_lab_portfolio_watchdog(
        max_portfolios=1,
        max_programs_per_portfolio=2,
        max_campaigns_per_program=2,
        max_sweeps_per_campaign=2,
        max_sessions=2,
        max_replays_per_session=2,
        history_limit=6,
        adaptive_budgeting=True,
        adaptive_goal="stabilize",
        pack="long_horizon_and_replay",
        trigger_source="daemon",
    )

    assert payload["status"] == "success"
    assert payload["auto_created_portfolio_count"] == 1
    assert payload["executed_portfolio_count"] == 1
    assert payload["executed_wave_count"] == 1
    assert payload["executed_program_count"] == 1
    assert payload["executed_campaign_count"] == 1
    assert payload["executed_sweep_count"] == 1
    assert payload["stable_portfolio_count"] == 1
    assert payload["campaign_stop_reason_counts"]["stable"] == 1
    assert payload["auto_created_app_names"] == ["settings"]
    assert payload["adaptive_budgeting"] is True
    assert payload["adaptive_goal"] == "stabilize"
    assert payload["adaptive_portfolio_count"] == 1
    assert payload["planned_wave_budget_total"] >= 1
    assert payload["budget_profile_counts"]["stabilize"] >= 1
    assert create_calls and create_calls[0]["app"] == "settings"
    assert cycle_calls == ["portfolio-settings"]


def test_evaluation_runner_portfolio_diagnostics_surfaces_hotspots(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4)

    monkeypatch.setattr(
        runner,
        "lab_portfolios",
        lambda **_: {
            "status": "success",
            "count": 2,
            "total": 2,
            "items": [
                {
                    "portfolio_id": "portfolio-settings",
                    "label": "settings replay portfolio",
                    "status": "attention",
                    "portfolio_pressure_score": 4.5,
                    "latest_wave_stop_reason": "regression",
                    "pending_program_count": 2,
                    "pending_campaign_count": 3,
                    "pending_session_count": 4,
                    "pending_app_target_count": 1,
                }
            ],
            "top_portfolios": [
                {
                    "portfolio_id": "portfolio-settings",
                    "label": "settings replay portfolio",
                    "status": "attention",
                    "portfolio_priority": "critical",
                    "portfolio_pressure_score": 4.5,
                    "latest_wave_stop_reason": "regression",
                    "latest_campaign_status": "success",
                    "latest_campaign_stop_reason": "regression_detected",
                    "campaign_count": 2,
                    "target_apps": ["settings"],
                    "focus_summary": ["desktop_workflow"],
                }
            ],
            "summary": {
                "portfolio_pressure_total": 5.2,
                "portfolio_pressure_avg": 2.6,
                "pending_programs": 2,
                "attention_programs": 1,
                "pending_campaigns": 3,
                "pending_sessions": 4,
                "pending_app_targets": 1,
                "long_horizon_pending_count": 2,
                "stable_waves": 0,
                "regression_waves": 1,
                "campaign_count": 2,
                "stable_campaigns": 0,
                "regression_campaigns": 1,
                "wave_stop_reason_counts": {"regression": 1, "stable": 1},
                "campaign_stop_reason_counts": {"regression_detected": 1, "stable": 1},
                "focus_summary_counts": {"desktop_workflow": 2, "native_focus": 1},
                "trend_direction_counts": {"regression": 1, "stable": 1},
                "campaign_trend_direction_counts": {"regressing": 1, "stable": 1},
                "latest_campaign_status_counts": {"success": 1, "idle": 1},
                "app_target_counts": {"settings": 2, "vscode": 1},
            },
        },
    )
    monkeypatch.setattr(
        runner,
        "native_control_targets",
        lambda **_: {
            "status": "success",
            "target_apps": [
                {
                    "app_name": "settings",
                    "priority": 1.0,
                    "portfolio_pressure": 3.0,
                    "program_pressure": 1.0,
                    "campaign_pressure": 0.5,
                    "replay_pressure": 0.25,
                    "portfolio_pending_program_count": 2,
                    "portfolio_pending_campaign_count": 3,
                    "portfolio_pending_session_count": 4,
                    "portfolio_pending_app_target_count": 1,
                    "portfolio_latest_wave_status": "attention",
                    "portfolio_latest_wave_stop_reason": "regression",
                    "campaign_focus_summary": ["desktop_workflow"],
                    "portfolio_hint_query": "settings bluetooth dialog",
                }
            ],
            "coverage_gap_apps": ["outlook"],
        },
    )
    monkeypatch.setattr(
        runner,
        "control_guidance",
        lambda: {
            "status": "success",
            "focus_summary": ["desktop_workflow"],
            "control_biases": {"native_focus": 0.7},
        },
    )

    payload = runner.lab_portfolio_diagnostics(limit=6, history_limit=8)

    assert payload["status"] == "success"
    assert payload["summary"]["top_app_name"] == "settings"
    assert payload["summary"]["top_stop_reason"] == "regression"
    assert payload["summary"]["top_campaign_stop_reason"] == "regression_detected"
    assert payload["backlog"]["pending_programs"] == 2
    assert payload["backlog"]["campaigns"] == 2
    assert payload["top_portfolios"][0]["portfolio_id"] == "portfolio-settings"
    assert payload["app_pressure_leaderboard"][0]["app_name"] == "settings"
    assert payload["stop_reason_leaderboard"][0]["stop_reason"] == "regression"
    assert payload["campaign_stop_reason_leaderboard"][0]["stop_reason"] == "regression_detected"
    assert payload["focus_leaderboard"][0]["focus_area"] == "desktop_workflow"
    assert payload["cycle_plans"][0]["budget_profile"] == "stabilize"
    assert payload["daemon_recommendation"]["adaptive_budgeting"] is True
    assert payload["daemon_recommendation"]["max_waves_per_portfolio"] >= 1


def test_evaluation_runner_native_control_targets_aggregates_portfolio_native_signals(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=6, lab_memory=DesktopBenchmarkLabMemory())

    monkeypatch.setattr(
        runner,
        "lab",
        lambda **_: {
            "status": "success",
            "coverage": {"long_horizon": {"count": 1}},
            "replay_candidates": [],
            "installed_app_coverage": {"missing_apps": []},
            "history_trend": {"run_count": 0},
            "filters": {},
        },
    )
    monkeypatch.setattr(runner, "_select_scenarios", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(runner, "lab_sessions", lambda **_: {"status": "success", "items": []})
    monkeypatch.setattr(runner, "lab_campaigns", lambda **_: {"status": "success", "items": []})
    monkeypatch.setattr(runner, "lab_programs", lambda **_: {"status": "success", "items": []})
    monkeypatch.setattr(
        runner,
        "lab_portfolios",
        lambda **_: {
            "status": "success",
            "items": [
                {
                    "portfolio_id": "portfolio-settings",
                    "label": "settings replay portfolio",
                    "filters": {"app": "settings"},
                    "program_count": 2,
                    "wave_count": 3,
                    "pending_program_count": 1,
                    "attention_program_count": 1,
                    "pending_campaign_count": 1,
                    "pending_session_count": 2,
                    "pending_app_target_count": 1,
                    "regression_wave_count": 2,
                    "completed_campaign_count": 4,
                    "stable_campaign_count": 2,
                    "regression_campaign_count": 3,
                    "stable_campaign_streak": 1,
                    "regression_campaign_streak": 2,
                    "long_horizon_pending_count": 1,
                    "latest_wave_status": "failed",
                    "latest_wave_stop_reason": "regression_attention",
                    "latest_campaign_status": "failed",
                    "latest_campaign_stop_reason": "confirm_pairing_attention",
                    "latest_campaign_trend_direction": "regressing",
                    "native_targets_snapshot": {
                        "target_apps": [
                            {
                                "app_name": "settings",
                                "priority": 2.2,
                                "control_biases": {
                                    "dialog_resolution": 0.72,
                                    "descendant_focus": 0.94,
                                    "recovery_reacquire": 0.83,
                                    "native_focus": 0.91,
                                },
                                "portfolio_pressure": 1.4,
                                "portfolio_hint_query": "confirm pairing | allow device",
                                "portfolio_descendant_title_hints": ["Confirm pairing", "Allow device"],
                                "portfolio_descendant_title_sequence": ["Confirm pairing", "Allow device"],
                                "portfolio_descendant_hint_query": "Confirm pairing | Allow device",
                                "portfolio_preferred_window_title": "Allow device",
                                "portfolio_confirmation_pressure": 0.92,
                                "portfolio_confirmation_title_sequence": ["Confirm pairing", "Allow device"],
                                "portfolio_confirmation_hint_query": "confirm pairing | allow device",
                                "portfolio_confirmation_preferred_window_title": "Allow device",
                            }
                        ]
                    },
                }
            ],
        },
    )

    native_targets = runner.native_control_targets(app="settings", history_limit=6)

    assert native_targets["status"] == "success"
    assert native_targets["replay_portfolio_summary"]["portfolio_count"] == 1
    assert native_targets["replay_portfolio_summary"]["latest_portfolio_id"] == "portfolio-settings"
    target_row = next(item for item in native_targets["target_apps"] if str(item.get("app_name", "")) == "settings")
    assert int(target_row["portfolio_count"]) == 1
    assert int(target_row["portfolio_wave_count"]) == 3
    assert float(target_row["portfolio_pressure"]) > 1.0
    assert list(target_row["portfolio_descendant_title_sequence"]) == ["Confirm pairing", "Allow device"]
    assert str(target_row["portfolio_hint_query"]) == "confirm pairing | allow device"
    assert str(target_row["portfolio_preferred_window_title"]) == "Allow device"
    assert float(target_row["portfolio_confirmation_pressure"]) > 0.5
    assert float(target_row["portfolio_campaign_confirmation_pressure"]) >= float(target_row["portfolio_confirmation_pressure"])
    assert list(target_row["portfolio_confirmation_title_sequence"]) == ["Confirm pairing", "Allow device"]
    assert str(target_row["portfolio_confirmation_hint_query"]) == "confirm pairing | allow device"
    assert str(target_row["portfolio_confirmation_preferred_window_title"]) == "Allow device"
    assert int(target_row["portfolio_completed_campaign_count"]) == 4
    assert int(target_row["portfolio_regression_campaign_count"]) == 3
    assert int(target_row["portfolio_regression_campaign_streak"]) == 2
    assert str(target_row["portfolio_latest_campaign_status"]) == "failed"
    assert str(target_row["portfolio_latest_campaign_stop_reason"]) == "confirm_pairing_attention"
    assert str(target_row["portfolio_latest_campaign_trend_direction"]) == "regressing"
    assert float(target_row["control_biases"]["descendant_focus"]) >= 0.94
