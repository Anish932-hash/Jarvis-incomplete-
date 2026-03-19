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
    assert str(vscode_row["descendant_hint_query"]).strip()
    assert str(vscode_row["preferred_window_title"]).strip()
    assert float(vscode_row["replay_pressure"]) > 0.0


def test_evaluation_runner_campaign_watchdog_prioritizes_attention_and_regressions(monkeypatch) -> None:
    runner = EvaluationRunner(history_limit=4, lab_memory=DesktopBenchmarkLabMemory())
    sweep_calls: list[str] = []

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

    def _run_lab_campaign_sweep(**kwargs):  # noqa: ANN001
        campaign_id = str(kwargs.get("campaign_id", "") or "")
        sweep_calls.append(campaign_id)
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
                },
                "sweep": {"executed_session_count": 2, "regression_status": "stable"},
                "created_session_count": 0,
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
            },
            "sweep": {"executed_session_count": 1, "regression_status": "regression"},
            "created_session_count": 1,
        }

    monkeypatch.setattr(runner, "run_lab_campaign_sweep", _run_lab_campaign_sweep)
    monkeypatch.setattr(runner, "native_control_targets", lambda **_: {"status": "success", "target_apps": []})
    monkeypatch.setattr(runner, "control_guidance", lambda: {"status": "success", "focus_summary": ["campaign_watchdog"]})

    payload = runner.run_lab_campaign_watchdog(
        max_campaigns=2,
        max_sessions=2,
        max_replays_per_session=2,
        history_limit=6,
        pack="long_horizon_and_replay",
    )

    assert payload["status"] == "success"
    assert payload["targeted_campaign_count"] == 2
    assert payload["executed_campaign_count"] == 2
    assert payload["regression_campaign_count"] == 1
    assert sweep_calls == ["camp-settings", "camp-installer"]
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
    sweep_calls: list[str] = []

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

    def _run_lab_campaign_sweep(**kwargs):  # noqa: ANN001
        campaign_id = str(kwargs.get("campaign_id", "") or "").strip()
        sweep_calls.append(campaign_id)
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
            "sweep": {"executed_session_count": 1, "regression_status": "stable"},
            "created_session_count": 0,
        }

    monkeypatch.setattr(runner, "lab_campaigns", _lab_campaigns)
    monkeypatch.setattr(runner, "create_lab_campaign", _create_lab_campaign)
    monkeypatch.setattr(runner, "run_lab_campaign_sweep", _run_lab_campaign_sweep)
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
    assert payload["auto_created_app_names"] == ["settings"]
    assert "auto-created 1 replay campaign(s)" in str(payload.get("message", ""))
    assert create_calls and create_calls[0]["app"] == "settings"
    assert sweep_calls == ["camp-settings"]
