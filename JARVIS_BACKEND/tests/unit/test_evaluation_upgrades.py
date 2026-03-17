from __future__ import annotations

import asyncio

from backend.python.core.contracts import ExecutionPlan, PlanStep
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
