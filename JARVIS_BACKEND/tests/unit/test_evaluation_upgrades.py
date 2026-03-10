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
        Scenario("strict_case", "strict", ["open_app", "tts_speak"], weight=2.0, strict_order=True),
        Scenario(
            "flex_case",
            "flex",
            ["external_email_send"],
            weight=1.0,
            strict_order=False,
            required_actions=["external_email_send"],
        ),
    ]

    payload = runner.run_with_summary(scenarios)
    items = payload["items"]
    summary = payload["summary"]

    assert len(items) == 2
    assert all(bool(item["passed"]) for item in items)
    assert float(summary["weighted_pass_rate"]) == 1.0
    assert float(summary["weighted_score"]) > 0.85


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
