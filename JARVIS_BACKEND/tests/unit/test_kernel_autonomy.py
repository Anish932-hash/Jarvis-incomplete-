from __future__ import annotations

import asyncio

from backend.python.core.contracts import ActionResult
from backend.python.core.kernel import AgentKernel


def _configure_store_paths(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("JARVIS_GOAL_STORE", str(tmp_path / "goals.json"))
    monkeypatch.setenv("JARVIS_MISSION_STORE", str(tmp_path / "missions.json"))
    monkeypatch.setenv("JARVIS_RUNTIME_MEMORY_STORE", str(tmp_path / "runtime_memory.jsonl"))
    monkeypatch.setenv("JARVIS_EPISODIC_MEMORY_STORE", str(tmp_path / "episodic_memory.jsonl"))
    monkeypatch.setenv("JARVIS_ROLLBACK_STORE", str(tmp_path / "rollback_journal.json"))
    monkeypatch.setenv("JARVIS_SCHEDULE_STORE", str(tmp_path / "schedules.json"))
    monkeypatch.setenv("JARVIS_TRIGGER_STORE", str(tmp_path / "triggers.json"))
    monkeypatch.setenv("JARVIS_MACRO_STORE", str(tmp_path / "macros.json"))
    monkeypatch.setenv("JARVIS_DESKTOP_ANCHOR_MEMORY_STORE", str(tmp_path / "desktop_anchor_memory.json"))
    monkeypatch.setenv("JARVIS_POLICY_BANDIT_STORE", str(tmp_path / "policy_bandit.json"))
    monkeypatch.setenv("JARVIS_EXECUTION_STRATEGY_STORE", str(tmp_path / "execution_strategy.json"))
    monkeypatch.setenv("JARVIS_EXTERNAL_RELIABILITY_STORE", str(tmp_path / "external_reliability.json"))


def test_explain_goal_returns_actionable_payload(monkeypatch, tmp_path) -> None:
    _configure_store_paths(monkeypatch, tmp_path)
    kernel = AgentKernel()
    goal_id = asyncio.run(kernel.submit_goal("open example site", source="desktop-ui"))
    goal = kernel.get_goal(goal_id)
    assert goal is not None

    goal.results = [
        ActionResult(
            action="open_url",
            status="failed",
            error="request timed out",
            output={"status": "failed"},
            attempt=2,
        )
    ]
    kernel.goal_manager.mark_failed(goal, "open_url: request timed out")

    payload = kernel.explain_goal(goal_id, include_memory_hints=True)

    assert payload["status"] == "success"
    assert payload["goal_id"] == goal_id
    assert payload["goal"]["status"] == "failed"
    assert isinstance(payload["recommendations"], list)
    assert payload["results"]["failed_action_counts"]["open_url"] >= 1


def test_autonomy_report_and_tune(monkeypatch, tmp_path) -> None:
    _configure_store_paths(monkeypatch, tmp_path)
    kernel = AgentKernel()

    goal_ok_id = asyncio.run(kernel.submit_goal("what time is it", source="desktop-ui"))
    goal_ok = kernel.get_goal(goal_ok_id)
    assert goal_ok is not None
    goal_ok.results = [
        ActionResult(
            action="time_now",
            status="success",
            output={"status": "success", "timezone": "UTC"},
            attempt=1,
        )
    ]
    kernel.goal_manager.mark_completed(goal_ok)

    goal_fail_id = asyncio.run(kernel.submit_goal("open failing page", source="desktop-ui"))
    goal_fail = kernel.get_goal(goal_fail_id)
    assert goal_fail is not None
    goal_fail.results = [
        ActionResult(
            action="open_url",
            status="failed",
            error="timeout",
            output={"status": "failed"},
            attempt=1,
        )
    ]
    kernel.goal_manager.mark_failed(goal_fail, "open_url: timeout")

    threshold = max(1, int(kernel.action_circuit_breaker.failure_threshold))
    for _ in range(threshold):
        kernel.action_circuit_breaker.record_outcome(
            action="open_url",
            status="failed",
            failure_category="timeout",
            error="timeout",
        )

    report = kernel.autonomy_report(limit_recent_goals=100)
    assert report["status"] == "success"
    assert report["scores"]["tier"] in {"developing", "medium", "high"}
    assert report["circuit_breakers"]["open_count"] >= 1
    assert isinstance(report.get("memory", {}).get("desktop_anchor_memory", {}), dict)
    assert "quarantine_count" in report["memory"]["desktop_anchor_memory"]

    dry_run = kernel.autonomy_tune(dry_run=True, reason="unit-test")
    assert dry_run["status"] == "success"
    assert dry_run["dry_run"] is True
    assert dry_run["reason"] == "unit-test"
    assert isinstance(dry_run.get("mission_summary"), dict)
    assert isinstance(dry_run.get("policy_tuning"), dict)
    assert str(dry_run["policy_tuning"].get("status", "")) == "success"
    assert isinstance(dry_run.get("policy_bandit_tuning"), dict)
    assert str(dry_run["policy_bandit_tuning"].get("status", "")) == "success"
    assert isinstance(dry_run.get("execution_strategy_tuning"), dict)
    assert str(dry_run["execution_strategy_tuning"].get("status", "")) == "success"
    assert isinstance(dry_run.get("external_reliability_tuning"), dict)
    assert str(dry_run["external_reliability_tuning"].get("status", "")) == "success"

    applied = kernel.autonomy_tune(dry_run=False, reason="unit-test-apply")
    assert applied["status"] == "success"
    assert applied["reason"] == "unit-test-apply"
    assert isinstance(applied.get("policy_tuning"), dict)
    assert isinstance(applied.get("policy_bandit_tuning"), dict)
    assert isinstance(applied.get("execution_strategy_tuning"), dict)
    assert isinstance(applied.get("external_reliability_tuning"), dict)


def test_submit_goal_applies_execution_strategy_recommendation(monkeypatch, tmp_path) -> None:
    _configure_store_paths(monkeypatch, tmp_path)
    kernel = AgentKernel()

    text = "open desktop window and click submit button and click confirm"
    task_class = kernel._infer_policy_task_class(text=text, source="desktop-ui")  # noqa: SLF001
    failed_rows = [
        ActionResult(
            action="computer_click_target",
            status="failed",
            error="target not visible",
            output={"status": "failed"},
            attempt=2,
            duration_ms=1200,
        )
    ]
    for _ in range(8):
        kernel.execution_strategy.record_outcome(
            task_class=task_class,
            outcome="failed",
            results=failed_rows,
            metadata={"goal_id": "historical", "source": "desktop-ui"},
        )

    goal_id = asyncio.run(kernel.submit_goal(text, source="desktop-ui"))
    goal = kernel.get_goal(goal_id)
    assert goal is not None
    metadata = goal.request.metadata if isinstance(goal.request.metadata, dict) else {}
    assert metadata.get("execution_strategy_mode") == "strict"
    assert metadata.get("external_branch_strategy") == "enforce"
    assert metadata.get("execution_allow_parallel") is False
    assert metadata.get("execution_max_parallel_steps") == 1
    assert metadata.get("verification_strictness") == "strict"
