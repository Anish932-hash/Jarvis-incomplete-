from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path

import pytest

from backend.python.core.contracts import ActionResult, ExecutionPlan, GoalRecord, GoalRequest, PlanStep
from backend.python.core.goal_manager import GoalManager
from backend.python.core.task_state import GoalStatus, StepStatus


def _manager(store: Path) -> GoalManager:
    return GoalManager(store_path=str(store), max_records=500)


def test_cancel_pending_goal_before_dequeue(tmp_path: Path) -> None:
    async def _scenario() -> None:
        manager = _manager(tmp_path / "goals.json")
        goal = GoalRecord(goal_id="goal-1", request=GoalRequest(text="what time is it"))
        await manager.enqueue(goal)

        ok, message, updated = manager.request_cancel("goal-1", reason="stop")
        assert ok, message
        assert updated is not None
        assert updated.status == GoalStatus.CANCELLED

        dequeued = await manager.dequeue(timeout_s=0.05)
        assert dequeued is None

    asyncio.run(_scenario())


def test_cancel_running_goal_creates_interrupt_flag(tmp_path: Path) -> None:
    async def _scenario() -> None:
        manager = _manager(tmp_path / "goals.json")
        goal = GoalRecord(goal_id="goal-2", request=GoalRequest(text="open app"))
        await manager.enqueue(goal)
        dequeued = await manager.dequeue(timeout_s=0.05)
        assert dequeued is not None
        manager.mark_running(dequeued)

        ok, message, updated = manager.request_cancel("goal-2", reason="halt")
        assert ok, message
        assert updated is not None
        assert updated.status == GoalStatus.RUNNING
        assert manager.is_cancel_requested("goal-2") is True
        assert manager.cancel_reason("goal-2") == "halt"

    asyncio.run(_scenario())


def test_recovery_requeues_running_and_pending_goals(tmp_path: Path) -> None:
    async def _scenario() -> None:
        store = tmp_path / "goals.json"
        manager = _manager(store)

        running_goal = GoalRecord(goal_id="goal-running", request=GoalRequest(text="compose report"))
        await manager.enqueue(running_goal)
        running = await manager.dequeue(timeout_s=0.05)
        assert running is not None
        manager.mark_running(running)

        queued_goal = GoalRecord(goal_id="goal-queued", request=GoalRequest(text="send summary"))
        await manager.enqueue(queued_goal)

        reloaded = _manager(store)
        summary = reloaded.recovery_summary()
        assert summary["recovered_running_count"] == 1
        assert summary["requeued_count"] == 2

        recovered_running = reloaded.get("goal-running")
        assert recovered_running is not None
        assert recovered_running.status == GoalStatus.PENDING
        assert recovered_running.request.metadata.get("recovered_from_status") == GoalStatus.RUNNING.value
        assert bool(recovered_running.request.metadata.get("recovered_at")) is True

        first = await reloaded.dequeue(timeout_s=0.05)
        second = await reloaded.dequeue(timeout_s=0.05)
        assert first is not None
        assert second is not None
        assert {first.goal_id, second.goal_id} == {"goal-running", "goal-queued"}

    asyncio.run(_scenario())


def test_cancel_request_persists_across_restart(tmp_path: Path) -> None:
    async def _scenario() -> None:
        store = tmp_path / "goals.json"
        manager = _manager(store)
        goal = GoalRecord(goal_id="goal-cancel", request=GoalRequest(text="open browser"))
        await manager.enqueue(goal)
        dequeued = await manager.dequeue(timeout_s=0.05)
        assert dequeued is not None
        manager.mark_running(dequeued)

        ok, message, _updated = manager.request_cancel("goal-cancel", reason="halt now")
        assert ok, message

        reloaded = _manager(store)
        assert reloaded.is_cancel_requested("goal-cancel") is True
        assert reloaded.cancel_reason("goal-cancel") == "halt now"

    asyncio.run(_scenario())


def test_sync_persists_plan_and_results_snapshot(tmp_path: Path) -> None:
    async def _scenario() -> None:
        store = tmp_path / "goals.json"
        manager = _manager(store)
        goal = GoalRecord(goal_id="goal-state", request=GoalRequest(text="draft email"))
        await manager.enqueue(goal)
        goal.plan = ExecutionPlan(
            plan_id="plan-1",
            goal_id=goal.goal_id,
            intent="draft_email",
            steps=[
                PlanStep(
                    step_id="step-1",
                    action="compose_text",
                    args={"tone": "formal"},
                    verify={"kind": "contains"},
                    status=StepStatus.RUNNING,
                )
            ],
            context={"planner_mode": "rule"},
        )
        goal.results = [
            ActionResult(
                action="compose_text",
                status="success",
                output={"text": "Hello team"},
                duration_ms=42,
            )
        ]
        manager.sync(goal)

        reloaded = _manager(store)
        loaded = reloaded.get("goal-state")
        assert loaded is not None
        assert loaded.plan is not None
        assert loaded.plan.plan_id == "plan-1"
        assert loaded.plan.intent == "draft_email"
        assert loaded.plan.context.get("planner_mode") == "rule"
        assert len(loaded.plan.steps) == 1
        assert loaded.plan.steps[0].action == "compose_text"
        assert loaded.plan.steps[0].status == StepStatus.RUNNING
        assert len(loaded.results) == 1
        assert loaded.results[0].action == "compose_text"
        assert loaded.results[0].status == "success"
        assert loaded.results[0].output.get("text") == "Hello team"

    asyncio.run(_scenario())


def test_priority_dequeue_prefers_interactive_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JARVIS_GOAL_PRIORITY_DEQUEUE_ENABLED", "1")

    async def _scenario() -> None:
        manager = _manager(tmp_path / "goals.json")

        schedule_goal = GoalRecord(
            goal_id="goal-schedule",
            request=GoalRequest(text="scheduled automation", source="desktop-schedule"),
        )
        interactive_goal = GoalRecord(
            goal_id="goal-ui",
            request=GoalRequest(text="what time is it in UTC", source="desktop-ui"),
        )

        await manager.enqueue(schedule_goal)
        await manager.enqueue(interactive_goal)

        first = await manager.dequeue(timeout_s=0.1)
        assert first is not None
        assert first.goal_id == "goal-ui"

    asyncio.run(_scenario())


def test_promote_and_wait_for_terminal_roundtrip(tmp_path: Path) -> None:
    async def _scenario() -> None:
        manager = _manager(tmp_path / "goals.json")
        goal = GoalRecord(
            goal_id="goal-promote",
            request=GoalRequest(text="queued task", source="desktop-schedule"),
        )
        await manager.enqueue(goal)
        promoted = manager.promote("goal-promote", temporary_priority=-4, reason="api_wait_for_goal")
        assert promoted is True

        dequeued = await manager.dequeue(timeout_s=0.1)
        assert dequeued is not None
        assert dequeued.goal_id == "goal-promote"
        manager.mark_running(dequeued)

        def _complete() -> None:
            time.sleep(0.03)
            manager.mark_completed(dequeued)

        worker = threading.Thread(target=_complete, daemon=True)
        worker.start()
        waited = manager.wait_for_terminal("goal-promote", timeout_s=1.0)
        worker.join(timeout=1.0)
        assert waited is not None
        assert waited.status == GoalStatus.COMPLETED

    asyncio.run(_scenario())
