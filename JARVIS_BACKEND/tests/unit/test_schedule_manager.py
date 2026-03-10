from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from backend.python.core.schedule_manager import ScheduleManager, _parse_iso


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def test_create_and_due_and_cancel(tmp_path) -> None:
    store = tmp_path / "schedules.json"
    manager = ScheduleManager(store_path=str(store))

    now = datetime.now(timezone.utc)
    record = manager.create(
        text="what time is it in UTC",
        run_at=_iso(now - timedelta(seconds=2)),
        source="test",
    )

    due_items = manager.due(now=now)
    assert any(item.schedule_id == record.schedule_id for item in due_items)

    ok, message, cancelled = manager.cancel(record.schedule_id)
    assert ok, message
    assert cancelled is not None
    assert cancelled.status == "cancelled"


def test_dispatch_and_retry_then_complete(tmp_path) -> None:
    store = tmp_path / "schedules.json"
    manager = ScheduleManager(store_path=str(store))
    now = datetime.now(timezone.utc)
    record = manager.create(
        text="open notepad",
        run_at=_iso(now),
        max_attempts=2,
        retry_delay_s=30,
    )

    dispatched = manager.mark_dispatched(record.schedule_id, goal_id="goal-1")
    assert dispatched is not None
    assert dispatched.attempt_count == 1
    assert dispatched.status == "dispatched"

    retried = manager.mark_goal_result(
        record.schedule_id,
        goal_id="goal-1",
        goal_status="failed",
        failure_reason="transient failure",
    )
    assert retried is not None
    assert retried.status == "retry_wait"
    assert retried.last_error == "transient failure"

    manager.mark_dispatched(record.schedule_id, goal_id="goal-2")
    completed = manager.mark_goal_result(
        record.schedule_id,
        goal_id="goal-2",
        goal_status="completed",
    )
    assert completed is not None
    assert completed.status == "completed"
    assert completed.last_error == ""
    assert completed.run_count == 2


def test_recurring_schedule_resets_to_next_cycle(tmp_path) -> None:
    store = tmp_path / "schedules.json"
    manager = ScheduleManager(store_path=str(store))
    now = datetime.now(timezone.utc)
    record = manager.create(
        text="system snapshot",
        run_at=_iso(now - timedelta(seconds=1)),
        repeat_interval_s=120,
        max_attempts=2,
        retry_delay_s=10,
    )

    manager.mark_dispatched(record.schedule_id, goal_id="goal-1")
    completed = manager.mark_goal_result(record.schedule_id, goal_id="goal-1", goal_status="completed")
    assert completed is not None
    assert completed.status == "pending"
    assert completed.attempt_count == 0
    assert completed.repeat_interval_s == 120
    assert _parse_iso(completed.next_run_at) is not None
    assert completed.checkpoint.get("next_cycle_at")


def test_pause_resume_and_run_now_lifecycle(tmp_path) -> None:
    store = tmp_path / "schedules.json"
    manager = ScheduleManager(store_path=str(store))
    now = datetime.now(timezone.utc)
    record = manager.create(
        text="what time is it in UTC",
        run_at=_iso(now + timedelta(hours=1)),
    )

    ok, message, paused = manager.pause(record.schedule_id)
    assert ok, message
    assert paused is not None
    assert paused.status == "paused"

    due_items = manager.due(now=now + timedelta(hours=2))
    assert all(item.schedule_id != record.schedule_id for item in due_items)

    ok, message, resumed = manager.resume(record.schedule_id)
    assert ok, message
    assert resumed is not None
    assert resumed.status == "pending"

    ok, message, immediate = manager.run_now(record.schedule_id)
    assert ok, message
    assert immediate is not None
    assert immediate.status == "pending"

    due_items = manager.due(now=datetime.now(timezone.utc) + timedelta(seconds=1))
    assert any(item.schedule_id == record.schedule_id for item in due_items)


def test_load_recovers_dispatched_state_to_pending(tmp_path) -> None:
    store = tmp_path / "schedules.json"
    raw = [
        {
            "schedule_id": "schedule-1",
            "text": "hello",
            "source": "desktop-ui",
            "metadata": {},
            "run_at": "2026-01-01T00:00:00+00:00",
            "next_run_at": "2026-01-01T00:00:00+00:00",
            "status": "dispatched",
            "max_attempts": 3,
            "retry_delay_s": 60,
            "attempt_count": 1,
            "last_goal_id": "goal-1",
            "last_error": "",
            "checkpoint": {},
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
    ]
    store.write_text(json.dumps(raw), encoding="utf-8")

    manager = ScheduleManager(store_path=str(store))
    loaded = manager.get("schedule-1")
    assert loaded is not None
    assert loaded.status == "pending"
    assert loaded.checkpoint.get("resume_note")
