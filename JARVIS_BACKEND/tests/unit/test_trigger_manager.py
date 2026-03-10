from __future__ import annotations

from datetime import datetime, timedelta, timezone

from backend.python.core.trigger_manager import TriggerManager


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def test_create_and_due_and_pause_resume(tmp_path) -> None:
    store = tmp_path / "triggers.json"
    manager = TriggerManager(store_path=str(store))

    now = datetime.now(timezone.utc)
    record = manager.create(
        text="what time is it in UTC",
        interval_s=60,
        start_at=_iso(now - timedelta(seconds=1)),
        source="test",
    )

    due_items = manager.due(now=now + timedelta(seconds=2))
    assert any(item.trigger_id == record.trigger_id for item in due_items)

    ok, message, paused = manager.pause(record.trigger_id)
    assert ok, message
    assert paused is not None
    assert paused.status == "paused"

    due_items = manager.due(now=now + timedelta(minutes=3))
    assert all(item.trigger_id != record.trigger_id for item in due_items)

    ok, message, resumed = manager.resume(record.trigger_id)
    assert ok, message
    assert resumed is not None
    assert resumed.status == "active"


def test_dispatch_and_run_now_and_cancel(tmp_path) -> None:
    store = tmp_path / "triggers.json"
    manager = TriggerManager(store_path=str(store))

    record = manager.create(text="system snapshot", interval_s=30)
    updated = manager.mark_dispatched(record.trigger_id, goal_id="goal-1")
    assert updated is not None
    assert updated.run_count == 1
    assert updated.last_goal_id == "goal-1"

    ok, message, run_now = manager.run_now(record.trigger_id)
    assert ok, message
    assert run_now is not None
    assert run_now.status == "active"

    ok, message, cancelled = manager.cancel(record.trigger_id)
    assert ok, message
    assert cancelled is not None
    assert cancelled.status == "cancelled"
