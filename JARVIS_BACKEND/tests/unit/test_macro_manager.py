from __future__ import annotations

from backend.python.core.contracts import ActionResult
from backend.python.core.macro_manager import MacroManager


def _result(action: str, status: str = "success") -> ActionResult:
    return ActionResult(action=action, status=status, output={"status": status})


def test_learn_updates_existing_macro_and_persists(tmp_path) -> None:
    store = tmp_path / "macros.json"
    manager = MacroManager(store_path=str(store))

    first = manager.learn_from_goal(
        text="open notepad and write project notes",
        source="desktop-ui",
        status="completed",
        results=[_result("open_app"), _result("keyboard_type")],
    )
    assert first is not None
    assert first.success_count == 1
    assert "open_app" in first.actions

    second = manager.learn_from_goal(
        text="open notepad and write project notes",
        source="desktop-ui",
        status="completed",
        results=[_result("open_app"), _result("write_file")],
    )
    assert second is not None
    assert second.macro_id == first.macro_id
    assert second.success_count == 2
    assert "keyboard_type" in second.actions
    assert "write_file" in second.actions

    rows = manager.list(query="notepad", limit=10)
    assert rows
    assert rows[0]["macro_id"] == first.macro_id

    reloaded = MacroManager(store_path=str(store))
    loaded = reloaded.get(first.macro_id)
    assert loaded is not None
    assert loaded.success_count == 2


def test_learn_ignores_failed_or_tts_only_sequences(tmp_path) -> None:
    store = tmp_path / "macros.json"
    manager = MacroManager(store_path=str(store))

    failed = manager.learn_from_goal(
        text="open calculator",
        source="desktop-ui",
        status="failed",
        results=[_result("open_app", status="failed")],
    )
    assert failed is None

    tts_only = manager.learn_from_goal(
        text="say hello",
        source="desktop-ui",
        status="completed",
        results=[_result("tts_speak")],
    )
    assert tts_only is None
    assert manager.list(limit=10) == []


def test_mark_used_increments_usage(tmp_path) -> None:
    store = tmp_path / "macros.json"
    manager = MacroManager(store_path=str(store))

    record = manager.learn_from_goal(
        text="show system snapshot",
        source="desktop-ui",
        status="completed",
        results=[_result("system_snapshot")],
    )
    assert record is not None

    used = manager.mark_used(record.macro_id)
    assert used is not None
    assert used.usage_count == 1
    assert used.last_used_at
