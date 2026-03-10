from __future__ import annotations

from backend.python.core.desktop_state import DesktopState


def test_desktop_state_observe_and_latest(tmp_path) -> None:
    store = tmp_path / "desktop_state.jsonl"
    state = DesktopState(max_items=50, store_path=str(store))

    row = state.observe(
        action="active_window",
        output={"status": "success", "window": {"title": "Notepad", "hwnd": 123}},
        goal_id="goal-1",
        source="desktop-ui",
    )
    assert row["action"] == "active_window"
    assert row["state_hash"]

    latest = state.latest()
    assert latest["status"] == "success"
    assert latest["action"] == "active_window"
    assert latest["normalized"]["window"]["title"] == "Notepad"


def test_desktop_state_diff_detects_changed_fields(tmp_path) -> None:
    store = tmp_path / "desktop_state.jsonl"
    state = DesktopState(max_items=50, store_path=str(store))

    first = state.observe(
        action="active_window",
        output={"status": "success", "window": {"title": "Notepad", "hwnd": 123}},
        goal_id="goal-1",
        source="desktop-ui",
    )
    second = state.observe(
        action="active_window",
        output={"status": "success", "window": {"title": "Calculator", "hwnd": 456}},
        goal_id="goal-1",
        source="desktop-ui",
    )

    diff = state.diff(from_hash=first["state_hash"], to_hash=second["state_hash"])
    assert diff["status"] == "success"
    assert diff["change_count"] >= 1
    assert any(path.startswith("window.") for path in diff["changed_paths"])
