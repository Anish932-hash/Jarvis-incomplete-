from __future__ import annotations

from pathlib import Path

from backend.python.core.desktop_app_memory_supervisor import DesktopAppMemorySupervisor


def test_desktop_app_memory_supervisor_trigger_and_history(tmp_path: Path) -> None:
    supervisor = DesktopAppMemorySupervisor(
        state_path=str(Path(tmp_path) / "desktop_app_memory_supervisor.json"),
        enabled=False,
        max_apps=2,
        per_app_limit=24,
    )

    def _execute(**kwargs: object) -> dict[str, object]:
        assert int(kwargs["max_apps"]) == 2
        return {
            "status": "success",
            "message": "surveyed apps",
            "surveyed_app_count": 2,
            "success_count": 2,
            "partial_count": 0,
            "error_count": 0,
            "failed_apps": [],
        }

    supervisor.start(_execute)
    try:
        payload = supervisor.trigger_now(source="manual")
        assert payload["status"] == "success"
        assert payload["supervisor"]["status"] == "success"
        assert payload["supervisor"]["latest_run"]["surveyed_app_count"] == 2

        history = supervisor.history(limit=4)
        assert history["status"] == "success"
        assert history["count"] == 1
        assert history["summary"]["success_total"] == 2
    finally:
        supervisor.stop()


def test_desktop_app_memory_supervisor_configure_updates_query_filters(tmp_path: Path) -> None:
    supervisor = DesktopAppMemorySupervisor(
        state_path=str(Path(tmp_path) / "desktop_app_memory_supervisor.json"),
        enabled=False,
    )
    payload = supervisor.configure(
        enabled=True,
        query="settings",
        category="system",
        max_apps=3,
        probe_controls=True,
        max_probe_controls=5,
        source="unit_test",
    )
    assert payload["status"] == "success"
    assert payload["enabled"] is True
    assert payload["query"] == "settings"
    assert payload["category"] == "system"
    assert payload["max_apps"] == 3
    assert payload["probe_controls"] is True
    assert payload["max_probe_controls"] == 5
