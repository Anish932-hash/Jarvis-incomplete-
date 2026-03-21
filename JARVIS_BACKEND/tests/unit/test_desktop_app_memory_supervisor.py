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
        skip_known_apps=True,
        prefer_unknown_apps=True,
        source="unit_test",
    )
    assert payload["status"] == "success"
    assert payload["enabled"] is True
    assert payload["query"] == "settings"
    assert payload["category"] == "system"
    assert payload["max_apps"] == 3
    assert payload["probe_controls"] is True
    assert payload["max_probe_controls"] == 5
    assert payload["skip_known_apps"] is True
    assert payload["prefer_unknown_apps"] is True


def test_desktop_app_memory_supervisor_campaign_create_and_run(tmp_path: Path) -> None:
    supervisor = DesktopAppMemorySupervisor(
        state_path=str(Path(tmp_path) / "desktop_app_memory_supervisor.json"),
        enabled=False,
        max_apps=2,
        per_app_limit=24,
    )

    def _execute(**kwargs: object) -> dict[str, object]:
        names = [str(item) for item in kwargs.get("app_names", [])] if isinstance(kwargs.get("app_names", []), list) else []
        assert kwargs["skip_known_apps"] is True
        return {
            "status": "success",
            "message": "campaign surveyed apps",
            "surveyed_app_count": len(names),
            "success_count": len([name for name in names if name != "notepad"]),
            "partial_count": 0,
            "error_count": 0,
            "skipped_app_count": len([name for name in names if name == "notepad"]),
            "items": [
                {"app_name": name, "status": "success", "message": "ok"}
                for name in names
                if name != "notepad"
            ],
            "skipped_apps": [
                {"app_name": "notepad", "reason": "healthy_memory_reuse"}
                for name in names
                if name == "notepad"
            ],
            "failed_apps": [],
        }

    supervisor.start(_execute)
    try:
        created = supervisor.create_campaign(
            app_names=["notepad", "calculator"],
            label="Installed app learner",
            query="note",
            skip_known_apps=True,
            prefer_unknown_apps=True,
        )
        assert created["status"] == "success"
        campaign_id = str(created["campaign"]["campaign_id"])
        assert campaign_id

        executed = supervisor.run_campaign(campaign_id=campaign_id, max_apps=2, source="manual")
        assert executed["status"] == "success"
        assert executed["campaign"]["skipped_app_count"] == 1
        assert executed["campaign"]["completed_app_count"] == 1
        campaigns = supervisor.campaigns(limit=4)
        assert campaigns["status"] == "success"
        assert campaigns["count"] == 1
        assert campaigns["summary"]["completed_app_total"] == 1
    finally:
        supervisor.stop()
