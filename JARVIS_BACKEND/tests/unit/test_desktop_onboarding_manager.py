from __future__ import annotations

from backend.python.core.desktop_onboarding_manager import DesktopOnboardingManager


def test_desktop_onboarding_manager_records_and_lists_runs(tmp_path) -> None:
    manager = DesktopOnboardingManager(store_path=str(tmp_path / "desktop_onboarding.json"))

    recorded = manager.record_run(
        {
            "status": "success",
            "task": "reasoning",
            "summary": {"provider_update_count": 1},
        },
        source="machine_onboarding",
    )

    assert recorded["status"] == "success"
    assert recorded["source"] == "machine_onboarding"
    assert recorded["recorded_at"]

    latest = manager.latest_run()
    assert latest["task"] == "reasoning"

    history = manager.history(limit=4)
    assert history["status"] == "success"
    assert history["count"] == 1
    assert history["items"][0]["task"] == "reasoning"
    assert history["summary"]["status_counts"]["success"] == 1
    assert history["summary"]["source_counts"]["machine_onboarding"] == 1
