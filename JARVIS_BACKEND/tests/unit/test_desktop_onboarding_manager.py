from __future__ import annotations

from backend.python.core.desktop_onboarding_manager import DesktopOnboardingManager


def test_desktop_onboarding_manager_records_and_lists_runs(tmp_path) -> None:
    manager = DesktopOnboardingManager(store_path=str(tmp_path / "desktop_onboarding.json"))

    recorded = manager.record_run(
        {
            "status": "success",
            "task": "reasoning",
            "summary": {
                "provider_update_count": 1,
                "launch_seed_count": 2,
                "prepared_app_count": 3,
                "prepared_blocked_count": 1,
                "prepared_degraded_count": 2,
            },
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
    assert history["summary"]["provider_update_total"] == 1
    assert history["summary"]["launch_seed_total"] == 2
    assert history["summary"]["prepared_app_total"] == 3
    assert history["summary"]["prepared_blocked_total"] == 1
    assert history["summary"]["prepared_degraded_total"] == 2
