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
        assert bool(kwargs["follow_surface_waves"]) is True
        assert int(kwargs["max_surface_waves"]) == 3
        return {
            "status": "success",
            "message": "surveyed apps",
            "surveyed_app_count": 2,
            "success_count": 2,
            "partial_count": 0,
            "error_count": 0,
            "failed_apps": [],
            "wave_summary": {
                "wave_attempt_total": 4,
                "learned_surface_total": 3,
                "known_surface_total": 1,
            },
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
        assert history["summary"]["wave_attempt_total"] == 4
        assert history["summary"]["learned_surface_total"] == 3
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
        follow_surface_waves=True,
        max_surface_waves=4,
        skip_known_apps=True,
        prefer_unknown_apps=True,
        continuous_learning=True,
        revisit_stale_apps=True,
        stale_after_hours=96.0,
        revisit_failed_apps=True,
        source="unit_test",
    )
    assert payload["status"] == "success"
    assert payload["enabled"] is True
    assert payload["query"] == "settings"
    assert payload["category"] == "system"
    assert payload["max_apps"] == 3
    assert payload["probe_controls"] is True
    assert payload["max_probe_controls"] == 5
    assert payload["follow_surface_waves"] is True
    assert payload["max_surface_waves"] == 4
    assert payload["skip_known_apps"] is True
    assert payload["prefer_unknown_apps"] is True
    assert payload["continuous_learning"] is True
    assert payload["revisit_stale_apps"] is True
    assert payload["stale_after_hours"] == 96.0
    assert payload["revisit_failed_apps"] is True


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
        assert kwargs["follow_surface_waves"] is True
        assert int(kwargs["max_surface_waves"]) == 4
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
            "wave_summary": {
                "wave_attempt_total": 5,
                "learned_surface_total": 4,
                "known_surface_total": 2,
            },
        }

    supervisor.start(_execute)
    try:
        created = supervisor.create_campaign(
            app_names=["notepad", "calculator"],
            label="Installed app learner",
            query="note",
            skip_known_apps=True,
            prefer_unknown_apps=True,
            follow_surface_waves=True,
            max_surface_waves=4,
        )
        assert created["status"] == "success"
        campaign_id = str(created["campaign"]["campaign_id"])
        assert campaign_id
        assert created["campaign"]["follow_surface_waves"] is True
        assert created["campaign"]["max_surface_waves"] == 4

        executed = supervisor.run_campaign(campaign_id=campaign_id, max_apps=2, source="manual")
        assert executed["status"] == "success"
        assert executed["campaign"]["skipped_app_count"] == 1
        assert executed["campaign"]["completed_app_count"] == 1
        assert executed["campaign"]["wave_attempt_count"] == 5
        assert executed["campaign"]["learned_surface_count"] == 4
        campaigns = supervisor.campaigns(limit=4)
        assert campaigns["status"] == "success"
        assert campaigns["count"] == 1
        assert campaigns["summary"]["completed_app_total"] == 1
        assert campaigns["summary"]["wave_attempt_total"] == 5
    finally:
        supervisor.stop()


def test_desktop_app_memory_supervisor_campaign_adapts_hotspot_roles_and_wave_depth(tmp_path: Path) -> None:
    supervisor = DesktopAppMemorySupervisor(
        state_path=str(Path(tmp_path) / "desktop_app_memory_supervisor.json"),
        enabled=False,
        max_apps=1,
        per_app_limit=24,
    )
    captured: list[dict[str, object]] = []

    def _execute(**kwargs: object) -> dict[str, object]:
        captured.append(dict(kwargs))
        names = [str(item) for item in kwargs.get("app_names", [])] if isinstance(kwargs.get("app_names", []), list) else []
        return {
            "status": "success",
            "message": "campaign adapted hotspot traversal",
            "surveyed_app_count": len(names),
            "success_count": len(names),
            "partial_count": 0,
            "error_count": 0,
            "skipped_app_count": 0,
            "items": [{"app_name": name, "status": "success", "message": "ok"} for name in names],
            "failed_apps": [],
            "wave_summary": {
                "wave_attempt_total": len(names) * 3,
                "learned_surface_total": len(names) * 2,
                "known_surface_total": 0,
                "traversed_container_roles": ["dialog", "menu"],
                "role_attempt_counts": {"dialog": len(names) * 2, "menu": len(names)},
                "role_learned_counts": {"dialog": len(names), "menu": len(names)},
                "recommended_container_roles": ["dialog", "menu"],
            },
        }

    def _memory_snapshot(**_: object) -> dict[str, object]:
        return {
            "status": "success",
            "items": [
                {
                    "app_name": "notepad",
                    "profile_id": "notepad",
                    "staleness": {"age_hours": 12.0, "stale_after_hours": 72.0, "stale": False},
                    "learning_health": {"status": "degraded"},
                    "failure_memory_summary": {"entry_count": 2},
                    "wave_strategy_summary": {
                        "recommended_actions": ["open_command_palette", "focus_sidebar"],
                        "recommended_container_roles": ["dialog", "menu"],
                        "top_followup_roles": [{"value": "tree", "count": 2}],
                    },
                    "safe_traversal_summary": {
                        "recommended_paths": ["menu", "dialog", "tree"],
                    },
                    "revalidation_summary": {
                        "target_count": 5,
                        "overdue_count": 1,
                        "priority_total": 420.0,
                        "failure_hotspot_count": 2,
                        "top_container_roles": [{"value": "dialog", "count": 3}, {"value": "menu", "count": 2}],
                        "reason_counts": {"never_verified": 3, "blocked_history": 2},
                    },
                }
            ],
        }

    supervisor.start(_execute, _memory_snapshot)
    try:
        created = supervisor.create_campaign(
            app_names=["notepad"],
            label="Hotspot learner",
            follow_surface_waves=True,
            max_surface_waves=2,
            continuous_learning=True,
            revisit_stale_apps=True,
            revisit_failed_apps=True,
            revalidate_known_controls=True,
            prioritize_failure_hotspots=True,
        )
        assert created["status"] == "success"
        assert created["campaign"]["target_container_roles"] == ["dialog", "menu"]
        assert created["campaign"]["adaptive_target_container_roles"] is True
        assert int(created["campaign"]["effective_max_surface_waves"] or 0) > 2
        assert created["campaign"]["adaptive_surface_wave_depth"] is True
        assert created["campaign"]["preferred_wave_actions"] == ["open_command_palette", "focus_sidebar"]
        assert created["campaign"]["adaptive_preferred_wave_actions"] is True
        assert {"menu", "dialog", "tree"}.issubset(set(created["campaign"]["recommended_traversal_paths"]))

        campaign_id = str(created["campaign"]["campaign_id"])
        executed = supervisor.run_campaign(campaign_id=campaign_id, max_apps=1, source="manual")

        assert executed["status"] == "success"
        assert captured[-1]["target_container_roles"] == ["dialog", "menu"]
        assert captured[-1]["preferred_wave_actions"] == ["open_command_palette", "focus_sidebar"]
        assert int(captured[-1]["max_surface_waves"] or 0) > 2
        assert executed["campaign"]["adaptive_target_container_roles"] is True
        assert executed["campaign"]["adaptive_surface_wave_depth"] is True
        assert executed["campaign"]["adaptive_preferred_wave_actions"] is True
        assert executed["campaign"]["preferred_wave_actions"] == ["open_command_palette", "focus_sidebar"]
        assert {"menu", "dialog", "tree"}.issubset(set(executed["campaign"]["recommended_traversal_paths"]))
        assert executed["campaign"]["traversed_container_roles"] == ["dialog", "menu"]
        assert int(dict(executed["campaign"].get("role_learned_counts", {})).get("dialog", 0) or 0) >= 1
        assert executed["campaign"]["revalidation_focus_summary"]["top_container_roles"][0]["value"] == "dialog"
        top_traversed_roles = {
            str(item.get("value")): int(item.get("count", 0) or 0)
            for item in list(executed["campaigns"]["summary"].get("top_traversed_container_roles", []))
            if isinstance(item, dict)
        }
        assert int(top_traversed_roles.get("dialog", 0) or 0) >= 1
        assert int(top_traversed_roles.get("menu", 0) or 0) >= 1
        top_preferred_actions = {
            str(item.get("value")): int(item.get("count", 0) or 0)
            for item in list(executed["campaigns"]["summary"].get("top_preferred_wave_actions", []))
            if isinstance(item, dict)
        }
        assert int(top_preferred_actions.get("open_command_palette", 0) or 0) >= 1
    finally:
        supervisor.stop()


def test_desktop_app_memory_supervisor_campaign_reseeds_stale_targets(tmp_path: Path) -> None:
    supervisor = DesktopAppMemorySupervisor(
        state_path=str(Path(tmp_path) / "desktop_app_memory_supervisor.json"),
        enabled=False,
        max_apps=1,
        per_app_limit=24,
    )
    callback_runs: list[dict[str, object]] = []
    snapshot_calls = {"count": 0}

    def _execute(**kwargs: object) -> dict[str, object]:
        callback_runs.append(dict(kwargs))
        names = [str(item) for item in kwargs.get("app_names", [])] if isinstance(kwargs.get("app_names", []), list) else []
        return {
            "status": "success",
            "message": "campaign surveyed apps",
            "surveyed_app_count": len(names),
            "success_count": len(names),
            "partial_count": 0,
            "error_count": 0,
            "skipped_app_count": 0,
            "items": [
                {"app_name": name, "status": "success", "message": "ok"}
                for name in names
            ],
            "failed_apps": [],
            "wave_summary": {
                "wave_attempt_total": len(names),
                "learned_surface_total": len(names),
                "known_surface_total": 0,
            },
        }

    def _memory_snapshot(**_: object) -> dict[str, object]:
        snapshot_calls["count"] += 1
        if snapshot_calls["count"] <= 1:
            items = [
                {
                    "app_name": "notepad",
                    "profile_id": "notepad",
                    "staleness": {"age_hours": 120.0, "stale_after_hours": 24.0, "stale": True},
                    "learning_health": {"status": "degraded"},
                    "failure_memory_summary": {"entry_count": 1},
                },
                {
                    "app_name": "calculator",
                    "profile_id": "calculator",
                    "staleness": {"age_hours": 2.0, "stale_after_hours": 24.0, "stale": False},
                    "learning_health": {"status": "healthy"},
                    "failure_memory_summary": {"entry_count": 0},
                },
            ]
        else:
            items = [
                {
                    "app_name": "notepad",
                    "profile_id": "notepad",
                    "staleness": {"age_hours": 150.0, "stale_after_hours": 24.0, "stale": True},
                    "learning_health": {"status": "degraded"},
                    "failure_memory_summary": {"entry_count": 1},
                },
                {
                    "app_name": "calculator",
                    "profile_id": "calculator",
                    "staleness": {"age_hours": 4.0, "stale_after_hours": 24.0, "stale": False},
                    "learning_health": {"status": "healthy"},
                    "failure_memory_summary": {"entry_count": 0},
                },
            ]
        return {"status": "success", "items": items}

    supervisor.start(_execute, _memory_snapshot)
    try:
        created = supervisor.create_campaign(
            app_names=["notepad"],
            label="Stale app campaign",
            continuous_learning=True,
            revisit_stale_apps=True,
            stale_after_hours=24.0,
            revisit_failed_apps=True,
            skip_known_apps=True,
            prefer_unknown_apps=True,
        )
        campaign_id = str(created["campaign"]["campaign_id"])
        assert created["campaign"]["target_apps"][0] == "notepad"

        first = supervisor.run_campaign(campaign_id=campaign_id, max_apps=1, source="manual")
        assert first["status"] == "success"
        assert callback_runs[-1]["app_names"] == ["notepad"]

        second = supervisor.run_campaign(campaign_id=campaign_id, max_apps=1, source="manual")
        assert second["status"] == "success"
        assert second["reseed"]["selection_strategy"] == "campaign_reseed"
        assert int(second["campaign"]["reseed_count"]) >= 1
        assert int(second["campaign"]["stale_reseed_count"]) >= 1
        assert callback_runs[-1]["app_names"] == ["notepad"]
        assert callback_runs[-1]["skip_known_apps"] is False
    finally:
        supervisor.stop()


def test_desktop_app_memory_supervisor_trigger_prioritizes_stale_memory_targets(tmp_path: Path) -> None:
    supervisor = DesktopAppMemorySupervisor(
        state_path=str(Path(tmp_path) / "desktop_app_memory_supervisor.json"),
        enabled=False,
        max_apps=2,
        per_app_limit=24,
    )
    captured: list[dict[str, object]] = []

    def _execute(**kwargs: object) -> dict[str, object]:
        captured.append(dict(kwargs))
        names = [str(item) for item in kwargs.get("app_names", [])] if isinstance(kwargs.get("app_names", []), list) else []
        return {
            "status": "success",
            "message": "supervisor targeted stale apps",
            "surveyed_app_count": len(names),
            "success_count": len(names),
            "partial_count": 0,
            "error_count": 0,
            "skipped_app_count": 0,
            "items": [{"app_name": name, "status": "success", "message": "ok"} for name in names],
            "failed_apps": [],
            "wave_summary": {"wave_attempt_total": len(names), "learned_surface_total": len(names), "known_surface_total": 0},
        }

    def _memory_snapshot(**_: object) -> dict[str, object]:
        return {
            "status": "success",
            "items": [
                {
                    "app_name": "notepad",
                    "profile_id": "notepad",
                    "staleness": {"age_hours": 144.0, "stale_after_hours": 72.0, "stale": True},
                    "learning_health": {"status": "degraded"},
                    "failure_memory_summary": {"entry_count": 2},
                },
                {
                    "app_name": "calculator",
                    "profile_id": "calculator",
                    "staleness": {"age_hours": 80.0, "stale_after_hours": 72.0, "stale": True},
                    "learning_health": {"status": "healthy"},
                    "failure_memory_summary": {"entry_count": 0},
                },
            ],
        }

    supervisor.start(_execute, _memory_snapshot)
    try:
        payload = supervisor.trigger_now(
            source="daemon",
            max_apps=2,
            continuous_learning=True,
            revisit_stale_apps=True,
            stale_after_hours=72.0,
            revisit_failed_apps=True,
        )
        assert payload["status"] == "success"
        assert captured[-1]["app_names"] == ["notepad", "calculator"]
        assert captured[-1]["skip_known_apps"] is False
        latest = payload["supervisor"]["latest_run"]
        assert latest["selection_strategy"] == "stale_memory_revisit"
        assert int(latest["stale_candidate_count"]) >= 2
        assert int(latest["revisit_app_count"]) >= 2
    finally:
        supervisor.stop()


def test_desktop_app_memory_supervisor_trigger_prioritizes_revalidation_hotspots(tmp_path: Path) -> None:
    supervisor = DesktopAppMemorySupervisor(
        state_path=str(Path(tmp_path) / "desktop_app_memory_supervisor.json"),
        enabled=False,
        max_apps=1,
        per_app_limit=24,
    )
    captured: list[dict[str, object]] = []

    def _execute(**kwargs: object) -> dict[str, object]:
        captured.append(dict(kwargs))
        names = [str(item) for item in kwargs.get("app_names", [])] if isinstance(kwargs.get("app_names", []), list) else []
        return {
            "status": "success",
            "message": "supervisor targeted revalidation hotspots",
            "surveyed_app_count": len(names),
            "success_count": len(names),
            "partial_count": 0,
            "error_count": 0,
            "skipped_app_count": 0,
            "items": [{"app_name": name, "status": "success", "message": "ok"} for name in names],
            "failed_apps": [],
            "wave_summary": {"wave_attempt_total": len(names), "learned_surface_total": len(names), "known_surface_total": 0},
        }

    def _memory_snapshot(**_: object) -> dict[str, object]:
        return {
            "status": "success",
            "items": [
                {
                    "app_name": "notepad",
                    "profile_id": "notepad",
                    "staleness": {"age_hours": 12.0, "stale_after_hours": 72.0, "stale": False},
                    "learning_health": {"status": "degraded"},
                    "failure_memory_summary": {"entry_count": 1},
                    "revalidation_summary": {"target_count": 3, "overdue_count": 1, "priority_total": 240.0},
                },
                {
                    "app_name": "calculator",
                    "profile_id": "calculator",
                    "staleness": {"age_hours": 90.0, "stale_after_hours": 72.0, "stale": True},
                    "learning_health": {"status": "healthy"},
                    "failure_memory_summary": {"entry_count": 0},
                    "revalidation_summary": {"target_count": 0, "overdue_count": 0, "priority_total": 0.0},
                },
            ],
        }

    supervisor.start(_execute, _memory_snapshot)
    try:
        payload = supervisor.trigger_now(
            source="daemon",
            max_apps=1,
            continuous_learning=True,
            revisit_stale_apps=True,
            stale_after_hours=72.0,
            revisit_failed_apps=True,
            revalidate_known_controls=True,
            prioritize_failure_hotspots=True,
            target_container_roles=["menu"],
        )
        assert payload["status"] == "success"
        assert captured[-1]["app_names"] == ["notepad"]
        assert captured[-1]["revalidate_known_controls"] is True
        assert captured[-1]["prefer_failure_memory"] is True
        assert captured[-1]["target_container_roles"] == ["menu"]
        latest = payload["supervisor"]["latest_run"]
        assert latest["selection_strategy"] == "revalidation_hotspot_revisit"
        assert int(latest["revalidation_candidate_count"]) >= 1
    finally:
        supervisor.stop()


def test_desktop_app_memory_supervisor_adapts_target_container_roles_from_revalidation_hotspots(tmp_path: Path) -> None:
    supervisor = DesktopAppMemorySupervisor(
        state_path=str(Path(tmp_path) / "desktop_app_memory_supervisor.json"),
        enabled=False,
        max_apps=1,
        per_app_limit=24,
    )
    captured: list[dict[str, object]] = []

    def _execute(**kwargs: object) -> dict[str, object]:
        captured.append(dict(kwargs))
        names = [str(item) for item in kwargs.get("app_names", [])] if isinstance(kwargs.get("app_names", []), list) else []
        return {
            "status": "success",
            "message": "supervisor adapted container roles",
            "surveyed_app_count": len(names),
            "success_count": len(names),
            "partial_count": 0,
            "error_count": 0,
            "skipped_app_count": 0,
            "items": [{"app_name": name, "status": "success", "message": "ok"} for name in names],
            "failed_apps": [],
            "wave_summary": {"wave_attempt_total": len(names), "learned_surface_total": len(names), "known_surface_total": 0},
        }

    def _memory_snapshot(**_: object) -> dict[str, object]:
        return {
            "status": "success",
            "items": [
                {
                    "app_name": "notepad",
                    "profile_id": "notepad",
                    "staleness": {"age_hours": 8.0, "stale_after_hours": 72.0, "stale": False},
                    "learning_health": {"status": "degraded"},
                    "failure_memory_summary": {"entry_count": 1},
                    "revalidation_summary": {
                        "target_count": 4,
                        "overdue_count": 1,
                        "priority_total": 340.0,
                        "top_container_roles": [{"value": "menu", "count": 3}, {"value": "dialog", "count": 1}],
                        "reason_counts": {"never_verified": 3, "uncertain_effect": 1},
                    },
                }
            ],
        }

    supervisor.start(_execute, _memory_snapshot)
    try:
        payload = supervisor.trigger_now(
            source="daemon",
            max_apps=1,
            continuous_learning=True,
            revisit_stale_apps=True,
            stale_after_hours=72.0,
            revisit_failed_apps=True,
            revalidate_known_controls=True,
            prioritize_failure_hotspots=True,
        )
        assert payload["status"] == "success"
        assert captured[-1]["app_names"] == ["notepad"]
        assert captured[-1]["target_container_roles"] == ["menu", "dialog"]
        latest = payload["supervisor"]["latest_run"]
        assert latest["adaptive_target_container_roles"] is True
        assert latest["selection_summary"]["top_revalidation_container_roles"][0]["value"] == "menu"
    finally:
        supervisor.stop()
