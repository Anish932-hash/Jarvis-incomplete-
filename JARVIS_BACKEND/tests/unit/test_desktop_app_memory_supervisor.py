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
        preferred_wave_actions=["command", "focus_sidebar"],
        preferred_traversal_paths=["menu", "dialog", "tree"],
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
    assert payload["preferred_wave_actions"] == ["command", "focus_sidebar"]
    assert payload["preferred_traversal_paths"] == ["menu", "dialog", "tree"]


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
                "preferred_path_hits": len(names) * 2,
                "traversal_path_execution_count": len(names) * 2,
                "traversed_container_roles": ["dialog", "menu"],
                "executed_traversal_paths": ["dialog", "tree"],
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
            adaptive_app_profiles=[
                {
                    "app_name": "notepad",
                    "memory_mission": {
                        "status": "strong",
                        "seed_query": "settings",
                        "query_hints": ["settings", "preferences"],
                        "hotkey_hints": ["Alt+F", "Ctrl+F"],
                        "followthrough_recommended": True,
                    },
                    "learning_profile": "hybrid_guided_explore",
                    "execution_mode": "hybrid_ready",
                    "adaptive_runtime_strategy_profile": "balanced_hybrid_guided_explore",
                    "runtime_band_preference": "hybrid",
                    "runtime_strategy": {
                        "strategy_profile": "balanced_hybrid_guided_explore",
                        "runtime_band_preference": "hybrid",
                        "preferred_probe_mode": "local_vision_assist",
                    },
                    "provider_model_readiness": {
                        "ai_route_status": "matched",
                        "ai_route_confidence": 0.84,
                        "ai_route_confidence_band": "high",
                        "selected_ai_runtime_band": "hybrid",
                        "selected_ai_route_profile": "local_vision_assist_native_stabilized",
                        "selected_ai_model_preference": "hybrid_runtime",
                        "selected_ai_provider_source": "local_runtime_plus_ocr",
                        "selected_ai_reasoning_stack": "desktop_agent",
                        "selected_ai_vision_stack": "perception",
                        "selected_ai_memory_stack": "memory",
                        "selected_ai_stack_names": ["desktop_agent", "perception", "memory"],
                        "ai_route_reason_codes": ["hybrid_runtime_priority"],
                    },
                }
            ],
        )
        assert created["status"] == "success"
        assert created["campaign"]["target_container_roles"] == ["dialog", "menu"]
        assert created["campaign"]["adaptive_target_container_roles"] is True
        assert int(created["campaign"]["effective_max_surface_waves"] or 0) > 2
        assert created["campaign"]["adaptive_surface_wave_depth"] is True
        assert created["campaign"]["preferred_wave_actions"] == ["open_command_palette", "focus_sidebar"]
        assert created["campaign"]["adaptive_preferred_wave_actions"] is True
        assert created["campaign"]["adaptive_preferred_traversal_paths"] is True
        assert {"menu", "dialog", "tree"}.issubset(set(created["campaign"]["preferred_traversal_paths"]))
        assert {"menu", "dialog", "tree"}.issubset(set(created["campaign"]["recommended_traversal_paths"]))
        assert created["campaign"]["adaptive_runtime_strategy_counts"]["balanced_hybrid_guided_explore"] == 1
        assert created["campaign"]["runtime_band_counts"]["hybrid"] == 1
        assert created["campaign"]["expected_route_profile_counts"]["local_vision_assist"] == 1
        assert created["campaign"]["expected_model_preference_counts"]["hybrid_runtime"] == 1
        assert created["campaign"]["expected_provider_source_counts"]["local_runtime_plus_ocr"] == 1
        assert created["campaign"]["ai_route_status_counts"]["matched"] == 1
        assert created["campaign"]["ai_route_runtime_band_counts"]["hybrid"] == 1
        assert created["campaign"]["ai_route_profile_counts"]["local_vision_assist_native_stabilized"] == 1
        assert created["campaign"]["ai_route_provider_source_counts"]["local_runtime_plus_ocr"] == 1
        assert created["campaign"]["ai_route_stack_name_counts"]["desktop_agent"] == 1
        assert created["campaign"]["ai_route_confident_count"] == 1
        assert created["campaign"]["ai_route_fallback_count"] == 0
        assert created["campaign"]["memory_mission_status_counts"]["strong"] == 1
        assert created["campaign"]["memory_mission_followthrough_count"] == 1
        assert created["campaign"]["query"] == "settings"
        assert created["campaign"]["query_hints_by_app"]["notepad"][0] == "settings"
        assert "ctrl+f" in {str(item).strip().lower() for item in created["campaign"]["semantic_hotkeys_by_app"]["notepad"]}
        assert created["campaign"]["top_memory_mission_queries"]["settings"] == 1
        assert created["campaign"]["top_memory_mission_hotkeys"]["Alt+F"] == 1

        campaign_id = str(created["campaign"]["campaign_id"])
        executed = supervisor.run_campaign(campaign_id=campaign_id, max_apps=1, source="manual")

        assert executed["status"] == "success"
        assert captured[-1]["query"] == "settings"
        assert captured[-1]["target_container_roles"] == ["dialog", "menu"]
        assert captured[-1]["preferred_wave_actions"] == ["open_command_palette", "focus_sidebar"]
        assert {"menu", "dialog", "tree"}.issubset(set(captured[-1]["preferred_traversal_paths"]))
        assert int(captured[-1]["max_surface_waves"] or 0) > 2
        assert captured[-1]["query_hints_by_app"]["notepad"][0] == "settings"
        assert "ctrl+f" in {str(item).strip().lower() for item in captured[-1]["semantic_hotkeys_by_app"]["notepad"]}
        assert captured[-1]["adaptive_app_profiles"][0]["adaptive_runtime_strategy_profile"] == "balanced_hybrid_guided_explore"
        assert executed["campaign"]["adaptive_target_container_roles"] is True
        assert executed["campaign"]["adaptive_surface_wave_depth"] is True
        assert executed["campaign"]["adaptive_preferred_wave_actions"] is True
        assert executed["campaign"]["adaptive_preferred_traversal_paths"] is True
        assert executed["campaign"]["adaptive_runtime_strategy_counts"]["balanced_hybrid_guided_explore"] == 1
        assert executed["campaign"]["runtime_band_counts"]["hybrid"] == 1
        assert executed["campaign"]["route_profile_counts"]["local_vision_assist"] == 1
        assert executed["campaign"]["model_preference_counts"]["hybrid_runtime"] == 1
        assert executed["campaign"]["provider_source_counts"]["local_runtime_plus_ocr"] == 1
        assert executed["campaign"]["ai_route_status_counts"]["matched"] == 1
        assert executed["campaign"]["ai_route_runtime_band_counts"]["hybrid"] == 1
        assert executed["campaign"]["ai_route_profile_counts"]["local_vision_assist_native_stabilized"] == 1
        assert executed["campaign"]["ai_route_provider_source_counts"]["local_runtime_plus_ocr"] == 1
        assert executed["campaign"]["ai_route_stack_name_counts"]["desktop_agent"] == 1
        assert executed["campaign"]["ai_route_confident_count"] == 1
        assert executed["campaign"]["ai_route_fallback_count"] == 0
        assert executed["campaign"]["memory_mission_status_counts"]["strong"] == 1
        assert int(executed["campaign"].get("route_fallback_app_count", 0) or 0) == 0
        assert executed["campaign"]["preferred_wave_actions"] == ["open_command_palette", "focus_sidebar"]
        assert {"menu", "dialog", "tree"}.issubset(set(executed["campaign"]["preferred_traversal_paths"]))
        assert {"menu", "dialog", "tree"}.issubset(set(executed["campaign"]["recommended_traversal_paths"]))
        assert executed["campaign"]["traversed_container_roles"] == ["dialog", "menu"]
        assert executed["campaign"]["executed_traversal_paths"] == ["dialog", "tree"]
        assert int(executed["campaign"].get("preferred_path_hits", 0) or 0) >= 2
        assert int(executed["campaign"].get("traversal_path_execution_count", 0) or 0) >= 2
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


def test_desktop_app_memory_supervisor_campaign_uses_setup_and_continuation_guidance(tmp_path: Path) -> None:
    supervisor = DesktopAppMemorySupervisor(
        state_path=str(Path(tmp_path) / "desktop_app_memory_supervisor_guided.json"),
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
            "message": "guided campaign executed",
            "surveyed_app_count": len(names),
            "success_count": len(names),
            "partial_count": 0,
            "error_count": 0,
            "skipped_app_count": 0,
            "items": [{"app_name": name, "status": "success", "message": "ok"} for name in names],
            "failed_apps": [],
            "wave_summary": {
                "wave_attempt_total": 3,
                "learned_surface_total": 2,
                "known_surface_total": 1,
            },
        }

    supervisor.start(_execute)
    try:
        created = supervisor.create_campaign(
            app_names=["notepad"],
            label="Guided learner",
            adaptive_app_profiles=[
                {
                    "app_name": "notepad",
                    "memory_mission": {
                        "status": "partial",
                        "seed_query": "settings",
                        "query_hints": ["settings"],
                        "hotkey_hints": ["Alt+F"],
                        "followthrough_recommended": True,
                    },
                    "recent_setup_followthrough_recommended": True,
                    "recent_setup_followthrough_required": True,
                    "recent_setup_guided_queries": ["advanced", "display"],
                    "recent_continuation_recommended": True,
                    "recent_continuation_top_memory_mission_queries": ["preferences"],
                    "recent_continuation_top_memory_mission_hotkeys": ["Ctrl+Shift+P"],
                    "provider_model_readiness": {
                        "memory_guidance_status": "partial",
                        "memory_route_alignment_status": "underused",
                    },
                }
            ],
            follow_surface_waves=True,
            max_surface_waves=2,
            max_probe_controls=3,
        )

        assert created["status"] == "success"
        assert created["campaign"]["setup_guided_target_count"] == 1
        assert created["campaign"]["continuation_guided_target_count"] == 1
        assert int(created["campaign"]["effective_max_surface_waves"] or 0) >= 5
        assert int(created["campaign"]["effective_max_probe_controls"] or 0) >= 6
        assert "focus_toolbar" in created["campaign"]["preferred_wave_actions"]
        assert "focus_navigation_tree" in created["campaign"]["preferred_wave_actions"]
        assert "menu" in created["campaign"]["target_container_roles"]
        assert "tree" in created["campaign"]["target_container_roles"]
        assert "advanced" in created["campaign"]["query_hints_by_app"]["notepad"]
        assert "preferences" in created["campaign"]["query_hints_by_app"]["notepad"]
        assert "Ctrl+Shift+P" in created["campaign"]["semantic_hotkeys_by_app"]["notepad"]

        campaign_id = str(created["campaign"]["campaign_id"])
        executed = supervisor.run_campaign(campaign_id=campaign_id, max_apps=1, source="manual")

        assert executed["status"] == "success"
        assert int(captured[-1]["max_surface_waves"] or 0) >= 5
        assert int(captured[-1]["max_probe_controls"] or 0) >= 6
        assert "focus_toolbar" in captured[-1]["preferred_wave_actions"]
        assert "focus_navigation_tree" in captured[-1]["preferred_wave_actions"]
        assert "menu" in captured[-1]["target_container_roles"]
        assert "tree" in captured[-1]["target_container_roles"]
        assert executed["campaign"]["setup_guided_target_count"] == 1
        assert executed["campaign"]["continuation_guided_target_count"] == 1
    finally:
        supervisor.stop()


def test_desktop_app_memory_supervisor_campaign_tracks_memory_guided_routes(tmp_path: Path) -> None:
    supervisor = DesktopAppMemorySupervisor(
        state_path=str(Path(tmp_path) / "desktop_app_memory_supervisor.json"),
        enabled=False,
        max_apps=1,
        per_app_limit=24,
    )

    def _execute(**kwargs: object) -> dict[str, object]:
        names = [str(item) for item in kwargs.get("app_names", [])] if isinstance(kwargs.get("app_names", []), list) else []
        return {
            "status": "success",
            "message": "memory-guided route campaign",
            "surveyed_app_count": len(names),
            "success_count": len(names),
            "partial_count": 0,
            "error_count": 0,
            "skipped_app_count": 0,
            "items": [{"app_name": name, "status": "success", "message": "ok"} for name in names],
            "failed_apps": [],
            "wave_summary": {
                "wave_attempt_total": len(names),
                "learned_surface_total": len(names),
                "known_surface_total": 0,
            },
        }

    supervisor.start(_execute)
    try:
        created = supervisor.create_campaign(
            app_names=["notepad"],
            label="Memory guided learner",
            adaptive_app_profiles=[
                {
                    "app_name": "notepad",
                    "learning_profile": "hybrid_guided_explore",
                    "execution_mode": "hybrid_ready",
                    "adaptive_runtime_strategy_profile": "memory_guided_hybrid_explore",
                    "runtime_band_preference": "hybrid",
                    "runtime_strategy": {
                        "strategy_profile": "memory_guided_hybrid_explore",
                        "runtime_band_preference": "hybrid",
                        "preferred_probe_mode": "local_vision_assist",
                    },
                    "provider_model_readiness": {
                        "ai_route_status": "matched",
                        "selected_ai_runtime_band": "hybrid",
                        "selected_ai_route_profile": "memory_guided_local_vision_assist_native_stabilized",
                        "selected_ai_provider_source": "local_runtime_plus_ocr",
                        "selected_ai_stack_names": ["desktop_agent", "perception", "memory"],
                        "ai_route_reason_codes": ["semantic_memory_route_bias", "structured_memory_ready"],
                    },
                }
            ],
        )
        assert created["status"] == "success"
        assert created["campaign"]["memory_guidance_status_counts"]["strong"] == 1
        assert created["campaign"]["memory_route_alignment_counts"]["aligned"] == 1
        assert created["campaign"]["memory_guided_route_count"] == 1
        assert created["campaign"]["memory_assisted_route_count"] == 0
        assert created["campaign"]["memory_followthrough_enabled"] is False
        assert created["campaign"]["memory_underused_count"] == 0
        assert created["campaign"]["memory_aligned_count"] == 1
        assert created["campaign"]["effective_max_probe_controls"] == 4

        campaign_id = str(created["campaign"]["campaign_id"])
        executed = supervisor.run_campaign(campaign_id=campaign_id, max_apps=1, source="manual")
        assert executed["status"] == "success"
        assert executed["campaign"]["memory_guidance_status_counts"]["strong"] == 1
        assert executed["campaign"]["memory_route_alignment_counts"]["aligned"] == 1
        assert executed["campaign"]["memory_guided_route_count"] == 1
        assert executed["campaign"]["memory_assisted_route_count"] == 0
        assert executed["campaign"]["memory_followthrough_enabled"] is False
        assert executed["campaign"]["memory_underused_count"] == 0
        assert executed["campaign"]["memory_aligned_count"] == 1
        assert executed["campaign"]["effective_max_probe_controls"] == 4
        assert executed["campaigns"]["summary"]["memory_guided_route_total"] >= 1
        assert executed["campaigns"]["summary"]["memory_guidance_status_counts"]["strong"] >= 1
    finally:
        supervisor.stop()


def test_desktop_app_memory_supervisor_memory_followthrough_boosts_campaign_depth(tmp_path: Path) -> None:
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
            "message": "memory followthrough campaign",
            "surveyed_app_count": len(names),
            "success_count": len(names),
            "partial_count": 0,
            "error_count": 0,
            "skipped_app_count": 0,
            "items": [
                {
                    "app_name": name,
                    "status": "success",
                    "message": "ok",
                    "memory_guided_route": False,
                    "memory_assisted_route": False,
                    "memory_route_alignment_status": "underused",
                }
                for name in names
            ],
            "failed_apps": [],
            "wave_summary": {
                "wave_attempt_total": len(names),
                "learned_surface_total": len(names),
                "known_surface_total": 0,
            },
        }

    supervisor.start(_execute)
    try:
        created = supervisor.create_campaign(
            app_names=["notepad"],
            label="Memory followthrough learner",
            max_probe_controls=3,
            adaptive_app_profiles=[
                {
                    "app_name": "notepad",
                    "learning_profile": "hybrid_guided_explore",
                    "execution_mode": "hybrid_ready",
                    "adaptive_runtime_strategy_profile": "balanced_hybrid_guided_explore",
                    "runtime_band_preference": "hybrid",
                    "semantic_guidance_status": "strong",
                    "memory_guided_route": False,
                    "memory_assisted_route": False,
                    "memory_route_alignment_status": "underused",
                    "provider_model_readiness": {
                        "ai_route_status": "matched",
                        "selected_ai_runtime_band": "hybrid",
                        "selected_ai_route_profile": "local_vision_assist_native_stabilized",
                        "selected_ai_provider_source": "local_runtime_plus_ocr",
                        "selected_ai_stack_names": ["desktop_agent", "perception", "memory"],
                    },
                }
            ],
        )
        assert created["status"] == "success"
        assert created["campaign"]["memory_followthrough_enabled"] is True
        assert created["campaign"]["memory_underused_count"] == 1
        assert created["campaign"]["memory_aligned_count"] == 0
        assert created["campaign"]["effective_max_probe_controls"] == 5
        assert created["campaign"]["memory_followthrough_preferred_wave_actions"][:2] == [
            "focus_navigation_tree",
            "focus_list_surface",
        ]

        campaign_id = str(created["campaign"]["campaign_id"])
        executed = supervisor.run_campaign(campaign_id=campaign_id, max_apps=1, source="manual")
        assert executed["status"] == "success"
        assert captured[-1]["max_probe_controls"] == 5
        assert executed["campaign"]["memory_followthrough_enabled"] is True
        assert executed["campaign"]["memory_underused_count"] == 1
        assert executed["campaign"]["effective_max_probe_controls"] == 5
        assert executed["campaign"]["memory_route_alignment_counts"]["underused"] == 1
        assert executed["campaigns"]["summary"]["memory_followthrough_total"] >= 1
        assert executed["campaigns"]["summary"]["memory_underused_total"] >= 1
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
                    "wave_strategy_summary": {
                        "recommended_actions": ["open_command_palette", "focus_sidebar"],
                    },
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
        assert captured[-1]["preferred_wave_actions"] == ["open_command_palette", "focus_sidebar"]
        assert int(captured[-1]["max_surface_waves"] or 0) > 1
        latest = payload["supervisor"]["latest_run"]
        assert latest["adaptive_target_container_roles"] is True
        assert latest["adaptive_preferred_wave_actions"] is True
        assert latest["preferred_wave_actions"] == ["open_command_palette", "focus_sidebar"]
        assert latest["adaptive_surface_wave_depth"] is True
        assert int(latest["effective_max_surface_waves"] or 0) > int(latest["max_surface_waves"] or 0)
        assert latest["selection_summary"]["top_revalidation_container_roles"][0]["value"] == "menu"
    finally:
        supervisor.stop()
