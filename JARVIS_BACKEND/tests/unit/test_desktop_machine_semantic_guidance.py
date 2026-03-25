from __future__ import annotations

from backend.python.desktop_api import DesktopBackendService


def test_desktop_machine_prepare_app_control_uses_semantic_memory_guidance() -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)

    service.desktop_machine_profile = lambda **_kwargs: {"status": "success"}
    service.desktop_machine_app_learning_plan = lambda **_kwargs: {
        "status": "success",
        "plan": {
            "targets": [
                {
                    "app_name": "Notepad",
                    "category": "utility",
                    "status": "attention",
                    "usage_score": 12.0,
                    "reason_codes": ["high_usage"],
                    "target_container_roles": ["dialog"],
                    "preferred_wave_actions": ["focus_form_surface"],
                    "preferred_traversal_paths": ["dialog"],
                    "recommended_queries": ["settings"],
                    "recommended_max_surface_waves": 3,
                }
            ],
            "campaign_defaults": {
                "target_container_roles": ["dialog"],
                "preferred_wave_actions": ["focus_form_surface"],
                "preferred_traversal_paths": ["dialog"],
                "recommended_queries": ["settings"],
                "max_surface_waves": 3,
            },
        },
    }
    service.model_setup_plan = lambda **_kwargs: {"status": "success"}
    service._desktop_machine_select_model_items = lambda **_kwargs: {}
    service._desktop_machine_onboarding_provider_actions = lambda **_kwargs: {}
    service.desktop_app_launcher_resolve = lambda **_kwargs: {
        "status": "success",
        "requested_app": "notepad",
        "display_name": "Notepad",
        "path": r"C:\Windows\notepad.exe",
        "profile": {},
    }
    service._desktop_machine_prepare_readiness_annotation = lambda **_kwargs: {
        "prepare_priority_score": 22.0,
        "prepare_priority_band": "medium",
        "auto_prepare_allowed": True,
        "execution_mode": "hybrid_ready",
        "readiness_status": "ready",
        "required_tasks": ["control", "vision"],
        "related_setup_action_codes": [],
        "blocker_codes": [],
        "blocker_count": 0,
    }
    service._desktop_machine_learning_strategy_for_target = lambda **_kwargs: {
        "learning_profile": "hybrid_guided_explore",
        "auto_learn_allowed": True,
        "effective_per_app_limit": 24,
        "effective_max_surface_waves": 3,
        "effective_max_probe_controls": 2,
        "prefer_failure_memory": True,
        "revalidate_known_controls": True,
    }
    service._desktop_machine_learning_runtime_strategy_for_target = lambda **_kwargs: {
        "strategy_profile": "hybrid_guided_explore",
        "runtime_band_preference": "hybrid",
    }
    service._desktop_machine_expected_runtime_route = lambda **_kwargs: {
        "expected_route_profile": "hybrid_verify",
        "expected_model_preference": "hybrid_runtime",
        "expected_provider_source": "local_runtime_plus_ocr",
    }
    service._desktop_machine_ai_route_plan = lambda **_kwargs: {
        "ai_route_status": "matched",
        "ai_route_confidence": 0.81,
        "ai_route_confidence_band": "high",
        "ai_route_fallback_applied": False,
        "selected_ai_runtime_band": "hybrid",
        "selected_ai_route_profile": "hybrid_verify",
        "selected_ai_model_preference": "hybrid_runtime",
        "selected_ai_provider_source": "local_runtime_plus_ocr",
        "selected_ai_reasoning_stack": "desktop_agent",
        "selected_ai_vision_stack": "perception",
        "selected_ai_memory_stack": "memory",
        "selected_ai_stack_names": ["desktop_agent", "perception", "memory"],
        "ai_route_reason_codes": ["matched_runtime"],
        "ai_runtime_status": "ready",
        "ai_runtime_ready_stack_count": 2,
        "ai_runtime_blocked_stack_count": 0,
        "ai_runtime_action_required_task_count": 0,
        "ai_runtime_reasoning_ready": True,
        "ai_runtime_vision_ready": True,
        "ai_runtime_setup_action_count": 0,
    }
    service.desktop_app_launcher_launch = lambda **_kwargs: {
        "status": "success",
        "launch_method": "launch_memory",
    }

    captured_survey: dict[str, object] = {}

    def _survey_desktop_app_memory(**kwargs):
        captured_survey.update(kwargs)
        return {
            "status": "success",
            "query": kwargs.get("query", ""),
            "targeting": {
                "target_container_roles": list(kwargs.get("target_container_roles", []) or []),
                "preferred_wave_actions": list(kwargs.get("preferred_wave_actions", []) or []),
            },
            "adaptive_learning_runtime": {
                "route_profile": "hybrid_verify",
                "model_preference": "hybrid_runtime",
                "runtime_provider_source": "local_runtime_plus_ocr",
                "route_resolution_status": "matched",
            },
            "memory_entry": {
                "discovered_control_count": 3,
                "known_surface_count": 1,
                "metrics": {
                    "known_surface_count": 1,
                    "wave_attempt_count": 2,
                },
            },
            "probe_report": {
                "attempted_count": 1,
                "successful_count": 1,
            },
        }

    service.survey_desktop_app_memory = _survey_desktop_app_memory
    service.desktop_app_memory_status = lambda **_kwargs: {"status": "success", "count": 1, "total": 1}
    service.desktop_app_launcher_memory = lambda **_kwargs: {"status": "success", "count": 1}
    service.desktop_app_memory_semantic_search = lambda **_kwargs: {
        "status": "success",
        "count": 2,
        "items": [
            {
                "label": "Settings",
                "control_type": "menuitem",
                "container_role": "menu",
                "semantic_role": "settings",
                "hotkeys": ["Alt+F"],
                "similarity": 0.93,
            },
            {
                "label": "Find",
                "control_type": "edit",
                "container_role": "",
                "semantic_role": "search",
                "hotkeys": ["Ctrl+F"],
                "similarity": 0.88,
            },
        ],
    }

    payload = service.desktop_machine_prepare_app_control(
        task="control",
        app_name="notepad",
        query="settings",
        ensure_app_launch=False,
    )

    assert payload["status"] == "success"
    assert payload["semantic_memory_guidance"]["guidance_status"] == "strong"
    assert payload["summary"]["semantic_guidance_match_count"] == 2
    assert payload["summary"]["semantic_guidance_alignment"] == "matched"
    assert "menu" in list(captured_survey.get("target_container_roles", []))
    assert "focus_toolbar" in list(captured_survey.get("preferred_wave_actions", []))
    assert "focus_search_box" in list(captured_survey.get("preferred_wave_actions", []))


def test_desktop_machine_app_learning_plan_tracks_semantic_guidance() -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)

    service._desktop_machine_memory_knowledge_profile = lambda *, memory_entry=None: {
        "entry_count": 1 if isinstance(memory_entry, dict) and memory_entry else 0,
        "control_count": 2 if isinstance(memory_entry, dict) and memory_entry else 0,
        "command_count": 1 if isinstance(memory_entry, dict) and memory_entry else 0,
        "vector_count": 3 if isinstance(memory_entry, dict) and memory_entry else 0,
        "hotkey_count": 1 if isinstance(memory_entry, dict) and memory_entry else 0,
        "semantic_memory_available": bool(memory_entry),
        "gap_level": "cold" if isinstance(memory_entry, dict) and memory_entry else "thin",
        "gap_reasons": ["low_controls"] if isinstance(memory_entry, dict) and memory_entry else ["new_surface"],
        "coverage_score": 0.24 if isinstance(memory_entry, dict) and memory_entry else 0.08,
        "surface_node_count": 1,
        "surface_transition_count": 0,
    }
    service._desktop_machine_app_learning_defaults = lambda **_kwargs: {
        "target_container_roles": ["dialog"],
        "preferred_wave_actions": ["focus_form_surface"],
        "preferred_traversal_paths": ["dialog"],
        "recommended_queries": ["settings"],
        "recommended_max_surface_waves": 3,
    }
    service._desktop_machine_semantic_memory_guidance = lambda *, app_name="", **_kwargs: (
        {
            "status": "success",
            "query": "settings",
            "count": 2,
            "items": [],
            "guidance_status": "strong",
            "top_similarity": 0.93,
            "recommended_container_roles": ["menu", "toolbar"],
            "recommended_wave_actions": ["focus_toolbar", "focus_search_box"],
            "recommended_traversal_paths": ["menu"],
            "recommended_queries": ["settings", "preferences"],
            "top_match_labels": ["Settings", "Preferences"],
            "top_hotkeys": ["Alt+F", "Ctrl+F"],
            "reason_codes": ["semantic_match_settings", "high_confidence_semantic_match"],
        }
        if str(app_name or "").strip().lower() == "notepad"
        else {
            "status": "success",
            "query": "settings",
            "count": 0,
            "items": [],
            "guidance_status": "cold",
            "top_similarity": 0.0,
            "recommended_container_roles": [],
            "recommended_wave_actions": [],
            "recommended_traversal_paths": [],
            "recommended_queries": ["settings"],
            "top_match_labels": [],
            "top_hotkeys": [],
            "reason_codes": ["no_semantic_match"],
        }
    )

    plan = service._desktop_machine_app_learning_plan_payload(
        app_inventory={
            "total": 2,
            "items": [
                {
                    "display_name": "Notepad",
                    "canonical_name": "windows_notepad",
                    "category": "utility",
                    "usage_score": 9.0,
                    "path_ready": True,
                    "path": r"C:\Windows\notepad.exe",
                },
                {
                    "display_name": "Calculator",
                    "canonical_name": "windows_calculator",
                    "category": "utility",
                    "usage_score": 6.0,
                    "path_ready": True,
                    "path": r"C:\Windows\System32\calc.exe",
                },
            ],
        },
        app_memory={
            "total": 1,
            "items": [
                {
                    "app_name": "Notepad",
                    "discovered_control_count": 2,
                    "metrics": {"survey_count": 1},
                    "knowledge_store": {
                        "entry_count": 1,
                        "control_count": 2,
                        "command_count": 1,
                        "vector_count": 3,
                        "hotkey_count": 1,
                    },
                }
            ],
        },
        task_focus=[{"task": "vision"}],
        max_targets=2,
    )

    assert plan["status"] == "success"
    assert plan["targets"][0]["app_name"] == "Notepad"
    assert plan["targets"][0]["semantic_guidance_status"] == "strong"
    assert "menu" in plan["targets"][0]["target_container_roles"]
    assert "focus_toolbar" in plan["targets"][0]["preferred_wave_actions"]
    assert plan["summary"]["semantic_guided_count"] == 1
    assert plan["summary"]["semantic_guidance_status_counts"]["strong"] == 1
    assert plan["summary"]["top_semantic_match_labels"]["Settings"] == 1
    assert plan["campaign_defaults"]["semantic_guided_count"] == 1

    service.model_setup_plan = lambda **_kwargs: {"status": "success"}
    service._desktop_machine_select_model_items = lambda **_kwargs: {}
    service._desktop_machine_onboarding_provider_actions = lambda **_kwargs: {}
    service._desktop_machine_recent_route_feedback = lambda **_kwargs: {"feedback_by_app": {}}
    service._desktop_machine_apply_route_feedback = lambda *, target_row, recent_feedback=None: dict(target_row)
    service._desktop_machine_prepare_readiness_annotation = lambda **_kwargs: {
        "prepare_priority_score": 24.0,
        "prepare_priority_band": "high",
        "auto_prepare_allowed": True,
        "execution_mode": "hybrid_ready",
        "readiness_status": "ready",
        "required_tasks": ["control", "vision"],
        "related_setup_action_codes": [],
        "blocker_codes": [],
        "blocker_count": 0,
        "local_ready_tasks": ["control"],
        "remote_ready_tasks": ["vision"],
        "install_ready_tasks": [],
    }
    service._desktop_machine_learning_strategy_for_target = lambda **_kwargs: {
        "learning_profile": "hybrid_guided_explore",
        "auto_learn_allowed": True,
        "effective_per_app_limit": 24,
        "effective_max_surface_waves": 4,
        "effective_max_probe_controls": 3,
        "prefer_failure_memory": True,
        "revalidate_known_controls": True,
        "strategy_notes": "semantic-aware learning",
    }
    service._desktop_machine_learning_runtime_strategy_for_target = lambda **_kwargs: {
        "strategy_profile": "hybrid_guided_explore",
        "runtime_band_preference": "hybrid",
    }
    service._desktop_machine_expected_runtime_route = lambda **_kwargs: {
        "expected_route_profile": "local_vision_assist",
        "expected_model_preference": "hybrid_runtime",
        "expected_provider_source": "local_runtime_plus_ocr",
    }
    service._desktop_machine_ai_route_plan = lambda **_kwargs: {
        "ai_route_status": "matched",
        "ai_route_confidence": 0.84,
        "ai_route_confidence_band": "high",
        "ai_route_fallback_applied": False,
        "selected_ai_runtime_band": "hybrid",
        "selected_ai_route_profile": "local_vision_assist",
        "selected_ai_model_preference": "hybrid_runtime",
        "selected_ai_provider_source": "local_runtime_plus_ocr",
        "selected_ai_reasoning_stack": "desktop_agent",
        "selected_ai_vision_stack": "perception",
        "selected_ai_memory_stack": "memory",
        "selected_ai_stack_names": ["desktop_agent", "perception", "memory"],
        "ai_route_reason_codes": ["matched_runtime"],
        "ai_runtime_status": "ready",
        "ai_runtime_ready_stack_count": 2,
        "ai_runtime_blocked_stack_count": 0,
        "ai_runtime_action_required_task_count": 0,
        "ai_runtime_reasoning_ready": True,
        "ai_runtime_vision_ready": True,
        "ai_runtime_setup_action_count": 0,
    }

    finalized = service._desktop_machine_finalize_app_learning_plan(
        profile={"status": "success"},
        app_learning_plan=plan,
        task="control",
        max_targets=2,
    )

    assert finalized["summary"]["semantic_guided_count"] == 1
    assert finalized["summary"]["semantic_followup_count"] == 1
    assert finalized["campaign_defaults"]["semantic_guidance_status_counts"]["strong"] == 1
    assert finalized["campaign_defaults"]["top_semantic_match_labels"]["Settings"] == 1
    assert any(
        profile["app_name"] == "Notepad"
        and profile["semantic_guidance_status"] == "strong"
        and "Settings" in profile["semantic_guidance_top_labels"]
        for profile in finalized["campaign_defaults"]["adaptive_app_profiles"]
    )


def test_desktop_machine_onboarding_continuation_plan_adds_semantic_followup() -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)

    continuation = service._desktop_machine_onboarding_continuation_plan(
        execution_queue={"items": []},
        app_learning_plan={
            "plan": {
                "targets": [
                    {
                        "app_name": "Notepad",
                        "auto_learn_allowed": True,
                        "semantic_guidance_status": "cold",
                        "knowledge_gap_level": "cold",
                        "readiness_status": "ready",
                        "remediation_progress_status": "",
                        "remediation_retry_recommended": False,
                        "remediation_provider_blocked": False,
                        "remediation_setup_followup_required": False,
                        "remediation_recent_action_code": "",
                    }
                ]
            }
        },
        app_control_prepare_plan={"items": []},
        route_remediation={"items": []},
        route_remediation_progress={"items": []},
        limit=4,
    )

    assert continuation["status"] == "success"
    assert continuation["count"] == 1
    assert continuation["items"][0]["kind"] == "deepen_app_learning"
    assert continuation["items"][0]["semantic_followup_recommended"] is True
    assert continuation["summary"]["semantic_followup_count"] == 1
    assert continuation["summary"]["semantic_guidance_status_counts"]["cold"] == 1
