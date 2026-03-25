from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any, Dict

from backend.python.core.desktop_action_router import DesktopActionRouter
from backend.python.core.desktop_app_memory import DesktopAppMemory
from backend.python.core.desktop_workflow_memory import DesktopWorkflowMemory


def _isolated_workflow_memory() -> DesktopWorkflowMemory:
    return DesktopWorkflowMemory(store_path=str(Path(tempfile.mkdtemp()) / "desktop_workflow_memory.json"))


def _isolated_app_memory() -> DesktopAppMemory:
    return DesktopAppMemory(store_path=str(Path(tempfile.mkdtemp()) / "desktop_app_memory.json"))


def test_desktop_app_memory_records_learned_controls_and_shortcuts(tmp_path: Path) -> None:
    memory = DesktopAppMemory(store_path=str(tmp_path / "desktop_app_memory.json"))

    entry = memory.record_survey(
        app_name="notepad",
        query="save",
        app_profile={"profile_id": "notepad", "name": "Notepad", "category": "utility"},
        launch_result={"status": "success", "launch_method": "system_path"},
        snapshot={
            "target_window": {"title": "Untitled - Notepad", "window_signature": "notepad-main"},
            "active_window": {"title": "Untitled - Notepad"},
            "elements": {
                "items": [
                    {
                        "element_id": "file_menu",
                        "name": "File",
                        "control_type": "menuitem",
                        "automation_id": "FileMenu",
                        "root_window_title": "Untitled - Notepad",
                    },
                    {
                        "element_id": "save_button",
                        "name": "Save",
                        "control_type": "button",
                        "automation_id": "SaveButton",
                        "root_window_title": "Untitled - Notepad",
                    },
                ]
            },
            "surface_summary": {
                "control_counts": {"menuitem": 1, "button": 1},
                "top_labels": [{"label": "Save", "count": 1}, {"label": "File", "count": 1}],
                "query_candidates": [{"name": "Save", "control_type": "button"}],
                "recommended_actions": ["click", "command"],
                "confirmation_candidates": ["Save"],
                "destructive_candidates": [],
                "control_inventory": [
                    {"name": "Save", "control_type": "button", "automation_id": "SaveButton"},
                    {"name": "File", "control_type": "menuitem", "automation_id": "FileMenu"},
                ],
            },
            "surface_intelligence": {"surface_role": "editor", "interaction_mode": "keyboard_first"},
            "workflow_surfaces": [{"action": "search", "primary_hotkey": ["ctrl+f"]}],
            "recommended_actions": ["search"],
            "native_window_topology": {
                "signature": "native-notepad",
                "descendant_chain_depth": 1,
                "same_process_window_count": 1,
                "related_window_count": 1,
                "dialog_like_window_count": 0,
            },
            "window_reacquisition": {"candidate": {"title": "Untitled - Notepad"}},
        },
        exploration_plan={
            "branch_actions": [{"action": "focus_related_window"}],
            "top_hypotheses": [{"label": "Save"}],
        },
    )

    assert entry["app_name"] == "notepad"
    assert entry["profile_id"] == "notepad"
    assert entry["discovered_control_count"] == 2
    assert entry["metrics"]["survey_count"] == 1
    assert entry["metrics"]["launch_success_count"] == 1
    assert any(str(item.get("value", "")) == "save" for item in entry["command_candidates"])
    assert any(str(item.get("action", "")) == "search" for item in entry["shortcut_actions"])

    snapshot = memory.snapshot(app_name="note")
    assert snapshot["status"] == "success"
    assert snapshot["count"] == 1
    assert snapshot["summary"]["survey_count_total"] == 1
    assert snapshot["summary"]["discovered_control_total"] == 2


def test_desktop_action_router_surveys_app_memory_and_returns_snapshot() -> None:
    def _open_app(_payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "success", "requested_app": "notepad", "launch_method": "system_path"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 501, "title": "Untitled - Notepad", "exe": r"C:\Windows\notepad.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 501, "title": "Untitled - Notepad"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "open_app": _open_app,
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {
                        "element_id": "file_menu",
                        "name": "File\tAlt+F",
                        "control_type": "menuitem",
                        "automation_id": "FileMenu",
                        "root_window_title": "Untitled - Notepad",
                        "access_key": "F",
                    },
                    {
                        "element_id": "save_button",
                        "name": "Save\tCtrl+S",
                        "control_type": "button",
                        "automation_id": "SaveButton",
                        "class_name": "ToolbarButton",
                        "root_window_title": "Untitled - Notepad",
                        "accelerator_key": "Ctrl+S",
                    },
                ],
            },
        },
        workflow_memory=_isolated_workflow_memory(),
        app_memory=_isolated_app_memory(),
        settle_delay_s=0.0,
    )

    payload = router.survey_app_memory(
        app_name="notepad",
        query="save",
        ensure_app_launch=True,
        include_observation=False,
    )

    assert payload["status"] == "success"
    assert payload["launch_result"]["status"] == "success"
    assert payload["surface_snapshot"]["status"] == "success"
    assert payload["memory_entry"]["discovered_control_count"] >= 2
    assert payload["app_memory"]["count"] == 1
    assert "Learned" in str(payload["message"])
    assert "Semantic harvesting captured" in str(payload["message"])

    repeated = router.survey_app_memory(
        app_name="notepad",
        query="save",
        ensure_app_launch=True,
        include_observation=False,
    )

    assert repeated["status"] == "success"
    assert bool(dict(repeated.get("surface_hint", {})).get("known", False)) is True
    assert int(dict(repeated.get("semantic_memory_guidance", {})).get("count", 0) or 0) >= 1
    assert "Save" in list(dict(repeated.get("semantic_memory_guidance", {})).get("top_match_labels", []))
    assert "Known surface memory was reused" in str(repeated["message"])
    assert "Vector memory surfaced" in str(repeated["message"])


def test_desktop_app_memory_surfaces_revalidation_targets_for_uncertain_controls(tmp_path: Path) -> None:
    memory = DesktopAppMemory(store_path=str(tmp_path / "desktop_app_memory.json"))

    entry = memory.record_survey(
        app_name="settings",
        query="sync",
        app_profile={"profile_id": "settings", "name": "Settings", "category": "system"},
        snapshot={
            "surface_fingerprint": "settings|main|surface",
            "target_window": {"title": "Settings"},
            "active_window": {"title": "Settings"},
            "surface_summary": {
                "control_counts": {"button": 1},
                "top_labels": [{"label": "Sync", "count": 1}],
                "recommended_actions": ["click"],
                "control_inventory": [{"name": "Sync", "control_type": "button", "automation_id": "SyncButton"}],
            },
            "surface_intelligence": {"surface_role": "settings", "interaction_mode": "hybrid"},
            "elements": {
                "items": [
                    {
                        "element_id": "sync_button",
                        "name": "Sync",
                        "control_type": "button",
                        "automation_id": "SyncButton",
                        "root_window_title": "Settings",
                    }
                ]
            },
            "workflow_surfaces": [],
        },
        probe_report={
            "status": "partial",
            "attempted_count": 1,
            "successful_count": 1,
            "verified_count": 0,
            "uncertain_count": 1,
            "items": [
                {
                    "label": "Sync",
                    "control_type": "button",
                    "element_id": "sync_button",
                    "automation_id": "SyncButton",
                    "probe_status": "success",
                    "method": "accessibility_invoke_element",
                    "effect_kind": "no_observed_change",
                    "semantic_role": "action",
                    "effect_summary": "No reliable visible effect was observed.",
                    "verification_confidence": 0.28,
                    "verification_summary": {"verified_effect": False, "confidence": 0.28},
                    "pre_surface_fingerprint": "settings|main|surface",
                    "post_surface_fingerprint": "settings|main|surface",
                }
            ],
        },
        source="manual",
    )

    assert entry["revalidation_summary"]["target_count"] >= 1
    assert any(
        str(item.get("label", "")).strip().lower() == "sync"
        for item in entry["revalidation_targets"]
    )

    snapshot = memory.snapshot(app_name="settings")
    assert snapshot["revalidation"]["count"] >= 1
    first_target = dict(snapshot["revalidation"]["items"][0])
    assert first_target["label"].lower() == "sync"
    assert first_target["revalidation_due"] is True
    assert any(
        reason in {"never_verified", "low_confidence", "uncertain_effect"}
        for reason in first_target["reason_codes"]
    )


def test_desktop_app_memory_revalidation_targets_filter_by_surface_and_role(tmp_path: Path) -> None:
    memory = DesktopAppMemory(store_path=str(tmp_path / "desktop_app_memory.json"))

    memory.record_survey(
        app_name="notepad",
        query="file",
        app_profile={"profile_id": "notepad", "name": "Notepad", "category": "utility"},
        snapshot={
            "surface_fingerprint": "notepad|menu|surface",
            "target_window": {"title": "Untitled - Notepad"},
            "active_window": {"title": "Untitled - Notepad"},
            "surface_summary": {
                "control_counts": {"menuitem": 1},
                "control_inventory": [{"name": "File", "control_type": "menuitem", "automation_id": "FileMenu"}],
            },
            "surface_intelligence": {"surface_role": "editor", "interaction_mode": "keyboard_first"},
            "elements": {
                "items": [
                    {
                        "element_id": "file_menu",
                        "name": "File",
                        "control_type": "menuitem",
                        "automation_id": "FileMenu",
                        "root_window_title": "Untitled - Notepad",
                    }
                ]
            },
        },
        probe_report={
            "status": "partial",
            "attempted_count": 1,
            "successful_count": 1,
            "verified_count": 0,
            "uncertain_count": 1,
            "items": [
                {
                    "label": "File",
                    "control_type": "menuitem",
                    "element_id": "file_menu",
                    "automation_id": "FileMenu",
                    "probe_status": "success",
                    "method": "accessibility_invoke_element",
                    "effect_kind": "surface_change",
                    "semantic_role": "navigation",
                    "verification_confidence": 0.41,
                    "verification_summary": {"verified_effect": False, "confidence": 0.41},
                    "pre_surface_fingerprint": "notepad|menu|surface",
                    "post_surface_fingerprint": "notepad|menu|surface",
                }
            ],
        },
    )
    memory.record_survey(
        app_name="notepad",
        query="project",
        app_profile={"profile_id": "notepad", "name": "Notepad", "category": "utility"},
        snapshot={
            "surface_fingerprint": "notepad|sidebar|surface",
            "target_window": {"title": "Untitled - Notepad"},
            "active_window": {"title": "Untitled - Notepad"},
            "surface_summary": {
                "control_counts": {"treeitem": 1},
                "control_inventory": [{"name": "Project", "control_type": "treeitem", "automation_id": "ProjectTree"}],
            },
            "surface_intelligence": {"surface_role": "editor", "interaction_mode": "hybrid"},
            "elements": {
                "items": [
                    {
                        "element_id": "project_tree",
                        "name": "Project",
                        "control_type": "treeitem",
                        "automation_id": "ProjectTree",
                        "root_window_title": "Untitled - Notepad",
                    }
                ]
            },
        },
        probe_report={
            "status": "partial",
            "attempted_count": 1,
            "successful_count": 1,
            "verified_count": 0,
            "uncertain_count": 1,
            "items": [
                {
                    "label": "Project",
                    "control_type": "treeitem",
                    "element_id": "project_tree",
                    "automation_id": "ProjectTree",
                    "probe_status": "success",
                    "method": "accessibility_invoke_element",
                    "effect_kind": "surface_change",
                    "semantic_role": "navigation",
                    "verification_confidence": 0.44,
                    "verification_summary": {"verified_effect": False, "confidence": 0.44},
                    "pre_surface_fingerprint": "notepad|sidebar|surface",
                    "post_surface_fingerprint": "notepad|sidebar|surface",
                }
            ],
        },
    )

    filtered = memory.revalidation_targets(
        app_name="notepad",
        surface_fingerprint="notepad|menu|surface",
        container_roles=["menu"],
        minimum_priority=1.0,
    )

    assert filtered["count"] == 1
    assert filtered["filters"]["surface_fingerprint"] == "notepad|menu|surface"
    assert filtered["filters"]["minimum_priority"] == 1.0
    assert filtered["items"][0]["label"].lower() == "file"
    assert filtered["items"][0]["container_role"] == "menu"
    assert filtered["summary"]["top_container_roles"][0]["value"] == "menu"


def test_desktop_app_memory_surface_hint_recommends_container_roles_from_wave_memory(tmp_path: Path) -> None:
    memory = DesktopAppMemory(store_path=str(tmp_path / "desktop_app_memory.json"))

    entry = memory.record_survey(
        app_name="demo-settings",
        query="advanced",
        app_profile={"profile_id": "demo-settings", "name": "Demo Settings", "category": "utility"},
        snapshot={
            "surface_fingerprint": "demo-settings|main|surface",
            "target_window": {"title": "Demo Settings"},
            "active_window": {"title": "Demo Settings"},
            "surface_summary": {
                "control_counts": {"tabitem": 1},
                "control_inventory": [{"name": "Advanced", "control_type": "tabitem", "automation_id": "AdvancedTab"}],
            },
            "surface_intelligence": {"surface_role": "settings", "interaction_mode": "hybrid"},
            "elements": {
                "items": [
                    {
                        "element_id": "advanced_tab",
                        "name": "Advanced",
                        "control_type": "tabitem",
                        "automation_id": "AdvancedTab",
                        "root_window_title": "Demo Settings",
                    }
                ]
            },
        },
        wave_report={
            "attempted_count": 1,
            "learned_surface_count": 1,
            "known_surface_count": 0,
            "stop_reason": "captured_linked_surface",
            "items": [
                {
                    "action": "traverse_tab_advanced",
                    "title": "Advanced",
                    "container_role": "tab",
                    "recommended_followups": ["traverse_sidebar", "traverse_dialog"],
                    "surface_fingerprint": "demo-settings|advanced|surface",
                    "pre_surface_fingerprint": "demo-settings|main|surface",
                    "post_surface_fingerprint": "demo-settings|advanced|surface",
                }
            ],
            "traversed_container_roles": ["tab"],
            "role_attempt_counts": {"tab": 1},
            "role_learned_counts": {"tab": 1},
            "strategy_profile": {"recommended_container_roles": ["tab", "sidebar"]},
            "recursive_depth_limit": 3,
        },
    )

    assert "tab" in dict(entry.get("wave_strategy_summary", {})).get("recommended_container_roles", [])

    hint = memory.surface_hint(
        app_name="demo-settings",
        profile_id="demo-settings",
        surface_fingerprint="demo-settings|main|surface",
    )

    assert hint["known"] is True
    assert "tab" in hint["recommended_wave_container_roles"]
    assert "tab" in hint["recommended_traversal_paths"]
    assert "sidebar" in hint["recommended_traversal_paths"]
    assert "tab" in dict(hint.get("wave_strategy_summary", {})).get("recommended_container_roles", [])


def test_desktop_action_router_surveys_app_memory_batch() -> None:
    def _open_app(payload: Dict[str, Any]) -> Dict[str, Any]:
        requested = str(payload.get("app_name", "") or "").strip()
        return {"status": "success", "requested_app": requested, "launch_method": "system_path"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 501, "title": "Untitled - Notepad", "exe": r"C:\Windows\notepad.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 501, "title": "Untitled - Notepad"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "open_app": _open_app,
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {
                        "element_id": "save_button",
                        "name": "Save",
                        "control_type": "button",
                        "automation_id": "SaveButton",
                        "root_window_title": "Untitled - Notepad",
                    }
                ],
            },
        },
        workflow_memory=_isolated_workflow_memory(),
        app_memory=_isolated_app_memory(),
        settle_delay_s=0.0,
    )

    payload = router.survey_app_memory_batch(
        query="note",
        max_apps=2,
        per_app_limit=16,
        ensure_app_launch=True,
        source="batch",
    )

    assert payload["status"] == "success"
    assert payload["surveyed_app_count"] >= 1
    assert payload["success_count"] >= 1
    assert payload["app_memory"]["status"] == "success"


def test_desktop_action_router_batch_adapts_targeting_from_revalidation_hotspots() -> None:
    app_memory = _isolated_app_memory()
    app_memory.record_survey(
        app_name="notepad",
        query="confirm",
        app_profile={"profile_id": "notepad", "name": "Notepad", "category": "utility"},
        snapshot={
            "surface_fingerprint": "notepad|dialog|surface",
            "target_window": {"title": "Untitled - Notepad"},
            "active_window": {"title": "Untitled - Notepad"},
            "surface_summary": {
                "control_counts": {"button": 2, "menuitem": 1},
                "control_inventory": [
                    {"name": "Confirm", "control_type": "button", "automation_id": "ConfirmButton"},
                    {"name": "Apply", "control_type": "button", "automation_id": "ApplyButton"},
                    {"name": "File", "control_type": "menuitem", "automation_id": "FileMenu"},
                ],
            },
            "surface_intelligence": {"surface_role": "editor", "interaction_mode": "hybrid"},
            "elements": {
                "items": [
                    {"element_id": "confirm_button", "name": "Confirm", "control_type": "button", "automation_id": "ConfirmButton"},
                    {"element_id": "apply_button", "name": "Apply", "control_type": "button", "automation_id": "ApplyButton"},
                    {"element_id": "file_menu", "name": "File", "control_type": "menuitem", "automation_id": "FileMenu"},
                ]
            },
        },
        probe_report={
            "status": "partial",
            "attempted_count": 3,
            "successful_count": 3,
            "verified_count": 0,
            "uncertain_count": 3,
            "items": [
                {
                    "label": "Confirm",
                    "control_type": "button",
                    "element_id": "confirm_button",
                    "automation_id": "ConfirmButton",
                    "probe_status": "success",
                    "method": "accessibility_invoke_element",
                    "effect_kind": "surface_change",
                    "semantic_role": "review_required",
                    "verification_confidence": 0.32,
                    "verification_summary": {"verified_effect": False, "confidence": 0.32},
                    "pre_surface_fingerprint": "notepad|dialog|surface",
                    "post_surface_fingerprint": "notepad|dialog|surface",
                },
                {
                    "label": "Apply",
                    "control_type": "button",
                    "element_id": "apply_button",
                    "automation_id": "ApplyButton",
                    "probe_status": "success",
                    "method": "accessibility_invoke_element",
                    "effect_kind": "surface_change",
                    "semantic_role": "review_required",
                    "verification_confidence": 0.35,
                    "verification_summary": {"verified_effect": False, "confidence": 0.35},
                    "pre_surface_fingerprint": "notepad|dialog|surface",
                    "post_surface_fingerprint": "notepad|dialog|surface",
                },
                {
                    "label": "File",
                    "control_type": "menuitem",
                    "element_id": "file_menu",
                    "automation_id": "FileMenu",
                    "probe_status": "success",
                    "method": "accessibility_invoke_element",
                    "effect_kind": "surface_change",
                    "semantic_role": "navigation",
                    "verification_confidence": 0.41,
                    "verification_summary": {"verified_effect": False, "confidence": 0.41},
                    "pre_surface_fingerprint": "notepad|dialog|surface",
                    "post_surface_fingerprint": "notepad|dialog|surface",
                },
            ],
        },
        wave_report={
            "attempted_count": 1,
            "learned_surface_count": 1,
            "known_surface_count": 0,
            "items": [
                {
                    "action": "command",
                    "title": "Command Palette",
                    "container_role": "dialog",
                    "recommended_followups": ["traverse_menu", "focus_sidebar"],
                    "surface_fingerprint": "notepad|command|surface",
                    "pre_surface_fingerprint": "notepad|dialog|surface",
                    "post_surface_fingerprint": "notepad|command|surface",
                }
            ],
            "traversed_container_roles": ["dialog"],
            "role_attempt_counts": {"dialog": 1},
            "role_learned_counts": {"dialog": 1},
            "strategy_profile": {"recommended_container_roles": ["dialog", "menu"]},
            "recursive_depth_limit": 4,
        },
    )

    router = DesktopActionRouter(
        action_handlers={},
        workflow_memory=_isolated_workflow_memory(),
        app_memory=app_memory,
        settle_delay_s=0.0,
    )
    captured: list[Dict[str, Any]] = []

    def _fake_survey_app_memory(**kwargs: Any) -> Dict[str, Any]:
        captured.append(dict(kwargs))
        return {
            "status": "success",
            "message": "ok",
            "memory_entry": {"app_name": kwargs.get("app_name", ""), "profile_id": "notepad", "metrics": {}},
            "wave_report": {"attempted_count": 0, "learned_surface_count": 0},
            "surface_hint": {},
            "app_memory": {"status": "success", "count": 1, "items": []},
        }

    object.__setattr__(router, "survey_app_memory", _fake_survey_app_memory)

    payload = router.survey_app_memory_batch(
        app_names=["notepad"],
        max_apps=1,
        probe_controls=False,
        follow_surface_waves=True,
        max_surface_waves=2,
        adaptive_app_profiles=[
            {
                "app_name": "notepad",
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

    assert payload["status"] == "success"
    assert captured[-1]["app_name"] == "notepad"
    assert captured[-1]["target_container_roles"][:2] == ["dialog", "menu"]
    assert "sidebar" in captured[-1]["target_container_roles"]
    assert captured[-1]["preferred_wave_actions"] == ["command"]
    assert int(captured[-1]["max_surface_waves"] or 0) > 2
    assert payload["wave_summary"]["adaptive_targeted_app_count"] == 1
    assert payload["wave_summary"]["adaptive_wave_depth_app_count"] == 1
    assert payload["targeting"]["runtime_strategy_counts"]["balanced_hybrid_guided_explore"] == 1
    assert payload["targeting"]["runtime_band_counts"]["hybrid"] == 1
    assert payload["targeting"]["route_profile_counts"]["local_vision_assist_native_stabilized"] == 1
    assert payload["targeting"]["model_preference_counts"]["hybrid_runtime"] == 1
    assert payload["targeting"]["runtime_provider_source_counts"]["local_runtime_plus_ocr"] == 1
    assert payload["targeting"]["ai_route_status_counts"]["matched"] == 1
    assert payload["targeting"]["ai_route_runtime_band_counts"]["hybrid"] == 1
    assert payload["targeting"]["ai_route_profile_counts"]["local_vision_assist_native_stabilized"] == 1
    assert payload["targeting"]["ai_route_provider_source_counts"]["local_runtime_plus_ocr"] == 1
    assert payload["targeting"]["ai_route_stack_name_counts"]["desktop_agent"] == 1
    assert payload["targeting"]["ai_route_confident_count"] == 1
    assert payload["targeting"]["ai_route_fallback_count"] == 0
    assert payload["targeting"]["route_fallback_app_count"] == 0
    assert payload["items"][0]["targeting"]["target_container_roles"][:2] == ["dialog", "menu"]
    assert "sidebar" in payload["items"][0]["targeting"]["target_container_roles"]
    assert payload["items"][0]["targeting"]["preferred_wave_actions"] == ["command"]
    assert payload["items"][0]["adaptive_learning_runtime"]["strategy_profile"] == "balanced_hybrid_guided_explore"
    assert payload["items"][0]["adaptive_learning_runtime"]["selected_runtime_band"] == "hybrid"
    assert payload["items"][0]["adaptive_learning_runtime"]["route_profile"] == "local_vision_assist_native_stabilized"
    assert payload["items"][0]["adaptive_learning_runtime"]["model_preference"] == "hybrid_runtime"
    assert payload["items"][0]["adaptive_learning_runtime"]["runtime_provider_source"] == "local_runtime_plus_ocr"
    assert payload["items"][0]["adaptive_learning_runtime"]["ai_route_status"] == "matched"
    assert payload["items"][0]["adaptive_learning_runtime"]["selected_ai_runtime_band"] == "hybrid"
    assert payload["items"][0]["adaptive_learning_runtime"]["selected_ai_route_profile"] == "local_vision_assist_native_stabilized"
    assert payload["items"][0]["adaptive_learning_runtime"]["selected_ai_reasoning_stack"] == "desktop_agent"
    assert payload["items"][0]["adaptive_learning_runtime"]["selected_ai_stack_names"] == [
        "desktop_agent",
        "perception",
        "memory",
    ]
    assert payload["targeting"]["route_resolution_counts"]["matched"] == 1
    recommended_paths = payload["items"][0]["targeting"]["recommended_traversal_paths"]
    assert recommended_paths[:2] == ["dialog", "menu"]
    assert "sidebar" in recommended_paths


def test_desktop_action_router_batch_skips_known_healthy_apps() -> None:
    app_memory = _isolated_app_memory()
    app_memory.record_survey(
        app_name="notepad",
        app_profile={"profile_id": "notepad", "name": "Notepad", "category": "utility"},
        snapshot={
            "surface_fingerprint": "notepad|main|surface",
            "surface_summary": {
                "control_counts": {"button": 3},
                "top_labels": [{"label": "Save", "count": 1}],
                "recommended_actions": ["search"],
                "surface_flags": {"search_visible": True},
            },
            "surface_intelligence": {"surface_role": "editor", "interaction_mode": "keyboard_first"},
            "elements": {"items": [{"name": "Save", "control_type": "button"}]},
            "workflow_surfaces": [{"action": "search", "primary_hotkey": ["ctrl+f"]}],
        },
        source="manual",
    )
    app_memory.record_survey(
        app_name="notepad",
        app_profile={"profile_id": "notepad", "name": "Notepad", "category": "utility"},
        snapshot={
            "surface_fingerprint": "notepad|main|surface",
            "surface_summary": {
                "control_counts": {"button": 3},
                "top_labels": [{"label": "Save", "count": 1}],
                "recommended_actions": ["search"],
                "surface_flags": {"search_visible": True},
            },
            "surface_intelligence": {"surface_role": "editor", "interaction_mode": "keyboard_first"},
            "elements": {"items": [{"name": "Save", "control_type": "button"}]},
            "workflow_surfaces": [{"action": "search", "primary_hotkey": ["ctrl+f"]}],
        },
        source="manual",
    )

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 501, "title": "Calculator", "exe": r"C:\Windows\System32\calc.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 501, "title": "Calculator"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "open_app": lambda payload: {"status": "success", "requested_app": str(payload.get("app_name", "")), "launch_method": "system_path"},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [{"element_id": "one", "name": "One", "control_type": "button", "automation_id": "One"}],
            },
        },
        workflow_memory=_isolated_workflow_memory(),
        app_memory=app_memory,
        settle_delay_s=0.0,
    )

    payload = router.survey_app_memory_batch(
        app_names=["notepad", "calculator"],
        query="note",
        max_apps=2,
        skip_known_apps=True,
        prefer_unknown_apps=True,
        ensure_app_launch=True,
        source="batch",
    )

    assert payload["status"] in {"success", "partial"}
    assert payload["surveyed_app_count"] == 1
    assert payload["skipped_app_count"] == 1
    assert payload["items"][0]["app_name"] == "calculator"
    assert payload["skipped_apps"][0]["app_name"] == "notepad"


def test_desktop_app_memory_reset_filters_by_app_name(tmp_path: Path) -> None:
    memory = DesktopAppMemory(store_path=str(tmp_path / "desktop_app_memory.json"))
    memory.record_survey(
        app_name="notepad",
        snapshot={"surface_summary": {}, "surface_intelligence": {}, "elements": {"items": []}},
    )
    memory.record_survey(
        app_name="calculator",
        snapshot={"surface_summary": {}, "surface_intelligence": {}, "elements": {"items": []}},
    )

    cleared = memory.reset(app_name="note")
    assert cleared["status"] == "success"
    assert cleared["removed"] == 1

    snapshot = memory.snapshot()
    assert snapshot["count"] == 1
    assert snapshot["items"][0]["app_name"] == "calculator"


def test_desktop_app_memory_tracks_learning_health_and_aliases(tmp_path: Path) -> None:
    memory = DesktopAppMemory(store_path=str(tmp_path / "desktop_app_memory.json"))
    memory.record_survey(
        app_name="settings",
        query="bluetooth",
        snapshot={
            "status": "success",
            "surface_summary": {},
            "surface_intelligence": {"surface_role": "settings"},
            "elements": {
                "items": [
                    {
                        "name": "Bluetooth",
                        "automation_id": "BluetoothButton",
                        "control_type": "button",
                        "access_key": "B",
                        "accelerator_key": "Ctrl+B",
                    }
                ]
            },
        },
        survey_status="success",
        source="daemon",
    )
    degraded = memory.record_survey(
        app_name="settings",
        query="bluetooth",
        snapshot={"status": "error", "message": "surface unavailable"},
        survey_status="error",
        error_message="surface unavailable",
        source="batch",
    )

    assert degraded["learning_health"]["failure_count"] == 1
    assert degraded["learning_health"]["status"] in {"attention", "degraded"}
    assert degraded["survey_sources"][0]["value"] in {"daemon", "batch"}
    top_control = degraded["top_controls"][0]
    assert "bluetoothbutton" in [str(item).lower() for item in top_control.get("command_aliases", [])]

    snapshot = memory.snapshot(app_name="settings")
    assert snapshot["summary"]["survey_failure_total"] == 1
    assert snapshot["summary"]["survey_source_counts"]["daemon"] == 1


def test_desktop_app_memory_harvests_menu_toolbar_ocr_and_hotkeys(tmp_path: Path) -> None:
    memory = DesktopAppMemory(store_path=str(tmp_path / "desktop_app_memory.json"))
    entry = memory.record_survey(
        app_name="word",
        query="save",
        app_profile={"profile_id": "word", "name": "Word", "category": "office"},
        snapshot={
            "target_window": {"title": "Document1 - Word", "window_signature": "word-main"},
            "active_window": {"title": "Document1 - Word"},
            "elements": {
                "items": [
                    {
                        "element_id": "file_menu",
                        "name": "File\tAlt+F",
                        "control_type": "menuitem",
                        "automation_id": "FileMenu",
                        "root_window_title": "Document1 - Word",
                        "access_key": "F",
                    },
                    {
                        "element_id": "save_as",
                        "name": "Save As\tCtrl+Shift+S",
                        "control_type": "button",
                        "automation_id": "RibbonSaveAs",
                        "class_name": "RibbonButton",
                        "root_window_title": "Document1 - Word",
                        "accelerator_key": "Ctrl+Shift+S",
                    },
                    {
                        "element_id": "home_tab",
                        "name": "Home",
                        "control_type": "tabitem",
                        "automation_id": "RibbonTabHome",
                        "class_name": "RibbonTab",
                    },
                ]
            },
            "observation": {
                "targets": [
                    {"text": "Find Ctrl+F"},
                    {"text": "Replace Ctrl+H"},
                ]
            },
            "surface_summary": {
                "control_counts": {"menuitem": 1, "button": 1, "tabitem": 1},
                "top_labels": [{"label": "Save As", "count": 1}, {"label": "File", "count": 1}],
                "query_candidates": [{"name": "Save As", "control_type": "button"}],
                "recommended_actions": ["command", "search"],
                "control_inventory": [
                    {"name": "File\tAlt+F", "control_type": "menuitem", "automation_id": "FileMenu"},
                    {"name": "Save As\tCtrl+Shift+S", "control_type": "button", "automation_id": "RibbonSaveAs"},
                ],
            },
            "surface_intelligence": {"surface_role": "editor", "interaction_mode": "keyboard_first"},
            "workflow_surfaces": [{"action": "search", "primary_hotkey": ["ctrl+f"]}],
        },
        survey_status="success",
        source="manual",
    )

    harvest_summary = dict(entry.get("harvest_summary", {}))
    assert int(harvest_summary.get("menu_command_count", 0) or 0) >= 1
    assert int(harvest_summary.get("ribbon_action_count", 0) or 0) >= 1
    assert int(harvest_summary.get("ocr_command_phrase_count", 0) or 0) >= 2
    assert int(harvest_summary.get("harvested_hotkey_count", 0) or 0) >= 3
    assert any(str(item.get("label", "")) == "save as" for item in entry.get("ribbon_actions", []))
    assert any(str(item.get("hotkey", "")) == "ctrl+shift+s" for item in entry.get("harvested_hotkeys", []))
    assert any(
        str(command.get("label", "")).lower() == "save as"
        and "ribbon_action" in [str(role) for role in command.get("semantic_roles", [])]
        for command in entry.get("learned_commands", [])
        if isinstance(command, dict)
    )

    snapshot = memory.snapshot(app_name="word")
    assert snapshot["summary"]["menu_command_total"] >= 1
    assert snapshot["summary"]["ribbon_action_total"] >= 1
    assert snapshot["summary"]["ocr_command_phrase_total"] >= 1
    assert snapshot["summary"]["harvested_hotkey_total"] >= 1

    hint = memory.surface_hint(app_name="word", profile_id="word", surface_fingerprint=str(entry.get("last_surface_fingerprint", "") or ""))
    assert hint["known"] is True
    assert any(str(item.get("label", "")) == "file" for item in hint.get("menu_commands", []))
    assert any(str(item.get("hotkey", "")) == "ctrl+shift+s" for item in hint.get("harvested_hotkeys", []))


def test_desktop_app_memory_records_probe_metrics_and_effects(tmp_path: Path) -> None:
    memory = DesktopAppMemory(store_path=str(tmp_path / "desktop_app_memory.json"))
    entry = memory.record_survey(
        app_name="explorer",
        query="view",
        snapshot={
            "status": "success",
            "surface_summary": {
                "query_candidates": [{"name": "View", "control_type": "menuitem"}],
            },
            "surface_intelligence": {"surface_role": "file_manager"},
            "elements": {
                "items": [
                    {
                        "element_id": "view_menu",
                        "name": "View",
                        "control_type": "menuitem",
                        "automation_id": "ViewMenu",
                    }
                ]
            },
        },
        probe_report={
            "status": "success",
            "candidate_count": 2,
            "ocr_target_count": 4,
            "attempted_count": 1,
            "successful_count": 1,
            "verified_count": 1,
            "uncertain_count": 0,
            "blocked_count": 0,
            "error_count": 0,
            "items": [
                {
                    "element_id": "view_menu",
                    "label": "View",
                    "control_type": "menuitem",
                    "automation_id": "ViewMenu",
                    "probe_status": "success",
                    "method": "accessibility_invoke_element",
                    "effect_kind": "navigation",
                    "semantic_role": "navigator",
                    "effect_summary": "Invoking View changed the visible app surface.",
                    "pre_surface_fingerprint": "explorer|main",
                    "post_surface_fingerprint": "explorer|view-menu",
                    "vision_labels": ["View"],
                    "verification_confidence": 0.91,
                    "verified_effect": True,
                    "verification_summary": {
                        "verified_effect": True,
                        "confidence": 0.91,
                        "screen_changed": True,
                        "surface_changed": True,
                    },
                    "native_learning_signals": {"custom_surface_suspected": False, "reparenting_risk": 0.1},
                }
            ],
        },
    )

    assert entry["metrics"]["probe_attempt_count"] == 1
    assert entry["metrics"]["probe_success_count"] == 1
    assert entry["metrics"]["ocr_target_count"] == 4
    assert entry["probe_summary"]["successful_count"] == 1
    assert entry["verification_summary"]["verified_count"] == 1
    assert entry["vision_summary"]["confidence"] == 0.0
    assert entry["version_profile"]["signature"]
    assert entry["staleness"]["stale"] is False
    assert entry["probe_effects"][0]["value"] == "navigation"
    assert entry["tested_controls"][0]["label"] == "view"
    assert entry["top_controls"][0]["last_probe_effect"] == "navigation"
    assert entry["failure_memory_summary"]["entry_count"] == 0

    snapshot = memory.snapshot(app_name="explorer")
    assert snapshot["summary"]["probe_attempt_total"] == 1
    assert snapshot["summary"]["probe_success_total"] == 1
    assert snapshot["summary"]["verified_effect_total"] == 1
    assert snapshot["summary"]["ocr_target_total"] == 4
    assert snapshot["items"][0]["surface_transitions"][0]["label"] == "View"


def test_desktop_action_router_surveys_app_memory_with_safe_probe_learning() -> None:
    observe_calls = {"count": 0}

    def _computer_observe(payload: Dict[str, Any]) -> Dict[str, Any]:
        observe_calls["count"] += 1
        include_targets = bool(payload.get("include_targets", False))
        if observe_calls["count"] == 1:
            return {
                "status": "success",
                "screen_hash": "hash-before",
                "text": "Notepad File Edit View",
                "screenshot_path": "before.png",
                "targets": (
                    [
                        {"text": "File", "confidence": 91.0},
                        {"text": "View", "confidence": 88.0},
                    ]
                    if include_targets
                    else []
                ),
            }
        return {
            "status": "success",
            "screen_hash": "hash-after",
            "text": "Notepad View menu open",
            "screenshot_path": "after.png",
            "targets": (
                [
                    {"text": "Zoom", "confidence": 90.0},
                        {"text": "Layout", "confidence": 84.0},
                    ]
                    if include_targets
                    else []
                ),
        }

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 501, "title": "Untitled - Notepad", "exe": r"C:\Windows\notepad.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 501, "title": "Untitled - Notepad"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "open_app": lambda _payload: {"status": "success", "requested_app": "notepad", "launch_method": "system_path"},
            "computer_observe": _computer_observe,
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {
                        "element_id": "file_menu",
                        "name": "File",
                        "control_type": "menuitem",
                        "automation_id": "FileMenu",
                        "root_window_title": "Untitled - Notepad",
                    },
                    {
                        "element_id": "view_menu",
                        "name": "View",
                        "control_type": "menuitem",
                        "automation_id": "ViewMenu",
                        "root_window_title": "Untitled - Notepad",
                    },
                ],
            },
            "accessibility_invoke_element": lambda payload: {
                "status": "success",
                "message": f"invoked {payload.get('element_id', payload.get('query', 'unknown'))}",
            },
            "computer_click_target": lambda payload: {
                "status": "success",
                "message": f"clicked {payload.get('query', 'unknown')}",
            },
        },
        workflow_memory=_isolated_workflow_memory(),
        app_memory=_isolated_app_memory(),
        settle_delay_s=0.0,
    )

    payload = router.survey_app_memory(
        app_name="notepad",
        query="view",
        ensure_app_launch=True,
        probe_controls=True,
        max_probe_controls=1,
        include_ocr_targets=True,
    )

    assert payload["status"] == "success"
    assert payload["probe_report"]["attempted_count"] == 1
    assert payload["probe_report"]["successful_count"] == 1
    assert payload["probe_report"]["ocr_target_count"] >= 2
    assert payload["memory_entry"]["metrics"]["probe_success_count"] == 1
    assert payload["memory_entry"]["tested_controls"][0]["label"] == "view"
    assert payload["memory_entry"]["probe_effects"][0]["value"] in {"navigation", "surface_change", "surface_wave"}
    assert payload["memory_entry"]["surface_transitions"]
    assert payload["memory_entry"]["surface_nodes"]


def test_desktop_action_router_surveys_linked_surface_waves_into_app_memory() -> None:
    state = {"surface": "main"}

    class _WaveRegistry:
        def match(self, *, app_name: str = "", window_title: str = "", exe_name: str = "") -> Dict[str, Any]:
            del window_title, exe_name
            clean_app_name = str(app_name or "").strip().lower()
            if clean_app_name not in {"vscode", "visual studio code", "code"}:
                return {}
            return {
                "status": "success",
                "profile_id": "vscode",
                "name": "Visual Studio Code",
                "category": "code_editor",
                "workflow_defaults": {
                    "command_hotkeys": [["ctrl+shift+p"]],
                    "quick_open_hotkeys": [["ctrl+p"]],
                    "workspace_search_hotkeys": [["ctrl+shift+f"]],
                    "search_hotkeys": [["ctrl+f"]],
                },
                "workflow_capabilities": {
                    "command": {"supported": True},
                    "quick_open": {"supported": True},
                    "workspace_search": {"supported": True},
                    "search": {"supported": True},
                },
            }

        def catalog(self, *, query: str = "", category: str = "", limit: int = 24) -> Dict[str, Any]:
            del query, category, limit
            return {"status": "success", "count": 0, "total": 0, "items": []}

    def _accessibility_rows() -> list[Dict[str, Any]]:
        if state["surface"] == "command":
            return [
                {
                    "element_id": "command_input",
                    "name": "Command Palette",
                    "control_type": "edit",
                    "automation_id": "CommandPaletteInput",
                    "root_window_title": "Visual Studio Code",
                }
            ]
        if state["surface"] == "quick_open":
            return [
                {
                    "element_id": "quick_open",
                    "name": "Go to File",
                    "control_type": "edit",
                    "automation_id": "QuickOpenInput",
                    "root_window_title": "Visual Studio Code",
                }
            ]
        if state["surface"] == "workspace_search":
            return [
                {
                    "element_id": "workspace_search",
                    "name": "Search Files",
                    "control_type": "edit",
                    "automation_id": "WorkspaceSearchInput",
                    "root_window_title": "Visual Studio Code",
                }
            ]
        return [
            {
                "element_id": "explorer_view",
                "name": "Explorer",
                "control_type": "treeitem",
                "automation_id": "ExplorerView",
                "root_window_title": "Visual Studio Code",
            }
        ]

    def _computer_observe(payload: Dict[str, Any]) -> Dict[str, Any]:
        include_targets = bool(payload.get("include_targets", False))
        text_map = {
            "main": "Explorer editor tab",
            "command": "Command Palette >",
            "quick_open": "Go to File",
            "workspace_search": "Search files",
        }
        return {
            "status": "success",
            "screen_hash": f"hash-{state['surface']}",
            "text": text_map[state["surface"]],
            "screenshot_path": f"{state['surface']}.png",
            "targets": (
                [{"text": "Command Palette"}, {"text": "Go to File"}, {"text": "Search files"}]
                if include_targets
                else []
            ),
        }

    def _keyboard_hotkey(payload: Dict[str, Any]) -> Dict[str, Any]:
        keys = [str(item).strip().lower() for item in payload.get("keys", []) if str(item).strip()]
        if keys == ["ctrl+shift+p"] and state["surface"] == "main":
            state["surface"] = "command"
        elif keys == ["ctrl+p"] and state["surface"] == "command":
            state["surface"] = "quick_open"
        elif keys == ["ctrl+shift+f"] and state["surface"] == "quick_open":
            state["surface"] = "workspace_search"
        return {"status": "success", "keys": keys, "surface": state["surface"]}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 901, "title": "Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 901, "title": "Visual Studio Code"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "open_app": lambda _payload: {"status": "success", "requested_app": "vscode", "launch_method": "system_path"},
            "keyboard_hotkey": _keyboard_hotkey,
            "computer_observe": _computer_observe,
            "accessibility_list_elements": lambda _payload: {"status": "success", "items": _accessibility_rows()},
        },
        app_profile_registry=_WaveRegistry(),
        workflow_memory=_isolated_workflow_memory(),
        app_memory=_isolated_app_memory(),
        settle_delay_s=0.0,
    )

    payload = router.survey_app_memory(
        app_name="vscode",
        query="search workspace",
        ensure_app_launch=True,
        include_observation=True,
        include_ocr_targets=True,
        follow_surface_waves=True,
        max_surface_waves=3,
    )

    assert payload["status"] == "success"
    assert "Linked-surface learning opened" in str(payload["message"])
    wave_report = dict(payload.get("wave_report", {}))
    assert int(wave_report.get("learned_surface_count", 0) or 0) == 3
    assert len([row for row in wave_report.get("items", []) if isinstance(row, dict)]) == 3
    strategy_profile = dict(wave_report.get("strategy_profile", {}))
    assert strategy_profile
    assert isinstance(strategy_profile.get("top_actions", []), list)

    memory_entry = dict(payload.get("memory_entry", {}))
    metrics = dict(memory_entry.get("metrics", {}))
    assert int(metrics.get("wave_survey_count", 0) or 0) >= 3
    assert int(metrics.get("wave_attempt_count", 0) or 0) >= 3

    app_memory = dict(payload.get("app_memory", {}))
    summary = dict(app_memory.get("summary", {}))
    assert int(summary.get("wave_survey_total", 0) or 0) >= 3
    assert int(summary.get("wave_attempt_total", 0) or 0) >= 3

    rows = [dict(row) for row in app_memory.get("items", []) if isinstance(row, dict)]
    assert rows
    transitions = [dict(row) for row in rows[0].get("surface_transitions", []) if isinstance(row, dict)]
    labels = {str(row.get("label", "") or "").strip().lower() for row in transitions}
    assert "command palette" in labels
    assert "quick open" in labels
    assert "workspace search" in labels
    assert rows[0]["wave_strategies"]
    assert "recommended_actions" in dict(rows[0].get("wave_strategy_summary", {}))


def test_desktop_action_router_surveys_safe_traversal_wave_candidates() -> None:
    state = {"surface": "main"}

    def _accessibility_rows() -> list[Dict[str, Any]]:
        if state["surface"] == "main":
            return [
                {
                    "element_id": "advanced_tab",
                    "name": "Advanced",
                    "control_type": "tabitem",
                    "automation_id": "AdvancedTab",
                    "root_window_title": "Demo Settings",
                },
                {
                    "element_id": "general_tab",
                    "name": "General",
                    "control_type": "tabitem",
                    "automation_id": "GeneralTab",
                    "root_window_title": "Demo Settings",
                },
            ]
        return [
            {
                "element_id": "expert_toggle",
                "name": "Expert Mode",
                "control_type": "togglebutton",
                "automation_id": "ExpertToggle",
                "root_window_title": "Demo Settings",
            }
        ]

    def _computer_observe(payload: Dict[str, Any]) -> Dict[str, Any]:
        include_targets = bool(payload.get("include_targets", False))
        if state["surface"] == "main":
            return {
                "status": "success",
                "screen_hash": "demo-main",
                "text": "Demo Settings General Advanced",
                "targets": (
                    [
                        {"text": "General", "confidence": 90.0},
                        {"text": "Advanced", "confidence": 92.0},
                    ]
                    if include_targets
                    else []
                ),
            }
        return {
            "status": "success",
            "screen_hash": "demo-advanced",
            "text": "Advanced Settings Expert Mode Diagnostics",
            "targets": (
                [
                    {"text": "Expert Mode", "confidence": 93.0},
                    {"text": "Diagnostics", "confidence": 85.0},
                ]
                if include_targets
                else []
            ),
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        if str(payload.get("element_id", "") or "").strip() == "advanced_tab":
            state["surface"] = "advanced"
        return {"status": "success", "message": "invoked"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1777, "title": "Demo Settings", "exe": r"C:\Demo\settings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1777, "title": "Demo Settings"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "open_app": lambda _payload: {"status": "success", "requested_app": "demo-settings", "launch_method": "system_path"},
            "computer_observe": _computer_observe,
            "accessibility_list_elements": lambda _payload: {"status": "success", "items": _accessibility_rows()},
            "accessibility_invoke_element": _invoke,
        },
        workflow_memory=_isolated_workflow_memory(),
        app_memory=_isolated_app_memory(),
        settle_delay_s=0.0,
    )

    payload = router.survey_app_memory(
        app_name="demo-settings",
        query="advanced diagnostics",
        ensure_app_launch=True,
        include_observation=True,
        include_ocr_targets=True,
        follow_surface_waves=True,
        max_surface_waves=1,
    )

    assert payload["status"] == "success"
    wave_report = dict(payload.get("wave_report", {}))
    assert int(wave_report.get("learned_surface_count", 0) or 0) == 1
    assert any(str(item.get("method", "") or "") == "accessibility_invoke_element" for item in wave_report.get("items", []) if isinstance(item, dict))
    assert "tab" in [str(item).strip().lower() for item in wave_report.get("traversed_container_roles", []) if str(item).strip()]
    assert int(dict(wave_report.get("role_learned_counts", {})).get("tab", 0) or 0) >= 1
    memory_entry = dict(payload.get("memory_entry", {}))
    assert memory_entry["safe_traversal_summary"]["candidate_count"] >= 1
    assert memory_entry["surface_transitions"]
    assert "tab" in [
        str(item).strip().lower()
        for item in dict(memory_entry.get("wave_strategy_summary", {})).get("recommended_container_roles", [])
        if str(item).strip()
    ]


def test_desktop_app_memory_tracks_failure_memory_and_surface_staleness(tmp_path: Path) -> None:
    memory = DesktopAppMemory(store_path=str(tmp_path / "desktop_app_memory.json"))
    entry = memory.record_survey(
        app_name="custom-tool",
        app_profile={"profile_id": "custom-tool", "name": "Custom Tool", "category": "utility"},
        snapshot={
            "surface_fingerprint": "custom-tool|main|surface",
            "target_window": {"title": "Custom Tool", "class_name": "CustomSurface", "window_signature": "custom-main"},
            "active_window": {"title": "Custom Tool"},
            "surface_summary": {"control_counts": {"button": 1}, "top_labels": [{"label": "Sync", "count": 1}]},
            "surface_intelligence": {"surface_role": "utility", "interaction_mode": "mouse_first"},
            "vision_fusion": {"model_mode": "hybrid_vision_plus_accessibility", "confidence": 0.78, "top_labels": ["Sync"], "ocr_terms": ["Sync"]},
            "native_learning_signals": {"custom_surface_suspected": True, "reparenting_risk": 0.62, "anomaly_flags": ["custom_surface", "reparenting_risk"]},
            "safe_traversal_plan": {"candidate_count": 2, "recommended_paths": ["dialog", "tab"], "recursive_depth_limit": 2},
            "elements": {"items": [{"element_id": "sync_btn", "name": "Sync", "control_type": "button"}]},
        },
        probe_report={
            "status": "partial",
            "attempted_count": 1,
            "successful_count": 0,
            "verified_count": 0,
            "uncertain_count": 1,
            "blocked_count": 0,
            "error_count": 1,
            "items": [
                {
                    "label": "Sync",
                    "control_type": "button",
                    "probe_status": "error",
                    "effect_kind": "no_observed_change",
                    "semantic_role": "action",
                    "message": "dialog reparents and control disappears",
                    "verification_confidence": 0.21,
                    "pre_surface_fingerprint": "custom-tool|main|surface",
                    "post_surface_fingerprint": "custom-tool|main|surface",
                    "source": "element",
                    "container_role": "dialog",
                }
            ],
        },
        survey_status="partial",
    )

    assert entry["vision_summary"]["model_mode"] == "hybrid_vision_plus_accessibility"
    assert entry["native_learning_signals"]["custom_surface_suspected"] is True
    assert entry["failure_memory_summary"]["entry_count"] >= 1
    assert entry["discouraged_wave_actions"]
    stale_snapshot = DesktopAppMemory._staleness_snapshot(
        updated_at="2020-01-01T00:00:00+00:00",
        version_signature=entry["version_profile"]["signature"],
    )
    assert stale_snapshot["stale"] is True


def test_desktop_app_memory_builds_surface_graph_and_command_hints(tmp_path: Path) -> None:
    memory = DesktopAppMemory(store_path=str(tmp_path / "desktop_app_memory.json"))

    entry = memory.record_survey(
        app_name="settings",
        query="bluetooth",
        app_profile={"profile_id": "settings", "name": "Settings", "category": "system"},
        snapshot={
            "surface_fingerprint": "settings|bluetooth|panel",
            "target_window": {
                "title": "Bluetooth & devices",
                "class_name": "ApplicationFrameWindow",
                "window_signature": "settings-main",
            },
            "active_window": {"title": "Bluetooth & devices"},
            "surface_summary": {
                "summary": "Settings navigation surface with search and toggle controls.",
                "control_counts": {"button": 2, "togglebutton": 1, "treeitem": 1},
                "top_labels": [{"label": "Bluetooth", "count": 1}, {"label": "Add device", "count": 1}],
                "recommended_actions": ["search", "focus_main_content"],
                "surface_flags": {
                    "search_visible": True,
                    "navigation_tree_visible": True,
                    "form_surface_visible": True,
                },
            },
            "surface_intelligence": {"surface_role": "settings", "interaction_mode": "keyboard_first"},
            "observation": {
                "targets": [
                    {"text": "Bluetooth"},
                    {"text": "Add device"},
                    {"text": "Search settings"},
                ]
            },
            "elements": {
                "items": [
                    {
                        "element_id": "search_box",
                        "name": "Search settings",
                        "control_type": "edit",
                        "automation_id": "SearchBox",
                        "accelerator_key": "Ctrl+F",
                    },
                    {
                        "element_id": "bluetooth_toggle",
                        "name": "Bluetooth",
                        "control_type": "togglebutton",
                        "automation_id": "BluetoothToggle",
                        "access_key": "B",
                    },
                    {
                        "element_id": "add_device",
                        "name": "Add device",
                        "control_type": "button",
                        "automation_id": "AddDeviceButton",
                    },
                ]
            },
            "workflow_surfaces": [{"action": "search", "title": "Search", "primary_hotkey": ["Ctrl+F"]}],
        },
        probe_report={
            "status": "success",
            "attempted_count": 1,
            "successful_count": 1,
            "items": [
                {
                    "label": "Add device",
                    "control_type": "button",
                    "probe_status": "success",
                    "effect_kind": "window_transition",
                    "semantic_role": "navigator",
                    "pre_surface_fingerprint": "settings|bluetooth|panel",
                    "post_surface_fingerprint": "settings|bluetooth|add-device-dialog",
                }
            ],
        },
    )

    assert entry["surface_nodes"]
    assert entry["surface_nodes"][0]["fingerprint"] == "settings|bluetooth|panel"
    assert entry["surface_transitions"][0]["label"] == "Add device"
    assert entry["surface_transitions"][0]["to_surface_fingerprint"] == "settings|bluetooth|add-device-dialog"
    assert any(str(item.get("label", "")) == "Search settings" for item in entry["learned_commands"])
    assert any(str(item.get("label", "")) == "search" for item in entry["learned_commands"])
    assert bool(dict(entry["capability_profile"]["features"]).get("search_surface", False)) is True
    assert bool(dict(entry["capability_profile"]["features"]).get("keyboard_shortcuts", False)) is True

    hint = memory.surface_hint(
        app_name="settings",
        profile_id="settings",
        surface_fingerprint="settings|bluetooth|panel",
    )

    assert hint["status"] == "success"
    assert hint["known"] is True
    assert dict(hint["surface_node"])["fingerprint"] == "settings|bluetooth|panel"
    assert any(str(item.get("label", "")) == "Search settings" for item in hint["learned_commands"])


def test_desktop_app_memory_populates_sqlite_knowledge_store_and_semantic_lookup(tmp_path: Path) -> None:
    memory = DesktopAppMemory(
        store_path=str(tmp_path / "desktop_app_memory.json"),
        knowledge_store_path=str(tmp_path / "desktop_app_memory.sqlite3"),
    )

    memory.record_survey(
        app_name="notepad",
        query="save file",
        app_profile={"profile_id": "notepad", "name": "Notepad", "category": "utility"},
        snapshot={
            "surface_fingerprint": "notepad|main|surface",
            "target_window": {"title": "Untitled - Notepad"},
            "active_window": {"title": "Untitled - Notepad"},
            "surface_summary": {
                "control_inventory": [
                    {"name": "Save", "control_type": "button", "automation_id": "SaveButton"},
                    {"name": "Save As", "control_type": "menuitem", "automation_id": "SaveAsMenu"},
                ],
                "recommended_actions": ["save", "export"],
            },
            "elements": {
                "items": [
                    {"name": "Save", "control_type": "button", "automation_id": "SaveButton"},
                    {"name": "Save As", "control_type": "menuitem", "automation_id": "SaveAsMenu"},
                ]
            },
            "workflow_surfaces": [
                {"action": "save", "primary_hotkey": ["ctrl+s"], "title": "Save"},
                {"action": "save_as", "primary_hotkey": ["ctrl+shift+s"], "title": "Save As"},
            ],
        },
        source="manual",
    )

    snapshot = memory.snapshot(app_name="notepad", limit=4)
    assert snapshot["knowledge_store"]["status"] == "success"
    assert int(snapshot["knowledge_store"]["entry_count"] or 0) >= 1
    assert int(snapshot["knowledge_store"]["control_count"] or 0) >= 2
    assert int(snapshot["knowledge_store"]["vector_count"] or 0) >= 2
    assert snapshot["items"]
    item_knowledge = dict(snapshot["items"][0].get("knowledge_store", {}))
    assert int(item_knowledge.get("control_count", 0) or 0) >= 2
    assert int(item_knowledge.get("command_count", 0) or 0) >= 2
    assert bool(item_knowledge.get("semantic_memory_available", False)) is True

    matches = memory.semantic_lookup(query="save file", app_name="notepad", limit=4)
    assert matches["status"] == "success"
    assert matches["count"] >= 1
    assert any("save" in str(item.get("label", "")).lower() for item in matches["items"])

    hint = memory.surface_hint(app_name="notepad", query="save")
    assert hint["knowledge_store"]["status"] == "success"
    assert any("save" in str(item.get("label", "")).lower() for item in hint["semantic_matches"])


def test_desktop_app_memory_records_adaptive_wave_strategies(tmp_path: Path) -> None:
    memory = DesktopAppMemory(store_path=str(tmp_path / "desktop_app_memory.json"))

    entry = memory.record_survey(
        app_name="vscode",
        query="search workspace",
        app_profile={"profile_id": "vscode", "name": "Visual Studio Code", "category": "editor"},
        snapshot={
            "surface_fingerprint": "vscode|main|surface",
            "surface_summary": {
                "recommended_actions": ["command"],
                "control_counts": {"pane": 1},
            },
            "surface_intelligence": {"surface_role": "editor", "interaction_mode": "keyboard_first"},
            "elements": {"items": []},
        },
        wave_report={
            "attempted_count": 3,
            "learned_surface_count": 2,
            "known_surface_count": 1,
            "stop_reason": "max_surface_waves_reached",
            "recommended_next_actions": ["quick_open", "workspace_search"],
            "items": [
                {
                    "action": "command",
                    "title": "Command Palette",
                    "hotkeys": ["ctrl+shift+p"],
                    "recommended_followups": ["quick_open"],
                    "surface_fingerprint": "vscode|command|surface",
                    "known_surface": False,
                },
                {
                    "action": "quick_open",
                    "title": "Quick Open",
                    "hotkeys": ["ctrl+p"],
                    "recommended_followups": ["workspace_search"],
                    "surface_fingerprint": "vscode|quick_open|surface",
                    "known_surface": True,
                },
            ],
            "skipped": [
                {
                    "action": "workspace_search",
                    "title": "Workspace Search",
                    "status": "skipped",
                    "message": "already known",
                }
            ],
        },
        source="manual",
    )

    assert entry["wave_summary"]["attempted_count"] == 3
    assert entry["wave_strategies"]
    assert entry["wave_strategy_summary"]["recommended_actions"][0] in {"command", "quick_open"}
    assert any(str(item.get("action", "")) == "command" for item in entry["wave_strategies"])

    hint = memory.surface_hint(
        app_name="vscode",
        profile_id="vscode",
        surface_fingerprint="vscode|main|surface",
    )

    assert hint["known"] is True
    assert hint["wave_strategies"]
    assert hint["recommended_wave_actions"]


def test_desktop_action_router_surface_snapshot_builds_api_assist_route_for_weird_app() -> None:
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 700, "title": "Custom Tool", "exe": r"C:\Apps\custom-tool.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 700, "title": "Custom Tool", "window_signature": "custom-tool-main"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "computer_observe": lambda payload: {
                "status": "success",
                "screen_hash": "weird-surface",
                "text": "Custom Tool detached floating dialog",
                "targets": (
                    [
                        {"text": "Sync", "confidence": 91.0},
                        {"text": "Advanced", "confidence": 88.0},
                        {"text": "Detached dialog", "confidence": 86.0},
                    ]
                    if bool(payload.get("include_targets", False))
                    else []
                ),
            },
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {
                        "element_id": "sync_btn",
                        "name": "Sync",
                        "control_type": "button",
                        "automation_id": "SyncButton",
                        "root_window_title": "Custom Tool",
                    }
                ],
            },
            "window_topology": lambda _payload: {
                "status": "success",
                "topology_signature": "custom-topology",
                "descendant_chain_depth": 2,
                "descendant_dialog_chain_depth": 2,
                "active_owner_chain_depth": 1,
                "same_root_owner_dialog_like_count": 2,
                "direct_child_dialog_like_count": 1,
                "owner_chain_visible": True,
                "child_dialog_like_visible": True,
            },
            "reacquire_window": lambda _payload: {
                "status": "success",
                "candidate": {"title": "Custom Tool Child", "hwnd": 701, "owner_chain_depth": 1},
                "descendant_chain_depth": 2,
                "descendant_dialog_chain_depth": 2,
                "same_root_owner_dialog_like_count": 2,
                "direct_child_dialog_like_count": 1,
                "descendant_anchor_recovery_available": True,
                "descendant_anchor_recovery_match_score": 0.82,
                "descendant_anchor_recovery_pressure": 0.77,
                "preferred_descendant": {"title": "Advanced"},
            },
        },
        workflow_memory=_isolated_workflow_memory(),
        app_memory=_isolated_app_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(
        app_name="custom-tool",
        query="diagnostics",
        include_observation=True,
        include_ocr_targets=True,
        include_elements=True,
        include_workflow_probes=False,
    )

    route = dict(payload.get("vision_learning_route", {}))
    assert payload["status"] == "success"
    assert route["api_assist_recommended"] is True
    assert route["needs_native_stabilization"] is True
    assert str(route.get("preferred_probe_mode", "")) == "api_vision_assist"
    assert str(route.get("native_recovery_mode", "")) == "focus_related_window_chain"
    assert float(route.get("weird_app_pressure", 0.0) or 0.0) >= 0.58


def test_desktop_action_router_stabilizes_custom_probe_and_persists_route_memory() -> None:
    state = {"surface": "main"}

    def _current_title() -> str:
        if state["surface"] == "unstable":
            return "Custom Tool Floating Dialog"
        if state["surface"] == "stabilized":
            return "Custom Tool Advanced"
        return "Custom Tool"

    def _computer_observe(payload: Dict[str, Any]) -> Dict[str, Any]:
        include_targets = bool(payload.get("include_targets", False))
        if state["surface"] == "unstable":
            return {
                "status": "success",
                "screen_hash": "hash-unstable",
                "text": "Detached floating sync dialog",
                "targets": (
                    [
                        {"text": "Detached", "confidence": 89.0},
                        {"text": "Confirm", "confidence": 87.0},
                        {"text": "Advanced", "confidence": 82.0},
                    ]
                    if include_targets
                    else []
                ),
            }
        if state["surface"] == "stabilized":
            return {
                "status": "success",
                "screen_hash": "hash-stable",
                "text": "Advanced sync details",
                "targets": (
                    [
                        {"text": "Advanced", "confidence": 92.0},
                        {"text": "Diagnostics", "confidence": 88.0},
                    ]
                    if include_targets
                    else []
                ),
            }
        return {
            "status": "success",
            "screen_hash": "hash-main",
            "text": "Custom Tool main surface",
            "targets": (
                [
                    {"text": "Sync", "confidence": 91.0},
                    {"text": "Open", "confidence": 84.0},
                ]
                if include_targets
                else []
            ),
        }

    def _accessibility_rows() -> list[Dict[str, Any]]:
        if state["surface"] == "unstable":
            return []
        if state["surface"] == "stabilized":
            return [
                {
                    "element_id": "advanced_btn",
                    "name": "Advanced",
                    "control_type": "button",
                    "automation_id": "AdvancedButton",
                    "root_window_title": _current_title(),
                }
            ]
        return [
            {
                "element_id": "sync_btn",
                "name": "Sync",
                "control_type": "button",
                "automation_id": "SyncButton",
                "root_window_title": _current_title(),
            }
        ]

    def _window_topology(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if state["surface"] == "unstable":
            return {
                "status": "success",
                "topology_signature": "custom-unstable",
                "descendant_chain_depth": 2,
                "descendant_dialog_chain_depth": 2,
                "active_owner_chain_depth": 1,
                "same_root_owner_dialog_like_count": 2,
                "direct_child_dialog_like_count": 1,
                "owner_chain_visible": True,
                "child_dialog_like_visible": True,
            }
        return {
            "status": "success",
            "topology_signature": f"custom-{state['surface']}",
            "descendant_chain_depth": 1 if state["surface"] == "stabilized" else 0,
            "descendant_dialog_chain_depth": 0,
            "active_owner_chain_depth": 0,
            "same_root_owner_dialog_like_count": 0,
            "direct_child_dialog_like_count": 0,
            "owner_chain_visible": False,
            "child_dialog_like_visible": False,
        }

    def _reacquire_window(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if state["surface"] == "unstable":
            return {
                "status": "success",
                "candidate": {"title": "Custom Tool Floating Dialog", "hwnd": 702, "owner_chain_depth": 1},
                "descendant_chain_depth": 2,
                "descendant_dialog_chain_depth": 2,
                "same_root_owner_dialog_like_count": 2,
                "direct_child_dialog_like_count": 1,
                "descendant_anchor_recovery_available": True,
                "descendant_anchor_recovery_match_score": 0.85,
                "descendant_anchor_recovery_pressure": 0.81,
                "preferred_descendant": {"title": "Advanced"},
            }
        return {
            "status": "success",
            "candidate": {"title": _current_title(), "hwnd": 700, "owner_chain_depth": 0},
            "descendant_chain_depth": 0,
            "descendant_dialog_chain_depth": 0,
            "same_root_owner_dialog_like_count": 0,
            "direct_child_dialog_like_count": 0,
            "descendant_anchor_recovery_available": False,
            "descendant_anchor_recovery_match_score": 0.0,
            "descendant_anchor_recovery_pressure": 0.0,
            "preferred_descendant": {"title": "Advanced" if state["surface"] == "stabilized" else ""},
        }

    def _accessibility_invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        if str(payload.get("element_id", "") or "").strip() == "sync_btn":
            state["surface"] = "unstable"
        return {"status": "success", "message": f"invoked {payload.get('element_id', payload.get('query', 'unknown'))}"}

    def _focus_related_window(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if state["surface"] == "unstable":
            state["surface"] = "stabilized"
        return {
            "status": "success",
            "focus_applied": True,
            "window": {"title": _current_title(), "hwnd": 700 if state["surface"] == "stabilized" else 702},
        }

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 700, "title": _current_title(), "exe": r"C:\Apps\custom-tool.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 700, "title": _current_title(), "window_signature": f"custom-{state['surface']}"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "open_app": lambda _payload: {"status": "success", "requested_app": "custom-tool", "launch_method": "system_path"},
            "computer_observe": _computer_observe,
            "accessibility_list_elements": lambda _payload: {"status": "success", "items": _accessibility_rows()},
            "accessibility_invoke_element": _accessibility_invoke,
            "computer_click_target": lambda payload: {"status": "success", "message": f"clicked {payload.get('query', 'unknown')}"},
            "window_topology": _window_topology,
            "reacquire_window": _reacquire_window,
            "focus_related_window": _focus_related_window,
            "focus_related_window_chain": _focus_related_window,
            "focus_window": lambda payload: {"status": "success", "window": {"title": str(payload.get("title", "") or _current_title())}},
        },
        workflow_memory=_isolated_workflow_memory(),
        app_memory=_isolated_app_memory(),
        vision_runtime_provider=lambda: {
            "status": "success",
            "runtime_status": "ready",
            "loaded_count": 2,
            "available": True,
            "profile_id": "vision-runtime-balanced",
            "template_id": "local-vision",
        },
        settle_delay_s=0.0,
    )

    payload = router.survey_app_memory(
        app_name="custom-tool",
        query="sync",
        ensure_app_launch=True,
        include_observation=True,
        include_ocr_targets=True,
        probe_controls=True,
        max_probe_controls=1,
        follow_surface_waves=False,
    )

    assert payload["status"] == "success"
    assert int(payload["probe_report"]["stabilized_count"] or 0) == 1
    assert "local vision runtime ready" in str(payload["message"]).lower()
    tested_control = payload["memory_entry"]["tested_controls"][0]
    assert bool(tested_control.get("native_stabilized", False)) is True
    assert str(dict(tested_control.get("verification_summary", {})).get("verification_mode", "")) == "native_stabilized_before_after"
    assert str(dict(tested_control.get("vision_learning_route", {})).get("native_recovery_mode", "")) == "focus_related_window_chain"
    assert payload["memory_entry"]["vision_learning_route"]["local_runtime_ready"] is True
    assert int(payload["memory_entry"]["native_stabilization_summary"]["stabilized_total"] or 0) >= 1
    assert int(payload["app_memory"]["summary"]["native_stabilization_total"] or 0) >= 1


def test_desktop_action_router_marks_setup_constrained_runtime_route() -> None:
    router = DesktopActionRouter(
        action_handlers={
            "open_app": lambda _payload: {"status": "success", "requested_app": "custom-tool", "launch_method": "system_path"},
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 901, "title": "Custom Tool", "exe": r"C:\Tools\custom-tool.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 901, "title": "Custom Tool"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {
                        "element_id": "sync_button",
                        "name": "Sync",
                        "control_type": "button",
                        "automation_id": "SyncButton",
                        "root_window_title": "Custom Tool",
                    }
                ],
            },
            "computer_observe": lambda payload: {
                "status": "success",
                "text": "Custom tool sync panel",
                "screen_hash": "hash-custom-sync",
                "targets": (
                    [{"text": "Sync", "confidence": 92.0}, {"text": "Advanced", "confidence": 84.0}]
                    if bool(payload.get("include_targets", False))
                    else []
                ),
            },
        },
        workflow_memory=_isolated_workflow_memory(),
        app_memory=_isolated_app_memory(),
        settle_delay_s=0.0,
    )

    payload = router.survey_app_memory(
        app_name="custom-tool",
        query="sync",
        ensure_app_launch=True,
        include_observation=True,
        include_ocr_targets=True,
        probe_controls=False,
        follow_surface_waves=False,
        learning_profile="hybrid_guided_explore",
        execution_mode="hybrid_ready",
        adaptive_runtime_strategy={
            "strategy_profile": "hybrid_guided_runtime",
            "runtime_band_preference": "hybrid",
            "preferred_probe_mode": "hybrid_verify",
            "preferred_wave_mode": "vision_guided_safe_traversal",
            "preferred_target_mode": "hybrid",
            "preferred_verification_mode": "multi_signal_before_after",
            "preferred_native_recovery_mode": "focus_related_window_chain",
            "allow_api_assist": True,
            "prefer_local_runtime": True,
        },
        provider_model_readiness={
            "required_tasks": ["vision"],
            "related_setup_action_codes": ["configure_huggingface_token"],
            "local_ready_tasks": ["reasoning"],
            "remote_ready_tasks": ["vision"],
            "blocker_codes": ["provider_missing_huggingface"],
            "readiness_status": "degraded",
        },
    )

    adaptive_runtime = dict(payload.get("adaptive_learning_runtime", {}))
    assert payload["status"] == "success"
    assert adaptive_runtime["selected_runtime_band"] == "accessibility"
    assert adaptive_runtime["route_profile"] == "vision_first"
    assert adaptive_runtime["model_preference"] == "ocr_only"
    assert adaptive_runtime["runtime_provider_source"] == "ocr_runtime"
    assert adaptive_runtime["route_resolution_status"] == "setup_constrained"
    assert adaptive_runtime["setup_followup_required"] is True
    assert adaptive_runtime["provider_blocked"] is True
    assert "setup constrained" in str(payload["message"]).lower()
