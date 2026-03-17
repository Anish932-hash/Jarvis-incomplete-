from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any, Dict, List

import pytest

from backend.python.core.desktop_app_profile_registry import DesktopAppProfileRegistry
from backend.python.core.desktop_action_router import DesktopActionRouter
from backend.python.core.desktop_mission_memory import DesktopMissionMemory
from backend.python.core.desktop_workflow_memory import DesktopWorkflowMemory


def _isolated_workflow_memory() -> DesktopWorkflowMemory:
    return DesktopWorkflowMemory(store_path=str(Path(tempfile.mkdtemp()) / "desktop_workflow_memory.json"))


@pytest.fixture(autouse=True)
def _reset_default_desktop_mission_memory(tmp_path: Path):
    DesktopMissionMemory._DEFAULT_INSTANCE = DesktopMissionMemory(
        store_path=str(tmp_path / "desktop_mission_memory.json")
    )
    yield
    DesktopMissionMemory._DEFAULT_INSTANCE = None


def _build_router(
    action_handlers: Dict[str, Any],
    *,
    rust_request_handler: Any | None = None,
    benchmark_guidance_provider: Any | None = None,
) -> DesktopActionRouter:
    return DesktopActionRouter(
        action_handlers=action_handlers,
        workflow_memory=_isolated_workflow_memory(),
        rust_request_handler=rust_request_handler,
        benchmark_guidance_provider=benchmark_guidance_provider,
        settle_delay_s=0.0,
    )


def _build_registry(tmp_path: Path, rows: List[str]) -> DesktopAppProfileRegistry:
    apps_file = tmp_path / "apps.txt"
    apps_file.write_text(
        "\n".join(
            [
                "Name                                      Id                           Version    Available  Source",
                "---------------------------------------------------------------------------------------------------",
                *rows,
            ]
        ),
        encoding="utf-8",
    )
    return DesktopAppProfileRegistry(source_paths=[str(apps_file)])


def test_desktop_action_router_advises_accessibility_then_ocr_click_strategy() -> None:
    router = _build_router(
        {
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [
                    {"hwnd": 101, "title": "Notepad - notes", "exe": r"C:\Windows\notepad.exe"},
                    {"hwnd": 102, "title": "Chrome - Docs", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"},
                ],
            },
            "active_window": lambda _payload: {"status": "success", "window": {"hwnd": 102, "title": "Chrome - Docs"}},
            "accessibility_status": lambda _payload: {
                "status": "success",
                "provider": "pywinauto_uia",
                "capabilities": {"invoke_element": True},
            },
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        }
    )

    payload = router.advise({"action": "click", "app_name": "notepad", "query": "Save"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "accessibility_then_ocr"
    assert payload["target_window"]["hwnd"] == 101
    assert [step["action"] for step in payload["execution_plan"]] == ["focus_window", "computer_click_target"]


def test_desktop_action_router_executes_launch_click_and_type_chain() -> None:
    calls: List[str] = []

    def _record(name: str, result: Dict[str, Any]) -> Any:
        def _inner(payload: Dict[str, Any]) -> Dict[str, Any]:
            calls.append(f"{name}:{payload}")
            return dict(result)

        return _inner

    router = _build_router(
        {
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "open_app": _record("open_app", {"status": "success", "app_name": "notepad"}),
            "focus_window": _record("focus_window", {"status": "success", "window": {"hwnd": 401, "title": "Notepad"}}),
            "computer_click_target": _record("computer_click_target", {"status": "success", "method": "accessibility"}),
            "keyboard_type": _record("keyboard_type", {"status": "success", "chars": 11}),
        }
    )

    payload = router.execute(
        {
            "action": "click_and_type",
            "app_name": "notepad",
            "query": "Document body",
            "text": "hello world",
            "ensure_app_launch": True,
        }
    )

    assert payload["status"] == "success"
    assert [row["action"] for row in payload["results"]] == ["open_app", "focus_window", "computer_click_target", "keyboard_type"]
    assert any(call.startswith("open_app:") for call in calls)
    assert any(call.startswith("keyboard_type:") for call in calls)


def test_desktop_action_router_retries_with_ocr_when_primary_attempt_cannot_be_verified() -> None:
    state: Dict[str, Any] = {"mode": "idle", "typed_text": ""}
    click_modes: List[str] = []

    def _computer_click_target(payload: Dict[str, Any]) -> Dict[str, Any]:
        target_mode = str(payload.get("target_mode", "auto") or "auto").strip().lower()
        click_modes.append(target_mode)
        state["mode"] = target_mode
        return {
            "status": "success",
            "method": "ocr_text" if target_mode == "ocr" else "accessibility",
            "screen_changed": target_mode == "ocr",
        }

    def _keyboard_type(payload: Dict[str, Any]) -> Dict[str, Any]:
        state["typed_text"] = str(payload.get("text", "") or "") if state.get("mode") == "ocr" else ""
        return {"status": "success", "chars": len(str(payload.get("text", "") or ""))}

    def _computer_observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if state.get("mode") == "ocr" and state.get("typed_text"):
            return {
                "status": "success",
                "screen_hash": "hash_after",
                "text": str(state.get("typed_text", "")),
                "screenshot_path": "E:/tmp/observe_after.png",
            }
        return {
            "status": "success",
            "screen_hash": "hash_before",
            "text": "",
            "screenshot_path": "E:/tmp/observe_before.png",
        }

    router = _build_router(
        {
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 501, "title": "Notepad - scratch", "exe": r"C:\Windows\notepad.exe"}],
            },
            "active_window": lambda _payload: {"status": "success", "window": {"hwnd": 501, "title": "Notepad - scratch"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 501), "title": "Notepad - scratch"}},
            "computer_click_target": _computer_click_target,
            "keyboard_type": _keyboard_type,
            "computer_observe": _computer_observe,
        }
    )

    payload = router.execute(
        {
            "action": "click_and_type",
            "app_name": "notepad",
            "query": "Document body",
            "text": "hello world",
            "verify_after_action": True,
            "retry_on_verification_failure": True,
            "max_strategy_attempts": 2,
        }
    )

    assert payload["status"] == "success"
    assert payload["attempt_count"] == 2
    assert payload["executed_strategy"]["strategy_id"] == "ocr_retry"
    assert click_modes == ["accessibility", "ocr"]
    assert payload["attempts"][0]["verification"]["verified"] is False
    assert payload["attempts"][1]["verification"]["verified"] is True


def test_desktop_action_router_verifies_launch_by_window_presence() -> None:
    state: Dict[str, Any] = {"launched": False}

    def _list_windows(_payload: Dict[str, Any]) -> Dict[str, Any]:
        windows = (
            [{"hwnd": 902, "title": "Calculator", "exe": r"C:\Windows\System32\calc.exe"}]
            if state["launched"]
            else []
        )
        return {"status": "success", "windows": windows}

    def _open_app(_payload: Dict[str, Any]) -> Dict[str, Any]:
        state["launched"] = True
        return {"status": "success", "app_name": "calculator"}

    router = _build_router(
        {
            "list_windows": _list_windows,
            "active_window": lambda _payload: {"status": "success", "window": {"hwnd": 777, "title": "Terminal"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "open_app": _open_app,
        }
    )

    payload = router.execute(
        {
            "action": "launch",
            "app_name": "calculator",
            "ensure_app_launch": True,
        }
    )

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert "launch verified" in str(payload["message"]).lower()


def test_desktop_action_router_blocks_click_when_no_targeting_capability_exists() -> None:
    router = _build_router(
        {
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "degraded", "capabilities": {"invoke_element": False}},
            "vision_status": lambda _payload: {"status": "degraded", "capabilities": {"ocr_targets": False}},
        }
    )

    payload = router.advise({"action": "click", "query": "Save"})

    assert payload["status"] == "blocked"
    assert any("Neither accessibility automation nor OCR vision targeting is available." in blocker for blocker in payload["blockers"])


def test_desktop_action_router_applies_app_profile_defaults(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "degraded", "capabilities": {"ocr_targets": False}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "click", "app_name": "chrome", "query": "Settings"})

    assert payload["status"] == "success"


def test_desktop_action_router_surface_exploration_ranks_list_target() -> None:
    router = _build_router({})
    snapshot = {
        "status": "success",
        "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
        "target_window": {"title": "Settings"},
        "query_targets": [
            {
                "element_id": "list_bluetooth",
                "name": "Bluetooth",
                "control_type": "ListItem",
                "enabled": True,
                "visible": True,
                "match_score": 1.0,
            }
        ],
        "query_related_candidates": [],
        "selection_candidates": [
            {
                "element_id": "list_bluetooth",
                "name": "Bluetooth",
                "control_type": "ListItem",
                "enabled": True,
                "visible": True,
            }
        ],
        "workflow_surfaces": [],
        "surface_flags": {"list_visible": True, "window_targeted": True},
        "safety_signals": {},
        "recommended_actions": ["focus_list_surface"],
        "filters": {"app_name": "settings", "query": "Bluetooth"},
    }
    router.surface_snapshot = lambda **_kwargs: snapshot  # type: ignore[method-assign]

    payload = router.surface_exploration_plan(
        app_name="settings",
        query="Bluetooth",
        include_workflow_probes=False,
    )

    assert payload["status"] == "success"
    assert payload["surface_mode"] == "list_navigation"
    assert payload["automation_ready"] is True
    assert payload["top_hypotheses"][0]["label"] == "Bluetooth"
    assert payload["top_hypotheses"][0]["suggested_action"] == "select_list_item"
    assert payload["top_path"][-1]["action"] == "select_list_item"


def test_desktop_action_router_surface_exploration_ranks_dialog_buttons() -> None:
    router = _build_router({})
    snapshot = {
        "status": "success",
        "app_profile": {"status": "success", "category": "utility", "name": "Installer"},
        "target_window": {"title": "Confirm Setup"},
        "query_targets": [],
        "query_related_candidates": [],
        "selection_candidates": [],
        "workflow_surfaces": [
            {
                "action": "confirm_dialog",
                "title": "Confirm Dialog",
                "supported": True,
                "matched": True,
                "recommended_followups": [],
            },
            {
                "action": "dismiss_dialog",
                "title": "Dismiss Dialog",
                "supported": True,
                "matched": False,
                "recommended_followups": [],
            },
        ],
        "surface_flags": {"dialog_visible": True, "window_targeted": True},
        "safety_signals": {
            "dialog_visible": True,
            "dialog_button_targets": [
                {
                    "element_id": "btn_continue",
                    "name": "Continue",
                    "control_type": "Button",
                    "enabled": True,
                    "visible": True,
                },
                {
                    "element_id": "btn_cancel",
                    "name": "Cancel",
                    "control_type": "Button",
                    "enabled": True,
                    "visible": True,
                },
            ],
            "preferred_confirmation_button": "Continue",
        },
        "recommended_actions": ["confirm_dialog", "dismiss_dialog"],
        "filters": {"app_name": "installer", "query": "Continue"},
    }
    router.surface_snapshot = lambda **_kwargs: snapshot  # type: ignore[method-assign]

    payload = router.surface_exploration_plan(
        app_name="installer",
        query="Continue",
        include_workflow_probes=True,
    )

    assert payload["status"] == "success"
    assert payload["surface_mode"] == "dialog"
    assert payload["top_hypotheses"][0]["label"] == "Continue"
    assert payload["top_hypotheses"][0]["suggested_action"] == "press_dialog_button"
    assert payload["branch_actions"][0]["action"] == "confirm_dialog"


def test_desktop_action_router_surface_exploration_adds_preferred_descendant_adoption_branch() -> None:
    router = _build_router({})
    snapshot = {
        "status": "success",
        "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
        "target_window": {"hwnd": 5001, "title": "Bluetooth & devices"},
        "active_window": {"hwnd": 5001, "title": "Bluetooth & devices"},
        "query_targets": [],
        "query_related_candidates": [],
        "selection_candidates": [],
        "workflow_surfaces": [],
        "surface_flags": {"window_targeted": True},
        "safety_signals": {},
        "recommended_actions": [],
        "native_window_topology": {
            "same_process_window_count": 3,
            "related_window_count": 2,
            "owner_link_count": 2,
            "owner_chain_visible": True,
            "same_root_owner_window_count": 3,
            "same_root_owner_dialog_like_count": 2,
            "direct_child_window_count": 1,
            "direct_child_dialog_like_count": 1,
            "descendant_chain_depth": 2,
            "descendant_dialog_chain_depth": 1,
            "descendant_query_match_count": 1,
            "descendant_chain_titles": ["Pair device", "Confirm pairing"],
            "child_chain_signature": "5001|1|2|Pair device|Confirm pairing",
            "preferred_descendant": {"hwnd": 5002, "title": "Pair device"},
        },
        "window_reacquisition": {
            "candidate": {
                "hwnd": 5001,
                "title": "Bluetooth & devices",
                "root_owner_hwnd": 5000,
                "owner_chain_depth": 1,
            },
            "same_process_window_count": 3,
            "related_window_count": 2,
            "owner_link_count": 2,
            "owner_chain_visible": True,
            "same_root_owner_window_count": 3,
            "same_root_owner_dialog_like_count": 2,
            "descendant_chain_depth": 2,
            "descendant_dialog_chain_depth": 1,
            "descendant_query_match_count": 1,
            "descendant_chain_titles": ["Pair device", "Confirm pairing"],
            "child_chain_signature": "5001|1|2|Pair device|Confirm pairing",
            "preferred_descendant": {"hwnd": 5002, "title": "Pair device"},
        },
        "filters": {"app_name": "settings", "query": "Pair device"},
    }
    router.surface_snapshot = lambda **_kwargs: snapshot  # type: ignore[method-assign]

    payload = router.surface_exploration_plan(
        app_name="settings",
        query="Pair device",
        include_workflow_probes=False,
    )

    assert payload["status"] == "success"
    assert payload["branch_actions"]
    assert payload["branch_actions"][0]["action"] == "focus"
    assert payload["branch_actions"][0]["candidate_id"] == "5002"
    assert payload["branch_actions"][0]["action_payload"]["window_title"] == "Pair device"
    assert payload["branch_actions"][0]["action_payload"]["hwnd"] == 5002
    assert "deeper child surface" in payload["branch_actions"][0]["reason"].lower()


def test_desktop_action_router_branch_scoring_uses_benchmark_dialog_pressure() -> None:
    router = _build_router(
        {},
        benchmark_guidance_provider=lambda: {
            "status": "success",
            "benchmark_ready": True,
            "weakest_pack": "unsupported_and_recovery",
            "weakest_capability": "surface_exploration",
            "focus_summary": ["unsupported_and_recovery", "surface_exploration"],
            "control_biases": {
                "dialog_resolution": 0.95,
                "descendant_focus": 0.25,
                "navigation_branch": 0.1,
                "recovery_reacquire": 0.35,
                "loop_guard": 0.1,
                "native_focus": 0.2,
            },
        },
    )
    branch_context = {
        "active": True,
        "current_window_title": "Confirm setup",
        "current_surface_path": [],
        "current_reacquired_title": "",
        "current_reacquired_hwnd": 0,
        "native_same_process_window_count": 0,
        "native_related_window_count": 0,
        "native_owner_link_count": 0,
        "native_owner_chain_visible": False,
        "native_same_root_owner_window_count": 0,
        "native_same_root_owner_dialog_like_count": 0,
        "native_direct_child_window_count": 0,
        "native_direct_child_dialog_like_count": 0,
        "native_active_owner_chain_depth": 0,
        "native_max_owner_chain_depth": 0,
        "native_descendant_chain_depth": 0,
        "native_descendant_dialog_chain_depth": 0,
        "native_descendant_query_match_count": 0,
        "native_descendant_chain_titles": [],
        "preferred_descendant_title": "",
        "preferred_descendant_hwnd": 0,
        "native_child_dialog_like_visible": False,
        "native_modal_chain_signature": "",
        "native_child_chain_signature": "",
        "native_branch_family_signature": "",
        "latest_branch_occurrences": 0,
        "latest_branch_family_signature": "",
        "branch_family_repeat_count": 0,
        "branch_family_switch_count": 0,
        "branch_family_continuity": False,
        "branch_cascade_count": 0,
        "branch_cascade_kind_count": 0,
        "branch_cascade_signature": "",
        "benchmark_dialog_pressure": 0.95,
        "benchmark_descendant_focus_pressure": 0.25,
        "benchmark_navigation_pressure": 0.1,
        "benchmark_reacquire_pressure": 0.35,
        "benchmark_loop_guard_pressure": 0.1,
        "benchmark_native_focus_pressure": 0.2,
        "recent_selection_keys": set(),
    }
    dialog_row = {
        "kind": "branch_action",
        "selected_action": "press_dialog_button",
        "candidate_id": "",
        "label": "Continue",
        "action_payload": {},
    }
    navigation_row = {
        "kind": "hypothesis",
        "selected_action": "select_sidebar_item",
        "candidate_id": "settings_sidebar",
        "label": "Settings",
        "action_payload": {},
    }

    dialog_score = router._surface_exploration_branch_selection_score(  # noqa: SLF001
        row=dialog_row,
        branch_context=branch_context,
    )
    navigation_score = router._surface_exploration_branch_selection_score(  # noqa: SLF001
        row=navigation_row,
        branch_context=branch_context,
    )

    assert dialog_score > navigation_score


def test_desktop_action_router_branch_scoring_uses_benchmark_target_app_context() -> None:
    router = _build_router({})
    branch_context = {
        "active": True,
        "current_window_title": "Bluetooth & devices",
        "current_window_app_name": "settings",
        "current_surface_path": ["Devices", "Bluetooth"],
        "current_reacquired_title": "Pair device",
        "current_reacquired_app_name": "settings",
        "current_reacquired_hwnd": 5002,
        "native_same_process_window_count": 2,
        "native_related_window_count": 2,
        "native_owner_link_count": 1,
        "native_owner_chain_visible": True,
        "native_same_root_owner_window_count": 2,
        "native_same_root_owner_dialog_like_count": 1,
        "native_direct_child_window_count": 1,
        "native_direct_child_dialog_like_count": 1,
        "native_active_owner_chain_depth": 1,
        "native_max_owner_chain_depth": 2,
        "native_descendant_chain_depth": 1,
        "native_descendant_dialog_chain_depth": 1,
        "native_descendant_query_match_count": 1,
        "native_descendant_chain_titles": ["Pair device", "Confirm pairing"],
        "preferred_descendant_title": "Pair device",
        "preferred_descendant_hwnd": 5002,
        "native_child_dialog_like_visible": True,
        "native_modal_chain_signature": "5000|2|1|2",
        "native_child_chain_signature": "5001|1|1|Pair device",
        "native_branch_family_signature": "5000|2|Pair device",
        "latest_branch_occurrences": 1,
        "latest_branch_family_signature": "5000|2|Pair device",
        "branch_family_repeat_count": 1,
        "branch_family_switch_count": 0,
        "branch_family_continuity": True,
        "branch_cascade_count": 1,
        "branch_cascade_kind_count": 1,
        "branch_cascade_signature": "child_window_chain",
        "benchmark_dialog_pressure": 0.3,
        "benchmark_descendant_focus_pressure": 0.3,
        "benchmark_navigation_pressure": 0.1,
        "benchmark_reacquire_pressure": 0.2,
        "benchmark_loop_guard_pressure": 0.1,
        "benchmark_native_focus_pressure": 0.2,
        "benchmark_target_app_name": "settings",
        "benchmark_target_app_matched": True,
        "benchmark_target_app_match_score": 1.0,
        "benchmark_target_query_hints": ["pair device", "confirm pairing"],
        "benchmark_target_priority": 2.6,
        "benchmark_target_max_horizon_steps": 5,
        "benchmark_target_dialog_pressure": 0.85,
        "benchmark_target_descendant_focus_pressure": 0.92,
        "benchmark_target_navigation_pressure": 0.25,
        "benchmark_target_reacquire_pressure": 0.86,
        "benchmark_target_loop_guard_pressure": 0.35,
        "benchmark_target_native_focus_pressure": 0.94,
        "recent_selection_keys": set(),
        "last_branch_kind": "child_window_chain",
    }
    focus_row = {
        "kind": "branch_action",
        "selected_action": "focus",
        "candidate_id": "5002",
        "label": "Adopt child surface: Pair device",
        "action_payload": {"hwnd": 5002, "window_title": "Pair device"},
    }
    click_row = {
        "kind": "branch_action",
        "selected_action": "click",
        "candidate_id": "",
        "label": "Open Bluetooth settings",
        "action_payload": {"window_title": "Bluetooth & devices"},
    }

    focus_score = router._surface_exploration_branch_selection_score(  # noqa: SLF001
        row=focus_row,
        branch_context=branch_context,
    )
    click_score = router._surface_exploration_branch_selection_score(  # noqa: SLF001
        row=click_row,
        branch_context=branch_context,
    )

    assert focus_score > click_score


def test_desktop_action_router_select_surface_exploration_target_prefers_preferred_descendant_adoption() -> None:
    router = _build_router({})
    snapshot = {
        "status": "success",
        "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
        "target_window": {"hwnd": 5001, "title": "Bluetooth & devices"},
        "active_window": {"hwnd": 5001, "title": "Bluetooth & devices"},
        "query_targets": [
            {
                "element_id": "list_devices",
                "name": "Devices",
                "control_type": "ListItem",
                "enabled": True,
                "visible": True,
                "match_score": 0.32,
            }
        ],
        "query_related_candidates": [],
        "selection_candidates": [
            {
                "element_id": "list_devices",
                "name": "Devices",
                "control_type": "ListItem",
                "enabled": True,
                "visible": True,
            }
        ],
        "workflow_surfaces": [],
        "surface_flags": {"window_targeted": True, "list_visible": True},
        "safety_signals": {},
        "recommended_actions": [],
        "native_window_topology": {
            "same_process_window_count": 3,
            "related_window_count": 2,
            "owner_link_count": 2,
            "owner_chain_visible": True,
            "same_root_owner_window_count": 3,
            "same_root_owner_dialog_like_count": 2,
            "direct_child_window_count": 1,
            "direct_child_dialog_like_count": 1,
            "active_owner_chain_depth": 1,
            "max_owner_chain_depth": 2,
            "descendant_chain_depth": 2,
            "descendant_dialog_chain_depth": 1,
            "descendant_query_match_count": 1,
            "descendant_chain_titles": ["Pair device", "Confirm pairing"],
            "child_dialog_like_visible": True,
            "child_chain_signature": "5001|1|2|Pair device|Confirm pairing",
            "modal_chain_signature": "5000|2|1|Pair device",
            "branch_family_signature": "5000|2|Bluetooth & devices|Pair device",
            "preferred_descendant": {"hwnd": 5002, "title": "Pair device"},
        },
        "window_reacquisition": {
            "candidate": {
                "hwnd": 5001,
                "title": "Bluetooth & devices",
                "root_owner_hwnd": 5000,
                "owner_chain_depth": 1,
                "match_score": 0.86,
            },
            "same_process_window_count": 3,
            "related_window_count": 2,
            "owner_link_count": 2,
            "owner_chain_visible": True,
            "same_root_owner_window_count": 3,
            "same_root_owner_dialog_like_count": 2,
            "candidate_root_owner_hwnd": 5000,
            "candidate_owner_chain_depth": 1,
            "descendant_chain_depth": 2,
            "descendant_dialog_chain_depth": 1,
            "descendant_query_match_count": 1,
            "descendant_chain_titles": ["Pair device", "Confirm pairing"],
            "child_chain_signature": "5001|1|2|Pair device|Confirm pairing",
            "branch_family_signature": "5000|2|Bluetooth & devices|Pair device",
            "preferred_descendant": {"hwnd": 5002, "title": "Pair device"},
        },
        "filters": {"app_name": "settings", "query": "Pair device"},
    }
    router.surface_snapshot = lambda **_kwargs: snapshot  # type: ignore[method-assign]

    plan = router.surface_exploration_plan(
        app_name="settings",
        query="Pair device",
        include_workflow_probes=False,
    )
    selected = router._select_surface_exploration_target(  # noqa: SLF001
        exploration_plan=plan,
        args={
            "app_name": "settings",
            "query": "Pair device",
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "select_list_item",
                    "selected_candidate_id": "list_bluetooth",
                    "selected_candidate_label": "Bluetooth",
                    "window_title": "Bluetooth & devices",
                    "surface_path_tail": ["Devices", "Bluetooth"],
                    "topology_branch_family_signature": "5000|2|Bluetooth & devices|Pair device",
                    "occurrences": 1,
                }
            ],
        },
    )

    assert selected["status"] == "success"
    assert selected["selected_action"] == "focus"
    assert selected["candidate_id"] == "5002"
    assert selected["action_payload"]["window_title"] == "Pair device"
    assert selected["action_payload"]["hwnd"] == 5002
    assert selected["confidence"] > 0.8


def test_desktop_action_router_surface_exploration_uses_surface_intelligence_for_mode_and_message() -> None:
    router = _build_router({})
    snapshot = {
        "status": "success",
        "app_profile": {"status": "success", "category": "utility", "name": "Ops Console"},
        "target_window": {"title": "Ops Console"},
        "query_targets": [
            {
                "element_id": "row_chrome",
                "name": "chrome.exe",
                "control_type": "Row",
                "match_score": 0.84,
                "visible": True,
                "enabled": True,
            }
        ],
        "query_related_candidates": [],
        "selection_candidates": [],
        "surface_flags": {"window_targeted": True},
        "safety_signals": {},
        "recommended_actions": [],
        "surface_intelligence": {
            "surface_role": "content",
            "interaction_mode": "table_navigation",
            "grounding_confidence": 0.77,
            "affordances": ["query_target_available", "selection_targeting"],
            "recovery_hints": ["stay on the visible data table and prefer exact row selection"],
            "risk_flags": [],
            "query_resolution": {
                "query": "chrome",
                "candidate_count": 1,
                "best_candidate_name": "chrome.exe",
                "best_candidate_type": "Row",
                "best_candidate_id": "row_chrome",
                "confidence": 0.84,
            },
        },
        "filters": {"app_name": "ops console", "query": "chrome"},
    }
    router.surface_snapshot = lambda **_kwargs: snapshot  # type: ignore[method-assign]

    payload = router.surface_exploration_plan(
        app_name="ops console",
        query="chrome",
        include_workflow_probes=False,
    )

    assert payload["status"] == "success"
    assert payload["surface_mode"] == "table_navigation"
    assert payload["surface_intelligence"]["interaction_mode"] == "table_navigation"
    assert payload["top_hypotheses"][0]["suggested_action"] == "select_table_row"
    assert "Grounded as content with table_navigation" in payload["message"]


def test_desktop_action_router_surface_snapshot_includes_rust_topology() -> None:
    rust_calls: List[str] = []

    def _rust_request(event: str, payload: Dict[str, Any], timeout_s: float) -> Dict[str, Any]:
        rust_calls.append(event)
        assert timeout_s > 0.0
        if event == "window_topology_snapshot":
            assert str(payload.get("query", "") or "").strip() == "bluetooth"
            return {
                "status": "success",
                "data": {
                    "topology_signature": "settings|2|1",
                    "visible_window_count": 2,
                    "dialog_like_count": 1,
                    "same_process_window_count": 2,
                    "window_title_tail": ["Settings", "Bluetooth & devices"],
                },
            }
        return {"status": "error", "message": "unexpected rust event"}

    router = _build_router(
        {
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "snapshot_hash",
                "text": "Settings bluetooth devices",
            },
        },
        rust_request_handler=_rust_request,
    )

    payload = router.surface_snapshot(app_name="settings", query="bluetooth")

    assert payload["status"] == "success"
    assert payload["surface_topology"]["topology_signature"] == "settings|2|1"
    assert payload["surface_topology"]["visible_window_count"] == 2
    assert "window_topology_snapshot" in rust_calls


def test_desktop_action_router_surface_snapshot_promotes_native_reacquired_candidate() -> None:
    router = _build_router(
        {
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "window_topology": lambda _payload: {
                "status": "success",
                "topology_signature": "settings|3|2",
                "same_process_window_count": 3,
                "related_window_count": 2,
                "owner_link_count": 2,
                "owner_chain_visible": True,
                "same_root_owner_window_count": 3,
                "same_root_owner_dialog_like_count": 2,
                "active_root_owner_hwnd": 5000,
                "active_owner_chain_depth": 1,
                "max_owner_chain_depth": 2,
                "direct_child_window_count": 1,
                "direct_child_dialog_like_count": 1,
                "direct_child_titles": ["Pair device"],
                "descendant_chain_depth": 1,
                "descendant_dialog_chain_depth": 1,
                "descendant_query_match_count": 1,
                "descendant_chain_titles": ["Pair device"],
                "child_chain_signature": "4401|1|1|Pair device",
                "modal_chain_signature": "4400|2|2|1",
                "child_dialog_like_visible": True,
            },
            "reacquire_window": lambda _payload: {
                "status": "success",
                "same_process_window_count": 3,
                "related_window_count": 2,
                "owner_link_count": 2,
                "owner_chain_visible": True,
                "same_root_owner_window_count": 3,
                "same_root_owner_dialog_like_count": 2,
                "candidate_root_owner_hwnd": 4400,
                "candidate_owner_chain_depth": 2,
                "max_owner_chain_depth": 2,
                "direct_child_window_count": 0,
                "direct_child_dialog_like_count": 0,
                "descendant_chain_depth": 0,
                "descendant_dialog_chain_depth": 0,
                "descendant_query_match_count": 0,
                "descendant_chain_titles": [],
                "child_chain_signature": "4410|0|0",
                "modal_chain_signature": "4400|2|2|2",
                "child_dialog_like_visible": True,
                "candidate": {
                    "hwnd": 4410,
                    "owner_hwnd": 4401,
                    "root_owner_hwnd": 4400,
                    "owner_chain_depth": 2,
                    "title": "Pair device",
                    "app_name": "settings",
                    "process_name": "SystemSettings.exe",
                    "window_signature": "settings|dialog|1280x720|pair_device",
                    "match_score": 0.81,
                },
            },
        }
    )

    payload = router.surface_snapshot(app_name="settings", query="bluetooth")

    assert payload["status"] == "success"
    assert payload["target_window"]["hwnd"] == 4410
    assert payload["window_reacquisition"]["candidate"]["title"] == "Pair device"
    assert payload["window_reacquisition"]["candidate"]["owner_hwnd"] == 4401
    assert payload["window_reacquisition"]["candidate"]["root_owner_hwnd"] == 4400
    assert payload["window_reacquisition"]["candidate"]["owner_chain_depth"] == 2
    assert payload["window_reacquisition"]["owner_chain_visible"] is True
    assert payload["window_reacquisition"]["owner_link_count"] == 2
    assert payload["window_reacquisition"]["same_root_owner_window_count"] == 3
    assert payload["window_reacquisition"]["same_root_owner_dialog_like_count"] == 2
    assert payload["window_reacquisition"]["child_chain_signature"] == "4410|0|0"
    assert payload["window_reacquisition"]["modal_chain_signature"] == "4400|2|2|2"
    assert payload["native_window_topology"]["same_process_window_count"] == 3
    assert payload["native_window_topology"]["owner_chain_visible"] is True
    assert payload["native_window_topology"]["owner_link_count"] == 2
    assert payload["native_window_topology"]["same_root_owner_window_count"] == 3
    assert payload["native_window_topology"]["same_root_owner_dialog_like_count"] == 2
    assert payload["native_window_topology"]["direct_child_window_count"] == 1
    assert payload["native_window_topology"]["descendant_chain_depth"] == 1
    assert payload["native_window_topology"]["child_chain_signature"] == "4401|1|1|Pair device"
    assert payload["native_window_topology"]["active_owner_chain_depth"] == 1
    assert payload["native_window_topology"]["max_owner_chain_depth"] == 2
    assert payload["native_window_topology"]["modal_chain_signature"] == "4400|2|2|1"
    assert payload["native_window_topology"]["child_dialog_like_visible"] is True


def test_desktop_action_router_transition_summary_detects_descendant_title_adoption() -> None:
    router = _build_router({})

    before_plan = {
        "surface_mode": "dialog_resolution",
        "surface_snapshot": {
            "target_window": {"hwnd": 5001, "title": "Bluetooth & devices"},
            "active_window": {"hwnd": 5001, "title": "Bluetooth & devices"},
            "observation": {"screen_hash": "before_hash"},
            "native_window_topology": {
                "same_process_window_count": 3,
                "related_window_count": 2,
                "owner_link_count": 2,
                "owner_chain_visible": True,
                "same_root_owner_window_count": 3,
                "same_root_owner_dialog_like_count": 2,
                "active_root_owner_hwnd": 5000,
                "active_owner_chain_depth": 1,
                "max_owner_chain_depth": 2,
                "descendant_chain_depth": 1,
                "descendant_dialog_chain_depth": 1,
                "descendant_chain_titles": ["Pair device"],
                "child_chain_signature": "5001|1|1|Pair device",
                "modal_chain_signature": "5000|2|1|Pair device",
                "branch_family_signature": "5000|2|Bluetooth & devices|Pair device",
            },
        },
    }
    after_plan = {
        "surface_mode": "dialog_resolution",
        "surface_snapshot": {
            "target_window": {"hwnd": 5002, "title": "Pair device"},
            "active_window": {"hwnd": 5002, "title": "Pair device"},
            "observation": {"screen_hash": "after_hash"},
            "native_window_topology": {
                "same_process_window_count": 3,
                "related_window_count": 2,
                "owner_link_count": 2,
                "owner_chain_visible": True,
                "same_root_owner_window_count": 3,
                "same_root_owner_dialog_like_count": 2,
                "active_root_owner_hwnd": 5000,
                "active_owner_chain_depth": 2,
                "max_owner_chain_depth": 2,
                "descendant_chain_depth": 0,
                "descendant_dialog_chain_depth": 0,
                "descendant_chain_titles": [],
                "child_chain_signature": "5002|0|0",
                "modal_chain_signature": "5000|2|2|Pair device",
                "branch_family_signature": "5000|2|Bluetooth & devices|Pair device",
            },
            "window_reacquisition": {
                "candidate": {
                    "hwnd": 5002,
                    "owner_hwnd": 5001,
                    "root_owner_hwnd": 5000,
                    "owner_chain_depth": 2,
                    "title": "Pair device",
                },
                "child_chain_signature": "5002|0|0",
            },
        },
    }

    payload = router._surface_exploration_transition_summary(  # noqa: SLF001
        before_plan=before_plan,
        after_plan=after_plan,
    )

    assert payload["transition_kind"] == "child_window_chain"
    assert payload["child_window_chain_progressed"] is True
    assert payload["descendant_title_adoption"] is True
    assert payload["child_chain_signature_changed"] is True


def test_desktop_action_router_advise_surface_exploration_advance_prefers_rust_router_ranked_candidate(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    rust_calls: List[str] = []
    router = DesktopActionRouter(
        action_handlers={},
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        rust_request_handler=lambda event, payload, _timeout_s: (
            rust_calls.append(event)
            or {
                "status": "success",
                "data": {
                    "router_hint": "prefer_query_match",
                    "prefer_nested_branch": True,
                    "loop_risk": False,
                    "topology": {
                        "topology_signature": "settings|2|1",
                        "visible_window_count": 2,
                        "dialog_like_count": 1,
                        "same_process_window_count": 2,
                    },
                    "ranked_candidates": [
                        {
                            "selection_key": router._surface_exploration_selection_key(
                                kind="hypothesis",
                                candidate_id="list_bluetooth",
                                selected_action="select_list_item",
                                label="Bluetooth",
                            ),
                            "rank": 1,
                            "rust_score": 0.18,
                            "router_hint": "prefer_query_match",
                            "reasons": ["query label overlap", "dialog visible"],
                        },
                        {
                            "selection_key": router._surface_exploration_selection_key(
                                kind="hypothesis",
                                candidate_id="list_devices",
                                selected_action="select_list_item",
                                label="Devices",
                            ),
                            "rank": 2,
                            "rust_score": 0.0,
                            "router_hint": "fallback_rank",
                            "reasons": ["fallback"],
                        },
                    ],
                },
            }
            if event == "surface_exploration_router"
            else {"status": "error", "message": "unexpected rust event"}
        ),
        settle_delay_s=0.0,
    )
    router.surface_exploration_plan = lambda **_kwargs: {  # type: ignore[method-assign]
        "status": "success",
        "surface_mode": "list_navigation",
        "automation_ready": True,
        "manual_attention_required": False,
        "attention_signals": [],
        "hypothesis_count": 2,
        "branch_action_count": 1,
        "top_hypotheses": [
            {
                "candidate_id": "list_devices",
                "label": "Devices",
                "suggested_action": "select_list_item",
                "confidence": 0.65,
                "reason": "Devices list is visible.",
                "action_payload": {"action": "select_list_item", "app_name": "settings", "query": "Devices"},
            },
            {
                "candidate_id": "list_bluetooth",
                "label": "Bluetooth",
                "suggested_action": "select_list_item",
                "confidence": 0.60,
                "reason": "Bluetooth row is visible.",
                "action_payload": {"action": "select_list_item", "app_name": "settings", "query": "Bluetooth"},
            },
        ],
        "branch_actions": [
            {
                "action": "focus_list_surface",
                "title": "Focus List Surface",
                "supported": True,
                "matched": True,
                "confidence": 0.15,
                "reason": "Keep focus on the current list.",
                "action_payload": {"action": "focus_list_surface", "app_name": "settings"},
            }
        ],
        "surface_snapshot": {"target_window": {"title": "Settings"}},
        "surface_topology": {
            "topology_signature": "settings|2|1",
            "visible_window_count": 2,
            "dialog_like_count": 1,
            "same_process_window_count": 2,
        },
        "message": "Surface recon found safe list targets.",
    }

    payload = router.advise({"action": "advance_surface_exploration", "app_name": "settings", "query": "Bluetooth"})

    assert payload["status"] == "success"
    assert payload["exploration_selection"]["candidate_id"] == "list_bluetooth"
    assert payload["exploration_selection"]["rust_router_hint"] == "prefer_query_match"
    assert payload["exploration_selection"]["rust_score"] == pytest.approx(0.18)
    assert payload["exploration_selection"]["surface_topology"]["topology_signature"] == "settings|2|1"
    assert "surface_exploration_router" in rust_calls


def test_desktop_action_router_advise_surface_exploration_advance_selects_top_target(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2201, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2201, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_bluetooth",
                "text": "Settings bluetooth devices sidebar content",
                "screenshot_path": "E:/tmp/settings_bluetooth.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    router.surface_exploration_plan = lambda **_kwargs: {  # type: ignore[method-assign]
        "status": "success",
        "surface_mode": "list_navigation",
        "automation_ready": True,
        "manual_attention_required": False,
        "hypothesis_count": 1,
        "branch_action_count": 1,
        "top_hypotheses": [
            {
                "candidate_id": "list_bluetooth",
                "label": "Bluetooth",
                "suggested_action": "select_list_item",
                "confidence": 0.93,
                "reason": "The Bluetooth list item is the strongest visible recon target.",
                "action_payload": {
                    "action": "select_list_item",
                    "app_name": "settings",
                    "window_title": "Settings",
                    "query": "Bluetooth",
                    "control_type": "ListItem",
                    "element_id": "list_bluetooth",
                },
            }
        ],
        "branch_actions": [
            {
                "action": "focus_list_surface",
                "title": "Focus List",
                "matched": False,
                "supported": True,
                "confidence": 0.7,
                "reason": "List focus is available as a fallback.",
                "action_payload": {"action": "focus_list_surface", "app_name": "settings", "window_title": "Settings"},
            }
        ],
        "surface_snapshot": {
            "status": "success",
            "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
            "target_window": {"hwnd": 2201, "title": "Settings"},
            "active_window": {"hwnd": 2201, "title": "Settings"},
            "candidate_windows": [{"hwnd": 2201, "title": "Settings"}],
            "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
            "safety_signals": {},
            "surface_flags": {"list_visible": True, "window_targeted": True},
            "observation": {"screen_hash": "settings_bluetooth"},
        },
        "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
        "message": "Top target: Bluetooth via select_list_item.",
    }

    payload = router.advise({"action": "advance_surface_exploration", "app_name": "settings", "query": "Bluetooth"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "surface_exploration_advance"
    assert payload["exploration_selection"]["selected_action"] == "select_list_item"
    assert payload["exploration_selection"]["candidate_id"] == "list_bluetooth"
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert payload["execution_plan"][-1]["args"]["query"] == "Bluetooth"
    assert payload["execution_plan"][-1]["args"]["control_type"] == "ListItem"


def test_desktop_action_router_advise_surface_exploration_advance_skips_attempted_target(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2205, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2205, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_bluetooth_skip",
                "text": "Settings bluetooth devices sidebar content",
                "screenshot_path": "E:/tmp/settings_bluetooth_skip.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    router.surface_exploration_plan = lambda **_kwargs: {  # type: ignore[method-assign]
        "status": "success",
        "surface_mode": "list_navigation",
        "automation_ready": True,
        "manual_attention_required": False,
        "hypothesis_count": 2,
        "branch_action_count": 0,
        "top_hypotheses": [
            {
                "candidate_id": "list_bluetooth",
                "label": "Bluetooth",
                "suggested_action": "select_list_item",
                "confidence": 0.93,
                "reason": "Bluetooth is still visible.",
                "action_payload": {
                    "action": "select_list_item",
                    "app_name": "settings",
                    "window_title": "Settings",
                    "query": "Bluetooth",
                    "control_type": "ListItem",
                    "element_id": "list_bluetooth",
                },
            },
            {
                "candidate_id": "list_devices",
                "label": "Devices",
                "suggested_action": "select_list_item",
                "confidence": 0.88,
                "reason": "Devices is the strongest untried follow-up target.",
                "action_payload": {
                    "action": "select_list_item",
                    "app_name": "settings",
                    "window_title": "Settings",
                    "query": "Devices",
                    "control_type": "ListItem",
                    "element_id": "list_devices",
                },
            },
        ],
        "branch_actions": [],
        "surface_snapshot": {
            "status": "success",
            "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
            "target_window": {"hwnd": 2205, "title": "Settings"},
            "active_window": {"hwnd": 2205, "title": "Settings"},
            "candidate_windows": [{"hwnd": 2205, "title": "Settings"}],
            "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
            "safety_signals": {},
            "surface_flags": {"list_visible": True, "window_targeted": True},
            "observation": {"screen_hash": "settings_bluetooth_skip"},
        },
        "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
        "message": "Top target: Devices via select_list_item.",
    }

    payload = router.advise(
        {
            "action": "advance_surface_exploration",
            "app_name": "settings",
            "query": "Bluetooth",
                "attempted_targets": [
                    {
                        "kind": "hypothesis",
                        "candidate_id": "list_bluetooth",
                        "selected_action": "select_list_item",
                        "label": "Bluetooth",
                        "selected_candidate_label": "Bluetooth",
                    }
                ],
            }
        )

    assert payload["status"] == "success"
    assert payload["route_mode"] == "surface_exploration_advance"
    assert payload["exploration_selection"]["candidate_id"] == "list_devices"
    assert payload["exploration_selection"]["attempted_target_count"] == 1
    assert payload["exploration_plan"]["attempted_target_count"] == 1
    assert payload["exploration_plan"]["remaining_target_count"] == 1


def test_desktop_action_router_advise_surface_exploration_prefers_nested_dialog_resolution(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2210, "title": "Bluetooth & devices", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2210, "title": "Bluetooth & devices", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    router.surface_exploration_plan = lambda **_kwargs: {  # type: ignore[method-assign]
        "status": "success",
        "surface_mode": "dialog_resolution",
        "automation_ready": True,
        "manual_attention_required": False,
        "hypothesis_count": 2,
        "branch_action_count": 0,
        "top_hypotheses": [
            {
                "candidate_id": "row_previous",
                "label": "Previous page",
                "suggested_action": "select_list_item",
                "confidence": 0.92,
                "reason": "Previous page is a strong visible target.",
                "action_payload": {
                    "action": "select_list_item",
                    "app_name": "settings",
                    "window_title": "Bluetooth & devices",
                    "query": "Previous page",
                    "control_type": "ListItem",
                    "element_id": "row_previous",
                },
            },
            {
                "candidate_id": "dialog_ok",
                "label": "OK",
                "suggested_action": "press_dialog_button",
                "confidence": 0.84,
                "reason": "OK resolves the visible nested dialog.",
                "action_payload": {
                    "action": "press_dialog_button",
                    "app_name": "settings",
                    "window_title": "Bluetooth & devices",
                    "query": "OK",
                    "control_type": "Button",
                    "element_id": "dialog_ok",
                },
            },
        ],
        "branch_actions": [],
        "surface_snapshot": {
            "status": "success",
            "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
            "target_window": {"hwnd": 2210, "title": "Bluetooth & devices"},
            "active_window": {"hwnd": 2210, "title": "Bluetooth & devices"},
            "candidate_windows": [{"hwnd": 2210, "title": "Bluetooth & devices"}],
            "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
            "safety_signals": {"dialog_visible": True},
            "surface_flags": {"dialog_visible": True, "window_targeted": True},
            "observation": {"screen_hash": "nested_dialog_surface"},
        },
        "filters": {"app_name": "settings", "window_title": "Bluetooth & devices", "query": "Bluetooth"},
        "message": "Nested dialog surfaced inside Bluetooth settings.",
    }

    payload = router.advise(
        {
            "action": "advance_surface_exploration",
            "app_name": "settings",
            "window_title": "Bluetooth & devices",
            "query": "Bluetooth",
            "branch_history": [
                {
                    "transition_kind": "dialog_shift",
                    "selected_action": "press_dialog_button",
                    "selected_candidate_id": "dialog_ok",
                    "selected_candidate_label": "OK",
                    "window_title": "Bluetooth & devices",
                    "surface_path_tail": ["Devices", "Bluetooth"],
                }
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["exploration_selection"]["selected_action"] == "press_dialog_button"
    assert payload["exploration_selection"]["candidate_id"] == "dialog_ok"
    assert float(payload["exploration_selection"]["branch_score"]) > 0.0
    assert payload["execution_plan"][-1]["args"]["query"] == "OK"


def test_desktop_action_router_advise_surface_exploration_prefers_native_child_dialog_cluster(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [
                    {"hwnd": 2310, "title": "Bluetooth & devices", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
                    {"hwnd": 2311, "title": "Pair device", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
                ],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2311, "title": "Pair device", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    router.surface_exploration_plan = lambda **_kwargs: {  # type: ignore[method-assign]
        "status": "success",
        "surface_mode": "dialog_resolution",
        "automation_ready": True,
        "manual_attention_required": False,
        "hypothesis_count": 2,
        "branch_action_count": 0,
        "top_hypotheses": [
            {
                "candidate_id": "row_previous",
                "label": "Previous page",
                "suggested_action": "select_list_item",
                "confidence": 0.76,
                "reason": "Previous page is a strong visible target.",
                "action_payload": {
                    "action": "select_list_item",
                    "app_name": "settings",
                    "window_title": "Bluetooth & devices",
                    "query": "Previous page",
                    "control_type": "ListItem",
                    "element_id": "row_previous",
                },
            },
            {
                "candidate_id": "dialog_ok",
                "label": "OK",
                "suggested_action": "press_dialog_button",
                "confidence": 0.65,
                "reason": "OK resolves the adopted child dialog.",
                "action_payload": {
                    "action": "press_dialog_button",
                    "app_name": "settings",
                    "window_title": "Pair device",
                    "query": "OK",
                    "control_type": "Button",
                    "element_id": "dialog_ok",
                },
            },
        ],
        "branch_actions": [],
        "surface_snapshot": {
            "status": "success",
            "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
            "target_window": {"hwnd": 2311, "title": "Pair device"},
            "active_window": {"hwnd": 2310, "title": "Bluetooth & devices"},
            "candidate_windows": [{"hwnd": 2311, "title": "Pair device"}],
            "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
            "safety_signals": {},
            "surface_flags": {"window_targeted": True},
            "native_window_topology": {
                "topology_signature": "settings|3|2",
                "same_process_window_count": 3,
                "related_window_count": 2,
                "child_dialog_like_visible": True,
            },
            "window_reacquisition": {
                "candidate": {"hwnd": 2311, "title": "Pair device", "match_score": 0.82},
                "same_process_window_count": 3,
                "related_window_count": 2,
                "child_dialog_like_visible": True,
            },
            "observation": {"screen_hash": "pair_device_child_dialog"},
        },
        "filters": {"app_name": "settings", "window_title": "Bluetooth & devices", "query": "Bluetooth"},
        "message": "A child pairing dialog is open inside Bluetooth settings.",
    }

    payload = router.advise(
        {
            "action": "advance_surface_exploration",
            "app_name": "settings",
            "window_title": "Bluetooth & devices",
            "query": "Bluetooth",
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "select_list_item",
                    "selected_candidate_id": "row_previous",
                    "selected_candidate_label": "Previous page",
                    "window_title": "Bluetooth & devices",
                    "surface_path_tail": ["Devices", "Bluetooth"],
                }
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["exploration_selection"]["selected_action"] == "press_dialog_button"
    assert payload["exploration_selection"]["candidate_id"] == "dialog_ok"
    assert float(payload["exploration_selection"]["branch_score"]) >= 0.2
    assert payload["execution_plan"][-1]["args"]["query"] == "OK"


def test_desktop_action_router_advise_surface_exploration_forwards_branch_cascade_context_to_rust(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    rust_calls: List[str] = []
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        rust_request_handler=lambda event, payload, _timeout_s: (
            rust_calls.append(event)
            or (
                {
                    "status": "success",
                    "data": {
                        "router_hint": "prefer_branch_cascade_dialog",
                        "prefer_nested_branch": True,
                        "loop_risk": False,
                        "ranked_candidates": [
                            {
                                "selection_key": router._surface_exploration_selection_key(
                                    kind="hypothesis",
                                    candidate_id="dialog_ok",
                                    selected_action="press_dialog_button",
                                    label="OK",
                                ),
                                "rank": 1,
                                "rust_score": 0.31,
                                "router_hint": "prefer_branch_cascade_dialog",
                                "reasons": ["branch_cascade_dialog_resolution:2"],
                            }
                        ],
                    },
                }
                if event == "surface_exploration_router"
                and payload.get("branch_cascade_count") == 2
                and payload.get("branch_cascade_kind_count") == 2
                and payload.get("branch_cascade_signature") == "child_window_chain>dialog_shift"
                else {"status": "error", "message": "unexpected rust payload"}
            )
        ),
        settle_delay_s=0.0,
    )
    router.surface_exploration_plan = lambda **_kwargs: {  # type: ignore[method-assign]
        "status": "success",
        "surface_mode": "dialog_resolution",
        "automation_ready": True,
        "manual_attention_required": False,
        "hypothesis_count": 2,
        "branch_action_count": 0,
        "top_hypotheses": [
            {
                "candidate_id": "dialog_ok",
                "label": "OK",
                "suggested_action": "press_dialog_button",
                "confidence": 0.74,
                "reason": "OK resolves the active dialog branch.",
                "action_payload": {
                    "action": "press_dialog_button",
                    "app_name": "settings",
                    "window_title": "Pair device",
                    "query": "OK",
                    "control_type": "Button",
                    "element_id": "dialog_ok",
                },
            },
            {
                "candidate_id": "row_previous",
                "label": "Previous page",
                "suggested_action": "select_list_item",
                "confidence": 0.77,
                "reason": "Previous page is visible.",
                "action_payload": {
                    "action": "select_list_item",
                    "app_name": "settings",
                    "window_title": "Pair device",
                    "query": "Previous page",
                    "control_type": "ListItem",
                    "element_id": "row_previous",
                },
            },
        ],
        "branch_actions": [],
        "surface_snapshot": {
            "status": "success",
            "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
            "target_window": {"hwnd": 2411, "title": "Pair device"},
            "active_window": {"hwnd": 2411, "title": "Pair device"},
            "candidate_windows": [{"hwnd": 2411, "title": "Pair device"}],
            "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
            "safety_signals": {"dialog_visible": True},
            "surface_flags": {"window_targeted": True, "dialog_visible": True},
            "native_window_topology": {
                "topology_signature": "settings|4|3|3",
                "same_process_window_count": 4,
                "related_window_count": 3,
                "owner_link_count": 3,
                "owner_chain_visible": True,
                "same_root_owner_window_count": 3,
                "same_root_owner_dialog_like_count": 2,
                "active_owner_chain_depth": 2,
                "max_owner_chain_depth": 2,
                "modal_chain_signature": "2410|2|2|2",
                "child_dialog_like_visible": True,
            },
            "window_reacquisition": {
                "candidate": {"hwnd": 2411, "title": "Pair device", "match_score": 0.85},
                "same_process_window_count": 4,
                "related_window_count": 3,
                "owner_link_count": 3,
                "owner_chain_visible": True,
                "same_root_owner_window_count": 3,
                "same_root_owner_dialog_like_count": 2,
                "modal_chain_signature": "2410|2|2|2",
                "child_dialog_like_visible": True,
            },
            "observation": {"screen_hash": "pair_device_branch_cascade"},
        },
        "filters": {"app_name": "settings", "window_title": "Pair device", "query": "Bluetooth"},
        "message": "A mixed dialog cascade is active in Bluetooth settings.",
    }

    payload = router.advise(
        {
            "action": "advance_surface_exploration",
            "app_name": "settings",
            "window_title": "Pair device",
            "query": "Bluetooth",
            "branch_history": [
                {
                    "transition_kind": "child_window_chain",
                    "selected_action": "select_list_item",
                    "selected_candidate_id": "row_bluetooth",
                    "selected_candidate_label": "Bluetooth",
                    "window_title": "Pair device",
                    "surface_path_tail": ["Devices", "Bluetooth", "Pair device"],
                },
                {
                    "transition_kind": "dialog_shift",
                    "selected_action": "press_dialog_button",
                    "selected_candidate_id": "dialog_confirm",
                    "selected_candidate_label": "Confirm",
                    "window_title": "Pair device",
                    "surface_path_tail": ["Devices", "Bluetooth", "Pair device"],
                },
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["exploration_selection"]["selected_action"] == "press_dialog_button"
    assert payload["exploration_selection"]["candidate_id"] == "dialog_ok"
    assert payload["exploration_selection"]["rust_router_hint"] == "prefer_branch_cascade_dialog"
    assert "surface_exploration_router" in rust_calls


def test_desktop_action_router_advise_surface_exploration_forwards_branch_family_context_to_rust(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    rust_calls: List[str] = []
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        rust_request_handler=lambda event, payload, _timeout_s: (
            rust_calls.append(event)
            or (
                {
                    "status": "success",
                    "data": {
                        "router_hint": "prefer_branch_family_dialog",
                        "prefer_nested_branch": True,
                        "loop_risk": False,
                        "ranked_candidates": [
                            {
                                "selection_key": router._surface_exploration_selection_key(
                                    kind="hypothesis",
                                    candidate_id="dialog_ok",
                                    selected_action="press_dialog_button",
                                    label="OK",
                                ),
                                "rank": 1,
                                "rust_score": 0.27,
                                "router_hint": "prefer_branch_family_dialog",
                                "reasons": ["branch_family_dialog_continuity:2"],
                            }
                        ],
                    },
                }
                if event == "surface_exploration_router"
                and payload.get("native_branch_family_signature") == "2410|2|Bluetooth & devices|Pair device"
                and payload.get("branch_family_repeat_count") == 2
                and payload.get("branch_family_switch_count") == 0
                and payload.get("branch_family_continuity") is True
                else {"status": "error", "message": "unexpected rust payload"}
            )
        ),
        settle_delay_s=0.0,
    )
    router.surface_exploration_plan = lambda **_kwargs: {  # type: ignore[method-assign]
        "status": "success",
        "surface_mode": "dialog_resolution",
        "automation_ready": True,
        "manual_attention_required": False,
        "hypothesis_count": 2,
        "branch_action_count": 0,
        "top_hypotheses": [
            {
                "candidate_id": "dialog_ok",
                "label": "OK",
                "suggested_action": "press_dialog_button",
                "confidence": 0.72,
                "reason": "OK resolves the active family dialog.",
                "action_payload": {
                    "action": "press_dialog_button",
                    "app_name": "settings",
                    "window_title": "Pair device",
                    "query": "OK",
                    "control_type": "Button",
                    "element_id": "dialog_ok",
                },
            },
            {
                "candidate_id": "row_previous",
                "label": "Previous page",
                "suggested_action": "select_list_item",
                "confidence": 0.75,
                "reason": "Previous page is visible.",
                "action_payload": {
                    "action": "select_list_item",
                    "app_name": "settings",
                    "window_title": "Pair device",
                    "query": "Previous page",
                    "control_type": "ListItem",
                    "element_id": "row_previous",
                },
            },
        ],
        "branch_actions": [],
        "surface_snapshot": {
            "status": "success",
            "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
            "target_window": {"hwnd": 2411, "title": "Pair device"},
            "active_window": {"hwnd": 2411, "title": "Pair device"},
            "candidate_windows": [{"hwnd": 2411, "title": "Pair device"}],
            "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
            "safety_signals": {"dialog_visible": True},
            "surface_flags": {"window_targeted": True, "dialog_visible": True},
            "native_window_topology": {
                "topology_signature": "settings|4|3|3",
                "same_process_window_count": 4,
                "related_window_count": 3,
                "owner_link_count": 3,
                "owner_chain_visible": True,
                "same_root_owner_window_count": 3,
                "same_root_owner_dialog_like_count": 2,
                "active_owner_chain_depth": 2,
                "max_owner_chain_depth": 2,
                "modal_chain_signature": "2410|2|2|Pair device|Confirm pairing",
                "branch_family_signature": "2410|2|Bluetooth & devices|Pair device",
                "child_dialog_like_visible": True,
            },
            "window_reacquisition": {
                "candidate": {"hwnd": 2411, "title": "Pair device", "match_score": 0.85},
                "same_process_window_count": 4,
                "related_window_count": 3,
                "owner_link_count": 3,
                "owner_chain_visible": True,
                "same_root_owner_window_count": 3,
                "same_root_owner_dialog_like_count": 2,
                "modal_chain_signature": "2410|2|2|Pair device|Confirm pairing",
                "branch_family_signature": "2410|2|Bluetooth & devices|Pair device",
                "child_dialog_like_visible": True,
            },
            "observation": {"screen_hash": "pair_device_branch_family"},
        },
        "filters": {"app_name": "settings", "window_title": "Pair device", "query": "Bluetooth"},
        "message": "A stable modal family is active in Bluetooth settings.",
    }

    payload = router.advise(
        {
            "action": "advance_surface_exploration",
            "app_name": "settings",
            "window_title": "Pair device",
            "query": "Bluetooth",
            "branch_history": [
                {
                    "transition_kind": "child_window_chain",
                    "selected_action": "press_dialog_button",
                    "selected_candidate_id": "dialog_continue",
                    "selected_candidate_label": "Continue",
                    "window_title": "Add a device",
                    "surface_path_tail": ["Devices", "Bluetooth", "Add a device"],
                    "topology_branch_family_signature": "2410|2|Bluetooth & devices|Pair device",
                },
                {
                    "transition_kind": "dialog_shift",
                    "selected_action": "press_dialog_button",
                    "selected_candidate_id": "dialog_confirm",
                    "selected_candidate_label": "Confirm",
                    "window_title": "Pair device",
                    "surface_path_tail": ["Devices", "Bluetooth", "Pair device"],
                    "topology_branch_family_signature": "2410|2|Bluetooth & devices|Pair device",
                },
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["exploration_selection"]["selected_action"] == "press_dialog_button"
    assert payload["exploration_selection"]["candidate_id"] == "dialog_ok"
    assert payload["exploration_selection"]["rust_router_hint"] == "prefer_branch_family_dialog"
    assert "surface_exploration_router" in rust_calls


def test_desktop_action_router_branch_history_merge_keeps_latest_repeated_branch_last(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={},
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    merged = router._merge_surface_exploration_branch_history(  # noqa: SLF001
        existing=[
            {
                "transition_kind": "child_window_chain",
                "selected_action": "press_dialog_button",
                "selected_candidate_id": "dialog_continue",
                "selected_candidate_label": "Continue",
                "window_title": "Add a device",
                "surface_path_tail": ["Devices", "Bluetooth", "Add a device"],
                "topology_branch_family_signature": "2410|2|Bluetooth & devices|Pair device",
                "occurrences": 1,
            },
            {
                "transition_kind": "dialog_shift",
                "selected_action": "press_dialog_button",
                "selected_candidate_id": "dialog_confirm",
                "selected_candidate_label": "Confirm",
                "window_title": "Pair device",
                "surface_path_tail": ["Devices", "Bluetooth", "Pair device"],
                "topology_branch_family_signature": "2410|2|Bluetooth & devices|Pair device",
                "occurrences": 1,
            },
        ],
        new_entry={
            "transition_kind": "child_window_chain",
            "selected_action": "press_dialog_button",
            "selected_candidate_id": "dialog_continue",
            "selected_candidate_label": "Continue",
            "window_title": "Add a device",
            "surface_path_tail": ["Devices", "Bluetooth", "Add a device"],
            "topology_branch_family_signature": "2410|2|Bluetooth & devices|Pair device",
        },
    )

    assert len(merged) == 2
    assert merged[-1]["transition_kind"] == "child_window_chain"
    assert merged[-1]["occurrences"] == 2
    assert merged[-1]["topology_branch_family_signature"] == "2410|2|Bluetooth & devices|Pair device"


def test_desktop_action_router_execute_surface_exploration_persists_followup_mission(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    invoked_targets: List[str] = []
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2202, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2202, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2202), "title": "Settings"}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": lambda payload: (
                invoked_targets.append(str(payload.get("element_id", "") or payload.get("query", ""))) or {"status": "success", "method": "invoke"}
            ),
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_surface",
                "text": "Settings bluetooth devices content",
                "screenshot_path": "E:/tmp/settings_surface.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    plans = [
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "list_bluetooth",
                    "label": "Bluetooth",
                    "suggested_action": "select_list_item",
                    "confidence": 0.91,
                    "reason": "Bluetooth is the strongest visible target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Bluetooth",
                        "control_type": "ListItem",
                        "element_id": "list_bluetooth",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2202, "title": "Settings"},
                "active_window": {"hwnd": 2202, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2202, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_before"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Bluetooth via select_list_item.",
        },
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "list_devices",
                    "label": "Devices",
                    "suggested_action": "select_list_item",
                    "confidence": 0.88,
                    "reason": "Devices is now the strongest visible follow-up target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Devices",
                        "control_type": "ListItem",
                        "element_id": "list_devices",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2202, "title": "Settings"},
                "active_window": {"hwnd": 2202, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2202, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_after"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Devices via select_list_item.",
        },
    ]
    plan_index = {"value": 0}

    def _exploration_plan(**_kwargs: Any) -> Dict[str, Any]:
        current = plans[min(plan_index["value"], len(plans) - 1)]
        plan_index["value"] += 1
        return current

    router.surface_exploration_plan = _exploration_plan  # type: ignore[method-assign]

    payload = router.execute(
        {
            "action": "advance_surface_exploration",
            "app_name": "settings",
            "query": "Bluetooth",
            "verify_after_action": False,
        }
    )

    assert payload["status"] == "partial"
    assert invoked_targets == ["List", "Bluetooth"]
    assert payload["exploration_mission"]["stop_reason_code"] == "exploration_followup_available"
    assert payload["exploration_mission"]["attempted_target_count"] == 1
    assert payload["exploration_mission"]["alternative_target_count"] == 1
    assert payload["mission_record"]["mission_kind"] == "exploration"
    assert payload["mission_record"]["resume_action"] == "advance_surface_exploration"
    assert payload["mission_record"]["selected_candidate_label"] == "Bluetooth"
    assert payload["mission_record"]["attempted_target_count"] == 1
    assert payload["mission_record"]["alternative_target_count"] == 1
    assert payload["exploration_mission"]["blocking_surface"]["attempted_target_count"] == 1
    assert payload["exploration_mission"]["blocking_surface"]["alternative_target_count"] == 1
    assert payload["exploration_mission"]["resume_contract"]["resume_payload"]["attempted_targets"][0]["candidate_id"] == "list_bluetooth"
    assert payload["exploration_plan"]["top_hypotheses"][0]["label"] == "Devices"


def test_desktop_action_router_surface_exploration_flow_auto_continues_to_completion(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    invoked_targets: List[str] = []
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2203, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2203, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2203), "title": "Settings"}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": lambda payload: (
                invoked_targets.append(str(payload.get("element_id", "") or payload.get("query", ""))) or {"status": "success", "method": "invoke"}
            ),
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_surface_flow",
                "text": "Settings bluetooth devices network content",
                "screenshot_path": "E:/tmp/settings_surface_flow.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    plans = [
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "list_bluetooth",
                    "label": "Bluetooth",
                    "suggested_action": "select_list_item",
                    "confidence": 0.91,
                    "reason": "Bluetooth is the strongest visible target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Bluetooth",
                        "control_type": "ListItem",
                        "element_id": "list_bluetooth",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2203, "title": "Settings"},
                "active_window": {"hwnd": 2203, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2203, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_before_flow_1"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Bluetooth via select_list_item.",
        },
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "list_devices",
                    "label": "Devices",
                    "suggested_action": "select_list_item",
                    "confidence": 0.88,
                    "reason": "Devices is now the strongest visible follow-up target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Devices",
                        "control_type": "ListItem",
                        "element_id": "list_devices",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2203, "title": "Settings"},
                "active_window": {"hwnd": 2203, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2203, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_after_flow_1"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Devices via select_list_item.",
        },
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "list_devices",
                    "label": "Devices",
                    "suggested_action": "select_list_item",
                    "confidence": 0.88,
                    "reason": "Devices remains the next best target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Devices",
                        "control_type": "ListItem",
                        "element_id": "list_devices",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2203, "title": "Settings"},
                "active_window": {"hwnd": 2203, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2203, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_before_flow_2"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Devices via select_list_item.",
        },
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": False,
            "manual_attention_required": False,
            "hypothesis_count": 0,
            "branch_action_count": 0,
            "top_hypotheses": [],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2203, "title": "Settings"},
                "active_window": {"hwnd": 2203, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2203, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_after_flow_2"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Surface recon completed without another strong target.",
        },
    ]
    plan_index = {"value": 0}

    def _exploration_plan(**_kwargs: Any) -> Dict[str, Any]:
        current = plans[min(plan_index["value"], len(plans) - 1)]
        plan_index["value"] += 1
        return current

    router.surface_exploration_plan = _exploration_plan  # type: ignore[method-assign]

    payload = router.execute(
        {
            "action": "complete_surface_exploration_flow",
            "app_name": "settings",
            "query": "Bluetooth",
            "verify_after_action": False,
            "max_exploration_steps": 3,
        }
    )

    assert payload["status"] == "success"
    assert "Bluetooth" in invoked_targets
    assert "Devices" in invoked_targets
    assert payload["exploration_mission"]["completed"] is True
    assert payload["exploration_mission"]["step_count"] == 2
    assert payload["exploration_mission"]["auto_continued"] is True
    assert payload["exploration_mission"]["attempted_target_count"] == 2
    assert payload["exploration_mission"]["alternative_target_count"] == 0


def test_desktop_action_router_surface_exploration_tracks_child_window_progress(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    invoked_targets: List[str] = []
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 3301, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 3301, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 3301), "title": "Settings"}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": lambda payload: (
                invoked_targets.append(str(payload.get("element_id", "") or payload.get("query", ""))) or {"status": "success", "method": "invoke"}
            ),
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_child_surface",
                "text": "Settings bluetooth details child surface",
                "screenshot_path": "E:/tmp/settings_child_surface.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    plans = [
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "row_bluetooth",
                    "label": "Bluetooth",
                    "suggested_action": "select_list_item",
                    "confidence": 0.92,
                    "reason": "Bluetooth is the strongest visible target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Bluetooth",
                        "control_type": "ListItem",
                        "element_id": "row_bluetooth",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 3301, "title": "Settings"},
                "active_window": {"hwnd": 3301, "title": "Settings"},
                "candidate_windows": [{"hwnd": 3301, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_child_before"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Bluetooth via select_list_item.",
        },
        {
            "status": "success",
            "surface_mode": "form_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "row_bluetooth",
                    "label": "Bluetooth",
                    "suggested_action": "select_list_item",
                    "confidence": 0.9,
                    "reason": "Bluetooth remains the active focus on the child settings surface.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Bluetooth & devices",
                        "query": "Bluetooth",
                        "control_type": "ListItem",
                        "element_id": "row_bluetooth",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 3302, "title": "Bluetooth & devices"},
                "active_window": {"hwnd": 3302, "title": "Bluetooth & devices"},
                "candidate_windows": [{"hwnd": 3302, "title": "Bluetooth & devices"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "form_page_state": {
                    "page_kind": "form_navigation",
                    "selected_navigation_target": "Bluetooth",
                    "breadcrumb_path": ["Devices", "Bluetooth"],
                },
                "surface_flags": {"form_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_child_after"},
            },
            "filters": {"app_name": "settings", "window_title": "Bluetooth & devices", "query": "Bluetooth"},
            "message": "Bluetooth moved into a nested child surface with more device controls.",
        },
    ]
    plan_index = {"value": 0}

    def _exploration_plan(**_kwargs: Any) -> Dict[str, Any]:
        current = plans[min(plan_index["value"], len(plans) - 1)]
        plan_index["value"] += 1
        return current

    router.surface_exploration_plan = _exploration_plan  # type: ignore[method-assign]

    payload = router.execute(
        {
            "action": "advance_surface_exploration",
            "app_name": "settings",
            "query": "Bluetooth",
            "verify_after_action": False,
        }
    )

    assert payload["status"] == "partial"
    assert "Bluetooth" in invoked_targets
    assert payload["exploration_mission"]["stop_reason_code"] == "exploration_nested_branch_available"
    assert payload["exploration_mission"]["transition_kind"] == "child_window"
    assert payload["exploration_mission"]["nested_surface_progressed"] is True
    assert payload["exploration_mission"]["child_window_adopted"] is True
    assert payload["exploration_mission"]["surface_path_tail"] == ["Devices", "Bluetooth"]
    assert payload["exploration_mission"]["window_title_history_tail"][-1] == "Bluetooth & devices"
    assert payload["exploration_mission"]["last_branch_kind"] == "child_window"
    assert payload["exploration_mission"]["branch_transition_count"] == 1
    assert payload["exploration_mission"]["branch_history_tail"][0]["window_title"] == "Bluetooth & devices"
    assert payload["mission_record"]["transition_kind"] == "child_window"
    assert payload["mission_record"]["child_window_adopted"] is True
    assert payload["mission_record"]["last_branch_kind"] == "child_window"
    assert payload["mission_record"]["branch_transition_count"] == 1
    assert payload["mission_record"]["page_history_tail"][0]["surface_path_after"] == ["Devices", "Bluetooth"]
    assert payload["mission_record"]["page_history_tail"][0]["window_title_after"] == "Bluetooth & devices"


def test_desktop_action_router_surface_exploration_flow_pauses_at_step_limit(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2204, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2204, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2204), "title": "Settings"}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": lambda _payload: {"status": "success", "method": "invoke"},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_surface_flow_limit",
                "text": "Settings bluetooth devices network more content",
                "screenshot_path": "E:/tmp/settings_surface_flow_limit.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    plans = [
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "list_bluetooth",
                    "label": "Bluetooth",
                    "suggested_action": "select_list_item",
                    "confidence": 0.91,
                    "reason": "Bluetooth is the strongest visible target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Bluetooth",
                        "control_type": "ListItem",
                        "element_id": "list_bluetooth",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2204, "title": "Settings"},
                "active_window": {"hwnd": 2204, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2204, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_limit_before_1"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Bluetooth via select_list_item.",
        },
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "list_devices",
                    "label": "Devices",
                    "suggested_action": "select_list_item",
                    "confidence": 0.88,
                    "reason": "Devices is now the strongest visible follow-up target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Devices",
                        "control_type": "ListItem",
                        "element_id": "list_devices",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2204, "title": "Settings"},
                "active_window": {"hwnd": 2204, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2204, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_limit_after_1"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Devices via select_list_item.",
        },
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "list_devices",
                    "label": "Devices",
                    "suggested_action": "select_list_item",
                    "confidence": 0.88,
                    "reason": "Devices remains the next best target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Devices",
                        "control_type": "ListItem",
                        "element_id": "list_devices",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2204, "title": "Settings"},
                "active_window": {"hwnd": 2204, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2204, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_limit_before_2"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Devices via select_list_item.",
        },
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "list_network",
                    "label": "Network",
                    "suggested_action": "select_list_item",
                    "confidence": 0.84,
                    "reason": "Network is now the next safe recon target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Network",
                        "control_type": "ListItem",
                        "element_id": "list_network",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2204, "title": "Settings"},
                "active_window": {"hwnd": 2204, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2204, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_limit_after_2"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Network via select_list_item.",
        },
    ]
    plan_index = {"value": 0}

    def _exploration_plan(**_kwargs: Any) -> Dict[str, Any]:
        current = plans[min(plan_index["value"], len(plans) - 1)]
        plan_index["value"] += 1
        return current

    router.surface_exploration_plan = _exploration_plan  # type: ignore[method-assign]

    payload = router.execute(
        {
            "action": "complete_surface_exploration_flow",
            "app_name": "settings",
            "query": "Bluetooth",
            "verify_after_action": False,
            "max_exploration_steps": 2,
        }
    )

    assert payload["status"] == "partial"
    assert payload["exploration_mission"]["completed"] is False
    assert payload["exploration_mission"]["stop_reason_code"] == "exploration_step_limit_reached"
    assert payload["exploration_mission"]["step_count"] == 2
    assert payload["exploration_mission"]["auto_continued"] is True
    assert payload["mission_record"]["mission_kind"] == "exploration"
    assert payload["mission_record"]["resume_action"] == "complete_surface_exploration_flow"
    assert payload["mission_record"]["recovery_profile"] == "resume_ready"


def test_desktop_action_router_surface_exploration_flow_pauses_at_nested_branch_limit(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    invoked_targets: List[str] = []
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2205, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2205, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2205), "title": "Settings"}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": lambda payload: (
                invoked_targets.append(str(payload.get("element_id", "") or payload.get("query", ""))) or {"status": "success", "method": "invoke"}
            ),
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_nested_limit",
                "text": "Settings bluetooth nested surface",
                "screenshot_path": "E:/tmp/settings_nested_limit.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    plans = [
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "row_bluetooth",
                    "label": "Bluetooth",
                    "suggested_action": "select_list_item",
                    "confidence": 0.92,
                    "reason": "Bluetooth is the strongest visible target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Bluetooth",
                        "control_type": "ListItem",
                        "element_id": "row_bluetooth",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2205, "title": "Settings"},
                "active_window": {"hwnd": 2205, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2205, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_nested_limit_before"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Bluetooth via select_list_item.",
        },
        {
            "status": "success",
            "surface_mode": "form_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "child_toggle",
                    "label": "Bluetooth toggle",
                    "suggested_action": "toggle_switch",
                    "confidence": 0.9,
                    "reason": "The nested child surface exposes a direct Bluetooth control.",
                    "action_payload": {
                        "action": "toggle_switch",
                        "app_name": "settings",
                        "window_title": "Bluetooth & devices",
                        "query": "Bluetooth toggle",
                        "control_type": "ToggleButton",
                        "element_id": "child_toggle",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2206, "title": "Bluetooth & devices"},
                "active_window": {"hwnd": 2206, "title": "Bluetooth & devices"},
                "candidate_windows": [{"hwnd": 2206, "title": "Bluetooth & devices"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "form_page_state": {
                    "page_kind": "form_navigation",
                    "selected_navigation_target": "Bluetooth",
                    "breadcrumb_path": ["Devices", "Bluetooth"],
                },
                "surface_flags": {"form_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_nested_limit_after"},
            },
            "filters": {"app_name": "settings", "window_title": "Bluetooth & devices", "query": "Bluetooth"},
            "message": "Bluetooth moved into a nested child surface with more device controls.",
        },
    ]
    plan_index = {"value": 0}

    def _exploration_plan(**_kwargs: Any) -> Dict[str, Any]:
        current = plans[min(plan_index["value"], len(plans) - 1)]
        plan_index["value"] += 1
        return current

    router.surface_exploration_plan = _exploration_plan  # type: ignore[method-assign]

    payload = router.execute(
        {
            "action": "complete_surface_exploration_flow",
            "app_name": "settings",
            "query": "Bluetooth",
            "verify_after_action": False,
            "max_exploration_steps": 1,
        }
    )

    assert payload["status"] == "partial"
    assert "Bluetooth" in invoked_targets
    assert payload["exploration_mission"]["completed"] is False
    assert payload["exploration_mission"]["stop_reason_code"] == "exploration_nested_branch_limit_reached"
    assert payload["exploration_mission"]["branch_transition_count"] == 1
    assert payload["exploration_mission"]["last_branch_kind"] == "child_window"
    assert payload["mission_record"]["recovery_profile"] == "resume_ready"
    assert payload["mission_record"]["last_branch_kind"] == "child_window"
    assert payload["mission_record"]["branch_transition_count"] == 1


def test_desktop_action_router_surface_exploration_blocks_repeated_nested_branch_loop(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    invoked_targets: List[str] = []
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2211, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2211, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2211), "title": "Settings"}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": lambda payload: (
                invoked_targets.append(str(payload.get("element_id", "") or payload.get("query", ""))) or {"status": "success", "method": "invoke"}
            ),
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_nested_repeat",
                "text": "Settings repeated bluetooth child surface",
                "screenshot_path": "E:/tmp/settings_nested_repeat.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    plans = [
        {
            "status": "success",
            "surface_mode": "list_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "row_bluetooth",
                    "label": "Bluetooth",
                    "suggested_action": "select_list_item",
                    "confidence": 0.92,
                    "reason": "Bluetooth is the strongest visible target.",
                    "action_payload": {
                        "action": "select_list_item",
                        "app_name": "settings",
                        "window_title": "Settings",
                        "query": "Bluetooth",
                        "control_type": "ListItem",
                        "element_id": "row_bluetooth",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2211, "title": "Settings"},
                "active_window": {"hwnd": 2211, "title": "Settings"},
                "candidate_windows": [{"hwnd": 2211, "title": "Settings"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"list_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_repeat_before"},
            },
            "filters": {"app_name": "settings", "window_title": "Settings", "query": "Bluetooth"},
            "message": "Top target: Bluetooth via select_list_item.",
        },
        {
            "status": "success",
            "surface_mode": "form_navigation",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "child_toggle",
                    "label": "Bluetooth toggle",
                    "suggested_action": "toggle_switch",
                    "confidence": 0.89,
                    "reason": "The repeated nested child surface exposes a Bluetooth control.",
                    "action_payload": {
                        "action": "toggle_switch",
                        "app_name": "settings",
                        "window_title": "Bluetooth & devices",
                        "query": "Bluetooth toggle",
                        "control_type": "ToggleButton",
                        "element_id": "child_toggle",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2212, "title": "Bluetooth & devices"},
                "active_window": {"hwnd": 2212, "title": "Bluetooth & devices"},
                "candidate_windows": [{"hwnd": 2212, "title": "Bluetooth & devices"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "form_page_state": {
                    "page_kind": "form_navigation",
                    "selected_navigation_target": "Bluetooth",
                    "breadcrumb_path": ["Devices", "Bluetooth"],
                },
                "surface_flags": {"form_visible": True, "window_targeted": True},
                "observation": {"screen_hash": "settings_repeat_after"},
            },
            "filters": {"app_name": "settings", "window_title": "Bluetooth & devices", "query": "Bluetooth"},
            "message": "Bluetooth moved into a nested child surface with more device controls.",
        },
    ]
    plan_index = {"value": 0}

    def _exploration_plan(**_kwargs: Any) -> Dict[str, Any]:
        current = plans[min(plan_index["value"], len(plans) - 1)]
        plan_index["value"] += 1
        return current

    router.surface_exploration_plan = _exploration_plan  # type: ignore[method-assign]

    payload = router.execute(
        {
            "action": "advance_surface_exploration",
            "app_name": "settings",
            "query": "Bluetooth",
            "verify_after_action": False,
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "select_list_item",
                    "selected_candidate_id": "row_bluetooth",
                    "selected_candidate_label": "Bluetooth",
                    "window_title": "Bluetooth & devices",
                    "surface_path_tail": ["Devices", "Bluetooth"],
                    "occurrences": 1,
                }
            ],
        }
    )

    assert payload["status"] == "blocked"
    assert "Bluetooth" in invoked_targets
    assert payload["exploration_mission"]["stop_reason_code"] == "exploration_nested_branch_loop_guard"
    assert payload["exploration_mission"]["branch_repeat_count"] == 2
    assert payload["mission_record"]["recovery_profile"] == "surface_review"


def test_desktop_action_router_surface_exploration_flow_respects_nested_chain_limit(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    invoked_targets: List[str] = []
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [
                    {"hwnd": 2221, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
                    {"hwnd": 2222, "title": "Pair device", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
                ],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2222, "title": "Pair device", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2222), "title": "Pair device"}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": lambda payload: (
                invoked_targets.append(str(payload.get("element_id", "") or payload.get("query", ""))) or {"status": "success", "method": "invoke"}
            ),
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_nested_chain_limit",
                "text": "Settings bluetooth pair device nested child surface",
                "screenshot_path": "E:/tmp/settings_nested_chain_limit.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    plan_calls = {"count": 0}

    def _surface_plan(**_kwargs: Any) -> Dict[str, Any]:
        plan_calls["count"] += 1
        if plan_calls["count"] == 1:
            return {
                "status": "success",
                "surface_mode": "dialog_resolution",
                "automation_ready": True,
                "manual_attention_required": False,
                "hypothesis_count": 1,
                "branch_action_count": 0,
                "top_hypotheses": [
                    {
                        "candidate_id": "dialog_pair",
                        "label": "Pair",
                        "suggested_action": "press_dialog_button",
                        "confidence": 0.92,
                        "reason": "The adopted child dialog exposes a safe next step.",
                        "action_payload": {
                            "action": "press_dialog_button",
                            "app_name": "settings",
                            "window_title": "Pair device",
                            "query": "Pair",
                            "control_type": "Button",
                            "element_id": "dialog_pair",
                        },
                    }
                ],
                "branch_actions": [],
                "surface_snapshot": {
                    "status": "success",
                    "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                    "target_window": {"hwnd": 2222, "title": "Pair device"},
                    "active_window": {"hwnd": 2222, "title": "Pair device"},
                    "candidate_windows": [{"hwnd": 2222, "title": "Pair device"}],
                    "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                    "safety_signals": {},
                    "surface_flags": {"window_targeted": True, "dialog_visible": True},
                    "native_window_topology": {
                        "topology_signature": "settings|3|2|2",
                        "same_process_window_count": 3,
                        "related_window_count": 2,
                        "owner_link_count": 2,
                        "owner_chain_visible": True,
                        "same_root_owner_window_count": 3,
                        "same_root_owner_dialog_like_count": 2,
                        "active_owner_chain_depth": 1,
                        "max_owner_chain_depth": 2,
                        "modal_chain_signature": "2221|2|2|1",
                        "child_dialog_like_visible": True,
                    },
                    "window_reacquisition": {
                        "candidate": {
                            "hwnd": 2222,
                            "title": "Pair device",
                            "match_score": 0.84,
                            "owner_hwnd": 2221,
                            "root_owner_hwnd": 2221,
                            "owner_chain_depth": 1,
                        },
                        "same_process_window_count": 3,
                        "related_window_count": 2,
                        "owner_link_count": 2,
                        "owner_chain_visible": True,
                        "same_root_owner_window_count": 3,
                        "same_root_owner_dialog_like_count": 2,
                        "modal_chain_signature": "2221|2|2|1",
                        "child_dialog_like_visible": True,
                    },
                    "form_page_state": {
                        "page_kind": "dialog_resolution",
                        "breadcrumb_path": ["Devices", "Bluetooth", "Pair device"],
                    },
                    "observation": {"screen_hash": "settings_nested_chain_limit"},
                },
                "filters": {"app_name": "settings", "window_title": "Pair device", "query": "Bluetooth"},
                "message": "A nested pairing dialog chain is still active.",
            }
        return {
            "status": "success",
            "surface_mode": "dialog_resolution",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "dialog_confirm_pair",
                    "label": "Confirm pairing",
                    "suggested_action": "press_dialog_button",
                    "confidence": 0.88,
                    "reason": "A deeper child confirmation dialog is now active.",
                    "action_payload": {
                        "action": "press_dialog_button",
                        "app_name": "settings",
                        "window_title": "Confirm Pairing",
                        "query": "Confirm",
                        "control_type": "Button",
                        "element_id": "dialog_confirm_pair",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2223, "title": "Confirm Pairing"},
                "active_window": {"hwnd": 2223, "title": "Confirm Pairing"},
                "candidate_windows": [{"hwnd": 2223, "title": "Confirm Pairing"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"window_targeted": True, "dialog_visible": True},
                "native_window_topology": {
                    "topology_signature": "settings|4|3|3",
                    "same_process_window_count": 4,
                    "related_window_count": 3,
                    "owner_link_count": 3,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 3,
                    "same_root_owner_dialog_like_count": 2,
                    "active_owner_chain_depth": 2,
                    "max_owner_chain_depth": 2,
                    "modal_chain_signature": "2221|2|2|2",
                    "child_dialog_like_visible": True,
                },
                "window_reacquisition": {
                    "candidate": {
                        "hwnd": 2223,
                        "title": "Confirm Pairing",
                        "match_score": 0.89,
                        "owner_hwnd": 2222,
                        "root_owner_hwnd": 2221,
                        "owner_chain_depth": 2,
                    },
                    "same_process_window_count": 4,
                    "related_window_count": 3,
                    "owner_link_count": 3,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 3,
                    "same_root_owner_dialog_like_count": 2,
                    "modal_chain_signature": "2221|2|2|2",
                    "child_dialog_like_visible": True,
                },
                "form_page_state": {
                    "page_kind": "dialog_resolution",
                    "breadcrumb_path": ["Devices", "Bluetooth", "Pair device", "Confirm Pairing"],
                },
                "observation": {"screen_hash": "settings_nested_chain_limit_confirm"},
            },
            "filters": {"app_name": "settings", "window_title": "Confirm Pairing", "query": "Bluetooth"},
            "message": "A deeper child confirmation dialog is active.",
        }

    router.surface_exploration_plan = _surface_plan  # type: ignore[method-assign]

    payload = router.execute(
        {
            "action": "complete_surface_exploration_flow",
            "app_name": "settings",
            "query": "Bluetooth",
            "verify_after_action": False,
            "max_exploration_steps": 3,
            "max_nested_branch_steps": 1,
            "max_branch_cascade_steps": 1,
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "select_list_item",
                    "selected_candidate_id": "list_bluetooth",
                    "selected_candidate_label": "Bluetooth",
                    "window_title": "Bluetooth & devices",
                    "surface_path_tail": ["Devices", "Bluetooth"],
                    "occurrences": 1,
                }
            ],
        }
    )

    assert payload["status"] == "partial"
    assert invoked_targets
    assert invoked_targets[0] in {"dialog_pair", "Pair"}
    assert payload["exploration_mission"]["stop_reason_code"] == "exploration_branch_cascade_limit_reached"
    assert payload["exploration_mission"]["transition_kind"] == "child_window_chain"
    assert payload["exploration_mission"]["last_branch_kind"] == "child_window_chain"
    assert payload["exploration_mission"]["nested_chain_count"] == 2
    assert payload["exploration_mission"]["child_window_chain_count"] == 2
    assert payload["exploration_mission"]["dialog_cascade_count"] == 0
    assert payload["exploration_mission"]["max_nested_branch_steps"] == 1
    assert payload["exploration_mission"]["branch_cascade_count"] == 1
    assert payload["exploration_mission"]["branch_cascade_kind_count"] == 1
    assert payload["exploration_mission"]["branch_cascade_signature"] == "child_window_chain"
    assert payload["exploration_mission"]["max_branch_cascade_steps"] == 1
    assert payload["exploration_mission"]["topology_same_root_owner_window_count"] == 3
    assert payload["exploration_mission"]["topology_same_root_owner_dialog_like_count"] == 2
    assert payload["exploration_mission"]["topology_active_owner_chain_depth"] == 2
    assert payload["exploration_mission"]["topology_max_owner_chain_depth"] == 2
    assert payload["exploration_mission"]["topology_modal_chain_signature"] == "2221|2|2|2"
    assert payload["mission_record"]["recovery_profile"] == "resume_ready"
    assert payload["mission_record"]["nested_chain_count"] == 2
    assert payload["mission_record"]["branch_cascade_count"] == 1
    assert payload["mission_record"]["branch_cascade_kind_count"] == 1
    assert payload["mission_record"]["branch_cascade_signature"] == "child_window_chain"
    assert payload["mission_record"]["topology_same_root_owner_window_count"] == 3
    assert payload["mission_record"]["topology_same_root_owner_dialog_like_count"] == 2
    assert payload["mission_record"]["dialog_cascade_count"] == 0


def test_desktop_action_router_surface_exploration_flow_continues_across_same_branch_family_chain(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    invoked_targets: List[str] = []
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2221, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2221, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2221), "title": "Settings"}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": lambda payload: (
                invoked_targets.append(str(payload.get("element_id", "") or payload.get("query", "")))
                or {"status": "success", "method": "invoke"}
            ),
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_same_family_chain",
                "text": "Settings bluetooth modal family chain",
                "screenshot_path": "E:/tmp/settings_same_family_chain.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    family_signature = "2221|2|Bluetooth & devices|Pair device"
    plans: List[Dict[str, Any]] = [
        {
            "status": "success",
            "surface_mode": "dialog_resolution",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "dialog_pair",
                    "label": "Pair",
                    "suggested_action": "press_dialog_button",
                    "confidence": 0.94,
                    "reason": "The pairing dialog is ready.",
                    "action_payload": {
                        "action": "press_dialog_button",
                        "app_name": "settings",
                        "window_title": "Pair device",
                        "query": "Pair",
                        "control_type": "Button",
                        "element_id": "dialog_pair",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2222, "title": "Pair device"},
                "active_window": {"hwnd": 2222, "title": "Pair device"},
                "candidate_windows": [{"hwnd": 2222, "title": "Pair device"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"window_targeted": True, "dialog_visible": True},
                "native_window_topology": {
                    "topology_signature": "settings|4|3|3",
                    "same_process_window_count": 4,
                    "related_window_count": 3,
                    "owner_link_count": 3,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 3,
                    "same_root_owner_dialog_like_count": 2,
                    "active_owner_chain_depth": 1,
                    "max_owner_chain_depth": 2,
                    "modal_chain_signature": "2221|2|2|1",
                    "branch_family_signature": family_signature,
                    "child_dialog_like_visible": True,
                },
                "window_reacquisition": {
                    "candidate": {
                        "hwnd": 2222,
                        "title": "Pair device",
                        "match_score": 0.9,
                        "owner_hwnd": 2221,
                        "root_owner_hwnd": 2221,
                        "owner_chain_depth": 1,
                    },
                    "same_process_window_count": 4,
                    "related_window_count": 3,
                    "owner_link_count": 3,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 3,
                    "same_root_owner_dialog_like_count": 2,
                    "modal_chain_signature": "2221|2|2|1",
                    "branch_family_signature": family_signature,
                    "child_dialog_like_visible": True,
                },
                "form_page_state": {
                    "page_kind": "dialog_resolution",
                    "breadcrumb_path": ["Devices", "Bluetooth", "Pair device"],
                },
                "observation": {"screen_hash": "settings_same_family_pair"},
            },
            "filters": {"app_name": "settings", "window_title": "Pair device", "query": "Bluetooth"},
            "message": "A pairing dialog is active.",
        },
        {
            "status": "success",
            "surface_mode": "dialog_resolution",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "dialog_confirm",
                    "label": "Confirm",
                    "suggested_action": "press_dialog_button",
                    "confidence": 0.91,
                    "reason": "The same modal family is still active and ready to confirm.",
                    "action_payload": {
                        "action": "press_dialog_button",
                        "app_name": "settings",
                        "window_title": "Confirm Pairing",
                        "query": "Confirm",
                        "control_type": "Button",
                        "element_id": "dialog_confirm",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2223, "title": "Confirm Pairing"},
                "active_window": {"hwnd": 2223, "title": "Confirm Pairing"},
                "candidate_windows": [{"hwnd": 2223, "title": "Confirm Pairing"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"window_targeted": True, "dialog_visible": True},
                "native_window_topology": {
                    "topology_signature": "settings|4|3|3",
                    "same_process_window_count": 4,
                    "related_window_count": 3,
                    "owner_link_count": 3,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 3,
                    "same_root_owner_dialog_like_count": 2,
                    "active_owner_chain_depth": 2,
                    "max_owner_chain_depth": 2,
                    "modal_chain_signature": "2221|2|2|2",
                    "branch_family_signature": family_signature,
                    "child_dialog_like_visible": True,
                },
                "window_reacquisition": {
                    "candidate": {
                        "hwnd": 2223,
                        "title": "Confirm Pairing",
                        "match_score": 0.88,
                        "owner_hwnd": 2222,
                        "root_owner_hwnd": 2221,
                        "owner_chain_depth": 2,
                    },
                    "same_process_window_count": 4,
                    "related_window_count": 3,
                    "owner_link_count": 3,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 3,
                    "same_root_owner_dialog_like_count": 2,
                    "modal_chain_signature": "2221|2|2|2",
                    "branch_family_signature": family_signature,
                    "child_dialog_like_visible": True,
                },
                "form_page_state": {
                    "page_kind": "dialog_resolution",
                    "breadcrumb_path": ["Devices", "Bluetooth", "Pair device", "Confirm Pairing"],
                },
                "observation": {"screen_hash": "settings_same_family_confirm"},
            },
            "filters": {"app_name": "settings", "window_title": "Confirm Pairing", "query": "Bluetooth"},
            "message": "A confirmation dialog in the same modal family is active.",
        },
        {
            "status": "success",
            "surface_mode": "dialog_resolution",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 0,
            "branch_action_count": 0,
            "top_hypotheses": [],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2221, "title": "Bluetooth & devices"},
                "active_window": {"hwnd": 2221, "title": "Bluetooth & devices"},
                "candidate_windows": [{"hwnd": 2221, "title": "Bluetooth & devices"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"window_targeted": True},
                "native_window_topology": {
                    "topology_signature": "settings|2|1|1",
                    "same_process_window_count": 2,
                    "related_window_count": 1,
                    "owner_link_count": 1,
                    "owner_chain_visible": False,
                    "same_root_owner_window_count": 1,
                    "same_root_owner_dialog_like_count": 0,
                    "active_owner_chain_depth": 0,
                    "max_owner_chain_depth": 0,
                    "modal_chain_signature": "",
                    "branch_family_signature": family_signature,
                    "child_dialog_like_visible": False,
                },
                "window_reacquisition": {
                    "candidate": {"hwnd": 2221, "title": "Bluetooth & devices", "match_score": 0.86, "owner_hwnd": 0, "root_owner_hwnd": 0, "owner_chain_depth": 0},
                    "same_process_window_count": 2,
                    "related_window_count": 1,
                    "owner_link_count": 1,
                    "owner_chain_visible": False,
                    "same_root_owner_window_count": 1,
                    "same_root_owner_dialog_like_count": 0,
                    "modal_chain_signature": "",
                    "branch_family_signature": family_signature,
                    "child_dialog_like_visible": False,
                },
                "observation": {"screen_hash": "settings_same_family_complete"},
            },
            "filters": {"app_name": "settings", "window_title": "Bluetooth & devices", "query": "Bluetooth"},
            "message": "Surface recon completed without another strong target.",
        },
    ]
    plan_index = {"value": 0}

    def _surface_plan(**_kwargs: Any) -> Dict[str, Any]:
        current = plans[min(plan_index["value"], len(plans) - 1)]
        plan_index["value"] += 1
        return current

    router.surface_exploration_plan = _surface_plan  # type: ignore[method-assign]

    payload = router.execute(
        {
            "action": "complete_surface_exploration_flow",
            "app_name": "settings",
            "query": "Bluetooth",
            "verify_after_action": False,
            "max_exploration_steps": 3,
            "max_nested_branch_steps": 1,
            "max_branch_cascade_steps": 1,
            "max_branch_family_switches": 1,
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "select_list_item",
                    "selected_candidate_id": "list_bluetooth",
                    "selected_candidate_label": "Bluetooth",
                    "window_title": "Bluetooth & devices",
                    "surface_path_tail": ["Devices", "Bluetooth"],
                    "topology_branch_family_signature": family_signature,
                    "occurrences": 1,
                }
            ],
        }
    )

    assert payload["status"] == "success"
    assert any(target in {"dialog_pair", "Pair"} for target in invoked_targets)
    assert any(target in {"dialog_confirm", "Confirm"} for target in invoked_targets)
    assert payload["exploration_mission"]["completed"] is True
    assert payload["exploration_mission"]["step_count"] >= 2
    assert payload["exploration_mission"]["max_branch_family_switches"] == 1
    assert payload["exploration_mission"]["branch_family_signature"] == family_signature
    assert payload["exploration_mission"]["branch_family_switch_count"] == 0
    assert payload["exploration_mission"]["branch_family_continuity"] is True
    assert payload["exploration_mission"]["stop_reason_code"] == ""


def test_desktop_action_router_surface_exploration_flow_uses_descendant_chain_budget(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    invoked_targets: List[str] = []
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2221, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2221, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2221), "title": "Settings"}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": lambda payload: (
                invoked_targets.append(str(payload.get("element_id", "") or payload.get("query", "")))
                or {"status": "success", "method": "invoke"}
            ),
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_descendant_chain_limit",
                "text": "Settings bluetooth descendant dialog chain",
                "screenshot_path": "E:/tmp/settings_descendant_chain_limit.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    child_chain_signature = "2221|Bluetooth & devices|pairing_chain"
    plans: List[Dict[str, Any]] = [
        {
            "status": "success",
            "surface_mode": "dialog_resolution",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "dialog_pair",
                    "label": "Pair",
                    "suggested_action": "press_dialog_button",
                    "confidence": 0.94,
                    "reason": "The pairing dialog is ready.",
                    "action_payload": {
                        "action": "press_dialog_button",
                        "app_name": "settings",
                        "window_title": "Pair device",
                        "query": "Pair",
                        "control_type": "Button",
                        "element_id": "dialog_pair",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2222, "title": "Pair device"},
                "active_window": {"hwnd": 2222, "title": "Pair device"},
                "candidate_windows": [{"hwnd": 2222, "title": "Pair device"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"window_targeted": True, "dialog_visible": True},
                "native_window_topology": {
                    "topology_signature": "settings|4|3|3",
                    "same_process_window_count": 4,
                    "related_window_count": 3,
                    "owner_link_count": 3,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 3,
                    "same_root_owner_dialog_like_count": 2,
                    "active_owner_chain_depth": 1,
                    "max_owner_chain_depth": 3,
                    "direct_child_window_count": 1,
                    "direct_child_dialog_like_count": 1,
                    "descendant_chain_depth": 1,
                    "descendant_dialog_chain_depth": 1,
                    "descendant_query_match_count": 1,
                    "child_chain_signature": child_chain_signature,
                    "modal_chain_signature": "2221|2|2|1",
                    "branch_family_signature": "2221|2|Bluetooth & devices|pair",
                    "child_dialog_like_visible": True,
                },
                "window_reacquisition": {
                    "candidate": {
                        "hwnd": 2222,
                        "title": "Pair device",
                        "match_score": 0.9,
                        "owner_hwnd": 2221,
                        "root_owner_hwnd": 2221,
                        "owner_chain_depth": 1,
                    },
                    "same_process_window_count": 4,
                    "related_window_count": 3,
                    "owner_link_count": 3,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 3,
                    "same_root_owner_dialog_like_count": 2,
                    "direct_child_window_count": 1,
                    "direct_child_dialog_like_count": 1,
                    "descendant_chain_depth": 1,
                    "descendant_dialog_chain_depth": 1,
                    "descendant_query_match_count": 1,
                    "child_chain_signature": child_chain_signature,
                    "modal_chain_signature": "2221|2|2|1",
                    "branch_family_signature": "2221|2|Bluetooth & devices|pair",
                    "child_dialog_like_visible": True,
                },
                "observation": {"screen_hash": "settings_descendant_pair"},
            },
            "filters": {"app_name": "settings", "window_title": "Pair device", "query": "Bluetooth"},
            "message": "A descendant pairing dialog is active.",
        },
        {
            "status": "success",
            "surface_mode": "dialog_resolution",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "dialog_confirm",
                    "label": "Confirm",
                    "suggested_action": "press_dialog_button",
                    "confidence": 0.93,
                    "reason": "The same descendant chain is ready to continue.",
                    "action_payload": {
                        "action": "press_dialog_button",
                        "app_name": "settings",
                        "window_title": "Confirm Pairing",
                        "query": "Confirm",
                        "control_type": "Button",
                        "element_id": "dialog_confirm",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2223, "title": "Confirm Pairing"},
                "active_window": {"hwnd": 2223, "title": "Confirm Pairing"},
                "candidate_windows": [{"hwnd": 2223, "title": "Confirm Pairing"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"window_targeted": True, "dialog_visible": True},
                "native_window_topology": {
                    "topology_signature": "settings|5|4|4",
                    "same_process_window_count": 5,
                    "related_window_count": 4,
                    "owner_link_count": 4,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 4,
                    "same_root_owner_dialog_like_count": 3,
                    "active_owner_chain_depth": 2,
                    "max_owner_chain_depth": 3,
                    "direct_child_window_count": 1,
                    "direct_child_dialog_like_count": 1,
                    "descendant_chain_depth": 2,
                    "descendant_dialog_chain_depth": 2,
                    "descendant_query_match_count": 1,
                    "child_chain_signature": child_chain_signature,
                    "modal_chain_signature": "2221|2|2|2",
                    "branch_family_signature": "2221|2|Bluetooth & devices|pair",
                    "child_dialog_like_visible": True,
                },
                "window_reacquisition": {
                    "candidate": {
                        "hwnd": 2223,
                        "title": "Confirm Pairing",
                        "match_score": 0.9,
                        "owner_hwnd": 2222,
                        "root_owner_hwnd": 2221,
                        "owner_chain_depth": 2,
                    },
                    "same_process_window_count": 5,
                    "related_window_count": 4,
                    "owner_link_count": 4,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 4,
                    "same_root_owner_dialog_like_count": 3,
                    "direct_child_window_count": 1,
                    "direct_child_dialog_like_count": 1,
                    "descendant_chain_depth": 2,
                    "descendant_dialog_chain_depth": 2,
                    "descendant_query_match_count": 1,
                    "child_chain_signature": child_chain_signature,
                    "modal_chain_signature": "2221|2|2|2",
                    "branch_family_signature": "2221|2|Bluetooth & devices|pair",
                    "child_dialog_like_visible": True,
                },
                "observation": {"screen_hash": "settings_descendant_confirm"},
            },
            "filters": {"app_name": "settings", "window_title": "Confirm Pairing", "query": "Bluetooth"},
            "message": "A deeper descendant confirmation dialog is active.",
        },
        {
            "status": "success",
            "surface_mode": "dialog_resolution",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "dialog_finish",
                    "label": "Finish Pairing",
                    "suggested_action": "press_dialog_button",
                    "confidence": 0.9,
                    "reason": "Another descendant dialog is ready, but the chain is already deep.",
                    "action_payload": {
                        "action": "press_dialog_button",
                        "app_name": "settings",
                        "window_title": "Finish Pairing",
                        "query": "Finish",
                        "control_type": "Button",
                        "element_id": "dialog_finish",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2224, "title": "Finish Pairing"},
                "active_window": {"hwnd": 2224, "title": "Finish Pairing"},
                "candidate_windows": [{"hwnd": 2224, "title": "Finish Pairing"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"window_targeted": True, "dialog_visible": True},
                "native_window_topology": {
                    "topology_signature": "settings|6|5|5",
                    "same_process_window_count": 6,
                    "related_window_count": 5,
                    "owner_link_count": 5,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 4,
                    "same_root_owner_dialog_like_count": 3,
                    "active_owner_chain_depth": 3,
                    "max_owner_chain_depth": 3,
                    "direct_child_window_count": 1,
                    "direct_child_dialog_like_count": 1,
                    "descendant_chain_depth": 3,
                    "descendant_dialog_chain_depth": 3,
                    "descendant_query_match_count": 1,
                    "child_chain_signature": child_chain_signature,
                    "modal_chain_signature": "2221|2|2|3",
                    "branch_family_signature": "2221|2|Bluetooth & devices|pair",
                    "child_dialog_like_visible": True,
                },
                "window_reacquisition": {
                    "candidate": {
                        "hwnd": 2224,
                        "title": "Finish Pairing",
                        "match_score": 0.89,
                        "owner_hwnd": 2223,
                        "root_owner_hwnd": 2221,
                        "owner_chain_depth": 3,
                    },
                    "same_process_window_count": 6,
                    "related_window_count": 5,
                    "owner_link_count": 5,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 4,
                    "same_root_owner_dialog_like_count": 3,
                    "direct_child_window_count": 1,
                    "direct_child_dialog_like_count": 1,
                    "descendant_chain_depth": 3,
                    "descendant_dialog_chain_depth": 3,
                    "descendant_query_match_count": 1,
                    "child_chain_signature": child_chain_signature,
                    "modal_chain_signature": "2221|2|2|3",
                    "branch_family_signature": "2221|2|Bluetooth & devices|pair",
                    "child_dialog_like_visible": True,
                },
                "observation": {"screen_hash": "settings_descendant_finish"},
            },
            "filters": {"app_name": "settings", "window_title": "Finish Pairing", "query": "Bluetooth"},
            "message": "The descendant chain can still continue, but the bounded budget should stop here.",
        },
    ]
    plan_index = {"value": 0}

    def _surface_plan(**_kwargs: Any) -> Dict[str, Any]:
        current = plans[min(plan_index["value"], len(plans) - 1)]
        plan_index["value"] += 1
        return current

    router.surface_exploration_plan = _surface_plan  # type: ignore[method-assign]

    payload = router.execute(
        {
            "action": "complete_surface_exploration_flow",
            "app_name": "settings",
            "query": "Bluetooth",
            "verify_after_action": False,
            "max_exploration_steps": 4,
            "max_nested_branch_steps": 1,
            "max_branch_cascade_steps": 1,
            "max_descendant_chain_steps": 2,
            "max_branch_family_switches": 1,
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "select_list_item",
                    "selected_candidate_id": "list_bluetooth",
                    "selected_candidate_label": "Bluetooth",
                    "window_title": "Bluetooth & devices",
                    "surface_path_tail": ["Devices", "Bluetooth"],
                    "occurrences": 1,
                }
            ],
        }
    )

    assert payload["status"] == "partial"
    assert len(invoked_targets) >= 2
    assert invoked_targets[0] in {"dialog_pair", "Pair"}
    assert invoked_targets[1] in {"dialog_confirm", "Confirm"}
    assert payload["exploration_mission"]["stop_reason_code"] == "exploration_descendant_chain_limit_reached"
    assert payload["exploration_mission"]["step_count"] == 2
    assert payload["exploration_mission"]["max_nested_branch_steps"] == 1
    assert payload["exploration_mission"]["max_branch_cascade_steps"] == 1
    assert payload["exploration_mission"]["max_descendant_chain_steps"] == 2
    assert payload["exploration_mission"]["descendant_chain_repeat_count"] == 2
    assert payload["exploration_mission"]["descendant_chain_continuity"] is True
    assert payload["exploration_mission"]["topology_descendant_chain_depth"] == 3
    assert payload["exploration_mission"]["topology_child_chain_signature"] == child_chain_signature
    assert payload["mission_record"]["recovery_profile"] == "resume_ready"
    assert payload["mission_record"]["stop_reason_code"] == "exploration_descendant_chain_limit_reached"
    assert payload["mission_record"]["max_descendant_chain_steps"] == 2
    assert payload["mission_record"]["descendant_chain_repeat_count"] == 2
    assert payload["mission_record"]["topology_descendant_chain_depth"] == 3
    assert payload["mission_record"]["topology_child_chain_signature"] == child_chain_signature


def test_desktop_action_router_surface_exploration_flow_pauses_at_branch_family_switch_limit(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    invoked_targets: List[str] = []
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2231, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2231, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2231), "title": "Settings"}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": lambda payload: (
                invoked_targets.append(str(payload.get("element_id", "") or payload.get("query", "")))
                or {"status": "success", "method": "invoke"}
            ),
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_branch_family_switch",
                "text": "Settings alternate modal family",
                "screenshot_path": "E:/tmp/settings_branch_family_switch.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )
    prior_family_signature = "2231|2|Bluetooth & devices|Pair device"
    switched_family_signature = "2231|2|Bluetooth & devices|Add device"
    plans: List[Dict[str, Any]] = [
        {
            "status": "success",
            "surface_mode": "dialog_resolution",
            "automation_ready": True,
            "manual_attention_required": False,
            "hypothesis_count": 1,
            "branch_action_count": 0,
            "top_hypotheses": [
                {
                    "candidate_id": "dialog_add_device",
                    "label": "Add device",
                    "suggested_action": "press_dialog_button",
                    "confidence": 0.93,
                    "reason": "A sibling modal family is active.",
                    "action_payload": {
                        "action": "press_dialog_button",
                        "app_name": "settings",
                        "window_title": "Add device",
                        "query": "Add device",
                        "control_type": "Button",
                        "element_id": "dialog_add_device",
                    },
                }
            ],
            "branch_actions": [],
            "surface_snapshot": {
                "status": "success",
                "app_profile": {"status": "success", "category": "utility", "name": "Settings"},
                "target_window": {"hwnd": 2232, "title": "Add device"},
                "active_window": {"hwnd": 2232, "title": "Add device"},
                "candidate_windows": [{"hwnd": 2232, "title": "Add device"}],
                "capabilities": {"accessibility": {"available": True}, "vision": {"available": True}},
                "safety_signals": {},
                "surface_flags": {"window_targeted": True, "dialog_visible": True},
                "native_window_topology": {
                    "topology_signature": "settings|4|3|3",
                    "same_process_window_count": 4,
                    "related_window_count": 3,
                    "owner_link_count": 3,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 3,
                    "same_root_owner_dialog_like_count": 2,
                    "active_owner_chain_depth": 2,
                    "max_owner_chain_depth": 2,
                    "modal_chain_signature": "2231|2|2|2",
                    "branch_family_signature": switched_family_signature,
                    "child_dialog_like_visible": True,
                },
                "window_reacquisition": {
                    "candidate": {
                        "hwnd": 2232,
                        "title": "Add device",
                        "match_score": 0.9,
                        "owner_hwnd": 2231,
                        "root_owner_hwnd": 2231,
                        "owner_chain_depth": 1,
                    },
                    "same_process_window_count": 4,
                    "related_window_count": 3,
                    "owner_link_count": 3,
                    "owner_chain_visible": True,
                    "same_root_owner_window_count": 3,
                    "same_root_owner_dialog_like_count": 2,
                    "modal_chain_signature": "2231|2|2|2",
                    "branch_family_signature": switched_family_signature,
                    "child_dialog_like_visible": True,
                },
                "form_page_state": {
                    "page_kind": "dialog_resolution",
                    "breadcrumb_path": ["Devices", "Bluetooth", "Add device"],
                },
                "observation": {"screen_hash": "settings_branch_family_switch_after"},
            },
            "filters": {"app_name": "settings", "window_title": "Add device", "query": "Bluetooth"},
            "message": "A sibling modal family is active.",
        },
    ]
    plan_index = {"value": 0}

    def _surface_plan(**_kwargs: Any) -> Dict[str, Any]:
        current = plans[min(plan_index["value"], len(plans) - 1)]
        plan_index["value"] += 1
        return current

    router.surface_exploration_plan = _surface_plan  # type: ignore[method-assign]

    payload = router.execute(
        {
            "action": "complete_surface_exploration_flow",
            "app_name": "settings",
            "query": "Bluetooth",
            "verify_after_action": False,
            "max_exploration_steps": 3,
            "max_nested_branch_steps": 3,
            "max_branch_cascade_steps": 3,
            "max_branch_family_switches": 1,
            "branch_history": [
                {
                    "transition_kind": "child_window",
                    "selected_action": "press_dialog_button",
                    "selected_candidate_id": "dialog_pair",
                    "selected_candidate_label": "Pair",
                    "window_title": "Pair device",
                    "surface_path_tail": ["Devices", "Bluetooth", "Pair device"],
                    "topology_branch_family_signature": prior_family_signature,
                    "occurrences": 1,
                }
            ],
        }
    )

    assert payload["status"] == "partial"
    assert any(target in {"dialog_add_device", "Add device"} for target in invoked_targets)
    assert payload["exploration_mission"]["completed"] is False
    assert payload["exploration_mission"]["stop_reason_code"] == "exploration_branch_family_switch_limit_reached"
    assert payload["exploration_mission"]["branch_family_signature"] == switched_family_signature
    assert payload["exploration_mission"]["branch_family_switch_count"] == 1
    assert payload["exploration_mission"]["max_branch_family_switches"] == 1
    assert payload["mission_record"]["recovery_profile"] == "resume_ready"
    assert payload["mission_record"]["recovery_priority"] == 92


def test_desktop_action_router_builds_navigation_workflow_for_browser(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "navigate", "app_name": "chrome", "query": "https://openai.com"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_navigation"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "l"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey", "keyboard_type"]


def test_desktop_action_router_skips_navigation_hotkey_when_address_bar_is_ready(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 777, "title": "OpenAI Docs - Google Chrome", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 777, "title": "OpenAI Docs - Google Chrome", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda payload: {
                "status": "success",
                "count": 1 if str(payload.get("query", "")).strip().lower() in {"address", "location"} else 0,
                "items": (
                    [{"name": str(payload.get("query", "")).strip().title(), "control_type": "Edit"}]
                    if str(payload.get("query", "")).strip().lower() in {"address", "location"}
                    else []
                ),
            },
            "computer_assert_text_visible": lambda payload: {
                "status": "success",
                "found": str(payload.get("text", "")).strip().lower() == "openai docs",
                "chars": len(str(payload.get("text", "") or "")),
                "text": payload.get("text", ""),
            },
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "chrome_address_ready",
                "text": "OpenAI Docs Address Bar",
                "screenshot_path": "E:/tmp/chrome_address_ready.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "navigate", "app_name": "chrome", "query": "https://openai.com/docs"})

    assert payload["status"] == "success"
    assert payload["surface_snapshot"]["surface_flags"]["address_bar_ready"] is True
    assert [step["action"] for step in payload["execution_plan"]] == ["keyboard_type"]
    assert any("type directly" in str(warning).lower() for warning in payload["warnings"])


def test_desktop_action_router_retries_command_palette_with_f1_fallback(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget"],
    )
    state: Dict[str, Any] = {"mode": "", "typed_text": ""}

    def _keyboard_hotkey(payload: Dict[str, Any]) -> Dict[str, Any]:
        keys = [str(item).strip().lower() for item in payload.get("keys", []) if str(item).strip()]
        state["mode"] = "+".join(keys)
        return {"status": "success", "keys": keys}

    def _keyboard_type(payload: Dict[str, Any]) -> Dict[str, Any]:
        if state.get("mode") == "f1":
            state["typed_text"] = str(payload.get("text", "") or "")
        else:
            state["typed_text"] = ""
        return {"status": "success", "chars": len(str(payload.get("text", "") or ""))}

    def _computer_observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if state.get("typed_text"):
            return {
                "status": "success",
                "screen_hash": "hash_after",
                "text": str(state.get("typed_text", "")),
                "screenshot_path": "E:/tmp/vscode_after.png",
            }
        return {
            "status": "success",
            "screen_hash": "hash_before",
            "text": "",
            "screenshot_path": "E:/tmp/vscode_before.png",
        }

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 601, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}],
            },
            "active_window": lambda _payload: {"status": "success", "window": {"hwnd": 601, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 601), "title": "main.py - Visual Studio Code"}},
            "keyboard_hotkey": _keyboard_hotkey,
            "keyboard_type": _keyboard_type,
            "computer_observe": _computer_observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "command",
            "app_name": "vscode",
            "text": "Preferences: Open Settings (JSON)",
            "verify_after_action": True,
            "retry_on_verification_failure": True,
            "max_strategy_attempts": 2,
        }
    )

    assert payload["status"] == "success"
    assert payload["attempt_count"] == 2
    assert payload["executed_strategy"]["strategy_id"] == "workflow_retry_2"
    assert payload["attempts"][0]["verification"]["verified"] is False
    assert payload["attempts"][1]["verification"]["verified"] is True
    assert payload["attempts"][1]["payload"]["keys"] == ["f1"]


def test_desktop_action_router_blocks_command_palette_when_profile_has_no_workflow(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Calculator                               Microsoft.WindowsCalculator  11.2402               winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "command", "app_name": "calculator", "text": "Toggle Word Wrap"})

    assert payload["status"] == "blocked"
    assert any("command palette" in blocker.lower() for blocker in payload["blockers"])


def test_desktop_action_router_promotes_learned_workflow_strategy_on_followup(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget"],
    )
    memory_path = tmp_path / "desktop_workflow_memory.json"
    state: Dict[str, Any] = {"mode": "", "typed_text": ""}

    def _keyboard_hotkey(payload: Dict[str, Any]) -> Dict[str, Any]:
        keys = [str(item).strip().lower() for item in payload.get("keys", []) if str(item).strip()]
        state["mode"] = "+".join(keys)
        return {"status": "success", "keys": keys}

    def _keyboard_type(payload: Dict[str, Any]) -> Dict[str, Any]:
        if state.get("mode") == "f1":
            state["typed_text"] = str(payload.get("text", "") or "")
        else:
            state["typed_text"] = ""
        return {"status": "success", "chars": len(str(payload.get("text", "") or ""))}

    def _computer_observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if state.get("typed_text"):
            return {
                "status": "success",
                "screen_hash": "hash_after",
                "text": str(state.get("typed_text", "")),
                "screenshot_path": "E:/tmp/vscode_memory_after.png",
            }
        return {
            "status": "success",
            "screen_hash": "hash_before",
            "text": "",
            "screenshot_path": "E:/tmp/vscode_memory_before.png",
        }

    learning_router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 701, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}],
            },
            "active_window": lambda _payload: {"status": "success", "window": {"hwnd": 701, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 701), "title": "main.py - Visual Studio Code"}},
            "keyboard_hotkey": _keyboard_hotkey,
            "keyboard_type": _keyboard_type,
            "computer_observe": _computer_observe,
        },
        app_profile_registry=registry,
        workflow_memory=DesktopWorkflowMemory(store_path=str(memory_path)),
        settle_delay_s=0.0,
    )

    result = learning_router.execute(
        {
            "action": "command",
            "app_name": "vscode",
            "text": "Preferences: Open Settings (JSON)",
            "verify_after_action": True,
            "retry_on_verification_failure": True,
            "max_strategy_attempts": 2,
        }
    )

    assert result["status"] == "success"
    assert result["executed_strategy"]["strategy_id"] == "workflow_retry_2"

    followup_router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 701, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}],
            },
            "active_window": lambda _payload: {"status": "success", "window": {"hwnd": 701, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=DesktopWorkflowMemory(store_path=str(memory_path)),
        settle_delay_s=0.0,
    )

    advice = followup_router.advise(
        {
            "action": "command",
            "app_name": "vscode",
            "text": "Preferences: Open Settings (JSON)",
            "verify_after_action": True,
            "retry_on_verification_failure": True,
            "max_strategy_attempts": 2,
        }
    )

    assert advice["status"] == "success"
    assert advice["adaptive_strategy"]["applied"] is True
    assert advice["strategy_variants"][0]["strategy_id"] == "workflow_retry_2"


def test_desktop_action_router_applies_learned_workflow_defaults(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget"],
    )
    memory_path = tmp_path / "desktop_workflow_memory.json"
    memory = DesktopWorkflowMemory(store_path=str(memory_path))
    for _ in range(2):
        memory.record_outcome(
            action="command",
            args={
                "action": "command",
                "app_name": "vscode",
                "text": "Preferences: Open Settings (JSON)",
                "focus_first": False,
                "ensure_app_launch": True,
                "target_mode": "ocr",
                "verify_mode": "ocr",
                "retry_on_verification_failure": True,
                "max_strategy_attempts": 3,
            },
            app_profile={"profile_id": "microsoft-visual-studio-code", "category": "editor"},
            strategy={"strategy_id": "workflow_retry_2", "payload_overrides": {"keys": ["f1"]}},
            attempt={"status": "success", "attempt": 2, "verification": {"enabled": True, "verified": True}},
            advice={
                "route_mode": "workflow_command",
                "surface_snapshot": {
                    "surface_intelligence": {
                        "surface_role": "command_palette",
                        "interaction_mode": "keyboard",
                    }
                },
            },
        )

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 701, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}],
            },
            "active_window": lambda _payload: {"status": "success", "window": {"hwnd": 701, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=DesktopWorkflowMemory(store_path=str(memory_path)),
        settle_delay_s=0.0,
    )

    advice = router.advise(
        {
            "action": "command",
            "app_name": "vscode",
            "text": "Preferences: Open Settings (JSON)",
        }
    )

    assert advice["status"] == "success"
    assert advice["adaptive_skill"]["status"] == "learned"
    assert advice["adaptive_skill"]["scope"] == "intent"
    assert advice["adaptive_skill"]["applied"] is True
    assert advice["adaptive_skill"]["applied_overrides"]["focus_first"] is False
    assert advice["adaptive_skill"]["applied_overrides"]["target_mode"] == "ocr"
    assert advice["adaptive_skill"]["recommended_overrides"]["ensure_app_launch"] is True
    assert advice["autonomy"]["focus_first"] is False
    assert advice["autonomy"]["ensure_app_launch"] is True
    assert advice["adaptive_strategy"]["skill_profile"]["recommended_overrides"]["target_mode"] == "ocr"


def test_desktop_action_router_builds_quick_open_workflow_for_editor(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "quick_open", "app_name": "vscode", "query": "settings.json"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_quick_open"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "p"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey", "keyboard_type"]
    assert payload["execution_plan"][-1]["args"]["press_enter"] is True


def test_desktop_action_router_builds_bookmarks_workflow_for_browser(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "open_bookmarks", "app_name": "chrome"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_bookmarks"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "shift", "o"]
    assert payload["verification_plan"]["verify_text"] == "bookmarks"
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_builds_focus_address_bar_workflow_for_browser(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "focus_address_bar", "app_name": "chrome"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_focus_address_bar"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "l"]
    assert payload["verification_plan"]["verify_text"] == "address"
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_builds_new_folder_workflow_for_file_manager() -> None:
    registry = DesktopAppProfileRegistry(source_paths=[])
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "new_folder", "app_name": "explorer"})

    assert payload["status"] == "success"
    assert payload["app_profile"]["category"] == "file_manager"
    assert payload["route_mode"] == "workflow_new_folder"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "shift", "n"]
    assert payload["verification_plan"]["verify_text"] == "new folder"
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "accessibility_invoke_element", "keyboard_hotkey"]
    assert payload["execution_plan"][2]["args"]["query"] == "Items View"


def test_desktop_action_router_builds_workspace_search_workflow_for_editor(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "workspace_search", "app_name": "vscode", "query": "TODO"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_workspace_search"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "shift", "f"]
    assert payload["execution_plan"][-1]["action"] == "keyboard_type"
    assert payload["execution_plan"][-1]["args"]["press_enter"] is False


def test_desktop_action_router_surface_snapshot_detects_browser_surfaces(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 901, "title": "Chrome - History", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 901, "title": "Chrome - History", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [{"name": "History", "control_type": "Document"}, {"name": "Bookmarks", "control_type": "Link"}],
            },
            "accessibility_find_element": lambda payload: {
                "status": "success",
                "count": 1 if str(payload.get("query", "")).lower() in {"history", "bookmarks"} else 0,
                "items": [{"name": str(payload.get("query", "")).title(), "control_type": "Link"}] if str(payload.get("query", "")).lower() in {"history", "bookmarks"} else [],
            },
            "computer_assert_text_visible": lambda payload: {
                "status": "success",
                "found": str(payload.get("text", "")).lower() == "history",
                "chars": len(str(payload.get("text", ""))),
                "text": payload.get("text", ""),
            },
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "history_hash",
                "text": "History Bookmarks Recent Tabs",
                "screenshot_path": "E:/tmp/chrome_surface.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="chrome", query="history", limit=10)

    assert payload["status"] == "success"
    assert payload["app_profile"]["category"] == "browser"
    assert payload["surface_flags"]["history_visible"] is True
    assert payload["surface_flags"]["bookmarks_visible"] is True
    assert "navigate" in payload["recommended_actions"]
    assert any(row["action"] == "open_history" and row["matched"] is True for row in payload["workflow_surfaces"])


def test_desktop_action_router_builds_new_tab_workflow_for_browser(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "new_tab", "app_name": "chrome"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_new_tab"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "t"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_builds_switch_tab_workflow_for_browser_direction(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "switch_tab", "app_name": "chrome", "query": "next"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_switch_tab"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "tab"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_builds_switch_tab_workflow_for_browser_index(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Mozilla Firefox                           Mozilla.Firefox              146.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "switch_tab", "app_name": "firefox", "query": "3"})

    assert payload["status"] == "success"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "3"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_builds_open_tab_search_workflow_for_browser(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "open_tab_search", "app_name": "chrome"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_open_tab_search"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "shift", "a"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_bootstraps_tab_search_before_typing_query(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1771, "title": "OpenAI Docs - Google Chrome", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1771, "title": "OpenAI Docs - Google Chrome", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_assert_text_visible": lambda payload: {
                "status": "success",
                "found": False,
                "chars": len(str(payload.get("text", "") or "")),
                "text": payload.get("text", ""),
            },
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "browser_without_tab_search",
                "text": "OpenAI Docs documentation",
                "screenshot_path": "E:/tmp/chrome_tab_search_idle.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "search_tabs", "app_name": "chrome", "query": "OpenAI"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_search_tabs"
    assert payload["surface_snapshot"]["surface_flags"]["tab_search_visible"] is False
    assert [step["action"] for step in payload["execution_plan"]] == ["keyboard_hotkey", "keyboard_type"]
    assert payload["execution_plan"][0]["phase"] == "preflight"
    assert payload["execution_plan"][0]["args"]["keys"] == ["ctrl", "shift", "a"]
    assert payload["execution_plan"][1]["args"]["text"] == "OpenAI"
    assert sum(1 for step in payload["execution_plan"] if step["action"] == "keyboard_hotkey") == 1
    assert any("bootstrap the surface" in str(warning).lower() for warning in payload["warnings"])


def test_desktop_action_router_preserves_ready_tab_search_surface_without_reopen(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1772, "title": "OpenAI Docs - Google Chrome", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1772, "title": "OpenAI Docs - Google Chrome", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "degraded", "capabilities": {"ocr_targets": False}},
            "focus_window": lambda payload: {
                "status": "success",
                "window": {"hwnd": payload.get("hwnd", 1772), "title": "OpenAI Docs - Google Chrome"},
            },
            "accessibility_find_element": lambda payload: {
                "status": "success",
                "count": 1 if "search tabs" in str(payload.get("query", "")).lower() else 0,
                "items": ([{"name": "Search tabs", "control_type": "Edit"}] if "search tabs" in str(payload.get("query", "")).lower() else []),
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "search_tabs", "app_name": "chrome", "query": "OpenAI"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_search_tabs"
    assert payload["surface_snapshot"]["surface_flags"]["tab_search_visible"] is True
    assert [step["action"] for step in payload["execution_plan"]] == ["keyboard_type"]
    assert payload["execution_plan"][0]["args"]["text"] == "OpenAI"
    assert any("type directly" in str(warning).lower() for warning in payload["warnings"])


def test_desktop_action_router_builds_new_tab_workflow_for_explorer(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["File Explorer                             Microsoft.FileExplorer      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "new_tab", "app_name": "explorer"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_new_tab"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "t"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_builds_go_back_workflow_for_browser(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "go_back", "app_name": "chrome"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_go_back"
    assert payload["workflow_profile"]["primary_hotkey"] == ["alt", "left"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_builds_go_forward_workflow_for_explorer(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["File Explorer                             Microsoft.FileExplorer      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "go_forward", "app_name": "explorer"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_go_forward"
    assert payload["workflow_profile"]["primary_hotkey"] == ["alt", "right"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_builds_focus_folder_tree_workflow_for_explorer(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["File Explorer                             Microsoft.FileExplorer      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "focus_folder_tree", "app_name": "explorer"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_focus_folder_tree"
    assert payload["workflow_profile"]["supports_action_dispatch"] is True
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "accessibility_invoke_element"]
    assert payload["execution_plan"][2]["args"]["query"] == "Navigation Pane"
    assert payload["execution_plan"][2]["args"]["action"] == "focus"


def test_desktop_action_router_builds_focus_file_list_workflow_for_explorer(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["File Explorer                             Microsoft.FileExplorer      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "focus_file_list", "app_name": "explorer"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_focus_file_list"
    assert payload["workflow_profile"]["supports_action_dispatch"] is True
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "accessibility_invoke_element"]
    assert payload["execution_plan"][2]["args"]["query"] == "Items View"
    assert payload["execution_plan"][2]["args"]["action"] == "focus"


def test_desktop_action_router_builds_zoom_workflow_for_browser(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Mozilla Firefox                           Mozilla.Firefox              146.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "zoom_in", "app_name": "firefox"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_zoom_in"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "equal"]
    assert payload["verification_plan"]["verify_text"] == "zoom"
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_builds_media_transport_workflow_for_media_app(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Spotify                                   Spotify.Spotify             1.2.71               winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "play_pause_media", "app_name": "spotify"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_play_pause_media"
    assert payload["workflow_profile"]["supports_system_action"] is True
    assert payload["workflow_profile"]["workflow_action"] == "media_play_pause"
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "media_play_pause"]


def test_desktop_action_router_builds_history_workflow_for_browser(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "open_history", "app_name": "chrome"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_history"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "h"]
    assert payload["verification_plan"]["verify_text"] == "history"
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_builds_jump_to_conversation_workflow_for_chat_app(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Slack                                     SlackTechnologies.Slack     4.41.105             winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "jump_to_conversation", "app_name": "slack", "query": "Alice"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_jump_to_conversation"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "k"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey", "keyboard_type"]
    assert payload["execution_plan"][-1]["args"]["text"] == "Alice"
    assert payload["execution_plan"][-1]["args"]["press_enter"] is True


def test_desktop_action_router_builds_send_message_workflow_for_chat_app(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Slack                                     SlackTechnologies.Slack     4.41.105             winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise(
        {
            "action": "send_message",
            "app_name": "slack",
            "query": "Alice",
            "text": "Standup in 10 minutes",
        }
    )

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_send_message"
    assert payload["workflow_profile"]["supports_direct_input"] is True
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey", "keyboard_type", "keyboard_type"]
    assert payload["execution_plan"][3]["args"]["text"] == "Alice"
    assert payload["execution_plan"][3]["args"]["press_enter"] is True
    assert payload["execution_plan"][4]["args"]["text"] == "Standup in 10 minutes"
    assert payload["execution_plan"][4]["args"]["press_enter"] is True


def test_desktop_action_router_sends_message_directly_in_active_chat_when_no_target_switch_is_needed(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Slack                                     SlackTechnologies.Slack     4.41.105             winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 734, "title": "Slack | Engineering", "exe": r"C:\Users\thecy\AppData\Local\slack\slack.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 734, "title": "Slack | Engineering", "exe": r"C:\Users\thecy\AppData\Local\slack\slack.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "send_message", "app_name": "slack", "text": "Build is green"})

    assert payload["status"] == "success"
    assert [step["action"] for step in payload["execution_plan"]] == ["keyboard_type"]
    assert payload["execution_plan"][0]["args"]["text"] == "Build is green"
    assert payload["execution_plan"][0]["args"]["press_enter"] is True


def test_desktop_action_router_skips_target_switch_when_requested_conversation_is_already_active(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Slack                                     SlackTechnologies.Slack     4.41.105             winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 734, "title": "Slack | Alice", "exe": r"C:\Users\thecy\AppData\Local\slack\slack.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 734, "title": "Slack | Alice", "exe": r"C:\Users\thecy\AppData\Local\slack\slack.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "slack_alice",
                "text": "Slack Alice Type a message",
                "screenshot_path": "E:/tmp/slack_alice.png",
            },
            "accessibility_find_element": lambda payload: {
                "status": "success",
                "count": 1 if str(payload.get("query", "")).lower() in {"type a message", "write a message", "reply"} else 0,
                "items": [{"name": "Type a message", "control_type": "Edit"}]
                if str(payload.get("query", "")).lower() in {"type a message", "write a message", "reply"}
                else [],
            },
            "computer_assert_text_visible": lambda payload: {
                "status": "success",
                "found": str(payload.get("text", "")).lower() in {"type a message", "alice"},
                "chars": len(str(payload.get("text", "") or "")),
                "text": payload.get("text", ""),
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "send_message", "app_name": "slack", "query": "Alice", "text": "Build is green"})

    assert payload["status"] == "success"
    assert payload["surface_snapshot"]["surface_flags"]["conversation_target_active"] is True
    assert [step["action"] for step in payload["execution_plan"]] == ["keyboard_type"]
    assert payload["execution_plan"][0]["args"]["text"] == "Build is green"
    assert payload["execution_plan"][0]["args"]["press_enter"] is True


def test_desktop_action_router_builds_start_presentation_workflow_for_office_app(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Microsoft PowerPoint                      Microsoft.Office.PowerPoint 2502.0               winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "start_presentation", "app_name": "powerpoint"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_start_presentation"
    assert payload["workflow_profile"]["primary_hotkey"] == ["f5"]
    assert payload["verification_plan"]["verify_text"] == "slide show"
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_builds_toggle_terminal_workflow_for_editor(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "toggle_terminal", "app_name": "vscode"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_toggle_terminal"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "`"]
    assert payload["verification_plan"]["verify_text"] == "terminal"
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "keyboard_hotkey"]


def test_desktop_action_router_verifies_bookmarks_with_accessibility_probe(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )

    def _computer_observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "status": "success",
            "screen_hash": "hash_static",
            "text": "",
            "screenshot_path": "E:/tmp/chrome_static.png",
        }

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 801, "title": "Chrome - Bookmarks", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 801, "title": "Chrome - Bookmarks", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 801), "title": "Chrome - Bookmarks"}},
            "keyboard_hotkey": lambda payload: {"status": "success", "keys": payload.get("keys", [])},
            "computer_observe": _computer_observe,
            "accessibility_find_element": lambda payload: {
                "status": "success",
                "count": 1 if str(payload.get("query", "")).lower() == "bookmarks" else 0,
                "items": [{"name": "Bookmarks", "control_type": "TreeItem"}] if str(payload.get("query", "")).lower() == "bookmarks" else [],
            },
            "computer_assert_text_visible": lambda payload: {"status": "success", "found": False, "chars": 0, "text": payload.get("text", "")},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "open_bookmarks",
            "app_name": "chrome",
            "verify_after_action": True,
            "retry_on_verification_failure": False,
        }
    )

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert any(check.get("name") == "workflow_probe_match" and check.get("passed") is True for check in payload["verification"]["checks"])


def test_desktop_action_router_workflow_catalog_annotates_profile_support(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        [
            "Google Chrome                             Google.Chrome.EXE            145.0                winget",
            "Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget",
        ],
    )
    router = DesktopActionRouter(
        action_handlers={},
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    browser_catalog = router.workflow_catalog(app_name="chrome", category="browser", limit=80)
    editor_catalog = router.workflow_catalog(app_name="vscode", query="terminal", limit=20)

    history_item = next(item for item in browser_catalog["items"] if item["action"] == "open_history")
    terminal_item = next(item for item in editor_catalog["items"] if item["action"] == "toggle_terminal")

    assert browser_catalog["status"] == "success"
    assert browser_catalog["profile"]["category"] == "browser"
    assert history_item["supported"] is True
    assert history_item["primary_hotkey"] == ["ctrl", "h"]
    assert editor_catalog["status"] == "success"
    assert editor_catalog["profile"]["category"] == "code_editor"
    assert terminal_item["supported"] is True
    assert terminal_item["verify_hint"] == "terminal"


def test_desktop_action_router_workflow_catalog_exposes_probe_metadata(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        [
            "Google Chrome                             Google.Chrome.EXE            145.0                winget",
            "Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget",
        ],
    )
    router = DesktopActionRouter(
        action_handlers={},
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    browser_catalog = router.workflow_catalog(app_name="chrome", query="bookmark", limit=20)
    editor_catalog = router.workflow_catalog(app_name="vscode", query="workspace", limit=20)

    bookmarks_item = next(item for item in browser_catalog["items"] if item["action"] == "open_bookmarks")
    workspace_item = next(item for item in editor_catalog["items"] if item["action"] == "workspace_search")

    assert any(str(row.get("query", "")).lower() == "bookmarks" for row in bookmarks_item["probe_queries"])
    assert "navigate" in bookmarks_item["recommended_followups"]
    assert any(str(row.get("query", "")).lower() == "search" for row in workspace_item["probe_queries"])
    assert "go_to_symbol" in workspace_item["recommended_followups"]


def test_desktop_action_router_workflow_catalog_exposes_chat_and_office_workflow_metadata(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        [
            "Slack                                     SlackTechnologies.Slack     4.41.105             winget",
            "Microsoft PowerPoint                      Microsoft.Office.PowerPoint 2502.0               winget",
        ],
    )
    router = DesktopActionRouter(
        action_handlers={},
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    chat_catalog = router.workflow_catalog(app_name="slack", query="message", limit=20)
    office_catalog = router.workflow_catalog(app_name="powerpoint", query="presentation", limit=20)

    send_item = next(item for item in chat_catalog["items"] if item["action"] == "send_message")
    presentation_item = next(item for item in office_catalog["items"] if item["action"] == "start_presentation")

    assert chat_catalog["profile"]["category"] == "chat"
    assert send_item["supported"] is True
    assert send_item["required_fields"] == ["text"]
    assert len(send_item["input_sequence"]) == 2
    assert office_catalog["profile"]["category"] == "office"
    assert presentation_item["supported"] is True
    assert presentation_item["primary_hotkey"] == ["f5"]


def test_desktop_action_router_workflow_catalog_exposes_system_and_zoom_workflows(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        [
            "Mozilla Firefox                           Mozilla.Firefox              146.0                winget",
            "Windows Settings                          Microsoft.WindowsSettings   1.0                  winget",
            "Task Manager                              Microsoft.TaskManager       11.0                 winget",
        ],
    )
    router = DesktopActionRouter(
        action_handlers={},
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    browser_catalog = router.workflow_catalog(app_name="firefox", query="zoom", limit=20)
    settings_catalog = router.workflow_catalog(app_name="settings", query="tab", limit=20)
    settings_generic_catalog = router.workflow_catalog(app_name="settings", query="context", limit=20)
    task_manager_catalog = router.workflow_catalog(app_name="task manager", query="tab", limit=20)

    zoom_item = next(item for item in browser_catalog["items"] if item["action"] == "zoom_in")
    settings_tab_item = next(item for item in settings_catalog["items"] if item["action"] == "switch_tab")
    settings_context_item = next(item for item in settings_generic_catalog["items"] if item["action"] == "open_context_menu")
    task_manager_tab_item = next(item for item in task_manager_catalog["items"] if item["action"] == "switch_tab")

    assert browser_catalog["profile"]["category"] == "browser"
    assert zoom_item["supported"] is True
    assert zoom_item["primary_hotkey"] == ["ctrl", "equal"]
    assert settings_catalog["profile"]["category"] == "utility"
    assert settings_tab_item["supported"] is True
    assert settings_tab_item["primary_hotkey"] == ["ctrl", "tab"]
    assert settings_context_item["supported"] is True
    assert settings_context_item["primary_hotkey"] == ["shift", "f10"]
    assert task_manager_catalog["profile"]["category"] == "ops_console"
    assert task_manager_tab_item["supported"] is True
    assert task_manager_tab_item["primary_hotkey"] == ["ctrl", "tab"]


def test_desktop_action_router_builds_focus_sidebar_workflow_for_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "focus_sidebar", "app_name": "settings"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_focus_sidebar"
    assert payload["workflow_profile"]["supports_action_dispatch"] is True
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "accessibility_invoke_element"]
    assert payload["execution_plan"][2]["args"]["query"] == "Sidebar"


def test_desktop_action_router_builds_select_sidebar_item_workflow_for_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "select_sidebar_item", "app_name": "settings", "query": "Bluetooth"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_select_sidebar_item"
    assert payload["workflow_profile"]["supports_action_dispatch"] is True
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "accessibility_invoke_element", "accessibility_invoke_element"]
    assert payload["execution_plan"][2]["args"]["query"] == "Sidebar"
    assert payload["execution_plan"][3]["args"]["query"] == "Bluetooth"


def test_desktop_action_router_preflights_sidebar_before_select_sidebar_item_in_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2013, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2013, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_main_only",
                "text": "Settings content toolbar bluetooth devices personalization",
                "screenshot_path": "E:/tmp/settings_main_only.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "select_sidebar_item", "app_name": "settings", "query": "Bluetooth"})

    assert payload["status"] == "success"
    assert payload["surface_snapshot"]["surface_flags"]["sidebar_visible"] is False
    assert payload["surface_branch"]["prep_actions"] == ["focus_sidebar"]
    assert [step["action"] for step in payload["execution_plan"]] == ["accessibility_invoke_element", "accessibility_invoke_element"]
    assert payload["execution_plan"][0]["args"]["query"] == "Sidebar"
    assert payload["execution_plan"][1]["args"]["query"] == "Bluetooth"


def test_desktop_action_router_preflights_main_content_before_context_menu_in_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2011, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2011, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_sidebar_only",
                "text": "Settings sidebar network bluetooth personalization",
                "screenshot_path": "E:/tmp/settings_sidebar_only.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "open_context_menu", "app_name": "settings"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_open_context_menu"
    assert payload["surface_snapshot"]["surface_flags"]["sidebar_visible"] is True
    assert payload["surface_snapshot"]["surface_flags"]["main_content_visible"] is False
    assert payload["surface_branch"]["prep_actions"] == ["focus_main_content"]
    assert [step["action"] for step in payload["execution_plan"]] == ["accessibility_invoke_element", "keyboard_hotkey"]
    assert payload["execution_plan"][0]["args"]["query"] == "Content"
    assert payload["execution_plan"][1]["args"]["keys"] == ["shift", "f10"]


def test_desktop_action_router_preflights_context_menu_item_for_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2014, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2014, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_sidebar_only",
                "text": "Settings sidebar network bluetooth personalization",
                "screenshot_path": "E:/tmp/settings_sidebar_only.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "select_context_menu_item", "app_name": "settings", "query": "Copy"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_select_context_menu_item"
    assert payload["surface_branch"]["prep_actions"] == ["focus_main_content", "open_context_menu"]
    assert [step["action"] for step in payload["execution_plan"]] == ["accessibility_invoke_element", "keyboard_hotkey", "accessibility_invoke_element"]
    assert payload["execution_plan"][0]["args"]["query"] == "Content"
    assert payload["execution_plan"][1]["args"]["keys"] == ["shift", "f10"]
    assert payload["execution_plan"][2]["args"]["query"] == "Copy"


def test_desktop_action_router_builds_dialog_control_workflows_for_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    dismiss_payload = router.advise({"action": "dismiss_dialog", "app_name": "settings"})
    confirm_payload = router.advise({"action": "confirm_dialog", "app_name": "settings"})

    assert dismiss_payload["status"] == "success"
    assert dismiss_payload["route_mode"] == "workflow_dismiss_dialog"
    assert dismiss_payload["workflow_profile"]["primary_hotkey"] == ["esc"]
    assert dismiss_payload["execution_plan"][-1]["action"] == "keyboard_hotkey"
    assert dismiss_payload["execution_plan"][-1]["args"]["keys"] == ["esc"]

    assert confirm_payload["status"] == "success"
    assert confirm_payload["route_mode"] == "workflow_confirm_dialog"
    assert confirm_payload["workflow_profile"]["primary_hotkey"] == ["enter"]
    assert confirm_payload["execution_plan"][-1]["action"] == "keyboard_hotkey"
    assert confirm_payload["execution_plan"][-1]["args"]["keys"] == ["enter"]


def test_desktop_action_router_uses_exact_safe_dialog_button_for_dismiss(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2101, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2101, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "dialog-1", "name": "Confirm changes", "control_type": "Pane"},
                    {"element_id": "btn-continue", "parent_id": "dialog-1", "name": "Continue", "control_type": "Button"},
                    {"element_id": "btn-cancel", "parent_id": "dialog-1", "name": "Cancel", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_dialog_confirm",
                "text": "Settings dialog continue cancel",
                "screenshot_path": "E:/tmp/settings_dialog_confirm.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "dismiss_dialog", "app_name": "settings"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_dismiss_dialog"
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert payload["execution_plan"][-1]["args"]["query"] == "Cancel"
    assert payload["execution_plan"][-1]["args"]["element_id"] == "btn-cancel"
    assert not any(step["action"] == "keyboard_hotkey" and step["args"].get("keys") == ["esc"] for step in payload["execution_plan"])


def test_desktop_action_router_uses_exact_confirmation_button_on_dangerous_surface(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2102, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2102, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "dialog-1", "name": "Ready to install", "control_type": "Pane"},
                    {"element_id": "btn-install", "parent_id": "dialog-1", "name": "Install", "control_type": "Button"},
                    {"element_id": "btn-cancel", "parent_id": "dialog-1", "name": "Cancel", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "wizard_ready_to_install",
                "text": "Setup Wizard ready to install. Warning: this action will make changes to your device and cannot be undone.",
                "screenshot_path": "E:/tmp/wizard_ready_to_install.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "confirm_dialog", "app_name": "installer"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_confirm_dialog"
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert payload["execution_plan"][-1]["args"]["query"] == "Install"
    assert payload["execution_plan"][-1]["args"]["element_id"] == "btn-install"
    assert payload["risk_level"] == "high"
    assert any("safer alternatives" in warning.lower() for warning in payload["warnings"])
    assert not any(step["action"] == "keyboard_hotkey" and step["args"].get("keys") == ["enter"] for step in payload["execution_plan"])


def test_desktop_action_router_builds_press_dialog_button_workflow_for_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "press_dialog_button", "app_name": "settings", "query": "Continue"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_press_dialog_button"
    assert payload["workflow_profile"]["supports_action_dispatch"] is True
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert payload["execution_plan"][-1]["args"]["query"] == "Continue"
    assert payload["execution_plan"][-1]["args"]["control_type"] == "Button"


def test_desktop_action_router_uses_exact_live_dialog_button_instance(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2427, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2427, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "dialog-1", "name": "Confirm changes", "control_type": "Pane"},
                    {"element_id": "btn-continue", "parent_id": "dialog-1", "name": "Continue", "control_type": "Button"},
                    {"element_id": "btn-cancel", "parent_id": "dialog-1", "name": "Cancel", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_dialog",
                "text": "Settings dialog continue cancel",
                "screenshot_path": "E:/tmp/settings_dialog.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "press_dialog_button", "app_name": "settings", "query": "Continue"})

    assert payload["status"] == "success"
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert payload["execution_plan"][-1]["args"]["query"] == "Continue"
    assert payload["execution_plan"][-1]["args"]["element_id"] == "btn-continue"


def test_desktop_action_router_builds_wizard_workflows_for_installer(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    next_payload = router.advise({"action": "next_wizard_step", "app_name": "installer"})
    back_payload = router.advise({"action": "previous_wizard_step", "app_name": "installer"})
    finish_payload = router.advise({"action": "finish_wizard", "app_name": "installer"})
    flow_payload = router.advise({"action": "complete_wizard_flow", "app_name": "installer"})

    assert next_payload["status"] == "success"
    assert next_payload["route_mode"] == "workflow_next_wizard_step"
    assert next_payload["workflow_profile"]["primary_hotkey"] == ["alt", "n"]
    assert next_payload["workflow_profile"]["supports_action_dispatch"] is True
    assert next_payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert next_payload["execution_plan"][-1]["args"]["query"] == "Next"

    assert back_payload["status"] == "success"
    assert back_payload["route_mode"] == "workflow_previous_wizard_step"
    assert back_payload["workflow_profile"]["primary_hotkey"] == ["alt", "b"]
    assert back_payload["execution_plan"][-1]["args"]["query"] == "Back"

    assert finish_payload["status"] == "success"
    assert finish_payload["route_mode"] == "workflow_finish_wizard"
    assert finish_payload["workflow_profile"]["primary_hotkey"] == ["alt", "f"]
    assert finish_payload["execution_plan"][-1]["args"]["query"] == "Finish"

    assert flow_payload["status"] == "success"
    assert flow_payload["route_mode"] == "workflow_complete_wizard_flow"
    assert flow_payload["workflow_profile"]["supports_stateful_execution"] is True


def test_desktop_action_router_completes_license_wizard_page_before_advancing(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 6101, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 6101, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "page-1", "name": "License Agreement", "control_type": "Pane"},
                    {"element_id": "check-accept", "parent_id": "page-1", "name": "I accept the license agreement", "control_type": "CheckBox", "checked": False},
                    {"element_id": "btn-next", "parent_id": "page-1", "name": "Next", "control_type": "Button"},
                    {"element_id": "btn-cancel", "parent_id": "page-1", "name": "Cancel", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "wizard_license_page",
                "text": "Setup Wizard license agreement. Please accept the terms before continuing.",
                "screenshot_path": "E:/tmp/wizard_license_page.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "complete_wizard_page", "app_name": "installer"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_complete_wizard_page"
    assert payload["surface_snapshot"]["wizard_page_state"]["page_kind"] == "license_agreement"
    assert payload["surface_snapshot"]["wizard_page_state"]["pending_requirement_count"] == 1
    assert payload["surface_snapshot"]["wizard_page_state"]["autonomous_progress_supported"] is True
    assert [step["action"] for step in payload["execution_plan"]] == ["accessibility_invoke_element", "accessibility_invoke_element"]
    assert payload["execution_plan"][0]["args"]["query"] == "I accept the license agreement"
    assert payload["execution_plan"][0]["args"]["element_id"] == "check-accept"
    assert payload["execution_plan"][1]["args"]["query"] == "Next"
    assert payload["execution_plan"][1]["args"]["element_id"] == "btn-next"


def test_desktop_action_router_completes_ready_to_install_page_with_exact_commit_target(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 6102, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 6102, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "page-2", "name": "Ready to Install", "control_type": "Pane"},
                    {"element_id": "btn-install", "parent_id": "page-2", "name": "Install", "control_type": "Button"},
                    {"element_id": "btn-cancel", "parent_id": "page-2", "name": "Cancel", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "wizard_ready_page",
                "text": "Setup Wizard ready to install. Warning: this action will make changes to your device and cannot be undone.",
                "screenshot_path": "E:/tmp/wizard_ready_page.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "complete_wizard_page", "app_name": "installer"})

    assert payload["status"] == "success"
    assert payload["surface_snapshot"]["wizard_page_state"]["page_kind"] == "ready_to_install"
    assert payload["surface_snapshot"]["wizard_page_state"]["autonomous_progress_supported"] is True
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert payload["execution_plan"][-1]["args"]["query"] == "Install"
    assert payload["execution_plan"][-1]["args"]["element_id"] == "btn-install"
    assert any("safer alternatives" in warning.lower() for warning in payload["warnings"])


def test_desktop_action_router_executes_complete_wizard_flow_until_setup_closes(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    state: Dict[str, Any] = {"page": 0, "accepted": False}

    def _elements(_payload: Dict[str, Any]) -> Dict[str, Any]:
        page = int(state.get("page", 0) or 0)
        if page == 0:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-1", "name": "License Agreement", "control_type": "Pane"},
                    {
                        "element_id": "check-accept",
                        "parent_id": "page-1",
                        "name": "I accept the license agreement",
                        "control_type": "CheckBox",
                        "checked": bool(state.get("accepted", False)),
                    },
                    {"element_id": "btn-next", "parent_id": "page-1", "name": "Next", "control_type": "Button"},
                    {"element_id": "btn-cancel", "parent_id": "page-1", "name": "Cancel", "control_type": "Button"},
                ],
            }
        if page == 1:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-2", "name": "Ready to Install", "control_type": "Pane"},
                    {"element_id": "btn-install", "parent_id": "page-2", "name": "Install", "control_type": "Button"},
                    {"element_id": "btn-cancel", "parent_id": "page-2", "name": "Cancel", "control_type": "Button"},
                ],
            }
        if page == 2:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-3", "name": "Installation Complete", "control_type": "Pane"},
                    {"element_id": "btn-finish", "parent_id": "page-3", "name": "Finish", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        page = int(state.get("page", 0) or 0)
        if page == 0:
            return {
                "status": "success",
                "screen_hash": "wizard_license_page",
                "text": "Setup Wizard license agreement. Please accept the terms before continuing.",
                "screenshot_path": "E:/tmp/wizard_license_page.png",
            }
        if page == 1:
            return {
                "status": "success",
                "screen_hash": "wizard_ready_page",
                "text": "Setup Wizard ready to install. Warning: this action will make changes to your device and cannot be undone.",
                "screenshot_path": "E:/tmp/wizard_ready_page.png",
            }
        if page == 2:
            return {
                "status": "success",
                "screen_hash": "wizard_complete_page",
                "text": "Setup Wizard installation complete. Click Finish to close setup.",
                "screenshot_path": "E:/tmp/wizard_complete_page.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_after_install",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_after_install.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        page = int(state.get("page", 0) or 0)
        if page == 0 and target in {"check-accept", "i accept the license agreement"}:
            state["accepted"] = True
            return {"status": "success", "invoked": target}
        if page == 0 and target in {"btn-next", "next"} and bool(state.get("accepted", False)):
            state["page"] = 1
            return {"status": "success", "invoked": target}
        if page == 1 and target in {"btn-install", "install"}:
            state["page"] = 2
            return {"status": "success", "invoked": target}
        if page == 2 and target in {"btn-finish", "finish"}:
            state["page"] = 3
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 7101, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}] if int(state.get("page", 0) or 0) < 3 else [],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 7101, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"} if int(state.get("page", 0) or 0) < 3 else {"hwnd": 9001, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"},
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 7101), "title": "Setup Wizard"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_wizard_flow", "app_name": "installer", "max_wizard_pages": 5})

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["wizard_mission"]["completed"] is True
    assert payload["wizard_mission"]["pages_completed"] == 3
    assert payload["wizard_mission"]["page_count"] == 3
    assert payload["wizard_mission"]["final_page"]["wizard_visible"] is False
    assert payload["wizard_mission"]["page_history"][0]["before"]["page_kind"] == "license_agreement"
    assert payload["wizard_mission"]["page_history"][1]["before"]["page_kind"] == "ready_to_install"
    assert payload["wizard_mission"]["page_history"][2]["before"]["page_kind"] == "completion"
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_complete_wizard_flow_adopts_child_window_after_page_transition(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    state: Dict[str, Any] = {
        "launcher_open": True,
        "child_open": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if bool(state["launcher_open"]):
            rows.append({"hwnd": 7105, "title": "Installer Launcher", "exe": r"C:\Installers\setup.exe"})
        if bool(state["child_open"]):
            rows.append({"hwnd": 7106, "title": "Nested Setup Wizard", "exe": r"C:\Installers\setup.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("window_title", "") or "").strip()
        if title == "Nested Setup Wizard" and bool(state["child_open"]):
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-child-complete", "name": "Installation Complete", "control_type": "Pane"},
                    {"element_id": "btn-finish", "parent_id": "page-child-complete", "name": "Finish", "control_type": "Button"},
                    {"element_id": "btn-cancel-child", "parent_id": "page-child-complete", "name": "Cancel", "control_type": "Button"},
                ],
            }
        if bool(state["launcher_open"]):
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-parent-welcome", "name": "Welcome", "control_type": "Pane"},
                    {"element_id": "btn-next-parent", "parent_id": "page-parent-welcome", "name": "Next", "control_type": "Button"},
                    {"element_id": "btn-cancel-parent", "parent_id": "page-parent-welcome", "name": "Cancel", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["child_open"]):
            return {
                "status": "success",
                "screen_hash": "wizard_child_complete_page",
                "text": "Setup Wizard installation complete. Click Finish to close setup.",
                "screenshot_path": "E:/tmp/wizard_child_complete_page.png",
            }
        if bool(state["launcher_open"]):
            return {
                "status": "success",
                "screen_hash": "wizard_launcher_page",
                "text": "Setup Wizard welcome page. Click Next to continue.",
                "screenshot_path": "E:/tmp/wizard_launcher_page.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_ready_after_wizard",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_ready_after_wizard.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"btn-next-parent", "next"} and bool(state["launcher_open"]) and not bool(state["child_open"]):
            state["child_open"] = True
            return {"status": "success", "invoked": target}
        if target in {"btn-finish", "finish"} and bool(state["child_open"]):
            state["launcher_open"] = False
            state["child_open"] = False
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 7106, "title": "Nested Setup Wizard", "exe": r"C:\Installers\setup.exe"}
                    if bool(state["child_open"])
                    else {"hwnd": 7105, "title": "Installer Launcher", "exe": r"C:\Installers\setup.exe"}
                    if bool(state["launcher_open"])
                    else {"hwnd": 9105, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"}
                ),
            },
            "focus_window": lambda payload: {
                "status": "success",
                "window": {
                    "hwnd": payload.get("hwnd", 7105),
                    "title": str(payload.get("window_title", "") or "Installer Launcher"),
                },
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_wizard_flow", "app_name": "installer", "max_wizard_pages": 4})

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["wizard_mission"]["completed"] is True
    assert payload["wizard_mission"]["pages_completed"] == 2
    assert payload["wizard_mission"]["page_history"][0]["after"]["window_title"] == "Nested Setup Wizard"
    assert payload["wizard_mission"]["page_history"][0]["after"]["window_adopted"] is True
    assert payload["wizard_mission"]["page_history"][1]["before"]["window_title"] == "Nested Setup Wizard"
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_complete_wizard_flow_resolves_benign_interstitial_dialog(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    state: Dict[str, Any] = {
        "page": 0,
        "dialog_open": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if int(state["page"]) < 2:
            rows.append({"hwnd": 7110, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"})
        if bool(state["dialog_open"]):
            rows.append({"hwnd": 7111, "title": "Component Check", "exe": r"C:\Installers\setup.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "items": [
                    {"element_id": "dialog-body", "name": "Component check completed", "control_type": "Pane"},
                    {"element_id": "btn-ok", "name": "OK", "control_type": "Button"},
                ],
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-welcome", "name": "Welcome", "control_type": "Pane"},
                    {"element_id": "btn-next", "parent_id": "page-welcome", "name": "Next", "control_type": "Button"},
                    {"element_id": "btn-cancel", "parent_id": "page-welcome", "name": "Cancel", "control_type": "Button"},
                ],
            }
        if int(state["page"]) == 1:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-complete", "name": "Installation Complete", "control_type": "Pane"},
                    {"element_id": "btn-finish", "parent_id": "page-complete", "name": "Finish", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "screen_hash": "wizard_notice_dialog",
                "text": "Component check dialog completed successfully. Click OK to continue.",
                "screenshot_path": "E:/tmp/wizard_notice_dialog.png",
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "screen_hash": "wizard_welcome_page",
                "text": "Setup Wizard welcome page. Click Next to continue.",
                "screenshot_path": "E:/tmp/wizard_welcome_page.png",
            }
        if int(state["page"]) == 1:
            return {
                "status": "success",
                "screen_hash": "wizard_completion_page",
                "text": "Setup Wizard installation complete. Click Finish to close setup.",
                "screenshot_path": "E:/tmp/wizard_completion_page.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_after_benign_dialog",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_after_benign_dialog.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if int(state["page"]) == 0 and target in {"btn-next", "next"}:
            state["dialog_open"] = True
            return {"status": "success", "invoked": target}
        if bool(state["dialog_open"]) and target in {"btn-ok", "ok"}:
            state["dialog_open"] = False
            state["page"] = 1
            return {"status": "success", "invoked": target}
        if int(state["page"]) == 1 and target in {"btn-finish", "finish"}:
            state["page"] = 2
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 7111, "title": "Component Check", "exe": r"C:\Installers\setup.exe"}
                    if bool(state["dialog_open"])
                    else {"hwnd": 7110, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}
                    if int(state["page"]) < 2
                    else {"hwnd": 9110, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"}
                ),
            },
            "focus_window": lambda payload: {
                "status": "success",
                "window": {"hwnd": payload.get("hwnd", 7110), "title": str(payload.get("window_title", "") or "Setup Wizard")},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_wizard_flow", "app_name": "installer", "max_wizard_pages": 5})

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["wizard_mission"]["completed"] is True
    assert payload["wizard_mission"]["pages_completed"] == 3
    assert payload["wizard_mission"]["page_history"][1]["status"] == "success"
    assert payload["wizard_mission"]["page_history"][1]["before"]["screen_hash"] == "wizard_notice_dialog"
    assert payload["wizard_mission"]["page_history"][1]["before"]["preferred_confirmation_button"] == "OK"
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_stops_complete_wizard_flow_on_risky_interstitial_dialog(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    state: Dict[str, Any] = {
        "page": 0,
        "dialog_open": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if int(state["page"]) == 0:
            rows.append({"hwnd": 7120, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"})
        if bool(state["dialog_open"]):
            rows.append({"hwnd": 7121, "title": "Review Required", "exe": r"C:\Installers\setup.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "items": [
                    {"element_id": "dialog-warning", "name": "Ready to continue", "control_type": "Pane"},
                    {"element_id": "btn-continue", "name": "Continue", "control_type": "Button"},
                    {"element_id": "btn-cancel", "name": "Cancel", "control_type": "Button"},
                ],
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-ready", "name": "Ready", "control_type": "Pane"},
                    {"element_id": "btn-next", "parent_id": "page-ready", "name": "Next", "control_type": "Button"},
                    {"element_id": "btn-cancel-main", "parent_id": "page-ready", "name": "Cancel", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "screen_hash": "wizard_warning_dialog",
                "text": "Warning dialog: continuing will skip the validation review step. Continue or Cancel.",
                "screenshot_path": "E:/tmp/wizard_warning_dialog.png",
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "screen_hash": "wizard_ready_root",
                "text": "Setup Wizard ready page. Click Next to continue.",
                "screenshot_path": "E:/tmp/wizard_ready_root.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_after_warning",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_after_warning.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if int(state["page"]) == 0 and target in {"btn-next", "next"}:
            state["dialog_open"] = True
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 7121, "title": "Review Required", "exe": r"C:\Installers\setup.exe"}
                    if bool(state["dialog_open"])
                    else {"hwnd": 7120, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}
                ),
            },
            "focus_window": lambda payload: {
                "status": "success",
                "window": {"hwnd": payload.get("hwnd", 7120), "title": str(payload.get("window_title", "") or "Setup Wizard")},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_wizard_flow", "app_name": "installer", "max_wizard_pages": 4})

    assert payload["status"] == "partial"
    assert payload["verification"]["verified"] is False
    assert payload["wizard_mission"]["completed"] is False
    assert payload["wizard_mission"]["stop_reason_code"] == "warning_confirmation_requires_review"
    assert payload["wizard_mission"]["page_history"][1]["status"] == "blocked"
    assert payload["wizard_mission"]["page_history"][1]["before"]["screen_hash"] == "wizard_warning_dialog"
    assert payload["wizard_mission"]["page_history"][1]["before"]["preferred_confirmation_button"] == "Continue"
    assert payload["wizard_mission"]["final_page"]["screen_hash"] == "wizard_warning_dialog"
    assert [row["action"] for row in payload["results"]] == ["accessibility_invoke_element"]


def test_desktop_action_router_stops_complete_wizard_flow_on_credential_interstitial_dialog(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    state: Dict[str, Any] = {
        "page": 0,
        "dialog_open": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if int(state["page"]) == 0:
            rows.append({"hwnd": 7126, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"})
        if bool(state["dialog_open"]):
            rows.append({"hwnd": 7127, "title": "Windows Security", "exe": r"C:\Windows\System32\CredentialUIBroker.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("window_title", "") or "").strip()
        if bool(state["dialog_open"]) and title != "Setup Wizard":
            return {
                "status": "success",
                "items": [
                    {"element_id": "field-username", "name": "Username", "control_type": "Edit", "value_text": ""},
                    {"element_id": "field-password", "name": "Password", "control_type": "Edit", "value_text": ""},
                    {"element_id": "btn-sign-in", "name": "Sign in", "control_type": "Button"},
                    {"element_id": "btn-cancel", "name": "Cancel", "control_type": "Button"},
                ],
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-ready", "name": "Ready", "control_type": "Pane"},
                    {"element_id": "btn-next", "parent_id": "page-ready", "name": "Next", "control_type": "Button"},
                    {"element_id": "btn-cancel-main", "parent_id": "page-ready", "name": "Cancel", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "screen_hash": "wizard_credential_dialog",
                "text": "Windows Security sign in dialog. Enter username and password to continue the setup.",
                "screenshot_path": "E:/tmp/wizard_credential_dialog.png",
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "screen_hash": "wizard_ready_for_auth",
                "text": "Setup Wizard ready page. Click Next to continue.",
                "screenshot_path": "E:/tmp/wizard_ready_for_auth.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_after_auth_dialog",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_after_auth_dialog.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if int(state["page"]) == 0 and target in {"btn-next", "next"}:
            state["dialog_open"] = True
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 7127, "title": "Windows Security", "exe": r"C:\Windows\System32\CredentialUIBroker.exe"}
                    if bool(state["dialog_open"])
                    else {"hwnd": 7126, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}
                ),
            },
            "focus_window": lambda payload: {
                "status": "success",
                "window": {"hwnd": payload.get("hwnd", 7126), "title": str(payload.get("window_title", "") or "Setup Wizard")},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_wizard_flow", "app_name": "installer", "max_wizard_pages": 4})

    assert payload["status"] == "partial"
    assert payload["verification"]["verified"] is False
    assert payload["wizard_mission"]["completed"] is False
    assert payload["wizard_mission"]["stop_reason_code"] == "credential_input_required"
    assert payload["wizard_mission"]["page_history"][1]["status"] == "blocked"
    assert payload["wizard_mission"]["page_history"][0]["after"]["dialog_kind"] == "credential_prompt"
    assert payload["wizard_mission"]["page_history"][0]["after"]["approval_kind"] == "credential_input"
    assert payload["wizard_mission"]["page_history"][1]["before"]["window_title"] == "Windows Security"
    assert payload["wizard_mission"]["page_history"][1]["before"]["dialog_kind"] == "credential_prompt"
    assert payload["wizard_mission"]["page_history"][1]["before"]["approval_kind"] == "credential_input"
    assert payload["wizard_mission"]["page_history"][1]["blocking_surface"]["approval_kind"] == "credential_input"
    assert payload["wizard_mission"]["blocking_surface"]["approval_kind"] == "credential_input"
    assert payload["wizard_mission"]["blocking_surface"]["resume_action"] == "complete_wizard_flow"
    assert "provide_credentials" in payload["wizard_mission"]["blocking_surface"]["resume_preconditions"]
    assert payload["wizard_mission"]["resume_contract"]["resume_action"] == "complete_wizard_flow"
    assert payload["wizard_mission"]["resume_contract"]["resume_strategy"] == "reacquire_app_surface"
    assert payload["wizard_mission"]["resume_contract"]["resume_payload"]["app_name"] == "installer"
    assert payload["wizard_mission"]["resume_contract"]["resume_payload"]["window_title"] == ""
    assert "provide_credentials" in payload["wizard_mission"]["resume_contract"]["resume_preconditions"]
    assert payload["wizard_mission"]["final_page"]["screen_hash"] == "wizard_credential_dialog"
    assert [row["action"] for row in payload["results"]] == ["accessibility_invoke_element"]


def test_desktop_action_router_stops_complete_wizard_flow_on_uac_consent_interstitial_dialog(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    state: Dict[str, Any] = {
        "page": 0,
        "dialog_open": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if int(state["page"]) == 0:
            rows.append({"hwnd": 7128, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"})
        if bool(state["dialog_open"]):
            rows.append({"hwnd": 7129, "title": "User Account Control", "exe": r"C:\Windows\System32\consent.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("window_title", "") or "").strip()
        if bool(state["dialog_open"]) and title != "Setup Wizard":
            return {
                "status": "success",
                "items": [
                    {"element_id": "btn-yes", "name": "Yes", "control_type": "Button"},
                    {"element_id": "btn-no", "name": "No", "control_type": "Button"},
                ],
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-ready", "name": "Ready", "control_type": "Pane"},
                    {"element_id": "btn-next", "parent_id": "page-ready", "name": "Next", "control_type": "Button"},
                    {"element_id": "btn-cancel-main", "parent_id": "page-ready", "name": "Cancel", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "screen_hash": "wizard_uac_dialog",
                "text": "User Account Control. Do you want to allow this app to make changes to your device?",
                "screenshot_path": "E:/tmp/wizard_uac_dialog.png",
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "screen_hash": "wizard_ready_for_uac",
                "text": "Setup Wizard ready page. Click Next to continue.",
                "screenshot_path": "E:/tmp/wizard_ready_for_uac.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_after_uac_dialog",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_after_uac_dialog.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if int(state["page"]) == 0 and target in {"btn-next", "next"}:
            state["dialog_open"] = True
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 7129, "title": "User Account Control", "exe": r"C:\Windows\System32\consent.exe"}
                    if bool(state["dialog_open"])
                    else {"hwnd": 7128, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}
                ),
            },
            "focus_window": lambda payload: {
                "status": "success",
                "window": {"hwnd": payload.get("hwnd", 7128), "title": str(payload.get("window_title", "") or "Setup Wizard")},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_wizard_flow", "app_name": "installer", "max_wizard_pages": 4})

    assert payload["status"] == "partial"
    assert payload["verification"]["verified"] is False
    assert payload["wizard_mission"]["completed"] is False
    assert payload["wizard_mission"]["stop_reason_code"] == "elevation_consent_required"
    assert payload["wizard_mission"]["page_history"][1]["status"] == "blocked"
    assert payload["wizard_mission"]["page_history"][1]["before"]["window_title"] == "User Account Control"
    assert payload["wizard_mission"]["page_history"][1]["before"]["dialog_kind"] == "elevation_prompt"
    assert payload["wizard_mission"]["page_history"][1]["before"]["approval_kind"] == "elevation_consent"
    assert payload["wizard_mission"]["page_history"][1]["blocking_surface"]["approval_kind"] == "elevation_consent"
    assert payload["wizard_mission"]["page_history"][1]["dialog_followup"]["dialog_kind"] == "elevation_prompt"
    assert payload["wizard_mission"]["page_history"][1]["dialog_followup"]["approval_kind"] == "elevation_consent"
    assert payload["wizard_mission"]["page_history"][1]["dialog_followup"]["secure_desktop_likely"] is True
    assert payload["wizard_mission"]["blocking_surface"]["approval_kind"] == "elevation_consent"
    assert payload["wizard_mission"]["blocking_surface"]["secure_desktop_likely"] is True
    assert "approve_elevation_request" in payload["wizard_mission"]["blocking_surface"]["resume_preconditions"]
    assert payload["wizard_mission"]["resume_contract"]["resume_action"] == "complete_wizard_flow"
    assert payload["wizard_mission"]["resume_contract"]["resume_strategy"] == "reacquire_app_surface"
    assert payload["wizard_mission"]["resume_contract"]["resume_payload"]["app_name"] == "installer"
    assert "approve_elevation_request" in payload["wizard_mission"]["resume_contract"]["resume_preconditions"]
    assert payload["wizard_mission"]["final_page"]["screen_hash"] == "wizard_uac_dialog"
    assert [row["action"] for row in payload["results"]] == ["accessibility_invoke_element"]


def test_desktop_action_router_resume_mission_stays_blocked_while_uac_surface_is_active(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    state: Dict[str, Any] = {
        "page": 0,
        "dialog_open": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if int(state["page"]) in {0, 1} and not bool(state["dialog_open"]):
            rows.append({"hwnd": 8128, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"})
        if bool(state["dialog_open"]):
            rows.append({"hwnd": 8129, "title": "User Account Control", "exe": r"C:\Windows\System32\consent.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("window_title", "") or "").strip()
        if bool(state["dialog_open"]) and title != "Setup Wizard":
            return {
                "status": "success",
                "items": [
                    {"element_id": "btn-yes", "name": "Yes", "control_type": "Button"},
                    {"element_id": "btn-no", "name": "No", "control_type": "Button"},
                ],
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-ready", "name": "Ready", "control_type": "Pane"},
                    {"element_id": "btn-next", "parent_id": "page-ready", "name": "Next", "control_type": "Button"},
                ],
            }
        if int(state["page"]) == 1:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-complete", "name": "Complete", "control_type": "Pane"},
                    {"element_id": "btn-finish", "parent_id": "page-complete", "name": "Finish", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "screen_hash": "resume_blocked_uac",
                "text": "User Account Control. Do you want to allow this app to make changes to your device?",
                "screenshot_path": "E:/tmp/resume_blocked_uac.png",
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "screen_hash": "resume_wizard_ready",
                "text": "Setup Wizard ready page. Click Next to continue.",
                "screenshot_path": "E:/tmp/resume_wizard_ready.png",
            }
        if int(state["page"]) == 1:
            return {
                "status": "success",
                "screen_hash": "resume_wizard_complete",
                "text": "Setup Wizard complete. Click Finish to exit.",
                "screenshot_path": "E:/tmp/resume_wizard_complete.png",
            }
        return {
            "status": "success",
            "screen_hash": "resume_desktop_after",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/resume_desktop_after.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if int(state["page"]) == 0 and target in {"btn-next", "next"}:
            state["dialog_open"] = True
            return {"status": "success", "invoked": target}
        if int(state["page"]) == 1 and target in {"btn-finish", "finish"}:
            state["page"] = 2
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 8129, "title": "User Account Control", "exe": r"C:\Windows\System32\consent.exe"}
                    if bool(state["dialog_open"])
                    else {"hwnd": 8128, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}
                ),
            },
            "focus_window": lambda payload: {
                "status": "success",
                "window": {"hwnd": payload.get("hwnd", 8128), "title": str(payload.get("title", "") or "Setup Wizard")},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    blocked = router.execute({"action": "complete_wizard_flow", "app_name": "installer", "max_wizard_pages": 4})
    mission_id = str(blocked.get("mission_record", {}).get("mission_id", "") or "")
    advice = router.advise(
        {
            "action": "resume_mission",
            "mission_id": mission_id,
        }
    )

    assert blocked["wizard_mission"]["stop_reason_code"] == "elevation_consent_required"
    assert mission_id
    assert blocked["mission_record"]["status"] == "paused"
    assert blocked["wizard_mission"]["resume_contract"]["mission_id"] == mission_id
    assert blocked["wizard_mission"]["blocking_surface"]["mission_id"] == mission_id
    assert advice["status"] == "blocked"
    assert advice["action"] == "resume_mission"
    assert advice["route_mode"] == "resume_desktop_mission"
    assert advice["resume_action"] == "complete_wizard_flow"
    assert advice["mission_record"]["mission_id"] == mission_id
    assert advice["resume_context"]["status"] == "blocked"
    assert advice["resume_context"]["blocking_surface_still_visible"] is True
    assert advice["resume_context"]["current_approval_kind"] == "elevation_consent"
    assert advice["blocking_surface"]["approval_kind"] == "elevation_consent"


def test_desktop_action_router_resume_mission_context_uses_related_window_reacquisition_cluster() -> None:
    router = _build_router({})
    router.surface_snapshot = lambda **_kwargs: {  # type: ignore[method-assign]
        "status": "success",
        "target_window": {},
        "active_window": {},
        "surface_flags": {},
        "safety_signals": {},
        "observation": {"screen_hash": "resume_related_cluster"},
        "native_window_topology": {
            "topology_signature": "settings|3|2",
            "same_process_window_count": 3,
            "related_window_count": 2,
            "child_dialog_like_visible": True,
        },
        "window_reacquisition": {
            "candidate": {
                "hwnd": 1440,
                "title": "Bluetooth & devices",
                "match_score": 0.67,
            },
            "same_process_window_count": 3,
            "related_window_count": 2,
            "child_dialog_like_visible": True,
        },
    }

    context = router._resume_mission_context(
        args={},
        resume_contract={
            "mission_kind": "form",
            "resume_action": "complete_form_flow",
            "surface_match_hints": {
                "anchor_app_name": "settings",
                "anchor_window_title": "Device settings",
            },
        },
        blocking_surface={},
        resume_payload={
            "action": "complete_form_flow",
            "app_name": "settings",
        },
    )

    assert context["status"] == "ready"
    assert context["window_reacquired"] is True
    assert context["reacquired_candidate_hwnd"] == 1440
    assert context["reacquired_candidate_title"] == "Bluetooth & devices"
    assert float(context["reacquisition_match_score"]) == pytest.approx(0.67)
    assert context["native_same_process_window_count"] == 3
    assert context["native_related_window_count"] == 2
    assert context["native_child_dialog_like_visible"] is True


def test_desktop_action_router_execute_resume_mission_completes_wizard_after_uac_clears(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    state: Dict[str, Any] = {
        "page": 0,
        "dialog_open": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if int(state["page"]) in {0, 1} and not bool(state["dialog_open"]):
            rows.append({"hwnd": 9228, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"})
        if bool(state["dialog_open"]):
            rows.append({"hwnd": 9229, "title": "User Account Control", "exe": r"C:\Windows\System32\consent.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("window_title", "") or "").strip()
        if bool(state["dialog_open"]) and title != "Setup Wizard":
            return {
                "status": "success",
                "items": [
                    {"element_id": "btn-yes", "name": "Yes", "control_type": "Button"},
                    {"element_id": "btn-no", "name": "No", "control_type": "Button"},
                ],
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-ready", "name": "Ready", "control_type": "Pane"},
                    {"element_id": "btn-next", "parent_id": "page-ready", "name": "Next", "control_type": "Button"},
                ],
            }
        if int(state["page"]) == 1:
            return {
                "status": "success",
                "items": [
                    {"element_id": "page-complete", "name": "Complete", "control_type": "Pane"},
                    {"element_id": "btn-finish", "parent_id": "page-complete", "name": "Finish", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "screen_hash": "resume_execute_uac",
                "text": "User Account Control. Do you want to allow this app to make changes to your device?",
                "screenshot_path": "E:/tmp/resume_execute_uac.png",
            }
        if int(state["page"]) == 0:
            return {
                "status": "success",
                "screen_hash": "resume_execute_ready",
                "text": "Setup Wizard ready page. Click Next to continue.",
                "screenshot_path": "E:/tmp/resume_execute_ready.png",
            }
        if int(state["page"]) == 1:
            return {
                "status": "success",
                "screen_hash": "resume_execute_complete",
                "text": "Setup Wizard complete. Click Finish to exit.",
                "screenshot_path": "E:/tmp/resume_execute_complete.png",
            }
        return {
            "status": "success",
            "screen_hash": "resume_execute_desktop",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/resume_execute_desktop.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if int(state["page"]) == 0 and target in {"btn-next", "next"}:
            state["dialog_open"] = True
            return {"status": "success", "invoked": target}
        if int(state["page"]) == 1 and target in {"btn-finish", "finish"}:
            state["page"] = 2
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 9229, "title": "User Account Control", "exe": r"C:\Windows\System32\consent.exe"}
                    if bool(state["dialog_open"])
                    else (
                        {"hwnd": 9228, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}
                        if int(state["page"]) in {0, 1}
                        else {"hwnd": 9901, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"}
                    )
                ),
            },
            "focus_window": lambda payload: {
                "status": "success",
                "window": {"hwnd": payload.get("hwnd", 9228), "title": str(payload.get("title", "") or "Setup Wizard")},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    blocked = router.execute({"action": "complete_wizard_flow", "app_name": "installer", "max_wizard_pages": 4})
    mission_id = str(blocked.get("mission_record", {}).get("mission_id", "") or "")
    state["dialog_open"] = False
    state["page"] = 1

    resumed = router.execute(
        {
            "action": "resume_mission",
            "mission_id": mission_id,
        }
    )

    assert blocked["wizard_mission"]["stop_reason_code"] == "elevation_consent_required"
    assert mission_id
    assert resumed["status"] == "success"
    assert resumed["action"] == "resume_mission"
    assert resumed["resume_action"] == "complete_wizard_flow"
    assert resumed["final_action"] == "complete_wizard_flow"
    assert resumed["mission_record"]["mission_id"] == mission_id
    assert resumed["mission_record"]["status"] == "completed"
    assert int(resumed["mission_record"]["resume_attempts"] or 0) >= 1
    assert resumed["resume_context"]["status"] == "resumed"
    assert resumed["resume_context"]["blocking_surface_still_visible"] is False
    assert resumed["wizard_mission"]["completed"] is True
    assert resumed["wizard_mission"]["stop_reason_code"] == ""
    assert [row["action"] for row in resumed["results"]] == ["accessibility_invoke_element"]


def test_desktop_action_router_stops_complete_wizard_flow_when_manual_input_is_required(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 7201, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 7201, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "page-options", "name": "Custom Options", "control_type": "Pane"},
                    {"element_id": "edit-path", "parent_id": "page-options", "name": "Installation path", "control_type": "Edit", "value_text": ""},
                    {"element_id": "btn-back", "parent_id": "page-options", "name": "Back", "control_type": "Button"},
                    {"element_id": "btn-cancel", "parent_id": "page-options", "name": "Cancel", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "wizard_options_page",
                "text": "Setup Wizard custom options. Choose an installation path before continuing.",
                "screenshot_path": "E:/tmp/wizard_options_page.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_wizard_flow", "app_name": "installer", "max_wizard_pages": 4})

    assert payload["status"] == "partial"
    assert payload["verification"]["verified"] is False
    assert payload["wizard_mission"]["completed"] is False
    assert payload["wizard_mission"]["stop_reason_code"] == "manual_input_required"
    assert payload["wizard_mission"]["page_history"][0]["status"] == "blocked"
    assert payload["wizard_mission"]["page_history"][0]["before"]["manual_input_likely"] is True
    assert payload["wizard_mission"]["final_page"]["autonomous_blocker"] == "manual_input_required"


def test_desktop_action_router_surface_snapshot_detects_wizard_safety_signals(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 4412, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 4412, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"name": "Back", "control_type": "Button"},
                    {"name": "Next", "control_type": "Button"},
                    {"name": "Finish", "control_type": "Button"},
                    {"name": "Cancel", "control_type": "Button"},
                ],
            },
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "wizard_warning_hash",
                "text": "Setup Wizard ready to install. Warning: this action will make changes to your device and cannot be undone.",
                "screenshot_path": "E:/tmp/setup_wizard_warning.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="installer", query="finish", limit=10)

    assert payload["status"] == "success"
    assert payload["app_profile"]["category"] == "utility"
    assert payload["safety_signals"]["wizard_surface_visible"] is True
    assert payload["safety_signals"]["wizard_next_available"] is True
    assert payload["safety_signals"]["wizard_back_available"] is True
    assert payload["safety_signals"]["wizard_finish_available"] is True
    assert payload["safety_signals"]["warning_surface_visible"] is True
    assert payload["safety_signals"]["destructive_warning_visible"] is True
    assert payload["safety_signals"]["elevation_prompt_visible"] is False
    assert payload["safety_signals"]["requires_confirmation"] is True
    assert payload["surface_flags"]["wizard_surface_visible"] is True
    assert payload["surface_flags"]["wizard_finish_available"] is True
    assert "Next" in payload["safety_signals"]["dialog_buttons"]
    assert "Cancel" in payload["safety_signals"]["safe_dialog_buttons"]
    assert "Finish" in payload["safety_signals"]["destructive_dialog_buttons"]
    assert payload["safety_signals"]["preferred_dismiss_button"] == "Cancel"
    assert payload["safety_signals"]["preferred_confirmation_button"] == "Next"
    assert payload["dialog_state"]["dialog_kind"] == "destructive_confirmation"
    assert payload["dialog_state"]["approval_kind"] == "destructive_confirmation"
    assert payload["dialog_state"]["review_required"] is True
    assert payload["target_group_state"]["group_role"] == "wizard_actions"
    assert "Cancel" in payload["target_group_state"]["safe_options"]
    assert payload["wizard_page_state"]["page_kind"] == "ready_to_install"
    assert payload["wizard_page_state"]["dialog_kind"] == "destructive_confirmation"
    assert payload["wizard_page_state"]["approval_kind"] == "destructive_confirmation"
    assert payload["wizard_page_state"]["advance_action"] == "next_wizard_step"
    assert payload["wizard_page_state"]["preferred_confirmation_button"] == "Next"
    assert payload["wizard_page_state"]["autonomous_progress_supported"] is True
    assert "next_wizard_step" in payload["recommended_actions"]
    assert "complete_wizard_flow" in payload["recommended_actions"]
    assert "complete_wizard_page" in payload["recommended_actions"]
    assert "dismiss_dialog" in payload["recommended_actions"]


def test_desktop_action_router_escalates_finish_wizard_risk_when_surface_is_dangerous(tmp_path: Path) -> None:
    registry = _build_registry(tmp_path, [])
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 5511, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 5511, "title": "Setup Wizard", "exe": r"C:\Installers\setup.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"name": "Finish", "control_type": "Button"},
                    {"name": "Cancel", "control_type": "Button"},
                ],
            },
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "wizard_finish_hash",
                "text": "Setup Wizard warning: this action will make changes to your device and cannot be undone.",
                "screenshot_path": "E:/tmp/setup_wizard_finish.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "finish_wizard", "app_name": "installer"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_finish_wizard"
    assert payload["risk_level"] == "high"
    assert payload["safety_signals"]["destructive_warning_visible"] is True
    assert payload["safety_signals"]["elevation_prompt_visible"] is False
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert any("destructive" in warning.lower() for warning in payload["warnings"])
    assert any("confirmation path" in warning.lower() for warning in payload["warnings"])


def test_desktop_action_router_surface_snapshot_detects_generic_settings_surfaces(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2012, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2012, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {"status": "success", "items": [{"name": "Sidebar", "control_type": "Pane"}]},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_surface",
                "text": "Settings sidebar content toolbar dialog ok cancel",
                "screenshot_path": "E:/tmp/settings_surface.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="settings", query="sidebar", limit=10)

    assert payload["status"] == "success"
    assert payload["app_profile"]["category"] == "utility"
    assert payload["surface_flags"]["sidebar_visible"] is True
    assert payload["surface_flags"]["main_content_visible"] is True
    assert payload["surface_flags"]["toolbar_visible"] is True
    assert payload["surface_flags"]["dialog_visible"] is True
    assert "focus_main_content" in payload["recommended_actions"]


def test_desktop_action_router_surface_snapshot_detects_form_page_state(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2310, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2310, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "check-review", "name": "I understand the pending changes", "control_type": "CheckBox", "checked": False},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                    {"element_id": "btn-cancel", "name": "Cancel", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_review_page",
                "text": "Settings review changes. I understand the pending changes before applying.",
                "screenshot_path": "E:/tmp/settings_review_page.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="settings", query="", limit=10)

    assert payload["status"] == "success"
    assert payload["form_page_state"]["page_kind"] == "review_confirmation"
    assert payload["form_page_state"]["pending_requirement_count"] == 1
    assert payload["form_page_state"]["preferred_commit_button"] == "Apply"
    assert payload["form_page_state"]["autonomous_progress_supported"] is True
    assert "complete_form_page" in payload["recommended_actions"]
    assert "complete_form_flow" in payload["recommended_actions"]


def test_desktop_action_router_surface_snapshot_detects_credential_dialog_state(tmp_path: Path) -> None:
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2314, "title": "Windows Security", "exe": r"C:\Windows\System32\CredentialUIBroker.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2314, "title": "Windows Security", "exe": r"C:\Windows\System32\CredentialUIBroker.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "field-username", "name": "Username", "control_type": "Edit", "value_text": ""},
                    {"element_id": "field-password", "name": "Password", "control_type": "Edit", "value_text": ""},
                    {"element_id": "btn-sign-in", "name": "Sign in", "control_type": "Button"},
                    {"element_id": "btn-cancel", "name": "Cancel", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "credential_dialog_surface",
                "text": "Windows Security sign in dialog. Enter username and password to continue.",
                "screenshot_path": "E:/tmp/credential_dialog_surface.png",
            },
        },
        app_profile_registry=_build_registry(tmp_path, []),
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(window_title="Windows Security", query="sign in", limit=10)

    assert payload["status"] == "success"
    assert payload["surface_flags"]["dialog_visible"] is True
    assert payload["safety_signals"]["credential_prompt_visible"] is True
    assert payload["dialog_state"]["dialog_kind"] == "credential_prompt"
    assert payload["dialog_state"]["manual_input_required"] is True
    assert payload["dialog_state"]["credential_field_count"] == 2
    assert payload["form_page_state"]["page_kind"] == "credential_dialog"
    assert payload["form_page_state"]["autonomous_blocker"] == "credential_input_required"
    assert "focus_input_field" in payload["recommended_actions"]
    assert "set_field_value" in payload["recommended_actions"]
    assert "dismiss_dialog" in payload["recommended_actions"]


def test_desktop_action_router_surface_snapshot_detects_uac_consent_dialog_state(tmp_path: Path) -> None:
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2315, "title": "User Account Control", "exe": r"C:\Windows\System32\consent.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2315, "title": "User Account Control", "exe": r"C:\Windows\System32\consent.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "btn-yes", "name": "Yes", "control_type": "Button"},
                    {"element_id": "btn-no", "name": "No", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "uac_consent_dialog_surface",
                "text": "User Account Control. Do you want to allow this app to make changes to your device?",
                "screenshot_path": "E:/tmp/uac_consent_dialog_surface.png",
            },
        },
        app_profile_registry=_build_registry(tmp_path, []),
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(window_title="User Account Control", query="allow", limit=10)

    assert payload["status"] == "success"
    assert payload["surface_flags"]["dialog_visible"] is True
    assert payload["safety_signals"]["elevation_prompt_visible"] is True
    assert payload["safety_signals"]["admin_approval_required"] is True
    assert payload["safety_signals"]["secure_desktop_likely"] is True
    assert payload["dialog_state"]["dialog_kind"] == "elevation_prompt"
    assert payload["dialog_state"]["approval_kind"] == "elevation_consent"
    assert payload["dialog_state"]["secure_desktop_likely"] is True
    assert payload["form_page_state"]["page_kind"] == "elevation_dialog"
    assert payload["form_page_state"]["approval_kind"] == "elevation_consent"
    assert payload["form_page_state"]["autonomous_blocker"] == "elevation_consent_required"
    assert "dismiss_dialog" in payload["recommended_actions"]


def test_desktop_action_router_surface_snapshot_detects_permission_review_dialog_state(tmp_path: Path) -> None:
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2316, "title": "Camera access", "exe": r"C:\Windows\System32\ApplicationFrameHost.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2316, "title": "Camera access", "exe": r"C:\Windows\System32\ApplicationFrameHost.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "btn-allow", "name": "Allow", "control_type": "Button"},
                    {"element_id": "btn-dont-allow", "name": "Don't Allow", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "permission_review_dialog_surface",
                "text": "Let this app access your camera? Allow or Don't Allow.",
                "screenshot_path": "E:/tmp/permission_review_dialog_surface.png",
            },
        },
        app_profile_registry=_build_registry(tmp_path, []),
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(window_title="Camera access", query="allow", limit=10)

    assert payload["status"] == "success"
    assert payload["safety_signals"]["permission_review_visible"] is True
    assert payload["dialog_state"]["dialog_kind"] == "permission_review"
    assert payload["dialog_state"]["approval_kind"] == "permission_review"
    assert payload["form_page_state"]["page_kind"] == "permission_dialog"
    assert payload["form_page_state"]["autonomous_blocker"] == "permission_review_required"
    assert "dismiss_dialog" in payload["recommended_actions"]


def test_desktop_action_router_completes_form_page_before_committing_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2311, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2311, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "check-review", "name": "I understand the pending changes", "control_type": "CheckBox", "checked": False},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                    {"element_id": "btn-cancel", "name": "Cancel", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_review_page",
                "text": "Settings review changes. I understand the pending changes before applying.",
                "screenshot_path": "E:/tmp/settings_review_page.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "complete_form_page", "app_name": "settings"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_complete_form_page"
    assert payload["surface_snapshot"]["form_page_state"]["pending_requirement_count"] == 1
    assert payload["surface_snapshot"]["form_page_state"]["preferred_commit_button"] == "Apply"
    assert [step["action"] for step in payload["execution_plan"][-2:]] == ["accessibility_invoke_element", "accessibility_invoke_element"]
    assert payload["execution_plan"][-2]["args"]["query"] == "I understand the pending changes"
    assert payload["execution_plan"][-2]["args"]["element_id"] == "check-review"
    assert payload["execution_plan"][-1]["args"]["query"] == "Apply"
    assert payload["execution_plan"][-1]["args"]["element_id"] == "btn-apply"


def test_desktop_action_router_executes_complete_form_flow_until_settings_surface_closes(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    state: Dict[str, Any] = {"page": 0, "acknowledged": False}

    def _elements(_payload: Dict[str, Any]) -> Dict[str, Any]:
        page = int(state.get("page", 0) or 0)
        if page == 0:
            return {
                "status": "success",
                "items": [
                    {"element_id": "check-review", "name": "I understand the pending changes", "control_type": "CheckBox", "checked": bool(state.get("acknowledged", False))},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                    {"element_id": "btn-cancel", "name": "Cancel", "control_type": "Button"},
                ],
            }
        if page == 1:
            return {
                "status": "success",
                "items": [
                    {"element_id": "dialog-complete", "name": "Settings saved", "control_type": "Pane"},
                    {"element_id": "btn-ok", "name": "OK", "control_type": "Button"},
                    {"element_id": "btn-cancel", "name": "Cancel", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        page = int(state.get("page", 0) or 0)
        if page == 0:
            return {
                "status": "success",
                "screen_hash": "settings_review_page",
                "text": "Settings review changes. I understand the pending changes before applying.",
                "screenshot_path": "E:/tmp/settings_review_page.png",
            }
        if page == 1:
            return {
                "status": "success",
                "screen_hash": "settings_saved_dialog",
                "text": "Settings saved successfully. Click OK to close this dialog.",
                "screenshot_path": "E:/tmp/settings_saved_dialog.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_after_settings",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_after_settings.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        page = int(state.get("page", 0) or 0)
        if page == 0 and target in {"check-review", "i understand the pending changes"}:
            state["acknowledged"] = True
            return {"status": "success", "invoked": target}
        if page == 0 and target in {"btn-apply", "apply"} and bool(state.get("acknowledged", False)):
            state["page"] = 1
            return {"status": "success", "invoked": target}
        if page == 1 and target in {"btn-ok", "ok"}:
            state["page"] = 2
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2312, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}] if int(state.get("page", 0) or 0) < 2 else [],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2312, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"} if int(state.get("page", 0) or 0) < 2 else {"hwnd": 9001, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"},
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2312), "title": "Settings"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_form_flow", "app_name": "settings", "max_form_pages": 4})

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["form_mission"]["completed"] is True
    assert payload["form_mission"]["pages_completed"] == 2
    assert payload["form_mission"]["page_count"] == 2
    assert payload["form_mission"]["final_page"]["form_visible"] is False
    assert payload["form_mission"]["page_history"][0]["before"]["page_kind"] == "review_confirmation"
    assert payload["form_mission"]["page_history"][1]["before"]["preferred_commit_button"] == "OK"
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_complete_form_flow_resolves_benign_interstitial_dialog(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    state: Dict[str, Any] = {
        "settings_open": True,
        "dialog_open": False,
        "acknowledged": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if bool(state["settings_open"]):
            rows.append({"hwnd": 2322, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"})
        if bool(state["dialog_open"]):
            rows.append({"hwnd": 2323, "title": "Settings saved", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("window_title", "") or "").strip()
        if bool(state["dialog_open"]) and title != "Settings":
            return {
                "status": "success",
                "items": [
                    {"element_id": "dialog-complete", "name": "Settings saved", "control_type": "Pane"},
                    {"element_id": "btn-ok", "name": "OK", "control_type": "Button"},
                ],
            }
        if bool(state["settings_open"]):
            return {
                "status": "success",
                "items": [
                    {"element_id": "check-review", "name": "I understand the pending changes", "control_type": "CheckBox", "checked": bool(state["acknowledged"])},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                    {"element_id": "btn-cancel", "name": "Cancel", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "screen_hash": "settings_saved_dialog",
                "text": "Settings saved dialog. Click OK to close this message.",
                "screenshot_path": "E:/tmp/settings_saved_dialog.png",
            }
        if bool(state["settings_open"]):
            return {
                "status": "success",
                "screen_hash": "settings_review_page",
                "text": "Settings review changes. I understand the pending changes before applying.",
                "screenshot_path": "E:/tmp/settings_review_page.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_after_settings_dialog",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_after_settings_dialog.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"check-review", "i understand the pending changes"} and bool(state["settings_open"]) and not bool(state["dialog_open"]):
            state["acknowledged"] = True
            return {"status": "success", "invoked": target}
        if target in {"btn-apply", "apply"} and bool(state["settings_open"]) and bool(state["acknowledged"]) and not bool(state["dialog_open"]):
            state["dialog_open"] = True
            return {"status": "success", "invoked": target}
        if target in {"btn-ok", "ok"} and bool(state["dialog_open"]):
            state["dialog_open"] = False
            state["settings_open"] = False
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 2323, "title": "Settings saved", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}
                    if bool(state["dialog_open"])
                    else {"hwnd": 2322, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}
                    if bool(state["settings_open"])
                    else {"hwnd": 9009, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"}
                ),
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2322), "title": str(payload.get("window_title", "") or "Settings")}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_form_flow", "app_name": "settings", "max_form_pages": 4})

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["form_mission"]["completed"] is True
    assert payload["form_mission"]["pages_completed"] == 2
    assert payload["form_mission"]["page_history"][1]["status"] == "dialog_confirmed"
    assert payload["form_mission"]["page_history"][1]["before"]["window_title"] == "Settings saved"
    assert payload["form_mission"]["page_history"][1]["before"]["preferred_dialog_confirmation_button"] == "OK"
    assert payload["form_mission"]["page_history"][1]["dialog_followup"]["button_label"] == "OK"
    assert payload["form_mission"]["final_page"]["form_visible"] is False
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_stops_complete_form_flow_on_risky_interstitial_dialog(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    state: Dict[str, Any] = {
        "settings_open": True,
        "dialog_open": False,
        "acknowledged": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if bool(state["settings_open"]):
            rows.append({"hwnd": 2324, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"})
        if bool(state["dialog_open"]):
            rows.append({"hwnd": 2325, "title": "Confirm changes", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("window_title", "") or "").strip()
        if bool(state["dialog_open"]) and title != "Settings":
            return {
                "status": "success",
                "items": [
                    {"element_id": "dialog-warning", "name": "Warning", "control_type": "Pane"},
                    {"element_id": "btn-continue", "name": "Continue", "control_type": "Button"},
                    {"element_id": "btn-cancel", "name": "Cancel", "control_type": "Button"},
                ],
            }
        if bool(state["settings_open"]):
            return {
                "status": "success",
                "items": [
                    {"element_id": "check-review", "name": "I understand the pending changes", "control_type": "CheckBox", "checked": bool(state["acknowledged"])},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                    {"element_id": "btn-cancel-main", "name": "Cancel", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "screen_hash": "settings_warning_dialog",
                "text": "Warning dialog: continuing will skip the review step. Continue or Cancel.",
                "screenshot_path": "E:/tmp/settings_warning_dialog.png",
            }
        if bool(state["settings_open"]):
            return {
                "status": "success",
                "screen_hash": "settings_review_page",
                "text": "Settings review changes. I understand the pending changes before applying.",
                "screenshot_path": "E:/tmp/settings_review_page.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_after_risky_dialog",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_after_risky_dialog.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"check-review", "i understand the pending changes"} and bool(state["settings_open"]) and not bool(state["dialog_open"]):
            state["acknowledged"] = True
            return {"status": "success", "invoked": target}
        if target in {"btn-apply", "apply"} and bool(state["settings_open"]) and bool(state["acknowledged"]) and not bool(state["dialog_open"]):
            state["dialog_open"] = True
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 2325, "title": "Confirm changes", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}
                    if bool(state["dialog_open"])
                    else {"hwnd": 2324, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}
                ),
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2324), "title": str(payload.get("window_title", "") or "Settings")}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_form_flow", "app_name": "settings", "max_form_pages": 4})

    assert payload["status"] == "partial"
    assert payload["verification"]["verified"] is False
    assert payload["form_mission"]["completed"] is False
    assert payload["form_mission"]["stop_reason_code"] == "form_dialog_review_required"
    assert payload["form_mission"]["page_history"][1]["status"] == "blocked"
    assert payload["form_mission"]["page_history"][1]["before"]["window_title"] == "Confirm changes"
    assert payload["form_mission"]["page_history"][1]["before"]["preferred_dialog_confirmation_button"] == "Continue"
    assert payload["form_mission"]["final_page"]["screen_hash"] == "settings_warning_dialog"
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_stops_complete_form_flow_on_credential_interstitial_dialog(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    state: Dict[str, Any] = {
        "settings_open": True,
        "dialog_open": False,
        "acknowledged": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if bool(state["settings_open"]):
            rows.append({"hwnd": 2328, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"})
        if bool(state["dialog_open"]):
            rows.append({"hwnd": 2329, "title": "Windows Security", "exe": r"C:\Windows\System32\CredentialUIBroker.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("window_title", "") or "").strip()
        if bool(state["dialog_open"]) and title != "Settings":
            return {
                "status": "success",
                "items": [
                    {"element_id": "field-username", "name": "Username", "control_type": "Edit", "value_text": ""},
                    {"element_id": "field-password", "name": "Password", "control_type": "Edit", "value_text": ""},
                    {"element_id": "btn-sign-in", "name": "Sign in", "control_type": "Button"},
                    {"element_id": "btn-cancel", "name": "Cancel", "control_type": "Button"},
                ],
            }
        if bool(state["settings_open"]):
            return {
                "status": "success",
                "items": [
                    {"element_id": "check-review", "name": "I understand the pending changes", "control_type": "CheckBox", "checked": bool(state["acknowledged"])},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                    {"element_id": "btn-cancel-main", "name": "Cancel", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "screen_hash": "settings_credential_dialog",
                "text": "Windows Security sign in dialog. Enter username and password to continue.",
                "screenshot_path": "E:/tmp/settings_credential_dialog.png",
            }
        if bool(state["settings_open"]):
            return {
                "status": "success",
                "screen_hash": "settings_review_page",
                "text": "Settings review changes. I understand the pending changes before applying.",
                "screenshot_path": "E:/tmp/settings_review_page.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_after_credential_dialog",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_after_credential_dialog.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"check-review", "i understand the pending changes"} and bool(state["settings_open"]) and not bool(state["dialog_open"]):
            state["acknowledged"] = True
            return {"status": "success", "invoked": target}
        if target in {"btn-apply", "apply"} and bool(state["settings_open"]) and bool(state["acknowledged"]) and not bool(state["dialog_open"]):
            state["dialog_open"] = True
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 2329, "title": "Windows Security", "exe": r"C:\Windows\System32\CredentialUIBroker.exe"}
                    if bool(state["dialog_open"])
                    else {"hwnd": 2328, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}
                ),
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2328), "title": str(payload.get("window_title", "") or "Settings")}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_form_flow", "app_name": "settings", "max_form_pages": 4})

    assert payload["status"] == "partial"
    assert payload["verification"]["verified"] is False
    assert payload["form_mission"]["completed"] is False
    assert payload["form_mission"]["stop_reason_code"] == "credential_input_required"
    assert payload["form_mission"]["page_history"][1]["status"] == "blocked"
    assert payload["form_mission"]["page_history"][1]["before"]["window_title"] == "Windows Security"
    assert payload["form_mission"]["page_history"][1]["before"]["dialog_kind"] == "credential_prompt"
    assert payload["form_mission"]["page_history"][1]["blocking_surface"]["approval_kind"] == "credential_input"
    assert payload["form_mission"]["page_history"][1]["dialog_followup"]["dialog_kind"] == "credential_prompt"
    assert payload["form_mission"]["blocking_surface"]["approval_kind"] == "credential_input"
    assert payload["form_mission"]["blocking_surface"]["resume_action"] == "complete_form_flow"
    assert "provide_credentials" in payload["form_mission"]["blocking_surface"]["resume_preconditions"]
    assert payload["form_mission"]["resume_contract"]["resume_action"] == "complete_form_flow"
    assert payload["form_mission"]["resume_contract"]["resume_strategy"] == "reacquire_app_surface"
    assert payload["form_mission"]["resume_contract"]["resume_payload"]["app_name"] == "settings"
    assert payload["form_mission"]["resume_contract"]["resume_payload"]["expected_form_target_count"] == 0
    assert payload["form_mission"]["final_page"]["screen_hash"] == "settings_credential_dialog"
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_stops_complete_form_flow_on_uac_consent_interstitial_dialog(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    state: Dict[str, Any] = {
        "settings_open": True,
        "dialog_open": False,
        "acknowledged": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if bool(state["settings_open"]):
            rows.append({"hwnd": 2330, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"})
        if bool(state["dialog_open"]):
            rows.append({"hwnd": 2331, "title": "User Account Control", "exe": r"C:\Windows\System32\consent.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("window_title", "") or "").strip()
        if bool(state["dialog_open"]) and title != "Settings":
            return {
                "status": "success",
                "items": [
                    {"element_id": "btn-yes", "name": "Yes", "control_type": "Button"},
                    {"element_id": "btn-no", "name": "No", "control_type": "Button"},
                ],
            }
        if bool(state["settings_open"]):
            return {
                "status": "success",
                "items": [
                    {"element_id": "check-review", "name": "I understand the pending changes", "control_type": "CheckBox", "checked": bool(state["acknowledged"])},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                    {"element_id": "btn-cancel-main", "name": "Cancel", "control_type": "Button"},
                ],
            }
        return {"status": "success", "items": []}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["dialog_open"]):
            return {
                "status": "success",
                "screen_hash": "settings_uac_dialog",
                "text": "User Account Control. Do you want to allow this app to make changes to your device?",
                "screenshot_path": "E:/tmp/settings_uac_dialog.png",
            }
        if bool(state["settings_open"]):
            return {
                "status": "success",
                "screen_hash": "settings_review_page",
                "text": "Settings review changes. I understand the pending changes before applying.",
                "screenshot_path": "E:/tmp/settings_review_page.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_after_settings_uac_dialog",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_after_settings_uac_dialog.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"check-review", "i understand the pending changes"} and bool(state["settings_open"]) and not bool(state["dialog_open"]):
            state["acknowledged"] = True
            return {"status": "success", "invoked": target}
        if target in {"btn-apply", "apply"} and bool(state["settings_open"]) and bool(state["acknowledged"]) and not bool(state["dialog_open"]):
            state["dialog_open"] = True
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 2331, "title": "User Account Control", "exe": r"C:\Windows\System32\consent.exe"}
                    if bool(state["dialog_open"])
                    else {"hwnd": 2330, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}
                ),
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2330), "title": str(payload.get("window_title", "") or "Settings")}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_form_flow", "app_name": "settings", "max_form_pages": 4})

    assert payload["status"] == "partial"
    assert payload["verification"]["verified"] is False
    assert payload["form_mission"]["completed"] is False
    assert payload["form_mission"]["stop_reason_code"] == "elevation_consent_required"
    assert payload["form_mission"]["page_history"][1]["status"] == "blocked"
    assert payload["form_mission"]["page_history"][1]["before"]["window_title"] == "User Account Control"
    assert payload["form_mission"]["page_history"][1]["before"]["dialog_kind"] == "elevation_prompt"
    assert payload["form_mission"]["page_history"][1]["before"]["approval_kind"] == "elevation_consent"
    assert payload["form_mission"]["page_history"][1]["blocking_surface"]["approval_kind"] == "elevation_consent"
    assert payload["form_mission"]["page_history"][1]["dialog_followup"]["dialog_kind"] == "elevation_prompt"
    assert payload["form_mission"]["page_history"][1]["dialog_followup"]["approval_kind"] == "elevation_consent"
    assert payload["form_mission"]["page_history"][1]["dialog_followup"]["secure_desktop_likely"] is True
    assert payload["form_mission"]["blocking_surface"]["approval_kind"] == "elevation_consent"
    assert payload["form_mission"]["blocking_surface"]["secure_desktop_likely"] is True
    assert "approve_elevation_request" in payload["form_mission"]["blocking_surface"]["resume_preconditions"]
    assert payload["form_mission"]["resume_contract"]["resume_action"] == "complete_form_flow"
    assert payload["form_mission"]["resume_contract"]["resume_strategy"] == "reacquire_app_surface"
    assert payload["form_mission"]["resume_contract"]["resume_payload"]["app_name"] == "settings"
    assert "approve_elevation_request" in payload["form_mission"]["resume_contract"]["resume_preconditions"]
    assert payload["form_mission"]["final_page"]["screen_hash"] == "settings_uac_dialog"
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_complete_form_page_reconciles_requested_targets_before_commit(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2320, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2320, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "toggle-bluetooth", "name": "Bluetooth", "control_type": "ToggleButton", "checked": False, "toggle_state": "Off"},
                    {"element_id": "slider-brightness", "name": "Brightness", "control_type": "Slider", "value_text": "40", "range_value": 40, "range_min": 0, "range_max": 100},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_targets_page",
                "text": "Settings brightness 40 bluetooth disabled. Apply changes when ready.",
                "screenshot_path": "E:/tmp/settings_targets_page.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise(
        {
            "action": "complete_form_page",
            "app_name": "settings",
            "form_target_plan": [
                {"action": "enable_switch", "query": "Bluetooth"},
                {"action": "set_value_control", "query": "Brightness", "text": "80"},
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["form_target_state"]["requested_count"] == 2
    assert payload["form_target_state"]["planned_target_count"] == 2
    assert [step["action"] for step in payload["execution_plan"][-5:]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "keyboard_hotkey",
        "keyboard_type",
        "accessibility_invoke_element",
    ]
    assert payload["execution_plan"][-5]["args"]["element_id"] == "toggle-bluetooth"
    assert payload["execution_plan"][-4]["args"]["element_id"] == "slider-brightness"
    assert payload["execution_plan"][-3]["args"]["keys"] == ["ctrl", "a"]
    assert payload["execution_plan"][-2]["args"]["text"] == "80"
    assert payload["execution_plan"][-1]["args"]["element_id"] == "btn-apply"


def test_desktop_action_router_complete_form_flow_tracks_requested_form_targets(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    state: Dict[str, Any] = {"form_open": True, "bluetooth_on": False, "brightness": 40, "focused": ""}

    def _elements(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["form_open"]):
            return {"status": "success", "items": []}
        return {
            "status": "success",
            "items": [
                {
                    "element_id": "toggle-bluetooth",
                    "name": "Bluetooth",
                    "control_type": "ToggleButton",
                    "checked": bool(state["bluetooth_on"]),
                    "toggle_state": "On" if state["bluetooth_on"] else "Off",
                },
                {
                    "element_id": "slider-brightness",
                    "name": "Brightness",
                    "control_type": "Slider",
                    "value_text": str(state["brightness"]),
                    "range_value": int(state["brightness"]),
                    "range_min": 0,
                    "range_max": 100,
                },
                {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
            ],
        }

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["form_open"]):
            return {
                "status": "success",
                "screen_hash": f"settings_targets_{int(state['brightness'])}_{'on' if state['bluetooth_on'] else 'off'}",
                "text": f"Settings brightness {state['brightness']} bluetooth {'enabled' if state['bluetooth_on'] else 'disabled'}. Apply changes when ready.",
                "screenshot_path": "E:/tmp/settings_targets_page.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_ready",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_ready.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"toggle-bluetooth", "bluetooth"}:
            state["bluetooth_on"] = not state["bluetooth_on"]
            return {"status": "success", "invoked": target}
        if target in {"slider-brightness", "brightness"}:
            state["focused"] = "brightness"
            return {"status": "success", "invoked": target}
        if target in {"btn-apply", "apply"} and bool(state["bluetooth_on"]) and int(state["brightness"]) == 80:
            state["form_open"] = False
            state["focused"] = ""
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    def _keyboard_hotkey(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"status": "success", "keys": payload.get("keys", [])}

    def _keyboard_type(payload: Dict[str, Any]) -> Dict[str, Any]:
        if state["focused"] == "brightness":
            state["brightness"] = int(str(payload.get("text", "0") or "0"))
            return {"status": "success", "text": payload.get("text", "")}
        return {"status": "error", "message": "no focused control"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2321, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}] if bool(state["form_open"]) else [],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2321, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"} if bool(state["form_open"]) else {"hwnd": 9002, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"},
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2321), "title": "Settings"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "keyboard_hotkey": _keyboard_hotkey,
            "keyboard_type": _keyboard_type,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "complete_form_flow",
            "app_name": "settings",
            "max_form_pages": 3,
            "form_target_plan": [
                {"action": "enable_switch", "query": "Bluetooth"},
                {"action": "set_value_control", "query": "Brightness", "text": "80"},
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["form_mission"]["completed"] is True
    assert payload["form_mission"]["requested_target_count"] == 2
    assert payload["form_mission"]["resolved_target_count"] == 2
    assert payload["form_mission"]["remaining_target_count"] == 0
    assert payload["form_mission"]["page_history"][0]["target_state_before"]["remaining_count"] == 2
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "keyboard_hotkey",
        "keyboard_type",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_complete_form_flow_switches_tabs_to_find_requested_targets(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Control Panel                             Microsoft.ControlPanel       1.0                  winget"],
    )
    state: Dict[str, Any] = {
        "form_open": True,
        "selected_tab": "General",
        "security_alerts_enabled": False,
    }

    def _elements(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["form_open"]):
            return {"status": "success", "items": []}
        selected_tab = str(state["selected_tab"])
        items: List[Dict[str, Any]] = [
            {"element_id": "tab-general", "name": "General", "control_type": "TabItem", "selected": selected_tab == "General"},
            {"element_id": "tab-security", "name": "Security", "control_type": "TabItem", "selected": selected_tab == "Security"},
            {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
        ]
        if selected_tab == "Security":
            items.insert(
                2,
                {
                    "element_id": "toggle-security-alerts",
                    "name": "Security Alerts",
                    "control_type": "ToggleButton",
                    "checked": bool(state["security_alerts_enabled"]),
                    "toggle_state": "On" if state["security_alerts_enabled"] else "Off",
                },
            )
        return {"status": "success", "items": items}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["form_open"]):
            return {
                "status": "success",
                "screen_hash": "desktop_ready",
                "text": "Desktop ready",
                "screenshot_path": "E:/tmp/desktop_ready.png",
            }
        selected_tab = str(state["selected_tab"])
        if selected_tab == "Security":
            return {
                "status": "success",
                "screen_hash": f"control_panel_security_{'on' if state['security_alerts_enabled'] else 'off'}",
                "text": f"Control panel security tab security alerts {'enabled' if state['security_alerts_enabled'] else 'disabled'}. Apply changes when ready.",
                "screenshot_path": "E:/tmp/control_panel_security.png",
            }
        return {
            "status": "success",
            "screen_hash": "control_panel_general",
            "text": "Control panel general tab selected. Apply changes when ready.",
            "screenshot_path": "E:/tmp/control_panel_general.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"tab-security", "security"}:
            state["selected_tab"] = "Security"
            return {"status": "success", "invoked": target}
        if target in {"toggle-security-alerts", "security alerts"}:
            state["security_alerts_enabled"] = not state["security_alerts_enabled"]
            return {"status": "success", "invoked": target}
        if target in {"btn-apply", "apply"} and str(state["selected_tab"]) == "Security" and bool(state["security_alerts_enabled"]):
            state["form_open"] = False
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2410, "title": "Control Panel", "exe": r"C:\Windows\System32\control.exe"}] if bool(state["form_open"]) else [],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2410, "title": "Control Panel", "exe": r"C:\Windows\System32\control.exe"} if bool(state["form_open"]) else {"hwnd": 9003, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"},
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2410), "title": "Control Panel"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "complete_form_flow",
            "app_name": "control panel",
            "max_form_pages": 4,
            "form_target_plan": [
                {"action": "enable_switch", "query": "Security Alerts"},
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["form_mission"]["completed"] is True
    assert payload["form_mission"]["resolved_target_count"] == 1
    assert payload["form_mission"]["remaining_target_count"] == 0
    assert payload["form_mission"]["page_history"][0]["status"] == "tab_switched"
    assert payload["form_mission"]["page_history"][0]["tab_hunt"]["candidate_tabs"][0]["name"] == "Security"
    assert payload["form_mission"]["page_history"][0]["tab_hunt"]["attempts"][0]["progressed"] is True
    assert payload["form_mission"]["page_history"][1]["before"]["selected_tab"] == "Security"
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_complete_form_flow_switches_sidebar_sections_to_find_requested_targets(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    state: Dict[str, Any] = {
        "form_open": True,
        "selected_section": "Display",
        "bluetooth_enabled": False,
    }

    def _elements(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["form_open"]):
            return {"status": "success", "items": []}
        selected_section = str(state["selected_section"])
        items: List[Dict[str, Any]] = [
            {"element_id": "nav-display", "name": "Display", "control_type": "ListItem", "selected": selected_section == "Display"},
            {"element_id": "nav-bluetooth", "name": "Bluetooth", "control_type": "ListItem", "selected": selected_section == "Bluetooth"},
            {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
        ]
        if selected_section == "Bluetooth":
            items.insert(
                2,
                {
                    "element_id": "toggle-bluetooth",
                    "name": "Bluetooth",
                    "control_type": "ToggleButton",
                    "checked": bool(state["bluetooth_enabled"]),
                    "toggle_state": "On" if state["bluetooth_enabled"] else "Off",
                },
            )
        return {"status": "success", "items": items}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["form_open"]):
            return {
                "status": "success",
                "screen_hash": "desktop_ready",
                "text": "Desktop ready",
                "screenshot_path": "E:/tmp/desktop_ready.png",
            }
        selected_section = str(state["selected_section"])
        if selected_section == "Bluetooth":
            return {
                "status": "success",
                "screen_hash": f"settings_bluetooth_{'on' if state['bluetooth_enabled'] else 'off'}",
                "text": f"Settings sidebar bluetooth section bluetooth {'enabled' if state['bluetooth_enabled'] else 'disabled'}. Apply changes when ready.",
                "screenshot_path": "E:/tmp/settings_bluetooth_section.png",
            }
        return {
            "status": "success",
            "screen_hash": "settings_display_section",
            "text": "Settings sidebar display section brightness layout content",
            "screenshot_path": "E:/tmp/settings_display_section.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"nav-bluetooth", "bluetooth"} and str(state["selected_section"]) != "Bluetooth":
            state["selected_section"] = "Bluetooth"
            return {"status": "success", "invoked": target}
        if target in {"toggle-bluetooth"} or (target == "bluetooth" and str(state["selected_section"]) == "Bluetooth"):
            state["bluetooth_enabled"] = not state["bluetooth_enabled"]
            return {"status": "success", "invoked": target}
        if target in {"btn-apply", "apply"} and str(state["selected_section"]) == "Bluetooth" and bool(state["bluetooth_enabled"]):
            state["form_open"] = False
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2411, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}] if bool(state["form_open"]) else [],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2411, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"} if bool(state["form_open"]) else {"hwnd": 9004, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"},
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2411), "title": "Settings"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "complete_form_flow",
            "app_name": "settings",
            "max_form_pages": 4,
            "form_target_plan": [
                {"action": "enable_switch", "query": "Bluetooth"},
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["form_mission"]["completed"] is True
    assert payload["form_mission"]["resolved_target_count"] == 1
    assert payload["form_mission"]["remaining_target_count"] == 0
    assert payload["form_mission"]["page_history"][0]["status"] == "navigation_switched"
    assert payload["form_mission"]["page_history"][0]["navigation_hunt"]["candidate_targets"][0]["name"] == "Bluetooth"
    assert payload["form_mission"]["page_history"][0]["navigation_hunt"]["attempts"][0]["progressed"] is True
    assert payload["form_mission"]["page_history"][1]["before"]["selected_navigation_target"] == "Bluetooth"
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_complete_form_flow_expands_group_to_find_requested_targets(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    state: Dict[str, Any] = {
        "form_open": True,
        "advanced_display_expanded": False,
        "hdr_enabled": False,
    }

    def _elements(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["form_open"]):
            return {"status": "success", "items": []}
        items: List[Dict[str, Any]] = [
            {
                "element_id": "group-advanced-display",
                "name": "Advanced display",
                "control_type": "Button",
                "expanded": bool(state["advanced_display_expanded"]),
                "state_text": "expanded" if state["advanced_display_expanded"] else "collapsed",
            },
            {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
        ]
        if bool(state["advanced_display_expanded"]):
            items.insert(
                1,
                {
                    "element_id": "toggle-hdr",
                    "name": "HDR",
                    "control_type": "ToggleButton",
                    "checked": bool(state["hdr_enabled"]),
                    "toggle_state": "On" if state["hdr_enabled"] else "Off",
                },
            )
        return {"status": "success", "items": items}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["form_open"]):
            return {
                "status": "success",
                "screen_hash": "desktop_ready",
                "text": "Desktop ready",
                "screenshot_path": "E:/tmp/desktop_ready.png",
            }
        return {
            "status": "success",
            "screen_hash": f"settings_hdr_{'expanded' if state['advanced_display_expanded'] else 'collapsed'}_{'on' if state['hdr_enabled'] else 'off'}",
            "text": (
                "Settings advanced display section "
                f"{'expanded' if state['advanced_display_expanded'] else 'collapsed'} "
                f"HDR {'enabled' if state['hdr_enabled'] else 'disabled'}."
            ),
            "screenshot_path": "E:/tmp/settings_hdr_page.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"group-advanced-display", "advanced display"}:
            state["advanced_display_expanded"] = True
            return {"status": "success", "invoked": target}
        if target in {"toggle-hdr", "hdr"} and bool(state["advanced_display_expanded"]):
            state["hdr_enabled"] = not state["hdr_enabled"]
            return {"status": "success", "invoked": target}
        if target in {"btn-apply", "apply"} and bool(state["hdr_enabled"]):
            state["form_open"] = False
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2412, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}] if bool(state["form_open"]) else [],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2412, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"} if bool(state["form_open"]) else {"hwnd": 9005, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"},
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2412), "title": "Settings"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "complete_form_flow",
            "app_name": "settings",
            "max_form_pages": 4,
            "form_target_plan": [
                {"action": "enable_switch", "query": "HDR"},
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["form_mission"]["completed"] is True
    assert payload["form_mission"]["resolved_target_count"] == 1
    assert payload["form_mission"]["remaining_target_count"] == 0
    assert payload["form_mission"]["page_history"][0]["status"] == "group_expanded"
    assert payload["form_mission"]["page_history"][0]["group_hunt"]["candidate_groups"][0]["name"] == "Advanced display"
    assert payload["form_mission"]["page_history"][0]["group_hunt"]["attempts"][0]["progressed"] is True
    assert payload["form_mission"]["page_history"][1]["before"]["expanded_group_count"] == 1
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_complete_form_flow_scrolls_to_find_requested_targets(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    state: Dict[str, Any] = {
        "form_open": True,
        "scroll_position": 0,
        "night_light_enabled": False,
    }

    def _elements(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["form_open"]):
            return {"status": "success", "items": []}
        items: List[Dict[str, Any]] = [
            {"element_id": "nav-display", "name": "Display", "control_type": "ListItem", "selected": True},
            {"element_id": "scroll-main", "name": "Main scroll", "control_type": "ScrollBar"},
            {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
        ]
        if int(state["scroll_position"]) > 0:
            items.insert(
                1,
                {
                    "element_id": "toggle-night-light",
                    "name": "Night light",
                    "control_type": "ToggleButton",
                    "checked": bool(state["night_light_enabled"]),
                    "toggle_state": "On" if state["night_light_enabled"] else "Off",
                },
            )
        return {"status": "success", "items": items}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["form_open"]):
            return {
                "status": "success",
                "screen_hash": "desktop_ready",
                "text": "Desktop ready",
                "screenshot_path": "E:/tmp/desktop_ready.png",
            }
        if int(state["scroll_position"]) > 0:
            return {
                "status": "success",
                "screen_hash": f"settings_scrolled_night_light_{'on' if state['night_light_enabled'] else 'off'}",
                "text": f"Settings list pane scrolled content night light {'enabled' if state['night_light_enabled'] else 'disabled'}. Apply changes when ready.",
                "screenshot_path": "E:/tmp/settings_scrolled_night_light.png",
            }
        return {
            "status": "success",
            "screen_hash": "settings_top_of_page",
            "text": "Settings list pane top of page with scroll bar available",
            "screenshot_path": "E:/tmp/settings_top_of_page.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"toggle-night-light", "night light"} and int(state["scroll_position"]) > 0:
            state["night_light_enabled"] = not state["night_light_enabled"]
            return {"status": "success", "invoked": target}
        if target in {"btn-apply", "apply"} and bool(state["night_light_enabled"]):
            state["form_open"] = False
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    def _mouse_scroll(payload: Dict[str, Any]) -> Dict[str, Any]:
        amount = int(payload.get("amount", 0) or 0)
        if amount < 0:
            state["scroll_position"] = 1
        return {"status": "success", "amount": amount}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2413, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}] if bool(state["form_open"]) else [],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2413, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"} if bool(state["form_open"]) else {"hwnd": 9006, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"},
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2413), "title": "Settings"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "mouse_scroll": _mouse_scroll,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "complete_form_flow",
            "app_name": "settings",
            "max_form_pages": 4,
            "form_target_plan": [
                {"action": "enable_switch", "query": "Night light"},
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["form_mission"]["completed"] is True
    assert payload["form_mission"]["resolved_target_count"] == 1
    assert payload["form_mission"]["remaining_target_count"] == 0
    assert payload["form_mission"]["page_history"][0]["status"] == "scroll_progressed"
    assert payload["form_mission"]["page_history"][0]["scroll_hunt"]["attempts"][0]["method"] == "mouse_wheel_down"
    assert payload["form_mission"]["page_history"][0]["scroll_hunt"]["attempts"][0]["progressed"] is True
    assert payload["form_mission"]["page_history"][1]["target_state_before"]["visible_pending_count"] == 1
    assert [row["action"] for row in payload["results"]] == [
        "mouse_scroll",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_complete_form_flow_opens_drilldown_target_to_find_requested_targets(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    state: Dict[str, Any] = {
        "form_open": True,
        "surface": "Display",
        "hardware_acceleration_enabled": False,
    }

    def _elements(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["form_open"]):
            return {"status": "success", "items": []}
        items: List[Dict[str, Any]] = [
            {"element_id": "nav-display", "name": "Display", "control_type": "ListItem", "selected": True},
            {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
        ]
        if str(state["surface"]) == "Display":
            items.insert(
                1,
                {
                    "element_id": "link-advanced-graphics-settings",
                    "name": "Advanced graphics settings",
                    "control_type": "Hyperlink",
                },
            )
        else:
            items.insert(
                1,
                {
                    "element_id": "toggle-hardware-acceleration",
                    "name": "Hardware acceleration",
                    "control_type": "ToggleButton",
                    "checked": bool(state["hardware_acceleration_enabled"]),
                    "toggle_state": "On" if state["hardware_acceleration_enabled"] else "Off",
                },
            )
        return {"status": "success", "items": items}

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["form_open"]):
            return {
                "status": "success",
                "screen_hash": "desktop_ready",
                "text": "Desktop ready",
                "screenshot_path": "E:/tmp/desktop_ready.png",
            }
        if str(state["surface"]) == "Display":
            return {
                "status": "success",
                "screen_hash": "settings_display_root",
                "text": "Settings list pane display page advanced graphics settings link available",
                "screenshot_path": "E:/tmp/settings_display_root.png",
            }
        return {
            "status": "success",
            "screen_hash": f"settings_advanced_graphics_{'on' if state['hardware_acceleration_enabled'] else 'off'}",
            "text": f"Settings list pane advanced graphics page hardware acceleration {'enabled' if state['hardware_acceleration_enabled'] else 'disabled'}. Apply changes when ready.",
            "screenshot_path": "E:/tmp/settings_advanced_graphics.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"link-advanced-graphics-settings", "advanced graphics settings"} and str(state["surface"]) == "Display":
            state["surface"] = "Advanced graphics"
            return {"status": "success", "invoked": target}
        if target in {"toggle-hardware-acceleration", "hardware acceleration"} and str(state["surface"]) == "Advanced graphics":
            state["hardware_acceleration_enabled"] = not state["hardware_acceleration_enabled"]
            return {"status": "success", "invoked": target}
        if target in {"btn-apply", "apply"} and str(state["surface"]) == "Advanced graphics" and bool(state["hardware_acceleration_enabled"]):
            state["form_open"] = False
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2414, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}] if bool(state["form_open"]) else [],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2414, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"} if bool(state["form_open"]) else {"hwnd": 9007, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"},
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2414), "title": "Settings"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "complete_form_flow",
            "app_name": "settings",
            "max_form_pages": 4,
            "form_target_plan": [
                {"action": "enable_switch", "query": "Hardware acceleration"},
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["form_mission"]["completed"] is True
    assert payload["form_mission"]["resolved_target_count"] == 1
    assert payload["form_mission"]["remaining_target_count"] == 0
    assert payload["form_mission"]["page_history"][0]["status"] == "drilldown_opened"
    assert payload["form_mission"]["page_history"][0]["drilldown_hunt"]["candidate_targets"][0]["name"] == "Advanced graphics settings"
    assert payload["form_mission"]["page_history"][0]["drilldown_hunt"]["attempts"][0]["progressed"] is True
    assert payload["form_mission"]["page_history"][1]["before"]["breadcrumb_path"] == ["Display"]
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_complete_form_flow_adopts_child_window_after_drilldown(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        [
            "Windows Settings                          Microsoft.WindowsSettings   1.0                  winget",
            "Control Panel                             Microsoft.ControlPanel       1.0                  winget",
        ],
    )
    state: Dict[str, Any] = {
        "settings_open": True,
        "child_open": False,
        "legacy_toggle_enabled": False,
    }

    def _windows() -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if bool(state["settings_open"]):
            rows.append({"hwnd": 2510, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"})
        if bool(state["child_open"]):
            rows.append({"hwnd": 2511, "title": "Adapter Properties", "exe": r"C:\Windows\System32\control.exe"})
        return rows

    def _elements(payload: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(state["settings_open"] or state["child_open"]):
            return {"status": "success", "items": []}
        title = str(payload.get("window_title", "") or "").strip()
        if title == "Adapter Properties":
            return {
                "status": "success",
                "items": [
                    {"element_id": "toggle-legacy-gpu", "name": "Legacy GPU Scheduling", "control_type": "ToggleButton", "checked": bool(state["legacy_toggle_enabled"]), "toggle_state": "On" if state["legacy_toggle_enabled"] else "Off"},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                ],
            }
        return {
            "status": "success",
            "items": [
                {"element_id": "nav-display", "name": "Display", "control_type": "ListItem", "selected": True},
                {"element_id": "link-adapter-properties", "name": "Related adapter properties", "control_type": "Hyperlink"},
                {"element_id": "btn-settings-apply", "name": "Apply", "control_type": "Button"},
            ],
        }

    def _observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if bool(state["child_open"]):
            return {
                "status": "success",
                "screen_hash": f"adapter_properties_{'on' if state['legacy_toggle_enabled'] else 'off'}",
                "text": f"Adapter properties dialog legacy gpu scheduling {'enabled' if state['legacy_toggle_enabled'] else 'disabled'}. Apply changes when ready.",
                "screenshot_path": "E:/tmp/adapter_properties.png",
            }
        if bool(state["settings_open"]):
            return {
                "status": "success",
                "screen_hash": "settings_display_root",
                "text": "Settings list pane display page related adapter properties link available",
                "screenshot_path": "E:/tmp/settings_display_root.png",
            }
        return {
            "status": "success",
            "screen_hash": "desktop_ready",
            "text": "Desktop ready",
            "screenshot_path": "E:/tmp/desktop_ready.png",
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target in {"link-adapter-properties", "related adapter properties"} and bool(state["settings_open"]):
            state["child_open"] = True
            return {"status": "success", "invoked": target}
        if target in {"toggle-legacy-gpu", "legacy gpu scheduling"} and bool(state["child_open"]):
            state["legacy_toggle_enabled"] = not state["legacy_toggle_enabled"]
            return {"status": "success", "invoked": target}
        if target in {"btn-apply", "apply"} and bool(state["child_open"]) and bool(state["legacy_toggle_enabled"]):
            state["settings_open"] = False
            state["child_open"] = False
            return {"status": "success", "invoked": target}
        return {"status": "error", "message": f"unexpected invoke target: {target}"}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": _windows()},
            "active_window": lambda _payload: {
                "status": "success",
                "window": (
                    {"hwnd": 2511, "title": "Adapter Properties", "exe": r"C:\Windows\System32\control.exe"}
                    if bool(state["child_open"])
                    else {"hwnd": 2510, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}
                    if bool(state["settings_open"])
                    else {"hwnd": 9008, "title": "Desktop", "exe": r"C:\Windows\explorer.exe"}
                ),
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2510), "title": str(payload.get("window_title", "") or "Settings")}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": _observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "complete_form_flow",
            "app_name": "settings",
            "max_form_pages": 4,
            "form_target_plan": [
                {"action": "enable_switch", "query": "Legacy GPU Scheduling"},
            ],
        }
    )

    assert payload["status"] == "success"
    assert payload["verification"]["verified"] is True
    assert payload["form_mission"]["completed"] is True
    assert payload["form_mission"]["resolved_target_count"] == 1
    assert payload["form_mission"]["page_history"][0]["status"] == "drilldown_opened"
    assert payload["form_mission"]["page_history"][0]["after"]["window_title"] == "Adapter Properties"
    assert payload["form_mission"]["page_history"][0]["after"]["window_adopted"] is True
    assert payload["form_mission"]["page_history"][1]["before"]["window_title"] == "Adapter Properties"
    assert [row["action"] for row in payload["results"]] == [
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
    ]


def test_desktop_action_router_stops_complete_form_flow_when_manual_input_is_required(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2313, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2313, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "edit-proxy", "name": "Proxy address", "control_type": "Edit", "value_text": ""},
                    {"element_id": "btn-save", "name": "Save", "control_type": "Button"},
                    {"element_id": "btn-cancel", "name": "Cancel", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_proxy_dialog",
                "text": "Settings proxy dialog. Enter proxy address before saving.",
                "screenshot_path": "E:/tmp/settings_proxy_dialog.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute({"action": "complete_form_flow", "app_name": "settings", "max_form_pages": 3})

    assert payload["status"] == "partial"
    assert payload["verification"]["verified"] is False
    assert payload["form_mission"]["completed"] is False
    assert payload["form_mission"]["stop_reason_code"] == "manual_input_required"
    assert payload["form_mission"]["page_history"][0]["status"] == "blocked"
    assert payload["form_mission"]["page_history"][0]["before"]["manual_input_likely"] is True
    assert payload["form_mission"]["final_page"]["autonomous_blocker"] == "manual_input_required"


def test_desktop_action_router_builds_form_field_workflow_for_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {"status": "success", "screen_hash": "settings_blank", "text": "", "screenshot_path": "E:/tmp/settings_blank.png"},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "set_field_value", "app_name": "settings", "query": "Device name", "text": "JARVIS"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_set_field_value"
    assert payload["surface_branch"]["prep_actions"] == ["focus_form_surface"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "accessibility_invoke_element", "accessibility_invoke_element", "keyboard_hotkey", "keyboard_type"]
    assert payload["execution_plan"][2]["args"]["query"] == "Form"
    assert payload["execution_plan"][3]["args"]["query"] == "Device name"
    assert payload["execution_plan"][4]["args"]["keys"] == ["ctrl", "a"]
    assert payload["execution_plan"][5]["args"]["text"] == "JARVIS"


def test_desktop_action_router_enables_and_disables_switch_with_live_state(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    state: Dict[str, bool] = {"bluetooth_on": False}

    def _elements(_payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "status": "success",
            "items": [
                {
                    "element_id": "toggle-bluetooth",
                    "name": "Bluetooth",
                    "control_type": "ToggleButton",
                    "checked": bool(state["bluetooth_on"]),
                    "toggle_state": "On" if state["bluetooth_on"] else "Off",
                },
            ],
        }

    def _invoke(payload: Dict[str, Any]) -> Dict[str, Any]:
        target = str(payload.get("element_id", "") or payload.get("query", "")).strip().lower()
        if target not in {"toggle-bluetooth", "bluetooth"}:
            return {"status": "error", "message": target}
        state["bluetooth_on"] = not state["bluetooth_on"]
        return {"status": "success", "invoked": target, "bluetooth_on": state["bluetooth_on"]}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2314, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2314, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "focus_window": lambda payload: {"status": "success", "window": {"hwnd": payload.get("hwnd", 2314), "title": "Settings"}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": _elements,
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "accessibility_invoke_element": _invoke,
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_bluetooth_on" if state["bluetooth_on"] else "settings_bluetooth_off",
                "text": f"Settings bluetooth {'enabled' if state['bluetooth_on'] else 'disabled'}",
                "screenshot_path": "E:/tmp/settings_bluetooth.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    enable_payload = router.execute({"action": "enable_switch", "app_name": "settings", "query": "Bluetooth"})
    assert enable_payload["status"] == "success"
    assert enable_payload["verification"]["verified"] is True
    assert state["bluetooth_on"] is True

    redundant_enable = router.advise({"action": "enable_switch", "app_name": "settings", "query": "Bluetooth"})
    assert redundant_enable["status"] == "success"
    assert not any(
        step["args"].get("element_id") == "toggle-bluetooth"
        for step in redundant_enable["execution_plan"]
        if isinstance(step, dict) and step.get("action") == "accessibility_invoke_element"
    )

    disable_payload = router.execute({"action": "disable_switch", "app_name": "settings", "query": "Bluetooth"})
    assert disable_payload["status"] == "success"
    assert disable_payload["verification"]["verified"] is True
    assert state["bluetooth_on"] is False


def test_desktop_action_router_builds_dropdown_selection_workflow_for_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {"status": "success", "screen_hash": "settings_blank", "text": "", "screenshot_path": "E:/tmp/settings_blank.png"},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "select_dropdown_option", "app_name": "settings", "query": "Language", "text": "English"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_select_dropdown_option"
    assert payload["surface_branch"]["prep_actions"] == ["focus_form_surface", "focus_input_field"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "accessibility_invoke_element", "accessibility_invoke_element", "keyboard_hotkey", "keyboard_type"]
    assert payload["execution_plan"][2]["args"]["query"] == "Form"
    assert payload["execution_plan"][3]["args"]["query"] == "Language"
    assert payload["execution_plan"][4]["args"]["keys"] == ["alt", "down"]
    assert payload["execution_plan"][5]["args"]["text"] == "English"
    assert payload["execution_plan"][5]["args"]["press_enter"] is True


def test_desktop_action_router_prefers_live_dropdown_option_dispatch_when_option_is_visible(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2424, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2424, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "pane-1", "name": "Form", "control_type": "Pane"},
                    {"element_id": "combo-1", "parent_id": "pane-1", "name": "Language", "control_type": "ComboBox", "expanded": True},
                    {"element_id": "opt-1", "parent_id": "combo-1", "name": "English", "control_type": "ListItem", "selected": False},
                    {"element_id": "opt-2", "parent_id": "combo-1", "name": "French", "control_type": "ListItem", "selected": False},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_dropdown_open",
                "text": "Settings form language dropdown list box choose an option",
                "screenshot_path": "E:/tmp/settings_dropdown_open.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "select_dropdown_option", "app_name": "settings", "query": "Language", "text": "English"})

    assert payload["status"] == "success"
    assert payload["surface_branch"]["surface_ready"] is True
    assert payload["surface_branch"]["skip_primary_hotkey"] is True
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert payload["execution_plan"][-1]["args"]["query"] == "English"
    assert payload["execution_plan"][-1]["args"]["element_id"] == "opt-1"
    assert not any(step["action"] == "keyboard_type" for step in payload["execution_plan"])
    assert not any(step["action"] == "keyboard_hotkey" and step["args"].get("keys") == ["alt", "down"] for step in payload["execution_plan"])


def test_desktop_action_router_surface_snapshot_detects_checkbox_query_state(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2024, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2024, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {"status": "success", "items": [{"name": "Telemetry", "control_type": "CheckBox"}]},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_checkbox",
                "text": "Settings form telemetry checked checkbox",
                "screenshot_path": "E:/tmp/settings_checkbox.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="settings", query="Telemetry", limit=10)

    assert payload["status"] == "success"
    assert payload["surface_flags"]["form_visible"] is True
    assert payload["surface_flags"]["checkbox_visible"] is True
    assert payload["surface_flags"]["checkbox_target_checked"] is True
    assert payload["surface_flags"]["checkbox_target_unchecked"] is False


def test_desktop_action_router_skips_checkbox_toggle_when_state_already_matches(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2425, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2425, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [{"element_id": "check-1", "name": "Telemetry", "control_type": "CheckBox", "checked": True}],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_checkbox_checked",
                "text": "Settings form telemetry checked checkbox",
                "screenshot_path": "E:/tmp/settings_checkbox_checked.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "check_checkbox", "app_name": "settings", "query": "Telemetry"})

    assert payload["status"] == "success"
    assert payload["surface_branch"]["target_state_ready"] is True
    assert not any(step["action"] == "keyboard_hotkey" and step["args"].get("keys") == ["space"] for step in payload["execution_plan"])
    assert not any(step["action"] == "accessibility_invoke_element" and step["args"].get("query") == "Telemetry" for step in payload["execution_plan"])


def test_desktop_action_router_uses_exact_checkbox_instance_for_check_action(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 24255, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 24255, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [{"element_id": "check-telemetry", "name": "Telemetry", "control_type": "CheckBox", "checked": False}],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_checkbox_unchecked",
                "text": "Settings form telemetry unchecked checkbox",
                "screenshot_path": "E:/tmp/settings_checkbox_unchecked.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "check_checkbox", "app_name": "settings", "query": "Telemetry"})

    assert payload["status"] == "success"
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert payload["execution_plan"][-1]["args"]["query"] == "Telemetry"
    assert payload["execution_plan"][-1]["args"]["element_id"] == "check-telemetry"
    assert not any(step["action"] == "keyboard_hotkey" and step["args"].get("keys") == ["space"] for step in payload["execution_plan"])


def test_desktop_action_router_builds_radio_selection_workflow_for_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {"status": "success", "screen_hash": "settings_blank", "text": "", "screenshot_path": "E:/tmp/settings_blank.png"},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "select_radio_option", "app_name": "settings", "query": "Dark Mode"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_select_radio_option"
    assert payload["surface_branch"]["prep_actions"] == ["focus_form_surface"]
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "accessibility_invoke_element", "accessibility_invoke_element"]
    assert payload["execution_plan"][2]["args"]["query"] == "Form"
    assert payload["execution_plan"][3]["args"]["query"] == "Dark Mode"
    assert payload["execution_plan"][3]["args"]["control_type"] == "RadioButton"


def test_desktop_action_router_builds_value_adjustment_workflow_for_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {"status": "success", "screen_hash": "settings_blank", "text": "", "screenshot_path": "E:/tmp/settings_blank.png"},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "increase_value", "app_name": "settings", "query": "Brightness", "amount": 3})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_increase_value"
    assert payload["surface_branch"]["prep_actions"] == ["focus_form_surface"]
    assert [step["action"] for step in payload["execution_plan"]] == [
        "open_app",
        "focus_window",
        "accessibility_invoke_element",
        "accessibility_invoke_element",
        "keyboard_hotkey",
        "keyboard_hotkey",
        "keyboard_hotkey",
    ]
    assert payload["execution_plan"][2]["args"]["query"] == "Form"
    assert payload["execution_plan"][3]["args"]["query"] == "Brightness"
    assert all(step["args"]["keys"] == ["up"] for step in payload["execution_plan"][4:])


def test_desktop_action_router_builds_absolute_value_control_workflow_from_live_state(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2026, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2026, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"name": "Brightness", "control_type": "Slider", "range_value": 75, "range_min": 0, "range_max": 100, "value_text": "75"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_brightness",
                "text": "Settings form brightness slider value 75",
                "screenshot_path": "E:/tmp/settings_brightness.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "set_value_control", "app_name": "settings", "query": "Brightness", "text": "80"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_set_value_control"
    assert payload["surface_snapshot"]["target_control_state"]["range_value"] == 75
    assert payload["surface_snapshot"]["target_control_state"]["control_type"] == "Slider"
    assert [step["action"] for step in payload["execution_plan"]] == [
        "accessibility_invoke_element",
        "keyboard_hotkey",
        "keyboard_hotkey",
        "keyboard_hotkey",
        "keyboard_hotkey",
        "keyboard_hotkey",
    ]
    assert payload["execution_plan"][0]["args"]["query"] == "Brightness"
    assert all(step["args"]["keys"] == ["up"] for step in payload["execution_plan"][1:])


def test_desktop_action_router_skips_value_control_update_when_target_matches_live_state(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2027, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2027, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"name": "Brightness", "control_type": "Slider", "range_value": 80, "range_min": 0, "range_max": 100, "value_text": "80"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_brightness_80",
                "text": "Settings form brightness slider value 80",
                "screenshot_path": "E:/tmp/settings_brightness_80.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "set_value_control", "app_name": "settings", "query": "Brightness", "text": "80"})

    assert payload["status"] == "success"
    assert payload["surface_branch"]["target_state_ready"] is True
    assert payload["execution_plan"] == []
    assert any("already matches the requested target value" in warning for warning in payload["warnings"])


def test_desktop_action_router_surface_snapshot_detects_radio_and_value_state(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2025, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2025, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"name": "Dark Mode", "control_type": "RadioButton", "selected": True, "state_text": "selected"},
                    {"name": "Brightness", "control_type": "Slider", "range_value": 75, "value_text": "75", "state_text": "value 75"},
                    {"name": "Timeout", "control_type": "Spinner", "value_text": "10", "state_text": "value 10"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_value_controls",
                "text": "Settings form slider spinner radio button",
                "screenshot_path": "E:/tmp/settings_value_controls.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    radio_payload = router.surface_snapshot(app_name="settings", query="Dark Mode", limit=10)
    value_payload = router.surface_snapshot(app_name="settings", query="Brightness", limit=10)

    assert radio_payload["surface_flags"]["radio_option_visible"] is True
    assert radio_payload["surface_flags"]["radio_target_selected"] is True
    assert value_payload["surface_flags"]["slider_visible"] is True
    assert value_payload["surface_flags"]["spinner_visible"] is True
    assert value_payload["surface_flags"]["value_control_visible"] is True


def test_desktop_action_router_surface_snapshot_includes_related_candidates_and_control_inventory(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2426, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2426, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "pane-1", "name": "Form", "control_type": "Pane"},
                    {"element_id": "combo-1", "parent_id": "pane-1", "name": "Language", "control_type": "ComboBox", "expanded": True},
                    {"element_id": "opt-1", "parent_id": "combo-1", "name": "English", "control_type": "ListItem", "selected": True},
                    {"element_id": "opt-2", "parent_id": "combo-1", "name": "French", "control_type": "ListItem", "selected": False},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_related_controls",
                "text": "Settings form language dropdown list box",
                "screenshot_path": "E:/tmp/settings_related_controls.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="settings", query="Language", limit=10)

    assert payload["status"] == "success"
    assert payload["query_targets"][0]["element_id"] == "combo-1"
    assert payload["control_inventory"]["listitem"] == 2
    assert payload["target_group_state"]["group_role"] == "dropdown_options"
    assert payload["target_group_state"]["option_count"] == 3
    assert "English" in payload["target_group_state"]["selected_options"]
    assert any(item["name"] == "English" for item in payload["selection_candidates"])
    assert any(item["name"] == "English" for item in payload["query_related_candidates"])


def test_desktop_action_router_builds_tab_page_workflow_for_control_panel(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Control Panel                             Microsoft.ControlPanel       1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {"status": "success", "screen_hash": "control_panel_blank", "text": "", "screenshot_path": "E:/tmp/control_panel_blank.png"},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "select_tab_page", "app_name": "control panel", "query": "Security"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_select_tab_page"
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "accessibility_invoke_element"]
    assert payload["execution_plan"][2]["args"]["query"] == "Security"
    assert payload["execution_plan"][2]["args"]["control_type"] == "TabItem"


def test_desktop_action_router_surface_snapshot_detects_tab_target_state(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Control Panel                             Microsoft.ControlPanel       1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2028, "title": "Control Panel", "exe": r"C:\Windows\System32\control.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2028, "title": "Control Panel", "exe": r"C:\Windows\System32\control.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"name": "General", "control_type": "TabItem", "selected": False},
                    {"name": "Security", "control_type": "TabItem", "selected": True},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "control_panel_tabs",
                "text": "Control panel property sheet security tab selected",
                "screenshot_path": "E:/tmp/control_panel_tabs.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="control panel", query="Security", limit=10)

    assert payload["status"] == "success"
    assert payload["surface_flags"]["tab_page_visible"] is True
    assert payload["surface_flags"]["tab_target_active"] is True
    assert payload["target_control_state"]["control_type"] == "TabItem"
    assert payload["target_control_state"]["selected"] is True
    assert payload["query_targets"][0]["name"] == "Security"


def test_desktop_action_router_surface_snapshot_exposes_form_page_tabs(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Control Panel                             Microsoft.ControlPanel       1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2092, "title": "Control Panel", "exe": r"C:\Windows\System32\control.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2092, "title": "Control Panel", "exe": r"C:\Windows\System32\control.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "tab-general", "name": "General", "control_type": "TabItem", "selected": True},
                    {"element_id": "tab-security", "name": "Security", "control_type": "TabItem", "selected": False},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "control_panel_property_sheet_general",
                "text": "Control panel properties general tab selected",
                "screenshot_path": "E:/tmp/control_panel_property_sheet_general.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="control panel", limit=10)

    assert payload["status"] == "success"
    assert payload["form_page_state"]["page_kind"] == "property_sheet"
    assert payload["form_page_state"]["tab_count"] == 2
    assert payload["form_page_state"]["selected_tab"] == "General"
    assert [row["name"] for row in payload["form_page_state"]["available_tabs"]] == ["General", "Security"]


def test_desktop_action_router_surface_snapshot_exposes_form_page_navigation_targets(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2093, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2093, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "nav-display", "name": "Display", "control_type": "ListItem", "selected": True},
                    {"element_id": "nav-bluetooth", "name": "Bluetooth", "control_type": "ListItem", "selected": False},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_navigation_display",
                "text": "Settings sidebar display bluetooth section",
                "screenshot_path": "E:/tmp/settings_navigation_display.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="settings", limit=10)

    assert payload["status"] == "success"
    assert payload["form_page_state"]["navigation_target_count"] == 2
    assert payload["form_page_state"]["selected_navigation_target"] == "Display"
    assert payload["form_page_state"]["available_navigation_targets"][0]["navigation_action"] == "select_sidebar_item"
    assert [row["name"] for row in payload["form_page_state"]["available_navigation_targets"]] == ["Display", "Bluetooth"]


def test_desktop_action_router_surface_snapshot_exposes_expandable_groups_and_scroll_search(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2094, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2094, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "nav-display", "name": "Display", "control_type": "ListItem", "selected": True},
                    {"element_id": "group-advanced-display", "name": "Advanced display", "control_type": "Button", "expanded": False},
                    {"element_id": "scroll-main", "name": "Main scroll", "control_type": "ScrollBar"},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_advanced_display_page",
                "text": "Settings list pane advanced display page with scroll bar available",
                "screenshot_path": "E:/tmp/settings_advanced_display_page.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="settings", limit=10)

    assert payload["status"] == "success"
    assert payload["form_page_state"]["expandable_group_count"] == 1
    assert payload["form_page_state"]["available_expandable_groups"][0]["name"] == "Advanced display"
    assert payload["form_page_state"]["available_expandable_groups"][0]["expand_action"] == "expand_group"
    assert payload["form_page_state"]["scroll_search_supported"] is True


def test_desktop_action_router_surface_snapshot_exposes_form_page_drilldown_targets(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2095, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2095, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "nav-display", "name": "Display", "control_type": "ListItem", "selected": True},
                    {"element_id": "link-adapter-properties", "name": "Related adapter properties", "control_type": "Hyperlink"},
                    {"element_id": "btn-apply", "name": "Apply", "control_type": "Button"},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_display_related_adapter_properties",
                "text": "Settings list pane display page related adapter properties link available",
                "screenshot_path": "E:/tmp/settings_display_related_adapter_properties.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="settings", limit=10)

    assert payload["status"] == "success"
    assert payload["form_page_state"]["drilldown_target_count"] == 1
    assert payload["form_page_state"]["available_drilldown_targets"][0]["name"] == "Related adapter properties"
    assert payload["form_page_state"]["breadcrumb_path"] == ["Display"]


def test_desktop_action_router_preflights_navigation_tree_before_select_tree_item_in_device_manager(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Device Manager                            Microsoft.DeviceManager      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2021, "title": "Device Manager", "exe": r"C:\Windows\System32\devmgmt.msc"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2021, "title": "Device Manager", "exe": r"C:\Windows\System32\devmgmt.msc"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "device_manager_main",
                "text": "Device Manager hardware resources content area",
                "screenshot_path": "E:/tmp/device_manager_main.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "select_tree_item", "app_name": "device manager", "query": "Display adapters"})

    assert payload["status"] == "success"
    assert payload["surface_snapshot"]["surface_flags"]["tree_visible"] is False
    assert payload["surface_branch"]["prep_actions"] == ["focus_navigation_tree"]
    assert [step["action"] for step in payload["execution_plan"]] == ["accessibility_invoke_element", "accessibility_invoke_element"]
    assert payload["execution_plan"][0]["args"]["query"] == "Tree"
    assert payload["execution_plan"][1]["args"]["query"] == "Display adapters"
    assert payload["execution_plan"][1]["args"]["control_type"] == "TreeItem"


def test_desktop_action_router_uses_exact_live_tree_item_instance_when_visible(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Device Manager                            Microsoft.DeviceManager      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2428, "title": "Device Manager", "exe": r"C:\Windows\System32\devmgmt.msc"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2428, "title": "Device Manager", "exe": r"C:\Windows\System32\devmgmt.msc"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "tree-1", "name": "Tree", "control_type": "Tree"},
                    {"element_id": "node-1", "parent_id": "tree-1", "name": "Display adapters", "control_type": "TreeItem", "selected": False},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "device_manager_tree_ready",
                "text": "Device Manager tree view display adapters",
                "screenshot_path": "E:/tmp/device_manager_tree_ready.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "select_tree_item", "app_name": "device manager", "query": "Display adapters"})

    assert payload["status"] == "success"
    assert payload["surface_branch"]["surface_ready"] is True
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert payload["execution_plan"][-1]["args"]["query"] == "Display adapters"
    assert payload["execution_plan"][-1]["args"]["element_id"] == "node-1"


def test_desktop_action_router_builds_expand_tree_item_workflow_for_device_manager(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Device Manager                            Microsoft.DeviceManager      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "expand_tree_item", "app_name": "device manager", "query": "Display adapters"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_expand_tree_item"
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert payload["execution_plan"][-1]["args"]["query"] == "Display adapters"
    assert payload["execution_plan"][-1]["args"]["action"] == "double_click"


def test_desktop_action_router_skips_expand_tree_item_when_node_is_already_expanded(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Device Manager                            Microsoft.DeviceManager      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2429, "title": "Device Manager", "exe": r"C:\Windows\System32\devmgmt.msc"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2429, "title": "Device Manager", "exe": r"C:\Windows\System32\devmgmt.msc"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "tree-1", "name": "Tree", "control_type": "Tree"},
                    {"element_id": "node-1", "parent_id": "tree-1", "name": "Display adapters", "control_type": "TreeItem", "expanded": True},
                ],
            },
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "device_manager_tree_expanded",
                "text": "Device Manager tree view display adapters expanded",
                "screenshot_path": "E:/tmp/device_manager_tree_expanded.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "expand_tree_item", "app_name": "device manager", "query": "Display adapters"})

    assert payload["status"] == "success"
    assert payload["surface_branch"]["target_state_ready"] is True
    assert not any(step["action"] == "accessibility_invoke_element" and step["args"].get("action") == "double_click" for step in payload["execution_plan"])


def test_desktop_action_router_preflights_list_surface_before_select_list_item_in_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                          Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2022, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2022, "title": "Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_sidebar_content",
                "text": "Settings sidebar content bluetooth devices personalization",
                "screenshot_path": "E:/tmp/settings_sidebar_content.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "select_list_item", "app_name": "settings", "query": "Bluetooth"})

    assert payload["status"] == "success"
    assert payload["surface_snapshot"]["surface_flags"]["list_visible"] is False
    assert payload["surface_branch"]["prep_actions"] == ["focus_list_surface"]
    assert [step["action"] for step in payload["execution_plan"]] == ["accessibility_invoke_element", "accessibility_invoke_element"]
    assert payload["execution_plan"][0]["args"]["query"] == "List"
    assert payload["execution_plan"][1]["args"]["query"] == "Bluetooth"
    assert payload["execution_plan"][1]["args"]["control_type"] == "ListItem"


def test_desktop_action_router_preflights_data_table_before_select_table_row_in_task_manager(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Task Manager                              Microsoft.TaskManager       11.0                 winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2023, "title": "Task Manager", "exe": r"C:\Windows\System32\Taskmgr.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2023, "title": "Task Manager", "exe": r"C:\Windows\System32\Taskmgr.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "task_manager_no_grid",
                "text": "Task Manager processes cpu memory disk network sidebar content",
                "screenshot_path": "E:/tmp/task_manager_no_grid.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "select_table_row", "app_name": "task manager", "query": "chrome"})

    assert payload["status"] == "success"
    assert payload["surface_snapshot"]["surface_flags"]["table_visible"] is False
    assert payload["surface_branch"]["prep_actions"] == ["focus_data_table"]
    assert [step["action"] for step in payload["execution_plan"]] == ["accessibility_invoke_element", "accessibility_invoke_element"]
    assert payload["execution_plan"][0]["args"]["query"] == "Table"
    assert payload["execution_plan"][1]["args"]["query"] == "chrome"


def test_desktop_action_router_surface_snapshot_detects_generic_collection_surfaces(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Task Manager                              Microsoft.TaskManager       11.0                 winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2024, "title": "Task Manager", "exe": r"C:\Windows\System32\Taskmgr.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2024, "title": "Task Manager", "exe": r"C:\Windows\System32\Taskmgr.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {"status": "success", "items": [{"name": "Processes Table", "control_type": "Table"}]},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "collections_surface",
                "text": "Task Manager tree view results list data grid columns rows",
                "screenshot_path": "E:/tmp/collections_surface.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="task manager", query="chrome", limit=10)

    assert payload["status"] == "success"
    assert payload["surface_flags"]["tree_visible"] is True
    assert payload["surface_flags"]["list_visible"] is True
    assert payload["surface_flags"]["table_visible"] is True


def test_desktop_action_router_surface_snapshot_embeds_surface_intelligence_for_settings(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Windows Settings                         Microsoft.WindowsSettings   1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 2408, "title": "Bluetooth & devices - Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 2408, "title": "Bluetooth & devices - Settings", "exe": r"C:\Windows\ImmersiveControlPanel\SystemSettings.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_list_elements": lambda _payload: {
                "status": "success",
                "items": [
                    {"element_id": "uia_sidebar_bluetooth", "name": "Bluetooth", "control_type": "ListItem", "root_window_title": "Settings"},
                    {"element_id": "uia_device_name", "name": "Device name", "control_type": "Edit", "root_window_title": "Settings"},
                    {"element_id": "uia_apply", "name": "Apply", "control_type": "Button", "root_window_title": "Settings"},
                ],
            },
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "settings_surface",
                "text": "Bluetooth & devices Settings list input apply",
                "screenshot_path": "E:/tmp/settings_surface.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.surface_snapshot(app_name="settings", query="Bluetooth", limit=10)

    assert payload["status"] == "success"
    assert payload["surface_intelligence"]["surface_role"] == "settings"
    assert payload["surface_intelligence"]["interaction_mode"] == "settings_navigation"
    assert payload["surface_intelligence"]["query_resolution"]["best_candidate_name"] == "Bluetooth"
    assert payload["surface_summary"]["surface_flags"]["settings_surface_visible"] is True
    assert "complete_surface_exploration_flow" in payload["recommended_actions"]


def test_desktop_action_router_executes_terminal_command_in_terminal_without_hotkey(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["PowerShell                                Microsoft.PowerShell         7.5.0                winget"],
    )
    state: Dict[str, Any] = {"typed_text": ""}

    def _keyboard_type(payload: Dict[str, Any]) -> Dict[str, Any]:
        state["typed_text"] = str(payload.get("text", "") or "")
        return {"status": "success", "chars": len(state["typed_text"])}

    def _computer_observe(_payload: Dict[str, Any]) -> Dict[str, Any]:
        if state.get("typed_text"):
            return {
                "status": "success",
                "screen_hash": "terminal_after",
                "text": str(state.get("typed_text", "")),
                "screenshot_path": "E:/tmp/powershell_after.png",
            }
        return {
            "status": "success",
            "screen_hash": "terminal_before",
            "text": "",
            "screenshot_path": "E:/tmp/powershell_before.png",
        }

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 801, "title": "PowerShell", "exe": r"C:\Program Files\PowerShell\7\pwsh.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 801, "title": "PowerShell", "exe": r"C:\Program Files\PowerShell\7\pwsh.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "keyboard_type": _keyboard_type,
            "computer_observe": _computer_observe,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "terminal_command",
            "app_name": "powershell",
            "text": "npm test",
            "verify_after_action": True,
        }
    )

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_terminal_command"
    assert payload["advice"]["workflow_profile"]["supports_direct_input"] is True
    assert [row["action"] for row in payload["results"]] == ["keyboard_type"]
    assert payload["verification"]["verified"] is True


def test_desktop_action_router_executes_media_pause_with_native_transport_verification(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Spotify                                   Spotify.Spotify             1.2.71               winget"],
    )
    state: Dict[str, Any] = {"paused": False}

    def _media_pause(_payload: Dict[str, Any]) -> Dict[str, Any]:
        state["paused"] = True
        return {"status": "success", "paused": True}

    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1204, "title": "Spotify Premium", "exe": r"C:\Users\thecy\AppData\Roaming\Spotify\Spotify.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1204, "title": "Spotify Premium", "exe": r"C:\Users\thecy\AppData\Roaming\Spotify\Spotify.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "media_pause": _media_pause,
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "pause_media",
            "app_name": "spotify",
            "verify_after_action": True,
        }
    )

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_pause_media"
    assert any(row["action"] == "media_pause" for row in payload["results"])
    assert payload["verification"]["verified"] is True
    assert state["paused"] is True


def test_desktop_action_router_bootstraps_terminal_surface_before_editor_terminal_command(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 991, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 991, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_assert_text_visible": lambda payload: {
                "status": "success",
                "found": False,
                "chars": len(str(payload.get("text", "") or "")),
                "text": payload.get("text", ""),
            },
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "editor_no_terminal",
                "text": "Explorer main.py README.md",
                "screenshot_path": "E:/tmp/vscode_editor.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "terminal_command", "app_name": "vscode", "text": "npm test"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_terminal_command"
    assert payload["surface_snapshot"]["surface_flags"]["terminal_visible"] is False
    assert [step["action"] for step in payload["execution_plan"]] == ["keyboard_hotkey", "keyboard_type"]
    assert payload["execution_plan"][0]["phase"] == "preflight"
    assert payload["execution_plan"][0]["args"]["keys"] == ["ctrl", "`"]
    assert sum(1 for step in payload["execution_plan"] if step["action"] == "keyboard_hotkey") == 1
    assert any("bootstrap the surface" in str(warning).lower() for warning in payload["warnings"])


def test_desktop_action_router_preserves_ready_terminal_surface_without_retoggle(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1201, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1201, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "degraded", "capabilities": {"ocr_targets": False}},
            "focus_window": lambda payload: {
                "status": "success",
                "window": {"hwnd": payload.get("hwnd", 1201), "title": "main.py - Visual Studio Code"},
            },
            "accessibility_find_element": lambda payload: {
                "status": "success",
                "count": 1 if "terminal" in str(payload.get("query", "")).lower() else 0,
                "items": ([{"name": "Terminal", "control_type": "Pane"}] if "terminal" in str(payload.get("query", "")).lower() else []),
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "toggle_terminal",
            "app_name": "vscode",
            "verify_after_action": True,
        }
    )

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_toggle_terminal"
    assert payload["results"] == []
    assert payload["verification"]["verified"] is True
    assert any("ready surface" in str(warning).lower() for warning in payload["advice"]["warnings"])


def test_desktop_action_router_preserves_ready_search_surface_without_replay(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Google Chrome                             Google.Chrome.EXE            145.0                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1401, "title": "OpenAI Docs - Google Chrome", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1401, "title": "OpenAI Docs - Google Chrome", "exe": r"C:\Program Files\Google\Chrome\Application\chrome.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "degraded", "capabilities": {"ocr_targets": False}},
            "focus_window": lambda payload: {
                "status": "success",
                "window": {"hwnd": payload.get("hwnd", 1401), "title": "OpenAI Docs - Google Chrome"},
            },
            "accessibility_find_element": lambda payload: {
                "status": "success",
                "count": 1 if str(payload.get("query", "")).strip().lower() in {"search", "find"} else 0,
                "items": ([{"name": "Search", "control_type": "Edit"}] if str(payload.get("query", "")).strip().lower() in {"search", "find"} else []),
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "focus_search_box",
            "app_name": "chrome",
            "verify_after_action": True,
        }
    )

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_focus_search_box"
    assert payload["results"] == []
    assert payload["verification"]["verified"] is True
    assert any("ready surface" in str(warning).lower() for warning in payload["advice"]["warnings"])


def test_desktop_action_router_builds_find_replace_sequence_for_editor(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Microsoft Visual Studio Code             Microsoft.VisualStudioCode   1.105                winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1337, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1337, "title": "main.py - Visual Studio Code", "exe": r"C:\Users\thecy\AppData\Local\Programs\Microsoft VS Code\Code.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_assert_text_visible": lambda payload: {
                "status": "success",
                "found": False,
                "chars": len(str(payload.get("text", "") or "")),
                "text": payload.get("text", ""),
            },
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "editor_idle",
                "text": "Explorer main.py README.md",
                "screenshot_path": "E:/tmp/vscode_editor_idle.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "find_replace", "app_name": "vscode", "query": "TODO", "text": "DONE"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_find_replace"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "h"]
    assert [step["action"] for step in payload["execution_plan"]] == [
        "keyboard_hotkey",
        "keyboard_type",
        "keyboard_hotkey",
        "keyboard_type",
    ]
    assert payload["execution_plan"][1]["args"]["text"] == "TODO"
    assert payload["execution_plan"][2]["args"]["keys"] == ["tab"]
    assert payload["execution_plan"][3]["args"]["text"] == "DONE"


def test_desktop_action_router_builds_rename_selection_workflow_for_explorer(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["File Explorer                             Microsoft.FileExplorer      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1501, "title": "Documents - File Explorer", "exe": r"C:\Windows\explorer.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1501, "title": "Documents - File Explorer", "exe": r"C:\Windows\explorer.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_assert_text_visible": lambda payload: {
                "status": "success",
                "found": False,
                "chars": len(str(payload.get("text", "") or "")),
                "text": payload.get("text", ""),
            },
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "explorer_before_rename",
                "text": "Documents report.txt",
                "screenshot_path": "E:/tmp/explorer_before_rename.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "rename_selection", "app_name": "explorer", "text": "report-final.txt"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_rename_selection"
    assert payload["workflow_profile"]["primary_hotkey"] == ["f2"]
    assert [step["action"] for step in payload["execution_plan"]] == ["accessibility_invoke_element", "keyboard_hotkey", "keyboard_type"]
    assert payload["execution_plan"][0]["args"]["query"] == "Items View"
    assert payload["execution_plan"][2]["args"]["text"] == "report-final.txt"


def test_desktop_action_router_preflights_file_list_before_new_folder_when_navigation_tree_is_active(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["File Explorer                             Microsoft.FileExplorer      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1511, "title": "Documents - File Explorer", "exe": r"C:\Windows\explorer.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1511, "title": "Documents - File Explorer", "exe": r"C:\Windows\explorer.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda payload: {
                "status": "success",
                "count": 1 if str(payload.get("query", "")).strip().lower() == "navigation pane" else 0,
                "items": ([{"name": "Navigation Pane", "control_type": "Tree"}] if str(payload.get("query", "")).strip().lower() == "navigation pane" else []),
            },
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "explorer_navigation_tree_only",
                "text": "Navigation Pane Quick access Documents",
                "screenshot_path": "E:/tmp/explorer_navigation_tree_only.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "new_folder", "app_name": "explorer"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_new_folder"
    assert payload["surface_snapshot"]["surface_flags"]["file_list_visible"] is False
    assert payload["surface_branch"]["prep_actions"] == ["focus_file_list"]
    assert [step["action"] for step in payload["execution_plan"]] == ["accessibility_invoke_element", "keyboard_hotkey"]
    assert payload["execution_plan"][0]["args"]["query"] == "Items View"
    assert payload["execution_plan"][1]["args"]["keys"] == ["ctrl", "shift", "n"]


def test_desktop_action_router_builds_properties_workflow_for_explorer(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["File Explorer                             Microsoft.FileExplorer      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "open_properties_dialog", "app_name": "explorer"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_open_properties_dialog"
    assert payload["workflow_profile"]["primary_hotkey"] == ["alt", "enter"]
    assert payload["workflow_profile"]["supported"] is True


def test_desktop_action_router_preserves_ready_preview_pane_without_replay(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["File Explorer                             Microsoft.FileExplorer      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1601, "title": "Documents - File Explorer", "exe": r"C:\Windows\explorer.exe"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1601, "title": "Documents - File Explorer", "exe": r"C:\Windows\explorer.exe"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "degraded", "capabilities": {"ocr_targets": False}},
            "focus_window": lambda payload: {
                "status": "success",
                "window": {"hwnd": payload.get("hwnd", 1601), "title": "Documents - File Explorer"},
            },
            "accessibility_find_element": lambda payload: {
                "status": "success",
                "count": 1 if "preview" in str(payload.get("query", "")).lower() else 0,
                "items": ([{"name": "Preview Pane", "control_type": "Pane"}] if "preview" in str(payload.get("query", "")).lower() else []),
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.execute(
        {
            "action": "open_preview_pane",
            "app_name": "explorer",
            "verify_after_action": True,
        }
    )

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_open_preview_pane"
    assert payload["results"] == []
    assert payload["verification"]["verified"] is True
    assert any("ready surface" in str(warning).lower() for warning in payload["advice"]["warnings"])


def test_desktop_action_router_builds_details_pane_workflow_for_explorer(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["File Explorer                             Microsoft.FileExplorer      1.0                  winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "open_details_pane", "app_name": "explorer"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_open_details_pane"
    assert payload["workflow_profile"]["primary_hotkey"] == ["alt", "shift", "p"]
    assert payload["workflow_profile"]["supported"] is True


def test_desktop_action_router_builds_new_email_draft_workflow_for_outlook(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Outlook for Windows                       Microsoft.Outlook           1.2026               winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "new_email_draft", "app_name": "outlook"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_new_email_draft"
    assert payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "n"]
    assert payload["workflow_profile"]["supported"] is True


def test_desktop_action_router_builds_calendar_and_mail_view_workflows_for_outlook(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Outlook for Windows                       Microsoft.Outlook           1.2026               winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    calendar_payload = router.advise({"action": "open_calendar_view", "app_name": "outlook"})
    mail_payload = router.advise({"action": "open_mail_view", "app_name": "outlook"})

    assert calendar_payload["status"] == "success"
    assert calendar_payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "2"]
    assert calendar_payload["route_mode"] == "workflow_open_calendar_view"
    assert mail_payload["status"] == "success"
    assert mail_payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "1"]
    assert mail_payload["route_mode"] == "workflow_open_mail_view"


def test_desktop_action_router_builds_people_and_tasks_view_workflows_for_outlook(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Outlook for Windows                       Microsoft.Outlook           1.2026               winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    people_payload = router.advise({"action": "open_people_view", "app_name": "outlook"})
    tasks_payload = router.advise({"action": "open_tasks_view", "app_name": "outlook"})

    assert people_payload["status"] == "success"
    assert people_payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "3"]
    assert people_payload["route_mode"] == "workflow_open_people_view"
    assert tasks_payload["status"] == "success"
    assert tasks_payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "4"]
    assert tasks_payload["route_mode"] == "workflow_open_tasks_view"


def test_desktop_action_router_builds_reply_forward_and_event_workflows_for_outlook(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Outlook for Windows                       Microsoft.Outlook           1.2026               winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    reply_payload = router.advise({"action": "reply_email", "app_name": "outlook"})
    reply_all_payload = router.advise({"action": "reply_all_email", "app_name": "outlook"})
    forward_payload = router.advise({"action": "forward_email", "app_name": "outlook"})
    event_payload = router.advise({"action": "new_calendar_event", "app_name": "outlook"})

    assert reply_payload["status"] == "success"
    assert reply_payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "r"]
    assert reply_payload["route_mode"] == "workflow_reply_email"
    assert reply_all_payload["status"] == "success"
    assert reply_all_payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "shift", "r"]
    assert reply_all_payload["route_mode"] == "workflow_reply_all_email"
    assert forward_payload["status"] == "success"
    assert forward_payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "f"]
    assert forward_payload["route_mode"] == "workflow_forward_email"
    assert event_payload["status"] == "success"
    assert event_payload["workflow_profile"]["primary_hotkey"] == ["ctrl", "shift", "a"]
    assert event_payload["route_mode"] == "workflow_new_calendar_event"


def test_desktop_action_router_preserves_ready_reply_surface_without_reopen(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Outlook for Windows                       Microsoft.Outlook           1.2026               winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1888, "title": "RE: Status Update - Outlook", "exe": r"C:\Program Files\Microsoft Office\root\Office16\OUTLOOK.EXE"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1888, "title": "RE: Status Update - Outlook", "exe": r"C:\Program Files\Microsoft Office\root\Office16\OUTLOOK.EXE"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "degraded", "capabilities": {"ocr_targets": False}},
            "focus_window": lambda payload: {
                "status": "success",
                "window": {"hwnd": payload.get("hwnd", 1888), "title": "RE: Status Update - Outlook"},
            },
            "accessibility_find_element": lambda payload: {
                "status": "success",
                "count": 1 if str(payload.get("query", "")).strip().lower() in {"subject", "to", "reply"} else 0,
                "items": ([{"name": "Subject", "control_type": "Edit"}] if str(payload.get("query", "")).strip().lower() in {"subject", "to", "reply"} else []),
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "reply_email", "app_name": "outlook"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_reply_email"
    assert payload["surface_snapshot"]["surface_flags"]["email_compose_ready"] is True
    assert all(step["action"] != "keyboard_hotkey" for step in payload["execution_plan"])
    assert any("preserve the ready surface" in str(warning).lower() for warning in payload["warnings"])


def test_desktop_action_router_stages_mail_view_and_message_list_before_reply(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Outlook for Windows                       Microsoft.Outlook           1.2026               winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1891, "title": "Calendar - Outlook", "exe": r"C:\Program Files\Microsoft Office\root\Office16\OUTLOOK.EXE"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1891, "title": "Calendar - Outlook", "exe": r"C:\Program Files\Microsoft Office\root\Office16\OUTLOOK.EXE"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "outlook_calendar",
                "text": "Calendar agenda appointments",
                "screenshot_path": "E:/tmp/outlook_calendar.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "reply_email", "app_name": "outlook"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_reply_email"
    assert payload["surface_snapshot"]["surface_flags"]["mail_view_active"] is False
    assert payload["surface_branch"]["prep_actions"] == ["open_mail_view", "focus_message_list"]
    assert [step["action"] for step in payload["execution_plan"]] == ["keyboard_hotkey", "accessibility_invoke_element", "keyboard_hotkey"]
    assert payload["execution_plan"][0]["args"]["keys"] == ["ctrl", "1"]
    assert payload["execution_plan"][1]["args"]["query"] == "Message List"
    assert payload["execution_plan"][2]["args"]["keys"] == ["ctrl", "r"]


def test_desktop_action_router_opens_calendar_view_before_new_calendar_event(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Outlook for Windows                       Microsoft.Outlook           1.2026               winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {
                "status": "success",
                "windows": [{"hwnd": 1892, "title": "Inbox - Outlook", "exe": r"C:\Program Files\Microsoft Office\root\Office16\OUTLOOK.EXE"}],
            },
            "active_window": lambda _payload: {
                "status": "success",
                "window": {"hwnd": 1892, "title": "Inbox - Outlook", "exe": r"C:\Program Files\Microsoft Office\root\Office16\OUTLOOK.EXE"},
            },
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
            "accessibility_find_element": lambda _payload: {"status": "success", "count": 0, "items": []},
            "computer_observe": lambda _payload: {
                "status": "success",
                "screen_hash": "outlook_inbox",
                "text": "Inbox message list favorites",
                "screenshot_path": "E:/tmp/outlook_inbox.png",
            },
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "new_calendar_event", "app_name": "outlook"})

    assert payload["status"] == "success"
    assert payload["route_mode"] == "workflow_new_calendar_event"
    assert payload["surface_snapshot"]["surface_flags"]["calendar_view_active"] is False
    assert payload["surface_branch"]["prep_actions"] == ["open_calendar_view"]
    assert [step["action"] for step in payload["execution_plan"]] == ["keyboard_hotkey", "keyboard_hotkey"]
    assert payload["execution_plan"][0]["args"]["keys"] == ["ctrl", "2"]
    assert payload["execution_plan"][1]["args"]["keys"] == ["ctrl", "shift", "a"]


def test_desktop_action_router_builds_outlook_pane_focus_workflows(tmp_path: Path) -> None:
    registry = _build_registry(
        tmp_path,
        ["Outlook for Windows                       Microsoft.Outlook           1.2026               winget"],
    )
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "success", "capabilities": {"ocr_targets": True}},
        },
        app_profile_registry=registry,
        workflow_memory=_isolated_workflow_memory(),
        settle_delay_s=0.0,
    )

    folder_payload = router.advise({"action": "focus_folder_pane", "app_name": "outlook"})
    message_payload = router.advise({"action": "focus_message_list", "app_name": "outlook"})
    reading_payload = router.advise({"action": "focus_reading_pane", "app_name": "outlook"})

    assert folder_payload["status"] == "success"
    assert folder_payload["route_mode"] == "workflow_focus_folder_pane"
    assert folder_payload["workflow_profile"]["supports_action_dispatch"] is True
    assert folder_payload["execution_plan"][2]["action"] == "accessibility_invoke_element"
    assert folder_payload["execution_plan"][2]["args"]["query"] == "Folder Pane"

    assert message_payload["status"] == "success"
    assert message_payload["route_mode"] == "workflow_focus_message_list"
    assert message_payload["workflow_profile"]["supports_action_dispatch"] is True
    assert message_payload["execution_plan"][2]["action"] == "accessibility_invoke_element"
    assert message_payload["execution_plan"][2]["args"]["query"] == "Message List"

    assert reading_payload["status"] == "success"
    assert reading_payload["route_mode"] == "workflow_focus_reading_pane"
    assert reading_payload["workflow_profile"]["supports_action_dispatch"] is True
    assert reading_payload["execution_plan"][2]["action"] == "accessibility_invoke_element"
    assert reading_payload["execution_plan"][2]["args"]["query"] == "Reading Pane"
