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
                    "vision_labels": ["View"],
                }
            ],
        },
    )

    assert entry["metrics"]["probe_attempt_count"] == 1
    assert entry["metrics"]["probe_success_count"] == 1
    assert entry["metrics"]["ocr_target_count"] == 4
    assert entry["probe_summary"]["successful_count"] == 1
    assert entry["probe_effects"][0]["value"] == "navigation"
    assert entry["tested_controls"][0]["label"] == "view"
    assert entry["top_controls"][0]["last_probe_effect"] == "navigation"

    snapshot = memory.snapshot(app_name="explorer")
    assert snapshot["summary"]["probe_attempt_total"] == 1
    assert snapshot["summary"]["probe_success_total"] == 1
    assert snapshot["summary"]["ocr_target_total"] == 4


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
    assert payload["memory_entry"]["probe_effects"][0]["value"] in {"navigation", "surface_change"}
