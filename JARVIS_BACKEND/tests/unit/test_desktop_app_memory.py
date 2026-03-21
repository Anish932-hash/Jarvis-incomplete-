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
    assert "Known surface memory was reused" in str(repeated["message"])


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
    memory_entry = dict(payload.get("memory_entry", {}))
    assert memory_entry["safe_traversal_summary"]["candidate_count"] >= 1
    assert memory_entry["surface_transitions"]


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
