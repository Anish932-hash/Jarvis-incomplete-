from __future__ import annotations

from pathlib import Path
import tempfile
from typing import Any, Dict, List

from backend.python.core.desktop_app_profile_registry import DesktopAppProfileRegistry
from backend.python.core.desktop_action_router import DesktopActionRouter
from backend.python.core.desktop_workflow_memory import DesktopWorkflowMemory


def _isolated_workflow_memory() -> DesktopWorkflowMemory:
    return DesktopWorkflowMemory(store_path=str(Path(tempfile.mkdtemp()) / "desktop_workflow_memory.json"))


def _build_router(action_handlers: Dict[str, Any]) -> DesktopActionRouter:
    return DesktopActionRouter(
        action_handlers=action_handlers,
        workflow_memory=_isolated_workflow_memory(),
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
    assert payload["app_profile"]["category"] == "browser"
    assert payload["profile_defaults_applied"]["ensure_app_launch"] is True
    assert payload["profile_defaults_applied"]["target_mode"] == "accessibility"
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "computer_click_target"]


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
    assert payload["safety_signals"]["elevation_prompt_visible"] is True
    assert payload["safety_signals"]["requires_confirmation"] is True
    assert payload["surface_flags"]["wizard_surface_visible"] is True
    assert payload["surface_flags"]["wizard_finish_available"] is True
    assert "Next" in payload["safety_signals"]["dialog_buttons"]
    assert "Cancel" in payload["safety_signals"]["safe_dialog_buttons"]
    assert "Finish" in payload["safety_signals"]["destructive_dialog_buttons"]
    assert payload["safety_signals"]["preferred_dismiss_button"] == "Cancel"
    assert payload["safety_signals"]["preferred_confirmation_button"] == "Next"
    assert payload["target_group_state"]["group_role"] == "wizard_actions"
    assert "Cancel" in payload["target_group_state"]["safe_options"]
    assert payload["wizard_page_state"]["page_kind"] == "ready_to_install"
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
    assert payload["safety_signals"]["elevation_prompt_visible"] is True
    assert payload["execution_plan"][-1]["action"] == "accessibility_invoke_element"
    assert any("destructive" in warning.lower() for warning in payload["warnings"])
    assert any("elevation" in warning.lower() for warning in payload["warnings"])


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
    assert redundant_enable["execution_plan"][-1]["action"] != "accessibility_invoke_element" or not any(
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
