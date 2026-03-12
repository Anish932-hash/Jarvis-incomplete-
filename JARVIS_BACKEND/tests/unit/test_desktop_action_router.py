from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from backend.python.core.desktop_app_profile_registry import DesktopAppProfileRegistry
from backend.python.core.desktop_action_router import DesktopActionRouter


def _build_router(action_handlers: Dict[str, Any]) -> DesktopActionRouter:
    return DesktopActionRouter(action_handlers=action_handlers, settle_delay_s=0.0)


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
    apps_file = tmp_path / "apps.txt"
    apps_file.write_text(
        "\n".join(
            [
                "Name                                      Id                           Version    Available  Source",
                "---------------------------------------------------------------------------------------------------",
                "Google Chrome                             Google.Chrome.EXE            145.0                winget",
            ]
        ),
        encoding="utf-8",
    )
    registry = DesktopAppProfileRegistry(source_paths=[str(apps_file)])
    router = DesktopActionRouter(
        action_handlers={
            "list_windows": lambda _payload: {"status": "success", "windows": []},
            "active_window": lambda _payload: {"status": "success", "window": {}},
            "accessibility_status": lambda _payload: {"status": "success", "capabilities": {"invoke_element": True}},
            "vision_status": lambda _payload: {"status": "degraded", "capabilities": {"ocr_targets": False}},
        },
        app_profile_registry=registry,
        settle_delay_s=0.0,
    )

    payload = router.advise({"action": "click", "app_name": "chrome", "query": "Settings"})

    assert payload["status"] == "success"
    assert payload["app_profile"]["category"] == "browser"
    assert payload["profile_defaults_applied"]["ensure_app_launch"] is True
    assert payload["profile_defaults_applied"]["target_mode"] == "accessibility"
    assert [step["action"] for step in payload["execution_plan"]] == ["open_app", "focus_window", "computer_click_target"]
