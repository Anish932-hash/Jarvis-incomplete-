from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from backend.python.core.contracts import GoalRecord, GoalRequest
from backend.python.core.planner import Planner


def test_open_app_template_has_confirm_process_check() -> None:
    planner = Planner()

    step = planner._step("open_app", args={"app_name": "notepad"})  # noqa: SLF001
    confirm = step.verify.get("confirm", {})
    checks = step.verify.get("checks", [])

    assert confirm.get("action") == "list_processes"
    assert any(item.get("type") == "list_any_contains_arg" for item in checks)


def test_write_file_template_has_readback_confirmation() -> None:
    planner = Planner()

    step = planner._step(  # noqa: SLF001
        "write_file",
        args={"path": "notes.txt", "content": "hello world", "overwrite": True},
    )
    confirm = step.verify.get("confirm", {})
    checks = step.verify.get("checks", [])

    assert confirm.get("action") == "read_file"
    assert any(item.get("type") == "contains_arg" and item.get("arg") == "content" for item in checks)


def test_search_files_template_includes_folder_confirm_and_base_dir_check() -> None:
    planner = Planner()

    step = planner._step("search_files", args={"base_dir": "C:/Users/tester", "pattern": "*.py"})  # noqa: SLF001
    confirm = step.verify.get("confirm", {})
    checks = step.verify.get("checks", [])

    assert confirm.get("action") == "list_folder"
    assert any(item.get("type") == "equals_arg" and item.get("key") == "base_dir" for item in checks)


def test_list_folder_template_includes_size_confirmation_and_path_check() -> None:
    planner = Planner()

    step = planner._step("list_folder", args={"path": "C:/Users/tester"})  # noqa: SLF001
    confirm = step.verify.get("confirm", {})
    checks = step.verify.get("checks", [])

    assert confirm.get("action") == "folder_size"
    assert any(item.get("type") == "equals_arg" and item.get("key") == "path" for item in checks)
    assert any(item.get("source") == "confirm" and item.get("type") == "number_gte" for item in checks)


def test_explorer_open_path_template_checks_explorer_adapter() -> None:
    planner = Planner()

    step = planner._step("explorer_open_path", args={"path": "C:/Users/tester/Documents"})  # noqa: SLF001
    checks = step.verify.get("checks", [])

    assert any(item.get("type") == "contains" and item.get("key") == "adapter" for item in checks)
    assert any(item.get("type") == "equals_arg" and item.get("key") == "path" for item in checks)


def test_step_merges_template_with_custom_checks() -> None:
    planner = Planner()

    step = planner._step(  # noqa: SLF001
        "time_now",
        args={"timezone": "UTC"},
        verify={"checks": [{"source": "result", "type": "key_exists", "key": "iso"}]},
    )
    checks = step.verify.get("checks", [])

    assert any(item.get("type") == "regex" and item.get("key") == "iso" for item in checks)
    assert any(item.get("type") == "key_exists" and item.get("key") == "iso" for item in checks)


def test_browser_read_dom_template_includes_retry_profile() -> None:
    planner = Planner()

    step = planner._step("browser_read_dom", args={"url": "https://example.com"})  # noqa: SLF001
    retry = step.verify.get("retry", {})

    assert retry.get("base_delay_s") == 0.8
    assert retry.get("max_delay_s") == 6.0
    assert retry.get("multiplier") == 1.9


def test_custom_retry_rules_override_template_retry_profile() -> None:
    planner = Planner()

    step = planner._step(  # noqa: SLF001
        "open_url",
        args={"url": "https://example.com"},
        verify={"retry": {"base_delay_s": 2.0, "max_delay_s": 7.0}},
    )
    retry = step.verify.get("retry", {})

    assert retry.get("base_delay_s") == 2.0
    assert retry.get("max_delay_s") == 7.0
    assert retry.get("multiplier") == 1.8


def test_llm_step_coercion_applies_verification_template_when_verify_missing() -> None:
    planner = Planner()

    steps = planner._coerce_llm_steps([{"action": "open_url", "args": {"url": "github.com"}}])  # noqa: SLF001
    assert len(steps) == 1
    checks = steps[0].verify.get("checks", [])
    assert any(item.get("type") == "contains_arg" and item.get("arg") == "url" for item in checks)


def test_primary_file_intent_is_not_hijacked_by_domain_pattern() -> None:
    planner = Planner()

    text = 'write file "C:/Users/tester/github.com.txt" content: hello'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "write_file"
    assert steps and steps[0].action == "write_file"


def test_hotkey_intent_maps_to_keyboard_hotkey_action() -> None:
    planner = Planner()

    text = "press key ctrl+shift+s"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "keyboard_hotkey"
    assert steps and steps[0].action == "keyboard_hotkey"
    assert steps[0].args.get("keys") == ["ctrl", "shift", "s"]


def test_reasoning_intent_capture_screen_maps_to_screenshot_action() -> None:
    planner = Planner()

    step = planner._map_reasoning_intent_to_step(  # noqa: SLF001
        intent="capture_screen",
        arguments={"path": "C:/captures/shot.png"},
        original_text="",
    )

    assert step is not None
    assert step.action == "screenshot_capture"
    assert step.args.get("path") == "C:/captures/shot.png"


def test_clipboard_write_template_uses_confirm_readback() -> None:
    planner = Planner()

    step = planner._step("clipboard_write", args={"text": "hello world"})  # noqa: SLF001
    confirm = step.verify.get("confirm", {})
    checks = step.verify.get("checks", [])

    assert confirm.get("action") == "clipboard_read"
    assert any(item.get("source") == "confirm" and item.get("type") == "contains_arg" for item in checks)


def test_run_trusted_script_template_checks_pid_and_script_name() -> None:
    planner = Planner()

    step = planner._step("run_trusted_script", args={"script_name": "do_work.ps1"})  # noqa: SLF001
    checks = step.verify.get("checks", [])

    assert any(item.get("type") == "number_gte" and item.get("key") == "pid" for item in checks)
    assert any(item.get("type") == "contains_arg" and item.get("key") == "script_name" for item in checks)


def test_read_webpage_intent_routes_to_browser_read_dom() -> None:
    planner = Planner()

    text = "read webpage https://example.com/docs"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "browser_read_dom"
    assert steps and steps[0].action == "browser_read_dom"
    assert steps[0].args.get("url") == "https://example.com/docs"


def test_extract_links_intent_routes_to_browser_extract_links() -> None:
    planner = Planner()

    text = "extract links from https://example.com"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "browser_extract_links"
    assert steps and steps[0].action == "browser_extract_links"


def test_browser_session_create_intent_routes_to_session_action() -> None:
    planner = Planner()

    text = "create browser session for https://example.com with google oauth"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "browser_session_create"
    assert steps and steps[0].action == "browser_session_create"
    assert steps[0].args.get("base_url") == "https://example.com"
    assert steps[0].args.get("oauth_provider") == "google"


def test_browser_session_request_intent_routes_to_authenticated_request() -> None:
    planner = Planner()

    text = "session request session 123e4567-e89b-12d3-a456-426614174000 to https://example.com/api"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "browser_session_request"
    assert steps and steps[0].action == "browser_session_request"
    assert steps[0].args.get("session_id") == "123e4567-e89b-12d3-a456-426614174000"
    assert steps[0].args.get("url") == "https://example.com/api"


def test_desktop_interact_step_routes_open_and_type_in_app() -> None:
    planner = Planner()

    text = 'open notepad and type "hello there"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "notepad"
    assert steps[0].args.get("text") == "hello there"
    assert steps[0].args.get("action") == "type"
    assert steps[0].args.get("ensure_app_launch") is True


def test_desktop_interact_step_routes_click_in_app_context() -> None:
    planner = Planner()

    text = 'click "Save" in notepad'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "notepad"
    assert steps[0].args.get("query") == "Save"
    assert steps[0].args.get("action") == "click"


def test_desktop_interact_step_routes_navigation_in_browser_context() -> None:
    planner = Planner()

    text = "open chrome and navigate to https://openai.com"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[0].args.get("query") == "https://openai.com"
    assert steps[0].args.get("action") == "navigate"
    assert steps[0].args.get("press_enter") is True


def test_desktop_interact_step_routes_command_palette_in_editor_context() -> None:
    planner = Planner()

    text = 'run command "Preferences: Open Settings (JSON)" in vscode'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "vscode"
    assert steps[0].args.get("text") == "Preferences: Open Settings (JSON)"
    assert steps[0].args.get("action") == "command"
    assert steps[0].args.get("press_enter") is True


def test_desktop_interact_step_routes_quick_open_in_editor_context() -> None:
    planner = Planner()

    text = 'open file "settings.json" in vscode'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "vscode"
    assert steps[0].args.get("query") == "settings.json"
    assert steps[0].args.get("action") == "quick_open"
    assert steps[0].args.get("press_enter") is True


def test_desktop_interact_step_routes_bookmarks_in_browser_context() -> None:
    planner = Planner()

    text = "open bookmarks in chrome"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[0].args.get("action") == "open_bookmarks"


def test_desktop_interact_step_routes_focus_address_bar_in_browser_context() -> None:
    planner = Planner()

    text = "focus address bar in chrome"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[0].args.get("action") == "focus_address_bar"


def test_desktop_interact_step_routes_new_folder_in_explorer_context() -> None:
    planner = Planner()

    text = "new folder in file explorer"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "file explorer"
    assert steps[0].args.get("action") == "new_folder"


def test_desktop_interact_step_routes_refresh_in_explorer_context() -> None:
    planner = Planner()

    text = "refresh view in explorer"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "explorer"
    assert steps[0].args.get("action") == "refresh_view"


def test_desktop_interact_step_routes_go_up_level_in_explorer_context() -> None:
    planner = Planner()

    text = "go up in explorer"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "explorer"
    assert steps[0].args.get("action") == "go_up_level"


def test_desktop_interact_step_routes_focus_explorer_in_editor_context() -> None:
    planner = Planner()

    text = "focus explorer in vscode"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "vscode"
    assert steps[0].args.get("action") == "focus_explorer"


def test_desktop_interact_step_routes_workspace_search_in_editor_context() -> None:
    planner = Planner()

    text = 'search workspace for "TODO" in vscode'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "vscode"
    assert steps[0].args.get("query") == "TODO"
    assert steps[0].args.get("action") == "workspace_search"
    assert steps[0].args.get("press_enter") is not True


def test_desktop_interact_step_routes_go_to_symbol_in_editor_context() -> None:
    planner = Planner()

    text = 'go to symbol "AppShell" in vscode'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "vscode"
    assert steps[0].args.get("query") == "AppShell"
    assert steps[0].args.get("action") == "go_to_symbol"
    assert steps[0].args.get("press_enter") is not True


def test_desktop_interact_step_routes_rename_symbol_in_editor_context() -> None:
    planner = Planner()

    text = 'rename symbol to "WorkspaceSearch" in vscode'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "vscode"
    assert steps[0].args.get("text") == "WorkspaceSearch"
    assert steps[0].args.get("action") == "rename_symbol"
    assert steps[0].args.get("press_enter") is True


def test_desktop_interact_step_routes_new_tab_in_browser_context() -> None:
    planner = Planner()

    text = "open new tab in chrome"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[0].args.get("action") == "new_tab"


def test_desktop_interact_step_routes_switch_tab_in_browser_context() -> None:
    planner = Planner()

    text = "switch to tab 3 in firefox"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "firefox"
    assert steps[0].args.get("query") == "3"
    assert steps[0].args.get("action") == "switch_tab"


def test_desktop_interact_step_routes_next_tab_in_browser_context() -> None:
    planner = Planner()

    text = "next tab in chrome"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[0].args.get("query") == "next"
    assert steps[0].args.get("action") == "switch_tab"


def test_desktop_interact_step_routes_history_in_browser_context() -> None:
    planner = Planner()

    text = "open history in chrome"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[0].args.get("action") == "open_history"


def test_desktop_interact_step_routes_go_back_in_browser_context() -> None:
    planner = Planner()

    text = "go back in chrome"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[0].args.get("action") == "go_back"


def test_desktop_interact_step_routes_go_forward_in_explorer_context() -> None:
    planner = Planner()

    text = "go forward in explorer"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "explorer"
    assert steps[0].args.get("action") == "go_forward"


def test_desktop_interact_step_routes_folder_tree_and_file_list_in_explorer_context() -> None:
    planner = Planner()

    folder_tree_text = "focus folder tree in explorer"
    folder_tree_intent, folder_tree_steps = planner._build_primary_steps(folder_tree_text, folder_tree_text.lower())  # noqa: SLF001

    file_list_text = "focus file list in explorer"
    file_list_intent, file_list_steps = planner._build_primary_steps(file_list_text, file_list_text.lower())  # noqa: SLF001

    assert folder_tree_intent == "desktop_interact"
    assert folder_tree_steps[0].args.get("app_name") == "explorer"
    assert folder_tree_steps[0].args.get("action") == "focus_folder_tree"
    assert file_list_intent == "desktop_interact"
    assert file_list_steps[0].args.get("app_name") == "explorer"
    assert file_list_steps[0].args.get("action") == "focus_file_list"


def test_desktop_interact_step_routes_open_tab_search_in_browser_context() -> None:
    planner = Planner()

    text = "open tab search in chrome"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[0].args.get("action") == "open_tab_search"


def test_desktop_interact_step_routes_search_tabs_query_in_browser_context() -> None:
    planner = Planner()

    text = 'search tabs for "OpenAI" in chrome'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[0].args.get("query") == "OpenAI"
    assert steps[0].args.get("action") == "search_tabs"


def test_desktop_interact_step_routes_named_tab_switch_to_search_tabs_in_browser_context() -> None:
    planner = Planner()

    text = 'switch to "OpenAI Docs" tab in chrome'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[0].args.get("query") == "OpenAI Docs"
    assert steps[0].args.get("action") == "search_tabs"


def test_desktop_interact_step_routes_new_tab_in_explorer_context() -> None:
    planner = Planner()

    text = "open new tab in explorer"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "explorer"
    assert steps[0].args.get("action") == "new_tab"


def test_desktop_interact_step_routes_switch_tab_in_explorer_context() -> None:
    planner = Planner()

    text = "next tab in explorer"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "explorer"
    assert steps[0].args.get("query") == "next"
    assert steps[0].args.get("action") == "switch_tab"


def test_desktop_interact_step_routes_toggle_terminal_in_editor_context() -> None:
    planner = Planner()

    text = "toggle terminal in vscode"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "vscode"
    assert steps[0].args.get("action") == "toggle_terminal"


def test_desktop_interact_step_routes_format_document_in_editor_context() -> None:
    planner = Planner()

    text = "format document in vscode"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "vscode"
    assert steps[0].args.get("action") == "format_document"


def test_desktop_interact_step_routes_zoom_in_in_browser_context() -> None:
    planner = Planner()

    text = "zoom in in firefox"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "firefox"
    assert steps[0].args.get("action") == "zoom_in"


def test_desktop_interact_step_routes_reset_zoom_in_editor_context() -> None:
    planner = Planner()

    text = "reset zoom in vscode"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "vscode"
    assert steps[0].args.get("action") == "reset_zoom"


def test_desktop_interact_step_routes_jump_to_conversation_in_chat_context() -> None:
    planner = Planner()

    text = 'open chat with "Alice" in slack'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "slack"
    assert steps[0].args.get("query") == "Alice"
    assert steps[0].args.get("action") == "jump_to_conversation"
    assert steps[0].args.get("press_enter") is True


def test_desktop_interact_step_routes_send_message_in_chat_context() -> None:
    planner = Planner()

    text = 'send message to Alice in teams saying "Standup in 10 minutes"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "teams"
    assert steps[0].args.get("query") == "Alice"
    assert steps[0].args.get("text") == "Standup in 10 minutes"
    assert steps[0].args.get("action") == "send_message"
    assert steps[0].args.get("press_enter") is True


def test_desktop_interact_step_routes_save_document_in_office_context() -> None:
    planner = Planner()

    text = "save document in word"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "word"
    assert steps[0].args.get("action") == "save_document"


def test_desktop_interact_step_routes_start_presentation_in_office_context() -> None:
    planner = Planner()

    text = "start presentation in powerpoint"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "powerpoint"
    assert steps[0].args.get("action") == "start_presentation"


def test_desktop_interact_step_routes_terminal_command_in_terminal_context() -> None:
    planner = Planner()

    text = 'run "npm test" in powershell'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "powershell"
    assert steps[0].args.get("text") == "npm test"
    assert steps[0].args.get("action") == "terminal_command"
    assert steps[0].args.get("press_enter") is True


def test_desktop_interact_step_routes_surface_exploration_flow_in_settings_context() -> None:
    planner = Planner()

    text = "explore surface for bluetooth in settings"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "settings"
    assert steps[0].args.get("query") == "bluetooth"
    assert steps[0].args.get("action") == "complete_surface_exploration_flow"


def test_desktop_interact_step_routes_advance_surface_exploration_in_settings_context() -> None:
    planner = Planner()

    text = "advance recon for bluetooth in settings"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps and steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "settings"
    assert steps[0].args.get("query") == "bluetooth"
    assert steps[0].args.get("action") == "advance_surface_exploration"


def test_voice_delivery_fallback_uses_notification_when_tts_route_is_blocked() -> None:
    planner = Planner()

    step = planner._voice_delivery_fallback_step(  # noqa: SLF001
        message="Policy profile blocked the requested desktop action.",
        context={
            "source": "voice-session",
            "voice_route_policy": {
                "tts": {
                    "status": "blocked",
                    "route_blocked": True,
                    "reason_code": "local_launch_template_blacklisted",
                    "reason": "TTS route blocked by launcher policy.",
                },
                "summary": {
                    "status": "blocked",
                    "reason_code": "local_launch_template_blacklisted",
                },
            },
        },
        allowed_actions={"tts_speak", "send_notification"},
    )

    assert step.action == "send_notification"
    assert "blocked" in str(step.args.get("message", "")).lower()
    assert str(step.args.get("title", "")) == "JARVIS Voice Fallback"


def test_voice_delivery_policy_compacts_tts_during_route_recovery() -> None:
    planner = Planner()
    long_text = (
        "The local voice route is unstable, so I am keeping the response brief while recovery completes. "
        "You can still ask follow-up questions after the route stabilizes. "
        "If this recovery takes longer than expected, I will automatically switch to a shorter spoken summary "
        "and move the full explanation into a notification so the desktop flow keeps progressing."
    )
    steps = [planner._step("tts_speak", args={"text": long_text})]  # noqa: SLF001

    adjusted_steps, summary = planner._apply_voice_delivery_policy(  # noqa: SLF001
        steps,
        {
            "source": "voice-session",
            "voice_prefer_brief_response": True,
            "voice_route_policy": {
                "tts": {
                    "status": "recovery",
                    "route_adjusted": True,
                    "recovery_pending": True,
                    "reason_code": "local_launch_template_blacklisted",
                },
                "summary": {"status": "recovery"},
                "local_voice_pressure_score": 0.82,
            },
        },
        allowed_actions={"tts_speak", "send_notification"},
    )

    assert adjusted_steps[0].action == "tts_speak"
    assert len(str(adjusted_steps[0].args.get("text", ""))) < len(long_text)
    assert summary.get("compacted_tts_count") == 1
    assert summary.get("notification_fallback_count") == 0


def test_voice_interaction_policy_prefers_notification_during_polling_only_recovery() -> None:
    planner = Planner()

    policy = planner._voice_interaction_policy(  # noqa: SLF001
        {
            "source": "voice-session",
            "voice_route_policy": {
                "local_voice_pressure_score": 0.81,
                "mission_reliability": {
                    "sessions": 5,
                    "route_policy_pause_count": 4,
                    "wakeword_gate_events": 4,
                },
                "wakeword_supervision": {
                    "status": "polling_only",
                    "strategy": "polling_only",
                },
                "route_recovery_recommendation": {
                    "wakeword_strategy": "polling_only",
                },
                "planning_constraints": {
                    "prefer_brief_response": True,
                },
            },
        },
        allowed_actions={"tts_speak", "send_notification"},
    )

    assert policy.get("followup_mode") == "notification"
    assert policy.get("confirmation_mode") == "explicit"
    assert policy.get("prefer_notification_followup") is True
    assert policy.get("avoid_multi_turn_voice_loop") is True
    assert int(policy.get("max_steps_hint", 0) or 0) == 2


def test_voice_delivery_policy_uses_normalized_route_policy_summary() -> None:
    planner = Planner()
    planner.update_voice_route_policy_snapshot(
        {
            "route_policy_summary": {
                "status": "recovery",
                "reason_code": "voice_route_recovery_guard",
                "reason": "Voice route remains in recovery.",
            },
            "tts": {
                "status": "recovery",
                "route_adjusted": True,
                "recovery_pending": True,
            },
            "planning_constraints": {
                "prefer_brief_response": True,
                "voice_followup_mode": "hybrid",
            },
        }
    )

    policy = planner._voice_delivery_policy(  # noqa: SLF001
        {
            "source": "voice-session",
            "voice_route_policy": planner.voice_route_policy_snapshot(refresh=False),
        },
        allowed_actions={"tts_speak", "send_notification"},
    )

    assert policy.get("mode") == "brief_speech"
    assert policy.get("tts_recovery_pending") is True
    assert policy.get("reason_code") == "voice_route_recovery_guard"
    assert policy.get("followup_mode") == "hybrid"


def test_build_plan_voice_execution_policy_filters_tts_from_llm_candidates() -> None:
    planner = Planner()
    planner.llm_enabled = True
    planner.update_voice_route_policy_snapshot(
        {
            "planning_constraints": {
                "voice_followup_mode": "notification",
                "prefer_brief_response": True,
            },
            "wakeword_supervision": {
                "status": "polling_only",
                "strategy": "polling_only",
            },
            "mission_reliability": {
                "sessions": 4,
                "route_policy_pause_count": 3,
                "wakeword_gate_events": 3,
            },
        }
    )

    async def fake_llm_plan(*, text: str, context: dict[str, object], allowed_actions: set[str] | None = None):  # noqa: ANN001
        allowed = set(allowed_actions or set())
        assert "send_notification" in allowed
        assert "tts_speak" not in allowed
        return (
            "voice_notification_followup",
            [
                planner._step(
                    "send_notification",
                    args={"title": "JARVIS Voice Follow-up", "message": "Route recovery is active."},
                    verify={"expect_status": "success"},
                )
            ],
            {"provider": "groq", "model": "voice-policy-test"},
        )

    planner._build_llm_plan = fake_llm_plan  # type: ignore[method-assign]
    planner._should_try_llm = lambda **_: (True, "voice_execution_policy_test")  # type: ignore[method-assign]
    goal = GoalRecord(
        goal_id="goal-voice-execution-llm",
        request=GoalRequest(text="handle this voice follow-up", source="voice-session", metadata={}),
    )

    plan = asyncio.run(planner.build_plan(goal, context={"source": "voice-session"}))

    assert plan.context.get("planner_mode") == "llm_hybrid"
    assert plan.context.get("verification_strictness") == "strict"
    assert plan.context.get("planner_voice_filtered_llm_actions") == ["tts_speak"]
    assert plan.steps and plan.steps[0].action == "send_notification"


def test_build_plan_voice_execution_policy_marks_steps_strict_for_explicit_confirmation() -> None:
    planner = Planner()
    planner.update_voice_route_policy_snapshot(
        {
            "planning_constraints": {
                "voice_followup_mode": "notification",
            },
            "wakeword_supervision": {
                "status": "polling_only",
                "strategy": "polling_only",
            },
            "mission_reliability": {
                "sessions": 3,
                "route_policy_pause_count": 2,
                "wakeword_gate_events": 2,
            },
        }
    )
    goal = GoalRecord(
        goal_id="goal-voice-execution-deterministic",
        request=GoalRequest(text="open app notepad", source="voice-session", metadata={}),
    )

    plan = asyncio.run(planner.build_plan(goal, context={"source": "voice-session"}))

    assert plan.context.get("verification_strictness") == "strict"
    assert plan.context.get("voice_execution_policy", {}).get("verification_strictness") == "strict"
    assert plan.steps
    assert plan.steps[0].action == "open_app"
    assert str(plan.steps[0].verify.get("verification_strictness", "")).strip().lower() == "strict"


def test_voice_execution_policy_prefers_notification_channel_for_high_risk_recovery() -> None:
    planner = Planner()

    policy = planner._voice_execution_policy(  # noqa: SLF001
        {
            "source": "voice-session",
            "voice_interaction_policy": {
                "followup_mode": "hybrid",
                "confirmation_mode": "explicit",
                "prefer_non_voice_completion": True,
                "prefer_notification_followup": False,
                "local_voice_pressure_score": 0.82,
                "pause_pressure": 0.53,
            },
            "voice_delivery_policy": {
                "tts_recovery_pending": True,
            },
        },
        allowed_actions={"tts_speak", "send_notification", "clipboard_write", "open_url"},
    )

    assert policy.get("preferred_followup_action") == "send_notification"
    assert policy.get("runtime_redirect_action") == "send_notification"
    assert policy.get("followup_channel_priority", [])[0] == "send_notification"


def test_apply_voice_execution_policy_compacts_open_url_followup_metadata() -> None:
    planner = Planner()
    step = planner._step(  # noqa: SLF001
        "open_url",
        args={
            "url": "https://example.com/voice/recovery",
            "title": (
                "Wakeword recovery handoff page for the current mission with extra operator notes that should be "
                "compacted before a fragile voice follow-up tries to read them back in full."
            ),
            "description": (
                "This page explains the recovery, the fallback channel, and the next operator-safe steps after the "
                "local voice route became unstable."
            ),
        },
    )

    adjusted_steps, summary = planner._apply_voice_execution_policy(  # noqa: SLF001
        [step],
        {
            "source": "voice-session",
            "voice_interaction_policy": {
                "followup_mode": "hybrid",
                "confirmation_mode": "compact",
                "prefer_non_voice_completion": True,
                "local_voice_pressure_score": 0.74,
                "pause_pressure": 0.41,
            },
        },
        allowed_actions={"open_url", "send_notification", "clipboard_write"},
    )

    adjusted = adjusted_steps[0]
    assert adjusted.action == "open_url"
    assert len(str(adjusted.args.get("title", ""))) < 170
    assert len(str(adjusted.args.get("description", ""))) < 170
    assert summary.get("open_url_normalizations") == 1
    assert summary.get("preferred_followup_action") in {"send_notification", "clipboard_write", "open_url"}


def test_apply_voice_execution_policy_selects_present_followup_and_redirect_args() -> None:
    planner = Planner()
    steps = [
        planner._step("tts_speak", args={"text": "Voice route is degraded."}),  # noqa: SLF001
        planner._step("clipboard_write", args={"text": "Fallback summary for the operator."}),  # noqa: SLF001
        planner._step(  # noqa: SLF001
            "open_url",
            args={
                "url": "https://example.com/recovery",
                "title": "Mission recovery handoff for the current operator workflow",
            },
        ),
    ]

    adjusted_steps, summary = planner._apply_voice_execution_policy(  # noqa: SLF001
        steps,
        {
            "source": "voice-session",
            "voice_interaction_policy": {
                "followup_mode": "notification",
                "confirmation_mode": "explicit",
                "prefer_non_voice_completion": True,
                "prefer_notification_followup": False,
                "local_voice_pressure_score": 0.79,
                "pause_pressure": 0.52,
            },
        },
        allowed_actions={"tts_speak", "clipboard_write", "open_url"},
    )

    assert adjusted_steps[1].action == "clipboard_write"
    assert summary.get("runtime_redirect_action") == "clipboard_write"
    assert summary.get("present_followup_actions") == ["clipboard_write", "open_url"]
    assert summary.get("planner_followup_candidates", [])[0]["action"] == "clipboard_write"
    assert summary.get("runtime_redirect_args", {}).get("text") == "Fallback summary for the operator."
    assert adjusted_steps[1].verify.get("voice_followup_rank") == 1


def test_voice_execution_policy_can_rank_open_app_followup_for_low_pressure_voice_runs() -> None:
    planner = Planner()

    policy = planner._voice_execution_policy(  # noqa: SLF001
        {
            "source": "voice-session",
            "voice_interaction_policy": {
                "followup_mode": "spoken",
                "confirmation_mode": "minimal",
                "prefer_non_voice_completion": False,
                "prefer_notification_followup": False,
                "local_voice_pressure_score": 0.34,
                "pause_pressure": 0.18,
            },
            "voice_delivery_policy": {
                "tts_recovery_pending": False,
            },
        },
        allowed_actions={"tts_speak", "open_app", "open_url"},
    )

    assert policy.get("preferred_followup_action") == "open_app"
    assert "open_app" in policy.get("followup_channel_priority", [])
    assert policy.get("runtime_redirect_action") == "open_app"


def test_apply_voice_execution_policy_prefers_notification_step_for_high_risk_voice_recovery() -> None:
    planner = Planner()
    steps = [
        planner._step("open_app", args={"app_name": "Settings"}),  # noqa: SLF001
        planner._step("send_notification", args={"title": "Voice Recovery", "message": "Route recovery in progress."}),  # noqa: SLF001
        planner._step("open_url", args={"url": "https://example.com/recovery"}),  # noqa: SLF001
    ]

    adjusted_steps, summary = planner._apply_voice_execution_policy(  # noqa: SLF001
        steps,
        {
            "source": "voice-session",
            "mission_risk_level": "high",
            "voice_interaction_policy": {
                "followup_mode": "hybrid",
                "confirmation_mode": "explicit",
                "prefer_non_voice_completion": True,
                "local_voice_pressure_score": 0.84,
                "pause_pressure": 0.48,
            },
        },
        allowed_actions={"send_notification", "open_url", "open_app"},
    )

    assert summary.get("mission_risk_level") == "high"
    assert summary.get("selected_present_followup_action") == "send_notification"
    assert summary.get("runtime_redirect_action") == "send_notification"
    assert adjusted_steps[1].verify.get("voice_followup_rank") == 1


def test_apply_voice_execution_policy_supports_non_voice_recovery_handoff_context() -> None:
    planner = Planner()
    steps = [
        planner._step("open_app", args={"app_name": "Browser"}),  # noqa: SLF001
        planner._step("open_url", args={"url": "https://example.com/recovery", "title": "Operator recovery handoff"}),  # noqa: SLF001
        planner._step("send_notification", args={"title": "Recovery", "message": "Voice recovery needs operator attention."}),  # noqa: SLF001
    ]

    adjusted_steps, summary = planner._apply_voice_execution_policy(  # noqa: SLF001
        steps,
        {
            "source": "mission_resume",
            "metadata": {
                "voice_recovery_handoff": True,
                "voice_session_id": "voice-session-17",
            },
            "voice_route_policy": {
                "summary": {
                    "status": "blocked",
                    "reason": "Wakeword route is recovering from restart exhaustion.",
                }
            },
            "voice_route_recovery_recommendation": {
                "status": "success",
                "strategy": "notification_handoff",
                "reason": "Operator follow-up is safer while voice recovery is unstable.",
                "risk_level": "high",
                "pause_pressure": 0.58,
                "local_voice_pressure_score": 0.81,
            },
        },
        allowed_actions={"send_notification", "open_url", "open_app"},
    )

    assert summary.get("runtime_redirect_action") == "send_notification"
    assert summary.get("selected_present_followup_action") == "send_notification"
    assert summary.get("mission_risk_level") == "high"
    assert summary.get("planner_followup_contract", {}).get("policy_scope") == "voice_recovery_handoff"
    assert summary.get("planner_followup_contract", {}).get("recovery_handoff_active") is True
    assert summary.get("planner_followup_candidates", [])[0]["channel_reason"] == "high_risk_confirmation_path"
    assert adjusted_steps[2].verify.get("voice_recovery_handoff") is True


def test_screen_text_check_intent_routes_to_computer_assert() -> None:
    planner = Planner()

    text = 'is text visible "Error 404"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "computer_assert_text_visible"
    assert steps and steps[0].action == "computer_assert_text_visible"
    assert steps[0].args.get("text") == "Error 404"


def test_click_text_intent_routes_to_computer_click_target() -> None:
    planner = Planner()

    text = 'click text "Submit"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "computer_click_target"
    assert steps and steps[0].action == "computer_click_target"
    assert steps[0].args.get("query") == "Submit"
    assert steps[0].args.get("target_mode") == "auto"


def test_send_email_intent_routes_to_external_email_send() -> None:
    planner = Planner()

    text = "send email to alice@example.com subject: Status body: done"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "external_email_send"
    assert steps and steps[0].action == "external_email_send"
    recipients = steps[0].args.get("to", [])
    assert isinstance(recipients, list)
    assert "alice@example.com" in recipients


def test_find_ui_element_intent_routes_to_accessibility_find_element() -> None:
    planner = Planner()

    text = 'find ui element "Submit"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "accessibility_find_element"
    assert steps and steps[0].action == "accessibility_find_element"
    assert steps[0].args.get("query") == "Submit"


def test_click_ui_element_intent_routes_to_accessibility_invoke_element() -> None:
    planner = Planner()

    text = 'click ui element "Sign in"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "accessibility_invoke_element"
    assert steps and steps[0].action == "accessibility_invoke_element"
    assert steps[0].args.get("query") == "Sign in"


def test_compound_request_builds_multi_action_plan_without_llm() -> None:
    planner = Planner()

    text = "open https://example.com then extract links from https://example.com"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    actions = [step.action for step in steps]
    assert actions[:2] == ["open_url", "browser_extract_links"]


def test_compound_request_strips_intermediate_tts_acknowledgements() -> None:
    planner = Planner()

    text = "open notepad then list processes"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    actions = [step.action for step in steps]
    assert actions[0] == "open_app"
    assert "list_processes" in actions
    assert actions.count("tts_speak") == 0


def test_compound_request_supports_and_joined_actions() -> None:
    planner = Planner()

    text = "open notepad and list processes and active window"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    actions = [step.action for step in steps]
    assert actions[0] == "open_app"
    assert "list_processes" in actions
    assert "active_window" in actions


def test_compound_desktop_request_inherits_chat_app_context_in_mixed_chain() -> None:
    planner = Planner()

    text = 'open slack and list processes and message Alice saying "Standup in 10 minutes"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert not intent.startswith("compound_parallel_")
    assert len(steps) == 3
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "slack"
    assert steps[1].action == "list_processes"
    assert steps[2].action == "desktop_interact"
    assert steps[2].args.get("app_name") == "slack"
    assert steps[2].args.get("action") == "send_message"
    assert steps[2].args.get("query") == "Alice"
    assert steps[2].args.get("text") == "Standup in 10 minutes"
    assert steps[2].depends_on == [steps[0].step_id]


def test_compound_desktop_request_inherits_office_app_context_in_mixed_chain() -> None:
    planner = Planner()

    text = "open powerpoint and list processes and start presentation"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert not intent.startswith("compound_parallel_")
    assert len(steps) == 3
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "powerpoint"
    assert steps[1].action == "list_processes"
    assert steps[2].action == "desktop_interact"
    assert steps[2].args.get("app_name") == "powerpoint"
    assert steps[2].args.get("action") == "start_presentation"
    assert steps[2].depends_on == [steps[0].step_id]


def test_desktop_interact_step_routes_pause_media_in_media_context() -> None:
    planner = Planner()

    text = "pause playback in spotify"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert len(steps) == 1
    assert steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "spotify"
    assert steps[0].args.get("action") == "pause_media"


def test_desktop_interact_step_routes_find_replace_in_editor_context() -> None:
    planner = Planner()

    text = 'replace "TODO" with "DONE" in vscode'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert len(steps) == 1
    assert steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "vscode"
    assert steps[0].args.get("action") == "find_replace"
    assert steps[0].args.get("query") == "TODO"
    assert steps[0].args.get("text") == "DONE"


def test_desktop_interact_step_routes_focus_search_box_in_browser_context() -> None:
    planner = Planner()

    text = "focus search box in chrome"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert len(steps) == 1
    assert steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[0].args.get("action") == "focus_search_box"


def test_desktop_interact_step_routes_generic_surface_actions_in_settings_context() -> None:
    planner = Planner()

    sidebar_text = "focus sidebar in settings"
    sidebar_intent, sidebar_steps = planner._build_primary_steps(sidebar_text, sidebar_text.lower())  # noqa: SLF001

    content_text = "focus main content in settings"
    content_intent, content_steps = planner._build_primary_steps(content_text, content_text.lower())  # noqa: SLF001

    menu_text = "open context menu in settings"
    menu_intent, menu_steps = planner._build_primary_steps(menu_text, menu_text.lower())  # noqa: SLF001

    assert sidebar_intent == "desktop_interact"
    assert sidebar_steps[0].args.get("app_name") == "settings"
    assert sidebar_steps[0].args.get("action") == "focus_sidebar"
    assert content_intent == "desktop_interact"
    assert content_steps[0].args.get("app_name") == "settings"
    assert content_steps[0].args.get("action") == "focus_main_content"
    assert menu_intent == "desktop_interact"
    assert menu_steps[0].args.get("app_name") == "settings"
    assert menu_steps[0].args.get("action") == "open_context_menu"


def test_desktop_interact_step_routes_generic_item_actions_in_settings_context() -> None:
    planner = Planner()

    sidebar_text = "open bluetooth in settings"
    sidebar_intent, sidebar_steps = planner._build_primary_steps(sidebar_text, sidebar_text.lower())  # noqa: SLF001

    toolbar_text = "click sync in toolbar in settings"
    toolbar_intent, toolbar_steps = planner._build_primary_steps(toolbar_text, toolbar_text.lower())  # noqa: SLF001

    dialog_text = "press continue in dialog in settings"
    dialog_intent, dialog_steps = planner._build_primary_steps(dialog_text, dialog_text.lower())  # noqa: SLF001

    assert sidebar_intent == "desktop_interact"
    assert sidebar_steps[0].args.get("app_name") == "settings"
    assert sidebar_steps[0].args.get("action") == "select_sidebar_item"
    assert sidebar_steps[0].args.get("query") == "bluetooth"
    assert toolbar_intent == "desktop_interact"
    assert toolbar_steps[0].args.get("app_name") == "settings"
    assert toolbar_steps[0].args.get("action") == "invoke_toolbar_action"
    assert toolbar_steps[0].args.get("query") == "sync"
    assert dialog_intent == "desktop_interact"
    assert dialog_steps[0].args.get("app_name") == "settings"
    assert dialog_steps[0].args.get("action") == "press_dialog_button"
    assert dialog_steps[0].args.get("query") == "continue"


def test_desktop_interact_step_routes_tree_and_table_actions_in_admin_apps() -> None:
    planner = Planner()

    tree_text = "expand display adapters in device manager"
    tree_intent, tree_steps = planner._build_primary_steps(tree_text, tree_text.lower())  # noqa: SLF001

    tree_explicit_text = "select windows logs in tree in event viewer"
    tree_explicit_intent, tree_explicit_steps = planner._build_primary_steps(tree_explicit_text, tree_explicit_text.lower())  # noqa: SLF001

    table_text = "select chrome row in task manager"
    table_intent, table_steps = planner._build_primary_steps(table_text, table_text.lower())  # noqa: SLF001

    assert tree_intent == "desktop_interact"
    assert tree_steps[0].args.get("app_name") == "device manager"
    assert tree_steps[0].args.get("action") == "expand_tree_item"
    assert tree_steps[0].args.get("query") == "display adapters"
    assert tree_explicit_intent == "desktop_interact"
    assert tree_explicit_steps[0].args.get("app_name") == "event viewer"
    assert tree_explicit_steps[0].args.get("action") == "select_tree_item"
    assert tree_explicit_steps[0].args.get("query") == "windows logs"
    assert table_intent == "desktop_interact"
    assert table_steps[0].args.get("app_name") == "task manager"
    assert table_steps[0].args.get("action") == "select_table_row"
    assert table_steps[0].args.get("query") == "chrome"


def test_desktop_interact_step_routes_list_surface_actions_in_settings_context() -> None:
    planner = Planner()

    focus_text = "focus list in settings"
    focus_intent, focus_steps = planner._build_primary_steps(focus_text, focus_text.lower())  # noqa: SLF001

    select_text = "select bluetooth in list in settings"
    select_intent, select_steps = planner._build_primary_steps(select_text, select_text.lower())  # noqa: SLF001

    assert focus_intent == "desktop_interact"
    assert focus_steps[0].args.get("app_name") == "settings"
    assert focus_steps[0].args.get("action") == "focus_list_surface"
    assert select_intent == "desktop_interact"
    assert select_steps[0].args.get("app_name") == "settings"
    assert select_steps[0].args.get("action") == "select_list_item"
    assert select_steps[0].args.get("query") == "bluetooth"


def test_desktop_interact_step_routes_form_field_actions_in_settings_context() -> None:
    planner = Planner()

    focus_text = "focus device name field in settings"
    focus_intent, focus_steps = planner._build_primary_steps(focus_text, focus_text.lower())  # noqa: SLF001

    set_text = 'set device name to "JARVIS" in settings'
    set_intent, set_steps = planner._build_primary_steps(set_text, set_text.lower())  # noqa: SLF001

    assert focus_intent == "desktop_interact"
    assert focus_steps[0].args.get("app_name") == "settings"
    assert focus_steps[0].args.get("action") == "focus_input_field"
    assert focus_steps[0].args.get("query") == "device name"
    assert set_intent == "desktop_interact"
    assert set_steps[0].args.get("app_name") == "settings"
    assert set_steps[0].args.get("action") == "set_field_value"
    assert set_steps[0].args.get("query") == "device name"
    assert set_steps[0].args.get("text") == "JARVIS"


def test_desktop_interact_step_routes_dropdown_actions_in_settings_context() -> None:
    planner = Planner()

    open_text = "open language dropdown in settings"
    open_intent, open_steps = planner._build_primary_steps(open_text, open_text.lower())  # noqa: SLF001

    select_text = 'select English in language dropdown in settings'
    select_intent, select_steps = planner._build_primary_steps(select_text, select_text.lower())  # noqa: SLF001

    assert open_intent == "desktop_interact"
    assert open_steps[0].args.get("app_name") == "settings"
    assert open_steps[0].args.get("action") == "open_dropdown"
    assert open_steps[0].args.get("query") == "language"
    assert select_intent == "desktop_interact"
    assert select_steps[0].args.get("app_name") == "settings"
    assert select_steps[0].args.get("action") == "select_dropdown_option"
    assert select_steps[0].args.get("query") == "language"
    assert select_steps[0].args.get("text") == "English"


def test_desktop_interact_step_routes_checkbox_and_toggle_actions_in_settings_context() -> None:
    planner = Planner()

    check_text = "check telemetry checkbox in settings"
    check_intent, check_steps = planner._build_primary_steps(check_text, check_text.lower())  # noqa: SLF001

    uncheck_text = "uncheck notifications checkbox in settings"
    uncheck_intent, uncheck_steps = planner._build_primary_steps(uncheck_text, uncheck_text.lower())  # noqa: SLF001

    toggle_text = "toggle bluetooth switch in settings"
    toggle_intent, toggle_steps = planner._build_primary_steps(toggle_text, toggle_text.lower())  # noqa: SLF001

    assert check_intent == "desktop_interact"
    assert check_steps[0].args.get("app_name") == "settings"
    assert check_steps[0].args.get("action") == "check_checkbox"
    assert check_steps[0].args.get("query") == "telemetry"
    assert uncheck_intent == "desktop_interact"
    assert uncheck_steps[0].args.get("app_name") == "settings"
    assert uncheck_steps[0].args.get("action") == "uncheck_checkbox"
    assert uncheck_steps[0].args.get("query") == "notifications"
    assert toggle_intent == "desktop_interact"
    assert toggle_steps[0].args.get("app_name") == "settings"
    assert toggle_steps[0].args.get("action") == "toggle_switch"
    assert toggle_steps[0].args.get("query") == "bluetooth"


def test_desktop_interact_step_routes_radio_and_value_actions_in_settings_context() -> None:
    planner = Planner()

    radio_text = "select dark mode radio button in settings"
    radio_intent, radio_steps = planner._build_primary_steps(radio_text, radio_text.lower())  # noqa: SLF001

    increase_text = "increase brightness slider by 3 in settings"
    increase_intent, increase_steps = planner._build_primary_steps(increase_text, increase_text.lower())  # noqa: SLF001

    decrease_text = "decrease timeout value by 2 in settings"
    decrease_intent, decrease_steps = planner._build_primary_steps(decrease_text, decrease_text.lower())  # noqa: SLF001

    assert radio_intent == "desktop_interact"
    assert radio_steps[0].args.get("app_name") == "settings"
    assert radio_steps[0].args.get("action") == "select_radio_option"
    assert radio_steps[0].args.get("query") == "dark mode"

    assert increase_intent == "desktop_interact"
    assert increase_steps[0].args.get("app_name") == "settings"
    assert increase_steps[0].args.get("action") == "increase_value"
    assert increase_steps[0].args.get("query") == "brightness"
    assert increase_steps[0].args.get("amount") == 3

    assert decrease_intent == "desktop_interact"
    assert decrease_steps[0].args.get("app_name") == "settings"
    assert decrease_steps[0].args.get("action") == "decrease_value"
    assert decrease_steps[0].args.get("query") == "timeout"
    assert decrease_steps[0].args.get("amount") == 2


def test_desktop_interact_step_routes_absolute_value_and_tab_page_actions() -> None:
    planner = Planner()

    value_text = "set brightness slider to 80 in settings"
    value_intent, value_steps = planner._build_primary_steps(value_text, value_text.lower())  # noqa: SLF001

    tab_text = "switch to security tab in control panel"
    tab_intent, tab_steps = planner._build_primary_steps(tab_text, tab_text.lower())  # noqa: SLF001

    assert value_intent == "desktop_interact"
    assert value_steps[0].args.get("app_name") == "settings"
    assert value_steps[0].args.get("action") == "set_value_control"
    assert value_steps[0].args.get("query") == "brightness"
    assert value_steps[0].args.get("text") == "80"

    assert tab_intent == "desktop_interact"
    assert tab_steps[0].args.get("app_name") == "control panel"
    assert tab_steps[0].args.get("action") == "select_tab_page"
    assert tab_steps[0].args.get("query") == "security"


def test_desktop_interact_step_routes_dialog_control_in_settings_context() -> None:
    planner = Planner()

    dismiss_text = "dismiss dialog in settings"
    dismiss_intent, dismiss_steps = planner._build_primary_steps(dismiss_text, dismiss_text.lower())  # noqa: SLF001

    confirm_text = "confirm dialog in settings"
    confirm_intent, confirm_steps = planner._build_primary_steps(confirm_text, confirm_text.lower())  # noqa: SLF001

    assert dismiss_intent == "desktop_interact"
    assert dismiss_steps[0].args.get("app_name") == "settings"
    assert dismiss_steps[0].args.get("action") == "dismiss_dialog"
    assert confirm_intent == "desktop_interact"
    assert confirm_steps[0].args.get("app_name") == "settings"
    assert confirm_steps[0].args.get("action") == "confirm_dialog"


def test_desktop_interact_step_routes_wizard_actions_for_installer_context() -> None:
    planner = Planner()

    next_text = "go to next step in installer"
    next_intent, next_steps = planner._build_primary_steps(next_text, next_text.lower())  # noqa: SLF001

    back_text = "go back in setup wizard"
    back_intent, back_steps = planner._build_primary_steps(back_text, back_text.lower())  # noqa: SLF001

    finish_text = "finish installer"
    finish_intent, finish_steps = planner._build_primary_steps(finish_text, finish_text.lower())  # noqa: SLF001

    assert next_intent == "desktop_interact"
    assert next_steps[0].args.get("app_name") == "installer"
    assert next_steps[0].args.get("action") == "next_wizard_step"

    assert back_intent == "desktop_interact"
    assert back_steps[0].args.get("app_name") == "setup wizard"
    assert back_steps[0].args.get("action") == "previous_wizard_step"

    assert finish_intent == "desktop_interact"
    assert finish_steps[0].args.get("app_name") == "installer"
    assert finish_steps[0].args.get("action") == "finish_wizard"


def test_desktop_interact_step_routes_complete_wizard_page_for_continue_installer() -> None:
    planner = Planner()

    text = "continue installer"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert steps[0].args.get("app_name") == "installer"
    assert steps[0].args.get("action") == "complete_wizard_page"


def test_desktop_interact_step_routes_complete_wizard_flow_for_setup_flow_commands() -> None:
    planner = Planner()

    continue_text = "continue through installer"
    continue_intent, continue_steps = planner._build_primary_steps(continue_text, continue_text.lower())  # noqa: SLF001

    finish_flow_text = "finish setup flow"
    finish_flow_intent, finish_flow_steps = planner._build_primary_steps(finish_flow_text, finish_flow_text.lower())  # noqa: SLF001

    assert continue_intent == "desktop_interact"
    assert continue_steps[0].args.get("app_name") == "installer"
    assert continue_steps[0].args.get("action") == "complete_wizard_flow"

    assert finish_flow_intent == "desktop_interact"
    assert finish_flow_steps[0].args.get("app_name") == "setup"
    assert finish_flow_steps[0].args.get("action") == "complete_wizard_flow"


def test_desktop_interact_step_routes_complete_form_page_and_flow_for_settings_commands() -> None:
    planner = Planner()

    page_text = "apply settings in settings"
    page_intent, page_steps = planner._build_primary_steps(page_text, page_text.lower())  # noqa: SLF001

    flow_text = "complete settings flow in settings"
    flow_intent, flow_steps = planner._build_primary_steps(flow_text, flow_text.lower())  # noqa: SLF001

    assert page_intent == "desktop_interact"
    assert page_steps[0].args.get("app_name") == "settings"
    assert page_steps[0].args.get("action") == "complete_form_page"

    assert flow_intent == "desktop_interact"
    assert flow_steps[0].args.get("app_name") == "settings"
    assert flow_steps[0].args.get("action") == "complete_form_flow"


def test_desktop_interact_step_routes_resume_mission_by_id_without_target_context() -> None:
    planner = Planner()

    text = "resume mission dm_a1b2c3d4e5f6"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert len(steps) == 1
    assert steps[0].args.get("action") == "resume_mission"
    assert steps[0].args.get("mission_id") == "dm_a1b2c3d4e5f6"
    assert "app_name" not in steps[0].args


def test_desktop_interact_step_routes_resume_blocked_settings_flow_to_resume_mission() -> None:
    planner = Planner()

    text = "continue blocked settings flow"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert len(steps) == 1
    assert steps[0].args.get("action") == "resume_mission"
    assert steps[0].args.get("mission_kind") == "form"
    assert steps[0].args.get("app_name") == "settings"


def test_desktop_interact_step_routes_resume_paused_surface_exploration_to_resume_mission() -> None:
    planner = Planner()

    text = "resume the paused exploration in settings"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert len(steps) == 1
    assert steps[0].args.get("action") == "resume_mission"
    assert steps[0].args.get("mission_kind") == "exploration"
    assert steps[0].args.get("app_name") == "settings"


def test_desktop_interact_step_routes_enable_and_disable_switch_for_settings_commands() -> None:
    planner = Planner()

    enable_text = "turn on bluetooth in settings"
    enable_intent, enable_steps = planner._build_primary_steps(enable_text, enable_text.lower())  # noqa: SLF001

    disable_text = "turn off bluetooth in settings"
    disable_intent, disable_steps = planner._build_primary_steps(disable_text, disable_text.lower())  # noqa: SLF001

    assert enable_intent == "desktop_interact"
    assert enable_steps[0].args.get("app_name") == "settings"
    assert enable_steps[0].args.get("action") == "enable_switch"
    assert enable_steps[0].args.get("query") == "bluetooth"

    assert disable_intent == "desktop_interact"
    assert disable_steps[0].args.get("app_name") == "settings"
    assert disable_steps[0].args.get("action") == "disable_switch"
    assert disable_steps[0].args.get("query") == "bluetooth"


def test_desktop_interact_step_routes_context_menu_item_in_explorer_context() -> None:
    planner = Planner()

    text = "select properties in context menu in explorer"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert len(steps) == 1
    assert steps[0].args.get("app_name") == "explorer"
    assert steps[0].args.get("action") == "select_context_menu_item"
    assert steps[0].args.get("query") == "properties"


def test_desktop_interact_step_routes_rename_selection_in_file_manager_context() -> None:
    planner = Planner()

    text = 'rename selected file to "report-final.txt" in explorer'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert len(steps) == 1
    assert steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "explorer"
    assert steps[0].args.get("action") == "rename_selection"
    assert steps[0].args.get("text") == "report-final.txt"


def test_desktop_interact_step_routes_properties_dialog_in_file_manager_context() -> None:
    planner = Planner()

    text = "open properties in explorer"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert len(steps) == 1
    assert steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "explorer"
    assert steps[0].args.get("action") == "open_properties_dialog"


def test_desktop_interact_step_routes_preview_and_details_panes_in_file_manager_context() -> None:
    planner = Planner()

    preview_text = "open preview pane in explorer"
    preview_intent, preview_steps = planner._build_primary_steps(preview_text, preview_text.lower())  # noqa: SLF001

    details_text = "open details pane in explorer"
    details_intent, details_steps = planner._build_primary_steps(details_text, details_text.lower())  # noqa: SLF001

    assert preview_intent == "desktop_interact"
    assert preview_steps[0].args.get("app_name") == "explorer"
    assert preview_steps[0].args.get("action") == "open_preview_pane"
    assert details_intent == "desktop_interact"
    assert details_steps[0].args.get("app_name") == "explorer"
    assert details_steps[0].args.get("action") == "open_details_pane"


def test_desktop_interact_step_routes_new_chat_in_chat_context() -> None:
    planner = Planner()

    text = "new chat in slack"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert len(steps) == 1
    assert steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "slack"
    assert steps[0].args.get("action") == "new_chat"


def test_desktop_interact_step_routes_new_email_draft_in_mail_context() -> None:
    planner = Planner()

    text = "compose email in outlook"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "desktop_interact"
    assert len(steps) == 1
    assert steps[0].action == "desktop_interact"
    assert steps[0].args.get("app_name") == "outlook"
    assert steps[0].args.get("action") == "new_email_draft"


def test_desktop_interact_step_routes_mail_and_calendar_views_in_mail_context() -> None:
    planner = Planner()

    calendar_text = "open calendar in outlook"
    calendar_intent, calendar_steps = planner._build_primary_steps(calendar_text, calendar_text.lower())  # noqa: SLF001

    mail_text = "open inbox in outlook"
    mail_intent, mail_steps = planner._build_primary_steps(mail_text, mail_text.lower())  # noqa: SLF001

    assert calendar_intent == "desktop_interact"
    assert calendar_steps[0].args.get("app_name") == "outlook"
    assert calendar_steps[0].args.get("action") == "open_calendar_view"
    assert mail_intent == "desktop_interact"
    assert mail_steps[0].args.get("app_name") == "outlook"
    assert mail_steps[0].args.get("action") == "open_mail_view"


def test_desktop_interact_step_routes_people_and_tasks_views_in_mail_context() -> None:
    planner = Planner()

    people_text = "open contacts in outlook"
    people_intent, people_steps = planner._build_primary_steps(people_text, people_text.lower())  # noqa: SLF001

    tasks_text = "open tasks in outlook"
    tasks_intent, tasks_steps = planner._build_primary_steps(tasks_text, tasks_text.lower())  # noqa: SLF001

    assert people_intent == "desktop_interact"
    assert people_steps[0].args.get("app_name") == "outlook"
    assert people_steps[0].args.get("action") == "open_people_view"
    assert tasks_intent == "desktop_interact"
    assert tasks_steps[0].args.get("app_name") == "outlook"
    assert tasks_steps[0].args.get("action") == "open_tasks_view"


def test_desktop_interact_step_routes_reply_forward_and_event_actions_in_mail_context() -> None:
    planner = Planner()

    reply_text = "reply to email in outlook"
    reply_intent, reply_steps = planner._build_primary_steps(reply_text, reply_text.lower())  # noqa: SLF001

    reply_all_text = "reply all in outlook"
    reply_all_intent, reply_all_steps = planner._build_primary_steps(reply_all_text, reply_all_text.lower())  # noqa: SLF001

    forward_text = "forward email in outlook"
    forward_intent, forward_steps = planner._build_primary_steps(forward_text, forward_text.lower())  # noqa: SLF001

    event_text = "new meeting in outlook"
    event_intent, event_steps = planner._build_primary_steps(event_text, event_text.lower())  # noqa: SLF001

    assert reply_intent == "desktop_interact"
    assert reply_steps[0].args.get("app_name") == "outlook"
    assert reply_steps[0].args.get("action") == "reply_email"
    assert reply_all_intent == "desktop_interact"
    assert reply_all_steps[0].args.get("app_name") == "outlook"
    assert reply_all_steps[0].args.get("action") == "reply_all_email"
    assert forward_intent == "desktop_interact"
    assert forward_steps[0].args.get("app_name") == "outlook"
    assert forward_steps[0].args.get("action") == "forward_email"
    assert event_intent == "desktop_interact"
    assert event_steps[0].args.get("app_name") == "outlook"
    assert event_steps[0].args.get("action") == "new_calendar_event"


def test_desktop_interact_step_routes_outlook_pane_focus_actions() -> None:
    planner = Planner()

    folder_text = "focus folder pane in outlook"
    folder_intent, folder_steps = planner._build_primary_steps(folder_text, folder_text.lower())  # noqa: SLF001

    message_text = "focus message list in outlook"
    message_intent, message_steps = planner._build_primary_steps(message_text, message_text.lower())  # noqa: SLF001

    reading_text = "focus reading pane in outlook"
    reading_intent, reading_steps = planner._build_primary_steps(reading_text, reading_text.lower())  # noqa: SLF001

    assert folder_intent == "desktop_interact"
    assert folder_steps[0].args.get("app_name") == "outlook"
    assert folder_steps[0].args.get("action") == "focus_folder_pane"
    assert message_intent == "desktop_interact"
    assert message_steps[0].args.get("app_name") == "outlook"
    assert message_steps[0].args.get("action") == "focus_message_list"
    assert reading_intent == "desktop_interact"
    assert reading_steps[0].args.get("app_name") == "outlook"
    assert reading_steps[0].args.get("action") == "focus_reading_pane"


def test_compound_desktop_request_inherits_media_app_context_in_mixed_chain() -> None:
    planner = Planner()

    text = "open spotify and list processes and next track"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert not intent.startswith("compound_parallel_")
    assert len(steps) == 3
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "spotify"
    assert steps[1].action == "list_processes"
    assert steps[2].action == "desktop_interact"
    assert steps[2].args.get("app_name") == "spotify"
    assert steps[2].args.get("action") == "next_track"
    assert steps[2].depends_on == [steps[0].step_id]


def test_compound_desktop_request_inherits_browser_context_for_named_tab_search() -> None:
    planner = Planner()

    text = 'open chrome and list processes and switch to "OpenAI Docs" tab'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert not intent.startswith("compound_parallel_")
    assert len(steps) == 3
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[1].action == "list_processes"
    assert steps[2].action == "desktop_interact"
    assert steps[2].args.get("app_name") == "chrome"
    assert steps[2].args.get("query") == "OpenAI Docs"
    assert steps[2].args.get("action") == "search_tabs"
    assert steps[2].depends_on == [steps[0].step_id]


def test_compound_desktop_request_depends_on_app_context_step_in_mixed_chain() -> None:
    planner = Planner()

    text = "open chrome and list processes and open new tab"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert not intent.startswith("compound_parallel_")
    assert len(steps) == 3
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "chrome"
    assert steps[1].action == "list_processes"
    assert steps[2].action == "desktop_interact"
    assert steps[2].args.get("app_name") == "chrome"
    assert steps[2].args.get("action") == "new_tab"
    assert steps[2].depends_on == [steps[0].step_id]


def test_compound_desktop_request_inherits_installer_context_for_wizard_followup() -> None:
    planner = Planner()

    text = "open installer and list processes and go to next step"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert not intent.startswith("compound_parallel_")
    assert len(steps) == 3
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "installer"
    assert steps[1].action == "list_processes"
    assert steps[2].action == "desktop_interact"
    assert steps[2].args.get("app_name") == "installer"
    assert steps[2].args.get("action") == "next_wizard_step"
    assert steps[0].step_id in steps[2].depends_on


def test_compound_desktop_request_inherits_installer_context_for_complete_wizard_page() -> None:
    planner = Planner()

    text = "open installer and continue installer"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert len(steps) == 2
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "installer"
    assert steps[1].action == "desktop_interact"
    assert steps[1].args.get("app_name") == "installer"
    assert steps[1].args.get("action") == "complete_wizard_page"
    assert steps[0].step_id in steps[1].depends_on


def test_compound_desktop_request_inherits_installer_context_for_complete_wizard_flow() -> None:
    planner = Planner()

    text = "open installer and list processes and continue through installer"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert len(steps) == 3
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "installer"
    assert steps[1].action == "list_processes"
    assert steps[2].action == "desktop_interact"
    assert steps[2].args.get("app_name") == "installer"
    assert steps[2].args.get("action") == "complete_wizard_flow"
    assert steps[0].step_id in steps[2].depends_on


def test_compound_desktop_request_inherits_settings_context_for_complete_form_flow() -> None:
    planner = Planner()

    text = "open settings and list processes and complete settings flow"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert len(steps) == 3
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "settings"
    assert steps[1].action == "list_processes"
    assert steps[2].action == "desktop_interact"
    assert steps[2].args.get("app_name") == "settings"
    assert steps[2].args.get("action") == "complete_form_flow"
    assert steps[0].step_id in steps[2].depends_on


def test_compound_desktop_request_inherits_installer_context_for_resume_mission() -> None:
    planner = Planner()

    text = "open installer and resume blocked task"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert len(steps) == 2
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "installer"
    assert steps[1].action == "desktop_interact"
    assert steps[1].args.get("app_name") == "installer"
    assert steps[1].args.get("action") == "resume_mission"
    assert steps[1].args.get("mission_kind") == "wizard"
    assert steps[0].step_id in steps[1].depends_on


def test_compound_desktop_request_inherits_settings_context_for_surface_exploration_flow() -> None:
    planner = Planner()

    text = "open settings and list processes and explore surface for bluetooth"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert len(steps) == 3
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "settings"
    assert steps[1].action == "list_processes"
    assert steps[2].action == "desktop_interact"
    assert steps[2].args.get("app_name") == "settings"
    assert steps[2].args.get("query") == "bluetooth"
    assert steps[2].args.get("action") == "complete_surface_exploration_flow"
    assert steps[0].step_id in steps[2].depends_on


def test_compound_desktop_request_builds_settings_change_and_commit_chain() -> None:
    planner = Planner()

    text = "open settings and set brightness slider to 80 and apply settings"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert len(steps) == 3
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "settings"
    assert steps[1].action == "desktop_interact"
    assert steps[1].args.get("app_name") == "settings"
    assert steps[1].args.get("action") == "set_value_control"
    assert steps[1].args.get("query") == "brightness"
    assert steps[1].args.get("text") == "80"
    assert steps[2].action == "desktop_interact"
    assert steps[2].args.get("app_name") == "settings"
    assert steps[2].args.get("action") == "complete_form_page"
    assert steps[0].step_id in steps[1].depends_on
    assert steps[0].step_id in steps[2].depends_on


def test_compound_desktop_request_builds_settings_toggle_and_commit_chain() -> None:
    planner = Planner()

    text = "open settings and turn on bluetooth and apply settings"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert len(steps) == 3
    assert steps[0].action == "open_app"
    assert steps[0].args.get("app_name") == "settings"
    assert steps[1].action == "desktop_interact"
    assert steps[1].args.get("app_name") == "settings"
    assert steps[1].args.get("action") == "enable_switch"
    assert steps[1].args.get("query") == "bluetooth"
    assert steps[2].action == "desktop_interact"
    assert steps[2].args.get("action") == "complete_form_page"


def test_compound_desktop_request_attaches_form_target_plan_to_settings_commit_step() -> None:
    planner = Planner()

    text = "open settings and turn on bluetooth and set brightness slider to 80 and apply settings"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent.startswith("compound_")
    assert len(steps) == 4
    assert steps[3].action == "desktop_interact"
    assert steps[3].args.get("action") == "complete_form_page"
    assert steps[3].args.get("expected_form_target_count") == 2
    form_target_plan = steps[3].args.get("form_target_plan", [])
    assert isinstance(form_target_plan, list)
    assert {
        (
            str(row.get("action", "") or ""),
            str(row.get("query", "") or ""),
            str(row.get("text", "") or ""),
        )
        for row in form_target_plan
        if isinstance(row, dict)
    } == {
        ("enable_switch", "bluetooth", ""),
        ("set_value_control", "brightness", "80"),
    }


def test_replan_uses_failure_category_for_browser_timeout() -> None:
    planner = Planner()

    text = "read webpage https://example.com/docs"
    intent, steps = planner._build_replan_steps(  # noqa: SLF001
        text,
        text.lower(),
        {
            "last_failure_action": "browser_read_dom",
            "last_failure_error": "request timed out",
            "last_failure_category": "timeout",
            "last_failure_attempt": 2,
            "last_failure_retry_count": 1,
        },
    )

    assert intent == "browser_timeout_replan"
    assert len(steps) == 2
    assert steps[0].action == "open_url"
    assert steps[0].args.get("url") == "https://example.com/docs"
    assert steps[1].action == "tts_speak"


def test_replan_click_uses_accessibility_path_when_confirm_policy_fails() -> None:
    planner = Planner()

    text = 'click text "Submit"'
    intent, steps = planner._build_replan_steps(  # noqa: SLF001
        text,
        text.lower(),
        {
            "last_failure_action": "computer_click_target",
            "last_failure_error": "Verification confirm policy failed",
            "last_failure_category": "unknown",
            "last_failure_attempt": 2,
            "last_failure_retry_count": 1,
            "last_failure_confirm_policy": {
                "mode": "all",
                "required": True,
                "satisfied": False,
                "success_count": 0,
                "total_count": 2,
            },
            "execution_feedback": {"quality_score": 0.38},
        },
    )

    assert intent == "computer_click_accessibility_replan"
    assert len(steps) == 3
    assert steps[0].action == "accessibility_find_element"
    assert steps[1].action == "accessibility_invoke_element"


def test_replan_external_connector_adds_oauth_maintenance_for_transient_failures() -> None:
    planner = Planner()

    text = "list emails"
    intent, steps = planner._build_replan_steps(  # noqa: SLF001
        text,
        text.lower(),
        {
            "last_failure_action": "external_email_list",
            "last_failure_error": "request timed out",
            "last_failure_category": "timeout",
            "last_failure_attempt": 2,
            "last_failure_retry_count": 2,
            "mission_feedback": {"recommended_recovery_profile": "safe"},
        },
    )

    assert intent == "external_connector_replan"
    actions = [step.action for step in steps]
    assert actions[0] == "oauth_token_maintain"
    assert "external_connector_preflight" in actions
    assert "external_connector_status" in actions


def test_replan_external_contract_switches_provider_and_retries_action() -> None:
    planner = Planner()

    text = "read document id doc-42"
    intent, steps = planner._build_replan_steps(  # noqa: SLF001
        text,
        text.lower(),
        {
            "last_failure_action": "external_doc_read",
            "last_failure_error": "provider contract failed",
            "last_failure_category": "non_retryable",
            "last_failure_attempt": 2,
            "last_failure_retry_count": 0,
            "last_failure_request": {"args": {"provider": "smtp", "document_id": "doc-42"}},
            "last_failure_external_contract": {
                "code": "provider_not_supported_for_action",
                "allowed_providers": ["google", "graph"],
                "requested_provider": "smtp",
                "remediation_hints": [{"id": "switch_provider"}],
            },
        },
    )

    assert intent == "external_contract_repair_replan"
    actions = [step.action for step in steps]
    assert actions[0] == "external_connector_preflight"
    assert "external_connector_status" in actions
    retry_step = next(step for step in steps if step.action == "external_doc_read")
    assert retry_step.args.get("provider") == "google"
    assert retry_step.args.get("document_id") == "doc-42"


def test_replan_external_auth_contract_runs_maintenance_then_retries() -> None:
    planner = Planner()

    text = "list emails"
    intent, steps = planner._build_replan_steps(  # noqa: SLF001
        text,
        text.lower(),
        {
            "last_failure_action": "external_email_list",
            "last_failure_error": "auth contract failed",
            "last_failure_category": "auth",
            "last_failure_attempt": 2,
            "last_failure_retry_count": 1,
            "last_failure_request": {"args": {"provider": "google", "max_results": 20}},
            "last_failure_external_contract": {
                "code": "auth_preflight_failed",
                "auth_blocked_providers": ["google"],
                "remediation_hints": [{"id": "refresh_access_token"}],
            },
        },
    )

    assert intent == "external_contract_repair_replan"
    actions = [step.action for step in steps]
    assert actions[0] == "oauth_token_maintain"
    assert "external_connector_preflight" in actions
    assert "external_connector_status" in actions
    retry_step = next(step for step in steps if step.action == "external_email_list")
    assert retry_step.args.get("provider") == "google"


def test_replan_external_runtime_reliability_contract_reroutes_provider_and_runs_diagnostics() -> None:
    planner = Planner()

    text = "send email to Alex"
    intent, steps = planner._build_replan_steps(  # noqa: SLF001
        text,
        text.lower(),
        {
            "last_failure_action": "external_email_send",
            "last_failure_error": "provider cooldown active",
            "last_failure_category": "transient",
            "last_failure_attempt": 3,
            "last_failure_retry_count": 1,
            "last_failure_request": {"args": {"provider": "google", "to": "alex@example.com", "subject": "Hi", "body": "Hello"}},
            "last_failure_external_contract": {
                "code": "provider_cooldown_blocked",
                "blocked_providers": ["google"],
                "blocked_ratio": 1.0,
                "retry_after_s": 24.0,
                "checks": [
                    {"check": "provider_cooldown", "status": "failed", "severity": "warning"},
                ],
                "remediation_plan": [
                    {"phase": "repair_dependency", "tool_action": {"action": "oauth_token_maintain", "args": {"provider": "google", "refresh_window_s": 900, "dry_run": False}}},
                    {"phase": "diagnose", "tool_action": {"action": "external_connector_status", "args": {"provider": "google"}}},
                ],
            },
        },
    )

    assert intent == "external_contract_repair_replan"
    actions = [step.action for step in steps]
    assert actions[0] == "external_connector_status"
    assert "external_connector_status" in actions
    assert "oauth_token_maintain" in actions
    assert "external_connector_preflight" in actions
    assert actions.index("oauth_token_maintain") < actions.index("external_connector_preflight")
    retry_step = next(step for step in steps if step.action == "external_email_send")
    assert str(retry_step.args.get("provider", "")).strip().lower() != "google"
    assert retry_step.args.get("to") == "alex@example.com"


def test_replan_external_contract_attaches_confidence_budget_metadata() -> None:
    planner = Planner()

    text = "send email to Alex"
    intent, steps = planner._build_replan_steps(  # noqa: SLF001
        text,
        text.lower(),
        {
            "last_failure_action": "external_email_send",
            "last_failure_error": "provider cooldown active",
            "last_failure_category": "transient",
            "last_failure_attempt": 3,
            "last_failure_retry_count": 1,
            "last_failure_request": {"args": {"provider": "google", "to": "alex@example.com", "subject": "Hi", "body": "Hello"}},
            "external_reliability_mission_analysis": {
                "volatility_mode": "surging",
                "volatility_index": 0.71,
                "at_risk_ratio": 0.58,
            },
            "last_failure_external_contract": {
                "code": "provider_cooldown_blocked",
                "blocked_providers": ["google"],
                "blocked_ratio": 0.82,
                "retry_after_s": 36.0,
                "checks": [
                    {"check": "provider_cooldown", "status": "failed", "severity": "warning"},
                ],
                "remediation_plan": [
                    {"phase": "repair_dependency", "tool_action": {"action": "oauth_token_maintain", "args": {"provider": "google", "refresh_window_s": 900, "dry_run": False}}},
                    {"phase": "diagnose", "tool_action": {"action": "external_connector_status", "args": {"provider": "google"}}},
                ],
            },
        },
    )

    assert intent == "external_contract_repair_replan"
    remediation = next(
        step
        for step in steps
        if step.action in {"external_connector_status", "external_connector_preflight", "oauth_token_maintain"}
        and isinstance(step.verify, dict)
        and "planner_replan_budget_mode" in step.verify
    )
    assert str(remediation.verify.get("planner_replan_budget_mode", "")) in {"guarded", "strict", "stable"}
    retry_step = next(step for step in steps if step.action == "external_email_send")
    assert isinstance(retry_step.verify, dict)
    assert float(retry_step.verify.get("planner_retry_confidence", 0.0) or 0.0) >= 0.0


def test_replan_external_contract_missing_identifier_uses_discovery_step() -> None:
    planner = Planner()

    text = "read email"
    intent, steps = planner._build_replan_steps(  # noqa: SLF001
        text,
        text.lower(),
        {
            "last_failure_action": "external_email_read",
            "last_failure_error": "message_id missing",
            "last_failure_category": "non_retryable",
            "last_failure_attempt": 2,
            "last_failure_retry_count": 0,
            "last_failure_request": {"args": {"provider": "auto"}},
            "last_failure_external_contract": {
                "code": "missing_required_fields",
                "missing_fields": ["message_id"],
                "remediation_hints": [{"id": "provide_required_fields"}],
            },
        },
    )

    assert intent == "external_contract_discovery_replan"
    actions = [step.action for step in steps]
    assert "external_email_list" in actions
    assert "external_email_read" not in actions


def test_replan_external_contract_missing_mutation_field_auto_fills_from_text() -> None:
    planner = Planner()

    text = "complete task task id TASK-99"
    intent, steps = planner._build_replan_steps(  # noqa: SLF001
        text,
        text.lower(),
        {
            "last_failure_action": "external_task_update",
            "last_failure_error": "At least one mutable field is required",
            "last_failure_category": "non_retryable",
            "last_failure_attempt": 2,
            "last_failure_retry_count": 0,
            "last_failure_request": {"args": {"task_id": "TASK-99", "provider": "auto"}},
            "last_failure_external_contract": {
                "code": "missing_any_of_fields",
                "any_of": [["title", "notes", "due", "status"]],
                "remediation_hints": [{"id": "provide_mutation_payload"}],
            },
        },
    )

    assert intent == "external_contract_repair_replan"
    retry_step = next(step for step in steps if step.action == "external_task_update")
    assert retry_step.args.get("task_id") == "TASK-99"
    assert retry_step.args.get("status") == "completed"


def test_replan_external_contract_uses_repair_memory_hint_patch() -> None:
    planner = Planner()

    text = "read email"
    intent, steps = planner._build_replan_steps(  # noqa: SLF001
        text,
        text.lower(),
        {
            "last_failure_action": "external_email_read",
            "last_failure_error": "message_id missing",
            "last_failure_category": "non_retryable",
            "last_failure_attempt": 3,
            "last_failure_retry_count": 1,
            "last_failure_request": {"args": {"provider": "auto"}},
            "last_failure_external_contract": {
                "code": "missing_required_fields",
                "missing_fields": ["message_id"],
            },
            "repair_memory_hints": [
                {
                    "memory_score": 1.2,
                    "signals": [
                        {
                            "action": "external_email_read",
                            "status": "success",
                            "provider": "google",
                            "contract_code": "missing_required_fields",
                            "args": {"message_id": "msg_123", "provider": "google"},
                        }
                    ],
                }
            ],
        },
    )

    assert intent == "external_contract_repair_replan"
    retry_step = next(step for step in steps if step.action == "external_email_read")
    assert retry_step.args.get("message_id") == "msg_123"
    assert retry_step.args.get("provider") == "google"


def test_build_plan_replan_uses_llm_hybrid_when_deterministic_replan_is_not_actionable() -> None:
    planner = Planner()
    planner.llm_enabled = True

    async def fake_llm_plan(*, text: str, context: dict[str, object], allowed_actions: set[str] | None = None):  # noqa: ANN001
        assert "Repair objective" in text
        return (
            "llm_external_contract_repair",
            [
                planner._step(
                    "external_connector_status",
                    args={},
                    verify={"expect_status": "success"},
                )
            ],
            {"provider": "groq", "model": "unit-test-model"},
        )

    planner._build_llm_plan = fake_llm_plan  # type: ignore[method-assign]
    goal = GoalRecord(
        goal_id="goal-llm-replan",
        request=GoalRequest(text="read document", source="desktop-ui", metadata={}),
    )
    plan = asyncio.run(
        planner.build_plan(
            goal,
            context={
                "source": "desktop-ui",
                "replan_attempt": 1,
                "last_failure_action": "external_doc_read",
                "last_failure_error": "contract failed",
                "last_failure_category": "non_retryable",
                "last_failure_request": {"args": {}},
                "last_failure_external_contract": {
                    "code": "invalid_field_type_or_range",
                    "missing_fields": [],
                },
            },
        )
    )

    assert plan.context.get("planner_mode") == "llm_replan_hybrid"
    assert plan.context.get("planner_reason") == "contract_repair_assist"
    assert plan.steps and plan.steps[0].action == "external_connector_status"


def test_build_llm_prompt_includes_external_reliability_trend_context() -> None:
    planner = Planner()
    prompt = planner._build_llm_prompt(  # noqa: SLF001
        text="list unread emails",
        context={
            "source": "desktop-ui",
            "external_reliability_trend": {
                "mode": "worsening",
                "mission_profile": "defensive",
                "trend_pressure": 0.73,
                "top_provider_risks": [{"provider": "google", "risk_score": 0.82}],
            },
        },
        allowed_actions={"external_email_list", "tts_speak"},
    )

    assert '"external_reliability_trend"' in prompt
    assert '"mission_profile": "defensive"' in prompt


def test_backup_and_hash_intent_uses_dependency_placeholder() -> None:
    planner = Planner()

    text = 'backup file and hash "C:/Users/tester/Documents/report.txt"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "backup_and_hash_file"
    assert len(steps) == 2
    assert steps[0].action == "backup_file"
    assert steps[1].action == "hash_file"
    assert steps[0].step_id in steps[1].depends_on
    assert f"steps.{steps[0].step_id}.output.backup_path" in str(steps[1].args.get("path", ""))


def test_copy_and_hash_intent_uses_dependency_placeholder() -> None:
    planner = Planner()

    text = 'copy and hash "C:/Users/tester/Documents/report.txt" "C:/Users/tester/Documents/report_copy.txt"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "copy_and_hash_file"
    assert len(steps) == 2
    assert steps[0].action == "copy_file"
    assert steps[1].action == "hash_file"
    assert steps[0].step_id in steps[1].depends_on
    assert f"steps.{steps[0].step_id}.output.destination" in str(steps[1].args.get("path", ""))


def test_and_split_disambiguation_avoids_single_intent_phrase() -> None:
    planner = Planner()

    text = "play rock and roll music"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert not intent.startswith("compound_")
    assert steps and steps[0].action == "media_search"


def test_open_folder_intent_routes_to_explorer_adapter_tool() -> None:
    planner = Planner()

    text = 'open folder "C:/Users/tester/Documents"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "explorer_open_path"
    assert steps and steps[0].action == "explorer_open_path"


def test_show_file_in_explorer_intent_routes_to_select_tool() -> None:
    planner = Planner()

    text = 'show file in explorer "C:/Users/tester/Documents/report.txt"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "explorer_select_file"
    assert steps and steps[0].action == "explorer_select_file"


def test_connector_preflight_phrase_routes_to_preflight_action() -> None:
    planner = Planner()

    text = "run connector preflight for calendar"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "external_connector_preflight"
    assert steps
    step = steps[0]
    assert step.action == "external_connector_preflight"
    assert str(step.args.get("action", "")) == "external_calendar_create_event"


def test_profile_constraints_fallback_when_requested_action_blocked() -> None:
    planner = Planner()
    planner.llm_enabled = False
    planner.profile_allow_actions = {"sandbox": {"time_now"}}
    planner.profile_deny_actions = {"sandbox": {"open_app"}}
    planner.default_profile_name = ""

    goal = GoalRecord(
        goal_id="goal-1",
        request=GoalRequest(text="open notepad", source="desktop-ui", metadata={"policy_profile": "sandbox"}),
    )
    plan = asyncio.run(planner.build_plan(goal, context={"source": "desktop-ui"}))

    assert plan.intent == "policy_profile_blocked"
    assert len(plan.steps) == 1
    assert plan.steps[0].action == "tts_speak"
    assert "sandbox" in str(plan.steps[0].args.get("text", ""))


def test_profile_constraints_keep_allowed_steps_and_drop_blocked_compound_steps() -> None:
    planner = Planner()
    planner.llm_enabled = False
    planner.profile_allow_actions = {"sandbox": {"open_url"}}
    planner.profile_deny_actions = {"sandbox": {"list_processes"}}
    planner.default_profile_name = ""

    goal = GoalRecord(
        goal_id="goal-2",
        request=GoalRequest(
            text="open https://example.com then list processes",
            source="desktop-ui",
            metadata={"policy_profile": "sandbox"},
        ),
    )
    plan = asyncio.run(planner.build_plan(goal, context={"source": "desktop-ui"}))

    actions = [step.action for step in plan.steps]
    assert actions == ["open_url"]
    assert "list_processes" in plan.context.get("policy_filtered_actions", [])


def test_profile_verification_override_increases_confirm_depth() -> None:
    planner = Planner()
    planner.llm_enabled = False
    planner.profile_allow_actions = {"automation_power": {"open_app", "tts_speak"}}
    planner.profile_deny_actions = {"automation_power": set()}
    planner.default_profile_name = ""

    goal = GoalRecord(
        goal_id="goal-3",
        request=GoalRequest(text="open notepad", source="desktop-ui", metadata={"policy_profile": "automation_power"}),
    )
    plan = asyncio.run(planner.build_plan(goal, context={"source": "desktop-ui"}))
    open_step = next(step for step in plan.steps if step.action == "open_app")
    confirm = open_step.verify.get("confirm", {})

    assert confirm.get("required") is True
    assert confirm.get("attempts") == 3


def test_computer_click_text_template_has_find_confirm() -> None:
    planner = Planner()

    step = planner._step("computer_click_text", args={"query": "Submit"})  # noqa: SLF001
    confirm = step.verify.get("confirm", {})
    checks = step.verify.get("checks", [])

    assert confirm.get("action") == "computer_find_text_targets"
    assert any(item.get("type") == "number_gte" and item.get("key") == "x" for item in checks)


def test_computer_click_target_template_has_method_check_and_find_confirm() -> None:
    planner = Planner()

    step = planner._step("computer_click_target", args={"query": "Submit"})  # noqa: SLF001
    confirm = step.verify.get("confirm", {})
    checks = step.verify.get("checks", [])

    assert confirm.get("action") == "computer_find_text_targets"
    assert any(item.get("type") == "in" and item.get("key") == "method" for item in checks)


def test_reasoning_intent_click_text_maps_to_computer_click_target() -> None:
    planner = Planner()

    step = planner._map_reasoning_intent_to_step(  # noqa: SLF001
        intent="click_text",
        arguments={"query": "Sign in"},
        original_text='click text "Sign in"',
    )

    assert step is not None
    assert step.action == "computer_click_target"
    assert step.args.get("query") == "Sign in"


def test_reasoning_intent_create_document_maps_to_external_doc_create() -> None:
    planner = Planner()

    step = planner._map_reasoning_intent_to_step(  # noqa: SLF001
        intent="create_document",
        arguments={"title": "Weekly Notes", "content": "hello"},
        original_text="create document weekly notes",
    )

    assert step is not None
    assert step.action == "external_doc_create"
    assert step.args.get("title") == "Weekly Notes"


def test_extract_runtime_constraints_parses_steps_time_and_strictness() -> None:
    planner = Planner()

    constraints = planner._extract_runtime_constraints("Finish this within 2 minutes and at most 4 steps with strict verification")  # noqa: SLF001

    assert constraints["time_budget_s"] == 120
    assert constraints["max_steps_hint"] == 4
    assert constraints["verification_strictness"] == "strict"


def test_extract_runtime_constraints_parses_deadline_hint() -> None:
    planner = Planner()

    constraints = planner._extract_runtime_constraints("Complete this by 5:30 pm")  # noqa: SLF001
    deadline_at = str(constraints.get("deadline_at", "")).strip()

    assert deadline_at
    parsed = datetime.fromisoformat(deadline_at.replace("Z", "+00:00"))
    assert parsed.tzinfo is not None
    assert parsed.astimezone(timezone.utc) > datetime.now(timezone.utc)


def test_build_plan_surfaces_runtime_constraints_in_context() -> None:
    planner = Planner()
    planner.llm_enabled = False
    goal = GoalRecord(
        goal_id="goal-runtime-hints",
        request=GoalRequest(
            text="Open notepad within 30 seconds in 1 step with strict verification",
            source="desktop-ui",
            metadata={},
        ),
    )

    plan = asyncio.run(planner.build_plan(goal, context={"source": "desktop-ui"}))
    runtime = plan.context.get("runtime_constraints", {})

    assert isinstance(runtime, dict)
    assert runtime.get("time_budget_s") == 30
    assert runtime.get("max_steps_hint") == 1
    assert runtime.get("verification_strictness") == "strict"


def test_external_email_read_template_checks_message_id_arg() -> None:
    planner = Planner()

    step = planner._step("external_email_read", args={"message_id": "17c9f31c1b"})  # noqa: SLF001
    checks = step.verify.get("checks", [])

    assert any(item.get("type") == "equals_arg" and item.get("key") == "message_id" for item in checks)


def test_external_doc_update_template_checks_document_id_arg() -> None:
    planner = Planner()

    step = planner._step("external_doc_update", args={"document_id": "abc123", "title": "Updated"})  # noqa: SLF001
    checks = step.verify.get("checks", [])

    assert any(item.get("type") == "equals_arg" and item.get("key") == "document_id" for item in checks)


def test_list_emails_intent_routes_to_external_email_list() -> None:
    planner = Planner()

    text = "list emails"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "external_email_list"
    assert steps and steps[0].action == "external_email_list"


def test_list_tasks_intent_routes_to_external_task_list() -> None:
    planner = Planner()

    text = "list tasks"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "external_task_list"
    assert steps and steps[0].action == "external_task_list"
    assert steps[0].args.get("provider") == "auto"


def test_create_task_intent_routes_to_external_task_create() -> None:
    planner = Planner()

    text = 'create task "Pay electricity bill" due: 2026-04-01'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "external_task_create"
    assert steps and steps[0].action == "external_task_create"
    assert steps[0].args.get("title") == "Pay electricity bill"


def test_complete_task_intent_routes_to_external_task_update() -> None:
    planner = Planner()

    text = "complete task id task_12345"
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "external_task_update"
    assert steps and steps[0].action == "external_task_update"
    assert steps[0].args.get("task_id") == "task_12345"
    assert steps[0].args.get("status") == "completed"


def test_external_task_update_template_checks_task_id_arg() -> None:
    planner = Planner()

    step = planner._step("external_task_update", args={"task_id": "task_123", "status": "completed"})  # noqa: SLF001
    checks = step.verify.get("checks", [])

    assert any(item.get("type") == "equals_arg" and item.get("key") == "task_id" for item in checks)


def test_update_calendar_event_intent_routes_to_external_calendar_update_event() -> None:
    planner = Planner()

    text = 'update calendar event "evt_1234" to 2026-04-01T09:00:00Z'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "external_calendar_update_event"
    assert steps and steps[0].action == "external_calendar_update_event"
    assert steps[0].args.get("event_id") == "evt_1234"


def test_read_document_intent_routes_to_external_doc_read() -> None:
    planner = Planner()

    text = 'read document "doc_9988"'
    intent, steps = planner._build_primary_steps(text, text.lower())  # noqa: SLF001

    assert intent == "external_doc_read"
    assert steps and steps[0].action == "external_doc_read"
    assert steps[0].args.get("document_id") == "doc_9988"


def test_episodic_strategy_marks_avoid_actions_with_stronger_confirm() -> None:
    planner = Planner()
    planner.llm_enabled = False

    goal = GoalRecord(
        goal_id="goal-strategy-avoid",
        request=GoalRequest(text='click text "Submit"', source="desktop-ui", metadata={}),
    )
    plan = asyncio.run(
        planner.build_plan(
            goal,
            context={
                "source": "desktop-ui",
                "retrieved_episodic_strategy": {
                    "avoid_actions": [
                        {
                            "action": "computer_click_target",
                            "support": 0.55,
                            "success_rate": 0.2,
                            "failure_rate": 0.9,
                        }
                    ]
                },
            },
        )
    )

    click_step = next(step for step in plan.steps if step.action == "computer_click_target")
    confirm = click_step.verify.get("confirm", {})
    strategy_meta = click_step.verify.get("strategy", {})
    assert confirm.get("required") is True
    assert int(confirm.get("attempts", 0) or 0) >= 3
    assert click_step.max_retries <= 1
    assert strategy_meta.get("memory_avoid_action") is True
    applied = plan.context.get("strategy_applied", {})
    assert "computer_click_target" in applied.get("avoid_actions", [])


def test_episodic_strategy_boosts_recommended_action_retries() -> None:
    planner = Planner()
    planner.llm_enabled = False

    goal = GoalRecord(
        goal_id="goal-strategy-recommended",
        request=GoalRequest(text="list tasks", source="desktop-ui", metadata={}),
    )
    plan = asyncio.run(
        planner.build_plan(
            goal,
            context={
                "source": "desktop-ui",
                "retrieved_episodic_strategy": {
                    "recommended_actions": [
                        {
                            "action": "external_task_list",
                            "support": 0.72,
                            "success_rate": 0.92,
                            "failure_rate": 0.08,
                        }
                    ]
                },
            },
        )
    )

    task_step = next(step for step in plan.steps if step.action == "external_task_list")
    confirm = task_step.verify.get("confirm", {})
    assert confirm.get("required") is False
    assert int(confirm.get("attempts", 0) or 0) >= 2
    assert task_step.max_retries >= 2
    applied = plan.context.get("strategy_applied", {})
    assert "external_task_list" in applied.get("recommended_actions", [])


def test_circuit_context_switches_external_provider_away_from_blocked_scope() -> None:
    planner = Planner()
    planner.llm_enabled = False
    goal = GoalRecord(
        goal_id="goal-circuit-provider",
        request=GoalRequest(text="list emails", source="desktop-ui", metadata={}),
    )

    plan = asyncio.run(
        planner.build_plan(
            goal,
            context={
                "source": "desktop-ui",
                "open_action_circuits": [
                    {"action": "external_email_list", "scope": "graph", "retry_after_s": 60.0},
                ],
                "external_provider_health": [
                    {"provider": "graph", "cooldown_active": True, "retry_after_s": 55.0, "failure_ema": 0.91},
                ],
            },
        )
    )
    step = next(item for item in plan.steps if item.action == "external_email_list")
    assert step.args.get("provider") == "google"
    tuning = plan.context.get("circuit_step_tuning", {})
    assert isinstance(tuning, dict)
    assert tuning.get("provider_switches")


def test_external_provider_candidates_follow_contract_rules() -> None:
    planner = Planner()
    candidates = planner._external_provider_candidates(  # noqa: SLF001
        action="external_email_list",
        args={"provider": "auto"},
    )
    assert candidates == ["google", "graph"]


def test_circuit_context_marks_external_step_strict_when_all_providers_cooling_down() -> None:
    planner = Planner()
    planner.llm_enabled = False
    goal = GoalRecord(
        goal_id="goal-circuit-all-blocked",
        request=GoalRequest(text="list emails", source="desktop-ui", metadata={}),
    )

    plan = asyncio.run(
        planner.build_plan(
            goal,
            context={
                "source": "desktop-ui",
                "open_action_circuits": [
                    {"action": "external_email_list", "scope": "google", "retry_after_s": 40.0},
                    {"action": "external_email_list", "scope": "graph", "retry_after_s": 60.0},
                ],
                "external_provider_health": [
                    {"provider": "google", "cooldown_active": True, "retry_after_s": 40.0, "failure_ema": 0.8},
                    {"provider": "graph", "cooldown_active": True, "retry_after_s": 60.0, "failure_ema": 0.9},
                ],
            },
        )
    )
    step = next(item for item in plan.steps if item.action == "external_email_list")
    external_preflight = step.verify.get("external_preflight", {})
    provider_selection = step.verify.get("provider_selection", {})
    assert external_preflight.get("required") is True
    assert sorted(provider_selection.get("blocked", [])) == ["google", "graph"]
    assert step.max_retries <= 1


def test_circuit_context_prefers_provider_with_lower_operation_risk_even_if_ema_is_higher() -> None:
    planner = Planner()
    planner.llm_enabled = False
    goal = GoalRecord(
        goal_id="goal-circuit-op-risk-switch",
        request=GoalRequest(text="list emails", source="desktop-ui", metadata={}),
    )

    plan = asyncio.run(
        planner.build_plan(
            goal,
            context={
                "source": "desktop-ui",
                "external_provider_health": [
                    {
                        "provider": "google",
                        "cooldown_active": False,
                        "health_score": 0.84,
                        "failure_ema": 0.16,
                        "failure_trend_ema": 0.18,
                        "top_operation_risks": [
                            {"operation": "read", "failure_ema": 0.96, "failure_trend_ema": 0.22, "consecutive_failures": 4}
                        ],
                    },
                    {
                        "provider": "graph",
                        "cooldown_active": False,
                        "health_score": 0.7,
                        "failure_ema": 0.31,
                        "failure_trend_ema": -0.1,
                        "top_operation_risks": [
                            {"operation": "read", "failure_ema": 0.18, "failure_trend_ema": -0.14, "consecutive_failures": 0}
                        ],
                    },
                ],
            },
        )
    )
    step = next(item for item in plan.steps if item.action == "external_email_list")
    assert step.args.get("provider") == "graph"
    provider_selection = step.verify.get("provider_selection", {})
    assert provider_selection.get("operation_class") == "read"
    scores = provider_selection.get("scores", {})
    assert isinstance(scores, dict)
    assert float(scores.get("graph", 1000.0)) < float(scores.get("google", 1000.0))


def test_provider_health_penalty_scales_with_mission_trend_mode() -> None:
    health = {
        "health_score": 0.74,
        "failure_ema": 0.29,
        "failure_trend_ema": 0.36,
        "consecutive_failures": 2,
        "cooldown_active": False,
        "top_operation_risks": [
            {"operation": "read", "failure_ema": 0.42, "failure_trend_ema": 0.22, "consecutive_failures": 2}
        ],
    }
    baseline = Planner._provider_health_penalty(  # noqa: SLF001
        provider="google",
        health=health,
        action="external_email_list",
        mission_trend={},
    )
    worsening = Planner._provider_health_penalty(  # noqa: SLF001
        provider="google",
        health=health,
        action="external_email_list",
        mission_trend={"mode": "worsening", "trend_pressure": 0.72},
    )
    improving = Planner._provider_health_penalty(  # noqa: SLF001
        provider="google",
        health=health,
        action="external_email_list",
        mission_trend={"mode": "improving", "trend_pressure": 0.72},
    )

    assert worsening > baseline
    assert improving < baseline


def test_provider_health_penalty_scales_with_external_reliability_trend() -> None:
    health = {
        "health_score": 0.78,
        "failure_ema": 0.24,
        "failure_trend_ema": 0.18,
        "consecutive_failures": 1,
        "cooldown_active": False,
        "top_operation_risks": [{"operation": "read", "failure_ema": 0.28, "failure_trend_ema": 0.1, "consecutive_failures": 1}],
    }
    baseline = Planner._provider_health_penalty(  # noqa: SLF001
        provider="google",
        health=health,
        action="external_email_list",
        mission_trend={},
        external_trend={},
    )
    worsening = Planner._provider_health_penalty(  # noqa: SLF001
        provider="google",
        health=health,
        action="external_email_list",
        mission_trend={},
        external_trend={
            "mode": "worsening",
            "trend_pressure": 0.78,
            "mission_profile": "defensive",
            "top_provider_risks": [{"provider": "google", "risk_score": 0.81, "cooldown_active": True, "outage_active": False}],
        },
    )
    improving = Planner._provider_health_penalty(  # noqa: SLF001
        provider="google",
        health=health,
        action="external_email_list",
        mission_trend={},
        external_trend={
            "mode": "improving",
            "trend_pressure": 0.78,
            "top_provider_risks": [{"provider": "google", "risk_score": 0.18, "cooldown_active": False, "outage_active": False}],
        },
    )

    assert worsening > baseline
    assert improving < baseline


def test_circuit_context_hardens_external_step_when_external_trend_worsens() -> None:
    planner = Planner()
    step = planner._step(  # noqa: SLF001
        "external_task_update",
        args={"provider": "auto", "task_id": "task-1", "status": "completed"},
        verify={},
        max_retries=4,
        timeout_s=20,
    )
    tuning = planner._apply_circuit_breaker_overrides(  # noqa: SLF001
        [step],
        planning_context={
            "external_provider_health": [
                {"provider": "google", "cooldown_active": False, "health_score": 0.72, "failure_ema": 0.28},
                {"provider": "graph", "cooldown_active": False, "health_score": 0.64, "failure_ema": 0.34},
            ],
            "external_reliability_trend": {
                "mode": "worsening",
                "trend_pressure": 0.82,
                "mission_profile": "defensive",
                "top_provider_risks": [{"provider": "graph", "risk_score": 0.86, "cooldown_active": True}],
            },
        },
    )

    assert isinstance(tuning, dict)
    assert tuning.get("tuned_steps", 0) >= 1
    external_preflight = step.verify.get("external_preflight", {}) if isinstance(step.verify, dict) else {}
    assert isinstance(external_preflight, dict)
    assert external_preflight.get("required") is True
    external_trend = step.verify.get("external_trend", {}) if isinstance(step.verify, dict) else {}
    assert isinstance(external_trend, dict)
    assert str(external_trend.get("mode", "")) == "worsening"
    assert float(external_trend.get("trend_pressure", 0.0) or 0.0) >= 0.8
    assert step.max_retries <= 2
    assert step.timeout_s >= 20
