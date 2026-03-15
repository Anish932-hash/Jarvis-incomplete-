from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


WORKFLOW_CAPABILITY_SPECS: Dict[str, Dict[str, Any]] = {
    "navigate": {
        "field": "navigation_hotkeys",
        "requires_input": True,
        "input_field": "query",
        "default_press_enter": True,
        "fallback_categories": {"browser"},
    },
    "search": {
        "field": "search_hotkeys",
        "requires_input": True,
        "input_field": "query",
        "default_press_enter": False,
        "fallback_categories": {"browser", "chat", "office", "utility", "general_desktop"},
    },
    "focus_search_box": {
        "field": "search_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "chat", "office", "utility", "general_desktop", "code_editor", "ide", "terminal", "file_manager"},
    },
    "command": {
        "field": "command_hotkeys",
        "requires_input": True,
        "input_field": "text",
        "default_press_enter": True,
        "fallback_categories": {"code_editor", "ide"},
    },
    "quick_open": {
        "field": "quick_open_hotkeys",
        "requires_input": True,
        "input_field": "query",
        "default_press_enter": True,
        "fallback_categories": {"code_editor", "ide"},
    },
    "open_bookmarks": {
        "field": "bookmarks_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser"},
    },
    "focus_explorer": {
        "field": "explorer_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"code_editor", "ide"},
    },
    "focus_folder_tree": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"file_manager"},
    },
    "focus_file_list": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"file_manager"},
    },
    "focus_navigation_tree": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"file_manager", "utility", "ops_console", "general_desktop", "security", "office"},
    },
    "focus_list_surface": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"file_manager", "utility", "ops_console", "general_desktop", "chat", "office", "security"},
    },
    "focus_data_table": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"utility", "ops_console", "general_desktop", "security", "office"},
    },
    "focus_sidebar": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "select_sidebar_item": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "focus_main_content": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "focus_toolbar": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "invoke_toolbar_action": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "focus_form_surface": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "focus_input_field": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "set_field_value": {
        "field": "",
        "requires_input": True,
        "input_field": "text",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "open_dropdown": {
        "field": "dropdown_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "select_dropdown_option": {
        "field": "dropdown_hotkeys",
        "requires_input": True,
        "input_field": "text",
        "default_press_enter": True,
        "fallback_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "focus_checkbox": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "check_checkbox": {
        "field": "checkbox_toggle_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "uncheck_checkbox": {
        "field": "checkbox_toggle_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "toggle_switch": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "enable_switch": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "disable_switch": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "select_radio_option": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "focus_value_control": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "increase_value": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "decrease_value": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "set_value_control": {
        "field": "",
        "requires_input": True,
        "input_field": "text",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "select_tab_page": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"file_manager", "office", "utility", "ops_console", "security", "general_desktop", "ai_companion"},
    },
    "select_tree_item": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"file_manager", "utility", "ops_console", "general_desktop", "security", "office"},
    },
    "expand_tree_item": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"file_manager", "utility", "ops_console", "general_desktop", "security", "office"},
    },
    "select_list_item": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"file_manager", "utility", "ops_console", "general_desktop", "chat", "office", "security"},
    },
    "select_table_row": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"utility", "ops_console", "general_desktop", "security", "office"},
    },
    "focus_folder_pane": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"office"},
    },
    "focus_message_list": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"office"},
    },
    "focus_reading_pane": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"office"},
    },
    "workspace_search": {
        "field": "workspace_search_hotkeys",
        "requires_input": True,
        "input_field": "query",
        "default_press_enter": False,
        "fallback_categories": {"code_editor", "ide"},
    },
    "find_replace": {
        "field": "replace_hotkeys",
        "requires_input": True,
        "input_field": "query",
        "default_press_enter": False,
        "fallback_categories": {"code_editor", "ide", "office"},
    },
    "go_to_symbol": {
        "field": "symbol_hotkeys",
        "requires_input": True,
        "input_field": "query",
        "default_press_enter": False,
        "fallback_categories": {"code_editor", "ide"},
    },
    "rename_symbol": {
        "field": "rename_hotkeys",
        "requires_input": True,
        "input_field": "text",
        "default_press_enter": True,
        "fallback_categories": {"code_editor", "ide"},
    },
    "focus_address_bar": {
        "field": "address_bar_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "file_manager"},
    },
    "new_folder": {
        "field": "new_folder_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"file_manager"},
    },
    "rename_selection": {
        "field": "item_rename_hotkeys",
        "requires_input": True,
        "input_field": "text",
        "default_press_enter": True,
        "fallback_categories": {"file_manager"},
    },
    "open_properties_dialog": {
        "field": "properties_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"file_manager"},
    },
    "open_preview_pane": {
        "field": "preview_pane_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"file_manager"},
    },
    "open_details_pane": {
        "field": "details_pane_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"file_manager"},
    },
    "open_context_menu": {
        "field": "context_menu_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "select_context_menu_item": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "dismiss_dialog": {
        "field": "dismiss_dialog_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "confirm_dialog": {
        "field": "confirm_dialog_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "press_dialog_button": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "next_wizard_step": {
        "field": "wizard_next_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
        "supports_action_dispatch_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
    },
    "previous_wizard_step": {
        "field": "wizard_back_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
        "supports_action_dispatch_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
    },
    "finish_wizard": {
        "field": "wizard_finish_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
        "supports_action_dispatch_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
    },
    "complete_wizard_page": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
        "supports_action_dispatch_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
    },
    "complete_wizard_flow": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
    },
    "complete_form_page": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"file_manager", "office", "utility", "ops_console", "security", "general_desktop", "ai_companion"},
        "supports_action_dispatch_categories": {"file_manager", "office", "utility", "ops_console", "security", "general_desktop", "ai_companion"},
    },
    "complete_form_flow": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"file_manager", "office", "utility", "ops_console", "security", "general_desktop", "ai_companion"},
    },
    "refresh_view": {
        "field": "refresh_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "file_manager", "ops_console", "general_desktop"},
    },
    "go_back": {
        "field": "back_navigation_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "file_manager"},
    },
    "go_forward": {
        "field": "forward_navigation_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "file_manager"},
    },
    "go_up_level": {
        "field": "up_level_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"file_manager"},
    },
    "new_tab": {
        "field": "new_tab_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "terminal", "file_manager"},
    },
    "switch_tab": {
        "field": "next_tab_hotkeys",
        "requires_input": True,
        "input_field": "query",
        "default_press_enter": False,
        "fallback_categories": {"browser", "code_editor", "ide", "terminal", "file_manager"},
    },
    "close_tab": {
        "field": "close_tab_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "code_editor", "ide", "file_manager"},
    },
    "reopen_tab": {
        "field": "reopen_tab_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "code_editor", "ide", "file_manager"},
    },
    "open_tab_search": {
        "field": "tab_search_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser"},
    },
    "search_tabs": {
        "field": "tab_search_hotkeys",
        "requires_input": True,
        "input_field": "query",
        "default_press_enter": False,
        "fallback_categories": {"browser"},
    },
    "open_history": {
        "field": "history_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser"},
    },
    "open_downloads": {
        "field": "downloads_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser"},
    },
    "open_devtools": {
        "field": "devtools_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser"},
    },
    "new_chat": {
        "field": "new_chat_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"chat"},
    },
    "jump_to_conversation": {
        "field": "conversation_hotkeys",
        "requires_input": True,
        "input_field": "query",
        "default_press_enter": True,
        "fallback_categories": {"chat"},
    },
    "send_message": {
        "field": "conversation_hotkeys",
        "requires_input": True,
        "input_field": "text",
        "default_press_enter": True,
        "fallback_categories": {"chat"},
        "supports_direct_input_categories": {"chat", "ai_companion"},
    },
    "new_email_draft": {
        "field": "new_email_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": set(),
    },
    "open_mail_view": {
        "field": "mail_view_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": set(),
    },
    "open_calendar_view": {
        "field": "calendar_view_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": set(),
    },
    "open_people_view": {
        "field": "people_view_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": set(),
    },
    "open_tasks_view": {
        "field": "tasks_view_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": set(),
    },
    "reply_email": {
        "field": "reply_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": set(),
    },
    "reply_all_email": {
        "field": "reply_all_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": set(),
    },
    "forward_email": {
        "field": "forward_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": set(),
    },
    "new_calendar_event": {
        "field": "new_calendar_event_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": set(),
    },
    "new_document": {
        "field": "new_document_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"office", "code_editor", "ide"},
    },
    "save_document": {
        "field": "save_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"office", "code_editor", "ide"},
    },
    "open_print_dialog": {
        "field": "print_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"office", "browser", "code_editor", "ide", "general_desktop"},
    },
    "start_presentation": {
        "field": "presentation_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"office"},
    },
    "toggle_terminal": {
        "field": "toggle_terminal_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"code_editor", "ide"},
    },
    "format_document": {
        "field": "format_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"code_editor", "ide"},
    },
    "zoom_in": {
        "field": "zoom_in_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "code_editor", "ide", "office"},
    },
    "zoom_out": {
        "field": "zoom_out_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "code_editor", "ide", "office"},
    },
    "reset_zoom": {
        "field": "reset_zoom_hotkeys",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "fallback_categories": {"browser", "code_editor", "ide", "office"},
    },
    "play_pause_media": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_system_action_categories": {"media"},
    },
    "pause_media": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_system_action_categories": {"media"},
    },
    "resume_media": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_system_action_categories": {"media"},
    },
    "next_track": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_system_action_categories": {"media"},
    },
    "previous_track": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_system_action_categories": {"media"},
    },
    "stop_media": {
        "field": "",
        "requires_input": False,
        "input_field": "",
        "default_press_enter": False,
        "supports_system_action_categories": {"media"},
    },
    "terminal_command": {
        "field": "terminal_hotkeys",
        "requires_input": True,
        "input_field": "text",
        "default_press_enter": True,
        "fallback_categories": {"terminal"},
        "supports_direct_input_categories": {"terminal"},
    },
}


def _dedupe_strings(values: Iterable[str]) -> List[str]:
    rows: List[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip()
        if not clean:
            continue
        key = clean.lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append(clean)
    return rows


def _defaults(
    *,
    target_mode: str,
    verify_mode: str,
    verify_text_source: str,
    capability_preferences: List[str],
    risk_posture: str = "medium",
    max_strategy_attempts: int = 2,
    workflow_defaults: Optional[Dict[str, Any]] = None,
    warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "routing_defaults": {"target_mode": target_mode, "verify_mode": verify_mode},
        "autonomy_defaults": {
            "ensure_app_launch": True,
            "focus_first": True,
            "verify_after_action": True,
            "retry_on_verification_failure": True,
            "max_strategy_attempts": max_strategy_attempts,
        },
        "verification_defaults": {
            "prefer_window_match": True,
            "prefer_text_visibility": verify_text_source in {"typed_text", "query_or_typed"},
            "verify_text_source": verify_text_source,
        },
        "capability_preferences": list(capability_preferences),
        "workflow_defaults": dict(workflow_defaults or {}),
        "risk_posture": risk_posture,
        "warnings": list(warnings or []),
    }


class DesktopAppProfileRegistry:
    DEFAULT_PATHS = (r"E:\apps.txt", r"C:\apps.txt")
    PACKAGE_ID_STOPWORDS = {
        "app", "application", "apps", "arp", "desktop", "exe", "machine", "msix", "store", "user", "users", "winget", "x64", "x86",
    }
    CATEGORY_DEFAULTS: Dict[str, Dict[str, Any]] = {
        "browser": _defaults(
            target_mode="accessibility",
            verify_mode="state_or_visibility",
            verify_text_source="query_or_typed",
            capability_preferences=["accessibility", "vision"],
            max_strategy_attempts=3,
            workflow_defaults={
                "navigation_hotkeys": [["ctrl", "l"], ["alt", "d"]],
                "address_bar_hotkeys": [["ctrl", "l"], ["alt", "d"]],
                "search_hotkeys": [["ctrl", "f"]],
                "tab_search_hotkeys": [["ctrl", "shift", "a"]],
                "refresh_hotkeys": [["f5"], ["ctrl", "r"]],
                "back_navigation_hotkeys": [["alt", "left"]],
                "forward_navigation_hotkeys": [["alt", "right"]],
                "new_tab_hotkeys": [["ctrl", "t"]],
                "next_tab_hotkeys": [["ctrl", "tab"], ["ctrl", "pgdn"]],
                "previous_tab_hotkeys": [["ctrl", "shift", "tab"], ["ctrl", "pgup"]],
                "last_tab_hotkeys": [["ctrl", "9"]],
                "close_tab_hotkeys": [["ctrl", "w"]],
                "reopen_tab_hotkeys": [["ctrl", "shift", "t"]],
                "bookmarks_hotkeys": [["ctrl", "shift", "o"]],
                "history_hotkeys": [["ctrl", "h"]],
                "downloads_hotkeys": [["ctrl", "j"]],
                "devtools_hotkeys": [["f12"], ["ctrl", "shift", "i"]],
                "zoom_in_hotkeys": [["ctrl", "equal"], ["ctrl", "plus"]],
                "zoom_out_hotkeys": [["ctrl", "minus"]],
                "reset_zoom_hotkeys": [["ctrl", "0"]],
            },
        ),
        "file_manager": _defaults(
            target_mode="accessibility",
            verify_mode="state_or_visibility",
            verify_text_source="query_or_typed",
            capability_preferences=["accessibility", "vision"],
            max_strategy_attempts=3,
            workflow_defaults={
                "navigation_hotkeys": [["ctrl", "l"], ["alt", "d"]],
                "address_bar_hotkeys": [["ctrl", "l"], ["alt", "d"]],
                "search_hotkeys": [["ctrl", "e"], ["f3"]],
                "back_navigation_hotkeys": [["alt", "left"]],
                "forward_navigation_hotkeys": [["alt", "right"]],
                "new_tab_hotkeys": [["ctrl", "t"]],
                "next_tab_hotkeys": [["ctrl", "tab"]],
                "previous_tab_hotkeys": [["ctrl", "shift", "tab"]],
                "close_tab_hotkeys": [["ctrl", "w"]],
                "reopen_tab_hotkeys": [["ctrl", "shift", "t"]],
                "new_folder_hotkeys": [["ctrl", "shift", "n"]],
                "item_rename_hotkeys": [["f2"]],
                "properties_hotkeys": [["alt", "enter"]],
                "preview_pane_hotkeys": [["alt", "p"]],
                "details_pane_hotkeys": [["alt", "shift", "p"]],
                "refresh_hotkeys": [["f5"]],
                "up_level_hotkeys": [["alt", "up"]],
            },
            warnings=["File manager views can virtualize folders and recycle slow shell extensions, so verification may need OCR and accessibility together."],
        ),
        "code_editor": _defaults(
            target_mode="accessibility",
            verify_mode="state_or_visibility",
            verify_text_source="typed_text",
            capability_preferences=["accessibility", "vision"],
            workflow_defaults={
                "search_hotkeys": [["ctrl", "f"]],
                "command_hotkeys": [["ctrl", "shift", "p"], ["f1"]],
                "quick_open_hotkeys": [["ctrl", "p"]],
                "explorer_hotkeys": [["ctrl", "shift", "e"]],
                "workspace_search_hotkeys": [["ctrl", "shift", "f"]],
                "replace_hotkeys": [["ctrl", "h"]],
                "symbol_hotkeys": [["ctrl", "shift", "o"]],
                "rename_hotkeys": [["f2"]],
                "next_tab_hotkeys": [["ctrl", "tab"]],
                "previous_tab_hotkeys": [["ctrl", "shift", "tab"]],
                "close_tab_hotkeys": [["ctrl", "w"]],
                "reopen_tab_hotkeys": [["ctrl", "shift", "t"]],
                "terminal_hotkeys": [["ctrl", "`"], ["ctrl", "shift", "`"]],
                "toggle_terminal_hotkeys": [["ctrl", "`"], ["ctrl", "shift", "`"]],
                "format_hotkeys": [["shift", "alt", "f"]],
                "zoom_in_hotkeys": [["ctrl", "equal"], ["ctrl", "plus"]],
                "zoom_out_hotkeys": [["ctrl", "minus"]],
                "reset_zoom_hotkeys": [["ctrl", "0"]],
            },
        ),
        "ide": _defaults(
            target_mode="accessibility",
            verify_mode="state_or_visibility",
            verify_text_source="typed_text",
            capability_preferences=["accessibility", "vision"],
            workflow_defaults={
                "search_hotkeys": [["ctrl", "f"]],
                "command_hotkeys": [["ctrl", "shift", "p"], ["f1"]],
                "quick_open_hotkeys": [["ctrl", "p"]],
                "explorer_hotkeys": [["alt", "1"], ["ctrl", "shift", "e"]],
                "workspace_search_hotkeys": [["ctrl", "shift", "f"]],
                "replace_hotkeys": [["ctrl", "h"], ["ctrl", "r"]],
                "symbol_hotkeys": [["ctrl", "alt", "shift", "n"], ["ctrl", "shift", "o"]],
                "rename_hotkeys": [["shift", "f6"], ["f2"]],
                "next_tab_hotkeys": [["ctrl", "tab"]],
                "previous_tab_hotkeys": [["ctrl", "shift", "tab"]],
                "close_tab_hotkeys": [["ctrl", "w"]],
                "reopen_tab_hotkeys": [["ctrl", "shift", "t"]],
                "terminal_hotkeys": [["ctrl", "`"], ["ctrl", "shift", "`"]],
                "toggle_terminal_hotkeys": [["alt", "f12"], ["ctrl", "`"]],
                "format_hotkeys": [["ctrl", "alt", "l"], ["shift", "alt", "f"]],
                "zoom_in_hotkeys": [["ctrl", "equal"], ["ctrl", "plus"]],
                "zoom_out_hotkeys": [["ctrl", "minus"]],
                "reset_zoom_hotkeys": [["ctrl", "0"]],
            },
        ),
        "terminal": _defaults(
            target_mode="accessibility",
            verify_mode="state_or_visibility",
            verify_text_source="typed_text",
            capability_preferences=["accessibility", "vision"],
            workflow_defaults={
                "search_hotkeys": [["ctrl", "shift", "f"], ["ctrl", "f"]],
                "new_tab_hotkeys": [["ctrl", "shift", "t"], ["ctrl", "t"]],
                "next_tab_hotkeys": [["ctrl", "tab"]],
                "previous_tab_hotkeys": [["ctrl", "shift", "tab"]],
            },
        ),
        "chat": _defaults(
            target_mode="accessibility",
            verify_mode="state_or_visibility",
            verify_text_source="typed_text",
            capability_preferences=["accessibility", "vision"],
            max_strategy_attempts=3,
            workflow_defaults={
                "search_hotkeys": [["ctrl", "f"]],
                "new_chat_hotkeys": [["ctrl", "n"], ["ctrl", "k"], ["ctrl", "e"]],
                "conversation_hotkeys": [["ctrl", "k"], ["ctrl", "e"], ["ctrl", "n"]],
            },
            warnings=["Message and chat clients may contain transient banners, so OCR verification can be noisy."],
        ),
        "office": _defaults(
            target_mode="accessibility",
            verify_mode="state_or_visibility",
            verify_text_source="typed_text",
            capability_preferences=["accessibility", "vision"],
            max_strategy_attempts=3,
            workflow_defaults={
                "search_hotkeys": [["ctrl", "f"]],
                "replace_hotkeys": [["ctrl", "h"]],
                "new_document_hotkeys": [["ctrl", "n"]],
                "save_hotkeys": [["ctrl", "s"]],
                "print_hotkeys": [["ctrl", "p"]],
                "presentation_hotkeys": [["f5"], ["shift", "f5"]],
                "zoom_in_hotkeys": [["ctrl", "equal"], ["ctrl", "plus"]],
                "zoom_out_hotkeys": [["ctrl", "minus"]],
                "reset_zoom_hotkeys": [["ctrl", "0"]],
            },
        ),
        "media": _defaults(target_mode="ocr", verify_mode="hash_changed", verify_text_source="query", capability_preferences=["vision", "accessibility"], max_strategy_attempts=3, warnings=["Media and creative apps often render custom canvases, so OCR fallbacks may be required."]),
        "utility": _defaults(
            target_mode="accessibility",
            verify_mode="state_or_visibility",
            verify_text_source="query",
            capability_preferences=["accessibility", "vision"],
            workflow_defaults={"search_hotkeys": [["ctrl", "f"]]},
        ),
        "ops_console": _defaults(
            target_mode="accessibility",
            verify_mode="state_or_visibility",
            verify_text_source="typed_text",
            capability_preferences=["accessibility", "vision"],
            risk_posture="high",
            workflow_defaults={"search_hotkeys": [["ctrl", "f"]]},
            warnings=["Ops and infrastructure tools can trigger destructive workflows, so verification remains strict."],
        ),
        "security": _defaults(
            target_mode="accessibility",
            verify_mode="state_or_visibility",
            verify_text_source="query",
            capability_preferences=["accessibility", "vision"],
            risk_posture="high",
            workflow_defaults={"search_hotkeys": [["ctrl", "f"]]},
            warnings=["Security and VPN apps may require elevated prompts or protected UI flows."],
        ),
        "remote_support": _defaults(target_mode="ocr", verify_mode="state_or_visibility", verify_text_source="query", capability_preferences=["vision", "accessibility"], risk_posture="high", max_strategy_attempts=3, warnings=["Remote desktop surfaces can duplicate or proxy UI, so OCR-based verification is preferred."]),
        "game": _defaults(target_mode="ocr", verify_mode="hash_changed", verify_text_source="none", capability_preferences=["vision"], risk_posture="high", warnings=["Games often use custom rendering or anti-cheat protections, so direct automation can be flaky."]),
        "ai_companion": _defaults(
            target_mode="accessibility",
            verify_mode="state_or_visibility",
            verify_text_source="typed_text",
            capability_preferences=["accessibility", "vision"],
            workflow_defaults={"search_hotkeys": [["ctrl", "f"]]},
        ),
        "general_desktop": _defaults(
            target_mode="auto",
            verify_mode="state_or_visibility",
            verify_text_source="query_or_typed",
            capability_preferences=["accessibility", "vision"],
            workflow_defaults={"search_hotkeys": [["ctrl", "f"]]},
        ),
    }
    SPECIAL_OVERRIDES: Dict[str, Dict[str, Any]] = {
        "file explorer": {
            "aliases": ["file explorer", "explorer", "windows explorer"],
            "exe_hints": ["explorer.exe"],
            "category": "file_manager",
        },
        "google chrome": {"aliases": ["chrome", "google chrome"], "exe_hints": ["chrome.exe"], "category": "browser"},
        "microsoft edge": {"aliases": ["edge", "microsoft edge"], "exe_hints": ["msedge.exe"], "category": "browser"},
        "mozilla firefox": {"aliases": ["firefox", "mozilla firefox"], "exe_hints": ["firefox.exe"], "category": "browser"},
        "opera": {"aliases": ["opera"], "exe_hints": ["opera.exe"], "category": "browser"},
        "vivaldi": {"aliases": ["vivaldi"], "exe_hints": ["vivaldi.exe"], "category": "browser"},
        "brave": {"aliases": ["brave"], "exe_hints": ["brave.exe"], "category": "browser"},
        "warp": {"aliases": ["warp", "warp terminal"], "exe_hints": ["warp.exe"], "category": "terminal"},
        "cloudflare warp": {"aliases": ["cloudflare warp", "warp vpn"], "exe_hints": ["cloudflarewarp.exe"], "category": "security"},
        "microsoft visual studio code": {"aliases": ["vscode", "visual studio code", "code"], "exe_hints": ["code.exe"], "category": "code_editor"},
        "cursor": {"aliases": ["cursor"], "exe_hints": ["cursor.exe"], "category": "code_editor"},
        "zed": {"aliases": ["zed"], "exe_hints": ["zed.exe"], "category": "code_editor"},
        "notepad plus plus": {"aliases": ["notepad++", "notepad plus plus"], "exe_hints": ["notepad++.exe"], "category": "code_editor"},
        "visual studio community": {"aliases": ["visual studio", "vs"], "exe_hints": ["devenv.exe"], "category": "ide"},
        "pycharm": {"aliases": ["pycharm"], "exe_hints": ["pycharm64.exe"], "category": "ide"},
        "powershell": {"aliases": ["powershell", "pwsh"], "exe_hints": ["pwsh.exe", "powershell.exe"], "category": "terminal"},
        "windows terminal": {"aliases": ["windows terminal", "terminal"], "exe_hints": ["windowsterminal.exe", "wt.exe"], "category": "terminal"},
        "windows settings": {
            "aliases": ["settings", "windows settings", "system settings"],
            "exe_hints": ["systemsettings.exe"],
            "category": "utility",
            "workflow_defaults": {
                "search_hotkeys": [["ctrl", "f"]],
                "next_tab_hotkeys": [["ctrl", "tab"]],
                "previous_tab_hotkeys": [["ctrl", "shift", "tab"]],
            },
        },
        "installer": {
            "aliases": ["installer", "setup", "setup wizard", "installation wizard", "install wizard"],
            "exe_hints": ["setup.exe", "installer.exe", "install.exe"],
            "category": "utility",
            "warnings": ["Installers and setup wizards can trigger system changes, so risky confirmations stay elevated."],
        },
        "task manager": {
            "aliases": ["task manager"],
            "exe_hints": ["taskmgr.exe"],
            "category": "ops_console",
            "workflow_defaults": {
                "search_hotkeys": [["ctrl", "f"]],
                "next_tab_hotkeys": [["ctrl", "tab"]],
                "previous_tab_hotkeys": [["ctrl", "shift", "tab"]],
            },
        },
        "discord": {
            "aliases": ["discord"],
            "exe_hints": ["discord.exe"],
            "category": "chat",
            "workflow_defaults": {
                "new_chat_hotkeys": [["ctrl", "k"]],
                "conversation_hotkeys": [["ctrl", "k"]],
                "search_hotkeys": [["ctrl", "f"]],
            },
        },
        "telegram desktop": {
            "aliases": ["telegram", "telegram desktop"],
            "exe_hints": ["telegram.exe"],
            "category": "chat",
            "workflow_defaults": {
                "new_chat_hotkeys": [["ctrl", "n"], ["ctrl", "k"]],
                "conversation_hotkeys": [["ctrl", "k"], ["ctrl", "f"]],
                "search_hotkeys": [["ctrl", "f"]],
            },
        },
        "whatsapp": {
            "aliases": ["whatsapp"],
            "exe_hints": ["whatsapp.exe"],
            "category": "chat",
            "workflow_defaults": {
                "new_chat_hotkeys": [["ctrl", "n"], ["ctrl", "f"]],
                "conversation_hotkeys": [["ctrl", "n"], ["ctrl", "f"]],
                "search_hotkeys": [["ctrl", "f"]],
            },
        },
        "slack": {
            "aliases": ["slack"],
            "exe_hints": ["slack.exe"],
            "category": "chat",
            "workflow_defaults": {
                "new_chat_hotkeys": [["ctrl", "n"], ["ctrl", "k"]],
                "conversation_hotkeys": [["ctrl", "k"]],
                "search_hotkeys": [["ctrl", "f"]],
            },
        },
        "signal": {
            "aliases": ["signal"],
            "exe_hints": ["signal.exe"],
            "category": "chat",
            "workflow_defaults": {
                "new_chat_hotkeys": [["ctrl", "n"], ["ctrl", "k"]],
                "conversation_hotkeys": [["ctrl", "k"], ["ctrl", "f"]],
                "search_hotkeys": [["ctrl", "f"]],
            },
        },
        "microsoft teams": {
            "aliases": ["teams", "microsoft teams"],
            "exe_hints": ["ms-teams.exe", "teams.exe"],
            "category": "chat",
            "workflow_defaults": {
                "new_chat_hotkeys": [["ctrl", "n"], ["ctrl", "e"]],
                "conversation_hotkeys": [["ctrl", "n"], ["ctrl", "e"]],
                "search_hotkeys": [["ctrl", "f"], ["ctrl", "e"]],
            },
        },
        "proton mail": {
            "aliases": ["proton mail"],
            "exe_hints": ["protonmail.exe"],
            "category": "office",
            "workflow_defaults": {
                "new_email_hotkeys": [["ctrl", "n"]],
            },
        },
        "outlook for windows": {
            "aliases": ["outlook"],
            "exe_hints": ["olk.exe", "outlook.exe"],
            "category": "office",
            "workflow_defaults": {
                "new_email_hotkeys": [["ctrl", "n"]],
                "mail_view_hotkeys": [["ctrl", "1"]],
                "calendar_view_hotkeys": [["ctrl", "2"]],
                "people_view_hotkeys": [["ctrl", "3"]],
                "tasks_view_hotkeys": [["ctrl", "4"]],
                "reply_hotkeys": [["ctrl", "r"]],
                "reply_all_hotkeys": [["ctrl", "shift", "r"]],
                "forward_hotkeys": [["ctrl", "f"]],
                "new_calendar_event_hotkeys": [["ctrl", "shift", "a"]],
            },
        },
        "microsoft word": {
            "aliases": ["word", "microsoft word"],
            "exe_hints": ["winword.exe"],
            "category": "office",
            "workflow_defaults": {
                "new_document_hotkeys": [["ctrl", "n"]],
                "save_hotkeys": [["ctrl", "s"]],
                "print_hotkeys": [["ctrl", "p"]],
            },
        },
        "microsoft excel": {
            "aliases": ["excel", "microsoft excel"],
            "exe_hints": ["excel.exe"],
            "category": "office",
            "workflow_defaults": {
                "new_document_hotkeys": [["ctrl", "n"]],
                "save_hotkeys": [["ctrl", "s"]],
                "print_hotkeys": [["ctrl", "p"]],
            },
        },
        "microsoft powerpoint": {
            "aliases": ["powerpoint", "microsoft powerpoint"],
            "exe_hints": ["powerpnt.exe"],
            "category": "office",
            "workflow_defaults": {
                "new_document_hotkeys": [["ctrl", "n"]],
                "save_hotkeys": [["ctrl", "s"]],
                "print_hotkeys": [["ctrl", "p"]],
                "presentation_hotkeys": [["f5"], ["shift", "f5"]],
            },
        },
        "microsoft onenote": {
            "aliases": ["onenote", "microsoft onenote"],
            "exe_hints": ["onenote.exe"],
            "category": "office",
            "workflow_defaults": {
                "new_document_hotkeys": [["ctrl", "n"]],
                "save_hotkeys": [["ctrl", "s"]],
                "print_hotkeys": [["ctrl", "p"]],
            },
        },
        "docker desktop": {"aliases": ["docker", "docker desktop"], "exe_hints": ["docker.exe"], "category": "ops_console"},
        "vmware workstation": {"aliases": ["vmware", "vmware workstation"], "exe_hints": ["vmware.exe"], "category": "ops_console"},
        "adobe acrobat": {
            "aliases": ["adobe acrobat", "acrobat", "acrobat reader", "adobe reader", "adobe acrobat reader"],
            "exe_hints": ["acrobat.exe", "acrord32.exe"],
            "category": "utility",
            "workflow_defaults": {
                "search_hotkeys": [["ctrl", "f"]],
                "zoom_in_hotkeys": [["ctrl", "equal"], ["ctrl", "plus"]],
                "zoom_out_hotkeys": [["ctrl", "minus"]],
                "reset_zoom_hotkeys": [["ctrl", "0"]],
            },
        },
        "spotify": {"aliases": ["spotify"], "exe_hints": ["spotify.exe"], "category": "media"},
        "vlc media player": {"aliases": ["vlc", "vlc media player"], "exe_hints": ["vlc.exe"], "category": "media"},
        "obs studio": {"aliases": ["obs", "obs studio"], "exe_hints": ["obs64.exe", "obs32.exe"], "category": "media"},
        "anydesk": {"aliases": ["anydesk"], "exe_hints": ["anydesk.exe"], "category": "remote_support"},
        "chatgpt": {"aliases": ["chatgpt"], "exe_hints": ["chatgpt.exe"], "category": "ai_companion"},
        "claude": {"aliases": ["claude"], "exe_hints": ["claude.exe"], "category": "ai_companion"},
        "codex": {"aliases": ["codex"], "exe_hints": ["codex.exe"], "category": "ai_companion"},
        "copilot": {"aliases": ["copilot", "microsoft copilot"], "exe_hints": ["copilot.exe"], "category": "ai_companion"},
        "ollama": {"aliases": ["ollama"], "exe_hints": ["ollama.exe"], "category": "ai_companion"},
        "roblox player": {"aliases": ["roblox", "roblox player"], "exe_hints": ["robloxplayerbeta.exe"], "category": "game"},
        "roblox studio": {"aliases": ["roblox studio"], "exe_hints": ["robloxstudiobeta.exe"], "category": "game"},
        "x minecraft launcher": {"aliases": ["minecraft", "minecraft launcher"], "exe_hints": ["minecraftlauncher.exe"], "category": "game"},
        "tlauncher": {"aliases": ["tlauncher"], "exe_hints": ["tlauncher.exe"], "category": "game"},
    }

    def __init__(self, *, source_paths: Optional[List[str]] = None) -> None:
        self._source_paths = [str(Path(path).expanduser()) for path in (source_paths or list(self.DEFAULT_PATHS)) if str(path or "").strip()]
        self._profile_list: List[Dict[str, Any]] = []
        self._category_counts: Dict[str, int] = {}
        self._loaded = False

    def catalog(self, *, query: str = "", category: str = "", limit: int = 400) -> Dict[str, Any]:
        self._ensure_loaded()
        clean_query = self._normalize_text(query)
        clean_category = self._normalize_text(category)
        rows = [dict(profile) for profile in self._profile_list]
        if clean_category:
            rows = [profile for profile in rows if self._normalize_text(profile.get("category", "")) == clean_category]
        if clean_query:
            rows = [
                profile
                for profile in rows
                if clean_query in self._normalize_text(profile.get("name", ""))
                or any(clean_query in self._normalize_text(alias) for alias in profile.get("aliases", []) if str(alias).strip())
                or any(clean_query in self._normalize_text(package_id) for package_id in profile.get("package_ids", []) if str(package_id).strip())
            ]
        bounded = max(1, min(int(limit or 400), 2000))
        return {
            "status": "success",
            "count": min(len(rows), bounded),
            "total": len(rows),
            "category_counts": dict(self._category_counts),
            "items": rows[:bounded],
            "source_paths": list(self._source_paths),
        }

    def match(self, *, app_name: str = "", window_title: str = "", exe_name: str = "") -> Dict[str, Any]:
        self._ensure_loaded()
        clean_app = self._normalize_text(app_name)
        clean_title = self._normalize_text(window_title)
        clean_exe = self._normalize_text(exe_name)
        best: Dict[str, Any] = {}
        best_score = 0.0
        best_reasons: List[str] = []
        for profile in self._profile_list:
            score, reasons = self._score_profile(profile, app_name=clean_app, window_title=clean_title, exe_name=clean_exe)
            if score <= best_score:
                continue
            best_score = score
            best_reasons = reasons
            best = dict(profile)
            best["match_score"] = round(score, 6)
        if not best:
            fallback = self._fallback_profile(app_name=app_name, window_title=window_title, exe_name=exe_name)
            if fallback:
                fallback["status"] = "success"
                fallback["match_score"] = 0.42
                fallback["match_reasons"] = ["special_override"]
                return fallback
            return {"status": "unmatched", "match_score": 0.0, "match_reasons": []}
        best["status"] = "success"
        best["match_reasons"] = best_reasons
        return best

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        apps: Dict[str, Dict[str, Any]] = {}
        for source_path in self._source_paths:
            path_obj = Path(source_path)
            if not path_obj.exists():
                continue
            try:
                raw_text = path_obj.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            for app_row in self._parse_app_rows(raw_text):
                app_name = str(app_row.get("name", "") or "").strip()
                normalized = self._normalize_text(app_name)
                if not normalized:
                    continue
                row = apps.get(normalized, {"name": app_name, "sources": [], "source_paths": [], "package_ids": [], "versions": [], "available_versions": [], "package_sources": []})
                row["name"] = str(row.get("name", app_name) or app_name)
                row["sources"] = _dedupe_strings(list(row.get("sources", [])) + [path_obj.name])
                row["source_paths"] = _dedupe_strings(list(row.get("source_paths", [])) + [str(path_obj)])
                row["package_ids"] = _dedupe_strings(list(row.get("package_ids", [])) + [str(app_row.get("package_id", "") or "")])
                row["versions"] = _dedupe_strings(list(row.get("versions", [])) + [str(app_row.get("version", "") or "")])
                row["available_versions"] = _dedupe_strings(list(row.get("available_versions", [])) + [str(app_row.get("available", "") or "")])
                row["package_sources"] = _dedupe_strings(list(row.get("package_sources", [])) + [str(app_row.get("source", "") or "")])
                apps[normalized] = row
        profiles: List[Dict[str, Any]] = []
        counts: Dict[str, int] = {}
        for normalized_name, app_row in apps.items():
            profile = self._build_profile(app_name=str(app_row.get("name", normalized_name)), app_row=app_row)
            profiles.append(profile)
            category_name = str(profile.get("category", "general_desktop") or "general_desktop").strip().lower()
            counts[category_name] = int(counts.get(category_name, 0)) + 1
        profiles.sort(key=lambda row: (str(row.get("category", "")), str(row.get("name", "")).lower()))
        self._profile_list = profiles
        self._category_counts = counts
        self._loaded = True

    def _build_profile(self, *, app_name: str, app_row: Dict[str, Any]) -> Dict[str, Any]:
        package_ids = [str(value).strip() for value in app_row.get("package_ids", []) if str(value).strip()]
        canonical_name = self._canonical_name(app_name)
        override = self._special_override(canonical_name, app_name, package_ids)
        category = str(override.get("category", self._infer_category(canonical_name, app_name, package_ids)) or "general_desktop").strip().lower()
        defaults = self.CATEGORY_DEFAULTS.get(category, self.CATEGORY_DEFAULTS["general_desktop"])
        workflow_defaults = self._merge_workflow_defaults(defaults.get("workflow_defaults", {}), override.get("workflow_defaults", {}))
        aliases = _dedupe_strings(
            list(override.get("aliases", []))
            + [app_name, canonical_name]
            + self._keyword_aliases(canonical_name, app_name)
            + self._derived_aliases(canonical_name, app_name)
            + self._package_id_aliases(package_ids)
        )
        exe_hints = _dedupe_strings(list(override.get("exe_hints", [])) + self._package_id_exe_hints(package_ids) + self._exe_hints(canonical_name, aliases))
        return {
            "profile_id": self._slug(aliases[0] if aliases else app_name),
            "name": app_name,
            "canonical_name": canonical_name,
            "category": category,
            "risk_posture": str(defaults.get("risk_posture", "medium") or "medium"),
            "aliases": aliases,
            "exe_hints": exe_hints,
            "window_title_hints": aliases[:10],
            "package_ids": package_ids,
            "versions": [str(value).strip() for value in app_row.get("versions", []) if str(value).strip()],
            "available_versions": [str(value).strip() for value in app_row.get("available_versions", []) if str(value).strip()],
            "package_sources": [str(value).strip() for value in app_row.get("package_sources", []) if str(value).strip()],
            "autonomy_defaults": dict(defaults.get("autonomy_defaults", {})),
            "routing_defaults": dict(defaults.get("routing_defaults", {})),
            "verification_defaults": dict(defaults.get("verification_defaults", {})),
            "capability_preferences": list(defaults.get("capability_preferences", [])),
            "workflow_defaults": dict(workflow_defaults),
            "workflow_capabilities": self._workflow_capabilities(
                category=category,
                workflow_defaults=workflow_defaults,
            ),
            "warnings": _dedupe_strings(list(defaults.get("warnings", [])) + list(override.get("warnings", []))),
            "installed_sources": list(app_row.get("sources", [])),
            "source_paths": list(app_row.get("source_paths", [])),
        }

    def _fallback_profile(self, *, app_name: str, window_title: str, exe_name: str) -> Dict[str, Any]:
        seed_name = str(app_name or "").strip() or str(window_title or "").strip() or str(exe_name or "").strip()
        normalized_seed = self._normalize_text(seed_name)
        normalized_exe = self._normalize_text(exe_name)
        synthetic_package_ids = [value for value in [normalized_exe] if value]
        canonical_name = self._canonical_name(seed_name or normalized_exe or "desktop app")
        override = self._special_override(canonical_name, seed_name, synthetic_package_ids)
        if not override:
            return {}
        category = str(override.get("category", self._infer_category(canonical_name, seed_name, synthetic_package_ids)) or "general_desktop").strip().lower()
        defaults = self.CATEGORY_DEFAULTS.get(category, self.CATEGORY_DEFAULTS["general_desktop"])
        workflow_defaults = self._merge_workflow_defaults(defaults.get("workflow_defaults", {}), override.get("workflow_defaults", {}))
        aliases = _dedupe_strings(
            list(override.get("aliases", []))
            + ([seed_name] if seed_name else [])
            + [canonical_name]
            + self._keyword_aliases(canonical_name, seed_name)
            + self._derived_aliases(canonical_name, seed_name)
            + self._package_id_aliases(synthetic_package_ids)
        )
        exe_hints = _dedupe_strings(
            list(override.get("exe_hints", []))
            + self._package_id_exe_hints(synthetic_package_ids)
            + self._exe_hints(canonical_name, aliases)
        )
        return {
            "profile_id": self._slug(aliases[0] if aliases else seed_name or canonical_name),
            "name": seed_name or canonical_name.title(),
            "canonical_name": canonical_name,
            "category": category,
            "risk_posture": str(defaults.get("risk_posture", "medium") or "medium"),
            "aliases": aliases,
            "exe_hints": exe_hints,
            "window_title_hints": aliases[:10],
            "package_ids": synthetic_package_ids,
            "versions": [],
            "available_versions": [],
            "package_sources": [],
            "autonomy_defaults": dict(defaults.get("autonomy_defaults", {})),
            "routing_defaults": dict(defaults.get("routing_defaults", {})),
            "verification_defaults": dict(defaults.get("verification_defaults", {})),
            "capability_preferences": list(defaults.get("capability_preferences", [])),
            "workflow_defaults": dict(workflow_defaults),
            "workflow_capabilities": self._workflow_capabilities(
                category=category,
                workflow_defaults=workflow_defaults,
            ),
            "warnings": _dedupe_strings(list(defaults.get("warnings", [])) + list(override.get("warnings", []))),
            "installed_sources": [],
            "source_paths": [],
            "synthetic": True,
        }

    @staticmethod
    def _merge_workflow_defaults(base_defaults: Any, override_defaults: Any) -> Dict[str, Any]:
        merged = dict(base_defaults) if isinstance(base_defaults, dict) else {}
        if not isinstance(override_defaults, dict):
            return merged
        for key, value in override_defaults.items():
            clean_key = str(key or "").strip()
            if not clean_key:
                continue
            if isinstance(value, list):
                normalized_rows: List[Any] = []
                for row in value:
                    if isinstance(row, list):
                        normalized_rows.append([str(item).strip().lower() for item in row if str(item).strip()])
                    elif isinstance(row, str) and row.strip():
                        normalized_rows.append([part.strip().lower() for part in re.split(r"[+,]", row) if part.strip()])
                merged[clean_key] = normalized_rows
            else:
                merged[clean_key] = value
        return merged

    @staticmethod
    def _workflow_capabilities(*, category: str, workflow_defaults: Any) -> Dict[str, Any]:
        clean_category = str(category or "").strip().lower()
        defaults = workflow_defaults if isinstance(workflow_defaults, dict) else {}
        capabilities: Dict[str, Any] = {}
        for action, spec in WORKFLOW_CAPABILITY_SPECS.items():
            field_name = str(spec.get("field", "") or "").strip()
            hotkeys = defaults.get(field_name, []) if field_name else []
            hotkey_rows = [list(row) for row in hotkeys if isinstance(row, list) and row]
            if not hotkey_rows and action == "open_context_menu" and clean_category in {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
                hotkey_rows = [["shift", "f10"], ["apps"]]
            if not hotkey_rows and action == "dismiss_dialog" and clean_category in {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
                hotkey_rows = [["esc"]]
            if not hotkey_rows and action == "confirm_dialog" and clean_category in {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
                hotkey_rows = [["enter"]]
            if not hotkey_rows and action == "next_wizard_step" and clean_category in {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"}:
                hotkey_rows = [["alt", "n"], ["enter"]]
            if not hotkey_rows and action == "previous_wizard_step" and clean_category in {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"}:
                hotkey_rows = [["alt", "b"]]
            if not hotkey_rows and action == "finish_wizard" and clean_category in {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"}:
                hotkey_rows = [["alt", "f"], ["enter"]]
            if not hotkey_rows and action in {"open_dropdown", "select_dropdown_option"} and clean_category in {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
                hotkey_rows = [["alt", "down"]]
            if not hotkey_rows and action in {"check_checkbox", "uncheck_checkbox"} and clean_category in {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
                hotkey_rows = [["space"]]
            fallback_categories = {
                str(item).strip().lower()
                for item in spec.get("fallback_categories", set())
                if str(item).strip()
            }
            supports_direct_input_categories = {
                str(item).strip().lower()
                for item in spec.get("supports_direct_input_categories", set())
                if str(item).strip()
            }
            supports_system_action_categories = {
                str(item).strip().lower()
                for item in spec.get("supports_system_action_categories", set())
                if str(item).strip()
            }
            supports_action_dispatch_categories = {
                str(item).strip().lower()
                for item in spec.get("supports_action_dispatch_categories", set())
                if str(item).strip()
            }
            supports_system_action = clean_category in supports_system_action_categories
            supports_action_dispatch = clean_category in supports_action_dispatch_categories
            supported = (
                bool(hotkey_rows)
                or clean_category in fallback_categories
                or clean_category in supports_direct_input_categories
                or supports_system_action
                or supports_action_dispatch
            )
            capabilities[action] = {
                "supported": supported,
                "hotkey_count": len(hotkey_rows),
                "primary_hotkey": list(hotkey_rows[0]) if hotkey_rows else [],
                "requires_input": bool(spec.get("requires_input", False)),
                "input_field": str(spec.get("input_field", "") or "").strip(),
                "default_press_enter": bool(spec.get("default_press_enter", False)),
                "supports_direct_input": clean_category in supports_direct_input_categories,
                "supports_system_action": supports_system_action,
                "supports_action_dispatch": supports_action_dispatch,
            }
        return capabilities

    def _special_override(self, canonical_name: str, app_name: str, package_ids: List[str]) -> Dict[str, Any]:
        exact_candidates = {canonical_name, self._normalize_text(app_name), *[self._normalize_text(package_id) for package_id in package_ids]}
        for key, override in self.SPECIAL_OVERRIDES.items():
            override_candidates = {
                self._normalize_text(key),
                *[self._normalize_text(alias) for alias in override.get("aliases", []) if str(alias).strip()],
                *[self._normalize_text(exe_name) for exe_name in override.get("exe_hints", []) if str(exe_name).strip()],
            }
            if any(candidate in exact_candidates for candidate in override_candidates if candidate):
                return dict(override)
        haystack = " ".join(sorted(value for value in exact_candidates if value))
        for key, override in self.SPECIAL_OVERRIDES.items():
            override_candidates = {
                self._normalize_text(key),
                *[self._normalize_text(alias) for alias in override.get("aliases", []) if str(alias).strip()],
                *[self._normalize_text(exe_name) for exe_name in override.get("exe_hints", []) if str(exe_name).strip()],
            }
            if any(candidate and candidate in haystack for candidate in override_candidates):
                return dict(override)
        return {}

    def _infer_category(self, canonical_name: str, app_name: str, package_ids: List[str]) -> str:
        haystack = " ".join(value for value in [canonical_name, self._normalize_text(app_name)] + [self._normalize_text(package_id) for package_id in package_ids] if value)
        if any(keyword in haystack for keyword in ("roblox", "minecraft", "krunker", "launcher", "xbox", "solitaire", "warships", "steam", "tlauncher")):
            return "game"
        if any(keyword in haystack for keyword in ("cloudflare warp", "vpn", "security", "defender", "antivirus", "firewall", "authenticator")):
            return "security"
        if any(keyword in haystack for keyword in ("anydesk", "quick assist", "teamviewer", "remote desktop")):
            return "remote_support"
        if any(keyword in haystack for keyword in ("chrome", "edge", "brave", "browser", "firefox", "opera", "vivaldi")):
            return "browser"
        if any(keyword in haystack for keyword in ("file explorer", "windows explorer", "explorer.exe")):
            return "file_manager"
        if any(keyword in haystack for keyword in ("visual studio code", "vscode", "notepad", "notepad++", "sublime", "zed", "cursor")):
            return "code_editor"
        if any(keyword in haystack for keyword in ("pycharm", "visual studio", "intellij", "android studio", "webstorm", "rider", "claude code")):
            return "ide"
        if any(keyword in haystack for keyword in ("powershell", "pwsh", "windows terminal", "terminal", "command prompt", "cmd", "hyper", "tabby")):
            return "terminal"
        if any(keyword in haystack for keyword in ("discord", "telegram", "teams", "whatsapp", "signal", "slack")):
            return "chat"
        if any(keyword in haystack for keyword in ("word", "excel", "powerpoint", "office", "outlook", "mail", "calendar", "onenote", "sticky notes", "to do", "proton mail")):
            return "office"
        if any(keyword in haystack for keyword in ("screen recorder", "loom", "medal", "photos", "paint", "clipchamp", "sound recorder", "media player", "freetube", "youtube", "vlc", "obs", "fxsound", "spotify")):
            return "media"
        if any(keyword in haystack for keyword in ("dev home", "docker", "vmware", "wsl", "build tools", "sdk", "git", "github desktop", "postman", "insomnia", "virtualbox")):
            return "ops_console"
        if any(keyword in haystack for keyword in ("chatgpt", "claude", "codex", "copilot", "jarvis", "ollama", "hackerai", "wispr", "firebase studio", "jioai", "antigravity")):
            return "ai_companion"
        if any(keyword in haystack for keyword in ("zip", "recuva", "everything", "onedrive", "gopeed", "torrent", "rufus", "terabox", "installer", "dropbox", "drive", "calculator", "store", "camera", "clock", "feedback hub", "snipping tool", "myasus", "lively wallpaper", "realtek audio")):
            return "utility"
        return "general_desktop"

    @staticmethod
    def _parse_app_rows(raw_text: str) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        seen: set[Tuple[str, str]] = set()
        for line in str(raw_text or "").splitlines():
            stripped = line.rstrip()
            if not stripped.strip():
                continue
            clean = stripped.strip()
            if clean in {"-", "\\", "/"} or set(clean) == {"-"}:
                continue
            parts = [part.strip() for part in re.split(r"\s{2,}", stripped) if part.strip()]
            if not parts or parts[0].lower() == "name":
                continue
            if len(parts) == 1 and len(parts[0]) < 3:
                continue
            row = {
                "name": str(parts[0] or "").strip(),
                "package_id": str(parts[1] if len(parts) > 1 else "").strip(),
                "version": str(parts[2] if len(parts) > 2 else "").strip(),
                "available": str(parts[3] if len(parts) > 3 else "").strip(),
                "source": str(parts[4] if len(parts) > 4 else "").strip(),
            }
            if not row["name"]:
                continue
            dedupe_key = (DesktopAppProfileRegistry._normalize_text(row["name"]), DesktopAppProfileRegistry._normalize_text(row["package_id"]))
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(row)
        return rows

    @staticmethod
    def _canonical_name(app_name: str) -> str:
        text = DesktopAppProfileRegistry._normalize_text(app_name)
        text = re.sub(r"\bversion\b.*$", "", text).strip()
        text = re.sub(r"\b\d+(?:\.\d+)+(?:\.\d+)?\b", "", text).strip()
        text = re.sub(r"\b(x64|x86|64-bit|32-bit|user)\b", "", text).strip()
        return re.sub(r"\s+", " ", text).strip() or DesktopAppProfileRegistry._normalize_text(app_name)

    @staticmethod
    def _keyword_aliases(canonical_name: str, app_name: str) -> List[str]:
        aliases = [canonical_name]
        normalized_name = DesktopAppProfileRegistry._normalize_text(app_name)
        if "visual studio code" in normalized_name:
            aliases.extend(["vscode", "code"])
        if "visual studio community" in normalized_name:
            aliases.extend(["visual studio", "vs"])
        if "google chrome" in normalized_name:
            aliases.append("chrome")
        if "microsoft edge" in normalized_name:
            aliases.append("edge")
        if "windows terminal" in normalized_name:
            aliases.append("terminal")
        if "powershell" in normalized_name:
            aliases.append("pwsh")
        return aliases

    @staticmethod
    def _derived_aliases(canonical_name: str, app_name: str) -> List[str]:
        aliases: List[str] = []
        candidates = [canonical_name, DesktopAppProfileRegistry._normalize_text(app_name)]
        prefixes = ("microsoft ", "windows ", "google ", "adobe ")
        suffixes = (" for windows", " desktop", " app", " application")
        for candidate in candidates:
            clean_candidate = DesktopAppProfileRegistry._normalize_text(candidate)
            if not clean_candidate:
                continue
            aliases.append(clean_candidate)
            for prefix in prefixes:
                if clean_candidate.startswith(prefix):
                    aliases.append(clean_candidate[len(prefix):].strip())
            for suffix in suffixes:
                if clean_candidate.endswith(suffix):
                    aliases.append(clean_candidate[: -len(suffix)].strip())
            trimmed = clean_candidate
            for prefix in prefixes:
                if trimmed.startswith(prefix):
                    trimmed = trimmed[len(prefix):].strip()
            for suffix in suffixes:
                if trimmed.endswith(suffix):
                    trimmed = trimmed[: -len(suffix)].strip()
            if trimmed and trimmed != clean_candidate:
                aliases.append(trimmed)
        return _dedupe_strings(aliases)

    def _package_id_aliases(self, package_ids: List[str]) -> List[str]:
        aliases: List[str] = []
        for package_id in package_ids:
            normalized_id = self._normalize_text(package_id)
            if not normalized_id:
                continue
            aliases.append(normalized_id.replace(".", " "))
            parts = [part for part in re.split(r"[./_\\-]+", str(package_id or "").strip()) if part.strip()]
            cleaned = [self._humanize_identifier(part) for part in parts if self._normalize_text(part) not in self.PACKAGE_ID_STOPWORDS]
            if cleaned and (len(cleaned) == 1 or cleaned[-1] == cleaned[-2]):
                aliases.append(cleaned[-1])
            if len(cleaned) >= 2:
                aliases.append(" ".join(cleaned[-2:]))
        return _dedupe_strings(aliases)

    def _package_id_exe_hints(self, package_ids: List[str]) -> List[str]:
        rows: List[str] = []
        for package_id in package_ids:
            parts = [part for part in re.split(r"[./_\\-]+", str(package_id or "").strip()) if part.strip()]
            cleaned = [part for part in parts if self._normalize_text(part) not in self.PACKAGE_ID_STOPWORDS]
            if not cleaned:
                continue
            candidate = cleaned[-2] if cleaned[-1].lower() == "exe" and len(cleaned) >= 2 else cleaned[-1]
            candidate_slug = self._slug(self._humanize_identifier(candidate))
            if candidate_slug:
                rows.append(f"{candidate_slug}.exe")
        return _dedupe_strings(rows)

    @staticmethod
    def _exe_hints(canonical_name: str, aliases: List[str]) -> List[str]:
        rows: List[str] = []
        slug = DesktopAppProfileRegistry._slug(canonical_name)
        if slug:
            rows.append(f"{slug}.exe")
        for alias in aliases:
            alias_slug = DesktopAppProfileRegistry._slug(alias)
            if alias_slug:
                rows.append(f"{alias_slug}.exe")
        return rows

    def _score_profile(self, profile: Dict[str, Any], *, app_name: str, window_title: str, exe_name: str) -> Tuple[float, List[str]]:
        score = 0.0
        reasons: List[str] = []
        aliases = [self._normalize_text(alias) for alias in profile.get("aliases", []) if str(alias).strip()]
        title_hints = [self._normalize_text(alias) for alias in profile.get("window_title_hints", []) if str(alias).strip()]
        exe_hints = [self._normalize_text(alias) for alias in profile.get("exe_hints", []) if str(alias).strip()]
        package_ids = [self._normalize_text(package_id) for package_id in profile.get("package_ids", []) if str(package_id).strip()]

        def promote(next_score: float, reason: str) -> None:
            nonlocal score
            if next_score <= score:
                return
            score = next_score
            if reason not in reasons:
                reasons.append(reason)

        for alias in aliases:
            if app_name and app_name == alias:
                promote(1.0, "alias_exact")
            elif app_name and alias and (alias in app_name or app_name in alias):
                promote(0.9, "alias_partial")
            if window_title and alias and alias in window_title:
                promote(0.82, "window_alias")
        for hint in title_hints:
            if window_title and hint and hint in window_title:
                promote(0.8, "window_hint")
        for hint in exe_hints:
            if exe_name and hint == exe_name:
                promote(0.98, "exe_exact")
            elif exe_name and hint and hint in exe_name:
                promote(0.88, "exe_partial")
        for package_id in package_ids:
            if app_name and package_id and package_id in app_name:
                promote(0.84, "package_id")
            if window_title and package_id and package_id in window_title:
                promote(0.74, "package_window")
        return round(score, 6), reasons

    @staticmethod
    def _humanize_identifier(value: str) -> str:
        text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(value or "").strip())
        text = re.sub(r"[^A-Za-z0-9]+", " ", text)
        return re.sub(r"\s+", " ", text).strip().lower()

    @staticmethod
    def _normalize_text(value: Any) -> str:
        text = str(value or "").strip().lower()
        text = re.sub(r"[^a-z0-9.+#]+", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _slug(value: str) -> str:
        text = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower())
        return text.strip("_")
