from __future__ import annotations

import hashlib
import json
import re
import time
from typing import Any, Callable, Dict, List, Optional

from backend.python.core.desktop_app_profile_registry import DesktopAppProfileRegistry
from backend.python.core.desktop_mission_memory import DesktopMissionMemory
from backend.python.core.desktop_workflow_memory import DesktopWorkflowMemory
from backend.python.perception.surface_intelligence import SurfaceIntelligenceAnalyzer


ActionHandler = Callable[[Dict[str, Any]], Dict[str, Any]]
EXPLORATION_ADVANCE_ACTION = "advance_surface_exploration"
EXPLORATION_FLOW_ACTION = "complete_surface_exploration_flow"
RESUMEABLE_MISSION_ACTIONS = {"complete_wizard_flow", "complete_form_flow", EXPLORATION_ADVANCE_ACTION, EXPLORATION_FLOW_ACTION}
RESUME_APPROVAL_KINDS = {
    "elevation_consent",
    "elevation_credentials",
    "credential_input",
    "authentication_review",
    "permission_review",
}

WORKFLOW_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "navigate": {
        "title": "Navigate",
        "category_hints": {"browser"},
        "hotkey_field": "navigation_hotkeys",
        "input_field": "query",
        "requires_input": True,
        "default_press_enter": True,
        "route_mode": "workflow_navigation",
        "missing_input_message": "query is required for navigation workflows.",
        "support_message": "No navigation workflow is configured for this app. Browser profiles expose address-bar navigation shortcuts.",
        "hotkey_reason": "Move focus to the app's address or destination field before typing the target.",
        "input_reason": "Type the requested destination and submit it through the current desktop app.",
        "retry_label": "Address Bar Retry",
        "retry_reason": "Retry with an alternate address-bar shortcut for apps that remap navigation focus.",
        "verification_success": "navigation verified",
        "verification_failure": "navigation finished, but JARVIS could not confirm the destination was reached",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful navigation workflow as best-effort confirmation.",
        "surface_flag": "address_bar_ready",
        "skip_hotkey_when_ready": True,
    },
    "search": {
        "title": "Search",
        "category_hints": {"browser", "chat", "office", "utility", "general_desktop", "code_editor", "ide", "terminal"},
        "hotkey_field": "search_hotkeys",
        "input_field": "query",
        "requires_input": True,
        "default_press_enter": False,
        "route_mode": "workflow_search",
        "missing_input_message": "query is required for desktop search workflows.",
        "support_message": "No in-app search workflow is configured for this app.",
        "hotkey_reason": "Open the app's in-context search surface before typing the requested query.",
        "input_reason": "Type the requested search query into the app's active search surface.",
        "retry_label": "Search Retry",
        "retry_reason": "Retry with an alternate search shortcut for apps with custom find bindings.",
        "verification_success": "search verified",
        "verification_failure": "search finished, but the follow-up search state could not be confirmed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful search workflow as best-effort confirmation.",
        "probe_terms": ["search", "find"],
        "recommended_followups": ["quick_open", "command"],
        "surface_flag": "search_visible",
        "skip_hotkey_when_ready": True,
    },
    "focus_search_box": {
        "title": "Focus Search Box",
        "category_hints": {"browser", "chat", "office", "utility", "general_desktop", "code_editor", "ide", "terminal", "file_manager"},
        "hotkey_field": "search_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_search_box",
        "missing_input_message": "",
        "support_message": "No search-surface workflow is configured for this app.",
        "hotkey_reason": "Focus the app's active search or find surface before a follow-up query or inspection action.",
        "input_reason": "",
        "retry_label": "Search Surface Retry",
        "retry_reason": "Retry with an alternate search shortcut for apps that remap find or search bindings.",
        "verification_success": "search surface verified",
        "verification_failure": "search focus finished, but JARVIS could not confirm the search surface became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful search-surface workflow as best-effort confirmation.",
        "verify_hint": "search",
        "probe_terms": ["search", "find"],
        "recommended_followups": ["search", "quick_open", "command"],
        "surface_flag": "search_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "command": {
        "title": "Command Palette",
        "category_hints": {"code_editor", "ide"},
        "hotkey_field": "command_hotkeys",
        "input_field": "text",
        "requires_input": True,
        "default_press_enter": True,
        "route_mode": "workflow_command_palette",
        "missing_input_message": "text is required for command palette workflows.",
        "support_message": "No command palette workflow is configured for this app. IDE-style profiles expose command shortcuts.",
        "hotkey_reason": "Open the app's command palette before dispatching the requested command.",
        "input_reason": "Type the requested command into the app's command palette.",
        "retry_label": "Command Palette Retry",
        "retry_reason": "Retry with an alternate command-palette shortcut for IDE-style apps.",
        "verification_success": "command verified",
        "verification_failure": "command palette finished, but the resulting UI state could not be confirmed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful command palette workflow as best-effort confirmation.",
        "probe_terms": ["command palette", "command"],
        "recommended_followups": ["quick_open", "go_to_symbol"],
        "surface_flag": "command_palette_visible",
        "skip_hotkey_when_ready": True,
    },
    "quick_open": {
        "title": "Quick Open",
        "category_hints": {"code_editor", "ide"},
        "hotkey_field": "quick_open_hotkeys",
        "input_field": "query",
        "requires_input": True,
        "default_press_enter": True,
        "route_mode": "workflow_quick_open",
        "missing_input_message": "query is required for quick-open workflows.",
        "support_message": "No quick-open workflow is configured for this app. Editor-style profiles expose file switcher shortcuts.",
        "hotkey_reason": "Open the app's quick-open surface before typing the requested file, symbol, or workspace target.",
        "input_reason": "Type the requested file, symbol, or workspace target into quick open.",
        "retry_label": "Quick Open Retry",
        "retry_reason": "Retry with an alternate quick-open shortcut for editor-style apps.",
        "verification_success": "quick open verified",
        "verification_failure": "quick open finished, but the requested target could not be confirmed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful quick-open workflow as best-effort confirmation.",
        "probe_terms": ["quick open", "open file"],
        "recommended_followups": ["go_to_symbol", "workspace_search"],
        "surface_flag": "quick_open_visible",
        "skip_hotkey_when_ready": True,
    },
    "focus_address_bar": {
        "title": "Focus Address Bar",
        "category_hints": {"browser", "file_manager"},
        "hotkey_field": "address_bar_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_address_bar",
        "missing_input_message": "",
        "support_message": "No address-bar workflow is configured for this app.",
        "hotkey_reason": "Focus the active address or location field before a follow-up navigation workflow.",
        "input_reason": "",
        "retry_label": "Address Focus Retry",
        "retry_reason": "Retry with an alternate address-bar shortcut if the app remapped the location focus binding.",
        "verification_success": "address bar verified",
        "verification_failure": "address bar focus finished, but JARVIS could not confirm the location surface became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful address-bar workflow as best-effort confirmation.",
        "verify_hint": "address",
        "probe_terms": ["address", "location"],
        "recommended_followups": ["navigate", "search"],
        "surface_flag": "address_bar_ready",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "open_bookmarks": {
        "title": "Open Bookmarks",
        "category_hints": {"browser"},
        "hotkey_field": "bookmarks_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_bookmarks",
        "missing_input_message": "",
        "support_message": "No bookmarks workflow is configured for this app.",
        "hotkey_reason": "Open the browser bookmarks surface for navigation and curation workflows.",
        "input_reason": "",
        "retry_label": "Bookmarks Retry",
        "retry_reason": "Retry with an alternate bookmarks shortcut if the browser remapped the bookmark manager binding.",
        "verification_success": "bookmarks verified",
        "verification_failure": "bookmarks finished, but JARVIS could not confirm the bookmarks surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful bookmarks workflow as best-effort confirmation.",
        "verify_hint": "bookmarks",
        "probe_terms": ["bookmarks", "bookmark"],
        "recommended_followups": ["navigate", "search", "new_tab"],
        "surface_flag": "bookmarks_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "focus_explorer": {
        "title": "Focus Explorer",
        "category_hints": {"code_editor", "ide"},
        "hotkey_field": "explorer_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_explorer",
        "missing_input_message": "",
        "support_message": "No project explorer workflow is configured for this app.",
        "hotkey_reason": "Focus the workspace explorer or project tree before follow-up file actions.",
        "input_reason": "",
        "retry_label": "Explorer Retry",
        "retry_reason": "Retry with an alternate explorer shortcut for editors or IDEs with custom sidebar bindings.",
        "verification_success": "explorer verified",
        "verification_failure": "explorer focus finished, but JARVIS could not confirm the project explorer became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful explorer workflow as best-effort confirmation.",
        "verify_hint": "explorer",
        "probe_terms": ["explorer", "files"],
        "recommended_followups": ["quick_open", "workspace_search"],
        "surface_flag": "explorer_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "focus_folder_tree": {
        "title": "Focus Folder Tree",
        "category_hints": {"file_manager"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_folder_tree",
        "missing_input_message": "",
        "support_message": "No folder-tree workflow is configured for this file manager.",
        "hotkey_reason": "Focus the file manager navigation tree before folder browsing actions.",
        "input_reason": "",
        "retry_label": "Folder Tree Retry",
        "retry_reason": "Retry the navigation-tree focus action if the shell delayed accessibility exposure.",
        "verification_success": "folder tree focused",
        "verification_failure": "folder-tree focus finished, but JARVIS could not confirm the navigation pane became ready",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful folder-tree focus workflow as best-effort confirmation.",
        "probe_terms": ["navigation pane", "folders", "quick access", "this pc"],
        "recommended_followups": ["go_up_level", "search", "focus_file_list"],
        "surface_flag": "folder_tree_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Navigation Pane", "action": "focus", "control_type": "Tree"},
        "workflow_action_reason": "Focus the file manager navigation tree through accessibility before follow-up folder actions.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"file_manager"},
    },
    "focus_file_list": {
        "title": "Focus File List",
        "category_hints": {"file_manager"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_file_list",
        "missing_input_message": "",
        "support_message": "No file-list workflow is configured for this file manager.",
        "hotkey_reason": "Focus the visible file list before rename, properties, or search actions.",
        "input_reason": "",
        "retry_label": "File List Retry",
        "retry_reason": "Retry the file-list focus action if the shell delayed accessibility exposure.",
        "verification_success": "file list focused",
        "verification_failure": "file-list focus finished, but JARVIS could not confirm the items view became ready",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful file-list focus workflow as best-effort confirmation.",
        "probe_terms": ["items view", "file list", "list view", "details view"],
        "recommended_followups": ["rename_selection", "open_properties_dialog", "search"],
        "surface_flag": "file_list_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Items View", "action": "focus", "control_type": "List"},
        "workflow_action_reason": "Focus the file manager items list through accessibility before file actions.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"file_manager"},
    },
    "focus_navigation_tree": {
        "title": "Focus Navigation Tree",
        "category_hints": {"file_manager", "utility", "ops_console", "general_desktop", "security", "office"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_navigation_tree",
        "missing_input_message": "",
        "support_message": "No generic navigation-tree workflow is configured for this app.",
        "hotkey_reason": "Focus the app's tree surface before follow-up hierarchy actions.",
        "input_reason": "",
        "retry_label": "Navigation Tree Retry",
        "retry_reason": "Retry the navigation-tree focus action if the app delayed exposing its hierarchy surface.",
        "verification_success": "navigation tree focused",
        "verification_failure": "navigation-tree focus finished, but JARVIS could not confirm the hierarchy surface became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful navigation-tree focus as best-effort confirmation.",
        "verify_hint": "tree",
        "probe_terms": ["tree view", "navigation tree", "nodes"],
        "recommended_followups": ["select_tree_item", "expand_tree_item", "open_context_menu"],
        "surface_flag": "tree_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Tree", "action": "focus", "control_type": "Tree"},
        "workflow_action_reason": "Focus the app's navigation tree through accessibility before hierarchy actions.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"file_manager", "utility", "ops_console", "general_desktop", "security", "office"},
    },
    "focus_list_surface": {
        "title": "Focus List Surface",
        "category_hints": {"file_manager", "utility", "ops_console", "general_desktop", "chat", "office", "security"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_list_surface",
        "missing_input_message": "",
        "support_message": "No generic list-surface workflow is configured for this app.",
        "hotkey_reason": "Focus the app's list surface before follow-up item actions.",
        "input_reason": "",
        "retry_label": "List Surface Retry",
        "retry_reason": "Retry the list focus action if the app delayed exposing its item list.",
        "verification_success": "list surface focused",
        "verification_failure": "list-surface focus finished, but JARVIS could not confirm the list became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful list focus as best-effort confirmation.",
        "verify_hint": "list",
        "probe_terms": ["list view", "results list", "items list"],
        "recommended_followups": ["select_list_item", "open_context_menu", "search"],
        "surface_flag": "list_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "List", "action": "focus", "control_type": "List"},
        "workflow_action_reason": "Focus the app's list surface through accessibility before follow-up item actions.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"file_manager", "utility", "ops_console", "general_desktop", "chat", "office", "security"},
    },
    "focus_data_table": {
        "title": "Focus Data Table",
        "category_hints": {"utility", "ops_console", "general_desktop", "security", "office"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_data_table",
        "missing_input_message": "",
        "support_message": "No generic table workflow is configured for this app.",
        "hotkey_reason": "Focus the app's data table before follow-up row actions.",
        "input_reason": "",
        "retry_label": "Data Table Retry",
        "retry_reason": "Retry the table focus action if the app delayed exposing its grid surface.",
        "verification_success": "data table focused",
        "verification_failure": "data-table focus finished, but JARVIS could not confirm the grid became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful data-table focus as best-effort confirmation.",
        "verify_hint": "table",
        "probe_terms": ["table", "grid", "rows"],
        "recommended_followups": ["select_table_row", "open_context_menu", "search"],
        "surface_flag": "table_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Table", "action": "focus", "control_type": "Table"},
        "workflow_action_reason": "Focus the app's data table through accessibility before row-level actions.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"utility", "ops_console", "general_desktop", "security", "office"},
    },
    "focus_sidebar": {
        "title": "Focus Sidebar",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_sidebar",
        "missing_input_message": "",
        "support_message": "No generic sidebar workflow is configured for this app.",
        "hotkey_reason": "Focus the app's sidebar or navigation surface before follow-up actions.",
        "input_reason": "",
        "retry_label": "Sidebar Focus Retry",
        "retry_reason": "Retry the sidebar focus action if the app exposed its navigation surface after a render delay.",
        "verification_success": "sidebar focused",
        "verification_failure": "sidebar focus finished, but JARVIS could not confirm the sidebar became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful sidebar focus workflow as best-effort confirmation.",
        "verify_hint": "sidebar",
        "probe_terms": ["sidebar", "navigation pane", "left pane", "side panel"],
        "recommended_followups": ["focus_main_content", "search", "open_context_menu"],
        "surface_flag": "sidebar_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Sidebar", "action": "focus", "control_type": "Pane"},
        "workflow_action_reason": "Focus the app's sidebar through accessibility before follow-up navigation or context actions.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "select_sidebar_item": {
        "title": "Select Sidebar Item",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_select_sidebar_item",
        "missing_input_message": "query is required to select a sidebar item.",
        "support_message": "No generic sidebar-item workflow is configured for this app.",
        "hotkey_reason": "Stage the app's sidebar before invoking the requested navigation item.",
        "input_reason": "",
        "retry_label": "Sidebar Item Retry",
        "retry_reason": "Retry the sidebar item action if the app delayed exposing the requested navigation target.",
        "verification_success": "sidebar item invoked",
        "verification_failure": "sidebar item finished, but JARVIS could not confirm the requested target was activated",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful sidebar-item action as best-effort confirmation.",
        "verify_hint": "sidebar item",
        "probe_terms": ["sidebar", "navigation", "side panel"],
        "recommended_followups": ["focus_main_content", "open_context_menu", "search"],
        "surface_flag": "sidebar_visible",
        "prep_workflows": ["focus_sidebar"],
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click"},
        "workflow_action_reason": "Invoke the requested sidebar item through accessibility after staging the app's navigation surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "focus_main_content": {
        "title": "Focus Main Content",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_main_content",
        "missing_input_message": "",
        "support_message": "No generic main-content workflow is configured for this app.",
        "hotkey_reason": "Focus the app's main content surface before context, selection, or follow-up actions.",
        "input_reason": "",
        "retry_label": "Main Content Retry",
        "retry_reason": "Retry the main-content focus action if the app delayed exposing its primary content surface.",
        "verification_success": "main content focused",
        "verification_failure": "main-content focus finished, but JARVIS could not confirm the primary content surface became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful content-focus workflow as best-effort confirmation.",
        "verify_hint": "content",
        "probe_terms": ["content", "document", "main pane", "results"],
        "recommended_followups": ["search", "open_context_menu", "focus_toolbar"],
        "surface_flag": "main_content_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Content", "action": "focus", "control_type": "Pane"},
        "workflow_action_reason": "Focus the app's main content surface through accessibility before follow-up actions.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "focus_toolbar": {
        "title": "Focus Toolbar",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_toolbar",
        "missing_input_message": "",
        "support_message": "No generic toolbar workflow is configured for this app.",
        "hotkey_reason": "Focus the app's toolbar or command bar before follow-up tool actions.",
        "input_reason": "",
        "retry_label": "Toolbar Focus Retry",
        "retry_reason": "Retry the toolbar focus action if the app delayed exposing its tool surface.",
        "verification_success": "toolbar focused",
        "verification_failure": "toolbar focus finished, but JARVIS could not confirm the toolbar became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful toolbar focus workflow as best-effort confirmation.",
        "verify_hint": "toolbar",
        "probe_terms": ["toolbar", "command bar", "menu bar", "ribbon"],
        "recommended_followups": ["search", "focus_main_content", "open_context_menu"],
        "surface_flag": "toolbar_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Toolbar", "action": "focus", "control_type": "ToolBar"},
        "workflow_action_reason": "Focus the app's toolbar through accessibility before follow-up tool actions.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "invoke_toolbar_action": {
        "title": "Invoke Toolbar Action",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_invoke_toolbar_action",
        "missing_input_message": "query is required to invoke a toolbar action.",
        "support_message": "No generic toolbar-action workflow is configured for this app.",
        "hotkey_reason": "Stage the app's toolbar before invoking the requested control.",
        "input_reason": "",
        "retry_label": "Toolbar Action Retry",
        "retry_reason": "Retry the toolbar action if the app delayed exposing the requested command surface.",
        "verification_success": "toolbar action invoked",
        "verification_failure": "toolbar action finished, but JARVIS could not confirm the requested tool action was triggered",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful toolbar action as best-effort confirmation.",
        "verify_hint": "toolbar action",
        "probe_terms": ["toolbar", "command bar", "menu bar", "ribbon"],
        "recommended_followups": ["focus_main_content", "search", "open_context_menu"],
        "surface_flag": "toolbar_visible",
        "prep_workflows": ["focus_toolbar"],
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click"},
        "workflow_action_reason": "Invoke the requested toolbar control through accessibility after staging the tool surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "focus_form_surface": {
        "title": "Focus Form Surface",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_form_surface",
        "missing_input_message": "",
        "support_message": "No generic form workflow is configured for this app.",
        "hotkey_reason": "Focus the app's form surface before editing fields, dropdowns, or checkbox controls.",
        "input_reason": "",
        "retry_label": "Form Focus Retry",
        "retry_reason": "Retry the form-focus action if the app delayed exposing its editable control surface.",
        "verification_success": "form surface focused",
        "verification_failure": "form focus finished, but JARVIS could not confirm the editable form surface became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful form-focus workflow as best-effort confirmation.",
        "verify_hint": "form",
        "probe_terms": ["form", "text box", "input", "dropdown", "checkbox"],
        "recommended_followups": ["focus_input_field", "open_dropdown", "check_checkbox"],
        "surface_flag": "form_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Form", "action": "focus", "control_type": "Pane"},
        "workflow_action_reason": "Focus the app's form surface through accessibility before field or option editing.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "focus_input_field": {
        "title": "Focus Input Field",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_focus_input_field",
        "missing_input_message": "query is required to focus an input field.",
        "support_message": "No generic input-field workflow is configured for this app.",
        "hotkey_reason": "Stage the app's form surface before focusing the requested editable field.",
        "input_reason": "",
        "retry_label": "Input Field Retry",
        "retry_reason": "Retry the input-field focus action if the requested editor appeared after a render delay.",
        "verification_success": "input field focused",
        "verification_failure": "input-field focus finished, but JARVIS could not confirm the requested editor became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful input-field focus as best-effort confirmation.",
        "verify_hint": "field",
        "probe_terms": ["field", "input", "text box", "edit"],
        "recommended_followups": ["set_field_value", "open_dropdown", "focus_main_content"],
        "surface_flag": "input_field_visible",
        "prep_workflows": ["focus_form_surface"],
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "focus", "control_type": "Edit"},
        "workflow_action_reason": "Focus the requested editable field through accessibility after staging the app's form surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "set_field_value": {
        "title": "Set Field Value",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "text",
        "requires_input": True,
        "required_fields": ["query", "text"],
        "default_press_enter": False,
        "route_mode": "workflow_set_field_value",
        "missing_input_message": "query and text are required to set a field value.",
        "support_message": "No generic field-edit workflow is configured for this app.",
        "hotkey_reason": "Focus the requested field before replacing its current value.",
        "input_reason": "Replace the requested field value with the provided text.",
        "retry_label": "Field Value Retry",
        "retry_reason": "Retry the field edit if the target control ignored the first text replacement.",
        "verification_success": "field value updated",
        "verification_failure": "field edit finished, but JARVIS could not confirm the requested value appeared",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful field edit as best-effort confirmation.",
        "verify_hint": "field value",
        "probe_terms": ["field", "value", "text box", "input"],
        "recommended_followups": ["open_dropdown", "check_checkbox", "focus_main_content"],
        "surface_flag": "input_field_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "focus", "control_type": "Edit"},
        "workflow_action_reason": "Focus the requested field through accessibility before replacing its current value.",
        "prefer_workflow_action": True,
        "input_sequence": [
            {
                "action": "keyboard_hotkey",
                "keys": ["ctrl", "a"],
                "phase": "workflow_target",
                "reason": "Select the current field contents before replacing them with the requested value.",
            },
            {
                "action": "keyboard_type",
                "field": "text",
                "phase": "input",
                "press_enter": False,
                "reason": "Type the requested replacement value into the focused field.",
            },
        ],
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "open_dropdown": {
        "title": "Open Dropdown",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "dropdown_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_open_dropdown",
        "missing_input_message": "query is required to open a dropdown.",
        "support_message": "No generic dropdown workflow is configured for this app.",
        "hotkey_reason": "Open the requested dropdown after focusing its control.",
        "input_reason": "",
        "retry_label": "Dropdown Retry",
        "retry_reason": "Retry the dropdown expansion if the target control ignored the first open request.",
        "verification_success": "dropdown opened",
        "verification_failure": "dropdown finished, but JARVIS could not confirm the option list opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful dropdown workflow as best-effort confirmation.",
        "verify_hint": "dropdown",
        "probe_terms": ["dropdown", "combo box", "select an option"],
        "recommended_followups": ["select_dropdown_option", "focus_main_content", "search"],
        "surface_flag": "dropdown_open",
        "prep_workflows": ["focus_input_field"],
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "select_dropdown_option": {
        "title": "Select Dropdown Option",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "dropdown_hotkeys",
        "input_field": "text",
        "requires_input": True,
        "required_fields": ["query", "text"],
        "default_press_enter": True,
        "route_mode": "workflow_select_dropdown_option",
        "missing_input_message": "query and text are required to select a dropdown option.",
        "support_message": "No generic dropdown-selection workflow is configured for this app.",
        "hotkey_reason": "Open the requested dropdown before selecting the requested option.",
        "input_reason": "Type and confirm the requested dropdown option once the option list is open.",
        "retry_label": "Dropdown Option Retry",
        "retry_reason": "Retry the dropdown selection if the option list rendered after the first request.",
        "verification_success": "dropdown option selected",
        "verification_failure": "dropdown selection finished, but JARVIS could not confirm the requested option became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful dropdown selection as best-effort confirmation.",
        "verify_hint": "dropdown option",
        "probe_terms": ["dropdown", "combo box", "option", "selected"],
        "recommended_followups": ["set_field_value", "check_checkbox", "focus_main_content"],
        "surface_flag": "dropdown_open",
        "prep_workflows": ["focus_input_field"],
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
        "input_sequence": [
            {
                "action": "keyboard_type",
                "field": "text",
                "phase": "input",
                "press_enter": True,
                "reason": "Type and confirm the requested option in the focused dropdown.",
            },
        ],
    },
    "focus_checkbox": {
        "title": "Focus Checkbox",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_focus_checkbox",
        "missing_input_message": "query is required to focus a checkbox.",
        "support_message": "No generic checkbox workflow is configured for this app.",
        "hotkey_reason": "Stage the app's form surface before focusing the requested checkbox control.",
        "input_reason": "",
        "retry_label": "Checkbox Focus Retry",
        "retry_reason": "Retry the checkbox focus action if the requested control appeared after a render delay.",
        "verification_success": "checkbox focused",
        "verification_failure": "checkbox focus finished, but JARVIS could not confirm the requested checkbox became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful checkbox focus as best-effort confirmation.",
        "verify_hint": "checkbox",
        "probe_terms": ["checkbox", "check box", "checked", "unchecked"],
        "recommended_followups": ["check_checkbox", "uncheck_checkbox", "focus_main_content"],
        "surface_flag": "checkbox_visible",
        "prep_workflows": ["focus_form_surface"],
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "focus", "control_type": "CheckBox"},
        "workflow_action_reason": "Focus the requested checkbox through accessibility after staging the app's form surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "check_checkbox": {
        "title": "Check Checkbox",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "checkbox_toggle_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_check_checkbox",
        "missing_input_message": "query is required to check a checkbox.",
        "support_message": "No generic checkbox-toggle workflow is configured for this app.",
        "hotkey_reason": "Toggle the requested checkbox into its checked state after focusing the control.",
        "input_reason": "",
        "retry_label": "Checkbox Check Retry",
        "retry_reason": "Retry the checkbox toggle if the target control ignored the first request.",
        "verification_success": "checkbox checked",
        "verification_failure": "checkbox toggle finished, but JARVIS could not confirm the requested checkbox became checked",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful checkbox toggle as best-effort confirmation.",
        "verify_hint": "checked",
        "probe_terms": ["checkbox", "checked", "enabled", "on"],
        "recommended_followups": ["uncheck_checkbox", "focus_main_content", "search"],
        "surface_flag": "checkbox_target_checked",
        "prep_workflows": ["focus_checkbox"],
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "uncheck_checkbox": {
        "title": "Uncheck Checkbox",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "checkbox_toggle_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_uncheck_checkbox",
        "missing_input_message": "query is required to uncheck a checkbox.",
        "support_message": "No generic checkbox-toggle workflow is configured for this app.",
        "hotkey_reason": "Toggle the requested checkbox into its unchecked state after focusing the control.",
        "input_reason": "",
        "retry_label": "Checkbox Uncheck Retry",
        "retry_reason": "Retry the checkbox toggle if the target control ignored the first request.",
        "verification_success": "checkbox unchecked",
        "verification_failure": "checkbox toggle finished, but JARVIS could not confirm the requested checkbox became unchecked",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful checkbox toggle as best-effort confirmation.",
        "verify_hint": "unchecked",
        "probe_terms": ["checkbox", "unchecked", "disabled", "off"],
        "recommended_followups": ["check_checkbox", "focus_main_content", "search"],
        "surface_flag": "checkbox_target_unchecked",
        "prep_workflows": ["focus_checkbox"],
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "toggle_switch": {
        "title": "Toggle Switch",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_toggle_switch",
        "missing_input_message": "query is required to toggle a switch.",
        "support_message": "No generic switch-toggle workflow is configured for this app.",
        "hotkey_reason": "Toggle the requested switch or stateful control.",
        "input_reason": "",
        "retry_label": "Switch Toggle Retry",
        "retry_reason": "Retry the switch action if the requested control appeared after a render delay.",
        "verification_success": "switch toggled",
        "verification_failure": "switch action finished, but JARVIS could not confirm the requested control changed state",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful switch action as best-effort confirmation.",
        "verify_hint": "switch",
        "probe_terms": ["toggle", "switch", "on", "off"],
        "recommended_followups": ["focus_main_content", "search"],
        "surface_flag": "toggle_visible",
        "prep_workflows": ["focus_form_surface"],
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click"},
        "workflow_action_reason": "Toggle the requested switch through accessibility after staging the app's form surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "enable_switch": {
        "title": "Enable Switch",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_enable_switch",
        "missing_input_message": "query is required to enable a specific switch.",
        "support_message": "No generic switch-enable workflow is configured for this app.",
        "hotkey_reason": "Turn on the requested switch or stateful control.",
        "input_reason": "",
        "retry_label": "Switch Enable Retry",
        "retry_reason": "Retry the switch-enable action if the requested control appeared after a render delay.",
        "verification_success": "switch enabled",
        "verification_failure": "switch-enable action finished, but JARVIS could not confirm the requested control is on",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful switch-enable action as best-effort confirmation.",
        "verify_hint": "enabled",
        "probe_terms": ["enable", "turn on", "switch on", "on"],
        "recommended_followups": ["disable_switch", "focus_main_content", "search"],
        "surface_flag": "toggle_visible",
        "prep_workflows": ["focus_form_surface"],
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click"},
        "workflow_action_reason": "Enable the requested switch through accessibility after staging the app's form surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "disable_switch": {
        "title": "Disable Switch",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_disable_switch",
        "missing_input_message": "query is required to disable a specific switch.",
        "support_message": "No generic switch-disable workflow is configured for this app.",
        "hotkey_reason": "Turn off the requested switch or stateful control.",
        "input_reason": "",
        "retry_label": "Switch Disable Retry",
        "retry_reason": "Retry the switch-disable action if the requested control appeared after a render delay.",
        "verification_success": "switch disabled",
        "verification_failure": "switch-disable action finished, but JARVIS could not confirm the requested control is off",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful switch-disable action as best-effort confirmation.",
        "verify_hint": "disabled",
        "probe_terms": ["disable", "turn off", "switch off", "off"],
        "recommended_followups": ["enable_switch", "focus_main_content", "search"],
        "surface_flag": "toggle_visible",
        "prep_workflows": ["focus_form_surface"],
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click"},
        "workflow_action_reason": "Disable the requested switch through accessibility after staging the app's form surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "select_radio_option": {
        "title": "Select Radio Option",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_select_radio_option",
        "missing_input_message": "query is required to select a radio option.",
        "support_message": "No generic radio-option workflow is configured for this app.",
        "hotkey_reason": "Select the requested radio option after staging the app's form surface.",
        "input_reason": "",
        "retry_label": "Radio Option Retry",
        "retry_reason": "Retry the radio-option action if the requested control appeared after a render delay.",
        "verification_success": "radio option selected",
        "verification_failure": "radio selection finished, but JARVIS could not confirm the requested option became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful radio selection as best-effort confirmation.",
        "verify_hint": "radio option",
        "probe_terms": ["radio button", "radio option", "selected"],
        "recommended_followups": ["focus_main_content", "open_dropdown", "focus_value_control"],
        "surface_flag": "radio_target_selected",
        "prep_workflows": ["focus_form_surface"],
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click", "control_type": "RadioButton"},
        "workflow_action_reason": "Select the requested radio option through accessibility after staging the app's form surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "focus_value_control": {
        "title": "Focus Value Control",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_focus_value_control",
        "missing_input_message": "query is required to focus a value control.",
        "support_message": "No generic value-control workflow is configured for this app.",
        "hotkey_reason": "Stage the app's form surface before focusing the requested slider, spinner, or value control.",
        "input_reason": "",
        "retry_label": "Value Control Retry",
        "retry_reason": "Retry the value-control focus action if the requested control appeared after a render delay.",
        "verification_success": "value control focused",
        "verification_failure": "value-control focus finished, but JARVIS could not confirm the requested control became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful value-control focus as best-effort confirmation.",
        "verify_hint": "value control",
        "probe_terms": ["slider", "spinner", "stepper", "value control", "number input"],
        "recommended_followups": ["increase_value", "decrease_value", "focus_main_content"],
        "surface_flag": "value_control_visible",
        "prep_workflows": ["focus_form_surface"],
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "focus"},
        "workflow_action_reason": "Focus the requested value control through accessibility after staging the app's form surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "increase_value": {
        "title": "Increase Value",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_increase_value",
        "missing_input_message": "query is required to increase a value control.",
        "support_message": "No generic value-adjustment workflow is configured for this app.",
        "hotkey_reason": "Focus the requested slider, spinner, or value control before increasing it.",
        "input_reason": "Increase the focused value control by the requested amount.",
        "retry_label": "Increase Value Retry",
        "retry_reason": "Retry the value increase if the control ignored the first adjustment.",
        "verification_success": "value increased",
        "verification_failure": "value increase finished, but JARVIS could not confirm the requested control changed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful value adjustment as best-effort confirmation.",
        "verify_hint": "value",
        "probe_terms": ["slider", "spinner", "stepper", "value", "increment"],
        "recommended_followups": ["decrease_value", "focus_main_content", "focus_value_control"],
        "surface_flag": "value_control_visible",
        "prep_workflows": ["focus_form_surface"],
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "focus"},
        "workflow_action_reason": "Focus the requested value control through accessibility before increasing it.",
        "prefer_workflow_action": True,
        "input_sequence": [
            {
                "action": "keyboard_hotkey",
                "keys": ["up"],
                "repeat_field": "amount",
                "max_repeat": 20,
                "phase": "input",
                "reason": "Increase the focused value control by the requested amount.",
            },
        ],
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "decrease_value": {
        "title": "Decrease Value",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_decrease_value",
        "missing_input_message": "query is required to decrease a value control.",
        "support_message": "No generic value-adjustment workflow is configured for this app.",
        "hotkey_reason": "Focus the requested slider, spinner, or value control before decreasing it.",
        "input_reason": "Decrease the focused value control by the requested amount.",
        "retry_label": "Decrease Value Retry",
        "retry_reason": "Retry the value decrease if the control ignored the first adjustment.",
        "verification_success": "value decreased",
        "verification_failure": "value decrease finished, but JARVIS could not confirm the requested control changed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful value adjustment as best-effort confirmation.",
        "verify_hint": "value",
        "probe_terms": ["slider", "spinner", "stepper", "value", "decrement"],
        "recommended_followups": ["increase_value", "focus_main_content", "focus_value_control"],
        "surface_flag": "value_control_visible",
        "prep_workflows": ["focus_form_surface"],
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "focus"},
        "workflow_action_reason": "Focus the requested value control through accessibility before decreasing it.",
        "prefer_workflow_action": True,
        "input_sequence": [
            {
                "action": "keyboard_hotkey",
                "keys": ["down"],
                "repeat_field": "amount",
                "max_repeat": 20,
                "phase": "input",
                "reason": "Decrease the focused value control by the requested amount.",
            },
        ],
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "set_value_control": {
        "title": "Set Value Control",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "text",
        "requires_input": True,
        "required_fields": ["query", "text"],
        "default_press_enter": False,
        "route_mode": "workflow_set_value_control",
        "missing_input_message": "query and text are required to set a value control.",
        "support_message": "No generic absolute value-control workflow is configured for this app.",
        "hotkey_reason": "Focus the requested slider, spinner, or numeric control before moving it to the requested value.",
        "input_reason": "Set the focused value control to the requested target value.",
        "retry_label": "Set Value Retry",
        "retry_reason": "Retry the value-setting action after refocusing the requested control if the target state was not reached.",
        "verification_success": "value control updated",
        "verification_failure": "value-control update finished, but JARVIS could not confirm the requested target value",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful value-control update as best-effort confirmation.",
        "verify_hint": "value",
        "probe_terms": ["slider", "spinner", "stepper", "value", "numeric"],
        "recommended_followups": ["focus_value_control", "increase_value", "decrease_value"],
        "surface_flag": "value_control_visible",
        "prep_workflows": ["focus_form_surface"],
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "focus"},
        "workflow_action_reason": "Focus the requested value control through accessibility before setting its target value.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "select_tab_page": {
        "title": "Select Tab Page",
        "category_hints": {"file_manager", "office", "utility", "ops_console", "security", "general_desktop", "ai_companion"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_select_tab_page",
        "missing_input_message": "query is required to select a tab page.",
        "support_message": "No generic tab-page workflow is configured for this app.",
        "hotkey_reason": "Activate the requested property or settings tab before follow-up actions.",
        "input_reason": "",
        "retry_label": "Tab Page Retry",
        "retry_reason": "Retry the tab-page selection if the property sheet rendered after the first click.",
        "verification_success": "tab page selected",
        "verification_failure": "tab-page selection finished, but JARVIS could not confirm the requested tab became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful tab-page selection as best-effort confirmation.",
        "verify_hint": "tab",
        "probe_terms": ["tab", "page", "property sheet", "settings page"],
        "recommended_followups": ["focus_form_surface", "focus_main_content", "set_value_control"],
        "surface_flag": "tab_target_active",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click", "control_type": "TabItem"},
        "workflow_action_reason": "Activate the requested tab page through accessibility.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"file_manager", "office", "utility", "ops_console", "security", "general_desktop", "ai_companion"},
    },
    "select_tree_item": {
        "title": "Select Tree Item",
        "category_hints": {"file_manager", "utility", "ops_console", "general_desktop", "security", "office"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_select_tree_item",
        "missing_input_message": "query is required to select a tree item.",
        "support_message": "No generic tree-item workflow is configured for this app.",
        "hotkey_reason": "Stage the app's hierarchy surface before selecting the requested tree item.",
        "input_reason": "",
        "retry_label": "Tree Item Retry",
        "retry_reason": "Retry the tree-item action if the hierarchy target appeared after a render delay.",
        "verification_success": "tree item selected",
        "verification_failure": "tree item finished, but JARVIS could not confirm the requested hierarchy target was activated",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful tree-item action as best-effort confirmation.",
        "verify_hint": "tree item",
        "probe_terms": ["tree", "node", "folder"],
        "recommended_followups": ["expand_tree_item", "open_context_menu", "focus_list_surface"],
        "surface_flag": "tree_visible",
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click", "control_type": "TreeItem"},
        "workflow_action_reason": "Select the requested tree item through accessibility after staging the hierarchy surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"file_manager", "utility", "ops_console", "general_desktop", "security", "office"},
    },
    "expand_tree_item": {
        "title": "Expand Tree Item",
        "category_hints": {"file_manager", "utility", "ops_console", "general_desktop", "security", "office"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_expand_tree_item",
        "missing_input_message": "query is required to expand a tree item.",
        "support_message": "No generic tree-expansion workflow is configured for this app.",
        "hotkey_reason": "Stage the app's hierarchy surface before expanding the requested tree item.",
        "input_reason": "",
        "retry_label": "Tree Expansion Retry",
        "retry_reason": "Retry the tree-expansion action if the hierarchy target appeared after a render delay.",
        "verification_success": "tree item expanded",
        "verification_failure": "tree expansion finished, but JARVIS could not confirm the requested hierarchy target expanded",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful tree expansion as best-effort confirmation.",
        "verify_hint": "expanded",
        "probe_terms": ["tree", "expanded", "node"],
        "recommended_followups": ["select_tree_item", "focus_list_surface", "open_context_menu"],
        "surface_flag": "tree_visible",
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "double_click", "control_type": "TreeItem"},
        "workflow_action_reason": "Expand the requested tree item through accessibility after staging the hierarchy surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"file_manager", "utility", "ops_console", "general_desktop", "security", "office"},
    },
    "select_list_item": {
        "title": "Select List Item",
        "category_hints": {"file_manager", "utility", "ops_console", "general_desktop", "chat", "office", "security"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_select_list_item",
        "missing_input_message": "query is required to select a list item.",
        "support_message": "No generic list-item workflow is configured for this app.",
        "hotkey_reason": "Stage the app's list surface before selecting the requested item.",
        "input_reason": "",
        "retry_label": "List Item Retry",
        "retry_reason": "Retry the list-item action if the requested target appeared after a render delay.",
        "verification_success": "list item selected",
        "verification_failure": "list item finished, but JARVIS could not confirm the requested list target was activated",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful list-item action as best-effort confirmation.",
        "verify_hint": "list item",
        "probe_terms": ["list", "item", "results"],
        "recommended_followups": ["open_context_menu", "search", "focus_data_table"],
        "surface_flag": "list_visible",
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click", "control_type": "ListItem"},
        "workflow_action_reason": "Select the requested list item through accessibility after staging the list surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"file_manager", "utility", "ops_console", "general_desktop", "chat", "office", "security"},
    },
    "select_table_row": {
        "title": "Select Table Row",
        "category_hints": {"utility", "ops_console", "general_desktop", "security", "office"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_select_table_row",
        "missing_input_message": "query is required to select a table row.",
        "support_message": "No generic table-row workflow is configured for this app.",
        "hotkey_reason": "Stage the app's data table before selecting the requested row.",
        "input_reason": "",
        "retry_label": "Table Row Retry",
        "retry_reason": "Retry the table-row action if the requested row appeared after a render delay.",
        "verification_success": "table row selected",
        "verification_failure": "table row finished, but JARVIS could not confirm the requested row was activated",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful table-row action as best-effort confirmation.",
        "verify_hint": "row",
        "probe_terms": ["table", "row", "grid"],
        "recommended_followups": ["open_context_menu", "search", "focus_main_content"],
        "surface_flag": "table_visible",
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click"},
        "workflow_action_reason": "Select the requested data row through accessibility after staging the table surface.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"utility", "ops_console", "general_desktop", "security", "office"},
    },
    "new_folder": {
        "title": "New Folder",
        "category_hints": {"file_manager"},
        "hotkey_field": "new_folder_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_new_folder",
        "missing_input_message": "",
        "support_message": "No new-folder workflow is configured for this app.",
        "hotkey_reason": "Create a fresh folder in the current file manager location before follow-up file actions.",
        "input_reason": "",
        "retry_label": "New Folder Retry",
        "retry_reason": "Retry with an alternate new-folder shortcut if the file manager remapped the binding.",
        "verification_success": "new folder verified",
        "verification_failure": "new folder finished, but JARVIS could not confirm that a new folder placeholder appeared",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful new-folder workflow as best-effort confirmation.",
        "verify_hint": "new folder",
        "probe_terms": ["new folder"],
        "recommended_followups": ["search", "refresh_view"],
    },
    "rename_selection": {
        "title": "Rename Selection",
        "category_hints": {"file_manager"},
        "hotkey_field": "item_rename_hotkeys",
        "input_field": "text",
        "requires_input": True,
        "required_fields": ["text"],
        "default_press_enter": True,
        "route_mode": "workflow_rename_selection",
        "missing_input_message": "text is required to rename the selected item.",
        "support_message": "No selection-rename workflow is configured for this app.",
        "hotkey_reason": "Open the active selection's rename surface before typing the requested replacement name.",
        "input_reason": "Type and submit the requested replacement name for the current selection.",
        "retry_label": "Rename Selection Retry",
        "retry_reason": "Retry with an alternate rename shortcut if the current app remapped rename for the selected item.",
        "verification_success": "selection rename verified",
        "verification_failure": "selection rename finished, but JARVIS could not confirm the rename surface or replacement name",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful selection-rename workflow as best-effort confirmation.",
        "verify_hint": "rename",
        "probe_terms": ["rename", "name"],
        "recommended_followups": ["refresh_view", "open_properties_dialog", "search"],
        "surface_flag": "rename_active",
        "skip_hotkey_when_ready": True,
    },
    "open_properties_dialog": {
        "title": "Open Properties",
        "category_hints": {"file_manager"},
        "hotkey_field": "properties_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_open_properties_dialog",
        "missing_input_message": "",
        "support_message": "No properties-dialog workflow is configured for this app.",
        "hotkey_reason": "Open the active item's properties surface for inspection and follow-up actions.",
        "input_reason": "",
        "retry_label": "Properties Retry",
        "retry_reason": "Retry with an alternate properties shortcut if the current app remapped it.",
        "verification_success": "properties dialog verified",
        "verification_failure": "properties dialog finished, but JARVIS could not confirm the properties surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful properties workflow as best-effort confirmation.",
        "verify_hint": "properties",
        "probe_terms": ["properties", "general", "details"],
        "recommended_followups": ["rename_selection", "search", "refresh_view"],
        "surface_flag": "properties_dialog_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "open_preview_pane": {
        "title": "Open Preview Pane",
        "category_hints": {"file_manager"},
        "hotkey_field": "preview_pane_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_open_preview_pane",
        "missing_input_message": "",
        "support_message": "No preview-pane workflow is configured for this app.",
        "hotkey_reason": "Open the active preview pane so JARVIS can inspect or branch from the file preview surface.",
        "input_reason": "",
        "retry_label": "Preview Pane Retry",
        "retry_reason": "Retry with an alternate preview-pane shortcut if the file manager remapped pane visibility controls.",
        "verification_success": "preview pane verified",
        "verification_failure": "preview pane finished, but JARVIS could not confirm the preview surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful preview-pane workflow as best-effort confirmation.",
        "verify_hint": "preview",
        "probe_terms": ["preview", "preview pane"],
        "recommended_followups": ["search", "open_properties_dialog", "refresh_view"],
        "surface_flag": "preview_pane_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "open_details_pane": {
        "title": "Open Details Pane",
        "category_hints": {"file_manager"},
        "hotkey_field": "details_pane_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_open_details_pane",
        "missing_input_message": "",
        "support_message": "No details-pane workflow is configured for this app.",
        "hotkey_reason": "Open the active details pane so JARVIS can branch from metadata and selection details.",
        "input_reason": "",
        "retry_label": "Details Pane Retry",
        "retry_reason": "Retry with an alternate details-pane shortcut if the file manager remapped pane visibility controls.",
        "verification_success": "details pane verified",
        "verification_failure": "details pane finished, but JARVIS could not confirm the details surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful details-pane workflow as best-effort confirmation.",
        "verify_hint": "details",
        "probe_terms": ["details", "details pane"],
        "recommended_followups": ["open_properties_dialog", "rename_selection", "search"],
        "surface_flag": "details_pane_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "open_context_menu": {
        "title": "Open Context Menu",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "context_menu_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_open_context_menu",
        "missing_input_message": "",
        "support_message": "No context-menu workflow is configured for this app.",
        "hotkey_reason": "Open the active context or shortcut menu before a follow-up menu action.",
        "input_reason": "",
        "retry_label": "Context Menu Retry",
        "retry_reason": "Retry with an alternate context-menu shortcut if the app remapped or delayed menu activation.",
        "verification_success": "context menu verified",
        "verification_failure": "context menu finished, but JARVIS could not confirm the shortcut menu opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful context-menu workflow as best-effort confirmation.",
        "verify_hint": "menu",
        "probe_terms": ["context menu", "shortcut menu", "right click menu"],
        "recommended_followups": ["dismiss_dialog", "confirm_dialog", "focus_main_content"],
        "surface_flag": "context_menu_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "select_context_menu_item": {
        "title": "Select Context Menu Item",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_select_context_menu_item",
        "missing_input_message": "query is required to select a context-menu item.",
        "support_message": "No generic context-menu item workflow is configured for this app.",
        "hotkey_reason": "Open the active context menu before invoking the requested menu item.",
        "input_reason": "",
        "retry_label": "Context Menu Item Retry",
        "retry_reason": "Retry the context-menu item action if the requested menu target appeared after a render delay.",
        "verification_success": "context menu item invoked",
        "verification_failure": "context menu item finished, but JARVIS could not confirm the requested menu target was activated",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful context-menu item action as best-effort confirmation.",
        "verify_hint": "menu item",
        "probe_terms": ["context menu", "shortcut menu", "menu item"],
        "recommended_followups": ["focus_main_content", "dismiss_dialog", "confirm_dialog"],
        "surface_flag": "context_menu_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
        "prep_workflows": ["open_context_menu"],
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click", "control_type": "MenuItem"},
        "workflow_action_reason": "Invoke the requested context-menu item through accessibility after staging the active shortcut menu.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "dismiss_dialog": {
        "title": "Dismiss Dialog",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "dismiss_dialog_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_dismiss_dialog",
        "missing_input_message": "",
        "support_message": "No dismiss-dialog workflow is configured for this app.",
        "hotkey_reason": "Dismiss the active dialog, popup, or context surface before continuing.",
        "input_reason": "",
        "retry_label": "Dismiss Surface Retry",
        "retry_reason": "Retry with an alternate dismiss shortcut if the active surface ignored the first cancel request.",
        "verification_success": "dismiss surface dispatched",
        "verification_failure": "dismiss surface finished, but JARVIS could not confirm the modal or menu closed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful dismiss action as best-effort confirmation.",
        "verify_hint": "cancel",
        "probe_terms": ["cancel", "close", "dismiss", "popup"],
        "recommended_followups": ["focus_main_content", "search"],
        "surface_flag": "dismissible_surface_visible",
    },
    "confirm_dialog": {
        "title": "Confirm Dialog",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "confirm_dialog_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_confirm_dialog",
        "missing_input_message": "",
        "support_message": "No confirm-dialog workflow is configured for this app.",
        "hotkey_reason": "Confirm the active dialog or modal surface before continuing.",
        "input_reason": "",
        "retry_label": "Confirm Surface Retry",
        "retry_reason": "Retry with an alternate confirm shortcut if the active dialog ignored the first confirmation.",
        "verification_success": "confirm surface dispatched",
        "verification_failure": "confirm surface finished, but JARVIS could not confirm the dialog was accepted",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful confirm action as best-effort confirmation.",
        "verify_hint": "ok",
        "probe_terms": ["ok", "apply", "confirm", "continue"],
        "recommended_followups": ["focus_main_content", "search"],
        "surface_flag": "dialog_visible",
    },
    "press_dialog_button": {
        "title": "Press Dialog Button",
        "category_hints": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "required_fields": ["query"],
        "default_press_enter": False,
        "route_mode": "workflow_press_dialog_button",
        "missing_input_message": "query is required to press a dialog button.",
        "support_message": "No generic dialog-button workflow is configured for this app.",
        "hotkey_reason": "Target the active dialog before invoking the requested button.",
        "input_reason": "",
        "retry_label": "Dialog Button Retry",
        "retry_reason": "Retry the dialog-button action if the requested control appeared after a render delay.",
        "verification_success": "dialog button invoked",
        "verification_failure": "dialog button finished, but JARVIS could not confirm the requested dialog action was accepted",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful dialog-button action as best-effort confirmation.",
        "verify_hint": "button",
        "probe_terms": ["dialog", "popup", "modal", "button"],
        "recommended_followups": ["focus_main_content", "search"],
        "surface_flag": "dialog_visible",
        "skip_input_steps": True,
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "{{args.query}}", "action": "click", "control_type": "Button"},
        "workflow_action_reason": "Invoke the requested dialog button through accessibility while the modal surface is active.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"},
    },
    "next_wizard_step": {
        "title": "Next Wizard Step",
        "category_hints": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
        "hotkey_field": "wizard_next_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_next_wizard_step",
        "missing_input_message": "",
        "support_message": "No wizard-step workflow is configured for this app.",
        "hotkey_reason": "Advance the active setup or installer wizard to the next step.",
        "input_reason": "",
        "retry_label": "Wizard Next Retry",
        "retry_reason": "Retry with an alternate wizard accelerator if the setup flow ignored the first advance action.",
        "verification_success": "wizard advanced",
        "verification_failure": "wizard advance finished, but JARVIS could not confirm that the next step opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful wizard advance as best-effort confirmation.",
        "verify_hint": "next",
        "probe_terms": ["wizard", "installer", "setup", "next", "continue"],
        "recommended_followups": ["finish_wizard", "dismiss_dialog", "confirm_dialog"],
        "surface_flag": "wizard_next_available",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Next", "action": "click", "control_type": "Button"},
        "workflow_action_reason": "Invoke the wizard Next button through accessibility before falling back to keyboard accelerators.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
    },
    "previous_wizard_step": {
        "title": "Previous Wizard Step",
        "category_hints": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
        "hotkey_field": "wizard_back_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_previous_wizard_step",
        "missing_input_message": "",
        "support_message": "No wizard-back workflow is configured for this app.",
        "hotkey_reason": "Return the active setup or installer wizard to the previous step.",
        "input_reason": "",
        "retry_label": "Wizard Back Retry",
        "retry_reason": "Retry with an alternate wizard accelerator if the setup flow ignored the first back action.",
        "verification_success": "wizard moved back",
        "verification_failure": "wizard back action finished, but JARVIS could not confirm that the previous step opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful wizard back action as best-effort confirmation.",
        "verify_hint": "back",
        "probe_terms": ["wizard", "installer", "setup", "back", "previous"],
        "recommended_followups": ["next_wizard_step", "dismiss_dialog"],
        "surface_flag": "wizard_back_available",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Back", "action": "click", "control_type": "Button"},
        "workflow_action_reason": "Invoke the wizard Back button through accessibility before falling back to keyboard accelerators.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
    },
    "finish_wizard": {
        "title": "Finish Wizard",
        "category_hints": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
        "hotkey_field": "wizard_finish_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_finish_wizard",
        "missing_input_message": "",
        "support_message": "No wizard-finish workflow is configured for this app.",
        "hotkey_reason": "Finish the active setup or installer wizard when the completion step is ready.",
        "input_reason": "",
        "retry_label": "Wizard Finish Retry",
        "retry_reason": "Retry with an alternate finish accelerator if the setup flow ignored the first completion action.",
        "verification_success": "wizard finish dispatched",
        "verification_failure": "wizard finish finished, but JARVIS could not confirm the installer accepted the completion step",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful wizard finish action as best-effort confirmation.",
        "verify_hint": "finish",
        "probe_terms": ["wizard", "installer", "setup", "finish", "done", "complete"],
        "recommended_followups": ["confirm_dialog", "dismiss_dialog"],
        "surface_flag": "wizard_finish_available",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Finish", "action": "click", "control_type": "Button"},
        "workflow_action_reason": "Invoke the wizard Finish button through accessibility before falling back to keyboard accelerators.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
    },
    "complete_wizard_page": {
        "title": "Complete Wizard Page",
        "category_hints": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_complete_wizard_page",
        "missing_input_message": "",
        "support_message": "No page-completion workflow is configured for this wizard surface.",
        "hotkey_reason": "Resolve the current wizard page requirements and then advance through the preferred live confirmation control.",
        "input_reason": "",
        "retry_label": "Wizard Page Completion Retry",
        "retry_reason": "Retry the wizard page completion sequence if the setup surface refreshed after JARVIS staged a prerequisite control.",
        "verification_success": "wizard page completion dispatched",
        "verification_failure": "wizard page completion finished, but JARVIS could not confirm the setup page advanced",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful wizard page completion as best-effort confirmation.",
        "verify_hint": "wizard",
        "probe_terms": ["wizard", "installer", "setup", "continue setup", "license agreement", "ready to install"],
        "recommended_followups": ["next_wizard_step", "finish_wizard", "dismiss_dialog"],
        "surface_flag": "wizard_surface_visible",
        "skip_input_steps": True,
        "supports_stateful_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
        "supports_action_dispatch_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
    },
    "complete_wizard_flow": {
        "title": "Complete Wizard Flow",
        "category_hints": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_complete_wizard_flow",
        "missing_input_message": "",
        "support_message": "No autonomous wizard-flow workflow is configured for this setup surface.",
        "hotkey_reason": "",
        "input_reason": "",
        "retry_label": "Wizard Flow Recovery Retry",
        "retry_reason": "Retry the autonomous wizard flow with an alternate recovery strategy if the setup surface stalls.",
        "verification_success": "wizard flow completed",
        "verification_failure": "wizard flow stopped before all setup pages could be completed safely",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful wizard mission result as best-effort confirmation.",
        "verify_hint": "setup complete",
        "probe_terms": ["wizard", "installer", "setup", "installation complete", "setup complete", "finish", "done"],
        "recommended_followups": ["dismiss_dialog"],
        "surface_flag": "wizard_surface_visible",
        "skip_input_steps": True,
        "supports_stateful_categories": {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"},
    },
    "complete_form_page": {
        "title": "Complete Form Page",
        "category_hints": {"file_manager", "office", "utility", "ops_console", "security", "general_desktop", "ai_companion"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_complete_form_page",
        "missing_input_message": "",
        "support_message": "No autonomous form-page workflow is configured for this settings or dialog surface.",
        "hotkey_reason": "",
        "input_reason": "",
        "retry_label": "Form Page Completion Retry",
        "retry_reason": "Retry the form-page completion sequence if the settings or dialog surface refreshed after JARVIS staged a prerequisite control.",
        "verification_success": "form page completion dispatched",
        "verification_failure": "form page completion finished, but JARVIS could not confirm the form committed or advanced",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful form-page completion as best-effort confirmation.",
        "verify_hint": "settings saved",
        "probe_terms": ["settings", "options", "properties", "dialog", "save", "apply", "ok", "done", "submit"],
        "recommended_followups": ["confirm_dialog", "dismiss_dialog"],
        "surface_flag": "form_visible",
        "skip_input_steps": True,
        "supports_stateful_categories": {"file_manager", "office", "utility", "ops_console", "security", "general_desktop", "ai_companion"},
        "supports_action_dispatch_categories": {"file_manager", "office", "utility", "ops_console", "security", "general_desktop", "ai_companion"},
    },
    "complete_form_flow": {
        "title": "Complete Form Flow",
        "category_hints": {"file_manager", "office", "utility", "ops_console", "security", "general_desktop", "ai_companion"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_complete_form_flow",
        "missing_input_message": "",
        "support_message": "No autonomous form-flow workflow is configured for this settings or dialog surface.",
        "hotkey_reason": "",
        "input_reason": "",
        "retry_label": "Form Flow Recovery Retry",
        "retry_reason": "Retry the autonomous form flow with an alternate recovery strategy if the settings surface stalls.",
        "verification_success": "form flow completed",
        "verification_failure": "form flow stopped before all settings or dialog pages could be completed safely",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful form mission result as best-effort confirmation.",
        "verify_hint": "settings applied",
        "probe_terms": ["settings", "options", "properties", "dialog", "save changes", "apply changes", "ok", "done", "submit"],
        "recommended_followups": ["dismiss_dialog"],
        "surface_flag": "form_visible",
        "skip_input_steps": True,
        "supports_stateful_categories": {"file_manager", "office", "utility", "ops_console", "security", "general_desktop", "ai_companion"},
    },
    "refresh_view": {
        "title": "Refresh View",
        "category_hints": {"browser", "file_manager", "ops_console", "general_desktop"},
        "hotkey_field": "refresh_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_refresh_view",
        "missing_input_message": "",
        "support_message": "No refresh workflow is configured for this app.",
        "hotkey_reason": "Refresh the current view to reload the visible state before a follow-up action.",
        "input_reason": "",
        "retry_label": "Refresh Retry",
        "retry_reason": "Retry with an alternate refresh shortcut for apps with custom reload bindings.",
        "verification_success": "refresh verified",
        "verification_failure": "refresh finished, but JARVIS could not confirm the view changed or reloaded",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful refresh workflow as best-effort confirmation.",
        "probe_terms": ["refresh"],
        "recommended_followups": ["search", "navigate"],
        "surface_flag": "navigation_surface_ready",
        "self_verifying": True,
    },
    "go_back": {
        "title": "Go Back",
        "category_hints": {"browser", "file_manager"},
        "hotkey_field": "back_navigation_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_go_back",
        "missing_input_message": "",
        "support_message": "No back-navigation workflow is configured for this app.",
        "hotkey_reason": "Navigate backward in the active browser or file-manager history stack.",
        "input_reason": "",
        "retry_label": "Back Navigation Retry",
        "retry_reason": "Retry with an alternate back-navigation shortcut if the app remapped history traversal.",
        "verification_success": "back navigation verified",
        "verification_failure": "back navigation finished, but JARVIS could not confirm the history transition completed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful back-navigation workflow as best-effort confirmation.",
        "recommended_followups": ["go_forward", "refresh_view", "search"],
        "surface_flag": "navigation_surface_ready",
        "self_verifying": True,
    },
    "go_forward": {
        "title": "Go Forward",
        "category_hints": {"browser", "file_manager"},
        "hotkey_field": "forward_navigation_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_go_forward",
        "missing_input_message": "",
        "support_message": "No forward-navigation workflow is configured for this app.",
        "hotkey_reason": "Navigate forward in the active browser or file-manager history stack.",
        "input_reason": "",
        "retry_label": "Forward Navigation Retry",
        "retry_reason": "Retry with an alternate forward-navigation shortcut if the app remapped history traversal.",
        "verification_success": "forward navigation verified",
        "verification_failure": "forward navigation finished, but JARVIS could not confirm the history transition completed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful forward-navigation workflow as best-effort confirmation.",
        "recommended_followups": ["go_back", "refresh_view", "search"],
        "surface_flag": "navigation_surface_ready",
        "self_verifying": True,
    },
    "go_up_level": {
        "title": "Go Up Level",
        "category_hints": {"file_manager"},
        "hotkey_field": "up_level_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_go_up_level",
        "missing_input_message": "",
        "support_message": "No up-level workflow is configured for this app.",
        "hotkey_reason": "Move to the parent folder before continuing the file manager workflow.",
        "input_reason": "",
        "retry_label": "Up Level Retry",
        "retry_reason": "Retry with an alternate parent-folder shortcut if the file manager remapped navigation.",
        "verification_success": "parent folder verified",
        "verification_failure": "up-level finished, but JARVIS could not confirm the file manager location changed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful up-level workflow as best-effort confirmation.",
        "probe_terms": ["up", "parent folder"],
        "recommended_followups": ["focus_address_bar", "search"],
    },
    "workspace_search": {
        "title": "Workspace Search",
        "category_hints": {"code_editor", "ide"},
        "hotkey_field": "workspace_search_hotkeys",
        "input_field": "query",
        "requires_input": True,
        "default_press_enter": False,
        "route_mode": "workflow_workspace_search",
        "missing_input_message": "query is required for workspace search workflows.",
        "support_message": "No workspace-search workflow is configured for this app.",
        "hotkey_reason": "Open the workspace search surface before typing the requested cross-file query.",
        "input_reason": "Type the requested cross-file query into the workspace search panel.",
        "retry_label": "Workspace Search Retry",
        "retry_reason": "Retry with an alternate workspace-search shortcut for editor and IDE workflows.",
        "verification_success": "workspace search verified",
        "verification_failure": "workspace search finished, but JARVIS could not confirm the search surface or query state",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful workspace-search workflow as best-effort confirmation.",
        "probe_terms": ["search", "find in files"],
        "recommended_followups": ["quick_open", "go_to_symbol"],
        "surface_flag": "workspace_search_visible",
        "skip_hotkey_when_ready": True,
    },
    "find_replace": {
        "title": "Find And Replace",
        "category_hints": {"code_editor", "ide", "office"},
        "hotkey_field": "replace_hotkeys",
        "input_field": "query",
        "requires_input": True,
        "required_fields": ["query", "text"],
        "default_press_enter": False,
        "route_mode": "workflow_find_replace",
        "missing_input_message": "query and text are required for find-and-replace workflows.",
        "support_message": "No find-and-replace workflow is configured for this app.",
        "hotkey_reason": "Open the app's replace surface before seeding the requested find and replacement text.",
        "input_reason": "Seed the active replace surface with the requested find and replacement text.",
        "retry_label": "Replace Retry",
        "retry_reason": "Retry with an alternate replace shortcut for editors, IDEs, and document apps.",
        "verification_success": "find and replace verified",
        "verification_failure": "find and replace finished, but JARVIS could not confirm the replace surface or requested values",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful find-and-replace workflow as best-effort confirmation.",
        "verify_hint": "replace",
        "probe_terms": ["replace", "replace with", "find what"],
        "recommended_followups": ["search", "save_document", "format_document"],
        "surface_flag": "replace_visible",
        "skip_hotkey_when_ready": True,
        "input_sequence": [
            {
                "field": "query",
                "phase": "workflow_target",
                "press_enter": False,
                "reason": "Type the requested match text into the replace surface's find field.",
            },
            {
                "action": "keyboard_hotkey",
                "keys": ["tab"],
                "phase": "workflow_target",
                "reason": "Move focus from the find field into the replacement field.",
            },
            {
                "field": "text",
                "phase": "input",
                "press_enter": False,
                "reason": "Type the requested replacement text into the active replace field.",
            },
        ],
    },
    "go_to_symbol": {
        "title": "Go To Symbol",
        "category_hints": {"code_editor", "ide"},
        "hotkey_field": "symbol_hotkeys",
        "input_field": "query",
        "requires_input": True,
        "default_press_enter": False,
        "route_mode": "workflow_go_to_symbol",
        "missing_input_message": "query is required for symbol workflows.",
        "support_message": "No go-to-symbol workflow is configured for this app.",
        "hotkey_reason": "Open the symbol search surface before typing the requested symbol name.",
        "input_reason": "Type the requested symbol name into the active symbol picker.",
        "retry_label": "Symbol Retry",
        "retry_reason": "Retry with an alternate symbol-search shortcut for editor-style apps.",
        "verification_success": "symbol search verified",
        "verification_failure": "symbol search finished, but JARVIS could not confirm the symbol picker or requested symbol state",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful symbol-search workflow as best-effort confirmation.",
        "verify_hint": "symbol",
        "probe_terms": ["symbol", "outline"],
        "recommended_followups": ["rename_symbol", "command"],
        "surface_flag": "symbol_picker_visible",
        "skip_hotkey_when_ready": True,
    },
    "rename_symbol": {
        "title": "Rename Symbol",
        "category_hints": {"code_editor", "ide"},
        "hotkey_field": "rename_hotkeys",
        "input_field": "text",
        "requires_input": True,
        "default_press_enter": True,
        "route_mode": "workflow_rename_symbol",
        "missing_input_message": "text is required for rename-symbol workflows.",
        "support_message": "No rename-symbol workflow is configured for this app.",
        "hotkey_reason": "Open the active symbol rename surface before typing the new symbol name.",
        "input_reason": "Type the requested replacement symbol into the active rename surface.",
        "retry_label": "Rename Retry",
        "retry_reason": "Retry with an alternate rename shortcut for editor-style apps.",
        "verification_success": "rename verified",
        "verification_failure": "rename finished, but JARVIS could not confirm the rename surface or replacement symbol state",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful rename workflow as best-effort confirmation.",
        "probe_terms": ["rename", "symbol"],
        "recommended_followups": ["format_document", "workspace_search"],
    },
    "new_tab": {
        "title": "New Tab",
        "category_hints": {"browser", "terminal", "file_manager"},
        "hotkey_field": "new_tab_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_new_tab",
        "missing_input_message": "",
        "support_message": "No new-tab workflow is configured for this app.",
        "hotkey_reason": "Create a fresh tab or workspace surface before continuing with follow-up actions.",
        "input_reason": "",
        "retry_label": "New Tab Retry",
        "retry_reason": "Retry with an alternate new-tab shortcut for apps with custom bindings.",
        "verification_success": "new tab verified",
        "verification_failure": "new tab finished, but JARVIS could not confirm a fresh tab or surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful new-tab workflow as best-effort confirmation.",
    },
    "switch_tab": {
        "title": "Switch Tab",
        "category_hints": {"browser", "code_editor", "ide", "terminal", "ops_console", "utility", "file_manager"},
        "hotkey_field": "next_tab_hotkeys",
        "input_field": "query",
        "requires_input": True,
        "default_press_enter": False,
        "route_mode": "workflow_switch_tab",
        "missing_input_message": "query is required to switch tabs or sections.",
        "support_message": "No tab-switch workflow is configured for this app.",
        "hotkey_reason": "Switch the active tab or section using a hotkey chosen from the requested tab target.",
        "input_reason": "",
        "retry_label": "Tab Switch Retry",
        "retry_reason": "Retry with an alternate tab-navigation shortcut if the app remapped the standard tab controls.",
        "verification_success": "tab switch verified",
        "verification_failure": "tab switch finished, but JARVIS could not confirm the active tab or section changed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful tab switch as best-effort confirmation.",
        "verify_hint": "tab",
        "probe_terms": ["tab"],
        "recommended_followups": ["search", "close_tab", "navigate"],
        "surface_flag": "tabbed_surface_ready",
    },
    "close_tab": {
        "title": "Close Tab",
        "category_hints": {"browser", "code_editor", "ide", "file_manager"},
        "hotkey_field": "close_tab_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_close_tab",
        "missing_input_message": "",
        "support_message": "No close-tab workflow is configured for this app.",
        "hotkey_reason": "Close the active tab or editor surface in the current app.",
        "input_reason": "",
        "retry_label": "Close Tab Retry",
        "retry_reason": "Retry with an alternate close-tab shortcut for apps with custom bindings.",
        "verification_success": "close tab verified",
        "verification_failure": "close tab finished, but JARVIS could not confirm the active surface changed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful close-tab workflow as best-effort confirmation.",
    },
    "reopen_tab": {
        "title": "Reopen Tab",
        "category_hints": {"browser", "code_editor", "ide", "file_manager"},
        "hotkey_field": "reopen_tab_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_reopen_tab",
        "missing_input_message": "",
        "support_message": "No reopen-tab workflow is configured for this app.",
        "hotkey_reason": "Restore the most recently closed tab or editor surface.",
        "input_reason": "",
        "retry_label": "Reopen Tab Retry",
        "retry_reason": "Retry with an alternate reopen-tab shortcut for apps with custom bindings.",
        "verification_success": "reopen tab verified",
        "verification_failure": "reopen tab finished, but JARVIS could not confirm the previous surface was restored",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful reopen-tab workflow as best-effort confirmation.",
    },
    "open_history": {
        "title": "Open History",
        "category_hints": {"browser"},
        "hotkey_field": "history_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_history",
        "missing_input_message": "",
        "support_message": "No history workflow is configured for this app.",
        "hotkey_reason": "Open the browser history surface to inspect recent destinations and sessions.",
        "input_reason": "",
        "retry_label": "History Retry",
        "retry_reason": "Retry with an alternate history shortcut if the app remapped the standard browser binding.",
        "verification_success": "history verified",
        "verification_failure": "history finished, but JARVIS could not confirm the history surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful history workflow as best-effort confirmation.",
        "verify_hint": "history",
        "probe_terms": ["history", "recently closed"],
        "recommended_followups": ["navigate", "new_tab", "open_bookmarks"],
        "surface_flag": "history_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "open_downloads": {
        "title": "Open Downloads",
        "category_hints": {"browser"},
        "hotkey_field": "downloads_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_downloads",
        "missing_input_message": "",
        "support_message": "No downloads workflow is configured for this app.",
        "hotkey_reason": "Open the browser downloads surface to inspect recent files and transfers.",
        "input_reason": "",
        "retry_label": "Downloads Retry",
        "retry_reason": "Retry with an alternate downloads shortcut if the app remapped the standard browser binding.",
        "verification_success": "downloads verified",
        "verification_failure": "downloads finished, but JARVIS could not confirm the downloads surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful downloads workflow as best-effort confirmation.",
        "verify_hint": "downloads",
        "probe_terms": ["downloads", "download"],
        "recommended_followups": ["new_tab", "search", "navigate"],
        "surface_flag": "downloads_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "open_devtools": {
        "title": "Open DevTools",
        "category_hints": {"browser"},
        "hotkey_field": "devtools_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_devtools",
        "missing_input_message": "",
        "support_message": "No developer-tools workflow is configured for this app.",
        "hotkey_reason": "Open the app's developer tools surface for inspection and debugging.",
        "input_reason": "",
        "retry_label": "DevTools Retry",
        "retry_reason": "Retry with an alternate developer-tools shortcut for apps that prefer a different binding.",
        "verification_success": "developer tools verified",
        "verification_failure": "developer tools finished, but JARVIS could not confirm the tool surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful developer-tools workflow as best-effort confirmation.",
        "verify_hint": "elements",
        "probe_terms": ["developer tools", "elements", "console"],
        "recommended_followups": ["search", "navigate", "new_tab"],
        "surface_flag": "devtools_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "open_tab_search": {
        "title": "Open Tab Search",
        "category_hints": {"browser"},
        "hotkey_field": "tab_search_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_open_tab_search",
        "missing_input_message": "",
        "support_message": "No tab-search workflow is configured for this browser.",
        "hotkey_reason": "Open the browser's tab-search surface before filtering the active tab list.",
        "input_reason": "",
        "retry_label": "Tab Search Retry",
        "retry_reason": "Retry with an alternate tab-search shortcut if the browser remapped the tab switcher.",
        "verification_success": "tab search verified",
        "verification_failure": "tab search finished, but JARVIS could not confirm the open-tab search surface became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful tab-search workflow as best-effort confirmation.",
        "verify_hint": "search tabs",
        "probe_terms": ["search tabs", "search open tabs", "tab search", "open tabs"],
        "recommended_followups": ["search_tabs", "switch_tab", "close_tab"],
        "surface_flag": "tab_search_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "search_tabs": {
        "title": "Search Tabs",
        "category_hints": {"browser"},
        "hotkey_field": "tab_search_hotkeys",
        "input_field": "query",
        "requires_input": True,
        "default_press_enter": False,
        "route_mode": "workflow_search_tabs",
        "missing_input_message": "query is required to search browser tabs.",
        "support_message": "No tab-search workflow is configured for this browser.",
        "hotkey_reason": "Open the browser's tab-search surface before filtering the active tab list.",
        "input_reason": "Type the requested tab query into the browser's open-tab search surface.",
        "retry_label": "Search Tabs Retry",
        "retry_reason": "Retry with an alternate tab-search shortcut if the browser remapped the tab switcher.",
        "verification_success": "tab query verified",
        "verification_failure": "tab search finished, but JARVIS could not confirm the requested tab query became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful tab-search workflow as best-effort confirmation.",
        "verify_hint": "search tabs",
        "probe_terms": ["search tabs", "search open tabs", "tab search", "open tabs"],
        "recommended_followups": ["switch_tab", "close_tab", "new_tab"],
        "surface_flag": "tab_search_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
        "prep_workflows": ["open_tab_search"],
        "replace_primary_hotkey_with_prep": True,
    },
    "new_chat": {
        "title": "New Chat",
        "category_hints": {"chat"},
        "hotkey_field": "new_chat_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_new_chat",
        "missing_input_message": "",
        "support_message": "No new-chat workflow is configured for this chat app.",
        "hotkey_reason": "Open the chat app's new-conversation surface before a follow-up recipient or message action.",
        "input_reason": "",
        "retry_label": "New Chat Retry",
        "retry_reason": "Retry with an alternate new-conversation shortcut for chat apps that remap message composition.",
        "verification_success": "new chat verified",
        "verification_failure": "new chat finished, but JARVIS could not confirm the chat composer or recipient picker opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful new-chat workflow as best-effort confirmation.",
        "verify_hint": "new message",
        "probe_terms": ["new message", "new chat", "search or start new chat"],
        "recommended_followups": ["jump_to_conversation", "send_message"],
        "surface_flag": "conversation_picker_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "jump_to_conversation": {
        "title": "Jump To Conversation",
        "category_hints": {"chat"},
        "hotkey_field": "conversation_hotkeys",
        "input_field": "query",
        "requires_input": True,
        "default_press_enter": True,
        "route_mode": "workflow_jump_to_conversation",
        "missing_input_message": "query is required to target a chat conversation.",
        "support_message": "No conversation-switch workflow is configured for this chat app.",
        "hotkey_reason": "Open the chat app's conversation switcher before selecting the requested recipient or thread.",
        "input_reason": "Type the requested recipient or conversation target into the active switcher.",
        "retry_label": "Conversation Switch Retry",
        "retry_reason": "Retry with an alternate conversation-switch shortcut for chat apps that remap quick switching.",
        "verification_success": "conversation switch verified",
        "verification_failure": "conversation switch finished, but JARVIS could not confirm the requested conversation target became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful conversation switch as best-effort confirmation.",
        "probe_terms": ["search", "messages", "people"],
        "recommended_followups": ["send_message", "search"],
        "surface_flag": "conversation_picker_visible",
        "skip_hotkey_when_ready": True,
    },
    "send_message": {
        "title": "Send Message",
        "category_hints": {"chat", "ai_companion"},
        "hotkey_field": "conversation_hotkeys",
        "input_field": "text",
        "requires_input": True,
        "required_fields": ["text"],
        "default_press_enter": True,
        "route_mode": "workflow_send_message",
        "missing_input_message": "text is required to send a message.",
        "support_message": "No message-send workflow is configured for this app. Chat and AI companion profiles can usually type directly into the active composer.",
        "hotkey_reason": "Open the app's conversation switcher before targeting the requested recipient.",
        "input_reason": "Type and submit the requested message in the active conversation or prompt surface.",
        "retry_label": "Send Message Retry",
        "retry_reason": "Retry with an alternate conversation-switch shortcut before sending the requested message.",
        "verification_success": "message send verified",
        "verification_failure": "message send finished, but JARVIS could not confirm the requested message reached the active conversation",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful message-send workflow as best-effort confirmation.",
        "probe_terms": ["type a message", "write a message", "reply"],
        "recommended_followups": ["search", "jump_to_conversation"],
        "supports_without_hotkey_categories": {"chat", "ai_companion"},
        "hotkey_requires_target_query": True,
        "surface_flag": "message_compose_ready",
        "input_sequence": [
            {
                "field": "query",
                "phase": "workflow_target",
                "press_enter": True,
                "optional": True,
                "reason": "Select the requested recipient or conversation before sending the message.",
            },
            {
                "field": "text",
                "phase": "input",
                "press_enter": True,
                "reason": "Type and submit the requested message in the active conversation or prompt surface.",
            },
        ],
    },
    "new_email_draft": {
        "title": "New Email Draft",
        "category_hints": {"office"},
        "hotkey_field": "new_email_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_new_email_draft",
        "missing_input_message": "",
        "support_message": "No new-email workflow is configured for this app.",
        "hotkey_reason": "Open a fresh mail compose surface before filling recipients, subject lines, or body content.",
        "input_reason": "",
        "retry_label": "New Email Retry",
        "retry_reason": "Retry with an alternate compose shortcut for desktop mail apps that remap message creation.",
        "verification_success": "new email draft verified",
        "verification_failure": "new email draft finished, but JARVIS could not confirm the compose window opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful new-email workflow as best-effort confirmation.",
        "verify_hint": "subject",
        "probe_terms": ["new message", "subject", "to"],
        "recommended_followups": ["search", "save_document"],
        "surface_flag": "email_compose_ready",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "open_mail_view": {
        "title": "Open Mail View",
        "category_hints": {"office"},
        "hotkey_field": "mail_view_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_open_mail_view",
        "missing_input_message": "",
        "support_message": "No mail-view workflow is configured for this app.",
        "hotkey_reason": "Switch the mail client back to its primary mail or inbox view before follow-up actions.",
        "input_reason": "",
        "retry_label": "Mail View Retry",
        "retry_reason": "Retry with an alternate mail-view shortcut if the app remapped module switching.",
        "verification_success": "mail view verified",
        "verification_failure": "mail view finished, but JARVIS could not confirm the mail surface became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful mail-view workflow as best-effort confirmation.",
        "verify_hint": "inbox",
        "probe_terms": ["inbox", "mail", "message list"],
        "recommended_followups": ["new_email_draft", "search"],
        "surface_flag": "mail_view_active",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "open_calendar_view": {
        "title": "Open Calendar View",
        "category_hints": {"office"},
        "hotkey_field": "calendar_view_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_open_calendar_view",
        "missing_input_message": "",
        "support_message": "No calendar-view workflow is configured for this app.",
        "hotkey_reason": "Switch the mail client into its calendar surface before follow-up scheduling actions.",
        "input_reason": "",
        "retry_label": "Calendar View Retry",
        "retry_reason": "Retry with an alternate calendar-view shortcut if the app remapped module switching.",
        "verification_success": "calendar view verified",
        "verification_failure": "calendar view finished, but JARVIS could not confirm the calendar surface became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful calendar-view workflow as best-effort confirmation.",
        "verify_hint": "calendar",
        "probe_terms": ["calendar", "meeting", "schedule"],
        "recommended_followups": ["search", "new_email_draft"],
        "surface_flag": "calendar_view_active",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "open_people_view": {
        "title": "Open People View",
        "category_hints": {"office"},
        "hotkey_field": "people_view_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_open_people_view",
        "missing_input_message": "",
        "support_message": "No people-view workflow is configured for this app.",
        "hotkey_reason": "Switch the mail client into its people or contacts surface before follow-up contact actions.",
        "input_reason": "",
        "retry_label": "People View Retry",
        "retry_reason": "Retry with an alternate people-view shortcut if the app remapped module switching.",
        "verification_success": "people view verified",
        "verification_failure": "people view finished, but JARVIS could not confirm the contacts surface became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful people-view workflow as best-effort confirmation.",
        "verify_hint": "people",
        "probe_terms": ["people", "contacts", "contact list"],
        "recommended_followups": ["search", "new_email_draft"],
        "surface_flag": "people_view_active",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "open_tasks_view": {
        "title": "Open Tasks View",
        "category_hints": {"office"},
        "hotkey_field": "tasks_view_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_open_tasks_view",
        "missing_input_message": "",
        "support_message": "No tasks-view workflow is configured for this app.",
        "hotkey_reason": "Switch the mail client into its task or to-do surface before follow-up planning actions.",
        "input_reason": "",
        "retry_label": "Tasks View Retry",
        "retry_reason": "Retry with an alternate tasks-view shortcut if the app remapped module switching.",
        "verification_success": "tasks view verified",
        "verification_failure": "tasks view finished, but JARVIS could not confirm the tasks surface became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful tasks-view workflow as best-effort confirmation.",
        "verify_hint": "tasks",
        "probe_terms": ["tasks", "to do", "todo"],
        "recommended_followups": ["search", "new_email_draft"],
        "surface_flag": "tasks_view_active",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "focus_folder_pane": {
        "title": "Focus Folder Pane",
        "category_hints": {"office"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_folder_pane",
        "missing_input_message": "",
        "support_message": "No folder-pane workflow is configured for this mail app.",
        "hotkey_reason": "Focus the Outlook folder pane before mailbox navigation actions.",
        "input_reason": "",
        "retry_label": "Folder Pane Retry",
        "retry_reason": "Retry the folder-pane focus action if Outlook delayed accessibility exposure.",
        "verification_success": "folder pane focused",
        "verification_failure": "folder-pane focus finished, but JARVIS could not confirm the mailbox folder surface became ready",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful folder-pane focus workflow as best-effort confirmation.",
        "probe_terms": ["folder pane", "folders", "mail folders", "favorites"],
        "recommended_followups": ["open_mail_view", "open_calendar_view", "focus_message_list"],
        "surface_flag": "folder_pane_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Folder Pane", "action": "focus", "control_type": "Pane"},
        "workflow_action_reason": "Focus the Outlook folder pane through accessibility before mailbox navigation.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"office"},
    },
    "focus_message_list": {
        "title": "Focus Message List",
        "category_hints": {"office"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_message_list",
        "missing_input_message": "",
        "support_message": "No message-list workflow is configured for this mail app.",
        "hotkey_reason": "Focus the Outlook message list before triage or reply workflows.",
        "input_reason": "",
        "retry_label": "Message List Retry",
        "retry_reason": "Retry the message-list focus action if Outlook delayed accessibility exposure.",
        "verification_success": "message list focused",
        "verification_failure": "message-list focus finished, but JARVIS could not confirm the message list became ready",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful message-list focus workflow as best-effort confirmation.",
        "probe_terms": ["message list", "inbox list", "messages", "conversation list"],
        "recommended_followups": ["reply_email", "reply_all_email", "forward_email"],
        "surface_flag": "message_list_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Message List", "action": "focus", "control_type": "List"},
        "workflow_action_reason": "Focus the Outlook message list through accessibility before email triage actions.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"office"},
    },
    "focus_reading_pane": {
        "title": "Focus Reading Pane",
        "category_hints": {"office"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_focus_reading_pane",
        "missing_input_message": "",
        "support_message": "No reading-pane workflow is configured for this mail app.",
        "hotkey_reason": "Focus the Outlook reading pane before review or follow-up compose actions.",
        "input_reason": "",
        "retry_label": "Reading Pane Retry",
        "retry_reason": "Retry the reading-pane focus action if Outlook delayed accessibility exposure.",
        "verification_success": "reading pane focused",
        "verification_failure": "reading-pane focus finished, but JARVIS could not confirm the preview surface became ready",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful reading-pane focus workflow as best-effort confirmation.",
        "probe_terms": ["reading pane", "preview", "message preview", "reading"],
        "recommended_followups": ["reply_email", "reply_all_email", "forward_email"],
        "surface_flag": "reading_pane_visible",
        "workflow_action": "accessibility_invoke_element",
        "workflow_action_args": {"query": "Reading Pane", "action": "focus", "control_type": "Pane"},
        "workflow_action_reason": "Focus the Outlook reading pane through accessibility before follow-up mail actions.",
        "prefer_workflow_action": True,
        "supports_action_dispatch_categories": {"office"},
    },
    "reply_email": {
        "title": "Reply To Email",
        "category_hints": {"office"},
        "hotkey_field": "reply_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_reply_email",
        "missing_input_message": "",
        "support_message": "No email-reply workflow is configured for this app.",
        "hotkey_reason": "Open a reply compose surface for the active email before follow-up editing or sending actions.",
        "input_reason": "",
        "retry_label": "Reply Retry",
        "retry_reason": "Retry with an alternate reply shortcut if the mail app remapped compose actions.",
        "verification_success": "reply compose verified",
        "verification_failure": "reply finished, but JARVIS could not confirm the reply compose surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful reply workflow as best-effort confirmation.",
        "verify_hint": "subject",
        "probe_terms": ["reply", "subject", "to"],
        "recommended_followups": ["search", "save_document"],
        "surface_flag": "email_compose_ready",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "reply_all_email": {
        "title": "Reply All To Email",
        "category_hints": {"office"},
        "hotkey_field": "reply_all_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_reply_all_email",
        "missing_input_message": "",
        "support_message": "No reply-all workflow is configured for this app.",
        "hotkey_reason": "Open a reply-all compose surface for the active email before follow-up editing or sending actions.",
        "input_reason": "",
        "retry_label": "Reply All Retry",
        "retry_reason": "Retry with an alternate reply-all shortcut if the mail app remapped compose actions.",
        "verification_success": "reply all compose verified",
        "verification_failure": "reply all finished, but JARVIS could not confirm the reply-all compose surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful reply-all workflow as best-effort confirmation.",
        "verify_hint": "cc",
        "probe_terms": ["reply all", "subject", "cc"],
        "recommended_followups": ["search", "save_document"],
        "surface_flag": "email_compose_ready",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "forward_email": {
        "title": "Forward Email",
        "category_hints": {"office"},
        "hotkey_field": "forward_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_forward_email",
        "missing_input_message": "",
        "support_message": "No email-forward workflow is configured for this app.",
        "hotkey_reason": "Open a forward compose surface for the active email before follow-up editing or sending actions.",
        "input_reason": "",
        "retry_label": "Forward Retry",
        "retry_reason": "Retry with an alternate forward shortcut if the mail app remapped compose actions.",
        "verification_success": "forward compose verified",
        "verification_failure": "forward finished, but JARVIS could not confirm the forward compose surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful forward workflow as best-effort confirmation.",
        "verify_hint": "to",
        "probe_terms": ["forward", "subject", "to"],
        "recommended_followups": ["search", "save_document"],
        "surface_flag": "email_compose_ready",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "new_calendar_event": {
        "title": "New Calendar Event",
        "category_hints": {"office"},
        "hotkey_field": "new_calendar_event_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_new_calendar_event",
        "missing_input_message": "",
        "support_message": "No calendar-event workflow is configured for this app.",
        "hotkey_reason": "Open a fresh calendar event or meeting compose surface before adding attendees, titles, or timing details.",
        "input_reason": "",
        "retry_label": "New Event Retry",
        "retry_reason": "Retry with an alternate event-compose shortcut if the mail app remapped calendar actions.",
        "verification_success": "calendar event compose verified",
        "verification_failure": "new calendar event finished, but JARVIS could not confirm the event compose surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful calendar-event workflow as best-effort confirmation.",
        "verify_hint": "appointment",
        "probe_terms": ["new event", "appointment", "invite attendees", "all day"],
        "recommended_followups": ["open_calendar_view", "search"],
        "surface_flag": "calendar_event_compose_ready",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "new_document": {
        "title": "New Document",
        "category_hints": {"office", "code_editor", "ide"},
        "hotkey_field": "new_document_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_new_document",
        "missing_input_message": "",
        "support_message": "No new-document workflow is configured for this app.",
        "hotkey_reason": "Create a new document, workbook, presentation, or note in the target app.",
        "input_reason": "",
        "retry_label": "New Document Retry",
        "retry_reason": "Retry with an alternate new-document shortcut if the app remapped the binding.",
        "verification_success": "new document verified",
        "verification_failure": "new document finished, but JARVIS could not confirm a fresh document surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful new-document workflow as best-effort confirmation.",
        "recommended_followups": ["save_document", "open_print_dialog"],
    },
    "save_document": {
        "title": "Save Document",
        "category_hints": {"office", "code_editor", "ide"},
        "hotkey_field": "save_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_save_document",
        "missing_input_message": "",
        "support_message": "No save-document workflow is configured for this app.",
        "hotkey_reason": "Save the active document, workbook, note, or editor buffer.",
        "input_reason": "",
        "retry_label": "Save Retry",
        "retry_reason": "Retry with an alternate save shortcut if the app remapped the binding.",
        "verification_success": "save verified",
        "verification_failure": "save finished, but JARVIS could not confirm the active document state persisted",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful save workflow as best-effort confirmation.",
        "recommended_followups": ["open_print_dialog", "search"],
    },
    "open_print_dialog": {
        "title": "Open Print Dialog",
        "category_hints": {"office", "browser", "code_editor", "ide", "general_desktop"},
        "hotkey_field": "print_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_print_dialog",
        "missing_input_message": "",
        "support_message": "No print-dialog workflow is configured for this app.",
        "hotkey_reason": "Open the current app's print surface for export, PDF, or hard-copy actions.",
        "input_reason": "",
        "retry_label": "Print Dialog Retry",
        "retry_reason": "Retry with an alternate print shortcut if the app remapped the binding.",
        "verification_success": "print dialog verified",
        "verification_failure": "print dialog finished, but JARVIS could not confirm the print surface opened",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful print-dialog workflow as best-effort confirmation.",
        "verify_hint": "print",
        "probe_terms": ["print", "printer"],
        "recommended_followups": ["save_document", "new_document"],
        "surface_flag": "print_dialog_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "start_presentation": {
        "title": "Start Presentation",
        "category_hints": {"office"},
        "hotkey_field": "presentation_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_start_presentation",
        "missing_input_message": "",
        "support_message": "No presentation-start workflow is configured for this app.",
        "hotkey_reason": "Start the active slideshow or presentation surface.",
        "input_reason": "",
        "retry_label": "Presentation Retry",
        "retry_reason": "Retry with an alternate slideshow shortcut if the app remapped the presentation binding.",
        "verification_success": "presentation verified",
        "verification_failure": "presentation finished, but JARVIS could not confirm slideshow mode became active",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful presentation start as best-effort confirmation.",
        "verify_hint": "slide show",
        "probe_terms": ["slide show", "presenter view", "slideshow"],
        "recommended_followups": ["save_document", "open_print_dialog"],
        "surface_flag": "presentation_active",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "toggle_terminal": {
        "title": "Toggle Terminal",
        "category_hints": {"code_editor", "ide"},
        "hotkey_field": "toggle_terminal_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_toggle_terminal",
        "missing_input_message": "",
        "support_message": "No integrated-terminal workflow is configured for this app.",
        "hotkey_reason": "Toggle the integrated terminal surface for editor or IDE workflows.",
        "input_reason": "",
        "retry_label": "Terminal Toggle Retry",
        "retry_reason": "Retry with an alternate terminal shortcut for IDE-style apps.",
        "verification_success": "terminal verified",
        "verification_failure": "terminal toggle finished, but JARVIS could not confirm the terminal surface changed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful terminal-toggle workflow as best-effort confirmation.",
        "verify_hint": "terminal",
        "probe_terms": ["terminal", "problems"],
        "recommended_followups": ["terminal_command", "workspace_search"],
        "surface_flag": "terminal_visible",
        "skip_hotkey_when_ready": True,
        "preserve_ready_surface": True,
    },
    "format_document": {
        "title": "Format Document",
        "category_hints": {"code_editor", "ide"},
        "hotkey_field": "format_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_format_document",
        "missing_input_message": "",
        "support_message": "No document-format workflow is configured for this app.",
        "hotkey_reason": "Run the app's formatting shortcut to normalize the active document or selection.",
        "input_reason": "",
        "retry_label": "Format Retry",
        "retry_reason": "Retry with an alternate format shortcut for editor-style apps.",
        "verification_success": "format verified",
        "verification_failure": "format finished, but JARVIS could not confirm the document changed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful formatting workflow as best-effort confirmation.",
    },
    "zoom_in": {
        "title": "Zoom In",
        "category_hints": {"browser", "code_editor", "ide", "office", "utility"},
        "hotkey_field": "zoom_in_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_zoom_in",
        "missing_input_message": "",
        "support_message": "No zoom-in workflow is configured for this app.",
        "hotkey_reason": "Increase the zoom level for the active document or surface.",
        "input_reason": "",
        "retry_label": "Zoom In Retry",
        "retry_reason": "Retry with an alternate zoom-in shortcut if the app remapped the binding.",
        "verification_success": "zoom in verified",
        "verification_failure": "zoom in finished, but JARVIS could not confirm that the surface zoom level changed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful zoom-in workflow as best-effort confirmation.",
        "verify_hint": "zoom",
        "probe_terms": ["zoom"],
        "recommended_followups": ["search", "open_print_dialog"],
        "surface_flag": "zoomable_surface",
    },
    "zoom_out": {
        "title": "Zoom Out",
        "category_hints": {"browser", "code_editor", "ide", "office", "utility"},
        "hotkey_field": "zoom_out_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_zoom_out",
        "missing_input_message": "",
        "support_message": "No zoom-out workflow is configured for this app.",
        "hotkey_reason": "Decrease the zoom level for the active document or surface.",
        "input_reason": "",
        "retry_label": "Zoom Out Retry",
        "retry_reason": "Retry with an alternate zoom-out shortcut if the app remapped the binding.",
        "verification_success": "zoom out verified",
        "verification_failure": "zoom out finished, but JARVIS could not confirm that the surface zoom level changed",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful zoom-out workflow as best-effort confirmation.",
        "verify_hint": "zoom",
        "probe_terms": ["zoom"],
        "recommended_followups": ["search", "open_print_dialog"],
        "surface_flag": "zoomable_surface",
    },
    "reset_zoom": {
        "title": "Reset Zoom",
        "category_hints": {"browser", "code_editor", "ide", "office", "utility"},
        "hotkey_field": "reset_zoom_hotkeys",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_reset_zoom",
        "missing_input_message": "",
        "support_message": "No reset-zoom workflow is configured for this app.",
        "hotkey_reason": "Reset the active surface to its default zoom level.",
        "input_reason": "",
        "retry_label": "Reset Zoom Retry",
        "retry_reason": "Retry with an alternate reset-zoom shortcut if the app remapped the binding.",
        "verification_success": "reset zoom verified",
        "verification_failure": "reset zoom finished, but JARVIS could not confirm that the default zoom level was restored",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful reset-zoom workflow as best-effort confirmation.",
        "verify_hint": "100%",
        "probe_terms": ["100%", "zoom"],
        "recommended_followups": ["search", "open_print_dialog"],
        "surface_flag": "zoomable_surface",
    },
    "play_pause_media": {
        "title": "Play Or Pause Media",
        "category_hints": {"media"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_play_pause_media",
        "missing_input_message": "",
        "support_message": "No focused media-control workflow is configured for this app.",
        "hotkey_reason": "",
        "workflow_action": "media_play_pause",
        "workflow_action_reason": "Dispatch the focused app's media transport toggle through the native Windows media session controls.",
        "prefer_workflow_action": True,
        "self_verifying": True,
        "retry_label": "Media Toggle Retry",
        "retry_reason": "Retry the media transport toggle after refocusing the requested media surface.",
        "verification_success": "media transport toggled",
        "verification_failure": "media toggle finished, but JARVIS could not confirm the focused media transport changed state",
        "vision_warning": "Visual playback signals were unavailable, so JARVIS accepted the successful media transport result as best-effort confirmation.",
        "verify_hint": "playback",
        "probe_terms": ["play", "pause", "playing"],
        "recommended_followups": ["next_track", "previous_track", "stop_media"],
        "supports_without_hotkey_categories": {"media"},
        "supports_system_action_categories": {"media"},
        "surface_flag": "media_surface_ready",
    },
    "pause_media": {
        "title": "Pause Media",
        "category_hints": {"media"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_pause_media",
        "missing_input_message": "",
        "support_message": "No focused pause-media workflow is configured for this app.",
        "hotkey_reason": "",
        "workflow_action": "media_pause",
        "workflow_action_reason": "Pause the focused media session through the native Windows media transport controls.",
        "prefer_workflow_action": True,
        "self_verifying": True,
        "retry_label": "Media Pause Retry",
        "retry_reason": "Retry the media pause after refocusing the requested media surface.",
        "verification_success": "media paused",
        "verification_failure": "pause finished, but JARVIS could not confirm the focused media session paused",
        "vision_warning": "Visual playback signals were unavailable, so JARVIS accepted the successful media pause result as best-effort confirmation.",
        "verify_hint": "pause",
        "probe_terms": ["pause", "paused"],
        "recommended_followups": ["resume_media", "next_track"],
        "supports_without_hotkey_categories": {"media"},
        "supports_system_action_categories": {"media"},
        "surface_flag": "media_surface_ready",
    },
    "resume_media": {
        "title": "Resume Media",
        "category_hints": {"media"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_resume_media",
        "missing_input_message": "",
        "support_message": "No focused resume-media workflow is configured for this app.",
        "hotkey_reason": "",
        "workflow_action": "media_play",
        "workflow_action_reason": "Resume playback for the focused media session through the native Windows media transport controls.",
        "prefer_workflow_action": True,
        "self_verifying": True,
        "retry_label": "Media Resume Retry",
        "retry_reason": "Retry the media resume after refocusing the requested media surface.",
        "verification_success": "media resumed",
        "verification_failure": "resume finished, but JARVIS could not confirm the focused media session resumed",
        "vision_warning": "Visual playback signals were unavailable, so JARVIS accepted the successful media resume result as best-effort confirmation.",
        "verify_hint": "play",
        "probe_terms": ["play", "playing"],
        "recommended_followups": ["pause_media", "next_track"],
        "supports_without_hotkey_categories": {"media"},
        "supports_system_action_categories": {"media"},
        "surface_flag": "media_surface_ready",
    },
    "next_track": {
        "title": "Next Track",
        "category_hints": {"media"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_next_track",
        "missing_input_message": "",
        "support_message": "No focused next-track workflow is configured for this app.",
        "hotkey_reason": "",
        "workflow_action": "media_next",
        "workflow_action_reason": "Advance the focused media session to the next track through the native Windows media transport controls.",
        "prefer_workflow_action": True,
        "self_verifying": True,
        "retry_label": "Next Track Retry",
        "retry_reason": "Retry the next-track control after refocusing the requested media surface.",
        "verification_success": "next track dispatched",
        "verification_failure": "next-track finished, but JARVIS could not confirm the focused media session advanced",
        "vision_warning": "Visual playback signals were unavailable, so JARVIS accepted the successful next-track result as best-effort confirmation.",
        "verify_hint": "track",
        "probe_terms": ["track", "song", "playing"],
        "recommended_followups": ["previous_track", "pause_media", "stop_media"],
        "supports_without_hotkey_categories": {"media"},
        "supports_system_action_categories": {"media"},
        "surface_flag": "media_surface_ready",
    },
    "previous_track": {
        "title": "Previous Track",
        "category_hints": {"media"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_previous_track",
        "missing_input_message": "",
        "support_message": "No focused previous-track workflow is configured for this app.",
        "hotkey_reason": "",
        "workflow_action": "media_previous",
        "workflow_action_reason": "Return the focused media session to the previous track through the native Windows media transport controls.",
        "prefer_workflow_action": True,
        "self_verifying": True,
        "retry_label": "Previous Track Retry",
        "retry_reason": "Retry the previous-track control after refocusing the requested media surface.",
        "verification_success": "previous track dispatched",
        "verification_failure": "previous-track finished, but JARVIS could not confirm the focused media session moved back",
        "vision_warning": "Visual playback signals were unavailable, so JARVIS accepted the successful previous-track result as best-effort confirmation.",
        "verify_hint": "track",
        "probe_terms": ["track", "song", "playing"],
        "recommended_followups": ["next_track", "pause_media", "stop_media"],
        "supports_without_hotkey_categories": {"media"},
        "supports_system_action_categories": {"media"},
        "surface_flag": "media_surface_ready",
    },
    "stop_media": {
        "title": "Stop Media",
        "category_hints": {"media"},
        "hotkey_field": "",
        "input_field": "none",
        "requires_input": False,
        "default_press_enter": False,
        "route_mode": "workflow_stop_media",
        "missing_input_message": "",
        "support_message": "No focused stop-media workflow is configured for this app.",
        "hotkey_reason": "",
        "workflow_action": "media_stop",
        "workflow_action_reason": "Stop playback for the focused media session through the native Windows media transport controls.",
        "prefer_workflow_action": True,
        "self_verifying": True,
        "retry_label": "Stop Media Retry",
        "retry_reason": "Retry the media stop after refocusing the requested media surface.",
        "verification_success": "media stopped",
        "verification_failure": "stop finished, but JARVIS could not confirm the focused media session stopped",
        "vision_warning": "Visual playback signals were unavailable, so JARVIS accepted the successful media stop result as best-effort confirmation.",
        "verify_hint": "stopped",
        "probe_terms": ["stopped", "pause", "playback"],
        "recommended_followups": ["resume_media", "next_track"],
        "supports_without_hotkey_categories": {"media"},
        "supports_system_action_categories": {"media"},
        "surface_flag": "media_surface_ready",
    },
    "terminal_command": {
        "title": "Terminal Command",
        "category_hints": {"terminal", "code_editor", "ide"},
        "hotkey_field": "terminal_hotkeys",
        "input_field": "text",
        "requires_input": True,
        "default_press_enter": True,
        "route_mode": "workflow_terminal_command",
        "missing_input_message": "text is required for terminal command workflows.",
        "support_message": "No terminal-command workflow is configured for this app. Terminal profiles can type commands directly and editor profiles expose integrated terminal shortcuts.",
        "hotkey_reason": "Open the app's terminal surface before typing the requested shell command.",
        "input_reason": "Type the requested shell command into the active terminal surface.",
        "retry_label": "Terminal Surface Retry",
        "retry_reason": "Retry with an alternate terminal shortcut before dispatching the requested command.",
        "verification_success": "terminal command verified",
        "verification_failure": "terminal command finished, but JARVIS could not confirm the command reached the intended terminal surface",
        "vision_warning": "Visual verification was unavailable, so JARVIS accepted the successful terminal command workflow as best-effort confirmation.",
        "supports_without_hotkey_categories": {"terminal"},
        "probe_terms": ["terminal"],
        "recommended_followups": ["workspace_search", "quick_open"],
        "surface_flag": "terminal_visible",
        "skip_hotkey_when_ready": True,
        "prep_workflows": ["toggle_terminal"],
        "replace_primary_hotkey_with_prep": True,
    },
}

WORKFLOW_ACTIONS = frozenset(WORKFLOW_DEFINITIONS)


class DesktopActionRouter:
    def __init__(
        self,
        *,
        action_handlers: Optional[Dict[str, ActionHandler]] = None,
        app_profile_registry: Optional[DesktopAppProfileRegistry] = None,
        workflow_memory: Optional[DesktopWorkflowMemory] = None,
        mission_memory: Optional[DesktopMissionMemory] = None,
        settle_delay_s: float = 0.35,
    ) -> None:
        self._handlers = self._default_handlers()
        if isinstance(action_handlers, dict):
            self._handlers.update({str(key): value for key, value in action_handlers.items() if callable(value)})
        self._app_profile_registry = app_profile_registry or DesktopAppProfileRegistry()
        self._workflow_memory = workflow_memory or DesktopWorkflowMemory.default()
        self._mission_memory = mission_memory or DesktopMissionMemory.default()
        self._surface_intelligence = SurfaceIntelligenceAnalyzer()
        self.settle_delay_s = max(0.0, min(float(settle_delay_s), 5.0))

    def advise(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        args = self._normalize_payload(payload)
        requested_action = str(args.get("action", "observe") or "observe")
        if requested_action == "resume_mission":
            return self._advise_resume_mission(args=args)
        if requested_action == EXPLORATION_ADVANCE_ACTION:
            return self._advise_surface_exploration_advance(args=args)
        if requested_action == EXPLORATION_FLOW_ACTION:
            return self._advise_surface_exploration_flow(args=args)
        app_profile = self._resolve_app_profile(args=args)
        args, defaults_applied = self._apply_profile_defaults(args=args, app_profile=app_profile)
        workflow_profile = self._workflow_profile(requested_action=requested_action, args=args, app_profile=app_profile)
        capabilities = self._capabilities()
        windows = self._list_windows()
        active_window = self._active_window()
        candidates = self._rank_window_candidates(
            windows=windows,
            active_window=active_window,
            app_name=str(args.get("app_name", "") or ""),
            window_title=str(args.get("window_title", "") or ""),
            app_profile=app_profile,
        )
        primary_candidate = candidates[0] if candidates else {}
        refined_profile = self._resolve_app_profile(args=args, primary_candidate=primary_candidate, active_window=active_window)
        if refined_profile.get("status") == "success" and refined_profile.get("profile_id") != app_profile.get("profile_id"):
            app_profile = refined_profile
            args, extra_defaults = self._apply_profile_defaults(args=args, app_profile=app_profile)
            defaults_applied.update(extra_defaults)
            workflow_profile = self._workflow_profile(requested_action=requested_action, args=args, app_profile=app_profile)
            candidates = self._rank_window_candidates(
                windows=windows,
                active_window=active_window,
                app_name=str(args.get("app_name", "") or ""),
                window_title=str(args.get("window_title", "") or ""),
                app_profile=app_profile,
            )
            primary_candidate = candidates[0] if candidates else {}

        blockers: List[str] = []
        warnings: List[str] = []
        plan: List[Dict[str, Any]] = []
        warnings.extend([str(item).strip() for item in app_profile.get("warnings", []) if str(item).strip()])

        workflow_definition = self._workflow_definition(requested_action)
        missing_workflow_fields = self._workflow_missing_required_fields(requested_action=requested_action, args=args)

        if requested_action == "launch" and not str(args.get("app_name", "") or "").strip():
            blockers.append("app_name is required to launch an application.")
        if requested_action in WORKFLOW_ACTIONS and missing_workflow_fields:
            blockers.append(str(workflow_definition.get("missing_input_message", "") or "workflow input is required."))
        if requested_action in {"click", "click_and_type"} and not str(args.get("query", "") or "").strip():
            blockers.append("query is required for click-oriented desktop interaction.")
        if requested_action in {"type", "click_and_type"} and not str(args.get("text", "") or "").strip():
            blockers.append("text is required for typing interactions.")
        if requested_action == "hotkey" and not list(args.get("keys", [])):
            blockers.append("keys are required for hotkey interactions.")
        if requested_action in WORKFLOW_ACTIONS and not bool(workflow_profile.get("supported", False)):
            blockers.append(
                str(workflow_profile.get("message", "") or "No workflow hotkeys are configured for this desktop interaction.")
            )
        if requested_action in {"click", "click_and_type"} and not (
            bool(capabilities["accessibility"].get("available")) or bool(capabilities["vision"].get("available"))
        ):
            blockers.append("Neither accessibility automation nor OCR vision targeting is available.")
        if requested_action == "observe" and not bool(capabilities["vision"].get("available")):
            blockers.append("Vision capture dependencies are unavailable for screen observation.")

        app_name = str(args.get("app_name", "") or "").strip()
        window_title = str(args.get("window_title", "") or "").strip()
        ensure_app_launch = bool(args.get("ensure_app_launch", False))
        focus_first = bool(args.get("focus_first", True))
        active_matches = self._window_matches(active_window, app_name=app_name, window_title=window_title)

        if app_name and not candidates and ensure_app_launch and requested_action in {"launch", "focus", "click", "type", "click_and_type", "hotkey", *WORKFLOW_ACTIONS}:
            plan.append(
                self._plan_step(
                    action="open_app",
                    args={"app_name": app_name},
                    phase="bootstrap",
                    optional=False,
                    reason="No matching window is currently open, so the app should be launched first.",
                )
            )
            warnings.append(f"No running window matched '{app_name}'. The router will launch it first.")
        elif app_name and not candidates and requested_action in {"focus", "click", "type", "click_and_type", "hotkey", *WORKFLOW_ACTIONS}:
            warnings.append(f"No running window matched '{app_name}'. Enable ensure_app_launch to open it automatically.")

        focus_title = str(primary_candidate.get("title", "") or window_title or app_name).strip()
        focus_hwnd = self._to_int(primary_candidate.get("hwnd"))
        active_hwnd = self._to_int(active_window.get("hwnd"))
        target_already_focused = bool(
            active_matches
            or (active_hwnd is not None and focus_hwnd is not None and active_hwnd == focus_hwnd)
        )
        if requested_action in {"focus", "click", "type", "click_and_type", "hotkey", *WORKFLOW_ACTIONS} and focus_first and focus_title and not target_already_focused:
            focus_args: Dict[str, Any] = {"title": focus_title}
            if focus_hwnd is not None and focus_hwnd > 0:
                focus_args["hwnd"] = focus_hwnd
            plan.append(
                self._plan_step(
                    action="focus_window",
                    args=focus_args,
                    phase="focus",
                    optional=False,
                    reason="Bring the target app/window to the foreground before sending desktop input.",
                )
            )

        surface_preflight = {
            "enabled": False,
            "snapshot": {},
            "skip_primary_hotkey": False,
            "prep_steps": [],
            "warnings": [],
            "candidate_prep_actions": [],
            "prep_actions": [],
        }
        if requested_action in WORKFLOW_ACTIONS:
            surface_preflight = self._workflow_surface_preflight(
                requested_action=requested_action,
                args=args,
                app_profile=app_profile,
                capabilities=capabilities,
                active_window=active_window,
                primary_candidate=primary_candidate,
            )
            warnings.extend([str(item).strip() for item in surface_preflight.get("warnings", []) if str(item).strip()])
            if bool(surface_preflight.get("target_query_already_active", False)):
                args["_target_query_already_active"] = True
            runtime_overrides = self._workflow_stateful_overrides(
                requested_action=requested_action,
                args=args,
                snapshot=surface_preflight.get("snapshot", {}),
            )
            if isinstance(runtime_overrides.get("arg_updates", {}), dict):
                args.update(runtime_overrides.get("arg_updates", {}))
            warnings.extend([str(item).strip() for item in runtime_overrides.get("warnings", []) if str(item).strip()])
            surface_preflight["target_state_ready"] = bool(runtime_overrides.get("target_state_ready", False))
            surface_preflight["form_target_state"] = (
                runtime_overrides.get("form_target_state", {})
                if isinstance(runtime_overrides.get("form_target_state", {}), dict)
                else {}
            )
            if bool(runtime_overrides.get("target_state_ready", False)):
                surface_preflight["prep_steps"] = []
                surface_preflight["prep_actions"] = []

        if requested_action in {"click", "click_and_type"}:
            click_args = {
                "query": str(args.get("query", "") or ""),
                "target_mode": str(args.get("target_mode", "auto") or "auto"),
                "verify_mode": str(args.get("verify_mode", "state_or_visibility") or "state_or_visibility"),
            }
            if str(args.get("verify_text", "") or "").strip():
                click_args["verify_text"] = str(args.get("verify_text", "") or "").strip()
            if focus_title:
                click_args["window_title"] = focus_title
            if str(args.get("control_type", "") or "").strip():
                click_args["control_type"] = str(args.get("control_type"))
            if str(args.get("element_id", "") or "").strip():
                click_args["element_id"] = str(args.get("element_id"))
            plan.append(
                self._plan_step(
                    action="computer_click_target",
                    args=click_args,
                    phase="target",
                    optional=False,
                    reason="Use accessibility-first targeting with OCR fallback for resilient cross-app clicking.",
                )
            )

        if requested_action in {"type", "click_and_type"}:
            plan.append(
                self._plan_step(
                    action="keyboard_type",
                    args={
                        "text": str(args.get("text", "") or ""),
                        "press_enter": bool(args.get("press_enter", False) or args.get("submit", False)),
                    },
                    phase="input",
                    optional=False,
                    reason="Send the requested text to the focused desktop target.",
                )
            )

        if requested_action == "hotkey":
            plan.append(
                self._plan_step(
                    action="keyboard_hotkey",
                    args={"keys": list(args.get("keys", []))},
                    phase="input",
                    optional=False,
                    reason="Dispatch the requested key chord against the focused window.",
                )
            )

        if requested_action in WORKFLOW_ACTIONS:
            for preflight_step in surface_preflight.get("prep_steps", []):
                if isinstance(preflight_step, dict):
                    plan.append(preflight_step)
            workflow_hotkeys = workflow_profile.get("hotkeys", []) if isinstance(workflow_profile.get("hotkeys", []), list) else []
            primary_hotkey = workflow_hotkeys[0] if workflow_hotkeys and isinstance(workflow_hotkeys[0], list) else []
            workflow_action_name = str(workflow_profile.get("workflow_action", "") or "").strip().lower()
            workflow_action_args = self._resolve_workflow_action_args(workflow_profile.get("workflow_action_args", {}), args)
            if not isinstance(workflow_action_args, dict):
                workflow_action_args = {}
            workflow_action_override = args.get("_workflow_action_args_override", {})
            if isinstance(workflow_action_override, dict) and workflow_action_override:
                workflow_action_args.update(
                    {
                        str(key): value
                        for key, value in workflow_action_override.items()
                        if str(key).strip() and value not in (None, "", [], {})
                    }
                )
            if workflow_action_name == "accessibility_invoke_element" and focus_title and not str(workflow_action_args.get("window_title", "") or "").strip():
                workflow_action_args["window_title"] = focus_title
            use_workflow_action = bool(
                workflow_action_name
                and (
                    workflow_profile.get("supports_system_action", False)
                    or workflow_profile.get("supports_action_dispatch", False)
                )
                and (workflow_profile.get("prefer_workflow_action", False) or not primary_hotkey)
            )
            hotkey_requires_target_query = bool(workflow_definition.get("hotkey_requires_target_query", False))
            has_target_query = bool(str(args.get("query", "") or "").strip())
            if use_workflow_action and not bool(args.get("_skip_workflow_action", False)):
                plan.append(
                    self._plan_step(
                        action=workflow_action_name,
                        args=workflow_action_args,
                        phase="workflow",
                        optional=False,
                        reason=str(
                            workflow_profile.get("workflow_action_reason", "")
                            or "Dispatch the requested workflow through a native desktop control action."
                        ),
                    )
                )
            elif (
                primary_hotkey
                and not bool(args.get("_skip_primary_hotkey", False))
                and not bool(surface_preflight.get("skip_primary_hotkey", False))
                and not (hotkey_requires_target_query and not has_target_query)
            ):
                plan.append(
                    self._plan_step(
                        action="keyboard_hotkey",
                        args={"keys": list(primary_hotkey)},
                        phase="workflow",
                        optional=False,
                        reason=str(workflow_profile.get("hotkey_reason", "") or "Prepare the target app for the requested workflow."),
                    )
                )
            workflow_steps = self._workflow_input_steps(
                requested_action=requested_action,
                args=args,
                workflow_profile=workflow_profile,
            )
            for workflow_step in workflow_steps:
                if isinstance(workflow_step, dict):
                    plan.append(workflow_step)

        if requested_action == "observe":
            plan.append(
                self._plan_step(
                    action="computer_observe",
                    args={"include_targets": bool(args.get("include_targets", False))},
                    phase="observe",
                    optional=False,
                    reason="Capture the current screen and OCR state for grounded desktop reasoning.",
                )
            )

        surface_snapshot = surface_preflight.get("snapshot", {}) if isinstance(surface_preflight.get("snapshot", {}), dict) else {}
        safety_signals = surface_snapshot.get("safety_signals", {}) if isinstance(surface_snapshot.get("safety_signals", {}), dict) else {}

        route_mode = self._route_mode(requested_action=requested_action, args=args, capabilities=capabilities, app_profile=app_profile)
        confidence = self._confidence(
            requested_action=requested_action,
            primary_candidate=primary_candidate,
            capabilities=capabilities,
            blockers=blockers,
            warnings=warnings,
            app_profile=app_profile,
        )
        risk_level = str(app_profile.get("risk_posture", "") or "").strip().lower() or (
            "low" if requested_action in {"observe", "focus"} else ("medium" if requested_action in {"launch", "hotkey"} else "medium")
        )
        risky_confirmation_actions = {"confirm_dialog", "press_dialog_button", "next_wizard_step", "finish_wizard", "complete_form_page", "complete_form_flow"}
        if requested_action in risky_confirmation_actions and any(
            bool(safety_signals.get(key, False))
            for key in (
                "warning_surface_visible",
                "destructive_warning_visible",
                "elevation_prompt_visible",
                "permission_review_visible",
                "requires_confirmation",
                "admin_approval_required",
            )
        ):
            risk_level = "high"
        if requested_action in risky_confirmation_actions and bool(safety_signals.get("destructive_warning_visible", False)):
            warnings.append(
                "Surface safety detected a destructive confirmation prompt, so review the pending change carefully before continuing."
            )
        if requested_action in risky_confirmation_actions and bool(safety_signals.get("warning_surface_visible", False)):
            warnings.append(
                "Surface safety detected an elevated warning or review step, so JARVIS is treating this confirmation path as high risk."
            )
        if requested_action in risky_confirmation_actions and bool(safety_signals.get("elevation_prompt_visible", False)):
            warnings.append(
                "Surface safety detected an elevation prompt, so the action may require administrator approval or trigger privileged system changes."
            )
        if requested_action in risky_confirmation_actions and bool(safety_signals.get("permission_review_visible", False)):
            warnings.append(
                "Surface safety detected a permission or consent review prompt, so JARVIS is treating the action as high risk until an explicit approval surface is reviewed."
            )
        if requested_action in risky_confirmation_actions and bool(safety_signals.get("secure_desktop_likely", False)):
            warnings.append(
                "Surface safety detected a likely secure desktop prompt, so follow-up actions may require explicit administrator review instead of normal UI automation."
            )
        if requested_action in {"next_wizard_step", "finish_wizard"} and bool(safety_signals.get("requires_confirmation", False)):
            warnings.append(
                "Surface safety detected a confirmation-style wizard step, so advancing may commit changes instead of only moving through the installer."
            )
        if requested_action in {"complete_form_page", "complete_form_flow"} and bool(safety_signals.get("requires_confirmation", False)):
            warnings.append(
                "Surface safety detected a form or dialog confirmation path, so JARVIS is treating the commit workflow as a potentially state-changing operation."
            )

        status = "blocked" if blockers else "success"
        strategy_variants = self._build_strategy_variants(args=args, capabilities=capabilities, app_profile=app_profile)
        adaptive_strategy = self._workflow_memory.recommend(
            action=requested_action,
            args=args,
            app_profile=app_profile,
            variants=strategy_variants,
        )
        if isinstance(adaptive_strategy, dict) and isinstance(adaptive_strategy.get("variants", []), list) and adaptive_strategy.get("variants"):
            strategy_variants = [row for row in adaptive_strategy.get("variants", []) if isinstance(row, dict)]
        exploration_plan: Dict[str, Any] = {}
        if requested_action == "observe" or bool(blockers):
            exploration_snapshot = surface_snapshot if isinstance(surface_snapshot, dict) and surface_snapshot else self.surface_snapshot(
                app_name=app_name,
                window_title=window_title,
                query=str(args.get("query", "") or "").strip(),
                limit=max(12, int(args.get("max_strategy_attempts", 2) or 2) * 8),
                include_observation=True,
                include_elements=True,
                include_workflow_probes=True,
            )
            if isinstance(exploration_snapshot, dict) and exploration_snapshot.get("status") == "success":
                surface_snapshot = exploration_snapshot
                safety_signals = surface_snapshot.get("safety_signals", {}) if isinstance(surface_snapshot.get("safety_signals", {}), dict) else {}
                exploration_plan = self._surface_exploration_from_snapshot(
                    snapshot=exploration_snapshot,
                    app_name=app_name,
                    window_title=window_title,
                    query=str(args.get("query", "") or "").strip(),
                    limit=6,
                )
        return {
            "status": status,
            "action": requested_action,
            "route_mode": route_mode,
            "confidence": confidence,
            "risk_level": risk_level,
            "app_profile": app_profile if app_profile.get("status") == "success" else {},
            "workflow_profile": workflow_profile,
            "profile_defaults_applied": defaults_applied,
            "target_window": primary_candidate,
            "active_window": active_window,
            "candidate_windows": candidates[:6],
            "capabilities": capabilities,
            "execution_plan": plan,
            "blockers": self._dedupe_strings(blockers),
            "warnings": self._dedupe_strings(warnings),
            "autonomy": {
                "ensure_app_launch": ensure_app_launch,
                "focus_first": focus_first,
                "supports_cross_app_fallback": bool(capabilities["vision"].get("available")) and bool(capabilities["accessibility"].get("available")),
                "requires_visual": requested_action in {"click", "click_and_type", "observe"},
            },
            "surface_snapshot": surface_snapshot,
            "safety_signals": safety_signals,
            "form_target_state": surface_preflight.get("form_target_state", {}) if isinstance(surface_preflight.get("form_target_state", {}), dict) else {},
            "surface_branch": {
                "enabled": bool(surface_preflight.get("enabled", False)),
                "surface_flag": str(surface_preflight.get("surface_flag", "") or ""),
                "surface_ready": bool(surface_preflight.get("surface_ready", False)),
                "skip_primary_hotkey": bool(surface_preflight.get("skip_primary_hotkey", False)),
                "candidate_prep_actions": list(surface_preflight.get("candidate_prep_actions", [])),
                "prep_actions": list(surface_preflight.get("prep_actions", [])),
                "target_query_already_active": bool(surface_preflight.get("target_query_already_active", False)),
                "target_state_ready": bool(surface_preflight.get("target_state_ready", False)),
                "form_target_state": surface_preflight.get("form_target_state", {}) if isinstance(surface_preflight.get("form_target_state", {}), dict) else {},
            },
            "verification_plan": self._verification_plan(
                args=args,
                primary_candidate=primary_candidate,
                capabilities=capabilities,
                app_profile=app_profile,
            ),
            "adaptive_strategy": adaptive_strategy,
            "strategy_variants": strategy_variants,
            "exploration_plan": exploration_plan,
        }

    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        args = self._normalize_payload(payload)
        advice = self.advise(args)
        if advice.get("status") != "success":
            message = "; ".join(str(item) for item in advice.get("blockers", []) if str(item).strip()) or "desktop interaction unavailable"
            return {
                "status": "blocked" if advice.get("blockers") else "error",
                "action": advice.get("action", args.get("action", "")),
                "route_mode": advice.get("route_mode", ""),
                "confidence": advice.get("confidence", 0.0),
                "risk_level": advice.get("risk_level", ""),
                "app_profile": advice.get("app_profile", {}),
                "target_window": advice.get("target_window", {}),
                "surface_snapshot": advice.get("surface_snapshot", {}),
                "safety_signals": advice.get("safety_signals", {}),
                "form_target_state": advice.get("form_target_state", {}),
                "surface_branch": advice.get("surface_branch", {}),
                "resume_action": advice.get("resume_action", ""),
                "resume_payload": advice.get("resume_payload", {}),
                "resume_contract": advice.get("resume_contract", {}),
                "blocking_surface": advice.get("blocking_surface", {}),
                "mission_record": advice.get("mission_record", {}),
                "resume_context": advice.get("resume_context", {}),
                "exploration_plan": advice.get("exploration_plan", {}),
                "exploration_selection": advice.get("exploration_selection", {}),
                "message": message,
                "advice": advice,
                "results": [],
                "verification": {
                    "enabled": bool(args.get("verify_after_action", True)),
                    "status": "skipped",
                    "verified": False,
                    "message": "execution skipped because the routed desktop advice was blocked",
                },
            }
        attempts: List[Dict[str, Any]] = []
        strategy_variants = advice.get("strategy_variants", []) if isinstance(advice.get("strategy_variants"), list) else []
        max_attempts = max(1, min(int(args.get("max_strategy_attempts", len(strategy_variants) or 1) or 1), 4))
        variants = [row for row in strategy_variants if isinstance(row, dict)][:max_attempts] or [
            {"strategy_id": "primary", "title": "Primary Route", "reason": "Use the advised routed plan.", "payload_overrides": {}}
        ]
        retry_on_verification_failure = bool(args.get("retry_on_verification_failure", True))
        final_attempt: Dict[str, Any] = {}

        for attempt_index, variant in enumerate(variants, start=1):
            strategy_overrides = variant.get("payload_overrides", {}) if isinstance(variant.get("payload_overrides", {}), dict) else {}
            attempt_args = dict(args)
            attempt_args.update(strategy_overrides)
            if strategy_overrides:
                attempt_args["_provided_fields"] = self._dedupe_strings(
                    list(attempt_args.get("_provided_fields", [])) + list(strategy_overrides.keys())
                )
            attempt_advice = advice if attempt_index == 1 and not strategy_overrides else self.advise(attempt_args)
            if attempt_advice.get("status") != "success":
                attempt_payload = {
                    "attempt": attempt_index,
                    "strategy_id": str(variant.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
                    "strategy_title": str(variant.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
                    "status": "blocked" if attempt_advice.get("blockers") else "error",
                    "message": "; ".join(
                        str(item) for item in attempt_advice.get("blockers", []) if str(item).strip()
                    ) or str(attempt_advice.get("message", "desktop interaction unavailable") or "desktop interaction unavailable"),
                    "payload": attempt_args,
                    "advice": attempt_advice,
                    "results": [],
                    "verification": {
                        "enabled": bool(attempt_args.get("verify_after_action", True)),
                        "status": "skipped",
                        "verified": False,
                        "message": "route planning failed before execution",
                        "checks": [],
                    },
                }
                attempts.append(attempt_payload)
                final_attempt = attempt_payload
                continue

            attempt_payload = self._execute_strategy(
                args=attempt_args,
                advice=attempt_advice,
                strategy=variant,
                attempt_index=attempt_index,
            )
            self._record_adaptive_strategy_outcome(
                args=attempt_args,
                advice=attempt_advice,
                strategy=variant,
                attempt_payload=attempt_payload,
            )
            attempts.append(attempt_payload)
            final_attempt = attempt_payload
            verification = attempt_payload.get("verification", {}) if isinstance(attempt_payload.get("verification", {}), dict) else {}
            verified = bool(verification.get("verified", False)) or not bool(verification.get("enabled", False))
            if attempt_payload.get("status") == "success" and verified:
                return self._build_execution_response(
                    base_advice=advice,
                    selected_attempt=attempt_payload,
                    attempts=attempts,
                    recovered=attempt_index > 1,
                )
            if attempt_payload.get("status") == "error":
                continue
            if not retry_on_verification_failure:
                break
            if self.settle_delay_s > 0:
                time.sleep(min(self.settle_delay_s, 0.5))

        selected_attempt = final_attempt if isinstance(final_attempt, dict) else {}
        verification = selected_attempt.get("verification", {}) if isinstance(selected_attempt.get("verification", {}), dict) else {}
        unverified = bool(verification.get("enabled", False)) and not bool(verification.get("verified", False))
        selected_status = str(selected_attempt.get("status", "") or "").strip().lower()
        if selected_status in {"partial", "blocked"}:
            status = selected_status
        else:
            status = "partial" if attempts and any(str(item.get("status", "") or "").strip().lower() == "success" for item in attempts) else "error"
            if unverified and status == "error":
                status = "partial"
        message = str(selected_attempt.get("message", "") or "").strip()
        if not message:
            if unverified:
                message = str(verification.get("message", "desktop action could not be verified after execution") or "desktop action could not be verified after execution")
            else:
                message = "desktop interaction did not complete successfully"
        return self._build_execution_response(
            base_advice=advice,
            selected_attempt=selected_attempt,
            attempts=attempts,
            recovered=False,
            status_override=status,
            message_override=message,
        )

    def _capabilities(self) -> Dict[str, Any]:
        accessibility_status = self._call("accessibility_status", {})
        vision_status = self._call("vision_status", {})
        return {
            "accessibility": {
                "available": str(accessibility_status.get("status", "")).strip().lower() == "success"
                or bool(accessibility_status.get("capabilities", {}).get("invoke_element")),
                "provider": str(accessibility_status.get("provider", "") or ""),
                "capabilities": accessibility_status.get("capabilities", {}) if isinstance(accessibility_status.get("capabilities", {}), dict) else {},
            },
            "vision": {
                "available": str(vision_status.get("status", "")).strip().lower() == "success"
                or bool(vision_status.get("capabilities", {}).get("ocr_targets")),
                "capabilities": vision_status.get("capabilities", {}) if isinstance(vision_status.get("capabilities", {}), dict) else {},
            },
        }

    def _list_windows(self) -> List[Dict[str, Any]]:
        payload = self._call("list_windows", {"limit": 80})
        rows = payload.get("windows", []) if isinstance(payload, dict) else []
        return [row for row in rows if isinstance(row, dict)]

    def _active_window(self) -> Dict[str, Any]:
        payload = self._call("active_window", {})
        if isinstance(payload, dict) and isinstance(payload.get("window"), dict):
            return payload.get("window", {})
        return payload if isinstance(payload, dict) else {}

    def _call(self, action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        handler = self._handlers.get(action)
        if handler is None:
            return {"status": "error", "message": f"missing handler for {action}"}
        try:
            result = handler(dict(payload))
            return result if isinstance(result, dict) else {"status": "error", "message": f"invalid result from {action}"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @staticmethod
    def _plan_step(*, action: str, args: Dict[str, Any], phase: str, optional: bool, reason: str) -> Dict[str, Any]:
        return {
            "action": action,
            "args": args,
            "phase": phase,
            "optional": optional,
            "reason": reason,
        }

    def _rank_window_candidates(
        self,
        *,
        windows: List[Dict[str, Any]],
        active_window: Dict[str, Any],
        app_name: str,
        window_title: str,
        app_profile: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        active_hwnd = self._to_int(active_window.get("hwnd"))
        profile_aliases = [str(alias).strip() for alias in app_profile.get("aliases", []) if str(alias).strip()]
        profile_exe_hints = [str(alias).strip().lower() for alias in app_profile.get("exe_hints", []) if str(alias).strip()]
        rows: List[tuple[float, Dict[str, Any]]] = []
        for window in windows:
            title = str(window.get("title", "") or "").strip()
            exe_path = str(window.get("exe", "") or "").strip()
            exe_name = exe_path.rsplit("\\", 1)[-1].rsplit("/", 1)[-1].strip().lower()
            score = 0.0
            reasons: List[str] = []
            if window_title:
                match_score = self._text_match_score(title, window_title)
                if match_score > 0:
                    score += 0.68 * match_score
                    reasons.append("window_title")
            if app_name:
                title_score = self._text_match_score(title, app_name)
                exe_score = self._text_match_score(exe_name, app_name)
                if title_score > 0:
                    score += 0.46 * title_score
                    reasons.append("app_title")
                if exe_score > 0:
                    score += 0.55 * exe_score
                    reasons.append("exe_name")
            for alias in profile_aliases:
                alias_score = self._text_match_score(title, alias)
                if alias_score > 0:
                    score += 0.38 * alias_score
                    reasons.append("profile_alias")
            for exe_hint in profile_exe_hints:
                exe_hint_score = self._text_match_score(exe_name, exe_hint)
                if exe_hint_score > 0:
                    score += 0.58 * exe_hint_score
                    reasons.append("profile_exe")
            if active_hwnd is not None and active_hwnd == self._to_int(window.get("hwnd")):
                score += 0.14
                reasons.append("active")
            if score <= 0 and not (app_name or window_title):
                if title:
                    score = 0.1
                    reasons.append("visible_window")
            if score <= 0:
                continue
            enriched = dict(window)
            enriched["score"] = round(min(score, 1.0), 6)
            enriched["exe_name"] = exe_name
            enriched["match_reasons"] = self._dedupe_strings(reasons)
            rows.append((score, enriched))
        rows.sort(key=lambda item: item[0], reverse=True)
        return [row for _, row in rows]

    def _route_mode(self, *, requested_action: str, args: Dict[str, Any], capabilities: Dict[str, Any], app_profile: Dict[str, Any]) -> str:
        if requested_action == "resume_mission":
            return "resume_desktop_mission"
        accessibility_ready = bool(capabilities["accessibility"].get("available"))
        vision_ready = bool(capabilities["vision"].get("available"))
        target_mode = str(args.get("target_mode", "auto") or "auto").strip().lower() or "auto"
        capability_preferences = [
            str(item).strip().lower()
            for item in app_profile.get("capability_preferences", [])
            if str(item).strip()
        ]
        if requested_action in {"click", "click_and_type"}:
            can_retry = bool(args.get("retry_on_verification_failure", True))
            if target_mode == "accessibility":
                return "accessibility_then_ocr" if vision_ready and can_retry else "accessibility_only"
            if target_mode == "ocr":
                return "ocr_then_accessibility" if accessibility_ready and can_retry else "ocr_only"
            if accessibility_ready and vision_ready:
                if capability_preferences[:1] == ["vision"]:
                    return "ocr_then_accessibility"
                return "accessibility_then_ocr"
            if accessibility_ready:
                return "accessibility_only"
            if vision_ready:
                return "ocr_only"
        if requested_action in WORKFLOW_ACTIONS:
            return str(self._workflow_definition(requested_action).get("route_mode", "workflow_desktop") or "workflow_desktop")
        if requested_action in {"type", "hotkey"}:
            return "focused_input"
        if requested_action == "launch":
            return "launch_and_focus"
        if requested_action == "observe":
            return "vision_observe"
        return "generic_desktop"

    def _confidence(
        self,
        *,
        requested_action: str,
        primary_candidate: Dict[str, Any],
        capabilities: Dict[str, Any],
        blockers: List[str],
        warnings: List[str],
        app_profile: Dict[str, Any],
    ) -> float:
        if blockers:
            return 0.0
        score = 0.42
        candidate_score = float(primary_candidate.get("score", 0.0) or 0.0)
        score += min(0.35, candidate_score * 0.35)
        match_score = float(app_profile.get("match_score", 0.0) or 0.0)
        score += min(0.12, match_score * 0.12)
        if requested_action in {"click", "click_and_type"}:
            if bool(capabilities["accessibility"].get("available")):
                score += 0.12
            if bool(capabilities["vision"].get("available")):
                score += 0.09
        elif requested_action in {"type", "hotkey"}:
            score += 0.1
        elif requested_action in WORKFLOW_ACTIONS:
            score += 0.14
        elif requested_action == "observe":
            score += 0.18 if bool(capabilities["vision"].get("available")) else 0.0
        if warnings:
            score -= min(0.18, 0.06 * len(warnings))
        return round(max(0.0, min(score, 0.99)), 4)

    def _normalize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raw = payload if isinstance(payload, dict) else {}
        if isinstance(raw.get("_provided_fields"), list):
            provided_fields = [str(item).strip() for item in raw.get("_provided_fields", []) if str(item).strip()]
        else:
            provided_fields = [
                field_name
                for field_name, aliases in {
                    "app_name": ("app_name", "app"),
                    "window_title": ("window_title", "title"),
                    "query": ("query", "target"),
                    "text": ("text",),
                    "amount": ("amount",),
                    "keys": ("keys", "key"),
                    "press_enter": ("press_enter", "submit"),
                    "ensure_app_launch": ("ensure_app_launch", "launch_if_missing"),
                    "focus_first": ("focus_first",),
                    "target_mode": ("target_mode",),
                    "verify_mode": ("verify_mode",),
                    "verify_after_action": ("verify_after_action",),
                    "verify_text": ("verify_text",),
                    "retry_on_verification_failure": ("retry_on_verification_failure",),
                    "max_strategy_attempts": ("max_strategy_attempts",),
                    "max_exploration_steps": ("max_exploration_steps",),
                    "max_wizard_pages": ("max_wizard_pages",),
                    "allow_warning_pages": ("allow_warning_pages",),
                    "max_form_pages": ("max_form_pages",),
                    "allow_destructive_forms": ("allow_destructive_forms",),
                    "exploration_limit": ("exploration_limit",),
                    "candidate_id": ("candidate_id",),
                    "branch_action": ("branch_action",),
                    "attempted_targets": ("attempted_targets",),
                    "surface_signature_history": ("surface_signature_history",),
                    "form_target_plan": ("form_target_plan",),
                    "expected_form_target_count": ("expected_form_target_count",),
                    "control_type": ("control_type",),
                    "element_id": ("element_id",),
                    "include_targets": ("include_targets",),
                    "mission_id": ("mission_id",),
                    "mission_kind": ("mission_kind",),
                    "resume_contract": ("resume_contract",),
                    "blocking_surface": ("blocking_surface",),
                    "resume_force": ("resume_force",),
                }.items()
                if any(alias in raw and raw.get(alias) is not None for alias in aliases)
            ]
        normalized_action = str(raw.get("action", "") or "").strip().lower()
        resume_contract = self._normalize_resume_contract_payload(raw.get("resume_contract"))
        blocking_surface = self._normalize_blocking_surface_payload(raw.get("blocking_surface"))
        text = str(raw.get("text", "") or "").strip()
        query = str(raw.get("query", raw.get("target", "")) or "").strip()
        hotkey_keys = raw.get("keys")
        if isinstance(hotkey_keys, str):
            keys = [part.strip().lower() for part in re.split(r"[+,]", hotkey_keys) if part.strip()]
        elif isinstance(hotkey_keys, list):
            keys = [str(part).strip().lower() for part in hotkey_keys if str(part).strip()]
        else:
            key = str(raw.get("key", "") or "").strip().lower()
            keys = [key] if key else []

        if not normalized_action and resume_contract:
            normalized_action = "resume_mission"
        if normalized_action not in {"launch", "focus", "click", "type", "click_and_type", "hotkey", "observe", "resume_mission", EXPLORATION_ADVANCE_ACTION, EXPLORATION_FLOW_ACTION, *WORKFLOW_ACTIONS}:
            if keys:
                normalized_action = "hotkey"
            elif text and query:
                normalized_action = "click_and_type"
            elif text:
                normalized_action = "type"
            elif query:
                normalized_action = "click"
            elif str(raw.get("app_name", "") or raw.get("app", "")).strip():
                normalized_action = "launch"
            else:
                normalized_action = "observe"

        return {
            "action": normalized_action,
            "app_name": str(raw.get("app_name", raw.get("app", "")) or "").strip(),
            "window_title": str(raw.get("window_title", raw.get("title", "")) or "").strip(),
            "query": query,
            "text": text,
            "amount": max(1, min(int(raw.get("amount", 1) or 1), 20)),
            "keys": keys,
            "press_enter": bool(raw.get("press_enter", False)),
            "submit": bool(raw.get("submit", False)),
            "ensure_app_launch": bool(raw.get("ensure_app_launch", False) or raw.get("launch_if_missing", False)),
            "focus_first": bool(raw.get("focus_first", True)),
            "target_mode": str(raw.get("target_mode", "auto") or "auto").strip().lower() or "auto",
            "verify_mode": str(raw.get("verify_mode", "state_or_visibility") or "state_or_visibility").strip().lower() or "state_or_visibility",
            "verify_after_action": bool(raw.get("verify_after_action", True)),
            "verify_text": str(raw.get("verify_text", "") or "").strip(),
            "retry_on_verification_failure": bool(raw.get("retry_on_verification_failure", True)),
            "max_strategy_attempts": max(1, min(int(raw.get("max_strategy_attempts", 2) or 2), 4)),
            "max_exploration_steps": max(1, min(int(raw.get("max_exploration_steps", 3) or 3), 8)),
            "max_wizard_pages": max(1, min(int(raw.get("max_wizard_pages", 6) or 6), 12)),
            "allow_warning_pages": bool(raw.get("allow_warning_pages", False)),
            "max_form_pages": max(1, min(int(raw.get("max_form_pages", 5) or 5), 10)),
            "allow_destructive_forms": bool(raw.get("allow_destructive_forms", False)),
            "form_target_plan": [dict(row) for row in raw.get("form_target_plan", []) if isinstance(row, dict)] if isinstance(raw.get("form_target_plan", []), list) else [],
            "expected_form_target_count": max(0, int(raw.get("expected_form_target_count", 0) or 0)),
            "candidate_id": str(raw.get("candidate_id", "") or "").strip(),
            "branch_action": str(raw.get("branch_action", "") or "").strip(),
            "attempted_targets": [dict(row) for row in raw.get("attempted_targets", []) if isinstance(row, dict)]
            if isinstance(raw.get("attempted_targets", []), list)
            else [],
            "surface_signature_history": [
                str(item).strip()
                for item in raw.get("surface_signature_history", [])
                if str(item).strip()
            ][:24]
            if isinstance(raw.get("surface_signature_history", []), list)
            else [],
            "control_type": str(raw.get("control_type", "") or "").strip(),
            "element_id": str(raw.get("element_id", "") or "").strip(),
            "include_targets": bool(raw.get("include_targets", False)),
            "mission_id": str(raw.get("mission_id", "") or "").strip(),
            "mission_kind": str(raw.get("mission_kind", "") or "").strip().lower(),
            "resume_contract": resume_contract,
            "blocking_surface": blocking_surface,
            "resume_force": bool(raw.get("resume_force", False)),
            "exploration_limit": max(1, min(int(raw.get("exploration_limit", 6) or 6), 12)),
            "_provided_fields": provided_fields,
        }

    def _execute_strategy(
        self,
        *,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        strategy: Dict[str, Any],
        attempt_index: int,
    ) -> Dict[str, Any]:
        action = str(args.get("action", "observe") or "observe").strip().lower()
        if action == "resume_mission":
            return self._execute_resume_mission_strategy(
                args=args,
                advice=advice,
                strategy=strategy,
                attempt_index=attempt_index,
            )
        if action == EXPLORATION_ADVANCE_ACTION:
            return self._execute_surface_exploration_strategy(
                args=args,
                advice=advice,
                strategy=strategy,
                attempt_index=attempt_index,
            )
        if action == EXPLORATION_FLOW_ACTION:
            return self._execute_surface_exploration_flow_strategy(
                args=args,
                advice=advice,
                strategy=strategy,
                attempt_index=attempt_index,
            )
        if action == "complete_wizard_flow":
            return self._execute_wizard_flow_strategy(
                args=args,
                advice=advice,
                strategy=strategy,
                attempt_index=attempt_index,
            )
        if action == "complete_form_flow":
            return self._execute_form_flow_strategy(
                args=args,
                advice=advice,
                strategy=strategy,
                attempt_index=attempt_index,
            )
        pre_context = self._capture_verification_context(args=args, advice=advice)
        execution_payload = self._run_execution_plan(plan=advice.get("execution_plan", []))
        results = execution_payload.get("results", []) if isinstance(execution_payload.get("results", []), list) else []
        message = str(execution_payload.get("message", "") or "")
        status = str(execution_payload.get("status", "success") or "success")
        final_action = str(execution_payload.get("final_action", "") or advice.get("action", ""))
        post_context = self._capture_verification_context(args=args, advice=advice) if status == "success" else {}
        verification = self._verify_execution(
            args=args,
            advice=advice,
            results=results,
            pre_context=pre_context,
            post_context=post_context,
            step_status=status,
        )
        if status == "success" and bool(verification.get("enabled", False)) and not bool(verification.get("verified", False)):
            message = str(verification.get("message", "desktop action could not be verified after execution") or "desktop action could not be verified after execution")
        elif status == "success" and not message:
            message = str(verification.get("message", "desktop action executed") or "desktop action executed")
        return {
            "attempt": attempt_index,
            "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
            "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
            "strategy_reason": str(strategy.get("reason", "") or "").strip(),
            "payload": self._sanitize_payload_for_response(args),
            "status": status,
            "message": message,
            "final_action": final_action,
            "results": results,
            "advice": advice,
            "verification": verification,
        }

    def _run_execution_plan(
        self,
        *,
        plan: Any,
        result_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        results: List[Dict[str, Any]] = []
        message = ""
        status = "success"
        final_action = ""
        metadata = dict(result_metadata) if isinstance(result_metadata, dict) else {}
        for step in plan if isinstance(plan, list) else []:
            if not isinstance(step, dict):
                continue
            action = str(step.get("action", "") or "").strip()
            action_args = step.get("args", {}) if isinstance(step.get("args", {}), dict) else {}
            handler = self._handlers.get(action)
            if handler is None:
                message = f"missing handler for {action}"
                status = "error"
                final_action = action
                break
            result = handler(dict(action_args))
            result_row = {
                "action": action,
                "phase": str(step.get("phase", "") or ""),
                "result": result,
            }
            if metadata:
                result_row.update(metadata)
            results.append(result_row)
            final_action = action
            if action.lower() == "open_app" and result.get("status") == "success" and self.settle_delay_s > 0:
                time.sleep(self.settle_delay_s)
            if result.get("status") != "success" and not bool(step.get("optional", False)):
                message = str(result.get("message", f"{action} failed") or f"{action} failed")
                status = "error"
                break
        return {
            "status": status,
            "message": message,
            "final_action": final_action,
            "results": results,
        }

    def _execute_wizard_flow_strategy(
        self,
        *,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        strategy: Dict[str, Any],
        attempt_index: int,
    ) -> Dict[str, Any]:
        results: List[Dict[str, Any]] = []
        page_history: List[Dict[str, Any]] = []
        mission_warnings: List[str] = []
        status = "success"
        message = ""
        stop_reason_code = ""
        stop_reason = ""
        completed = False
        pages_completed = 0
        max_pages = max(1, min(int(args.get("max_wizard_pages", 6) or 6), 12))
        allow_warning_pages = bool(args.get("allow_warning_pages", False))
        pre_context = self._capture_verification_context(args=args, advice=advice)
        advice_target_window = advice.get("target_window", {}) if isinstance(advice.get("target_window", {}), dict) else {}
        wizard_anchor_title = str(args.get("window_title", "") or advice_target_window.get("title", "") or "").strip()
        wizard_anchor_app_name = str(args.get("app_name", "") or "").strip()
        wizard_window_hint = str(args.get("window_title", "") or "").strip()
        wizard_window_locked = False

        def _remember_wizard_window(summary: Dict[str, Any]) -> None:
            nonlocal wizard_window_hint, wizard_window_locked
            if not isinstance(summary, dict):
                return
            hinted_title = str(summary.get("window_title", "") or "").strip()
            if hinted_title:
                wizard_window_hint = hinted_title
            if bool(summary.get("window_adopted", False)):
                wizard_window_locked = True

        bootstrap_plan = [dict(step) for step in advice.get("execution_plan", []) if isinstance(step, dict)]
        if bootstrap_plan:
            bootstrap_payload = self._run_execution_plan(
                plan=bootstrap_plan,
                result_metadata={"wizard_stage": "bootstrap"},
            )
            results.extend(bootstrap_payload.get("results", []) if isinstance(bootstrap_payload.get("results", []), list) else [])
            if str(bootstrap_payload.get("status", "success") or "success") != "success":
                message = str(bootstrap_payload.get("message", "wizard bootstrap failed") or "wizard bootstrap failed")
                status = "error"
                verification = {
                    "enabled": True,
                    "status": "failed",
                    "verified": False,
                    "message": message,
                    "checks": [],
                }
                return {
                    "attempt": attempt_index,
                    "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
                    "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
                    "strategy_reason": str(strategy.get("reason", "") or "").strip(),
                    "payload": self._sanitize_payload_for_response(args),
                    "status": status,
                    "message": message,
                    "final_action": str(bootstrap_payload.get("final_action", "") or advice.get("action", "")),
                    "results": results,
                    "advice": advice,
                    "verification": verification,
                    "wizard_mission": {
                        "enabled": True,
                        "completed": False,
                        "pages_completed": 0,
                        "page_count": 0,
                        "max_pages": max_pages,
                        "allow_warning_pages": allow_warning_pages,
                        "stop_reason_code": "wizard_bootstrap_failed",
                        "stop_reason": message,
                        "page_history": [],
                    },
                }

        last_snapshot = self._wizard_flow_snapshot(args=args, advice=advice)
        for page_index in range(1, max_pages + 1):
            page_args = dict(args)
            if wizard_window_hint:
                page_args["window_title"] = wizard_window_hint
            if wizard_window_locked and wizard_window_hint:
                page_args["app_name"] = ""
                page_args["focus_first"] = True
            page_args["action"] = "complete_wizard_page"
            page_args["_provided_fields"] = self._dedupe_strings(
                list(page_args.get("_provided_fields", [])) + ["action"]
            )
            page_advice = self.advise(page_args)
            page_snapshot = self._wizard_flow_snapshot(args=page_args, advice=page_advice)
            page_advice["surface_snapshot"] = page_snapshot
            page_state = page_snapshot.get("wizard_page_state", {}) if isinstance(page_snapshot.get("wizard_page_state", {}), dict) else {}
            safety_signals = page_snapshot.get("safety_signals", {}) if isinstance(page_snapshot.get("safety_signals", {}), dict) else {}
            surface_flags = page_snapshot.get("surface_flags", {}) if isinstance(page_snapshot.get("surface_flags", {}), dict) else {}
            before_summary = self._wizard_flow_summary(snapshot=page_snapshot)
            _remember_wizard_window(before_summary)
            page_record: Dict[str, Any] = {
                "page_index": page_index,
                "before": before_summary,
                "warnings": [str(item).strip() for item in page_advice.get("warnings", []) if str(item).strip()],
                "recommended_actions": [str(item).strip() for item in page_snapshot.get("recommended_actions", []) if str(item).strip()] if isinstance(page_snapshot.get("recommended_actions", []), list) else [],
            }

            if not bool(surface_flags.get("wizard_surface_visible", False)):
                dialog_followup = self._resolve_wizard_dialog_interstitial(
                    args=page_args,
                    snapshot=page_snapshot,
                    page_index=page_index,
                )
                if bool(dialog_followup.get("handled", False)):
                    dialog_execution = dialog_followup.get("execution", {}) if isinstance(dialog_followup.get("execution", {}), dict) else {}
                    dialog_after_snapshot = dialog_followup.get("after_snapshot", {}) if isinstance(dialog_followup.get("after_snapshot", {}), dict) else page_snapshot
                    dialog_after_summary = dialog_followup.get("after_summary", {}) if isinstance(dialog_followup.get("after_summary", {}), dict) else before_summary
                    _remember_wizard_window(dialog_after_summary)
                    results.extend(dialog_execution.get("results", []) if isinstance(dialog_execution.get("results", []), list) else [])
                    page_record["after"] = dialog_after_summary
                    page_record["status"] = str(dialog_followup.get("status", "dialog_resolved") or "dialog_resolved")
                    page_record["message"] = str(dialog_followup.get("message", "") or "")
                    page_record["progressed"] = bool(dialog_followup.get("progressed", False))
                    page_record["dialog_followup"] = {
                        "action": str(dialog_followup.get("action", "") or "").strip(),
                        "button_label": str(dialog_followup.get("button_label", "") or "").strip(),
                        "button_role": str(dialog_followup.get("button_role", "") or "").strip(),
                        "route_mode": str(dialog_followup.get("route_mode", "") or "").strip(),
                        "dialog_kind": str(dialog_followup.get("dialog_kind", "") or "").strip(),
                        "approval_kind": str(dialog_followup.get("approval_kind", "") or "").strip(),
                        "secure_desktop_likely": bool(dialog_followup.get("secure_desktop_likely", False)),
                    }
                    page_record["executed_actions"] = [
                        str(row.get("action", "") or "").strip()
                        for row in dialog_execution.get("results", [])
                        if isinstance(row, dict) and str(row.get("action", "") or "").strip()
                    ]
                    page_history.append(page_record)
                    mission_warnings.extend([str(item).strip() for item in dialog_followup.get("warnings", []) if str(item).strip()])
                    last_snapshot = dialog_after_snapshot
                    if not bool(dialog_after_summary.get("wizard_visible", False)) and not bool(dialog_after_summary.get("dialog_visible", False)):
                        completed = True
                        message = "wizard flow completed and the setup surface closed"
                        break
                    continue
                if bool(dialog_followup.get("blocked", False)):
                    stop_reason_code = str(dialog_followup.get("stop_reason_code", "") or "wizard_dialog_review_required")
                    stop_reason = str(dialog_followup.get("stop_reason", "") or "wizard mission paused on an interstitial dialog that requires review")
                    page_record["status"] = "blocked"
                    page_record["stop_reason_code"] = stop_reason_code
                    page_record["stop_reason"] = stop_reason
                    page_record["blocking_surface"] = self._blocking_surface_state(
                        snapshot=page_snapshot,
                        stop_reason_code=stop_reason_code,
                        mission_kind="wizard",
                    )
                    page_record["dialog_followup"] = {
                        "blocked": True,
                        "button_label": str(dialog_followup.get("button_label", "") or "").strip(),
                        "button_role": str(dialog_followup.get("button_role", "") or "").strip(),
                        "dialog_kind": str(dialog_followup.get("dialog_kind", "") or "").strip(),
                        "approval_kind": str(dialog_followup.get("approval_kind", "") or "").strip(),
                        "secure_desktop_likely": bool(dialog_followup.get("secure_desktop_likely", False)),
                    }
                    if stop_reason:
                        mission_warnings.append(stop_reason)
                    page_history.append(page_record)
                    last_snapshot = page_snapshot
                    break
                if page_index == 1 and not results:
                    stop_reason_code = "wizard_not_visible"
                    stop_reason = "No active setup wizard surface could be detected after focusing the requested app."
                    page_record["status"] = "blocked"
                    page_record["stop_reason_code"] = stop_reason_code
                    page_record["stop_reason"] = stop_reason
                    page_history.append(page_record)
                    last_snapshot = page_snapshot
                else:
                    completed = True
                    message = "wizard flow completed and the setup surface closed"
                break

            gate = self._wizard_flow_gate(
                page_state=page_state,
                safety_signals=safety_signals,
                allow_warning_pages=allow_warning_pages,
            )
            if not bool(gate.get("allowed", False)):
                stop_reason_code = str(gate.get("code", "") or "wizard_manual_review_required")
                stop_reason = str(gate.get("message", "") or "wizard page requires manual review before automation can continue")
                page_record["status"] = "blocked"
                page_record["stop_reason_code"] = stop_reason_code
                page_record["stop_reason"] = stop_reason
                page_record["blocking_surface"] = self._blocking_surface_state(
                    snapshot=page_snapshot,
                    stop_reason_code=stop_reason_code,
                    mission_kind="wizard",
                )
                if stop_reason:
                    mission_warnings.append(stop_reason)
                page_history.append(page_record)
                break

            if page_advice.get("status") != "success":
                stop_reason_code = "wizard_page_route_unavailable"
                stop_reason = "; ".join(
                    str(item) for item in page_advice.get("blockers", []) if str(item).strip()
                ) or str(page_advice.get("message", "wizard page route unavailable") or "wizard page route unavailable")
                page_record["status"] = "blocked"
                page_record["stop_reason_code"] = stop_reason_code
                page_record["stop_reason"] = stop_reason
                mission_warnings.extend([str(item).strip() for item in page_advice.get("warnings", []) if str(item).strip()])
                page_history.append(page_record)
                break

            page_execution = self._run_execution_plan(
                plan=page_advice.get("execution_plan", []),
                result_metadata={
                    "wizard_stage": "page",
                    "wizard_page_index": page_index,
                    "wizard_page_kind": str(page_state.get("page_kind", "") or ""),
                },
            )
            results.extend(page_execution.get("results", []) if isinstance(page_execution.get("results", []), list) else [])
            after_snapshot = self._wizard_flow_snapshot(args=page_args, advice=page_advice)
            after_summary = self._wizard_flow_summary(snapshot=after_snapshot)
            _remember_wizard_window(after_summary)
            progressed = self._wizard_flow_progressed(before_snapshot=page_snapshot, after_snapshot=after_snapshot)
            page_record["after"] = after_summary
            page_record["status"] = str(page_execution.get("status", "success") or "success")
            page_record["message"] = str(page_execution.get("message", "") or "")
            page_record["progressed"] = progressed
            page_record["executed_actions"] = [
                str(row.get("action", "") or "").strip()
                for row in page_execution.get("results", [])
                if isinstance(row, dict) and str(row.get("action", "") or "").strip()
            ]
            page_history.append(page_record)
            mission_warnings.extend([str(item).strip() for item in page_advice.get("warnings", []) if str(item).strip()])

            if str(page_execution.get("status", "success") or "success") != "success":
                status = "error" if not results else "success"
                stop_reason_code = "wizard_page_execution_failed"
                stop_reason = str(page_execution.get("message", "wizard page execution failed") or "wizard page execution failed")
                break

            interstitial_dialog_only = bool(after_summary.get("dialog_visible", False)) and not bool(after_summary.get("wizard_visible", False))
            if (
                progressed
                and not interstitial_dialog_only
            ) or (
                not bool(after_summary.get("wizard_visible", False))
                and not bool(after_summary.get("dialog_visible", False))
            ):
                pages_completed += 1
            if not bool(after_summary.get("wizard_visible", False)) and not bool(after_summary.get("dialog_visible", False)):
                completed = True
                message = "wizard flow completed and the setup surface closed"
                last_snapshot = after_snapshot
                break
            if not progressed and not bool(after_summary.get("dialog_visible", False)):
                stop_reason_code = "wizard_page_stalled"
                stop_reason = "Wizard page execution completed, but the setup surface did not advance to a new state."
                break

            last_snapshot = after_snapshot

        if not completed and not stop_reason_code and not message and pages_completed >= max_pages:
            stop_reason_code = "wizard_page_limit_reached"
            stop_reason = f"Wizard mission reached the configured page limit of {max_pages} without reaching a completed setup state."
        if completed and not message:
            message = "wizard flow completed"
        elif not message:
            message = stop_reason or "wizard flow stopped before completion"

        post_context = self._capture_verification_context(args=args, advice=advice) if status == "success" else {}
        final_summary = self._wizard_flow_summary(snapshot=last_snapshot)
        verification = self._verify_wizard_flow_execution(
            args=args,
            pre_context=pre_context,
            post_context=post_context,
            completed=completed,
            pages_completed=pages_completed,
            max_pages=max_pages,
            stop_reason_code=stop_reason_code,
            stop_reason=stop_reason,
            final_summary=final_summary,
            warnings=mission_warnings,
        )
        if status == "success" and bool(verification.get("enabled", False)) and not bool(verification.get("verified", False)):
            message = str(verification.get("message", message) or message)
        blocking_surface = self._blocking_surface_state(
            snapshot=last_snapshot,
            stop_reason_code=stop_reason_code,
            mission_kind="wizard",
        )
        resume_contract = self._mission_resume_contract(
            mission_kind="wizard",
            args=args,
            stop_reason_code=stop_reason_code,
            blocking_surface=blocking_surface,
            anchor_window_title=wizard_anchor_title,
            anchor_app_name=wizard_anchor_app_name,
        )
        wizard_mission_payload = {
            "enabled": True,
            "completed": completed,
            "pages_completed": pages_completed,
            "page_count": len(page_history),
            "max_pages": max_pages,
            "allow_warning_pages": allow_warning_pages,
            "stop_reason_code": stop_reason_code,
            "stop_reason": stop_reason,
            "blocking_surface": blocking_surface,
            "resume_contract": resume_contract,
            "page_history": page_history,
            "final_page": final_summary,
            "risk_level": advice.get("risk_level", ""),
            "status": status,
            "message": message,
        }
        mission_record = {}
        if blocking_surface and resume_contract:
            mission_record = self._persist_paused_mission(
                mission_kind="wizard",
                args=args,
                blocking_surface=blocking_surface,
                resume_contract=resume_contract,
                mission_payload=wizard_mission_payload,
                warnings=mission_warnings,
                message=message,
            )
            if mission_record:
                wizard_mission_payload["mission_record"] = mission_record

        return {
            "attempt": attempt_index,
            "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
            "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
            "strategy_reason": str(strategy.get("reason", "") or "").strip(),
            "payload": self._sanitize_payload_for_response(args),
            "status": status,
            "message": message,
            "final_action": "complete_wizard_flow" if completed else str(results[-1]["action"] if results else advice.get("action", "")),
            "results": results,
            "advice": advice,
            "verification": verification,
            "wizard_mission": wizard_mission_payload,
            "mission_record": mission_record,
        }

    def _blocking_surface_state(
        self,
        *,
        snapshot: Dict[str, Any],
        stop_reason_code: str,
        mission_kind: str,
    ) -> Dict[str, Any]:
        if not isinstance(snapshot, dict) or not str(stop_reason_code or "").strip():
            return {}
        stop_code = str(stop_reason_code or "").strip()
        safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        target_group_state = snapshot.get("target_group_state", {}) if isinstance(snapshot.get("target_group_state", {}), dict) else {}
        wizard_page_state = snapshot.get("wizard_page_state", {}) if isinstance(snapshot.get("wizard_page_state", {}), dict) else {}
        form_page_state = snapshot.get("form_page_state", {}) if isinstance(snapshot.get("form_page_state", {}), dict) else {}
        page_state = wizard_page_state if mission_kind == "wizard" else form_page_state
        summary = self._wizard_flow_summary(snapshot=snapshot) if mission_kind == "wizard" else self._form_flow_summary(snapshot=snapshot)
        recommended_actions = [
            str(item).strip()
            for item in snapshot.get("recommended_actions", [])
            if str(item).strip()
        ] if isinstance(snapshot.get("recommended_actions", []), list) else []
        credential_fields = [
            dict(row)
            for row in dialog_state.get("credential_fields", [])
            if isinstance(row, dict)
        ][:6]
        pending_requirements = [
            dict(row)
            for row in page_state.get("pending_requirements", [])
            if isinstance(row, dict)
        ][:8]
        manual_required_controls = [
            dict(row)
            for row in page_state.get("manual_required_controls", [])
            if isinstance(row, dict)
        ][:8]
        resume_preconditions_map = {
            "credential_input_required": ["provide_credentials"],
            "elevation_credentials_required": ["provide_admin_credentials", "review_elevation_request"],
            "elevation_consent_required": ["approve_elevation_request"],
            "authentication_review_required": ["review_authentication_request"],
            "permission_review_required": ["review_permission_request"],
            "warning_confirmation_requires_review": ["review_warning_surface"],
            "destructive_form_review_required": ["review_destructive_change"],
            "form_dialog_review_required": ["review_dialog_surface"],
            "wizard_dialog_review_required": ["review_dialog_surface"],
            "manual_input_required": ["provide_required_values"],
            "unsupported_form_requirements": ["review_unresolved_form_requirements"],
            "unsupported_wizard_requirements": ["review_unresolved_wizard_requirements"],
        }
        operator_steps_map = {
            "credential_input_required": [
                "Complete the credential prompt manually.",
                "Wait for the protected dialog to close or move behind the target app.",
                "Resume the paused desktop mission with the provided continuation payload.",
            ],
            "elevation_credentials_required": [
                "Review the administrator request carefully.",
                "Enter administrator credentials on the secure prompt if you want to proceed.",
                "Resume the paused desktop mission after the elevation prompt closes.",
            ],
            "elevation_consent_required": [
                "Review the administrator prompt carefully.",
                "Approve or dismiss the elevation request manually.",
                "Resume the paused desktop mission after the UAC surface closes.",
            ],
            "authentication_review_required": [
                "Review the authentication request carefully.",
                "Confirm or dismiss the identity-sensitive prompt manually.",
                "Resume the paused desktop mission once the review surface is cleared.",
            ],
            "permission_review_required": [
                "Review the permission or consent request carefully.",
                "Approve or dismiss the app access request manually.",
                "Resume the paused desktop mission after the review dialog closes.",
            ],
            "manual_input_required": [
                "Fill in the required values on the current page.",
                "Confirm the page state is ready to continue.",
                "Resume the paused desktop mission with the continuation payload.",
            ],
            "warning_confirmation_requires_review": [
                "Review the warning surface carefully.",
                "Confirm or cancel the risky step manually if appropriate.",
                "Resume the paused desktop mission after the warning surface clears.",
            ],
            "destructive_form_review_required": [
                "Review the destructive change carefully.",
                "Manually confirm or cancel the destructive action.",
                "Resume the paused desktop mission only if the change should continue.",
            ],
        }
        resume_action = "complete_wizard_flow" if mission_kind == "wizard" else "complete_form_flow"
        signature_parts = [
            mission_kind,
            stop_code,
            str(summary.get("window_title", "") or "").strip().lower(),
            str(summary.get("window_hwnd", 0) or 0),
            str(summary.get("screen_hash", "") or "").strip().lower(),
            str(dialog_state.get("dialog_kind", "") or "").strip().lower(),
            str(dialog_state.get("approval_kind", "") or "").strip().lower(),
            str(page_state.get("page_kind", "") or "").strip().lower(),
            str(page_state.get("autonomous_blocker", "") or "").strip().lower(),
        ]
        surface_signature = hashlib.sha1("|".join(signature_parts).encode("utf-8")).hexdigest()[:16]
        return {
            "mission_kind": mission_kind,
            "stop_reason_code": stop_code,
            "resume_action": resume_action,
            "resume_preconditions": resume_preconditions_map.get(stop_code, ["review_blocking_surface"]),
            "window_title": str(summary.get("window_title", "") or "").strip(),
            "window_hwnd": int(summary.get("window_hwnd", 0) or 0),
            "screen_hash": str(summary.get("screen_hash", "") or "").strip(),
            "page_kind": str(summary.get("page_kind", "") or page_state.get("page_kind", "") or "").strip(),
            "dialog_kind": str(dialog_state.get("dialog_kind", "") or "").strip(),
            "approval_kind": str(dialog_state.get("approval_kind", "") or "").strip(),
            "dialog_visible": bool(summary.get("dialog_visible", False) or dialog_state.get("visible", False)),
            "dialog_review_required": bool(dialog_state.get("review_required", False)),
            "secure_desktop_likely": bool(dialog_state.get("secure_desktop_likely", False)),
            "manual_input_required": bool(dialog_state.get("manual_input_required", False)),
            "credential_field_count": int(dialog_state.get("credential_field_count", 0) or 0),
            "preferred_confirmation_button": str(safety_signals.get("preferred_confirmation_button", "") or "").strip(),
            "preferred_dismiss_button": str(safety_signals.get("preferred_dismiss_button", "") or "").strip(),
            "safe_dialog_buttons": [str(item).strip() for item in safety_signals.get("safe_dialog_buttons", []) if str(item).strip()][:6],
            "confirmation_dialog_buttons": [str(item).strip() for item in safety_signals.get("confirmation_dialog_buttons", []) if str(item).strip()][:6],
            "destructive_dialog_buttons": [str(item).strip() for item in safety_signals.get("destructive_dialog_buttons", []) if str(item).strip()][:6],
            "credential_fields": credential_fields,
            "pending_requirements": pending_requirements,
            "manual_required_controls": manual_required_controls,
            "blocking_controls": credential_fields or manual_required_controls or pending_requirements,
            "autonomous_blocker": str(page_state.get("autonomous_blocker", "") or "").strip(),
            "recommended_actions": recommended_actions[:8],
            "operator_steps": operator_steps_map.get(
                stop_code,
                [
                    "Review the blocking surface carefully.",
                    "Resolve the surface manually if you want automation to continue.",
                    "Resume the paused desktop mission with the continuation payload.",
                ],
            ),
            "surface_signature": surface_signature,
            "target_group_state": dict(target_group_state) if target_group_state else {},
            "notes": [str(item).strip() for item in page_state.get("notes", []) if str(item).strip()] if isinstance(page_state.get("notes", []), list) else [],
        }

    def _mission_resume_contract(
        self,
        *,
        mission_kind: str,
        args: Dict[str, Any],
        stop_reason_code: str,
        blocking_surface: Dict[str, Any],
        anchor_window_title: str,
        anchor_app_name: str,
        remaining_form_targets: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if not str(stop_reason_code or "").strip() or not isinstance(blocking_surface, dict) or not blocking_surface:
            return {}
        resume_action = "complete_wizard_flow" if mission_kind == "wizard" else "complete_form_flow"
        clean_anchor_title = str(anchor_window_title or "").strip()
        clean_anchor_app = str(anchor_app_name or "").strip()
        blocking_window_title = str(blocking_surface.get("window_title", "") or "").strip()
        use_anchor_window = bool(
            not clean_anchor_app
            and clean_anchor_title
            and self._normalize_probe_text(clean_anchor_title) != self._normalize_probe_text(blocking_window_title)
        )
        resume_payload: Dict[str, Any] = {
            "action": resume_action,
            "app_name": clean_anchor_app,
            "window_title": clean_anchor_title if use_anchor_window else "",
            "focus_first": True,
        }
        if mission_kind == "wizard":
            resume_payload["max_wizard_pages"] = max(1, min(int(args.get("max_wizard_pages", 6) or 6), 12))
            resume_payload["allow_warning_pages"] = bool(args.get("allow_warning_pages", False))
        else:
            normalized_targets = [
                dict(row)
                for row in (remaining_form_targets or [])
                if isinstance(row, dict)
            ]
            resume_payload["max_form_pages"] = max(1, min(int(args.get("max_form_pages", 5) or 5), 10))
            resume_payload["allow_destructive_forms"] = bool(args.get("allow_destructive_forms", False))
            resume_payload["expected_form_target_count"] = len(normalized_targets)
            if normalized_targets:
                resume_payload["form_target_plan"] = normalized_targets
        sanitized_resume_payload = self._sanitize_payload_for_response(resume_payload)
        resume_strategy = (
            "reacquire_anchor_window"
            if use_anchor_window
            else ("reacquire_app_surface" if clean_anchor_app else "reacquire_current_surface")
        )
        signature_parts = [
            mission_kind,
            str(stop_reason_code or "").strip().lower(),
            str(blocking_surface.get("surface_signature", "") or "").strip().lower(),
            clean_anchor_app.lower(),
            clean_anchor_title.lower(),
            str(sanitized_resume_payload.get("window_title", "") or "").strip().lower(),
            str(len(remaining_form_targets or [])),
        ]
        resume_signature = hashlib.sha1("|".join(signature_parts).encode("utf-8")).hexdigest()[:16]
        return {
            "mission_kind": mission_kind,
            "resume_action": resume_action,
            "resume_strategy": resume_strategy,
            "resume_signature": resume_signature,
            "resume_payload": sanitized_resume_payload,
            "resume_preconditions": [str(item).strip() for item in blocking_surface.get("resume_preconditions", []) if str(item).strip()],
            "operator_steps": [str(item).strip() for item in blocking_surface.get("operator_steps", []) if str(item).strip()],
            "anchor_app_name": clean_anchor_app,
            "anchor_window_title": clean_anchor_title,
            "blocking_window_title": blocking_window_title,
            "surface_match_hints": {
                "anchor_app_name": clean_anchor_app,
                "anchor_window_title": clean_anchor_title,
                "blocking_window_title": blocking_window_title,
                "blocking_window_hwnd": int(blocking_surface.get("window_hwnd", 0) or 0),
                "screen_hash": str(blocking_surface.get("screen_hash", "") or "").strip(),
                "surface_signature": str(blocking_surface.get("surface_signature", "") or "").strip(),
                "approval_kind": str(blocking_surface.get("approval_kind", "") or "").strip(),
                "dialog_kind": str(blocking_surface.get("dialog_kind", "") or "").strip(),
                "prefer_anchor_on_resume": use_anchor_window,
                "allow_child_window_adoption": True,
            },
            "continuation_targets": [dict(row) for row in (remaining_form_targets or [])[:12] if isinstance(row, dict)],
        }

    @staticmethod
    def _normalize_resume_contract_payload(value: Any) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if isinstance(value, dict):
            payload = value
        elif isinstance(value, str) and str(value).strip():
            try:
                parsed = json.loads(str(value))
            except Exception:  # noqa: BLE001
                parsed = {}
            if isinstance(parsed, dict):
                payload = parsed
        if not payload:
            return {}
        return {
            "mission_kind": str(payload.get("mission_kind", "") or "").strip().lower(),
            "resume_action": str(payload.get("resume_action", "") or "").strip().lower(),
            "resume_strategy": str(payload.get("resume_strategy", "") or "").strip(),
            "resume_signature": str(payload.get("resume_signature", "") or "").strip(),
            "resume_payload": dict(payload.get("resume_payload", {})) if isinstance(payload.get("resume_payload", {}), dict) else {},
            "resume_preconditions": [str(item).strip() for item in payload.get("resume_preconditions", []) if str(item).strip()] if isinstance(payload.get("resume_preconditions", []), list) else [],
            "operator_steps": [str(item).strip() for item in payload.get("operator_steps", []) if str(item).strip()] if isinstance(payload.get("operator_steps", []), list) else [],
            "anchor_app_name": str(payload.get("anchor_app_name", "") or "").strip(),
            "anchor_window_title": str(payload.get("anchor_window_title", "") or "").strip(),
            "blocking_window_title": str(payload.get("blocking_window_title", "") or "").strip(),
            "surface_match_hints": dict(payload.get("surface_match_hints", {})) if isinstance(payload.get("surface_match_hints", {}), dict) else {},
            "continuation_targets": [dict(row) for row in payload.get("continuation_targets", []) if isinstance(row, dict)] if isinstance(payload.get("continuation_targets", []), list) else [],
        }

    @staticmethod
    def _normalize_blocking_surface_payload(value: Any) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if isinstance(value, dict):
            payload = value
        elif isinstance(value, str) and str(value).strip():
            try:
                parsed = json.loads(str(value))
            except Exception:  # noqa: BLE001
                parsed = {}
            if isinstance(parsed, dict):
                payload = parsed
        if not payload:
            return {}
        return {
            "mission_kind": str(payload.get("mission_kind", "") or "").strip().lower(),
            "stop_reason_code": str(payload.get("stop_reason_code", "") or "").strip(),
            "resume_action": str(payload.get("resume_action", "") or "").strip().lower(),
            "resume_preconditions": [str(item).strip() for item in payload.get("resume_preconditions", []) if str(item).strip()] if isinstance(payload.get("resume_preconditions", []), list) else [],
            "window_title": str(payload.get("window_title", "") or "").strip(),
            "window_hwnd": int(payload.get("window_hwnd", 0) or 0),
            "screen_hash": str(payload.get("screen_hash", "") or "").strip(),
            "page_kind": str(payload.get("page_kind", "") or "").strip(),
            "dialog_kind": str(payload.get("dialog_kind", "") or "").strip(),
            "approval_kind": str(payload.get("approval_kind", "") or "").strip(),
            "dialog_visible": bool(payload.get("dialog_visible", False)),
            "dialog_review_required": bool(payload.get("dialog_review_required", False)),
            "secure_desktop_likely": bool(payload.get("secure_desktop_likely", False)),
            "manual_input_required": bool(payload.get("manual_input_required", False)),
            "credential_field_count": int(payload.get("credential_field_count", 0) or 0),
            "preferred_confirmation_button": str(payload.get("preferred_confirmation_button", "") or "").strip(),
            "preferred_dismiss_button": str(payload.get("preferred_dismiss_button", "") or "").strip(),
            "safe_dialog_buttons": [str(item).strip() for item in payload.get("safe_dialog_buttons", []) if str(item).strip()] if isinstance(payload.get("safe_dialog_buttons", []), list) else [],
            "confirmation_dialog_buttons": [str(item).strip() for item in payload.get("confirmation_dialog_buttons", []) if str(item).strip()] if isinstance(payload.get("confirmation_dialog_buttons", []), list) else [],
            "destructive_dialog_buttons": [str(item).strip() for item in payload.get("destructive_dialog_buttons", []) if str(item).strip()] if isinstance(payload.get("destructive_dialog_buttons", []), list) else [],
            "credential_fields": [dict(row) for row in payload.get("credential_fields", []) if isinstance(row, dict)] if isinstance(payload.get("credential_fields", []), list) else [],
            "pending_requirements": [dict(row) for row in payload.get("pending_requirements", []) if isinstance(row, dict)] if isinstance(payload.get("pending_requirements", []), list) else [],
            "manual_required_controls": [dict(row) for row in payload.get("manual_required_controls", []) if isinstance(row, dict)] if isinstance(payload.get("manual_required_controls", []), list) else [],
            "blocking_controls": [dict(row) for row in payload.get("blocking_controls", []) if isinstance(row, dict)] if isinstance(payload.get("blocking_controls", []), list) else [],
            "autonomous_blocker": str(payload.get("autonomous_blocker", "") or "").strip(),
            "recommended_actions": [str(item).strip() for item in payload.get("recommended_actions", []) if str(item).strip()] if isinstance(payload.get("recommended_actions", []), list) else [],
            "operator_steps": [str(item).strip() for item in payload.get("operator_steps", []) if str(item).strip()] if isinstance(payload.get("operator_steps", []), list) else [],
            "surface_signature": str(payload.get("surface_signature", "") or "").strip(),
            "target_group_state": dict(payload.get("target_group_state", {})) if isinstance(payload.get("target_group_state", {}), dict) else {},
            "notes": [str(item).strip() for item in payload.get("notes", []) if str(item).strip()] if isinstance(payload.get("notes", []), list) else [],
        }

    def _resume_payload_from_contract(
        self,
        *,
        args: Dict[str, Any],
        resume_contract: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(resume_contract, dict) or not resume_contract:
            return {}
        base_payload = (
            dict(resume_contract.get("resume_payload", {}))
            if isinstance(resume_contract.get("resume_payload", {}), dict)
            else {}
        )
        resume_action = str(base_payload.get("action", "") or resume_contract.get("resume_action", "")).strip().lower()
        if resume_action not in RESUMEABLE_MISSION_ACTIONS:
            return {}
        base_payload["action"] = resume_action
        provided_fields = {
            str(item).strip()
            for item in args.get("_provided_fields", [])
            if str(item).strip()
        } if isinstance(args.get("_provided_fields", []), list) else set()
        override_fields = {
            "mission_id",
            "mission_kind",
            "app_name",
            "window_title",
            "focus_first",
            "verify_after_action",
            "verify_text",
            "retry_on_verification_failure",
            "max_strategy_attempts",
            "max_exploration_steps",
            "max_wizard_pages",
            "allow_warning_pages",
            "max_form_pages",
            "allow_destructive_forms",
            "form_target_plan",
            "expected_form_target_count",
            "exploration_limit",
            "attempted_targets",
            "surface_signature_history",
        }
        for field_name in override_fields:
            if field_name not in provided_fields:
                continue
            base_payload[field_name] = args.get(field_name)
        normalized_payload = self._normalize_payload(base_payload)
        if str(normalized_payload.get("action", "") or "").strip().lower() not in RESUMEABLE_MISSION_ACTIONS:
            return {}
        normalized_payload.pop("resume_contract", None)
        normalized_payload.pop("blocking_surface", None)
        if str(args.get("mission_id", "") or "").strip():
            normalized_payload["mission_id"] = str(args.get("mission_id", "") or "").strip()
        if str(args.get("mission_kind", "") or "").strip():
            normalized_payload["mission_kind"] = str(args.get("mission_kind", "") or "").strip().lower()
        normalized_payload["resume_force"] = bool(args.get("resume_force", False))
        return normalized_payload

    def _mission_surface_signature(self, *, snapshot: Dict[str, Any], mission_kind: str) -> str:
        if not isinstance(snapshot, dict) or mission_kind not in {"wizard", "form", "exploration"}:
            return ""
        if mission_kind == "exploration":
            target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
            active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
            observation = snapshot.get("observation", {}) if isinstance(snapshot.get("observation", {}), dict) else {}
            safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
            dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
            surface_flags = snapshot.get("surface_flags", {}) if isinstance(snapshot.get("surface_flags", {}), dict) else {}
            surface_mode = self._surface_exploration_surface_mode(
                app_profile=snapshot.get("app_profile", {}) if isinstance(snapshot.get("app_profile", {}), dict) else {},
                surface_flags=surface_flags,
                safety_signals=safety_signals,
                snapshot=snapshot,
            )
            query_targets = snapshot.get("query_targets", []) if isinstance(snapshot.get("query_targets", []), list) else []
            top_target = query_targets[0] if query_targets and isinstance(query_targets[0], dict) else {}
            signature_parts = [
                mission_kind,
                str(target_window.get("title", "") or active_window.get("title", "") or "").strip().lower(),
                str(target_window.get("hwnd", 0) or active_window.get("hwnd", 0) or 0),
                str(observation.get("screen_hash", "") or "").strip().lower(),
                str(surface_mode or "").strip().lower(),
                str(dialog_state.get("dialog_kind", "") or "").strip().lower(),
                str(dialog_state.get("approval_kind", "") or "").strip().lower(),
                str(top_target.get("element_id", "") or top_target.get("automation_id", "") or top_target.get("name", "") or "").strip().lower(),
            ]
            return hashlib.sha1("|".join(signature_parts).encode("utf-8")).hexdigest()[:16]
        safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        wizard_page_state = snapshot.get("wizard_page_state", {}) if isinstance(snapshot.get("wizard_page_state", {}), dict) else {}
        form_page_state = snapshot.get("form_page_state", {}) if isinstance(snapshot.get("form_page_state", {}), dict) else {}
        page_state = wizard_page_state if mission_kind == "wizard" else form_page_state
        summary = self._wizard_flow_summary(snapshot=snapshot) if mission_kind == "wizard" else self._form_flow_summary(snapshot=snapshot)
        signature_parts = [
            mission_kind,
            str(summary.get("window_title", "") or "").strip().lower(),
            str(summary.get("window_hwnd", 0) or 0),
            str(summary.get("screen_hash", "") or "").strip().lower(),
            str(dialog_state.get("dialog_kind", "") or "").strip().lower(),
            str(dialog_state.get("approval_kind", "") or "").strip().lower(),
            str(page_state.get("page_kind", "") or "").strip().lower(),
            str(page_state.get("autonomous_blocker", "") or "").strip().lower(),
        ]
        return hashlib.sha1("|".join(signature_parts).encode("utf-8")).hexdigest()[:16]

    def _resume_mission_context(
        self,
        *,
        args: Dict[str, Any],
        resume_contract: Dict[str, Any],
        blocking_surface: Dict[str, Any],
        resume_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(resume_contract, dict) or not resume_contract or not isinstance(resume_payload, dict) or not resume_payload:
            return {
                "status": "invalid",
                "message": "resume_contract is required to continue a paused desktop mission.",
                "warnings": [],
                "blockers": ["resume_contract is required to continue a paused desktop mission."],
            }
        resume_action = str(resume_payload.get("action", "") or resume_contract.get("resume_action", "")).strip().lower()
        mission_kind = str(
            resume_contract.get("mission_kind", "")
            or (
                "wizard"
                if resume_action == "complete_wizard_flow"
                else "form"
                if resume_action == "complete_form_flow"
                else "exploration"
                if resume_action in {EXPLORATION_ADVANCE_ACTION, EXPLORATION_FLOW_ACTION}
                else ""
            )
        ).strip().lower()
        if mission_kind not in {"wizard", "form", "exploration"} or resume_action not in RESUMEABLE_MISSION_ACTIONS:
            return {
                "status": "invalid",
                "mission_kind": mission_kind,
                "resume_action": resume_action,
                "message": "resume_contract does not describe a supported desktop mission continuation.",
                "warnings": [],
                "blockers": ["resume_contract does not describe a supported desktop mission continuation."],
            }
        surface_hints = resume_contract.get("surface_match_hints", {}) if isinstance(resume_contract.get("surface_match_hints", {}), dict) else {}
        anchor_app_name = str(
            resume_payload.get("app_name", "") or surface_hints.get("anchor_app_name", "") or resume_contract.get("anchor_app_name", "") or args.get("app_name", "")
        ).strip()
        anchor_window_title = str(
            resume_payload.get("window_title", "") or surface_hints.get("anchor_window_title", "") or resume_contract.get("anchor_window_title", "")
        ).strip()
        blocking_window_title = str(
            blocking_surface.get("window_title", "") or surface_hints.get("blocking_window_title", "") or resume_contract.get("blocking_window_title", "")
        ).strip()
        continuation_targets = [
            dict(row)
            for row in (
                resume_contract.get("continuation_targets", [])
                if isinstance(resume_contract.get("continuation_targets", []), list)
                else resume_payload.get("form_target_plan", [])
            )
            if isinstance(row, dict)
        ]
        query_hint = ""
        for target_row in continuation_targets:
            query_hint = (
                str(target_row.get("query", "") or target_row.get("target", "") or target_row.get("label", "") or target_row.get("name", "")).strip()
            )
            if query_hint:
                break
        preferred_actions = [resume_action, "dismiss_dialog", "confirm_dialog"]
        current_snapshot = self.surface_snapshot(
            app_name=anchor_app_name,
            window_title=blocking_window_title or anchor_window_title,
            query=query_hint,
            limit=12,
            include_observation=True,
            include_elements=True,
            include_workflow_probes=True,
            preferred_actions=preferred_actions,
        )
        current_snapshot = current_snapshot if isinstance(current_snapshot, dict) else {}
        safety_signals = current_snapshot.get("safety_signals", {}) if isinstance(current_snapshot.get("safety_signals", {}), dict) else {}
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        target_window = current_snapshot.get("target_window", {}) if isinstance(current_snapshot.get("target_window", {}), dict) else {}
        active_window = current_snapshot.get("active_window", {}) if isinstance(current_snapshot.get("active_window", {}), dict) else {}
        surface_flags = current_snapshot.get("surface_flags", {}) if isinstance(current_snapshot.get("surface_flags", {}), dict) else {}
        current_window_title = str(target_window.get("title", "") or active_window.get("title", "") or "").strip()
        current_window_hwnd = int(target_window.get("hwnd", 0) or active_window.get("hwnd", 0) or 0)
        current_dialog_kind = str(dialog_state.get("dialog_kind", "") or "").strip()
        current_approval_kind = str(dialog_state.get("approval_kind", "") or "").strip()
        window_reacquired = bool(
            surface_flags.get("window_targeted", False)
            or self._window_matches(target_window, app_name=anchor_app_name, window_title=anchor_window_title or blocking_window_title)
            or self._window_matches(active_window, app_name=anchor_app_name, window_title=anchor_window_title or blocking_window_title)
        )
        current_signature = self._mission_surface_signature(snapshot=current_snapshot, mission_kind=mission_kind)
        blocking_signature = str(
            blocking_surface.get("surface_signature", "") or surface_hints.get("surface_signature", "")
        ).strip()
        blocking_dialog_kind = str(
            blocking_surface.get("dialog_kind", "") or surface_hints.get("dialog_kind", "")
        ).strip()
        blocking_approval_kind = str(
            blocking_surface.get("approval_kind", "") or surface_hints.get("approval_kind", "")
        ).strip()
        dialog_visible = bool(dialog_state.get("visible", False) or surface_flags.get("dialog_visible", False))
        blocking_surface_still_visible = False
        if dialog_visible:
            blocking_surface_still_visible = bool(
                (blocking_signature and current_signature and blocking_signature == current_signature)
                or (blocking_approval_kind and self._normalize_probe_text(current_approval_kind) == self._normalize_probe_text(blocking_approval_kind))
                or (blocking_dialog_kind and self._normalize_probe_text(current_dialog_kind) == self._normalize_probe_text(blocking_dialog_kind))
                or self._normalize_probe_text(current_approval_kind) in RESUME_APPROVAL_KINDS
                or (
                    self._normalize_probe_text(current_dialog_kind) in {"credential_prompt", "authentication_prompt", "elevation_prompt"}
                    and (
                        bool(dialog_state.get("manual_input_required", False))
                        or bool(dialog_state.get("credential_required", False))
                        or bool(dialog_state.get("authentication_required", False))
                        or bool(dialog_state.get("review_required", False))
                    )
                )
                or bool(dialog_state.get("secure_desktop_likely", False))
            )
        form_target_state = (
            self._form_target_plan_state(plan=continuation_targets, snapshot=current_snapshot)
            if mission_kind == "form" and continuation_targets
            else {}
        )
        visible_continuation_target_count = (
            int(form_target_state.get("visible_pending_count", 0) or 0)
            if isinstance(form_target_state, dict)
            else 0
        )
        remaining_continuation_target_count = (
            int(form_target_state.get("remaining_count", 0) or 0)
            if isinstance(form_target_state, dict)
            else 0
        )
        warnings: List[str] = []
        blockers: List[str] = []
        if blocking_surface_still_visible and not bool(args.get("resume_force", False)):
            warnings.append(
                "The blocking review or approval surface still appears to be active, so JARVIS will wait for it to clear before resuming the paused mission."
            )
            blockers.append(
                "The blocking review or approval surface still appears to be active."
            )
        elif not window_reacquired:
            warnings.append(
                "The blocking surface appears to be clear, but JARVIS could not strongly reacquire the target app window and will rely on normal target discovery during resume."
            )
        message = (
            "The paused desktop mission is ready to resume."
            if not blockers
            else "The paused desktop mission still appears to be waiting on manual review or approval."
        )
        return {
            "status": "ready" if not blockers else "blocked",
            "mission_kind": mission_kind,
            "resume_action": resume_action,
            "resume_strategy": str(resume_contract.get("resume_strategy", "") or "").strip(),
            "resume_signature": str(resume_contract.get("resume_signature", "") or "").strip(),
            "window_reacquired": window_reacquired,
            "blocking_surface_still_visible": blocking_surface_still_visible,
            "surface_signature_match": bool(blocking_signature and current_signature and blocking_signature == current_signature),
            "current_window_title": current_window_title,
            "current_window_hwnd": current_window_hwnd,
            "current_dialog_kind": current_dialog_kind,
            "current_approval_kind": current_approval_kind,
            "continuation_target_count": len(continuation_targets),
            "visible_continuation_target_count": visible_continuation_target_count,
            "remaining_continuation_target_count": remaining_continuation_target_count,
            "warnings": warnings,
            "blockers": blockers,
            "message": message,
            "current_snapshot": current_snapshot,
            "form_target_state": form_target_state,
        }

    def _mission_record_for_resume(self, *, args: Dict[str, Any]) -> Dict[str, Any]:
        mission_id = str(args.get("mission_id", "") or "").strip()
        mission_kind = str(args.get("mission_kind", "") or "").strip().lower()
        app_name = str(args.get("app_name", "") or "").strip()
        payload = self._mission_memory.resolve_resume_reference(
            mission_id=mission_id,
            mission_kind=mission_kind,
            app_name=app_name,
        )
        mission = payload.get("mission", {}) if isinstance(payload.get("mission", {}), dict) else {}
        return mission if mission else {}

    def _persist_paused_mission(
        self,
        *,
        mission_kind: str,
        args: Dict[str, Any],
        blocking_surface: Dict[str, Any],
        resume_contract: Dict[str, Any],
        mission_payload: Dict[str, Any],
        warnings: List[str],
        message: str,
    ) -> Dict[str, Any]:
        payload = self._mission_memory.save_paused_mission(
            mission_kind=mission_kind,
            args=args,
            resume_contract=resume_contract,
            blocking_surface=blocking_surface,
            mission_payload=mission_payload,
            message=message,
            warnings=warnings,
        )
        mission = payload.get("mission", {}) if isinstance(payload.get("mission", {}), dict) else {}
        mission_id = str(mission.get("mission_id", "") or "").strip()
        if not mission_id:
            return {}
        if isinstance(blocking_surface, dict):
            blocking_surface["mission_id"] = mission_id
        if isinstance(resume_contract, dict):
            resume_contract["mission_id"] = mission_id
            resume_payload = resume_contract.get("resume_payload", {}) if isinstance(resume_contract.get("resume_payload", {}), dict) else {}
            if resume_payload is not None:
                resume_payload = dict(resume_payload)
                resume_payload["mission_id"] = mission_id
                if not str(resume_payload.get("mission_kind", "") or "").strip():
                    resume_payload["mission_kind"] = mission_kind
                resume_contract["resume_payload"] = resume_payload
        return mission

    def _advise_resume_mission(self, *, args: Dict[str, Any]) -> Dict[str, Any]:
        resume_contract = (
            dict(args.get("resume_contract", {}))
            if isinstance(args.get("resume_contract", {}), dict)
            else {}
        )
        blocking_surface = (
            dict(args.get("blocking_surface", {}))
            if isinstance(args.get("blocking_surface", {}), dict)
            else {}
        )
        mission_record = self._mission_record_for_resume(args=args) if not resume_contract else {}
        if mission_record:
            if not resume_contract:
                resume_contract = dict(mission_record.get("resume_contract", {})) if isinstance(mission_record.get("resume_contract", {}), dict) else {}
            if not blocking_surface:
                blocking_surface = dict(mission_record.get("blocking_surface", {})) if isinstance(mission_record.get("blocking_surface", {}), dict) else {}
            if not str(args.get("mission_id", "") or "").strip():
                args["mission_id"] = str(mission_record.get("mission_id", "") or "").strip()
            if not str(args.get("mission_kind", "") or "").strip():
                args["mission_kind"] = str(mission_record.get("mission_kind", "") or "").strip().lower()
        elif not resume_contract and (
            str(args.get("mission_id", "") or "").strip()
            or str(args.get("app_name", "") or "").strip()
            or str(args.get("mission_kind", "") or "").strip()
        ):
            message = "No paused desktop mission matched the requested mission reference."
            return {
                "status": "blocked",
                "action": "resume_mission",
                "resume_action": "",
                "resume_payload": {},
                "resume_contract": {},
                "blocking_surface": {},
                "mission_record": {},
                "resume_context": {
                    "status": "missing",
                    "message": message,
                    "warnings": [],
                    "blockers": [message],
                },
                "route_mode": "resume_desktop_mission",
                "confidence": 0.0,
                "risk_level": "medium",
                "app_profile": {},
                "profile_defaults_applied": {},
                "target_window": {},
                "active_window": {},
                "candidate_windows": [],
                "capabilities": self._capabilities(),
                "execution_plan": [],
                "blockers": [message],
                "warnings": [],
                "autonomy": {"supports_resume": True, "requires_manual_clearance": False, "resume_force": bool(args.get("resume_force", False))},
                "workflow_profile": {},
                "surface_snapshot": {},
                "safety_signals": {},
                "form_target_state": {},
                "surface_branch": {},
                "verification_plan": {},
                "adaptive_strategy": {},
                "strategy_variants": [],
                "message": message,
            }
        resume_payload = self._resume_payload_from_contract(args=args, resume_contract=resume_contract)
        resume_context = self._resume_mission_context(
            args=args,
            resume_contract=resume_contract,
            blocking_surface=blocking_surface,
            resume_payload=resume_payload,
        )
        current_snapshot = resume_context.get("current_snapshot", {}) if isinstance(resume_context.get("current_snapshot", {}), dict) else {}
        if str(resume_context.get("status", "") or "") != "ready":
            app_profile = current_snapshot.get("app_profile", {}) if isinstance(current_snapshot.get("app_profile", {}), dict) else {}
            return {
                "status": "blocked",
                "action": "resume_mission",
                "resume_action": str(resume_context.get("resume_action", "") or ""),
                "resume_payload": self._sanitize_payload_for_response(resume_payload) if resume_payload else {},
                "resume_contract": resume_contract,
                "blocking_surface": blocking_surface,
                "mission_record": mission_record,
                "resume_context": resume_context,
                "route_mode": "resume_desktop_mission",
                "confidence": 0.0,
                "risk_level": "high" if bool(resume_context.get("blocking_surface_still_visible", False)) else "medium",
                "app_profile": app_profile,
                "profile_defaults_applied": current_snapshot.get("profile_defaults_applied", {}) if isinstance(current_snapshot.get("profile_defaults_applied", {}), dict) else {},
                "target_window": current_snapshot.get("target_window", {}) if isinstance(current_snapshot.get("target_window", {}), dict) else {},
                "active_window": current_snapshot.get("active_window", {}) if isinstance(current_snapshot.get("active_window", {}), dict) else {},
                "candidate_windows": current_snapshot.get("candidate_windows", []) if isinstance(current_snapshot.get("candidate_windows", []), list) else [],
                "capabilities": current_snapshot.get("capabilities", {}) if isinstance(current_snapshot.get("capabilities", {}), dict) else self._capabilities(),
                "execution_plan": [],
                "blockers": [str(item).strip() for item in resume_context.get("blockers", []) if str(item).strip()],
                "warnings": [str(item).strip() for item in resume_context.get("warnings", []) if str(item).strip()],
                "autonomy": {
                    "supports_resume": True,
                    "requires_manual_clearance": bool(resume_context.get("blocking_surface_still_visible", False)),
                    "resume_force": bool(args.get("resume_force", False)),
                },
                "workflow_profile": {},
                "surface_snapshot": current_snapshot,
                "safety_signals": current_snapshot.get("safety_signals", {}) if isinstance(current_snapshot.get("safety_signals", {}), dict) else {},
                "form_target_state": resume_context.get("form_target_state", {}) if isinstance(resume_context.get("form_target_state", {}), dict) else {},
                "surface_branch": {},
                "verification_plan": {},
                "adaptive_strategy": {},
                "strategy_variants": [],
                "message": str(resume_context.get("message", "") or ""),
            }
        target_advice = self.advise(resume_payload)
        warnings = self._dedupe_strings(
            [str(item).strip() for item in target_advice.get("warnings", []) if str(item).strip()]
            + [str(item).strip() for item in resume_context.get("warnings", []) if str(item).strip()]
        )
        blockers = self._dedupe_strings(
            [str(item).strip() for item in target_advice.get("blockers", []) if str(item).strip()]
        )
        return {
            **target_advice,
            "action": "resume_mission",
            "resume_action": str(resume_context.get("resume_action", "") or ""),
            "resume_payload": self._sanitize_payload_for_response(resume_payload),
            "resume_contract": resume_contract,
            "blocking_surface": blocking_surface,
            "mission_record": mission_record,
            "resume_context": resume_context,
            "route_mode": "resume_desktop_mission",
            "warnings": warnings,
            "blockers": blockers,
            "message": str(target_advice.get("message", "") or resume_context.get("message", "") or ""),
        }

    def _execute_resume_mission_strategy(
        self,
        *,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        strategy: Dict[str, Any],
        attempt_index: int,
    ) -> Dict[str, Any]:
        resume_context = advice.get("resume_context", {}) if isinstance(advice.get("resume_context", {}), dict) else {}
        resume_payload = advice.get("resume_payload", {}) if isinstance(advice.get("resume_payload", {}), dict) else {}
        mission_record = advice.get("mission_record", {}) if isinstance(advice.get("mission_record", {}), dict) else {}
        if str(resume_context.get("status", "") or "") != "ready" or not resume_payload:
            return {
                "attempt": attempt_index,
                "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
                "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
                "strategy_reason": str(strategy.get("reason", "") or "").strip(),
                "payload": self._sanitize_payload_for_response(args),
                "status": "blocked",
                "message": str(resume_context.get("message", "") or "desktop mission resume is not ready"),
                "final_action": "",
                "results": [],
                "advice": advice,
                "verification": {
                    "enabled": bool(args.get("verify_after_action", True)),
                    "status": "skipped",
                    "verified": False,
                    "message": "resume execution skipped because the paused mission is not ready to continue",
                },
                "mission_record": mission_record,
                "resume_context": resume_context,
            }
        nested_result = self.execute(resume_payload)
        verification = nested_result.get("verification", {}) if isinstance(nested_result.get("verification", {}), dict) else {}
        mission_id = str(
            args.get("mission_id", "")
            or mission_record.get("mission_id", "")
            or advice.get("resume_contract", {}).get("mission_id", "")
        ).strip() if isinstance(advice.get("resume_contract", {}), dict) else str(args.get("mission_id", "") or mission_record.get("mission_id", "")).strip()
        updated_mission_record = {}
        if mission_id:
            completed = bool(
                str(nested_result.get("status", "") or "") == "success"
                and (
                    bool(nested_result.get("wizard_mission", {}).get("completed", False)) if isinstance(nested_result.get("wizard_mission", {}), dict) else False
                    or bool(nested_result.get("form_mission", {}).get("completed", False)) if isinstance(nested_result.get("form_mission", {}), dict) else False
                    or bool(nested_result.get("exploration_mission", {}).get("completed", False)) if isinstance(nested_result.get("exploration_mission", {}), dict) else False
                )
            )
            mission_payload = (
                nested_result.get("wizard_mission", {})
                if isinstance(nested_result.get("wizard_mission", {}), dict) and nested_result.get("wizard_mission")
                else nested_result.get("form_mission", {})
                if isinstance(nested_result.get("form_mission", {}), dict) and nested_result.get("form_mission")
                else nested_result.get("exploration_mission", {})
                if isinstance(nested_result.get("exploration_mission", {}), dict)
                else {}
            )
            updated = self._mission_memory.mark_resumed(
                mission_id=mission_id,
                outcome_status=str(nested_result.get("status", "") or "unknown"),
                message=str(nested_result.get("message", "") or ""),
                completed=completed,
                mission_payload=mission_payload if isinstance(mission_payload, dict) else {},
            )
            updated_mission_record = updated.get("mission", {}) if isinstance(updated.get("mission", {}), dict) else {}
        nested_resume_context = {
            **resume_context,
            "status": "resumed" if str(nested_result.get("status", "") or "") in {"success", "partial"} else "resume_error",
            "message": str(nested_result.get("message", "") or resume_context.get("message", "") or ""),
        }
        return {
            "attempt": attempt_index,
            "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
            "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
            "strategy_reason": str(strategy.get("reason", "") or "").strip(),
            "payload": self._sanitize_payload_for_response(args),
            "status": str(nested_result.get("status", "success") or "success"),
            "message": str(nested_result.get("message", "") or ""),
            "final_action": str(nested_result.get("final_action", "") or advice.get("resume_action", "")),
            "results": nested_result.get("results", []) if isinstance(nested_result.get("results", []), list) else [],
            "advice": advice,
            "verification": verification,
            "wizard_mission": nested_result.get("wizard_mission", {}) if isinstance(nested_result.get("wizard_mission", {}), dict) else {},
            "form_mission": nested_result.get("form_mission", {}) if isinstance(nested_result.get("form_mission", {}), dict) else {},
            "exploration_mission": nested_result.get("exploration_mission", {}) if isinstance(nested_result.get("exploration_mission", {}), dict) else {},
            "mission_record": updated_mission_record or mission_record,
            "resume_context": nested_resume_context,
        }

    def _resolve_wizard_dialog_interstitial(
        self,
        *,
        args: Dict[str, Any],
        snapshot: Dict[str, Any],
        page_index: int,
    ) -> Dict[str, Any]:
        flags = snapshot.get("surface_flags", {}) if isinstance(snapshot.get("surface_flags", {}), dict) else {}
        safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
        if not bool(flags.get("dialog_visible", False)):
            return {"handled": False, "blocked": False}

        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        dialog_kind = self._normalize_probe_text(dialog_state.get("dialog_kind", ""))
        approval_kind = self._normalize_probe_text(dialog_state.get("approval_kind", ""))
        dialog_manual_input_required = bool(dialog_state.get("manual_input_required", False))
        dialog_credential_required = bool(dialog_state.get("credential_required", False))
        secure_desktop_likely = bool(dialog_state.get("secure_desktop_likely", False))
        destructive_warning_visible = bool(safety_signals.get("destructive_warning_visible", False))
        warning_surface_visible = bool(safety_signals.get("warning_surface_visible", False))
        elevation_prompt_visible = bool(safety_signals.get("elevation_prompt_visible", False))
        preferred_confirmation_button = str(safety_signals.get("preferred_confirmation_button", "") or "").strip()
        preferred_confirmation_target = safety_signals.get("preferred_confirmation_target", {}) if isinstance(safety_signals.get("preferred_confirmation_target", {}), dict) else {}
        preferred_dismiss_button = str(safety_signals.get("preferred_dismiss_button", "") or "").strip()
        preferred_dismiss_target = safety_signals.get("preferred_dismiss_target", {}) if isinstance(safety_signals.get("preferred_dismiss_target", {}), dict) else {}
        if approval_kind == "elevation_credentials":
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "elevation_credentials_required",
                "stop_reason": "Wizard mission paused because an interstitial elevation dialog is requesting administrator credentials or secure sign-in input.",
                "button_label": preferred_confirmation_button or preferred_dismiss_button,
                "button_role": "",
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }
        if approval_kind == "elevation_consent":
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "elevation_consent_required",
                "stop_reason": "Wizard mission paused because an interstitial elevation dialog is requesting administrator approval."
                + (" The prompt also appears to be on a secure desktop surface." if secure_desktop_likely else ""),
                "button_label": preferred_confirmation_button or preferred_dismiss_button,
                "button_role": "",
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }
        if dialog_manual_input_required and dialog_credential_required:
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "credential_input_required",
                "stop_reason": "Wizard mission paused because an interstitial dialog is requesting credentials or sign-in input.",
                "button_label": preferred_confirmation_button or preferred_dismiss_button,
                "button_role": "",
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }
        if dialog_kind == "authentication_review":
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "authentication_review_required",
                "stop_reason": "Wizard mission paused on an interstitial authentication confirmation so JARVIS does not approve identity-sensitive changes blindly.",
                "button_label": preferred_confirmation_button or preferred_dismiss_button,
                "button_role": "",
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }
        if dialog_kind == "permission_review":
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "permission_review_required",
                "stop_reason": "Wizard mission paused on an interstitial permission review so JARVIS does not approve app access or consent changes blindly.",
                "button_label": preferred_confirmation_button or preferred_dismiss_button,
                "button_role": "",
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }

        action = ""
        button_label = ""
        button_role = ""
        button_target: Dict[str, Any] = {}
        normalized_dismiss = self._normalize_probe_text(preferred_dismiss_button)
        if preferred_confirmation_button and not destructive_warning_visible and not warning_surface_visible and not elevation_prompt_visible:
            action = "press_dialog_button"
            button_label = preferred_confirmation_button
            button_role = "confirm"
            button_target = preferred_confirmation_target
        elif preferred_dismiss_button and normalized_dismiss in {"close", "dismiss"} and not destructive_warning_visible and not warning_surface_visible and not elevation_prompt_visible:
            action = "press_dialog_button"
            button_label = preferred_dismiss_button
            button_role = "dismiss"
            button_target = preferred_dismiss_target
        else:
            blocker_message = "Wizard mission paused on an interstitial dialog that requires review before automation can continue."
            blocker_code = "wizard_dialog_review_required"
            if elevation_prompt_visible:
                blocker_code = "elevation_prompt_requires_approval"
                blocker_message = "Wizard mission paused because an interstitial dialog is requesting elevated privileges."
            elif destructive_warning_visible or warning_surface_visible:
                blocker_code = "wizard_dialog_review_required"
                blocker_message = "Wizard mission paused on an interstitial warning dialog so JARVIS does not auto-confirm a risky step."
            elif not preferred_confirmation_button and not preferred_dismiss_button:
                blocker_code = "wizard_dialog_route_unavailable"
                blocker_message = "Wizard mission found an interstitial dialog, but it does not expose a reliable button target for autonomous continuation."
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": blocker_code,
                "stop_reason": blocker_message,
                "button_label": preferred_confirmation_button or preferred_dismiss_button,
                "button_role": "confirm" if preferred_confirmation_button else ("dismiss" if preferred_dismiss_button else ""),
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }

        dialog_args = dict(args)
        summary = self._wizard_flow_summary(snapshot=snapshot)
        dialog_window_title = str(summary.get("window_title", "") or "").strip()
        if dialog_window_title:
            dialog_args["window_title"] = dialog_window_title
            dialog_args["app_name"] = ""
            dialog_args["focus_first"] = True
        dialog_args["action"] = action
        dialog_args["query"] = button_label
        dialog_args["control_type"] = "Button"
        element_id = str(button_target.get("element_id", "") or "").strip()
        if element_id:
            dialog_args["element_id"] = element_id
        dialog_args["_provided_fields"] = self._dedupe_strings(
            list(dialog_args.get("_provided_fields", []))
            + ["action", "query", "control_type"]
            + ([] if not element_id else ["element_id"])
        )

        dialog_advice = self.advise(dialog_args)
        if dialog_advice.get("status") != "success":
            blockers = "; ".join(str(item).strip() for item in dialog_advice.get("blockers", []) if str(item).strip())
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "wizard_dialog_route_unavailable",
                "stop_reason": blockers or str(dialog_advice.get("message", "wizard dialog route unavailable") or "wizard dialog route unavailable"),
                "button_label": button_label,
                "button_role": button_role,
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }

        dialog_execution = self._run_execution_plan(
            plan=dialog_advice.get("execution_plan", []),
            result_metadata={
                "wizard_stage": "dialog_followup",
                "wizard_page_index": page_index,
                "dialog_button": button_label,
                "dialog_button_role": button_role,
            },
        )
        after_snapshot = self._wizard_flow_snapshot(args=dialog_args, advice=dialog_advice)
        after_summary = self._wizard_flow_summary(snapshot=after_snapshot)
        progressed = self._wizard_flow_progressed(before_snapshot=snapshot, after_snapshot=after_snapshot)
        if bool(flags.get("dialog_visible", False)) and not bool(after_summary.get("dialog_visible", False)):
            progressed = True
        if str(dialog_execution.get("status", "success") or "success") != "success":
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "wizard_dialog_execution_failed",
                "stop_reason": str(dialog_execution.get("message", "wizard dialog execution failed") or "wizard dialog execution failed"),
                "button_label": button_label,
                "button_role": button_role,
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }

        return {
            "handled": str(dialog_execution.get("status", "success") or "success") == "success",
            "blocked": False,
            "action": action,
            "button_label": button_label,
            "button_role": button_role,
            "route_mode": str(dialog_advice.get("route_mode", "") or "").strip(),
            "dialog_kind": dialog_kind,
            "approval_kind": approval_kind,
            "secure_desktop_likely": secure_desktop_likely,
            "status": "dialog_confirmed" if button_role == "confirm" else "dialog_dismissed",
            "message": (
                f"Resolved the interstitial dialog through '{button_label}' and continued the wizard mission."
                if button_label
                else "Resolved the interstitial dialog and continued the wizard mission."
            ),
            "warnings": [str(item).strip() for item in dialog_advice.get("warnings", []) if str(item).strip()],
            "execution": dialog_execution,
            "after_snapshot": after_snapshot,
            "after_summary": after_summary,
            "progressed": progressed,
        }

    def _wizard_flow_snapshot(self, *, args: Dict[str, Any], advice: Dict[str, Any]) -> Dict[str, Any]:
        target_window = advice.get("target_window", {}) if isinstance(advice.get("target_window", {}), dict) else {}
        focus_title = str(
            args.get("window_title", "")
            or target_window.get("title", "")
            or args.get("app_name", "")
            or ""
        ).strip()
        snapshot = self.surface_snapshot(
            app_name=str(args.get("app_name", "") or "").strip(),
            window_title=focus_title,
            query="",
            limit=18,
            include_observation=True,
            include_elements=True,
            include_workflow_probes=True,
            preferred_actions=["complete_wizard_flow", "complete_wizard_page", "finish_wizard", "next_wizard_step", "dismiss_dialog"],
        )
        candidate_windows = snapshot.get("candidate_windows", []) if isinstance(snapshot.get("candidate_windows", []), list) else []
        active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
        app_name = str(args.get("app_name", "") or "").strip()
        window_title = str(args.get("window_title", "") or "").strip()
        target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
        target_visible = bool(target_window) or any(isinstance(row, dict) for row in candidate_windows)
        active_matches = self._window_matches(active_window, app_name=app_name, window_title=window_title or focus_title)
        adopted_title = str(active_window.get("title", "") or "").strip()
        normalized_adopted = self._normalize_probe_text(adopted_title)
        normalized_focus = self._normalize_probe_text(focus_title)
        normalized_target = self._normalize_probe_text(target_window.get("title", ""))
        active_hwnd = self._to_int(active_window.get("hwnd"))
        target_hwnd = self._to_int(target_window.get("hwnd"))
        should_try_adopt = bool(
            adopted_title
            and (
                not active_matches
                or (
                    active_hwnd is not None
                    and target_hwnd is not None
                    and active_hwnd != target_hwnd
                    and normalized_adopted != normalized_target
                )
            )
        )
        if should_try_adopt:
            if adopted_title and (normalized_adopted != normalized_focus or active_hwnd != target_hwnd):
                adopted_snapshot = self.surface_snapshot(
                    app_name="",
                    window_title=adopted_title,
                    query="",
                    limit=18,
                    include_observation=True,
                    include_elements=True,
                    include_workflow_probes=True,
                    preferred_actions=["complete_wizard_flow", "complete_wizard_page", "finish_wizard", "next_wizard_step", "dismiss_dialog"],
                )
                adopted_flags = adopted_snapshot.get("surface_flags", {}) if isinstance(adopted_snapshot.get("surface_flags", {}), dict) else {}
                adopted_page_state = adopted_snapshot.get("wizard_page_state", {}) if isinstance(adopted_snapshot.get("wizard_page_state", {}), dict) else {}
                adopted_target_window = adopted_snapshot.get("target_window", {}) if isinstance(adopted_snapshot.get("target_window", {}), dict) else {}
                adopted_active_window = adopted_snapshot.get("active_window", {}) if isinstance(adopted_snapshot.get("active_window", {}), dict) else {}
                adopted_actionable_surface = bool(
                    adopted_flags.get("wizard_surface_visible", False)
                    or adopted_flags.get("dialog_visible", False)
                    or adopted_page_state
                )
                if adopted_actionable_surface and (adopted_target_window or adopted_active_window):
                    adopted_active_title = str(adopted_active_window.get("title", "") or adopted_title).strip()
                    adopted_target_title = str(adopted_target_window.get("title", "") or "").strip()
                    resolved_adopted_title = adopted_active_title or adopted_target_title or adopted_title
                    adopted_snapshot["adopted_wizard_window"] = {
                        "title": resolved_adopted_title,
                        "hwnd": self._to_int(adopted_active_window.get("hwnd")) or self._to_int(adopted_target_window.get("hwnd")),
                        "previous_title": focus_title,
                        "reason": "active_child_window",
                    }
                    snapshot = adopted_snapshot
                    candidate_windows = snapshot.get("candidate_windows", []) if isinstance(snapshot.get("candidate_windows", []), list) else []
                    active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
                    target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
                    target_visible = bool(target_window) or any(isinstance(row, dict) for row in candidate_windows)
                    active_matches = self._window_matches(active_window, app_name=app_name, window_title=adopted_title)
        if not target_visible and not active_matches:
            flags = snapshot.get("surface_flags", {}) if isinstance(snapshot.get("surface_flags", {}), dict) else {}
            snapshot["surface_flags"] = {
                **flags,
                "wizard_surface_visible": False,
                "wizard_next_available": False,
                "wizard_back_available": False,
                "wizard_finish_available": False,
            }
            snapshot["wizard_page_state"] = {}
        return snapshot

    @staticmethod
    def _wizard_flow_summary(*, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        flags = snapshot.get("surface_flags", {}) if isinstance(snapshot.get("surface_flags", {}), dict) else {}
        page_state = snapshot.get("wizard_page_state", {}) if isinstance(snapshot.get("wizard_page_state", {}), dict) else {}
        observation = snapshot.get("observation", {}) if isinstance(snapshot.get("observation", {}), dict) else {}
        safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
        active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
        adopted_window = snapshot.get("adopted_wizard_window", {}) if isinstance(snapshot.get("adopted_wizard_window", {}), dict) else {}
        return {
            "wizard_visible": bool(flags.get("wizard_surface_visible", False)),
            "dialog_visible": bool(flags.get("dialog_visible", False)),
            "page_kind": str(page_state.get("page_kind", "") or "").strip(),
            "advance_action": str(page_state.get("advance_action", "") or "").strip(),
            "ready_for_advance": bool(page_state.get("ready_for_advance", False)),
            "pending_requirement_count": int(page_state.get("pending_requirement_count", 0) or 0),
            "preferred_confirmation_button": str(page_state.get("preferred_confirmation_button", "") or "").strip(),
            "preferred_dialog_confirmation_button": str(safety_signals.get("preferred_confirmation_button", "") or "").strip(),
            "preferred_dialog_dismiss_button": str(safety_signals.get("preferred_dismiss_button", "") or "").strip(),
            "window_title": str(adopted_window.get("title", "") or target_window.get("title", "") or active_window.get("title", "") or "").strip(),
            "window_hwnd": int(adopted_window.get("hwnd", 0) or target_window.get("hwnd", 0) or active_window.get("hwnd", 0) or 0),
            "window_adopted": bool(adopted_window),
            "autonomous_progress_supported": bool(page_state.get("autonomous_progress_supported", False)),
            "autonomous_blocker": str(page_state.get("autonomous_blocker", "") or "").strip(),
            "manual_input_likely": bool(page_state.get("manual_input_likely", False)),
            "warning_surface_visible": bool(safety_signals.get("warning_surface_visible", False)),
            "destructive_warning_visible": bool(safety_signals.get("destructive_warning_visible", False)),
            "dialog_kind": str(dialog_state.get("dialog_kind", "") or "").strip(),
            "approval_kind": str(dialog_state.get("approval_kind", "") or "").strip(),
            "dialog_review_required": bool(dialog_state.get("review_required", False)),
            "dialog_auto_resolve_supported": bool(dialog_state.get("auto_resolve_supported", False)),
            "secure_desktop_likely": bool(dialog_state.get("secure_desktop_likely", False)),
            "credential_field_count": int(dialog_state.get("credential_field_count", 0) or 0),
            "screen_hash": str(observation.get("screen_hash", "") or "").strip(),
        }

    @classmethod
    def _wizard_flow_signature(cls, *, snapshot: Dict[str, Any]) -> tuple[str, str, int, str, bool, str, str, str, int, bool, bool, str, str, bool, bool, bool, int]:
        summary = cls._wizard_flow_summary(snapshot=snapshot)
        return (
            str(summary.get("screen_hash", "") or "").strip(),
            str(summary.get("page_kind", "") or "").strip(),
            int(summary.get("pending_requirement_count", 0) or 0),
            str(summary.get("preferred_confirmation_button", "") or "").strip().lower(),
            bool(summary.get("dialog_visible", False)),
            str(summary.get("preferred_dialog_confirmation_button", "") or "").strip().lower(),
            str(summary.get("preferred_dialog_dismiss_button", "") or "").strip().lower(),
            str(summary.get("window_title", "") or "").strip().lower(),
            int(summary.get("window_hwnd", 0) or 0),
            bool(summary.get("warning_surface_visible", False)),
            bool(summary.get("destructive_warning_visible", False)),
            str(summary.get("dialog_kind", "") or "").strip().lower(),
            str(summary.get("approval_kind", "") or "").strip().lower(),
            bool(summary.get("dialog_review_required", False)),
            bool(summary.get("dialog_auto_resolve_supported", False)),
            bool(summary.get("secure_desktop_likely", False)),
            int(summary.get("credential_field_count", 0) or 0),
        )

    @classmethod
    def _wizard_flow_progressed(cls, *, before_snapshot: Dict[str, Any], after_snapshot: Dict[str, Any]) -> bool:
        before_summary = cls._wizard_flow_summary(snapshot=before_snapshot)
        after_summary = cls._wizard_flow_summary(snapshot=after_snapshot)
        if bool(before_summary.get("wizard_visible", False)) and not bool(after_summary.get("wizard_visible", False)):
            return True
        return cls._wizard_flow_signature(snapshot=before_snapshot) != cls._wizard_flow_signature(snapshot=after_snapshot)

    @staticmethod
    def _wizard_flow_gate(
        *,
        page_state: Dict[str, Any],
        safety_signals: Dict[str, Any],
        allow_warning_pages: bool,
    ) -> Dict[str, Any]:
        page_kind = str(page_state.get("page_kind", "") or "").strip().lower()
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        approval_kind = str(dialog_state.get("approval_kind", "") or "").strip().lower()
        secure_desktop_likely = bool(dialog_state.get("secure_desktop_likely", False))
        if approval_kind == "elevation_credentials":
            return {
                "allowed": False,
                "code": "elevation_credentials_required",
                "message": "Wizard mission stopped because the current setup step raised an administrator credential prompt that requires manual approval and input.",
            }
        if approval_kind == "elevation_consent" and page_kind not in {"ready_to_install", "completion"}:
            return {
                "allowed": False,
                "code": "elevation_consent_required",
                "message": "Wizard mission stopped because the current setup step raised an elevation approval prompt that requires explicit confirmation."
                + (" The prompt also appears to be on a secure desktop surface." if secure_desktop_likely else ""),
            }
        if approval_kind == "permission_review":
            return {
                "allowed": False,
                "code": "permission_review_required",
                "message": "Wizard mission paused because the current setup step is asking for a permission or consent review that JARVIS should not approve blindly.",
            }
        blocker = str(page_state.get("autonomous_blocker", "") or "").strip()
        if blocker == "warning_confirmation_requires_review" and allow_warning_pages:
            return {"allowed": True, "code": "", "message": ""}
        blocker_messages = {
            "warning_confirmation_requires_review": "Wizard mission paused on a warning-confirmation page so JARVIS does not auto-commit a risky step without review.",
            "unsupported_wizard_requirements": "Wizard mission found page requirements that are not yet safe to auto-resolve, so this setup step needs manual review.",
            "manual_input_required": "Wizard mission paused because the current setup page appears to require manual text or dropdown input before it can continue.",
            "credential_input_required": "Wizard mission paused because the current setup surface appears to require credentials or sign-in input before it can continue.",
            "authentication_review_required": "Wizard mission paused because the current setup surface is asking for authentication review or identity confirmation.",
            "permission_review_required": "Wizard mission paused because the current setup surface is asking for a permission or consent review before it can continue.",
            "elevation_consent_required": "Wizard mission paused because the current setup surface raised an elevation approval prompt that needs explicit confirmation.",
            "elevation_credentials_required": "Wizard mission paused because the current setup surface raised an administrator credential prompt that needs manual approval and input.",
            "elevation_prompt_requires_approval": "Wizard mission paused because the current setup surface raised an elevation prompt that needs explicit approval.",
            "no_advance_control_available": "Wizard mission paused because the current setup page exposes no safe advance control yet.",
        }
        if blocker:
            return {
                "allowed": False,
                "code": blocker,
                "message": blocker_messages.get(blocker, "Wizard mission paused because the current setup page is not safe for autonomous progression."),
            }
        return {"allowed": True, "code": "", "message": ""}

    @staticmethod
    def _verify_wizard_flow_execution(
        *,
        args: Dict[str, Any],
        pre_context: Dict[str, Any],
        post_context: Dict[str, Any],
        completed: bool,
        pages_completed: int,
        max_pages: int,
        stop_reason_code: str,
        stop_reason: str,
        final_summary: Dict[str, Any],
        warnings: List[str],
    ) -> Dict[str, Any]:
        checks = [
            {
                "name": "wizard_pages_completed",
                "passed": pages_completed > 0 or completed,
                "pages_completed": pages_completed,
                "max_pages": max_pages,
            },
            {
                "name": "wizard_surface_closed",
                "passed": completed,
                "wizard_visible_after": bool(final_summary.get("wizard_visible", False)),
                "final_page_kind": str(final_summary.get("page_kind", "") or "").strip(),
            },
        ]
        if stop_reason_code:
            checks.append(
                {
                    "name": "wizard_safe_stop",
                    "passed": False,
                    "reason_code": stop_reason_code,
                    "reason": stop_reason,
                }
            )
        verified = completed
        message = "wizard flow completed" if verified else (stop_reason or "wizard flow stopped before completion")
        status = "degraded" if verified and warnings else ("success" if verified else "failed")
        return {
            "enabled": True,
            "status": status,
            "verified": verified,
            "message": message,
            "checks": checks,
            "warnings": DesktopActionRouter._dedupe_strings(warnings),
            "pre_context": {
                "active_window": pre_context.get("active_window", {}) if isinstance(pre_context.get("active_window", {}), dict) else {},
                "screen_hash": str((pre_context.get("observation", {}) if isinstance(pre_context.get("observation", {}), dict) else {}).get("screen_hash", "") or "").strip(),
            },
            "post_context": {
                "active_window": post_context.get("active_window", {}) if isinstance(post_context.get("active_window", {}), dict) else {},
                "screen_hash": str((post_context.get("observation", {}) if isinstance(post_context.get("observation", {}), dict) else {}).get("screen_hash", "") or "").strip(),
            },
            "verify_text": str(args.get("verify_text", "") or "").strip() or "setup complete",
        }

    def _execute_form_flow_strategy(
        self,
        *,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        strategy: Dict[str, Any],
        attempt_index: int,
    ) -> Dict[str, Any]:
        results: List[Dict[str, Any]] = []
        page_history: List[Dict[str, Any]] = []
        mission_warnings: List[str] = []
        status = "success"
        message = ""
        stop_reason_code = ""
        stop_reason = ""
        completed = False
        pages_completed = 0
        max_pages = max(1, min(int(args.get("max_form_pages", 5) or 5), 10))
        allow_destructive_forms = bool(args.get("allow_destructive_forms", False))
        pre_context = self._capture_verification_context(args=args, advice=advice)
        advice_target_window = advice.get("target_window", {}) if isinstance(advice.get("target_window", {}), dict) else {}
        form_anchor_title = str(args.get("window_title", "") or advice_target_window.get("title", "") or "").strip()
        form_anchor_app_name = str(args.get("app_name", "") or "").strip()
        requested_form_targets = self._normalize_form_target_plan(args.get("form_target_plan", []))
        remaining_form_targets = [dict(row) for row in requested_form_targets]
        resolved_form_targets: List[Dict[str, Any]] = []
        visited_form_tabs: set[str] = set()
        visited_form_navigation_targets: set[str] = set()
        visited_form_drilldown_targets: set[str] = set()
        visited_form_expandable_groups: set[str] = set()
        form_scroll_hunts_used = 0
        max_form_scroll_hunts = max(1, min(max_pages, 4))
        form_window_hint = str(args.get("window_title", "") or "").strip()
        form_window_locked = False

        def _remember_form_window(summary: Dict[str, Any]) -> None:
            nonlocal form_window_hint, form_window_locked
            if not isinstance(summary, dict):
                return
            hinted_title = str(summary.get("window_title", "") or "").strip()
            if hinted_title:
                form_window_hint = hinted_title
            if bool(summary.get("window_adopted", False)):
                form_window_locked = True

        def _record_resolved_targets(targets: List[Dict[str, Any]]) -> None:
            seen_keys = {self._form_target_key(row) for row in resolved_form_targets if isinstance(row, dict)}
            for row in targets:
                if not isinstance(row, dict):
                    continue
                clean = {
                    "action": str(row.get("action", "") or "").strip(),
                    "query": str(row.get("query", "") or "").strip(),
                    "text": str(row.get("text", "") or "").strip(),
                    "family": str(row.get("family", "") or "").strip(),
                }
                target_key = self._form_target_key(clean)
                if not target_key or target_key in seen_keys:
                    continue
                seen_keys.add(target_key)
                resolved_form_targets.append(clean)

        def _remove_resolved_targets(targets: List[Dict[str, Any]]) -> None:
            nonlocal remaining_form_targets
            resolved_keys = {
                self._form_target_key(row)
                for row in targets
                if isinstance(row, dict) and self._form_target_key(row)
            }
            if not resolved_keys:
                return
            remaining_form_targets = [
                row
                for row in remaining_form_targets
                if self._form_target_key(row) not in resolved_keys
            ]

        bootstrap_plan = [dict(step) for step in advice.get("execution_plan", []) if isinstance(step, dict)]
        if bootstrap_plan:
            bootstrap_payload = self._run_execution_plan(
                plan=bootstrap_plan,
                result_metadata={"form_stage": "bootstrap"},
            )
            results.extend(bootstrap_payload.get("results", []) if isinstance(bootstrap_payload.get("results", []), list) else [])
            if str(bootstrap_payload.get("status", "success") or "success") != "success":
                message = str(bootstrap_payload.get("message", "form bootstrap failed") or "form bootstrap failed")
                status = "error"
                verification = {
                    "enabled": True,
                    "status": "failed",
                    "verified": False,
                    "message": message,
                    "checks": [],
                }
                return {
                    "attempt": attempt_index,
                    "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
                    "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
                    "strategy_reason": str(strategy.get("reason", "") or "").strip(),
                    "payload": self._sanitize_payload_for_response(args),
                    "status": status,
                    "message": message,
                    "final_action": str(bootstrap_payload.get("final_action", "") or advice.get("action", "")),
                    "results": results,
                    "advice": advice,
                    "verification": verification,
                    "form_mission": {
                        "enabled": True,
                        "completed": False,
                        "pages_completed": 0,
                        "page_count": 0,
                        "max_pages": max_pages,
                        "allow_destructive_forms": allow_destructive_forms,
                        "stop_reason_code": "form_bootstrap_failed",
                        "stop_reason": message,
                        "page_history": [],
                    },
                }

        last_snapshot = self._form_flow_snapshot(args=args, advice=advice)
        for page_index in range(1, max_pages + 1):
            page_args = dict(args)
            if form_window_hint:
                page_args["window_title"] = form_window_hint
            if form_window_locked and form_window_hint:
                page_args["app_name"] = ""
                page_args["focus_first"] = True
            page_args["action"] = "complete_form_page"
            if remaining_form_targets:
                page_args["form_target_plan"] = [dict(row) for row in remaining_form_targets]
            page_args["_provided_fields"] = self._dedupe_strings(
                list(page_args.get("_provided_fields", [])) + ["action", *([] if not remaining_form_targets else ["form_target_plan"])]
            )
            page_advice = self.advise(page_args)
            page_snapshot = self._form_flow_snapshot(args=page_args, advice=page_advice)
            page_advice["surface_snapshot"] = page_snapshot
            page_state = page_snapshot.get("form_page_state", {}) if isinstance(page_snapshot.get("form_page_state", {}), dict) else {}
            safety_signals = page_snapshot.get("safety_signals", {}) if isinstance(page_snapshot.get("safety_signals", {}), dict) else {}
            surface_flags = page_snapshot.get("surface_flags", {}) if isinstance(page_snapshot.get("surface_flags", {}), dict) else {}
            raw_page_target_state = page_advice.get("form_target_state", {}) if isinstance(page_advice.get("form_target_state", {}), dict) else {}
            live_page_target_state = self._form_target_plan_state(plan=remaining_form_targets, snapshot=page_snapshot)
            page_target_state = {
                **raw_page_target_state,
                **live_page_target_state,
            } if raw_page_target_state or live_page_target_state else {}
            if page_target_state:
                page_advice["form_target_state"] = page_target_state
            before_summary = self._form_flow_summary(snapshot=page_snapshot)
            _remember_form_window(before_summary)
            selected_tab = str(page_state.get("selected_tab", "") or "").strip()
            if selected_tab:
                visited_form_tabs.add(self._normalize_probe_text(selected_tab))
            selected_navigation_target = str(page_state.get("selected_navigation_target", "") or "").strip()
            if selected_navigation_target:
                visited_form_navigation_targets.add(self._normalize_probe_text(selected_navigation_target))
            page_record: Dict[str, Any] = {
                "page_index": page_index,
                "before": before_summary,
                "warnings": [str(item).strip() for item in page_advice.get("warnings", []) if str(item).strip()],
                "recommended_actions": [str(item).strip() for item in page_snapshot.get("recommended_actions", []) if str(item).strip()] if isinstance(page_snapshot.get("recommended_actions", []), list) else [],
            }
            if page_target_state:
                page_record["target_state_before"] = page_target_state
                resolved_before = page_target_state.get("resolved_targets", []) if isinstance(page_target_state.get("resolved_targets", []), list) else []
                _record_resolved_targets(resolved_before)
                _remove_resolved_targets(resolved_before)

            current_form_surface_visible = bool(
                surface_flags.get("form_visible", False)
                or surface_flags.get("dialog_visible", False)
                or surface_flags.get("tab_page_visible", False)
                or page_state
            )
            if not current_form_surface_visible:
                if page_index == 1 and not results:
                    stop_reason_code = "form_not_visible"
                    stop_reason = "No active settings or dialog form surface could be detected after focusing the requested app."
                    page_record["status"] = "blocked"
                    page_record["stop_reason_code"] = stop_reason_code
                    page_record["stop_reason"] = stop_reason
                    page_history.append(page_record)
                else:
                    completed = True
                    message = "form flow completed and the settings surface closed"
                break

            dialog_followup = self._resolve_form_dialog_interstitial(
                args=page_args,
                snapshot=page_snapshot,
                page_index=page_index,
            )
            if bool(dialog_followup.get("handled", False)):
                dialog_execution = dialog_followup.get("execution", {}) if isinstance(dialog_followup.get("execution", {}), dict) else {}
                dialog_after_snapshot = dialog_followup.get("after_snapshot", {}) if isinstance(dialog_followup.get("after_snapshot", {}), dict) else page_snapshot
                dialog_after_summary = dialog_followup.get("after_summary", {}) if isinstance(dialog_followup.get("after_summary", {}), dict) else before_summary
                _remember_form_window(dialog_after_summary)
                results.extend(dialog_execution.get("results", []) if isinstance(dialog_execution.get("results", []), list) else [])
                dialog_after_target_state = self._form_target_plan_state(plan=remaining_form_targets, snapshot=dialog_after_snapshot)
                if dialog_after_target_state:
                    page_record["target_state_after"] = dialog_after_target_state
                    resolved_after = dialog_after_target_state.get("resolved_targets", []) if isinstance(dialog_after_target_state.get("resolved_targets", []), list) else []
                    _record_resolved_targets(resolved_after)
                    _remove_resolved_targets(resolved_after)
                page_record["after"] = dialog_after_summary
                page_record["status"] = str(dialog_followup.get("status", "dialog_confirmed") or "dialog_confirmed")
                page_record["message"] = str(dialog_followup.get("message", "") or "")
                page_record["progressed"] = bool(dialog_followup.get("progressed", False))
                page_record["dialog_followup"] = {
                    "action": str(dialog_followup.get("action", "") or "").strip(),
                    "button_label": str(dialog_followup.get("button_label", "") or "").strip(),
                    "button_role": str(dialog_followup.get("button_role", "") or "").strip(),
                    "route_mode": str(dialog_followup.get("route_mode", "") or "").strip(),
                    "dialog_kind": str(dialog_followup.get("dialog_kind", "") or "").strip(),
                    "approval_kind": str(dialog_followup.get("approval_kind", "") or "").strip(),
                    "secure_desktop_likely": bool(dialog_followup.get("secure_desktop_likely", False)),
                }
                page_record["executed_actions"] = [
                    str(row.get("action", "") or "").strip()
                    for row in dialog_execution.get("results", [])
                    if isinstance(row, dict) and str(row.get("action", "") or "").strip()
                ]
                page_history.append(page_record)
                mission_warnings.extend([str(item).strip() for item in dialog_followup.get("warnings", []) if str(item).strip()])
                if bool(dialog_followup.get("progressed", False)) or not bool(dialog_after_summary.get("form_visible", False)):
                    pages_completed += 1
                last_snapshot = dialog_after_snapshot
                if not bool(dialog_after_summary.get("form_visible", False)):
                    completed = True
                    message = "form flow completed and the settings surface closed"
                    break
                continue
            if bool(dialog_followup.get("blocked", False)):
                stop_reason_code = str(dialog_followup.get("stop_reason_code", "") or "form_dialog_review_required")
                stop_reason = str(dialog_followup.get("stop_reason", "") or "form mission paused on an interstitial dialog that requires review")
                page_record["status"] = "blocked"
                page_record["stop_reason_code"] = stop_reason_code
                page_record["stop_reason"] = stop_reason
                page_record["blocking_surface"] = self._blocking_surface_state(
                    snapshot=page_snapshot,
                    stop_reason_code=stop_reason_code,
                    mission_kind="form",
                )
                page_record["dialog_followup"] = {
                    "blocked": True,
                    "button_label": str(dialog_followup.get("button_label", "") or "").strip(),
                    "button_role": str(dialog_followup.get("button_role", "") or "").strip(),
                    "dialog_kind": str(dialog_followup.get("dialog_kind", "") or "").strip(),
                    "approval_kind": str(dialog_followup.get("approval_kind", "") or "").strip(),
                    "secure_desktop_likely": bool(dialog_followup.get("secure_desktop_likely", False)),
                }
                if stop_reason:
                    mission_warnings.append(stop_reason)
                page_history.append(page_record)
                last_snapshot = page_snapshot
                break

            remaining_target_count = int(page_target_state.get("remaining_count", 0) or 0) if isinstance(page_target_state, dict) else 0
            visible_pending_target_count = int(page_target_state.get("visible_pending_count", 0) or 0) if isinstance(page_target_state, dict) else 0
            hidden_requested_targets = bool(
                remaining_form_targets
                and remaining_target_count > 0
                and visible_pending_target_count <= 0
            )
            tab_candidates = self._rank_form_target_tabs(
                page_state=page_state,
                remaining_targets=remaining_form_targets,
                visited_tabs=visited_form_tabs,
            )
            tab_search_supported = int(page_state.get("tab_count", 0) or 0) > 1
            tab_hunt_record: Optional[Dict[str, Any]] = None
            reveal_progressed = False
            if hidden_requested_targets and tab_search_supported:
                tab_hunt_record = {
                    "reason": "requested_targets_not_visible_on_current_tab",
                    "selected_tab": selected_tab,
                    "candidate_tabs": [
                        {
                            "name": str(row.get("name", "") or "").strip(),
                            "match_score": float(row.get("match_score", 0.0) or 0.0),
                            "fallback_candidate": bool(row.get("fallback_candidate", False)),
                            "matched_targets": [dict(item) for item in row.get("matched_targets", []) if isinstance(item, dict)][:4],
                        }
                        for row in tab_candidates[:6]
                    ],
                    "attempts": [],
                }
                for tab_candidate in tab_candidates[:4]:
                    tab_name = str(tab_candidate.get("name", "") or "").strip()
                    normalized_tab_name = self._normalize_probe_text(tab_name)
                    if not tab_name:
                        continue
                    if normalized_tab_name:
                        visited_form_tabs.add(normalized_tab_name)
                    tab_args: Dict[str, Any] = {
                        "query": tab_name,
                        "control_type": "TabItem",
                    }
                    tab_element_id = str(tab_candidate.get("element_id", "") or "").strip()
                    if tab_element_id:
                        tab_args["element_id"] = tab_element_id
                    tab_execution = self._run_execution_plan(
                        plan=[
                            self._plan_step(
                                action="accessibility_invoke_element",
                                args=tab_args,
                                phase="act",
                                optional=False,
                                reason=f"Switch to the '{tab_name}' tab so JARVIS can keep hunting for the remaining requested settings targets.",
                            )
                        ],
                        result_metadata={
                            "form_stage": "tab_hunt",
                            "form_page_index": page_index,
                            "form_page_kind": str(page_state.get("page_kind", "") or ""),
                            "form_tab": tab_name,
                        },
                    )
                    results.extend(tab_execution.get("results", []) if isinstance(tab_execution.get("results", []), list) else [])
                    after_tab_snapshot = self._form_flow_snapshot(args=page_args, advice=page_advice)
                    after_tab_summary = self._form_flow_summary(snapshot=after_tab_snapshot)
                    _remember_form_window(after_tab_summary)
                    after_target_state = self._form_target_plan_state(plan=remaining_form_targets, snapshot=after_tab_snapshot)
                    progressed = self._form_flow_progressed(before_snapshot=page_snapshot, after_snapshot=after_tab_snapshot)
                    tab_attempt = {
                        "tab_name": tab_name,
                        "match_score": float(tab_candidate.get("match_score", 0.0) or 0.0),
                        "fallback_candidate": bool(tab_candidate.get("fallback_candidate", False)),
                        "matched_targets": [dict(item) for item in tab_candidate.get("matched_targets", []) if isinstance(item, dict)][:4],
                        "status": str(tab_execution.get("status", "success") or "success"),
                        "message": str(tab_execution.get("message", "") or ""),
                        "progressed": progressed,
                        "after": after_tab_summary,
                    }
                    if after_target_state:
                        tab_attempt["target_state_after"] = after_target_state
                        resolved_after = after_target_state.get("resolved_targets", []) if isinstance(after_target_state.get("resolved_targets", []), list) else []
                        _record_resolved_targets(resolved_after)
                        _remove_resolved_targets(resolved_after)
                    tab_hunt_record["attempts"].append(tab_attempt)
                    if str(tab_execution.get("status", "success") or "success") == "success" and progressed:
                        page_record["tab_hunt"] = tab_hunt_record
                        page_record["after"] = after_tab_summary
                        page_record["status"] = "tab_switched"
                        page_record["message"] = f"Switched to the '{tab_name}' tab to continue resolving the requested settings targets."
                        page_record["progressed"] = True
                        page_record["executed_actions"] = [
                            str(row.get("action", "") or "").strip()
                            for row in tab_execution.get("results", [])
                            if isinstance(row, dict) and str(row.get("action", "") or "").strip()
                        ]
                        if after_target_state:
                            page_record["target_state_after"] = after_target_state
                        page_history.append(page_record)
                        last_snapshot = after_tab_snapshot
                        reveal_progressed = True
                        break
            if reveal_progressed:
                continue

            navigation_target_count = int(page_state.get("navigation_target_count", 0) or 0)
            navigation_candidates = self._rank_form_navigation_targets(
                page_state=page_state,
                remaining_targets=remaining_form_targets,
                visited_targets=visited_form_navigation_targets,
            )
            navigation_hunt_record: Optional[Dict[str, Any]] = None
            if hidden_requested_targets and navigation_target_count > 0:
                navigation_hunt_record = {
                    "reason": "requested_targets_not_visible_in_current_section",
                    "selected_navigation_target": selected_navigation_target,
                    "candidate_targets": [
                        {
                            "name": str(row.get("name", "") or "").strip(),
                            "navigation_action": str(row.get("navigation_action", "") or "").strip(),
                            "navigation_role": str(row.get("navigation_role", "") or "").strip(),
                            "match_score": float(row.get("match_score", 0.0) or 0.0),
                            "matched_targets": [dict(item) for item in row.get("matched_targets", []) if isinstance(item, dict)][:4],
                        }
                        for row in navigation_candidates[:6]
                    ],
                    "attempts": [],
                }
                for navigation_candidate in navigation_candidates[:4]:
                    candidate_name = str(navigation_candidate.get("name", "") or "").strip()
                    normalized_candidate_name = self._normalize_probe_text(candidate_name)
                    navigation_action = str(navigation_candidate.get("navigation_action", "") or "").strip().lower()
                    if not candidate_name or not navigation_action:
                        continue
                    if normalized_candidate_name:
                        visited_form_navigation_targets.add(normalized_candidate_name)
                    navigation_args: Dict[str, Any] = {"query": candidate_name}
                    control_type = str(navigation_candidate.get("control_type", "") or "").strip()
                    if navigation_action == "select_tree_item":
                        navigation_args["control_type"] = "TreeItem"
                    elif navigation_action == "select_list_item":
                        navigation_args["control_type"] = "ListItem"
                    elif control_type:
                        navigation_args["control_type"] = control_type
                    candidate_element_id = str(navigation_candidate.get("element_id", "") or "").strip()
                    if candidate_element_id:
                        navigation_args["element_id"] = candidate_element_id
                    navigation_execution = self._run_execution_plan(
                        plan=[
                            self._plan_step(
                                action="accessibility_invoke_element",
                                args=navigation_args,
                                phase="act",
                                optional=False,
                                reason=f"Switch to the '{candidate_name}' section so JARVIS can keep hunting for the remaining requested settings targets.",
                            )
                        ],
                        result_metadata={
                            "form_stage": "navigation_hunt",
                            "form_page_index": page_index,
                            "form_page_kind": str(page_state.get("page_kind", "") or ""),
                            "form_navigation_action": navigation_action,
                            "form_navigation_target": candidate_name,
                        },
                    )
                    results.extend(navigation_execution.get("results", []) if isinstance(navigation_execution.get("results", []), list) else [])
                    after_navigation_snapshot = self._form_flow_snapshot(args=page_args, advice=page_advice)
                    after_navigation_summary = self._form_flow_summary(snapshot=after_navigation_snapshot)
                    _remember_form_window(after_navigation_summary)
                    after_navigation_target_state = self._form_target_plan_state(plan=remaining_form_targets, snapshot=after_navigation_snapshot)
                    progressed = self._form_flow_progressed(before_snapshot=page_snapshot, after_snapshot=after_navigation_snapshot)
                    navigation_attempt = {
                        "name": candidate_name,
                        "navigation_action": navigation_action,
                        "navigation_role": str(navigation_candidate.get("navigation_role", "") or "").strip(),
                        "match_score": float(navigation_candidate.get("match_score", 0.0) or 0.0),
                        "matched_targets": [dict(item) for item in navigation_candidate.get("matched_targets", []) if isinstance(item, dict)][:4],
                        "status": str(navigation_execution.get("status", "success") or "success"),
                        "message": str(navigation_execution.get("message", "") or ""),
                        "progressed": progressed,
                        "after": after_navigation_summary,
                    }
                    if after_navigation_target_state:
                        navigation_attempt["target_state_after"] = after_navigation_target_state
                        resolved_after = after_navigation_target_state.get("resolved_targets", []) if isinstance(after_navigation_target_state.get("resolved_targets", []), list) else []
                        _record_resolved_targets(resolved_after)
                        _remove_resolved_targets(resolved_after)
                    navigation_hunt_record["attempts"].append(navigation_attempt)
                    if str(navigation_execution.get("status", "success") or "success") == "success" and progressed:
                        page_record["navigation_hunt"] = navigation_hunt_record
                        page_record["after"] = after_navigation_summary
                        page_record["status"] = "navigation_switched"
                        page_record["message"] = f"Switched to the '{candidate_name}' section to continue resolving the requested settings targets."
                        page_record["progressed"] = True
                        page_record["executed_actions"] = [
                            str(row.get("action", "") or "").strip()
                            for row in navigation_execution.get("results", [])
                            if isinstance(row, dict) and str(row.get("action", "") or "").strip()
                        ]
                        if after_navigation_target_state:
                            page_record["target_state_after"] = after_navigation_target_state
                        page_history.append(page_record)
                        last_snapshot = after_navigation_snapshot
                        reveal_progressed = True
                        break
            if reveal_progressed:
                continue

            drilldown_target_count = int(page_state.get("drilldown_target_count", 0) or 0)
            drilldown_candidates = self._rank_form_drilldown_targets(
                page_state=page_state,
                remaining_targets=remaining_form_targets,
                visited_targets=visited_form_drilldown_targets,
            )
            drilldown_hunt_record: Optional[Dict[str, Any]] = None
            if hidden_requested_targets and drilldown_target_count > 0:
                drilldown_hunt_record = {
                    "reason": "requested_targets_hidden_on_child_surface",
                    "breadcrumb_path": [str(item).strip() for item in page_state.get("breadcrumb_path", []) if str(item).strip()] if isinstance(page_state.get("breadcrumb_path", []), list) else [],
                    "candidate_targets": [
                        {
                            "name": str(row.get("name", "") or "").strip(),
                            "drilldown_action": str(row.get("drilldown_action", "") or "").strip(),
                            "invoke_action": str(row.get("invoke_action", "") or "").strip(),
                            "match_score": float(row.get("match_score", 0.0) or 0.0),
                            "fallback_candidate": bool(row.get("fallback_candidate", False)),
                            "matched_targets": [dict(item) for item in row.get("matched_targets", []) if isinstance(item, dict)][:4],
                        }
                        for row in drilldown_candidates[:6]
                    ],
                    "attempts": [],
                }
                for drilldown_candidate in drilldown_candidates[:4]:
                    candidate_name = str(drilldown_candidate.get("name", "") or "").strip()
                    normalized_candidate_name = self._normalize_probe_text(candidate_name)
                    invoke_action = str(drilldown_candidate.get("invoke_action", "") or "").strip().lower() or "click"
                    if not candidate_name:
                        continue
                    if normalized_candidate_name:
                        visited_form_drilldown_targets.add(normalized_candidate_name)
                    drilldown_args: Dict[str, Any] = {"query": candidate_name}
                    control_type = str(drilldown_candidate.get("control_type", "") or "").strip()
                    if control_type:
                        drilldown_args["control_type"] = control_type
                    if invoke_action and invoke_action != "click":
                        drilldown_args["action"] = invoke_action
                    candidate_element_id = str(drilldown_candidate.get("element_id", "") or "").strip()
                    if candidate_element_id:
                        drilldown_args["element_id"] = candidate_element_id
                    drilldown_execution = self._run_execution_plan(
                        plan=[
                            self._plan_step(
                                action="accessibility_invoke_element",
                                args=drilldown_args,
                                phase="act",
                                optional=False,
                                reason=f"Open the '{candidate_name}' child surface so JARVIS can keep hunting for the remaining requested settings targets.",
                            )
                        ],
                        result_metadata={
                            "form_stage": "drilldown_hunt",
                            "form_page_index": page_index,
                            "form_page_kind": str(page_state.get("page_kind", "") or ""),
                            "form_drilldown_target": candidate_name,
                        },
                    )
                    results.extend(drilldown_execution.get("results", []) if isinstance(drilldown_execution.get("results", []), list) else [])
                    after_drilldown_snapshot = self._form_flow_snapshot(args=page_args, advice=page_advice)
                    after_drilldown_summary = self._form_flow_summary(snapshot=after_drilldown_snapshot)
                    _remember_form_window(after_drilldown_summary)
                    after_drilldown_target_state = self._form_target_plan_state(plan=remaining_form_targets, snapshot=after_drilldown_snapshot)
                    progressed = self._form_flow_progressed(before_snapshot=page_snapshot, after_snapshot=after_drilldown_snapshot)
                    drilldown_attempt = {
                        "name": candidate_name,
                        "invoke_action": invoke_action,
                        "match_score": float(drilldown_candidate.get("match_score", 0.0) or 0.0),
                        "fallback_candidate": bool(drilldown_candidate.get("fallback_candidate", False)),
                        "matched_targets": [dict(item) for item in drilldown_candidate.get("matched_targets", []) if isinstance(item, dict)][:4],
                        "status": str(drilldown_execution.get("status", "success") or "success"),
                        "message": str(drilldown_execution.get("message", "") or ""),
                        "progressed": progressed,
                        "after": after_drilldown_summary,
                    }
                    if after_drilldown_target_state:
                        drilldown_attempt["target_state_after"] = after_drilldown_target_state
                        resolved_after = after_drilldown_target_state.get("resolved_targets", []) if isinstance(after_drilldown_target_state.get("resolved_targets", []), list) else []
                        _record_resolved_targets(resolved_after)
                        _remove_resolved_targets(resolved_after)
                    drilldown_hunt_record["attempts"].append(drilldown_attempt)
                    if str(drilldown_execution.get("status", "success") or "success") == "success" and progressed:
                        page_record["drilldown_hunt"] = drilldown_hunt_record
                        page_record["after"] = after_drilldown_summary
                        page_record["status"] = "drilldown_opened"
                        page_record["message"] = f"Opened the '{candidate_name}' child surface to continue resolving the requested settings targets."
                        page_record["progressed"] = True
                        page_record["executed_actions"] = [
                            str(row.get("action", "") or "").strip()
                            for row in drilldown_execution.get("results", [])
                            if isinstance(row, dict) and str(row.get("action", "") or "").strip()
                        ]
                        if after_drilldown_target_state:
                            page_record["target_state_after"] = after_drilldown_target_state
                        page_history.append(page_record)
                        last_snapshot = after_drilldown_snapshot
                        reveal_progressed = True
                        break
            if reveal_progressed:
                continue

            expandable_group_count = int(page_state.get("expandable_group_count", 0) or 0)
            expandable_group_candidates = self._rank_form_expandable_groups(
                page_state=page_state,
                remaining_targets=remaining_form_targets,
                visited_groups=visited_form_expandable_groups,
            )
            group_hunt_record: Optional[Dict[str, Any]] = None
            if hidden_requested_targets and expandable_group_count > 0:
                group_hunt_record = {
                    "reason": "requested_targets_hidden_inside_collapsed_group",
                    "candidate_groups": [
                        {
                            "name": str(row.get("name", "") or "").strip(),
                            "expand_action": str(row.get("expand_action", "") or "").strip(),
                            "invoke_action": str(row.get("invoke_action", "") or "").strip(),
                            "match_score": float(row.get("match_score", 0.0) or 0.0),
                            "fallback_candidate": bool(row.get("fallback_candidate", False)),
                            "matched_targets": [dict(item) for item in row.get("matched_targets", []) if isinstance(item, dict)][:4],
                        }
                        for row in expandable_group_candidates[:6]
                    ],
                    "attempts": [],
                }
                for group_candidate in expandable_group_candidates[:4]:
                    candidate_name = str(group_candidate.get("name", "") or "").strip()
                    normalized_candidate_name = self._normalize_probe_text(candidate_name)
                    expand_action = str(group_candidate.get("expand_action", "") or "").strip().lower()
                    invoke_action = str(group_candidate.get("invoke_action", "") or "").strip().lower() or "click"
                    if not candidate_name or not expand_action:
                        continue
                    if normalized_candidate_name:
                        visited_form_expandable_groups.add(normalized_candidate_name)
                    group_args: Dict[str, Any] = {"query": candidate_name}
                    control_type = str(group_candidate.get("control_type", "") or "").strip()
                    if control_type:
                        group_args["control_type"] = control_type
                    if invoke_action and invoke_action != "click":
                        group_args["action"] = invoke_action
                    candidate_element_id = str(group_candidate.get("element_id", "") or "").strip()
                    if candidate_element_id:
                        group_args["element_id"] = candidate_element_id
                    group_execution = self._run_execution_plan(
                        plan=[
                            self._plan_step(
                                action="accessibility_invoke_element",
                                args=group_args,
                                phase="act",
                                optional=False,
                                reason=f"Expand the '{candidate_name}' group so JARVIS can reveal hidden settings targets before committing changes.",
                            )
                        ],
                        result_metadata={
                            "form_stage": "group_hunt",
                            "form_page_index": page_index,
                            "form_page_kind": str(page_state.get("page_kind", "") or ""),
                            "form_group": candidate_name,
                            "form_group_action": expand_action,
                        },
                    )
                    results.extend(group_execution.get("results", []) if isinstance(group_execution.get("results", []), list) else [])
                    after_group_snapshot = self._form_flow_snapshot(args=page_args, advice=page_advice)
                    after_group_summary = self._form_flow_summary(snapshot=after_group_snapshot)
                    _remember_form_window(after_group_summary)
                    after_group_target_state = self._form_target_plan_state(plan=remaining_form_targets, snapshot=after_group_snapshot)
                    progressed = self._form_flow_progressed(before_snapshot=page_snapshot, after_snapshot=after_group_snapshot)
                    group_attempt = {
                        "name": candidate_name,
                        "expand_action": expand_action,
                        "invoke_action": invoke_action,
                        "match_score": float(group_candidate.get("match_score", 0.0) or 0.0),
                        "fallback_candidate": bool(group_candidate.get("fallback_candidate", False)),
                        "matched_targets": [dict(item) for item in group_candidate.get("matched_targets", []) if isinstance(item, dict)][:4],
                        "status": str(group_execution.get("status", "success") or "success"),
                        "message": str(group_execution.get("message", "") or ""),
                        "progressed": progressed,
                        "after": after_group_summary,
                    }
                    if after_group_target_state:
                        group_attempt["target_state_after"] = after_group_target_state
                        resolved_after = after_group_target_state.get("resolved_targets", []) if isinstance(after_group_target_state.get("resolved_targets", []), list) else []
                        _record_resolved_targets(resolved_after)
                        _remove_resolved_targets(resolved_after)
                    group_hunt_record["attempts"].append(group_attempt)
                    if str(group_execution.get("status", "success") or "success") == "success" and progressed:
                        page_record["group_hunt"] = group_hunt_record
                        page_record["after"] = after_group_summary
                        page_record["status"] = "group_expanded"
                        page_record["message"] = f"Expanded the '{candidate_name}' group to continue resolving the requested settings targets."
                        page_record["progressed"] = True
                        page_record["executed_actions"] = [
                            str(row.get("action", "") or "").strip()
                            for row in group_execution.get("results", [])
                            if isinstance(row, dict) and str(row.get("action", "") or "").strip()
                        ]
                        if after_group_target_state:
                            page_record["target_state_after"] = after_group_target_state
                        page_history.append(page_record)
                        last_snapshot = after_group_snapshot
                        reveal_progressed = True
                        break
            if reveal_progressed:
                continue

            scroll_search_supported = bool(page_state.get("scroll_search_supported", False)) and form_scroll_hunts_used < max_form_scroll_hunts
            scroll_hunt_record: Optional[Dict[str, Any]] = None
            if hidden_requested_targets and scroll_search_supported:
                form_scroll_hunts_used += 1
                scroll_hunt_record = {
                    "reason": "requested_targets_not_visible_in_current_viewport",
                    "attempt_index": form_scroll_hunts_used,
                    "max_attempts": max_form_scroll_hunts,
                    "attempts": [],
                }
                scroll_strategies = [
                    ("mouse_scroll", {"amount": -700}, "mouse_wheel_down"),
                    ("keyboard_hotkey", {"keys": ["pagedown"]}, "page_down"),
                ]
                for action_name, action_args, method_name in scroll_strategies:
                    scroll_execution = self._run_execution_plan(
                        plan=[
                            self._plan_step(
                                action=action_name,
                                args=dict(action_args),
                                phase="act",
                                optional=False,
                                reason="Scroll the current form surface so JARVIS can keep hunting below the visible viewport for the requested settings targets.",
                            )
                        ],
                        result_metadata={
                            "form_stage": "scroll_hunt",
                            "form_page_index": page_index,
                            "form_page_kind": str(page_state.get("page_kind", "") or ""),
                            "form_scroll_method": method_name,
                        },
                    )
                    results.extend(scroll_execution.get("results", []) if isinstance(scroll_execution.get("results", []), list) else [])
                    after_scroll_snapshot = self._form_flow_snapshot(args=page_args, advice=page_advice)
                    after_scroll_summary = self._form_flow_summary(snapshot=after_scroll_snapshot)
                    _remember_form_window(after_scroll_summary)
                    after_scroll_target_state = self._form_target_plan_state(plan=remaining_form_targets, snapshot=after_scroll_snapshot)
                    progressed = self._form_flow_progressed(before_snapshot=page_snapshot, after_snapshot=after_scroll_snapshot)
                    scroll_attempt = {
                        "method": method_name,
                        "action": action_name,
                        "status": str(scroll_execution.get("status", "success") or "success"),
                        "message": str(scroll_execution.get("message", "") or ""),
                        "progressed": progressed,
                        "after": after_scroll_summary,
                    }
                    if after_scroll_target_state:
                        scroll_attempt["target_state_after"] = after_scroll_target_state
                        resolved_after = after_scroll_target_state.get("resolved_targets", []) if isinstance(after_scroll_target_state.get("resolved_targets", []), list) else []
                        _record_resolved_targets(resolved_after)
                        _remove_resolved_targets(resolved_after)
                    scroll_hunt_record["attempts"].append(scroll_attempt)
                    if str(scroll_execution.get("status", "success") or "success") == "success" and progressed:
                        page_record["scroll_hunt"] = scroll_hunt_record
                        page_record["after"] = after_scroll_summary
                        page_record["status"] = "scroll_progressed"
                        page_record["message"] = "Scrolled the current form surface to continue resolving the requested settings targets."
                        page_record["progressed"] = True
                        page_record["executed_actions"] = [
                            str(row.get("action", "") or "").strip()
                            for row in scroll_execution.get("results", [])
                            if isinstance(row, dict) and str(row.get("action", "") or "").strip()
                        ]
                        if after_scroll_target_state:
                            page_record["target_state_after"] = after_scroll_target_state
                        page_history.append(page_record)
                        last_snapshot = after_scroll_snapshot
                        reveal_progressed = True
                        break
            if reveal_progressed:
                continue

            if hidden_requested_targets:
                if tab_hunt_record:
                    page_record["tab_hunt"] = tab_hunt_record
                if navigation_hunt_record:
                    page_record["navigation_hunt"] = navigation_hunt_record
                if drilldown_hunt_record:
                    page_record["drilldown_hunt"] = drilldown_hunt_record
                if group_hunt_record:
                    page_record["group_hunt"] = group_hunt_record
                if scroll_hunt_record:
                    page_record["scroll_hunt"] = scroll_hunt_record
                discovery_supported = bool(
                    tab_search_supported
                    or navigation_target_count > 0
                    or drilldown_target_count > 0
                    or expandable_group_count > 0
                    or page_state.get("scroll_search_supported", False)
                )
                if discovery_supported:
                    stop_reason_code = "form_target_discovery_exhausted"
                    stop_reason = "Requested settings targets are not visible on the current form surface, and JARVIS could not reveal them by switching tabs or sections, opening child pages, expanding groups, or scrolling safely."
                else:
                    stop_reason_code = "form_target_visibility_unknown"
                    stop_reason = "Requested settings targets are not currently visible, and the form does not expose tabs, sections, child pages, expandable groups, or scrolling that JARVIS can use to reveal them safely."
                page_record["status"] = "blocked"
                page_record["stop_reason_code"] = stop_reason_code
                page_record["stop_reason"] = stop_reason
                page_record["blocking_surface"] = self._blocking_surface_state(
                    snapshot=page_snapshot,
                    stop_reason_code=stop_reason_code,
                    mission_kind="form",
                )
                mission_warnings.append(stop_reason)
                page_history.append(page_record)
                break

            gate = self._form_flow_gate(
                page_state=page_state,
                safety_signals=safety_signals,
                allow_destructive_forms=allow_destructive_forms,
            )
            if not bool(gate.get("allowed", False)):
                stop_reason_code = str(gate.get("code", "") or "form_manual_review_required")
                stop_reason = str(gate.get("message", "") or "form page requires manual review before automation can continue")
                page_record["status"] = "blocked"
                page_record["stop_reason_code"] = stop_reason_code
                page_record["stop_reason"] = stop_reason
                page_record["blocking_surface"] = self._blocking_surface_state(
                    snapshot=page_snapshot,
                    stop_reason_code=stop_reason_code,
                    mission_kind="form",
                )
                if stop_reason:
                    mission_warnings.append(stop_reason)
                page_history.append(page_record)
                break

            if page_advice.get("status") != "success":
                stop_reason_code = "form_page_route_unavailable"
                stop_reason = "; ".join(
                    str(item) for item in page_advice.get("blockers", []) if str(item).strip()
                ) or str(page_advice.get("message", "form page route unavailable") or "form page route unavailable")
                page_record["status"] = "blocked"
                page_record["stop_reason_code"] = stop_reason_code
                page_record["stop_reason"] = stop_reason
                mission_warnings.extend([str(item).strip() for item in page_advice.get("warnings", []) if str(item).strip()])
                page_history.append(page_record)
                break

            page_execution = self._run_execution_plan(
                plan=page_advice.get("execution_plan", []),
                result_metadata={
                    "form_stage": "page",
                    "form_page_index": page_index,
                    "form_page_kind": str(page_state.get("page_kind", "") or ""),
                },
            )
            results.extend(page_execution.get("results", []) if isinstance(page_execution.get("results", []), list) else [])
            if str(page_execution.get("status", "success") or "success") == "success" and page_target_state:
                planned_targets = page_target_state.get("planned_targets", []) if isinstance(page_target_state.get("planned_targets", []), list) else []
                _record_resolved_targets(planned_targets)
                _remove_resolved_targets(planned_targets)
            after_snapshot = self._form_flow_snapshot(args=page_args, advice=page_advice)
            after_summary = self._form_flow_summary(snapshot=after_snapshot)
            _remember_form_window(after_summary)
            after_target_state = self._form_target_plan_state(plan=remaining_form_targets, snapshot=after_snapshot)
            progressed = self._form_flow_progressed(before_snapshot=page_snapshot, after_snapshot=after_snapshot)
            page_record["after"] = after_summary
            if after_target_state:
                page_record["target_state_after"] = after_target_state
                resolved_after = after_target_state.get("resolved_targets", []) if isinstance(after_target_state.get("resolved_targets", []), list) else []
                _record_resolved_targets(resolved_after)
                _remove_resolved_targets(resolved_after)
            page_record["status"] = str(page_execution.get("status", "success") or "success")
            page_record["message"] = str(page_execution.get("message", "") or "")
            page_record["progressed"] = progressed
            page_record["executed_actions"] = [
                str(row.get("action", "") or "").strip()
                for row in page_execution.get("results", [])
                if isinstance(row, dict) and str(row.get("action", "") or "").strip()
            ]
            page_history.append(page_record)
            mission_warnings.extend([str(item).strip() for item in page_advice.get("warnings", []) if str(item).strip()])

            if str(page_execution.get("status", "success") or "success") != "success":
                status = "error" if not results else "success"
                stop_reason_code = "form_page_execution_failed"
                stop_reason = str(page_execution.get("message", "form page execution failed") or "form page execution failed")
                break

            if progressed or not bool(after_summary.get("form_visible", False)):
                pages_completed += 1
            if not bool(after_summary.get("form_visible", False)):
                completed = True
                message = "form flow completed and the settings surface closed"
                last_snapshot = after_snapshot
                break
            if not progressed:
                stop_reason_code = "form_page_stalled"
                stop_reason = "Form page execution completed, but the settings surface did not change to a new state."
                break

            last_snapshot = after_snapshot

        if not completed and not stop_reason_code and not message and pages_completed >= max_pages:
            stop_reason_code = "form_page_limit_reached"
            stop_reason = f"Form mission reached the configured page limit of {max_pages} without reaching a completed settings state."
        if completed and not message:
            message = "form flow completed"
        elif not message:
            message = stop_reason or "form flow stopped before completion"

        post_context = self._capture_verification_context(args=args, advice=advice) if status == "success" else {}
        final_summary = self._form_flow_summary(snapshot=last_snapshot)
        if completed and remaining_form_targets:
            mission_warnings.append(
                f"Form flow completed, but {len(remaining_form_targets)} requested target state(s) could not be confirmed before the surface closed."
            )
        verification = self._verify_form_flow_execution(
            args=args,
            pre_context=pre_context,
            post_context=post_context,
            completed=completed,
            pages_completed=pages_completed,
            max_pages=max_pages,
            stop_reason_code=stop_reason_code,
            stop_reason=stop_reason,
            final_summary=final_summary,
            warnings=mission_warnings,
            requested_target_count=len(requested_form_targets),
            resolved_target_count=len(resolved_form_targets),
            remaining_target_count=len(remaining_form_targets),
        )
        if status == "success" and bool(verification.get("enabled", False)) and not bool(verification.get("verified", False)):
            message = str(verification.get("message", message) or message)
        blocking_surface = self._blocking_surface_state(
            snapshot=last_snapshot,
            stop_reason_code=stop_reason_code,
            mission_kind="form",
        )
        resume_contract = self._mission_resume_contract(
            mission_kind="form",
            args=args,
            stop_reason_code=stop_reason_code,
            blocking_surface=blocking_surface,
            anchor_window_title=form_anchor_title,
            anchor_app_name=form_anchor_app_name,
            remaining_form_targets=remaining_form_targets,
        )
        form_mission_payload = {
            "enabled": True,
            "completed": completed,
            "pages_completed": pages_completed,
            "page_count": len(page_history),
            "max_pages": max_pages,
            "allow_destructive_forms": allow_destructive_forms,
            "stop_reason_code": stop_reason_code,
            "stop_reason": stop_reason,
            "blocking_surface": blocking_surface,
            "resume_contract": resume_contract,
            "requested_target_count": len(requested_form_targets),
            "resolved_target_count": len(resolved_form_targets),
            "remaining_target_count": len(remaining_form_targets),
            "resolved_targets": resolved_form_targets[:12],
            "remaining_targets": [dict(row) for row in remaining_form_targets[:12]],
            "page_history": page_history,
            "final_page": final_summary,
            "risk_level": advice.get("risk_level", ""),
            "status": status,
            "message": message,
        }
        mission_record = {}
        if blocking_surface and resume_contract:
            mission_record = self._persist_paused_mission(
                mission_kind="form",
                args=args,
                blocking_surface=blocking_surface,
                resume_contract=resume_contract,
                mission_payload=form_mission_payload,
                warnings=mission_warnings,
                message=message,
            )
            if mission_record:
                form_mission_payload["mission_record"] = mission_record

        return {
            "attempt": attempt_index,
            "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
            "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
            "strategy_reason": str(strategy.get("reason", "") or "").strip(),
            "payload": self._sanitize_payload_for_response(args),
            "status": status,
            "message": message,
            "final_action": "complete_form_flow" if completed else str(results[-1]["action"] if results else advice.get("action", "")),
            "results": results,
            "advice": advice,
            "verification": verification,
            "form_mission": form_mission_payload,
            "mission_record": mission_record,
        }

    def _resolve_form_dialog_interstitial(
        self,
        *,
        args: Dict[str, Any],
        snapshot: Dict[str, Any],
        page_index: int,
    ) -> Dict[str, Any]:
        flags = snapshot.get("surface_flags", {}) if isinstance(snapshot.get("surface_flags", {}), dict) else {}
        if not bool(flags.get("dialog_visible", False)):
            return {"handled": False, "blocked": False}

        page_state = snapshot.get("form_page_state", {}) if isinstance(snapshot.get("form_page_state", {}), dict) else {}
        safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        dialog_kind = self._normalize_probe_text(dialog_state.get("dialog_kind", ""))
        approval_kind = self._normalize_probe_text(dialog_state.get("approval_kind", ""))
        dialog_manual_input_required = bool(dialog_state.get("manual_input_required", False))
        dialog_credential_required = bool(dialog_state.get("credential_required", False))
        secure_desktop_likely = bool(dialog_state.get("secure_desktop_likely", False))
        pending_requirement_count = int(page_state.get("pending_requirement_count", 0) or 0)
        if approval_kind == "elevation_credentials":
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "elevation_credentials_required",
                "stop_reason": "Form mission paused because an interstitial elevation dialog is requesting administrator credentials or secure sign-in input.",
                "button_label": str(safety_signals.get("preferred_confirmation_button", "") or safety_signals.get("preferred_dismiss_button", "") or "").strip(),
                "button_role": "",
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }
        if approval_kind == "elevation_consent":
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "elevation_consent_required",
                "stop_reason": "Form mission paused because an interstitial elevation dialog is requesting administrator approval."
                + (" The prompt also appears to be on a secure desktop surface." if secure_desktop_likely else ""),
                "button_label": str(safety_signals.get("preferred_confirmation_button", "") or safety_signals.get("preferred_dismiss_button", "") or "").strip(),
                "button_role": "",
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }
        if dialog_manual_input_required and dialog_credential_required:
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "credential_input_required",
                "stop_reason": "Form mission paused because an interstitial dialog is requesting credentials or sign-in input.",
                "button_label": str(safety_signals.get("preferred_confirmation_button", "") or safety_signals.get("preferred_dismiss_button", "") or "").strip(),
                "button_role": "",
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }
        if dialog_kind == "authentication_review":
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "authentication_review_required",
                "stop_reason": "Form mission paused on an interstitial authentication confirmation so JARVIS does not approve identity-sensitive changes blindly.",
                "button_label": str(safety_signals.get("preferred_confirmation_button", "") or safety_signals.get("preferred_dismiss_button", "") or "").strip(),
                "button_role": "",
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }
        if dialog_kind == "permission_review":
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "permission_review_required",
                "stop_reason": "Form mission paused on an interstitial permission review so JARVIS does not approve app access or consent changes blindly.",
                "button_label": str(safety_signals.get("preferred_confirmation_button", "") or safety_signals.get("preferred_dismiss_button", "") or "").strip(),
                "button_role": "",
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }
        if pending_requirement_count > 0 or bool(page_state.get("manual_input_likely", False)):
            return {"handled": False, "blocked": False}
        if any(
            int(page_state.get(count_key, 0) or 0) > 0
            for count_key in ("tab_count", "navigation_target_count", "drilldown_target_count", "expandable_group_count")
        ) or bool(page_state.get("scroll_search_supported", False)):
            return {"handled": False, "blocked": False}

        element_payload = snapshot.get("elements", {}) if isinstance(snapshot.get("elements", {}), dict) else {}
        element_rows = element_payload.get("items", []) if isinstance(element_payload.get("items", []), list) else []
        interactive_control_types = {
            "checkbox",
            "radiobutton",
            "combobox",
            "edit",
            "slider",
            "spinner",
            "togglebutton",
            "tabitem",
            "treeitem",
            "listitem",
            "menuitem",
            "dataitem",
            "table",
            "toolbar",
        }
        if any(
            self._normalize_probe_text(row.get("control_type", "")) in interactive_control_types
            for row in element_rows
            if isinstance(row, dict)
        ):
            return {"handled": False, "blocked": False}

        destructive_warning_visible = bool(safety_signals.get("destructive_warning_visible", False))
        warning_surface_visible = bool(safety_signals.get("warning_surface_visible", False))
        elevation_prompt_visible = bool(safety_signals.get("elevation_prompt_visible", False))
        preferred_confirmation_button = str(
            safety_signals.get("preferred_confirmation_button", "")
            or page_state.get("preferred_commit_button", "")
            or ""
        ).strip()
        preferred_confirmation_target = (
            safety_signals.get("preferred_confirmation_target", {})
            if isinstance(safety_signals.get("preferred_confirmation_target", {}), dict)
            else {}
        )
        if not preferred_confirmation_target and isinstance(page_state.get("preferred_commit_target", {}), dict):
            preferred_confirmation_target = dict(page_state.get("preferred_commit_target", {}))
        preferred_dismiss_button = str(
            safety_signals.get("preferred_dismiss_button", "")
            or page_state.get("preferred_dismiss_button", "")
            or ""
        ).strip()
        preferred_dismiss_target = (
            safety_signals.get("preferred_dismiss_target", {})
            if isinstance(safety_signals.get("preferred_dismiss_target", {}), dict)
            else {}
        )

        action = ""
        button_label = ""
        button_role = ""
        button_target: Dict[str, Any] = {}
        normalized_dismiss = self._normalize_probe_text(preferred_dismiss_button)
        if preferred_confirmation_button and not destructive_warning_visible and not warning_surface_visible and not elevation_prompt_visible:
            action = "press_dialog_button"
            button_label = preferred_confirmation_button
            button_role = "confirm"
            button_target = preferred_confirmation_target
        elif preferred_dismiss_button and normalized_dismiss in {"close", "dismiss"} and not destructive_warning_visible and not warning_surface_visible and not elevation_prompt_visible:
            action = "press_dialog_button"
            button_label = preferred_dismiss_button
            button_role = "dismiss"
            button_target = preferred_dismiss_target
        else:
            blocker_code = "form_dialog_review_required"
            blocker_message = "Form mission paused on an interstitial dialog that requires review before automation can continue."
            if elevation_prompt_visible:
                blocker_code = "elevation_confirmation_required"
                blocker_message = "Form mission paused because an interstitial dialog is requesting elevated privileges."
            elif destructive_warning_visible:
                blocker_code = "destructive_form_review_required"
                blocker_message = "Form mission paused on an interstitial destructive confirmation so JARVIS does not auto-commit a risky settings change."
            elif warning_surface_visible:
                blocker_code = "form_dialog_review_required"
                blocker_message = "Form mission paused on an interstitial warning dialog so JARVIS does not auto-confirm a risky settings step."
            elif not preferred_confirmation_button and not preferred_dismiss_button:
                blocker_code = "form_dialog_route_unavailable"
                blocker_message = "Form mission found an interstitial dialog, but it does not expose a reliable button target for autonomous continuation."
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": blocker_code,
                "stop_reason": blocker_message,
                "button_label": preferred_confirmation_button or preferred_dismiss_button,
                "button_role": "confirm" if preferred_confirmation_button else ("dismiss" if preferred_dismiss_button else ""),
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }

        dialog_args = dict(args)
        summary = self._form_flow_summary(snapshot=snapshot)
        dialog_window_title = str(summary.get("window_title", "") or "").strip()
        if dialog_window_title:
            dialog_args["window_title"] = dialog_window_title
            dialog_args["app_name"] = ""
            dialog_args["focus_first"] = True
        dialog_args["action"] = action
        dialog_args["query"] = button_label
        dialog_args["control_type"] = "Button"
        element_id = str(button_target.get("element_id", "") or "").strip()
        if element_id:
            dialog_args["element_id"] = element_id
        dialog_args["_provided_fields"] = self._dedupe_strings(
            list(dialog_args.get("_provided_fields", []))
            + ["action", "query", "control_type"]
            + ([] if not element_id else ["element_id"])
        )

        dialog_advice = self.advise(dialog_args)
        if dialog_advice.get("status") != "success":
            blockers = "; ".join(str(item).strip() for item in dialog_advice.get("blockers", []) if str(item).strip())
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "form_dialog_route_unavailable",
                "stop_reason": blockers or str(dialog_advice.get("message", "form dialog route unavailable") or "form dialog route unavailable"),
                "button_label": button_label,
                "button_role": button_role,
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }

        dialog_execution = self._run_execution_plan(
            plan=dialog_advice.get("execution_plan", []),
            result_metadata={
                "form_stage": "dialog_followup",
                "form_page_index": page_index,
                "dialog_button": button_label,
                "dialog_button_role": button_role,
            },
        )
        after_snapshot = self._form_flow_snapshot(args=dialog_args, advice=dialog_advice)
        after_summary = self._form_flow_summary(snapshot=after_snapshot)
        progressed = self._form_flow_progressed(before_snapshot=snapshot, after_snapshot=after_snapshot)
        if bool(flags.get("dialog_visible", False)) and not bool(after_summary.get("dialog_visible", False)):
            progressed = True
        if str(dialog_execution.get("status", "success") or "success") != "success":
            return {
                "handled": False,
                "blocked": True,
                "stop_reason_code": "form_dialog_execution_failed",
                "stop_reason": str(dialog_execution.get("message", "form dialog execution failed") or "form dialog execution failed"),
                "button_label": button_label,
                "button_role": button_role,
                "dialog_kind": dialog_kind,
                "approval_kind": approval_kind,
                "secure_desktop_likely": secure_desktop_likely,
            }

        return {
            "handled": str(dialog_execution.get("status", "success") or "success") == "success",
            "blocked": False,
            "action": action,
            "button_label": button_label,
            "button_role": button_role,
            "route_mode": str(dialog_advice.get("route_mode", "") or "").strip(),
            "dialog_kind": dialog_kind,
            "approval_kind": approval_kind,
            "secure_desktop_likely": secure_desktop_likely,
            "status": "dialog_confirmed" if button_role == "confirm" else "dialog_dismissed",
            "message": (
                f"Resolved the interstitial dialog through '{button_label}' and continued the form mission."
                if button_label
                else "Resolved the interstitial dialog and continued the form mission."
            ),
            "warnings": [str(item).strip() for item in dialog_advice.get("warnings", []) if str(item).strip()],
            "execution": dialog_execution,
            "after_snapshot": after_snapshot,
            "after_summary": after_summary,
            "progressed": progressed,
        }

    def _form_flow_snapshot(self, *, args: Dict[str, Any], advice: Dict[str, Any]) -> Dict[str, Any]:
        target_window = advice.get("target_window", {}) if isinstance(advice.get("target_window", {}), dict) else {}
        focus_title = str(
            args.get("window_title", "")
            or target_window.get("title", "")
            or args.get("app_name", "")
            or ""
        ).strip()
        snapshot = self.surface_snapshot(
            app_name=str(args.get("app_name", "") or "").strip(),
            window_title=focus_title,
            query="",
            limit=18,
            include_observation=True,
            include_elements=True,
            include_workflow_probes=True,
            preferred_actions=["complete_form_flow", "complete_form_page", "confirm_dialog", "dismiss_dialog", "focus_form_surface"],
        )
        candidate_windows = snapshot.get("candidate_windows", []) if isinstance(snapshot.get("candidate_windows", []), list) else []
        active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
        app_name = str(args.get("app_name", "") or "").strip()
        window_title = str(args.get("window_title", "") or "").strip()
        target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
        target_visible = bool(target_window) or any(isinstance(row, dict) for row in candidate_windows)
        active_matches = self._window_matches(active_window, app_name=app_name, window_title=window_title or focus_title)
        adopted_title = str(active_window.get("title", "") or "").strip()
        normalized_adopted = self._normalize_probe_text(adopted_title)
        normalized_focus = self._normalize_probe_text(focus_title)
        normalized_target = self._normalize_probe_text(target_window.get("title", ""))
        active_hwnd = self._to_int(active_window.get("hwnd"))
        target_hwnd = self._to_int(target_window.get("hwnd"))
        should_try_adopt = bool(
            adopted_title
            and (
                not active_matches
                or normalized_adopted != normalized_focus
                or normalized_adopted != normalized_target
                or (active_hwnd is not None and target_hwnd is not None and active_hwnd != target_hwnd and normalized_adopted != normalized_target)
            )
        )
        if should_try_adopt:
            if adopted_title and (normalized_adopted != normalized_focus or active_hwnd != target_hwnd):
                adopted_snapshot = self.surface_snapshot(
                    app_name="",
                    window_title=adopted_title,
                    query="",
                    limit=18,
                    include_observation=True,
                    include_elements=True,
                    include_workflow_probes=True,
                    preferred_actions=["complete_form_flow", "complete_form_page", "confirm_dialog", "dismiss_dialog", "focus_form_surface"],
                )
                adopted_flags = adopted_snapshot.get("surface_flags", {}) if isinstance(adopted_snapshot.get("surface_flags", {}), dict) else {}
                adopted_page_state = adopted_snapshot.get("form_page_state", {}) if isinstance(adopted_snapshot.get("form_page_state", {}), dict) else {}
                adopted_target_window = adopted_snapshot.get("target_window", {}) if isinstance(adopted_snapshot.get("target_window", {}), dict) else {}
                adopted_active_window = adopted_snapshot.get("active_window", {}) if isinstance(adopted_snapshot.get("active_window", {}), dict) else {}
                adopted_form_visible = bool(
                    adopted_flags.get("form_visible", False)
                    or adopted_flags.get("dialog_visible", False)
                    or adopted_flags.get("tab_page_visible", False)
                    or adopted_page_state
                )
                if adopted_form_visible and (adopted_target_window or adopted_active_window):
                    adopted_snapshot["adopted_form_window"] = {
                        "title": str(adopted_active_window.get("title", "") or adopted_target_window.get("title", "") or adopted_title).strip(),
                        "hwnd": self._to_int(adopted_active_window.get("hwnd")) or self._to_int(adopted_target_window.get("hwnd")),
                        "previous_title": focus_title,
                        "reason": "active_child_window",
                    }
                    snapshot = adopted_snapshot
                    candidate_windows = snapshot.get("candidate_windows", []) if isinstance(snapshot.get("candidate_windows", []), list) else []
                    active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
                    target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
                    target_visible = bool(target_window) or any(isinstance(row, dict) for row in candidate_windows)
                    active_matches = self._window_matches(active_window, app_name=app_name, window_title=adopted_title)
        if not target_visible and not active_matches:
            flags = snapshot.get("surface_flags", {}) if isinstance(snapshot.get("surface_flags", {}), dict) else {}
            snapshot["surface_flags"] = {
                **flags,
                "form_visible": False,
                "dialog_visible": False,
            }
            snapshot["form_page_state"] = {}
        return snapshot

    @staticmethod
    def _form_flow_summary(*, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        flags = snapshot.get("surface_flags", {}) if isinstance(snapshot.get("surface_flags", {}), dict) else {}
        page_state = snapshot.get("form_page_state", {}) if isinstance(snapshot.get("form_page_state", {}), dict) else {}
        observation = snapshot.get("observation", {}) if isinstance(snapshot.get("observation", {}), dict) else {}
        safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
        active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
        adopted_window = snapshot.get("adopted_form_window", {}) if isinstance(snapshot.get("adopted_form_window", {}), dict) else {}
        form_visible = bool(
            flags.get("form_visible", False)
            or flags.get("dialog_visible", False)
            or flags.get("tab_page_visible", False)
            or page_state
        )
        return {
            "form_visible": form_visible,
            "dialog_visible": bool(flags.get("dialog_visible", False)),
            "page_kind": str(page_state.get("page_kind", "") or "").strip(),
            "commit_action": str(page_state.get("commit_action", "") or "").strip(),
            "ready_for_commit": bool(page_state.get("ready_for_commit", False)),
            "pending_requirement_count": int(page_state.get("pending_requirement_count", 0) or 0),
            "preferred_commit_button": str(page_state.get("preferred_commit_button", "") or "").strip(),
            "selected_tab": str(page_state.get("selected_tab", "") or "").strip(),
            "tab_count": int(page_state.get("tab_count", 0) or 0),
            "selected_navigation_target": str(page_state.get("selected_navigation_target", "") or "").strip(),
            "navigation_target_count": int(page_state.get("navigation_target_count", 0) or 0),
            "drilldown_target_count": int(page_state.get("drilldown_target_count", 0) or 0),
            "expandable_group_count": int(page_state.get("expandable_group_count", 0) or 0),
            "expanded_group_count": int(page_state.get("expanded_group_count", 0) or 0),
            "scroll_search_supported": bool(page_state.get("scroll_search_supported", False)),
            "breadcrumb_path": [str(item).strip() for item in page_state.get("breadcrumb_path", []) if str(item).strip()] if isinstance(page_state.get("breadcrumb_path", []), list) else [],
            "window_title": str(adopted_window.get("title", "") or target_window.get("title", "") or active_window.get("title", "") or "").strip(),
            "window_hwnd": int(adopted_window.get("hwnd", 0) or target_window.get("hwnd", 0) or active_window.get("hwnd", 0) or 0),
            "window_adopted": bool(adopted_window),
            "autonomous_progress_supported": bool(page_state.get("autonomous_progress_supported", False)),
            "autonomous_blocker": str(page_state.get("autonomous_blocker", "") or "").strip(),
            "manual_input_likely": bool(page_state.get("manual_input_likely", False)),
            "preferred_dialog_confirmation_button": str(safety_signals.get("preferred_confirmation_button", "") or "").strip(),
            "preferred_dialog_dismiss_button": str(safety_signals.get("preferred_dismiss_button", "") or "").strip(),
            "warning_surface_visible": bool(safety_signals.get("warning_surface_visible", False)),
            "destructive_warning_visible": bool(safety_signals.get("destructive_warning_visible", False)),
            "dialog_kind": str(dialog_state.get("dialog_kind", "") or "").strip(),
            "approval_kind": str(dialog_state.get("approval_kind", "") or "").strip(),
            "dialog_review_required": bool(dialog_state.get("review_required", False)),
            "dialog_auto_resolve_supported": bool(dialog_state.get("auto_resolve_supported", False)),
            "secure_desktop_likely": bool(dialog_state.get("secure_desktop_likely", False)),
            "credential_field_count": int(dialog_state.get("credential_field_count", 0) or 0),
            "screen_hash": str(observation.get("screen_hash", "") or "").strip(),
        }

    @classmethod
    def _form_flow_signature(cls, *, snapshot: Dict[str, Any]) -> tuple[str, str, int, str, bool, bool, str, str, str, str, int, int, str, str, int, bool, bool, str, str, bool, bool, bool, int]:
        summary = cls._form_flow_summary(snapshot=snapshot)
        return (
            str(summary.get("page_kind", "") or "").strip().lower(),
            str(summary.get("preferred_commit_button", "") or "").strip().lower(),
            int(summary.get("pending_requirement_count", 0) or 0),
            str(summary.get("screen_hash", "") or "").strip(),
            bool(summary.get("form_visible", False)),
            bool(summary.get("dialog_visible", False)),
            str(summary.get("preferred_dialog_confirmation_button", "") or "").strip().lower(),
            str(summary.get("preferred_dialog_dismiss_button", "") or "").strip().lower(),
            str(summary.get("selected_tab", "") or "").strip().lower(),
            str(summary.get("selected_navigation_target", "") or "").strip().lower(),
            int(summary.get("drilldown_target_count", 0) or 0),
            int(summary.get("expanded_group_count", 0) or 0),
            " > ".join(str(item).strip().lower() for item in summary.get("breadcrumb_path", []) if str(item).strip()),
            str(summary.get("window_title", "") or "").strip().lower(),
            int(summary.get("window_hwnd", 0) or 0),
            bool(summary.get("warning_surface_visible", False)),
            bool(summary.get("destructive_warning_visible", False)),
            str(summary.get("dialog_kind", "") or "").strip().lower(),
            str(summary.get("approval_kind", "") or "").strip().lower(),
            bool(summary.get("dialog_review_required", False)),
            bool(summary.get("dialog_auto_resolve_supported", False)),
            bool(summary.get("secure_desktop_likely", False)),
            int(summary.get("credential_field_count", 0) or 0),
        )

    @classmethod
    def _form_flow_progressed(cls, *, before_snapshot: Dict[str, Any], after_snapshot: Dict[str, Any]) -> bool:
        after_summary = cls._form_flow_summary(snapshot=after_snapshot)
        if not bool(after_summary.get("form_visible", False)):
            return True
        return cls._form_flow_signature(snapshot=before_snapshot) != cls._form_flow_signature(snapshot=after_snapshot)

    @staticmethod
    def _form_flow_gate(
        *,
        page_state: Dict[str, Any],
        safety_signals: Dict[str, Any],
        allow_destructive_forms: bool,
    ) -> Dict[str, Any]:
        blocker = str(page_state.get("autonomous_blocker", "") or "").strip()
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        approval_kind = str(dialog_state.get("approval_kind", "") or "").strip().lower()
        secure_desktop_likely = bool(dialog_state.get("secure_desktop_likely", False))
        blocker_messages = {
            "manual_input_required": "The current form likely requires manual text or option input before JARVIS can continue safely.",
            "credential_input_required": "The current dialog appears to require credentials or sign-in input before JARVIS can continue safely.",
            "authentication_review_required": "The current dialog is asking for authentication review or identity confirmation before JARVIS can continue.",
            "permission_review_required": "The current dialog is asking for a permission or consent review before JARVIS can continue.",
            "elevation_consent_required": "The current form is requesting elevated approval, so JARVIS is pausing for explicit confirmation instead of continuing autonomously.",
            "elevation_credentials_required": "The current form is requesting administrator credentials, so JARVIS is pausing for explicit review and manual input instead of continuing autonomously.",
            "elevation_confirmation_required": "The current form is requesting elevated privileges, so JARVIS is pausing for explicit review instead of continuing autonomously.",
            "unsupported_form_requirements": "The current form exposes requirements that JARVIS cannot yet resolve safely without a more specific requested target.",
            "no_commit_target_available": "The current form does not expose a reliable Save, Apply, OK, or Done control for autonomous completion.",
        }
        if approval_kind == "elevation_credentials":
            return {
                "allowed": False,
                "code": "elevation_credentials_required",
                "message": blocker_messages["elevation_credentials_required"],
            }
        if approval_kind == "elevation_consent":
            return {
                "allowed": False,
                "code": "elevation_consent_required",
                "message": blocker_messages["elevation_consent_required"]
                + (" The prompt also appears to be on a secure desktop surface." if secure_desktop_likely else ""),
            }
        if approval_kind == "permission_review":
            return {
                "allowed": False,
                "code": "permission_review_required",
                "message": blocker_messages["permission_review_required"],
            }
        if bool(safety_signals.get("destructive_warning_visible", False)) and not allow_destructive_forms:
            return {
                "allowed": False,
                "code": "destructive_form_review_required",
                "message": "The current form exposes a destructive or irreversible warning, so JARVIS is pausing before committing the change.",
            }
        if blocker:
            return {
                "allowed": False,
                "code": blocker,
                "message": blocker_messages.get(blocker, "Form mission paused because the current settings page is not safe for autonomous progression."),
            }
        if not bool(page_state.get("autonomous_progress_supported", False)):
            return {
                "allowed": False,
                "code": "form_progress_not_supported",
                "message": "The current form page is visible, but it does not expose a reliable autonomous commit path yet.",
            }
        return {"allowed": True, "code": "", "message": ""}

    @staticmethod
    def _verify_form_flow_execution(
        *,
        args: Dict[str, Any],
        pre_context: Dict[str, Any],
        post_context: Dict[str, Any],
        completed: bool,
        pages_completed: int,
        max_pages: int,
        stop_reason_code: str,
        stop_reason: str,
        final_summary: Dict[str, Any],
        warnings: List[str],
        requested_target_count: int = 0,
        resolved_target_count: int = 0,
        remaining_target_count: int = 0,
    ) -> Dict[str, Any]:
        checks = [
            {
                "name": "form_pages_completed",
                "passed": pages_completed > 0 or completed,
                "pages_completed": pages_completed,
                "max_pages": max_pages,
            },
            {
                "name": "form_surface_closed",
                "passed": completed,
                "form_visible_after": bool(final_summary.get("form_visible", False)),
                "final_page_kind": str(final_summary.get("page_kind", "") or "").strip(),
            },
        ]
        if requested_target_count > 0:
            checks.append(
                {
                    "name": "form_targets_confirmed",
                    "passed": remaining_target_count == 0,
                    "requested_target_count": requested_target_count,
                    "resolved_target_count": resolved_target_count,
                    "remaining_target_count": remaining_target_count,
                }
            )
        if stop_reason_code:
            checks.append(
                {
                    "name": "form_safe_stop",
                    "passed": False,
                    "reason_code": stop_reason_code,
                    "reason": stop_reason,
                }
            )
        verified = bool(completed and remaining_target_count == 0)
        if completed and remaining_target_count > 0:
            message = "form flow completed, but some requested target states were not confirmed"
        else:
            message = "form flow completed" if verified else (stop_reason or "form flow stopped before completion")
        status = "degraded" if verified and warnings else ("success" if verified else "failed")
        return {
            "enabled": True,
            "status": status,
            "verified": verified,
            "message": message,
            "checks": checks,
            "warnings": DesktopActionRouter._dedupe_strings(warnings),
            "pre_context": {
                "active_window": pre_context.get("active_window", {}) if isinstance(pre_context.get("active_window", {}), dict) else {},
                "screen_hash": str((pre_context.get("observation", {}) if isinstance(pre_context.get("observation", {}), dict) else {}).get("screen_hash", "") or "").strip(),
            },
            "post_context": {
                "active_window": post_context.get("active_window", {}) if isinstance(post_context.get("active_window", {}), dict) else {},
                "screen_hash": str((post_context.get("observation", {}) if isinstance(post_context.get("observation", {}), dict) else {}).get("screen_hash", "") or "").strip(),
            },
            "verify_text": str(args.get("verify_text", "") or "").strip() or "settings applied",
        }

    def _capture_verification_context(self, *, args: Dict[str, Any], advice: Dict[str, Any]) -> Dict[str, Any]:
        context: Dict[str, Any] = {"timestamp": time.time()}
        action = str(args.get("action", "observe") or "observe").strip().lower()
        verify_enabled = bool(args.get("verify_after_action", True))
        if not verify_enabled:
            return context
        if action in {"launch", "focus", "type", "click", "click_and_type", "hotkey", *WORKFLOW_ACTIONS}:
            context["active_window"] = self._active_window()
        capabilities = advice.get("capabilities", {}) if isinstance(advice.get("capabilities", {}), dict) else {}
        vision_ready = bool(capabilities.get("vision", {}).get("available")) if isinstance(capabilities.get("vision", {}), dict) else False
        if vision_ready and action in {"observe", "click", "click_and_type", "type", "hotkey", *WORKFLOW_ACTIONS}:
            context["observation"] = self._call("computer_observe", {"include_targets": False})
        if action in WORKFLOW_ACTIONS:
            context["workflow_probe"] = self._run_workflow_probes(
                action=action,
                args=args,
                advice=advice,
                capabilities=capabilities,
            )
        return context

    def _verify_execution(
        self,
        *,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        results: List[Dict[str, Any]],
        pre_context: Dict[str, Any],
        post_context: Dict[str, Any],
        step_status: str,
    ) -> Dict[str, Any]:
        enabled = bool(args.get("verify_after_action", True))
        if not enabled:
            return {
                "enabled": False,
                "status": "skipped",
                "verified": True,
                "message": "post-action verification disabled",
                "checks": [],
            }
        if str(step_status).strip().lower() != "success":
            return {
                "enabled": True,
                "status": "skipped",
                "verified": False,
                "message": "execution failed before verification could run",
                "checks": [],
            }
        action = str(args.get("action", "observe") or "observe").strip().lower()
        app_name = str(args.get("app_name", "") or "").strip()
        window_title = str(args.get("window_title", "") or "").strip()
        verify_text = str(args.get("verify_text", "") or "").strip()
        if not verify_text:
            if action in {"type", "click_and_type", "command", "rename_symbol", "terminal_command"}:
                verify_text = str(args.get("text", "") or "").strip()
            elif action in {"navigate", "search", "quick_open", "workspace_search", "go_to_symbol"}:
                verify_text = str(args.get("query", "") or "").strip()
            else:
                verify_text = str(args.get("query", "") or "").strip()

        pre_active = pre_context.get("active_window", {}) if isinstance(pre_context.get("active_window", {}), dict) else {}
        post_active = post_context.get("active_window", {}) if isinstance(post_context.get("active_window", {}), dict) else {}
        pre_observation = pre_context.get("observation", {}) if isinstance(pre_context.get("observation", {}), dict) else {}
        post_observation = post_context.get("observation", {}) if isinstance(post_context.get("observation", {}), dict) else {}
        pre_probe = pre_context.get("workflow_probe", {}) if isinstance(pre_context.get("workflow_probe", {}), dict) else {}
        post_probe = post_context.get("workflow_probe", {}) if isinstance(post_context.get("workflow_probe", {}), dict) else {}
        checks: List[Dict[str, Any]] = []
        warnings: List[str] = []
        focus_step = self._find_step_result(results, "focus_window")
        if not post_active and isinstance(focus_step.get("window", {}), dict):
            post_active = focus_step.get("window", {})

        active_match = self._window_matches(post_active, app_name=app_name, window_title=window_title)
        target_window = advice.get("target_window", {}) if isinstance(advice.get("target_window", {}), dict) else {}
        if not active_match and target_window:
            active_match = self._to_int(post_active.get("hwnd")) == self._to_int(target_window.get("hwnd"))
        window_present = False
        if app_name or window_title:
            windows = self._list_windows()
            window_present = any(self._window_matches(row, app_name=app_name, window_title=window_title) for row in windows)
        pre_hash = str(pre_observation.get("screen_hash", "") or "").strip()
        post_hash = str(post_observation.get("screen_hash", "") or "").strip()
        screen_changed = bool(pre_hash and post_hash and pre_hash != post_hash)
        pre_text = str(pre_observation.get("text", "") or "")
        post_text = str(post_observation.get("text", "") or "")
        text_visible = self._contains_text(post_text, verify_text)
        pre_text_visible = self._contains_text(pre_text, verify_text)
        probe_queries = post_probe.get("queries", []) if isinstance(post_probe.get("queries", []), list) else []
        if not probe_queries:
            probe_queries = pre_probe.get("queries", []) if isinstance(pre_probe.get("queries", []), list) else []
        probe_matches = post_probe.get("matches", []) if isinstance(post_probe.get("matches", []), list) else []
        probe_sources = post_probe.get("sources", []) if isinstance(post_probe.get("sources", []), list) else []
        probe_match = bool(post_probe.get("matched", False))
        pre_probe_match = bool(pre_probe.get("matched", False))
        active_changed = self._to_int(pre_active.get("hwnd")) != self._to_int(post_active.get("hwnd")) if pre_active and post_active else False
        workflow_state_signal = bool(screen_changed or text_visible or probe_match)
        workflow_surface_signal = bool(screen_changed or active_changed or text_visible or probe_match)
        final_step = results[-1].get("result", {}) if results and isinstance(results[-1].get("result", {}), dict) else {}
        click_step = self._find_step_result(results, "computer_click_target")
        type_step = self._find_step_result(results, "keyboard_type")
        click_changed = bool(click_step.get("screen_changed", False)) if isinstance(click_step, dict) else False
        screenshot_path = str(post_observation.get("screenshot_path", "") or "").strip()
        vision_signal_available = bool(pre_hash or post_hash or post_text or screenshot_path)

        if app_name or window_title:
            checks.append(
                {
                    "name": "window_match",
                    "passed": bool(active_match or (action == "launch" and window_present)),
                    "expected": window_title or app_name,
                    "observed": str(post_active.get("title", "") or post_active.get("process_name", "") or ""),
                }
            )
        if action in {"click", "click_and_type", "type", "hotkey", *WORKFLOW_ACTIONS}:
            checks.append(
                {
                    "name": "screen_changed",
                    "passed": bool(screen_changed or click_changed),
                    "pre_hash": pre_hash,
                    "post_hash": post_hash,
                }
            )
        if verify_text and action in {"click", "click_and_type", "type", "navigate", "search", "command", "quick_open", "workspace_search", "go_to_symbol", "rename_symbol", "terminal_command"}:
            checks.append(
                {
                    "name": "text_visible",
                    "passed": bool(text_visible),
                    "expected": verify_text[:120],
                    "was_visible_before": bool(pre_text_visible),
                }
            )
        if action in WORKFLOW_ACTIONS and probe_queries:
            checks.append(
                {
                    "name": "workflow_probe_match",
                    "passed": probe_match,
                    "queries": probe_queries,
                    "matched_before": pre_probe_match,
                    "matched_terms": [str(row.get("query", "") or "") for row in probe_matches if isinstance(row, dict)][:4],
                    "sources": self._dedupe_strings([str(item) for item in probe_sources if str(item).strip()]),
                }
            )
        if action == "observe":
            checks.append(
                {
                    "name": "observation_ready",
                    "passed": bool(screenshot_path or str(final_step.get("screenshot_path", "") or "").strip()),
                    "screenshot_path": screenshot_path or str(final_step.get("screenshot_path", "") or "").strip(),
                }
            )

        verified = False
        message = "desktop action executed"
        if action == "launch":
            verified = bool(active_match or window_present)
            message = "launch verified" if verified else "launch finished, but no matching window became available"
        elif action == "focus":
            verified = bool(active_match)
            message = "focus verified" if verified else "focus step completed, but the expected window is not active"
        elif action == "click":
            verified = bool(
                str(click_step.get("status", "") or "").strip().lower() == "success"
                and (bool(screen_changed or click_changed or text_visible) or str(args.get("verify_mode", "") or "").strip().lower() == "none")
            )
            if not verified and not vision_signal_available and str(click_step.get("status", "") or "").strip().lower() == "success":
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append("Visual verification was unavailable, so JARVIS accepted the successful click handler result as best-effort confirmation.")
            message = "click verified" if verified else "click finished, but no reliable post-click signal was observed"
        elif action == "type":
            verified = bool((screen_changed or text_visible) and (active_match or not (app_name or window_title)))
            if not verified and not vision_signal_available and str(type_step.get("status", "") or "").strip().lower() == "success":
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append("Visual verification was unavailable, so JARVIS accepted the successful typing step as best-effort confirmation.")
            message = "typing verified" if verified else "typing finished, but JARVIS could not confirm the text landed in the intended window"
        elif action == "click_and_type":
            verified = bool(
                str(click_step.get("status", "") or "").strip().lower() == "success"
                and (screen_changed or text_visible)
                and (active_match or not (app_name or window_title))
            )
            if (
                not verified
                and not vision_signal_available
                and str(click_step.get("status", "") or "").strip().lower() == "success"
                and str(type_step.get("status", "") or "").strip().lower() == "success"
            ):
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append("Visual verification was unavailable, so JARVIS accepted the successful click-and-type chain as best-effort confirmation.")
            message = "click-and-type verified" if verified else "click-and-type finished, but the follow-up state change could not be confirmed"
        elif action == "navigate":
            verified = bool(
                str(type_step.get("status", "") or "").strip().lower() == "success"
                and (workflow_state_signal or (active_match and not verify_text))
                and (active_match or not (app_name or window_title))
            )
            if not verified and not vision_signal_available and str(type_step.get("status", "") or "").strip().lower() == "success":
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append(str(self._workflow_definition(action).get("vision_warning", "") or "Visual verification was unavailable, so JARVIS accepted the successful workflow as best-effort confirmation."))
            message = str(
                self._workflow_definition(action).get(
                    "verification_success" if verified else "verification_failure",
                    "navigation verified" if verified else "navigation finished, but JARVIS could not confirm the destination was reached",
                )
                or ("navigation verified" if verified else "navigation finished, but JARVIS could not confirm the destination was reached")
            )
        elif action == "search":
            verified = bool(
                str(type_step.get("status", "") or "").strip().lower() == "success"
                and workflow_state_signal
                and (active_match or not (app_name or window_title))
            )
            if not verified and not vision_signal_available and str(type_step.get("status", "") or "").strip().lower() == "success":
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append(str(self._workflow_definition(action).get("vision_warning", "") or "Visual verification was unavailable, so JARVIS accepted the successful workflow as best-effort confirmation."))
            message = str(
                self._workflow_definition(action).get(
                    "verification_success" if verified else "verification_failure",
                    "search verified" if verified else "search finished, but the follow-up search state could not be confirmed",
                )
                or ("search verified" if verified else "search finished, but the follow-up search state could not be confirmed")
            )
        elif action == "command":
            verified = bool(
                str(type_step.get("status", "") or "").strip().lower() == "success"
                and workflow_state_signal
                and (active_match or not (app_name or window_title))
            )
            if not verified and not vision_signal_available and str(type_step.get("status", "") or "").strip().lower() == "success":
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append(str(self._workflow_definition(action).get("vision_warning", "") or "Visual verification was unavailable, so JARVIS accepted the successful workflow as best-effort confirmation."))
            message = str(
                self._workflow_definition(action).get(
                    "verification_success" if verified else "verification_failure",
                    "command verified" if verified else "command palette finished, but the resulting UI state could not be confirmed",
                )
                or ("command verified" if verified else "command palette finished, but the resulting UI state could not be confirmed")
            )
        elif action == "quick_open":
            verified = bool(
                str(type_step.get("status", "") or "").strip().lower() == "success"
                and workflow_state_signal
                and (active_match or not (app_name or window_title))
            )
            if not verified and not vision_signal_available and str(type_step.get("status", "") or "").strip().lower() == "success":
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append(str(self._workflow_definition(action).get("vision_warning", "") or "Visual verification was unavailable, so JARVIS accepted the successful workflow as best-effort confirmation."))
            message = str(
                self._workflow_definition(action).get(
                    "verification_success" if verified else "verification_failure",
                    "quick open verified" if verified else "quick open finished, but the requested target could not be confirmed",
                )
                or ("quick open verified" if verified else "quick open finished, but the requested target could not be confirmed")
            )
        elif action == "terminal_command":
            verified = bool(
                str(type_step.get("status", "") or "").strip().lower() == "success"
                and (workflow_state_signal or (active_match and not verify_text))
                and (active_match or not (app_name or window_title))
            )
            if not verified and not vision_signal_available and str(type_step.get("status", "") or "").strip().lower() == "success":
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append(str(self._workflow_definition(action).get("vision_warning", "") or "Visual verification was unavailable, so JARVIS accepted the successful workflow as best-effort confirmation."))
            message = str(
                self._workflow_definition(action).get(
                    "verification_success" if verified else "verification_failure",
                    "terminal command verified" if verified else "terminal command finished, but JARVIS could not confirm the command reached the intended terminal surface",
                )
                or ("terminal command verified" if verified else "terminal command finished, but JARVIS could not confirm the command reached the intended terminal surface")
            )
        elif action in WORKFLOW_ACTIONS and bool(self._workflow_definition(action).get("requires_input", False)):
            verified = bool(
                str(type_step.get("status", "") or "").strip().lower() == "success"
                and (workflow_state_signal or (active_match and not verify_text))
                and (active_match or not (app_name or window_title))
            )
            if not verified and not vision_signal_available and str(type_step.get("status", "") or "").strip().lower() == "success":
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append(str(self._workflow_definition(action).get("vision_warning", "") or "Visual verification was unavailable, so JARVIS accepted the successful workflow as best-effort confirmation."))
            message = str(
                self._workflow_definition(action).get(
                    "verification_success" if verified else "verification_failure",
                    "workflow verified" if verified else "workflow finished, but the target state could not be confirmed",
                )
                or ("workflow verified" if verified else "workflow finished, but the target state could not be confirmed")
            )
        elif action in WORKFLOW_ACTIONS and not bool(self._workflow_definition(action).get("requires_input", False)):
            workflow_definition = self._workflow_definition(action)
            workflow_action_name = str(workflow_definition.get("workflow_action", "") or "").strip().lower()
            workflow_step = self._find_step_result(results, workflow_action_name) if workflow_action_name else {}
            primary_step = workflow_step if workflow_step else self._find_step_result(results, "keyboard_hotkey")
            primary_step_status = str(primary_step.get("status", "") or "").strip().lower()
            self_verifying = bool(workflow_definition.get("self_verifying", False))
            surface_snapshot = advice.get("surface_snapshot", {}) if isinstance(advice.get("surface_snapshot", {}), dict) else {}
            surface_flags = surface_snapshot.get("surface_flags", {}) if isinstance(surface_snapshot.get("surface_flags", {}), dict) else {}
            surface_flag_name = str(workflow_definition.get("surface_flag", "") or "").strip()
            surface_ready_before = bool(surface_flags.get(surface_flag_name)) if surface_flag_name else False
            ready_surface_short_circuit = bool(
                workflow_definition.get("preserve_ready_surface", False)
                and workflow_definition.get("skip_hotkey_when_ready", False)
                and surface_ready_before
                and not primary_step
            )
            workflow_ready_signal = bool(workflow_surface_signal or (ready_surface_short_circuit and (probe_match or pre_probe_match or surface_ready_before)))
            verified = bool(
                (primary_step_status == "success" or ready_surface_short_circuit)
                and (workflow_ready_signal or (self_verifying and (active_match or not (app_name or window_title))))
                and (active_match or not (app_name or window_title))
            )
            if not verified and self_verifying and (primary_step_status == "success" or ready_surface_short_circuit):
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append(str(workflow_definition.get("vision_warning", "") or "Visual verification was unavailable, so JARVIS accepted the successful workflow as best-effort confirmation."))
            elif not verified and not vision_signal_available and (primary_step_status == "success" or ready_surface_short_circuit):
                verified = bool(active_match or not (app_name or window_title))
                if verified:
                    warnings.append(str(workflow_definition.get("vision_warning", "") or "Visual verification was unavailable, so JARVIS accepted the successful workflow as best-effort confirmation."))
            message = str(
                workflow_definition.get(
                    "verification_success" if verified else "verification_failure",
                    "workflow verified" if verified else "workflow finished, but the target state could not be confirmed",
                )
                or ("workflow verified" if verified else "workflow finished, but the target state could not be confirmed")
            )
        elif action == "hotkey":
            verified = bool(screen_changed or active_changed or (active_match if (app_name or window_title) else False))
            if not verified and not vision_signal_available and active_match:
                verified = True
                warnings.append("No visual change was available after the hotkey, so JARVIS relied on the focused target window match.")
            message = "hotkey verified" if verified else "hotkey finished, but no UI change was detected afterward"
        elif action == "observe":
            verified = bool(screenshot_path or str(final_step.get("screenshot_path", "") or "").strip())
            message = "observation verified" if verified else "observe returned, but no screenshot evidence was captured"
        else:
            verified = True

        if not verified and action in {"type", "click_and_type", "navigate", "search", "command", "quick_open", "workspace_search", "go_to_symbol", "rename_symbol", "terminal_command"} and verify_text and not text_visible:
            warnings.append(f"Expected text '{verify_text[:80]}' was not visible in the post-action OCR snapshot.")
        if not verified and action in WORKFLOW_ACTIONS and probe_queries and not probe_match:
            warnings.append("No workflow probe matched the expected post-action surface.")
        if not verified and action in {"click", "click_and_type", "hotkey", *WORKFLOW_ACTIONS} and not (screen_changed or click_changed or active_changed):
            warnings.append("No post-action screen hash change was detected.")
        status = "degraded" if verified and warnings else ("success" if verified else "failed")
        return {
            "enabled": True,
            "status": status,
            "verified": verified,
            "message": message,
            "checks": checks,
            "warnings": self._dedupe_strings(warnings),
            "verify_text": verify_text,
            "pre_context": {
                "active_window": pre_active,
                "screen_hash": pre_hash,
                "workflow_probe": pre_probe,
            },
            "post_context": {
                "active_window": post_active,
                "screen_hash": post_hash,
                "screenshot_path": screenshot_path or str(final_step.get("screenshot_path", "") or "").strip(),
                "workflow_probe": post_probe,
            },
        }

    def _verification_plan(
        self,
        *,
        args: Dict[str, Any],
        primary_candidate: Dict[str, Any],
        capabilities: Dict[str, Any],
        app_profile: Dict[str, Any],
    ) -> Dict[str, Any]:
        action = str(args.get("action", "observe") or "observe").strip().lower()
        verify_text = self._derive_verify_text(args=args, app_profile=app_profile)
        checks: List[str] = []
        if action in {"launch", "focus", "type", "click", "click_and_type", "hotkey", *WORKFLOW_ACTIONS} and (args.get("app_name") or args.get("window_title") or primary_candidate):
            checks.append("active_window_match")
        if action in {"click", "click_and_type", "type", "hotkey", *WORKFLOW_ACTIONS} and bool(capabilities.get("vision", {}).get("available")):
            checks.append("screen_hash_change")
        if verify_text and action in {"click", "click_and_type", "type", "navigate", "search", "command", "quick_open", "workspace_search", "go_to_symbol", "rename_symbol", "terminal_command"} and bool(capabilities.get("vision", {}).get("available")):
            checks.append("ocr_text_visibility")
        probe_plan = self._workflow_probe_queries(
            requested_action=action,
            args=args,
            advice={"app_profile": app_profile, "target_window": primary_candidate},
        )
        if action in WORKFLOW_ACTIONS and probe_plan:
            checks.append("workflow_probe_match")
        if action == "launch":
            checks.append("window_presence")
        if action == "observe":
            checks.append("screenshot_capture")
        return {
            "enabled": bool(args.get("verify_after_action", True)),
            "expected_window": str(args.get("window_title", "") or args.get("app_name", "") or ""),
            "verify_text": verify_text,
            "profile_id": str(app_profile.get("profile_id", "") or "").strip(),
            "profile_verify_text_source": str(
                (app_profile.get("verification_defaults", {}) if isinstance(app_profile.get("verification_defaults", {}), dict) else {}).get("verify_text_source", "")
                or ""
            ).strip(),
            "retry_on_verification_failure": bool(args.get("retry_on_verification_failure", True)),
            "max_strategy_attempts": max(1, min(int(args.get("max_strategy_attempts", 2) or 2), 4)),
            "checks": checks,
            "probe_plan": probe_plan,
        }

    def _build_strategy_variants(self, *, args: Dict[str, Any], capabilities: Dict[str, Any], app_profile: Dict[str, Any]) -> List[Dict[str, Any]]:
        action = str(args.get("action", "observe") or "observe").strip().lower()
        accessibility_ready = bool(capabilities.get("accessibility", {}).get("available")) if isinstance(capabilities.get("accessibility", {}), dict) else False
        vision_ready = bool(capabilities.get("vision", {}).get("available")) if isinstance(capabilities.get("vision", {}), dict) else False
        target_mode = str(args.get("target_mode", "auto") or "auto").strip().lower() or "auto"
        focus_retry_needed = bool((args.get("app_name") or args.get("window_title")) and not bool(args.get("focus_first", True)))
        capability_preferences = [
            str(item).strip().lower()
            for item in app_profile.get("capability_preferences", [])
            if str(item).strip()
        ]
        variants: List[Dict[str, Any]] = [
            {
                "strategy_id": "primary",
                "title": "Primary Route",
                "reason": "Run the advised desktop route first.",
                "payload_overrides": {},
            }
        ]
        if action in {"focus", "type", "hotkey"} and focus_retry_needed:
            variants.append(
                {
                    "strategy_id": "refocus_retry",
                    "title": "Refocus Retry",
                    "reason": "Retry after explicitly restoring focus to the target window.",
                    "payload_overrides": {"focus_first": True},
                }
            )
        if action in {"click", "click_and_type"}:
            if focus_retry_needed:
                variants.append(
                    {
                        "strategy_id": "refocus_primary_retry",
                        "title": "Refocus Primary Retry",
                        "reason": "Retry the routed click after re-focusing the target app.",
                        "payload_overrides": {"focus_first": True},
                    }
                )
            prefer_vision = capability_preferences[:1] == ["vision"]
            if prefer_vision and vision_ready and target_mode != "ocr":
                variants.append(
                    {
                        "strategy_id": "ocr_retry",
                        "title": "OCR Retry",
                        "reason": "Retry with OCR-only targeting in case accessibility coordinates drifted.",
                        "payload_overrides": {"target_mode": "ocr", "focus_first": True},
                    }
                )
            if accessibility_ready and target_mode != "accessibility":
                variants.append(
                    {
                        "strategy_id": "accessibility_retry",
                        "title": "Accessibility Retry",
                        "reason": "Retry with accessibility-only targeting for structured UI controls.",
                        "payload_overrides": {"target_mode": "accessibility", "focus_first": True},
                    }
                )
            if not prefer_vision and vision_ready and target_mode != "ocr":
                variants.append(
                    {
                        "strategy_id": "ocr_retry",
                        "title": "OCR Retry",
                        "reason": "Retry with OCR-only targeting in case accessibility coordinates drifted.",
                        "payload_overrides": {"target_mode": "ocr", "focus_first": True},
                    }
                )
        if action in WORKFLOW_ACTIONS:
            workflow_profile = self._workflow_profile(requested_action=action, args=args, app_profile=app_profile)
            hotkeys = workflow_profile.get("hotkeys", []) if isinstance(workflow_profile.get("hotkeys", []), list) else []
            definition = self._workflow_definition(action)
            if focus_retry_needed:
                variants.append(
                    {
                        "strategy_id": "workflow_refocus_retry",
                        "title": "Workflow Refocus Retry",
                        "reason": "Retry after explicitly restoring focus before replaying the desktop workflow.",
                        "payload_overrides": {"focus_first": True},
                    }
                )
            for index, hotkey in enumerate(hotkeys[1:], start=2):
                if not isinstance(hotkey, list) or not hotkey:
                    continue
                variants.append(
                    {
                        "strategy_id": f"workflow_retry_{index}",
                        "title": str(definition.get("retry_label", "Workflow Retry") or "Workflow Retry"),
                        "reason": str(definition.get("retry_reason", "Retry with an alternate workflow shortcut.") or "Retry with an alternate workflow shortcut."),
                        "payload_overrides": {"keys": list(hotkey), "focus_first": True},
                    }
                )
        deduped: List[Dict[str, Any]] = []
        seen: set[tuple] = set()
        for variant in variants:
            overrides = variant.get("payload_overrides", {}) if isinstance(variant.get("payload_overrides", {}), dict) else {}
            key = (
                str(variant.get("strategy_id", "") or "").strip().lower(),
                tuple(sorted((str(k), str(v)) for k, v in overrides.items())),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(variant)
        return deduped[: max(1, min(int(args.get("max_strategy_attempts", 2) or 2), 4))]

    def _build_execution_response(
        self,
        *,
        base_advice: Dict[str, Any],
        selected_attempt: Dict[str, Any],
        attempts: List[Dict[str, Any]],
        recovered: bool,
        status_override: str = "",
        message_override: str = "",
    ) -> Dict[str, Any]:
        advice = selected_attempt.get("advice", {}) if isinstance(selected_attempt.get("advice", {}), dict) else base_advice
        verification = selected_attempt.get("verification", {}) if isinstance(selected_attempt.get("verification", {}), dict) else {}
        return {
            "status": str(status_override or selected_attempt.get("status", "success") or "success"),
            "action": advice.get("action", base_advice.get("action", "")),
            "final_action": selected_attempt.get("final_action", advice.get("action", "")),
            "route_mode": advice.get("route_mode", base_advice.get("route_mode", "")),
            "confidence": advice.get("confidence", base_advice.get("confidence", 0.0)),
            "risk_level": advice.get("risk_level", base_advice.get("risk_level", "")),
            "app_profile": advice.get("app_profile", base_advice.get("app_profile", {})),
            "profile_defaults_applied": advice.get("profile_defaults_applied", base_advice.get("profile_defaults_applied", {})),
            "target_window": advice.get("target_window", base_advice.get("target_window", {})),
            "surface_snapshot": advice.get("surface_snapshot", base_advice.get("surface_snapshot", {})),
            "safety_signals": advice.get("safety_signals", base_advice.get("safety_signals", {})),
            "form_target_state": advice.get("form_target_state", base_advice.get("form_target_state", {})),
            "surface_branch": advice.get("surface_branch", base_advice.get("surface_branch", {})),
            "exploration_plan": advice.get("exploration_plan", base_advice.get("exploration_plan", {})),
            "exploration_selection": advice.get("exploration_selection", base_advice.get("exploration_selection", {})),
            "resume_action": advice.get("resume_action", base_advice.get("resume_action", "")),
            "resume_payload": advice.get("resume_payload", base_advice.get("resume_payload", {})),
            "resume_contract": advice.get("resume_contract", base_advice.get("resume_contract", {})),
            "blocking_surface": advice.get("blocking_surface", base_advice.get("blocking_surface", {})),
            "mission_record": selected_attempt.get("mission_record", advice.get("mission_record", base_advice.get("mission_record", {}))),
            "resume_context": selected_attempt.get("resume_context", advice.get("resume_context", base_advice.get("resume_context", {}))),
            "results": selected_attempt.get("results", []),
            "advice": advice,
            "verification": verification,
            "wizard_mission": selected_attempt.get("wizard_mission", {}),
            "form_mission": selected_attempt.get("form_mission", {}),
            "exploration_mission": selected_attempt.get("exploration_mission", {}),
            "attempts": attempts,
            "attempt_count": len(attempts),
            "executed_strategy": {
                "strategy_id": selected_attempt.get("strategy_id", ""),
                "title": selected_attempt.get("strategy_title", ""),
                "reason": selected_attempt.get("strategy_reason", ""),
                "recovered": bool(recovered),
            },
            "message": str(message_override or selected_attempt.get("message", "") or ""),
        }

    def _record_adaptive_strategy_outcome(
        self,
        *,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        strategy: Dict[str, Any],
        attempt_payload: Dict[str, Any],
    ) -> None:
        try:
            self._workflow_memory.record_outcome(
                action=str(args.get("action", "") or ""),
                args=args,
                app_profile=advice.get("app_profile", {}) if isinstance(advice.get("app_profile", {}), dict) else {},
                strategy=strategy,
                attempt=attempt_payload,
            )
        except Exception:  # noqa: BLE001
            return

    def app_profile_catalog(self, *, query: str = "", category: str = "", limit: int = 400) -> Dict[str, Any]:
        return self._app_profile_registry.catalog(query=query, category=category, limit=limit)

    def workflow_catalog(
        self,
        *,
        query: str = "",
        category: str = "",
        app_name: str = "",
        window_title: str = "",
        limit: int = 200,
    ) -> Dict[str, Any]:
        clean_query = " ".join(str(query or "").strip().lower().split())
        clean_category = " ".join(str(category or "").strip().lower().split())
        resolved_profile = self._app_profile_registry.match(app_name=app_name, window_title=window_title)
        profile = resolved_profile if resolved_profile.get("status") == "success" else {}
        items: List[Dict[str, Any]] = []
        for action in sorted(WORKFLOW_ACTIONS):
            definition = self._workflow_definition(action)
            category_hints = self._workflow_category_hints(definition)
            if clean_category and clean_category not in category_hints:
                if not (profile and clean_category == str(profile.get("category", "") or "").strip().lower()):
                    continue
            workflow_profile = self._workflow_profile(requested_action=action, args={"action": action}, app_profile=profile if profile else {})
            capability = (
                profile.get("workflow_capabilities", {}).get(action, {})
                if isinstance(profile.get("workflow_capabilities", {}), dict)
                and isinstance(profile.get("workflow_capabilities", {}).get(action, {}), dict)
                else {}
            )
            item = {
                "action": action,
                "title": str(definition.get("title", action.replace("_", " ").title()) or action.replace("_", " ").title()),
                "route_mode": str(definition.get("route_mode", "workflow_desktop") or "workflow_desktop"),
                "requires_input": bool(definition.get("requires_input", False)),
                "input_field": str(definition.get("input_field", "") or "").strip(),
                "required_fields": self._workflow_required_fields(requested_action=action),
                "input_sequence": [dict(row) for row in definition.get("input_sequence", []) if isinstance(row, dict)],
                "default_press_enter": bool(definition.get("default_press_enter", False)),
                "category_hints": category_hints,
                "support_message": str(definition.get("support_message", "") or ""),
                "verify_hint": str(definition.get("verify_hint", "") or ""),
                "supported": workflow_profile.get("supported") if workflow_profile else None,
                "primary_hotkey": workflow_profile.get("primary_hotkey", []) if workflow_profile else [],
                "alternate_hotkeys": workflow_profile.get("alternate_hotkeys", []) if workflow_profile else [],
                "supports_direct_input": bool(workflow_profile.get("supports_direct_input", False)) if workflow_profile else False,
                "supports_system_action": bool(workflow_profile.get("supports_system_action", False)) if workflow_profile else False,
                "supports_action_dispatch": bool(workflow_profile.get("supports_action_dispatch", False)) if workflow_profile else False,
                "supports_stateful_execution": bool(workflow_profile.get("supports_stateful_execution", False)) if workflow_profile else False,
                "probe_queries": workflow_profile.get("probe_queries", []) if workflow_profile else [],
                "recommended_followups": workflow_profile.get("recommended_followups", []) if workflow_profile else [],
                "capability": capability,
            }
            haystacks = [
                str(item.get("action", "") or ""),
                str(item.get("title", "") or ""),
                str(item.get("support_message", "") or ""),
            ]
            if clean_query and not any(clean_query in " ".join(str(value).strip().lower().split()) for value in haystacks):
                continue
            items.append(item)
        bounded = max(1, min(int(limit or 200), 2000))
        return {
            "status": "success",
            "count": min(len(items), bounded),
            "total": len(items),
            "items": items[:bounded],
            "profile": profile if profile else {},
            "filters": {
                "query": clean_query,
                "category": clean_category,
                "app_name": str(app_name or "").strip(),
                "window_title": str(window_title or "").strip(),
            },
        }

    def workflow_memory_snapshot(
        self,
        *,
        limit: int = 200,
        action: str = "",
        app_name: str = "",
        profile_id: str = "",
        intent: str = "",
    ) -> Dict[str, Any]:
        return self._workflow_memory.snapshot(
            limit=limit,
            action=action,
            app_name=app_name,
            profile_id=profile_id,
            intent=intent,
        )

    def workflow_memory_reset(
        self,
        *,
        action: str = "",
        app_name: str = "",
        profile_id: str = "",
        intent: str = "",
    ) -> Dict[str, Any]:
        return self._workflow_memory.reset(
            action=action,
            app_name=app_name,
            profile_id=profile_id,
            intent=intent,
        )

    def mission_memory_snapshot(
        self,
        *,
        limit: int = 200,
        mission_id: str = "",
        status: str = "",
        mission_kind: str = "",
        app_name: str = "",
        stop_reason_code: str = "",
    ) -> Dict[str, Any]:
        return self._mission_memory.snapshot(
            limit=limit,
            mission_id=mission_id,
            status=status,
            mission_kind=mission_kind,
            app_name=app_name,
            stop_reason_code=stop_reason_code,
        )

    def mission_memory_reset(
        self,
        *,
        mission_id: str = "",
        status: str = "",
        mission_kind: str = "",
        app_name: str = "",
    ) -> Dict[str, Any]:
        return self._mission_memory.reset(
            mission_id=mission_id,
            status=status,
            mission_kind=mission_kind,
            app_name=app_name,
        )

    def surface_snapshot(
        self,
        *,
        app_name: str = "",
        window_title: str = "",
        query: str = "",
        limit: int = 24,
        include_observation: bool = True,
        include_elements: bool = True,
        include_workflow_probes: bool = True,
        preferred_actions: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        args = self._normalize_payload(
            {
                "action": "observe",
                "app_name": app_name,
                "window_title": window_title,
                "query": query,
                "include_targets": False,
            }
        )
        app_profile = self._resolve_app_profile(args=args)
        args, defaults_applied = self._apply_profile_defaults(args=args, app_profile=app_profile)
        capabilities = self._capabilities()
        windows = self._list_windows()
        active_window = self._active_window()
        candidates = self._rank_window_candidates(
            windows=windows,
            active_window=active_window,
            app_name=str(args.get("app_name", "") or ""),
            window_title=str(args.get("window_title", "") or ""),
            app_profile=app_profile,
        )
        primary_candidate = candidates[0] if candidates else {}
        refined_profile = self._resolve_app_profile(args=args, primary_candidate=primary_candidate, active_window=active_window)
        if refined_profile.get("status") == "success" and refined_profile.get("profile_id") != app_profile.get("profile_id"):
            app_profile = refined_profile
            args, extra_defaults = self._apply_profile_defaults(args=args, app_profile=app_profile)
            defaults_applied.update(extra_defaults)
            candidates = self._rank_window_candidates(
                windows=windows,
                active_window=active_window,
                app_name=str(args.get("app_name", "") or ""),
                window_title=str(args.get("window_title", "") or ""),
                app_profile=app_profile,
            )
            primary_candidate = candidates[0] if candidates else {}

        bounded = max(1, min(int(limit or 24), 80))
        accessibility_ready = bool(capabilities.get("accessibility", {}).get("available")) if isinstance(capabilities.get("accessibility", {}), dict) else False
        vision_ready = bool(capabilities.get("vision", {}).get("available")) if isinstance(capabilities.get("vision", {}), dict) else False
        focus_title = str(args.get("window_title", "") or primary_candidate.get("title", "") or args.get("app_name", "") or "").strip()

        observation: Dict[str, Any] = {}
        if include_observation and vision_ready:
            observation = self._call("computer_observe", {"include_targets": False})

        element_rows: List[Dict[str, Any]] = []
        elements_result: Dict[str, Any] = {}
        elements_supplemented = False
        if include_elements and accessibility_ready:
            element_payload: Dict[str, Any] = {
                "max_elements": bounded,
                "include_descendants": True,
            }
            if focus_title:
                element_payload["window_title"] = focus_title
            if str(query or "").strip():
                element_payload["query"] = str(query).strip()
            elements_result = self._call("accessibility_list_elements", element_payload)
            raw_items = elements_result.get("items", []) if isinstance(elements_result.get("items", []), list) else []
            element_rows = [dict(item) for item in raw_items if isinstance(item, dict)][:bounded]
            if str(query or "").strip() and len(element_rows) < min(bounded, 24):
                supplemental_payload: Dict[str, Any] = {
                    "max_elements": bounded,
                    "include_descendants": True,
                }
                if focus_title:
                    supplemental_payload["window_title"] = focus_title
                supplemental_result = self._call("accessibility_list_elements", supplemental_payload)
                supplemental_items = supplemental_result.get("items", []) if isinstance(supplemental_result.get("items", []), list) else []
                merged_rows: List[Dict[str, Any]] = []
                seen_element_keys: set[str] = set()
                for row in [*element_rows, *[dict(item) for item in supplemental_items if isinstance(item, dict)]]:
                    element_key = self._element_identity_key(row)
                    if not element_key or element_key in seen_element_keys:
                        continue
                    seen_element_keys.add(element_key)
                    merged_rows.append(row)
                    if len(merged_rows) >= bounded:
                        break
                elements_supplemented = len(merged_rows) > len(element_rows)
                element_rows = merged_rows
        query_targets = self._query_target_elements(elements=element_rows, query=str(query or "").strip(), limit=5)
        target_control_state = query_targets[0] if query_targets else {}
        query_related_candidates = self._related_target_elements(elements=element_rows, target=target_control_state, limit=12)
        selection_candidates = self._selection_candidate_elements(elements=element_rows, limit=12)
        control_inventory = self._control_inventory(elements=element_rows)

        workflow_surfaces: List[Dict[str, Any]] = []
        if include_workflow_probes:
            workflow_capabilities = app_profile.get("workflow_capabilities", {}) if isinstance(app_profile.get("workflow_capabilities", {}), dict) else {}
            supported_actions = [
                str(action_name).strip().lower()
                for action_name, capability in workflow_capabilities.items()
                if str(action_name).strip()
                and isinstance(capability, dict)
                and bool(capability.get("supported", False))
                and str(action_name).strip().lower() in WORKFLOW_ACTIONS
            ]
            preferred = [
                str(action_name).strip().lower()
                for action_name in (preferred_actions or [])
                if str(action_name).strip().lower() in supported_actions
            ]
            normalized_probe_query = self._normalize_probe_text(query)
            preferred_order = {action_name: index for index, action_name in enumerate(preferred)}

            def _workflow_priority(action_name: str) -> tuple[int, int, str]:
                if action_name in preferred_order:
                    return (0, preferred_order[action_name], action_name)
                definition = self._workflow_definition(action_name)
                search_haystack = self._normalize_probe_text(
                    " ".join(
                        [
                            action_name,
                            str(definition.get("title", "") or ""),
                            str(definition.get("verify_hint", "") or ""),
                            str(definition.get("support_message", "") or ""),
                            " ".join(str(term).strip() for term in definition.get("probe_terms", []) if str(term).strip()),
                        ]
                    )
                )
                query_matched = bool(normalized_probe_query and normalized_probe_query in search_haystack)
                return (1 if query_matched else 2, 0, action_name)

            prioritized_actions: List[str] = []
            for action_name in sorted(supported_actions, key=_workflow_priority):
                if action_name and action_name not in prioritized_actions:
                    prioritized_actions.append(action_name)
            for action_name in prioritized_actions[: min(18, bounded)]:
                workflow_args: Dict[str, Any] = {
                    "action": action_name,
                    "app_name": str(args.get("app_name", "") or ""),
                    "window_title": str(args.get("window_title", "") or ""),
                }
                if str(query or "").strip():
                    workflow_args["query"] = str(query).strip()
                workflow_profile = self._workflow_profile(requested_action=action_name, args=workflow_args, app_profile=app_profile)
                probe_result = self._run_workflow_probes(
                    action=action_name,
                    args=workflow_args,
                    advice={"app_profile": app_profile, "target_window": primary_candidate},
                    capabilities=capabilities,
                )
                workflow_surfaces.append(
                    {
                        "action": action_name,
                        "title": str(workflow_profile.get("title", action_name.replace("_", " ").title()) or action_name.replace("_", " ").title()),
                        "supported": bool(workflow_profile.get("supported", False)),
                        "primary_hotkey": workflow_profile.get("primary_hotkey", []),
                        "probe_queries": workflow_profile.get("probe_queries", []),
                        "matched": bool(probe_result.get("matched", False)),
                        "match_count": len(probe_result.get("matches", [])) if isinstance(probe_result.get("matches", []), list) else 0,
                        "matches": probe_result.get("matches", []) if isinstance(probe_result.get("matches", []), list) else [],
                        "recommended_followups": workflow_profile.get("recommended_followups", []),
                    }
                )

        flags = self._surface_flags(
            app_profile=app_profile,
            workflow_surfaces=workflow_surfaces,
            observation=observation,
            active_window=active_window,
            target_window=primary_candidate,
            query=str(query or "").strip(),
            elements=element_rows,
        )
        safety_signals = self._surface_safety_signals(
            app_profile=app_profile,
            observation=observation,
            active_window=active_window,
            target_window=primary_candidate,
            elements=element_rows,
        )
        target_group_state = self._target_group_state(
            target=target_control_state,
            related_candidates=query_related_candidates,
            safety_signals=safety_signals,
        )
        wizard_page_state = self._wizard_page_state(
            observation=observation,
            elements=element_rows,
            safety_signals=safety_signals,
        )
        form_page_state = self._form_page_state(
            observation=observation,
            elements=element_rows,
            safety_signals=safety_signals,
            surface_flags=flags,
        )
        flags.update(
            {
                key: bool(safety_signals.get(key, False))
                for key in (
                    "dialog_visible",
                    "wizard_surface_visible",
                    "wizard_next_available",
                    "wizard_back_available",
                    "wizard_finish_available",
                    "warning_surface_visible",
                    "destructive_warning_visible",
                    "elevation_prompt_visible",
                    "permission_review_visible",
                    "requires_confirmation",
                    "dialog_review_required",
                    "authentication_prompt_visible",
                    "credential_prompt_visible",
                    "secure_desktop_likely",
                    "admin_approval_required",
                )
            }
        )
        recommended_actions = self._surface_recommendations(workflow_surfaces=workflow_surfaces)
        recommended_actions = [
            *self._surface_safety_recommendations(safety_signals=safety_signals),
            *recommended_actions,
        ]
        if bool(wizard_page_state) and bool(wizard_page_state.get("autonomous_progress_supported", False)):
            recommended_actions = ["complete_wizard_flow", *[action for action in recommended_actions if action != "complete_wizard_flow"]]
        if bool(wizard_page_state):
            recommended_actions = ["complete_wizard_page", *[action for action in recommended_actions if action != "complete_wizard_page"]]
        if bool(form_page_state) and bool(form_page_state.get("autonomous_progress_supported", False)):
            recommended_actions = ["complete_form_flow", *[action for action in recommended_actions if action != "complete_form_flow"]]
            recommended_actions = ["complete_form_page", *[action for action in recommended_actions if action != "complete_form_page"]]
        if bool(flags.get("sidebar_visible")) and not bool(flags.get("wizard_surface_visible")) and "focus_main_content" not in recommended_actions:
            recommended_actions = ["focus_main_content", *recommended_actions]
        surface_summary = self._surface_summary_from_snapshot(
            app_profile=app_profile,
            elements=element_rows,
            query=str(query or "").strip(),
            query_targets=query_targets,
            query_related_candidates=query_related_candidates,
            selection_candidates=selection_candidates,
            target_control_state=target_control_state,
            target_group_state=target_group_state,
            wizard_page_state=wizard_page_state,
            form_page_state=form_page_state,
            safety_signals=safety_signals,
            surface_flags=flags,
            recommended_actions=recommended_actions,
        )
        surface_intelligence = self._surface_intelligence.analyze(
            window=primary_candidate or active_window,
            surface_summary=surface_summary,
            visual_context=None,
            query=str(query or "").strip(),
        )
        recommended_actions = self._dedupe_strings(
            [
                *self._surface_intelligence_recommendations(
                    surface_intelligence=surface_intelligence,
                    app_profile=app_profile,
                    workflow_surfaces=workflow_surfaces,
                    surface_flags=flags,
                ),
                *recommended_actions,
            ]
        )[:10]
        return {
            "status": "success",
            "app_profile": app_profile if app_profile.get("status") == "success" else {},
            "profile_defaults_applied": defaults_applied,
            "capabilities": capabilities,
            "active_window": active_window,
            "target_window": primary_candidate,
            "candidate_windows": candidates[:6],
            "elements": {
                "status": str(elements_result.get("status", "skipped") or "skipped") if elements_result else ("success" if element_rows else "skipped"),
                "count": len(element_rows),
                "items": element_rows,
                "supplemented": elements_supplemented,
            },
            "query_targets": query_targets,
            "query_related_candidates": query_related_candidates,
            "selection_candidates": selection_candidates,
            "control_inventory": control_inventory,
            "target_control_state": target_control_state,
            "target_group_state": target_group_state,
            "wizard_page_state": wizard_page_state,
            "form_page_state": form_page_state,
            "dialog_state": safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {},
            "safety_signals": safety_signals,
            "observation": {
                "status": str(observation.get("status", "skipped") or "skipped") if observation else "skipped",
                "screen_hash": str(observation.get("screen_hash", "") or ""),
                "text": str(observation.get("text", "") or ""),
                "screenshot_path": str(observation.get("screenshot_path", "") or ""),
            },
            "workflow_surfaces": workflow_surfaces,
            "surface_summary": surface_summary,
            "surface_intelligence": surface_intelligence,
            "surface_flags": flags,
            "recommended_actions": recommended_actions,
            "filters": {
                "app_name": str(app_name or "").strip(),
                "window_title": str(window_title or "").strip(),
                "query": str(query or "").strip(),
                "limit": bounded,
                "include_observation": bool(include_observation),
                "include_elements": bool(include_elements),
                "include_workflow_probes": bool(include_workflow_probes),
            },
        }

    def _surface_summary_from_snapshot(
        self,
        *,
        app_profile: Dict[str, Any],
        elements: List[Dict[str, Any]],
        query: str,
        query_targets: List[Dict[str, Any]],
        query_related_candidates: List[Dict[str, Any]],
        selection_candidates: List[Dict[str, Any]],
        target_control_state: Dict[str, Any],
        target_group_state: Dict[str, Any],
        wizard_page_state: Dict[str, Any],
        form_page_state: Dict[str, Any],
        safety_signals: Dict[str, Any],
        surface_flags: Dict[str, Any],
        recommended_actions: List[str],
    ) -> Dict[str, Any]:
        from backend.python.tools.accessibility_tools import AccessibilityTools

        summary = AccessibilityTools.summarize_rows(
            rows=[dict(row) for row in elements if isinstance(row, dict)],
            window_title="",
            query=str(query or "").strip(),
            include_inventory=True,
        )
        summary = dict(summary) if isinstance(summary, dict) else {}
        base_flags = summary.get("surface_flags", {}) if isinstance(summary.get("surface_flags", {}), dict) else {}
        category = str(app_profile.get("category", "") or "").strip().lower()
        profile_markers = self._normalize_probe_text(
            " ".join(
                str(app_profile.get(key, "") or "").strip()
                for key in (
                    "name",
                    "display_name",
                    "app_name",
                    "package_family",
                    "package_id",
                    "bundle_id",
                    "exe",
                    "path",
                )
                if str(app_profile.get(key, "") or "").strip()
            )
        )
        element_markers = self._normalize_probe_text(
            " ".join(
                str(
                    row.get("root_window_title", "")
                    or row.get("window_title", "")
                    or row.get("name", "")
                    or row.get("automation_id", "")
                    or ""
                ).strip()
                for row in elements
                if isinstance(row, dict)
            )
        )
        likely_settings_surface = bool(
            base_flags.get("settings_surface_visible", False)
            or surface_flags.get("settings_window_ready", False)
            or "settings" in profile_markers
            or "microsoft windowssettings" in profile_markers
            or "immersivecontrolpanel" in profile_markers
            or "settings" in element_markers
        )
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        dialog_button_labels = [
            self._normalize_probe_text(item)
            for item in dialog_state.get("dialog_buttons", [])
            if str(item).strip()
        ]
        approval_or_review_dialog = bool(
            dialog_state.get("review_required", False)
            or dialog_state.get("approval_required", False)
            or dialog_state.get("admin_approval_required", False)
            or dialog_state.get("permission_review_required", False)
            or dialog_state.get("credential_required", False)
            or dialog_state.get("authentication_required", False)
            or dialog_state.get("manual_input_required", False)
            or dialog_state.get("secure_desktop_likely", False)
        )
        settings_commit_only_dialog = bool(
            likely_settings_surface
            and dialog_button_labels
            and set(dialog_button_labels).issubset({"apply", "save", "done", "submit"})
            and not approval_or_review_dialog
        )
        dialog_visible = bool(
            base_flags.get("dialog_visible", False)
            or surface_flags.get("dialog_visible", False)
            or safety_signals.get("dialog_visible", False)
        )
        strong_dialog_evidence = bool(
            surface_flags.get("context_menu_visible", False)
            or surface_flags.get("properties_dialog_visible", False)
            or surface_flags.get("print_dialog_visible", False)
            or bool(wizard_page_state)
            or approval_or_review_dialog
            or bool(dialog_state.get("destructive_buttons"))
            or len(dialog_button_labels) >= 2
            or (
                bool(safety_signals.get("dialog_visible", False))
                and not settings_commit_only_dialog
                and (
                    bool(dialog_button_labels)
                    or bool(dialog_state.get("confirmation_buttons"))
                    or str(dialog_state.get("preferred_confirmation_button", "") or "").strip()
                )
            )
            or str(dialog_state.get("preferred_dismiss_button", "") or "").strip()
        )
        if (
            dialog_visible
            and likely_settings_surface
            and not strong_dialog_evidence
        ):
            dialog_visible = False
        merged_flags = {
            **base_flags,
            "dialog_visible": dialog_visible,
            "navigation_tree_visible": bool(
                base_flags.get("navigation_tree_visible", False)
                or surface_flags.get("tree_visible", False)
                or surface_flags.get("folder_tree_visible", False)
                or surface_flags.get("navigation_tree_visible", False)
            ),
            "list_surface_visible": bool(
                base_flags.get("list_surface_visible", False)
                or surface_flags.get("list_visible", False)
                or surface_flags.get("file_list_visible", False)
                or surface_flags.get("message_list_visible", False)
            ),
            "data_table_visible": bool(base_flags.get("data_table_visible", False) or surface_flags.get("table_visible", False)),
            "tab_strip_visible": bool(
                base_flags.get("tab_strip_visible", False)
                or surface_flags.get("tab_strip_visible", False)
                or surface_flags.get("tab_page_visible", False)
            ),
            "toolbar_visible": bool(base_flags.get("toolbar_visible", False) or surface_flags.get("toolbar_visible", False)),
            "menu_visible": bool(base_flags.get("menu_visible", False) or surface_flags.get("context_menu_visible", False)),
            "form_surface_visible": bool(
                base_flags.get("form_surface_visible", False)
                or surface_flags.get("form_visible", False)
                or bool(form_page_state)
            ),
            "text_entry_surface_visible": bool(
                base_flags.get("text_entry_surface_visible", False)
                or surface_flags.get("input_field_visible", False)
                or surface_flags.get("rename_active", False)
            ),
            "selection_surface_visible": bool(
                base_flags.get("selection_surface_visible", False)
                or bool(query_targets)
                or bool(query_related_candidates)
                or bool(selection_candidates)
                or surface_flags.get("checkbox_visible", False)
                or surface_flags.get("radio_option_visible", False)
                or surface_flags.get("tab_page_visible", False)
            ),
            "value_control_visible": bool(
                base_flags.get("value_control_visible", False)
                or surface_flags.get("value_control_visible", False)
                or surface_flags.get("slider_visible", False)
                or surface_flags.get("spinner_visible", False)
            ),
            "scrollable_surface_visible": bool(
                base_flags.get("scrollable_surface_visible", False)
                or surface_flags.get("scrollbar_visible", False)
            ),
            "settings_surface_visible": bool(
                base_flags.get("settings_surface_visible", False)
                or surface_flags.get("settings_window_ready", False)
                or likely_settings_surface
            ),
            "search_surface_visible": bool(base_flags.get("search_surface_visible", False) or surface_flags.get("search_visible", False)),
        }

        combined_query_candidates: List[Dict[str, Any]] = []
        seen_candidates: set[str] = set()
        for row in [
            *[dict(item) for item in query_targets if isinstance(item, dict)],
            *[dict(item) for item in summary.get("query_candidates", []) if isinstance(item, dict)],
        ]:
            identity = self._element_identity_key(row) or "|".join(
                [
                    str(row.get("name", "") or "").strip().lower(),
                    str(row.get("control_type", "") or "").strip().lower(),
                    str(row.get("automation_id", "") or "").strip().lower(),
                ]
            )
            if not identity or identity in seen_candidates:
                continue
            seen_candidates.add(identity)
            combined_query_candidates.append(row)
            if len(combined_query_candidates) >= 8:
                break

        inventory_rows = [
            dict(row)
            for row in summary.get("control_inventory", [])
            if isinstance(row, dict)
        ]
        seen_inventory = {
            self._element_identity_key(row)
            or "|".join(
                [
                    str(row.get("name", "") or "").strip().lower(),
                    str(row.get("control_type", "") or "").strip().lower(),
                    str(row.get("automation_id", "") or "").strip().lower(),
                ]
            )
            for row in inventory_rows
        }
        for row in [
            *[dict(item) for item in query_related_candidates if isinstance(item, dict)],
            *[dict(item) for item in selection_candidates if isinstance(item, dict)],
            *([dict(target_control_state)] if isinstance(target_control_state, dict) and target_control_state else []),
        ]:
            identity = self._element_identity_key(row) or "|".join(
                [
                    str(row.get("name", "") or "").strip().lower(),
                    str(row.get("control_type", "") or "").strip().lower(),
                    str(row.get("automation_id", "") or "").strip().lower(),
                ]
            )
            if not identity or identity in seen_inventory:
                continue
            seen_inventory.add(identity)
            inventory_rows.append(
                {
                    "element_id": row.get("element_id", ""),
                    "name": row.get("name", ""),
                    "control_type": row.get("control_type", ""),
                    "automation_id": row.get("automation_id", ""),
                    "state_text": row.get("state_text", ""),
                    "root_window_title": row.get("root_window_title", row.get("window_title", "")),
                }
            )
            if len(inventory_rows) >= 40:
                break

        role_candidates = [
            str(item).strip()
            for item in summary.get("surface_role_candidates", [])
            if str(item).strip()
        ]
        priority_roles: List[str] = []
        if merged_flags.get("dialog_visible", False):
            priority_roles.append("dialog")
        if bool(wizard_page_state):
            priority_roles.append("wizard")
        if merged_flags.get("settings_surface_visible", False):
            priority_roles.append("settings")
        if category == "file_manager":
            priority_roles.append("file_manager")
        if category == "browser":
            priority_roles.append("browser")
        if category in {"code_editor", "ide"} and (
            merged_flags.get("text_entry_surface_visible", False)
            or merged_flags.get("form_surface_visible", False)
        ):
            priority_roles.append("editor")
        if category == "terminal":
            priority_roles.append("terminal")
        if merged_flags.get("data_table_visible", False):
            priority_roles.append("data_console")
        if merged_flags.get("navigation_tree_visible", False) and merged_flags.get("list_surface_visible", False):
            priority_roles.append("navigator")
        if merged_flags.get("form_surface_visible", False) and "dialog" not in priority_roles:
            priority_roles.append("form")
        role_candidates = self._dedupe_strings([*priority_roles, *role_candidates]) or ["content"]

        destructive_candidates = [
            str(item).strip()
            for item in summary.get("destructive_candidates", [])
            if str(item).strip()
        ]
        confirmation_candidates = [
            str(item).strip()
            for item in summary.get("confirmation_candidates", [])
            if str(item).strip()
        ]
        destructive_candidates.extend(
            str(item).strip()
            for item in dialog_state.get("destructive_buttons", [])
            if str(item).strip()
        )
        confirmation_candidates.extend(
            str(item).strip()
            for item in dialog_state.get("confirmation_buttons", [])
            if str(item).strip()
        )
        preferred_button = str(dialog_state.get("preferred_confirmation_button", "") or "").strip()
        if preferred_button:
            confirmation_candidates.append(preferred_button)
        recommended = self._dedupe_strings(
            [
                *[str(item).strip() for item in recommended_actions if str(item).strip()],
                *[str(item).strip() for item in summary.get("recommended_actions", []) if str(item).strip()],
            ]
        )
        summary_parts: List[str] = []
        if role_candidates:
            summary_parts.append(f"grounded as {role_candidates[0]}")
        if merged_flags.get("navigation_tree_visible", False):
            summary_parts.append("tree navigation available")
        if merged_flags.get("list_surface_visible", False):
            summary_parts.append("list targeting available")
        if merged_flags.get("data_table_visible", False):
            summary_parts.append("table navigation available")
        if merged_flags.get("form_surface_visible", False):
            summary_parts.append("form controls visible")
        if merged_flags.get("dialog_visible", False):
            summary_parts.append("dialog resolution likely")
        if combined_query_candidates:
            summary_parts.append(f"{len(combined_query_candidates)} query candidates visible")
        if not summary_parts:
            summary_parts.append("surface summary derived from current desktop state")

        return {
            "status": "success",
            "window_title_filter": "",
            "query": str(query or "").strip(),
            "element_count": int(summary.get("element_count", len(elements)) or len(elements)),
            "control_counts": summary.get("control_counts", {}) if isinstance(summary.get("control_counts", {}), dict) else {},
            "state_counts": summary.get("state_counts", {}) if isinstance(summary.get("state_counts", {}), dict) else {},
            "surface_flags": merged_flags,
            "surface_role_candidates": role_candidates,
            "actionable_candidate_count": int(summary.get("actionable_candidate_count", len(inventory_rows)) or len(inventory_rows)),
            "input_control_count": int(summary.get("input_control_count", 0) or 0),
            "selection_control_count": int(summary.get("selection_control_count", 0) or 0),
            "value_control_count": int(summary.get("value_control_count", 0) or 0),
            "top_labels": summary.get("top_labels", []) if isinstance(summary.get("top_labels", []), list) else [],
            "query_candidates": combined_query_candidates,
            "recommended_actions": recommended,
            "destructive_candidates": self._dedupe_strings(destructive_candidates)[:10],
            "confirmation_candidates": self._dedupe_strings(confirmation_candidates)[:10],
            "control_inventory": inventory_rows[:40],
            "summary": "; ".join(part for part in summary_parts if part).strip(),
            "target_group_state": dict(target_group_state) if isinstance(target_group_state, dict) else {},
        }

    def _surface_intelligence_recommendations(
        self,
        *,
        surface_intelligence: Dict[str, Any],
        app_profile: Dict[str, Any],
        workflow_surfaces: List[Dict[str, Any]],
        surface_flags: Dict[str, Any],
    ) -> List[str]:
        if not isinstance(surface_intelligence, dict) or not surface_intelligence:
            return []
        category = str(app_profile.get("category", "") or "").strip().lower()
        interaction_mode = self._normalize_probe_text(surface_intelligence.get("interaction_mode", ""))
        surface_role = self._normalize_probe_text(surface_intelligence.get("surface_role", ""))
        affordances = [
            self._normalize_probe_text(item)
            for item in surface_intelligence.get("affordances", [])
            if str(item).strip()
        ]
        query_resolution = (
            surface_intelligence.get("query_resolution", {})
            if isinstance(surface_intelligence.get("query_resolution", {}), dict)
            else {}
        )
        best_candidate_type = self._normalize_probe_text(query_resolution.get("best_candidate_type", ""))
        workflow_matched = any(
            isinstance(row, dict) and bool(row.get("matched", False))
            for row in workflow_surfaces
        )

        actions: List[str] = []
        if interaction_mode == "dialog_resolution" or surface_role == "dialog":
            actions.extend(["confirm_dialog", "dismiss_dialog"])
        if interaction_mode == "settings_navigation":
            actions.extend(["focus_sidebar", "focus_main_content"])
        if interaction_mode == "tree_list_navigation":
            actions.extend(["focus_navigation_tree", "focus_list_surface"])
        if interaction_mode == "table_navigation":
            actions.extend(["focus_data_table", "select_table_row"])
        if interaction_mode == "form_fill":
            actions.extend(["focus_form_surface", "focus_input_field"])
        if interaction_mode == "document_editing" and not surface_flags.get("main_content_visible", False):
            actions.append("focus_main_content")

        candidate_type_map = {
            "treeitem": "select_tree_item",
            "listitem": "select_list_item",
            "dataitem": "select_table_row",
            "row": "select_table_row",
            "checkbox": "focus_checkbox",
            "radiobutton": "select_radio_option",
            "tabitem": "select_tab_page",
            "combobox": "open_dropdown",
            "edit": "focus_input_field",
            "document": "focus_input_field",
            "button": "click",
            "hyperlink": "click",
        }
        mapped_candidate_action = candidate_type_map.get(best_candidate_type, "")
        if mapped_candidate_action:
            actions.append(mapped_candidate_action)

        generic_or_unsupported_surface = (
            category in {"", "utility", "general_desktop", "file_manager"}
            or surface_role in {"settings", "navigator", "form", "content"}
        )
        if generic_or_unsupported_surface and not workflow_matched:
            if "query_target_available" in affordances or "selection_targeting" in affordances:
                actions.append(EXPLORATION_ADVANCE_ACTION)
            if "scroll_search" in affordances or interaction_mode in {"settings_navigation", "tree_list_navigation", "form_fill", "table_navigation"}:
                actions.append(EXPLORATION_FLOW_ACTION)

        return self._dedupe_strings(actions)

    def surface_exploration_plan(
        self,
        *,
        app_name: str = "",
        window_title: str = "",
        query: str = "",
        limit: int = 8,
        include_observation: bool = True,
        include_elements: bool = True,
        include_workflow_probes: bool = True,
    ) -> Dict[str, Any]:
        snapshot = self.surface_snapshot(
            app_name=app_name,
            window_title=window_title,
            query=query,
            limit=max(12, limit * 2),
            include_observation=include_observation,
            include_elements=include_elements,
            include_workflow_probes=include_workflow_probes,
        )
        if not isinstance(snapshot, dict):
            return {"status": "error", "message": "invalid desktop surface exploration snapshot"}
        if str(snapshot.get("status", "") or "").strip().lower() != "success":
            return snapshot
        return self._surface_exploration_from_snapshot(
            snapshot=snapshot,
            app_name=app_name,
            window_title=window_title,
            query=query,
            limit=limit,
        )

    def _surface_exploration_from_snapshot(
        self,
        *,
        snapshot: Dict[str, Any],
        app_name: str = "",
        window_title: str = "",
        query: str = "",
        limit: int = 8,
    ) -> Dict[str, Any]:
        snapshot_payload = dict(snapshot) if isinstance(snapshot, dict) else {}
        bounded = max(1, min(int(limit or 8), 12))
        app_profile = snapshot_payload.get("app_profile", {}) if isinstance(snapshot_payload.get("app_profile", {}), dict) else {}
        target_window = snapshot_payload.get("target_window", {}) if isinstance(snapshot_payload.get("target_window", {}), dict) else {}
        surface_flags = snapshot_payload.get("surface_flags", {}) if isinstance(snapshot_payload.get("surface_flags", {}), dict) else {}
        safety_signals = snapshot_payload.get("safety_signals", {}) if isinstance(snapshot_payload.get("safety_signals", {}), dict) else {}
        surface_intelligence = (
            snapshot_payload.get("surface_intelligence", {})
            if isinstance(snapshot_payload.get("surface_intelligence", {}), dict)
            else {}
        )
        clean_query = str(query or snapshot_payload.get("filters", {}).get("query", "") or "").strip()
        surface_mode = self._surface_exploration_surface_mode(
            app_profile=app_profile,
            surface_flags=surface_flags,
            safety_signals=safety_signals,
            snapshot=snapshot_payload,
            surface_intelligence=surface_intelligence,
        )
        top_hypotheses = self._surface_exploration_hypotheses(
            snapshot=snapshot_payload,
            app_name=app_name,
            window_title=window_title,
            query=clean_query,
            surface_mode=surface_mode,
            limit=bounded,
            surface_intelligence=surface_intelligence,
        )
        branch_actions = self._surface_exploration_branch_actions(
            snapshot=snapshot_payload,
            app_name=app_name,
            window_title=window_title,
            query=clean_query,
            limit=bounded,
        )
        top_path = top_hypotheses[0].get("recommended_path", []) if top_hypotheses else []
        manual_attention_signals = [
            signal_name
            for signal_name in (
                "admin_approval_required",
                "secure_desktop_likely",
                "dialog_review_required",
                "permission_review_visible",
                "elevation_prompt_visible",
                "credential_prompt_visible",
                "authentication_prompt_visible",
                "destructive_warning_visible",
            )
            if bool(safety_signals.get(signal_name, False))
        ]
        risk_flags = [
            self._normalize_probe_text(item)
            for item in surface_intelligence.get("risk_flags", [])
            if str(item).strip()
        ]
        if "destructive_controls_visible" in risk_flags and "destructive_controls_visible" not in manual_attention_signals:
            manual_attention_signals.append("destructive_controls_visible")
        if "approval_or_credential_surface" in risk_flags and "approval_or_credential_surface" not in manual_attention_signals:
            manual_attention_signals.append("approval_or_credential_surface")
        manual_attention_required = bool(manual_attention_signals)
        automation_ready = bool(top_hypotheses or branch_actions) and not manual_attention_required
        profile_name = str(app_profile.get("name", "") or target_window.get("title", "") or app_name or "").strip()
        category = str(app_profile.get("category", "") or "").strip().lower()
        summary_parts: List[str] = []
        grounded_role = str(surface_intelligence.get("surface_role", "") or "").strip()
        interaction_mode = str(surface_intelligence.get("interaction_mode", "") or "").strip()
        grounding_confidence = float(surface_intelligence.get("grounding_confidence", 0.0) or 0.0)
        if grounded_role or interaction_mode:
            summary_parts.append(
                "Grounded as "
                + (grounded_role or "surface")
                + (f" with {interaction_mode}" if interaction_mode else "")
                + f" ({grounding_confidence:.2f})."
            )
        if top_hypotheses:
            primary_label = str(top_hypotheses[0].get("label", "") or "target").strip()
            primary_action = str(top_hypotheses[0].get("suggested_action", "") or "action").strip()
            summary_parts.append(f"Top target: {primary_label} via {primary_action}.")
        if branch_actions:
            summary_parts.append(f"{len(branch_actions)} follow-up branch action{'s' if len(branch_actions) != 1 else ''} available.")
        if manual_attention_required:
            summary_parts.append("Manual review is still recommended before autonomous continuation.")
        recovery_hints = [
            str(item).strip()
            for item in surface_intelligence.get("recovery_hints", [])
            if str(item).strip()
        ]
        if recovery_hints:
            summary_parts.append(recovery_hints[0][0:180])
        if not summary_parts:
            summary_parts.append("Surface recon found only low-confidence generic candidates.")
        return {
            "status": "success",
            "profile_name": profile_name,
            "category": category,
            "surface_mode": surface_mode,
            "surface_intelligence": surface_intelligence,
            "automation_ready": automation_ready,
            "manual_attention_required": manual_attention_required,
            "attention_signals": manual_attention_signals,
            "hypothesis_count": len(top_hypotheses),
            "branch_action_count": len(branch_actions),
            "top_hypotheses": top_hypotheses,
            "branch_actions": branch_actions,
            "top_path": top_path,
            "surface_snapshot": snapshot_payload,
            "filters": {
                "app_name": str(app_name or snapshot_payload.get("filters", {}).get("app_name", "") or "").strip(),
                "window_title": str(window_title or snapshot_payload.get("filters", {}).get("window_title", "") or "").strip(),
                "query": clean_query,
                "limit": bounded,
            },
            "message": " ".join(part for part in summary_parts if part).strip(),
        }

    def _surface_exploration_signature(self, *, exploration_plan: Dict[str, Any]) -> str:
        if not isinstance(exploration_plan, dict) or not exploration_plan:
            return ""
        snapshot = (
            exploration_plan.get("surface_snapshot", {})
            if isinstance(exploration_plan.get("surface_snapshot", {}), dict)
            else {}
        )
        target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
        active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
        observation = snapshot.get("observation", {}) if isinstance(snapshot.get("observation", {}), dict) else {}
        top_hypothesis = (
            exploration_plan.get("top_hypotheses", [])[0]
            if isinstance(exploration_plan.get("top_hypotheses", []), list) and exploration_plan.get("top_hypotheses", [])
            else {}
        )
        top_hypothesis = top_hypothesis if isinstance(top_hypothesis, dict) else {}
        top_branch = (
            exploration_plan.get("branch_actions", [])[0]
            if isinstance(exploration_plan.get("branch_actions", []), list) and exploration_plan.get("branch_actions", [])
            else {}
        )
        top_branch = top_branch if isinstance(top_branch, dict) else {}
        signature_parts = [
            str(exploration_plan.get("surface_mode", "") or "").strip().lower(),
            str(target_window.get("title", "") or active_window.get("title", "") or "").strip().lower(),
            str(target_window.get("hwnd", 0) or active_window.get("hwnd", 0) or 0),
            str(observation.get("screen_hash", "") or "").strip().lower(),
            str(top_hypothesis.get("candidate_id", "") or "").strip().lower(),
            str(top_hypothesis.get("suggested_action", "") or "").strip().lower(),
            str(top_branch.get("action", "") or "").strip().lower(),
            str(exploration_plan.get("filters", {}).get("query", "") or "").strip().lower()
            if isinstance(exploration_plan.get("filters", {}), dict)
            else "",
        ]
        return hashlib.sha1("|".join(signature_parts).encode("utf-8")).hexdigest()[:16]

    def _surface_exploration_state_summary(self, *, exploration_plan: Dict[str, Any]) -> Dict[str, Any]:
        plan = dict(exploration_plan) if isinstance(exploration_plan, dict) else {}
        snapshot = plan.get("surface_snapshot", {}) if isinstance(plan.get("surface_snapshot", {}), dict) else {}
        target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
        active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
        observation = snapshot.get("observation", {}) if isinstance(snapshot.get("observation", {}), dict) else {}
        safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        form_page_state = snapshot.get("form_page_state", {}) if isinstance(snapshot.get("form_page_state", {}), dict) else {}
        wizard_page_state = snapshot.get("wizard_page_state", {}) if isinstance(snapshot.get("wizard_page_state", {}), dict) else {}
        breadcrumb_path = [
            str(item).strip()
            for item in form_page_state.get("breadcrumb_path", [])
            if str(item).strip()
        ] if isinstance(form_page_state.get("breadcrumb_path", []), list) else []
        if not breadcrumb_path:
            breadcrumb_path = [
                str(item).strip()
                for item in wizard_page_state.get("breadcrumb_path", [])
                if str(item).strip()
            ] if isinstance(wizard_page_state.get("breadcrumb_path", []), list) else []
        selected_navigation_target = str(
            form_page_state.get("selected_navigation_target", "")
            or wizard_page_state.get("selected_navigation_target", "")
            or ""
        ).strip()
        selected_tab = str(
            form_page_state.get("selected_tab", "")
            or wizard_page_state.get("selected_tab", "")
            or ""
        ).strip()
        surface_path = self._dedupe_strings(
            [
                *breadcrumb_path,
                selected_navigation_target,
                selected_tab,
            ]
        )[:8]
        return {
            "surface_mode": str(plan.get("surface_mode", "") or "").strip(),
            "screen_hash": str(observation.get("screen_hash", "") or "").strip(),
            "window_title": str(target_window.get("title", "") or active_window.get("title", "") or "").strip(),
            "window_hwnd": int(target_window.get("hwnd", 0) or active_window.get("hwnd", 0) or 0),
            "dialog_visible": bool(dialog_state.get("visible", False) or safety_signals.get("dialog_visible", False)),
            "dialog_kind": str(dialog_state.get("dialog_kind", "") or "").strip(),
            "approval_kind": str(dialog_state.get("approval_kind", "") or "").strip(),
            "selected_navigation_target": selected_navigation_target,
            "selected_tab": selected_tab,
            "surface_path": surface_path,
        }

    def _surface_exploration_transition_summary(
        self,
        *,
        before_plan: Dict[str, Any],
        after_plan: Dict[str, Any],
    ) -> Dict[str, Any]:
        before_state = self._surface_exploration_state_summary(exploration_plan=before_plan)
        after_state = self._surface_exploration_state_summary(exploration_plan=after_plan)
        before_window_title = str(before_state.get("window_title", "") or "").strip()
        after_window_title = str(after_state.get("window_title", "") or "").strip()
        before_window_hwnd = int(before_state.get("window_hwnd", 0) or 0)
        after_window_hwnd = int(after_state.get("window_hwnd", 0) or 0)
        window_title_changed = bool(
            self._normalize_probe_text(before_window_title) != self._normalize_probe_text(after_window_title)
        )
        window_hwnd_changed = bool(
            before_window_hwnd
            and after_window_hwnd
            and before_window_hwnd != after_window_hwnd
        )
        window_changed = bool(
            after_window_title
            and (window_hwnd_changed or window_title_changed)
        )
        before_path = [
            str(item).strip()
            for item in before_state.get("surface_path", [])
            if str(item).strip()
        ] if isinstance(before_state.get("surface_path", []), list) else []
        after_path = [
            str(item).strip()
            for item in after_state.get("surface_path", [])
            if str(item).strip()
        ] if isinstance(after_state.get("surface_path", []), list) else []
        path_changed = before_path != after_path
        drilldown_progressed = bool(
            path_changed
            and after_path
            and (
                not before_path
                or (
                    len(after_path) >= len(before_path)
                    and after_path[: len(before_path)] == before_path
                )
            )
        )
        pane_shift = bool(
            self._normalize_probe_text(before_state.get("selected_navigation_target", ""))
            != self._normalize_probe_text(after_state.get("selected_navigation_target", ""))
            or self._normalize_probe_text(before_state.get("selected_tab", ""))
            != self._normalize_probe_text(after_state.get("selected_tab", ""))
        )
        dialog_shift = bool(
            bool(before_state.get("dialog_visible", False)) != bool(after_state.get("dialog_visible", False))
            or self._normalize_probe_text(before_state.get("dialog_kind", ""))
            != self._normalize_probe_text(after_state.get("dialog_kind", ""))
            or self._normalize_probe_text(before_state.get("approval_kind", ""))
            != self._normalize_probe_text(after_state.get("approval_kind", ""))
        )
        surface_mode_changed = bool(
            self._normalize_probe_text(before_state.get("surface_mode", ""))
            != self._normalize_probe_text(after_state.get("surface_mode", ""))
        )
        screen_hash_changed = bool(
            str(before_state.get("screen_hash", "") or "").strip()
            and str(after_state.get("screen_hash", "") or "").strip()
            and str(before_state.get("screen_hash", "") or "").strip()
            != str(after_state.get("screen_hash", "") or "").strip()
        )
        transition_kind = "steady_state"
        if window_changed:
            transition_kind = "child_window"
        elif dialog_shift:
            transition_kind = "dialog_shift"
        elif drilldown_progressed:
            transition_kind = "drilldown"
        elif pane_shift:
            transition_kind = "pane_shift"
        elif path_changed or surface_mode_changed or screen_hash_changed:
            transition_kind = "surface_shift"
        return {
            "transition_kind": transition_kind,
            "nested_surface_progressed": bool(transition_kind != "steady_state"),
            "child_window_adopted": bool(window_changed),
            "window_title_before": before_window_title,
            "window_title_after": after_window_title,
            "window_hwnd_before": before_window_hwnd,
            "window_hwnd_after": after_window_hwnd,
            "surface_path_before": before_path,
            "surface_path_after": after_path,
            "path_changed": path_changed,
            "screen_hash_changed": screen_hash_changed,
            "surface_mode_changed": surface_mode_changed,
            "dialog_shift": dialog_shift,
            "pane_shift": pane_shift,
            "drilldown_progressed": drilldown_progressed,
        }

    def _surface_exploration_selection_key(
        self,
        *,
        kind: str = "",
        candidate_id: str = "",
        selected_action: str = "",
        label: str = "",
    ) -> str:
        normalized_kind = self._normalize_probe_text(kind)
        normalized_candidate_id = str(candidate_id or "").strip().lower()
        normalized_action = self._normalize_probe_text(selected_action)
        normalized_label = self._normalize_probe_text(label)
        return "|".join(
            (
                normalized_kind or "target",
                normalized_candidate_id,
                normalized_action,
                normalized_label,
            )
        ).strip("|")

    def _normalize_surface_exploration_attempt_entry(self, value: Any) -> Dict[str, Any]:
        row = dict(value) if isinstance(value, dict) else {}
        if not row:
            return {}
        kind = str(row.get("kind", "") or "").strip().lower()
        candidate_id = str(row.get("candidate_id", "") or "").strip()
        selected_action = str(row.get("selected_action", "") or "").strip().lower()
        label = str(row.get("label", "") or row.get("selected_candidate_label", "") or "").strip()
        selection_key = self._surface_exploration_selection_key(
            kind=kind,
            candidate_id=candidate_id,
            selected_action=selected_action,
            label=label,
        )
        if not selection_key:
            return {}
        return {
            "kind": kind,
            "candidate_id": candidate_id,
            "selected_action": selected_action,
            "label": label,
            "selection_key": selection_key,
            "status": str(row.get("status", "") or "").strip().lower(),
            "progressed": bool(row.get("progressed", False)),
            "transition_kind": str(row.get("transition_kind", "") or "").strip().lower(),
            "nested_surface_progressed": bool(
                row.get("nested_surface_progressed", row.get("progressed", False))
            ),
            "child_window_adopted": bool(row.get("child_window_adopted", False)),
            "step_index": max(0, int(row.get("step_index", 0) or 0)),
            "surface_signature_before": str(row.get("surface_signature_before", "") or "").strip(),
            "surface_signature_after": str(row.get("surface_signature_after", "") or "").strip(),
            "window_title_before": str(row.get("window_title_before", "") or "").strip(),
            "window_title_after": str(row.get("window_title_after", "") or "").strip(),
            "surface_path_before": [
                str(item).strip()
                for item in row.get("surface_path_before", [])
                if str(item).strip()
            ][:8] if isinstance(row.get("surface_path_before", []), list) else [],
            "surface_path_after": [
                str(item).strip()
                for item in row.get("surface_path_after", [])
                if str(item).strip()
            ][:8] if isinstance(row.get("surface_path_after", []), list) else [],
        }

    def _surface_exploration_attempt_history(self, *, args: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(args, dict):
            return []
        rows = args.get("attempted_targets", [])
        if not isinstance(rows, list):
            return []
        normalized_rows: List[Dict[str, Any]] = []
        for row in rows:
            normalized = self._normalize_surface_exploration_attempt_entry(row)
            if normalized:
                normalized_rows.append(normalized)
        return normalized_rows[:24]

    def _surface_exploration_signature_history(self, *, args: Dict[str, Any]) -> List[str]:
        if not isinstance(args, dict):
            return []
        rows = args.get("surface_signature_history", [])
        if not isinstance(rows, list):
            return []
        return [str(item).strip() for item in rows if str(item).strip()][:24]

    def _merge_surface_exploration_attempt_history(
        self,
        *,
        attempted_targets: List[Dict[str, Any]],
        new_entry: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in attempted_targets if isinstance(row, dict)]
        normalized = self._normalize_surface_exploration_attempt_entry(new_entry)
        if not normalized:
            return rows[:24]
        selection_key = str(normalized.get("selection_key", "") or "").strip()
        filtered = [
            dict(row)
            for row in rows
            if str(row.get("selection_key", "") or "").strip() != selection_key
        ]
        filtered.append(normalized)
        return filtered[-24:]

    @staticmethod
    def _merge_surface_signature_history(
        *,
        existing: List[str],
        additions: List[str],
    ) -> List[str]:
        merged: List[str] = []
        for value in [*existing, *additions]:
            clean = str(value or "").strip()
            if clean and clean not in merged:
                merged.append(clean)
        return merged[-24:]

    def _surface_exploration_selection_rows(
        self,
        *,
        exploration_plan: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        plan = dict(exploration_plan) if isinstance(exploration_plan, dict) else {}
        rows: List[Dict[str, Any]] = []
        for row in plan.get("top_hypotheses", []) if isinstance(plan.get("top_hypotheses", []), list) else []:
            if not isinstance(row, dict):
                continue
            action_payload = dict(row.get("action_payload", {})) if isinstance(row.get("action_payload", {}), dict) else {}
            action_name = str(action_payload.get("action", "") or row.get("suggested_action", "")).strip().lower()
            if not action_payload or not action_name:
                continue
            score = float(row.get("confidence", row.get("score", 0.0)) or 0.0)
            if bool(row.get("already_active", False)):
                score -= 0.08
            label = str(row.get("label", "") or "").strip()
            candidate_id = str(row.get("candidate_id", "") or "").strip()
            rows.append(
                {
                    "kind": "hypothesis",
                    "candidate_id": candidate_id,
                    "label": label,
                    "selected_action": action_name,
                    "selection_key": self._surface_exploration_selection_key(
                        kind="hypothesis",
                        candidate_id=candidate_id,
                        selected_action=action_name,
                        label=label,
                    ),
                    "confidence": round(max(0.0, score), 4),
                    "reason": str(row.get("reason", "") or "").strip(),
                    "action_payload": action_payload,
                    "raw": row,
                }
            )
        for row in plan.get("branch_actions", []) if isinstance(plan.get("branch_actions", []), list) else []:
            if not isinstance(row, dict):
                continue
            action_payload = dict(row.get("action_payload", {})) if isinstance(row.get("action_payload", {}), dict) else {}
            action_name = str(action_payload.get("action", "") or row.get("action", "")).strip().lower()
            if not action_payload or not action_name:
                continue
            score = float(row.get("confidence", 0.0) or 0.0) + (0.05 if bool(row.get("matched", False)) else 0.0)
            label = str(row.get("title", "") or row.get("action", "") or "").strip()
            rows.append(
                {
                    "kind": "branch_action",
                    "candidate_id": "",
                    "label": label,
                    "selected_action": action_name,
                    "selection_key": self._surface_exploration_selection_key(
                        kind="branch_action",
                        candidate_id="",
                        selected_action=action_name,
                        label=label,
                    ),
                    "confidence": round(max(0.0, score), 4),
                    "reason": str(row.get("reason", "") or "").strip(),
                    "action_payload": action_payload,
                    "raw": row,
                }
            )
        return rows

    def _surface_exploration_remaining_options(
        self,
        *,
        exploration_plan: Dict[str, Any],
        attempted_targets: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        selection_rows = self._surface_exploration_selection_rows(exploration_plan=exploration_plan)
        attempted_keys = {
            str(row.get("selection_key", "") or "").strip()
            for row in attempted_targets
            if str(row.get("selection_key", "") or "").strip()
        }
        remaining_rows = [
            dict(row)
            for row in selection_rows
            if str(row.get("selection_key", "") or "").strip() not in attempted_keys
        ]
        remaining_rows.sort(
            key=lambda row: (
                -float(row.get("confidence", 0.0) or 0.0),
                0 if row.get("kind") == "hypothesis" else 1,
                str(row.get("label", "") or "").lower(),
            )
        )
        return {
            "remaining_rows": remaining_rows[:12],
            "remaining_target_count": len(remaining_rows),
            "remaining_hypothesis_count": sum(1 for row in remaining_rows if row.get("kind") == "hypothesis"),
            "remaining_branch_action_count": sum(1 for row in remaining_rows if row.get("kind") == "branch_action"),
        }

    def _select_surface_exploration_target(
        self,
        *,
        exploration_plan: Dict[str, Any],
        args: Dict[str, Any],
    ) -> Dict[str, Any]:
        plan = dict(exploration_plan) if isinstance(exploration_plan, dict) else {}
        attention_signals = [
            str(item).strip()
            for item in plan.get("attention_signals", [])
            if str(item).strip()
        ] if isinstance(plan.get("attention_signals", []), list) else []
        if bool(plan.get("manual_attention_required", False)):
            message = (
                str(plan.get("message", "") or "").strip()
                or "The current surface still needs manual review before JARVIS should continue exploring it."
            )
            blocker = "Manual review is still required before autonomous surface exploration can continue."
            return {
                "status": "blocked",
                "stop_reason_code": "exploration_manual_review_required",
                "message": message,
                "warnings": [message] if message else [],
                "blockers": [blocker],
                "attention_signals": attention_signals,
            }

        explicit_candidate_id = str(args.get("candidate_id", "") or "").strip()
        explicit_branch_action = self._normalize_probe_text(args.get("branch_action", ""))
        attempted_targets = self._surface_exploration_attempt_history(args=args)
        attempted_keys = {
            str(row.get("selection_key", "") or "").strip()
            for row in attempted_targets
            if str(row.get("selection_key", "") or "").strip()
        }
        selection_rows = self._surface_exploration_selection_rows(exploration_plan=plan)

        matched_selection: Dict[str, Any] = {}
        if explicit_candidate_id:
            matched_selection = next(
                (
                    row
                    for row in selection_rows
                    if row.get("kind") == "hypothesis"
                    and str(row.get("candidate_id", "") or "").strip() == explicit_candidate_id
                ),
                {},
            )
        elif explicit_branch_action:
            matched_selection = next(
                (
                    row
                    for row in selection_rows
                    if row.get("kind") == "branch_action"
                    and (
                        self._normalize_probe_text(row.get("selected_action", "")) == explicit_branch_action
                        or self._normalize_probe_text(row.get("label", "")) == explicit_branch_action
                    )
                ),
                {},
            )
        if matched_selection:
            selection_rows = [matched_selection]
        else:
            if attempted_keys:
                untried_rows = [
                    dict(row)
                    for row in selection_rows
                    if str(row.get("selection_key", "") or "").strip() not in attempted_keys
                ]
                if untried_rows:
                    selection_rows = untried_rows
            selection_rows.sort(
                key=lambda row: (
                    -float(row.get("confidence", 0.0) or 0.0),
                    0 if row.get("kind") == "hypothesis" else 1,
                    str(row.get("label", "") or "").lower(),
                )
            )

        if not selection_rows:
            message = (
                str(plan.get("message", "") or "").strip()
                or "Surface recon did not find a safe next action to advance automatically."
            )
            return {
                "status": "blocked",
                "stop_reason_code": "exploration_no_safe_path",
                "message": message,
                "warnings": [message] if message else [],
                "blockers": ["No safe exploration target is ready for autonomous continuation on the current surface."],
                "attention_signals": attention_signals,
            }

        selected = dict(selection_rows[0])
        selected_payload = dict(selected.get("action_payload", {}))
        if not selected_payload:
            return {
                "status": "blocked",
                "stop_reason_code": "exploration_no_safe_path",
                "message": "Surface recon selected a target, but no executable payload could be built for it.",
                "warnings": [],
                "blockers": ["JARVIS could not build an executable payload for the selected surface target."],
                "attention_signals": attention_signals,
            }

        for field_name in (
            "ensure_app_launch",
            "focus_first",
            "press_enter",
            "target_mode",
            "verify_mode",
            "verify_after_action",
            "verify_text",
            "retry_on_verification_failure",
            "max_strategy_attempts",
        ):
            if field_name not in args:
                continue
            field_value = args.get(field_name)
            if field_name == "verify_text" and not str(field_value or "").strip():
                continue
            selected_payload[field_name] = field_value
        if not str(selected_payload.get("app_name", "") or "").strip() and str(args.get("app_name", "") or "").strip():
            selected_payload["app_name"] = str(args.get("app_name", "") or "").strip()
        if not str(selected_payload.get("window_title", "") or "").strip() and str(args.get("window_title", "") or "").strip():
            selected_payload["window_title"] = str(args.get("window_title", "") or "").strip()
        selected_payload["_provided_fields"] = self._dedupe_strings(
            [str(key).strip() for key in selected_payload.keys() if str(key).strip()]
        )
        selected["action_payload"] = selected_payload
        selected["status"] = "success"
        selected["stop_reason_code"] = ""
        selected["selection_key"] = self._surface_exploration_selection_key(
            kind=str(selected.get("kind", "") or "").strip(),
            candidate_id=str(selected.get("candidate_id", "") or "").strip(),
            selected_action=str(selected.get("selected_action", "") or "").strip(),
            label=str(selected.get("label", "") or "").strip(),
        )
        selected["attempted_target_count"] = len(attempted_keys)
        selected["message"] = str(selected.get("reason", "") or "").strip() or (
            f"Surface recon selected {selected.get('label', 'target')} via {selected.get('selected_action', 'action')}."
        )
        selected["warnings"] = []
        selected["blockers"] = []
        return selected

    def _surface_exploration_blocking_surface(
        self,
        *,
        exploration_plan: Dict[str, Any],
        stop_reason_code: str,
        selected: Dict[str, Any],
        attempted_targets: Optional[List[Dict[str, Any]]] = None,
        alternative_target_count: int = 0,
        alternative_hypothesis_count: int = 0,
        alternative_branch_action_count: int = 0,
    ) -> Dict[str, Any]:
        plan = dict(exploration_plan) if isinstance(exploration_plan, dict) else {}
        attempted_rows = [dict(row) for row in attempted_targets if isinstance(row, dict)] if isinstance(attempted_targets, list) else []
        snapshot = plan.get("surface_snapshot", {}) if isinstance(plan.get("surface_snapshot", {}), dict) else {}
        target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
        active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
        observation = snapshot.get("observation", {}) if isinstance(snapshot.get("observation", {}), dict) else {}
        safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        form_page_state = snapshot.get("form_page_state", {}) if isinstance(snapshot.get("form_page_state", {}), dict) else {}
        wizard_page_state = snapshot.get("wizard_page_state", {}) if isinstance(snapshot.get("wizard_page_state", {}), dict) else {}
        recommended_actions = self._dedupe_strings(
            [
                str(selected.get("selected_action", "") or "").strip(),
                *[
                    str(row.get("action", "") or "").strip()
                    for row in plan.get("branch_actions", [])
                    if isinstance(row, dict) and str(row.get("action", "") or "").strip()
                ],
                "resume_mission",
            ]
        )
        operator_steps_map = {
            "exploration_followup_available": [
                "Review the refreshed recon summary if you want to sanity-check the next move.",
                "Resume the paused exploration mission to let JARVIS take the next bounded surface step.",
            ],
            "exploration_step_limit_reached": [
                "Review the refreshed recon summary if you want to sanity-check the next bounded move.",
                "Resume the paused exploration flow mission to let JARVIS continue exploring this app in another bounded wave.",
            ],
            "exploration_no_safe_path": [
                "Inspect the current window and decide what surface JARVIS should target next.",
                "Use a more explicit query or manual steering before resuming automated exploration.",
            ],
            "exploration_no_progress": [
                "The same surface target is still leading the recon loop, so inspect the app state manually.",
                "Adjust the query or visible surface before resuming exploration.",
            ],
            "exploration_route_unavailable": [
                "The selected recon target could not be routed safely through the current automation capabilities.",
                "Inspect the app surface or adjust the control query before resuming exploration.",
            ],
            "exploration_manual_review_required": [
                "Review the current surface carefully before allowing more autonomous exploration.",
                "Clear the blocking review or approval surface manually if you want JARVIS to continue.",
            ],
        }
        notes = self._dedupe_strings(
            [
                *[
                    str(item).strip()
                    for item in plan.get("attention_signals", [])
                    if str(item).strip()
                ],
                str(plan.get("message", "") or "").strip(),
                str(selected.get("message", "") or "").strip(),
                (
                    f"{max(0, int(alternative_target_count or 0))} untried recon branch"
                    f"{'' if max(0, int(alternative_target_count or 0)) == 1 else 'es'} remain available."
                    if int(alternative_target_count or 0) > 0
                    else ""
                ),
            ]
        )
        pending_requirements = [
            dict(row)
            for row in form_page_state.get("pending_requirements", [])
            if isinstance(row, dict)
        ] if isinstance(form_page_state.get("pending_requirements", []), list) else []
        pending_requirements.extend(
            [
                dict(row)
                for row in wizard_page_state.get("pending_requirements", [])
                if isinstance(row, dict)
            ]
        )
        latest_attempt = dict(attempted_rows[-1]) if attempted_rows else {}
        transition_kind = str(latest_attempt.get("transition_kind", "") or "").strip().lower()
        nested_surface_progressed = bool(
            latest_attempt.get("nested_surface_progressed", latest_attempt.get("progressed", False))
        )
        child_window_adopted = bool(latest_attempt.get("child_window_adopted", False))
        surface_path_tail = [
            str(item).strip()
            for item in latest_attempt.get("surface_path_after", [])
            if str(item).strip()
        ] if isinstance(latest_attempt.get("surface_path_after", []), list) else []
        if not surface_path_tail:
            transition_state = self._surface_exploration_state_summary(exploration_plan=plan)
            surface_path_tail = [
                str(item).strip()
                for item in transition_state.get("surface_path", [])
                if str(item).strip()
            ] if isinstance(transition_state.get("surface_path", []), list) else []
        window_title_history_tail = self._dedupe_strings(
            [
                *[
                    str(row.get("window_title_after", "") or row.get("window_title_before", "") or "").strip()
                    for row in attempted_rows
                    if isinstance(row, dict)
                ],
                str(target_window.get("title", "") or active_window.get("title", "") or "").strip(),
            ]
        )[:8]
        nested_progress_count = sum(
            1
            for row in attempted_rows
            if isinstance(row, dict) and bool(row.get("nested_surface_progressed", row.get("progressed", False)))
        )
        return {
            "mission_kind": "exploration",
            "stop_reason_code": str(stop_reason_code or "").strip(),
            "resume_action": EXPLORATION_ADVANCE_ACTION,
            "resume_preconditions": (
                ["review_current_surface"]
                if stop_reason_code in {"exploration_manual_review_required", "exploration_no_safe_path", "exploration_no_progress", "exploration_route_unavailable"}
                else ["reacquire_current_surface"]
            ),
            "window_title": str(target_window.get("title", "") or active_window.get("title", "") or "").strip(),
            "window_hwnd": int(target_window.get("hwnd", 0) or active_window.get("hwnd", 0) or 0),
            "screen_hash": str(observation.get("screen_hash", "") or "").strip(),
            "page_kind": str(plan.get("surface_mode", "") or "").strip(),
            "dialog_kind": str(dialog_state.get("dialog_kind", "") or "").strip(),
            "approval_kind": str(dialog_state.get("approval_kind", "") or "").strip(),
            "dialog_visible": bool(dialog_state.get("visible", False) or safety_signals.get("dialog_visible", False)),
            "dialog_review_required": bool(dialog_state.get("review_required", False)),
            "secure_desktop_likely": bool(dialog_state.get("secure_desktop_likely", False)),
            "manual_input_required": bool(dialog_state.get("manual_input_required", False)),
            "credential_field_count": int(dialog_state.get("credential_field_count", 0) or 0),
            "preferred_confirmation_button": str(safety_signals.get("preferred_confirmation_button", "") or "").strip(),
            "preferred_dismiss_button": str(safety_signals.get("preferred_dismiss_button", "") or "").strip(),
            "safe_dialog_buttons": [str(item).strip() for item in safety_signals.get("safe_dialog_buttons", []) if str(item).strip()][:6] if isinstance(safety_signals.get("safe_dialog_buttons", []), list) else [],
            "confirmation_dialog_buttons": [str(item).strip() for item in safety_signals.get("confirmation_dialog_buttons", []) if str(item).strip()][:6] if isinstance(safety_signals.get("confirmation_dialog_buttons", []), list) else [],
            "destructive_dialog_buttons": [str(item).strip() for item in safety_signals.get("destructive_dialog_buttons", []) if str(item).strip()][:6] if isinstance(safety_signals.get("destructive_dialog_buttons", []), list) else [],
            "credential_fields": [dict(row) for row in dialog_state.get("credential_fields", []) if isinstance(row, dict)] if isinstance(dialog_state.get("credential_fields", []), list) else [],
            "pending_requirements": pending_requirements[:12],
            "manual_required_controls": [dict(row) for row in form_page_state.get("manual_required_controls", []) if isinstance(row, dict)][:12] if isinstance(form_page_state.get("manual_required_controls", []), list) else [],
            "blocking_controls": pending_requirements[:12],
            "autonomous_blocker": str(dialog_state.get("approval_kind", "") or stop_reason_code).strip(),
            "recommended_actions": recommended_actions[:8],
            "operator_steps": operator_steps_map.get(
                stop_reason_code,
                [
                    "Inspect the current unsupported-app surface carefully.",
                    "Resume the paused exploration mission only after the surface looks ready for another bounded action.",
                ],
            ),
            "surface_signature": self._surface_exploration_signature(exploration_plan=plan),
            "target_group_state": dict(snapshot.get("target_group_state", {})) if isinstance(snapshot.get("target_group_state", {}), dict) else {},
            "surface_mode": str(plan.get("surface_mode", "") or "").strip(),
            "hypothesis_count": int(plan.get("hypothesis_count", 0) or 0),
            "branch_action_count": int(plan.get("branch_action_count", 0) or 0),
            "attempted_target_count": len(attempted_rows),
            "alternative_target_count": max(0, int(alternative_target_count or 0)),
            "alternative_hypothesis_count": max(0, int(alternative_hypothesis_count or 0)),
            "alternative_branch_action_count": max(0, int(alternative_branch_action_count or 0)),
            "transition_kind": transition_kind,
            "nested_surface_progressed": nested_surface_progressed,
            "child_window_adopted": child_window_adopted,
            "surface_path_tail": surface_path_tail,
            "window_title_history_tail": window_title_history_tail,
            "nested_progress_count": nested_progress_count,
            "attempted_targets_tail": [dict(row) for row in attempted_rows[-6:]],
            "notes": notes[:12],
        }

    def _surface_exploration_resume_contract(
        self,
        *,
        args: Dict[str, Any],
        exploration_plan: Dict[str, Any],
        blocking_surface: Dict[str, Any],
        resume_action: str = EXPLORATION_ADVANCE_ACTION,
    ) -> Dict[str, Any]:
        if not isinstance(exploration_plan, dict) or not exploration_plan or not isinstance(blocking_surface, dict) or not blocking_surface:
            return {}
        snapshot = exploration_plan.get("surface_snapshot", {}) if isinstance(exploration_plan.get("surface_snapshot", {}), dict) else {}
        target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
        active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
        clean_anchor_app = str(args.get("app_name", "") or "").strip()
        clean_anchor_title = str(
            args.get("window_title", "")
            or target_window.get("title", "")
            or active_window.get("title", "")
            or ""
        ).strip()
        blocking_window_title = str(blocking_surface.get("window_title", "") or "").strip()
        use_anchor_window = bool(
            not clean_anchor_app
            and clean_anchor_title
            and self._normalize_probe_text(clean_anchor_title) != self._normalize_probe_text(blocking_window_title)
        )
        query = str(
            args.get("query", "")
            or exploration_plan.get("filters", {}).get("query", "")
            or ""
        ).strip() if isinstance(exploration_plan.get("filters", {}), dict) else str(args.get("query", "") or "").strip()
        attempted_targets = self._surface_exploration_attempt_history(args=args)
        surface_signature_history = self._surface_exploration_signature_history(args=args)
        resume_payload: Dict[str, Any] = {
            "action": resume_action,
            "app_name": clean_anchor_app,
            "window_title": clean_anchor_title if use_anchor_window else "",
            "query": query,
            "focus_first": True,
            "verify_after_action": bool(args.get("verify_after_action", True)),
            "retry_on_verification_failure": bool(args.get("retry_on_verification_failure", True)),
            "max_strategy_attempts": max(1, min(int(args.get("max_strategy_attempts", 2) or 2), 4)),
            "exploration_limit": max(1, min(int(args.get("exploration_limit", 6) or 6), 12)),
            "attempted_targets": [dict(row) for row in attempted_targets],
            "surface_signature_history": list(surface_signature_history),
        }
        if resume_action == EXPLORATION_FLOW_ACTION:
            resume_payload["max_exploration_steps"] = max(1, min(int(args.get("max_exploration_steps", 3) or 3), 8))
        remaining_options = self._surface_exploration_remaining_options(
            exploration_plan=exploration_plan,
            attempted_targets=attempted_targets,
        )
        continuation_targets = [
            {
                "query": str(row.get("label", "") or "").strip(),
                "candidate_id": str(row.get("candidate_id", "") or "").strip(),
                "action": str(row.get("selected_action", row.get("suggested_action", "")) or "").strip(),
            }
            for row in remaining_options.get("remaining_rows", [])
            if isinstance(row, dict) and str(row.get("label", "") or "").strip()
        ][:4]
        signature_parts = [
            "exploration",
            str(blocking_surface.get("stop_reason_code", "") or "").strip().lower(),
            str(blocking_surface.get("surface_signature", "") or "").strip().lower(),
            clean_anchor_app.lower(),
            clean_anchor_title.lower(),
            blocking_window_title.lower(),
            query.lower(),
        ]
        return {
            "mission_kind": "exploration",
            "resume_action": resume_action,
            "resume_strategy": (
                "reacquire_anchor_window"
                if use_anchor_window
                else ("reacquire_app_surface" if clean_anchor_app else "reacquire_current_surface")
            ),
            "resume_signature": hashlib.sha1("|".join(signature_parts).encode("utf-8")).hexdigest()[:16],
            "resume_payload": self._sanitize_payload_for_response(resume_payload),
            "resume_preconditions": [str(item).strip() for item in blocking_surface.get("resume_preconditions", []) if str(item).strip()] if isinstance(blocking_surface.get("resume_preconditions", []), list) else [],
            "operator_steps": [str(item).strip() for item in blocking_surface.get("operator_steps", []) if str(item).strip()] if isinstance(blocking_surface.get("operator_steps", []), list) else [],
            "anchor_app_name": clean_anchor_app,
            "anchor_window_title": clean_anchor_title,
            "blocking_window_title": blocking_window_title,
            "surface_match_hints": {
                "anchor_app_name": clean_anchor_app,
                "anchor_window_title": clean_anchor_title,
                "blocking_window_title": blocking_window_title,
                "blocking_window_hwnd": int(blocking_surface.get("window_hwnd", 0) or 0),
                "screen_hash": str(blocking_surface.get("screen_hash", "") or "").strip(),
                "surface_signature": str(blocking_surface.get("surface_signature", "") or "").strip(),
                "approval_kind": str(blocking_surface.get("approval_kind", "") or "").strip(),
                "dialog_kind": str(blocking_surface.get("dialog_kind", "") or "").strip(),
                "surface_mode": str(blocking_surface.get("surface_mode", "") or "").strip(),
                "transition_kind": str(blocking_surface.get("transition_kind", "") or "").strip(),
                "nested_surface_progressed": bool(blocking_surface.get("nested_surface_progressed", False)),
                "child_window_adopted": bool(blocking_surface.get("child_window_adopted", False)),
                "surface_path_tail": [
                    str(item).strip()
                    for item in blocking_surface.get("surface_path_tail", [])
                    if str(item).strip()
                ] if isinstance(blocking_surface.get("surface_path_tail", []), list) else [],
                "prefer_anchor_on_resume": use_anchor_window,
                "allow_child_window_adoption": True,
            },
            "continuation_targets": continuation_targets,
            "window_title_history_tail": [
                str(item).strip()
                for item in blocking_surface.get("window_title_history_tail", [])
                if str(item).strip()
            ] if isinstance(blocking_surface.get("window_title_history_tail", []), list) else [],
        }

    def _advise_surface_exploration_advance(self, *, args: Dict[str, Any]) -> Dict[str, Any]:
        exploration_limit = max(1, min(int(args.get("exploration_limit", 6) or 6), 12))
        app_name = str(args.get("app_name", "") or "").strip()
        window_title = str(args.get("window_title", "") or "").strip()
        query = str(args.get("query", "") or "").strip()
        exploration_plan = self.surface_exploration_plan(
            app_name=app_name,
            window_title=window_title,
            query=query,
            limit=exploration_limit,
            include_observation=True,
            include_elements=True,
            include_workflow_probes=True,
        )
        snapshot = exploration_plan.get("surface_snapshot", {}) if isinstance(exploration_plan.get("surface_snapshot", {}), dict) else {}
        app_profile = snapshot.get("app_profile", {}) if isinstance(snapshot.get("app_profile", {}), dict) else {}
        target_window = snapshot.get("target_window", {}) if isinstance(snapshot.get("target_window", {}), dict) else {}
        active_window = snapshot.get("active_window", {}) if isinstance(snapshot.get("active_window", {}), dict) else {}
        candidate_windows = snapshot.get("candidate_windows", []) if isinstance(snapshot.get("candidate_windows", []), list) else []
        capabilities = snapshot.get("capabilities", {}) if isinstance(snapshot.get("capabilities", {}), dict) else self._capabilities()
        safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
        blocked_response = {
            "status": "blocked",
            "action": EXPLORATION_ADVANCE_ACTION,
            "route_mode": "surface_exploration_advance",
            "confidence": 0.0,
            "risk_level": "high" if bool(exploration_plan.get("manual_attention_required", False)) else "medium",
            "app_profile": app_profile if app_profile.get("status") == "success" else app_profile,
            "workflow_profile": {},
            "profile_defaults_applied": {},
            "target_window": target_window,
            "active_window": active_window,
            "candidate_windows": candidate_windows[:6],
            "capabilities": capabilities,
            "execution_plan": [],
            "blockers": [],
            "warnings": [],
            "autonomy": {
                "supports_resume": True,
                "supports_cross_app_fallback": bool(capabilities.get("vision", {}).get("available")) and bool(capabilities.get("accessibility", {}).get("available")) if isinstance(capabilities, dict) else False,
                "exploration_ready": bool(exploration_plan.get("automation_ready", False)),
                "requires_manual_clearance": bool(exploration_plan.get("manual_attention_required", False)),
            },
            "surface_snapshot": snapshot,
            "safety_signals": safety_signals,
            "form_target_state": {},
            "surface_branch": {},
            "verification_plan": {},
            "adaptive_strategy": {},
            "strategy_variants": [{"strategy_id": "primary", "title": "Primary Recon Step", "reason": "Use the strongest supported recon target.", "payload_overrides": {}}],
            "exploration_plan": exploration_plan if isinstance(exploration_plan, dict) else {},
            "exploration_selection": {},
            "message": str(exploration_plan.get("message", "") or "Surface recon could not build a safe next step.").strip(),
        }
        if not isinstance(exploration_plan, dict) or str(exploration_plan.get("status", "") or "").strip().lower() != "success":
            blocked_response["status"] = "error" if str(exploration_plan.get("status", "") or "").strip().lower() == "error" else "blocked"
            blocked_response["message"] = str(exploration_plan.get("message", "") or blocked_response["message"]).strip()
            blocked_response["blockers"] = [blocked_response["message"]] if blocked_response["message"] else []
            return blocked_response

        attempted_targets = self._surface_exploration_attempt_history(args=args)
        remaining_options = self._surface_exploration_remaining_options(
            exploration_plan=exploration_plan,
            attempted_targets=attempted_targets,
        )
        exploration_plan["attempted_target_count"] = len(attempted_targets)
        exploration_plan["remaining_target_count"] = int(remaining_options.get("remaining_target_count", 0) or 0)
        exploration_plan["remaining_hypothesis_count"] = int(remaining_options.get("remaining_hypothesis_count", 0) or 0)
        exploration_plan["remaining_branch_action_count"] = int(remaining_options.get("remaining_branch_action_count", 0) or 0)
        exploration_plan["attempted_targets_tail"] = [dict(row) for row in attempted_targets[-6:]]

        selected = self._select_surface_exploration_target(exploration_plan=exploration_plan, args=args)
        if str(selected.get("status", "") or "") != "success":
            blocked_response["warnings"] = [str(item).strip() for item in selected.get("warnings", []) if str(item).strip()]
            blocked_response["blockers"] = [str(item).strip() for item in selected.get("blockers", []) if str(item).strip()]
            blocked_response["exploration_selection"] = {
                "status": str(selected.get("status", "") or "").strip(),
                "stop_reason_code": str(selected.get("stop_reason_code", "") or "").strip(),
                "attention_signals": [str(item).strip() for item in selected.get("attention_signals", []) if str(item).strip()] if isinstance(selected.get("attention_signals", []), list) else [],
            }
            blocked_response["message"] = str(selected.get("message", "") or blocked_response["message"]).strip()
            return blocked_response

        selected_payload = dict(selected.get("action_payload", {})) if isinstance(selected.get("action_payload", {}), dict) else {}
        nested_advice = self.advise(selected_payload)
        nested_status = str(nested_advice.get("status", "") or "").strip().lower()
        blockers = self._dedupe_strings(
            [str(item).strip() for item in selected.get("blockers", []) if str(item).strip()]
            + [str(item).strip() for item in nested_advice.get("blockers", []) if str(item).strip()]
        )
        warnings = self._dedupe_strings(
            [str(item).strip() for item in selected.get("warnings", []) if str(item).strip()]
            + [str(item).strip() for item in nested_advice.get("warnings", []) if str(item).strip()]
        )
        if nested_status != "success" and not blockers:
            message = str(nested_advice.get("message", "") or "The selected surface target could not be routed safely.").strip()
            blockers.append(message or "The selected surface target could not be routed safely.")
        route_message = (
            str(selected.get("message", "") or "").strip()
            or str(exploration_plan.get("message", "") or "").strip()
            or "Surface recon selected the next bounded automation step."
        )
        return {
            "status": "success" if nested_status == "success" and not blockers else "blocked",
            "action": EXPLORATION_ADVANCE_ACTION,
            "route_mode": "surface_exploration_advance",
            "confidence": round(
                max(
                    float(selected.get("confidence", 0.0) or 0.0),
                    float(nested_advice.get("confidence", 0.0) or 0.0),
                ),
                4,
            ),
            "risk_level": (
                "high"
                if bool(exploration_plan.get("manual_attention_required", False))
                else str(nested_advice.get("risk_level", "") or "medium").strip().lower()
            ),
            "app_profile": nested_advice.get("app_profile", app_profile if app_profile.get("status") == "success" else app_profile),
            "workflow_profile": nested_advice.get("workflow_profile", {}),
            "profile_defaults_applied": nested_advice.get("profile_defaults_applied", {}),
            "target_window": nested_advice.get("target_window", target_window),
            "active_window": nested_advice.get("active_window", active_window),
            "candidate_windows": nested_advice.get("candidate_windows", candidate_windows[:6]),
            "capabilities": nested_advice.get("capabilities", capabilities),
            "execution_plan": nested_advice.get("execution_plan", []),
            "blockers": blockers,
            "warnings": warnings,
            "autonomy": {
                **(nested_advice.get("autonomy", {}) if isinstance(nested_advice.get("autonomy", {}), dict) else {}),
                "supports_resume": True,
                "exploration_ready": bool(exploration_plan.get("automation_ready", False)),
                "requires_manual_clearance": bool(exploration_plan.get("manual_attention_required", False)),
            },
            "surface_snapshot": nested_advice.get("surface_snapshot", snapshot),
            "safety_signals": nested_advice.get("safety_signals", safety_signals),
            "form_target_state": nested_advice.get("form_target_state", {}),
            "surface_branch": nested_advice.get("surface_branch", {}),
            "verification_plan": nested_advice.get("verification_plan", {}),
            "adaptive_strategy": nested_advice.get("adaptive_strategy", {}),
            "strategy_variants": nested_advice.get("strategy_variants", [{"strategy_id": "primary", "title": "Primary Recon Step", "reason": route_message, "payload_overrides": {}}]),
            "exploration_plan": exploration_plan,
            "exploration_selection": {
                "kind": str(selected.get("kind", "") or "").strip(),
                "candidate_id": str(selected.get("candidate_id", "") or "").strip(),
                "label": str(selected.get("label", "") or "").strip(),
                "selected_action": str(selected.get("selected_action", "") or "").strip(),
                "confidence": float(selected.get("confidence", 0.0) or 0.0),
                "selection_key": str(selected.get("selection_key", "") or "").strip(),
                "attempted_target_count": int(selected.get("attempted_target_count", 0) or 0),
                "reason": str(selected.get("reason", "") or "").strip(),
                "action_payload": self._sanitize_payload_for_response(selected_payload),
            },
            "message": route_message,
        }

    def _advise_surface_exploration_flow(self, *, args: Dict[str, Any]) -> Dict[str, Any]:
        max_exploration_steps = max(1, min(int(args.get("max_exploration_steps", 3) or 3), 8))
        nested_advice = self._advise_surface_exploration_advance(args=args)
        payload = dict(nested_advice) if isinstance(nested_advice, dict) else {}
        autonomy = payload.get("autonomy", {}) if isinstance(payload.get("autonomy", {}), dict) else {}
        payload["action"] = EXPLORATION_FLOW_ACTION
        payload["route_mode"] = "surface_exploration_flow"
        payload["autonomy"] = {
            **autonomy,
            "supports_resume": True,
            "exploration_flow": True,
            "max_exploration_steps": max_exploration_steps,
        }
        existing_message = str(payload.get("message", "") or "").strip()
        if payload.get("status") == "success":
            payload["message"] = existing_message or (
                f"JARVIS can continue up to {max_exploration_steps} bounded recon step"
                f"{'' if max_exploration_steps == 1 else 's'} while the surface keeps progressing safely."
            )
        elif not existing_message:
            payload["message"] = (
                f"JARVIS could not start a bounded recon flow of up to {max_exploration_steps} step"
                f"{'' if max_exploration_steps == 1 else 's'} on the current surface."
            )
        return payload

    def _execute_surface_exploration_attempt(
        self,
        *,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        strategy: Dict[str, Any],
        attempt_index: int,
        persist_pause: bool = True,
        resume_action: str = EXPLORATION_ADVANCE_ACTION,
        step_index: int = 1,
    ) -> Dict[str, Any]:
        selection = advice.get("exploration_selection", {}) if isinstance(advice.get("exploration_selection", {}), dict) else {}
        selected_payload = selection.get("action_payload", {}) if isinstance(selection.get("action_payload", {}), dict) else {}
        if not selected_payload:
            message = "Surface recon did not have an executable next step."
            return {
                "attempt": attempt_index,
                "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
                "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
                "strategy_reason": str(strategy.get("reason", "") or "").strip(),
                "payload": self._sanitize_payload_for_response(args),
                "status": "blocked",
                "message": message,
                "final_action": "",
                "results": [],
                "advice": advice,
                "verification": {
                    "enabled": bool(args.get("verify_after_action", True)),
                    "status": "skipped",
                    "verified": False,
                    "message": message,
                    "checks": [],
                },
                "exploration_mission": {
                    "enabled": True,
                    "completed": False,
                    "step_index": step_index,
                    "stop_reason_code": "exploration_no_safe_path",
                    "stop_reason": message,
                },
                "exploration_runtime": {
                    "step_index": step_index,
                    "progressed": False,
                    "stop_reason_code": "exploration_no_safe_path",
                    "stop_reason": message,
                    "blocking_surface": {},
                    "resume_contract": {},
                    "page_record": {
                        "step_index": step_index,
                        "selected_action": "",
                        "selected_candidate_id": "",
                        "selected_candidate_label": "",
                        "status": "blocked",
                        "message": message,
                        "progressed": False,
                    },
                },
            }

        initial_plan = advice.get("exploration_plan", {}) if isinstance(advice.get("exploration_plan", {}), dict) else {}
        attempted_targets_before = self._surface_exploration_attempt_history(args=args)
        surface_signature_history_before = self._surface_exploration_signature_history(args=args)
        nested_result = self.execute(selected_payload)
        nested_status = str(nested_result.get("status", "") or "error").strip().lower() or "error"
        nested_results = nested_result.get("results", []) if isinstance(nested_result.get("results", []), list) else []
        nested_verification = nested_result.get("verification", {}) if isinstance(nested_result.get("verification", {}), dict) else {}
        selected_action = str(selection.get("selected_action", "") or selected_payload.get("action", "")).strip().lower()
        followup_window_title = str(
            nested_result.get("target_window", {}).get("title", "")
            if isinstance(nested_result.get("target_window", {}), dict)
            else ""
        ).strip() or str(args.get("window_title", "") or "").strip()
        followup_plan = self.surface_exploration_plan(
            app_name=str(args.get("app_name", "") or "").strip(),
            window_title=followup_window_title,
            query=str(args.get("query", "") or "").strip(),
            limit=max(1, min(int(args.get("exploration_limit", 6) or 6), 12)),
            include_observation=True,
            include_elements=True,
            include_workflow_probes=True,
        )
        if not isinstance(followup_plan, dict) or str(followup_plan.get("status", "") or "").strip().lower() != "success":
            followup_plan = initial_plan if isinstance(initial_plan, dict) else {}
        followup_snapshot = followup_plan.get("surface_snapshot", {}) if isinstance(followup_plan.get("surface_snapshot", {}), dict) else {}
        followup_hypothesis_count = int(followup_plan.get("hypothesis_count", 0) or 0)
        followup_branch_count = int(followup_plan.get("branch_action_count", 0) or 0)
        followup_ready = bool(followup_plan.get("automation_ready", False))
        followup_manual = bool(followup_plan.get("manual_attention_required", False))
        initial_signature = self._surface_exploration_signature(exploration_plan=initial_plan)
        followup_signature = self._surface_exploration_signature(exploration_plan=followup_plan)
        transition_summary = self._surface_exploration_transition_summary(
            before_plan=initial_plan,
            after_plan=followup_plan,
        )
        attempted_target_entry = {
            "kind": str(selection.get("kind", "") or "").strip(),
            "candidate_id": str(selection.get("candidate_id", "") or "").strip(),
            "label": str(selection.get("label", "") or "").strip(),
            "selected_action": selected_action,
            "status": nested_status,
            "progressed": False,
            "transition_kind": str(transition_summary.get("transition_kind", "") or "").strip(),
            "nested_surface_progressed": bool(transition_summary.get("nested_surface_progressed", False)),
            "child_window_adopted": bool(transition_summary.get("child_window_adopted", False)),
            "step_index": step_index,
            "surface_signature_before": initial_signature,
            "surface_signature_after": followup_signature,
            "window_title_before": str(transition_summary.get("window_title_before", "") or "").strip(),
            "window_title_after": str(transition_summary.get("window_title_after", "") or "").strip(),
            "surface_path_before": [
                str(item).strip()
                for item in transition_summary.get("surface_path_before", [])
                if str(item).strip()
            ] if isinstance(transition_summary.get("surface_path_before", []), list) else [],
            "surface_path_after": [
                str(item).strip()
                for item in transition_summary.get("surface_path_after", [])
                if str(item).strip()
            ] if isinstance(transition_summary.get("surface_path_after", []), list) else [],
        }
        attempted_targets = self._merge_surface_exploration_attempt_history(
            attempted_targets=attempted_targets_before,
            new_entry=attempted_target_entry,
        )
        surface_signature_history = self._merge_surface_signature_history(
            existing=surface_signature_history_before,
            additions=[initial_signature, followup_signature],
        )
        remaining_options = self._surface_exploration_remaining_options(
            exploration_plan=followup_plan if isinstance(followup_plan, dict) else {},
            attempted_targets=attempted_targets,
        )
        alternative_target_count = int(remaining_options.get("remaining_target_count", 0) or 0)
        alternative_hypothesis_count = int(remaining_options.get("remaining_hypothesis_count", 0) or 0)
        alternative_branch_action_count = int(remaining_options.get("remaining_branch_action_count", 0) or 0)
        alternative_ready = bool(alternative_target_count > 0 and not followup_manual)
        same_signature = bool(initial_signature and followup_signature and initial_signature == followup_signature)
        top_followup = (
            followup_plan.get("top_hypotheses", [])[0]
            if isinstance(followup_plan.get("top_hypotheses", []), list) and followup_plan.get("top_hypotheses", [])
            else {}
        )
        top_followup = top_followup if isinstance(top_followup, dict) else {}
        same_top_target = bool(
            str(top_followup.get("candidate_id", "") or "").strip()
            and str(top_followup.get("candidate_id", "") or "").strip() == str(selection.get("candidate_id", "") or "").strip()
            and str(top_followup.get("suggested_action", "") or "").strip().lower() == selected_action
        )
        mission_record: Dict[str, Any] = {}
        transition_progressed = bool(transition_summary.get("nested_surface_progressed", False))
        progress_made = bool(
            nested_status in {"success", "partial"}
            and (
                not same_signature
                or not same_top_target
                or transition_progressed
            )
        )
        attempted_target_entry["progressed"] = progress_made
        attempted_target_entry["nested_surface_progressed"] = transition_progressed
        updated_advice = {
            **advice,
            "exploration_plan": followup_plan if isinstance(followup_plan, dict) else initial_plan,
            "surface_snapshot": followup_snapshot or advice.get("surface_snapshot", {}),
            "target_window": (
                followup_snapshot.get("target_window", {})
                if isinstance(followup_snapshot.get("target_window", {}), dict) and followup_snapshot.get("target_window")
                else advice.get("target_window", {})
            ),
        }
        stop_reason_code = ""
        stop_reason = ""
        mission_status = nested_status
        mission_completed = False
        if nested_status not in {"success", "partial"}:
            stop_reason_code = "exploration_route_unavailable"
            stop_reason = str(nested_result.get("message", "") or "The selected recon target could not be executed safely.").strip()
        elif followup_manual:
            stop_reason_code = "exploration_manual_review_required"
            stop_reason = str(
                followup_plan.get("message", "")
                or "The current surface still needs manual review before JARVIS should continue exploring it."
            ).strip()
            mission_status = "blocked"
        elif (same_signature or same_top_target) and not transition_progressed:
            if alternative_ready:
                stop_reason_code = "exploration_followup_available"
                stop_reason = (
                    "Surface recon stayed on the same primary target, but JARVIS found another safe branch "
                    "and is ready to continue without repeating the last step."
                )
                mission_status = "partial"
            else:
                stop_reason_code = "exploration_no_progress"
                stop_reason = "Surface recon remained on the same top target after execution, so JARVIS is pausing to avoid looping."
                mission_status = "blocked"
        elif followup_hypothesis_count > 0 or followup_branch_count > 0:
            if followup_ready:
                stop_reason_code = "exploration_followup_available"
                stop_reason = str(
                    followup_plan.get("message", "")
                    or "JARVIS advanced the surface and found another bounded recon step."
                ).strip()
                mission_status = "partial"
            else:
                stop_reason_code = "exploration_no_safe_path"
                stop_reason = str(
                    followup_plan.get("message", "")
                    or "Surface recon needs more explicit human guidance before another autonomous step."
                ).strip()
                mission_status = "blocked"
        else:
            mission_completed = nested_status == "success"
            stop_reason = str(
                nested_result.get("message", "")
                or "Surface recon completed without finding another high-confidence follow-up step."
            ).strip()

        surface_path_tail = [
            str(item).strip()
            for item in transition_summary.get("surface_path_after", [])
            if str(item).strip()
        ] if isinstance(transition_summary.get("surface_path_after", []), list) else []
        window_title_history_tail = self._dedupe_strings(
            [
                *[
                    str(row.get("window_title_after", "") or row.get("window_title_before", "") or "").strip()
                    for row in attempted_targets
                    if isinstance(row, dict)
                ],
                str(transition_summary.get("window_title_after", "") or "").strip(),
            ]
        )[:8]
        nested_progress_count = sum(
            1
            for row in attempted_targets
            if isinstance(row, dict) and bool(row.get("nested_surface_progressed", row.get("progressed", False)))
        )

        blocking_surface: Dict[str, Any] = {}
        resume_contract: Dict[str, Any] = {}
        exploration_mission: Dict[str, Any] = {
            "enabled": True,
            "completed": mission_completed,
            "selected_action": selected_action,
            "selected_candidate_id": str(selection.get("candidate_id", "") or "").strip(),
            "selected_candidate_label": str(selection.get("label", "") or "").strip(),
            "surface_mode": str(followup_plan.get("surface_mode", initial_plan.get("surface_mode", "")) or "").strip() if isinstance(initial_plan, dict) else str(followup_plan.get("surface_mode", "") or "").strip(),
            "stop_reason_code": stop_reason_code,
            "stop_reason": stop_reason,
            "hypothesis_count": followup_hypothesis_count,
            "branch_action_count": followup_branch_count,
            "automation_ready": followup_ready,
            "manual_attention_required": followup_manual,
            "step_index": step_index,
            "attempted_target_count": len(attempted_targets),
            "alternative_target_count": alternative_target_count,
            "alternative_hypothesis_count": alternative_hypothesis_count,
            "alternative_branch_action_count": alternative_branch_action_count,
            "alternative_ready": alternative_ready,
            "transition_kind": str(transition_summary.get("transition_kind", "") or "").strip(),
            "nested_surface_progressed": transition_progressed,
            "child_window_adopted": bool(transition_summary.get("child_window_adopted", False)),
            "surface_path_tail": surface_path_tail,
            "window_title_history_tail": window_title_history_tail,
            "nested_progress_count": nested_progress_count,
            "attempted_targets_tail": [dict(row) for row in attempted_targets[-6:]],
            "surface_signature_history": surface_signature_history,
            "next_actions": self._dedupe_strings(
                [
                    *[
                        str(row.get("suggested_action", "") or "").strip()
                        for row in followup_plan.get("top_hypotheses", [])
                        if isinstance(row, dict) and str(row.get("suggested_action", "") or "").strip()
                    ],
                    *[
                        str(row.get("action", "") or "").strip()
                        for row in followup_plan.get("branch_actions", [])
                        if isinstance(row, dict) and str(row.get("action", "") or "").strip()
                    ],
                ]
            )[:8] if isinstance(followup_plan, dict) else [],
        }
        page_record = {
            "step_index": step_index,
            "selected_action": selected_action,
            "selected_candidate_id": str(selection.get("candidate_id", "") or "").strip(),
            "selected_candidate_label": str(selection.get("label", "") or "").strip(),
            "status": mission_status if stop_reason_code else nested_status,
            "message": stop_reason if stop_reason else str(nested_result.get("message", "") or ""),
            "before_signature": initial_signature,
            "after_signature": followup_signature,
            "progressed": progress_made,
            "transition_kind": str(transition_summary.get("transition_kind", "") or "").strip(),
            "nested_surface_progressed": transition_progressed,
            "child_window_adopted": bool(transition_summary.get("child_window_adopted", False)),
            "surface_path_before": [
                str(item).strip()
                for item in transition_summary.get("surface_path_before", [])
                if str(item).strip()
            ] if isinstance(transition_summary.get("surface_path_before", []), list) else [],
            "surface_path_after": surface_path_tail,
            "window_title_before": str(transition_summary.get("window_title_before", "") or "").strip(),
            "window_title_after": str(transition_summary.get("window_title_after", "") or "").strip(),
            "attempted_target_count": len(attempted_targets),
            "alternative_target_count": alternative_target_count,
        }
        pause_payload: Dict[str, Any] = {}
        if stop_reason_code:
            blocking_surface = self._surface_exploration_blocking_surface(
                exploration_plan=followup_plan if isinstance(followup_plan, dict) and followup_plan else initial_plan,
                stop_reason_code=stop_reason_code,
                selected=selection,
                attempted_targets=attempted_targets,
                alternative_target_count=alternative_target_count,
                alternative_hypothesis_count=alternative_hypothesis_count,
                alternative_branch_action_count=alternative_branch_action_count,
            )
            resume_args = dict(args)
            resume_args["attempted_targets"] = [dict(row) for row in attempted_targets]
            resume_args["surface_signature_history"] = list(surface_signature_history)
            resume_contract = self._surface_exploration_resume_contract(
                args=resume_args,
                exploration_plan=followup_plan if isinstance(followup_plan, dict) and followup_plan else initial_plan,
                blocking_surface=blocking_surface,
                resume_action=resume_action,
            )
            exploration_mission["blocking_surface"] = blocking_surface
            exploration_mission["resume_contract"] = resume_contract
            pause_payload = {
                "status": mission_status,
                "message": stop_reason,
                "stop_reason_code": stop_reason_code,
                "stop_reason": stop_reason,
                "page_count": 1,
                "pages_completed": 1 if nested_status in {"success", "partial"} else 0,
                "requested_target_count": 1,
                "resolved_target_count": 1 if nested_status in {"success", "partial"} else 0,
                "remaining_target_count": alternative_target_count,
                "surface_mode": str(exploration_mission.get("surface_mode", "") or "").strip(),
                "exploration_query": str(args.get("query", "") or "").strip(),
                "hypothesis_count": followup_hypothesis_count,
                "branch_action_count": followup_branch_count,
                "attempted_target_count": len(attempted_targets),
                "alternative_target_count": alternative_target_count,
                "alternative_hypothesis_count": alternative_hypothesis_count,
                "alternative_branch_action_count": alternative_branch_action_count,
                "transition_kind": str(transition_summary.get("transition_kind", "") or "").strip(),
                "nested_surface_progressed": transition_progressed,
                "child_window_adopted": bool(transition_summary.get("child_window_adopted", False)),
                "surface_path_tail": surface_path_tail,
                "window_title_history_tail": window_title_history_tail,
                "nested_progress_count": nested_progress_count,
                "attempted_targets": [dict(row) for row in attempted_targets],
                "surface_signature_history": surface_signature_history,
                "selected_action": selected_action,
                "selected_candidate_id": str(selection.get("candidate_id", "") or "").strip(),
                "selected_candidate_label": str(selection.get("label", "") or "").strip(),
                "final_page": {
                    "window_title": str(transition_summary.get("window_title_after", "") or "").strip(),
                    "screen_hash": str(
                        followup_snapshot.get("observation", {}).get("screen_hash", "")
                        if isinstance(followup_snapshot.get("observation", {}), dict)
                        else ""
                    ).strip(),
                    "surface_mode": str(exploration_mission.get("surface_mode", "") or "").strip(),
                    "surface_path": surface_path_tail,
                    "child_window_adopted": bool(transition_summary.get("child_window_adopted", False)),
                },
                "page_history": [page_record],
            }
            if persist_pause:
                mission_record = self._persist_paused_mission(
                    mission_kind="exploration",
                    args=args,
                    blocking_surface=blocking_surface,
                    resume_contract=resume_contract,
                    mission_payload=pause_payload,
                    warnings=self._dedupe_strings(
                        [str(item).strip() for item in advice.get("warnings", []) if str(item).strip()]
                        + [str(item).strip() for item in nested_result.get("warnings", []) if str(item).strip()]
                    ),
                    message=stop_reason,
                )

        return {
            "attempt": attempt_index,
            "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
            "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
            "strategy_reason": str(strategy.get("reason", "") or "").strip(),
            "payload": self._sanitize_payload_for_response(args),
            "status": mission_status if stop_reason_code else nested_status,
            "message": stop_reason if stop_reason else str(nested_result.get("message", "") or ""),
            "final_action": selected_action or str(nested_result.get("final_action", "") or ""),
            "results": nested_results,
            "advice": updated_advice,
            "verification": nested_verification,
            "mission_record": mission_record if isinstance(mission_record, dict) else {},
            "exploration_mission": exploration_mission,
            "exploration_runtime": {
                "step_index": step_index,
                "progressed": progress_made,
                "transition_kind": str(transition_summary.get("transition_kind", "") or "").strip(),
                "nested_surface_progressed": transition_progressed,
                "child_window_adopted": bool(transition_summary.get("child_window_adopted", False)),
                "surface_path_tail": surface_path_tail,
                "window_title_history_tail": window_title_history_tail,
                "nested_progress_count": nested_progress_count,
                "stop_reason_code": stop_reason_code,
                "stop_reason": stop_reason,
                "blocking_surface": blocking_surface,
                "resume_contract": resume_contract,
                "page_record": page_record,
                "pause_payload": pause_payload,
                "remaining_target_count": alternative_target_count,
                "attempted_targets": [dict(row) for row in attempted_targets],
                "surface_signature_history": surface_signature_history,
                "alternative_target_count": alternative_target_count,
                "alternative_hypothesis_count": alternative_hypothesis_count,
                "alternative_branch_action_count": alternative_branch_action_count,
                "followup_plan": followup_plan if isinstance(followup_plan, dict) else {},
                "selected_action": selected_action,
                "selected_candidate_id": str(selection.get("candidate_id", "") or "").strip(),
                "selected_candidate_label": str(selection.get("label", "") or "").strip(),
            },
        }

    def _execute_surface_exploration_strategy(
        self,
        *,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        strategy: Dict[str, Any],
        attempt_index: int,
    ) -> Dict[str, Any]:
        return self._execute_surface_exploration_attempt(
            args=args,
            advice=advice,
            strategy=strategy,
            attempt_index=attempt_index,
            persist_pause=True,
            resume_action=EXPLORATION_ADVANCE_ACTION,
            step_index=1,
        )

    def _execute_surface_exploration_flow_strategy(
        self,
        *,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        strategy: Dict[str, Any],
        attempt_index: int,
    ) -> Dict[str, Any]:
        max_exploration_steps = max(1, min(int(args.get("max_exploration_steps", 3) or 3), 8))
        results: List[Dict[str, Any]] = []
        step_history: List[Dict[str, Any]] = []
        current_args = dict(args)
        current_args["action"] = EXPLORATION_ADVANCE_ACTION
        current_args["_provided_fields"] = self._dedupe_strings(
            list(current_args.get("_provided_fields", [])) + ["action"]
        )
        current_advice = advice
        last_verification: Dict[str, Any] = {
            "enabled": bool(args.get("verify_after_action", True)),
            "status": "skipped",
            "verified": False,
            "message": "surface exploration flow did not execute any bounded recon steps",
            "checks": [],
        }
        mission_record: Dict[str, Any] = {}
        completed = False
        stop_reason_code = ""
        stop_reason = ""
        message = ""
        latest_runtime: Dict[str, Any] = {}
        latest_followup_plan: Dict[str, Any] = (
            current_advice.get("exploration_plan", {})
            if isinstance(current_advice.get("exploration_plan", {}), dict)
            else {}
        )
        latest_selected_action = ""
        latest_selected_candidate_id = ""
        latest_selected_candidate_label = ""

        for step_index in range(1, max_exploration_steps + 1):
            if step_index > 1:
                current_advice = self._advise_surface_exploration_advance(args=current_args)
            if current_advice.get("status") != "success":
                stop_reason_code = "exploration_route_unavailable"
                stop_reason = "; ".join(
                    str(item).strip()
                    for item in current_advice.get("blockers", [])
                    if str(item).strip()
                ) or str(
                    current_advice.get("message", "")
                    or "JARVIS could not route the next bounded exploration step safely."
                ).strip()
                latest_followup_plan = (
                    current_advice.get("exploration_plan", {})
                    if isinstance(current_advice.get("exploration_plan", {}), dict)
                    else latest_followup_plan
                )
                step_history.append(
                    {
                        "step_index": step_index,
                        "selected_action": "",
                        "selected_candidate_id": "",
                        "selected_candidate_label": "",
                        "status": "blocked",
                        "message": stop_reason,
                        "progressed": False,
                    }
                )
                break

            step_payload = self._execute_surface_exploration_attempt(
                args=current_args,
                advice=current_advice,
                strategy=strategy,
                attempt_index=attempt_index,
                persist_pause=False,
                resume_action=EXPLORATION_FLOW_ACTION,
                step_index=step_index,
            )
            results.extend(step_payload.get("results", []) if isinstance(step_payload.get("results", []), list) else [])
            last_verification = step_payload.get("verification", {}) if isinstance(step_payload.get("verification", {}), dict) else last_verification
            latest_runtime = (
                step_payload.get("exploration_runtime", {})
                if isinstance(step_payload.get("exploration_runtime", {}), dict)
                else {}
            )
            latest_followup_plan = (
                latest_runtime.get("followup_plan", {})
                if isinstance(latest_runtime.get("followup_plan", {}), dict)
                else latest_followup_plan
            )
            page_record = latest_runtime.get("page_record", {}) if isinstance(latest_runtime.get("page_record", {}), dict) else {}
            step_history.append(page_record or {
                "step_index": step_index,
                "selected_action": "",
                "selected_candidate_id": "",
                "selected_candidate_label": "",
                "status": str(step_payload.get("status", "") or "").strip().lower(),
                "message": str(step_payload.get("message", "") or "").strip(),
                "progressed": False,
            })
            step_mission = (
                step_payload.get("exploration_mission", {})
                if isinstance(step_payload.get("exploration_mission", {}), dict)
                else {}
            )
            latest_selected_action = str(step_mission.get("selected_action", "") or latest_runtime.get("selected_action", "") or "").strip()
            latest_selected_candidate_id = str(step_mission.get("selected_candidate_id", "") or latest_runtime.get("selected_candidate_id", "") or "").strip()
            latest_selected_candidate_label = str(step_mission.get("selected_candidate_label", "") or latest_runtime.get("selected_candidate_label", "") or "").strip()
            current_args["attempted_targets"] = [
                dict(row)
                for row in latest_runtime.get("attempted_targets", [])
                if isinstance(row, dict)
            ] if isinstance(latest_runtime.get("attempted_targets", []), list) else current_args.get("attempted_targets", [])
            current_args["surface_signature_history"] = [
                str(item).strip()
                for item in latest_runtime.get("surface_signature_history", [])
                if str(item).strip()
            ] if isinstance(latest_runtime.get("surface_signature_history", []), list) else current_args.get("surface_signature_history", [])
            if bool(step_mission.get("completed", False)):
                completed = True
                message = str(step_payload.get("message", "") or "surface exploration flow completed").strip()
                break
            step_stop_reason_code = str(step_mission.get("stop_reason_code", "") or latest_runtime.get("stop_reason_code", "") or "").strip()
            step_stop_reason = str(step_mission.get("stop_reason", "") or latest_runtime.get("stop_reason", "") or step_payload.get("message", "") or "").strip()
            if step_stop_reason_code == "exploration_followup_available" and step_index < max_exploration_steps:
                latest_snapshot = (
                    latest_followup_plan.get("surface_snapshot", {})
                    if isinstance(latest_followup_plan.get("surface_snapshot", {}), dict)
                    else {}
                )
                target_window = latest_snapshot.get("target_window", {}) if isinstance(latest_snapshot.get("target_window", {}), dict) else {}
                active_window = latest_snapshot.get("active_window", {}) if isinstance(latest_snapshot.get("active_window", {}), dict) else {}
                latest_window_title = str(
                    target_window.get("title", "")
                    or active_window.get("title", "")
                    or current_args.get("window_title", "")
                    or ""
                ).strip()
                latest_filters = latest_followup_plan.get("filters", {}) if isinstance(latest_followup_plan.get("filters", {}), dict) else {}
                if latest_window_title:
                    current_args["window_title"] = latest_window_title
                elif "window_title" in current_args:
                    current_args.pop("window_title", None)
                if str(latest_filters.get("app_name", "") or "").strip() and not str(current_args.get("app_name", "") or "").strip():
                    current_args["app_name"] = str(latest_filters.get("app_name", "") or "").strip()
                continue
            stop_reason_code = step_stop_reason_code or "exploration_no_safe_path"
            stop_reason = step_stop_reason or "surface exploration flow stopped before a safe follow-up could be selected"
            if stop_reason_code == "exploration_followup_available" and step_index >= max_exploration_steps:
                stop_reason_code = "exploration_step_limit_reached"
                stop_reason = (
                    f"JARVIS completed {step_index} bounded recon step"
                    f"{'' if step_index == 1 else 's'} and found another safe follow-up, "
                    f"so it paused at the configured step limit of {max_exploration_steps}."
                )
            message = stop_reason
            break

        if completed and not message:
            message = "surface exploration flow completed"
        if not completed and not stop_reason_code and not message:
            stop_reason_code = "exploration_no_safe_path"
            stop_reason = "JARVIS could not identify a safe bounded continuation for the current unsupported-app surface."
            message = stop_reason

        if completed:
            verification = {
                "enabled": bool(args.get("verify_after_action", True)),
                "status": "passed",
                "verified": True,
                "message": message,
                "checks": [
                    {
                        "name": "exploration_steps_completed",
                        "passed": len(step_history) > 0,
                        "steps_completed": len(step_history),
                    },
                    {
                        "name": "exploration_flow_completed",
                        "passed": True,
                        "max_steps": max_exploration_steps,
                    },
                ],
            }
            exploration_mission = {
                "enabled": True,
                "completed": True,
                "step_count": len(step_history),
                "steps_completed": len(step_history),
                "max_steps": max_exploration_steps,
                "auto_continued": len(step_history) > 1,
                "selected_action": latest_selected_action,
                "selected_candidate_id": latest_selected_candidate_id,
                "selected_candidate_label": latest_selected_candidate_label,
                "surface_mode": str(latest_followup_plan.get("surface_mode", "") or "").strip(),
                "stop_reason_code": "",
                "stop_reason": "",
                "hypothesis_count": int(latest_followup_plan.get("hypothesis_count", 0) or 0) if isinstance(latest_followup_plan, dict) else 0,
                "branch_action_count": int(latest_followup_plan.get("branch_action_count", 0) or 0) if isinstance(latest_followup_plan, dict) else 0,
                "attempted_target_count": len(
                    [row for row in current_args.get("attempted_targets", []) if isinstance(row, dict)]
                )
                if isinstance(current_args.get("attempted_targets", []), list)
                else 0,
                "alternative_target_count": int(latest_runtime.get("alternative_target_count", 0) or 0),
                "alternative_hypothesis_count": int(latest_runtime.get("alternative_hypothesis_count", 0) or 0),
                "alternative_branch_action_count": int(latest_runtime.get("alternative_branch_action_count", 0) or 0),
                "transition_kind": str(latest_runtime.get("transition_kind", "") or "").strip(),
                "nested_surface_progressed": bool(latest_runtime.get("nested_surface_progressed", False)),
                "child_window_adopted": bool(latest_runtime.get("child_window_adopted", False)),
                "surface_path_tail": [
                    str(item).strip()
                    for item in latest_runtime.get("surface_path_tail", [])
                    if str(item).strip()
                ] if isinstance(latest_runtime.get("surface_path_tail", []), list) else [],
                "window_title_history_tail": [
                    str(item).strip()
                    for item in latest_runtime.get("window_title_history_tail", [])
                    if str(item).strip()
                ] if isinstance(latest_runtime.get("window_title_history_tail", []), list) else [],
                "nested_progress_count": int(latest_runtime.get("nested_progress_count", 0) or 0),
                "attempted_targets_tail": [
                    dict(row)
                    for row in current_args.get("attempted_targets", [])[-6:]
                    if isinstance(row, dict)
                ]
                if isinstance(current_args.get("attempted_targets", []), list)
                else [],
                "surface_signature_history": [
                    str(item).strip()
                    for item in current_args.get("surface_signature_history", [])
                    if str(item).strip()
                ]
                if isinstance(current_args.get("surface_signature_history", []), list)
                else [],
                "automation_ready": bool(latest_followup_plan.get("automation_ready", False)) if isinstance(latest_followup_plan, dict) else False,
                "manual_attention_required": bool(latest_followup_plan.get("manual_attention_required", False)) if isinstance(latest_followup_plan, dict) else False,
                "next_actions": [],
                "step_history": step_history,
            }
            return {
                "attempt": attempt_index,
                "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
                "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
                "strategy_reason": str(strategy.get("reason", "") or "").strip(),
                "payload": self._sanitize_payload_for_response(args),
                "status": "success",
                "message": message,
                "final_action": latest_selected_action or EXPLORATION_FLOW_ACTION,
                "results": results,
                "advice": {
                    **current_advice,
                    "action": EXPLORATION_FLOW_ACTION,
                    "route_mode": "surface_exploration_flow",
                    "exploration_plan": latest_followup_plan if isinstance(latest_followup_plan, dict) else current_advice.get("exploration_plan", {}),
                },
                "verification": verification,
                "mission_record": {},
                "exploration_mission": exploration_mission,
            }

        pause_snapshot = latest_followup_plan if isinstance(latest_followup_plan, dict) and latest_followup_plan else (
            current_advice.get("exploration_plan", {})
            if isinstance(current_advice.get("exploration_plan", {}), dict)
            else {}
        )
        selected_stub = {
            "candidate_id": latest_selected_candidate_id,
            "label": latest_selected_candidate_label,
            "selected_action": latest_selected_action,
        }
        blocking_surface = latest_runtime.get("blocking_surface", {}) if isinstance(latest_runtime.get("blocking_surface", {}), dict) else {}
        if not blocking_surface:
            blocking_surface = self._surface_exploration_blocking_surface(
                exploration_plan=pause_snapshot,
                stop_reason_code=stop_reason_code,
                selected=selected_stub,
                attempted_targets=[
                    dict(row)
                    for row in current_args.get("attempted_targets", [])
                    if isinstance(row, dict)
                ] if isinstance(current_args.get("attempted_targets", []), list) else [],
                alternative_target_count=int(latest_runtime.get("alternative_target_count", 0) or 0),
                alternative_hypothesis_count=int(latest_runtime.get("alternative_hypothesis_count", 0) or 0),
                alternative_branch_action_count=int(latest_runtime.get("alternative_branch_action_count", 0) or 0),
            )
        if isinstance(blocking_surface, dict) and blocking_surface:
            blocking_surface["stop_reason_code"] = stop_reason_code
            blocking_surface["stop_reason"] = stop_reason
            blocking_surface["resume_action"] = EXPLORATION_FLOW_ACTION
        resume_contract = latest_runtime.get("resume_contract", {}) if isinstance(latest_runtime.get("resume_contract", {}), dict) else {}
        if not resume_contract:
            resume_contract = self._surface_exploration_resume_contract(
                args=current_args,
                exploration_plan=pause_snapshot,
                blocking_surface=blocking_surface,
                resume_action=EXPLORATION_FLOW_ACTION,
            )
        pause_payload = {
            "status": "partial" if stop_reason_code in {"exploration_followup_available", "exploration_step_limit_reached"} and results else "blocked",
            "message": message,
            "stop_reason_code": stop_reason_code,
            "stop_reason": stop_reason,
            "page_count": len(step_history),
            "pages_completed": sum(1 for row in step_history if bool(row.get("progressed", False))),
            "requested_target_count": len(step_history),
            "resolved_target_count": sum(1 for row in step_history if str(row.get("status", "") or "").strip().lower() in {"success", "partial", "blocked"}),
            "remaining_target_count": int(latest_runtime.get("remaining_target_count", 0) or 0),
            "surface_mode": str(pause_snapshot.get("surface_mode", "") or "").strip(),
            "exploration_query": str(args.get("query", "") or "").strip(),
            "hypothesis_count": int(pause_snapshot.get("hypothesis_count", 0) or 0) if isinstance(pause_snapshot, dict) else 0,
            "branch_action_count": int(pause_snapshot.get("branch_action_count", 0) or 0) if isinstance(pause_snapshot, dict) else 0,
            "attempted_target_count": len(
                [row for row in current_args.get("attempted_targets", []) if isinstance(row, dict)]
            )
            if isinstance(current_args.get("attempted_targets", []), list)
            else 0,
            "alternative_target_count": int(latest_runtime.get("alternative_target_count", 0) or 0),
            "alternative_hypothesis_count": int(latest_runtime.get("alternative_hypothesis_count", 0) or 0),
            "alternative_branch_action_count": int(latest_runtime.get("alternative_branch_action_count", 0) or 0),
            "transition_kind": str(latest_runtime.get("transition_kind", "") or "").strip(),
            "nested_surface_progressed": bool(latest_runtime.get("nested_surface_progressed", False)),
            "child_window_adopted": bool(latest_runtime.get("child_window_adopted", False)),
            "surface_path_tail": [
                str(item).strip()
                for item in latest_runtime.get("surface_path_tail", [])
                if str(item).strip()
            ] if isinstance(latest_runtime.get("surface_path_tail", []), list) else [],
            "window_title_history_tail": [
                str(item).strip()
                for item in latest_runtime.get("window_title_history_tail", [])
                if str(item).strip()
            ] if isinstance(latest_runtime.get("window_title_history_tail", []), list) else [],
            "nested_progress_count": int(latest_runtime.get("nested_progress_count", 0) or 0),
            "attempted_targets": [
                dict(row)
                for row in current_args.get("attempted_targets", [])
                if isinstance(row, dict)
            ] if isinstance(current_args.get("attempted_targets", []), list) else [],
            "surface_signature_history": [
                str(item).strip()
                for item in current_args.get("surface_signature_history", [])
                if str(item).strip()
            ] if isinstance(current_args.get("surface_signature_history", []), list) else [],
            "selected_action": latest_selected_action,
            "selected_candidate_id": latest_selected_candidate_id,
            "selected_candidate_label": latest_selected_candidate_label,
            "final_page": {
                "window_title": str(
                    pause_snapshot.get("surface_snapshot", {}).get("target_window", {}).get("title", "")
                    if isinstance(pause_snapshot.get("surface_snapshot", {}), dict)
                    and isinstance(pause_snapshot.get("surface_snapshot", {}).get("target_window", {}), dict)
                    else ""
                ).strip(),
                "surface_mode": str(pause_snapshot.get("surface_mode", "") or "").strip(),
            },
            "page_history": step_history,
            "step_count": len(step_history),
            "steps_completed": sum(1 for row in step_history if bool(row.get("progressed", False))),
            "max_steps": max_exploration_steps,
            "auto_continued": len(step_history) > 1,
        }
        mission_record = self._persist_paused_mission(
            mission_kind="exploration",
            args=args,
            blocking_surface=blocking_surface,
            resume_contract=resume_contract,
            mission_payload=pause_payload,
            warnings=self._dedupe_strings(
                [str(item).strip() for item in current_advice.get("warnings", []) if str(item).strip()]
            ),
            message=message,
        )
        if mission_record:
            pause_payload["mission_record"] = mission_record
        verification_status = "passed" if results else "skipped"
        verification = {
            "enabled": bool(args.get("verify_after_action", True)),
            "status": verification_status,
            "verified": bool(results),
            "message": message,
            "checks": [
                {
                    "name": "exploration_steps_completed",
                    "passed": len(step_history) > 0,
                    "steps_completed": len(step_history),
                },
                {
                    "name": "exploration_flow_progress",
                    "passed": any(bool(row.get("progressed", False)) for row in step_history),
                    "max_steps": max_exploration_steps,
                },
            ],
        }
        pause_status = "partial" if results and stop_reason_code in {"exploration_followup_available", "exploration_step_limit_reached"} else "blocked"
        exploration_mission = {
            "enabled": True,
            "completed": False,
            "step_count": len(step_history),
            "steps_completed": sum(1 for row in step_history if bool(row.get("progressed", False))),
            "max_steps": max_exploration_steps,
            "auto_continued": len(step_history) > 1,
            "selected_action": latest_selected_action,
            "selected_candidate_id": latest_selected_candidate_id,
            "selected_candidate_label": latest_selected_candidate_label,
            "surface_mode": str(pause_payload.get("surface_mode", "") or "").strip(),
            "stop_reason_code": stop_reason_code,
            "stop_reason": stop_reason,
            "hypothesis_count": int(pause_payload.get("hypothesis_count", 0) or 0),
            "branch_action_count": int(pause_payload.get("branch_action_count", 0) or 0),
            "attempted_target_count": int(pause_payload.get("attempted_target_count", 0) or 0),
            "alternative_target_count": int(pause_payload.get("alternative_target_count", 0) or 0),
            "alternative_hypothesis_count": int(pause_payload.get("alternative_hypothesis_count", 0) or 0),
            "alternative_branch_action_count": int(pause_payload.get("alternative_branch_action_count", 0) or 0),
            "transition_kind": str(pause_payload.get("transition_kind", "") or "").strip(),
            "nested_surface_progressed": bool(pause_payload.get("nested_surface_progressed", False)),
            "child_window_adopted": bool(pause_payload.get("child_window_adopted", False)),
            "surface_path_tail": [
                str(item).strip()
                for item in pause_payload.get("surface_path_tail", [])
                if str(item).strip()
            ] if isinstance(pause_payload.get("surface_path_tail", []), list) else [],
            "window_title_history_tail": [
                str(item).strip()
                for item in pause_payload.get("window_title_history_tail", [])
                if str(item).strip()
            ] if isinstance(pause_payload.get("window_title_history_tail", []), list) else [],
            "nested_progress_count": int(pause_payload.get("nested_progress_count", 0) or 0),
            "attempted_targets_tail": [
                dict(row)
                for row in pause_payload.get("attempted_targets", [])[-6:]
                if isinstance(row, dict)
            ] if isinstance(pause_payload.get("attempted_targets", []), list) else [],
            "surface_signature_history": [
                str(item).strip()
                for item in pause_payload.get("surface_signature_history", [])
                if str(item).strip()
            ] if isinstance(pause_payload.get("surface_signature_history", []), list) else [],
            "automation_ready": bool(pause_snapshot.get("automation_ready", False)) if isinstance(pause_snapshot, dict) else False,
            "manual_attention_required": bool(pause_snapshot.get("manual_attention_required", False)) if isinstance(pause_snapshot, dict) else False,
            "next_actions": self._dedupe_strings(
                [
                    *[
                        str(row.get("suggested_action", "") or "").strip()
                        for row in pause_snapshot.get("top_hypotheses", [])
                        if isinstance(row, dict) and str(row.get("suggested_action", "") or "").strip()
                    ],
                    *[
                        str(row.get("action", "") or "").strip()
                        for row in pause_snapshot.get("branch_actions", [])
                        if isinstance(row, dict) and str(row.get("action", "") or "").strip()
                    ],
                ]
            )[:8] if isinstance(pause_snapshot, dict) else [],
            "blocking_surface": blocking_surface,
            "resume_contract": resume_contract,
            "step_history": step_history,
            "mission_record": mission_record,
        }
        return {
            "attempt": attempt_index,
            "strategy_id": str(strategy.get("strategy_id", f"attempt_{attempt_index}") or f"attempt_{attempt_index}"),
            "strategy_title": str(strategy.get("title", f"Attempt {attempt_index}") or f"Attempt {attempt_index}"),
            "strategy_reason": str(strategy.get("reason", "") or "").strip(),
            "payload": self._sanitize_payload_for_response(args),
            "status": pause_status,
            "message": message,
            "final_action": latest_selected_action or EXPLORATION_FLOW_ACTION,
            "results": results,
            "advice": {
                **current_advice,
                "action": EXPLORATION_FLOW_ACTION,
                "route_mode": "surface_exploration_flow",
                "exploration_plan": pause_snapshot if isinstance(pause_snapshot, dict) else current_advice.get("exploration_plan", {}),
                "resume_action": EXPLORATION_FLOW_ACTION,
                "resume_contract": resume_contract,
                "blocking_surface": blocking_surface,
                "mission_record": mission_record,
            },
            "verification": verification,
            "mission_record": mission_record,
            "exploration_mission": exploration_mission,
        }

    def _surface_exploration_hypotheses(
        self,
        *,
        snapshot: Dict[str, Any],
        app_name: str,
        window_title: str,
        query: str,
        surface_mode: str,
        limit: int,
        surface_intelligence: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        query_targets = [dict(row) for row in snapshot.get("query_targets", []) if isinstance(row, dict)]
        related_candidates = [dict(row) for row in snapshot.get("query_related_candidates", []) if isinstance(row, dict)]
        selection_candidates = [dict(row) for row in snapshot.get("selection_candidates", []) if isinstance(row, dict)]
        safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
        dialog_targets = [
            dict(row)
            for row in safety_signals.get("dialog_button_targets", [])
            if isinstance(row, dict)
        ]
        form_page_state = snapshot.get("form_page_state", {}) if isinstance(snapshot.get("form_page_state", {}), dict) else {}
        wizard_page_state = snapshot.get("wizard_page_state", {}) if isinstance(snapshot.get("wizard_page_state", {}), dict) else {}
        candidate_specs: List[tuple[str, Dict[str, Any]]] = []
        for source_name, rows in (
            ("query_target", query_targets),
            ("related_candidate", related_candidates),
            ("selection_candidate", selection_candidates),
            ("dialog_button", dialog_targets),
            ("navigation_target", [dict(row) for row in form_page_state.get("available_navigation_targets", []) if isinstance(row, dict)]),
            ("tab_target", [dict(row) for row in form_page_state.get("available_tabs", []) if isinstance(row, dict)]),
            ("drilldown_target", [dict(row) for row in form_page_state.get("available_drilldown_targets", []) if isinstance(row, dict)]),
            ("expandable_group", [dict(row) for row in form_page_state.get("available_expandable_groups", []) if isinstance(row, dict)]),
            ("wizard_requirement", [dict(row) for row in wizard_page_state.get("pending_requirements", []) if isinstance(row, dict)]),
            ("form_requirement", [dict(row) for row in form_page_state.get("pending_requirements", []) if isinstance(row, dict)]),
        ):
            for row in rows:
                candidate_specs.append((source_name, row))

        hypotheses: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for source_name, candidate in candidate_specs:
            candidate_key = self._element_identity_key(candidate) or "|".join(
                [
                    source_name,
                    str(candidate.get("name", "") or "").strip(),
                    str(candidate.get("control_type", "") or "").strip(),
                ]
            )
            if not candidate_key or candidate_key in seen:
                continue
            seen.add(candidate_key)
            hypothesis = self._surface_exploration_hypothesis(
                candidate=candidate,
                source_name=source_name,
                snapshot=snapshot,
                app_name=app_name,
                window_title=window_title,
                query=query,
                surface_mode=surface_mode,
                surface_intelligence=surface_intelligence or {},
            )
            if isinstance(hypothesis, dict) and hypothesis:
                hypotheses.append(hypothesis)
        hypotheses.sort(
            key=lambda row: (
                -float(row.get("score", 0.0) or 0.0),
                1 if bool(row.get("manual_attention_required", False)) else 0,
                str(row.get("label", "") or "").lower(),
            )
        )
        return hypotheses[: max(1, min(int(limit or 8), 12))]

    def _surface_exploration_hypothesis(
        self,
        *,
        candidate: Dict[str, Any],
        source_name: str,
        snapshot: Dict[str, Any],
        app_name: str,
        window_title: str,
        query: str,
        surface_mode: str,
        surface_intelligence: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_candidate = self._element_state_summary(
            dict(candidate),
            match_score=float(candidate.get("match_score")) if isinstance(candidate.get("match_score"), (int, float)) else None,
        )
        label = str(normalized_candidate.get("name", "") or "").strip()
        control_type = self._normalize_probe_text(normalized_candidate.get("control_type", ""))
        if not label and not control_type:
            return {}
        surface_flags = snapshot.get("surface_flags", {}) if isinstance(snapshot.get("surface_flags", {}), dict) else {}
        safety_signals = snapshot.get("safety_signals", {}) if isinstance(snapshot.get("safety_signals", {}), dict) else {}
        suggested_action = self._surface_exploration_action_for_candidate(
            candidate=normalized_candidate,
            source_name=source_name,
            surface_flags=surface_flags,
            safety_signals=safety_signals,
        )
        if not suggested_action:
            return {}
        app_hint = str(app_name or snapshot.get("filters", {}).get("app_name", "") or "").strip()
        window_hint = str(
            window_title
            or snapshot.get("target_window", {}).get("title", "")
            or snapshot.get("filters", {}).get("window_title", "")
            or ""
        ).strip()
        action_payload: Dict[str, Any] = {
            "action": suggested_action,
        }
        if app_hint:
            action_payload["app_name"] = app_hint
        if window_hint:
            action_payload["window_title"] = window_hint
        target_query = label or str(query or "").strip()
        if suggested_action in {
            "click",
            "select_list_item",
            "select_tree_item",
            "expand_tree_item",
            "select_sidebar_item",
            "select_context_menu_item",
            "press_dialog_button",
            "select_tab_page",
            "open_dropdown",
            "focus_input_field",
            "focus_checkbox",
            "select_radio_option",
            "select_table_row",
            "invoke_toolbar_action",
        } and target_query:
            action_payload["query"] = target_query
        if str(normalized_candidate.get("element_id", "") or "").strip():
            action_payload["element_id"] = str(normalized_candidate.get("element_id", "") or "").strip()
        if str(normalized_candidate.get("control_type", "") or "").strip():
            action_payload["control_type"] = str(normalized_candidate.get("control_type", "") or "").strip()

        recommended_path = self._surface_exploration_action_path(
            suggested_action=suggested_action,
            action_payload=action_payload,
            candidate=normalized_candidate,
            surface_flags=surface_flags,
            source_name=source_name,
        )
        source_base_scores = {
            "query_target": 0.9,
            "dialog_button": 0.88,
            "navigation_target": 0.84,
            "tab_target": 0.84,
            "drilldown_target": 0.8,
            "expandable_group": 0.78,
            "selection_candidate": 0.74,
            "related_candidate": 0.68,
            "wizard_requirement": 0.7,
            "form_requirement": 0.7,
        }
        score = float(source_base_scores.get(source_name, 0.6))
        explicit_match = (
            float(candidate.get("match_score"))
            if isinstance(candidate.get("match_score"), (int, float))
            else self._element_query_match_score(candidate, query) if str(query or "").strip() else 0.0
        )
        score += min(0.2, max(0.0, explicit_match) * 0.18)
        if self._coerce_surface_bool(normalized_candidate.get("enabled")) is not False:
            score += 0.03
        if self._coerce_surface_bool(normalized_candidate.get("visible")) is not False:
            score += 0.03
        if suggested_action == "press_dialog_button" and bool(safety_signals.get("destructive_warning_visible", False)):
            score -= 0.18
        intelligence_payload = surface_intelligence if isinstance(surface_intelligence, dict) else {}
        interaction_mode = self._normalize_probe_text(intelligence_payload.get("interaction_mode", ""))
        surface_role = self._normalize_probe_text(intelligence_payload.get("surface_role", ""))
        query_resolution = (
            intelligence_payload.get("query_resolution", {})
            if isinstance(intelligence_payload.get("query_resolution", {}), dict)
            else {}
        )
        best_candidate_id = str(query_resolution.get("best_candidate_id", "") or "").strip()
        if best_candidate_id and best_candidate_id == str(normalized_candidate.get("element_id", "") or "").strip():
            score += 0.08
        if interaction_mode == "tree_list_navigation" and suggested_action in {"select_tree_item", "expand_tree_item", "select_list_item", "select_table_row"}:
            score += 0.04
        if interaction_mode == "form_fill" and suggested_action in {"focus_input_field", "focus_checkbox", "select_radio_option", "open_dropdown"}:
            score += 0.04
        if interaction_mode == "settings_navigation" and suggested_action in {"select_sidebar_item", "select_list_item", "focus_input_field", "focus_checkbox"}:
            score += 0.03
        if surface_role == "dialog" and suggested_action == "press_dialog_button":
            score += 0.06
        risk_flags = [
            self._normalize_probe_text(item)
            for item in intelligence_payload.get("risk_flags", [])
            if str(item).strip()
        ]
        if "destructive_dialog_path" in risk_flags and suggested_action in {"press_dialog_button", "click"}:
            score -= 0.08
        already_active = bool(
            self._coerce_surface_bool(normalized_candidate.get("selected")) is True
            or self._coerce_surface_bool(normalized_candidate.get("checked")) is True
            or (suggested_action == "expand_tree_item" and self._coerce_surface_bool(normalized_candidate.get("expanded")) is True)
        )
        if already_active:
            score -= 0.06
        confidence = max(0.05, min(0.99, round(score, 3)))
        state_tags = [
            tag
            for tag, enabled in (
                ("enabled", self._coerce_surface_bool(normalized_candidate.get("enabled")) is not False),
                ("visible", self._coerce_surface_bool(normalized_candidate.get("visible")) is not False),
                ("selected", self._coerce_surface_bool(normalized_candidate.get("selected")) is True),
                ("checked", self._coerce_surface_bool(normalized_candidate.get("checked")) is True),
                ("expanded", self._coerce_surface_bool(normalized_candidate.get("expanded")) is True),
            )
            if enabled
        ]
        manual_attention_required = bool(
            safety_signals.get("destructive_warning_visible", False)
            or safety_signals.get("admin_approval_required", False)
            or safety_signals.get("secure_desktop_likely", False)
            or "approval_or_credential_surface" in risk_flags
            or "destructive_dialog_path" in risk_flags
        )
        return {
            "candidate_id": self._element_identity_key(normalized_candidate) or f"{source_name}:{label}:{control_type}",
            "label": label or str(normalized_candidate.get("automation_id", "") or "").strip() or control_type or "target",
            "control_type": str(normalized_candidate.get("control_type", "") or "").strip(),
            "source": source_name,
            "surface_mode": surface_mode,
            "score": confidence,
            "confidence": confidence,
            "query_match_score": round(max(0.0, explicit_match), 3),
            "suggested_action": suggested_action,
            "action_payload": action_payload,
            "recommended_path": recommended_path,
            "state_tags": state_tags,
            "already_active": already_active,
            "manual_attention_required": manual_attention_required,
            "reason": self._surface_exploration_reason(
                candidate=normalized_candidate,
                source_name=source_name,
                suggested_action=suggested_action,
                query=query,
                already_active=already_active,
            ),
            "candidate_state": normalized_candidate,
        }

    def _surface_exploration_branch_actions(
        self,
        *,
        snapshot: Dict[str, Any],
        app_name: str,
        window_title: str,
        query: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        workflow_surfaces = [dict(row) for row in snapshot.get("workflow_surfaces", []) if isinstance(row, dict)]
        recommended_actions = [str(action).strip().lower() for action in snapshot.get("recommended_actions", []) if str(action).strip()]
        app_hint = str(app_name or snapshot.get("filters", {}).get("app_name", "") or "").strip()
        window_hint = str(
            window_title
            or snapshot.get("target_window", {}).get("title", "")
            or snapshot.get("filters", {}).get("window_title", "")
            or ""
        ).strip()
        rows: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for workflow in workflow_surfaces:
            action_name = str(workflow.get("action", "") or "").strip().lower()
            if not action_name or action_name in seen:
                continue
            if not bool(workflow.get("matched", False)) and action_name not in recommended_actions:
                continue
            seen.add(action_name)
            definition = self._workflow_definition(action_name)
            payload: Dict[str, Any] = {"action": action_name}
            if app_hint:
                payload["app_name"] = app_hint
            if window_hint:
                payload["window_title"] = window_hint
            if bool(definition.get("requires_input", False)) and str(query or "").strip():
                input_field = str(definition.get("input_field", "") or "").strip()
                if input_field and input_field.lower() != "none":
                    payload[input_field] = str(query).strip()
                    if input_field != "query":
                        payload["query"] = str(query).strip()
            rows.append(
                {
                    "action": action_name,
                    "title": str(workflow.get("title", action_name.replace("_", " ").title()) or action_name.replace("_", " ").title()),
                    "matched": bool(workflow.get("matched", False)),
                    "supported": bool(workflow.get("supported", False)),
                    "confidence": 0.88 if bool(workflow.get("matched", False)) else 0.72,
                    "reason": (
                        "This workflow already matches the visible surface."
                        if bool(workflow.get("matched", False))
                        else "This workflow is supported for the current surface and profile."
                    ),
                    "action_payload": payload,
                    "recommended_followups": [str(item).strip() for item in workflow.get("recommended_followups", []) if str(item).strip()][:6],
                }
            )
        for action_name in recommended_actions:
            if action_name in seen:
                continue
            seen.add(action_name)
            definition = self._workflow_definition(action_name)
            payload: Dict[str, Any] = {"action": action_name}
            if app_hint:
                payload["app_name"] = app_hint
            if window_hint:
                payload["window_title"] = window_hint
            if bool(definition.get("requires_input", False)) and str(query or "").strip():
                input_field = str(definition.get("input_field", "") or "").strip()
                if input_field and input_field.lower() != "none":
                    payload[input_field] = str(query).strip()
                    if input_field != "query":
                        payload["query"] = str(query).strip()
            rows.append(
                {
                    "action": action_name,
                    "title": str(definition.get("title", action_name.replace("_", " ").title()) or action_name.replace("_", " ").title()),
                    "matched": False,
                    "supported": True,
                    "confidence": 0.68,
                    "reason": "The current surface recommends this next action.",
                    "action_payload": payload,
                    "recommended_followups": [str(item).strip() for item in definition.get("recommended_followups", []) if str(item).strip()][:6],
                }
            )
        rows.sort(key=lambda row: (-float(row.get("confidence", 0.0) or 0.0), str(row.get("action", "") or "")))
        return rows[: max(1, min(int(limit or 8), 12))]

    def _surface_exploration_action_for_candidate(
        self,
        *,
        candidate: Dict[str, Any],
        source_name: str,
        surface_flags: Dict[str, Any],
        safety_signals: Dict[str, Any],
    ) -> str:
        control_type = self._normalize_probe_text(candidate.get("control_type", ""))
        dialog_visible = bool(surface_flags.get("dialog_visible", False) or safety_signals.get("dialog_visible", False))
        if source_name == "dialog_button":
            return "press_dialog_button"
        if source_name == "navigation_target" and control_type in {"treeitem", "listitem"}:
            return "select_sidebar_item"
        if source_name == "tab_target" or control_type == "tabitem":
            return "select_tab_page"
        if source_name == "expandable_group":
            return "expand_tree_item"
        if source_name == "drilldown_target":
            return "click"
        if control_type in {"button", "splitbutton"}:
            return "press_dialog_button" if dialog_visible else "click"
        if control_type == "hyperlink":
            return "click"
        if control_type == "menuitem":
            return "select_context_menu_item" if bool(surface_flags.get("context_menu_visible", False)) else "click"
        if control_type == "listitem":
            if bool(surface_flags.get("sidebar_visible", False)) and not bool(surface_flags.get("main_content_visible", False)):
                return "select_sidebar_item"
            return "select_list_item"
        if control_type == "treeitem":
            return "expand_tree_item" if self._coerce_surface_bool(candidate.get("expanded")) is False else "select_tree_item"
        if control_type in {"dataitem", "row"}:
            return "select_table_row"
        if control_type == "combobox":
            return "open_dropdown"
        if control_type in {"edit", "document"}:
            return "focus_input_field"
        if control_type == "checkbox":
            return "focus_checkbox"
        if control_type == "radiobutton":
            return "select_radio_option"
        if control_type in {"toolbar", "tool bar"}:
            return "focus_toolbar"
        return "click"

    def _surface_exploration_action_path(
        self,
        *,
        suggested_action: str,
        action_payload: Dict[str, Any],
        candidate: Dict[str, Any],
        surface_flags: Dict[str, Any],
        source_name: str,
    ) -> List[Dict[str, Any]]:
        prep_steps: List[Dict[str, Any]] = []
        clean_action = str(suggested_action or "").strip().lower()
        if clean_action == "select_sidebar_item" and not bool(surface_flags.get("sidebar_visible", False)):
            prep_steps.append(
                self._plan_step(
                    action="focus_sidebar",
                    args={},
                    phase="recon_prep",
                    optional=False,
                    reason="Focus the visible sidebar before selecting a sidebar target.",
                )
            )
        elif clean_action in {"select_tree_item", "expand_tree_item"} and not bool(
            surface_flags.get("tree_visible", False) or surface_flags.get("folder_tree_visible", False) or surface_flags.get("navigation_tree_visible", False)
        ):
            prep_steps.append(
                self._plan_step(
                    action="focus_navigation_tree",
                    args={},
                    phase="recon_prep",
                    optional=False,
                    reason="Focus the navigation tree before targeting a tree item.",
                )
            )
        elif clean_action == "select_list_item" and not bool(
            surface_flags.get("list_visible", False) or surface_flags.get("file_list_visible", False) or surface_flags.get("message_list_visible", False)
        ):
            prep_steps.append(
                self._plan_step(
                    action="focus_list_surface",
                    args={},
                    phase="recon_prep",
                    optional=False,
                    reason="Focus the list surface before selecting a list item.",
                )
            )
        elif clean_action == "select_table_row" and not bool(surface_flags.get("table_visible", False)):
            prep_steps.append(
                self._plan_step(
                    action="focus_data_table",
                    args={},
                    phase="recon_prep",
                    optional=False,
                    reason="Focus the data table before selecting a table row.",
                )
            )
        elif clean_action == "focus_input_field" and bool(surface_flags.get("sidebar_visible", False)) and not bool(surface_flags.get("main_content_visible", False)):
            prep_steps.append(
                self._plan_step(
                    action="focus_main_content",
                    args={},
                    phase="recon_prep",
                    optional=False,
                    reason="Move focus into the main content area before targeting an input field.",
                )
            )
        action_reason = (
            f"Act on the surfaced {str(candidate.get('control_type', '') or 'control').strip() or 'control'} target"
            if source_name != "dialog_button"
            else "Press the surfaced dialog button target."
        )
        prep_steps.append(
            self._plan_step(
                action=clean_action,
                args=action_payload,
                phase="recon_action",
                optional=False,
                reason=action_reason,
            )
        )
        return prep_steps

    def _surface_exploration_reason(
        self,
        *,
        candidate: Dict[str, Any],
        source_name: str,
        suggested_action: str,
        query: str,
        already_active: bool,
    ) -> str:
        label = str(candidate.get("name", "") or "").strip() or "target"
        source_messages = {
            "query_target": "directly matched the requested query",
            "related_candidate": "is closely related to the matched target",
            "selection_candidate": "looks selectable on the current surface",
            "dialog_button": "is exposed as a live dialog action",
            "navigation_target": "is exposed as a navigation target on the current page",
            "tab_target": "is exposed as an available tab on the current page",
            "drilldown_target": "looks like a nested page or deeper settings entry",
            "expandable_group": "looks like a collapsed group that may reveal more controls",
            "wizard_requirement": "looks like a wizard requirement that should be resolved before continuing",
            "form_requirement": "looks like a form requirement that should be resolved before committing",
        }
        reason = f"{label} {source_messages.get(source_name, 'is visible on the current surface')}."
        if str(query or "").strip():
            reason = f"{reason} Query: {str(query).strip()}."
        if already_active:
            reason = f"{reason} The target already appears active, so this path may be more useful as a refocus or verification step."
        return f"{reason} Suggested action: {suggested_action}."

    def _surface_exploration_surface_mode(
        self,
        *,
        app_profile: Dict[str, Any],
        surface_flags: Dict[str, Any],
        safety_signals: Dict[str, Any],
        snapshot: Dict[str, Any],
        surface_intelligence: Optional[Dict[str, Any]] = None,
    ) -> str:
        intelligence_payload = surface_intelligence if isinstance(surface_intelligence, dict) else {}
        interaction_mode = self._normalize_probe_text(intelligence_payload.get("interaction_mode", ""))
        surface_role = self._normalize_probe_text(intelligence_payload.get("surface_role", ""))
        if bool(surface_flags.get("dialog_visible", False) or safety_signals.get("dialog_visible", False)):
            return "dialog"
        if bool(snapshot.get("wizard_page_state", {})):
            return "wizard"
        if bool(snapshot.get("form_page_state", {})):
            return "form"
        if interaction_mode == "dialog_resolution" or surface_role == "dialog":
            return "dialog"
        if interaction_mode == "table_navigation":
            return "table_navigation"
        if interaction_mode == "form_fill":
            return "form"
        if interaction_mode == "settings_navigation":
            if bool(surface_flags.get("tree_visible", False) or surface_flags.get("folder_tree_visible", False) or surface_flags.get("navigation_tree_visible", False)):
                return "tree_navigation"
            if bool(surface_flags.get("sidebar_visible", False)):
                return "sidebar_navigation"
            if bool(surface_flags.get("list_visible", False) or surface_flags.get("file_list_visible", False) or surface_flags.get("message_list_visible", False)):
                return "list_navigation"
        if interaction_mode == "tree_list_navigation":
            if bool(surface_flags.get("tree_visible", False) or surface_flags.get("folder_tree_visible", False) or surface_flags.get("navigation_tree_visible", False)):
                return "tree_navigation"
            if bool(surface_flags.get("table_visible", False)):
                return "table_navigation"
            if bool(surface_flags.get("list_visible", False) or surface_flags.get("file_list_visible", False) or surface_flags.get("message_list_visible", False)):
                return "list_navigation"
        if bool(surface_flags.get("tree_visible", False) or surface_flags.get("folder_tree_visible", False) or surface_flags.get("navigation_tree_visible", False)):
            return "tree_navigation"
        if bool(surface_flags.get("sidebar_visible", False)):
            return "sidebar_navigation"
        if bool(surface_flags.get("table_visible", False)):
            return "table_navigation"
        if bool(surface_flags.get("list_visible", False) or surface_flags.get("file_list_visible", False) or surface_flags.get("message_list_visible", False)):
            return "list_navigation"
        category = str(app_profile.get("category", "") or "").strip().lower()
        return f"{category}_surface" if category else "generic_surface"

    def _surface_flags(
        self,
        *,
        app_profile: Dict[str, Any],
        workflow_surfaces: List[Dict[str, Any]],
        observation: Dict[str, Any],
        active_window: Dict[str, Any],
        target_window: Dict[str, Any],
        query: str = "",
        elements: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, bool]:
        category = str(app_profile.get("category", "") or "").strip().lower()
        matched_actions = {
            str(row.get("action", "") or "").strip().lower()
            for row in workflow_surfaces
            if isinstance(row, dict) and bool(row.get("matched", False))
        }
        observation_text = self._normalize_probe_text(observation.get("text", ""))
        normalized_query = self._normalize_probe_text(query)
        target_title = self._normalize_probe_text(target_window.get("title", ""))
        active_title = self._normalize_probe_text(active_window.get("title", ""))
        profile_name = self._normalize_probe_text(app_profile.get("name", ""))
        element_rows = [dict(row) for row in (elements or []) if isinstance(row, dict)]

        def _coerce_bool(value: Any) -> Optional[bool]:
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                if int(value) in {0, 1}:
                    return bool(int(value))
                return None
            clean = self._normalize_probe_text(value)
            if clean in {"true", "yes", "on", "checked", "selected", "expanded", "open"}:
                return True
            if clean in {"false", "no", "off", "unchecked", "unselected", "collapsed", "closed"}:
                return False
            return None

        def _normalize_toggle(value: Any) -> str:
            clean = self._normalize_probe_text(value)
            if clean in {"1", "true"}:
                return "on"
            if clean in {"0", "false"}:
                return "off"
            return clean

        def _element_control_type(row: Dict[str, Any]) -> str:
            return self._normalize_probe_text(row.get("control_type", ""))

        def _element_text(row: Dict[str, Any]) -> str:
            parts = [
                str(row.get("name", "") or ""),
                str(row.get("automation_id", "") or ""),
                str(row.get("class_name", "") or ""),
                str(row.get("control_type", "") or ""),
                str(row.get("state_text", "") or ""),
                str(row.get("value_text", "") or ""),
            ]
            if row.get("range_value") is not None:
                parts.append(str(row.get("range_value")))
            return self._normalize_probe_text(" ".join(parts))

        def _element_matches_query(row: Dict[str, Any]) -> bool:
            if not normalized_query:
                return True
            return normalized_query in _element_text(row)

        def _element_has_toggle_state(row: Dict[str, Any]) -> bool:
            return bool(_normalize_toggle(row.get("toggle_state", "")))

        def _element_is_checked(row: Dict[str, Any]) -> Optional[bool]:
            checked = _coerce_bool(row.get("checked"))
            if checked is not None:
                return checked
            toggle = _normalize_toggle(row.get("toggle_state", ""))
            if toggle in {"on", "checked"}:
                return True
            if toggle in {"off", "unchecked"}:
                return False
            return None

        def _element_is_selected(row: Dict[str, Any]) -> Optional[bool]:
            selected = _coerce_bool(row.get("selected"))
            if selected is not None:
                return selected
            return _element_is_checked(row)

        def _element_is_expanded(row: Dict[str, Any]) -> Optional[bool]:
            expanded = _coerce_bool(row.get("expanded"))
            if expanded is not None:
                return expanded
            state_text = self._normalize_probe_text(row.get("state_text", ""))
            if "expanded" in state_text or "opened" in state_text:
                return True
            if "collapsed" in state_text or "closed" in state_text:
                return False
            return None

        query_elements = [row for row in element_rows if _element_matches_query(row)]
        edit_elements = [row for row in element_rows if _element_control_type(row) == "edit"]
        combo_elements = [row for row in element_rows if _element_control_type(row) == "combobox"]
        checkbox_elements = [row for row in element_rows if _element_control_type(row) == "checkbox"]
        radio_elements = [row for row in element_rows if _element_control_type(row) == "radiobutton"]
        tab_elements = [row for row in element_rows if _element_control_type(row) == "tabitem"]
        slider_elements = [row for row in element_rows if _element_control_type(row) == "slider"]
        spinner_elements = [
            row
            for row in element_rows
            if _element_control_type(row) in {"spinner", "updown"}
        ]
        value_control_elements = [
            row
            for row in element_rows
            if _element_control_type(row) in {"slider", "spinner", "updown"}
            or row.get("range_value") is not None
            or bool(str(row.get("value_text", "") or "").strip())
        ]
        query_checkbox_elements = [row for row in checkbox_elements if _element_matches_query(row)]
        query_radio_elements = [row for row in radio_elements if _element_matches_query(row)]
        query_tab_elements = [row for row in tab_elements if _element_matches_query(row)]
        query_value_elements = [row for row in value_control_elements if _element_matches_query(row)]
        history_visible = "open_history" in matched_actions or any(
            phrase in observation_text for phrase in ("history", "recently closed", "recent tabs")
        )
        downloads_visible = "open_downloads" in matched_actions or any(
            phrase in observation_text for phrase in ("downloads", "download")
        )
        bookmarks_visible = "open_bookmarks" in matched_actions or any(
            phrase in observation_text for phrase in ("bookmarks", "bookmark manager")
        )
        devtools_visible = "open_devtools" in matched_actions or any(
            phrase in observation_text for phrase in ("developer tools", "devtools", "elements", "console")
        )
        tab_strip_visible = bool(tab_elements) or any(phrase in observation_text for phrase in ("tab strip", "tabs", "new tab"))
        tab_search_visible = "open_tab_search" in matched_actions or "search_tabs" in matched_actions or any(
            phrase in observation_text for phrase in ("search tabs", "search open tabs", "tab search", "open tabs")
        )
        explorer_visible = "focus_explorer" in matched_actions or any(
            phrase in observation_text for phrase in ("explorer", "project", "outline", "files")
        )
        folder_tree_visible = "focus_folder_tree" in matched_actions or any(
            phrase in observation_text for phrase in ("navigation pane", "quick access", "this pc", "folders")
        )
        file_list_visible = "focus_file_list" in matched_actions or any(
            phrase in observation_text for phrase in ("items view", "file list", "details view", "list view")
        )
        tree_visible = "focus_navigation_tree" in matched_actions or "select_tree_item" in matched_actions or "expand_tree_item" in matched_actions or folder_tree_visible or any(
            phrase in observation_text for phrase in ("tree view", "navigation tree", "nodes", "expanded", "collapsed")
        )
        list_visible = "focus_list_surface" in matched_actions or "select_list_item" in matched_actions or file_list_visible or any(
            phrase in observation_text for phrase in ("results list", "items list", "list view", "list pane")
        )
        table_visible = "focus_data_table" in matched_actions or "select_table_row" in matched_actions or any(
            phrase in observation_text for phrase in ("data grid", "table", "grid", "rows", "columns")
        )
        replace_visible = "find_replace" in matched_actions or (
            "replace" in observation_text and any(phrase in observation_text for phrase in ("find", "replace with", "find what"))
        )
        rename_active = "rename_selection" in matched_actions or any(
            phrase in observation_text for phrase in ("rename", "new name")
        )
        properties_dialog_visible = "open_properties_dialog" in matched_actions or any(
            phrase in observation_text for phrase in ("properties", "details", "type of file")
        )
        preview_pane_visible = "open_preview_pane" in matched_actions or any(
            phrase in observation_text for phrase in ("preview", "preview pane")
        )
        details_pane_visible = "open_details_pane" in matched_actions or any(
            phrase in observation_text for phrase in ("details pane", "size", "date modified")
        )
        conversation_picker_visible = "jump_to_conversation" in matched_actions or "new_chat" in matched_actions or any(
            phrase in observation_text for phrase in ("search or start new chat", "find or start a conversation", "new message", "people")
        )
        email_compose_ready = bool({"new_email_draft", "reply_email", "reply_all_email", "forward_email"} & matched_actions) or (
            any(phrase in observation_text for phrase in ("new message", "compose", "draft", "reply", "forward"))
            and ("subject" in observation_text or any(token in observation_text for token in ("to", "cc", "bcc")))
        )
        calendar_event_compose_ready = "new_calendar_event" in matched_actions or (
            any(phrase in observation_text for phrase in ("new event", "appointment", "invite attendees", "event details"))
            and any(token in observation_text for token in ("start", "end", "all day", "location"))
        )
        calendar_view_active = "open_calendar_view" in matched_actions or any(
            phrase in haystack
            for haystack in (observation_text, target_title, active_title)
            for phrase in ("calendar", "meetings", "schedule")
        ) and not calendar_event_compose_ready
        people_view_active = "open_people_view" in matched_actions or any(
            phrase in haystack
            for haystack in (observation_text, target_title, active_title)
            for phrase in ("people", "contacts", "contact list")
        )
        tasks_view_active = "open_tasks_view" in matched_actions or any(
            phrase in haystack
            for haystack in (observation_text, target_title, active_title)
            for phrase in ("tasks", "to do", "todo")
        )
        mail_view_active = "open_mail_view" in matched_actions or (
            any(phrase in haystack for haystack in (observation_text, target_title, active_title) for phrase in ("inbox", "mail", "message list"))
            and not email_compose_ready
            and not calendar_event_compose_ready
            and not calendar_view_active
            and not people_view_active
            and not tasks_view_active
        )
        folder_pane_visible = "focus_folder_pane" in matched_actions or any(
            phrase in observation_text for phrase in ("folder pane", "mail folders", "favorites", "mailbox")
        )
        message_list_visible = "focus_message_list" in matched_actions or any(
            phrase in observation_text for phrase in ("message list", "conversation list", "inbox list", "messages")
        )
        reading_pane_visible = "focus_reading_pane" in matched_actions or any(
            phrase in observation_text for phrase in ("reading pane", "message preview", "preview")
        )
        sidebar_visible = "focus_sidebar" in matched_actions or tree_visible or folder_pane_visible or any(
            phrase in observation_text for phrase in ("sidebar", "side panel", "left pane", "navigation")
        )
        main_content_visible = "focus_main_content" in matched_actions or list_visible or table_visible or message_list_visible or reading_pane_visible or any(
            phrase in observation_text for phrase in ("main pane", "content", "document", "results")
        )
        toolbar_visible = "focus_toolbar" in matched_actions or any(
            phrase in observation_text for phrase in ("toolbar", "command bar", "menu bar", "ribbon")
        )
        scrollbar_visible = bool(
            any(_element_control_type(row) == "scrollbar" for row in element_rows)
            or any(phrase in observation_text for phrase in ("scroll bar", "scrollbar", "scroll down", "scroll up"))
        )
        form_visible = (
            "focus_form_surface" in matched_actions
            or "set_field_value" in matched_actions
            or "select_dropdown_option" in matched_actions
            or "toggle_switch" in matched_actions
            or "enable_switch" in matched_actions
            or "disable_switch" in matched_actions
            or bool(edit_elements or combo_elements or checkbox_elements or radio_elements or value_control_elements)
            or any(
                phrase in observation_text
                for phrase in ("form", "text box", "input field", "combo box", "dropdown", "checkbox", "radio button", "slider", "spinner", "switch", "toggle")
            )
        )
        input_field_visible = (
            "focus_input_field" in matched_actions
            or "set_field_value" in matched_actions
            or bool(edit_elements)
            or any(phrase in observation_text for phrase in ("text box", "input field", "edit", "enter text"))
        )
        dropdown_visible = (
            "open_dropdown" in matched_actions
            or "select_dropdown_option" in matched_actions
            or bool(combo_elements)
            or any(phrase in observation_text for phrase in ("dropdown", "combo box", "select an option", "choose an option"))
        )
        dropdown_open = (
            bool({"open_dropdown", "select_dropdown_option"} & matched_actions)
            or any(_element_is_expanded(row) is True for row in combo_elements)
            or any(phrase in observation_text for phrase in ("select an option", "choose an option", "dropdown list", "list box"))
        )
        checkbox_visible = (
            "focus_checkbox" in matched_actions
            or bool({"check_checkbox", "uncheck_checkbox"} & matched_actions)
            or bool(checkbox_elements)
            or any(phrase in observation_text for phrase in ("checkbox", "check box", "checked", "unchecked"))
        )
        radio_option_visible = (
            "select_radio_option" in matched_actions
            or bool(radio_elements)
            or any(phrase in observation_text for phrase in ("radio button", "radio option"))
        )
        slider_visible = (
            "focus_value_control" in matched_actions
            or bool({"increase_value", "decrease_value"} & matched_actions)
            or bool(slider_elements)
            or any(phrase in observation_text for phrase in ("slider", "trackbar"))
        )
        spinner_visible = bool(spinner_elements) or any(
            phrase in observation_text for phrase in ("spinner", "stepper", "up down")
        )
        value_control_visible = (
            bool({"focus_value_control", "increase_value", "decrease_value"} & matched_actions)
            or bool(query_value_elements if normalized_query else value_control_elements)
            or slider_visible
            or spinner_visible
            or any(phrase in observation_text for phrase in ("value control", "slider", "spinner", "stepper", "number input"))
        )
        toggle_visible = (
            "toggle_switch" in matched_actions
            or any(phrase in observation_text for phrase in ("toggle", "switch"))
            or any(_element_has_toggle_state(row) for row in (query_elements or element_rows))
        )
        form_visible = bool(form_visible or toggle_visible)
        query_haystacks = [haystack for haystack in (observation_text, target_title, active_title) if haystack]
        checkbox_target_checked = (
            bool(normalized_query)
            and (
                any(_element_is_checked(row) is True for row in query_checkbox_elements)
                or any(
                    phrase in haystack
                    for haystack in query_haystacks
                    for phrase in (
                        f"{normalized_query} checked",
                        f"{normalized_query} enabled",
                        f"{normalized_query} on",
                        f"checked {normalized_query}",
                        f"enabled {normalized_query}",
                    )
                )
            )
        )
        checkbox_target_unchecked = (
            bool(normalized_query)
            and (
                any(_element_is_checked(row) is False for row in query_checkbox_elements)
                or any(
                    phrase in haystack
                    for haystack in query_haystacks
                    for phrase in (
                        f"{normalized_query} unchecked",
                        f"{normalized_query} disabled",
                        f"{normalized_query} off",
                        f"{normalized_query} not checked",
                        f"unchecked {normalized_query}",
                        f"disabled {normalized_query}",
                    )
                )
            )
        )
        radio_target_selected = bool(normalized_query) and (
            any(_element_is_selected(row) is True for row in query_radio_elements)
            or any(
                phrase in haystack
                for haystack in query_haystacks
                for phrase in (
                    f"{normalized_query} selected",
                    f"selected {normalized_query}",
                    f"{normalized_query} enabled",
                )
            )
        )
        tab_page_visible = bool(tab_elements) or any(
            phrase in observation_text for phrase in ("property sheet", "tab page", "selected tab")
        )
        tab_target_active = bool(normalized_query) and (
            any(_element_is_selected(row) is True for row in query_tab_elements)
            or any(
                phrase in haystack
                for haystack in query_haystacks
                for phrase in (
                    f"{normalized_query} tab",
                    f"{normalized_query} selected",
                    f"selected {normalized_query}",
                )
            )
        )
        context_menu_visible = "open_context_menu" in matched_actions or any(
            phrase in observation_text for phrase in ("context menu", "shortcut menu", "right click menu")
        )
        dialog_visible = bool(properties_dialog_visible or "open_print_dialog" in matched_actions or context_menu_visible is False and any(
            phrase in observation_text for phrase in ("dialog", "modal", "popup", "are you sure", "ok cancel", "apply")
        ))
        dismissible_surface_visible = bool(dialog_visible or context_menu_visible)
        flags: Dict[str, bool] = {
            "window_targeted": bool(target_window),
            "window_active": bool(target_window) and self._to_int(active_window.get("hwnd")) == self._to_int(target_window.get("hwnd")),
            "search_visible": bool({"search", "focus_search_box"} & matched_actions),
            "tab_strip_visible": tab_strip_visible,
            "tab_page_visible": tab_page_visible,
            "tab_target_active": tab_target_active,
            "tab_search_visible": tab_search_visible,
            "command_palette_visible": "command" in matched_actions,
            "quick_open_visible": "quick_open" in matched_actions,
            "conversation_picker_visible": conversation_picker_visible,
            "history_visible": history_visible,
            "downloads_visible": downloads_visible,
            "bookmarks_visible": bookmarks_visible,
            "devtools_visible": devtools_visible,
            "address_bar_ready": bool({"focus_address_bar", "navigate"} & matched_actions),
            "terminal_visible": "toggle_terminal" in matched_actions or "terminal_command" in matched_actions,
            "message_compose_ready": "send_message" in matched_actions or any(
                phrase in observation_text for phrase in ("type a message", "write a message", "reply")
            ),
            "print_dialog_visible": "open_print_dialog" in matched_actions or "print" in observation_text,
            "presentation_active": "start_presentation" in matched_actions or any(
                phrase in observation_text for phrase in ("slide show", "slideshow", "presenter view")
            ),
            "explorer_visible": explorer_visible,
            "workspace_search_visible": "workspace_search" in matched_actions,
            "replace_visible": replace_visible,
            "rename_active": rename_active,
            "symbol_picker_visible": "go_to_symbol" in matched_actions,
            "file_manager_ready": category == "file_manager",
            "folder_tree_visible": folder_tree_visible,
            "file_list_visible": file_list_visible,
            "tree_visible": tree_visible,
            "list_visible": list_visible,
            "table_visible": table_visible,
            "new_folder_visible": "new_folder" in matched_actions or "new folder" in observation_text,
            "properties_dialog_visible": properties_dialog_visible,
            "preview_pane_visible": preview_pane_visible,
            "details_pane_visible": details_pane_visible,
            "navigation_surface_ready": category in {"browser", "file_manager"} and bool(target_window or active_window),
            "browser_library_visible": bool(history_visible or downloads_visible or bookmarks_visible),
            "conversation_target_active": category in {"chat", "ai_companion"} and bool(normalized_query) and any(
                normalized_query in haystack for haystack in (target_title, active_title, observation_text) if haystack
            ),
            "conversation_ready": False,
            "email_compose_ready": email_compose_ready,
            "calendar_event_compose_ready": calendar_event_compose_ready,
            "calendar_view_active": calendar_view_active,
            "mail_view_active": mail_view_active,
            "people_view_active": people_view_active,
            "tasks_view_active": tasks_view_active,
            "folder_pane_visible": folder_pane_visible,
            "message_list_visible": message_list_visible,
            "reading_pane_visible": reading_pane_visible,
            "sidebar_visible": sidebar_visible,
            "main_content_visible": bool(main_content_visible or form_visible),
            "toolbar_visible": toolbar_visible,
            "scrollbar_visible": scrollbar_visible,
            "form_visible": form_visible,
            "input_field_visible": input_field_visible,
            "dropdown_visible": dropdown_visible,
            "dropdown_open": dropdown_open,
            "checkbox_visible": checkbox_visible,
            "checkbox_target_checked": checkbox_target_checked,
            "checkbox_target_unchecked": checkbox_target_unchecked,
            "radio_option_visible": radio_option_visible,
            "radio_target_selected": radio_target_selected,
            "toggle_visible": toggle_visible,
            "slider_visible": slider_visible,
            "spinner_visible": spinner_visible,
            "value_control_visible": value_control_visible,
            "context_menu_visible": context_menu_visible,
            "dialog_visible": dialog_visible,
            "dismissible_surface_visible": dismissible_surface_visible,
            "tabbed_surface_ready": category in {"browser", "code_editor", "ide", "terminal", "ops_console", "utility", "file_manager"} and bool(target_window or active_window),
            "zoomable_surface": category in {"browser", "code_editor", "ide", "office", "utility"} and bool(target_window or active_window),
            "media_surface_ready": category == "media" and bool(target_window or active_window),
            "settings_window_ready": any("settings" in haystack for haystack in (target_title, active_title, profile_name) if haystack),
            "task_manager_ready": any("task manager" in haystack for haystack in (target_title, active_title, profile_name) if haystack),
        }
        flags["conversation_ready"] = bool(
            flags.get("conversation_picker_visible")
            or flags.get("message_compose_ready")
            or flags.get("conversation_target_active")
        )
        if category == "browser":
            flags["browser_ready"] = bool(target_window)
        if category in {"code_editor", "ide"}:
            flags["editor_ready"] = bool(target_window)
        return flags

    def _surface_recommendations(self, *, workflow_surfaces: List[Dict[str, Any]]) -> List[str]:
        rows: List[str] = []
        for row in workflow_surfaces:
            if not isinstance(row, dict) or not bool(row.get("matched", False)):
                continue
            followups = row.get("recommended_followups", []) if isinstance(row.get("recommended_followups", []), list) else []
            for action_name in followups:
                clean = str(action_name or "").strip().lower()
                if clean and clean in WORKFLOW_ACTIONS and clean not in rows:
                    rows.append(clean)
        for row in workflow_surfaces:
            if not isinstance(row, dict):
                continue
            if bool(row.get("matched", False)):
                continue
            clean_action = str(row.get("action", "") or "").strip().lower()
            if clean_action and clean_action not in rows:
                rows.append(clean_action)
        return rows[:8]

    def _surface_safety_recommendations(self, *, safety_signals: Dict[str, Any]) -> List[str]:
        if not isinstance(safety_signals, dict):
            return []
        actions: List[str] = []
        dialog_state = safety_signals.get("dialog_state", {}) if isinstance(safety_signals.get("dialog_state", {}), dict) else {}
        dialog_kind = self._normalize_probe_text(dialog_state.get("dialog_kind", ""))
        approval_kind = self._normalize_probe_text(dialog_state.get("approval_kind", ""))
        dialog_review_required = bool(dialog_state.get("review_required", False))
        dialog_auto_resolve_supported = bool(dialog_state.get("auto_resolve_supported", False))
        dialog_manual_input_required = bool(dialog_state.get("manual_input_required", False))
        dialog_credential_required = bool(dialog_state.get("credential_required", False))
        preferred_action = self._normalize_probe_text(dialog_state.get("preferred_action", ""))
        if bool(safety_signals.get("wizard_next_available", False)):
            actions.append("next_wizard_step")
        if bool(safety_signals.get("wizard_finish_available", False)):
            actions.append("finish_wizard")
        if bool(safety_signals.get("wizard_back_available", False)):
            actions.append("previous_wizard_step")
        if dialog_manual_input_required or dialog_credential_required or approval_kind in {"credential_input", "elevation_credentials"}:
            dialog_actions = ["focus_input_field", "set_field_value"]
            if str(safety_signals.get("preferred_confirmation_button", "") or "").strip():
                dialog_actions.append("confirm_dialog")
            if str(safety_signals.get("preferred_dismiss_button", "") or "").strip():
                dialog_actions.append("dismiss_dialog")
            for action_name in dialog_actions:
                if action_name not in actions:
                    actions.append(action_name)
        elif bool(safety_signals.get("requires_confirmation", False)) or dialog_review_required or dialog_auto_resolve_supported:
            if dialog_review_required or approval_kind in {
                "elevation_consent",
                "permission_review",
                "authentication_review",
                "destructive_confirmation",
                "warning_confirmation",
            } or dialog_kind in {"elevation_prompt", "credential_prompt", "authentication_review", "permission_review"}:
                confirmation_actions = ["dismiss_dialog", "confirm_dialog"]
            elif preferred_action == "dismiss":
                confirmation_actions = ["dismiss_dialog", "confirm_dialog"]
            elif preferred_action == "confirm":
                confirmation_actions = ["confirm_dialog", "dismiss_dialog"]
            else:
                confirmation_actions = (
                    ["dismiss_dialog", "confirm_dialog"]
                    if bool(safety_signals.get("destructive_warning_visible", False))
                    else ["confirm_dialog", "dismiss_dialog"]
                )
            for action_name in confirmation_actions:
                if action_name not in actions:
                    actions.append(action_name)
        return actions[:6]

    def _surface_safety_signals(
        self,
        *,
        app_profile: Dict[str, Any],
        observation: Dict[str, Any],
        active_window: Dict[str, Any],
        target_window: Dict[str, Any],
        elements: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        profile_name = self._normalize_probe_text(app_profile.get("name", ""))
        category = self._normalize_probe_text(app_profile.get("category", ""))
        observation_text = self._normalize_probe_text(observation.get("text", ""))
        active_title = self._normalize_probe_text(active_window.get("title", ""))
        target_title = self._normalize_probe_text(target_window.get("title", ""))
        active_exe = str(active_window.get("exe", "") or "")
        target_exe = str(target_window.get("exe", "") or "")
        window_exe_text = " ".join(value.strip().lower() for value in (active_exe, target_exe) if str(value).strip())
        combined_text = " ".join(value for value in (observation_text, active_title, target_title, profile_name, category) if value)
        live_surface_text = " ".join(value for value in (observation_text, active_title, target_title) if value)
        element_rows = [dict(row) for row in (elements or []) if isinstance(row, dict)]

        def _element_control_type(row: Dict[str, Any]) -> str:
            return self._normalize_probe_text(row.get("control_type", ""))

        def _element_text(row: Dict[str, Any]) -> str:
            parts = [
                str(row.get("name", "") or ""),
                str(row.get("automation_id", "") or ""),
                str(row.get("class_name", "") or ""),
                str(row.get("control_type", "") or ""),
                str(row.get("state_text", "") or ""),
                str(row.get("value_text", "") or ""),
            ]
            return self._normalize_probe_text(" ".join(parts))

        def _normalize_button_label(value: Any) -> str:
            clean = self._normalize_probe_text(value)
            clean = clean.replace("&", " ")
            clean = re.sub(r"[^a-z0-9 ]+", " ", clean)
            clean = " ".join(clean.split())
            return clean

        button_labels: List[str] = []
        normalized_button_labels: List[str] = []
        for row in element_rows:
            control_type = _element_control_type(row)
            if control_type not in {"button", "splitbutton", "hyperlink", "link"}:
                continue
            raw_label = str(row.get("name", "") or row.get("automation_id", "") or "").strip()
            normalized_label = _normalize_button_label(raw_label)
            if not raw_label or not normalized_label or normalized_label in normalized_button_labels:
                continue
            button_labels.append(raw_label)
            normalized_button_labels.append(normalized_label)

        wizard_text_markers = (
            "setup wizard",
            "installation wizard",
            "install wizard",
            "installer",
            "installshield",
            "setup",
            "welcome to",
            "step 1 of",
            "step 2 of",
            "step 3 of",
            "step 4 of",
            "step 5 of",
        )
        warning_markers = (
            "warning",
            "attention",
            "are you sure",
            "review changes",
            "review the changes",
            "confirm",
            "security warning",
            "this action will",
        )
        destructive_markers = (
            "cannot be undone",
            "permanently",
            "permanent",
            "delete",
            "remove",
            "erase",
            "discard",
            "overwrite",
            "replace existing",
            "reset",
            "format",
            "uninstall",
            "make changes to your device",
        )
        strong_elevation_markers = (
            "user account control",
            "do you want to allow",
            "administrator permission",
            "admin permission",
            "requires administrator",
            "run as administrator",
            "elevation",
            "elevated",
            "uac",
        )
        weak_elevation_markers = (
            "make changes to your device",
        )
        secure_desktop_title_markers = (
            "user account control",
            "windows security",
            "credential ui",
        )
        secure_desktop_exe_markers = (
            "consent.exe",
            "credentialuibroker.exe",
            "logonui.exe",
        )
        permission_markers = (
            "allow access",
            "grant access",
            "permission",
            "let this app",
            "needs access",
            "access your camera",
            "access your microphone",
            "access your location",
            "access your contacts",
            "access your files",
            "access to your",
        )
        confirmation_markers = (
            "ok cancel",
            "yes no",
            "continue",
            "apply",
            "accept",
            "confirm",
            "allow",
        )
        safe_button_markers = (
            "cancel",
            "no",
            "close",
            "back",
            "later",
            "skip",
            "not now",
            "abort",
        )
        confirmation_button_markers = (
            "ok",
            "yes",
            "continue",
            "allow",
            "accept",
            "apply",
            "install",
            "finish",
            "next",
            "proceed",
            "launch",
            "delete",
            "remove",
            "erase",
            "overwrite",
            "replace",
            "reset",
            "format",
            "uninstall",
        )
        destructive_button_markers = (
            "delete",
            "remove",
            "erase",
            "overwrite",
            "replace",
            "reset",
            "format",
            "uninstall",
            "install",
            "finish",
            "apply",
            "launch",
        )
        auth_markers = (
            "sign in",
            "signin",
            "sign-in",
            "log in",
            "login",
            "authenticate",
            "authentication",
            "verify your identity",
            "verify it is you",
            "confirm your identity",
            "credentials",
            "windows security",
            "security verification",
            "account verification",
            "two-factor",
            "two factor",
            "2fa",
            "mfa",
            "one-time code",
            "one time code",
            "verification code",
        )
        credential_field_markers = (
            "user",
            "username",
            "user name",
            "email",
            "account",
            "password",
            "passcode",
            "pin",
            "otp",
            "security code",
            "verification code",
            "credentials",
        )
        username_field_markers = ("username", "user name", "email", "account", "login")
        password_field_markers = ("password", "passcode", "pin", "otp", "security code", "verification code")

        def _label_matches(candidates: tuple[str, ...]) -> bool:
            for label in normalized_button_labels:
                if any(
                    label == candidate
                    or label.startswith(f"{candidate} ")
                    or label.endswith(f" {candidate}")
                    for candidate in candidates
                ):
                    return True
            return False

        wizard_next_available = _label_matches(("next", "continue", "proceed"))
        wizard_back_available = _label_matches(("back", "previous", "prev"))
        wizard_finish_available = _label_matches(("finish", "done", "complete", "install", "launch"))
        confirmation_buttons_present = _label_matches(("ok", "yes", "continue", "allow", "accept", "apply", "install", "finish", "next"))
        dismiss_buttons_present = _label_matches(("cancel", "no", "close", "back", "later", "skip"))
        destructive_warning_visible = bool(any(marker in combined_text for marker in destructive_markers))
        secure_desktop_likely = bool(
            any(marker in live_surface_text for marker in secure_desktop_title_markers)
            or any(marker in window_exe_text for marker in secure_desktop_exe_markers)
        )
        elevation_prompt_visible = bool(
            any(marker in live_surface_text for marker in strong_elevation_markers)
            or (
                any(marker in live_surface_text for marker in weak_elevation_markers)
                and secure_desktop_likely
            )
        )
        dialog_button_targets = [
            self._element_state_summary(row)
            for row in element_rows
            if _element_control_type(row) in {"button", "splitbutton"}
        ][:12]

        def _target_label_matches(target: Dict[str, Any], candidates: tuple[str, ...]) -> bool:
            label = self._normalize_probe_text(target.get("name", ""))
            if not label:
                return False
            return any(
                label == candidate
                or label.startswith(f"{candidate} ")
                or label.endswith(f" {candidate}")
                for candidate in candidates
            )

        safe_dialog_targets = [
            target
            for target in dialog_button_targets
            if _target_label_matches(target, safe_button_markers)
        ]
        confirmation_dialog_targets = [
            target
            for target in dialog_button_targets
            if _target_label_matches(target, confirmation_button_markers)
        ]
        destructive_dialog_targets = [
            target
            for target in dialog_button_targets
            if _target_label_matches(target, destructive_button_markers)
            or (
                destructive_warning_visible
                and _target_label_matches(target, ("continue", "ok", "yes", "accept", "apply", "install", "finish", "next", "proceed", "launch"))
            )
        ]

        def _preferred_target(targets: List[Dict[str, Any]], priority: tuple[str, ...]) -> Dict[str, Any]:
            if not targets:
                return {}
            def _rank(target: Dict[str, Any]) -> tuple[int, int, str]:
                label = self._normalize_probe_text(target.get("name", ""))
                enabled = self._coerce_surface_bool(target.get("enabled"))
                visible = self._coerce_surface_bool(target.get("visible"))
                for index, candidate in enumerate(priority):
                    if label == candidate or label.startswith(f"{candidate} ") or label.endswith(f" {candidate}"):
                        return (0 if enabled is not False and visible is not False else 1, index, label)
                return (0 if enabled is not False and visible is not False else 1, len(priority), label)
            return dict(sorted(targets, key=_rank)[0])

        preferred_confirmation_target = _preferred_target(
            confirmation_dialog_targets,
            ("continue", "next", "ok", "yes", "apply", "install", "finish", "launch", "accept", "allow"),
        )
        preferred_dismiss_target = _preferred_target(
            safe_dialog_targets,
            ("cancel", "back", "no", "close", "later", "skip", "not now", "abort"),
        )
        input_rows = [
            row
            for row in element_rows
            if _element_control_type(row) in {"edit", "combobox"}
        ]
        interactive_non_button_rows = [
            row
            for row in element_rows
            if _element_control_type(row)
            in {
                "checkbox",
                "radiobutton",
                "combobox",
                "edit",
                "slider",
                "spinner",
                "togglebutton",
                "tabitem",
                "treeitem",
                "listitem",
                "menuitem",
                "dataitem",
                "table",
                "toolbar",
            }
        ]

        def _dedupe_targets(targets: List[Dict[str, Any]], *, limit: int = 6) -> List[Dict[str, Any]]:
            rows: List[Dict[str, Any]] = []
            seen: set[str] = set()
            for target in targets:
                if not isinstance(target, dict):
                    continue
                target_key = self._element_identity_key(target)
                fallback_key = self._normalize_probe_text(target.get("name", ""))
                identity = target_key or fallback_key
                if not identity or identity in seen:
                    continue
                seen.add(identity)
                rows.append(dict(target))
                if len(rows) >= limit:
                    break
            return rows

        username_field_targets = _dedupe_targets(
            [
                self._element_state_summary(row)
                for row in input_rows
                if any(marker in _element_text(row) for marker in username_field_markers)
            ]
        )
        password_field_targets = _dedupe_targets(
            [
                self._element_state_summary(row)
                for row in input_rows
                if any(marker in _element_text(row) for marker in password_field_markers)
            ]
        )
        credential_field_targets = _dedupe_targets(
            [
                *username_field_targets,
                *password_field_targets,
                *[
                    self._element_state_summary(row)
                    for row in input_rows
                    if any(marker in _element_text(row) for marker in credential_field_markers)
                ],
            ]
        )
        auth_surface_visible = bool(any(marker in combined_text for marker in auth_markers))
        credential_prompt_visible = bool(
            (auth_surface_visible or any(marker in combined_text for marker in credential_field_markers))
            and (
                credential_field_targets
                or any(
                    marker in live_surface_text
                    for marker in (
                        "enter your credentials",
                        "sign in required",
                        "credentials required",
                        "windows security",
                        "enter password",
                    )
                )
            )
        )
        if credential_prompt_visible and not credential_field_targets and input_rows:
            credential_field_targets = _dedupe_targets([self._element_state_summary(row) for row in input_rows[:2]])
        authentication_prompt_visible = bool(auth_surface_visible or credential_prompt_visible)
        permission_review_visible = bool(
            not authentication_prompt_visible
            and any(marker in live_surface_text for marker in permission_markers)
            and (dialog_button_targets or confirmation_buttons_present or dismiss_buttons_present)
        )

        wizard_surface_visible = bool(
            any(marker in live_surface_text for marker in wizard_text_markers)
            or (
                bool(normalized_button_labels)
                and (wizard_next_available or wizard_back_available or wizard_finish_available)
                and any(label in {"cancel", "close", "back"} for label in normalized_button_labels)
            )
        )
        warning_surface_visible = bool(
            any(marker in live_surface_text for marker in warning_markers)
            or (wizard_surface_visible and any(marker in live_surface_text for marker in ("license", "agreement", "ready to install", "review")))
        )
        requires_confirmation = bool(
            warning_surface_visible
            or destructive_warning_visible
            or elevation_prompt_visible
            or any(marker in combined_text for marker in confirmation_markers)
            or (confirmation_buttons_present and dismiss_buttons_present)
        )
        dialog_visible = bool(
            button_labels
            or credential_field_targets
            or authentication_prompt_visible
            or permission_review_visible
            or elevation_prompt_visible
            or any(marker in live_surface_text for marker in ("dialog", "modal", "popup", "ok cancel", "yes no"))
        )
        authentication_review_visible = bool(authentication_prompt_visible and not credential_prompt_visible and not elevation_prompt_visible)
        dialog_kind = ""
        if dialog_visible:
            if elevation_prompt_visible:
                dialog_kind = "elevation_prompt"
            elif credential_prompt_visible:
                dialog_kind = "credential_prompt"
            elif authentication_review_visible:
                dialog_kind = "authentication_review"
            elif permission_review_visible:
                dialog_kind = "permission_review"
            elif destructive_warning_visible:
                dialog_kind = "destructive_confirmation"
            elif warning_surface_visible:
                dialog_kind = "warning_confirmation"
            elif preferred_confirmation_target or preferred_dismiss_target:
                dialog_kind = "acknowledgement"
            else:
                dialog_kind = "generic_dialog"
        review_required = bool(
            dialog_kind in {"elevation_prompt", "credential_prompt", "authentication_review", "permission_review", "destructive_confirmation", "warning_confirmation"}
        )
        manual_input_required = bool(
            credential_field_targets
            and dialog_kind in {"credential_prompt", "elevation_prompt"}
        )
        approval_kind = ""
        if dialog_kind == "elevation_prompt":
            approval_kind = "elevation_credentials" if manual_input_required else "elevation_consent"
        elif dialog_kind == "credential_prompt":
            approval_kind = "credential_input"
        elif dialog_kind == "authentication_review":
            approval_kind = "authentication_review"
        elif dialog_kind == "permission_review":
            approval_kind = "permission_review"
        elif dialog_kind == "destructive_confirmation":
            approval_kind = "destructive_confirmation"
        elif dialog_kind == "warning_confirmation":
            approval_kind = "warning_confirmation"
        elif dialog_kind == "acknowledgement":
            approval_kind = "acknowledgement"
        preferred_action = ""
        if manual_input_required:
            preferred_action = "input_required"
        elif review_required and preferred_dismiss_target:
            preferred_action = "dismiss"
        elif preferred_confirmation_target:
            preferred_action = "confirm"
        elif preferred_dismiss_target:
            preferred_action = "dismiss"
        elif review_required:
            preferred_action = "review"
        auto_resolve_supported = bool(
            dialog_visible
            and not review_required
            and not manual_input_required
            and (preferred_confirmation_target or preferred_dismiss_target)
            and not interactive_non_button_rows
        )
        dialog_state = {
            "visible": dialog_visible,
            "dialog_kind": dialog_kind,
            "approval_kind": approval_kind,
            "review_required": review_required,
            "auto_resolve_supported": auto_resolve_supported,
            "manual_input_required": manual_input_required,
            "credential_required": bool(credential_prompt_visible or approval_kind == "elevation_credentials"),
            "authentication_required": authentication_prompt_visible,
            "approval_required": review_required,
            "admin_approval_required": bool(approval_kind in {"elevation_consent", "elevation_credentials"}),
            "permission_review_required": bool(approval_kind == "permission_review"),
            "privileged_operation": bool(elevation_prompt_visible or destructive_warning_visible or permission_review_visible),
            "preferred_action": preferred_action,
            "preferred_button": str(preferred_confirmation_target.get("name", "") or preferred_dismiss_target.get("name", "") or "").strip(),
            "credential_fields": credential_field_targets[:6],
            "credential_field_count": len(credential_field_targets),
            "username_field_count": len(username_field_targets),
            "password_field_count": len(password_field_targets),
            "secure_desktop_likely": secure_desktop_likely,
            "button_only": bool(dialog_visible and not interactive_non_button_rows),
            "safe_button_count": len(safe_dialog_targets),
            "confirmation_button_count": len(confirmation_dialog_targets),
            "destructive_button_count": len(destructive_dialog_targets),
        }

        return {
            "wizard_surface_visible": wizard_surface_visible,
            "wizard_next_available": wizard_surface_visible and wizard_next_available,
            "wizard_back_available": wizard_surface_visible and wizard_back_available,
            "wizard_finish_available": wizard_surface_visible and wizard_finish_available,
            "warning_surface_visible": warning_surface_visible,
            "destructive_warning_visible": destructive_warning_visible,
            "elevation_prompt_visible": elevation_prompt_visible,
            "permission_review_visible": permission_review_visible,
            "requires_confirmation": requires_confirmation,
            "dialog_visible": dialog_visible,
            "dialog_review_required": review_required,
            "authentication_prompt_visible": authentication_prompt_visible,
            "credential_prompt_visible": credential_prompt_visible,
            "secure_desktop_likely": secure_desktop_likely,
            "admin_approval_required": bool(dialog_state.get("admin_approval_required", False)),
            "dialog_buttons": button_labels,
            "dialog_button_targets": dialog_button_targets,
            "safe_dialog_buttons": [str(target.get("name", "") or "").strip() for target in safe_dialog_targets if str(target.get("name", "") or "").strip()],
            "confirmation_dialog_buttons": [str(target.get("name", "") or "").strip() for target in confirmation_dialog_targets if str(target.get("name", "") or "").strip()],
            "destructive_dialog_buttons": [str(target.get("name", "") or "").strip() for target in destructive_dialog_targets if str(target.get("name", "") or "").strip()],
            "preferred_confirmation_button": str(preferred_confirmation_target.get("name", "") or "").strip(),
            "preferred_dismiss_button": str(preferred_dismiss_target.get("name", "") or "").strip(),
            "preferred_confirmation_target": preferred_confirmation_target,
            "preferred_dismiss_target": preferred_dismiss_target,
            "dialog_state": dialog_state,
            "accessible_dialog_elements": [
                row for row in element_rows if _element_control_type(row) in {"button", "text", "document", "pane", "window"} and _element_text(row)
            ][:12],
        }

    @classmethod
    def _wizard_page_state(
        cls,
        *,
        observation: Dict[str, Any],
        elements: Any,
        safety_signals: Any,
    ) -> Dict[str, Any]:
        safety_payload = dict(safety_signals) if isinstance(safety_signals, dict) else {}
        dialog_state = safety_payload.get("dialog_state", {}) if isinstance(safety_payload.get("dialog_state", {}), dict) else {}
        if not bool(safety_payload.get("wizard_surface_visible", False)):
            return {}
        observation_text = cls._normalize_probe_text(observation.get("text", ""))
        element_rows = [dict(row) for row in elements if isinstance(row, dict)] if isinstance(elements, list) else []
        dialog_kind = cls._normalize_probe_text(dialog_state.get("dialog_kind", ""))
        approval_kind = cls._normalize_probe_text(dialog_state.get("approval_kind", ""))
        dialog_review_required = bool(dialog_state.get("review_required", False))
        dialog_auto_resolve_supported = bool(dialog_state.get("auto_resolve_supported", False))
        secure_desktop_likely = bool(dialog_state.get("secure_desktop_likely", False))
        credential_field_count = int(dialog_state.get("credential_field_count", 0) or 0)

        def _control_type(row: Dict[str, Any]) -> str:
            return cls._normalize_probe_text(row.get("control_type", ""))

        def _label(row: Dict[str, Any]) -> str:
            return str(row.get("name", "") or "").strip()

        def _label_text(row: Dict[str, Any]) -> str:
            return cls._normalize_probe_text(row.get("name", ""))

        checkbox_rows = [row for row in element_rows if _control_type(row) == "checkbox"]
        radio_rows = [row for row in element_rows if _control_type(row) == "radiobutton"]
        combo_rows = [row for row in element_rows if _control_type(row) == "combobox"]
        edit_rows = [row for row in element_rows if _control_type(row) == "edit"]
        positive_acceptance_markers = ("accept", "agree", "i agree", "i accept", "accept the", "agree to", "license", "terms", "eula")
        negative_acceptance_markers = ("decline", "do not", "don't", "refuse", "reject")

        pending_requirements: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()

        def _append_pending(row: Dict[str, Any], requirement_type: str, reason: str) -> None:
            row_id = cls._element_identity_key(row)
            if row_id and row_id in seen_ids:
                return
            if row_id:
                seen_ids.add(row_id)
            pending_requirements.append(
                {
                    **cls._element_state_summary(row),
                    "requirement_type": requirement_type,
                    "reason": reason,
                }
            )

        for row in checkbox_rows:
            label_text = _label_text(row)
            if not label_text or not any(marker in label_text for marker in positive_acceptance_markers):
                continue
            if cls._coerce_surface_bool(row.get("checked")) is True:
                continue
            _append_pending(row, "checkbox", "Wizard page appears to require an agreement or license checkbox before continuing.")

        for row in radio_rows:
            label_text = _label_text(row)
            if not label_text or any(marker in label_text for marker in negative_acceptance_markers):
                continue
            if not any(marker in label_text for marker in positive_acceptance_markers):
                continue
            if cls._coerce_surface_bool(row.get("selected")) is True or cls._coerce_surface_bool(row.get("checked")) is True:
                continue
            _append_pending(row, "radio", "Wizard page appears to require selecting an agreement or acceptance option before continuing.")

        page_kind = "wizard_page"
        if any(marker in observation_text for marker in ("license", "agreement", "terms", "eula")) or pending_requirements:
            page_kind = "license_agreement"
        elif any(marker in observation_text for marker in ("ready to install", "ready to begin installation", "install now", "review the changes")):
            page_kind = "ready_to_install"
        elif any(marker in observation_text for marker in ("completed", "successfully installed", "setup has finished", "installation complete")):
            page_kind = "completion"
        elif bool(safety_payload.get("warning_surface_visible", False)):
            page_kind = "warning_confirmation"
        elif combo_rows or edit_rows or checkbox_rows or radio_rows:
            page_kind = "options"

        preferred_confirmation_target = safety_payload.get("preferred_confirmation_target", {})
        preferred_confirmation_button = str(safety_payload.get("preferred_confirmation_button", "") or "").strip()
        advance_action = "finish_wizard" if cls._normalize_probe_text(preferred_confirmation_button) in {"finish", "done", "complete", "install", "apply", "launch"} else "next_wizard_step"
        if not preferred_confirmation_button and bool(safety_payload.get("wizard_finish_available", False)):
            advance_action = "finish_wizard"
        elif not preferred_confirmation_button and bool(safety_payload.get("wizard_next_available", False)):
            advance_action = "next_wizard_step"
        ready_for_advance = not pending_requirements and bool(
            preferred_confirmation_button
            or safety_payload.get("wizard_next_available", False)
            or safety_payload.get("wizard_finish_available", False)
        )
        auto_resolve_supported = bool(
            preferred_confirmation_button
            and all(str(row.get("requirement_type", "") or "").strip().lower() in {"checkbox", "radio"} for row in pending_requirements)
        )
        available_controls = [
            cls._element_state_summary(row)
            for row in [*checkbox_rows, *radio_rows, *combo_rows, *edit_rows]
        ][:12]
        manual_input_likely = bool(
            not pending_requirements
            and not ready_for_advance
            and any(
                cls._normalize_probe_text(row.get("control_type", "")) in {"edit", "combobox"}
                for row in available_controls
                if isinstance(row, dict)
            )
        )
        if dialog_kind == "credential_prompt" and credential_field_count > 0:
            manual_input_likely = True
        autonomous_blocker = ""
        if approval_kind == "elevation_credentials":
            autonomous_blocker = "elevation_credentials_required"
        elif approval_kind == "elevation_consent" and page_kind not in {"ready_to_install", "completion"}:
            autonomous_blocker = "elevation_consent_required"
        elif approval_kind == "permission_review":
            autonomous_blocker = "permission_review_required"
        elif dialog_kind == "credential_prompt" and credential_field_count > 0:
            autonomous_blocker = "credential_input_required"
        elif dialog_kind == "authentication_review":
            autonomous_blocker = "authentication_review_required"
        elif dialog_kind == "elevation_prompt" and page_kind not in {"ready_to_install", "completion"}:
            autonomous_blocker = "elevation_prompt_requires_approval"
        elif page_kind == "warning_confirmation":
            autonomous_blocker = "warning_confirmation_requires_review"
        elif pending_requirements and not auto_resolve_supported:
            autonomous_blocker = "unsupported_wizard_requirements"
        elif manual_input_likely:
            autonomous_blocker = "manual_input_required"
        elif not ready_for_advance and not preferred_confirmation_button:
            autonomous_blocker = "no_advance_control_available"
        autonomous_progress_supported = not autonomous_blocker and bool(
            ready_for_advance
            or auto_resolve_supported
            or page_kind in {"ready_to_install", "completion"}
        )
        return {
            "page_kind": page_kind,
            "advance_action": advance_action,
            "ready_for_advance": ready_for_advance,
            "auto_resolve_supported": auto_resolve_supported,
            "autonomous_progress_supported": autonomous_progress_supported,
            "autonomous_blocker": autonomous_blocker,
            "manual_input_likely": manual_input_likely,
            "pending_requirements": pending_requirements[:8],
            "pending_requirement_count": len(pending_requirements),
            "available_controls": available_controls,
            "preferred_confirmation_button": preferred_confirmation_button,
            "preferred_confirmation_target": preferred_confirmation_target if isinstance(preferred_confirmation_target, dict) else {},
            "preferred_dismiss_button": str(safety_payload.get("preferred_dismiss_button", "") or "").strip(),
            "safe_exit_options": [str(item).strip() for item in safety_payload.get("safe_dialog_buttons", []) if str(item).strip()][:6],
            "destructive_options": [str(item).strip() for item in safety_payload.get("destructive_dialog_buttons", []) if str(item).strip()][:6],
            "dialog_kind": dialog_kind,
            "approval_kind": approval_kind,
            "dialog_review_required": dialog_review_required,
            "dialog_auto_resolve_supported": dialog_auto_resolve_supported,
            "secure_desktop_likely": secure_desktop_likely,
            "credential_field_count": credential_field_count,
            "notes": [
                note
                for note in [
                    "Wizard page exposes agreement or acceptance prerequisites." if pending_requirements else "",
                    "Preferred confirmation button is available through accessibility." if preferred_confirmation_button else "",
                    "Page likely requires installation or finish confirmation." if page_kind in {"ready_to_install", "completion"} else "",
                    "Current setup surface appears to require credentials or sign-in input." if dialog_kind == "credential_prompt" and credential_field_count > 0 else "",
                    "Current setup surface appears to require administrator approval." if approval_kind == "elevation_consent" else "",
                    "Current setup surface appears to require administrator credentials." if approval_kind == "elevation_credentials" else "",
                    "Current setup surface appears to require permission or consent review." if approval_kind == "permission_review" else "",
                    "The current setup prompt appears to be on a secure desktop surface." if secure_desktop_likely else "",
                ]
                if note
            ],
        }

    @classmethod
    def _form_page_state(
        cls,
        *,
        observation: Dict[str, Any],
        elements: Any,
        safety_signals: Any,
        surface_flags: Any,
    ) -> Dict[str, Any]:
        safety_payload = dict(safety_signals) if isinstance(safety_signals, dict) else {}
        flags = dict(surface_flags) if isinstance(surface_flags, dict) else {}
        dialog_state = safety_payload.get("dialog_state", {}) if isinstance(safety_payload.get("dialog_state", {}), dict) else {}
        if bool(safety_payload.get("wizard_surface_visible", False)):
            return {}
        dialog_kind = cls._normalize_probe_text(dialog_state.get("dialog_kind", ""))
        approval_kind = cls._normalize_probe_text(dialog_state.get("approval_kind", ""))
        dialog_review_required = bool(dialog_state.get("review_required", False))
        dialog_auto_resolve_supported = bool(dialog_state.get("auto_resolve_supported", False))
        dialog_manual_input_required = bool(dialog_state.get("manual_input_required", False))
        secure_desktop_likely = bool(dialog_state.get("secure_desktop_likely", False))
        credential_field_count = int(dialog_state.get("credential_field_count", 0) or 0)
        settings_navigation_surface = bool(
            flags.get("settings_window_ready", False)
            and (
                flags.get("sidebar_visible", False)
                or flags.get("tree_visible", False)
                or flags.get("list_visible", False)
            )
        )
        if not bool(
            flags.get("form_visible", False)
            or flags.get("dialog_visible", False)
            or flags.get("tab_page_visible", False)
            or safety_payload.get("requires_confirmation", False)
            or settings_navigation_surface
        ):
            return {}
        observation_text = cls._normalize_probe_text(observation.get("text", ""))
        element_rows = [dict(row) for row in elements if isinstance(row, dict)] if isinstance(elements, list) else []

        def _control_type(row: Dict[str, Any]) -> str:
            return cls._normalize_probe_text(row.get("control_type", ""))

        def _label(row: Dict[str, Any]) -> str:
            return str(row.get("name", "") or row.get("automation_id", "") or "").strip()

        def _label_text(row: Dict[str, Any]) -> str:
            return cls._normalize_probe_text(_label(row))

        checkbox_rows = [row for row in element_rows if _control_type(row) == "checkbox"]
        radio_rows = [row for row in element_rows if _control_type(row) == "radiobutton"]
        combo_rows = [row for row in element_rows if _control_type(row) == "combobox"]
        edit_rows = [row for row in element_rows if _control_type(row) == "edit"]
        value_rows = [row for row in element_rows if _control_type(row) in {"slider", "spinner"}]
        tab_rows = [row for row in element_rows if _control_type(row) == "tabitem"]
        tree_item_rows = [row for row in element_rows if _control_type(row) == "treeitem"]
        list_item_rows = [row for row in element_rows if _control_type(row) == "listitem"]
        menu_item_rows = [row for row in element_rows if _control_type(row) == "menuitem"]
        button_rows = [row for row in element_rows if _control_type(row) in {"button", "splitbutton"}]
        hyperlink_rows = [row for row in element_rows if _control_type(row) == "hyperlink"]
        group_rows = [row for row in element_rows if _control_type(row) == "group"]
        available_tabs = [
            cls._element_state_summary(row)
            for row in tab_rows
            if str(row.get("name", "") or row.get("automation_id", "") or "").strip()
        ][:12]
        selected_tab_target = next(
            (
                row
                for row in available_tabs
                if cls._coerce_surface_bool(row.get("selected")) is True
            ),
            {},
        )
        selected_tab = str(selected_tab_target.get("name", "") or "").strip()

        acknowledgement_markers = ("accept", "agree", "acknowledge", "understand", "confirm", "consent", "reviewed", "review")
        negative_markers = ("decline", "do not", "don't", "refuse", "reject")
        required_input_markers = ("name", "path", "folder", "directory", "location", "address", "email", "server", "key", "token", "value", "username", "password")
        commit_markers = ("save", "apply", "ok", "done", "submit", "continue", "finish", "next", "confirm")
        safe_exit_markers = ("cancel", "close", "back", "later", "skip", "not now")
        expandable_group_markers = (
            "advanced",
            "more",
            "additional",
            "details",
            "extra",
            "optional",
            "show",
            "hide",
            "expand",
            "collapse",
            "related settings",
            "advanced settings",
            "more settings",
            "advanced display",
            "advanced options",
        )
        drilldown_target_markers = (
            "advanced",
            "more",
            "additional",
            "details",
            "settings",
            "options",
            "properties",
            "configure",
            "manage",
            "related",
            "adapter",
            "open",
            "view",
            "show",
        )

        pending_requirements: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()

        def _append_pending(row: Dict[str, Any], requirement_type: str, reason: str) -> None:
            row_id = cls._element_identity_key(row)
            if row_id and row_id in seen_ids:
                return
            if row_id:
                seen_ids.add(row_id)
            pending_requirements.append(
                {
                    **cls._element_state_summary(row),
                    "requirement_type": requirement_type,
                    "reason": reason,
                }
            )

        for row in checkbox_rows:
            label_text = _label_text(row)
            if not label_text or any(marker in label_text for marker in negative_markers):
                continue
            if not any(marker in label_text for marker in acknowledgement_markers):
                continue
            if cls._coerce_surface_bool(row.get("checked")) is True:
                continue
            _append_pending(row, "checkbox", "The form appears to require acknowledging or accepting a review checkbox before the changes can be committed.")

        for row in radio_rows:
            label_text = _label_text(row)
            if not label_text or any(marker in label_text for marker in negative_markers):
                continue
            if not any(marker in label_text for marker in acknowledgement_markers):
                continue
            if cls._coerce_surface_bool(row.get("selected")) is True or cls._coerce_surface_bool(row.get("checked")) is True:
                continue
            _append_pending(row, "radio", "The form appears to require choosing an acknowledgement or confirmation option before the changes can be committed.")

        def _button_priority(row: Dict[str, Any]) -> tuple[int, str]:
            label_text = _label_text(row)
            for index, marker in enumerate(commit_markers):
                if label_text == marker or label_text.startswith(f"{marker} ") or label_text.endswith(f" {marker}"):
                    enabled = cls._coerce_surface_bool(row.get("enabled"))
                    visible = cls._coerce_surface_bool(row.get("visible"))
                    return (0 if enabled is not False and visible is not False else 1, f"{index:02d}:{label_text}")
            return (2, label_text)

        commit_targets = [
            cls._element_state_summary(row)
            for row in sorted(button_rows, key=_button_priority)
            if _button_priority(row)[0] < 2
        ]
        preferred_commit_target = dict(commit_targets[0]) if commit_targets else {}
        preferred_commit_button = str(preferred_commit_target.get("name", "") or "").strip()
        safe_exit_options = [
            _label(row)
            for row in button_rows
            if any(marker in _label_text(row) for marker in safe_exit_markers)
        ]
        destructive_options = [str(item).strip() for item in safety_payload.get("destructive_dialog_buttons", []) if str(item).strip()]

        page_kind = "form_page"
        if dialog_kind == "credential_prompt":
            page_kind = "credential_dialog"
        elif dialog_kind == "authentication_review":
            page_kind = "authentication_dialog"
        elif dialog_kind == "permission_review":
            page_kind = "permission_dialog"
        elif dialog_kind == "elevation_prompt":
            page_kind = "elevation_dialog"
        elif dialog_kind == "destructive_confirmation":
            page_kind = "destructive_confirmation"
        elif bool(tab_rows) or any(marker in observation_text for marker in ("properties", "options", "property sheet", "tab page")):
            page_kind = "property_sheet"
        elif bool(safety_payload.get("warning_surface_visible", False)) or bool(safety_payload.get("requires_confirmation", False)) or dialog_review_required:
            page_kind = "review_confirmation"
        elif any(marker in observation_text for marker in ("settings", "preferences", "configuration", "options")):
            page_kind = "settings_form"
        elif bool(flags.get("dialog_visible", False)):
            page_kind = "dialog_form"
        sidebar_hint_visible = bool(
            flags.get("sidebar_visible", False)
            or any(marker in observation_text for marker in ("sidebar", "navigation", "left pane", "side panel"))
        )
        selected_list_navigation = any(
            cls._coerce_surface_bool(row.get("selected")) is True or cls._coerce_surface_bool(row.get("checked")) is True
            for row in [*list_item_rows, *menu_item_rows]
        )
        sidebar_surface_visible = bool(
            sidebar_hint_visible
            or (
                page_kind in {"settings_form", "property_sheet"}
                and len([*list_item_rows, *menu_item_rows]) >= 2
                and selected_list_navigation
            )
        )
        tree_surface_visible = bool(
            flags.get("tree_visible", False)
            or bool(tree_item_rows)
            or any(marker in observation_text for marker in ("tree view", "navigation tree", "nodes", "folder tree"))
        )
        list_surface_visible = bool(
            flags.get("list_visible", False)
            or any(marker in observation_text for marker in ("results list", "items list", "list view", "list pane"))
            or (bool([*list_item_rows, *menu_item_rows]) and not sidebar_surface_visible)
        )

        def _row_empty(row: Dict[str, Any]) -> bool:
            value_text = str(row.get("value_text", "") or "").strip()
            state_text = str(row.get("state_text", "") or "").strip()
            return not value_text and not state_text

        manual_required_controls = [
            cls._element_state_summary(row)
            for row in [*edit_rows, *combo_rows]
            if _row_empty(row) and any(marker in _label_text(row) for marker in required_input_markers)
        ][:8]
        if dialog_manual_input_required:
            for row in dialog_state.get("credential_fields", []):
                if not isinstance(row, dict):
                    continue
                element_id = str(row.get("element_id", "") or "").strip()
                if element_id and any(str(existing.get("element_id", "") or "").strip() == element_id for existing in manual_required_controls if isinstance(existing, dict)):
                    continue
                if len(manual_required_controls) >= 8:
                    break
                manual_required_controls.append(dict(row))
        manual_input_likely = bool((manual_required_controls and not pending_requirements) or dialog_manual_input_required)
        ready_for_commit = not pending_requirements and bool(preferred_commit_button)
        auto_resolve_supported = bool(
            preferred_commit_button
            and all(str(row.get("requirement_type", "") or "").strip().lower() in {"checkbox", "radio"} for row in pending_requirements)
        )
        autonomous_blocker = ""
        if approval_kind == "elevation_credentials":
            autonomous_blocker = "elevation_credentials_required"
        elif approval_kind == "elevation_consent":
            autonomous_blocker = "elevation_consent_required"
        elif approval_kind == "permission_review":
            autonomous_blocker = "permission_review_required"
        elif dialog_kind == "elevation_prompt":
            autonomous_blocker = "elevation_confirmation_required"
        elif dialog_kind == "credential_prompt" and credential_field_count > 0:
            autonomous_blocker = "credential_input_required"
        elif dialog_kind == "authentication_review":
            autonomous_blocker = "authentication_review_required"
        elif pending_requirements and not auto_resolve_supported:
            autonomous_blocker = "unsupported_form_requirements"
        elif manual_input_likely:
            autonomous_blocker = "manual_input_required"
        elif not preferred_commit_button:
            autonomous_blocker = "no_commit_target_available"
        autonomous_progress_supported = not autonomous_blocker and bool(ready_for_commit or auto_resolve_supported)
        commit_action = "press_dialog_button" if preferred_commit_button else ""
        available_controls = [
            cls._element_state_summary(row)
            for row in [*checkbox_rows, *radio_rows, *combo_rows, *edit_rows, *value_rows, *tab_rows, *button_rows]
        ][:16]
        navigation_candidates: List[Dict[str, Any]] = []
        navigation_seen: set[str] = set()
        drilldown_targets: List[Dict[str, Any]] = []
        drilldown_seen: set[str] = set()
        expandable_groups: List[Dict[str, Any]] = []
        expandable_seen: set[str] = set()

        def _navigation_action_for_row(row: Dict[str, Any]) -> tuple[str, str]:
            control_type = _control_type(row)
            label_text = _label_text(row)
            if control_type == "treeitem" and tree_surface_visible:
                return ("select_tree_item", "tree_item")
            if control_type in {"listitem", "menuitem"} and sidebar_surface_visible:
                return ("select_sidebar_item", "sidebar_item")
            if control_type in {"listitem", "menuitem"} and list_surface_visible:
                return ("select_list_item", "list_item")
            if control_type in {"button", "splitbutton"} and sidebar_surface_visible:
                if not label_text or any(marker in label_text for marker in [*commit_markers, *safe_exit_markers]):
                    return ("", "")
                return ("select_sidebar_item", "sidebar_item")
            return ("", "")

        def _append_navigation_candidate(row: Dict[str, Any]) -> None:
            action_name, navigation_role = _navigation_action_for_row(row)
            if not action_name:
                return
            summary = cls._element_state_summary(row)
            candidate_name = str(summary.get("name", "") or "").strip()
            if not candidate_name:
                return
            candidate_key = str(summary.get("element_id", "") or "").strip() or f"{action_name}:{cls._normalize_probe_text(candidate_name)}"
            if not candidate_key or candidate_key in navigation_seen:
                return
            navigation_seen.add(candidate_key)
            navigation_candidates.append(
                {
                    **summary,
                    "navigation_action": action_name,
                    "navigation_role": navigation_role,
                }
            )

        def _drilldown_target_config(row: Dict[str, Any]) -> tuple[str, str]:
            control_type = _control_type(row)
            label_text = _label_text(row)
            state_text = cls._normalize_probe_text(row.get("state_text", ""))
            expanded_state = cls._coerce_surface_bool(row.get("expanded"))
            if not label_text:
                return ("", "")
            if any(marker in label_text for marker in [*commit_markers, *safe_exit_markers]):
                return ("", "")
            if control_type not in {"button", "splitbutton", "hyperlink", "listitem", "menuitem", "treeitem"}:
                return ("", "")
            if expanded_state is not None and control_type in {"button", "splitbutton", "treeitem"}:
                return ("", "")
            if control_type in {"listitem", "menuitem", "treeitem"} and sidebar_surface_visible:
                return ("", "")
            if not any(marker in label_text or marker in state_text for marker in drilldown_target_markers):
                return ("", "")
            return ("open_subpage", "double_click" if control_type == "treeitem" else "click")

        def _append_drilldown_target(row: Dict[str, Any]) -> None:
            drilldown_action, invoke_action = _drilldown_target_config(row)
            if not drilldown_action:
                return
            summary = cls._element_state_summary(row)
            candidate_name = str(summary.get("name", "") or "").strip()
            if not candidate_name:
                return
            candidate_key = str(summary.get("element_id", "") or "").strip() or f"{drilldown_action}:{cls._normalize_probe_text(candidate_name)}"
            if not candidate_key or candidate_key in drilldown_seen:
                return
            drilldown_seen.add(candidate_key)
            drilldown_targets.append(
                {
                    **summary,
                    "drilldown_action": drilldown_action,
                    "invoke_action": invoke_action,
                }
            )

        def _expandable_group_config(row: Dict[str, Any]) -> tuple[str, str]:
            control_type = _control_type(row)
            label_text = _label_text(row)
            state_text = cls._normalize_probe_text(row.get("state_text", ""))
            expanded_state = cls._coerce_surface_bool(row.get("expanded"))
            if not label_text:
                return ("", "")
            if any(marker in label_text for marker in [*commit_markers, *safe_exit_markers]):
                return ("", "")
            if control_type == "treeitem":
                if expanded_state is None and not any(marker in label_text or marker in state_text for marker in expandable_group_markers):
                    return ("", "")
                return ("expand_tree_item", "double_click")
            if control_type == "group":
                return ("expand_group", "click")
            if expanded_state is not None:
                return ("expand_group", "click")
            if control_type in {"button", "splitbutton", "hyperlink"} and any(
                marker in label_text or marker in state_text
                for marker in expandable_group_markers
            ):
                return ("expand_group", "click")
            return ("", "")

        def _append_expandable_group(row: Dict[str, Any]) -> None:
            expand_action, invoke_action = _expandable_group_config(row)
            if not expand_action:
                return
            summary = cls._element_state_summary(row)
            candidate_name = str(summary.get("name", "") or "").strip()
            if not candidate_name:
                return
            candidate_key = str(summary.get("element_id", "") or "").strip() or f"{expand_action}:{cls._normalize_probe_text(candidate_name)}"
            if not candidate_key or candidate_key in expandable_seen:
                return
            expandable_seen.add(candidate_key)
            expandable_groups.append(
                {
                    **summary,
                    "expand_action": expand_action,
                    "invoke_action": invoke_action,
                }
            )

        for row in tree_item_rows:
            _append_navigation_candidate(row)
        for row in list_item_rows:
            _append_navigation_candidate(row)
        for row in menu_item_rows:
            _append_navigation_candidate(row)
        if sidebar_surface_visible:
            for row in button_rows:
                _append_navigation_candidate(row)
        for row in [*button_rows, *hyperlink_rows, *list_item_rows, *menu_item_rows, *tree_item_rows]:
            _append_drilldown_target(row)
        for row in [*group_rows, *tree_item_rows, *button_rows, *hyperlink_rows]:
            _append_expandable_group(row)
        selected_navigation_target = next(
            (
                str(row.get("name", "") or "").strip()
                for row in navigation_candidates
                if cls._coerce_surface_bool(row.get("selected")) is True or cls._coerce_surface_bool(row.get("checked")) is True
            ),
            "",
        )
        expanded_group_count = sum(
            1
            for row in expandable_groups
            if cls._coerce_surface_bool(row.get("expanded")) is True
        )
        scroll_search_supported = bool(
            (page_kind in {"settings_form", "property_sheet"} or flags.get("settings_window_ready", False))
            and (
                flags.get("scrollbar_visible", False)
                or any(marker in observation_text for marker in ("scroll", "below", "more settings"))
                or len(available_controls) >= 4
                or bool(navigation_candidates)
                or len(available_tabs) > 1
                or bool(expandable_groups)
            )
        )
        breadcrumb_path = [
            item
            for item in (selected_navigation_target, selected_tab)
            if str(item or "").strip()
        ]
        return {
            "page_kind": page_kind,
            "commit_action": commit_action,
            "ready_for_commit": ready_for_commit,
            "auto_resolve_supported": auto_resolve_supported,
            "autonomous_progress_supported": autonomous_progress_supported,
            "autonomous_blocker": autonomous_blocker,
            "manual_input_likely": manual_input_likely,
            "pending_requirements": pending_requirements[:8],
            "pending_requirement_count": len(pending_requirements),
            "available_controls": available_controls,
            "available_tabs": available_tabs,
            "selected_tab": selected_tab,
            "tab_count": len(available_tabs),
            "available_navigation_targets": navigation_candidates[:16],
            "selected_navigation_target": selected_navigation_target,
            "navigation_target_count": len(navigation_candidates),
            "available_drilldown_targets": drilldown_targets[:16],
            "drilldown_target_count": len(drilldown_targets),
            "available_expandable_groups": expandable_groups[:16],
            "expandable_group_count": len(expandable_groups),
            "expanded_group_count": expanded_group_count,
            "scroll_search_supported": scroll_search_supported,
            "breadcrumb_path": breadcrumb_path,
            "breadcrumb_depth": len(breadcrumb_path),
            "manual_required_controls": manual_required_controls,
            "preferred_commit_button": preferred_commit_button,
            "preferred_commit_target": preferred_commit_target,
            "preferred_dismiss_button": str(safety_payload.get("preferred_dismiss_button", "") or "").strip(),
            "safe_exit_options": [str(item).strip() for item in safe_exit_options if str(item).strip()][:6],
            "destructive_options": destructive_options[:6],
            "dialog_kind": dialog_kind,
            "approval_kind": approval_kind,
            "dialog_review_required": dialog_review_required,
            "dialog_auto_resolve_supported": dialog_auto_resolve_supported,
            "secure_desktop_likely": secure_desktop_likely,
            "credential_field_count": credential_field_count,
            "notes": [
                note
                for note in [
                    "Form page exposes acknowledgement-style prerequisites." if pending_requirements else "",
                    "Preferred commit button is available through accessibility." if preferred_commit_button else "",
                    "Form likely still needs manual text or option input before it can be committed." if manual_input_likely else "",
                    "Current dialog appears to require credentials or sign-in input before the form can continue." if dialog_kind == "credential_prompt" and credential_field_count > 0 else "",
                    "Current form surface appears to require administrator approval." if approval_kind == "elevation_consent" else "",
                    "Current form surface appears to require administrator credentials." if approval_kind == "elevation_credentials" else "",
                    "Current form surface appears to require permission or consent review." if approval_kind == "permission_review" else "",
                    "The current form prompt appears to be on a secure desktop surface." if secure_desktop_likely else "",
                    "Form exposes multiple tabs, so JARVIS can hunt across the property sheet for requested targets." if len(available_tabs) > 1 else "",
                    "Form exposes section navigation, so JARVIS can hunt across sidebar, tree, or list surfaces for requested targets." if navigation_candidates else "",
                    "Form exposes child-page links, so JARVIS can open deeper settings surfaces before committing requested changes." if drilldown_targets else "",
                    "Form exposes expandable groups, so JARVIS can drill into collapsed sections before committing requested changes." if expandable_groups else "",
                    "Form looks scrollable, so JARVIS can keep hunting below the visible viewport for requested targets." if scroll_search_supported else "",
                ]
                if note
            ],
        }

    def _workflow_surface_preflight(
        self,
        *,
        requested_action: str,
        args: Dict[str, Any],
        app_profile: Dict[str, Any],
        capabilities: Dict[str, Any],
        active_window: Dict[str, Any],
        primary_candidate: Dict[str, Any],
    ) -> Dict[str, Any]:
        clean_action = str(requested_action or "").strip().lower()
        definition = self._workflow_definition(clean_action)
        surface_flag = str(definition.get("surface_flag", "") or "").strip()
        static_prep_workflows = [
            str(action_name).strip().lower()
            for action_name in definition.get("prep_workflows", [])
            if str(action_name).strip()
        ]
        topology_candidate_actions = self._workflow_topology_prep_candidates(
            requested_action=clean_action,
            app_profile=app_profile,
        )
        should_probe = bool(surface_flag or static_prep_workflows or topology_candidate_actions)
        accessibility_ready = bool(capabilities.get("accessibility", {}).get("available")) if isinstance(capabilities.get("accessibility", {}), dict) else False
        vision_ready = bool(capabilities.get("vision", {}).get("available")) if isinstance(capabilities.get("vision", {}), dict) else False
        if not should_probe or not (accessibility_ready or vision_ready) or not (primary_candidate or active_window or args.get("app_name") or args.get("window_title")):
            return {
                "enabled": False,
                "snapshot": {},
                "skip_primary_hotkey": False,
                "prep_steps": [],
                "warnings": [],
                "candidate_prep_actions": list(topology_candidate_actions),
                "prep_actions": [],
                "target_query_already_active": False,
            }

        workflow_query = self._workflow_input_text(requested_action=clean_action, args=args) or str(args.get("query", "") or "").strip()
        surface_query = str(args.get("query", "") or workflow_query or "").strip() if clean_action == "send_message" else workflow_query
        snapshot = self.surface_snapshot(
            app_name=str(args.get("app_name", "") or ""),
            window_title=str(args.get("window_title", "") or ""),
            query=surface_query,
            limit=12,
            include_observation=True,
            include_elements=accessibility_ready,
            include_workflow_probes=True,
            preferred_actions=[clean_action, *static_prep_workflows, *topology_candidate_actions],
        )
        flags = snapshot.get("surface_flags", {}) if isinstance(snapshot.get("surface_flags", {}), dict) else {}
        topology_prep_actions = self._workflow_topology_prep_actions(
            requested_action=clean_action,
            args=args,
            app_profile=app_profile,
            snapshot=snapshot,
        )
        prep_workflows = self._dedupe_strings([*topology_prep_actions, *static_prep_workflows])
        surface_ready = bool(flags.get(surface_flag)) if surface_flag else False
        workflow_text = self._workflow_input_text(requested_action=clean_action, args=args)
        preserve_ready_surface = bool(definition.get("preserve_ready_surface", False))
        skip_primary_hotkey = bool(
            surface_ready
            and bool(definition.get("skip_hotkey_when_ready", False))
            and (bool(workflow_text) or preserve_ready_surface)
        )
        target_query_already_active = bool(
            clean_action == "send_message"
            and str(args.get("query", "") or "").strip()
            and flags.get("conversation_target_active")
            and flags.get("message_compose_ready")
        )
        warnings: List[str] = []
        prep_steps: List[Dict[str, Any]] = []
        prep_actions_applied: List[str] = []
        focus_title = str(primary_candidate.get("title", "") or args.get("window_title", "") or args.get("app_name", "")).strip()

        if skip_primary_hotkey:
            if workflow_text:
                warnings.append(
                    f"Surface preflight detected the '{surface_flag}' state, so JARVIS will type directly without reopening the workflow surface."
                )
            else:
                warnings.append(
                    f"Surface preflight detected the '{surface_flag}' state, so JARVIS will preserve the ready surface instead of replaying its toggle shortcut."
                )
        if target_query_already_active:
            skip_primary_hotkey = True
            warnings.append(
                "Surface preflight detected that the requested conversation is already active, so JARVIS will send the message directly."
            )

        if not surface_ready:
            for prep_action in prep_workflows:
                prep_step = self._workflow_preflight_step(
                    requested_action=prep_action,
                    args=args,
                    app_profile=app_profile,
                    focus_title=focus_title,
                )
                if not prep_step:
                    continue
                prep_steps.append(prep_step)
                prep_actions_applied.append(prep_action)
            if prep_steps and bool(definition.get("replace_primary_hotkey_with_prep", False)):
                skip_primary_hotkey = True
                warnings.append(
                    f"Surface preflight did not detect '{surface_flag}', so JARVIS will bootstrap the surface with {', '.join(prep_actions_applied)} before typing."
                )
            elif prep_steps:
                warnings.append(
                    f"Surface preflight will stage {', '.join(prep_actions_applied)} before continuing the '{clean_action}' workflow."
                )

        return {
            "enabled": True,
            "snapshot": snapshot if isinstance(snapshot, dict) else {},
            "surface_flag": surface_flag,
            "surface_ready": surface_ready,
            "skip_primary_hotkey": skip_primary_hotkey,
            "prep_steps": prep_steps,
            "warnings": warnings,
            "candidate_prep_actions": list(topology_candidate_actions),
            "prep_actions": list(prep_actions_applied),
            "target_query_already_active": target_query_already_active,
        }

    def _workflow_topology_prep_candidates(
        self,
        *,
        requested_action: str,
        app_profile: Dict[str, Any],
    ) -> List[str]:
        clean_action = str(requested_action or "").strip().lower()
        category = str(app_profile.get("category", "") or "").strip().lower()
        actions: List[str] = []
        if clean_action in {
            "focus_input_field",
            "set_field_value",
            "open_dropdown",
            "select_dropdown_option",
            "focus_checkbox",
            "check_checkbox",
            "uncheck_checkbox",
            "toggle_switch",
            "select_radio_option",
            "focus_value_control",
            "increase_value",
            "decrease_value",
            "set_value_control",
            "complete_form_page",
            "complete_form_flow",
        }:
            actions.append("focus_form_surface")
        if category == "file_manager" and clean_action in {
            "new_folder",
            "rename_selection",
            "open_properties_dialog",
            "open_preview_pane",
            "open_details_pane",
        }:
            actions.append("focus_file_list")
        if category == "office" and clean_action in {"reply_email", "reply_all_email", "forward_email"}:
            actions.extend(["open_mail_view", "focus_message_list"])
        if category == "office" and clean_action == "new_email_draft":
            actions.append("open_mail_view")
        if category == "office" and clean_action == "new_calendar_event":
            actions.append("open_calendar_view")
        if clean_action in {"select_tree_item", "expand_tree_item"}:
            if category == "file_manager":
                actions.append("focus_folder_tree")
            else:
                actions.append("focus_navigation_tree")
        if clean_action == "select_list_item":
            if category == "file_manager":
                actions.append("focus_file_list")
            elif category == "office":
                actions.append("focus_message_list")
            else:
                actions.append("focus_list_surface")
        if clean_action == "select_table_row":
            actions.append("focus_data_table")
        if clean_action == "open_context_menu":
            if category == "file_manager":
                actions.append("focus_file_list")
            elif category == "office":
                actions.append("focus_message_list")
            elif category in {"browser", "code_editor", "ide", "terminal", "chat", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
                actions.append("focus_main_content")
        if clean_action == "select_context_menu_item":
            if category == "file_manager":
                actions.append("focus_file_list")
            elif category == "office":
                actions.append("focus_message_list")
            elif category in {"browser", "code_editor", "ide", "terminal", "chat", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
                actions.append("focus_main_content")
        return self._dedupe_strings(actions)

    def _workflow_topology_prep_actions(
        self,
        *,
        requested_action: str,
        args: Dict[str, Any],
        app_profile: Dict[str, Any],
        snapshot: Dict[str, Any],
    ) -> List[str]:
        del args
        clean_action = str(requested_action or "").strip().lower()
        category = str(app_profile.get("category", "") or "").strip().lower()
        candidate_actions = self._workflow_topology_prep_candidates(
            requested_action=clean_action,
            app_profile=app_profile,
        )
        if not candidate_actions:
            return []
        flags = snapshot.get("surface_flags", {}) if isinstance(snapshot.get("surface_flags", {}), dict) else {}
        actions: List[str] = []
        if clean_action in {
            "focus_input_field",
            "set_field_value",
            "open_dropdown",
            "select_dropdown_option",
            "focus_checkbox",
            "check_checkbox",
            "uncheck_checkbox",
            "toggle_switch",
            "select_radio_option",
            "focus_value_control",
            "increase_value",
            "decrease_value",
            "set_value_control",
        } and not bool(flags.get("form_visible")):
            actions.append("focus_form_surface")
        if clean_action in {
            "new_folder",
            "rename_selection",
            "open_properties_dialog",
            "open_preview_pane",
            "open_details_pane",
        } and not bool(flags.get("file_list_visible")):
            actions.append("focus_file_list")
        if clean_action in {"reply_email", "reply_all_email", "forward_email"} and not bool(flags.get("email_compose_ready")):
            if not bool(flags.get("mail_view_active")):
                actions.append("open_mail_view")
            if not bool(flags.get("message_list_visible")):
                actions.append("focus_message_list")
        if clean_action == "new_email_draft" and not bool(flags.get("email_compose_ready")) and not bool(flags.get("mail_view_active")):
            actions.append("open_mail_view")
        if clean_action == "new_calendar_event" and not bool(flags.get("calendar_event_compose_ready")) and not bool(flags.get("calendar_view_active")):
            actions.append("open_calendar_view")
        if clean_action in {"select_tree_item", "expand_tree_item"}:
            if category == "file_manager" and not bool(flags.get("folder_tree_visible")):
                actions.append("focus_folder_tree")
            elif category != "file_manager" and not bool(flags.get("tree_visible")):
                actions.append("focus_navigation_tree")
        if clean_action == "select_list_item":
            if category == "file_manager" and not bool(flags.get("file_list_visible")):
                actions.append("focus_file_list")
            elif category == "office" and bool(flags.get("mail_view_active")) and not bool(flags.get("message_list_visible")):
                actions.append("focus_message_list")
            elif category not in {"file_manager", "office"} and not bool(flags.get("list_visible")):
                actions.append("focus_list_surface")
        if clean_action == "select_table_row" and not bool(flags.get("table_visible")):
            actions.append("focus_data_table")
        if clean_action == "open_context_menu":
            if category == "file_manager" and not bool(flags.get("file_list_visible")):
                actions.append("focus_file_list")
            elif category == "office" and bool(flags.get("mail_view_active")) and not bool(flags.get("message_list_visible")):
                actions.append("focus_message_list")
            elif category in {"browser", "code_editor", "ide", "terminal", "chat", "utility", "ops_console", "security", "ai_companion", "general_desktop"} and not bool(flags.get("main_content_visible")):
                actions.append("focus_main_content")
        if clean_action == "select_context_menu_item":
            if category == "file_manager" and not bool(flags.get("file_list_visible")):
                actions.append("focus_file_list")
            elif category == "office" and bool(flags.get("mail_view_active")) and not bool(flags.get("message_list_visible")):
                actions.append("focus_message_list")
            elif category in {"browser", "code_editor", "ide", "terminal", "chat", "utility", "ops_console", "security", "ai_companion", "general_desktop"} and not bool(flags.get("main_content_visible")):
                actions.append("focus_main_content")
        return [action for action in self._dedupe_strings(actions) if action in candidate_actions]

    def _workflow_preflight_step(
        self,
        *,
        requested_action: str,
        args: Dict[str, Any],
        app_profile: Dict[str, Any],
        focus_title: str,
    ) -> Dict[str, Any]:
        prep_profile = self._workflow_profile(requested_action=requested_action, args=args, app_profile=app_profile)
        if not bool(prep_profile.get("supported", False)):
            return {}
        workflow_action_name = str(prep_profile.get("workflow_action", "") or "").strip().lower()
        workflow_action_args = self._resolve_workflow_action_args(prep_profile.get("workflow_action_args", {}), args)
        if not isinstance(workflow_action_args, dict):
            workflow_action_args = {}
        if workflow_action_name == "accessibility_invoke_element" and focus_title and not str(workflow_action_args.get("window_title", "") or "").strip():
            workflow_action_args["window_title"] = focus_title
        primary_hotkey = prep_profile.get("primary_hotkey", []) if isinstance(prep_profile.get("primary_hotkey", []), list) else []
        use_workflow_action = bool(
            workflow_action_name
            and (
                prep_profile.get("supports_system_action", False)
                or prep_profile.get("supports_action_dispatch", False)
            )
            and (prep_profile.get("prefer_workflow_action", False) or not primary_hotkey)
        )
        if use_workflow_action:
            return self._plan_step(
                action=workflow_action_name,
                args=workflow_action_args,
                phase="preflight",
                optional=False,
                reason=str(
                    prep_profile.get("workflow_action_reason", "")
                    or prep_profile.get("hotkey_reason", "")
                    or "Prepare the desktop surface through a native control dispatch before continuing the workflow."
                ),
            )
        if primary_hotkey:
            return self._plan_step(
                action="keyboard_hotkey",
                args={"keys": list(primary_hotkey)},
                phase="preflight",
                optional=False,
                reason=str(
                    prep_profile.get("hotkey_reason", "")
                    or "Prepare the desktop surface with a workflow shortcut before continuing the requested action."
                ),
            )
        return {}

    def _workflow_stateful_overrides(
        self,
        *,
        requested_action: str,
        args: Dict[str, Any],
        snapshot: Dict[str, Any],
    ) -> Dict[str, Any]:
        clean_action = str(requested_action or "").strip().lower()
        snapshot_payload = snapshot if isinstance(snapshot, dict) else {}
        target_state = snapshot_payload.get("target_control_state", {})
        target = target_state if isinstance(target_state, dict) else {}
        element_payload = snapshot_payload.get("elements", {}) if isinstance(snapshot_payload.get("elements", {}), dict) else {}
        element_items = element_payload.get("items", []) if isinstance(element_payload.get("items", []), list) else []
        target_window = snapshot_payload.get("target_window", {}) if isinstance(snapshot_payload.get("target_window", {}), dict) else {}
        active_window = snapshot_payload.get("active_window", {}) if isinstance(snapshot_payload.get("active_window", {}), dict) else {}
        target_group_state = snapshot_payload.get("target_group_state", {}) if isinstance(snapshot_payload.get("target_group_state", {}), dict) else {}
        wizard_page_state = snapshot_payload.get("wizard_page_state", {}) if isinstance(snapshot_payload.get("wizard_page_state", {}), dict) else {}
        safety_signals = snapshot_payload.get("safety_signals", {}) if isinstance(snapshot_payload.get("safety_signals", {}), dict) else {}
        warnings: List[str] = []
        arg_updates: Dict[str, Any] = {}
        target_state_ready = False
        form_target_state: Dict[str, Any] = {}
        if clean_action in {"complete_form_page", "complete_form_flow"}:
            form_target_state = self._form_target_plan_state(
                plan=args.get("form_target_plan", []),
                snapshot=snapshot_payload,
            )

        selected = self._coerce_surface_bool(target.get("selected"))
        checked = self._coerce_surface_bool(target.get("checked"))
        current_value_text = str(target.get("value_text", "") or "").strip()
        current_value_numeric = self._normalize_surface_number(target.get("range_value"))
        if current_value_numeric is None:
            current_value_numeric = self._normalize_surface_number(current_value_text)
        minimum = self._normalize_surface_number(target.get("range_min"))
        maximum = self._normalize_surface_number(target.get("range_max"))
        control_type = self._normalize_probe_text(target.get("control_type", ""))
        desired_text = str(args.get("text", "") or "").strip()
        desired_numeric = self._normalize_surface_number(desired_text)
        desired_query = str(args.get("query", "") or "").strip()
        query_related_candidates = snapshot_payload.get("query_related_candidates", []) if isinstance(snapshot_payload.get("query_related_candidates", []), list) else []
        selection_candidates = snapshot_payload.get("selection_candidates", []) if isinstance(snapshot_payload.get("selection_candidates", []), list) else []

        def _snapshot_window_title() -> str:
            return str(target_window.get("title", "") or active_window.get("title", "") or args.get("window_title", "") or args.get("app_name", "")).strip()

        def _candidate_action_args(*, candidate: Dict[str, Any], action_name: str) -> Dict[str, Any]:
            step_args: Dict[str, Any] = {
                "query": str(candidate.get("name", "") or desired_text or desired_query).strip(),
                "action": action_name,
            }
            element_id = str(candidate.get("element_id", "") or "").strip()
            if element_id:
                step_args["element_id"] = element_id
            control_name = str(candidate.get("control_type", "") or "").strip()
            if control_name:
                step_args["control_type"] = control_name
            window_title = str(candidate.get("window_title", "") or "").strip() or _snapshot_window_title()
            if window_title:
                step_args["window_title"] = window_title
            return step_args

        def _strong_candidate(*, query_text: str, control_types: Optional[set[str]] = None, minimum_score: float = 0.76) -> Dict[str, Any]:
            if not query_text:
                return {}
            best_candidate: Dict[str, Any] = {}
            best_score = 0.0
            for candidate_source, source_bonus in (
                (query_related_candidates, 0.05),
                (selection_candidates, 0.03),
                (element_items, 0.0),
            ):
                candidate_rows = self._query_target_elements(
                    elements=candidate_source,
                    query=query_text,
                    control_types=control_types,
                    limit=6,
                )
                for row in candidate_rows:
                    try:
                        score = float(row.get("match_score", 0.0) or 0.0) + source_bonus
                    except Exception:
                        score = source_bonus
                    if score <= best_score:
                        continue
                    best_score = score
                    best_candidate = row
            return best_candidate if best_score >= minimum_score else {}

        def _alias_candidate(*, aliases: List[str], control_types: Optional[set[str]] = None) -> Dict[str, Any]:
            best_candidate: Dict[str, Any] = {}
            best_score = 0.0
            for alias in aliases:
                candidate = _strong_candidate(query_text=alias, control_types=control_types, minimum_score=0.72)
                if not candidate:
                    continue
                try:
                    score = float(candidate.get("match_score", 0.0) or 0.0)
                except Exception:
                    score = 0.0
                if score <= best_score:
                    continue
                best_score = score
                best_candidate = candidate
            return best_candidate

        def _followup_accessibility_step(*, candidate: Dict[str, Any], reason: str) -> Dict[str, Any]:
            return self._plan_step(
                action="accessibility_invoke_element",
                args=_candidate_action_args(candidate=candidate, action_name="click"),
                phase="input",
                optional=False,
                reason=reason,
            )

        def _followup_hotkey_step(*, keys: List[str], reason: str) -> Dict[str, Any]:
            return self._plan_step(
                action="keyboard_hotkey",
                args={"keys": [str(item).strip().lower() for item in keys if str(item).strip()]},
                phase="input",
                optional=False,
                reason=reason,
            )

        def _skip_ready_state(message: str) -> Dict[str, Any]:
            arg_updates["_skip_workflow_action"] = True
            arg_updates["_skip_primary_hotkey"] = True
            arg_updates["_skip_input_steps"] = True
            arg_updates["_target_state_ready"] = True
            warnings.append(message)
            return {
                "arg_updates": arg_updates,
                "warnings": warnings,
                "target_state_ready": True,
                "form_target_state": form_target_state,
            }

        dispatch_override_specs: Dict[str, Dict[str, Any]] = {
            "focus_input_field": {"control_types": {"edit", "document", "combobox"}, "action": "focus"},
            "focus_checkbox": {"control_types": {"checkbox"}, "action": "focus"},
            "focus_value_control": {"control_types": {"slider", "spinner", "edit", "combobox"}, "action": "focus"},
            "focus_sidebar": {"control_types": {"pane", "tree", "list"}, "action": "focus"},
            "focus_toolbar": {"control_types": {"toolbar", "pane"}, "action": "focus"},
            "focus_navigation_tree": {"control_types": {"tree"}, "action": "focus"},
            "focus_list_surface": {"control_types": {"list"}, "action": "focus"},
            "focus_data_table": {"control_types": {"table", "datagrid"}, "action": "focus"},
            "focus_folder_tree": {"control_types": {"tree"}, "action": "focus"},
            "focus_file_list": {"control_types": {"list", "table"}, "action": "focus"},
            "focus_main_content": {"control_types": {"pane", "document", "list", "table"}, "action": "focus"},
            "focus_folder_pane": {"control_types": {"pane", "tree"}, "action": "focus"},
            "focus_message_list": {"control_types": {"list", "table"}, "action": "focus"},
            "focus_reading_pane": {"control_types": {"pane", "document"}, "action": "focus"},
            "select_sidebar_item": {"control_types": {"treeitem", "listitem", "button", "menuitem", "hyperlink"}, "action": "click"},
            "invoke_toolbar_action": {"control_types": {"button", "splitbutton", "menuitem", "togglebutton"}, "action": "click"},
            "select_radio_option": {"control_types": {"radiobutton"}, "action": "click", "skip_if_selected": True},
            "select_tab_page": {"control_types": {"tabitem"}, "action": "click", "skip_if_selected": True},
            "select_tree_item": {"control_types": {"treeitem"}, "action": "click", "skip_if_selected": True},
            "expand_tree_item": {"control_types": {"treeitem"}, "action": "double_click", "skip_if_expanded": True},
            "select_list_item": {"control_types": {"listitem"}, "action": "click", "skip_if_selected": True},
            "select_table_row": {"control_types": {"dataitem", "listitem", "row"}, "action": "click", "skip_if_selected": True},
            "select_context_menu_item": {"control_types": {"menuitem"}, "action": "click"},
            "press_dialog_button": {"control_types": {"button", "splitbutton"}, "action": "click"},
            "toggle_switch": {"control_types": {"checkbox", "button", "togglebutton"}, "action": "click"},
            "enable_switch": {"control_types": {"checkbox", "button", "togglebutton"}, "action": "click"},
            "disable_switch": {"control_types": {"checkbox", "button", "togglebutton"}, "action": "click"},
        }

        if clean_action in {"select_radio_option", "select_tab_page"} and (selected is True or checked is True):
            return _skip_ready_state(
                "Surface state shows the requested target is already active, so JARVIS will preserve it instead of replaying the selection action."
            )

        if clean_action == "check_checkbox" and checked is True:
            return _skip_ready_state(
                "Surface state shows the requested checkbox is already checked, so JARVIS will preserve the current control state."
            )

        if clean_action == "uncheck_checkbox" and checked is False:
            return _skip_ready_state(
                "Surface state shows the requested checkbox is already unchecked, so JARVIS will preserve the current control state."
            )

        dispatch_override = dispatch_override_specs.get(clean_action, {})
        if dispatch_override and desired_query:
            refined_candidate = _strong_candidate(
                query_text=desired_query,
                control_types=dispatch_override.get("control_types"),
                minimum_score=0.78,
            )
            if refined_candidate:
                candidate_name = str(refined_candidate.get("name", "") or "").strip()
                candidate_selected = self._coerce_surface_bool(refined_candidate.get("selected"))
                candidate_checked = self._coerce_surface_bool(refined_candidate.get("checked"))
                candidate_expanded = self._coerce_surface_bool(refined_candidate.get("expanded"))
                if bool(dispatch_override.get("skip_if_selected")) and (candidate_selected is True or candidate_checked is True):
                    return _skip_ready_state(
                        f"Surface state shows the requested '{candidate_name or desired_query}' target is already selected, so JARVIS will preserve it instead of replaying the action."
                    )
                if bool(dispatch_override.get("skip_if_expanded")) and candidate_expanded is True:
                    return _skip_ready_state(
                        f"Surface state shows the requested '{candidate_name or desired_query}' tree item is already expanded, so JARVIS will preserve the current hierarchy state."
                    )
                arg_updates["_workflow_action_args_override"] = _candidate_action_args(
                    candidate=refined_candidate,
                    action_name=str(dispatch_override.get("action", "click") or "click"),
                )
                if candidate_name and candidate_name != desired_query:
                    arg_updates["query"] = candidate_name
                warnings.append(
                    f"Surface state exposed an exact live target for '{candidate_name or desired_query}', so JARVIS will dispatch directly against that control instance."
                )
            elif clean_action in {"select_radio_option", "select_tab_page", "press_dialog_button", "select_context_menu_item"}:
                refined_name = str(refined_candidate.get("name", "") or "").strip()
                if refined_name and refined_name != desired_query:
                    arg_updates["query"] = refined_name
                    warnings.append(
                        f"Surface state exposed a stronger live target match ('{refined_name}'), so JARVIS will dispatch against that control label."
                    )

        if clean_action in {"check_checkbox", "uncheck_checkbox", "toggle_switch", "enable_switch", "disable_switch"} and desired_query:
            toggle_type_hints = {
                "check_checkbox": {"checkbox"},
                "uncheck_checkbox": {"checkbox"},
                "toggle_switch": {"checkbox", "button", "togglebutton"},
                "enable_switch": {"checkbox", "button", "togglebutton"},
                "disable_switch": {"checkbox", "button", "togglebutton"},
            }
            toggle_candidate = _strong_candidate(
                query_text=desired_query,
                control_types=toggle_type_hints.get(clean_action),
                minimum_score=0.78,
            )
            if toggle_candidate:
                candidate_name = str(toggle_candidate.get("name", "") or desired_query).strip()
                candidate_checked = self._coerce_surface_bool(toggle_candidate.get("checked"))
                candidate_toggle = self._normalize_probe_text(toggle_candidate.get("toggle_state", ""))
                if clean_action == "check_checkbox" and (candidate_checked is True or candidate_toggle in {"on", "checked"}):
                    return _skip_ready_state(
                        f"Surface state shows the requested checkbox ('{candidate_name}') is already checked, so JARVIS will preserve it."
                    )
                if clean_action == "uncheck_checkbox" and (candidate_checked is False or candidate_toggle in {"off", "unchecked"}):
                    return _skip_ready_state(
                        f"Surface state shows the requested checkbox ('{candidate_name}') is already unchecked, so JARVIS will preserve it."
                    )
                if clean_action == "enable_switch" and (candidate_checked is True or candidate_toggle in {"on", "checked"}):
                    return _skip_ready_state(
                        f"Surface state shows the requested switch ('{candidate_name}') is already on, so JARVIS will preserve it."
                    )
                if clean_action == "disable_switch" and (candidate_checked is False or candidate_toggle in {"off", "unchecked"}):
                    return _skip_ready_state(
                        f"Surface state shows the requested switch ('{candidate_name}') is already off, so JARVIS will preserve it."
                    )
                arg_updates["_skip_primary_hotkey"] = True
                if clean_action in {"toggle_switch", "enable_switch", "disable_switch"}:
                    arg_updates["_workflow_action_args_override"] = _candidate_action_args(
                        candidate=toggle_candidate,
                        action_name="click",
                    )
                else:
                    arg_updates["_workflow_followup_steps"] = [
                        _followup_accessibility_step(
                            candidate=toggle_candidate,
                            reason=(
                                f"Use the live '{candidate_name}' control exposed by the form surface instead of a generic keyboard toggle."
                                if clean_action == "toggle_switch"
                                else f"Use the live '{candidate_name}' control exposed by the form surface instead of a blind switch state change."
                            ),
                        )
                    ]
                group_role = str(target_group_state.get("group_role", "") or "").strip()
                if group_role:
                    warnings.append(
                        f"Surface state resolved '{candidate_name}' inside the live {group_role.replace('_', ' ')} group, so JARVIS will toggle that exact control instance."
                    )
                else:
                    warnings.append(
                        f"Surface state exposed the exact '{candidate_name}' control, so JARVIS will toggle it directly through accessibility."
                    )
                return {
                    "arg_updates": arg_updates,
                    "warnings": warnings,
                    "target_state_ready": target_state_ready,
                }

        if clean_action in {"next_wizard_step", "finish_wizard"}:
            pending_requirements = wizard_page_state.get("pending_requirements", []) if isinstance(wizard_page_state.get("pending_requirements", []), list) else []
            if pending_requirements:
                warnings.append(
                    f"Wizard page intelligence detected {len(pending_requirements)} prerequisite control(s) that may need resolution before this page can advance cleanly."
                )

        def _preferred_dialog_target(signal_key: str, aliases: List[str]) -> Dict[str, Any]:
            signal_target = safety_signals.get(signal_key, {})
            if isinstance(signal_target, dict) and str(signal_target.get("name", "") or "").strip():
                return signal_target
            return _alias_candidate(aliases=aliases, control_types={"button", "splitbutton"})

        if clean_action in {"confirm_dialog", "dismiss_dialog", "next_wizard_step", "previous_wizard_step", "finish_wizard"}:
            semantic_aliases = {
                "confirm_dialog": ["ok", "yes", "continue", "allow", "accept", "apply", "install", "finish", "next"],
                "dismiss_dialog": ["cancel", "no", "close", "back", "later", "skip"],
                "next_wizard_step": ["next", "continue", "proceed"],
                "previous_wizard_step": ["back", "previous", "prev"],
                "finish_wizard": ["finish", "done", "complete", "install", "apply", "launch"],
            }
            signal_target_fields = {
                "confirm_dialog": "preferred_confirmation_target",
                "dismiss_dialog": "preferred_dismiss_target",
                "next_wizard_step": "preferred_confirmation_target",
                "previous_wizard_step": "preferred_dismiss_target",
                "finish_wizard": "preferred_confirmation_target",
            }
            matched_button = _preferred_dialog_target(
                signal_key=signal_target_fields.get(clean_action, ""),
                aliases=semantic_aliases.get(clean_action, []),
            )
            if matched_button:
                button_name = str(matched_button.get("name", "") or "").strip()
                arg_updates["_skip_workflow_action"] = True
                arg_updates["_skip_primary_hotkey"] = True
                arg_updates["_workflow_followup_steps"] = [
                    _followup_accessibility_step(
                        candidate=matched_button,
                        reason=f"Use the live '{button_name or clean_action}' button exposed by the dialog surface instead of a blind accelerator.",
                    )
                ]
                warnings.append(
                    f"Surface state exposed the live '{button_name or clean_action}' button, so JARVIS will invoke it directly through accessibility."
                )
                safe_buttons = [str(item).strip() for item in safety_signals.get("safe_dialog_buttons", []) if str(item).strip()]
                destructive_buttons = [str(item).strip() for item in safety_signals.get("destructive_dialog_buttons", []) if str(item).strip()]
                if clean_action in {"confirm_dialog", "next_wizard_step", "finish_wizard"} and destructive_buttons:
                    if safe_buttons:
                        warnings.append(
                            f"Surface safety also exposed safer alternatives ({', '.join(safe_buttons[:3])}), while the selected confirmation path will press '{button_name or clean_action}'."
                        )
                    else:
                        warnings.append(
                            f"Surface safety marked '{button_name or clean_action}' as a commit-style action on a risky surface, so JARVIS is using the exact visible control instead of a generic confirmation hotkey."
                        )
                return {
                    "arg_updates": arg_updates,
                    "warnings": warnings,
                    "target_state_ready": target_state_ready,
                }

        if clean_action == "complete_wizard_page":
            pending_requirements = wizard_page_state.get("pending_requirements", []) if isinstance(wizard_page_state.get("pending_requirements", []), list) else []
            followup_steps: List[Dict[str, Any]] = []
            for row in pending_requirements[:4]:
                if not isinstance(row, dict):
                    continue
                control_name = str(row.get("name", "") or "").strip() or str(row.get("target_label", "") or "").strip()
                requirement_type = str(row.get("requirement_type", "") or "").strip().lower()
                if not control_name:
                    continue
                followup_steps.append(
                    _followup_accessibility_step(
                        candidate=row,
                        reason=(
                            f"Resolve the wizard page prerequisite '{control_name}' before advancing the setup."
                            if requirement_type
                            else f"Resolve the wizard page prerequisite '{control_name}' before advancing the setup."
                        ),
                    )
                )
            preferred_confirmation_target = wizard_page_state.get("preferred_confirmation_target", {})
            preferred_confirmation_button = str(wizard_page_state.get("preferred_confirmation_button", "") or "").strip()
            advance_action = str(wizard_page_state.get("advance_action", "") or "").strip().lower()
            if isinstance(preferred_confirmation_target, dict) and str(preferred_confirmation_target.get("name", "") or "").strip():
                followup_steps.append(
                    _followup_accessibility_step(
                        candidate=preferred_confirmation_target,
                        reason=f"Advance the wizard through the preferred '{preferred_confirmation_button or advance_action or 'next'}' control exposed on the current setup page.",
                    )
                )
            elif advance_action == "finish_wizard":
                followup_steps.append(
                    _followup_hotkey_step(
                        keys=["alt", "f"],
                        reason="Advance the setup through the generic finish accelerator because the final page did not expose a clickable confirmation target.",
                    )
                )
            else:
                followup_steps.append(
                    _followup_hotkey_step(
                        keys=["alt", "n"],
                        reason="Advance the setup through the generic next-step accelerator because the current page did not expose a clickable confirmation target.",
                    )
                )
            arg_updates["_skip_workflow_action"] = True
            arg_updates["_skip_primary_hotkey"] = True
            arg_updates["_workflow_followup_steps"] = followup_steps
            page_kind = str(wizard_page_state.get("page_kind", "") or "").replace("_", " ").strip()
            if page_kind:
                warnings.append(
                    f"Surface state classified the current setup surface as a {page_kind} page, so JARVIS will resolve the page prerequisites before advancing."
                )
            if pending_requirements:
                warnings.append(
                    f"Wizard page intelligence found {len(pending_requirements)} pending prerequisite control(s), so JARVIS will stage them before continuing."
                )
            if bool(safety_signals.get("destructive_warning_visible", False)):
                safe_buttons = [str(item).strip() for item in safety_signals.get("safe_dialog_buttons", []) if str(item).strip()]
                if safe_buttons:
                    warnings.append(
                        f"The current setup page exposes safer alternatives ({', '.join(safe_buttons[:3])}), while the completion workflow will continue through '{preferred_confirmation_button or advance_action or 'next'}'."
                    )
            return {
                "arg_updates": arg_updates,
                "warnings": warnings,
                "target_state_ready": target_state_ready,
            }

        if clean_action == "complete_form_page":
            form_page_state = snapshot_payload.get("form_page_state", {}) if isinstance(snapshot_payload.get("form_page_state", {}), dict) else {}
            pending_requirements = form_page_state.get("pending_requirements", []) if isinstance(form_page_state.get("pending_requirements", []), list) else []
            followup_steps: List[Dict[str, Any]] = []
            target_followup_steps: List[Dict[str, Any]] = []
            planned_target_count = 0
            planned_targets: List[Dict[str, Any]] = []
            for target_row in form_target_state.get("targets", []) if isinstance(form_target_state.get("targets", []), list) else []:
                if not isinstance(target_row, dict) or bool(target_row.get("satisfied", False)) or not bool(target_row.get("visible", False)):
                    continue
                target_action = str(target_row.get("action", "") or "").strip().lower()
                target_query = str(target_row.get("query", "") or "").strip()
                target_text = str(target_row.get("text", "") or "").strip()
                target_descriptor = {
                    "action": target_action,
                    "query": target_query,
                    "text": target_text,
                    "family": str(target_row.get("family", "") or "").strip(),
                }
                control_candidate = target_row.get("control_candidate", {}) if isinstance(target_row.get("control_candidate", {}), dict) else {}
                option_candidate = target_row.get("option_candidate", {}) if isinstance(target_row.get("option_candidate", {}), dict) else {}
                candidate = control_candidate or option_candidate
                if target_action in {"check_checkbox", "uncheck_checkbox", "enable_switch", "disable_switch", "select_radio_option", "select_tab_page"} and candidate:
                    planned_target_count += 1
                    planned_targets.append(target_descriptor)
                    target_followup_steps.append(
                        _followup_accessibility_step(
                            candidate=candidate,
                            reason=f"Apply the requested {target_query} target state before committing the current settings page.",
                        )
                    )
                    continue
                if target_action in {"set_field_value", "set_value_control"} and control_candidate and target_text:
                    planned_target_count += 1
                    planned_targets.append(target_descriptor)
                    target_followup_steps.extend(
                        [
                            _followup_accessibility_step(
                                candidate=control_candidate,
                                reason=f"Focus the '{target_query}' control before applying the requested target value.",
                            ),
                            self._plan_step(
                                action="keyboard_hotkey",
                                args={"keys": ["ctrl", "a"]},
                                phase="input",
                                optional=False,
                                reason=f"Replace the existing value in '{target_query}' with the requested target state.",
                            ),
                            self._plan_step(
                                action="keyboard_type",
                                args={"text": target_text, "press_enter": False},
                                phase="input",
                                optional=False,
                                reason=f"Type the requested value for '{target_query}' before committing the current settings page.",
                            ),
                        ]
                    )
                    continue
                if target_action == "select_dropdown_option" and target_text:
                    if option_candidate:
                        planned_target_count += 1
                        planned_targets.append(target_descriptor)
                        target_followup_steps.append(
                            _followup_accessibility_step(
                                candidate=option_candidate,
                                reason=f"Select the visible '{target_text}' option for '{target_query}' before committing the current settings page.",
                            )
                        )
                        continue
                    if control_candidate:
                        planned_target_count += 1
                        planned_targets.append(target_descriptor)
                        target_followup_steps.extend(
                            [
                                _followup_accessibility_step(
                                    candidate=control_candidate,
                                    reason=f"Open the '{target_query}' control before choosing the requested option.",
                                ),
                                self._plan_step(
                                    action="keyboard_type",
                                    args={"text": target_text, "press_enter": True},
                                    phase="input",
                                    optional=False,
                                    reason=f"Type the requested '{target_text}' option for '{target_query}' before committing the current settings page.",
                                ),
                            ]
                        )
            if target_followup_steps:
                followup_steps.extend(target_followup_steps)
                warnings.append(
                    f"Form target planning found {planned_target_count} visible requested setting target(s), so JARVIS will reconcile them before committing this page."
                )
            visible_pending_target_count = int(form_target_state.get("visible_pending_count", 0) or 0) if isinstance(form_target_state, dict) else 0
            if visible_pending_target_count and planned_target_count < visible_pending_target_count:
                warnings.append(
                    f"Form target planning could not auto-stage every visible requested target on this page ({planned_target_count}/{visible_pending_target_count}), so JARVIS will continue with best-effort commit sequencing."
                )
            if form_target_state:
                form_target_state = {
                    **form_target_state,
                    "planned_target_count": planned_target_count,
                    "planned_targets": planned_targets[:12],
                }
            for row in pending_requirements[:4]:
                if not isinstance(row, dict):
                    continue
                control_name = str(row.get("name", "") or "").strip() or str(row.get("target_label", "") or "").strip()
                if not control_name:
                    continue
                followup_steps.append(
                    _followup_accessibility_step(
                        candidate=row,
                        reason=f"Resolve the form prerequisite '{control_name}' before committing the current settings page.",
                    )
                )
            preferred_commit_target = form_page_state.get("preferred_commit_target", {})
            preferred_commit_button = str(form_page_state.get("preferred_commit_button", "") or "").strip()
            if isinstance(preferred_commit_target, dict) and str(preferred_commit_target.get("name", "") or "").strip():
                followup_steps.append(
                    _followup_accessibility_step(
                        candidate=preferred_commit_target,
                        reason=f"Commit the current form through the preferred '{preferred_commit_button or 'save'}' control exposed on the surface.",
                    )
                )
            else:
                followup_steps.append(
                    _followup_hotkey_step(
                        keys=["enter"],
                        reason="Commit the current form through the generic confirmation accelerator because the surface did not expose a reliable clickable commit target.",
                    )
                )
            arg_updates["_skip_workflow_action"] = True
            arg_updates["_skip_primary_hotkey"] = True
            arg_updates["_workflow_followup_steps"] = followup_steps
            page_kind = str(form_page_state.get("page_kind", "") or "").replace("_", " ").strip()
            if page_kind:
                warnings.append(
                    f"Surface state classified the current settings surface as a {page_kind} page, so JARVIS will resolve the page prerequisites before committing."
                )
            if pending_requirements:
                warnings.append(
                    f"Form page intelligence found {len(pending_requirements)} pending prerequisite control(s), so JARVIS will stage them before continuing."
                )
            if bool(safety_signals.get("destructive_warning_visible", False)):
                safe_buttons = [str(item).strip() for item in safety_signals.get("safe_dialog_buttons", []) if str(item).strip()]
                if safe_buttons:
                    warnings.append(
                        f"The current form exposes safer alternatives ({', '.join(safe_buttons[:3])}), while the completion workflow will continue through '{preferred_commit_button or 'enter'}'."
                    )
            return {
                "arg_updates": arg_updates,
                "warnings": warnings,
                "target_state_ready": target_state_ready,
                "form_target_state": form_target_state,
            }

        if clean_action == "select_dropdown_option":
            desired_option = self._normalize_probe_text(desired_text)
            current_option = self._normalize_probe_text(current_value_text)
            if desired_option and current_option and desired_option == current_option:
                return _skip_ready_state(
                    "Surface state shows the requested dropdown option is already selected, so JARVIS will preserve the current control state."
                )
            live_option_candidate = _strong_candidate(
                query_text=desired_text,
                control_types={"listitem", "menuitem", "text", "button"},
                minimum_score=0.78,
            )
            if live_option_candidate:
                option_name = str(live_option_candidate.get("name", "") or desired_text).strip()
                option_selected = self._coerce_surface_bool(live_option_candidate.get("selected"))
                if option_selected is True:
                    return _skip_ready_state(
                        f"Surface state shows the requested dropdown option ('{option_name or desired_text}') is already active, so JARVIS will preserve the current selection."
                    )
                arg_updates["_workflow_followup_steps"] = [
                    _followup_accessibility_step(
                        candidate=live_option_candidate,
                        reason=f"Use the live '{option_name}' option exposed by the dropdown instead of typing blindly into the option list.",
                    )
                ]
                warnings.append(
                    f"Surface state exposed the requested dropdown option ('{option_name}'), so JARVIS will select it directly through accessibility."
                )
                return {
                    "arg_updates": arg_updates,
                    "warnings": warnings,
                    "target_state_ready": target_state_ready,
                }

        if clean_action != "set_value_control":
            return {
                "arg_updates": arg_updates,
                "warnings": warnings,
                "target_state_ready": target_state_ready,
                "form_target_state": form_target_state,
            }

        if desired_numeric is not None:
            clamped_value = desired_numeric
            if minimum is not None and float(clamped_value) < float(minimum):
                clamped_value = minimum
                warnings.append(
                    f"The requested target is below the detected control minimum, so JARVIS will clamp it to {minimum}."
                )
            if maximum is not None and float(clamped_value) > float(maximum):
                clamped_value = maximum
                warnings.append(
                    f"The requested target exceeds the detected control maximum, so JARVIS will clamp it to {maximum}."
                )
            arg_updates["text"] = str(clamped_value)
            if current_value_numeric is not None:
                delta = float(clamped_value) - float(current_value_numeric)
                rounded_delta = int(round(delta))
                if abs(delta) < 1e-9 or rounded_delta == 0:
                    target_state_ready = True
                    arg_updates["_skip_workflow_action"] = True
                    arg_updates["_skip_input_steps"] = True
                    arg_updates["_target_state_ready"] = True
                    warnings.append(
                        "Surface state already matches the requested target value, so JARVIS will preserve the current control value."
                    )
                else:
                    arg_updates["_value_control_mode"] = "adjust"
                    arg_updates["_value_adjust_amount"] = max(1, min(abs(rounded_delta), 50))
                    arg_updates["_value_adjust_keys"] = ["up"] if rounded_delta > 0 else ["down"]
                    if abs(delta - rounded_delta) > 0.001:
                        warnings.append(
                            "The detected value delta was not an exact integer step, so JARVIS will use the nearest adjustment count as best effort."
                        )
            else:
                arg_updates["_value_control_mode"] = "type"
                if control_type == "slider":
                    warnings.append(
                        "The current slider value could not be read, so JARVIS will use best-effort direct input after focusing the control."
                    )
        elif desired_text:
            arg_updates["_value_control_mode"] = "type"

        return {
            "arg_updates": arg_updates,
            "warnings": warnings,
            "target_state_ready": target_state_ready,
            "form_target_state": form_target_state,
        }

    def _resolve_app_profile(
        self,
        *,
        args: Dict[str, Any],
        primary_candidate: Optional[Dict[str, Any]] = None,
        active_window: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        profile = self._app_profile_registry.match(
            app_name=str(args.get("app_name", "") or "").strip(),
            window_title=str(args.get("window_title", "") or "").strip(),
        )
        if profile.get("status") == "success":
            return profile
        candidate = primary_candidate if isinstance(primary_candidate, dict) else {}
        active = active_window if isinstance(active_window, dict) else {}
        candidate_exe = str(candidate.get("exe_name", "") or str(candidate.get("exe", "") or "")).rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
        active_exe = str(active.get("exe", "") or "").rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
        return self._app_profile_registry.match(
            app_name=str(args.get("app_name", "") or str(candidate.get("process_name", "") or "")).strip(),
            window_title=str(candidate.get("title", "") or str(active.get("title", "") or "")).strip(),
            exe_name=str(candidate_exe or active_exe).strip(),
        )

    def _apply_profile_defaults(self, *, args: Dict[str, Any], app_profile: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
        if app_profile.get("status") != "success":
            return args, {}
        next_args = dict(args)
        provided_fields = {str(item).strip() for item in next_args.get("_provided_fields", []) if str(item).strip()}
        defaults_applied: Dict[str, Any] = {}
        routing_defaults = app_profile.get("routing_defaults", {}) if isinstance(app_profile.get("routing_defaults", {}), dict) else {}
        autonomy_defaults = app_profile.get("autonomy_defaults", {}) if isinstance(app_profile.get("autonomy_defaults", {}), dict) else {}

        for field_name, source in {
            "target_mode": routing_defaults,
            "verify_mode": routing_defaults,
            "ensure_app_launch": autonomy_defaults,
            "focus_first": autonomy_defaults,
            "verify_after_action": autonomy_defaults,
            "retry_on_verification_failure": autonomy_defaults,
            "max_strategy_attempts": autonomy_defaults,
        }.items():
            if field_name in provided_fields or field_name not in source:
                continue
            next_args[field_name] = source[field_name]
            defaults_applied[field_name] = source[field_name]

        if "verify_text" not in provided_fields and not str(next_args.get("verify_text", "") or "").strip():
            derived_verify_text = self._derive_verify_text(args=next_args, app_profile=app_profile)
            if derived_verify_text:
                next_args["verify_text"] = derived_verify_text
                defaults_applied["verify_text"] = derived_verify_text
        return next_args, defaults_applied

    def _derive_verify_text(self, *, args: Dict[str, Any], app_profile: Dict[str, Any]) -> str:
        explicit = str(args.get("verify_text", "") or "").strip()
        if explicit:
            return explicit
        action = str(args.get("action", "") or "").strip().lower()
        verification_defaults = app_profile.get("verification_defaults", {}) if isinstance(app_profile.get("verification_defaults", {}), dict) else {}
        verify_text_source = str(verification_defaults.get("verify_text_source", "query_or_typed") or "query_or_typed").strip().lower()
        typed_text = str(args.get("text", "") or "").strip()
        query_text = str(args.get("query", "") or "").strip()
        if action == "navigate":
            navigation_hint = self._navigation_verify_text(query_text)
            if navigation_hint:
                return navigation_hint
        definition = self._workflow_definition(action)
        verify_hint = str(definition.get("verify_hint", "") or "").strip()
        if action == "search":
            return query_text or typed_text
        if action == "focus_search_box":
            return "search"
        if action == "go_back":
            return "back"
        if action == "go_forward":
            return "forward"
        if action == "focus_folder_tree":
            return "navigation pane"
        if action == "focus_file_list":
            return "items view"
        if action == "focus_navigation_tree":
            return "tree"
        if action == "focus_list_surface":
            return "list"
        if action == "focus_data_table":
            return "table"
        if action == "focus_sidebar":
            return "sidebar"
        if action == "select_sidebar_item":
            return query_text or typed_text
        if action == "focus_form_surface":
            return "form"
        if action == "focus_input_field":
            return query_text or "field"
        if action == "set_field_value":
            return typed_text or query_text
        if action == "open_dropdown":
            return query_text or "dropdown"
        if action == "select_dropdown_option":
            return typed_text or query_text
        if action == "focus_checkbox":
            return query_text or "checkbox"
        if action == "check_checkbox":
            return f"{query_text} checked".strip() if query_text else "checked"
        if action == "uncheck_checkbox":
            return f"{query_text} unchecked".strip() if query_text else "unchecked"
        if action == "select_radio_option":
            return f"{query_text} selected".strip() if query_text else "selected"
        if action == "select_tab_page":
            return query_text or "tab"
        if action == "focus_value_control":
            return query_text or "value control"
        if action in {"increase_value", "decrease_value"}:
            return query_text or "value"
        if action == "set_value_control":
            return typed_text or query_text
        if action == "toggle_switch":
            return query_text or "switch"
        if action == "enable_switch":
            return f"{query_text} enabled".strip() if query_text else "enabled"
        if action == "disable_switch":
            return f"{query_text} disabled".strip() if query_text else "disabled"
        if action == "focus_main_content":
            return "content"
        if action == "focus_toolbar":
            return "toolbar"
        if action == "invoke_toolbar_action":
            return query_text or typed_text
        if action == "open_context_menu":
            return "menu"
        if action == "select_context_menu_item":
            return query_text or typed_text
        if action == "dismiss_dialog":
            return "cancel"
        if action == "confirm_dialog":
            return "ok"
        if action == "press_dialog_button":
            return query_text or typed_text
        if action == "next_wizard_step":
            return "next"
        if action == "previous_wizard_step":
            return "back"
        if action == "finish_wizard":
            return "finish"
        if action == "complete_wizard_page":
            return "wizard"
        if action == "complete_wizard_flow":
            return "completed"
        if action == "complete_form_page":
            return "settings saved"
        if action == "complete_form_flow":
            return "settings applied"
        if action in {"select_tree_item", "expand_tree_item", "select_list_item", "select_table_row"}:
            return query_text or typed_text
        if action == "open_tab_search":
            return "search tabs"
        if action == "search_tabs":
            return query_text or typed_text
        if action == "command":
            return typed_text or query_text
        if action == "quick_open":
            return query_text or typed_text
        if action == "jump_to_conversation":
            return query_text or typed_text
        if action == "switch_tab":
            tab_target = self._normalize_switch_tab_target(query_text)
            return "" if tab_target in {"next", "previous", "last"} or tab_target.isdigit() else (query_text or "tab")
        if action == "workspace_search":
            return query_text or typed_text
        if action == "find_replace":
            return typed_text or query_text
        if action == "rename_selection":
            return typed_text or query_text
        if action == "open_mail_view":
            return "inbox"
        if action == "open_calendar_view":
            return "calendar"
        if action == "open_people_view":
            return "people"
        if action == "open_tasks_view":
            return "tasks"
        if action == "focus_folder_pane":
            return "folder pane"
        if action == "focus_message_list":
            return "message list"
        if action == "focus_reading_pane":
            return "reading pane"
        if action in {"reply_email", "forward_email"}:
            return "subject"
        if action == "reply_all_email":
            return "cc"
        if action == "new_calendar_event":
            return "appointment"
        if action == "go_to_symbol":
            return query_text or typed_text
        if action == "rename_symbol":
            return typed_text or query_text
        if action == "send_message":
            return typed_text or query_text
        if action in {"zoom_in", "zoom_out"}:
            return "zoom"
        if action == "reset_zoom":
            return "100%"
        if action == "terminal_command":
            return typed_text or query_text
        if verify_hint:
            return verify_hint
        if verify_text_source == "typed_text":
            return typed_text
        if verify_text_source == "query":
            return query_text
        return typed_text or query_text

    def _workflow_profile(self, *, requested_action: str, args: Dict[str, Any], app_profile: Dict[str, Any]) -> Dict[str, Any]:
        clean_action = str(requested_action or "").strip().lower()
        if clean_action not in WORKFLOW_ACTIONS:
            return {}
        definition = self._workflow_definition(clean_action)
        hotkeys = self._workflow_hotkey_candidates(requested_action=clean_action, args=args, app_profile=app_profile)
        category = str(app_profile.get("category", "") or "").strip().lower()
        workflow_action_name = str(definition.get("workflow_action", "") or "").strip().lower()
        supports_without_hotkey_categories = {
            str(item).strip().lower()
            for item in definition.get("supports_without_hotkey_categories", set())
            if str(item).strip()
        }
        supports_system_action_categories = {
            str(item).strip().lower()
            for item in definition.get("supports_system_action_categories", set())
            if str(item).strip()
        }
        supports_action_dispatch_categories = {
            str(item).strip().lower()
            for item in definition.get("supports_action_dispatch_categories", set())
            if str(item).strip()
        }
        supports_stateful_categories = {
            str(item).strip().lower()
            for item in definition.get("supports_stateful_categories", set())
            if str(item).strip()
        }
        supports_direct_input = category in supports_without_hotkey_categories
        supports_system_action = bool(workflow_action_name) and category in supports_system_action_categories
        supports_action_dispatch = bool(workflow_action_name) and category in supports_action_dispatch_categories
        supports_stateful_execution = category in supports_stateful_categories
        explicit_window_target = bool(str(args.get("window_title", "") or "").strip())
        if clean_action in {"complete_form_page", "complete_form_flow", "complete_wizard_page", "complete_wizard_flow"} and explicit_window_target:
            supports_stateful_execution = True
        supported = bool(hotkeys) or supports_direct_input or supports_system_action or supports_action_dispatch or supports_stateful_execution
        return {
            "action": clean_action,
            "title": str(definition.get("title", clean_action.replace("_", " ").title()) or clean_action.replace("_", " ").title()),
            "supported": supported,
            "category": category,
            "category_hints": self._workflow_category_hints(definition),
            "hotkeys": [list(row) for row in hotkeys],
            "primary_hotkey": list(hotkeys[0]) if hotkeys else [],
            "alternate_hotkeys": [list(row) for row in hotkeys[1:4]],
            "supports_direct_input": supports_direct_input,
            "supports_system_action": supports_system_action,
            "supports_action_dispatch": supports_action_dispatch,
            "supports_stateful_execution": supports_stateful_execution,
            "workflow_action": workflow_action_name,
            "workflow_action_args": dict(definition.get("workflow_action_args", {}))
            if isinstance(definition.get("workflow_action_args", {}), dict)
            else {},
            "workflow_action_reason": str(definition.get("workflow_action_reason", "") or ""),
            "prefer_workflow_action": bool(definition.get("prefer_workflow_action", False)),
            "requires_input": bool(definition.get("requires_input", False)),
            "input_field": str(definition.get("input_field", "") or "").strip(),
            "required_fields": self._workflow_required_fields(requested_action=clean_action),
            "input_sequence": [dict(row) for row in definition.get("input_sequence", []) if isinstance(row, dict)],
            "press_enter_default": bool(definition.get("default_press_enter", False)),
            "verify_hint": str(definition.get("verify_hint", "") or ""),
            "route_mode": str(definition.get("route_mode", "workflow_desktop") or "workflow_desktop"),
            "hotkey_reason": str(definition.get("hotkey_reason", "") or ""),
            "input_reason": str(definition.get("input_reason", "") or ""),
            "probe_queries": self._workflow_probe_queries(
                requested_action=clean_action,
                args=args,
                advice={"app_profile": app_profile},
            ),
            "recommended_followups": self._workflow_followups(clean_action),
            "message": "" if supported else str(definition.get("support_message", "Unsupported desktop workflow.") or "Unsupported desktop workflow."),
        }

    def _workflow_hotkey_candidates(self, *, requested_action: str, args: Dict[str, Any], app_profile: Dict[str, Any]) -> List[List[str]]:
        clean_action = str(requested_action or "").strip().lower()
        definition = self._workflow_definition(clean_action)
        explicit_keys = [str(item).strip().lower() for item in args.get("keys", []) if str(item).strip()]
        candidates: List[List[str]] = [explicit_keys] if explicit_keys else []
        if clean_action == "switch_tab":
            dynamic_candidates = self._switch_tab_hotkeys(args=args, app_profile=app_profile)
            for row in dynamic_candidates:
                if row and row not in candidates:
                    candidates.append(row)
            return candidates
        workflow_defaults = app_profile.get("workflow_defaults", {}) if isinstance(app_profile.get("workflow_defaults", {}), dict) else {}
        field_name = str(definition.get("hotkey_field", "") or "").strip()
        raw_rows = workflow_defaults.get(field_name, []) if field_name else []
        source_rows = raw_rows if isinstance(raw_rows, list) else []
        for row in source_rows:
            if isinstance(row, list):
                normalized = [str(item).strip().lower() for item in row if str(item).strip()]
            elif isinstance(row, str):
                normalized = [part.strip().lower() for part in re.split(r"[+,]", row) if part.strip()]
            else:
                normalized = []
            if normalized and normalized not in candidates:
                candidates.append(normalized)
        category = str(app_profile.get("category", "") or "").strip().lower()
        if not candidates and clean_action == "search":
            candidates.append(["ctrl", "f"])
        if not candidates and clean_action == "focus_search_box":
            candidates.append(["ctrl", "f"])
        if not candidates and clean_action == "navigate" and category == "browser":
            candidates.append(["ctrl", "l"])
        if not candidates and clean_action == "command" and category in {"code_editor", "ide"}:
            candidates.append(["ctrl", "shift", "p"])
        if not candidates and clean_action == "quick_open" and category in {"code_editor", "ide"}:
            candidates.append(["ctrl", "p"])
        if not candidates and clean_action == "focus_address_bar" and category in {"browser", "file_manager"}:
            candidates.extend([["ctrl", "l"], ["alt", "d"]])
        if not candidates and clean_action == "open_bookmarks" and category == "browser":
            candidates.append(["ctrl", "shift", "o"])
        if not candidates and clean_action == "focus_explorer" and category == "code_editor":
            candidates.append(["ctrl", "shift", "e"])
        if not candidates and clean_action == "focus_explorer" and category == "ide":
            candidates.extend([["alt", "1"], ["ctrl", "shift", "e"]])
        if not candidates and clean_action == "new_folder" and category == "file_manager":
            candidates.append(["ctrl", "shift", "n"])
        if not candidates and clean_action == "rename_selection" and category == "file_manager":
            candidates.append(["f2"])
        if not candidates and clean_action == "open_properties_dialog" and category == "file_manager":
            candidates.append(["alt", "enter"])
        if not candidates and clean_action == "open_preview_pane" and category == "file_manager":
            candidates.append(["alt", "p"])
        if not candidates and clean_action == "open_details_pane" and category == "file_manager":
            candidates.append(["alt", "shift", "p"])
        if not candidates and clean_action == "open_context_menu" and category in {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
            candidates.extend([["shift", "f10"], ["apps"]])
        if not candidates and clean_action == "dismiss_dialog" and category in {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
            candidates.append(["esc"])
        if not candidates and clean_action == "confirm_dialog" and category in {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
            candidates.append(["enter"])
        if not candidates and clean_action == "next_wizard_step" and category in {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"}:
            candidates.extend([["alt", "n"], ["enter"]])
        if not candidates and clean_action == "previous_wizard_step" and category in {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"}:
            candidates.append(["alt", "b"])
        if not candidates and clean_action == "finish_wizard" and category in {"utility", "ops_console", "security", "office", "general_desktop", "ai_companion"}:
            candidates.extend([["alt", "f"], ["enter"]])
        if not candidates and clean_action in {"open_dropdown", "select_dropdown_option"} and category in {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
            candidates.append(["alt", "down"])
        if not candidates and clean_action in {"check_checkbox", "uncheck_checkbox"} and category in {"browser", "file_manager", "code_editor", "ide", "terminal", "chat", "office", "utility", "ops_console", "security", "ai_companion", "general_desktop"}:
            candidates.append(["space"])
        if not candidates and clean_action == "refresh_view" and category == "browser":
            candidates.extend([["f5"], ["ctrl", "r"]])
        if not candidates and clean_action == "refresh_view" and category in {"file_manager", "ops_console", "general_desktop"}:
            candidates.append(["f5"])
        if not candidates and clean_action == "go_back" and category in {"browser", "file_manager"}:
            candidates.append(["alt", "left"])
        if not candidates and clean_action == "go_forward" and category in {"browser", "file_manager"}:
            candidates.append(["alt", "right"])
        if not candidates and clean_action == "go_up_level" and category == "file_manager":
            candidates.append(["alt", "up"])
        if not candidates and clean_action == "workspace_search" and category in {"code_editor", "ide"}:
            candidates.append(["ctrl", "shift", "f"])
        if not candidates and clean_action == "find_replace" and category in {"code_editor", "office"}:
            candidates.append(["ctrl", "h"])
        if not candidates and clean_action == "find_replace" and category == "ide":
            candidates.extend([["ctrl", "h"], ["ctrl", "r"]])
        if not candidates and clean_action == "go_to_symbol" and category == "code_editor":
            candidates.append(["ctrl", "shift", "o"])
        if not candidates and clean_action == "go_to_symbol" and category == "ide":
            candidates.extend([["ctrl", "alt", "shift", "n"], ["ctrl", "shift", "o"]])
        if not candidates and clean_action == "rename_symbol" and category == "code_editor":
            candidates.append(["f2"])
        if not candidates and clean_action == "rename_symbol" and category == "ide":
            candidates.extend([["shift", "f6"], ["f2"]])
        if not candidates and clean_action == "new_tab" and category == "browser":
            candidates.append(["ctrl", "t"])
        if not candidates and clean_action == "new_tab" and category == "terminal":
            candidates.extend([["ctrl", "shift", "t"], ["ctrl", "t"]])
        if not candidates and clean_action == "new_tab" and category == "file_manager":
            candidates.append(["ctrl", "t"])
        if not candidates and clean_action == "switch_tab":
            candidates.extend(self._switch_tab_hotkeys(args=args, app_profile=app_profile))
        if not candidates and clean_action == "close_tab" and category in {"browser", "code_editor", "ide", "file_manager"}:
            candidates.append(["ctrl", "w"])
        if not candidates and clean_action == "reopen_tab" and category in {"browser", "code_editor", "ide", "file_manager"}:
            candidates.append(["ctrl", "shift", "t"])
        if not candidates and clean_action == "open_history" and category == "browser":
            candidates.append(["ctrl", "h"])
        if not candidates and clean_action == "open_downloads" and category == "browser":
            candidates.append(["ctrl", "j"])
        if not candidates and clean_action == "open_devtools" and category == "browser":
            candidates.extend([["f12"], ["ctrl", "shift", "i"]])
        if not candidates and clean_action in {"open_tab_search", "search_tabs"} and category == "browser":
            candidates.append(["ctrl", "shift", "a"])
        if not candidates and clean_action == "new_chat" and category == "chat":
            candidates.extend([["ctrl", "n"], ["ctrl", "k"], ["ctrl", "e"]])
        if not candidates and clean_action == "jump_to_conversation" and category == "chat":
            candidates.extend([["ctrl", "k"], ["ctrl", "e"], ["ctrl", "n"]])
        if not candidates and clean_action == "send_message" and category == "chat":
            candidates.extend([["ctrl", "k"], ["ctrl", "e"], ["ctrl", "n"]])
        if not candidates and clean_action == "new_document" and category in {"office", "code_editor", "ide"}:
            candidates.append(["ctrl", "n"])
        if not candidates and clean_action == "save_document" and category in {"office", "code_editor", "ide"}:
            candidates.append(["ctrl", "s"])
        if not candidates and clean_action == "open_print_dialog" and category in {"office", "browser", "code_editor", "ide", "general_desktop"}:
            candidates.append(["ctrl", "p"])
        if not candidates and clean_action == "start_presentation" and category == "office":
            candidates.extend([["f5"], ["shift", "f5"]])
        if not candidates and clean_action == "open_people_view" and category == "office":
            candidates.append(["ctrl", "3"])
        if not candidates and clean_action == "open_tasks_view" and category == "office":
            candidates.append(["ctrl", "4"])
        if not candidates and clean_action == "reply_email" and category == "office":
            candidates.append(["ctrl", "r"])
        if not candidates and clean_action == "reply_all_email" and category == "office":
            candidates.append(["ctrl", "shift", "r"])
        if not candidates and clean_action == "forward_email" and category == "office":
            candidates.append(["ctrl", "f"])
        if not candidates and clean_action == "new_calendar_event" and category == "office":
            candidates.append(["ctrl", "shift", "a"])
        if not candidates and clean_action == "toggle_terminal" and category == "code_editor":
            candidates.extend([["ctrl", "`"], ["ctrl", "shift", "`"]])
        if not candidates and clean_action == "toggle_terminal" and category == "ide":
            candidates.extend([["alt", "f12"], ["ctrl", "`"]])
        if not candidates and clean_action == "format_document" and category == "code_editor":
            candidates.append(["shift", "alt", "f"])
        if not candidates and clean_action == "format_document" and category == "ide":
            candidates.extend([["ctrl", "alt", "l"], ["shift", "alt", "f"]])
        if not candidates and clean_action == "zoom_in" and category in {"browser", "code_editor", "ide", "office", "utility"}:
            candidates.extend([["ctrl", "equal"], ["ctrl", "plus"]])
        if not candidates and clean_action == "zoom_out" and category in {"browser", "code_editor", "ide", "office", "utility"}:
            candidates.append(["ctrl", "minus"])
        if not candidates and clean_action == "reset_zoom" and category in {"browser", "code_editor", "ide", "office", "utility"}:
            candidates.append(["ctrl", "0"])
        if not candidates and clean_action == "terminal_command" and category in {"code_editor", "ide"}:
            candidates.extend([["ctrl", "`"], ["ctrl", "shift", "`"]])
        return candidates

    @staticmethod
    def _normalize_switch_tab_target(value: Any) -> str:
        clean = " ".join(str(value or "").strip().lower().split())
        if not clean:
            return ""
        alias_map = {
            "next": "next",
            "next tab": "next",
            "forward": "next",
            "right": "next",
            "following": "next",
            "previous": "previous",
            "previous tab": "previous",
            "prev": "previous",
            "prev tab": "previous",
            "prior": "previous",
            "back": "previous",
            "left": "previous",
            "last": "last",
            "last tab": "last",
            "final": "last",
            "end": "last",
            "first": "1",
            "first tab": "1",
            "1st": "1",
            "one": "1",
        }
        if clean in alias_map:
            return alias_map[clean]
        ordinal_words = {
            "second": "2",
            "2nd": "2",
            "two": "2",
            "third": "3",
            "3rd": "3",
            "three": "3",
            "fourth": "4",
            "4th": "4",
            "four": "4",
            "fifth": "5",
            "5th": "5",
            "five": "5",
            "sixth": "6",
            "6th": "6",
            "six": "6",
            "seventh": "7",
            "7th": "7",
            "seven": "7",
            "eighth": "8",
            "8th": "8",
            "eight": "8",
            "ninth": "9",
            "9th": "9",
            "nine": "9",
        }
        if clean in ordinal_words:
            return ordinal_words[clean]
        digit_match = re.search(r"\b([1-9])(?:st|nd|rd|th)?\b", clean)
        if digit_match:
            return str(digit_match.group(1))
        tab_match = re.search(r"\btab\s+([1-9])\b", clean)
        if tab_match:
            return str(tab_match.group(1))
        return ""

    def _switch_tab_hotkeys(self, *, args: Dict[str, Any], app_profile: Dict[str, Any]) -> List[List[str]]:
        workflow_defaults = app_profile.get("workflow_defaults", {}) if isinstance(app_profile.get("workflow_defaults", {}), dict) else {}
        category = str(app_profile.get("category", "") or "").strip().lower()
        target = self._normalize_switch_tab_target(args.get("query", ""))
        field_name = ""
        if not target or target == "next":
            field_name = "next_tab_hotkeys"
        elif target == "previous":
            field_name = "previous_tab_hotkeys"
        elif target == "last":
            field_name = "last_tab_hotkeys"

        rows: List[List[str]] = []
        if field_name:
            raw_rows = workflow_defaults.get(field_name, []) if isinstance(workflow_defaults.get(field_name, []), list) else []
            for row in raw_rows:
                if isinstance(row, list):
                    normalized = [str(item).strip().lower() for item in row if str(item).strip()]
                elif isinstance(row, str):
                    normalized = [part.strip().lower() for part in re.split(r"[+,]", row) if part.strip()]
                else:
                    normalized = []
                if normalized and normalized not in rows:
                    rows.append(normalized)

        if target.isdigit() and category == "browser":
            rows.append(["ctrl", target])
        elif target == "last" and category == "browser" and ["ctrl", "9"] not in rows:
            rows.append(["ctrl", "9"])
        elif not rows and target == "next" and category in {"browser", "code_editor", "ide", "terminal", "ops_console", "utility", "file_manager"}:
            rows.extend([["ctrl", "tab"], ["ctrl", "pgdn"]])
        elif not rows and target == "previous" and category in {"browser", "code_editor", "ide", "terminal", "ops_console", "utility", "file_manager"}:
            rows.extend([["ctrl", "shift", "tab"], ["ctrl", "pgup"]])

        deduped: List[List[str]] = []
        for row in rows:
            if row and row not in deduped:
                deduped.append(row)
        return deduped

    def _workflow_required_fields(self, *, requested_action: str) -> List[str]:
        definition = self._workflow_definition(requested_action)
        rows = [
            str(field_name).strip()
            for field_name in definition.get("required_fields", [])
            if str(field_name).strip() and str(field_name).strip().lower() != "none"
        ]
        if rows:
            return self._dedupe_strings(rows)
        if bool(definition.get("requires_input", False)):
            input_field = str(definition.get("input_field", "") or "").strip()
            if input_field and input_field.lower() != "none":
                return [input_field]
        return []

    def _workflow_missing_required_fields(self, *, requested_action: str, args: Dict[str, Any]) -> List[str]:
        missing: List[str] = []
        for field_name in self._workflow_required_fields(requested_action=requested_action):
            if not str(args.get(field_name, "") or "").strip():
                missing.append(field_name)
        return missing

    def _workflow_input_steps(
        self,
        *,
        requested_action: str,
        args: Dict[str, Any],
        workflow_profile: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        clean_action = str(requested_action or "").strip().lower()
        definition = self._workflow_definition(clean_action)
        direct_steps = args.get("_workflow_followup_steps", [])
        if isinstance(direct_steps, list) and any(isinstance(step, dict) for step in direct_steps):
            return [dict(step) for step in direct_steps if isinstance(step, dict)]
        if bool(args.get("_skip_input_steps", False)):
            return []
        if clean_action == "set_value_control":
            if bool(args.get("_target_state_ready", False)):
                return []
            target_text = str(args.get("text", "") or "").strip()
            mode = str(args.get("_value_control_mode", "") or "").strip().lower()
            if mode == "adjust":
                keys = [str(item).strip().lower() for item in args.get("_value_adjust_keys", []) if str(item).strip()]
                repeat_count = max(1, min(int(args.get("_value_adjust_amount", 1) or 1), 50))
                if not keys:
                    keys = ["up"]
                return [
                    self._plan_step(
                        action="keyboard_hotkey",
                        args={"keys": list(keys)},
                        phase="input",
                        optional=False,
                        reason="Move the focused value control toward the requested target state.",
                    )
                    for _ in range(repeat_count)
                ]
            if target_text:
                return [
                    self._plan_step(
                        action="keyboard_hotkey",
                        args={"keys": ["ctrl", "a"]},
                        phase="workflow_target",
                        optional=False,
                        reason="Select the current control value before replacing it with the requested target.",
                    ),
                    self._plan_step(
                        action="keyboard_type",
                        args={"text": target_text, "press_enter": False},
                        phase="input",
                        optional=False,
                        reason="Type the requested target value into the focused control.",
                    ),
                ]
            return []
        if bool(definition.get("skip_input_steps", False)):
            return []
        sequence = [dict(row) for row in definition.get("input_sequence", []) if isinstance(row, dict)]
        fallback_reason = str(workflow_profile.get("input_reason", "") or "Send the workflow input to the focused desktop target.")
        if not sequence:
            workflow_text = self._workflow_input_text(requested_action=clean_action, args=args)
            if not workflow_text:
                return []
            return [
                self._plan_step(
                    action="keyboard_type",
                    args={
                        "text": workflow_text,
                        "press_enter": self._workflow_press_enter(requested_action=clean_action, args=args),
                    },
                    phase="input",
                    optional=False,
                    reason=fallback_reason,
                )
            ]

        steps: List[Dict[str, Any]] = []
        for row in sequence:
            row_action = str(row.get("action", "") or "").strip().lower()
            if row_action and row_action != "keyboard_type":
                if row_action == "keyboard_hotkey":
                    keys_value = row.get("keys", [])
                    if isinstance(keys_value, list):
                        keys = [str(item).strip().lower() for item in keys_value if str(item).strip()]
                    elif isinstance(keys_value, str):
                        keys = [part.strip().lower() for part in re.split(r"[+,]", keys_value) if part.strip()]
                    else:
                        keys = []
                    if not keys:
                        continue
                    repeat_count = 1
                    repeat_field = str(row.get("repeat_field", "") or "").strip()
                    repeat_value = row.get("repeat")
                    if repeat_field:
                        repeat_value = args.get(repeat_field, repeat_value)
                    if repeat_value is None:
                        repeat_value = row.get("repeat_default", 1)
                    try:
                        repeat_count = max(1, min(int(repeat_value or 1), int(row.get("max_repeat", 20) or 20)))
                    except Exception:
                        repeat_count = 1
                    for _ in range(repeat_count):
                        steps.append(
                            self._plan_step(
                                action="keyboard_hotkey",
                                args={"keys": keys},
                                phase=str(row.get("phase", "workflow_target") or "workflow_target"),
                                optional=bool(row.get("optional", False)),
                                reason=str(row.get("reason", "") or "Dispatch the workflow's intermediate key chord."),
                            )
                        )
                continue
            field_name = str(row.get("field", "") or "").strip()
            if not field_name:
                continue
            if field_name == "query" and bool(args.get("_target_query_already_active", False)):
                continue
            text_value = str(args.get(field_name, "") or "").strip()
            if not text_value:
                if bool(row.get("optional", False)):
                    continue
                continue
            if "press_enter" in row:
                press_enter = bool(row.get("press_enter", False))
            else:
                press_enter = self._workflow_press_enter(requested_action=clean_action, args=args)
            steps.append(
                self._plan_step(
                    action="keyboard_type",
                    args={"text": text_value, "press_enter": press_enter},
                    phase=str(row.get("phase", "input") or "input"),
                    optional=False,
                    reason=str(row.get("reason", "") or fallback_reason),
                )
            )
        return steps

    @staticmethod
    def _workflow_input_text(*, requested_action: str, args: Dict[str, Any]) -> str:
        clean_action = str(requested_action or "").strip().lower()
        if clean_action in {"navigate", "search", "quick_open", "workspace_search", "go_to_symbol", "jump_to_conversation", "search_tabs", "select_sidebar_item", "invoke_toolbar_action", "select_context_menu_item", "press_dialog_button", "select_tree_item", "expand_tree_item", "select_list_item", "select_table_row", "focus_input_field", "set_field_value", "open_dropdown", "select_dropdown_option", "focus_checkbox", "check_checkbox", "uncheck_checkbox", "select_radio_option", "select_tab_page", "focus_value_control", "increase_value", "decrease_value", "set_value_control", "toggle_switch", "enable_switch", "disable_switch"}:
            return str(args.get("query", "") or "").strip()
        if clean_action == "find_replace":
            return str(args.get("query", "") or args.get("text", "") or "").strip()
        if clean_action in {"command", "terminal_command", "rename_symbol", "rename_selection", "send_message"}:
            return str(args.get("text", "") or args.get("query", "") or "").strip()
        return ""

    @staticmethod
    def _resolve_workflow_action_args(template_args: Any, args: Dict[str, Any]) -> Any:
        if isinstance(template_args, dict):
            return {
                str(key): DesktopActionRouter._resolve_workflow_action_args(value, args)
                for key, value in template_args.items()
                if str(key).strip()
            }
        if isinstance(template_args, list):
            return [DesktopActionRouter._resolve_workflow_action_args(value, args) for value in template_args]
        if isinstance(template_args, str):
            match = re.fullmatch(r"\{\{\s*args\.([a-zA-Z0-9_]+)\s*\}\}", template_args.strip())
            if match:
                value = args.get(match.group(1))
                if isinstance(value, str):
                    return value.strip()
                return value
        return template_args

    @staticmethod
    def _workflow_press_enter(*, requested_action: str, args: Dict[str, Any]) -> bool:
        provided_fields = {str(item).strip() for item in args.get("_provided_fields", []) if str(item).strip()}
        if "press_enter" in provided_fields:
            return bool(args.get("press_enter", False) or args.get("submit", False))
        clean_action = str(requested_action or "").strip().lower()
        definition = WORKFLOW_DEFINITIONS.get(clean_action, {})
        if bool(definition.get("default_press_enter", False)):
            return True
        return bool(args.get("press_enter", False) or args.get("submit", False))

    @staticmethod
    def _workflow_definition(requested_action: str) -> Dict[str, Any]:
        clean_action = str(requested_action or "").strip().lower()
        definition = WORKFLOW_DEFINITIONS.get(clean_action, {})
        return dict(definition) if isinstance(definition, dict) else {}

    @staticmethod
    def _normalize_probe_text(value: Any) -> str:
        return " ".join(str(value or "").strip().lower().split())

    @staticmethod
    def _coerce_surface_bool(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            if int(value) in {0, 1}:
                return bool(int(value))
            return None
        clean = DesktopActionRouter._normalize_probe_text(value)
        if clean in {"true", "yes", "on", "checked", "selected", "expanded", "open"}:
            return True
        if clean in {"false", "no", "off", "unchecked", "unselected", "collapsed", "closed"}:
            return False
        return None

    @staticmethod
    def _normalize_surface_number(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else round(value, 6)
        clean = str(value).strip().rstrip("%")
        if not clean:
            return None
        try:
            number = float(clean)
        except Exception:
            return None
        return int(number) if number.is_integer() else round(number, 6)

    @classmethod
    def _element_search_text(cls, row: Dict[str, Any]) -> str:
        parts: List[str] = []
        for field in ("name", "automation_id", "class_name", "control_type", "state_text", "value_text"):
            value = str(row.get(field, "") or "").strip()
            if value:
                parts.append(value.replace("_", " ").replace("-", " "))
        if row.get("range_value") is not None:
            parts.append(str(row.get("range_value")))
        return cls._normalize_probe_text(" ".join(parts))

    @classmethod
    def _element_query_match_score(cls, row: Dict[str, Any], query: str) -> float:
        normalized_query = cls._normalize_probe_text(query)
        if not normalized_query:
            return 0.0
        name = cls._normalize_probe_text(row.get("name", ""))
        haystack = cls._element_search_text(row)
        if not haystack:
            return 0.0
        if name == normalized_query or haystack == normalized_query:
            return 1.0
        if normalized_query in name:
            return 0.92
        if normalized_query in haystack:
            return 0.82
        query_tokens = {token for token in re.split(r"[^a-z0-9]+", normalized_query) if token}
        haystack_tokens = {token for token in re.split(r"[^a-z0-9]+", haystack) if token}
        if not query_tokens or not haystack_tokens:
            return 0.0
        overlap = len(query_tokens.intersection(haystack_tokens))
        if overlap <= 0:
            return 0.0
        return overlap / max(1.0, len(query_tokens))

    @classmethod
    def _element_state_summary(cls, row: Dict[str, Any], *, match_score: Optional[float] = None) -> Dict[str, Any]:
        toggle_state = cls._normalize_probe_text(row.get("toggle_state", ""))
        summary = {
            "element_id": str(row.get("element_id", "") or "").strip(),
            "parent_id": str(row.get("parent_id", "") or "").strip(),
            "name": str(row.get("name", "") or "").strip(),
            "window_title": str(row.get("window_title", "") or "").strip(),
            "control_type": str(row.get("control_type", "") or "").strip(),
            "automation_id": str(row.get("automation_id", "") or "").strip(),
            "class_name": str(row.get("class_name", "") or "").strip(),
            "enabled": cls._coerce_surface_bool(row.get("enabled")),
            "visible": cls._coerce_surface_bool(row.get("visible")),
            "selected": cls._coerce_surface_bool(row.get("selected")),
            "checked": cls._coerce_surface_bool(row.get("checked")),
            "expanded": cls._coerce_surface_bool(row.get("expanded")),
            "toggle_state": toggle_state,
            "value_text": str(row.get("value_text", "") or "").strip(),
            "state_text": str(row.get("state_text", "") or "").strip(),
            "range_value": cls._normalize_surface_number(row.get("range_value")),
            "range_min": cls._normalize_surface_number(row.get("range_min")),
            "range_max": cls._normalize_surface_number(row.get("range_max")),
        }
        if match_score is not None:
            summary["match_score"] = round(match_score, 6)
        return {key: value for key, value in summary.items() if value not in {None, ""}}

    @classmethod
    def _query_target_elements(
        cls,
        *,
        elements: Any,
        query: str,
        limit: int = 5,
        control_types: Optional[set[str]] = None,
    ) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in elements if isinstance(row, dict)] if isinstance(elements, list) else []
        bounded = max(1, min(int(limit or 5), 12))
        normalized_control_types = {
            cls._normalize_probe_text(value)
            for value in (control_types or set())
            if cls._normalize_probe_text(value)
        }
        ranked: List[tuple[float, Dict[str, Any]]] = []
        for row in rows:
            if normalized_control_types:
                row_control_type = cls._normalize_probe_text(row.get("control_type", ""))
                if row_control_type not in normalized_control_types:
                    continue
            score = cls._element_query_match_score(row, query)
            if score <= 0:
                continue
            ranked.append((score, cls._element_state_summary(row, match_score=score)))
        ranked.sort(key=lambda item: item[0], reverse=True)
        return [row for _, row in ranked[:bounded]]

    @classmethod
    def _selection_candidate_elements(cls, *, elements: Any, limit: int = 12) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in elements if isinstance(row, dict)] if isinstance(elements, list) else []
        bounded = max(1, min(int(limit or 12), 24))
        selection_types = {"listitem", "menuitem", "radiobutton", "tabitem", "button", "splitbutton", "checkbox"}
        candidates: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            control_type = cls._normalize_probe_text(row.get("control_type", ""))
            if control_type not in selection_types:
                continue
            key = cls._element_identity_key(row)
            if not key or key in seen:
                continue
            seen.add(key)
            candidates.append(cls._element_state_summary(row))
            if len(candidates) >= bounded:
                break
        return candidates

    @staticmethod
    def _form_target_family(action_name: str) -> str:
        action = str(action_name or "").strip().lower()
        family_map = {
            "set_field_value": "field_value",
            "set_value_control": "value_control",
            "select_dropdown_option": "dropdown_selection",
            "check_checkbox": "checkbox_state",
            "uncheck_checkbox": "checkbox_state",
            "enable_switch": "switch_state",
            "disable_switch": "switch_state",
            "select_radio_option": "radio_selection",
            "select_tab_page": "tab_selection",
        }
        return family_map.get(action, action)

    @classmethod
    def _form_target_key(cls, target: Dict[str, Any]) -> str:
        if not isinstance(target, dict):
            return ""
        family = cls._form_target_family(str(target.get("action") or ""))
        query = cls._normalize_probe_text(target.get("query", ""))
        if not family or not query:
            return ""
        return f"{family}:{query}"

    @classmethod
    def _normalize_form_target_plan(cls, plan: Any) -> List[Dict[str, Any]]:
        allowed_actions = {
            "set_field_value",
            "set_value_control",
            "select_dropdown_option",
            "check_checkbox",
            "uncheck_checkbox",
            "enable_switch",
            "disable_switch",
            "select_radio_option",
            "select_tab_page",
        }
        normalized: List[Dict[str, Any]] = []
        for row in plan if isinstance(plan, list) else []:
            if not isinstance(row, dict):
                continue
            action_name = str(row.get("action") or "").strip().lower()
            query = str(row.get("query") or "").strip()
            text_value = str(row.get("text") or "").strip()
            if action_name not in allowed_actions or not query:
                continue
            if action_name in {"set_field_value", "set_value_control", "select_dropdown_option"} and not text_value:
                continue
            target = {
                "action": action_name,
                "query": query,
                "family": cls._form_target_family(action_name),
            }
            if text_value:
                target["text"] = text_value
            target_key = cls._form_target_key(target)
            if not target_key:
                continue
            normalized = [item for item in normalized if cls._form_target_key(item) != target_key]
            normalized.append(target)
        return normalized

    @classmethod
    def _form_target_control_types(cls, *, action_name: str, option_phase: bool = False) -> set[str]:
        action = str(action_name or "").strip().lower()
        if action == "set_field_value":
            return {"edit", "combobox", "document"}
        if action == "set_value_control":
            return {"slider", "spinner", "edit", "combobox"}
        if action == "select_dropdown_option":
            return {"listitem", "menuitem", "text", "button"} if option_phase else {"combobox", "button", "edit"}
        if action in {"check_checkbox", "uncheck_checkbox"}:
            return {"checkbox"}
        if action in {"enable_switch", "disable_switch"}:
            return {"checkbox", "button", "togglebutton"}
        if action == "select_radio_option":
            return {"radiobutton"}
        if action == "select_tab_page":
            return {"tabitem"}
        return set()

    @classmethod
    def _form_target_satisfied(
        cls,
        *,
        target: Dict[str, Any],
        control_candidate: Dict[str, Any],
        option_candidate: Dict[str, Any],
    ) -> bool:
        action_name = str(target.get("action") or "").strip().lower()
        desired_text = cls._normalize_probe_text(target.get("text", ""))
        control_checked = cls._coerce_surface_bool(control_candidate.get("checked"))
        control_selected = cls._coerce_surface_bool(control_candidate.get("selected"))
        control_toggle = cls._normalize_probe_text(control_candidate.get("toggle_state", ""))
        control_value = cls._normalize_probe_text(control_candidate.get("value_text", "") or control_candidate.get("state_text", ""))
        control_numeric = cls._normalize_surface_number(control_candidate.get("range_value"))
        if control_numeric is None:
            control_numeric = cls._normalize_surface_number(control_candidate.get("value_text"))
        desired_numeric = cls._normalize_surface_number(target.get("text"))
        option_selected = cls._coerce_surface_bool(option_candidate.get("selected"))
        option_checked = cls._coerce_surface_bool(option_candidate.get("checked"))
        option_name = cls._normalize_probe_text(option_candidate.get("name", ""))
        if action_name in {"check_checkbox", "enable_switch"}:
            return bool(control_checked is True or control_toggle in {"on", "checked"})
        if action_name in {"uncheck_checkbox", "disable_switch"}:
            return bool(control_checked is False or control_toggle in {"off", "unchecked"})
        if action_name in {"select_radio_option", "select_tab_page"}:
            return bool(control_selected is True or control_checked is True)
        if action_name == "set_field_value":
            return bool(desired_text and desired_text == control_value)
        if action_name == "set_value_control":
            if desired_numeric is not None and control_numeric is not None:
                return abs(control_numeric - desired_numeric) < 0.0001
            return bool(desired_text and desired_text == control_value)
        if action_name == "select_dropdown_option":
            if option_candidate:
                return bool((option_selected is True or option_checked is True) and option_name == desired_text)
            return bool(desired_text and desired_text == control_value)
        return False

    @classmethod
    def _form_target_plan_state(
        cls,
        *,
        plan: Any,
        snapshot: Dict[str, Any],
    ) -> Dict[str, Any]:
        normalized_plan = cls._normalize_form_target_plan(plan)
        if not normalized_plan:
            return {}
        element_payload = snapshot.get("elements", {}) if isinstance(snapshot.get("elements", {}), dict) else {}
        elements = element_payload.get("items", []) if isinstance(element_payload.get("items", []), list) else []
        targets: List[Dict[str, Any]] = []
        for target in normalized_plan:
            action_name = str(target.get("action") or "").strip().lower()
            query = str(target.get("query") or "").strip()
            text_value = str(target.get("text") or "").strip()
            control_candidate_rows = cls._query_target_elements(
                elements=elements,
                query=query,
                control_types=cls._form_target_control_types(action_name=action_name),
                limit=3,
            )
            control_candidate = dict(control_candidate_rows[0]) if control_candidate_rows else {}
            option_candidate_rows: List[Dict[str, Any]] = []
            if action_name == "select_dropdown_option" and text_value:
                option_candidate_rows = cls._query_target_elements(
                    elements=elements,
                    query=text_value,
                    control_types=cls._form_target_control_types(action_name=action_name, option_phase=True),
                    limit=3,
                )
            option_candidate = dict(option_candidate_rows[0]) if option_candidate_rows else {}
            satisfied = cls._form_target_satisfied(
                target=target,
                control_candidate=control_candidate,
                option_candidate=option_candidate,
            )
            visible = bool(control_candidate or option_candidate)
            target_row = {
                **target,
                "key": cls._form_target_key(target),
                "visible": visible,
                "satisfied": satisfied,
                "control_candidate": control_candidate,
                "option_candidate": option_candidate,
            }
            if control_candidate or option_candidate:
                target_row["candidate"] = option_candidate or control_candidate
            targets.append(target_row)
        resolved_targets = [
            {
                "action": str(row.get("action") or "").strip(),
                "query": str(row.get("query") or "").strip(),
                "text": str(row.get("text") or "").strip(),
                "family": str(row.get("family") or "").strip(),
            }
            for row in targets
            if bool(row.get("satisfied", False))
        ]
        remaining_targets = [
            {
                "action": str(row.get("action") or "").strip(),
                "query": str(row.get("query") or "").strip(),
                "text": str(row.get("text") or "").strip(),
                "family": str(row.get("family") or "").strip(),
            }
            for row in targets
            if not bool(row.get("satisfied", False))
        ]
        visible_pending_targets = [
            {
                "action": str(row.get("action") or "").strip(),
                "query": str(row.get("query") or "").strip(),
                "text": str(row.get("text") or "").strip(),
                "family": str(row.get("family") or "").strip(),
            }
            for row in targets
            if bool(row.get("visible", False)) and not bool(row.get("satisfied", False))
        ]
        return {
            "requested_count": len(normalized_plan),
            "resolved_count": len(resolved_targets),
            "remaining_count": len(remaining_targets),
            "visible_pending_count": len(visible_pending_targets),
            "targets": targets,
            "resolved_targets": resolved_targets,
            "remaining_targets": remaining_targets,
            "visible_pending_targets": visible_pending_targets,
        }

    @classmethod
    def _rank_form_target_tabs(
        cls,
        *,
        page_state: Dict[str, Any],
        remaining_targets: Any,
        visited_tabs: Any,
    ) -> List[Dict[str, Any]]:
        tab_rows = [
            dict(row)
            for row in page_state.get("available_tabs", [])
            if isinstance(row, dict)
        ]
        if len(tab_rows) <= 1:
            return []
        selected_tab = cls._normalize_probe_text(page_state.get("selected_tab", ""))
        visited = {
            cls._normalize_probe_text(row.get("name", "") if isinstance(row, dict) else row)
            for row in (visited_tabs if isinstance(visited_tabs, (list, set, tuple)) else [visited_tabs])
            if cls._normalize_probe_text(row.get("name", "") if isinstance(row, dict) else row)
        }
        normalized_targets = cls._normalize_form_target_plan(remaining_targets)
        ranked: List[tuple[int, float, int, str, Dict[str, Any]]] = []
        for index, row in enumerate(tab_rows):
            tab_summary = dict(row)
            tab_name = str(tab_summary.get("name", "") or "").strip()
            normalized_tab_name = cls._normalize_probe_text(tab_name)
            if not normalized_tab_name or normalized_tab_name == selected_tab or normalized_tab_name in visited:
                continue
            if cls._coerce_surface_bool(tab_summary.get("enabled")) is False:
                continue
            if cls._coerce_surface_bool(tab_summary.get("visible")) is False:
                continue
            best_match = 0.0
            matched_targets: List[Dict[str, Any]] = []
            for target in normalized_targets:
                if not isinstance(target, dict):
                    continue
                query_text = str(target.get("query", "") or "").strip()
                text_value = str(target.get("text", "") or "").strip()
                match_score = max(
                    cls._element_query_match_score({"name": tab_name}, query_text),
                    cls._element_query_match_score({"name": tab_name}, text_value) if text_value else 0.0,
                )
                if match_score <= 0:
                    continue
                best_match = max(best_match, match_score)
                matched_targets.append(
                    {
                        "action": str(target.get("action", "") or "").strip(),
                        "query": query_text,
                        "text": text_value,
                        "family": str(target.get("family", "") or "").strip(),
                        "match_score": round(match_score, 6),
                    }
                )
            ranked.append(
                (
                    0 if best_match > 0 else 1,
                    -best_match,
                    index,
                    normalized_tab_name,
                    {
                        **tab_summary,
                        "match_score": round(best_match, 6),
                        "matched_targets": matched_targets[:6],
                        "fallback_candidate": best_match <= 0,
                    },
                )
            )
        ranked.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
        return [dict(row) for _, _, _, _, row in ranked]

    @classmethod
    def _rank_form_navigation_targets(
        cls,
        *,
        page_state: Dict[str, Any],
        remaining_targets: Any,
        visited_targets: Any,
    ) -> List[Dict[str, Any]]:
        candidate_rows = [
            dict(row)
            for row in page_state.get("available_navigation_targets", [])
            if isinstance(row, dict)
        ]
        if not candidate_rows:
            return []
        selected_target = cls._normalize_probe_text(page_state.get("selected_navigation_target", ""))
        visited = {
            cls._normalize_probe_text(row.get("name", "") if isinstance(row, dict) else row)
            for row in (visited_targets if isinstance(visited_targets, (list, set, tuple)) else [visited_targets])
            if cls._normalize_probe_text(row.get("name", "") if isinstance(row, dict) else row)
        }
        normalized_targets = cls._normalize_form_target_plan(remaining_targets)
        if not normalized_targets:
            return []
        action_priority = {
            "select_sidebar_item": 0,
            "select_tree_item": 1,
            "select_list_item": 2,
            "select_table_row": 3,
        }
        ranked: List[tuple[int, float, int, int, str, Dict[str, Any]]] = []
        for index, row in enumerate(candidate_rows):
            candidate_name = str(row.get("name", "") or "").strip()
            navigation_action = str(row.get("navigation_action", "") or "").strip().lower()
            normalized_candidate_name = cls._normalize_probe_text(candidate_name)
            if not candidate_name or not navigation_action:
                continue
            if normalized_candidate_name == selected_target or normalized_candidate_name in visited:
                continue
            if cls._coerce_surface_bool(row.get("enabled")) is False:
                continue
            if cls._coerce_surface_bool(row.get("visible")) is False:
                continue
            best_match = 0.0
            matched_targets: List[Dict[str, Any]] = []
            for target in normalized_targets:
                if not isinstance(target, dict):
                    continue
                query_text = str(target.get("query", "") or "").strip()
                text_value = str(target.get("text", "") or "").strip()
                match_score = max(
                    cls._element_query_match_score({"name": candidate_name}, query_text),
                    cls._element_query_match_score({"name": candidate_name}, text_value) if text_value else 0.0,
                )
                if match_score <= 0:
                    continue
                best_match = max(best_match, match_score)
                matched_targets.append(
                    {
                        "action": str(target.get("action", "") or "").strip(),
                        "query": query_text,
                        "text": text_value,
                        "family": str(target.get("family", "") or "").strip(),
                        "match_score": round(match_score, 6),
                    }
                )
            if best_match <= 0:
                continue
            ranked.append(
                (
                    action_priority.get(navigation_action, 9),
                    -best_match,
                    0 if cls._coerce_surface_bool(row.get("selected")) is not True else 1,
                    index,
                    normalized_candidate_name,
                    {
                        **row,
                        "match_score": round(best_match, 6),
                        "matched_targets": matched_targets[:6],
                    },
                )
            )
        ranked.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))
        return [dict(row) for _, _, _, _, _, row in ranked]

    @classmethod
    def _rank_form_drilldown_targets(
        cls,
        *,
        page_state: Dict[str, Any],
        remaining_targets: Any,
        visited_targets: Any,
    ) -> List[Dict[str, Any]]:
        candidate_rows = [
            dict(row)
            for row in page_state.get("available_drilldown_targets", [])
            if isinstance(row, dict)
        ]
        if not candidate_rows:
            return []
        visited = {
            cls._normalize_probe_text(row.get("name", "") if isinstance(row, dict) else row)
            for row in (visited_targets if isinstance(visited_targets, (list, set, tuple)) else [visited_targets])
            if cls._normalize_probe_text(row.get("name", "") if isinstance(row, dict) else row)
        }
        normalized_targets = cls._normalize_form_target_plan(remaining_targets)
        fallback_markers = (
            "advanced",
            "more",
            "additional",
            "details",
            "settings",
            "options",
            "properties",
            "configure",
            "manage",
            "related",
            "adapter",
        )
        control_priority = {
            "hyperlink": 0,
            "button": 1,
            "splitbutton": 2,
            "listitem": 3,
            "menuitem": 4,
            "treeitem": 5,
        }
        ranked: List[tuple[int, int, float, int, str, Dict[str, Any]]] = []
        for index, row in enumerate(candidate_rows):
            candidate_name = str(row.get("name", "") or "").strip()
            normalized_candidate_name = cls._normalize_probe_text(candidate_name)
            candidate_text = cls._normalize_probe_text(
                " ".join(
                    value
                    for value in (
                        candidate_name,
                        row.get("state_text", ""),
                        row.get("automation_id", ""),
                        row.get("class_name", ""),
                    )
                    if str(value or "").strip()
                )
            )
            control_type = cls._normalize_probe_text(row.get("control_type", ""))
            if not candidate_name or normalized_candidate_name in visited:
                continue
            if cls._coerce_surface_bool(row.get("enabled")) is False:
                continue
            if cls._coerce_surface_bool(row.get("visible")) is False:
                continue
            best_match = 0.0
            matched_targets: List[Dict[str, Any]] = []
            for target in normalized_targets:
                if not isinstance(target, dict):
                    continue
                query_text = str(target.get("query", "") or "").strip()
                text_value = str(target.get("text", "") or "").strip()
                match_score = max(
                    cls._element_query_match_score({"name": candidate_name, "state_text": candidate_text}, query_text),
                    cls._element_query_match_score({"name": candidate_name, "state_text": candidate_text}, text_value) if text_value else 0.0,
                )
                if match_score <= 0:
                    continue
                best_match = max(best_match, match_score)
                matched_targets.append(
                    {
                        "action": str(target.get("action", "") or "").strip(),
                        "query": query_text,
                        "text": text_value,
                        "family": str(target.get("family", "") or "").strip(),
                        "match_score": round(match_score, 6),
                    }
                )
            fallback_candidate = bool(best_match <= 0 and any(marker in candidate_text for marker in fallback_markers))
            if best_match <= 0 and not fallback_candidate:
                continue
            ranked.append(
                (
                    0 if best_match > 0 else 1,
                    control_priority.get(control_type, 9),
                    -best_match,
                    index,
                    normalized_candidate_name,
                    {
                        **row,
                        "match_score": round(best_match, 6),
                        "matched_targets": matched_targets[:6],
                        "fallback_candidate": fallback_candidate,
                    },
                )
            )
        ranked.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))
        return [dict(row) for _, _, _, _, _, row in ranked]

    @classmethod
    def _rank_form_expandable_groups(
        cls,
        *,
        page_state: Dict[str, Any],
        remaining_targets: Any,
        visited_groups: Any,
    ) -> List[Dict[str, Any]]:
        candidate_rows = [
            dict(row)
            for row in page_state.get("available_expandable_groups", [])
            if isinstance(row, dict)
        ]
        if not candidate_rows:
            return []
        visited = {
            cls._normalize_probe_text(row.get("name", "") if isinstance(row, dict) else row)
            for row in (visited_groups if isinstance(visited_groups, (list, set, tuple)) else [visited_groups])
            if cls._normalize_probe_text(row.get("name", "") if isinstance(row, dict) else row)
        }
        normalized_targets = cls._normalize_form_target_plan(remaining_targets)
        fallback_markers = (
            "advanced",
            "more",
            "additional",
            "details",
            "extra",
            "optional",
            "show",
            "expand",
            "collapse",
        )
        action_priority = {
            "expand_tree_item": 0,
            "expand_group": 1,
        }
        ranked: List[tuple[int, int, float, int, str, Dict[str, Any]]] = []
        for index, row in enumerate(candidate_rows):
            candidate_name = str(row.get("name", "") or "").strip()
            normalized_candidate_name = cls._normalize_probe_text(candidate_name)
            expand_action = str(row.get("expand_action", "") or "").strip().lower()
            candidate_text = cls._normalize_probe_text(
                " ".join(
                    value
                    for value in (
                        candidate_name,
                        row.get("state_text", ""),
                        row.get("automation_id", ""),
                        row.get("class_name", ""),
                    )
                    if str(value or "").strip()
                )
            )
            if not candidate_name or not expand_action:
                continue
            if normalized_candidate_name in visited:
                continue
            if cls._coerce_surface_bool(row.get("enabled")) is False:
                continue
            if cls._coerce_surface_bool(row.get("visible")) is False:
                continue
            if cls._coerce_surface_bool(row.get("expanded")) is True:
                continue
            best_match = 0.0
            matched_targets: List[Dict[str, Any]] = []
            for target in normalized_targets:
                if not isinstance(target, dict):
                    continue
                query_text = str(target.get("query", "") or "").strip()
                text_value = str(target.get("text", "") or "").strip()
                match_score = max(
                    cls._element_query_match_score({"name": candidate_name, "state_text": candidate_text}, query_text),
                    cls._element_query_match_score({"name": candidate_name, "state_text": candidate_text}, text_value) if text_value else 0.0,
                )
                if match_score <= 0:
                    continue
                best_match = max(best_match, match_score)
                matched_targets.append(
                    {
                        "action": str(target.get("action", "") or "").strip(),
                        "query": query_text,
                        "text": text_value,
                        "family": str(target.get("family", "") or "").strip(),
                        "match_score": round(match_score, 6),
                    }
                )
            fallback_candidate = bool(
                best_match <= 0
                and (
                    expand_action == "expand_tree_item"
                    or any(marker in candidate_text for marker in fallback_markers)
                )
            )
            if best_match <= 0 and not fallback_candidate:
                continue
            ranked.append(
                (
                    0 if best_match > 0 else 1,
                    action_priority.get(expand_action, 9),
                    -best_match,
                    index,
                    normalized_candidate_name,
                    {
                        **row,
                        "match_score": round(best_match, 6),
                        "matched_targets": matched_targets[:6],
                        "fallback_candidate": fallback_candidate,
                    },
                )
            )
        ranked.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))
        return [dict(row) for _, _, _, _, _, row in ranked]

    @classmethod
    def _related_target_elements(cls, *, elements: Any, target: Any, limit: int = 12) -> List[Dict[str, Any]]:
        rows = [dict(row) for row in elements if isinstance(row, dict)] if isinstance(elements, list) else []
        target_row = dict(target) if isinstance(target, dict) else {}
        target_id = str(target_row.get("element_id", "") or "").strip()
        parent_id = str(target_row.get("parent_id", "") or "").strip()
        if not target_id and not parent_id:
            return []
        bounded = max(1, min(int(limit or 12), 24))
        candidates: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            row_id = str(row.get("element_id", "") or "").strip()
            row_parent_id = str(row.get("parent_id", "") or "").strip()
            if row_id and row_id == target_id:
                continue
            related = False
            if parent_id and row_parent_id and row_parent_id == parent_id:
                related = True
            elif target_id and row_parent_id and row_parent_id == target_id:
                related = True
            elif target_id and row_id and parent_id and row_id == parent_id:
                related = True
            if not related:
                continue
            key = cls._element_identity_key(row)
            if not key or key in seen:
                continue
            seen.add(key)
            candidates.append(cls._element_state_summary(row))
            if len(candidates) >= bounded:
                break
        return candidates

    @classmethod
    def _target_group_state(
        cls,
        *,
        target: Any,
        related_candidates: Any,
        safety_signals: Any,
    ) -> Dict[str, Any]:
        target_row = dict(target) if isinstance(target, dict) else {}
        related_rows = [dict(row) for row in related_candidates if isinstance(row, dict)] if isinstance(related_candidates, list) else []
        safety_payload = dict(safety_signals) if isinstance(safety_signals, dict) else {}
        option_rows: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in ([target_row] if target_row else []) + related_rows:
            key = cls._element_identity_key(row)
            if not key or key in seen:
                continue
            seen.add(key)
            option_rows.append(row)
        if not option_rows:
            dialog_targets = safety_payload.get("dialog_button_targets", [])
            option_rows = [dict(row) for row in dialog_targets if isinstance(row, dict)]
            if not option_rows:
                return {}
        option_types = {cls._normalize_probe_text(row.get("control_type", "")) for row in option_rows}
        target_type = cls._normalize_probe_text(target_row.get("control_type", ""))
        group_role = "generic_options"
        if "combobox" in option_types or target_type == "combobox":
            group_role = "dropdown_options"
        elif "radiobutton" in option_types or target_type == "radiobutton":
            group_role = "radio_group"
        elif "tabitem" in option_types or target_type == "tabitem":
            group_role = "tab_group"
        elif "treeitem" in option_types or target_type == "treeitem":
            group_role = "tree_group"
        elif option_types.intersection({"dataitem", "row"}):
            group_role = "table_rows"
        elif "listitem" in option_types or target_type == "listitem":
            group_role = "list_group"
        elif option_types.intersection({"button", "splitbutton"}):
            group_role = "wizard_actions" if bool(safety_payload.get("wizard_surface_visible", False)) else "dialog_actions"
        filtered_option_rows = list(option_rows)
        if group_role == "dropdown_options":
            dropdown_specific = {"combobox", "listitem", "menuitem", "text"}
            focused_rows = [
                row
                for row in option_rows
                if cls._normalize_probe_text(row.get("control_type", "")) in dropdown_specific
            ]
            if focused_rows:
                filtered_option_rows = focused_rows
        if group_role in {"dialog_actions", "wizard_actions"}:
            button_rows = [
                row
                for row in filtered_option_rows
                if cls._normalize_probe_text(row.get("control_type", "")) in {"button", "splitbutton"}
            ]
            if button_rows:
                filtered_option_rows = button_rows
        selected_options = [
            str(row.get("name", "") or "").strip()
            for row in filtered_option_rows
            if cls._coerce_surface_bool(row.get("selected")) is True or cls._coerce_surface_bool(row.get("checked")) is True
        ]
        checked_options = [
            str(row.get("name", "") or "").strip()
            for row in filtered_option_rows
            if cls._coerce_surface_bool(row.get("checked")) is True
        ]
        enabled_options = [
            str(row.get("name", "") or "").strip()
            for row in filtered_option_rows
            if cls._coerce_surface_bool(row.get("enabled")) is not False
        ]
        visible_options = [
            str(row.get("name", "") or "").strip()
            for row in filtered_option_rows
            if cls._coerce_surface_bool(row.get("visible")) is not False
        ]
        safe_options = [str(item).strip() for item in safety_payload.get("safe_dialog_buttons", []) if str(item).strip()]
        destructive_options = [str(item).strip() for item in safety_payload.get("destructive_dialog_buttons", []) if str(item).strip()]
        return {
            "group_role": group_role,
            "target_label": str(target_row.get("name", "") or "").strip(),
            "target_control_type": str(target_row.get("control_type", "") or "").strip(),
            "option_count": len(filtered_option_rows),
            "options": filtered_option_rows[:12],
            "selected_options": selected_options[:6],
            "checked_options": checked_options[:6],
            "enabled_options": enabled_options[:8],
            "visible_options": visible_options[:8],
            "safe_options": safe_options[:6],
            "destructive_options": destructive_options[:6],
            "preferred_safe_option": str(safety_payload.get("preferred_dismiss_button", "") or "").strip(),
            "preferred_confirmation_option": str(safety_payload.get("preferred_confirmation_button", "") or "").strip(),
        }

    @classmethod
    def _control_inventory(cls, *, elements: Any) -> Dict[str, int]:
        rows = [dict(row) for row in elements if isinstance(row, dict)] if isinstance(elements, list) else []
        counts: Dict[str, int] = {}
        for row in rows:
            control_type = cls._normalize_probe_text(row.get("control_type", "")) or "unknown"
            counts[control_type] = int(counts.get(control_type, 0)) + 1
        return counts

    @classmethod
    def _element_identity_key(cls, row: Dict[str, Any]) -> str:
        explicit_id = str(row.get("element_id", "") or "").strip()
        if explicit_id:
            return explicit_id
        fallback_parts = [
            str(row.get("name", "") or "").strip(),
            str(row.get("automation_id", "") or "").strip(),
            str(row.get("control_type", "") or "").strip(),
            str(row.get("class_name", "") or "").strip(),
            str(row.get("left", "") or "").strip(),
            str(row.get("top", "") or "").strip(),
        ]
        return "|".join(part for part in fallback_parts if part)

    @staticmethod
    def _workflow_category_hints(definition: Dict[str, Any]) -> List[str]:
        rows: List[str] = []
        for field_name in ("category_hints", "supports_without_hotkey_categories", "supports_action_dispatch_categories"):
            raw = definition.get(field_name, [])
            values = raw if isinstance(raw, (list, tuple, set)) else []
            for value in values:
                clean = str(value or "").strip().lower()
                if clean and clean not in rows:
                    rows.append(clean)
        return rows

    def _workflow_followups(self, requested_action: str) -> List[str]:
        definition = self._workflow_definition(requested_action)
        rows: List[str] = []
        for value in definition.get("recommended_followups", []):
            clean = str(value or "").strip().lower()
            if clean and clean in WORKFLOW_ACTIONS and clean not in rows:
                rows.append(clean)
        return rows

    def _workflow_probe_queries(self, *, requested_action: str, args: Dict[str, Any], advice: Dict[str, Any]) -> List[Dict[str, Any]]:
        clean_action = str(requested_action or "").strip().lower()
        if clean_action not in WORKFLOW_ACTIONS:
            return []
        definition = self._workflow_definition(clean_action)
        app_profile = advice.get("app_profile", {}) if isinstance(advice.get("app_profile", {}), dict) else {}
        queries: List[Dict[str, Any]] = []

        def _append_probe(term: Any, *, source: str, control_type: str = "") -> None:
            clean_term = str(term or "").strip()
            normalized = self._normalize_probe_text(clean_term)
            if not normalized:
                return
            row: Dict[str, Any] = {"query": clean_term, "source": source}
            if control_type:
                row["control_type"] = control_type
            queries.append(row)

        for term in definition.get("probe_terms", []):
            _append_probe(term, source="definition")
        verify_text = self._derive_verify_text(args=args, app_profile=app_profile)
        if verify_text:
            _append_probe(verify_text, source="verify_text")
        workflow_input = self._workflow_input_text(requested_action=clean_action, args=args)
        if workflow_input and self._normalize_probe_text(workflow_input) != self._normalize_probe_text(verify_text):
            _append_probe(workflow_input, source="workflow_input")
        if clean_action == "navigate":
            navigation_hint = self._navigation_verify_text(str(args.get("query", "") or "").strip())
            if navigation_hint and self._normalize_probe_text(navigation_hint) != self._normalize_probe_text(verify_text):
                _append_probe(navigation_hint, source="navigation_hint")

        deduped: List[Dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for row in queries:
            normalized_query = self._normalize_probe_text(row.get("query", ""))
            normalized_control = self._normalize_probe_text(row.get("control_type", ""))
            key = (normalized_query, normalized_control)
            if not normalized_query or key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped[:6]

    def _run_workflow_probes(
        self,
        *,
        action: str,
        args: Dict[str, Any],
        advice: Dict[str, Any],
        capabilities: Dict[str, Any],
    ) -> Dict[str, Any]:
        queries = self._workflow_probe_queries(requested_action=action, args=args, advice=advice)
        target_window = advice.get("target_window", {}) if isinstance(advice.get("target_window", {}), dict) else {}
        app_profile = advice.get("app_profile", {}) if isinstance(advice.get("app_profile", {}), dict) else {}
        resolved_window_title = str(
            target_window.get("title", "")
            or args.get("window_title", "")
            or app_profile.get("name", "")
            or args.get("app_name", "")
            or ""
        ).strip()
        accessibility_ready = bool(capabilities.get("accessibility", {}).get("available")) if isinstance(capabilities.get("accessibility", {}), dict) else False
        vision_ready = bool(capabilities.get("vision", {}).get("available")) if isinstance(capabilities.get("vision", {}), dict) else False
        if not queries:
            return {
                "status": "skipped",
                "matched": False,
                "queries": [],
                "matches": [],
                "sources": [],
                "window_title": resolved_window_title,
            }

        matches: List[Dict[str, Any]] = []
        sources: List[str] = []
        attempted = False

        for row in queries:
            term = str(row.get("query", "") or "").strip()
            control_type = str(row.get("control_type", "") or "").strip()
            source = str(row.get("source", "") or "").strip() or "definition"
            if accessibility_ready:
                attempted = True
                accessibility_payload: Dict[str, Any] = {
                    "query": term,
                    "max_results": 6,
                }
                if resolved_window_title:
                    accessibility_payload["window_title"] = resolved_window_title
                if control_type:
                    accessibility_payload["control_type"] = control_type
                accessibility_result = self._call("accessibility_find_element", accessibility_payload)
                items = accessibility_result.get("items", []) if isinstance(accessibility_result.get("items", []), list) else []
                count = self._to_int(accessibility_result.get("count"))
                accessibility_found = bool(
                    str(accessibility_result.get("status", "") or "").strip().lower() == "success"
                    and (bool(items) or bool(accessibility_result.get("found")) or (count is not None and count > 0))
                )
                if accessibility_found:
                    matches.append(
                        {
                            "query": term,
                            "control_type": control_type,
                            "source": source,
                            "match_source": "accessibility",
                            "count": count if count is not None else len(items),
                        }
                    )
                    sources.append("accessibility")
            if vision_ready:
                attempted = True
                vision_result = self._call("computer_assert_text_visible", {"text": term})
                if (
                    str(vision_result.get("status", "") or "").strip().lower() == "success"
                    and bool(vision_result.get("found"))
                ):
                    matches.append(
                        {
                            "query": term,
                            "control_type": control_type,
                            "source": source,
                            "match_source": "ocr",
                            "chars": self._to_int(vision_result.get("chars")),
                        }
                    )
                    sources.append("ocr")
        return {
            "status": "success" if matches else ("no_match" if attempted else "skipped"),
            "matched": bool(matches),
            "queries": queries,
            "matches": matches,
            "sources": self._dedupe_strings(sources),
            "window_title": resolved_window_title,
        }

    @staticmethod
    def _navigation_verify_text(destination: str) -> str:
        clean = str(destination or "").strip()
        if not clean:
            return ""
        if clean.startswith(("http://", "https://")):
            clean = re.sub(r"^https?://", "", clean, flags=re.IGNORECASE)
        clean = clean.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0].strip()
        return clean.rstrip(".,)")

    @staticmethod
    def _sanitize_payload_for_response(args: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in dict(args).items() if not str(key).startswith("_")}

    @staticmethod
    def _contains_text(haystack: str, needle: str) -> bool:
        clean_haystack = " ".join(str(haystack or "").strip().lower().split())
        clean_needle = " ".join(str(needle or "").strip().lower().split())
        if not clean_haystack or not clean_needle:
            return False
        return clean_needle in clean_haystack

    @staticmethod
    def _find_step_result(results: List[Dict[str, Any]], action: str) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        for row in reversed(results):
            if not isinstance(row, dict):
                continue
            if str(row.get("action", "") or "").strip().lower() == clean_action and isinstance(row.get("result", {}), dict):
                return row.get("result", {})
        return {}

    @staticmethod
    def _window_matches(window: Dict[str, Any], *, app_name: str, window_title: str) -> bool:
        title = str(window.get("title", "") or "").strip()
        exe_name = str(window.get("exe", "") or "").rsplit("\\", 1)[-1].rsplit("/", 1)[-1]
        return (
            DesktopActionRouter._text_match_score(title, window_title) > 0
            if window_title
            else False
        ) or (
            DesktopActionRouter._text_match_score(title, app_name) > 0
            if app_name
            else False
        ) or (
            DesktopActionRouter._text_match_score(exe_name, app_name) > 0
            if app_name
            else False
        )

    @staticmethod
    def _text_match_score(left: str, right: str) -> float:
        clean_left = str(left or "").strip().lower()
        clean_right = str(right or "").strip().lower()
        if not clean_left or not clean_right:
            return 0.0
        if clean_left == clean_right:
            return 1.0
        if clean_right in clean_left or clean_left in clean_right:
            return 0.82
        left_tokens = {token for token in re.split(r"[^a-z0-9]+", clean_left) if token}
        right_tokens = {token for token in re.split(r"[^a-z0-9]+", clean_right) if token}
        if not left_tokens or not right_tokens:
            return 0.0
        overlap = len(left_tokens.intersection(right_tokens))
        return overlap / max(1.0, len(right_tokens))

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _dedupe_strings(values: List[str]) -> List[str]:
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

    @staticmethod
    def _default_handlers() -> Dict[str, ActionHandler]:
        def _open_app(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _open_app as open_app_impl

            return open_app_impl(payload)

        def _list_windows(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _list_windows as list_windows_impl

            return list_windows_impl(payload)

        def _active_window(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _active_window as active_window_impl

            return active_window_impl(payload)

        def _focus_window(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _focus_window as focus_window_impl

            return focus_window_impl(payload)

        def _keyboard_type(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _keyboard_type as keyboard_type_impl

            return keyboard_type_impl(payload)

        def _keyboard_hotkey(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _keyboard_hotkey as keyboard_hotkey_impl

            return keyboard_hotkey_impl(payload)

        def _computer_click_target(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _computer_click_target as computer_click_target_impl

            return computer_click_target_impl(payload)

        def _computer_observe(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _computer_observe as computer_observe_impl

            return computer_observe_impl(payload)

        def _computer_assert_text_visible(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _computer_assert_text_visible as computer_assert_text_visible_impl

            return computer_assert_text_visible_impl(payload)

        def _computer_wait_for_text(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _computer_wait_for_text as computer_wait_for_text_impl

            return computer_wait_for_text_impl(payload)

        def _accessibility_list_elements(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _accessibility_list_elements as accessibility_list_elements_impl

            return accessibility_list_elements_impl(payload)

        def _accessibility_find_element(payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.route_handlers import _accessibility_find_element as accessibility_find_element_impl

            return accessibility_find_element_impl(payload)

        def _accessibility_status(_payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.accessibility_tools import AccessibilityTools

            return AccessibilityTools.health()

        def _vision_status(_payload: Dict[str, Any]) -> Dict[str, Any]:
            from backend.python.tools.vision_tools import VisionTools

            return VisionTools.health()

        return {
            "open_app": _open_app,
            "list_windows": _list_windows,
            "active_window": _active_window,
            "focus_window": _focus_window,
            "keyboard_type": _keyboard_type,
            "keyboard_hotkey": _keyboard_hotkey,
            "computer_click_target": _computer_click_target,
            "computer_observe": _computer_observe,
            "computer_assert_text_visible": _computer_assert_text_visible,
            "computer_wait_for_text": _computer_wait_for_text,
            "accessibility_list_elements": _accessibility_list_elements,
            "accessibility_find_element": _accessibility_find_element,
            "accessibility_status": _accessibility_status,
            "vision_status": _vision_status,
        }
