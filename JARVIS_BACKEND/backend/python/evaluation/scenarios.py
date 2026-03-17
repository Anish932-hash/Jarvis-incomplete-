from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass(slots=True)
class Scenario:
    name: str
    user_text: str
    expected_actions: List[str]
    weight: float = 1.0
    strict_order: bool = True
    required_actions: List[str] = field(default_factory=list)
    category: str = "general"
    capabilities: List[str] = field(default_factory=list)
    risk_level: str = "standard"
    notes: str = ""
    pack: str = "desktop_core"
    platform: str = "windows"
    mission_family: str = "task"
    autonomy_tier: str = "assisted"
    apps: List[str] = field(default_factory=list)
    recovery_expected: bool = False
    native_hybrid_focus: bool = False
    tags: List[str] = field(default_factory=list)


def default_scenarios() -> List[Scenario]:
    return [
        Scenario(
            "open_notepad",
            "Open notepad",
            ["open_app", "tts_speak"],
            category="desktop_basics",
            capabilities=["launch", "desktop_control"],
            pack="desktop_core",
            mission_family="launch",
            autonomy_tier="assisted",
            apps=["notepad"],
            native_hybrid_focus=True,
            tags=["baseline", "launch"],
        ),
        Scenario(
            "open_url",
            "Open github.com",
            ["open_url"],
            category="browser_navigation",
            capabilities=["browser", "navigation"],
            pack="browser_productivity",
            mission_family="browser",
            autonomy_tier="assisted",
            apps=["browser"],
            tags=["browser", "navigation"],
        ),
        Scenario(
            "desktop_workflow_navigation",
            "Open chrome and navigate to openai.com",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="browser_navigation",
            capabilities=["desktop_workflow", "browser", "navigation"],
            notes="Validates natural-language routing into app-aware desktop workflows.",
            pack="browser_productivity",
            mission_family="workflow",
            autonomy_tier="bounded_autonomy",
            apps=["chrome"],
            native_hybrid_focus=True,
            tags=["browser", "workflow"],
        ),
        Scenario(
            "browser_tab_search",
            "Search tabs in chrome for openai docs",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="browser_navigation",
            capabilities=["browser", "tab_management", "desktop_workflow"],
            pack="browser_productivity",
            mission_family="workflow",
            autonomy_tier="bounded_autonomy",
            apps=["chrome"],
            tags=["browser", "tabs"],
        ),
        Scenario(
            "settings_toggle_apply",
            "Open settings and turn on bluetooth and apply settings",
            ["open_app", "desktop_interact", "desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="settings",
            capabilities=["settings_control", "form_mission", "switch_control"],
            risk_level="guarded",
            pack="settings_and_admin",
            mission_family="form",
            autonomy_tier="bounded_autonomy",
            apps=["settings"],
            native_hybrid_focus=True,
            tags=["settings", "toggle", "apply"],
        ),
        Scenario(
            "settings_multi_control_apply",
            "Open settings and turn on bluetooth and set brightness slider to 80 and apply settings",
            ["open_app", "desktop_interact", "desktop_interact", "desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="settings",
            capabilities=["settings_control", "form_mission", "switch_control", "value_control"],
            risk_level="guarded",
            pack="settings_and_admin",
            mission_family="form",
            autonomy_tier="autonomous",
            apps=["settings"],
            recovery_expected=True,
            native_hybrid_focus=True,
            tags=["settings", "multi_control", "apply"],
        ),
        Scenario(
            "explorer_rename_file",
            "Rename selected file to report-final.txt in explorer",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="file_manager",
            capabilities=["file_manager", "selection_control", "rename"],
            pack="desktop_core",
            mission_family="workflow",
            autonomy_tier="bounded_autonomy",
            apps=["explorer"],
            tags=["file_manager", "rename"],
        ),
        Scenario(
            "explorer_new_folder",
            "Create a new folder in explorer",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="file_manager",
            capabilities=["file_manager", "desktop_workflow", "creation"],
            pack="desktop_core",
            mission_family="workflow",
            autonomy_tier="bounded_autonomy",
            apps=["explorer"],
            tags=["file_manager", "creation"],
        ),
        Scenario(
            "vscode_terminal_command",
            "Run npm test in vscode terminal",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="editor_workflow",
            capabilities=["editor", "terminal", "command_execution"],
            pack="browser_productivity",
            mission_family="workflow",
            autonomy_tier="bounded_autonomy",
            apps=["vscode"],
            tags=["editor", "terminal"],
        ),
        Scenario(
            "vscode_quick_open",
            "Open settings.json with quick open in vscode",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="editor_workflow",
            capabilities=["editor", "quick_open", "desktop_workflow"],
            pack="browser_productivity",
            mission_family="workflow",
            autonomy_tier="bounded_autonomy",
            apps=["vscode"],
            tags=["editor", "quick_open"],
        ),
        Scenario(
            "outlook_reply",
            "Reply all in outlook",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="communication",
            capabilities=["mail", "desktop_workflow"],
            risk_level="guarded",
            pack="communication_and_productivity",
            mission_family="workflow",
            autonomy_tier="bounded_autonomy",
            apps=["outlook"],
            tags=["mail", "reply_all"],
        ),
        Scenario(
            "outlook_new_event",
            "Create a new calendar event in outlook",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="communication",
            capabilities=["calendar", "desktop_workflow"],
            risk_level="guarded",
            pack="communication_and_productivity",
            mission_family="workflow",
            autonomy_tier="bounded_autonomy",
            apps=["outlook"],
            tags=["calendar", "event"],
        ),
        Scenario(
            "unsupported_surface_exploration",
            "Explore surface for bluetooth in settings",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="unsupported_app",
            capabilities=["surface_exploration", "settings_control", "recovery"],
            risk_level="guarded",
            notes="Measures fallback exploration on semi-structured Windows surfaces.",
            pack="unsupported_and_recovery",
            mission_family="exploration",
            autonomy_tier="bounded_autonomy",
            apps=["settings"],
            recovery_expected=True,
            native_hybrid_focus=True,
            tags=["exploration", "recovery"],
        ),
        Scenario(
            "unsupported_child_dialog_chain",
            "Explore surface for add bluetooth device in settings and continue through the child dialog chain",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="unsupported_app",
            capabilities=["surface_exploration", "child_window_adoption", "recovery"],
            risk_level="guarded",
            pack="unsupported_and_recovery",
            mission_family="exploration",
            autonomy_tier="autonomous",
            apps=["settings"],
            recovery_expected=True,
            native_hybrid_focus=True,
            tags=["exploration", "child_window", "dialog_chain"],
        ),
        Scenario(
            "installer_continue_flow",
            "Continue through installer",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="installer",
            capabilities=["wizard_mission", "desktop_recovery"],
            risk_level="high",
            pack="installer_and_governance",
            mission_family="wizard",
            autonomy_tier="autonomous",
            apps=["installer"],
            recovery_expected=True,
            native_hybrid_focus=True,
            tags=["installer", "recovery"],
        ),
        Scenario(
            "installer_resume_after_prompt",
            "Resume the blocked installer after approval is completed",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="installer",
            capabilities=["wizard_mission", "desktop_recovery", "governance"],
            risk_level="high",
            pack="installer_and_governance",
            mission_family="recovery",
            autonomy_tier="autonomous",
            apps=["installer"],
            recovery_expected=True,
            native_hybrid_focus=True,
            tags=["installer", "resume", "approval"],
        ),
        Scenario(
            "security_status",
            "Check defender status",
            ["defender_status"],
            category="system_ops",
            capabilities=["system_status", "security"],
            risk_level="guarded",
            pack="settings_and_admin",
            mission_family="read_only",
            autonomy_tier="assisted",
            apps=["defender"],
            tags=["security", "status"],
        ),
        Scenario(
            "task_manager_review_kill",
            "Open task manager and review ending a task",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="system_ops",
            capabilities=["task_manager", "governance", "desktop_workflow"],
            risk_level="critical",
            pack="settings_and_admin",
            mission_family="review",
            autonomy_tier="guardrailed",
            apps=["task_manager"],
            recovery_expected=True,
            native_hybrid_focus=True,
            tags=["task_manager", "destructive_review"],
        ),
        Scenario(
            "system_snapshot",
            "Show cpu and ram usage",
            ["system_snapshot"],
            category="system_ops",
            capabilities=["system_status"],
            pack="desktop_core",
            mission_family="read_only",
            autonomy_tier="assisted",
            apps=["system"],
            tags=["system", "snapshot"],
        ),
        Scenario(
            "media_search",
            "Search lo-fi music on youtube",
            ["media_search"],
            category="browser_media",
            capabilities=["browser", "media"],
            pack="browser_productivity",
            mission_family="workflow",
            autonomy_tier="assisted",
            apps=["browser"],
            tags=["media", "browser"],
        ),
        Scenario(
            "time_query",
            "What is the time in UTC",
            ["time_now"],
            category="utility",
            capabilities=["time", "utility"],
            pack="desktop_core",
            mission_family="read_only",
            autonomy_tier="assisted",
            apps=["system"],
            tags=["utility", "time"],
        ),
        Scenario(
            "fallback_speak",
            "Hello there",
            ["tts_speak"],
            category="conversation",
            capabilities=["speech", "fallback_response"],
            pack="desktop_core",
            mission_family="response",
            autonomy_tier="assisted",
            apps=["speech"],
            tags=["fallback", "speech"],
        ),
    ]


def scenario_catalog(
    *,
    scenarios: List[Scenario] | None = None,
    pack: str = "",
    category: str = "",
    capability: str = "",
    risk_level: str = "",
    autonomy_tier: str = "",
    mission_family: str = "",
    app: str = "",
    limit: int = 200,
) -> List[Scenario]:
    rows = list(scenarios or default_scenarios())
    clean_pack = " ".join(str(pack or "").strip().lower().split())
    clean_category = " ".join(str(category or "").strip().lower().split())
    clean_capability = " ".join(str(capability or "").strip().lower().split())
    clean_risk = " ".join(str(risk_level or "").strip().lower().split())
    clean_autonomy = " ".join(str(autonomy_tier or "").strip().lower().split())
    clean_mission = " ".join(str(mission_family or "").strip().lower().split())
    clean_app = " ".join(str(app or "").strip().lower().split())
    if clean_pack:
        rows = [row for row in rows if " ".join(str(row.pack or "").strip().lower().split()) == clean_pack]
    if clean_category:
        rows = [row for row in rows if " ".join(str(row.category or "").strip().lower().split()) == clean_category]
    if clean_capability:
        rows = [
            row
            for row in rows
            if any(" ".join(str(item or "").strip().lower().split()) == clean_capability for item in row.capabilities)
        ]
    if clean_risk:
        rows = [row for row in rows if " ".join(str(row.risk_level or "").strip().lower().split()) == clean_risk]
    if clean_autonomy:
        rows = [row for row in rows if " ".join(str(row.autonomy_tier or "").strip().lower().split()) == clean_autonomy]
    if clean_mission:
        rows = [row for row in rows if " ".join(str(row.mission_family or "").strip().lower().split()) == clean_mission]
    if clean_app:
        rows = [
            row
            for row in rows
            if any(" ".join(str(item or "").strip().lower().split()) == clean_app for item in row.apps)
        ]
    bounded = max(1, min(int(limit or 200), 5000))
    return rows[:bounded]
