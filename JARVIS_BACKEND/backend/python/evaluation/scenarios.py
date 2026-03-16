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


def default_scenarios() -> List[Scenario]:
    return [
        Scenario(
            "open_notepad",
            "Open notepad",
            ["open_app", "tts_speak"],
            category="desktop_basics",
            capabilities=["launch", "desktop_control"],
        ),
        Scenario(
            "security_status",
            "Check defender status",
            ["defender_status"],
            category="system_ops",
            capabilities=["system_status", "security"],
            risk_level="guarded",
        ),
        Scenario(
            "media_search",
            "Search lo-fi music on youtube",
            ["media_search"],
            category="browser_media",
            capabilities=["browser", "media"],
        ),
        Scenario(
            "system_snapshot",
            "Show cpu and ram usage",
            ["system_snapshot"],
            category="system_ops",
            capabilities=["system_status"],
        ),
        Scenario(
            "open_url",
            "Open github.com",
            ["open_url"],
            category="browser_navigation",
            capabilities=["browser", "navigation"],
        ),
        Scenario(
            "time_query",
            "What is the time in UTC",
            ["time_now"],
            category="utility",
            capabilities=["time", "utility"],
        ),
        Scenario(
            "fallback_speak",
            "Hello there",
            ["tts_speak"],
            category="conversation",
            capabilities=["speech", "fallback_response"],
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
        ),
        Scenario(
            "explorer_rename_file",
            "Rename selected file to report-final.txt in explorer",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="file_manager",
            capabilities=["file_manager", "selection_control", "rename"],
        ),
        Scenario(
            "vscode_terminal_command",
            "Run npm test in vscode terminal",
            ["desktop_interact"],
            strict_order=False,
            required_actions=["desktop_interact"],
            category="editor_workflow",
            capabilities=["editor", "terminal", "command_execution"],
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
        ),
    ]
