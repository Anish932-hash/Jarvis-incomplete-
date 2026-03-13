from __future__ import annotations

from pathlib import Path

from backend.python.core.desktop_workflow_memory import DesktopWorkflowMemory


def test_desktop_workflow_memory_snapshot_filters_by_action_and_app(tmp_path: Path) -> None:
    memory = DesktopWorkflowMemory(store_path=str(tmp_path / "desktop_workflow_memory.json"))
    memory.record_outcome(
        action="command",
        args={"action": "command", "app_name": "vscode", "text": "Preferences: Open Settings (JSON)"},
        app_profile={"profile_id": "microsoft-visual-studio-code"},
        strategy={"strategy_id": "workflow_retry_2", "payload_overrides": {"keys": ["f1"]}},
        attempt={"status": "success", "verification": {"verified": True}},
    )
    memory.record_outcome(
        action="terminal_command",
        args={"action": "terminal_command", "app_name": "powershell", "text": "npm test"},
        app_profile={"profile_id": "powershell"},
        strategy={"strategy_id": "primary", "payload_overrides": {}},
        attempt={"status": "success", "verification": {"verified": True}},
    )

    filtered = memory.snapshot(action="command", app_name="vscode", limit=10)

    assert filtered["status"] == "success"
    assert filtered["count"] == 1
    assert filtered["items"][0]["profile_id"] == "microsoft-visual-studio-code"
    assert filtered["filters"]["action"] == "command"


def test_desktop_workflow_memory_reset_can_target_profile_id(tmp_path: Path) -> None:
    memory_path = Path(tmp_path) / "desktop_workflow_memory.json"
    memory = DesktopWorkflowMemory(store_path=str(memory_path))
    memory.record_outcome(
        action="command",
        args={"action": "command", "app_name": "vscode", "text": "Preferences: Open Settings (JSON)"},
        app_profile={"profile_id": "microsoft-visual-studio-code"},
        strategy={"strategy_id": "workflow_retry_2", "payload_overrides": {"keys": ["f1"]}},
        attempt={"status": "success", "verification": {"verified": True}},
    )
    memory.record_outcome(
        action="terminal_command",
        args={"action": "terminal_command", "app_name": "powershell", "text": "npm test"},
        app_profile={"profile_id": "powershell"},
        strategy={"strategy_id": "primary", "payload_overrides": {}},
        attempt={"status": "success", "verification": {"verified": True}},
    )

    reset = memory.reset(profile_id="microsoft-visual-studio-code")
    remaining = memory.snapshot(limit=10)

    assert reset["status"] == "success"
    assert reset["removed"] == 1
    assert remaining["count"] == 1
    assert remaining["items"][0]["profile_id"] == "powershell"
