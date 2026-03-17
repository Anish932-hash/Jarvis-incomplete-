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


def test_desktop_workflow_memory_learns_skill_profile_and_summary(tmp_path: Path) -> None:
    memory = DesktopWorkflowMemory(store_path=str(tmp_path / "desktop_workflow_memory.json"))
    for _ in range(2):
        memory.record_outcome(
            action="command",
            args={
                "action": "command",
                "app_name": "vscode",
                "text": "Preferences: Open Settings (JSON)",
                "focus_first": False,
                "ensure_app_launch": True,
                "target_mode": "ocr",
                "verify_mode": "ocr",
                "retry_on_verification_failure": True,
                "max_strategy_attempts": 3,
            },
            app_profile={"profile_id": "microsoft-visual-studio-code", "category": "editor"},
            strategy={"strategy_id": "workflow_retry_2", "payload_overrides": {"keys": ["f1"]}},
            attempt={"status": "success", "attempt": 2, "verification": {"enabled": True, "verified": True}},
            advice={
                "route_mode": "workflow_command",
                "surface_snapshot": {
                    "surface_intelligence": {
                        "surface_role": "command_palette",
                        "interaction_mode": "keyboard",
                    }
                },
            },
        )

    skill = memory.skill_profile(
        action="command",
        args={"action": "command", "app_name": "vscode", "text": "Preferences: Open Settings (JSON)"},
        app_profile={"profile_id": "microsoft-visual-studio-code", "category": "editor"},
    )
    snapshot = memory.snapshot(action="command", app_name="vscode", limit=10)

    assert skill["status"] == "learned"
    assert skill["scope"] == "exact"
    assert skill["should_apply"] is True
    assert skill["recommended_overrides"]["focus_first"] is False
    assert skill["recommended_overrides"]["ensure_app_launch"] is True
    assert skill["recommended_overrides"]["target_mode"] == "ocr"
    assert skill["preferred_route_mode"] == "workflow_command"
    assert snapshot["items"][0]["skill_profile"]["should_apply"] is True
    assert snapshot["items"][0]["metrics"]["verified_successes"] == 2
    assert snapshot["summary"]["action_counts"]["command"] == 1
    assert snapshot["summary"]["route_mode_counts"]["workflow_command"] == 2
    assert snapshot["summary"]["surface_role_counts"]["command_palette"] == 2
