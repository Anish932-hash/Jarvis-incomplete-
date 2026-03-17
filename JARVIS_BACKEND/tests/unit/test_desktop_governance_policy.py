from __future__ import annotations

from backend.python.core.desktop_governance_policy import DesktopGovernancePolicyManager


def test_desktop_governance_policy_defaults_follow_profile(tmp_path) -> None:
    manager = DesktopGovernancePolicyManager(
        state_path=str(tmp_path / "desktop_governance.json"),
        policy_profile="power",
    )

    payload = manager.status()

    assert payload["status"] == "success"
    assert payload["policy_profile"] == "power"
    assert payload["allow_high_risk"] is True
    assert payload["allow_critical_risk"] is True
    assert payload["allow_desktop_approval_reuse"] is True
    assert payload["desktop_approval_reuse_window_s"] == 240
    assert payload["action_confirmation_reuse_window_s"] == 120


def test_desktop_governance_policy_custom_override_persists_and_resolves(tmp_path) -> None:
    path = tmp_path / "desktop_governance.json"
    manager = DesktopGovernancePolicyManager(
        state_path=str(path),
        policy_profile="balanced",
    )

    updated = manager.configure(
        allow_desktop_approval_reuse=False,
        action_confirmation_reuse_window_s=15,
    )

    assert updated["policy_profile"] == "custom"
    assert updated["allow_desktop_approval_reuse"] is False
    assert updated["action_confirmation_reuse_window_s"] == 15

    reloaded = DesktopGovernancePolicyManager(state_path=str(path))
    payload = reloaded.status()
    resolved = reloaded.resolve(policy_profile="power")

    assert payload["policy_profile"] == "custom"
    assert payload["allow_desktop_approval_reuse"] is False
    assert payload["action_confirmation_reuse_window_s"] == 15
    assert resolved["policy_profile"] == "power"
    assert resolved["allow_desktop_approval_reuse"] is True
    assert resolved["action_confirmation_reuse_window_s"] == 120
