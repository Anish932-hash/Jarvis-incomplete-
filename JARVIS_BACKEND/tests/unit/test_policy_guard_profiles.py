from __future__ import annotations

import json

from backend.python.core.contracts import ActionRequest
from backend.python.policies.policy_guard import PolicyGuard


def _guard(tmp_path, payload: dict) -> PolicyGuard:
    permissions_path = tmp_path / "permissions.json"
    permissions_path.write_text(json.dumps(payload), encoding="utf-8")
    guard = PolicyGuard(permissions_path=str(permissions_path))
    return guard


def test_source_and_profile_deny_rules_apply(tmp_path) -> None:
    guard = _guard(
        tmp_path,
        {
            "allowed_actions": ["open_app", "write_file"],
            "source_overrides": {"desktop-ui": {"deny": ["open_app"]}},
            "profiles": {"strict": {"allow": ["open_app"], "deny": ["write_file"]}},
        },
    )
    guard.set_runtime_actions({"open_app", "write_file"})

    source_denied, message = guard.authorize(ActionRequest(action="open_app", source="desktop-ui"))
    assert source_denied is False
    assert "denied for source" in message

    profile_denied, message = guard.authorize(
        ActionRequest(action="write_file", source="user", metadata={"policy_profile": "strict"})
    )
    assert profile_denied is False
    assert "denied for policy profile" in message


def test_profile_high_risk_requires_explicit_allow(tmp_path) -> None:
    guard = _guard(
        tmp_path,
        {
            "allowed_actions": ["terminate_process"],
            "profiles": {
                "strict": {"allow": ["terminate_process"]},
                "ops": {"allow": ["terminate_process"], "allow_high_risk": True},
            },
        },
    )
    guard.set_runtime_actions({"terminate_process"})

    blocked, message = guard.authorize(
        ActionRequest(action="terminate_process", source="desktop-ui", metadata={"policy_profile": "strict"})
    )
    assert blocked is False
    assert "allow_high_risk" in message

    allowed, message = guard.authorize(
        ActionRequest(action="terminate_process", source="desktop-ui", metadata={"policy_profile": "ops"})
    )
    assert allowed is True
    assert message == "Allowed"


def test_source_allow_list_scopes_non_safe_actions(tmp_path) -> None:
    guard = _guard(
        tmp_path,
        {
            "allowed_actions": ["open_app", "write_file"],
            "source_overrides": {"trusted-scheduler": {"allow": ["open_app"]}},
        },
    )
    guard.set_runtime_actions({"open_app", "write_file"})

    denied, message = guard.authorize(ActionRequest(action="write_file", source="trusted-scheduler"))
    assert denied is False
    assert "not allowed for source" in message

    allowed, message = guard.authorize(ActionRequest(action="open_app", source="trusted-scheduler"))
    assert allowed is True
    assert message == "Allowed"


def test_default_profile_for_source_applies_when_metadata_missing(tmp_path) -> None:
    guard = _guard(
        tmp_path,
        {
            "allowed_actions": ["terminate_process"],
            "profiles": {
                "automation_safe": {"allow": ["terminate_process"], "allow_high_risk": False},
                "automation_power": {"allow": ["terminate_process"], "allow_high_risk": True},
            },
            "default_profiles": {"desktop-trigger": "automation_safe"},
        },
    )
    guard.set_runtime_actions({"terminate_process"})

    blocked, message = guard.authorize(ActionRequest(action="terminate_process", source="desktop-trigger"))
    assert blocked is False
    assert "allow_high_risk" in message

    allowed, message = guard.authorize(
        ActionRequest(
            action="terminate_process",
            source="desktop-trigger",
            metadata={"policy_profile": "automation_power"},
        )
    )
    assert allowed is True
    assert message == "Allowed"


def test_decorate_metadata_and_profile_catalog(tmp_path) -> None:
    guard = _guard(
        tmp_path,
        {
            "profiles": {
                "interactive": {
                    "allow": ["time_now"],
                    "default_max_runtime_s": 210,
                    "default_max_steps": 18,
                }
            },
            "default_profile": "interactive",
            "default_profiles": {"desktop-schedule": "interactive"},
        },
    )

    enriched = guard.decorate_metadata_with_defaults("desktop-schedule", metadata={})
    assert enriched["policy_profile"] == "interactive"
    assert enriched["max_runtime_s"] == 210
    assert enriched["max_steps"] == 18

    catalog = guard.list_profiles()
    assert catalog["count"] == 1
    assert catalog["default_profile"] == "interactive"
    assert catalog["source_defaults"]["desktop-schedule"] == "interactive"
    assert catalog["items"][0]["default_max_runtime_s"] == 210
    assert catalog["items"][0]["default_max_steps"] == 18


def test_decorate_metadata_keeps_explicit_budget(tmp_path) -> None:
    guard = _guard(
        tmp_path,
        {
            "profiles": {
                "interactive": {
                    "allow": ["time_now"],
                    "default_max_runtime_s": 200,
                    "default_max_steps": 16,
                }
            },
            "default_profile": "interactive",
        },
    )

    enriched = guard.decorate_metadata_with_defaults(
        "desktop-ui",
        metadata={"max_runtime_s": 90, "max_steps": 7},
    )
    assert enriched["max_runtime_s"] == 90
    assert enriched["max_steps"] == 7


def test_dynamic_guardrail_blocks_unstable_high_risk_action(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_POLICY_GUARDRAILS_STORE", str(tmp_path / "guardrails.json"))
    guard = _guard(
        tmp_path,
        {
            "allowed_actions": ["terminate_process"],
            "profiles": {
                "ops": {"allow": ["terminate_process"], "allow_high_risk": True},
            },
        },
    )
    guard.set_runtime_actions({"terminate_process"})
    for _ in range(14):
        guard.record_action_outcome(
            action="terminate_process",
            status="failed",
            source="desktop-ui",
            error="request timed out",
        )

    blocked, message = guard.authorize(
        ActionRequest(
            action="terminate_process",
            source="desktop-ui",
            metadata={"policy_profile": "ops"},
        )
    )
    assert blocked is False
    assert "adaptive guardrail blocked" in message.lower()

    allowed, message = guard.authorize(
        ActionRequest(
            action="terminate_process",
            source="desktop-ui",
            metadata={"policy_profile": "ops", "guardrail_override": True},
        )
    )
    assert allowed is True
    assert message == "Allowed"


def test_guardrail_runtime_overrides_and_reset(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_POLICY_GUARDRAILS_STORE", str(tmp_path / "guardrails.json"))
    guard = _guard(
        tmp_path,
        {
            "allowed_actions": ["external_task_update"],
            "profiles": {
                "automation_safe": {"allow": ["external_task_update"], "allow_high_risk": False},
                "automation_power": {"allow": ["external_task_update"], "allow_high_risk": True},
            },
        },
    )
    guard.set_runtime_actions({"external_task_update"})
    for _ in range(11):
        guard.record_action_outcome(
            action="external_task_update",
            status="failed",
            source="desktop-schedule",
            error="confirm policy failed",
        )

    guidance = guard.recommend_runtime_overrides_for_actions(
        actions=["external_task_update"],
        source_name="desktop-schedule",
        metadata={
            "policy_profile": "automation_power",
            "verification_strictness": "off",
            "recovery_profile": "aggressive",
        },
    )
    assert guidance["status"] == "success"
    assert guidance["metadata_overrides"]["verification_strictness"] == "strict"
    assert guidance["metadata_overrides"]["recovery_profile"] == "safe"
    assert guidance["metadata_overrides"]["policy_profile"] == "automation_safe"
    assert guidance["action_overrides"]["external_task_update"]["max_retries_cap"] == 1

    snapshot = guard.guardrail_snapshot(action="external_task_update", limit=5, min_samples=5)
    assert snapshot["status"] == "success"
    assert snapshot["count"] == 1
    assert snapshot["items"][0]["action"] == "external_task_update"

    cleared = guard.reset_guardrails(action="external_task_update")
    assert cleared["status"] == "success"
    assert cleared["removed"] == 1


def test_guardrail_runtime_overrides_consider_contract_pressure_metadata(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_POLICY_GUARDRAILS_STORE", str(tmp_path / "guardrails.json"))
    guard = _guard(
        tmp_path,
        {
            "allowed_actions": ["external_email_read"],
            "profiles": {
                "automation_safe": {"allow": ["external_email_read"], "allow_high_risk": False},
                "automation_power": {"allow": ["external_email_read"], "allow_high_risk": True},
            },
        },
    )
    guard.set_runtime_actions({"external_email_read"})

    guidance = guard.recommend_runtime_overrides_for_actions(
        actions=["external_email_read"],
        source_name="desktop-ui",
        metadata={
            "policy_profile": "automation_power",
            "verification_strictness": "off",
            "recovery_profile": "aggressive",
            "external_contract_pressure": {
                "external_email_read": {
                    "pressure": 0.78,
                    "code": "auth_preflight_failed",
                    "severity": "error",
                }
            },
        },
    )

    assert guidance["status"] == "success"
    assert guidance["recommended_level"] in {"high", "critical"}
    assert guidance["metadata_overrides"]["verification_strictness"] == "strict"
    assert guidance["metadata_overrides"]["recovery_profile"] == "safe"
    assert guidance["metadata_overrides"]["policy_profile"] == "automation_safe"
    assert guidance["action_overrides"]["external_email_read"]["max_retries_cap"] == 1


def test_policy_tune_from_operational_signals_applies_adaptive_defaults(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_POLICY_GUARDRAILS_STORE", str(tmp_path / "guardrails.json"))
    guard = _guard(
        tmp_path,
        {
            "profiles": {
                "automation_safe": {"allow": ["time_now"]},
                "automation_power": {"allow": ["time_now"], "allow_high_risk": True},
                "interactive": {"allow": ["time_now"]},
            },
            "default_profile": "interactive",
        },
    )

    dry_run_payload = guard.tune_from_operational_signals(
        autonomy_report={
            "pressures": {"failure_pressure": 0.52, "open_breaker_pressure": 0.22},
            "scores": {"reliability": 44.0, "autonomy": 39.0},
            "policy_guardrails": {"critical_count": 4, "unstable_count": 9},
        },
        mission_summary={
            "risk": {"avg_score": 0.72},
            "quality": {"avg_score": 0.35},
            "failed_ratio": 0.41,
        },
        dry_run=True,
        reason="unit-test",
    )
    assert dry_run_payload["status"] == "success"
    assert dry_run_payload["mode"] == "stability"
    assert dry_run_payload["changed"] is True
    assert dry_run_payload["applied"] is False

    applied_payload = guard.tune_from_operational_signals(
        autonomy_report={
            "pressures": {"failure_pressure": 0.52, "open_breaker_pressure": 0.22},
            "scores": {"reliability": 44.0, "autonomy": 39.0},
            "policy_guardrails": {"critical_count": 4, "unstable_count": 9},
        },
        mission_summary={
            "risk": {"avg_score": 0.72},
            "quality": {"avg_score": 0.35},
            "failed_ratio": 0.41,
        },
        dry_run=False,
        reason="unit-test",
    )
    assert applied_payload["status"] == "success"
    assert applied_payload["mode"] == "stability"
    assert applied_payload["applied"] is True
    resolved = guard.resolve_policy_profile("desktop-schedule", metadata={})
    assert resolved == "automation_safe"


def test_policy_contract_evaluation_includes_checks_and_overrides(tmp_path) -> None:
    guard = _guard(
        tmp_path,
        {
            "allowed_actions": ["external_email_send"],
            "profiles": {
                "automation_power": {"allow": ["external_email_send"], "allow_high_risk": True},
            },
            "default_profile": "automation_power",
        },
    )
    guard.set_runtime_actions({"external_email_send"})

    contract = guard.evaluate_policy_contract(
        ActionRequest(
            action="external_email_send",
            source="desktop-ui",
            args={"to": ["a@example.com"], "subject": "Hello", "body": "Body"},
            metadata={"policy_profile": "automation_power"},
        ),
        include_runtime_overrides=True,
    )
    assert contract["status"] == "success"
    assert bool(contract["allowed"]) is True
    checks = contract.get("checks", [])
    assert isinstance(checks, list) and checks
    assert isinstance(contract.get("risk"), dict)
    overrides = contract.get("runtime_overrides", {})
    assert isinstance(overrides, dict)


def test_policy_authorize_batch_reports_warning_when_high_risk_budget_exceeded(tmp_path) -> None:
    guard = _guard(
        tmp_path,
        {
            "allowed_actions": ["terminate_process", "external_task_update", "open_url"],
            "profiles": {
                "ops": {"allow": ["terminate_process", "external_task_update", "open_url"], "allow_high_risk": True},
            },
            "default_profile": "ops",
        },
    )
    guard.set_runtime_actions({"terminate_process", "external_task_update", "open_url"})

    requests = [
        ActionRequest(action="open_url", source="desktop-ui", metadata={"policy_profile": "ops"}, args={"url": "https://example.com"}),
        ActionRequest(action="external_task_update", source="desktop-ui", metadata={"policy_profile": "ops"}, args={"task_id": "1", "status": "done"}),
        ActionRequest(action="terminate_process", source="desktop-ui", metadata={"policy_profile": "ops"}, args={"name": "notepad.exe"}),
    ]
    payload = guard.authorize_batch(requests, max_critical=0, max_high=1)
    assert payload["status"] == "success"
    assert int(payload.get("count", 0)) == 3
    warnings = payload.get("warnings", [])
    assert isinstance(warnings, list)
    assert any("high_risk_actions_exceeded" in str(item) for item in warnings)
    batch_risk = payload.get("batch_risk", {})
    assert isinstance(batch_risk, dict)
