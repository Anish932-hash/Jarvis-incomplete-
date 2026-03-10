from __future__ import annotations

from backend.python.policies.risk_engine import RiskEngine


def test_risk_engine_increases_open_url_risk_for_sensitive_targets() -> None:
    engine = RiskEngine()

    normal = engine.rate("open_url", args={"url": "https://example.com"})
    risky = engine.rate("open_url", args={"url": "file://localhost/admin"})

    assert risky.score > normal.score
    assert any("url_local_or_file" in factor for factor in risky.factors)


def test_risk_engine_automation_source_bias_for_high_risk_actions() -> None:
    engine = RiskEngine()

    interactive = engine.rate("mouse_click", source="desktop-ui", metadata={"policy_profile": "interactive"})
    automated = engine.rate("mouse_click", source="desktop-schedule", metadata={"policy_profile": "interactive"})

    assert automated.score > interactive.score
    assert any("source_automation_high_risk" in factor for factor in automated.factors)


def test_risk_engine_marks_computer_click_target_as_high_risk() -> None:
    engine = RiskEngine()

    rating = engine.rate("computer_click_target", source="desktop-ui", metadata={"policy_profile": "interactive"})

    assert rating.score >= 70
    assert rating.level in {"high", "critical"}


def test_risk_engine_marks_external_task_update_as_high_risk() -> None:
    engine = RiskEngine()

    rating = engine.rate("external_task_update", source="desktop-ui", metadata={"policy_profile": "interactive"})

    assert rating.score >= 60
    assert rating.level in {"high", "critical"}
