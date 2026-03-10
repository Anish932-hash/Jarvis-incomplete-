from __future__ import annotations

from backend.python.policies.risk_engine import RiskEngine


def test_risk_engine_adaptive_failure_drift_increases_score() -> None:
    engine = RiskEngine()
    baseline = engine.rate("external_email_send", source="desktop-ui", metadata={})

    for _ in range(10):
        engine.record_outcome(
            action="external_email_send",
            status="failed",
            source="desktop-ui",
            error="permission denied by provider",
        )

    adapted = engine.rate("external_email_send", source="desktop-ui", metadata={})
    assert adapted.score > baseline.score
    assert any("adaptive_failure_drift" in factor for factor in adapted.factors)


def test_risk_engine_runtime_snapshot_contains_recorded_action() -> None:
    engine = RiskEngine()
    engine.record_outcome(action="open_url", status="success", source="desktop-ui", error="")
    snapshot = engine.runtime_snapshot(limit=10)

    assert snapshot["status"] == "success"
    actions = snapshot.get("actions", [])
    assert any(str(row.get("action", "")) == "open_url" for row in actions if isinstance(row, dict))


def test_risk_engine_mission_feedback_pressure_influences_rating() -> None:
    engine = RiskEngine()
    baseline = engine.rate("external_task_update", source="desktop-ui", metadata={})

    payload = engine.ingest_mission_feedback(
        autonomy_report={"pressures": {"failure_pressure": 0.74, "open_breaker_pressure": 0.4}},
        mission_summary={"risk": {"avg_score": 0.78}, "quality": {"avg_score": 0.32}, "failed_ratio": 0.46},
        reason="unit-test",
    )
    assert payload["status"] == "success"

    adapted = engine.rate(
        "external_task_update",
        source="desktop-ui",
        metadata={"mission_pressure": 0.82},
    )
    assert adapted.score > baseline.score
    assert any("mission_pressure" in factor for factor in adapted.factors)


def test_risk_engine_rate_batch_applies_burst_penalty() -> None:
    engine = RiskEngine()
    payload = engine.rate_batch(
        ["open_url", "external_email_send", "external_task_update", "terminate_process"],
        source="desktop-ui",
        metadata={"policy_profile": "ops"},
    )
    assert payload["status"] == "success"
    assert int(payload["count"]) == 4
    rows = payload.get("items", [])
    assert isinstance(rows, list) and rows
    penalties = [int(row.get("burst_penalty", 0)) for row in rows if isinstance(row, dict)]
    assert max(penalties) >= 2
