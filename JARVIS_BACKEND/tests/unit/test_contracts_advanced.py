from __future__ import annotations

from backend.python.core.contracts import ActionRequest, ActionResult, ExecutionPlan, PlanStep


def test_action_request_budget_and_dedupe_helpers() -> None:
    req = ActionRequest(
        action="Write_File",
        source="Desktop-UI",
        args={"path": "notes.txt", "content": "hello"},
        deadline_at="2026-03-03T00:00:02+00:00",
    )

    assert req.normalized_action() == "write_file"
    assert req.dedupe_key().startswith("desktop-ui:write_file:")
    remaining = req.remaining_budget_ms(now_iso="2026-03-03T00:00:00+00:00")
    assert isinstance(remaining, int)
    assert 1800 <= int(remaining) <= 2200


def test_action_result_error_code_prefers_structured_fields() -> None:
    row = ActionResult(
        action="external_send_email",
        status="failed",
        output={"error_code": "AUTH_EXPIRED"},
        error="provider timeout",
    )
    assert row.error_code() == "auth_expired"


def test_execution_plan_runtime_contract_reports_layers_and_critical_path() -> None:
    step1 = PlanStep(step_id="s1", action="open_app", timeout_s=12)
    step2 = PlanStep(step_id="s2", action="write_file", depends_on=["s1"], timeout_s=18, guardrails={"risk_level": "high"})
    step3 = PlanStep(
        step_id="s3",
        action="external_email_send",
        depends_on=["s1"],
        timeout_s=9,
        guardrails={"risk_level": "high", "requires_approval": True},
        verify={"mode": "strict"},
    )
    plan = ExecutionPlan(
        plan_id="plan-contract-1",
        goal_id="goal-contract-1",
        intent="compose_and_send",
        steps=[step1, step2, step3],
    )

    contract = plan.runtime_contract()
    assert contract["status"] == "success"
    assert contract["execution_depth"] >= 2
    assert contract["critical_path_timeout_s"] == 30
    risk = contract.get("risk", {})
    assert risk.get("step_count") == 3
    assert float(risk.get("estimated_total_cost_units", 0.0)) > 0.0
    assert int(risk.get("high_risk_count", 0)) >= 1
