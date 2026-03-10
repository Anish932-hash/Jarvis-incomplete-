from __future__ import annotations

from backend.python.core.contracts import ExecutionPlan, PlanStep
from backend.python.core.kernel import AgentKernel
from backend.python.core.tool_registry import ToolRegistry


def _kernel_with_registry(registry: ToolRegistry) -> AgentKernel:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.registry = registry
    return kernel


def test_plan_diagnostics_reports_missing_required_args_and_bad_template() -> None:
    registry = ToolRegistry()
    registry.register("write_file", lambda _args: {"status": "success"}, required_args=["path", "content"], risk="high")
    kernel = _kernel_with_registry(registry)

    plan = ExecutionPlan(
        plan_id="plan-1",
        goal_id="goal-1",
        intent="write",
        steps=[
            PlanStep(
                step_id="s1",
                action="write_file",
                args={"path": "notes.txt", "note": "{{result.anything}}"},
                verify={},
            )
        ],
        context={},
    )

    diagnostics = kernel._analyze_plan_readiness(plan)  # noqa: SLF001
    error_codes = {str(item.get("code")) for item in diagnostics.get("errors", [])}

    assert diagnostics["can_execute"] is False
    assert diagnostics["error_count"] >= 2
    assert "missing_required_args" in error_codes
    assert "unsupported_template_namespace" in error_codes


def test_plan_diagnostics_allows_step_dependency_templates() -> None:
    registry = ToolRegistry()
    registry.register("backup_file", lambda _args: {"status": "success"}, required_args=["source"])
    registry.register("hash_file", lambda _args: {"status": "success"}, required_args=["path"])
    kernel = _kernel_with_registry(registry)

    backup = PlanStep(step_id="s1", action="backup_file", args={"source": "notes.txt"})
    hash_step = PlanStep(
        step_id="s2",
        action="hash_file",
        args={"path": "{{steps.s1.output.backup_path}}"},
        depends_on=["s1"],
    )
    plan = ExecutionPlan(
        plan_id="plan-2",
        goal_id="goal-2",
        intent="backup_hash",
        steps=[backup, hash_step],
        context={},
    )

    diagnostics = kernel._analyze_plan_readiness(plan)  # noqa: SLF001

    assert diagnostics["can_execute"] is True
    assert diagnostics["error_count"] == 0


def test_plan_diagnostics_includes_contract_summary() -> None:
    registry = ToolRegistry()
    registry.register("open_app", lambda _args: {"status": "success"})
    registry.register("write_file", lambda _args: {"status": "success"}, required_args=["path"], risk="high")
    kernel = _kernel_with_registry(registry)

    plan = ExecutionPlan(
        plan_id="plan-3",
        goal_id="goal-3",
        intent="open_and_write",
        steps=[
            PlanStep(step_id="s1", action="open_app", timeout_s=10),
            PlanStep(
                step_id="s2",
                action="write_file",
                args={"path": "notes.txt"},
                timeout_s=14,
                depends_on=["s1"],
                guardrails={"risk_level": "high"},
            ),
        ],
        context={},
    )

    diagnostics = kernel._analyze_plan_readiness(plan)  # noqa: SLF001
    summary = diagnostics.get("summary", {})
    contract = diagnostics.get("contract", {})
    assert diagnostics["can_execute"] is True
    assert int(summary.get("execution_depth", 0)) >= 1
    assert int(summary.get("critical_path_timeout_s", 0)) >= 24
    assert float(summary.get("estimated_total_cost_units", 0.0)) > 0.0
    assert isinstance(contract, dict)
    assert contract.get("status") == "success"
