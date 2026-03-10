from importlib import import_module
from typing import Any

__all__ = [
    "ActionRequest",
    "ActionResult",
    "ExecutionPlan",
    "GoalRecord",
    "GoalRequest",
    "PlanStep",
    "GoalStatus",
    "StepStatus",
    "AgentKernel",
]


def __getattr__(name: str) -> Any:
    if name in {"ActionRequest", "ActionResult", "ExecutionPlan", "GoalRecord", "GoalRequest", "PlanStep"}:
        module = import_module("backend.python.core.contracts")
        return getattr(module, name)
    if name in {"GoalStatus", "StepStatus"}:
        module = import_module("backend.python.core.task_state")
        return getattr(module, name)
    if name == "AgentKernel":
        module = import_module("backend.python.core.kernel")
        return getattr(module, name)
    raise AttributeError(name)
