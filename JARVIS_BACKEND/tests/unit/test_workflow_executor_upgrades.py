from __future__ import annotations

import asyncio

from backend.python.tools.automation_tools import AutomationTools


def test_workflow_executor_runs_dag_with_dependencies() -> None:
    shared: dict[str, int] = {}

    def _a():
        shared["a"] = 1
        return "A"

    async def _b():
        await asyncio.sleep(0.01)
        shared["b"] = shared["a"] + 1
        return "B"

    def _c():
        shared["c"] = shared["b"] + 1
        return "C"

    result = asyncio.run(
        AutomationTools.workflow_executor(
            [
                {"name": "task_a", "action": _a},
                {"name": "task_b", "action": _b, "depends_on": ["task_a"]},
                {"name": "task_c", "action": _c, "depends_on": ["task_b"]},
            ],
            max_concurrency=2,
        )
    )

    assert result["status"] == "success"
    assert result["results"]["task_a"] == "A"
    assert result["results"]["task_b"] == "B"
    assert result["results"]["task_c"] == "C"
    assert shared["c"] == 3


def test_workflow_executor_retries_and_skips_dependents_on_failure() -> None:
    counter = {"attempts": 0}

    def _unstable():
        counter["attempts"] += 1
        if counter["attempts"] < 2:
            raise RuntimeError("temporary failure")
        return "OK"

    def _always_fail():
        raise RuntimeError("fatal")

    result_retry = asyncio.run(
        AutomationTools.workflow_executor(
            [
                {"name": "unstable", "action": _unstable, "retries": 1},
            ],
        )
    )
    assert result_retry["status"] == "success"
    assert result_retry["results"]["unstable"] == "OK"
    assert counter["attempts"] == 2

    result_fail = asyncio.run(
        AutomationTools.workflow_executor(
            [
                {"name": "root", "action": _always_fail, "retries": 0},
                {"name": "child", "action": lambda: "never", "depends_on": ["root"]},
            ],
            continue_on_error=False,
        )
    )
    assert result_fail["status"] in {"failed", "partial"}
    assert "root" in result_fail["errors"]
    assert result_fail["skipped"].get("child", "").startswith("dependency_failed")
