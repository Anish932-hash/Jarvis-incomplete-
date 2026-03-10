import asyncio
from typing import Dict, List

from backend.python.core.contracts import GoalRecord, GoalRequest
from backend.python.core.planner import Planner
from backend.python.core.task_state import GoalStatus
from .scenarios import Scenario, default_scenarios


class EvaluationRunner:
    def __init__(self) -> None:
        self.planner = Planner()
        self.last_summary: Dict[str, object] = {}

    def run(self, scenarios: List[Scenario] | None = None) -> List[Dict[str, object]]:
        try:
            return asyncio.run(self.run_async(scenarios))
        except RuntimeError:
            # Fallback if called inside an existing event loop.
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(self.run_async(scenarios))
            finally:
                loop.close()

    def run_with_summary(self, scenarios: List[Scenario] | None = None) -> Dict[str, object]:
        items = self.run(scenarios)
        return {"items": items, "summary": dict(self.last_summary)}

    async def run_async(self, scenarios: List[Scenario] | None = None) -> List[Dict[str, object]]:
        scenarios = scenarios or default_scenarios()
        report: List[Dict[str, object]] = []
        aggregate: List[Dict[str, object]] = []
        for idx, scenario in enumerate(scenarios, start=1):
            goal = GoalRecord(
                goal_id=f"eval-{idx}",
                request=GoalRequest(text=scenario.user_text, source="evaluation"),
                status=GoalStatus.PENDING,
            )
            plan = await self.planner.build_plan(goal, context={"source": "evaluation"})
            actual_actions = [step.action for step in plan.steps]
            metrics = self._scenario_metrics(
                expected=scenario.expected_actions,
                actual=actual_actions,
                required=scenario.required_actions,
                strict_order=bool(scenario.strict_order),
            )
            passed = bool(metrics["passed"])
            weight = max(0.1, min(float(scenario.weight), 50.0))
            aggregate.append(
                {
                    "weight": weight,
                    "passed": passed,
                    "score": float(metrics["score"]),
                    "unexpected_actions": list(metrics["unexpected_actions"]),
                }
            )
            report.append(
                {
                    "scenario": scenario.name,
                    "passed": passed,
                    "expected": scenario.expected_actions,
                    "actual": actual_actions,
                    "score": metrics["score"],
                    "precision": metrics["precision"],
                    "recall": metrics["recall"],
                    "order_score": metrics["order_score"],
                    "required_coverage": metrics["required_coverage"],
                    "missing_required": metrics["missing_required"],
                    "unexpected_actions": metrics["unexpected_actions"],
                    "weight": weight,
                }
            )
        self.last_summary = self._summarize(aggregate)
        return report

    @staticmethod
    def _scenario_metrics(
        *,
        expected: List[str],
        actual: List[str],
        required: List[str],
        strict_order: bool,
    ) -> Dict[str, object]:
        expected_actions = [str(item or "").strip() for item in expected if str(item or "").strip()]
        actual_actions = [str(item or "").strip() for item in actual if str(item or "").strip()]
        required_actions = [str(item or "").strip() for item in required if str(item or "").strip()]

        expected_len = max(1, len(expected_actions))
        actual_len = max(1, len(actual_actions))
        lcs = EvaluationRunner._lcs_length(expected_actions, actual_actions)
        precision = lcs / float(actual_len)
        recall = lcs / float(expected_len)
        order_score = recall if strict_order else max(recall, min(1.0, lcs / float(expected_len)))
        required_hits = sum(1 for action in required_actions if action in actual_actions)
        required_coverage = (required_hits / float(len(required_actions))) if required_actions else 1.0
        missing_required = [action for action in required_actions if action not in actual_actions]
        unexpected_actions = [action for action in actual_actions if action not in expected_actions]

        strict_match = actual_actions[: len(expected_actions)] == expected_actions
        score = (0.45 * precision) + (0.45 * recall) + (0.10 * required_coverage)
        if strict_order and strict_match:
            score = max(score, 0.95)

        if strict_order:
            passed = strict_match and required_coverage >= 1.0
        else:
            passed = score >= 0.74 and required_coverage >= 1.0

        return {
            "passed": bool(passed),
            "score": round(max(0.0, min(score, 1.0)), 6),
            "precision": round(max(0.0, min(precision, 1.0)), 6),
            "recall": round(max(0.0, min(recall, 1.0)), 6),
            "order_score": round(max(0.0, min(order_score, 1.0)), 6),
            "required_coverage": round(max(0.0, min(required_coverage, 1.0)), 6),
            "missing_required": missing_required,
            "unexpected_actions": unexpected_actions,
        }

    @staticmethod
    def _lcs_length(expected: List[str], actual: List[str]) -> int:
        if not expected or not actual:
            return 0
        rows = len(expected) + 1
        cols = len(actual) + 1
        table = [[0] * cols for _ in range(rows)]
        for i in range(1, rows):
            exp = expected[i - 1]
            for j in range(1, cols):
                if exp == actual[j - 1]:
                    table[i][j] = table[i - 1][j - 1] + 1
                else:
                    table[i][j] = max(table[i - 1][j], table[i][j - 1])
        return int(table[-1][-1])

    @staticmethod
    def _summarize(rows: List[Dict[str, object]]) -> Dict[str, object]:
        if not rows:
            return {
                "count": 0,
                "weighted_pass_rate": 0.0,
                "weighted_score": 0.0,
                "top_unexpected_actions": [],
            }
        total_weight = 0.0
        pass_weight = 0.0
        score_weight = 0.0
        unexpected_counts: Dict[str, int] = {}
        for row in rows:
            weight = max(0.1, float(row.get("weight", 1.0) or 1.0))
            total_weight += weight
            if bool(row.get("passed", False)):
                pass_weight += weight
            score_weight += weight * float(row.get("score", 0.0) or 0.0)
            for action in row.get("unexpected_actions", []):
                clean = str(action or "").strip()
                if clean:
                    unexpected_counts[clean] = int(unexpected_counts.get(clean, 0)) + 1
        top_unexpected = sorted(
            unexpected_counts.items(),
            key=lambda item: (-int(item[1]), item[0]),
        )[:8]
        return {
            "count": len(rows),
            "weighted_pass_rate": round(pass_weight / max(1e-9, total_weight), 6),
            "weighted_score": round(score_weight / max(1e-9, total_weight), 6),
            "top_unexpected_actions": [{"action": name, "count": count} for name, count in top_unexpected],
        }
