from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict, List

from backend.python.core.contracts import GoalRecord, GoalRequest
from backend.python.core.planner import Planner
from backend.python.core.task_state import GoalStatus
from .scenarios import Scenario, default_scenarios, scenario_catalog


class EvaluationRunner:
    def __init__(self, *, history_limit: int = 12) -> None:
        self.planner = Planner()
        self.history_limit = max(1, min(int(history_limit), 128))
        self.last_summary: Dict[str, object] = {}
        self.last_items: List[Dict[str, object]] = []
        self.last_run: Dict[str, object] = {}
        self.run_history: List[Dict[str, object]] = []

    def catalog(
        self,
        scenarios: List[Scenario] | None = None,
        *,
        pack: str = "",
        category: str = "",
        capability: str = "",
        risk_level: str = "",
        autonomy_tier: str = "",
        mission_family: str = "",
        app: str = "",
        limit: int = 200,
    ) -> Dict[str, object]:
        selected = self._select_scenarios(
            scenarios,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
        )
        items = [self._scenario_descriptor(row) for row in selected]
        return {
            "status": "success",
            "count": len(items),
            "items": items,
            "filters": self._filters_payload(
                pack=pack,
                category=category,
                capability=capability,
                risk_level=risk_level,
                autonomy_tier=autonomy_tier,
                mission_family=mission_family,
                app=app,
                limit=limit,
            ),
            "summary": self._catalog_summary(selected),
            "latest_run": dict(self.last_run) if isinstance(self.last_run, dict) else {},
            "history_size": len(self.run_history),
        }

    def history(self, *, limit: int = 12) -> Dict[str, object]:
        normalized_limit = max(1, min(int(limit or 12), self.history_limit))
        items = [dict(item) for item in self.run_history[-normalized_limit:]]
        items.reverse()
        return {
            "status": "success",
            "count": len(items),
            "limit": normalized_limit,
            "items": items,
            "latest_run": dict(self.last_run) if isinstance(self.last_run, dict) else {},
        }

    def run(self, scenarios: List[Scenario] | None = None) -> List[Dict[str, object]]:
        try:
            return asyncio.run(self.run_async(scenarios))
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(self.run_async(scenarios))
            finally:
                loop.close()

    def run_with_summary(
        self,
        scenarios: List[Scenario] | None = None,
        *,
        pack: str = "",
        category: str = "",
        capability: str = "",
        risk_level: str = "",
        autonomy_tier: str = "",
        mission_family: str = "",
        app: str = "",
        limit: int = 200,
    ) -> Dict[str, object]:
        items = self.run(
            self._select_scenarios(
                scenarios,
                pack=pack,
                category=category,
                capability=capability,
                risk_level=risk_level,
                autonomy_tier=autonomy_tier,
                mission_family=mission_family,
                app=app,
                limit=limit,
            )
        )
        filters = self._filters_payload(
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
        )
        return {
            "status": "success",
            "items": items,
            "summary": dict(self.last_summary),
            "regression": self._last_run_regression_payload(),
            "filters": filters,
            "history_size": len(self.run_history),
            "latest_run": dict(self.last_run),
        }

    async def run_async(self, scenarios: List[Scenario] | None = None) -> List[Dict[str, object]]:
        selected = list(scenarios or default_scenarios())
        previous_items = [dict(item) for item in self.last_items]
        previous_summary = dict(self.last_summary)
        report: List[Dict[str, object]] = []
        aggregate: List[Dict[str, object]] = []
        for idx, scenario in enumerate(selected, start=1):
            goal = GoalRecord(
                goal_id=f"eval-{idx}",
                request=GoalRequest(text=scenario.user_text, source="evaluation"),
                status=GoalStatus.PENDING,
            )
            plan = await self.planner.build_plan(goal, context={"source": "evaluation", "scenario": scenario.name, "pack": scenario.pack})
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
                    "category": str(scenario.category or "general"),
                    "pack": str(scenario.pack or "desktop_core"),
                    "mission_family": str(scenario.mission_family or "task"),
                    "autonomy_tier": str(scenario.autonomy_tier or "assisted"),
                    "capabilities": list(scenario.capabilities),
                    "risk_level": str(scenario.risk_level or "standard"),
                    "recovery_expected": bool(scenario.recovery_expected),
                    "native_hybrid_focus": bool(scenario.native_hybrid_focus),
                    "apps": list(scenario.apps),
                }
            )
            report.append(
                {
                    "scenario": scenario.name,
                    "category": scenario.category,
                    "pack": scenario.pack,
                    "platform": scenario.platform,
                    "mission_family": scenario.mission_family,
                    "autonomy_tier": scenario.autonomy_tier,
                    "capabilities": list(scenario.capabilities),
                    "risk_level": scenario.risk_level,
                    "apps": list(scenario.apps),
                    "recovery_expected": bool(scenario.recovery_expected),
                    "native_hybrid_focus": bool(scenario.native_hybrid_focus),
                    "tags": list(scenario.tags),
                    "passed": passed,
                    "expected": scenario.expected_actions,
                    "actual": actual_actions,
                    "score": metrics["score"],
                    "precision": metrics["precision"],
                    "recall": metrics["recall"],
                    "order_score": metrics["order_score"],
                    "required_coverage": metrics["required_coverage"],
                    "missing_required": metrics["missing_required"],
                    "missing_expected": metrics["missing_expected"],
                    "unexpected_actions": metrics["unexpected_actions"],
                    "weight": weight,
                    "notes": scenario.notes,
                }
            )
        self.last_items = report
        self.last_summary = self._summarize(aggregate)
        regression = self._compare_runs(previous_items=previous_items, previous_summary=previous_summary, current_items=report, current_summary=self.last_summary)
        self.last_run = {
            "status": "success",
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "scenario_count": len(report),
            "summary": dict(self.last_summary),
            "regression": dict(regression),
        }
        self.run_history.append(dict(self.last_run))
        self.run_history = self.run_history[-self.history_limit :]
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
        missing_expected = [action for action in expected_actions if action not in actual_actions]

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
            "missing_expected": missing_expected,
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

    def _select_scenarios(
        self,
        scenarios: List[Scenario] | None,
        *,
        pack: str,
        category: str,
        capability: str,
        risk_level: str,
        autonomy_tier: str,
        mission_family: str,
        app: str,
        limit: int,
    ) -> List[Scenario]:
        return scenario_catalog(
            scenarios=scenarios,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
        )

    @staticmethod
    def _filters_payload(
        *,
        pack: str,
        category: str,
        capability: str,
        risk_level: str,
        autonomy_tier: str,
        mission_family: str,
        app: str,
        limit: int,
    ) -> Dict[str, object]:
        return {
            "pack": str(pack or "").strip().lower(),
            "category": str(category or "").strip().lower(),
            "capability": str(capability or "").strip().lower(),
            "risk_level": str(risk_level or "").strip().lower(),
            "autonomy_tier": str(autonomy_tier or "").strip().lower(),
            "mission_family": str(mission_family or "").strip().lower(),
            "app": str(app or "").strip().lower(),
            "limit": max(1, min(int(limit or 200), 5000)),
        }

    @staticmethod
    def _scenario_descriptor(scenario: Scenario) -> Dict[str, object]:
        return {
            "name": scenario.name,
            "user_text": scenario.user_text,
            "expected_actions": list(scenario.expected_actions),
            "required_actions": list(scenario.required_actions),
            "weight": float(scenario.weight),
            "strict_order": bool(scenario.strict_order),
            "category": scenario.category,
            "capabilities": list(scenario.capabilities),
            "risk_level": scenario.risk_level,
            "notes": scenario.notes,
            "pack": scenario.pack,
            "platform": scenario.platform,
            "mission_family": scenario.mission_family,
            "autonomy_tier": scenario.autonomy_tier,
            "apps": list(scenario.apps),
            "recovery_expected": bool(scenario.recovery_expected),
            "native_hybrid_focus": bool(scenario.native_hybrid_focus),
            "tags": list(scenario.tags),
        }

    def _catalog_summary(self, scenarios: List[Scenario]) -> Dict[str, object]:
        pack_counts: Dict[str, int] = {}
        category_counts: Dict[str, int] = {}
        capability_counts: Dict[str, int] = {}
        risk_counts: Dict[str, int] = {}
        autonomy_counts: Dict[str, int] = {}
        mission_counts: Dict[str, int] = {}
        app_counts: Dict[str, int] = {}
        recovery_expected_count = 0
        native_hybrid_focus_count = 0
        for row in scenarios:
            self._increment_count(pack_counts, row.pack)
            self._increment_count(category_counts, row.category)
            self._increment_count(risk_counts, row.risk_level)
            self._increment_count(autonomy_counts, row.autonomy_tier)
            self._increment_count(mission_counts, row.mission_family)
            if row.recovery_expected:
                recovery_expected_count += 1
            if row.native_hybrid_focus:
                native_hybrid_focus_count += 1
            for capability in row.capabilities:
                self._increment_count(capability_counts, capability)
            for app in row.apps:
                self._increment_count(app_counts, app)
        return {
            "scenario_count": len(scenarios),
            "pack_counts": self._sorted_count_map(pack_counts),
            "category_counts": self._sorted_count_map(category_counts),
            "capability_counts": self._sorted_count_map(capability_counts),
            "risk_counts": self._sorted_count_map(risk_counts),
            "autonomy_tier_counts": self._sorted_count_map(autonomy_counts),
            "mission_family_counts": self._sorted_count_map(mission_counts),
            "app_counts": self._sorted_count_map(app_counts),
            "recovery_expected_count": recovery_expected_count,
            "native_hybrid_focus_count": native_hybrid_focus_count,
        }

    @staticmethod
    def _increment_count(target: Dict[str, int], key: str) -> None:
        clean_key = " ".join(str(key or "").strip().lower().split())
        if not clean_key:
            return
        target[clean_key] = int(target.get(clean_key, 0)) + 1

    @staticmethod
    def _sorted_count_map(source: Dict[str, int], *, limit: int = 24) -> Dict[str, int]:
        ordered = sorted(source.items(), key=lambda item: (-int(item[1]), item[0]))
        return {name: count for name, count in ordered[: max(1, limit)]}

    @staticmethod
    def _bucket_view(source: Dict[str, Dict[str, float]]) -> List[Dict[str, object]]:
        ordered = sorted(source.items(), key=lambda item: (-item[1]["weight"], item[0]))
        payload: List[Dict[str, object]] = []
        for name, bucket in ordered:
            weight = max(1e-9, float(bucket.get("weight", 0.0) or 0.0))
            payload.append(
                {
                    "name": name,
                    "weighted_pass_rate": round(float(bucket.get("pass_weight", 0.0) or 0.0) / weight, 6),
                    "weighted_score": round(float(bucket.get("score_weight", 0.0) or 0.0) / weight, 6),
                    "weight": round(weight, 6),
                }
            )
        return payload

    def _summarize(self, rows: List[Dict[str, object]]) -> Dict[str, object]:
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
        category_rows: Dict[str, Dict[str, float]] = {}
        capability_rows: Dict[str, Dict[str, float]] = {}
        risk_rows: Dict[str, Dict[str, float]] = {}
        pack_rows: Dict[str, Dict[str, float]] = {}
        autonomy_rows: Dict[str, Dict[str, float]] = {}
        mission_rows: Dict[str, Dict[str, float]] = {}
        recovery_weight = 0.0
        recovery_pass_weight = 0.0
        recovery_score_weight = 0.0
        hybrid_weight = 0.0
        hybrid_pass_weight = 0.0
        hybrid_score_weight = 0.0
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
            self._accumulate_bucket(category_rows, str(row.get("category", "general") or "general"), weight, row)
            self._accumulate_bucket(pack_rows, str(row.get("pack", "desktop_core") or "desktop_core"), weight, row)
            self._accumulate_bucket(risk_rows, str(row.get("risk_level", "standard") or "standard"), weight, row)
            self._accumulate_bucket(autonomy_rows, str(row.get("autonomy_tier", "assisted") or "assisted"), weight, row)
            self._accumulate_bucket(mission_rows, str(row.get("mission_family", "task") or "task"), weight, row)
            for capability in row.get("capabilities", []):
                clean_capability = str(capability or "").strip()
                if clean_capability:
                    self._accumulate_bucket(capability_rows, clean_capability, weight, row)
            if bool(row.get("recovery_expected", False)):
                recovery_weight += weight
                recovery_score_weight += weight * float(row.get("score", 0.0) or 0.0)
                if bool(row.get("passed", False)):
                    recovery_pass_weight += weight
            if bool(row.get("native_hybrid_focus", False)):
                hybrid_weight += weight
                hybrid_score_weight += weight * float(row.get("score", 0.0) or 0.0)
                if bool(row.get("passed", False)):
                    hybrid_pass_weight += weight
        top_unexpected = sorted(unexpected_counts.items(), key=lambda item: (-int(item[1]), item[0]))[:8]
        return {
            "count": len(rows),
            "weighted_pass_rate": round(pass_weight / max(1e-9, total_weight), 6),
            "weighted_score": round(score_weight / max(1e-9, total_weight), 6),
            "top_unexpected_actions": [{"action": name, "count": count} for name, count in top_unexpected],
            "pack_breakdown": self._bucket_view(pack_rows),
            "category_breakdown": self._bucket_view(category_rows),
            "capability_coverage": self._bucket_view(capability_rows),
            "risk_breakdown": self._bucket_view(risk_rows),
            "autonomy_tier_breakdown": self._bucket_view(autonomy_rows),
            "mission_family_breakdown": self._bucket_view(mission_rows),
            "recovery_readiness": {
                "weighted_pass_rate": round(recovery_pass_weight / max(1e-9, recovery_weight), 6) if recovery_weight else 0.0,
                "weighted_score": round(recovery_score_weight / max(1e-9, recovery_weight), 6) if recovery_weight else 0.0,
                "weight": round(recovery_weight, 6),
            },
            "native_hybrid_coverage": {
                "weighted_pass_rate": round(hybrid_pass_weight / max(1e-9, hybrid_weight), 6) if hybrid_weight else 0.0,
                "weighted_score": round(hybrid_score_weight / max(1e-9, hybrid_weight), 6) if hybrid_weight else 0.0,
                "weight": round(hybrid_weight, 6),
            },
            "improvement_candidates": self._improvement_candidates(
                pack_rows=pack_rows,
                category_rows=category_rows,
                capability_rows=capability_rows,
                mission_rows=mission_rows,
                recovery_score=recovery_score_weight / max(1e-9, recovery_weight) if recovery_weight else None,
                hybrid_score=hybrid_score_weight / max(1e-9, hybrid_weight) if hybrid_weight else None,
            ),
        }

    @staticmethod
    def _accumulate_bucket(target: Dict[str, Dict[str, float]], name: str, weight: float, row: Dict[str, object]) -> None:
        bucket = target.setdefault(name, {"weight": 0.0, "pass_weight": 0.0, "score_weight": 0.0})
        bucket["weight"] += weight
        bucket["score_weight"] += weight * float(row.get("score", 0.0) or 0.0)
        if bool(row.get("passed", False)):
            bucket["pass_weight"] += weight

    def _compare_runs(
        self,
        *,
        previous_items: List[Dict[str, object]],
        previous_summary: Dict[str, object],
        current_items: List[Dict[str, object]],
        current_summary: Dict[str, object],
    ) -> Dict[str, object]:
        if not previous_summary:
            return {
                "status": "baseline",
                "weighted_pass_rate_delta": 0.0,
                "weighted_score_delta": 0.0,
                "scenario_regressions": [],
                "pack_regressions": [],
                "category_regressions": [],
                "capability_regressions": [],
            }
        previous_by_name = {
            str(row.get("scenario", "") or "").strip(): row
            for row in previous_items
            if isinstance(row, dict) and str(row.get("scenario", "") or "").strip()
        }
        current_by_name = {
            str(row.get("scenario", "") or "").strip(): row
            for row in current_items
            if isinstance(row, dict) and str(row.get("scenario", "") or "").strip()
        }
        scenario_regressions: List[Dict[str, object]] = []
        for scenario_name, current_row in current_by_name.items():
            previous_row = previous_by_name.get(scenario_name, {})
            if not isinstance(previous_row, dict):
                continue
            current_score = float(current_row.get("score", 0.0) or 0.0)
            previous_score = float(previous_row.get("score", 0.0) or 0.0)
            if current_score < (previous_score - 0.08):
                scenario_regressions.append(
                    {
                        "scenario": scenario_name,
                        "score_delta": round(current_score - previous_score, 6),
                        "previous_score": round(previous_score, 6),
                        "current_score": round(current_score, 6),
                        "pack": str(current_row.get("pack", "") or ""),
                        "category": str(current_row.get("category", "") or ""),
                    }
                )
        return {
            "status": "regression" if scenario_regressions else "stable",
            "weighted_pass_rate_delta": round(
                float(current_summary.get("weighted_pass_rate", 0.0) or 0.0)
                - float(previous_summary.get("weighted_pass_rate", 0.0) or 0.0),
                6,
            ),
            "weighted_score_delta": round(
                float(current_summary.get("weighted_score", 0.0) or 0.0)
                - float(previous_summary.get("weighted_score", 0.0) or 0.0),
                6,
            ),
            "scenario_regressions": scenario_regressions[:10],
            "pack_regressions": self._compare_bucket_breakdowns(
                previous_summary.get("pack_breakdown", []),
                current_summary.get("pack_breakdown", []),
            ),
            "category_regressions": self._compare_bucket_breakdowns(
                previous_summary.get("category_breakdown", []),
                current_summary.get("category_breakdown", []),
            ),
            "capability_regressions": self._compare_bucket_breakdowns(
                previous_summary.get("capability_coverage", []),
                current_summary.get("capability_coverage", []),
            ),
        }

    @staticmethod
    def _compare_bucket_breakdowns(previous: object, current: object) -> List[Dict[str, object]]:
        previous_rows = {
            str(row.get("name", "") or "").strip(): row
            for row in previous
            if isinstance(previous, list) and isinstance(row, dict) and str(row.get("name", "") or "").strip()
        }
        regressions: List[Dict[str, object]] = []
        if not isinstance(current, list):
            return regressions
        for row in current:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name", "") or "").strip()
            if not name or name not in previous_rows:
                continue
            previous_row = previous_rows[name]
            previous_score = float(previous_row.get("weighted_score", 0.0) or 0.0)
            current_score = float(row.get("weighted_score", 0.0) or 0.0)
            previous_pass_rate = float(previous_row.get("weighted_pass_rate", 0.0) or 0.0)
            current_pass_rate = float(row.get("weighted_pass_rate", 0.0) or 0.0)
            if current_score < (previous_score - 0.08) or current_pass_rate < (previous_pass_rate - 0.10):
                regressions.append(
                    {
                        "name": name,
                        "weighted_score_delta": round(current_score - previous_score, 6),
                        "weighted_pass_rate_delta": round(current_pass_rate - previous_pass_rate, 6),
                    }
                )
        regressions.sort(key=lambda item: (item["weighted_score_delta"], item["weighted_pass_rate_delta"], item["name"]))
        return regressions[:10]

    def _improvement_candidates(
        self,
        *,
        pack_rows: Dict[str, Dict[str, float]],
        category_rows: Dict[str, Dict[str, float]],
        capability_rows: Dict[str, Dict[str, float]],
        mission_rows: Dict[str, Dict[str, float]],
        recovery_score: float | None,
        hybrid_score: float | None,
    ) -> Dict[str, object]:
        return {
            "packs": self._weakest_bucket_candidates(pack_rows),
            "categories": self._weakest_bucket_candidates(category_rows),
            "capabilities": self._weakest_bucket_candidates(capability_rows, limit=6),
            "mission_families": self._weakest_bucket_candidates(mission_rows),
            "recovery_focus": (
                {
                    "target": "recovery_readiness",
                    "weighted_score": round(recovery_score, 6),
                }
                if recovery_score is not None and recovery_score < 0.9
                else None
            ),
            "native_hybrid_focus": (
                {
                    "target": "native_hybrid_coverage",
                    "weighted_score": round(hybrid_score, 6),
                }
                if hybrid_score is not None and hybrid_score < 0.9
                else None
            ),
        }

    @staticmethod
    def _weakest_bucket_candidates(
        source: Dict[str, Dict[str, float]],
        *,
        limit: int = 4,
        min_weight: float = 0.75,
        score_threshold: float = 0.9,
    ) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for name, bucket in source.items():
            weight = float(bucket.get("weight", 0.0) or 0.0)
            if weight < min_weight:
                continue
            weighted_score = float(bucket.get("score_weight", 0.0) or 0.0) / max(1e-9, weight)
            weighted_pass_rate = float(bucket.get("pass_weight", 0.0) or 0.0) / max(1e-9, weight)
            if weighted_score >= score_threshold and weighted_pass_rate >= score_threshold:
                continue
            rows.append(
                {
                    "name": name,
                    "weighted_score": round(weighted_score, 6),
                    "weighted_pass_rate": round(weighted_pass_rate, 6),
                    "weight": round(weight, 6),
                }
            )
        rows.sort(key=lambda item: (item["weighted_score"], item["weighted_pass_rate"], -float(item["weight"]), item["name"]))
        return rows[: max(1, limit)]

    def _last_run_regression_payload(self) -> Dict[str, object]:
        return dict(self.last_run.get("regression", {})) if isinstance(self.last_run, dict) else {}
