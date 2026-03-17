from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Callable, Dict, List

from backend.python.core.contracts import GoalRecord, GoalRequest
from backend.python.core.planner import Planner
from backend.python.core.task_state import GoalStatus
from .scenarios import Scenario, default_scenarios, scenario_catalog


class EvaluationRunner:
    def __init__(
        self,
        *,
        history_limit: int = 12,
        installed_app_catalog_provider: Callable[..., Dict[str, object]] | None = None,
    ) -> None:
        self.planner = Planner()
        self.history_limit = max(1, min(int(history_limit), 128))
        self.installed_app_catalog_provider = installed_app_catalog_provider
        self.last_summary: Dict[str, object] = {}
        self.last_items: List[Dict[str, object]] = []
        self.last_run: Dict[str, object] = {}
        self.run_history: List[Dict[str, object]] = []

    def catalog(
        self,
        scenarios: List[Scenario] | None = None,
        *,
        scenario_name: str = "",
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
            scenario_name=scenario_name,
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
                scenario_name=scenario_name,
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

    def lab(
        self,
        *,
        scenario_name: str = "",
        pack: str = "",
        category: str = "",
        capability: str = "",
        risk_level: str = "",
        autonomy_tier: str = "",
        mission_family: str = "",
        app: str = "",
        limit: int = 200,
        history_limit: int = 8,
    ) -> Dict[str, object]:
        filters = self._filters_payload(
            scenario_name=scenario_name,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
        )
        selected = self._select_scenarios(
            None,
            scenario_name=scenario_name,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
        )
        latest_run = dict(self.last_run) if isinstance(self.last_run, dict) else {}
        latest_summary = (
            dict(latest_run.get("summary", {}))
            if isinstance(latest_run.get("summary", {}), dict)
            else dict(self.last_summary)
        )
        latest_regression = (
            dict(latest_run.get("regression", {}))
            if isinstance(latest_run.get("regression", {}), dict)
            else self._last_run_regression_payload()
        )
        filtered_latest_items = self._filter_item_rows(self.last_items, filters=filters)
        filtered_history = self._filtered_history(filters=filters, limit=history_limit)
        return {
            "status": "success",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "filters": filters,
            "catalog_summary": self._catalog_summary(selected),
            "coverage": self._lab_coverage(selected),
            "history_trend": self._history_trend(filtered_history),
            "latest_run": latest_run,
            "latest_summary": latest_summary,
            "latest_regression": latest_regression,
            "replay_candidates": self._replay_candidates(filtered_latest_items, filters=filters),
            "installed_app_coverage": self._installed_app_coverage(selected),
            "history_size": len(self.run_history),
        }

    def control_guidance(self) -> Dict[str, object]:
        latest_run = dict(self.last_run) if isinstance(self.last_run, dict) else {}
        latest_summary = (
            dict(latest_run.get("summary", {}))
            if isinstance(latest_run.get("summary", {}), dict)
            else dict(self.last_summary)
        )
        improvement_candidates = (
            dict(latest_summary.get("improvement_candidates", {}))
            if isinstance(latest_summary.get("improvement_candidates", {}), dict)
            else {}
        )
        weakest_pack = self._first_candidate_name(improvement_candidates.get("packs", []))
        weakest_category = self._first_candidate_name(improvement_candidates.get("categories", []))
        weakest_capability = self._first_candidate_name(improvement_candidates.get("capabilities", []))
        weakest_mission_family = self._first_candidate_name(improvement_candidates.get("mission_families", []))
        recovery_focus = (
            dict(improvement_candidates.get("recovery_focus", {}))
            if isinstance(improvement_candidates.get("recovery_focus", {}), dict)
            else {}
        )
        native_hybrid_focus = (
            dict(improvement_candidates.get("native_hybrid_focus", {}))
            if isinstance(improvement_candidates.get("native_hybrid_focus", {}), dict)
            else {}
        )
        biases = self._control_biases_from_summary(
            weakest_pack=weakest_pack,
            weakest_category=weakest_category,
            weakest_capability=weakest_capability,
            weakest_mission_family=weakest_mission_family,
            recovery_focus=recovery_focus,
            native_hybrid_focus=native_hybrid_focus,
        )
        focus_summary = [
            value
            for value in [weakest_pack, weakest_category, weakest_capability, weakest_mission_family]
            if value
        ][:6]
        return {
            "status": "success",
            "benchmark_ready": bool(latest_summary),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "latest_run_executed_at": str(latest_run.get("executed_at", "") or "").strip(),
            "latest_weighted_pass_rate": round(float(latest_summary.get("weighted_pass_rate", 0.0) or 0.0), 6),
            "latest_weighted_score": round(float(latest_summary.get("weighted_score", 0.0) or 0.0), 6),
            "weakest_pack": weakest_pack,
            "weakest_category": weakest_category,
            "weakest_capability": weakest_capability,
            "weakest_mission_family": weakest_mission_family,
            "focus_summary": focus_summary,
            "control_biases": biases,
            "recovery_focus": recovery_focus,
            "native_hybrid_focus": native_hybrid_focus,
            "improvement_candidates": improvement_candidates,
            "history_size": len(self.run_history),
        }

    def native_control_targets(
        self,
        *,
        scenario_name: str = "",
        pack: str = "",
        category: str = "",
        capability: str = "",
        risk_level: str = "",
        autonomy_tier: str = "",
        mission_family: str = "",
        app: str = "",
        limit: int = 200,
        history_limit: int = 8,
    ) -> Dict[str, object]:
        lab_payload = self.lab(
            scenario_name=scenario_name,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
            history_limit=history_limit,
        )
        selected = self._select_scenarios(
            None,
            scenario_name=scenario_name,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
        )
        scenario_by_name = {row.name: row for row in selected}
        replay_candidates = (
            [
                dict(item)
                for item in lab_payload.get("replay_candidates", [])
                if isinstance(item, dict)
            ]
            if isinstance(lab_payload, dict)
            else []
        )
        target_apps: Dict[str, Dict[str, object]] = {}
        tactic_totals = {
            "dialog_resolution": 0.0,
            "descendant_focus": 0.0,
            "navigation_branch": 0.0,
            "recovery_reacquire": 0.0,
            "loop_guard": 0.0,
            "native_focus": 0.0,
        }
        for candidate in replay_candidates:
            scenario_name_value = str(candidate.get("scenario", "") or "").strip()
            scenario = scenario_by_name.get(scenario_name_value)
            if scenario is None:
                continue
            tactic_profile = self._scenario_native_tactic_profile(scenario=scenario)
            for app_name in [str(item).strip().lower() for item in scenario.apps if str(item).strip()]:
                entry = target_apps.setdefault(
                    app_name,
                    {
                        "app_name": app_name,
                        "priority": 0.0,
                        "scenario_names": [],
                        "packs": set(),
                        "mission_families": set(),
                        "query_hints": [],
                        "max_horizon_steps": 0,
                        "control_biases": {
                            "dialog_resolution": 0.0,
                            "descendant_focus": 0.0,
                            "navigation_branch": 0.0,
                            "recovery_reacquire": 0.0,
                            "loop_guard": 0.0,
                            "native_focus": 0.0,
                        },
                    },
                )
                entry["priority"] = float(entry.get("priority", 0.0) or 0.0) + float(
                    candidate.get("weight", candidate.get("score", 0.0)) or 0.0
                ) + max(0.0, 1.0 - float(candidate.get("score", 0.0) or 0.0))
                scenario_names = entry["scenario_names"] if isinstance(entry.get("scenario_names"), list) else []
                if scenario.name not in scenario_names:
                    scenario_names.append(scenario.name)
                entry["scenario_names"] = scenario_names[:6]
                packs = entry["packs"] if isinstance(entry.get("packs"), set) else set(entry.get("packs", []))
                packs.add(str(scenario.pack or "").strip())
                entry["packs"] = packs
                missions = entry["mission_families"] if isinstance(entry.get("mission_families"), set) else set(entry.get("mission_families", []))
                missions.add(str(scenario.mission_family or "").strip())
                entry["mission_families"] = missions
                hints = entry["query_hints"] if isinstance(entry.get("query_hints"), list) else []
                for hint in self._scenario_query_hints(scenario=scenario):
                    if hint not in hints:
                        hints.append(hint)
                entry["query_hints"] = hints[:8]
                entry["max_horizon_steps"] = max(
                    int(entry.get("max_horizon_steps", 0) or 0),
                    max(1, int(scenario.horizon_steps or 1)),
                )
                control_biases = (
                    dict(entry.get("control_biases", {}))
                    if isinstance(entry.get("control_biases", {}), dict)
                    else {}
                )
                for key, value in tactic_profile.items():
                    tactic_value = max(0.0, min(float(value or 0.0), 1.0))
                    control_biases[key] = max(float(control_biases.get(key, 0.0) or 0.0), tactic_value)
                    tactic_totals[key] += tactic_value
                entry["control_biases"] = control_biases
        target_app_rows: List[Dict[str, object]] = []
        for row in target_apps.values():
            target_app_rows.append(
                {
                    "app_name": str(row.get("app_name", "") or "").strip(),
                    "priority": round(float(row.get("priority", 0.0) or 0.0), 6),
                    "scenario_names": list(row.get("scenario_names", []))[:6],
                    "packs": sorted(str(item).strip() for item in row.get("packs", set()) if str(item).strip())[:6],
                    "mission_families": sorted(
                        str(item).strip() for item in row.get("mission_families", set()) if str(item).strip()
                    )[:6],
                    "query_hints": list(row.get("query_hints", []))[:8],
                    "max_horizon_steps": int(row.get("max_horizon_steps", 0) or 0),
                    "control_biases": {
                        key: round(max(0.0, min(float(value or 0.0), 1.0)), 6)
                        for key, value in dict(row.get("control_biases", {})).items()
                    },
                }
            )
        target_app_rows.sort(
            key=lambda item: (
                -float(item.get("priority", 0.0) or 0.0),
                -int(item.get("max_horizon_steps", 0) or 0),
                str(item.get("app_name", "") or ""),
            )
        )
        strongest_tactics = {
            key: round(
                max(0.0, min(float(value or 0.0) / max(1.0, float(len(target_app_rows) or 1.0)), 1.0)),
                6,
            )
            for key, value in tactic_totals.items()
        }
        installed_app_coverage = (
            dict(lab_payload.get("installed_app_coverage", {}))
            if isinstance(lab_payload.get("installed_app_coverage", {}), dict)
            else {}
        )
        return {
            "status": "success",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "benchmark_ready": bool(target_app_rows),
            "filters": dict(lab_payload.get("filters", {})) if isinstance(lab_payload, dict) else {},
            "focus_summary": list(lab_payload.get("coverage", {}).keys())[:4]
            if isinstance(lab_payload.get("coverage", {}), dict)
            else [],
            "target_apps": target_app_rows[:8],
            "target_app_biases": {
                str(item.get("app_name", "") or "").strip().lower(): dict(item.get("control_biases", {}))
                for item in target_app_rows[:8]
                if str(item.get("app_name", "") or "").strip()
            },
            "replay_candidates": replay_candidates[:8],
            "strongest_tactics": strongest_tactics,
            "coverage_gap_apps": [
                str(item).strip()
                for item in installed_app_coverage.get("missing_apps", [])
                if str(item).strip()
            ][:12] if isinstance(installed_app_coverage.get("missing_apps", []), list) else [],
            "installed_app_coverage": installed_app_coverage,
            "history_trend": dict(lab_payload.get("history_trend", {}))
            if isinstance(lab_payload.get("history_trend", {}), dict)
            else {},
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
        scenario_name: str = "",
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
                scenario_name=scenario_name,
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
            scenario_name=scenario_name,
            pack=pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
        )
        if isinstance(self.last_run, dict):
            self.last_run["filters"] = dict(filters)
        if self.run_history:
            self.run_history[-1]["filters"] = dict(filters)
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
                    "replayable": bool(scenario.replayable),
                    "horizon_steps": max(1, int(scenario.horizon_steps or 1)),
                    "apps": list(scenario.apps),
                }
            )
            report.append(
                {
                    "scenario": scenario.name,
                    "user_text": scenario.user_text,
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
                    "replayable": bool(scenario.replayable),
                    "horizon_steps": max(1, int(scenario.horizon_steps or 1)),
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
        scenario_name: str,
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
            scenario_name=scenario_name,
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
        scenario_name: str,
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
            "scenario_name": str(scenario_name or "").strip().lower(),
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
            "replayable": bool(scenario.replayable),
            "horizon_steps": max(1, int(scenario.horizon_steps or 1)),
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
        replayable_count = 0
        long_horizon_count = 0
        total_horizon_steps = 0
        max_horizon_steps = 0
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
            if row.replayable:
                replayable_count += 1
            horizon_steps = max(1, int(row.horizon_steps or 1))
            total_horizon_steps += horizon_steps
            max_horizon_steps = max(max_horizon_steps, horizon_steps)
            if horizon_steps >= 4:
                long_horizon_count += 1
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
            "replayable_count": replayable_count,
            "long_horizon_count": long_horizon_count,
            "avg_horizon_steps": round(total_horizon_steps / max(1, len(scenarios)), 6),
            "max_horizon_steps": max_horizon_steps,
        }

    def _lab_coverage(self, scenarios: List[Scenario]) -> Dict[str, object]:
        total = len(scenarios)
        replayable = [row for row in scenarios if bool(row.replayable)]
        long_horizon = [row for row in scenarios if max(1, int(row.horizon_steps or 1)) >= 4]
        return {
            "scenario_count": total,
            "replayable": {
                "count": len(replayable),
                "ratio": round(len(replayable) / max(1, total), 6),
                "sample_scenarios": [row.name for row in replayable[:6]],
            },
            "long_horizon": {
                "count": len(long_horizon),
                "ratio": round(len(long_horizon) / max(1, total), 6),
                "avg_horizon_steps": round(
                    sum(max(1, int(row.horizon_steps or 1)) for row in scenarios) / max(1, total),
                    6,
                ),
                "max_horizon_steps": max((max(1, int(row.horizon_steps or 1)) for row in scenarios), default=0),
                "sample_scenarios": [row.name for row in sorted(long_horizon, key=lambda row: (-max(1, int(row.horizon_steps or 1)), row.name))[:6]],
            },
            "real_app_focus": {
                "count": sum(1 for row in scenarios if any(str(app or "").strip() and str(app or "").strip().lower() not in {"system", "speech", "browser"} for app in row.apps)),
                "app_counts": self._sorted_count_map(
                    {
                        str(app or "").strip().lower(): sum(
                            1
                            for row in scenarios
                            if any(str(item or "").strip().lower() == str(app or "").strip().lower() for item in row.apps)
                        )
                        for app in {
                            str(item or "").strip().lower()
                            for row in scenarios
                            for item in row.apps
                            if str(item or "").strip()
                        }
                    },
                    limit=16,
                ),
            },
        }

    def _filtered_history(self, *, filters: Dict[str, object], limit: int) -> List[Dict[str, object]]:
        bounded = max(1, min(int(limit or 8), self.history_limit))
        rows: List[Dict[str, object]] = []
        for item in self.run_history[-bounded:]:
            if not isinstance(item, dict):
                continue
            item_filters = dict(item.get("filters", {})) if isinstance(item.get("filters", {}), dict) else {}
            if self._filters_match(item_filters, filters):
                rows.append(dict(item))
        return rows

    @staticmethod
    def _filters_match(left: Dict[str, object], right: Dict[str, object]) -> bool:
        for key, value in right.items():
            clean_value = str(value or "").strip().lower()
            if key == "limit" or not clean_value:
                continue
            if str(left.get(key, "") or "").strip().lower() != clean_value:
                return False
        return True

    def _filter_item_rows(self, rows: List[Dict[str, object]], *, filters: Dict[str, object]) -> List[Dict[str, object]]:
        selected: List[Dict[str, object]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if filters.get("scenario_name") and str(row.get("scenario", "") or "").strip().lower() != str(filters.get("scenario_name", "") or "").strip().lower():
                continue
            if filters.get("pack") and str(row.get("pack", "") or "").strip().lower() != str(filters.get("pack", "") or "").strip().lower():
                continue
            if filters.get("category") and str(row.get("category", "") or "").strip().lower() != str(filters.get("category", "") or "").strip().lower():
                continue
            if filters.get("risk_level") and str(row.get("risk_level", "") or "").strip().lower() != str(filters.get("risk_level", "") or "").strip().lower():
                continue
            if filters.get("autonomy_tier") and str(row.get("autonomy_tier", "") or "").strip().lower() != str(filters.get("autonomy_tier", "") or "").strip().lower():
                continue
            if filters.get("mission_family") and str(row.get("mission_family", "") or "").strip().lower() != str(filters.get("mission_family", "") or "").strip().lower():
                continue
            if filters.get("capability"):
                capability = str(filters.get("capability", "") or "").strip().lower()
                if not any(str(item or "").strip().lower() == capability for item in row.get("capabilities", [])):
                    continue
            if filters.get("app"):
                app_name = str(filters.get("app", "") or "").strip().lower()
                if not any(str(item or "").strip().lower() == app_name for item in row.get("apps", [])):
                    continue
            selected.append(dict(row))
        return selected

    def _history_trend(self, rows: List[Dict[str, object]]) -> Dict[str, object]:
        if not rows:
            return {
                "run_count": 0,
                "direction": "insufficient_history",
                "weighted_score_delta": 0.0,
                "weighted_pass_rate_delta": 0.0,
                "regression_run_count": 0,
                "recent_regression_count": 0,
            }
        first = rows[0]
        last = rows[-1]
        first_summary = dict(first.get("summary", {})) if isinstance(first.get("summary", {}), dict) else {}
        last_summary = dict(last.get("summary", {})) if isinstance(last.get("summary", {}), dict) else {}
        score_delta = round(
            float(last_summary.get("weighted_score", 0.0) or 0.0)
            - float(first_summary.get("weighted_score", 0.0) or 0.0),
            6,
        )
        pass_delta = round(
            float(last_summary.get("weighted_pass_rate", 0.0) or 0.0)
            - float(first_summary.get("weighted_pass_rate", 0.0) or 0.0),
            6,
        )
        regression_run_count = sum(
            1
            for item in rows
            if str(dict(item.get("regression", {})).get("status", "") or "").strip().lower() == "regression"
        )
        recurring_pack_counts: Dict[str, int] = {}
        recurring_capability_counts: Dict[str, int] = {}
        for item in rows:
            regression = dict(item.get("regression", {})) if isinstance(item.get("regression", {}), dict) else {}
            for pack_row in regression.get("pack_regressions", []) if isinstance(regression.get("pack_regressions", []), list) else []:
                if isinstance(pack_row, dict):
                    self._increment_count(recurring_pack_counts, str(pack_row.get("name", "") or ""))
            for capability_row in regression.get("capability_regressions", []) if isinstance(regression.get("capability_regressions", []), list) else []:
                if isinstance(capability_row, dict):
                    self._increment_count(recurring_capability_counts, str(capability_row.get("name", "") or ""))
        direction = "stable"
        if score_delta > 0.04 or pass_delta > 0.05:
            direction = "improving"
        elif score_delta < -0.04 or pass_delta < -0.05:
            direction = "regressing"
        return {
            "run_count": len(rows),
            "direction": direction,
            "weighted_score_delta": score_delta,
            "weighted_pass_rate_delta": pass_delta,
            "regression_run_count": regression_run_count,
            "recent_regression_count": len(
                dict(last.get("regression", {})).get("scenario_regressions", [])
                if isinstance(last.get("regression", {}), dict)
                and isinstance(dict(last.get("regression", {})).get("scenario_regressions", []), list)
                else []
            ),
            "recurring_pack_regressions": self._sorted_count_map(recurring_pack_counts, limit=6),
            "recurring_capability_regressions": self._sorted_count_map(recurring_capability_counts, limit=6),
        }

    def _replay_candidates(self, rows: List[Dict[str, object]], *, filters: Dict[str, object]) -> List[Dict[str, object]]:
        ranked: List[Dict[str, object]] = []
        for row in rows:
            score = float(row.get("score", 0.0) or 0.0)
            replayable = bool(row.get("replayable", False))
            priority = (
                (0 if replayable else 1),
                score,
                -float(row.get("weight", 0.0) or 0.0),
                -max(1, int(row.get("horizon_steps", 1) or 1)),
                str(row.get("scenario", "") or ""),
            )
            reasons: List[str] = []
            if not bool(row.get("passed", False)):
                reasons.append("failed_latest_run")
            if bool(row.get("recovery_expected", False)):
                reasons.append("recovery_expected")
            if bool(row.get("native_hybrid_focus", False)):
                reasons.append("native_hybrid_focus")
            if max(1, int(row.get("horizon_steps", 1) or 1)) >= 4:
                reasons.append("long_horizon")
            ranked.append(
                {
                    "priority_key": priority,
                    "scenario": str(row.get("scenario", "") or "").strip(),
                    "user_text": str(row.get("user_text", "") or "").strip(),
                    "pack": str(row.get("pack", "") or "").strip(),
                    "category": str(row.get("category", "") or "").strip(),
                    "mission_family": str(row.get("mission_family", "") or "").strip(),
                    "risk_level": str(row.get("risk_level", "") or "").strip(),
                    "apps": list(row.get("apps", [])) if isinstance(row.get("apps", []), list) else [],
                    "score": round(score, 6),
                    "weight": round(float(row.get("weight", 0.0) or 0.0), 6),
                    "replayable": replayable,
                    "horizon_steps": max(1, int(row.get("horizon_steps", 1) or 1)),
                    "reasons": reasons,
                    "replay_query": {
                        **{key: value for key, value in filters.items() if key != "scenario_name" and key != "limit" and str(value or "").strip()},
                        "scenario_name": str(row.get("scenario", "") or "").strip(),
                        "limit": 1,
                    },
                }
            )
        ranked.sort(key=lambda item: item["priority_key"])
        return [
            {key: value for key, value in item.items() if key != "priority_key"}
            for item in ranked[:6]
        ]

    def _installed_app_coverage(self, scenarios: List[Scenario]) -> Dict[str, object]:
        provider = self.installed_app_catalog_provider
        if provider is None:
            return {"status": "unavailable", "message": "installed app catalog unavailable"}
        try:
            payload = provider(limit=800)
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}
        if not isinstance(payload, dict) or str(payload.get("status", "") or "").strip().lower() not in {"success"}:
            return {
                "status": "error",
                "message": str(payload.get("message", "") if isinstance(payload, dict) else "").strip() or "invalid installed app catalog payload",
            }
        items = payload.get("items", []) if isinstance(payload.get("items", []), list) else []
        installed_aliases: Dict[str, Dict[str, object]] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            app_names = {
                " ".join(str(item.get("name", "") or "").strip().lower().split()),
                " ".join(str(item.get("profile_id", "") or "").strip().lower().split()),
            }
            aliases = item.get("aliases", []) if isinstance(item.get("aliases", []), list) else []
            app_names.update(" ".join(str(alias or "").strip().lower().split()) for alias in aliases if str(alias or "").strip())
            app_names = {name for name in app_names if name}
            for name in app_names:
                installed_aliases[name] = dict(item)
        benchmarked_apps = {
            " ".join(str(app or "").strip().lower().split())
            for row in scenarios
            for app in row.apps
            if str(app or "").strip()
        }
        covered_profiles: Dict[str, Dict[str, object]] = {}
        missing_profiles: Dict[str, Dict[str, object]] = {}
        for key, item in installed_aliases.items():
            profile_id = str(item.get("profile_id", item.get("name", key)) or key).strip().lower()
            if key in benchmarked_apps:
                covered_profiles[profile_id] = dict(item)
            else:
                missing_profiles[profile_id] = dict(item)
        missing_category_counts: Dict[str, int] = {}
        for item in missing_profiles.values():
            self._increment_count(missing_category_counts, str(item.get("category", "general_desktop") or "general_desktop"))
        return {
            "status": "success",
            "installed_profile_count": int(payload.get("total", payload.get("count", len(items))) or len(items)),
            "benchmarked_installed_app_count": len(covered_profiles),
            "benchmarked_ratio": round(len(covered_profiles) / max(1, len(covered_profiles) + len(missing_profiles)), 6),
            "covered_apps": sorted(
                [str(item.get("name", item.get("profile_id", "")) or "").strip() for item in covered_profiles.values() if str(item.get("name", item.get("profile_id", "")) or "").strip()]
            )[:12],
            "missing_apps": sorted(
                [str(item.get("name", item.get("profile_id", "")) or "").strip() for item in missing_profiles.values() if str(item.get("name", item.get("profile_id", "")) or "").strip()]
            )[:12],
            "missing_category_counts": self._sorted_count_map(missing_category_counts, limit=8),
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
        replayable_weight = 0.0
        replayable_pass_weight = 0.0
        replayable_score_weight = 0.0
        long_horizon_weight = 0.0
        long_horizon_pass_weight = 0.0
        long_horizon_score_weight = 0.0
        max_horizon_steps = 0
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
            if bool(row.get("replayable", False)):
                replayable_weight += weight
                replayable_score_weight += weight * float(row.get("score", 0.0) or 0.0)
                if bool(row.get("passed", False)):
                    replayable_pass_weight += weight
            horizon_steps = max(1, int(row.get("horizon_steps", 1) or 1))
            max_horizon_steps = max(max_horizon_steps, horizon_steps)
            if horizon_steps >= 4:
                long_horizon_weight += weight
                long_horizon_score_weight += weight * float(row.get("score", 0.0) or 0.0)
                if bool(row.get("passed", False)):
                    long_horizon_pass_weight += weight
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
            "replayability_coverage": {
                "weighted_pass_rate": round(replayable_pass_weight / max(1e-9, replayable_weight), 6) if replayable_weight else 0.0,
                "weighted_score": round(replayable_score_weight / max(1e-9, replayable_weight), 6) if replayable_weight else 0.0,
                "weight": round(replayable_weight, 6),
            },
            "long_horizon_coverage": {
                "weighted_pass_rate": round(long_horizon_pass_weight / max(1e-9, long_horizon_weight), 6) if long_horizon_weight else 0.0,
                "weighted_score": round(long_horizon_score_weight / max(1e-9, long_horizon_weight), 6) if long_horizon_weight else 0.0,
                "weight": round(long_horizon_weight, 6),
                "max_horizon_steps": max_horizon_steps,
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

    @staticmethod
    def _first_candidate_name(rows: object) -> str:
        if not isinstance(rows, list):
            return ""
        for row in rows:
            if isinstance(row, dict):
                clean = str(row.get("name", "") or "").strip()
                if clean:
                    return clean
        return ""

    @staticmethod
    def _control_biases_from_summary(
        *,
        weakest_pack: str,
        weakest_category: str,
        weakest_capability: str,
        weakest_mission_family: str,
        recovery_focus: Dict[str, object],
        native_hybrid_focus: Dict[str, object],
    ) -> Dict[str, float]:
        biases = {
            "dialog_resolution": 0.12,
            "descendant_focus": 0.12,
            "navigation_branch": 0.1,
            "recovery_reacquire": 0.1,
            "loop_guard": 0.12,
            "native_focus": 0.1,
        }
        normalized_pack = str(weakest_pack or "").strip().lower()
        normalized_category = str(weakest_category or "").strip().lower()
        normalized_capability = str(weakest_capability or "").strip().lower()
        normalized_mission_family = str(weakest_mission_family or "").strip().lower()

        if normalized_pack == "unsupported_and_recovery":
            biases["dialog_resolution"] += 0.22
            biases["descendant_focus"] += 0.22
            biases["recovery_reacquire"] += 0.18
            biases["loop_guard"] += 0.2
            biases["native_focus"] += 0.2
        if normalized_pack == "installer_and_governance":
            biases["dialog_resolution"] += 0.18
            biases["recovery_reacquire"] += 0.18
            biases["descendant_focus"] += 0.12
        if normalized_category in {"unsupported_app", "installer", "settings"}:
            biases["dialog_resolution"] += 0.08
            biases["descendant_focus"] += 0.08
        if normalized_capability in {"surface_exploration", "child_window_adoption"}:
            biases["descendant_focus"] += 0.18
            biases["navigation_branch"] += 0.12
            biases["native_focus"] += 0.12
        if normalized_capability in {"desktop_recovery", "wizard_mission", "form_mission"}:
            biases["dialog_resolution"] += 0.14
            biases["recovery_reacquire"] += 0.2
            biases["loop_guard"] += 0.08
        if normalized_mission_family in {"exploration", "recovery", "wizard", "form"}:
            biases["recovery_reacquire"] += 0.08
            biases["loop_guard"] += 0.08
        recovery_score = float(recovery_focus.get("weighted_score", 1.0) or 1.0) if recovery_focus else 1.0
        native_score = float(native_hybrid_focus.get("weighted_score", 1.0) or 1.0) if native_hybrid_focus else 1.0
        if recovery_focus and recovery_score < 0.9:
            pressure = min(0.22, (0.9 - recovery_score) * 0.45)
            biases["dialog_resolution"] += pressure
            biases["recovery_reacquire"] += pressure
            biases["loop_guard"] += pressure * 0.75
        if native_hybrid_focus and native_score < 0.9:
            pressure = min(0.22, (0.9 - native_score) * 0.45)
            biases["native_focus"] += pressure
            biases["descendant_focus"] += pressure
            biases["recovery_reacquire"] += pressure * 0.75
        return {key: round(max(0.0, min(value, 1.0)), 6) for key, value in biases.items()}

    @staticmethod
    def _scenario_query_hints(*, scenario: Scenario) -> List[str]:
        hints: List[str] = []
        text = " ".join(str(scenario.user_text or "").strip().split()).lower()
        token_map = {
            "bluetooth": "bluetooth",
            "device": "device",
            "installer": "installer",
            "approval": "approval",
            "terminal": "terminal",
            "quick open": "quick open",
            "privacy": "privacy",
            "folder": "folder",
            "reply": "reply",
        }
        for phrase, hint in token_map.items():
            if phrase in text and hint not in hints:
                hints.append(hint)
        for tag in scenario.tags:
            clean = str(tag or "").strip().replace("_", " ")
            if clean and clean not in hints:
                hints.append(clean)
        return hints[:8]

    @staticmethod
    def _scenario_native_tactic_profile(*, scenario: Scenario) -> Dict[str, float]:
        profile = {
            "dialog_resolution": 0.14,
            "descendant_focus": 0.14,
            "navigation_branch": 0.12,
            "recovery_reacquire": 0.12,
            "loop_guard": 0.14,
            "native_focus": 0.12,
        }
        mission_family = str(scenario.mission_family or "").strip().lower()
        category = str(scenario.category or "").strip().lower()
        pack = str(scenario.pack or "").strip().lower()
        capabilities = {str(item or "").strip().lower() for item in scenario.capabilities if str(item or "").strip()}
        if mission_family in {"exploration", "recovery"} or pack in {"unsupported_and_recovery", "installer_and_governance"}:
            profile["dialog_resolution"] += 0.24
            profile["descendant_focus"] += 0.24
            profile["recovery_reacquire"] += 0.22
            profile["loop_guard"] += 0.18
            profile["native_focus"] += 0.16
        if mission_family in {"workflow", "form"} or category in {"editor_workflow", "file_manager", "settings"}:
            profile["navigation_branch"] += 0.18
        if "surface_exploration" in capabilities or "child_window_adoption" in capabilities:
            profile["descendant_focus"] += 0.22
            profile["dialog_resolution"] += 0.14
            profile["native_focus"] += 0.16
        if "wizard_mission" in capabilities or "desktop_recovery" in capabilities:
            profile["recovery_reacquire"] += 0.22
            profile["dialog_resolution"] += 0.14
        if "quick_open" in capabilities or "desktop_workflow" in capabilities:
            profile["navigation_branch"] += 0.16
        if "settings_control" in capabilities or "form_mission" in capabilities:
            profile["navigation_branch"] += 0.08
            profile["dialog_resolution"] += 0.08
        if max(1, int(scenario.horizon_steps or 1)) >= 4:
            profile["loop_guard"] += 0.12
            profile["navigation_branch"] += 0.08
            profile["native_focus"] += 0.08
        if bool(scenario.native_hybrid_focus):
            profile["native_focus"] += 0.14
        if bool(scenario.recovery_expected):
            profile["recovery_reacquire"] += 0.12
        return {
            key: round(max(0.0, min(float(value or 0.0), 1.0)), 6)
            for key, value in profile.items()
        }

    def _last_run_regression_payload(self) -> Dict[str, object]:
        return dict(self.last_run.get("regression", {})) if isinstance(self.last_run, dict) else {}
