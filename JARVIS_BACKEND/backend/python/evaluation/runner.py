from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List

from backend.python.core.contracts import GoalRecord, GoalRequest
from backend.python.core.planner import Planner
from backend.python.core.task_state import GoalStatus
from .benchmark_lab_memory import DesktopBenchmarkLabMemory
from .scenarios import Scenario, default_scenarios, scenario_catalog


class EvaluationRunner:
    def __init__(
        self,
        *,
        history_limit: int = 12,
        installed_app_catalog_provider: Callable[..., Dict[str, object]] | None = None,
        lab_memory: DesktopBenchmarkLabMemory | None = None,
    ) -> None:
        self.planner = Planner()
        self.history_limit = max(1, min(int(history_limit), 128))
        self.installed_app_catalog_provider = installed_app_catalog_provider
        self.lab_memory = lab_memory
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

    def lab_sessions(
        self,
        *,
        limit: int = 12,
        session_id: str = "",
        status: str = "",
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        return memory.session_history(limit=limit, session_id=session_id, status=status)

    def lab_campaigns(
        self,
        *,
        limit: int = 12,
        campaign_id: str = "",
        status: str = "",
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        return memory.campaign_history(limit=limit, campaign_id=campaign_id, status=status)

    def lab_programs(
        self,
        *,
        limit: int = 12,
        program_id: str = "",
        status: str = "",
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        return memory.program_history(limit=limit, program_id=program_id, status=status)

    def lab_portfolios(
        self,
        *,
        limit: int = 12,
        portfolio_id: str = "",
        status: str = "",
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        return memory.portfolio_history(limit=limit, portfolio_id=portfolio_id, status=status)

    def lab_portfolio_diagnostics(
        self,
        *,
        limit: int = 12,
        portfolio_id: str = "",
        status: str = "",
        history_limit: int = 8,
    ) -> Dict[str, object]:
        portfolio_payload = self.lab_portfolios(limit=limit, portfolio_id=portfolio_id, status=status)
        if str(portfolio_payload.get("status", "") or "").strip().lower() != "success":
            return portfolio_payload
        summary = (
            dict(portfolio_payload.get("summary", {}))
            if isinstance(portfolio_payload.get("summary", {}), dict)
            else {}
        )
        top_portfolios = [
            dict(item)
            for item in portfolio_payload.get("top_portfolios", [])
            if isinstance(item, dict)
        ] if isinstance(portfolio_payload.get("top_portfolios", []), list) else []
        native_targets_payload = self.native_control_targets(
            limit=max(12, min(int(limit or 12) * 2, 48)),
            history_limit=max(1, min(int(history_limit or 8), 64)),
        )
        guidance_payload = self.control_guidance()
        app_pressure_leaderboard: List[Dict[str, object]] = []
        for row in native_targets_payload.get("target_apps", []) if isinstance(native_targets_payload.get("target_apps", []), list) else []:
            if not isinstance(row, dict):
                continue
            app_name = str(row.get("app_name", "") or "").strip()
            if not app_name:
                continue
            total_pressure = round(
                float(row.get("portfolio_pressure", 0.0) or 0.0)
                + float(row.get("program_pressure", 0.0) or 0.0)
                + float(row.get("campaign_pressure", 0.0) or 0.0)
                + float(row.get("replay_pressure", 0.0) or 0.0),
                6,
            )
            app_pressure_leaderboard.append(
                {
                    "app_name": app_name,
                    "total_pressure": total_pressure,
                    "portfolio_pressure": round(float(row.get("portfolio_pressure", 0.0) or 0.0), 6),
                    "program_pressure": round(float(row.get("program_pressure", 0.0) or 0.0), 6),
                    "campaign_pressure": round(float(row.get("campaign_pressure", 0.0) or 0.0), 6),
                    "replay_pressure": round(float(row.get("replay_pressure", 0.0) or 0.0), 6),
                    "priority": round(float(row.get("priority", 0.0) or 0.0), 6),
                    "pending_program_count": int(row.get("portfolio_pending_program_count", 0) or 0),
                    "pending_campaign_count": int(row.get("portfolio_pending_campaign_count", 0) or 0)
                    + int(row.get("program_pending_campaign_count", 0) or 0),
                    "pending_session_count": int(row.get("portfolio_pending_session_count", 0) or 0)
                    + int(row.get("campaign_pending_session_count", 0) or 0),
                    "pending_app_target_count": int(row.get("portfolio_pending_app_target_count", 0) or 0)
                    + int(row.get("program_pending_app_target_count", 0) or 0)
                    + int(row.get("campaign_pending_app_target_count", 0) or 0),
                    "latest_portfolio_status": str(row.get("portfolio_latest_wave_status", "") or "idle"),
                    "latest_stop_reason": str(
                        row.get("portfolio_latest_wave_stop_reason", "")
                        or row.get("program_latest_cycle_stop_reason", "")
                        or row.get("campaign_latest_sweep_regression_status", "")
                        or row.get("campaign_latest_sweep_status", "")
                        or "idle"
                    ),
                    "focus_summary": list(row.get("campaign_focus_summary", []))[:6]
                    if isinstance(row.get("campaign_focus_summary", []), list)
                    else [],
                    "hint_query": str(
                        row.get("portfolio_hint_query", "")
                        or row.get("program_hint_query", "")
                        or row.get("campaign_hint_query", "")
                        or row.get("hint_query", "")
                        or ""
                    ),
                }
            )
        app_pressure_leaderboard.sort(
            key=lambda item: (
                -float(item.get("total_pressure", 0.0) or 0.0),
                -float(item.get("priority", 0.0) or 0.0),
                str(item.get("app_name", "") or ""),
            )
        )
        stop_reason_leaderboard = self._count_map_leaderboard(
            summary.get("wave_stop_reason_counts", {}),
            label_key="stop_reason",
        )
        focus_leaderboard = self._count_map_leaderboard(
            summary.get("focus_summary_counts", {}),
            label_key="focus_area",
        )
        trend_leaderboard = self._count_map_leaderboard(
            summary.get("trend_direction_counts", {}),
            label_key="direction",
        )
        app_target_leaderboard = self._count_map_leaderboard(
            summary.get("app_target_counts", {}),
            label_key="app_name",
        )
        backlog = {
            "pending_programs": int(summary.get("pending_programs", 0) or 0),
            "attention_programs": int(summary.get("attention_programs", 0) or 0),
            "pending_campaigns": int(summary.get("pending_campaigns", 0) or 0),
            "pending_sessions": int(summary.get("pending_sessions", 0) or 0),
            "pending_app_targets": int(summary.get("pending_app_targets", 0) or 0),
            "long_horizon_pending_count": int(summary.get("long_horizon_pending_count", 0) or 0),
            "stable_waves": int(summary.get("stable_waves", 0) or 0),
            "regression_waves": int(summary.get("regression_waves", 0) or 0),
        }
        return {
            "status": "success",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "filters": {
                "limit": max(1, int(limit or 12)),
                "portfolio_id": str(portfolio_id or "").strip(),
                "status": str(status or "").strip(),
                "history_limit": max(1, min(int(history_limit or 8), 64)),
            },
            "portfolios": portfolio_payload,
            "summary": {
                "portfolio_count": int(portfolio_payload.get("count", 0) or 0),
                "portfolio_total": int(portfolio_payload.get("total", 0) or 0),
                "portfolio_pressure_total": round(float(summary.get("portfolio_pressure_total", 0.0) or 0.0), 6),
                "portfolio_pressure_avg": round(float(summary.get("portfolio_pressure_avg", 0.0) or 0.0), 6),
                "top_app_name": str(dict(app_pressure_leaderboard[0]).get("app_name", "") or "") if app_pressure_leaderboard else "",
                "top_stop_reason": str(dict(stop_reason_leaderboard[0]).get("stop_reason", "") or "") if stop_reason_leaderboard else "",
                "top_focus_area": str(dict(focus_leaderboard[0]).get("focus_area", "") or "") if focus_leaderboard else "",
            },
            "backlog": backlog,
            "top_portfolios": top_portfolios,
            "app_pressure_leaderboard": app_pressure_leaderboard[:6],
            "app_target_leaderboard": app_target_leaderboard[:6],
            "stop_reason_leaderboard": stop_reason_leaderboard[:6],
            "focus_leaderboard": focus_leaderboard[:6],
            "trend_leaderboard": trend_leaderboard[:6],
            "native_targets": native_targets_payload,
            "guidance": guidance_payload,
        }

    def create_lab_campaign(
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
        source: str = "",
        label: str = "",
        max_sessions: int = 4,
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        normalized_max_sessions = max(1, min(int(max_sessions or 4), 8))
        effective_pack = str(pack or "").strip()
        if (
            not effective_pack
            and not str(scenario_name or "").strip()
            and not str(category or "").strip()
            and not str(capability or "").strip()
            and not str(risk_level or "").strip()
            and not str(autonomy_tier or "").strip()
            and not str(mission_family or "").strip()
        ):
            effective_pack = "long_horizon_and_replay"
        filters = self._filters_payload(
            scenario_name=scenario_name,
            pack=effective_pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
        )
        lab_payload = self.lab(
            scenario_name=scenario_name,
            pack=effective_pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
            history_limit=history_limit,
        )
        native_targets_payload = self.native_control_targets(
            scenario_name=scenario_name,
            pack=effective_pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
            history_limit=history_limit,
        )
        guidance_payload = self.control_guidance()
        target_app_rows = [
            dict(item)
            for item in native_targets_payload.get("target_apps", [])
            if isinstance(item, dict)
        ] if isinstance(native_targets_payload.get("target_apps", []), list) else []
        target_apps = self._unique_strings(
            [
                *([str(app).strip()] if str(app or "").strip() else []),
                *(
                    str(item.get("app_name", "") or "").strip()
                    for item in target_app_rows
                    if str(item.get("app_name", "") or "").strip()
                ),
            ]
        )[: max(normalized_max_sessions, 1)]
        created_sessions: List[Dict[str, object]] = []
        session_ids: List[str] = []
        session_rows: List[Dict[str, object]] = []
        if not target_apps:
            session_payload = self.create_lab_session(
                scenario_name=scenario_name,
                pack=effective_pack,
                category=category,
                capability=capability,
                risk_level=risk_level,
                autonomy_tier=autonomy_tier,
                mission_family=mission_family,
                app=app,
                limit=limit,
                history_limit=history_limit,
                source=source or "benchmark_campaign",
                label=str(label or "").strip(),
            )
            if str(session_payload.get("status", "") or "").strip().lower() == "success":
                session = dict(session_payload.get("session", {})) if isinstance(session_payload.get("session", {}), dict) else {}
                if session:
                    created_sessions.append(dict(session_payload))
                    session_rows.append(session)
                    session_id = str(session.get("session_id", "") or "").strip()
                    if session_id:
                        session_ids.append(session_id)
        else:
            base_label = str(label or "").strip()
            for target_app in target_apps[:normalized_max_sessions]:
                session_label = (
                    f"{base_label} / {target_app}"
                    if base_label and len(target_apps) > 1
                    else (base_label or f"{target_app} replay lab")
                )
                session_payload = self.create_lab_session(
                    scenario_name=scenario_name,
                    pack=effective_pack,
                    category=category,
                    capability=capability,
                    risk_level=risk_level,
                    autonomy_tier=autonomy_tier,
                    mission_family=mission_family,
                    app=target_app,
                    limit=limit,
                    history_limit=history_limit,
                    source=source or "benchmark_campaign",
                    label=session_label,
                )
                if str(session_payload.get("status", "") or "").strip().lower() != "success":
                    continue
                session = dict(session_payload.get("session", {})) if isinstance(session_payload.get("session", {}), dict) else {}
                if not session:
                    continue
                created_sessions.append(dict(session_payload))
                session_rows.append(session)
                session_id = str(session.get("session_id", "") or "").strip()
                if session_id:
                    session_ids.append(session_id)
        campaign_payload = memory.record_campaign(
            filters=filters,
            lab_payload=lab_payload,
            native_targets_payload=native_targets_payload,
            guidance_payload=guidance_payload,
            source=source or "benchmark_campaign",
            label=label,
            session_ids=session_ids,
            app_targets=target_apps,
            session_rows=session_rows,
        )
        return {
            "status": str(campaign_payload.get("status", "success") or "success"),
            "campaign": dict(campaign_payload.get("campaign", {})) if isinstance(campaign_payload.get("campaign", {}), dict) else {},
            "created_sessions": created_sessions,
            "created_session_count": len(created_sessions),
            "lab": lab_payload,
            "native_targets": native_targets_payload,
            "guidance": guidance_payload,
        }

    def create_lab_program(
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
        source: str = "",
        label: str = "",
        max_campaigns: int = 3,
        max_sessions_per_campaign: int = 3,
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        normalized_max_campaigns = max(1, min(int(max_campaigns or 3), 6))
        normalized_max_sessions = max(1, min(int(max_sessions_per_campaign or 3), 6))
        effective_pack = str(pack or "").strip()
        if (
            not effective_pack
            and not str(scenario_name or "").strip()
            and not str(category or "").strip()
            and not str(capability or "").strip()
            and not str(risk_level or "").strip()
            and not str(autonomy_tier or "").strip()
            and not str(mission_family or "").strip()
        ):
            effective_pack = "long_horizon_and_replay"
        filters = self._filters_payload(
            scenario_name=scenario_name,
            pack=effective_pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
        )
        lab_payload = self.lab(
            scenario_name=scenario_name,
            pack=effective_pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
            history_limit=history_limit,
        )
        native_targets_payload = self.native_control_targets(
            scenario_name=scenario_name,
            pack=effective_pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
            history_limit=history_limit,
        )
        guidance_payload = self.control_guidance()
        target_rows = [
            dict(item)
            for item in native_targets_payload.get("target_apps", [])
            if isinstance(item, dict)
        ] if isinstance(native_targets_payload.get("target_apps", []), list) else []
        target_apps = self._unique_strings(
            [
                *([str(app).strip()] if str(app or "").strip() else []),
                *(
                    str(item.get("app_name", "") or "").strip()
                    for item in target_rows
                    if str(item.get("app_name", "") or "").strip()
                ),
            ]
        )[:normalized_max_campaigns]
        created_campaigns: List[Dict[str, object]] = []
        campaign_ids: List[str] = []
        campaign_rows: List[Dict[str, object]] = []
        base_label = str(label or "").strip()
        if not target_apps:
            campaign_payload = self.create_lab_campaign(
                scenario_name=scenario_name,
                pack=effective_pack,
                category=category,
                capability=capability,
                risk_level=risk_level,
                autonomy_tier=autonomy_tier,
                mission_family=mission_family,
                app=app,
                limit=limit,
                history_limit=history_limit,
                source=source or "benchmark_program",
                label=base_label,
                max_sessions=normalized_max_sessions,
            )
            if str(campaign_payload.get("status", "") or "").strip().lower() == "success":
                campaign = dict(campaign_payload.get("campaign", {})) if isinstance(campaign_payload.get("campaign", {}), dict) else {}
                if campaign:
                    created_campaigns.append(dict(campaign_payload))
                    campaign_rows.append(campaign)
                    campaign_id = str(campaign.get("campaign_id", "") or "").strip()
                    if campaign_id:
                        campaign_ids.append(campaign_id)
        else:
            for target_app in target_apps:
                campaign_label = (
                    f"{base_label} / {target_app}"
                    if base_label and len(target_apps) > 1
                    else (base_label or f"{target_app} replay campaign")
                )
                campaign_payload = self.create_lab_campaign(
                    scenario_name=scenario_name,
                    pack=effective_pack,
                    category=category,
                    capability=capability,
                    risk_level=risk_level,
                    autonomy_tier=autonomy_tier,
                    mission_family=mission_family,
                    app=target_app,
                    limit=limit,
                    history_limit=history_limit,
                    source=source or "benchmark_program",
                    label=campaign_label,
                    max_sessions=normalized_max_sessions,
                )
                if str(campaign_payload.get("status", "") or "").strip().lower() != "success":
                    continue
                campaign = dict(campaign_payload.get("campaign", {})) if isinstance(campaign_payload.get("campaign", {}), dict) else {}
                if not campaign:
                    continue
                created_campaigns.append(dict(campaign_payload))
                campaign_rows.append(campaign)
                campaign_id = str(campaign.get("campaign_id", "") or "").strip()
                if campaign_id:
                    campaign_ids.append(campaign_id)
        program_payload = memory.record_program(
            filters=filters,
            lab_payload=lab_payload,
            native_targets_payload=native_targets_payload,
            guidance_payload=guidance_payload,
            source=source or "benchmark_program",
            label=label,
            campaign_ids=campaign_ids,
            app_targets=target_apps,
            campaign_rows=[dict(item) for item in campaign_rows],
        )
        created_session_count = sum(
            int(item.get("created_session_count", 0) or 0)
            for item in created_campaigns
            if isinstance(item, dict)
        )
        return {
            "status": str(program_payload.get("status", "success") or "success"),
            "program": dict(program_payload.get("program", {})) if isinstance(program_payload.get("program", {}), dict) else {},
            "created_campaigns": created_campaigns,
            "created_campaign_count": len(created_campaigns),
            "created_session_count": created_session_count,
            "lab": lab_payload,
            "native_targets": native_targets_payload,
            "guidance": guidance_payload,
        }

    def create_lab_portfolio(
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
        source: str = "",
        label: str = "",
        max_programs: int = 3,
        max_campaigns_per_program: int = 3,
        max_sessions_per_campaign: int = 3,
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        normalized_max_programs = max(1, min(int(max_programs or 3), 6))
        normalized_max_campaigns = max(1, min(int(max_campaigns_per_program or 3), 6))
        normalized_max_sessions = max(1, min(int(max_sessions_per_campaign or 3), 6))
        effective_pack = str(pack or "").strip()
        if (
            not effective_pack
            and not str(scenario_name or "").strip()
            and not str(category or "").strip()
            and not str(capability or "").strip()
            and not str(risk_level or "").strip()
            and not str(autonomy_tier or "").strip()
            and not str(mission_family or "").strip()
        ):
            effective_pack = "long_horizon_and_replay"
        filters = self._filters_payload(
            scenario_name=scenario_name,
            pack=effective_pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
        )
        lab_payload = self.lab(
            scenario_name=scenario_name,
            pack=effective_pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
            history_limit=history_limit,
        )
        native_targets_payload = self.native_control_targets(
            scenario_name=scenario_name,
            pack=effective_pack,
            category=category,
            capability=capability,
            risk_level=risk_level,
            autonomy_tier=autonomy_tier,
            mission_family=mission_family,
            app=app,
            limit=limit,
            history_limit=history_limit,
        )
        guidance_payload = self.control_guidance()
        target_app_rows = [
            dict(item)
            for item in native_targets_payload.get("target_apps", [])
            if isinstance(item, dict)
        ] if isinstance(native_targets_payload.get("target_apps", []), list) else []
        target_apps = self._unique_strings(
            [
                *([str(app).strip()] if str(app or "").strip() else []),
                *(
                    str(item.get("app_name", "") or "").strip()
                    for item in target_app_rows
                    if str(item.get("app_name", "") or "").strip()
                ),
            ]
        )[: max(normalized_max_programs, 1)]
        created_programs: List[Dict[str, object]] = []
        program_ids: List[str] = []
        program_rows: List[Dict[str, object]] = []
        if not target_apps:
            program_payload = self.create_lab_program(
                scenario_name=scenario_name,
                pack=effective_pack,
                category=category,
                capability=capability,
                risk_level=risk_level,
                autonomy_tier=autonomy_tier,
                mission_family=mission_family,
                app=app,
                limit=limit,
                history_limit=history_limit,
                source=source or "benchmark_portfolio",
                label=str(label or "").strip(),
                max_campaigns=normalized_max_campaigns,
                max_sessions_per_campaign=normalized_max_sessions,
            )
            if str(program_payload.get("status", "") or "").strip().lower() == "success":
                program = dict(program_payload.get("program", {})) if isinstance(program_payload.get("program", {}), dict) else {}
                if program:
                    created_programs.append(dict(program_payload))
                    program_rows.append(program)
                    program_id = str(program.get("program_id", "") or "").strip()
                    if program_id:
                        program_ids.append(program_id)
        else:
            base_label = str(label or "").strip()
            for target_app in target_apps[:normalized_max_programs]:
                program_label = (
                    f"{base_label} / {target_app}"
                    if base_label and len(target_apps) > 1
                    else (base_label or f"{target_app} replay portfolio")
                )
                program_payload = self.create_lab_program(
                    scenario_name=scenario_name,
                    pack=effective_pack,
                    category=category,
                    capability=capability,
                    risk_level=risk_level,
                    autonomy_tier=autonomy_tier,
                    mission_family=mission_family,
                    app=target_app,
                    limit=limit,
                    history_limit=history_limit,
                    source=source or "benchmark_portfolio",
                    label=program_label,
                    max_campaigns=normalized_max_campaigns,
                    max_sessions_per_campaign=normalized_max_sessions,
                )
                if str(program_payload.get("status", "") or "").strip().lower() != "success":
                    continue
                program = dict(program_payload.get("program", {})) if isinstance(program_payload.get("program", {}), dict) else {}
                if not program:
                    continue
                created_programs.append(dict(program_payload))
                program_rows.append(program)
                program_id = str(program.get("program_id", "") or "").strip()
                if program_id:
                    program_ids.append(program_id)
        portfolio_payload = memory.record_portfolio(
            filters=filters,
            lab_payload=lab_payload,
            native_targets_payload=native_targets_payload,
            guidance_payload=guidance_payload,
            source=source or "benchmark_portfolio",
            label=label,
            program_ids=program_ids,
            app_targets=target_apps,
            program_rows=[dict(item) for item in program_rows],
        )
        return {
            "status": str(portfolio_payload.get("status", "success") or "success"),
            "portfolio": dict(portfolio_payload.get("portfolio", {})) if isinstance(portfolio_payload.get("portfolio", {}), dict) else {},
            "created_programs": created_programs,
            "created_program_count": len(created_programs),
            "created_campaign_count": sum(
                int(item.get("created_campaign_count", 0) or 0)
                for item in created_programs
                if isinstance(item, dict)
            ),
            "created_session_count": sum(
                int(item.get("created_session_count", 0) or 0)
                for item in created_programs
                if isinstance(item, dict)
            ),
            "lab": lab_payload,
            "native_targets": native_targets_payload,
            "guidance": guidance_payload,
        }

    def run_lab_campaign_sweep(
        self,
        *,
        campaign_id: str,
        max_sessions: int = 3,
        max_replays_per_session: int = 2,
        history_limit: int = 8,
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        campaign_payload = memory.get_campaign(campaign_id)
        campaign = dict(campaign_payload.get("campaign", {})) if isinstance(campaign_payload.get("campaign", {}), dict) else {}
        if not campaign:
            return {"status": "error", "message": str(campaign_payload.get("message", "") or "benchmark lab campaign not found")}
        filters = dict(campaign.get("filters", {})) if isinstance(campaign.get("filters", {}), dict) else {}
        normalized_max_sessions = max(1, min(int(max_sessions or 3), 8))
        normalized_max_replays = max(1, min(int(max_replays_per_session or 2), 8))
        normalized_history_limit = max(1, min(int(history_limit or 8), 64))
        campaign_session_ids = self._unique_strings(
            [
                str(item).strip()
                for item in campaign.get("session_ids", [])
                if str(item).strip()
            ]
        ) if isinstance(campaign.get("session_ids", []), list) else []
        current_sessions: List[Dict[str, object]] = []
        for session_id in campaign_session_ids:
            session_payload = memory.get_session(session_id)
            if isinstance(session_payload.get("session", {}), dict):
                current_sessions.append(dict(session_payload["session"]))
        represented_apps = {
            str(session.get("filters", {}).get("app", session.get("filters", {}).get("app_name", "")) or "").strip().lower()
            for session in current_sessions
            if isinstance(session.get("filters", {}), dict)
            and str(session.get("filters", {}).get("app", session.get("filters", {}).get("app_name", "")) or "").strip()
        }
        app_targets = self._unique_strings(
            [
                str(item).strip()
                for item in campaign.get("app_targets", campaign.get("target_apps", []))
                if str(item).strip()
            ]
        ) if isinstance(campaign.get("app_targets", campaign.get("target_apps", [])), list) else []
        created_sessions: List[Dict[str, object]] = []
        for target_app in app_targets:
            if target_app.strip().lower() in represented_apps:
                continue
            if len(current_sessions) + len(created_sessions) >= max(normalized_max_sessions, len(current_sessions)):
                break
            create_payload = self.create_lab_session(
                scenario_name=str(filters.get("scenario_name", "") or "").strip(),
                pack=str(filters.get("pack", "") or "").strip(),
                category=str(filters.get("category", "") or "").strip(),
                capability=str(filters.get("capability", "") or "").strip(),
                risk_level=str(filters.get("risk_level", "") or "").strip(),
                autonomy_tier=str(filters.get("autonomy_tier", "") or "").strip(),
                mission_family=str(filters.get("mission_family", "") or "").strip(),
                app=target_app,
                limit=int(filters.get("limit", 200) or 200),
                history_limit=normalized_history_limit,
                source="benchmark_campaign_sweep",
                label=f"{str(campaign.get('label', '') or 'replay campaign').strip()} / {target_app}",
            )
            if str(create_payload.get("status", "") or "").strip().lower() != "success":
                continue
            created_sessions.append(dict(create_payload))
            if isinstance(create_payload.get("session", {}), dict):
                current_sessions.append(dict(create_payload["session"]))
                represented_apps.add(target_app.strip().lower())
                session_id = str(create_payload["session"].get("session_id", "") or "").strip()
                if session_id and session_id not in campaign_session_ids:
                    campaign_session_ids.append(session_id)
        ranked_sessions = sorted(
            current_sessions,
            key=lambda session: (
                1 if str(session.get("status", "") or "").strip().lower() == "attention" else 0,
                int(session.get("failed_replay_count", 0) or 0),
                int(session.get("pending_replay_count", 0) or 0),
                int(session.get("long_horizon_pending_count", 0) or 0),
                int(session.get("regression_cycle_count", 0) or 0),
                -int(session.get("cycle_count", 0) or 0),
            ),
            reverse=True,
        )
        selected_sessions = ranked_sessions[:normalized_max_sessions]
        session_results: List[Dict[str, object]] = []
        for session in selected_sessions:
            session_id = str(session.get("session_id", "") or "").strip()
            if not session_id:
                continue
            cycle_payload = self.run_lab_session_cycle(session_id=session_id, history_limit=normalized_history_limit)
            latest_session = dict(cycle_payload.get("session", {})) if isinstance(cycle_payload.get("session", {}), dict) else dict(session)
            advance_payload: Dict[str, object] = {}
            if int(latest_session.get("pending_replay_count", 0) or 0) > 0:
                advance_payload = self.advance_lab_session(session_id=session_id, max_replays=normalized_max_replays)
                if isinstance(advance_payload.get("session", {}), dict):
                    latest_session = dict(advance_payload["session"])
            session_results.append(
                {
                    "session_id": session_id,
                    "label": str(latest_session.get("label", session.get("label", "")) or "").strip(),
                    "status": str(advance_payload.get("status", cycle_payload.get("status", latest_session.get("status", "success"))) or latest_session.get("status", "success")).strip() or "success",
                    "cycle_status": str(cycle_payload.get("status", "") or "success").strip() or "success",
                    "advance_status": str(advance_payload.get("status", "") or "").strip(),
                    "pending_replay_count": int(latest_session.get("pending_replay_count", 0) or 0),
                    "failed_replay_count": int(latest_session.get("failed_replay_count", 0) or 0),
                    "regression_cycle_count": int(latest_session.get("regression_cycle_count", 0) or 0),
                    "latest_cycle_regression_status": str(latest_session.get("latest_cycle_regression_status", latest_session.get("latest_cycle_status", "")) or "").strip(),
                }
            )
        refreshed_sessions: List[Dict[str, object]] = []
        for session_id in campaign_session_ids:
            session_payload = memory.get_session(session_id)
            if isinstance(session_payload.get("session", {}), dict):
                refreshed_sessions.append(dict(session_payload["session"]))
        lab_payload = self.lab(
            scenario_name=str(filters.get("scenario_name", "") or "").strip(),
            pack=str(filters.get("pack", "") or "").strip(),
            category=str(filters.get("category", "") or "").strip(),
            capability=str(filters.get("capability", "") or "").strip(),
            risk_level=str(filters.get("risk_level", "") or "").strip(),
            autonomy_tier=str(filters.get("autonomy_tier", "") or "").strip(),
            mission_family=str(filters.get("mission_family", "") or "").strip(),
            app=str(filters.get("app", filters.get("app_name", "")) or "").strip(),
            limit=int(filters.get("limit", 200) or 200),
            history_limit=normalized_history_limit,
        )
        native_targets_payload = self.native_control_targets(
            scenario_name=str(filters.get("scenario_name", "") or "").strip(),
            pack=str(filters.get("pack", "") or "").strip(),
            category=str(filters.get("category", "") or "").strip(),
            capability=str(filters.get("capability", "") or "").strip(),
            risk_level=str(filters.get("risk_level", "") or "").strip(),
            autonomy_tier=str(filters.get("autonomy_tier", "") or "").strip(),
            mission_family=str(filters.get("mission_family", "") or "").strip(),
            app=str(filters.get("app", filters.get("app_name", "")) or "").strip(),
            limit=int(filters.get("limit", 200) or 200),
            history_limit=normalized_history_limit,
        )
        guidance_payload = self.control_guidance()
        regression_status = (
            "regression"
            if any(str(item.get("latest_cycle_regression_status", "") or "").strip().lower() in {"regression", "failed"} for item in session_results)
            else "stable"
        )
        sweep_update = memory.record_campaign_sweep(
            campaign_id=str(campaign.get("campaign_id", campaign_id) or campaign_id).strip(),
            sweep_payload={
                "status": "success",
                "regression_status": regression_status,
                "executed_at": datetime.now(timezone.utc).isoformat(),
                "executed_session_count": len(session_results),
                "created_session_count": len(created_sessions),
                "pending_session_count": sum(1 for item in refreshed_sessions if str(item.get("status", "") or "").strip().lower() != "complete"),
                "attention_session_count": sum(1 for item in refreshed_sessions if str(item.get("status", "") or "").strip().lower() == "attention"),
                "long_horizon_pending_count": sum(int(item.get("long_horizon_pending_count", 0) or 0) for item in refreshed_sessions),
                "pending_app_target_count": max(
                    0,
                    len(app_targets)
                    - len(
                        {
                            str(item.get("filters", {}).get("app", item.get("filters", {}).get("app_name", "")) or "").strip().lower()
                            for item in refreshed_sessions
                            if isinstance(item.get("filters", {}), dict)
                            and str(item.get("filters", {}).get("app", item.get("filters", {}).get("app_name", "")) or "").strip()
                        }
                    ),
                ),
                "weighted_score": round(
                    float(dict(lab_payload.get("latest_summary", {})).get("weighted_score", 0.0) or 0.0)
                    if isinstance(lab_payload.get("latest_summary", {}), dict)
                    else 0.0,
                    6,
                ),
                "weighted_pass_rate": round(
                    float(dict(lab_payload.get("latest_summary", {})).get("weighted_pass_rate", 0.0) or 0.0)
                    if isinstance(lab_payload.get("latest_summary", {}), dict)
                    else 0.0,
                    6,
                ),
                "history_direction": str(
                    dict(lab_payload.get("history_trend", {})).get("direction", "")
                    if isinstance(lab_payload.get("history_trend", {}), dict)
                    else ""
                ).strip().lower(),
                "query": {
                    **filters,
                    "history_limit": normalized_history_limit,
                    "max_sessions": normalized_max_sessions,
                    "max_replays_per_session": normalized_max_replays,
                },
            },
            lab_payload=lab_payload,
            native_targets_payload=native_targets_payload,
            guidance_payload=guidance_payload,
            session_ids=campaign_session_ids,
            app_targets=app_targets,
            session_rows=refreshed_sessions,
        )
        return {
            "status": str(sweep_update.get("status", "success") or "success"),
            "campaign": dict(sweep_update.get("campaign", {})) if isinstance(sweep_update.get("campaign", {}), dict) else {},
            "sweep": dict(sweep_update.get("sweep", {})) if isinstance(sweep_update.get("sweep", {}), dict) else {},
            "results": session_results,
            "created_sessions": created_sessions,
            "created_session_count": len(created_sessions),
            "lab": lab_payload,
            "native_targets": native_targets_payload,
            "guidance": guidance_payload,
        }

    def run_lab_campaign_cycle(
        self,
        *,
        campaign_id: str,
        max_sweeps: int = 2,
        max_sessions: int = 3,
        max_replays_per_session: int = 2,
        history_limit: int = 8,
        stop_on_stable: bool = True,
    ) -> Dict[str, object]:
        normalized_max_sweeps = max(1, min(int(max_sweeps or 2), 8))
        normalized_max_sessions = max(1, min(int(max_sessions or 3), 8))
        normalized_max_replays = max(1, min(int(max_replays_per_session or 2), 8))
        normalized_history_limit = max(1, min(int(history_limit or 8), 64))
        cycle_results: List[Dict[str, object]] = []
        stop_reason = "max_sweeps_reached"
        final_payload: Dict[str, object] = {}
        previous_signature: tuple[int, int, int, int, str] | None = None

        for sweep_index in range(normalized_max_sweeps):
            sweep_payload = self.run_lab_campaign_sweep(
                campaign_id=campaign_id,
                max_sessions=normalized_max_sessions,
                max_replays_per_session=normalized_max_replays,
                history_limit=normalized_history_limit,
            )
            final_payload = dict(sweep_payload)
            campaign_row = (
                dict(sweep_payload.get("campaign", {}))
                if isinstance(sweep_payload.get("campaign", {}), dict)
                else {}
            )
            sweep_row = dict(sweep_payload.get("sweep", {})) if isinstance(sweep_payload.get("sweep", {}), dict) else {}
            trend_summary = (
                dict(campaign_row.get("trend_summary", {}))
                if isinstance(campaign_row.get("trend_summary", {}), dict)
                else {}
            )
            latest_regression = str(
                campaign_row.get("latest_sweep_regression_status", campaign_row.get("latest_sweep_status", ""))
                or ""
            ).strip().lower()
            cycle_results.append(
                {
                    "index": sweep_index + 1,
                    "status": str(sweep_payload.get("status", campaign_row.get("status", "success")) or "success").strip() or "success",
                    "campaign_id": str(campaign_row.get("campaign_id", campaign_id) or campaign_id).strip(),
                    "label": str(campaign_row.get("label", "") or "").strip(),
                    "pending_session_count": int(campaign_row.get("pending_session_count", 0) or 0),
                    "attention_session_count": int(campaign_row.get("attention_session_count", 0) or 0),
                    "pending_app_target_count": int(campaign_row.get("pending_app_target_count", 0) or 0),
                    "long_horizon_pending_count": int(campaign_row.get("long_horizon_pending_count", 0) or 0),
                    "latest_sweep_status": latest_regression,
                    "weighted_score": round(float(sweep_row.get("weighted_score", campaign_row.get("latest_sweep_score", 0.0)) or 0.0), 6),
                    "weighted_pass_rate": round(float(sweep_row.get("weighted_pass_rate", campaign_row.get("latest_sweep_pass_rate", 0.0)) or 0.0), 6),
                    "trend_direction": str(trend_summary.get("direction", campaign_row.get("history_direction", "")) or "").strip().lower(),
                    "campaign_priority": str(campaign_row.get("campaign_priority", "") or "").strip().lower(),
                    "created_session_count": int(sweep_payload.get("created_session_count", 0) or 0),
                    "executed_session_count": int(sweep_row.get("executed_session_count", 0) or 0),
                }
            )
            if str(sweep_payload.get("status", "") or "").strip().lower() == "error":
                stop_reason = "error"
                break
            signature = (
                int(campaign_row.get("pending_session_count", 0) or 0),
                int(campaign_row.get("attention_session_count", 0) or 0),
                int(campaign_row.get("pending_app_target_count", 0) or 0),
                int(campaign_row.get("long_horizon_pending_count", 0) or 0),
                latest_regression,
            )
            if (
                stop_on_stable
                and signature[0] <= 0
                and signature[1] <= 0
                and signature[2] <= 0
                and latest_regression not in {"regression", "failed", "error"}
            ):
                stop_reason = "stable"
                break
            if previous_signature is not None and signature == previous_signature:
                stop_reason = "no_additional_progress"
                break
            previous_signature = signature

        final_campaign = (
            dict(final_payload.get("campaign", {}))
            if isinstance(final_payload.get("campaign", {}), dict)
            else {}
        )
        cycle_summary = {
            "cycle_count": len(cycle_results),
            "max_sweeps": normalized_max_sweeps,
            "stop_reason": stop_reason,
            "stable": stop_reason == "stable",
            "executed_sweep_count": len(cycle_results),
            "executed_session_count": sum(int(item.get("executed_session_count", 0) or 0) for item in cycle_results),
            "created_session_count": sum(int(item.get("created_session_count", 0) or 0) for item in cycle_results),
            "trend_direction": str(
                dict(final_campaign.get("trend_summary", {})).get("direction", final_campaign.get("history_direction", ""))
                if isinstance(final_campaign.get("trend_summary", {}), dict)
                else final_campaign.get("history_direction", "")
            ).strip().lower(),
            "campaign_priority": str(final_campaign.get("campaign_priority", "") or "").strip().lower(),
        }
        return {
            "status": str(final_payload.get("status", "success") or "success"),
            "message": f"campaign cycle executed {len(cycle_results)} sweep(s) | stop:{stop_reason}",
            "campaign": final_campaign,
            "cycle": cycle_summary,
            "results": cycle_results,
            "lab": dict(final_payload.get("lab", {})) if isinstance(final_payload.get("lab", {}), dict) else {},
            "native_targets": dict(final_payload.get("native_targets", {}))
            if isinstance(final_payload.get("native_targets", {}), dict)
            else {},
            "guidance": dict(final_payload.get("guidance", {})) if isinstance(final_payload.get("guidance", {}), dict) else {},
        }

    def run_lab_program_cycle(
        self,
        *,
        program_id: str,
        max_campaigns: int = 3,
        max_sweeps_per_campaign: int = 2,
        max_sessions: int = 3,
        max_replays_per_session: int = 2,
        history_limit: int = 8,
        stop_on_stable: bool = True,
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        program_payload = memory.get_program(program_id)
        program = dict(program_payload.get("program", {})) if isinstance(program_payload.get("program", {}), dict) else {}
        if not program:
            return {"status": "error", "message": str(program_payload.get("message", "") or "benchmark lab program not found")}
        filters = dict(program.get("filters", {})) if isinstance(program.get("filters", {}), dict) else {}
        normalized_max_campaigns = max(1, min(int(max_campaigns or 3), 8))
        normalized_max_sweeps = max(1, min(int(max_sweeps_per_campaign or 2), 8))
        normalized_max_sessions = max(1, min(int(max_sessions or 3), 8))
        normalized_max_replays = max(1, min(int(max_replays_per_session or 2), 8))
        normalized_history_limit = max(1, min(int(history_limit or 8), 64))
        program_campaign_ids = self._unique_strings(
            [str(item).strip() for item in program.get("campaign_ids", []) if str(item).strip()]
        ) if isinstance(program.get("campaign_ids", []), list) else []
        current_campaigns: List[Dict[str, object]] = []
        for campaign_id in program_campaign_ids:
            campaign_payload = memory.get_campaign(campaign_id)
            campaign = dict(campaign_payload.get("campaign", {})) if isinstance(campaign_payload.get("campaign", {}), dict) else {}
            if campaign:
                current_campaigns.append(campaign)
        represented_apps = {
            str(item.get("filters", {}).get("app", "") or "").strip().lower()
            for item in current_campaigns
            if isinstance(item.get("filters", {}), dict) and str(item.get("filters", {}).get("app", "") or "").strip()
        }
        app_targets = self._unique_strings(
            [
                str(item).strip()
                for item in program.get("app_targets", program.get("target_apps", []))
                if str(item).strip()
            ]
        ) if isinstance(program.get("app_targets", program.get("target_apps", [])), list) else []
        created_campaigns: List[Dict[str, object]] = []
        for target_app in app_targets:
            if target_app.strip().lower() in represented_apps:
                continue
            if len(current_campaigns) + len(created_campaigns) >= max(normalized_max_campaigns, len(current_campaigns)):
                break
            create_payload = self.create_lab_campaign(
                scenario_name=str(filters.get("scenario_name", "") or "").strip(),
                pack=str(filters.get("pack", "") or "").strip(),
                category=str(filters.get("category", "") or "").strip(),
                capability=str(filters.get("capability", "") or "").strip(),
                risk_level=str(filters.get("risk_level", "") or "").strip(),
                autonomy_tier=str(filters.get("autonomy_tier", "") or "").strip(),
                mission_family=str(filters.get("mission_family", "") or "").strip(),
                app=target_app,
                limit=int(filters.get("limit", 200) or 200),
                history_limit=normalized_history_limit,
                source="benchmark_program_cycle",
                label=f"{str(program.get('label', '') or 'replay program').strip()} / {target_app}",
                max_sessions=normalized_max_sessions,
            )
            if str(create_payload.get("status", "") or "").strip().lower() != "success":
                continue
            created_campaigns.append(dict(create_payload))
            campaign = dict(create_payload.get("campaign", {})) if isinstance(create_payload.get("campaign", {}), dict) else {}
            if campaign:
                current_campaigns.append(campaign)
                represented_apps.add(target_app.strip().lower())
                campaign_id = str(campaign.get("campaign_id", "") or "").strip()
                if campaign_id and campaign_id not in program_campaign_ids:
                    program_campaign_ids.append(campaign_id)
        ranked_campaigns = sorted(
            current_campaigns,
            key=lambda item: (
                1 if str(item.get("status", "") or "").strip().lower() == "attention" else 0,
                1 if str(item.get("campaign_priority", "") or "").strip().lower() == "critical" else 0,
                1 if str(item.get("campaign_priority", "") or "").strip().lower() == "elevated" else 0,
                float(item.get("campaign_pressure_score", 0.0) or 0.0),
                int(item.get("attention_session_count", 0) or 0),
                int(item.get("pending_session_count", 0) or 0),
                int(item.get("pending_app_target_count", 0) or 0),
                int(item.get("long_horizon_pending_count", 0) or 0),
            ),
            reverse=True,
        )
        selected_campaigns = ranked_campaigns[:normalized_max_campaigns]
        results: List[Dict[str, object]] = []
        refreshed_campaigns: List[Dict[str, object]] = []
        for campaign in selected_campaigns:
            campaign_id = str(campaign.get("campaign_id", "") or "").strip()
            if not campaign_id:
                continue
            cycle_payload = self.run_lab_campaign_cycle(
                campaign_id=campaign_id,
                max_sweeps=normalized_max_sweeps,
                max_sessions=normalized_max_sessions,
                max_replays_per_session=normalized_max_replays,
                history_limit=normalized_history_limit,
                stop_on_stable=stop_on_stable,
            )
            results.append(cycle_payload)
            updated_campaign = dict(cycle_payload.get("campaign", {})) if isinstance(cycle_payload.get("campaign", {}), dict) else {}
            if updated_campaign:
                refreshed_campaigns.append(updated_campaign)
        if not refreshed_campaigns:
            refreshed_campaigns = current_campaigns[:normalized_max_campaigns]
        all_campaign_rows: List[Dict[str, object]] = []
        known_campaign_ids = {
            str(item.get("campaign_id", "") or "").strip()
            for item in refreshed_campaigns
            if isinstance(item, dict)
        }
        all_campaign_rows.extend(refreshed_campaigns)
        for campaign in current_campaigns:
            campaign_id = str(campaign.get("campaign_id", "") or "").strip()
            if not campaign_id or campaign_id in known_campaign_ids:
                continue
            all_campaign_rows.append(campaign)
        lab_payload = self.lab(**self._filters_to_run_kwargs(filters), history_limit=normalized_history_limit)
        native_targets_payload = self.native_control_targets(**self._filters_to_run_kwargs(filters), history_limit=normalized_history_limit)
        guidance_payload = self.control_guidance()
        pending_session_count = sum(int(item.get("pending_session_count", 0) or 0) for item in all_campaign_rows)
        attention_session_count = sum(int(item.get("attention_session_count", 0) or 0) for item in all_campaign_rows)
        pending_app_target_count = sum(int(item.get("pending_app_target_count", 0) or 0) for item in all_campaign_rows)
        long_horizon_pending_count = sum(int(item.get("long_horizon_pending_count", 0) or 0) for item in all_campaign_rows)
        stable_campaign_count = sum(
            1
            for item in refreshed_campaigns
            if str(item.get("status", "") or "").strip().lower() == "complete"
            and str(item.get("latest_sweep_regression_status", item.get("latest_sweep_status", "")) or "").strip().lower()
            not in {"regression", "failed", "error"}
        )
        regression_campaign_count = sum(
            1
            for item in refreshed_campaigns
            if str(item.get("latest_sweep_regression_status", item.get("latest_sweep_status", "")) or "").strip().lower()
            in {"regression", "failed", "error"}
        )
        cycle_trend_direction = "stable"
        if regression_campaign_count > 0 or attention_session_count > 0:
            cycle_trend_direction = "regressing"
        elif pending_session_count > 0 or pending_app_target_count > 0:
            cycle_trend_direction = "warming"
        weighted_scores = [
            float(item.get("latest_sweep_score", 0.0) or 0.0)
            for item in refreshed_campaigns
            if item.get("latest_sweep_score") is not None
        ]
        weighted_pass_rates = [
            float(item.get("latest_sweep_pass_rate", 0.0) or 0.0)
            for item in refreshed_campaigns
            if item.get("latest_sweep_pass_rate") is not None
        ]
        stop_reason = "max_campaigns_reached"
        if not selected_campaigns:
            stop_reason = "no_matching_campaigns"
        elif (
            stop_on_stable
            and pending_session_count <= 0
            and attention_session_count <= 0
            and pending_app_target_count <= 0
            and regression_campaign_count <= 0
        ):
            stop_reason = "stable"
        cycle_update = memory.record_program_cycle(
            program_id=program_id,
            cycle_payload={
                "status": "success" if results else "idle",
                "executed_at": datetime.now(timezone.utc).isoformat(),
                "stop_reason": stop_reason,
                "executed_campaign_count": len(refreshed_campaigns),
                "created_campaign_count": len(created_campaigns),
                "executed_sweep_count": sum(
                    int(dict(item.get("cycle", {})).get("executed_sweep_count", 0) or 0)
                    for item in results
                    if isinstance(item, dict)
                ),
                "stable_campaign_count": stable_campaign_count,
                "regression_campaign_count": regression_campaign_count,
                "pending_session_count": pending_session_count,
                "attention_session_count": attention_session_count,
                "pending_app_target_count": pending_app_target_count,
                "long_horizon_pending_count": long_horizon_pending_count,
                "weighted_score": round(sum(weighted_scores) / len(weighted_scores), 6) if weighted_scores else 0.0,
                "weighted_pass_rate": round(sum(weighted_pass_rates) / len(weighted_pass_rates), 6) if weighted_pass_rates else 0.0,
                "trend_direction": cycle_trend_direction,
                "query": {
                    **filters,
                    "history_limit": normalized_history_limit,
                    "max_campaigns": normalized_max_campaigns,
                    "max_sweeps_per_campaign": normalized_max_sweeps,
                },
            },
            lab_payload=lab_payload,
            native_targets_payload=native_targets_payload,
            guidance_payload=guidance_payload,
            campaign_ids=program_campaign_ids,
            app_targets=app_targets,
            campaign_rows=[dict(item) for item in all_campaign_rows],
        )
        program_row = dict(cycle_update.get("program", {})) if isinstance(cycle_update.get("program", {}), dict) else {}
        cycle_row = dict(cycle_update.get("cycle", {})) if isinstance(cycle_update.get("cycle", {}), dict) else {}
        return {
            "status": str(cycle_update.get("status", "success") or "success"),
            "message": f"program cycle executed {len(refreshed_campaigns)} campaign(s) | stop:{stop_reason}",
            "program": program_row,
            "cycle": cycle_row,
            "results": results,
            "created_campaigns": created_campaigns,
            "created_campaign_count": len(created_campaigns),
            "created_session_count": sum(
                int(item.get("created_session_count", 0) or 0)
                for item in created_campaigns
                if isinstance(item, dict)
            ),
            "lab": lab_payload,
            "native_targets": native_targets_payload,
            "guidance": guidance_payload,
        }

    def run_lab_portfolio_cycle(
        self,
        *,
        portfolio_id: str,
        max_programs: int = 3,
        max_campaigns_per_program: int = 3,
        max_sweeps_per_campaign: int = 2,
        max_sessions: int = 3,
        max_replays_per_session: int = 2,
        history_limit: int = 8,
        stop_on_stable: bool = True,
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        portfolio_payload = memory.get_portfolio(portfolio_id)
        portfolio = dict(portfolio_payload.get("portfolio", {})) if isinstance(portfolio_payload.get("portfolio", {}), dict) else {}
        if not portfolio:
            return {"status": "error", "message": str(portfolio_payload.get("message", "") or "benchmark lab portfolio not found")}
        filters = dict(portfolio.get("filters", {})) if isinstance(portfolio.get("filters", {}), dict) else {}
        normalized_max_programs = max(1, min(int(max_programs or 3), 8))
        normalized_max_campaigns = max(1, min(int(max_campaigns_per_program or 3), 8))
        normalized_max_sweeps = max(1, min(int(max_sweeps_per_campaign or 2), 8))
        normalized_max_sessions = max(1, min(int(max_sessions or 3), 8))
        normalized_max_replays = max(1, min(int(max_replays_per_session or 2), 8))
        normalized_history_limit = max(1, min(int(history_limit or 8), 64))
        portfolio_program_ids = self._unique_strings(
            [str(item).strip() for item in portfolio.get("program_ids", []) if str(item).strip()]
        ) if isinstance(portfolio.get("program_ids", []), list) else []
        current_programs: List[Dict[str, object]] = []
        for program_id_value in portfolio_program_ids:
            program_payload = memory.get_program(program_id_value)
            program = dict(program_payload.get("program", {})) if isinstance(program_payload.get("program", {}), dict) else {}
            if program:
                current_programs.append(program)
        represented_apps = {
            str(item.get("filters", {}).get("app", item.get("filters", {}).get("app_name", "")) or "").strip().lower()
            for item in current_programs
            if isinstance(item.get("filters", {}), dict)
            and str(item.get("filters", {}).get("app", item.get("filters", {}).get("app_name", "")) or "").strip()
        }
        app_targets = self._unique_strings(
            [
                str(item).strip()
                for item in portfolio.get("app_targets", portfolio.get("target_apps", []))
                if str(item).strip()
            ]
        ) if isinstance(portfolio.get("app_targets", portfolio.get("target_apps", [])), list) else []
        created_programs: List[Dict[str, object]] = []
        for target_app in app_targets:
            if target_app.strip().lower() in represented_apps:
                continue
            if len(current_programs) + len(created_programs) >= max(normalized_max_programs, len(current_programs)):
                break
            create_payload = self.create_lab_program(
                scenario_name=str(filters.get("scenario_name", "") or "").strip(),
                pack=str(filters.get("pack", "") or "").strip(),
                category=str(filters.get("category", "") or "").strip(),
                capability=str(filters.get("capability", "") or "").strip(),
                risk_level=str(filters.get("risk_level", "") or "").strip(),
                autonomy_tier=str(filters.get("autonomy_tier", "") or "").strip(),
                mission_family=str(filters.get("mission_family", "") or "").strip(),
                app=target_app,
                limit=int(filters.get("limit", 200) or 200),
                history_limit=normalized_history_limit,
                source="benchmark_portfolio_cycle",
                label=f"{str(portfolio.get('label', '') or 'replay portfolio').strip()} / {target_app}",
                max_campaigns=normalized_max_campaigns,
                max_sessions_per_campaign=normalized_max_sessions,
            )
            if str(create_payload.get("status", "") or "").strip().lower() != "success":
                continue
            created_programs.append(dict(create_payload))
            program = dict(create_payload.get("program", {})) if isinstance(create_payload.get("program", {}), dict) else {}
            if program:
                current_programs.append(program)
                represented_apps.add(target_app.strip().lower())
                program_id_value = str(program.get("program_id", "") or "").strip()
                if program_id_value and program_id_value not in portfolio_program_ids:
                    portfolio_program_ids.append(program_id_value)
        ranked_programs = sorted(
            current_programs,
            key=self._program_watchdog_sort_key,
            reverse=True,
        )
        selected_programs = ranked_programs[:normalized_max_programs]
        results: List[Dict[str, object]] = []
        refreshed_programs: List[Dict[str, object]] = []
        for program in selected_programs:
            program_id_value = str(program.get("program_id", "") or "").strip()
            if not program_id_value:
                continue
            cycle_payload = self.run_lab_program_cycle(
                program_id=program_id_value,
                max_campaigns=normalized_max_campaigns,
                max_sweeps_per_campaign=normalized_max_sweeps,
                max_sessions=normalized_max_sessions,
                max_replays_per_session=normalized_max_replays,
                history_limit=normalized_history_limit,
                stop_on_stable=stop_on_stable,
            )
            results.append(cycle_payload)
            updated_program = dict(cycle_payload.get("program", {})) if isinstance(cycle_payload.get("program", {}), dict) else {}
            if updated_program:
                refreshed_programs.append(updated_program)
        if not refreshed_programs:
            refreshed_programs = current_programs[:normalized_max_programs]
        all_program_rows: List[Dict[str, object]] = []
        known_program_ids = {
            str(item.get("program_id", "") or "").strip()
            for item in refreshed_programs
            if isinstance(item, dict)
        }
        all_program_rows.extend(refreshed_programs)
        for program in current_programs:
            program_id_value = str(program.get("program_id", "") or "").strip()
            if not program_id_value or program_id_value in known_program_ids:
                continue
            all_program_rows.append(program)
        lab_payload = self.lab(**self._filters_to_run_kwargs(filters), history_limit=normalized_history_limit)
        native_targets_payload = self.native_control_targets(**self._filters_to_run_kwargs(filters), history_limit=normalized_history_limit)
        guidance_payload = self.control_guidance()
        pending_program_count = sum(
            1 for item in all_program_rows if str(item.get("status", "") or "").strip().lower() != "complete"
        )
        attention_program_count = sum(
            1 for item in all_program_rows if str(item.get("status", "") or "").strip().lower() == "attention"
        )
        pending_campaign_count = sum(int(item.get("pending_campaign_count", 0) or 0) for item in all_program_rows)
        attention_campaign_count = sum(int(item.get("attention_campaign_count", 0) or 0) for item in all_program_rows)
        pending_session_count = sum(int(item.get("pending_session_count", 0) or 0) for item in all_program_rows)
        pending_app_target_count = sum(int(item.get("pending_app_target_count", 0) or 0) for item in all_program_rows)
        long_horizon_pending_count = sum(int(item.get("long_horizon_pending_count", 0) or 0) for item in all_program_rows)
        stable_program_count = sum(
            1
            for item in refreshed_programs
            if str(item.get("status", "") or "").strip().lower() == "complete"
            and str(item.get("latest_cycle_status", "") or "").strip().lower() not in {"regression", "failed", "error"}
        )
        regression_program_count = sum(
            1
            for item in refreshed_programs
            if str(item.get("latest_cycle_status", "") or "").strip().lower() in {"regression", "failed", "error"}
        )
        wave_trend_direction = "stable"
        if regression_program_count > 0 or attention_program_count > 0:
            wave_trend_direction = "regressing"
        elif pending_program_count > 0 or pending_app_target_count > 0:
            wave_trend_direction = "warming"
        weighted_scores = [
            float(item.get("latest_cycle_weighted_score", 0.0) or 0.0)
            for item in refreshed_programs
            if item.get("latest_cycle_weighted_score") is not None
        ]
        weighted_pass_rates = [
            float(item.get("latest_cycle_weighted_pass_rate", 0.0) or 0.0)
            for item in refreshed_programs
            if item.get("latest_cycle_weighted_pass_rate") is not None
        ]
        stop_reason = "max_programs_reached"
        if not selected_programs:
            stop_reason = "no_matching_programs"
        elif (
            stop_on_stable
            and pending_program_count <= 0
            and attention_program_count <= 0
            and pending_app_target_count <= 0
            and regression_program_count <= 0
        ):
            stop_reason = "stable"
        wave_update = memory.record_portfolio_wave(
            portfolio_id=portfolio_id,
            wave_payload={
                "status": "success" if results else "idle",
                "executed_at": datetime.now(timezone.utc).isoformat(),
                "stop_reason": stop_reason,
                "executed_program_count": len(refreshed_programs),
                "created_program_count": len(created_programs),
                "executed_campaign_count": sum(
                    int(dict(item.get("cycle", {})).get("executed_campaign_count", 0) or 0)
                    for item in results
                    if isinstance(item, dict)
                ),
                "executed_sweep_count": sum(
                    int(dict(item.get("cycle", {})).get("executed_sweep_count", 0) or 0)
                    for item in results
                    if isinstance(item, dict)
                ),
                "stable_program_count": stable_program_count,
                "regression_program_count": regression_program_count,
                "pending_campaign_count": pending_campaign_count,
                "attention_campaign_count": attention_campaign_count,
                "pending_session_count": pending_session_count,
                "pending_app_target_count": pending_app_target_count,
                "long_horizon_pending_count": long_horizon_pending_count,
                "weighted_score": round(sum(weighted_scores) / len(weighted_scores), 6) if weighted_scores else 0.0,
                "weighted_pass_rate": round(sum(weighted_pass_rates) / len(weighted_pass_rates), 6) if weighted_pass_rates else 0.0,
                "trend_direction": wave_trend_direction,
                "query": {
                    **filters,
                    "history_limit": normalized_history_limit,
                    "max_programs": normalized_max_programs,
                    "max_campaigns_per_program": normalized_max_campaigns,
                    "max_sweeps_per_campaign": normalized_max_sweeps,
                },
            },
            lab_payload=lab_payload,
            native_targets_payload=native_targets_payload,
            guidance_payload=guidance_payload,
            program_ids=portfolio_program_ids,
            app_targets=app_targets,
            program_rows=[dict(item) for item in all_program_rows],
        )
        portfolio_row = dict(wave_update.get("portfolio", {})) if isinstance(wave_update.get("portfolio", {}), dict) else {}
        wave_row = dict(wave_update.get("wave", {})) if isinstance(wave_update.get("wave", {}), dict) else {}
        return {
            "status": str(wave_update.get("status", "success") or "success"),
            "message": f"portfolio cycle executed {len(refreshed_programs)} program(s) | stop:{stop_reason}",
            "portfolio": portfolio_row,
            "wave": wave_row,
            "results": results,
            "created_programs": created_programs,
            "created_program_count": len(created_programs),
            "created_campaign_count": sum(
                int(item.get("created_campaign_count", 0) or 0)
                for item in created_programs
                if isinstance(item, dict)
            ),
            "created_session_count": sum(
                int(item.get("created_session_count", 0) or 0)
                for item in created_programs
                if isinstance(item, dict)
            ),
            "lab": lab_payload,
            "native_targets": native_targets_payload,
            "guidance": guidance_payload,
        }

    def run_lab_program_watchdog(
        self,
        *,
        max_programs: int = 2,
        max_campaigns_per_program: int = 3,
        max_sweeps_per_campaign: int = 2,
        max_sessions: int = 3,
        max_replays_per_session: int = 2,
        history_limit: int = 8,
        program_status: str = "",
        pack: str = "",
        app_name: str = "",
        trigger_source: str = "manual",
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        normalized_max_programs = max(1, min(int(max_programs or 2), 32))
        normalized_max_campaigns = max(1, min(int(max_campaigns_per_program or 3), 8))
        normalized_max_sweeps = max(1, min(int(max_sweeps_per_campaign or 2), 8))
        normalized_max_sessions = max(1, min(int(max_sessions or 3), 8))
        normalized_max_replays = max(1, min(int(max_replays_per_session or 2), 8))
        normalized_history_limit = max(1, min(int(history_limit or 8), 64))
        normalized_status = str(program_status or "").strip()
        normalized_pack = str(pack or "").strip().lower()
        normalized_app = str(app_name or "").strip().lower()

        all_program_payload = self.lab_programs(limit=max(normalized_max_programs * 4, 16), status="")
        program_rows = [dict(item) for item in all_program_payload.get("items", []) if isinstance(item, dict)]
        all_campaign_payload = self.lab_campaigns(limit=max(normalized_max_programs * 8, 24), status="")
        campaign_rows = [dict(item) for item in all_campaign_payload.get("items", []) if isinstance(item, dict)]
        native_targets_payload = self.native_control_targets(
            pack=normalized_pack,
            app=normalized_app,
            history_limit=normalized_history_limit,
        )
        guidance_payload = self.control_guidance()
        filtered_programs = [
            row
            for row in program_rows
            if self._program_matches_watchdog_filters(
                row,
                program_status=normalized_status,
                pack=normalized_pack,
                app_name=normalized_app,
            )
        ]
        auto_created_programs: List[Dict[str, object]] = []
        if len(filtered_programs) < normalized_max_programs:
            auto_created_programs = self._watchdog_auto_create_programs(
                existing_programs=program_rows,
                existing_campaigns=campaign_rows,
                native_targets_payload=native_targets_payload,
                max_programs=normalized_max_programs - len(filtered_programs),
                max_campaigns_per_program=normalized_max_campaigns,
                max_sessions_per_campaign=normalized_max_sessions,
                history_limit=normalized_history_limit,
                program_status=normalized_status,
                pack=normalized_pack,
                app_name=normalized_app,
                trigger_source=trigger_source,
            )
            if auto_created_programs:
                all_program_payload = self.lab_programs(limit=max(normalized_max_programs * 6, 24), status="")
                program_rows = [dict(item) for item in all_program_payload.get("items", []) if isinstance(item, dict)]
                filtered_programs = [
                    row
                    for row in program_rows
                    if self._program_matches_watchdog_filters(
                        row,
                        program_status=normalized_status,
                        pack=normalized_pack,
                        app_name=normalized_app,
                    )
                ]
        if not filtered_programs:
            message_bits = ["no matching replay programs ready for cycle"]
            if normalized_pack:
                message_bits.append(f"pack={normalized_pack}")
            if normalized_app:
                message_bits.append(f"app={normalized_app}")
            if auto_created_programs:
                message_bits.insert(0, f"auto-created {len(auto_created_programs)} replay program(s)")
            return {
                "status": "partial" if auto_created_programs else "idle",
                "message": " | ".join(message_bits),
                "trigger_source": str(trigger_source or "manual").strip().lower() or "manual",
                "filters": {
                    "program_status": normalized_status,
                    "pack": normalized_pack,
                    "app_name": normalized_app,
                    "history_limit": normalized_history_limit,
                    "max_campaigns_per_program": normalized_max_campaigns,
                    "max_sweeps_per_campaign": normalized_max_sweeps,
                },
                "targeted_program_count": 0,
                "executed_program_count": 0,
                "executed_campaign_count": 0,
                "executed_sweep_count": 0,
                "stable_program_count": 0,
                "regression_program_count": 0,
                "pending_campaign_count": 0,
                "attention_campaign_count": 0,
                "pending_session_count": 0,
                "pending_app_target_count": 0,
                "long_horizon_pending_count": 0,
                "error_count": 0,
                "latest_program_label": "",
                "cycle_stop_reason_counts": {},
                "trend_direction_counts": {},
                "auto_created_program_count": len(auto_created_programs),
                "auto_created_programs": auto_created_programs,
                "auto_created_app_names": self._unique_strings(
                    [
                        str(item.get("app_name", "") or "").strip()
                        for item in auto_created_programs
                        if isinstance(item, dict)
                    ]
                ),
                "programs": [],
                "results": [],
                "lab_programs": self.lab_programs(limit=max(normalized_max_programs * 4, 16), status=normalized_status),
                "native_targets": native_targets_payload,
                "guidance": guidance_payload,
            }

        ranked_programs = sorted(filtered_programs, key=self._program_watchdog_sort_key, reverse=True)
        selected_programs = ranked_programs[:normalized_max_programs]
        results: List[Dict[str, object]] = []
        error_count = 0
        executed_campaign_count = 0
        executed_sweep_count = 0
        stable_program_count = 0
        regression_program_count = 0
        pending_campaign_count = 0
        attention_campaign_count = 0
        pending_session_count = 0
        pending_app_target_count = 0
        long_horizon_pending_count = 0
        latest_program_label = ""
        cycle_stop_reason_counts: Dict[str, int] = {}
        trend_direction_counts: Dict[str, int] = {}

        for program in selected_programs:
            program_id = str(program.get("program_id", "") or "").strip()
            if not program_id:
                continue
            cycle_payload = self.run_lab_program_cycle(
                program_id=program_id,
                max_campaigns=normalized_max_campaigns,
                max_sweeps_per_campaign=normalized_max_sweeps,
                max_sessions=normalized_max_sessions,
                max_replays_per_session=normalized_max_replays,
                history_limit=normalized_history_limit,
            )
            program_row = dict(cycle_payload.get("program", {})) if isinstance(cycle_payload.get("program", {}), dict) else dict(program)
            cycle_row = dict(cycle_payload.get("cycle", {})) if isinstance(cycle_payload.get("cycle", {}), dict) else {}
            latest_program_label = latest_program_label or str(program_row.get("label", program.get("label", "")) or "").strip()
            if str(cycle_payload.get("status", "") or "").strip().lower() == "error":
                error_count += 1
            if str(program_row.get("latest_cycle_status", "") or "").strip().lower() in {"regression", "failed", "error"}:
                regression_program_count += 1
            executed_campaign_count += int(cycle_row.get("executed_campaign_count", 0) or 0)
            executed_sweep_count += int(cycle_row.get("executed_sweep_count", 0) or 0)
            if str(cycle_row.get("stop_reason", "") or "").strip().lower() == "stable" or int(program_row.get("stable_cycle_streak", 0) or 0) > 0:
                stable_program_count += 1
            pending_campaign_count += int(program_row.get("pending_campaign_count", 0) or 0)
            attention_campaign_count += int(program_row.get("attention_campaign_count", 0) or 0)
            pending_session_count += int(program_row.get("pending_session_count", 0) or 0)
            pending_app_target_count += int(program_row.get("pending_app_target_count", 0) or 0)
            long_horizon_pending_count += int(program_row.get("long_horizon_pending_count", 0) or 0)
            stop_reason = str(cycle_row.get("stop_reason", "") or "unknown").strip().lower() or "unknown"
            cycle_stop_reason_counts[stop_reason] = int(cycle_stop_reason_counts.get(stop_reason, 0)) + 1
            trend_direction = str(
                dict(program_row.get("trend_summary", {})).get("direction", program_row.get("history_direction", ""))
                if isinstance(program_row.get("trend_summary", {}), dict)
                else program_row.get("history_direction", "")
            ).strip().lower() or "unknown"
            trend_direction_counts[trend_direction] = int(trend_direction_counts.get(trend_direction, 0)) + 1
            results.append(
                {
                    "program_id": program_id,
                    "label": str(program_row.get("label", program.get("label", "")) or "").strip(),
                    "status": str(cycle_payload.get("status", program_row.get("status", "success")) or "success").strip() or "success",
                    "pending_campaign_count": int(program_row.get("pending_campaign_count", 0) or 0),
                    "attention_campaign_count": int(program_row.get("attention_campaign_count", 0) or 0),
                    "pending_session_count": int(program_row.get("pending_session_count", 0) or 0),
                    "pending_app_target_count": int(program_row.get("pending_app_target_count", 0) or 0),
                    "long_horizon_pending_count": int(program_row.get("long_horizon_pending_count", 0) or 0),
                    "latest_cycle_status": str(program_row.get("latest_cycle_status", "") or "").strip(),
                    "executed_campaign_count": int(cycle_row.get("executed_campaign_count", 0) or 0),
                    "executed_sweep_count": int(cycle_row.get("executed_sweep_count", 0) or 0),
                    "created_campaign_count": int(cycle_payload.get("created_campaign_count", 0) or 0),
                    "cycle_stop_reason": stop_reason,
                    "trend_direction": trend_direction,
                    "program_priority": str(program_row.get("program_priority", "") or "").strip().lower(),
                }
            )

        refreshed_programs = self.lab_programs(limit=max(normalized_max_programs * 2, 8), status=normalized_status)
        executed_program_count = len(results)
        if executed_program_count <= 0 and error_count > 0:
            status = "error"
        elif executed_program_count <= 0:
            status = "idle"
        elif error_count > 0:
            status = "partial"
        else:
            status = "success"
        message = (
            f"program watchdog executed {executed_program_count} program(s)"
            if executed_program_count > 0
            else "program watchdog found no executable replay programs"
        )
        if auto_created_programs:
            message = f"{message} | auto-created {len(auto_created_programs)} replay program(s)"
        return {
            "status": status,
            "message": message,
            "trigger_source": str(trigger_source or "manual").strip().lower() or "manual",
            "filters": {
                "program_status": normalized_status,
                "pack": normalized_pack,
                "app_name": normalized_app,
                "history_limit": normalized_history_limit,
                "max_campaigns_per_program": normalized_max_campaigns,
                "max_sweeps_per_campaign": normalized_max_sweeps,
            },
            "targeted_program_count": len(selected_programs),
            "executed_program_count": executed_program_count,
            "executed_campaign_count": executed_campaign_count,
            "executed_sweep_count": executed_sweep_count,
            "stable_program_count": stable_program_count,
            "regression_program_count": regression_program_count,
            "pending_campaign_count": pending_campaign_count,
            "attention_campaign_count": attention_campaign_count,
            "pending_session_count": pending_session_count,
            "pending_app_target_count": pending_app_target_count,
            "long_horizon_pending_count": long_horizon_pending_count,
            "error_count": error_count,
            "latest_program_label": latest_program_label,
            "cycle_stop_reason_counts": cycle_stop_reason_counts,
            "trend_direction_counts": trend_direction_counts,
            "auto_created_program_count": len(auto_created_programs),
            "auto_created_programs": auto_created_programs,
            "auto_created_app_names": self._unique_strings(
                [
                    str(item.get("app_name", "") or "").strip()
                    for item in auto_created_programs
                    if isinstance(item, dict)
                ]
            ),
            "programs": selected_programs,
            "results": results,
            "lab_programs": refreshed_programs,
            "native_targets": native_targets_payload,
            "guidance": guidance_payload,
        }

    def run_lab_portfolio_watchdog(
        self,
        *,
        max_portfolios: int = 2,
        max_programs_per_portfolio: int = 3,
        max_campaigns_per_program: int = 3,
        max_sweeps_per_campaign: int = 2,
        max_sessions: int = 3,
        max_replays_per_session: int = 2,
        history_limit: int = 8,
        portfolio_status: str = "",
        pack: str = "",
        app_name: str = "",
        trigger_source: str = "manual",
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        normalized_max_portfolios = max(1, min(int(max_portfolios or 2), 32))
        normalized_max_programs = max(1, min(int(max_programs_per_portfolio or 3), 8))
        normalized_max_campaigns = max(1, min(int(max_campaigns_per_program or 3), 8))
        normalized_max_sweeps = max(1, min(int(max_sweeps_per_campaign or 2), 8))
        normalized_max_sessions = max(1, min(int(max_sessions or 3), 8))
        normalized_max_replays = max(1, min(int(max_replays_per_session or 2), 8))
        normalized_history_limit = max(1, min(int(history_limit or 8), 64))
        normalized_status = str(portfolio_status or "").strip()
        normalized_pack = str(pack or "").strip().lower()
        normalized_app = str(app_name or "").strip().lower()

        all_portfolio_payload = self.lab_portfolios(limit=max(normalized_max_portfolios * 4, 16), status="")
        portfolio_rows = [dict(item) for item in all_portfolio_payload.get("items", []) if isinstance(item, dict)]
        all_program_payload = self.lab_programs(limit=max(normalized_max_portfolios * 8, 24), status="")
        program_rows = [dict(item) for item in all_program_payload.get("items", []) if isinstance(item, dict)]
        all_campaign_payload = self.lab_campaigns(limit=max(normalized_max_portfolios * 10, 32), status="")
        campaign_rows = [dict(item) for item in all_campaign_payload.get("items", []) if isinstance(item, dict)]
        native_targets_payload = self.native_control_targets(
            pack=normalized_pack,
            app=normalized_app,
            history_limit=normalized_history_limit,
        )
        guidance_payload = self.control_guidance()
        filtered_portfolios = [
            row
            for row in portfolio_rows
            if self._portfolio_matches_watchdog_filters(
                row,
                portfolio_status=normalized_status,
                pack=normalized_pack,
                app_name=normalized_app,
            )
        ]
        auto_created_portfolios: List[Dict[str, object]] = []
        if len(filtered_portfolios) < normalized_max_portfolios:
            auto_created_portfolios = self._watchdog_auto_create_portfolios(
                existing_portfolios=portfolio_rows,
                existing_programs=program_rows,
                existing_campaigns=campaign_rows,
                native_targets_payload=native_targets_payload,
                max_portfolios=normalized_max_portfolios - len(filtered_portfolios),
                max_programs_per_portfolio=normalized_max_programs,
                max_campaigns_per_program=normalized_max_campaigns,
                max_sessions_per_campaign=normalized_max_sessions,
                history_limit=normalized_history_limit,
                portfolio_status=normalized_status,
                pack=normalized_pack,
                app_name=normalized_app,
                trigger_source=trigger_source,
            )
            if auto_created_portfolios:
                all_portfolio_payload = self.lab_portfolios(limit=max(normalized_max_portfolios * 6, 24), status="")
                portfolio_rows = [dict(item) for item in all_portfolio_payload.get("items", []) if isinstance(item, dict)]
                filtered_portfolios = [
                    row
                    for row in portfolio_rows
                    if self._portfolio_matches_watchdog_filters(
                        row,
                        portfolio_status=normalized_status,
                        pack=normalized_pack,
                        app_name=normalized_app,
                    )
                ]
        if not filtered_portfolios:
            return {
                "status": "partial" if auto_created_portfolios else "idle",
                "message": "no matching replay portfolios ready for cycle",
                "trigger_source": str(trigger_source or "manual").strip().lower() or "manual",
                "filters": {
                    "portfolio_status": normalized_status,
                    "pack": normalized_pack,
                    "app_name": normalized_app,
                    "history_limit": normalized_history_limit,
                    "max_programs_per_portfolio": normalized_max_programs,
                    "max_campaigns_per_program": normalized_max_campaigns,
                    "max_sweeps_per_campaign": normalized_max_sweeps,
                },
                "targeted_portfolio_count": 0,
                "executed_portfolio_count": 0,
                "executed_program_count": 0,
                "executed_campaign_count": 0,
                "executed_sweep_count": 0,
                "stable_portfolio_count": 0,
                "regression_portfolio_count": 0,
                "pending_program_count": 0,
                "attention_program_count": 0,
                "pending_campaign_count": 0,
                "pending_session_count": 0,
                "pending_app_target_count": 0,
                "long_horizon_pending_count": 0,
                "error_count": 0,
                "latest_portfolio_label": "",
                "wave_stop_reason_counts": {},
                "trend_direction_counts": {},
                "auto_created_portfolio_count": len(auto_created_portfolios),
                "auto_created_portfolios": auto_created_portfolios,
                "auto_created_app_names": self._unique_strings(
                    [
                        str(item.get("app_name", "") or "").strip()
                        for item in auto_created_portfolios
                        if isinstance(item, dict)
                    ]
                ),
                "portfolios": [],
                "results": [],
                "lab_portfolios": self.lab_portfolios(limit=max(normalized_max_portfolios * 4, 16), status=normalized_status),
                "native_targets": native_targets_payload,
                "guidance": guidance_payload,
            }
        ranked_portfolios = sorted(filtered_portfolios, key=self._portfolio_watchdog_sort_key, reverse=True)
        selected_portfolios = ranked_portfolios[:normalized_max_portfolios]
        results: List[Dict[str, object]] = []
        error_count = 0
        executed_program_count = 0
        executed_campaign_count = 0
        executed_sweep_count = 0
        stable_portfolio_count = 0
        regression_portfolio_count = 0
        pending_program_count = 0
        attention_program_count = 0
        pending_campaign_count = 0
        pending_session_count = 0
        pending_app_target_count = 0
        long_horizon_pending_count = 0
        latest_portfolio_label = ""
        wave_stop_reason_counts: Dict[str, int] = {}
        trend_direction_counts: Dict[str, int] = {}

        for portfolio in selected_portfolios:
            portfolio_id_value = str(portfolio.get("portfolio_id", "") or "").strip()
            if not portfolio_id_value:
                continue
            cycle_payload = self.run_lab_portfolio_cycle(
                portfolio_id=portfolio_id_value,
                max_programs=normalized_max_programs,
                max_campaigns_per_program=normalized_max_campaigns,
                max_sweeps_per_campaign=normalized_max_sweeps,
                max_sessions=normalized_max_sessions,
                max_replays_per_session=normalized_max_replays,
                history_limit=normalized_history_limit,
            )
            portfolio_row = dict(cycle_payload.get("portfolio", {})) if isinstance(cycle_payload.get("portfolio", {}), dict) else dict(portfolio)
            wave_row = dict(cycle_payload.get("wave", {})) if isinstance(cycle_payload.get("wave", {}), dict) else {}
            latest_portfolio_label = latest_portfolio_label or str(portfolio_row.get("label", portfolio.get("label", "")) or "").strip()
            if str(cycle_payload.get("status", "") or "").strip().lower() == "error":
                error_count += 1
            if str(portfolio_row.get("latest_wave_status", "") or "").strip().lower() in {"regression", "failed", "error"}:
                regression_portfolio_count += 1
            executed_program_count += int(wave_row.get("executed_program_count", 0) or 0)
            executed_campaign_count += int(wave_row.get("executed_campaign_count", 0) or 0)
            executed_sweep_count += int(wave_row.get("executed_sweep_count", 0) or 0)
            if str(wave_row.get("stop_reason", "") or "").strip().lower() == "stable" or int(portfolio_row.get("stable_wave_streak", 0) or 0) > 0:
                stable_portfolio_count += 1
            pending_program_count += int(portfolio_row.get("pending_program_count", 0) or 0)
            attention_program_count += int(portfolio_row.get("attention_program_count", 0) or 0)
            pending_campaign_count += int(portfolio_row.get("pending_campaign_count", 0) or 0)
            pending_session_count += int(portfolio_row.get("pending_session_count", 0) or 0)
            pending_app_target_count += int(portfolio_row.get("pending_app_target_count", 0) or 0)
            long_horizon_pending_count += int(portfolio_row.get("long_horizon_pending_count", 0) or 0)
            stop_reason = str(wave_row.get("stop_reason", "") or "unknown").strip().lower() or "unknown"
            wave_stop_reason_counts[stop_reason] = int(wave_stop_reason_counts.get(stop_reason, 0)) + 1
            trend_direction = str(
                dict(portfolio_row.get("trend_summary", {})).get("direction", portfolio_row.get("history_direction", ""))
                if isinstance(portfolio_row.get("trend_summary", {}), dict)
                else portfolio_row.get("history_direction", "")
            ).strip().lower() or "unknown"
            trend_direction_counts[trend_direction] = int(trend_direction_counts.get(trend_direction, 0)) + 1
            results.append(
                {
                    "portfolio_id": portfolio_id_value,
                    "label": str(portfolio_row.get("label", portfolio.get("label", "")) or "").strip(),
                    "status": str(cycle_payload.get("status", portfolio_row.get("status", "success")) or "success").strip() or "success",
                    "pending_program_count": int(portfolio_row.get("pending_program_count", 0) or 0),
                    "attention_program_count": int(portfolio_row.get("attention_program_count", 0) or 0),
                    "pending_campaign_count": int(portfolio_row.get("pending_campaign_count", 0) or 0),
                    "pending_session_count": int(portfolio_row.get("pending_session_count", 0) or 0),
                    "pending_app_target_count": int(portfolio_row.get("pending_app_target_count", 0) or 0),
                    "long_horizon_pending_count": int(portfolio_row.get("long_horizon_pending_count", 0) or 0),
                    "latest_wave_status": str(portfolio_row.get("latest_wave_status", "") or "").strip(),
                    "executed_program_count": int(wave_row.get("executed_program_count", 0) or 0),
                    "executed_campaign_count": int(wave_row.get("executed_campaign_count", 0) or 0),
                    "executed_sweep_count": int(wave_row.get("executed_sweep_count", 0) or 0),
                    "created_program_count": int(cycle_payload.get("created_program_count", 0) or 0),
                    "wave_stop_reason": stop_reason,
                    "trend_direction": trend_direction,
                    "portfolio_priority": str(portfolio_row.get("portfolio_priority", "") or "").strip().lower(),
                }
            )
        refreshed_portfolios = self.lab_portfolios(limit=max(normalized_max_portfolios * 2, 8), status=normalized_status)
        executed_portfolio_count = len(results)
        if executed_portfolio_count <= 0 and error_count > 0:
            status = "error"
        elif executed_portfolio_count <= 0:
            status = "idle"
        elif error_count > 0:
            status = "partial"
        else:
            status = "success"
        message = (
            f"portfolio watchdog executed {executed_portfolio_count} portfolio(s)"
            if executed_portfolio_count > 0
            else "portfolio watchdog found no executable replay portfolios"
        )
        if auto_created_portfolios:
            message = f"{message} | auto-created {len(auto_created_portfolios)} replay portfolio(s)"
        return {
            "status": status,
            "message": message,
            "trigger_source": str(trigger_source or "manual").strip().lower() or "manual",
            "filters": {
                "portfolio_status": normalized_status,
                "pack": normalized_pack,
                "app_name": normalized_app,
                "history_limit": normalized_history_limit,
                "max_programs_per_portfolio": normalized_max_programs,
                "max_campaigns_per_program": normalized_max_campaigns,
                "max_sweeps_per_campaign": normalized_max_sweeps,
            },
            "targeted_portfolio_count": len(selected_portfolios),
            "executed_portfolio_count": executed_portfolio_count,
            "executed_program_count": executed_program_count,
            "executed_campaign_count": executed_campaign_count,
            "executed_sweep_count": executed_sweep_count,
            "stable_portfolio_count": stable_portfolio_count,
            "regression_portfolio_count": regression_portfolio_count,
            "pending_program_count": pending_program_count,
            "attention_program_count": attention_program_count,
            "pending_campaign_count": pending_campaign_count,
            "pending_session_count": pending_session_count,
            "pending_app_target_count": pending_app_target_count,
            "long_horizon_pending_count": long_horizon_pending_count,
            "error_count": error_count,
            "latest_portfolio_label": latest_portfolio_label,
            "wave_stop_reason_counts": wave_stop_reason_counts,
            "trend_direction_counts": trend_direction_counts,
            "auto_created_portfolio_count": len(auto_created_portfolios),
            "auto_created_portfolios": auto_created_portfolios,
            "auto_created_app_names": self._unique_strings(
                [
                    str(item.get("app_name", "") or "").strip()
                    for item in auto_created_portfolios
                    if isinstance(item, dict)
                ]
            ),
            "portfolios": selected_portfolios,
            "results": results,
            "lab_portfolios": refreshed_portfolios,
            "native_targets": native_targets_payload,
            "guidance": guidance_payload,
        }

    def run_lab_campaign_watchdog(
        self,
        *,
        max_campaigns: int = 2,
        max_sweeps_per_campaign: int = 2,
        max_sessions: int = 3,
        max_replays_per_session: int = 2,
        history_limit: int = 8,
        campaign_status: str = "",
        pack: str = "",
        app_name: str = "",
        trigger_source: str = "manual",
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        normalized_max_campaigns = max(1, min(int(max_campaigns or 2), 32))
        normalized_max_sweeps = max(1, min(int(max_sweeps_per_campaign or 2), 8))
        normalized_max_sessions = max(1, min(int(max_sessions or 3), 8))
        normalized_max_replays = max(1, min(int(max_replays_per_session or 2), 8))
        normalized_history_limit = max(1, min(int(history_limit or 8), 64))
        normalized_status = str(campaign_status or "").strip()
        normalized_pack = str(pack or "").strip().lower()
        normalized_app = str(app_name or "").strip().lower()

        all_campaign_payload = self.lab_campaigns(limit=max(normalized_max_campaigns * 4, 16), status="")
        campaign_rows = [dict(item) for item in all_campaign_payload.get("items", []) if isinstance(item, dict)]
        native_targets_payload = self.native_control_targets(
            pack=normalized_pack,
            app=normalized_app,
            history_limit=normalized_history_limit,
        )
        guidance_payload = self.control_guidance()
        filtered_campaigns = [
            row
            for row in campaign_rows
            if self._campaign_matches_watchdog_filters(
                row,
                campaign_status=normalized_status,
                pack=normalized_pack,
                app_name=normalized_app,
            )
        ]
        auto_created_campaigns: List[Dict[str, object]] = []
        if len(filtered_campaigns) < normalized_max_campaigns:
            auto_created_campaigns = self._watchdog_auto_create_campaigns(
                existing_campaigns=campaign_rows,
                native_targets_payload=native_targets_payload,
                max_campaigns=normalized_max_campaigns - len(filtered_campaigns),
                max_sessions=normalized_max_sessions,
                history_limit=normalized_history_limit,
                campaign_status=normalized_status,
                pack=normalized_pack,
                app_name=normalized_app,
                trigger_source=trigger_source,
            )
            if auto_created_campaigns:
                all_campaign_payload = self.lab_campaigns(limit=max(normalized_max_campaigns * 6, 24), status="")
                campaign_rows = [
                    dict(item)
                    for item in all_campaign_payload.get("items", [])
                    if isinstance(item, dict)
                ]
                filtered_campaigns = [
                    row
                    for row in campaign_rows
                    if self._campaign_matches_watchdog_filters(
                        row,
                        campaign_status=normalized_status,
                        pack=normalized_pack,
                        app_name=normalized_app,
                    )
                ]
        if not filtered_campaigns:
            message_bits = ["no matching replay campaigns ready for sweep"]
            if normalized_pack:
                message_bits.append(f"pack={normalized_pack}")
            if normalized_app:
                message_bits.append(f"app={normalized_app}")
            if auto_created_campaigns:
                message_bits.insert(0, f"auto-created {len(auto_created_campaigns)} replay campaign(s)")
            return {
                "status": "partial" if auto_created_campaigns else "idle",
                "message": " | ".join(message_bits),
                "trigger_source": str(trigger_source or "manual").strip().lower() or "manual",
                "filters": {
                    "campaign_status": normalized_status,
                    "pack": normalized_pack,
                    "app_name": normalized_app,
                    "history_limit": normalized_history_limit,
                },
                "targeted_campaign_count": 0,
                "executed_campaign_count": 0,
                "executed_sweep_count": 0,
                "stable_campaign_count": 0,
                "regression_campaign_count": 0,
                "pending_session_count": 0,
                "attention_session_count": 0,
                "pending_app_target_count": 0,
                "long_horizon_pending_count": 0,
                "error_count": 0,
                "latest_campaign_label": "",
                "cycle_stop_reason_counts": {},
                "trend_direction_counts": {},
                "auto_created_campaign_count": len(auto_created_campaigns),
                "auto_created_campaigns": auto_created_campaigns,
                "auto_created_app_names": self._unique_strings(
                    [
                        str(item.get("app_name", "") or "").strip()
                        for item in auto_created_campaigns
                        if isinstance(item, dict)
                    ]
                ),
                "campaigns": [],
                "results": [],
                "lab_campaigns": self.lab_campaigns(
                    limit=max(normalized_max_campaigns * 4, 16),
                    status=normalized_status,
                ),
                "native_targets": native_targets_payload,
                "guidance": guidance_payload,
            }

        ranked_campaigns = sorted(filtered_campaigns, key=self._campaign_watchdog_sort_key, reverse=True)
        selected_campaigns = ranked_campaigns[:normalized_max_campaigns]
        results: List[Dict[str, object]] = []
        error_count = 0
        executed_sweep_count = 0
        stable_campaign_count = 0
        regression_campaign_count = 0
        pending_session_count = 0
        attention_session_count = 0
        pending_app_target_count = 0
        long_horizon_pending_count = 0
        latest_campaign_label = ""
        cycle_stop_reason_counts: Dict[str, int] = {}
        trend_direction_counts: Dict[str, int] = {}

        for campaign in selected_campaigns:
            campaign_id = str(campaign.get("campaign_id", "") or "").strip()
            if not campaign_id:
                continue
            cycle_payload = self.run_lab_campaign_cycle(
                campaign_id=campaign_id,
                max_sweeps=normalized_max_sweeps,
                max_sessions=normalized_max_sessions,
                max_replays_per_session=normalized_max_replays,
                history_limit=normalized_history_limit,
            )
            campaign_row = (
                dict(cycle_payload.get("campaign", {}))
                if isinstance(cycle_payload.get("campaign", {}), dict)
                else dict(campaign)
            )
            cycle_row = dict(cycle_payload.get("cycle", {})) if isinstance(cycle_payload.get("cycle", {}), dict) else {}
            latest_campaign_label = latest_campaign_label or str(campaign_row.get("label", campaign.get("label", "")) or "").strip()
            if str(cycle_payload.get("status", "") or "").strip().lower() == "error":
                error_count += 1
            if str(campaign_row.get("latest_sweep_regression_status", campaign_row.get("latest_sweep_status", "")) or "").strip().lower() in {
                "regression",
                "failed",
            }:
                regression_campaign_count += 1
            executed_sweep_count += int(cycle_row.get("executed_sweep_count", 0) or 0)
            if bool(cycle_row.get("stable", False)):
                stable_campaign_count += 1
            pending_session_count += int(campaign_row.get("pending_session_count", 0) or 0)
            attention_session_count += int(campaign_row.get("attention_session_count", 0) or 0)
            pending_app_target_count += int(campaign_row.get("pending_app_target_count", 0) or 0)
            long_horizon_pending_count += int(campaign_row.get("long_horizon_pending_count", 0) or 0)
            stop_reason = str(cycle_row.get("stop_reason", "") or "unknown").strip().lower() or "unknown"
            cycle_stop_reason_counts[stop_reason] = int(cycle_stop_reason_counts.get(stop_reason, 0)) + 1
            trend_direction = str(
                dict(campaign_row.get("trend_summary", {})).get("direction", campaign_row.get("history_direction", ""))
                if isinstance(campaign_row.get("trend_summary", {}), dict)
                else campaign_row.get("history_direction", "")
            ).strip().lower() or "unknown"
            trend_direction_counts[trend_direction] = int(trend_direction_counts.get(trend_direction, 0)) + 1
            results.append(
                {
                    "campaign_id": campaign_id,
                    "label": str(campaign_row.get("label", campaign.get("label", "")) or "").strip(),
                    "status": str(cycle_payload.get("status", campaign_row.get("status", "success")) or "success").strip() or "success",
                    "pending_session_count": int(campaign_row.get("pending_session_count", 0) or 0),
                    "attention_session_count": int(campaign_row.get("attention_session_count", 0) or 0),
                    "pending_app_target_count": int(campaign_row.get("pending_app_target_count", 0) or 0),
                    "long_horizon_pending_count": int(campaign_row.get("long_horizon_pending_count", 0) or 0),
                    "latest_sweep_status": str(
                        campaign_row.get("latest_sweep_regression_status", campaign_row.get("latest_sweep_status", ""))
                        or ""
                    ).strip(),
                    "executed_session_count": int(cycle_row.get("executed_session_count", 0) or 0),
                    "created_session_count": int(cycle_row.get("created_session_count", 0) or 0),
                    "executed_sweep_count": int(cycle_row.get("executed_sweep_count", 0) or 0),
                    "cycle_stop_reason": stop_reason,
                    "trend_direction": trend_direction,
                    "campaign_priority": str(campaign_row.get("campaign_priority", "") or "").strip().lower(),
                }
            )

        refreshed_campaigns = self.lab_campaigns(limit=max(normalized_max_campaigns * 2, 8), status=normalized_status)
        executed_campaign_count = len(results)
        if executed_campaign_count <= 0 and error_count > 0:
            status = "error"
        elif executed_campaign_count <= 0:
            status = "idle"
        elif error_count > 0:
            status = "partial"
        else:
            status = "success"
        message = (
            f"campaign watchdog executed {executed_campaign_count} campaign(s)"
            if executed_campaign_count > 0
            else "campaign watchdog found no executable replay campaigns"
        )
        if auto_created_campaigns:
            message = f"{message} | auto-created {len(auto_created_campaigns)} replay campaign(s)"
        return {
            "status": status,
            "message": message,
            "trigger_source": str(trigger_source or "manual").strip().lower() or "manual",
            "filters": {
                "campaign_status": normalized_status,
                "pack": normalized_pack,
                "app_name": normalized_app,
                "history_limit": normalized_history_limit,
                "max_sweeps_per_campaign": normalized_max_sweeps,
            },
            "targeted_campaign_count": len(selected_campaigns),
            "executed_campaign_count": executed_campaign_count,
            "executed_sweep_count": executed_sweep_count,
            "stable_campaign_count": stable_campaign_count,
            "regression_campaign_count": regression_campaign_count,
            "pending_session_count": pending_session_count,
            "attention_session_count": attention_session_count,
            "pending_app_target_count": pending_app_target_count,
            "long_horizon_pending_count": long_horizon_pending_count,
            "error_count": error_count,
            "latest_campaign_label": latest_campaign_label,
            "cycle_stop_reason_counts": cycle_stop_reason_counts,
            "trend_direction_counts": trend_direction_counts,
            "auto_created_campaign_count": len(auto_created_campaigns),
            "auto_created_campaigns": auto_created_campaigns,
            "auto_created_app_names": self._unique_strings(
                [
                    str(item.get("app_name", "") or "").strip()
                    for item in auto_created_campaigns
                    if isinstance(item, dict)
                ]
            ),
            "campaigns": selected_campaigns,
            "results": results,
            "lab_campaigns": refreshed_campaigns,
            "native_targets": native_targets_payload,
            "guidance": guidance_payload,
        }

    def create_lab_session(
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
        source: str = "",
        label: str = "",
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
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
        native_targets_payload = self.native_control_targets(
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
        guidance_payload = self.control_guidance()
        payload = memory.record_session(
            filters=filters,
            lab_payload=lab_payload,
            native_targets_payload=native_targets_payload,
            guidance_payload=guidance_payload,
            source=source,
            label=label,
        )
        session = dict(payload.get("session", {})) if isinstance(payload.get("session", {}), dict) else {}
        return {
            "status": str(payload.get("status", "success") or "success"),
            "session": session,
            "lab": lab_payload,
            "native_targets": native_targets_payload,
            "guidance": guidance_payload,
        }

    def replay_lab_session(
        self,
        *,
        session_id: str,
        scenario_name: str = "",
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        session_payload = memory.get_session(session_id)
        session = dict(session_payload.get("session", {})) if isinstance(session_payload.get("session", {}), dict) else {}
        if not session:
            return {"status": "error", "message": str(session_payload.get("message", "") or "benchmark lab session not found")}
        candidates = (
            [dict(item) for item in session.get("replay_candidates", []) if isinstance(item, dict)]
            if isinstance(session.get("replay_candidates", []), list)
            else []
        )
        selected_candidate: Dict[str, Any] | None = None
        clean_scenario = str(scenario_name or "").strip()
        if clean_scenario:
            for candidate in candidates:
                if str(candidate.get("scenario", "") or "").strip() == clean_scenario:
                    selected_candidate = dict(candidate)
                    break
        if selected_candidate is None and candidates:
            selected_candidate = dict(candidates[0])
        if selected_candidate is None:
            return {"status": "error", "message": "benchmark lab session has no replay candidates"}
        replay_query = (
            dict(selected_candidate.get("replay_query", {}))
            if isinstance(selected_candidate.get("replay_query", {}), dict)
            else {}
        )
        if not replay_query:
            replay_query = {
                **self._filters_payload(
                    scenario_name=str(session.get("filters", {}).get("scenario_name", "") or "").strip()
                    if isinstance(session.get("filters", {}), dict)
                    else "",
                    pack=str(session.get("filters", {}).get("pack", "") or "").strip()
                    if isinstance(session.get("filters", {}), dict)
                    else "",
                    category=str(session.get("filters", {}).get("category", "") or "").strip()
                    if isinstance(session.get("filters", {}), dict)
                    else "",
                    capability=str(session.get("filters", {}).get("capability", "") or "").strip()
                    if isinstance(session.get("filters", {}), dict)
                    else "",
                    risk_level=str(session.get("filters", {}).get("risk_level", "") or "").strip()
                    if isinstance(session.get("filters", {}), dict)
                    else "",
                    autonomy_tier=str(session.get("filters", {}).get("autonomy_tier", "") or "").strip()
                    if isinstance(session.get("filters", {}), dict)
                    else "",
                    mission_family=str(session.get("filters", {}).get("mission_family", "") or "").strip()
                    if isinstance(session.get("filters", {}), dict)
                    else "",
                    app=str(session.get("filters", {}).get("app", "") or "").strip()
                    if isinstance(session.get("filters", {}), dict)
                    else "",
                    limit=1,
                ),
                "scenario_name": str(selected_candidate.get("scenario", "") or "").strip(),
                "limit": 1,
            }
        replay_query = {**replay_query, "scenario_name": str(selected_candidate.get("scenario", "") or "").strip(), "limit": 1}
        replay_result = self.run_with_summary(**self._filters_to_run_kwargs(replay_query))
        session_filters = (
            dict(session.get("filters", {}))
            if isinstance(session.get("filters", {}), dict)
            else {}
        )
        refreshed_payloads = self._lab_session_refresh_payloads(session_filters=session_filters, history_limit=8)
        refreshed_lab = refreshed_payloads["lab"]
        refreshed_native_targets = refreshed_payloads["native_targets"]
        refreshed_guidance = refreshed_payloads["guidance"]
        update_payload = memory.record_replay_result(
            session_id=str(session.get("session_id", session_id) or session_id).strip(),
            scenario_name=str(selected_candidate.get("scenario", "") or "").strip(),
            replay_payload=replay_result,
            replay_query=replay_query,
            lab_payload=refreshed_lab,
            native_targets_payload=refreshed_native_targets,
            guidance_payload=refreshed_guidance,
        )
        return {
            "status": str(update_payload.get("status", "success") or "success"),
            "session": dict(update_payload.get("session", {})) if isinstance(update_payload.get("session", {}), dict) else {},
            "replay_candidate": selected_candidate,
            "updated_candidate": dict(update_payload.get("updated_candidate", {})) if isinstance(update_payload.get("updated_candidate", {}), dict) else {},
            "replay_result": replay_result,
            "lab": refreshed_lab,
            "native_targets": refreshed_native_targets,
            "guidance": refreshed_guidance,
        }

    def run_lab_session_cycle(
        self,
        *,
        session_id: str,
        history_limit: int = 8,
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        session_payload = memory.get_session(session_id)
        session = dict(session_payload.get("session", {})) if isinstance(session_payload.get("session", {}), dict) else {}
        if not session:
            return {"status": "error", "message": str(session_payload.get("message", "") or "benchmark lab session not found")}
        session_filters = dict(session.get("filters", {})) if isinstance(session.get("filters", {}), dict) else {}
        run_kwargs = self._filters_to_run_kwargs(session_filters)
        normalized_history_limit = max(1, min(int(history_limit or 8), 64))
        cycle_result = self.run_with_summary(**run_kwargs)
        refreshed_payloads = self._lab_session_refresh_payloads(
            session_filters=session_filters,
            history_limit=normalized_history_limit,
        )
        update_payload = memory.record_run_cycle(
            session_id=str(session.get("session_id", session_id) or session_id).strip(),
            cycle_payload=cycle_result,
            cycle_query={**run_kwargs, "history_limit": normalized_history_limit},
            lab_payload=refreshed_payloads["lab"],
            native_targets_payload=refreshed_payloads["native_targets"],
            guidance_payload=refreshed_payloads["guidance"],
        )
        return {
            "status": str(update_payload.get("status", "success") or "success"),
            "session": dict(update_payload.get("session", {})) if isinstance(update_payload.get("session", {}), dict) else {},
            "cycle": dict(update_payload.get("cycle", {})) if isinstance(update_payload.get("cycle", {}), dict) else {},
            "cycle_result": cycle_result,
            "lab": refreshed_payloads["lab"],
            "native_targets": refreshed_payloads["native_targets"],
            "guidance": refreshed_payloads["guidance"],
        }

    def advance_lab_session(
        self,
        *,
        session_id: str,
        max_replays: int = 2,
        replay_status: str = "",
    ) -> Dict[str, object]:
        memory = self.lab_memory
        if memory is None:
            return {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        session_payload = memory.get_session(session_id)
        session = dict(session_payload.get("session", {})) if isinstance(session_payload.get("session", {}), dict) else {}
        if not session:
            return {"status": "error", "message": str(session_payload.get("message", "") or "benchmark lab session not found")}
        normalized_max = max(1, min(int(max_replays or 2), 8))
        clean_replay_status = str(replay_status or "").strip().lower()
        candidates = [
            dict(item)
            for item in session.get("replay_candidates", [])
            if isinstance(session.get("replay_candidates", []), list) and isinstance(item, dict)
        ]
        selected_candidates: List[Dict[str, object]] = []
        for candidate in candidates:
            candidate_status = str(candidate.get("replay_status", "pending") or "pending").strip().lower()
            if not str(candidate.get("scenario", "") or "").strip():
                continue
            if clean_replay_status == "failed" and candidate_status != "failed":
                continue
            if clean_replay_status == "pending" and candidate_status not in {"pending", "ready", "queued", "staged"}:
                continue
            if not clean_replay_status and candidate_status == "completed":
                continue
            selected_candidates.append(candidate)
            if len(selected_candidates) >= normalized_max:
                break
        if not selected_candidates:
            return {
                "status": "error",
                "message": "benchmark lab session has no replay candidates for the requested batch",
            }
        results: List[Dict[str, object]] = []
        final_session: Dict[str, object] = dict(session)
        final_lab: Dict[str, object] = {}
        final_native_targets: Dict[str, object] = {}
        final_guidance: Dict[str, object] = {}
        replayed_scenarios: List[str] = []
        for candidate in selected_candidates:
            scenario_name_value = str(candidate.get("scenario", "") or "").strip()
            replay_payload = self.replay_lab_session(
                session_id=str(session.get("session_id", session_id) or session_id).strip(),
                scenario_name=scenario_name_value,
            )
            results.append(
                {
                    "scenario": scenario_name_value,
                    "status": str(replay_payload.get("status", "") or "").strip() or "success",
                    "updated_candidate": dict(replay_payload.get("updated_candidate", {}))
                    if isinstance(replay_payload.get("updated_candidate", {}), dict)
                    else {},
                    "replay_result": dict(replay_payload.get("replay_result", {}))
                    if isinstance(replay_payload.get("replay_result", {}), dict)
                    else {},
                }
            )
            if str(replay_payload.get("status", "") or "").strip().lower() != "success":
                final_session = dict(replay_payload.get("session", {})) if isinstance(replay_payload.get("session", {}), dict) else final_session
                break
            replayed_scenarios.append(scenario_name_value)
            if isinstance(replay_payload.get("session", {}), dict):
                final_session = dict(replay_payload.get("session", {}))
            if isinstance(replay_payload.get("lab", {}), dict):
                final_lab = dict(replay_payload.get("lab", {}))
            if isinstance(replay_payload.get("native_targets", {}), dict):
                final_native_targets = dict(replay_payload.get("native_targets", {}))
            if isinstance(replay_payload.get("guidance", {}), dict):
                final_guidance = dict(replay_payload.get("guidance", {}))
        batch_status = "success"
        if any(str(item.get("status", "") or "").strip().lower() != "success" for item in results):
            batch_status = "partial"
        return {
            "status": batch_status,
            "session": final_session,
            "results": results,
            "batch_count": len(results),
            "replayed_scenarios": replayed_scenarios,
            "lab": final_lab,
            "native_targets": final_native_targets,
            "guidance": final_guidance,
        }

    def _lab_session_refresh_payloads(
        self,
        *,
        session_filters: Dict[str, object],
        history_limit: int,
    ) -> Dict[str, Dict[str, object]]:
        run_kwargs = self._filters_to_run_kwargs(session_filters)
        return {
            "lab": self.lab(**run_kwargs, history_limit=history_limit),
            "native_targets": self.native_control_targets(**run_kwargs, history_limit=history_limit),
            "guidance": self.control_guidance(),
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
        latest_rows_by_name = {
            str(row.get("scenario", "") or "").strip(): dict(row)
            for row in self.last_items
            if isinstance(row, dict) and str(row.get("scenario", "") or "").strip()
        }
        replay_candidates = (
            [
                dict(item)
                for item in lab_payload.get("replay_candidates", [])
                if isinstance(item, dict)
            ]
            if isinstance(lab_payload, dict)
            else []
        )
        lab_sessions_payload = (
            self.lab_sessions(limit=max(12, history_limit * 3))
            if self.lab_memory is not None
            else {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        )
        replay_sessions = [
            dict(item)
            for item in lab_sessions_payload.get("items", [])
            if isinstance(item, dict) and self._lab_session_matches_filters(session=item, filters=filters)
        ] if isinstance(lab_sessions_payload, dict) and isinstance(lab_sessions_payload.get("items", []), list) else []
        lab_campaigns_payload = (
            self.lab_campaigns(limit=max(8, history_limit * 2))
            if self.lab_memory is not None
            else {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        )
        replay_campaigns = [
            dict(item)
            for item in lab_campaigns_payload.get("items", [])
            if isinstance(item, dict)
            and self._filters_match(
                dict(item.get("filters", {})) if isinstance(item.get("filters", {}), dict) else {},
                filters,
            )
        ] if isinstance(lab_campaigns_payload, dict) and isinstance(lab_campaigns_payload.get("items", []), list) else []
        lab_programs_payload = (
            self.lab_programs(limit=max(6, history_limit * 2))
            if self.lab_memory is not None
            else {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        )
        replay_programs = [
            dict(item)
            for item in lab_programs_payload.get("items", [])
            if isinstance(item, dict)
            and self._filters_match(
                dict(item.get("filters", {})) if isinstance(item.get("filters", {}), dict) else {},
                filters,
            )
        ] if isinstance(lab_programs_payload, dict) and isinstance(lab_programs_payload.get("items", []), list) else []
        lab_portfolios_payload = (
            self.lab_portfolios(limit=max(4, history_limit * 2))
            if self.lab_memory is not None
            else {"status": "unavailable", "message": "desktop benchmark lab memory unavailable"}
        )
        replay_portfolios = [
            dict(item)
            for item in lab_portfolios_payload.get("items", [])
            if isinstance(item, dict)
            and self._filters_match(
                dict(item.get("filters", {})) if isinstance(item.get("filters", {}), dict) else {},
                filters,
            )
        ] if isinstance(lab_portfolios_payload, dict) and isinstance(lab_portfolios_payload.get("items", []), list) else []
        target_apps: Dict[str, Dict[str, object]] = {}
        tactic_totals = {
            "dialog_resolution": 0.0,
            "descendant_focus": 0.0,
            "navigation_branch": 0.0,
            "recovery_reacquire": 0.0,
            "loop_guard": 0.0,
            "native_focus": 0.0,
        }
        replay_session_summary = {
            "session_count": len(replay_sessions),
            "pending_replays": 0,
            "failed_replays": 0,
            "completed_replays": 0,
            "replayable_candidates": 0,
            "cycle_count": 0,
            "regression_cycle_count": 0,
            "long_horizon_pending_count": 0,
            "latest_session_id": "",
            "latest_session_label": "",
            "latest_cycle_regression_status": "",
        }
        replay_campaign_summary = {
            "campaign_count": len(replay_campaigns),
            "sweep_count": 0,
            "pending_session_count": 0,
            "attention_session_count": 0,
            "pending_app_target_count": 0,
            "regression_cycle_count": 0,
            "long_horizon_pending_count": 0,
            "latest_campaign_id": "",
            "latest_campaign_label": "",
            "latest_sweep_status": "",
            "latest_sweep_regression_status": "",
        }
        replay_program_summary = {
            "program_count": len(replay_programs),
            "campaign_count": 0,
            "cycle_count": 0,
            "pending_campaign_count": 0,
            "attention_campaign_count": 0,
            "pending_session_count": 0,
            "attention_session_count": 0,
            "pending_app_target_count": 0,
            "regression_cycle_count": 0,
            "long_horizon_pending_count": 0,
            "latest_program_id": "",
            "latest_program_label": "",
            "latest_cycle_status": "",
            "latest_cycle_stop_reason": "",
        }
        replay_portfolio_summary = {
            "portfolio_count": len(replay_portfolios),
            "program_count": 0,
            "wave_count": 0,
            "pending_program_count": 0,
            "attention_program_count": 0,
            "pending_campaign_count": 0,
            "pending_session_count": 0,
            "pending_app_target_count": 0,
            "regression_wave_count": 0,
            "long_horizon_pending_count": 0,
            "latest_portfolio_id": "",
            "latest_portfolio_label": "",
            "latest_wave_status": "",
            "latest_wave_stop_reason": "",
        }

        def _ordered_descendant_sequence(*titles: object) -> List[str]:
            ordered: List[str] = []
            for title in titles:
                values = title if isinstance(title, list) else [title]
                for value in values:
                    clean = str(value or "").strip()
                    if clean and clean not in ordered:
                        ordered.append(clean)
            return ordered[:8]

        def _ensure_target_entry(app_name_value: str) -> Dict[str, object]:
            return target_apps.setdefault(
                app_name_value,
                {
                    "app_name": app_name_value,
                    "priority": 0.0,
                    "scenario_names": [],
                    "packs": set(),
                    "mission_families": set(),
                    "query_hints": [],
                    "descendant_title_hints": [],
                    "descendant_title_sequence": [],
                    "descendant_hint_query": "",
                    "preferred_window_title": "",
                    "max_horizon_steps": 0,
                    "hint_query": "",
                    "replay_pressure": 0.0,
                    "replay_session_ids": set(),
                    "replay_session_labels": [],
                    "replay_scenarios": [],
                    "replay_pending_count": 0,
                    "replay_failed_count": 0,
                    "replay_completed_count": 0,
                    "campaign_ids": set(),
                    "campaign_labels": [],
                    "campaign_focus_summary": [],
                    "campaign_count": 0,
                    "campaign_sweep_count": 0,
                    "campaign_pending_session_count": 0,
                    "campaign_attention_session_count": 0,
                    "campaign_pending_app_target_count": 0,
                    "campaign_regression_cycle_count": 0,
                    "campaign_long_horizon_pending_count": 0,
                    "campaign_pressure": 0.0,
                    "campaign_hint_query": "",
                    "campaign_descendant_title_hints": [],
                    "campaign_descendant_title_sequence": [],
                    "campaign_descendant_hint_query": "",
                    "campaign_preferred_window_title": "",
                    "campaign_latest_sweep_status": "",
                    "campaign_latest_sweep_regression_status": "",
                    "program_ids": set(),
                    "program_labels": [],
                    "program_count": 0,
                    "program_cycle_count": 0,
                    "program_pending_campaign_count": 0,
                    "program_attention_campaign_count": 0,
                    "program_pending_app_target_count": 0,
                    "program_regression_cycle_count": 0,
                    "program_long_horizon_pending_count": 0,
                    "program_pressure": 0.0,
                    "program_hint_query": "",
                    "program_descendant_title_hints": [],
                    "program_descendant_title_sequence": [],
                    "program_descendant_hint_query": "",
                    "program_preferred_window_title": "",
                    "program_latest_cycle_status": "",
                    "program_latest_cycle_stop_reason": "",
                    "portfolio_ids": set(),
                    "portfolio_labels": [],
                    "portfolio_count": 0,
                    "portfolio_wave_count": 0,
                    "portfolio_pending_program_count": 0,
                    "portfolio_attention_program_count": 0,
                    "portfolio_pending_campaign_count": 0,
                    "portfolio_pending_session_count": 0,
                    "portfolio_pending_app_target_count": 0,
                    "portfolio_regression_wave_count": 0,
                    "portfolio_long_horizon_pending_count": 0,
                    "portfolio_pressure": 0.0,
                    "portfolio_hint_query": "",
                    "portfolio_descendant_title_hints": [],
                    "portfolio_descendant_title_sequence": [],
                    "portfolio_descendant_hint_query": "",
                    "portfolio_preferred_window_title": "",
                    "portfolio_confirmation_pressure": 0.0,
                    "portfolio_confirmation_title_hints": [],
                    "portfolio_confirmation_title_sequence": [],
                    "portfolio_confirmation_hint_query": "",
                    "portfolio_confirmation_preferred_window_title": "",
                    "portfolio_latest_wave_status": "",
                    "portfolio_latest_wave_stop_reason": "",
                    "session_cycle_count": 0,
                    "session_regression_cycle_count": 0,
                    "session_long_horizon_pending_count": 0,
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

        def _ingest_native_target_candidate(
            candidate: Dict[str, Any],
            *,
            scenario: Scenario | None,
            row_fallback: Dict[str, Any],
            session: Dict[str, Any] | None = None,
        ) -> None:
            scenario_name_value = str(candidate.get("scenario", "") or "").strip()
            apps = [str(item).strip().lower() for item in scenario.apps if str(item).strip()] if scenario is not None else [
                str(item).strip().lower()
                for item in candidate.get("apps", row_fallback.get("apps", []))
                if str(item).strip()
            ] if isinstance(candidate.get("apps", row_fallback.get("apps", [])), list) else []
            if not apps:
                return
            tactic_profile = (
                self._scenario_native_tactic_profile(scenario=scenario)
                if scenario is not None
                else self._row_native_tactic_profile(row={**row_fallback, **candidate})
            )
            query_hints = (
                self._scenario_query_hints(scenario=scenario)
                if scenario is not None
                else self._row_query_hints(row={**row_fallback, **candidate})
            )
            pack_name = str(scenario.pack or "").strip() if scenario is not None else str(candidate.get("pack", row_fallback.get("pack", "")) or "").strip()
            mission_name = str(scenario.mission_family or "").strip() if scenario is not None else str(candidate.get("mission_family", row_fallback.get("mission_family", "")) or "").strip()
            max_horizon_steps = max(
                1,
                int(
                    scenario.horizon_steps
                    if scenario is not None
                    else candidate.get("horizon_steps", row_fallback.get("horizon_steps", 1))
                    or 1
                ),
            )
            replay_status = str(candidate.get("replay_status", "") or "").strip().lower()
            last_result_status = str(candidate.get("last_result_status", "") or "").strip().lower()
            last_regression_status = str(candidate.get("last_regression_status", "") or "").strip().lower()
            failed_replay = replay_status == "failed" or last_result_status == "failed" or last_regression_status in {"regressed", "failed"}
            completed_replay = replay_status == "completed" or last_result_status == "success"
            pending_replay = replay_status in {"pending", "ready", "queued", "staged"} or (
                not completed_replay and not failed_replay and session is not None
            )
            replay_hint_query = self._native_target_hint_query(
                query_hints=query_hints,
                replay_scenarios=[scenario_name_value] if scenario_name_value else [],
            )
            descendant_title_hints = self._native_target_descendant_title_hints(
                query_hints=query_hints,
                replay_scenarios=[scenario_name_value] if scenario_name_value else [],
            )
            descendant_hint_query = self._native_target_hint_query(
                query_hints=descendant_title_hints or query_hints,
                replay_scenarios=[scenario_name_value] if scenario_name_value else [],
            )
            preferred_window_title = self._native_target_preferred_window_title(
                descendant_title_hints=descendant_title_hints,
                query_hints=query_hints,
                replay_scenarios=[scenario_name_value] if scenario_name_value else [],
            )
            descendant_title_sequence = _ordered_descendant_sequence(
                descendant_title_hints,
                preferred_window_title,
            )
            session_id = str(session.get("session_id", "") or "").strip() if isinstance(session, dict) else ""
            session_label = str(session.get("label", "") or "").strip() if isinstance(session, dict) else ""
            session_cycle_count = int(session.get("cycle_count", 0) or 0) if isinstance(session, dict) else 0
            session_regression_cycle_count = int(session.get("regression_cycle_count", 0) or 0) if isinstance(session, dict) else 0
            session_long_horizon_pending_count = int(session.get("long_horizon_pending_count", 0) or 0) if isinstance(session, dict) else 0
            candidate_priority = float(candidate.get("weight", candidate.get("score", 0.0)) or 0.0) + max(
                0.0,
                1.0 - float(candidate.get("score", 0.0) or 0.0),
            )
            if session_id:
                candidate_priority += 0.08
            if session_regression_cycle_count > 0:
                candidate_priority += min(0.16, session_regression_cycle_count * 0.03)
            if max_horizon_steps >= 4 and session_long_horizon_pending_count > 0:
                candidate_priority += min(0.12, session_long_horizon_pending_count * 0.02)
            if failed_replay:
                candidate_priority += 0.18
            elif pending_replay:
                candidate_priority += 0.1
            elif completed_replay:
                candidate_priority += 0.04
            replay_pressure = max(
                0.0,
                min(
                    candidate_priority + (0.25 if failed_replay else 0.12 if pending_replay else 0.05 if completed_replay else 0.0),
                    6.0,
                ),
            )
            for app_name_value in apps:
                entry = _ensure_target_entry(app_name_value)
                entry["priority"] = float(entry.get("priority", 0.0) or 0.0) + candidate_priority
                scenario_names = entry["scenario_names"] if isinstance(entry.get("scenario_names"), list) else []
                if scenario_name_value and scenario_name_value not in scenario_names:
                    scenario_names.append(scenario_name_value)
                entry["scenario_names"] = scenario_names[:8]
                packs = entry["packs"] if isinstance(entry.get("packs"), set) else set(entry.get("packs", []))
                if pack_name:
                    packs.add(pack_name)
                entry["packs"] = packs
                missions = entry["mission_families"] if isinstance(entry.get("mission_families"), set) else set(entry.get("mission_families", []))
                if mission_name:
                    missions.add(mission_name)
                entry["mission_families"] = missions
                hints = entry["query_hints"] if isinstance(entry.get("query_hints"), list) else []
                for hint in query_hints:
                    if hint not in hints:
                        hints.append(hint)
                entry["query_hints"] = hints[:8]
                descendant_hints = entry["descendant_title_hints"] if isinstance(entry.get("descendant_title_hints"), list) else []
                for hint in descendant_title_hints:
                    if hint not in descendant_hints:
                        descendant_hints.append(hint)
                entry["descendant_title_hints"] = descendant_hints[:8]
                descendant_sequence = entry["descendant_title_sequence"] if isinstance(entry.get("descendant_title_sequence"), list) else []
                for hint in descendant_title_sequence:
                    if hint not in descendant_sequence:
                        descendant_sequence.append(hint)
                entry["descendant_title_sequence"] = descendant_sequence[:8]
                entry["max_horizon_steps"] = max(int(entry.get("max_horizon_steps", 0) or 0), max_horizon_steps)
                if replay_hint_query and not str(entry.get("hint_query", "") or "").strip():
                    entry["hint_query"] = replay_hint_query
                if descendant_hint_query and not str(entry.get("descendant_hint_query", "") or "").strip():
                    entry["descendant_hint_query"] = descendant_hint_query
                if preferred_window_title and not str(entry.get("preferred_window_title", "") or "").strip():
                    entry["preferred_window_title"] = preferred_window_title
                entry["replay_pressure"] = float(entry.get("replay_pressure", 0.0) or 0.0) + replay_pressure
                replay_scenarios = entry["replay_scenarios"] if isinstance(entry.get("replay_scenarios"), list) else []
                if scenario_name_value and scenario_name_value not in replay_scenarios:
                    replay_scenarios.append(scenario_name_value)
                entry["replay_scenarios"] = replay_scenarios[:8]
                if session_id:
                    replay_session_ids = entry["replay_session_ids"] if isinstance(entry.get("replay_session_ids"), set) else set(entry.get("replay_session_ids", []))
                    replay_session_ids.add(session_id)
                    entry["replay_session_ids"] = replay_session_ids
                if session_label:
                    replay_session_labels = entry["replay_session_labels"] if isinstance(entry.get("replay_session_labels"), list) else []
                    if session_label not in replay_session_labels:
                        replay_session_labels.append(session_label)
                    entry["replay_session_labels"] = replay_session_labels[:6]
                if pending_replay:
                    entry["replay_pending_count"] = int(entry.get("replay_pending_count", 0) or 0) + 1
                if failed_replay:
                    entry["replay_failed_count"] = int(entry.get("replay_failed_count", 0) or 0) + 1
                if completed_replay:
                    entry["replay_completed_count"] = int(entry.get("replay_completed_count", 0) or 0) + 1
                entry["session_cycle_count"] = max(
                    int(entry.get("session_cycle_count", 0) or 0),
                    session_cycle_count,
                )
                entry["session_regression_cycle_count"] = max(
                    int(entry.get("session_regression_cycle_count", 0) or 0),
                    session_regression_cycle_count,
                )
                entry["session_long_horizon_pending_count"] = max(
                    int(entry.get("session_long_horizon_pending_count", 0) or 0),
                    session_long_horizon_pending_count,
                )
                control_biases = (
                    dict(entry.get("control_biases", {}))
                    if isinstance(entry.get("control_biases", {}), dict)
                    else {}
                )
                for key, value in tactic_profile.items():
                    tactic_value = max(0.0, min(float(value or 0.0), 1.0))
                    if failed_replay and key in {"descendant_focus", "dialog_resolution", "recovery_reacquire", "native_focus"}:
                        tactic_value = min(1.0, tactic_value + 0.06)
                    elif pending_replay and key in {"recovery_reacquire", "native_focus"}:
                        tactic_value = min(1.0, tactic_value + 0.03)
                    control_biases[key] = max(float(control_biases.get(key, 0.0) or 0.0), tactic_value)
                    tactic_totals[key] += tactic_value
                entry["control_biases"] = control_biases

        def _ingest_native_target_campaign(
            campaign: Dict[str, Any],
            target_row: Dict[str, Any],
        ) -> None:
            app_name_value = str(target_row.get("app_name", "") or "").strip().lower()
            if not app_name_value:
                return
            entry = _ensure_target_entry(app_name_value)
            campaign_id = str(campaign.get("campaign_id", "") or "").strip()
            campaign_label = str(campaign.get("label", "") or "").strip()
            campaign_sweep_count = max(0, int(campaign.get("sweep_count", 0) or 0))
            campaign_pending_session_count = max(0, int(campaign.get("pending_session_count", 0) or 0))
            campaign_attention_session_count = max(0, int(campaign.get("attention_session_count", 0) or 0))
            campaign_pending_app_target_count = max(0, int(campaign.get("pending_app_target_count", 0) or 0))
            campaign_regression_cycle_count = max(0, int(campaign.get("regression_cycle_count", 0) or 0))
            campaign_long_horizon_pending_count = max(0, int(campaign.get("long_horizon_pending_count", 0) or 0))
            campaign_latest_sweep_status = str(campaign.get("latest_sweep_status", "") or "").strip().lower()
            campaign_latest_sweep_regression_status = str(
                campaign.get("latest_sweep_regression_status", "")
                or ""
            ).strip().lower()
            campaign_priority = max(
                0.0,
                min(
                    6.0,
                    (0.2 * min(max(0.0, float(target_row.get("priority", 0.0) or 0.0)), 6.0))
                    + (0.25 * min(max(0.0, float(target_row.get("replay_pressure", 0.0) or 0.0)), 4.0))
                    + (0.12 * min(campaign_sweep_count, 4))
                    + (0.18 * min(campaign_attention_session_count, 3))
                    + (0.08 * min(campaign_pending_session_count, 4))
                    + (0.1 * min(campaign_pending_app_target_count, 3))
                    + (0.1 * min(campaign_regression_cycle_count, 4))
                    + (0.06 * min(campaign_long_horizon_pending_count, 4))
                    + (0.12 if campaign_latest_sweep_regression_status in {"regression", "failed"} else 0.0)
                    + (0.08 if campaign_latest_sweep_status in {"error", "failed"} else 0.0),
                ),
            )
            entry["priority"] = float(entry.get("priority", 0.0) or 0.0) + campaign_priority
            entry["campaign_count"] = int(entry.get("campaign_count", 0) or 0) + 1
            entry["campaign_sweep_count"] = int(entry.get("campaign_sweep_count", 0) or 0) + campaign_sweep_count
            entry["campaign_pending_session_count"] = int(entry.get("campaign_pending_session_count", 0) or 0) + campaign_pending_session_count
            entry["campaign_attention_session_count"] = int(entry.get("campaign_attention_session_count", 0) or 0) + campaign_attention_session_count
            entry["campaign_pending_app_target_count"] = int(entry.get("campaign_pending_app_target_count", 0) or 0) + campaign_pending_app_target_count
            entry["campaign_regression_cycle_count"] = int(entry.get("campaign_regression_cycle_count", 0) or 0) + campaign_regression_cycle_count
            entry["campaign_long_horizon_pending_count"] = int(entry.get("campaign_long_horizon_pending_count", 0) or 0) + campaign_long_horizon_pending_count
            entry["campaign_pressure"] = float(entry.get("campaign_pressure", 0.0) or 0.0) + campaign_priority
            campaign_ids = entry["campaign_ids"] if isinstance(entry.get("campaign_ids"), set) else set(entry.get("campaign_ids", []))
            if campaign_id:
                campaign_ids.add(campaign_id)
            entry["campaign_ids"] = campaign_ids
            campaign_labels = entry["campaign_labels"] if isinstance(entry.get("campaign_labels"), list) else []
            if campaign_label and campaign_label not in campaign_labels:
                campaign_labels.append(campaign_label)
            entry["campaign_labels"] = campaign_labels[:6]
            campaign_focus_summary = entry["campaign_focus_summary"] if isinstance(entry.get("campaign_focus_summary"), list) else []
            for hint in campaign.get("focus_summary", []) if isinstance(campaign.get("focus_summary", []), list) else []:
                clean_hint = str(hint).strip()
                if clean_hint and clean_hint not in campaign_focus_summary:
                    campaign_focus_summary.append(clean_hint)
            entry["campaign_focus_summary"] = campaign_focus_summary[:6]
            query_hints = [
                str(item).strip()
                for item in target_row.get("query_hints", [])
                if str(item).strip()
            ] if isinstance(target_row.get("query_hints", []), list) else []
            descendant_title_hints = [
                str(item).strip()
                for item in target_row.get("descendant_title_hints", [])
                if str(item).strip()
            ] if isinstance(target_row.get("descendant_title_hints", []), list) else []
            descendant_title_sequence = _ordered_descendant_sequence(
                target_row.get("descendant_title_sequence", []),
                descendant_title_hints,
                target_row.get("preferred_window_title", ""),
            )
            hints = entry["query_hints"] if isinstance(entry.get("query_hints"), list) else []
            for hint in query_hints:
                if hint not in hints:
                    hints.append(hint)
            entry["query_hints"] = hints[:8]
            campaign_descendant_hints = entry["campaign_descendant_title_hints"] if isinstance(entry.get("campaign_descendant_title_hints"), list) else []
            for hint in descendant_title_hints:
                if hint not in campaign_descendant_hints:
                    campaign_descendant_hints.append(hint)
            entry["campaign_descendant_title_hints"] = campaign_descendant_hints[:8]
            descendant_hints = entry["descendant_title_hints"] if isinstance(entry.get("descendant_title_hints"), list) else []
            for hint in descendant_title_hints:
                if hint not in descendant_hints:
                    descendant_hints.append(hint)
            entry["descendant_title_hints"] = descendant_hints[:8]
            descendant_sequence_hints = entry["descendant_title_sequence"] if isinstance(entry.get("descendant_title_sequence"), list) else []
            for hint in descendant_title_sequence:
                if hint not in descendant_sequence_hints:
                    descendant_sequence_hints.append(hint)
            entry["descendant_title_sequence"] = descendant_sequence_hints[:8]
            hint_query = str(target_row.get("hint_query", "") or "").strip()
            descendant_hint_query = str(target_row.get("descendant_hint_query", "") or "").strip()
            preferred_window_title = str(target_row.get("preferred_window_title", "") or "").strip()
            campaign_descendant_title_sequence = _ordered_descendant_sequence(
                target_row.get("campaign_descendant_title_sequence", []),
                target_row.get("campaign_descendant_title_hints", []),
                descendant_title_sequence,
                target_row.get("campaign_preferred_window_title", ""),
            )
            if hint_query and not str(entry.get("hint_query", "") or "").strip():
                entry["hint_query"] = hint_query
            if descendant_hint_query and not str(entry.get("descendant_hint_query", "") or "").strip():
                entry["descendant_hint_query"] = descendant_hint_query
            if preferred_window_title and not str(entry.get("preferred_window_title", "") or "").strip():
                entry["preferred_window_title"] = preferred_window_title
            if hint_query and not str(entry.get("campaign_hint_query", "") or "").strip():
                entry["campaign_hint_query"] = hint_query
            if descendant_hint_query and not str(entry.get("campaign_descendant_hint_query", "") or "").strip():
                entry["campaign_descendant_hint_query"] = descendant_hint_query
            if preferred_window_title and not str(entry.get("campaign_preferred_window_title", "") or "").strip():
                entry["campaign_preferred_window_title"] = preferred_window_title
            campaign_sequence_hints = entry["campaign_descendant_title_sequence"] if isinstance(entry.get("campaign_descendant_title_sequence"), list) else []
            for hint in campaign_descendant_title_sequence:
                if hint not in campaign_sequence_hints:
                    campaign_sequence_hints.append(hint)
            entry["campaign_descendant_title_sequence"] = campaign_sequence_hints[:8]
            current_sweep_status = str(entry.get("campaign_latest_sweep_status", "") or "").strip().lower()
            current_sweep_regression_status = str(entry.get("campaign_latest_sweep_regression_status", "") or "").strip().lower()
            if campaign_latest_sweep_status and (
                not current_sweep_status
                or current_sweep_status in {"idle", "ready"}
                or campaign_latest_sweep_status in {"error", "failed"}
            ):
                entry["campaign_latest_sweep_status"] = campaign_latest_sweep_status
            if campaign_latest_sweep_regression_status and (
                not current_sweep_regression_status
                or current_sweep_regression_status in {"idle", "ready", "success"}
                or campaign_latest_sweep_regression_status in {"regression", "failed"}
            ):
                entry["campaign_latest_sweep_regression_status"] = campaign_latest_sweep_regression_status
            control_biases = (
                dict(entry.get("control_biases", {}))
                if isinstance(entry.get("control_biases", {}), dict)
                else {}
            )
            target_biases = (
                dict(target_row.get("control_biases", {}))
                if isinstance(target_row.get("control_biases", {}), dict)
                else {}
            )
            for key, value in target_biases.items():
                tactic_value = max(0.0, min(float(value or 0.0), 1.0))
                if campaign_latest_sweep_regression_status in {"regression", "failed"} and key in {"descendant_focus", "dialog_resolution", "recovery_reacquire", "native_focus"}:
                    tactic_value = min(1.0, tactic_value + 0.05)
                elif campaign_attention_session_count > 0 and key in {"recovery_reacquire", "native_focus"}:
                    tactic_value = min(1.0, tactic_value + 0.03)
                control_biases[key] = max(float(control_biases.get(key, 0.0) or 0.0), tactic_value)
                tactic_totals[key] += tactic_value
            entry["control_biases"] = control_biases

        def _ingest_native_target_program(
            program: Dict[str, Any],
            target_row: Dict[str, Any],
        ) -> None:
            app_name_value = str(target_row.get("app_name", "") or "").strip().lower()
            if not app_name_value:
                return
            entry = _ensure_target_entry(app_name_value)
            program_id = str(program.get("program_id", "") or "").strip()
            program_label = str(program.get("label", "") or "").strip()
            program_cycle_count = max(0, int(program.get("cycle_count", 0) or 0))
            program_pending_campaign_count = max(0, int(program.get("pending_campaign_count", 0) or 0))
            program_attention_campaign_count = max(0, int(program.get("attention_campaign_count", 0) or 0))
            program_pending_app_target_count = max(0, int(program.get("pending_app_target_count", 0) or 0))
            program_regression_cycle_count = max(0, int(program.get("regression_cycle_count", 0) or 0))
            program_long_horizon_pending_count = max(0, int(program.get("long_horizon_pending_count", 0) or 0))
            program_latest_cycle_status = str(program.get("latest_cycle_status", "") or "").strip().lower()
            program_latest_cycle_stop_reason = str(program.get("latest_cycle_stop_reason", "") or "").strip().lower()
            program_priority = max(
                0.0,
                min(
                    8.0,
                    (0.22 * min(max(0.0, float(target_row.get("priority", 0.0) or 0.0)), 8.0))
                    + (0.28 * min(max(0.0, float(target_row.get("campaign_pressure", 0.0) or 0.0)), 6.0))
                    + (0.16 * min(program_cycle_count, 5))
                    + (0.16 * min(program_attention_campaign_count, 3))
                    + (0.12 * min(program_pending_campaign_count, 4))
                    + (0.12 * min(program_pending_app_target_count, 3))
                    + (0.12 * min(program_regression_cycle_count, 4))
                    + (0.08 * min(program_long_horizon_pending_count, 4))
                    + (0.14 if program_latest_cycle_status in {"error", "failed"} else 0.0)
                ),
            )
            entry["priority"] = float(entry.get("priority", 0.0) or 0.0) + program_priority
            entry["program_count"] = int(entry.get("program_count", 0) or 0) + 1
            entry["program_cycle_count"] = int(entry.get("program_cycle_count", 0) or 0) + program_cycle_count
            entry["program_pending_campaign_count"] = int(entry.get("program_pending_campaign_count", 0) or 0) + program_pending_campaign_count
            entry["program_attention_campaign_count"] = int(entry.get("program_attention_campaign_count", 0) or 0) + program_attention_campaign_count
            entry["program_pending_app_target_count"] = int(entry.get("program_pending_app_target_count", 0) or 0) + program_pending_app_target_count
            entry["program_regression_cycle_count"] = int(entry.get("program_regression_cycle_count", 0) or 0) + program_regression_cycle_count
            entry["program_long_horizon_pending_count"] = int(entry.get("program_long_horizon_pending_count", 0) or 0) + program_long_horizon_pending_count
            entry["program_pressure"] = float(entry.get("program_pressure", 0.0) or 0.0) + program_priority
            program_ids = entry["program_ids"] if isinstance(entry.get("program_ids"), set) else set(entry.get("program_ids", []))
            if program_id:
                program_ids.add(program_id)
            entry["program_ids"] = program_ids
            program_labels = entry["program_labels"] if isinstance(entry.get("program_labels"), list) else []
            if program_label and program_label not in program_labels:
                program_labels.append(program_label)
            entry["program_labels"] = program_labels[:6]
            query_hints = [
                str(item).strip()
                for item in target_row.get("query_hints", [])
                if str(item).strip()
            ] if isinstance(target_row.get("query_hints", []), list) else []
            descendant_title_hints = [
                str(item).strip()
                for item in target_row.get("descendant_title_hints", [])
                if str(item).strip()
            ] if isinstance(target_row.get("descendant_title_hints", []), list) else []
            descendant_title_sequence = _ordered_descendant_sequence(
                target_row.get("descendant_title_sequence", []),
                descendant_title_hints,
                target_row.get("preferred_window_title", ""),
            )
            program_descendant_hints = entry["program_descendant_title_hints"] if isinstance(entry.get("program_descendant_title_hints"), list) else []
            for hint in descendant_title_hints:
                if hint not in program_descendant_hints:
                    program_descendant_hints.append(hint)
            entry["program_descendant_title_hints"] = program_descendant_hints[:8]
            program_descendant_sequence = _ordered_descendant_sequence(
                target_row.get("program_descendant_title_sequence", []),
                target_row.get("program_descendant_title_hints", []),
                descendant_title_sequence,
                target_row.get("program_preferred_window_title", ""),
            )
            program_sequence_hints = entry["program_descendant_title_sequence"] if isinstance(entry.get("program_descendant_title_sequence"), list) else []
            for hint in program_descendant_sequence:
                if hint not in program_sequence_hints:
                    program_sequence_hints.append(hint)
            entry["program_descendant_title_sequence"] = program_sequence_hints[:8]
            hint_query = str(target_row.get("hint_query", "") or "").strip()
            descendant_hint_query = str(target_row.get("descendant_hint_query", "") or "").strip()
            preferred_window_title = str(target_row.get("preferred_window_title", "") or "").strip()
            if hint_query and not str(entry.get("program_hint_query", "") or "").strip():
                entry["program_hint_query"] = hint_query
            if descendant_hint_query and not str(entry.get("program_descendant_hint_query", "") or "").strip():
                entry["program_descendant_hint_query"] = descendant_hint_query
            if preferred_window_title and not str(entry.get("program_preferred_window_title", "") or "").strip():
                entry["program_preferred_window_title"] = preferred_window_title
            if program_latest_cycle_status and (
                not str(entry.get("program_latest_cycle_status", "") or "").strip()
                or str(entry.get("program_latest_cycle_status", "") or "").strip().lower() in {"idle", "ready"}
                or program_latest_cycle_status in {"error", "failed"}
            ):
                entry["program_latest_cycle_status"] = program_latest_cycle_status
            if program_latest_cycle_stop_reason and not str(entry.get("program_latest_cycle_stop_reason", "") or "").strip():
                entry["program_latest_cycle_stop_reason"] = program_latest_cycle_stop_reason
            control_biases = (
                dict(entry.get("control_biases", {}))
                if isinstance(entry.get("control_biases", {}), dict)
                else {}
            )
            target_biases = (
                dict(target_row.get("control_biases", {}))
                if isinstance(target_row.get("control_biases", {}), dict)
                else {}
            )
            for key, value in target_biases.items():
                tactic_value = max(0.0, min(float(value or 0.0), 1.0))
                if program_regression_cycle_count > 0 and key in {"descendant_focus", "dialog_resolution", "recovery_reacquire", "native_focus"}:
                    tactic_value = min(1.0, tactic_value + 0.05)
                elif program_pending_campaign_count > 0 and key in {"navigation_branch", "recovery_reacquire"}:
                    tactic_value = min(1.0, tactic_value + 0.03)
                control_biases[key] = max(float(control_biases.get(key, 0.0) or 0.0), tactic_value)
                tactic_totals[key] += tactic_value
            entry["control_biases"] = control_biases

        def _ingest_native_target_portfolio(
            portfolio: Dict[str, Any],
            target_row: Dict[str, Any],
        ) -> None:
            app_name_value = str(target_row.get("app_name", "") or "").strip().lower()
            if not app_name_value:
                return
            entry = _ensure_target_entry(app_name_value)
            portfolio_id = str(portfolio.get("portfolio_id", "") or "").strip()
            portfolio_label = str(portfolio.get("label", "") or "").strip()
            portfolio_wave_count = max(0, int(portfolio.get("wave_count", 0) or 0))
            portfolio_pending_program_count = max(0, int(portfolio.get("pending_program_count", 0) or 0))
            portfolio_attention_program_count = max(0, int(portfolio.get("attention_program_count", 0) or 0))
            portfolio_pending_campaign_count = max(0, int(portfolio.get("pending_campaign_count", 0) or 0))
            portfolio_pending_session_count = max(0, int(portfolio.get("pending_session_count", 0) or 0))
            portfolio_pending_app_target_count = max(0, int(portfolio.get("pending_app_target_count", 0) or 0))
            portfolio_regression_wave_count = max(0, int(portfolio.get("regression_wave_count", 0) or 0))
            portfolio_long_horizon_pending_count = max(0, int(portfolio.get("long_horizon_pending_count", 0) or 0))
            portfolio_latest_wave_status = str(portfolio.get("latest_wave_status", "") or "").strip().lower()
            portfolio_latest_wave_stop_reason = str(portfolio.get("latest_wave_stop_reason", "") or "").strip().lower()
            portfolio_priority = max(
                0.0,
                min(
                    8.0,
                    (0.14 * min(max(0.0, float(target_row.get("priority", 0.0) or 0.0)), 8.0))
                    + (0.14 * min(max(0.0, float(target_row.get("replay_pressure", 0.0) or 0.0)), 5.0))
                    + (0.14 * min(max(0.0, float(target_row.get("campaign_pressure", 0.0) or 0.0)), 5.0))
                    + (0.18 * min(max(0.0, float(target_row.get("program_pressure", 0.0) or 0.0)), 5.0))
                    + (0.18 * min(portfolio_wave_count, 5))
                    + (0.18 * min(portfolio_attention_program_count, 4))
                    + (0.12 * min(portfolio_pending_program_count, 4))
                    + (0.1 * min(portfolio_pending_campaign_count, 4))
                    + (0.1 * min(portfolio_pending_session_count, 4))
                    + (0.12 * min(portfolio_pending_app_target_count, 4))
                    + (0.16 * min(portfolio_regression_wave_count, 4))
                    + (0.08 * min(portfolio_long_horizon_pending_count, 4))
                    + (0.14 if portfolio_latest_wave_status in {"error", "failed"} else 0.0),
                ),
            )
            entry["priority"] = float(entry.get("priority", 0.0) or 0.0) + portfolio_priority
            entry["portfolio_count"] = int(entry.get("portfolio_count", 0) or 0) + 1
            entry["portfolio_wave_count"] = int(entry.get("portfolio_wave_count", 0) or 0) + portfolio_wave_count
            entry["portfolio_pending_program_count"] = int(entry.get("portfolio_pending_program_count", 0) or 0) + portfolio_pending_program_count
            entry["portfolio_attention_program_count"] = int(entry.get("portfolio_attention_program_count", 0) or 0) + portfolio_attention_program_count
            entry["portfolio_pending_campaign_count"] = int(entry.get("portfolio_pending_campaign_count", 0) or 0) + portfolio_pending_campaign_count
            entry["portfolio_pending_session_count"] = int(entry.get("portfolio_pending_session_count", 0) or 0) + portfolio_pending_session_count
            entry["portfolio_pending_app_target_count"] = int(entry.get("portfolio_pending_app_target_count", 0) or 0) + portfolio_pending_app_target_count
            entry["portfolio_regression_wave_count"] = int(entry.get("portfolio_regression_wave_count", 0) or 0) + portfolio_regression_wave_count
            entry["portfolio_long_horizon_pending_count"] = int(entry.get("portfolio_long_horizon_pending_count", 0) or 0) + portfolio_long_horizon_pending_count
            entry["portfolio_pressure"] = float(entry.get("portfolio_pressure", 0.0) or 0.0) + portfolio_priority
            portfolio_ids = entry["portfolio_ids"] if isinstance(entry.get("portfolio_ids"), set) else set(entry.get("portfolio_ids", []))
            if portfolio_id:
                portfolio_ids.add(portfolio_id)
            entry["portfolio_ids"] = portfolio_ids
            portfolio_labels = entry["portfolio_labels"] if isinstance(entry.get("portfolio_labels"), list) else []
            if portfolio_label and portfolio_label not in portfolio_labels:
                portfolio_labels.append(portfolio_label)
            entry["portfolio_labels"] = portfolio_labels[:6]
            portfolio_hints = self._unique_strings(
                [
                    str(item).strip()
                    for item in target_row.get(
                        "portfolio_descendant_title_hints",
                        target_row.get(
                            "program_descendant_title_hints",
                            target_row.get("campaign_descendant_title_hints", []),
                        ),
                    )
                    if str(item).strip()
                ]
            )[:8]
            portfolio_descendant_hints = entry["portfolio_descendant_title_hints"] if isinstance(entry.get("portfolio_descendant_title_hints"), list) else []
            for hint in portfolio_hints:
                if hint not in portfolio_descendant_hints:
                    portfolio_descendant_hints.append(hint)
            entry["portfolio_descendant_title_hints"] = portfolio_descendant_hints[:8]
            portfolio_sequence = _ordered_descendant_sequence(
                target_row.get(
                    "portfolio_descendant_title_sequence",
                    target_row.get(
                        "program_descendant_title_sequence",
                        target_row.get("campaign_descendant_title_sequence", []),
                    ),
                ),
                portfolio_hints,
                target_row.get("portfolio_preferred_window_title", ""),
                target_row.get("program_preferred_window_title", ""),
                target_row.get("campaign_preferred_window_title", ""),
            )
            portfolio_sequence_hints = entry["portfolio_descendant_title_sequence"] if isinstance(entry.get("portfolio_descendant_title_sequence"), list) else []
            for hint in portfolio_sequence:
                if hint not in portfolio_sequence_hints:
                    portfolio_sequence_hints.append(hint)
            entry["portfolio_descendant_title_sequence"] = portfolio_sequence_hints[:8]
            portfolio_confirmation_hints = self._native_target_confirmation_title_hints(
                primary_titles=[
                    *(
                        [
                            str(item).strip()
                            for item in target_row.get("portfolio_confirmation_title_hints", [])
                            if str(item).strip()
                        ]
                        if isinstance(target_row.get("portfolio_confirmation_title_hints", []), list)
                        else []
                    ),
                    *portfolio_hints,
                ],
                fallback_titles=[
                    *portfolio_sequence,
                    str(target_row.get("portfolio_confirmation_preferred_window_title", "") or "").strip(),
                    str(target_row.get("portfolio_preferred_window_title", "") or "").strip(),
                ],
                stop_reason=portfolio_latest_wave_stop_reason,
            )
            portfolio_confirmation_sequence = _ordered_descendant_sequence(
                target_row.get("portfolio_confirmation_title_sequence", []),
                portfolio_confirmation_hints,
                target_row.get("portfolio_confirmation_preferred_window_title", ""),
                target_row.get("portfolio_preferred_window_title", ""),
            )
            portfolio_confirmation_sequence_hints = (
                entry["portfolio_confirmation_title_sequence"]
                if isinstance(entry.get("portfolio_confirmation_title_sequence"), list)
                else []
            )
            for hint in portfolio_confirmation_sequence:
                if hint not in portfolio_confirmation_sequence_hints:
                    portfolio_confirmation_sequence_hints.append(hint)
            entry["portfolio_confirmation_title_sequence"] = portfolio_confirmation_sequence_hints[:8]
            existing_portfolio_confirmation_hints = (
                entry["portfolio_confirmation_title_hints"]
                if isinstance(entry.get("portfolio_confirmation_title_hints"), list)
                else []
            )
            for hint in portfolio_confirmation_hints:
                if hint not in existing_portfolio_confirmation_hints:
                    existing_portfolio_confirmation_hints.append(hint)
            entry["portfolio_confirmation_title_hints"] = existing_portfolio_confirmation_hints[:8]
            hint_query = str(target_row.get("portfolio_hint_query", "") or "").strip()
            descendant_hint_query = str(target_row.get("portfolio_descendant_hint_query", "") or "").strip()
            preferred_window_title = str(target_row.get("portfolio_preferred_window_title", "") or "").strip()
            confirmation_hint_query = str(
                target_row.get("portfolio_confirmation_hint_query", "") or ""
            ).strip()
            if not confirmation_hint_query and portfolio_confirmation_hints:
                confirmation_hint_query = self._native_target_hint_query(
                    query_hints=portfolio_confirmation_hints,
                    replay_scenarios=[portfolio_label] if portfolio_label else [],
                )
            confirmation_preferred_window_title = str(
                target_row.get("portfolio_confirmation_preferred_window_title", "") or ""
            ).strip()
            if not confirmation_preferred_window_title and portfolio_confirmation_hints:
                confirmation_preferred_window_title = self._native_target_preferred_window_title(
                    descendant_title_hints=portfolio_confirmation_hints,
                    query_hints=portfolio_confirmation_hints,
                    replay_scenarios=[portfolio_label] if portfolio_label else [],
                )
            confirmation_pressure = self._native_target_confirmation_pressure(
                confirmation_titles=portfolio_confirmation_hints,
                portfolio_pressure=portfolio_priority,
                regression_wave_count=portfolio_regression_wave_count,
                long_horizon_pending_count=portfolio_long_horizon_pending_count,
                latest_wave_stop_reason=portfolio_latest_wave_stop_reason,
            )
            if hint_query and not str(entry.get("portfolio_hint_query", "") or "").strip():
                entry["portfolio_hint_query"] = hint_query
            if descendant_hint_query and not str(entry.get("portfolio_descendant_hint_query", "") or "").strip():
                entry["portfolio_descendant_hint_query"] = descendant_hint_query
            if preferred_window_title and not str(entry.get("portfolio_preferred_window_title", "") or "").strip():
                entry["portfolio_preferred_window_title"] = preferred_window_title
            if confirmation_hint_query and not str(entry.get("portfolio_confirmation_hint_query", "") or "").strip():
                entry["portfolio_confirmation_hint_query"] = confirmation_hint_query
            if confirmation_preferred_window_title and not str(entry.get("portfolio_confirmation_preferred_window_title", "") or "").strip():
                entry["portfolio_confirmation_preferred_window_title"] = confirmation_preferred_window_title
            if confirmation_pressure > 0.0:
                entry["portfolio_confirmation_pressure"] = max(
                    float(entry.get("portfolio_confirmation_pressure", 0.0) or 0.0),
                    confirmation_pressure,
                )
            if portfolio_latest_wave_status and (
                not str(entry.get("portfolio_latest_wave_status", "") or "").strip()
                or str(entry.get("portfolio_latest_wave_status", "") or "").strip().lower() in {"idle", "ready"}
                or portfolio_latest_wave_status in {"error", "failed"}
            ):
                entry["portfolio_latest_wave_status"] = portfolio_latest_wave_status
            if portfolio_latest_wave_stop_reason and not str(entry.get("portfolio_latest_wave_stop_reason", "") or "").strip():
                entry["portfolio_latest_wave_stop_reason"] = portfolio_latest_wave_stop_reason
            control_biases = (
                dict(entry.get("control_biases", {}))
                if isinstance(entry.get("control_biases", {}), dict)
                else {}
            )
            target_biases = (
                dict(target_row.get("control_biases", {}))
                if isinstance(target_row.get("control_biases", {}), dict)
                else {}
            )
            for key, value in target_biases.items():
                tactic_value = max(0.0, min(float(value or 0.0), 1.0))
                if portfolio_regression_wave_count > 0 and key in {"descendant_focus", "dialog_resolution", "recovery_reacquire", "native_focus"}:
                    tactic_value = min(1.0, tactic_value + 0.08)
                elif portfolio_pending_program_count > 0 and key in {"navigation_branch", "recovery_reacquire", "native_focus"}:
                    tactic_value = min(1.0, tactic_value + 0.04)
                if confirmation_pressure > 0.0 and key in {"dialog_resolution", "descendant_focus", "native_focus"}:
                    tactic_value = min(1.0, tactic_value + min(0.08, confirmation_pressure * 0.04))
                control_biases[key] = max(float(control_biases.get(key, 0.0) or 0.0), tactic_value)
                tactic_totals[key] += tactic_value
            entry["control_biases"] = control_biases

        for candidate in replay_candidates:
            scenario_name_value = str(candidate.get("scenario", "") or "").strip()
            scenario = scenario_by_name.get(scenario_name_value)
            row_fallback = dict(latest_rows_by_name.get(scenario_name_value, {}))
            _ingest_native_target_candidate(candidate, scenario=scenario, row_fallback=row_fallback)
        for session in replay_sessions:
            session_id = str(session.get("session_id", "") or "").strip()
            session_label = str(session.get("label", "") or "").strip()
            if session_id and not replay_session_summary["latest_session_id"]:
                replay_session_summary["latest_session_id"] = session_id
                replay_session_summary["latest_session_label"] = session_label
            replay_session_summary["pending_replays"] = int(replay_session_summary["pending_replays"]) + int(session.get("pending_replay_count", 0) or 0)
            replay_session_summary["failed_replays"] = int(replay_session_summary["failed_replays"]) + int(session.get("failed_replay_count", 0) or 0)
            replay_session_summary["completed_replays"] = int(replay_session_summary["completed_replays"]) + int(session.get("completed_replay_count", 0) or 0)
            replay_session_summary["cycle_count"] = int(replay_session_summary["cycle_count"]) + int(session.get("cycle_count", 0) or 0)
            replay_session_summary["regression_cycle_count"] = int(replay_session_summary["regression_cycle_count"]) + int(session.get("regression_cycle_count", 0) or 0)
            replay_session_summary["long_horizon_pending_count"] = int(replay_session_summary["long_horizon_pending_count"]) + int(session.get("long_horizon_pending_count", 0) or 0)
            if not replay_session_summary["latest_cycle_regression_status"]:
                replay_session_summary["latest_cycle_regression_status"] = str(
                    session.get("latest_cycle_regression_status", session.get("latest_cycle_status", "")) or ""
                ).strip()
            replay_candidate_rows = [
                dict(item)
                for item in session.get("replay_candidates", [])
                if isinstance(item, dict)
            ] if isinstance(session.get("replay_candidates", []), list) else []
            replay_session_summary["replayable_candidates"] = int(replay_session_summary["replayable_candidates"]) + len(replay_candidate_rows)
            for candidate in replay_candidate_rows:
                scenario_name_value = str(candidate.get("scenario", "") or "").strip()
                scenario = scenario_by_name.get(scenario_name_value)
                row_fallback = dict(latest_rows_by_name.get(scenario_name_value, {}))
                _ingest_native_target_candidate(candidate, scenario=scenario, row_fallback=row_fallback, session=session)
        for campaign in replay_campaigns:
            campaign_id = str(campaign.get("campaign_id", "") or "").strip()
            campaign_label = str(campaign.get("label", "") or "").strip()
            if campaign_id and not replay_campaign_summary["latest_campaign_id"]:
                replay_campaign_summary["latest_campaign_id"] = campaign_id
                replay_campaign_summary["latest_campaign_label"] = campaign_label
            replay_campaign_summary["sweep_count"] = int(replay_campaign_summary["sweep_count"]) + int(campaign.get("sweep_count", 0) or 0)
            replay_campaign_summary["pending_session_count"] = int(replay_campaign_summary["pending_session_count"]) + int(campaign.get("pending_session_count", 0) or 0)
            replay_campaign_summary["attention_session_count"] = int(replay_campaign_summary["attention_session_count"]) + int(campaign.get("attention_session_count", 0) or 0)
            replay_campaign_summary["pending_app_target_count"] = int(replay_campaign_summary["pending_app_target_count"]) + int(campaign.get("pending_app_target_count", 0) or 0)
            replay_campaign_summary["regression_cycle_count"] = int(replay_campaign_summary["regression_cycle_count"]) + int(campaign.get("regression_cycle_count", 0) or 0)
            replay_campaign_summary["long_horizon_pending_count"] = int(replay_campaign_summary["long_horizon_pending_count"]) + int(campaign.get("long_horizon_pending_count", 0) or 0)
            if not replay_campaign_summary["latest_sweep_status"]:
                replay_campaign_summary["latest_sweep_status"] = str(campaign.get("latest_sweep_status", "") or "").strip()
            if not replay_campaign_summary["latest_sweep_regression_status"]:
                replay_campaign_summary["latest_sweep_regression_status"] = str(
                    campaign.get("latest_sweep_regression_status", "")
                    or ""
                ).strip()
            native_targets_snapshot = (
                dict(campaign.get("native_targets_snapshot", {}))
                if isinstance(campaign.get("native_targets_snapshot", {}), dict)
                else {}
            )
            for target_row in native_targets_snapshot.get("target_apps", []) if isinstance(native_targets_snapshot.get("target_apps", []), list) else []:
                if isinstance(target_row, dict):
                    _ingest_native_target_campaign(campaign, target_row)
        for program in replay_programs:
            program_id = str(program.get("program_id", "") or "").strip()
            program_label = str(program.get("label", "") or "").strip()
            if program_id and not replay_program_summary["latest_program_id"]:
                replay_program_summary["latest_program_id"] = program_id
                replay_program_summary["latest_program_label"] = program_label
            replay_program_summary["campaign_count"] = int(replay_program_summary["campaign_count"]) + int(program.get("campaign_count", 0) or 0)
            replay_program_summary["cycle_count"] = int(replay_program_summary["cycle_count"]) + int(program.get("cycle_count", 0) or 0)
            replay_program_summary["pending_campaign_count"] = int(replay_program_summary["pending_campaign_count"]) + int(program.get("pending_campaign_count", 0) or 0)
            replay_program_summary["attention_campaign_count"] = int(replay_program_summary["attention_campaign_count"]) + int(program.get("attention_campaign_count", 0) or 0)
            replay_program_summary["pending_session_count"] = int(replay_program_summary["pending_session_count"]) + int(program.get("pending_session_count", 0) or 0)
            replay_program_summary["attention_session_count"] = int(replay_program_summary["attention_session_count"]) + int(program.get("attention_session_count", 0) or 0)
            replay_program_summary["pending_app_target_count"] = int(replay_program_summary["pending_app_target_count"]) + int(program.get("pending_app_target_count", 0) or 0)
            replay_program_summary["regression_cycle_count"] = int(replay_program_summary["regression_cycle_count"]) + int(program.get("regression_cycle_count", 0) or 0)
            replay_program_summary["long_horizon_pending_count"] = int(replay_program_summary["long_horizon_pending_count"]) + int(program.get("long_horizon_pending_count", 0) or 0)
            if not replay_program_summary["latest_cycle_status"]:
                replay_program_summary["latest_cycle_status"] = str(program.get("latest_cycle_status", "") or "").strip()
            if not replay_program_summary["latest_cycle_stop_reason"]:
                replay_program_summary["latest_cycle_stop_reason"] = str(program.get("latest_cycle_stop_reason", "") or "").strip()
            native_targets_snapshot = (
                dict(program.get("native_targets_snapshot", {}))
                if isinstance(program.get("native_targets_snapshot", {}), dict)
                else {}
            )
            for target_row in native_targets_snapshot.get("target_apps", []) if isinstance(native_targets_snapshot.get("target_apps", []), list) else []:
                if isinstance(target_row, dict):
                    _ingest_native_target_program(program, target_row)
        for portfolio in replay_portfolios:
            portfolio_id = str(portfolio.get("portfolio_id", "") or "").strip()
            portfolio_label = str(portfolio.get("label", "") or "").strip()
            if portfolio_id and not replay_portfolio_summary["latest_portfolio_id"]:
                replay_portfolio_summary["latest_portfolio_id"] = portfolio_id
                replay_portfolio_summary["latest_portfolio_label"] = portfolio_label
            replay_portfolio_summary["program_count"] = int(replay_portfolio_summary["program_count"]) + int(portfolio.get("program_count", 0) or 0)
            replay_portfolio_summary["wave_count"] = int(replay_portfolio_summary["wave_count"]) + int(portfolio.get("wave_count", 0) or 0)
            replay_portfolio_summary["pending_program_count"] = int(replay_portfolio_summary["pending_program_count"]) + int(portfolio.get("pending_program_count", 0) or 0)
            replay_portfolio_summary["attention_program_count"] = int(replay_portfolio_summary["attention_program_count"]) + int(portfolio.get("attention_program_count", 0) or 0)
            replay_portfolio_summary["pending_campaign_count"] = int(replay_portfolio_summary["pending_campaign_count"]) + int(portfolio.get("pending_campaign_count", 0) or 0)
            replay_portfolio_summary["pending_session_count"] = int(replay_portfolio_summary["pending_session_count"]) + int(portfolio.get("pending_session_count", 0) or 0)
            replay_portfolio_summary["pending_app_target_count"] = int(replay_portfolio_summary["pending_app_target_count"]) + int(portfolio.get("pending_app_target_count", 0) or 0)
            replay_portfolio_summary["regression_wave_count"] = int(replay_portfolio_summary["regression_wave_count"]) + int(portfolio.get("regression_wave_count", 0) or 0)
            replay_portfolio_summary["long_horizon_pending_count"] = int(replay_portfolio_summary["long_horizon_pending_count"]) + int(portfolio.get("long_horizon_pending_count", 0) or 0)
            if not replay_portfolio_summary["latest_wave_status"]:
                replay_portfolio_summary["latest_wave_status"] = str(portfolio.get("latest_wave_status", "") or "").strip()
            if not replay_portfolio_summary["latest_wave_stop_reason"]:
                replay_portfolio_summary["latest_wave_stop_reason"] = str(portfolio.get("latest_wave_stop_reason", "") or "").strip()
            native_targets_snapshot = (
                dict(portfolio.get("native_targets_snapshot", {}))
                if isinstance(portfolio.get("native_targets_snapshot", {}), dict)
                else {}
            )
            for target_row in native_targets_snapshot.get("target_apps", []) if isinstance(native_targets_snapshot.get("target_apps", []), list) else []:
                if isinstance(target_row, dict):
                    _ingest_native_target_portfolio(portfolio, target_row)
        target_app_rows: List[Dict[str, object]] = []
        for row in target_apps.values():
            replay_session_ids = row.get("replay_session_ids", set())
            replay_session_count = len(replay_session_ids) if isinstance(replay_session_ids, set) else len(
                [item for item in replay_session_ids if str(item).strip()]
            ) if isinstance(replay_session_ids, list) else 0
            replay_scenarios = list(row.get("replay_scenarios", []))[:8] if isinstance(row.get("replay_scenarios", []), list) else []
            replay_session_labels = list(row.get("replay_session_labels", []))[:6] if isinstance(row.get("replay_session_labels", []), list) else []
            query_hints = list(row.get("query_hints", []))[:8]
            descendant_title_hints = list(row.get("descendant_title_hints", []))[:8] if isinstance(row.get("descendant_title_hints", []), list) else []
            hint_query = str(row.get("hint_query", "") or "").strip() or self._native_target_hint_query(
                query_hints=query_hints,
                replay_scenarios=replay_scenarios,
            )
            descendant_hint_query = str(row.get("descendant_hint_query", "") or "").strip() or self._native_target_hint_query(
                query_hints=descendant_title_hints or query_hints,
                replay_scenarios=replay_scenarios,
            )
            preferred_window_title = str(row.get("preferred_window_title", "") or "").strip() or self._native_target_preferred_window_title(
                descendant_title_hints=descendant_title_hints,
                query_hints=query_hints,
                replay_scenarios=replay_scenarios,
            )
            campaign_descendant_title_hints = list(row.get("campaign_descendant_title_hints", []))[:8] if isinstance(row.get("campaign_descendant_title_hints", []), list) else []
            campaign_hint_query = str(row.get("campaign_hint_query", "") or "").strip() or self._native_target_hint_query(
                query_hints=query_hints,
                replay_scenarios=list(row.get("campaign_labels", []))[:4] if isinstance(row.get("campaign_labels", []), list) else [],
            )
            campaign_descendant_hint_query = str(row.get("campaign_descendant_hint_query", "") or "").strip() or self._native_target_hint_query(
                query_hints=campaign_descendant_title_hints or descendant_title_hints or query_hints,
                replay_scenarios=list(row.get("campaign_labels", []))[:4] if isinstance(row.get("campaign_labels", []), list) else [],
            )
            campaign_preferred_window_title = str(row.get("campaign_preferred_window_title", "") or "").strip() or self._native_target_preferred_window_title(
                descendant_title_hints=campaign_descendant_title_hints or descendant_title_hints,
                query_hints=query_hints,
                replay_scenarios=list(row.get("campaign_labels", []))[:4] if isinstance(row.get("campaign_labels", []), list) else [],
            )
            target_app_rows.append(
                {
                    "app_name": str(row.get("app_name", "") or "").strip(),
                    "priority": round(float(row.get("priority", 0.0) or 0.0), 6),
                    "scenario_names": list(row.get("scenario_names", []))[:6],
                    "packs": sorted(str(item).strip() for item in row.get("packs", set()) if str(item).strip())[:6],
                    "mission_families": sorted(
                        str(item).strip() for item in row.get("mission_families", set()) if str(item).strip()
                    )[:6],
                    "query_hints": query_hints,
                    "descendant_title_hints": descendant_title_hints,
                    "descendant_title_sequence": list(row.get("descendant_title_sequence", []))[:8]
                    if isinstance(row.get("descendant_title_sequence", []), list)
                    else [],
                    "descendant_hint_query": descendant_hint_query,
                    "preferred_window_title": preferred_window_title,
                    "hint_query": hint_query,
                    "max_horizon_steps": int(row.get("max_horizon_steps", 0) or 0),
                    "replay_pressure": round(float(row.get("replay_pressure", 0.0) or 0.0), 6),
                    "replay_session_count": replay_session_count,
                    "replay_pending_count": int(row.get("replay_pending_count", 0) or 0),
                    "replay_failed_count": int(row.get("replay_failed_count", 0) or 0),
                    "replay_completed_count": int(row.get("replay_completed_count", 0) or 0),
                    "session_cycle_count": int(row.get("session_cycle_count", 0) or 0),
                    "session_regression_cycle_count": int(row.get("session_regression_cycle_count", 0) or 0),
                    "session_long_horizon_pending_count": int(row.get("session_long_horizon_pending_count", 0) or 0),
                    "replay_scenarios": replay_scenarios,
                    "replay_session_labels": replay_session_labels,
                    "campaign_ids": sorted(str(item).strip() for item in row.get("campaign_ids", set()) if str(item).strip())[:6],
                    "campaign_labels": list(row.get("campaign_labels", []))[:6] if isinstance(row.get("campaign_labels", []), list) else [],
                    "campaign_focus_summary": list(row.get("campaign_focus_summary", []))[:6] if isinstance(row.get("campaign_focus_summary", []), list) else [],
                    "campaign_count": int(row.get("campaign_count", 0) or 0),
                    "campaign_sweep_count": int(row.get("campaign_sweep_count", 0) or 0),
                    "campaign_pending_session_count": int(row.get("campaign_pending_session_count", 0) or 0),
                    "campaign_attention_session_count": int(row.get("campaign_attention_session_count", 0) or 0),
                    "campaign_pending_app_target_count": int(row.get("campaign_pending_app_target_count", 0) or 0),
                    "campaign_regression_cycle_count": int(row.get("campaign_regression_cycle_count", 0) or 0),
                    "campaign_long_horizon_pending_count": int(row.get("campaign_long_horizon_pending_count", 0) or 0),
                    "campaign_pressure": round(float(row.get("campaign_pressure", 0.0) or 0.0), 6),
                    "campaign_hint_query": campaign_hint_query,
                    "campaign_descendant_title_hints": campaign_descendant_title_hints,
                    "campaign_descendant_title_sequence": list(row.get("campaign_descendant_title_sequence", []))[:8]
                    if isinstance(row.get("campaign_descendant_title_sequence", []), list)
                    else [],
                    "campaign_descendant_hint_query": campaign_descendant_hint_query,
                    "campaign_preferred_window_title": campaign_preferred_window_title,
                    "campaign_latest_sweep_status": str(row.get("campaign_latest_sweep_status", "") or "").strip(),
                    "campaign_latest_sweep_regression_status": str(row.get("campaign_latest_sweep_regression_status", "") or "").strip(),
                    "program_ids": sorted(str(item).strip() for item in row.get("program_ids", set()) if str(item).strip())[:6],
                    "program_labels": list(row.get("program_labels", []))[:6] if isinstance(row.get("program_labels", []), list) else [],
                    "program_count": int(row.get("program_count", 0) or 0),
                    "program_cycle_count": int(row.get("program_cycle_count", 0) or 0),
                    "program_pending_campaign_count": int(row.get("program_pending_campaign_count", 0) or 0),
                    "program_attention_campaign_count": int(row.get("program_attention_campaign_count", 0) or 0),
                    "program_pending_app_target_count": int(row.get("program_pending_app_target_count", 0) or 0),
                    "program_regression_cycle_count": int(row.get("program_regression_cycle_count", 0) or 0),
                    "program_long_horizon_pending_count": int(row.get("program_long_horizon_pending_count", 0) or 0),
                    "program_pressure": round(float(row.get("program_pressure", 0.0) or 0.0), 6),
                    "program_hint_query": str(row.get("program_hint_query", "") or "").strip(),
                    "program_descendant_title_hints": list(row.get("program_descendant_title_hints", []))[:8]
                    if isinstance(row.get("program_descendant_title_hints", []), list)
                    else [],
                    "program_descendant_title_sequence": list(row.get("program_descendant_title_sequence", []))[:8]
                    if isinstance(row.get("program_descendant_title_sequence", []), list)
                    else [],
                    "program_descendant_hint_query": str(row.get("program_descendant_hint_query", "") or "").strip(),
                    "program_preferred_window_title": str(row.get("program_preferred_window_title", "") or "").strip(),
                    "program_latest_cycle_status": str(row.get("program_latest_cycle_status", "") or "").strip(),
                    "program_latest_cycle_stop_reason": str(row.get("program_latest_cycle_stop_reason", "") or "").strip(),
                    "portfolio_ids": sorted(str(item).strip() for item in row.get("portfolio_ids", set()) if str(item).strip())[:6],
                    "portfolio_labels": list(row.get("portfolio_labels", []))[:6] if isinstance(row.get("portfolio_labels", []), list) else [],
                    "portfolio_count": int(row.get("portfolio_count", 0) or 0),
                    "portfolio_wave_count": int(row.get("portfolio_wave_count", 0) or 0),
                    "portfolio_pending_program_count": int(row.get("portfolio_pending_program_count", 0) or 0),
                    "portfolio_attention_program_count": int(row.get("portfolio_attention_program_count", 0) or 0),
                    "portfolio_pending_campaign_count": int(row.get("portfolio_pending_campaign_count", 0) or 0),
                    "portfolio_pending_session_count": int(row.get("portfolio_pending_session_count", 0) or 0),
                    "portfolio_pending_app_target_count": int(row.get("portfolio_pending_app_target_count", 0) or 0),
                    "portfolio_regression_wave_count": int(row.get("portfolio_regression_wave_count", 0) or 0),
                    "portfolio_long_horizon_pending_count": int(row.get("portfolio_long_horizon_pending_count", 0) or 0),
                    "portfolio_pressure": round(float(row.get("portfolio_pressure", 0.0) or 0.0), 6),
                    "portfolio_hint_query": str(row.get("portfolio_hint_query", "") or "").strip(),
                    "portfolio_descendant_title_hints": list(row.get("portfolio_descendant_title_hints", []))[:8]
                    if isinstance(row.get("portfolio_descendant_title_hints", []), list)
                    else [],
                    "portfolio_descendant_title_sequence": list(row.get("portfolio_descendant_title_sequence", []))[:8]
                    if isinstance(row.get("portfolio_descendant_title_sequence", []), list)
                    else [],
                    "portfolio_descendant_hint_query": str(row.get("portfolio_descendant_hint_query", "") or "").strip(),
                    "portfolio_preferred_window_title": str(row.get("portfolio_preferred_window_title", "") or "").strip(),
                    "portfolio_confirmation_pressure": round(float(row.get("portfolio_confirmation_pressure", 0.0) or 0.0), 6),
                    "portfolio_confirmation_title_hints": list(row.get("portfolio_confirmation_title_hints", []))[:8]
                    if isinstance(row.get("portfolio_confirmation_title_hints", []), list)
                    else [],
                    "portfolio_confirmation_title_sequence": list(row.get("portfolio_confirmation_title_sequence", []))[:8]
                    if isinstance(row.get("portfolio_confirmation_title_sequence", []), list)
                    else [],
                    "portfolio_confirmation_hint_query": str(row.get("portfolio_confirmation_hint_query", "") or "").strip(),
                    "portfolio_confirmation_preferred_window_title": str(row.get("portfolio_confirmation_preferred_window_title", "") or "").strip(),
                    "portfolio_latest_wave_status": str(row.get("portfolio_latest_wave_status", "") or "").strip(),
                    "portfolio_latest_wave_stop_reason": str(row.get("portfolio_latest_wave_stop_reason", "") or "").strip(),
                    "control_biases": {
                        key: round(max(0.0, min(float(value or 0.0), 1.0)), 6)
                        for key, value in dict(row.get("control_biases", {})).items()
                    },
                }
            )
        target_app_rows.sort(
            key=lambda item: (
                -float(item.get("priority", 0.0) or 0.0),
                -float(item.get("replay_pressure", 0.0) or 0.0),
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
            "replay_session_summary": replay_session_summary,
            "replay_campaign_summary": replay_campaign_summary,
            "replay_program_summary": replay_program_summary,
            "replay_portfolio_summary": replay_portfolio_summary,
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

    def _lab_session_matches_filters(
        self,
        *,
        session: Dict[str, Any],
        filters: Dict[str, object],
    ) -> bool:
        session_filters = dict(session.get("filters", {})) if isinstance(session.get("filters", {}), dict) else {}
        for key in (
            "scenario_name",
            "pack",
            "category",
            "capability",
            "risk_level",
            "autonomy_tier",
            "mission_family",
            "app",
        ):
            expected = str(filters.get(key, "") or "").strip().lower()
            if not expected:
                continue
            actual = str(session_filters.get(key, "") or "").strip().lower()
            if actual != expected:
                return False
        return True

    @staticmethod
    def _campaign_matches_watchdog_filters(
        campaign: Dict[str, object],
        *,
        campaign_status: str,
        pack: str,
        app_name: str,
    ) -> bool:
        if campaign_status:
            current_status = str(campaign.get("status", "") or "").strip().lower()
            if current_status != campaign_status.strip().lower():
                return False
        filters = dict(campaign.get("filters", {})) if isinstance(campaign.get("filters", {}), dict) else {}
        if pack:
            campaign_pack = str(campaign.get("pack", filters.get("pack", "")) or "").strip().lower()
            if campaign_pack != pack:
                return False
        if app_name:
            candidate_values = {
                str(campaign.get("app_name", "") or "").strip().lower(),
                str(filters.get("app", filters.get("app_name", "")) or "").strip().lower(),
            }
            target_apps = campaign.get("target_apps", campaign.get("app_targets", []))
            if isinstance(target_apps, list):
                candidate_values.update(str(item).strip().lower() for item in target_apps if str(item).strip())
            if app_name not in candidate_values:
                return False
        return True

    @staticmethod
    def _program_matches_watchdog_filters(
        program: Dict[str, object],
        *,
        program_status: str,
        pack: str,
        app_name: str,
    ) -> bool:
        if program_status:
            current_status = str(program.get("status", "") or "").strip().lower()
            if current_status != program_status.strip().lower():
                return False
        filters = dict(program.get("filters", {})) if isinstance(program.get("filters", {}), dict) else {}
        if pack:
            program_pack = str(program.get("pack", filters.get("pack", "")) or "").strip().lower()
            if program_pack != pack:
                return False
        if app_name:
            candidate_values = {
                str(program.get("app_name", "") or "").strip().lower(),
                str(filters.get("app", filters.get("app_name", "")) or "").strip().lower(),
            }
            target_apps = program.get("target_apps", program.get("app_targets", []))
            if isinstance(target_apps, list):
                candidate_values.update(str(item).strip().lower() for item in target_apps if str(item).strip())
            if app_name not in candidate_values:
                return False
        return True

    @staticmethod
    def _portfolio_matches_watchdog_filters(
        portfolio: Dict[str, object],
        *,
        portfolio_status: str,
        pack: str,
        app_name: str,
    ) -> bool:
        if portfolio_status:
            current_status = str(portfolio.get("status", "") or "").strip().lower()
            if current_status != portfolio_status.strip().lower():
                return False
        filters = dict(portfolio.get("filters", {})) if isinstance(portfolio.get("filters", {}), dict) else {}
        if pack:
            portfolio_pack = str(portfolio.get("pack", filters.get("pack", "")) or "").strip().lower()
            if portfolio_pack != pack:
                return False
        if app_name:
            candidate_values = {
                str(portfolio.get("app_name", "") or "").strip().lower(),
                str(filters.get("app", filters.get("app_name", "")) or "").strip().lower(),
            }
            target_apps = portfolio.get("target_apps", portfolio.get("app_targets", []))
            if isinstance(target_apps, list):
                candidate_values.update(str(item).strip().lower() for item in target_apps if str(item).strip())
            if app_name not in candidate_values:
                return False
        return True

    def _watchdog_auto_create_campaigns(
        self,
        *,
        existing_campaigns: List[Dict[str, object]],
        native_targets_payload: Dict[str, object],
        max_campaigns: int,
        max_sessions: int,
        history_limit: int,
        campaign_status: str,
        pack: str,
        app_name: str,
        trigger_source: str,
    ) -> List[Dict[str, object]]:
        if max_campaigns <= 0:
            return []
        normalized_status = str(campaign_status or "").strip().lower()
        if normalized_status and normalized_status not in {"ready", "attention"}:
            return []
        target_rows = [
            dict(item)
            for item in native_targets_payload.get("target_apps", [])
            if isinstance(item, dict) and str(item.get("app_name", "") or "").strip()
        ] if isinstance(native_targets_payload.get("target_apps", []), list) else []
        if not target_rows:
            return []
        existing_keys: set[tuple[str, str]] = set()
        explicit_pack = bool(pack)
        for campaign in existing_campaigns:
            filters = dict(campaign.get("filters", {})) if isinstance(campaign.get("filters", {}), dict) else {}
            campaign_pack = str(campaign.get("pack", filters.get("pack", "")) or "").strip().lower()
            campaign_app_candidates = [
                str(campaign.get("app_name", "") or "").strip().lower(),
                str(filters.get("app", filters.get("app_name", "")) or "").strip().lower(),
            ]
            target_apps = campaign.get("target_apps", campaign.get("app_targets", []))
            if isinstance(target_apps, list):
                campaign_app_candidates.extend(
                    str(item).strip().lower()
                    for item in target_apps
                    if str(item).strip()
                )
            campaign_apps = self._unique_strings(campaign_app_candidates)
            for candidate_app in campaign_apps:
                if candidate_app:
                    existing_keys.add((campaign_pack if explicit_pack else "", candidate_app))
        ranked_specs: List[Dict[str, object]] = []
        for row in target_rows:
            candidate_app = str(row.get("app_name", "") or "").strip().lower()
            if not candidate_app:
                continue
            if app_name and candidate_app != app_name:
                continue
            row_packs = self._unique_strings(
                [
                    str(item).strip().lower()
                    for item in row.get("packs", [])
                    if str(item).strip()
                ]
            ) if isinstance(row.get("packs", []), list) else []
            candidate_pack = pack or (row_packs[0] if row_packs else "")
            dedupe_key = (candidate_pack if explicit_pack else "", candidate_app)
            if dedupe_key in existing_keys:
                continue
            ranked_specs.append(
                {
                    "app_name": candidate_app,
                    "pack": candidate_pack,
                    "priority": float(row.get("campaign_pressure", row.get("replay_pressure", row.get("priority", 0.0))) or 0.0),
                    "replay_pressure": float(row.get("replay_pressure", 0.0) or 0.0),
                    "max_horizon_steps": int(row.get("max_horizon_steps", 0) or 0),
                }
            )
            existing_keys.add(dedupe_key)
        ranked_specs.sort(
            key=lambda item: (
                float(item.get("priority", 0.0) or 0.0),
                float(item.get("replay_pressure", 0.0) or 0.0),
                int(item.get("max_horizon_steps", 0) or 0),
            ),
            reverse=True,
        )
        created: List[Dict[str, object]] = []
        for spec in ranked_specs[:max_campaigns]:
            create_payload = self.create_lab_campaign(
                pack=str(spec.get("pack", "") or "").strip(),
                app=str(spec.get("app_name", "") or "").strip(),
                history_limit=history_limit,
                source=f"campaign_watchdog_auto_create:{str(trigger_source or 'manual').strip().lower() or 'manual'}",
                max_sessions=max_sessions,
            )
            if str(create_payload.get("status", "") or "").strip().lower() != "success":
                continue
            campaign = (
                dict(create_payload.get("campaign", {}))
                if isinstance(create_payload.get("campaign", {}), dict)
                else {}
            )
            if not campaign:
                continue
            created.append(
                {
                    "campaign_id": str(campaign.get("campaign_id", "") or "").strip(),
                    "label": str(campaign.get("label", "") or "").strip(),
                    "status": str(campaign.get("status", "") or "").strip().lower(),
                    "pack": str(spec.get("pack", "") or "").strip(),
                    "app_name": str(spec.get("app_name", "") or "").strip(),
                    "target_apps": [
                        str(item).strip()
                        for item in campaign.get("target_apps", campaign.get("app_targets", []))
                        if str(item).strip()
                    ][:8]
                    if isinstance(campaign.get("target_apps", campaign.get("app_targets", [])), list)
                    else [],
                    "created_session_count": int(create_payload.get("created_session_count", 0) or 0),
                }
            )
        return created

    def _watchdog_auto_create_programs(
        self,
        *,
        existing_programs: List[Dict[str, object]],
        existing_campaigns: List[Dict[str, object]],
        native_targets_payload: Dict[str, object],
        max_programs: int,
        max_campaigns_per_program: int,
        max_sessions_per_campaign: int,
        history_limit: int,
        program_status: str,
        pack: str,
        app_name: str,
        trigger_source: str,
    ) -> List[Dict[str, object]]:
        if max_programs <= 0:
            return []
        normalized_status = str(program_status or "").strip().lower()
        if normalized_status and normalized_status not in {"ready", "attention"}:
            return []
        target_rows = [
            dict(item)
            for item in native_targets_payload.get("target_apps", [])
            if isinstance(item, dict) and str(item.get("app_name", "") or "").strip()
        ] if isinstance(native_targets_payload.get("target_apps", []), list) else []
        if not target_rows:
            return []
        existing_keys: set[tuple[str, str]] = set()
        explicit_pack = bool(pack)
        for program in existing_programs:
            filters = dict(program.get("filters", {})) if isinstance(program.get("filters", {}), dict) else {}
            program_pack = str(program.get("pack", filters.get("pack", "")) or "").strip().lower()
            program_target_apps = (
                program.get("target_apps", program.get("app_targets", []))
                if isinstance(program.get("target_apps", program.get("app_targets", [])), list)
                else []
            )
            program_apps = self._unique_strings(
                [
                    str(program.get("app_name", "") or "").strip().lower(),
                    str(filters.get("app", filters.get("app_name", "")) or "").strip().lower(),
                    *(str(item).strip().lower() for item in program_target_apps if str(item).strip()),
                ]
            )
            for candidate_app in program_apps:
                if candidate_app:
                    existing_keys.add((program_pack if explicit_pack else "", candidate_app))
        campaign_map: Dict[tuple[str, str], List[Dict[str, object]]] = {}
        for campaign in existing_campaigns:
            filters = dict(campaign.get("filters", {})) if isinstance(campaign.get("filters", {}), dict) else {}
            campaign_pack = str(campaign.get("pack", filters.get("pack", "")) or "").strip().lower()
            campaign_target_apps = (
                campaign.get("target_apps", campaign.get("app_targets", []))
                if isinstance(campaign.get("target_apps", campaign.get("app_targets", [])), list)
                else []
            )
            campaign_apps = self._unique_strings(
                [
                    str(filters.get("app", filters.get("app_name", "")) or "").strip().lower(),
                    *(str(item).strip().lower() for item in campaign_target_apps if str(item).strip()),
                ]
            )
            for candidate_app in campaign_apps:
                if candidate_app:
                    campaign_map.setdefault((campaign_pack, candidate_app), []).append(dict(campaign))
        ranked_specs: List[Dict[str, object]] = []
        for row in target_rows:
            candidate_app = str(row.get("app_name", "") or "").strip().lower()
            if not candidate_app:
                continue
            if app_name and candidate_app != app_name:
                continue
            row_packs = self._unique_strings(
                [
                    str(item).strip().lower()
                    for item in row.get("packs", [])
                    if str(item).strip()
                ]
            ) if isinstance(row.get("packs", []), list) else []
            candidate_pack = pack or (row_packs[0] if row_packs else "")
            dedupe_key = (candidate_pack if explicit_pack else "", candidate_app)
            if dedupe_key in existing_keys:
                continue
            matching_campaigns = campaign_map.get((candidate_pack, candidate_app), [])
            ranked_specs.append(
                {
                    "app_name": candidate_app,
                    "pack": candidate_pack,
                    "priority": float(
                        row.get("campaign_pressure", row.get("replay_pressure", row.get("priority", 0.0))) or 0.0
                    ),
                    "replay_pressure": float(row.get("replay_pressure", 0.0) or 0.0),
                    "max_horizon_steps": int(row.get("max_horizon_steps", 0) or 0),
                    "campaign_count": len(matching_campaigns),
                    "attention_campaign_count": sum(
                        1 for campaign in matching_campaigns if str(campaign.get("status", "") or "").strip().lower() == "attention"
                    ),
                }
            )
            existing_keys.add(dedupe_key)
        ranked_specs.sort(
            key=lambda item: (
                float(item.get("priority", 0.0) or 0.0),
                int(item.get("attention_campaign_count", 0) or 0),
                int(item.get("campaign_count", 0) or 0),
                float(item.get("replay_pressure", 0.0) or 0.0),
                int(item.get("max_horizon_steps", 0) or 0),
            ),
            reverse=True,
        )
        created: List[Dict[str, object]] = []
        for spec in ranked_specs[:max_programs]:
            create_payload = self.create_lab_program(
                pack=str(spec.get("pack", "") or "").strip(),
                app=str(spec.get("app_name", "") or "").strip(),
                history_limit=history_limit,
                source=f"program_watchdog_auto_create:{str(trigger_source or 'manual').strip().lower() or 'manual'}",
                max_campaigns=max_campaigns_per_program,
                max_sessions_per_campaign=max_sessions_per_campaign,
            )
            if str(create_payload.get("status", "") or "").strip().lower() != "success":
                continue
            program = dict(create_payload.get("program", {})) if isinstance(create_payload.get("program", {}), dict) else {}
            if not program:
                continue
            created.append(
                {
                    "program_id": str(program.get("program_id", "") or "").strip(),
                    "label": str(program.get("label", "") or "").strip(),
                    "status": str(program.get("status", "") or "").strip().lower(),
                    "pack": str(spec.get("pack", "") or "").strip(),
                    "app_name": str(spec.get("app_name", "") or "").strip(),
                    "target_apps": [
                        str(item).strip()
                        for item in program.get("target_apps", program.get("app_targets", []))
                        if str(item).strip()
                    ][:8]
                    if isinstance(program.get("target_apps", program.get("app_targets", [])), list)
                    else [],
                    "created_campaign_count": int(create_payload.get("created_campaign_count", 0) or 0),
                    "created_session_count": int(create_payload.get("created_session_count", 0) or 0),
                }
            )
        return created

    def _watchdog_auto_create_portfolios(
        self,
        *,
        existing_portfolios: List[Dict[str, object]],
        existing_programs: List[Dict[str, object]],
        existing_campaigns: List[Dict[str, object]],
        native_targets_payload: Dict[str, object],
        max_portfolios: int,
        max_programs_per_portfolio: int,
        max_campaigns_per_program: int,
        max_sessions_per_campaign: int,
        history_limit: int,
        portfolio_status: str,
        pack: str,
        app_name: str,
        trigger_source: str,
    ) -> List[Dict[str, object]]:
        if max_portfolios <= 0:
            return []
        normalized_status = str(portfolio_status or "").strip().lower()
        if normalized_status and normalized_status not in {"ready", "attention"}:
            return []
        target_rows = [
            dict(item)
            for item in native_targets_payload.get("target_apps", [])
            if isinstance(item, dict) and str(item.get("app_name", "") or "").strip()
        ] if isinstance(native_targets_payload.get("target_apps", []), list) else []
        if not target_rows:
            return []
        existing_keys: set[tuple[str, str]] = set()
        explicit_pack = bool(pack)
        for portfolio in existing_portfolios:
            filters = dict(portfolio.get("filters", {})) if isinstance(portfolio.get("filters", {}), dict) else {}
            portfolio_pack = str(portfolio.get("pack", filters.get("pack", "")) or "").strip().lower()
            portfolio_target_apps = (
                portfolio.get("target_apps", portfolio.get("app_targets", []))
                if isinstance(portfolio.get("target_apps", portfolio.get("app_targets", [])), list)
                else []
            )
            portfolio_apps = self._unique_strings(
                [
                    str(portfolio.get("app_name", "") or "").strip().lower(),
                    str(filters.get("app", filters.get("app_name", "")) or "").strip().lower(),
                    *(str(item).strip().lower() for item in portfolio_target_apps if str(item).strip()),
                ]
            )
            for candidate_app in portfolio_apps:
                if candidate_app:
                    existing_keys.add((portfolio_pack if explicit_pack else "", candidate_app))
        program_map: Dict[tuple[str, str], List[Dict[str, object]]] = {}
        for program in existing_programs:
            filters = dict(program.get("filters", {})) if isinstance(program.get("filters", {}), dict) else {}
            program_pack = str(program.get("pack", filters.get("pack", "")) or "").strip().lower()
            program_target_apps = (
                program.get("target_apps", program.get("app_targets", []))
                if isinstance(program.get("target_apps", program.get("app_targets", [])), list)
                else []
            )
            program_apps = self._unique_strings(
                [
                    str(filters.get("app", filters.get("app_name", "")) or "").strip().lower(),
                    *(str(item).strip().lower() for item in program_target_apps if str(item).strip()),
                ]
            )
            for candidate_app in program_apps:
                if candidate_app:
                    program_map.setdefault((program_pack, candidate_app), []).append(dict(program))
        ranked_specs: List[Dict[str, object]] = []
        for row in target_rows:
            candidate_app = str(row.get("app_name", "") or "").strip().lower()
            if not candidate_app:
                continue
            if app_name and candidate_app != app_name:
                continue
            row_packs = self._unique_strings(
                [
                    str(item).strip().lower()
                    for item in row.get("packs", [])
                    if str(item).strip()
                ]
            ) if isinstance(row.get("packs", []), list) else []
            candidate_pack = pack or (row_packs[0] if row_packs else "")
            dedupe_key = (candidate_pack if explicit_pack else "", candidate_app)
            if dedupe_key in existing_keys:
                continue
            matching_programs = program_map.get((candidate_pack, candidate_app), [])
            matching_campaign_count = sum(int(program.get("campaign_count", 0) or 0) for program in matching_programs)
            ranked_specs.append(
                {
                    "app_name": candidate_app,
                    "pack": candidate_pack,
                    "priority": float(
                        row.get("program_pressure", row.get("campaign_pressure", row.get("replay_pressure", row.get("priority", 0.0))))
                        or 0.0
                    ),
                    "replay_pressure": float(row.get("replay_pressure", 0.0) or 0.0),
                    "max_horizon_steps": int(row.get("max_horizon_steps", 0) or 0),
                    "program_count": len(matching_programs),
                    "campaign_count": matching_campaign_count,
                }
            )
            existing_keys.add(dedupe_key)
        ranked_specs.sort(
            key=lambda item: (
                float(item.get("priority", 0.0) or 0.0),
                int(item.get("program_count", 0) or 0),
                int(item.get("campaign_count", 0) or 0),
                float(item.get("replay_pressure", 0.0) or 0.0),
                int(item.get("max_horizon_steps", 0) or 0),
            ),
            reverse=True,
        )
        created: List[Dict[str, object]] = []
        for spec in ranked_specs[:max_portfolios]:
            create_payload = self.create_lab_portfolio(
                pack=str(spec.get("pack", "") or "").strip(),
                app=str(spec.get("app_name", "") or "").strip(),
                history_limit=history_limit,
                source=f"portfolio_watchdog_auto_create:{str(trigger_source or 'manual').strip().lower() or 'manual'}",
                max_programs=max_programs_per_portfolio,
                max_campaigns_per_program=max_campaigns_per_program,
                max_sessions_per_campaign=max_sessions_per_campaign,
            )
            if str(create_payload.get("status", "") or "").strip().lower() != "success":
                continue
            portfolio = dict(create_payload.get("portfolio", {})) if isinstance(create_payload.get("portfolio", {}), dict) else {}
            if not portfolio:
                continue
            created.append(
                {
                    "portfolio_id": str(portfolio.get("portfolio_id", "") or "").strip(),
                    "label": str(portfolio.get("label", "") or "").strip(),
                    "status": str(portfolio.get("status", "") or "").strip().lower(),
                    "pack": str(spec.get("pack", "") or "").strip(),
                    "app_name": str(spec.get("app_name", "") or "").strip(),
                    "target_apps": [
                        str(item).strip()
                        for item in portfolio.get("target_apps", portfolio.get("app_targets", []))
                        if str(item).strip()
                    ][:10]
                    if isinstance(portfolio.get("target_apps", portfolio.get("app_targets", [])), list)
                    else [],
                    "created_program_count": int(create_payload.get("created_program_count", 0) or 0),
                    "created_campaign_count": int(create_payload.get("created_campaign_count", 0) or 0),
                    "created_session_count": int(create_payload.get("created_session_count", 0) or 0),
                }
            )
        return created

    @staticmethod
    def _campaign_watchdog_sort_key(campaign: Dict[str, object]) -> tuple[int, int, int, int, int, int, int]:
        latest_regression = str(
            campaign.get("latest_sweep_regression_status", campaign.get("latest_sweep_status", ""))
            or ""
        ).strip().lower()
        return (
            int(float(campaign.get("campaign_pressure_score", 0.0) or 0.0) * 100),
            int(campaign.get("attention_session_count", 0) or 0),
            1 if latest_regression in {"regression", "failed"} else 0,
            int(campaign.get("pending_session_count", 0) or 0),
            int(campaign.get("pending_app_target_count", 0) or 0),
            int(campaign.get("long_horizon_pending_count", 0) or 0),
            int(campaign.get("regression_cycle_count", 0) or 0) + int(campaign.get("regression_sweep_streak", 0) or 0),
        )

    @staticmethod
    def _program_watchdog_sort_key(program: Dict[str, object]) -> tuple[int, int, int, int, int, int, int]:
        latest_cycle = str(program.get("latest_cycle_status", "") or "").strip().lower()
        return (
            int(float(program.get("program_pressure_score", 0.0) or 0.0) * 100),
            1 if latest_cycle in {"regression", "failed", "error"} else 0,
            int(program.get("attention_campaign_count", 0) or 0),
            int(program.get("pending_campaign_count", 0) or 0),
            int(program.get("pending_session_count", 0) or 0),
            int(program.get("pending_app_target_count", 0) or 0),
            int(program.get("regression_cycle_count", 0) or 0) + int(program.get("regression_cycle_streak", 0) or 0),
        )

    @staticmethod
    def _portfolio_watchdog_sort_key(portfolio: Dict[str, object]) -> tuple[int, int, int, int, int, int, int]:
        latest_wave = str(portfolio.get("latest_wave_status", "") or "").strip().lower()
        return (
            int(float(portfolio.get("portfolio_pressure_score", 0.0) or 0.0) * 100),
            1 if latest_wave in {"regression", "failed", "error"} else 0,
            int(portfolio.get("attention_program_count", 0) or 0),
            int(portfolio.get("pending_program_count", 0) or 0),
            int(portfolio.get("pending_campaign_count", 0) or 0),
            int(portfolio.get("pending_session_count", 0) or 0),
            int(portfolio.get("regression_wave_count", 0) or 0) + int(portfolio.get("regression_wave_streak", 0) or 0),
        )

    def _native_target_hint_query(
        self,
        *,
        query_hints: List[str],
        replay_scenarios: List[str],
    ) -> str:
        terms: List[str] = []
        for hint in query_hints:
            clean_hint = str(hint or "").strip()
            if clean_hint and clean_hint not in terms:
                terms.append(clean_hint)
            if len(terms) >= 2:
                break
        for scenario_name in replay_scenarios:
            clean_scenario = str(scenario_name or "").strip().replace("_", " ")
            if clean_scenario and clean_scenario not in terms:
                terms.append(clean_scenario)
            if len(terms) >= 2:
                break
        return " | ".join(terms[:2])

    @staticmethod
    def _native_target_descendant_title_hints(
        *,
        query_hints: List[str],
        replay_scenarios: List[str],
    ) -> List[str]:
        hints: List[str] = []
        for value in [*query_hints, *replay_scenarios]:
            clean = " ".join(
                str(value or "")
                .replace("_", " ")
                .replace("-", " ")
                .replace("|", " ")
                .split()
            ).strip()
            if not clean:
                continue
            formatted = f"{clean[:1].upper()}{clean[1:]}" if clean.islower() else clean
            if formatted not in hints:
                hints.append(formatted)
        return hints[:8]

    @staticmethod
    def _native_target_preferred_window_title(
        *,
        descendant_title_hints: List[str],
        query_hints: List[str],
        replay_scenarios: List[str],
    ) -> str:
        for value in [*descendant_title_hints, *query_hints, *replay_scenarios]:
            clean = " ".join(
                str(value or "")
                .replace("_", " ")
                .replace("-", " ")
                .replace("|", " ")
                .split()
            ).strip()
            if not clean:
                continue
            return f"{clean[:1].upper()}{clean[1:]}" if clean.islower() else clean
        return ""

    @staticmethod
    def _native_target_is_confirmation_like(value: str) -> bool:
        lowered = str(value or "").strip().lower()
        if not lowered:
            return False
        return any(
            marker in lowered
            for marker in (
                "confirm",
                "confirmation",
                "allow",
                "accept",
                "approve",
                "continue",
                "apply",
                "install",
                "finish",
                "next",
                "yes",
                "ok",
                "pair",
                "permission",
                "review",
                "warning",
            )
        )

    @classmethod
    def _native_target_confirmation_title_hints(
        cls,
        *,
        primary_titles: List[str],
        fallback_titles: List[str],
        stop_reason: str = "",
    ) -> List[str]:
        ordered_titles = cls._unique_strings(
            [
                str(item).strip()
                for item in [*primary_titles, *fallback_titles]
                if str(item).strip()
            ]
        )
        confirmation_titles = [
            title
            for title in ordered_titles
            if cls._native_target_is_confirmation_like(title)
        ]
        if confirmation_titles:
            return confirmation_titles[:8]
        if cls._native_target_is_confirmation_like(stop_reason):
            return ordered_titles[:4]
        return []

    @classmethod
    def _native_target_confirmation_pressure(
        cls,
        *,
        confirmation_titles: List[str],
        portfolio_pressure: float,
        regression_wave_count: int,
        long_horizon_pending_count: int,
        latest_wave_stop_reason: str,
    ) -> float:
        if not confirmation_titles and not cls._native_target_is_confirmation_like(latest_wave_stop_reason):
            return 0.0
        pressure = max(0.0, float(portfolio_pressure or 0.0))
        if confirmation_titles:
            pressure = max(pressure, 0.65)
        if cls._native_target_is_confirmation_like(latest_wave_stop_reason):
            pressure += 0.2
        pressure += min(0.18, max(0, int(regression_wave_count or 0)) * 0.04)
        pressure += min(0.12, max(0, int(long_horizon_pending_count or 0)) * 0.03)
        return min(6.0, pressure)

    @staticmethod
    def _unique_strings(values: List[str]) -> List[str]:
        seen: set[str] = set()
        result: List[str] = []
        for value in values:
            clean = str(value or "").strip()
            if not clean:
                continue
            lowered = clean.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            result.append(clean)
        return result

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
                    "capabilities": list(row.get("capabilities", [])) if isinstance(row.get("capabilities", []), list) else [],
                    "native_hybrid_focus": bool(row.get("native_hybrid_focus", False)),
                    "recovery_expected": bool(row.get("recovery_expected", False)),
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
    def _count_map_leaderboard(
        source: object,
        *,
        label_key: str = "name",
        limit: int = 8,
    ) -> List[Dict[str, object]]:
        if not isinstance(source, dict):
            return []
        rows = []
        for name, count in source.items():
            clean_name = str(name or "").strip()
            if not clean_name:
                continue
            rows.append({label_key: clean_name, "count": int(count or 0)})
        rows.sort(key=lambda item: (-int(item.get("count", 0) or 0), str(item.get(label_key, "") or "")))
        return rows[: max(1, limit)]

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
    def _filters_to_run_kwargs(filters: Dict[str, object]) -> Dict[str, object]:
        clean = dict(filters) if isinstance(filters, dict) else {}
        return {
            "scenario_name": str(clean.get("scenario_name", "") or "").strip(),
            "pack": str(clean.get("pack", "") or "").strip(),
            "category": str(clean.get("category", "") or "").strip(),
            "capability": str(clean.get("capability", "") or "").strip(),
            "risk_level": str(clean.get("risk_level", "") or "").strip(),
            "autonomy_tier": str(clean.get("autonomy_tier", "") or "").strip(),
            "mission_family": str(clean.get("mission_family", "") or "").strip(),
            "app": str(clean.get("app", clean.get("app_name", "")) or "").strip(),
            "limit": max(1, min(int(clean.get("limit", 200) or 200), 5000)),
        }

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

    @classmethod
    def _row_query_hints(cls, *, row: Dict[str, object]) -> List[str]:
        text = " ".join(
            [
                str(row.get("user_text", "") or "").strip(),
                " ".join(str(item).strip() for item in row.get("tags", []) if str(item).strip()) if isinstance(row.get("tags", []), list) else "",
                " ".join(str(item).strip() for item in row.get("capabilities", []) if str(item).strip()) if isinstance(row.get("capabilities", []), list) else "",
            ]
        ).strip()
        scenario = Scenario(
            name=str(row.get("scenario", row.get("name", "")) or "").strip() or "replay_row",
            user_text=text or str(row.get("user_text", "") or "").strip(),
            expected_actions=[],
            capabilities=[str(item).strip() for item in row.get("capabilities", []) if str(item).strip()] if isinstance(row.get("capabilities", []), list) else [],
            tags=[str(item).strip() for item in row.get("tags", []) if str(item).strip()] if isinstance(row.get("tags", []), list) else [],
        )
        return cls._scenario_query_hints(scenario=scenario)

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

    @classmethod
    def _row_native_tactic_profile(cls, *, row: Dict[str, object]) -> Dict[str, float]:
        scenario = Scenario(
            name=str(row.get("scenario", row.get("name", "")) or "").strip() or "replay_row",
            user_text=str(row.get("user_text", "") or "").strip(),
            expected_actions=[],
            category=str(row.get("category", "") or "").strip() or "general",
            capabilities=[str(item).strip() for item in row.get("capabilities", []) if str(item).strip()] if isinstance(row.get("capabilities", []), list) else [],
            risk_level=str(row.get("risk_level", "") or "").strip() or "standard",
            pack=str(row.get("pack", "") or "").strip() or "desktop_core",
            mission_family=str(row.get("mission_family", "") or "").strip() or "task",
            autonomy_tier=str(row.get("autonomy_tier", "") or "").strip() or "assisted",
            apps=[str(item).strip() for item in row.get("apps", []) if str(item).strip()] if isinstance(row.get("apps", []), list) else [],
            recovery_expected=bool(row.get("recovery_expected", False)),
            native_hybrid_focus=bool(row.get("native_hybrid_focus", False)),
            horizon_steps=max(1, int(row.get("horizon_steps", 1) or 1)),
            tags=[str(item).strip() for item in row.get("tags", []) if str(item).strip()] if isinstance(row.get("tags", []), list) else [],
        )
        return cls._scenario_native_tactic_profile(scenario=scenario)

    def _last_run_regression_payload(self) -> Dict[str, object]:
        return dict(self.last_run.get("regression", {})) if isinstance(self.last_run, dict) else {}
