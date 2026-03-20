from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List


class DesktopBenchmarkLabMemory:
    _DEFAULT_INSTANCE: "DesktopBenchmarkLabMemory | None" = None
    _DEFAULT_LOCK = RLock()

    def __init__(
        self,
        *,
        store_path: str = "data/desktop_benchmark_lab_memory.json",
        max_sessions: int = 200,
        max_campaigns: int = 128,
        max_programs: int = 64,
        max_portfolios: int = 32,
        max_replay_events: int = 16,
        max_run_cycles: int = 24,
        max_sweep_runs: int = 32,
    ) -> None:
        self.store_path = Path(store_path)
        self.max_sessions = self._coerce_int(max_sessions, minimum=8, maximum=5000, default=200)
        self.max_campaigns = self._coerce_int(max_campaigns, minimum=4, maximum=2000, default=128)
        self.max_programs = self._coerce_int(max_programs, minimum=2, maximum=512, default=64)
        self.max_portfolios = self._coerce_int(max_portfolios, minimum=1, maximum=256, default=32)
        self.max_replay_events = self._coerce_int(max_replay_events, minimum=2, maximum=128, default=16)
        self.max_run_cycles = self._coerce_int(max_run_cycles, minimum=1, maximum=256, default=24)
        self.max_sweep_runs = self._coerce_int(max_sweep_runs, minimum=1, maximum=256, default=32)
        self._lock = RLock()
        self._sessions: Dict[str, Dict[str, Any]] = {}
        self._campaigns: Dict[str, Dict[str, Any]] = {}
        self._programs: Dict[str, Dict[str, Any]] = {}
        self._portfolios: Dict[str, Dict[str, Any]] = {}
        self._updates_since_save = 0
        self._last_save_monotonic = 0.0
        self._load()

    @classmethod
    def default(cls) -> "DesktopBenchmarkLabMemory":
        with cls._DEFAULT_LOCK:
            if cls._DEFAULT_INSTANCE is None:
                cls._DEFAULT_INSTANCE = cls()
            return cls._DEFAULT_INSTANCE

    def record_session(
        self,
        *,
        filters: Dict[str, Any] | None,
        lab_payload: Dict[str, Any] | None,
        native_targets_payload: Dict[str, Any] | None,
        guidance_payload: Dict[str, Any] | None = None,
        source: str = "",
        label: str = "",
    ) -> Dict[str, Any]:
        clean_filters = dict(filters) if isinstance(filters, dict) else {}
        lab_snapshot = dict(lab_payload) if isinstance(lab_payload, dict) else {}
        native_snapshot = dict(native_targets_payload) if isinstance(native_targets_payload, dict) else {}
        guidance_snapshot = dict(guidance_payload) if isinstance(guidance_payload, dict) else {}
        now = datetime.now(timezone.utc).isoformat()
        session_id = f"benchlab-{uuid.uuid4().hex[:12]}"
        row = self._session_row(
            session_id=session_id,
            created_at=now,
            updated_at=now,
            filters=clean_filters,
            lab_snapshot=lab_snapshot,
            native_targets_snapshot=native_snapshot,
            guidance_snapshot=guidance_snapshot,
            source=source,
            label=label,
            replay_events=[],
            run_cycles=self._seed_run_cycles(
                recorded_at=now,
                filters=clean_filters,
                lab_snapshot=lab_snapshot,
                native_targets_snapshot=native_snapshot,
            ),
        )
        with self._lock:
            self._sessions[session_id] = row
            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
        return {"status": "success", "session": self._public_row(row)}

    def get_session(self, session_id: str) -> Dict[str, Any]:
        clean_id = str(session_id or "").strip()
        if not clean_id:
            return {"status": "error", "message": "session_id required"}
        with self._lock:
            row = self._sessions.get(clean_id)
            if not isinstance(row, dict):
                return {"status": "error", "message": "benchmark lab session not found"}
            return {"status": "success", "session": self._public_row(row)}

    def record_campaign(
        self,
        *,
        filters: Dict[str, Any] | None,
        lab_payload: Dict[str, Any] | None,
        native_targets_payload: Dict[str, Any] | None,
        guidance_payload: Dict[str, Any] | None = None,
        source: str = "",
        label: str = "",
        session_ids: List[str] | None = None,
        app_targets: List[str] | None = None,
        session_rows: List[Dict[str, Any]] | None = None,
        sweep_runs: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        clean_filters = dict(filters) if isinstance(filters, dict) else {}
        lab_snapshot = dict(lab_payload) if isinstance(lab_payload, dict) else {}
        native_snapshot = dict(native_targets_payload) if isinstance(native_targets_payload, dict) else {}
        guidance_snapshot = dict(guidance_payload) if isinstance(guidance_payload, dict) else {}
        clean_session_ids = self._dedupe_strings([str(item).strip() for item in (session_ids or []) if str(item).strip()])
        clean_app_targets = self._dedupe_strings([str(item).strip() for item in (app_targets or []) if str(item).strip()])
        now = datetime.now(timezone.utc).isoformat()
        campaign_id = f"benchcampaign-{uuid.uuid4().hex[:12]}"
        with self._lock:
            hydrated_session_rows = self._hydrate_campaign_sessions(
                session_ids=clean_session_ids,
                session_rows=session_rows,
            )
            row = self._campaign_row(
                campaign_id=campaign_id,
                created_at=now,
                updated_at=now,
                filters=clean_filters,
                lab_snapshot=lab_snapshot,
                native_targets_snapshot=native_snapshot,
                guidance_snapshot=guidance_snapshot,
                source=source,
                label=label,
                session_ids=clean_session_ids,
                app_targets=clean_app_targets,
                session_rows=hydrated_session_rows,
                sweep_runs=sweep_runs or [],
            )
            self._campaigns[campaign_id] = row
            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
        return {"status": "success", "campaign": self._public_row(row)}

    def get_campaign(self, campaign_id: str) -> Dict[str, Any]:
        clean_id = str(campaign_id or "").strip()
        if not clean_id:
            return {"status": "error", "message": "campaign_id required"}
        with self._lock:
            row = self._campaigns.get(clean_id)
            if not isinstance(row, dict):
                return {"status": "error", "message": "benchmark lab campaign not found"}
            return {"status": "success", "campaign": self._public_row(row)}

    def record_program(
        self,
        *,
        filters: Dict[str, Any] | None,
        lab_payload: Dict[str, Any] | None,
        native_targets_payload: Dict[str, Any] | None,
        guidance_payload: Dict[str, Any] | None = None,
        source: str = "",
        label: str = "",
        campaign_ids: List[str] | None = None,
        app_targets: List[str] | None = None,
        campaign_rows: List[Dict[str, Any]] | None = None,
        cycle_runs: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        clean_filters = dict(filters) if isinstance(filters, dict) else {}
        lab_snapshot = dict(lab_payload) if isinstance(lab_payload, dict) else {}
        native_snapshot = dict(native_targets_payload) if isinstance(native_targets_payload, dict) else {}
        guidance_snapshot = dict(guidance_payload) if isinstance(guidance_payload, dict) else {}
        clean_campaign_ids = self._dedupe_strings([str(item).strip() for item in (campaign_ids or []) if str(item).strip()])
        clean_app_targets = self._dedupe_strings([str(item).strip() for item in (app_targets or []) if str(item).strip()])
        now = datetime.now(timezone.utc).isoformat()
        program_id = f"benchprogram-{uuid.uuid4().hex[:12]}"
        with self._lock:
            hydrated_campaign_rows = self._hydrate_program_campaigns(
                campaign_ids=clean_campaign_ids,
                campaign_rows=campaign_rows,
            )
            row = self._program_row(
                program_id=program_id,
                created_at=now,
                updated_at=now,
                filters=clean_filters,
                lab_snapshot=lab_snapshot,
                native_targets_snapshot=native_snapshot,
                guidance_snapshot=guidance_snapshot,
                source=source,
                label=label,
                campaign_ids=clean_campaign_ids,
                app_targets=clean_app_targets,
                campaign_rows=hydrated_campaign_rows,
                cycle_runs=cycle_runs or [],
            )
            self._programs[program_id] = row
            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
        return {"status": "success", "program": self._public_row(row)}

    def get_program(self, program_id: str) -> Dict[str, Any]:
        clean_id = str(program_id or "").strip()
        if not clean_id:
            return {"status": "error", "message": "program_id required"}
        with self._lock:
            row = self._programs.get(clean_id)
            if not isinstance(row, dict):
                return {"status": "error", "message": "benchmark lab program not found"}
            return {"status": "success", "program": self._public_row(row)}

    def record_portfolio(
        self,
        *,
        filters: Dict[str, Any] | None,
        lab_payload: Dict[str, Any] | None,
        native_targets_payload: Dict[str, Any] | None,
        guidance_payload: Dict[str, Any] | None = None,
        source: str = "",
        label: str = "",
        program_ids: List[str] | None = None,
        app_targets: List[str] | None = None,
        program_rows: List[Dict[str, Any]] | None = None,
        wave_runs: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        clean_filters = dict(filters) if isinstance(filters, dict) else {}
        lab_snapshot = dict(lab_payload) if isinstance(lab_payload, dict) else {}
        native_snapshot = dict(native_targets_payload) if isinstance(native_targets_payload, dict) else {}
        guidance_snapshot = dict(guidance_payload) if isinstance(guidance_payload, dict) else {}
        clean_program_ids = self._dedupe_strings([str(item).strip() for item in (program_ids or []) if str(item).strip()])
        clean_app_targets = self._dedupe_strings([str(item).strip() for item in (app_targets or []) if str(item).strip()])
        now = datetime.now(timezone.utc).isoformat()
        portfolio_id = f"benchportfolio-{uuid.uuid4().hex[:12]}"
        with self._lock:
            hydrated_program_rows = self._hydrate_portfolio_programs(
                program_ids=clean_program_ids,
                program_rows=program_rows,
            )
            row = self._portfolio_row(
                portfolio_id=portfolio_id,
                created_at=now,
                updated_at=now,
                filters=clean_filters,
                lab_snapshot=lab_snapshot,
                native_targets_snapshot=native_snapshot,
                guidance_snapshot=guidance_snapshot,
                source=source,
                label=label,
                program_ids=clean_program_ids,
                app_targets=clean_app_targets,
                program_rows=hydrated_program_rows,
                wave_runs=wave_runs or [],
            )
            self._portfolios[portfolio_id] = row
            self._trim_locked()
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
        return {"status": "success", "portfolio": self._public_row(row)}

    def get_portfolio(self, portfolio_id: str) -> Dict[str, Any]:
        clean_id = str(portfolio_id or "").strip()
        if not clean_id:
            return {"status": "error", "message": "portfolio_id required"}
        with self._lock:
            row = self._portfolios.get(clean_id)
            if not isinstance(row, dict):
                return {"status": "error", "message": "benchmark lab portfolio not found"}
            return {"status": "success", "portfolio": self._public_row(row)}

    def session_history(
        self,
        *,
        limit: int = 12,
        session_id: str = "",
        status: str = "",
    ) -> Dict[str, Any]:
        normalized_limit = self._coerce_int(limit, minimum=1, maximum=self.max_sessions, default=12)
        clean_session_id = str(session_id or "").strip()
        clean_status = str(status or "").strip().lower()
        with self._lock:
            rows = sorted(
                (dict(item) for item in self._sessions.values() if isinstance(item, dict)),
                key=lambda item: str(item.get("updated_at", "") or ""),
                reverse=True,
            )
        if clean_session_id:
            rows = [row for row in rows if str(row.get("session_id", "") or "").strip() == clean_session_id]
        if clean_status:
            rows = [row for row in rows if str(row.get("status", "") or "").strip().lower() == clean_status]
        selected = rows[:normalized_limit]
        status_counts: Dict[str, int] = {}
        replay_status_counts: Dict[str, int] = {}
        latest_cycle_status_counts: Dict[str, int] = {}
        for row in rows:
            self._increment_count(status_counts, str(row.get("status", "") or "ready"))
            self._increment_count(
                latest_cycle_status_counts,
                str(row.get("latest_cycle_regression_status", row.get("latest_cycle_status", "")) or "idle"),
            )
            for candidate in row.get("replay_candidates", []) if isinstance(row.get("replay_candidates", []), list) else []:
                if isinstance(candidate, dict):
                    self._increment_count(replay_status_counts, str(candidate.get("replay_status", "") or "pending"))
        latest = selected[0] if selected else {}
        return {
            "status": "success",
            "count": len(selected),
            "total": len(rows),
            "limit": normalized_limit,
            "items": [self._public_row(row) for row in selected],
            "latest_session": self._public_row(latest) if latest else {},
            "summary": {
                "status_counts": self._sorted_count_map(status_counts),
                "replay_status_counts": self._sorted_count_map(replay_status_counts),
                "latest_cycle_status_counts": self._sorted_count_map(latest_cycle_status_counts),
                "pending_replays": sum(int(row.get("pending_replay_count", 0) or 0) for row in rows),
                "failed_replays": sum(int(row.get("failed_replay_count", 0) or 0) for row in rows),
                "completed_replays": sum(int(row.get("completed_replay_count", 0) or 0) for row in rows),
                "cycle_count": sum(int(row.get("cycle_count", 0) or 0) for row in rows),
                "completed_cycles": sum(int(row.get("completed_cycle_count", 0) or 0) for row in rows),
                "regression_cycles": sum(int(row.get("regression_cycle_count", 0) or 0) for row in rows),
                "long_horizon_pending_replays": sum(int(row.get("long_horizon_pending_count", 0) or 0) for row in rows),
            },
        }

    def campaign_history(
        self,
        *,
        limit: int = 12,
        campaign_id: str = "",
        status: str = "",
    ) -> Dict[str, Any]:
        normalized_limit = self._coerce_int(limit, minimum=1, maximum=self.max_campaigns, default=12)
        clean_campaign_id = str(campaign_id or "").strip()
        clean_status = str(status or "").strip().lower()
        with self._lock:
            rows = sorted(
                (dict(item) for item in self._campaigns.values() if isinstance(item, dict)),
                key=lambda item: str(item.get("updated_at", "") or ""),
                reverse=True,
            )
        if clean_campaign_id:
            rows = [row for row in rows if str(row.get("campaign_id", "") or "").strip() == clean_campaign_id]
        if clean_status:
            rows = [row for row in rows if str(row.get("status", "") or "").strip().lower() == clean_status]
        selected = rows[:normalized_limit]
        status_counts: Dict[str, int] = {}
        latest_sweep_status_counts: Dict[str, int] = {}
        trend_direction_counts: Dict[str, int] = {}
        priority_counts: Dict[str, int] = {}
        for row in rows:
            self._increment_count(status_counts, str(row.get("status", "") or "ready"))
            self._increment_count(
                latest_sweep_status_counts,
                str(row.get("latest_sweep_regression_status", row.get("latest_sweep_status", "")) or "idle"),
            )
            trend_summary = dict(row.get("trend_summary", {})) if isinstance(row.get("trend_summary", {}), dict) else {}
            self._increment_count(
                trend_direction_counts,
                str(trend_summary.get("direction", row.get("history_direction", "")) or "stable"),
            )
            self._increment_count(priority_counts, str(row.get("campaign_priority", "") or "steady"))
        latest = selected[0] if selected else {}
        pressure_total = sum(float(row.get("campaign_pressure_score", 0.0) or 0.0) for row in rows)
        return {
            "status": "success",
            "count": len(selected),
            "total": len(rows),
            "limit": normalized_limit,
            "items": [self._public_row(row) for row in selected],
            "latest_campaign": self._public_row(latest) if latest else {},
            "summary": {
                "status_counts": self._sorted_count_map(status_counts),
                "latest_sweep_status_counts": self._sorted_count_map(latest_sweep_status_counts),
                "pending_sessions": sum(int(row.get("pending_session_count", 0) or 0) for row in rows),
                "attention_sessions": sum(int(row.get("attention_session_count", 0) or 0) for row in rows),
                "complete_sessions": sum(int(row.get("complete_session_count", 0) or 0) for row in rows),
                "pending_replays": sum(int(row.get("pending_replay_count", 0) or 0) for row in rows),
                "failed_replays": sum(int(row.get("failed_replay_count", 0) or 0) for row in rows),
                "completed_replays": sum(int(row.get("completed_replay_count", 0) or 0) for row in rows),
                "cycle_count": sum(int(row.get("cycle_count", 0) or 0) for row in rows),
                "regression_cycles": sum(int(row.get("regression_cycle_count", 0) or 0) for row in rows),
                "long_horizon_pending_replays": sum(int(row.get("long_horizon_pending_count", 0) or 0) for row in rows),
                "sweep_count": sum(int(row.get("sweep_count", 0) or 0) for row in rows),
                "completed_sweep_count": sum(int(row.get("completed_sweep_count", 0) or 0) for row in rows),
                "regression_sweep_count": sum(int(row.get("regression_sweep_count", 0) or 0) for row in rows),
                "pending_app_targets": sum(int(row.get("pending_app_target_count", 0) or 0) for row in rows),
                "trend_direction_counts": self._sorted_count_map(trend_direction_counts),
                "priority_counts": self._sorted_count_map(priority_counts),
                "campaign_pressure_total": round(pressure_total, 6),
                "campaign_pressure_avg": round(pressure_total / len(rows), 6) if rows else 0.0,
            },
        }

    def program_history(
        self,
        *,
        limit: int = 12,
        program_id: str = "",
        status: str = "",
    ) -> Dict[str, Any]:
        normalized_limit = self._coerce_int(limit, minimum=1, maximum=self.max_programs, default=12)
        clean_program_id = str(program_id or "").strip()
        clean_status = str(status or "").strip().lower()
        with self._lock:
            rows = sorted(
                (dict(item) for item in self._programs.values() if isinstance(item, dict)),
                key=lambda item: str(item.get("updated_at", "") or ""),
                reverse=True,
            )
        if clean_program_id:
            rows = [row for row in rows if str(row.get("program_id", "") or "").strip() == clean_program_id]
        if clean_status:
            rows = [row for row in rows if str(row.get("status", "") or "").strip().lower() == clean_status]
        selected = rows[:normalized_limit]
        status_counts: Dict[str, int] = {}
        latest_cycle_status_counts: Dict[str, int] = {}
        trend_direction_counts: Dict[str, int] = {}
        priority_counts: Dict[str, int] = {}
        pressure_total = 0.0
        for row in rows:
            self._increment_count(status_counts, str(row.get("status", "") or "ready"))
            self._increment_count(
                latest_cycle_status_counts,
                str(row.get("latest_cycle_status", "") or "idle"),
            )
            trend_summary = dict(row.get("trend_summary", {})) if isinstance(row.get("trend_summary", {}), dict) else {}
            self._increment_count(
                trend_direction_counts,
                str(trend_summary.get("direction", row.get("history_direction", "")) or "stable"),
            )
            self._increment_count(priority_counts, str(row.get("program_priority", "") or "steady"))
            pressure_total += float(row.get("program_pressure_score", 0.0) or 0.0)
        latest = selected[0] if selected else {}
        return {
            "status": "success",
            "count": len(selected),
            "total": len(rows),
            "limit": normalized_limit,
            "items": [self._public_row(row) for row in selected],
            "latest_program": self._public_row(latest) if latest else {},
            "summary": {
                "status_counts": self._sorted_count_map(status_counts),
                "latest_cycle_status_counts": self._sorted_count_map(latest_cycle_status_counts),
                "trend_direction_counts": self._sorted_count_map(trend_direction_counts),
                "priority_counts": self._sorted_count_map(priority_counts),
                "campaign_count": sum(int(row.get("campaign_count", 0) or 0) for row in rows),
                "pending_campaigns": sum(int(row.get("pending_campaign_count", 0) or 0) for row in rows),
                "attention_campaigns": sum(int(row.get("attention_campaign_count", 0) or 0) for row in rows),
                "pending_sessions": sum(int(row.get("pending_session_count", 0) or 0) for row in rows),
                "attention_sessions": sum(int(row.get("attention_session_count", 0) or 0) for row in rows),
                "pending_app_targets": sum(int(row.get("pending_app_target_count", 0) or 0) for row in rows),
                "cycle_count": sum(int(row.get("cycle_count", 0) or 0) for row in rows),
                "completed_cycles": sum(int(row.get("completed_cycle_count", 0) or 0) for row in rows),
                "stable_cycles": sum(int(row.get("stable_cycle_count", 0) or 0) for row in rows),
                "regression_cycles": sum(int(row.get("regression_cycle_count", 0) or 0) for row in rows),
                "program_pressure_total": round(pressure_total, 6),
                "program_pressure_avg": round(pressure_total / len(rows), 6) if rows else 0.0,
            },
        }

    def portfolio_history(
        self,
        *,
        limit: int = 12,
        portfolio_id: str = "",
        status: str = "",
    ) -> Dict[str, Any]:
        normalized_limit = self._coerce_int(limit, minimum=1, maximum=self.max_portfolios, default=12)
        clean_portfolio_id = str(portfolio_id or "").strip()
        clean_status = str(status or "").strip().lower()
        with self._lock:
            rows = sorted(
                (dict(item) for item in self._portfolios.values() if isinstance(item, dict)),
                key=lambda item: str(item.get("updated_at", "") or ""),
                reverse=True,
            )
        if clean_portfolio_id:
            rows = [row for row in rows if str(row.get("portfolio_id", "") or "").strip() == clean_portfolio_id]
        if clean_status:
            rows = [row for row in rows if str(row.get("status", "") or "").strip().lower() == clean_status]
        selected = rows[:normalized_limit]
        status_counts: Dict[str, int] = {}
        latest_wave_status_counts: Dict[str, int] = {}
        trend_direction_counts: Dict[str, int] = {}
        priority_counts: Dict[str, int] = {}
        app_target_counts: Dict[str, int] = {}
        focus_summary_counts: Dict[str, int] = {}
        wave_stop_reason_counts: Dict[str, int] = {}
        pressure_total = 0.0
        for row in rows:
            self._increment_count(status_counts, str(row.get("status", "") or "ready"))
            self._increment_count(
                latest_wave_status_counts,
                str(row.get("latest_wave_status", "") or "idle"),
            )
            trend_summary = dict(row.get("trend_summary", {})) if isinstance(row.get("trend_summary", {}), dict) else {}
            self._increment_count(
                trend_direction_counts,
                str(trend_summary.get("direction", row.get("history_direction", "")) or "stable"),
            )
            self._increment_count(priority_counts, str(row.get("portfolio_priority", "") or "steady"))
            self._increment_count(
                wave_stop_reason_counts,
                str(row.get("latest_wave_stop_reason", "") or "idle"),
            )
            for app_name in row.get("target_apps", row.get("app_targets", [])) if isinstance(
                row.get("target_apps", row.get("app_targets", [])),
                list,
            ) else []:
                clean_app = str(app_name or "").strip()
                if clean_app:
                    self._increment_count(app_target_counts, clean_app)
            for hint in row.get("focus_summary", []) if isinstance(row.get("focus_summary", []), list) else []:
                clean_hint = str(hint or "").strip()
                if clean_hint:
                    self._increment_count(focus_summary_counts, clean_hint)
            pressure_total += float(row.get("portfolio_pressure_score", 0.0) or 0.0)
        latest = selected[0] if selected else {}
        ranked_rows = sorted(
            rows,
            key=lambda item: (
                -float(item.get("portfolio_pressure_score", 0.0) or 0.0),
                -int(item.get("pending_program_count", 0) or 0),
                -int(item.get("pending_campaign_count", 0) or 0),
                -int(item.get("pending_session_count", 0) or 0),
                str(item.get("updated_at", "") or ""),
            ),
            reverse=False,
        )
        top_portfolios = [
            {
                "portfolio_id": str(item.get("portfolio_id", "") or ""),
                "label": str(item.get("label", "") or "desktop replay portfolio"),
                "status": str(item.get("status", "") or "ready"),
                "portfolio_priority": str(item.get("portfolio_priority", "") or "steady"),
                "portfolio_pressure_score": round(float(item.get("portfolio_pressure_score", 0.0) or 0.0), 6),
                "trend_direction": str(
                    dict(item.get("trend_summary", {})).get("direction", item.get("history_direction", ""))
                    if isinstance(item.get("trend_summary", {}), dict)
                    else item.get("history_direction", "")
                )
                or "stable",
                "latest_wave_status": str(item.get("latest_wave_status", "") or "idle"),
                "latest_wave_stop_reason": str(item.get("latest_wave_stop_reason", "") or "idle"),
                "pending_program_count": int(item.get("pending_program_count", 0) or 0),
                "pending_campaign_count": int(item.get("pending_campaign_count", 0) or 0),
                "pending_session_count": int(item.get("pending_session_count", 0) or 0),
                "pending_app_target_count": int(item.get("pending_app_target_count", 0) or 0),
                "long_horizon_pending_count": int(item.get("long_horizon_pending_count", 0) or 0),
                "target_apps": [
                    str(app_name).strip()
                    for app_name in item.get("target_apps", item.get("app_targets", []))
                    if str(app_name).strip()
                ][:6]
                if isinstance(item.get("target_apps", item.get("app_targets", [])), list)
                else [],
                "focus_summary": [
                    str(hint).strip()
                    for hint in item.get("focus_summary", [])
                    if str(hint).strip()
                ][:6]
                if isinstance(item.get("focus_summary", []), list)
                else [],
            }
            for item in ranked_rows[: min(5, len(ranked_rows))]
        ]
        return {
            "status": "success",
            "count": len(selected),
            "total": len(rows),
            "limit": normalized_limit,
            "items": [self._public_row(row) for row in selected],
            "latest_portfolio": self._public_row(latest) if latest else {},
            "top_portfolios": top_portfolios,
            "summary": {
                "status_counts": self._sorted_count_map(status_counts),
                "latest_wave_status_counts": self._sorted_count_map(latest_wave_status_counts),
                "trend_direction_counts": self._sorted_count_map(trend_direction_counts),
                "priority_counts": self._sorted_count_map(priority_counts),
                "app_target_counts": self._sorted_count_map(app_target_counts),
                "focus_summary_counts": self._sorted_count_map(focus_summary_counts),
                "wave_stop_reason_counts": self._sorted_count_map(wave_stop_reason_counts),
                "program_count": sum(int(row.get("program_count", 0) or 0) for row in rows),
                "pending_programs": sum(int(row.get("pending_program_count", 0) or 0) for row in rows),
                "attention_programs": sum(int(row.get("attention_program_count", 0) or 0) for row in rows),
                "pending_campaigns": sum(int(row.get("pending_campaign_count", 0) or 0) for row in rows),
                "attention_campaigns": sum(int(row.get("attention_campaign_count", 0) or 0) for row in rows),
                "pending_sessions": sum(int(row.get("pending_session_count", 0) or 0) for row in rows),
                "pending_app_targets": sum(int(row.get("pending_app_target_count", 0) or 0) for row in rows),
                "long_horizon_pending_count": sum(int(row.get("long_horizon_pending_count", 0) or 0) for row in rows),
                "wave_count": sum(int(row.get("wave_count", 0) or 0) for row in rows),
                "completed_waves": sum(int(row.get("completed_wave_count", 0) or 0) for row in rows),
                "stable_waves": sum(int(row.get("stable_wave_count", 0) or 0) for row in rows),
                "regression_waves": sum(int(row.get("regression_wave_count", 0) or 0) for row in rows),
                "portfolio_pressure_total": round(pressure_total, 6),
                "portfolio_pressure_avg": round(pressure_total / len(rows), 6) if rows else 0.0,
            },
        }

    def record_replay_result(
        self,
        *,
        session_id: str,
        scenario_name: str,
        replay_payload: Dict[str, Any] | None,
        replay_query: Dict[str, Any] | None = None,
        lab_payload: Dict[str, Any] | None = None,
        native_targets_payload: Dict[str, Any] | None = None,
        guidance_payload: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_id = str(session_id or "").strip()
        clean_scenario = str(scenario_name or "").strip()
        if not clean_id or not clean_scenario:
            return {"status": "error", "message": "session_id and scenario_name required"}
        replay_snapshot = dict(replay_payload) if isinstance(replay_payload, dict) else {}
        replay_query_snapshot = dict(replay_query) if isinstance(replay_query, dict) else {}
        with self._lock:
            existing = self._sessions.get(clean_id)
            if not isinstance(existing, dict):
                return {"status": "error", "message": "benchmark lab session not found"}
            candidates = self._normalize_replay_candidates(existing.get("replay_candidates", []))
            replay_status = self._replay_status_from_payload(replay_snapshot)
            now = datetime.now(timezone.utc).isoformat()
            updated_candidate: Dict[str, Any] | None = None
            found = False
            for candidate in candidates:
                if str(candidate.get("scenario", "") or "").strip() != clean_scenario:
                    continue
                candidate["replay_status"] = replay_status
                candidate["replay_count"] = int(candidate.get("replay_count", 0) or 0) + 1
                candidate["last_replayed_at"] = now
                candidate["last_result_status"] = str(replay_snapshot.get("status", "") or "success").strip() or "success"
                candidate["last_regression_status"] = str(
                    dict(replay_snapshot.get("regression", {})).get("status", "") if isinstance(replay_snapshot.get("regression", {}), dict) else ""
                ).strip()
                candidate["last_weighted_score"] = round(
                    float(dict(replay_snapshot.get("summary", {})).get("weighted_score", 0.0) or 0.0)
                    if isinstance(replay_snapshot.get("summary", {}), dict)
                    else 0.0,
                    6,
                )
                candidate["last_weighted_pass_rate"] = round(
                    float(dict(replay_snapshot.get("summary", {})).get("weighted_pass_rate", 0.0) or 0.0)
                    if isinstance(replay_snapshot.get("summary", {}), dict)
                    else 0.0,
                    6,
                )
                if replay_query_snapshot:
                    candidate["replay_query"] = dict(replay_query_snapshot)
                updated_candidate = dict(candidate)
                found = True
                break
            if not found:
                appended = {
                    "scenario": clean_scenario,
                    "replay_query": dict(replay_query_snapshot),
                    "replay_status": replay_status,
                    "replay_count": 1,
                    "last_replayed_at": now,
                    "last_result_status": str(replay_snapshot.get("status", "") or "success").strip() or "success",
                    "reasons": ["ad_hoc_replay"],
                }
                candidates.append(appended)
                updated_candidate = dict(appended)
            replay_events = list(existing.get("replay_events", [])) if isinstance(existing.get("replay_events", []), list) else []
            replay_events.append(
                {
                    "scenario": clean_scenario,
                    "replay_status": replay_status,
                    "recorded_at": now,
                    "weighted_score": round(
                        float(dict(replay_snapshot.get("summary", {})).get("weighted_score", 0.0) or 0.0)
                        if isinstance(replay_snapshot.get("summary", {}), dict)
                        else 0.0,
                        6,
                    ),
                    "weighted_pass_rate": round(
                        float(dict(replay_snapshot.get("summary", {})).get("weighted_pass_rate", 0.0) or 0.0)
                        if isinstance(replay_snapshot.get("summary", {}), dict)
                        else 0.0,
                        6,
                    ),
                    "regression_status": str(
                        dict(replay_snapshot.get("regression", {})).get("status", "") if isinstance(replay_snapshot.get("regression", {}), dict) else ""
                    ).strip(),
                }
            )
            replay_events = replay_events[-self.max_replay_events :]
            lab_snapshot = dict(lab_payload) if isinstance(lab_payload, dict) else dict(existing.get("lab_snapshot", {}))
            merged_candidates = self._merge_replay_candidates(
                candidates,
                self._normalize_replay_candidates(lab_snapshot.get("replay_candidates", [])),
            )
            row = self._session_row(
                session_id=clean_id,
                created_at=str(existing.get("created_at", "") or now),
                updated_at=now,
                filters=dict(existing.get("filters", {})) if isinstance(existing.get("filters", {}), dict) else {},
                lab_snapshot=lab_snapshot,
                native_targets_snapshot=(
                    dict(native_targets_payload)
                    if isinstance(native_targets_payload, dict)
                    else dict(existing.get("native_targets_snapshot", {}))
                ),
                guidance_snapshot=(
                    dict(guidance_payload)
                    if isinstance(guidance_payload, dict)
                    else dict(existing.get("guidance_snapshot", {}))
                ),
                source=str(existing.get("source", "") or ""),
                label=str(existing.get("label", "") or ""),
                replay_candidates=merged_candidates,
                replay_events=replay_events,
                run_cycles=self._normalize_run_cycles(existing.get("run_cycles", [])),
            )
            self._sessions[clean_id] = row
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
        return {
            "status": "success",
            "session": self._public_row(row),
            "updated_candidate": updated_candidate or {},
        }

    def record_campaign_sweep(
        self,
        *,
        campaign_id: str,
        sweep_payload: Dict[str, Any] | None,
        lab_payload: Dict[str, Any] | None = None,
        native_targets_payload: Dict[str, Any] | None = None,
        guidance_payload: Dict[str, Any] | None = None,
        session_ids: List[str] | None = None,
        app_targets: List[str] | None = None,
        session_rows: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        clean_id = str(campaign_id or "").strip()
        if not clean_id:
            return {"status": "error", "message": "campaign_id required"}
        sweep_snapshot = dict(sweep_payload) if isinstance(sweep_payload, dict) else {}
        with self._lock:
            existing = self._campaigns.get(clean_id)
            if not isinstance(existing, dict):
                return {"status": "error", "message": "benchmark lab campaign not found"}
            now = datetime.now(timezone.utc).isoformat()
            clean_session_ids = self._dedupe_strings(
                [
                    *(
                        [str(item).strip() for item in existing.get("session_ids", [])]
                        if isinstance(existing.get("session_ids", []), list)
                        else []
                    ),
                    *([str(item).strip() for item in (session_ids or []) if str(item).strip()]),
                ]
            )
            clean_app_targets = self._dedupe_strings(
                [
                    *(
                        [str(item).strip() for item in existing.get("app_targets", [])]
                        if isinstance(existing.get("app_targets", []), list)
                        else []
                    ),
                    *([str(item).strip() for item in (app_targets or []) if str(item).strip()]),
                ]
            )
            hydrated_session_rows = self._hydrate_campaign_sessions(
                session_ids=clean_session_ids,
                session_rows=session_rows,
            )
            sweep_runs = self._normalize_campaign_sweep_runs(existing.get("sweep_runs", []))
            sweep_runs.append(
                self._campaign_sweep_row(
                    recorded_at=now,
                    sweep_payload=sweep_snapshot,
                )
            )
            sweep_runs = sweep_runs[-self.max_sweep_runs :]
            row = self._campaign_row(
                campaign_id=clean_id,
                created_at=str(existing.get("created_at", "") or now),
                updated_at=now,
                filters=dict(existing.get("filters", {})) if isinstance(existing.get("filters", {}), dict) else {},
                lab_snapshot=dict(lab_payload) if isinstance(lab_payload, dict) else dict(existing.get("lab_snapshot", {})),
                native_targets_snapshot=(
                    dict(native_targets_payload)
                    if isinstance(native_targets_payload, dict)
                    else dict(existing.get("native_targets_snapshot", {}))
                ),
                guidance_snapshot=(
                    dict(guidance_payload)
                    if isinstance(guidance_payload, dict)
                    else dict(existing.get("guidance_snapshot", {}))
                ),
                source=str(existing.get("source", "") or ""),
                label=str(existing.get("label", "") or ""),
                session_ids=clean_session_ids,
                app_targets=clean_app_targets,
                session_rows=hydrated_session_rows,
                sweep_runs=sweep_runs,
            )
            self._campaigns[clean_id] = row
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
        return {
            "status": "success",
            "campaign": self._public_row(row),
            "sweep": dict(sweep_runs[-1]) if sweep_runs else {},
        }

    def record_program_cycle(
        self,
        *,
        program_id: str,
        cycle_payload: Dict[str, Any] | None,
        lab_payload: Dict[str, Any] | None = None,
        native_targets_payload: Dict[str, Any] | None = None,
        guidance_payload: Dict[str, Any] | None = None,
        campaign_ids: List[str] | None = None,
        app_targets: List[str] | None = None,
        campaign_rows: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        clean_id = str(program_id or "").strip()
        if not clean_id:
            return {"status": "error", "message": "program_id required"}
        cycle_snapshot = dict(cycle_payload) if isinstance(cycle_payload, dict) else {}
        with self._lock:
            existing = self._programs.get(clean_id)
            if not isinstance(existing, dict):
                return {"status": "error", "message": "benchmark lab program not found"}
            now = datetime.now(timezone.utc).isoformat()
            clean_campaign_ids = self._dedupe_strings(
                [
                    *(
                        [str(item).strip() for item in existing.get("campaign_ids", [])]
                        if isinstance(existing.get("campaign_ids", []), list)
                        else []
                    ),
                    *([str(item).strip() for item in (campaign_ids or []) if str(item).strip()]),
                ]
            )
            clean_app_targets = self._dedupe_strings(
                [
                    *(
                        [str(item).strip() for item in existing.get("app_targets", [])]
                        if isinstance(existing.get("app_targets", []), list)
                        else []
                    ),
                    *([str(item).strip() for item in (app_targets or []) if str(item).strip()]),
                ]
            )
            hydrated_campaign_rows = self._hydrate_program_campaigns(
                campaign_ids=clean_campaign_ids,
                campaign_rows=campaign_rows,
            )
            cycle_runs = self._normalize_program_cycle_runs(existing.get("cycle_runs", []))
            cycle_runs.append(
                self._program_cycle_row(
                    recorded_at=now,
                    cycle_payload=cycle_snapshot,
                )
            )
            cycle_runs = cycle_runs[-self.max_run_cycles :]
            row = self._program_row(
                program_id=clean_id,
                created_at=str(existing.get("created_at", "") or now),
                updated_at=now,
                filters=dict(existing.get("filters", {})) if isinstance(existing.get("filters", {}), dict) else {},
                lab_snapshot=dict(lab_payload) if isinstance(lab_payload, dict) else dict(existing.get("lab_snapshot", {})),
                native_targets_snapshot=(
                    dict(native_targets_payload)
                    if isinstance(native_targets_payload, dict)
                    else dict(existing.get("native_targets_snapshot", {}))
                ),
                guidance_snapshot=(
                    dict(guidance_payload)
                    if isinstance(guidance_payload, dict)
                    else dict(existing.get("guidance_snapshot", {}))
                ),
                source=str(existing.get("source", "") or ""),
                label=str(existing.get("label", "") or ""),
                campaign_ids=clean_campaign_ids,
                app_targets=clean_app_targets,
                campaign_rows=hydrated_campaign_rows,
                cycle_runs=cycle_runs,
            )
            self._programs[clean_id] = row
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
        return {
            "status": "success",
            "program": self._public_row(row),
            "cycle": dict(cycle_runs[-1]) if cycle_runs else {},
        }

    def record_portfolio_wave(
        self,
        *,
        portfolio_id: str,
        wave_payload: Dict[str, Any] | None,
        lab_payload: Dict[str, Any] | None = None,
        native_targets_payload: Dict[str, Any] | None = None,
        guidance_payload: Dict[str, Any] | None = None,
        program_ids: List[str] | None = None,
        app_targets: List[str] | None = None,
        program_rows: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        clean_id = str(portfolio_id or "").strip()
        if not clean_id:
            return {"status": "error", "message": "portfolio_id required"}
        wave_snapshot = dict(wave_payload) if isinstance(wave_payload, dict) else {}
        with self._lock:
            existing = self._portfolios.get(clean_id)
            if not isinstance(existing, dict):
                return {"status": "error", "message": "benchmark lab portfolio not found"}
            now = datetime.now(timezone.utc).isoformat()
            clean_program_ids = self._dedupe_strings(
                [
                    *(
                        [str(item).strip() for item in existing.get("program_ids", [])]
                        if isinstance(existing.get("program_ids", []), list)
                        else []
                    ),
                    *([str(item).strip() for item in (program_ids or []) if str(item).strip()]),
                ]
            )
            clean_app_targets = self._dedupe_strings(
                [
                    *(
                        [str(item).strip() for item in existing.get("app_targets", [])]
                        if isinstance(existing.get("app_targets", []), list)
                        else []
                    ),
                    *([str(item).strip() for item in (app_targets or []) if str(item).strip()]),
                ]
            )
            hydrated_program_rows = self._hydrate_portfolio_programs(
                program_ids=clean_program_ids,
                program_rows=program_rows,
            )
            wave_runs = self._normalize_portfolio_wave_runs(existing.get("wave_runs", []))
            wave_runs.append(
                self._portfolio_wave_row(
                    recorded_at=now,
                    wave_payload=wave_snapshot,
                )
            )
            wave_runs = wave_runs[-self.max_run_cycles :]
            row = self._portfolio_row(
                portfolio_id=clean_id,
                created_at=str(existing.get("created_at", "") or now),
                updated_at=now,
                filters=dict(existing.get("filters", {})) if isinstance(existing.get("filters", {}), dict) else {},
                lab_snapshot=dict(lab_payload) if isinstance(lab_payload, dict) else dict(existing.get("lab_snapshot", {})),
                native_targets_snapshot=(
                    dict(native_targets_payload)
                    if isinstance(native_targets_payload, dict)
                    else dict(existing.get("native_targets_snapshot", {}))
                ),
                guidance_snapshot=(
                    dict(guidance_payload)
                    if isinstance(guidance_payload, dict)
                    else dict(existing.get("guidance_snapshot", {}))
                ),
                source=str(existing.get("source", "") or ""),
                label=str(existing.get("label", "") or ""),
                program_ids=clean_program_ids,
                app_targets=clean_app_targets,
                program_rows=hydrated_program_rows,
                wave_runs=wave_runs,
            )
            self._portfolios[clean_id] = row
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
        return {
            "status": "success",
            "portfolio": self._public_row(row),
            "wave": dict(wave_runs[-1]) if wave_runs else {},
        }

    def record_run_cycle(
        self,
        *,
        session_id: str,
        cycle_payload: Dict[str, Any] | None,
        cycle_query: Dict[str, Any] | None = None,
        lab_payload: Dict[str, Any] | None = None,
        native_targets_payload: Dict[str, Any] | None = None,
        guidance_payload: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        clean_id = str(session_id or "").strip()
        if not clean_id:
            return {"status": "error", "message": "session_id required"}
        cycle_snapshot = dict(cycle_payload) if isinstance(cycle_payload, dict) else {}
        cycle_query_snapshot = dict(cycle_query) if isinstance(cycle_query, dict) else {}
        with self._lock:
            existing = self._sessions.get(clean_id)
            if not isinstance(existing, dict):
                return {"status": "error", "message": "benchmark lab session not found"}
            now = datetime.now(timezone.utc).isoformat()
            filters = dict(existing.get("filters", {})) if isinstance(existing.get("filters", {}), dict) else {}
            lab_snapshot = dict(lab_payload) if isinstance(lab_payload, dict) else dict(existing.get("lab_snapshot", {}))
            native_targets_snapshot = (
                dict(native_targets_payload)
                if isinstance(native_targets_payload, dict)
                else dict(existing.get("native_targets_snapshot", {}))
            )
            guidance_snapshot = (
                dict(guidance_payload)
                if isinstance(guidance_payload, dict)
                else dict(existing.get("guidance_snapshot", {}))
            )
            merged_candidates = self._merge_replay_candidates(
                self._normalize_replay_candidates(existing.get("replay_candidates", [])),
                self._normalize_replay_candidates(lab_snapshot.get("replay_candidates", [])),
            )
            run_cycles = self._normalize_run_cycles(existing.get("run_cycles", []))
            run_cycles.append(
                self._cycle_row(
                    recorded_at=now,
                    cycle_payload=cycle_snapshot,
                    cycle_query=cycle_query_snapshot,
                    filters=filters,
                    lab_snapshot=lab_snapshot,
                    native_targets_snapshot=native_targets_snapshot,
                )
            )
            run_cycles = run_cycles[-self.max_run_cycles :]
            row = self._session_row(
                session_id=clean_id,
                created_at=str(existing.get("created_at", "") or now),
                updated_at=now,
                filters=filters,
                lab_snapshot=lab_snapshot,
                native_targets_snapshot=native_targets_snapshot,
                guidance_snapshot=guidance_snapshot,
                source=str(existing.get("source", "") or ""),
                label=str(existing.get("label", "") or ""),
                replay_candidates=merged_candidates,
                replay_events=list(existing.get("replay_events", [])) if isinstance(existing.get("replay_events", []), list) else [],
                run_cycles=run_cycles,
            )
            self._sessions[clean_id] = row
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
        return {
            "status": "success",
            "session": self._public_row(row),
            "cycle": dict(run_cycles[-1]) if run_cycles else {},
        }

    def _campaign_row(
        self,
        *,
        campaign_id: str,
        created_at: str,
        updated_at: str,
        filters: Dict[str, Any],
        lab_snapshot: Dict[str, Any],
        native_targets_snapshot: Dict[str, Any],
        guidance_snapshot: Dict[str, Any],
        source: str,
        label: str,
        session_ids: List[str],
        app_targets: List[str],
        session_rows: List[Dict[str, Any]],
        sweep_runs: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        clean_session_ids = self._dedupe_strings([str(item).strip() for item in session_ids if str(item).strip()])
        clean_sessions = self._normalize_campaign_sessions(session_rows)
        clean_sweep_runs = self._normalize_campaign_sweep_runs(sweep_runs or [])
        target_apps = self._dedupe_strings(
            [
                *app_targets,
                *(
                    [str(item.get("app_name", "") or "").strip() for item in native_targets_snapshot.get("target_apps", []) if isinstance(item, dict)]
                    if isinstance(native_targets_snapshot.get("target_apps", []), list)
                    else []
                ),
            ]
        )[:16]
        session_target_apps = {
            str(app_name).strip().lower()
            for session in clean_sessions
            for app_name in session.get("target_apps", [])
            if str(app_name).strip()
        }
        pending_session_count = sum(1 for item in clean_sessions if str(item.get("status", "") or "ready").strip().lower() != "complete")
        attention_session_count = sum(1 for item in clean_sessions if str(item.get("status", "") or "").strip().lower() == "attention")
        complete_session_count = sum(1 for item in clean_sessions if str(item.get("status", "") or "").strip().lower() == "complete")
        pending_replay_count = sum(int(item.get("pending_replay_count", 0) or 0) for item in clean_sessions)
        failed_replay_count = sum(int(item.get("failed_replay_count", 0) or 0) for item in clean_sessions)
        completed_replay_count = sum(int(item.get("completed_replay_count", 0) or 0) for item in clean_sessions)
        cycle_count = sum(int(item.get("cycle_count", 0) or 0) for item in clean_sessions)
        regression_cycle_count = sum(int(item.get("regression_cycle_count", 0) or 0) for item in clean_sessions)
        long_horizon_pending_count = sum(int(item.get("long_horizon_pending_count", 0) or 0) for item in clean_sessions)
        latest_sweep = clean_sweep_runs[-1] if clean_sweep_runs else {}
        latest_sweep_status = str(latest_sweep.get("status", "") or "").strip().lower()
        latest_sweep_regression_status = str(latest_sweep.get("regression_status", "") or "").strip().lower()
        trend_summary = self._campaign_trend_summary(clean_sweep_runs, clean_sessions)
        campaign_pressure_score = self._campaign_pressure_score(
            pending_session_count=pending_session_count,
            attention_session_count=attention_session_count,
            pending_app_target_count=sum(
                1
                for app_name in target_apps
                if str(app_name).strip().lower() not in session_target_apps
            ),
            long_horizon_pending_count=long_horizon_pending_count,
            regression_cycle_count=regression_cycle_count,
            regression_sweep_streak=int(trend_summary.get("regression_sweep_streak", 0) or 0),
        )
        campaign_priority = self._campaign_priority(campaign_pressure_score, trend_summary)
        campaign_status = "ready"
        if attention_session_count > 0 or latest_sweep_status in {"error", "failed"} or latest_sweep_regression_status in {"regression", "failed"}:
            campaign_status = "attention"
        elif clean_sessions and pending_session_count == 0:
            campaign_status = "complete"
        label_text = str(label or "").strip() or self._default_campaign_label(
            filters=filters,
            target_apps=target_apps,
            native_targets_snapshot=native_targets_snapshot,
        )
        return {
            "campaign_id": campaign_id,
            "status": campaign_status,
            "label": label_text,
            "source": str(source or "").strip() or "operator_panel",
            "created_at": created_at,
            "updated_at": updated_at,
            "filters": dict(filters),
            "focus_summary": self._dedupe_strings(
                [
                    *(
                        [str(item).strip() for item in native_targets_snapshot.get("focus_summary", [])]
                        if isinstance(native_targets_snapshot.get("focus_summary", []), list)
                        else []
                    ),
                    *(
                        [str(item).strip() for item in guidance_snapshot.get("focus_summary", [])]
                        if isinstance(guidance_snapshot.get("focus_summary", []), list)
                        else []
                    ),
                ]
            )[:8],
            "session_ids": clean_session_ids,
            "sessions": clean_sessions[:8],
            "session_count": len(clean_sessions),
            "pending_session_count": pending_session_count,
            "attention_session_count": attention_session_count,
            "complete_session_count": complete_session_count,
            "pending_replay_count": pending_replay_count,
            "failed_replay_count": failed_replay_count,
            "completed_replay_count": completed_replay_count,
            "cycle_count": cycle_count,
            "regression_cycle_count": regression_cycle_count,
            "long_horizon_pending_count": long_horizon_pending_count,
            "completed_sweep_count": int(trend_summary.get("completed_sweep_count", 0) or 0),
            "regression_sweep_count": int(trend_summary.get("regression_sweep_count", 0) or 0),
            "stable_sweep_streak": int(trend_summary.get("stable_sweep_streak", 0) or 0),
            "regression_sweep_streak": int(trend_summary.get("regression_sweep_streak", 0) or 0),
            "target_app_count": len(target_apps),
            "target_apps": target_apps,
            "app_targets": target_apps,
            "pending_app_target_count": sum(
                1
                for app_name in target_apps
                if str(app_name).strip().lower() not in session_target_apps
            ),
            "sweep_runs": clean_sweep_runs[-self.max_sweep_runs :],
            "sweep_count": len(clean_sweep_runs),
            "latest_sweep_status": latest_sweep_status,
            "latest_sweep_regression_status": latest_sweep_regression_status,
            "latest_sweep_executed_at": str(latest_sweep.get("executed_at", "") or "").strip(),
            "latest_sweep_score": round(float(latest_sweep.get("weighted_score", 0.0) or 0.0), 6),
            "latest_sweep_pass_rate": round(float(latest_sweep.get("weighted_pass_rate", 0.0) or 0.0), 6),
            "latest_sweep_history_direction": str(latest_sweep.get("history_direction", "") or "").strip().lower(),
            "history_direction": str(trend_summary.get("direction", "") or "").strip().lower(),
            "trend_summary": trend_summary,
            "campaign_pressure_score": campaign_pressure_score,
            "campaign_priority": campaign_priority,
            "lab_snapshot": dict(lab_snapshot),
            "native_targets_snapshot": dict(native_targets_snapshot),
            "guidance_snapshot": dict(guidance_snapshot),
        }

    def _program_row(
        self,
        *,
        program_id: str,
        created_at: str,
        updated_at: str,
        filters: Dict[str, Any],
        lab_snapshot: Dict[str, Any],
        native_targets_snapshot: Dict[str, Any],
        guidance_snapshot: Dict[str, Any],
        source: str,
        label: str,
        campaign_ids: List[str],
        app_targets: List[str],
        campaign_rows: List[Dict[str, Any]],
        cycle_runs: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        clean_campaign_ids = self._dedupe_strings([str(item).strip() for item in campaign_ids if str(item).strip()])
        clean_campaigns = self._normalize_program_campaigns(campaign_rows)
        clean_cycle_runs = self._normalize_program_cycle_runs(cycle_runs or [])
        target_apps = self._dedupe_strings(
            [
                *app_targets,
                *(
                    [
                        str(item.get("app_name", "") or "").strip()
                        for item in native_targets_snapshot.get("target_apps", [])
                        if isinstance(item, dict) and str(item.get("app_name", "") or "").strip()
                    ]
                    if isinstance(native_targets_snapshot.get("target_apps", []), list)
                    else []
                ),
            ]
        )[:20]
        represented_apps = {
            str(app_name).strip().lower()
            for campaign in clean_campaigns
            for app_name in campaign.get("target_apps", [])
            if str(app_name).strip()
        }
        pending_campaign_count = sum(
            1 for item in clean_campaigns if str(item.get("status", "") or "ready").strip().lower() != "complete"
        )
        attention_campaign_count = sum(
            1 for item in clean_campaigns if str(item.get("status", "") or "").strip().lower() == "attention"
        )
        complete_campaign_count = sum(
            1 for item in clean_campaigns if str(item.get("status", "") or "").strip().lower() == "complete"
        )
        pending_session_count = sum(int(item.get("pending_session_count", 0) or 0) for item in clean_campaigns)
        attention_session_count = sum(int(item.get("attention_session_count", 0) or 0) for item in clean_campaigns)
        complete_session_count = sum(int(item.get("complete_session_count", 0) or 0) for item in clean_campaigns)
        pending_replay_count = sum(int(item.get("pending_replay_count", 0) or 0) for item in clean_campaigns)
        failed_replay_count = sum(int(item.get("failed_replay_count", 0) or 0) for item in clean_campaigns)
        completed_replay_count = sum(int(item.get("completed_replay_count", 0) or 0) for item in clean_campaigns)
        pending_app_target_count = sum(
            1 for app_name in target_apps if str(app_name).strip().lower() not in represented_apps
        )
        long_horizon_pending_count = sum(
            int(item.get("long_horizon_pending_count", 0) or 0) for item in clean_campaigns
        )
        sweep_count = sum(int(item.get("sweep_count", 0) or 0) for item in clean_campaigns)
        cycle_summary = self._program_trend_summary(clean_cycle_runs, clean_campaigns)
        program_pressure_score = self._program_pressure_score(
            pending_campaign_count=pending_campaign_count,
            attention_campaign_count=attention_campaign_count,
            pending_session_count=pending_session_count,
            attention_session_count=attention_session_count,
            pending_app_target_count=pending_app_target_count,
            long_horizon_pending_count=long_horizon_pending_count,
            regression_cycle_streak=int(cycle_summary.get("regression_cycle_streak", 0) or 0),
        )
        program_priority = self._program_priority(program_pressure_score, cycle_summary)
        latest_cycle = clean_cycle_runs[-1] if clean_cycle_runs else {}
        program_status = "ready"
        if (
            attention_campaign_count > 0
            or str(latest_cycle.get("status", "") or "").strip().lower() in {"error", "failed"}
            or str(latest_cycle.get("trend_direction", "") or "").strip().lower() in {"regressing", "degraded"}
        ):
            program_status = "attention"
        elif clean_campaigns and pending_campaign_count == 0 and pending_app_target_count == 0:
            program_status = "complete"
        label_text = str(label or "").strip() or self._default_program_label(
            filters=filters,
            target_apps=target_apps,
            native_targets_snapshot=native_targets_snapshot,
        )
        return {
            "program_id": program_id,
            "status": program_status,
            "label": label_text,
            "source": str(source or "").strip() or "operator_panel",
            "created_at": created_at,
            "updated_at": updated_at,
            "filters": dict(filters),
            "focus_summary": self._dedupe_strings(
                [
                    *(
                        [str(item).strip() for item in native_targets_snapshot.get("focus_summary", [])]
                        if isinstance(native_targets_snapshot.get("focus_summary", []), list)
                        else []
                    ),
                    *(
                        [str(item).strip() for item in guidance_snapshot.get("focus_summary", [])]
                        if isinstance(guidance_snapshot.get("focus_summary", []), list)
                        else []
                    ),
                ]
            )[:10],
            "campaign_ids": clean_campaign_ids,
            "campaigns": clean_campaigns[:10],
            "campaign_count": len(clean_campaigns),
            "pending_campaign_count": pending_campaign_count,
            "attention_campaign_count": attention_campaign_count,
            "complete_campaign_count": complete_campaign_count,
            "pending_session_count": pending_session_count,
            "attention_session_count": attention_session_count,
            "complete_session_count": complete_session_count,
            "pending_replay_count": pending_replay_count,
            "failed_replay_count": failed_replay_count,
            "completed_replay_count": completed_replay_count,
            "sweep_count": sweep_count,
            "target_app_count": len(target_apps),
            "target_apps": target_apps,
            "app_targets": target_apps,
            "pending_app_target_count": pending_app_target_count,
            "cycle_runs": clean_cycle_runs[-self.max_run_cycles :],
            "cycle_count": len(clean_cycle_runs),
            "completed_cycle_count": int(cycle_summary.get("completed_cycle_count", 0) or 0),
            "stable_cycle_count": int(cycle_summary.get("stable_cycle_count", 0) or 0),
            "regression_cycle_count": int(cycle_summary.get("regression_cycle_count", 0) or 0),
            "stable_cycle_streak": int(cycle_summary.get("stable_cycle_streak", 0) or 0),
            "regression_cycle_streak": int(cycle_summary.get("regression_cycle_streak", 0) or 0),
            "latest_cycle_status": str(latest_cycle.get("status", "") or "").strip().lower(),
            "latest_cycle_stop_reason": str(latest_cycle.get("stop_reason", "") or "").strip().lower(),
            "latest_cycle_executed_at": str(latest_cycle.get("executed_at", "") or "").strip(),
            "latest_cycle_executed_campaign_count": int(latest_cycle.get("executed_campaign_count", 0) or 0),
            "latest_cycle_executed_sweep_count": int(latest_cycle.get("executed_sweep_count", 0) or 0),
            "latest_cycle_weighted_score": round(float(latest_cycle.get("weighted_score", 0.0) or 0.0), 6),
            "latest_cycle_weighted_pass_rate": round(float(latest_cycle.get("weighted_pass_rate", 0.0) or 0.0), 6),
            "latest_cycle_trend_direction": str(latest_cycle.get("trend_direction", "") or "").strip().lower(),
            "history_direction": str(cycle_summary.get("direction", "") or "").strip().lower(),
            "trend_summary": cycle_summary,
            "program_pressure_score": program_pressure_score,
            "program_priority": program_priority,
            "long_horizon_pending_count": long_horizon_pending_count,
            "lab_snapshot": dict(lab_snapshot),
            "native_targets_snapshot": dict(native_targets_snapshot),
            "guidance_snapshot": dict(guidance_snapshot),
        }

    def _portfolio_row(
        self,
        *,
        portfolio_id: str,
        created_at: str,
        updated_at: str,
        filters: Dict[str, Any],
        lab_snapshot: Dict[str, Any],
        native_targets_snapshot: Dict[str, Any],
        guidance_snapshot: Dict[str, Any],
        source: str,
        label: str,
        program_ids: List[str],
        app_targets: List[str],
        program_rows: List[Dict[str, Any]],
        wave_runs: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        clean_program_ids = self._dedupe_strings([str(item).strip() for item in program_ids if str(item).strip()])
        clean_programs = self._normalize_portfolio_programs(program_rows)
        clean_wave_runs = self._normalize_portfolio_wave_runs(wave_runs or [])
        target_apps = self._dedupe_strings(
            [
                *app_targets,
                *(
                    [
                        str(item.get("app_name", "") or "").strip()
                        for item in native_targets_snapshot.get("target_apps", [])
                        if isinstance(item, dict) and str(item.get("app_name", "") or "").strip()
                    ]
                    if isinstance(native_targets_snapshot.get("target_apps", []), list)
                    else []
                ),
            ]
        )[:24]
        represented_apps = {
            str(app_name).strip().lower()
            for program in clean_programs
            for app_name in program.get("target_apps", [])
            if str(app_name).strip()
        }
        pending_program_count = sum(
            1 for item in clean_programs if str(item.get("status", "") or "ready").strip().lower() != "complete"
        )
        attention_program_count = sum(
            1 for item in clean_programs if str(item.get("status", "") or "").strip().lower() == "attention"
        )
        complete_program_count = sum(
            1 for item in clean_programs if str(item.get("status", "") or "").strip().lower() == "complete"
        )
        pending_campaign_count = sum(int(item.get("pending_campaign_count", 0) or 0) for item in clean_programs)
        attention_campaign_count = sum(int(item.get("attention_campaign_count", 0) or 0) for item in clean_programs)
        pending_session_count = sum(int(item.get("pending_session_count", 0) or 0) for item in clean_programs)
        pending_app_target_count = sum(
            1 for app_name in target_apps if str(app_name).strip().lower() not in represented_apps
        )
        long_horizon_pending_count = sum(
            int(item.get("long_horizon_pending_count", 0) or 0) for item in clean_programs
        )
        executed_campaign_total = sum(int(item.get("campaign_count", 0) or 0) for item in clean_programs)
        executed_sweep_total = sum(int(item.get("sweep_count", 0) or 0) for item in clean_programs)
        trend_summary = self._portfolio_trend_summary(clean_wave_runs, clean_programs)
        portfolio_pressure_score = self._portfolio_pressure_score(
            pending_program_count=pending_program_count,
            attention_program_count=attention_program_count,
            pending_campaign_count=pending_campaign_count,
            attention_campaign_count=attention_campaign_count,
            pending_session_count=pending_session_count,
            pending_app_target_count=pending_app_target_count,
            long_horizon_pending_count=long_horizon_pending_count,
            regression_wave_streak=int(trend_summary.get("regression_wave_streak", 0) or 0),
        )
        portfolio_priority = self._portfolio_priority(portfolio_pressure_score, trend_summary)
        latest_wave = clean_wave_runs[-1] if clean_wave_runs else {}
        portfolio_status = "ready"
        if (
            attention_program_count > 0
            or str(latest_wave.get("status", "") or "").strip().lower() in {"error", "failed"}
            or str(latest_wave.get("trend_direction", "") or "").strip().lower() in {"regressing", "degraded"}
        ):
            portfolio_status = "attention"
        elif clean_programs and pending_program_count == 0 and pending_app_target_count == 0:
            portfolio_status = "complete"
        label_text = str(label or "").strip() or self._default_portfolio_label(
            filters=filters,
            target_apps=target_apps,
            native_targets_snapshot=native_targets_snapshot,
        )
        return {
            "portfolio_id": portfolio_id,
            "status": portfolio_status,
            "label": label_text,
            "source": str(source or "").strip() or "operator_panel",
            "created_at": created_at,
            "updated_at": updated_at,
            "filters": dict(filters),
            "focus_summary": self._dedupe_strings(
                [
                    *(
                        [str(item).strip() for item in native_targets_snapshot.get("focus_summary", [])]
                        if isinstance(native_targets_snapshot.get("focus_summary", []), list)
                        else []
                    ),
                    *(
                        [str(item).strip() for item in guidance_snapshot.get("focus_summary", [])]
                        if isinstance(guidance_snapshot.get("focus_summary", []), list)
                        else []
                    ),
                ]
            )[:12],
            "program_ids": clean_program_ids,
            "programs": clean_programs[:12],
            "program_count": len(clean_programs),
            "pending_program_count": pending_program_count,
            "attention_program_count": attention_program_count,
            "complete_program_count": complete_program_count,
            "pending_campaign_count": pending_campaign_count,
            "attention_campaign_count": attention_campaign_count,
            "pending_session_count": pending_session_count,
            "target_app_count": len(target_apps),
            "target_apps": target_apps,
            "app_targets": target_apps,
            "pending_app_target_count": pending_app_target_count,
            "wave_runs": clean_wave_runs[-self.max_run_cycles :],
            "wave_count": len(clean_wave_runs),
            "completed_wave_count": int(trend_summary.get("completed_wave_count", 0) or 0),
            "stable_wave_count": int(trend_summary.get("stable_wave_count", 0) or 0),
            "regression_wave_count": int(trend_summary.get("regression_wave_count", 0) or 0),
            "stable_wave_streak": int(trend_summary.get("stable_wave_streak", 0) or 0),
            "regression_wave_streak": int(trend_summary.get("regression_wave_streak", 0) or 0),
            "latest_wave_status": str(latest_wave.get("status", "") or "").strip().lower(),
            "latest_wave_stop_reason": str(latest_wave.get("stop_reason", "") or "").strip().lower(),
            "latest_wave_executed_at": str(latest_wave.get("executed_at", "") or "").strip(),
            "latest_wave_executed_program_count": int(latest_wave.get("executed_program_count", 0) or 0),
            "latest_wave_executed_campaign_count": int(latest_wave.get("executed_campaign_count", 0) or 0),
            "latest_wave_executed_sweep_count": int(latest_wave.get("executed_sweep_count", 0) or 0),
            "latest_wave_weighted_score": round(float(latest_wave.get("weighted_score", 0.0) or 0.0), 6),
            "latest_wave_weighted_pass_rate": round(float(latest_wave.get("weighted_pass_rate", 0.0) or 0.0), 6),
            "latest_wave_trend_direction": str(latest_wave.get("trend_direction", "") or "").strip().lower(),
            "history_direction": str(trend_summary.get("direction", "") or "").strip().lower(),
            "trend_summary": trend_summary,
            "portfolio_pressure_score": portfolio_pressure_score,
            "portfolio_priority": portfolio_priority,
            "long_horizon_pending_count": long_horizon_pending_count,
            "executed_campaign_total": executed_campaign_total,
            "executed_sweep_total": executed_sweep_total,
            "lab_snapshot": dict(lab_snapshot),
            "native_targets_snapshot": dict(native_targets_snapshot),
            "guidance_snapshot": dict(guidance_snapshot),
        }

    def _session_row(
        self,
        *,
        session_id: str,
        created_at: str,
        updated_at: str,
        filters: Dict[str, Any],
        lab_snapshot: Dict[str, Any],
        native_targets_snapshot: Dict[str, Any],
        guidance_snapshot: Dict[str, Any],
        source: str,
        label: str,
        replay_candidates: List[Dict[str, Any]] | None = None,
        replay_events: List[Dict[str, Any]] | None = None,
        run_cycles: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        clean_candidates = (
            [dict(item) for item in replay_candidates]
            if isinstance(replay_candidates, list)
            else self._normalize_replay_candidates(lab_snapshot.get("replay_candidates", []))
        )
        clean_run_cycles = self._normalize_run_cycles(run_cycles or [])
        pending_replay_count = sum(1 for item in clean_candidates if str(item.get("replay_status", "pending") or "pending").strip().lower() == "pending")
        failed_replay_count = sum(1 for item in clean_candidates if str(item.get("replay_status", "") or "").strip().lower() == "failed")
        completed_replay_count = sum(1 for item in clean_candidates if str(item.get("replay_status", "") or "").strip().lower() == "completed")
        long_horizon_candidate_count = sum(1 for item in clean_candidates if int(item.get("horizon_steps", 1) or 1) >= 4)
        long_horizon_pending_count = sum(
            1
            for item in clean_candidates
            if int(item.get("horizon_steps", 1) or 1) >= 4
            and str(item.get("replay_status", "pending") or "pending").strip().lower() == "pending"
        )
        latest_cycle = clean_run_cycles[-1] if clean_run_cycles else {}
        latest_cycle_status = str(latest_cycle.get("status", "") or "").strip().lower()
        latest_cycle_regression_status = str(latest_cycle.get("regression_status", "") or "").strip().lower()
        completed_cycle_count = sum(
            1 for item in clean_run_cycles if str(item.get("status", "") or "").strip().lower() in {"success", "completed"}
        )
        regression_cycle_count = sum(
            1 for item in clean_run_cycles if str(item.get("regression_status", "") or "").strip().lower() in {"regression", "failed"}
        )
        session_status = "ready"
        if failed_replay_count > 0 or latest_cycle_status in {"error", "failed"} or latest_cycle_regression_status in {"regression", "failed"}:
            session_status = "attention"
        elif pending_replay_count == 0 and (clean_candidates or clean_run_cycles):
            session_status = "complete"
        label_text = str(label or "").strip() or self._default_label(filters=filters, lab_snapshot=lab_snapshot, native_targets_snapshot=native_targets_snapshot)
        target_apps = [
            str(item.get("app_name", "") or "").strip()
            for item in native_targets_snapshot.get("target_apps", [])
            if isinstance(item, dict) and str(item.get("app_name", "") or "").strip()
        ] if isinstance(native_targets_snapshot.get("target_apps", []), list) else []
        return {
            "session_id": session_id,
            "status": session_status,
            "label": label_text,
            "source": str(source or "").strip() or "operator_panel",
            "created_at": created_at,
            "updated_at": updated_at,
            "filters": dict(filters),
            "focus_summary": self._dedupe_strings(
                [
                    *(
                        [str(item).strip() for item in native_targets_snapshot.get("focus_summary", [])]
                        if isinstance(native_targets_snapshot.get("focus_summary", []), list)
                        else []
                    ),
                    *(
                        [str(item).strip() for item in guidance_snapshot.get("focus_summary", [])]
                        if isinstance(guidance_snapshot.get("focus_summary", []), list)
                        else []
                    ),
                ]
            )[:8],
            "replay_candidates": clean_candidates[:12],
            "replay_events": [
                dict(item)
                for item in (replay_events or [])
                if isinstance(item, dict)
            ][-self.max_replay_events :],
            "replay_candidate_count": len(clean_candidates),
            "pending_replay_count": pending_replay_count,
            "failed_replay_count": failed_replay_count,
            "completed_replay_count": completed_replay_count,
            "long_horizon_candidate_count": long_horizon_candidate_count,
            "long_horizon_pending_count": long_horizon_pending_count,
            "run_cycles": clean_run_cycles[-self.max_run_cycles :],
            "cycle_count": len(clean_run_cycles),
            "completed_cycle_count": completed_cycle_count,
            "regression_cycle_count": regression_cycle_count,
            "latest_cycle_status": latest_cycle_status,
            "latest_cycle_regression_status": latest_cycle_regression_status,
            "latest_cycle_score": round(float(latest_cycle.get("weighted_score", 0.0) or 0.0), 6),
            "latest_cycle_pass_rate": round(float(latest_cycle.get("weighted_pass_rate", 0.0) or 0.0), 6),
            "latest_cycle_executed_at": str(latest_cycle.get("executed_at", "") or "").strip(),
            "target_app_count": len(target_apps),
            "target_apps": target_apps[:12],
            "strongest_tactics": dict(native_targets_snapshot.get("strongest_tactics", {})) if isinstance(native_targets_snapshot.get("strongest_tactics", {}), dict) else {},
            "coverage_gap_apps": [
                str(item).strip()
                for item in native_targets_snapshot.get("coverage_gap_apps", [])
                if str(item).strip()
            ][:12] if isinstance(native_targets_snapshot.get("coverage_gap_apps", []), list) else [],
            "history_direction": str(dict(lab_snapshot.get("history_trend", {})).get("direction", "") or "").strip(),
            "history_run_count": self._coerce_int(dict(lab_snapshot.get("history_trend", {})).get("run_count", 0), minimum=0, maximum=100_000, default=0)
            if isinstance(lab_snapshot.get("history_trend", {}), dict)
            else 0,
            "latest_run_executed_at": str(dict(lab_snapshot.get("latest_run", {})).get("executed_at", "") or "").strip()
            if isinstance(lab_snapshot.get("latest_run", {}), dict)
            else "",
            "latest_weighted_score": round(
                float(dict(lab_snapshot.get("latest_summary", {})).get("weighted_score", 0.0) or 0.0)
                if isinstance(lab_snapshot.get("latest_summary", {}), dict)
                else 0.0,
                6,
            ),
            "latest_weighted_pass_rate": round(
                float(dict(lab_snapshot.get("latest_summary", {})).get("weighted_pass_rate", 0.0) or 0.0)
                if isinstance(lab_snapshot.get("latest_summary", {}), dict)
                else 0.0,
                6,
            ),
            "catalog_summary": dict(lab_snapshot.get("catalog_summary", {})) if isinstance(lab_snapshot.get("catalog_summary", {}), dict) else {},
            "coverage": dict(lab_snapshot.get("coverage", {})) if isinstance(lab_snapshot.get("coverage", {}), dict) else {},
            "history_trend": dict(lab_snapshot.get("history_trend", {})) if isinstance(lab_snapshot.get("history_trend", {}), dict) else {},
            "lab_snapshot": lab_snapshot,
            "native_targets_snapshot": native_targets_snapshot,
            "guidance_snapshot": guidance_snapshot,
        }

    def _normalize_replay_candidates(self, candidates: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not isinstance(candidates, list):
            return rows
        for item in candidates:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "scenario": str(item.get("scenario", "") or "").strip(),
                    "user_text": str(item.get("user_text", "") or "").strip(),
                    "pack": str(item.get("pack", "") or "").strip(),
                    "category": str(item.get("category", "") or "").strip(),
                    "mission_family": str(item.get("mission_family", "") or "").strip(),
                    "risk_level": str(item.get("risk_level", "") or "").strip(),
                    "apps": [str(app).strip() for app in item.get("apps", []) if str(app).strip()] if isinstance(item.get("apps", []), list) else [],
                    "capabilities": [str(cap).strip() for cap in item.get("capabilities", []) if str(cap).strip()] if isinstance(item.get("capabilities", []), list) else [],
                    "score": round(float(item.get("score", 0.0) or 0.0), 6),
                    "weight": round(float(item.get("weight", 0.0) or 0.0), 6),
                    "replayable": bool(item.get("replayable", True)),
                    "horizon_steps": self._coerce_int(item.get("horizon_steps", 1), minimum=1, maximum=100_000, default=1),
                    "reasons": self._dedupe_strings([str(reason).strip() for reason in item.get("reasons", []) if str(reason).strip()])[:8] if isinstance(item.get("reasons", []), list) else [],
                    "replay_query": dict(item.get("replay_query", {})) if isinstance(item.get("replay_query", {}), dict) else {},
                    "replay_status": str(item.get("replay_status", "pending") or "pending").strip().lower() or "pending",
                    "replay_count": self._coerce_int(item.get("replay_count", 0), minimum=0, maximum=100_000, default=0),
                    "last_replayed_at": str(item.get("last_replayed_at", "") or "").strip(),
                    "last_result_status": str(item.get("last_result_status", "") or "").strip(),
                    "last_regression_status": str(item.get("last_regression_status", "") or "").strip(),
                    "last_weighted_score": round(float(item.get("last_weighted_score", 0.0) or 0.0), 6),
                    "last_weighted_pass_rate": round(float(item.get("last_weighted_pass_rate", 0.0) or 0.0), 6),
                }
            )
        return rows[:24]

    def _merge_replay_candidates(
        self,
        existing_candidates: List[Dict[str, Any]] | Any,
        latest_candidates: List[Dict[str, Any]] | Any,
    ) -> List[Dict[str, Any]]:
        existing_rows = self._normalize_replay_candidates(existing_candidates)
        latest_rows = self._normalize_replay_candidates(latest_candidates)
        existing_by_scenario = {
            str(item.get("scenario", "") or "").strip(): dict(item)
            for item in existing_rows
            if str(item.get("scenario", "") or "").strip()
        }
        merged: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for item in latest_rows:
            scenario_name = str(item.get("scenario", "") or "").strip()
            current = dict(item)
            preserved = existing_by_scenario.get(scenario_name)
            if preserved:
                for key in (
                    "replay_status",
                    "replay_count",
                    "last_replayed_at",
                    "last_result_status",
                    "last_regression_status",
                    "last_weighted_score",
                    "last_weighted_pass_rate",
                ):
                    current[key] = preserved.get(key, current.get(key))
                if not current.get("replay_query") and isinstance(preserved.get("replay_query", {}), dict):
                    current["replay_query"] = dict(preserved.get("replay_query", {}))
                current["reasons"] = self._dedupe_strings(
                    [
                        *(
                            [str(reason).strip() for reason in preserved.get("reasons", [])]
                            if isinstance(preserved.get("reasons", []), list)
                            else []
                        ),
                        *(
                            [str(reason).strip() for reason in current.get("reasons", [])]
                            if isinstance(current.get("reasons", []), list)
                            else []
                        ),
                    ]
                )[:8]
            merged.append(current)
            if scenario_name:
                seen.add(scenario_name)
        for item in existing_rows:
            scenario_name = str(item.get("scenario", "") or "").strip()
            if scenario_name and scenario_name in seen:
                continue
            merged.append(dict(item))
        return merged[:24]

    def _normalize_run_cycles(self, cycles: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not isinstance(cycles, list):
            return rows
        for item in cycles:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "cycle_id": str(item.get("cycle_id", "") or "").strip() or f"cycle-{uuid.uuid4().hex[:10]}",
                    "kind": str(item.get("kind", "") or "run_cycle").strip().lower() or "run_cycle",
                    "recorded_at": str(item.get("recorded_at", "") or "").strip(),
                    "executed_at": str(item.get("executed_at", "") or "").strip(),
                    "status": str(item.get("status", "") or "success").strip().lower() or "success",
                    "regression_status": str(item.get("regression_status", "") or "").strip().lower(),
                    "weighted_score": round(float(item.get("weighted_score", 0.0) or 0.0), 6),
                    "weighted_pass_rate": round(float(item.get("weighted_pass_rate", 0.0) or 0.0), 6),
                    "scenario_count": self._coerce_int(item.get("scenario_count", 0), minimum=0, maximum=100_000, default=0),
                    "history_direction": str(item.get("history_direction", "") or "").strip().lower(),
                    "replay_candidate_count": self._coerce_int(item.get("replay_candidate_count", 0), minimum=0, maximum=100_000, default=0),
                    "long_horizon_count": self._coerce_int(item.get("long_horizon_count", 0), minimum=0, maximum=100_000, default=0),
                    "target_app_count": self._coerce_int(item.get("target_app_count", 0), minimum=0, maximum=100_000, default=0),
                    "query": dict(item.get("query", {})) if isinstance(item.get("query", {}), dict) else {},
                }
            )
        return rows[-self.max_run_cycles :]

    def _seed_run_cycles(
        self,
        *,
        recorded_at: str,
        filters: Dict[str, Any],
        lab_snapshot: Dict[str, Any],
        native_targets_snapshot: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        if not isinstance(lab_snapshot, dict) or (
            not isinstance(lab_snapshot.get("latest_run", {}), dict)
            and not isinstance(lab_snapshot.get("latest_summary", {}), dict)
        ):
            return []
        return [
            self._cycle_row(
                recorded_at=recorded_at,
                cycle_payload=dict(lab_snapshot.get("latest_run", {})) if isinstance(lab_snapshot.get("latest_run", {}), dict) else {},
                cycle_query=dict(filters),
                filters=filters,
                lab_snapshot=lab_snapshot,
                native_targets_snapshot=native_targets_snapshot,
                kind="seed",
            )
        ]

    def _cycle_row(
        self,
        *,
        recorded_at: str,
        cycle_payload: Dict[str, Any],
        cycle_query: Dict[str, Any],
        filters: Dict[str, Any],
        lab_snapshot: Dict[str, Any],
        native_targets_snapshot: Dict[str, Any],
        kind: str = "run_cycle",
    ) -> Dict[str, Any]:
        summary = dict(cycle_payload.get("summary", {})) if isinstance(cycle_payload.get("summary", {}), dict) else {}
        regression = dict(cycle_payload.get("regression", {})) if isinstance(cycle_payload.get("regression", {}), dict) else {}
        latest_summary = dict(lab_snapshot.get("latest_summary", {})) if isinstance(lab_snapshot.get("latest_summary", {}), dict) else {}
        latest_regression = dict(lab_snapshot.get("latest_regression", {})) if isinstance(lab_snapshot.get("latest_regression", {}), dict) else {}
        history_trend = dict(lab_snapshot.get("history_trend", {})) if isinstance(lab_snapshot.get("history_trend", {}), dict) else {}
        coverage = dict(lab_snapshot.get("coverage", {})) if isinstance(lab_snapshot.get("coverage", {}), dict) else {}
        long_horizon = dict(coverage.get("long_horizon", {})) if isinstance(coverage.get("long_horizon", {}), dict) else {}
        target_apps = native_targets_snapshot.get("target_apps", []) if isinstance(native_targets_snapshot.get("target_apps", []), list) else []
        return {
            "cycle_id": f"cycle-{uuid.uuid4().hex[:10]}",
            "kind": str(kind or "run_cycle").strip().lower() or "run_cycle",
            "recorded_at": recorded_at,
            "executed_at": str(cycle_payload.get("executed_at", "") or dict(lab_snapshot.get("latest_run", {})).get("executed_at", "") or recorded_at).strip(),
            "status": str(cycle_payload.get("status", "") or "success").strip().lower() or "success",
            "regression_status": str(regression.get("status", "") or latest_regression.get("status", "") or "").strip().lower(),
            "weighted_score": round(float(summary.get("weighted_score", latest_summary.get("weighted_score", 0.0)) or 0.0), 6),
            "weighted_pass_rate": round(float(summary.get("weighted_pass_rate", latest_summary.get("weighted_pass_rate", 0.0)) or 0.0), 6),
            "scenario_count": self._coerce_int(
                cycle_payload.get(
                    "scenario_count",
                    summary.get(
                        "count",
                        dict(lab_snapshot.get("catalog_summary", {})).get("scenario_count", 0),
                    ),
                ),
                minimum=0,
                maximum=100_000,
                default=0,
            ),
            "history_direction": str(history_trend.get("direction", "") or "").strip().lower(),
            "replay_candidate_count": len(
                self._normalize_replay_candidates(lab_snapshot.get("replay_candidates", []))
            ),
            "long_horizon_count": self._coerce_int(long_horizon.get("count", 0), minimum=0, maximum=100_000, default=0),
            "target_app_count": sum(
                1 for item in target_apps if isinstance(item, dict) and str(item.get("app_name", "") or "").strip()
            ),
            "query": dict(cycle_query) if isinstance(cycle_query, dict) else dict(filters),
        }

    @staticmethod
    def _replay_status_from_payload(payload: Dict[str, Any]) -> str:
        items = payload.get("items", [])
        if isinstance(items, list) and items:
            if all(bool(item.get("passed", False)) for item in items if isinstance(item, dict)):
                return "completed"
            return "failed"
        status = str(payload.get("status", "") or "").strip().lower()
        return "completed" if status == "success" else "failed"

    @staticmethod
    def _default_label(
        *,
        filters: Dict[str, Any],
        lab_snapshot: Dict[str, Any],
        native_targets_snapshot: Dict[str, Any],
    ) -> str:
        pack = str(filters.get("pack", "") or "").strip()
        category = str(filters.get("category", "") or "").strip()
        app_name = str(filters.get("app", "") or filters.get("app_name", "") or "").strip()
        if pack:
            return f"{pack} benchmark lab"
        if category:
            return f"{category} benchmark lab"
        if app_name:
            return f"{app_name} benchmark lab"
        target_apps = native_targets_snapshot.get("target_apps", [])
        if isinstance(target_apps, list):
            for item in target_apps:
                if isinstance(item, dict):
                    name = str(item.get("app_name", "") or "").strip()
                    if name:
                        return f"{name} benchmark lab"
        coverage = dict(lab_snapshot.get("coverage", {})) if isinstance(lab_snapshot.get("coverage", {}), dict) else {}
        if coverage:
            return "desktop benchmark lab"
        return "benchmark lab session"

    @staticmethod
    def _default_campaign_label(
        *,
        filters: Dict[str, Any],
        target_apps: List[str],
        native_targets_snapshot: Dict[str, Any],
    ) -> str:
        pack = str(filters.get("pack", "") or "").strip()
        category = str(filters.get("category", "") or "").strip()
        app_name = str(filters.get("app", "") or filters.get("app_name", "") or "").strip()
        if pack:
            return f"{pack} replay campaign"
        if category:
            return f"{category} replay campaign"
        if app_name:
            return f"{app_name} replay campaign"
        if target_apps:
            return f"{target_apps[0]} replay campaign"
        target_rows = native_targets_snapshot.get("target_apps", []) if isinstance(native_targets_snapshot.get("target_apps", []), list) else []
        for item in target_rows:
            if isinstance(item, dict):
                name = str(item.get("app_name", "") or "").strip()
                if name:
                    return f"{name} replay campaign"
        return "benchmark replay campaign"

    @staticmethod
    def _default_program_label(
        *,
        filters: Dict[str, Any],
        target_apps: List[str],
        native_targets_snapshot: Dict[str, Any],
    ) -> str:
        pack = str(filters.get("pack", "") or "").strip()
        category = str(filters.get("category", "") or "").strip()
        app_name = str(filters.get("app", "") or filters.get("app_name", "") or "").strip()
        if pack:
            return f"{pack} replay program"
        if category:
            return f"{category} replay program"
        if app_name:
            return f"{app_name} replay program"
        if target_apps:
            return f"{target_apps[0]} replay program"
        target_rows = native_targets_snapshot.get("target_apps", []) if isinstance(native_targets_snapshot.get("target_apps", []), list) else []
        for item in target_rows:
            if isinstance(item, dict):
                name = str(item.get("app_name", "") or "").strip()
                if name:
                    return f"{name} replay program"
        return "desktop replay program"

    @staticmethod
    def _default_portfolio_label(
        *,
        filters: Dict[str, Any],
        target_apps: List[str],
        native_targets_snapshot: Dict[str, Any],
    ) -> str:
        pack = str(filters.get("pack", "") or "").strip()
        category = str(filters.get("category", "") or "").strip()
        app_name = str(filters.get("app", "") or filters.get("app_name", "") or "").strip()
        if pack:
            return f"{pack} replay portfolio"
        if category:
            return f"{category} replay portfolio"
        if app_name:
            return f"{app_name} replay portfolio"
        if target_apps:
            return f"{target_apps[0]} replay portfolio"
        target_rows = native_targets_snapshot.get("target_apps", []) if isinstance(native_targets_snapshot.get("target_apps", []), list) else []
        for item in target_rows:
            if isinstance(item, dict):
                name = str(item.get("app_name", "") or "").strip()
                if name:
                    return f"{name} replay portfolio"
        return "desktop replay portfolio"

    def _program_cycle_row(
        self,
        *,
        recorded_at: str,
        cycle_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "cycle_id": str(cycle_payload.get("cycle_id", "") or "").strip() or f"program-cycle-{uuid.uuid4().hex[:10]}",
            "recorded_at": recorded_at,
            "executed_at": str(cycle_payload.get("executed_at", "") or recorded_at).strip(),
            "status": str(cycle_payload.get("status", "") or "success").strip().lower() or "success",
            "stop_reason": str(cycle_payload.get("stop_reason", "") or "").strip().lower(),
            "executed_campaign_count": self._coerce_int(
                cycle_payload.get("executed_campaign_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "created_campaign_count": self._coerce_int(
                cycle_payload.get("created_campaign_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "executed_sweep_count": self._coerce_int(
                cycle_payload.get("executed_sweep_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "stable_campaign_count": self._coerce_int(
                cycle_payload.get("stable_campaign_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "regression_campaign_count": self._coerce_int(
                cycle_payload.get("regression_campaign_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "pending_session_count": self._coerce_int(
                cycle_payload.get("pending_session_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "attention_session_count": self._coerce_int(
                cycle_payload.get("attention_session_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "pending_app_target_count": self._coerce_int(
                cycle_payload.get("pending_app_target_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "long_horizon_pending_count": self._coerce_int(
                cycle_payload.get("long_horizon_pending_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "weighted_score": round(float(cycle_payload.get("weighted_score", 0.0) or 0.0), 6),
            "weighted_pass_rate": round(float(cycle_payload.get("weighted_pass_rate", 0.0) or 0.0), 6),
            "trend_direction": str(cycle_payload.get("trend_direction", "") or "").strip().lower(),
            "query": dict(cycle_payload.get("query", {})) if isinstance(cycle_payload.get("query", {}), dict) else {},
        }

    def _portfolio_wave_row(
        self,
        *,
        recorded_at: str,
        wave_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "wave_id": str(wave_payload.get("wave_id", "") or "").strip() or f"portfolio-wave-{uuid.uuid4().hex[:10]}",
            "recorded_at": recorded_at,
            "executed_at": str(wave_payload.get("executed_at", "") or recorded_at).strip(),
            "status": str(wave_payload.get("status", "") or "success").strip().lower() or "success",
            "stop_reason": str(wave_payload.get("stop_reason", "") or "").strip().lower(),
            "executed_program_count": self._coerce_int(
                wave_payload.get("executed_program_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "created_program_count": self._coerce_int(
                wave_payload.get("created_program_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "executed_campaign_count": self._coerce_int(
                wave_payload.get("executed_campaign_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "executed_sweep_count": self._coerce_int(
                wave_payload.get("executed_sweep_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "stable_program_count": self._coerce_int(
                wave_payload.get("stable_program_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "regression_program_count": self._coerce_int(
                wave_payload.get("regression_program_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "pending_campaign_count": self._coerce_int(
                wave_payload.get("pending_campaign_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "attention_campaign_count": self._coerce_int(
                wave_payload.get("attention_campaign_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "pending_session_count": self._coerce_int(
                wave_payload.get("pending_session_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "pending_app_target_count": self._coerce_int(
                wave_payload.get("pending_app_target_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "long_horizon_pending_count": self._coerce_int(
                wave_payload.get("long_horizon_pending_count", 0), minimum=0, maximum=100_000, default=0
            ),
            "weighted_score": round(float(wave_payload.get("weighted_score", 0.0) or 0.0), 6),
            "weighted_pass_rate": round(float(wave_payload.get("weighted_pass_rate", 0.0) or 0.0), 6),
            "trend_direction": str(wave_payload.get("trend_direction", "") or "").strip().lower(),
            "query": dict(wave_payload.get("query", {})) if isinstance(wave_payload.get("query", {}), dict) else {},
        }

    def _normalize_program_campaigns(self, campaigns: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not isinstance(campaigns, list):
            return rows
        for item in campaigns:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "campaign_id": str(item.get("campaign_id", "") or "").strip(),
                    "label": str(item.get("label", "") or "").strip(),
                    "status": str(item.get("status", "") or "ready").strip().lower() or "ready",
                    "session_count": self._coerce_int(item.get("session_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_session_count": self._coerce_int(item.get("pending_session_count", 0), minimum=0, maximum=100_000, default=0),
                    "attention_session_count": self._coerce_int(item.get("attention_session_count", 0), minimum=0, maximum=100_000, default=0),
                    "complete_session_count": self._coerce_int(item.get("complete_session_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_replay_count": self._coerce_int(item.get("pending_replay_count", 0), minimum=0, maximum=100_000, default=0),
                    "failed_replay_count": self._coerce_int(item.get("failed_replay_count", 0), minimum=0, maximum=100_000, default=0),
                    "completed_replay_count": self._coerce_int(item.get("completed_replay_count", 0), minimum=0, maximum=100_000, default=0),
                    "cycle_count": self._coerce_int(item.get("cycle_count", 0), minimum=0, maximum=100_000, default=0),
                    "regression_cycle_count": self._coerce_int(item.get("regression_cycle_count", 0), minimum=0, maximum=100_000, default=0),
                    "long_horizon_pending_count": self._coerce_int(item.get("long_horizon_pending_count", 0), minimum=0, maximum=100_000, default=0),
                    "sweep_count": self._coerce_int(item.get("sweep_count", 0), minimum=0, maximum=100_000, default=0),
                    "target_app_count": self._coerce_int(item.get("target_app_count", 0), minimum=0, maximum=100_000, default=0),
                    "target_apps": [
                        str(app_name).strip()
                        for app_name in item.get("target_apps", [])
                        if str(app_name).strip()
                    ][:8] if isinstance(item.get("target_apps", []), list) else [],
                    "pending_app_target_count": self._coerce_int(item.get("pending_app_target_count", 0), minimum=0, maximum=100_000, default=0),
                    "latest_sweep_status": str(item.get("latest_sweep_status", "") or "").strip().lower(),
                    "latest_sweep_regression_status": str(item.get("latest_sweep_regression_status", "") or "").strip().lower(),
                    "history_direction": str(item.get("history_direction", "") or "").strip().lower(),
                    "campaign_pressure_score": round(float(item.get("campaign_pressure_score", 0.0) or 0.0), 6),
                    "campaign_priority": str(item.get("campaign_priority", "") or "").strip().lower(),
                }
            )
        return rows[:16]

    def _normalize_portfolio_programs(self, programs: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not isinstance(programs, list):
            return rows
        for item in programs:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "program_id": str(item.get("program_id", "") or "").strip(),
                    "label": str(item.get("label", "") or "").strip(),
                    "status": str(item.get("status", "") or "ready").strip().lower() or "ready",
                    "campaign_count": self._coerce_int(item.get("campaign_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_campaign_count": self._coerce_int(item.get("pending_campaign_count", 0), minimum=0, maximum=100_000, default=0),
                    "attention_campaign_count": self._coerce_int(item.get("attention_campaign_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_session_count": self._coerce_int(item.get("pending_session_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_replay_count": self._coerce_int(item.get("pending_replay_count", 0), minimum=0, maximum=100_000, default=0),
                    "failed_replay_count": self._coerce_int(item.get("failed_replay_count", 0), minimum=0, maximum=100_000, default=0),
                    "completed_replay_count": self._coerce_int(item.get("completed_replay_count", 0), minimum=0, maximum=100_000, default=0),
                    "sweep_count": self._coerce_int(item.get("sweep_count", 0), minimum=0, maximum=100_000, default=0),
                    "cycle_count": self._coerce_int(item.get("cycle_count", 0), minimum=0, maximum=100_000, default=0),
                    "long_horizon_pending_count": self._coerce_int(item.get("long_horizon_pending_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_app_target_count": self._coerce_int(item.get("pending_app_target_count", 0), minimum=0, maximum=100_000, default=0),
                    "target_app_count": self._coerce_int(item.get("target_app_count", 0), minimum=0, maximum=100_000, default=0),
                    "target_apps": [
                        str(app_name).strip()
                        for app_name in item.get("target_apps", [])
                        if str(app_name).strip()
                    ][:10] if isinstance(item.get("target_apps", []), list) else [],
                    "latest_cycle_status": str(item.get("latest_cycle_status", "") or "").strip().lower(),
                    "latest_cycle_stop_reason": str(item.get("latest_cycle_stop_reason", "") or "").strip().lower(),
                    "history_direction": str(item.get("history_direction", "") or "").strip().lower(),
                    "program_pressure_score": round(float(item.get("program_pressure_score", 0.0) or 0.0), 6),
                    "program_priority": str(item.get("program_priority", "") or "").strip().lower(),
                }
            )
        return rows[:20]

    def _normalize_program_cycle_runs(self, cycles: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not isinstance(cycles, list):
            return rows
        for item in cycles:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "cycle_id": str(item.get("cycle_id", "") or "").strip() or f"program-cycle-{uuid.uuid4().hex[:10]}",
                    "recorded_at": str(item.get("recorded_at", "") or "").strip(),
                    "executed_at": str(item.get("executed_at", "") or "").strip(),
                    "status": str(item.get("status", "") or "success").strip().lower() or "success",
                    "stop_reason": str(item.get("stop_reason", "") or "").strip().lower(),
                    "executed_campaign_count": self._coerce_int(item.get("executed_campaign_count", 0), minimum=0, maximum=100_000, default=0),
                    "created_campaign_count": self._coerce_int(item.get("created_campaign_count", 0), minimum=0, maximum=100_000, default=0),
                    "executed_sweep_count": self._coerce_int(item.get("executed_sweep_count", 0), minimum=0, maximum=100_000, default=0),
                    "stable_campaign_count": self._coerce_int(item.get("stable_campaign_count", 0), minimum=0, maximum=100_000, default=0),
                    "regression_campaign_count": self._coerce_int(item.get("regression_campaign_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_session_count": self._coerce_int(item.get("pending_session_count", 0), minimum=0, maximum=100_000, default=0),
                    "attention_session_count": self._coerce_int(item.get("attention_session_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_app_target_count": self._coerce_int(item.get("pending_app_target_count", 0), minimum=0, maximum=100_000, default=0),
                    "long_horizon_pending_count": self._coerce_int(item.get("long_horizon_pending_count", 0), minimum=0, maximum=100_000, default=0),
                    "weighted_score": round(float(item.get("weighted_score", 0.0) or 0.0), 6),
                    "weighted_pass_rate": round(float(item.get("weighted_pass_rate", 0.0) or 0.0), 6),
                    "trend_direction": str(item.get("trend_direction", "") or "").strip().lower(),
                    "query": dict(item.get("query", {})) if isinstance(item.get("query", {}), dict) else {},
                }
            )
        return rows[-self.max_run_cycles :]

    def _normalize_portfolio_wave_runs(self, waves: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not isinstance(waves, list):
            return rows
        for item in waves:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "wave_id": str(item.get("wave_id", "") or "").strip() or f"portfolio-wave-{uuid.uuid4().hex[:10]}",
                    "recorded_at": str(item.get("recorded_at", "") or "").strip(),
                    "executed_at": str(item.get("executed_at", "") or "").strip(),
                    "status": str(item.get("status", "") or "success").strip().lower() or "success",
                    "stop_reason": str(item.get("stop_reason", "") or "").strip().lower(),
                    "executed_program_count": self._coerce_int(item.get("executed_program_count", 0), minimum=0, maximum=100_000, default=0),
                    "created_program_count": self._coerce_int(item.get("created_program_count", 0), minimum=0, maximum=100_000, default=0),
                    "executed_campaign_count": self._coerce_int(item.get("executed_campaign_count", 0), minimum=0, maximum=100_000, default=0),
                    "executed_sweep_count": self._coerce_int(item.get("executed_sweep_count", 0), minimum=0, maximum=100_000, default=0),
                    "stable_program_count": self._coerce_int(item.get("stable_program_count", 0), minimum=0, maximum=100_000, default=0),
                    "regression_program_count": self._coerce_int(item.get("regression_program_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_campaign_count": self._coerce_int(item.get("pending_campaign_count", 0), minimum=0, maximum=100_000, default=0),
                    "attention_campaign_count": self._coerce_int(item.get("attention_campaign_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_session_count": self._coerce_int(item.get("pending_session_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_app_target_count": self._coerce_int(item.get("pending_app_target_count", 0), minimum=0, maximum=100_000, default=0),
                    "long_horizon_pending_count": self._coerce_int(item.get("long_horizon_pending_count", 0), minimum=0, maximum=100_000, default=0),
                    "weighted_score": round(float(item.get("weighted_score", 0.0) or 0.0), 6),
                    "weighted_pass_rate": round(float(item.get("weighted_pass_rate", 0.0) or 0.0), 6),
                    "trend_direction": str(item.get("trend_direction", "") or "").strip().lower(),
                    "query": dict(item.get("query", {})) if isinstance(item.get("query", {}), dict) else {},
                }
            )
        return rows[-self.max_run_cycles :]

    @staticmethod
    def _program_trend_summary(
        cycle_runs: List[Dict[str, Any]],
        campaign_rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        stable_streak = 0
        regression_streak = 0
        for item in reversed(cycle_runs):
            trend = str(item.get("trend_direction", "") or "").strip().lower()
            status = str(item.get("status", "") or "").strip().lower()
            if trend in {"regressing", "degraded"} or status in {"error", "failed"}:
                regression_streak += 1
                break
            if trend in {"stable", "improving", "warming"} and status in {"success", "completed"}:
                stable_streak += 1
            else:
                break
        for item in reversed(cycle_runs):
            trend = str(item.get("trend_direction", "") or "").strip().lower()
            status = str(item.get("status", "") or "").strip().lower()
            if trend in {"regressing", "degraded"} or status in {"error", "failed"}:
                regression_streak += 1
            else:
                break
        latest = cycle_runs[-1] if cycle_runs else {}
        direction = str(latest.get("trend_direction", "") or "").strip().lower()
        if not direction:
            attention_campaigns = sum(
                1 for row in campaign_rows if str(row.get("status", "") or "").strip().lower() == "attention"
            )
            pending_campaigns = sum(
                1 for row in campaign_rows if str(row.get("status", "") or "").strip().lower() != "complete"
            )
            if attention_campaigns > 0:
                direction = "regressing"
            elif pending_campaigns > 0:
                direction = "warming"
            else:
                direction = "stable"
        return {
            "direction": direction,
            "completed_cycle_count": sum(
                1 for item in cycle_runs if str(item.get("status", "") or "").strip().lower() in {"success", "completed"}
            ),
            "stable_cycle_count": sum(
                1
                for item in cycle_runs
                if str(item.get("trend_direction", "") or "").strip().lower() in {"stable", "improving"}
            ),
            "regression_cycle_count": sum(
                1
                for item in cycle_runs
                if str(item.get("trend_direction", "") or "").strip().lower() in {"regressing", "degraded"}
                or str(item.get("status", "") or "").strip().lower() in {"error", "failed"}
            ),
            "stable_cycle_streak": stable_streak,
            "regression_cycle_streak": regression_streak,
            "executed_campaign_total": sum(int(item.get("executed_campaign_count", 0) or 0) for item in cycle_runs),
            "executed_sweep_total": sum(int(item.get("executed_sweep_count", 0) or 0) for item in cycle_runs),
            "stable_campaign_total": sum(int(item.get("stable_campaign_count", 0) or 0) for item in cycle_runs),
            "regression_campaign_total": sum(int(item.get("regression_campaign_count", 0) or 0) for item in cycle_runs),
        }

    @staticmethod
    def _program_pressure_score(
        *,
        pending_campaign_count: int,
        attention_campaign_count: int,
        pending_session_count: int,
        attention_session_count: int,
        pending_app_target_count: int,
        long_horizon_pending_count: int,
        regression_cycle_streak: int,
    ) -> float:
        return round(
            float(
                (attention_campaign_count * 2.4)
                + (pending_campaign_count * 1.3)
                + (attention_session_count * 1.15)
                + (pending_session_count * 0.55)
                + (pending_app_target_count * 1.35)
                + (long_horizon_pending_count * 0.28)
                + (regression_cycle_streak * 1.9)
            ),
            6,
        )

    @staticmethod
    def _program_priority(program_pressure_score: float, trend_summary: Dict[str, Any]) -> str:
        direction = str(trend_summary.get("direction", "") or "").strip().lower()
        regression_streak = int(trend_summary.get("regression_cycle_streak", 0) or 0)
        if program_pressure_score >= 18.0 or regression_streak >= 2 or direction == "regressing":
            return "critical"
        if program_pressure_score >= 10.0 or direction in {"warming", "volatile"}:
            return "elevated"
        if program_pressure_score >= 4.0:
            return "active"
        return "steady"

    @staticmethod
    def _portfolio_trend_summary(
        wave_runs: List[Dict[str, Any]],
        program_rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        stable_streak = 0
        regression_streak = 0
        for item in reversed(wave_runs):
            trend = str(item.get("trend_direction", "") or "").strip().lower()
            status = str(item.get("status", "") or "").strip().lower()
            if trend in {"regressing", "degraded"} or status in {"error", "failed"}:
                regression_streak += 1
                break
            if trend in {"stable", "improving", "warming"} and status in {"success", "completed"}:
                stable_streak += 1
            else:
                break
        for item in reversed(wave_runs):
            trend = str(item.get("trend_direction", "") or "").strip().lower()
            status = str(item.get("status", "") or "").strip().lower()
            if trend in {"regressing", "degraded"} or status in {"error", "failed"}:
                regression_streak += 1
            else:
                break
        latest = wave_runs[-1] if wave_runs else {}
        direction = str(latest.get("trend_direction", "") or "").strip().lower()
        if not direction:
            attention_programs = sum(
                1 for row in program_rows if str(row.get("status", "") or "").strip().lower() == "attention"
            )
            pending_programs = sum(
                1 for row in program_rows if str(row.get("status", "") or "").strip().lower() != "complete"
            )
            if attention_programs > 0:
                direction = "regressing"
            elif pending_programs > 0:
                direction = "warming"
            else:
                direction = "stable"
        return {
            "direction": direction,
            "completed_wave_count": sum(
                1 for item in wave_runs if str(item.get("status", "") or "").strip().lower() in {"success", "completed"}
            ),
            "stable_wave_count": sum(
                1
                for item in wave_runs
                if str(item.get("trend_direction", "") or "").strip().lower() in {"stable", "improving"}
            ),
            "regression_wave_count": sum(
                1
                for item in wave_runs
                if str(item.get("trend_direction", "") or "").strip().lower() in {"regressing", "degraded"}
                or str(item.get("status", "") or "").strip().lower() in {"error", "failed"}
            ),
            "stable_wave_streak": stable_streak,
            "regression_wave_streak": regression_streak,
            "executed_program_total": sum(int(item.get("executed_program_count", 0) or 0) for item in wave_runs),
            "executed_campaign_total": sum(int(item.get("executed_campaign_count", 0) or 0) for item in wave_runs),
            "executed_sweep_total": sum(int(item.get("executed_sweep_count", 0) or 0) for item in wave_runs),
            "stable_program_total": sum(int(item.get("stable_program_count", 0) or 0) for item in wave_runs),
            "regression_program_total": sum(int(item.get("regression_program_count", 0) or 0) for item in wave_runs),
        }

    @staticmethod
    def _portfolio_pressure_score(
        *,
        pending_program_count: int,
        attention_program_count: int,
        pending_campaign_count: int,
        attention_campaign_count: int,
        pending_session_count: int,
        pending_app_target_count: int,
        long_horizon_pending_count: int,
        regression_wave_streak: int,
    ) -> float:
        return round(
            float(
                (attention_program_count * 2.8)
                + (pending_program_count * 1.6)
                + (attention_campaign_count * 1.2)
                + (pending_campaign_count * 0.7)
                + (pending_session_count * 0.4)
                + (pending_app_target_count * 1.5)
                + (long_horizon_pending_count * 0.32)
                + (regression_wave_streak * 2.15)
            ),
            6,
        )

    @staticmethod
    def _portfolio_priority(portfolio_pressure_score: float, trend_summary: Dict[str, Any]) -> str:
        direction = str(trend_summary.get("direction", "") or "").strip().lower()
        regression_streak = int(trend_summary.get("regression_wave_streak", 0) or 0)
        if portfolio_pressure_score >= 24.0 or regression_streak >= 2 or direction == "regressing":
            return "critical"
        if portfolio_pressure_score >= 12.0 or direction in {"warming", "volatile"}:
            return "elevated"
        if portfolio_pressure_score >= 5.0:
            return "active"
        return "steady"

    @staticmethod
    def _portfolio_trend_summary(
        wave_runs: List[Dict[str, Any]],
        program_rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        stable_streak = 0
        regression_streak = 0
        for item in reversed(wave_runs):
            trend = str(item.get("trend_direction", "") or "").strip().lower()
            status = str(item.get("status", "") or "").strip().lower()
            if trend in {"regressing", "degraded"} or status in {"error", "failed"}:
                regression_streak += 1
                break
            if trend in {"stable", "improving", "warming"} and status in {"success", "completed"}:
                stable_streak += 1
            else:
                break
        for item in reversed(wave_runs):
            trend = str(item.get("trend_direction", "") or "").strip().lower()
            status = str(item.get("status", "") or "").strip().lower()
            if trend in {"regressing", "degraded"} or status in {"error", "failed"}:
                regression_streak += 1
            else:
                break
        latest = wave_runs[-1] if wave_runs else {}
        direction = str(latest.get("trend_direction", "") or "").strip().lower()
        if not direction:
            attention_programs = sum(
                1 for row in program_rows if str(row.get("status", "") or "").strip().lower() == "attention"
            )
            pending_programs = sum(
                1 for row in program_rows if str(row.get("status", "") or "").strip().lower() != "complete"
            )
            if attention_programs > 0:
                direction = "regressing"
            elif pending_programs > 0:
                direction = "warming"
            else:
                direction = "stable"
        return {
            "direction": direction,
            "completed_wave_count": sum(
                1 for item in wave_runs if str(item.get("status", "") or "").strip().lower() in {"success", "completed"}
            ),
            "stable_wave_count": sum(
                1
                for item in wave_runs
                if str(item.get("trend_direction", "") or "").strip().lower() in {"stable", "improving"}
            ),
            "regression_wave_count": sum(
                1
                for item in wave_runs
                if str(item.get("trend_direction", "") or "").strip().lower() in {"regressing", "degraded"}
                or str(item.get("status", "") or "").strip().lower() in {"error", "failed"}
            ),
            "stable_wave_streak": stable_streak,
            "regression_wave_streak": regression_streak,
            "executed_program_total": sum(int(item.get("executed_program_count", 0) or 0) for item in wave_runs),
            "executed_campaign_total": sum(int(item.get("executed_campaign_count", 0) or 0) for item in wave_runs),
            "executed_sweep_total": sum(int(item.get("executed_sweep_count", 0) or 0) for item in wave_runs),
            "stable_program_total": sum(int(item.get("stable_program_count", 0) or 0) for item in wave_runs),
            "regression_program_total": sum(int(item.get("regression_program_count", 0) or 0) for item in wave_runs),
        }

    @staticmethod
    def _portfolio_pressure_score(
        *,
        pending_program_count: int,
        attention_program_count: int,
        pending_campaign_count: int,
        attention_campaign_count: int,
        pending_session_count: int,
        pending_app_target_count: int,
        long_horizon_pending_count: int,
        regression_wave_streak: int,
    ) -> float:
        return round(
            float(
                (attention_program_count * 2.8)
                + (pending_program_count * 1.6)
                + (attention_campaign_count * 1.2)
                + (pending_campaign_count * 0.7)
                + (pending_session_count * 0.4)
                + (pending_app_target_count * 1.5)
                + (long_horizon_pending_count * 0.32)
                + (regression_wave_streak * 2.15)
            ),
            6,
        )

    @staticmethod
    def _portfolio_priority(portfolio_pressure_score: float, trend_summary: Dict[str, Any]) -> str:
        direction = str(trend_summary.get("direction", "") or "").strip().lower()
        regression_streak = int(trend_summary.get("regression_wave_streak", 0) or 0)
        if portfolio_pressure_score >= 24.0 or regression_streak >= 2 or direction == "regressing":
            return "critical"
        if portfolio_pressure_score >= 12.0 or direction in {"warming", "volatile"}:
            return "elevated"
        if portfolio_pressure_score >= 5.0:
            return "active"
        return "steady"

    def _load(self) -> None:
        try:
            if not self.store_path.exists():
                return
            raw = json.loads(self.store_path.read_text(encoding="utf-8"))
        except Exception:
            return
        rows = raw.get("sessions", []) if isinstance(raw, dict) else []
        campaign_rows = raw.get("campaigns", []) if isinstance(raw, dict) else []
        program_rows = raw.get("programs", []) if isinstance(raw, dict) else []
        portfolio_rows = raw.get("portfolios", []) if isinstance(raw, dict) else []
        loaded: Dict[str, Dict[str, Any]] = {}
        for item in rows:
            if not isinstance(item, dict):
                continue
            session_id = str(item.get("session_id", "") or "").strip()
            if not session_id:
                continue
            loaded[session_id] = dict(item)
        self._sessions = loaded
        loaded_campaigns: Dict[str, Dict[str, Any]] = {}
        if isinstance(campaign_rows, list):
            for item in campaign_rows:
                if not isinstance(item, dict):
                    continue
                campaign_id = str(item.get("campaign_id", "") or "").strip()
                if not campaign_id:
                    continue
                loaded_campaigns[campaign_id] = dict(item)
        self._campaigns = loaded_campaigns
        loaded_programs: Dict[str, Dict[str, Any]] = {}
        if isinstance(program_rows, list):
            for item in program_rows:
                if not isinstance(item, dict):
                    continue
                program_id = str(item.get("program_id", "") or "").strip()
                if not program_id:
                    continue
                loaded_programs[program_id] = dict(item)
        self._programs = loaded_programs
        loaded_portfolios: Dict[str, Dict[str, Any]] = {}
        if isinstance(portfolio_rows, list):
            for item in portfolio_rows:
                if not isinstance(item, dict):
                    continue
                portfolio_id = str(item.get("portfolio_id", "") or "").strip()
                if not portfolio_id:
                    continue
                loaded_portfolios[portfolio_id] = dict(item)
        self._portfolios = loaded_portfolios

    def _save_locked(self) -> None:
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        rows = sorted(
            (dict(item) for item in self._sessions.values() if isinstance(item, dict)),
            key=lambda item: str(item.get("updated_at", "") or ""),
            reverse=True,
        )[: self.max_sessions]
        campaigns = sorted(
            (dict(item) for item in self._campaigns.values() if isinstance(item, dict)),
            key=lambda item: str(item.get("updated_at", "") or ""),
            reverse=True,
        )[: self.max_campaigns]
        programs = sorted(
            (dict(item) for item in self._programs.values() if isinstance(item, dict)),
            key=lambda item: str(item.get("updated_at", "") or ""),
            reverse=True,
        )[: self.max_programs]
        portfolios = sorted(
            (dict(item) for item in self._portfolios.values() if isinstance(item, dict)),
            key=lambda item: str(item.get("updated_at", "") or ""),
            reverse=True,
        )[: self.max_portfolios]
        payload = {"sessions": rows, "campaigns": campaigns, "programs": programs, "portfolios": portfolios}
        self.store_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        self._updates_since_save = 0
        self._last_save_monotonic = time.monotonic()

    def _maybe_save_locked(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if force or self._updates_since_save >= 1 or (now - self._last_save_monotonic) >= 10.0:
            self._save_locked()

    def _trim_locked(self) -> None:
        if len(self._sessions) > self.max_sessions:
            rows = sorted(
                self._sessions.values(),
                key=lambda item: str(item.get("updated_at", "") or ""),
                reverse=True,
            )
            self._sessions = {
                str(item.get("session_id", "") or ""): dict(item)
                for item in rows[: self.max_sessions]
                if str(item.get("session_id", "") or "").strip()
            }
        if len(self._campaigns) > self.max_campaigns:
            rows = sorted(
                self._campaigns.values(),
                key=lambda item: str(item.get("updated_at", "") or ""),
                reverse=True,
            )
            self._campaigns = {
                str(item.get("campaign_id", "") or ""): dict(item)
                for item in rows[: self.max_campaigns]
                if str(item.get("campaign_id", "") or "").strip()
            }
        if len(self._programs) > self.max_programs:
            rows = sorted(
                self._programs.values(),
                key=lambda item: str(item.get("updated_at", "") or ""),
                reverse=True,
            )
            self._programs = {
                str(item.get("program_id", "") or ""): dict(item)
                for item in rows[: self.max_programs]
                if str(item.get("program_id", "") or "").strip()
            }
        if len(self._portfolios) > self.max_portfolios:
            rows = sorted(
                self._portfolios.values(),
                key=lambda item: str(item.get("updated_at", "") or ""),
                reverse=True,
            )
            self._portfolios = {
                str(item.get("portfolio_id", "") or ""): dict(item)
                for item in rows[: self.max_portfolios]
                if str(item.get("portfolio_id", "") or "").strip()
            }

    @staticmethod
    def _public_row(row: Dict[str, Any]) -> Dict[str, Any]:
        return dict(row) if isinstance(row, dict) else {}

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            return default
        return max(minimum, min(parsed, maximum))

    @staticmethod
    def _increment_count(counts: Dict[str, int], key: str) -> None:
        clean = str(key or "").strip().lower() or "unknown"
        counts[clean] = int(counts.get(clean, 0)) + 1

    @staticmethod
    def _sorted_count_map(source: Dict[str, int]) -> Dict[str, int]:
        items = sorted(source.items(), key=lambda item: (-int(item[1]), item[0]))
        return {key: int(value) for key, value in items}

    @staticmethod
    def _dedupe_strings(values: List[str]) -> List[str]:
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

    def _hydrate_campaign_sessions(
        self,
        *,
        session_ids: List[str],
        session_rows: List[Dict[str, Any]] | None,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if isinstance(session_rows, list):
            rows.extend(dict(item) for item in session_rows if isinstance(item, dict))
        known_ids = {
            str(item.get("session_id", "") or "").strip()
            for item in rows
            if isinstance(item, dict) and str(item.get("session_id", "") or "").strip()
        }
        for session_id in session_ids:
            clean_id = str(session_id or "").strip()
            if not clean_id or clean_id in known_ids:
                continue
            current = self._sessions.get(clean_id)
            if isinstance(current, dict):
                rows.append(dict(current))
                known_ids.add(clean_id)
        return rows

    def _hydrate_program_campaigns(
        self,
        *,
        campaign_ids: List[str],
        campaign_rows: List[Dict[str, Any]] | None,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if isinstance(campaign_rows, list):
            rows.extend(dict(item) for item in campaign_rows if isinstance(item, dict))
        known_ids = {
            str(item.get("campaign_id", "") or "").strip()
            for item in rows
            if isinstance(item, dict) and str(item.get("campaign_id", "") or "").strip()
        }
        for campaign_id in campaign_ids:
            clean_id = str(campaign_id or "").strip()
            if not clean_id or clean_id in known_ids:
                continue
            current = self._campaigns.get(clean_id)
            if isinstance(current, dict):
                rows.append(dict(current))
                known_ids.add(clean_id)
        return rows

    def _hydrate_portfolio_programs(
        self,
        *,
        program_ids: List[str],
        program_rows: List[Dict[str, Any]] | None,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if isinstance(program_rows, list):
            rows.extend(dict(item) for item in program_rows if isinstance(item, dict))
        known_ids = {
            str(item.get("program_id", "") or "").strip()
            for item in rows
            if isinstance(item, dict) and str(item.get("program_id", "") or "").strip()
        }
        for program_id in program_ids:
            clean_id = str(program_id or "").strip()
            if not clean_id or clean_id in known_ids:
                continue
            current = self._programs.get(clean_id)
            if isinstance(current, dict):
                rows.append(dict(current))
                known_ids.add(clean_id)
        return rows

    def _normalize_campaign_sessions(self, sessions: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not isinstance(sessions, list):
            return rows
        for item in sessions:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "session_id": str(item.get("session_id", "") or "").strip(),
                    "label": str(item.get("label", "") or "").strip(),
                    "status": str(item.get("status", "") or "ready").strip().lower() or "ready",
                    "pending_replay_count": self._coerce_int(item.get("pending_replay_count", 0), minimum=0, maximum=100_000, default=0),
                    "failed_replay_count": self._coerce_int(item.get("failed_replay_count", 0), minimum=0, maximum=100_000, default=0),
                    "completed_replay_count": self._coerce_int(item.get("completed_replay_count", 0), minimum=0, maximum=100_000, default=0),
                    "cycle_count": self._coerce_int(item.get("cycle_count", 0), minimum=0, maximum=100_000, default=0),
                    "regression_cycle_count": self._coerce_int(item.get("regression_cycle_count", 0), minimum=0, maximum=100_000, default=0),
                    "long_horizon_pending_count": self._coerce_int(item.get("long_horizon_pending_count", 0), minimum=0, maximum=100_000, default=0),
                    "target_app_count": self._coerce_int(item.get("target_app_count", 0), minimum=0, maximum=100_000, default=0),
                    "target_apps": [
                        str(app_name).strip()
                        for app_name in item.get("target_apps", [])
                        if str(app_name).strip()
                    ][:8] if isinstance(item.get("target_apps", []), list) else [],
                    "history_direction": str(item.get("history_direction", "") or "").strip(),
                    "latest_cycle_regression_status": str(item.get("latest_cycle_regression_status", "") or "").strip().lower(),
                    "latest_cycle_score": round(float(item.get("latest_cycle_score", 0.0) or 0.0), 6),
                    "updated_at": str(item.get("updated_at", "") or "").strip(),
                    "created_at": str(item.get("created_at", "") or "").strip(),
                }
            )
        rows.sort(key=lambda item: str(item.get("updated_at", "") or ""), reverse=True)
        return rows[:12]

    def _normalize_campaign_sweep_runs(self, sweeps: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not isinstance(sweeps, list):
            return rows
        for item in sweeps:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "sweep_id": str(item.get("sweep_id", "") or "").strip() or f"sweep-{uuid.uuid4().hex[:10]}",
                    "recorded_at": str(item.get("recorded_at", "") or "").strip(),
                    "executed_at": str(item.get("executed_at", "") or "").strip(),
                    "status": str(item.get("status", "") or "success").strip().lower() or "success",
                    "regression_status": str(item.get("regression_status", "") or "").strip().lower(),
                    "executed_session_count": self._coerce_int(item.get("executed_session_count", 0), minimum=0, maximum=100_000, default=0),
                    "created_session_count": self._coerce_int(item.get("created_session_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_session_count": self._coerce_int(item.get("pending_session_count", 0), minimum=0, maximum=100_000, default=0),
                    "attention_session_count": self._coerce_int(item.get("attention_session_count", 0), minimum=0, maximum=100_000, default=0),
                    "long_horizon_pending_count": self._coerce_int(item.get("long_horizon_pending_count", 0), minimum=0, maximum=100_000, default=0),
                    "pending_app_target_count": self._coerce_int(item.get("pending_app_target_count", 0), minimum=0, maximum=100_000, default=0),
                    "weighted_score": round(float(item.get("weighted_score", 0.0) or 0.0), 6),
                    "weighted_pass_rate": round(float(item.get("weighted_pass_rate", 0.0) or 0.0), 6),
                    "history_direction": str(item.get("history_direction", "") or "").strip().lower(),
                    "query": dict(item.get("query", {})) if isinstance(item.get("query", {}), dict) else {},
                }
            )
        return rows[-self.max_sweep_runs :]

    def _campaign_sweep_row(
        self,
        *,
        recorded_at: str,
        sweep_payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "sweep_id": f"sweep-{uuid.uuid4().hex[:10]}",
            "recorded_at": recorded_at,
            "executed_at": str(sweep_payload.get("executed_at", "") or recorded_at).strip(),
            "status": str(sweep_payload.get("status", "") or "success").strip().lower() or "success",
            "regression_status": str(sweep_payload.get("regression_status", "") or "").strip().lower(),
            "executed_session_count": self._coerce_int(sweep_payload.get("executed_session_count", 0), minimum=0, maximum=100_000, default=0),
            "created_session_count": self._coerce_int(sweep_payload.get("created_session_count", 0), minimum=0, maximum=100_000, default=0),
            "pending_session_count": self._coerce_int(sweep_payload.get("pending_session_count", 0), minimum=0, maximum=100_000, default=0),
            "attention_session_count": self._coerce_int(sweep_payload.get("attention_session_count", 0), minimum=0, maximum=100_000, default=0),
            "long_horizon_pending_count": self._coerce_int(sweep_payload.get("long_horizon_pending_count", 0), minimum=0, maximum=100_000, default=0),
            "pending_app_target_count": self._coerce_int(sweep_payload.get("pending_app_target_count", 0), minimum=0, maximum=100_000, default=0),
            "weighted_score": round(float(sweep_payload.get("weighted_score", 0.0) or 0.0), 6),
            "weighted_pass_rate": round(float(sweep_payload.get("weighted_pass_rate", 0.0) or 0.0), 6),
            "history_direction": str(sweep_payload.get("history_direction", "") or "").strip().lower(),
            "query": dict(sweep_payload.get("query", {})) if isinstance(sweep_payload.get("query", {}), dict) else {},
        }

    def _campaign_trend_summary(
        self,
        sweep_runs: List[Dict[str, Any]],
        session_rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        latest = dict(sweep_runs[-1]) if sweep_runs else {}
        previous = dict(sweep_runs[-2]) if len(sweep_runs) >= 2 else {}
        latest_score = round(float(latest.get("weighted_score", 0.0) or 0.0), 6)
        previous_score = round(float(previous.get("weighted_score", latest_score) or latest_score), 6)
        latest_pass_rate = round(float(latest.get("weighted_pass_rate", 0.0) or 0.0), 6)
        previous_pass_rate = round(float(previous.get("weighted_pass_rate", latest_pass_rate) or latest_pass_rate), 6)
        latest_pending = self._coerce_int(latest.get("pending_session_count", 0), minimum=0, maximum=100_000, default=0)
        previous_pending = self._coerce_int(previous.get("pending_session_count", latest_pending), minimum=0, maximum=100_000, default=latest_pending)
        latest_attention = self._coerce_int(latest.get("attention_session_count", 0), minimum=0, maximum=100_000, default=0)
        previous_attention = self._coerce_int(previous.get("attention_session_count", latest_attention), minimum=0, maximum=100_000, default=latest_attention)
        latest_long_horizon = self._coerce_int(latest.get("long_horizon_pending_count", 0), minimum=0, maximum=100_000, default=0)
        previous_long_horizon = self._coerce_int(previous.get("long_horizon_pending_count", latest_long_horizon), minimum=0, maximum=100_000, default=latest_long_horizon)
        latest_regression_status = str(latest.get("regression_status", "") or "").strip().lower()
        completed_sweep_count = sum(
            1 for item in sweep_runs if str(item.get("status", "") or "").strip().lower() in {"success", "completed"}
        )
        regression_sweep_count = sum(
            1 for item in sweep_runs if str(item.get("regression_status", "") or "").strip().lower() in {"regression", "failed"}
        )
        stable_sweep_streak = 0
        regression_sweep_streak = 0
        for item in reversed(sweep_runs):
            item_regression = str(item.get("regression_status", item.get("status", "")) or "").strip().lower()
            if item_regression in {"stable", "success", "completed", ""}:
                if regression_sweep_streak == 0:
                    stable_sweep_streak += 1
                else:
                    break
            else:
                break
        for item in reversed(sweep_runs):
            item_regression = str(item.get("regression_status", item.get("status", "")) or "").strip().lower()
            if item_regression in {"regression", "failed", "error"}:
                if stable_sweep_streak == 0:
                    regression_sweep_streak += 1
                else:
                    break
            else:
                break
        score_delta = round(latest_score - previous_score, 6)
        pass_rate_delta = round(latest_pass_rate - previous_pass_rate, 6)
        pending_delta = latest_pending - previous_pending
        attention_delta = latest_attention - previous_attention
        long_horizon_delta = latest_long_horizon - previous_long_horizon
        improve_signals = 0
        regress_signals = 0
        if score_delta >= 0.02:
            improve_signals += 1
        elif score_delta <= -0.02:
            regress_signals += 1
        if pass_rate_delta >= 0.02:
            improve_signals += 1
        elif pass_rate_delta <= -0.02:
            regress_signals += 1
        if pending_delta < 0:
            improve_signals += 1
        elif pending_delta > 0:
            regress_signals += 1
        if attention_delta < 0:
            improve_signals += 1
        elif attention_delta > 0:
            regress_signals += 1
        if latest_regression_status in {"regression", "failed", "error"}:
            regress_signals += 2
        direction = "stable"
        if improve_signals > 0 and regress_signals > 0:
            direction = "volatile"
        elif regress_signals > 0:
            direction = "regressing"
        elif improve_signals > 0:
            direction = "improving"
        elif sweep_runs:
            direction = "stable"
        elif session_rows:
            direction = "warming"
        return {
            "direction": direction,
            "run_count": len(sweep_runs),
            "latest_score": latest_score,
            "previous_score": previous_score,
            "score_delta": score_delta,
            "latest_pass_rate": latest_pass_rate,
            "previous_pass_rate": previous_pass_rate,
            "pass_rate_delta": pass_rate_delta,
            "latest_pending_session_count": latest_pending,
            "pending_session_delta": pending_delta,
            "latest_attention_session_count": latest_attention,
            "attention_session_delta": attention_delta,
            "latest_long_horizon_pending_count": latest_long_horizon,
            "long_horizon_pending_delta": long_horizon_delta,
            "completed_sweep_count": completed_sweep_count,
            "regression_sweep_count": regression_sweep_count,
            "stable_sweep_streak": stable_sweep_streak,
            "regression_sweep_streak": regression_sweep_streak,
            "latest_regression_status": latest_regression_status,
            "history_direction": str(latest.get("history_direction", "") or "").strip().lower(),
        }

    @staticmethod
    def _campaign_pressure_score(
        *,
        pending_session_count: int,
        attention_session_count: int,
        pending_app_target_count: int,
        long_horizon_pending_count: int,
        regression_cycle_count: int,
        regression_sweep_streak: int,
    ) -> float:
        return round(
            (attention_session_count * 3.0)
            + (pending_session_count * 1.35)
            + (pending_app_target_count * 2.1)
            + (long_horizon_pending_count * 0.55)
            + (regression_cycle_count * 0.8)
            + (regression_sweep_streak * 2.4),
            6,
        )

    @staticmethod
    def _campaign_priority(campaign_pressure_score: float, trend_summary: Dict[str, Any]) -> str:
        direction = str(trend_summary.get("direction", "") or "").strip().lower()
        regression_streak = int(trend_summary.get("regression_sweep_streak", 0) or 0)
        if campaign_pressure_score >= 12.0 or regression_streak >= 2 or direction == "regressing":
            return "critical"
        if campaign_pressure_score >= 7.0 or direction == "volatile":
            return "elevated"
        if campaign_pressure_score >= 3.0 or direction == "warming":
            return "steady"
        return "stable"
