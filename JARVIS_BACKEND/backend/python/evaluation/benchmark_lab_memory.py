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
        max_replay_events: int = 16,
        max_run_cycles: int = 24,
    ) -> None:
        self.store_path = Path(store_path)
        self.max_sessions = self._coerce_int(max_sessions, minimum=8, maximum=5000, default=200)
        self.max_replay_events = self._coerce_int(max_replay_events, minimum=2, maximum=128, default=16)
        self.max_run_cycles = self._coerce_int(max_run_cycles, minimum=1, maximum=256, default=24)
        self._lock = RLock()
        self._sessions: Dict[str, Dict[str, Any]] = {}
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

    def _load(self) -> None:
        try:
            if not self.store_path.exists():
                return
            raw = json.loads(self.store_path.read_text(encoding="utf-8"))
        except Exception:
            return
        rows = raw.get("sessions", []) if isinstance(raw, dict) else []
        if not isinstance(rows, list):
            return
        loaded: Dict[str, Dict[str, Any]] = {}
        for item in rows:
            if not isinstance(item, dict):
                continue
            session_id = str(item.get("session_id", "") or "").strip()
            if not session_id:
                continue
            loaded[session_id] = dict(item)
        self._sessions = loaded

    def _save_locked(self) -> None:
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        rows = sorted(
            (dict(item) for item in self._sessions.values() if isinstance(item, dict)),
            key=lambda item: str(item.get("updated_at", "") or ""),
            reverse=True,
        )[: self.max_sessions]
        payload = {"sessions": rows}
        self.store_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        self._updates_since_save = 0
        self._last_save_monotonic = time.monotonic()

    def _maybe_save_locked(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if force or self._updates_since_save >= 1 or (now - self._last_save_monotonic) >= 10.0:
            self._save_locked()

    def _trim_locked(self) -> None:
        if len(self._sessions) <= self.max_sessions:
            return
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
