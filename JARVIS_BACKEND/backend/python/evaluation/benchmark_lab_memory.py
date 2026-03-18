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
    ) -> None:
        self.store_path = Path(store_path)
        self.max_sessions = self._coerce_int(max_sessions, minimum=8, maximum=5000, default=200)
        self.max_replay_events = self._coerce_int(max_replay_events, minimum=2, maximum=128, default=16)
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
        for row in rows:
            self._increment_count(status_counts, str(row.get("status", "") or "ready"))
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
                "pending_replays": sum(int(row.get("pending_replay_count", 0) or 0) for row in rows),
                "failed_replays": sum(int(row.get("failed_replay_count", 0) or 0) for row in rows),
                "completed_replays": sum(int(row.get("completed_replay_count", 0) or 0) for row in rows),
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
            row = self._session_row(
                session_id=clean_id,
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
                replay_candidates=candidates,
                replay_events=replay_events,
            )
            self._sessions[clean_id] = row
            self._updates_since_save += 1
            self._maybe_save_locked(force=True)
        return {
            "status": "success",
            "session": self._public_row(row),
            "updated_candidate": updated_candidate or {},
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
    ) -> Dict[str, Any]:
        clean_candidates = (
            [dict(item) for item in replay_candidates]
            if isinstance(replay_candidates, list)
            else self._normalize_replay_candidates(lab_snapshot.get("replay_candidates", []))
        )
        pending_replay_count = sum(1 for item in clean_candidates if str(item.get("replay_status", "pending") or "pending").strip().lower() == "pending")
        failed_replay_count = sum(1 for item in clean_candidates if str(item.get("replay_status", "") or "").strip().lower() == "failed")
        completed_replay_count = sum(1 for item in clean_candidates if str(item.get("replay_status", "") or "").strip().lower() == "completed")
        session_status = "ready"
        if failed_replay_count > 0:
            session_status = "attention"
        elif clean_candidates and pending_replay_count == 0:
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
