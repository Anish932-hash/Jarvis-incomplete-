from dataclasses import dataclass
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(slots=True)
class RiskRating:
    score: int
    level: str
    reason: str
    factors: List[str]


class RiskEngine:
    _risk_table: Dict[str, int] = {
        "tts_speak": 5,
        "tts_stop": 4,
        "time_now": 5,
        "defender_status": 10,
        "system_snapshot": 10,
        "list_processes": 10,
        "list_windows": 10,
        "active_window": 10,
        "media_info": 10,
        "folder_size": 10,
        "explorer_open_path": 22,
        "explorer_select_file": 28,
        "list_folder": 12,
        "list_files": 12,
        "open_url": 20,
        "media_search": 20,
        "screenshot_capture": 30,
        "browser_read_dom": 32,
        "browser_extract_links": 34,
        "browser_session_create": 30,
        "browser_session_list": 18,
        "browser_session_close": 28,
        "browser_session_request": 42,
        "browser_session_read_dom": 40,
        "browser_session_extract_links": 42,
        "computer_observe": 45,
        "computer_assert_text_visible": 60,
        "computer_find_text_targets": 58,
        "computer_wait_for_text": 55,
        "computer_click_text": 78,
        "computer_click_target": 80,
        "extract_text_from_image": 35,
        "clipboard_read": 35,
        "search_files": 25,
        "search_text": 25,
        "scan_directory": 25,
        "hash_file": 25,
        "backup_file": 35,
        "copy_file": 45,
        "clipboard_write": 60,
        "keyboard_type": 70,
        "keyboard_hotkey": 72,
        "mouse_move": 72,
        "mouse_click": 75,
        "mouse_scroll": 72,
        "run_whitelisted_app": 75,
        "run_trusted_script": 85,
        "open_app": 35,
        "focus_window": 35,
        "create_folder": 40,
        "media_play_pause": 40,
        "media_play": 40,
        "media_pause": 40,
        "media_stop": 45,
        "media_next": 45,
        "media_previous": 45,
        "send_notification": 45,
        "read_file": 50,
        "write_file": 60,
        "terminate_process": 80,
        "external_connector_status": 20,
        "external_email_send": 72,
        "external_email_list": 45,
        "external_email_read": 55,
        "external_calendar_create_event": 58,
        "external_calendar_list_events": 46,
        "external_calendar_update_event": 68,
        "external_doc_create": 52,
        "external_doc_list": 44,
        "external_doc_read": 52,
        "external_doc_update": 66,
        "external_task_list": 46,
        "external_task_create": 58,
        "external_task_update": 68,
        "oauth_token_list": 28,
        "oauth_token_upsert": 78,
        "oauth_token_refresh": 54,
        "oauth_token_revoke": 72,
        "accessibility_status": 22,
        "accessibility_list_elements": 45,
        "accessibility_find_element": 50,
        "accessibility_invoke_element": 75,
        "delete_file": 90,
        "run_script": 95,
    }

    _high_risk_actions = {
        "terminate_process",
        "run_trusted_script",
        "keyboard_type",
        "keyboard_hotkey",
        "mouse_click",
        "mouse_move",
        "mouse_scroll",
        "computer_click_text",
        "computer_click_target",
        "accessibility_invoke_element",
        "external_email_send",
        "external_calendar_update_event",
        "external_doc_update",
        "external_task_update",
        "oauth_token_upsert",
        "oauth_token_revoke",
    }

    def __init__(self) -> None:
        self._adaptive_action_state: Dict[str, Dict[str, float]] = {}
        self._adaptive_source_state: Dict[str, Dict[str, float]] = {}
        self._adaptive_profile_state: Dict[str, Dict[str, float]] = {}
        self._adaptive_decay = 0.9
        self._adaptive_enabled = True
        self._mission_feedback_state: Dict[str, float | str] = {
            "pressure_ema": 0.0,
            "risk_ema": 0.0,
            "quality_ema": 0.5,
            "failed_ratio_ema": 0.0,
            "blocked_ratio_ema": 0.0,
            "updated_at": 0.0,
            "last_reason": "",
        }

    @staticmethod
    def _ema(current: float, value: float, *, alpha: float) -> float:
        return (1.0 - alpha) * float(current) + (alpha * float(value))

    def rate(
        self,
        action: str,
        *,
        args: Optional[Dict[str, Any]] = None,
        source: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> RiskRating:
        clean_action = str(action or "").strip()
        score = self._risk_table.get(clean_action, 50)
        factors = [f"base={score}({clean_action})"]
        payload = args if isinstance(args, dict) else {}
        source_name = str(source or "").strip().lower()
        meta = metadata if isinstance(metadata, dict) else {}

        if source_name in {"desktop-trigger", "desktop-schedule"} and clean_action in self._high_risk_actions:
            score += 4
            factors.append("source_automation_high_risk:+4")
        if source_name == "voice-loop" and clean_action in self._high_risk_actions:
            score += 5
            factors.append("source_voice_high_risk:+5")

        profile = str(meta.get("policy_profile", "")).strip().lower()
        if profile == "automation_safe":
            score += 5
            factors.append("profile_automation_safe:+5")
        elif profile == "automation_power":
            score -= 4
            factors.append("profile_automation_power:-4")

        mission_pressure = self._mission_pressure(meta)
        if mission_pressure >= 0.12:
            mission_delta = int(round(min(16.0, mission_pressure * 18.0)))
            score += mission_delta
            factors.append(f"mission_pressure:+{mission_delta}")

        if clean_action in {
            "open_url",
            "browser_read_dom",
            "browser_extract_links",
            "browser_session_request",
            "browser_session_read_dom",
            "browser_session_extract_links",
        }:
            url = str(payload.get("url", "")).strip().lower()
            if any(marker in url for marker in ("file://", "127.0.0.1", "localhost")):
                score += 20
                factors.append("url_local_or_file:+20")
            if any(marker in url for marker in ("admin", "login", "oauth", "token", "reset-password")):
                score += 8
                factors.append("url_sensitive_surface:+8")

        if clean_action in {"write_file", "copy_file", "backup_file"}:
            paths = [payload.get("path"), payload.get("source"), payload.get("destination")]
            if any(self._is_system_path(path_value) for path_value in paths):
                score += 18
                factors.append("filesystem_system_path:+18")

        if clean_action == "external_email_send":
            recipients = payload.get("to")
            count = len(recipients) if isinstance(recipients, list) else (1 if isinstance(recipients, str) and recipients.strip() else 0)
            if count >= 5:
                score += 10
                factors.append("email_broadcast:+10")
            if bool(payload.get("attachments")):
                score += 6
                factors.append("email_attachment:+6")

        if clean_action in {"external_doc_create", "external_doc_update"}:
            content = str(payload.get("content", ""))
            if len(content) > 4000:
                score += 4
                factors.append("doc_large_payload:+4")

        external_pressure = meta.get("external_contract_pressure")
        if isinstance(external_pressure, dict):
            action_pressure = external_pressure.get(clean_action)
            if isinstance(action_pressure, dict):
                pressure = max(0.0, min(float(action_pressure.get("pressure", 0.0) or 0.0), 1.0))
                if pressure >= 0.2:
                    delta = min(18.0, pressure * 22.0)
                    score += int(round(delta))
                    factors.append(f"external_contract_pressure:+{int(round(delta))}")

        if self._adaptive_enabled:
            adaptive_delta, adaptive_factors = self._adaptive_risk_delta(clean_action, source_name)
            score += adaptive_delta
            factors.extend(adaptive_factors)
            profile_delta, profile_factors = self._adaptive_profile_delta(profile)
            score += profile_delta
            factors.extend(profile_factors)

        if clean_action in {"external_calendar_create_event", "external_calendar_update_event"}:
            attendees = payload.get("attendees")
            if isinstance(attendees, list) and len(attendees) >= 8:
                score += 6
                factors.append("calendar_large_audience:+6")

        if clean_action == "accessibility_invoke_element":
            if not str(payload.get("element_id", "")).strip() and not str(payload.get("query", "")).strip():
                score += 10
                factors.append("accessibility_unscoped_target:+10")

        score = max(0, min(int(score), 100))
        if score <= 20:
            level = "low"
        elif score <= 60:
            level = "medium"
        elif score <= 85:
            level = "high"
        else:
            level = "critical"
        return RiskRating(
            score=score,
            level=level,
            reason=f"action={clean_action} risk={score} level={level}",
            factors=factors,
        )

    def record_outcome(
        self,
        *,
        action: str,
        status: str,
        source: str = "",
        error: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip()
        if not clean_action:
            return {"status": "error", "message": "action is required"}
        normalized_status = str(status or "").strip().lower() or "unknown"
        source_name = str(source or "").strip().lower()
        data = metadata if isinstance(metadata, dict) else {}
        profile_name = str(data.get("policy_profile", "")).strip().lower()
        failed = normalized_status in {"failed", "blocked", "error"}
        severe = failed and any(token in str(error or "").lower() for token in ("permission", "denied", "unsafe", "critical"))
        fail_signal = 1.0 if failed else 0.0
        severe_signal = 1.0 if severe else 0.0

        action_state = self._adaptive_action_state.setdefault(
            clean_action,
            {"samples": 0.0, "failure_ema": 0.0, "severe_ema": 0.0, "last_updated": 0.0},
        )
        action_state["samples"] = float(action_state.get("samples", 0.0) or 0.0) + 1.0
        action_state["failure_ema"] = self._ema(float(action_state.get("failure_ema", 0.0) or 0.0), fail_signal, alpha=0.22)
        action_state["severe_ema"] = self._ema(float(action_state.get("severe_ema", 0.0) or 0.0), severe_signal, alpha=0.18)
        action_state["last_updated"] = time.time()

        if source_name:
            source_state = self._adaptive_source_state.setdefault(
                source_name,
                {"samples": 0.0, "failure_ema": 0.0, "severe_ema": 0.0, "last_updated": 0.0},
            )
            source_state["samples"] = float(source_state.get("samples", 0.0) or 0.0) + 1.0
            source_state["failure_ema"] = self._ema(float(source_state.get("failure_ema", 0.0) or 0.0), fail_signal, alpha=0.2)
            source_state["severe_ema"] = self._ema(float(source_state.get("severe_ema", 0.0) or 0.0), severe_signal, alpha=0.16)
            source_state["last_updated"] = time.time()

        if profile_name:
            profile_state = self._adaptive_profile_state.setdefault(
                profile_name,
                {"samples": 0.0, "failure_ema": 0.0, "severe_ema": 0.0, "last_updated": 0.0},
            )
            profile_state["samples"] = float(profile_state.get("samples", 0.0) or 0.0) + 1.0
            profile_state["failure_ema"] = self._ema(float(profile_state.get("failure_ema", 0.0) or 0.0), fail_signal, alpha=0.18)
            profile_state["severe_ema"] = self._ema(float(profile_state.get("severe_ema", 0.0) or 0.0), severe_signal, alpha=0.14)
            profile_state["last_updated"] = time.time()

        return {
            "status": "success",
            "action": clean_action,
            "source": source_name,
            "profile": profile_name,
            "failed": failed,
            "severe": severe,
            "failure_ema": round(float(action_state.get("failure_ema", 0.0) or 0.0), 6),
            "severe_ema": round(float(action_state.get("severe_ema", 0.0) or 0.0), 6),
        }

    def ingest_mission_feedback(
        self,
        *,
        autonomy_report: Optional[Dict[str, Any]] = None,
        mission_summary: Optional[Dict[str, Any]] = None,
        reason: str = "manual",
    ) -> Dict[str, Any]:
        report = autonomy_report if isinstance(autonomy_report, dict) else {}
        missions = mission_summary if isinstance(mission_summary, dict) else {}
        pressures = report.get("pressures", {}) if isinstance(report.get("pressures", {}), dict) else {}
        mission_risk = missions.get("risk", {}) if isinstance(missions.get("risk", {}), dict) else {}
        mission_quality = missions.get("quality", {}) if isinstance(missions.get("quality", {}), dict) else {}

        failure_pressure = self._coerce_float(pressures.get("failure_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        breaker_pressure = self._coerce_float(pressures.get("open_breaker_pressure", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        risk_score = self._coerce_float(mission_risk.get("avg_score", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        quality_score = self._coerce_float(mission_quality.get("avg_score", 0.5), minimum=0.0, maximum=1.0, default=0.5)
        failed_ratio = self._coerce_float(missions.get("failed_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        blocked_ratio = self._coerce_float(missions.get("blocked_ratio", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        pressure = self._coerce_float(
            (failure_pressure * 0.4) + (breaker_pressure * 0.2) + (risk_score * 0.25) + (failed_ratio * 0.1) + (blocked_ratio * 0.05),
            minimum=0.0,
            maximum=1.0,
            default=0.0,
        )

        alpha = 0.2
        state = self._mission_feedback_state
        state["pressure_ema"] = self._ema(float(state.get("pressure_ema", 0.0) or 0.0), pressure, alpha=alpha)
        state["risk_ema"] = self._ema(float(state.get("risk_ema", 0.0) or 0.0), risk_score, alpha=alpha)
        state["quality_ema"] = self._ema(float(state.get("quality_ema", 0.5) or 0.5), quality_score, alpha=alpha)
        state["failed_ratio_ema"] = self._ema(float(state.get("failed_ratio_ema", 0.0) or 0.0), failed_ratio, alpha=alpha)
        state["blocked_ratio_ema"] = self._ema(float(state.get("blocked_ratio_ema", 0.0) or 0.0), blocked_ratio, alpha=alpha)
        state["updated_at"] = time.time()
        state["last_reason"] = str(reason or "").strip()
        return {
            "status": "success",
            "pressure": round(pressure, 6),
            "state": {
                "pressure_ema": round(float(state.get("pressure_ema", 0.0) or 0.0), 6),
                "risk_ema": round(float(state.get("risk_ema", 0.0) or 0.0), 6),
                "quality_ema": round(float(state.get("quality_ema", 0.5) or 0.5), 6),
                "failed_ratio_ema": round(float(state.get("failed_ratio_ema", 0.0) or 0.0), 6),
                "blocked_ratio_ema": round(float(state.get("blocked_ratio_ema", 0.0) or 0.0), 6),
            },
            "reason": str(reason or "").strip(),
        }

    def rate_batch(
        self,
        actions: List[str],
        *,
        args_map: Optional[Dict[str, Dict[str, Any]]] = None,
        source: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []
        for index, action_name in enumerate(actions):
            clean_action = str(action_name or "").strip()
            if not clean_action:
                continue
            payload_args = {}
            if isinstance(args_map, dict):
                maybe_args = args_map.get(clean_action, {})
                payload_args = maybe_args if isinstance(maybe_args, dict) else {}
            rating = self.rate(
                clean_action,
                args=payload_args,
                source=source,
                metadata=metadata if isinstance(metadata, dict) else {},
            )
            burst_penalty = 0
            if index >= 1 and rating.level in {"high", "critical"}:
                burst_penalty = min(10, index * 2)
            final_score = max(0, min(100, int(rating.score + burst_penalty)))
            final_level = "low" if final_score <= 20 else ("medium" if final_score <= 60 else ("high" if final_score <= 85 else "critical"))
            rows.append(
                {
                    "index": index + 1,
                    "action": clean_action,
                    "score": final_score,
                    "level": final_level,
                    "burst_penalty": burst_penalty,
                    "factors": list(rating.factors),
                }
            )
        max_score = max([int(row["score"]) for row in rows] or [0])
        avg_score = (sum(int(row["score"]) for row in rows) / float(max(1, len(rows)))) if rows else 0.0
        risk_band = "low" if max_score <= 20 else ("medium" if max_score <= 60 else ("high" if max_score <= 85 else "critical"))
        return {
            "status": "success",
            "count": len(rows),
            "max_score": int(max_score),
            "avg_score": round(avg_score, 6),
            "risk_band": risk_band,
            "items": rows,
        }

    def runtime_snapshot(self, *, limit: int = 200) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 5000))
        action_rows = []
        for action, row in self._adaptive_action_state.items():
            action_rows.append(
                {
                    "action": action,
                    "samples": int(row.get("samples", 0.0) or 0.0),
                    "failure_ema": round(float(row.get("failure_ema", 0.0) or 0.0), 6),
                    "severe_ema": round(float(row.get("severe_ema", 0.0) or 0.0), 6),
                    "last_updated": float(row.get("last_updated", 0.0) or 0.0),
                }
            )
        action_rows.sort(key=lambda item: (-(item["failure_ema"]), -(item["samples"]), item["action"]))
        source_rows = []
        for source, row in self._adaptive_source_state.items():
            source_rows.append(
                {
                    "source": source,
                    "samples": int(row.get("samples", 0.0) or 0.0),
                    "failure_ema": round(float(row.get("failure_ema", 0.0) or 0.0), 6),
                    "severe_ema": round(float(row.get("severe_ema", 0.0) or 0.0), 6),
                    "last_updated": float(row.get("last_updated", 0.0) or 0.0),
                }
            )
        source_rows.sort(key=lambda item: (-(item["failure_ema"]), -(item["samples"]), item["source"]))
        profile_rows = []
        for profile, row in self._adaptive_profile_state.items():
            profile_rows.append(
                {
                    "profile": profile,
                    "samples": int(row.get("samples", 0.0) or 0.0),
                    "failure_ema": round(float(row.get("failure_ema", 0.0) or 0.0), 6),
                    "severe_ema": round(float(row.get("severe_ema", 0.0) or 0.0), 6),
                    "last_updated": float(row.get("last_updated", 0.0) or 0.0),
                }
            )
        profile_rows.sort(key=lambda item: (-(item["failure_ema"]), -(item["samples"]), item["profile"]))
        mission = self._mission_feedback_state
        return {
            "status": "success",
            "action_count": len(action_rows),
            "source_count": len(source_rows),
            "profile_count": len(profile_rows),
            "actions": action_rows[:bounded],
            "sources": source_rows[:bounded],
            "profiles": profile_rows[:bounded],
            "mission_feedback": {
                "pressure_ema": round(float(mission.get("pressure_ema", 0.0) or 0.0), 6),
                "risk_ema": round(float(mission.get("risk_ema", 0.0) or 0.0), 6),
                "quality_ema": round(float(mission.get("quality_ema", 0.5) or 0.5), 6),
                "failed_ratio_ema": round(float(mission.get("failed_ratio_ema", 0.0) or 0.0), 6),
                "blocked_ratio_ema": round(float(mission.get("blocked_ratio_ema", 0.0) or 0.0), 6),
                "updated_at": float(mission.get("updated_at", 0.0) or 0.0),
                "last_reason": str(mission.get("last_reason", "") or ""),
            },
        }

    def _adaptive_risk_delta(self, action: str, source: str) -> tuple[int, List[str]]:
        factors: List[str] = []
        row = self._adaptive_action_state.get(action, {})
        source_row = self._adaptive_source_state.get(source, {}) if source else {}
        samples = int(row.get("samples", 0.0) or 0.0)
        if samples < 4:
            return 0, factors

        failure_ema = max(0.0, min(float(row.get("failure_ema", 0.0) or 0.0), 1.0))
        severe_ema = max(0.0, min(float(row.get("severe_ema", 0.0) or 0.0), 1.0))
        source_failure_ema = max(0.0, min(float(source_row.get("failure_ema", 0.0) or 0.0), 1.0))
        source_severe_ema = max(0.0, min(float(source_row.get("severe_ema", 0.0) or 0.0), 1.0))

        delta = int(round((failure_ema * 12.0) + (severe_ema * 14.0) + (source_failure_ema * 5.0) + (source_severe_ema * 7.0)))
        base_risk = int(self._risk_table.get(action, 50))
        max_delta = 24
        if base_risk >= 70:
            # Keep adaptive adjustment bounded for already-high-risk actions
            # so guardrail policy remains the primary blocker.
            max_delta = 5
        delta = max(-12, min(delta, max_delta))
        if delta > 0:
            factors.append(f"adaptive_failure_drift:+{delta}")
        return delta, factors

    def _adaptive_profile_delta(self, profile_name: str) -> tuple[int, List[str]]:
        clean_profile = str(profile_name or "").strip().lower()
        if not clean_profile:
            return 0, []
        row = self._adaptive_profile_state.get(clean_profile, {})
        samples = int(row.get("samples", 0.0) or 0.0)
        if samples < 4:
            return 0, []
        failure_ema = max(0.0, min(float(row.get("failure_ema", 0.0) or 0.0), 1.0))
        severe_ema = max(0.0, min(float(row.get("severe_ema", 0.0) or 0.0), 1.0))
        delta = int(round((failure_ema * 8.0) + (severe_ema * 10.0)))
        delta = max(0, min(delta, 16))
        if delta <= 0:
            return 0, []
        return delta, [f"adaptive_profile_drift({clean_profile}):+{delta}"]

    def _mission_pressure(self, metadata: Dict[str, Any]) -> float:
        payload = metadata if isinstance(metadata, dict) else {}
        direct = payload.get("mission_pressure")
        if isinstance(direct, (int, float)):
            return self._coerce_float(direct, minimum=0.0, maximum=1.0, default=0.0)

        nested = payload.get("mission_feedback")
        if isinstance(nested, dict):
            pressure = nested.get("pressure")
            if isinstance(pressure, (int, float)):
                return self._coerce_float(pressure, minimum=0.0, maximum=1.0, default=0.0)
            trend = nested.get("trend_pressure")
            if isinstance(trend, (int, float)):
                return self._coerce_float(trend, minimum=0.0, maximum=1.0, default=0.0)

        state_pressure = self._coerce_float(self._mission_feedback_state.get("pressure_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        risk_pressure = self._coerce_float(self._mission_feedback_state.get("risk_ema", 0.0), minimum=0.0, maximum=1.0, default=0.0)
        quality_relief = 1.0 - self._coerce_float(self._mission_feedback_state.get("quality_ema", 0.5), minimum=0.0, maximum=1.0, default=0.5)
        return self._coerce_float((state_pressure * 0.62) + (risk_pressure * 0.25) + (quality_relief * 0.13), minimum=0.0, maximum=1.0, default=0.0)

    @staticmethod
    def _coerce_float(value: Any, *, minimum: float, maximum: float, default: float) -> float:
        try:
            parsed = float(value)
        except Exception:
            parsed = float(default)
        return max(float(minimum), min(float(maximum), parsed))

    @staticmethod
    def _is_system_path(raw_path: Any) -> bool:
        text = str(raw_path or "").strip()
        if not text:
            return False
        lowered = text.lower().replace("/", "\\")
        if lowered.startswith(("c:\\windows", "c:\\program files", "c:\\programdata")):
            return True
        try:
            path = Path(text).expanduser().resolve()
        except Exception:
            return False
        probe = str(path).lower().replace("/", "\\")
        return probe.startswith(("c:\\windows", "c:\\program files", "c:\\programdata"))
