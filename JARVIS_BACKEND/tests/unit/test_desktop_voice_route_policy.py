from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

from backend.python.desktop_api import DesktopBackendService
from backend.python.utils.logger import Logger


class _FakeTelemetry:
    def __init__(self) -> None:
        self.events: List[Tuple[str, Dict[str, Any]]] = []

    def emit(self, event: str, payload: Dict[str, Any]) -> None:
        self.events.append((event, payload))


class _StubVoicePolicySession:
    def __init__(self) -> None:
        self.restored_restart_snapshot: Dict[str, Any] | None = None

    def route_policy_status(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        del force_refresh
        return {"status": "success"}

    def route_policy_timeline(self, *, limit: int = 80) -> Dict[str, Any]:
        del limit
        current = {
            "stt": {
                "task": "stt",
                "status": "rerouted",
                "selected_provider": "local",
                "recommended_provider": "groq",
                "route_adjusted": True,
                "route_blocked": False,
                "blacklisted": True,
                "recovery_pending": True,
                "cooldown_hint_s": 30,
                "next_retry_at": "2026-03-08T10:03:00+00:00",
                "reason_code": "local_launch_template_blacklisted",
                "reason": "Local STT path rerouted to Groq.",
            },
            "wakeword": {
                "task": "wakeword",
                "status": "recovery",
                "selected_provider": "local",
                "recommended_provider": "local",
                "route_adjusted": False,
                "route_blocked": False,
                "blacklisted": True,
                "recovery_pending": True,
                "cooldown_hint_s": 60,
                "next_retry_at": "2026-03-08T10:03:30+00:00",
                "reason_code": "local_launch_template_blacklisted",
                "reason": "Wakeword cooling down.",
            },
            "tts": {
                "task": "tts",
                "status": "stable",
                "selected_provider": "elevenlabs",
                "recommended_provider": "elevenlabs",
                "route_adjusted": False,
                "route_blocked": False,
            },
            "summary": {
                "status": "recovery",
                "reason_code": "local_launch_template_blacklisted",
                "reason": "Wakeword cooling down.",
                "next_retry_at": "2026-03-08T10:03:30+00:00",
            },
            "route_bundle": {},
        }
        return {
            "status": "success",
            "count": 3,
            "items": [
                {
                    "event_id": "voice-route-1",
                    "occurred_at": "2026-03-08T10:00:00+00:00",
                    "source": "provider",
                    "task": "stt",
                    "status": "rerouted",
                    "previous_status": "stable",
                    "selected_provider": "local",
                    "recommended_provider": "groq",
                    "route_adjusted": True,
                    "route_blocked": False,
                    "blacklisted": True,
                    "recovery_pending": True,
                    "cooldown_hint_s": 30,
                    "next_retry_at": "2026-03-08T10:03:00+00:00",
                    "reason_code": "local_launch_template_blacklisted",
                    "reason": "Local STT path rerouted to Groq.",
                },
                {
                    "event_id": "voice-route-2",
                    "occurred_at": "2026-03-08T10:01:00+00:00",
                    "source": "provider",
                    "task": "wakeword",
                    "status": "recovery",
                    "previous_status": "stable",
                    "selected_provider": "local",
                    "recommended_provider": "local",
                    "route_adjusted": False,
                    "route_blocked": False,
                    "blacklisted": True,
                    "recovery_pending": True,
                    "cooldown_hint_s": 60,
                    "next_retry_at": "2026-03-08T10:03:30+00:00",
                    "reason_code": "local_launch_template_blacklisted",
                    "reason": "Wakeword cooling down.",
                },
                {
                    "event_id": "voice-route-3",
                    "occurred_at": "2026-03-08T10:04:00+00:00",
                    "source": "provider",
                    "task": "wakeword",
                    "status": "stable",
                    "previous_status": "recovery",
                    "selected_provider": "local",
                    "recommended_provider": "local",
                    "route_adjusted": False,
                    "route_blocked": False,
                    "blacklisted": False,
                    "recovery_pending": False,
                    "cooldown_hint_s": 0,
                    "next_retry_at": "",
                    "reason_code": "",
                    "reason": "",
                },
            ],
            "current": current,
        }

    def status(self) -> Dict[str, Any]:
        return {
            "running": True,
            "transcription_count": 0,
            "wakeword_status": "gated:local_launch_template_blacklisted",
            "route_policy": self.route_policy_timeline()["current"]["stt"],
            "wakeword_route_policy": self.route_policy_timeline()["current"]["wakeword"],
            "tts_route_policy": self.route_policy_timeline()["current"]["tts"],
            "route_policy_summary": self.route_policy_timeline()["current"]["summary"],
        }

    def supervision_status(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        del force_refresh
        return {
            "mission_id": "mission-voice-guard",
            "risk_level": "medium",
            "policy_profile": "balanced",
            "wakeword_supervision": {
                "status": "hybrid_polling",
                "strategy": "hybrid_polling",
                "allow_wakeword": False,
                "restart_delay_s": 12.0,
                "next_retry_at": "2026-03-08T10:03:30+00:00",
                "reason_code": "mission_reliability_hybrid_polling",
                "reason": "Mission recovery history prefers hybrid polling.",
            },
        }

    def wakeword_supervision_timeline(self, *, limit: int = 60) -> Dict[str, Any]:
        del limit
        return {
            "status": "success",
            "count": 3,
            "items": [
                {
                    "event_id": "wakeword-supervision-1",
                    "occurred_at": "2026-03-08T10:00:30+00:00",
                    "mission_id": "mission-voice-guard",
                    "risk_level": "medium",
                    "policy_profile": "balanced",
                    "status": "recovery",
                    "previous_status": "active",
                    "reason_code": "local_launch_template_blacklisted",
                    "reason": "Wakeword route cooling down.",
                    "strategy": "hybrid_polling",
                    "allow_wakeword": False,
                    "restart_delay_s": 12.0,
                    "next_retry_at": "2026-03-08T10:03:30+00:00",
                    "fallback_interval_s": 2.2,
                    "resume_stability_s": 1.1,
                    "local_voice_pressure_score": 0.72,
                    "mission_sessions": 5,
                    "wakeword_gate_events": 4,
                    "route_policy_pause_count": 4,
                    "route_policy_resume_count": 2,
                    "recovered": False,
                },
                {
                    "event_id": "wakeword-supervision-2",
                    "occurred_at": "2026-03-08T10:02:30+00:00",
                    "mission_id": "mission-voice-guard",
                    "risk_level": "medium",
                    "policy_profile": "balanced",
                    "status": "hybrid_polling",
                    "previous_status": "recovery",
                    "reason_code": "mission_reliability_hybrid_polling",
                    "reason": "Mission recovery history prefers hybrid polling.",
                    "strategy": "hybrid_polling",
                    "allow_wakeword": False,
                    "restart_delay_s": 8.0,
                    "next_retry_at": "2026-03-08T10:04:00+00:00",
                    "fallback_interval_s": 1.6,
                    "resume_stability_s": 0.9,
                    "local_voice_pressure_score": 0.69,
                    "mission_sessions": 5,
                    "wakeword_gate_events": 4,
                    "route_policy_pause_count": 4,
                    "route_policy_resume_count": 2,
                    "recovered": False,
                },
                {
                    "event_id": "wakeword-supervision-3",
                    "occurred_at": "2026-03-08T10:05:00+00:00",
                    "mission_id": "mission-voice-guard",
                    "risk_level": "medium",
                    "policy_profile": "balanced",
                    "status": "active",
                    "previous_status": "hybrid_polling",
                    "reason_code": "",
                    "reason": "",
                    "strategy": "wakeword",
                    "allow_wakeword": True,
                    "restart_delay_s": 0.0,
                    "next_retry_at": "",
                    "fallback_interval_s": 0.0,
                    "resume_stability_s": 0.8,
                    "local_voice_pressure_score": 0.33,
                    "mission_sessions": 5,
                    "wakeword_gate_events": 4,
                    "route_policy_pause_count": 4,
                    "route_policy_resume_count": 3,
                    "recovered": True,
                },
            ],
            "current": {
                "status": "active",
                "strategy": "wakeword",
                "allow_wakeword": True,
                "restart_delay_s": 0.0,
                "next_retry_at": "",
                "reason_code": "",
                "reason": "",
                "resume_stability_s": 0.8,
            },
            "diagnostics": {
                "status_counts": {"recovery": 1, "hybrid_polling": 1, "active": 1},
                "strategy_counts": {"hybrid_polling": 2, "wakeword": 1},
                "recovered_events": 1,
                "deferred_events": 2,
                "latest_event_at": "2026-03-08T10:05:00+00:00",
                "latest_next_retry_at": "2026-03-08T10:04:00+00:00",
                "latest_active_at": "2026-03-08T10:05:00+00:00",
                "latest_pause_at": "2026-03-08T10:02:30+00:00",
                "avg_restart_delay_s": 10.0,
                "timeline_buckets": [
                    {
                        "bucket_start": "2026-03-08T10:00:00+00:00",
                        "count": 3,
                        "active_count": 1,
                        "paused_count": 2,
                        "recovered_count": 1,
                    }
                ],
            },
        }

    def wakeword_restart_timeline(self, *, limit: int = 80) -> Dict[str, Any]:
        del limit
        return {
            "status": "success",
            "count": 4,
            "items": [
                {
                    "event_id": "wakeword-restart-1",
                    "occurred_at": "2026-03-08T10:00:15+00:00",
                    "event_type": "start_failed",
                    "status": "degraded:wakeword bootstrap failed",
                    "reason_code": "wakeword_start_failed",
                    "reason": "wakeword bootstrap failed",
                    "restart_delay_s": 6.0,
                    "next_retry_at": "2026-03-08T10:00:21+00:00",
                    "failure_count": 1,
                    "wakeword_sensitivity": 0.58,
                    "fallback_interval_s": 2.6,
                    "resume_stability_s": 1.1,
                    "polling_bias": 0.32,
                    "recovered": False,
                    "exhausted": False,
                    "policy": {
                        "recent_failures": 1,
                        "recent_successes": 0,
                        "consecutive_failures": 1,
                        "max_failures_before_polling": 3,
                        "cooldown_scale": 1.8,
                        "recommended_fallback_interval_s": 2.6,
                        "exhausted": False,
                    },
                },
                {
                    "event_id": "wakeword-restart-2",
                    "occurred_at": "2026-03-08T10:00:15+00:00",
                    "event_type": "restart_backoff",
                    "status": "degraded:wakeword bootstrap failed",
                    "reason_code": "wakeword_start_failed",
                    "reason": "wakeword bootstrap failed",
                    "restart_delay_s": 6.0,
                    "next_retry_at": "2026-03-08T10:00:21+00:00",
                    "failure_count": 1,
                    "wakeword_sensitivity": 0.58,
                    "fallback_interval_s": 2.6,
                    "resume_stability_s": 1.1,
                    "polling_bias": 0.32,
                    "recovered": False,
                    "exhausted": False,
                    "policy": {
                        "recent_failures": 1,
                        "recent_successes": 0,
                        "consecutive_failures": 1,
                        "max_failures_before_polling": 3,
                        "cooldown_scale": 1.8,
                        "recommended_fallback_interval_s": 2.6,
                        "exhausted": False,
                    },
                },
                {
                    "event_id": "wakeword-restart-3",
                    "occurred_at": "2026-03-08T10:00:21+00:00",
                    "event_type": "recovery_window_elapsed",
                    "status": "recovery:mission_recovery_policy",
                    "reason_code": "wakeword_start_failed",
                    "reason": "Wakeword restart recovery window elapsed.",
                    "restart_delay_s": 0.0,
                    "next_retry_at": "2026-03-08T10:00:21+00:00",
                    "failure_count": 0,
                    "wakeword_sensitivity": 0.6,
                    "fallback_interval_s": 2.4,
                    "resume_stability_s": 0.95,
                    "polling_bias": 0.26,
                    "recovered": True,
                    "exhausted": False,
                    "policy": {
                        "recent_failures": 1,
                        "recent_successes": 1,
                        "consecutive_failures": 0,
                        "max_failures_before_polling": 3,
                        "cooldown_scale": 1.4,
                        "recommended_fallback_interval_s": 2.4,
                        "exhausted": False,
                    },
                },
                {
                    "event_id": "wakeword-restart-4",
                    "occurred_at": "2026-03-08T10:00:22+00:00",
                    "event_type": "started",
                    "status": "active",
                    "reason_code": "",
                    "reason": "",
                    "restart_delay_s": 0.0,
                    "next_retry_at": "",
                    "failure_count": 0,
                    "wakeword_sensitivity": 0.62,
                    "fallback_interval_s": 2.0,
                    "resume_stability_s": 0.8,
                    "polling_bias": 0.2,
                    "recovered": True,
                    "exhausted": False,
                    "policy": {
                        "recent_failures": 1,
                        "recent_successes": 2,
                        "consecutive_failures": 0,
                        "max_failures_before_polling": 4,
                        "cooldown_scale": 1.2,
                        "recommended_fallback_interval_s": 2.0,
                        "exhausted": False,
                    },
                },
            ],
            "current": {
                "recent_failures": 1,
                "recent_successes": 2,
                "consecutive_failures": 0,
                "max_failures_before_polling": 4,
                "cooldown_scale": 1.2,
                "recommended_fallback_interval_s": 2.0,
                "recommended_resume_stability_s": 0.8,
                "polling_bias": 0.2,
                "exhausted": False,
            },
            "diagnostics": {
                "event_counts": {
                    "start_failed": 1,
                    "restart_backoff": 1,
                    "recovery_window_elapsed": 1,
                    "started": 1,
                },
                "exhausted_events": 0,
                "recovered_events": 2,
                "latest_event_at": "2026-03-08T10:00:22+00:00",
                "latest_next_retry_at": "2026-03-08T10:00:21+00:00",
                "avg_restart_delay_s": 6.0,
                "timeline_buckets": [
                    {
                        "bucket_start": "2026-03-08T10:00:00+00:00",
                        "count": 4,
                        "failure_count": 2,
                        "recovered_count": 2,
                        "exhausted_count": 0,
                    }
                ],
            },
        }

    def restore_wakeword_restart_snapshot(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.restored_restart_snapshot = dict(payload)
        return {
            "status": "success",
            "restored": len(payload.get("items", [])) if isinstance(payload.get("items", []), list) else 0,
        }


def _build_service(tmp_path: Path) -> tuple[DesktopBackendService, _FakeTelemetry]:
    service = DesktopBackendService.__new__(DesktopBackendService)
    service.log = Logger.get_logger("DesktopVoiceRoutePolicyTest")
    telemetry = _FakeTelemetry()
    service.kernel = SimpleNamespace(telemetry=telemetry)
    service._voice_session = _StubVoicePolicySession()
    service._initialize_voice_stack = lambda: None
    service._voice_route_policy_history_lock = threading.RLock()
    service._voice_route_policy_history = []
    service._voice_route_policy_history_loaded = False
    service._voice_route_policy_history_dirty = False
    service._voice_route_policy_history_max = 120
    service._voice_route_policy_history_path = str(tmp_path / "voice_route_policy_history.jsonl")
    service._wakeword_supervision_history_lock = threading.RLock()
    service._wakeword_supervision_history = []
    service._wakeword_supervision_history_loaded = False
    service._wakeword_supervision_history_dirty = False
    service._wakeword_supervision_history_max = 120
    service._wakeword_supervision_history_path = str(tmp_path / "wakeword_supervision_history.jsonl")
    service._wakeword_restart_history_lock = threading.RLock()
    service._wakeword_restart_history = []
    service._wakeword_restart_history_loaded = False
    service._wakeword_restart_history_dirty = False
    service._wakeword_restart_history_max = 120
    service._wakeword_restart_history_path = str(tmp_path / "wakeword_restart_history.jsonl")
    service._wakeword_restart_policy_history_lock = threading.RLock()
    service._wakeword_restart_policy_history = []
    service._wakeword_restart_policy_history_loaded = False
    service._wakeword_restart_policy_history_dirty = False
    service._wakeword_restart_policy_history_max = 120
    service._wakeword_restart_policy_history_path = str(tmp_path / "wakeword_restart_policy_history.jsonl")
    service._voice_route_policy_recovery_wait_s = 0.5
    service._voice_route_policy_resume_stability_s = 0.02
    service._voice_adaptive_lock = threading.RLock()
    service._voice_mission_reliability_by_id = {}
    service._extract_mission_id = lambda metadata: str(
        (metadata or {}).get("__jarvis_mission_id", "") or (metadata or {}).get("mission_id", "")
    ).strip()
    service._voice_continuous_lock = threading.RLock()
    service._voice_continuous_runs = {}
    service._voice_continuous_max_runs = 20
    return service, telemetry


def test_voice_route_policy_history_records_and_aggregates_events(tmp_path: Path) -> None:
    service, telemetry = _build_service(tmp_path)

    payload = service.voice_route_policy_history(limit=20, refresh=True)

    assert payload["status"] == "success"
    assert int(payload.get("count", 0) or 0) == 3
    diagnostics = payload.get("diagnostics", {})
    assert diagnostics.get("rerouted_events") == 1
    assert diagnostics.get("recovered_events") == 1
    assert diagnostics.get("blacklisted_events") == 2
    assert isinstance(diagnostics.get("timeline_buckets", []), list)
    assert Path(payload.get("history_path", "")).exists()
    assert any(event == "voice.route_policy_history_recorded" for event, _ in telemetry.events)


def test_wakeword_supervision_history_records_and_aggregates_events(tmp_path: Path) -> None:
    service, telemetry = _build_service(tmp_path)

    payload = service.wakeword_supervision_history(limit=20, refresh=True)

    assert payload["status"] == "success"
    assert int(payload.get("count", 0) or 0) == 3
    diagnostics = payload.get("diagnostics", {})
    assert diagnostics.get("recovered_events") == 1
    assert diagnostics.get("deferred_events") == 2
    assert isinstance(diagnostics.get("timeline_buckets", []), list)
    assert Path(payload.get("history_path", "")).exists()
    assert any(event == "voice.wakeword_supervision_history_recorded" for event, _ in telemetry.events)


def test_wakeword_restart_history_records_and_aggregates_events(tmp_path: Path) -> None:
    service, telemetry = _build_service(tmp_path)

    payload = service.wakeword_restart_history(limit=20, refresh=True)

    assert payload["status"] == "success"
    assert int(payload.get("count", 0) or 0) == 4
    diagnostics = payload.get("diagnostics", {})
    assert int(diagnostics.get("event_counts", {}).get("restart_backoff", 0) or 0) == 1
    assert diagnostics.get("recovered_events") == 2
    assert isinstance(diagnostics.get("timeline_buckets", []), list)
    assert Path(payload.get("history_path", "")).exists()
    assert any(event == "voice.wakeword_restart_policy_history_recorded" for event, _ in telemetry.events)


def test_wakeword_restart_policy_history_records_and_aggregates_events(tmp_path: Path) -> None:
    service, _telemetry = _build_service(tmp_path)

    payload = service.wakeword_restart_policy_history(limit=20, refresh=True)

    assert payload["status"] == "success"
    assert int(payload.get("count", 0) or 0) == 4
    current = payload.get("current", {})
    diagnostics = payload.get("diagnostics", {})
    assert isinstance(current, dict)
    assert int(current.get("max_failures_before_polling", 0) or 0) >= 3
    assert float(current.get("fallback_interval_s", 0.0) or 0.0) > 0.0
    assert float(current.get("resume_stability_s", 0.0) or 0.0) > 0.0
    assert "avg_threshold_bias" in diagnostics
    assert "avg_cooldown_scale" in diagnostics
    assert "drift_score" in diagnostics
    assert diagnostics.get("recommended_profile") in {"balanced_recovery", "hybrid_guarded", "stability_guard", "recovered_wakeword"}
    assert str(diagnostics.get("applied_profile", "")).strip() in {
        "balanced_recovery",
        "hybrid_guarded",
        "stability_guard",
        "recovered_wakeword",
    }
    assert str(diagnostics.get("profile_decision_source", "")).strip()
    assert isinstance(diagnostics.get("profile_timeline", []), list)
    assert isinstance(diagnostics.get("profile_shift_timeline", []), list)
    assert all(
        isinstance(item, dict) and str(item.get("recommended_profile", "")).strip()
        for item in diagnostics.get("profile_timeline", [])
    )
    assert isinstance(diagnostics.get("timeline_buckets", []), list)
    assert all(
        isinstance(item, dict) and "recommended_profile" in item
        for item in diagnostics.get("timeline_buckets", [])
    )
    assert Path(payload.get("history_path", "")).exists()


def test_restore_voice_session_wakeword_restart_state_uses_persisted_history(tmp_path: Path) -> None:
    service, _telemetry = _build_service(tmp_path)
    service.wakeword_restart_history(limit=20, refresh=True)
    service.wakeword_restart_policy_history(limit=20, refresh=True)

    result = service._restore_voice_session_wakeword_restart_state()  # noqa: SLF001
    restored = service._voice_session.restored_restart_snapshot if isinstance(service._voice_session, _StubVoicePolicySession) else None

    assert result.get("status") == "success"
    assert int(result.get("restored", 0) or 0) >= 1
    assert isinstance(restored, dict)
    assert int(restored.get("count", 0) or 0) >= 1
    current = restored.get("current", {}) if isinstance(restored.get("current", {}), dict) else {}
    assert int(current.get("recovery_expiry_count", 0) or 0) >= 1
    assert str(current.get("next_retry_at", "")).strip()
    assert float(current.get("fallback_interval_s", 0.0) or 0.0) > 0.0
    assert float(current.get("resume_stability_s", 0.0) or 0.0) > 0.0
    assert str(current.get("recommended_profile", "")).strip() in {
        "balanced_recovery",
        "hybrid_guarded",
        "stability_guard",
        "recovered_wakeword",
    }
    assert str(current.get("applied_profile", "")).strip() in {
        "balanced_recovery",
        "hybrid_guarded",
        "stability_guard",
        "recovered_wakeword",
    }
    assert str(current.get("profile_decision_source", "")).strip()
    policy = current.get("policy", {}) if isinstance(current.get("policy", {}), dict) else {}
    assert str(policy.get("recommended_profile", "")).strip() == str(current.get("recommended_profile", "")).strip()
    assert str(policy.get("applied_profile", "")).strip() == str(current.get("applied_profile", "")).strip()
    assert "drift_score" in current


def test_adaptive_voice_session_config_applies_cross_session_wakeword_runtime_posture(tmp_path: Path) -> None:
    service, _telemetry = _build_service(tmp_path)
    service.wakeword_restart_policy_history = lambda limit=48, refresh=False: {
        "status": "success",
        "current": {
            "applied_profile": "stability_guard",
            "profile_action": "demote",
            "auto_profile_applied": True,
            "last_profile_shift_at": "2026-03-08T10:07:00+00:00",
            "drift_score": 0.72,
            "fallback_interval_s": 2.6,
            "resume_stability_s": 1.6,
            "wakeword_sensitivity": 0.52,
            "restart_delay_s": 12.0,
            "runtime_posture": {
                "runtime_mode": "stability_guard",
                "wakeword_supervision_mode": "stability_guard",
                "continuous_resume_mode": "tight_guard",
                "barge_in_enabled": True,
                "hard_barge_in": False,
                "session_overrides": {
                    "fallback_interval_s": 2.6,
                    "route_policy_resume_stability_s": 1.8,
                    "max_route_policy_pauses": 2,
                    "max_route_policy_pause_total_s": 42.0,
                },
                "wakeword_tuning": {
                    "strategy": "recovery_guard",
                    "wakeword_sensitivity": 0.48,
                    "restart_delay_s": 12.0,
                    "resume_stability_s": 1.8,
                    "fallback_interval_s": 2.6,
                    "polling_bias": 0.78,
                },
                "reasons": ["Stability-guard posture tightened live wakeword recovery behavior."],
            },
        },
        "diagnostics": {},
    }

    config = service._adaptive_voice_session_config(  # noqa: SLF001
        {
            "metadata": {
                "__jarvis_mission_id": "mission-voice-profile-runtime",
                "risk_level": "medium",
                "policy_profile": "balanced",
            }
        }
    )

    metadata = config.get("metadata", {}) if isinstance(config.get("metadata", {}), dict) else {}
    assert metadata.get("wakeword_profile_runtime_mode") == "stability_guard"
    assert metadata.get("wakeword_supervision_mode") == "stability_guard"
    assert metadata.get("continuous_resume_mode") == "tight_guard"
    assert bool(config.get("barge_in_enabled", True)) is True
    assert bool(config.get("hard_barge_in", True)) is False
    assert float(config.get("route_policy_resume_stability_s", 0.0) or 0.0) >= 1.8
    assert int(config.get("max_route_policy_pauses", 0) or 0) <= 2


def test_run_voice_session_continuous_pauses_and_resumes_on_route_policy_recovery(tmp_path: Path) -> None:
    service, telemetry = _build_service(tmp_path)
    service._voice_session = object()
    service._initialize_voice_stack = lambda: None
    service.start_voice_session = lambda config=None: {"status": "success", "voice": {"running": True, "config": config or {}}}
    service.stop_voice_session = lambda: {"status": "success", "voice": {"running": False}}

    calls = {"count": 0}

    def _status() -> Dict[str, Any]:
        calls["count"] += 1
        if calls["count"] <= 2:
            return {
                "available": True,
                "running": True,
                "transcription_count": 0,
                "wakeword_status": "active",
                "route_policy": {
                    "task": "stt",
                    "status": "blocked",
                    "route_blocked": True,
                    "reason_code": "local_launch_template_blacklisted",
                    "reason": "Local STT route blocked by launcher policy.",
                    "cooldown_hint_s": 0.01,
                    "next_retry_at": "",
                },
                "wakeword_route_policy": {"task": "wakeword", "status": "stable"},
                "tts_route_policy": {"task": "tts", "status": "stable"},
                "route_policy_summary": {"status": "blocked"},
            }
        if calls["count"] == 3:
            return {
                "available": True,
                "running": True,
                "transcription_count": 0,
                "wakeword_status": "active",
                "route_policy": {"task": "stt", "status": "stable", "route_blocked": False},
                "wakeword_route_policy": {"task": "wakeword", "status": "stable"},
                "tts_route_policy": {"task": "tts", "status": "stable"},
                "route_policy_summary": {"status": "stable"},
            }
        return {
            "available": True,
            "running": True,
            "transcription_count": 1,
            "last_trigger_type": "polling",
            "last_trigger_at": "2026-03-08T10:10:00+00:00",
            "last_transcript": "hello jarvis",
            "last_reply": "online",
            "wakeword_status": "active",
            "route_policy": {"task": "stt", "status": "stable", "route_blocked": False},
            "wakeword_route_policy": {"task": "wakeword", "status": "stable"},
            "tts_route_policy": {"task": "tts", "status": "stable"},
            "route_policy_summary": {"status": "stable"},
        }

    service.get_voice_session_status = _status

    payload = service.run_voice_session_continuous(
        duration_s=3.0,
        max_turns=1,
        stop_on_idle_s=0.0,
        stop_after=True,
        config={
            "wakeword_enabled": False,
            "route_policy_recovery_wait_s": 0.4,
            "route_policy_resume_stability_s": 0.01,
        },
        _session_id="voice-cont-unit",
    )

    assert payload["status"] == "success"
    assert payload["end_reason"] == "max_turns"
    assert int(payload.get("captured_turns", 0) or 0) == 1
    assert int(payload.get("route_policy_pause_count", 0) or 0) == 1
    assert int(payload.get("route_policy_resume_count", 0) or 0) == 1
    assert float(payload.get("route_policy_pause_total_s", 0.0) or 0.0) > 0.0
    assert isinstance(payload.get("route_policy_pause_events", []), list)
    assert any(event == "voice.continuous.route_paused" for event, _ in telemetry.events)
    assert any(event == "voice.continuous.route_resumed" for event, _ in telemetry.events)


def test_run_voice_session_continuous_exhausts_resume_budget_for_unstable_mission(tmp_path: Path) -> None:
    service, _telemetry = _build_service(tmp_path)
    service._voice_session = object()
    service._initialize_voice_stack = lambda: None
    service.start_voice_session = lambda config=None: {"status": "success", "voice": {"running": True, "config": config or {}}}
    service.stop_voice_session = lambda: {"status": "success", "voice": {"running": False}}
    service._voice_mission_reliability_by_id["mission-voice-unstable"] = {
        "mission_id": "mission-voice-unstable",
        "sessions": 3,
        "route_policy_pause_count": 4,
        "route_policy_resume_count": 0,
        "wakeword_gate_events": 3,
        "stt_block_events": 2,
        "updated_at": "2026-03-08T10:09:00+00:00",
    }

    def _status() -> Dict[str, Any]:
        return {
            "available": True,
            "running": True,
            "transcription_count": 0,
            "wakeword_status": "gated:local_launch_template_blacklisted",
            "route_policy": {
                "task": "stt",
                "status": "blocked",
                "route_blocked": True,
                "reason_code": "local_launch_template_blacklisted",
                "reason": "Local STT route blocked by launcher policy.",
                "cooldown_hint_s": 0.01,
                "next_retry_at": "",
            },
            "wakeword_route_policy": {
                "task": "wakeword",
                "status": "recovery",
                "route_blocked": False,
            },
            "tts_route_policy": {"task": "tts", "status": "stable"},
            "route_policy_summary": {"status": "blocked"},
        }

    service.get_voice_session_status = _status

    payload = service.run_voice_session_continuous(
        duration_s=3.0,
        max_turns=1,
        stop_on_idle_s=0.0,
        stop_after=True,
        config={
            "metadata": {
                "__jarvis_mission_id": "mission-voice-unstable",
                "risk_level": "high",
                "policy_profile": "automation_safe",
            },
            "wakeword_enabled": False,
            "route_policy_recovery_wait_s": 0.4,
            "route_policy_resume_stability_s": 0.01,
            "max_route_policy_pauses": 1,
        },
        _session_id="voice-cont-exhausted",
    )

    assert payload["status"] == "success"
    assert payload["end_reason"] == "route_policy_recovery_exhausted"
    decision = payload.get("route_policy_recovery_decision", {})
    assert decision.get("resume_allowed") is False
    assert str(payload.get("route_policy_end_reason", "")).strip().lower() in {
        "pause_budget_exhausted",
        "mission_recovery_score_too_low",
    }


def test_voice_continuous_recovery_decision_uses_applied_wakeword_profile(tmp_path: Path) -> None:
    service, _telemetry = _build_service(tmp_path)
    service._voice_mission_reliability_by_id["mission-voice-profiled"] = {
        "mission_id": "mission-voice-profiled",
        "sessions": 4,
        "route_policy_pause_count": 2,
        "route_policy_resume_count": 1,
        "wakeword_gate_events": 3,
        "stt_block_events": 1,
        "updated_at": "2026-03-08T10:09:00+00:00",
    }
    service.wakeword_restart_policy_history = lambda limit=48, refresh=False: {
        "status": "success",
        "current": {
            "applied_profile": "stability_guard",
            "profile_action": "demote",
            "auto_profile_applied": True,
            "last_profile_shift_at": "2026-03-08T10:07:00+00:00",
        },
        "diagnostics": {},
    }
    service._voice_route_recovery_recommendation = lambda **_kwargs: {
        "status": "success",
        "recovery_profile": "balanced",
    }

    decision = service._voice_continuous_recovery_decision(  # noqa: SLF001
        session_config={
            "route_policy_recovery_wait_s": 60.0,
            "route_policy_resume_stability_s": 0.75,
            "max_route_policy_pauses": 5,
            "max_route_policy_pause_total_s": 120.0,
        },
        pause_gate={
            "task": "wakeword",
            "policy": {"route_blocked": False},
        },
        route_policy_pause_count=1,
        route_policy_pause_total_s=8.0,
        mission_id="mission-voice-profiled",
        risk_level="medium",
        policy_profile="balanced",
    )

    assert decision["applied_wakeword_profile"] == "stability_guard"
    assert decision["wakeword_profile_action"] == "demote"
    assert decision["wakeword_profile_auto_applied"] is True
    assert float(decision.get("effective_resume_stability_s", 0.0) or 0.0) >= 1.8
    assert int(decision.get("effective_max_pause_count", 0) or 0) <= 2
    assert any("stability-guard" in str(reason).lower() for reason in decision.get("reasons", []))


def test_run_voice_session_continuous_surfaces_applied_wakeword_runtime_modes(tmp_path: Path) -> None:
    service, _telemetry = _build_service(tmp_path)
    service._voice_session = object()
    service._initialize_voice_stack = lambda: None
    started: Dict[str, Any] = {}

    def _start(config=None):
        started["config"] = dict(config or {})
        return {"status": "success", "voice": {"running": True, "config": config or {}}}

    service.start_voice_session = _start
    service.stop_voice_session = lambda: {"status": "success", "voice": {"running": False}}
    service.wakeword_restart_policy_history = lambda limit=48, refresh=False: {
        "status": "success",
        "current": {
            "applied_profile": "recovered_wakeword",
            "profile_action": "recover",
            "auto_profile_applied": True,
            "last_profile_shift_at": "2026-03-08T10:09:00+00:00",
            "runtime_posture": {
                "runtime_mode": "recovered_wakeword",
                "wakeword_supervision_mode": "recovered_wakeword",
                "continuous_resume_mode": "resume_ready",
                "barge_in_enabled": True,
                "hard_barge_in": True,
                "session_overrides": {
                    "fallback_interval_s": 6.0,
                    "route_policy_resume_stability_s": 0.9,
                    "max_route_policy_pauses": 5,
                },
                "wakeword_tuning": {
                    "strategy": "patient_recovery",
                    "wakeword_sensitivity": 0.7,
                    "restart_delay_s": 3.0,
                    "resume_stability_s": 0.9,
                    "fallback_interval_s": 6.0,
                    "polling_bias": 0.18,
                },
                "reasons": ["Recovered-wakeword posture relaxed continuous auto-resume."],
            },
        },
        "diagnostics": {},
    }

    calls = {"count": 0}

    def _status() -> Dict[str, Any]:
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "available": True,
                "running": True,
                "transcription_count": 0,
                "wakeword_status": "active",
                "route_policy": {"task": "stt", "status": "stable", "route_blocked": False},
                "wakeword_route_policy": {"task": "wakeword", "status": "stable"},
                "tts_route_policy": {"task": "tts", "status": "stable"},
                "route_policy_summary": {"status": "stable"},
            }
        return {
            "available": True,
            "running": True,
            "transcription_count": 1,
            "last_trigger_type": "wakeword",
            "last_trigger_at": "2026-03-08T10:10:00+00:00",
            "last_transcript": "resume ready",
            "last_reply": "runtime posture active",
            "wakeword_status": "active",
            "route_policy": {"task": "stt", "status": "stable", "route_blocked": False},
            "wakeword_route_policy": {"task": "wakeword", "status": "stable"},
            "tts_route_policy": {"task": "tts", "status": "stable"},
            "route_policy_summary": {"status": "stable"},
        }

    service.get_voice_session_status = _status

    payload = service.run_voice_session_continuous(
        duration_s=3.0,
        max_turns=1,
        stop_on_idle_s=0.0,
        stop_after=True,
        config={},
        _session_id="voice-cont-runtime-mode",
    )

    metadata = started.get("config", {}).get("metadata", {}) if isinstance(started.get("config", {}).get("metadata", {}), dict) else {}
    assert metadata.get("wakeword_profile_runtime_mode") == "recovered_wakeword"
    assert metadata.get("wakeword_supervision_mode") == "recovered_wakeword"
    assert metadata.get("continuous_resume_mode") == "resume_ready"
    assert payload["wakeword_profile_runtime_mode"] == "recovered_wakeword"
    assert payload["wakeword_supervision_mode"] == "recovered_wakeword"
    assert payload["continuous_resume_mode"] == "resume_ready"
    assert payload["hard_barge_in"] is True
