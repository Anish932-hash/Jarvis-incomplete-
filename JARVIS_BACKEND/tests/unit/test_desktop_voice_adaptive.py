from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any, Dict

from backend.python.desktop_api import DesktopBackendService
from backend.python.utils.logger import Logger


class _StubVoiceSession:
    def __init__(self) -> None:
        self.last_config: Dict[str, Any] = {}

    def start(self, config: Dict[str, Any]) -> Dict[str, Any]:
        self.last_config = dict(config)
        return {"running": True, "config": dict(config)}


def _build_service() -> DesktopBackendService:
    service = DesktopBackendService.__new__(DesktopBackendService)
    service.log = Logger.get_logger("DesktopVoiceAdaptiveTest")
    service._voice_adaptive_learning_enabled = True
    service._voice_adaptive_learning_alpha = 0.3
    service._voice_adaptive_learning_min_samples = 1
    service._voice_adaptive_learning_bad_threshold = 0.4
    service._voice_adaptive_learning_good_threshold = 0.8
    service._voice_adaptive_state_path = ""
    service._voice_adaptive_persist_every = 1000
    service._voice_adaptive_dirty_updates = 0
    service._voice_adaptive_last_loaded_at = ""
    service._voice_adaptive_last_saved_at = ""
    service._voice_adaptive_last_save_error = ""
    service._voice_adaptive_dynamic_profile_by_risk = {}
    service._voice_adaptive_dynamic_guardrails_by_risk = {}
    service._voice_adaptive_dynamic_session_by_risk = {}
    service._voice_adaptive_dynamic_wakeword_by_risk = {}
    service._voice_adaptive_learning_state = {}
    service._voice_mission_reliability_by_id = {}
    service._voice_action_reliability_by_type = {}
    service._voice_adaptive_lock = threading.RLock()
    service._voice_route_policy_history_lock = threading.RLock()
    service._voice_route_policy_history = []
    service._voice_route_policy_history_loaded = True
    service._voice_route_policy_history_dirty = False
    service._voice_route_policy_history_max = 120
    service._voice_route_policy_history_path = ""
    service._voice_route_policy_recovery_wait_s = 90.0
    service._voice_route_policy_resume_stability_s = 0.75
    service._stt_engine = SimpleNamespace(
        diagnostics=lambda history_limit=8: {
            "provider_health": "degraded",
            "fallback_rate_ema": 0.61,
            "history_limit": history_limit,
        }
    )
    service._voice_session = _StubVoiceSession()
    service.kernel = SimpleNamespace(
        telemetry=SimpleNamespace(emit=lambda *_args, **_kwargs: None),
        mission_diagnostics=lambda mission_id, hotspot_limit=4: {
            "mission_id": mission_id,
            "risk": {"level": "high"},
            "quality": {"score": 0.41},
            "hotspot_limit": hotspot_limit,
        },
        get_mission=lambda mission_id: {
            "mission_id": mission_id,
            "metadata": {"policy_profile": "automation_safe"},
        },
    )
    service._mission_autonomy_target = lambda *, risk_level, quality_score: {
        "risk_level": risk_level,
        "quality_score": quality_score,
        "target_policy_profile": "automation_safe",
        "target_rbac_role": "developer",
        "context_opportunity_pressure": 0.2,
    }
    return service


def test_adaptive_voice_session_config_enriches_mission_and_stt_context() -> None:
    service = _build_service()

    config = service._adaptive_voice_session_config({"metadata": {"__jarvis_mission_id": "mission-voice-1"}})  # noqa: SLF001
    metadata = config.get("metadata", {})
    adaptive_context = config.get("__voice_adaptive_context", {})

    assert metadata.get("mission_id") == "mission-voice-1"
    assert metadata.get("risk_level") == "high"
    assert metadata.get("policy_profile") == "automation_safe"
    assert metadata.get("stt_provider_health") == "degraded"
    assert adaptive_context.get("mission_id") == "mission-voice-1"
    recommendation = adaptive_context.get("recommendation", {})
    assert recommendation.get("target_policy_profile") == "automation_safe"


def test_start_voice_session_returns_adaptive_context_payload() -> None:
    service = _build_service()

    result = service.start_voice_session({"metadata": {"__jarvis_mission_id": "mission-voice-2"}})

    assert result.get("status") == "success"
    assert result.get("voice", {}).get("running") is True
    adaptive_context = result.get("adaptive_context", {})
    assert adaptive_context.get("mission_id") == "mission-voice-2"
    assert adaptive_context.get("policy_profile") == "automation_safe"


def test_voice_adaptive_learning_updates_dynamic_profile_and_guardrails() -> None:
    service = _build_service()

    for _ in range(3):
        service._voice_adaptive_record_interaction(  # noqa: SLF001
            metadata={"risk_level": "low", "policy_profile": "automation_power"},
            outcome={"status": "failed"},
        )

    status = service.voice_adaptive_status(limit=50)
    dynamic_profiles = status.get("dynamic_profile_by_risk", {})
    assert dynamic_profiles.get("low") == "strict"
    dynamic_guardrails = status.get("dynamic_guardrails_by_risk", {})
    low_guardrails = dynamic_guardrails.get("low", {})
    assert float(low_guardrails.get("min_confidence_offset", 0.0) or 0.0) > 0.0


def test_adaptive_voice_config_uses_voice_learning_recommendation() -> None:
    service = _build_service()
    service._voice_adaptive_dynamic_profile_by_risk["high"] = "strict"
    service._voice_adaptive_dynamic_guardrails_by_risk["high"] = {
        "min_confidence_offset": 0.08,
        "max_callbacks_scale": 0.7,
        "max_transcriptions_scale": 0.75,
    }

    config = service._adaptive_voice_session_config(  # noqa: SLF001
        {"metadata": {"risk_level": "high", "policy_profile": "automation_safe"}}
    )
    metadata = config.get("metadata", {})
    adaptive_context = config.get("__voice_adaptive_context", {})

    assert metadata.get("voice_profile_hint") == "strict"
    guardrails = metadata.get("voice_guardrail_overrides", {})
    assert float(guardrails.get("min_confidence_offset", 0.0) or 0.0) >= 0.08
    recommendation = adaptive_context.get("voice_adaptive_recommendation", {})
    assert recommendation.get("profile_hint") == "strict"


def test_adaptive_voice_config_applies_session_overrides_before_runtime() -> None:
    service = _build_service()
    service._voice_adaptive_dynamic_session_by_risk["high"] = {
        "risk_level": "high",
        "samples": 4,
        "strategy": "hybrid_polling",
        "session_overrides": {
            "fallback_interval_s": 2.2,
            "route_policy_recovery_wait_s": 42.0,
            "route_policy_resume_stability_s": 1.0,
            "max_route_policy_pauses": 2,
            "max_route_policy_pause_total_s": 54.0,
        },
    }

    config = service._adaptive_voice_session_config(  # noqa: SLF001
        {"metadata": {"risk_level": "high", "policy_profile": "automation_safe"}}
    )

    assert float(config.get("fallback_interval_s", 0.0) or 0.0) == 2.2
    assert float(config.get("route_policy_recovery_wait_s", 0.0) or 0.0) == 42.0
    assert float(config.get("route_policy_resume_stability_s", 0.0) or 0.0) == 1.0
    assert int(config.get("max_route_policy_pauses", 0) or 0) == 2
    assert float(config.get("max_route_policy_pause_total_s", 0.0) or 0.0) == 54.0


def test_adaptive_voice_config_applies_wakeword_tuning_before_runtime() -> None:
    service = _build_service()
    service._voice_adaptive_dynamic_wakeword_by_risk["high"] = {
        "risk_level": "high",
        "policy_profile": "automation_safe",
        "samples": 5,
        "wakeword_tuning": {
            "strategy": "hybrid_polling",
            "wakeword_enabled": True,
            "wakeword_sensitivity": 0.44,
            "restart_delay_s": 12.0,
            "resume_stability_s": 1.1,
            "fallback_interval_s": 2.0,
            "polling_bias": 0.72,
        },
    }

    config = service._adaptive_voice_session_config(  # noqa: SLF001
        {"metadata": {"risk_level": "high", "policy_profile": "automation_safe"}}
    )

    metadata = config.get("metadata", {})
    adaptive_context = config.get("__voice_adaptive_context", {})

    assert float(config.get("wakeword_sensitivity", 0.0) or 0.0) == 0.44
    assert float(config.get("fallback_interval_s", 0.0) or 0.0) == 2.0
    assert metadata.get("voice_wakeword_strategy") == "hybrid_polling"
    assert float(metadata.get("voice_wakeword_tuning", {}).get("restart_delay_s", 0.0) or 0.0) == 12.0
    assert adaptive_context.get("voice_adaptive_recommendation", {}).get("wakeword_tuning", {}).get("strategy") == "hybrid_polling"


def test_voice_route_recovery_recommendation_prefers_hybrid_polling_for_mission_instability() -> None:
    service = _build_service()
    service._voice_route_policy_history = [
        {
            "event_id": "voice-route-1",
            "occurred_at": "2026-03-08T10:00:00+00:00",
            "task": "wakeword",
            "status": "recovery",
            "previous_status": "stable",
            "route_blocked": False,
            "route_adjusted": False,
            "blacklisted": True,
            "recovery_pending": True,
            "cooldown_hint_s": 18.0,
            "next_retry_at": "2026-03-08T10:01:00+00:00",
        },
        {
            "event_id": "voice-route-2",
            "occurred_at": "2026-03-08T10:02:00+00:00",
            "task": "stt",
            "status": "rerouted",
            "previous_status": "blocked",
            "route_blocked": False,
            "route_adjusted": True,
            "blacklisted": True,
            "recovery_pending": True,
            "cooldown_hint_s": 12.0,
            "next_retry_at": "",
        },
    ]
    service._voice_mission_reliability_by_id["mission-voice-3"] = {
        "mission_id": "mission-voice-3",
        "updated_at": "2026-03-08T10:03:00+00:00",
        "sessions": 4,
        "route_policy_pause_count": 3,
        "route_policy_resume_count": 1,
        "wakeword_gate_events": 3,
        "stt_block_events": 1,
    }

    recommendation = service._voice_route_recovery_recommendation(  # noqa: SLF001
        mission_id="mission-voice-3",
        risk_level="medium",
        policy_profile="automation_safe",
    )

    assert recommendation.get("wakeword_strategy") == "hybrid_polling"
    assert recommendation.get("recovery_profile") in {"hybrid_polling", "polling_only"}
    overrides = recommendation.get("session_overrides", {})
    assert float(overrides.get("fallback_interval_s", 99.0) or 99.0) <= 2.4
    assert recommendation.get("mission_reliability", {}).get("mission_id") == "mission-voice-3"


def test_voice_route_recovery_recommendation_includes_adaptive_wakeword_tuning() -> None:
    service = _build_service()
    service._voice_adaptive_dynamic_wakeword_by_risk["medium"] = {
        "risk_level": "medium",
        "policy_profile": "automation_safe",
        "samples": 4,
        "wakeword_tuning": {
            "strategy": "polling_only",
            "wakeword_enabled": False,
            "wakeword_sensitivity": 0.3,
            "restart_delay_s": 24.0,
            "resume_stability_s": 1.4,
            "fallback_interval_s": 1.5,
            "polling_bias": 1.0,
        },
    }

    recommendation = service._voice_route_recovery_recommendation(  # noqa: SLF001
        mission_id="mission-voice-3",
        risk_level="medium",
        policy_profile="automation_safe",
    )

    assert recommendation.get("wakeword_strategy") == "polling_only"
    assert recommendation.get("recovery_profile") == "polling_only"
    assert recommendation.get("adaptive_wakeword_tuning", {}).get("wakeword_enabled") is False
    assert float(recommendation.get("session_overrides", {}).get("fallback_interval_s", 0.0) or 0.0) == 1.5


def test_voice_mission_reliability_recording_updates_persisted_mission_memory() -> None:
    service = _build_service()

    service._voice_adaptive_record_mission_reliability(  # noqa: SLF001
        mission_id="mission-voice-4",
        metadata={
            "voice_profile_hint": "balanced",
            "policy_profile": "automation_safe",
            "risk_level": "medium",
        },
        outcome={
            "end_reason": "max_turns",
            "captured_turns": 2,
            "route_policy_pause_count": 1,
            "route_policy_resume_count": 1,
            "route_policy_pause_total_s": 3.2,
            "route_policy_pause_events": [
                {"task": "wakeword"},
                {"task": "stt"},
            ],
        },
    )

    payload = service.voice_mission_reliability_status(mission_id="mission-voice-4", limit=4)
    current = payload.get("current", {})
    assert payload.get("status") == "success"
    assert current.get("mission_id") == "mission-voice-4"
    assert int(current.get("sessions", 0) or 0) == 1
    assert int(current.get("route_policy_pause_count", 0) or 0) == 1
    assert int(current.get("route_policy_resume_count", 0) or 0) == 1
    assert int(current.get("wakeword_gate_events", 0) or 0) == 1
    assert int(current.get("stt_block_events", 0) or 0) == 1


def test_voice_mission_reliability_recording_updates_session_tuning_and_stt_autotune() -> None:
    service = _build_service()
    recorded_signals: list[Dict[str, Any]] = []
    service._stt_autotune_record_signal = lambda **kwargs: recorded_signals.append(dict(kwargs))

    service._voice_adaptive_record_mission_reliability(  # noqa: SLF001
        mission_id="mission-voice-5",
        metadata={
            "voice_profile_hint": "balanced",
            "policy_profile": "automation_safe",
            "risk_level": "high",
        },
        outcome={
            "end_reason": "route_policy_timeout",
            "captured_turns": 0,
            "route_policy_pause_count": 2,
            "route_policy_resume_count": 0,
            "route_policy_pause_total_s": 14.0,
            "route_policy_pause_events": [
                {"task": "wakeword"},
                {"task": "wakeword"},
                {"task": "stt"},
            ],
        },
    )

    status = service.voice_adaptive_status(limit=20)
    session_state = status.get("dynamic_session_by_risk", {}).get("high", {})
    overrides = session_state.get("session_overrides", {})
    assert session_state.get("strategy") in {"polling_only", "hybrid_polling", "stt_pressure"}
    assert float(session_state.get("ema_pause_pressure", 0.0) or 0.0) > 0.0
    assert float(overrides.get("route_policy_recovery_wait_s", 0.0) or 0.0) > 0.0
    assert int(overrides.get("max_route_policy_pauses", 0) or 0) >= 1
    assert recorded_signals
    assert recorded_signals[-1]["mission_id"] == "mission-voice-5"
    assert recorded_signals[-1]["source"] == "voice-session"


def test_voice_mission_reliability_recording_updates_wakeword_tuning_state() -> None:
    service = _build_service()

    service._voice_adaptive_record_mission_reliability(  # noqa: SLF001
        mission_id="mission-voice-6",
        metadata={
            "voice_profile_hint": "balanced",
            "policy_profile": "automation_safe",
            "risk_level": "high",
        },
        outcome={
            "end_reason": "route_policy_timeout",
            "captured_turns": 0,
            "route_policy_pause_count": 3,
            "route_policy_resume_count": 0,
            "route_policy_pause_total_s": 19.0,
            "route_policy_pause_events": [
                {"task": "wakeword"},
                {"task": "wakeword"},
                {"task": "wakeword"},
                {"task": "stt"},
            ],
        },
    )

    status = service.voice_adaptive_status(limit=20)
    wakeword_state = status.get("dynamic_wakeword_by_risk", {}).get("high", {})
    tuning = wakeword_state.get("wakeword_tuning", {})

    assert wakeword_state.get("strategy") in {"polling_only", "hybrid_polling", "stt_pressure"}
    assert float(wakeword_state.get("ema_wakeword_pressure", 0.0) or 0.0) > 0.0
    assert "wakeword_enabled" in tuning
    assert "wakeword_sensitivity" in tuning
    assert float(tuning.get("restart_delay_s", 0.0) or 0.0) >= 0.0
