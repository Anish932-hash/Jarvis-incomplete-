from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from backend.python.speech.voice_session import VoiceSessionController


class _FailingSTT:
    def transcribe_stream(self, **_: Any) -> Dict[str, Any]:
        return {"status": "error", "message": "microphone unavailable", "source": "fake-stt"}

    def transcribe(self, duration: float = 4.0) -> Dict[str, Any]:
        return {"status": "error", "message": f"failed duration={duration}", "source": "fake-stt"}


class _SuccessfulSTT:
    def __init__(self) -> None:
        self.calls = 0

    def transcribe_stream(self, **_: Any) -> Dict[str, Any]:
        self.calls += 1
        return {"status": "success", "text": "hello jarvis", "source": "fake-stt", "model": "stub"}

    def transcribe(self, duration: float = 4.0) -> Dict[str, Any]:
        self.calls += 1
        return {"status": "success", "text": "hello jarvis", "source": "fake-stt", "model": "stub", "duration": duration}


class _LowConfidenceSTT:
    def transcribe_stream(self, **_: Any) -> Dict[str, Any]:
        return {"status": "success", "text": "low confidence transcript", "source": "fake-stt", "model": "stub", "confidence": 0.05}

    def transcribe(self, duration: float = 4.0) -> Dict[str, Any]:
        return {
            "status": "success",
            "text": "low confidence transcript",
            "source": "fake-stt",
            "model": "stub",
            "confidence": 0.05,
            "duration": duration,
        }


class _RouteBlockedPolicy:
    def __call__(self) -> Dict[str, Any]:
        return {
            "generated_at": time.time(),
            "stt": {
                "task": "stt",
                "route_blocked": True,
                "reason_code": "local_launch_template_blacklisted",
                "reason": "Local STT route blocked by launcher policy.",
                "cooldown_hint_s": 30,
                "selected_provider": "local",
                "recommended_provider": "",
                "local_route_viable": False,
            },
            "wakeword": {
                "task": "wakeword",
                "route_blocked": False,
                "selected_provider": "local",
                "recommended_provider": "local",
                "local_route_viable": True,
            },
        }


class _WakewordRecoveryPolicy:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> Dict[str, Any]:
        self.calls += 1
        return {
            "generated_at": time.time(),
            "stt": {
                "task": "stt",
                "route_blocked": False,
                "selected_provider": "local",
                "recommended_provider": "local",
                "local_route_viable": True,
            },
            "wakeword": {
                "task": "wakeword",
                "route_blocked": False,
                "blacklisted": True,
                "recovery_pending": True,
                "cooldown_hint_s": 10,
                "reason_code": "local_launch_template_blacklisted",
                "reason": "Wakeword route in recovery.",
                "selected_provider": "local",
                "recommended_provider": "local",
                "local_route_viable": False,
            },
        }


class _FlappingVoiceRoutePolicy:
    def __init__(self) -> None:
        self.phase = 0

    def __call__(self) -> Dict[str, Any]:
        if self.phase == 0:
            payload = {
                "generated_at": time.time(),
                "stt": {
                    "task": "stt",
                    "route_blocked": False,
                    "route_adjusted": True,
                    "selected_provider": "local",
                    "recommended_provider": "groq",
                    "reason_code": "local_launch_template_blacklisted",
                    "reason": "Local STT route rerouted to Groq.",
                    "cooldown_hint_s": 15,
                    "local_route_viable": False,
                },
                "wakeword": {"task": "wakeword", "selected_provider": "local", "recommended_provider": "local"},
            }
        else:
            payload = {
                "generated_at": time.time(),
                "stt": {
                    "task": "stt",
                    "route_blocked": False,
                    "route_adjusted": False,
                    "selected_provider": "local",
                    "recommended_provider": "local",
                    "reason_code": "",
                    "reason": "",
                    "cooldown_hint_s": 0,
                    "local_route_viable": True,
                },
                "wakeword": {"task": "wakeword", "selected_provider": "local", "recommended_provider": "local"},
            }
        self.phase += 1
        return payload


def _wait_until(predicate, timeout_s: float = 2.0, interval_s: float = 0.02) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return False


class _FakeWakewordEngine:
    def __init__(self, keyword_path: str, sensitivity: float) -> None:
        self.keyword_path = keyword_path
        self.sensitivity = sensitivity
        self.callback = None

    def start(self, callback) -> None:
        self.callback = callback

    def stop(self) -> None:
        self.callback = None


class _FailingWakewordEngine(_FakeWakewordEngine):
    def start(self, callback) -> None:  # noqa: ANN001
        raise RuntimeError("wakeword bootstrap failed")


class _WakewordGateThenRecoverPolicy:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> Dict[str, Any]:
        self.calls += 1
        if self.calls <= 1:
            return {
                "generated_at": time.time(),
                "stt": {
                    "task": "stt",
                    "route_blocked": False,
                    "selected_provider": "local",
                    "recommended_provider": "local",
                    "local_route_viable": True,
                },
                "wakeword": {
                    "task": "wakeword",
                    "route_blocked": False,
                    "selected_provider": "local",
                    "recommended_provider": "local",
                    "local_route_viable": False,
                    "blacklisted": True,
                    "recovery_pending": True,
                    "cooldown_hint_s": 0.05,
                    "reason_code": "local_launch_template_blacklisted",
                    "reason": "Wakeword route is cooling down.",
                },
            }
        return {
            "generated_at": time.time(),
            "stt": {
                "task": "stt",
                "route_blocked": False,
                "selected_provider": "local",
                "recommended_provider": "local",
                "local_route_viable": True,
            },
            "wakeword": {
                "task": "wakeword",
                "route_blocked": False,
                "selected_provider": "local",
                "recommended_provider": "local",
                "local_route_viable": True,
                "blacklisted": False,
                "recovery_pending": False,
            },
        }


class _AllClearPolicy:
    def __call__(self) -> Dict[str, Any]:
        return {
            "generated_at": time.time(),
            "stt": {
                "task": "stt",
                "route_blocked": False,
                "selected_provider": "local",
                "recommended_provider": "local",
                "local_route_viable": True,
            },
            "wakeword": {
                "task": "wakeword",
                "route_blocked": False,
                "selected_provider": "local",
                "recommended_provider": "local",
                "local_route_viable": True,
            },
        }


class _MissionPollingSupervision:
    def __call__(self, _context: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "mission_id": "mission-voice-guard",
            "risk_level": "medium",
            "policy_profile": "balanced",
            "session_overrides": {
                "fallback_interval_s": 0.05,
                "route_policy_resume_stability_s": 0.8,
            },
            "mission_reliability": {
                "mission_id": "mission-voice-guard",
                "sessions": 5,
                "wakeword_gate_events": 4,
                "route_policy_pause_count": 4,
            },
            "route_recovery_recommendation": {
                "mission_id": "mission-voice-guard",
                "wakeword_strategy": "polling_only",
                "recovery_profile": "polling_only",
            },
            "wakeword_supervision": {
                "status": "polling_only",
                "strategy": "polling_only",
                "allow_wakeword": False,
                "reason_code": "mission_reliability_polling_only",
                "reason": "Mission recovery history prefers polling over wakeword restarts.",
                "fallback_interval_s": 0.05,
            },
        }


class _AdaptiveWakewordSupervision:
    def __call__(self, _context: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "mission_id": "mission-voice-runtime",
            "risk_level": "medium",
            "policy_profile": "balanced",
            "session_overrides": {
                "fallback_interval_s": 2.5,
                "route_policy_resume_stability_s": 1.4,
            },
            "wakeword_supervision": {
                "status": "active",
                "strategy": "wakeword",
                "allow_wakeword": True,
                "reason_code": "adaptive_runtime_tuning",
                "reason": "Adaptive voice tuning is tightening wakeword sensitivity.",
                "restart_delay_s": 1.25,
                "wakeword_sensitivity": 0.83,
                "polling_bias": 0.45,
                "fallback_interval_s": 2.5,
                "resume_stability_s": 1.4,
            },
        }


def test_voice_session_opens_failure_circuit_after_repeated_errors() -> None:
    telemetry: List[Tuple[str, Dict[str, Any]]] = []
    controller = VoiceSessionController(
        stt_engine=_FailingSTT(),
        on_transcript=lambda _text, _ctx: {"status": "noop"},
        emit_telemetry=lambda event, payload: telemetry.append((event, payload)),
    )
    try:
        controller.start(
            {
                "wakeword_enabled": False,
                "stt_mode": "auto",
                "fallback_interval_s": 0.05,
                "cooldown_s": 0.01,
                "max_consecutive_errors": 2,
                "error_backoff_s": 0.2,
            }
        )

        opened = _wait_until(lambda: bool(controller.status().get("circuit_open_until", "")), timeout_s=2.0)
        assert opened, controller.status()
        assert any(event == "voice.circuit_open" for event, _ in telemetry)
    finally:
        controller.stop()


def test_voice_session_surfaces_callback_failures_without_crashing_loop() -> None:
    telemetry: List[Tuple[str, Dict[str, Any]]] = []

    def _failing_callback(_text: str, _ctx: Dict[str, Any]) -> Dict[str, Any]:
        raise RuntimeError("callback exploded")

    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=_failing_callback,
        emit_telemetry=lambda event, payload: telemetry.append((event, payload)),
    )
    try:
        controller.start(
            {
                "wakeword_enabled": False,
                "stt_mode": "auto",
                "fallback_interval_s": 60.0,
                "cooldown_s": 0.01,
            }
        )
        controller.trigger_once(trigger_type="manual")

        seen = _wait_until(
            lambda: any(event == "voice.callback_failed" for event, _ in telemetry),
            timeout_s=2.0,
        )
        assert seen, telemetry
        state = controller.status()
        assert state.get("running") is True
        assert int(state.get("error_count", 0)) >= 1
        assert "callback exploded" in str(state.get("last_error", ""))
    finally:
        controller.stop()


def test_voice_session_rejects_low_confidence_transcripts_and_opens_circuit() -> None:
    telemetry: List[Tuple[str, Dict[str, Any]]] = []
    callback_count = {"value": 0}

    def _callback(_text: str, _ctx: Dict[str, Any]) -> Dict[str, Any]:
        callback_count["value"] += 1
        return {"status": "ok"}

    controller = VoiceSessionController(
        stt_engine=_LowConfidenceSTT(),
        on_transcript=_callback,
        emit_telemetry=lambda event, payload: telemetry.append((event, payload)),
    )
    try:
        controller.start(
            {
                "wakeword_enabled": False,
                "stt_mode": "auto",
                "fallback_interval_s": 60.0,
                "cooldown_s": 0.01,
                "min_confidence": 0.8,
                "max_low_confidence_streak": 1,
                "low_confidence_backoff_s": 0.2,
            }
        )
        controller.trigger_once(trigger_type="manual")

        seen_rejection = _wait_until(
            lambda: any(event == "voice.transcribe_rejected" for event, _ in telemetry),
            timeout_s=2.0,
        )
        assert seen_rejection, telemetry
        assert callback_count["value"] == 0
        state = controller.status()
        assert int(state.get("rejected_transcription_count", 0)) >= 1
        assert any(event == "voice.circuit_open" for event, _ in telemetry)
    finally:
        controller.stop()


def test_voice_session_enforces_callback_rate_limit() -> None:
    telemetry: List[Tuple[str, Dict[str, Any]]] = []
    callback_count = {"value": 0}

    def _callback(_text: str, _ctx: Dict[str, Any]) -> Dict[str, Any]:
        callback_count["value"] += 1
        return {"reply": "done"}

    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=_callback,
        emit_telemetry=lambda event, payload: telemetry.append((event, payload)),
    )
    try:
        controller.start(
            {
                "wakeword_enabled": False,
                "stt_mode": "auto",
                "fallback_interval_s": 60.0,
                "cooldown_s": 0.01,
                "max_callbacks_per_minute": 1,
            }
        )
        controller.trigger_once(trigger_type="manual")
        first_callback_done = _wait_until(
            lambda: any(event == "voice.callback_completed" for event, _ in telemetry),
            timeout_s=2.0,
        )
        assert first_callback_done, telemetry

        controller.trigger_once(trigger_type="manual")
        saw_rate_limit = _wait_until(
            lambda: any(
                event == "voice.rate_limited" and str(payload.get("scope", "")) == "callback"
                for event, payload in telemetry
            ),
            timeout_s=2.0,
        )
        assert saw_rate_limit, telemetry
        assert callback_count["value"] == 1
    finally:
        controller.stop()


def test_voice_session_applies_adaptive_profile_from_metadata() -> None:
    telemetry: List[Tuple[str, Dict[str, Any]]] = []
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=lambda event, payload: telemetry.append((event, payload)),
    )
    try:
        controller.start(
            {
                "wakeword_enabled": False,
                "stt_mode": "auto",
                "fallback_interval_s": 60.0,
                "metadata": {"risk_level": "high", "policy_profile": "automation_safe"},
            }
        )
        state = controller.status()
        config = state.get("config", {})
        adaptive = config.get("adaptive_profile", {})

        assert adaptive.get("profile") == "strict"
        assert float(config.get("min_confidence", 0.0)) >= 0.4
        assert int(config.get("max_callbacks_per_minute", 999)) <= 40
    finally:
        controller.stop()


def test_voice_session_applies_profile_hint_and_guardrail_overrides() -> None:
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=None,
    )
    try:
        controller.start(
            {
                "wakeword_enabled": False,
                "stt_mode": "auto",
                "fallback_interval_s": 60.0,
                "metadata": {
                    "voice_profile_hint": "power",
                    "voice_guardrail_overrides": {
                        "min_confidence_offset": -0.05,
                        "max_callbacks_scale": 1.5,
                    },
                },
            }
        )
        state = controller.status()
        config = state.get("config", {})
        adaptive = config.get("adaptive_profile", {})

        assert adaptive.get("profile") == "power"
        assert float(config.get("min_confidence", 1.0)) < 0.22
        assert int(config.get("max_callbacks_per_minute", 0)) >= 170
    finally:
        controller.stop()


def test_voice_session_blocks_manual_transcription_when_route_policy_blocks_stt() -> None:
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=None,
        route_policy_provider=_RouteBlockedPolicy(),
    )

    result = controller.trigger_once(trigger_type="manual")

    assert result["status"] == "error"
    assert result["accepted"] is False
    assert "launcher policy" in str(result.get("message", "")).lower()


def test_voice_session_falls_back_to_polling_when_wakeword_route_is_gated() -> None:
    telemetry: List[Tuple[str, Dict[str, Any]]] = []
    stt = _SuccessfulSTT()
    controller = VoiceSessionController(
        stt_engine=stt,
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=lambda event, payload: telemetry.append((event, payload)),
        route_policy_provider=_WakewordRecoveryPolicy(),
    )
    try:
        controller.start(
            {
                "wakeword_enabled": True,
                "wakeword_keyword_path": "fake.ppn",
                "fallback_interval_s": 0.05,
                "cooldown_s": 0.01,
                "stt_mode": "auto",
            }
        )

        seen = _wait_until(
            lambda: any(
                event == "voice.transcribe_started" and str(payload.get("trigger_type", "")) == "wakeword_fallback"
                for event, payload in telemetry
            ),
            timeout_s=2.0,
        )
        assert seen, telemetry
        assert stt.calls >= 1
        assert str(controller.status().get("wakeword_status", "")).startswith("gated:")
    finally:
        controller.stop()


def test_voice_session_records_route_policy_timeline_transitions() -> None:
    policy = _FlappingVoiceRoutePolicy()
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=None,
        route_policy_provider=policy,
        route_policy_snapshot_ttl_s=0.0,
    )

    controller.route_policy_status(force_refresh=True)
    controller.route_policy_status(force_refresh=True)
    timeline = controller.route_policy_timeline(limit=10)

    assert timeline["status"] == "success"
    assert int(timeline.get("count", 0)) >= 2
    assert any(str(item.get("recommended_provider", "")) == "groq" for item in timeline.get("items", []))
    assert any(str(item.get("recommended_provider", "")) == "local" for item in timeline.get("items", []))


def test_voice_session_tracks_wakeword_recovery_after_route_gate(monkeypatch) -> None:
    monkeypatch.setattr("backend.python.speech.voice_session.WakewordEngine", _FakeWakewordEngine)
    telemetry: List[Tuple[str, Dict[str, Any]]] = []
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=lambda event, payload: telemetry.append((event, payload)),
        route_policy_provider=_WakewordGateThenRecoverPolicy(),
        route_policy_snapshot_ttl_s=0.0,
    )
    try:
        controller._config = controller._normalize_config(  # noqa: SLF001
            {
                "wakeword_enabled": True,
                "wakeword_keyword_path": "fake.ppn",
                "fallback_interval_s": 60.0,
                "cooldown_s": 0.01,
            }
        )
        controller._initialize_wakeword_if_enabled()  # noqa: SLF001
        snapshot = controller.route_policy_status(force_refresh=True)
        controller._reconcile_wakeword_route_policy(snapshot)  # noqa: SLF001
        state = controller.status()
        assert str(state.get("wakeword_status", "")).strip().lower() == "active"
        assert state.get("wakeword_status") == "active"
        assert int(state.get("wakeword_gate_count", 0) or 0) >= 1
        assert int(state.get("wakeword_recovery_count", 0) or 0) >= 1
        assert any(event == "voice.wakeword_gated" for event, _ in telemetry)
        assert any(event == "voice.wakeword_recovered" for event, _ in telemetry)
    finally:
        controller._shutdown_wakeword()  # noqa: SLF001


def test_voice_session_applies_mission_supervision_and_uses_polling_fallback(monkeypatch) -> None:
    monkeypatch.setattr("backend.python.speech.voice_session.WakewordEngine", _FakeWakewordEngine)
    telemetry: List[Tuple[str, Dict[str, Any]]] = []
    stt = _SuccessfulSTT()
    controller = VoiceSessionController(
        stt_engine=stt,
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=lambda event, payload: telemetry.append((event, payload)),
        route_policy_provider=_AllClearPolicy(),
        supervision_provider=_MissionPollingSupervision(),
        route_policy_snapshot_ttl_s=0.0,
        supervision_snapshot_ttl_s=0.0,
    )
    try:
        controller.start(
            {
                "wakeword_enabled": True,
                "wakeword_keyword_path": "fake.ppn",
                "fallback_interval_s": 60.0,
                "cooldown_s": 0.01,
                "stt_mode": "auto",
            }
        )

        seen = _wait_until(
            lambda: any(
                event == "voice.transcribe_started" and str(payload.get("trigger_type", "")) == "wakeword_fallback"
                for event, payload in telemetry
            ),
            timeout_s=2.0,
        )
        assert seen, telemetry
        state = controller.status()
        assert str(state.get("wakeword_status", "")).startswith("gated:mission_reliability_polling_only")
        assert str(state.get("wakeword_supervision_status", "")) == "polling_only"
        assert str(state.get("wakeword_supervision_reason", "")) == "mission_reliability_polling_only"
        assert str(state.get("voice_mission_reliability", {}).get("mission_id", "")) == "mission-voice-guard"
        assert str(state.get("voice_route_recovery_recommendation", {}).get("wakeword_strategy", "")) == "polling_only"
        assert any(event == "voice.wakeword_supervision_changed" for event, _ in telemetry)
        assert stt.calls >= 1
    finally:
        controller.stop()


def test_voice_session_surfaces_wakeword_supervision_timeline() -> None:
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=None,
        route_policy_provider=_AllClearPolicy(),
        supervision_provider=_MissionPollingSupervision(),
        route_policy_snapshot_ttl_s=0.0,
        supervision_snapshot_ttl_s=0.0,
    )
    try:
        controller.start(
            {
                "wakeword_enabled": False,
                "fallback_interval_s": 60.0,
                "cooldown_s": 0.01,
                "stt_mode": "auto",
            }
        )
        controller.supervision_status(force_refresh=True)
        timeline = controller.wakeword_supervision_timeline(limit=10)

        assert timeline["status"] == "success"
        assert int(timeline.get("count", 0) or 0) >= 1
        assert str(timeline.get("current", {}).get("status", "")) == "polling_only"
        assert any(str(item.get("strategy", "")) == "polling_only" for item in timeline.get("items", []))
        diagnostics = timeline.get("diagnostics", {})
        status_counts = diagnostics.get("status_counts", {})
        assert int(status_counts.get("polling_only", 0) or 0) >= 1
        assert isinstance(diagnostics.get("timeline_buckets", []), list)
    finally:
        controller.stop()


def test_voice_session_applies_runtime_wakeword_tuning_before_start(monkeypatch) -> None:
    monkeypatch.setattr("backend.python.speech.voice_session.WakewordEngine", _FakeWakewordEngine)
    telemetry: List[Tuple[str, Dict[str, Any]]] = []
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=lambda event, payload: telemetry.append((event, payload)),
        route_policy_provider=_AllClearPolicy(),
        supervision_provider=_AdaptiveWakewordSupervision(),
        route_policy_snapshot_ttl_s=0.0,
        supervision_snapshot_ttl_s=0.0,
    )
    try:
        controller.start(
            {
                "wakeword_enabled": True,
                "wakeword_keyword_path": "fake.ppn",
                "wakeword_sensitivity": 0.55,
                "fallback_interval_s": 20.0,
                "route_policy_resume_stability_s": 0.6,
                "cooldown_s": 0.01,
                "stt_mode": "auto",
            }
        )

        state = controller.status()
        assert str(state.get("wakeword_status", "")).strip().lower() == "active"
        assert round(float(state.get("wakeword_supervision_sensitivity", 0.0) or 0.0), 2) == 0.83
        assert round(float(controller._config.get("wakeword_sensitivity", 0.0) or 0.0), 2) == 0.83  # noqa: SLF001
        started_payload = next(payload for event, payload in telemetry if event == "voice.wakeword_started")
        assert round(float(started_payload.get("sensitivity", 0.0) or 0.0), 2) == 0.83
        assert any(event == "voice.wakeword_runtime_tuned" for event, _ in telemetry)
    finally:
        controller.stop()


def test_voice_session_schedules_restart_backoff_when_wakeword_start_fails(monkeypatch) -> None:
    monkeypatch.setattr("backend.python.speech.voice_session.WakewordEngine", _FailingWakewordEngine)
    telemetry: List[Tuple[str, Dict[str, Any]]] = []
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=lambda event, payload: telemetry.append((event, payload)),
        route_policy_provider=_AllClearPolicy(),
        supervision_provider=_AdaptiveWakewordSupervision(),
        route_policy_snapshot_ttl_s=0.0,
        supervision_snapshot_ttl_s=0.0,
    )
    try:
        controller.start(
            {
                "wakeword_enabled": True,
                "wakeword_keyword_path": "fake.ppn",
                "cooldown_s": 0.01,
                "stt_mode": "auto",
            }
        )

        state = controller.status()
        assert str(state.get("wakeword_status", "")).strip().lower().startswith("degraded:")
        assert int(state.get("wakeword_start_failure_count", 0) or 0) == 1
        assert str(state.get("wakeword_supervision_restart_not_before", "")).strip()
        assert any(event == "voice.wakeword_restart_backoff" for event, _ in telemetry)
        failed_payload = next(payload for event, payload in telemetry if event == "voice.wakeword_failed")
        assert int(failed_payload.get("failure_count", 0) or 0) == 1
        assert str(failed_payload.get("next_retry_at", "")).strip()
        assert float(failed_payload.get("restart_delay_s", 0.0) or 0.0) >= 1.0
    finally:
        controller.stop()


def test_voice_session_exposes_wakeword_restart_timeline_after_start_failure(monkeypatch) -> None:
    monkeypatch.setattr("backend.python.speech.voice_session.WakewordEngine", _FailingWakewordEngine)
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=None,
        route_policy_provider=_AllClearPolicy(),
        supervision_provider=_AdaptiveWakewordSupervision(),
        route_policy_snapshot_ttl_s=0.0,
        supervision_snapshot_ttl_s=0.0,
    )
    try:
        controller.start(
            {
                "wakeword_enabled": True,
                "wakeword_keyword_path": "fake.ppn",
                "cooldown_s": 0.01,
                "stt_mode": "auto",
            }
        )

        timeline = controller.wakeword_restart_timeline(limit=10)
        diagnostics = timeline.get("diagnostics", {})
        event_counts = diagnostics.get("event_counts", {})

        assert timeline["status"] == "success"
        assert int(timeline.get("count", 0) or 0) >= 2
        assert int(event_counts.get("start_failed", 0) or 0) >= 1
        assert int(event_counts.get("restart_backoff", 0) or 0) >= 1
        assert int(timeline.get("current", {}).get("recent_failures", 0) or 0) >= 1
    finally:
        controller.stop()


def test_voice_session_records_recovery_window_elapsed_in_restart_timeline(monkeypatch) -> None:
    monkeypatch.setattr("backend.python.speech.voice_session.WakewordEngine", _FailingWakewordEngine)
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=None,
        route_policy_provider=_AllClearPolicy(),
        supervision_provider=_AdaptiveWakewordSupervision(),
        route_policy_snapshot_ttl_s=0.0,
        supervision_snapshot_ttl_s=0.0,
    )
    try:
        controller.start(
            {
                "wakeword_enabled": True,
                "wakeword_keyword_path": "fake.ppn",
                "cooldown_s": 0.01,
                "stt_mode": "auto",
            }
        )
        retry_at = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        controller._record_wakeword_restart_event(  # noqa: SLF001
            event_type="recovery_window_elapsed",
            status="recovery:mission_recovery_policy",
            reason_code="adaptive_runtime_tuning",
            reason="Wakeword restart recovery window elapsed.",
            next_retry_at=retry_at,
            recovered=True,
        )
        timeline = controller.wakeword_restart_timeline(limit=12, event_type="recovery_window_elapsed")

        assert timeline["status"] == "success"
        assert int(timeline.get("count", 0) or 0) >= 1
        assert any(str(item.get("event_type", "")).strip().lower() == "recovery_window_elapsed" for item in timeline.get("items", []))
    finally:
        controller.stop()


def test_voice_session_autotunes_restart_policy_from_long_horizon_failures() -> None:
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=None,
        route_policy_provider=_AllClearPolicy(),
        supervision_provider=_AdaptiveWakewordSupervision(),
        route_policy_snapshot_ttl_s=0.0,
        supervision_snapshot_ttl_s=0.0,
    )
    controller._config = controller._normalize_config(  # noqa: SLF001
        {
            "wakeword_enabled": True,
            "wakeword_keyword_path": "fake.ppn",
            "fallback_interval_s": 2.0,
            "route_policy_resume_stability_s": 0.75,
        }
    )
    for index in range(5):
        controller._record_wakeword_restart_event(  # noqa: SLF001
            event_type="restart_backoff",
            status="degraded:wakeword bootstrap failed",
            reason_code="wakeword_start_failed",
            reason="wakeword bootstrap failed",
            restart_delay_s=4.0 + index,
            next_retry_at=(datetime.now(timezone.utc) + timedelta(seconds=10 + index)).isoformat(),
            failure_count=index + 1,
            exhausted=index >= 3,
            exhausted_until=(datetime.now(timezone.utc) + timedelta(seconds=20 + index)).isoformat() if index >= 3 else "",
        )
    controller._record_wakeword_restart_event(  # noqa: SLF001
        event_type="restart_exhausted",
        status="degraded:wakeword bootstrap failed",
        reason_code="wakeword_start_failed",
        reason="Wakeword restart failures crossed the adaptive exhaustion threshold.",
        restart_delay_s=9.0,
        next_retry_at=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
        failure_count=5,
        exhausted=True,
        exhausted_until=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
    )
    controller._record_wakeword_restart_event(  # noqa: SLF001
        event_type="restart_exhaustion_expired",
        status="recovery:mission_recovery_policy",
        reason_code="wakeword_start_failed",
        reason="Wakeword restart exhaustion recovery window elapsed.",
        next_retry_at=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
        recovered=True,
    )
    controller._record_wakeword_restart_event(  # noqa: SLF001
        event_type="started",
        status="active",
        reason_code="wakeword_recovered",
        reason="Wakeword runtime recovered.",
        recovered=True,
    )
    controller._record_wakeword_restart_event(  # noqa: SLF001
        event_type="recovered",
        status="active",
        reason_code="wakeword_recovered",
        reason="Wakeword route stayed stable after restart recovery.",
        recovered=True,
    )

    policy = controller._compute_wakeword_restart_policy(  # noqa: SLF001
        {
            "polling_bias": 0.45,
            "fallback_interval_s": 2.0,
            "resume_stability_s": 0.8,
            "restart_delay_s": 2.0,
        }
    )

    assert int(policy.get("long_failures", 0) or 0) >= 5
    assert int(policy.get("long_exhaustions", 0) or 0) >= 1
    assert float(policy.get("recovery_credit", 0.0) or 0.0) > 0.0
    assert float(policy.get("cooldown_recovery_factor", 1.0) or 1.0) < 1.0
    assert float(policy.get("recommended_resume_stability_s", 0.0) or 0.0) >= 1.0
    assert float(policy.get("recovery_expiry_s", 0.0) or 0.0) >= float(
        policy.get("recommended_fallback_interval_s", 0.0) or 0.0
    )
    assert int(policy.get("threshold_bias", 0) or 0) <= 0


def test_voice_session_relaxes_restart_state_after_sustained_recovery() -> None:
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=None,
        route_policy_provider=_AllClearPolicy(),
        supervision_provider=_AdaptiveWakewordSupervision(),
        route_policy_snapshot_ttl_s=0.0,
        supervision_snapshot_ttl_s=0.0,
    )
    controller._config = controller._normalize_config(  # noqa: SLF001
        {
            "wakeword_enabled": True,
            "wakeword_keyword_path": "fake.ppn",
            "fallback_interval_s": 2.0,
            "route_policy_resume_stability_s": 0.9,
        }
    )
    retry_at = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
    exhausted_until = (datetime.now(timezone.utc) + timedelta(seconds=180)).isoformat()
    controller._state["wakeword_status"] = "recovery:mission_recovery_policy"  # noqa: SLF001
    controller._state["wakeword_restart_backoff_count"] = 4  # noqa: SLF001
    controller._state["wakeword_restart_exhausted_count"] = 2  # noqa: SLF001
    controller._state["wakeword_start_failure_count"] = 5  # noqa: SLF001
    controller._state["wakeword_supervision_restart_delay_s"] = 12.0  # noqa: SLF001
    controller._state["wakeword_supervision_restart_not_before"] = retry_at  # noqa: SLF001
    controller._state["wakeword_restart_exhausted_until"] = exhausted_until  # noqa: SLF001

    for index in range(3):
        controller._record_wakeword_restart_event(  # noqa: SLF001
            event_type="restart_backoff",
            status="degraded:wakeword bootstrap failed",
            reason_code="wakeword_start_failed",
            reason="wakeword bootstrap failed",
            restart_delay_s=5.0 + index,
            next_retry_at=(datetime.now(timezone.utc) + timedelta(seconds=45 + index)).isoformat(),
            failure_count=2 + index,
            exhausted=index >= 2,
            exhausted_until=(datetime.now(timezone.utc) + timedelta(seconds=100 + index)).isoformat() if index >= 2 else "",
        )
    for event_type in ("started", "recovered", "recovery_window_elapsed", "restart_exhaustion_expired"):
        controller._record_wakeword_restart_event(  # noqa: SLF001
            event_type=event_type,
            status="active",
            reason_code="wakeword_recovered",
            reason="Wakeword remained stable after recovery.",
            next_retry_at="",
            recovered=True,
        )

    result = controller._relax_wakeword_restart_state(  # noqa: SLF001
        {
            "polling_bias": 0.18,
            "fallback_interval_s": 2.0,
            "resume_stability_s": 0.9,
            "restart_delay_s": 1.5,
        },
        trigger="wakeword_started_recovered",
    )
    state = controller.status()

    assert result["status"] == "relaxed"
    assert int(state.get("wakeword_restart_backoff_count", 0) or 0) < 4
    assert int(state.get("wakeword_restart_exhausted_count", 0) or 0) < 2
    assert int(state.get("wakeword_start_failure_count", 0) or 0) < 5
    assert float(state.get("wakeword_supervision_restart_delay_s", 0.0) or 0.0) < 12.0
    assert controller._parse_epoch_seconds(str(state.get("wakeword_supervision_restart_not_before", "")).strip()) < controller._parse_epoch_seconds(retry_at)  # noqa: SLF001
    new_exhausted_until = str(state.get("wakeword_restart_exhausted_until", "")).strip()
    assert not new_exhausted_until or controller._parse_epoch_seconds(new_exhausted_until) < controller._parse_epoch_seconds(exhausted_until)  # noqa: SLF001
    timeline = controller.wakeword_restart_timeline(limit=12, event_type="restart_policy_relaxed")
    assert timeline["status"] == "success"
    assert int(timeline.get("count", 0) or 0) >= 1


def test_voice_session_restores_persisted_restart_snapshot_and_preserves_it_on_start() -> None:
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=None,
        route_policy_provider=_AllClearPolicy(),
        supervision_provider=_AdaptiveWakewordSupervision(),
        route_policy_snapshot_ttl_s=0.0,
        supervision_snapshot_ttl_s=0.0,
    )

    restored = controller.restore_wakeword_restart_snapshot(
        {
            "items": [
                {
                    "event_id": "wakeword-restart-restored-1",
                    "occurred_at": "2026-03-08T10:00:00+00:00",
                    "event_type": "restart_policy_relaxed",
                    "status": "active",
                    "restart_delay_s": 4.0,
                    "next_retry_at": "2026-03-08T10:01:00+00:00",
                    "failure_count": 1,
                    "wakeword_sensitivity": 0.62,
                    "fallback_interval_s": 6.5,
                    "resume_stability_s": 1.45,
                    "polling_bias": 0.31,
                    "recovered": True,
                    "exhausted": False,
                    "policy": {
                        "recent_failures": 1,
                        "recent_successes": 3,
                        "long_failures": 2,
                        "long_successes": 4,
                        "long_exhaustions": 1,
                        "recommended_exhaustion_relaxation": 1,
                        "recovery_credit": 1.1,
                        "cooldown_scale": 1.1,
                        "drift_score": 0.12,
                        "recommended_profile": "recovered_wakeword",
                        "profile_action": "recover",
                        "recent_recovery_rate": 0.72,
                    },
                }
            ],
            "current": {
                "policy": {
                    "recent_failures": 1,
                    "recent_successes": 3,
                    "long_failures": 2,
                    "long_successes": 4,
                    "long_exhaustions": 1,
                    "recommended_exhaustion_relaxation": 1,
                    "recovery_credit": 1.1,
                    "cooldown_scale": 1.1,
                    "drift_score": 0.12,
                    "recommended_profile": "recovered_wakeword",
                    "profile_action": "recover",
                    "recent_recovery_rate": 0.72,
                },
                "failure_count": 1,
                "backoff_count": 1,
                "exhausted_count": 0,
                "recovery_expiry_count": 1,
                "relaxation_count": 1,
                "restart_delay_s": 4.0,
                "next_retry_at": "2026-03-08T10:01:00+00:00",
                "exhausted_until": "",
                "wakeword_sensitivity": 0.62,
                "fallback_interval_s": 6.5,
                "resume_stability_s": 1.45,
                "polling_bias": 0.31,
                "drift_score": 0.12,
                "recommended_profile": "recovered_wakeword",
                "profile_action": "recover",
                "recent_recovery_rate": 0.72,
            },
            "diagnostics": {
                "recovery_expiry_events": 1,
            },
        }
    )

    assert restored.get("status") == "success"
    applied_config = restored.get("applied_config", {}) if isinstance(restored.get("applied_config", {}), dict) else {}
    assert float(applied_config.get("wakeword_sensitivity", 0.0) or 0.0) == 0.62
    assert float(applied_config.get("fallback_interval_s", 0.0) or 0.0) == 6.5
    assert float(applied_config.get("resume_stability_s", 0.0) or 0.0) == 1.45
    assert float(applied_config.get("polling_bias", 0.0) or 0.0) == 0.31
    started = controller.start(
        {
            "wakeword_enabled": False,
            "cooldown_s": 0.01,
            "stt_mode": "auto",
        }
    )
    try:
        assert int(started.get("wakeword_restart_backoff_count", 0) or 0) == 1
        assert int(started.get("wakeword_restart_relaxation_count", 0) or 0) == 1
        assert float(started.get("wakeword_supervision_restart_delay_s", 0.0) or 0.0) == 4.0
        assert str(started.get("wakeword_supervision_restart_not_before", "")).strip() == "2026-03-08T10:01:00+00:00"
        assert float(controller._config.get("wakeword_sensitivity", 0.0) or 0.0) == 0.62  # noqa: SLF001
        assert float(controller._config.get("fallback_interval_s", 0.0) or 0.0) == 6.5  # noqa: SLF001
        assert float(controller._config.get("route_policy_resume_stability_s", 0.0) or 0.0) == 1.45  # noqa: SLF001
        assert float(controller._state.get("wakeword_supervision_polling_bias", 0.0) or 0.0) == 0.31  # noqa: SLF001
        restart_policy = controller._state.get("wakeword_restart_policy", {})  # noqa: SLF001
        assert isinstance(restart_policy, dict)
        assert restart_policy.get("recommended_profile") == "recovered_wakeword"
        assert restart_policy.get("profile_action") == "recover"
        assert float(restart_policy.get("drift_score", 0.0) or 0.0) == 0.12
        assert len(started.get("wakeword_restart_timeline", [])) >= 1
    finally:
        controller.stop()


def test_voice_session_restart_policy_uses_restored_drift_posture() -> None:
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=None,
        route_policy_provider=_AllClearPolicy(),
        supervision_provider=_AdaptiveWakewordSupervision(),
        route_policy_snapshot_ttl_s=0.0,
        supervision_snapshot_ttl_s=0.0,
    )
    controller._state["wakeword_restart_policy"] = {  # noqa: SLF001
        "drift_score": 0.74,
        "recommended_profile": "stability_guard",
        "profile_action": "demote",
        "recent_exhaustion_rate": 0.34,
        "recent_recovery_rate": 0.12,
    }

    policy = controller._compute_wakeword_restart_policy(  # noqa: SLF001
        {
            "polling_bias": 0.12,
            "fallback_interval_s": 2.0,
            "resume_stability_s": 0.8,
            "restart_delay_s": 1.6,
        }
    )

    assert policy.get("recommended_profile") == "stability_guard"
    assert policy.get("profile_action") == "demote"
    assert float(policy.get("drift_score", 0.0) or 0.0) == 0.74
    assert int(policy.get("threshold_bias", 0) or 0) <= -1
    assert float(policy.get("cooldown_scale", 0.0) or 0.0) > 1.0
    assert float(policy.get("recommended_resume_stability_s", 0.0) or 0.0) >= 1.0


def test_voice_session_records_restart_exhaustion_expiry_transition(monkeypatch) -> None:
    monkeypatch.setattr("backend.python.speech.voice_session.WakewordEngine", _FakeWakewordEngine)
    telemetry: List[Tuple[str, Dict[str, Any]]] = []
    controller = VoiceSessionController(
        stt_engine=_SuccessfulSTT(),
        on_transcript=lambda _text, _ctx: {"status": "ok"},
        emit_telemetry=lambda event, payload: telemetry.append((event, payload)),
        route_policy_provider=_AllClearPolicy(),
        supervision_provider=_AdaptiveWakewordSupervision(),
        route_policy_snapshot_ttl_s=0.0,
        supervision_snapshot_ttl_s=0.0,
    )
    try:
        controller._config = controller._normalize_config(  # noqa: SLF001
            {
                "wakeword_enabled": True,
                "wakeword_keyword_path": "fake.ppn",
                "fallback_interval_s": 2.0,
                "cooldown_s": 0.01,
            }
        )
        expired_retry_at = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        controller._state["wakeword_status"] = "recovery:mission_recovery_policy"  # noqa: SLF001
        controller._state["wakeword_supervision_restart_not_before"] = expired_retry_at  # noqa: SLF001
        controller._state["wakeword_restart_exhausted_until"] = expired_retry_at  # noqa: SLF001
        controller._state["wakeword_restart_policy"] = {  # noqa: SLF001
            "recent_failures": 3,
            "recent_successes": 0,
            "long_failures": 5,
            "long_successes": 1,
            "long_exhaustions": 1,
            "long_recoveries": 0,
            "max_failures_before_polling": 3,
            "cooldown_scale": 2.1,
            "recommended_fallback_interval_s": 2.8,
            "recommended_resume_stability_s": 1.6,
            "recovery_expiry_s": 6.0,
            "exhausted": True,
        }

        snapshot = controller.route_policy_status(force_refresh=True)
        controller._reconcile_wakeword_route_policy(snapshot)  # noqa: SLF001

        timeline = controller.wakeword_restart_timeline(limit=12, event_type="restart_exhaustion_expired")
        state = controller.status()

        assert timeline["status"] == "success"
        assert int(timeline.get("count", 0) or 0) >= 1
        assert any(str(item.get("event_type", "")).strip().lower() == "restart_exhaustion_expired" for item in timeline.get("items", []))
        assert int(state.get("wakeword_restart_recovery_expiry_count", 0) or 0) >= 1
        assert str(state.get("wakeword_restart_exhausted_until", "")).strip() == ""
        assert any(event == "voice.wakeword_restart_expiry_elapsed" for event, _ in telemetry)
    finally:
        controller.stop()
