from __future__ import annotations

import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .stt_engine import STTEngine
from .wakeword_engine import WakewordEngine


VoiceCallback = Callable[[str, Dict[str, Any]], Dict[str, Any]]
TelemetryCallback = Callable[[str, Dict[str, Any]], None]
SupervisionCallback = Callable[[Dict[str, Any]], Dict[str, Any]]


class VoiceSessionController:
    """
    Orchestrates wakeword -> STT -> callback execution in a resilient background loop.
    """

    def __init__(
        self,
        *,
        stt_engine: STTEngine,
        on_transcript: VoiceCallback,
        emit_telemetry: Optional[TelemetryCallback] = None,
        route_policy_provider: Optional[Callable[[], Dict[str, Any]]] = None,
        route_policy_snapshot_ttl_s: float = 2.0,
        supervision_provider: Optional[SupervisionCallback] = None,
        supervision_snapshot_ttl_s: float = 3.0,
    ) -> None:
        self.stt_engine = stt_engine
        self.on_transcript = on_transcript
        self.emit_telemetry = emit_telemetry
        self._route_policy_provider = route_policy_provider
        self._route_policy_snapshot_ttl_s = max(0.0, float(route_policy_snapshot_ttl_s or 0.0))
        self._supervision_provider = supervision_provider
        self._supervision_snapshot_ttl_s = max(0.0, float(supervision_snapshot_ttl_s or 0.0))

        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._wakeword_engine: Optional[WakewordEngine] = None
        self._stop_event = threading.Event()
        self._manual_trigger = threading.Event()
        self._wake_trigger = threading.Event()
        self._running = False
        self._circuit_open_until_epoch = 0.0
        self._transcription_timestamps: deque[float] = deque(maxlen=600)
        self._callback_timestamps: deque[float] = deque(maxlen=600)
        self._route_policy_snapshot: Dict[str, Any] = {}
        self._route_policy_last_refresh_epoch = 0.0
        self._route_policy_history: deque[Dict[str, Any]] = deque(maxlen=180)
        self._route_policy_last_signature = ""
        self._supervision_snapshot: Dict[str, Any] = {}
        self._supervision_last_refresh_epoch = 0.0
        self._wakeword_supervision_history: deque[Dict[str, Any]] = deque(maxlen=180)
        self._wakeword_restart_history: deque[Dict[str, Any]] = deque(maxlen=240)

        self._config: Dict[str, Any] = {}
        self._state: Dict[str, Any] = {
            "running": False,
            "session_started_at": "",
            "last_trigger_at": "",
            "last_trigger_type": "",
            "last_transcript": "",
            "last_reply": "",
            "wakeword_status": "disabled",
            "wakeword_gate_count": 0,
            "wakeword_recovery_count": 0,
            "wakeword_last_gated_at": "",
            "wakeword_last_recovered_at": "",
            "stt_backend": "unknown",
            "transcription_count": 0,
            "error_count": 0,
            "consecutive_errors": 0,
            "last_error": "",
            "last_success_at": "",
            "circuit_open_until": "",
            "last_confidence": 0.0,
            "low_confidence_streak": 0,
            "rejected_transcription_count": 0,
            "last_rejection_reason": "",
            "rate_limited_count": 0,
            "callback_count": 0,
            "callback_latency_ema_ms": 0.0,
            "adaptive_profile": {},
            "route_policy_status": "unknown",
            "route_policy_reason": "",
            "route_policy_last_changed_at": "",
            "route_policy_next_retry_at": "",
            "route_policy_block_count": 0,
            "route_policy_reroute_count": 0,
            "route_policy_recovery_count": 0,
            "route_policy": {},
            "wakeword_route_policy": {},
            "tts_route_policy": {},
            "route_policy_summary": {},
            "route_policy_timeline": [],
            "wakeword_supervision": {},
            "wakeword_supervision_status": "unknown",
            "wakeword_supervision_reason": "",
            "wakeword_supervision_last_changed_at": "",
            "wakeword_supervision_restart_delay_s": 0.0,
            "wakeword_supervision_restart_not_before": "",
            "wakeword_supervision_sensitivity": 0.0,
            "wakeword_supervision_polling_bias": 0.0,
            "wakeword_supervision_pause_count": 0,
            "wakeword_supervision_resume_count": 0,
            "wakeword_supervision_timeline": [],
            "wakeword_start_failure_count": 0,
            "wakeword_restart_policy": {},
            "wakeword_restart_timeline": [],
            "wakeword_restart_backoff_count": 0,
            "wakeword_restart_exhausted_count": 0,
            "wakeword_restart_recovery_expiry_count": 0,
            "wakeword_restart_relaxation_count": 0,
            "wakeword_restart_last_expired_retry_at": "",
            "wakeword_restart_exhausted_until": "",
            "wakeword_restart_last_exhausted_at": "",
            "wakeword_restart_last_exhaustion_expired_at": "",
            "wakeword_restart_last_relaxed_at": "",
            "voice_mission_reliability": {},
            "voice_route_recovery_recommendation": {},
        }

    def start(self, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload = config if isinstance(config, dict) else {}
        normalized = self._normalize_config(payload)

        with self._lock:
            if self._running:
                self._config.update(normalized)
                self._state["adaptive_profile"] = self._config.get("adaptive_profile", {})
                return self.status()

            self._stop_event.clear()
            self._manual_trigger.clear()
            self._wake_trigger.clear()
            preserved_restart_history = [dict(item) for item in list(self._wakeword_restart_history)]
            preserved_restart_state = {
                "wakeword_supervision_restart_delay_s": float(
                    self._state.get("wakeword_supervision_restart_delay_s", 0.0) or 0.0
                ),
                "wakeword_supervision_restart_not_before": str(
                    self._state.get("wakeword_supervision_restart_not_before", "")
                ).strip(),
                "wakeword_start_failure_count": int(self._state.get("wakeword_start_failure_count", 0) or 0),
                "wakeword_restart_policy": dict(self._state.get("wakeword_restart_policy", {}))
                if isinstance(self._state.get("wakeword_restart_policy", {}), dict)
                else {},
                "wakeword_restart_backoff_count": int(self._state.get("wakeword_restart_backoff_count", 0) or 0),
                "wakeword_restart_exhausted_count": int(
                    self._state.get("wakeword_restart_exhausted_count", 0) or 0
                ),
                "wakeword_restart_recovery_expiry_count": int(
                    self._state.get("wakeword_restart_recovery_expiry_count", 0) or 0
                ),
                "wakeword_restart_relaxation_count": int(
                    self._state.get("wakeword_restart_relaxation_count", 0) or 0
                ),
                "wakeword_restart_last_expired_retry_at": str(
                    self._state.get("wakeword_restart_last_expired_retry_at", "")
                ).strip(),
                "wakeword_restart_exhausted_until": str(
                    self._state.get("wakeword_restart_exhausted_until", "")
                ).strip(),
                "wakeword_restart_last_exhausted_at": str(
                    self._state.get("wakeword_restart_last_exhausted_at", "")
                ).strip(),
                "wakeword_restart_last_exhaustion_expired_at": str(
                    self._state.get("wakeword_restart_last_exhaustion_expired_at", "")
                ).strip(),
                "wakeword_restart_last_relaxed_at": str(
                    self._state.get("wakeword_restart_last_relaxed_at", "")
                ).strip(),
                "wakeword_restart_timeline": [dict(item) for item in list(self._wakeword_restart_history)[-24:]],
                "wakeword_sensitivity": float(self._config.get("wakeword_sensitivity", normalized.get("wakeword_sensitivity", 0.5)) or 0.5),
                "fallback_interval_s": float(
                    self._config.get("fallback_interval_s", normalized.get("fallback_interval_s", 10.0)) or 10.0
                ),
                "resume_stability_s": float(
                    self._config.get(
                        "route_policy_resume_stability_s",
                        normalized.get("route_policy_resume_stability_s", 0.75),
                    )
                    or 0.75
                ),
                "polling_bias": float(self._state.get("wakeword_supervision_polling_bias", 0.0) or 0.0),
            }
            normalized["wakeword_sensitivity"] = self._normalize_float(
                preserved_restart_state.get("wakeword_sensitivity", normalized.get("wakeword_sensitivity", 0.5)),
                minimum=0.0,
                maximum=1.0,
            )
            normalized["fallback_interval_s"] = self._normalize_float(
                preserved_restart_state.get("fallback_interval_s", normalized.get("fallback_interval_s", 10.0)),
                minimum=0.5,
                maximum=60.0,
            )
            normalized["route_policy_resume_stability_s"] = self._normalize_float(
                preserved_restart_state.get(
                    "resume_stability_s",
                    normalized.get("route_policy_resume_stability_s", 0.75),
                ),
                minimum=0.5,
                maximum=30.0,
            )
            self._config = normalized
            self._running = True
            self._transcription_timestamps.clear()
            self._callback_timestamps.clear()
            self._route_policy_history.clear()
            self._route_policy_snapshot = {}
            self._route_policy_last_refresh_epoch = 0.0
            self._route_policy_last_signature = ""
            self._supervision_snapshot = {}
            self._supervision_last_refresh_epoch = 0.0
            self._wakeword_supervision_history.clear()
            self._state.update(
                {
                    "running": True,
                    "session_started_at": datetime.now(timezone.utc).isoformat(),
                    "last_reply": "",
                    "last_transcript": "",
                    "last_trigger_at": "",
                    "last_trigger_type": "",
                    "wakeword_status": "disabled",
                    "wakeword_gate_count": 0,
                    "wakeword_recovery_count": 0,
                    "wakeword_last_gated_at": "",
                    "wakeword_last_recovered_at": "",
                    "transcription_count": 0,
                    "error_count": 0,
                    "consecutive_errors": 0,
                    "last_error": "",
                    "last_success_at": "",
                    "circuit_open_until": "",
                    "last_confidence": 0.0,
                    "low_confidence_streak": 0,
                    "rejected_transcription_count": 0,
                    "last_rejection_reason": "",
                    "rate_limited_count": 0,
                    "callback_count": 0,
                    "callback_latency_ema_ms": 0.0,
                    "adaptive_profile": normalized.get("adaptive_profile", {}),
                    "route_policy_status": "unknown",
                    "route_policy_reason": "",
                    "route_policy_last_changed_at": "",
                    "route_policy_next_retry_at": "",
                    "route_policy_block_count": 0,
                    "route_policy_reroute_count": 0,
                    "route_policy_recovery_count": 0,
                    "route_policy": {},
                    "wakeword_route_policy": {},
                    "tts_route_policy": {},
                    "route_policy_summary": {},
                    "route_policy_timeline": [],
                    "wakeword_supervision": {},
                    "wakeword_supervision_status": "unknown",
                    "wakeword_supervision_reason": "",
                    "wakeword_supervision_last_changed_at": "",
                    "wakeword_supervision_restart_delay_s": float(
                        preserved_restart_state.get("wakeword_supervision_restart_delay_s", 0.0) or 0.0
                    ),
                    "wakeword_supervision_restart_not_before": str(
                        preserved_restart_state.get("wakeword_supervision_restart_not_before", "")
                    ).strip(),
                    "wakeword_supervision_sensitivity": float(
                        preserved_restart_state.get("wakeword_sensitivity", normalized.get("wakeword_sensitivity", 0.0)) or 0.0
                    ),
                    "wakeword_supervision_polling_bias": float(
                        preserved_restart_state.get("polling_bias", 0.0) or 0.0
                    ),
                    "wakeword_supervision_pause_count": 0,
                    "wakeword_supervision_resume_count": 0,
                    "wakeword_supervision_timeline": [],
                    "wakeword_start_failure_count": int(
                        preserved_restart_state.get("wakeword_start_failure_count", 0) or 0
                    ),
                    "wakeword_restart_policy": dict(preserved_restart_state.get("wakeword_restart_policy", {}))
                    if isinstance(preserved_restart_state.get("wakeword_restart_policy", {}), dict)
                    else {},
                    "wakeword_restart_timeline": list(
                        preserved_restart_state.get("wakeword_restart_timeline", [])
                    )
                    if isinstance(preserved_restart_state.get("wakeword_restart_timeline", []), list)
                    else [],
                    "wakeword_restart_backoff_count": int(
                        preserved_restart_state.get("wakeword_restart_backoff_count", 0) or 0
                    ),
                    "wakeword_restart_exhausted_count": int(
                        preserved_restart_state.get("wakeword_restart_exhausted_count", 0) or 0
                    ),
                    "wakeword_restart_recovery_expiry_count": int(
                        preserved_restart_state.get("wakeword_restart_recovery_expiry_count", 0) or 0
                    ),
                    "wakeword_restart_relaxation_count": int(
                        preserved_restart_state.get("wakeword_restart_relaxation_count", 0) or 0
                    ),
                    "wakeword_restart_last_expired_retry_at": str(
                        preserved_restart_state.get("wakeword_restart_last_expired_retry_at", "")
                    ).strip(),
                    "wakeword_restart_exhausted_until": str(
                        preserved_restart_state.get("wakeword_restart_exhausted_until", "")
                    ).strip(),
                    "wakeword_restart_last_exhausted_at": str(
                        preserved_restart_state.get("wakeword_restart_last_exhausted_at", "")
                    ).strip(),
                    "wakeword_restart_last_exhaustion_expired_at": str(
                        preserved_restart_state.get("wakeword_restart_last_exhaustion_expired_at", "")
                    ).strip(),
                    "wakeword_restart_last_relaxed_at": str(
                        preserved_restart_state.get("wakeword_restart_last_relaxed_at", "")
                    ).strip(),
                    "voice_mission_reliability": {},
                    "voice_route_recovery_recommendation": {},
                }
            )
            self._wakeword_restart_history = deque(
                preserved_restart_history[-240:],
                maxlen=self._wakeword_restart_history.maxlen or 240,
            )
            self._circuit_open_until_epoch = 0.0

            self._refresh_route_policy_snapshot(force=True)
            self._initialize_wakeword_if_enabled()
            self._thread = threading.Thread(target=self._worker_loop, name="jarvis-voice-session", daemon=True)
            self._thread.start()

        self._emit("voice.session_started", {"wakeword_status": self._state.get("wakeword_status", "disabled")})
        return self.status()

    def stop(self) -> Dict[str, Any]:
        with self._lock:
            if not self._running:
                return self.status()
            self._running = False
            self._stop_event.set()
            self._manual_trigger.set()
            self._wake_trigger.set()

        self._shutdown_wakeword()

        thread = self._thread
        if thread:
            thread.join(timeout=6.0)
        self._thread = None
        with self._lock:
            self._state["running"] = False
        self._emit("voice.session_stopped", {"reason": "user_request"})
        return self.status()

    def trigger_once(self, *, trigger_type: str = "manual") -> Dict[str, Any]:
        with self._lock:
            if not self._running:
                route_snapshot = self._refresh_route_policy_snapshot(force=True)
                route_gate = self._stt_route_gate(route_snapshot, trigger_type=trigger_type)
                if route_gate.get("blocked", False):
                    message = self._route_gate_message(task="stt", gate=route_gate)
                    self._state["last_error"] = message
                    self._state["last_rejection_reason"] = str(route_gate.get("reason_code", "route_policy_blocked"))
                    self._emit(
                        "voice.transcribe_blocked",
                        {
                            "trigger_type": trigger_type,
                            "task": "stt",
                            "message": message,
                            "reason_code": str(route_gate.get("reason_code", "")),
                            "cooldown_hint_s": float(route_gate.get("cooldown_hint_s", 0.0) or 0.0),
                        },
                    )
                    return {
                        "status": "error",
                        "running": False,
                        "accepted": False,
                        "message": message,
                        "route_policy": route_snapshot,
                    }
                duration = float(self._normalize_float(self._config.get("stt_duration_s", 4.0), minimum=1.0, maximum=20.0))
                result = self.stt_engine.transcribe(duration=duration)
                self._state["stt_backend"] = str(result.get("source") or self._state.get("stt_backend", "unknown"))
                status = str(result.get("status", "")).strip().lower()
                text = str(result.get("text", "")).strip()
                confidence = self._derive_confidence(result if isinstance(result, dict) else {}, text=text)
                self._state["last_confidence"] = round(float(confidence), 6)
                if status == "success" and text:
                    accepted, reason = self._validate_transcript(text=text, confidence=confidence, trigger_type=trigger_type)
                    if not accepted:
                        self._record_rejected_transcription(reason=reason, confidence=confidence)
                        return {
                            "status": "success",
                            "running": False,
                            "transcription": result,
                            "accepted": False,
                            "reason": reason,
                        }
                    self._record_success()
                else:
                    self._record_error(str(result.get("message", "STT failed")))
                return {"status": "success", "running": False, "transcription": result, "accepted": bool(status == "success" and text)}

            self._state["last_trigger_type"] = str(trigger_type or "manual")
            self._state["last_trigger_at"] = datetime.now(timezone.utc).isoformat()
            self._manual_trigger.set()

        self._emit(
            "voice.triggered",
            {"trigger_type": str(trigger_type or "manual"), "running": True},
        )
        return {"status": "success", "running": True, "queued": True}

    def status(self) -> Dict[str, Any]:
        with self._lock:
            config = dict(self._config)
            state = dict(self._state)
            state["config"] = config
            route_snapshot = dict(self._route_policy_snapshot) if isinstance(self._route_policy_snapshot, dict) else {}
            state["route_policy"] = dict(route_snapshot.get("stt", {})) if isinstance(route_snapshot.get("stt", {}), dict) else {}
            state["wakeword_route_policy"] = (
                dict(route_snapshot.get("wakeword", {})) if isinstance(route_snapshot.get("wakeword", {}), dict) else {}
            )
            state["tts_route_policy"] = dict(route_snapshot.get("tts", {})) if isinstance(route_snapshot.get("tts", {}), dict) else {}
            state["route_policy_summary"] = (
                dict(route_snapshot.get("summary", {})) if isinstance(route_snapshot.get("summary", {}), dict) else {}
            )
            state["route_policy_timeline"] = [dict(item) for item in list(self._route_policy_history)[-24:]]
            supervision_snapshot = dict(self._supervision_snapshot) if isinstance(self._supervision_snapshot, dict) else {}
            state["wakeword_supervision"] = (
                dict(supervision_snapshot.get("wakeword_supervision", {}))
                if isinstance(supervision_snapshot.get("wakeword_supervision", {}), dict)
                else {}
            )
            state["voice_mission_reliability"] = (
                dict(supervision_snapshot.get("mission_reliability", {}))
                if isinstance(supervision_snapshot.get("mission_reliability", {}), dict)
                else {}
            )
            state["voice_route_recovery_recommendation"] = (
                dict(supervision_snapshot.get("route_recovery_recommendation", {}))
                if isinstance(supervision_snapshot.get("route_recovery_recommendation", {}), dict)
                else {}
            )
            state["wakeword_supervision_timeline"] = [dict(item) for item in list(self._wakeword_supervision_history)[-24:]]
            state["wakeword_restart_timeline"] = [dict(item) for item in list(self._wakeword_restart_history)[-24:]]
        return state

    def restore_wakeword_restart_snapshot(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {"status": "error", "message": "invalid payload"}
        raw_items = payload.get("items", []) if isinstance(payload.get("items", []), list) else []
        current = payload.get("current", {}) if isinstance(payload.get("current", {}), dict) else {}
        diagnostics = payload.get("diagnostics", {}) if isinstance(payload.get("diagnostics", {}), dict) else {}
        sanitized_rows: List[Dict[str, Any]] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            row = {
                "event_id": str(item.get("event_id", "")).strip(),
                "occurred_at": str(item.get("occurred_at", "")).strip(),
                "event_type": str(item.get("event_type", "")).strip().lower(),
                "status": str(item.get("status", "")).strip().lower(),
                "reason_code": str(item.get("reason_code", "")).strip().lower(),
                "reason": str(item.get("reason", "")).strip(),
                "restart_delay_s": round(float(item.get("restart_delay_s", 0.0) or 0.0), 4),
                "next_retry_at": str(item.get("next_retry_at", "")).strip(),
                "exhausted_until": str(item.get("exhausted_until", "")).strip(),
                "failure_count": int(item.get("failure_count", 0) or 0),
                "wakeword_sensitivity": round(float(item.get("wakeword_sensitivity", 0.0) or 0.0), 4),
                "fallback_interval_s": round(float(item.get("fallback_interval_s", 0.0) or 0.0), 4),
                "resume_stability_s": round(float(item.get("resume_stability_s", 0.0) or 0.0), 4),
                "polling_bias": round(float(item.get("polling_bias", 0.0) or 0.0), 4),
                "recovered": bool(item.get("recovered", False)),
                "exhausted": bool(item.get("exhausted", False)),
                "policy": dict(item.get("policy", {})) if isinstance(item.get("policy", {}), dict) else {},
            }
            sanitized_rows.append(row)
        latest_row = sanitized_rows[-1] if sanitized_rows else {}
        policy = (
            dict(current.get("policy", {}))
            if isinstance(current.get("policy", {}), dict)
            else dict(latest_row.get("policy", {}))
            if isinstance(latest_row.get("policy", {}), dict)
            else {}
        )
        for key in (
            "drift_score",
            "recommended_profile",
            "profile_action",
            "profile_reason",
            "applied_profile",
            "applied_profile_reason",
            "profile_decision_source",
            "auto_profile_applied",
            "last_profile_shift_at",
            "profile_shift_count",
            "recent_exhaustion_rate",
            "recent_recovery_rate",
            "policy_recorded_at",
        ):
            if current.get(key) not in {"", None}:
                policy[key] = current.get(key)
            elif diagnostics.get(key) not in {"", None}:
                policy[key] = diagnostics.get(key)
        failure_count = int(
            current.get("failure_count", latest_row.get("failure_count", policy.get("recent_failures", 0))) or 0
        )
        backoff_count = int(
            current.get("backoff_count", latest_row.get("failure_count", policy.get("recent_failures", 0))) or 0
        )
        exhausted_count = int(
            current.get(
                "exhausted_count",
                max(
                    0,
                    int(policy.get("long_exhaustions", 0) or 0)
                    - int(policy.get("recommended_exhaustion_relaxation", 0) or 0),
                ),
            )
            or 0
        )
        recovery_expiry_count = int(
            current.get(
                "recovery_expiry_count",
                diagnostics.get(
                    "recovery_expiry_events",
                    sum(
                        1
                        for row in sanitized_rows
                        if str(row.get("event_type", "")).strip().lower()
                        in {"recovery_window_elapsed", "restart_exhaustion_expired"}
                    ),
                ),
            )
            or 0
        )
        relaxation_count = int(
            current.get(
                "relaxation_count",
                sum(
                    1
                    for row in sanitized_rows
                    if str(row.get("event_type", "")).strip().lower() == "restart_policy_relaxed"
                ),
            )
            or 0
        )
        exhausted_until = str(current.get("exhausted_until", latest_row.get("exhausted_until", "")) or "").strip()
        next_retry_at = str(current.get("next_retry_at", latest_row.get("next_retry_at", "")) or "").strip()
        restart_delay_s = round(
            float(current.get("restart_delay_s", latest_row.get("restart_delay_s", 0.0)) or 0.0),
            4,
        )
        restored_wakeword_sensitivity = round(
            self._normalize_float(
                current.get("wakeword_sensitivity", latest_row.get("wakeword_sensitivity", self._config.get("wakeword_sensitivity", 0.5))),
                minimum=0.0,
                maximum=1.0,
            ),
            4,
        )
        restored_fallback_interval_s = round(
            self._normalize_float(
                current.get(
                    "fallback_interval_s",
                    latest_row.get(
                        "fallback_interval_s",
                        policy.get("recommended_fallback_interval_s", self._config.get("fallback_interval_s", 10.0)),
                    ),
                ),
                minimum=0.5,
                maximum=60.0,
            ),
            4,
        )
        restored_resume_stability_s = round(
            self._normalize_float(
                current.get(
                    "resume_stability_s",
                    latest_row.get(
                        "resume_stability_s",
                        policy.get(
                            "recommended_resume_stability_s",
                            self._config.get("route_policy_resume_stability_s", 0.75),
                        ),
                    ),
                ),
                minimum=0.5,
                maximum=30.0,
            ),
            4,
        )
        restored_polling_bias = round(
            max(
                0.0,
                min(
                    1.0,
                    float(
                        current.get(
                            "polling_bias",
                            latest_row.get("polling_bias", policy.get("polling_bias", self._state.get("wakeword_supervision_polling_bias", 0.0))),
                        )
                        or 0.0
                    ),
                ),
            ),
            4,
        )
        last_expired_retry_at = ""
        last_exhausted_at = ""
        last_exhaustion_expired_at = ""
        last_relaxed_at = ""
        for row in reversed(sanitized_rows):
            event_type = str(row.get("event_type", "")).strip().lower()
            occurred_at = str(row.get("occurred_at", "")).strip()
            next_retry = str(row.get("next_retry_at", "")).strip()
            if not last_expired_retry_at and event_type == "recovery_window_elapsed":
                last_expired_retry_at = next_retry or occurred_at
            if not last_exhausted_at and event_type == "restart_exhausted":
                last_exhausted_at = occurred_at
            if not last_exhaustion_expired_at and event_type == "restart_exhaustion_expired":
                last_exhaustion_expired_at = occurred_at
            if not last_relaxed_at and event_type == "restart_policy_relaxed":
                last_relaxed_at = occurred_at
        with self._lock:
            self._wakeword_restart_history = deque(
                sanitized_rows[-240:],
                maxlen=self._wakeword_restart_history.maxlen or 240,
            )
            self._state["wakeword_restart_policy"] = dict(policy)
            self._state["wakeword_restart_timeline"] = [dict(item) for item in list(self._wakeword_restart_history)[-24:]]
            self._state["wakeword_start_failure_count"] = max(0, failure_count)
            self._state["wakeword_restart_backoff_count"] = max(0, backoff_count)
            self._state["wakeword_restart_exhausted_count"] = max(0, exhausted_count)
            self._state["wakeword_restart_recovery_expiry_count"] = max(0, recovery_expiry_count)
            self._state["wakeword_restart_relaxation_count"] = max(0, relaxation_count)
            self._state["wakeword_supervision_restart_delay_s"] = max(0.0, restart_delay_s)
            self._state["wakeword_supervision_restart_not_before"] = next_retry_at
            self._state["wakeword_restart_exhausted_until"] = exhausted_until
            self._state["wakeword_restart_last_expired_retry_at"] = last_expired_retry_at
            self._state["wakeword_restart_last_exhausted_at"] = last_exhausted_at
            self._state["wakeword_restart_last_exhaustion_expired_at"] = last_exhaustion_expired_at
            self._state["wakeword_restart_last_relaxed_at"] = last_relaxed_at
            self._state["wakeword_supervision_polling_bias"] = restored_polling_bias
            self._state["wakeword_restart_profile"] = str(
                policy.get("applied_profile", policy.get("recommended_profile", ""))
            ).strip().lower()
            self._state["wakeword_restart_profile_reason"] = str(
                policy.get("applied_profile_reason", policy.get("profile_reason", ""))
            ).strip()
            self._state["wakeword_restart_profile_decision_source"] = str(
                policy.get("profile_decision_source", "")
            ).strip().lower()
            self._state["wakeword_restart_profile_auto_applied"] = bool(
                policy.get("auto_profile_applied", False)
            )
            self._state["wakeword_restart_profile_shift_count"] = max(
                0,
                int(policy.get("profile_shift_count", 0) or 0),
            )
            self._state["wakeword_restart_last_profile_shift_at"] = str(
                policy.get("last_profile_shift_at", "")
            ).strip()
            self._config["wakeword_sensitivity"] = restored_wakeword_sensitivity
            self._config["fallback_interval_s"] = restored_fallback_interval_s
            self._config["route_policy_resume_stability_s"] = restored_resume_stability_s
        return {
            "status": "success",
            "restored": len(sanitized_rows),
            "backoff_count": max(0, backoff_count),
            "failure_count": max(0, failure_count),
            "exhausted_count": max(0, exhausted_count),
            "recovery_expiry_count": max(0, recovery_expiry_count),
            "relaxation_count": max(0, relaxation_count),
            "next_retry_at": next_retry_at,
            "exhausted_until": exhausted_until,
            "applied_config": {
                "wakeword_sensitivity": restored_wakeword_sensitivity,
                "fallback_interval_s": restored_fallback_interval_s,
                "resume_stability_s": restored_resume_stability_s,
                "polling_bias": restored_polling_bias,
                "applied_profile": str(policy.get("applied_profile", policy.get("recommended_profile", ""))).strip().lower(),
            },
        }

    @staticmethod
    def _extract_mission_context(metadata: Dict[str, Any]) -> Dict[str, str]:
        mission_id = ""
        for key in (
            "mission_id",
            "active_mission_id",
            "goal_id",
            "parent_goal_id",
            "conversation_mission_id",
        ):
            value = str(metadata.get(key, "") or "").strip()
            if value:
                mission_id = value
                break
        risk_level = str(
            metadata.get("risk_level", "")
            or metadata.get("mission_risk_level", "")
            or metadata.get("action_risk_level", "")
        ).strip().lower()
        policy_profile = str(
            metadata.get("policy_profile", "")
            or metadata.get("target_policy_profile", "")
        ).strip().lower()
        return {
            "mission_id": mission_id,
            "risk_level": risk_level,
            "policy_profile": policy_profile,
        }

    @staticmethod
    def _normalize_route_policy_provider(value: Any) -> str:
        clean = str(value or "").strip().lower()
        if clean in {"cloud", "remote"}:
            return "cloud"
        return clean

    def _normalize_route_policy_task(self, payload: Dict[str, Any] | None, *, task: str) -> Dict[str, Any]:
        row = dict(payload) if isinstance(payload, dict) else {}
        fallback_candidates: List[str] = []
        seen: set[str] = set()
        for item in row.get("fallback_candidates", []) if isinstance(row.get("fallback_candidates", []), list) else []:
            clean = self._normalize_route_policy_provider(item)
            if not clean or clean in seen:
                continue
            seen.add(clean)
            fallback_candidates.append(clean)
        selected_provider = self._normalize_route_policy_provider(row.get("selected_provider", ""))
        recommended_provider = self._normalize_route_policy_provider(row.get("recommended_provider", ""))
        cooldown_hint_s = max(0.0, float(row.get("cooldown_hint_s", 0.0) or 0.0))
        route_blocked = bool(row.get("route_blocked", False))
        route_adjusted = bool(row.get("route_adjusted", False))
        blacklisted = bool(row.get("blacklisted", False))
        suppressed = bool(row.get("suppressed", False))
        demoted = bool(row.get("demoted", False))
        local_route_viable = bool(row.get("local_route_viable", selected_provider == "local"))
        recovery_pending = bool(row.get("recovery_pending", False)) or cooldown_hint_s > 0.0
        status = "stable"
        if route_blocked:
            status = "blocked"
        elif recovery_pending:
            status = "recovery"
        elif route_adjusted:
            status = "rerouted"
        elif blacklisted or suppressed or demoted or not local_route_viable:
            status = "gated"
        next_retry_at = ""
        if cooldown_hint_s > 0.0:
            next_retry_at = datetime.fromtimestamp(time.time() + cooldown_hint_s, tz=timezone.utc).isoformat()
        return {
            "task": str(task or "").strip().lower(),
            "status": status,
            "selected_provider": selected_provider,
            "recommended_provider": recommended_provider,
            "selected_model": str(row.get("selected_model", "") or row.get("model", "")).strip(),
            "route_adjusted": route_adjusted,
            "route_blocked": route_blocked,
            "local_route_viable": local_route_viable,
            "autonomy_safe": bool(row.get("autonomy_safe", False)),
            "autonomous_allowed": bool(row.get("autonomous_allowed", True)),
            "review_required": bool(row.get("review_required", False)),
            "blacklisted": blacklisted,
            "suppressed": suppressed,
            "demoted": demoted,
            "recovery_pending": recovery_pending,
            "cooldown_hint_s": round(cooldown_hint_s, 6),
            "next_retry_at": next_retry_at,
            "reason_code": str(row.get("reason_code", "") or "").strip().lower(),
            "reason": str(row.get("reason", "") or row.get("route_warning", "") or "").strip(),
            "fallback_candidates": fallback_candidates,
            "summary": dict(row.get("summary", {})) if isinstance(row.get("summary", {}), dict) else {},
            "route_item": dict(row.get("route_item", {})) if isinstance(row.get("route_item", {}), dict) else {},
        }

    def _normalize_route_policy_snapshot(self, payload: Dict[str, Any] | None) -> Dict[str, Any]:
        row = dict(payload) if isinstance(payload, dict) else {}
        tasks: Dict[str, Dict[str, Any]] = {}
        for task in ("stt", "wakeword", "tts"):
            task_payload = row.get(task)
            if not isinstance(task_payload, dict) and str(row.get("task", "")).strip().lower() == task:
                task_payload = row
            tasks[task] = self._normalize_route_policy_task(task_payload if isinstance(task_payload, dict) else {}, task=task)
        blocked_tasks = [task for task, item in tasks.items() if item.get("status") == "blocked"]
        recovery_tasks = [task for task, item in tasks.items() if item.get("status") == "recovery"]
        rerouted_tasks = [task for task, item in tasks.items() if item.get("status") == "rerouted"]
        gated_tasks = [task for task, item in tasks.items() if item.get("status") == "gated"]
        summary_status = "stable"
        if blocked_tasks:
            summary_status = "blocked"
        elif recovery_tasks:
            summary_status = "recovery"
        elif rerouted_tasks:
            summary_status = "rerouted"
        elif gated_tasks:
            summary_status = "gated"
        next_retry_candidates = [
            str(item.get("next_retry_at", "")).strip()
            for item in tasks.values()
            if isinstance(item, dict) and str(item.get("next_retry_at", "")).strip()
        ]
        next_retry_at = min(next_retry_candidates) if next_retry_candidates else ""
        reason_code = ""
        reason = ""
        for task_name in ("stt", "wakeword", "tts"):
            item = tasks.get(task_name, {})
            if not isinstance(item, dict):
                continue
            if str(item.get("status", "")).strip().lower() in {"blocked", "recovery", "rerouted", "gated"}:
                reason_code = str(item.get("reason_code", "")).strip().lower()
                reason = str(item.get("reason", "")).strip()
                break
        return {
            "generated_at": float(row.get("generated_at", time.time()) or time.time()),
            "stt": tasks["stt"],
            "wakeword": tasks["wakeword"],
            "tts": tasks["tts"],
            "summary": {
                "status": summary_status,
                "blocked_tasks": blocked_tasks,
                "recovery_tasks": recovery_tasks,
                "rerouted_tasks": rerouted_tasks,
                "gated_tasks": gated_tasks,
                "reason_code": reason_code,
                "reason": reason,
                "next_retry_at": next_retry_at,
            },
        }

    def _route_policy_signature(self, snapshot: Dict[str, Any]) -> str:
        parts: List[str] = []
        for task in ("stt", "wakeword", "tts"):
            item = snapshot.get(task, {}) if isinstance(snapshot.get(task, {}), dict) else {}
            parts.append(
                ":".join(
                    [
                        task,
                        str(item.get("status", "")).strip().lower(),
                        str(item.get("reason_code", "")).strip().lower(),
                        str(item.get("selected_provider", "")).strip().lower(),
                        str(item.get("recommended_provider", "")).strip().lower(),
                        str(int(round(float(item.get("cooldown_hint_s", 0.0) or 0.0)))),
                    ]
                )
            )
        return "|".join(parts)

    def _task_route_changed(self, previous: Dict[str, Any], current: Dict[str, Any]) -> bool:
        keys = (
            "status",
            "reason_code",
            "selected_provider",
            "recommended_provider",
            "route_blocked",
            "route_adjusted",
            "blacklisted",
            "suppressed",
            "demoted",
            "recovery_pending",
            "cooldown_hint_s",
            "next_retry_at",
        )
        return any(previous.get(key) != current.get(key) for key in keys)

    def _record_route_policy_transition(self, snapshot: Dict[str, Any], *, source: str) -> None:
        previous_snapshot = dict(self._route_policy_snapshot) if isinstance(self._route_policy_snapshot, dict) else {}
        previous_signature = str(self._route_policy_last_signature or "")
        current_signature = self._route_policy_signature(snapshot)
        now_iso = datetime.now(timezone.utc).isoformat()
        events: List[Dict[str, Any]] = []
        if current_signature != previous_signature:
            for task in ("stt", "wakeword", "tts"):
                previous = previous_snapshot.get(task, {}) if isinstance(previous_snapshot.get(task, {}), dict) else {}
                current = snapshot.get(task, {}) if isinstance(snapshot.get(task, {}), dict) else {}
                if not self._task_route_changed(previous, current):
                    continue
                event = {
                    "event_id": f"voice-route-{task}-{int(time.time() * 1000)}",
                    "occurred_at": now_iso,
                    "source": str(source or "runtime").strip().lower() or "runtime",
                    "task": task,
                    "status": str(current.get("status", "")).strip().lower(),
                    "previous_status": str(previous.get("status", "")).strip().lower(),
                    "reason_code": str(current.get("reason_code", "")).strip().lower(),
                    "reason": str(current.get("reason", "")).strip(),
                    "selected_provider": str(current.get("selected_provider", "")).strip().lower(),
                    "recommended_provider": str(current.get("recommended_provider", "")).strip().lower(),
                    "route_blocked": bool(current.get("route_blocked", False)),
                    "route_adjusted": bool(current.get("route_adjusted", False)),
                    "blacklisted": bool(current.get("blacklisted", False)),
                    "suppressed": bool(current.get("suppressed", False)),
                    "demoted": bool(current.get("demoted", False)),
                    "recovery_pending": bool(current.get("recovery_pending", False)),
                    "cooldown_hint_s": round(float(current.get("cooldown_hint_s", 0.0) or 0.0), 6),
                    "next_retry_at": str(current.get("next_retry_at", "")).strip(),
                }
                events.append(event)
                self._route_policy_history.append(event)
                if event["status"] == "blocked":
                    self._state["route_policy_block_count"] = int(self._state.get("route_policy_block_count", 0)) + 1
                elif event["status"] == "rerouted":
                    self._state["route_policy_reroute_count"] = int(self._state.get("route_policy_reroute_count", 0)) + 1
                elif event["status"] == "recovery":
                    self._state["route_policy_recovery_count"] = int(self._state.get("route_policy_recovery_count", 0)) + 1
            self._state["route_policy_last_changed_at"] = now_iso
            summary = snapshot.get("summary", {}) if isinstance(snapshot.get("summary", {}), dict) else {}
            self._state["route_policy_status"] = str(summary.get("status", "unknown")).strip().lower() or "unknown"
            self._state["route_policy_reason"] = str(summary.get("reason", "") or summary.get("reason_code", "")).strip()
            self._state["route_policy_next_retry_at"] = str(summary.get("next_retry_at", "")).strip()
        self._state["route_policy"] = dict(snapshot.get("stt", {})) if isinstance(snapshot.get("stt", {}), dict) else {}
        self._state["wakeword_route_policy"] = (
            dict(snapshot.get("wakeword", {})) if isinstance(snapshot.get("wakeword", {}), dict) else {}
        )
        self._state["tts_route_policy"] = dict(snapshot.get("tts", {})) if isinstance(snapshot.get("tts", {}), dict) else {}
        self._state["route_policy_summary"] = (
            dict(snapshot.get("summary", {})) if isinstance(snapshot.get("summary", {}), dict) else {}
        )
        self._state["route_policy_timeline"] = [dict(item) for item in list(self._route_policy_history)[-24:]]
        self._route_policy_snapshot = snapshot
        self._route_policy_last_signature = current_signature
        if events:
            self._emit(
                "voice.route_policy_changed",
                {
                    "status": str(self._state.get("route_policy_status", "unknown")),
                    "reason": str(self._state.get("route_policy_reason", "")),
                    "events": events[:6],
                },
            )

    def _refresh_route_policy_snapshot(self, *, force: bool = False) -> Dict[str, Any]:
        now = time.time()
        provider = self._route_policy_provider
        with self._lock:
            current = dict(self._route_policy_snapshot) if isinstance(self._route_policy_snapshot, dict) else {}
            last_refresh = float(self._route_policy_last_refresh_epoch or 0.0)
        payload: Dict[str, Any]
        if callable(provider) and (
            force
            or not current
            or self._route_policy_snapshot_ttl_s <= 0.0
            or (now - last_refresh) >= self._route_policy_snapshot_ttl_s
        ):
            try:
                raw = provider()
            except Exception as exc:  # noqa: BLE001
                raw = {
                    "generated_at": now,
                    "stt": {
                        "task": "stt",
                        "route_blocked": True,
                        "reason_code": "route_policy_provider_error",
                        "reason": str(exc),
                        "cooldown_hint_s": 2.0,
                    },
                    "wakeword": {
                        "task": "wakeword",
                        "route_blocked": True,
                        "reason_code": "route_policy_provider_error",
                        "reason": str(exc),
                        "cooldown_hint_s": 2.0,
                    },
                }
            payload = self._normalize_route_policy_snapshot(raw if isinstance(raw, dict) else {})
            with self._lock:
                self._route_policy_last_refresh_epoch = now
                self._record_route_policy_transition(payload, source="provider")
            return payload
        return current

    @staticmethod
    def _normalize_supervision_status(value: Any) -> str:
        clean = str(value or "").strip().lower()
        if clean in {"active", "observe", "wakeword", "healthy"}:
            return "active"
        if clean in {"hybrid_polling", "hybrid", "deferred_recovery"}:
            return "hybrid_polling"
        if clean in {"polling_only", "polling", "disabled"}:
            return "polling_only"
        if clean in {"blocked", "hold", "recovery"}:
            return clean
        return "unknown"

    def _normalize_supervision_snapshot(self, payload: Dict[str, Any] | None) -> Dict[str, Any]:
        raw = dict(payload or {}) if isinstance(payload, dict) else {}
        wakeword_supervision = raw.get("wakeword_supervision") if isinstance(raw.get("wakeword_supervision"), dict) else {}
        session_overrides = raw.get("session_overrides") if isinstance(raw.get("session_overrides"), dict) else {}
        mission_reliability = raw.get("mission_reliability") if isinstance(raw.get("mission_reliability"), dict) else {}
        recovery_payload = (
            raw.get("route_recovery_recommendation")
            if isinstance(raw.get("route_recovery_recommendation"), dict)
            else {}
        )
        restart_delay_s = max(0.0, float(wakeword_supervision.get("restart_delay_s", 0.0) or 0.0))
        next_retry_at = str(wakeword_supervision.get("next_retry_at", "") or "").strip()
        return {
            "mission_id": str(raw.get("mission_id", "") or "").strip(),
            "risk_level": str(raw.get("risk_level", "") or "").strip().lower(),
            "policy_profile": str(raw.get("policy_profile", "") or "").strip().lower(),
            "generated_at": float(raw.get("generated_at", time.time()) or time.time()),
            "wakeword_supervision": {
                "status": self._normalize_supervision_status(wakeword_supervision.get("status", "")),
                "strategy": str(
                    wakeword_supervision.get("strategy", recovery_payload.get("wakeword_strategy", ""))
                    or ""
                ).strip().lower(),
                "allow_wakeword": bool(wakeword_supervision.get("allow_wakeword", True)),
                "reason_code": str(wakeword_supervision.get("reason_code", "") or "").strip().lower(),
                "reason": str(wakeword_supervision.get("reason", "") or "").strip(),
                "restart_delay_s": round(restart_delay_s, 4),
                "next_retry_at": next_retry_at,
                "wakeword_sensitivity": max(
                    0.0,
                    float(wakeword_supervision.get("wakeword_sensitivity", 0.0) or 0.0),
                ),
                "polling_bias": max(
                    0.0,
                    min(1.0, float(wakeword_supervision.get("polling_bias", 0.0) or 0.0)),
                ),
                "fallback_interval_s": max(
                    0.0,
                    float(
                        wakeword_supervision.get(
                            "fallback_interval_s",
                            session_overrides.get("fallback_interval_s", 0.0),
                        )
                        or 0.0
                    ),
                ),
                "resume_stability_s": max(
                    0.0,
                    float(
                        wakeword_supervision.get(
                            "resume_stability_s",
                            session_overrides.get("route_policy_resume_stability_s", 0.0),
                        )
                        or 0.0
                    ),
                ),
            },
            "session_overrides": dict(session_overrides),
            "mission_reliability": dict(mission_reliability),
            "route_recovery_recommendation": dict(recovery_payload),
        }

    def _update_supervision_state(self, snapshot: Dict[str, Any], *, previous: Optional[Dict[str, Any]] = None) -> None:
        prev = dict(previous or {}) if isinstance(previous, dict) else {}
        wakeword_supervision = (
            dict(snapshot.get("wakeword_supervision", {}))
            if isinstance(snapshot.get("wakeword_supervision", {}), dict)
            else {}
        )
        previous_supervision = (
            dict(prev.get("wakeword_supervision", {}))
            if isinstance(prev.get("wakeword_supervision", {}), dict)
            else {}
        )
        current_status = str(wakeword_supervision.get("status", "unknown") or "unknown").strip().lower() or "unknown"
        previous_status = str(previous_supervision.get("status", "unknown") or "unknown").strip().lower() or "unknown"
        current_reason = str(wakeword_supervision.get("reason_code", "") or wakeword_supervision.get("reason", "")).strip().lower()
        previous_reason = str(
            previous_supervision.get("reason_code", "") or previous_supervision.get("reason", "")
        ).strip().lower()
        changed = current_status != previous_status or current_reason != previous_reason
        now_iso = datetime.now(timezone.utc).isoformat()
        self._state["wakeword_supervision"] = wakeword_supervision
        self._state["wakeword_supervision_status"] = current_status
        self._state["wakeword_supervision_reason"] = current_reason
        self._state["wakeword_supervision_restart_delay_s"] = round(
            float(wakeword_supervision.get("restart_delay_s", 0.0) or 0.0),
            4,
        )
        self._state["wakeword_supervision_sensitivity"] = round(
            float(wakeword_supervision.get("wakeword_sensitivity", 0.0) or 0.0),
            4,
        )
        self._state["wakeword_supervision_polling_bias"] = round(
            float(wakeword_supervision.get("polling_bias", 0.0) or 0.0),
            4,
        )
        if str(wakeword_supervision.get("next_retry_at", "")).strip():
            self._state["wakeword_supervision_restart_not_before"] = str(
                wakeword_supervision.get("next_retry_at", "")
            ).strip()
        elif current_status == "active":
            self._state["wakeword_supervision_restart_not_before"] = ""
        self._state["voice_mission_reliability"] = (
            dict(snapshot.get("mission_reliability", {}))
            if isinstance(snapshot.get("mission_reliability", {}), dict)
            else {}
        )
        self._state["voice_route_recovery_recommendation"] = (
            dict(snapshot.get("route_recovery_recommendation", {}))
            if isinstance(snapshot.get("route_recovery_recommendation", {}), dict)
            else {}
        )
        if changed:
            self._state["wakeword_supervision_last_changed_at"] = now_iso
            if current_status in {"polling_only", "hybrid_polling", "blocked", "recovery"}:
                self._state["wakeword_supervision_pause_count"] = int(
                    self._state.get("wakeword_supervision_pause_count", 0) or 0
                ) + 1
            elif previous_status in {"polling_only", "hybrid_polling", "blocked", "recovery"} and current_status == "active":
                self._state["wakeword_supervision_resume_count"] = int(
                    self._state.get("wakeword_supervision_resume_count", 0) or 0
                ) + 1
            history_row = {
                "event_id": f"wakeword-supervision-{int(time.time() * 1000)}",
                "occurred_at": now_iso,
                "mission_id": str(snapshot.get("mission_id", "")).strip(),
                "risk_level": str(snapshot.get("risk_level", "")).strip().lower(),
                "policy_profile": str(snapshot.get("policy_profile", "")).strip().lower(),
                "status": current_status,
                "previous_status": previous_status,
                "reason_code": str(wakeword_supervision.get("reason_code", "") or "").strip().lower(),
                "reason": str(wakeword_supervision.get("reason", "") or "").strip(),
                "strategy": str(wakeword_supervision.get("strategy", "") or "").strip().lower(),
                "allow_wakeword": bool(wakeword_supervision.get("allow_wakeword", True)),
                "restart_delay_s": round(float(wakeword_supervision.get("restart_delay_s", 0.0) or 0.0), 4),
                "next_retry_at": str(wakeword_supervision.get("next_retry_at", "") or "").strip(),
                "fallback_interval_s": round(float(wakeword_supervision.get("fallback_interval_s", 0.0) or 0.0), 4),
                "resume_stability_s": round(float(wakeword_supervision.get("resume_stability_s", 0.0) or 0.0), 4),
                "local_voice_pressure_score": round(
                    float(snapshot.get("local_voice_pressure_score", 0.0) or 0.0),
                    6,
                ),
                "mission_sessions": int(
                    self._state.get("voice_mission_reliability", {}).get("sessions", 0)
                    if isinstance(self._state.get("voice_mission_reliability", {}), dict)
                    else 0
                ),
                "wakeword_gate_events": int(
                    self._state.get("voice_mission_reliability", {}).get("wakeword_gate_events", 0)
                    if isinstance(self._state.get("voice_mission_reliability", {}), dict)
                    else 0
                ),
                "route_policy_pause_count": int(
                    self._state.get("voice_mission_reliability", {}).get("route_policy_pause_count", 0)
                    if isinstance(self._state.get("voice_mission_reliability", {}), dict)
                    else 0
                ),
                "route_policy_resume_count": int(
                    self._state.get("voice_mission_reliability", {}).get("route_policy_resume_count", 0)
                    if isinstance(self._state.get("voice_mission_reliability", {}), dict)
                    else 0
                ),
                "recovered": bool(
                    previous_status in {"polling_only", "hybrid_polling", "blocked", "recovery"}
                    and current_status == "active"
                ),
            }
            self._wakeword_supervision_history.append(history_row)
            self._state["wakeword_supervision_timeline"] = [
                dict(item) for item in list(self._wakeword_supervision_history)[-24:]
            ]
            self._emit(
                "voice.wakeword_supervision_changed",
                {
                    "status": current_status,
                    "reason": current_reason,
                    "mission_id": str(snapshot.get("mission_id", "")).strip(),
                },
            )

    def _apply_wakeword_runtime_tuning(self, supervision_row: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(supervision_row, dict):
            return {"changed": False, "restart_required": False, "applied": {}}
        restart_policy = self._compute_wakeword_restart_policy(supervision_row)
        changed: Dict[str, Any] = {}
        sensitivity = float(supervision_row.get("wakeword_sensitivity", 0.0) or 0.0)
        if sensitivity > 0.0:
            target_sensitivity = round(self._normalize_float(sensitivity, minimum=0.1, maximum=1.0), 4)
            current_sensitivity = round(float(self._config.get("wakeword_sensitivity", 0.6) or 0.6), 4)
            if abs(current_sensitivity - target_sensitivity) >= 0.02:
                self._config["wakeword_sensitivity"] = target_sensitivity
                changed["wakeword_sensitivity"] = target_sensitivity
        fallback_interval_s = max(
            float(supervision_row.get("fallback_interval_s", 0.0) or 0.0),
            float(restart_policy.get("recommended_fallback_interval_s", 0.0) or 0.0),
        )
        if fallback_interval_s > 0.0:
            target_fallback_interval_s = round(
                self._normalize_float(
                    max(fallback_interval_s, float(restart_policy.get("recommended_fallback_interval_s", fallback_interval_s) or fallback_interval_s)),
                    minimum=0.5,
                    maximum=60.0,
                ),
                4,
            )
            current_fallback_interval_s = round(float(self._config.get("fallback_interval_s", 10.0) or 10.0), 4)
            if abs(current_fallback_interval_s - target_fallback_interval_s) >= 0.05:
                self._config["fallback_interval_s"] = target_fallback_interval_s
                changed["fallback_interval_s"] = target_fallback_interval_s
        resume_stability_s = max(
            float(supervision_row.get("resume_stability_s", 0.0) or 0.0),
            float(restart_policy.get("recommended_resume_stability_s", 0.0) or 0.0),
        )
        if resume_stability_s > 0.0:
            target_resume_stability_s = round(
                self._normalize_float(
                    max(
                        resume_stability_s,
                        float(restart_policy.get("recommended_resume_stability_s", resume_stability_s) or resume_stability_s),
                    ),
                    minimum=0.1,
                    maximum=30.0,
                ),
                4,
            )
            current_resume_stability_s = round(float(self._config.get("route_policy_resume_stability_s", 0.75) or 0.75), 4)
            if abs(current_resume_stability_s - target_resume_stability_s) >= 0.05:
                self._config["route_policy_resume_stability_s"] = target_resume_stability_s
                changed["route_policy_resume_stability_s"] = target_resume_stability_s
        if changed:
            self._emit(
                "voice.wakeword_runtime_tuned",
                {
                    "changes": dict(changed),
                    "strategy": str(supervision_row.get("strategy", "") or "").strip().lower(),
                    "reason_code": str(supervision_row.get("reason_code", "") or "").strip().lower(),
                    "restart_policy": dict(restart_policy),
                },
            )
        return {
            "changed": bool(changed),
            "restart_required": bool("wakeword_sensitivity" in changed and self._wakeword_engine is not None),
            "applied": dict(changed),
        }

    def _schedule_wakeword_restart_backoff(
        self,
        *,
        supervision_row: Dict[str, Any],
        reason_code: str,
        message: str,
    ) -> Dict[str, Any]:
        failure_count = int(self._state.get("wakeword_start_failure_count", 0) or 0) + 1
        self._state["wakeword_start_failure_count"] = failure_count
        base_delay_s = max(0.5, float(supervision_row.get("restart_delay_s", 1.5) or 1.5))
        polling_bias = max(0.0, min(1.0, float(supervision_row.get("polling_bias", 0.0) or 0.0)))
        previous_policy = (
            dict(self._state.get("wakeword_restart_policy", {}))
            if isinstance(self._state.get("wakeword_restart_policy", {}), dict)
            else {}
        )
        restart_policy = self._compute_wakeword_restart_policy(supervision_row)
        backoff_scale = float(restart_policy.get("cooldown_scale", 1.0) or 1.0)
        restart_delay_s = round(min(120.0, max(base_delay_s, 1.0) * backoff_scale), 4)
        restart_epoch = time.time() + restart_delay_s
        standard_retry_at = datetime.fromtimestamp(restart_epoch, tz=timezone.utc).isoformat()
        self._state["wakeword_supervision_restart_delay_s"] = restart_delay_s
        self._state["wakeword_restart_backoff_count"] = int(self._state.get("wakeword_restart_backoff_count", 0) or 0) + 1
        exhausted = bool(restart_policy.get("exhausted", False))
        previously_exhausted = bool(previous_policy.get("exhausted", False))
        exhausted_until = ""
        if exhausted:
            exhaustion_epoch = time.time() + max(
                restart_delay_s,
                float(restart_policy.get("recovery_expiry_s", restart_delay_s) or restart_delay_s),
            )
            exhausted_until = datetime.fromtimestamp(exhaustion_epoch, tz=timezone.utc).isoformat()
            self._state["wakeword_restart_exhausted_until"] = exhausted_until
            if not previously_exhausted:
                self._state["wakeword_restart_last_exhausted_at"] = datetime.now(timezone.utc).isoformat()
        else:
            self._state["wakeword_restart_exhausted_until"] = ""
        next_retry_at = exhausted_until or standard_retry_at
        self._state["wakeword_supervision_restart_not_before"] = next_retry_at
        if exhausted:
            self._state["wakeword_restart_exhausted_count"] = int(
                self._state.get("wakeword_restart_exhausted_count", 0) or 0
            ) + 1
        self._record_wakeword_restart_event(
            event_type="restart_backoff",
            status=self._state.get("wakeword_status", ""),
            reason_code=reason_code,
            reason=message,
            restart_delay_s=restart_delay_s,
            next_retry_at=next_retry_at,
            failure_count=failure_count,
            exhausted=exhausted,
            exhausted_until=exhausted_until,
            policy=restart_policy,
        )
        if exhausted and not previously_exhausted:
            self._record_wakeword_restart_event(
                event_type="restart_exhausted",
                status=self._state.get("wakeword_status", ""),
                reason_code=reason_code or "restart_exhausted",
                reason="Wakeword restart failures crossed the adaptive exhaustion threshold.",
                restart_delay_s=restart_delay_s,
                next_retry_at=next_retry_at,
                failure_count=failure_count,
                exhausted=True,
                exhausted_until=exhausted_until,
                policy=restart_policy,
            )
            self._emit(
                "voice.wakeword_restart_exhausted",
                {
                    "reason_code": reason_code,
                    "failure_count": failure_count,
                    "restart_delay_s": restart_delay_s,
                    "next_retry_at": next_retry_at,
                    "exhausted_until": exhausted_until,
                    "restart_policy": dict(restart_policy),
                },
            )
        self._emit(
            "voice.wakeword_restart_backoff",
            {
                "reason_code": reason_code,
                "message": message,
                "failure_count": failure_count,
                "restart_delay_s": restart_delay_s,
                "next_retry_at": next_retry_at,
                "standard_retry_at": standard_retry_at,
                "polling_bias": round(polling_bias, 4),
                "restart_policy": dict(restart_policy),
                "exhausted": exhausted,
                "exhausted_until": exhausted_until,
            },
        )
        return {
            "failure_count": failure_count,
            "restart_delay_s": restart_delay_s,
            "next_retry_at": next_retry_at,
            "exhausted": exhausted,
            "exhausted_until": exhausted_until,
            "restart_policy": restart_policy,
        }

    def _refresh_supervision_snapshot(
        self,
        *,
        force: bool = False,
        route_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        provider = self._supervision_provider
        if provider is None:
            return dict(self._supervision_snapshot) if isinstance(self._supervision_snapshot, dict) else {}
        now = time.time()
        with self._lock:
            current = dict(self._supervision_snapshot) if isinstance(self._supervision_snapshot, dict) else {}
            last_refresh = float(self._supervision_last_refresh_epoch or 0.0)
        if (
            force
            or not current
            or self._supervision_snapshot_ttl_s <= 0.0
            or (now - last_refresh) >= self._supervision_snapshot_ttl_s
        ):
            metadata = self._config.get("metadata") if isinstance(self._config.get("metadata"), dict) else {}
            mission_context = self._extract_mission_context(metadata if isinstance(metadata, dict) else {})
            context = {
                "metadata": dict(metadata) if isinstance(metadata, dict) else {},
                "config": dict(self._config),
                "state": dict(self._state),
                "route_policy": (
                    dict(route_snapshot) if isinstance(route_snapshot, dict) else dict(self._route_policy_snapshot)
                ),
                **mission_context,
            }
            try:
                raw = provider(context)
            except Exception as exc:  # noqa: BLE001
                raw = {
                    "mission_id": mission_context.get("mission_id", ""),
                    "risk_level": mission_context.get("risk_level", ""),
                    "policy_profile": mission_context.get("policy_profile", ""),
                    "wakeword_supervision": {
                        "status": "unknown",
                        "allow_wakeword": True,
                        "reason_code": "supervision_provider_error",
                        "reason": str(exc),
                    },
                }
            payload = self._normalize_supervision_snapshot(raw if isinstance(raw, dict) else {})
            with self._lock:
                previous = dict(self._supervision_snapshot) if isinstance(self._supervision_snapshot, dict) else {}
                self._supervision_snapshot = payload
                self._supervision_last_refresh_epoch = now
                self._update_supervision_state(payload, previous=previous)
            return payload
        return current

    def route_policy_status(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        return self._refresh_route_policy_snapshot(force=force_refresh)

    def supervision_status(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        return self._refresh_supervision_snapshot(force=force_refresh)

    def route_policy_timeline(self, *, limit: int = 60) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 180))
        snapshot = self._refresh_route_policy_snapshot(force=False)
        with self._lock:
            items = [dict(item) for item in list(self._route_policy_history)[-bounded:]]
        status_counts: Dict[str, int] = {}
        task_counts: Dict[str, int] = {}
        for item in items:
            status = str(item.get("status", "")).strip().lower() or "unknown"
            task = str(item.get("task", "")).strip().lower() or "unknown"
            status_counts[status] = status_counts.get(status, 0) + 1
            task_counts[task] = task_counts.get(task, 0) + 1
        return {
            "status": "success",
            "count": len(items),
            "limit": bounded,
            "items": items,
            "current": snapshot,
            "diagnostics": {
                "status_counts": status_counts,
                "task_counts": task_counts,
                "next_retry_at": str(snapshot.get("summary", {}).get("next_retry_at", "")).strip()
                if isinstance(snapshot.get("summary", {}), dict)
                else "",
            },
        }

    def wakeword_supervision_timeline(self, *, limit: int = 60) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 180))
        snapshot = self._refresh_supervision_snapshot(force=False)
        current = (
            dict(snapshot.get("wakeword_supervision", {}))
            if isinstance(snapshot.get("wakeword_supervision", {}), dict)
            else {}
        )
        with self._lock:
            items = [dict(item) for item in list(self._wakeword_supervision_history)[-bounded:]]
        status_counts: Dict[str, int] = {}
        strategy_counts: Dict[str, int] = {}
        timeline_buckets: Dict[str, Dict[str, Any]] = {}
        latest_event_at = ""
        latest_next_retry_at = ""
        latest_active_at = ""
        latest_pause_at = ""
        recovered_events = 0
        deferred_events = 0
        total_restart_delay_s = 0.0
        restart_delay_samples = 0
        for row in items:
            status = str(row.get("status", "")).strip().lower() or "unknown"
            strategy = str(row.get("strategy", "")).strip().lower() or "unknown"
            status_counts[status] = status_counts.get(status, 0) + 1
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
            if status == "active":
                latest_active_at = str(row.get("occurred_at", "") or latest_active_at)
            if status in {"polling_only", "hybrid_polling", "blocked", "recovery"}:
                latest_pause_at = str(row.get("occurred_at", "") or latest_pause_at)
            if bool(row.get("recovered", False)):
                recovered_events += 1
            if status in {"recovery", "hybrid_polling"}:
                deferred_events += 1
            restart_delay_s = float(row.get("restart_delay_s", 0.0) or 0.0)
            if restart_delay_s > 0:
                total_restart_delay_s += restart_delay_s
                restart_delay_samples += 1
            occurred_at = str(row.get("occurred_at", "")).strip()
            if occurred_at:
                latest_event_at = occurred_at
                stamp = self._parse_epoch_seconds(occurred_at)
                if stamp > 0:
                    bucket_dt = datetime.fromtimestamp(stamp, tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
                    bucket_key = bucket_dt.isoformat()
                    bucket = timeline_buckets.setdefault(
                        bucket_key,
                        {
                            "bucket_start": bucket_key,
                            "count": 0,
                            "active_count": 0,
                            "paused_count": 0,
                            "recovered_count": 0,
                        },
                    )
                    bucket["count"] = int(bucket.get("count", 0) or 0) + 1
                    if status == "active":
                        bucket["active_count"] = int(bucket.get("active_count", 0) or 0) + 1
                    if status in {"polling_only", "hybrid_polling", "blocked", "recovery"}:
                        bucket["paused_count"] = int(bucket.get("paused_count", 0) or 0) + 1
                    if bool(row.get("recovered", False)):
                        bucket["recovered_count"] = int(bucket.get("recovered_count", 0) or 0) + 1
            next_retry_at = str(row.get("next_retry_at", "")).strip()
            if self._parse_epoch_seconds(next_retry_at) >= self._parse_epoch_seconds(latest_next_retry_at):
                latest_next_retry_at = next_retry_at
        bucket_items = [timeline_buckets[key] for key in sorted(timeline_buckets.keys())[-24:]]
        return {
            "status": "success",
            "count": len(items),
            "limit": bounded,
            "items": items,
            "current": current,
            "diagnostics": {
                "status_counts": status_counts,
                "strategy_counts": strategy_counts,
                "recovered_events": int(recovered_events),
                "deferred_events": int(deferred_events),
                "latest_event_at": latest_event_at,
                "latest_next_retry_at": latest_next_retry_at,
                "latest_active_at": latest_active_at,
                "latest_pause_at": latest_pause_at,
                "avg_restart_delay_s": round(total_restart_delay_s / restart_delay_samples, 4)
                if restart_delay_samples
                else 0.0,
                "timeline_buckets": bucket_items,
            },
        }

    @staticmethod
    def _wakeword_restart_event_failed(row: Dict[str, Any]) -> bool:
        event_type = str(row.get("event_type", "")).strip().lower()
        status = str(row.get("status", "")).strip().lower()
        return event_type in {"start_failed", "restart_backoff"} or status.startswith("degraded:")

    def _compute_wakeword_restart_policy(self, supervision_row: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            history = [dict(item) for item in list(self._wakeword_restart_history)]
        now = time.time()
        window_s = 900.0
        long_window_s = 21600.0
        recent_rows = [
            row
            for row in history
            if (stamp := self._parse_epoch_seconds(str(row.get("occurred_at", "")).strip())) > 0
            and (now - stamp) <= window_s
        ]
        long_rows = [
            row
            for row in history
            if (stamp := self._parse_epoch_seconds(str(row.get("occurred_at", "")).strip())) > 0
            and (now - stamp) <= long_window_s
        ]
        recent_failures = sum(1 for row in recent_rows if self._wakeword_restart_event_failed(row))
        recent_successes = sum(
            1
            for row in recent_rows
            if str(row.get("event_type", "")).strip().lower() in {"started", "recovered", "recovery_window_elapsed"}
        )
        long_failures = sum(1 for row in long_rows if self._wakeword_restart_event_failed(row))
        long_successes = sum(
            1
            for row in long_rows
            if str(row.get("event_type", "")).strip().lower()
            in {"started", "recovered", "recovery_window_elapsed", "restart_exhaustion_expired"}
        )
        long_exhaustions = sum(
            1
            for row in long_rows
            if str(row.get("event_type", "")).strip().lower() == "restart_exhausted"
        )
        long_recoveries = sum(
            1
            for row in long_rows
            if bool(row.get("recovered", False))
            or str(row.get("event_type", "")).strip().lower() in {"recovered", "restart_exhaustion_expired"}
        )
        consecutive_failures = 0
        for row in reversed(recent_rows):
            if self._wakeword_restart_event_failed(row):
                consecutive_failures += 1
                continue
            break
        polling_bias = max(0.0, min(1.0, float(supervision_row.get("polling_bias", 0.0) or 0.0)))
        recent_recovery_ratio = float(recent_successes) / float(max(1, recent_failures + recent_successes))
        long_recovery_ratio = float(long_successes) / float(max(1, long_failures + long_successes))
        prior_policy = (
            dict(self._state.get("wakeword_restart_policy", {}))
            if isinstance(self._state.get("wakeword_restart_policy", {}), dict)
            else {}
        )
        persisted_drift_score = max(0.0, min(1.0, float(prior_policy.get("drift_score", 0.0) or 0.0)))
        persisted_profile = str(prior_policy.get("recommended_profile", "") or "").strip().lower()
        persisted_profile_action = str(prior_policy.get("profile_action", "") or "").strip().lower()
        persisted_profile_reason = str(prior_policy.get("profile_reason", "") or "").strip()
        persisted_recent_exhaustion_rate = max(
            0.0,
            min(1.0, float(prior_policy.get("recent_exhaustion_rate", 0.0) or 0.0)),
        )
        persisted_recent_recovery_rate = max(
            0.0,
            min(1.0, float(prior_policy.get("recent_recovery_rate", 0.0) or 0.0)),
        )
        recovery_credit = max(
            0.0,
            min(
                2.0,
                (float(recent_successes) * 0.22)
                + (float(long_recoveries) * 0.08)
                + (long_recovery_ratio * 0.95)
                - (float(recent_failures) * 0.06),
            ),
        )
        effective_long_exhaustions = max(0.0, float(long_exhaustions) - min(float(long_exhaustions), float(long_recoveries) * 0.45))
        base_threshold = 4
        if persisted_profile == "stability_guard" or persisted_profile_action == "demote":
            base_threshold -= 1
        elif persisted_profile == "hybrid_guarded" or persisted_profile_action == "guard":
            if persisted_drift_score >= 0.36 or persisted_recent_exhaustion_rate >= 0.2:
                base_threshold -= 1
        elif persisted_profile == "recovered_wakeword" or persisted_profile_action == "recover":
            recovery_credit = min(
                2.0,
                recovery_credit
                + 0.18
                + min(0.12, persisted_recent_recovery_rate * 0.18)
                - min(0.05, persisted_drift_score * 0.04),
            )
            base_threshold += 1
        if polling_bias >= 0.35:
            base_threshold -= 1
        if recent_failures >= 4:
            base_threshold -= 1
        if recent_successes >= max(1, recent_failures):
            base_threshold += 1
        if long_failures >= 8:
            base_threshold -= 1
        if effective_long_exhaustions >= 2:
            base_threshold -= 1
        if long_recovery_ratio >= 0.62 and long_successes >= 3:
            base_threshold += 1
        if recent_successes >= 2 and recent_recovery_ratio >= 0.58:
            base_threshold += 1
        if recent_successes >= 3 and long_recovery_ratio >= 0.45 and recent_failures <= recent_successes:
            base_threshold += 1
        max_failures_before_polling = max(2, min(6, base_threshold))
        threshold_bias = int(max_failures_before_polling - 4)
        raw_cooldown_scale = max(
            1.0,
            min(
                5.25,
                1.0
                + polling_bias
                + min(1.25, float(recent_failures) * 0.22)
                + min(1.0, float(max(0, consecutive_failures - 1)) * 0.27)
                + min(0.9, float(long_failures) * 0.08)
                + min(0.85, effective_long_exhaustions * 0.22)
                + min(0.75, persisted_drift_score * 0.6)
                + min(0.45, persisted_recent_exhaustion_rate * 0.7)
            ),
        )
        if persisted_profile == "recovered_wakeword" or persisted_profile_action == "recover":
            raw_cooldown_scale = max(
                1.0,
                raw_cooldown_scale
                - min(0.55, (persisted_recent_recovery_rate * 0.32) + (recovery_credit * 0.08)),
            )
        cooldown_recovery_factor = round(
            max(0.55, min(1.0, 1.0 - min(0.42, recovery_credit * 0.18))),
            4,
        )
        cooldown_scale = round(max(1.0, raw_cooldown_scale * cooldown_recovery_factor), 4)
        exhausted = consecutive_failures >= max_failures_before_polling and recent_failures > 0
        recommended_delay_decay_factor = round(
            max(0.45, min(0.95, 0.94 - min(0.36, recovery_credit * 0.16))),
            4,
        )
        recommended_backoff_relaxation = int(
            max(
                0,
                min(
                    3,
                    int(recovery_credit >= 0.55)
                    + int(recovery_credit >= 1.0)
                    + int(long_recovery_ratio >= 0.68 and recent_recovery_ratio >= 0.5),
                ),
            )
        )
        recommended_exhaustion_relaxation = int(
            1
            if long_recovery_ratio >= 0.52
            and effective_long_exhaustions <= max(1.0, float(long_exhaustions) * 0.75)
            else 0
        )
        recommended_fallback_interval_s = round(
            self._normalize_float(
                supervision_row.get("fallback_interval_s", self._config.get("fallback_interval_s", 10.0)),
                minimum=0.5,
                maximum=60.0,
            )
            * (
                1.0
                + min(1.0, polling_bias + (0.18 * recent_failures))
                + min(0.75, float(long_exhaustions) * 0.16)
                + min(0.4, persisted_drift_score * 0.45)
                - min(0.18, persisted_recent_recovery_rate * 0.22)
            ),
            4,
        )
        recommended_resume_stability_s = round(
            self._normalize_float(
                max(
                    float(supervision_row.get("resume_stability_s", self._config.get("route_policy_resume_stability_s", 0.75)) or 0.75),
                    0.75
                    + (0.25 * polling_bias)
                    + min(2.25, float(recent_failures) * 0.2)
                    + min(3.0, float(long_exhaustions) * 0.75)
                    + min(1.75, persisted_drift_score * 1.35)
                    + min(1.2, persisted_recent_exhaustion_rate * 2.0)
                    - min(0.4, long_recovery_ratio * 0.35)
                    - min(0.5, persisted_recent_recovery_rate * 0.42)
                ),
                minimum=0.5,
                maximum=30.0,
            ),
            4,
        )
        recovery_expiry_s = round(
            self._normalize_float(
                max(
                    recommended_fallback_interval_s,
                    float(supervision_row.get("restart_delay_s", 1.5) or 1.5) * 2.0,
                )
                * (
                    1.0
                    + min(0.9, polling_bias + float(max(0, consecutive_failures - 1)) * 0.12)
                    + min(0.8, float(long_exhaustions) * 0.24)
                    + min(0.45, persisted_drift_score * 0.5)
                    + min(0.3, persisted_recent_exhaustion_rate * 0.6)
                ),
                minimum=2.0,
                maximum=180.0,
            ),
            4,
        )
        policy = {
            "window_s": window_s,
            "long_window_s": long_window_s,
            "recent_failures": int(recent_failures),
            "recent_successes": int(recent_successes),
            "recent_recovery_ratio": round(recent_recovery_ratio, 6),
            "long_failures": int(long_failures),
            "long_successes": int(long_successes),
            "long_exhaustions": int(long_exhaustions),
            "long_recoveries": int(long_recoveries),
            "long_recovery_ratio": round(long_recovery_ratio, 6),
            "effective_long_exhaustions": round(effective_long_exhaustions, 6),
            "recovery_credit": round(recovery_credit, 6),
            "consecutive_failures": int(consecutive_failures),
            "polling_bias": round(polling_bias, 4),
            "threshold_bias": int(threshold_bias),
            "max_failures_before_polling": int(max_failures_before_polling),
            "cooldown_recovery_factor": float(cooldown_recovery_factor),
            "cooldown_scale": float(cooldown_scale),
            "recommended_delay_decay_factor": float(recommended_delay_decay_factor),
            "recommended_backoff_relaxation": int(recommended_backoff_relaxation),
            "recommended_exhaustion_relaxation": int(recommended_exhaustion_relaxation),
            "recommended_fallback_interval_s": float(recommended_fallback_interval_s),
            "recommended_resume_stability_s": float(recommended_resume_stability_s),
            "recovery_expiry_s": float(recovery_expiry_s),
            "drift_score": round(persisted_drift_score, 6),
            "recommended_profile": persisted_profile,
            "profile_action": persisted_profile_action,
            "profile_reason": persisted_profile_reason,
            "recent_exhaustion_rate": round(persisted_recent_exhaustion_rate, 6),
            "recent_recovery_rate": round(persisted_recent_recovery_rate, 6),
            "exhausted": bool(exhausted),
        }
        with self._lock:
            self._state["wakeword_restart_policy"] = dict(policy)
        return policy

    def _relax_wakeword_restart_state(
        self,
        supervision_row: Dict[str, Any],
        *,
        trigger: str,
    ) -> Dict[str, Any]:
        policy = self._compute_wakeword_restart_policy(supervision_row)
        backoff_relaxation = max(0, int(policy.get("recommended_backoff_relaxation", 0) or 0))
        exhaustion_relaxation = max(0, int(policy.get("recommended_exhaustion_relaxation", 0) or 0))
        delay_decay_factor = max(0.45, min(1.0, float(policy.get("recommended_delay_decay_factor", 1.0) or 1.0)))
        cooldown_recovery_factor = max(0.35, min(1.0, float(policy.get("cooldown_recovery_factor", 1.0) or 1.0)))
        recovery_credit = max(0.0, float(policy.get("recovery_credit", 0.0) or 0.0))
        resume_stability_s = max(
            0.5,
            float(policy.get("recommended_resume_stability_s", self._config.get("route_policy_resume_stability_s", 0.75)) or 0.75),
        )
        recovery_expiry_s = max(
            resume_stability_s,
            float(policy.get("recovery_expiry_s", resume_stability_s) or resume_stability_s),
        )
        relaxed_failure_count = max(
            0,
            backoff_relaxation
            + int(recovery_credit >= 0.85)
            + int(cooldown_recovery_factor <= 0.82),
        )
        now_epoch = time.time()

        def _relax_retry_stamp(stamp_text: str, *, minimum_s: float, clear_bias: float = 0.0) -> str:
            stamp = self._parse_epoch_seconds(str(stamp_text or "").strip())
            if stamp <= 0:
                return ""
            remaining_s = stamp - now_epoch
            if remaining_s <= 0:
                return ""
            decay = max(0.2, delay_decay_factor * cooldown_recovery_factor)
            relaxed_remaining_s = remaining_s * decay
            if recovery_credit >= 1.1:
                relaxed_remaining_s = min(relaxed_remaining_s, remaining_s * 0.45)
            if relaxed_remaining_s <= max(minimum_s * 0.5, clear_bias):
                return ""
            return datetime.fromtimestamp(now_epoch + max(minimum_s, relaxed_remaining_s), tz=timezone.utc).isoformat()

        with self._lock:
            previous_backoff = int(self._state.get("wakeword_restart_backoff_count", 0) or 0)
            previous_exhausted = int(self._state.get("wakeword_restart_exhausted_count", 0) or 0)
            previous_delay = float(self._state.get("wakeword_supervision_restart_delay_s", 0.0) or 0.0)
            previous_failure_count = int(self._state.get("wakeword_start_failure_count", 0) or 0)
            previous_retry_at = str(self._state.get("wakeword_supervision_restart_not_before", "")).strip()
            previous_exhausted_until = str(self._state.get("wakeword_restart_exhausted_until", "")).strip()
            if (
                previous_backoff <= 0
                and previous_exhausted <= 0
                and previous_delay <= 0
                and previous_failure_count <= 0
                and not previous_retry_at
                and not previous_exhausted_until
            ):
                return {"status": "noop", "policy": policy}
            new_backoff = max(0, previous_backoff - backoff_relaxation)
            new_exhausted = max(0, previous_exhausted - exhaustion_relaxation)
            new_failure_count = max(0, previous_failure_count - relaxed_failure_count)
            base_delay = max(
                0.0,
                float(
                    supervision_row.get("restart_delay_s", self._config.get("route_policy_resume_stability_s", 0.0))
                    or 0.0
                ),
            )
            new_delay = round(max(base_delay, previous_delay * delay_decay_factor), 4) if previous_delay > 0 else 0.0
            new_retry_at = _relax_retry_stamp(
                previous_retry_at,
                minimum_s=max(base_delay, resume_stability_s * 0.5),
                clear_bias=0.35 * resume_stability_s,
            )
            if new_backoff <= 0 and recovery_credit >= 0.75:
                new_retry_at = ""
            new_exhausted_until = _relax_retry_stamp(
                previous_exhausted_until,
                minimum_s=recovery_expiry_s,
                clear_bias=resume_stability_s,
            )
            if new_exhausted <= 0 and recovery_credit >= 0.7:
                new_exhausted_until = ""
            if (
                new_backoff == previous_backoff
                and new_exhausted == previous_exhausted
                and abs(new_delay - previous_delay) < 1e-6
                and new_failure_count == previous_failure_count
                and new_retry_at == previous_retry_at
                and new_exhausted_until == previous_exhausted_until
            ):
                return {"status": "noop", "policy": policy}
            self._state["wakeword_restart_backoff_count"] = new_backoff
            self._state["wakeword_restart_exhausted_count"] = new_exhausted
            self._state["wakeword_start_failure_count"] = new_failure_count
            self._state["wakeword_supervision_restart_delay_s"] = new_delay
            self._state["wakeword_supervision_restart_not_before"] = new_retry_at
            self._state["wakeword_restart_exhausted_until"] = new_exhausted_until
            self._state["wakeword_restart_relaxation_count"] = int(
                self._state.get("wakeword_restart_relaxation_count", 0) or 0
            ) + 1
            self._state["wakeword_restart_last_relaxed_at"] = datetime.now(timezone.utc).isoformat()
            current_status = str(self._state.get("wakeword_status", "")).strip()
            relaxation_count = int(self._state.get("wakeword_restart_relaxation_count", 0) or 0)
            relaxed_at = str(self._state.get("wakeword_restart_last_relaxed_at", "")).strip()
        self._record_wakeword_restart_event(
            event_type="restart_policy_relaxed",
            status=current_status,
            reason_code="sustained_recovery",
            reason=f"Wakeword restart penalties relaxed after {trigger}.",
            restart_delay_s=new_delay,
            next_retry_at=new_retry_at,
            failure_count=new_failure_count,
            recovered=True,
            exhausted=bool(new_exhausted > 0),
            exhausted_until=new_exhausted_until,
            policy=policy,
        )
        self._emit(
            "voice.wakeword_restart_relaxed",
            {
                "trigger": trigger,
                "backoff_count": new_backoff,
                "exhausted_count": new_exhausted,
                "start_failure_count": new_failure_count,
                "restart_delay_s": new_delay,
                "next_retry_at": new_retry_at,
                "exhausted_until": new_exhausted_until,
                "relaxation_count": relaxation_count,
                "relaxed_at": relaxed_at,
                "policy": policy,
            },
        )
        return {
            "status": "relaxed",
            "trigger": trigger,
            "backoff_count": new_backoff,
            "exhausted_count": new_exhausted,
            "start_failure_count": new_failure_count,
            "restart_delay_s": new_delay,
            "next_retry_at": new_retry_at,
            "exhausted_until": new_exhausted_until,
            "relaxation_count": relaxation_count,
            "policy": policy,
        }

    def _record_wakeword_restart_event(
        self,
        *,
        event_type: str,
        status: str,
        reason_code: str = "",
        reason: str = "",
        restart_delay_s: float = 0.0,
        next_retry_at: str = "",
        failure_count: int = 0,
        recovered: bool = False,
        exhausted: bool = False,
        exhausted_until: str = "",
        policy: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        row = {
            "event_id": f"wakeword-restart-{int(time.time() * 1000)}",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "event_type": str(event_type or "unknown").strip().lower() or "unknown",
            "status": str(status or "").strip().lower(),
            "reason_code": str(reason_code or "").strip().lower(),
            "reason": str(reason or "").strip(),
            "restart_delay_s": round(max(0.0, float(restart_delay_s or 0.0)), 4),
            "next_retry_at": str(next_retry_at or "").strip(),
            "exhausted_until": str(exhausted_until or "").strip(),
            "failure_count": int(max(0, int(failure_count or 0))),
            "wakeword_sensitivity": round(float(self._config.get("wakeword_sensitivity", 0.0) or 0.0), 4),
            "fallback_interval_s": round(float(self._config.get("fallback_interval_s", 0.0) or 0.0), 4),
            "resume_stability_s": round(float(self._config.get("route_policy_resume_stability_s", 0.0) or 0.0), 4),
            "polling_bias": round(float(self._state.get("wakeword_supervision_polling_bias", 0.0) or 0.0), 4),
            "recovered": bool(recovered),
            "exhausted": bool(exhausted),
            "policy": dict(policy or self._state.get("wakeword_restart_policy", {})),
        }
        with self._lock:
            self._wakeword_restart_history.append(row)
            self._state["wakeword_restart_timeline"] = [dict(item) for item in list(self._wakeword_restart_history)[-24:]]
        return row

    def wakeword_restart_timeline(self, *, limit: int = 80, event_type: str = "") -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 240))
        clean_event_type = str(event_type or "").strip().lower()
        with self._lock:
            current = dict(self._state.get("wakeword_restart_policy", {}))
            current["next_retry_at"] = str(self._state.get("wakeword_supervision_restart_not_before", "")).strip()
            current["exhausted_until"] = str(self._state.get("wakeword_restart_exhausted_until", "")).strip()
            current["last_exhausted_at"] = str(self._state.get("wakeword_restart_last_exhausted_at", "")).strip()
            current["last_exhaustion_expired_at"] = str(
                self._state.get("wakeword_restart_last_exhaustion_expired_at", "")
            ).strip()
            current["recovery_expiry_count"] = int(self._state.get("wakeword_restart_recovery_expiry_count", 0) or 0)
            items = [dict(item) for item in list(self._wakeword_restart_history)]
        if clean_event_type:
            items = [item for item in items if str(item.get("event_type", "")).strip().lower() == clean_event_type]
        total = len(items)
        items = items[-bounded:]
        event_counts: Dict[str, int] = {}
        timeline_buckets: Dict[str, Dict[str, Any]] = {}
        exhausted_events = 0
        recovered_events = 0
        recovery_expiry_events = 0
        exhaustion_transition_count = 0
        latest_event_at = ""
        latest_next_retry_at = ""
        latest_exhausted_at = ""
        latest_exhausted_until = ""
        latest_recovery_expiry_at = ""
        total_restart_delay_s = 0.0
        restart_delay_samples = 0
        for row in items:
            event_key = str(row.get("event_type", "")).strip().lower() or "unknown"
            event_counts[event_key] = event_counts.get(event_key, 0) + 1
            if bool(row.get("exhausted", False)):
                exhausted_events += 1
            if bool(row.get("recovered", False)):
                recovered_events += 1
            if event_key in {"recovery_window_elapsed", "restart_exhaustion_expired"}:
                recovery_expiry_events += 1
                latest_recovery_expiry_at = str(row.get("occurred_at", "")).strip() or latest_recovery_expiry_at
            if event_key == "restart_exhausted":
                exhaustion_transition_count += 1
                latest_exhausted_at = str(row.get("occurred_at", "")).strip() or latest_exhausted_at
            restart_delay_s = float(row.get("restart_delay_s", 0.0) or 0.0)
            if restart_delay_s > 0:
                total_restart_delay_s += restart_delay_s
                restart_delay_samples += 1
            occurred_at = str(row.get("occurred_at", "")).strip()
            if occurred_at:
                latest_event_at = occurred_at
                stamp = self._parse_epoch_seconds(occurred_at)
                if stamp > 0:
                    bucket_dt = datetime.fromtimestamp(stamp, tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
                    bucket_key = bucket_dt.isoformat()
                    bucket = timeline_buckets.setdefault(
                        bucket_key,
                        {
                            "bucket_start": bucket_key,
                            "count": 0,
                            "failure_count": 0,
                            "recovered_count": 0,
                            "exhausted_count": 0,
                            "expiry_count": 0,
                            "exhaustion_transition_count": 0,
                        },
                    )
                    bucket["count"] = int(bucket.get("count", 0) or 0) + 1
                    if self._wakeword_restart_event_failed(row):
                        bucket["failure_count"] = int(bucket.get("failure_count", 0) or 0) + 1
                    if bool(row.get("recovered", False)):
                        bucket["recovered_count"] = int(bucket.get("recovered_count", 0) or 0) + 1
                    if bool(row.get("exhausted", False)):
                        bucket["exhausted_count"] = int(bucket.get("exhausted_count", 0) or 0) + 1
                    if event_key in {"recovery_window_elapsed", "restart_exhaustion_expired"}:
                        bucket["expiry_count"] = int(bucket.get("expiry_count", 0) or 0) + 1
                    if event_key == "restart_exhausted":
                        bucket["exhaustion_transition_count"] = int(
                            bucket.get("exhaustion_transition_count", 0) or 0
                        ) + 1
            next_retry_at = str(row.get("next_retry_at", "")).strip()
            if self._parse_epoch_seconds(next_retry_at) >= self._parse_epoch_seconds(latest_next_retry_at):
                latest_next_retry_at = next_retry_at
            exhausted_until = str(row.get("exhausted_until", "")).strip()
            if self._parse_epoch_seconds(exhausted_until) >= self._parse_epoch_seconds(latest_exhausted_until):
                latest_exhausted_until = exhausted_until
        bucket_items = [timeline_buckets[key] for key in sorted(timeline_buckets.keys())[-24:]]
        return {
            "status": "success",
            "count": len(items),
            "total": total,
            "limit": bounded,
            "event_type_filter": clean_event_type,
            "items": items,
            "current": current,
            "diagnostics": {
                "event_counts": event_counts,
                "exhausted_events": int(exhausted_events),
                "recovered_events": int(recovered_events),
                "recovery_expiry_events": int(recovery_expiry_events),
                "exhaustion_transition_count": int(exhaustion_transition_count),
                "latest_event_at": latest_event_at,
                "latest_next_retry_at": latest_next_retry_at,
                "latest_exhausted_at": latest_exhausted_at,
                "latest_exhausted_until": latest_exhausted_until,
                "latest_recovery_expiry_at": latest_recovery_expiry_at,
                "avg_restart_delay_s": round(total_restart_delay_s / restart_delay_samples, 4)
                if restart_delay_samples
                else 0.0,
                "timeline_buckets": bucket_items,
            },
        }

    @staticmethod
    def _task_route_allowed(task_policy: Dict[str, Any], *, allow_reroute: bool = True) -> bool:
        if not isinstance(task_policy, dict):
            return True
        if bool(task_policy.get("route_blocked", False)):
            return False
        if bool(task_policy.get("blacklisted", False)) or bool(task_policy.get("suppressed", False)) or bool(task_policy.get("demoted", False)):
            if allow_reroute and str(task_policy.get("recommended_provider", "")).strip().lower() not in {"", "local"}:
                return True
            return False
        if bool(task_policy.get("recovery_pending", False)):
            if allow_reroute and str(task_policy.get("recommended_provider", "")).strip().lower() not in {"", "local"}:
                return True
            return False
        if not bool(task_policy.get("local_route_viable", True)):
            if allow_reroute and str(task_policy.get("recommended_provider", "")).strip().lower() not in {"", "local"}:
                return True
            return False
        return True

    def _stt_route_gate(self, snapshot: Dict[str, Any], *, trigger_type: str) -> Dict[str, Any]:
        stt_policy = snapshot.get("stt", {}) if isinstance(snapshot.get("stt", {}), dict) else {}
        allowed = self._task_route_allowed(stt_policy, allow_reroute=True)
        blocked = not allowed and bool(stt_policy.get("route_blocked", False))
        return {
            "blocked": blocked,
            "task": "stt",
            "trigger_type": str(trigger_type or "voice").strip().lower() or "voice",
            "reason_code": str(stt_policy.get("reason_code", "")).strip().lower(),
            "reason": str(stt_policy.get("reason", "")).strip(),
            "cooldown_hint_s": float(stt_policy.get("cooldown_hint_s", 0.0) or 0.0),
            "policy": dict(stt_policy),
        }

    @staticmethod
    def _route_gate_message(*, task: str, gate: Dict[str, Any]) -> str:
        reason = str(gate.get("reason", "")).strip()
        task_name = str(task or "route").strip().upper()
        if reason:
            return reason
        reason_code = str(gate.get("reason_code", "")).strip().lower() or "route_policy_blocked"
        return f"{task_name} route blocked by policy ({reason_code})."

    @staticmethod
    def _parse_epoch_seconds(value: str) -> float:
        clean = str(value or "").strip()
        if not clean:
            return 0.0
        try:
            return datetime.fromisoformat(clean.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

    def _reconcile_wakeword_route_policy(self, snapshot: Dict[str, Any]) -> None:
        wake_policy = snapshot.get("wakeword", {}) if isinstance(snapshot.get("wakeword", {}), dict) else {}
        supervision = self._refresh_supervision_snapshot(force=False, route_snapshot=snapshot)
        supervision_row = (
            supervision.get("wakeword_supervision")
            if isinstance(supervision.get("wakeword_supervision"), dict)
            else {}
        )
        tuning_result = self._apply_wakeword_runtime_tuning(supervision_row)
        use_wakeword = bool(self._config.get("wakeword_enabled", False))
        if not use_wakeword:
            if self._wakeword_engine is not None:
                self._shutdown_wakeword()
            self._state["wakeword_status"] = "disabled"
            return
        previous_status = str(self._state.get("wakeword_status", "")).strip().lower()
        allowed = self._task_route_allowed(wake_policy, allow_reroute=False)
        if not allowed:
            if self._wakeword_engine is not None:
                self._shutdown_wakeword()
            reason_code = str(wake_policy.get("reason_code", "")).strip().lower() or "route_policy_gated"
            next_retry_at = str(wake_policy.get("next_retry_at", "")).strip()
            new_status = f"gated:{reason_code}"
            self._state["wakeword_status"] = new_status
            if next_retry_at:
                self._state["route_policy_next_retry_at"] = next_retry_at
            if previous_status != new_status:
                self._state["wakeword_gate_count"] = int(self._state.get("wakeword_gate_count", 0) or 0) + 1
                self._state["wakeword_last_gated_at"] = datetime.now(timezone.utc).isoformat()
                self._emit(
                    "voice.wakeword_gated",
                    {
                        "reason_code": reason_code,
                        "reason": str(wake_policy.get("reason", "")).strip(),
                        "cooldown_hint_s": float(wake_policy.get("cooldown_hint_s", 0.0) or 0.0),
                        "next_retry_at": next_retry_at,
                    },
                )
            return
        if not bool(supervision_row.get("allow_wakeword", True)):
            if self._wakeword_engine is not None:
                self._shutdown_wakeword()
            reason_code = str(supervision_row.get("reason_code", "")).strip().lower() or "mission_recovery_policy"
            next_retry_at = str(supervision_row.get("next_retry_at", "")).strip()
            new_status = f"gated:{reason_code}"
            self._state["wakeword_status"] = new_status
            if next_retry_at:
                self._state["wakeword_supervision_restart_not_before"] = next_retry_at
            if previous_status != new_status:
                self._state["wakeword_gate_count"] = int(self._state.get("wakeword_gate_count", 0) or 0) + 1
                self._state["wakeword_last_gated_at"] = datetime.now(timezone.utc).isoformat()
                self._emit(
                    "voice.wakeword_gated",
                    {
                        "reason_code": reason_code,
                        "reason": str(supervision_row.get("reason", "")).strip(),
                        "cooldown_hint_s": float(supervision_row.get("restart_delay_s", 0.0) or 0.0),
                        "next_retry_at": next_retry_at,
                        "source": "mission_supervision",
                    },
                )
            return
        if bool(tuning_result.get("restart_required", False)) and self._wakeword_engine is not None:
            self._shutdown_wakeword()
            self._state["wakeword_status"] = "restarting:runtime_tuning"
            self._record_wakeword_restart_event(
                event_type="restart_required",
                status=self._state.get("wakeword_status", ""),
                reason_code="runtime_tuning_changed",
                reason="Adaptive wakeword tuning requires engine restart.",
            )
            self._emit(
                "voice.wakeword_restart_required",
                {
                    "reason_code": "runtime_tuning_changed",
                    "applied": dict(tuning_result.get("applied", {})),
                },
            )
        restart_not_before = self._parse_epoch_seconds(
            str(self._state.get("wakeword_supervision_restart_not_before", "")).strip()
        )
        exhausted_until = self._parse_epoch_seconds(str(self._state.get("wakeword_restart_exhausted_until", "")).strip())
        if self._wakeword_engine is None and exhausted_until > 0:
            exhausted_retry_at = str(self._state.get("wakeword_restart_exhausted_until", "")).strip()
            last_exhaustion_expired = str(self._state.get("wakeword_restart_last_exhaustion_expired_at", "")).strip()
            if exhausted_until <= time.time() and exhausted_retry_at and exhausted_retry_at != last_exhaustion_expired:
                self._state["wakeword_restart_last_exhaustion_expired_at"] = exhausted_retry_at
                self._state["wakeword_restart_last_expired_retry_at"] = exhausted_retry_at
                self._state["wakeword_restart_recovery_expiry_count"] = int(
                    self._state.get("wakeword_restart_recovery_expiry_count", 0) or 0
                ) + 1
                self._state["wakeword_restart_exhausted_until"] = ""
                self._record_wakeword_restart_event(
                    event_type="restart_exhaustion_expired",
                    status=self._state.get("wakeword_status", ""),
                    reason_code=str(supervision_row.get("reason_code", "")).strip().lower(),
                    reason="Wakeword restart exhaustion recovery window elapsed.",
                    next_retry_at=exhausted_retry_at,
                    recovered=True,
                    exhausted=False,
                    policy=dict(self._state.get("wakeword_restart_policy", {}))
                    if isinstance(self._state.get("wakeword_restart_policy", {}), dict)
                    else {},
                )
                self._emit(
                    "voice.wakeword_restart_expiry_elapsed",
                    {
                        "next_retry_at": exhausted_retry_at,
                        "policy": dict(self._state.get("wakeword_restart_policy", {}))
                        if isinstance(self._state.get("wakeword_restart_policy", {}), dict)
                        else {},
                    },
                )
        if self._wakeword_engine is None and restart_not_before > 0:
            retry_at = str(self._state.get("wakeword_supervision_restart_not_before", "")).strip()
            last_expired = str(self._state.get("wakeword_restart_last_expired_retry_at", "")).strip()
            if restart_not_before <= time.time() and retry_at and retry_at != last_expired:
                self._state["wakeword_restart_last_expired_retry_at"] = retry_at
                self._record_wakeword_restart_event(
                    event_type="recovery_window_elapsed",
                    status=self._state.get("wakeword_status", ""),
                    reason_code=str(supervision_row.get("reason_code", "")).strip().lower(),
                    reason="Wakeword restart recovery window elapsed.",
                    next_retry_at=retry_at,
                    recovered=True,
                )
        if self._wakeword_engine is None and restart_not_before > time.time():
            self._state["wakeword_status"] = "recovery:mission_recovery_policy"
            return
        if self._wakeword_engine is None:
            self._initialize_wakeword_if_enabled(supervision_snapshot=supervision)

    def _worker_loop(self) -> None:
        last_poll = 0.0
        last_capture = 0.0
        while not self._stop_event.is_set():
            try:
                circuit_remaining_s = self._circuit_remaining_seconds()
                if circuit_remaining_s > 0:
                    time.sleep(min(0.25, circuit_remaining_s))
                    continue
                now = time.monotonic()
                cooldown_s = float(self._config.get("cooldown_s", 0.8))
                route_snapshot = self._refresh_route_policy_snapshot(force=False)
                supervision_snapshot = self._refresh_supervision_snapshot(force=False, route_snapshot=route_snapshot)
                supervision_row = (
                    supervision_snapshot.get("wakeword_supervision")
                    if isinstance(supervision_snapshot.get("wakeword_supervision"), dict)
                    else {}
                )
                fallback_interval_s = float(
                    supervision_row.get("fallback_interval_s", self._config.get("fallback_interval_s", 10.0))
                    or self._config.get("fallback_interval_s", 10.0)
                )
                use_wakeword = bool(self._config.get("wakeword_enabled", False))
                self._reconcile_wakeword_route_policy(route_snapshot)
                should_capture = False
                trigger_type = ""
                wakeword_ready = str(self._state.get("wakeword_status", "")).strip().lower() == "active"

                if self._manual_trigger.is_set():
                    self._manual_trigger.clear()
                    should_capture = True
                    trigger_type = "manual"
                elif self._wake_trigger.is_set():
                    self._wake_trigger.clear()
                    should_capture = True
                    trigger_type = "wakeword"
                elif ((not use_wakeword) or not wakeword_ready) and fallback_interval_s > 0 and (now - last_poll) >= fallback_interval_s:
                    last_poll = now
                    should_capture = True
                    trigger_type = "wakeword_fallback" if use_wakeword and not wakeword_ready else "polling"

                if not should_capture:
                    time.sleep(0.08)
                    continue

                route_gate = self._stt_route_gate(route_snapshot, trigger_type=trigger_type)
                if route_gate.get("blocked", False):
                    message = self._route_gate_message(task="stt", gate=route_gate)
                    with self._lock:
                        self._state["last_error"] = message
                        self._state["last_rejection_reason"] = str(route_gate.get("reason_code", "route_policy_blocked"))
                    self._emit(
                        "voice.transcribe_blocked",
                        {
                            "trigger_type": trigger_type,
                            "task": "stt",
                            "message": message,
                            "reason_code": str(route_gate.get("reason_code", "")),
                            "cooldown_hint_s": float(route_gate.get("cooldown_hint_s", 0.0) or 0.0),
                        },
                    )
                    time.sleep(max(0.08, min(0.8, float(route_gate.get("cooldown_hint_s", 0.2) or 0.2))))
                    continue

                if cooldown_s > 0 and (now - last_capture) < cooldown_s:
                    time.sleep(max(0.05, cooldown_s - (now - last_capture)))
                last_capture = time.monotonic()
                if not self._consume_rate_budget(
                    queue_name="transcribe",
                    max_per_minute=int(self._config.get("max_transcriptions_per_minute", 90)),
                ):
                    self._emit(
                        "voice.rate_limited",
                        {
                            "scope": "transcribe",
                            "trigger_type": trigger_type,
                            "max_per_minute": int(self._config.get("max_transcriptions_per_minute", 90)),
                        },
                    )
                    continue
                self._run_transcription(trigger_type=trigger_type)
            except Exception as exc:  # noqa: BLE001
                self._record_error(str(exc))
                self._emit("voice.loop_error", {"message": str(exc)})
                time.sleep(0.25)

    def _run_transcription(self, *, trigger_type: str) -> None:
        duration_s = float(self._normalize_float(self._config.get("stt_duration_s", 4.0), minimum=1.0, maximum=20.0))
        stt_mode = str(self._config.get("stt_mode", "auto")).strip().lower() or "auto"
        self._emit(
            "voice.transcribe_started",
            {
                "trigger_type": trigger_type,
                "duration_s": duration_s,
                "adaptive_profile": self._config.get("adaptive_profile", {}),
            },
        )
        if stt_mode in {"stream", "vad", "auto"}:
            stream_result = self.stt_engine.transcribe_stream(
                max_duration_s=duration_s,
                frame_duration_s=float(self._config.get("vad_frame_s", 0.2)),
                energy_threshold=float(self._config.get("vad_energy_threshold", 0.015)),
                silence_duration_s=float(self._config.get("vad_silence_s", 0.9)),
                min_speech_s=float(self._config.get("vad_min_speech_s", 0.35)),
                sample_rate=16000,
                fallback_to_chunk=(stt_mode == "auto"),
            )
            if stream_result.get("status") == "success" or stt_mode in {"stream", "vad"}:
                result = stream_result
            else:
                result = self.stt_engine.transcribe(duration=duration_s)
        else:
            result = self.stt_engine.transcribe(duration=duration_s)
        status = str(result.get("status", "")).strip().lower()
        text = str(result.get("text", "")).strip()
        source = str(result.get("source", "")).strip() or "unknown"
        model = str(result.get("model", "")).strip()
        confidence = self._derive_confidence(result, text=text)

        with self._lock:
            self._state["stt_backend"] = source
            self._state["last_trigger_type"] = trigger_type
            self._state["last_trigger_at"] = datetime.now(timezone.utc).isoformat()
            self._state["last_confidence"] = round(float(confidence), 6)
            if status == "success" and text:
                self._state["last_transcript"] = text
                self._state["transcription_count"] = int(self._state.get("transcription_count", 0)) + 1
            else:
                self._record_error(str(result.get("message", "STT failed")))

        if status != "success" or not text:
            self._emit(
                "voice.transcribe_failed",
                {
                    "trigger_type": trigger_type,
                    "source": source,
                    "model": model,
                    "confidence": round(float(confidence), 6),
                    "message": str(result.get("message", "STT failed")),
                },
            )
            return

        accepted, reject_reason = self._validate_transcript(
            text=text,
            confidence=confidence,
            trigger_type=trigger_type,
        )
        if not accepted:
            self._record_rejected_transcription(reason=reject_reason, confidence=confidence)
            self._emit(
                "voice.transcribe_rejected",
                {
                    "trigger_type": trigger_type,
                    "source": source,
                    "model": model,
                    "confidence": round(float(confidence), 6),
                    "reason": reject_reason,
                },
            )
            return

        self._record_success(emit_event=False)
        self._emit(
            "voice.transcribed",
            {
                "trigger_type": trigger_type,
                "source": source,
                "model": model,
                "text": text,
                "confidence": round(float(confidence), 6),
                "attempt_chain": result.get("attempt_chain", []),
                "adaptive_profile": self._config.get("adaptive_profile", {}),
            },
        )

        if not self._consume_rate_budget(
            queue_name="callback",
            max_per_minute=int(self._config.get("max_callbacks_per_minute", 90)),
        ):
            self._emit(
                "voice.rate_limited",
                {
                    "scope": "callback",
                    "trigger_type": trigger_type,
                    "max_per_minute": int(self._config.get("max_callbacks_per_minute", 90)),
                },
            )
            return

        callback_context: Dict[str, Any] = {
            "trigger_type": trigger_type,
            "stt_source": source,
            "model": model,
            "confidence": round(float(confidence), 6),
            "attempt_chain": result.get("attempt_chain", []),
            "capture": result.get("capture", {}),
            "auto_submit": bool(self._config.get("auto_submit", True)),
            "auto_tts": bool(self._config.get("auto_tts", True)),
            "goal_source": "voice-loop",
            "metadata": self._config.get("metadata", {}),
            "adaptive_profile": self._config.get("adaptive_profile", {}),
        }
        callback_started = time.monotonic()
        try:
            response = self.on_transcript(text, callback_context)
        except Exception as exc:  # noqa: BLE001
            self._record_callback_latency(max(0.0, (time.monotonic() - callback_started) * 1000.0))
            self._record_error(str(exc))
            self._emit(
                "voice.callback_failed",
                {
                    "trigger_type": trigger_type,
                    "confidence": round(float(confidence), 6),
                    "message": str(exc),
                },
            )
            return
        callback_latency_ms = max(0.0, (time.monotonic() - callback_started) * 1000.0)
        self._record_callback_latency(callback_latency_ms)
        if isinstance(response, dict):
            reply = str(response.get("reply", "")).strip()
            if reply:
                with self._lock:
                    self._state["last_reply"] = reply
            self._ingest_callback_feedback(response=response, trigger_type=trigger_type)
        with self._lock:
            self._state["callback_count"] = int(self._state.get("callback_count", 0)) + 1
        self._emit(
            "voice.callback_completed",
            {
                "trigger_type": trigger_type,
                "has_reply": bool(str(response.get("reply", "")).strip()) if isinstance(response, dict) else False,
                "latency_ms": round(callback_latency_ms, 6),
            },
        )

    def _consume_rate_budget(self, *, queue_name: str, max_per_minute: int) -> bool:
        budget = max(1, int(max_per_minute))
        now = time.time()
        with self._lock:
            queue = self._transcription_timestamps if queue_name == "transcribe" else self._callback_timestamps
            while queue and (now - queue[0]) > 60.0:
                queue.popleft()
            if len(queue) >= budget:
                self._state["rate_limited_count"] = int(self._state.get("rate_limited_count", 0)) + 1
                return False
            queue.append(now)
        return True

    def _derive_confidence(self, result: Dict[str, Any], *, text: str) -> float:
        raw_confidence = result.get("confidence")
        if raw_confidence is not None:
            try:
                parsed = float(raw_confidence)
                return max(0.0, min(1.0, parsed))
            except Exception:
                pass
        clean = str(text or "").strip()
        if not clean:
            return 0.0
        token_count = len([token for token in clean.split() if token.strip()])
        length_score = min(1.0, float(len(clean)) / 48.0)
        token_score = min(1.0, float(token_count) / 8.0)
        return max(0.0, min(1.0, (0.62 * length_score) + (0.38 * token_score)))

    def _validate_transcript(self, *, text: str, confidence: float, trigger_type: str) -> tuple[bool, str]:
        cleaned = str(text or "").strip()
        if not cleaned:
            return False, "empty_text"
        min_chars = int(self._normalize_float(self._config.get("min_transcript_chars", 3), minimum=1.0, maximum=80.0))
        if len(cleaned) < min_chars:
            return False, "too_short"
        min_tokens = int(self._normalize_float(self._config.get("min_transcript_tokens", 1), minimum=1.0, maximum=20.0))
        token_count = len([token for token in cleaned.split() if token.strip()])
        if token_count < min_tokens:
            return False, "too_few_tokens"
        if trigger_type != "manual":
            threshold = float(self._normalize_float(self._config.get("min_confidence", 0.28), minimum=0.0, maximum=0.99))
        else:
            threshold = float(self._normalize_float(self._config.get("min_confidence_manual", 0.2), minimum=0.0, maximum=0.99))
        if float(confidence) < threshold:
            return False, "low_confidence"
        return True, ""

    def _record_rejected_transcription(self, *, reason: str, confidence: float) -> None:
        circuit_payload: Dict[str, Any] | None = None
        with self._lock:
            self._state["rejected_transcription_count"] = int(self._state.get("rejected_transcription_count", 0)) + 1
            self._state["last_rejection_reason"] = str(reason or "unknown_rejection")
            if str(reason) == "low_confidence":
                streak = int(self._state.get("low_confidence_streak", 0)) + 1
                self._state["low_confidence_streak"] = streak
                threshold = int(
                    self._normalize_float(
                        self._config.get("max_low_confidence_streak", 3),
                        minimum=1.0,
                        maximum=40.0,
                    )
                )
                if streak >= threshold:
                    backoff_s = float(
                        self._normalize_float(
                            self._config.get("low_confidence_backoff_s", 1.5),
                            minimum=0.1,
                            maximum=30.0,
                        )
                    )
                    open_until = time.time() + backoff_s
                    if open_until > self._circuit_open_until_epoch:
                        self._circuit_open_until_epoch = open_until
                        until_iso = datetime.fromtimestamp(open_until, tz=timezone.utc).isoformat()
                        self._state["circuit_open_until"] = until_iso
                        circuit_payload = {
                            "reason": "low_confidence",
                            "confidence": round(float(confidence), 6),
                            "streak": streak,
                            "backoff_s": round(backoff_s, 3),
                            "until": until_iso,
                        }
            else:
                self._state["low_confidence_streak"] = 0
        if circuit_payload:
            self._emit("voice.circuit_open", circuit_payload)

    def _record_callback_latency(self, latency_ms: float) -> None:
        latency = max(0.0, float(latency_ms))
        with self._lock:
            current = float(self._state.get("callback_latency_ema_ms", 0.0) or 0.0)
            next_value = latency if current <= 0.0 else ((0.82 * current) + (0.18 * latency))
            self._state["callback_latency_ema_ms"] = round(max(0.0, next_value), 6)

    def _ingest_callback_feedback(self, *, response: Dict[str, Any], trigger_type: str) -> None:
        status = str(response.get("status", "")).strip().lower()
        outcome = response.get("outcome") if isinstance(response.get("outcome"), dict) else {}
        if isinstance(outcome, dict):
            outcome_status = str(outcome.get("status", "")).strip().lower()
            if outcome_status:
                status = outcome_status
        goal_status = ""
        goal = response.get("goal")
        if isinstance(goal, dict):
            goal_status = str(goal.get("status", "")).strip().lower()
        if not status and goal_status:
            status = goal_status

        strictness_delta = 0.0
        rate_delta = 0
        reason = ""
        if status in {"blocked", "error", "failed", "failure"} or goal_status in {"blocked", "error", "failed", "failure"}:
            strictness_delta = 0.03
            rate_delta = -4
            reason = "callback_failure_signal"
        elif status in {"success", "completed", "done"} or goal_status in {"success", "completed", "done"}:
            strictness_delta = -0.01
            rate_delta = 2
            reason = "callback_success_signal"

        if strictness_delta == 0.0 and rate_delta == 0:
            return
        with self._lock:
            adaptive_profile = self._config.get("adaptive_profile")
            if not isinstance(adaptive_profile, dict):
                adaptive_profile = {}
            feedback = adaptive_profile.get("live_feedback")
            if not isinstance(feedback, dict):
                feedback = {
                    "events": 0,
                    "strictness_delta": 0.0,
                    "rate_delta": 0,
                    "last_reason": "",
                    "updated_at": "",
                }
            feedback["events"] = int(feedback.get("events", 0) or 0) + 1
            feedback["strictness_delta"] = round(
                max(-0.2, min(0.25, float(feedback.get("strictness_delta", 0.0) or 0.0) + strictness_delta)),
                6,
            )
            feedback["rate_delta"] = int(max(-40, min(40, int(feedback.get("rate_delta", 0) or 0) + rate_delta)))
            feedback["last_reason"] = reason
            feedback["updated_at"] = datetime.now(timezone.utc).isoformat()
            adaptive_profile["live_feedback"] = feedback
            self._config["adaptive_profile"] = adaptive_profile
            self._state["adaptive_profile"] = adaptive_profile
            self._config["min_confidence"] = self._normalize_float(
                float(self._config.get("min_confidence", 0.28) or 0.28) + float(strictness_delta),
                minimum=0.0,
                maximum=0.99,
            )
            self._config["max_callbacks_per_minute"] = int(
                self._normalize_float(
                    int(self._config.get("max_callbacks_per_minute", 90) or 90) + int(rate_delta),
                    minimum=1.0,
                    maximum=600.0,
                )
            )
        self._emit(
            "voice.adaptive_feedback_applied",
            {
                "trigger_type": trigger_type,
                "reason": reason,
                "strictness_delta": strictness_delta,
                "rate_delta": rate_delta,
                "status": status,
                "goal_status": goal_status,
            },
        )

    def _initialize_wakeword_if_enabled(self, supervision_snapshot: Optional[Dict[str, Any]] = None) -> None:
        if not bool(self._config.get("wakeword_enabled", False)):
            self._state["wakeword_status"] = "disabled"
            return
        previous_status = str(self._state.get("wakeword_status", "")).strip().lower()
        route_snapshot = self._refresh_route_policy_snapshot(force=False)
        wake_policy = route_snapshot.get("wakeword", {}) if isinstance(route_snapshot.get("wakeword", {}), dict) else {}
        supervision = (
            dict(supervision_snapshot)
            if isinstance(supervision_snapshot, dict)
            else self._refresh_supervision_snapshot(force=False, route_snapshot=route_snapshot)
        )
        supervision_row = (
            supervision.get("wakeword_supervision")
            if isinstance(supervision.get("wakeword_supervision"), dict)
            else {}
        )
        if not self._task_route_allowed(wake_policy, allow_reroute=False):
            reason_code = str(wake_policy.get("reason_code", "")).strip().lower() or "route_policy_gated"
            new_status = f"gated:{reason_code}"
            self._state["wakeword_status"] = new_status
            if previous_status != new_status:
                self._state["wakeword_gate_count"] = int(self._state.get("wakeword_gate_count", 0) or 0) + 1
                self._state["wakeword_last_gated_at"] = datetime.now(timezone.utc).isoformat()
                self._emit(
                    "voice.wakeword_gated",
                    {
                        "reason_code": reason_code,
                        "reason": str(wake_policy.get("reason", "")).strip(),
                        "cooldown_hint_s": float(wake_policy.get("cooldown_hint_s", 0.0) or 0.0),
                        "next_retry_at": str(wake_policy.get("next_retry_at", "")).strip(),
                    },
                )
            return
        if not bool(supervision_row.get("allow_wakeword", True)):
            reason_code = str(supervision_row.get("reason_code", "")).strip().lower() or "mission_recovery_policy"
            self._state["wakeword_status"] = f"gated:{reason_code}"
            restart_delay_s = max(0.0, float(supervision_row.get("restart_delay_s", 0.0) or 0.0))
            if restart_delay_s > 0.0:
                restart_epoch = time.time() + restart_delay_s
                self._state["wakeword_supervision_restart_not_before"] = datetime.fromtimestamp(
                    restart_epoch,
                    tz=timezone.utc,
                ).isoformat()
            if previous_status != self._state["wakeword_status"]:
                self._state["wakeword_gate_count"] = int(self._state.get("wakeword_gate_count", 0) or 0) + 1
                self._state["wakeword_last_gated_at"] = datetime.now(timezone.utc).isoformat()
                self._emit(
                    "voice.wakeword_gated",
                    {
                        "reason_code": reason_code,
                        "reason": str(supervision_row.get("reason", "")).strip(),
                        "cooldown_hint_s": round(restart_delay_s, 4),
                        "next_retry_at": str(self._state.get("wakeword_supervision_restart_not_before", "")).strip(),
                        "source": "mission_supervision",
                    },
                )
            return
        restart_delay_s = max(0.0, float(supervision_row.get("restart_delay_s", 0.0) or 0.0))
        restart_not_before_epoch = self._parse_epoch_seconds(
            str(self._state.get("wakeword_supervision_restart_not_before", "")).strip()
        )
        if restart_delay_s > 0.0 and previous_status.startswith(("gated:", "degraded:", "recovery:")) and restart_not_before_epoch <= 0.0:
            restart_epoch = time.time() + restart_delay_s
            self._state["wakeword_supervision_restart_not_before"] = datetime.fromtimestamp(
                restart_epoch,
                tz=timezone.utc,
            ).isoformat()
            self._state["wakeword_status"] = "recovery:mission_recovery_policy"
            self._record_wakeword_restart_event(
                event_type="recovery_deferred",
                status=self._state.get("wakeword_status", ""),
                reason_code=str(supervision_row.get("reason_code", "")).strip().lower(),
                reason=str(supervision_row.get("reason", "")).strip(),
                restart_delay_s=restart_delay_s,
                next_retry_at=str(self._state.get("wakeword_supervision_restart_not_before", "")).strip(),
            )
            self._emit(
                "voice.wakeword_recovery_deferred",
                {
                    "restart_delay_s": round(restart_delay_s, 4),
                    "resume_at": self._state.get("wakeword_supervision_restart_not_before", ""),
                    "reason_code": str(supervision_row.get("reason_code", "")).strip().lower(),
                },
            )
            return
        if restart_not_before_epoch > time.time():
            self._state["wakeword_status"] = "recovery:mission_recovery_policy"
            return

        keyword_path = str(self._config.get("wakeword_keyword_path", "")).strip()
        if not keyword_path:
            self._state["wakeword_status"] = "disabled_missing_keyword"
            return
        tuning_result = self._apply_wakeword_runtime_tuning(supervision_row)
        sensitivity = float(self._normalize_float(self._config.get("wakeword_sensitivity", 0.6), minimum=0.1, maximum=1.0))

        try:
            self._wakeword_engine = WakewordEngine(keyword_path=keyword_path, sensitivity=sensitivity)
            self._wakeword_engine.start(self._on_wakeword_detected)
            self._state["wakeword_status"] = "active"
            self._state["wakeword_supervision_restart_not_before"] = ""
            self._state["wakeword_start_failure_count"] = 0
            self._state["wakeword_restart_last_expired_retry_at"] = ""
            self._state["wakeword_restart_exhausted_until"] = ""
            self._record_wakeword_restart_event(
                event_type="started",
                status="active",
                reason_code=str(supervision_row.get("reason_code", "")).strip().lower(),
                reason=str(supervision_row.get("reason", "")).strip(),
                recovered=previous_status.startswith(("gated:", "degraded:", "recovery:")),
            )
            self._emit("voice.wakeword_started", {"keyword_path": keyword_path, "sensitivity": sensitivity})
            if bool(tuning_result.get("changed", False)):
                self._emit(
                    "voice.wakeword_runtime_tuning_applied",
                    {
                        "applied": dict(tuning_result.get("applied", {})),
                        "status": "active",
                    },
                )
            if previous_status.startswith(("gated:", "degraded:", "recovery:")):
                self._state["wakeword_recovery_count"] = int(self._state.get("wakeword_recovery_count", 0) or 0) + 1
                self._state["wakeword_last_recovered_at"] = datetime.now(timezone.utc).isoformat()
                self._emit(
                    "voice.wakeword_recovered",
                    {
                        "previous_status": previous_status,
                        "recovery_count": int(self._state.get("wakeword_recovery_count", 0) or 0),
                    },
                )
            self._relax_wakeword_restart_state(
                supervision_row,
                trigger="wakeword_started_recovered"
                if previous_status.startswith(("gated:", "degraded:", "recovery:"))
                else "wakeword_started",
            )
        except Exception as exc:  # noqa: BLE001
            self._wakeword_engine = None
            self._state["wakeword_status"] = f"degraded:{exc}"
            self._record_wakeword_restart_event(
                event_type="start_failed",
                status=self._state.get("wakeword_status", ""),
                reason_code=str(supervision_row.get("reason_code", "") or "wakeword_start_failed").strip().lower(),
                reason=str(exc),
                failure_count=int(self._state.get("wakeword_start_failure_count", 0) or 0) + 1,
            )
            backoff = self._schedule_wakeword_restart_backoff(
                supervision_row=supervision_row,
                reason_code=str(supervision_row.get("reason_code", "") or "wakeword_start_failed").strip().lower(),
                message=str(exc),
            )
            self._emit(
                "voice.wakeword_failed",
                {
                    "message": str(exc),
                    "failure_count": int(backoff.get("failure_count", 0) or 0),
                    "next_retry_at": str(backoff.get("next_retry_at", "")).strip(),
                    "restart_delay_s": float(backoff.get("restart_delay_s", 0.0) or 0.0),
                },
            )

    def _shutdown_wakeword(self) -> None:
        engine = self._wakeword_engine
        self._wakeword_engine = None
        if engine is None:
            return
        try:
            engine.stop()
        except Exception as exc:  # noqa: BLE001
            self._emit("voice.wakeword_stop_failed", {"message": str(exc)})
            return

    def _on_wakeword_detected(self) -> None:
        with self._lock:
            if not self._running:
                return
        self._wake_trigger.set()
        self._emit("voice.wakeword_detected", {"status": "hit"})

    def _emit(self, event: str, payload: Dict[str, Any]) -> None:
        if not self.emit_telemetry:
            return
        try:
            self.emit_telemetry(event, payload)
        except Exception:
            return

    def _circuit_remaining_seconds(self) -> float:
        with self._lock:
            until_epoch = float(self._circuit_open_until_epoch)
        if until_epoch <= 0:
            return 0.0
        now_epoch = time.time()
        remaining = until_epoch - now_epoch
        if remaining <= 0:
            with self._lock:
                if self._circuit_open_until_epoch > 0:
                    self._circuit_open_until_epoch = 0.0
                    self._state["circuit_open_until"] = ""
            self._emit("voice.circuit_closed", {"reason": "backoff_elapsed"})
            return 0.0
        return remaining

    def _record_success(self, *, emit_event: bool = True) -> None:
        now_iso = datetime.now(timezone.utc).isoformat()
        closed = False
        with self._lock:
            self._state["consecutive_errors"] = 0
            self._state["last_error"] = ""
            self._state["last_success_at"] = now_iso
            self._state["low_confidence_streak"] = 0
            self._state["last_rejection_reason"] = ""
            if self._circuit_open_until_epoch > 0:
                self._circuit_open_until_epoch = 0.0
                self._state["circuit_open_until"] = ""
                closed = True
        if closed and emit_event:
            self._emit("voice.circuit_closed", {"reason": "success"})

    def _record_error(self, message: str, *, emit_event: bool = True) -> None:
        detail = str(message or "unknown voice error").strip() or "unknown voice error"
        circuit_payload: Dict[str, Any] | None = None
        with self._lock:
            self._state["error_count"] = int(self._state.get("error_count", 0)) + 1
            consecutive = int(self._state.get("consecutive_errors", 0)) + 1
            self._state["consecutive_errors"] = consecutive
            self._state["last_error"] = detail

            threshold = int(self._normalize_float(self._config.get("max_consecutive_errors", 4), minimum=1.0, maximum=64.0))
            backoff_s = float(self._normalize_float(self._config.get("error_backoff_s", 2.5), minimum=0.1, maximum=60.0))
            if threshold > 0 and consecutive >= threshold and backoff_s > 0:
                open_until = time.time() + backoff_s
                if open_until > self._circuit_open_until_epoch:
                    self._circuit_open_until_epoch = open_until
                    until_iso = datetime.fromtimestamp(open_until, tz=timezone.utc).isoformat()
                    self._state["circuit_open_until"] = until_iso
                    circuit_payload = {
                        "error": detail,
                        "consecutive_errors": consecutive,
                        "max_consecutive_errors": threshold,
                        "backoff_s": round(backoff_s, 3),
                        "until": until_iso,
                    }
        if circuit_payload and emit_event:
            self._emit("voice.circuit_open", circuit_payload)

    @staticmethod
    def _normalize_float(value: Any, *, minimum: float, maximum: float) -> float:
        try:
            parsed = float(value)
        except Exception:
            parsed = minimum
        return max(minimum, min(maximum, parsed))

    def _normalize_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        keyword_path = str(payload.get("wakeword_keyword_path") or os.getenv("JARVIS_WAKEWORD_KEYWORD_PATH", "")).strip()
        wakeword_enabled_default = bool(keyword_path) and os.getenv("JARVIS_WAKEWORD_ENABLED", "1") == "1"
        wakeword_enabled = bool(payload.get("wakeword_enabled", wakeword_enabled_default))
        stt_mode = str(payload.get("stt_mode", os.getenv("JARVIS_VOICE_STT_MODE", "auto"))).strip().lower() or "auto"
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        normalized = {
            "stt_mode": stt_mode if stt_mode in {"stream", "vad", "chunk", "auto"} else "auto",
            "wakeword_enabled": wakeword_enabled,
            "wakeword_keyword_path": keyword_path,
            "wakeword_sensitivity": self._normalize_float(
                payload.get("wakeword_sensitivity", os.getenv("JARVIS_WAKEWORD_SENSITIVITY", "0.6")),
                minimum=0.1,
                maximum=1.0,
            ),
            "stt_duration_s": self._normalize_float(
                payload.get("stt_duration_s", os.getenv("JARVIS_VOICE_STT_DURATION_S", "4.0")),
                minimum=1.0,
                maximum=20.0,
            ),
            "fallback_interval_s": self._normalize_float(
                payload.get("fallback_interval_s", os.getenv("JARVIS_VOICE_FALLBACK_INTERVAL_S", "10.0")),
                minimum=0.5,
                maximum=60.0,
            ),
            "cooldown_s": self._normalize_float(
                payload.get("cooldown_s", os.getenv("JARVIS_VOICE_COOLDOWN_S", "0.8")),
                minimum=0.05,
                maximum=8.0,
            ),
            "vad_frame_s": self._normalize_float(
                payload.get("vad_frame_s", os.getenv("JARVIS_VOICE_VAD_FRAME_S", "0.2")),
                minimum=0.05,
                maximum=1.0,
            ),
            "vad_energy_threshold": self._normalize_float(
                payload.get("vad_energy_threshold", os.getenv("JARVIS_VOICE_VAD_ENERGY_THRESHOLD", "0.015")),
                minimum=0.001,
                maximum=0.3,
            ),
            "vad_silence_s": self._normalize_float(
                payload.get("vad_silence_s", os.getenv("JARVIS_VOICE_VAD_SILENCE_S", "0.9")),
                minimum=0.2,
                maximum=4.0,
            ),
            "vad_min_speech_s": self._normalize_float(
                payload.get("vad_min_speech_s", os.getenv("JARVIS_VOICE_VAD_MIN_SPEECH_S", "0.35")),
                minimum=0.1,
                maximum=8.0,
            ),
            "max_consecutive_errors": int(
                self._normalize_float(
                    payload.get("max_consecutive_errors", os.getenv("JARVIS_VOICE_MAX_CONSECUTIVE_ERRORS", "4")),
                    minimum=1.0,
                    maximum=64.0,
                )
            ),
            "error_backoff_s": self._normalize_float(
                payload.get("error_backoff_s", os.getenv("JARVIS_VOICE_ERROR_BACKOFF_S", "2.5")),
                minimum=0.1,
                maximum=60.0,
            ),
            "max_transcriptions_per_minute": int(
                self._normalize_float(
                    payload.get("max_transcriptions_per_minute", os.getenv("JARVIS_VOICE_MAX_TRANSCRIPTIONS_PER_MINUTE", "90")),
                    minimum=1.0,
                    maximum=600.0,
                )
            ),
            "max_callbacks_per_minute": int(
                self._normalize_float(
                    payload.get("max_callbacks_per_minute", os.getenv("JARVIS_VOICE_MAX_CALLBACKS_PER_MINUTE", "90")),
                    minimum=1.0,
                    maximum=600.0,
                )
            ),
            "min_confidence": self._normalize_float(
                payload.get("min_confidence", os.getenv("JARVIS_VOICE_MIN_CONFIDENCE", "0.28")),
                minimum=0.0,
                maximum=0.99,
            ),
            "min_confidence_manual": self._normalize_float(
                payload.get("min_confidence_manual", os.getenv("JARVIS_VOICE_MIN_CONFIDENCE_MANUAL", "0.2")),
                minimum=0.0,
                maximum=0.99,
            ),
            "min_transcript_chars": int(
                self._normalize_float(
                    payload.get("min_transcript_chars", os.getenv("JARVIS_VOICE_MIN_TRANSCRIPT_CHARS", "3")),
                    minimum=1.0,
                    maximum=80.0,
                )
            ),
            "min_transcript_tokens": int(
                self._normalize_float(
                    payload.get("min_transcript_tokens", os.getenv("JARVIS_VOICE_MIN_TRANSCRIPT_TOKENS", "1")),
                    minimum=1.0,
                    maximum=20.0,
                )
            ),
            "max_low_confidence_streak": int(
                self._normalize_float(
                    payload.get("max_low_confidence_streak", os.getenv("JARVIS_VOICE_MAX_LOW_CONFIDENCE_STREAK", "3")),
                    minimum=1.0,
                    maximum=40.0,
                )
            ),
            "low_confidence_backoff_s": self._normalize_float(
                payload.get("low_confidence_backoff_s", os.getenv("JARVIS_VOICE_LOW_CONFIDENCE_BACKOFF_S", "1.5")),
                minimum=0.1,
                maximum=30.0,
            ),
            "auto_submit": bool(payload.get("auto_submit", True)),
            "auto_tts": bool(payload.get("auto_tts", True)),
            "metadata": metadata,
            "adaptive_profile": {},
        }
        self._apply_adaptive_profile(payload=payload, normalized=normalized, metadata=metadata)
        return normalized

    def _apply_adaptive_profile(self, *, payload: Dict[str, Any], normalized: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        explicit = {str(key).strip() for key in payload.keys()}
        policy_profile = str(metadata.get("policy_profile", "") or metadata.get("target_policy_profile", "")).strip().lower()
        risk_level = str(
            metadata.get("risk_level", "")
            or metadata.get("mission_risk_level", "")
            or metadata.get("action_risk_level", "")
        ).strip().lower()
        profile_hint = str(metadata.get("voice_profile_hint", "")).strip().lower()
        guardrail_overrides = (
            metadata.get("voice_guardrail_overrides")
            if isinstance(metadata.get("voice_guardrail_overrides"), dict)
            else {}
        )
        if not isinstance(guardrail_overrides, dict):
            guardrail_overrides = {}
        if not risk_level and policy_profile in {"automation_safe"}:
            risk_level = "high"
        elif not risk_level and policy_profile in {"interactive"}:
            risk_level = "medium"
        elif not risk_level and policy_profile in {"automation_power"}:
            risk_level = "low"

        profile_name = "balanced"
        if profile_hint in {"strict", "balanced", "power"}:
            profile_name = profile_hint
        elif risk_level == "high" or policy_profile in {"automation_safe"}:
            profile_name = "strict"
        elif risk_level == "low" or policy_profile in {"automation_power"}:
            profile_name = "power"

        profile_overrides: Dict[str, Any]
        if profile_name == "strict":
            profile_overrides = {
                "min_confidence": 0.42,
                "min_confidence_manual": 0.3,
                "max_callbacks_per_minute": 30,
                "max_transcriptions_per_minute": 40,
                "max_low_confidence_streak": 2,
                "low_confidence_backoff_s": 2.5,
            }
        elif profile_name == "power":
            profile_overrides = {
                "min_confidence": 0.22,
                "min_confidence_manual": 0.14,
                "max_callbacks_per_minute": 120,
                "max_transcriptions_per_minute": 120,
                "max_low_confidence_streak": 4,
                "low_confidence_backoff_s": 1.0,
            }
        else:
            profile_overrides = {
                "min_confidence": 0.3,
                "min_confidence_manual": 0.2,
                "max_callbacks_per_minute": 80,
                "max_transcriptions_per_minute": 80,
                "max_low_confidence_streak": 3,
                "low_confidence_backoff_s": 1.6,
            }

        applied: Dict[str, Any] = {}
        for key, value in profile_overrides.items():
            if key in explicit:
                continue
            normalized[key] = value
            applied[key] = value

        stt_health = "unknown"
        stt_fallback_rate = 0.0
        diagnostics_fn = getattr(self.stt_engine, "diagnostics", None)
        if callable(diagnostics_fn):
            try:
                diagnostics = diagnostics_fn(history_limit=8)
            except Exception:
                diagnostics = {}
            if isinstance(diagnostics, dict):
                stt_health = str(
                    diagnostics.get("provider_health", "") or diagnostics.get("health", "")
                ).strip().lower() or "unknown"
                try:
                    stt_fallback_rate = float(diagnostics.get("fallback_rate_ema", 0.0) or 0.0)
                except Exception:
                    stt_fallback_rate = 0.0

        health_adjustments: Dict[str, Any] = {}
        if stt_health in {"degraded", "critical"}:
            if "min_confidence" not in explicit:
                normalized["min_confidence"] = round(
                    min(
                        0.95,
                        float(normalized.get("min_confidence", 0.28))
                        + (0.12 if stt_health == "critical" else 0.06),
                    ),
                    6,
                )
                health_adjustments["min_confidence"] = normalized["min_confidence"]
            if "max_callbacks_per_minute" not in explicit:
                reduction = 0.55 if stt_health == "critical" else 0.75
                normalized["max_callbacks_per_minute"] = max(
                    10,
                    int(int(normalized.get("max_callbacks_per_minute", 80)) * reduction),
                )
                health_adjustments["max_callbacks_per_minute"] = normalized["max_callbacks_per_minute"]
            if "max_transcriptions_per_minute" not in explicit:
                reduction = 0.55 if stt_health == "critical" else 0.75
                normalized["max_transcriptions_per_minute"] = max(
                    12,
                    int(int(normalized.get("max_transcriptions_per_minute", 80)) * reduction),
                )
                health_adjustments["max_transcriptions_per_minute"] = normalized["max_transcriptions_per_minute"]
            if "max_low_confidence_streak" not in explicit and stt_health == "critical":
                normalized["max_low_confidence_streak"] = 1
                health_adjustments["max_low_confidence_streak"] = 1

        if stt_fallback_rate >= 0.55 and "min_confidence" not in explicit:
            normalized["min_confidence"] = round(
                min(0.95, float(normalized.get("min_confidence", 0.28)) + 0.05),
                6,
            )
            health_adjustments["min_confidence"] = normalized["min_confidence"]

        override_effects: Dict[str, Any] = {}
        try:
            min_confidence_offset = float(guardrail_overrides.get("min_confidence_offset", 0.0) or 0.0)
        except Exception:
            min_confidence_offset = 0.0
        try:
            min_confidence_manual_offset = float(guardrail_overrides.get("min_confidence_manual_offset", 0.0) or 0.0)
        except Exception:
            min_confidence_manual_offset = 0.0
        try:
            callback_scale = float(guardrail_overrides.get("max_callbacks_scale", 1.0) or 1.0)
        except Exception:
            callback_scale = 1.0
        try:
            transcription_scale = float(guardrail_overrides.get("max_transcriptions_scale", 1.0) or 1.0)
        except Exception:
            transcription_scale = 1.0
        try:
            low_confidence_backoff_scale = float(guardrail_overrides.get("low_confidence_backoff_scale", 1.0) or 1.0)
        except Exception:
            low_confidence_backoff_scale = 1.0
        low_confidence_streak_override = guardrail_overrides.get("max_low_confidence_streak")

        if "min_confidence" not in explicit and abs(min_confidence_offset) > 1e-9:
            normalized["min_confidence"] = round(
                self._normalize_float(
                    float(normalized.get("min_confidence", 0.28) or 0.28) + min_confidence_offset,
                    minimum=0.0,
                    maximum=0.99,
                ),
                6,
            )
            override_effects["min_confidence"] = normalized["min_confidence"]
        if "min_confidence_manual" not in explicit and abs(min_confidence_manual_offset) > 1e-9:
            normalized["min_confidence_manual"] = round(
                self._normalize_float(
                    float(normalized.get("min_confidence_manual", 0.2) or 0.2) + min_confidence_manual_offset,
                    minimum=0.0,
                    maximum=0.99,
                ),
                6,
            )
            override_effects["min_confidence_manual"] = normalized["min_confidence_manual"]

        callback_scale = max(0.25, min(2.0, callback_scale))
        if "max_callbacks_per_minute" not in explicit and abs(callback_scale - 1.0) > 1e-9:
            normalized["max_callbacks_per_minute"] = int(
                self._normalize_float(
                    int(normalized.get("max_callbacks_per_minute", 80) or 80) * callback_scale,
                    minimum=1.0,
                    maximum=600.0,
                )
            )
            override_effects["max_callbacks_per_minute"] = normalized["max_callbacks_per_minute"]

        transcription_scale = max(0.25, min(2.0, transcription_scale))
        if "max_transcriptions_per_minute" not in explicit and abs(transcription_scale - 1.0) > 1e-9:
            normalized["max_transcriptions_per_minute"] = int(
                self._normalize_float(
                    int(normalized.get("max_transcriptions_per_minute", 80) or 80) * transcription_scale,
                    minimum=1.0,
                    maximum=600.0,
                )
            )
            override_effects["max_transcriptions_per_minute"] = normalized["max_transcriptions_per_minute"]

        low_confidence_backoff_scale = max(0.4, min(2.5, low_confidence_backoff_scale))
        if "low_confidence_backoff_s" not in explicit and abs(low_confidence_backoff_scale - 1.0) > 1e-9:
            normalized["low_confidence_backoff_s"] = round(
                self._normalize_float(
                    float(normalized.get("low_confidence_backoff_s", 1.5) or 1.5) * low_confidence_backoff_scale,
                    minimum=0.1,
                    maximum=30.0,
                ),
                6,
            )
            override_effects["low_confidence_backoff_s"] = normalized["low_confidence_backoff_s"]

        if "max_low_confidence_streak" not in explicit and low_confidence_streak_override is not None:
            normalized["max_low_confidence_streak"] = int(
                self._normalize_float(low_confidence_streak_override, minimum=1.0, maximum=40.0)
            )
            override_effects["max_low_confidence_streak"] = normalized["max_low_confidence_streak"]

        normalized["adaptive_profile"] = {
            "profile": profile_name,
            "risk_level": risk_level or "unknown",
            "policy_profile": policy_profile or "unknown",
            "profile_hint": profile_hint or "",
            "explicit_keys": sorted(explicit),
            "applied_overrides": applied,
            "health_adjustments": health_adjustments,
            "guardrail_overrides": guardrail_overrides,
            "override_effects": override_effects,
            "stt_health": stt_health,
            "stt_fallback_rate_ema": round(max(0.0, min(stt_fallback_rate, 1.0)), 6),
            "applied_at": datetime.now(timezone.utc).isoformat(),
        }
