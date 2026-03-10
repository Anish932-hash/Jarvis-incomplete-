from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp_float(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = default
    return max(minimum, min(maximum, parsed))


def _clamp_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, min(maximum, parsed))


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def canonical_provider(raw: str) -> str:
    value = str(raw or "").strip().lower()
    if value in {"", "auto"}:
        return "auto"
    if value in {"local", "pyttsx3", "local-pyttsx3", "win32", "sapi", "win32_sapi", "local-win32-sapi"}:
        return "local"
    if value in {"elevenlabs", "remote"}:
        return "elevenlabs"
    return "unknown"


class TtsPolicyManager:
    _instance: "TtsPolicyManager | None" = None
    _instance_lock = threading.RLock()
    _providers = ("local", "elevenlabs")

    @classmethod
    def shared(cls) -> "TtsPolicyManager":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.enabled = _to_bool(os.getenv("JARVIS_TTS_POLICY_ENABLED", "1"), True)
        self.learning_enabled = _to_bool(os.getenv("JARVIS_TTS_POLICY_LEARNING_ENABLED", "1"), True)
        self.alpha = _clamp_float(os.getenv("JARVIS_TTS_POLICY_ALPHA", "0.24"), 0.24, 0.05, 0.95)
        self.failure_weight = _clamp_float(os.getenv("JARVIS_TTS_POLICY_FAILURE_WEIGHT", "2.4"), 2.4, 0.1, 8.0)
        self.latency_weight = _clamp_float(os.getenv("JARVIS_TTS_POLICY_LATENCY_WEIGHT", "0.6"), 0.6, 0.0, 3.0)
        self.cooldown_failure_threshold = _clamp_float(
            os.getenv("JARVIS_TTS_POLICY_COOLDOWN_FAILURE_THRESHOLD", "0.58"),
            0.58,
            0.1,
            0.98,
        )
        self.cooldown_min_s = _clamp_float(os.getenv("JARVIS_TTS_POLICY_COOLDOWN_MIN_S", "18"), 18.0, 2.0, 3600.0)
        self.cooldown_max_s = _clamp_float(os.getenv("JARVIS_TTS_POLICY_COOLDOWN_MAX_S", "240"), 240.0, 5.0, 86400.0)
        self.min_samples = _clamp_int(os.getenv("JARVIS_TTS_POLICY_MIN_SAMPLES", "6"), 6, 1, 5000)
        self.bad_threshold = _clamp_float(os.getenv("JARVIS_TTS_POLICY_BAD_THRESHOLD", "0.56"), 0.56, 0.1, 0.99)
        self.good_threshold = _clamp_float(os.getenv("JARVIS_TTS_POLICY_GOOD_THRESHOLD", "0.28"), 0.28, 0.01, 0.9)
        self.bias_adjust_step = _clamp_float(os.getenv("JARVIS_TTS_POLICY_BIAS_ADJUST_STEP", "0.08"), 0.08, 0.005, 1.0)
        self.persist_every = _clamp_int(os.getenv("JARVIS_TTS_POLICY_PERSIST_EVERY", "3"), 3, 1, 200)
        self.history_limit = _clamp_int(os.getenv("JARVIS_TTS_POLICY_HISTORY_LIMIT", "320"), 320, 40, 5000)
        self.state_path = str(os.getenv("JARVIS_TTS_POLICY_STATE_PATH", "data/tts_policy_state.json") or "data/tts_policy_state.json").strip() or "data/tts_policy_state.json"

        self.route_bias: Dict[str, float] = {"local": 0.0, "elevenlabs": 0.18}
        self.risk_bias: Dict[str, Dict[str, float]] = {
            "low": {"local": 0.0, "elevenlabs": 0.1},
            "medium": {"local": 0.08, "elevenlabs": 0.04},
            "high": {"local": 0.22, "elevenlabs": -0.08},
        }
        self.profile_bias: Dict[str, Dict[str, float]] = {
            "interactive": {"local": 0.06, "elevenlabs": 0.08},
            "automation_safe": {"local": 0.14, "elevenlabs": -0.02},
            "automation_power": {"local": 0.02, "elevenlabs": 0.14},
            "privacy": {"local": 0.4, "elevenlabs": -0.4},
        }
        self.provider_state: Dict[str, Dict[str, Any]] = {}
        self.decision_history: List[Dict[str, Any]] = []
        self.attempt_history: List[Dict[str, Any]] = []

        self.last_loaded_at = ""
        self.last_saved_at = ""
        self.last_save_error = ""
        self.dirty_updates = 0
        self.mutation_count = 0

        for provider in self._providers:
            self._state_for(provider)
        self._load_state()

    def _state_for(self, provider: str) -> Dict[str, Any]:
        key = str(provider or "").strip().lower()
        row = self.provider_state.get(key)
        if isinstance(row, dict):
            return row
        created = {
            "attempts": 0,
            "successes": 0,
            "failures": 0,
            "failure_ema": 0.0,
            "latency_ema_s": 0.0,
            "cooldown_until": 0.0,
            "last_status": "",
            "last_error": "",
            "last_attempt_at": 0.0,
            "last_success_at": 0.0,
            "last_failure_at": 0.0,
        }
        self.provider_state[key] = created
        return created

    def _normalized_availability(self, availability: Dict[str, bool] | None = None) -> Dict[str, bool]:
        data = availability if isinstance(availability, dict) else {}
        return {provider: bool(data.get(provider, True)) for provider in self._providers}

    def _risk_bias_for(self, provider: str, risk_level: str) -> float:
        row = self.risk_bias.get(str(risk_level or "").strip().lower())
        return float(row.get(provider, 0.0) if isinstance(row, dict) else 0.0)

    def _profile_bias_for(self, provider: str, profile: str) -> float:
        row = self.profile_bias.get(str(profile or "").strip().lower())
        return float(row.get(provider, 0.0) if isinstance(row, dict) else 0.0)

    def _adaptive_auto_chain(
        self,
        *,
        availability: Dict[str, bool],
        context: Dict[str, Any],
    ) -> tuple[List[str], Dict[str, float]]:
        candidates = [provider for provider in self._providers if availability.get(provider, True)]
        if not candidates:
            candidates = list(self._providers)
        now = time.time()
        risk_level = str(context.get("risk_level", "") or context.get("mission_risk_level", "")).strip().lower()
        policy_profile = str(context.get("policy_profile", "")).strip().lower()
        requires_offline = _to_bool(context.get("requires_offline"), False)
        privacy_mode = _to_bool(context.get("privacy_mode"), False)

        scores: Dict[str, float] = {}
        for provider in candidates:
            state = self._state_for(provider)
            retry_after_s = max(0.0, float(state.get("cooldown_until", 0.0) or 0.0) - now)
            failure_ema = float(state.get("failure_ema", 0.0) or 0.0)
            latency_ema = float(state.get("latency_ema_s", 0.0) or 0.0)
            score = float(self.route_bias.get(provider, 0.0) or 0.0)
            score += 0.25 if retry_after_s <= 0.0 else -1.2
            score -= self.failure_weight * max(0.0, min(1.0, failure_ema))
            score -= self.latency_weight * max(0.0, min(6.0, latency_ema))
            score += self._risk_bias_for(provider, risk_level)
            score += self._profile_bias_for(provider, policy_profile)
            if requires_offline:
                score += 0.7 if provider == "local" else -0.7
            if privacy_mode:
                score += 0.9 if provider == "local" else -0.9
            scores[provider] = round(score, 6)

        chain = sorted(candidates, key=lambda item: scores.get(item, 0.0), reverse=True)
        if len(chain) < len(self._providers):
            for provider in self._providers:
                if provider not in chain:
                    chain.append(provider)
        return chain, scores

    def _trim_history(self) -> None:
        limit = max(40, int(self.history_limit))
        if len(self.decision_history) > limit:
            self.decision_history = self.decision_history[-limit:]
        if len(self.attempt_history) > limit:
            self.attempt_history = self.attempt_history[-limit:]

    def choose_provider(
        self,
        *,
        requested_provider: str,
        availability: Dict[str, bool] | None = None,
        context: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        requested = canonical_provider(requested_provider)
        available = self._normalized_availability(availability)
        context_row = dict(context or {})

        with self._lock:
            if requested == "unknown":
                return {
                    "status": "error",
                    "message": f"Unsupported provider '{requested_provider}'.",
                    "selected_provider": "",
                    "chain": [],
                }

            if requested in self._providers:
                chain = [requested]
                alt = "local" if requested == "elevenlabs" else "elevenlabs"
                if available.get(alt, False):
                    chain.append(alt)
                decision = {
                    "status": "success",
                    "enabled": bool(self.enabled),
                    "requested_provider": requested,
                    "selected_provider": chain[0] if chain else requested,
                    "chain": chain,
                    "reason": "explicit",
                    "scores": {requested: 1.0},
                    "context": context_row,
                }
                self._record_decision(decision)
                return decision

            if not self.enabled:
                chain = ["elevenlabs", "local"] if available.get("elevenlabs", False) else ["local", "elevenlabs"]
                decision = {
                    "status": "success",
                    "enabled": False,
                    "requested_provider": "auto",
                    "selected_provider": chain[0],
                    "chain": chain,
                    "reason": "disabled",
                    "scores": {},
                    "context": context_row,
                }
                self._record_decision(decision)
                return decision

            chain, scores = self._adaptive_auto_chain(availability=available, context=context_row)
            decision = {
                "status": "success",
                "enabled": True,
                "requested_provider": "auto",
                "selected_provider": chain[0],
                "chain": chain,
                "reason": "adaptive_score",
                "scores": scores,
                "context": context_row,
            }
            self._record_decision(decision)
            return decision

    def record_attempt(
        self,
        *,
        provider: str,
        status: str,
        latency_s: float = 0.0,
        message: str = "",
        context: Dict[str, Any] | None = None,
        decision: Dict[str, Any] | None = None,
        attempt_index: int = 1,
    ) -> Dict[str, Any]:
        provider_name = canonical_provider(provider)
        if provider_name not in self._providers:
            return {"status": "ignored", "reason": "unknown_provider"}

        clean_status = str(status or "").strip().lower() or "unknown"
        latency = max(0.0, float(latency_s or 0.0))
        error_text = str(message or "").strip()

        with self._lock:
            state = self._state_for(provider_name)
            now = time.time()
            state["attempts"] = int(state.get("attempts", 0) or 0) + 1
            state["last_attempt_at"] = now
            state["last_status"] = clean_status

            if clean_status == "success":
                state["successes"] = int(state.get("successes", 0) or 0) + 1
                current_latency = float(state.get("latency_ema_s", 0.0) or 0.0)
                state["latency_ema_s"] = latency if current_latency <= 0.0 else ((self.alpha * latency) + ((1.0 - self.alpha) * current_latency))
                state["failure_ema"] = max(0.0, min(1.0, (1.0 - self.alpha) * float(state.get("failure_ema", 0.0) or 0.0)))
                state["last_success_at"] = now
                if float(state.get("failure_ema", 0.0) or 0.0) < 0.08:
                    state["cooldown_until"] = 0.0
                    state["last_error"] = ""
            elif clean_status == "skipped":
                state["failure_ema"] = max(
                    0.0,
                    min(1.0, (1.0 - self.alpha) * float(state.get("failure_ema", 0.0) or 0.0) + (self.alpha * 0.36)),
                )
            else:
                state["failures"] = int(state.get("failures", 0) or 0) + 1
                state["failure_ema"] = max(
                    0.0,
                    min(1.0, (1.0 - self.alpha) * float(state.get("failure_ema", 0.0) or 0.0) + self.alpha),
                )
                state["last_error"] = error_text
                state["last_failure_at"] = now
                if float(state.get("failure_ema", 0.0) or 0.0) >= self.cooldown_failure_threshold:
                    cooldown_s = self.cooldown_min_s + (self.cooldown_max_s - self.cooldown_min_s) * float(state["failure_ema"])
                    state["cooldown_until"] = max(float(state.get("cooldown_until", 0.0) or 0.0), now + cooldown_s)

            self._self_tune(provider_name)
            history_row = {
                "at": _utc_now_iso(),
                "provider": provider_name,
                "status": clean_status,
                "latency_s": round(latency, 6),
                "message": error_text,
                "attempt_index": max(1, int(attempt_index)),
                "context": dict(context or {}),
                "decision": dict(decision or {}),
            }
            self.attempt_history.append(history_row)
            self._trim_history()
            self.mutation_count += 1
            self._mark_dirty()
            return {
                "status": "success",
                "provider": provider_name,
                "failure_ema": round(float(state.get("failure_ema", 0.0) or 0.0), 6),
                "latency_ema_s": round(float(state.get("latency_ema_s", 0.0) or 0.0), 6),
                "retry_after_s": round(max(0.0, float(state.get("cooldown_until", 0.0) or 0.0) - now), 3),
            }

    def status(self, limit: int = 80, context: Dict[str, Any] | None = None, availability: Dict[str, bool] | None = None) -> Dict[str, Any]:
        bounded = max(1, min(int(limit), 2000))
        with self._lock:
            now = time.time()
            context_row = dict(context or {})
            available = self._normalized_availability(availability)
            providers: Dict[str, Dict[str, Any]] = {}
            for provider in self._providers:
                row = self._state_for(provider)
                attempts = int(row.get("attempts", 0) or 0)
                successes = int(row.get("successes", 0) or 0)
                failures = int(row.get("failures", 0) or 0)
                retry_after_s = max(0.0, float(row.get("cooldown_until", 0.0) or 0.0) - now)
                providers[provider] = {
                    "provider": provider,
                    "ready": retry_after_s <= 0.0,
                    "retry_after_s": round(retry_after_s, 3),
                    "attempts": attempts,
                    "successes": successes,
                    "failures": failures,
                    "success_rate": round((float(successes) / float(attempts)) if attempts > 0 else 0.0, 6),
                    "failure_ema": round(float(row.get("failure_ema", 0.0) or 0.0), 6),
                    "latency_ema_s": round(float(row.get("latency_ema_s", 0.0) or 0.0), 6),
                    "last_status": str(row.get("last_status", "")).strip(),
                    "last_error": str(row.get("last_error", "")).strip(),
                    "last_attempt_at": float(row.get("last_attempt_at", 0.0) or 0.0),
                    "last_success_at": float(row.get("last_success_at", 0.0) or 0.0),
                    "last_failure_at": float(row.get("last_failure_at", 0.0) or 0.0),
                }

            recommended_chain, _ = self._adaptive_auto_chain(availability=available, context=context_row)
            recommended = recommended_chain[0] if recommended_chain else "local"
            return {
                "status": "success",
                "enabled": bool(self.enabled),
                "learning_enabled": bool(self.learning_enabled),
                "alpha": round(float(self.alpha), 6),
                "failure_weight": round(float(self.failure_weight), 6),
                "latency_weight": round(float(self.latency_weight), 6),
                "min_samples": int(self.min_samples),
                "bad_threshold": round(float(self.bad_threshold), 6),
                "good_threshold": round(float(self.good_threshold), 6),
                "cooldown_failure_threshold": round(float(self.cooldown_failure_threshold), 6),
                "cooldown_min_s": round(float(self.cooldown_min_s), 3),
                "cooldown_max_s": round(float(self.cooldown_max_s), 3),
                "persist_every": int(self.persist_every),
                "route_bias": dict(self.route_bias),
                "risk_bias": {key: dict(value) for key, value in self.risk_bias.items()},
                "profile_bias": {key: dict(value) for key, value in self.profile_bias.items()},
                "providers": providers,
                "recommended_provider": str(recommended or "local"),
                "recommended_chain": list(recommended_chain),
                "decision_history": list(self.decision_history[-bounded:]),
                "history_tail": list(self.attempt_history[-bounded:]),
                "decision_count": len(self.decision_history),
                "attempt_count": len(self.attempt_history),
                "mutation_count": int(self.mutation_count),
                "state_path": self.state_path,
                "last_loaded_at": self.last_loaded_at,
                "last_saved_at": self.last_saved_at,
                "last_save_error": self.last_save_error,
                "dirty_updates": int(self.dirty_updates),
            }

    def update(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = payload if isinstance(payload, dict) else {}
        changed: Dict[str, Any] = {}

        with self._lock:
            if "enabled" in data:
                self.enabled = _to_bool(data.get("enabled"), self.enabled)
                changed["enabled"] = self.enabled
            if "learning_enabled" in data:
                self.learning_enabled = _to_bool(data.get("learning_enabled"), self.learning_enabled)
                changed["learning_enabled"] = self.learning_enabled
            if "alpha" in data:
                self.alpha = _clamp_float(data.get("alpha"), self.alpha, 0.05, 0.95)
                changed["alpha"] = self.alpha
            if "failure_weight" in data:
                self.failure_weight = _clamp_float(data.get("failure_weight"), self.failure_weight, 0.1, 8.0)
                changed["failure_weight"] = self.failure_weight
            if "latency_weight" in data:
                self.latency_weight = _clamp_float(data.get("latency_weight"), self.latency_weight, 0.0, 3.0)
                changed["latency_weight"] = self.latency_weight
            if "min_samples" in data:
                self.min_samples = _clamp_int(data.get("min_samples"), self.min_samples, 1, 5000)
                changed["min_samples"] = self.min_samples
            if "bad_threshold" in data:
                self.bad_threshold = _clamp_float(data.get("bad_threshold"), self.bad_threshold, 0.1, 0.99)
                changed["bad_threshold"] = self.bad_threshold
            if "good_threshold" in data:
                self.good_threshold = _clamp_float(data.get("good_threshold"), self.good_threshold, 0.01, 0.9)
                changed["good_threshold"] = self.good_threshold
            if "cooldown_failure_threshold" in data:
                self.cooldown_failure_threshold = _clamp_float(data.get("cooldown_failure_threshold"), self.cooldown_failure_threshold, 0.1, 0.99)
                changed["cooldown_failure_threshold"] = self.cooldown_failure_threshold
            if "cooldown_min_s" in data:
                self.cooldown_min_s = _clamp_float(data.get("cooldown_min_s"), self.cooldown_min_s, 2.0, 3600.0)
                changed["cooldown_min_s"] = self.cooldown_min_s
            if "cooldown_max_s" in data:
                self.cooldown_max_s = _clamp_float(data.get("cooldown_max_s"), self.cooldown_max_s, 5.0, 86400.0)
                changed["cooldown_max_s"] = self.cooldown_max_s
            if "persist_every" in data:
                self.persist_every = _clamp_int(data.get("persist_every"), self.persist_every, 1, 200)
                changed["persist_every"] = self.persist_every
            if "state_path" in data:
                new_path = str(data.get("state_path", "")).strip()
                if new_path:
                    self.state_path = new_path
                    changed["state_path"] = self.state_path

            route_bias = data.get("route_bias")
            if isinstance(route_bias, dict):
                for provider in self._providers:
                    if provider in route_bias:
                        self.route_bias[provider] = _clamp_float(route_bias.get(provider), self.route_bias.get(provider, 0.0), -4.0, 4.0)
                changed["route_bias"] = dict(self.route_bias)

            if _to_bool(data.get("reset_learning_state"), False):
                for provider in self._providers:
                    self.provider_state[provider] = {
                        "attempts": 0,
                        "successes": 0,
                        "failures": 0,
                        "failure_ema": 0.0,
                        "latency_ema_s": 0.0,
                        "cooldown_until": 0.0,
                        "last_status": "",
                        "last_error": "",
                        "last_attempt_at": 0.0,
                        "last_success_at": 0.0,
                        "last_failure_at": 0.0,
                    }
                changed["reset_learning_state"] = True

            if _to_bool(data.get("reset_history"), False):
                self.decision_history = []
                self.attempt_history = []
                changed["reset_history"] = True

            if changed:
                self.mutation_count += 1
                self._mark_dirty()

            if _to_bool(data.get("persist_now"), False):
                self._save_state(force=True)

        return {
            "status": "success",
            "updated": bool(changed),
            "changed": changed,
            "policy": self.status(limit=_clamp_int(data.get("limit", 120), 120, 1, 2000)),
        }

    def _record_decision(self, payload: Dict[str, Any]) -> None:
        row = dict(payload)
        row["at"] = _utc_now_iso()
        self.decision_history.append(row)
        self._trim_history()
        self.mutation_count += 1
        self._mark_dirty()

    def _self_tune(self, provider: str) -> None:
        if not self.learning_enabled:
            return
        state = self._state_for(provider)
        attempts = int(state.get("attempts", 0) or 0)
        if attempts < self.min_samples:
            return
        failure_ema = float(state.get("failure_ema", 0.0) or 0.0)
        successes = int(state.get("successes", 0) or 0)
        success_rate = (float(successes) / float(attempts)) if attempts > 0 else 0.0
        current_bias = float(self.route_bias.get(provider, 0.0) or 0.0)
        if failure_ema >= self.bad_threshold:
            self.route_bias[provider] = max(-4.0, current_bias - self.bias_adjust_step)
            return
        if failure_ema <= self.good_threshold and success_rate >= 0.62:
            self.route_bias[provider] = min(4.0, current_bias + self.bias_adjust_step)
            return
        latency_ema = float(state.get("latency_ema_s", 0.0) or 0.0)
        if latency_ema > 2.0:
            self.route_bias[provider] = max(-4.0, current_bias - (self.bias_adjust_step * 0.35))

    def _mark_dirty(self) -> None:
        self.dirty_updates += 1
        if self.dirty_updates >= self.persist_every:
            self._save_state(force=True)

    def _load_state(self) -> None:
        with self._lock:
            target = Path(self.state_path)
            if not target.exists():
                self.last_loaded_at = _utc_now_iso()
                return
            try:
                payload = json.loads(target.read_text(encoding="utf-8"))
            except Exception as exc:
                self.last_loaded_at = _utc_now_iso()
                self.last_save_error = f"load_error:{exc}"
                return

            if isinstance(payload, dict):
                if isinstance(payload.get("route_bias"), dict):
                    for provider in self._providers:
                        if provider in payload["route_bias"]:
                            self.route_bias[provider] = _clamp_float(payload["route_bias"].get(provider), self.route_bias.get(provider, 0.0), -4.0, 4.0)
                if isinstance(payload.get("provider_state"), dict):
                    for provider in self._providers:
                        row = payload["provider_state"].get(provider)
                        if isinstance(row, dict):
                            state = self._state_for(provider)
                            state.update(
                                {
                                    "attempts": _clamp_int(row.get("attempts"), 0, 0, 10_000_000),
                                    "successes": _clamp_int(row.get("successes"), 0, 0, 10_000_000),
                                    "failures": _clamp_int(row.get("failures"), 0, 0, 10_000_000),
                                    "failure_ema": _clamp_float(row.get("failure_ema"), 0.0, 0.0, 1.0),
                                    "latency_ema_s": _clamp_float(row.get("latency_ema_s"), 0.0, 0.0, 20.0),
                                    "cooldown_until": max(0.0, float(row.get("cooldown_until", 0.0) or 0.0)),
                                    "last_status": str(row.get("last_status", "")).strip().lower(),
                                    "last_error": str(row.get("last_error", "")).strip(),
                                    "last_attempt_at": max(0.0, float(row.get("last_attempt_at", 0.0) or 0.0)),
                                    "last_success_at": max(0.0, float(row.get("last_success_at", 0.0) or 0.0)),
                                    "last_failure_at": max(0.0, float(row.get("last_failure_at", 0.0) or 0.0)),
                                }
                            )
                if isinstance(payload.get("decision_history"), list):
                    self.decision_history = [item for item in payload["decision_history"] if isinstance(item, dict)][-self.history_limit :]
                if isinstance(payload.get("attempt_history"), list):
                    self.attempt_history = [item for item in payload["attempt_history"] if isinstance(item, dict)][-self.history_limit :]

            self.last_loaded_at = _utc_now_iso()

    def _save_state(self, force: bool = False) -> None:
        if not force and self.dirty_updates <= 0:
            return

        target = Path(self.state_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "route_bias": dict(self.route_bias),
            "provider_state": {provider: dict(self._state_for(provider)) for provider in self._providers},
            "decision_history": list(self.decision_history[-self.history_limit :]),
            "attempt_history": list(self.attempt_history[-self.history_limit :]),
            "saved_at": _utc_now_iso(),
        }
        temp_path = target.with_suffix(f"{target.suffix}.tmp")
        try:
            temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            temp_path.replace(target)
            self.last_saved_at = _utc_now_iso()
            self.last_save_error = ""
            self.dirty_updates = 0
        except Exception as exc:
            self.last_save_error = str(exc)
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception:
                pass
