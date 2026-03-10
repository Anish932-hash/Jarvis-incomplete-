import io
import json
import os
import queue
import threading
import time
import wave
from collections import deque
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import sounddevice as sd


class STTEngine:
    """
    Hybrid STT engine with local-first strategy, adaptive provider routing, and cloud fallback.
    """

    _LOCAL_PROVIDER = "local"
    _CLOUD_PROVIDER = "groq"

    def __init__(
        self,
        groq_api_key: Optional[str] = None,
        model: str = "whisper-large-v3",
        local_model_path: str = "stt",
        *,
        provider_failure_streak_threshold: int = 3,
        provider_cooldown_s: float = 10.0,
        provider_max_cooldown_s: float = 180.0,
        provider_state_path: Optional[str] = None,
        provider_state_enabled: Optional[bool] = None,
        provider_state_persist_interval_s: float = 5.0,
        route_policy_provider: Optional[Callable[[], Dict[str, Any]]] = None,
        route_policy_snapshot_ttl_s: float = 2.0,
    ):
        self.groq_api_key = groq_api_key
        self.model = model
        self.local_model_path = Path(local_model_path)
        self.audio_queue: queue.Queue[np.ndarray] = queue.Queue()
        self.running = False
        self._asr_pipeline: Any = None
        self._runtime_lock = threading.RLock()
        self._runtime: Dict[str, Any] = {
            "calls_total": 0,
            "stream_calls": 0,
            "chunk_calls": 0,
            "local_success": 0,
            "local_error": 0,
            "cloud_success": 0,
            "cloud_error": 0,
            "fallback_success": 0,
            "fallback_error": 0,
            "latency_ema_ms": 0.0,
            "local_latency_ema_ms": 0.0,
            "cloud_latency_ema_ms": 0.0,
            "capture_latency_ema_ms": 0.0,
            "confidence_ema": 0.0,
            "fallback_rate_ema": 0.0,
            "last_error": "",
            "last_source": "",
            "last_model": "",
            "last_mode": "",
            "last_called_at": 0.0,
            "provider_plan_skips": 0,
            "route_policy_plan_skips": 0,
            "route_policy_reroutes": 0,
            "route_policy_blocks": 0,
            "provider_order_ema": {
                self._LOCAL_PROVIDER: 1.0,
                self._CLOUD_PROVIDER: 0.0,
            },
            "attempt_chain_history": [],
            "history": [],
        }
        self._max_history = 240
        self._max_attempt_history = 120

        self._provider_failure_streak_threshold = max(1, int(provider_failure_streak_threshold))
        self._provider_cooldown_s = max(1.0, float(provider_cooldown_s))
        self._provider_max_cooldown_s = max(self._provider_cooldown_s, float(provider_max_cooldown_s))
        self._provider_state: Dict[str, Dict[str, Any]] = {
            self._LOCAL_PROVIDER: self._new_provider_state(enabled=True),
            self._CLOUD_PROVIDER: self._new_provider_state(enabled=bool(str(groq_api_key or "").strip())),
        }
        self._provider_policy_failure_streak_threshold = max(
            1,
            int(
                os.getenv(
                    "JARVIS_STT_PROVIDER_POLICY_FAILURE_STREAK_THRESHOLD",
                    str(self._provider_failure_streak_threshold),
                )
                or self._provider_failure_streak_threshold
            ),
        )
        self._provider_policy_base_cooldown_s = max(
            0.5,
            float(
                os.getenv(
                    "JARVIS_STT_PROVIDER_POLICY_BASE_COOLDOWN_S",
                    str(self._provider_cooldown_s),
                )
                or self._provider_cooldown_s
            ),
        )
        self._provider_policy_max_cooldown_s = max(
            self._provider_policy_base_cooldown_s,
            float(
                os.getenv(
                    "JARVIS_STT_PROVIDER_POLICY_MAX_COOLDOWN_S",
                    str(max(self._provider_max_cooldown_s, self._provider_cooldown_s * 2.0)),
                )
                or max(self._provider_max_cooldown_s, self._provider_cooldown_s * 2.0)
            ),
        )
        self._provider_policy_state: Dict[str, Dict[str, Any]] = {}
        enabled_env = str(os.getenv("JARVIS_STT_PROVIDER_STATE_ENABLED", "1")).strip().lower()
        self._provider_state_enabled = (
            bool(provider_state_enabled)
            if provider_state_enabled is not None
            else enabled_env in {"1", "true", "yes", "on"}
        )
        default_state_path = str(os.getenv("JARVIS_STT_PROVIDER_STATE_PATH", "data/stt_provider_state.json")).strip() or "data/stt_provider_state.json"
        configured_state_path = str(provider_state_path).strip() if isinstance(provider_state_path, str) else default_state_path
        self._provider_state_path = Path(configured_state_path)
        self._provider_state_persist_interval_s = max(
            0.2,
            float(
                os.getenv(
                    "JARVIS_STT_PROVIDER_STATE_PERSIST_INTERVAL_S",
                    str(provider_state_persist_interval_s),
                )
                or provider_state_persist_interval_s
            ),
        )
        self._provider_state_last_persist_epoch = 0.0
        self._provider_state_last_error = ""
        self._provider_state_loaded = False
        self._route_policy_provider = route_policy_provider
        self._route_policy_snapshot_ttl_s = max(0.0, float(route_policy_snapshot_ttl_s or 0.0))
        self._route_policy_snapshot: Dict[str, Any] = {}
        self._route_policy_last_refresh_epoch = 0.0
        if self._provider_state_enabled:
            self._load_provider_state()

    def _new_provider_state(self, *, enabled: bool) -> Dict[str, Any]:
        return {
            "enabled": bool(enabled),
            "attempts": 0,
            "success": 0,
            "error": 0,
            "failure_streak": 0,
            "cooldown_until_epoch": 0.0,
            "last_cooldown_s": 0.0,
            "last_attempt_at": 0.0,
            "last_success_at": 0.0,
            "last_failure_at": 0.0,
            "last_error": "",
            "last_latency_ms": 0.0,
            "latency_ema_ms": 0.0,
            "health_score": 0.78 if enabled else 0.0,
            "health": "healthy" if enabled else "disabled",
        }

    def _normalize_model_label(self, model_name: str) -> str:
        clean = str(model_name or "").strip()
        if not clean:
            clean = str(self.model or "").strip() or "default"
        try:
            base = Path(clean).name.strip()
        except Exception:
            base = clean
        lowered = str(base or clean).strip().lower()
        return lowered.replace(" ", "_") or "default"

    def _provider_policy_key(self, provider: str, model_name: str = "") -> str:
        clean_provider = str(provider or "").strip().lower() or "unknown"
        model_label = self._normalize_model_label(model_name)
        return f"{clean_provider}:{model_label}"

    def _new_provider_policy_state(self, *, provider: str, model_name: str) -> Dict[str, Any]:
        return {
            "provider": str(provider or "").strip().lower() or "unknown",
            "model": self._normalize_model_label(model_name),
            "attempts": 0,
            "success": 0,
            "error": 0,
            "failure_streak": 0,
            "timeout_error": 0,
            "rate_limit_error": 0,
            "auth_error": 0,
            "transport_error": 0,
            "model_error": 0,
            "other_error": 0,
            "outage_score": 0.0,
            "outage_level": "nominal",
            "cooldown_until_epoch": 0.0,
            "last_error_bucket": "",
            "last_error": "",
            "last_success_at": 0.0,
            "last_failure_at": 0.0,
            "updated_at": 0.0,
        }

    @staticmethod
    def _normalize_route_policy_provider(value: Any) -> str:
        clean = str(value or "").strip().lower()
        if clean in {"cloud", "remote"}:
            return STTEngine._CLOUD_PROVIDER
        if clean in {STTEngine._LOCAL_PROVIDER, STTEngine._CLOUD_PROVIDER}:
            return clean
        return clean

    def _normalize_route_policy_snapshot(self, payload: Dict[str, Any] | None) -> Dict[str, Any]:
        row = dict(payload) if isinstance(payload, dict) else {}
        fallback_candidates: List[str] = []
        seen: set[str] = set()
        for item in row.get("fallback_candidates", []) if isinstance(row.get("fallback_candidates", []), list) else []:
            clean = self._normalize_route_policy_provider(item)
            if clean not in {self._LOCAL_PROVIDER, self._CLOUD_PROVIDER} or clean in seen:
                continue
            seen.add(clean)
            fallback_candidates.append(clean)
        recommended_provider = self._normalize_route_policy_provider(row.get("recommended_provider", ""))
        selected_provider = self._normalize_route_policy_provider(row.get("selected_provider", ""))
        return {
            "status": str(row.get("status", "success") or "success").strip().lower() or "success",
            "task": str(row.get("task", "stt") or "stt").strip().lower() or "stt",
            "generated_at": float(row.get("generated_at", time.time()) or time.time()),
            "requires_offline": bool(row.get("requires_offline", False)),
            "privacy_mode": bool(row.get("privacy_mode", False)),
            "mission_profile": str(row.get("mission_profile", "balanced") or "balanced").strip().lower() or "balanced",
            "selected_provider": selected_provider,
            "recommended_provider": recommended_provider,
            "route_adjusted": bool(row.get("route_adjusted", False)),
            "route_blocked": bool(row.get("route_blocked", False)),
            "local_route_viable": bool(row.get("local_route_viable", selected_provider == self._LOCAL_PROVIDER)),
            "autonomy_safe": bool(row.get("autonomy_safe", False)),
            "autonomous_allowed": bool(row.get("autonomous_allowed", True)),
            "review_required": bool(row.get("review_required", False)),
            "blacklisted": bool(row.get("blacklisted", False)),
            "suppressed": bool(row.get("suppressed", False)),
            "demoted": bool(row.get("demoted", False)),
            "recovery_pending": bool(row.get("recovery_pending", False)),
            "cooldown_hint_s": max(0.0, float(row.get("cooldown_hint_s", 0.0) or 0.0)),
            "reason_code": str(row.get("reason_code", "") or "").strip().lower(),
            "reason": str(row.get("reason", "") or "").strip(),
            "fallback_candidates": fallback_candidates,
            "summary": dict(row.get("summary", {})) if isinstance(row.get("summary", {}), dict) else {},
            "route_item": dict(row.get("route_item", {})) if isinstance(row.get("route_item", {}), dict) else {},
        }

    def update_route_policy_snapshot(self, payload: Dict[str, Any] | None, *, source: str = "manual") -> Dict[str, Any]:
        normalized = self._normalize_route_policy_snapshot(payload)
        normalized["source"] = str(source or "manual").strip().lower() or "manual"
        with self._runtime_lock:
            self._route_policy_snapshot = normalized
            self._route_policy_last_refresh_epoch = time.time()
        return dict(normalized)

    def _refresh_route_policy_snapshot(self, *, force: bool = False) -> Dict[str, Any]:
        now = time.time()
        provider = self._route_policy_provider
        if callable(provider):
            with self._runtime_lock:
                current = dict(self._route_policy_snapshot) if isinstance(self._route_policy_snapshot, dict) else {}
                last_refresh = float(self._route_policy_last_refresh_epoch or 0.0)
            if force or not current or (self._route_policy_snapshot_ttl_s <= 0.0) or ((now - last_refresh) >= self._route_policy_snapshot_ttl_s):
                try:
                    payload = provider()
                except Exception as exc:  # noqa: BLE001
                    payload = {
                        "status": "error",
                        "task": "stt",
                        "reason_code": "route_policy_provider_error",
                        "reason": str(exc),
                    }
                return self.update_route_policy_snapshot(payload, source="provider")
        with self._runtime_lock:
            return dict(self._route_policy_snapshot) if isinstance(self._route_policy_snapshot, dict) else {}

    def route_policy_status(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        return self._refresh_route_policy_snapshot(force=force_refresh)

    def _provider_route_policy_gate(self, provider: str, *, route_policy: Dict[str, Any]) -> Tuple[bool, str, float, float]:
        clean_provider = self._normalize_route_policy_provider(provider)
        if clean_provider not in {self._LOCAL_PROVIDER, self._CLOUD_PROVIDER}:
            return True, "", 0.0, 0.0
        policy = dict(route_policy) if isinstance(route_policy, dict) else {}
        recommended = self._normalize_route_policy_provider(policy.get("recommended_provider", ""))
        fallback_candidates = [
            self._normalize_route_policy_provider(item)
            for item in policy.get("fallback_candidates", [])
            if self._normalize_route_policy_provider(item) in {self._LOCAL_PROVIDER, self._CLOUD_PROVIDER}
        ] if isinstance(policy.get("fallback_candidates", []), list) else []
        route_blocked = bool(policy.get("route_blocked", False))
        route_adjusted = bool(policy.get("route_adjusted", False))
        blacklisted = bool(policy.get("blacklisted", False))
        demoted = bool(policy.get("demoted", False))
        suppressed = bool(policy.get("suppressed", False))
        local_route_viable = bool(policy.get("local_route_viable", True))
        autonomous_allowed = bool(policy.get("autonomous_allowed", True))
        review_required = bool(policy.get("review_required", False))
        cooldown_hint_s = max(0.0, float(policy.get("cooldown_hint_s", 0.0) or 0.0))
        recommended_cloud = recommended == self._CLOUD_PROVIDER or self._CLOUD_PROVIDER in fallback_candidates

        if clean_provider == self._LOCAL_PROVIDER:
            if route_blocked:
                return False, "route_policy_blocked", cooldown_hint_s, -0.55
            if blacklisted:
                return False, "route_policy_blacklisted", cooldown_hint_s, -0.45
            if demoted or suppressed:
                return False, "route_policy_demoted" if demoted else "route_policy_suppressed", cooldown_hint_s, -0.35
            if not local_route_viable:
                if recommended_cloud or route_adjusted:
                    return False, "route_policy_rerouted", cooldown_hint_s, -0.3
                return False, "route_policy_local_unviable", cooldown_hint_s, -0.28
            if recommended == self._CLOUD_PROVIDER and (route_adjusted or review_required or not autonomous_allowed):
                return False, "route_policy_rerouted", cooldown_hint_s, -0.24
            return True, "", 0.0, 0.24 if recommended == self._LOCAL_PROVIDER else 0.0

        if route_blocked and not recommended_cloud:
            return False, "route_policy_no_safe_reroute", cooldown_hint_s, -0.5
        if policy.get("requires_offline", False) and not recommended_cloud:
            return False, "route_policy_offline_local_only", cooldown_hint_s, -0.45
        if policy.get("privacy_mode", False) and not recommended_cloud:
            return False, "route_policy_privacy_local_only", cooldown_hint_s, -0.45
        if recommended == self._CLOUD_PROVIDER or self._CLOUD_PROVIDER in fallback_candidates:
            return True, "", 0.0, 0.42 if route_adjusted else 0.34
        if recommended == self._LOCAL_PROVIDER and local_route_viable and not route_adjusted and not review_required:
            return True, "", 0.0, -0.18
        return True, "", 0.0, 0.0

    def _load_provider_state(self) -> None:
        path = self._provider_state_path
        try:
            if not path.exists():
                return
            raw = path.read_text(encoding="utf-8")
            payload = json.loads(raw) if raw.strip() else {}
            if not isinstance(payload, dict):
                return
        except Exception as exc:  # noqa: BLE001
            self._provider_state_last_error = str(exc)
            return

        now = time.time()
        providers_payload = payload.get("providers") if isinstance(payload.get("providers"), dict) else {}
        runtime_payload = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
        policy_payload = payload.get("provider_policies") if isinstance(payload.get("provider_policies"), dict) else {}

        with self._runtime_lock:
            for provider in (self._LOCAL_PROVIDER, self._CLOUD_PROVIDER):
                current = self._provider_state.setdefault(provider, self._new_provider_state(enabled=(provider == self._LOCAL_PROVIDER)))
                loaded = providers_payload.get(provider) if isinstance(providers_payload.get(provider), dict) else {}
                if not isinstance(loaded, dict):
                    continue
                for key in ("attempts", "success", "error", "failure_streak"):
                    current[key] = int(loaded.get(key, current.get(key, 0)) or 0)
                for key in ("health_score", "latency_ema_ms", "last_latency_ms", "last_cooldown_s"):
                    current[key] = float(loaded.get(key, current.get(key, 0.0)) or 0.0)
                for key in ("last_attempt_at", "last_success_at", "last_failure_at"):
                    current[key] = float(loaded.get(key, current.get(key, 0.0)) or 0.0)
                current["last_error"] = str(loaded.get("last_error", current.get("last_error", "")) or "")
                cooldown_until = float(loaded.get("cooldown_until_epoch", current.get("cooldown_until_epoch", 0.0)) or 0.0)
                current["cooldown_until_epoch"] = cooldown_until if cooldown_until > now else 0.0
                current["health"] = self._provider_health_bucket(
                    health_score=float(current.get("health_score", 0.0) or 0.0),
                    failure_streak=int(current.get("failure_streak", 0) or 0),
                    cooldown_until=float(current.get("cooldown_until_epoch", 0.0) or 0.0),
                    now_epoch=now,
                    enabled=bool(current.get("enabled", False)),
                )

            order_payload = runtime_payload.get("provider_order_ema") if isinstance(runtime_payload.get("provider_order_ema"), dict) else {}
            if isinstance(order_payload, dict):
                existing_order = self._runtime.get("provider_order_ema", {})
                if not isinstance(existing_order, dict):
                    existing_order = {}
                for provider in (self._LOCAL_PROVIDER, self._CLOUD_PROVIDER):
                    existing_order[provider] = round(max(0.0, min(1.0, float(order_payload.get(provider, existing_order.get(provider, 0.0)) or 0.0))), 6)
                self._runtime["provider_order_ema"] = existing_order

            for key in ("fallback_rate_ema", "local_latency_ema_ms", "cloud_latency_ema_ms"):
                if key in runtime_payload:
                    self._runtime[key] = max(0.0, float(runtime_payload.get(key, self._runtime.get(key, 0.0)) or 0.0))

            loaded_policy_state: Dict[str, Dict[str, Any]] = {}
            for raw_key, row in policy_payload.items():
                if not isinstance(row, dict):
                    continue
                key = str(raw_key or "").strip().lower()
                provider = str(row.get("provider", "") or "").strip().lower()
                model_name = str(row.get("model", "") or "").strip()
                if not key:
                    key = self._provider_policy_key(provider, model_name)
                template = self._new_provider_policy_state(provider=provider, model_name=model_name)
                template["attempts"] = int(row.get("attempts", template["attempts"]) or template["attempts"])
                template["success"] = int(row.get("success", template["success"]) or template["success"])
                template["error"] = int(row.get("error", template["error"]) or template["error"])
                template["failure_streak"] = int(row.get("failure_streak", template["failure_streak"]) or template["failure_streak"])
                for bucket in (
                    "timeout_error",
                    "rate_limit_error",
                    "auth_error",
                    "transport_error",
                    "model_error",
                    "other_error",
                ):
                    template[bucket] = int(row.get(bucket, template[bucket]) or template[bucket])
                template["outage_score"] = max(0.0, min(1.0, float(row.get("outage_score", template["outage_score"]) or template["outage_score"])))
                cooldown_until = float(row.get("cooldown_until_epoch", template["cooldown_until_epoch"]) or template["cooldown_until_epoch"])
                template["cooldown_until_epoch"] = cooldown_until if cooldown_until > now else 0.0
                template["last_error_bucket"] = str(row.get("last_error_bucket", template["last_error_bucket"]) or template["last_error_bucket"])
                template["last_error"] = str(row.get("last_error", template["last_error"]) or template["last_error"])
                template["last_success_at"] = float(row.get("last_success_at", template["last_success_at"]) or template["last_success_at"])
                template["last_failure_at"] = float(row.get("last_failure_at", template["last_failure_at"]) or template["last_failure_at"])
                template["updated_at"] = float(row.get("updated_at", template["updated_at"]) or template["updated_at"])
                template["outage_level"] = self._provider_outage_level(
                    outage_score=float(template.get("outage_score", 0.0) or 0.0),
                    failure_streak=int(template.get("failure_streak", 0) or 0),
                    cooldown_until=float(template.get("cooldown_until_epoch", 0.0) or 0.0),
                    now_epoch=now,
                )
                loaded_policy_state[key] = template
            if loaded_policy_state:
                self._provider_policy_state = loaded_policy_state

            self._provider_state_loaded = True
            self._provider_state_last_error = ""
            self._provider_state_last_persist_epoch = max(
                self._provider_state_last_persist_epoch,
                float(payload.get("persisted_at", 0.0) or 0.0),
            )

    def _build_provider_state_payload(self) -> Dict[str, Any]:
        with self._runtime_lock:
            providers: Dict[str, Dict[str, Any]] = {}
            for provider in (self._LOCAL_PROVIDER, self._CLOUD_PROVIDER):
                row = self._provider_state.get(provider, {})
                if not isinstance(row, dict):
                    continue
                providers[provider] = {
                    "enabled": bool(row.get("enabled", False)),
                    "attempts": int(row.get("attempts", 0) or 0),
                    "success": int(row.get("success", 0) or 0),
                    "error": int(row.get("error", 0) or 0),
                    "failure_streak": int(row.get("failure_streak", 0) or 0),
                    "cooldown_until_epoch": float(row.get("cooldown_until_epoch", 0.0) or 0.0),
                    "last_cooldown_s": float(row.get("last_cooldown_s", 0.0) or 0.0),
                    "last_attempt_at": float(row.get("last_attempt_at", 0.0) or 0.0),
                    "last_success_at": float(row.get("last_success_at", 0.0) or 0.0),
                    "last_failure_at": float(row.get("last_failure_at", 0.0) or 0.0),
                    "last_error": str(row.get("last_error", "") or ""),
                    "last_latency_ms": float(row.get("last_latency_ms", 0.0) or 0.0),
                    "latency_ema_ms": float(row.get("latency_ema_ms", 0.0) or 0.0),
                    "health_score": float(row.get("health_score", 0.0) or 0.0),
                    "health": str(row.get("health", "") or ""),
                }
            runtime = {
                "provider_order_ema": dict(self._runtime.get("provider_order_ema", {})),
                "fallback_rate_ema": float(self._runtime.get("fallback_rate_ema", 0.0) or 0.0),
                "local_latency_ema_ms": float(self._runtime.get("local_latency_ema_ms", 0.0) or 0.0),
                "cloud_latency_ema_ms": float(self._runtime.get("cloud_latency_ema_ms", 0.0) or 0.0),
            }
            provider_policies = {
                key: {
                    "provider": str(value.get("provider", "") or ""),
                    "model": str(value.get("model", "") or ""),
                    "attempts": int(value.get("attempts", 0) or 0),
                    "success": int(value.get("success", 0) or 0),
                    "error": int(value.get("error", 0) or 0),
                    "failure_streak": int(value.get("failure_streak", 0) or 0),
                    "timeout_error": int(value.get("timeout_error", 0) or 0),
                    "rate_limit_error": int(value.get("rate_limit_error", 0) or 0),
                    "auth_error": int(value.get("auth_error", 0) or 0),
                    "transport_error": int(value.get("transport_error", 0) or 0),
                    "model_error": int(value.get("model_error", 0) or 0),
                    "other_error": int(value.get("other_error", 0) or 0),
                    "outage_score": float(value.get("outage_score", 0.0) or 0.0),
                    "outage_level": str(value.get("outage_level", "") or ""),
                    "cooldown_until_epoch": float(value.get("cooldown_until_epoch", 0.0) or 0.0),
                    "last_error_bucket": str(value.get("last_error_bucket", "") or ""),
                    "last_error": str(value.get("last_error", "") or ""),
                    "last_success_at": float(value.get("last_success_at", 0.0) or 0.0),
                    "last_failure_at": float(value.get("last_failure_at", 0.0) or 0.0),
                    "updated_at": float(value.get("updated_at", 0.0) or 0.0),
                }
                for key, value in self._provider_policy_state.items()
                if isinstance(value, dict)
            }
        return {
            "version": 1,
            "persisted_at": time.time(),
            "model": self.model,
            "providers": providers,
            "runtime": runtime,
            "provider_policies": provider_policies,
        }

    def _persist_provider_state(self, *, force: bool = False, reason: str = "") -> None:
        if not self._provider_state_enabled:
            return
        now = time.time()
        with self._runtime_lock:
            if not force and (now - self._provider_state_last_persist_epoch) < self._provider_state_persist_interval_s:
                return
            payload = self._build_provider_state_payload()
            payload["reason"] = str(reason or "")
            path = self._provider_state_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = path.with_suffix(f"{path.suffix}.tmp")
            temp_path.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")
            temp_path.replace(path)
            with self._runtime_lock:
                self._provider_state_last_persist_epoch = float(payload.get("persisted_at", now) or now)
                self._provider_state_last_error = ""
        except Exception as exc:  # noqa: BLE001
            with self._runtime_lock:
                self._provider_state_last_error = str(exc)

    def _record_audio(self, duration: float, sample_rate: int = 16000) -> np.ndarray:
        audio = sd.rec(
            int(duration * sample_rate),
            samplerate=sample_rate,
            channels=1,
            dtype="float32",
        )
        sd.wait()
        return audio.reshape(-1)

    def transcribe(self, duration: float = 4.0) -> Dict[str, Any]:
        started = time.monotonic()
        self._register_call("chunk")
        audio = self._record_audio(duration)
        result = self._transcribe_audio(audio, mode="chunk", overall_started=started)
        if result.get("status") == "success":
            result["audio_duration_s"] = round(float(audio.size) / 16000.0, 4) if isinstance(audio, np.ndarray) else 0.0
        return result

    def transcribe_stream(
        self,
        *,
        max_duration_s: float = 8.0,
        frame_duration_s: float = 0.2,
        energy_threshold: float = 0.015,
        silence_duration_s: float = 0.9,
        min_speech_s: float = 0.35,
        sample_rate: int = 16000,
        fallback_to_chunk: bool = True,
    ) -> Dict[str, Any]:
        started = time.monotonic()
        self._register_call("stream")
        capture_started = time.monotonic()
        capture = self._capture_vad_audio(
            max_duration_s=max_duration_s,
            frame_duration_s=frame_duration_s,
            energy_threshold=energy_threshold,
            silence_duration_s=silence_duration_s,
            min_speech_s=min_speech_s,
            sample_rate=sample_rate,
        )
        capture_latency_ms = max(0.0, (time.monotonic() - capture_started) * 1000.0)
        self._record_capture_latency(capture_latency_ms)

        if capture.get("status") != "success":
            if fallback_to_chunk:
                fallback_duration = max(1.0, min(float(max_duration_s), 6.0))
                result = self.transcribe(duration=fallback_duration)
                if result.get("status") == "success":
                    result["mode"] = "chunk_fallback"
                    result["fallback_reason"] = str(capture.get("message", "stream_capture_failed"))
                    result["capture"] = {
                        "speech_detected": bool(capture.get("speech_detected", False)),
                        "captured_duration_s": float(capture.get("captured_duration_s", 0.0)),
                        "max_duration_s": float(max_duration_s),
                        "capture_latency_ms": round(capture_latency_ms, 6),
                    }
                return result

            error_message = str(capture.get("message", "stream capture failed"))
            self._record_runtime(
                source="capture",
                status="error",
                mode="stream_vad",
                latency_ms=max(0.0, (time.monotonic() - started) * 1000.0),
                provider_latency_ms=0.0,
                confidence=0.0,
                model="",
                message=error_message,
                attempt_chain=[
                    {
                        "provider": "capture",
                        "status": "error",
                        "attempted": True,
                        "latency_ms": round(capture_latency_ms, 6),
                        "error": error_message,
                    }
                ],
            )
            return capture

        audio = capture.get("audio")
        if not isinstance(audio, np.ndarray) or audio.size <= 0:
            message = "No captured speech audio."
            self._record_runtime(
                source="capture",
                status="error",
                mode="stream_vad",
                latency_ms=max(0.0, (time.monotonic() - started) * 1000.0),
                provider_latency_ms=0.0,
                confidence=0.0,
                model="",
                message=message,
                attempt_chain=[
                    {
                        "provider": "capture",
                        "status": "error",
                        "attempted": True,
                        "latency_ms": round(capture_latency_ms, 6),
                        "error": message,
                    }
                ],
            )
            return {"status": "error", "message": message}

        result = self._transcribe_audio(audio, mode="stream_vad", overall_started=started)
        if result.get("status") == "success":
            result["capture"] = {
                "speech_detected": bool(capture.get("speech_detected", False)),
                "captured_duration_s": float(capture.get("captured_duration_s", 0.0)),
                "max_duration_s": float(max_duration_s),
                "capture_latency_ms": round(capture_latency_ms, 6),
            }
        return result

    def _register_call(self, mode: str) -> None:
        with self._runtime_lock:
            self._runtime["calls_total"] = int(self._runtime.get("calls_total", 0) or 0) + 1
            if str(mode or "").strip().lower() == "stream":
                self._runtime["stream_calls"] = int(self._runtime.get("stream_calls", 0) or 0) + 1
            else:
                self._runtime["chunk_calls"] = int(self._runtime.get("chunk_calls", 0) or 0) + 1
            self._runtime["last_called_at"] = time.time()

    def _transcribe_audio(self, audio: np.ndarray, *, mode: str, overall_started: float) -> Dict[str, Any]:
        provider_plan = self._build_provider_plan(mode=mode)
        attempt_chain: List[Dict[str, Any]] = list(provider_plan.get("skipped", []))
        ordered = list(provider_plan.get("ordered", []))
        skipped_local = any(
            str(item.get("provider", "")).strip().lower() == self._LOCAL_PROVIDER
            and str(item.get("status", "")).strip().lower() == "skipped"
            for item in attempt_chain
            if isinstance(item, dict)
        )
        errors: Dict[str, str] = {}
        attempted_providers: List[str] = []
        first_attempted_provider = ""
        last_provider_latency_ms = 0.0
        last_model = ""

        if not ordered:
            message = "No STT provider available (all providers unavailable or cooling down)."
            self._record_runtime(
                source="none",
                status="error",
                mode=mode,
                latency_ms=max(0.0, (time.monotonic() - overall_started) * 1000.0),
                provider_latency_ms=0.0,
                confidence=0.0,
                model="",
                message=message,
                attempt_chain=attempt_chain,
            )
            return {
                "status": "error",
                "message": message,
                "source": "none",
                "mode": mode,
                "errors": {},
                "attempt_chain": attempt_chain,
            }

        for provider in ordered:
            attempted_providers.append(provider)
            if not first_attempted_provider:
                first_attempted_provider = provider

            attempt_started = time.monotonic()
            provider_result = self._transcribe_with_provider(provider, audio)
            provider_latency_ms = max(0.0, (time.monotonic() - attempt_started) * 1000.0)
            last_provider_latency_ms = provider_latency_ms

            status_name = str(provider_result.get("status", "error")).strip().lower() or "error"
            message = str(provider_result.get("message", "")).strip()
            model = str(provider_result.get("model", "")).strip()
            last_model = model or last_model
            text = str(provider_result.get("text", "")).strip()

            success = status_name == "success" and bool(text)
            if status_name == "success" and not text:
                success = False
                message = "STT provider returned an empty transcript."
                provider_result = {
                    "status": "error",
                    "source": provider,
                    "model": model,
                    "message": message,
                }

            self._update_provider_state(
                provider=provider,
                success=success,
                latency_ms=provider_latency_ms,
                model_name=model,
                error_message=message if not success else "",
            )

            attempt_chain.append(
                {
                    "provider": provider,
                    "attempted": True,
                    "status": "success" if success else "error",
                    "latency_ms": round(provider_latency_ms, 6),
                    "model": model,
                    "error": "" if success else message,
                }
            )

            if success:
                confidence = self._estimate_transcript_confidence(text)
                fallback_used = len(attempted_providers) > 1 or (provider != self._LOCAL_PROVIDER and skipped_local)
                result = dict(provider_result)
                result["status"] = "success"
                result["source"] = provider
                result["mode"] = mode
                result["confidence"] = confidence
                result["attempt_chain"] = attempt_chain
                if fallback_used:
                    if skipped_local and provider != self._LOCAL_PROVIDER:
                        result["fallback_from"] = self._LOCAL_PROVIDER
                    elif first_attempted_provider and first_attempted_provider != provider:
                        result["fallback_from"] = first_attempted_provider

                self._record_provider_order(attempted_providers)
                self._record_runtime(
                    source=provider,
                    status="success",
                    mode=mode,
                    latency_ms=max(0.0, (time.monotonic() - overall_started) * 1000.0),
                    provider_latency_ms=provider_latency_ms,
                    confidence=confidence,
                    model=model,
                    message="",
                    fallback=fallback_used,
                    attempt_chain=attempt_chain,
                )
                return result

            errors[provider] = message or "unknown stt provider error"

        fallback_used = len(attempted_providers) > 1
        attempted_sequence = " -> ".join(attempted_providers) if attempted_providers else "none"
        error_summary = " | ".join(f"{name}: {detail}" for name, detail in errors.items()) if errors else "unknown failure"
        message = f"STT failed after provider chain [{attempted_sequence}]. {error_summary}"
        primary_source = first_attempted_provider or "none"
        self._record_provider_order(attempted_providers)
        self._record_runtime(
            source=primary_source,
            status="error",
            mode=mode,
            latency_ms=max(0.0, (time.monotonic() - overall_started) * 1000.0),
            provider_latency_ms=last_provider_latency_ms,
            confidence=0.0,
            model=last_model,
            message=message,
            fallback=fallback_used,
            attempt_chain=attempt_chain,
        )
        return {
            "status": "error",
            "source": primary_source,
            "mode": mode,
            "message": message,
            "errors": errors,
            "attempt_chain": attempt_chain,
        }

    def _transcribe_with_provider(self, provider: str, audio: np.ndarray) -> Dict[str, Any]:
        name = str(provider or "").strip().lower()
        if name == self._LOCAL_PROVIDER:
            return self._transcribe_local(audio)
        if name in {self._CLOUD_PROVIDER, "cloud"}:
            return self._transcribe_groq(audio)
        return {"status": "error", "source": name, "message": f"unknown STT provider '{provider}'"}

    def _build_provider_plan(self, *, mode: str) -> Dict[str, Any]:
        self._refresh_provider_availability()
        route_policy = self._refresh_route_policy_snapshot(force=False)
        now = time.time()
        candidates = [self._LOCAL_PROVIDER, self._CLOUD_PROVIDER]
        ready: List[Tuple[str, float]] = []
        delayed: List[Tuple[str, float, float]] = []
        skipped: List[Dict[str, Any]] = []
        route_policy_skip_count = 0
        route_policy_reroute_count = 0
        route_policy_block_count = 0

        for provider in candidates:
            is_ready, reason, remaining_s = self._provider_readiness(provider, now_epoch=now)
            state = self._provider_state.get(provider, self._new_provider_state(enabled=False))
            policy = self._provider_policy_lookup(provider, model_name=self.model, now_epoch=now)
            policy_cooldown_until = float(policy.get("cooldown_until_epoch", 0.0) or 0.0)
            policy_remaining_s = max(0.0, policy_cooldown_until - now)
            outage_score = max(0.0, min(1.0, float(policy.get("outage_score", 0.0) or 0.0)))
            outage_level = str(policy.get("outage_level", "nominal") or "nominal").strip().lower()
            if is_ready and policy_remaining_s > 0.0 and outage_level in {"critical", "degraded"}:
                is_ready = False
                reason = "policy_cooldown"
                remaining_s = max(remaining_s, policy_remaining_s)
            health_score = float(state.get("health_score", 0.0) or 0.0)
            mode_bias = 0.08 if provider == self._LOCAL_PROVIDER else 0.02
            if str(mode or "").strip().lower() in {"stream_vad", "stream"} and provider == self._LOCAL_PROVIDER:
                mode_bias = 0.05
            if str(mode or "").strip().lower() == "chunk_fallback" and provider == self._CLOUD_PROVIDER:
                mode_bias += 0.05
            outage_penalty = min(0.55, outage_score * 0.42)
            if outage_level == "watch":
                outage_penalty += 0.03
            elif outage_level == "degraded":
                outage_penalty += 0.08
            elif outage_level == "critical":
                outage_penalty += 0.14
            ranking_score = (health_score + mode_bias) - outage_penalty

            route_allowed, route_reason, route_remaining_s, route_bias = self._provider_route_policy_gate(
                provider,
                route_policy=route_policy,
            )
            ranking_score += route_bias
            if is_ready and not route_allowed:
                is_ready = False
                reason = route_reason or "route_policy_blocked"
                remaining_s = max(remaining_s, route_remaining_s)
            if not route_allowed:
                route_policy_skip_count += 1
                if reason == "route_policy_blocked":
                    route_policy_block_count += 1
                if reason == "route_policy_rerouted":
                    route_policy_reroute_count += 1

            if is_ready:
                ready.append((provider, ranking_score))
                continue

            skipped.append(
                {
                    "provider": provider,
                    "attempted": False,
                    "status": "skipped",
                    "reason": reason,
                    "cooldown_remaining_s": round(max(0.0, remaining_s), 6),
                    "outage_level": outage_level,
                    "outage_score": round(outage_score, 6),
                    "route_policy_reason": route_reason if route_reason else "",
                }
            )
            if reason in {"cooldown", "temporarily_disabled", "policy_cooldown"}:
                delayed_penalty = min(0.65, remaining_s / 30.0) + min(0.5, outage_score * 0.35)
                delayed.append((provider, ranking_score - delayed_penalty, remaining_s))

        ordered = [provider for provider, _score in sorted(ready, key=lambda item: item[1], reverse=True)]
        if not ordered and delayed:
            delayed_sorted = sorted(delayed, key=lambda item: (item[2], -item[1]))
            ordered = [provider for provider, _score, _remaining in delayed_sorted]

        if skipped:
            with self._runtime_lock:
                self._runtime["provider_plan_skips"] = int(self._runtime.get("provider_plan_skips", 0) or 0) + len(skipped)
                self._runtime["route_policy_plan_skips"] = int(self._runtime.get("route_policy_plan_skips", 0) or 0) + route_policy_skip_count
                self._runtime["route_policy_reroutes"] = int(self._runtime.get("route_policy_reroutes", 0) or 0) + route_policy_reroute_count
                self._runtime["route_policy_blocks"] = int(self._runtime.get("route_policy_blocks", 0) or 0) + route_policy_block_count

        return {"ordered": ordered, "skipped": skipped, "route_policy": route_policy}

    def _refresh_provider_availability(self) -> None:
        with self._runtime_lock:
            local = self._provider_state.setdefault(self._LOCAL_PROVIDER, self._new_provider_state(enabled=True))
            local["enabled"] = True
            if float(local.get("health_score", 0.0) or 0.0) <= 0.0:
                local["health_score"] = 0.72
                local["health"] = "degraded"

            cloud = self._provider_state.setdefault(
                self._CLOUD_PROVIDER,
                self._new_provider_state(enabled=bool(str(self.groq_api_key or "").strip())),
            )
            cloud_enabled = bool(str(self.groq_api_key or "").strip())
            cloud["enabled"] = cloud_enabled
            if not cloud_enabled:
                cloud["health"] = "disabled"
                cloud["cooldown_until_epoch"] = 0.0
                cloud["last_cooldown_s"] = 0.0
                cloud["failure_streak"] = 0
                cloud["health_score"] = 0.0
            elif float(cloud.get("health_score", 0.0) or 0.0) <= 0.0:
                cloud["health_score"] = 0.62
                cloud["health"] = "degraded"

    def _provider_readiness(self, provider: str, *, now_epoch: float) -> Tuple[bool, str, float]:
        state = self._provider_state.get(provider)
        if not isinstance(state, dict):
            return False, "unknown_provider", 0.0
        if not bool(state.get("enabled", False)):
            reason = "missing_api_key" if provider == self._CLOUD_PROVIDER else "disabled"
            return False, reason, 0.0

        cooldown_until = float(state.get("cooldown_until_epoch", 0.0) or 0.0)
        remaining = cooldown_until - now_epoch
        if remaining > 0.0:
            return False, "cooldown", remaining
        if cooldown_until > 0.0 and remaining <= 0.0:
            state["cooldown_until_epoch"] = 0.0
            state["last_cooldown_s"] = 0.0

        return True, "", 0.0

    @staticmethod
    def _classify_provider_error(error_message: str) -> str:
        text = str(error_message or "").strip().lower()
        if not text:
            return "other_error"
        if "timeout" in text or "timed out" in text:
            return "timeout_error"
        if "rate limit" in text or "429" in text or "too many requests" in text:
            return "rate_limit_error"
        if "401" in text or "403" in text or "unauthorized" in text or "forbidden" in text or "api key" in text:
            return "auth_error"
        if "connection" in text or "network" in text or "dns" in text or "refused" in text or "socket" in text:
            return "transport_error"
        if "model" in text or "checkpoint" in text or "transformers unavailable" in text:
            return "model_error"
        return "other_error"

    @staticmethod
    def _provider_error_severity(bucket: str) -> float:
        mapping = {
            "timeout_error": 0.95,
            "rate_limit_error": 0.85,
            "auth_error": 1.2,
            "transport_error": 1.05,
            "model_error": 1.1,
            "other_error": 0.75,
        }
        return float(mapping.get(str(bucket or "").strip().lower(), 0.75))

    def _provider_outage_level(
        self,
        *,
        outage_score: float,
        failure_streak: int,
        cooldown_until: float,
        now_epoch: float,
    ) -> str:
        score = max(0.0, min(1.0, float(outage_score)))
        streak = max(0, int(failure_streak))
        if cooldown_until > now_epoch and (score >= 0.82 or streak >= (self._provider_policy_failure_streak_threshold + 2)):
            return "critical"
        if cooldown_until > now_epoch or score >= 0.58 or streak >= self._provider_policy_failure_streak_threshold:
            return "degraded"
        if score >= 0.34 or streak >= 1:
            return "watch"
        return "nominal"

    def _provider_policy_lookup(self, provider: str, *, model_name: str = "", now_epoch: Optional[float] = None) -> Dict[str, Any]:
        now = float(now_epoch) if now_epoch is not None else time.time()
        key = self._provider_policy_key(provider, model_name)
        with self._runtime_lock:
            row = self._provider_policy_state.get(key)
            if not isinstance(row, dict):
                row = self._new_provider_policy_state(provider=provider, model_name=model_name)
                self._provider_policy_state[key] = row
            cooldown_until = float(row.get("cooldown_until_epoch", 0.0) or 0.0)
            if cooldown_until > 0.0 and cooldown_until <= now:
                row["cooldown_until_epoch"] = 0.0
                cooldown_until = 0.0
            row["outage_level"] = self._provider_outage_level(
                outage_score=float(row.get("outage_score", 0.0) or 0.0),
                failure_streak=int(row.get("failure_streak", 0) or 0),
                cooldown_until=cooldown_until,
                now_epoch=now,
            )
            return dict(row)

    def _update_provider_policy(
        self,
        *,
        provider: str,
        model_name: str,
        success: bool,
        latency_ms: float,
        error_message: str,
        now_epoch: float,
    ) -> None:
        key = self._provider_policy_key(provider, model_name)
        with self._runtime_lock:
            row = self._provider_policy_state.get(key)
            if not isinstance(row, dict):
                row = self._new_provider_policy_state(provider=provider, model_name=model_name)
                self._provider_policy_state[key] = row

            row["provider"] = str(provider or "").strip().lower() or "unknown"
            row["model"] = self._normalize_model_label(model_name)
            row["attempts"] = int(row.get("attempts", 0) or 0) + 1
            row["updated_at"] = float(now_epoch)
            row["last_error"] = str(error_message or "").strip()

            current_score = max(0.0, min(1.0, float(row.get("outage_score", 0.0) or 0.0)))
            if success:
                row["success"] = int(row.get("success", 0) or 0) + 1
                row["failure_streak"] = 0
                row["last_error_bucket"] = ""
                row["last_success_at"] = float(now_epoch)
                row["cooldown_until_epoch"] = 0.0
                recovered = (0.78 * current_score) - 0.06
                row["outage_score"] = max(0.0, min(1.0, recovered))
            else:
                row["error"] = int(row.get("error", 0) or 0) + 1
                failure_streak = int(row.get("failure_streak", 0) or 0) + 1
                row["failure_streak"] = failure_streak
                row["last_failure_at"] = float(now_epoch)
                bucket = self._classify_provider_error(error_message)
                row[bucket] = int(row.get(bucket, 0) or 0) + 1
                row["last_error_bucket"] = bucket

                severity = self._provider_error_severity(bucket)
                next_score = (0.76 * current_score) + (0.24 * min(1.6, severity + (0.08 * failure_streak)))
                row["outage_score"] = max(0.0, min(1.0, next_score))
                if failure_streak >= self._provider_policy_failure_streak_threshold or float(row["outage_score"]) >= 0.62:
                    multiplier = 1.0 + (0.48 * max(0, failure_streak - self._provider_policy_failure_streak_threshold))
                    if bucket == "auth_error":
                        multiplier += 0.9
                    elif bucket in {"timeout_error", "transport_error"}:
                        multiplier += 0.35
                    cooldown_s = min(
                        self._provider_policy_max_cooldown_s,
                        self._provider_policy_base_cooldown_s * max(1.0, multiplier) * (1.0 + (float(row["outage_score"]) * 0.9)),
                    )
                    next_cooldown_until = now_epoch + cooldown_s
                    if next_cooldown_until > float(row.get("cooldown_until_epoch", 0.0) or 0.0):
                        row["cooldown_until_epoch"] = next_cooldown_until

            row["outage_level"] = self._provider_outage_level(
                outage_score=float(row.get("outage_score", 0.0) or 0.0),
                failure_streak=int(row.get("failure_streak", 0) or 0),
                cooldown_until=float(row.get("cooldown_until_epoch", 0.0) or 0.0),
                now_epoch=now_epoch,
            )

    def _update_provider_state(
        self,
        *,
        provider: str,
        success: bool,
        latency_ms: float,
        model_name: str,
        error_message: str,
    ) -> None:
        now = time.time()
        with self._runtime_lock:
            state = self._provider_state.setdefault(provider, self._new_provider_state(enabled=(provider == self._LOCAL_PROVIDER)))
            state["attempts"] = int(state.get("attempts", 0) or 0) + 1
            state["last_attempt_at"] = now
            state["last_latency_ms"] = round(max(0.0, float(latency_ms)), 6)
            latency_ema = float(state.get("latency_ema_ms", 0.0) or 0.0)
            state["latency_ema_ms"] = (
                max(0.0, float(latency_ms))
                if latency_ema <= 0.0
                else max(0.0, (0.78 * latency_ema) + (0.22 * max(0.0, float(latency_ms))))
            )
            previous_score = float(state.get("health_score", 0.65) or 0.65)

            if success:
                state["success"] = int(state.get("success", 0) or 0) + 1
                state["failure_streak"] = 0
                state["last_success_at"] = now
                state["last_error"] = ""
                state["cooldown_until_epoch"] = 0.0
                state["last_cooldown_s"] = 0.0
                next_score = previous_score + (0.14 * (1.0 - previous_score))
                state["health_score"] = max(0.0, min(1.0, next_score))
            else:
                state["error"] = int(state.get("error", 0) or 0) + 1
                failure_streak = int(state.get("failure_streak", 0) or 0) + 1
                state["failure_streak"] = failure_streak
                state["last_failure_at"] = now
                state["last_error"] = str(error_message or "").strip()

                penalty = min(0.55, 0.12 + (0.08 * float(failure_streak)))
                next_score = previous_score * (1.0 - penalty)
                state["health_score"] = max(0.03, min(1.0, next_score))

                if failure_streak >= self._provider_failure_streak_threshold:
                    multiplier = 1.0 + (0.65 * float(failure_streak - self._provider_failure_streak_threshold))
                    cooldown_s = min(self._provider_max_cooldown_s, self._provider_cooldown_s * multiplier)
                    cooldown_until = now + cooldown_s
                    if cooldown_until > float(state.get("cooldown_until_epoch", 0.0) or 0.0):
                        state["cooldown_until_epoch"] = cooldown_until
                        state["last_cooldown_s"] = round(cooldown_s, 6)

            state["health"] = self._provider_health_bucket(
                health_score=float(state.get("health_score", 0.0) or 0.0),
                failure_streak=int(state.get("failure_streak", 0) or 0),
                cooldown_until=float(state.get("cooldown_until_epoch", 0.0) or 0.0),
                now_epoch=now,
                enabled=bool(state.get("enabled", False)),
            )
        self._update_provider_policy(
            provider=provider,
            model_name=model_name,
            success=success,
            latency_ms=latency_ms,
            error_message=error_message,
            now_epoch=now,
        )
        self._persist_provider_state(reason=f"provider_update:{provider}:{'success' if success else 'error'}")

    @staticmethod
    def _provider_health_bucket(
        *,
        health_score: float,
        failure_streak: int,
        cooldown_until: float,
        now_epoch: float,
        enabled: bool,
    ) -> str:
        if not enabled:
            return "disabled"
        if cooldown_until > now_epoch:
            return "cooldown"
        if failure_streak >= 4 or health_score < 0.25:
            return "critical"
        if failure_streak >= 2 or health_score < 0.55:
            return "degraded"
        return "healthy"

    def _record_provider_order(self, attempted_providers: List[str]) -> None:
        if not attempted_providers:
            return
        with self._runtime_lock:
            weights = self._runtime.get("provider_order_ema", {})
            if not isinstance(weights, dict):
                weights = {self._LOCAL_PROVIDER: 0.5, self._CLOUD_PROVIDER: 0.5}
            for provider in (self._LOCAL_PROVIDER, self._CLOUD_PROVIDER):
                current = float(weights.get(provider, 0.0) or 0.0)
                target = 1.0 if attempted_providers and attempted_providers[0] == provider else 0.0
                updated = target if current <= 0.0 else ((0.82 * current) + (0.18 * target))
                weights[provider] = round(max(0.0, min(1.0, updated)), 6)
            self._runtime["provider_order_ema"] = weights
        self._persist_provider_state(reason="provider_order")

    def _capture_vad_audio(
        self,
        *,
        max_duration_s: float,
        frame_duration_s: float,
        energy_threshold: float,
        silence_duration_s: float,
        min_speech_s: float,
        sample_rate: int,
    ) -> Dict[str, Any]:
        max_duration = max(1.0, min(float(max_duration_s), 30.0))
        frame_duration = max(0.05, min(float(frame_duration_s), 1.0))
        threshold = max(0.001, min(float(energy_threshold), 0.3))
        silence_hold = max(0.2, min(float(silence_duration_s), 4.0))
        min_speech = max(0.1, min(float(min_speech_s), 8.0))
        rate = max(8000, min(int(sample_rate), 48000))
        block_size = max(160, int(rate * frame_duration))

        audio_queue: queue.Queue[np.ndarray] = queue.Queue()

        def _on_audio(indata: np.ndarray, _frames: int, _time_info: Any, _status: Any) -> None:
            try:
                audio_queue.put_nowait(np.array(indata, dtype=np.float32, copy=True))
            except Exception:
                return

        pre_roll: deque[np.ndarray] = deque(maxlen=4)
        chunks: list[np.ndarray] = []
        speech_started_at: Optional[float] = None
        last_voice_at: Optional[float] = None
        started_at = time.monotonic()

        try:
            with sd.InputStream(
                samplerate=rate,
                channels=1,
                dtype="float32",
                callback=_on_audio,
                blocksize=block_size,
            ):
                while (time.monotonic() - started_at) < max_duration:
                    try:
                        chunk = audio_queue.get(timeout=0.25)
                    except queue.Empty:
                        continue
                    frame = np.asarray(chunk, dtype=np.float32).reshape(-1)
                    if frame.size <= 0:
                        continue
                    energy = float(np.sqrt(np.mean(np.square(frame))))
                    now = time.monotonic()
                    if speech_started_at is None:
                        pre_roll.append(frame)

                    if energy >= threshold:
                        if speech_started_at is None:
                            speech_started_at = now
                            last_voice_at = now
                            if pre_roll:
                                chunks.extend(list(pre_roll))
                        else:
                            last_voice_at = now
                        chunks.append(frame)
                    elif speech_started_at is not None:
                        chunks.append(frame)
                        if last_voice_at is not None and (now - last_voice_at) >= silence_hold and (now - speech_started_at) >= min_speech:
                            break
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": f"stream capture failed: {exc}"}

        if not chunks:
            return {"status": "error", "message": "No speech detected during stream capture.", "speech_detected": False}

        captured = np.concatenate(chunks).astype(np.float32, copy=False)
        duration = float(captured.size) / float(rate) if captured.size > 0 else 0.0
        return {
            "status": "success",
            "speech_detected": speech_started_at is not None,
            "captured_duration_s": round(duration, 4),
            "audio": captured,
        }

    def _transcribe_local(self, audio: np.ndarray) -> Dict[str, Any]:
        try:
            from transformers import pipeline  # type: ignore
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": f"transformers unavailable: {exc}", "source": self._LOCAL_PROVIDER}

        model_source = self._resolve_local_model_source()
        try:
            if self._asr_pipeline is None:
                self._asr_pipeline = pipeline(
                    task="automatic-speech-recognition",
                    model=model_source,
                    device=-1,
                )
            result = self._asr_pipeline({"raw": audio, "sampling_rate": 16000})
            text = result.get("text", "").strip() if isinstance(result, dict) else str(result)
            return {"status": "success", "text": text, "source": self._LOCAL_PROVIDER, "model": model_source}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": f"local STT failed: {exc}", "source": self._LOCAL_PROVIDER, "model": model_source}

    def _resolve_local_model_source(self) -> str:
        # Direct explicit path.
        if self.local_model_path.exists():
            if self.local_model_path.is_dir():
                if (self.local_model_path / "config.json").exists():
                    return str(self.local_model_path)
                for child in self.local_model_path.iterdir():
                    if child.is_dir() and self.model.lower() in child.name.lower() and (child / "config.json").exists():
                        return str(child)
            else:
                return str(self.local_model_path)

        # Probe common project roots.
        roots = [
            Path("stt"),
            Path("../stt"),
            Path("../../stt"),
            Path.cwd() / "stt",
            Path.cwd().parent / "stt",
        ]
        for root in roots:
            try:
                resolved = root.resolve()
            except Exception:
                continue
            if not resolved.exists() or not resolved.is_dir():
                continue
            for child in resolved.iterdir():
                if child.is_dir() and self.model.lower() in child.name.lower() and (child / "config.json").exists():
                    return str(child)

        return self.model

    def _transcribe_groq(self, audio: np.ndarray) -> Dict[str, Any]:
        import requests

        if not str(self.groq_api_key or "").strip():
            return {"status": "error", "message": "Groq API key is missing.", "source": self._CLOUD_PROVIDER, "model": self.model}

        try:
            wav_buffer = io.BytesIO()
            with wave.open(wav_buffer, "wb") as wav_file:
                wav_file.setnchannels(1)
                wav_file.setsampwidth(2)
                wav_file.setframerate(16000)
                pcm16 = np.clip(audio, -1.0, 1.0)
                pcm16 = (pcm16 * 32767).astype(np.int16)
                wav_file.writeframes(pcm16.tobytes())
            wav_buffer.seek(0)

            resp = requests.post(
                "https://api.groq.com/openai/v1/audio/transcriptions",
                data={"model": self.model},
                files={"file": ("audio.wav", wav_buffer, "audio/wav")},
                headers={"Authorization": f"Bearer {self.groq_api_key}"},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                "status": "success",
                "text": str(data.get("text", "")).strip(),
                "source": self._CLOUD_PROVIDER,
                "model": self.model,
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc), "source": self._CLOUD_PROVIDER, "model": self.model}

    def diagnostics(self, *, history_limit: int = 24) -> Dict[str, Any]:
        bounded = max(1, min(int(history_limit), self._max_history))
        attempt_bounded = max(1, min(int(history_limit), self._max_attempt_history))
        route_policy = self._refresh_route_policy_snapshot(force=False)
        with self._runtime_lock:
            history = list(self._runtime.get("history", []))
            attempts = list(self._runtime.get("attempt_chain_history", []))
            provider_order_ema = dict(self._runtime.get("provider_order_ema", {}))
            provider_snapshot = self._provider_state_snapshot()
            provider_policy_snapshot = self._provider_policy_snapshot()
            payload = {
                "status": "success",
                "calls_total": int(self._runtime.get("calls_total", 0) or 0),
                "stream_calls": int(self._runtime.get("stream_calls", 0) or 0),
                "chunk_calls": int(self._runtime.get("chunk_calls", 0) or 0),
                "local_success": int(self._runtime.get("local_success", 0) or 0),
                "local_error": int(self._runtime.get("local_error", 0) or 0),
                "cloud_success": int(self._runtime.get("cloud_success", 0) or 0),
                "cloud_error": int(self._runtime.get("cloud_error", 0) or 0),
                "fallback_success": int(self._runtime.get("fallback_success", 0) or 0),
                "fallback_error": int(self._runtime.get("fallback_error", 0) or 0),
                "latency_ema_ms": round(float(self._runtime.get("latency_ema_ms", 0.0) or 0.0), 6),
                "local_latency_ema_ms": round(float(self._runtime.get("local_latency_ema_ms", 0.0) or 0.0), 6),
                "cloud_latency_ema_ms": round(float(self._runtime.get("cloud_latency_ema_ms", 0.0) or 0.0), 6),
                "capture_latency_ema_ms": round(float(self._runtime.get("capture_latency_ema_ms", 0.0) or 0.0), 6),
                "confidence_ema": round(float(self._runtime.get("confidence_ema", 0.0) or 0.0), 6),
                "fallback_rate_ema": round(float(self._runtime.get("fallback_rate_ema", 0.0) or 0.0), 6),
                "provider_plan_skips": int(self._runtime.get("provider_plan_skips", 0) or 0),
                "route_policy_plan_skips": int(self._runtime.get("route_policy_plan_skips", 0) or 0),
                "route_policy_reroutes": int(self._runtime.get("route_policy_reroutes", 0) or 0),
                "route_policy_blocks": int(self._runtime.get("route_policy_blocks", 0) or 0),
                "provider_order_ema": provider_order_ema,
                "last_error": str(self._runtime.get("last_error", "") or ""),
                "last_source": str(self._runtime.get("last_source", "") or ""),
                "last_model": str(self._runtime.get("last_model", "") or ""),
                "last_mode": str(self._runtime.get("last_mode", "") or ""),
                "last_called_at": float(self._runtime.get("last_called_at", 0.0) or 0.0),
                "history": history[-bounded:],
                "attempt_chain_history": attempts[-attempt_bounded:],
                "providers": provider_snapshot,
                "provider_policies": provider_policy_snapshot,
                "provider_state_persistence": {
                    "enabled": bool(self._provider_state_enabled),
                    "path": str(self._provider_state_path),
                    "loaded": bool(self._provider_state_loaded),
                    "last_persist_epoch": float(self._provider_state_last_persist_epoch),
                    "last_error": str(self._provider_state_last_error or ""),
                    "persist_interval_s": float(self._provider_state_persist_interval_s),
                },
                "route_policy": route_policy,
            }
        total_success = float(payload["local_success"] + payload["cloud_success"])
        total_error = float(payload["local_error"] + payload["cloud_error"])
        success_rate = total_success / max(1.0, total_success + total_error)
        payload["success_rate"] = round(success_rate, 6)
        payload["health"] = "healthy" if success_rate >= 0.75 else ("degraded" if success_rate >= 0.45 else "critical")
        payload["provider_health"] = self._aggregate_provider_health(payload.get("providers", {}))
        return payload

    @staticmethod
    def _coerce_bool(value: Any, default: bool = False) -> bool:
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

    def policy_status(self, *, history_limit: int = 80) -> Dict[str, Any]:
        bounded = max(1, min(int(history_limit), max(self._max_history, self._max_attempt_history)))
        diagnostics = self.diagnostics(history_limit=bounded)
        return {
            "status": "success",
            "history_limit": bounded,
            "provider_failure_streak_threshold": int(self._provider_failure_streak_threshold),
            "provider_cooldown_s": float(self._provider_cooldown_s),
            "provider_max_cooldown_s": float(self._provider_max_cooldown_s),
            "policy_failure_streak_threshold": int(self._provider_policy_failure_streak_threshold),
            "policy_base_cooldown_s": float(self._provider_policy_base_cooldown_s),
            "policy_max_cooldown_s": float(self._provider_policy_max_cooldown_s),
            "provider_state_persistence": diagnostics.get("provider_state_persistence", {}),
            "provider_health": diagnostics.get("provider_health", "unknown"),
            "providers": diagnostics.get("providers", {}),
            "provider_policies": diagnostics.get("provider_policies", {}),
            "provider_order_ema": diagnostics.get("provider_order_ema", {}),
            "fallback_rate_ema": diagnostics.get("fallback_rate_ema", 0.0),
            "success_rate": diagnostics.get("success_rate", 0.0),
            "health": diagnostics.get("health", "unknown"),
            "attempt_chain_history": diagnostics.get("attempt_chain_history", []),
            "route_policy": diagnostics.get("route_policy", {}),
        }

    def update_policy(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = payload if isinstance(payload, dict) else {}
        changed: Dict[str, Any] = {}

        with self._runtime_lock:
            if "provider_failure_streak_threshold" in data:
                value = max(1, min(int(data.get("provider_failure_streak_threshold", self._provider_failure_streak_threshold)), 20))
                self._provider_failure_streak_threshold = value
                changed["provider_failure_streak_threshold"] = value
            if "provider_cooldown_s" in data:
                value = max(0.5, min(float(data.get("provider_cooldown_s", self._provider_cooldown_s)), 1800.0))
                self._provider_cooldown_s = value
                if self._provider_max_cooldown_s < value:
                    self._provider_max_cooldown_s = value
                changed["provider_cooldown_s"] = value
            if "provider_max_cooldown_s" in data:
                value = max(self._provider_cooldown_s, min(float(data.get("provider_max_cooldown_s", self._provider_max_cooldown_s)), 86400.0))
                self._provider_max_cooldown_s = value
                changed["provider_max_cooldown_s"] = value

            if "policy_failure_streak_threshold" in data:
                value = max(1, min(int(data.get("policy_failure_streak_threshold", self._provider_policy_failure_streak_threshold)), 30))
                self._provider_policy_failure_streak_threshold = value
                changed["policy_failure_streak_threshold"] = value
            if "policy_base_cooldown_s" in data:
                value = max(0.5, min(float(data.get("policy_base_cooldown_s", self._provider_policy_base_cooldown_s)), 3600.0))
                self._provider_policy_base_cooldown_s = value
                if self._provider_policy_max_cooldown_s < value:
                    self._provider_policy_max_cooldown_s = value
                changed["policy_base_cooldown_s"] = value
            if "policy_max_cooldown_s" in data:
                value = max(self._provider_policy_base_cooldown_s, min(float(data.get("policy_max_cooldown_s", self._provider_policy_max_cooldown_s)), 86400.0))
                self._provider_policy_max_cooldown_s = value
                changed["policy_max_cooldown_s"] = value

            if "provider_state_enabled" in data:
                enabled = self._coerce_bool(data.get("provider_state_enabled"), self._provider_state_enabled)
                self._provider_state_enabled = enabled
                changed["provider_state_enabled"] = enabled
            if "provider_state_persist_interval_s" in data:
                interval = max(0.2, min(float(data.get("provider_state_persist_interval_s", self._provider_state_persist_interval_s)), 300.0))
                self._provider_state_persist_interval_s = interval
                changed["provider_state_persist_interval_s"] = interval
            if "provider_state_path" in data:
                state_path = str(data.get("provider_state_path", "") or "").strip()
                if state_path:
                    self._provider_state_path = Path(state_path)
                    changed["provider_state_path"] = state_path

            providers_payload = data.get("providers")
            if isinstance(providers_payload, dict):
                providers_changed: Dict[str, Any] = {}
                for provider_key, value in providers_payload.items():
                    provider = str(provider_key or "").strip().lower()
                    if provider not in {self._LOCAL_PROVIDER, self._CLOUD_PROVIDER}:
                        continue
                    state = self._provider_state.setdefault(
                        provider,
                        self._new_provider_state(enabled=(provider == self._LOCAL_PROVIDER)),
                    )
                    if isinstance(value, dict):
                        if "enabled" in value:
                            state["enabled"] = self._coerce_bool(value.get("enabled"), bool(state.get("enabled", False)))
                            providers_changed[f"{provider}.enabled"] = bool(state["enabled"])
                    else:
                        state["enabled"] = self._coerce_bool(value, bool(state.get("enabled", False)))
                        providers_changed[f"{provider}.enabled"] = bool(state["enabled"])
                    if not bool(state.get("enabled", False)):
                        state["cooldown_until_epoch"] = 0.0
                        state["last_cooldown_s"] = 0.0
                        state["health"] = "disabled"
                if providers_changed:
                    changed["providers"] = providers_changed

            if self._coerce_bool(data.get("reset_cooldowns"), False):
                for row in self._provider_state.values():
                    if isinstance(row, dict):
                        row["cooldown_until_epoch"] = 0.0
                        row["last_cooldown_s"] = 0.0
                for row in self._provider_policy_state.values():
                    if isinstance(row, dict):
                        row["cooldown_until_epoch"] = 0.0
                changed["reset_cooldowns"] = True

            if self._coerce_bool(data.get("reset_provider_policies"), False):
                self._provider_policy_state = {}
                changed["reset_provider_policies"] = True

            if self._coerce_bool(data.get("reset_runtime_history"), False):
                self._runtime["history"] = []
                self._runtime["attempt_chain_history"] = []
                self._runtime["provider_plan_skips"] = 0
                changed["reset_runtime_history"] = True

        persist_now = self._coerce_bool(data.get("persist_now"), False)
        if persist_now:
            self._persist_provider_state(force=True, reason="manual_policy_update")
            changed["persist_now"] = True
        elif changed:
            self._persist_provider_state(force=False, reason="policy_update")

        return {
            "status": "success",
            "updated": bool(changed),
            "changed": changed,
            "policy": self.policy_status(history_limit=max(40, int(data.get("history_limit", 120) or 120))),
        }

    def _provider_state_snapshot(self) -> Dict[str, Dict[str, Any]]:
        now = time.time()
        payload: Dict[str, Dict[str, Any]] = {}
        for provider, state in self._provider_state.items():
            if not isinstance(state, dict):
                continue
            cooldown_until = float(state.get("cooldown_until_epoch", 0.0) or 0.0)
            remaining = max(0.0, cooldown_until - now)
            payload[provider] = {
                "enabled": bool(state.get("enabled", False)),
                "attempts": int(state.get("attempts", 0) or 0),
                "success": int(state.get("success", 0) or 0),
                "error": int(state.get("error", 0) or 0),
                "failure_streak": int(state.get("failure_streak", 0) or 0),
                "health_score": round(float(state.get("health_score", 0.0) or 0.0), 6),
                "health": str(state.get("health", "unknown") or "unknown"),
                "latency_ema_ms": round(float(state.get("latency_ema_ms", 0.0) or 0.0), 6),
                "last_latency_ms": round(float(state.get("last_latency_ms", 0.0) or 0.0), 6),
                "last_error": str(state.get("last_error", "") or ""),
                "last_attempt_at": float(state.get("last_attempt_at", 0.0) or 0.0),
                "last_success_at": float(state.get("last_success_at", 0.0) or 0.0),
                "last_failure_at": float(state.get("last_failure_at", 0.0) or 0.0),
                "cooldown_until_epoch": cooldown_until,
                "cooldown_remaining_s": round(remaining, 6),
            }
        return payload

    def _provider_policy_snapshot(self) -> Dict[str, Dict[str, Any]]:
        now = time.time()
        payload: Dict[str, Dict[str, Any]] = {}
        for key, row in self._provider_policy_state.items():
            if not isinstance(row, dict):
                continue
            cooldown_until = float(row.get("cooldown_until_epoch", 0.0) or 0.0)
            payload[str(key)] = {
                "provider": str(row.get("provider", "") or ""),
                "model": str(row.get("model", "") or ""),
                "attempts": int(row.get("attempts", 0) or 0),
                "success": int(row.get("success", 0) or 0),
                "error": int(row.get("error", 0) or 0),
                "failure_streak": int(row.get("failure_streak", 0) or 0),
                "outage_score": round(float(row.get("outage_score", 0.0) or 0.0), 6),
                "outage_level": str(row.get("outage_level", "nominal") or "nominal"),
                "last_error_bucket": str(row.get("last_error_bucket", "") or ""),
                "last_error": str(row.get("last_error", "") or ""),
                "timeout_error": int(row.get("timeout_error", 0) or 0),
                "rate_limit_error": int(row.get("rate_limit_error", 0) or 0),
                "auth_error": int(row.get("auth_error", 0) or 0),
                "transport_error": int(row.get("transport_error", 0) or 0),
                "model_error": int(row.get("model_error", 0) or 0),
                "other_error": int(row.get("other_error", 0) or 0),
                "last_success_at": float(row.get("last_success_at", 0.0) or 0.0),
                "last_failure_at": float(row.get("last_failure_at", 0.0) or 0.0),
                "updated_at": float(row.get("updated_at", 0.0) or 0.0),
                "cooldown_until_epoch": cooldown_until,
                "cooldown_remaining_s": round(max(0.0, cooldown_until - now), 6),
            }
        return payload

    @staticmethod
    def _aggregate_provider_health(provider_snapshot: Dict[str, Dict[str, Any]]) -> str:
        if not isinstance(provider_snapshot, dict) or not provider_snapshot:
            return "unknown"
        health_values = [str(item.get("health", "unknown")) for item in provider_snapshot.values() if isinstance(item, dict)]
        if not health_values:
            return "unknown"
        if any(value == "critical" for value in health_values):
            return "critical"
        if any(value in {"degraded", "cooldown"} for value in health_values):
            return "degraded"
        if all(value == "disabled" for value in health_values):
            return "disabled"
        return "healthy"

    def _record_capture_latency(self, capture_latency_ms: float) -> None:
        self._update_ema("capture_latency_ema_ms", max(0.0, float(capture_latency_ms)), alpha=0.22)

    def _record_runtime(
        self,
        *,
        source: str,
        status: str,
        mode: str,
        latency_ms: float,
        provider_latency_ms: float,
        confidence: float,
        model: str,
        message: str,
        fallback: bool = False,
        attempt_chain: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        source_name = str(source or "").strip().lower() or "unknown"
        status_name = str(status or "").strip().lower() or "error"
        mode_name = str(mode or "").strip().lower() or "unknown"
        with self._runtime_lock:
            self._runtime["last_source"] = source_name
            self._runtime["last_model"] = str(model or "").strip()
            self._runtime["last_mode"] = mode_name
            self._runtime["last_error"] = str(message or "").strip()
            if status_name == "success":
                if source_name == self._LOCAL_PROVIDER:
                    self._runtime["local_success"] = int(self._runtime.get("local_success", 0) or 0) + 1
                elif source_name in {self._CLOUD_PROVIDER, "cloud"}:
                    self._runtime["cloud_success"] = int(self._runtime.get("cloud_success", 0) or 0) + 1
                if fallback:
                    self._runtime["fallback_success"] = int(self._runtime.get("fallback_success", 0) or 0) + 1
            else:
                if source_name == self._LOCAL_PROVIDER:
                    self._runtime["local_error"] = int(self._runtime.get("local_error", 0) or 0) + 1
                elif source_name in {self._CLOUD_PROVIDER, "cloud"}:
                    self._runtime["cloud_error"] = int(self._runtime.get("cloud_error", 0) or 0) + 1
                if fallback:
                    self._runtime["fallback_error"] = int(self._runtime.get("fallback_error", 0) or 0) + 1

            self._update_ema("latency_ema_ms", max(0.0, float(latency_ms)), alpha=0.18)
            if source_name == self._LOCAL_PROVIDER:
                self._update_ema("local_latency_ema_ms", max(0.0, float(provider_latency_ms)), alpha=0.2)
            elif source_name in {self._CLOUD_PROVIDER, "cloud"}:
                self._update_ema("cloud_latency_ema_ms", max(0.0, float(provider_latency_ms)), alpha=0.2)
            self._update_ema("confidence_ema", max(0.0, min(float(confidence), 1.0)), alpha=0.16)
            self._update_ema("fallback_rate_ema", 1.0 if fallback else 0.0, alpha=0.12)

            history = self._runtime.get("history", [])
            if not isinstance(history, list):
                history = []
            history.append(
                {
                    "at": time.time(),
                    "source": source_name,
                    "status": status_name,
                    "mode": mode_name,
                    "latency_ms": round(max(0.0, float(latency_ms)), 6),
                    "provider_latency_ms": round(max(0.0, float(provider_latency_ms)), 6),
                    "confidence": round(max(0.0, min(float(confidence), 1.0)), 6),
                    "model": str(model or "").strip(),
                    "fallback": bool(fallback),
                    "error": str(message or "").strip(),
                }
            )
            if len(history) > self._max_history:
                history = history[-self._max_history :]
            self._runtime["history"] = history

            if isinstance(attempt_chain, list):
                attempts = self._runtime.get("attempt_chain_history", [])
                if not isinstance(attempts, list):
                    attempts = []
                attempts.append(
                    {
                        "at": time.time(),
                        "mode": mode_name,
                        "source": source_name,
                        "status": status_name,
                        "chain": attempt_chain,
                    }
                )
                if len(attempts) > self._max_attempt_history:
                    attempts = attempts[-self._max_attempt_history :]
                self._runtime["attempt_chain_history"] = attempts

    def _update_ema(self, key: str, value: float, *, alpha: float) -> None:
        with self._runtime_lock:
            current = float(self._runtime.get(key, 0.0) or 0.0)
            next_value = float(value) if current <= 0.0 else ((1.0 - float(alpha)) * current) + (float(alpha) * float(value))
            self._runtime[key] = max(0.0, next_value)

    @staticmethod
    def _estimate_transcript_confidence(text: str) -> float:
        clean = str(text or "").strip()
        if not clean:
            return 0.0
        tokens = [token for token in clean.lower().split() if token.strip()]
        token_count = len(tokens)
        if token_count <= 0:
            return 0.0
        unique_ratio = len(set(tokens)) / float(max(1, token_count))
        char_count = len(clean)
        length_score = min(1.0, char_count / 48.0)
        repetition_penalty = 0.0
        if token_count >= 4:
            top_freq = max(tokens.count(token) for token in set(tokens))
            repetition_penalty = min(0.32, max(0.0, (float(top_freq) / float(token_count)) - 0.42))
        punctuation_bonus = 0.06 if any(ch in clean for ch in ".?!") else 0.0
        confidence = (0.42 * unique_ratio) + (0.52 * length_score) + punctuation_bonus - repetition_penalty
        return max(0.0, min(1.0, round(confidence, 6)))
