from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, List


class ModelConnectorOrchestrator:
    """
    Adaptive routing governor for model connectors/providers.

    Responsibilities:
    - merge provider readiness + reliability into a route plan
    - maintain connector outcome learning (failure/latency EMAs)
    - expose diagnostics and policy tuning hooks
    """

    def __init__(self, *, state_path: str = "data/model_connector_orchestrator.json") -> None:
        self._lock = threading.RLock()
        self._state_path = Path(state_path)
        self._persist_every = 5
        self._dirty_updates = 0
        self._history_max = 160
        self._provider_state: Dict[str, Dict[str, Any]] = {}
        self._decision_history: List[Dict[str, Any]] = []
        self._policy: Dict[str, float] = {
            "readiness_weight": 1.8,
            "reliability_weight": 2.2,
            "quality_weight": 1.7,
            "latency_weight": 1.1,
            "privacy_weight": 1.2,
            "failure_ema_penalty_weight": 2.4,
            "cooldown_penalty_weight": 3.0,
            "offline_local_bonus": 2.6,
            "privacy_local_bonus": 1.8,
        }
        self._cooldown_base_s = 8.0
        self._cooldown_max_s = 240.0
        self._failure_streak_threshold = 3
        self._load_state()

    def plan_reasoning_route(
        self,
        *,
        registry: Any,
        provider_snapshot: Dict[str, Any],
        requires_offline: bool,
        privacy_mode: bool,
        mission_profile: str = "balanced",
        max_fallbacks: int = 3,
    ) -> Dict[str, Any]:
        clean_profile = str(mission_profile or "balanced").strip().lower() or "balanced"
        bounded_fallbacks = max(1, min(int(max_fallbacks), 6))
        now_epoch = time.time()

        candidates_raw = registry.list_by_task("reasoning") if registry is not None else []
        candidates = [row for row in candidates_raw if str(getattr(row, "provider", "")).strip()]
        provider_rows: Dict[str, Dict[str, Any]] = {}

        for profile in candidates:
            provider = str(getattr(profile, "provider", "")).strip().lower()
            if not provider:
                continue
            row = provider_rows.setdefault(
                provider,
                {
                    "provider": provider,
                    "models": [],
                    "quality_avg": 0.0,
                    "latency_avg": 0.0,
                    "privacy_avg": 0.0,
                    "count": 0,
                    "available_count": 0,
                },
            )
            row["models"].append(profile)
            row["count"] = int(row["count"]) + 1
            if bool(getattr(profile, "available", False)):
                row["available_count"] = int(row["available_count"]) + 1
            row["quality_avg"] += float(getattr(profile, "quality", 0) or 0)
            row["latency_avg"] += float(getattr(profile, "latency", 0) or 0)
            row["privacy_avg"] += float(getattr(profile, "privacy", 0) or 0)

        scored_rows: List[Dict[str, Any]] = []
        banned_providers: List[str] = []

        for provider, row in provider_rows.items():
            count = max(1, int(row.get("count", 1) or 1))
            row["quality_avg"] = float(row["quality_avg"]) / float(count)
            row["latency_avg"] = float(row["latency_avg"]) / float(count)
            row["privacy_avg"] = float(row["privacy_avg"]) / float(count)

            provider_diag = provider_snapshot.get(provider, {}) if isinstance(provider_snapshot, dict) else {}
            provider_ready = self._provider_ready(provider=provider, provider_diag=provider_diag)
            state = self._provider_state_row(provider)
            cooldown_until = float(state.get("cooldown_until_epoch", 0.0) or 0.0)
            cooldown_active = cooldown_until > now_epoch
            failure_ema = max(
                0.0,
                min(
                    max(
                        float(state.get("failure_ema", 0.0) or 0.0),
                        self._diag_failure_ema(provider_diag),
                    ),
                    1.0,
                ),
            )
            latency_ms = max(
                1.0,
                min(
                    float(state.get("latency_ema_ms", row.get("latency_avg", 120.0)) or row.get("latency_avg", 120.0)),
                    120_000.0,
                ),
            )
            quality_score = max(0.0, min(float(row.get("quality_avg", 0.0) or 0.0) / 100.0, 1.0))
            privacy_score = max(0.0, min(float(row.get("privacy_avg", 0.0) or 0.0) / 100.0, 1.0))
            latency_score = max(0.0, min(1.0, 1.0 - (latency_ms / 2000.0)))
            readiness_score = 1.0 if provider_ready else 0.0
            reliability_score = max(0.0, 1.0 - failure_ema)

            score = (
                readiness_score * self._policy["readiness_weight"]
                + reliability_score * self._policy["reliability_weight"]
                + quality_score * self._policy["quality_weight"]
                + latency_score * self._policy["latency_weight"]
                + privacy_score * self._policy["privacy_weight"]
            )
            score -= failure_ema * self._policy["failure_ema_penalty_weight"]
            if cooldown_active:
                score -= self._policy["cooldown_penalty_weight"]

            if requires_offline:
                if provider == "local":
                    score += self._policy["offline_local_bonus"]
                else:
                    score -= self._policy["offline_local_bonus"] * 1.4
            if privacy_mode:
                if provider == "local":
                    score += self._policy["privacy_local_bonus"]
                else:
                    score -= 0.35

            blocked = bool(cooldown_active) or bool(requires_offline and provider != "local")
            if not provider_ready and provider != "local":
                blocked = True

            if blocked:
                banned_providers.append(provider)

            scored_rows.append(
                {
                    "provider": provider,
                    "score": round(float(score), 6),
                    "blocked": blocked,
                    "ready": provider_ready,
                    "cooldown_active": cooldown_active,
                    "cooldown_until_epoch": cooldown_until,
                    "failure_ema": round(float(failure_ema), 6),
                    "quality_score": round(float(quality_score), 6),
                    "latency_score": round(float(latency_score), 6),
                    "privacy_score": round(float(privacy_score), 6),
                    "reliability_score": round(float(reliability_score), 6),
                    "model_count": int(count),
                    "available_count": int(row.get("available_count", 0) or 0),
                }
            )

        scored_rows.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
        preferred_provider = "local"
        for row in scored_rows:
            if bool(row.get("blocked", False)):
                continue
            preferred_provider = str(row.get("provider", "local"))
            break

        if not scored_rows:
            preferred_provider = "local"

        provider_affinity = self._build_provider_affinity(scored_rows)
        fallback_providers = [
            str(row.get("provider", ""))
            for row in scored_rows
            if str(row.get("provider", "")).strip() and str(row.get("provider", "")).strip() != preferred_provider
        ][:bounded_fallbacks]

        preferred_model = ""
        if candidates:
            preferred_model = self._pick_model_name(
                candidates=candidates,
                provider=preferred_provider,
                privacy_mode=privacy_mode,
            )

        route_plan = {
            "status": "success",
            "task": "reasoning",
            "mission_profile": clean_profile,
            "requires_offline": bool(requires_offline),
            "privacy_mode": bool(privacy_mode),
            "preferred_provider": preferred_provider,
            "preferred_model": preferred_model,
            "fallback_providers": fallback_providers,
            "banned_providers": sorted(set(banned_providers)),
            "provider_affinity": provider_affinity,
            "providers": scored_rows,
            "generated_at": time.time(),
        }

        with self._lock:
            self._decision_history.append(
                {
                    "generated_at": route_plan["generated_at"],
                    "preferred_provider": preferred_provider,
                    "preferred_model": preferred_model,
                    "fallback_providers": list(fallback_providers),
                    "requires_offline": bool(requires_offline),
                    "privacy_mode": bool(privacy_mode),
                }
            )
            if len(self._decision_history) > self._history_max:
                self._decision_history = self._decision_history[-self._history_max :]
            self._mark_dirty_and_maybe_persist_locked()

        return route_plan

    def report_outcome(
        self,
        *,
        provider: str,
        success: bool,
        latency_ms: float = 0.0,
        error: str = "",
    ) -> Dict[str, Any]:
        clean_provider = str(provider or "").strip().lower()
        if not clean_provider:
            return {"status": "error", "message": "provider is required"}
        bounded_latency = max(0.0, min(float(latency_ms or 0.0), 120_000.0))

        with self._lock:
            row = self._provider_state_row(clean_provider)
            row["attempts"] = int(row.get("attempts", 0) or 0) + 1
            if success:
                row["success"] = int(row.get("success", 0) or 0) + 1
                row["failure_streak"] = 0
                row["last_success_at"] = time.time()
                row["last_error"] = ""
                row["cooldown_until_epoch"] = 0.0
            else:
                row["error"] = int(row.get("error", 0) or 0) + 1
                row["failure_streak"] = int(row.get("failure_streak", 0) or 0) + 1
                row["last_failure_at"] = time.time()
                row["last_error"] = str(error or "")
                if int(row.get("failure_streak", 0) or 0) >= self._failure_streak_threshold:
                    multiplier = 2 ** max(0, int(row.get("failure_streak", 0) or 0) - self._failure_streak_threshold)
                    cooldown_s = min(self._cooldown_max_s, self._cooldown_base_s * float(multiplier))
                    row["cooldown_until_epoch"] = max(float(row.get("cooldown_until_epoch", 0.0) or 0.0), time.time() + cooldown_s)

            alpha = 0.22
            sample_failure = 0.0 if success else 1.0
            row["failure_ema"] = ((1.0 - alpha) * float(row.get("failure_ema", 0.0) or 0.0)) + (alpha * sample_failure)
            if bounded_latency > 0:
                row["latency_ema_ms"] = ((1.0 - alpha) * float(row.get("latency_ema_ms", bounded_latency) or bounded_latency)) + (
                    alpha * bounded_latency
                )
                row["last_latency_ms"] = bounded_latency
            row["updated_at"] = time.time()
            self._mark_dirty_and_maybe_persist_locked()

            return {
                "status": "success",
                "provider": clean_provider,
                "failure_ema": round(float(row.get("failure_ema", 0.0) or 0.0), 6),
                "failure_streak": int(row.get("failure_streak", 0) or 0),
                "cooldown_until_epoch": float(row.get("cooldown_until_epoch", 0.0) or 0.0),
            }

    def update_policy(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(updates, dict):
            return {"status": "error", "message": "updates must be an object"}
        changed: Dict[str, float] = {}
        with self._lock:
            for key, value in updates.items():
                clean_key = str(key or "").strip()
                if clean_key in self._policy:
                    try:
                        bounded = max(0.05, min(float(value), 8.0))
                    except Exception:
                        continue
                    self._policy[clean_key] = bounded
                    changed[clean_key] = bounded
                elif clean_key == "cooldown_base_s":
                    try:
                        self._cooldown_base_s = max(1.0, min(float(value), 600.0))
                        changed[clean_key] = float(self._cooldown_base_s)
                    except Exception:
                        continue
                elif clean_key == "cooldown_max_s":
                    try:
                        self._cooldown_max_s = max(self._cooldown_base_s, min(float(value), 7200.0))
                        changed[clean_key] = float(self._cooldown_max_s)
                    except Exception:
                        continue
                elif clean_key == "failure_streak_threshold":
                    try:
                        self._failure_streak_threshold = max(2, min(int(value), 12))
                        changed[clean_key] = float(self._failure_streak_threshold)
                    except Exception:
                        continue
            self._mark_dirty_and_maybe_persist_locked(force=True)
        return {"status": "success", "changed": changed, "count": len(changed)}

    def diagnostics(self, *, provider: str = "", limit_history: int = 40) -> Dict[str, Any]:
        clean_provider = str(provider or "").strip().lower()
        bounded_history = max(1, min(int(limit_history), 400))
        with self._lock:
            provider_rows = {key: dict(value) for key, value in self._provider_state.items()}
            if clean_provider:
                provider_rows = {clean_provider: dict(provider_rows.get(clean_provider, {}))}
            history = [dict(row) for row in self._decision_history[-bounded_history:]]
            return {
                "status": "success",
                "policy": dict(self._policy),
                "cooldown_base_s": float(self._cooldown_base_s),
                "cooldown_max_s": float(self._cooldown_max_s),
                "failure_streak_threshold": int(self._failure_streak_threshold),
                "providers": provider_rows,
                "provider_count": len(provider_rows),
                "history": history,
                "history_count": len(history),
                "state_path": str(self._state_path),
            }

    def reset(self, *, provider: str = "") -> Dict[str, Any]:
        clean_provider = str(provider or "").strip().lower()
        with self._lock:
            if clean_provider:
                existed = clean_provider in self._provider_state
                self._provider_state.pop(clean_provider, None)
                self._mark_dirty_and_maybe_persist_locked(force=True)
                return {"status": "success", "provider": clean_provider, "removed": bool(existed)}
            provider_count = len(self._provider_state)
            history_count = len(self._decision_history)
            self._provider_state = {}
            self._decision_history = []
            self._mark_dirty_and_maybe_persist_locked(force=True)
            return {"status": "success", "provider_count": provider_count, "history_count": history_count}

    def _provider_state_row(self, provider: str) -> Dict[str, Any]:
        row = self._provider_state.get(provider)
        if isinstance(row, dict):
            return row
        row = {
            "provider": provider,
            "attempts": 0,
            "success": 0,
            "error": 0,
            "failure_streak": 0,
            "failure_ema": 0.0,
            "latency_ema_ms": 0.0,
            "last_latency_ms": 0.0,
            "cooldown_until_epoch": 0.0,
            "last_error": "",
            "last_success_at": 0.0,
            "last_failure_at": 0.0,
            "updated_at": 0.0,
        }
        self._provider_state[provider] = row
        return row

    @staticmethod
    def _provider_ready(*, provider: str, provider_diag: Dict[str, Any]) -> bool:
        if provider == "local":
            return True
        if not isinstance(provider_diag, dict):
            return False
        ready = provider_diag.get("ready")
        if ready is None:
            return bool(provider_diag.get("present", False))
        return bool(ready)

    @staticmethod
    def _diag_failure_ema(provider_diag: Dict[str, Any]) -> float:
        if not isinstance(provider_diag, dict):
            return 0.0
        for key in ("failure_ema", "failure_rate_ema"):
            if key in provider_diag:
                try:
                    return max(0.0, min(float(provider_diag.get(key, 0.0) or 0.0), 1.0))
                except Exception:
                    continue
        return 0.0

    @staticmethod
    def _build_provider_affinity(scored_rows: List[Dict[str, Any]]) -> Dict[str, float]:
        if not scored_rows:
            return {}
        max_score = max(float(row.get("score", 0.0) or 0.0) for row in scored_rows)
        min_score = min(float(row.get("score", 0.0) or 0.0) for row in scored_rows)
        span = max(0.0001, max_score - min_score)
        affinity: Dict[str, float] = {}
        for row in scored_rows:
            provider = str(row.get("provider", "")).strip().lower()
            if not provider:
                continue
            normalized = ((float(row.get("score", 0.0) or 0.0) - min_score) / span) * 2.0 - 1.0
            affinity[provider] = round(max(-1.0, min(normalized, 1.0)), 6)
        return affinity

    @staticmethod
    def _pick_model_name(*, candidates: List[Any], provider: str, privacy_mode: bool) -> str:
        clean_provider = str(provider or "").strip().lower()
        provider_candidates = [row for row in candidates if str(getattr(row, "provider", "")).strip().lower() == clean_provider]
        if not provider_candidates:
            provider_candidates = list(candidates)
        provider_candidates.sort(
            key=lambda item: (
                -int(getattr(item, "quality", 0) or 0),
                float(getattr(item, "latency", 0.0) or 0.0),
                -int(getattr(item, "privacy", 0) or 0),
            )
        )
        if privacy_mode:
            provider_candidates.sort(
                key=lambda item: (
                    -int(getattr(item, "privacy", 0) or 0),
                    -int(getattr(item, "quality", 0) or 0),
                    float(getattr(item, "latency", 0.0) or 0.0),
                )
            )
        best = provider_candidates[0] if provider_candidates else None
        return str(getattr(best, "name", "")).strip().lower() if best is not None else ""

    def _load_state(self) -> None:
        path = self._state_path
        try:
            if not path.exists():
                return
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return
        except Exception:
            return
        with self._lock:
            policy = payload.get("policy", {})
            if isinstance(policy, dict):
                for key, value in policy.items():
                    if key in self._policy:
                        try:
                            self._policy[key] = max(0.05, min(float(value), 8.0))
                        except Exception:
                            continue
            providers = payload.get("providers", {})
            if isinstance(providers, dict):
                self._provider_state = {
                    str(key).strip().lower(): dict(value)
                    for key, value in providers.items()
                    if str(key).strip() and isinstance(value, dict)
                }
            history = payload.get("history", [])
            if isinstance(history, list):
                self._decision_history = [dict(row) for row in history if isinstance(row, dict)][-self._history_max :]

    def _mark_dirty_and_maybe_persist_locked(self, *, force: bool = False) -> None:
        self._dirty_updates += 1
        if not force and self._dirty_updates < self._persist_every:
            return
        self._dirty_updates = 0
        self._persist_locked()

    def _persist_locked(self) -> None:
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "policy": dict(self._policy),
                "providers": {key: dict(value) for key, value in self._provider_state.items()},
                "history": [dict(row) for row in self._decision_history[-self._history_max :]],
                "updated_at": time.time(),
            }
            self._state_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        except Exception:
            return
