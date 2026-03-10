from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from .model_registry import ModelProfile, ModelRegistry


@dataclass(slots=True)
class RouteDecision:
    task: str
    model: str
    provider: str
    reason: str
    score: float = 0.0
    alternatives: list[str] | None = None
    diagnostics: dict[str, object] | None = None


class ModelRouter:
    """
    Chooses the best model profile per task and runtime policy.
    """

    DEFAULT_STACKS: Dict[str, List[str]] = {
        "desktop_agent": ["reasoning", "embedding", "intent", "vision", "wakeword", "stt", "tts"],
        "voice": ["wakeword", "stt", "reasoning", "tts"],
        "memory": ["embedding", "intent", "reasoning"],
        "perception": ["vision", "wakeword", "stt"],
    }
    _PRIVACY_LOCAL_TASKS = {"wakeword", "stt", "tts", "embedding", "intent", "vision"}

    def __init__(self, registry: ModelRegistry | None = None) -> None:
        self.registry = registry or ModelRegistry()

    def choose(
        self,
        task: str,
        *,
        requires_offline: bool = False,
        high_quality: bool = False,
        privacy_mode: bool = False,
        latency_sensitive: bool = False,
        max_latency_ms: Optional[float] = None,
        preferred_provider: str = "",
        banned_providers: Optional[Iterable[str]] = None,
        mission_profile: str = "balanced",
        cost_sensitive: bool = False,
        max_cost_units: Optional[float] = None,
        provider_affinity: Optional[Dict[str, float]] = None,
    ) -> RouteDecision:
        candidates = self.registry.list_by_task(task)
        if not candidates:
            raise RuntimeError(f"No available models for task: {task}")
        banned = {str(item or "").strip().lower() for item in (banned_providers or []) if str(item or "").strip()}
        preferred = str(preferred_provider or "").strip().lower()
        bounded_max_latency = None if max_latency_ms is None else max(1.0, min(float(max_latency_ms), 120_000.0))
        clean_profile = str(mission_profile or "").strip().lower() or "balanced"
        bounded_max_cost = None if max_cost_units is None else max(0.001, min(float(max_cost_units), 10_000.0))
        affinity = self._normalize_provider_affinity(provider_affinity or {})

        ranked_rows: List[Dict[str, object]] = []
        for model in candidates:
            score, details = self._score_advanced(
                model,
                requires_offline=requires_offline,
                high_quality=high_quality,
                privacy_mode=privacy_mode,
                latency_sensitive=latency_sensitive,
                max_latency_ms=bounded_max_latency,
                preferred_provider=preferred,
                banned_providers=banned,
                mission_profile=clean_profile,
                cost_sensitive=bool(cost_sensitive),
                max_cost_units=bounded_max_cost,
                provider_affinity=affinity,
            )
            ranked_rows.append({"model": model, "score": score, "details": details})
        ranked_rows.sort(key=lambda row: float(row["score"]), reverse=True)
        winner_row = ranked_rows[0]
        winner = winner_row["model"]
        details = winner_row["details"] if isinstance(winner_row.get("details"), dict) else {}

        alternatives = [
            str(row["model"].name)
            for row in ranked_rows[1:4]
            if isinstance(row.get("model"), ModelProfile)
        ]
        reason = (
            f"selected={winner.name} score={float(winner_row['score']):.3f} "
            f"provider={winner.provider} offline={requires_offline} privacy_mode={privacy_mode} "
            f"latency_sensitive={latency_sensitive} mission_profile={clean_profile} "
            f"cost_sensitive={bool(cost_sensitive)} details={details}"
        )
        diagnostics = {
            "mission_profile": clean_profile,
            "cost_sensitive": bool(cost_sensitive),
            "max_cost_units": bounded_max_cost,
            "provider_affinity": affinity,
            "winner": {
                "model": winner.name,
                "provider": winner.provider,
                "score": round(float(winner_row["score"]), 6),
                "details": details,
            },
            "top_candidates": [
                {
                    "model": str(row["model"].name),
                    "provider": str(row["model"].provider),
                    "score": round(float(row["score"]), 6),
                    "details": row["details"] if isinstance(row.get("details"), dict) else {},
                }
                for row in ranked_rows[:5]
                if isinstance(row.get("model"), ModelProfile)
            ],
        }
        return RouteDecision(
            task=task,
            model=winner.name,
            provider=winner.provider,
            reason=reason,
            score=float(winner_row["score"]),
            alternatives=alternatives,
            diagnostics=diagnostics,
        )

    def plan_routes(
        self,
        tasks: Iterable[str],
        *,
        requires_offline: bool = False,
        high_quality: bool = False,
        privacy_mode: bool = False,
        latency_sensitive: bool = False,
        max_latency_ms: Optional[float] = None,
        preferred_provider: str = "",
        banned_providers: Optional[Iterable[str]] = None,
        mission_profile: str = "balanced",
        max_provider_share: float = 0.78,
        cost_sensitive: bool = False,
        max_cost_units: Optional[float] = None,
        provider_affinity: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        requested = [str(item or "").strip().lower() for item in tasks if str(item or "").strip()]
        if not requested:
            return {"status": "error", "message": "tasks are required", "count": 0, "items": []}
        hard_banned = {str(item or "").strip().lower() for item in (banned_providers or []) if str(item or "").strip()}
        bounded_share = max(0.45, min(float(max_provider_share), 1.0))
        rows: List[Dict[str, Any]] = []
        provider_counts: Dict[str, int] = {}
        warnings: List[str] = []

        for index, task_name in enumerate(requested, start=1):
            dynamic_banned = set(hard_banned)
            if provider_counts and bounded_share < 1.0:
                total = float(max(1, len(rows)))
                for provider_name, provider_count in provider_counts.items():
                    share = float(provider_count) / total
                    if share >= bounded_share:
                        dynamic_banned.add(provider_name)
            try:
                decision = self.choose(
                    task_name,
                    requires_offline=requires_offline,
                    high_quality=high_quality,
                    privacy_mode=privacy_mode,
                    latency_sensitive=latency_sensitive,
                    max_latency_ms=max_latency_ms,
                    preferred_provider=preferred_provider,
                    banned_providers=dynamic_banned,
                    mission_profile=mission_profile,
                    cost_sensitive=cost_sensitive,
                    max_cost_units=max_cost_units,
                    provider_affinity=provider_affinity,
                )
            except Exception as first_exc:  # noqa: BLE001
                if dynamic_banned != hard_banned:
                    warnings.append(
                        f"task:{task_name}:dynamic_provider_cap_triggered_fallback:{str(first_exc)}"
                    )
                    decision = self.choose(
                        task_name,
                        requires_offline=requires_offline,
                        high_quality=high_quality,
                        privacy_mode=privacy_mode,
                        latency_sensitive=latency_sensitive,
                        max_latency_ms=max_latency_ms,
                        preferred_provider=preferred_provider,
                        banned_providers=hard_banned,
                        mission_profile=mission_profile,
                        cost_sensitive=cost_sensitive,
                        max_cost_units=max_cost_units,
                        provider_affinity=provider_affinity,
                    )
                else:
                    rows.append(
                        {
                            "index": index,
                            "task": task_name,
                            "status": "error",
                            "message": str(first_exc),
                        }
                    )
                    continue

            provider_name = str(decision.provider or "").strip().lower()
            provider_counts[provider_name] = int(provider_counts.get(provider_name, 0)) + 1
            rows.append(
                {
                    "index": index,
                    "status": "success",
                    "task": task_name,
                    "model": decision.model,
                    "provider": decision.provider,
                    "score": round(float(decision.score), 6),
                    "alternatives": list(decision.alternatives or []),
                    "diagnostics": dict(decision.diagnostics or {}),
                }
            )

        success_count = sum(1 for row in rows if str(row.get("status", "")).strip().lower() == "success")
        error_count = sum(1 for row in rows if str(row.get("status", "")).strip().lower() != "success")
        total_count = len(rows)
        provider_distribution = {
            name: round(float(count) / max(1.0, float(success_count)), 6)
            for name, count in provider_counts.items()
            if success_count > 0
        }
        status = "success"
        if success_count <= 0:
            status = "error"
        elif error_count > 0:
            status = "partial"
        return {
            "status": status,
            "count": total_count,
            "success_count": success_count,
            "error_count": error_count,
            "items": rows,
            "provider_distribution": provider_distribution,
            "provider_counts": provider_counts,
            "warnings": warnings[:16],
            "mission_profile": str(mission_profile or "").strip().lower() or "balanced",
        }

    def route_bundle(
        self,
        *,
        stack_name: str = "desktop_agent",
        tasks: Optional[Iterable[str]] = None,
        requires_offline: bool = False,
        high_quality: bool = True,
        privacy_mode: bool = False,
        latency_sensitive: bool = False,
        mission_profile: str = "balanced",
        max_provider_share: float = 0.8,
        cost_sensitive: bool = False,
        max_cost_units: Optional[float] = None,
        preferred_providers: Optional[Dict[str, str]] = None,
        provider_affinity: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        requested_tasks = self._resolve_bundle_tasks(stack_name=stack_name, tasks=tasks)
        if not requested_tasks:
            return {"status": "error", "message": "tasks are required", "stack_name": stack_name, "items": []}

        clean_profile = str(mission_profile or "").strip().lower() or "balanced"
        bounded_share = max(0.45, min(float(max_provider_share), 1.0))
        preferred_map = {
            str(task_name).strip().lower(): str(provider_name or "").strip().lower()
            for task_name, provider_name in (preferred_providers or {}).items()
            if str(task_name).strip()
        }
        base_affinity = self._normalize_provider_affinity(provider_affinity or {})
        provider_counts: Dict[str, int] = {}
        provider_local_path_count: Dict[str, int] = {}
        items: List[Dict[str, Any]] = []
        warnings: List[str] = []

        for index, task_name in enumerate(requested_tasks, start=1):
            dynamic_affinity = dict(base_affinity)
            dominant_provider = self._dominant_provider(provider_counts)
            if dominant_provider:
                dynamic_affinity[dominant_provider] = max(
                    -1.0,
                    min(1.0, float(dynamic_affinity.get(dominant_provider, 0.0) or 0.0) - 0.12),
                )
            if task_name in {"embedding", "vision"} and provider_local_path_count.get("local", 0) > 0:
                dynamic_affinity["local"] = max(-1.0, min(1.0, float(dynamic_affinity.get("local", 0.0) or 0.0) + 0.08))

            dynamic_banned = self._relax_bundle_bans(
                self._bundle_dynamic_bans(
                    provider_counts=provider_counts,
                    item_count=len(items),
                    max_provider_share=bounded_share,
                ),
                task_name=task_name,
                requires_offline=requires_offline,
                privacy_mode=privacy_mode,
            )
            preferred_provider = preferred_map.get(task_name, "")
            if preferred_provider and preferred_provider in dynamic_banned:
                dynamic_banned = [provider for provider in dynamic_banned if provider != preferred_provider]
            if requires_offline:
                dynamic_affinity["local"] = max(-1.0, min(1.0, float(dynamic_affinity.get("local", 0.0) or 0.0) + 0.35))
            elif privacy_mode and task_name in self._PRIVACY_LOCAL_TASKS:
                dynamic_affinity["local"] = max(-1.0, min(1.0, float(dynamic_affinity.get("local", 0.0) or 0.0) + 0.12))
            try:
                decision = self.choose(
                    task_name,
                    requires_offline=requires_offline,
                    high_quality=high_quality,
                    privacy_mode=privacy_mode,
                    latency_sensitive=latency_sensitive,
                    preferred_provider=preferred_provider,
                    banned_providers=dynamic_banned,
                    mission_profile=clean_profile,
                    cost_sensitive=cost_sensitive,
                    max_cost_units=max_cost_units,
                    provider_affinity=dynamic_affinity,
                )
            except Exception as first_exc:  # noqa: BLE001
                if dynamic_banned:
                    warnings.append(f"task:{task_name}:provider_share_fallback:{str(first_exc)}")
                    try:
                        decision = self.choose(
                            task_name,
                            requires_offline=requires_offline,
                            high_quality=high_quality,
                            privacy_mode=privacy_mode,
                            latency_sensitive=latency_sensitive,
                            preferred_provider=preferred_provider,
                            banned_providers=[],
                            mission_profile=clean_profile,
                            cost_sensitive=cost_sensitive,
                            max_cost_units=max_cost_units,
                            provider_affinity=dynamic_affinity,
                        )
                    except Exception as second_exc:  # noqa: BLE001
                        items.append(
                            {
                                "index": index,
                                "task": task_name,
                                "status": "error",
                                "message": str(second_exc),
                            }
                        )
                        continue
                else:
                    items.append(
                        {
                            "index": index,
                            "task": task_name,
                            "status": "error",
                            "message": str(first_exc),
                        }
                    )
                    continue

            provider_name = str(decision.provider or "").strip().lower()
            provider_counts[provider_name] = int(provider_counts.get(provider_name, 0)) + 1
            profile = self.registry.get(decision.model)
            metadata = dict(profile.metadata if profile is not None and isinstance(profile.metadata, dict) else {})
            selected_path = str(metadata.get("path", "")).strip()
            if provider_name == "local" and selected_path:
                provider_local_path_count["local"] = int(provider_local_path_count.get("local", 0)) + 1

            diagnostics = dict(decision.diagnostics or {})
            fallback_chain = self._decision_fallback_chain(diagnostics=diagnostics, selected_provider=provider_name)
            items.append(
                {
                    "index": index,
                    "status": "success",
                    "task": task_name,
                    "model": decision.model,
                    "provider": decision.provider,
                    "score": round(float(decision.score), 6),
                    "fallback_chain": fallback_chain,
                    "alternatives": list(decision.alternatives or []),
                    "selected_path": selected_path,
                    "metadata": metadata,
                    "diagnostics": diagnostics,
                }
            )

        success_count = sum(1 for row in items if str(row.get("status", "")).strip().lower() == "success")
        error_count = len(items) - success_count
        provider_distribution = {
            name: round(float(count) / max(1.0, float(success_count)), 6)
            for name, count in provider_counts.items()
            if success_count > 0
        }
        selected_local_paths = {
            str(row.get("task", "")).strip().lower(): str(row.get("selected_path", "")).strip()
            for row in items
            if str(row.get("status", "")).strip().lower() == "success"
            and str(row.get("provider", "")).strip().lower() == "local"
            and str(row.get("selected_path", "")).strip()
        }
        status = "success"
        if success_count <= 0:
            status = "error"
        elif error_count > 0:
            status = "partial"
        return {
            "status": status,
            "stack_name": str(stack_name or "custom").strip().lower() or "custom",
            "count": len(items),
            "success_count": success_count,
            "error_count": error_count,
            "requires_offline": bool(requires_offline),
            "privacy_mode": bool(privacy_mode),
            "latency_sensitive": bool(latency_sensitive),
            "mission_profile": clean_profile,
            "provider_counts": provider_counts,
            "provider_distribution": provider_distribution,
            "selected_local_paths": selected_local_paths,
            "items": items,
            "warnings": warnings[:16],
            "capabilities": self.registry.capability_summary(limit_per_task=3),
        }

    def _score_advanced(
        self,
        model: ModelProfile,
        *,
        requires_offline: bool,
        high_quality: bool,
        privacy_mode: bool,
        latency_sensitive: bool,
        max_latency_ms: Optional[float],
        preferred_provider: str,
        banned_providers: set[str],
        mission_profile: str,
        cost_sensitive: bool,
        max_cost_units: Optional[float],
        provider_affinity: Dict[str, float],
    ) -> tuple[float, Dict[str, float | str]]:
        score = 0.0
        details: Dict[str, float | str] = {}

        quality_component = float(model.quality if high_quality else model.quality * 0.55)
        latency_component = float(max(0, 100 - model.latency))
        privacy_component = float(model.privacy * (1.2 if privacy_mode else 0.5))
        score += quality_component + latency_component + privacy_component
        details["quality"] = round(quality_component, 3)
        details["latency"] = round(latency_component, 3)
        details["privacy"] = round(privacy_component, 3)

        if requires_offline and model.provider == "local":
            score += 140.0
            details["offline_bonus"] = 140.0
        if requires_offline and model.provider != "local":
            score -= 180.0
            details["offline_penalty"] = -180.0

        if latency_sensitive:
            speed_bonus = max(0.0, 90.0 - (float(model.latency) * 1.2))
            score += speed_bonus
            details["latency_sensitive_bonus"] = round(speed_bonus, 3)

        if max_latency_ms is not None and float(model.latency) > max_latency_ms:
            latency_penalty = min(120.0, float(model.latency) - max_latency_ms)
            score -= latency_penalty
            details["max_latency_penalty"] = round(-latency_penalty, 3)

        if preferred_provider and model.provider == preferred_provider:
            score += 18.0
            details["preferred_provider_bonus"] = 18.0

        if model.provider in banned_providers:
            score -= 500.0
            details["banned_provider_penalty"] = -500.0

        adaptive_penalty = float(self.registry.model_penalty(model.name))
        penalty_delta = adaptive_penalty * 140.0
        score -= penalty_delta
        details["adaptive_penalty"] = round(-penalty_delta, 3)

        mission_adjust, mission_details = self._mission_profile_adjustment(
            model,
            mission_profile=mission_profile,
            high_quality=high_quality,
            privacy_mode=privacy_mode,
            latency_sensitive=latency_sensitive,
        )
        score += mission_adjust
        details.update(mission_details)

        provider_affinity_bonus = float(provider_affinity.get(model.provider, 0.0) or 0.0) * 30.0
        if provider_affinity_bonus != 0.0:
            score += provider_affinity_bonus
            details["provider_affinity_bonus"] = round(provider_affinity_bonus, 3)

        cost_units = self._model_cost_units(model)
        details["cost_units"] = round(cost_units, 6)
        if cost_sensitive:
            cost_penalty = min(120.0, max(0.0, cost_units) * 60.0)
            score -= cost_penalty
            details["cost_penalty"] = round(-cost_penalty, 3)
        if max_cost_units is not None and cost_units > max_cost_units:
            overrun = max(0.0, cost_units - max_cost_units)
            overrun_penalty = min(260.0, 80.0 + (overrun * 160.0))
            score -= overrun_penalty
            details["max_cost_penalty"] = round(-overrun_penalty, 3)

        runtime = self.registry._runtime.get(model.name, {})  # noqa: SLF001
        latency_ema = float(runtime.get("latency_ema_ms", model.latency) or model.latency)
        if latency_ema > 0:
            latency_ema_penalty = min(80.0, max(0.0, (latency_ema - float(model.latency)) * 0.45))
            score -= latency_ema_penalty
            details["latency_drift_penalty"] = round(-latency_ema_penalty, 3)

        quality_ema = float(runtime.get("quality_ema", float(model.quality) / 100.0) or 0.0)
        quality_delta = (quality_ema - (float(model.quality) / 100.0)) * 55.0
        score += quality_delta
        details["quality_drift_adjustment"] = round(quality_delta, 3)

        provider_runtime = self.registry._provider_runtime.get(model.provider, {})  # noqa: SLF001
        provider_failure = max(0.0, min(float(provider_runtime.get("failure_ema", 0.0) or 0.0), 1.0))
        provider_reliability = 1.0 - provider_failure
        provider_reliability_adjust = (provider_reliability - 0.5) * 36.0
        score += provider_reliability_adjust
        details["provider_reliability_adjustment"] = round(provider_reliability_adjust, 3)

        details["provider"] = model.provider
        return score, details

    @staticmethod
    def _normalize_provider_affinity(payload: Dict[str, float]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for key, value in payload.items():
            provider = str(key or "").strip().lower()
            if not provider:
                continue
            try:
                out[provider] = max(-1.0, min(float(value), 1.0))
            except Exception:
                continue
        return out

    @classmethod
    def _resolve_bundle_tasks(cls, *, stack_name: str, tasks: Optional[Iterable[str]]) -> List[str]:
        requested = [str(item or "").strip().lower() for item in (tasks or []) if str(item or "").strip()]
        if requested:
            return requested
        clean_stack = str(stack_name or "").strip().lower() or "desktop_agent"
        defaults = cls.DEFAULT_STACKS.get(clean_stack, cls.DEFAULT_STACKS.get("desktop_agent", []))
        return [str(item).strip().lower() for item in defaults if str(item).strip()]

    @staticmethod
    def _dominant_provider(provider_counts: Dict[str, int]) -> str:
        if not provider_counts:
            return ""
        return max(provider_counts.items(), key=lambda item: (int(item[1]), str(item[0])))[0]

    @staticmethod
    def _bundle_dynamic_bans(*, provider_counts: Dict[str, int], item_count: int, max_provider_share: float) -> List[str]:
        if not provider_counts or item_count <= 0:
            return []
        dynamic_banned: List[str] = []
        total = float(max(1, item_count))
        for provider_name, provider_count in provider_counts.items():
            share = float(provider_count) / total
            if share >= max_provider_share:
                dynamic_banned.append(str(provider_name))
        return dynamic_banned

    @classmethod
    def _relax_bundle_bans(
        cls,
        dynamic_banned: List[str],
        *,
        task_name: str,
        requires_offline: bool,
        privacy_mode: bool,
    ) -> List[str]:
        if not dynamic_banned:
            return []
        clean_task = str(task_name or "").strip().lower()
        protected: set[str] = set()
        if requires_offline:
            protected.add("local")
        if clean_task == "wakeword":
            protected.add("local")
        if privacy_mode and clean_task in cls._PRIVACY_LOCAL_TASKS:
            protected.add("local")
        if not protected:
            return dynamic_banned
        return [provider for provider in dynamic_banned if str(provider or "").strip().lower() not in protected]

    @staticmethod
    def _decision_fallback_chain(*, diagnostics: Dict[str, Any], selected_provider: str) -> List[str]:
        rows = diagnostics.get("top_candidates", []) if isinstance(diagnostics.get("top_candidates", []), list) else []
        chain: List[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            provider_name = str(row.get("provider", "")).strip().lower()
            if not provider_name or provider_name == selected_provider or provider_name in chain:
                continue
            chain.append(provider_name)
        return chain

    @staticmethod
    def _model_cost_units(model: ModelProfile) -> float:
        metadata = model.metadata if isinstance(model.metadata, dict) else {}
        explicit = metadata.get("cost_units")
        if explicit is not None:
            try:
                return max(0.0, float(explicit))
            except Exception:
                pass
        provider_defaults = {
            "local": 0.08,
            "groq": 0.24,
            "nvidia": 0.3,
            "elevenlabs": 0.36,
        }
        return float(provider_defaults.get(str(model.provider or "").strip().lower(), 0.2))

    @classmethod
    def _mission_profile_adjustment(
        cls,
        model: ModelProfile,
        *,
        mission_profile: str,
        high_quality: bool,
        privacy_mode: bool,
        latency_sensitive: bool,
    ) -> tuple[float, Dict[str, float | str]]:
        profile = str(mission_profile or "").strip().lower() or "balanced"
        score = 0.0
        details: Dict[str, float | str] = {"mission_profile": profile}
        provider = str(model.provider or "").strip().lower()

        if profile in {"stability", "safe", "automation_safe"}:
            if provider == "local":
                score += 24.0
            score += max(0.0, 26.0 - (float(model.latency) * 0.35))
            score += (float(model.privacy) / 100.0) * 12.0
        elif profile in {"throughput", "speed", "automation_power"}:
            score += max(0.0, 70.0 - float(model.latency)) * 0.75
            score += (float(model.quality) / 100.0) * 10.0
        elif profile in {"privacy", "locked_down"}:
            score += (float(model.privacy) / 100.0) * 34.0
            if provider == "local":
                score += 26.0
            else:
                score -= 8.0
        elif profile in {"quality", "analysis"}:
            score += (float(model.quality) / 100.0) * 28.0
            if float(model.latency) <= 45.0:
                score += 6.0
        else:
            score += (float(model.quality) / 100.0) * 8.0
            score += (float(model.privacy) / 100.0) * 4.0

        if high_quality:
            score += (float(model.quality) / 100.0) * 6.0
        if privacy_mode:
            score += (float(model.privacy) / 100.0) * 7.0
        if latency_sensitive:
            score += max(0.0, 30.0 - (float(model.latency) * 0.25))

        details["mission_profile_adjustment"] = round(score, 3)
        return score, details

    @staticmethod
    def _score(
        model: ModelProfile,
        requires_offline: bool,
        high_quality: bool,
        privacy_mode: bool,
    ) -> int:
        # Backward compatibility shim for legacy callers/tests using the old signature.
        score = 0
        score += model.quality if high_quality else model.quality // 2
        score += max(0, 100 - model.latency)
        score += model.privacy // 2
        if requires_offline and model.provider == "local":
            score += 100
        if requires_offline and model.provider != "local":
            score -= 120
        if privacy_mode:
            score += model.privacy
        return score
