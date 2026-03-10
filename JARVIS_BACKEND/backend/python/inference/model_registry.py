from __future__ import annotations

import os
import re
import threading
import time
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from backend.python.core.provider_credentials import ProviderCredentialManager


_MODEL_EXTENSIONS = {
    ".gguf",
    ".onnx",
    ".pt",
    ".pth",
    ".bin",
    ".safetensors",
    ".tflite",
    ".ppn",
    ".ckpt",
}
_DIRECTORY_MODEL_MARKERS = {
    "config.json",
    "pytorch_model.bin",
    "model.safetensors",
    "model.safetensors.index.json",
    "model.bin",
    "flax_model.msgpack",
    "tf_model.h5",
}


@dataclass(slots=True)
class ModelProfile:
    name: str
    task: str
    provider: str
    quality: int = 70
    latency: float = 120.0
    privacy: int = 70
    available: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.name = str(self.name or "").strip().lower()
        self.task = str(self.task or "").strip().lower()
        self.provider = str(self.provider or "").strip().lower()
        self.quality = int(max(0, min(int(self.quality), 100)))
        self.latency = float(max(1.0, min(float(self.latency), 120_000.0)))
        self.privacy = int(max(0, min(int(self.privacy), 100)))
        self.available = bool(self.available)
        self.metadata = dict(self.metadata or {})


class ModelRegistry:
    """
    Adaptive model registry with:
    - runtime model/provider reliability tracking
    - local model inventory scanning
    - cloud provider credential readiness diagnostics
    """

    _LOCAL_PROVIDERS = {"local", "on_device", "offline"}
    _DEFAULT_SCAN_SPECS = (
        ("stt", "stt"),
        ("tts", "tts"),
        ("embedding", "embeddings"),
        ("intent", "custom_intents"),
        ("reasoning", "reasoning"),
        ("reasoning", "all_rounder"),
        ("reasoning", "all-rounder"),
        ("wakeword", "wakeword"),
        ("vision", "JARVIS_BACKEND/models/vision"),
        ("auto", "models"),
        ("auto", "JARVIS_BACKEND/models"),
    )

    def __init__(
        self,
        *,
        provider_credentials: Optional[ProviderCredentialManager] = None,
        enforce_provider_keys: Optional[bool] = None,
        scan_local_models: Optional[bool] = None,
        refresh_interval_s: float = 30.0,
    ) -> None:
        self._lock = threading.RLock()
        self._profiles: Dict[str, ModelProfile] = {}
        self._runtime: Dict[str, Dict[str, Any]] = {}
        self._provider_runtime: Dict[str, Dict[str, Any]] = {}
        self._inventory_profiles: set[str] = set()
        self._inventory_rows: List[Dict[str, Any]] = []
        self._last_refresh_at = 0.0
        self._refresh_interval_s = max(2.0, min(float(refresh_interval_s), 600.0))

        self._provider_credentials = provider_credentials or ProviderCredentialManager()
        self._provider_status: Dict[str, Dict[str, Any]] = {}
        self._enforce_provider_keys = bool(enforce_provider_keys) if enforce_provider_keys is not None else False
        self._scan_local_models = (
            self._env_bool("JARVIS_SCAN_LOCAL_MODELS", True)
            if scan_local_models is None
            else bool(scan_local_models)
        )
        self._max_scanned_files = max(100, min(self._env_int("JARVIS_LOCAL_MODEL_SCAN_MAX_FILES", 2500), 20_000))
        self._max_scan_depth = max(1, min(self._env_int("JARVIS_LOCAL_MODEL_SCAN_MAX_DEPTH", 3), 8))
        self._custom_model_roots = self._parse_custom_scan_roots()

        self._register_defaults()
        self.refresh_environment(force=True)

    def register(
        self,
        name: str,
        *,
        task: str,
        provider: str,
        quality: int = 70,
        latency: float = 120.0,
        privacy: int = 70,
        available: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ModelProfile:
        profile = ModelProfile(
            name=name,
            task=task,
            provider=provider,
            quality=quality,
            latency=latency,
            privacy=privacy,
            available=available,
            metadata=dict(metadata or {}),
        )
        if not profile.name or not profile.task or not profile.provider:
            raise ValueError("name, task, and provider are required")
        with self._lock:
            self._profiles[profile.name] = profile
            runtime = self._ensure_runtime_locked(profile.name)
            if float(runtime.get("latency_ema_ms", 0.0) or 0.0) <= 0.0:
                runtime["latency_ema_ms"] = float(profile.latency)
            if float(runtime.get("quality_ema", 0.0) or 0.0) <= 0.0:
                runtime["quality_ema"] = float(profile.quality) / 100.0
            self._ensure_provider_runtime_locked(profile.provider)
        return profile

    def get(self, name: str) -> Optional[ModelProfile]:
        key = str(name or "").strip().lower()
        if not key:
            return None
        with self._lock:
            profile = self._profiles.get(key)
            return deepcopy(profile) if isinstance(profile, ModelProfile) else None

    def list_by_task(self, task: str) -> List[ModelProfile]:
        self.refresh_environment(force=False)
        clean_task = str(task or "").strip().lower()
        if not clean_task:
            return []
        with self._lock:
            rows: List[ModelProfile] = []
            for profile in self._profiles.values():
                if profile.task != clean_task:
                    continue
                if not profile.available:
                    continue
                if self._enforce_provider_keys and profile.provider not in self._LOCAL_PROVIDERS:
                    status = self._provider_status.get(profile.provider, {})
                    if not bool(status.get("ready", False)):
                        continue
                rows.append(deepcopy(profile))
        rows.sort(key=lambda item: (-item.quality, item.latency, -item.privacy, item.name))
        return rows

    def mark_availability(self, model_name: str, *, available: bool) -> Dict[str, Any]:
        key = str(model_name or "").strip().lower()
        if not key:
            return {"status": "error", "message": "model_name is required"}
        with self._lock:
            profile = self._profiles.get(key)
            if not isinstance(profile, ModelProfile):
                return {"status": "error", "message": f"model not found: {key}"}
            profile.available = bool(available)
        return {"status": "success", "model": key, "available": bool(available)}

    def note_result(
        self,
        model_name: str,
        *,
        success: bool,
        latency_ms: Optional[float] = None,
        quality: Optional[float] = None,
    ) -> Dict[str, Any]:
        key = str(model_name or "").strip().lower()
        if not key:
            return {"status": "error", "message": "model_name is required"}
        now = time.time()
        with self._lock:
            profile = self._profiles.get(key)
            if not isinstance(profile, ModelProfile):
                return {"status": "error", "message": f"model not found: {key}"}

            model_runtime = self._ensure_runtime_locked(key)
            provider_runtime = self._ensure_provider_runtime_locked(profile.provider)
            alpha = 0.24
            sample_failure = 0.0 if success else 1.0
            model_runtime["attempts"] = int(model_runtime.get("attempts", 0) or 0) + 1
            provider_runtime["attempts"] = int(provider_runtime.get("attempts", 0) or 0) + 1
            if success:
                model_runtime["success"] = int(model_runtime.get("success", 0) or 0) + 1
                model_runtime["failure_streak"] = 0
                provider_runtime["success"] = int(provider_runtime.get("success", 0) or 0) + 1
                provider_runtime["failure_streak"] = 0
                model_runtime["last_success_at"] = now
                provider_runtime["last_success_at"] = now
                if float(provider_runtime.get("outage_until_epoch", 0.0) or 0.0) > 0.0:
                    provider_runtime["outage_until_epoch"] = 0.0
                    provider_runtime["outage_penalty"] = max(
                        0.0,
                        float(provider_runtime.get("outage_penalty", 0.0) or 0.0) * 0.6,
                    )
            else:
                model_runtime["error"] = int(model_runtime.get("error", 0) or 0) + 1
                model_runtime["failure_streak"] = int(model_runtime.get("failure_streak", 0) or 0) + 1
                provider_runtime["error"] = int(provider_runtime.get("error", 0) or 0) + 1
                provider_runtime["failure_streak"] = int(provider_runtime.get("failure_streak", 0) or 0) + 1
                model_runtime["last_failure_at"] = now
                provider_runtime["last_failure_at"] = now

            model_runtime["failure_ema"] = self._ema(
                previous=float(model_runtime.get("failure_ema", 0.0) or 0.0),
                sample=sample_failure,
                alpha=alpha,
            )
            provider_runtime["failure_ema"] = self._ema(
                previous=float(provider_runtime.get("failure_ema", 0.0) or 0.0),
                sample=sample_failure,
                alpha=alpha,
            )

            if latency_ms is not None:
                bounded_latency = max(1.0, min(float(latency_ms), 120_000.0))
                model_runtime["latency_ema_ms"] = self._ema(
                    previous=float(model_runtime.get("latency_ema_ms", profile.latency) or profile.latency),
                    sample=bounded_latency,
                    alpha=0.21,
                )
                provider_runtime["latency_ema_ms"] = self._ema(
                    previous=float(provider_runtime.get("latency_ema_ms", bounded_latency) or bounded_latency),
                    sample=bounded_latency,
                    alpha=0.21,
                )
                model_runtime["last_latency_ms"] = bounded_latency
                provider_runtime["last_latency_ms"] = bounded_latency

            if quality is not None:
                bounded_quality = max(0.0, min(float(quality), 1.0))
                model_runtime["quality_ema"] = self._ema(
                    previous=float(model_runtime.get("quality_ema", profile.quality / 100.0) or 0.0),
                    sample=bounded_quality,
                    alpha=0.18,
                )

            model_runtime["updated_at"] = now
            provider_runtime["updated_at"] = now

            return {
                "status": "success",
                "model": key,
                "provider": profile.provider,
                "success": bool(success),
                "failure_ema": round(float(model_runtime.get("failure_ema", 0.0) or 0.0), 6),
            }

    def mark_outage(self, *, provider: str, penalty: float = 0.72, cooldown_s: float = 90.0) -> Dict[str, Any]:
        clean_provider = str(provider or "").strip().lower()
        if not clean_provider:
            return {"status": "error", "message": "provider is required"}
        with self._lock:
            row = self._ensure_provider_runtime_locked(clean_provider)
            now = time.time()
            bounded_penalty = max(0.0, min(float(penalty), 2.0))
            bounded_cooldown = max(5.0, min(float(cooldown_s), 86_400.0))
            row["outage_until_epoch"] = now + bounded_cooldown
            row["outage_penalty"] = max(bounded_penalty, float(row.get("outage_penalty", 0.0) or 0.0))
            row["failure_ema"] = max(float(row.get("failure_ema", 0.0) or 0.0), 0.82)
            row["updated_at"] = now
        return {
            "status": "success",
            "provider": clean_provider,
            "outage_until_epoch": row["outage_until_epoch"],
            "penalty": row["outage_penalty"],
        }

    def model_penalty(self, model_name: str) -> float:
        key = str(model_name or "").strip().lower()
        if not key:
            return 0.0
        now = time.time()
        with self._lock:
            profile = self._profiles.get(key)
            model_runtime = self._runtime.get(key, {})
            provider_runtime = self._provider_runtime.get(str(profile.provider if profile else ""), {})
            profile_provider = str(profile.provider if profile else "").strip().lower()
            provider_status = self._provider_status.get(profile_provider, {})

            penalty = 0.0
            failure_ema = max(0.0, min(float(model_runtime.get("failure_ema", 0.0) or 0.0), 1.0))
            failure_streak = max(0, int(model_runtime.get("failure_streak", 0) or 0))
            penalty += (failure_ema * 0.78) + min(0.26, failure_streak / 12.0)

            provider_failure = max(0.0, min(float(provider_runtime.get("failure_ema", 0.0) or 0.0), 1.0))
            provider_penalty = provider_failure * 0.74
            outage_until = float(provider_runtime.get("outage_until_epoch", 0.0) or 0.0)
            if outage_until > now:
                provider_penalty += max(0.25, float(provider_runtime.get("outage_penalty", 0.0) or 0.0))
            penalty += provider_penalty

            if (
                self._enforce_provider_keys
                and profile_provider
                and profile_provider not in self._LOCAL_PROVIDERS
                and not bool(provider_status.get("ready", False))
            ):
                penalty += 0.65

            return round(max(0.0, min(penalty, 3.0)), 6)

    def refresh_environment(self, *, force: bool = False) -> Dict[str, Any]:
        now_monotonic = time.monotonic()
        if not force and (now_monotonic - self._last_refresh_at) < self._refresh_interval_s:
            return {"status": "success", "refreshed": False}

        credential_snapshot = self._provider_credentials.refresh(overwrite_env=False)
        providers_payload = credential_snapshot.get("providers", {})
        provider_status = providers_payload if isinstance(providers_payload, dict) else {}
        inventory_rows = self._scan_local_inventory() if self._scan_local_models else []

        with self._lock:
            self._provider_status = {
                str(name).strip().lower(): dict(row)
                for name, row in provider_status.items()
                if isinstance(row, dict)
            }
            if self._scan_local_models:
                self._sync_local_profiles_locked(inventory_rows)
            self._apply_provider_availability_locked()
            self._last_refresh_at = now_monotonic

        return {
            "status": "success",
            "refreshed": True,
            "provider_count": len(self._provider_status),
            "inventory_count": len(inventory_rows),
        }

    def provider_status_snapshot(self) -> Dict[str, Any]:
        self.refresh_environment(force=False)
        with self._lock:
            return {name: dict(row) for name, row in self._provider_status.items()}

    def local_inventory_snapshot(self, *, task: str = "", limit: int = 200) -> Dict[str, Any]:
        self.refresh_environment(force=False)
        clean_task = str(task or "").strip().lower()
        bounded_limit = max(1, min(int(limit), 5000))
        with self._lock:
            rows = list(self._inventory_rows)
        if clean_task:
            rows = [row for row in rows if str(row.get("task", "")).strip().lower() == clean_task]
        return {
            "status": "success",
            "count": len(rows),
            "items": rows[:bounded_limit],
            "task": clean_task,
        }

    def capability_summary(self, *, limit_per_task: int = 4) -> Dict[str, Any]:
        self.refresh_environment(force=False)
        bounded_limit = max(1, min(int(limit_per_task), 20))
        with self._lock:
            profiles = [deepcopy(profile) for profile in self._profiles.values()]
            inventory_rows = [dict(row) for row in self._inventory_rows]
            provider_status = {str(name): dict(row) for name, row in self._provider_status.items()}

        task_names = {
            str(profile.task).strip().lower()
            for profile in profiles
            if str(profile.task).strip()
        }
        task_names.update(
            str(row.get("task", "")).strip().lower()
            for row in inventory_rows
            if str(row.get("task", "")).strip()
        )

        items: List[Dict[str, Any]] = []
        for task_name in sorted(task_names):
            task_profiles = [profile for profile in profiles if profile.task == task_name]
            task_profiles.sort(key=lambda item: (-item.quality, item.latency, -item.privacy, item.name))
            inventory_for_task = [row for row in inventory_rows if str(row.get("task", "")).strip().lower() == task_name]

            provider_counts: Dict[str, int] = {}
            available_count = 0
            local_paths: List[str] = []
            top_models: List[Dict[str, Any]] = []
            for profile in task_profiles:
                provider_counts[profile.provider] = int(provider_counts.get(profile.provider, 0)) + 1
                if profile.available:
                    available_count += 1
                metadata = dict(profile.metadata or {})
                path = str(metadata.get("path", "")).strip()
                if profile.provider in self._LOCAL_PROVIDERS and path and path not in local_paths:
                    local_paths.append(path)
                if len(top_models) < bounded_limit:
                    top_models.append(
                        {
                            "name": profile.name,
                            "provider": profile.provider,
                            "quality": int(profile.quality),
                            "latency": float(profile.latency),
                            "privacy": int(profile.privacy),
                            "available": bool(profile.available),
                            "metadata": metadata,
                        }
                    )

            items.append(
                {
                    "task": task_name,
                    "profile_count": len(task_profiles),
                    "available_count": available_count,
                    "inventory_count": len(inventory_for_task),
                    "providers": provider_counts,
                    "local_paths": local_paths[:bounded_limit],
                    "top_models": top_models,
                }
            )

        return {
            "status": "success",
            "task_count": len(items),
            "provider_count": len(provider_status),
            "providers": provider_status,
            "tasks": items,
        }

    def runtime_snapshot(self, *, task: str = "", limit: int = 200) -> Dict[str, Any]:
        self.refresh_environment(force=False)
        clean_task = str(task or "").strip().lower()
        bounded_limit = max(1, min(int(limit), 5000))

        with self._lock:
            rows: List[Dict[str, Any]] = []
            for profile in self._profiles.values():
                if clean_task and profile.task != clean_task:
                    continue
                runtime = self._runtime.get(profile.name, {})
                provider_runtime = self._provider_runtime.get(profile.provider, {})
                penalty = self.model_penalty(profile.name)
                rows.append(
                    {
                        "name": profile.name,
                        "task": profile.task,
                        "provider": profile.provider,
                        "available": bool(profile.available),
                        "quality": int(profile.quality),
                        "latency": float(profile.latency),
                        "privacy": int(profile.privacy),
                        "penalty": penalty,
                        "runtime": {
                            "attempts": int(runtime.get("attempts", 0) or 0),
                            "success": int(runtime.get("success", 0) or 0),
                            "error": int(runtime.get("error", 0) or 0),
                            "failure_ema": float(runtime.get("failure_ema", 0.0) or 0.0),
                            "failure_streak": int(runtime.get("failure_streak", 0) or 0),
                            "latency_ema_ms": float(runtime.get("latency_ema_ms", profile.latency) or profile.latency),
                            "quality_ema": float(runtime.get("quality_ema", profile.quality / 100.0) or 0.0),
                            "updated_at": float(runtime.get("updated_at", 0.0) or 0.0),
                        },
                        "provider_runtime": {
                            "attempts": int(provider_runtime.get("attempts", 0) or 0),
                            "success": int(provider_runtime.get("success", 0) or 0),
                            "error": int(provider_runtime.get("error", 0) or 0),
                            "failure_ema": float(provider_runtime.get("failure_ema", 0.0) or 0.0),
                            "failure_streak": int(provider_runtime.get("failure_streak", 0) or 0),
                            "outage_until_epoch": float(provider_runtime.get("outage_until_epoch", 0.0) or 0.0),
                            "outage_penalty": float(provider_runtime.get("outage_penalty", 0.0) or 0.0),
                            "updated_at": float(provider_runtime.get("updated_at", 0.0) or 0.0),
                        },
                        "provider_ready": bool(self._provider_status.get(profile.provider, {}).get("ready", True)),
                        "metadata": dict(profile.metadata or {}),
                    }
                )

            rows.sort(key=lambda row: (str(row.get("task", "")), float(row.get("penalty", 0.0)), -int(row.get("quality", 0))))
            providers = {name: dict(payload) for name, payload in self._provider_status.items()}

            return {
                "status": "success",
                "count": len(rows),
                "task": clean_task,
                "enforce_provider_keys": bool(self._enforce_provider_keys),
                "provider_status": providers,
                "local_inventory_count": len(self._inventory_rows),
                "items": rows[:bounded_limit],
            }

    def _register_defaults(self) -> None:
        defaults = [
            ModelProfile("groq-llm", "reasoning", "groq", quality=90, latency=200, privacy=46, metadata={"cost_units": 0.24}),
            ModelProfile("nvidia-nim", "reasoning", "nvidia", quality=92, latency=220, privacy=52, metadata={"cost_units": 0.30}),
            ModelProfile("local-llm", "reasoning", "local", quality=58, latency=180, privacy=82, metadata={"cost_units": 0.06}),
            ModelProfile("local-whisper", "stt", "local", quality=88, latency=72, privacy=98, metadata={"cost_units": 0.05}),
            ModelProfile("groq-whisper", "stt", "groq", quality=85, latency=42, privacy=55, metadata={"cost_units": 0.18}),
            ModelProfile("local-tts", "tts", "local", quality=80, latency=66, privacy=98, metadata={"cost_units": 0.04}),
            ModelProfile("elevenlabs-tts", "tts", "elevenlabs", quality=93, latency=44, privacy=42, metadata={"cost_units": 0.36}),
            ModelProfile("wakeword-local", "wakeword", "local", quality=95, latency=8, privacy=99, metadata={"cost_units": 0.01}),
            ModelProfile("local-embedding", "embedding", "local", quality=90, latency=28, privacy=99, metadata={"cost_units": 0.03}),
            ModelProfile("nvidia-embed-qa", "embedding", "nvidia", quality=88, latency=56, privacy=52, metadata={"cost_units": 0.12}),
            ModelProfile("local-intent", "intent", "local", quality=87, latency=26, privacy=99, metadata={"cost_units": 0.02}),
            ModelProfile("local-vision", "vision", "local", quality=86, latency=92, privacy=96, metadata={"cost_units": 0.08}),
            ModelProfile("nvidia-vision", "vision", "nvidia", quality=89, latency=84, privacy=50, metadata={"cost_units": 0.20}),
        ]
        for profile in defaults:
            self.register(
                profile.name,
                task=profile.task,
                provider=profile.provider,
                quality=profile.quality,
                latency=profile.latency,
                privacy=profile.privacy,
                available=profile.available,
                metadata=dict(profile.metadata),
            )

    def _ensure_runtime_locked(self, model_name: str) -> Dict[str, Any]:
        runtime = self._runtime.get(model_name)
        if isinstance(runtime, dict):
            return runtime
        runtime = {
            "attempts": 0,
            "success": 0,
            "error": 0,
            "failure_ema": 0.0,
            "failure_streak": 0,
            "latency_ema_ms": 0.0,
            "last_latency_ms": 0.0,
            "quality_ema": 0.0,
            "last_success_at": 0.0,
            "last_failure_at": 0.0,
            "updated_at": 0.0,
        }
        self._runtime[model_name] = runtime
        return runtime

    def _ensure_provider_runtime_locked(self, provider: str) -> Dict[str, Any]:
        clean_provider = str(provider or "").strip().lower() or "unknown"
        runtime = self._provider_runtime.get(clean_provider)
        if isinstance(runtime, dict):
            return runtime
        runtime = {
            "attempts": 0,
            "success": 0,
            "error": 0,
            "failure_ema": 0.0,
            "failure_streak": 0,
            "latency_ema_ms": 0.0,
            "last_latency_ms": 0.0,
            "outage_until_epoch": 0.0,
            "outage_penalty": 0.0,
            "last_success_at": 0.0,
            "last_failure_at": 0.0,
            "updated_at": 0.0,
        }
        self._provider_runtime[clean_provider] = runtime
        return runtime

    def _apply_provider_availability_locked(self) -> None:
        for profile in self._profiles.values():
            if profile.provider in self._LOCAL_PROVIDERS:
                continue
            status = self._provider_status.get(profile.provider, {})
            if self._enforce_provider_keys:
                profile.available = bool(status.get("ready", False))

    def _scan_local_inventory(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        seen: set[str] = set()
        scanned_files = 0
        for task, root in self._iter_scan_roots():
            if scanned_files >= self._max_scanned_files:
                break
            if not root.exists() or not root.is_dir():
                continue
            for row in self._scan_root(task=task, root=root):
                key = str(row.get("key", "")).strip().lower()
                if not key or key in seen:
                    continue
                seen.add(key)
                rows.append(row)
                scanned_files += 1
                if scanned_files >= self._max_scanned_files:
                    break
        rows.sort(key=lambda row: (str(row.get("task", "")), -int(row.get("size_bytes", 0) or 0), str(row.get("name", ""))))
        return rows

    def _scan_root(self, *, task: str, root: Path) -> Iterable[Dict[str, Any]]:
        for current_root, dirnames, filenames in os.walk(root):
            current = Path(current_root)
            try:
                depth = len(current.relative_to(root).parts)
            except Exception:
                depth = 0
            if depth > self._max_scan_depth:
                dirnames[:] = []
                continue
            lowered_files = {str(name).strip().lower() for name in filenames}
            if self._is_directory_model(lowered_files):
                yield self._inventory_row(task_hint=task, path=current, source="directory")
                dirnames[:] = []
                continue
            for file_name in filenames:
                file_path = current / file_name
                suffix = file_path.suffix.strip().lower()
                if suffix not in _MODEL_EXTENSIONS:
                    continue
                yield self._inventory_row(task_hint=task, path=file_path, source="file")

    def _sync_local_profiles_locked(self, rows: List[Dict[str, Any]]) -> None:
        live_names: set[str] = set()
        for row in rows:
            name = self._inventory_profile_name(row)
            task = str(row.get("task", "reasoning")).strip().lower() or "reasoning"
            quality, latency, privacy, metadata = self._inventory_profile_heuristics(row)
            live_names.add(name)
            if name in self._profiles:
                profile = self._profiles[name]
                profile.task = task
                profile.provider = "local"
                profile.quality = quality
                profile.latency = latency
                profile.privacy = privacy
                profile.available = True
                profile.metadata = metadata
            else:
                self._profiles[name] = ModelProfile(
                    name=name,
                    task=task,
                    provider="local",
                    quality=quality,
                    latency=latency,
                    privacy=privacy,
                    available=True,
                    metadata=metadata,
                )
            model_runtime = self._ensure_runtime_locked(name)
            if int(model_runtime.get("attempts", 0) or 0) <= 0:
                model_runtime["latency_ema_ms"] = float(latency)
                model_runtime["quality_ema"] = float(quality) / 100.0
            self._ensure_provider_runtime_locked("local")

        stale_names = self._inventory_profiles.difference(live_names)
        for stale in stale_names:
            if stale in self._profiles:
                del self._profiles[stale]
            self._runtime.pop(stale, None)
        self._inventory_profiles = live_names
        self._inventory_rows = list(rows)

    def _inventory_row(self, *, task_hint: str, path: Path, source: str) -> Dict[str, Any]:
        resolved = path.resolve()
        task = self._infer_task(task_hint=task_hint, path=resolved)
        try:
            stat = resolved.stat()
            if resolved.is_file():
                size_bytes = int(stat.st_size)
            else:
                size_bytes = int(sum(f.stat().st_size for f in resolved.glob("*") if f.is_file()))
            modified_epoch = float(stat.st_mtime)
        except Exception:
            size_bytes = 0
            modified_epoch = 0.0
        fmt = resolved.suffix.strip().lower() if resolved.is_file() else "directory"
        key = f"{task}:{str(resolved).lower()}"
        return {
            "key": key,
            "task": task,
            "name": resolved.stem if resolved.is_file() else resolved.name,
            "path": str(resolved),
            "source": source,
            "format": fmt or "directory",
            "size_bytes": max(0, size_bytes),
            "modified_epoch": modified_epoch,
        }

    @staticmethod
    def _inventory_profile_name(row: Dict[str, Any]) -> str:
        task = str(row.get("task", "reasoning")).strip().lower() or "reasoning"
        base = str(row.get("name", "model")).strip().lower() or "model"
        slug = re.sub(r"[^a-z0-9]+", "-", base).strip("-") or "model"
        return f"local-auto-{task}-{slug}"

    @staticmethod
    def _inventory_profile_heuristics(row: Dict[str, Any]) -> tuple[int, float, int, Dict[str, Any]]:
        task = str(row.get("task", "reasoning")).strip().lower()
        name = str(row.get("name", "")).strip().lower()
        size_bytes = max(0, int(row.get("size_bytes", 0) or 0))
        size_gb = float(size_bytes) / float(1024**3) if size_bytes > 0 else 0.0

        quality = 72
        latency = 120.0
        privacy = 95
        if task == "stt":
            quality = 88
            latency = 54.0 + (size_gb * 4.4)
            if "large-v3" in name or "large" in name:
                quality += 6
                latency += 12.0
        elif task == "tts":
            quality = 86
            latency = 34.0 + (size_gb * 3.2)
            if "orpheus" in name or "hifigan" in name:
                quality += 6
                latency += 10.0
        elif task == "wakeword":
            quality = 95
            latency = 7.0 + (size_gb * 0.2)
        elif task == "embedding":
            quality = 91
            latency = 18.0 + (size_gb * 1.8)
        elif task == "intent":
            quality = 89
            latency = 24.0 + (size_gb * 2.2)
        elif task == "vision":
            quality = 84
            latency = 78.0 + (size_gb * 5.6)
            privacy = 92
        else:
            quality = 60
            latency = 220.0 + (size_gb * 8.6)
            privacy = 85

        quality = int(max(20, min(quality, 100)))
        latency = float(max(1.0, min(latency, 120_000.0)))
        metadata = dict(row)
        metadata["cost_units"] = {
            "reasoning": 0.06,
            "stt": 0.05,
            "tts": 0.04,
            "wakeword": 0.01,
            "embedding": 0.03,
            "intent": 0.02,
            "vision": 0.08,
        }.get(task, 0.04)
        metadata["detected"] = True
        metadata["backend"] = ModelRegistry._infer_backend(task=task, row=row)
        metadata["family"] = ModelRegistry._infer_family(task=task, name=name, path=str(row.get("path", "")))
        metadata["modality"] = task
        return quality, latency, privacy, metadata

    @staticmethod
    def _is_directory_model(lowered_files: set[str]) -> bool:
        if not lowered_files:
            return False
        if lowered_files.intersection(_DIRECTORY_MODEL_MARKERS):
            return True
        if "config.json" in lowered_files and any(name.endswith(tuple(_MODEL_EXTENSIONS)) for name in lowered_files):
            return True
        return False

    @staticmethod
    def _infer_task(*, task_hint: str, path: Path) -> str:
        clean_hint = str(task_hint or "").strip().lower()
        if clean_hint and clean_hint not in {"auto", "model"}:
            return clean_hint
        text = str(path).strip().lower()
        if text.endswith(".ppn") or "wakeword" in text:
            return "wakeword"
        if "yolo" in text or "vision" in text or "segment_anything" in text or "sam" in text:
            return "vision"
        if "custom_intents" in text or "bart-large-mnli" in text or "mnli" in text:
            return "intent"
        if "all_rounder" in text or "all-rounder" in text or "qwen3" in text:
            return "reasoning"
        if "/stt/" in text or "\\stt\\" in text or "whisper" in text or "speech-to-text" in text:
            return "stt"
        if "/tts/" in text or "\\tts\\" in text or "hifigan" in text or "orpheus" in text or "voice" in text:
            return "tts"
        if "embed" in text or "mpnet" in text:
            return "embedding"
        return "reasoning"

    @staticmethod
    def _infer_backend(*, task: str, row: Dict[str, Any]) -> str:
        path = str(row.get("path", "")).strip().lower()
        fmt = str(row.get("format", "")).strip().lower()
        if task == "wakeword":
            return "porcupine"
        if fmt == ".gguf":
            return "llama_cpp"
        if fmt == ".onnx":
            return "onnx"
        if task in {"embedding", "intent"} or fmt == "directory":
            return "transformers"
        if task == "vision":
            if path.endswith(".pt") or "yolo" in path:
                return "torch"
            if path.endswith(".pth") or "sam" in path:
                return "segment_anything"
            if path.endswith(".gguf"):
                return "llama_cpp"
        return "local_runtime"

    @staticmethod
    def _infer_family(*, task: str, name: str, path: str) -> str:
        text = f"{name} {path}".strip().lower()
        if "qwen" in text:
            return "qwen"
        if "llama" in text:
            return "llama"
        if "deepseek" in text:
            return "deepseek"
        if "whisper" in text:
            return "whisper"
        if "mpnet" in text:
            return "mpnet"
        if "bart" in text or "mnli" in text:
            return "bart-mnli"
        if "orpheus" in text:
            return "orpheus"
        if "porcupine" in text or text.endswith(".ppn"):
            return "porcupine"
        if "sam" in text:
            return "sam"
        if "yolo" in text:
            return "yolo"
        return task or "unknown"

    def _iter_scan_roots(self) -> Iterable[tuple[str, Path]]:
        yielded: set[str] = set()
        for task, relative in self._DEFAULT_SCAN_SPECS:
            path = self._resolve_candidate_path(relative)
            key = str(path).lower()
            if key in yielded:
                continue
            yielded.add(key)
            yield task, path
        for task, raw_path in self._custom_model_roots:
            path = self._resolve_candidate_path(raw_path)
            key = str(path).lower()
            if key in yielded:
                continue
            yielded.add(key)
            yield task, path

    def _resolve_candidate_path(self, raw: str) -> Path:
        clean = str(raw or "").strip()
        if not clean:
            return Path.cwd()
        candidate = Path(clean)
        if candidate.is_absolute():
            return candidate
        cwd = Path.cwd().resolve()
        for option in (cwd / clean, cwd.parent / clean, cwd.parent.parent / clean):
            if option.exists():
                return option
        return cwd / clean

    def _parse_custom_scan_roots(self) -> List[tuple[str, str]]:
        payload = str(os.getenv("JARVIS_LOCAL_MODEL_ROOTS", "")).strip()
        if not payload:
            return []
        rows: List[tuple[str, str]] = []
        for raw_item in payload.split(";"):
            item = str(raw_item or "").strip()
            if not item:
                continue
            if ":" in item:
                task, path = item.split(":", 1)
                clean_task = str(task or "").strip().lower() or "auto"
                clean_path = str(path or "").strip()
            else:
                clean_task = "auto"
                clean_path = item
            if clean_path:
                rows.append((clean_task, clean_path))
        return rows

    @staticmethod
    def _ema(*, previous: float, sample: float, alpha: float) -> float:
        bounded_alpha = max(0.01, min(float(alpha), 1.0))
        return (previous * (1.0 - bounded_alpha)) + (sample * bounded_alpha)

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        raw = str(os.getenv(name, "1" if default else "0")).strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            return True
        if raw in {"0", "false", "no", "off"}:
            return False
        return bool(default)

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        raw = str(os.getenv(name, str(default))).strip()
        try:
            return int(raw)
        except Exception:
            return int(default)
