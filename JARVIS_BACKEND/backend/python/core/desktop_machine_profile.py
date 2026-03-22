from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from backend.python.database.local_store import LocalStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dedupe_strings(values: Iterable[str]) -> List[str]:
    rows: List[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip()
        if not clean:
            continue
        lowered = clean.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        rows.append(clean)
    return rows


class DesktopMachineProfileManager:
    _CATEGORY_TASK_HINTS: Dict[str, List[str]] = {
        "ai_companion": ["reasoning"],
        "browser": ["reasoning", "vision"],
        "communication": ["reasoning", "stt", "tts"],
        "creative": ["vision"],
        "data": ["reasoning"],
        "developer_tool": ["reasoning"],
        "education": ["reasoning"],
        "media": ["vision", "tts"],
        "notes": ["reasoning"],
        "office": ["reasoning", "vision"],
        "ops_console": ["reasoning"],
        "productivity": ["reasoning"],
        "remote_support": ["vision", "reasoning"],
        "search": ["reasoning"],
        "terminal": ["reasoning"],
        "utility": ["reasoning"],
    }

    def __init__(self, *, store_path: str = "data/desktop_machine_profile.json") -> None:
        self._store = LocalStore(store_path)

    def latest_snapshot(self) -> Dict[str, Any]:
        payload = self._store.get("latest_snapshot", {})
        return dict(payload) if isinstance(payload, dict) else {}

    def snapshot_history(self, *, limit: int = 12) -> Dict[str, Any]:
        bounded = max(1, min(int(limit or 12), 120))
        rows = self._store.get("snapshot_history", [])
        items = [dict(item) for item in rows if isinstance(item, dict)][:bounded] if isinstance(rows, list) else []
        return {
            "status": "success",
            "count": len(items),
            "items": items,
        }

    def task_model_preferences(self) -> Dict[str, Any]:
        rows = self._store.get("task_model_preferences", {})
        by_task = dict(rows) if isinstance(rows, dict) else {}
        items = []
        for task_name, payload in sorted(by_task.items()):
            if not isinstance(payload, dict):
                continue
            row = dict(payload)
            row["task"] = str(task_name or "").strip().lower()
            items.append(row)
        return {
            "status": "success",
            "count": len(items),
            "items": items,
            "by_task": by_task,
        }

    def update_task_preferences(
        self,
        *,
        task: str = "",
        provider: str = "",
        model_name: str = "",
        execution_backend: str = "",
        model_path: str = "",
        notes: str = "",
        allow_remote: Optional[bool] = None,
        preferred_runtime: str = "",
        preferences: Optional[Dict[str, Any]] = None,
        source: str = "manual",
    ) -> Dict[str, Any]:
        clean_task = str(task or "").strip().lower()
        by_task = self._store.get("task_model_preferences", {})
        payload = dict(by_task) if isinstance(by_task, dict) else {}
        now_iso = _utc_now_iso()
        updated_count = 0

        if isinstance(preferences, dict) and preferences:
            for raw_task, raw_row in preferences.items():
                task_name = str(raw_task or "").strip().lower()
                if not task_name or not isinstance(raw_row, dict):
                    continue
                merged = dict(payload.get(task_name, {})) if isinstance(payload.get(task_name, {}), dict) else {}
                merged.update({key: value for key, value in raw_row.items() if value not in {"", None}})
                merged["task"] = task_name
                merged["updated_at"] = now_iso
                merged["source"] = source
                payload[task_name] = merged
                updated_count += 1
        elif clean_task:
            merged = dict(payload.get(clean_task, {})) if isinstance(payload.get(clean_task, {}), dict) else {}
            if provider:
                merged["provider"] = str(provider).strip().lower()
            if model_name:
                merged["model_name"] = str(model_name).strip()
            if execution_backend:
                merged["execution_backend"] = str(execution_backend).strip().lower()
            if model_path:
                merged["model_path"] = str(model_path).strip()
            if notes:
                merged["notes"] = str(notes).strip()
            if preferred_runtime:
                merged["preferred_runtime"] = str(preferred_runtime).strip().lower()
            if allow_remote is not None:
                merged["allow_remote"] = bool(allow_remote)
            merged["task"] = clean_task
            merged["updated_at"] = now_iso
            merged["source"] = source
            payload[clean_task] = merged
            updated_count = 1
        else:
            return {"status": "error", "message": "task or preferences is required"}

        self._store.set("task_model_preferences", payload)
        current = self.task_model_preferences()
        current["updated_count"] = updated_count
        current["updated_at"] = now_iso
        return current

    def build_snapshot(
        self,
        *,
        system_profile: Dict[str, Any],
        app_inventory: Dict[str, Any],
        launch_memory: Dict[str, Any],
        provider_snapshot: Dict[str, Any],
        provider_verifications: Dict[str, Any],
        local_models: Dict[str, Any],
        model_setup_workspace: Dict[str, Any],
        task_preferences: Dict[str, Any],
        source: str = "api",
    ) -> Dict[str, Any]:
        apps = app_inventory.get("items", []) if isinstance(app_inventory.get("items", []), list) else []
        top_apps = [
            {
                "name": str(item.get("display_name", "") or item.get("name", "") or "").strip(),
                "category": str(item.get("category", "") or "").strip().lower(),
                "usage_score": float(item.get("usage_score", 0.0) or 0.0),
                "path_ready": bool(item.get("path_ready", False)),
            }
            for item in apps[:24]
            if isinstance(item, dict)
        ]
        task_focus = self._infer_task_focus(apps)
        recommended_models = self._recommended_models(
            local_models=local_models,
            task_preferences=task_preferences,
            task_focus=task_focus,
        )
        recommendations = self._build_recommendations(
            system_profile=system_profile,
            app_inventory=app_inventory,
            launch_memory=launch_memory,
            provider_snapshot=provider_snapshot,
            provider_verifications=provider_verifications,
            local_models=local_models,
            model_setup_workspace=model_setup_workspace,
            task_preferences=task_preferences,
            task_focus=task_focus,
            recommended_models=recommended_models,
        )
        readiness = self._build_readiness(
            provider_snapshot=provider_snapshot,
            local_models=local_models,
            app_inventory=app_inventory,
            recommendations=recommendations,
        )
        machine_id = self._machine_id(system_profile)
        return {
            "status": "success",
            "captured_at": _utc_now_iso(),
            "machine_id": machine_id,
            "source": str(source or "api").strip().lower() or "api",
            "system_profile": dict(system_profile or {}),
            "applications": {
                "inventory": dict(app_inventory or {}),
                "launch_memory": dict(launch_memory or {}),
                "top_used_apps": top_apps,
                "task_focus": task_focus,
            },
            "providers": {
                "snapshot": dict(provider_snapshot or {}),
                "verifications": dict(provider_verifications or {}),
            },
            "models": {
                "local_inventory": dict(local_models or {}),
                "setup_workspace": dict(model_setup_workspace or {}),
                "task_preferences": dict(task_preferences or {}),
                "recommended_models": recommended_models,
            },
            "recommendations": recommendations,
            "setup_actions": list(recommendations),
            "readiness": readiness,
        }

    def record_snapshot(self, snapshot: Dict[str, Any], *, source: str = "api") -> Dict[str, Any]:
        payload = dict(snapshot or {})
        payload["source"] = str(source or payload.get("source", "api") or "api").strip().lower() or "api"
        payload["captured_at"] = str(payload.get("captured_at", "") or _utc_now_iso()).strip()
        self._store.set("latest_snapshot", payload)
        history = self._store.get("snapshot_history", [])
        rows = [dict(item) for item in history if isinstance(item, dict)] if isinstance(history, list) else []
        rows.insert(0, payload)
        self._store.set("snapshot_history", rows[:32])
        return payload

    @staticmethod
    def _machine_id(system_profile: Dict[str, Any]) -> str:
        hostname = str(system_profile.get("hostname", "") or "").strip().lower()
        windows = system_profile.get("windows", {}) if isinstance(system_profile.get("windows", {}), dict) else {}
        cpu = system_profile.get("cpu", {}) if isinstance(system_profile.get("cpu", {}), dict) else {}
        seed = "|".join(
            [
                hostname,
                str(windows.get("caption", "") or "").strip().lower(),
                str(windows.get("build_number", "") or "").strip(),
                str(cpu.get("name", "") or "").strip().lower(),
            ]
        )
        return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]

    def _infer_task_focus(self, apps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        scores: Dict[str, float] = {}
        evidence: Dict[str, List[str]] = {}
        for item in apps:
            if not isinstance(item, dict):
                continue
            category = str(item.get("category", "") or "").strip().lower()
            app_name = str(item.get("display_name", "") or item.get("name", "") or "").strip()
            usage_score = max(1.0, float(item.get("usage_score", 0.0) or 0.0))
            for task_name in self._CATEGORY_TASK_HINTS.get(category, []):
                scores[task_name] = float(scores.get(task_name, 0.0) or 0.0) + usage_score
                evidence.setdefault(task_name, []).append(app_name)
        rows = []
        for task_name, score in sorted(scores.items(), key=lambda item: (-item[1], item[0])):
            rows.append(
                {
                    "task": task_name,
                    "score": round(float(score), 3),
                    "evidence_apps": _dedupe_strings(evidence.get(task_name, []))[:8],
                }
            )
        return rows

    def _recommended_models(
        self,
        *,
        local_models: Dict[str, Any],
        task_preferences: Dict[str, Any],
        task_focus: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        preference_rows = task_preferences.get("by_task", {}) if isinstance(task_preferences.get("by_task", {}), dict) else {}
        inventory = local_models.get("inventory", {}) if isinstance(local_models.get("inventory", {}), dict) else {}
        inventory_items = inventory.get("items", []) if isinstance(inventory.get("items", []), list) else []
        bridge_profiles = local_models.get("bridge_profiles", []) if isinstance(local_models.get("bridge_profiles", []), list) else []
        tasks = {str(row.get("task", "") or "").strip().lower() for row in task_focus if isinstance(row, dict)}
        tasks.update(str(name or "").strip().lower() for name in preference_rows.keys())
        tasks.update(
            str(item.get("task", "") or "").strip().lower()
            for item in inventory_items
            if isinstance(item, dict) and str(item.get("task", "") or "").strip()
        )
        rows: List[Dict[str, Any]] = []
        for task_name in sorted(task for task in tasks if task):
            preference = preference_rows.get(task_name, {}) if isinstance(preference_rows.get(task_name, {}), dict) else {}
            if preference:
                rows.append(
                    {
                        "task": task_name,
                        "source": "task_preference",
                        "provider": str(preference.get("provider", "") or "").strip().lower(),
                        "model_name": str(preference.get("model_name", "") or "").strip(),
                        "execution_backend": str(preference.get("execution_backend", "") or "").strip().lower(),
                        "model_path": str(preference.get("model_path", "") or "").strip(),
                    }
                )
                continue
            bridge_match = next(
                (
                    row for row in bridge_profiles
                    if isinstance(row, dict)
                    and str(row.get("task", "") or "").strip().lower() == task_name
                    and int(row.get("launch_ready_count", 0) or 0) > 0
                ),
                None,
            )
            if isinstance(bridge_match, dict):
                rows.append(
                    {
                        "task": task_name,
                        "source": "bridge_profile",
                        "provider": str(bridge_match.get("provider", "") or "").strip().lower(),
                        "model_name": str(bridge_match.get("name", "") or "").strip(),
                        "execution_backend": str(bridge_match.get("execution_backend", "") or "").strip().lower(),
                        "model_path": str(bridge_match.get("path", "") or "").strip(),
                    }
                )
                continue
            inventory_match = next(
                (
                    row for row in inventory_items
                    if isinstance(row, dict)
                    and bool(row.get("present", False))
                    and str(row.get("task", "") or "").strip().lower() == task_name
                ),
                None,
            )
            if isinstance(inventory_match, dict):
                rows.append(
                    {
                        "task": task_name,
                        "source": "local_inventory",
                        "provider": str(inventory_match.get("provider", "local") or "local").strip().lower(),
                        "model_name": str(inventory_match.get("name", "") or "").strip(),
                        "execution_backend": str(inventory_match.get("backend", "") or "").strip().lower(),
                        "model_path": str(inventory_match.get("path", "") or "").strip(),
                    }
                )
        return rows

    def _build_recommendations(
        self,
        *,
        system_profile: Dict[str, Any],
        app_inventory: Dict[str, Any],
        launch_memory: Dict[str, Any],
        provider_snapshot: Dict[str, Any],
        provider_verifications: Dict[str, Any],
        local_models: Dict[str, Any],
        model_setup_workspace: Dict[str, Any],
        task_preferences: Dict[str, Any],
        task_focus: List[Dict[str, Any]],
        recommended_models: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        recommendations: List[Dict[str, Any]] = []
        providers = provider_snapshot.get("providers", {}) if isinstance(provider_snapshot.get("providers", {}), dict) else {}
        missing_required_count = int(provider_snapshot.get("missing_required_count", 0) or 0)
        required_providers = [
            str(item).strip().lower()
            for item in provider_snapshot.get("manifest_required_providers", [])
            if str(item).strip()
        ] if isinstance(provider_snapshot.get("manifest_required_providers", []), list) else []
        system_gpus = system_profile.get("gpus", []) if isinstance(system_profile.get("gpus", []), list) else []
        runtimes = system_profile.get("runtimes", {}) if isinstance(system_profile.get("runtimes", {}), dict) else {}
        inventory = local_models.get("inventory", {}) if isinstance(local_models.get("inventory", {}), dict) else {}
        present_count = int(inventory.get("present_count", 0) or 0)
        task_counts = local_models.get("task_counts", {}) if isinstance(local_models.get("task_counts", {}), dict) else {}
        missing_task_counts = local_models.get("missing_task_counts", {}) if isinstance(local_models.get("missing_task_counts", {}), dict) else {}
        preference_items = task_preferences.get("items", []) if isinstance(task_preferences.get("items", []), list) else []
        launch_memory_total = int(launch_memory.get("total", 0) or 0)
        path_ready_count = int(app_inventory.get("path_ready_count", 0) or 0)

        if missing_required_count > 0:
            recommendations.append(
                {
                    "code": "configure_required_providers",
                    "severity": "high",
                    "title": "Configure required model providers",
                    "message": f"{missing_required_count} required provider credential set is still not ready.",
                    "providers": required_providers,
                }
            )
        hf_row = providers.get("huggingface", {}) if isinstance(providers.get("huggingface", {}), dict) else {}
        hf_verification = provider_verifications.get("huggingface", {}) if isinstance(provider_verifications.get("huggingface", {}), dict) else {}
        if "huggingface" in required_providers and not bool(hf_row.get("ready", False)):
            recommendations.append(
                {
                    "code": "configure_huggingface_token",
                    "severity": "high",
                    "title": "Add Hugging Face access token",
                    "message": "Local model acquisition and gated repo access need a valid Hugging Face token.",
                    "provider": "huggingface",
                    "verification_summary": str(hf_verification.get("summary", "") or hf_verification.get("message", "") or "").strip(),
                }
            )
        for provider_name, verification in provider_verifications.items():
            if not isinstance(verification, dict):
                continue
            if str(verification.get("status", "") or "").strip().lower() in {"success"} and bool(verification.get("verified", False)):
                continue
            if not bool((providers.get(provider_name, {}) if isinstance(providers.get(provider_name, {}), dict) else {}).get("present", False)):
                continue
            recommendations.append(
                {
                    "code": f"refresh_{provider_name}_credential",
                    "severity": "medium",
                    "title": f"Recheck {provider_name} credential",
                    "message": str(verification.get("summary", "") or verification.get("message", "") or "Provider verification needs attention.").strip(),
                    "provider": provider_name,
                }
            )
        if not bool(runtimes.get("rustc", {}).get("available", False)) or not bool(runtimes.get("cargo", {}).get("available", False)):
            recommendations.append(
                {
                    "code": "install_rust_toolchain",
                    "severity": "medium",
                    "title": "Install Rust toolchain",
                    "message": "Rust runtime checks are incomplete, which can limit native bridge and local runtime setup.",
                }
            )
        if not bool(runtimes.get("huggingface_cli", {}).get("available", False)) and not bool(runtimes.get("hf", {}).get("available", False)):
            recommendations.append(
                {
                    "code": "install_huggingface_cli",
                    "severity": "medium",
                    "title": "Install Hugging Face CLI",
                    "message": "Hugging Face CLI is missing, so gated/local model acquisition cannot be fully automated yet.",
                }
            )
        if present_count == 0:
            recommendations.append(
                {
                    "code": "install_first_local_models",
                    "severity": "high",
                    "title": "Install initial local models",
                    "message": "No local models are currently present. JARVIS will need at least one reasoning or speech stack to work offline well.",
                }
            )
        if system_gpus and int(task_counts.get("reasoning", 0) or 0) == 0:
            recommendations.append(
                {
                    "code": "install_local_reasoning_model",
                    "severity": "medium",
                    "title": "Install a local reasoning model",
                    "message": "A usable GPU is present, so a local reasoning model would improve privacy and resilience.",
                }
            )
        focus_tasks = {str(row.get("task", "") or "").strip().lower() for row in task_focus if isinstance(row, dict)}
        if "vision" in focus_tasks and int(task_counts.get("vision", 0) or 0) == 0 and int(missing_task_counts.get("vision", 0) or 0) > 0:
            recommendations.append(
                {
                    "code": "install_local_vision_model",
                    "severity": "medium",
                    "title": "Install a local vision model",
                    "message": "App usage suggests visual understanding matters, but no local vision model is currently ready.",
                }
            )
        if launch_memory_total == 0 or path_ready_count == 0:
            recommendations.append(
                {
                    "code": "run_app_discovery",
                    "severity": "medium",
                    "title": "Run installed-app discovery",
                    "message": "Saved launch targets are still thin. A discovery pass would improve automatic app opening and control reuse.",
                }
            )
        if not preference_items and recommended_models:
            recommendations.append(
                {
                    "code": "assign_task_model_preferences",
                    "severity": "low",
                    "title": "Save per-task model preferences",
                    "message": "Local/bridge candidates are ready, but no explicit task-model preferences are stored yet.",
                }
            )
        workspace_recommendations = model_setup_workspace.get("recommendations", []) if isinstance(model_setup_workspace.get("recommendations", []), list) else []
        for item in workspace_recommendations[:8]:
            if not isinstance(item, dict):
                continue
            code = str(item.get("code", "") or "").strip()
            title = str(item.get("title", "") or item.get("summary", "") or "").strip()
            message = str(item.get("message", "") or item.get("detail", "") or "").strip()
            if not (code or title or message):
                continue
            recommendations.append(
                {
                    "code": code or "workspace_recommendation",
                    "severity": str(item.get("severity", "low") or "low").strip().lower(),
                    "title": title or "Model setup recommendation",
                    "message": message or title or "Model workspace suggests additional setup work.",
                }
            )
        deduped: List[Dict[str, Any]] = []
        seen_codes: set[str] = set()
        for item in recommendations:
            code = str(item.get("code", "") or "").strip().lower()
            if code and code in seen_codes:
                continue
            if code:
                seen_codes.add(code)
            deduped.append(item)
        return deduped

    def _build_readiness(
        self,
        *,
        provider_snapshot: Dict[str, Any],
        local_models: Dict[str, Any],
        app_inventory: Dict[str, Any],
        recommendations: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        providers = provider_snapshot.get("providers", {}) if isinstance(provider_snapshot.get("providers", {}), dict) else {}
        ready_provider_count = len(
            [row for row in providers.values() if isinstance(row, dict) and bool(row.get("ready", False))]
        )
        inventory = local_models.get("inventory", {}) if isinstance(local_models.get("inventory", {}), dict) else {}
        present_model_count = int(inventory.get("present_count", 0) or 0)
        path_ready_count = int(app_inventory.get("path_ready_count", 0) or 0)
        score = 0
        score += min(30, present_model_count * 6)
        score += min(25, ready_provider_count * 5)
        score += min(25, path_ready_count // 4)
        score += max(0, 20 - len(recommendations) * 3)
        return {
            "status": "success",
            "score": max(0, min(100, int(score))),
            "provider_ready_count": ready_provider_count,
            "provider_total_count": len(providers),
            "local_model_present_count": present_model_count,
            "app_path_ready_count": path_ready_count,
            "recommendation_count": len(recommendations),
        }
