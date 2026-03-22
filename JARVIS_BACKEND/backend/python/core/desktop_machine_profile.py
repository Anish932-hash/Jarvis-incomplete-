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
        previous_snapshot = self.latest_snapshot()
        launch_strategy_summary = self._launch_strategy_summary(launch_memory)
        application_summary = self._application_summary(
            app_inventory=app_inventory,
            launch_memory=launch_memory,
            launch_strategy_summary=launch_strategy_summary,
        )
        change_detection = self._build_change_detection(
            previous_snapshot=previous_snapshot,
            system_profile=system_profile,
            app_inventory=app_inventory,
            provider_snapshot=provider_snapshot,
            local_models=local_models,
        )
        provider_summary = self._provider_summary(
            provider_snapshot=provider_snapshot,
            provider_verifications=provider_verifications,
        )
        recommended_models = self._recommended_models(
            local_models=local_models,
            task_preferences=task_preferences,
            task_focus=task_focus,
        )
        model_summary = self._model_summary(
            local_models=local_models,
            task_preferences=task_preferences,
            recommended_models=recommended_models,
            model_setup_workspace=model_setup_workspace,
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
            launch_strategy_summary=launch_strategy_summary,
            change_detection=change_detection,
        )
        readiness = self._build_readiness(
            provider_snapshot=provider_snapshot,
            local_models=local_models,
            app_inventory=app_inventory,
            recommendations=recommendations,
            system_profile=system_profile,
            launch_strategy_summary=launch_strategy_summary,
            change_detection=change_detection,
        )
        machine_id = self._machine_id(system_profile)
        return {
            "status": "success",
            "captured_at": _utc_now_iso(),
            "machine_id": machine_id,
            "source": str(source or "api").strip().lower() or "api",
            "system_profile": dict(system_profile or {}),
            "applications": {
                **application_summary,
                "inventory": dict(app_inventory or {}),
                "launch_memory": dict(launch_memory or {}),
                "launch_strategy_summary": launch_strategy_summary,
                "top_used_apps": top_apps,
                "task_focus": task_focus,
            },
            "providers": {
                "summary": provider_summary,
                "snapshot": dict(provider_snapshot or {}),
                "verifications": dict(provider_verifications or {}),
            },
            "models": {
                **model_summary,
                "local_inventory": dict(local_models or {}),
                "setup_workspace": dict(model_setup_workspace or {}),
                "task_preferences": dict(task_preferences or {}),
                "recommended_models": recommended_models,
            },
            "recommendations": recommendations,
            "setup_actions": list(recommendations),
            "readiness": readiness,
            "change_detection": change_detection,
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

    def _launch_strategy_summary(self, launch_memory: Dict[str, Any]) -> Dict[str, Any]:
        rows = launch_memory.get("items", []) if isinstance(launch_memory.get("items", []), list) else []
        kind_counts: Dict[str, int] = {}
        resolution_counts: Dict[str, int] = {}
        launch_count_total = 0
        resolve_count_total = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            kind = str(row.get("kind", "") or "unknown").strip().lower() or "unknown"
            resolution = str(row.get("resolution", "") or "unknown").strip().lower() or "unknown"
            kind_counts[kind] = int(kind_counts.get(kind, 0) or 0) + 1
            resolution_counts[resolution] = int(resolution_counts.get(resolution, 0) or 0) + 1
            launch_count_total += int(row.get("launch_count", 0) or 0)
            resolve_count_total += int(row.get("resolve_count", 0) or 0)
        return {
            "remembered_target_count": len([row for row in rows if isinstance(row, dict)]),
            "kind_counts": {str(key): int(value) for key, value in sorted(kind_counts.items(), key=lambda item: item[0])},
            "resolution_counts": {
                str(key): int(value) for key, value in sorted(resolution_counts.items(), key=lambda item: item[0])
            },
            "launch_count_total": launch_count_total,
            "resolve_count_total": resolve_count_total,
        }

    def _build_change_detection(
        self,
        *,
        previous_snapshot: Dict[str, Any],
        system_profile: Dict[str, Any],
        app_inventory: Dict[str, Any],
        provider_snapshot: Dict[str, Any],
        local_models: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(previous_snapshot, dict) or not previous_snapshot:
            return {
                "changed": False,
                "areas": [],
                "requires_revalidation": False,
                "previous_captured_at": "",
            }
        areas: List[str] = []
        previous_system = (
            previous_snapshot.get("system_profile", {})
            if isinstance(previous_snapshot.get("system_profile", {}), dict)
            else {}
        )
        previous_windows = previous_system.get("windows", {}) if isinstance(previous_system.get("windows", {}), dict) else {}
        current_windows = system_profile.get("windows", {}) if isinstance(system_profile.get("windows", {}), dict) else {}
        if str(previous_windows.get("build_number", "") or "").strip() != str(current_windows.get("build_number", "") or "").strip():
            areas.append("windows_build")
        previous_python = previous_system.get("python", {}) if isinstance(previous_system.get("python", {}), dict) else {}
        current_python = system_profile.get("python", {}) if isinstance(system_profile.get("python", {}), dict) else {}
        if str(previous_python.get("version", "") or "").strip() != str(current_python.get("version", "") or "").strip():
            areas.append("python_runtime")
        previous_runtimes = previous_system.get("runtimes", {}) if isinstance(previous_system.get("runtimes", {}), dict) else {}
        current_runtimes = system_profile.get("runtimes", {}) if isinstance(system_profile.get("runtimes", {}), dict) else {}
        for runtime_name in ("rustc", "cargo", "cython", "tesseract"):
            previous_version = (
                previous_runtimes.get(runtime_name, {}).get("version", "")
                if isinstance(previous_runtimes.get(runtime_name, {}), dict)
                else ""
            )
            current_version = (
                current_runtimes.get(runtime_name, {}).get("version", "")
                if isinstance(current_runtimes.get(runtime_name, {}), dict)
                else ""
            )
            if str(previous_version or "").strip() != str(current_version or "").strip():
                areas.append(runtime_name)
        previous_apps = (
            previous_snapshot.get("applications", {}).get("inventory", {})
            if isinstance(previous_snapshot.get("applications", {}), dict)
            and isinstance(previous_snapshot.get("applications", {}).get("inventory", {}), dict)
            else {}
        )
        previous_total = int(previous_apps.get("total", 0) or 0)
        current_total = int(app_inventory.get("total", 0) or 0)
        if abs(previous_total - current_total) >= 3:
            areas.append("app_inventory")
        previous_provider_missing = (
            int(previous_snapshot.get("providers", {}).get("snapshot", {}).get("missing_required_count", 0) or 0)
            if isinstance(previous_snapshot.get("providers", {}), dict)
            and isinstance(previous_snapshot.get("providers", {}).get("snapshot", {}), dict)
            else 0
        )
        current_provider_missing = int(provider_snapshot.get("missing_required_count", 0) or 0)
        if previous_provider_missing != current_provider_missing:
            areas.append("provider_state")
        previous_model_count = (
            int(previous_snapshot.get("models", {}).get("local_inventory", {}).get("inventory", {}).get("present_count", 0) or 0)
            if isinstance(previous_snapshot.get("models", {}), dict)
            and isinstance(previous_snapshot.get("models", {}).get("local_inventory", {}), dict)
            and isinstance(previous_snapshot.get("models", {}).get("local_inventory", {}).get("inventory", {}), dict)
            else 0
        )
        current_model_count = (
            int(local_models.get("inventory", {}).get("present_count", 0) or 0)
            if isinstance(local_models.get("inventory", {}), dict)
            else 0
        )
        if previous_model_count != current_model_count:
            areas.append("model_inventory")
        deduped_areas = _dedupe_strings(areas)
        return {
            "changed": bool(deduped_areas),
            "areas": deduped_areas,
            "requires_revalidation": bool(deduped_areas),
            "previous_captured_at": str(previous_snapshot.get("captured_at", "") or "").strip(),
        }

    def _application_summary(
        self,
        *,
        app_inventory: Dict[str, Any],
        launch_memory: Dict[str, Any],
        launch_strategy_summary: Dict[str, Any],
    ) -> Dict[str, Any]:
        inventory_items = app_inventory.get("items", []) if isinstance(app_inventory.get("items", []), list) else []
        category_counts: Dict[str, int] = {}
        for item in inventory_items:
            if not isinstance(item, dict):
                continue
            category = str(item.get("category", "") or "unknown").strip().lower() or "unknown"
            category_counts[category] = int(category_counts.get(category, 0) or 0) + 1
        return {
            "inventory_count": int(app_inventory.get("total", 0) or 0),
            "path_ready_count": int(app_inventory.get("path_ready_count", 0) or 0),
            "frequent_count": int(app_inventory.get("frequent_count", 0) or 0),
            "running_count": int(app_inventory.get("running_count", 0) or 0),
            "startup_entry_count": int(app_inventory.get("startup_entry_count", 0) or 0),
            "remembered_target_count": int(launch_strategy_summary.get("remembered_target_count", 0) or 0),
            "category_counts": {
                str(key): int(value)
                for key, value in sorted(category_counts.items(), key=lambda item: (-int(item[1]), item[0]))
            },
            "launch_memory_total": int(launch_memory.get("total", 0) or 0),
        }

    def _provider_summary(
        self,
        *,
        provider_snapshot: Dict[str, Any],
        provider_verifications: Dict[str, Any],
    ) -> Dict[str, Any]:
        providers = provider_snapshot.get("providers", {}) if isinstance(provider_snapshot.get("providers", {}), dict) else {}
        candidate_names = sorted(
            {
                *[str(name).strip().lower() for name in providers.keys() if str(name).strip()],
                *[
                    str(name).strip().lower()
                    for name in provider_verifications.keys()
                    if str(name).strip()
                ],
            }
        )
        verified_count = 0
        invalid_count = 0
        ready_count = 0
        present_count = 0
        required_count = 0
        attention_count = 0
        state_counts: Dict[str, int] = {}
        for provider_name in candidate_names:
            provider_row = providers.get(provider_name, {}) if isinstance(providers.get(provider_name, {}), dict) else {}
            verification = (
                provider_verifications.get(provider_name, {})
                if isinstance(provider_verifications.get(provider_name, {}), dict)
                else {}
            )
            present = bool(provider_row.get("present", False))
            required = bool(provider_row.get("required_by_manifest", False))
            ready = bool(provider_row.get("ready", False))
            verified = bool(verification.get("verified", False))
            verification_status = str(verification.get("status", "") or "").strip().lower()
            if present:
                present_count += 1
            if required:
                required_count += 1
            if ready:
                ready_count += 1
            if verified:
                verified_count += 1
            state = "ready"
            if verification_status in {"error", "invalid", "expired", "rate_limited", "network_failed"}:
                state = "invalid"
            elif required and not present:
                state = "missing"
            elif present and (verification_status in {"partial", "warning"} or not ready):
                state = "attention"
            if state == "invalid":
                invalid_count += 1
            if state == "attention":
                attention_count += 1
            state_counts[state] = int(state_counts.get(state, 0) or 0) + 1
        return {
            "provider_count": len(candidate_names),
            "present_count": present_count,
            "required_count": required_count,
            "ready_count": ready_count,
            "verified_count": verified_count,
            "invalid_count": invalid_count,
            "attention_count": attention_count,
            "state_counts": {
                str(key): int(value)
                for key, value in sorted(state_counts.items(), key=lambda item: item[0])
            },
        }

    def _model_summary(
        self,
        *,
        local_models: Dict[str, Any],
        task_preferences: Dict[str, Any],
        recommended_models: List[Dict[str, Any]],
        model_setup_workspace: Dict[str, Any],
    ) -> Dict[str, Any]:
        inventory = local_models.get("inventory", {}) if isinstance(local_models.get("inventory", {}), dict) else {}
        bridge_profiles = local_models.get("bridge_profiles", []) if isinstance(local_models.get("bridge_profiles", []), list) else []
        task_counts = local_models.get("task_counts", {}) if isinstance(local_models.get("task_counts", {}), dict) else {}
        preference_items = task_preferences.get("items", []) if isinstance(task_preferences.get("items", []), list) else []
        workspace_recommendations = (
            model_setup_workspace.get("recommendations", [])
            if isinstance(model_setup_workspace.get("recommendations", []), list)
            else []
        )
        launch_ready_count = sum(
            1
            for item in bridge_profiles
            if isinstance(item, dict) and int(item.get("launch_ready_count", 0) or 0) > 0
        )
        return {
            "inventory_count": int(inventory.get("present_count", 0) or 0),
            "task_counts": {str(key): int(value) for key, value in sorted(task_counts.items(), key=lambda item: item[0])},
            "recommended_count": len(recommended_models),
            "task_preference_count": len(preference_items),
            "bridge_profile_count": len([item for item in bridge_profiles if isinstance(item, dict)]),
            "launch_ready_count": launch_ready_count,
            "workspace_recommendation_count": len([item for item in workspace_recommendations if isinstance(item, dict)]),
        }

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
        launch_strategy_summary: Dict[str, Any],
        change_detection: Dict[str, Any],
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
        dependencies = system_profile.get("dependencies", {}) if isinstance(system_profile.get("dependencies", {}), dict) else {}
        permissions = system_profile.get("permissions", {}) if isinstance(system_profile.get("permissions", {}), dict) else {}
        virtualization = system_profile.get("virtualization", {}) if isinstance(system_profile.get("virtualization", {}), dict) else {}
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
        if int(launch_strategy_summary.get("remembered_target_count", 0) or 0) < max(4, path_ready_count // 2):
            recommendations.append(
                {
                    "code": "seed_launch_memory",
                    "severity": "medium",
                    "title": "Seed app launch memory",
                    "message": "Installed apps are available, but remembered launch strategies are still thin. Seeding launch memory would improve automatic app opening.",
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
        if not bool(dependencies.get("native_build_ready", True)):
            recommendations.append(
                {
                    "code": "install_native_build_toolchain",
                    "severity": "medium",
                    "title": "Install native build toolchain",
                    "message": "C++ build support is incomplete. Native Windows bridge and Cython-backed upgrades will be harder to build reliably.",
                }
            )
        if not bool(dependencies.get("ocr_ready", True)):
            recommendations.append(
                {
                    "code": "install_ocr_runtime",
                    "severity": "medium",
                    "title": "Install OCR runtime",
                    "message": "OCR support is not fully ready yet. App understanding and before/after verification will stay degraded.",
                }
            )
        if not bool(dependencies.get("vision_ready", True)):
            recommendations.append(
                {
                    "code": "install_vision_runtime",
                    "severity": "medium",
                    "title": "Install local vision runtime",
                    "message": "Local vision packages are incomplete, so multimodal app labeling and semantic surface understanding will be limited.",
                }
            )
        if not bool(dependencies.get("python_packages", {}).get("cython", False)):
            recommendations.append(
                {
                    "code": "install_cython_runtime",
                    "severity": "low",
                    "title": "Install Cython",
                    "message": "Cython is missing, so Python-to-native bridge iteration will be less complete than intended.",
                }
            )
        if not bool(permissions.get("cwd_writable", True)) or not bool(permissions.get("temp_writable", True)):
            recommendations.append(
                {
                    "code": "fix_workspace_permissions",
                    "severity": "high",
                    "title": "Fix workspace write permissions",
                    "message": "JARVIS needs a writable workspace and temp directory for setup, model downloads, and app-learning artifacts.",
                }
            )
        if not bool(virtualization.get("virtualization_firmware_enabled", True)):
            recommendations.append(
                {
                    "code": "enable_virtualization_support",
                    "severity": "low",
                    "title": "Enable virtualization support",
                    "message": "Virtualization is not fully enabled. Some local runtime stacks and future sandboxing options may stay unavailable.",
                }
            )
        if bool(change_detection.get("requires_revalidation", False)):
            recommendations.append(
                {
                    "code": "revalidate_machine_profile",
                    "severity": "medium",
                    "title": "Revalidate machine and app memory",
                    "message": "Machine state changed since the last snapshot, so saved launch/app-control memory should be rechecked.",
                    "areas": list(change_detection.get("areas", [])) if isinstance(change_detection.get("areas", []), list) else [],
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
        system_profile: Dict[str, Any],
        launch_strategy_summary: Dict[str, Any],
        change_detection: Dict[str, Any],
    ) -> Dict[str, Any]:
        providers = provider_snapshot.get("providers", {}) if isinstance(provider_snapshot.get("providers", {}), dict) else {}
        ready_provider_count = len(
            [row for row in providers.values() if isinstance(row, dict) and bool(row.get("ready", False))]
        )
        inventory = local_models.get("inventory", {}) if isinstance(local_models.get("inventory", {}), dict) else {}
        present_model_count = int(inventory.get("present_count", 0) or 0)
        path_ready_count = int(app_inventory.get("path_ready_count", 0) or 0)
        remembered_target_count = int(launch_strategy_summary.get("remembered_target_count", 0) or 0)
        dependencies = system_profile.get("dependencies", {}) if isinstance(system_profile.get("dependencies", {}), dict) else {}
        permissions = system_profile.get("permissions", {}) if isinstance(system_profile.get("permissions", {}), dict) else {}
        score = 0
        score += min(30, present_model_count * 6)
        score += min(25, ready_provider_count * 5)
        score += min(25, path_ready_count // 4)
        score += min(10, remembered_target_count * 2)
        score += int(round((float(dependencies.get("ready_count", 0) or 0) / max(1.0, float(dependencies.get("total_checks", 6) or 6))) * 10.0))
        if bool(permissions.get("cwd_writable", True)) and bool(permissions.get("temp_writable", True)):
            score += 5
        score += max(0, 20 - len(recommendations) * 3)
        score = max(0, min(100, int(score)))
        overall_status = "attention"
        if score >= 85 and not bool(change_detection.get("requires_revalidation", False)) and len(recommendations) <= 2:
            overall_status = "ready"
        elif score >= 70:
            overall_status = "strong"
        elif score >= 50:
            overall_status = "partial"
        return {
            "status": "success",
            "score": score,
            "overall_status": overall_status,
            "provider_ready_count": ready_provider_count,
            "provider_total_count": len(providers),
            "local_model_present_count": present_model_count,
            "app_path_ready_count": path_ready_count,
            "launch_memory_target_count": remembered_target_count,
            "dependency_ready_count": int(dependencies.get("ready_count", 0) or 0),
            "dependency_total_checks": int(dependencies.get("total_checks", 0) or 0),
            "permissions_ready": bool(permissions.get("cwd_writable", True) and permissions.get("temp_writable", True)),
            "recommendation_count": len(recommendations),
            "revalidation_required": bool(change_detection.get("requires_revalidation", False)),
        }
