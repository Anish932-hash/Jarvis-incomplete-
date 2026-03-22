from __future__ import annotations

import copy
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from backend.python.database.local_store import LocalStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso_from_ts(value: float) -> str:
    try:
        numeric = float(value)
    except Exception:  # noqa: BLE001
        return ""
    if numeric <= 0:
        return ""
    return datetime.fromtimestamp(numeric, tz=timezone.utc).isoformat()


class DesktopAppMemorySupervisor:
    def __init__(
        self,
        *,
        state_path: str = "data/desktop_app_memory_supervisor.json",
        enabled: bool = False,
        interval_s: float = 300.0,
        max_apps: int = 2,
        per_app_limit: int = 24,
        history_limit: int = 8,
        query: str = "",
        category: str = "",
        ensure_app_launch: bool = True,
        probe_controls: bool = True,
        max_probe_controls: int = 4,
        follow_surface_waves: bool = True,
        max_surface_waves: int = 3,
        allow_risky_probes: bool = False,
        skip_known_apps: bool = True,
        prefer_unknown_apps: bool = True,
        continuous_learning: bool = True,
        revisit_stale_apps: bool = True,
        stale_after_hours: float = 72.0,
        revisit_failed_apps: bool = True,
        revalidate_known_controls: bool = True,
        prioritize_failure_hotspots: bool = True,
        target_container_roles: Optional[list[str]] = None,
        preferred_wave_actions: Optional[list[str]] = None,
        preferred_traversal_paths: Optional[list[str]] = None,
    ) -> None:
        self._store = LocalStore(state_path)
        self._lock = threading.RLock()
        self._wakeup = threading.Event()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._execute_callback: Optional[Callable[..., Dict[str, Any]]] = None
        self._memory_snapshot_callback: Optional[Callable[..., Dict[str, Any]]] = None
        self._config = self._default_config(
            enabled=enabled,
            interval_s=interval_s,
            max_apps=max_apps,
            per_app_limit=per_app_limit,
            history_limit=history_limit,
            query=query,
            category=category,
            ensure_app_launch=ensure_app_launch,
            probe_controls=probe_controls,
            max_probe_controls=max_probe_controls,
            follow_surface_waves=follow_surface_waves,
            max_surface_waves=max_surface_waves,
            allow_risky_probes=allow_risky_probes,
            skip_known_apps=skip_known_apps,
            prefer_unknown_apps=prefer_unknown_apps,
            continuous_learning=continuous_learning,
            revisit_stale_apps=revisit_stale_apps,
            stale_after_hours=stale_after_hours,
            revisit_failed_apps=revisit_failed_apps,
            revalidate_known_controls=revalidate_known_controls,
            prioritize_failure_hotspots=prioritize_failure_hotspots,
            target_container_roles=target_container_roles or [],
            preferred_wave_actions=preferred_wave_actions or [],
            preferred_traversal_paths=preferred_traversal_paths or [],
        )
        self._runtime = self._default_runtime()
        self._history: list[Dict[str, Any]] = []
        self._campaigns: Dict[str, Dict[str, Any]] = {}
        self._load()

    def start(
        self,
        execute_callback: Callable[..., Dict[str, Any]],
        memory_snapshot_callback: Optional[Callable[..., Dict[str, Any]]] = None,
    ) -> None:
        with self._lock:
            self._execute_callback = execute_callback
            if memory_snapshot_callback is not None:
                self._memory_snapshot_callback = memory_snapshot_callback
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._wakeup.clear()
            self._thread = threading.Thread(
                target=self._worker,
                name="desktop-app-memory-supervisor",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            thread = self._thread
        if thread is None:
            return
        self._stop_event.set()
        self._wakeup.set()
        thread.join(timeout=5)
        with self._lock:
            self._thread = None

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return self._public_status_locked()

    def history(
        self,
        *,
        limit: int = 12,
        status: str = "",
        source: str = "",
    ) -> Dict[str, Any]:
        with self._lock:
            normalized_limit = self._coerce_int(limit, minimum=1, maximum=128, default=12)
            normalized_status = str(status or "").strip().lower()
            normalized_source = str(source or "").strip().lower()
            items = [
                copy.deepcopy(item)
                for item in self._history
                if isinstance(item, dict)
                and (
                    not normalized_status
                    or str(item.get("status", "") or "").strip().lower() == normalized_status
                )
                and (
                    not normalized_source
                    or str(item.get("source", "") or "").strip().lower() == normalized_source
                )
            ]
            limited = items[-normalized_limit:]
            latest = dict(limited[-1]) if limited else {}
            status_counts: Dict[str, int] = {}
            source_counts: Dict[str, int] = {}
            surveyed_app_total = 0
            success_total = 0
            partial_total = 0
            error_total = 0
            skipped_total = 0
            wave_attempt_total = 0
            learned_surface_total = 0
            known_surface_total = 0
            reseed_total = 0
            stale_reseed_total = 0
            revisit_app_total = 0
            stale_target_total = 0
            attention_target_total = 0
            failure_target_total = 0
            revalidation_target_total = 0
            unknown_target_total = 0
            selection_strategy_counts: Dict[str, int] = {}
            for item in items:
                self._increment_count(status_counts, str(item.get("status", "") or "unknown"))
                self._increment_count(source_counts, str(item.get("source", "") or "unknown"))
                self._increment_count(selection_strategy_counts, str(item.get("selection_strategy", "") or "unclassified"))
                surveyed_app_total += self._coerce_int(item.get("surveyed_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                success_total += self._coerce_int(item.get("success_count", 0), minimum=0, maximum=1_000_000, default=0)
                partial_total += self._coerce_int(item.get("partial_count", 0), minimum=0, maximum=1_000_000, default=0)
                error_total += self._coerce_int(item.get("error_count", 0), minimum=0, maximum=1_000_000, default=0)
                skipped_total += self._coerce_int(item.get("skipped_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                wave_attempt_total += self._coerce_int(item.get("wave_attempt_count", 0), minimum=0, maximum=1_000_000, default=0)
                learned_surface_total += self._coerce_int(item.get("learned_surface_count", 0), minimum=0, maximum=1_000_000, default=0)
                known_surface_total += self._coerce_int(item.get("known_surface_count", 0), minimum=0, maximum=1_000_000, default=0)
                reseed_total += self._coerce_int(item.get("reseed_count", 0), minimum=0, maximum=1_000_000, default=0)
                stale_reseed_total += self._coerce_int(item.get("stale_reseed_count", 0), minimum=0, maximum=1_000_000, default=0)
                revisit_app_total += self._coerce_int(item.get("revisit_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                stale_target_total += self._coerce_int(item.get("stale_candidate_count", 0), minimum=0, maximum=1_000_000, default=0)
                attention_target_total += self._coerce_int(item.get("attention_candidate_count", 0), minimum=0, maximum=1_000_000, default=0)
                failure_target_total += self._coerce_int(item.get("failure_candidate_count", 0), minimum=0, maximum=1_000_000, default=0)
                revalidation_target_total += self._coerce_int(item.get("revalidation_candidate_count", 0), minimum=0, maximum=1_000_000, default=0)
                unknown_target_total += self._coerce_int(item.get("unknown_candidate_count", 0), minimum=0, maximum=1_000_000, default=0)
            return {
                "status": "success",
                "count": len(limited),
                "total": len(items),
                "limit": normalized_limit,
                "filters": {
                    "status": normalized_status,
                    "source": normalized_source,
                },
                "items": limited,
                "latest_run": latest,
                "summary": {
                    "status_counts": self._sorted_count_map(status_counts),
                    "source_counts": self._sorted_count_map(source_counts),
                    "surveyed_app_total": surveyed_app_total,
                    "success_total": success_total,
                    "partial_total": partial_total,
                    "error_total": error_total,
                    "skipped_total": skipped_total,
                    "wave_attempt_total": wave_attempt_total,
                    "learned_surface_total": learned_surface_total,
                    "known_surface_total": known_surface_total,
                    "reseed_total": reseed_total,
                    "stale_reseed_total": stale_reseed_total,
                    "revisit_app_total": revisit_app_total,
                    "stale_candidate_total": stale_target_total,
                    "attention_candidate_total": attention_target_total,
                    "failure_candidate_total": failure_target_total,
                    "revalidation_candidate_total": revalidation_target_total,
                    "unknown_candidate_total": unknown_target_total,
                    "selection_strategy_counts": self._sorted_count_map(selection_strategy_counts),
                },
            }

    def reset_history(
        self,
        *,
        status: str = "",
        source: str = "",
    ) -> Dict[str, Any]:
        with self._lock:
            normalized_status = str(status or "").strip().lower()
            normalized_source = str(source or "").strip().lower()
            before = len(self._history)
            if normalized_status or normalized_source:
                self._history = [
                    item
                    for item in self._history
                    if not (
                        isinstance(item, dict)
                        and (
                            not normalized_status
                            or str(item.get("status", "") or "").strip().lower() == normalized_status
                        )
                        and (
                            not normalized_source
                            or str(item.get("source", "") or "").strip().lower() == normalized_source
                        )
                    )
                ]
            else:
                self._history = []
            removed = max(0, before - len(self._history))
            self._runtime["updated_at"] = _utc_now_iso()
            self._persist_locked()
            return {
                "status": "success",
                "removed_count": removed,
                "remaining_count": len(self._history),
                "filters": {
                    "status": normalized_status,
                    "source": normalized_source,
                },
                "latest_run": copy.deepcopy(self._history[-1]) if self._history else {},
            }

    def campaigns(
        self,
        *,
        limit: int = 12,
        campaign_id: str = "",
        status: str = "",
    ) -> Dict[str, Any]:
        with self._lock:
            normalized_limit = self._coerce_int(limit, minimum=1, maximum=128, default=12)
            normalized_campaign_id = str(campaign_id or "").strip()
            normalized_status = str(status or "").strip().lower()
            rows = [copy.deepcopy(item) for item in self._campaigns.values() if isinstance(item, dict)]
            rows.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
            if normalized_campaign_id:
                rows = [item for item in rows if str(item.get("campaign_id", "") or "").strip() == normalized_campaign_id]
            if normalized_status:
                rows = [item for item in rows if str(item.get("status", "") or "").strip().lower() == normalized_status]
            limited = rows[:normalized_limit]
            status_counts: Dict[str, int] = {}
            pending_total = 0
            completed_total = 0
            failed_total = 0
            skipped_total = 0
            wave_attempt_total = 0
            learned_surface_total = 0
            known_surface_total = 0
            stale_target_total = 0
            attention_target_total = 0
            failure_target_total = 0
            revalidation_target_total = 0
            unknown_target_total = 0
            reseed_total = 0
            stale_reseed_total = 0
            revisit_app_total = 0
            adaptive_target_role_total = 0
            adaptive_wave_depth_total = 0
            preferred_path_hit_total = 0
            traversal_path_execution_total = 0
            target_container_role_counts: Dict[str, int] = {}
            traversed_container_role_counts: Dict[str, int] = {}
            preferred_wave_action_counts: Dict[str, int] = {}
            preferred_traversal_path_counts: Dict[str, int] = {}
            recommended_traversal_path_counts: Dict[str, int] = {}
            for item in rows:
                self._increment_count(status_counts, str(item.get("status", "") or "unknown"))
                pending_total += self._coerce_int(item.get("pending_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                completed_total += self._coerce_int(item.get("completed_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                failed_total += self._coerce_int(item.get("failed_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                skipped_total += self._coerce_int(item.get("skipped_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                wave_attempt_total += self._coerce_int(item.get("wave_attempt_count", 0), minimum=0, maximum=1_000_000, default=0)
                learned_surface_total += self._coerce_int(item.get("learned_surface_count", 0), minimum=0, maximum=1_000_000, default=0)
                known_surface_total += self._coerce_int(item.get("known_surface_count", 0), minimum=0, maximum=1_000_000, default=0)
                stale_target_total += self._coerce_int(item.get("stale_target_count", 0), minimum=0, maximum=1_000_000, default=0)
                attention_target_total += self._coerce_int(item.get("attention_target_count", 0), minimum=0, maximum=1_000_000, default=0)
                failure_target_total += self._coerce_int(item.get("failure_target_count", 0), minimum=0, maximum=1_000_000, default=0)
                revalidation_target_total += self._coerce_int(item.get("revalidation_target_count", 0), minimum=0, maximum=1_000_000, default=0)
                unknown_target_total += self._coerce_int(item.get("unknown_target_count", 0), minimum=0, maximum=1_000_000, default=0)
                reseed_total += self._coerce_int(item.get("reseed_count", 0), minimum=0, maximum=1_000_000, default=0)
                stale_reseed_total += self._coerce_int(item.get("stale_reseed_count", 0), minimum=0, maximum=1_000_000, default=0)
                revisit_app_total += self._coerce_int(item.get("revisit_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                preferred_path_hit_total += self._coerce_int(item.get("preferred_path_hits", 0), minimum=0, maximum=1_000_000, default=0)
                traversal_path_execution_total += self._coerce_int(item.get("traversal_path_execution_count", 0), minimum=0, maximum=1_000_000, default=0)
                if bool(item.get("adaptive_target_container_roles", False)):
                    adaptive_target_role_total += 1
                if bool(item.get("adaptive_surface_wave_depth", False)):
                    adaptive_wave_depth_total += 1
                for role in item.get("target_container_roles", []) if isinstance(item.get("target_container_roles", []), list) else []:
                    clean_role = self._normalize_name(role)
                    if clean_role:
                        target_container_role_counts[clean_role] = int(target_container_role_counts.get(clean_role, 0) or 0) + 1
                for key, value in (
                    dict(item.get("role_learned_counts", {})).items()
                    if isinstance(item.get("role_learned_counts", {}), dict)
                    else []
                ):
                    clean_role = self._normalize_name(key)
                    count_value = self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                    if clean_role and count_value > 0:
                        traversed_container_role_counts[clean_role] = int(traversed_container_role_counts.get(clean_role, 0) or 0) + count_value
                for action_name in item.get("preferred_wave_actions", []) if isinstance(item.get("preferred_wave_actions", []), list) else []:
                    clean_action = str(action_name or "").strip().lower()
                    if clean_action:
                        preferred_wave_action_counts[clean_action] = int(preferred_wave_action_counts.get(clean_action, 0) or 0) + 1
                for path_name in item.get("preferred_traversal_paths", []) if isinstance(item.get("preferred_traversal_paths", []), list) else []:
                    clean_path = str(path_name or "").strip().lower()
                    if clean_path:
                        preferred_traversal_path_counts[clean_path] = int(preferred_traversal_path_counts.get(clean_path, 0) or 0) + 1
                for path_name in item.get("recommended_traversal_paths", []) if isinstance(item.get("recommended_traversal_paths", []), list) else []:
                    clean_path = str(path_name or "").strip().lower()
                    if clean_path:
                        recommended_traversal_path_counts[clean_path] = int(recommended_traversal_path_counts.get(clean_path, 0) or 0) + 1
            return {
                "status": "success",
                "count": len(limited),
                "total": len(rows),
                "items": limited,
                "latest_campaign": copy.deepcopy(limited[0]) if limited else {},
                "filters": {
                    "campaign_id": normalized_campaign_id,
                    "status": normalized_status,
                },
                "summary": {
                    "status_counts": self._sorted_count_map(status_counts),
                    "pending_app_total": pending_total,
                    "completed_app_total": completed_total,
                    "failed_app_total": failed_total,
                    "skipped_app_total": skipped_total,
                    "wave_attempt_total": wave_attempt_total,
                    "learned_surface_total": learned_surface_total,
                    "known_surface_total": known_surface_total,
                    "stale_target_total": stale_target_total,
                    "attention_target_total": attention_target_total,
                    "failure_target_total": failure_target_total,
                    "revalidation_target_total": revalidation_target_total,
                    "unknown_target_total": unknown_target_total,
                    "reseed_total": reseed_total,
                    "stale_reseed_total": stale_reseed_total,
                    "revisit_app_total": revisit_app_total,
                    "adaptive_target_role_total": adaptive_target_role_total,
                    "adaptive_wave_depth_total": adaptive_wave_depth_total,
                    "preferred_path_hit_total": preferred_path_hit_total,
                    "traversal_path_execution_total": traversal_path_execution_total,
                    "top_target_container_roles": [
                        {"value": str(key), "count": int(value)}
                        for key, value in sorted(
                            target_container_role_counts.items(),
                            key=lambda entry: (int(entry[1]), str(entry[0])),
                            reverse=True,
                        )[:6]
                    ],
                    "top_traversed_container_roles": [
                        {"value": str(key), "count": int(value)}
                        for key, value in sorted(
                            traversed_container_role_counts.items(),
                            key=lambda entry: (int(entry[1]), str(entry[0])),
                            reverse=True,
                        )[:6]
                    ],
                    "top_preferred_wave_actions": [
                        {"value": str(key), "count": int(value)}
                        for key, value in sorted(
                            preferred_wave_action_counts.items(),
                            key=lambda entry: (int(entry[1]), str(entry[0])),
                            reverse=True,
                        )[:6]
                    ],
                    "top_preferred_traversal_paths": [
                        {"value": str(key), "count": int(value)}
                        for key, value in sorted(
                            preferred_traversal_path_counts.items(),
                            key=lambda entry: (int(entry[1]), str(entry[0])),
                            reverse=True,
                        )[:6]
                    ],
                    "top_recommended_traversal_paths": [
                        {"value": str(key), "count": int(value)}
                        for key, value in sorted(
                            recommended_traversal_path_counts.items(),
                            key=lambda entry: (int(entry[1]), str(entry[0])),
                            reverse=True,
                        )[:6]
                    ],
                },
            }

    def create_campaign(
        self,
        *,
        app_names: list[str],
        label: str = "",
        query: str = "",
        category: str = "",
        max_apps: int = 4,
        per_app_limit: int = 24,
        ensure_app_launch: bool = True,
        probe_controls: bool = True,
        max_probe_controls: int = 4,
        follow_surface_waves: bool = True,
        max_surface_waves: int = 3,
        allow_risky_probes: bool = False,
        skip_known_apps: bool = True,
        prefer_unknown_apps: bool = True,
        continuous_learning: bool = True,
        revisit_stale_apps: bool = True,
        stale_after_hours: float = 72.0,
        revisit_failed_apps: bool = True,
        revalidate_known_controls: bool = True,
        prioritize_failure_hotspots: bool = True,
        target_container_roles: Optional[list[str]] = None,
        preferred_wave_actions: Optional[list[str]] = None,
        preferred_traversal_paths: Optional[list[str]] = None,
        adaptive_app_profiles: Optional[list[Dict[str, Any]]] = None,
        source: str = "manual",
    ) -> Dict[str, Any]:
        clean_apps = self._dedupe_strings([str(item).strip() for item in app_names if str(item).strip()])
        if not clean_apps:
            return {"status": "error", "message": "at least one app is required to create a learning campaign"}
        requested_preferred_wave_actions = self._dedupe_strings(
            [str(item).strip().lower() for item in (preferred_wave_actions or []) if str(item).strip()]
        )[:8]
        clean_adaptive_profiles = [
            dict(item)
            for item in (adaptive_app_profiles or [])
            if isinstance(item, dict)
        ]
        with self._lock:
            stale_after_hours_value = self._coerce_float(
                stale_after_hours,
                minimum=4.0,
                maximum=720.0,
                default=72.0,
            )
            target_summary = self._classify_target_apps_locked(
                clean_apps,
                category=str(category or "").strip(),
                stale_after_hours=stale_after_hours_value,
                prefer_unknown_apps=prefer_unknown_apps,
                prioritize_failure_hotspots=prioritize_failure_hotspots,
                target_container_roles=target_container_roles,
            )
            ordered_target_apps = (
                list(target_summary.get("ordered_apps", []))
                if isinstance(target_summary.get("ordered_apps", []), list)
                else []
            )
            if not ordered_target_apps:
                ordered_target_apps = clean_apps[:]
            effective_target_container_roles, adaptive_target_container_roles = self._adaptive_target_container_roles_from_summary(
                target_summary,
                requested_roles=target_container_roles,
            )
            preferred_wave_actions, adaptive_preferred_wave_actions = self._adaptive_preferred_wave_actions_from_summary(
                target_summary
            )
            if requested_preferred_wave_actions:
                preferred_wave_actions = requested_preferred_wave_actions[:]
                adaptive_preferred_wave_actions = False
            summary_recommended_traversal_paths = self._dedupe_strings(
                [
                    str(item.get("value", "") or "").strip().lower()
                    for item in target_summary.get("top_recommended_traversal_paths", [])
                    if isinstance(target_summary.get("top_recommended_traversal_paths", []), list)
                    and isinstance(item, dict)
                    and str(item.get("value", "") or "").strip()
                ]
            )[:8]
            explicit_preferred_traversal_paths = self._dedupe_strings(
                [str(item).strip().lower() for item in (preferred_traversal_paths or []) if str(item).strip()]
            )[:8]
            effective_preferred_traversal_paths = (
                explicit_preferred_traversal_paths[:]
                if explicit_preferred_traversal_paths
                else summary_recommended_traversal_paths[:]
            )
            adaptive_preferred_traversal_paths = bool(
                effective_preferred_traversal_paths and not explicit_preferred_traversal_paths
            )
            effective_max_surface_waves, adaptive_surface_wave_depth = self._adaptive_campaign_wave_depth(
                self._coerce_int(max_surface_waves, minimum=1, maximum=8, default=3),
                target_container_roles=effective_target_container_roles,
                summary=dict(target_summary.get("summary", {})) if isinstance(target_summary, dict) else {},
                explicit_roles=bool(target_container_roles),
            )
            adaptive_runtime_strategy_counts: Dict[str, int] = {}
            runtime_band_counts: Dict[str, int] = {}
            expected_route_profile_counts: Dict[str, int] = {}
            expected_model_preference_counts: Dict[str, int] = {}
            expected_provider_source_counts: Dict[str, int] = {}
            for item in clean_adaptive_profiles:
                runtime_strategy_payload = (
                    dict(item.get("runtime_strategy", {}))
                    if isinstance(item.get("runtime_strategy", {}), dict)
                    else {}
                )
                runtime_strategy_profile = str(
                    item.get("adaptive_runtime_strategy_profile", "")
                    or runtime_strategy_payload.get("strategy_profile", "")
                    or ""
                ).strip().lower()
                if runtime_strategy_profile:
                    adaptive_runtime_strategy_counts[runtime_strategy_profile] = int(
                        adaptive_runtime_strategy_counts.get(runtime_strategy_profile, 0) or 0
                    ) + 1
                runtime_band = str(
                    item.get("runtime_band_preference", "")
                    or runtime_strategy_payload.get("runtime_band_preference", "")
                    or ""
                ).strip().lower()
                if runtime_band:
                    runtime_band_counts[runtime_band] = int(runtime_band_counts.get(runtime_band, 0) or 0) + 1
                expected_route_profile = str(
                    runtime_strategy_payload.get("preferred_probe_mode", "") or ""
                ).strip().lower()
                if bool(runtime_strategy_payload.get("prefer_native_stabilization", False)) and expected_route_profile:
                    expected_route_profile = f"{expected_route_profile}_native_stabilized"
                if expected_route_profile:
                    expected_route_profile_counts[expected_route_profile] = int(
                        expected_route_profile_counts.get(expected_route_profile, 0) or 0
                    ) + 1
                expected_model_preference = "accessibility"
                if runtime_band == "local":
                    expected_model_preference = "local_runtime"
                elif runtime_band == "hybrid":
                    expected_model_preference = "hybrid_runtime"
                elif runtime_band == "api":
                    expected_model_preference = "api_assist"
                expected_model_preference_counts[expected_model_preference] = int(
                    expected_model_preference_counts.get(expected_model_preference, 0) or 0
                ) + 1
                expected_provider_source = "accessibility_only"
                if runtime_band == "local":
                    expected_provider_source = "local_runtime"
                elif runtime_band == "hybrid":
                    expected_provider_source = "local_runtime_plus_ocr"
                elif runtime_band == "api":
                    expected_provider_source = "api_assist_plus_ocr"
                expected_provider_source_counts[expected_provider_source] = int(
                    expected_provider_source_counts.get(expected_provider_source, 0) or 0
                ) + 1
            campaign_id = self._campaign_id(label=label, app_names=clean_apps)
            now = _utc_now_iso()
            campaign = {
                "campaign_id": campaign_id,
                "label": str(label or "learned app survey campaign").strip() or "learned app survey campaign",
                "status": "pending",
                "created_at": now,
                "updated_at": now,
                "query": str(query or "").strip(),
                "category": str(category or "").strip(),
                "target_apps": ordered_target_apps,
                "pending_apps": ordered_target_apps[:],
                "completed_apps": [],
                "partial_apps": [],
                "failed_apps": [],
                "skipped_apps": [],
                "max_apps": self._coerce_int(max_apps, minimum=1, maximum=32, default=4),
                "per_app_limit": self._coerce_int(per_app_limit, minimum=4, maximum=80, default=24),
                "ensure_app_launch": bool(ensure_app_launch),
                "probe_controls": bool(probe_controls),
                "max_probe_controls": self._coerce_int(max_probe_controls, minimum=1, maximum=12, default=4),
                "follow_surface_waves": bool(follow_surface_waves),
                "max_surface_waves": self._coerce_int(max_surface_waves, minimum=1, maximum=8, default=3),
                "effective_max_surface_waves": effective_max_surface_waves,
                "adaptive_surface_wave_depth": adaptive_surface_wave_depth,
                "allow_risky_probes": bool(allow_risky_probes),
                "skip_known_apps": bool(skip_known_apps),
                "prefer_unknown_apps": bool(prefer_unknown_apps),
                "continuous_learning": bool(continuous_learning),
                "revisit_stale_apps": bool(revisit_stale_apps),
                "stale_after_hours": stale_after_hours_value,
                "revisit_failed_apps": bool(revisit_failed_apps),
                "revalidate_known_controls": bool(revalidate_known_controls),
                "prioritize_failure_hotspots": bool(prioritize_failure_hotspots),
                "target_container_roles": effective_target_container_roles[:8],
                "adaptive_target_container_roles": adaptive_target_container_roles,
                "preferred_wave_actions": preferred_wave_actions[:8],
                "adaptive_preferred_wave_actions": adaptive_preferred_wave_actions,
                "preferred_traversal_paths": effective_preferred_traversal_paths[:8],
                "adaptive_preferred_traversal_paths": adaptive_preferred_traversal_paths,
                "recommended_traversal_paths": summary_recommended_traversal_paths[:8],
                "adaptive_app_profiles": clean_adaptive_profiles[:24],
                "adaptive_runtime_strategy_counts": {
                    str(key): int(value)
                    for key, value in sorted(adaptive_runtime_strategy_counts.items(), key=lambda entry: entry[0])
                },
                "runtime_band_counts": {
                    str(key): int(value)
                    for key, value in sorted(runtime_band_counts.items(), key=lambda entry: entry[0])
                },
                "expected_route_profile_counts": {
                    str(key): int(value)
                    for key, value in sorted(expected_route_profile_counts.items(), key=lambda entry: entry[0])
                },
                "expected_model_preference_counts": {
                    str(key): int(value)
                    for key, value in sorted(expected_model_preference_counts.items(), key=lambda entry: entry[0])
                },
                "expected_provider_source_counts": {
                    str(key): int(value)
                    for key, value in sorted(expected_provider_source_counts.items(), key=lambda entry: entry[0])
                },
                "target_selection_summary": dict(target_summary.get("summary", {})) if isinstance(target_summary, dict) else {},
                "revalidation_focus_summary": {
                    "top_container_roles": [
                        dict(item)
                        for item in target_summary.get("top_revalidation_container_roles", [])
                        if isinstance(target_summary.get("top_revalidation_container_roles", []), list) and isinstance(item, dict)
                    ][:6],
                    "top_reason_codes": [
                        dict(item)
                        for item in target_summary.get("top_revalidation_reason_codes", [])
                        if isinstance(target_summary.get("top_revalidation_reason_codes", []), list) and isinstance(item, dict)
                    ][:8],
                    "top_preferred_wave_actions": [
                        dict(item)
                        for item in target_summary.get("top_preferred_wave_actions", [])
                        if isinstance(target_summary.get("top_preferred_wave_actions", []), list) and isinstance(item, dict)
                    ][:6],
                    "top_recommended_traversal_paths": [
                        dict(item)
                        for item in target_summary.get("top_recommended_traversal_paths", [])
                        if isinstance(target_summary.get("top_recommended_traversal_paths", []), list) and isinstance(item, dict)
                    ][:6],
                },
                "unknown_target_count": self._coerce_int(dict(target_summary.get("summary", {})).get("unknown_count", 0), minimum=0, maximum=1_000_000, default=0),
                "stale_target_count": self._coerce_int(dict(target_summary.get("summary", {})).get("stale_count", 0), minimum=0, maximum=1_000_000, default=0),
                "attention_target_count": self._coerce_int(dict(target_summary.get("summary", {})).get("attention_count", 0), minimum=0, maximum=1_000_000, default=0),
                "failure_target_count": self._coerce_int(dict(target_summary.get("summary", {})).get("failure_memory_count", 0), minimum=0, maximum=1_000_000, default=0),
                "revalidation_target_count": self._coerce_int(dict(target_summary.get("summary", {})).get("revalidation_count", 0), minimum=0, maximum=1_000_000, default=0),
                "healthy_target_count": self._coerce_int(dict(target_summary.get("summary", {})).get("healthy_count", 0), minimum=0, maximum=1_000_000, default=0),
                "reseed_count": 0,
                "stale_reseed_count": 0,
                "revisit_app_count": 0,
                "cycle_generation": 0,
                "latest_reseed_reason": "",
                "latest_reseed_summary": {},
                "run_count": 0,
                "latest_cycle_status": "",
                "latest_cycle_message": "",
                "latest_cycle_at": "",
                "latest_cycle_source": str(source or "manual").strip().lower() or "manual",
                "history": [],
            }
            self._apply_campaign_counts_locked(campaign)
            self._campaigns[campaign_id] = campaign
            self._runtime["updated_at"] = now
            self._persist_locked()
            return {
                "status": "success",
                "campaign": copy.deepcopy(campaign),
                "campaigns": self.campaigns(limit=8),
            }

    def run_campaign(
        self,
        *,
        campaign_id: str,
        max_apps: Optional[int] = None,
        source: str = "manual",
    ) -> Dict[str, Any]:
        with self._lock:
            callback = self._execute_callback
            if callback is None:
                return {"status": "unavailable", "message": "desktop app memory supervisor callback unavailable"}
            campaign = self._campaigns.get(str(campaign_id or "").strip())
            if not isinstance(campaign, dict):
                return {"status": "error", "message": "desktop app memory campaign not found"}
            pending_apps = [str(item).strip() for item in campaign.get("pending_apps", []) if str(item).strip()]
            reseed_summary: Dict[str, Any] = {}
            if not pending_apps:
                reseed_summary = self._reseed_campaign_pending_locked(campaign)
                pending_apps = [str(item).strip() for item in campaign.get("pending_apps", []) if str(item).strip()]
            if not pending_apps:
                campaign["status"] = "completed" if not campaign.get("failed_apps") and not campaign.get("partial_apps") else "attention"
                campaign["updated_at"] = _utc_now_iso()
                self._apply_campaign_counts_locked(campaign)
                self._persist_locked()
                return {
                    "status": "success",
                    "message": "desktop app memory campaign has no pending apps left",
                    "campaign": copy.deepcopy(campaign),
                    "reseed": copy.deepcopy(reseed_summary),
                    "campaigns": self.campaigns(limit=8),
                }

            batch_size = self._coerce_int(
                max_apps if max_apps is not None else campaign.get("max_apps", 4),
                minimum=1,
                maximum=32,
                default=4,
            )
            target_batch = pending_apps[:batch_size]
            force_known_revisit = bool(reseed_summary.get("force_known_revisit", False))
            cycle_target_summary = self._classify_target_apps_locked(
                target_batch
                or [
                    str(item).strip()
                    for item in campaign.get("target_apps", [])
                    if isinstance(campaign.get("target_apps", []), list) and str(item).strip()
                ],
                category=str(campaign.get("category", "") or "").strip(),
                stale_after_hours=self._coerce_float(
                    campaign.get("stale_after_hours", self._config.get("stale_after_hours", 72.0)),
                    minimum=4.0,
                    maximum=720.0,
                    default=72.0,
                ),
                prefer_unknown_apps=bool(campaign.get("prefer_unknown_apps", True)),
                prioritize_failure_hotspots=bool(campaign.get("prioritize_failure_hotspots", True)),
                target_container_roles=[
                    str(item).strip().lower()
                    for item in campaign.get("target_container_roles", [])
                    if isinstance(campaign.get("target_container_roles", []), list) and str(item).strip()
                ],
            )
            effective_target_container_roles, adaptive_target_container_roles = self._adaptive_target_container_roles_from_summary(
                cycle_target_summary,
                requested_roles=(
                    []
                    if bool(campaign.get("adaptive_target_container_roles", False))
                    else [
                        str(item).strip().lower()
                        for item in campaign.get("target_container_roles", [])
                        if isinstance(campaign.get("target_container_roles", []), list) and str(item).strip()
                    ]
                ),
            )
            effective_preferred_wave_actions, adaptive_preferred_wave_actions = self._adaptive_preferred_wave_actions_from_summary(
                cycle_target_summary
            )
            if not bool(campaign.get("adaptive_preferred_wave_actions", False)):
                effective_preferred_wave_actions = self._dedupe_strings(
                    [
                        str(item).strip().lower()
                        for item in campaign.get("preferred_wave_actions", [])
                        if isinstance(campaign.get("preferred_wave_actions", []), list) and str(item).strip()
                    ]
                )[:8]
                adaptive_preferred_wave_actions = False
            recommended_traversal_paths = self._dedupe_strings(
                [
                    *[
                        str(item).strip().lower()
                        for item in campaign.get("recommended_traversal_paths", [])
                        if isinstance(campaign.get("recommended_traversal_paths", []), list) and str(item).strip()
                    ],
                    *[
                        str(item.get("value", "") or "").strip().lower()
                        for item in cycle_target_summary.get("top_recommended_traversal_paths", [])
                        if isinstance(cycle_target_summary.get("top_recommended_traversal_paths", []), list)
                        and isinstance(item, dict)
                        and str(item.get("value", "") or "").strip()
                    ],
                ]
            )[:8]
            explicit_preferred_traversal_paths = self._dedupe_strings(
                [
                    str(item).strip().lower()
                    for item in campaign.get("preferred_traversal_paths", [])
                    if isinstance(campaign.get("preferred_traversal_paths", []), list) and str(item).strip()
                ]
            )[:8]
            effective_preferred_traversal_paths = (
                explicit_preferred_traversal_paths[:]
                if explicit_preferred_traversal_paths and not bool(campaign.get("adaptive_preferred_traversal_paths", False))
                else recommended_traversal_paths[:]
            )
            adaptive_preferred_traversal_paths = bool(
                effective_preferred_traversal_paths
                and (
                    bool(campaign.get("adaptive_preferred_traversal_paths", False))
                    or not explicit_preferred_traversal_paths
                )
            )
            effective_max_surface_waves, adaptive_surface_wave_depth = self._adaptive_campaign_wave_depth(
                self._coerce_int(campaign.get("max_surface_waves", 3), minimum=1, maximum=8, default=3),
                target_container_roles=effective_target_container_roles,
                summary=dict(cycle_target_summary.get("summary", {})) if isinstance(cycle_target_summary, dict) else {},
                explicit_roles=not bool(campaign.get("adaptive_target_container_roles", False)),
            )
            campaign_adaptive_profiles = [
                dict(item)
                for item in campaign.get("adaptive_app_profiles", [])
                if isinstance(campaign.get("adaptive_app_profiles", []), list) and isinstance(item, dict)
            ]
            effective_adaptive_profiles: list[Dict[str, Any]] = []
            for app_name in target_batch:
                normalized_keys = {
                    str(app_name or "").strip().lower(),
                }
                for item in campaign_adaptive_profiles:
                    candidate_keys = {
                        str(item.get("app_name", "") or "").strip().lower(),
                        str(item.get("profile_id", "") or "").strip().lower(),
                        str(item.get("profile_name", "") or "").strip().lower(),
                    }
                    candidate_keys = {value for value in candidate_keys if value}
                    if candidate_keys and candidate_keys.intersection(normalized_keys):
                        effective_adaptive_profiles.append(dict(item))
                        break
            cycle_runtime_strategy_counts: Dict[str, int] = {}
            cycle_runtime_band_counts: Dict[str, int] = {}
            for item in effective_adaptive_profiles:
                runtime_strategy_profile = str(
                    item.get("adaptive_runtime_strategy_profile", "")
                    or (
                        dict(item.get("runtime_strategy", {})).get("strategy_profile", "")
                        if isinstance(item.get("runtime_strategy", {}), dict)
                        else ""
                    )
                    or ""
                ).strip().lower()
                if runtime_strategy_profile:
                    cycle_runtime_strategy_counts[runtime_strategy_profile] = int(
                        cycle_runtime_strategy_counts.get(runtime_strategy_profile, 0) or 0
                    ) + 1
                runtime_band = str(
                    item.get("runtime_band_preference", "")
                    or (
                        dict(item.get("runtime_strategy", {})).get("runtime_band_preference", "")
                        if isinstance(item.get("runtime_strategy", {}), dict)
                        else ""
                    )
                    or ""
                ).strip().lower()
                if runtime_band:
                    cycle_runtime_band_counts[runtime_band] = int(cycle_runtime_band_counts.get(runtime_band, 0) or 0) + 1
            result = callback(
                app_names=target_batch,
                max_apps=len(target_batch),
                per_app_limit=self._coerce_int(campaign.get("per_app_limit", 24), minimum=4, maximum=80, default=24),
                query=str(campaign.get("query", "") or "").strip(),
                category=str(campaign.get("category", "") or "").strip(),
                ensure_app_launch=bool(campaign.get("ensure_app_launch", True)),
                probe_controls=bool(campaign.get("probe_controls", True)),
                max_probe_controls=self._coerce_int(campaign.get("max_probe_controls", 4), minimum=1, maximum=12, default=4),
                follow_surface_waves=bool(campaign.get("follow_surface_waves", True)),
                max_surface_waves=effective_max_surface_waves,
                allow_risky_probes=bool(campaign.get("allow_risky_probes", False)),
                skip_known_apps=bool(campaign.get("skip_known_apps", True)) and not force_known_revisit,
                prefer_unknown_apps=bool(campaign.get("prefer_unknown_apps", True)),
                target_container_roles=effective_target_container_roles[:8],
                preferred_wave_actions=effective_preferred_wave_actions[:8],
                preferred_traversal_paths=effective_preferred_traversal_paths[:8],
                revalidate_known_controls=bool(campaign.get("revalidate_known_controls", True)),
                prefer_failure_memory=bool(campaign.get("prioritize_failure_hotspots", True)),
                adaptive_app_profiles=effective_adaptive_profiles[: len(target_batch) + 4],
                source=str(source or "manual").strip().lower() or "manual",
            )
            result = dict(result) if isinstance(result, dict) else {"status": "error", "message": "invalid campaign execution payload"}
            actual_route_profile_counts = (
                dict(dict(result.get("targeting", {})).get("route_profile_counts", {}))
                if isinstance(result.get("targeting", {}), dict)
                and isinstance(dict(result.get("targeting", {})).get("route_profile_counts", {}), dict)
                else {}
            )
            actual_model_preference_counts = (
                dict(dict(result.get("targeting", {})).get("model_preference_counts", {}))
                if isinstance(result.get("targeting", {}), dict)
                and isinstance(dict(result.get("targeting", {})).get("model_preference_counts", {}), dict)
                else {}
            )
            actual_provider_source_counts = (
                dict(dict(result.get("targeting", {})).get("runtime_provider_source_counts", {}))
                if isinstance(result.get("targeting", {}), dict)
                and isinstance(dict(result.get("targeting", {})).get("runtime_provider_source_counts", {}), dict)
                else {}
            )
            route_fallback_app_count = self._coerce_int(
                dict(result.get("targeting", {})).get("route_fallback_app_count", 0)
                if isinstance(result.get("targeting", {}), dict)
                else 0,
                minimum=0,
                maximum=1_000_000,
                default=0,
            )
            if not actual_route_profile_counts or not actual_model_preference_counts or not actual_provider_source_counts:
                for item in effective_adaptive_profiles:
                    runtime_strategy_payload = (
                        dict(item.get("runtime_strategy", {}))
                        if isinstance(item.get("runtime_strategy", {}), dict)
                        else {}
                    )
                    runtime_band = str(
                        item.get("runtime_band_preference", "")
                        or runtime_strategy_payload.get("runtime_band_preference", "")
                        or ""
                    ).strip().lower()
                    if not actual_route_profile_counts:
                        expected_route_profile = str(
                            runtime_strategy_payload.get("preferred_probe_mode", "") or ""
                        ).strip().lower()
                        if bool(runtime_strategy_payload.get("prefer_native_stabilization", False)) and expected_route_profile:
                            expected_route_profile = f"{expected_route_profile}_native_stabilized"
                        if expected_route_profile:
                            actual_route_profile_counts[expected_route_profile] = int(
                                actual_route_profile_counts.get(expected_route_profile, 0) or 0
                            ) + 1
                    if not actual_model_preference_counts:
                        expected_model_preference = "accessibility"
                        if runtime_band == "local":
                            expected_model_preference = "local_runtime"
                        elif runtime_band == "hybrid":
                            expected_model_preference = "hybrid_runtime"
                        elif runtime_band == "api":
                            expected_model_preference = "api_assist"
                        actual_model_preference_counts[expected_model_preference] = int(
                            actual_model_preference_counts.get(expected_model_preference, 0) or 0
                        ) + 1
                    if not actual_provider_source_counts:
                        expected_provider_source = "accessibility_only"
                        if runtime_band == "local":
                            expected_provider_source = "local_runtime"
                        elif runtime_band == "hybrid":
                            expected_provider_source = "local_runtime_plus_ocr"
                        elif runtime_band == "api":
                            expected_provider_source = "api_assist_plus_ocr"
                        actual_provider_source_counts[expected_provider_source] = int(
                            actual_provider_source_counts.get(expected_provider_source, 0) or 0
                        ) + 1
            result_items = {
                str(item.get("app_name", "") or "").strip().lower(): dict(item)
                for item in result.get("items", [])
                if isinstance(result.get("items", []), list) and isinstance(item, dict) and str(item.get("app_name", "") or "").strip()
            }
            skipped_items = {
                str(item.get("app_name", "") or "").strip().lower(): dict(item)
                for item in result.get("skipped_apps", [])
                if isinstance(result.get("skipped_apps", []), list) and isinstance(item, dict) and str(item.get("app_name", "") or "").strip()
            }
            completed_apps = [str(item).strip() for item in campaign.get("completed_apps", []) if str(item).strip()]
            partial_apps = [dict(item) for item in campaign.get("partial_apps", []) if isinstance(item, dict)]
            failed_apps = [dict(item) for item in campaign.get("failed_apps", []) if isinstance(item, dict)]
            skipped_apps = [dict(item) for item in campaign.get("skipped_apps", []) if isinstance(item, dict)]
            next_pending: list[str] = []
            for app_name in pending_apps:
                normalized = str(app_name or "").strip()
                key = normalized.lower()
                if key in skipped_items:
                    skipped_apps.append(skipped_items[key])
                    continue
                item = result_items.get(key)
                if not item:
                    next_pending.append(normalized)
                    continue
                status = str(item.get("status", "") or "").strip().lower()
                if status == "success":
                    completed_apps.append(normalized)
                elif status == "partial":
                    partial_apps.append({"app_name": normalized, "status": status, "message": str(item.get("message", "") or "").strip()})
                else:
                    failed_apps.append({"app_name": normalized, "status": status or "error", "message": str(item.get("message", "") or "").strip()})
            resolved_lookup = {
                str(item).strip().lower()
                for item in completed_apps
                if str(item).strip()
            }
            resolved_lookup.update(
                str(item.get("app_name", "") or "").strip().lower()
                for item in skipped_apps
                if isinstance(item, dict) and str(item.get("app_name", "") or "").strip()
            )
            partial_apps = [
                item
                for item in partial_apps
                if str(item.get("app_name", "") or "").strip().lower() not in resolved_lookup
            ]
            failed_apps = [
                item
                for item in failed_apps
                if str(item.get("app_name", "") or "").strip().lower() not in resolved_lookup
            ]
            cycle_record = {
                "executed_at": _utc_now_iso(),
                "source": str(source or "manual").strip().lower() or "manual",
                "status": str(result.get("status", "") or "error").strip().lower() or "error",
                "message": str(result.get("message", "") or "").strip(),
                "target_apps": target_batch,
                "surveyed_app_count": self._coerce_int(result.get("surveyed_app_count", 0), minimum=0, maximum=1_000_000, default=0),
                "success_count": self._coerce_int(result.get("success_count", 0), minimum=0, maximum=1_000_000, default=0),
                "partial_count": self._coerce_int(result.get("partial_count", 0), minimum=0, maximum=1_000_000, default=0),
                "error_count": self._coerce_int(result.get("error_count", 0), minimum=0, maximum=1_000_000, default=0),
                "skipped_app_count": self._coerce_int(result.get("skipped_app_count", 0), minimum=0, maximum=1_000_000, default=0),
                "wave_attempt_count": self._wave_summary_metric(
                    dict(result.get("wave_summary", {})) if isinstance(result.get("wave_summary", {}), dict) else {},
                    primary_key="wave_attempt_total",
                    fallback_key="attempted_count",
                ),
                "learned_surface_count": self._wave_summary_metric(
                    dict(result.get("wave_summary", {})) if isinstance(result.get("wave_summary", {}), dict) else {},
                    primary_key="learned_surface_total",
                    fallback_key="learned_surface_count",
                ),
                "known_surface_count": self._wave_summary_metric(
                    dict(result.get("wave_summary", {})) if isinstance(result.get("wave_summary", {}), dict) else {},
                    primary_key="known_surface_total",
                    fallback_key="known_surface_count",
                ),
                "target_container_roles": effective_target_container_roles[:8],
                "adaptive_target_container_roles": adaptive_target_container_roles,
                "preferred_wave_actions": effective_preferred_wave_actions[:8],
                "adaptive_preferred_wave_actions": adaptive_preferred_wave_actions,
                "preferred_traversal_paths": effective_preferred_traversal_paths[:8],
                "adaptive_preferred_traversal_paths": adaptive_preferred_traversal_paths,
                "recommended_traversal_paths": recommended_traversal_paths[:8],
                "adaptive_runtime_strategy_counts": {
                    str(key): int(value)
                    for key, value in sorted(cycle_runtime_strategy_counts.items(), key=lambda entry: entry[0])
                },
                "runtime_band_counts": {
                    str(key): int(value)
                    for key, value in sorted(cycle_runtime_band_counts.items(), key=lambda entry: entry[0])
                },
                "route_profile_counts": {
                    str(key).strip().lower(): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                    for key, value in actual_route_profile_counts.items()
                    if str(key).strip()
                },
                "model_preference_counts": {
                    str(key).strip().lower(): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                    for key, value in actual_model_preference_counts.items()
                    if str(key).strip()
                },
                "provider_source_counts": {
                    str(key).strip().lower(): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                    for key, value in actual_provider_source_counts.items()
                    if str(key).strip()
                },
                "route_fallback_app_count": route_fallback_app_count,
                "max_surface_waves": effective_max_surface_waves,
                "adaptive_surface_wave_depth": adaptive_surface_wave_depth,
                "preferred_path_hits": self._coerce_int(
                    dict(result.get("wave_summary", {})).get("preferred_path_hits", 0)
                    if isinstance(result.get("wave_summary", {}), dict)
                    else 0,
                    minimum=0,
                    maximum=1_000_000,
                    default=0,
                ),
                "traversal_path_execution_count": self._coerce_int(
                    dict(result.get("wave_summary", {})).get("traversal_path_execution_count", 0)
                    if isinstance(result.get("wave_summary", {}), dict)
                    else 0,
                    minimum=0,
                    maximum=1_000_000,
                    default=0,
                ),
                "traversed_container_roles": self._dedupe_strings(
                    [
                        str(item).strip().lower()
                        for item in dict(result.get("wave_summary", {})).get("traversed_container_roles", [])
                        if isinstance(result.get("wave_summary", {}), dict)
                        and isinstance(dict(result.get("wave_summary", {})).get("traversed_container_roles", []), list)
                        and str(item).strip()
                    ]
                )[:8],
                "role_attempt_counts": {
                    self._normalize_name(key): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                    for key, value in (
                        dict(dict(result.get("wave_summary", {})).get("role_attempt_counts", {})).items()
                        if isinstance(result.get("wave_summary", {}), dict)
                        and isinstance(dict(result.get("wave_summary", {})).get("role_attempt_counts", {}), dict)
                        else []
                    )
                    if self._normalize_name(key)
                },
                "role_learned_counts": {
                    self._normalize_name(key): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                    for key, value in (
                        dict(dict(result.get("wave_summary", {})).get("role_learned_counts", {})).items()
                        if isinstance(result.get("wave_summary", {}), dict)
                        and isinstance(dict(result.get("wave_summary", {})).get("role_learned_counts", {}), dict)
                        else []
                    )
                    if self._normalize_name(key)
                },
                "executed_traversal_paths": self._dedupe_strings(
                    [
                        str(item).strip().lower()
                        for item in (
                            dict(result.get("wave_summary", {})).get("executed_traversal_paths", [])
                            if isinstance(result.get("wave_summary", {}), dict)
                            and isinstance(dict(result.get("wave_summary", {})).get("executed_traversal_paths", []), list)
                            else []
                        )
                        if str(item).strip()
                    ]
                )[:8],
                "selection_strategy": str(
                    reseed_summary.get("selection_strategy", "campaign_pending")
                    if isinstance(reseed_summary, dict) and reseed_summary
                    else "campaign_pending"
                ),
                "reseed_count": 1 if reseed_summary else 0,
                "stale_reseed_count": self._coerce_int(reseed_summary.get("stale_count", 0), minimum=0, maximum=1_000_000, default=0),
                "revisit_app_count": self._coerce_int(reseed_summary.get("revisit_app_count", 0), minimum=0, maximum=1_000_000, default=0),
                "stale_candidate_count": self._coerce_int(reseed_summary.get("stale_count", 0), minimum=0, maximum=1_000_000, default=0),
                "attention_candidate_count": self._coerce_int(reseed_summary.get("attention_count", 0), minimum=0, maximum=1_000_000, default=0),
                "failure_candidate_count": self._coerce_int(reseed_summary.get("failure_memory_count", 0), minimum=0, maximum=1_000_000, default=0),
                "revalidation_candidate_count": self._coerce_int(reseed_summary.get("revalidation_count", 0), minimum=0, maximum=1_000_000, default=0),
                "unknown_candidate_count": self._coerce_int(reseed_summary.get("unknown_count", 0), minimum=0, maximum=1_000_000, default=0),
                "cycle_target_summary": copy.deepcopy(cycle_target_summary),
                "reseed_summary": copy.deepcopy(reseed_summary),
            }
            campaign["completed_apps"] = self._dedupe_strings(completed_apps)
            campaign["partial_apps"] = partial_apps[-32:]
            campaign["failed_apps"] = failed_apps[-32:]
            campaign["skipped_apps"] = skipped_apps[-32:]
            campaign["pending_apps"] = next_pending
            campaign["target_container_roles"] = effective_target_container_roles[:8]
            campaign["adaptive_target_container_roles"] = adaptive_target_container_roles
            campaign["preferred_wave_actions"] = self._dedupe_strings(
                [
                    *[
                        str(item).strip().lower()
                        for item in campaign.get("preferred_wave_actions", [])
                        if isinstance(campaign.get("preferred_wave_actions", []), list) and str(item).strip()
                    ],
                    *[
                        str(item).strip().lower()
                        for item in cycle_record.get("preferred_wave_actions", [])
                        if isinstance(cycle_record.get("preferred_wave_actions", []), list) and str(item).strip()
                    ],
                    *[
                        str(item).strip().lower()
                        for item in (
                            dict(result.get("targeting", {})).get("preferred_wave_actions", [])
                            if isinstance(result.get("targeting", {}), dict)
                            and isinstance(dict(result.get("targeting", {})).get("preferred_wave_actions", []), list)
                            else []
                        )
                        if str(item).strip()
                    ],
                ]
            )[:8]
            campaign["adaptive_preferred_wave_actions"] = adaptive_preferred_wave_actions
            campaign["preferred_traversal_paths"] = self._dedupe_strings(
                [
                    *[
                        str(item).strip().lower()
                        for item in campaign.get("preferred_traversal_paths", [])
                        if isinstance(campaign.get("preferred_traversal_paths", []), list) and str(item).strip()
                    ],
                    *[
                        str(item).strip().lower()
                        for item in cycle_record.get("preferred_traversal_paths", [])
                        if isinstance(cycle_record.get("preferred_traversal_paths", []), list) and str(item).strip()
                    ],
                ]
            )[:8]
            campaign["adaptive_preferred_traversal_paths"] = adaptive_preferred_traversal_paths
            campaign["recommended_traversal_paths"] = self._dedupe_strings(
                [
                    *[
                        str(item).strip().lower()
                        for item in campaign.get("recommended_traversal_paths", [])
                        if isinstance(campaign.get("recommended_traversal_paths", []), list) and str(item).strip()
                    ],
                    *[
                        str(item).strip().lower()
                        for item in cycle_record.get("recommended_traversal_paths", [])
                        if isinstance(cycle_record.get("recommended_traversal_paths", []), list) and str(item).strip()
                    ],
                    *[
                        str(item).strip().lower()
                        for item in (
                            dict(result.get("targeting", {})).get("recommended_traversal_paths", [])
                            if isinstance(result.get("targeting", {}), dict)
                            and isinstance(dict(result.get("targeting", {})).get("recommended_traversal_paths", []), list)
                            else []
                        )
                        if str(item).strip()
                    ],
                ]
            )[:8]
            campaign["adaptive_runtime_strategy_counts"] = {
                str(key): int(value)
                for key, value in sorted(cycle_runtime_strategy_counts.items(), key=lambda entry: entry[0])
            }
            campaign["runtime_band_counts"] = {
                str(key): int(value)
                for key, value in sorted(cycle_runtime_band_counts.items(), key=lambda entry: entry[0])
            }
            campaign["route_profile_counts"] = {
                str(key).strip().lower(): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                for key, value in actual_route_profile_counts.items()
                if str(key).strip()
            }
            campaign["model_preference_counts"] = {
                str(key).strip().lower(): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                for key, value in actual_model_preference_counts.items()
                if str(key).strip()
            }
            campaign["provider_source_counts"] = {
                str(key).strip().lower(): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                for key, value in actual_provider_source_counts.items()
                if str(key).strip()
            }
            campaign["route_fallback_app_count"] = route_fallback_app_count
            campaign["effective_max_surface_waves"] = effective_max_surface_waves
            campaign["adaptive_surface_wave_depth"] = adaptive_surface_wave_depth
            campaign["traversed_container_roles"] = self._dedupe_strings(
                [
                    *[
                        str(item).strip().lower()
                        for item in campaign.get("traversed_container_roles", [])
                        if isinstance(campaign.get("traversed_container_roles", []), list) and str(item).strip()
                    ],
                    *[
                        str(item).strip().lower()
                        for item in cycle_record.get("traversed_container_roles", [])
                        if isinstance(cycle_record.get("traversed_container_roles", []), list) and str(item).strip()
                    ],
                ]
            )[:8]
            campaign["executed_traversal_paths"] = self._dedupe_strings(
                [
                    *[
                        str(item).strip().lower()
                        for item in campaign.get("executed_traversal_paths", [])
                        if isinstance(campaign.get("executed_traversal_paths", []), list) and str(item).strip()
                    ],
                    *[
                        str(item).strip().lower()
                        for item in cycle_record.get("executed_traversal_paths", [])
                        if isinstance(cycle_record.get("executed_traversal_paths", []), list) and str(item).strip()
                    ],
                ]
            )[:8]
            campaign["preferred_path_hits"] = self._coerce_int(campaign.get("preferred_path_hits", 0), minimum=0, maximum=1_000_000, default=0) + self._coerce_int(cycle_record.get("preferred_path_hits", 0), minimum=0, maximum=1_000_000, default=0)
            campaign["traversal_path_execution_count"] = self._coerce_int(campaign.get("traversal_path_execution_count", 0), minimum=0, maximum=1_000_000, default=0) + self._coerce_int(cycle_record.get("traversal_path_execution_count", 0), minimum=0, maximum=1_000_000, default=0)
            merged_role_attempt_counts = (
                dict(campaign.get("role_attempt_counts", {}))
                if isinstance(campaign.get("role_attempt_counts", {}), dict)
                else {}
            )
            cycle_role_attempt_counts = (
                dict(cycle_record.get("role_attempt_counts", {}))
                if isinstance(cycle_record.get("role_attempt_counts", {}), dict)
                else {}
            )
            for key, value in cycle_role_attempt_counts.items():
                clean_key = self._normalize_name(key)
                if not clean_key:
                    continue
                merged_role_attempt_counts[clean_key] = self._coerce_int(merged_role_attempt_counts.get(clean_key, 0), minimum=0, maximum=1_000_000, default=0) + self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
            campaign["role_attempt_counts"] = merged_role_attempt_counts
            merged_role_learned_counts = (
                dict(campaign.get("role_learned_counts", {}))
                if isinstance(campaign.get("role_learned_counts", {}), dict)
                else {}
            )
            cycle_role_learned_counts = (
                dict(cycle_record.get("role_learned_counts", {}))
                if isinstance(cycle_record.get("role_learned_counts", {}), dict)
                else {}
            )
            for key, value in cycle_role_learned_counts.items():
                clean_key = self._normalize_name(key)
                if not clean_key:
                    continue
                merged_role_learned_counts[clean_key] = self._coerce_int(merged_role_learned_counts.get(clean_key, 0), minimum=0, maximum=1_000_000, default=0) + self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
            campaign["role_learned_counts"] = merged_role_learned_counts
            campaign["revalidation_focus_summary"] = {
                "top_container_roles": [
                    dict(item)
                    for item in cycle_target_summary.get("top_revalidation_container_roles", [])
                    if isinstance(cycle_target_summary.get("top_revalidation_container_roles", []), list) and isinstance(item, dict)
                ][:6],
                "top_reason_codes": [
                    dict(item)
                    for item in cycle_target_summary.get("top_revalidation_reason_codes", [])
                    if isinstance(cycle_target_summary.get("top_revalidation_reason_codes", []), list) and isinstance(item, dict)
                ][:8],
                "top_preferred_wave_actions": [
                    dict(item)
                    for item in cycle_target_summary.get("top_preferred_wave_actions", [])
                    if isinstance(cycle_target_summary.get("top_preferred_wave_actions", []), list) and isinstance(item, dict)
                ][:6],
                "top_recommended_traversal_paths": [
                    dict(item)
                    for item in cycle_target_summary.get("top_recommended_traversal_paths", [])
                    if isinstance(cycle_target_summary.get("top_recommended_traversal_paths", []), list) and isinstance(item, dict)
                ][:6],
            }
            campaign["reseed_count"] = self._coerce_int(campaign.get("reseed_count", 0), minimum=0, maximum=1_000_000, default=0) + (1 if reseed_summary else 0)
            campaign["stale_reseed_count"] = self._coerce_int(campaign.get("stale_reseed_count", 0), minimum=0, maximum=1_000_000, default=0) + self._coerce_int(reseed_summary.get("stale_count", 0), minimum=0, maximum=1_000_000, default=0)
            campaign["revisit_app_count"] = self._coerce_int(campaign.get("revisit_app_count", 0), minimum=0, maximum=1_000_000, default=0) + self._coerce_int(reseed_summary.get("revisit_app_count", 0), minimum=0, maximum=1_000_000, default=0)
            campaign["cycle_generation"] = self._coerce_int(campaign.get("cycle_generation", 0), minimum=0, maximum=1_000_000, default=0) + (1 if reseed_summary else 0)
            if reseed_summary:
                campaign["latest_reseed_reason"] = str(reseed_summary.get("selection_strategy", "") or "").strip()
                campaign["latest_reseed_summary"] = copy.deepcopy(reseed_summary)
            campaign["run_count"] = self._coerce_int(campaign.get("run_count", 0), minimum=0, maximum=1_000_000, default=0) + 1
            campaign["latest_cycle_status"] = cycle_record["status"]
            campaign["latest_cycle_message"] = cycle_record["message"]
            campaign["latest_cycle_at"] = cycle_record["executed_at"]
            campaign["latest_cycle_source"] = cycle_record["source"]
            campaign_history = [dict(item) for item in campaign.get("history", []) if isinstance(item, dict)]
            campaign_history.append(cycle_record)
            campaign["history"] = campaign_history[-16:]
            campaign["status"] = (
                "completed"
                if not next_pending and not failed_apps and not partial_apps
                else ("attention" if failed_apps or partial_apps else "active")
            )
            campaign["updated_at"] = _utc_now_iso()
            self._apply_campaign_counts_locked(campaign)
            self._campaigns[str(campaign.get("campaign_id", "") or "").strip()] = campaign
            self._runtime["updated_at"] = _utc_now_iso()
            self._persist_locked()
            return {
                "status": str(result.get("status", "") or "success").strip().lower() or "success",
                "message": cycle_record["message"] or "desktop app memory campaign cycle completed",
                "result": result,
                "reseed": copy.deepcopy(reseed_summary),
                "campaign": copy.deepcopy(campaign),
                "campaigns": self.campaigns(limit=8),
            }

    def configure(
        self,
        *,
        enabled: Optional[bool] = None,
        interval_s: Optional[float] = None,
        max_apps: Optional[int] = None,
        per_app_limit: Optional[int] = None,
        history_limit: Optional[int] = None,
        query: Optional[str] = None,
        category: Optional[str] = None,
        ensure_app_launch: Optional[bool] = None,
        probe_controls: Optional[bool] = None,
        max_probe_controls: Optional[int] = None,
        follow_surface_waves: Optional[bool] = None,
        max_surface_waves: Optional[int] = None,
        allow_risky_probes: Optional[bool] = None,
        skip_known_apps: Optional[bool] = None,
        prefer_unknown_apps: Optional[bool] = None,
        continuous_learning: Optional[bool] = None,
        revisit_stale_apps: Optional[bool] = None,
        stale_after_hours: Optional[float] = None,
        revisit_failed_apps: Optional[bool] = None,
        revalidate_known_controls: Optional[bool] = None,
        prioritize_failure_hotspots: Optional[bool] = None,
        target_container_roles: Optional[list[str]] = None,
        preferred_wave_actions: Optional[list[str]] = None,
        preferred_traversal_paths: Optional[list[str]] = None,
        source: str = "manual",
    ) -> Dict[str, Any]:
        with self._lock:
            if enabled is not None:
                self._config["enabled"] = bool(enabled)
            if interval_s is not None:
                self._config["interval_s"] = self._coerce_float(interval_s, minimum=10.0, maximum=3600.0, default=300.0)
            if max_apps is not None:
                self._config["max_apps"] = self._coerce_int(max_apps, minimum=1, maximum=32, default=2)
            if per_app_limit is not None:
                self._config["per_app_limit"] = self._coerce_int(per_app_limit, minimum=4, maximum=80, default=24)
            if history_limit is not None:
                self._config["history_limit"] = self._coerce_int(history_limit, minimum=1, maximum=64, default=8)
            if query is not None:
                self._config["query"] = str(query or "").strip()
            if category is not None:
                self._config["category"] = str(category or "").strip()
            if ensure_app_launch is not None:
                self._config["ensure_app_launch"] = bool(ensure_app_launch)
            if probe_controls is not None:
                self._config["probe_controls"] = bool(probe_controls)
            if max_probe_controls is not None:
                self._config["max_probe_controls"] = self._coerce_int(max_probe_controls, minimum=1, maximum=12, default=4)
            if follow_surface_waves is not None:
                self._config["follow_surface_waves"] = bool(follow_surface_waves)
            if max_surface_waves is not None:
                self._config["max_surface_waves"] = self._coerce_int(max_surface_waves, minimum=1, maximum=8, default=3)
            if allow_risky_probes is not None:
                self._config["allow_risky_probes"] = bool(allow_risky_probes)
            if skip_known_apps is not None:
                self._config["skip_known_apps"] = bool(skip_known_apps)
            if prefer_unknown_apps is not None:
                self._config["prefer_unknown_apps"] = bool(prefer_unknown_apps)
            if continuous_learning is not None:
                self._config["continuous_learning"] = bool(continuous_learning)
            if revisit_stale_apps is not None:
                self._config["revisit_stale_apps"] = bool(revisit_stale_apps)
            if stale_after_hours is not None:
                self._config["stale_after_hours"] = self._coerce_float(
                    stale_after_hours,
                    minimum=4.0,
                    maximum=720.0,
                    default=72.0,
                )
            if revisit_failed_apps is not None:
                self._config["revisit_failed_apps"] = bool(revisit_failed_apps)
            if revalidate_known_controls is not None:
                self._config["revalidate_known_controls"] = bool(revalidate_known_controls)
            if prioritize_failure_hotspots is not None:
                self._config["prioritize_failure_hotspots"] = bool(prioritize_failure_hotspots)
            if target_container_roles is not None:
                self._config["target_container_roles"] = self._dedupe_strings(
                    [str(item).strip().lower() for item in target_container_roles if str(item).strip()]
                )[:8]
            if preferred_wave_actions is not None:
                self._config["preferred_wave_actions"] = self._dedupe_strings(
                    [str(item).strip().lower() for item in preferred_wave_actions if str(item).strip()]
                )[:8]
            if preferred_traversal_paths is not None:
                self._config["preferred_traversal_paths"] = self._dedupe_strings(
                    [str(item).strip().lower() for item in preferred_traversal_paths if str(item).strip()]
                )[:8]
            self._runtime["last_config_source"] = str(source or "manual").strip().lower() or "manual"
            self._runtime["updated_at"] = _utc_now_iso()
            self._persist_locked()
            payload = self._public_status_locked()
        self._wakeup.set()
        return payload

    def trigger_now(
        self,
        *,
        app_names: Optional[list[str]] = None,
        max_apps: Optional[int] = None,
        per_app_limit: Optional[int] = None,
        history_limit: Optional[int] = None,
        query: Optional[str] = None,
        category: Optional[str] = None,
        ensure_app_launch: Optional[bool] = None,
        probe_controls: Optional[bool] = None,
        max_probe_controls: Optional[int] = None,
        follow_surface_waves: Optional[bool] = None,
        max_surface_waves: Optional[int] = None,
        allow_risky_probes: Optional[bool] = None,
        skip_known_apps: Optional[bool] = None,
        prefer_unknown_apps: Optional[bool] = None,
        continuous_learning: Optional[bool] = None,
        revisit_stale_apps: Optional[bool] = None,
        stale_after_hours: Optional[float] = None,
        revisit_failed_apps: Optional[bool] = None,
        revalidate_known_controls: Optional[bool] = None,
        prioritize_failure_hotspots: Optional[bool] = None,
        target_container_roles: Optional[list[str]] = None,
        preferred_wave_actions: Optional[list[str]] = None,
        preferred_traversal_paths: Optional[list[str]] = None,
        source: str = "manual",
    ) -> Dict[str, Any]:
        with self._lock:
            payload = self._execute_locked(
                source=source,
                app_names=app_names,
                max_apps=max_apps,
                per_app_limit=per_app_limit,
                history_limit=history_limit,
                query=query,
                category=category,
                ensure_app_launch=ensure_app_launch,
                probe_controls=probe_controls,
                max_probe_controls=max_probe_controls,
                follow_surface_waves=follow_surface_waves,
                max_surface_waves=max_surface_waves,
                allow_risky_probes=allow_risky_probes,
                skip_known_apps=skip_known_apps,
                prefer_unknown_apps=prefer_unknown_apps,
                continuous_learning=continuous_learning,
                revisit_stale_apps=revisit_stale_apps,
                stale_after_hours=stale_after_hours,
                revisit_failed_apps=revisit_failed_apps,
                revalidate_known_controls=revalidate_known_controls,
                prioritize_failure_hotspots=prioritize_failure_hotspots,
                target_container_roles=target_container_roles,
                preferred_wave_actions=preferred_wave_actions,
                preferred_traversal_paths=preferred_traversal_paths,
            )
        self._wakeup.set()
        return payload

    def _worker(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                interval_s = self._coerce_float(self._config.get("interval_s", 300.0), minimum=10.0, maximum=3600.0, default=300.0)
                enabled = bool(self._config.get("enabled", False))
                next_due_at = self._runtime.get("next_due_at_ts", 0.0)
                should_run = enabled and (not self._runtime.get("inflight", False)) and time.time() >= float(next_due_at or 0.0)
            if should_run:
                try:
                    with self._lock:
                        self._execute_locked(source="daemon")
                except Exception:
                    pass
            self._wakeup.wait(timeout=interval_s)
            self._wakeup.clear()

    def _execute_locked(
        self,
        *,
        source: str,
        app_names: Optional[list[str]] = None,
        max_apps: Optional[int] = None,
        per_app_limit: Optional[int] = None,
        history_limit: Optional[int] = None,
        query: Optional[str] = None,
        category: Optional[str] = None,
        ensure_app_launch: Optional[bool] = None,
        probe_controls: Optional[bool] = None,
        max_probe_controls: Optional[int] = None,
        follow_surface_waves: Optional[bool] = None,
        max_surface_waves: Optional[int] = None,
        allow_risky_probes: Optional[bool] = None,
        skip_known_apps: Optional[bool] = None,
        prefer_unknown_apps: Optional[bool] = None,
        continuous_learning: Optional[bool] = None,
        revisit_stale_apps: Optional[bool] = None,
        stale_after_hours: Optional[float] = None,
        revisit_failed_apps: Optional[bool] = None,
        revalidate_known_controls: Optional[bool] = None,
        prioritize_failure_hotspots: Optional[bool] = None,
        target_container_roles: Optional[list[str]] = None,
        preferred_wave_actions: Optional[list[str]] = None,
        preferred_traversal_paths: Optional[list[str]] = None,
    ) -> Dict[str, Any]:
        callback = self._execute_callback
        if callback is None:
            return {"status": "unavailable", "message": "desktop app memory supervisor callback unavailable"}

        max_apps_value = self._coerce_int(
            max_apps if max_apps is not None else self._config.get("max_apps", 2),
            minimum=1,
            maximum=32,
            default=2,
        )
        per_app_limit_value = self._coerce_int(
            per_app_limit if per_app_limit is not None else self._config.get("per_app_limit", 24),
            minimum=4,
            maximum=80,
            default=24,
        )
        history_limit_value = self._coerce_int(
            history_limit if history_limit is not None else self._config.get("history_limit", 8),
            minimum=1,
            maximum=64,
            default=8,
        )
        query_value = str(query if query is not None else self._config.get("query", "") or "").strip()
        category_value = str(category if category is not None else self._config.get("category", "") or "").strip()
        app_names_value = self._dedupe_strings([str(item).strip() for item in (app_names or []) if str(item).strip()])
        ensure_launch_value = bool(
            self._config.get("ensure_app_launch", True)
            if ensure_app_launch is None
            else ensure_app_launch
        )
        probe_controls_value = bool(
            self._config.get("probe_controls", True)
            if probe_controls is None
            else probe_controls
        )
        max_probe_controls_value = self._coerce_int(
            max_probe_controls if max_probe_controls is not None else self._config.get("max_probe_controls", 4),
            minimum=1,
            maximum=12,
            default=4,
        )
        follow_surface_waves_value = bool(
            self._config.get("follow_surface_waves", True)
            if follow_surface_waves is None
            else follow_surface_waves
        )
        max_surface_waves_value = self._coerce_int(
            max_surface_waves if max_surface_waves is not None else self._config.get("max_surface_waves", 3),
            minimum=1,
            maximum=8,
            default=3,
        )
        allow_risky_probes_value = bool(
            self._config.get("allow_risky_probes", False)
            if allow_risky_probes is None
            else allow_risky_probes
        )
        skip_known_apps_value = bool(
            self._config.get("skip_known_apps", True)
            if skip_known_apps is None
            else skip_known_apps
        )
        prefer_unknown_apps_value = bool(
            self._config.get("prefer_unknown_apps", True)
            if prefer_unknown_apps is None
            else prefer_unknown_apps
        )
        continuous_learning_value = bool(
            self._config.get("continuous_learning", True)
            if continuous_learning is None
            else continuous_learning
        )
        revisit_stale_apps_value = bool(
            self._config.get("revisit_stale_apps", True)
            if revisit_stale_apps is None
            else revisit_stale_apps
        )
        stale_after_hours_value = self._coerce_float(
            stale_after_hours if stale_after_hours is not None else self._config.get("stale_after_hours", 72.0),
            minimum=4.0,
            maximum=720.0,
            default=72.0,
        )
        revisit_failed_apps_value = bool(
            self._config.get("revisit_failed_apps", True)
            if revisit_failed_apps is None
            else revisit_failed_apps
        )
        revalidate_known_controls_value = bool(
            self._config.get("revalidate_known_controls", True)
            if revalidate_known_controls is None
            else revalidate_known_controls
        )
        prioritize_failure_hotspots_value = bool(
            self._config.get("prioritize_failure_hotspots", True)
            if prioritize_failure_hotspots is None
            else prioritize_failure_hotspots
        )
        target_container_roles_value = self._dedupe_strings(
            [
                str(item).strip().lower()
                for item in (
                    target_container_roles
                    if target_container_roles is not None
                    else self._config.get("target_container_roles", [])
                )
                if str(item).strip()
            ]
        )[:8]
        preferred_wave_actions_value = self._dedupe_strings(
            [
                str(item).strip().lower()
                for item in (
                    preferred_wave_actions
                    if preferred_wave_actions is not None
                    else self._config.get("preferred_wave_actions", [])
                )
                if str(item).strip()
            ]
        )[:8]
        preferred_traversal_paths_value = self._dedupe_strings(
            [
                str(item).strip().lower()
                for item in (
                    preferred_traversal_paths
                    if preferred_traversal_paths is not None
                    else self._config.get("preferred_traversal_paths", [])
                )
                if str(item).strip()
            ]
        )[:8]
        explicit_target_roles = bool(
            target_container_roles is not None
            or (
                target_container_roles is None
                and isinstance(self._config.get("target_container_roles", []), list)
                and any(str(item).strip() for item in self._config.get("target_container_roles", []))
            )
        )
        adaptive_target_container_roles_value = False
        adaptive_preferred_wave_actions_value = False
        effective_max_surface_waves_value = max_surface_waves_value
        adaptive_surface_wave_depth_value = False

        selection_summary: Dict[str, Any] = {}
        if not app_names_value and continuous_learning_value:
            selection_summary = self._select_supervisor_targets_locked(
                max_apps=max_apps_value,
                query=query_value,
                category=category_value,
                revisit_stale_apps=revisit_stale_apps_value,
                stale_after_hours=stale_after_hours_value,
                revisit_failed_apps=revisit_failed_apps_value,
                revalidate_known_controls=revalidate_known_controls_value,
                prioritize_failure_hotspots=prioritize_failure_hotspots_value,
                target_container_roles=target_container_roles_value,
            )
            selected_apps = selection_summary.get("selected_apps", []) if isinstance(selection_summary, dict) else []
            if isinstance(selected_apps, list) and selected_apps:
                app_names_value = self._dedupe_strings(
                    [str(item).strip() for item in selected_apps if str(item).strip()]
                )
                skip_known_apps_value = False
            if not target_container_roles_value:
                target_container_roles_value = self._dedupe_strings(
                    [
                        str(item.get("value", "") or "").strip().lower()
                        for item in selection_summary.get("top_revalidation_container_roles", [])
                        if isinstance(selection_summary.get("top_revalidation_container_roles", []), list)
                        and isinstance(item, dict)
                        and str(item.get("value", "") or "").strip()
                    ]
                )[:4] if isinstance(selection_summary, dict) else []
                adaptive_target_container_roles_value = bool(target_container_roles_value)
            if not preferred_wave_actions_value:
                preferred_wave_actions_value, adaptive_preferred_wave_actions_value = (
                    self._adaptive_preferred_wave_actions_from_summary(selection_summary)
                )
            if not preferred_traversal_paths_value:
                preferred_traversal_paths_value = self._dedupe_strings(
                    [
                        str(item.get("value", "") or "").strip().lower()
                        for item in selection_summary.get("top_recommended_traversal_paths", [])
                        if isinstance(selection_summary.get("top_recommended_traversal_paths", []), list)
                        and isinstance(item, dict)
                        and str(item.get("value", "") or "").strip()
                    ]
                )[:8] if isinstance(selection_summary, dict) else []
            if follow_surface_waves_value:
                effective_max_surface_waves_value, adaptive_surface_wave_depth_value = self._adaptive_campaign_wave_depth(
                    max_surface_waves_value,
                    target_container_roles=target_container_roles_value,
                    summary=selection_summary,
                    explicit_roles=explicit_target_roles,
                )

        started_at = time.time()
        started_iso = _iso_from_ts(started_at)
        self._runtime["inflight"] = True
        self._runtime["last_trigger_source"] = str(source or "manual").strip().lower() or "manual"
        self._runtime["last_trigger_at"] = started_iso
        self._runtime["updated_at"] = _utc_now_iso()
        self._persist_locked()

        try:
            result = callback(
                max_apps=max_apps_value,
                per_app_limit=per_app_limit_value,
                app_names=app_names_value,
                query=query_value,
                category=category_value,
                ensure_app_launch=ensure_launch_value,
                probe_controls=probe_controls_value,
                max_probe_controls=max_probe_controls_value,
                follow_surface_waves=follow_surface_waves_value,
                max_surface_waves=effective_max_surface_waves_value,
                allow_risky_probes=allow_risky_probes_value,
                skip_known_apps=skip_known_apps_value,
                prefer_unknown_apps=prefer_unknown_apps_value,
                target_container_roles=target_container_roles_value,
                preferred_wave_actions=preferred_wave_actions_value,
                preferred_traversal_paths=preferred_traversal_paths_value,
                revalidate_known_controls=revalidate_known_controls_value,
                prefer_failure_memory=prioritize_failure_hotspots_value,
                source=str(source or "manual").strip().lower() or "manual",
            )
        except Exception as exc:  # noqa: BLE001
            result = {"status": "error", "message": str(exc)}

        finished_at = time.time()
        duration_ms = round((finished_at - started_at) * 1000.0, 3)
        status = str(result.get("status", "") or "error").strip().lower() or "error"
        history_record = {
            "started_at": started_iso,
            "completed_at": _iso_from_ts(finished_at),
            "duration_ms": duration_ms,
            "source": str(source or "manual").strip().lower() or "manual",
            "status": status,
            "message": str(result.get("message", "") or "").strip(),
            "surveyed_app_count": self._coerce_int(result.get("surveyed_app_count", 0), minimum=0, maximum=1_000_000, default=0),
            "success_count": self._coerce_int(result.get("success_count", 0), minimum=0, maximum=1_000_000, default=0),
            "partial_count": self._coerce_int(result.get("partial_count", 0), minimum=0, maximum=1_000_000, default=0),
            "error_count": self._coerce_int(result.get("error_count", 0), minimum=0, maximum=1_000_000, default=0),
            "query": query_value,
            "category": category_value,
            "app_names": app_names_value[:16],
            "max_apps": max_apps_value,
            "ensure_app_launch": ensure_launch_value,
            "probe_controls": probe_controls_value,
            "max_probe_controls": max_probe_controls_value,
            "follow_surface_waves": follow_surface_waves_value,
            "max_surface_waves": max_surface_waves_value,
            "allow_risky_probes": allow_risky_probes_value,
            "skip_known_apps": skip_known_apps_value,
            "prefer_unknown_apps": prefer_unknown_apps_value,
            "continuous_learning": continuous_learning_value,
            "revisit_stale_apps": revisit_stale_apps_value,
            "stale_after_hours": round(stale_after_hours_value, 4),
            "revisit_failed_apps": revisit_failed_apps_value,
            "revalidate_known_controls": revalidate_known_controls_value,
            "prioritize_failure_hotspots": prioritize_failure_hotspots_value,
            "target_container_roles": target_container_roles_value[:8],
            "preferred_wave_actions": preferred_wave_actions_value[:8],
            "adaptive_preferred_wave_actions": adaptive_preferred_wave_actions_value,
            "preferred_traversal_paths": preferred_traversal_paths_value[:8],
            "adaptive_target_container_roles": adaptive_target_container_roles_value,
            "effective_max_surface_waves": effective_max_surface_waves_value,
            "adaptive_surface_wave_depth": adaptive_surface_wave_depth_value,
            "skipped_app_count": self._coerce_int(result.get("skipped_app_count", 0), minimum=0, maximum=1_000_000, default=0),
            "wave_attempt_count": self._wave_summary_metric(
                dict(result.get("wave_summary", {})) if isinstance(result.get("wave_summary", {}), dict) else {},
                primary_key="wave_attempt_total",
                fallback_key="attempted_count",
            ),
            "learned_surface_count": self._wave_summary_metric(
                dict(result.get("wave_summary", {})) if isinstance(result.get("wave_summary", {}), dict) else {},
                primary_key="learned_surface_total",
                fallback_key="learned_surface_count",
            ),
            "known_surface_count": self._wave_summary_metric(
                dict(result.get("wave_summary", {})) if isinstance(result.get("wave_summary", {}), dict) else {},
                primary_key="known_surface_total",
                fallback_key="known_surface_count",
            ),
            "preferred_path_hits": self._coerce_int(
                dict(result.get("wave_summary", {})).get("preferred_path_hits", 0)
                if isinstance(result.get("wave_summary", {}), dict)
                else 0,
                minimum=0,
                maximum=1_000_000,
                default=0,
            ),
            "traversal_path_execution_count": self._coerce_int(
                dict(result.get("wave_summary", {})).get("traversal_path_execution_count", 0)
                if isinstance(result.get("wave_summary", {}), dict)
                else 0,
                minimum=0,
                maximum=1_000_000,
                default=0,
            ),
            "traversed_container_roles": self._dedupe_strings(
                [
                    str(item).strip().lower()
                    for item in dict(result.get("wave_summary", {})).get("traversed_container_roles", [])
                    if isinstance(result.get("wave_summary", {}), dict)
                    and isinstance(dict(result.get("wave_summary", {})).get("traversed_container_roles", []), list)
                    and str(item).strip()
                ]
            )[:8],
            "role_attempt_counts": {
                self._normalize_name(key): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                for key, value in (
                    dict(dict(result.get("wave_summary", {})).get("role_attempt_counts", {})).items()
                    if isinstance(result.get("wave_summary", {}), dict)
                    and isinstance(dict(result.get("wave_summary", {})).get("role_attempt_counts", {}), dict)
                    else []
                )
                if self._normalize_name(key)
            },
            "role_learned_counts": {
                self._normalize_name(key): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                for key, value in (
                    dict(dict(result.get("wave_summary", {})).get("role_learned_counts", {})).items()
                    if isinstance(result.get("wave_summary", {}), dict)
                    and isinstance(dict(result.get("wave_summary", {})).get("role_learned_counts", {}), dict)
                    else []
                )
                    if self._normalize_name(key)
                },
            "executed_traversal_paths": self._dedupe_strings(
                [
                    str(item).strip().lower()
                    for item in dict(result.get("wave_summary", {})).get("executed_traversal_paths", [])
                    if isinstance(result.get("wave_summary", {}), dict)
                    and isinstance(dict(result.get("wave_summary", {})).get("executed_traversal_paths", []), list)
                    and str(item).strip()
                ]
            )[:8],
            "selection_strategy": str(
                selection_summary.get("selection_strategy", "explicit_targets" if app_names_value else "catalog_batch")
                or "catalog_batch"
            ),
            "stale_candidate_count": self._coerce_int(selection_summary.get("stale_count", 0), minimum=0, maximum=1_000_000, default=0),
            "attention_candidate_count": self._coerce_int(selection_summary.get("attention_count", 0), minimum=0, maximum=1_000_000, default=0),
            "failure_candidate_count": self._coerce_int(selection_summary.get("failure_memory_count", 0), minimum=0, maximum=1_000_000, default=0),
            "revalidation_candidate_count": self._coerce_int(selection_summary.get("revalidation_count", 0), minimum=0, maximum=1_000_000, default=0),
            "unknown_candidate_count": self._coerce_int(selection_summary.get("unknown_count", 0), minimum=0, maximum=1_000_000, default=0),
            "revisit_app_count": self._coerce_int(selection_summary.get("revisit_app_count", 0), minimum=0, maximum=1_000_000, default=0),
            "selection_summary": copy.deepcopy(selection_summary),
            "failed_apps": [
                dict(item)
                for item in result.get("failed_apps", [])
                if isinstance(item, dict)
            ][:8],
            "skipped_apps": [
                dict(item)
                for item in result.get("skipped_apps", [])
                if isinstance(item, dict)
            ][:12],
        }
        self._history.append(history_record)
        history_cap = self._coerce_int(self._config.get("history_limit", 8), minimum=1, maximum=64, default=8)
        self._history = self._history[-history_cap:]
        self._runtime["inflight"] = False
        self._runtime["last_tick_at"] = history_record["completed_at"]
        self._runtime["last_duration_ms"] = duration_ms
        self._runtime["last_result_status"] = status
        self._runtime["last_result_message"] = str(result.get("message", "") or "").strip()
        self._runtime["last_success_at"] = history_record["completed_at"] if status in {"success", "partial"} else str(self._runtime.get("last_success_at", "") or "")
        self._runtime["last_error_at"] = history_record["completed_at"] if status == "error" else str(self._runtime.get("last_error_at", "") or "")
        self._runtime["run_count"] = self._coerce_int(self._runtime.get("run_count", 0), minimum=0, maximum=1_000_000, default=0) + 1
        if history_record["source"] == "daemon":
            self._runtime["auto_trigger_count"] = self._coerce_int(self._runtime.get("auto_trigger_count", 0), minimum=0, maximum=1_000_000, default=0) + 1
        else:
            self._runtime["manual_trigger_count"] = self._coerce_int(self._runtime.get("manual_trigger_count", 0), minimum=0, maximum=1_000_000, default=0) + 1
        self._runtime["consecutive_error_count"] = (
            self._coerce_int(self._runtime.get("consecutive_error_count", 0), minimum=0, maximum=1_000_000, default=0) + 1
            if status == "error"
            else 0
        )
        self._runtime["last_summary"] = {
            "surveyed_app_count": history_record["surveyed_app_count"],
            "success_count": history_record["success_count"],
            "partial_count": history_record["partial_count"],
            "error_count": history_record["error_count"],
            "skipped_app_count": history_record["skipped_app_count"],
            "wave_attempt_count": history_record["wave_attempt_count"],
            "learned_surface_count": history_record["learned_surface_count"],
            "known_surface_count": history_record["known_surface_count"],
            "preferred_wave_actions": history_record["preferred_wave_actions"],
            "effective_max_surface_waves": history_record["effective_max_surface_waves"],
            "adaptive_surface_wave_depth": history_record["adaptive_surface_wave_depth"],
            "preferred_path_hits": history_record["preferred_path_hits"],
            "traversal_path_execution_count": history_record["traversal_path_execution_count"],
        }
        self._runtime["next_due_at_ts"] = finished_at + self._coerce_float(self._config.get("interval_s", 300.0), minimum=10.0, maximum=3600.0, default=300.0)
        self._runtime["next_due_at"] = _iso_from_ts(self._runtime["next_due_at_ts"])
        self._runtime["updated_at"] = _utc_now_iso()
        self._persist_locked()
        response = dict(result)
        response["supervisor"] = self._public_status_locked(history_limit=history_limit_value)
        return response

    def _public_status_locked(self, *, history_limit: Optional[int] = None) -> Dict[str, Any]:
        limit = self._coerce_int(
            history_limit if history_limit is not None else self._config.get("history_limit", 8),
            minimum=1,
            maximum=64,
            default=8,
        )
        return {
            "status": "success",
            "enabled": bool(self._config.get("enabled", False)),
            "active": bool(self._thread and self._thread.is_alive()),
            "inflight": bool(self._runtime.get("inflight", False)),
            "interval_s": self._coerce_float(self._config.get("interval_s", 300.0), minimum=10.0, maximum=3600.0, default=300.0),
            "max_apps": self._coerce_int(self._config.get("max_apps", 2), minimum=1, maximum=32, default=2),
            "per_app_limit": self._coerce_int(self._config.get("per_app_limit", 24), minimum=4, maximum=80, default=24),
            "history_limit": self._coerce_int(self._config.get("history_limit", 8), minimum=1, maximum=64, default=8),
            "query": str(self._config.get("query", "") or "").strip(),
            "category": str(self._config.get("category", "") or "").strip(),
            "ensure_app_launch": bool(self._config.get("ensure_app_launch", True)),
            "probe_controls": bool(self._config.get("probe_controls", True)),
            "max_probe_controls": self._coerce_int(self._config.get("max_probe_controls", 4), minimum=1, maximum=12, default=4),
            "follow_surface_waves": bool(self._config.get("follow_surface_waves", True)),
            "max_surface_waves": self._coerce_int(self._config.get("max_surface_waves", 3), minimum=1, maximum=8, default=3),
            "allow_risky_probes": bool(self._config.get("allow_risky_probes", False)),
            "skip_known_apps": bool(self._config.get("skip_known_apps", True)),
            "prefer_unknown_apps": bool(self._config.get("prefer_unknown_apps", True)),
            "continuous_learning": bool(self._config.get("continuous_learning", True)),
            "revisit_stale_apps": bool(self._config.get("revisit_stale_apps", True)),
            "stale_after_hours": self._coerce_float(self._config.get("stale_after_hours", 72.0), minimum=4.0, maximum=720.0, default=72.0),
            "revisit_failed_apps": bool(self._config.get("revisit_failed_apps", True)),
            "revalidate_known_controls": bool(self._config.get("revalidate_known_controls", True)),
            "prioritize_failure_hotspots": bool(self._config.get("prioritize_failure_hotspots", True)),
            "target_container_roles": [
                str(item).strip().lower()
                for item in self._config.get("target_container_roles", [])
                if isinstance(self._config.get("target_container_roles", []), list) and str(item).strip()
            ][:8],
            "preferred_wave_actions": [
                str(item).strip().lower()
                for item in self._config.get("preferred_wave_actions", [])
                if isinstance(self._config.get("preferred_wave_actions", []), list) and str(item).strip()
            ][:8],
            "preferred_traversal_paths": [
                str(item).strip().lower()
                for item in self._config.get("preferred_traversal_paths", [])
                if isinstance(self._config.get("preferred_traversal_paths", []), list) and str(item).strip()
            ][:8],
            "last_tick_at": str(self._runtime.get("last_tick_at", "") or ""),
            "last_success_at": str(self._runtime.get("last_success_at", "") or ""),
            "last_error_at": str(self._runtime.get("last_error_at", "") or ""),
            "last_duration_ms": float(self._runtime.get("last_duration_ms", 0.0) or 0.0),
            "last_result_status": str(self._runtime.get("last_result_status", "") or ""),
            "last_result_message": str(self._runtime.get("last_result_message", "") or ""),
            "last_trigger_source": str(self._runtime.get("last_trigger_source", "") or ""),
            "last_trigger_at": str(self._runtime.get("last_trigger_at", "") or ""),
            "last_config_source": str(self._runtime.get("last_config_source", "") or ""),
            "next_due_at": str(self._runtime.get("next_due_at", "") or ""),
            "run_count": self._coerce_int(self._runtime.get("run_count", 0), minimum=0, maximum=1_000_000, default=0),
            "manual_trigger_count": self._coerce_int(self._runtime.get("manual_trigger_count", 0), minimum=0, maximum=1_000_000, default=0),
            "auto_trigger_count": self._coerce_int(self._runtime.get("auto_trigger_count", 0), minimum=0, maximum=1_000_000, default=0),
            "consecutive_error_count": self._coerce_int(self._runtime.get("consecutive_error_count", 0), minimum=0, maximum=1_000_000, default=0),
            "last_summary": copy.deepcopy(self._runtime.get("last_summary", {})),
            "updated_at": str(self._runtime.get("updated_at", "") or ""),
            "latest_run": copy.deepcopy(self._history[-1]) if self._history else {},
            "history": self.history(limit=limit),
            "campaigns": self.campaigns(limit=min(8, limit)),
        }

    def _persist_locked(self) -> None:
        self._store.set(
            "desktop_app_memory_supervisor",
            {
                "config": copy.deepcopy(self._config),
                "runtime": copy.deepcopy(self._runtime),
                "history": copy.deepcopy(self._history),
                "campaigns": copy.deepcopy(self._campaigns),
            },
        )

    def _load(self) -> None:
        payload = self._store.get("desktop_app_memory_supervisor", default={})
        if not isinstance(payload, dict):
            return
        config = payload.get("config", {}) if isinstance(payload.get("config", {}), dict) else {}
        runtime = payload.get("runtime", {}) if isinstance(payload.get("runtime", {}), dict) else {}
        history = payload.get("history", []) if isinstance(payload.get("history", []), list) else []
        campaigns = payload.get("campaigns", {}) if isinstance(payload.get("campaigns", {}), dict) else {}
        self._config.update({
            "enabled": bool(config.get("enabled", self._config["enabled"])),
            "interval_s": self._coerce_float(config.get("interval_s", self._config["interval_s"]), minimum=10.0, maximum=3600.0, default=300.0),
            "max_apps": self._coerce_int(config.get("max_apps", self._config["max_apps"]), minimum=1, maximum=32, default=2),
            "per_app_limit": self._coerce_int(config.get("per_app_limit", self._config["per_app_limit"]), minimum=4, maximum=80, default=24),
            "history_limit": self._coerce_int(config.get("history_limit", self._config["history_limit"]), minimum=1, maximum=64, default=8),
            "query": str(config.get("query", self._config["query"]) or "").strip(),
            "category": str(config.get("category", self._config["category"]) or "").strip(),
            "ensure_app_launch": bool(config.get("ensure_app_launch", self._config["ensure_app_launch"])),
            "probe_controls": bool(config.get("probe_controls", self._config["probe_controls"])),
            "max_probe_controls": self._coerce_int(config.get("max_probe_controls", self._config["max_probe_controls"]), minimum=1, maximum=12, default=4),
            "follow_surface_waves": bool(config.get("follow_surface_waves", self._config["follow_surface_waves"])),
            "max_surface_waves": self._coerce_int(config.get("max_surface_waves", self._config["max_surface_waves"]), minimum=1, maximum=8, default=3),
            "allow_risky_probes": bool(config.get("allow_risky_probes", self._config["allow_risky_probes"])),
            "skip_known_apps": bool(config.get("skip_known_apps", self._config["skip_known_apps"])),
            "prefer_unknown_apps": bool(config.get("prefer_unknown_apps", self._config["prefer_unknown_apps"])),
            "continuous_learning": bool(config.get("continuous_learning", self._config["continuous_learning"])),
            "revisit_stale_apps": bool(config.get("revisit_stale_apps", self._config["revisit_stale_apps"])),
            "stale_after_hours": self._coerce_float(config.get("stale_after_hours", self._config["stale_after_hours"]), minimum=4.0, maximum=720.0, default=72.0),
            "revisit_failed_apps": bool(config.get("revisit_failed_apps", self._config["revisit_failed_apps"])),
            "revalidate_known_controls": bool(config.get("revalidate_known_controls", self._config["revalidate_known_controls"])),
            "prioritize_failure_hotspots": bool(config.get("prioritize_failure_hotspots", self._config["prioritize_failure_hotspots"])),
            "target_container_roles": self._dedupe_strings(
                [
                    str(item).strip().lower()
                    for item in config.get("target_container_roles", self._config["target_container_roles"])
                    if isinstance(config.get("target_container_roles", self._config["target_container_roles"]), list) and str(item).strip()
                ]
            )[:8],
            "preferred_wave_actions": self._dedupe_strings(
                [
                    str(item).strip().lower()
                    for item in config.get("preferred_wave_actions", self._config["preferred_wave_actions"])
                    if isinstance(config.get("preferred_wave_actions", self._config["preferred_wave_actions"]), list) and str(item).strip()
                ]
            )[:8],
            "preferred_traversal_paths": self._dedupe_strings(
                [
                    str(item).strip().lower()
                    for item in config.get("preferred_traversal_paths", self._config["preferred_traversal_paths"])
                    if isinstance(config.get("preferred_traversal_paths", self._config["preferred_traversal_paths"]), list) and str(item).strip()
                ]
            )[:8],
        })
        self._runtime.update({
            "last_tick_at": str(runtime.get("last_tick_at", "") or ""),
            "last_success_at": str(runtime.get("last_success_at", "") or ""),
            "last_error_at": str(runtime.get("last_error_at", "") or ""),
            "last_duration_ms": float(runtime.get("last_duration_ms", 0.0) or 0.0),
            "last_result_status": str(runtime.get("last_result_status", "") or ""),
            "last_result_message": str(runtime.get("last_result_message", "") or ""),
            "last_trigger_source": str(runtime.get("last_trigger_source", "") or ""),
            "last_trigger_at": str(runtime.get("last_trigger_at", "") or ""),
            "last_config_source": str(runtime.get("last_config_source", "") or ""),
            "next_due_at": str(runtime.get("next_due_at", "") or ""),
            "next_due_at_ts": float(runtime.get("next_due_at_ts", 0.0) or 0.0),
            "run_count": self._coerce_int(runtime.get("run_count", 0), minimum=0, maximum=1_000_000, default=0),
            "manual_trigger_count": self._coerce_int(runtime.get("manual_trigger_count", 0), minimum=0, maximum=1_000_000, default=0),
            "auto_trigger_count": self._coerce_int(runtime.get("auto_trigger_count", 0), minimum=0, maximum=1_000_000, default=0),
            "consecutive_error_count": self._coerce_int(runtime.get("consecutive_error_count", 0), minimum=0, maximum=1_000_000, default=0),
            "last_summary": copy.deepcopy(runtime.get("last_summary", {})),
            "updated_at": str(runtime.get("updated_at", "") or ""),
        })
        self._history = [dict(item) for item in history if isinstance(item, dict)][-self._coerce_int(self._config.get("history_limit", 8), minimum=1, maximum=64, default=8) :]
        self._campaigns = {
            str(key).strip(): dict(value)
            for key, value in campaigns.items()
            if str(key).strip() and isinstance(value, dict)
        }
        for item in self._campaigns.values():
            self._apply_campaign_counts_locked(item)

    @staticmethod
    def _default_config(
        *,
        enabled: bool,
        interval_s: float,
        max_apps: int,
        per_app_limit: int,
        history_limit: int,
        query: str,
        category: str,
        ensure_app_launch: bool,
        probe_controls: bool,
        max_probe_controls: int,
        follow_surface_waves: bool,
        max_surface_waves: int,
        allow_risky_probes: bool,
        skip_known_apps: bool,
        prefer_unknown_apps: bool,
        continuous_learning: bool,
        revisit_stale_apps: bool,
        stale_after_hours: float,
        revisit_failed_apps: bool,
        revalidate_known_controls: bool,
        prioritize_failure_hotspots: bool,
        target_container_roles: list[str],
        preferred_wave_actions: list[str],
        preferred_traversal_paths: list[str],
    ) -> Dict[str, Any]:
        return {
            "enabled": bool(enabled),
            "interval_s": float(interval_s),
            "max_apps": int(max_apps),
            "per_app_limit": int(per_app_limit),
            "history_limit": int(history_limit),
            "query": str(query or "").strip(),
            "category": str(category or "").strip(),
            "ensure_app_launch": bool(ensure_app_launch),
            "probe_controls": bool(probe_controls),
            "max_probe_controls": int(max_probe_controls),
            "follow_surface_waves": bool(follow_surface_waves),
            "max_surface_waves": int(max_surface_waves),
            "allow_risky_probes": bool(allow_risky_probes),
            "skip_known_apps": bool(skip_known_apps),
            "prefer_unknown_apps": bool(prefer_unknown_apps),
            "continuous_learning": bool(continuous_learning),
            "revisit_stale_apps": bool(revisit_stale_apps),
            "stale_after_hours": float(stale_after_hours),
            "revisit_failed_apps": bool(revisit_failed_apps),
            "revalidate_known_controls": bool(revalidate_known_controls),
            "prioritize_failure_hotspots": bool(prioritize_failure_hotspots),
            "target_container_roles": [
                str(item).strip().lower()
                for item in target_container_roles
                if str(item).strip()
            ][:8],
            "preferred_wave_actions": [
                str(item).strip().lower()
                for item in preferred_wave_actions
                if str(item).strip()
            ][:8],
            "preferred_traversal_paths": [
                str(item).strip().lower()
                for item in preferred_traversal_paths
                if str(item).strip()
            ][:8],
        }

    @staticmethod
    def _default_runtime() -> Dict[str, Any]:
        return {
            "inflight": False,
            "last_tick_at": "",
            "last_success_at": "",
            "last_error_at": "",
            "last_duration_ms": 0.0,
            "last_result_status": "idle",
            "last_result_message": "",
            "last_trigger_source": "",
            "last_trigger_at": "",
            "last_config_source": "",
            "next_due_at": "",
            "next_due_at_ts": 0.0,
            "run_count": 0,
            "manual_trigger_count": 0,
            "auto_trigger_count": 0,
            "consecutive_error_count": 0,
            "last_summary": {},
            "updated_at": "",
        }

    @staticmethod
    def _dedupe_strings(values: list[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for raw in values:
            clean = str(raw or "").strip()
            normalized = clean.lower()
            if not clean or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(clean)
        return ordered

    @classmethod
    def _adaptive_target_container_roles_from_summary(
        cls,
        summary: Dict[str, Any],
        *,
        requested_roles: Optional[list[str]] = None,
    ) -> tuple[list[str], bool]:
        explicit_roles = cls._dedupe_strings(
            [str(item).strip().lower() for item in (requested_roles or []) if str(item).strip()]
        )[:8]
        if explicit_roles:
            return (explicit_roles, False)
        if not isinstance(summary, dict):
            return ([], False)
        derived = cls._dedupe_strings(
            [
                str(item.get("value", "") or "").strip().lower()
                for item in summary.get("top_revalidation_container_roles", [])
                if isinstance(summary.get("top_revalidation_container_roles", []), list)
                and isinstance(item, dict)
                and str(item.get("value", "") or "").strip()
            ]
        )[:4]
        return (derived, bool(derived))

    @classmethod
    def _adaptive_preferred_wave_actions_from_summary(cls, summary: Dict[str, Any]) -> tuple[list[str], bool]:
        if not isinstance(summary, dict):
            return ([], False)
        derived = cls._dedupe_strings(
            [
                str(item.get("value", "") or "").strip().lower()
                for item in summary.get("top_preferred_wave_actions", [])
                if isinstance(summary.get("top_preferred_wave_actions", []), list)
                and isinstance(item, dict)
                and str(item.get("value", "") or "").strip()
            ]
        )[:6]
        return (derived, bool(derived))

    @classmethod
    def _adaptive_campaign_wave_depth(
        cls,
        base_max_surface_waves: int,
        *,
        target_container_roles: list[str],
        summary: Dict[str, Any],
        explicit_roles: bool,
    ) -> tuple[int, bool]:
        bounded_base = cls._coerce_int(base_max_surface_waves, minimum=1, maximum=8, default=3)
        if explicit_roles:
            return (bounded_base, False)
        role_set = {str(item).strip().lower() for item in target_container_roles if str(item).strip()}
        revalidation_count = cls._coerce_int(summary.get("revalidation_count", 0), minimum=0, maximum=1_000_000, default=0)
        failure_count = cls._coerce_int(summary.get("failure_memory_count", 0), minimum=0, maximum=1_000_000, default=0)
        attention_count = cls._coerce_int(summary.get("attention_count", 0), minimum=0, maximum=1_000_000, default=0)
        depth_bonus = 0
        if role_set.intersection({"menu", "tab", "toolbar", "ribbon"}):
            depth_bonus = max(depth_bonus, 1)
        if role_set.intersection({"tree", "sidebar", "table"}):
            depth_bonus = max(depth_bonus, 2)
        if "dialog" in role_set:
            depth_bonus = max(depth_bonus, 3)
        if revalidation_count >= 4:
            depth_bonus = max(depth_bonus, 2)
        if failure_count > 0 or attention_count > 0:
            depth_bonus = max(depth_bonus, 2)
        effective = max(bounded_base, min(8, bounded_base + depth_bonus))
        return (effective, bool(effective > bounded_base))

    def _wave_summary_metric(
        self,
        summary: Dict[str, Any],
        *,
        primary_key: str,
        fallback_key: str,
    ) -> int:
        if not isinstance(summary, dict):
            return 0
        if primary_key in summary:
            return self._coerce_int(summary.get(primary_key, 0), minimum=0, maximum=1_000_000, default=0)
        return self._coerce_int(summary.get(fallback_key, 0), minimum=0, maximum=1_000_000, default=0)

    @staticmethod
    def _campaign_id(*, label: str, app_names: list[str]) -> str:
        slug = "".join(
            character.lower() if character.isalnum() else "-"
            for character in (str(label or "").strip() or "app-memory-campaign")
        ).strip("-")
        compact_slug = "-".join(part for part in slug.split("-") if part) or "app-memory-campaign"
        return f"cam_{compact_slug}_{time.time_ns()}"

    def _apply_campaign_counts_locked(self, campaign: Dict[str, Any]) -> None:
        target_apps = [str(item).strip() for item in campaign.get("target_apps", []) if str(item).strip()]
        pending_apps = [str(item).strip() for item in campaign.get("pending_apps", []) if str(item).strip()]
        completed_apps = [str(item).strip() for item in campaign.get("completed_apps", []) if str(item).strip()]
        partial_apps = [dict(item) for item in campaign.get("partial_apps", []) if isinstance(item, dict)]
        failed_apps = [dict(item) for item in campaign.get("failed_apps", []) if isinstance(item, dict)]
        skipped_apps = [dict(item) for item in campaign.get("skipped_apps", []) if isinstance(item, dict)]
        history_items = [dict(item) for item in campaign.get("history", []) if isinstance(item, dict)]
        campaign["target_app_count"] = len(target_apps)
        campaign["pending_app_count"] = len(pending_apps)
        campaign["completed_app_count"] = len(completed_apps)
        campaign["partial_app_count"] = len(partial_apps)
        campaign["failed_app_count"] = len(failed_apps)
        campaign["skipped_app_count"] = len(skipped_apps)
        campaign["wave_attempt_count"] = sum(
            self._coerce_int(item.get("wave_attempt_count", 0), minimum=0, maximum=1_000_000, default=0)
            for item in history_items
        )
        campaign["learned_surface_count"] = sum(
            self._coerce_int(item.get("learned_surface_count", 0), minimum=0, maximum=1_000_000, default=0)
            for item in history_items
        )
        campaign["known_surface_count"] = sum(
            self._coerce_int(item.get("known_surface_count", 0), minimum=0, maximum=1_000_000, default=0)
            for item in history_items
        )
        campaign["reseed_count"] = self._coerce_int(campaign.get("reseed_count", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["stale_reseed_count"] = self._coerce_int(campaign.get("stale_reseed_count", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["revisit_app_count"] = self._coerce_int(campaign.get("revisit_app_count", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["max_surface_waves"] = self._coerce_int(campaign.get("max_surface_waves", 3), minimum=1, maximum=8, default=3)
        campaign["effective_max_surface_waves"] = self._coerce_int(
            campaign.get("effective_max_surface_waves", campaign.get("max_surface_waves", 3)),
            minimum=1,
            maximum=8,
            default=3,
        )
        campaign["adaptive_surface_wave_depth"] = bool(campaign.get("adaptive_surface_wave_depth", False))
        campaign["adaptive_target_container_roles"] = bool(campaign.get("adaptive_target_container_roles", False))
        campaign["adaptive_preferred_wave_actions"] = bool(campaign.get("adaptive_preferred_wave_actions", False))
        campaign["adaptive_preferred_traversal_paths"] = bool(campaign.get("adaptive_preferred_traversal_paths", False))
        campaign["target_container_roles"] = self._dedupe_strings(
            [str(item).strip().lower() for item in campaign.get("target_container_roles", []) if isinstance(campaign.get("target_container_roles", []), list) and str(item).strip()]
        )[:8]
        campaign["preferred_wave_actions"] = self._dedupe_strings(
            [str(item).strip().lower() for item in campaign.get("preferred_wave_actions", []) if isinstance(campaign.get("preferred_wave_actions", []), list) and str(item).strip()]
        )[:8]
        campaign["preferred_traversal_paths"] = self._dedupe_strings(
            [str(item).strip().lower() for item in campaign.get("preferred_traversal_paths", []) if isinstance(campaign.get("preferred_traversal_paths", []), list) and str(item).strip()]
        )[:8]
        campaign["recommended_traversal_paths"] = self._dedupe_strings(
            [str(item).strip().lower() for item in campaign.get("recommended_traversal_paths", []) if isinstance(campaign.get("recommended_traversal_paths", []), list) and str(item).strip()]
        )[:8]
        campaign["traversed_container_roles"] = self._dedupe_strings(
            [
                str(item).strip().lower()
                for item in campaign.get("traversed_container_roles", [])
                if isinstance(campaign.get("traversed_container_roles", []), list) and str(item).strip()
            ]
        )[:8]
        campaign["executed_traversal_paths"] = self._dedupe_strings(
            [
                str(item).strip().lower()
                for item in campaign.get("executed_traversal_paths", [])
                if isinstance(campaign.get("executed_traversal_paths", []), list) and str(item).strip()
            ]
        )[:8]
        campaign["role_attempt_counts"] = {
            self._normalize_name(key): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
            for key, value in (
                dict(campaign.get("role_attempt_counts", {})).items()
                if isinstance(campaign.get("role_attempt_counts", {}), dict)
                else []
            )
            if self._normalize_name(key)
        }
        campaign["role_learned_counts"] = {
            self._normalize_name(key): self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
            for key, value in (
                dict(campaign.get("role_learned_counts", {})).items()
                if isinstance(campaign.get("role_learned_counts", {}), dict)
                else []
            )
            if self._normalize_name(key)
        }
        campaign["preferred_path_hits"] = self._coerce_int(campaign.get("preferred_path_hits", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["traversal_path_execution_count"] = self._coerce_int(campaign.get("traversal_path_execution_count", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["unknown_target_count"] = self._coerce_int(campaign.get("unknown_target_count", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["stale_target_count"] = self._coerce_int(campaign.get("stale_target_count", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["attention_target_count"] = self._coerce_int(campaign.get("attention_target_count", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["failure_target_count"] = self._coerce_int(campaign.get("failure_target_count", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["revalidation_target_count"] = self._coerce_int(campaign.get("revalidation_target_count", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["healthy_target_count"] = self._coerce_int(campaign.get("healthy_target_count", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["cycle_generation"] = self._coerce_int(campaign.get("cycle_generation", 0), minimum=0, maximum=1_000_000, default=0)
        campaign["target_selection_summary"] = (
            dict(campaign.get("target_selection_summary", {}))
            if isinstance(campaign.get("target_selection_summary", {}), dict)
            else {}
        )
        campaign["revalidation_focus_summary"] = (
            dict(campaign.get("revalidation_focus_summary", {}))
            if isinstance(campaign.get("revalidation_focus_summary", {}), dict)
            else {}
        )
        campaign["latest_reseed_summary"] = (
            dict(campaign.get("latest_reseed_summary", {}))
            if isinstance(campaign.get("latest_reseed_summary", {}), dict)
            else {}
        )

    @staticmethod
    def _increment_count(mapping: Dict[str, int], key: str) -> None:
        clean = str(key or "").strip().lower()
        if not clean:
            return
        mapping[clean] = int(mapping.get(clean, 0)) + 1

    def _reseed_campaign_pending_locked(self, campaign: Dict[str, Any]) -> Dict[str, Any]:
        if not bool(campaign.get("continuous_learning", True)):
            return {}
        target_apps = [str(item).strip() for item in campaign.get("target_apps", []) if str(item).strip()]
        if not target_apps:
            return {}
        stale_after_hours = self._coerce_float(
            campaign.get("stale_after_hours", self._config.get("stale_after_hours", 72.0)),
            minimum=4.0,
            maximum=720.0,
            default=72.0,
        )
        target_summary = self._classify_target_apps_locked(
            target_apps,
            category=str(campaign.get("category", "") or "").strip(),
            stale_after_hours=stale_after_hours,
            prefer_unknown_apps=bool(campaign.get("prefer_unknown_apps", True)),
            prioritize_failure_hotspots=bool(campaign.get("prioritize_failure_hotspots", True)),
            target_container_roles=[
                str(item).strip().lower()
                for item in campaign.get("target_container_roles", [])
                if isinstance(campaign.get("target_container_roles", []), list) and str(item).strip()
            ],
        )
        completed_lookup = {
            str(item).strip().lower()
            for item in campaign.get("completed_apps", [])
            if str(item).strip()
        }
        reasons: Dict[str, str] = {}
        ordered: list[str] = []

        def _enqueue(values: list[str], reason: str) -> None:
            for raw in values:
                clean = str(raw or "").strip()
                lowered = clean.lower()
                if not clean or lowered in reasons:
                    continue
                reasons[lowered] = reason
                ordered.append(clean)

        if bool(campaign.get("revisit_failed_apps", True)):
            _enqueue(self._campaign_retry_candidates(campaign.get("failed_apps", []), allow_healthy_retry=True), "retry_failed")
            _enqueue(self._campaign_retry_candidates(campaign.get("partial_apps", []), allow_healthy_retry=True), "retry_partial")
            _enqueue(self._campaign_retry_candidates(campaign.get("skipped_apps", []), allow_healthy_retry=False), "retry_skipped")

        if bool(campaign.get("revalidate_known_controls", True)):
            _enqueue(
                [str(item).strip() for item in target_summary.get("revalidation_apps", []) if str(item).strip()]
                if isinstance(target_summary.get("revalidation_apps", []), list)
                else [],
                "revalidation",
            )
        unknown_candidates = [
            str(item).strip()
            for item in target_summary.get("unknown_apps", [])
            if str(item).strip() and str(item).strip().lower() not in completed_lookup
        ] if isinstance(target_summary.get("unknown_apps", []), list) else []
        _enqueue(unknown_candidates, "unknown")
        if bool(campaign.get("revisit_stale_apps", True)):
            _enqueue(
                [str(item).strip() for item in target_summary.get("stale_apps", []) if str(item).strip()]
                if isinstance(target_summary.get("stale_apps", []), list)
                else [],
                "stale",
            )
        _enqueue(
            [str(item).strip() for item in target_summary.get("attention_apps", []) if str(item).strip()]
            if isinstance(target_summary.get("attention_apps", []), list)
            else [],
            "attention",
        )
        _enqueue(
            [str(item).strip() for item in target_summary.get("failure_memory_apps", []) if str(item).strip()]
            if isinstance(target_summary.get("failure_memory_apps", []), list)
            else [],
            "failure_memory",
        )
        if not ordered:
            return {}
        reason_counts: Dict[str, int] = {}
        for value in reasons.values():
            reason_counts[value] = int(reason_counts.get(value, 0) or 0) + 1
        known_lookup = {
            str(item).strip().lower()
            for item in target_summary.get("known_apps", [])
            if str(item).strip()
        } if isinstance(target_summary.get("known_apps", []), list) else set()
        campaign["pending_apps"] = ordered
        summary = {
            "selection_strategy": "campaign_reseed",
            "ordered_apps": ordered[:],
            "reason_counts": reason_counts,
            "unknown_count": len(unknown_candidates),
            "revalidation_count": int(reason_counts.get("revalidation", 0) or 0),
            "stale_count": int(reason_counts.get("stale", 0) or 0),
            "attention_count": int(reason_counts.get("attention", 0) or 0),
            "failure_memory_count": int(reason_counts.get("failure_memory", 0) or 0),
            "retry_failed_count": int(reason_counts.get("retry_failed", 0) or 0),
            "retry_partial_count": int(reason_counts.get("retry_partial", 0) or 0),
            "retry_skipped_count": int(reason_counts.get("retry_skipped", 0) or 0),
            "revisit_app_count": sum(1 for item in ordered if str(item).strip().lower() in known_lookup),
            "top_revalidation_container_roles": [
                dict(item)
                for item in target_summary.get("top_revalidation_container_roles", [])
                if isinstance(target_summary.get("top_revalidation_container_roles", []), list) and isinstance(item, dict)
            ][:6],
            "top_revalidation_reason_codes": [
                dict(item)
                for item in target_summary.get("top_revalidation_reason_codes", [])
                if isinstance(target_summary.get("top_revalidation_reason_codes", []), list) and isinstance(item, dict)
            ][:8],
            "force_known_revisit": any(reason != "unknown" for reason in reasons.values()),
        }
        campaign["latest_reseed_reason"] = "campaign_reseed"
        campaign["latest_reseed_summary"] = copy.deepcopy(summary)
        return summary

    def _campaign_retry_candidates(self, raw_items: Any, *, allow_healthy_retry: bool) -> list[str]:
        if not isinstance(raw_items, list):
            return []
        ordered: list[str] = []
        seen: set[str] = set()
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            app_name = str(item.get("app_name", "") or "").strip()
            if not app_name:
                continue
            reason = str(item.get("reason", item.get("message", "")) or "").strip().lower()
            if (not allow_healthy_retry) and ("healthy" in reason or "known" in reason or "memory_reuse" in reason):
                continue
            lowered = app_name.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            ordered.append(app_name)
        return ordered

    def _select_supervisor_targets_locked(
        self,
        *,
        max_apps: int,
        query: str,
        category: str,
        revisit_stale_apps: bool,
        stale_after_hours: float,
        revisit_failed_apps: bool,
        revalidate_known_controls: bool,
        prioritize_failure_hotspots: bool,
        target_container_roles: list[str] | None = None,
    ) -> Dict[str, Any]:
        rows = self._memory_snapshot_rows_locked(limit=max(64, min(max_apps * 24, 512)), category=category)
        candidate_names = self._memory_candidate_app_names(rows, query=query, category=category)
        if not candidate_names:
            return {}
        target_summary = self._classify_target_apps_locked(
            candidate_names,
            category=category,
            stale_after_hours=stale_after_hours,
            prefer_unknown_apps=True,
            prioritize_failure_hotspots=prioritize_failure_hotspots,
            target_container_roles=target_container_roles,
        )
        candidates: list[str] = []
        if revalidate_known_controls:
            candidates.extend(
                [str(item).strip() for item in target_summary.get("revalidation_apps", []) if str(item).strip()]
                if isinstance(target_summary.get("revalidation_apps", []), list)
                else []
            )
        if revisit_stale_apps:
            candidates.extend(
                [str(item).strip() for item in target_summary.get("stale_apps", []) if str(item).strip()]
                if isinstance(target_summary.get("stale_apps", []), list)
                else []
            )
        candidates.extend(
            [str(item).strip() for item in target_summary.get("attention_apps", []) if str(item).strip()]
            if isinstance(target_summary.get("attention_apps", []), list)
            else []
        )
        if revisit_failed_apps:
            candidates.extend(
                [str(item).strip() for item in target_summary.get("failure_memory_apps", []) if str(item).strip()]
                if isinstance(target_summary.get("failure_memory_apps", []), list)
                else []
            )
        candidates = self._dedupe_strings(candidates)
        if not candidates:
            return {}
        selected = candidates[:max_apps]
        known_lookup = {
            str(item).strip().lower()
            for item in target_summary.get("known_apps", [])
            if str(item).strip()
        } if isinstance(target_summary.get("known_apps", []), list) else set()
        return {
            "selection_strategy": "revalidation_hotspot_revisit" if revalidate_known_controls and bool(target_summary.get("revalidation_apps", [])) else "stale_memory_revisit",
            "selected_apps": selected,
            "ordered_apps": candidates,
            "revalidation_count": len(target_summary.get("revalidation_apps", [])) if isinstance(target_summary.get("revalidation_apps", []), list) else 0,
            "stale_count": len(target_summary.get("stale_apps", [])) if isinstance(target_summary.get("stale_apps", []), list) else 0,
            "attention_count": len(target_summary.get("attention_apps", [])) if isinstance(target_summary.get("attention_apps", []), list) else 0,
            "failure_memory_count": len(target_summary.get("failure_memory_apps", [])) if isinstance(target_summary.get("failure_memory_apps", []), list) else 0,
            "unknown_count": len(target_summary.get("unknown_apps", [])) if isinstance(target_summary.get("unknown_apps", []), list) else 0,
            "revisit_app_count": sum(1 for item in selected if str(item).strip().lower() in known_lookup),
            "top_revalidation_container_roles": [
                dict(item)
                for item in target_summary.get("top_revalidation_container_roles", [])
                if isinstance(target_summary.get("top_revalidation_container_roles", []), list) and isinstance(item, dict)
            ][:6],
            "top_revalidation_reason_codes": [
                dict(item)
                for item in target_summary.get("top_revalidation_reason_codes", [])
                if isinstance(target_summary.get("top_revalidation_reason_codes", []), list) and isinstance(item, dict)
            ][:8],
            "top_preferred_wave_actions": [
                dict(item)
                for item in target_summary.get("top_preferred_wave_actions", [])
                if isinstance(target_summary.get("top_preferred_wave_actions", []), list) and isinstance(item, dict)
            ][:6],
            "top_recommended_traversal_paths": [
                dict(item)
                for item in target_summary.get("top_recommended_traversal_paths", [])
                if isinstance(target_summary.get("top_recommended_traversal_paths", []), list) and isinstance(item, dict)
            ][:8],
        }

    def _memory_snapshot_rows_locked(self, *, limit: int, category: str = "") -> list[Dict[str, Any]]:
        callback = self._memory_snapshot_callback
        if callback is None:
            return []
        try:
            payload = callback(limit=limit, category=category)
        except Exception:
            return []
        if not isinstance(payload, dict):
            return []
        return [
            dict(item)
            for item in payload.get("items", [])
            if isinstance(payload.get("items", []), list) and isinstance(item, dict)
        ]

    def _memory_candidate_app_names(self, rows: list[Dict[str, Any]], *, query: str, category: str) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        clean_query = self._normalize_name(query)
        clean_category = self._normalize_name(category)
        for row in rows:
            app_name = str(
                row.get("app_name", "")
                or row.get("profile_name", "")
                or row.get("profile_id", "")
                or ""
            ).strip()
            if not app_name:
                continue
            if clean_category:
                row_category = self._normalize_name(row.get("category", ""))
                if row_category and clean_category not in row_category:
                    continue
            if clean_query:
                haystack = " ".join(
                    part
                    for part in [
                        self._normalize_name(app_name),
                        self._normalize_name(row.get("profile_name", "")),
                        self._normalize_name(row.get("profile_id", "")),
                        self._normalize_name(row.get("window_title", "")),
                    ]
                    if part
                )
                if clean_query not in haystack:
                    continue
            lowered = app_name.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            ordered.append(app_name)
        return ordered

    def _classify_target_apps_locked(
        self,
        app_names: list[str],
        *,
        category: str,
        stale_after_hours: float,
        prefer_unknown_apps: bool,
        prioritize_failure_hotspots: bool,
        target_container_roles: list[str] | None = None,
    ) -> Dict[str, Any]:
        rows = self._memory_snapshot_rows_locked(limit=max(128, min(len(app_names) * 12, 640)), category=category)
        known_apps: list[str] = []
        unknown_apps: list[str] = []
        stale_apps: list[str] = []
        attention_apps: list[str] = []
        failure_memory_apps: list[str] = []
        revalidation_apps: list[str] = []
        healthy_apps: list[str] = []
        aggregate_revalidation_roles: dict[str, int] = {}
        aggregate_revalidation_reasons: dict[str, int] = {}
        aggregate_preferred_wave_actions: dict[str, int] = {}
        aggregate_traversal_paths: dict[str, int] = {}
        desired_roles = {
            self._normalize_name(item)
            for item in (target_container_roles or [])
            if self._normalize_name(item)
        }
        scored: list[tuple[float, str]] = []
        stale_threshold = max(4.0, float(stale_after_hours or 72.0))
        for app_name in self._dedupe_strings(app_names):
            matched_rows = [row for row in rows if self._app_name_matches_row(app_name, row)]
            known = bool(matched_rows)
            stale = False
            attention = False
            healthy = False
            failure_count = 0
            revalidation_count = 0
            revalidation_priority = 0.0
            revalidation_role_match_count = 0
            age_hours = 0.0
            for row in matched_rows:
                staleness = row.get("staleness", {}) if isinstance(row.get("staleness", {}), dict) else {}
                age_hours = max(age_hours, float(staleness.get("age_hours", 0.0) or 0.0))
                stale = stale or bool(staleness.get("stale", False)) or age_hours >= stale_threshold
                health = row.get("learning_health", {}) if isinstance(row.get("learning_health", {}), dict) else {}
                status = str(health.get("status", "") or "").strip().lower()
                attention = attention or status in {"degraded", "attention"}
                healthy = healthy or status == "healthy"
                failure_summary = row.get("failure_memory_summary", {}) if isinstance(row.get("failure_memory_summary", {}), dict) else {}
                failure_count = max(
                    failure_count,
                    self._coerce_int(failure_summary.get("entry_count", 0), minimum=0, maximum=1_000_000, default=0),
                )
                revalidation_summary = row.get("revalidation_summary", {}) if isinstance(row.get("revalidation_summary", {}), dict) else {}
                wave_strategy_summary = row.get("wave_strategy_summary", {}) if isinstance(row.get("wave_strategy_summary", {}), dict) else {}
                safe_traversal_summary = row.get("safe_traversal_summary", {}) if isinstance(row.get("safe_traversal_summary", {}), dict) else {}
                revalidation_count = max(
                    revalidation_count,
                    self._coerce_int(revalidation_summary.get("target_count", 0), minimum=0, maximum=1_000_000, default=0),
                )
                revalidation_priority = max(
                    revalidation_priority,
                    float(revalidation_summary.get("priority_total", 0.0) or 0.0),
                )
                if isinstance(revalidation_summary.get("top_container_roles", []), list):
                    for item in revalidation_summary.get("top_container_roles", []):
                        if not isinstance(item, dict):
                            continue
                        role_value = self._normalize_name(item.get("value", ""))
                        count_value = self._coerce_int(item.get("count", 0), minimum=0, maximum=1_000_000, default=0)
                        if not role_value or count_value <= 0:
                            continue
                        aggregate_revalidation_roles[role_value] = int(aggregate_revalidation_roles.get(role_value, 0) or 0) + count_value
                        if desired_roles and role_value in desired_roles:
                            revalidation_role_match_count += count_value
                if isinstance(revalidation_summary.get("reason_counts", {}), dict):
                    for key, value in revalidation_summary.get("reason_counts", {}).items():
                        reason_value = str(key or "").strip().lower()
                        count_value = self._coerce_int(value, minimum=0, maximum=1_000_000, default=0)
                        if not reason_value or count_value <= 0:
                            continue
                        aggregate_revalidation_reasons[reason_value] = int(aggregate_revalidation_reasons.get(reason_value, 0) or 0) + count_value
                for action_name in (
                    wave_strategy_summary.get("recommended_actions", [])
                    if isinstance(wave_strategy_summary.get("recommended_actions", []), list)
                    else []
                ):
                    normalized_action = str(action_name or "").strip().lower()
                    if not normalized_action:
                        continue
                    aggregate_preferred_wave_actions[normalized_action] = int(
                        aggregate_preferred_wave_actions.get(normalized_action, 0) or 0
                    ) + 1
                traversal_candidates = [
                    *(
                        [
                            str(item).strip().lower()
                            for item in safe_traversal_summary.get("recommended_paths", [])
                            if isinstance(safe_traversal_summary.get("recommended_paths", []), list)
                            and str(item).strip()
                        ]
                    ),
                    *(
                        [
                            str(item).strip().lower()
                            for item in wave_strategy_summary.get("recommended_container_roles", [])
                            if isinstance(wave_strategy_summary.get("recommended_container_roles", []), list)
                            and str(item).strip()
                        ]
                    ),
                    *(
                        [
                            str(item.get("value", "") or "").strip().lower()
                            for item in wave_strategy_summary.get("top_followup_roles", [])
                            if isinstance(wave_strategy_summary.get("top_followup_roles", []), list)
                            and isinstance(item, dict)
                            and str(item.get("value", "") or "").strip()
                        ]
                    ),
                ]
                for traversal_name in traversal_candidates:
                    if not traversal_name:
                        continue
                    aggregate_traversal_paths[traversal_name] = int(
                        aggregate_traversal_paths.get(traversal_name, 0) or 0
                    ) + 1
            priority = age_hours
            if known:
                known_apps.append(app_name)
            else:
                unknown_apps.append(app_name)
                priority += 500.0 if prefer_unknown_apps else 320.0
            if stale:
                stale_apps.append(app_name)
                priority += 260.0
            if attention:
                attention_apps.append(app_name)
                priority += 180.0
            if failure_count > 0:
                failure_memory_apps.append(app_name)
                priority += (120.0 + min(float(failure_count) * 16.0, 160.0)) if prioritize_failure_hotspots else 60.0
            if revalidation_count > 0:
                revalidation_apps.append(app_name)
                priority += min(260.0, 90.0 + float(revalidation_count) * 24.0 + min(revalidation_priority, 120.0))
            if revalidation_role_match_count > 0:
                priority += min(180.0, 70.0 + float(revalidation_role_match_count) * 22.0)
            if healthy and not stale and failure_count <= 0 and not attention:
                healthy_apps.append(app_name)
                priority -= 30.0
            scored.append((priority, app_name))
        scored.sort(key=lambda item: (item[0], item[1].lower()), reverse=True)
        return {
            "ordered_apps": [item[1] for item in scored],
            "known_apps": self._dedupe_strings(known_apps),
            "unknown_apps": self._dedupe_strings(unknown_apps),
            "stale_apps": self._dedupe_strings(stale_apps),
            "attention_apps": self._dedupe_strings(attention_apps),
            "failure_memory_apps": self._dedupe_strings(failure_memory_apps),
            "revalidation_apps": self._dedupe_strings(revalidation_apps),
            "healthy_apps": self._dedupe_strings(healthy_apps),
            "top_revalidation_container_roles": [
                {"value": str(key), "count": int(value)}
                for key, value in sorted(aggregate_revalidation_roles.items(), key=lambda item: (int(item[1]), str(item[0])), reverse=True)[:6]
            ],
            "top_revalidation_reason_codes": [
                {"value": str(key), "count": int(value)}
                for key, value in sorted(aggregate_revalidation_reasons.items(), key=lambda item: (int(item[1]), str(item[0])), reverse=True)[:8]
            ],
            "top_preferred_wave_actions": [
                {"value": str(key), "count": int(value)}
                for key, value in sorted(aggregate_preferred_wave_actions.items(), key=lambda item: (int(item[1]), str(item[0])), reverse=True)[:8]
            ],
            "top_recommended_traversal_paths": [
                {"value": str(key), "count": int(value)}
                for key, value in sorted(aggregate_traversal_paths.items(), key=lambda item: (int(item[1]), str(item[0])), reverse=True)[:8]
            ],
            "summary": {
                "known_count": len(self._dedupe_strings(known_apps)),
                "unknown_count": len(self._dedupe_strings(unknown_apps)),
                "stale_count": len(self._dedupe_strings(stale_apps)),
                "attention_count": len(self._dedupe_strings(attention_apps)),
                "failure_memory_count": len(self._dedupe_strings(failure_memory_apps)),
                "revalidation_count": len(self._dedupe_strings(revalidation_apps)),
                "healthy_count": len(self._dedupe_strings(healthy_apps)),
            },
        }

    @staticmethod
    def _normalize_name(value: Any) -> str:
        clean = "".join(character.lower() if character.isalnum() else " " for character in str(value or ""))
        return " ".join(part for part in clean.split() if part)

    def _app_name_matches_row(self, app_name: str, row: Dict[str, Any]) -> bool:
        target = self._normalize_name(app_name)
        if not target:
            return False
        for candidate in [
            row.get("app_name", ""),
            row.get("profile_name", ""),
            row.get("profile_id", ""),
            row.get("window_title", ""),
        ]:
            normalized = self._normalize_name(candidate)
            if normalized and (normalized == target or target in normalized or normalized in target):
                return True
        return False

    @staticmethod
    def _sorted_count_map(mapping: Dict[str, int]) -> Dict[str, int]:
        ordered = sorted(mapping.items(), key=lambda item: (int(item[1]), str(item[0])), reverse=True)
        return {str(key): int(value) for key, value in ordered}

    @staticmethod
    def _coerce_int(value: Any, *, minimum: int, maximum: int, default: int) -> int:
        try:
            numeric = int(value)
        except Exception:
            numeric = default
        return max(minimum, min(maximum, numeric))

    @staticmethod
    def _coerce_float(value: Any, *, minimum: float, maximum: float, default: float) -> float:
        try:
            numeric = float(value)
        except Exception:
            numeric = default
        return max(minimum, min(maximum, numeric))
