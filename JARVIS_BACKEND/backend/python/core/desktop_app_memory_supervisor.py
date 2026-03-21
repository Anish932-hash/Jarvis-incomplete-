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
    ) -> None:
        self._store = LocalStore(state_path)
        self._lock = threading.RLock()
        self._wakeup = threading.Event()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._execute_callback: Optional[Callable[..., Dict[str, Any]]] = None
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
        )
        self._runtime = self._default_runtime()
        self._history: list[Dict[str, Any]] = []
        self._campaigns: Dict[str, Dict[str, Any]] = {}
        self._load()

    def start(self, execute_callback: Callable[..., Dict[str, Any]]) -> None:
        with self._lock:
            self._execute_callback = execute_callback
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
            for item in items:
                self._increment_count(status_counts, str(item.get("status", "") or "unknown"))
                self._increment_count(source_counts, str(item.get("source", "") or "unknown"))
                surveyed_app_total += self._coerce_int(item.get("surveyed_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                success_total += self._coerce_int(item.get("success_count", 0), minimum=0, maximum=1_000_000, default=0)
                partial_total += self._coerce_int(item.get("partial_count", 0), minimum=0, maximum=1_000_000, default=0)
                error_total += self._coerce_int(item.get("error_count", 0), minimum=0, maximum=1_000_000, default=0)
                skipped_total += self._coerce_int(item.get("skipped_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                wave_attempt_total += self._coerce_int(item.get("wave_attempt_count", 0), minimum=0, maximum=1_000_000, default=0)
                learned_surface_total += self._coerce_int(item.get("learned_surface_count", 0), minimum=0, maximum=1_000_000, default=0)
                known_surface_total += self._coerce_int(item.get("known_surface_count", 0), minimum=0, maximum=1_000_000, default=0)
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
            for item in rows:
                self._increment_count(status_counts, str(item.get("status", "") or "unknown"))
                pending_total += self._coerce_int(item.get("pending_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                completed_total += self._coerce_int(item.get("completed_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                failed_total += self._coerce_int(item.get("failed_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                skipped_total += self._coerce_int(item.get("skipped_app_count", 0), minimum=0, maximum=1_000_000, default=0)
                wave_attempt_total += self._coerce_int(item.get("wave_attempt_count", 0), minimum=0, maximum=1_000_000, default=0)
                learned_surface_total += self._coerce_int(item.get("learned_surface_count", 0), minimum=0, maximum=1_000_000, default=0)
                known_surface_total += self._coerce_int(item.get("known_surface_count", 0), minimum=0, maximum=1_000_000, default=0)
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
        source: str = "manual",
    ) -> Dict[str, Any]:
        clean_apps = self._dedupe_strings([str(item).strip() for item in app_names if str(item).strip()])
        if not clean_apps:
            return {"status": "error", "message": "at least one app is required to create a learning campaign"}
        with self._lock:
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
                "target_apps": clean_apps,
                "pending_apps": clean_apps[:],
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
                "allow_risky_probes": bool(allow_risky_probes),
                "skip_known_apps": bool(skip_known_apps),
                "prefer_unknown_apps": bool(prefer_unknown_apps),
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
            if not pending_apps:
                campaign["status"] = "completed" if not campaign.get("failed_apps") and not campaign.get("partial_apps") else "attention"
                campaign["updated_at"] = _utc_now_iso()
                self._apply_campaign_counts_locked(campaign)
                self._persist_locked()
                return {
                    "status": "success",
                    "message": "desktop app memory campaign has no pending apps left",
                    "campaign": copy.deepcopy(campaign),
                    "campaigns": self.campaigns(limit=8),
                }

            batch_size = self._coerce_int(
                max_apps if max_apps is not None else campaign.get("max_apps", 4),
                minimum=1,
                maximum=32,
                default=4,
            )
            target_batch = pending_apps[:batch_size]
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
                max_surface_waves=self._coerce_int(campaign.get("max_surface_waves", 3), minimum=1, maximum=8, default=3),
                allow_risky_probes=bool(campaign.get("allow_risky_probes", False)),
                skip_known_apps=bool(campaign.get("skip_known_apps", True)),
                prefer_unknown_apps=bool(campaign.get("prefer_unknown_apps", True)),
                source=str(source or "manual").strip().lower() or "manual",
            )
            result = dict(result) if isinstance(result, dict) else {"status": "error", "message": "invalid campaign execution payload"}
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
                "wave_attempt_count": self._coerce_int(dict(result.get("wave_summary", {})).get("wave_attempt_total", 0), minimum=0, maximum=1_000_000, default=0),
                "learned_surface_count": self._coerce_int(dict(result.get("wave_summary", {})).get("learned_surface_total", 0), minimum=0, maximum=1_000_000, default=0),
                "known_surface_count": self._coerce_int(dict(result.get("wave_summary", {})).get("known_surface_total", 0), minimum=0, maximum=1_000_000, default=0),
            }
            campaign["completed_apps"] = self._dedupe_strings(completed_apps)
            campaign["partial_apps"] = partial_apps[-32:]
            campaign["failed_apps"] = failed_apps[-32:]
            campaign["skipped_apps"] = skipped_apps[-32:]
            campaign["pending_apps"] = next_pending
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
                max_surface_waves=max_surface_waves_value,
                allow_risky_probes=allow_risky_probes_value,
                skip_known_apps=skip_known_apps_value,
                prefer_unknown_apps=prefer_unknown_apps_value,
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
            "skipped_app_count": self._coerce_int(result.get("skipped_app_count", 0), minimum=0, maximum=1_000_000, default=0),
            "wave_attempt_count": self._coerce_int(dict(result.get("wave_summary", {})).get("wave_attempt_total", 0), minimum=0, maximum=1_000_000, default=0),
            "learned_surface_count": self._coerce_int(dict(result.get("wave_summary", {})).get("learned_surface_total", 0), minimum=0, maximum=1_000_000, default=0),
            "known_surface_count": self._coerce_int(dict(result.get("wave_summary", {})).get("known_surface_total", 0), minimum=0, maximum=1_000_000, default=0),
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

    @staticmethod
    def _increment_count(mapping: Dict[str, int], key: str) -> None:
        clean = str(key or "").strip().lower()
        if not clean:
            return
        mapping[clean] = int(mapping.get(clean, 0)) + 1

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
