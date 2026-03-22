from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from backend.python.database.local_store import LocalStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class DesktopOnboardingManager:
    def __init__(self, *, store_path: str = "data/desktop_onboarding.json") -> None:
        self._store = LocalStore(store_path)

    def latest_run(self) -> Dict[str, Any]:
        payload = self._store.get("latest_run", {})
        return dict(payload) if isinstance(payload, dict) else {}

    def history(
        self,
        *,
        limit: int = 12,
        status: str = "",
        source: str = "",
    ) -> Dict[str, Any]:
        bounded = max(1, min(int(limit or 12), 128))
        clean_status = str(status or "").strip().lower()
        clean_source = str(source or "").strip().lower()
        rows = self._store.get("runs", [])
        items = [dict(item) for item in rows if isinstance(item, dict)] if isinstance(rows, list) else []
        if clean_status:
            items = [
                item
                for item in items
                if str(item.get("status", "") or "").strip().lower() == clean_status
            ]
        if clean_source:
            items = [
                item
                for item in items
                if str(item.get("source", "") or "").strip().lower() == clean_source
            ]
        limited = items[:bounded]
        status_counts: Dict[str, int] = {}
        source_counts: Dict[str, int] = {}
        prepared_app_total = 0
        prepared_blocked_total = 0
        prepared_degraded_total = 0
        provider_update_total = 0
        launch_seed_total = 0
        execution_action_total = 0
        execution_ready_total = 0
        execution_success_total = 0
        execution_manual_total = 0
        execution_blocked_total = 0
        execution_error_total = 0
        setup_action_total = 0
        setup_action_auto_runnable_total = 0
        setup_action_success_total = 0
        setup_action_manual_total = 0
        setup_action_blocked_total = 0
        profile_setup_action_total = 0
        setup_execution_selected_action_total = 0
        setup_execution_continued_action_total = 0
        setup_execution_remaining_ready_total = 0
        setup_execution_resume_ready_total = 0
        app_learning_setup_aligned_total = 0
        app_learning_setup_boosted_total = 0
        app_learning_setup_constrained_total = 0
        prepared_setup_aligned_total = 0
        prepared_setup_boosted_total = 0
        prepared_setup_constrained_total = 0
        route_remediation_total = 0
        route_remediation_blocked_total = 0
        route_remediation_degraded_total = 0
        route_remediation_setup_followup_total = 0
        route_remediation_provider_blocked_total = 0
        for item in items:
            status_name = str(item.get("status", "") or "unknown").strip().lower() or "unknown"
            source_name = str(item.get("source", "") or "unknown").strip().lower() or "unknown"
            status_counts[status_name] = int(status_counts.get(status_name, 0) or 0) + 1
            source_counts[source_name] = int(source_counts.get(source_name, 0) or 0) + 1
            summary = item.get("summary", {}) if isinstance(item.get("summary", {}), dict) else {}
            prepared_app_total += int(summary.get("prepared_app_count", 0) or 0)
            prepared_blocked_total += int(summary.get("prepared_blocked_count", 0) or 0)
            prepared_degraded_total += int(summary.get("prepared_degraded_count", 0) or 0)
            provider_update_total += int(summary.get("provider_update_count", 0) or 0)
            launch_seed_total += int(summary.get("launch_seed_count", 0) or 0)
            execution_action_total += int(summary.get("execution_action_count", 0) or 0)
            execution_ready_total += int(summary.get("execution_ready_count", 0) or 0)
            execution_success_total += int(summary.get("execution_success_count", 0) or 0)
            execution_manual_total += int(summary.get("execution_manual_count", 0) or 0)
            execution_blocked_total += int(summary.get("execution_blocked_count", 0) or 0)
            execution_error_total += int(summary.get("execution_error_count", 0) or 0)
            setup_action_total += int(summary.get("setup_action_count", 0) or 0)
            setup_action_auto_runnable_total += int(summary.get("setup_action_auto_runnable_count", 0) or 0)
            setup_action_success_total += int(summary.get("setup_action_success_count", 0) or 0)
            setup_action_manual_total += int(summary.get("setup_action_manual_count", 0) or 0)
            setup_action_blocked_total += int(summary.get("setup_action_blocked_count", 0) or 0)
            profile_setup_action_total += int(summary.get("profile_setup_action_count", 0) or 0)
            setup_execution_selected_action_total += int(summary.get("setup_execution_selected_action_count", 0) or 0)
            setup_execution_continued_action_total += int(summary.get("setup_execution_continued_action_count", 0) or 0)
            setup_execution_remaining_ready_total += int(summary.get("setup_execution_remaining_ready_count", 0) or 0)
            if bool(summary.get("setup_execution_resume_ready", False)):
                setup_execution_resume_ready_total += 1
            app_learning_setup_aligned_total += int(summary.get("app_learning_setup_aligned_count", 0) or 0)
            app_learning_setup_boosted_total += int(summary.get("app_learning_setup_boosted_count", 0) or 0)
            app_learning_setup_constrained_total += int(summary.get("app_learning_setup_constrained_count", 0) or 0)
            prepared_setup_aligned_total += int(summary.get("prepared_setup_aligned_count", 0) or 0)
            prepared_setup_boosted_total += int(summary.get("prepared_setup_boosted_count", 0) or 0)
            prepared_setup_constrained_total += int(summary.get("prepared_setup_constrained_count", 0) or 0)
            route_remediation_total += int(summary.get("route_remediation_count", 0) or 0)
            route_remediation_blocked_total += int(summary.get("route_remediation_blocked_count", 0) or 0)
            route_remediation_degraded_total += int(summary.get("route_remediation_degraded_count", 0) or 0)
            route_remediation_setup_followup_total += int(
                summary.get("route_remediation_setup_followup_count", 0) or 0
            )
            route_remediation_provider_blocked_total += int(
                summary.get("route_remediation_provider_blocked_count", 0) or 0
            )
        return {
            "status": "success",
            "count": len(limited),
            "total": len(items),
            "limit": bounded,
            "items": limited,
            "latest_run": dict(limited[0]) if limited else {},
            "filters": {
                "status": clean_status,
                "source": clean_source,
            },
            "summary": {
                "status_counts": {
                    str(key): int(value)
                    for key, value in sorted(status_counts.items(), key=lambda entry: entry[0])
                },
                "source_counts": {
                    str(key): int(value)
                    for key, value in sorted(source_counts.items(), key=lambda entry: entry[0])
                },
                "prepared_app_total": prepared_app_total,
                "prepared_blocked_total": prepared_blocked_total,
                "prepared_degraded_total": prepared_degraded_total,
                "provider_update_total": provider_update_total,
                "launch_seed_total": launch_seed_total,
                "execution_action_total": execution_action_total,
                "execution_ready_total": execution_ready_total,
                "execution_success_total": execution_success_total,
                "execution_manual_total": execution_manual_total,
                "execution_blocked_total": execution_blocked_total,
                "execution_error_total": execution_error_total,
                "setup_action_total": setup_action_total,
                "setup_action_auto_runnable_total": setup_action_auto_runnable_total,
                "setup_action_success_total": setup_action_success_total,
                "setup_action_manual_total": setup_action_manual_total,
                "setup_action_blocked_total": setup_action_blocked_total,
                "profile_setup_action_total": profile_setup_action_total,
                "setup_execution_selected_action_total": setup_execution_selected_action_total,
                "setup_execution_continued_action_total": setup_execution_continued_action_total,
                "setup_execution_remaining_ready_total": setup_execution_remaining_ready_total,
                "setup_execution_resume_ready_total": setup_execution_resume_ready_total,
                "app_learning_setup_aligned_total": app_learning_setup_aligned_total,
                "app_learning_setup_boosted_total": app_learning_setup_boosted_total,
                "app_learning_setup_constrained_total": app_learning_setup_constrained_total,
                "prepared_setup_aligned_total": prepared_setup_aligned_total,
                "prepared_setup_boosted_total": prepared_setup_boosted_total,
                "prepared_setup_constrained_total": prepared_setup_constrained_total,
                "route_remediation_total": route_remediation_total,
                "route_remediation_blocked_total": route_remediation_blocked_total,
                "route_remediation_degraded_total": route_remediation_degraded_total,
                "route_remediation_setup_followup_total": route_remediation_setup_followup_total,
                "route_remediation_provider_blocked_total": route_remediation_provider_blocked_total,
            },
        }

    def record_run(self, payload: Dict[str, Any], *, source: str = "api") -> Dict[str, Any]:
        row = dict(payload or {})
        row["source"] = str(source or row.get("source", "api") or "api").strip().lower() or "api"
        row["recorded_at"] = str(row.get("recorded_at", "") or _utc_now_iso()).strip()
        self._store.set("latest_run", row)
        rows = self._store.get("runs", [])
        items = [dict(item) for item in rows if isinstance(item, dict)] if isinstance(rows, list) else []
        items.insert(0, row)
        self._store.set("runs", items[:48])
        return row
