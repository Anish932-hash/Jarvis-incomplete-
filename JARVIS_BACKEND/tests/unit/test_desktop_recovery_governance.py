from __future__ import annotations

from typing import Any, Dict, List

from backend.python.desktop_api import DesktopBackendService


def _resume_ready_row(**overrides: Any) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "mission_id": "desktop-mission-1",
        "status": "paused",
        "mission_kind": "wizard",
        "app_name": "installer",
        "blocking_window_title": "Installer Setup",
        "anchor_window_title": "Installer",
        "resume_ready": True,
        "manual_attention_required": False,
        "approval_blocked": False,
        "recovery_profile": "resume_ready",
        "recovery_priority": 8,
        "created_at": "2026-03-17T08:00:00+00:00",
        "updated_at": "2026-03-17T08:05:00+00:00",
        "high_risk": True,
        "critical_risk": True,
        "admin_clearance_required": False,
        "destructive_confirmation": False,
        "resume_contract": {"resume_action": "complete_wizard_flow"},
        "blocking_surface": {"window_title": "Installer Setup"},
    }
    row.update(overrides)
    return row


class _FakeRecoveryService:
    def __init__(self, rows: List[Dict[str, Any]]) -> None:
        self._rows = [dict(row) for row in rows]
        self.resume_calls: List[Dict[str, Any]] = []

    def desktop_mission_status(
        self,
        *,
        limit: int = 12,
        status: str = "",
        mission_kind: str = "",
        app_name: str = "",
        stop_reason_code: str = "",
    ) -> Dict[str, Any]:
        del status, mission_kind, app_name, stop_reason_code
        items = [dict(row) for row in self._rows[:limit]]
        return {
            "status": "success",
            "count": len(items),
            "items": items,
            "manual_attention_count": sum(1 for row in items if bool(row.get("manual_attention_required", False))),
        }

    def desktop_interact(self, **kwargs: Any) -> Dict[str, Any]:
        self.resume_calls.append(dict(kwargs))
        return {"status": "success", "message": "mission resumed"}

    def _record_desktop_recovery_watchdog_run(self, payload: Dict[str, Any], *, source: str = "manual") -> Dict[str, Any]:
        result = dict(payload)
        result["source"] = source
        return result


def test_desktop_recovery_tick_blocks_critical_mission_under_balanced_policy() -> None:
    service = _FakeRecoveryService([_resume_ready_row()])

    payload = DesktopBackendService._execute_desktop_recovery_supervisor_tick(
        service,  # type: ignore[arg-type]
        policy_profile="balanced",
        allow_high_risk=True,
        allow_critical_risk=False,
        allow_admin_clearance=False,
        allow_destructive=False,
        trigger_source="daemon",
    )

    assert payload["status"] == "blocked"
    assert payload["policy_blocked_count"] == 1
    assert payload["stop_reason"] == "policy_blocked"
    assert payload["policy_deferred_mission_ids"] == ["desktop-mission-1"]
    assert payload["results"][0]["classification_after"] == "policy_blocked"
    assert payload["results"][0]["policy_block_reason"] == "critical_risk_blocked_by_policy"
    assert service.resume_calls == []


def test_desktop_recovery_tick_allows_critical_mission_under_power_policy() -> None:
    service = _FakeRecoveryService([_resume_ready_row()])

    payload = DesktopBackendService._execute_desktop_recovery_supervisor_tick(
        service,  # type: ignore[arg-type]
        policy_profile="power",
        allow_high_risk=True,
        allow_critical_risk=True,
        allow_admin_clearance=False,
        allow_destructive=False,
        trigger_source="daemon",
    )

    assert payload["status"] == "success"
    assert payload["auto_resume_triggered_count"] == 1
    assert payload["policy_blocked_count"] == 0
    assert payload["triggered_mission_ids"] == ["desktop-mission-1"]
    assert len(service.resume_calls) == 1
    assert service.resume_calls[0]["action"] == "resume_mission"
