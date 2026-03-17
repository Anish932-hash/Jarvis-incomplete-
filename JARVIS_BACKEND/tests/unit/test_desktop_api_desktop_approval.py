from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List

from backend.python.core.approval_gate import ApprovalGate
from backend.python.desktop_api import DesktopBackendService
from backend.python.utils.logger import Logger


class _DesktopApprovalRouter:
    def __init__(self) -> None:
        self.advise_calls: List[Dict[str, Any]] = []
        self.execute_calls: List[Dict[str, Any]] = []

    def advise(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.advise_calls.append(dict(payload))
        return {
            "status": "success",
            "action": str(payload.get("action", "") or ""),
            "route_mode": "workflow",
            "confidence": 0.88,
            "risk_level": "high",
            "target_window": {"title": str(payload.get("window_title", "") or "Windows Settings")},
            "surface_snapshot": {"surface_signature": "surface-settings-apply"},
            "safety_signals": {
                "warning_surface_visible": True,
                "requires_confirmation": True,
                "dialog_state": {
                    "approval_kind": "permission_review",
                    "dialog_kind": "confirmation",
                },
            },
            "warnings": ["Applying this form may commit Windows settings changes."],
            "execution_plan": [],
        }

    def execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        self.execute_calls.append(dict(payload))
        return {
            "status": "success",
            "action": str(payload.get("action", "") or ""),
            "final_action": str(payload.get("action", "") or ""),
            "message": "Desktop action executed.",
        }


class _DesktopCriticalApprovalRouter(_DesktopApprovalRouter):
    def advise(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        base = super().advise(payload)
        base["risk_level"] = "critical"
        base["safety_signals"] = {
            "warning_surface_visible": True,
            "destructive_warning_visible": True,
            "requires_confirmation": True,
            "admin_approval_required": True,
            "dialog_state": {
                "approval_kind": "elevation_consent",
                "dialog_kind": "confirmation",
            },
        }
        return base


def _build_service(router: _DesktopApprovalRouter | None = None) -> DesktopBackendService:
    service = DesktopBackendService.__new__(DesktopBackendService)
    service.log = Logger.get_logger("DesktopApprovalTest")
    service.desktop_action_router = router or _DesktopApprovalRouter()
    service.kernel = SimpleNamespace(approval_gate=ApprovalGate(ttl_s=120, max_records=512))
    return service


def test_desktop_action_advice_adds_approval_contract_for_high_risk_route() -> None:
    service = _build_service()

    advice = service.desktop_action_advice(
        action="complete_form_flow",
        app_name="settings",
        window_title="Windows Settings",
        query="bluetooth",
    )

    assert advice["status"] == "success"
    assert advice["approval_required"] is True
    contract = advice["approval_contract"]
    assert contract["approval_scope"] == "desktop_interact"
    assert contract["approval_action"] == "complete_form_flow"
    assert contract["risk_level"] == "high"
    assert contract["requires_permission_review"] is True
    assert contract["approval_memory"]["profile"] == "balanced"
    assert contract["approval_memory"]["reusable"] is True
    assert advice["approval_memory"]["profile"] == "balanced"
    assert contract["contract_id"].startswith("desktop-approval-")


def test_desktop_interact_requests_approval_before_high_risk_execution() -> None:
    router = _DesktopApprovalRouter()
    service = _build_service(router)

    result = service.desktop_interact(
        action="complete_form_flow",
        app_name="settings",
        window_title="Windows Settings",
        query="bluetooth",
    )

    assert result["status"] == "blocked"
    assert result["approval_required"] is True
    assert result["approval"]["status"] == "pending"
    assert result["approval_id"] == result["approval"]["approval_id"]
    assert result["approval_contract"]["approval_action"] == "complete_form_flow"
    assert router.execute_calls == []


def test_desktop_interact_consumes_approval_and_executes() -> None:
    router = _DesktopApprovalRouter()
    service = _build_service(router)

    blocked = service.desktop_interact(
        action="complete_form_flow",
        app_name="settings",
        window_title="Windows Settings",
        query="bluetooth",
    )
    approval_id = str(blocked["approval_id"])
    approved, message, _ = service.kernel.approval_gate.approve(approval_id, note="ok")
    assert approved, message

    result = service.desktop_interact(
        action="complete_form_flow",
        app_name="settings",
        window_title="Windows Settings",
        query="bluetooth",
        approval_id=approval_id,
    )

    assert result["status"] == "success"
    assert result["approval_required"] is False
    assert result["approval_contract"]["approval_action"] == "complete_form_flow"
    assert len(router.execute_calls) == 1


def test_desktop_interact_auto_reuses_exact_match_approved_desktop_approval() -> None:
    router = _DesktopApprovalRouter()
    service = _build_service(router)

    blocked = service.desktop_interact(
        action="complete_form_flow",
        app_name="settings",
        window_title="Windows Settings",
        query="bluetooth",
    )
    approval_id = str(blocked["approval_id"])
    approved, message, _ = service.kernel.approval_gate.approve(approval_id, note="ok")
    assert approved, message

    result = service.desktop_interact(
        action="complete_form_flow",
        app_name="settings",
        window_title="Windows Settings",
        query="bluetooth",
    )

    assert result["status"] == "success"
    assert result["approval_reused"] is True
    assert result["approval_reuse_source"] == "approved"
    assert result["approval_memory"]["profile"] == "balanced"
    assert len(router.execute_calls) == 1


def test_desktop_interact_auto_reuses_recent_consumed_desktop_approval() -> None:
    router = _DesktopApprovalRouter()
    service = _build_service(router)

    blocked = service.desktop_interact(
        action="complete_form_flow",
        app_name="settings",
        window_title="Windows Settings",
        query="bluetooth",
    )
    approval_id = str(blocked["approval_id"])
    approved, message, _ = service.kernel.approval_gate.approve(approval_id, note="ok")
    assert approved, message

    first_result = service.desktop_interact(
        action="complete_form_flow",
        app_name="settings",
        window_title="Windows Settings",
        query="bluetooth",
    )
    assert first_result["status"] == "success"
    assert first_result["approval_reuse_source"] == "approved"

    second_result = service.desktop_interact(
        action="complete_form_flow",
        app_name="settings",
        window_title="Windows Settings",
        query="bluetooth",
    )

    assert second_result["status"] == "success"
    assert second_result["approval_reused"] is True
    assert second_result["approval_reuse_source"] == "consumed"
    assert len(router.execute_calls) == 2


def test_desktop_interact_does_not_auto_reuse_conservative_desktop_approval() -> None:
    router = _DesktopCriticalApprovalRouter()
    service = _build_service(router)

    blocked = service.desktop_interact(
        action="complete_form_flow",
        app_name="settings",
        window_title="Windows Settings",
        query="bluetooth",
    )
    approval_id = str(blocked["approval_id"])
    approved, message, _ = service.kernel.approval_gate.approve(approval_id, note="ok")
    assert approved, message

    result = service.desktop_interact(
        action="complete_form_flow",
        app_name="settings",
        window_title="Windows Settings",
        query="bluetooth",
    )

    assert result["status"] == "blocked"
    assert result["approval_required"] is True
    assert result["approval_memory"]["profile"] == "conservative"
    assert len(router.execute_calls) == 0
