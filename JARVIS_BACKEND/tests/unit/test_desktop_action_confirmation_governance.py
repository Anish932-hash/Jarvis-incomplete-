from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

from backend.python.core.approval_gate import ApprovalGate
from backend.python.core.contracts import ActionRequest, ActionResult
from backend.python.core.desktop_governance_policy import DesktopGovernancePolicyManager
from backend.python.desktop_api import DesktopBackendService
from backend.python.utils.logger import Logger


class _FakeTelemetry:
    def __init__(self) -> None:
        self.events: List[Tuple[str, Dict[str, Any]]] = []

    def emit(self, event: str, payload: Dict[str, Any]) -> None:
        self.events.append((event, payload))


class _FakePolicy:
    def __init__(self) -> None:
        self.risk_engine = SimpleNamespace(
            rate=lambda *_args, **_kwargs: SimpleNamespace(
                score=0.2,
                level="low",
                reason="unit-test",
                factors=["unit"],
            )
        )

    @staticmethod
    def authorize(_request: ActionRequest) -> tuple[bool, str]:
        return (True, "")


class _FakeRegistry:
    def __init__(self) -> None:
        self.definition = SimpleNamespace(requires_confirmation=True)
        self.execute_calls: List[str] = []

    def get(self, _action: str) -> Any:
        return self.definition

    async def execute(self, request: ActionRequest, timeout_s: int = 30) -> ActionResult:
        del timeout_s
        self.execute_calls.append(request.action)
        return ActionResult(action=request.action, status="success", output={"status": "success"})


class _NoopRollbackManager:
    @staticmethod
    def capture_pre_state(*, action: str, args: Dict[str, Any]) -> Dict[str, Any]:
        del action, args
        return {}

    @staticmethod
    def record_success(**_kwargs: Any) -> Dict[str, Any]:
        return {}


def _build_service(tmp_path, *, policy_profile: str = "balanced") -> tuple[DesktopBackendService, _FakeRegistry, _FakeTelemetry]:
    service = DesktopBackendService.__new__(DesktopBackendService)
    service.log = Logger.get_logger("DesktopActionConfirmationGovernanceTest")
    registry = _FakeRegistry()
    telemetry = _FakeTelemetry()
    service.desktop_governance_policy = DesktopGovernancePolicyManager(
        state_path=str(tmp_path / "desktop_governance.json"),
        policy_profile=policy_profile,
    )
    service.kernel = SimpleNamespace(
        telemetry=telemetry,
        registry=registry,
        policy=_FakePolicy(),
        rollback_manager=_NoopRollbackManager(),
        approval_gate=ApprovalGate(ttl_s=120, max_records=512),
    )
    service._mission_autonomy_overlay = lambda *args, **kwargs: {}  # type: ignore[assignment]
    service._connector_remediation_policy_runtime_guard = lambda *args, **kwargs: None  # type: ignore[assignment]
    service._check_request_permissions = lambda **kwargs: (True, "", {})  # type: ignore[assignment]
    service._record_policy_outcome = lambda *args, **kwargs: None  # type: ignore[assignment]
    service._mission_autonomy_record_outcome = lambda *args, **kwargs: None  # type: ignore[assignment]
    return service, registry, telemetry


def test_generic_action_confirmation_auto_reuses_exact_match_approval(tmp_path) -> None:
    service, registry, telemetry = _build_service(tmp_path, policy_profile="balanced")
    request = ActionRequest(
        action="guarded_action",
        args={"path": "notes.txt"},
        source="desktop-ui",
        metadata={"policy_profile": "balanced"},
    )

    blocked = asyncio.run(service._execute_action_async(request))  # noqa: SLF001
    approval_id = str((blocked.output or {}).get("approval", {}).get("approval_id", "") or "")
    assert blocked.status == "blocked"
    assert approval_id

    approved, message, _ = service.kernel.approval_gate.approve(approval_id, note="ok")
    assert approved, message

    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "success"
    assert registry.execute_calls == ["guarded_action"]
    assert (result.output or {}).get("approval_reused") is True
    assert (result.output or {}).get("approval_reuse_source") == "approved"
    assert (result.output or {}).get("approval_memory", {}).get("profile") == "balanced"
    event_names = [event for event, _ in telemetry.events]
    assert "approval.auto_reused" in event_names


def test_generic_action_confirmation_refuses_reuse_for_destructive_metadata(tmp_path) -> None:
    service, registry, _ = _build_service(tmp_path, policy_profile="power")
    request = ActionRequest(
        action="guarded_action",
        args={"path": "notes.txt"},
        source="desktop-ui",
        metadata={"policy_profile": "power", "destructive_confirmation": True},
    )

    blocked = asyncio.run(service._execute_action_async(request))  # noqa: SLF001
    approval_id = str((blocked.output or {}).get("approval", {}).get("approval_id", "") or "")
    assert blocked.status == "blocked"
    assert approval_id

    approved, message, _ = service.kernel.approval_gate.approve(approval_id, note="ok")
    assert approved, message

    blocked_again = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert blocked_again.status == "blocked"
    assert registry.execute_calls == []
    assert (blocked_again.output or {}).get("approval_memory", {}).get("profile") == "conservative"
