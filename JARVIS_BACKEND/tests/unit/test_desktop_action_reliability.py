from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

from backend.python.auth.permissions import Permissions
from backend.python.core.contracts import ActionRequest, ActionResult
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
                score=0.1,
                level="low",
                reason="unit-test",
                factors=["unit"],
            )
        )

    @staticmethod
    def authorize(_request: ActionRequest) -> tuple[bool, str]:
        return (True, "")

    @staticmethod
    def record_action_outcome(**_kwargs: Any) -> Dict[str, Any]:
        return {"status": "success"}


class _FakeRegistry:
    def __init__(self, definition: Any = None) -> None:
        self.definition = definition
        self.execute_calls: List[str] = []

    def get(self, _action: str) -> Any:
        return self.definition

    async def execute(self, request: ActionRequest, timeout_s: int = 30) -> ActionResult:
        del timeout_s
        self.execute_calls.append(request.action)
        return ActionResult(action=request.action, status="success", output={"status": "success", "ok": True})


class _FailingRollbackManager:
    @staticmethod
    def capture_pre_state(*, action: str, args: Dict[str, Any]) -> Dict[str, Any]:
        raise RuntimeError(f"capture failed for {action} {args}")

    @staticmethod
    def record_success(**_kwargs: Any) -> Dict[str, Any]:
        raise RuntimeError("record failed")


class _NoopRollbackManager:
    @staticmethod
    def capture_pre_state(*, action: str, args: Dict[str, Any]) -> Dict[str, Any]:
        del action, args
        return {}

    @staticmethod
    def record_success(**_kwargs: Any) -> Dict[str, Any]:
        return {}


def _build_service(
    *,
    registry: _FakeRegistry | None = None,
    rollback_manager: Any | None = None,
    policy: _FakePolicy | None = None,
) -> tuple[DesktopBackendService, _FakeTelemetry]:
    service = DesktopBackendService.__new__(DesktopBackendService)
    service.log = Logger.get_logger("DesktopAPITest")
    telemetry = _FakeTelemetry()
    service.permissions = Permissions()
    service._default_role = "developer"
    service._source_role_defaults = {}
    service._mission_rbac_enabled = False
    service._mission_role_by_policy = {}
    service._mission_role_by_risk = {}
    service._mission_role_cache_ttl_s = 30.0
    service._mission_role_cache = {}
    service._mission_role_cache_lock = threading.RLock()
    service._mission_autonomy_adapt_enabled = False
    service._mission_autonomy_refresh_s = 12.0
    service._mission_autonomy_profile_on_high = "automation_safe"
    service._mission_autonomy_profile_on_medium = "interactive"
    service._mission_autonomy_profile_on_low = "automation_power"
    service._mission_autonomy_role_on_high = "developer"
    service._mission_autonomy_role_on_medium = "developer"
    service._mission_autonomy_role_on_low = "admin"
    service._mission_autonomy_override_explicit = False
    service._mission_autonomy_learning_enabled = False
    service._mission_autonomy_learning_alpha = 0.28
    service._mission_autonomy_learning_min_samples = 6
    service._mission_autonomy_learning_bad_threshold = 0.46
    service._mission_autonomy_learning_good_threshold = 0.82
    service._mission_autonomy_dynamic_profile_by_risk = {}
    service._mission_autonomy_dynamic_role_by_risk = {}
    service._mission_autonomy_learning_state = {}
    service._mission_autonomy_cache = {}
    service._mission_autonomy_lock = threading.RLock()
    service._bridge_scheduler_runtime_context_cache = {}
    service._bridge_scheduler_runtime_context_ttl_s = 2.2
    service._external_connector_mission_analysis_cache = {}
    service._external_connector_mission_analysis_cache_ttl_s = 45.0
    service._external_connector_mission_analysis_provider_limit = 140
    service._external_connector_mission_analysis_history_limit = 36
    service._external_connector_mission_analysis_record_enabled = True
    service._connector_remediation_policy_lock = threading.RLock()
    service._connector_remediation_policy_state_loaded = True
    service._connector_remediation_policy_dirty = False
    service._connector_remediation_policy_history_max = 1200
    service._connector_remediation_policy_state = {
        "version": "1.0",
        "updated_at": "",
        "profiles": {},
        "history": [],
    }
    service._connector_execution_contract_lock = threading.RLock()
    service._connector_execution_contract_state_loaded = True
    service._connector_execution_contract_dirty = False
    service._connector_execution_contract_history_max = 1200
    service._connector_execution_contract_path = "data/external/test_connector_execution_contract_state.json"
    service._connector_execution_contract_state = {
        "version": "1.0",
        "updated_at": "",
        "contracts": {},
        "history": [],
    }
    service._persist_connector_execution_contract_state = lambda force=False: None  # type: ignore[assignment]
    service._connector_remediation_alert_lock = threading.RLock()
    service._connector_remediation_alert_last_emit = {}
    service._connector_remediation_alert_emit_cooldown_s = 30.0
    service._connector_remediation_alert_emit_cache_max = 200
    service.kernel = SimpleNamespace(
        telemetry=telemetry,
        registry=registry or _FakeRegistry(),
        policy=policy or _FakePolicy(),
        rollback_manager=rollback_manager or _NoopRollbackManager(),
    )
    return service, telemetry


def test_execute_action_keeps_success_with_nonfatal_rollback_failures() -> None:
    service, telemetry = _build_service(
        registry=_FakeRegistry(),
        rollback_manager=_FailingRollbackManager(),
    )

    request = ActionRequest(action="time_now", args={"timezone": "UTC"}, source="unit-test")
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "success"
    warnings = result.evidence.get("nonfatal_warnings", [])
    assert isinstance(warnings, list)
    assert warnings
    assert "Rollback record failed" in str(warnings[0])
    event_names = [event for event, _ in telemetry.events]
    assert "rollback.capture_error" in event_names
    assert "rollback.record_error" in event_names


def test_execute_action_blocks_when_role_lacks_permission() -> None:
    registry = _FakeRegistry()
    service, telemetry = _build_service(registry=registry)

    request = ActionRequest(
        action="copy_file",
        args={"source": "a.txt", "destination": "b.txt"},
        source="desktop-ui",
        metadata={"rbac_role": "user"},
    )
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "blocked"
    assert result.output.get("permission_required") is True
    rbac = result.output.get("rbac", {})
    assert rbac.get("role") == "user"
    assert rbac.get("required_permission") == "write"
    assert registry.execute_calls == []
    event_names = [event for event, _ in telemetry.events]
    assert "action.permission_denied" in event_names


def test_execute_action_uses_source_role_default_for_permission_checks() -> None:
    registry = _FakeRegistry()
    service, _ = _build_service(registry=registry)
    service._source_role_defaults = {"desktop-ui": "user"}

    request = ActionRequest(
        action="copy_file",
        args={"source": "a.txt", "destination": "b.txt"},
        source="desktop-ui",
        metadata={},
    )
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "blocked"
    assert result.output.get("permission_required") is True
    assert result.output.get("rbac", {}).get("role") == "user"
    assert registry.execute_calls == []


def test_execute_action_uses_mission_policy_profile_role_mapping() -> None:
    registry = _FakeRegistry()
    service, _ = _build_service(registry=registry)
    service._mission_rbac_enabled = True
    service._mission_role_by_policy = {"automation_power": "admin"}
    service.kernel.get_mission = lambda mission_id: {"mission_id": mission_id, "metadata": {"policy_profile": "automation_power"}}
    service.kernel.mission_diagnostics = lambda mission_id, hotspot_limit=4: {"risk": {"level": "high"}}

    request = ActionRequest(
        action="oauth_token_revoke",
        args={"provider": "google"},
        source="desktop-ui",
        metadata={"__jarvis_mission_id": "mission-1"},
    )
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "success"
    assert registry.execute_calls == ["oauth_token_revoke"]


def test_execute_action_uses_mission_risk_role_mapping_for_blocks() -> None:
    registry = _FakeRegistry()
    service, _ = _build_service(registry=registry)
    service._mission_rbac_enabled = True
    service._mission_role_by_policy = {}
    service._mission_role_by_risk = {"high": "user"}
    service.kernel.get_mission = lambda mission_id: {"mission_id": mission_id, "metadata": {}}
    service.kernel.mission_diagnostics = lambda mission_id, hotspot_limit=4: {"risk": {"level": "high"}}

    request = ActionRequest(
        action="copy_file",
        args={"source": "a.txt", "destination": "b.txt"},
        source="desktop-ui",
        metadata={"__jarvis_mission_id": "mission-2"},
    )
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "blocked"
    rbac = result.output.get("rbac", {})
    assert rbac.get("role") == "user"
    assert rbac.get("role_source") == "mission_risk_level"
    assert registry.execute_calls == []


def test_execute_action_applies_mission_autonomy_overlay_when_enabled() -> None:
    registry = _FakeRegistry()
    service, telemetry = _build_service(registry=registry)
    service._mission_autonomy_adapt_enabled = True
    service.kernel.get_mission = lambda mission_id: {"mission_id": mission_id, "metadata": {}, "status": "running"}
    service.kernel.mission_diagnostics = lambda mission_id, hotspot_limit=4: {  # noqa: ARG005
        "risk": {"level": "high"},
        "quality": {"score": 0.42},
    }

    request = ActionRequest(
        action="time_now",
        args={"timezone": "UTC"},
        source="desktop-ui",
        metadata={"__jarvis_mission_id": "mission-overlay-1"},
    )
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "success"
    assert request.metadata.get("policy_profile") == "automation_safe"
    assert request.metadata.get("rbac_role") == "developer"
    event_names = [event for event, _ in telemetry.events]
    assert "action.mission_overlay_applied" in event_names


def test_execute_action_respects_explicit_policy_and_role_without_override() -> None:
    registry = _FakeRegistry()
    service, telemetry = _build_service(registry=registry)
    service._mission_autonomy_adapt_enabled = True
    service._mission_autonomy_override_explicit = False
    service.kernel.get_mission = lambda mission_id: {"mission_id": mission_id, "metadata": {}, "status": "running"}
    service.kernel.mission_diagnostics = lambda mission_id, hotspot_limit=4: {  # noqa: ARG005
        "risk": {"level": "high"},
        "quality": {"score": 0.2},
    }

    request = ActionRequest(
        action="time_now",
        args={"timezone": "UTC"},
        source="desktop-ui",
        metadata={
            "__jarvis_mission_id": "mission-overlay-2",
            "policy_profile": "custom_profile",
            "rbac_role": "admin",
        },
    )
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "success"
    assert request.metadata.get("policy_profile") == "custom_profile"
    assert request.metadata.get("rbac_role") == "admin"
    event_names = [event for event, _ in telemetry.events]
    assert "action.mission_overlay_applied" not in event_names


def test_execute_action_blocks_external_high_risk_when_connector_policy_disallows() -> None:
    registry = _FakeRegistry()
    service, _ = _build_service(registry=registry)
    service._connector_remediation_policy_state = {
        "version": "1.0",
        "updated_at": "",
        "profiles": {
            "external_email_send|google|stable": {
                "scope_key": "external_email_send|google|stable",
                "action": "external_email_send",
                "provider": "google",
                "mission_mode": "stable",
                "profile": "strict",
                "controls": {
                    "allow_high_risk": False,
                    "max_steps": 4,
                    "require_compare": False,
                    "stop_on_blocked": True,
                },
            }
        },
        "history": [],
    }
    service.kernel.policy.risk_engine = SimpleNamespace(  # type: ignore[attr-defined]
        rate=lambda *_args, **_kwargs: SimpleNamespace(
            score=0.86,
            level="high",
            reason="unit-test-risk",
            factors=["risk:high"],
        )
    )

    request = ActionRequest(
        action="external_email_send",
        args={"provider": "google", "to": ["a@example.com"]},
        source="desktop-ui",
        metadata={},
    )
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "blocked"
    assert result.output.get("error_code") == "connector_policy_high_risk_blocked"
    assert registry.execute_calls == []


def test_execute_action_blocks_external_write_when_compare_is_required_and_missing() -> None:
    registry = _FakeRegistry()
    service, _ = _build_service(registry=registry)
    service._connector_remediation_policy_state = {
        "version": "1.0",
        "updated_at": "",
        "profiles": {
            "external_task_create|google|stable": {
                "scope_key": "external_task_create|google|stable",
                "action": "external_task_create",
                "provider": "google",
                "mission_mode": "stable",
                "profile": "strict",
                "controls": {
                    "allow_high_risk": True,
                    "max_steps": 4,
                    "require_compare": True,
                    "stop_on_blocked": True,
                },
            }
        },
        "history": [],
    }

    request = ActionRequest(
        action="external_task_create",
        args={"provider": "google", "title": "Ship release"},
        source="desktop-ui",
        metadata={},
    )
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "blocked"
    assert result.output.get("error_code") == "connector_policy_compare_required"
    assert registry.execute_calls == []


def test_execute_action_allows_external_write_when_compare_verified_and_pins_provider() -> None:
    registry = _FakeRegistry()
    service, _ = _build_service(registry=registry)
    service._connector_remediation_policy_state = {
        "version": "1.0",
        "updated_at": "",
        "profiles": {
            "external_task_create|graph|stable": {
                "scope_key": "external_task_create|graph|stable",
                "action": "external_task_create",
                "provider": "graph",
                "mission_mode": "stable",
                "profile": "strict",
                "controls": {
                    "allow_high_risk": True,
                    "max_steps": 4,
                    "require_compare": True,
                    "stop_on_blocked": True,
                },
            }
        },
        "history": [],
    }

    request = ActionRequest(
        action="external_task_create",
        args={"provider": "auto", "title": "Review contract"},
        source="desktop-ui",
        metadata={"external_compare_verified": True},
    )
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "success"
    assert registry.execute_calls == ["external_task_create"]
    assert str(request.args.get("provider", "")).strip().lower() == "graph"


def test_mission_autonomy_learning_adapts_to_failures() -> None:
    registry = _FakeRegistry()
    service, _ = _build_service(registry=registry)
    service._mission_autonomy_learning_enabled = True
    service._mission_autonomy_learning_min_samples = 1
    service._mission_autonomy_learning_bad_threshold = 0.0
    service._mission_autonomy_learning_good_threshold = 0.95

    request = ActionRequest(
        action="copy_file",
        args={"source": "a.txt", "destination": "b.txt"},
        source="desktop-ui",
        metadata={"__jarvis_mission_id": "mission-learn-failure", "rbac_role": "user"},
    )
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "blocked"
    assert service._mission_autonomy_dynamic_profile_by_risk.get("low") == "automation_safe"
    assert service._mission_autonomy_dynamic_role_by_risk.get("low") == "developer"


def test_mission_autonomy_learning_adapts_to_success_for_low_risk() -> None:
    registry = _FakeRegistry()
    service, _ = _build_service(registry=registry)
    service._mission_autonomy_learning_enabled = True
    service._mission_autonomy_learning_min_samples = 1
    service._mission_autonomy_learning_bad_threshold = 0.95
    service._mission_autonomy_learning_good_threshold = 0.0

    request = ActionRequest(
        action="time_now",
        args={"timezone": "UTC"},
        source="desktop-ui",
        metadata={"__jarvis_mission_id": "mission-learn-success"},
    )
    result = asyncio.run(service._execute_action_async(request))  # noqa: SLF001

    assert result.status == "success"
    assert service._mission_autonomy_dynamic_profile_by_risk.get("low") == "automation_power"
    assert service._mission_autonomy_dynamic_role_by_risk.get("low") == "admin"


def test_mission_autonomy_target_applies_context_pressure_guard() -> None:
    service, _ = _build_service()
    service._opportunity_lock = threading.RLock()
    service._context_opportunity_learning_state_by_type = {
        "workflow_automation": {"samples": 12, "badness": 0.9},
        "error_detected": {"samples": 8, "badness": 0.82},
    }

    target = service._mission_autonomy_target(risk_level="low", quality_score=0.91)  # noqa: SLF001

    assert target.get("target_policy_profile") == "interactive"
    assert target.get("target_rbac_role") == "developer"
    assert target.get("context_pressure_guard_applied") is True
    assert float(target.get("context_opportunity_pressure", 0.0) or 0.0) >= 0.62


def test_mission_autonomy_target_keeps_low_risk_autonomy_when_context_pressure_is_low() -> None:
    service, _ = _build_service()
    service._opportunity_lock = threading.RLock()
    service._context_opportunity_learning_state_by_type = {
        "workflow_automation": {"samples": 20, "badness": 0.1},
    }

    target = service._mission_autonomy_target(risk_level="low", quality_score=0.91)  # noqa: SLF001

    assert target.get("target_policy_profile") == "automation_power"
    assert target.get("target_rbac_role") == "admin"
    assert target.get("context_pressure_guard_applied") is False


def test_run_external_connector_preflight_adds_orchestration_diagnostics() -> None:
    service, _ = _build_service()
    service.execute_action = lambda _action, _args, source="desktop-ui", metadata=None: {  # type: ignore[assignment]
        "status": "success",
        "output": {
            "status": "success",
            "action": "external_email_send",
            "provider": "google",
            "provider_selected": "google",
            "message": "Connector preflight passed.",
            "preflight_ready": True,
            "contract_diagnostic": {"code": "ready", "severity": "info", "message": "ok"},
            "remediation_hints": [],
        },
    }
    service.kernel.external_reliability_status = lambda provider="", limit=180: {  # type: ignore[attr-defined]
        "status": "success",
        "mission_outage_policy": {"mode": "stable", "bias": 0.0, "pressure_ema": 0.12, "profile": "balanced"},
        "items": [
            {
                "provider": provider or "google",
                "health_score": 0.84,
                "failure_ema": 0.1,
                "cooldown_active": False,
                "outage_active": False,
                "retry_after_s": 0.0,
            }
        ],
    }

    payload = service.run_external_connector_preflight(  # noqa: SLF001
        args={"action": "external_email_send", "provider": "google", "args": {"to": ["a@example.com"]}},
        source="desktop-ui",
        metadata={},
    )

    assert payload["status"] == "success"
    orchestration = payload.get("orchestration_diagnostics", {})
    assert isinstance(orchestration, dict)
    assert str(orchestration.get("provider_requested", "")) == "google"
    assert int(orchestration.get("provider_count", 0) or 0) >= 1
    route_advisor = orchestration.get("route_weight_advisor", {})
    assert isinstance(route_advisor, dict)
    assert isinstance(route_advisor.get("weight_rows", []), list)
    assert isinstance(payload.get("execution_candidates", []), list)
    assert isinstance(payload.get("advisor_simulation_template", {}), dict)
    assert isinstance(payload.get("remediation_hints", []), list)


def test_run_external_connector_preflight_blocks_when_provider_is_in_cooldown() -> None:
    service, _ = _build_service()
    service.execute_action = lambda _action, _args, source="desktop-ui", metadata=None: {  # type: ignore[assignment]
        "status": "success",
        "output": {
            "status": "success",
            "action": "external_email_send",
            "provider": "google",
            "provider_selected": "google",
            "message": "Connector preflight passed.",
            "preflight_ready": True,
            "contract_diagnostic": {"code": "ready", "severity": "info", "message": "ok"},
            "remediation_hints": [],
        },
    }
    service.kernel.external_reliability_status = lambda provider="", limit=180: {  # type: ignore[attr-defined]
        "status": "success",
        "mission_outage_policy": {"mode": "degraded", "bias": 0.2, "pressure_ema": 0.84, "profile": "safe"},
        "items": [
            {
                "provider": provider or "google",
                "health_score": 0.22,
                "failure_ema": 0.88,
                "cooldown_active": True,
                "outage_active": True,
                "retry_after_s": 42.0,
            }
        ],
    }

    payload = service.run_external_connector_preflight(  # noqa: SLF001
        args={"action": "external_email_send", "provider": "google", "args": {"to": ["a@example.com"]}},
        source="desktop-ui",
        metadata={},
    )

    assert payload["status"] == "error"
    assert payload["preflight_ready"] is False
    contract = payload.get("contract_diagnostic", {})
    assert isinstance(contract, dict)
    assert str(contract.get("code", "")) == "provider_reliability_blocked"
    orchestration = payload.get("orchestration_diagnostics", {})
    assert isinstance(orchestration, dict)
    assert bool(orchestration.get("blocked_by_reliability", False)) is True
    cooldown_explainer = orchestration.get("cooldown_outage_explainer", {})
    assert isinstance(cooldown_explainer, dict)
    assert int(cooldown_explainer.get("outage_count", 0) or 0) >= 1
    assert isinstance(cooldown_explainer.get("recommended_actions", []), list)


def test_run_external_connector_preflight_flags_contract_alignment_drift() -> None:
    service, _ = _build_service()
    service.execute_action = lambda _action, _args, source="desktop-ui", metadata=None: {  # type: ignore[assignment]
        "status": "success",
        "output": {
            "status": "success",
            "action": "external_email_send",
            "provider": "google",
            "provider_selected": "google",
            "message": "Connector preflight passed.",
            "preflight_ready": True,
            "contract_diagnostic": {"code": "ready", "severity": "info", "message": "ok"},
            "preflight_orchestration": {
                "mode": "stable",
                "pressure": 0.08,
                "primary_provider": "graph",
                "fallback_provider": "google",
                "provider_rows": [
                    {"provider": "graph", "score": 0.81},
                    {"provider": "google", "score": 0.74},
                ],
                "retry_schedule": [{"provider": "graph", "delay_s": 0.0, "score": 0.81}],
            },
            "remediation_hints": [],
        },
    }
    service.kernel.external_reliability_status = lambda provider="", limit=180: {  # type: ignore[attr-defined]
        "status": "success",
        "mission_outage_policy": {"mode": "stable", "bias": 0.0, "pressure_ema": 0.14, "profile": "balanced"},
        "items": [
            {
                "provider": provider or "google",
                "health_score": 0.88,
                "failure_ema": 0.08,
                "cooldown_active": False,
                "outage_active": False,
                "retry_after_s": 0.0,
            }
        ],
    }

    payload = service.run_external_connector_preflight(  # noqa: SLF001
        args={"action": "external_email_send", "provider": "google", "args": {"to": ["a@example.com"]}},
        source="desktop-ui",
        metadata={},
    )

    assert payload["status"] == "error"
    assert payload["preflight_ready"] is False
    alignment = payload.get("orchestration_contract_diagnostics", {})
    assert isinstance(alignment, dict)
    assert str(alignment.get("status", "")).strip().lower() == "error"
    assert float(alignment.get("mismatch_score", 0.0) or 0.0) > 0.0
    contract = payload.get("contract_diagnostic", {})
    assert isinstance(contract, dict)
    assert str(contract.get("code", "")).strip().lower() == "preflight_contract_alignment_failed"


def test_run_external_connector_preflight_includes_provider_policy_autotune_and_runtime_lane() -> None:
    service, _ = _build_service()
    service.execute_action = lambda _action, _args, source="desktop-ui", metadata=None: {  # type: ignore[assignment]
        "status": "success",
        "output": {
            "status": "success",
            "action": "external_email_send",
            "provider": "google",
            "provider_selected": "google",
            "message": "Connector preflight passed.",
            "preflight_ready": True,
            "contract_diagnostic": {"code": "ready", "severity": "info", "message": "ok"},
            "remediation_hints": [],
        },
    }
    service.kernel.external_reliability_status = lambda provider="", limit=180: {  # type: ignore[attr-defined]
        "status": "success",
        "mission_outage_policy": {"mode": "degraded", "bias": 0.18, "pressure_ema": 0.64, "profile": "defensive"},
        "items": [
            {
                "provider": provider or "google",
                "health_score": 0.44,
                "failure_ema": 0.62,
                "cooldown_active": False,
                "outage_active": False,
                "retry_after_s": 12.0,
            }
        ],
    }
    service._external_connector_mission_analysis_cached = lambda force_refresh=False: {  # type: ignore[assignment]
        "status": "success",
        "mission_mode": "degraded",
        "trend_mode": "worsening",
        "volatility_mode": "elevated",
        "trend_pressure": 0.66,
        "at_risk_ratio": 0.58,
        "provider_policy_tuning_status": "success",
        "provider_policy_tuning_changed": True,
        "provider_policy_tuning_updated_count": 4,
    }
    service._bridge_scheduler_runtime_context = lambda force_refresh=False: {  # type: ignore[assignment]
        "rust_pressure_score": 0.84,
        "rust_recommended_parallel_cap": 1,
        "rust_retry_mode": "stabilize",
        "rust_suggested_retry_delay_s": 7.0,
        "external_trend_mode": "worsening",
        "external_volatility_mode": "elevated",
        "external_trend_pressure": 0.66,
        "external_at_risk_ratio": 0.58,
        "mission_mode": "degraded",
        "fetched_at": "2026-03-04T00:00:00+00:00",
    }

    payload = service.run_external_connector_preflight(  # noqa: SLF001
        args={"action": "external_email_send", "provider": "google", "args": {"to": ["a@example.com"]}},
        source="desktop-ui",
        metadata={},
    )

    orchestration = payload.get("orchestration_diagnostics", {})
    assert isinstance(orchestration, dict)
    assert str(orchestration.get("runtime_lane", "")).strip().lower() in {"serial", "limited_parallel"}
    tuning = orchestration.get("provider_policy_tuning", {})
    assert isinstance(tuning, dict)
    assert tuning.get("changed") is True
    assert int(tuning.get("updated_count", 0) or 0) >= 1
    hints = orchestration.get("remediation_hints", [])
    assert isinstance(hints, list)
    hint_ids = {
        str(row.get("id", "")).strip().lower()
        for row in hints
        if isinstance(row, dict)
    }
    assert "mission_provider_policy_autotuned" in hint_ids
    route_advisor = orchestration.get("route_weight_advisor", {})
    assert isinstance(route_advisor, dict)
    assert str(route_advisor.get("trigger_band", "")).strip().lower() in {"elevated", "critical"}


def test_simulate_external_connector_preflight_persists_advisor_context_and_compare() -> None:
    service, _ = _build_service()
    service._connector_simulation_history_lock = threading.RLock()
    service._connector_simulation_history = []
    service._connector_simulation_history_loaded = True
    service._connector_simulation_history_dirty = False
    service._connector_simulation_history_max = 120
    service._load_connector_simulation_history = lambda: None  # type: ignore[assignment]
    service._persist_connector_simulation_history = lambda force=False: None  # type: ignore[assignment]
    service.execute_action = lambda _action, _args, source="desktop-ui", metadata=None: {  # type: ignore[assignment]
        "status": "success",
        "output": {
            "status": "success",
            "action": "external_email_send",
            "provider": "google",
            "provider_selected": "google",
            "message": "Connector preflight passed.",
            "preflight_ready": True,
            "contract_diagnostic": {"code": "ready", "severity": "info", "message": "ok"},
            "remediation_hints": [],
        },
    }
    service.kernel.external_reliability_status = lambda provider="", limit=180: {  # type: ignore[attr-defined]
        "status": "success",
        "mission_outage_policy": {"mode": "stable", "bias": 0.0, "pressure_ema": 0.12, "profile": "balanced"},
        "items": [
            {
                "provider": provider or "google",
                "health_score": 0.84,
                "failure_ema": 0.1,
                "cooldown_active": False,
                "outage_active": False,
                "retry_after_s": 0.0,
            },
            {
                "provider": "graph",
                "health_score": 0.72,
                "failure_ema": 0.18,
                "cooldown_active": False,
                "outage_active": False,
                "retry_after_s": 2.0,
            },
        ],
    }

    baseline = service.simulate_external_connector_preflight(  # noqa: SLF001
        args={
            "action": "external_email_send",
            "provider": "google",
            "providers": ["google", "graph"],
            "args": {"to": ["a@example.com"], "subject": "Status"},
            "scenarios": [{"id": "baseline"}],
            "policy_autotune": False,
        },
        source="desktop-ui",
        metadata={},
    )
    assert baseline["status"] == "success"
    baseline_id = str(baseline.get("simulation_id", ""))
    assert baseline_id

    preflight = service.run_external_connector_preflight(  # noqa: SLF001
        args={"action": "external_email_send", "provider": "google", "args": {"to": ["a@example.com"], "subject": "Status"}},
        source="desktop-ui",
        metadata={},
    )
    template = preflight.get("advisor_simulation_template", {})
    assert isinstance(template, dict)
    payload = template.get("payload", {})
    assert isinstance(payload, dict)
    payload = dict(payload)
    payload["compare_against_simulation_id"] = baseline_id
    payload["policy_autotune"] = False

    advisor_run = service.simulate_external_connector_preflight(  # noqa: SLF001
        args=payload,
        source="desktop-ui",
        metadata={},
    )
    assert advisor_run["status"] == "success"
    assert isinstance(advisor_run.get("advisor_context", {}), dict)
    assert isinstance(advisor_run.get("comparison", {}), dict)
    assert str(advisor_run.get("compare_against_simulation_id", "")) == baseline_id
    assert int(advisor_run.get("execution_candidate_count", 0) or 0) >= 1
    latest_history = service._connector_simulation_history[-1]
    assert isinstance(latest_history.get("advisor_context", {}), dict)
    assert int(latest_history.get("execution_candidate_count", 0) or 0) >= 1


def test_run_external_connector_preflight_adds_candidate_approval_preview() -> None:
    service, _ = _build_service()
    service.execute_action = lambda _action, _args, source="desktop-ui", metadata=None: {  # type: ignore[assignment]
        "status": "success",
        "output": {
            "status": "success",
            "action": "external_email_send",
            "provider": "google",
            "provider_selected": "google",
            "message": "Connector preflight passed.",
            "preflight_ready": True,
            "contract_diagnostic": {"code": "ready", "severity": "info", "message": "ok"},
            "remediation_hints": [],
        },
    }
    service.kernel.external_reliability_status = lambda provider="", limit=180: {  # type: ignore[attr-defined]
        "status": "success",
        "mission_outage_policy": {"mode": "stable", "bias": 0.0, "pressure_ema": 0.12, "profile": "balanced"},
        "items": [
            {
                "provider": provider or "google",
                "health_score": 0.84,
                "failure_ema": 0.1,
                "cooldown_active": False,
                "outage_active": False,
                "retry_after_s": 0.0,
            }
        ],
    }

    payload = service.run_external_connector_preflight(  # noqa: SLF001
        args={"action": "external_email_send", "provider": "google", "args": {"to": ["a@example.com"], "subject": "Status"}},
        source="desktop-ui",
        metadata={},
    )

    approval_summary = payload.get("approval_summary", {})
    assert isinstance(approval_summary, dict)
    assert int(approval_summary.get("candidate_count", 0) or 0) >= 1
    candidates = payload.get("execution_candidates", [])
    assert isinstance(candidates, list)
    assert candidates and isinstance(candidates[0], dict)
    preview = candidates[0].get("approval_preview", {})
    assert isinstance(preview, dict)
    assert str(preview.get("posture", "")).strip().lower() in {
        "approval_required",
        "compare_required",
        "compare_then_approve",
        "review_required",
        "ready",
    }
    assert isinstance(preview.get("recommended_steps", []), list)


def test_external_connector_simulation_trends_and_promotion_apply() -> None:
    service, _ = _build_service()
    service._connector_simulation_history_lock = threading.RLock()
    service._connector_simulation_history = []
    service._connector_simulation_history_loaded = True
    service._connector_simulation_history_dirty = False
    service._connector_simulation_history_max = 120
    service._load_connector_simulation_history = lambda: None  # type: ignore[assignment]
    service._persist_connector_simulation_history = lambda force=False: None  # type: ignore[assignment]
    service.execute_action = lambda _action, _args, source="desktop-ui", metadata=None: {  # type: ignore[assignment]
        "status": "success",
        "output": {
            "status": "success",
            "action": "external_email_send",
            "provider": "google",
            "provider_selected": "google",
            "message": "Connector preflight passed.",
            "preflight_ready": True,
            "contract_diagnostic": {"code": "ready", "severity": "info", "message": "ok"},
            "remediation_hints": [],
        },
    }
    service.kernel.external_reliability_status = lambda provider="", limit=180: {  # type: ignore[attr-defined]
        "status": "success",
        "mission_outage_policy": {"mode": "stable", "bias": 0.0, "pressure_ema": 0.12, "profile": "balanced"},
        "items": [
            {
                "provider": provider or "google",
                "health_score": 0.84,
                "failure_ema": 0.1,
                "cooldown_active": False,
                "outage_active": False,
                "retry_after_s": 0.0,
            },
            {
                "provider": "graph",
                "health_score": 0.72,
                "failure_ema": 0.18,
                "cooldown_active": False,
                "outage_active": False,
                "retry_after_s": 1.0,
            },
        ],
    }

    simulation = service.simulate_external_connector_preflight(  # noqa: SLF001
        args={
            "action": "external_email_send",
            "provider": "google",
            "providers": ["google", "graph"],
            "args": {"to": ["a@example.com"], "subject": "Status"},
            "scenarios": [
                {"id": "baseline"},
                {
                    "id": "candidate_google",
                    "provider": "google",
                    "candidate_id": "candidate_google",
                    "execution_candidate": True,
                },
            ],
            "policy_autotune": False,
        },
        source="desktop-ui",
        metadata={},
    )
    assert simulation["status"] == "success"
    simulation_id = str(simulation.get("simulation_id", ""))
    assert simulation_id

    trends = service.external_connector_preflight_simulation_trends(
        action="external_email_send",
        provider="google",
        recent_window=8,
        baseline_window=24,
    )
    assert trends["status"] == "success"
    assert isinstance(trends.get("advisor_usage", {}), dict)
    assert isinstance(trends.get("approval_pressure", {}), dict)
    assert isinstance(trends.get("execution_readiness", {}), dict)
    assert isinstance(trends.get("promotion_readiness", {}), dict)

    promoted = service.external_connector_preflight_simulation_promote(
        simulation_id=simulation_id,
        require_compare=False,
        dry_run=False,
        force=True,
        mission_mode="stable",
        reason="unit_test_promote",
    )
    assert promoted["status"] == "applied"
    assert promoted["applied"] is True
    assert isinstance(promoted.get("promotion", {}), dict)
    assert isinstance(promoted.get("entry", {}), dict)


def test_external_connector_promotion_history_and_policy_restore() -> None:
    service, _ = _build_service()
    service._connector_simulation_history_lock = threading.RLock()
    service._connector_simulation_history = []
    service._connector_simulation_history_loaded = True
    service._connector_simulation_history_dirty = False
    service._connector_simulation_history_max = 120
    service._load_connector_simulation_history = lambda: None  # type: ignore[assignment]
    service._persist_connector_simulation_history = lambda force=False: None  # type: ignore[assignment]
    service.execute_action = lambda _action, _args, source="desktop-ui", metadata=None: {  # type: ignore[assignment]
        "status": "success",
        "output": {
            "status": "success",
            "action": "external_email_send",
            "provider": "google",
            "provider_selected": "google",
            "message": "Connector preflight passed.",
            "preflight_ready": True,
            "contract_diagnostic": {"code": "ready", "severity": "info", "message": "ok"},
            "remediation_hints": [],
        },
    }
    service.kernel.external_reliability_status = lambda provider="", limit=180: {  # type: ignore[attr-defined]
        "status": "success",
        "mission_outage_policy": {"mode": "stable", "bias": 0.0, "pressure_ema": 0.12, "profile": "balanced"},
        "items": [
            {
                "provider": provider or "google",
                "health_score": 0.84,
                "failure_ema": 0.1,
                "cooldown_active": False,
                "outage_active": False,
                "retry_after_s": 0.0,
            },
            {
                "provider": "graph",
                "health_score": 0.72,
                "failure_ema": 0.18,
                "cooldown_active": False,
                "outage_active": False,
                "retry_after_s": 1.0,
            },
        ],
    }

    simulation = service.simulate_external_connector_preflight(  # noqa: SLF001
        args={
            "action": "external_email_send",
            "provider": "google",
            "providers": ["google", "graph"],
            "args": {"to": ["a@example.com"], "subject": "Status"},
            "scenarios": [
                {"id": "baseline"},
                {
                    "id": "candidate_google",
                    "provider": "google",
                    "candidate_id": "candidate_google",
                    "execution_candidate": True,
                },
            ],
            "policy_autotune": False,
        },
        source="desktop-ui",
        metadata={},
    )
    simulation_id = str(simulation.get("simulation_id", ""))
    assert simulation["status"] == "success"
    assert simulation_id

    promoted = service.external_connector_preflight_simulation_promote(
        simulation_id=simulation_id,
        require_compare=False,
        dry_run=False,
        force=True,
        mission_mode="stable",
        reason="unit_test_promote_history",
    )
    assert promoted["status"] == "applied"
    apply_payload = promoted.get("apply", {}) if isinstance(promoted.get("apply"), dict) else {}
    promoted_entry = apply_payload.get("entry", {}) if isinstance(apply_payload.get("entry"), dict) else {}
    promoted_event_id = int(apply_payload.get("event_id", 0) or 0)
    assert promoted_event_id >= 1
    execution_contract = promoted.get("execution_contract", {}) if isinstance(promoted.get("execution_contract"), dict) else {}
    execution_contract_entry = (
        execution_contract.get("entry", {}) if isinstance(execution_contract.get("entry"), dict) else {}
    )
    execution_contract_event_id = int(execution_contract.get("event_id", 0) or 0)
    assert execution_contract_event_id >= 1
    assert str(execution_contract_entry.get("simulation_id", "")) == simulation_id

    contract_status = service.external_connector_execution_contract_status(
        action="external_email_send",
        provider="google",
        mission_mode="stable",
        include_history=True,
    )
    assert contract_status["status"] == "success"
    current_contract_entry = contract_status.get("entry", {}) if isinstance(contract_status.get("entry"), dict) else {}
    assert str(current_contract_entry.get("simulation_id", "")) == simulation_id
    assert int(current_contract_entry.get("candidate_count", 0) or 0) >= 1
    contract_history = contract_status.get("history", [])
    assert isinstance(contract_history, list)
    assert contract_history and int(contract_history[0].get("event_id", 0) or 0) == execution_contract_event_id

    promotions = service.external_connector_preflight_simulation_promotions(
        action="external_email_send",
        provider="google",
        mission_mode="stable",
        status="applied",
        applied_only=True,
    )
    assert promotions["status"] == "success"
    items = promotions.get("items", [])
    assert isinstance(items, list)
    assert items and isinstance(items[0], dict)
    assert str(items[0].get("simulation_id", "")) == simulation_id
    assert int(items[0].get("event_id", 0) or 0) == promoted_event_id

    overridden = service.external_connector_remediation_policy_apply(
        action="external_email_send",
        provider="google",
        mission_mode="stable",
        profile="strict",
        controls={
            "allow_high_risk": False,
            "max_steps": 4,
            "require_compare": True,
            "stop_on_blocked": True,
        },
        source="desktop-ui",
        reason="unit_test_override_policy",
        use_recommendation=False,
    )
    assert overridden["status"] == "success"
    assert int(overridden.get("event_id", 0) or 0) > promoted_event_id

    preview = service.external_connector_remediation_policy_restore(
        event_id=promoted_event_id,
        dry_run=True,
    )
    assert preview["status"] == "dry_run"
    diff = preview.get("diff", {})
    assert isinstance(diff, dict)
    assert bool(diff.get("profile_changed", False)) or int(diff.get("changed_control_count", 0) or 0) > 0

    restored = service.external_connector_remediation_policy_restore(
        event_id=promoted_event_id,
        dry_run=False,
        force=True,
        reason="unit_test_restore_policy",
    )
    assert restored["status"] == "applied"
    assert restored["applied"] is True
    restore_apply = restored.get("apply", {}) if isinstance(restored.get("apply"), dict) else {}
    assert int(restore_apply.get("event_id", 0) or 0) > int(overridden.get("event_id", 0) or 0)

    current = service.external_connector_remediation_policy_status(
        action="external_email_send",
        provider="google",
        mission_mode="stable",
        include_alerts=False,
    )
    current_entry = current.get("entry", {}) if isinstance(current.get("entry"), dict) else {}
    assert str(current_entry.get("profile", "")) == str(promoted_entry.get("profile", ""))
    assert current_entry.get("controls") == promoted_entry.get("controls")

    scope_key = "external_email_send|google|stable"
    live_contracts = service._connector_execution_contract_state.get("contracts", {})
    assert isinstance(live_contracts, dict)
    assert isinstance(live_contracts.get(scope_key), dict)
    live_contracts[scope_key]["selected_provider"] = "graph"
    live_contracts[scope_key]["route_signature"] = "graph:manual_override"
    service._connector_execution_contract_state["contracts"] = live_contracts

    contract_preview = service.external_connector_execution_contract_restore(
        event_id=execution_contract_event_id,
        dry_run=True,
    )
    assert contract_preview["status"] == "dry_run"
    contract_diff = contract_preview.get("diff", {})
    assert isinstance(contract_diff, dict)
    assert int(contract_diff.get("changed_field_count", 0) or 0) >= 1

    contract_restored = service.external_connector_execution_contract_restore(
        event_id=execution_contract_event_id,
        dry_run=False,
        force=True,
        reason="unit_test_restore_execution_contract",
    )
    assert contract_restored["status"] == "applied"
    restored_contract_status = service.external_connector_execution_contract_status(
        action="external_email_send",
        provider="google",
        mission_mode="stable",
        include_history=False,
    )
    restored_contract_entry = (
        restored_contract_status.get("entry", {}) if isinstance(restored_contract_status.get("entry"), dict) else {}
    )
    assert str(restored_contract_entry.get("selected_provider", "")) == str(
        execution_contract_entry.get("selected_provider", "")
    )
    assert str(restored_contract_entry.get("route_signature", "")) == str(execution_contract_entry.get("route_signature", ""))
