from __future__ import annotations

import asyncio

from backend.python.core.contracts import ActionResult, ExecutionPlan, GoalRecord, GoalRequest, PlanStep
from backend.python.core.kernel import AgentKernel


def test_resolve_goal_budget_reads_defaults(monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_GOAL_MAX_RUNTIME_S", "240")
    monkeypatch.setenv("JARVIS_GOAL_MAX_STEPS", "30")

    budget = AgentKernel._resolve_goal_budget("desktop-ui", metadata={})  # noqa: SLF001

    assert budget["max_runtime_s"] == 240
    assert budget["max_steps"] == 30


def test_resolve_goal_budget_applies_automation_caps(monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_GOAL_MAX_RUNTIME_S", "300")
    monkeypatch.setenv("JARVIS_GOAL_MAX_STEPS", "50")
    monkeypatch.setenv("JARVIS_AUTOMATION_MAX_RUNTIME_S", "90")
    monkeypatch.setenv("JARVIS_AUTOMATION_MAX_STEPS", "10")

    budget = AgentKernel._resolve_goal_budget("desktop-trigger", metadata={})  # noqa: SLF001

    assert budget["max_runtime_s"] == 90
    assert budget["max_steps"] == 10


def test_resolve_goal_budget_prefers_metadata_with_clamping(monkeypatch) -> None:
    monkeypatch.setenv("JARVIS_GOAL_MAX_RUNTIME_S", "180")
    monkeypatch.setenv("JARVIS_GOAL_MAX_STEPS", "24")
    monkeypatch.setenv("JARVIS_AUTOMATION_MAX_RUNTIME_S", "120")
    monkeypatch.setenv("JARVIS_AUTOMATION_MAX_STEPS", "12")

    budget = AgentKernel._resolve_goal_budget(  # noqa: SLF001
        "desktop-schedule",
        metadata={"max_runtime_s": "40", "max_steps": "5"},
    )

    assert budget["max_runtime_s"] == 40
    assert budget["max_steps"] == 5


def test_extract_replan_failure_context_uses_recovery_evidence() -> None:
    failed = ActionResult(
        action="open_url",
        status="failed",
        error="request timed out",
        attempt=3,
        evidence={
            "recovery": {
                "attempt": 3,
                "retry_count": 2,
                "last_category": "timeout",
                "retry_history": [
                    {"attempt": 1, "delay_s": 0.8, "category": "timeout", "reason": "Retrying"},
                    {"attempt": 2, "delay_s": 1.6, "category": "timeout", "reason": "Retrying"},
                ],
            }
        },
    )

    context = AgentKernel._extract_replan_failure_context(failed)  # noqa: SLF001

    assert context["last_failure_action"] == "open_url"
    assert context["last_failure_category"] == "timeout"
    assert context["last_failure_attempt"] == 3
    assert context["last_failure_retry_count"] == 2


def test_extract_replan_failure_context_classifies_from_error_when_missing_evidence() -> None:
    failed = ActionResult(action="write_file", status="failed", error="Approval required for write_file", attempt=1)

    context = AgentKernel._extract_replan_failure_context(failed)  # noqa: SLF001

    assert context["last_failure_action"] == "write_file"
    assert context["last_failure_category"] == "non_retryable"


def test_summarize_execution_feedback_tracks_confirm_and_quality() -> None:
    rows = [
        ActionResult(
            action="open_url",
            status="success",
            output={"status": "success"},
            duration_ms=180,
            evidence={"confirm_policy": {"satisfied": True}, "desktop_state": {"state_changed": True}},
        ),
        ActionResult(
            action="browser_read_dom",
            status="failed",
            error="request timed out",
            output={"status": "failed"},
            duration_ms=1200,
            evidence={"confirm_policy": {"satisfied": False}, "desktop_state": {"state_changed": False}},
        ),
    ]

    summary = AgentKernel._summarize_execution_feedback(rows)  # noqa: SLF001

    assert summary["window_size"] == 2
    assert summary["status_counts"]["success"] == 1
    assert summary["status_counts"]["failed"] == 1
    assert summary["confirm_checks_total"] == 2
    assert summary["confirm_checks_failed"] == 1
    assert summary["desktop_checks_total"] == 2
    assert summary["desktop_changed_count"] == 1
    assert 0.0 <= float(summary["quality_score"]) <= 1.0
    assert summary["latest_failed_action"] == "browser_read_dom"


def test_summarize_execution_feedback_includes_verification_pressure_signals() -> None:
    rows = [
        ActionResult(
            action="external_doc_update",
            status="success",
            output={"status": "success"},
            evidence={"confirm_policy": {"satisfied": True}},
        ),
        ActionResult(
            action="external_doc_update",
            status="failed",
            error="Verification confirm policy failed: mode=all, success=0/1, min_success=1.",
            output={"status": "failed"},
            evidence={"confirm_policy": {"satisfied": False}},
        ),
    ]

    summary = AgentKernel._summarize_execution_feedback(rows)  # noqa: SLF001

    assert int(summary.get("verification_signals", 0) or 0) >= 1
    assert int(summary.get("verification_failed", 0) or 0) >= 1
    assert float(summary.get("verification_failure_ratio", 0.0) or 0.0) > 0.0
    assert float(summary.get("verification_pressure", 0.0) or 0.0) > 0.0
    assert str(summary.get("verification_pressure_mode", "")) in {"moderate", "severe"}


def test_extract_replan_failure_context_includes_confirm_and_desktop_signals() -> None:
    failed = ActionResult(
        action="computer_click_target",
        status="failed",
        error="Verification confirm policy failed",
        attempt=2,
        evidence={
            "recovery": {"retry_count": 1, "last_category": "unknown", "retry_history": [{"attempt": 1, "delay_s": 0.5}]},
            "confirm_policy": {"mode": "all", "required": True, "satisfied": False, "success_count": 0, "total_count": 2},
            "desktop_state": {"state_changed": False, "change_count": 0, "state_hash": "hash_x"},
        },
    )

    context = AgentKernel._extract_replan_failure_context(failed)  # noqa: SLF001

    confirm = context["last_failure_confirm_policy"]
    desktop = context["last_failure_desktop_state"]
    assert isinstance(confirm, dict)
    assert confirm["satisfied"] is False
    assert confirm["total_count"] == 2
    assert isinstance(desktop, dict)
    assert desktop["state_changed"] is False
    assert desktop["state_hash"] == "hash_x"


def test_extract_replan_failure_context_includes_external_and_anchor_signals() -> None:
    failed = ActionResult(
        action="external_task_update",
        status="failed",
        error="request timed out",
        attempt=2,
        evidence={
            "external_reliability_preflight": {
                "provider_routing": {
                    "strategy": "fallback_ranked",
                    "selected_provider": "google",
                    "selected_health_score": 0.31,
                    "blocked_providers": [{"provider": "graph", "retry_after_s": 12.0}],
                },
                "retry_hint": {"base_delay_s": 1.8},
            },
            "desktop_anchor": {
                "action": "computer_find_text_targets",
                "confidence": 0.37,
                "required": True,
                "fallback_used": True,
                "chain": [{"action": "accessibility_find_element"}, {"action": "computer_find_text_targets"}],
            },
            "desktop_state": {"state_changed": True, "change_count": 2, "state_hash": "hash_a", "window_transition": True},
        },
    )

    context = AgentKernel._extract_replan_failure_context(failed)  # noqa: SLF001

    external = context["last_failure_external_reliability"]
    anchor = context["last_failure_desktop_anchor"]
    desktop = context["last_failure_desktop_state"]
    assert isinstance(external, dict)
    assert external["strategy"] == "fallback_ranked"
    assert external["selected_provider"] == "google"
    assert external["selected_health_score"] == 0.31
    assert external["blocked_provider_count"] == 1
    assert isinstance(anchor, dict)
    assert anchor["fallback_used"] is True
    assert anchor["chain_length"] == 2
    assert isinstance(desktop, dict)
    assert desktop["window_transition"] is True


def test_extract_replan_failure_context_includes_external_contract_and_request_args() -> None:
    failed = ActionResult(
        action="external_doc_read",
        status="failed",
        error="Provider contract failed",
        attempt=1,
        evidence={
            "request": {
                "action": "external_doc_read",
                "args": {"provider": "smtp", "document_id": "doc-42"},
                "source": "desktop-ui",
            },
            "external_reliability": {
                "status": "error",
                "message": "provider must be one of: google, graph",
                "contract_diagnostic": {
                    "code": "provider_not_supported_for_action",
                    "severity": "error",
                    "message": "provider must be one of: google, graph",
                    "requested_provider": "smtp",
                    "allowed_providers": ["google", "graph"],
                    "fields": [],
                    "any_of": [],
                },
                "auth_preflight": {
                    "required_min_ttl_s": 300,
                    "auth_rows": [{"provider": "google", "status": "blocked"}],
                },
                "remediation_hints": [{"id": "switch_provider", "summary": "Use a supported provider."}],
            },
        },
    )

    context = AgentKernel._extract_replan_failure_context(failed)  # noqa: SLF001

    external_contract = context["last_failure_external_contract"]
    request = context["last_failure_request"]
    reliability = context["last_failure_external_reliability"]
    assert isinstance(external_contract, dict)
    assert external_contract["code"] == "provider_not_supported_for_action"
    assert external_contract["requested_provider"] == "smtp"
    assert external_contract["allowed_providers"] == ["google", "graph"]
    assert external_contract["auth_blocked_providers"] == ["google"]
    assert isinstance(request, dict)
    assert request["source"] == "desktop-ui"
    assert request["args"]["provider"] == "smtp"
    assert isinstance(reliability, dict)
    assert reliability["preflight_status"] == "error"


def test_extract_replan_failure_context_captures_runtime_reliability_contract_details() -> None:
    failed = ActionResult(
        action="external_email_send",
        status="failed",
        error="External provider cooldown active",
        attempt=2,
        evidence={
            "external_reliability_preflight": {
                "status": "blocked",
                "retry_after_s": 17.4,
                "contract_diagnostic": {
                    "code": "provider_cooldown_blocked",
                    "severity": "warning",
                    "severity_score": 0.76,
                    "blocking_class": "provider",
                    "estimated_recovery_s": 420,
                    "automation_tier": "assisted",
                    "message": "External provider cooldown active for action 'external_email_send'.",
                    "blocked_providers": ["google"],
                    "retry_after_s": 17.4,
                    "diagnostics": {"blocked_ratio": 1.0, "reason": "cooldown"},
                    "remediation_contract": {
                        "automation_tier": "assisted",
                        "execution_contract": {
                            "mode": "assisted",
                            "max_retry_attempts": 1,
                            "verification": {"allow_provider_reroute": False},
                            "stop_conditions": ["manual_escalation", "checkpoint_failure"],
                        },
                    },
                    "checks": [
                        {"check": "provider_availability", "status": "failed", "severity": "error", "details": {"blocked_ratio": 1.0}},
                        {"check": "provider_cooldown", "status": "failed", "severity": "warning", "details": {"retry_after_s": 17.4}},
                    ],
                    "remediation_plan": [
                        {"phase": "diagnose", "objective": "check connector"},
                        {"phase": "retry", "verification": {"expect_status": "success"}},
                    ],
                },
            }
        },
    )

    context = AgentKernel._extract_replan_failure_context(failed)  # noqa: SLF001
    reliability = context["last_failure_external_reliability"]
    contract = context["last_failure_external_contract"]

    assert isinstance(reliability, dict)
    assert reliability["preflight_status"] == "blocked"
    assert reliability["contract_code"] == "provider_cooldown_blocked"
    assert reliability["retry_after_s"] == 17.4
    assert reliability["blocked_ratio"] == 1.0
    assert reliability["runtime_blocked_providers"] == ["google"]
    assert reliability["severity_score"] == 0.76
    assert reliability["blocking_class"] == "provider"
    assert reliability["estimated_recovery_s"] == 420
    assert reliability["automation_tier"] == "assisted"
    assert reliability["execution_mode"] == "assisted"
    assert reliability["execution_max_retry_attempts"] == 1
    assert reliability["allow_provider_reroute"] is False

    assert isinstance(contract, dict)
    assert contract["code"] == "provider_cooldown_blocked"
    assert contract["retry_after_s"] == 17.4
    assert contract["blocked_ratio"] == 1.0
    assert contract["blocked_providers"] == ["google"]
    assert contract["severity_score"] == 0.76
    assert contract["blocking_class"] == "provider"
    assert contract["estimated_recovery_s"] == 420
    assert contract["automation_tier"] == "assisted"
    execution_contract = contract.get("execution_contract", {})
    assert isinstance(execution_contract, dict)
    assert execution_contract["mode"] == "assisted"
    assert execution_contract["max_retry_attempts"] == 1
    assert execution_contract["allow_provider_reroute"] is False
    stop_conditions = execution_contract.get("stop_conditions", [])
    assert isinstance(stop_conditions, list)
    assert "manual_escalation" in [str(item) for item in stop_conditions]
    checks = contract.get("checks", [])
    assert isinstance(checks, list)
    assert any(
        isinstance(row, dict)
        and str(row.get("check", "")).strip().lower() == "provider_cooldown"
        for row in checks
    )
    remediation_plan = contract.get("remediation_plan", [])
    assert isinstance(remediation_plan, list)
    assert any(
        isinstance(row, dict)
        and str(row.get("phase", "")).strip().lower() == "retry"
        for row in remediation_plan
    )


def test_compact_mission_feedback_returns_core_operational_signals() -> None:
    payload = AgentKernel._compact_mission_feedback(  # noqa: SLF001
        {
            "status": "success",
            "mission_status": "failed",
            "risk": {"score": 0.73, "level": "high"},
            "quality": {
                "score": 0.41,
                "level": "low",
                "recommended_recovery_profile": "safe",
                "recommended_verification_strictness": "strict",
            },
            "resume": {"ready": True, "remaining_steps": 2},
            "hotspots": {
                "retry": [{"step_id": "s2", "attempts": 3, "action": "browser_read_dom"}],
                "failures": [{"step_id": "s2", "status": "failed", "action": "browser_read_dom"}],
            },
        }
    )

    assert payload["mission_status"] == "failed"
    assert payload["risk_level"] == "high"
    assert payload["quality_level"] == "low"
    assert payload["recommended_recovery_profile"] == "safe"
    assert payload["recommended_verification_strictness"] == "strict"
    assert payload["resume_ready"] is True
    assert payload["top_retry_hotspot"]["step_id"] == "s2"


def test_planner_reliability_context_includes_external_reliability_trend_summary() -> None:
    class _BreakerStub:
        def snapshot(self, *, limit: int) -> dict[str, object]:
            assert limit == 500
            return {"items": []}

    class _ExternalReliabilityStub:
        def snapshot(self, *, limit: int) -> dict[str, object]:
            assert limit in {120, 260}
            return {
                "status": "success",
                "mission_outage_policy": {
                    "mode": "worsening",
                    "profile": "defensive",
                    "bias": 0.24,
                    "pressure_ema": 0.62,
                    "failed_ratio_ema": 0.41,
                    "blocked_ratio_ema": 0.19,
                },
                "items": [
                    {
                        "provider": "google",
                        "cooldown_until": "2999-01-01T00:00:00+00:00",
                        "health_score": 0.34,
                        "failure_ema": 0.72,
                        "failure_trend_ema": 0.28,
                        "outage_active": True,
                        "outage_ema": 0.66,
                        "outage_mission_pressure": 0.61,
                        "mission_profile_alignment": -0.22,
                        "consecutive_failures": 4,
                        "samples": 40,
                        "top_action_risks": [{"action": "external_doc_update", "failure_ema": 0.77}],
                        "top_operation_risks": [{"operation": "mutate", "failure_ema": 0.73}],
                    },
                    {
                        "provider": "graph",
                        "cooldown_until": "",
                        "health_score": 0.87,
                        "failure_ema": 0.12,
                        "failure_trend_ema": -0.08,
                        "outage_active": False,
                        "outage_ema": 0.08,
                        "outage_mission_pressure": 0.12,
                        "mission_profile_alignment": 0.16,
                        "consecutive_failures": 0,
                        "samples": 52,
                        "top_action_risks": [{"action": "external_doc_update", "failure_ema": 0.18}],
                        "top_operation_risks": [{"operation": "mutate", "failure_ema": 0.16}],
                    },
                ],
            }

    kernel = AgentKernel.__new__(AgentKernel)
    kernel.action_circuit_breaker = _BreakerStub()
    kernel.external_reliability = _ExternalReliabilityStub()
    kernel._runtime_mission_trend_feedback = lambda force=False: {}  # type: ignore[method-assign]

    context = kernel._planner_reliability_context()  # noqa: SLF001

    trend = context.get("external_reliability_trend", {})
    assert isinstance(trend, dict)
    assert trend.get("provider_count") == 2
    assert str(trend.get("mode", "")) == "worsening"
    assert str(trend.get("mission_profile", "")) == "defensive"
    assert int(trend.get("cooldown_active_count", 0) or 0) >= 1
    top_risks = trend.get("top_provider_risks", [])
    assert isinstance(top_risks, list)
    assert top_risks
    assert str(top_risks[0].get("provider", "")) == "google"
    mission_analysis = context.get("external_reliability_mission_analysis", {})
    assert isinstance(mission_analysis, dict)
    assert float(mission_analysis.get("volatility_index", 0.0) or 0.0) >= 0.0


def test_external_reliability_mission_analysis_reports_volatility_and_recommendations() -> None:
    class _ExternalReliabilityStub:
        def snapshot(self, *, limit: int) -> dict[str, object]:
            assert limit == 260
            return {
                "status": "success",
                "mission_outage_policy": {
                    "mode": "worsening",
                    "profile": "defensive",
                    "bias": 0.31,
                    "pressure_ema": 0.64,
                    "risk_ema": 0.58,
                    "quality_ema": 0.42,
                    "failed_ratio_ema": 0.37,
                    "blocked_ratio_ema": 0.21,
                    "profile_history": [
                        {"profile": "balanced", "mode": "stable", "target_pressure": 0.24},
                        {"profile": "defensive", "mode": "worsening", "target_pressure": 0.58},
                        {"profile": "aggressive", "mode": "improving", "target_pressure": 0.41},
                        {"profile": "defensive", "mode": "worsening", "target_pressure": 0.66},
                    ],
                },
                "items": [
                    {
                        "provider": "google",
                        "cooldown_until": "2999-01-01T00:00:00+00:00",
                        "health_score": 0.31,
                        "failure_ema": 0.74,
                        "failure_trend_ema": 0.26,
                        "outage_active": True,
                        "outage_ema": 0.72,
                        "mission_profile_alignment": -0.21,
                        "samples": 42,
                    },
                    {
                        "provider": "graph",
                        "cooldown_until": "",
                        "health_score": 0.82,
                        "failure_ema": 0.18,
                        "failure_trend_ema": -0.04,
                        "outage_active": False,
                        "outage_ema": 0.09,
                        "mission_profile_alignment": 0.14,
                        "samples": 53,
                    },
                ],
            }

    kernel = AgentKernel.__new__(AgentKernel)
    kernel.external_reliability = _ExternalReliabilityStub()

    analysis = kernel.external_reliability_mission_analysis()  # noqa: SLF001
    assert analysis["status"] == "success"
    profile_analysis = analysis.get("profile_history_analysis", {})
    assert isinstance(profile_analysis, dict)
    assert float(profile_analysis.get("volatility_index", 0.0) or 0.0) > 0.0
    assert str(profile_analysis.get("volatility_mode", "")).strip().lower() in {"elevated", "surging", "stable", "calm"}
    provider_analysis = analysis.get("provider_risk_analysis", {})
    assert isinstance(provider_analysis, dict)
    assert int(provider_analysis.get("at_risk_count", 0) or 0) >= 1
    recommendations = analysis.get("recommendations", [])
    assert isinstance(recommendations, list)
    assert recommendations


def test_external_reliability_mission_analysis_records_history_and_provider_tuning() -> None:
    class _ExternalReliabilityStub:
        def __init__(self) -> None:
            self.record_calls = 0
            self.tune_calls = 0

        def snapshot(self, *, limit: int) -> dict[str, object]:
            assert limit == 260
            return {
                "status": "success",
                "mission_outage_policy": {
                    "mode": "worsening",
                    "profile": "defensive",
                    "bias": 0.27,
                    "pressure_ema": 0.58,
                    "risk_ema": 0.52,
                    "quality_ema": 0.45,
                    "failed_ratio_ema": 0.32,
                    "blocked_ratio_ema": 0.2,
                    "profile_history": [
                        {"profile": "balanced", "mode": "stable", "target_pressure": 0.24},
                        {"profile": "defensive", "mode": "worsening", "target_pressure": 0.62},
                    ],
                },
                "items": [
                    {
                        "provider": "google",
                        "cooldown_until": "2999-01-01T00:00:00+00:00",
                        "health_score": 0.33,
                        "failure_ema": 0.72,
                        "failure_trend_ema": 0.2,
                        "outage_active": True,
                        "outage_ema": 0.68,
                        "mission_profile_alignment": -0.18,
                        "samples": 32,
                    },
                    {
                        "provider": "graph",
                        "cooldown_until": "",
                        "health_score": 0.84,
                        "failure_ema": 0.12,
                        "failure_trend_ema": -0.07,
                        "outage_active": False,
                        "outage_ema": 0.08,
                        "mission_profile_alignment": 0.14,
                        "samples": 47,
                    },
                ],
            }

        def record_mission_analysis(self, *, analysis: dict[str, object], reason: str, dry_run: bool) -> dict[str, object]:
            self.record_calls += 1
            assert reason == "kernel_external_reliability_mission_analysis"
            assert dry_run is False
            return {
                "status": "success",
                "recorded": True,
                "delta_score": 0.19,
                "elapsed_s": 120.0,
                "drift": {"mode": "worsening", "drift_score": 0.62, "switch_pressure": 0.4},
            }

        def tune_provider_policy_from_mission_analysis(
            self,
            *,
            analysis: dict[str, object],
            dry_run: bool,
            reason: str,
        ) -> dict[str, object]:
            self.tune_calls += 1
            assert reason == "kernel_external_reliability_mission_analysis"
            return {
                "status": "success",
                "changed": True,
                "dry_run": dry_run,
                "updated_count": 2,
                "mission_mode": "worsening",
                "mission_profile": "defensive",
            }

        def mission_analysis_history(self, *, limit: int, window: int) -> dict[str, object]:
            assert limit > 0
            assert window > 0
            return {
                "status": "success",
                "count": 2,
                "total": 2,
                "diagnostics": {"mode": "worsening", "drift_score": 0.63},
                "items": [],
            }

    class _TelemetryStub:
        def __init__(self) -> None:
            self.events: list[tuple[str, dict[str, object]]] = []

        def emit(self, event: str, payload: dict[str, object]) -> None:
            self.events.append((event, payload))

    kernel = AgentKernel.__new__(AgentKernel)
    kernel.external_reliability = _ExternalReliabilityStub()
    kernel.external_reliability_provider_policy_autotune_enabled = True
    kernel.external_reliability_provider_policy_autotune_interval_s = 15
    kernel.external_reliability_provider_policy_autotune_dry_run = False
    kernel.external_reliability_mission_history_limit = 120
    kernel.external_reliability_mission_history_window = 24
    kernel._last_external_provider_policy_autotune_monotonic = 0.0
    kernel.telemetry = _TelemetryStub()

    analysis = kernel.external_reliability_mission_analysis()  # noqa: SLF001

    assert analysis["status"] == "success"
    record = analysis.get("mission_history_record", {})
    assert isinstance(record, dict)
    assert record.get("recorded") is True
    drift = analysis.get("mission_history_drift", {})
    assert isinstance(drift, dict)
    assert str(drift.get("mode", "")) == "worsening"
    tuning = analysis.get("provider_policy_tuning", {})
    assert isinstance(tuning, dict)
    assert tuning.get("changed") is True
    history = analysis.get("mission_history", {})
    assert isinstance(history, dict)
    assert int(history.get("count", 0) or 0) >= 1
    assert kernel.external_reliability.record_calls == 1  # type: ignore[attr-defined]
    assert kernel.external_reliability.tune_calls == 1  # type: ignore[attr-defined]
    assert kernel.telemetry.events  # type: ignore[attr-defined]


def test_periodic_external_reliability_analysis_emits_telemetry_once_per_interval() -> None:
    class _TelemetryStub:
        def __init__(self) -> None:
            self.events: list[tuple[str, dict[str, object]]] = []

        def emit(self, event: str, payload: dict[str, object]) -> None:
            self.events.append((event, payload))

    class _LogStub:
        @staticmethod
        def warning(_message: str) -> None:
            return None

    kernel = AgentKernel.__new__(AgentKernel)
    kernel.external_reliability_analysis_auto_emit_enabled = True
    kernel.external_reliability_analysis_auto_emit_interval_s = 60
    kernel.external_reliability_analysis_provider_limit = 200
    kernel.external_reliability_analysis_history_limit = 20
    kernel._last_external_reliability_analysis_monotonic = 0.0
    kernel._last_external_reliability_analysis = {}
    kernel.telemetry = _TelemetryStub()
    kernel.log = _LogStub()
    kernel.external_reliability_mission_analysis = lambda provider_limit=260, history_limit=40: {  # type: ignore[method-assign]
        "status": "success",
        "generated_at": "2026-03-03T00:00:00+00:00",
        "provider_count": 2,
        "profile_history_analysis": {"volatility_mode": "elevated", "volatility_index": 0.57},
        "provider_risk_analysis": {"at_risk_count": 1},
        "trend": {"trend_pressure": 0.61, "mode": "worsening"},
        "recommendations": [{"type": "autotune"}],
    }

    asyncio.run(kernel._run_periodic_external_reliability_analysis())  # noqa: SLF001
    assert kernel._last_external_reliability_analysis["status"] == "success"  # noqa: SLF001
    assert kernel.telemetry.events  # type: ignore[attr-defined]
    event_name, payload = kernel.telemetry.events[-1]  # type: ignore[attr-defined]
    assert event_name == "external_reliability.mission_analysis"
    assert payload["volatility_mode"] == "elevated"

    emitted_count = len(kernel.telemetry.events)  # type: ignore[attr-defined]
    asyncio.run(kernel._run_periodic_external_reliability_analysis())  # noqa: SLF001
    assert len(kernel.telemetry.events) == emitted_count  # type: ignore[attr-defined]


def test_resolve_replan_policy_honors_metadata_and_automation_caps() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.max_replans = 5
    kernel.replan_delay_base_s = 0.0
    kernel.replan_allow_blocked = False
    kernel.replan_allow_non_retryable = False
    kernel.replan_escalate_recovery_profile = True
    kernel.replan_escalate_verification = True
    kernel.replan_escalate_policy_profile = True

    policy = kernel._resolve_replan_policy(  # noqa: SLF001
        source_name="desktop-trigger",
        metadata={
            "max_replans": 6,
            "replan_allow_blocked": True,
            "replan_allow_non_retryable": True,
            "replan_category_limits": {"timeout": 4, "non_retryable": 1},
            "replan_delay_base_s": 0.4,
        },
    )

    # Trigger automation caps max replans to avoid runaway retries.
    assert policy["max_replans"] == 2
    assert policy["allow_blocked"] is True
    assert policy["allow_non_retryable"] is True
    assert policy["category_limits"]["timeout"] == 4
    assert policy["category_limits"]["non_retryable"] == 1
    assert policy["delay_base_s"] == 0.4


def test_should_replan_after_failure_stops_non_retryable_by_default() -> None:
    failed = ActionResult(action="write_file", status="failed", error="invalid argument")

    should_replan, reason = AgentKernel._should_replan_after_failure(  # noqa: SLF001
        failed=failed,
        attempt=0,
        failure_category="non_retryable",
        policy={
            "max_replans": 2,
            "allow_blocked": False,
            "allow_non_retryable": False,
            "category_limits": {"non_retryable": 0},
        },
    )

    assert should_replan is False
    assert "non-retryable" in reason


def test_derive_replan_overrides_escalates_profiles_for_hard_failures() -> None:
    kernel = AgentKernel.__new__(AgentKernel)

    updates = kernel._derive_replan_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "standard",
        },
        context={
            "last_failure_category": "non_retryable",
            "last_failure_retry_count": 2,
        },
        policy={
            "escalate_recovery_profile": True,
            "escalate_verification": True,
            "escalate_policy_profile": True,
        },
        replan_attempt=1,
    )

    assert updates["policy_profile"] == "automation_safe"
    assert updates["recovery_profile"] == "safe"
    assert updates["verification_strictness"] == "strict"


def test_derive_replan_overrides_uses_external_and_anchor_pressure() -> None:
    kernel = AgentKernel.__new__(AgentKernel)

    updates = kernel._derive_replan_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "standard",
        },
        context={
            "last_failure_category": "timeout",
            "last_failure_retry_count": 1,
            "last_failure_external_reliability": {
                "strategy": "fallback_ranked",
                "selected_provider": "google",
                "selected_health_score": 0.31,
                "blocked_provider_count": 1,
            },
            "last_failure_desktop_anchor": {
                "fallback_used": True,
                "confidence": 0.33,
            },
            "last_failure_desktop_state": {"window_transition": True},
        },
        policy={
            "escalate_recovery_profile": True,
            "escalate_verification": True,
            "escalate_policy_profile": True,
        },
        replan_attempt=1,
    )

    assert updates["policy_profile"] == "automation_safe"
    assert updates["verification_strictness"] == "strict"
    assert updates["recovery_profile"] in {"balanced", "safe"}


def test_derive_replan_overrides_escalates_on_external_contract_pressure() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_contract_severe_threshold = 0.62

    updates = kernel._derive_replan_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "standard",
        },
        context={
            "last_failure_category": "transient",
            "last_failure_retry_count": 0,
            "last_failure_external_contract": {
                "code": "provider_not_supported_for_action",
                "severity": "error",
            },
        },
        policy={
            "escalate_recovery_profile": True,
            "escalate_verification": True,
            "escalate_policy_profile": True,
        },
        replan_attempt=1,
    )

    assert updates["policy_profile"] == "automation_safe"
    assert updates["verification_strictness"] == "strict"
    assert updates["recovery_profile"] in {"balanced", "safe"}


def test_derive_runtime_adaptive_overrides_hardens_policy_under_operational_pressure() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = False
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56

    updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "off",
        },
        context={
            "execution_feedback": {
                "quality_score": 0.41,
                "confirm_failure_ratio": 0.52,
                "latest_failure_category": "unknown",
                "desktop_change_rate": 0.12,
            },
            "external_provider_health": [
                {"provider": "google", "cooldown_active": True, "failure_ema": 0.78, "health_score": 0.28},
                {"provider": "graph", "cooldown_active": False, "failure_ema": 0.66, "health_score": 0.35},
            ],
            "open_action_circuits": [{"action": "external_doc_update"}],
            "mission_feedback": {"risk_level": "high", "quality_level": "low"},
        },
        attempt=1,
    )

    assert updates["policy_profile"] == "automation_safe"
    assert updates["recovery_profile"] == "safe"
    assert updates["verification_strictness"] in {"standard", "strict"}


def test_derive_runtime_adaptive_overrides_hardens_on_external_contract_pressure() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = False
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56
    kernel.runtime_policy_contract_guardrail_enabled = True
    kernel.runtime_policy_contract_pressure_threshold = 0.38
    kernel.runtime_policy_contract_severe_threshold = 0.62

    updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "off",
        },
        context={
            "execution_feedback": {
                "quality_score": 0.86,
                "confirm_failure_ratio": 0.06,
                "latest_failure_category": "transient",
                "desktop_change_rate": 0.92,
            },
            "external_provider_health": [],
            "open_action_circuits": [],
            "mission_feedback": {"risk_level": "low", "quality_level": "high"},
            "last_failure_action": "external_email_read",
            "last_failure_external_contract": {
                "code": "auth_preflight_failed",
                "severity": "error",
            },
        },
        attempt=1,
    )

    assert updates["policy_profile"] == "automation_safe"
    assert updates["recovery_profile"] == "safe"
    assert updates["verification_strictness"] in {"standard", "strict"}


def test_derive_runtime_adaptive_overrides_sets_execution_controls_from_contract_signal() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = False
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56
    kernel.runtime_policy_contract_guardrail_enabled = True
    kernel.runtime_policy_contract_pressure_threshold = 0.38
    kernel.runtime_policy_contract_severe_threshold = 0.62

    updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "off",
        },
        context={
            "execution_feedback": {
                "quality_score": 0.82,
                "confirm_failure_ratio": 0.07,
                "latest_failure_category": "transient",
                "desktop_change_rate": 0.86,
            },
            "external_provider_health": [{"provider": "google", "cooldown_active": True, "failure_ema": 0.74, "health_score": 0.3}],
            "open_action_circuits": [],
            "mission_feedback": {"risk_level": "low", "quality_level": "high"},
            "last_failure_action": "external_doc_update",
            "last_failure_external_contract": {
                "code": "provider_outage_blocked",
                "severity": "error",
                "severity_score": 0.84,
                "blocking_class": "provider",
                "estimated_recovery_s": 1400,
                "automation_tier": "assisted",
                "execution_contract": {
                    "mode": "manual",
                    "max_retry_attempts": 1,
                    "allow_provider_reroute": False,
                    "stop_conditions": ["manual_escalation", "provider_reroute_locked"],
                },
            },
            "last_failure_external_reliability": {
                "preflight_status": "blocked",
            },
        },
        attempt=1,
    )

    assert updates["external_remediation_execution_mode"] == "assisted"
    assert updates["external_remediation_checkpoint_mode"] in {"standard", "strict"}
    assert updates["external_remediation_allow_provider_reroute"] == "false"


def test_derive_runtime_adaptive_overrides_hardens_on_failed_remediation_loops() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = False
    kernel.runtime_policy_signal_smoothing_enabled = False
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56
    kernel.runtime_policy_remediation_feedback_enabled = True
    kernel.runtime_policy_remediation_hard_floor = 0.34
    kernel.runtime_policy_remediation_relief_floor = 0.74
    kernel.runtime_policy_remediation_min_samples = 2

    updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "off",
        },
        context={
            "execution_feedback": {
                "quality_score": 0.9,
                "confirm_failure_ratio": 0.06,
                "latest_failure_category": "transient",
                "desktop_change_rate": 0.88,
                "remediation_attempted": 5,
                "remediation_success_rate": 0.12,
                "remediation_checkpoint_blocked_ratio": 0.74,
                "remediation_contract_risk": 0.84,
            },
            "external_provider_health": [
                {"provider": "google", "cooldown_active": False, "failure_ema": 0.72, "health_score": 0.31},
                {"provider": "graph", "cooldown_active": False, "failure_ema": 0.21, "health_score": 0.82},
                {"provider": "smtp", "cooldown_active": False, "failure_ema": 0.69, "health_score": 0.38},
                {"provider": "azure", "cooldown_active": False, "failure_ema": 0.2, "health_score": 0.86},
            ],
            "open_action_circuits": [],
            "mission_feedback": {"risk_level": "low", "quality_level": "high"},
        },
        attempt=1,
    )

    assert updates["policy_profile"] == "automation_safe"
    assert updates["recovery_profile"] == "safe"
    assert updates["verification_strictness"] in {"standard", "strict"}


def test_derive_runtime_adaptive_overrides_relieves_pressure_on_successful_remediation_loops() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = False
    kernel.runtime_policy_signal_smoothing_enabled = False
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56
    kernel.runtime_policy_remediation_feedback_enabled = True
    kernel.runtime_policy_remediation_hard_floor = 0.34
    kernel.runtime_policy_remediation_relief_floor = 0.74
    kernel.runtime_policy_remediation_min_samples = 2

    updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "balanced",
            "verification_strictness": "standard",
        },
        context={
            "execution_feedback": {
                "quality_score": 0.93,
                "confirm_failure_ratio": 0.04,
                "latest_failure_category": "transient",
                "desktop_change_rate": 0.9,
                "remediation_attempted": 6,
                "remediation_success_rate": 0.94,
                "remediation_checkpoint_blocked_ratio": 0.0,
                "remediation_contract_risk": 0.12,
            },
            "external_provider_health": [
                {"provider": "google", "cooldown_active": False, "failure_ema": 0.72, "health_score": 0.31},
                {"provider": "graph", "cooldown_active": False, "failure_ema": 0.21, "health_score": 0.82},
                {"provider": "smtp", "cooldown_active": False, "failure_ema": 0.69, "health_score": 0.38},
                {"provider": "azure", "cooldown_active": False, "failure_ema": 0.2, "health_score": 0.86},
            ],
            "open_action_circuits": [],
            "mission_feedback": {"risk_level": "low", "quality_level": "high"},
        },
        attempt=1,
    )

    assert "policy_profile" not in updates
    assert "recovery_profile" not in updates
    assert "verification_strictness" not in updates


def test_derive_runtime_adaptive_overrides_hardens_on_external_mission_drift() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = False
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56
    kernel.runtime_policy_signal_smoothing_enabled = False
    kernel.runtime_policy_mission_drift_enabled = True
    kernel.runtime_policy_mission_drift_weight = 0.46
    kernel.runtime_policy_mission_drift_relief_weight = 0.18
    kernel.runtime_policy_mission_drift_severe_threshold = 0.62
    kernel.runtime_policy_provider_policy_relief_enabled = True
    kernel.runtime_policy_provider_policy_relief_gain = 0.12

    updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "off",
        },
        context={
            "execution_feedback": {
                "quality_score": 0.91,
                "confirm_failure_ratio": 0.04,
                "latest_failure_category": "transient",
                "desktop_change_rate": 0.92,
            },
            "external_provider_health": [{"provider": "google", "cooldown_active": False, "failure_ema": 0.08, "health_score": 0.92}],
            "open_action_circuits": [],
            "mission_feedback": {"risk_level": "low", "quality_level": "high"},
            "external_reliability_mission_analysis": {
                "drift_mode": "severe",
                "drift_score": 0.84,
                "provider_policy_changed": False,
                "provider_policy_updated_count": 0,
            },
        },
        attempt=1,
    )

    assert updates["policy_profile"] == "automation_safe"
    assert updates["recovery_profile"] == "safe"
    assert updates["verification_strictness"] in {"standard", "strict"}


def test_derive_runtime_adaptive_overrides_relieves_pressure_after_provider_policy_tuning() -> None:
    base_kernel = AgentKernel.__new__(AgentKernel)
    base_kernel.runtime_policy_adaptation_enabled = True
    base_kernel.runtime_policy_auto_upgrade = False
    base_kernel.runtime_policy_external_pressure_threshold = 0.48
    base_kernel.runtime_policy_quality_floor = 0.56
    base_kernel.runtime_policy_signal_smoothing_enabled = False
    base_kernel.runtime_policy_mission_drift_enabled = True
    base_kernel.runtime_policy_mission_drift_weight = 0.32
    base_kernel.runtime_policy_mission_drift_relief_weight = 0.18
    base_kernel.runtime_policy_mission_drift_severe_threshold = 0.66
    base_kernel.runtime_policy_provider_policy_relief_enabled = True
    base_kernel.runtime_policy_provider_policy_relief_gain = 0.16

    tuned_kernel = AgentKernel.__new__(AgentKernel)
    tuned_kernel.runtime_policy_adaptation_enabled = True
    tuned_kernel.runtime_policy_auto_upgrade = False
    tuned_kernel.runtime_policy_external_pressure_threshold = 0.48
    tuned_kernel.runtime_policy_quality_floor = 0.56
    tuned_kernel.runtime_policy_signal_smoothing_enabled = False
    tuned_kernel.runtime_policy_mission_drift_enabled = True
    tuned_kernel.runtime_policy_mission_drift_weight = 0.32
    tuned_kernel.runtime_policy_mission_drift_relief_weight = 0.18
    tuned_kernel.runtime_policy_mission_drift_severe_threshold = 0.66
    tuned_kernel.runtime_policy_provider_policy_relief_enabled = True
    tuned_kernel.runtime_policy_provider_policy_relief_gain = 0.16

    metadata = {
        "policy_profile": "automation_power",
        "recovery_profile": "aggressive",
        "verification_strictness": "off",
    }
    shared_context = {
        "execution_feedback": {
            "quality_score": 0.88,
            "confirm_failure_ratio": 0.05,
            "latest_failure_category": "transient",
            "desktop_change_rate": 0.9,
        },
        "external_provider_health": [
            {"provider": "google", "cooldown_active": True, "failure_ema": 0.72, "health_score": 0.28},
            {"provider": "graph", "cooldown_active": False, "failure_ema": 0.08, "health_score": 0.92},
        ],
        "open_action_circuits": [],
        "mission_feedback": {"risk_level": "low", "quality_level": "high"},
        "external_reliability_mission_analysis": {
            "drift_mode": "improving",
            "drift_score": 0.16,
            "provider_policy_changed": False,
            "provider_policy_updated_count": 0,
        },
    }

    without_tuning = base_kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata=dict(metadata),
        context=dict(shared_context),
        attempt=1,
    )

    tuned_context = dict(shared_context)
    tuned_context["external_reliability_mission_analysis"] = {
        "drift_mode": "improving",
        "drift_score": 0.16,
        "provider_policy_changed": True,
        "provider_policy_updated_count": 14,
    }
    with_tuning = tuned_kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata=dict(metadata),
        context=tuned_context,
        attempt=1,
    )

    assert without_tuning.get("policy_profile") == "automation_safe"
    assert "policy_profile" not in with_tuning


def test_derive_runtime_adaptive_overrides_sets_external_route_controls_for_severe_mode() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = False
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56
    kernel.runtime_external_route_adaptation_enabled = True
    kernel.runtime_external_route_entropy_force_enabled = True
    kernel.runtime_external_route_severe_profile = "defensive"
    kernel.runtime_external_route_moderate_profile = "cautious"
    kernel.runtime_external_route_stable_profile = "balanced"
    kernel.runtime_external_route_throughput_profile = "throughput"
    kernel.runtime_external_route_throughput_quality_floor = 0.88
    kernel.runtime_external_route_probe_severe = 0.96
    kernel.runtime_external_route_probe_moderate = 0.72
    kernel.runtime_external_route_probe_stable = 0.28
    kernel.runtime_external_remediation_budget_enabled = True
    kernel.runtime_external_remediation_actions_severe = 5
    kernel.runtime_external_remediation_actions_moderate = 3
    kernel.runtime_external_remediation_actions_stable = 2
    kernel.runtime_external_remediation_total_severe = 12
    kernel.runtime_external_remediation_total_moderate = 8
    kernel.runtime_external_remediation_total_stable = 6
    kernel.runtime_external_contract_risk_floor = 0.14

    updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata={
            "__jarvis_goal_id": "goal-route-severe",
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "off",
            "external_route_profile": "throughput",
        },
        context={
            "execution_feedback": {
                "quality_score": 0.31,
                "confirm_failure_ratio": 0.62,
                "latest_failure_category": "unknown",
                "desktop_change_rate": 0.14,
            },
            "external_provider_health": [
                {"provider": "google", "cooldown_active": True, "failure_ema": 0.82, "health_score": 0.24},
                {"provider": "graph", "cooldown_active": False, "failure_ema": 0.72, "health_score": 0.34},
            ],
            "open_action_circuits": [{"action": "external_doc_update"}],
            "mission_feedback": {"risk_level": "high", "quality_level": "low"},
            "mission_trend_feedback": {"mode": "worsening", "trend_pressure": 0.72, "risk_trend": "worsening"},
        },
        attempt=1,
    )

    assert updates["external_route_profile"] == "defensive"
    assert updates["external_route_entropy_force"] == "false"
    assert updates["external_cooldown_override"] == "false"
    assert updates["external_outage_override"] == "false"
    assert int(updates["external_remediation_max_actions"]) >= 5
    assert int(updates["external_remediation_max_total_actions"]) >= 12
    assert float(updates["external_remediation_contract_risk_floor"]) >= 0.48


def test_derive_runtime_adaptive_overrides_promotes_throughput_route_in_healthy_mode() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = False
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56
    kernel.runtime_external_route_adaptation_enabled = True
    kernel.runtime_external_route_entropy_force_enabled = True
    kernel.runtime_external_route_severe_profile = "defensive"
    kernel.runtime_external_route_moderate_profile = "cautious"
    kernel.runtime_external_route_stable_profile = "balanced"
    kernel.runtime_external_route_throughput_profile = "throughput"
    kernel.runtime_external_route_throughput_quality_floor = 0.88
    kernel.runtime_external_route_probe_severe = 0.96
    kernel.runtime_external_route_probe_moderate = 0.72
    kernel.runtime_external_route_probe_stable = 0.28
    kernel.runtime_external_remediation_budget_enabled = True
    kernel.runtime_external_remediation_actions_severe = 5
    kernel.runtime_external_remediation_actions_moderate = 3
    kernel.runtime_external_remediation_actions_stable = 2
    kernel.runtime_external_remediation_total_severe = 12
    kernel.runtime_external_remediation_total_moderate = 8
    kernel.runtime_external_remediation_total_stable = 6
    kernel.runtime_external_contract_risk_floor = 0.14

    updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata={
            "__jarvis_goal_id": "goal-route-stable",
            "policy_profile": "automation_power",
            "recovery_profile": "balanced",
            "verification_strictness": "standard",
            "external_route_profile": "balanced",
        },
        context={
            "execution_feedback": {
                "quality_score": 0.97,
                "confirm_failure_ratio": 0.02,
                "latest_failure_category": "",
                "desktop_change_rate": 0.95,
            },
            "external_provider_health": [{"provider": "google", "cooldown_active": False, "failure_ema": 0.08, "health_score": 0.93}],
            "open_action_circuits": [],
            "mission_feedback": {"risk_level": "low", "quality_level": "high"},
            "mission_trend_feedback": {"mode": "improving", "trend_pressure": 0.08, "risk_trend": "improving"},
        },
        attempt=0,
    )

    assert updates["external_route_profile"] == "throughput"
    assert updates["external_route_entropy_force"] == "true"
    assert updates["external_cooldown_override"] == "true"
    assert updates["external_outage_override"] == "true"
    assert int(updates["external_remediation_max_actions"]) <= 2
    assert int(updates["external_remediation_max_total_actions"]) <= 6
    assert float(updates["external_remediation_contract_risk_floor"]) <= 0.2


def test_derive_replan_overrides_sets_external_route_defensive_for_severe_contract() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_contract_severe_threshold = 0.62

    updates = kernel._derive_replan_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "standard",
        },
        context={
            "last_failure_category": "transient",
            "last_failure_retry_count": 1,
            "last_failure_external_contract": {"code": "auth_preflight_failed", "severity": "error"},
            "last_failure_external_reliability": {
                "strategy": "fallback_ranked",
                "selected_provider": "google",
                "selected_health_score": 0.34,
                "selected_effective_score": 0.3,
                "blocked_provider_count": 1,
                "dropped_provider_count": 1,
            },
        },
        policy={
            "escalate_recovery_profile": True,
            "escalate_verification": True,
            "escalate_policy_profile": True,
        },
        replan_attempt=1,
    )

    assert updates["external_route_profile"] == "defensive"
    assert updates["external_route_entropy_force"] == "false"
    assert updates["external_cooldown_override"] == "false"
    assert updates["external_outage_override"] == "false"
    assert updates["external_remediation_max_actions"] == "5"
    assert updates["external_remediation_max_total_actions"] == "12"
    assert updates["external_remediation_contract_risk_floor"] == "0.520000"


def test_derive_replan_overrides_sets_execution_controls_for_manual_contracts() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_contract_severe_threshold = 0.62

    updates = kernel._derive_replan_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "standard",
        },
        context={
            "last_failure_category": "transient",
            "last_failure_retry_count": 1,
            "last_failure_external_contract": {
                "code": "provider_cooldown_blocked",
                "severity": "warning",
                "execution_contract": {
                    "mode": "manual",
                    "max_retry_attempts": 1,
                    "allow_provider_reroute": False,
                    "stop_conditions": ["manual_escalation", "provider_reroute_locked"],
                },
            },
            "last_failure_external_reliability": {
                "strategy": "fallback_ranked",
                "selected_provider": "google",
                "selected_health_score": 0.46,
                "selected_effective_score": 0.41,
                "blocked_provider_count": 1,
                "dropped_provider_count": 0,
            },
        },
        policy={
            "escalate_recovery_profile": True,
            "escalate_verification": True,
            "escalate_policy_profile": True,
        },
        replan_attempt=1,
    )

    assert updates["external_remediation_execution_mode"] == "assisted"
    assert updates["external_remediation_checkpoint_mode"] == "strict"
    assert updates["external_remediation_allow_provider_reroute"] == "false"


def test_repair_memory_hints_returns_matching_external_signals(tmp_path) -> None:
    from backend.python.core.runtime_memory import RuntimeMemory

    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_memory = RuntimeMemory(max_items=80, store_path=str(tmp_path / "runtime_memory.jsonl"))
    kernel.runtime_memory.remember_goal(
        text="read email message",
        status="completed",
        metadata={"source": "desktop-ui"},
        results=[
            ActionResult(
                action="external_email_read",
                status="success",
                output={"status": "success"},
                evidence={
                    "request": {"args": {"provider": "google", "message_id": "msg_222"}},
                    "external_reliability_preflight": {
                        "status": "ok",
                        "provider_routing": {"selected_provider": "google"},
                        "contract_diagnostic": {"code": "missing_required_fields"},
                    },
                },
            )
        ],
    )

    hints = kernel._repair_memory_hints(  # noqa: SLF001
        goal_text="read email",
        context={
            "last_failure_action": "external_email_read",
            "last_failure_external_contract": {"code": "missing_required_fields"},
        },
        limit=4,
    )

    assert hints
    signals = hints[0].get("signals", [])
    assert isinstance(signals, list)
    assert signals and signals[0].get("provider") == "google"


def test_derive_runtime_adaptive_overrides_hardens_on_worsening_mission_trend() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = False
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56
    kernel.runtime_policy_trend_weight = 0.35
    kernel.runtime_policy_trend_relief_weight = 0.24

    updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata={
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "off",
        },
        context={
            "execution_feedback": {
                "quality_score": 0.84,
                "confirm_failure_ratio": 0.08,
                "latest_failure_category": "transient",
                "desktop_change_rate": 0.9,
            },
            "external_provider_health": [],
            "open_action_circuits": [],
            "mission_feedback": {"risk_level": "low", "quality_level": "high"},
            "mission_trend_feedback": {
                "mode": "worsening",
                "trend_pressure": 0.72,
                "risk_trend": "worsening",
                "quality_trend": "degrading",
            },
        },
        attempt=1,
    )

    assert updates["policy_profile"] == "automation_safe"
    assert updates["recovery_profile"] == "safe"
    assert updates["verification_strictness"] in {"standard", "strict"}


def test_runtime_mission_trend_feedback_uses_cached_summary() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_trend_feedback_enabled = True
    kernel.runtime_policy_trend_refresh_s = 999
    kernel.runtime_policy_trend_limit = 120
    kernel._last_mission_trend_feedback_monotonic = 0.0
    kernel._last_mission_trend_feedback = {}
    call_count = {"count": 0}

    def fake_summary(self: AgentKernel, *, limit: int = 220) -> dict[str, object]:
        call_count["count"] += 1
        assert limit == 120
        return {
            "status": "success",
            "risk": {"level": "medium"},
            "quality": {"level": "medium"},
            "failed_ratio": 0.2,
            "blocked_ratio": 0.1,
            "recommendation": "balanced",
            "trend": {
                "mode": "worsening",
                "pressure": 0.44,
                "risk_trend": "worsening",
                "quality_trend": "degrading",
                "failed_trend": "worsening",
                "blocked_trend": "stable",
                "risk_delta": 0.11,
                "quality_delta": -0.08,
                "failed_ratio_delta": 0.06,
                "blocked_ratio_delta": 0.02,
            },
        }

    kernel._summarize_mission_trends = fake_summary.__get__(kernel, AgentKernel)  # type: ignore[method-assign]

    first = kernel._runtime_mission_trend_feedback()  # noqa: SLF001
    second = kernel._runtime_mission_trend_feedback()  # noqa: SLF001

    assert call_count["count"] == 1
    assert first["status"] == "success"
    assert second["status"] == "success"
    assert first["mode"] == "worsening"
    assert float(first["trend_pressure"]) == 0.44


def test_external_auth_runtime_state_prefers_best_oauth_token_row() -> None:
    class _OAuthStoreStub:
        def list(self, *, limit: int, include_secrets: bool) -> dict[str, object]:
            assert limit == 500
            assert include_secrets is False
            return {
                "status": "success",
                "items": [
                    {
                        "provider": "google",
                        "account_id": "default",
                        "has_access_token": False,
                        "has_refresh_token": True,
                        "expires_in_s": 120,
                        "scopes": ["gmail.readonly"],
                        "updated_at": "2026-03-01T00:00:00+00:00",
                    },
                    {
                        "provider": "google",
                        "account_id": "primary",
                        "has_access_token": True,
                        "has_refresh_token": True,
                        "expires_in_s": 2800,
                        "scopes": ["gmail.send", "gmail.readonly"],
                        "updated_at": "2026-03-02T00:00:00+00:00",
                    },
                ],
            }

    kernel = AgentKernel.__new__(AgentKernel)
    kernel.oauth_store = _OAuthStoreStub()

    payload = kernel._external_auth_runtime_state(providers=["google"])  # noqa: SLF001

    assert payload["source"] == "kernel_oauth_runtime"
    providers = payload.get("providers", {})
    assert isinstance(providers, dict)
    google = providers["google"]
    assert google["account_id"] == "primary"
    assert google["has_credentials"] is True
    assert google["has_refresh_token"] is True
    assert google["expires_in_s"] == 2800


def test_apply_guardrail_runtime_guidance_updates_metadata_and_step_settings() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    goal = GoalRecord(
        goal_id="goal-guardrail",
        request=GoalRequest(
            text="update external task",
            source="desktop-ui",
            metadata={
                "policy_profile": "automation_power",
                "recovery_profile": "aggressive",
                "verification_strictness": "off",
            },
        ),
    )
    plan = ExecutionPlan(
        plan_id="plan-guardrail",
        goal_id=goal.goal_id,
        intent="external_task_update",
        steps=[
            PlanStep(
                step_id="s1",
                action="external_task_update",
                args={"task_id": "t-1", "status": "done"},
                max_retries=4,
                timeout_s=20,
                verify={},
            )
        ],
    )
    context: dict[str, object] = {}

    applied = kernel._apply_guardrail_runtime_guidance(  # noqa: SLF001
        goal=goal,
        plan=plan,
        context=context,
        guidance={
            "status": "success",
            "recommended_level": "critical",
            "metadata_overrides": {
                "policy_profile": "automation_safe",
                "recovery_profile": "safe",
                "verification_strictness": "strict",
            },
            "action_overrides": {
                "external_task_update": {
                    "max_retries_cap": 1,
                    "timeout_factor": 1.35,
                    "retry_multiplier": 1.2,
                }
            },
            "triggered_actions": [{"action": "external_task_update", "severity": "critical"}],
        },
    )

    assert applied["recommended_level"] == "critical"
    assert applied["metadata_overrides"]["policy_profile"] == "automation_safe"
    assert applied["metadata_overrides"]["recovery_profile"] == "safe"
    assert applied["metadata_overrides"]["verification_strictness"] == "strict"
    assert plan.steps[0].max_retries == 1
    assert plan.steps[0].timeout_s > 20
    assert isinstance(plan.steps[0].verify.get("retry"), dict)
    assert context["guardrail_recommended_level"] == "critical"


def test_compute_replan_delay_uses_retry_history_floor() -> None:
    delay_s = AgentKernel._compute_replan_delay_s(  # noqa: SLF001
        policy={"delay_base_s": 0.2},
        failure_context={
            "last_failure_category": "rate_limited",
            "last_failure_recovery": {
                "retry_history": [
                    {"attempt": 1, "delay_s": 1.5, "category": "rate_limited"},
                    {"attempt": 2, "delay_s": 3.0, "category": "rate_limited"},
                ]
            },
        },
        next_attempt=2,
    )

    assert delay_s >= 1.5


def test_list_goals_supports_status_filter_and_limit() -> None:
    kernel = AgentKernel()
    first_goal_id = asyncio.run(kernel.submit_goal("what time is it in UTC", source="desktop-ui"))
    second_goal_id = asyncio.run(kernel.submit_goal("system snapshot", source="desktop-ui"))

    cancelled = kernel.cancel_goal(second_goal_id, reason="cancelled for test")
    assert cancelled["status"] == "success"

    cancelled_rows = kernel.list_goals(status="cancelled", limit=10)
    assert cancelled_rows["count"] >= 1
    assert all(str(item.get("status", "")).lower() == "cancelled" for item in cancelled_rows["items"])

    limited = kernel.list_goals(limit=1)
    assert limited["count"] == 1
    assert len(limited["items"]) == 1
    goal_ids = {str(item.get("goal_id", "")) for item in kernel.list_goals(limit=10)["items"]}
    assert first_goal_id in goal_ids or second_goal_id in goal_ids


def test_cancel_mission_cancels_running_mission() -> None:
    kernel = AgentKernel()
    goal_id = asyncio.run(kernel.submit_goal("long mission", source="desktop-ui"))
    mission_id = kernel.mission_control.mission_for_goal(goal_id)
    assert mission_id

    payload = kernel.cancel_mission(mission_id, reason="cancel mission for test")

    assert payload["status"] == "success"
    assert payload["mission_id"] == mission_id
    mission = kernel.get_mission(mission_id)
    assert mission is not None
    assert str(mission.get("status", "")).lower() == "cancelled"


def test_cancel_mission_rejects_completed_mission() -> None:
    kernel = AgentKernel()
    goal_id = asyncio.run(kernel.submit_goal("complete mission", source="desktop-ui"))
    mission_id = kernel.mission_control.mission_for_goal(goal_id)
    assert mission_id
    kernel.mission_control.mark_finished(mission_id, status="completed")

    payload = kernel.cancel_mission(mission_id, reason="cancel completed")

    assert payload["status"] == "error"
    assert "already completed" in str(payload.get("message", "")).lower()


def test_auto_rollback_policy_prefers_recovery_for_transient_failure() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.auto_rollback_enabled = True
    kernel.auto_rollback_allow_blocked = False
    kernel.auto_rollback_default_policy = "on_failure"

    should_run, reason = kernel._evaluate_auto_rollback_policy(  # noqa: SLF001
        policy="on_failure",
        failed_status="failed",
        failure_category="timeout",
        failed_error="request timed out",
    )
    assert should_run is False
    assert "prefer mission recovery" in reason


def test_auto_rollback_policy_runs_for_non_retryable_failure() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.auto_rollback_enabled = True
    kernel.auto_rollback_allow_blocked = False
    kernel.auto_rollback_default_policy = "on_failure"

    should_run, reason = kernel._evaluate_auto_rollback_policy(  # noqa: SLF001
        policy="on_failure",
        failed_status="failed",
        failure_category="non_retryable",
        failed_error="invalid argument",
    )
    assert should_run is True
    assert reason == "eligible"


def test_auto_mission_resume_policy_eligible_for_transient_failures() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.auto_mission_recovery_enabled = True
    kernel.auto_mission_recovery_allow_blocked = False
    kernel.auto_mission_recovery_allow_unknown = False
    kernel.auto_mission_recovery_profile_escalate = True
    kernel.auto_mission_recovery_max_resumes = 3
    kernel.auto_mission_recovery_base_delay_s = 10
    kernel.auto_mission_recovery_max_delay_s = 120

    mission = {"resume_count": 1, "metadata": {}}
    eligible, reason, delay_s = kernel._evaluate_auto_mission_resume_policy(  # noqa: SLF001
        mission=mission,
        goal_metadata={},
        failure_status="failed",
        failure_category="transient",
    )
    assert eligible is True
    assert reason == "eligible"
    assert delay_s == 20


def test_apply_plan_runtime_hints_uses_runtime_constraints_fallback() -> None:
    hints = AgentKernel._apply_plan_runtime_hints(  # noqa: SLF001
        runtime_budget_s=180,
        step_budget=20,
        plan_context={
            "runtime_constraints": {
                "time_budget_s": 35,
                "max_steps_hint": 3,
                "verification_strictness": "strict",
            }
        },
    )

    assert hints["max_runtime_s"] == 35
    assert hints["max_steps"] == 3
    assert hints["verification_strictness"] == "strict"


def test_auto_mission_resume_policy_allows_profile_escalation_for_policy_denial() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.auto_mission_recovery_enabled = True
    kernel.auto_mission_recovery_allow_blocked = False
    kernel.auto_mission_recovery_allow_unknown = False
    kernel.auto_mission_recovery_profile_escalate = True
    kernel.auto_mission_recovery_max_resumes = 3
    kernel.auto_mission_recovery_base_delay_s = 10
    kernel.auto_mission_recovery_max_delay_s = 120

    mission = {"resume_count": 0, "metadata": {}}
    eligible, reason, delay_s = kernel._evaluate_auto_mission_resume_policy(  # noqa: SLF001
        mission=mission,
        goal_metadata={},
        failure_status="failed",
        failure_category="non_retryable",
        failure_reason="Action denied for policy profile automation_safe",
    )

    assert eligible is True
    assert reason == "eligible"
    assert delay_s == 10


def test_resolve_auto_recovery_profile_escalates_to_power_profile() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.auto_mission_recovery_profile_escalate = True

    profile, escalated = kernel._resolve_auto_recovery_profile(  # noqa: SLF001
        current_profile="automation_safe",
        metadata={},
        failure_category="non_retryable",
        failure_reason="not allowed for policy profile automation_safe",
    )

    assert profile == "automation_power"
    assert escalated is True


def test_runtime_policy_hysteresis_lingers_after_severe_signals() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = False
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56
    kernel.runtime_policy_trend_weight = 0.35
    kernel.runtime_policy_trend_relief_weight = 0.24
    kernel.runtime_policy_signal_smoothing_enabled = True
    kernel.runtime_policy_signal_ema_alpha = 0.35
    kernel.runtime_policy_signal_stale_reset_s = 1200
    kernel.runtime_policy_signal_state_max_scopes = 32
    kernel.runtime_policy_hysteresis_external_margin = 0.06
    kernel.runtime_policy_hysteresis_quality_margin = 0.05
    kernel.runtime_policy_hysteresis_confirm_margin = 0.06
    kernel.runtime_policy_hysteresis_trend_margin = 0.07
    kernel._runtime_policy_signal_state = {}

    metadata: dict[str, object] = {
        "__jarvis_goal_id": "goal-hys",
        "policy_profile": "automation_power",
        "recovery_profile": "aggressive",
        "verification_strictness": "off",
    }
    severe_context = {
        "execution_feedback": {
            "quality_score": 0.36,
            "confirm_failure_ratio": 0.58,
            "latest_failure_category": "unknown",
            "desktop_change_rate": 0.1,
        },
        "external_provider_health": [
            {"provider": "google", "cooldown_active": True, "failure_ema": 0.8, "health_score": 0.2},
            {"provider": "graph", "cooldown_active": False, "failure_ema": 0.73, "health_score": 0.39},
        ],
        "open_action_circuits": [{"action": "external_task_update"}],
        "mission_feedback": {"risk_level": "high", "quality_level": "low"},
        "mission_trend_feedback": {"mode": "worsening", "trend_pressure": 0.7, "risk_trend": "worsening"},
    }
    first_updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata=metadata,
        context=severe_context,
        attempt=1,
    )
    metadata.update(first_updates)

    near_relief_context = {
        "execution_feedback": {
            "quality_score": 0.74,
            "confirm_failure_ratio": 0.22,
            "latest_failure_category": "",
            "desktop_change_rate": 0.52,
        },
        "external_provider_health": [{"provider": "google", "cooldown_active": False, "failure_ema": 0.29, "health_score": 0.71}],
        "open_action_circuits": [],
        "mission_feedback": {"risk_level": "medium", "quality_level": "medium"},
        "mission_trend_feedback": {"mode": "stable", "trend_pressure": 0.19, "risk_trend": "stable"},
    }
    second_updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata=metadata,
        context=near_relief_context,
        attempt=0,
    )

    state = kernel._runtime_policy_signal_state.get("goal:goal-hys", {})
    assert isinstance(state, dict)
    assert state.get("mode") == "severe"
    assert str(metadata.get("recovery_profile", "")).lower() == "safe"


def test_runtime_policy_can_upgrade_after_sustained_healthy_window() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = True
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56
    kernel.runtime_policy_trend_weight = 0.35
    kernel.runtime_policy_trend_relief_weight = 0.24
    kernel.runtime_policy_signal_smoothing_enabled = True
    kernel.runtime_policy_signal_ema_alpha = 0.75
    kernel.runtime_policy_signal_stale_reset_s = 1200
    kernel.runtime_policy_signal_state_max_scopes = 32
    kernel.runtime_policy_hysteresis_external_margin = 0.05
    kernel.runtime_policy_hysteresis_quality_margin = 0.04
    kernel.runtime_policy_hysteresis_confirm_margin = 0.05
    kernel.runtime_policy_hysteresis_trend_margin = 0.06
    kernel._runtime_policy_signal_state = {}

    metadata: dict[str, object] = {
        "__jarvis_goal_id": "goal-upgrade",
        "policy_profile": "interactive",
        "recovery_profile": "safe",
        "verification_strictness": "strict",
        "runtime_policy_allow_upgrade": True,
    }
    seed_context = {
        "execution_feedback": {
            "quality_score": 0.32,
            "confirm_failure_ratio": 0.55,
            "latest_failure_category": "unknown",
            "desktop_change_rate": 0.08,
        },
        "external_provider_health": [{"provider": "google", "cooldown_active": True, "failure_ema": 0.77, "health_score": 0.31}],
        "open_action_circuits": [{"action": "external_doc_update"}],
        "mission_feedback": {"risk_level": "high", "quality_level": "low"},
        "mission_trend_feedback": {"mode": "worsening", "trend_pressure": 0.66, "quality_trend": "degrading"},
    }
    seed_updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata=metadata,
        context=seed_context,
        attempt=1,
    )
    metadata.update(seed_updates)

    healthy_context = {
        "execution_feedback": {
            "quality_score": 0.98,
            "confirm_failure_ratio": 0.01,
            "latest_failure_category": "",
            "desktop_change_rate": 0.98,
        },
        "external_provider_health": [],
        "open_action_circuits": [],
        "mission_feedback": {"risk_level": "low", "quality_level": "high"},
        "mission_trend_feedback": {"mode": "improving", "trend_pressure": 0.0, "risk_trend": "improving"},
    }

    upgraded = False
    for _ in range(8):
        updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
            metadata=metadata,
            context=healthy_context,
            attempt=0,
        )
        metadata.update(updates)
        if updates.get("policy_profile") == "automation_power":
            upgraded = True
            break

    state = kernel._runtime_policy_signal_state.get("goal:goal-upgrade", {})
    assert isinstance(state, dict)
    assert state.get("mode") in {"stable", "moderate"}
    assert upgraded is True


def test_runtime_policy_signal_state_prunes_old_scope_rows() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_signal_smoothing_enabled = True
    kernel.runtime_policy_signal_ema_alpha = 0.5
    kernel.runtime_policy_signal_stale_reset_s = 1200
    kernel.runtime_policy_signal_state_max_scopes = 2
    kernel._runtime_policy_signal_state = {}

    for goal_id in ("goal-a", "goal-b", "goal-c"):
        kernel._runtime_policy_smooth_signals(  # noqa: SLF001
            metadata={"__jarvis_goal_id": goal_id},
            quality_score=0.9,
            confirm_failure_ratio=0.1,
            external_pressure=0.1,
            desktop_change_rate=0.9,
            trend_pressure=0.1,
        )

    state = kernel._runtime_policy_signal_state
    assert isinstance(state, dict)
    assert len(state) == 2
    assert "goal:goal-a" not in state


def test_derive_runtime_adaptive_overrides_uses_verification_and_telemetry_pressure() -> None:
    kernel = AgentKernel.__new__(AgentKernel)
    kernel.runtime_policy_adaptation_enabled = True
    kernel.runtime_policy_auto_upgrade = False
    kernel.runtime_policy_signal_smoothing_enabled = False
    kernel.runtime_policy_external_pressure_threshold = 0.48
    kernel.runtime_policy_quality_floor = 0.56
    kernel.runtime_policy_contract_guardrail_enabled = True
    kernel.runtime_policy_contract_pressure_threshold = 0.38
    kernel.runtime_policy_contract_severe_threshold = 0.62
    kernel.runtime_policy_verification_pressure_enabled = True
    kernel.runtime_policy_verification_pressure_threshold = 0.36
    kernel.runtime_policy_verification_pressure_severe_threshold = 0.62
    kernel.runtime_policy_telemetry_feedback_enabled = True
    kernel._runtime_policy_signal_state = {}

    updates = kernel._derive_runtime_adaptive_overrides(  # noqa: SLF001
        metadata={
            "__jarvis_goal_id": "goal-vp-1",
            "policy_profile": "automation_power",
            "recovery_profile": "aggressive",
            "verification_strictness": "off",
        },
        context={
            "execution_feedback": {
                "quality_score": 0.72,
                "confirm_failure_ratio": 0.18,
                "latest_failure_category": "transient",
                "desktop_change_rate": 0.84,
                "verification_failure_ratio": 0.74,
                "verification_pressure": 0.81,
                "verification_pressure_mode": "severe",
            },
            "telemetry_feedback": {
                "status": "success",
                "mode": "severe",
                "pressure": 0.78,
                "failure_ratio": 0.64,
                "event_rate_pressure": 0.52,
            },
            "external_provider_health": [],
            "open_action_circuits": [],
            "mission_feedback": {"risk_level": "medium", "quality_level": "medium"},
            "mission_trend_feedback": {"mode": "stable", "trend_pressure": 0.12},
        },
        attempt=1,
    )

    assert str(updates.get("runtime_verification_mode", "")) == "severe"
    assert float(updates.get("runtime_verification_pressure", 0.0) or 0.0) >= 0.62
    assert updates.get("verification_strictness") in {"standard", "strict"}
    assert updates.get("policy_profile") == "automation_safe"


def test_runtime_policy_telemetry_feedback_computes_pressure_mode() -> None:
    class _TelemetryStub:
        def summary(self, *, limit: int = 0) -> dict[str, object]:
            assert limit == 600
            return {
                "status": "success",
                "count": 140,
                "failure_ratio": 0.58,
                "events_per_s": 4.2,
            }

    kernel = AgentKernel.__new__(AgentKernel)
    kernel.telemetry = _TelemetryStub()
    kernel.runtime_policy_telemetry_feedback_enabled = True
    kernel.runtime_policy_telemetry_feedback_limit = 600
    kernel.runtime_policy_telemetry_feedback_min_events = 20
    kernel.runtime_policy_telemetry_feedback_decay = 0.7
    kernel.runtime_policy_telemetry_feedback_event_rate_scale = 7.0
    kernel.runtime_policy_telemetry_feedback_failure_weight = 0.74
    kernel._last_runtime_policy_telemetry_feedback = {"status": "success", "pressure": 0.72}

    feedback = kernel._runtime_policy_telemetry_feedback(force=True)  # noqa: SLF001

    assert feedback["status"] == "success"
    assert float(feedback.get("pressure", 0.0) or 0.0) > 0.0
    assert str(feedback.get("mode", "")) in {"moderate", "severe"}
