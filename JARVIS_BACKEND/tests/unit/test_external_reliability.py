from __future__ import annotations

import json

from backend.python.core.external_reliability import ExternalReliabilityOrchestrator


def test_preflight_contract_rejects_invalid_update_payload(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    payload = orchestrator.preflight(
        action="external_doc_update",
        args={"document_id": "doc-1"},
        metadata={},
    )

    assert payload["status"] == "error"
    assert "title, content" in str(payload.get("message", "")).lower()


def test_preflight_blocks_provider_during_cooldown(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    for _ in range(3):
        orchestrator.record_outcome(
            action="external_email_list",
            args={"provider": "graph"},
            status="failed",
            error="request timed out",
            output={"status": "error"},
            metadata={},
        )

    payload = orchestrator.preflight(
        action="external_email_list",
        args={"provider": "graph"},
        metadata={},
    )
    assert payload["status"] == "blocked"
    assert float(payload.get("retry_after_s", 0.0) or 0.0) > 0


def test_preflight_cooldown_block_includes_runtime_remediation_contract(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    for _ in range(4):
        orchestrator.record_outcome(
            action="external_doc_update",
            args={"provider": "google", "document_id": "doc-1", "content": "x"},
            status="failed",
            error="request timed out",
            output={"status": "error"},
            metadata={},
        )

    payload = orchestrator.preflight(
        action="external_doc_update",
        args={"provider": "google", "document_id": "doc-1", "content": "x"},
        metadata={},
    )

    assert payload["status"] == "blocked"
    contract_diagnostic = payload.get("contract_diagnostic", {})
    assert isinstance(contract_diagnostic, dict)
    assert str(contract_diagnostic.get("code", "")) == "provider_cooldown_blocked"
    assert str(contract_diagnostic.get("contract_stage", "")) == "runtime_reliability"
    assert isinstance(contract_diagnostic.get("checks", []), list)
    assert isinstance(contract_diagnostic.get("remediation_plan", []), list)
    remediation_contract = payload.get("remediation_contract", {})
    assert isinstance(remediation_contract, dict)
    strategies = remediation_contract.get("strategies", [])
    assert isinstance(strategies, list)
    assert any(
        isinstance(strategy, dict)
        and str(strategy.get("type", "")).strip().lower() == "tool_action"
        for strategy in strategies
    )
    hints = payload.get("remediation_hints", [])
    assert isinstance(hints, list)
    assert any(
        isinstance(row, dict)
        and isinstance(row.get("tool_action"), dict)
        and str(row.get("tool_action", {}).get("action", "")).strip().lower() in {"external_connector_status", "oauth_token_maintain"}
        for row in hints
    )


def test_preflight_outage_block_includes_runtime_contract_diagnostics(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))
    orchestrator.outage_preflight_block_enabled = True  # noqa: SLF001
    orchestrator.outage_preflight_block_threshold = 0.5  # noqa: SLF001
    orchestrator._provider_states["google"] = {  # noqa: SLF001
        "provider": "google",
        "samples": 12,
        "successes": 2,
        "failures": 10,
        "consecutive_failures": 5,
        "failure_ema": 0.84,
        "failure_trend_ema": 0.21,
        "availability_ema": 0.24,
        "latency_ema_ms": 980.0,
        "outage_ema": 0.93,
        "outage_streak": 6,
        "outage_active": True,
        "outage_since_at": "",
        "outage_policy_bias": 0.32,
        "cooldown_bias": 1.0,
        "operation_cooldown_bias": {"read": 1.0},
        "cooldown_until": "",
        "updated_at": "",
    }
    orchestrator._provider_states["graph"] = {  # noqa: SLF001
        "provider": "graph",
        "samples": 9,
        "successes": 1,
        "failures": 8,
        "consecutive_failures": 4,
        "failure_ema": 0.81,
        "failure_trend_ema": 0.19,
        "availability_ema": 0.28,
        "latency_ema_ms": 1020.0,
        "outage_ema": 0.91,
        "outage_streak": 5,
        "outage_active": True,
        "outage_since_at": "",
        "outage_policy_bias": 0.29,
        "cooldown_bias": 1.0,
        "operation_cooldown_bias": {"read": 1.0},
        "cooldown_until": "",
        "updated_at": "",
    }

    payload = orchestrator.preflight(
        action="external_doc_read",
        args={"provider": "auto", "document_id": "doc-1"},
        metadata={},
    )

    assert payload["status"] == "blocked"
    contract_diagnostic = payload.get("contract_diagnostic", {})
    assert isinstance(contract_diagnostic, dict)
    assert str(contract_diagnostic.get("code", "")) == "provider_outage_blocked"
    assert str(contract_diagnostic.get("contract_stage", "")) == "runtime_reliability"
    diagnostics = contract_diagnostic.get("diagnostics", {})
    assert isinstance(diagnostics, dict)
    assert float(diagnostics.get("blocked_ratio", 0.0) or 0.0) >= 1.0
    checks = contract_diagnostic.get("checks", [])
    assert isinstance(checks, list)
    assert any(
        isinstance(row, dict)
        and str(row.get("check", "")).strip().lower() == "provider_outage"
        and str(row.get("status", "")).strip().lower() in {"failed", "warning"}
        for row in checks
    )
    remediation_plan = contract_diagnostic.get("remediation_plan", [])
    assert isinstance(remediation_plan, list)
    assert any(
        isinstance(row, dict)
        and str(row.get("phase", "")).strip().lower() == "retry"
        for row in remediation_plan
    )


def test_preflight_runtime_hints_include_provider_playbook_metadata(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    for _ in range(4):
        orchestrator.record_outcome(
            action="external_doc_update",
            args={"provider": "google", "document_id": "doc-1", "content": "x"},
            status="failed",
            error="request timed out",
            output={"status": "error"},
            metadata={},
        )

    payload = orchestrator.preflight(
        action="external_doc_update",
        args={"provider": "google", "document_id": "doc-1", "content": "x"},
        metadata={},
    )

    assert payload["status"] == "blocked"
    hints = payload.get("remediation_hints", [])
    assert isinstance(hints, list)
    assert any(
        isinstance(row, dict)
        and str(row.get("id", "")).strip().lower().endswith("_playbook_maintain")
        and str(row.get("provider", "")).strip().lower() == "google"
        and str(row.get("operation_class", "")).strip().lower() == "mutate"
        for row in hints
    )
    assert any(
        isinstance(row, dict)
        and isinstance(row.get("remediation"), dict)
        and str(row.get("remediation", {}).get("type", "")).strip().lower() == "provider_playbook_maintenance"
        for row in hints
    )


def test_preflight_runtime_block_exposes_orchestration_and_staggered_retry_hints(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    for provider in ("google", "graph"):
        for _ in range(4):
            orchestrator.record_outcome(
                action="external_doc_update",
                args={"provider": provider, "document_id": "doc-1", "content": "x"},
                status="failed",
                error="request timed out",
                output={"status": "error"},
                metadata={},
            )

    payload = orchestrator.preflight(
        action="external_doc_update",
        args={"provider": "auto", "document_id": "doc-1", "content": "x"},
        metadata={},
    )

    assert payload["status"] == "blocked"
    diag = payload.get("contract_diagnostic", {})
    assert isinstance(diag, dict)
    assert str(diag.get("contract_stage", "")) == "runtime_reliability"
    orchestration = diag.get("provider_orchestration", [])
    assert isinstance(orchestration, list)
    assert orchestration
    assert str(diag.get("primary_provider", "")).strip()
    remediation_hints = payload.get("remediation_hints", [])
    assert isinstance(remediation_hints, list)
    hint_ids = {
        str(row.get("id", "")).strip().lower()
        for row in remediation_hints
        if isinstance(row, dict)
    }
    assert "connector_contract_preflight" in hint_ids
    assert "staggered_provider_retry" in hint_ids
    remediation_contract = payload.get("remediation_contract", {})
    assert isinstance(remediation_contract, dict)
    assert "automation_ready" in remediation_contract
    assert "confidence_avg" in remediation_contract


def test_preflight_runtime_block_includes_severity_score_and_execution_contract(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    for provider in ("google", "graph"):
        for _ in range(4):
            orchestrator.record_outcome(
                action="external_doc_update",
                args={"provider": provider, "document_id": "doc-1", "content": "x"},
                status="failed",
                error="request timed out",
                output={"status": "error"},
                metadata={},
            )

    payload = orchestrator.preflight(
        action="external_doc_update",
        args={"provider": "auto", "document_id": "doc-1", "content": "x"},
        metadata={},
    )

    assert payload["status"] == "blocked"
    diag = payload.get("contract_diagnostic", {})
    assert isinstance(diag, dict)
    assert 0.0 <= float(diag.get("severity_score", 0.0) or 0.0) <= 1.0
    assert str(diag.get("blocking_class", "")).strip()
    assert int(diag.get("estimated_recovery_s", 0) or 0) > 0
    remediation_contract = payload.get("remediation_contract", {})
    assert isinstance(remediation_contract, dict)
    assert str(remediation_contract.get("automation_tier", "")).strip() in {"manual", "assisted", "automated"}
    assert int(remediation_contract.get("estimated_recovery_s", 0) or 0) > 0
    execution_contract = remediation_contract.get("execution_contract", {})
    assert isinstance(execution_contract, dict)
    assert str(execution_contract.get("mode", "")).strip() in {"manual", "assisted", "automated"}
    phases = execution_contract.get("phases", [])
    assert isinstance(phases, list)
    assert any(
        isinstance(row, dict) and str(row.get("phase", "")).strip().lower() == "retry_with_verification"
        for row in phases
    )


def test_preflight_runtime_block_autotunes_remediation_execution_contract(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))
    orchestrator._mission_outage_policy = {  # noqa: SLF001
        "mode": "severe",
        "profile": "defensive",
        "bias": 0.42,
        "pressure_ema": 0.81,
        "stability_ema": 0.2,
        "updated_at": "",
    }

    for _ in range(4):
        orchestrator.record_outcome(
            action="external_doc_update",
            args={"provider": "google", "document_id": "doc-1", "content": "x"},
            status="failed",
            error="request timed out",
            output={"status": "error"},
            metadata={},
        )

    payload = orchestrator.preflight(
        action="external_doc_update",
        args={"provider": "google", "document_id": "doc-1", "content": "x"},
        metadata={"external_route_profile": "defensive"},
    )

    assert payload["status"] == "blocked"
    remediation_contract = payload.get("remediation_contract", {})
    assert isinstance(remediation_contract, dict)
    autotune = remediation_contract.get("autotune", {})
    assert isinstance(autotune, dict)
    assert str(autotune.get("mission_mode", "")).strip().lower() == "severe"
    execution_contract = remediation_contract.get("execution_contract", {})
    assert isinstance(execution_contract, dict)
    assert str(execution_contract.get("mode", "")).strip().lower() in {"manual", "assisted"}
    assert int(execution_contract.get("max_retry_attempts", 0) or 0) == 1
    verification = execution_contract.get("verification", {})
    assert isinstance(verification, dict)
    assert str(verification.get("checkpoint_mode", "")).strip().lower() == "strict"
    assert bool(verification.get("allow_provider_reroute", True)) is False
    stop_conditions = execution_contract.get("stop_conditions", [])
    assert isinstance(stop_conditions, list)
    assert "provider_reroute_locked" in [str(item) for item in stop_conditions]


def test_retry_hint_is_generated_for_degraded_provider(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    for _ in range(8):
        orchestrator.record_outcome(
            action="external_task_list",
            args={"provider": "google"},
            status="failed",
            error="429 rate limit",
            output={"status": "error"},
            metadata={},
        )

    hint = orchestrator.retry_hint(
        action="external_task_list",
        args={"provider": "google"},
        metadata={},
    )

    assert hint["status"] == "success"
    retry = hint.get("retry_hint", {})
    assert isinstance(retry, dict)
    assert float(retry.get("base_delay_s", 0.0) or 0.0) > 0.5


def test_retry_hint_exposes_structured_retry_contract(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    for _ in range(8):
        orchestrator.record_outcome(
            action="external_task_update",
            args={"provider": "google", "task_id": "task-1", "status": "completed"},
            status="failed",
            error="429 rate limit",
            output={"status": "error"},
            metadata={},
        )

    hint = orchestrator.retry_hint(
        action="external_task_update",
        args={"provider": "google", "task_id": "task-1"},
        metadata={},
    )

    assert hint["status"] == "success"
    retry_contract = hint.get("retry_contract", {})
    assert isinstance(retry_contract, dict)
    assert str(retry_contract.get("contract_id", "")).strip().startswith("diag_")
    assert str(retry_contract.get("mode", "")).strip() in {
        "adaptive_backoff",
        "probe_then_backoff",
        "stabilize",
        "light_retry",
        "abort",
    }
    timing = retry_contract.get("timing", {})
    assert isinstance(timing, dict)
    assert float(timing.get("base_delay_s", 0.0) or 0.0) > 0.0
    budget = retry_contract.get("budget", {})
    assert isinstance(budget, dict)
    assert 1 <= int(budget.get("max_attempts", 0) or 0) <= 8


def test_preflight_ok_includes_retry_contract_when_hint_available(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    for _ in range(8):
        orchestrator.record_outcome(
            action="external_task_list",
            args={"provider": "google"},
            status="failed",
            error="429 rate limit",
            output={"status": "error"},
            metadata={},
        )

    payload = orchestrator.preflight(
        action="external_task_list",
        args={"provider": "auto"},
        metadata={},
    )

    assert payload["status"] == "ok"
    retry_contract = payload.get("retry_contract", {})
    assert isinstance(retry_contract, dict)
    assert str(retry_contract.get("contract_id", "")).strip().startswith("diag_")
    assert str(retry_contract.get("operation_class", "")).strip().lower() == "read"


def test_preflight_routes_auto_provider_to_healthiest_candidate(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    for _ in range(8):
        orchestrator.record_outcome(
            action="external_task_list",
            args={"provider": "graph"},
            status="failed",
            error="429 rate limit",
            output={"status": "error"},
            metadata={},
        )
    for _ in range(4):
        orchestrator.record_outcome(
            action="external_task_list",
            args={"provider": "google"},
            status="success",
            error="",
            output={"status": "success", "provider": "google"},
            metadata={},
        )

    payload = orchestrator.preflight(
        action="external_task_list",
        args={"provider": "auto"},
        metadata={},
    )

    assert payload["status"] == "ok"
    routing = payload.get("provider_routing", {})
    assert isinstance(routing, dict)
    assert str(routing.get("selected_provider", "")) == "google"
    args_patch = payload.get("args_patch", {})
    assert isinstance(args_patch, dict)
    assert str(args_patch.get("provider", "")) == "google"


def test_snapshot_includes_provider_health_and_action_risk_rows(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))
    for _ in range(3):
        orchestrator.record_outcome(
            action="external_doc_update",
            args={"provider": "google"},
            status="success",
            error="",
            output={"status": "success"},
            metadata={},
        )
    for _ in range(6):
        orchestrator.record_outcome(
            action="external_task_update",
            args={"provider": "google"},
            status="failed",
            error="request timed out",
            output={"status": "error"},
            metadata={},
        )

    snapshot = orchestrator.snapshot(provider="google", limit=10)

    assert snapshot["status"] == "success"
    assert snapshot["count"] >= 1
    top = snapshot["items"][0]
    assert "health_score" in top
    assert isinstance(top.get("top_action_risks", []), list)


def test_preflight_rejects_provider_not_supported_for_action(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    payload = orchestrator.preflight(
        action="external_doc_update",
        args={"provider": "smtp", "document_id": "doc-1", "content": "patch"},
        metadata={},
    )

    assert payload["status"] == "error"
    assert payload.get("failure_category") == "non_retryable"
    assert "provider must be one of" in str(payload.get("message", "")).lower()


def test_preflight_contract_filters_auto_provider_candidates_for_capability(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    payload = orchestrator.preflight(
        action="external_email_list",
        args={"provider": "auto"},
        metadata={},
    )

    assert payload["status"] == "ok"
    providers = payload.get("provider_candidates", [])
    assert isinstance(providers, list)
    assert "smtp" not in providers
    negotiation = payload.get("contract_negotiation", {})
    assert isinstance(negotiation, dict)
    dropped = negotiation.get("dropped_providers", [])
    assert isinstance(dropped, list)
    assert not dropped


def test_snapshot_and_retry_hint_include_operation_class_signals(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))
    for _ in range(6):
        orchestrator.record_outcome(
            action="external_task_update",
            args={"provider": "google", "task_id": "task-1", "title": "updated"},
            status="failed",
            error="request timed out",
            output={"status": "error"},
            metadata={},
        )

    hint = orchestrator.retry_hint(
        action="external_task_update",
        args={"provider": "google", "task_id": "task-1"},
        metadata={},
    )
    assert hint["status"] == "success"
    assert str(hint.get("operation_class", "")) == "mutate"
    assert float(hint.get("health_pressure", 0.0) or 0.0) >= 0.0

    snapshot = orchestrator.snapshot(provider="google", limit=10)
    top = snapshot["items"][0]
    top_operations = top.get("top_operation_risks", [])
    assert isinstance(top_operations, list)
    assert any(str(item.get("operation", "")) == "mutate" for item in top_operations if isinstance(item, dict))


def test_preflight_schema_rejects_invalid_field_types(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    payload = orchestrator.preflight(
        action="external_email_list",
        args={"provider": "auto", "max_results": "many"},
        metadata={},
    )

    assert payload["status"] == "error"
    assert "max_results" in str(payload.get("message", "")).lower()


def test_preflight_semantic_rejects_invalid_calendar_window(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    payload = orchestrator.preflight(
        action="external_calendar_create_event",
        args={
            "provider": "auto",
            "title": "bad window",
            "start": "2026-03-01T18:00:00+00:00",
            "end": "2026-03-01T17:00:00+00:00",
        },
        metadata={},
    )

    assert payload["status"] == "error"
    assert "end to be after start" in str(payload.get("message", "")).lower()


def test_cooldown_seconds_increase_when_failure_trend_worsens(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    rising = orchestrator._cooldown_seconds(  # noqa: SLF001
        provider="google",
        category="timeout",
        operation_class="mutate",
        consecutive_failures=4,
        failure_ema=0.72,
        trend_ema=0.42,
        operation_trend_ema=0.35,
    )
    recovering = orchestrator._cooldown_seconds(  # noqa: SLF001
        provider="google",
        category="timeout",
        operation_class="mutate",
        consecutive_failures=4,
        failure_ema=0.72,
        trend_ema=-0.35,
        operation_trend_ema=-0.25,
    )
    assert rising > recovering


def test_load_preserves_provider_sla_fields_and_latency_stats(tmp_path) -> None:
    store = tmp_path / "external_reliability.json"
    store.write_text(
        json.dumps(
            {
                "updated_at": "2026-03-01T00:00:00+00:00",
                "items": [
                    {
                        "provider": "google",
                        "samples": 12,
                        "successes": 8,
                        "failures": 4,
                        "consecutive_failures": 1,
                        "failure_ema": 0.33,
                        "failure_trend_ema": 0.12,
                        "availability_ema": 0.81,
                        "latency_ema_ms": 1735.25,
                        "last_status": "success",
                        "last_error": "",
                        "last_category": "timeout",
                        "last_action": "external_doc_update",
                        "cooldown_until": "",
                        "last_cooldown_s": 0,
                        "last_success_at": "2026-03-01T00:00:00+00:00",
                        "last_failure_at": "2026-02-28T00:00:00+00:00",
                        "category_counts": {"timeout": 4},
                        "action_stats": {
                            "external_doc_update": {
                                "samples": 9,
                                "successes": 6,
                                "failures": 3,
                                "consecutive_failures": 1,
                                "failure_ema": 0.36,
                                "failure_trend_ema": 0.14,
                                "latency_ema_ms": 1942.5,
                                "last_status": "success",
                                "last_category": "timeout",
                                "updated_at": "2026-03-01T00:00:00+00:00",
                            }
                        },
                        "operation_stats": {
                            "mutate": {
                                "samples": 9,
                                "successes": 6,
                                "failures": 3,
                                "consecutive_failures": 1,
                                "failure_ema": 0.38,
                                "failure_trend_ema": 0.16,
                                "latency_ema_ms": 2210.0,
                                "last_status": "success",
                                "last_category": "timeout",
                                "updated_at": "2026-03-01T00:00:00+00:00",
                            }
                        },
                        "updated_at": "2026-03-01T00:00:00+00:00",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(store))

    snapshot = orchestrator.snapshot(provider="google", limit=5)

    assert snapshot["status"] == "success"
    top = snapshot["items"][0]
    assert float(top.get("availability_ema", 0.0) or 0.0) >= 0.8
    assert float(top.get("latency_ema_ms", 0.0) or 0.0) >= 1700.0
    top_action = top.get("top_action_risks", [])[0]
    top_operation = top.get("top_operation_risks", [])[0]
    assert float(top_action.get("latency_ema_ms", 0.0) or 0.0) >= 1900.0
    assert float(top_operation.get("latency_ema_ms", 0.0) or 0.0) >= 2200.0


def test_auth_preflight_prefers_ready_provider_and_exposes_auth_ranking(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))
    metadata = {
        "external_auth_state": {
            "providers": {
                "google": {
                    "has_credentials": False,
                    "expires_in_s": 0,
                    "has_refresh_token": False,
                    "scopes": [],
                },
                "graph": {
                    "has_credentials": True,
                    "expires_in_s": 1600,
                    "has_refresh_token": True,
                    "scopes": ["mail.send", "mail.read"],
                },
            }
        }
    }

    payload = orchestrator.preflight(
        action="external_email_send",
        args={
            "provider": "auto",
            "to": ["person@example.com"],
            "subject": "hello",
            "body": "test",
        },
        metadata=metadata,
    )

    assert payload["status"] == "ok"
    providers = payload.get("provider_candidates", [])
    assert providers == ["graph"]
    routing = payload.get("provider_routing", {})
    assert isinstance(routing, dict)
    assert str(routing.get("selected_provider", "")) == "graph"
    ranked = routing.get("ranked", [])
    assert isinstance(ranked, list)
    assert ranked
    assert str(ranked[0].get("auth_status", "")) in {"ready", "warning", "degraded"}
    assert bool(ranked[0].get("auth_ready", False)) is True


def test_route_provider_exposes_auth_preflight_summary(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))
    route = orchestrator.route_provider(
        action="external_doc_list",
        args={"provider": "auto", "max_results": 5},
        metadata={
            "external_auth_state": {
                "providers": {
                    "google": {
                        "has_credentials": True,
                        "expires_in_s": 1200,
                        "has_refresh_token": True,
                        "scopes": ["drive.readonly"],
                    },
                    "graph": {
                        "has_credentials": False,
                        "expires_in_s": 0,
                        "has_refresh_token": False,
                        "scopes": [],
                    },
                }
            }
        },
    )

    assert route["status"] == "success"
    assert isinstance(route.get("auth_preflight"), dict)
    ranked = route.get("ranked", [])
    assert isinstance(ranked, list)
    assert ranked
    assert "auth_status" in ranked[0]


def test_cooldown_seconds_respects_adaptive_bias_factor(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    baseline = orchestrator._cooldown_seconds(  # noqa: SLF001
        provider="google",
        category="timeout",
        operation_class="mutate",
        consecutive_failures=4,
        failure_ema=0.72,
        trend_ema=0.2,
        operation_trend_ema=0.2,
        cooldown_bias=1.0,
    )
    elevated = orchestrator._cooldown_seconds(  # noqa: SLF001
        provider="google",
        category="timeout",
        operation_class="mutate",
        consecutive_failures=4,
        failure_ema=0.72,
        trend_ema=0.2,
        operation_trend_ema=0.2,
        cooldown_bias=2.0,
    )
    relieved = orchestrator._cooldown_seconds(  # noqa: SLF001
        provider="google",
        category="timeout",
        operation_class="mutate",
        consecutive_failures=4,
        failure_ema=0.72,
        trend_ema=0.2,
        operation_trend_ema=0.2,
        cooldown_bias=0.6,
    )

    assert elevated > baseline
    assert baseline > relieved


def test_cooldown_bias_adapts_from_failures_and_successes(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))
    for attempt in range(4):
        orchestrator.record_outcome(
            action="external_task_update",
            args={"provider": "google", "task_id": "task-1", "title": "Updated"},
            status="failed",
            error="429 rate limit",
            output={"status": "error", "message": "429 rate limit"},
            metadata={
                "__result_attempt": attempt + 1,
                "__external_route_strategy": "fallback_ranked",
                "__confirm_policy_mode": "all",
                "__confirm_policy_satisfied": False,
            },
        )

    failed_snapshot = orchestrator.snapshot(provider="google", limit=2)
    failed_row = failed_snapshot["items"][0]
    failed_bias = float(failed_row.get("cooldown_bias", 1.0) or 1.0)
    assert failed_bias > 1.0

    for _ in range(8):
        orchestrator.record_outcome(
            action="external_task_update",
            args={"provider": "google", "task_id": "task-1", "title": "Updated"},
            status="success",
            error="",
            output={"status": "success"},
            metadata={"__result_attempt": 1},
        )

    recovered_snapshot = orchestrator.snapshot(provider="google", limit=2)
    recovered_row = recovered_snapshot["items"][0]
    recovered_bias = float(recovered_row.get("cooldown_bias", 1.0) or 1.0)
    assert recovered_bias < failed_bias

    hint = orchestrator.retry_hint(
        action="external_task_update",
        args={"provider": "google", "task_id": "task-1"},
        metadata={},
    )
    if hint.get("status") == "success":
        assert "cooldown_bias" in hint
        assert "cooldown_bias_pressure" in hint


def test_preflight_returns_structured_contract_diagnostics_for_missing_fields(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    payload = orchestrator.preflight(
        action="external_doc_update",
        args={"document_id": "doc-77"},
        metadata={},
    )

    assert payload["status"] == "error"
    diag = payload.get("contract_diagnostic", {})
    assert isinstance(diag, dict)
    assert str(diag.get("code", "")) in {"missing_any_of_fields", "missing_required_fields"}
    hints = payload.get("remediation_hints", [])
    assert isinstance(hints, list)
    assert hints
    remediation_contract = payload.get("remediation_contract", {})
    assert isinstance(remediation_contract, dict)
    assert int(remediation_contract.get("strategy_count", 0) or 0) >= 1


def test_preflight_returns_provider_contract_diagnostics_for_unsupported_provider(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    payload = orchestrator.preflight(
        action="external_doc_update",
        args={"provider": "smtp", "document_id": "doc-77", "content": "hello"},
        metadata={},
    )

    assert payload["status"] == "error"
    diag = payload.get("contract_diagnostic", {})
    assert isinstance(diag, dict)
    assert str(diag.get("code", "")).strip().lower() in {
        "provider_not_supported_for_action",
        "provider_contract_failed",
        "invalid_field_type_or_range",
    }
    hints = payload.get("remediation_hints", [])
    assert isinstance(hints, list)
    assert hints
    remediation_contract = payload.get("remediation_contract", {})
    strategies = remediation_contract.get("strategies", []) if isinstance(remediation_contract, dict) else []
    assert isinstance(strategies, list)
    if str(diag.get("code", "")).strip().lower() in {"provider_not_supported_for_action", "provider_contract_failed"}:
        assert any(
            isinstance(strategy, dict) and str(strategy.get("type", "")).strip().lower() == "args_patch"
            for strategy in strategies
        )


def test_preflight_returns_auth_contract_diagnostics_when_auth_missing(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))
    orchestrator.auth_precheck_fail_closed = True

    payload = orchestrator.preflight(
        action="external_email_send",
        args={"provider": "auto", "to": ["a@example.com"], "subject": "S", "body": "B"},
        metadata={
            "external_auth_state": {
                "providers": {
                    "google": {"has_credentials": False, "expires_in_s": 0, "has_refresh_token": False, "scopes": []},
                    "graph": {"has_credentials": False, "expires_in_s": 0, "has_refresh_token": False, "scopes": []},
                }
            }
        },
    )

    assert payload["status"] == "blocked"
    assert payload.get("failure_category") == "auth"
    diag = payload.get("contract_diagnostic", {})
    assert isinstance(diag, dict)
    assert str(diag.get("code", "")) == "auth_preflight_failed"
    hints = payload.get("remediation_hints", [])
    assert isinstance(hints, list)
    assert hints
    assert any(isinstance(row, dict) and str(row.get("id", "")).strip() for row in hints)


def test_outage_policy_bias_self_tunes_from_mission_pressure(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))
    provider = "graph"

    baseline = orchestrator.snapshot(provider=provider, limit=1)
    baseline_row = baseline.get("items", [{}])[0] if isinstance(baseline.get("items"), list) and baseline.get("items") else {}
    baseline_route_threshold = float(baseline_row.get("route_block_threshold", orchestrator.outage_route_hard_block_threshold) or orchestrator.outage_route_hard_block_threshold)

    for _ in range(6):
        orchestrator.record_outcome(
            action="external_task_update",
            args={"provider": provider, "task_id": "task-1", "status": "completed"},
            status="failed",
            error="request timed out",
            output={"status": "error"},
            metadata={
                "mission_trend_feedback": {
                    "mode": "worsening",
                    "trend_pressure": 0.84,
                    "risk_trend": "worsening",
                    "quality_trend": "degrading",
                },
                "__result_attempt": 2,
                "__external_route_strategy": "fallback_ranked",
            },
        )

    tuned = orchestrator.snapshot(provider=provider, limit=1)
    assert tuned["status"] == "success"
    tuned_row = tuned["items"][0]
    bias = float(tuned_row.get("outage_policy_bias", 0.0) or 0.0)
    tuned_route_threshold = float(tuned_row.get("route_block_threshold", orchestrator.outage_route_hard_block_threshold) or orchestrator.outage_route_hard_block_threshold)
    assert bias > 0.0
    assert tuned_route_threshold < baseline_route_threshold


def test_mission_outage_policy_autotune_updates_global_bias(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    before = orchestrator.snapshot(limit=1)
    before_policy = before.get("mission_outage_policy", {}) if isinstance(before, dict) else {}
    before_bias = float(before_policy.get("bias", 0.0) or 0.0)

    payload = orchestrator.tune_from_operational_signals(
        autonomy_report={
            "pressures": {
                "failure_pressure": 0.62,
                "open_breaker_pressure": 0.44,
            },
            "scores": {"reliability": 41.0},
        },
        mission_summary={
            "failed_ratio": 0.58,
            "blocked_ratio": 0.21,
            "recommendation": "stability",
            "risk": {"avg_score": 0.74},
            "quality": {"avg_score": 0.34},
            "trend": {"mode": "worsening", "pressure": 0.8},
        },
        dry_run=False,
        reason="unit-test",
    )
    assert payload["status"] == "success"
    after = orchestrator.snapshot(limit=1)
    after_policy = after.get("mission_outage_policy", {}) if isinstance(after, dict) else {}
    after_bias = float(after_policy.get("bias", 0.0) or 0.0)
    assert after_bias >= before_bias
    assert str(after_policy.get("mode", "")) == "worsening"


def test_mission_outage_profile_autotune_switches_under_high_cross_mission_pressure(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    result = orchestrator.tune_from_operational_signals(
        autonomy_report={
            "pressures": {
                "failure_pressure": 0.91,
                "open_breaker_pressure": 0.72,
            },
            "scores": {"reliability": 22.0},
        },
        mission_summary={
            "failed_ratio": 0.68,
            "blocked_ratio": 0.27,
            "recommendation": "stability",
            "risk": {"avg_score": 0.86},
            "quality": {"avg_score": 0.18},
            "trend": {"mode": "worsening", "pressure": 0.9},
        },
        dry_run=False,
        reason="profile-pressure-test",
    )
    assert result["status"] == "success"
    assert str(result.get("profile", "")) in {"defensive", "cautious"}

    snapshot = orchestrator.snapshot(limit=1)
    policy = snapshot.get("mission_outage_policy", {}) if isinstance(snapshot, dict) else {}
    assert str(policy.get("profile", "")) in {"defensive", "cautious"}
    assert int(policy.get("profile_switch_count", 0) or 0) >= 1


def test_mission_outage_pressure_includes_retry_contract_signals(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    baseline = orchestrator._mission_outage_pressure({})  # noqa: SLF001
    elevated = orchestrator._mission_outage_pressure(  # noqa: SLF001
        {
            "__external_retry_contract_mode": "stabilize",
            "__external_retry_contract_risk": 0.82,
            "__external_retry_contract_cooldown_s": 210.0,
            "mission_trend_feedback": {"mode": "stable", "trend_pressure": 0.12},
        }
    )

    assert float(elevated.get("pressure", 0.0) or 0.0) > float(baseline.get("pressure", 0.0) or 0.0)
    assert bool(elevated.get("worsening", False)) is True


def test_contract_diagnostics_include_structured_remediation_metadata(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    payload = orchestrator.preflight(
        action="external_doc_update",
        args={"document_id": "doc-1"},
        metadata={},
    )

    assert payload["status"] == "error"
    diag = payload.get("contract_diagnostic", {})
    assert isinstance(diag, dict)
    diagnostics = diag.get("diagnostics", {})
    assert isinstance(diagnostics, dict)
    assert str(diagnostics.get("domain", "")) == "connector_contract"
    remediation_contract = payload.get("remediation_contract", {})
    assert isinstance(remediation_contract, dict)
    assert int(remediation_contract.get("strategy_count", 0) or 0) >= 1
    assert int(remediation_contract.get("args_patch_count", 0) or 0) >= 0
    remediation_diag = remediation_contract.get("diagnostics", {})
    assert isinstance(remediation_diag, dict)
    assert str(remediation_diag.get("failure_code", "")).strip()


def test_contract_diagnostic_exposes_checks_and_remediation_plan(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    payload = orchestrator.preflight(
        action="external_doc_update",
        args={"document_id": "doc-42"},
        metadata={},
    )

    assert payload["status"] == "error"
    diag = payload.get("contract_diagnostic", {})
    assert isinstance(diag, dict)
    assert str(diag.get("diagnostic_id", "")).strip().startswith("diag_")
    assert str(diag.get("contract_stage", "")) == "payload_contract"
    checks = diag.get("checks", [])
    assert isinstance(checks, list)
    assert any(isinstance(row, dict) and str(row.get("check", "")) == "payload_schema" for row in checks)
    plan = diag.get("remediation_plan", [])
    assert isinstance(plan, list)
    assert any(isinstance(row, dict) and str(row.get("phase", "")) == "retry" for row in plan)


def test_route_provider_exposes_mission_profile_alignment_signals(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    for _ in range(6):
        orchestrator.record_outcome(
            action="external_task_list",
            args={"provider": "google"},
            status="success",
            error="",
            output={"status": "success"},
            metadata={"external_route_profile": "defensive"},
        )
    for _ in range(6):
        orchestrator.record_outcome(
            action="external_task_list",
            args={"provider": "graph"},
            status="failed",
            error="request timed out",
            output={"status": "error"},
            metadata={"external_route_profile": "defensive"},
        )

    route = orchestrator.route_provider(
        action="external_task_list",
        args={"provider": "auto"},
        metadata={"external_route_profile": "defensive"},
    )

    assert route["status"] == "success"
    ranked = route.get("ranked", [])
    assert isinstance(ranked, list)
    assert ranked
    assert "mission_profile_alignment" in ranked[0]
    assert "mission_profile_samples" in ranked[0]
    assert "mission_profile_success_rate" in ranked[0]
    assert str(route.get("selected_provider", "")) == "google"


def test_tune_from_operational_signals_tracks_capability_bias_from_hotspots(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    tuned = orchestrator.tune_from_operational_signals(
        autonomy_report={
            "pressures": {
                "failure_pressure": 0.62,
                "open_breaker_pressure": 0.28,
            },
            "scores": {"reliability": 42.0},
            "action_hotspots": [
                {"action": "external_email_send", "failures": 9, "runs": 11, "failure_rate": 0.82},
                {"action": "external_task_update", "failures": 6, "runs": 10, "failure_rate": 0.6},
            ],
        },
        mission_summary={
            "trend": {"mode": "worsening", "pressure": 0.66},
            "risk": {"avg_score": 0.71},
            "quality": {"avg_score": 0.39},
            "failed_ratio": 0.44,
            "blocked_ratio": 0.18,
            "recommendation": "stability",
        },
        dry_run=False,
        reason="unit-test-capability-bias",
    )

    assert tuned["status"] == "success"
    capability_targets = tuned.get("capability_targets", {})
    assert isinstance(capability_targets, dict)
    assert "email" in capability_targets

    state = tuned.get("state", {})
    assert isinstance(state, dict)
    capability_bias = state.get("capability_bias", {})
    assert isinstance(capability_bias, dict)
    assert "email" in capability_bias
    email_row = capability_bias.get("email", {})
    assert isinstance(email_row, dict)
    assert float(email_row.get("bias", 0.0) or 0.0) > 0.0

    snapshot = orchestrator.snapshot(limit=5)
    mission_policy = snapshot.get("mission_outage_policy", {})
    assert isinstance(mission_policy, dict)
    snapshot_capability_bias = mission_policy.get("capability_bias", {})
    assert isinstance(snapshot_capability_bias, dict)
    assert "email" in snapshot_capability_bias


def test_outage_policy_thresholds_apply_mission_capability_bias(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))
    orchestrator._mission_outage_policy["capability_bias"] = {  # noqa: SLF001
        "email": {
            "bias": 0.32,
            "pressure_ema": 0.74,
            "samples": 8,
            "weight": 0.88,
            "top_action": "external_email_send",
            "updated_at": "2026-03-03T00:00:00+00:00",
        }
    }

    email_thresholds = orchestrator._outage_policy_thresholds(  # noqa: SLF001
        state={"outage_policy_bias": 0.0},
        metadata={"__external_action": "external_email_send", "__external_capability": "email"},
    )
    task_thresholds = orchestrator._outage_policy_thresholds(  # noqa: SLF001
        state={"outage_policy_bias": 0.0},
        metadata={"__external_action": "external_task_list", "__external_capability": "task"},
    )

    assert str(email_thresholds.get("mission_capability", "")) == "email"
    assert float(email_thresholds.get("mission_capability_bias", 0.0) or 0.0) > 0.0
    assert float(task_thresholds.get("mission_capability_bias", 0.0) or 0.0) == 0.0
    assert float(email_thresholds.get("bias", 0.0) or 0.0) > float(task_thresholds.get("bias", 0.0) or 0.0)


def test_preflight_provider_capability_contract_blocks_disabled_runtime_providers(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    payload = orchestrator.preflight(
        action="external_email_send",
        args={
            "provider": "auto",
            "to": ["person@example.com"],
            "subject": "status",
            "body": "hello",
        },
        metadata={
            "external_provider_capabilities": {
                "google": {"enabled": False},
                "graph": {"enabled": False},
                "smtp": {"enabled": False},
            }
        },
    )

    assert payload["status"] == "error"
    diag = payload.get("contract_diagnostic", {})
    assert isinstance(diag, dict)
    assert str(diag.get("code", "")).strip().lower() in {
        "provider_runtime_capability_disabled",
        "provider_capability_contract_failed",
    }
    hints = payload.get("remediation_hints", [])
    assert isinstance(hints, list)
    assert any(
        isinstance(row, dict)
        and str(row.get("id", "")).strip().lower() in {"refresh_provider_capability_contract", "review_dropped_providers"}
        for row in hints
    )


def test_route_provider_filters_by_runtime_provider_capability_contract(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    route = orchestrator.route_provider(
        action="external_email_send",
        args={
            "provider": "auto",
            "to": ["person@example.com"],
            "subject": "status",
            "body": "hello",
        },
        metadata={
            "external_provider_capabilities": {
                "google": {"enabled": True, "action_allow": ["external_email_send"]},
                "graph": {"enabled": True, "action_deny": ["external_email_send"]},
                "smtp": {"enabled": True, "action_allow": ["external_email_list"]},
            },
            "external_auth_state": {
                "providers": {
                    "google": {
                        "has_credentials": True,
                        "has_refresh_token": True,
                        "expires_in_s": 3600,
                        "scopes": ["gmail.send"],
                    }
                }
            },
        },
    )

    assert route["status"] == "success"
    assert str(route.get("selected_provider", "")).strip().lower() == "google"
    contract_negotiation = route.get("contract_negotiation", {})
    assert isinstance(contract_negotiation, dict)
    dropped = contract_negotiation.get("dropped_providers", [])
    assert isinstance(dropped, list)
    assert any(
        isinstance(row, dict)
        and str(row.get("reason", "")).strip().lower() in {"provider_action_not_allowed", "provider_action_blocked"}
        for row in dropped
    )


def test_mission_policy_config_update_returns_validation_and_normalization(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    payload = orchestrator.update_mission_policy_config(
        config={
            "mission_outage_bias_gain": 1.35,
            "mission_outage_bias_decay": 0.6,
            "provider_policy_max_providers": 3,
            "outage_trip_threshold": 0.72,
            "outage_recover_threshold": 0.71,
            "outage_route_hard_block_threshold": 0.9,
            "outage_preflight_block_threshold": 0.84,
            "unknown_toggle": True,
        },
        persist_now=False,
    )

    assert payload["status"] == "success"
    assert bool(payload.get("updated", False)) is True
    validation = payload.get("validation", {})
    assert isinstance(validation, dict)
    summary = validation.get("summary", {})
    assert isinstance(summary, dict)
    assert int(summary.get("warning_count", 0) or 0) >= 2
    assert str(validation.get("recommended_preset_id", "")).strip().lower() == "stability_guard"
    normalized = validation.get("normalized", [])
    assert isinstance(normalized, list)
    assert any(isinstance(row, dict) and str(row.get("field", "")) == "unknown_toggle" for row in normalized)
    assert any(
        isinstance(row, dict) and str(row.get("field", "")) == "outage_preflight_block_threshold"
        for row in normalized
    )
    presets = validation.get("presets", [])
    assert isinstance(presets, list)
    assert any(isinstance(row, dict) and str(row.get("id", "")).strip().lower() == "stability_guard" for row in presets)
    history_context = validation.get("history_context", {})
    assert isinstance(history_context, dict)
    diagnostics = history_context.get("diagnostics", {})
    assert isinstance(diagnostics, dict)
    decision_trace = validation.get("decision_trace", {})
    assert isinstance(decision_trace, dict)
    assert str(decision_trace.get("recommended_preset_id", "")).strip().lower() == "stability_guard"
    metrics = validation.get("metrics", {})
    assert isinstance(metrics, dict)
    assert float(metrics.get("route_preflight_gap", 0.0) or 0.0) >= 0.01
    warnings = validation.get("warnings", [])
    assert isinstance(warnings, list)
    assert any(
        isinstance(row, dict)
        and str(row.get("code", "")).strip().lower() == "provider_candidate_pool_narrow"
        for row in warnings
    )


def test_mission_policy_config_resolves_recommended_preset_and_remediation_action(tmp_path) -> None:
    orchestrator = ExternalReliabilityOrchestrator(store_path=str(tmp_path / "external_reliability.json"))

    orchestrator.update_mission_policy_config(
        config={
            "mission_outage_bias_gain": 1.35,
            "mission_outage_bias_decay": 0.6,
            "mission_outage_profile_decay": 0.58,
            "mission_outage_profile_stability_decay": 0.64,
            "outage_trip_threshold": 0.72,
            "outage_recover_threshold": 0.71,
            "outage_route_hard_block_threshold": 0.9,
            "outage_preflight_block_threshold": 0.84,
        },
        persist_now=False,
    )

    preset_payload = orchestrator.update_mission_policy_config(
        config={"apply_recommended_preset": True},
        persist_now=False,
    )
    assert preset_payload["status"] == "success"
    preset_validation = preset_payload.get("validation", {})
    assert isinstance(preset_validation, dict)
    resolved_actions = preset_validation.get("resolved_actions", [])
    assert isinstance(resolved_actions, list)
    assert any(isinstance(row, dict) and str(row.get("kind", "")).strip().lower() == "preset" for row in resolved_actions)
    assert float(preset_payload.get("config", {}).get("mission_outage_profile_decay", 0.0) or 0.0) >= 0.84
    assert float(preset_payload.get("config", {}).get("mission_outage_bias_decay", 0.0) or 0.0) >= 0.82

    orchestrator.update_mission_policy_config(
        config={
            "outage_trip_threshold": 0.72,
            "outage_recover_threshold": 0.71,
        },
        persist_now=False,
    )
    remediation_payload = orchestrator.update_mission_policy_config(
        config={"remediation_action": "widen_trip_recover_gap"},
        persist_now=False,
    )
    assert remediation_payload["status"] == "success"
    remediation_validation = remediation_payload.get("validation", {})
    assert isinstance(remediation_validation, dict)
    remediation_actions = remediation_validation.get("resolved_actions", [])
    assert isinstance(remediation_actions, list)
    assert any(
        isinstance(row, dict) and str(row.get("kind", "")).strip().lower() == "remediation"
        for row in remediation_actions
    )
    assert float(remediation_payload.get("config", {}).get("outage_recover_threshold", 1.0) or 1.0) <= 0.64
