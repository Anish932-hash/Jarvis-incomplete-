from __future__ import annotations

from backend.python.tools.external_connectors import ExternalConnectors


def test_list_emails_requires_available_provider(monkeypatch) -> None:
    monkeypatch.delenv("GOOGLE_OAUTH_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("GOOGLE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MICROSOFT_GRAPH_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("SMTP_HOST", raising=False)
    monkeypatch.setattr(ExternalConnectors, "_token", classmethod(lambda _cls, *_args, **_kwargs: ""))

    result = ExternalConnectors.list_emails({})

    assert result["status"] == "error"
    assert "provider credentials" in str(result.get("message", "")).lower()


def test_read_email_requires_message_id() -> None:
    result = ExternalConnectors.read_email({})
    assert result["status"] == "error"
    assert "message_id" in str(result.get("message", ""))


def test_update_calendar_event_requires_event_id() -> None:
    result = ExternalConnectors.update_calendar_event({"title": "new title"})
    assert result["status"] == "error"
    assert "event_id" in str(result.get("message", ""))


def test_update_calendar_event_requires_mutable_fields() -> None:
    result = ExternalConnectors.update_calendar_event({"event_id": "evt-1"})
    assert result["status"] == "error"
    assert "mutable field" in str(result.get("message", "")).lower()


def test_update_document_requires_document_id() -> None:
    result = ExternalConnectors.update_document({"title": "Title"})
    assert result["status"] == "error"
    assert "document_id" in str(result.get("message", ""))


def test_update_document_requires_title_or_content() -> None:
    result = ExternalConnectors.update_document({"document_id": "doc-1"})
    assert result["status"] == "error"
    assert "title or content" in str(result.get("message", "")).lower()


def test_list_documents_calls_google_provider_when_requested(monkeypatch) -> None:
    monkeypatch.setattr(
        ExternalConnectors,
        "_list_google_docs",
        classmethod(lambda _cls, _payload, *, max_results, query: {"status": "success", "provider": "google_docs", "count": 0, "items": [], "max_results": max_results, "query": query}),
    )
    monkeypatch.setattr(ExternalConnectors, "_resolve_document_provider", classmethod(lambda _cls, _payload: "google_docs"))

    result = ExternalConnectors.list_documents({"provider": "google_docs", "max_results": 11, "query": "weekly"})

    assert result["status"] == "success"
    assert result["provider"] == "google_docs"
    assert result["max_results"] == 11
    assert result["query"] == "weekly"


def test_extract_google_doc_text_flattens_paragraph_runs() -> None:
    payload = {
        "body": {
            "content": [
                {
                    "paragraph": {
                        "elements": [
                            {"textRun": {"content": "Hello "}},
                            {"textRun": {"content": "world"}},
                        ]
                    }
                }
            ]
        }
    }

    text = ExternalConnectors._extract_google_doc_text(payload)  # noqa: SLF001

    assert text == "Hello world"


def test_list_tasks_requires_available_provider(monkeypatch) -> None:
    monkeypatch.delenv("GOOGLE_OAUTH_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("GOOGLE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MICROSOFT_GRAPH_ACCESS_TOKEN", raising=False)
    monkeypatch.setattr(ExternalConnectors, "_token", classmethod(lambda _cls, *_args, **_kwargs: ""))

    result = ExternalConnectors.list_tasks({})

    assert result["status"] == "error"
    assert "provider credentials" in str(result.get("message", "")).lower()


def test_create_task_requires_title() -> None:
    result = ExternalConnectors.create_task({"notes": "No title"})

    assert result["status"] == "error"
    assert "title" in str(result.get("message", "")).lower()


def test_update_task_requires_task_id() -> None:
    result = ExternalConnectors.update_task({"title": "Rename this"})

    assert result["status"] == "error"
    assert "task_id" in str(result.get("message", "")).lower()


def test_update_task_requires_mutable_fields() -> None:
    result = ExternalConnectors.update_task({"task_id": "task-1"})

    assert result["status"] == "error"
    assert "mutable field" in str(result.get("message", "")).lower()


def test_normalize_task_status_maps_completed_for_google_and_graph() -> None:
    google_value = ExternalConnectors._normalize_task_status("done", provider_hint="google_tasks")  # noqa: SLF001
    graph_value = ExternalConnectors._normalize_task_status("done", provider_hint="graph_todo")  # noqa: SLF001

    assert google_value == "completed"
    assert graph_value == "completed"


def test_list_emails_auto_provider_fallback_to_graph(monkeypatch) -> None:
    monkeypatch.setattr(ExternalConnectors, "_auto_email_candidates", classmethod(lambda _cls, _payload, include_smtp=False: ["gmail", "graph"]))
    monkeypatch.setattr(
        ExternalConnectors,
        "_list_gmail_messages",
        classmethod(lambda _cls, _payload, *, max_results, query: {"status": "error", "message": f"gmail failed {max_results}:{query}"}),
    )
    monkeypatch.setattr(
        ExternalConnectors,
        "_list_graph_messages",
        classmethod(lambda _cls, _payload, *, max_results, query: {"status": "success", "provider": "microsoft_graph_mail", "count": 1, "items": [{"id": "msg-1"}], "max_results": max_results, "query": query}),
    )

    result = ExternalConnectors.list_emails({"provider": "auto", "max_results": 7, "query": "invoice"})

    assert result["status"] == "success"
    assert result["provider"] == "microsoft_graph_mail"
    resilience = result.get("resilience", {})
    fallback = resilience.get("provider_fallback", {}) if isinstance(resilience, dict) else {}
    assert fallback.get("selected_provider") == "graph"
    assert int(fallback.get("attempt_count", 0)) == 2


def test_read_document_auto_provider_reports_chain_failures(monkeypatch) -> None:
    monkeypatch.setattr(ExternalConnectors, "_auto_document_candidates", classmethod(lambda _cls, _payload: ["google_docs", "graph_drive"]))
    monkeypatch.setattr(
        ExternalConnectors,
        "_read_google_doc",
        classmethod(lambda _cls, _payload, *, document_id: {"status": "error", "message": f"google failed {document_id}"}),
    )
    monkeypatch.setattr(
        ExternalConnectors,
        "_read_graph_doc",
        classmethod(lambda _cls, _payload, *, document_id: {"status": "error", "message": f"graph failed {document_id}"}),
    )

    result = ExternalConnectors.read_document({"provider": "auto", "document_id": "doc-42"})

    assert result["status"] == "error"
    resilience = result.get("resilience", {})
    fallback = resilience.get("provider_fallback", {}) if isinstance(resilience, dict) else {}
    assert fallback.get("capability") == "document_read"
    assert int(fallback.get("attempt_count", 0)) == 2
    failed_attempts = fallback.get("failed_attempts", [])
    assert isinstance(failed_attempts, list)
    assert len(failed_attempts) == 2


def test_send_email_auto_provider_fallback_to_smtp(monkeypatch) -> None:
    monkeypatch.setattr(ExternalConnectors, "_auto_email_candidates", classmethod(lambda _cls, _payload, include_smtp=True: ["gmail", "smtp"]))
    monkeypatch.setattr(
        ExternalConnectors,
        "_send_gmail",
        classmethod(lambda _cls, _payload, *, to_list, subject, body: {"status": "error", "message": "gmail outage", "to": to_list, "subject": subject, "body": body}),
    )
    monkeypatch.setattr(
        ExternalConnectors,
        "_send_smtp",
        classmethod(lambda _cls, _payload, *, to_list, subject, body: {"status": "success", "provider": "smtp", "to": to_list, "subject": subject, "chars": len(body)}),
    )

    result = ExternalConnectors.send_email({"provider": "auto", "to": "a@example.com", "subject": "Hello", "body": "Body"})

    assert result["status"] == "success"
    assert result["provider"] == "smtp"
    resilience = result.get("resilience", {})
    fallback = resilience.get("provider_fallback", {}) if isinstance(resilience, dict) else {}
    assert fallback.get("selected_provider") == "smtp"


def test_send_email_dry_run_selects_viable_provider_and_emits_args_patch(monkeypatch) -> None:
    monkeypatch.setattr(
        ExternalConnectors,
        "_auto_email_candidates",
        classmethod(lambda _cls, _payload, include_smtp=True: ["gmail", "graph"]),
    )

    def _token_stub(_cls, _payload, _payload_keys, _env_keys, *, provider=""):
        return "graph-token" if str(provider) == "graph" else ""

    monkeypatch.setattr(ExternalConnectors, "_token", classmethod(_token_stub))

    result = ExternalConnectors.send_email(
        {
            "provider": "auto",
            "to": "team@example.com",
            "subject": "Quarterly update",
            "body": "Hello from JARVIS",
            "dry_run": True,
        }
    )

    assert result["status"] == "success"
    assert bool(result.get("dry_run", False)) is True
    args_patch = result.get("args_patch", {})
    assert isinstance(args_patch, dict)
    assert args_patch.get("provider") == "graph"
    simulation = result.get("simulation", {})
    assert isinstance(simulation, dict)
    assert simulation.get("selected_provider") == "graph"
    diagnostics = simulation.get("provider_diagnostics", [])
    assert isinstance(diagnostics, list)
    assert len(diagnostics) >= 2


def test_doc_update_dry_run_reports_contract_failure_when_no_provider_credentials(monkeypatch) -> None:
    monkeypatch.setattr(
        ExternalConnectors,
        "_auto_document_candidates",
        classmethod(lambda _cls, _payload: ["google_docs", "graph_drive"]),
    )
    monkeypatch.setattr(ExternalConnectors, "_token", classmethod(lambda _cls, *_args, **_kwargs: ""))

    result = ExternalConnectors.update_document(
        {
            "provider": "auto",
            "document_id": "doc-123",
            "content": "Patch content",
            "dry_run": True,
        }
    )

    assert result["status"] == "error"
    assert bool(result.get("dry_run", False)) is True
    simulation = result.get("simulation", {})
    assert isinstance(simulation, dict)
    assert simulation.get("selected_provider") == ""
    hints = simulation.get("remediation_hints", [])
    assert isinstance(hints, list)
    assert hints


def test_provider_chain_skips_blocked_preflight_provider_before_fallback(monkeypatch) -> None:
    monkeypatch.setattr(ExternalConnectors, "_auto_email_candidates", classmethod(lambda _cls, _payload, include_smtp=False: ["gmail", "graph"]))

    def _token_stub(_cls, _payload, _payload_keys, _env_keys, *, provider=""):
        return "graph-token" if str(provider) == "graph" else ""

    monkeypatch.setattr(ExternalConnectors, "_token", classmethod(_token_stub))
    monkeypatch.setattr(
        ExternalConnectors,
        "_list_graph_messages",
        classmethod(lambda _cls, _payload, *, max_results, query: {"status": "success", "provider": "microsoft_graph_mail", "count": 1, "items": [{"id": "msg-7"}], "max_results": max_results, "query": query}),
    )

    result = ExternalConnectors.list_emails({"provider": "auto", "max_results": 5, "query": "alerts"})

    assert result["status"] == "success"
    resilience = result.get("resilience", {})
    assert isinstance(resilience, dict)
    fallback = resilience.get("provider_fallback", {})
    assert isinstance(fallback, dict)
    assert str(fallback.get("selected_provider", "")) == "graph"
    assert int(fallback.get("attempt_count", 0) or 0) == 2
    failed_attempts = fallback.get("failed_attempts", [])
    assert isinstance(failed_attempts, list)
    assert failed_attempts
    assert str(failed_attempts[0].get("status", "")) == "blocked_preflight"


def test_provider_chain_retries_transient_failure_before_success(monkeypatch) -> None:
    monkeypatch.setattr(ExternalConnectors, "_auto_email_candidates", classmethod(lambda _cls, _payload, include_smtp=False: ["gmail", "graph"]))
    monkeypatch.setattr(ExternalConnectors, "_token", classmethod(lambda _cls, *_args, **_kwargs: "gmail-token"))
    call_counter = {"gmail": 0}

    def _flaky_gmail(_cls, _payload, *, max_results, query):
        call_counter["gmail"] += 1
        if call_counter["gmail"] == 1:
            return {"status": "error", "message": "request timed out"}
        return {
            "status": "success",
            "provider": "gmail",
            "count": 1,
            "items": [{"id": "g-1"}],
            "max_results": max_results,
            "query": query,
        }

    monkeypatch.setattr(ExternalConnectors, "_list_gmail_messages", classmethod(_flaky_gmail))

    result = ExternalConnectors.list_emails({"provider": "auto", "max_results": 3, "query": "invoice"})

    assert result["status"] == "success"
    assert call_counter["gmail"] == 2
    resilience = result.get("resilience", {})
    assert isinstance(resilience, dict)
    fallback = resilience.get("provider_fallback", {})
    assert isinstance(fallback, dict)
    assert str(fallback.get("selected_provider", "")) == "gmail"
    assert int(fallback.get("attempt_count", 0) or 0) == 2
    failed_attempts = fallback.get("failed_attempts", [])
    assert isinstance(failed_attempts, list)
    assert failed_attempts
    assert str(failed_attempts[0].get("category", "")) == "transient"


def test_provider_chain_error_exposes_structured_remediation_contract(monkeypatch) -> None:
    monkeypatch.setattr(ExternalConnectors, "_auto_email_candidates", classmethod(lambda _cls, _payload, include_smtp=False: ["gmail"]))
    monkeypatch.setattr(ExternalConnectors, "_token", classmethod(lambda _cls, *_args, **_kwargs: "gmail-token"))
    monkeypatch.setattr(
        ExternalConnectors,
        "_list_gmail_messages",
        classmethod(lambda _cls, _payload, *, max_results, query: {"status": "error", "message": f"token expired for {query}", "max_results": max_results}),
    )

    result = ExternalConnectors.list_emails({"provider": "auto", "max_results": 2, "query": "ops"})

    assert result["status"] == "error"
    hints = result.get("remediation_hints", [])
    assert isinstance(hints, list)
    assert hints
    assert any(str(item.get("id", "")) == "auth_maintenance_cycle" for item in hints if isinstance(item, dict))
    remediation_contract = result.get("remediation_contract", {})
    assert isinstance(remediation_contract, dict)
    assert str(remediation_contract.get("version", "")) == "1.0"
    assert int(remediation_contract.get("strategy_count", 0) or 0) >= 1


def test_connector_preflight_reports_missing_required_fields(monkeypatch) -> None:
    monkeypatch.setattr(ExternalConnectors, "_auto_email_candidates", classmethod(lambda _cls, _payload, include_smtp=True: ["gmail", "graph"]))
    monkeypatch.setattr(ExternalConnectors, "_token", classmethod(lambda _cls, *_args, **_kwargs: "token"))

    result = ExternalConnectors.connector_preflight({"action": "external_email_send", "provider": "auto"})

    assert result["status"] == "error"
    diagnostic = result.get("contract_diagnostic", {})
    assert isinstance(diagnostic, dict)
    assert str(diagnostic.get("code", "")) == "missing_required_fields"
    assert "to" in (diagnostic.get("missing_fields", []) if isinstance(diagnostic.get("missing_fields", []), list) else [])


def test_connector_preflight_returns_auth_failure_with_structured_contract(monkeypatch) -> None:
    monkeypatch.setattr(ExternalConnectors, "_auto_email_candidates", classmethod(lambda _cls, _payload, include_smtp=False: ["gmail", "graph"]))
    monkeypatch.setattr(ExternalConnectors, "_token", classmethod(lambda _cls, *_args, **_kwargs: ""))

    result = ExternalConnectors.connector_preflight({"action": "external_email_list", "provider": "auto", "query": "ops"})

    assert result["status"] == "error"
    diagnostic = result.get("contract_diagnostic", {})
    assert isinstance(diagnostic, dict)
    assert str(diagnostic.get("code", "")) == "auth_preflight_failed"
    remediation_contract = result.get("remediation_contract", {})
    assert isinstance(remediation_contract, dict)
    assert str(remediation_contract.get("version", "")) == "1.1"
    strategies = remediation_contract.get("strategies", [])
    assert isinstance(strategies, list)
    assert any(
        isinstance(item, dict)
        and str(item.get("type", "")).strip().lower() == "tool_action"
        and isinstance(item.get("tool_action"), dict)
        and str(item.get("tool_action", {}).get("action", "")).strip().lower() == "oauth_token_maintain"
        for item in strategies
    )


def test_connector_preflight_success_reports_selected_provider(monkeypatch) -> None:
    monkeypatch.setattr(ExternalConnectors, "_auto_email_candidates", classmethod(lambda _cls, _payload, include_smtp=False: ["gmail", "graph"]))

    def _token_stub(_cls, _payload, _payload_keys, _env_keys, *, provider=""):
        return "graph-token" if str(provider) == "graph" else ""

    monkeypatch.setattr(ExternalConnectors, "_token", classmethod(_token_stub))

    result = ExternalConnectors.connector_preflight({"action": "external_email_list", "provider": "auto"})

    assert result["status"] == "success"
    assert bool(result.get("preflight_ready", False)) is True
    diagnostic = result.get("contract_diagnostic", {})
    assert isinstance(diagnostic, dict)
    assert str(diagnostic.get("selected_provider", "")) == "graph"


def test_connector_preflight_batch_aggregates_provider_summary_and_hints(monkeypatch) -> None:
    def _preflight_stub(_cls, payload):
        action = str(payload.get("action", "")).strip().lower()
        if action == "external_email_send":
            return {
                "status": "success",
                "preflight_ready": True,
                "message": "ok",
                "contract_diagnostic": {"selected_provider": "graph"},
                "remediation_hints": [],
            }
        return {
            "status": "error",
            "preflight_ready": False,
            "message": "auth failed",
            "contract_diagnostic": {"selected_provider": "gmail", "code": "auth_preflight_failed"},
            "remediation_hints": [{"id": "oauth_refresh", "summary": "refresh token"}],
        }

    monkeypatch.setattr(ExternalConnectors, "connector_preflight", classmethod(_preflight_stub))
    payload = ExternalConnectors.connector_preflight_batch(
        {
            "actions": [
                {"action": "external_email_send", "provider": "auto"},
                {"action": "external_email_list", "provider": "auto"},
            ],
            "strict": False,
        }
    )

    assert payload["status"] == "partial"
    assert int(payload.get("count", 0)) == 2
    assert int(payload.get("ready_count", 0)) == 1
    assert int(payload.get("blocked_count", 0)) == 1
    summary = payload.get("provider_summary", {})
    assert isinstance(summary, dict)
    assert "graph" in summary
    assert "gmail" in summary
    hints = payload.get("remediation_hints", [])
    assert isinstance(hints, list)
    assert any(str(item.get("id", "")) == "oauth_refresh" for item in hints if isinstance(item, dict))


def test_connector_preflight_batch_strict_mode_returns_error_when_blocked(monkeypatch) -> None:
    monkeypatch.setattr(
        ExternalConnectors,
        "connector_preflight",
        classmethod(
            lambda _cls, _payload: {
                "status": "error",
                "preflight_ready": False,
                "message": "blocked",
                "contract_diagnostic": {"selected_provider": "gmail", "code": "auth_preflight_failed"},
                "remediation_hints": [{"id": "oauth_refresh"}],
            }
        ),
    )
    payload = ExternalConnectors.connector_preflight_batch(
        {"actions": [{"action": "external_email_list"}], "strict": True}
    )

    assert payload["status"] == "error"
    assert int(payload.get("ready_count", 0)) == 0
    assert int(payload.get("blocked_count", 0)) == 1
