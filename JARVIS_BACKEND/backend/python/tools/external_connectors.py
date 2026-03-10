from __future__ import annotations

import base64
import os
import smtplib
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any, Dict, List, Tuple

from backend.python.core.oauth_token_store import OAuthTokenStore


class ExternalConnectors:
    """
    Cloud app connectors for email/calendar/docs across Google and Microsoft ecosystems.
    """

    _DRY_RUN_PROVIDER_ALIASES: Dict[str, Dict[str, str]] = {
        "external_email_send": {
            "auto": "auto",
            "gmail": "gmail",
            "google": "gmail",
            "graph": "graph",
            "microsoft": "graph",
            "microsoft_graph": "graph",
            "smtp": "smtp",
        },
        "external_calendar_create_event": {
            "auto": "auto",
            "google": "google",
            "gcal": "google",
            "graph": "graph",
            "microsoft": "graph",
            "microsoft_graph": "graph",
        },
        "external_calendar_update_event": {
            "auto": "auto",
            "google": "google",
            "gcal": "google",
            "graph": "graph",
            "microsoft": "graph",
            "microsoft_graph": "graph",
        },
        "external_doc_create": {
            "auto": "auto",
            "google": "google_docs",
            "google_docs": "google_docs",
            "gdocs": "google_docs",
            "graph": "graph_drive",
            "graph_drive": "graph_drive",
            "microsoft_graph": "graph_drive",
            "onedrive": "graph_drive",
        },
        "external_doc_update": {
            "auto": "auto",
            "google": "google_docs",
            "google_docs": "google_docs",
            "gdocs": "google_docs",
            "graph": "graph_drive",
            "graph_drive": "graph_drive",
            "microsoft_graph": "graph_drive",
            "onedrive": "graph_drive",
        },
        "external_task_create": {
            "auto": "auto",
            "google": "google_tasks",
            "google_tasks": "google_tasks",
            "gtasks": "google_tasks",
            "graph": "graph_todo",
            "microsoft": "graph_todo",
            "microsoft_graph": "graph_todo",
            "graph_todo": "graph_todo",
            "todo": "graph_todo",
            "microsoft_todo": "graph_todo",
        },
        "external_task_update": {
            "auto": "auto",
            "google": "google_tasks",
            "google_tasks": "google_tasks",
            "gtasks": "google_tasks",
            "graph": "graph_todo",
            "microsoft": "graph_todo",
            "microsoft_graph": "graph_todo",
            "graph_todo": "graph_todo",
            "todo": "graph_todo",
            "microsoft_todo": "graph_todo",
        },
    }
    _SUPPORTED_CONNECTOR_ACTIONS = {
        "external_email_send",
        "external_email_list",
        "external_email_read",
        "external_calendar_create_event",
        "external_calendar_list_events",
        "external_calendar_update_event",
        "external_doc_create",
        "external_doc_list",
        "external_doc_read",
        "external_doc_update",
        "external_task_list",
        "external_task_create",
        "external_task_update",
    }
    _CONTRACT_REQUIRED_FIELDS: Dict[str, List[str]] = {
        "external_email_send": ["to"],
        "external_email_read": ["message_id"],
        "external_calendar_create_event": ["title"],
        "external_calendar_update_event": ["event_id"],
        "external_doc_create": ["title"],
        "external_doc_read": ["document_id"],
        "external_doc_update": ["document_id"],
        "external_task_create": ["title"],
        "external_task_update": ["task_id"],
    }
    _CONTRACT_ANY_OF_FIELDS: Dict[str, List[List[str]]] = {
        "external_email_send": [["subject", "body"]],
        "external_calendar_update_event": [["title", "description", "start", "end", "attendees"]],
        "external_doc_update": [["title", "content"]],
        "external_task_update": [["title", "notes", "due", "status"]],
    }

    @staticmethod
    def connector_status() -> Dict[str, Any]:
        token_store = OAuthTokenStore.shared()
        google_store = token_store.list(provider="google", limit=1)
        graph_store = token_store.list(provider="graph", limit=1)
        ms_graph_store = token_store.list(provider="microsoft_graph", limit=1)
        google_token = bool(os.getenv("GOOGLE_OAUTH_ACCESS_TOKEN") or os.getenv("GOOGLE_ACCESS_TOKEN") or int(google_store.get("count", 0)) > 0)
        graph_token = bool(
            os.getenv("MICROSOFT_GRAPH_ACCESS_TOKEN")
            or int(graph_store.get("count", 0)) > 0
            or int(ms_graph_store.get("count", 0)) > 0
        )
        smtp_host = bool(os.getenv("SMTP_HOST"))
        return {
            "status": "success",
            "providers": {
                "gmail": {"available": google_token},
                "google_calendar": {"available": google_token},
                "google_docs": {"available": google_token},
                "google_tasks": {"available": google_token},
                "microsoft_graph_mail": {"available": graph_token},
                "microsoft_graph_calendar": {"available": graph_token},
                "microsoft_graph_drive": {"available": graph_token},
                "microsoft_graph_todo": {"available": graph_token},
                "smtp": {"available": smtp_host},
            },
        }

    @classmethod
    def connector_preflight(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        clean_action = str(payload.get("action", "")).strip().lower()
        if not clean_action:
            return {"status": "error", "message": "action is required for connector preflight"}
        if clean_action not in cls._SUPPORTED_CONNECTOR_ACTIONS:
            return {
                "status": "error",
                "message": f"Unsupported connector action for preflight: {clean_action}",
                "contract_diagnostic": {
                    "status": "error",
                    "code": "unsupported_connector_action",
                    "severity": "error",
                    "action": clean_action,
                    "contract_stage": "contract_validation",
                    "missing_fields": [],
                    "any_of": [],
                    "allowed_providers": [],
                    "auth_blocked_providers": [],
                    "checks": [
                        {
                            "id": "supported_action",
                            "status": "failed",
                            "severity": "error",
                            "message": f"Unsupported connector action: {clean_action}",
                        }
                    ],
                    "remediation_hints": [
                        {
                            "id": "use_supported_connector_action",
                            "priority": 1,
                            "confidence": 0.95,
                            "summary": "Use a supported external connector action before running preflight.",
                            "action": clean_action,
                        }
                    ],
                },
            }

        diagnostics = cls._connector_preflight_diagnostics(clean_action=clean_action, payload=payload)
        status = "success" if bool(diagnostics.get("preflight_ready", False)) else "error"
        message = "Connector preflight passed."
        if status != "success":
            code = str(diagnostics.get("code", "contract_failed")).strip().lower() or "contract_failed"
            message = f"Connector preflight failed with '{code}'."
        payload_row = {
            "status": status,
            "message": message,
            "action": clean_action,
            "contract_diagnostic": diagnostics,
            "remediation_hints": diagnostics.get("remediation_hints", []),
            "remediation_contract": diagnostics.get("remediation_contract", {}),
            "remediation_plan": diagnostics.get("remediation_plan", []),
            "provider_routing": diagnostics.get("provider_routing", {}),
            "provider_diagnostics": diagnostics.get("provider_diagnostics", []),
            "preflight_ready": bool(diagnostics.get("preflight_ready", False)),
        }
        return payload_row

    @classmethod
    def connector_preflight_batch(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        actions_raw = payload.get("actions", [])
        if not isinstance(actions_raw, list) or not actions_raw:
            return {"status": "error", "message": "actions is required", "count": 0, "items": []}

        strict = bool(payload.get("strict", False))
        max_actions = cls._to_int(payload.get("max_actions", 30), default=30, minimum=1, maximum=200)
        default_provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        default_metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

        items: List[Dict[str, Any]] = []
        provider_summary: Dict[str, Dict[str, int]] = {}
        remediation_hints: List[Dict[str, Any]] = []
        seen_hint_ids: set[str] = set()

        for index, row in enumerate(actions_raw[:max_actions], start=1):
            if not isinstance(row, dict):
                item = {
                    "index": index,
                    "status": "error",
                    "message": "action item must be an object",
                    "preflight_ready": False,
                }
                items.append(item)
                continue
            action_name = str(row.get("action", "")).strip().lower()
            args_payload = row.get("args", {})
            provider_name = str(row.get("provider", default_provider)).strip().lower() or default_provider
            req_payload: Dict[str, Any] = {"action": action_name, "provider": provider_name}
            if isinstance(args_payload, dict):
                req_payload.update(args_payload)
            metadata = row.get("metadata")
            if isinstance(default_metadata, dict) and default_metadata:
                req_payload["metadata"] = dict(default_metadata)
                if isinstance(metadata, dict):
                    req_payload["metadata"].update(metadata)
            elif isinstance(metadata, dict):
                req_payload["metadata"] = dict(metadata)
            result = cls.connector_preflight(req_payload)
            item = {
                "index": index,
                "action": action_name,
                "provider": provider_name,
                "status": str(result.get("status", "")).strip().lower() or "error",
                "preflight_ready": bool(result.get("preflight_ready", False)),
                "message": str(result.get("message", "")).strip(),
                "contract_diagnostic": result.get("contract_diagnostic", {}),
            }
            items.append(item)

            diagnostic = result.get("contract_diagnostic", {}) if isinstance(result.get("contract_diagnostic", {}), dict) else {}
            selected_provider = str(diagnostic.get("selected_provider", provider_name)).strip().lower() or provider_name
            summary_row = provider_summary.setdefault(selected_provider, {"passed": 0, "blocked": 0, "errors": 0})
            if bool(item["preflight_ready"]):
                summary_row["passed"] += 1
            elif item["status"] == "error":
                summary_row["errors"] += 1
                summary_row["blocked"] += 1
            else:
                summary_row["blocked"] += 1

            hint_rows = result.get("remediation_hints", [])
            if isinstance(hint_rows, list):
                for hint in hint_rows[:8]:
                    if not isinstance(hint, dict):
                        continue
                    hint_id = str(hint.get("id", "")).strip().lower()
                    if not hint_id or hint_id in seen_hint_ids:
                        continue
                    seen_hint_ids.add(hint_id)
                    remediation_hints.append(dict(hint))

        ready_count = sum(1 for item in items if bool(item.get("preflight_ready", False)))
        blocked_count = len(items) - ready_count
        status = "success"
        if blocked_count > 0:
            status = "error" if strict else "partial"
        return {
            "status": status,
            "strict": strict,
            "count": len(items),
            "ready_count": ready_count,
            "blocked_count": blocked_count,
            "provider_summary": provider_summary,
            "remediation_hints": remediation_hints[:16],
            "items": items,
        }

    @classmethod
    def send_email(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        to_list = cls._normalize_recipients(payload.get("to"))
        if not to_list:
            return {"status": "error", "message": "to is required"}
        subject = str(payload.get("subject", "")).strip()
        body = str(payload.get("body", "")).strip()
        if not subject and not body:
            return {"status": "error", "message": "subject or body is required"}

        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if cls._is_dry_run(payload):
            auto_candidates = cls._auto_email_candidates(payload, include_smtp=True) if provider == "auto" else [provider]
            return cls._dry_run_mutation_response(
                action="external_email_send",
                payload=payload,
                provider=provider,
                candidate_providers=auto_candidates,
                expected_mutation={
                    "recipient_count": len(to_list),
                    "has_subject": bool(subject),
                    "body_chars": len(body),
                },
                reversible=False,
                high_impact=True,
            )
        if provider == "auto":
            candidates = cls._auto_email_candidates(payload, include_smtp=True)
            if not candidates:
                return {"status": "error", "message": "No email provider credentials available."}
            return cls._run_provider_chain(
                action="external_email_send",
                capability="email_send",
                providers=candidates,
                payload=payload,
                invoker=lambda candidate: cls._send_email_via_provider(
                    candidate,
                    payload=payload,
                    to_list=to_list,
                    subject=subject,
                    body=body,
                ),
            )

        if provider == "gmail":
            return cls._send_gmail(payload, to_list=to_list, subject=subject, body=body)
        if provider in {"graph", "microsoft", "microsoft_graph"}:
            return cls._send_graph_mail(payload, to_list=to_list, subject=subject, body=body)
        if provider == "smtp":
            return cls._send_smtp(payload, to_list=to_list, subject=subject, body=body)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def create_calendar_event(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("title") or payload.get("summary") or "").strip()
        if not title:
            return {"status": "error", "message": "title is required"}
        start_iso = str(payload.get("start") or "").strip()
        end_iso = str(payload.get("end") or "").strip()
        timezone_name = str(payload.get("timezone", "UTC")).strip() or "UTC"

        start_dt, end_dt = cls._resolve_event_window(start_iso=start_iso, end_iso=end_iso)
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if cls._is_dry_run(payload):
            auto_candidates = cls._auto_calendar_candidates(payload) if provider == "auto" else [provider]
            return cls._dry_run_mutation_response(
                action="external_calendar_create_event",
                payload=payload,
                provider=provider,
                candidate_providers=auto_candidates,
                expected_mutation={
                    "title": title,
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat(),
                    "timezone": timezone_name,
                },
                reversible=False,
                high_impact=True,
            )
        if provider == "auto":
            candidates = cls._auto_calendar_candidates(payload)
            if not candidates:
                return {"status": "error", "message": "No calendar provider credentials available."}
            return cls._run_provider_chain(
                action="external_calendar_create_event",
                capability="calendar_create_event",
                providers=candidates,
                payload=payload,
                invoker=lambda candidate: cls._create_calendar_via_provider(
                    candidate,
                    payload=payload,
                    title=title,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    timezone_name=timezone_name,
                ),
            )

        if provider in {"google", "gcal"}:
            return cls._create_google_event(
                payload,
                title=title,
                start_dt=start_dt,
                end_dt=end_dt,
                timezone_name=timezone_name,
            )
        if provider in {"graph", "microsoft", "microsoft_graph"}:
            return cls._create_graph_event(
                payload,
                title=title,
                start_dt=start_dt,
                end_dt=end_dt,
                timezone_name=timezone_name,
            )
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def create_document(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("title", "")).strip() or "JARVIS Document"
        content = str(payload.get("content", "")).strip()
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if cls._is_dry_run(payload):
            auto_candidates = cls._auto_document_candidates(payload) if provider == "auto" else [provider]
            return cls._dry_run_mutation_response(
                action="external_doc_create",
                payload=payload,
                provider=provider,
                candidate_providers=auto_candidates,
                expected_mutation={
                    "title": title,
                    "content_chars": len(content),
                },
                reversible=False,
                high_impact=True,
            )

        if provider == "auto":
            candidates = cls._auto_document_candidates(payload)
            if not candidates:
                return {"status": "error", "message": "No document provider credentials available."}
            return cls._run_provider_chain(
                action="external_doc_create",
                capability="document_create",
                providers=candidates,
                payload=payload,
                invoker=lambda candidate: cls._create_document_via_provider(
                    candidate,
                    payload=payload,
                    title=title,
                    content=content,
                ),
            )

        if provider in {"google", "google_docs", "gdocs"}:
            return cls._create_google_doc(payload, title=title, content=content)
        if provider in {"graph", "graph_drive", "microsoft_graph", "onedrive"}:
            return cls._create_graph_doc(payload, title=title, content=content)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def list_emails(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        max_results = cls._to_int(payload.get("max_results", 20), default=20, minimum=1, maximum=100)
        query = str(payload.get("query", "")).strip()
        if provider == "auto":
            candidates = cls._auto_email_candidates(payload, include_smtp=False)
            if not candidates:
                return {"status": "error", "message": "No email provider credentials available."}
            return cls._run_provider_chain(
                action="external_email_list",
                capability="email_list",
                providers=candidates,
                payload=payload,
                invoker=lambda candidate: cls._list_email_via_provider(
                    candidate,
                    payload=payload,
                    max_results=max_results,
                    query=query,
                ),
            )
        provider = cls._resolve_email_provider(payload)
        if provider == "error":
            return {"status": "error", "message": "No email provider credentials available."}
        if provider == "gmail":
            return cls._list_gmail_messages(payload, max_results=max_results, query=query)
        if provider == "graph":
            return cls._list_graph_messages(payload, max_results=max_results, query=query)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def read_email(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        message_id = str(payload.get("message_id") or payload.get("id") or "").strip()
        if not message_id:
            return {"status": "error", "message": "message_id is required"}
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if provider == "auto":
            candidates = cls._auto_email_candidates(payload, include_smtp=False)
            if not candidates:
                return {"status": "error", "message": "No email provider credentials available."}
            return cls._run_provider_chain(
                action="external_email_read",
                capability="email_read",
                providers=candidates,
                payload=payload,
                invoker=lambda candidate: cls._read_email_via_provider(
                    candidate,
                    payload=payload,
                    message_id=message_id,
                ),
            )
        provider = cls._resolve_email_provider(payload)
        if provider == "error":
            return {"status": "error", "message": "No email provider credentials available."}
        if provider == "gmail":
            return cls._read_gmail_message(payload, message_id=message_id)
        if provider == "graph":
            return cls._read_graph_message(payload, message_id=message_id)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def list_calendar_events(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        max_results = cls._to_int(payload.get("max_results", 20), default=20, minimum=1, maximum=100)
        if provider == "auto":
            candidates = cls._auto_calendar_candidates(payload)
            if not candidates:
                return {"status": "error", "message": "No calendar provider credentials available."}
            return cls._run_provider_chain(
                action="external_calendar_list_events",
                capability="calendar_list",
                providers=candidates,
                payload=payload,
                invoker=lambda candidate: cls._list_calendar_via_provider(
                    candidate,
                    payload=payload,
                    max_results=max_results,
                ),
            )
        provider = cls._resolve_calendar_provider(payload)
        if provider == "error":
            return {"status": "error", "message": "No calendar provider credentials available."}
        if provider == "google":
            return cls._list_google_events(payload, max_results=max_results)
        if provider == "graph":
            return cls._list_graph_events(payload, max_results=max_results)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def update_calendar_event(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        event_id = str(payload.get("event_id") or payload.get("id") or "").strip()
        if not event_id:
            return {"status": "error", "message": "event_id is required"}
        title = str(payload.get("title") or payload.get("summary") or "").strip()
        description = str(payload.get("description", "")).strip()
        start_iso = str(payload.get("start", "")).strip()
        end_iso = str(payload.get("end", "")).strip()
        timezone_name = str(payload.get("timezone", "UTC")).strip() or "UTC"
        attendees = cls._normalize_recipients(payload.get("attendees"))
        if not any((title, description, start_iso, end_iso, attendees)):
            return {
                "status": "error",
                "message": "At least one mutable field is required (title/description/start/end/attendees).",
            }

        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if cls._is_dry_run(payload):
            auto_candidates = cls._auto_calendar_candidates(payload) if provider == "auto" else [provider]
            return cls._dry_run_mutation_response(
                action="external_calendar_update_event",
                payload=payload,
                provider=provider,
                candidate_providers=auto_candidates,
                expected_mutation={
                    "event_id": event_id,
                    "fields": [field for field, value in {"title": title, "description": description, "start": start_iso, "end": end_iso, "attendees": attendees}.items() if value],
                },
                reversible=False,
                high_impact=True,
            )

        provider = cls._resolve_calendar_provider(payload)
        if provider == "error":
            return {"status": "error", "message": "No calendar provider credentials available."}
        if provider == "google":
            return cls._update_google_event(
                payload,
                event_id=event_id,
                title=title,
                description=description,
                start_iso=start_iso,
                end_iso=end_iso,
                timezone_name=timezone_name,
                attendees=attendees,
            )
        if provider == "graph":
            return cls._update_graph_event(
                payload,
                event_id=event_id,
                title=title,
                description=description,
                start_iso=start_iso,
                end_iso=end_iso,
                timezone_name=timezone_name,
                attendees=attendees,
            )
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def list_documents(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        max_results = cls._to_int(payload.get("max_results", 20), default=20, minimum=1, maximum=100)
        query = str(payload.get("query", "")).strip()
        if provider == "auto":
            candidates = cls._auto_document_candidates(payload)
            if not candidates:
                return {"status": "error", "message": "No document provider credentials available."}
            return cls._run_provider_chain(
                action="external_doc_list",
                capability="document_list",
                providers=candidates,
                payload=payload,
                invoker=lambda candidate: cls._list_document_via_provider(
                    candidate,
                    payload=payload,
                    max_results=max_results,
                    query=query,
                ),
            )
        provider = cls._resolve_document_provider(payload)
        if provider == "error":
            return {"status": "error", "message": "No document provider credentials available."}
        if provider == "google_docs":
            return cls._list_google_docs(payload, max_results=max_results, query=query)
        if provider == "graph_drive":
            return cls._list_graph_docs(payload, max_results=max_results, query=query)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def read_document(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        document_id = str(payload.get("document_id") or payload.get("id") or "").strip()
        if not document_id:
            return {"status": "error", "message": "document_id is required"}
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if provider == "auto":
            candidates = cls._auto_document_candidates(payload)
            if not candidates:
                return {"status": "error", "message": "No document provider credentials available."}
            return cls._run_provider_chain(
                action="external_doc_read",
                capability="document_read",
                providers=candidates,
                payload=payload,
                invoker=lambda candidate: cls._read_document_via_provider(
                    candidate,
                    payload=payload,
                    document_id=document_id,
                ),
            )
        provider = cls._resolve_document_provider(payload)
        if provider == "error":
            return {"status": "error", "message": "No document provider credentials available."}
        if provider == "google_docs":
            return cls._read_google_doc(payload, document_id=document_id)
        if provider == "graph_drive":
            return cls._read_graph_doc(payload, document_id=document_id)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def update_document(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        document_id = str(payload.get("document_id") or payload.get("id") or "").strip()
        if not document_id:
            return {"status": "error", "message": "document_id is required"}
        title = str(payload.get("title", "")).strip()
        content = str(payload.get("content", "")).strip()
        if not title and not content:
            return {"status": "error", "message": "title or content is required"}
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if cls._is_dry_run(payload):
            auto_candidates = cls._auto_document_candidates(payload) if provider == "auto" else [provider]
            return cls._dry_run_mutation_response(
                action="external_doc_update",
                payload=payload,
                provider=provider,
                candidate_providers=auto_candidates,
                expected_mutation={
                    "document_id": document_id,
                    "fields": [field for field, value in {"title": title, "content": content}.items() if value],
                    "content_chars": len(content),
                },
                reversible=False,
                high_impact=True,
            )
        provider = cls._resolve_document_provider(payload)
        if provider == "error":
            return {"status": "error", "message": "No document provider credentials available."}
        if provider == "google_docs":
            return cls._update_google_doc(payload, document_id=document_id, title=title, content=content)
        if provider == "graph_drive":
            return cls._update_graph_doc(payload, document_id=document_id, title=title, content=content)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def list_tasks(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        max_results = cls._to_int(payload.get("max_results", 25), default=25, minimum=1, maximum=200)
        include_completed = bool(payload.get("include_completed", True))
        query = str(payload.get("query", "")).strip()
        if provider == "auto":
            candidates = cls._auto_task_candidates(payload)
            if not candidates:
                return {"status": "error", "message": "No task provider credentials available."}
            return cls._run_provider_chain(
                action="external_task_list",
                capability="task_list",
                providers=candidates,
                payload=payload,
                invoker=lambda candidate: cls._list_task_via_provider(
                    candidate,
                    payload=payload,
                    max_results=max_results,
                    include_completed=include_completed,
                    query=query,
                ),
            )
        provider = cls._resolve_task_provider(payload)
        if provider == "error":
            return {"status": "error", "message": "No task provider credentials available."}
        if provider == "google_tasks":
            return cls._list_google_tasks(payload, max_results=max_results, include_completed=include_completed, query=query)
        if provider == "graph_todo":
            return cls._list_graph_todo_tasks(payload, max_results=max_results, include_completed=include_completed, query=query)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def create_task(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        title = str(payload.get("title") or payload.get("task") or payload.get("name") or "").strip()
        if not title:
            return {"status": "error", "message": "title is required"}
        notes = str(payload.get("notes") or payload.get("body") or payload.get("content") or "").strip()
        due = str(payload.get("due") or payload.get("due_at") or "").strip()
        status = str(payload.get("status") or "").strip()
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if cls._is_dry_run(payload):
            auto_candidates = cls._auto_task_candidates(payload) if provider == "auto" else [provider]
            return cls._dry_run_mutation_response(
                action="external_task_create",
                payload=payload,
                provider=provider,
                candidate_providers=auto_candidates,
                expected_mutation={
                    "title": title,
                    "due": due,
                    "has_notes": bool(notes),
                    "status": status,
                },
                reversible=False,
                high_impact=True,
            )
        provider = cls._resolve_task_provider(payload)
        if provider == "error":
            return {"status": "error", "message": "No task provider credentials available."}
        if provider == "google_tasks":
            return cls._create_google_task(payload, title=title, notes=notes, due=due, status=status)
        if provider == "graph_todo":
            return cls._create_graph_todo_task(payload, title=title, notes=notes, due=due, status=status)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def update_task(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        task_id = str(payload.get("task_id") or payload.get("id") or "").strip()
        if not task_id:
            return {"status": "error", "message": "task_id is required"}
        title = str(payload.get("title") or payload.get("task") or payload.get("name") or "").strip()
        notes = str(payload.get("notes") or payload.get("body") or payload.get("content") or "").strip()
        due = str(payload.get("due") or payload.get("due_at") or "").strip()
        status = str(payload.get("status") or "").strip()
        if not any((title, notes, due, status)):
            return {
                "status": "error",
                "message": "At least one mutable field is required (title/notes/due/status).",
            }

        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if cls._is_dry_run(payload):
            auto_candidates = cls._auto_task_candidates(payload) if provider == "auto" else [provider]
            return cls._dry_run_mutation_response(
                action="external_task_update",
                payload=payload,
                provider=provider,
                candidate_providers=auto_candidates,
                expected_mutation={
                    "task_id": task_id,
                    "fields": [field for field, value in {"title": title, "notes": notes, "due": due, "status": status}.items() if value],
                },
                reversible=False,
                high_impact=True,
            )

        provider = cls._resolve_task_provider(payload)
        if provider == "error":
            return {"status": "error", "message": "No task provider credentials available."}
        if provider == "google_tasks":
            return cls._update_google_task(payload, task_id=task_id, title=title, notes=notes, due=due, status=status)
        if provider == "graph_todo":
            return cls._update_graph_todo_task(payload, task_id=task_id, title=title, notes=notes, due=due, status=status)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def _run_provider_chain(
        cls,
        *,
        action: str,
        capability: str,
        providers: List[str],
        payload: Dict[str, Any] | None = None,
        invoker: Any,
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        clean_capability = str(capability or "").strip().lower() or "external"
        payload_row = payload if isinstance(payload, dict) else {}
        attempts: List[Dict[str, Any]] = []
        provider_diagnostics: List[Dict[str, Any]] = []
        retry_override = cls._to_int(payload_row.get("provider_retry_attempts", 0), default=0, minimum=0, maximum=2)
        for provider in providers:
            preflight = cls._provider_preflight_diagnostic(provider=provider, payload=payload_row)
            if isinstance(preflight, dict):
                provider_diagnostics.append(dict(preflight))
            preflight_available = bool(preflight.get("available", False)) if isinstance(preflight, dict) else True
            preflight_hints = preflight.get("remediation_hints", []) if isinstance(preflight, dict) else []
            preflight_message = str(preflight_hints[0])[:400] if isinstance(preflight_hints, list) and preflight_hints else ""
            provider_attempt = 0
            try:
                while True:
                    provider_attempt += 1
                    try:
                        result = invoker(provider)
                    except Exception as exc:  # noqa: BLE001
                        result = {"status": "error", "message": str(exc)}
                    if not isinstance(result, dict):
                        result = {"status": "error", "message": "provider returned non-dict response"}
                    status = str(result.get("status", "")).strip().lower() or "error"
                    if status == "success":
                        resilience = result.setdefault("resilience", {})
                        if isinstance(resilience, dict):
                            if attempts:
                                resilience["provider_fallback"] = {
                                    "capability": clean_capability,
                                    "selected_provider": provider,
                                    "attempt_count": len(attempts) + 1,
                                    "failed_attempts": attempts[:6],
                                }
                            resilience["provider_chain"] = {
                                "action": clean_action,
                                "capability": clean_capability,
                                "selected_provider": provider,
                                "provider_attempt": provider_attempt,
                                "diagnostics": provider_diagnostics[:8],
                            }
                        return result
                    message = str(result.get("message", ""))[:400]
                    failure = cls._classify_provider_chain_failure(
                        status=status,
                        message=message,
                        result=result,
                    )
                    category = str(failure.get("category", "unknown")).strip().lower() or "unknown"
                    retryable = bool(failure.get("retryable", False))
                    if not preflight_available and provider_attempt == 1:
                        status = "blocked_preflight"
                        category = "auth_preflight_failed"
                        retryable = False
                        if not message:
                            message = preflight_message or "provider preflight unavailable"
                    retry_budget = max(
                        1,
                        min(
                            4,
                            cls._provider_retry_budget(capability=clean_capability, category=category) + retry_override,
                        ),
                    )
                    attempts.append(
                        {
                            "provider": provider,
                            "status": status,
                            "message": message,
                            "category": category,
                            "retryable": retryable,
                            "provider_attempt": provider_attempt,
                            "retry_budget": retry_budget,
                        }
                    )
                    if not retryable or provider_attempt >= retry_budget:
                        break
            except Exception as exc:  # noqa: BLE001
                attempts.append({"provider": provider, "status": "error", "message": str(exc), "category": "unknown", "retryable": False})

        message = f"No {clean_capability} provider succeeded."
        if attempts:
            message = f"{message} Last error: {attempts[-1].get('message', '')}"
        remediation_hints = cls._build_provider_chain_remediation_hints(
            action=clean_action,
            capability=clean_capability,
            diagnostics=provider_diagnostics,
            attempts=attempts,
        )
        remediation_contract = cls._build_provider_chain_remediation_contract(
            action=clean_action,
            capability=clean_capability,
            hints=remediation_hints,
        )
        return {
            "status": "error",
            "message": message.strip(),
            "remediation_hints": remediation_hints[:8],
            "remediation_contract": remediation_contract,
            "resilience": {
                "provider_fallback": {
                    "capability": clean_capability,
                    "selected_provider": "",
                    "attempt_count": len(attempts),
                    "failed_attempts": attempts[:8],
                },
                "provider_chain": {
                    "action": clean_action,
                    "capability": clean_capability,
                    "diagnostics": provider_diagnostics[:8],
                },
            },
        }

    @classmethod
    def _classify_provider_chain_failure(
        cls,
        *,
        status: str,
        message: str,
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        clean_status = str(status or "").strip().lower()
        text = str(message or "").strip().lower()
        status_code = cls._to_int(result.get("status_code", 0), default=0, minimum=0, maximum=999)

        if status_code == 429 or any(token in text for token in ("rate limit", "too many requests", "quota exceeded", "throttled")):
            return {"category": "rate_limited", "retryable": True}
        if status_code in {500, 502, 503, 504} or any(
            token in text
            for token in (
                "timed out",
                "timeout",
                "temporarily unavailable",
                "service unavailable",
                "connection reset",
                "network error",
                "gateway",
            )
        ):
            return {"category": "transient", "retryable": True}
        if status_code in {401, 403} or any(
            token in text
            for token in (
                "unauthorized",
                "forbidden",
                "invalid_grant",
                "invalid token",
                "token expired",
                "scope",
                "credentials",
                "authentication",
            )
        ):
            retryable = bool(any(token in text for token in ("token expired", "invalid token", "refresh", "temporary")))
            return {"category": "auth", "retryable": retryable}
        if status_code == 404 or any(token in text for token in ("required", "invalid", "unsupported provider", "malformed", "not found")):
            return {"category": "contract", "retryable": False}
        if clean_status in {"blocked_preflight"}:
            return {"category": "preflight", "retryable": False}
        return {"category": "unknown", "retryable": False}

    @staticmethod
    def _provider_retry_budget(*, capability: str, category: str) -> int:
        clean_capability = str(capability or "").strip().lower()
        clean_category = str(category or "").strip().lower()
        if clean_category == "rate_limited":
            return 2 if clean_capability in {"email_send", "calendar_create_event", "document_create", "task_list"} else 1
        if clean_category == "transient":
            return 2
        if clean_category == "auth":
            return 1
        return 1

    @classmethod
    def _build_provider_chain_remediation_hints(
        cls,
        *,
        action: str,
        capability: str,
        diagnostics: List[Dict[str, Any]],
        attempts: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        clean_action = str(action or "").strip().lower()
        clean_capability = str(capability or "").strip().lower()
        hints: List[Dict[str, Any]] = []
        unique_text_hints: List[str] = []
        seen_hint_text: set[str] = set()
        for diag in diagnostics:
            if not isinstance(diag, dict):
                continue
            rows = diag.get("remediation_hints", [])
            if not isinstance(rows, list):
                continue
            for row in rows:
                text = str(row).strip()
                if not text or text in seen_hint_text:
                    continue
                seen_hint_text.add(text)
                unique_text_hints.append(text)
        if unique_text_hints:
            hints.append(
                {
                    "id": "provider_preflight_repair",
                    "priority": 1,
                    "confidence": 0.86,
                    "summary": "Repair connector preflight prerequisites before retrying provider chain.",
                    "action": clean_action,
                    "capability": clean_capability,
                    "instructions": unique_text_hints[:6],
                }
            )
        category_counts: Dict[str, int] = {}
        for row in attempts:
            if not isinstance(row, dict):
                continue
            category = str(row.get("category", "")).strip().lower() or "unknown"
            category_counts[category] = int(category_counts.get(category, 0)) + 1
        if int(category_counts.get("auth", 0)) > 0:
            hints.append(
                {
                    "id": "auth_maintenance_cycle",
                    "priority": 1,
                    "confidence": 0.9,
                    "summary": "Run OAuth maintenance/refresh before retrying external connector action.",
                    "tool_action": {"action": "oauth_token_maintain", "args": {"provider": "auto", "limit": 40, "window_s": 7200}},
                    "action": clean_action,
                    "capability": clean_capability,
                }
            )
        if int(category_counts.get("rate_limited", 0)) > 0:
            hints.append(
                {
                    "id": "rate_limit_backoff",
                    "priority": 2,
                    "confidence": 0.82,
                    "summary": "Apply provider cooldown/backoff and allow alternate provider routing.",
                    "args_patch": {"provider": "auto"},
                    "action": clean_action,
                    "capability": clean_capability,
                }
            )
        if int(category_counts.get("transient", 0)) > 0:
            hints.append(
                {
                    "id": "connector_health_probe",
                    "priority": 2,
                    "confidence": 0.78,
                    "summary": "Run connector status probe to select healthiest provider before retry.",
                    "tool_action": {"action": "external_connector_status", "args": {}},
                    "action": clean_action,
                    "capability": clean_capability,
                }
            )
        dedup: Dict[str, Dict[str, Any]] = {}
        for hint in hints:
            if not isinstance(hint, dict):
                continue
            hint_id = str(hint.get("id", "")).strip().lower()
            if not hint_id:
                continue
            existing = dedup.get(hint_id)
            if existing is None:
                dedup[hint_id] = hint
                continue
            old_conf = float(existing.get("confidence", 0.0) or 0.0)
            new_conf = float(hint.get("confidence", 0.0) or 0.0)
            if new_conf > old_conf:
                dedup[hint_id] = hint
        rows = list(dedup.values())
        rows.sort(
            key=lambda row: (
                int(row.get("priority", 999) or 999),
                -float(row.get("confidence", 0.0) or 0.0),
                str(row.get("id", "")),
            )
        )
        return rows[:10]

    @classmethod
    def _build_provider_chain_remediation_contract(
        cls,
        *,
        action: str,
        capability: str,
        hints: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        rows = [row for row in hints if isinstance(row, dict)]
        strategies: List[Dict[str, Any]] = []
        for row in rows[:8]:
            if isinstance(row.get("tool_action"), dict):
                strategies.append(
                    {
                        "type": "tool_action",
                        "id": str(row.get("id", "")).strip().lower(),
                        "tool_action": dict(row.get("tool_action", {})),
                        "confidence": float(row.get("confidence", 0.0) or 0.0),
                    }
                )
            if isinstance(row.get("args_patch"), dict):
                strategies.append(
                    {
                        "type": "args_patch",
                        "id": str(row.get("id", "")).strip().lower(),
                        "args_patch": dict(row.get("args_patch", {})),
                        "confidence": float(row.get("confidence", 0.0) or 0.0),
                    }
                )
        return {
            "version": "1.0",
            "action": str(action or "").strip().lower(),
            "capability": str(capability or "").strip().lower(),
            "strategy_count": len(strategies),
            "strategies": strategies[:10],
        }

    @classmethod
    def _connector_preflight_diagnostics(cls, *, clean_action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        requested_provider = cls._canonical_provider_for_action(
            action=clean_action,
            provider=str(payload.get("provider", "auto")).strip().lower() or "auto",
        )
        candidates = cls._preflight_provider_candidates(action=clean_action, payload=payload, requested_provider=requested_provider)
        checks: List[Dict[str, Any]] = []
        provider_diagnostics: List[Dict[str, Any]] = []
        allowed_providers: List[str] = []
        auth_blocked: List[str] = []
        remediation_text_hints: List[str] = []
        for provider in candidates:
            diagnostic = cls._provider_preflight_diagnostic(provider=provider, payload=payload)
            provider_diagnostics.append(dict(diagnostic) if isinstance(diagnostic, dict) else {"provider": provider, "available": False})
            available = bool(diagnostic.get("available", False)) if isinstance(diagnostic, dict) else False
            if available:
                allowed_providers.append(provider)
                checks.append(
                    {
                        "id": f"provider_{provider}",
                        "status": "passed",
                        "severity": "info",
                        "message": f"Provider '{provider}' is available.",
                    }
                )
            else:
                auth_blocked.append(provider)
                checks.append(
                    {
                        "id": f"provider_{provider}",
                        "status": "failed",
                        "severity": "error",
                        "message": f"Provider '{provider}' preflight unavailable.",
                    }
                )
            hints = diagnostic.get("remediation_hints", []) if isinstance(diagnostic, dict) else []
            if isinstance(hints, list):
                for row in hints:
                    text = str(row).strip()
                    if text and text not in remediation_text_hints:
                        remediation_text_hints.append(text)

        missing_fields = cls._missing_required_fields(action=clean_action, payload=payload)
        if missing_fields:
            checks.append(
                {
                    "id": "required_fields",
                    "status": "failed",
                    "severity": "error",
                    "message": f"Missing required fields: {', '.join(missing_fields)}",
                    "missing_fields": missing_fields,
                }
            )
        any_of = cls._missing_any_of_fields(action=clean_action, payload=payload)
        if any_of:
            checks.append(
                {
                    "id": "any_of_fields",
                    "status": "failed",
                    "severity": "error",
                    "message": "At least one field from each required group must be present.",
                    "any_of": any_of,
                }
            )
        if requested_provider != "auto":
            if requested_provider not in candidates:
                checks.append(
                    {
                        "id": "requested_provider_supported",
                        "status": "failed",
                        "severity": "error",
                        "message": f"Provider '{requested_provider}' is not supported for action '{clean_action}'.",
                    }
                )
            elif requested_provider in auth_blocked:
                checks.append(
                    {
                        "id": "requested_provider_auth",
                        "status": "failed",
                        "severity": "error",
                        "message": f"Provider '{requested_provider}' is selected but preflight credentials are unavailable.",
                    }
                )

        code = "ready"
        severity = "info"
        contract_stage = "ready"
        if requested_provider != "auto" and requested_provider not in candidates:
            code = "provider_not_supported_for_action"
            severity = "error"
            contract_stage = "contract_validation"
        elif missing_fields:
            code = "missing_required_fields"
            severity = "error"
            contract_stage = "contract_validation"
        elif any_of:
            code = "missing_any_of_fields"
            severity = "error"
            contract_stage = "contract_validation"
        elif not allowed_providers:
            code = "auth_preflight_failed" if auth_blocked else "no_provider_candidates_after_contract"
            severity = "error"
            contract_stage = "provider_preflight"

        capability = str(cls._external_capability_for_action(clean_action))
        provider_routing = {
            "action": clean_action,
            "requested_provider": requested_provider,
            "candidate_providers": candidates,
            "allowed_providers": allowed_providers,
            "auth_blocked_providers": auth_blocked,
            "selected_provider": allowed_providers[0] if allowed_providers else "",
        }
        remediation_hints = cls._build_connector_preflight_remediation_hints(
            action=clean_action,
            capability=capability,
            code=code,
            missing_fields=missing_fields,
            any_of=any_of,
            requested_provider=requested_provider,
            allowed_providers=allowed_providers,
            auth_blocked=auth_blocked,
            text_hints=remediation_text_hints,
            checks=checks,
        )
        remediation_contract = cls._build_connector_preflight_remediation_contract(
            action=clean_action,
            capability=capability,
            hints=remediation_hints,
        )
        remediation_plan = cls._build_connector_preflight_remediation_plan(
            action=clean_action,
            capability=capability,
            code=code,
            hints=remediation_hints,
        )
        return {
            "status": "success" if code == "ready" else "error",
            "code": code,
            "severity": severity,
            "action": clean_action,
            "capability": capability,
            "contract_stage": contract_stage,
            "diagnostic_id": f"{clean_action}:{code}",
            "preflight_ready": code == "ready",
            "requested_provider": requested_provider,
            "candidate_providers": candidates,
            "allowed_providers": allowed_providers,
            "auth_blocked_providers": auth_blocked,
            "selected_provider": allowed_providers[0] if allowed_providers else "",
            "missing_fields": missing_fields,
            "any_of": any_of,
            "checks": checks[:24],
            "provider_routing": provider_routing,
            "provider_diagnostics": provider_diagnostics[:12],
            "remediation_hints": remediation_hints[:12],
            "remediation_contract": remediation_contract,
            "remediation_plan": remediation_plan,
        }

    @classmethod
    def _preflight_provider_candidates(cls, *, action: str, payload: Dict[str, Any], requested_provider: str) -> List[str]:
        clean_action = str(action or "").strip().lower()
        if requested_provider and requested_provider != "auto":
            return [requested_provider]
        fallback_aliases = cls._DRY_RUN_PROVIDER_ALIASES.get(clean_action, {})
        fallback_candidates = [
            str(value).strip().lower()
            for value in fallback_aliases.values()
            if str(value).strip().lower() and str(value).strip().lower() != "auto"
        ]
        fallback_candidates = list(dict.fromkeys(fallback_candidates))
        if not fallback_candidates:
            if clean_action == "external_email_send":
                fallback_candidates = ["gmail", "graph", "smtp"]
            elif clean_action in {"external_email_list", "external_email_read"}:
                fallback_candidates = ["gmail", "graph"]
            elif clean_action in {"external_calendar_create_event", "external_calendar_list_events", "external_calendar_update_event"}:
                fallback_candidates = ["google", "graph"]
            elif clean_action in {"external_doc_create", "external_doc_list", "external_doc_read", "external_doc_update"}:
                fallback_candidates = ["google_docs", "graph_drive"]
            elif clean_action in {"external_task_list", "external_task_create", "external_task_update"}:
                fallback_candidates = ["google_tasks", "graph_todo"]
        if clean_action in {"external_email_send", "external_email_list", "external_email_read"}:
            include_smtp = clean_action == "external_email_send"
            candidates = cls._auto_email_candidates(payload, include_smtp=include_smtp)
            return list(dict.fromkeys(candidates or fallback_candidates))
        if clean_action in {"external_calendar_create_event", "external_calendar_list_events", "external_calendar_update_event"}:
            candidates = cls._auto_calendar_candidates(payload)
            return list(dict.fromkeys(candidates or fallback_candidates))
        if clean_action in {"external_doc_create", "external_doc_list", "external_doc_read", "external_doc_update"}:
            candidates = cls._auto_document_candidates(payload)
            return list(dict.fromkeys(candidates or fallback_candidates))
        if clean_action in {"external_task_list", "external_task_create", "external_task_update"}:
            candidates = cls._auto_task_candidates(payload)
            return list(dict.fromkeys(candidates or fallback_candidates))
        return fallback_candidates

    @classmethod
    def _missing_required_fields(cls, *, action: str, payload: Dict[str, Any]) -> List[str]:
        required = cls._CONTRACT_REQUIRED_FIELDS.get(str(action or "").strip().lower(), [])
        out: List[str] = []
        for field in required:
            if not cls._payload_has_value(payload.get(field)):
                out.append(field)
        return out

    @classmethod
    def _missing_any_of_fields(cls, *, action: str, payload: Dict[str, Any]) -> List[List[str]]:
        groups = cls._CONTRACT_ANY_OF_FIELDS.get(str(action or "").strip().lower(), [])
        missing_groups: List[List[str]] = []
        for group in groups:
            clean_group = [str(item).strip() for item in group if str(item).strip()]
            if not clean_group:
                continue
            has_any = any(cls._payload_has_value(payload.get(field)) for field in clean_group)
            if not has_any:
                missing_groups.append(clean_group)
        return missing_groups

    @staticmethod
    def _payload_has_value(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (list, tuple, set, dict)):
            return len(value) > 0
        return True

    @staticmethod
    def _external_capability_for_action(action: str) -> str:
        clean_action = str(action or "").strip().lower()
        if clean_action.startswith("external_email_"):
            return "email"
        if clean_action.startswith("external_calendar_"):
            return "calendar"
        if clean_action.startswith("external_doc_"):
            return "document"
        if clean_action.startswith("external_task_"):
            return "task"
        return "external"

    @classmethod
    def _build_connector_preflight_remediation_hints(
        cls,
        *,
        action: str,
        capability: str,
        code: str,
        missing_fields: List[str],
        any_of: List[List[str]],
        requested_provider: str,
        allowed_providers: List[str],
        auth_blocked: List[str],
        text_hints: List[str],
        checks: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        hints: List[Dict[str, Any]] = []
        clean_action = str(action or "").strip().lower()
        clean_capability = str(capability or "").strip().lower() or "external"
        if missing_fields:
            hints.append(
                {
                    "id": "supply_required_fields",
                    "priority": 1,
                    "confidence": 0.94,
                    "summary": f"Provide required fields for {clean_action}.",
                    "action": clean_action,
                    "capability": clean_capability,
                    "missing_fields": missing_fields[:12],
                }
            )
        if any_of:
            hints.append(
                {
                    "id": "supply_any_of_fields",
                    "priority": 1,
                    "confidence": 0.9,
                    "summary": "Provide at least one field from each required mutable-field group.",
                    "action": clean_action,
                    "capability": clean_capability,
                    "any_of": any_of[:8],
                }
            )
        if code in {"auth_preflight_failed", "no_provider_candidates_after_contract"} or auth_blocked:
            hints.append(
                {
                    "id": "run_auth_maintenance",
                    "priority": 1,
                    "confidence": 0.92,
                    "summary": "Run OAuth maintenance and refresh token inventory before retry.",
                    "tool_action": {"action": "oauth_token_maintain", "args": {"provider": "auto", "limit": 40, "window_s": 7200}},
                    "action": clean_action,
                    "capability": clean_capability,
                    "auth_blocked_providers": auth_blocked[:8],
                }
            )
        if requested_provider and requested_provider != "auto" and allowed_providers and requested_provider not in allowed_providers:
            hints.append(
                {
                    "id": "switch_provider_auto",
                    "priority": 2,
                    "confidence": 0.84,
                    "summary": "Switch to an available provider for this action.",
                    "args_patch": {"provider": allowed_providers[0]},
                    "action": clean_action,
                    "capability": clean_capability,
                }
            )
        elif not requested_provider or requested_provider == "auto":
            if allowed_providers:
                hints.append(
                    {
                        "id": "pin_provider",
                        "priority": 3,
                        "confidence": 0.72,
                        "summary": "Pin provider for deterministic retries after preflight.",
                        "args_patch": {"provider": allowed_providers[0]},
                        "action": clean_action,
                        "capability": clean_capability,
                    }
                )
        if checks:
            hints.append(
                {
                    "id": "run_connector_status_probe",
                    "priority": 2,
                    "confidence": 0.78,
                    "summary": "Run connector status probe to validate provider health before execution.",
                    "tool_action": {"action": "external_connector_status", "args": {}},
                    "action": clean_action,
                    "capability": clean_capability,
                }
            )
        if text_hints:
            hints.append(
                {
                    "id": "provider_preflight_repair",
                    "priority": 2,
                    "confidence": 0.82,
                    "summary": "Apply provider-specific preflight repairs.",
                    "instructions": text_hints[:8],
                    "action": clean_action,
                    "capability": clean_capability,
                }
            )
        dedup: Dict[str, Dict[str, Any]] = {}
        for row in hints:
            if not isinstance(row, dict):
                continue
            hint_id = str(row.get("id", "")).strip().lower()
            if not hint_id:
                continue
            existing = dedup.get(hint_id)
            if existing is None:
                dedup[hint_id] = row
                continue
            old_conf = float(existing.get("confidence", 0.0) or 0.0)
            new_conf = float(row.get("confidence", 0.0) or 0.0)
            if new_conf > old_conf:
                dedup[hint_id] = row
        rows = list(dedup.values())
        rows.sort(
            key=lambda row: (
                int(row.get("priority", 999) or 999),
                -float(row.get("confidence", 0.0) or 0.0),
                str(row.get("id", "")),
            )
        )
        return rows[:12]

    @classmethod
    def _build_connector_preflight_remediation_contract(
        cls,
        *,
        action: str,
        capability: str,
        hints: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        strategies: List[Dict[str, Any]] = []
        for hint in hints:
            if not isinstance(hint, dict):
                continue
            hint_id = str(hint.get("id", "")).strip().lower()
            confidence = float(hint.get("confidence", 0.0) or 0.0)
            if isinstance(hint.get("tool_action"), dict):
                strategies.append(
                    {
                        "type": "tool_action",
                        "id": hint_id,
                        "tool_action": dict(hint.get("tool_action", {})),
                        "priority": int(hint.get("priority", 999) or 999),
                        "confidence": confidence,
                    }
                )
            if isinstance(hint.get("args_patch"), dict):
                strategies.append(
                    {
                        "type": "args_patch",
                        "id": hint_id,
                        "args_patch": dict(hint.get("args_patch", {})),
                        "priority": int(hint.get("priority", 999) or 999),
                        "confidence": confidence,
                    }
                )
        strategies.sort(
            key=lambda row: (
                int(row.get("priority", 999) or 999),
                -float(row.get("confidence", 0.0) or 0.0),
                str(row.get("id", "")),
            )
        )
        return {
            "version": "1.1",
            "action": str(action or "").strip().lower(),
            "capability": str(capability or "").strip().lower(),
            "strategy_count": len(strategies),
            "strategies": strategies[:14],
        }

    @classmethod
    def _build_connector_preflight_remediation_plan(
        cls,
        *,
        action: str,
        capability: str,
        code: str,
        hints: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        phases: List[Dict[str, Any]] = []
        if code in {"missing_required_fields", "missing_any_of_fields"}:
            phases.append(
                {
                    "phase": "normalize_args",
                    "confidence": 0.92,
                    "summary": "Repair payload fields before provider execution.",
                }
            )
        if code in {"auth_preflight_failed", "no_provider_candidates_after_contract", "provider_not_supported_for_action"}:
            phases.append(
                {
                    "phase": "repair_dependency",
                    "confidence": 0.9,
                    "summary": "Repair provider credentials/routing before retry.",
                    "tool_action": {"action": "oauth_token_maintain", "args": {"provider": "auto", "limit": 40, "window_s": 7200}},
                }
            )
        phases.append(
            {
                "phase": "diagnose",
                "confidence": 0.8,
                "summary": "Run connector health probe before execution retry.",
                "tool_action": {"action": "external_connector_status", "args": {}},
            }
        )
        best_patch = next((row.get("args_patch") for row in hints if isinstance(row, dict) and isinstance(row.get("args_patch"), dict)), None)
        if isinstance(best_patch, dict) and best_patch:
            phases.append(
                {
                    "phase": "normalize_args",
                    "confidence": 0.78,
                    "summary": "Apply best-ranked args patch from contract hints.",
                    "args_patch": dict(best_patch),
                }
            )
        phases.append(
            {
                "phase": "retry",
                "confidence": 0.74,
                "summary": f"Retry {str(action or '').strip().lower()} after preflight contract fixes.",
            }
        )
        return phases[:8]

    @staticmethod
    def _is_dry_run(payload: Dict[str, Any]) -> bool:
        value = payload.get("dry_run", False)
        if isinstance(value, bool):
            return value
        clean = str(value).strip().lower()
        if clean in {"1", "true", "yes", "on"}:
            return True
        if clean in {"0", "false", "no", "off", ""}:
            return False
        return False

    @classmethod
    def _canonical_provider_for_action(cls, *, action: str, provider: str) -> str:
        clean_action = str(action or "").strip().lower()
        clean_provider = str(provider or "").strip().lower() or "auto"
        aliases = cls._DRY_RUN_PROVIDER_ALIASES.get(clean_action, {})
        if clean_provider in aliases:
            return str(aliases.get(clean_provider, clean_provider))
        return clean_provider

    @classmethod
    def _provider_preflight_diagnostic(cls, *, provider: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        clean_provider = str(provider or "").strip().lower()
        if clean_provider in {"gmail", "google", "google_docs", "google_tasks"}:
            token = cls._token(
                payload,
                ["google_access_token", "access_token"],
                ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"],
                provider="google",
            )
            available = bool(token)
            hints = [] if available else ["Provide Google OAuth access token or store a refresh token for provider=google."]
            return {"provider": clean_provider, "available": available, "credential_type": "oauth_google", "remediation_hints": hints}
        if clean_provider in {"graph", "graph_drive", "graph_todo"}:
            token = cls._token(
                payload,
                ["graph_access_token", "access_token"],
                ["MICROSOFT_GRAPH_ACCESS_TOKEN"],
                provider="graph",
            )
            available = bool(token)
            hints = [] if available else ["Provide Microsoft Graph access token or store a refresh token for provider=graph."]
            return {"provider": clean_provider, "available": available, "credential_type": "oauth_graph", "remediation_hints": hints}
        if clean_provider == "smtp":
            host = str(payload.get("host") or os.getenv("SMTP_HOST", "")).strip()
            available = bool(host)
            hints = [] if available else ["Set SMTP_HOST (or payload.host) and optionally SMTP credentials before sending email."]
            return {"provider": clean_provider, "available": available, "credential_type": "smtp", "remediation_hints": hints}
        return {
            "provider": clean_provider,
            "available": False,
            "credential_type": "unknown",
            "remediation_hints": [f"Unsupported provider '{clean_provider}' for this action."],
        }

    @classmethod
    def _dry_run_mutation_response(
        cls,
        *,
        action: str,
        payload: Dict[str, Any],
        provider: str,
        candidate_providers: List[str],
        expected_mutation: Dict[str, Any],
        reversible: bool = False,
        high_impact: bool = True,
    ) -> Dict[str, Any]:
        clean_action = str(action or "").strip().lower()
        canonical_provider = cls._canonical_provider_for_action(action=clean_action, provider=provider)
        if canonical_provider == "auto":
            normalized_candidates = [cls._canonical_provider_for_action(action=clean_action, provider=item) for item in candidate_providers]
        else:
            normalized_candidates = [canonical_provider]
        normalized_candidates = [item for item in normalized_candidates if str(item).strip()]
        if not normalized_candidates:
            normalized_candidates = [canonical_provider] if canonical_provider and canonical_provider != "auto" else []

        diagnostics: List[Dict[str, Any]] = []
        viable: List[str] = []
        remediation_hints: List[str] = []
        for candidate in normalized_candidates:
            diag = cls._provider_preflight_diagnostic(provider=candidate, payload=payload)
            diagnostics.append(diag)
            if bool(diag.get("available", False)):
                viable.append(candidate)
            hint_rows = diag.get("remediation_hints", [])
            if isinstance(hint_rows, list):
                for hint in hint_rows:
                    text = str(hint).strip()
                    if text and text not in remediation_hints:
                        remediation_hints.append(text)

        selected_provider = viable[0] if viable else ""
        patch: Dict[str, Any] = {}
        if canonical_provider == "auto" and selected_provider:
            patch["provider"] = selected_provider
        elif canonical_provider and canonical_provider != "auto":
            patch["provider"] = canonical_provider

        simulation = {
            "action": clean_action,
            "mode": "dry_run",
            "high_impact": bool(high_impact),
            "rollback_supported": bool(reversible),
            "reversible": bool(reversible),
            "non_reversible": not bool(reversible),
            "provider_candidates": normalized_candidates,
            "provider_diagnostics": diagnostics,
            "viable_providers": viable,
            "selected_provider": selected_provider,
            "expected_mutation": expected_mutation,
            "recommended_args_patch": patch,
            "remediation_hints": remediation_hints[:6],
        }
        if viable:
            return {
                "status": "success",
                "dry_run": True,
                "provider": selected_provider or patch.get("provider", ""),
                "args_patch": patch,
                "simulation": simulation,
            }
        message = f"Dry-run preflight failed for '{clean_action}': no viable providers."
        return {
            "status": "error",
            "dry_run": True,
            "message": message,
            "args_patch": patch,
            "simulation": simulation,
            "remediation_hints": remediation_hints[:6],
        }

    @classmethod
    def _send_email_via_provider(
        cls,
        provider: str,
        *,
        payload: Dict[str, Any],
        to_list: List[str],
        subject: str,
        body: str,
    ) -> Dict[str, Any]:
        if provider == "gmail":
            return cls._send_gmail(payload, to_list=to_list, subject=subject, body=body)
        if provider == "graph":
            return cls._send_graph_mail(payload, to_list=to_list, subject=subject, body=body)
        if provider == "smtp":
            return cls._send_smtp(payload, to_list=to_list, subject=subject, body=body)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def _create_calendar_via_provider(
        cls,
        provider: str,
        *,
        payload: Dict[str, Any],
        title: str,
        start_dt: datetime,
        end_dt: datetime,
        timezone_name: str,
    ) -> Dict[str, Any]:
        if provider == "google":
            return cls._create_google_event(
                payload,
                title=title,
                start_dt=start_dt,
                end_dt=end_dt,
                timezone_name=timezone_name,
            )
        if provider == "graph":
            return cls._create_graph_event(
                payload,
                title=title,
                start_dt=start_dt,
                end_dt=end_dt,
                timezone_name=timezone_name,
            )
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def _create_document_via_provider(
        cls,
        provider: str,
        *,
        payload: Dict[str, Any],
        title: str,
        content: str,
    ) -> Dict[str, Any]:
        if provider == "google_docs":
            return cls._create_google_doc(payload, title=title, content=content)
        if provider == "graph_drive":
            return cls._create_graph_doc(payload, title=title, content=content)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def _list_email_via_provider(
        cls,
        provider: str,
        *,
        payload: Dict[str, Any],
        max_results: int,
        query: str,
    ) -> Dict[str, Any]:
        if provider == "gmail":
            return cls._list_gmail_messages(payload, max_results=max_results, query=query)
        if provider == "graph":
            return cls._list_graph_messages(payload, max_results=max_results, query=query)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def _read_email_via_provider(
        cls,
        provider: str,
        *,
        payload: Dict[str, Any],
        message_id: str,
    ) -> Dict[str, Any]:
        if provider == "gmail":
            return cls._read_gmail_message(payload, message_id=message_id)
        if provider == "graph":
            return cls._read_graph_message(payload, message_id=message_id)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def _list_calendar_via_provider(
        cls,
        provider: str,
        *,
        payload: Dict[str, Any],
        max_results: int,
    ) -> Dict[str, Any]:
        if provider == "google":
            return cls._list_google_events(payload, max_results=max_results)
        if provider == "graph":
            return cls._list_graph_events(payload, max_results=max_results)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def _list_document_via_provider(
        cls,
        provider: str,
        *,
        payload: Dict[str, Any],
        max_results: int,
        query: str,
    ) -> Dict[str, Any]:
        if provider == "google_docs":
            return cls._list_google_docs(payload, max_results=max_results, query=query)
        if provider == "graph_drive":
            return cls._list_graph_docs(payload, max_results=max_results, query=query)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def _read_document_via_provider(
        cls,
        provider: str,
        *,
        payload: Dict[str, Any],
        document_id: str,
    ) -> Dict[str, Any]:
        if provider == "google_docs":
            return cls._read_google_doc(payload, document_id=document_id)
        if provider == "graph_drive":
            return cls._read_graph_doc(payload, document_id=document_id)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def _list_task_via_provider(
        cls,
        provider: str,
        *,
        payload: Dict[str, Any],
        max_results: int,
        include_completed: bool,
        query: str,
    ) -> Dict[str, Any]:
        if provider == "google_tasks":
            return cls._list_google_tasks(payload, max_results=max_results, include_completed=include_completed, query=query)
        if provider == "graph_todo":
            return cls._list_graph_todo_tasks(payload, max_results=max_results, include_completed=include_completed, query=query)
        return {"status": "error", "message": f"Unsupported provider: {provider}"}

    @classmethod
    def _auto_email_candidates(cls, payload: Dict[str, Any], *, include_smtp: bool) -> List[str]:
        providers: List[str] = []
        if cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google"):
            providers.append("gmail")
        if cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph"):
            providers.append("graph")
        if include_smtp and os.getenv("SMTP_HOST"):
            providers.append("smtp")
        return providers

    @classmethod
    def _auto_calendar_candidates(cls, payload: Dict[str, Any]) -> List[str]:
        providers: List[str] = []
        if cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google"):
            providers.append("google")
        if cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph"):
            providers.append("graph")
        return providers

    @classmethod
    def _auto_document_candidates(cls, payload: Dict[str, Any]) -> List[str]:
        providers: List[str] = []
        if cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google"):
            providers.append("google_docs")
        if cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph"):
            providers.append("graph_drive")
        return providers

    @classmethod
    def _auto_task_candidates(cls, payload: Dict[str, Any]) -> List[str]:
        providers: List[str] = []
        if cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google"):
            providers.append("google_tasks")
        if cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph"):
            providers.append("graph_todo")
        return providers

    @classmethod
    def _resolve_email_provider(cls, payload: Dict[str, Any]) -> str:
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if provider == "auto":
            if cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google"):
                return "gmail"
            if cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph"):
                return "graph"
            if os.getenv("SMTP_HOST"):
                return "smtp"
            return "error"
        if provider in {"gmail", "google"}:
            return "gmail"
        if provider in {"graph", "microsoft", "microsoft_graph"}:
            return "graph"
        if provider == "smtp":
            return "smtp"
        return provider

    @classmethod
    def _resolve_calendar_provider(cls, payload: Dict[str, Any]) -> str:
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if provider == "auto":
            if cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google"):
                return "google"
            if cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph"):
                return "graph"
            return "error"
        if provider in {"google", "gcal"}:
            return "google"
        if provider in {"graph", "microsoft", "microsoft_graph"}:
            return "graph"
        return provider

    @classmethod
    def _resolve_document_provider(cls, payload: Dict[str, Any]) -> str:
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if provider == "auto":
            if cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google"):
                return "google_docs"
            if cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph"):
                return "graph_drive"
            return "error"
        if provider in {"google", "google_docs", "gdocs"}:
            return "google_docs"
        if provider in {"graph", "graph_drive", "microsoft_graph", "onedrive"}:
            return "graph_drive"
        return provider

    @classmethod
    def _resolve_task_provider(cls, payload: Dict[str, Any]) -> str:
        provider = str(payload.get("provider", "auto")).strip().lower() or "auto"
        if provider == "auto":
            if cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google"):
                return "google_tasks"
            if cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph"):
                return "graph_todo"
            return "error"
        if provider in {"google_tasks", "google", "gtasks"}:
            return "google_tasks"
        if provider in {"graph", "microsoft", "microsoft_graph", "graph_todo", "todo", "microsoft_todo"}:
            return "graph_todo"
        return provider

    @classmethod
    def _list_gmail_messages(cls, payload: Dict[str, Any], *, max_results: int, query: str) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required for Gmail."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        params: Dict[str, Any] = {"maxResults": max_results}
        if query:
            params["q"] = query
        try:
            response = requests.get(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages",
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Gmail list error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            rows = data.get("messages", [])
            items: List[Dict[str, Any]] = []
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    message_id = str(row.get("id", "")).strip()
                    if not message_id:
                        continue
                    items.append({"id": message_id, "thread_id": str(row.get("threadId", ""))})
            return {
                "status": "success",
                "provider": "gmail",
                "count": len(items),
                "items": items,
                "query": query,
                "max_results": max_results,
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _read_gmail_message(cls, payload: Dict[str, Any], *, message_id: str) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required for Gmail."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        try:
            response = requests.get(
                f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}",
                headers={"Authorization": f"Bearer {token}"},
                params={"format": "metadata", "metadataHeaders": ["Subject", "From", "To", "Date"]},
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Gmail read error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            payload_node = data.get("payload")
            headers: Dict[str, str] = {}
            if isinstance(payload_node, dict):
                header_rows = payload_node.get("headers")
                if isinstance(header_rows, list):
                    for item in header_rows:
                        if not isinstance(item, dict):
                            continue
                        name = str(item.get("name", "")).strip()
                        value = str(item.get("value", "")).strip()
                        if name:
                            headers[name.lower()] = value
            return {
                "status": "success",
                "provider": "gmail",
                "message_id": message_id,
                "thread_id": str(data.get("threadId", "")),
                "subject": headers.get("subject", ""),
                "from": headers.get("from", ""),
                "to": headers.get("to", ""),
                "date": headers.get("date", ""),
                "snippet": str(data.get("snippet", "")),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _list_graph_messages(cls, payload: Dict[str, Any], *, max_results: int, query: str) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        endpoint = str(payload.get("endpoint", "https://graph.microsoft.com/v1.0/me/messages")).strip()
        params: Dict[str, Any] = {"$top": max_results}
        if query:
            params["$search"] = f'"{query}"'
        try:
            response = requests.get(
                endpoint,
                headers={"Authorization": f"Bearer {token}", "ConsistencyLevel": "eventual"},
                params=params,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Graph mail list error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            rows = data.get("value", [])
            items: List[Dict[str, Any]] = []
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    sender = ""
                    from_node = row.get("from")
                    if isinstance(from_node, dict):
                        email_node = from_node.get("emailAddress")
                        if isinstance(email_node, dict):
                            sender = str(email_node.get("address", ""))
                    items.append(
                        {
                            "id": str(row.get("id", "")),
                            "subject": str(row.get("subject", "")),
                            "from": sender,
                            "received_at": str(row.get("receivedDateTime", "")),
                            "is_read": bool(row.get("isRead", False)),
                        }
                    )
            return {
                "status": "success",
                "provider": "microsoft_graph_mail",
                "count": len(items),
                "items": items,
                "query": query,
                "max_results": max_results,
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _read_graph_message(cls, payload: Dict[str, Any], *, message_id: str) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        endpoint = str(payload.get("endpoint", f"https://graph.microsoft.com/v1.0/me/messages/{message_id}")).strip()
        try:
            response = requests.get(
                endpoint,
                headers={"Authorization": f"Bearer {token}"},
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Graph mail read error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            to_rows = data.get("toRecipients", [])
            to_addresses: List[str] = []
            if isinstance(to_rows, list):
                for item in to_rows:
                    if not isinstance(item, dict):
                        continue
                    email_node = item.get("emailAddress")
                    if not isinstance(email_node, dict):
                        continue
                    address = str(email_node.get("address", "")).strip()
                    if address:
                        to_addresses.append(address)
            sender = ""
            from_node = data.get("from")
            if isinstance(from_node, dict):
                email_node = from_node.get("emailAddress")
                if isinstance(email_node, dict):
                    sender = str(email_node.get("address", "")).strip()
            return {
                "status": "success",
                "provider": "microsoft_graph_mail",
                "message_id": str(data.get("id", message_id)),
                "subject": str(data.get("subject", "")),
                "from": sender,
                "to": to_addresses,
                "received_at": str(data.get("receivedDateTime", "")),
                "body_preview": str(data.get("bodyPreview", "")),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _list_google_events(cls, payload: Dict[str, Any], *, max_results: int) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        calendar_id = str(payload.get("calendar_id", "primary")).strip() or "primary"
        params: Dict[str, Any] = {
            "maxResults": max_results,
            "singleEvents": bool(payload.get("single_events", True)),
            "orderBy": str(payload.get("order_by", "startTime")).strip() or "startTime",
        }
        time_min = str(payload.get("time_min", "")).strip()
        time_max = str(payload.get("time_max", "")).strip()
        if time_min:
            params["timeMin"] = time_min
        if time_max:
            params["timeMax"] = time_max
        try:
            response = requests.get(
                f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events",
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Google Calendar list error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            rows = data.get("items", [])
            items: List[Dict[str, Any]] = []
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    start_data = row.get("start") if isinstance(row.get("start"), dict) else {}
                    end_data = row.get("end") if isinstance(row.get("end"), dict) else {}
                    items.append(
                        {
                            "event_id": str(row.get("id", "")),
                            "title": str(row.get("summary", "")),
                            "start": str((start_data or {}).get("dateTime") or (start_data or {}).get("date") or ""),
                            "end": str((end_data or {}).get("dateTime") or (end_data or {}).get("date") or ""),
                            "html_link": str(row.get("htmlLink", "")),
                        }
                    )
            return {
                "status": "success",
                "provider": "google_calendar",
                "calendar_id": calendar_id,
                "count": len(items),
                "items": items,
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _list_graph_events(cls, payload: Dict[str, Any], *, max_results: int) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        endpoint = str(payload.get("endpoint", "https://graph.microsoft.com/v1.0/me/events")).strip()
        params: Dict[str, Any] = {"$top": max_results}
        try:
            response = requests.get(
                endpoint,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Graph calendar list error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            rows = data.get("value", [])
            items: List[Dict[str, Any]] = []
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    start_data = row.get("start") if isinstance(row.get("start"), dict) else {}
                    end_data = row.get("end") if isinstance(row.get("end"), dict) else {}
                    items.append(
                        {
                            "event_id": str(row.get("id", "")),
                            "title": str(row.get("subject", "")),
                            "start": str((start_data or {}).get("dateTime", "")),
                            "end": str((end_data or {}).get("dateTime", "")),
                            "web_link": str(row.get("webLink", "")),
                        }
                    )
            return {
                "status": "success",
                "provider": "microsoft_graph_calendar",
                "count": len(items),
                "items": items,
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _update_google_event(
        cls,
        payload: Dict[str, Any],
        *,
        event_id: str,
        title: str,
        description: str,
        start_iso: str,
        end_iso: str,
        timezone_name: str,
        attendees: List[str],
    ) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        calendar_id = str(payload.get("calendar_id", "primary")).strip() or "primary"
        patch_body: Dict[str, Any] = {}
        if title:
            patch_body["summary"] = title
        if description:
            patch_body["description"] = description
        if start_iso:
            patch_body["start"] = {"dateTime": start_iso, "timeZone": timezone_name}
        if end_iso:
            patch_body["end"] = {"dateTime": end_iso, "timeZone": timezone_name}
        if attendees:
            patch_body["attendees"] = [{"email": item} for item in attendees]
        try:
            response = requests.patch(
                f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events/{event_id}",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=patch_body,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Google Calendar update error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            return {
                "status": "success",
                "provider": "google_calendar",
                "event_id": str(data.get("id", event_id)),
                "title": str(data.get("summary", title)),
                "html_link": str(data.get("htmlLink", "")),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _update_graph_event(
        cls,
        payload: Dict[str, Any],
        *,
        event_id: str,
        title: str,
        description: str,
        start_iso: str,
        end_iso: str,
        timezone_name: str,
        attendees: List[str],
    ) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        patch_body: Dict[str, Any] = {}
        if title:
            patch_body["subject"] = title
        if description:
            patch_body["body"] = {"contentType": "Text", "content": description}
        if start_iso:
            patch_body["start"] = {"dateTime": start_iso, "timeZone": timezone_name}
        if end_iso:
            patch_body["end"] = {"dateTime": end_iso, "timeZone": timezone_name}
        if attendees:
            patch_body["attendees"] = [{"emailAddress": {"address": item}, "type": "required"} for item in attendees]
        endpoint = str(payload.get("endpoint", f"https://graph.microsoft.com/v1.0/me/events/{event_id}")).strip()
        try:
            response = requests.patch(
                endpoint,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=patch_body,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Graph calendar update error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            return {
                "status": "success",
                "provider": "microsoft_graph_calendar",
                "event_id": str(data.get("id", event_id)),
                "title": str(data.get("subject", title)),
                "web_link": str(data.get("webLink", "")),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _list_google_docs(cls, payload: Dict[str, Any], *, max_results: int, query: str) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        drive_query = "mimeType='application/vnd.google-apps.document'"
        if query:
            escaped = query.replace("'", "\\'")
            drive_query = f"{drive_query} and name contains '{escaped}'"
        params = {"q": drive_query, "pageSize": max_results, "fields": "files(id,name,webViewLink,modifiedTime)"}
        try:
            response = requests.get(
                "https://www.googleapis.com/drive/v3/files",
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Google Docs list error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            rows = data.get("files", [])
            items: List[Dict[str, Any]] = []
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    items.append(
                        {
                            "document_id": str(row.get("id", "")),
                            "title": str(row.get("name", "")),
                            "web_url": str(row.get("webViewLink", "")),
                            "modified_at": str(row.get("modifiedTime", "")),
                        }
                    )
            return {
                "status": "success",
                "provider": "google_docs",
                "count": len(items),
                "items": items,
                "query": query,
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _read_google_doc(cls, payload: Dict[str, Any], *, document_id: str) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        try:
            response = requests.get(
                f"https://docs.googleapis.com/v1/documents/{document_id}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Google Docs read error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            return {
                "status": "success",
                "provider": "google_docs",
                "document_id": document_id,
                "title": str(data.get("title", "")),
                "text": cls._extract_google_doc_text(data),
                "url": f"https://docs.google.com/document/d/{document_id}/edit",
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _update_google_doc(cls, payload: Dict[str, Any], *, document_id: str, title: str, content: str) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        try:
            if title:
                requests.patch(
                    f"https://www.googleapis.com/drive/v3/files/{document_id}",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"name": title},
                    timeout=25,
                )
            if content:
                update_response = requests.post(
                    f"https://docs.googleapis.com/v1/documents/{document_id}:batchUpdate",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"requests": [{"insertText": {"location": {"index": 1}, "text": f"{content}\n"}}]},
                    timeout=25,
                )
                if update_response.status_code >= 300:
                    return {"status": "error", "message": f"Google Docs update error: {update_response.status_code} {update_response.text[:200]}"}
            return {
                "status": "success",
                "provider": "google_docs",
                "document_id": document_id,
                "title": title,
                "updated_fields": [field for field, value in {"title": title, "content": content}.items() if value],
                "url": f"https://docs.google.com/document/d/{document_id}/edit",
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _list_graph_docs(cls, payload: Dict[str, Any], *, max_results: int, query: str) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        endpoint = str(payload.get("endpoint", "https://graph.microsoft.com/v1.0/me/drive/root/children")).strip()
        params: Dict[str, Any] = {"$top": max_results}
        if query:
            safe_query = query.replace("'", "''")
            params["$filter"] = f"contains(name,'{safe_query}')"
        try:
            response = requests.get(
                endpoint,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Graph drive list error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            rows = data.get("value", [])
            items: List[Dict[str, Any]] = []
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    items.append(
                        {
                            "document_id": str(row.get("id", "")),
                            "title": str(row.get("name", "")),
                            "web_url": str(row.get("webUrl", "")),
                            "size": int(row.get("size", 0) or 0),
                            "last_modified": str(row.get("lastModifiedDateTime", "")),
                        }
                    )
            return {
                "status": "success",
                "provider": "microsoft_graph_drive",
                "count": len(items),
                "items": items,
                "query": query,
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _read_graph_doc(cls, payload: Dict[str, Any], *, document_id: str) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        metadata_endpoint = str(payload.get("metadata_endpoint", f"https://graph.microsoft.com/v1.0/me/drive/items/{document_id}")).strip()
        content_endpoint = str(payload.get("content_endpoint", f"https://graph.microsoft.com/v1.0/me/drive/items/{document_id}/content")).strip()
        try:
            metadata_response = requests.get(
                metadata_endpoint,
                headers={"Authorization": f"Bearer {token}"},
                timeout=25,
            )
            if metadata_response.status_code >= 300:
                return {"status": "error", "message": f"Graph doc metadata error: {metadata_response.status_code} {metadata_response.text[:200]}"}
            metadata = metadata_response.json() if metadata_response.text else {}
            content_response = requests.get(
                content_endpoint,
                headers={"Authorization": f"Bearer {token}"},
                timeout=25,
            )
            if content_response.status_code >= 300:
                return {"status": "error", "message": f"Graph doc content error: {content_response.status_code} {content_response.text[:200]}"}
            return {
                "status": "success",
                "provider": "microsoft_graph_drive",
                "document_id": document_id,
                "title": str(metadata.get("name", "")),
                "web_url": str(metadata.get("webUrl", "")),
                "text": content_response.text if content_response.text is not None else "",
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _update_graph_doc(cls, payload: Dict[str, Any], *, document_id: str, title: str, content: str) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        metadata_endpoint = str(payload.get("metadata_endpoint", f"https://graph.microsoft.com/v1.0/me/drive/items/{document_id}")).strip()
        content_endpoint = str(payload.get("content_endpoint", f"https://graph.microsoft.com/v1.0/me/drive/items/{document_id}/content")).strip()
        try:
            item_data: Dict[str, Any] = {}
            if title:
                rename_response = requests.patch(
                    metadata_endpoint,
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"name": title},
                    timeout=25,
                )
                if rename_response.status_code >= 300:
                    return {"status": "error", "message": f"Graph doc rename error: {rename_response.status_code} {rename_response.text[:200]}"}
                item_data = rename_response.json() if rename_response.text else {}
            if content:
                content_response = requests.put(
                    content_endpoint,
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "text/plain"},
                    data=content.encode("utf-8", errors="ignore"),
                    timeout=25,
                )
                if content_response.status_code >= 300:
                    return {"status": "error", "message": f"Graph doc update error: {content_response.status_code} {content_response.text[:200]}"}
                item_data = content_response.json() if content_response.text else item_data
            return {
                "status": "success",
                "provider": "microsoft_graph_drive",
                "document_id": document_id,
                "title": str(item_data.get("name", title)),
                "web_url": str(item_data.get("webUrl", "")),
                "updated_fields": [field for field, value in {"title": title, "content": content}.items() if value],
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _send_gmail(cls, payload: Dict[str, Any], *, to_list: List[str], subject: str, body: str) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required for Gmail."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}

        message = EmailMessage()
        message["To"] = ", ".join(to_list)
        from_addr = str(payload.get("from") or "me").strip() or "me"
        if from_addr != "me":
            message["From"] = from_addr
        if subject:
            message["Subject"] = subject
        cc_list = cls._normalize_recipients(payload.get("cc"))
        bcc_list = cls._normalize_recipients(payload.get("bcc"))
        if cc_list:
            message["Cc"] = ", ".join(cc_list)
        if bcc_list:
            message["Bcc"] = ", ".join(bcc_list)
        message.set_content(body or "(empty body)")
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")

        try:
            response = requests.post(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"raw": raw},
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Gmail API error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            return {
                "status": "success",
                "provider": "gmail",
                "message_id": str(data.get("id", "")),
                "thread_id": str(data.get("threadId", "")),
                "to": to_list,
                "subject": subject,
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _send_graph_mail(cls, payload: Dict[str, Any], *, to_list: List[str], subject: str, body: str) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}

        html_mode = bool(payload.get("html", False))
        recipients = [{"emailAddress": {"address": addr}} for addr in to_list]
        cc_list = cls._normalize_recipients(payload.get("cc"))
        cc_recipients = [{"emailAddress": {"address": addr}} for addr in cc_list]
        endpoint = str(payload.get("endpoint", "https://graph.microsoft.com/v1.0/me/sendMail")).strip()
        body_type = "HTML" if html_mode else "Text"
        data = {
            "message": {
                "subject": subject,
                "body": {"contentType": body_type, "content": body or "(empty body)"},
                "toRecipients": recipients,
                "ccRecipients": cc_recipients,
            },
            "saveToSentItems": bool(payload.get("save_to_sent", True)),
        }

        try:
            response = requests.post(
                endpoint,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=data,
                timeout=25,
            )
            if response.status_code not in {200, 202}:
                return {"status": "error", "message": f"Graph mail error: {response.status_code} {response.text[:200]}"}
            return {"status": "success", "provider": "microsoft_graph_mail", "to": to_list, "subject": subject}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _send_smtp(cls, payload: Dict[str, Any], *, to_list: List[str], subject: str, body: str) -> Dict[str, Any]:
        host = str(payload.get("host") or os.getenv("SMTP_HOST", "")).strip()
        if not host:
            return {"status": "error", "message": "SMTP host is required."}
        port = cls._to_int(payload.get("port", os.getenv("SMTP_PORT", "587")), default=587, minimum=1, maximum=65535)
        username = str(payload.get("username") or os.getenv("SMTP_USERNAME", "")).strip()
        password = str(payload.get("password") or os.getenv("SMTP_PASSWORD", "")).strip()
        from_addr = str(payload.get("from") or os.getenv("SMTP_FROM", username)).strip()
        if not from_addr:
            return {"status": "error", "message": "SMTP from address is required."}
        use_tls = bool(payload.get("use_tls", str(os.getenv("SMTP_USE_TLS", "1")).strip() not in {"0", "false", "False"}))

        msg = EmailMessage()
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_list)
        if subject:
            msg["Subject"] = subject
        msg.set_content(body or "(empty body)")

        try:
            with smtplib.SMTP(host, port, timeout=20) as smtp:
                if use_tls:
                    smtp.starttls()
                if username and password:
                    smtp.login(username, password)
                smtp.send_message(msg)
            return {"status": "success", "provider": "smtp", "to": to_list, "subject": subject}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _create_google_event(
        cls,
        payload: Dict[str, Any],
        *,
        title: str,
        start_dt: datetime,
        end_dt: datetime,
        timezone_name: str,
    ) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        calendar_id = str(payload.get("calendar_id", "primary")).strip() or "primary"
        description = str(payload.get("description", "")).strip()
        attendees = cls._normalize_recipients(payload.get("attendees"))
        event_body = {
            "summary": title,
            "description": description,
            "start": {"dateTime": start_dt.isoformat(), "timeZone": timezone_name},
            "end": {"dateTime": end_dt.isoformat(), "timeZone": timezone_name},
            "attendees": [{"email": item} for item in attendees],
        }
        endpoint = f"https://www.googleapis.com/calendar/v3/calendars/{calendar_id}/events"
        try:
            response = requests.post(
                endpoint,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=event_body,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Google Calendar API error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            return {
                "status": "success",
                "provider": "google_calendar",
                "event_id": str(data.get("id", "")),
                "html_link": str(data.get("htmlLink", "")),
                "title": title,
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat(),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _create_graph_event(
        cls,
        payload: Dict[str, Any],
        *,
        title: str,
        start_dt: datetime,
        end_dt: datetime,
        timezone_name: str,
    ) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        description = str(payload.get("description", "")).strip()
        attendees = cls._normalize_recipients(payload.get("attendees"))
        event_body = {
            "subject": title,
            "body": {"contentType": "Text", "content": description},
            "start": {"dateTime": start_dt.isoformat(), "timeZone": timezone_name},
            "end": {"dateTime": end_dt.isoformat(), "timeZone": timezone_name},
            "attendees": [{"emailAddress": {"address": item}, "type": "required"} for item in attendees],
        }
        endpoint = str(payload.get("endpoint", "https://graph.microsoft.com/v1.0/me/events")).strip()
        try:
            response = requests.post(
                endpoint,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=event_body,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Graph calendar error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            return {
                "status": "success",
                "provider": "microsoft_graph_calendar",
                "event_id": str(data.get("id", "")),
                "web_link": str(data.get("webLink", "")),
                "title": title,
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat(),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _create_google_doc(cls, payload: Dict[str, Any], *, title: str, content: str) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}

        try:
            create_resp = requests.post(
                "https://docs.googleapis.com/v1/documents",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"title": title},
                timeout=25,
            )
            if create_resp.status_code >= 300:
                return {"status": "error", "message": f"Google Docs create error: {create_resp.status_code} {create_resp.text[:200]}"}
            created = create_resp.json() if create_resp.text else {}
            doc_id = str(created.get("documentId", ""))
            if content and doc_id:
                batch_body = {
                    "requests": [{"insertText": {"location": {"index": 1}, "text": content}}],
                }
                requests.post(
                    f"https://docs.googleapis.com/v1/documents/{doc_id}:batchUpdate",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json=batch_body,
                    timeout=25,
                )
            return {
                "status": "success",
                "provider": "google_docs",
                "document_id": doc_id,
                "title": title,
                "url": f"https://docs.google.com/document/d/{doc_id}/edit" if doc_id else "",
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _create_graph_doc(cls, payload: Dict[str, Any], *, title: str, content: str) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}

        filename = str(payload.get("filename", f"{title}.txt")).strip() or f"{title}.txt"
        endpoint = f"https://graph.microsoft.com/v1.0/me/drive/root:/{filename}:/content"
        body = content or ""
        try:
            response = requests.put(
                endpoint,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "text/plain"},
                data=body.encode("utf-8", errors="ignore"),
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Graph drive error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            return {
                "status": "success",
                "provider": "microsoft_graph_drive",
                "document_id": str(data.get("id", "")),
                "name": str(data.get("name", filename)),
                "web_url": str(data.get("webUrl", "")),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _list_google_tasks(
        cls,
        payload: Dict[str, Any],
        *,
        max_results: int,
        include_completed: bool,
        query: str,
    ) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required for Google Tasks."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        tasklist_id = str(payload.get("tasklist_id") or payload.get("task_list_id") or payload.get("list_id") or "@default").strip() or "@default"
        endpoint = f"https://tasks.googleapis.com/tasks/v1/lists/{tasklist_id}/tasks"
        params: Dict[str, Any] = {
            "maxResults": max_results,
            "showCompleted": "true" if include_completed else "false",
            "showHidden": "true",
        }
        try:
            response = requests.get(
                endpoint,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Google Tasks list error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            rows = data.get("items", [])
            q = query.lower()
            items: List[Dict[str, Any]] = []
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    title = str(row.get("title", "")).strip()
                    notes = str(row.get("notes", "")).strip()
                    if q and q not in title.lower() and q not in notes.lower():
                        continue
                    items.append(
                        {
                            "id": str(row.get("id", "")),
                            "title": title,
                            "notes": notes,
                            "status": str(row.get("status", "")),
                            "due": str(row.get("due", "")),
                            "updated": str(row.get("updated", "")),
                            "list_id": tasklist_id,
                            "web_url": str(row.get("selfLink", "")),
                        }
                    )
            return {
                "status": "success",
                "provider": "google_tasks",
                "count": len(items),
                "items": items,
                "list_id": tasklist_id,
                "query": query,
                "max_results": max_results,
                "include_completed": include_completed,
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _create_google_task(
        cls,
        payload: Dict[str, Any],
        *,
        title: str,
        notes: str,
        due: str,
        status: str,
    ) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required for Google Tasks."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        tasklist_id = str(payload.get("tasklist_id") or payload.get("task_list_id") or payload.get("list_id") or "@default").strip() or "@default"
        endpoint = f"https://tasks.googleapis.com/tasks/v1/lists/{tasklist_id}/tasks"
        body: Dict[str, Any] = {"title": title}
        if notes:
            body["notes"] = notes
        due_value = cls._normalize_due_datetime(due, default_hour=17)
        if due_value:
            body["due"] = due_value
        status_value = cls._normalize_task_status(status, provider_hint="google_tasks")
        if status_value:
            body["status"] = status_value
        try:
            response = requests.post(
                endpoint,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=body,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Google Tasks create error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            return {
                "status": "success",
                "provider": "google_tasks",
                "task_id": str(data.get("id", "")),
                "title": str(data.get("title", title)),
                "status_value": str(data.get("status", status_value)),
                "due": str(data.get("due", due_value)),
                "list_id": tasklist_id,
                "web_url": str(data.get("selfLink", "")),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _update_google_task(
        cls,
        payload: Dict[str, Any],
        *,
        task_id: str,
        title: str,
        notes: str,
        due: str,
        status: str,
    ) -> Dict[str, Any]:
        token = cls._token(payload, ["google_access_token", "access_token"], ["GOOGLE_OAUTH_ACCESS_TOKEN", "GOOGLE_ACCESS_TOKEN"], provider="google")
        if not token:
            return {"status": "error", "message": "Google access token is required for Google Tasks."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        tasklist_id = str(payload.get("tasklist_id") or payload.get("task_list_id") or payload.get("list_id") or "@default").strip() or "@default"
        endpoint = f"https://tasks.googleapis.com/tasks/v1/lists/{tasklist_id}/tasks/{task_id}"
        patch_body: Dict[str, Any] = {}
        if title:
            patch_body["title"] = title
        if notes:
            patch_body["notes"] = notes
        due_value = cls._normalize_due_datetime(due, default_hour=17)
        if due_value:
            patch_body["due"] = due_value
        status_value = cls._normalize_task_status(status, provider_hint="google_tasks")
        if status_value:
            patch_body["status"] = status_value
        try:
            response = requests.patch(
                endpoint,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=patch_body,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Google Tasks update error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            return {
                "status": "success",
                "provider": "google_tasks",
                "task_id": str(data.get("id", task_id)),
                "title": str(data.get("title", title)),
                "status_value": str(data.get("status", status_value)),
                "due": str(data.get("due", due_value)),
                "list_id": tasklist_id,
                "web_url": str(data.get("selfLink", "")),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _list_graph_todo_tasks(
        cls,
        payload: Dict[str, Any],
        *,
        max_results: int,
        include_completed: bool,
        query: str,
    ) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        list_id = cls._resolve_graph_todo_list_id(payload, token=token)
        if not list_id:
            return {"status": "error", "message": "Unable to resolve Microsoft To Do list id."}
        endpoint = f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks"
        params: Dict[str, Any] = {"$top": max_results}
        if not include_completed:
            params["$filter"] = "status ne 'completed'"
        try:
            response = requests.get(
                endpoint,
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Graph To Do list error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            rows = data.get("value", [])
            q = query.lower()
            items: List[Dict[str, Any]] = []
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    title = str(row.get("title", "")).strip()
                    body_node = row.get("body")
                    notes = ""
                    if isinstance(body_node, dict):
                        notes = str(body_node.get("content", "")).strip()
                    if q and q not in title.lower() and q not in notes.lower():
                        continue
                    due_node = row.get("dueDateTime")
                    due_value = ""
                    if isinstance(due_node, dict):
                        due_value = str(due_node.get("dateTime", "")).strip()
                    items.append(
                        {
                            "id": str(row.get("id", "")),
                            "title": title,
                            "notes": notes,
                            "status": str(row.get("status", "")),
                            "due": due_value,
                            "updated": str(row.get("lastModifiedDateTime", "")),
                            "list_id": list_id,
                            "web_url": str(row.get("webLink", "")),
                        }
                    )
            return {
                "status": "success",
                "provider": "microsoft_graph_todo",
                "count": len(items),
                "items": items,
                "list_id": list_id,
                "query": query,
                "max_results": max_results,
                "include_completed": include_completed,
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _create_graph_todo_task(
        cls,
        payload: Dict[str, Any],
        *,
        title: str,
        notes: str,
        due: str,
        status: str,
    ) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        list_id = cls._resolve_graph_todo_list_id(payload, token=token)
        if not list_id:
            return {"status": "error", "message": "Unable to resolve Microsoft To Do list id."}
        endpoint = f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks"
        body: Dict[str, Any] = {"title": title}
        if notes:
            body["body"] = {"contentType": "text", "content": notes}
        status_value = cls._normalize_task_status(status, provider_hint="graph_todo")
        if status_value:
            body["status"] = status_value
        due_value = cls._normalize_due_datetime(due, default_hour=17)
        if due_value:
            due_dt = datetime.fromisoformat(due_value.replace("Z", "+00:00")).astimezone(timezone.utc)
            body["dueDateTime"] = {"dateTime": due_dt.strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": "UTC"}
        try:
            response = requests.post(
                endpoint,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=body,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Graph To Do create error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            due_node = data.get("dueDateTime")
            due_out = ""
            if isinstance(due_node, dict):
                due_out = str(due_node.get("dateTime", ""))
            return {
                "status": "success",
                "provider": "microsoft_graph_todo",
                "task_id": str(data.get("id", "")),
                "title": str(data.get("title", title)),
                "status_value": str(data.get("status", status_value)),
                "due": due_out or due_value,
                "list_id": list_id,
                "web_url": str(data.get("webLink", "")),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _update_graph_todo_task(
        cls,
        payload: Dict[str, Any],
        *,
        task_id: str,
        title: str,
        notes: str,
        due: str,
        status: str,
    ) -> Dict[str, Any]:
        token = cls._token(payload, ["graph_access_token", "access_token"], ["MICROSOFT_GRAPH_ACCESS_TOKEN"], provider="graph")
        if not token:
            return {"status": "error", "message": "Microsoft Graph access token is required."}
        requests = cls._requests()
        if requests is None:
            return {"status": "error", "message": "requests package is unavailable."}
        list_id = cls._resolve_graph_todo_list_id(payload, token=token)
        if not list_id:
            return {"status": "error", "message": "Unable to resolve Microsoft To Do list id."}
        endpoint = f"https://graph.microsoft.com/v1.0/me/todo/lists/{list_id}/tasks/{task_id}"
        patch_body: Dict[str, Any] = {}
        if title:
            patch_body["title"] = title
        if notes:
            patch_body["body"] = {"contentType": "text", "content": notes}
        status_value = cls._normalize_task_status(status, provider_hint="graph_todo")
        if status_value:
            patch_body["status"] = status_value
        due_value = cls._normalize_due_datetime(due, default_hour=17)
        if due_value:
            due_dt = datetime.fromisoformat(due_value.replace("Z", "+00:00")).astimezone(timezone.utc)
            patch_body["dueDateTime"] = {"dateTime": due_dt.strftime("%Y-%m-%dT%H:%M:%S"), "timeZone": "UTC"}
        try:
            response = requests.patch(
                endpoint,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json=patch_body,
                timeout=25,
            )
            if response.status_code >= 300:
                return {"status": "error", "message": f"Graph To Do update error: {response.status_code} {response.text[:200]}"}
            data = response.json() if response.text else {}
            due_node = data.get("dueDateTime")
            due_out = ""
            if isinstance(due_node, dict):
                due_out = str(due_node.get("dateTime", ""))
            return {
                "status": "success",
                "provider": "microsoft_graph_todo",
                "task_id": str(data.get("id", task_id)),
                "title": str(data.get("title", title)),
                "status_value": str(data.get("status", status_value)),
                "due": due_out or due_value,
                "list_id": list_id,
                "web_url": str(data.get("webLink", "")),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": str(exc)}

    @classmethod
    def _resolve_graph_todo_list_id(cls, payload: Dict[str, Any], *, token: str) -> str:
        explicit_list_id = str(payload.get("todo_list_id") or payload.get("tasklist_id") or payload.get("list_id") or "").strip()
        if explicit_list_id:
            return explicit_list_id
        requests = cls._requests()
        if requests is None:
            return ""
        try:
            response = requests.get(
                "https://graph.microsoft.com/v1.0/me/todo/lists",
                headers={"Authorization": f"Bearer {token}"},
                params={"$top": 50},
                timeout=25,
            )
            if response.status_code >= 300:
                return ""
            data = response.json() if response.text else {}
            rows = data.get("value", [])
            if not isinstance(rows, list):
                return ""
            chosen = ""
            fallback = ""
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_id = str(row.get("id", "")).strip()
                if not row_id:
                    continue
                if not fallback:
                    fallback = row_id
                name = str(row.get("displayName", "")).strip().lower()
                if name in {"tasks", "to do", "todo"}:
                    chosen = row_id
                    break
            return chosen or fallback
        except Exception:
            return ""

    @staticmethod
    def _normalize_due_datetime(raw_value: str, *, default_hour: int = 17) -> str:
        value = str(raw_value or "").strip()
        if not value:
            return ""
        try:
            if len(value) == 10 and value.count("-") == 2:
                value = f"{value}T{default_hour:02d}:00:00+00:00"
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            return str(raw_value).strip()

    @staticmethod
    def _normalize_task_status(raw_status: Any, *, provider_hint: str) -> str:
        text = str(raw_status or "").strip().lower()
        if not text:
            return ""
        done_values = {"done", "complete", "completed", "closed", "finished", "resolved"}
        open_values = {"todo", "open", "pending", "active", "in_progress", "notstarted", "not_started", "needsaction"}
        provider = str(provider_hint or "").strip().lower()
        if provider in {"google_tasks", "google", "gtasks"}:
            if text in done_values:
                return "completed"
            if text in open_values:
                return "needsAction"
            if text in {"completed", "needsaction"}:
                return "completed" if text == "completed" else "needsAction"
            return ""
        if text in done_values:
            return "completed"
        if text in open_values:
            return "notStarted"
        if text in {"completed", "notstarted"}:
            return "completed" if text == "completed" else "notStarted"
        return ""

    @staticmethod
    def _requests():
        try:
            import requests  # type: ignore

            return requests
        except Exception:
            return None

    @classmethod
    def _token(
        cls,
        payload: Dict[str, Any],
        payload_keys: List[str],
        env_keys: List[str],
        *,
        provider: str = "",
    ) -> str:
        for key in payload_keys:
            value = payload.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        for key in env_keys:
            text = str(os.getenv(key, "")).strip()
            if text:
                return text
        normalized_provider = str(provider or "").strip().lower()
        if normalized_provider:
            account_id = str(payload.get("oauth_account_id") or payload.get("account_id") or "default").strip().lower() or "default"
            min_ttl_s = cls._to_int(payload.get("oauth_min_ttl_s", 120), default=120, minimum=0, maximum=86400)
            store = OAuthTokenStore.shared()
            for alias in cls._provider_aliases(normalized_provider):
                resolved = store.resolve_access_token(
                    provider=alias,
                    account_id=account_id,
                    min_ttl_s=min_ttl_s,
                    auto_refresh=True,
                )
                if resolved.get("status") == "success":
                    token = str(resolved.get("access_token", "")).strip()
                    if token:
                        return token
        return ""

    @staticmethod
    def _provider_aliases(provider: str) -> List[str]:
        normalized = str(provider or "").strip().lower()
        if normalized in {"graph", "microsoft_graph", "microsoft"}:
            return ["graph", "microsoft_graph", "microsoft"]
        if normalized in {"google", "gmail", "google_oauth"}:
            return ["google", "gmail"]
        return [normalized]

    @staticmethod
    def _normalize_recipients(value: Any) -> List[str]:
        if value is None:
            return []
        raw: List[str]
        if isinstance(value, str):
            raw = [part.strip() for part in value.replace(";", ",").split(",")]
        elif isinstance(value, list):
            raw = [str(item).strip() for item in value]
        else:
            raw = [str(value).strip()]
        return [item for item in raw if item]

    @staticmethod
    def _extract_google_doc_text(payload: Dict[str, Any]) -> str:
        body = payload.get("body")
        if not isinstance(body, dict):
            return ""
        content = body.get("content")
        if not isinstance(content, list):
            return ""
        parts: List[str] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            paragraph = block.get("paragraph")
            if not isinstance(paragraph, dict):
                continue
            elements = paragraph.get("elements")
            if not isinstance(elements, list):
                continue
            for element in elements:
                if not isinstance(element, dict):
                    continue
                text_run = element.get("textRun")
                if not isinstance(text_run, dict):
                    continue
                text = str(text_run.get("content", ""))
                if text:
                    parts.append(text)
        return "".join(parts).strip()

    @staticmethod
    def _resolve_event_window(start_iso: str, end_iso: str) -> Tuple[datetime, datetime]:
        now = datetime.now(timezone.utc)

        def _parse(value: str) -> datetime | None:
            if not value:
                return None
            try:
                parsed = datetime.fromisoformat(value)
            except Exception:
                return None
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed

        start = _parse(start_iso) or (now + timedelta(minutes=5))
        end = _parse(end_iso) or (start + timedelta(minutes=30))
        if end <= start:
            end = start + timedelta(minutes=30)
        return (start, end)

    @staticmethod
    def _to_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value)
        except Exception:
            parsed = default
        return max(minimum, min(maximum, parsed))
