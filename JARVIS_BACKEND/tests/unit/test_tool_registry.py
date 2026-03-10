from __future__ import annotations

import asyncio

from backend.python.core.contracts import ActionRequest
from backend.python.core.tool_registry import ToolRegistry
from backend.python.tools.route_handlers import register_tools


def test_register_unknown_risk_defaults_to_low() -> None:
    registry = ToolRegistry()
    registry.register("demo_tool", lambda _args: {"status": "success"}, risk="SEVERE")

    definition = registry.get("demo_tool")
    assert definition is not None
    assert definition.risk == "low"


def test_execute_checks_required_args() -> None:
    registry = ToolRegistry()
    registry.register(
        "copy_file",
        lambda _args: {"status": "success"},
        required_args=["source", "destination"],
    )
    request = ActionRequest(action="copy_file", args={"source": "a.txt"})

    result = asyncio.run(registry.execute(request))
    assert result.status == "failed"
    assert "Missing required args" in (result.error or "")
    assert result.evidence.get("missing_args") == ["destination"]


def test_execute_non_dict_args_returns_failed() -> None:
    registry = ToolRegistry()
    registry.register("echo", lambda _args: {"status": "success"})
    request = ActionRequest(action="echo", args=[])  # type: ignore[arg-type]

    result = asyncio.run(registry.execute(request))
    assert result.status == "failed"
    assert "JSON object" in (result.error or "")


def test_execute_propagates_blocked_status_from_handler() -> None:
    registry = ToolRegistry()
    registry.register("guarded_action", lambda _args: {"status": "blocked", "message": "policy denied"})
    request = ActionRequest(action="guarded_action", args={})

    result = asyncio.run(registry.execute(request))
    assert result.status == "blocked"
    assert result.error == "policy denied"


def test_route_handlers_register_required_args_for_critical_tools() -> None:
    registry = ToolRegistry()
    register_tools(registry)

    open_url = registry.get("open_url")
    write_file = registry.get("write_file")
    run_script = registry.get("run_trusted_script")
    click_text = registry.get("computer_click_text")
    click_target = registry.get("computer_click_target")
    send_email = registry.get("external_email_send")
    read_email = registry.get("external_email_read")
    update_event = registry.get("external_calendar_update_event")
    read_doc = registry.get("external_doc_read")
    update_doc = registry.get("external_doc_update")
    find_ui = registry.get("accessibility_find_element")
    browser_session_request = registry.get("browser_session_request")
    oauth_upsert = registry.get("oauth_token_upsert")
    oauth_maintain = registry.get("oauth_token_maintain")
    task_create = registry.get("external_task_create")
    task_update = registry.get("external_task_update")
    assert open_url is not None and "url" in open_url.required_args
    assert write_file is not None and set(write_file.required_args) == {"path", "content"}
    assert run_script is not None and set(run_script.required_args) == {"script_name"}
    assert click_text is not None and set(click_text.required_args) == {"query"}
    assert click_target is not None and set(click_target.required_args) == {"query"}
    assert send_email is not None and set(send_email.required_args) == {"to"}
    assert read_email is not None and set(read_email.required_args) == {"message_id"}
    assert update_event is not None and set(update_event.required_args) == {"event_id"}
    assert read_doc is not None and set(read_doc.required_args) == {"document_id"}
    assert update_doc is not None and set(update_doc.required_args) == {"document_id"}
    assert find_ui is not None and set(find_ui.required_args) == {"query"}
    assert browser_session_request is not None and set(browser_session_request.required_args) == {"session_id", "url"}
    assert oauth_upsert is not None and set(oauth_upsert.required_args) == {"provider", "access_token"}
    assert oauth_maintain is not None and set(oauth_maintain.required_args) == set()
    assert task_create is not None and set(task_create.required_args) == {"title"}
    assert task_update is not None and set(task_update.required_args) == {"task_id"}


def test_registered_write_file_tool_rejects_missing_required_content_before_handler() -> None:
    registry = ToolRegistry()
    register_tools(registry)
    request = ActionRequest(action="write_file", args={"path": "notes.txt"})

    result = asyncio.run(registry.execute(request))
    assert result.status == "failed"
    assert "Missing required args" in (result.error or "")
    assert "content" in result.evidence.get("missing_args", [])
