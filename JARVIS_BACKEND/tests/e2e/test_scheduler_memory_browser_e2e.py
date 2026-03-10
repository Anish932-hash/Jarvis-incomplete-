from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from backend.python.desktop_api import DesktopBackendService, JarvisAPIHandler, JarvisHTTPServer
from tests.helpers.http_client import request_json


class _WebPageHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        body = (
            "<html><head><title>Jarvis Local Page</title></head>"
            "<body><h1>Local Test</h1>"
            '<a href="/alpha">Alpha</a>'
            '<a href="https://example.com/beta">Beta</a>'
            "</body></html>"
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


@pytest.fixture(scope="module")
def live_api_server(tmp_path_factory: pytest.TempPathFactory) -> str:
    root = tmp_path_factory.mktemp("jarvis_e2e_state")
    os.environ["JARVIS_ENABLE_LLM_PLANNER"] = "0"
    os.environ.pop("GROQ_API_KEY", None)
    os.environ.pop("NVIDIA_API_KEY", None)
    os.environ["JARVIS_BROWSER_ALLOW_PRIVATE_HOSTS"] = "1"
    os.environ["JARVIS_SCHEDULE_STORE"] = str(Path(root) / "schedules.json")
    os.environ["JARVIS_RUNTIME_MEMORY_STORE"] = str(Path(root) / "runtime_memory.jsonl")

    service = DesktopBackendService()
    service.start()

    server = JarvisHTTPServer(("127.0.0.1", 0), JarvisAPIHandler, service)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    host, port = server.server_address

    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        service.stop()


@pytest.fixture(scope="module")
def local_web_server() -> str:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _WebPageHandler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    host, port = server.server_address
    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _wait_for_schedule(base_url: str, schedule_id: str, timeout_s: float = 20.0) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        status, payload = request_json("GET", f"{base_url}/schedules/{schedule_id}", timeout_s=10)
        assert status == 200
        schedule = payload.get("schedule", {})
        if schedule.get("status") in {"completed", "failed", "cancelled"}:
            return schedule
        time.sleep(0.2)
    raise AssertionError("Timed out waiting for schedule completion")


@pytest.mark.e2e
def test_schedule_executes_and_updates_checkpoint(live_api_server: str) -> None:
    run_at = (datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat()
    status, payload = request_json(
        "POST",
        f"{live_api_server}/schedules",
        payload={
            "text": "what time is it in UTC",
            "run_at": run_at,
            "max_attempts": 2,
            "retry_delay_s": 10,
        },
        timeout_s=20,
    )
    assert status == 200
    schedule = payload.get("schedule", {})
    schedule_id = str(schedule.get("schedule_id", ""))
    assert schedule_id

    final = _wait_for_schedule(live_api_server, schedule_id, timeout_s=25)
    assert final.get("status") == "completed"
    goal_id = str(final.get("last_goal_id", ""))
    assert goal_id

    status, goal = request_json("GET", f"{live_api_server}/goals/{goal_id}", timeout_s=20)
    assert status == 200
    assert goal.get("status") == "completed"


@pytest.mark.e2e
def test_memory_query_returns_recent_goal_context(live_api_server: str) -> None:
    status, _ = request_json(
        "POST",
        f"{live_api_server}/goals",
        payload={"text": "what time is it in UTC", "wait": True, "timeout_s": 12},
        timeout_s=20,
    )
    assert status == 200

    status, memory = request_json("GET", f"{live_api_server}/memory?query=time&limit=8", timeout_s=20)
    assert status == 200
    assert memory.get("count", 0) >= 1
    items = memory.get("items", [])
    assert isinstance(items, list) and items
    assert any("time" in str(item.get("text", "")).lower() for item in items if isinstance(item, dict))


@pytest.mark.e2e
def test_schedule_pause_resume_and_run_now_routes(live_api_server: str) -> None:
    run_at = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    status, payload = request_json(
        "POST",
        f"{live_api_server}/schedules",
        payload={"text": "what time is it in UTC", "run_at": run_at, "max_attempts": 2, "retry_delay_s": 10},
        timeout_s=20,
    )
    assert status == 200
    schedule_id = str(payload.get("schedule", {}).get("schedule_id", ""))
    assert schedule_id

    status, paused = request_json("POST", f"{live_api_server}/schedules/{schedule_id}/pause", payload={}, timeout_s=20)
    assert status == 200
    assert paused.get("status") == "success"
    assert paused.get("schedule", {}).get("status") == "paused"

    time.sleep(1.0)
    status, fetched = request_json("GET", f"{live_api_server}/schedules/{schedule_id}", timeout_s=20)
    assert status == 200
    assert fetched.get("schedule", {}).get("status") == "paused"

    status, resumed = request_json("POST", f"{live_api_server}/schedules/{schedule_id}/resume", payload={}, timeout_s=20)
    assert status == 200
    assert resumed.get("status") == "success"
    assert resumed.get("schedule", {}).get("status") == "pending"

    status, immediate = request_json("POST", f"{live_api_server}/schedules/{schedule_id}/run-now", payload={}, timeout_s=20)
    assert status == 200
    assert immediate.get("status") == "success"

    final = _wait_for_schedule(live_api_server, schedule_id, timeout_s=25)
    assert final.get("status") == "completed"


@pytest.mark.e2e
def test_trigger_interval_dispatch_and_cancel(live_api_server: str) -> None:
    status, created = request_json(
        "POST",
        f"{live_api_server}/triggers",
        payload={"text": "what time is it in UTC", "interval_s": 5},
        timeout_s=20,
    )
    assert status == 200
    trigger = created.get("trigger", {})
    trigger_id = str(trigger.get("trigger_id", ""))
    assert trigger_id

    deadline = time.time() + 15
    fired = False
    while time.time() < deadline:
        status, fetched = request_json("GET", f"{live_api_server}/triggers/{trigger_id}", timeout_s=20)
        assert status == 200
        current = fetched.get("trigger", {})
        if int(current.get("run_count", 0)) >= 1:
            fired = True
            break
        time.sleep(0.4)
    assert fired is True

    status, cancelled = request_json("POST", f"{live_api_server}/triggers/{trigger_id}/cancel", payload={}, timeout_s=20)
    assert status == 200
    assert cancelled.get("status") == "success"
    assert cancelled.get("trigger", {}).get("status") == "cancelled"


@pytest.mark.e2e
def test_browser_dom_and_link_extraction_actions(live_api_server: str, local_web_server: str) -> None:
    status, read_result = request_json(
        "POST",
        f"{live_api_server}/actions",
        payload={"action": "browser_read_dom", "args": {"url": local_web_server, "max_chars": 3000}},
        timeout_s=20,
    )
    assert status == 200
    assert read_result.get("status") == "success"
    output = read_result.get("output", {})
    assert isinstance(output, dict)
    assert "Jarvis Local Page" in str(output.get("title", ""))

    status, link_result = request_json(
        "POST",
        f"{live_api_server}/actions",
        payload={
            "action": "browser_extract_links",
            "args": {"url": local_web_server, "same_domain_only": True, "max_links": 20},
        },
        timeout_s=20,
    )
    assert status == 200
    assert link_result.get("status") == "success"
    output = link_result.get("output", {})
    assert isinstance(output, dict)
    assert int(output.get("count", 0)) >= 1


@pytest.mark.e2e
def test_computer_assert_text_visible_requires_approval(live_api_server: str) -> None:
    status, first = request_json(
        "POST",
        f"{live_api_server}/actions",
        payload={"action": "computer_assert_text_visible", "args": {"text": "JARVIS"}},
        timeout_s=20,
    )
    assert status == 200
    assert first.get("status") == "blocked"

    output = first.get("output", {})
    if isinstance(output, dict) and output.get("approval_required") is True:
        approval = output.get("approval", {})
        approval_id = str((approval or {}).get("approval_id", ""))
        assert approval_id

        status, approved = request_json(
            "POST",
            f"{live_api_server}/approvals/{approval_id}/approve",
            payload={"note": "e2e approve computer_assert"},
            timeout_s=20,
        )
        assert status == 200
        assert approved.get("status") == "success"

        status, second = request_json(
            "POST",
            f"{live_api_server}/actions",
            payload={
                "action": "computer_assert_text_visible",
                "args": {"text": "JARVIS"},
                "approval_id": approval_id,
            },
            timeout_s=20,
        )
        assert status == 200
        assert second.get("status") in {"success", "failed", "blocked"}
        output = second.get("output")
        if isinstance(output, dict):
            assert output.get("approval_required") is not True
