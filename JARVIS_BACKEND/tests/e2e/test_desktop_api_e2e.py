from __future__ import annotations

import os
import threading

import pytest

from backend.python.desktop_api import DesktopBackendService, JarvisAPIHandler, JarvisHTTPServer
from tests.helpers.http_client import request_json


@pytest.fixture(scope="module")
def live_api_server() -> str:
    os.environ["JARVIS_ENABLE_LLM_PLANNER"] = "0"
    os.environ.pop("GROQ_API_KEY", None)
    os.environ.pop("NVIDIA_API_KEY", None)

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


@pytest.mark.e2e
def test_live_goal_execution_time_query(live_api_server: str) -> None:
    status, payload = request_json(
        "POST",
        f"{live_api_server}/goals",
        payload={"text": "what time is it in UTC", "wait": True, "timeout_s": 12},
        timeout_s=20,
    )
    assert status == 200
    goal = payload.get("goal", {})
    assert goal.get("status") == "completed"
    results = goal.get("results", [])
    assert results
    first = results[0]
    assert first.get("action") == "time_now"
    assert first.get("status") == "success"
    assert "iso" in (first.get("output") or {})


@pytest.mark.e2e
def test_live_high_risk_action_requires_and_uses_approval(live_api_server: str) -> None:
    source = r"C:\Users\Public\jarvis_missing_source.txt"
    destination = r"C:\Users\Public\jarvis_missing_destination.txt"

    status, first = request_json(
        "POST",
        f"{live_api_server}/actions",
        payload={"action": "copy_file", "args": {"source": source, "destination": destination}},
        timeout_s=20,
    )
    assert status == 200
    assert first.get("status") == "blocked"
    assert (first.get("output") or {}).get("approval_required") is True

    approval = (first.get("output") or {}).get("approval") or {}
    approval_id = approval.get("approval_id")
    assert isinstance(approval_id, str) and approval_id

    status, approved = request_json(
        "POST",
        f"{live_api_server}/approvals/{approval_id}/approve",
        payload={"note": "approved by automated test"},
        timeout_s=20,
    )
    assert status == 200
    assert approved.get("status") == "success"

    status, second = request_json(
        "POST",
        f"{live_api_server}/actions",
        payload={
            "action": "copy_file",
            "args": {"source": source, "destination": destination},
            "approval_id": approval_id,
        },
        timeout_s=20,
    )
    assert status == 200
    assert second.get("status") in {"failed", "blocked", "success"}
    output = second.get("output")
    if isinstance(output, dict):
        assert output.get("approval_required") is not True


@pytest.mark.e2e
def test_live_plan_preview_route_returns_steps(live_api_server: str) -> None:
    status, payload = request_json(
        "POST",
        f"{live_api_server}/plans/preview",
        payload={"text": "what time is it in UTC"},
        timeout_s=20,
    )
    assert status == 200
    assert payload.get("status") == "success"
    plan = payload.get("plan", {})
    assert isinstance(plan, dict)
    assert int(plan.get("step_count", 0)) >= 1
    steps = plan.get("steps", [])
    assert isinstance(steps, list) and steps


@pytest.mark.e2e
def test_live_goal_emits_progress_telemetry(live_api_server: str) -> None:
    status, payload = request_json(
        "POST",
        f"{live_api_server}/goals",
        payload={"text": "what time is it in UTC", "source": "desktop-ui", "wait": True, "timeout_s": 12},
        timeout_s=20,
    )
    assert status == 200
    goal_id = str(payload.get("goal_id", "")).strip()
    assert goal_id

    status, telemetry_payload = request_json(
        "GET",
        f"{live_api_server}/telemetry/events?event=goal.progress&limit=200",
        timeout_s=20,
    )
    assert status == 200
    rows = telemetry_payload.get("items", [])
    assert isinstance(rows, list)

    matching = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        payload_row = item.get("payload")
        if not isinstance(payload_row, dict):
            continue
        if str(payload_row.get("goal_id", "")).strip() != goal_id:
            continue
        matching.append(payload_row)

    assert matching, f"Expected at least one goal.progress event for goal_id={goal_id}"
    latest = matching[-1]
    assert int(latest.get("completed_steps", 0)) >= 1
    assert int(latest.get("success_steps", 0)) >= 1
    assert float(latest.get("elapsed_s", 0.0)) >= 0.0
    assert "throughput_steps_per_min" in latest
