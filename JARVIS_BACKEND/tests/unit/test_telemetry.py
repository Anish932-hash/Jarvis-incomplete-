from __future__ import annotations

import json

from backend.python.core.telemetry import Telemetry


def test_telemetry_records_events_and_filters_by_after_id() -> None:
    telemetry = Telemetry(max_events=20)
    telemetry.emit("goal.started", {"goal_id": "g-1"})
    second = telemetry.emit("goal.completed", {"goal_id": "g-1"})
    telemetry.emit("goal.started", {"goal_id": "g-2"})

    payload = telemetry.list_events(event="goal.started", after_id=int(second["event_id"]), limit=10)
    assert payload["count"] == 1
    assert payload["items"][0]["event"] == "goal.started"
    assert payload["items"][0]["payload"]["goal_id"] == "g-2"


def test_telemetry_respects_max_events_buffer() -> None:
    telemetry = Telemetry(max_events=3)
    for index in range(6):
        telemetry.emit("tick", {"n": index})

    payload = telemetry.list_events(limit=10)
    assert payload["count"] == 3
    ids = [int(item["event_id"]) for item in payload["items"]]
    assert ids == [4, 5, 6]


def test_telemetry_persistence_and_summary(monkeypatch, tmp_path) -> None:
    store = tmp_path / "telemetry.jsonl"
    monkeypatch.setenv("JARVIS_TELEMETRY_PERSIST", "1")
    monkeypatch.setenv("JARVIS_TELEMETRY_STORE", str(store))
    monkeypatch.setenv("JARVIS_TELEMETRY_PERSIST_BATCH", "2")
    monkeypatch.setenv("JARVIS_TELEMETRY_PERSIST_INTERVAL_S", "30")

    telemetry = Telemetry(max_events=20)
    telemetry.emit("goal.started", {"goal_id": "g-1", "status": "running"})
    telemetry.emit("goal.failed", {"goal_id": "g-1", "status": "failed"})

    flush = telemetry.flush()
    assert flush["status"] == "success"
    assert store.exists() is True

    lines = [line for line in store.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 2
    decoded = [json.loads(line) for line in lines]
    assert decoded[0]["event"] == "goal.started"
    assert decoded[1]["event"] == "goal.failed"

    summary = telemetry.summary(limit=100)
    assert summary["status"] == "success"
    assert summary["count"] == 2
    assert summary["failure_events"] == 1
    assert summary["persist"]["enabled"] is True
    assert summary["persist"]["pending"] == 0
