from __future__ import annotations

from backend.python.core.contracts import ActionResult
from backend.python.core.runtime_memory import RuntimeMemory


def test_remember_and_recent_hints(tmp_path) -> None:
    store = tmp_path / "runtime_memory.jsonl"
    memory = RuntimeMemory(max_items=50, store_path=str(store))

    memory.remember_goal(
        text="open notepad and write notes",
        status="completed",
        results=[ActionResult(action="open_app", status="success", output={"status": "success"})],
    )

    hints = memory.recent_hints(limit=5)
    assert hints
    assert hints[-1]["text"] == "open notepad and write notes"
    assert hints[-1]["status"] == "completed"


def test_search_returns_best_matches(tmp_path) -> None:
    store = tmp_path / "runtime_memory.jsonl"
    memory = RuntimeMemory(max_items=50, store_path=str(store))

    memory.remember_goal(
        text="what time is it in UTC",
        status="completed",
        results=[ActionResult(action="time_now", status="success", output={"status": "success"})],
    )
    memory.remember_goal(
        text="copy file report.txt",
        status="failed",
        results=[ActionResult(action="copy_file", status="failed", error="missing source", output={"status": "error"})],
    )

    rows = memory.search("time utc", limit=3)
    assert rows
    assert rows[0]["text"] == "what time is it in UTC"
    assert "memory_score" in rows[0]


def test_memory_persistence_loads_existing_records(tmp_path) -> None:
    store = tmp_path / "runtime_memory.jsonl"
    memory = RuntimeMemory(max_items=50, store_path=str(store))
    memory.remember_goal(
        text="open calculator",
        status="completed",
        results=[ActionResult(action="open_app", status="success", output={"status": "success"})],
    )

    loaded = RuntimeMemory(max_items=50, store_path=str(store))
    rows = loaded.recent_hints(limit=10)
    assert any(item.get("text") == "open calculator" for item in rows)


def test_runtime_memory_extracts_external_repair_signals_and_uses_them_for_search(tmp_path) -> None:
    store = tmp_path / "runtime_memory.jsonl"
    memory = RuntimeMemory(max_items=50, store_path=str(store))
    memory.remember_goal(
        text="read document doc-42",
        status="completed",
        metadata={"source": "desktop-ui", "policy_profile": "automation_safe"},
        results=[
            ActionResult(
                action="external_doc_read",
                status="success",
                output={"status": "success"},
                evidence={
                    "request": {"args": {"provider": "google", "document_id": "doc-42"}},
                    "external_reliability_preflight": {
                        "status": "ok",
                        "provider_routing": {"selected_provider": "google"},
                        "contract_diagnostic": {"code": "provider_not_supported_for_action"},
                    },
                },
            )
        ],
    )

    hints = memory.recent_hints(limit=2)
    assert hints
    latest = hints[-1]
    assert latest.get("source") == "desktop-ui"
    repair_signals = latest.get("repair_signals", [])
    assert isinstance(repair_signals, list)
    assert repair_signals and repair_signals[0].get("action") == "external_doc_read"
    assert repair_signals[0].get("provider") == "google"

    rows = memory.search("repair external_doc_read google provider", limit=3)
    assert rows
    assert rows[0].get("text") == "read document doc-42"
