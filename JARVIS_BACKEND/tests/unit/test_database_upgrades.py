from __future__ import annotations

import time

from backend.python.database.local_store import LocalStore
from backend.python.database.memory_db import MemoryDB


def test_local_store_transaction_rollback_on_strict_failure(tmp_path) -> None:
    path = tmp_path / "local_store.json"
    store = LocalStore(str(path))
    store.set("alpha", {"value": 1})
    store.set("beta", {"value": 2})

    result = store.transact(
        [
            {"action": "set", "key": "alpha", "value": {"value": 5}},
            {"action": "compare_and_set", "key": "beta", "expected_value": {"value": 999}, "value": {"value": 3}},
        ],
        strict=True,
    )

    assert result["status"] == "rolled_back"
    assert store.get("alpha") == {"value": 1}
    assert store.get("beta") == {"value": 2}


def test_local_store_ttl_cleanup_and_metadata(tmp_path) -> None:
    path = tmp_path / "ttl_store.json"
    store = LocalStore(str(path))
    write = store.set("ephemeral", "hello", ttl_s=0.05)
    assert write["status"] == "success"
    snapshot = store.get_with_meta("ephemeral")
    assert snapshot["found"] is True
    assert isinstance(snapshot["meta"].get("version"), int)

    time.sleep(0.08)
    removed = store.cleanup_expired()
    assert removed >= 1
    assert store.get("ephemeral") is None


def test_memory_db_hybrid_search_filters_and_feedback() -> None:
    db = MemoryDB(max_entries=50)
    db.store(
        "m1",
        "prepare weekly project report for leadership",
        [1.0, 0.0, 0.0],
        tags=["work", "report"],
        priority=0.2,
    )
    db.store(
        "m2",
        "buy milk and bread from grocery store",
        [0.0, 1.0, 0.0],
        tags=["personal"],
        priority=0.1,
    )

    rows = db.hybrid_search(
        query_text="weekly report",
        query_embed=[1.0, 0.0, 0.0],
        top_k=5,
        must_tags=["report"],
    )
    assert rows
    assert rows[0]["id"] == "m1"

    before = float(db.entries["m1"]["priority"])
    db.record_feedback("m1", success=False, reward=-0.05)
    after_fail = float(db.entries["m1"]["priority"])
    assert after_fail < before
    db.record_feedback("m1", success=True, reward=0.1)
    after_success = float(db.entries["m1"]["priority"])
    assert after_success > after_fail


def test_memory_db_adaptive_profile_tunes_after_negative_feedback() -> None:
    db = MemoryDB(max_entries=40)
    db.store("m1", "draft release notes and publish summary", [1.0, 0.1, 0.0], tags=["work", "docs"], priority=0.2)

    for _ in range(6):
        payload = db.record_feedback("m1", success=False, reward=-0.18)
        assert payload["status"] == "success"

    profile = db.adaptive_profile()
    assert profile["status"] == "success"
    weights = profile["weights"]
    assert float(weights["semantic_weight"]) < 0.72
    assert float(weights["lexical_weight"]) > 0.14
    assert float(weights["recency_weight"]) >= 0.09


def test_memory_db_search_with_diagnostics_supports_diversification() -> None:
    db = MemoryDB(max_entries=100)
    db.store("a1", "create monthly report for leadership", [1.0, 0.0, 0.0], tags=["report"], metadata={"app": "excel"})
    db.store("a2", "share report summary by email", [0.95, 0.05, 0.0], tags=["email"], metadata={"app": "outlook"})
    db.store("a3", "report dashboard follow-up", [0.9, 0.1, 0.0], tags=["report"], metadata={"app": "excel"})
    db.store("a4", "prepare release checklist", [0.7, 0.3, 0.0], tags=["checklist"], metadata={"app": "notion"})

    payload = db.search_with_diagnostics(
        query_text="report summary",
        query_embed=[1.0, 0.0, 0.0],
        top_k=3,
        strategy="adaptive",
        diversify_by="tag",
        max_per_group=1,
        prefer_tags=["report", "email"],
    )

    assert payload["status"] == "success"
    assert int(payload["count"]) >= 2
    items = payload["items"]
    assert isinstance(items, list) and items
    assert all("score_components" in row for row in items)
    first_tags = [str((row.get("tags") or [""])[0]) for row in items if isinstance(row.get("tags"), list) and row.get("tags")]
    assert len(set(first_tags)) >= 2
    journal_tail = payload.get("search_journal_tail", [])
    assert isinstance(journal_tail, list) and journal_tail
