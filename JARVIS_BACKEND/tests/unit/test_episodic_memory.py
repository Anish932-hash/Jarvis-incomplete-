from __future__ import annotations

from backend.python.core.contracts import ActionResult
from backend.python.core.episodic_memory import EpisodicMemory


def test_episodic_memory_remember_and_search(tmp_path) -> None:
    store = tmp_path / "episodic_memory.jsonl"
    memory = EpisodicMemory(max_items=200, store_path=str(store), embedding_dim=96)

    memory.remember_goal(
        goal_id="goal-1",
        text="send an email to alice about status update",
        status="completed",
        source="desktop-ui",
        results=[ActionResult(action="external_email_send", status="success", output={"status": "success"})],
        metadata={"policy_profile": "interactive"},
    )
    memory.remember_goal(
        goal_id="goal-2",
        text="open notepad and write notes",
        status="completed",
        source="desktop-ui",
        results=[ActionResult(action="open_app", status="success", output={"status": "success"})],
        metadata={},
    )

    hits = memory.search("email alice", limit=3)
    assert hits
    assert hits[0]["memory_type"] == "episodic_semantic"
    assert "email" in hits[0]["text"].lower()
    assert isinstance(hits[0].get("memory_score"), float)


def test_episodic_memory_persistence_round_trip(tmp_path) -> None:
    store = tmp_path / "episodic_memory.jsonl"
    memory = EpisodicMemory(max_items=200, store_path=str(store), embedding_dim=64)

    memory.remember_goal(
        goal_id="goal-3",
        text="schedule meeting for planning",
        status="failed",
        source="desktop-trigger",
        results=[ActionResult(action="external_calendar_create_event", status="failed", error="missing token", output={"status": "error"})],
        metadata={"policy_profile": "automation_safe"},
    )

    loaded = EpisodicMemory(max_items=200, store_path=str(store), embedding_dim=64)
    recent = loaded.recent(limit=5)
    assert recent
    assert any(item.get("goal_id") == "goal-3" for item in recent)
    stats = loaded.stats()
    assert stats["count"] >= 1


def test_episodic_memory_search_with_policy_filters_tags_and_goal_exclusions(tmp_path) -> None:
    store = tmp_path / "episodic_memory.jsonl"
    memory = EpisodicMemory(max_items=200, store_path=str(store), embedding_dim=64)

    memory.remember_goal(
        goal_id="goal-dup",
        text="send email to alice about release",
        status="completed",
        source="desktop-ui",
        results=[ActionResult(action="external_email_send", status="success", output={"status": "success"})],
        metadata={"policy_profile": "interactive"},
    )
    memory.remember_goal(
        goal_id="goal-dup",
        text="send follow-up email to alice",
        status="completed",
        source="desktop-ui",
        results=[ActionResult(action="external_email_send", status="success", output={"status": "success"})],
        metadata={"policy_profile": "interactive"},
    )
    memory.remember_goal(
        goal_id="goal-keep",
        text="email bob project status",
        status="completed",
        source="desktop-ui",
        results=[ActionResult(action="external_email_send", status="success", output={"status": "success"})],
        metadata={"policy_profile": "automation_safe"},
    )
    memory.remember_goal(
        goal_id="goal-other",
        text="open notepad and write notes",
        status="completed",
        source="desktop-ui",
        results=[ActionResult(action="open_app", status="success", output={"status": "success"})],
        metadata={},
    )

    hits = memory.search_with_policy(
        "email status",
        limit=6,
        policy={
            "must_tags": ["action:external_email_send"],
            "exclude_goal_ids": ["goal-dup"],
            "diversify_by_goal": True,
        },
    )

    assert hits
    assert all(item.get("goal_id") != "goal-dup" for item in hits)
    assert all("external_email_send" in item.get("actions", []) for item in hits)


def test_episodic_memory_search_with_policy_can_disable_goal_diversification(tmp_path) -> None:
    store = tmp_path / "episodic_memory.jsonl"
    memory = EpisodicMemory(max_items=200, store_path=str(store), embedding_dim=64)

    memory.remember_goal(
        goal_id="goal-same",
        text="email alpha update",
        status="completed",
        source="desktop-ui",
        results=[ActionResult(action="external_email_send", status="success", output={"status": "success"})],
        metadata={},
    )
    memory.remember_goal(
        goal_id="goal-same",
        text="email alpha followup",
        status="completed",
        source="desktop-ui",
        results=[ActionResult(action="external_email_send", status="success", output={"status": "success"})],
        metadata={},
    )

    deduped = memory.search_with_policy(
        "email alpha",
        limit=5,
        policy={"must_tags": ["action:external_email_send"], "diversify_by_goal": True},
    )
    expanded = memory.search_with_policy(
        "email alpha",
        limit=5,
        policy={"must_tags": ["action:external_email_send"], "diversify_by_goal": False},
    )

    assert len(deduped) == 1
    assert len(expanded) >= 2


def test_episodic_memory_strategy_returns_recommended_and_avoid_actions(tmp_path) -> None:
    store = tmp_path / "episodic_memory.jsonl"
    memory = EpisodicMemory(max_items=300, store_path=str(store), embedding_dim=64)

    memory.remember_goal(
        goal_id="goal-a",
        text="send follow up email to alice",
        status="completed",
        source="desktop-ui",
        results=[ActionResult(action="external_email_send", status="success", output={"status": "success"})],
        metadata={},
    )
    memory.remember_goal(
        goal_id="goal-b",
        text="send email to bob about roadmap",
        status="completed",
        source="desktop-ui",
        results=[ActionResult(action="external_email_send", status="success", output={"status": "success"})],
        metadata={},
    )
    memory.remember_goal(
        goal_id="goal-c",
        text="read website content from internal dashboard",
        status="failed",
        source="desktop-ui",
        results=[ActionResult(action="browser_read_dom", status="failed", error="Timeout waiting for response", output={"status": "error"})],
        metadata={},
    )

    strategy = memory.strategy("", limit=10, min_score=0.01)

    assert strategy["status"] == "success"
    assert strategy["sample_count"] >= 2
    assert strategy["recommended_actions"]
    assert strategy["recommended_actions"][0]["action"] == "external_email_send"
    assert any(item.get("action") == "browser_read_dom" for item in strategy.get("avoid_actions", []))
    assert isinstance(strategy.get("strategy_hint"), str)


def test_episodic_memory_strategy_detects_failure_pattern_buckets(tmp_path) -> None:
    store = tmp_path / "episodic_memory.jsonl"
    memory = EpisodicMemory(max_items=200, store_path=str(store), embedding_dim=64)

    memory.remember_goal(
        goal_id="goal-f1",
        text="query slow endpoint",
        status="failed",
        source="desktop-ui",
        results=[ActionResult(action="browser_session_request", status="failed", error="Request timed out after 20 seconds", output={"status": "error"})],
        metadata={},
    )

    strategy = memory.strategy("slow endpoint", limit=6, min_score=0.0)

    patterns = strategy.get("failure_patterns", [])
    assert strategy["status"] == "success"
    assert isinstance(patterns, list)
    assert any(item.get("pattern") == "timeout" for item in patterns if isinstance(item, dict))


def test_episodic_memory_prefers_local_embedding_asset_when_present(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    local_embedding_dir = tmp_path / "embeddings" / "all-mpnet-base-v2(Embeddings_model)"
    local_embedding_dir.mkdir(parents=True, exist_ok=True)
    (local_embedding_dir / "config.json").write_text("{}", encoding="utf-8")
    (local_embedding_dir / "modules.json").write_text("[]", encoding="utf-8")

    store = tmp_path / "episodic_memory.jsonl"
    memory = EpisodicMemory(max_items=100, store_path=str(store), embedding_dim=64)

    assert "all-mpnet-base-v2" in memory._encoder_model_name.replace("\\", "/").lower()  # noqa: SLF001
