from __future__ import annotations

from backend.python.core.desktop_anchor_memory import DesktopAnchorMemory


def test_anchor_memory_records_and_returns_lookup_hit(tmp_path) -> None:
    memory = DesktopAnchorMemory(store_path=str(tmp_path / "desktop_anchor_memory.json"))
    memory.record_outcome(
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "accessibility"},
        status="success",
        output={"status": "success", "element_id": "btn_submit", "method": "accessibility"},
        evidence={},
        metadata={"desktop_app": "outlook"},
    )

    payload = memory.lookup(
        action="computer_click_target",
        args={"query": "Submit", "provider": "auto"},
        metadata={"desktop_app": "outlook"},
        limit=3,
    )

    assert payload["status"] == "success"
    assert payload["count"] >= 1
    top = payload["items"][0]
    assert top["element_id"] == "btn_submit"
    assert top["successes"] >= 1


def test_anchor_memory_reset_by_query(tmp_path) -> None:
    memory = DesktopAnchorMemory(store_path=str(tmp_path / "desktop_anchor_memory.json"))
    memory.record_outcome(
        action="computer_click_text",
        args={"query": "Send"},
        status="success",
        output={"status": "success", "x": 22, "y": 44},
        evidence={},
        metadata={},
    )
    memory.record_outcome(
        action="computer_click_text",
        args={"query": "Cancel"},
        status="success",
        output={"status": "success", "x": 28, "y": 52},
        evidence={},
        metadata={},
    )

    reset = memory.reset(query="send")
    snapshot = memory.snapshot(action="computer_click_text", limit=10)

    assert reset["status"] == "success"
    assert reset["removed"] == 1
    assert snapshot["count"] == 1
    assert snapshot["items"][0]["query"] == "cancel"


def test_anchor_memory_quarantine_blocks_lookup_until_cleared(tmp_path) -> None:
    memory = DesktopAnchorMemory(store_path=str(tmp_path / "desktop_anchor_memory.json"))
    memory.record_outcome(
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "accessibility"},
        status="success",
        output={"status": "success", "element_id": "btn_submit", "method": "accessibility"},
        evidence={},
        metadata={"desktop_app": "outlook"},
    )

    quarantined = memory.quarantine(
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "accessibility"},
        metadata={"desktop_app": "outlook"},
        reason="guardrail_context_shift",
        severity="hard",
        signals=["guardrail_context_shift", "window_transition"],
        ttl_s=600,
    )
    assert quarantined["status"] == "success"

    blocked_lookup = memory.lookup(
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "auto"},
        metadata={"desktop_app": "outlook"},
        limit=3,
    )
    assert blocked_lookup["status"] == "success"
    assert int(blocked_lookup.get("count", 0) or 0) == 0
    assert int(blocked_lookup.get("quarantine_skipped", 0) or 0) >= 1

    snapshot = memory.snapshot(action="computer_click_target", query="submit", limit=10)
    assert int(snapshot.get("quarantine_count", 0) or 0) >= 1

    cleared = memory.clear_quarantine(action="computer_click_target", query="submit")
    assert cleared["status"] == "success"
    assert int(cleared.get("removed", 0) or 0) >= 1

    recovered_lookup = memory.lookup(
        action="computer_click_target",
        args={"query": "Submit", "target_mode": "auto"},
        metadata={"desktop_app": "outlook"},
        limit=3,
    )
    assert recovered_lookup["status"] == "success"
    assert int(recovered_lookup.get("count", 0) or 0) >= 1


def test_anchor_memory_transition_profile_improves_matching_for_known_state_transition(tmp_path) -> None:
    memory = DesktopAnchorMemory(store_path=str(tmp_path / "desktop_anchor_memory.json"))
    pre_hash_good = "a1" * 16
    post_hash_good = "b2" * 16
    pre_hash_bad = "c3" * 16
    post_hash_bad = "d4" * 16

    for _ in range(3):
        memory.record_outcome(
            action="computer_click_target",
            args={"query": "Send", "target_mode": "accessibility"},
            status="success",
            output={"status": "success", "element_id": "btn_send", "method": "accessibility"},
            evidence={"desktop_state": {"pre_hash": pre_hash_good, "state_hash": post_hash_good}},
            metadata={"desktop_app": "outlook", "__desktop_transition_signature": f"{pre_hash_good}->{post_hash_good}"},
        )

    for _ in range(2):
        memory.record_outcome(
            action="computer_click_target",
            args={"query": "Send", "target_mode": "accessibility"},
            status="failed",
            output={"status": "error", "element_id": "btn_send", "method": "accessibility"},
            evidence={"desktop_state": {"pre_hash": pre_hash_bad, "state_hash": post_hash_bad}},
            metadata={
                "desktop_app": "outlook",
                "__desktop_transition_signature": f"{pre_hash_bad}->{post_hash_bad}",
                "__desktop_guardrail_feedback": [
                    {
                        "pre_hash": pre_hash_bad,
                        "state_hash": post_hash_bad,
                        "reason_tags": ["window_transition", "guardrail_context_shift"],
                        "changed_paths": ["window.title", "visual.screen_hash"],
                    }
                ],
            },
            error="context shift after click",
        )

    good_lookup = memory.lookup(
        action="computer_click_target",
        args={"query": "Send", "pre_state_hash": pre_hash_good, "post_state_hash": post_hash_good},
        metadata={"desktop_app": "outlook"},
        limit=1,
    )
    bad_lookup = memory.lookup(
        action="computer_click_target",
        args={"query": "Send", "pre_state_hash": pre_hash_bad, "post_state_hash": post_hash_bad},
        metadata={"desktop_app": "outlook"},
        limit=1,
    )

    assert good_lookup["status"] == "success"
    assert bad_lookup["status"] == "success"
    assert int(good_lookup.get("count", 0) or 0) >= 1
    assert int(bad_lookup.get("count", 0) or 0) >= 1
    good_score = float(good_lookup["items"][0].get("match_score", 0.0) or 0.0)
    bad_score = float(bad_lookup["items"][0].get("match_score", 0.0) or 0.0)
    assert good_score > bad_score


def test_anchor_memory_lookup_auto_quarantines_chronic_unstable_anchor(tmp_path) -> None:
    memory = DesktopAnchorMemory(store_path=str(tmp_path / "desktop_anchor_memory.json"))

    for _ in range(8):
        memory.record_outcome(
            action="computer_click_target",
            args={"query": "Delete", "target_mode": "accessibility"},
            status="failed",
            output={"status": "error", "element_id": "btn_delete", "method": "accessibility"},
            evidence={},
            metadata={"desktop_app": "outlook"},
            error="anchor fallback failed",
        )

    first_lookup = memory.lookup(
        action="computer_click_target",
        args={"query": "Delete", "target_mode": "auto"},
        metadata={"desktop_app": "outlook"},
        limit=3,
    )
    assert first_lookup["status"] == "success"
    assert int(first_lookup.get("count", 0) or 0) == 0
    assert (
        int(first_lookup.get("filtered_unstable", 0) or 0) >= 1
        or int(first_lookup.get("quarantine_skipped", 0) or 0) >= 1
    )
    assert int(first_lookup.get("auto_quarantined", 0) or 0) >= 0

    second_lookup = memory.lookup(
        action="computer_click_target",
        args={"query": "Delete", "target_mode": "auto"},
        metadata={"desktop_app": "outlook"},
        limit=3,
    )
    assert second_lookup["status"] == "success"
    assert int(second_lookup.get("quarantine_skipped", 0) or 0) >= 1
