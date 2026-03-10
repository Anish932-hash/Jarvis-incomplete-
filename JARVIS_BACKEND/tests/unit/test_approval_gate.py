from __future__ import annotations

from backend.python.core.approval_gate import ApprovalGate


def test_request_is_deduplicated_for_same_action_args_source() -> None:
    gate = ApprovalGate(ttl_s=120, max_records=512)
    args = {"source": "a.txt", "destination": "b.txt"}

    first = gate.request(action="copy_file", args=args, source="desktop-ui")
    second = gate.request(action="copy_file", args=args, source="desktop-ui")

    assert first.approval_id == second.approval_id
    assert gate.pending_count() == 1


def test_approve_and_consume_roundtrip() -> None:
    gate = ApprovalGate(ttl_s=120, max_records=512)
    args = {"source": "a.txt", "destination": "b.txt"}
    record = gate.request(action="copy_file", args=args, source="desktop-ui")

    approved, message, approved_record = gate.approve(record.approval_id, note="ok")
    assert approved, message
    assert approved_record is not None and approved_record.status == "approved"

    consumed, consume_message, consumed_record = gate.consume(
        record.approval_id,
        action="copy_file",
        args=args,
        source="desktop-ui",
    )
    assert consumed, consume_message
    assert consumed_record is not None and consumed_record.status == "consumed"

    consumed_again, _, _ = gate.consume(
        record.approval_id,
        action="copy_file",
        args=args,
        source="desktop-ui",
    )
    assert not consumed_again


def test_consume_fails_on_argument_mismatch() -> None:
    gate = ApprovalGate(ttl_s=120, max_records=512)
    requested_args = {"source": "a.txt", "destination": "b.txt"}
    consumed_args = {"source": "a.txt", "destination": "c.txt"}
    record = gate.request(action="copy_file", args=requested_args, source="desktop-ui")
    gate.approve(record.approval_id)

    ok, reason, _ = gate.consume(
        record.approval_id,
        action="copy_file",
        args=consumed_args,
        source="desktop-ui",
    )

    assert not ok
    assert "arguments mismatch" in reason.lower()

