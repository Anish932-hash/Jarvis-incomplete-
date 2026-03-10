from __future__ import annotations

from pathlib import Path

from backend.python.core.contracts import ActionResult
from backend.python.core.rollback_manager import RollbackManager


def test_rollback_restores_previous_file_content(tmp_path) -> None:
    target = tmp_path / "notes.txt"
    target.write_text("before", encoding="utf-8")
    manager = RollbackManager(
        store_path=str(tmp_path / "rollback.json"),
        backup_dir=str(tmp_path / "backups"),
        max_entries=100,
    )

    args = {"path": str(target), "content": "after"}
    pre_state = manager.capture_pre_state(action="write_file", args=args)
    target.write_text("after", encoding="utf-8")

    result = ActionResult(action="write_file", status="success", output={"status": "success", "bytes": 5})
    entry = manager.record_success(
        action="write_file",
        args=args,
        result=result,
        source="desktop-ui",
        goal_id="goal-1",
        pre_state=pre_state,
    )
    assert isinstance(entry, dict)
    rollback_id = str(entry.get("rollback_id", ""))
    assert rollback_id

    rollback_result = manager.rollback_entry(rollback_id)
    assert rollback_result["status"] == "success"
    assert target.read_text(encoding="utf-8") == "before"


def test_rollback_deletes_new_copy_destination(tmp_path) -> None:
    source = tmp_path / "source.txt"
    destination = tmp_path / "destination.txt"
    source.write_text("payload", encoding="utf-8")
    manager = RollbackManager(
        store_path=str(tmp_path / "rollback.json"),
        backup_dir=str(tmp_path / "backups"),
        max_entries=100,
    )

    args = {"source": str(source), "destination": str(destination)}
    pre_state = manager.capture_pre_state(action="copy_file", args=args)
    destination.write_text("payload", encoding="utf-8")

    result = ActionResult(action="copy_file", status="success", output={"status": "success"})
    entry = manager.record_success(
        action="copy_file",
        args=args,
        result=result,
        source="desktop-ui",
        goal_id="goal-2",
        pre_state=pre_state,
    )
    assert isinstance(entry, dict)
    rollback_id = str(entry.get("rollback_id", ""))
    assert rollback_id

    rollback_result = manager.rollback_entry(rollback_id)
    assert rollback_result["status"] == "success"
    assert destination.exists() is False


def test_goal_rollback_executes_entries_in_reverse_order(tmp_path) -> None:
    manager = RollbackManager(
        store_path=str(tmp_path / "rollback.json"),
        backup_dir=str(tmp_path / "backups"),
        max_entries=100,
    )
    goal_id = "goal-3"
    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"
    a.write_text("1", encoding="utf-8")
    b.write_text("2", encoding="utf-8")

    for path in (a, b):
        args = {"path": str(path), "content": "updated"}
        pre_state = manager.capture_pre_state(action="write_file", args=args)
        path.write_text("updated", encoding="utf-8")
        manager.record_success(
            action="write_file",
            args=args,
            result=ActionResult(action="write_file", status="success", output={"status": "success"}),
            source="desktop-ui",
            goal_id=goal_id,
            pre_state=pre_state,
        )

    summary = manager.rollback_goal(goal_id)
    assert summary["status"] == "success"
    assert summary["rolled_back"] == 2
    assert a.read_text(encoding="utf-8") == "1"
    assert b.read_text(encoding="utf-8") == "2"


def test_rollback_profile_marks_external_mutation_as_non_reversible(tmp_path) -> None:
    manager = RollbackManager(
        store_path=str(tmp_path / "rollback.json"),
        backup_dir=str(tmp_path / "backups"),
        max_entries=100,
    )

    profile = manager.rollback_profile(
        action="external_doc_update",
        args={"provider": "google", "document_id": "doc-1", "content": "patch"},
    )

    assert bool(profile.get("rollback_supported", True)) is False
    assert bool(profile.get("reversible", True)) is False
    assert bool(profile.get("requires_branch", False)) is True
    assert str(profile.get("branch_reason", "")) == "external_non_reversible_mutation"


def test_rollback_profile_marks_local_write_as_reversible(tmp_path) -> None:
    manager = RollbackManager(
        store_path=str(tmp_path / "rollback.json"),
        backup_dir=str(tmp_path / "backups"),
        max_entries=100,
    )

    profile = manager.rollback_profile(
        action="write_file",
        args={"path": str(tmp_path / "note.txt"), "content": "hello"},
    )

    assert bool(profile.get("rollback_supported", False)) is True
    assert bool(profile.get("reversible", False)) is True
    assert str(profile.get("reversibility_class", "")) == "full"
