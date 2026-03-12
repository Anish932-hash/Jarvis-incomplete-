from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from backend.python.inference.coworker_stack_activation import CoworkerStackActivationOrchestrator


def test_coworker_stack_activation_refreshes_inventory_and_applies_runtime_tasks(tmp_path: Path) -> None:
    orchestrator = CoworkerStackActivationOrchestrator()
    model_path = tmp_path / "reasoning" / "Qwen2.5-14B-Instruct-Q8_0.gguf"
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model_path.write_text("synthetic", encoding="utf-8")

    refresh_calls: list[bool] = []
    status_calls = 0
    apply_calls: list[Dict[str, Any]] = []

    before_status = {
        "status": "partial",
        "stack_name": "desktop_agent",
        "mission_profile": "balanced",
        "summary": {"score": 62.0},
        "tasks": [
            {
                "task": "reasoning",
                "provider": "local",
                "model": "Qwen2.5-14B-Instruct-Q8_0.gguf",
                "selected_path": str(model_path),
                "status": "action_required",
                "auto_applyable": True,
                "already_active": False,
                "profile_id": "reasoning-local-qwen",
                "template_id": "llama_server",
                "blockers": [],
                "warnings": [],
            },
            {
                "task": "embedding",
                "provider": "local",
                "model": "all-mpnet-base-v2",
                "selected_path": str(tmp_path / "embeddings" / "all-mpnet-base-v2"),
                "status": "ready",
                "auto_applyable": False,
                "already_active": False,
                "blockers": [],
                "warnings": [],
            },
        ],
        "warnings": [],
        "blockers": [],
    }
    after_status = {
        "status": "success",
        "stack_name": "desktop_agent",
        "mission_profile": "balanced",
        "summary": {"score": 88.0},
        "tasks": [
            {
                "task": "reasoning",
                "provider": "local",
                "model": "Qwen2.5-14B-Instruct-Q8_0.gguf",
                "selected_path": str(model_path),
                "status": "ready",
                "auto_applyable": True,
                "already_active": True,
                "profile_id": "reasoning-local-qwen",
                "template_id": "llama_server",
                "blockers": [],
                "warnings": [],
            },
            {
                "task": "embedding",
                "provider": "local",
                "model": "all-mpnet-base-v2",
                "selected_path": str(tmp_path / "embeddings" / "all-mpnet-base-v2"),
                "status": "ready",
                "auto_applyable": False,
                "already_active": False,
                "blockers": [],
                "warnings": [],
            },
        ],
        "warnings": [],
        "blockers": [],
    }

    def _refresh_registry(*, force: bool = False) -> Dict[str, Any]:
        refresh_calls.append(bool(force))
        return {"status": "success", "refreshed": True}

    def _stack_status(**_: Any) -> Dict[str, Any]:
        nonlocal status_calls
        status_calls += 1
        return before_status if status_calls == 1 else after_status

    def _apply_stack(**kwargs: Any) -> Dict[str, Any]:
        apply_calls.append(dict(kwargs))
        return {
            "status": "success",
            "requested_tasks": list(kwargs.get("tasks", [])),
            "apply": {
                "status": "success",
                "executed_count": 1,
                "items": [
                    {
                        "task": "reasoning",
                        "status": "success",
                        "ok": True,
                        "profile_id": "reasoning-local-qwen",
                        "template_id": "llama_server",
                        "result": {"status": "success", "ready": True},
                    }
                ],
            },
            "after": after_status,
        }

    def _inventory_snapshot(*, task: str = "", limit: int = 24) -> Dict[str, Any]:
        del limit
        return {
            "status": "success",
            "task": task,
            "present_count": 1,
            "missing_count": 0,
            "declared_count": 1,
            "detected_count": 1,
            "items": [
                {
                    "name": "artifact",
                    "path": str(model_path if task == "reasoning" else tmp_path / "embeddings" / "all-mpnet-base-v2"),
                    "present": True,
                    "missing": False,
                    "declared": True,
                }
            ],
        }

    payload = orchestrator.activate(
        source="setup_install",
        task="reasoning",
        run_payload={
            "status": "partial",
            "run_id": "run-123",
            "items": [
                {
                    "task": "reasoning",
                    "status": "success",
                    "path": str(model_path),
                },
                {
                    "task": "embedding",
                    "status": "success",
                    "path": str(tmp_path / "embeddings" / "all-mpnet-base-v2"),
                },
            ],
        },
        refresh_registry=_refresh_registry,
        stack_status=_stack_status,
        apply_stack=_apply_stack,
        inventory_snapshot=_inventory_snapshot,
    )

    assert payload["status"] == "success"
    assert payload["summary"]["affected_task_count"] == 2
    assert payload["summary"]["runtime_task_count"] == 1
    assert payload["summary"]["non_runtime_task_count"] == 1
    assert payload["summary"]["activation_candidate_count"] == 1
    assert payload["summary"]["activated_task_count"] == 1
    assert payload["summary"]["ready_after_count"] == 1
    assert refresh_calls == [True]
    assert status_calls == 1
    assert apply_calls and apply_calls[0]["tasks"] == ["reasoning"]
    assert payload["activation_candidates"][0]["task"] == "reasoning"
    assert payload["activation_candidates"][0]["activated"] is True
    assert "embedding" in payload["non_runtime_tasks"]
    assert payload["inventory"]["tasks"]["reasoning"]["present_count"] == 1
    assert any("embedding" in warning.lower() for warning in payload["warnings"])


def test_coworker_stack_activation_skips_dry_runs() -> None:
    orchestrator = CoworkerStackActivationOrchestrator()

    payload = orchestrator.activate(
        source="manual_pipeline",
        task="tts",
        run_payload={
            "status": "planned",
            "dry_run": True,
            "items": [
                {
                    "task": "tts",
                    "status": "planned",
                    "path": "E:/fake/tts.gguf",
                }
            ],
        },
        stack_status=lambda **_: {"status": "success", "tasks": []},
        apply_stack=lambda **_: {"status": "success"},
    )

    assert payload["status"] == "skipped"
    assert "dry-run" in str(payload["message"]).lower()
    assert payload["affected_tasks"] == []
