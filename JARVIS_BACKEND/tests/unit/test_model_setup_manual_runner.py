from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Optional

from backend.python.inference.model_setup_manual_run_manager import ModelSetupManualRunManager
from backend.python.inference.model_setup_manual_runner import ModelSetupManualRunner, _iso_now


def _ps_single(value: str) -> str:
    return value.replace("'", "''")


def _manual_pipeline_payload(tmp_path: Path, target_path: Path) -> Dict[str, Any]:
    target_dir = target_path.parent
    return {
        "status": "success",
        "pipeline_root": str(tmp_path),
        "items": [
            {
                "key": "tts-orpheus-3b-gguf",
                "name": "Orpheus-3B-TTS.f16.gguf",
                "task": "tts",
                "status": "ready",
                "path": str(target_path),
                "steps": [
                    {
                        "id": "promote-artifact",
                        "title": "Promote artifact",
                        "status": "ready",
                        "commands": [
                            f"New-Item -ItemType Directory -Force -Path '{_ps_single(str(target_dir))}' | Out-Null",
                            f"Set-Content -Path '{_ps_single(str(target_path))}' -Value 'synthetic-manual-model'",
                        ],
                    }
                ],
            }
        ],
    }


def _wait_for_run(manager: ModelSetupManualRunManager, run_id: str, *, timeout_s: float = 4.0) -> Dict[str, Any]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        payload = manager.get_run(run_id)
        run = payload.get("run", {}) if isinstance(payload.get("run"), dict) else {}
        status_name = str(run.get("status", "") or "").strip().lower()
        if status_name and status_name not in {"queued", "running", "cancelling"}:
            return payload
        time.sleep(0.05)
    raise AssertionError(f"manual run {run_id} did not finish within {timeout_s}s")


def test_model_setup_manual_runner_dry_run_marks_plan_without_creating_artifact(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    target_path = tmp_path / "tts" / "Orpheus-3B-TTS.f16.gguf"
    runner = ModelSetupManualRunner(history_path="data/manual_history.json")

    payload = runner.run(
        pipeline_payload=_manual_pipeline_payload(tmp_path, target_path),
        dry_run=True,
    )

    assert payload["status"] == "planned"
    assert payload["planned_count"] == 1
    assert payload["error_count"] == 0
    assert payload["items"][0]["status"] == "planned"
    assert not target_path.exists()


def test_model_setup_manual_runner_executes_commands_and_verifies_artifact(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    target_path = tmp_path / "tts" / "Orpheus-3B-TTS.f16.gguf"
    runner = ModelSetupManualRunner(history_path="data/manual_history.json")

    payload = runner.run(
        pipeline_payload=_manual_pipeline_payload(tmp_path, target_path),
        dry_run=False,
    )

    assert payload["status"] == "success"
    assert payload["success_count"] == 1
    item = payload["items"][0]
    assert item["status"] == "success"
    assert item["artifact"]["exists"] is True
    assert target_path.exists()
    assert "synthetic-manual-model" in target_path.read_text(encoding="utf-8")


class _SlowManualRunner:
    def run(
        self,
        *,
        pipeline_payload: Dict[str, Any],  # noqa: ARG002
        item_keys: Optional[list[str]] = None,  # noqa: ARG002
        dry_run: bool = False,  # noqa: ARG002
        force: bool = False,  # noqa: ARG002
        run_id: str = "",
        progress_callback=None,
        cancel_event=None,
        step_ids: Optional[list[str]] = None,  # noqa: ARG002
    ) -> Dict[str, Any]:
        if callable(progress_callback):
            progress_callback({"event": "run_started", "run_id": run_id, "selected_count": 1})
            progress_callback(
                {
                    "event": "item_started",
                    "run_id": run_id,
                    "index": 1,
                    "total_items": 1,
                    "item": {"key": "tts-orpheus-3b-gguf", "name": "Orpheus-3B-TTS.f16.gguf", "task": "tts"},
                }
            )
        while cancel_event is not None and not cancel_event.is_set():
            time.sleep(0.02)
        item = {
            "key": "tts-orpheus-3b-gguf",
            "name": "Orpheus-3B-TTS.f16.gguf",
            "task": "tts",
            "path": "E:/fake/Orpheus-3B-TTS.f16.gguf",
            "status": "cancelled",
            "message": "cancelled",
            "started_at": _iso_now(),
            "completed_at": _iso_now(),
            "duration_s": 0.1,
            "steps": [],
            "step_success_count": 0,
            "step_error_count": 0,
            "step_skipped_count": 0,
        }
        if callable(progress_callback):
            progress_callback({"event": "item_completed", "run_id": run_id, "index": 1, "total_items": 1, "item": item})
        payload = {
            "status": "cancelled",
            "run_id": run_id,
            "dry_run": False,
            "force": False,
            "selected_count": 1,
            "planned_count": 0,
            "success_count": 0,
            "warning_count": 0,
            "error_count": 0,
            "blocked_count": 0,
            "cancelled_count": 1,
            "step_success_count": 0,
            "step_error_count": 0,
            "step_skipped_count": 0,
            "requested_item_keys": [],
            "started_at": _iso_now(),
            "completed_at": _iso_now(),
            "duration_s": 0.1,
            "items": [item],
        }
        if callable(progress_callback):
            progress_callback({"event": "run_completed", "run_id": run_id, "payload": payload})
        return payload


def test_model_setup_manual_run_manager_tracks_background_dry_run(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    target_path = tmp_path / "tts" / "Orpheus-3B-TTS.f16.gguf"
    runner = ModelSetupManualRunner(history_path="data/manual_history.json")
    activation_calls: list[dict[str, Any]] = []

    def _activation_callback(*, source: str, task: str, run_payload: Dict[str, Any]) -> Dict[str, Any]:
        activation_calls.append(
            {
                "source": source,
                "task": task,
                "run_status": str(run_payload.get("status", "") or ""),
            }
        )
        return {"status": "skipped", "source": source, "task": task, "message": "dry-run activation skipped"}

    manager = ModelSetupManualRunManager(
        runner,
        state_path="data/manual_runs.json",
        completion_callback=_activation_callback,
    )

    launch = manager.start(
        pipeline_payload=_manual_pipeline_payload(tmp_path, target_path),
        dry_run=True,
        task="tts",
    )
    assert launch["status"] == "accepted"
    run_id = str(launch["run"]["run_id"])

    completed = _wait_for_run(manager, run_id)
    run = completed["run"]

    assert run["status"] == "planned"
    assert run["task"] == "tts"
    assert run["result"]["status"] == "planned"
    assert run["items"][0]["status"] == "planned"
    assert activation_calls == [{"source": "manual_pipeline", "task": "tts", "run_status": "planned"}]
    assert run["activation"]["status"] == "skipped"
    assert run["result"]["activation"]["message"] == "dry-run activation skipped"


def test_model_setup_manual_run_manager_cancel_requests_propagate_to_running_job(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manager = ModelSetupManualRunManager(_SlowManualRunner(), state_path="data/manual_runs.json")  # type: ignore[arg-type]

    launch = manager.start(
        pipeline_payload=_manual_pipeline_payload(tmp_path, tmp_path / "tts" / "Orpheus-3B-TTS.f16.gguf"),
        task="tts",
    )
    run_id = str(launch["run"]["run_id"])

    cancel_payload = manager.cancel(run_id)
    assert cancel_payload["status"] == "success"

    completed = _wait_for_run(manager, run_id)
    run = completed["run"]

    assert run["status"] == "cancelled"
    assert str(run["cancel_requested_at"]).strip()
    assert str(run["cancel_reason"]).strip() == "cancelled_by_user"
