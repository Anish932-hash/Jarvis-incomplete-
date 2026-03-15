from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Optional

from backend.python.inference.model_setup_install_manager import ModelSetupInstallManager
from backend.python.inference.model_setup_installer import ModelSetupInstaller, _iso_now


def _wait_for_run(manager: ModelSetupInstallManager, run_id: str, *, timeout_s: float = 3.0) -> Dict[str, Any]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        payload = manager.get_run(run_id)
        run = payload.get("run", {}) if isinstance(payload.get("run", {}), dict) else {}
        status = str(run.get("status", "") or "").strip().lower()
        if status and status not in {"queued", "running", "cancelling"}:
            return payload
        time.sleep(0.05)
    raise AssertionError(f"run {run_id} did not finish within {timeout_s}s")


def test_model_setup_install_manager_tracks_background_dry_run(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    installer = ModelSetupInstaller(history_path="data/install_history.json")
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

    manager = ModelSetupInstallManager(
        installer,
        state_path="data/install_runs.json",
        completion_callback=_activation_callback,
    )

    plan = {
        "manifest": {
            "path": str(tmp_path / "JARVIS_BACKEND" / "Models to Download.txt"),
            "workspace_root": str(tmp_path),
        },
        "items": [
            {
                "key": "embedding-main",
                "name": "all-mpnet-base-v2",
                "task": "embedding",
                "path": str(tmp_path / "embeddings" / "all-mpnet-base-v2"),
                "strategy": "huggingface_snapshot",
                "source_ref": "sentence-transformers/all-mpnet-base-v2",
                "automation_ready": True,
            }
        ],
    }

    launch = manager.start(plan_payload=plan, dry_run=True, task="embedding")
    assert launch["status"] == "accepted"
    run_id = str(launch["run"]["run_id"])

    completed = _wait_for_run(manager, run_id)
    run = completed["run"]

    assert run["status"] == "success"
    assert run["task"] == "embedding"
    assert run["manifest_path"] == str(tmp_path / "JARVIS_BACKEND" / "Models to Download.txt")
    assert run["workspace_root"] == str(tmp_path)
    assert run["result"]["status"] == "success"
    assert run["items"][0]["status"] == "planned"
    assert int(run["progress"]["completed_items"]) == 1
    assert str(run["last_event_name"]).strip().lower() == "run_completed"
    assert str(run["last_progress_at"]).strip()
    assert int(run["progress_event_count"]) >= 1
    assert str(run["progress"]["phase"]).strip().lower() == "completed"
    assert activation_calls == [{"source": "setup_install", "task": "embedding", "run_status": "success"}]
    assert run["activation"]["status"] == "skipped"
    assert run["result"]["activation"]["message"] == "dry-run activation skipped"


class _SlowInstaller:
    def install(
        self,
        *,
        plan_payload: Dict[str, Any],  # noqa: ARG002
        item_keys: Optional[list[str]] = None,  # noqa: ARG002
        dry_run: bool = False,  # noqa: ARG002
        force: bool = False,  # noqa: ARG002
        run_id: str = "",
        progress_callback=None,
        cancel_event=None,
        remote_metadata=None,  # noqa: ARG002
        verify_integrity: bool = True,  # noqa: ARG002
    ) -> Dict[str, Any]:
        if callable(progress_callback):
            progress_callback({"event": "run_started", "run_id": run_id, "selected_count": 1, "dry_run": False, "force": False})
            progress_callback(
                {
                    "event": "item_started",
                    "run_id": run_id,
                    "index": 1,
                    "total_items": 1,
                    "item": {"key": "vision-yolo", "name": "yolov10x.pt", "task": "vision"},
                }
            )
        while cancel_event is not None and not cancel_event.is_set():
            time.sleep(0.02)
        item = {
            "key": "vision-yolo",
            "name": "yolov10x.pt",
            "task": "vision",
            "path": "E:/fake/yolov10x.pt",
            "strategy": "direct_url",
            "status": "cancelled",
            "message": "cancelled",
            "bytes_written": 0,
            "started_at": _iso_now(),
            "completed_at": _iso_now(),
            "duration_s": 0.1,
        }
        if callable(progress_callback):
            progress_callback({"event": "item_completed", "run_id": run_id, "index": 1, "total_items": 1, "item": item})
        payload = {
            "status": "cancelled",
            "run_id": run_id,
            "dry_run": False,
            "force": False,
            "selected_count": 1,
            "success_count": 0,
            "error_count": 0,
            "skipped_count": 0,
            "blocked_count": 0,
            "cancelled_count": 1,
            "requested_item_keys": [],
            "started_at": _iso_now(),
            "completed_at": _iso_now(),
            "duration_s": 0.1,
            "items": [item],
            "history_path": "data/install_history.json",
            "manifest_path": "Models to Download.txt",
        }
        if callable(progress_callback):
            progress_callback({"event": "run_completed", "run_id": run_id, "payload": payload})
        return payload


def test_model_setup_install_manager_cancel_requests_propagate_to_running_job(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manager = ModelSetupInstallManager(_SlowInstaller(), state_path="data/install_runs.json")  # type: ignore[arg-type]

    plan = {
        "manifest": {"path": str(tmp_path / "JARVIS_BACKEND" / "Models to Download.txt")},
        "items": [
            {
                "key": "vision-yolo",
                "name": "yolov10x.pt",
                "task": "vision",
                "path": str(tmp_path / "models" / "vision" / "yolov10x.pt"),
                "strategy": "direct_url",
                "source_url": "https://example.com/models/yolov10x.pt",
                "automation_ready": True,
            }
        ],
    }

    launch = manager.start(plan_payload=plan, task="vision")
    run_id = str(launch["run"]["run_id"])
    cancel_payload = manager.cancel(run_id)
    assert cancel_payload["status"] == "success"

    completed = _wait_for_run(manager, run_id)
    run = completed["run"]

    assert run["status"] == "cancelled"
    assert str(run["cancel_requested_at"]).strip()
    assert str(run["cancel_reason"]).strip() == "cancelled_by_user"


def test_model_setup_install_manager_filters_runs_by_manifest_scope(tmp_path: Path) -> None:
    manager = ModelSetupInstallManager(_SlowInstaller(), state_path=str(tmp_path / "install_runs.json"))  # type: ignore[arg-type]
    manager._runs = {
        "run-a": {
            "run_id": "run-a",
            "status": "running",
            "updated_at": "2026-03-15T10:00:00+00:00",
            "manifest_path": "E:/ScopeA/JARVIS_BACKEND/Models to Download.txt",
            "workspace_root": "E:/ScopeA",
        },
        "run-b": {
            "run_id": "run-b",
            "status": "queued",
            "updated_at": "2026-03-15T09:00:00+00:00",
            "manifest_path": "E:/ScopeB/JARVIS_BACKEND/Models to Download.txt",
            "workspace_root": "E:/ScopeB",
        },
    }

    payload = manager.list_runs(
        manifest_path="E:/ScopeA/JARVIS_BACKEND/Models to Download.txt",
        workspace_root="E:/ScopeA",
        limit=10,
    )

    assert payload["status"] == "success"
    assert payload["count"] == 1
    assert payload["active_count"] == 1
    assert payload["items"][0]["run_id"] == "run-a"
    assert payload["filters"]["workspace_root"] == "E:/ScopeA"
