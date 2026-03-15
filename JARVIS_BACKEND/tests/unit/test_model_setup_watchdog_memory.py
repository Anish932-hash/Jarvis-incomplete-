from __future__ import annotations

from backend.python.inference.model_setup_watchdog_memory import ModelSetupRecoveryWatchdogMemory


def test_model_setup_watchdog_memory_records_and_summarizes_runs(tmp_path) -> None:
    memory = ModelSetupRecoveryWatchdogMemory(
        state_path=str(tmp_path / "watchdog_runs.json"),
        keep_runs=16,
    )

    recorded = memory.record(
        watchdog_payload={
            "status": "success",
            "message": "Recovery watchdog auto-resumed 1 stored mission.",
            "current_scope": False,
            "max_missions": 4,
            "max_auto_resumes": 2,
            "continue_followup_actions_requested": True,
            "evaluated_count": 3,
            "auto_resume_attempted_count": 1,
            "auto_resume_triggered_count": 1,
            "ready_count": 0,
            "watch_count": 1,
            "stalled_count": 1,
            "blocked_count": 0,
            "idle_count": 1,
            "complete_count": 0,
            "error_count": 0,
            "triggered_mission_ids": ["msm_ready"],
            "watched_mission_ids": ["msm_watch"],
            "stalled_mission_ids": ["msm_stalled"],
            "scope_counts": {"J.A.R.V.I.S::Models to Download.txt": 3},
            "history_after": {
                "filters": {
                    "workspace_root": "E:/J.A.R.V.I.S",
                    "manifest_path": "E:/J.A.R.V.I.S/JARVIS_BACKEND/Models to Download.txt",
                }
            },
            "latest_triggered_payload": {
                "status": "success",
                "message": "auto-resumed setup mission",
                "workspace": {
                    "workspace_root": "E:/J.A.R.V.I.S",
                    "manifest_path": "E:/J.A.R.V.I.S/JARVIS_BACKEND/Models to Download.txt",
                },
            },
        },
        source="watchdog",
    )

    run = recorded["run"]
    assert run["status"] == "success"
    assert run["auto_resume_triggered_count"] == 1
    assert run["watch_count"] == 1
    assert run["stalled_count"] == 1
    assert run["scope_label"] == "J.A.R.V.I.S::Models to Download.txt"

    snapshot = memory.snapshot(limit=8)

    assert snapshot["status"] == "success"
    assert snapshot["count"] == 1
    assert snapshot["triggered_run_count"] == 1
    assert snapshot["watch_run_count"] == 1
    assert snapshot["stalled_run_count"] == 1
    assert snapshot["latest_run"]["run_id"] == run["run_id"]
    assert snapshot["latest_triggered_run"]["run_id"] == run["run_id"]


def test_model_setup_watchdog_memory_resets_by_scope(tmp_path) -> None:
    memory = ModelSetupRecoveryWatchdogMemory(
        state_path=str(tmp_path / "watchdog_runs.json"),
        keep_runs=16,
    )

    memory.record(
        watchdog_payload={
            "status": "watch",
            "message": "Watching active setup runs.",
            "watch_count": 1,
            "history_after": {
                "filters": {
                    "workspace_root": "E:/ScopeOne",
                    "manifest_path": "E:/ScopeOne/JARVIS_BACKEND/Models to Download.txt",
                }
            },
        },
        source="watchdog",
    )
    memory.record(
        watchdog_payload={
            "status": "idle",
            "message": "No resumable work.",
            "history_after": {
                "filters": {
                    "workspace_root": "E:/ScopeTwo",
                    "manifest_path": "E:/ScopeTwo/JARVIS_BACKEND/Models to Download.txt",
                }
            },
        },
        source="watchdog",
    )

    reset_payload = memory.reset(workspace_root="E:/ScopeOne")
    assert reset_payload["status"] == "success"
    assert reset_payload["removed"] == 1

    snapshot = memory.snapshot(limit=8)
    assert snapshot["count"] == 1
    assert snapshot["latest_run"]["workspace_root"] == "E:/ScopeTwo"
