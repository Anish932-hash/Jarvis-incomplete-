from __future__ import annotations

from backend.python.inference.model_setup_installer import _iso_now
from backend.python.inference.model_setup_mission import build_model_setup_mission
from backend.python.inference.model_setup_mission_memory import ModelSetupMissionMemory


def _mission_payload(*, workspace_root: str = "E:/J.A.R.V.I.S", manifest_path: str = "E:/J.A.R.V.I.S/JARVIS_BACKEND/Models to Download.txt") -> dict:
    return build_model_setup_mission(
        workspace_payload={
            "status": "success",
            "workspace_root": workspace_root,
            "manifest_path": manifest_path,
            "summary": {
                "missing_directory_count": 1,
                "workspace_ready": False,
                "stack_ready": False,
                "readiness_score": 44,
            },
            "directory_actions": [
                {
                    "kind": "create_directory",
                    "name": "custom_intent",
                    "task": "intent",
                    "path": f"{workspace_root}/custom_intents",
                    "safe": True,
                }
            ],
            "required_providers": [
                {"provider": "groq", "ready": True},
                {
                    "provider": "elevenlabs",
                    "ready": False,
                    "missing_requirements": ["ELEVENLABS_VOICE_ID"],
                },
            ],
        },
        setup_plan_payload={
            "status": "success",
            "items": [
                {"key": "embedding-all-mpnet-base-v2", "automation_ready": True},
            ],
        },
        preflight_payload={"status": "success", "summary": {"blocked_count": 0}, "items": []},
        manual_pipeline_payload={
            "status": "success",
            "items": [
                {
                    "key": "reasoning-meta-llama-3.1-8b-gguf",
                    "status": "blocked",
                    "blockers": ["A verified Hugging Face access token is required."],
                }
            ],
        },
        install_runs_payload={"status": "success", "active_count": 0, "items": []},
        manual_runs_payload={"status": "success", "active_count": 0, "items": []},
    )


def test_model_setup_mission_memory_records_recovery_state(tmp_path) -> None:
    memory = ModelSetupMissionMemory(state_path=str(tmp_path / "model_setup_missions.json"))
    mission = _mission_payload()

    recorded = memory.record(
        mission_payload=mission,
        launch_payload={
            "status": "success",
            "generated_at": "2026-03-15T08:02:00+00:00",
            "executed_count": 2,
            "skipped_count": 0,
            "error_count": 0,
            "items": [],
        },
        selected_action_ids=["scaffold_workspace", "launch_setup_install:auto"],
        source="launch",
        dry_run=False,
    )

    row = recorded["mission"]
    assert recorded["status"] == "success"
    assert str(row["mission_id"]).startswith("msm_")
    assert row["status"] == "resume_ready"
    assert row["resume_ready"] is True
    assert row["manual_attention_required"] is True
    assert row["recovery_profile"] == "provider_credentials"
    assert "configure" in str(row["recovery_hint"]).lower()
    assert row["auto_resume_candidate"] is False
    assert row["resume_trigger"] == "manual_attention"
    assert row["resume_blockers"] == ["provider_credentials"]
    assert "configured" in str(row["auto_resume_reason"]).lower()
    assert row["launch_count"] == 1


def test_model_setup_mission_memory_tracks_running_install_recovery(tmp_path) -> None:
    memory = ModelSetupMissionMemory(state_path=str(tmp_path / "model_setup_missions.json"))
    mission = _mission_payload()
    now_iso = _iso_now()
    mission["install_runs"] = {
        "status": "success",
        "active_count": 1,
        "items": [
            {
                "run_id": "install-run-1",
                "status": "running",
                "task": "embedding",
                "updated_at": now_iso,
                "last_progress_at": now_iso,
                "last_event_name": "item_started",
                "progress_event_count": 3,
                "progress": {
                    "current_item_name": "embedding-all-mpnet-base-v2",
                    "completed_items": 1,
                    "total_items": 4,
                    "percent": 25.0,
                    "message": "installing embedding-all-mpnet-base-v2",
                },
            }
        ],
    }

    recorded = memory.record(mission_payload=mission, source="resume", dry_run=False)
    row = recorded["mission"]
    assert row["status"] == "running"
    assert row["recovery_profile"] == "install_running"
    assert row["active_install_run_ids"] == ["install-run-1"]
    assert row["active_run_count"] == 1
    assert row["active_run_summary"]["active_count"] == 1
    assert row["active_run_summary"]["top_label"] == "embedding-all-mpnet-base-v2"
    assert row["active_run_summary"]["top_health"] in {"active", "waiting"}
    assert row["watch_active_runs"] is True
    assert row["next_poll_s"] > 0
    assert row["active_install_runs"][0]["run_id"] == "install-run-1"
    assert "embedding-all-mpnet-base-v2" in str(row["recovery_hint"]).lower()
    assert row["auto_resume_candidate"] is False
    assert row["resume_trigger"] == "manual_attention"
    assert row["resume_blockers"] == ["provider_credentials"]


def test_model_setup_mission_memory_tracks_accepted_launch_run_ids(tmp_path) -> None:
    memory = ModelSetupMissionMemory(state_path=str(tmp_path / "model_setup_missions.json"))
    mission = _mission_payload()

    recorded = memory.record(
        mission_payload=mission,
        launch_payload={
            "status": "success",
            "generated_at": "2026-03-15T08:05:00+00:00",
            "executed_count": 2,
            "items": [
                {
                    "action_id": "launch_setup_install:auto",
                    "kind": "launch_setup_install",
                    "status": "accepted",
                    "ok": True,
                    "result": {"run": {"run_id": "install-run-9"}},
                },
                {
                    "action_id": "launch_manual_pipeline:all",
                    "kind": "launch_manual_pipeline",
                    "status": "accepted",
                    "ok": True,
                    "result": {"run": {"run_id": "manual-run-3"}},
                },
            ],
        },
        selected_action_ids=["launch_setup_install:auto", "launch_manual_pipeline:all"],
        source="launch",
        dry_run=False,
    )

    latest_launch = recorded["mission"]["latest_launch"]
    assert latest_launch["accepted_install_run_ids"] == ["install-run-9"]
    assert latest_launch["accepted_manual_run_ids"] == ["manual-run-3"]
    assert latest_launch["accepted_install_run_count"] == 1
    assert latest_launch["accepted_manual_run_count"] == 1


def test_model_setup_mission_memory_snapshot_filters_scope_and_reset(tmp_path) -> None:
    memory = ModelSetupMissionMemory(state_path=str(tmp_path / "model_setup_missions.json"))
    primary = _mission_payload(workspace_root="E:/J.A.R.V.I.S", manifest_path="E:/J.A.R.V.I.S/JARVIS_BACKEND/Models to Download.txt")
    secondary = _mission_payload(workspace_root="E:/Alt", manifest_path="E:/Alt/manifest.txt")

    primary_row = memory.record(mission_payload=primary)["mission"]
    memory.record(mission_payload=secondary)

    scoped = memory.snapshot(
        workspace_root="E:/J.A.R.V.I.S",
        manifest_path="E:/J.A.R.V.I.S/JARVIS_BACKEND/Models to Download.txt",
    )
    assert scoped["status"] == "success"
    assert scoped["count"] == 1
    assert scoped["items"][0]["mission_id"] == primary_row["mission_id"]

    resolved = memory.resolve_resume_reference(
        workspace_root="E:/J.A.R.V.I.S",
        manifest_path="E:/J.A.R.V.I.S/JARVIS_BACKEND/Models to Download.txt",
    )
    assert resolved["status"] == "success"
    assert resolved["mission"]["mission_id"] == primary_row["mission_id"]

    reset = memory.reset(mission_id=primary_row["mission_id"])
    assert reset["status"] == "success"
    assert reset["removed"] == 1


def test_model_setup_mission_memory_tracks_auto_resume_candidates(tmp_path) -> None:
    memory = ModelSetupMissionMemory(state_path=str(tmp_path / "model_setup_missions.json"))
    mission = _mission_payload()
    workspace = dict(mission["workspace"])
    workspace["required_providers"] = [
        {"provider": "groq", "ready": True},
        {"provider": "elevenlabs", "ready": True},
    ]
    mission["workspace"] = workspace
    mission["manual_pipeline"] = {"status": "success", "items": []}

    recorded = memory.record(mission_payload=mission, source="launch", dry_run=False)
    row = recorded["mission"]

    assert row["auto_resume_candidate"] is True
    assert row["resume_trigger"] == "ready_now"
    assert row["resume_blockers"] == []
    assert row["recovery_profile"] == "workspace_scaffold"
    assert "workspace scaffold" in str(row["auto_resume_reason"]).lower()

    snapshot = memory.snapshot(
        workspace_root="E:/J.A.R.V.I.S",
        manifest_path="E:/J.A.R.V.I.S/JARVIS_BACKEND/Models to Download.txt",
    )
    assert snapshot["status"] == "success"
    assert snapshot["auto_resume_candidate_count"] == 1
    assert snapshot["latest_auto_resume_candidate"]["mission_id"] == row["mission_id"]


def test_model_setup_mission_memory_detects_stalled_install_runs(tmp_path) -> None:
    memory = ModelSetupMissionMemory(state_path=str(tmp_path / "model_setup_missions.json"))
    mission = _mission_payload()
    workspace = dict(mission["workspace"])
    workspace["required_providers"] = [{"provider": "groq", "ready": True}]
    mission["workspace"] = workspace
    mission["manual_pipeline"] = {"status": "success", "items": []}
    mission["actions"] = [
        {
            "id": "launch_setup_install:auto",
            "kind": "launch_setup_install",
            "stage": "setup",
            "title": "Run auto-installable model setup tasks",
            "status": "ready",
            "auto_runnable": True,
        }
    ]
    mission["install_runs"] = {
        "status": "success",
        "active_count": 1,
        "items": [
            {
                "run_id": "install-run-stalled",
                "status": "running",
                "task": "embedding",
                "updated_at": "2026-03-15T00:00:00+00:00",
                "last_progress_at": "2026-03-15T00:00:00+00:00",
                "last_event_name": "item_started",
                "progress_event_count": 2,
                "progress": {
                    "current_item_name": "embedding-all-mpnet-base-v2",
                    "completed_items": 1,
                    "total_items": 4,
                    "percent": 25.0,
                    "message": "installing embedding-all-mpnet-base-v2",
                },
            }
        ],
    }

    recorded = memory.record(mission_payload=mission, source="resume", dry_run=False)
    row = recorded["mission"]

    assert row["status"] == "running"
    assert row["recovery_profile"] == "install_stalled"
    assert row["stalled_run_count"] == 1
    assert row["watch_active_runs"] is True
    assert row["active_run_health"] == "stalled"
    assert row["resume_trigger"] == "stalled_active_runs"
    assert row["resume_blockers"] == ["stalled_runs"]
    assert row["next_poll_s"] > 0
    assert row["active_run_summary"]["stalled_count"] == 1
    assert row["active_run_summary"]["top_health"] == "stalled"
    assert row["active_install_runs"][0]["health"] == "stalled"
