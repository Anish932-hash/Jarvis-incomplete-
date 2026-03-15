from __future__ import annotations

from backend.python.inference.model_setup_mission import build_model_setup_mission
from backend.python.inference.model_setup_mission import execute_model_setup_mission


def _workspace_payload() -> dict:
    return {
        "status": "success",
        "summary": {
            "missing_directory_count": 1,
            "workspace_ready": False,
            "stack_ready": False,
            "readiness_score": 42,
        },
        "directory_actions": [
            {
                "kind": "create_directory",
                "name": "custom_intent",
                "task": "intent",
                "path": "E:/J.A.R.V.I.S/custom_intents",
                "safe": True,
            }
        ],
        "required_providers": [
            {
                "provider": "groq",
                "ready": True,
                "verification_status": "",
                "verification_verified": False,
                "verification_checked_at": "",
                "verification_summary": "",
            },
            {
                "provider": "elevenlabs",
                "ready": False,
                "missing_requirements": ["ELEVENLABS_VOICE_ID"],
            },
        ],
    }


def _setup_plan_payload() -> dict:
    return {
        "status": "success",
        "items": [
            {"key": "embedding-all-mpnet-base-v2", "automation_ready": True},
            {"key": "stt-whisper-large-v3", "automation_ready": True},
        ],
    }


def _preflight_payload() -> dict:
    return {
        "status": "success",
        "summary": {"blocked_count": 0},
        "items": [],
    }


def _manual_pipeline_payload() -> dict:
    return {
        "status": "success",
        "items": [
            {
                "key": "reasoning-meta-llama-3.1-8b-gguf",
                "status": "blocked",
                "blockers": ["A verified Hugging Face access token is required before this source can be downloaded."],
            }
        ],
    }


def test_model_setup_mission_composes_workspace_provider_and_setup_actions() -> None:
    mission = build_model_setup_mission(
        workspace_payload=_workspace_payload(),
        setup_plan_payload=_setup_plan_payload(),
        preflight_payload=_preflight_payload(),
        manual_pipeline_payload=_manual_pipeline_payload(),
        install_runs_payload={"status": "success", "active_count": 0, "items": []},
        manual_runs_payload={"status": "success", "active_count": 0, "items": []},
    )

    assert mission["status"] == "success"
    assert mission["mission_status"] == "ready"
    assert int(mission["summary"]["ready_action_count"]) == 3
    assert int(mission["summary"]["manual_action_count"]) == 2
    assert int(mission["summary"]["blocked_action_count"]) == 0

    action_ids = {
        str(action.get("id", "")).strip()
        for action in mission["actions"]
        if isinstance(action, dict)
    }
    assert "scaffold_workspace" in action_ids
    assert "verify_provider:groq" in action_ids
    assert "configure_provider:elevenlabs" in action_ids
    assert "launch_setup_install:auto" in action_ids
    assert "review_manual_pipeline_blockers" in action_ids


def test_model_setup_mission_execute_runs_ready_auto_actions_in_order() -> None:
    mission = build_model_setup_mission(
        workspace_payload=_workspace_payload(),
        setup_plan_payload=_setup_plan_payload(),
        preflight_payload=_preflight_payload(),
        manual_pipeline_payload={"status": "success", "items": []},
        install_runs_payload={"status": "success", "active_count": 0, "items": []},
        manual_runs_payload={"status": "success", "active_count": 0, "items": []},
    )

    execution = execute_model_setup_mission(
        mission_payload=mission,
        execute_workspace_scaffold=lambda dry_run: {"status": "success", "dry_run": dry_run},
        launch_setup_install=lambda task_name, item_keys, dry_run: {
            "status": "accepted",
            "task": task_name,
            "item_keys": item_keys,
            "dry_run": dry_run,
        },
        launch_manual_pipeline=lambda task_name, item_keys, dry_run: {
            "status": "accepted",
            "task": task_name,
            "item_keys": item_keys,
            "dry_run": dry_run,
        },
        verify_provider_credentials=lambda provider_name, task_name, item_keys: {
            "status": "success",
            "provider": provider_name,
            "task": task_name,
            "item_keys": item_keys,
            "verification": {"verified": True, "summary": "verified"},
        },
        dry_run=False,
        continue_on_error=True,
    )

    assert execution["status"] == "success"
    assert int(execution["executed_count"]) == 3
    assert int(execution["error_count"]) == 0
    successful_ids = [item["action_id"] for item in execution["items"] if bool(item.get("ok"))]
    assert successful_ids == ["scaffold_workspace", "verify_provider:groq", "launch_setup_install:auto"]


def test_model_setup_mission_uses_preflight_ready_subset_and_surfaces_gated_blockers() -> None:
    mission = build_model_setup_mission(
        workspace_payload={
            "status": "success",
            "summary": {
                "missing_directory_count": 0,
                "workspace_ready": True,
                "stack_ready": False,
                "readiness_score": 61,
            },
            "directory_actions": [],
            "required_providers": [],
        },
        setup_plan_payload={
            "status": "success",
            "items": [
                {"key": "embedding-main", "automation_ready": True},
                {"key": "reasoning-llama", "automation_ready": True},
            ],
        },
        preflight_payload={
            "status": "success",
            "summary": {"blocked_count": 1},
            "items": [
                {
                    "key": "embedding-main",
                    "status": "ready",
                    "launch_ready": True,
                    "remote_probe": {
                        "credential_state": "not_required",
                        "acquisition_stage": "ready_public",
                    },
                    "blockers": [],
                },
                {
                    "key": "reasoning-llama",
                    "status": "blocked",
                    "launch_ready": False,
                    "remote_probe": {
                        "credential_state": "missing",
                        "acquisition_stage": "blocked_missing_auth",
                    },
                    "blockers": ["Remote source requires a configured Hugging Face access token before automation can download it."],
                },
            ],
        },
        manual_pipeline_payload={"status": "success", "items": []},
        install_runs_payload={"status": "success", "active_count": 0, "items": []},
        manual_runs_payload={"status": "success", "active_count": 0, "items": []},
    )

    actions = {
        str(action.get("id", "")).strip(): action
        for action in mission["actions"]
        if isinstance(action, dict)
    }

    install_action = actions["launch_setup_install:auto"]
    assert install_action["item_keys"] == ["embedding-main"]
    assert install_action["status"] == "ready"
    assert any("skipped" in str(entry).lower() for entry in install_action.get("warnings", []))

    assert actions["configure_provider:huggingface"]["provider"] == "huggingface"
    assert actions["configure_provider:huggingface"]["item_keys"] == ["reasoning-llama"]
    assert actions["review_setup_install_blockers"]["item_keys"] == ["reasoning-llama"]
    assert int(mission["summary"]["acquisition_ready_count"]) == 1
    assert int(mission["summary"]["acquisition_blocked_count"]) == 1
    assert int(mission["summary"]["auth_missing_count"]) == 1


def test_model_setup_mission_adds_verification_action_for_access_denied_provider() -> None:
    mission = build_model_setup_mission(
        workspace_payload={
            "status": "success",
            "summary": {
                "missing_directory_count": 0,
                "workspace_ready": True,
                "stack_ready": False,
                "readiness_score": 74,
            },
            "directory_actions": [],
            "required_providers": [],
        },
        setup_plan_payload={
            "status": "success",
            "items": [{"key": "reasoning-llama", "automation_ready": True}],
        },
        preflight_payload={
            "status": "success",
            "summary": {"blocked_count": 1},
            "items": [
                {
                    "key": "reasoning-llama",
                    "status": "blocked",
                    "launch_ready": False,
                    "strategy": "huggingface_snapshot",
                    "remote_probe": {
                        "repo_id": "meta-llama/Llama-3.1-8B-Instruct",
                        "credential_state": "access_denied",
                    },
                    "blockers": ["Configured Hugging Face credentials could not access this repository."],
                    "warnings": [],
                }
            ],
        },
        manual_pipeline_payload={"status": "success", "items": []},
        install_runs_payload={"status": "success", "active_count": 0, "items": []},
        manual_runs_payload={"status": "success", "active_count": 0, "items": []},
    )

    actions = {
        str(action.get("id", "")).strip(): action
        for action in mission["actions"]
        if isinstance(action, dict)
    }

    verify_action = actions["verify_provider:huggingface"]
    assert verify_action["kind"] == "verify_provider_credentials"
    assert verify_action["item_keys"] == ["reasoning-llama"]
    assert verify_action["status"] == "ready"
