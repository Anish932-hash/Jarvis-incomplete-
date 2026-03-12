from __future__ import annotations

from backend.python.inference.coworker_stack_recovery import CoworkerStackRecoveryPlanner


def test_coworker_stack_recovery_plan_surfaces_runtime_install_manual_and_provider_actions() -> None:
    planner = CoworkerStackRecoveryPlanner()

    payload = planner.build_plan(
        status_payload={
            "status": "partial",
            "stack_name": "desktop_agent",
            "mission_profile": "balanced",
            "summary": {"score": 58.0, "blocked_task_count": 2},
            "tasks": [
                {
                    "task": "reasoning",
                    "provider": "local",
                    "status": "action_required",
                    "auto_applyable": True,
                    "already_active": False,
                    "action_kind": "execute_launch_template",
                    "profile_id": "reasoning-local-qwen",
                    "template_id": "llama_server",
                    "recommendations": ["Activate the local reasoning profile."],
                },
                {
                    "task": "vision",
                    "provider": "local",
                    "status": "blocked",
                    "route_adjusted": True,
                    "blockers": ["Install the missing local vision artifact from the setup planner before using this route."],
                },
                {
                    "task": "tts",
                    "provider": "local",
                    "status": "blocked",
                    "blockers": ["The local tts route still needs a manual model pipeline or conversion step before it can be activated."],
                },
            ],
            "provider_credentials": {
                "providers": {
                    "huggingface": {
                        "provider": "huggingface",
                        "ready": True,
                        "present": True,
                        "verification_checked_at": "",
                    }
                }
            },
            "setup_plan": {
                "providers": [
                    {
                        "provider": "huggingface",
                        "fields": [{"name": "api_key", "label": "Access Token"}],
                    }
                ],
                "items": [
                    {
                        "key": "vision-yolo",
                        "task": "vision",
                        "name": "yolov10x.pt",
                        "present": False,
                        "automation_ready": True,
                        "strategy": "direct_url",
                        "source_kind": "huggingface",
                        "path": r"E:\J.A.R.V.I.S\JARVIS_BACKEND\models\vision\yolov10x.pt",
                    },
                    {
                        "key": "tts-orpheus-3b-gguf",
                        "task": "tts",
                        "name": "Orpheus-3B-TTS.f16.gguf",
                        "present": False,
                        "automation_ready": False,
                        "strategy": "manual_quantization",
                        "source_kind": "huggingface",
                        "path": r"E:\J.A.R.V.I.S\tts\Orpheus-3B-TTS.f16.gguf",
                    },
                ],
            },
        },
        install_runs_payload={"items": []},
        manual_pipeline_payload={
            "status": "success",
            "summary": {"manual_count": 1},
            "upgrade_actions": [{"title": "Install llama-cpp-python", "status": "recommended"}],
            "items": [
                {
                    "key": "tts-orpheus-3b-gguf",
                    "task": "tts",
                    "name": "Orpheus-3B-TTS.f16.gguf",
                    "status": "warning",
                    "convertible": True,
                    "pipeline_kind": "hf_to_gguf",
                    "recommended_next_action": "Bootstrap llama.cpp and convert the upstream checkpoint.",
                    "commands": ["python convert_hf_to_gguf.py"],
                    "warnings": ["llama.cpp is not prepared yet; bootstrap commands are included below."],
                    "blockers": ["A verified Hugging Face access token is required before this source can be downloaded."],
                }
            ],
        },
    )

    action_kinds = {str(item.get("kind")) for item in payload["actions"]}

    assert payload["status"] == "partial"
    assert payload["summary"]["auto_runnable_count"] == 3
    assert "apply_runtime_template" in action_kinds
    assert "launch_setup_install" in action_kinds
    assert "manual_pipeline_review" in action_kinds
    assert "verify_provider_credentials" in action_kinds


def test_coworker_stack_recovery_plan_marks_matching_install_as_in_progress() -> None:
    planner = CoworkerStackRecoveryPlanner()

    payload = planner.build_plan(
        status_payload={
            "status": "partial",
            "summary": {"score": 40.0, "blocked_task_count": 1},
            "tasks": [
                {
                    "task": "vision",
                    "provider": "local",
                    "status": "blocked",
                }
            ],
            "provider_credentials": {"providers": {}},
            "setup_plan": {
                "items": [
                    {
                        "key": "vision-yolo",
                        "task": "vision",
                        "name": "yolov10x.pt",
                        "present": False,
                        "automation_ready": True,
                        "strategy": "direct_url",
                        "source_kind": "direct_url",
                        "path": r"E:\J.A.R.V.I.S\JARVIS_BACKEND\models\vision\yolov10x.pt",
                    }
                ]
            },
        },
        install_runs_payload={
            "items": [
                {
                    "run_id": "run-1",
                    "task": "vision",
                    "status": "running",
                    "selected_item_keys": [],
                }
            ]
        },
        manual_pipeline_payload={"status": "success", "items": [], "upgrade_actions": []},
    )

    install_action = next(item for item in payload["actions"] if item["kind"] == "launch_setup_install")

    assert install_action["status"] == "in_progress"
    assert install_action["auto_runnable"] is False


def test_coworker_stack_recovery_plan_can_launch_manual_pipeline_when_ready() -> None:
    planner = CoworkerStackRecoveryPlanner()

    payload = planner.build_plan(
        status_payload={
            "status": "partial",
            "summary": {"score": 41.0, "blocked_task_count": 1},
            "tasks": [
                {
                    "task": "tts",
                    "provider": "local",
                    "status": "blocked",
                    "route_adjusted": True,
                }
            ],
            "provider_credentials": {"providers": {}},
            "setup_plan": {
                "items": [
                    {
                        "key": "tts-orpheus-3b-gguf",
                        "task": "tts",
                        "name": "Orpheus-3B-TTS.f16.gguf",
                        "present": False,
                        "automation_ready": False,
                        "strategy": "manual_quantization",
                        "source_kind": "huggingface",
                        "path": r"E:\J.A.R.V.I.S\tts\Orpheus-3B-TTS.f16.gguf",
                    }
                ]
            },
        },
        install_runs_payload={"items": []},
        manual_pipeline_payload={
            "status": "success",
            "items": [
                {
                    "key": "tts-orpheus-3b-gguf",
                    "task": "tts",
                    "name": "Orpheus-3B-TTS.f16.gguf",
                    "status": "ready",
                    "pipeline_kind": "hf_to_gguf",
                    "commands": ["python convert_hf_to_gguf.py"],
                    "steps": [
                        {
                            "id": "convert-f16-gguf",
                            "status": "ready",
                            "commands": ["python convert_hf_to_gguf.py"],
                        }
                    ],
                }
            ],
            "upgrade_actions": [],
        },
        manual_runs_payload={"items": []},
    )

    manual_action = next(item for item in payload["actions"] if item.get("task") == "tts")

    assert manual_action["kind"] == "launch_manual_pipeline"
    assert manual_action["status"] == "ready"
    assert manual_action["auto_runnable"] is True
    assert manual_action["runnable_item_keys"] == ["tts-orpheus-3b-gguf"]


def test_coworker_stack_recovery_execute_runs_safe_actions_and_skips_manual_steps() -> None:
    planner = CoworkerStackRecoveryPlanner()
    calls: list[str] = []

    payload = planner.execute(
        plan_payload={
            "actions": [
                {
                    "id": "manual-tts",
                    "kind": "launch_manual_pipeline",
                    "stage": "manual",
                    "status": "ready",
                    "auto_runnable": True,
                    "priority": 85.0,
                    "task": "tts",
                    "item_keys": ["tts-orpheus-3b-gguf"],
                },
                {
                    "id": "provider-verify-huggingface",
                    "kind": "verify_provider_credentials",
                    "stage": "provider",
                    "status": "ready",
                    "auto_runnable": True,
                    "priority": 96.0,
                    "provider": "huggingface",
                    "primary_task": "tts",
                    "item_keys": ["tts-orpheus-3b-gguf"],
                },
                {
                    "id": "install-vision",
                    "kind": "launch_setup_install",
                    "stage": "setup",
                    "status": "ready",
                    "auto_runnable": True,
                    "priority": 90.0,
                    "task": "vision",
                    "item_keys": ["vision-yolo"],
                },
                {
                    "id": "runtime-reasoning",
                    "kind": "apply_runtime_template",
                    "stage": "runtime",
                    "status": "ready",
                    "auto_runnable": True,
                    "priority": 120.0,
                    "task": "reasoning",
                    "profile_id": "reasoning-local-qwen",
                    "template_id": "llama_server",
                },
            ]
        },
        execute_launch_template=lambda task_name, profile_id, template_id: calls.append(
            f"runtime:{task_name}:{profile_id}:{template_id}"
        )
        or {"status": "success"},
        launch_setup_install=lambda task_name, item_keys: calls.append(
            f"install:{task_name}:{','.join(item_keys or [])}"
        )
        or {"status": "accepted", "run": {"run_id": "install-run-1"}},
        launch_manual_pipeline=lambda task_name, item_keys: calls.append(
            f"manual:{task_name}:{','.join(item_keys or [])}"
        )
        or {"status": "accepted", "run": {"run_id": "manual-run-1"}},
        verify_provider_credentials=lambda provider_name, task_name, item_keys: calls.append(
            f"verify:{provider_name}:{task_name}:{','.join(item_keys or [])}"
        )
        or {"status": "success", "verification": {"verified": True}},
    )

    assert payload["status"] == "success"
    assert payload["executed_count"] == 4
    assert payload["skipped_count"] == 0
    assert calls == [
        "runtime:reasoning:reasoning-local-qwen:llama_server",
        "verify:huggingface:tts:tts-orpheus-3b-gguf",
        "install:vision:vision-yolo",
        "manual:tts:tts-orpheus-3b-gguf",
    ]
    assert payload["accepted_manual_run_count"] == 1
