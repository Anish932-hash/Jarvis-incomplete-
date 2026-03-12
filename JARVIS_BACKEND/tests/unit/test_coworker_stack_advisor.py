from __future__ import annotations

from backend.python.inference.coworker_stack_advisor import CoworkerStackAdvisor


def test_coworker_stack_status_marks_local_runtime_action_required() -> None:
    advisor = CoworkerStackAdvisor()

    payload = advisor.build_status(
        stack_name="desktop_agent",
        mission_profile="balanced",
        route_bundle={
            "status": "success",
            "items": [
                {
                    "status": "success",
                    "task": "reasoning",
                    "provider": "local",
                    "model": "local-llm",
                    "selected_path": r"E:\J.A.R.V.I.S\reasoning\model.gguf",
                    "route_policy": {
                        "matched": True,
                        "profile_id": "reasoning-local-qwen",
                        "recommended_template_id": "llama_server",
                        "local_route_viable": True,
                        "review_required": False,
                        "blacklisted": False,
                    },
                    "local_launch_profile_id": "reasoning-local-qwen",
                    "local_launch_template_id": "llama_server",
                },
                {
                    "status": "success",
                    "task": "embedding",
                    "provider": "local",
                    "model": "all-mpnet-base-v2",
                    "selected_path": r"E:\J.A.R.V.I.S\embeddings\all-mpnet-base-v2",
                },
            ],
        },
        runtime_supervisors={
            "reasoning": {
                "status": "success",
                "runtime_ready": False,
            }
        },
        active_runtimes={
            "reasoning": {
                "status": "idle",
                "ready": False,
                "active_profile_id": "",
                "active_template_id": "",
            }
        },
        provider_credentials={"providers": {}},
        setup_plan={"items": []},
    )

    assert payload["status"] == "partial"
    assert payload["summary"]["action_required_count"] == 1
    reasoning_row = next(item for item in payload["tasks"] if item["task"] == "reasoning")
    assert reasoning_row["status"] == "action_required"
    assert reasoning_row["auto_applyable"] is True
    assert reasoning_row["profile_id"] == "reasoning-local-qwen"
    assert reasoning_row["template_id"] == "llama_server"


def test_coworker_stack_status_blocks_missing_cloud_credentials() -> None:
    advisor = CoworkerStackAdvisor()

    payload = advisor.build_status(
        stack_name="desktop_agent",
        mission_profile="balanced",
        route_bundle={
            "status": "success",
            "items": [
                {
                    "status": "success",
                    "task": "reasoning",
                    "provider": "groq",
                    "model": "groq-llm",
                    "route_adjusted": True,
                    "requested_provider": "local",
                }
            ],
        },
        runtime_supervisors={},
        active_runtimes={},
        provider_credentials={"providers": {"groq": {"status": "missing", "ready": False}}},
        setup_plan={"items": []},
    )

    assert payload["status"] == "error"
    assert payload["summary"]["provider_blocker_count"] == 1
    reasoning_row = payload["tasks"][0]
    assert reasoning_row["status"] == "blocked"
    assert reasoning_row["requires_credentials"] is True
    assert any("Groq" in blocker or "groq" in blocker.lower() for blocker in reasoning_row["blockers"])


def test_coworker_stack_apply_executes_only_actionable_runtime_tasks() -> None:
    advisor = CoworkerStackAdvisor()
    calls: list[tuple[str, str, str]] = []

    apply_payload = advisor.apply_recommended(
        status_payload={
            "tasks": [
                {
                    "task": "reasoning",
                    "action_kind": "execute_launch_template",
                    "auto_applyable": True,
                    "already_active": False,
                    "profile_id": "reasoning-local-qwen",
                    "template_id": "llama_server",
                },
                {
                    "task": "vision",
                    "action_kind": "execute_launch_template",
                    "auto_applyable": True,
                    "already_active": True,
                    "profile_id": "vision-local-yolo",
                    "template_id": "warm_runtime",
                },
                {
                    "task": "tts",
                    "action_kind": "",
                    "auto_applyable": False,
                    "already_active": False,
                    "profile_id": "tts-local-kokoro",
                    "template_id": "http_bridge",
                },
            ]
        },
        execute_launch_template=lambda task_name, profile_id, template_id: calls.append((task_name, profile_id, template_id))
        or {"status": "success", "task": task_name, "profile_id": profile_id, "template_id": template_id},
    )

    assert apply_payload["status"] == "success"
    assert apply_payload["executed_count"] == 1
    assert calls == [("reasoning", "reasoning-local-qwen", "llama_server")]
