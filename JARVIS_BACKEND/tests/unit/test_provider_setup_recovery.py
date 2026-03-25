from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.python.desktop_api import DesktopBackendService


class _DummyProviderCredentials:
    def __init__(self) -> None:
        self.update_calls: List[Dict[str, Any]] = []
        self.refresh_calls = 0

    def update_provider_credentials(self, **kwargs: Any) -> Dict[str, Any]:
        self.update_calls.append(dict(kwargs))
        return {
            "status": "success",
            "updated_fields": ["api_key"],
            "warnings": [],
            "storage": {"keystore_enabled": True},
            "provider_status": {
                "provider": "huggingface",
                "ready": True,
                "present": True,
                "source": "config",
                "format_valid": True,
            },
        }

    def snapshot(self) -> Dict[str, Any]:
        return {
            "status": "success",
            "providers": {
                "huggingface": {
                    "provider": "huggingface",
                    "ready": True,
                    "present": True,
                    "source": "config",
                    "format_valid": True,
                    "missing_requirements": [],
                }
            },
        }

    def refresh(self, *, overwrite_env: bool = False) -> Dict[str, Any]:  # noqa: ARG002
        self.refresh_calls += 1
        return self.snapshot()


class _DummyModelRegistry:
    def __init__(self) -> None:
        self.refresh_force_values: List[bool] = []

    def refresh_environment(self, *, force: bool = False) -> Dict[str, Any]:
        self.refresh_force_values.append(bool(force))
        return {"status": "success", "force": bool(force)}

    def requirement_manifest_snapshot(self) -> Dict[str, Any]:
        return {
            "status": "success",
            "path": "E:/J.A.R.V.I.S/JARVIS_BACKEND/Models to Download.txt",
            "workspace_root": "E:/J.A.R.V.I.S",
            "providers": ["huggingface"],
        }


class _DummyVerifier:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []

    def verify(
        self,
        *,
        provider: str,
        repo_items: Optional[List[Dict[str, Any]]] = None,
        force_refresh: bool = False,
        timeout_s: float = 8.0,
    ) -> Dict[str, Any]:
        self.calls.append(
            {
                "provider": provider,
                "repo_items": [dict(item) for item in repo_items or []],
                "force_refresh": bool(force_refresh),
                "timeout_s": float(timeout_s),
            }
        )
        return {
            "status": "success",
            "provider": provider,
            "verified": True,
            "summary": "Verified Hugging Face access token.",
            "provider_status": {
                "provider": provider,
                "ready": True,
                "present": True,
            },
        }

    def latest_map(self, providers: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        names = providers or []
        return {
            str(name).strip().lower(): {
                "status": "success",
                "verified": True,
                "checked_at": "2026-03-15T09:00:00+00:00",
                "summary": "Verified Hugging Face access token.",
            }
            for name in names
            if str(name).strip()
        }


def _service() -> DesktopBackendService:
    service = DesktopBackendService.__new__(DesktopBackendService)
    service.provider_credentials = _DummyProviderCredentials()
    service.provider_verifier = _DummyVerifier()
    service.model_registry = _DummyModelRegistry()
    service._coworker_stack_runtime_state = {}
    service.model_local_inventory = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "items": [],
        "provider_credentials": {
            "providers": {
                "huggingface": {
                    "provider": "huggingface",
                    "ready": True,
                    "present": True,
                    "verification_verified": True,
                }
            }
        },
    }
    service.coworker_stack_status = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "stack_name": "desktop_agent",
        "summary": {"ready_task_count": 1, "blocked_task_count": 0},
    }
    service.coworker_stack_recovery_plan = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "summary": {"auto_runnable_count": 1, "manual_action_count": 0},
        "actions": [],
    }
    service.model_setup_plan = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "providers": [
            {
                "provider": "huggingface",
                "ready": True,
                "present": True,
                "required_by_manifest": True,
                "optional": False,
                "fields": [{"name": "api_key", "label": "Access Token"}],
                "usage_hint": "Used for gated/private Hugging Face model access.",
                "task_scope": ["reasoning"],
                "verification_status": "",
                "verification_verified": False,
                "verification_checked_at": "",
                "verification_summary": "",
            }
        ],
        "items": [
            {
                "key": "reasoning-llama",
                "task": "reasoning",
                "name": "Llama-3.1-8B-Instruct",
                "source_kind": "huggingface",
                "source_ref": "meta-llama/Llama-3.1-8B-Instruct",
                "automation_ready": True,
            }
        ],
    }
    service.model_setup_workspace = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "provider_credentials": {
            "providers": {
                "huggingface": {
                    "provider": "huggingface",
                    "ready": True,
                    "present": True,
                    "verification_status": "success",
                    "verification_verified": True,
                    "verification_checked_at": "2026-03-15T09:00:00+00:00",
                    "verification_summary": "Verified Hugging Face access token.",
                }
            }
        },
    }
    service.model_setup_preflight = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "summary": {
            "launchable_count": 1,
            "blocked_count": 0,
            "warning_count": 0,
        },
        "items": [
            {
                "key": "reasoning-llama",
                "status": "ready",
                "launch_ready": True,
                "blockers": [],
                "warnings": [],
            }
        ],
    }
    service.model_setup_manual_pipeline = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "summary": {"manual_count": 0},
        "items": [],
    }
    service.model_setup_mission = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "actions": [
            {
                "id": "install:reasoning-llama",
                "kind": "launch_setup_install",
                "title": "Install reasoning model",
                "status": "ready",
                "auto_runnable": True,
                "task": "reasoning",
                "item_keys": ["reasoning-llama"],
                "estimated_impact_score": 30.0,
            },
            {
                "id": "verify_provider:huggingface",
                "kind": "verify_provider_credentials",
                "title": "Verify Hugging Face access",
                "status": "ready",
                "auto_runnable": True,
                "provider": "huggingface",
                "task": "reasoning",
                "item_keys": ["reasoning-llama"],
                "estimated_impact_score": 26.0,
            },
        ],
        "stored_mission": {
            "resume_ready": True,
            "manual_attention_required": False,
            "recovery_profile": "ready",
            "recovery_hint": "Install reasoning model",
        },
    }
    return service


def test_verify_provider_credentials_returns_recovery_bundle_for_provider_items() -> None:
    service = _service()

    payload = service.verify_provider_credentials(
        provider="huggingface",
        include_present=True,
        item_keys=["reasoning-llama"],
        force_refresh=True,
        refresh_remote=True,
    )

    assert payload["status"] == "success"
    assert payload["affected_item_keys"] == ["reasoning-llama"]
    assert payload["affected_tasks"] == ["reasoning"]
    assert payload["setup_recovery"]["launchable_count"] == 1
    assert payload["setup_recovery"]["resume_ready"] is True
    assert payload["setup_recovery"]["next_action"]["kind"] == "launch_setup_install"
    verifier = service.provider_verifier
    assert verifier.calls[0]["provider"] == "huggingface"
    assert verifier.calls[0]["repo_items"][0]["source_ref"] == "meta-llama/Llama-3.1-8B-Instruct"


def test_update_provider_credentials_can_verify_and_merge_recovery_bundle() -> None:
    service = _service()
    verify_calls: List[Dict[str, Any]] = []

    def _verify(**kwargs: Any) -> Dict[str, Any]:
        verify_calls.append(dict(kwargs))
        return {
            "status": "partial",
            "verification": {
                "verified": False,
                "summary": "Credential could not access meta-llama/Llama-3.1-8B-Instruct.",
            },
            "provider_setup": {"provider": "huggingface", "ready": True},
            "provider_credentials": {"providers": {"huggingface": {"ready": True}}},
            "inventory": {"status": "success", "items": []},
            "workspace": {"status": "success"},
            "setup_plan": {"status": "success"},
            "preflight": {"status": "success"},
            "manual_pipeline": {"status": "success"},
            "mission": {"status": "success"},
            "affected_item_keys": ["reasoning-llama"],
            "affected_tasks": ["reasoning"],
            "setup_actions": [{"kind": "review_provider_access"}],
            "setup_recovery": {
                "launchable_count": 0,
                "blocked_count": 1,
                "next_action": {
                    "kind": "review_provider_access",
                    "title": "Review Hugging Face repository access",
                },
            },
        }

    service.verify_provider_credentials = _verify  # type: ignore[method-assign]

    payload = service.update_provider_credentials(
        provider="huggingface",
        api_key="hf_" + ("A1b2C3d4E5f6G7h8" * 2),
        verify_after_update=True,
        include_present=True,
        item_keys=["reasoning-llama"],
        refresh_remote=True,
        timeout_s=12.0,
    )

    assert payload["status"] == "success"
    assert payload["verification_requested"] is True
    assert payload["verification_status"] == "partial"
    assert payload["affected_item_keys"] == ["reasoning-llama"]
    assert payload["setup_recovery"]["next_action"]["kind"] == "review_provider_access"
    assert verify_calls[0]["provider"] == "huggingface"
    assert verify_calls[0]["item_keys"] == ["reasoning-llama"]
    assert verify_calls[0]["refresh_remote"] is True
    registry = service.model_registry
    assert registry.refresh_force_values == [True]


def test_provider_setup_recovery_launch_executes_ready_provider_actions() -> None:
    service = _service()
    install_calls: List[Dict[str, Any]] = []

    def _launch_install(
        *,
        task: str = "",
        item_keys: Optional[List[str]] = None,
        dry_run: bool = False,
        force: bool = False,
        include_present: bool = False,
        limit: int = 200,
        refresh_remote: bool = False,
        remote_timeout_s: float = 8.0,
        verify_integrity: bool = False,
        manifest_path: str = "",
        workspace_root: str = "",
    ) -> Dict[str, Any]:
        install_calls.append(
            {
                "task": task,
                "item_keys": list(item_keys or []),
                "dry_run": dry_run,
                "force": force,
                "include_present": include_present,
                "limit": limit,
                "refresh_remote": refresh_remote,
                "remote_timeout_s": remote_timeout_s,
                "verify_integrity": verify_integrity,
                "manifest_path": manifest_path,
                "workspace_root": workspace_root,
            }
        )
        return {
            "status": "accepted",
            "task": task,
            "selected_item_keys": list(item_keys or []),
            "run": {"run_id": "install-run-01"},
        }

    service.model_setup_install_launch = _launch_install  # type: ignore[method-assign]
    service.model_setup_workspace_scaffold = lambda **kwargs: {"status": "success", "dry_run": kwargs.get("dry_run", False)}  # type: ignore[method-assign]
    service.model_setup_manual_run_launch = lambda **kwargs: {"status": "accepted", "task": kwargs.get("task", "")}  # type: ignore[method-assign]

    payload = service.provider_setup_recovery_launch(
        provider="huggingface",
        include_present=True,
        item_keys=["reasoning-llama"],
    )

    assert payload["status"] == "success"
    assert payload["provider"] == "huggingface"
    assert payload["selected_action_ids"] == ["verify_provider:huggingface"]
    assert "launch_setup_install:auto" in payload["continued_action_ids"]
    assert install_calls[0]["item_keys"] == ["reasoning-llama"]
    assert payload["setup_recovery"]["auto_runnable_ready_action_ids"]
    assert isinstance(payload["recovery_before"], dict)
    assert isinstance(payload["recovery_after"], dict)
    assert payload["coworker_stack"]["status"] == "success"
    assert payload["coworker_recovery"]["status"] == "success"


def test_verify_provider_credentials_can_auto_continue_and_include_coworker_context() -> None:
    service = _service()
    recovery_calls: List[Dict[str, Any]] = []

    def _recovery_launch(**kwargs: Any) -> Dict[str, Any]:
        recovery_calls.append(dict(kwargs))
        return {
            "status": "success",
            "provider": "huggingface",
            "task": "reasoning",
            "executed_count": 1,
            "affected_item_keys": ["reasoning-llama"],
            "affected_tasks": ["reasoning"],
            "setup_actions": [{"id": "install:reasoning-llama", "kind": "launch_setup_install"}],
            "setup_recovery": {
                "launchable_count": 1,
                "auto_runnable_ready_count": 1,
                "next_action": {"kind": "launch_setup_install", "title": "Install reasoning model"},
            },
            "workspace": {"status": "success"},
            "setup_plan": {"status": "success"},
            "preflight": {"status": "success"},
            "manual_pipeline": {"status": "success"},
            "mission": {"status": "success"},
            "updated_mission": {"status": "success"},
            "provider_setup": {"provider": "huggingface", "ready": True, "present": True},
            "provider_credentials": {"providers": {"huggingface": {"ready": True}}},
            "inventory": {"status": "success", "items": []},
            "coworker_stack": {"status": "success"},
            "coworker_recovery": {"status": "success"},
            "message": "continued provider recovery",
        }

    service.provider_setup_recovery_launch = _recovery_launch  # type: ignore[method-assign]

    payload = service.verify_provider_credentials(
        provider="huggingface",
        include_present=True,
        item_keys=["reasoning-llama"],
        continue_setup_recovery=True,
        include_coworker_status=True,
    )

    assert payload["status"] == "success"
    assert payload["continue_setup_recovery_requested"] is True
    assert payload["continue_setup_recovery_status"] == "success"
    assert payload["continue_followup_actions_requested"] is True
    assert payload["continue_followup_actions_status"] == "skipped"
    assert payload["setup_recovery"]["next_action"]["kind"] == "launch_setup_install"
    assert payload["recovery_launch"]["executed_count"] == 1
    assert payload["coworker_stack"]["status"] == "success"
    assert payload["coworker_recovery"]["status"] == "success"
    assert recovery_calls[0]["provider"] == "huggingface"
    assert recovery_calls[0]["item_keys"] == ["reasoning-llama"]
    assert recovery_calls[0]["continue_followup_actions"] is True


def test_desktop_machine_execute_provider_followthrough_executes_ready_items_and_reports_manual_input() -> None:
    service = _service()
    verify_calls: List[Dict[str, Any]] = []

    service._desktop_machine_onboarding_provider_actions = lambda **_kwargs: {
        "items": [
            {
                "provider": "huggingface",
                "state": "ready",
                "present": True,
                "verified": False,
                "summary": "Hugging Face token is ready for verification.",
            },
            {
                "provider": "openrouter",
                "state": "needs_input",
                "present": False,
                "verified": False,
                "summary": "OpenRouter API key still required.",
            },
        ]
    }

    def _verify(**kwargs: Any) -> Dict[str, Any]:
        verify_calls.append(dict(kwargs))
        return {
            "status": "success",
            "verification_status": "success",
            "verification": {"verified": True, "summary": "Verified Hugging Face token."},
            "continue_setup_recovery_status": "success",
            "provider_setup": {"provider": "huggingface", "ready": True, "present": True},
            "setup_recovery": {"launchable_count": 1},
            "message": "provider verified",
        }

    service.verify_provider_credentials = _verify  # type: ignore[method-assign]

    payload = service._desktop_machine_execute_provider_followthrough(
        profile={"status": "success"},
        model_selection={"selected_item_keys": ["reasoning-llama"]},
        task="reasoning",
        limit=48,
        continue_followup_actions=True,
        max_followup_waves=3,
        refresh_remote=True,
        source="unit_test",
    )

    assert payload["status"] == "success"
    assert payload["selected_provider_count"] == 2
    assert payload["executed_count"] == 1
    assert payload["verified_count"] == 1
    assert payload["recovery_continued_count"] == 1
    assert payload["manual_input_count"] == 1
    assert verify_calls[0]["provider"] == "huggingface"
    assert verify_calls[0]["item_keys"] == ["reasoning-llama"]
    assert verify_calls[0]["continue_setup_recovery"] is True
    assert verify_calls[0]["continue_followup_actions"] is True
    assert verify_calls[0]["refresh_remote"] is True
    assert any(item["status"] == "manual_input_required" for item in payload["items"])


def test_provider_setup_recovery_launch_can_cascade_followup_actions() -> None:
    service = _service()
    bundle_calls: List[Dict[str, Any]] = []
    execution_calls: List[Dict[str, Any]] = []

    initial_mission = {
        "status": "success",
        "actions": [
            {
                "id": "verify_provider:huggingface",
                "kind": "verify_provider_credentials",
                "title": "Verify Hugging Face access",
                "status": "ready",
                "auto_runnable": True,
                "provider": "huggingface",
                "task": "reasoning",
                "item_keys": ["reasoning-llama"],
            }
        ],
        "stored_mission": {
            "resume_ready": True,
            "manual_attention_required": False,
            "recovery_profile": "ready",
            "recovery_hint": "Verify Hugging Face access",
        },
    }

    def _bundle(**kwargs: Any) -> Dict[str, Any]:
        bundle_calls.append(dict(kwargs))
        if len(bundle_calls) == 1:
            return {
                "provider_setup": {"provider": "huggingface", "ready": True, "present": True},
                "provider_credentials": {"providers": {"huggingface": {"ready": True}}},
                "inventory": {"status": "success", "items": []},
                "workspace": {"status": "success"},
                "setup_plan": {"status": "success"},
                "preflight": {"status": "success"},
                "manual_pipeline": {"status": "success"},
                "mission": initial_mission,
                "affected_item_keys": ["reasoning-llama"],
                "affected_tasks": ["reasoning"],
                "setup_actions": [
                    {
                        "id": "verify_provider:huggingface",
                        "kind": "verify_provider_credentials",
                        "status": "ready",
                        "auto_runnable": True,
                    }
                ],
                "setup_recovery": {
                    "setup_action_ids": ["verify_provider:huggingface"],
                    "auto_runnable_ready_action_ids": ["verify_provider:huggingface"],
                    "launchable_count": 1,
                    "manual_attention_required": False,
                    "next_action": {
                        "id": "verify_provider:huggingface",
                        "kind": "verify_provider_credentials",
                        "title": "Verify Hugging Face access",
                    },
                },
            }
        return {
            "provider_setup": {"provider": "huggingface", "ready": True, "present": True},
            "provider_credentials": {"providers": {"huggingface": {"ready": True}}},
            "inventory": {"status": "success", "items": []},
            "workspace": {"status": "success"},
            "setup_plan": {"status": "success"},
            "preflight": {"status": "success"},
            "manual_pipeline": {"status": "success"},
            "mission": {"status": "success", "actions": [], "stored_mission": {"resume_ready": False}},
            "affected_item_keys": ["reasoning-llama"],
            "affected_tasks": ["reasoning"],
            "setup_actions": [],
            "setup_recovery": {
                "setup_action_ids": [],
                "auto_runnable_ready_action_ids": [],
                "launchable_count": 0,
                "manual_attention_required": False,
                "next_action": {},
            },
        }

    def _execute(**kwargs: Any) -> Dict[str, Any]:
        execution_calls.append(dict(kwargs))
        if len(execution_calls) == 1:
            return {
                "status": "success",
                "selected_action_ids": ["verify_provider:huggingface"],
                "executed_count": 1,
                "skipped_count": 0,
                "error_count": 0,
                "items": [{"action_id": "verify_provider:huggingface", "status": "success"}],
                "mission": initial_mission,
                "updated_mission": {
                    "status": "success",
                    "actions": [
                        {
                            "id": "launch_setup_install:auto",
                            "kind": "launch_setup_install",
                            "title": "Run auto-installable model setup tasks",
                            "status": "ready",
                            "auto_runnable": True,
                            "task": "reasoning",
                            "item_keys": ["reasoning-llama"],
                        }
                    ],
                },
                "workspace": {"status": "success"},
                "setup_plan": {"status": "success"},
                "mission_record": {"mission_id": "mission-01"},
                "mission_history": {"status": "success", "items": []},
            }
        return {
            "status": "success",
            "selected_action_ids": ["launch_setup_install:auto"],
            "executed_count": 1,
            "skipped_count": 0,
            "error_count": 0,
            "items": [{"action_id": "launch_setup_install:auto", "status": "success"}],
            "mission": {"status": "success"},
            "updated_mission": {"status": "success", "actions": []},
            "workspace": {"status": "success"},
            "setup_plan": {"status": "success"},
            "mission_record": {"mission_id": "mission-01"},
            "mission_history": {"status": "success", "items": []},
        }

    service._provider_setup_recovery_bundle = _bundle  # type: ignore[method-assign]
    service._execute_model_setup_mission_actions = _execute  # type: ignore[method-assign]
    service.model_setup_mission_history = lambda **kwargs: {"status": "success", "items": []}  # type: ignore[method-assign]

    payload = service.provider_setup_recovery_launch(
        provider="huggingface",
        task="reasoning",
        item_keys=["reasoning-llama"],
        selected_action_ids=["verify_provider:huggingface"],
        continue_followup_actions=True,
        max_followup_waves=4,
    )

    assert payload["status"] == "success"
    assert payload["executed_count"] == 2
    assert payload["selected_action_ids"] == ["verify_provider:huggingface"]
    assert payload["continued_action_ids"] == ["launch_setup_install:auto"]
    assert payload["executed_action_ids"] == ["verify_provider:huggingface", "launch_setup_install:auto"]
    assert payload["continue_followup_actions_requested"] is True
    assert payload["continue_followup_actions_status"] == "success"
    assert payload["continuation"]["waves_executed"] == 1
    assert payload["continuation"]["stop_reason"] == "no_ready_followup_actions"
    assert execution_calls[0]["selected_action_ids"] == ["verify_provider:huggingface"]
    assert execution_calls[1]["selected_action_ids"] == ["launch_setup_install:auto"]
