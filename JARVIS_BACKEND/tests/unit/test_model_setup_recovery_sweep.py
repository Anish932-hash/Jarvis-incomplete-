from __future__ import annotations

from types import SimpleNamespace

from backend.python import desktop_api as desktop_api_module
from backend.python.desktop_api import DesktopBackendService


def test_auto_resume_model_setup_mission_can_cascade_followup_actions() -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)
    service.model_setup_mission_resume_advice = lambda **kwargs: {  # noqa: ARG005
        "status": "ready",
        "can_resume_now": True,
        "can_auto_resume_now": True,
        "auto_resume_candidate": True,
        "resume_ready": True,
        "resume_trigger": "ready_now",
        "resume_blockers": [],
        "selected_action_ids": ["launch_setup_install:auto"],
        "message": "The stored mission can resume immediately.",
        "resolved_mission": {"mission_id": "msm_demo"},
    }
    service.model_setup_mission_resume = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "selected_action_ids": ["launch_setup_install:auto"],
        "executed_count": 1,
        "skipped_count": 0,
        "error_count": 0,
        "items": [{"action_id": "launch_setup_install:auto", "status": "success"}],
        "message": "resumed setup mission",
        "updated_mission": {"status": "success"},
        "mission_record": {"mission_id": "msm_demo", "resume_ready": True},
        "resume_advice": {"status": "ready", "can_auto_resume_now": True},
    }
    service._cascade_model_setup_followup_actions = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "executed_count": 1,
        "skipped_count": 0,
        "error_count": 0,
        "items": [{"action_id": "verify_provider:huggingface", "status": "success"}],
        "continued_action_ids": ["verify_provider:huggingface"],
        "final_payload": {
            "updated_mission": {"status": "success", "stage": "post_followup"},
            "mission_record": {"mission_id": "msm_demo", "resume_ready": False, "auto_resume_candidate": False},
            "resume_advice": {
                "status": "idle",
                "can_resume_now": False,
                "can_auto_resume_now": False,
                "message": "No additional auto-resumable setup actions are ready right now.",
            },
        },
    }

    payload = service.auto_resume_model_setup_mission(
        mission_id="msm_demo",
        continue_followup_actions=True,
        max_followup_waves=3,
    )

    assert payload["status"] == "success"
    assert payload["auto_resume_attempted"] is True
    assert payload["auto_resume_triggered"] is True
    assert payload["initial_resume_advice"]["status"] == "ready"
    assert payload["resume_advice"]["status"] == "idle"
    assert payload["continue_followup_actions_status"] == "success"
    assert payload["continued_action_ids"] == ["verify_provider:huggingface"]
    assert payload["executed_action_ids"] == [
        "launch_setup_install:auto",
        "verify_provider:huggingface",
    ]
    assert payload["executed_count"] == 2
    assert "Continued 1 follow-up action" in str(payload["message"])


def test_model_setup_mission_recovery_sweep_runs_auto_resume_until_idle() -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)
    advice_calls: list[dict] = []
    auto_resume_calls: list[dict] = []

    ready_advice = {
        "status": "ready",
        "mission_id": "msm_demo",
        "can_resume_now": True,
        "can_auto_resume_now": True,
        "auto_resume_candidate": True,
        "resume_ready": True,
        "resume_trigger": "ready_now",
        "resume_blockers": [],
        "selected_action_ids": ["launch_setup_install:auto"],
        "message": "The stored mission can resume immediately.",
        "resolved_mission": {"mission_id": "msm_demo"},
    }
    idle_advice = {
        "status": "idle",
        "mission_id": "msm_demo",
        "can_resume_now": False,
        "can_auto_resume_now": False,
        "auto_resume_candidate": False,
        "resume_ready": False,
        "resume_trigger": "settled",
        "resume_blockers": [],
        "selected_action_ids": [],
        "message": "No additional auto-resumable setup actions are ready right now.",
        "resolved_mission": {"mission_id": "msm_demo"},
    }

    def _history(**kwargs):  # noqa: ANN001
        return {
            "status": "success",
            "count": 1,
            "items": [{"mission_id": "msm_demo", "recovery_profile": "workspace_scaffold"}],
            "filters": dict(kwargs),
        }

    def _advice(**kwargs):  # noqa: ANN001
        advice_calls.append(dict(kwargs))
        return ready_advice if len(advice_calls) == 1 else idle_advice

    def _auto_resume(**kwargs):  # noqa: ANN001
        auto_resume_calls.append(dict(kwargs))
        return {
            "status": "success",
            "auto_resume_attempted": True,
            "auto_resume_triggered": True,
            "continue_followup_actions_status": "success",
            "continued_action_ids": ["verify_provider:huggingface"],
            "executed_action_ids": ["launch_setup_install:auto", "verify_provider:huggingface"],
            "executed_count": 2,
            "skipped_count": 0,
            "error_count": 0,
            "message": "auto-resumed setup mission and continued follow-up actions",
            "resume_advice": idle_advice,
            "updated_mission": {"status": "success"},
        }

    service.model_setup_mission_history = _history
    service.model_setup_mission_resume_advice = _advice
    service.auto_resume_model_setup_mission = _auto_resume

    payload = service.model_setup_mission_recovery_sweep(
        mission_id="msm_demo",
        current_scope=False,
        max_auto_resume_passes=3,
        continue_followup_actions=True,
        max_followup_waves=4,
    )

    assert payload["status"] == "success"
    assert payload["auto_resume_attempted_count"] == 1
    assert payload["auto_resume_triggered_count"] == 1
    assert payload["continue_followup_actions_requested"] is True
    assert payload["continued_action_ids"] == ["verify_provider:huggingface"]
    assert payload["passes_executed"] == 1
    assert payload["passes"][0]["continue_followup_actions_status"] == "success"
    assert payload["final_resume_advice"]["status"] == "idle"
    assert payload["stop_reason"] == "no_auto_resume_candidate"
    assert auto_resume_calls[0]["continue_followup_actions"] is True
    assert auto_resume_calls[0]["max_followup_waves"] == 4


def test_model_setup_resume_advice_recomposes_against_resolved_manifest_scope() -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)
    compose_calls: list[dict] = []
    scoped_manifest = "E:/AltScope/JARVIS_BACKEND/Models to Download.txt"
    scoped_root = "E:/AltScope"

    def _compose(**kwargs):  # noqa: ANN001
        compose_calls.append(dict(kwargs))
        manifest_path = str(kwargs.get("manifest_path", "") or "E:/Current/JARVIS_BACKEND/Models to Download.txt")
        workspace_root = str(kwargs.get("workspace_root", "") or "E:/Current")
        return {
            "status": "success",
            "workspace": {
                "workspace_root": workspace_root,
                "manifest_path": manifest_path,
            },
            "actions": [
                {
                    "id": "launch_setup_install:auto",
                    "status": "ready",
                    "auto_runnable": True,
                }
            ],
        }

    service._compose_model_setup_mission_payload = _compose
    service._attach_model_setup_mission_recovery = lambda payload, limit: payload  # noqa: ARG005
    service.model_setup_mission_memory = SimpleNamespace(
        resolve_resume_reference=lambda **kwargs: {
            "status": "success",
            "mission": {
                "mission_id": "msm_alt",
                "workspace_root": scoped_root,
                "manifest_path": scoped_manifest,
                "pending_auto_action_ids": ["launch_setup_install:auto"],
                "auto_resume_candidate": True,
                "resume_ready": True,
                "resume_trigger": "ready_now",
                "resume_blockers": [],
            },
        }
    )

    payload = service.model_setup_mission_resume_advice(
        mission_id="msm_alt",
        current_scope=False,
    )

    assert payload["status"] == "ready"
    assert payload["can_auto_resume_now"] is True
    assert compose_calls[1]["manifest_path"] == scoped_manifest
    assert compose_calls[1]["workspace_root"] == scoped_root
    assert payload["mission"]["workspace"]["manifest_path"] == scoped_manifest


def test_model_setup_resume_executes_against_resolved_manifest_scope() -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)
    compose_calls: list[dict] = []
    scoped_manifest = "E:/AltScope/JARVIS_BACKEND/Models to Download.txt"
    scoped_root = "E:/AltScope"

    def _compose(**kwargs):  # noqa: ANN001
        compose_calls.append(dict(kwargs))
        manifest_path = str(kwargs.get("manifest_path", "") or "E:/Current/JARVIS_BACKEND/Models to Download.txt")
        workspace_root = str(kwargs.get("workspace_root", "") or "E:/Current")
        return {
            "status": "success",
            "workspace": {
                "workspace_root": workspace_root,
                "manifest_path": manifest_path,
            },
            "actions": [
                {
                    "id": "launch_setup_install:auto",
                    "status": "ready",
                    "auto_runnable": True,
                }
            ],
        }

    service._compose_model_setup_mission_payload = _compose
    service.model_setup_mission_memory = SimpleNamespace(
        resolve_resume_reference=lambda **kwargs: {
            "status": "success",
            "mission": {
                "mission_id": "msm_alt",
                "workspace_root": scoped_root,
                "manifest_path": scoped_manifest,
                "pending_auto_action_ids": ["launch_setup_install:auto"],
                "auto_resume_candidate": True,
                "resume_ready": True,
                "resume_trigger": "ready_now",
                "resume_blockers": [],
            },
        },
        record=lambda **kwargs: {"status": "success", "mission": kwargs.get("mission_payload", {})},
    )
    service._execute_model_setup_mission_actions = lambda **kwargs: {  # noqa: ANN001
        "status": "success",
        "selected_action_ids": list(kwargs.get("selected_action_ids", [])),
        "mission_scope": kwargs["mission_payload"]["workspace"]["manifest_path"],
    }

    payload = service.model_setup_mission_resume(
        mission_id="msm_alt",
    )

    assert payload["status"] == "success"
    assert compose_calls[1]["manifest_path"] == scoped_manifest
    assert compose_calls[1]["workspace_root"] == scoped_root
    assert payload["mission_scope"] == scoped_manifest


def test_execute_model_setup_mission_actions_forwards_scope_to_launch_handlers(monkeypatch) -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)
    scoped_manifest = "E:/AltScope/JARVIS_BACKEND/Models to Download.txt"
    scoped_root = "E:/AltScope"
    captured: dict[str, dict] = {}

    def _fake_execute_model_setup_mission(**kwargs):  # noqa: ANN001
        kwargs["execute_workspace_scaffold"](False)
        kwargs["launch_setup_install"]("reasoning", ["reasoning-llama"], False)
        kwargs["launch_manual_pipeline"]("reasoning", ["reasoning-llama"], True)
        return {
            "status": "success",
            "selected_action_ids": ["launch_setup_install:auto"],
            "executed_count": 1,
            "skipped_count": 0,
            "error_count": 0,
            "items": [],
        }

    monkeypatch.setattr(desktop_api_module, "execute_model_setup_mission", _fake_execute_model_setup_mission)

    def _workspace_scaffold(**kwargs):  # noqa: ANN001
        captured["workspace_scaffold"] = dict(kwargs)
        return {"status": "success"}

    def _install_launch(**kwargs):  # noqa: ANN001
        captured["install_launch"] = dict(kwargs)
        return {"status": "accepted"}

    def _manual_launch(**kwargs):  # noqa: ANN001
        captured["manual_launch"] = dict(kwargs)
        return {"status": "accepted"}

    service.model_setup_workspace_scaffold = _workspace_scaffold
    service.model_setup_install_launch = _install_launch
    service.model_setup_manual_run_launch = _manual_launch
    service.verify_provider_credentials = lambda **kwargs: {"status": "success"}  # noqa: ARG005
    service._compose_model_setup_mission_payload = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "workspace": {
            "workspace_root": scoped_root,
            "manifest_path": scoped_manifest,
        },
    }
    service._attach_model_setup_mission_recovery = lambda payload, limit: payload  # noqa: ARG005
    service.model_setup_workspace = lambda **kwargs: {"status": "success"}  # noqa: ARG005
    service.model_setup_plan = lambda **kwargs: {"status": "success"}  # noqa: ARG005
    service.model_setup_mission_history = lambda **kwargs: {"status": "success"}  # noqa: ARG005
    service.model_setup_mission_memory = SimpleNamespace(
        record=lambda **kwargs: {"status": "success", "mission": {"mission_id": "msm_alt"}},
        snapshot=lambda **kwargs: {"status": "success", "items": [], "filters": dict(kwargs)},
    )

    payload = service._execute_model_setup_mission_actions(
        mission_payload={
            "status": "success",
            "workspace": {
                "workspace_root": scoped_root,
                "manifest_path": scoped_manifest,
            },
        },
        selected_action_ids=["launch_setup_install:auto"],
    )

    assert payload["status"] == "success"
    assert captured["workspace_scaffold"]["manifest_path"] == scoped_manifest
    assert captured["workspace_scaffold"]["workspace_root"] == scoped_root
    assert captured["install_launch"]["manifest_path"] == scoped_manifest
    assert captured["install_launch"]["workspace_root"] == scoped_root
    assert captured["manual_launch"]["manifest_path"] == scoped_manifest
    assert captured["manual_launch"]["workspace_root"] == scoped_root


def test_compose_model_setup_mission_payload_uses_scoped_run_history() -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)
    scoped_manifest = "E:/AltScope/JARVIS_BACKEND/Models to Download.txt"
    scoped_root = "E:/AltScope"
    install_calls: list[dict] = []
    manual_calls: list[dict] = []

    service.model_setup_workspace = lambda **kwargs: {"status": "success", "workspace_root": kwargs.get("workspace_root"), "manifest_path": kwargs.get("manifest_path")}  # noqa: ARG005
    service.model_setup_plan = lambda **kwargs: {"status": "success", "items": []}  # noqa: ARG005
    service.model_setup_preflight = lambda **kwargs: {"status": "success", "items": []}  # noqa: ARG005
    service.model_setup_manual_pipeline = lambda **kwargs: {"status": "success", "items": []}  # noqa: ARG005
    service.model_setup_install_runs = lambda **kwargs: install_calls.append(dict(kwargs)) or {"status": "success", "active_count": 0, "items": []}  # noqa: ARG005
    service.model_setup_manual_runs = lambda **kwargs: manual_calls.append(dict(kwargs)) or {"status": "success", "active_count": 0, "items": []}  # noqa: ARG005

    payload = service._compose_model_setup_mission_payload(
        manifest_path=scoped_manifest,
        workspace_root=scoped_root,
    )

    assert payload["status"] == "success"
    assert install_calls[0]["manifest_path"] == scoped_manifest
    assert install_calls[0]["workspace_root"] == scoped_root
    assert manual_calls[0]["manifest_path"] == scoped_manifest
    assert manual_calls[0]["workspace_root"] == scoped_root


def test_model_setup_recovery_watchdog_auto_resumes_ready_missions_and_tracks_watchers() -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)

    rows = [
        {
            "mission_id": "msm_ready",
            "workspace_root": "E:/ScopeReady",
            "manifest_path": "E:/ScopeReady/JARVIS_BACKEND/Models to Download.txt",
            "status": "running",
            "recovery_profile": "resume_ready",
            "recovery_priority": 9,
            "auto_resume_candidate": True,
            "resume_ready": True,
            "updated_at": "2026-03-15T10:00:00+00:00",
        },
        {
            "mission_id": "msm_watch",
            "workspace_root": "E:/ScopeWatch",
            "manifest_path": "E:/ScopeWatch/JARVIS_BACKEND/Models to Download.txt",
            "status": "running",
            "recovery_profile": "install_running",
            "recovery_priority": 6,
            "watch_active_runs": True,
            "active_run_count": 1,
            "updated_at": "2026-03-15T09:59:00+00:00",
        },
        {
            "mission_id": "msm_stalled",
            "workspace_root": "E:/ScopeStalled",
            "manifest_path": "E:/ScopeStalled/JARVIS_BACKEND/Models to Download.txt",
            "status": "running",
            "recovery_profile": "install_stalled",
            "recovery_priority": 8,
            "stalled_run_count": 1,
            "active_run_count": 1,
            "updated_at": "2026-03-15T09:58:00+00:00",
        },
    ]

    service.model_setup_mission_history = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "count": len(rows),
        "total": len(rows),
        "items": rows,
        "filters": dict(kwargs),
    }

    def _advice(**kwargs):  # noqa: ANN001
        mission_id = str(kwargs.get("mission_id", "") or "").strip()
        if mission_id == "msm_ready":
            return {
                "status": "ready",
                "mission_id": mission_id,
                "can_resume_now": True,
                "can_auto_resume_now": True,
                "auto_resume_candidate": True,
                "resume_ready": True,
                "resume_trigger": "ready_now",
                "resume_blockers": [],
                "selected_action_ids": ["launch_setup_install:auto"],
                "message": "Ready to continue immediately.",
            }
        if mission_id == "msm_watch":
            return {
                "status": "watch",
                "mission_id": mission_id,
                "can_resume_now": False,
                "can_auto_resume_now": False,
                "watch_active_runs": True,
                "active_run_count": 1,
                "waiting_run_count": 1,
                "message": "Waiting on active setup runs.",
            }
        return {
            "status": "stalled",
            "mission_id": mission_id,
            "can_resume_now": False,
            "can_auto_resume_now": False,
            "active_run_count": 1,
            "stalled_run_count": 1,
            "active_run_health": "stalled",
            "message": "One of the setup runs looks stalled.",
        }

    service.model_setup_mission_resume_advice = _advice
    service.auto_resume_model_setup_mission = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "auto_resume_attempted": True,
        "auto_resume_triggered": True,
        "continue_followup_actions_status": "success",
        "continued_action_ids": ["verify_provider:huggingface"],
        "executed_action_ids": ["launch_setup_install:auto", "verify_provider:huggingface"],
        "executed_count": 2,
        "skipped_count": 0,
        "error_count": 0,
        "message": "auto-resumed setup mission",
        "resume_advice": {
            "status": "idle",
            "can_resume_now": False,
            "can_auto_resume_now": False,
            "message": "No additional auto-resumable setup actions are ready right now.",
        },
    }

    payload = service.model_setup_mission_recovery_watchdog(
        current_scope=False,
        max_missions=5,
        max_auto_resumes=2,
    )

    assert payload["status"] == "success"
    assert payload["evaluated_count"] == 3
    assert payload["auto_resume_attempted_count"] == 1
    assert payload["auto_resume_triggered_count"] == 1
    assert payload["watch_count"] == 1
    assert payload["stalled_count"] == 1
    assert payload["idle_count"] == 1
    assert payload["ready_count"] == 0
    assert payload["triggered_mission_ids"] == ["msm_ready"]
    assert payload["watched_mission_ids"] == ["msm_watch"]
    assert payload["stalled_mission_ids"] == ["msm_stalled"]
    assert payload["latest_triggered_payload"]["auto_resume_triggered"] is True
    ready_result = next(item for item in payload["results"] if item["mission_id"] == "msm_ready")
    assert ready_result["classification_after"] == "idle"


def test_model_setup_recovery_watchdog_reports_ready_backlog_when_auto_resume_limit_is_hit() -> None:
    service = DesktopBackendService.__new__(DesktopBackendService)

    rows = [
        {
            "mission_id": "msm_ready_1",
            "workspace_root": "E:/ScopeOne",
            "manifest_path": "E:/ScopeOne/JARVIS_BACKEND/Models to Download.txt",
            "status": "running",
            "recovery_profile": "resume_ready",
            "recovery_priority": 9,
            "auto_resume_candidate": True,
            "resume_ready": True,
            "updated_at": "2026-03-15T10:00:00+00:00",
        },
        {
            "mission_id": "msm_ready_2",
            "workspace_root": "E:/ScopeTwo",
            "manifest_path": "E:/ScopeTwo/JARVIS_BACKEND/Models to Download.txt",
            "status": "running",
            "recovery_profile": "resume_ready",
            "recovery_priority": 8,
            "auto_resume_candidate": True,
            "resume_ready": True,
            "updated_at": "2026-03-15T09:59:00+00:00",
        },
    ]

    service.model_setup_mission_history = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "count": len(rows),
        "total": len(rows),
        "items": rows,
        "filters": dict(kwargs),
    }
    service.model_setup_mission_resume_advice = lambda **kwargs: {  # noqa: ARG005
        "status": "ready",
        "mission_id": str(kwargs.get("mission_id", "") or "").strip(),
        "can_resume_now": True,
        "can_auto_resume_now": True,
        "auto_resume_candidate": True,
        "resume_ready": True,
        "resume_trigger": "ready_now",
        "resume_blockers": [],
        "selected_action_ids": ["launch_setup_install:auto"],
        "message": "Ready to continue immediately.",
    }
    service.auto_resume_model_setup_mission = lambda **kwargs: {  # noqa: ARG005
        "status": "success",
        "auto_resume_attempted": True,
        "auto_resume_triggered": True,
        "message": "auto-resumed setup mission",
        "resume_advice": {
            "status": "idle",
            "can_resume_now": False,
            "can_auto_resume_now": False,
            "message": "No additional auto-resumable setup actions are ready right now.",
        },
    }

    payload = service.model_setup_mission_recovery_watchdog(
        current_scope=False,
        max_missions=4,
        max_auto_resumes=1,
    )

    assert payload["status"] == "success"
    assert payload["auto_resume_triggered_count"] == 1
    assert payload["ready_count"] == 1
    assert payload["triggered_mission_ids"] == ["msm_ready_1"]
    assert payload["ready_mission_ids"] == ["msm_ready_2"]
    assert payload["stop_reason"] == "max_auto_resumes_reached"
