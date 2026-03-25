from __future__ import annotations

from backend.python.core.desktop_vm_manager import DesktopVMManager


class _StubLauncher:
    def resolve_launch_target(self, app_name: str):
        return {"status": "success", "requested_app": app_name, "resolution": "launch_memory", "kind": "path"}

    def launch(self, app_name: str):
        return {"status": "success", "requested_app": app_name, "launch_method": "launch_memory"}


def test_desktop_vm_manager_inventory_plan_and_prepare(tmp_path, monkeypatch) -> None:
    manager = DesktopVMManager(store_path=str(tmp_path / "desktop_vm_manager.json"))
    monkeypatch.setattr("backend.python.core.desktop_vm_manager.shutil.which", lambda value: r"C:\Tools\VBoxManage.exe" if value == "VBoxManage.exe" else "")

    saved = manager.update_guest_profile(
        guest_name="Ubuntu Dev VM",
        provider="virtualbox",
        guest_os="linux",
        control_mode="provider_console",
        provider_app_name="VirtualBox",
        remote_endpoint="",
        enable_learning=True,
        source="unit_test",
    )
    assert saved["status"] == "success"
    assert saved["guest"]["guest_name"] == "Ubuntu Dev VM"

    inventory = manager.inventory_snapshot(
        system_profile={"virtualization": {"virtualization_firmware_enabled": True}},
        app_inventory={
            "items": [
                {"display_name": "VirtualBox", "canonical_name": "virtualbox", "path": r"C:\Program Files\Oracle\VirtualBox\VirtualBox.exe"},
            ]
        },
        launch_memory={"items": []},
        query="ubuntu",
        limit=12,
        task="linux",
        source="unit_test",
    )
    assert inventory["status"] == "success"
    assert inventory["summary"]["provider_count"] >= 1
    assert inventory["summary"]["ready_guest_count"] == 1
    assert inventory["items"][0]["provider"] == "virtualbox"
    assert inventory["items"][0]["readiness_status"] == "ready"

    machine_profile = {
        "ai_runtime_profile": {
            "status": "partial",
            "summary": {
                "ready_stack_count": 2,
                "blocked_stack_count": 1,
                "action_required_task_count": 1,
                "reasoning_runtime_ready": False,
            },
        },
        "multimodal_memory": {
            "summary": {
                "vision_runtime_available": True,
                "vision_loaded_model_count": 1,
                "vision_memory_app_count": 2,
                "weird_app_memory_app_count": 1,
                "knowledge_store_entry_count": 4,
                "knowledge_store_control_count": 18,
                "knowledge_store_command_count": 9,
                "knowledge_store_vector_count": 27,
                "knowledge_low_coverage_app_count": 2,
                "knowledge_semantic_ready_app_count": 3,
            }
        },
        "app_learning_plan": {
            "plan": {
                "summary": {
                    "semantic_guided_count": 2,
                    "semantic_followup_count": 1,
                    "memory_mission_status_counts": {"strong": 2, "cold": 1},
                    "top_memory_mission_queries": {"settings": 2, "preferences": 1},
                    "top_memory_mission_hotkeys": {"Alt+F": 2, "Ctrl+F": 1},
                }
            }
        },
        "setup_followthrough_memory": {
            "followthrough_status": "required",
            "followthrough_recommended": True,
            "followthrough_required": True,
            "setup_execution_remaining_ready_total": 2,
            "provider_blocked_total": 1,
            "setup_followup_total": 3,
            "reason_codes": ["recent_setup_followthrough_required", "recent_provider_blocked_pressure"],
        },
        "continuation_memory": {
            "continuation_status": "recommended",
            "continuation_recommended": True,
            "continuation_required": False,
            "app_learning_continuation_wave_total": 2,
            "vm_prepare_continuation_wave_total": 1,
            "continuation_retry_total": 1,
            "continuation_provider_blocked_total": 0,
            "continuation_setup_followup_total": 1,
            "continuation_memory_followthrough_total": 3,
            "reason_codes": ["recent_continuation_recommended", "recent_continuation_memory_followthrough"],
            "top_memory_mission_queries": {"settings": 2},
            "top_memory_mission_hotkeys": {"Alt+F": 1},
        },
    }
    plan = manager.build_vm_control_plan(
        inventory=inventory,
        task="linux",
        query="ubuntu",
        max_targets=4,
        machine_profile=machine_profile,
    )
    assert plan["status"] == "success"
    assert plan["count"] == 1
    assert plan["items"][0]["prepare_priority_band"] == "high"
    assert plan["items"][0]["guest_learning_profile"] == "linux_desktop_explore"
    assert plan["items"][0]["expected_route_profile"] == "linux_vm_desktop_control"
    assert plan["summary"]["execution_mode_counts"]["hybrid_ready"] == 1
    assert plan["summary"]["structured_memory_low_coverage_guest_count"] == 1
    assert plan["items"][0]["provider_model_readiness"]["vision_runtime_available"] is True
    assert plan["items"][0]["provider_model_readiness"]["vision_loaded_model_count"] == 1
    assert plan["items"][0]["provider_model_readiness"]["multimodal_memory_pressure"] == 3
    assert plan["items"][0]["provider_model_readiness"]["structured_memory_control_count"] == 18
    assert plan["items"][0]["provider_model_readiness"]["structured_memory_vector_count"] == 27
    assert plan["items"][0]["provider_model_readiness"]["memory_guidance_status"] == "partial"
    assert "semantic_memory_ready" in plan["items"][0]["provider_model_readiness"]["memory_guidance_reason_codes"]
    assert "learning_semantic_guidance_available" in plan["items"][0]["provider_model_readiness"]["memory_guidance_reason_codes"]
    assert "memory_assisted_vm_route" in plan["items"][0]["provider_model_readiness"]["ai_route_reason_codes"]
    assert plan["items"][0]["provider_model_readiness"]["app_learning_semantic_guided_count"] == 2
    assert plan["items"][0]["provider_model_readiness"]["app_learning_semantic_followup_count"] == 1
    assert plan["items"][0]["provider_model_readiness"]["app_learning_memory_mission_status_counts"]["strong"] == 2
    assert plan["items"][0]["provider_model_readiness"]["app_learning_top_memory_mission_queries"][0] == "settings"
    assert "Alt+F" in plan["items"][0]["provider_model_readiness"]["app_learning_top_memory_mission_hotkeys"]
    assert plan["items"][0]["provider_model_readiness"]["ai_runtime_status"] == "partial"
    assert plan["items"][0]["provider_model_readiness"]["ai_runtime_blocked_stack_count"] == 1
    assert "warm_local_reasoning_runtime" in plan["items"][0]["provider_model_readiness"]["setup_followup_codes"]
    assert plan["items"][0]["provider_model_readiness"]["ai_route_status"] == "fallback"
    assert plan["items"][0]["provider_model_readiness"]["selected_ai_runtime_band"] == "accessibility"
    assert plan["items"][0]["provider_model_readiness"]["recent_setup_followthrough_status"] == "required"
    assert plan["items"][0]["provider_model_readiness"]["recent_setup_followthrough_required"] is True
    assert plan["items"][0]["provider_model_readiness"]["recent_setup_remaining_ready_count"] == 2
    assert plan["items"][0]["provider_model_readiness"]["recent_setup_provider_blocked_count"] == 1
    assert "recent_setup_followthrough_required" in plan["items"][0]["provider_model_readiness"]["ai_route_reason_codes"]
    assert plan["items"][0]["provider_model_readiness"]["recent_continuation_status"] == "recommended"
    assert plan["items"][0]["provider_model_readiness"]["recent_continuation_recommended"] is True
    assert plan["items"][0]["provider_model_readiness"]["recent_continuation_learning_wave_total"] == 2
    assert plan["items"][0]["provider_model_readiness"]["recent_continuation_memory_followthrough_count"] == 3
    assert "recent_continuation_recommended" in plan["items"][0]["provider_model_readiness"]["ai_route_reason_codes"]
    assert plan["items"][0]["provider_model_readiness"]["memory_guided_route"] is False
    assert plan["items"][0]["provider_model_readiness"]["memory_assisted_route"] is True
    assert plan["items"][0]["provider_model_readiness"]["memory_route_alignment_status"] == "assisted"
    assert plan["items"][0]["memory_mission"]["status"] == "partial"
    assert plan["items"][0]["memory_mission"]["seed_query"] == "desktop settings"
    assert "settings" in plan["items"][0]["memory_mission"]["query_hints"]
    assert plan["summary"]["ai_route_status_counts"]["fallback"] == 1
    assert plan["summary"]["ai_route_runtime_band_counts"]["accessibility"] == 1
    assert plan["summary"]["memory_guidance_status_counts"]["partial"] == 1
    assert plan["summary"]["memory_guided_route_count"] == 0
    assert plan["summary"]["memory_assisted_route_count"] == 1
    assert plan["summary"]["memory_route_alignment_counts"]["assisted"] == 1
    assert plan["summary"]["memory_followthrough_guest_count"] == 1
    assert plan["summary"]["memory_mission_status_counts"]["partial"] == 1
    assert plan["summary"]["top_memory_mission_queries"]["settings"] >= 1
    assert plan["summary"]["top_memory_mission_hotkeys"]["Alt+F"] >= 1
    assert plan["defaults"]["memory_followthrough_enabled"] is True
    assert plan["defaults"]["max_surface_waves"] == 5
    assert plan["defaults"]["max_probe_controls"] == 4
    assert plan["defaults"]["memory_mission_status_counts"]["partial"] == 1
    assert plan["next_actions"][0]["kind"] == "deepen_vm_control_learning"

    prepared = manager.prepare_guest_control(
        inventory=inventory,
        guest_name="Ubuntu Dev VM",
        app_launcher=_StubLauncher(),
        ensure_provider_launch=True,
        query="desktop settings",
        source="unit_test",
        task="linux",
        machine_profile=machine_profile,
    )
    assert prepared["status"] == "success"
    assert prepared["summary"]["provider_launch_ready"] is True
    assert prepared["summary"]["attach_strategy"] == "provider_console"
    assert prepared["summary"]["guest_learning_profile"] == "linux_desktop_explore"
    assert prepared["summary"]["expected_route_profile"] == "linux_vm_desktop_control"
    assert prepared["summary"]["provider_model_readiness"]["vision_runtime_available"] is True
    assert prepared["summary"]["provider_model_readiness"]["ai_runtime_status"] == "partial"
    assert prepared["summary"]["provider_model_readiness"]["structured_memory_semantic_ready_count"] == 3
    assert prepared["summary"]["memory_guidance_status"] == "partial"
    assert prepared["summary"]["provider_model_readiness"]["app_learning_semantic_guided_count"] == 2
    assert prepared["summary"]["provider_model_readiness"]["recent_continuation_status"] == "recommended"
    assert prepared["summary"]["provider_model_readiness"]["recent_continuation_top_memory_mission_queries"][0] == "settings"
    assert prepared["summary"]["ai_route_status"] == "fallback"
    assert prepared["summary"]["selected_ai_runtime_band"] == "accessibility"
    assert prepared["summary"]["memory_guided_route"] is False
    assert prepared["summary"]["memory_assisted_route"] is True
    assert prepared["summary"]["memory_mission"]["status"] == "partial"
    assert prepared["summary"]["memory_mission"]["seed_query"] == "desktop settings"
    assert prepared["summary"]["memory_route_alignment_status"] == "assisted"
    assert prepared["summary"]["memory_assisted_route_count"] == 1
    assert prepared["summary"]["memory_followthrough_recommended"] is True
    assert prepared["summary"]["recommended_max_surface_waves"] == 5
    assert prepared["summary"]["recommended_max_probe_controls"] == 4
