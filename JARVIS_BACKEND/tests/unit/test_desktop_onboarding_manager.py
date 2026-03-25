from __future__ import annotations

from backend.python.core.desktop_onboarding_manager import DesktopOnboardingManager


def test_desktop_onboarding_manager_records_and_lists_runs(tmp_path) -> None:
    manager = DesktopOnboardingManager(store_path=str(tmp_path / "desktop_onboarding.json"))

    recorded = manager.record_run(
        {
            "status": "success",
            "task": "reasoning",
            "summary": {
                "provider_update_count": 1,
                "profile_setup_action_count": 2,
                "launch_seed_count": 2,
                "prepared_app_count": 3,
                "prepared_blocked_count": 1,
                "prepared_degraded_count": 2,
                "execution_action_count": 7,
                "execution_ready_count": 3,
                "execution_success_count": 2,
                "execution_manual_count": 1,
                "execution_blocked_count": 1,
                "execution_error_count": 0,
                "execution_memory_followthrough_count": 3,
                "execution_memory_guided_route_count": 2,
                "execution_memory_assisted_route_count": 1,
                "setup_action_count": 3,
                "setup_action_auto_runnable_count": 1,
                "setup_action_success_count": 1,
                "setup_action_manual_count": 1,
                "setup_action_blocked_count": 1,
                "ai_runtime_stack_count": 3,
                "ai_runtime_ready_stack_count": 2,
                "ai_runtime_blocked_stack_count": 1,
                "ai_runtime_action_required_task_count": 2,
                "ai_runtime_setup_action_count": 2,
                "ai_runtime_setup_auto_runnable_count": 1,
                "ai_runtime_setup_executed_count": 1,
                "ai_runtime_setup_success_count": 1,
                "ai_runtime_setup_error_count": 0,
                "setup_execution_selected_action_count": 2,
                "setup_execution_continued_action_count": 1,
                "setup_execution_remaining_ready_count": 1,
                "setup_execution_resume_ready": True,
                "multimodal_memory_app_count": 3,
                "multimodal_ocr_memory_app_count": 2,
                "multimodal_local_runtime_ready_app_count": 1,
                "multimodal_api_assist_app_count": 1,
                "multimodal_native_stabilization_app_count": 1,
                "multimodal_weird_app_count": 1,
                "multimodal_revalidation_target_count": 4,
                "multimodal_overdue_revalidation_count": 2,
                "multimodal_setup_action_count": 1,
                "multimodal_setup_auto_runnable_count": 1,
                "multimodal_setup_executed_count": 1,
                "multimodal_setup_success_count": 1,
                "multimodal_setup_loaded_model_count": 2,
                "app_learning_setup_aligned_count": 2,
                "app_learning_setup_boosted_count": 1,
                "app_learning_setup_constrained_count": 1,
                "app_learning_memory_followthrough_count": 2,
                "app_learning_remediation_retry_count": 1,
                "app_learning_remediation_provider_blocked_count": 1,
                "app_learning_remediation_setup_followup_count": 1,
                "prepared_setup_aligned_count": 2,
                "prepared_setup_boosted_count": 1,
                "prepared_setup_constrained_count": 1,
                "prepared_memory_followthrough_count": 2,
                "prepared_remediation_retry_count": 1,
                "prepared_remediation_provider_blocked_count": 0,
                "prepared_remediation_setup_followup_count": 1,
                "route_remediation_count": 2,
                "route_remediation_blocked_count": 1,
                "route_remediation_degraded_count": 1,
                "route_remediation_setup_followup_count": 2,
                "route_remediation_provider_blocked_count": 1,
                "route_remediation_resolved_count": 1,
                "route_remediation_improved_count": 1,
                "route_remediation_persistent_count": 0,
                "route_remediation_new_count": 0,
                "route_remediation_resolved_setup_followup_count": 1,
                "route_remediation_persistent_provider_blocked_count": 0,
                "continuation_count": 4,
                "continuation_auto_runnable_count": 3,
                "continuation_manual_count": 1,
                "continuation_retry_count": 2,
                "continuation_provider_blocked_count": 1,
                "continuation_setup_followup_count": 2,
                "vm_prepare_count": 2,
                "prepared_vm_control_count": 2,
                "vm_ready_guest_count": 1,
                "vm_attention_guest_count": 1,
                "vm_blocked_guest_count": 0,
                "vm_setup_followup_guest_count": 1,
                "vm_memory_followthrough_count": 1,
            },
        },
        source="machine_onboarding",
    )

    assert recorded["status"] == "success"
    assert recorded["source"] == "machine_onboarding"
    assert recorded["recorded_at"]

    latest = manager.latest_run()
    assert latest["task"] == "reasoning"

    history = manager.history(limit=4)
    assert history["status"] == "success"
    assert history["count"] == 1
    assert history["items"][0]["task"] == "reasoning"
    assert history["summary"]["status_counts"]["success"] == 1
    assert history["summary"]["source_counts"]["machine_onboarding"] == 1
    assert history["summary"]["provider_update_total"] == 1
    assert history["summary"]["launch_seed_total"] == 2
    assert history["summary"]["prepared_app_total"] == 3
    assert history["summary"]["prepared_blocked_total"] == 1
    assert history["summary"]["prepared_degraded_total"] == 2
    assert history["summary"]["execution_action_total"] == 7
    assert history["summary"]["execution_ready_total"] == 3
    assert history["summary"]["execution_success_total"] == 2
    assert history["summary"]["execution_manual_total"] == 1
    assert history["summary"]["execution_blocked_total"] == 1
    assert history["summary"]["execution_error_total"] == 0
    assert history["summary"]["execution_memory_followthrough_total"] == 3
    assert history["summary"]["execution_memory_guided_route_total"] == 2
    assert history["summary"]["execution_memory_assisted_route_total"] == 1
    assert history["summary"]["setup_action_total"] == 3
    assert history["summary"]["setup_action_auto_runnable_total"] == 1
    assert history["summary"]["setup_action_success_total"] == 1
    assert history["summary"]["setup_action_manual_total"] == 1
    assert history["summary"]["setup_action_blocked_total"] == 1
    assert history["summary"]["profile_setup_action_total"] == 2
    assert history["summary"]["ai_runtime_stack_total"] == 3
    assert history["summary"]["ai_runtime_ready_stack_total"] == 2
    assert history["summary"]["ai_runtime_blocked_stack_total"] == 1
    assert history["summary"]["ai_runtime_action_required_task_total"] == 2
    assert history["summary"]["ai_runtime_setup_action_total"] == 2
    assert history["summary"]["ai_runtime_setup_auto_runnable_total"] == 1
    assert history["summary"]["ai_runtime_setup_executed_total"] == 1
    assert history["summary"]["ai_runtime_setup_success_total"] == 1
    assert history["summary"]["ai_runtime_setup_error_total"] == 0
    assert history["summary"]["setup_execution_selected_action_total"] == 2
    assert history["summary"]["setup_execution_continued_action_total"] == 1
    assert history["summary"]["setup_execution_remaining_ready_total"] == 1
    assert history["summary"]["setup_execution_resume_ready_total"] == 1
    assert history["summary"]["multimodal_memory_app_total"] == 3
    assert history["summary"]["multimodal_ocr_memory_app_total"] == 2
    assert history["summary"]["multimodal_local_runtime_ready_app_total"] == 1
    assert history["summary"]["multimodal_api_assist_app_total"] == 1
    assert history["summary"]["multimodal_native_stabilization_app_total"] == 1
    assert history["summary"]["multimodal_weird_app_total"] == 1
    assert history["summary"]["multimodal_revalidation_target_total"] == 4
    assert history["summary"]["multimodal_overdue_revalidation_total"] == 2
    assert history["summary"]["multimodal_setup_action_total"] == 1
    assert history["summary"]["multimodal_setup_auto_runnable_total"] == 1
    assert history["summary"]["multimodal_setup_executed_total"] == 1
    assert history["summary"]["multimodal_setup_success_total"] == 1
    assert history["summary"]["multimodal_setup_loaded_model_total"] == 2
    assert history["summary"]["app_learning_setup_aligned_total"] == 2
    assert history["summary"]["app_learning_setup_boosted_total"] == 1
    assert history["summary"]["app_learning_setup_constrained_total"] == 1
    assert history["summary"]["app_learning_memory_followthrough_total"] == 2
    assert history["summary"]["app_learning_remediation_retry_total"] == 1
    assert history["summary"]["app_learning_remediation_provider_blocked_total"] == 1
    assert history["summary"]["app_learning_remediation_setup_followup_total"] == 1
    assert history["summary"]["prepared_setup_aligned_total"] == 2
    assert history["summary"]["prepared_setup_boosted_total"] == 1
    assert history["summary"]["prepared_setup_constrained_total"] == 1
    assert history["summary"]["prepared_memory_followthrough_total"] == 2
    assert history["summary"]["prepared_remediation_retry_total"] == 1
    assert history["summary"]["prepared_remediation_provider_blocked_total"] == 0
    assert history["summary"]["prepared_remediation_setup_followup_total"] == 1
    assert history["summary"]["route_remediation_total"] == 2
    assert history["summary"]["route_remediation_blocked_total"] == 1
    assert history["summary"]["route_remediation_degraded_total"] == 1
    assert history["summary"]["route_remediation_setup_followup_total"] == 2
    assert history["summary"]["route_remediation_provider_blocked_total"] == 1
    assert history["summary"]["route_remediation_resolved_total"] == 1
    assert history["summary"]["route_remediation_improved_total"] == 1
    assert history["summary"]["route_remediation_persistent_total"] == 0
    assert history["summary"]["route_remediation_new_total"] == 0
    assert history["summary"]["route_remediation_resolved_setup_followup_total"] == 1
    assert history["summary"]["route_remediation_persistent_provider_blocked_total"] == 0
    assert history["summary"]["continuation_total"] == 4
    assert history["summary"]["continuation_auto_runnable_total"] == 3
    assert history["summary"]["continuation_manual_total"] == 1
    assert history["summary"]["continuation_retry_total"] == 2
    assert history["summary"]["continuation_provider_blocked_total"] == 1
    assert history["summary"]["continuation_setup_followup_total"] == 2
    assert history["summary"]["vm_prepare_total"] == 2
    assert history["summary"]["prepared_vm_control_total"] == 2
    assert history["summary"]["vm_ready_guest_total"] == 1
    assert history["summary"]["vm_attention_guest_total"] == 1
    assert history["summary"]["vm_blocked_guest_total"] == 0
    assert history["summary"]["vm_setup_followup_guest_total"] == 1
    assert history["summary"]["vm_memory_followthrough_total"] == 1
