from __future__ import annotations

from pathlib import Path

from backend.python.core.desktop_mission_memory import DesktopMissionMemory


def test_desktop_mission_memory_saves_and_resolves_paused_mission(tmp_path: Path) -> None:
    memory = DesktopMissionMemory(store_path=str(tmp_path / "desktop_mission_memory.json"))

    saved = memory.save_paused_mission(
        mission_kind="wizard",
        args={"app_name": "installer"},
        resume_contract={
            "resume_action": "complete_wizard_flow",
            "resume_signature": "wizard-resume-1",
            "anchor_app_name": "installer",
            "resume_payload": {"action": "complete_wizard_flow", "app_name": "installer"},
        },
        blocking_surface={
            "approval_kind": "elevation_consent",
            "dialog_kind": "elevation_prompt",
            "window_title": "User Account Control",
            "surface_signature": "surface-uac-1",
        },
        mission_payload={
            "status": "partial",
            "message": "Installer paused on administrator approval.",
            "stop_reason_code": "elevation_consent_required",
            "stop_reason": "Administrator approval is required.",
            "risk_level": "high",
            "page_count": 2,
            "pages_completed": 1,
            "page_history": [{"page_index": 1, "status": "blocked"}],
            "final_page": {"screen_hash": "wizard_uac_dialog"},
        },
        warnings=["Administrator approval is required."],
        message="Installer paused on administrator approval.",
    )

    mission = saved["mission"]
    mission_id = str(mission["mission_id"])

    assert saved["status"] == "success"
    assert mission_id.startswith("dm_")
    assert mission["status"] == "paused"
    assert mission["resume_contract"]["mission_id"] == mission_id
    assert mission["blocking_surface"]["mission_id"] == mission_id

    resolved = memory.resolve_resume_reference(mission_id=mission_id)
    assert resolved["status"] == "success"
    assert resolved["mission"]["mission_id"] == mission_id

    fallback = memory.resolve_resume_reference(mission_kind="wizard", app_name="installer")
    assert fallback["status"] == "success"
    assert fallback["mission"]["mission_id"] == mission_id

    snapshot = memory.snapshot(status="paused", mission_kind="wizard", app_name="installer")
    assert snapshot["status"] == "success"
    assert snapshot["count"] == 1
    assert snapshot["items"][0]["mission_id"] == mission_id
    assert snapshot["status_counts"] == {"paused": 1}
    assert snapshot["mission_kind_counts"] == {"wizard": 1}
    assert snapshot["approval_kind_counts"] == {"elevation_consent": 1}
    assert snapshot["approval_state_counts"] == {"admin_blocked": 1}
    assert snapshot["approval_scope_counts"] == {"admin": 1}
    assert snapshot["recovery_profile_counts"] == {"admin_review": 1}
    assert snapshot["risk_level_counts"] == {"high": 1}
    assert snapshot["app_counts"] == {"installer": 1}
    assert snapshot["stop_reason_counts"] == {"elevation_consent_required": 1}
    assert snapshot["resume_ready_count"] == 0
    assert snapshot["manual_attention_count"] == 1
    assert snapshot["admin_approval_count"] == 1
    assert snapshot["destructive_approval_count"] == 0
    assert snapshot["critical_risk_count"] == 0
    assert snapshot["latest_paused"]["mission_id"] == mission_id
    assert snapshot["items"][0]["recovery_profile"] == "admin_review"
    assert snapshot["items"][0]["approval_blocked"] is True
    assert snapshot["items"][0]["manual_attention_required"] is True
    assert snapshot["items"][0]["resume_ready"] is False
    assert snapshot["items"][0]["approval_state"] == "admin_blocked"
    assert snapshot["items"][0]["admin_clearance_required"] is True
    assert snapshot["items"][0]["approval_summary"]


def test_desktop_mission_memory_marks_resume_and_resets_entries(tmp_path: Path) -> None:
    memory = DesktopMissionMemory(store_path=str(tmp_path / "desktop_mission_memory.json"))

    saved = memory.save_paused_mission(
        mission_kind="form",
        args={"app_name": "settings", "mission_id": "dm_form_1"},
        resume_contract={
            "mission_id": "dm_form_1",
            "resume_action": "complete_form_flow",
            "resume_signature": "form-resume-1",
            "anchor_app_name": "settings",
        },
        blocking_surface={
            "mission_id": "dm_form_1",
            "approval_kind": "permission_review",
            "dialog_kind": "permission_review",
            "window_title": "Windows Security",
        },
        mission_payload={
            "status": "partial",
            "message": "Settings flow paused for review.",
            "stop_reason_code": "permission_review_required",
            "page_count": 1,
            "pages_completed": 0,
        },
        warnings=["Permission review is required."],
        message="Settings flow paused for review.",
    )

    assert saved["status"] == "success"

    resumed = memory.mark_resumed(
        mission_id="dm_form_1",
        outcome_status="success",
        message="Settings flow completed.",
        completed=True,
        mission_payload={"final_page": {"screen_hash": "settings_done"}},
    )
    assert resumed["status"] == "success"
    assert resumed["mission"]["status"] == "completed"
    assert resumed["mission"]["latest_result_status"] == "success"
    assert resumed["mission"]["final_page"]["screen_hash"] == "settings_done"
    assert int(resumed["mission"]["resume_attempts"] or 0) == 1

    reset = memory.reset(mission_id="dm_form_1")
    assert reset["status"] == "success"
    assert reset["removed"] == 1

    remaining = memory.snapshot()
    assert remaining["status"] == "success"
    assert remaining["count"] == 0
    assert remaining["status_counts"] == {}
    assert remaining["recovery_profile_counts"] == {}
    assert remaining["app_counts"] == {}
    assert remaining["stop_reason_counts"] == {}
    assert remaining["approval_state_counts"] == {}
    assert remaining["approval_scope_counts"] == {}
    assert remaining["risk_level_counts"] == {}
    assert remaining["resume_ready_count"] == 0
    assert remaining["manual_attention_count"] == 0
    assert remaining["admin_approval_count"] == 0
    assert remaining["destructive_approval_count"] == 0
    assert remaining["critical_risk_count"] == 0
    assert remaining["latest_paused"] is None


def test_desktop_mission_memory_marks_resume_ready_profiles_for_non_blocked_pauses(tmp_path: Path) -> None:
    memory = DesktopMissionMemory(store_path=str(tmp_path / "desktop_mission_memory.json"))

    saved = memory.save_paused_mission(
        mission_kind="form",
        args={"app_name": "settings"},
        resume_contract={
            "resume_action": "complete_form_flow",
            "resume_signature": "resume-ready-1",
            "anchor_app_name": "settings",
        },
        blocking_surface={
            "window_title": "Bluetooth & devices",
            "surface_signature": "surface-settings-ready-1",
        },
        mission_payload={
            "status": "partial",
            "message": "Settings flow paused after the operator cleared the confirmation.",
            "stop_reason_code": "resume_ready",
        },
        message="Settings flow paused after the operator cleared the confirmation.",
    )

    mission = saved["mission"]
    snapshot = memory.snapshot(status="paused", app_name="settings")

    assert mission["recovery_profile"] == "resume_ready"
    assert mission["resume_ready"] is True
    assert mission["manual_attention_required"] is False
    assert mission["approval_state"] == "resume_ready"
    assert snapshot["recovery_profile_counts"] == {"resume_ready": 1}
    assert snapshot["approval_state_counts"] == {"resume_ready": 1}
    assert snapshot["approval_scope_counts"] == {"resume": 1}
    assert snapshot["resume_ready_count"] == 1
    assert snapshot["manual_attention_count"] == 0


def test_desktop_mission_memory_tracks_destructive_review_policy_state(tmp_path: Path) -> None:
    memory = DesktopMissionMemory(store_path=str(tmp_path / "desktop_mission_memory.json"))

    saved = memory.save_paused_mission(
        mission_kind="form",
        args={"app_name": "settings"},
        resume_contract={
            "resume_action": "complete_form_flow",
            "resume_signature": "destructive-review-1",
            "anchor_app_name": "settings",
        },
        blocking_surface={
            "window_title": "Delete Confirmation",
            "dialog_kind": "destructive_warning",
            "destructive_dialog_buttons": ["Delete"],
            "surface_signature": "surface-delete-confirmation",
        },
        mission_payload={
            "status": "partial",
            "message": "Settings flow paused on a destructive confirmation.",
            "stop_reason_code": "destructive_confirmation_required",
            "risk_level": "critical",
        },
        message="Settings flow paused on a destructive confirmation.",
    )

    mission = saved["mission"]
    snapshot = memory.snapshot(status="paused", app_name="settings")

    assert mission["approval_state"] == "destructive_review"
    assert mission["approval_scope"] == "destructive"
    assert mission["destructive_confirmation"] is True
    assert mission["critical_risk"] is True
    assert mission["recovery_profile"] == "destructive_review"
    assert snapshot["approval_state_counts"] == {"destructive_review": 1}
    assert snapshot["approval_scope_counts"] == {"destructive": 1}
    assert snapshot["destructive_approval_count"] == 1
    assert snapshot["critical_risk_count"] == 1


def test_desktop_mission_memory_matches_anchor_and_blocking_window_titles_for_app_filters(tmp_path: Path) -> None:
    memory = DesktopMissionMemory(store_path=str(tmp_path / "desktop_mission_memory.json"))

    saved = memory.save_paused_mission(
        mission_kind="wizard",
        args={"app_name": ""},
        resume_contract={
            "resume_action": "complete_wizard_flow",
            "resume_signature": "resume-window-1",
            "anchor_app_name": "",
            "anchor_window_title": "Windows Security",
        },
        blocking_surface={
            "window_title": "User Account Control",
            "approval_kind": "elevation_consent",
            "surface_signature": "surface-window-1",
        },
        mission_payload={
            "status": "partial",
            "message": "Security flow paused on a child approval window.",
            "stop_reason_code": "elevation_consent_required",
        },
        message="Security flow paused on a child approval window.",
    )

    mission_id = str(saved["mission"]["mission_id"])

    resolved = memory.resolve_resume_reference(mission_kind="wizard", app_name="account control")
    assert resolved["status"] == "success"
    assert resolved["mission"]["mission_id"] == mission_id

    snapshot = memory.snapshot(app_name="windows security")
    assert snapshot["count"] == 1
    assert snapshot["items"][0]["mission_id"] == mission_id

    reset = memory.reset(app_name="account control")
    assert reset["status"] == "success"
    assert reset["removed"] == 1


def test_desktop_mission_memory_tracks_exploration_recovery_profiles(tmp_path: Path) -> None:
    memory = DesktopMissionMemory(store_path=str(tmp_path / "desktop_mission_memory.json"))

    saved_ready = memory.save_paused_mission(
        mission_kind="exploration",
        args={"app_name": "settings", "query": "bluetooth"},
        resume_contract={
            "resume_action": "advance_surface_exploration",
            "resume_signature": "exploration-ready-1",
            "anchor_app_name": "settings",
            "resume_payload": {"action": "advance_surface_exploration", "app_name": "settings", "query": "bluetooth"},
        },
        blocking_surface={
            "window_title": "Settings",
            "surface_signature": "surface-exploration-ready-1",
            "surface_mode": "list_navigation",
        },
        mission_payload={
            "status": "partial",
            "message": "JARVIS advanced the surface and found another bounded recon step.",
            "stop_reason_code": "exploration_nested_branch_available",
            "surface_mode": "list_navigation",
            "exploration_query": "bluetooth",
            "hypothesis_count": 2,
            "branch_action_count": 1,
            "attempted_target_count": 1,
            "alternative_target_count": 1,
            "alternative_hypothesis_count": 1,
            "alternative_branch_action_count": 0,
            "step_count": 1,
            "steps_completed": 1,
            "max_steps": 3,
            "selected_action": "select_list_item",
            "selected_candidate_id": "list_bluetooth",
            "selected_candidate_label": "Bluetooth",
            "rust_router_hint": "prefer_query_match",
            "rust_loop_risk": False,
            "surface_topology_signature": "settings|2|1",
            "topology_visible_window_count": 2,
            "topology_dialog_like_count": 1,
            "topology_same_process_window_count": 2,
            "topology_owner_link_count": 2,
            "topology_owner_chain_visible": True,
            "topology_same_root_owner_window_count": 3,
            "topology_same_root_owner_dialog_like_count": 2,
            "topology_active_owner_chain_depth": 1,
            "topology_max_owner_chain_depth": 2,
            "topology_modal_chain_signature": "5000|2|2|1",
            "topology_branch_family_signature": "5000|2|Bluetooth & devices|Pair device",
            "transition_kind": "child_window",
            "nested_surface_progressed": True,
            "child_window_adopted": True,
            "surface_path_tail": ["Devices", "Bluetooth"],
            "window_title_history_tail": ["Settings", "Bluetooth & devices"],
            "nested_progress_count": 1,
            "last_branch_kind": "child_window",
            "branch_transition_count": 1,
            "branch_repeat_count": 1,
            "branch_family_signature": "5000|2|Bluetooth & devices|Pair device",
            "branch_family_repeat_count": 1,
            "branch_family_switch_count": 0,
            "branch_family_continuity": True,
            "surface_path_depth": 2,
            "nested_chain_count": 1,
            "child_window_chain_count": 1,
            "dialog_cascade_count": 0,
            "pane_cascade_count": 0,
            "drilldown_cascade_count": 0,
            "branch_history": [
                {
                    "step_index": 1,
                    "transition_kind": "child_window",
                    "selected_action": "select_list_item",
                    "selected_candidate_id": "list_bluetooth",
                    "selected_candidate_label": "Bluetooth",
                    "window_title": "Bluetooth & devices",
                    "surface_path_tail": ["Devices", "Bluetooth"],
                    "topology_branch_family_signature": "5000|2|Bluetooth & devices|Pair device",
                    "occurrences": 1,
                }
            ],
            "attempted_targets": [
                {
                    "candidate_id": "list_bluetooth",
                    "selected_action": "select_list_item",
                    "selected_candidate_label": "Bluetooth",
                }
            ],
            "surface_signature_history": ["surface-exploration-ready-1", "surface-exploration-ready-2"],
        },
        message="JARVIS advanced the surface and found another bounded recon step.",
    )

    ready_mission = saved_ready["mission"]
    assert ready_mission["mission_kind"] == "exploration"
    assert ready_mission["recovery_profile"] == "resume_ready"
    assert ready_mission["resume_ready"] is True
    assert ready_mission["surface_mode"] == "list_navigation"
    assert ready_mission["selected_action"] == "select_list_item"
    assert ready_mission["selected_candidate_label"] == "Bluetooth"
    assert ready_mission["rust_router_hint"] == "prefer_query_match"
    assert ready_mission["surface_topology_signature"] == "settings|2|1"
    assert ready_mission["topology_visible_window_count"] == 2
    assert ready_mission["topology_dialog_like_count"] == 1
    assert ready_mission["topology_same_process_window_count"] == 2
    assert ready_mission["topology_owner_link_count"] == 2
    assert ready_mission["topology_owner_chain_visible"] is True
    assert ready_mission["topology_same_root_owner_window_count"] == 3
    assert ready_mission["topology_same_root_owner_dialog_like_count"] == 2
    assert ready_mission["topology_active_owner_chain_depth"] == 1
    assert ready_mission["topology_max_owner_chain_depth"] == 2
    assert ready_mission["topology_modal_chain_signature"] == "5000|2|2|1"
    assert ready_mission["topology_branch_family_signature"] == "5000|2|Bluetooth & devices|Pair device"
    assert ready_mission["attempted_target_count"] == 1
    assert ready_mission["alternative_target_count"] == 1
    assert ready_mission["transition_kind"] == "child_window"
    assert ready_mission["nested_surface_progressed"] is True
    assert ready_mission["child_window_adopted"] is True
    assert ready_mission["surface_path_tail"] == ["Devices", "Bluetooth"]
    assert ready_mission["window_title_history_tail"] == ["Settings", "Bluetooth & devices"]
    assert ready_mission["nested_progress_count"] == 1
    assert ready_mission["last_branch_kind"] == "child_window"
    assert ready_mission["branch_transition_count"] == 1
    assert ready_mission["branch_repeat_count"] == 1
    assert ready_mission["branch_family_signature"] == "5000|2|Bluetooth & devices|Pair device"
    assert ready_mission["branch_family_repeat_count"] == 1
    assert ready_mission["branch_family_switch_count"] == 0
    assert ready_mission["branch_family_continuity"] is True
    assert ready_mission["surface_path_depth"] == 2
    assert ready_mission["nested_chain_count"] == 1
    assert ready_mission["child_window_chain_count"] == 1
    assert ready_mission["dialog_cascade_count"] == 0
    assert ready_mission["pane_cascade_count"] == 0
    assert ready_mission["drilldown_cascade_count"] == 0
    assert ready_mission["branch_history_tail"][0]["window_title"] == "Bluetooth & devices"
    assert ready_mission["attempted_targets_tail"][0]["candidate_id"] == "list_bluetooth"
    assert ready_mission["surface_signature_history"] == ["surface-exploration-ready-1", "surface-exploration-ready-2"]

    saved_review = memory.save_paused_mission(
        mission_kind="exploration",
        args={"app_name": "settings", "query": "advanced display"},
        resume_contract={
            "resume_action": "advance_surface_exploration",
            "resume_signature": "exploration-review-1",
            "anchor_app_name": "settings",
        },
        blocking_surface={
            "window_title": "Settings",
            "surface_signature": "surface-exploration-review-1",
            "surface_mode": "form_navigation",
        },
        mission_payload={
            "status": "blocked",
            "message": "The current unsupported-app surface still needs human review before safe exploration can continue.",
            "stop_reason_code": "exploration_no_safe_path",
            "surface_mode": "form_navigation",
            "selected_action": "select_sidebar_item",
            "selected_candidate_label": "Advanced display",
        },
        message="The current unsupported-app surface still needs human review before safe exploration can continue.",
    )

    review_mission = saved_review["mission"]
    snapshot = memory.snapshot(status="paused", mission_kind="exploration", app_name="settings")

    assert review_mission["recovery_profile"] == "surface_review"
    assert review_mission["manual_attention_required"] is True
    assert review_mission["resume_ready"] is False
    assert snapshot["count"] == 2
    assert snapshot["mission_kind_counts"] == {"exploration": 2}
    assert snapshot["recovery_profile_counts"] == {"resume_ready": 1, "surface_review": 1}


def test_desktop_mission_memory_tracks_nested_chain_limit_resume_ready(tmp_path: Path) -> None:
    memory = DesktopMissionMemory(store_path=str(tmp_path / "desktop_mission_memory.json"))

    saved = memory.save_paused_mission(
        mission_kind="exploration",
        args={"app_name": "settings", "query": "pair device"},
        resume_contract={
            "resume_action": "complete_surface_exploration_flow",
            "resume_signature": "exploration-chain-limit-1",
            "anchor_app_name": "settings",
            "resume_payload": {
                "action": "complete_surface_exploration_flow",
                "app_name": "settings",
                "query": "pair device",
                "max_nested_branch_steps": 1,
                "max_branch_family_switches": 1,
                "max_branch_cascade_steps": 1,
            },
        },
        blocking_surface={
            "window_title": "Pair device",
            "surface_signature": "surface-exploration-chain-limit-1",
            "surface_mode": "dialog_resolution",
            "nested_chain_count": 2,
            "child_window_chain_count": 2,
            "dialog_cascade_count": 1,
            "branch_cascade_count": 2,
            "branch_cascade_kind_count": 2,
            "branch_cascade_signature": "child_window_chain>dialog_shift",
            "max_branch_family_switches": 1,
            "max_branch_cascade_steps": 1,
            "topology_owner_link_count": 2,
            "topology_owner_chain_visible": True,
            "topology_same_root_owner_window_count": 3,
            "topology_same_root_owner_dialog_like_count": 2,
            "topology_active_owner_chain_depth": 2,
            "topology_max_owner_chain_depth": 2,
            "topology_modal_chain_signature": "5000|2|2|2",
            "topology_branch_family_signature": "5000|2|Bluetooth & devices|Pair device",
        },
        mission_payload={
            "status": "partial",
            "message": "JARVIS paused at the configured branch-cascade limit.",
            "stop_reason_code": "exploration_branch_cascade_limit_reached",
            "surface_mode": "dialog_resolution",
            "nested_chain_count": 2,
            "child_window_chain_count": 2,
            "dialog_cascade_count": 1,
            "pane_cascade_count": 0,
            "drilldown_cascade_count": 0,
            "branch_cascade_count": 2,
            "branch_cascade_kind_count": 2,
            "branch_cascade_signature": "child_window_chain>dialog_shift",
            "max_branch_family_switches": 1,
            "max_branch_cascade_steps": 1,
            "topology_owner_link_count": 2,
            "topology_owner_chain_visible": True,
            "topology_same_root_owner_window_count": 3,
            "topology_same_root_owner_dialog_like_count": 2,
            "topology_active_owner_chain_depth": 2,
            "topology_max_owner_chain_depth": 2,
            "topology_modal_chain_signature": "5000|2|2|2",
            "topology_branch_family_signature": "5000|2|Bluetooth & devices|Pair device",
            "branch_family_signature": "5000|2|Bluetooth & devices|Pair device",
            "branch_family_repeat_count": 2,
            "branch_family_switch_count": 0,
            "branch_family_continuity": True,
        },
        message="JARVIS paused at the configured branch-cascade limit.",
    )

    mission = saved["mission"]
    snapshot = memory.snapshot(status="paused", mission_kind="exploration", app_name="settings")

    assert mission["recovery_profile"] == "resume_ready"
    assert mission["recovery_priority"] == 91
    assert mission["resume_ready"] is True
    assert mission["nested_chain_count"] == 2
    assert mission["child_window_chain_count"] == 2
    assert mission["dialog_cascade_count"] == 1
    assert mission["branch_cascade_count"] == 2
    assert mission["branch_cascade_kind_count"] == 2
    assert mission["branch_cascade_signature"] == "child_window_chain>dialog_shift"
    assert mission["max_branch_family_switches"] == 1
    assert mission["max_branch_cascade_steps"] == 1
    assert mission["topology_owner_link_count"] == 2
    assert mission["topology_owner_chain_visible"] is True
    assert mission["topology_same_root_owner_window_count"] == 3
    assert mission["topology_same_root_owner_dialog_like_count"] == 2
    assert mission["topology_active_owner_chain_depth"] == 2
    assert mission["topology_max_owner_chain_depth"] == 2
    assert mission["topology_modal_chain_signature"] == "5000|2|2|2"
    assert mission["topology_branch_family_signature"] == "5000|2|Bluetooth & devices|Pair device"
    assert mission["branch_family_signature"] == "5000|2|Bluetooth & devices|Pair device"
    assert mission["branch_family_repeat_count"] == 2
    assert mission["branch_family_switch_count"] == 0
    assert mission["branch_family_continuity"] is True
    assert snapshot["recovery_profile_counts"] == {"resume_ready": 1}
    assert snapshot["stop_reason_counts"] == {"exploration_branch_cascade_limit_reached": 1}


def test_desktop_mission_memory_marks_branch_family_switch_limit_as_resume_ready() -> None:
    memory = DesktopMissionMemory()

    saved = memory.save_paused_mission(
        mission_kind="exploration",
        args={
            "action": "complete_surface_exploration_flow",
            "app_name": "settings",
            "query": "add device",
        },
        resume_contract={
            "mission_kind": "exploration",
            "resume_action": "complete_surface_exploration_flow",
            "anchor_app_name": "settings",
            "resume_payload": {
                "action": "complete_surface_exploration_flow",
                "app_name": "settings",
                "query": "add device",
                "max_branch_family_switches": 1,
            },
        },
        blocking_surface={
            "window_title": "Add device",
            "surface_signature": "surface-exploration-family-switch-1",
            "surface_mode": "dialog_resolution",
            "branch_family_signature": "5000|2|Bluetooth & devices|Add device",
            "branch_family_switch_count": 1,
            "max_branch_family_switches": 1,
        },
        mission_payload={
            "status": "partial",
            "message": "JARVIS paused at the configured branch-family switch limit.",
            "stop_reason_code": "exploration_branch_family_switch_limit_reached",
            "surface_mode": "dialog_resolution",
            "branch_family_signature": "5000|2|Bluetooth & devices|Add device",
            "branch_family_switch_count": 1,
            "max_branch_family_switches": 1,
        },
        message="JARVIS paused at the configured branch-family switch limit.",
    )

    mission = saved["mission"]

    assert mission["recovery_profile"] == "resume_ready"
    assert mission["recovery_priority"] == 92
    assert mission["resume_ready"] is True
    assert mission["branch_family_signature"] == "5000|2|Bluetooth & devices|Add device"
    assert mission["branch_family_switch_count"] == 1
    assert mission["max_branch_family_switches"] == 1


def test_desktop_mission_memory_marks_descendant_chain_limit_as_resume_ready(tmp_path: Path) -> None:
    memory = DesktopMissionMemory(store_path=str(tmp_path / "desktop_mission_memory_descendant.json"))

    saved = memory.save_paused_mission(
        mission_kind="exploration",
        args={
            "action": "complete_surface_exploration_flow",
            "app_name": "settings",
            "query": "pair bluetooth",
        },
        resume_contract={
            "mission_kind": "exploration",
            "resume_action": "complete_surface_exploration_flow",
            "anchor_app_name": "settings",
            "resume_payload": {
                "action": "complete_surface_exploration_flow",
                "app_name": "settings",
                "query": "pair bluetooth",
                "max_nested_branch_steps": 1,
                "max_branch_cascade_steps": 1,
                "max_descendant_chain_steps": 2,
            },
        },
        blocking_surface={
            "window_title": "Finish Pairing",
            "surface_signature": "surface-exploration-descendant-limit-1",
            "surface_mode": "dialog_resolution",
            "descendant_chain_repeat_count": 2,
            "descendant_chain_continuity": True,
            "max_descendant_chain_steps": 2,
            "topology_direct_child_window_count": 1,
            "topology_direct_child_dialog_like_count": 1,
            "topology_descendant_chain_depth": 3,
            "topology_descendant_dialog_chain_depth": 3,
            "topology_descendant_query_match_count": 1,
            "topology_child_chain_signature": "2221|Bluetooth & devices|pairing_chain",
        },
        mission_payload={
            "status": "partial",
            "message": "JARVIS paused at the configured descendant-chain limit.",
            "stop_reason_code": "exploration_descendant_chain_limit_reached",
            "surface_mode": "dialog_resolution",
            "descendant_chain_repeat_count": 2,
            "descendant_chain_continuity": True,
            "max_descendant_chain_steps": 2,
            "topology_direct_child_window_count": 1,
            "topology_direct_child_dialog_like_count": 1,
            "topology_descendant_chain_depth": 3,
            "topology_descendant_dialog_chain_depth": 3,
            "topology_descendant_query_match_count": 1,
            "topology_child_chain_signature": "2221|Bluetooth & devices|pairing_chain",
        },
        message="JARVIS paused at the configured descendant-chain limit.",
    )

    mission = saved["mission"]
    snapshot = memory.snapshot(status="paused", mission_kind="exploration", app_name="settings")

    assert mission["recovery_profile"] == "resume_ready"
    assert mission["recovery_priority"] == 93
    assert mission["resume_ready"] is True
    assert mission["max_descendant_chain_steps"] == 2
    assert mission["descendant_chain_repeat_count"] == 2
    assert mission["descendant_chain_continuity"] is True
    assert mission["topology_direct_child_window_count"] == 1
    assert mission["topology_direct_child_dialog_like_count"] == 1
    assert mission["topology_descendant_chain_depth"] == 3
    assert mission["topology_descendant_dialog_chain_depth"] == 3
    assert mission["topology_descendant_query_match_count"] == 1
    assert mission["topology_child_chain_signature"] == "2221|Bluetooth & devices|pairing_chain"
    assert snapshot["stop_reason_counts"] == {"exploration_descendant_chain_limit_reached": 1}
